/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX STORAGE
#include "ob_sstable_rowkey_helper.h"
#include "lib/container/ob_fixed_array_iterator.h"
#include "share/ob_get_compat_mode.h"
#include "storage/ob_sstable.h"

namespace oceanbase {
using namespace common;
using namespace blocksstable;
using namespace share;
using namespace share::schema;
namespace storage {
/*
 *ObRowkeyObjComparer
 */
// temporarily keep if in compare func
// remove if to build more compare funcs array when perf critical
int ObRowkeyObjComparer::sstable_number_cmp_func(const ObObj& obj1, const ObObj& obj2, const ObCompareCtx& cmp_ctx)
{
  int cmp = 0;
  UNUSED(cmp_ctx);
  if (OB_UNLIKELY(!obj1.is_number() || !obj2.is_number())) {
    LOG_ERROR("unexpected error. mismatch function for comparison", K(obj1), K(obj2));
    right_to_die_or_duty_to_live();
  } else if (OB_LIKELY(((is_smallint_number(obj1) && is_smallint_number(obj2))))) {
    cmp = obj1.v_.nmb_digits_[0] - obj2.v_.nmb_digits_[0];
    cmp = cmp > 0 ? ObObjCmpFuncs::CR_GT : cmp < 0 ? ObObjCmpFuncs::CR_LT : ObObjCmpFuncs::CR_EQ;
  } else {
    cmp = number::ObNumber::compare(obj1.nmb_desc_, obj1.v_.nmb_digits_, obj2.nmb_desc_, obj2.v_.nmb_digits_);
  }
  return cmp;
}

int ObRowkeyObjComparer::sstable_collation_free_cmp_func(
    const ObObj& obj1, const ObObj& obj2, const ObCompareCtx& cmp_ctx)
{
  int cmp = 0;

  UNUSED(cmp_ctx);
  if (OB_UNLIKELY(
          CS_TYPE_COLLATION_FREE != obj2.get_collation_type() || obj1.get_collation_type() != obj2.get_collation_type()
          //|| obj1.get_type() != obj2.get_type()
          //|| !obj1.is_varchar_or_char()
          )) {
    STORAGE_LOG(ERROR, "unexpected error, invalid argument", K(obj1), K(obj2));
    right_to_die_or_duty_to_live();
  } else {
    const int32_t lhs_len = obj1.get_val_len();
    const int32_t rhs_len = obj2.get_val_len();
    const int32_t cmp_len = MIN(lhs_len, rhs_len);
    cmp = MEMCMP(obj1.get_string_ptr(), obj2.get_string_ptr(), cmp_len);
    if (0 == cmp) {
      if (lhs_len != rhs_len) {
        // in mysql mode, we should strip all trailing blanks before comparing two strings
        const int32_t left_len = (lhs_len > cmp_len) ? lhs_len - cmp_len : rhs_len - cmp_len;
        const char* ptr = (lhs_len > cmp_len) ? obj1.get_string_ptr() : obj2.get_string_ptr();
        const unsigned char* uptr = reinterpret_cast<const unsigned char*>(ptr + cmp_len);
        // int32_t is always capable of stroing the max lenth of varchar or char
        for (int32_t i = 0; i < left_len; ++i) {
          if (*(uptr + i) != ' ') {
            // special behavior in mysql mode, 'a\1 < a' and 'ab > a'
            if (*(uptr + i) < ' ') {
              cmp = lhs_len > cmp_len ? ObObjCmpFuncs::CR_LT : ObObjCmpFuncs::CR_GT;
            } else {
              cmp = lhs_len > cmp_len ? ObObjCmpFuncs::CR_GT : ObObjCmpFuncs::CR_LT;
            }
            break;
          }
        }
      }
    } else {
      cmp = cmp > 0 ? ObObjCmpFuncs::CR_GT : ObObjCmpFuncs::CR_LT;
    }
  }

  return cmp;
}

int ObRowkeyObjComparer::sstable_oracle_collation_free_cmp_func(
    const ObObj& obj1, const ObObj& obj2, const ObCompareCtx& cmp_ctx)
{
  int cmp = 0;

  UNUSED(cmp_ctx);
  if (OB_UNLIKELY(
          CS_TYPE_COLLATION_FREE != obj2.get_collation_type() || obj1.get_collation_type() != obj2.get_collation_type()
          //|| obj1.get_type() != obj2.get_type()
          //|| !obj1.is_varchar_or_char()
          )) {
    STORAGE_LOG(ERROR, "unexpected error, invalid argument", K(obj1), K(obj2));
    right_to_die_or_duty_to_live();
  } else {
    const int32_t lhs_len = obj1.get_val_len();
    const int32_t rhs_len = obj2.get_val_len();
    const int32_t cmp_len = MIN(lhs_len, rhs_len);
    cmp = MEMCMP(obj1.get_string_ptr(), obj2.get_string_ptr(), cmp_len);
    if (0 == cmp) {
      // in oracle mode, we should consider the trailing blanks during comparing two varchar
      if (obj1.is_varying_len_char_type()) {
        if (lhs_len != rhs_len) {
          cmp = lhs_len > cmp_len ? ObObjCmpFuncs::CR_GT : ObObjCmpFuncs::CR_LT;
        }
      } else {
        // in oracle mode, we should strip all trailing blanks before comparing two char
        const int32_t left_len = (lhs_len > cmp_len) ? lhs_len - cmp_len : rhs_len - cmp_len;
        const char* ptr = (lhs_len > cmp_len) ? obj1.get_string_ptr() : obj2.get_string_ptr();
        const unsigned char* uptr = reinterpret_cast<const unsigned char*>(ptr + cmp_len);
        // int32_t is always capable of stroing the max lenth of varchar or char
        for (int32_t i = 0; i < left_len; ++i) {
          if (*(uptr + i) != ' ') {
            cmp = lhs_len > cmp_len ? ObObjCmpFuncs::CR_GT : ObObjCmpFuncs::CR_LT;
            break;
          }
        }
      }
    } else {
      cmp = cmp > 0 ? ObObjCmpFuncs::CR_GT : ObObjCmpFuncs::CR_LT;
    }
  }

  return cmp;
}

ObRowkeyObjComparer::ObRowkeyObjComparer()
    : cmp_func_(NULL),
      cmp_ctx_(ObMaxType, CS_TYPE_INVALID, false, INVALID_TZ_OFF, NULL_FIRST),
      is_collation_free_(false),
      type_(COMPARER_MYSQL)
{}

void ObRowkeyObjComparer::reset()
{
  cmp_func_ = NULL;
  cmp_ctx_.cmp_cs_type_ = CS_TYPE_INVALID;
  is_collation_free_ = false;
}

int ObRowkeyObjComparer::init_compare_func(const ObObjMeta& obj_meta)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!obj_meta.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to init compare func", K(obj_meta), K(ret));
  } else {
    reset();
    if (obj_meta.is_number()) {
      cmp_func_ = sstable_number_cmp_func;
    } else {
      ObObjTypeClass obj_tc = obj_meta.get_type_class();
      if (OB_FAIL(ObObjCmpFuncs::get_cmp_func(obj_tc, obj_tc, CO_CMP, cmp_func_))) {
        STORAGE_LOG(WARN, "Failed to find rowkey obj cmp func", K(obj_meta), K(obj_tc), K(ret));
      } else if (OB_ISNULL(cmp_func_)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Failed to find rowkey cmp func", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      cmp_ctx_.cmp_cs_type_ = obj_meta.get_collation_type();
      is_collation_free_ = false;
    }
  }

  return ret;
}

int ObRowkeyObjComparer::init_compare_func_collation_free(const common::ObObjMeta& obj_meta)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!obj_meta.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to init collation free compare func", K(obj_meta), K(ret));
  } else if (obj_meta.is_collation_free_compatible()) {
    reset();
    cmp_func_ = sstable_collation_free_cmp_func;
    cmp_ctx_.cmp_cs_type_ = obj_meta.get_collation_type();
    is_collation_free_ = true;
  } else if (OB_FAIL(init_compare_func(obj_meta))) {
    STORAGE_LOG(WARN, "Failed to init compare func", K(obj_meta), K(ret));
  }

  return ret;
}

/*
 *ObRowkeyObjComparerOracle
 */

int ObRowkeyObjComparerOracle::init_compare_func_collation_free(const common::ObObjMeta& obj_meta)
{
  int ret = OB_SUCCESS;

  // TODO  oracle mode need add raw type future
  if (OB_UNLIKELY(!obj_meta.is_valid() || !obj_meta.is_collation_free_compatible())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to init collation free compare func", K(obj_meta), K(ret));
  } else {
    reset();
    cmp_func_ = sstable_oracle_collation_free_cmp_func;
    cmp_ctx_.cmp_cs_type_ = obj_meta.get_collation_type();
    is_collation_free_ = true;
  }

  return ret;
}

/*
 *ObSSTableRowkeyHelper
 */

ObSSTableRowkeyHelper::ObSSTableRowkeyHelper()
    : endkeys_(),
      collation_free_endkeys_(),
      cmp_funcs_(),
      collation_free_cmp_funcs_(),
      rowkey_column_cnt_(0),
      column_type_array_(NULL),
      sstable_(nullptr),
      exist_collation_free_(false),
      use_cmp_nullsafe_(false),
      is_oracle_mode_(false),
      is_inited_(false)
{}

void ObSSTableRowkeyHelper::reset()
{
  endkeys_.reset();
  collation_free_endkeys_.reset();
  cmp_funcs_.reset();
  collation_free_cmp_funcs_.reset();
  rowkey_column_cnt_ = 0;
  column_type_array_ = NULL;
  sstable_ = nullptr;
  exist_collation_free_ = false;
  use_cmp_nullsafe_ = false;
  is_oracle_mode_ = false;
  is_inited_ = false;
}

int ObSSTableRowkeyHelper::get_macro_block_meta(
    const MacroBlockId& macro_id, ObFullMacroBlockMeta& macro_meta, const int64_t schema_rowkey_column_cnt)
{
  int ret = OB_SUCCESS;

  if (!macro_id.is_valid() || schema_rowkey_column_cnt <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to get macro block meta", K(macro_id), K(schema_rowkey_column_cnt), K(ret));
  } else if (OB_FAIL(sstable_->get_meta(macro_id, macro_meta))) {
    STORAGE_LOG(WARN, "fail to get meta", K(ret), K(macro_id));
  } else if (!macro_meta.is_valid()) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(WARN, "Unexpected null macro meta", K(ret));
  } else if (OB_ISNULL(macro_meta.meta_->endkey_) || OB_ISNULL(macro_meta.schema_->column_type_array_)) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(WARN, "Unexpected null data macro endkey", K(macro_meta), K(ret));
  } else if (OB_UNLIKELY(macro_meta.meta_->rowkey_column_number_ < schema_rowkey_column_cnt)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN,
        "Unexpected mis matched rowkey column number",
        K(macro_meta.meta_->rowkey_column_number_),
        K(schema_rowkey_column_cnt),
        K(ret));
  }

  return ret;
}

int ObSSTableRowkeyHelper::build_macro_endkeys(const ObIArray<MacroBlockId>& macro_ids, ObIAllocator& allocator,
    const int64_t schema_rowkey_column_cnt, const bool need_build_collation_free)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(macro_ids.count() == 0 || schema_rowkey_column_cnt <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to build macro endkeys", K(macro_ids), K(schema_rowkey_column_cnt), K(ret));
  } else {
    int64_t macro_block_count = macro_ids.count();
    ObFullMacroBlockMeta full_meta;
    ObStoreRowkey rowkey;

    endkeys_.set_allocator(&allocator);
    endkeys_.set_capacity(static_cast<uint32_t>(macro_block_count));
    for (int64_t i = 0; OB_SUCC(ret) && i < macro_block_count; i++) {
      if (OB_FAIL(get_macro_block_meta(macro_ids.at(i), full_meta, schema_rowkey_column_cnt))) {
        STORAGE_LOG(WARN, "Failed to get macro block meta", K(i), K(ret));
      } else {
        const ObMacroBlockMetaV2* macro_meta = full_meta.meta_;
        rowkey.assign(macro_meta->endkey_, macro_meta->rowkey_column_number_);
        // TODO consider column order
        if (rowkey.contains_min_or_max_obj() || (!use_cmp_nullsafe_ && rowkey.contains_null_obj())) {
          ret = OB_ERR_SYS;
          STORAGE_LOG(WARN, "Unexpected max or min macro endkey", K(rowkey), K(ret));
        } else if (0 == i) {
          rowkey_column_cnt_ = macro_meta->rowkey_column_number_;
          column_type_array_ = full_meta.schema_->column_type_array_;
          exist_collation_free_ = false;
          if (need_build_collation_free) {
            for (int64_t i = 0; !exist_collation_free_ && i < rowkey_column_cnt_; i++) {
              if (column_type_array_[i].is_collation_free_compatible()) {
                exist_collation_free_ = true;
              }
            }
            if (exist_collation_free_) {
              collation_free_endkeys_.set_allocator(&allocator);
              collation_free_endkeys_.set_capacity(static_cast<uint32_t>(macro_block_count));
            }
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(endkeys_.push_back(rowkey))) {
          STORAGE_LOG(WARN, "Failed to push back macroblock endkey", K(rowkey), K(ret));
        } else if (exist_collation_free_) {
          if (OB_ISNULL(macro_meta->collation_free_endkey_)) {
            // defend code to check null collation free endkey
            for (int64_t i = 0; i < rowkey_column_cnt_; i++) {
              // original varchar or char should be null now
              if (rowkey.get_obj_ptr()[i].is_collation_free_compatible()) {
                ret = OB_ERR_UNEXPECTED;
                STORAGE_LOG(WARN, "Unexpected null collation freekey with valid rowkey", K(i), K(*macro_meta), K(ret));
              }
            }
          } else {
            rowkey.assign(macro_meta->collation_free_endkey_, macro_meta->rowkey_column_number_);
          }
          if (OB_SUCC(ret)) {
            if (OB_FAIL(collation_free_endkeys_.push_back(rowkey))) {
              STORAGE_LOG(WARN, "Failed to push back macroblock endkey", K(rowkey), K(ret));
            }
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (endkeys_.count() != macro_block_count ||
          (exist_collation_free_ && collation_free_endkeys_.count() != macro_block_count)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN,
            "Unexpected macro block endkeys count",
            K(macro_block_count),
            K_(exist_collation_free),
            K(endkeys_.count()),
            K(collation_free_endkeys_.count()),
            K(ret));
      }
    }
  }

  return ret;
}

template <typename T>
int ObSSTableRowkeyHelper::make_rowkey_cmp_funcs(ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(column_type_array_) || rowkey_column_cnt_ <= 0) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN,
        "Unexpected null column type array or rowkey column cnt",
        KP_(column_type_array),
        K_(rowkey_column_cnt),
        K(ret));
  } else {
    void* buf = NULL;
    T* cmp_funcs = NULL;
    cmp_funcs_.set_allocator(&allocator);
    cmp_funcs_.set_capacity(rowkey_column_cnt_);
    if (OB_ISNULL(buf = allocator.alloc(sizeof(T) * rowkey_column_cnt_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "Failed to allocate memory for cmopare func", K_(rowkey_column_cnt), K(ret));
    } else {
      cmp_funcs = reinterpret_cast<T*>(new (buf) T[rowkey_column_cnt_]);
      for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_column_cnt_; i++) {
        if (OB_FAIL(cmp_funcs[i].init_compare_func(column_type_array_[i]))) {
          STORAGE_LOG(WARN, "Failed to init compare func", K(i), K(ret));
        } else if (OB_FAIL(cmp_funcs_.push_back(&cmp_funcs[i]))) {
          STORAGE_LOG(WARN, "Failed to push back compare func", K(i), K(ret));
        }
      }
    }
    if (OB_SUCC(ret) && exist_collation_free_) {
      collation_free_cmp_funcs_.set_allocator(&allocator);
      collation_free_cmp_funcs_.set_capacity(rowkey_column_cnt_);
      if (OB_ISNULL(buf = allocator.alloc(sizeof(T) * rowkey_column_cnt_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "Failed to allocate memory for cmopare func", K_(rowkey_column_cnt), K(ret));
      } else {
        cmp_funcs = reinterpret_cast<T*>(new (buf) T[rowkey_column_cnt_]);
        for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_column_cnt_; i++) {
          if (column_type_array_[i].is_collation_free_compatible()) {
            if (OB_FAIL(cmp_funcs[i].init_compare_func_collation_free(column_type_array_[i]))) {
              STORAGE_LOG(WARN, "Failed to init compare func", K(i), K(ret));
            }
          } else if (OB_FAIL(cmp_funcs[i].init_compare_func(column_type_array_[i]))) {
            STORAGE_LOG(WARN, "Failed to init compare func", K(i), K(ret));
          }
          if (OB_SUCC(ret) && OB_FAIL(collation_free_cmp_funcs_.push_back(&cmp_funcs[i]))) {
            STORAGE_LOG(WARN, "Failed to push back compare func", K(i), K(ret));
          }
        }
      }
    }
  }

  return ret;
}

int ObSSTableRowkeyHelper::init(const ObIArray<MacroBlockId>& macro_ids, const ObSSTableMeta& sstable_meta,
    const bool need_build_collation_free, ObSSTable* sstable, ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;

  reset();
  if (OB_UNLIKELY(!sstable_meta.is_valid()) || OB_ISNULL(sstable)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to init sstable rowkey helper", K(sstable_meta), KP(sstable), K(ret));
  } else if (macro_ids.count() > 0) {
    sstable_ = sstable;
    ObWorker::CompatMode mode;
    if (OB_FAIL(ObCompatModeGetter::get_tenant_mode(extract_tenant_id(sstable_meta.index_id_), mode))) {
      STORAGE_LOG(WARN, "Failed to get compat mode", K(sstable_meta), K(ret));
      // rewrite ret for caller deal with
      ret = OB_TENANT_NOT_EXIST;
    } else {
      is_oracle_mode_ = (mode == ObWorker::CompatMode::ORACLE) && !is_sys_table(sstable_meta.index_id_);
      use_cmp_nullsafe_ = ObTableSchema::is_index_table(static_cast<ObTableType>(sstable_meta.table_type_)) ||
                          TPKM_NEW_NO_PK == sstable_meta.table_mode_.pk_mode_;
      if (OB_FAIL(build_macro_endkeys(
              macro_ids, allocator, sstable_meta.rowkey_column_count_, need_build_collation_free))) {
        STORAGE_LOG(WARN, "Failed to build macro endkeys", K(ret));
      } else if (OB_FAIL(build_rowkey_cmp_funcs(allocator))) {
        STORAGE_LOG(WARN, "Failed to build endkey cmp funcs", K(ret));
      } else {
        is_inited_ = true;
      }
    }
  }

  return ret;
}

int ObSSTableRowkeyHelper::build_rowkey_cmp_funcs(ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(column_type_array_) || rowkey_column_cnt_ <= 0) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN,
        "Unexpected null column type array or rowkey column cnt",
        KP_(column_type_array),
        K_(rowkey_column_cnt),
        K(ret));
  } else if (is_oracle_mode_ && use_cmp_nullsafe_) {
    ret = make_rowkey_cmp_funcs<ObRowkeyObjComparerNullsafeOracle>(allocator);
  } else if (is_oracle_mode_) {
    ret = make_rowkey_cmp_funcs<ObRowkeyObjComparerOracle>(allocator);
  } else if (use_cmp_nullsafe_) {
    ret = make_rowkey_cmp_funcs<ObRowkeyObjComparerNullsafeMysql>(allocator);
  } else {
    ret = make_rowkey_cmp_funcs<ObRowkeyObjComparer>(allocator);
  }

  return ret;
}

int ObSSTableRowkeyHelper::locate_block_idx_(const ObExtStoreRowkey& ext_rowkey, const int64_t cmp_rowkey_col_cnt,
    const bool use_lower_bound, const int64_t last_block_idx, int64_t& block_idx)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(cmp_rowkey_col_cnt > ext_rowkey.get_store_rowkey().get_obj_cnt())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to locate block index", K(ext_rowkey), K(cmp_rowkey_col_cnt), K(ret));
  } else {
    RowkeyArray* rowkey_array = NULL;
    const ObStoreRowkey* cmp_rowkey = NULL;
    RowkeyCmpFuncArray* cmp_funcs = NULL;
    block_idx = -1;
    if (!exist_collation_free_) {
      cmp_rowkey = &(ext_rowkey.get_store_rowkey());
      rowkey_array = &endkeys_;
      cmp_funcs = &cmp_funcs_;
    } else {
      bool use_collation_free = false;
      if (OB_FAIL(ext_rowkey.check_use_collation_free(!exist_collation_free_, use_collation_free))) {
        STORAGE_LOG(WARN, "Fail to check use collation free, ", K(ret), K(ext_rowkey));
      } else if (use_collation_free && collation_free_cmp_funcs_.count() > 0) {
        cmp_rowkey = &(ext_rowkey.get_collation_free_store_rowkey());
        rowkey_array = &collation_free_endkeys_;
        cmp_funcs = &collation_free_cmp_funcs_;
      } else {
        cmp_rowkey = &(ext_rowkey.get_store_rowkey());
        rowkey_array = &endkeys_;
        cmp_funcs = &cmp_funcs_;
        use_collation_free = false;
      }
    }
    if (OB_SUCC(ret)) {
      if (cmp_rowkey->get_obj_cnt() == rowkey_column_cnt_ && last_block_idx >= 0 &&
          last_block_idx < rowkey_array->count()) {
        // the rowkey may be in the same macro block, so check last macro block first
        if (compare_nullsafe(rowkey_array->at(last_block_idx), *cmp_rowkey, *cmp_funcs) >= 0) {
          if (0 == last_block_idx ||
              compare_nullsafe(rowkey_array->at(last_block_idx - 1), *cmp_rowkey, *cmp_funcs) < 0) {
            block_idx = last_block_idx;
          }
        }
      }
      if (-1 == block_idx) {  // binary search
        RowkeyComparor comparor(*cmp_funcs, cmp_rowkey_col_cnt < rowkey_column_cnt_);
        ObStoreRowkey refine_cmp_rowkey((const_cast<ObStoreRowkey*>(cmp_rowkey))->get_obj_ptr(), cmp_rowkey_col_cnt);
        RowkeyArray::iterator begin = rowkey_array->begin();
        RowkeyArray::iterator end = rowkey_array->end();
        RowkeyArray::iterator iter;
        if (use_lower_bound) {
          iter = std::lower_bound(begin, end, &refine_cmp_rowkey, comparor);
        } else {
          iter = std::upper_bound(begin, end, &refine_cmp_rowkey, comparor);
        }
        if (iter < end) {
          block_idx = iter - begin;
        }
      }
    }
  }

  return ret;
}

int ObSSTableRowkeyHelper::locate_block_idx_index(
    const ObExtStoreRowkey& ext_rowkey, const int64_t last_block_idx, int64_t& block_idx)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObSSTableRowkeyHelper is not inited", K(ret));
  } else if (OB_UNLIKELY(!ext_rowkey.is_range_cutoffed())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "ExtRowkey is not range cutted", K(ext_rowkey), K(ret));
  } else if (OB_UNLIKELY(ext_rowkey.get_range_cut_pos() == 0)) {
    block_idx = ext_rowkey.is_range_check_min() ? 0 : -1;
  } else {
    ret = locate_block_idx_(
        ext_rowkey, ext_rowkey.get_range_cut_pos(), ext_rowkey.is_range_check_min(), last_block_idx, block_idx);
  }

  return ret;
}

int ObSSTableRowkeyHelper::locate_block_idx_table(
    const ObExtStoreRowkey& ext_rowkey, const int64_t last_block_idx, int64_t& block_idx)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObSSTableRowkeyHelper is not inited", K(ret));
  } else if (OB_UNLIKELY(!ext_rowkey.is_range_cutoffed())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "ExtRowkey is not range cutted", K(ext_rowkey), K(ret));
  } else if (OB_UNLIKELY(ext_rowkey.get_range_cut_pos() == 0)) {
    block_idx = ext_rowkey.is_range_check_min() ? 0 : -1;
  } else if (OB_UNLIKELY(ext_rowkey.get_first_null_pos() == 0)) {
    block_idx = is_oracle_mode_ ? -1 : 0;
  } else {
    int64_t cmp_rowkey_col_cnt = ext_rowkey.get_range_cut_pos();
    bool use_lower_bound = ext_rowkey.is_range_check_min();
    if (ext_rowkey.get_first_null_pos() > 0 && ext_rowkey.get_first_null_pos() < cmp_rowkey_col_cnt) {
      cmp_rowkey_col_cnt = ext_rowkey.get_first_null_pos();
      use_lower_bound = !is_oracle_mode_;
    }
    ret = locate_block_idx_(ext_rowkey, cmp_rowkey_col_cnt, use_lower_bound, last_block_idx, block_idx);
  }

  return ret;
}

}  // end namespace storage
}  // end namespace oceanbase
