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

#ifndef OCEANBASE_STORAGE_OB_SSTABLE_ROWKEY_HELPER_H_
#define OCEANBASE_STORAGE_OB_SSTABLE_ROWKEY_HELPER_H_

#include "lib/container/ob_array.h"
#include "lib/container/ob_iarray.h"
#include "common/rowkey/ob_rowkey.h"
#include "common/object/ob_obj_compare.h"
#include "common/ob_range.h"
#include "blocksstable/ob_macro_block_meta_mgr.h"
#include "blocksstable/ob_store_file_system.h"
#include "lib/number/ob_number_v2.h"

namespace oceanbase {
namespace storage {

class ObRowkeyObjComparer {
public:
  enum ObjComparerType {
    COMPARER_MYSQL = 0,
    COMPARER_ORACLE = 1,
    COMPARER_MYSQL_NULLSAFE = 2,
    COMPARER_ORACLE_NULLSAFE = 3,
    COMPARER_MAX
  };
  typedef int (*rowkey_obj_comp_func)(
      const common::ObObj& obj1, const common::ObObj& obj2, const common::ObCompareCtx& cmp_ctx);
  static OB_INLINE bool is_smallint_number(const common::ObObj& obj)
  {
    return obj.nmb_desc_.len_ == 1 && 1 == obj.nmb_desc_.exp_ &&
           common::number::ObNumber::POSITIVE == obj.nmb_desc_.sign_;
  }
  static int sstable_number_cmp_func(
      const common::ObObj& obj1, const common::ObObj& obj2, const common::ObCompareCtx& cmp_ctx);
  static int sstable_collation_free_cmp_func(
      const common::ObObj& obj1, const common::ObObj& obj2, const common::ObCompareCtx& cmp_ctx);
  static int sstable_oracle_collation_free_cmp_func(
      const common::ObObj& obj1, const common::ObObj& obj2, const common::ObCompareCtx& cmp_ctx);

public:
  ObRowkeyObjComparer();
  virtual ~ObRowkeyObjComparer() = default;
  virtual void reset();
  virtual int init_compare_func(const common::ObObjMeta& obj_meta);
  virtual int init_compare_func_collation_free(const common::ObObjMeta& obj_meta);
  virtual OB_INLINE int32_t compare(const common::ObObj& obj1, const common::ObObj& obj2)
  {
    return cmp_func_(obj1, obj2, cmp_ctx_);
  }
  virtual OB_INLINE int32_t compare_semi_nullsafe(const common::ObObj& obj1, const common::ObObj& obj2)
  {
    int32_t cmp_ret = ObObjCmpFuncs::CR_EQ;
    if (obj2.is_null()) {
      cmp_ret = ObObjCmpFuncs::CR_GT;
    } else {
      cmp_ret = cmp_func_(obj1, obj2, cmp_ctx_);
    }
    return cmp_ret;
  }
  virtual OB_INLINE int32_t compare_semi_safe(const common::ObObj& obj1, const common::ObObj& obj2)
  {
    int32_t cmp_ret = ObObjCmpFuncs::CR_EQ;
    if (OB_UNLIKELY(obj2.is_ext())) {
      if (obj2.is_max_value()) {
        cmp_ret = ObObjCmpFuncs::CR_LT;
      } else if (obj2.is_min_value()) {
        cmp_ret = ObObjCmpFuncs::CR_GT;
      } else {
        STORAGE_LOG(ERROR, "Unexpected compare rowkey obj", K(obj2), K(obj1), K(*this));
        right_to_die_or_duty_to_live();
      }
    } else {
      cmp_ret = compare_semi_nullsafe(obj1, obj2);
    }
    return cmp_ret;
  }
  TO_STRING_KV(K_(type), K_(is_collation_free), K_(cmp_ctx));

protected:
  rowkey_obj_comp_func cmp_func_;
  common::ObCompareCtx cmp_ctx_;
  bool is_collation_free_;
  ObjComparerType type_;
};

class ObRowkeyObjComparerOracle : public ObRowkeyObjComparer {
public:
  ObRowkeyObjComparerOracle()
  {
    type_ = COMPARER_ORACLE;
    cmp_ctx_.null_pos_ = NULL_LAST;
  }
  virtual ~ObRowkeyObjComparerOracle() = default;
  virtual int init_compare_func_collation_free(const common::ObObjMeta& obj_meta) override;
  virtual OB_INLINE int32_t compare_semi_nullsafe(const common::ObObj& obj1, const common::ObObj& obj2) override
  {
    int32_t cmp_ret = ObObjCmpFuncs::CR_EQ;
    if (obj2.is_null()) {
      cmp_ret = ObObjCmpFuncs::CR_LT;
    } else {
      cmp_ret = cmp_func_(obj1, obj2, cmp_ctx_);
    }
    return cmp_ret;
  }
};

class ObRowkeyObjComparerNullsafeMysql : public ObRowkeyObjComparer {
public:
  ObRowkeyObjComparerNullsafeMysql()
  {
    type_ = COMPARER_MYSQL_NULLSAFE;
    cmp_ctx_.is_null_safe_ = true;
    cmp_ctx_.null_pos_ = NULL_FIRST;
  }
  virtual ~ObRowkeyObjComparerNullsafeMysql() = default;
  virtual OB_INLINE int32_t compare(const common::ObObj& obj1, const common::ObObj& obj2) override
  {
    int32_t cmp_ret = 0;
    if (OB_UNLIKELY(obj1.is_null() || obj2.is_null())) {
      if (obj1.is_null() && obj2.is_null()) {
        cmp_ret = ObObjCmpFuncs::CR_EQ;
      } else if (obj1.is_null()) {
        cmp_ret = ObObjCmpFuncs::CR_LT;
      } else {
        cmp_ret = ObObjCmpFuncs::CR_GT;
      }
    } else {
      cmp_ret = cmp_func_(obj1, obj2, cmp_ctx_);
    }
    return cmp_ret;
  }
  virtual OB_INLINE int32_t compare_semi_nullsafe(const common::ObObj& obj1, const common::ObObj& obj2) override
  {
    return compare(obj1, obj2);
  }
};

class ObRowkeyObjComparerNullsafeOracle : public ObRowkeyObjComparerOracle {
public:
  ObRowkeyObjComparerNullsafeOracle()
  {
    type_ = COMPARER_ORACLE_NULLSAFE;
    cmp_ctx_.is_null_safe_ = true;
    cmp_ctx_.null_pos_ = NULL_LAST;
  }
  virtual ~ObRowkeyObjComparerNullsafeOracle() = default;
  virtual OB_INLINE int32_t compare(const common::ObObj& obj1, const common::ObObj& obj2) override
  {
    int32_t cmp_ret = 0;
    if (OB_UNLIKELY(obj1.is_null() || obj2.is_null())) {
      if (obj1.is_null() && obj2.is_null()) {
        cmp_ret = ObObjCmpFuncs::CR_EQ;
      } else if (obj1.is_null()) {
        cmp_ret = ObObjCmpFuncs::CR_GT;
      } else {
        cmp_ret = ObObjCmpFuncs::CR_LT;
      }
    } else {
      cmp_ret = cmp_func_(obj1, obj2, cmp_ctx_);
    }
    return cmp_ret;
  }
  virtual OB_INLINE int32_t compare_semi_nullsafe(const common::ObObj& obj1, const common::ObObj& obj2) override
  {
    return compare(obj1, obj2);
  }
};

class ObSSTableRowkeyHelper {
public:
  typedef common::ObFixedArray<common::ObStoreRowkey, common::ObIAllocator> RowkeyArray;
  typedef common::ObFixedArray<ObRowkeyObjComparer*, common::ObIAllocator> RowkeyCmpFuncArray;
  ObSSTableRowkeyHelper();
  virtual ~ObSSTableRowkeyHelper() = default;
  virtual void reset();
  OB_INLINE bool is_valid() const
  {
    return is_inited_ && cmp_funcs_.count() == rowkey_column_cnt_;
  }
  int init(const common::ObIArray<blocksstable::MacroBlockId>& macro_ids,
      const blocksstable::ObSSTableMeta& sstable_meta, const bool need_build_collation_free, ObSSTable* sstable,
      common::ObIAllocator& allocator);
  OB_INLINE int locate_block_idx(
      const common::ObExtStoreRowkey& ext_rowkey, const int64_t last_block_idx, int64_t& block_idx)
  {
    return use_cmp_nullsafe_ ? locate_block_idx_index(ext_rowkey, last_block_idx, block_idx)
                             : locate_block_idx_table(ext_rowkey, last_block_idx, block_idx);
  }

  // for index table and not new-pk table, obj1 and obj2 can be NULL
  // for other situation obj2 can be MIN | MAX | NULL, obj1 can't
  int compare_rowkey_obj(const int64_t idx, const ObObj& obj1, const ObObj& obj2, int32_t& cmp_ret) const
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(ObObjCmpFuncs::CR_OB_ERROR == (cmp_ret = cmp_funcs_.at(idx)->compare_semi_safe(obj1, obj2)))) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "failed to compare obj1 and obj2", K(ret), K(idx), K(obj1), K(obj2));
    }
    return ret;
  }
  OB_INLINE int32_t compare(const common::ObStoreRowkey& rowkey, const common::ObStoreRowkey& cmp_rowkey)
  {
    return compare(rowkey, cmp_rowkey, cmp_funcs_);
  }
  OB_INLINE ObIArray<common::ObStoreRowkey>& get_endkeys()
  {
    return endkeys_;
  }
  OB_INLINE ObIArray<common::ObStoreRowkey>& get_collation_free_endkeys()
  {
    return collation_free_endkeys_;
  }
  OB_INLINE ObIArray<ObRowkeyObjComparer*>& get_compare_funcs()
  {
    return cmp_funcs_;
  }
  OB_INLINE ObIArray<ObRowkeyObjComparer*>& get_collation_free_compare_funcs()
  {
    return collation_free_cmp_funcs_;
  }
  OB_INLINE int64_t get_rowkey_column_num() const
  {
    return rowkey_column_cnt_;
  }
  OB_INLINE bool is_oracle_mode() const
  {
    return is_oracle_mode_;
  }
  TO_STRING_KV(K_(endkeys), K_(collation_free_endkeys), K_(rowkey_column_cnt), KP_(column_type_array),
      K_(exist_collation_free), K_(use_cmp_nullsafe), K_(is_oracle_mode), K_(is_inited));
  static OB_INLINE int32_t compare(const common::ObStoreRowkey& rowkey, const common::ObStoreRowkey& cmp_rowkey,
      RowkeyCmpFuncArray& cmp_funcs, const bool with_prefix = false)
  {
    int32_t cmp_ret = 0;
    const ObObj* obj1 = rowkey.get_obj_ptr();
    const ObObj* obj2 = cmp_rowkey.get_obj_ptr();
    int64_t cmp_cnt = MIN(rowkey.get_obj_cnt(), cmp_rowkey.get_obj_cnt());

    for (int64_t i = 0; i < cmp_cnt && 0 == cmp_ret; i++) {
      if (OB_UNLIKELY(ObObjCmpFuncs::CR_OB_ERROR == (cmp_ret = cmp_funcs.at(i)->compare(obj1[i], obj2[i])))) {
        STORAGE_LOG(ERROR,
            "failed to compare obj1 and obj2 using collation free",
            K(rowkey),
            K(cmp_rowkey),
            K(obj1[i]),
            K(obj2[i]));
        right_to_die_or_duty_to_live();
      }
    }
    if (0 == cmp_ret && !with_prefix) {
      cmp_ret = static_cast<int32_t>(rowkey.get_obj_cnt() - cmp_rowkey.get_obj_cnt());
    }
    return cmp_ret;
  }
  OB_INLINE int32_t compare_nullsafe(
      const common::ObStoreRowkey& rowkey, const common::ObStoreRowkey& cmp_rowkey, RowkeyCmpFuncArray& cmp_funcs)
  {
    int32_t cmp_ret = 0;
    const ObObj* obj1 = rowkey.get_obj_ptr();
    const ObObj* obj2 = cmp_rowkey.get_obj_ptr();
    int64_t cmp_cnt = MIN(rowkey.get_obj_cnt(), cmp_rowkey.get_obj_cnt());

    for (int64_t i = 0; i < cmp_cnt && 0 == cmp_ret; i++) {
      if (OB_UNLIKELY(ObObjCmpFuncs::CR_OB_ERROR == (cmp_ret = cmp_funcs.at(i)->compare_semi_safe(obj1[i], obj2[i])))) {
        STORAGE_LOG(ERROR,
            "failed to compare obj1 and obj2 using collation free",
            K(rowkey),
            K(cmp_rowkey),
            K(obj1[i]),
            K(obj2[i]));
        right_to_die_or_duty_to_live();
      }
    }
    if (0 == cmp_ret) {
      cmp_ret = static_cast<int32_t>(rowkey.get_obj_cnt() - cmp_rowkey.get_obj_cnt());
    }
    return cmp_ret;
  }

public:
  class RowkeyComparor {
  public:
    RowkeyComparor() = delete;
    RowkeyComparor(RowkeyCmpFuncArray& cmp_funcs, const bool prefix_check)
        : cmp_funcs_(cmp_funcs), prefix_check_(prefix_check)
    {}
    virtual ~RowkeyComparor() = default;
    OB_INLINE bool operator()(const common::ObStoreRowkey& rowkey, const common::ObStoreRowkey* cmp_rowkey) const
    {
      return compare(rowkey, *cmp_rowkey, cmp_funcs_, prefix_check_) < 0;
    }
    OB_INLINE bool operator()(const common::ObStoreRowkey* cmp_rowkey, const common::ObStoreRowkey& rowkey) const
    {
      return compare(rowkey, *cmp_rowkey, cmp_funcs_, prefix_check_) > 0;
    }
    RowkeyCmpFuncArray& cmp_funcs_;
    const bool prefix_check_;
  };

private:
  int get_macro_block_meta(const blocksstable::MacroBlockId& macro_id, blocksstable::ObFullMacroBlockMeta& macro_meta,
      const int64_t schema_rowkey_column_cnt);
  int build_macro_endkeys(const common::ObIArray<blocksstable::MacroBlockId>& macro_ids, ObIAllocator& allocator,
      const int64_t schema_rowkey_column_cnt, const bool need_build_collation_free);
  int build_rowkey_cmp_funcs(common::ObIAllocator& allocator);
  template <typename T>
  int make_rowkey_cmp_funcs(ObIAllocator& allocator);
  int locate_block_idx_table(
      const common::ObExtStoreRowkey& ext_rowkey, const int64_t last_block_idx, int64_t& block_idx);
  int locate_block_idx_index(
      const common::ObExtStoreRowkey& ext_rowkey, const int64_t last_block_idx, int64_t& block_idx);
  int locate_block_idx_(const ObExtStoreRowkey& ext_rowkey, const int64_t cmp_rowkey_col_cnt,
      const bool use_lower_bound, const int64_t last_block_idx, int64_t& block_idx);

private:
  RowkeyArray endkeys_;
  RowkeyArray collation_free_endkeys_;
  RowkeyCmpFuncArray cmp_funcs_;
  RowkeyCmpFuncArray collation_free_cmp_funcs_;
  int64_t rowkey_column_cnt_;
  const common::ObObjMeta* column_type_array_;
  ObSSTable* sstable_;
  bool exist_collation_free_;
  bool use_cmp_nullsafe_;
  bool is_oracle_mode_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObSSTableRowkeyHelper);
};

}  // end namespace storage
}  // end namespace oceanbase

#endif  // OCEANBASE_STORAGE_OB_SSTABLE_ROWKEY_HELPER_H_
