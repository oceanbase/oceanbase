/**
 * Copyright (c) 2024 OceanBase
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
#include "ob_storage_datum.h"
#include "storage/ob_i_store.h"

namespace oceanbase
{
using namespace common;
namespace blocksstable
{
static int nonext_nonext_compare(const ObStorageDatum &left, const ObStorageDatum &right, const common::ObCmpFunc &cmp_func, int &cmp_ret)
{
  int ret = cmp_func.cmp_func_(left, right, cmp_ret);
  STORAGE_LOG(DEBUG, "chaser debug compare datum", K(ret), K(left), K(right), K(cmp_ret));
  return ret;
}

static int nonext_ext_compare(const ObStorageDatum &left, const ObStorageDatum &right, const common::ObCmpFunc &cmp_func, int &cmp_ret)
{
  int ret = OB_SUCCESS;
  UNUSEDx(left, cmp_func);
  if (right.is_max()) {
    cmp_ret = -1;
  } else if (right.is_min()) {
    cmp_ret = 1;
  } else {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "Unexpected datum in rowkey to compare", K(ret), K(right));
  }
  STORAGE_LOG(DEBUG, "chaser debug compare datum", K(ret), K(left), K(right), K(cmp_ret));
  return ret;
}

static int ext_nonext_compare(const ObStorageDatum &left, const ObStorageDatum &right, const common::ObCmpFunc &cmp_func, int &cmp_ret)
{
  int ret = OB_SUCCESS;
  UNUSEDx(right, cmp_func);
  if (left.is_max()) {
    cmp_ret = 1;
  } else if (left.is_min()) {
    cmp_ret = -1;
  } else {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "Unexpected datum in rowkey to compare", K(ret), K(left));
  }
  STORAGE_LOG(DEBUG, "chaser debug compare datum", K(ret), K(left), K(right), K(cmp_ret));
  return ret;
}

static int ext_ext_compare(const ObStorageDatum &left, const ObStorageDatum &right, const common::ObCmpFunc &cmp_func, int &cmp_ret)
{
  int ret = OB_SUCCESS;
  UNUSEDx(cmp_func);
  int64_t lv = left.is_max() - left.is_min();
  int64_t rv = right.is_max() - right.is_min();
  if (OB_UNLIKELY(0 == lv || 0 == rv)) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "Unexpected datum in rowkey to compare", K(ret), K(left), K(right));
  } else {
    cmp_ret = lv - rv;
  }
  STORAGE_LOG(DEBUG, "chaser debug compare datum", K(ret), K(left), K(right), K(cmp_ret));

  return ret;
}

typedef int (*ExtSafeCompareFunc)(const ObStorageDatum &left, const ObStorageDatum &right, const common::ObCmpFunc &cmp_func, int &cmp_ret);
static ExtSafeCompareFunc ext_safe_cmp_funcs[2][2] = {
  {nonext_nonext_compare, nonext_ext_compare},
  {ext_nonext_compare, ext_ext_compare}
};

int ObStorageDatumCmpFunc::compare(const ObStorageDatum &left, const ObStorageDatum &right, int &cmp_ret) const
{
  return ext_safe_cmp_funcs[left.is_ext()][right.is_ext()](left, right, cmp_func_, cmp_ret);
}

/*
 *ObStorageDatumUtils
 */
ObStorageDatumUtils::ObStorageDatumUtils()
  : rowkey_cnt_(0),
    cmp_funcs_(),
    hash_funcs_(),
    ext_hash_func_(),
    is_oracle_mode_(false),
    is_inited_(false)
{}

ObStorageDatumUtils::~ObStorageDatumUtils()
{}

int ObStorageDatumUtils::transform_multi_version_col_desc(const ObIArray<share::schema::ObColDesc> &col_descs,
                                                          const int64_t schema_rowkey_cnt,
                                                          ObIArray<share::schema::ObColDesc> &mv_col_descs)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(schema_rowkey_cnt > col_descs.count())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to transform mv col descs", K(ret), K(schema_rowkey_cnt), K(col_descs));
  } else {
    mv_col_descs.reuse();
    for (int64_t i = 0; OB_SUCC(ret) && i < schema_rowkey_cnt; i++) {
      if (OB_FAIL(mv_col_descs.push_back(col_descs.at(i)))) {
        STORAGE_LOG(WARN, "Failed to push back col desc", K(ret), K(i));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(storage::ObMultiVersionRowkeyHelpper::add_extra_rowkey_cols(mv_col_descs))) {
      STORAGE_LOG(WARN, "Fail to add extra_rowkey_cols", K(ret), K(schema_rowkey_cnt));
    } else {
      for (int64_t i = schema_rowkey_cnt; OB_SUCC(ret) && i < col_descs.count(); i++) {
        const share::schema::ObColDesc &col_desc = col_descs.at(i);
        if (col_desc.col_id_ == common::OB_HIDDEN_TRANS_VERSION_COLUMN_ID
            || col_desc.col_id_ == common::OB_HIDDEN_SQL_SEQUENCE_COLUMN_ID) {
          continue;
        } else if (OB_FAIL(mv_col_descs.push_back(col_desc))) {
          STORAGE_LOG(WARN, "Failed to push back col desc", K(ret), K(col_desc));
        }
      }
    }
  }

  return ret;
}

int ObStorageDatumUtils::init(const ObIArray<share::schema::ObColDesc> &col_descs,
                              const int64_t schema_rowkey_cnt,
                              const bool is_oracle_mode,
                              ObIAllocator &allocator,
                              const bool is_column_store)
{
  int ret = OB_SUCCESS;
  ObSEArray<share::schema::ObColDesc, 32> mv_col_descs;
  int64_t mv_rowkey_cnt = 0;
  int64_t mv_extra_rowkey_cnt = 0;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObStorageDatumUtils init twice", K(ret), K(*this));
  } else if (OB_UNLIKELY(schema_rowkey_cnt < 0 || schema_rowkey_cnt > OB_MAX_ROWKEY_COLUMN_NUMBER
                  || schema_rowkey_cnt > col_descs.count())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to init storage datum utils", K(ret), K(schema_rowkey_cnt), K(col_descs));
  } else if (OB_FAIL(transform_multi_version_col_desc(col_descs, schema_rowkey_cnt, mv_col_descs))) {
    STORAGE_LOG(WARN, "Failed to transform multi version col descs", K(ret));
  } else if (FALSE_IT(mv_extra_rowkey_cnt = is_column_store ? 0 : storage::ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt())) {
  } else if (FALSE_IT(mv_rowkey_cnt = schema_rowkey_cnt + mv_extra_rowkey_cnt)) {
  } else if (OB_FAIL(cmp_funcs_.init(mv_rowkey_cnt, allocator))) {
    STORAGE_LOG(WARN, "Failed to reserve cmp func array", K(ret));
  } else if (OB_FAIL(hash_funcs_.init(mv_rowkey_cnt, allocator))) {
    STORAGE_LOG(WARN, "Failed to reserve hash func array", K(ret));
  } else if (OB_FAIL(inner_init(mv_col_descs, mv_rowkey_cnt, is_oracle_mode))) {
    STORAGE_LOG(WARN, "Failed to inner init datum utils", K(ret), K(mv_col_descs), K(mv_rowkey_cnt));
  }

  return ret;
}

int ObStorageDatumUtils::init(const common::ObIArray<share::schema::ObColDesc> &col_descs,
                              const int64_t schema_rowkey_cnt,
                              const bool is_oracle_mode,
                              const int64_t arr_buf_len,
                              char *arr_buf)
{
  int ret = OB_SUCCESS;
  ObSEArray<share::schema::ObColDesc, 32> mv_col_descs;
  int64_t pos = 0;
  int64_t mv_rowkey_cnt = 0;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObStorageDatumUtils init twice", K(ret), K(*this));
  } else if (OB_UNLIKELY(schema_rowkey_cnt < 0 || schema_rowkey_cnt > OB_MAX_ROWKEY_COLUMN_NUMBER
                  || schema_rowkey_cnt > col_descs.count())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to init storage datum utils", K(ret), K(col_descs), K(schema_rowkey_cnt));
  } else if (OB_FAIL(transform_multi_version_col_desc(col_descs, schema_rowkey_cnt, mv_col_descs))) {
    STORAGE_LOG(WARN, "Failed to transform multi version col descs", K(ret));
  } else if (FALSE_IT(mv_rowkey_cnt = schema_rowkey_cnt + storage::ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt())) {
  } else if (OB_FAIL(cmp_funcs_.init(mv_rowkey_cnt, arr_buf_len, arr_buf, pos))) {
    STORAGE_LOG(WARN, "Failed to init compare function array", K(ret));
  } else if (OB_FAIL(hash_funcs_.init(mv_rowkey_cnt, arr_buf_len, arr_buf, pos))) {
    STORAGE_LOG(WARN, "Failed to init hash function array", K(ret));
  } else if (OB_FAIL(inner_init(mv_col_descs, mv_rowkey_cnt, is_oracle_mode))) {
    STORAGE_LOG(WARN, "Failed to inner init datum utils", K(ret), K(mv_col_descs), K(mv_rowkey_cnt));
  }
  return ret;
}

int ObStorageDatumUtils::inner_init(
    const common::ObIArray<share::schema::ObColDesc> &mv_col_descs,
    const int64_t mv_rowkey_col_cnt,
    const bool is_oracle_mode)
{
  int ret = OB_SUCCESS;
  is_oracle_mode_ = is_oracle_mode;
  // support column order index until next task done
  //
  // we could use the cmp funcs in the basic funcs directlly
  bool is_null_last = is_oracle_mode_;
  ObCmpFunc cmp_func;
  ObHashFunc hash_func;
  for (int64_t i = 0; OB_SUCC(ret) && i < mv_rowkey_col_cnt; i++) {
    const share::schema::ObColDesc &col_desc = mv_col_descs.at(i);
    //TODO @hanhui support desc rowkey
    bool is_ascending = true || col_desc.col_order_ == ObOrderType::ASC;
    bool has_lob_header = is_lob_storage(col_desc.col_type_.get_type());
    ObPrecision precision = PRECISION_UNKNOWN_YET;
    if (col_desc.col_type_.is_decimal_int()) {
      precision = col_desc.col_type_.get_stored_precision();
      OB_ASSERT(precision != PRECISION_UNKNOWN_YET);
    }
    sql::ObExprBasicFuncs *basic_funcs = ObDatumFuncs::get_basic_func(col_desc.col_type_.get_type(),
                                                                      col_desc.col_type_.get_collation_type(),
                                                                      col_desc.col_type_.get_scale(),
                                                                      is_oracle_mode,
                                                                      has_lob_header,
                                                                      precision);
    if (OB_UNLIKELY(nullptr == basic_funcs
                    || nullptr == basic_funcs->null_last_cmp_
                    || nullptr == basic_funcs->murmur_hash_v2_)) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(ERROR, "Unexpected null basic funcs", K(ret), K(col_desc));
    } else {
      cmp_func.cmp_func_ = is_null_last ? basic_funcs->null_last_cmp_ : basic_funcs->null_first_cmp_;
      hash_func.hash_func_ = basic_funcs->murmur_hash_v2_;
      if (OB_FAIL(hash_funcs_.push_back(hash_func))) {
        STORAGE_LOG(WARN, "Failed to push back hash func", K(ret), K(i), K(col_desc));
      } else if (is_ascending) {
        if (OB_FAIL(cmp_funcs_.push_back(ObStorageDatumCmpFunc(cmp_func)))) {
          STORAGE_LOG(WARN, "Failed to push back cmp func", K(ret), K(i), K(col_desc));
        }
      } else {
        ret = OB_ERR_SYS;
        STORAGE_LOG(WARN, "Unsupported desc column order", K(ret), K(col_desc), K(i));
      }
    }
  }
  if (OB_SUCC(ret)) {
    sql::ObExprBasicFuncs *basic_funcs = ObDatumFuncs::get_basic_func(ObExtendType, CS_TYPE_BINARY);
    if (OB_UNLIKELY(nullptr == basic_funcs || nullptr == basic_funcs->murmur_hash_v2_)) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(ERROR, "Unexpected null basic funcs for extend type", K(ret));
    } else {
      ext_hash_func_.hash_func_ = basic_funcs->murmur_hash_v2_;
      rowkey_cnt_ = mv_rowkey_col_cnt;
      is_inited_ = true;
    }
  }

  return ret;
}

int ObStorageDatumUtils::assign(const ObStorageDatumUtils &other_utils, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!other_utils.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to assign datum utils", K(ret), K(other_utils));
  } else {
    rowkey_cnt_ = other_utils.get_rowkey_count();
    is_oracle_mode_ = other_utils.is_oracle_mode();
    ext_hash_func_ = other_utils.get_ext_hash_funcs();
    if (OB_FAIL(cmp_funcs_.init_and_assign(other_utils.get_cmp_funcs(), allocator))) {
      STORAGE_LOG(WARN, "Failed to assign cmp func array", K(ret));
    } else if (OB_FAIL(hash_funcs_.init_and_assign(other_utils.get_hash_funcs(), allocator))) {
      STORAGE_LOG(WARN, "Failed to assign hash func array", K(ret));
    } else {
      is_inited_ = true;
    }
  }

  return ret;
}

void ObStorageDatumUtils::reset()
{
  rowkey_cnt_ = 0;
  cmp_funcs_.reset();
  hash_funcs_.reset();
  ext_hash_func_.hash_func_ = nullptr;
  is_inited_ = false;
}

int64_t ObStorageDatumUtils::get_deep_copy_size() const
{
  return cmp_funcs_.get_deep_copy_size() + hash_funcs_.get_deep_copy_size();
}

/*
 *ObStorageDatumBuffer
 */
ObStorageDatumBuffer::ObStorageDatumBuffer(common::ObIAllocator *allocator)
    : capacity_(LOCAL_BUFFER_ARRAY),
      local_datums_(),
      datums_(local_datums_),
      allocator_(allocator),
      is_inited_(nullptr != allocator)
{}

ObStorageDatumBuffer::~ObStorageDatumBuffer()
{
  if (datums_ != local_datums_ && nullptr != allocator_) {
    allocator_->free(datums_);
  }
}

void ObStorageDatumBuffer::reset()
{
  if (datums_ != local_datums_ && nullptr != allocator_) {
    allocator_->free(datums_);
  }
  allocator_ = nullptr;
  datums_ = local_datums_;
  capacity_ = LOCAL_BUFFER_ARRAY;
  for (int64_t i = 0; i < capacity_; i++) {
    datums_[i].reuse();
  }
  is_inited_ = false;
}

int ObStorageDatumBuffer::init(common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObStorageDatumBuffer init twice", K(ret), K(*this));
  } else {
    OB_ASSERT(datums_ == local_datums_);
    allocator_ = &allocator;
    is_inited_ = true;
  }

  return ret;
}

int ObStorageDatumBuffer::reserve(const int64_t count, const bool keep_data)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObStorageDatumBuffer is not inited", K(ret), K(*this));
  } else if (OB_UNLIKELY(count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to reserve datum buffer", K(ret), K(count));
  } else if (count <= capacity_){
  } else if (OB_ISNULL(buf = allocator_->alloc(sizeof(ObStorageDatum) * count))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "Failed to alloc memory", K(ret), K(count));
  } else {
    ObStorageDatum *new_datums = new (buf) ObStorageDatum [count];
    if (keep_data) {
      for (int64_t i = 0; i < capacity_; i++) {
        new_datums[i] = datums_[i];
      }
    }
    if (nullptr != datums_ && datums_ != local_datums_) {
      allocator_->free(datums_);
    }
    datums_ = new_datums;
    capacity_  = count;
  }

  return ret;
}

} // namespace blocksstable
} // namespace oceanbase
