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

#include "common/rowkey/ob_rowkey_info.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace std;

DEFINE_SERIALIZE(ObRowkeyColumn)
{
  int ret = OB_SUCCESS;
  int64_t tmp_pos = pos;
  if (NULL == buf || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments.", KP(buf), K(buf_len), K(ret));
  } else if (OB_FAIL(serialization::encode_vi64(buf, buf_len, tmp_pos, length_))) {
    COMMON_LOG(WARN, "encode length error.", K_(length), K(ret));
  } else if (OB_FAIL(serialization::encode_vi64(buf, buf_len, tmp_pos, column_id_))) {
    COMMON_LOG(WARN, "encode column_id error.", K_(column_id), K(ret));
  } else if (OB_FAIL(type_.serialize(buf, buf_len, tmp_pos))) {
    COMMON_LOG(WARN, "encode type error.", K_(type), K(ret));
  } else if (OB_FAIL(serialization::encode_vi32(buf, buf_len, tmp_pos, order_))) {
    COMMON_LOG(WARN, "encode order error.", K_(order), K(ret));
  } else {
    pos = tmp_pos;
  }
  return ret;
}

DEFINE_DESERIALIZE(ObRowkeyColumn)
{
  int ret = OB_SUCCESS;
  int64_t tmp_pos = pos;
  if (NULL == buf || data_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments.", KP(buf), K(data_len), K(ret));
  } else if (OB_FAIL(serialization::decode_vi64(
      buf, data_len, tmp_pos, &length_))) {
    COMMON_LOG(WARN, "decode length error.", K_(length), K(ret));
  } else if (OB_FAIL(serialization::decode_vi64(
      buf, data_len, tmp_pos, reinterpret_cast<int64_t *>(&column_id_)))) {
    COMMON_LOG(WARN, "decode column_id error.", K_(column_id), K(ret));
  } else if (OB_FAIL(type_.deserialize(buf, data_len, tmp_pos))) {
    COMMON_LOG(WARN, "decode type error.", K_(type), K(ret));
  } else if (OB_FAIL(serialization::decode_vi32(
      buf, data_len, tmp_pos, reinterpret_cast<int32_t *>(&order_)))) {
    COMMON_LOG(WARN, "decode order error.", K_(order), K(ret));
  } else {
    pos = tmp_pos;
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObRowkeyColumn)
{
  int64_t len = 0;
  len += serialization::encoded_length_vi64(length_);
  len += serialization::encoded_length_vi64(column_id_);
  len += type_.get_serialize_size();
  len += serialization::encoded_length_vi32(order_);
  return len;
}
bool ObRowkeyColumn::is_equal_except_column_id(const ObRowkeyColumn &other) const
{
   return length_ == other.length_ &&
          type_ == other.type_ &&
          order_ == other.order_ &&
          fulltext_flag_ == other.fulltext_flag_ &&
          spatial_flag_ == other.spatial_flag_;
}

ObRowkeyColumn& ObRowkeyColumn::operator=(const ObRowkeyColumn &other)
{
  this->length_ = other.length_;
  this->column_id_ = other.column_id_;
  this->type_ = other.type_;
  this->order_ = other.order_;
  this->fulltext_flag_ = other.fulltext_flag_;
  this->spatial_flag_ = other.spatial_flag_;
  this->multivalue_flag_ = other.multivalue_flag_;
  return *this;
}

bool ObRowkeyColumn::operator==(const ObRowkeyColumn &other) const
{
  return
      this->column_id_ == other.column_id_ &&
      this->type_ == other.type_ &&
      this->order_ == other.order_ &&
      this->fulltext_flag_ == other.fulltext_flag_ &&
      this->spatial_flag_ == other.spatial_flag_ &&
      this->multivalue_flag_ == other.multivalue_flag_;
}

ObRowkeyInfo::ObRowkeyInfo()
    : columns_(NULL), size_(0), capacity_(0), arena_(ObModIds::OB_SCHEMA_ROW_KEY), allocator_(&arena_)
{
}

ObRowkeyInfo::ObRowkeyInfo(ObIAllocator *allocator)
    : columns_(NULL), size_(0), capacity_(0), arena_(ObModIds::OB_SCHEMA_ROW_KEY), allocator_(allocator)
{
}

ObRowkeyInfo::~ObRowkeyInfo()
{
}

int ObRowkeyInfo::get_column(const int64_t index, ObRowkeyColumn &column) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(index < 0 || index >= size_)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid argument.", K(index), K_(size), K(ret));
  } else if (!is_valid()) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "columns has not initialized.",
               KP_(columns), K_(size), K(index), K(ret));
  } else {
    column = columns_[index];
  }
  return ret;
}

// returns NULL if error happens.
const ObRowkeyColumn *ObRowkeyInfo::get_column(const int64_t index) const
{
  const ObRowkeyColumn *ret = NULL;
  int tmp_ret = OB_SUCCESS;
  if (OB_UNLIKELY(index < 0 || index >= size_)) {
    tmp_ret = OB_INVALID_ARGUMENT;
    COMMON_LOG_RET(WARN, tmp_ret, "Invalid argument.", K(index), K_(size), K(tmp_ret));
  } else if (!is_valid()) {
    tmp_ret = OB_INVALID_DATA;
    COMMON_LOG_RET(WARN, tmp_ret, "columns has not initialized.",
               KP_(columns), K_(size), K(index), K(tmp_ret));
  } else {
    ret = &columns_[index];
  }
  return ret;
}

bool ObRowkeyInfo::contain_timestamp_ltz_column() const
{
  bool bret = false;
  for (int64_t i  = 0; i < size_ && !bret; ++i) {
    bret = columns_[i].get_meta_type().is_timestamp_ltz();
  }
  return bret;
}

int ObRowkeyInfo::get_column_id(const int64_t index, uint64_t &column_id) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(index < 0 || index >= size_)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid argument.", K(index), K_(size), K(ret));
  } else if (!is_valid()) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "columns has not initialized.",
               KP_(columns), K_(size), K(index), K(ret));
  } else {
    column_id = columns_[index].column_id_;
  }
  return ret;
}


int ObRowkeyInfo::get_index(const uint64_t column_id, int64_t &index, ObRowkeyColumn &column) const
{
  int ret = OB_SUCCESS;
  index = -1;
  int64_t i = 0;
  if (OB_UNLIKELY(OB_INVALID_ID == column_id)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid argument.", K(column_id), K(ret));
  } else if (!is_valid()) {
    ret = OB_ENTRY_NOT_EXIST;
    COMMON_LOG(INFO, "table has no rowkeys.",
               KP_(columns), K_(size), K(index), K(column_id), K(ret));
  }
  for (; OB_SUCCESS == ret && i < size_; ++i) {
    if (columns_[i].column_id_ == column_id) {
      index = i;
      column = columns_[i];
      break;
    }
  }
  if (OB_SUCC(ret) && i == size_) {
    ret = OB_ENTRY_NOT_EXIST;
  }
  return ret;
}

int ObRowkeyInfo::get_index(const uint64_t column_id, int64_t &index) const
{
  int ret = OB_SUCCESS;
  int64_t i = 0;
  index = -1;
  if (OB_UNLIKELY(OB_INVALID_ID == column_id)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid argument.", K(column_id), K(ret));
  } else if (!is_valid()) {
    ret = OB_ENTRY_NOT_EXIST;
    COMMON_LOG(INFO, "table has no rowkeys.",
                   KP_(columns), K_(size), K(index), K(column_id), K(ret));
  }
  for (; OB_SUCCESS == ret && i < size_; ++i) {
    if (columns_[i].column_id_ == column_id) {
      index = i;
      break;
    }
  }
  if (OB_SUCC(ret) && i == size_) {
    ret = OB_ENTRY_NOT_EXIST;
  }
  return ret;
}

int ObRowkeyInfo::is_rowkey_column(const uint64_t column_id, bool &is_rowkey) const
{
  int ret = OB_SUCCESS;
  int64_t index = -1;
  is_rowkey = false;
  if (OB_UNLIKELY(OB_INVALID_ID == column_id)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid argument.", K(column_id), K(ret));
  } else if (!is_valid()) {
    // some table has no rowkey;
    is_rowkey = false;
  } else if (OB_FAIL(get_index(column_id, index))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      is_rowkey = false;
      ret = OB_SUCCESS;
    } else {
      COMMON_LOG(WARN, "get index of column failed.", K(column_id), K(ret));
    }
  } else if (index >= 0) {
    is_rowkey = true;
  }
  return ret;
}

int ObRowkeyInfo::add_column(const ObRowkeyColumn &column)
{
  int ret = OB_SUCCESS;
  if (!column.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments.", K(column), K(ret));
  } else if (NULL == columns_) {
    if (OB_FAIL(expand(DEFAULT_ROWKEY_COLUMN_ARRAY_CAPACITY))) {
      COMMON_LOG(WARN, "Fail to allocate memory.", K(ret));
    }
  } else if (size_ >= capacity_) {
    if (OB_FAIL(expand(capacity_ * 2))) {
      COMMON_LOG(WARN, "Fail to allocate memory.", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (NULL == columns_) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "columns_ cannot be NULL.", K(ret));
    } else {
      columns_[size_] = column;
      ++size_;
    }
  }
  return ret;
}

int ObRowkeyInfo::set_column(const int64_t idx, const ObRowkeyColumn &column)
{
  int ret = OB_SUCCESS;
  if (idx < 0 || !column.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument.", K(idx), K(column), K(ret));
  } else {
    if (idx >= capacity_) {
      if (OB_FAIL(expand(idx + 1))) {
        COMMON_LOG(WARN, "Fail to expand rowkey info.", K(idx), K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      columns_[idx] = column;
      if (size_ <= idx) {
        size_ = 1 + idx;
      }
    }
  }
  return ret;
}

void ObRowkeyInfo::reset()
{
  columns_ = NULL;
  size_ = 0;
  capacity_ = 0;
  arena_.reset();
}

int ObRowkeyInfo::reserve(const int64_t capacity)
{
  int ret = OB_SUCCESS;

  if (NULL != columns_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The columns has been allocated.", K(ret));
  } else if (capacity < 0) {
    COMMON_LOG(WARN, "invalid arguments.", K(capacity), K(ret));
  } else if (OB_FAIL(expand(capacity))) {
    COMMON_LOG(WARN, "fail to expand memory.", K(capacity), K(ret));
  }

  return ret;
}


bool ObRowkeyInfo::is_valid() const
{
  return NULL != columns_ && size_ > 0 && size_ <= capacity_;
}


int64_t ObRowkeyInfo::get_convert_size() const
{
  return sizeof(*this) + sizeof(ObRowkeyColumn) * size_;
}

int ObRowkeyInfo::expand(const int64_t size)
{
  int ret = OB_SUCCESS;
  if (size < 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid expand size.", K(size), K(ret));
  } else if (OB_ISNULL(allocator_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "allocator is NULL.", K(ret));
  } else if (size > capacity_) {
    COMMON_LOG(DEBUG, "Expand rowkey info array.", "old_size", capacity_, "new_size", size);
    ObRowkeyColumn *tmp = static_cast<ObRowkeyColumn*>(allocator_->alloc(sizeof(ObRowkeyColumn)
        * size));
    if (NULL == tmp) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      COMMON_LOG(ERROR, "Fail to allocate memory.", K(size), K(ret));
    } else {
      memset(tmp, 0, sizeof(ObRowkeyColumn) * size);
      if (NULL != columns_) {
        MEMCPY(tmp, columns_, sizeof(ObRowkeyColumn) * size_);
      }
      columns_ = tmp;
      capacity_ = size;
    }
  }

  return ret;
}

DEFINE_SERIALIZE(ObRowkeyInfo)
{
  int ret = OB_SUCCESS;
  int64_t tmp_pos = pos;
  if (NULL == buf || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments.",
               KP(buf), K(buf_len), K(ret));
  } else if (OB_FAIL(serialization::encode_vi64(
      buf, buf_len, tmp_pos, size_))) {
    COMMON_LOG(WARN, "encode size failed.",
               KP(buf), K(buf_len), K(pos), K_(size), K(ret));
  }

  for (int32_t index = 0; OB_SUCC(ret) && index < size_; ++index) {
    if (OB_FAIL(columns_[index].serialize(buf, buf_len, tmp_pos))) {
      COMMON_LOG(WARN, "serialize column failed.",
                 KP(buf), K(buf_len), K(pos), K(columns_[index]), K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    pos = tmp_pos;
  }
  return ret;
}

DEFINE_DESERIALIZE(ObRowkeyInfo)
{
  int ret = OB_SUCCESS;
  int64_t tmp_pos = pos;
  int64_t tmp_size = 0;
  ObRowkeyColumn column;
  if (NULL == buf || data_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments.", KP(buf), K(data_len), K(ret));
  } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, tmp_pos, &tmp_size))) {
    COMMON_LOG(WARN, "decode column size failed.",
               KP(buf), K(data_len), K(pos), K(tmp_size), K(ret));
  }

  for (int64_t index = 0; OB_SUCC(ret) && index < tmp_size; ++index) {
    if (OB_FAIL(column.deserialize(buf, data_len, tmp_pos))) {
      COMMON_LOG(WARN, "Fail to deserialize column.", K(ret));
    } else if (OB_FAIL(add_column(column))) {
      COMMON_LOG(WARN, "Fail to add column.", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    pos = tmp_pos;
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObRowkeyInfo)
{
  int64_t len = 0;
  len += serialization::encoded_length_vi64(size_);
  for (int64_t index = 0; index < size_; ++index) {
    len += columns_[index].get_serialize_size();
  }
  return len;
}

int ObRowkeyInfo::get_column_ids(ObIArray<uint64_t> &column_ids) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(columns_)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "invalid columns array", K(columns_));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < size_; i++) {
    if (OB_FAIL(column_ids.push_back(columns_[i].column_id_))) {
      COMMON_LOG(WARN, "fail to push back column id", K(ret), K(i));
    }
  }
  return ret;
}

int ObRowkeyInfo::get_column_ids(ObBitSet<> &column_ids) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(columns_)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "invalid columns array", K(columns_));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < size_; i++) {
    if (OB_FAIL(column_ids.add_member(static_cast<int64_t>(columns_[i].column_id_)))) {
      COMMON_LOG(WARN, "fail to push back column id", K(ret), K(i));
    }
  }
  return ret;
}

int ObRowkeyInfo::get_spatial_col_id_by_type(uint64_t &column_id, ObObjType type) const
{
  int ret = OB_SUCCESS;
  bool found = false;
  if (OB_ISNULL(columns_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "columns is null");
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < size_ && !found; i++) {
    if (columns_[i].spatial_flag_ && columns_[i].type_.get_type() == type) {
      column_id = columns_[i].column_id_;
      found = true;
    }
  }

  if (OB_SUCC(ret) && !found) {
    ret = OB_SEARCH_NOT_FOUND;
    COMMON_LOG(WARN, "spatial column id not found", K(ret), K(type), K(*this));
  }

  return ret;
}

int ObRowkeyInfo::get_spatial_cellid_col_id(uint64_t &column_id) const
{
  return get_spatial_col_id_by_type(column_id, ObUInt64Type);
}

int ObRowkeyInfo::get_spatial_mbr_col_id(uint64_t &column_id) const
{
  return get_spatial_col_id_by_type(column_id, ObVarcharType);
}
