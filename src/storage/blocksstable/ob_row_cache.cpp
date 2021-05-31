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

#include "ob_row_cache.h"
#include "lib/stat/ob_diagnose_info.h"
#include "storage/ob_sstable.h"
namespace oceanbase {
using namespace common;
using namespace storage;
namespace blocksstable {
/**
 * -----------------------------------------------------ObRowCacheKey------------------------------------------------------
 */
ObRowCacheKey::ObRowCacheKey()
    : table_id_(0), file_id_(0), rowkey_size_(0), data_version_(-1), table_type_(ObITable::MAX_TABLE_TYPE)
{
  rowkey_.assign(NULL, 0);
}

ObRowCacheKey::ObRowCacheKey(const uint64_t table_id, const int64_t file_id, const ObStoreRowkey& rowkey,
    const int64_t data_version, const ObITable::TableType table_type)
{
  table_id_ = table_id;
  file_id_ = file_id;
  rowkey_size_ = rowkey.get_deep_copy_size();
  data_version_ = data_version;
  table_type_ = table_type;
  rowkey.hash();
  rowkey_ = rowkey;
}

ObRowCacheKey::~ObRowCacheKey()
{}

uint64_t ObRowCacheKey::hash() const
{
  uint64_t hash_val = static_cast<uint64_t>(table_type_);
  hash_val = common::murmurhash(&table_id_, sizeof(table_id_), hash_val);
  hash_val = common::murmurhash(&file_id_, sizeof(file_id_), hash_val);
  hash_val = common::murmurhash(&data_version_, sizeof(data_version_), hash_val);
  if (NULL != rowkey_.get_obj_ptr() && 0 < rowkey_.get_obj_cnt()) {
    hash_val = rowkey_.murmurhash(hash_val);
  }
  return hash_val;
}

bool ObRowCacheKey::operator==(const ObIKVCacheKey& other) const
{
  bool ret = true;
  const ObRowCacheKey& other_key = reinterpret_cast<const ObRowCacheKey&>(other);
  ret = table_id_ == other_key.table_id_;
  ret &= file_id_ == other_key.file_id_;
  ret &= (rowkey_size_ == other_key.rowkey_size_);
  ret &= (data_version_ == other_key.data_version_);
  ret &= (table_type_ == other_key.table_type_);
  if (ret && rowkey_size_ > 0) {
    if (NULL != rowkey_.get_obj_ptr() && NULL != other_key.rowkey_.get_obj_ptr()) {
      ret = rowkey_.simple_equal(other_key.rowkey_);
    } else {
      ret = false;
    }
  }
  return ret;
}

uint64_t ObRowCacheKey::get_tenant_id() const
{
  return extract_tenant_id(table_id_);
}

int64_t ObRowCacheKey::size() const
{
  return sizeof(*this) + rowkey_size_;
}

int ObRowCacheKey::deep_copy(char* buf, const int64_t buf_len, ObIKVCacheKey*& key) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == buf || buf_len < size())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument, ", K(buf), K(buf_len), K(ret));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN, "Invalid row cache key, ", K(*this), K(ret));
  } else {
    blocksstable::ObRowCacheKey* pkey = new (buf) ObRowCacheKey();
    pkey->table_id_ = table_id_;
    pkey->file_id_ = file_id_;
    pkey->data_version_ = data_version_;
    pkey->table_type_ = table_type_;
    if (NULL != rowkey_.get_obj_ptr() && 0 < rowkey_size_) {
      ObRawBufAllocatorWrapper temp_buf(buf + sizeof(*this), rowkey_size_);
      if (OB_FAIL(rowkey_.deep_copy(pkey->rowkey_, temp_buf))) {
        STORAGE_LOG(WARN, "Fail to deep copy rowkey, ", K(ret));
      } else {
        pkey->rowkey_size_ = rowkey_size_;
        key = pkey;
      }
    }

    if (OB_FAIL(ret)) {
      pkey->~ObRowCacheKey();
    }
  }
  return ret;
}

bool ObRowCacheKey::is_valid() const
{
  return OB_LIKELY(0 != table_id_ && rowkey_size_ > 0 && data_version_ > -1 &&
                   (ObITable::is_minor_sstable(table_type_) || ObITable::is_major_sstable(table_type_)) &&
                   NULL != rowkey_.get_obj_ptr());
}

/**
 * -----------------------------------------------------ObRowCacheValue------------------------------------------------------
 */
ObRowCacheValue::ObRowCacheValue()
    : obj_array_(NULL),
      column_ids_(NULL),
      size_(0),
      column_cnt_(0),
      schema_version_(0),
      flag_(-1),
      block_id_(),
      row_dml_()

{}

ObRowCacheValue::~ObRowCacheValue()
{}

int ObRowCacheValue::init(
    const ObFullMacroBlockMeta& macro_meta, const storage::ObStoreRow* row, const MacroBlockId& block_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!macro_meta.is_valid() || NULL == row || !block_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument, ", K(macro_meta), KP(row), K(block_id), K(ret));
  } else {
    obj_array_ = row->row_val_.cells_;
    column_ids_ = row->is_sparse_row_ ? row->column_ids_ : NULL;
    column_cnt_ = row->row_val_.count_;
    schema_version_ = macro_meta.meta_->schema_version_;
    flag_ = row->flag_;
    block_id_ = block_id;
    row_dml_.set_dml(row->get_dml());
    row_dml_.set_first_dml(row->get_first_dml());
    size_ = sizeof(ObObj) * column_cnt_;
    if (NULL != column_ids_) {
      size_ += sizeof(uint16_t) * column_cnt_;
    }
    int64_t data_length = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt_; i++) {
      const common::ObObj& tmp_obj_array = obj_array_[i];
      if (tmp_obj_array.need_deep_copy()) {
        size_ += tmp_obj_array.get_data_length();
      }
    }
  }
  return ret;
}

int64_t ObRowCacheValue::size() const
{
  return sizeof(*this) + size_;
}

int ObRowCacheValue::deep_copy(char* buf, const int64_t buf_len, ObIKVCacheValue*& value) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == buf || buf_len < size())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument, ", K(buf), K(buf_len), K(ret));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN, "Invalid row cache value, ", K(*this), K(ret));
  } else {
    ObRowCacheValue* pvalue = new (buf) ObRowCacheValue();
    if (NULL == obj_array_) {
      pvalue->obj_array_ = NULL;
    } else {
      char* tmp_buf = buf + sizeof(*this);
      MEMCPY(tmp_buf, obj_array_, sizeof(ObObj) * column_cnt_);
      pvalue->size_ = size_;
      pvalue->obj_array_ = reinterpret_cast<ObObj*>(tmp_buf);
      pvalue->column_cnt_ = column_cnt_;
      pvalue->schema_version_ = schema_version_;
      pvalue->flag_ = flag_;
      pvalue->block_id_ = block_id_;
      pvalue->row_dml_ = row_dml_;
      int64_t data_length = 0;
      tmp_buf += sizeof(ObObj) * column_cnt_;
      for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt_; i++) {
        if (pvalue->obj_array_[i].need_deep_copy()) {
          data_length = pvalue->obj_array_[i].get_data_length();
          if (pvalue->obj_array_[i].is_string_type() || pvalue->obj_array_[i].is_raw()) {
            MEMCPY(tmp_buf, pvalue->obj_array_[i].v_.string_, data_length);
            pvalue->obj_array_[i].v_.string_ = tmp_buf;
            tmp_buf += data_length;
          } else if (ob_is_number_tc(pvalue->obj_array_[i].get_type())) {
            MEMCPY(tmp_buf, pvalue->obj_array_[i].v_.nmb_digits_, data_length);
            pvalue->obj_array_[i].v_.nmb_digits_ = (uint32_t*)tmp_buf;
            tmp_buf += data_length;
          } else {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "meta is error, ", K(pvalue->obj_array_[i]), K(ret));
          }
        }
      }
      if (NULL != column_ids_) {
        MEMCPY(tmp_buf, column_ids_, sizeof(uint16_t) * column_cnt_);
        pvalue->column_ids_ = reinterpret_cast<uint16_t*>(tmp_buf);
      }
    }
    value = pvalue;
  }
  return ret;
}
/**
 * -----------------------------------------------------ObRowCache------------------------------------------------------
 */
ObRowCache::ObRowCache()
{}

ObRowCache::~ObRowCache()
{}

int ObRowCache::get_row(const ObRowCacheKey& key, ObRowValueHandle& handle)
{
  int ret = OB_SUCCESS;
  const ObRowCacheValue* value = NULL;

  if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid row cache key.", K(key), K(ret));
  } else if (OB_SUCCESS != (ret = get(key, value, handle.handle_))) {
    if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
      STORAGE_LOG(WARN, "Fail to get key from row cache, ", K(ret));
    }
    EVENT_INC(ObStatEventIds::ROW_CACHE_MISS);
  } else {
    EVENT_INC(ObStatEventIds::ROW_CACHE_HIT);
    if (OB_ISNULL(value)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Unexpected error, the value is NULL, ", K(ret));
    } else {
      handle.row_value_ = const_cast<ObRowCacheValue*>(value);
    }
  }
  return ret;
}

int ObRowCache::put_row(const ObRowCacheKey& key, const ObRowCacheValue& value)
{
  int ret = OB_SUCCESS;
  bool overwrite = true;
  if (OB_UNLIKELY(!key.is_valid() || !value.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid row cache input param.", K(key), K(value), K(ret));
  } else if (OB_SUCCESS != (ret = put(key, value, overwrite))) {
    STORAGE_LOG(WARN, "Fail to put row to row cache, ", K(ret));
  }
  return ret;
}

}  // end namespace blocksstable
}  // end namespace oceanbase
