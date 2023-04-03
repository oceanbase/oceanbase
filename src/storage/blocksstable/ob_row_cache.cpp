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
namespace oceanbase
{
using namespace common;
using namespace storage;
namespace blocksstable
{
/**
 * -----------------------------------------------------ObRowCacheKey------------------------------------------------------
 */
ObRowCacheKey::ObRowCacheKey()
  : rowkey_size_(0),
    tenant_id_(0),
    tablet_id_(),
    data_version_(-1),
    table_type_(ObITable::MAX_TABLE_TYPE),
    rowkey_(),
    datum_utils_(nullptr)
{
}

ObRowCacheKey::ObRowCacheKey(const uint64_t tenant_id,
                             const ObTabletID &tablet_id,
                             const ObDatumRowkey &rowkey,
                             const ObStorageDatumUtils &datum_utils,
                             const int64_t data_version,
                             const ObITable::TableType table_type)
{
  tenant_id_ = tenant_id;
  tablet_id_ = tablet_id;
  data_version_ = data_version;
  table_type_ = table_type;
  rowkey_ = rowkey;
  datum_utils_ = &datum_utils;
  rowkey_size_ = rowkey.get_deep_copy_size();
}

ObRowCacheKey::~ObRowCacheKey()
{
}

int ObRowCacheKey::hash(uint64_t &hash_val) const
{
  int ret = OB_SUCCESS;
  hash_val = static_cast<uint64_t>(table_type_);
  hash_val = common::murmurhash(&tenant_id_, sizeof(tenant_id_), hash_val);
  hash_val = common::murmurhash(&tablet_id_, sizeof(tablet_id_), hash_val);
  hash_val = common::murmurhash(&data_version_, sizeof(data_version_), hash_val);
  if (rowkey_.is_valid()) {
    if (OB_ISNULL(datum_utils_)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Unexpected error for null datum utils", K(ret), K(*this));
    } else if (OB_FAIL(rowkey_.hash(*datum_utils_, hash_val))) {
      STORAGE_LOG(WARN, "Failed to calc hash value for datum rowkey", K(ret), K(rowkey_));
    }
  }
  return ret;
}

int ObRowCacheKey::equal(const ObIKVCacheKey &other, bool &equal) const
{
  int ret = OB_SUCCESS;
  const ObRowCacheKey &other_key = reinterpret_cast<const ObRowCacheKey&>(other);
  equal = (rowkey_size_ == other_key.rowkey_size_);
  equal &= tenant_id_ == other_key.tenant_id_;
  equal &= tablet_id_ == other_key.tablet_id_;
  equal &= (data_version_ == other_key.data_version_);
  equal &= (table_type_ == other_key.table_type_);
  if (equal && rowkey_size_ > 0) {
    const ObStorageDatumUtils *datum_utils = (nullptr != datum_utils_) ? datum_utils_ : other_key.datum_utils_;
    if (OB_ISNULL(datum_utils)) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "Invalid argument to compare row cachekey", K(ret), K(*this), K(other_key));
    } else if (OB_FAIL(rowkey_.equal(other_key.rowkey_, *datum_utils, equal))) {
      STORAGE_LOG(WARN, "Failed to check rowkey cache key equal", K(ret), K(rowkey_), K(other_key));
    }
  }
  return ret;
}

uint64_t ObRowCacheKey::get_tenant_id() const
{
  return tenant_id_;
}

int64_t ObRowCacheKey::size() const
{
  return sizeof(*this) + rowkey_size_;
}

int ObRowCacheKey::deep_copy(char *buf, const int64_t buf_len, ObIKVCacheKey *&key) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == buf || buf_len < size())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument, ", K(buf), K(buf_len), K(ret));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN, "Invalid row cache key, ", K(*this), K(ret));
  } else {
    blocksstable::ObRowCacheKey *pkey = new (buf) ObRowCacheKey();
    pkey->tenant_id_ = tenant_id_;
    pkey->tablet_id_ = tablet_id_;
    pkey->data_version_ = data_version_;
    pkey->table_type_ = table_type_;
    pkey->datum_utils_ = nullptr;
    if (rowkey_.is_valid() && 0 < rowkey_size_) {
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
  return OB_LIKELY(0 != tenant_id_ && tablet_id_.is_valid() && rowkey_size_ > 0
      && data_version_ > -1 && (ObITable::is_minor_sstable(table_type_)
                                || ObITable::is_major_sstable(table_type_)
                                || ObITable::is_ddl_sstable(table_type_)
                                || ObITable::is_meta_major_sstable(table_type_))
      && rowkey_.is_valid());
}

/**
 * -----------------------------------------------------ObRowCacheValue------------------------------------------------------
 */
ObRowCacheValue::ObRowCacheValue()
  : datums_(nullptr),
    flag_(),
    size_(0),
    column_cnt_(0),
    start_log_ts_(0),
    block_id_()

{
}

ObRowCacheValue::~ObRowCacheValue()
{
}

int ObRowCacheValue::init(const int64_t start_log_ts,
                          const ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!row.row_flag_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected row", K(ret), K(row));
  } else {
    datums_ = row.storage_datums_;
    column_cnt_ = row.get_column_count();
    start_log_ts_ = start_log_ts;
    flag_ = row.row_flag_;
    size_ = sizeof(ObStorageDatum) * column_cnt_;
    for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt_; i ++) {
      size_ += datums_[i].get_deep_copy_size();
    }
  }
  return ret;
}

int64_t ObRowCacheValue::size() const
{
  return sizeof(*this) + size_;
}

int ObRowCacheValue::deep_copy(char *buf, const int64_t buf_len, ObIKVCacheValue *&value) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == buf || buf_len < size())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument, ", K(buf), K(buf_len), K(ret));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN, "Invalid row cache value, ", K(*this), K(ret));
  } else {
    ObRowCacheValue *pvalue = new (buf) ObRowCacheValue();
    if (NULL == datums_) {
      pvalue->datums_ = NULL;
    } else {
      pvalue->size_ = size_;
      pvalue->datums_ = new (buf + sizeof(*this)) ObStorageDatum [column_cnt_];
      pvalue->column_cnt_ = column_cnt_;
      pvalue->start_log_ts_ = start_log_ts_;
      pvalue->flag_ = flag_;
      pvalue->block_id_ = block_id_;
      int64_t pos = sizeof(ObRowCacheValue) + sizeof(ObStorageDatum) * column_cnt_;
      for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt_; i ++) {
        if (OB_FAIL(pvalue->datums_[i].deep_copy(datums_[i], buf, buf_len, pos))) {
          STORAGE_LOG(WARN, "Failed to deepl copy datum", K(ret), K(i));
        }
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
{
}

ObRowCache::~ObRowCache()
{
}

int ObRowCache::get_row(const ObRowCacheKey &key, ObRowValueHandle &handle)
{
  int ret = OB_SUCCESS;
  const ObRowCacheValue *value = NULL;

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


int ObRowCache::put_row(const ObRowCacheKey &key, const ObRowCacheValue &value)
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

}//end namespace blocksstable
}//end namespace oceanbase
