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

#include "ob_fuse_row_cache.h"

using namespace oceanbase::blocksstable;
using namespace oceanbase::storage;

ObFuseRowCacheKeyBase::ObFuseRowCacheKeyBase()
  : tenant_id_(0), rowkey_size_(0), rowkey_(), schema_column_count_(0), datum_utils_(nullptr)
{
}

ObFuseRowCacheKeyBase::ObFuseRowCacheKeyBase(
    const uint64_t tenant_id,
    const ObTabletID &tablet_id,
    const ObDatumRowkey &rowkey,
    const int64_t schema_column_count,
    const ObStorageDatumUtils &datum_utils)
{
  tenant_id_ = tenant_id;
  tablet_id_ = tablet_id;
  rowkey_ = rowkey;
  rowkey_size_ = rowkey.get_deep_copy_size();
  schema_column_count_ = schema_column_count;
  datum_utils_ = &datum_utils;
}

int ObFuseRowCacheKeyBase::hash(uint64_t &hash_val) const
{
  int ret = OB_SUCCESS;
  hash_val = common::murmurhash(&tenant_id_, sizeof(tenant_id_), 0);
  hash_val = common::murmurhash(&tablet_id_, sizeof(tablet_id_), hash_val);
  hash_val = common::murmurhash(&schema_column_count_, sizeof(schema_column_count_), hash_val);
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

int ObFuseRowCacheKeyBase::equal(const ObFuseRowCacheKeyBase &other, bool &equal) const
{
  int ret = OB_SUCCESS;
  equal = tenant_id_ == other.tenant_id_;
  equal &= tablet_id_ == other.tablet_id_;
  equal &= rowkey_size_ == other.rowkey_size_;
  equal &= schema_column_count_ == other.schema_column_count_;
  if (equal && rowkey_size_ > 0) {
    const ObStorageDatumUtils *datum_utils = (nullptr != datum_utils_) ? datum_utils_ : other.datum_utils_;
    if (OB_ISNULL(datum_utils)) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "Invalid argument to compare row cachekey", K(ret), K(*this), K(other));
    } else if (OB_FAIL(rowkey_.equal(other.rowkey_, *datum_utils, equal))) {
      STORAGE_LOG(WARN, "Failed to check rowkey cache key equal", K(ret), K(rowkey_), K(other));
    }
  }
  return ret;
}

int ObFuseRowCacheKeyBase::deep_copy(char *buf, const int64_t buf_len, ObFuseRowCacheKeyBase &dest) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == buf || buf_len < rowkey_size_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), K(buf_len), "request_size", rowkey_size_);
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid fuse row cache key", K(ret), K(*this));
  } else {
    dest.tenant_id_ = tenant_id_;
    dest.tablet_id_ = tablet_id_;
    dest.schema_column_count_ = schema_column_count_;
    if (rowkey_.is_valid() && rowkey_size_ > 0) {
      ObRawBufAllocatorWrapper tmp_buf(buf, rowkey_size_);
      if (OB_FAIL(rowkey_.deep_copy(dest.rowkey_, tmp_buf))) {
        LOG_WARN("fail to deep copy rowkey", K(ret));
      } else {
        dest.rowkey_size_ = rowkey_size_;
      }
    }
  }
  return ret;
}

ObFuseRowCacheKey::ObFuseRowCacheKey()
  : base_(),
    tablet_snapshot_version_(0)
{
}

ObFuseRowCacheKey::ObFuseRowCacheKey(
    const uint64_t tenant_id,
    const ObTabletID &tablet_id,
    const ObDatumRowkey &rowkey,
    const int64_t tablet_snapshot_version,
    const int64_t schema_column_count,
    const ObStorageDatumUtils &datum_utils)
  : base_(tenant_id, tablet_id, rowkey, schema_column_count, datum_utils),
    tablet_snapshot_version_(tablet_snapshot_version)
{
}

int ObFuseRowCacheKey::equal(const ObIKVCacheKey &other, bool &equal) const
{
  int ret = OB_SUCCESS;
  const ObFuseRowCacheKey &other_key = reinterpret_cast<const ObFuseRowCacheKey &>(other);
  return tablet_snapshot_version_ == other_key.tablet_snapshot_version_ && base_.equal(other_key.base_, equal);
}

int ObFuseRowCacheKey::hash(uint64_t &hash_value) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(base_.hash(hash_value))) {
    LOG_WARN("Failed to hash base key", K(ret), K(*this));
  } else {
    hash_value = common::murmurhash(&tablet_snapshot_version_, sizeof(tablet_snapshot_version_), hash_value);
  }
  return ret;
}

uint64_t ObFuseRowCacheKey::get_tenant_id() const
{
  return base_.get_tenant_id();
}

int64_t ObFuseRowCacheKey::size() const
{
  return sizeof(*this) + base_.rowkey_size();
}

int ObFuseRowCacheKey::deep_copy(char *buf, const int64_t buf_len, ObIKVCacheKey *&key) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == buf || buf_len < size())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), K(buf_len), "request_size", size());
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid fuse row cache key", K(ret), K(*this));
  } else {
    ObFuseRowCacheKey *pfuse_key = new (buf) ObFuseRowCacheKey();
    pfuse_key->tablet_snapshot_version_ = tablet_snapshot_version_;
    if (OB_FAIL(base_.deep_copy(buf + sizeof(ObFuseRowCacheKey), buf_len - sizeof(ObFuseRowCacheKey), pfuse_key->base_))) {
      LOG_WARN("fail to deep copy base key", K(ret));
    } else {
      key = pfuse_key;
    }
    if (OB_FAIL(ret)) {
      pfuse_key->~ObFuseRowCacheKey();
      pfuse_key = nullptr;
    }
  }
  return ret;
}

bool ObFuseRowCacheKey::is_valid() const
{
  return OB_LIKELY(base_.is_valid() && tablet_snapshot_version_ >= 0);
}

ObFuseRowCacheValue::ObFuseRowCacheValue()
  : datums_(nullptr),
    size_(0),
    column_cnt_(0),
    read_snapshot_version_(0),
    flag_()
{
}

int ObFuseRowCacheValue::init(const ObDatumRow &row, const int64_t read_snapshot_version)
{
  int ret = OB_SUCCESS;

  column_cnt_ = row.get_column_count();
  datums_ = 0 == column_cnt_ ? nullptr : row.storage_datums_;
  flag_ = row.row_flag_;
  size_ = sizeof(ObStorageDatum) * column_cnt_;
  for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt_; ++i) {
    size_ += datums_[i].get_deep_copy_size();
  }
  read_snapshot_version_ = read_snapshot_version;

  return ret;
}

int64_t ObFuseRowCacheValue::size() const
{
  return sizeof(*this) + size_;
}

int ObFuseRowCacheValue::deep_copy(char *buf, const int64_t buf_len, ObIKVCacheValue *&value) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == buf || buf_len < size())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), K(buf_len), "request_size", size());
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid row cache value", K(ret));
  } else {
    int64_t pos = 0;
    ObFuseRowCacheValue *pfuse_value = new (buf) ObFuseRowCacheValue();
    if (nullptr == datums_) {
      pfuse_value->datums_ = nullptr;
    } else {
      char *tmp_buf = buf + sizeof(*this);
      MEMCPY(tmp_buf, datums_, sizeof(ObStorageDatum) * column_cnt_);
      pfuse_value->datums_ = reinterpret_cast<ObStorageDatum *>(tmp_buf);
    }
    pfuse_value->column_cnt_ = column_cnt_;
    pfuse_value->flag_ = flag_;
    pfuse_value->read_snapshot_version_ = read_snapshot_version_;
    pfuse_value->size_ = size_;
    pos = sizeof(*this) + sizeof(ObStorageDatum) * column_cnt_;
    for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt_; ++i) {
      if (OB_FAIL(pfuse_value->datums_[i].deep_copy(datums_[i], buf, buf_len, pos))) {
        STORAGE_LOG(WARN, "Failed to deep copy datum", K(ret), K(i));
      }
    }

    if (OB_SUCC(ret)) {
      value = pfuse_value;
    } else if (nullptr != pfuse_value) {
      pfuse_value->~ObFuseRowCacheValue();
      pfuse_value = nullptr;
    }
  }
  return ret;
}

int ObFuseRowCache::get_row(const ObFuseRowCacheKey &key, ObFuseRowValueHandle &handle)
{
  int ret = OB_SUCCESS;
  const ObFuseRowCacheValue *value = nullptr;
  if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(key));
  } else if (OB_FAIL(get(key, value, handle.handle_))) {
    if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
      LOG_WARN("fail to get key from row cache", K(ret));
    }
    EVENT_INC(ObStatEventIds::FUSE_ROW_CACHE_MISS);
  } else {
    if (OB_ISNULL(value)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, the value must not be NULL", K(ret));
    } else {
      handle.value_ = const_cast<ObFuseRowCacheValue *>(value);
    }
  }
  return ret;
}

int ObFuseRowCache::put_row(const ObFuseRowCacheKey &key, const ObFuseRowCacheValue &value)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!key.is_valid() || !value.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(key), K(value));
  } else if (OB_FAIL(put(key, value, true/*overwrite*/))) {
    LOG_WARN("fail to put row to row cache", K(ret), K(key), K(value));
  }
  return ret;
}

ObMultiVersionFuseRowCacheKey::ObMultiVersionFuseRowCacheKey()
  : base_(), begin_version_(0), end_version_(0)
{
}

ObMultiVersionFuseRowCacheKey::ObMultiVersionFuseRowCacheKey(
    const int64_t begin_version,
    const int64_t end_version,
    const uint64_t tenant_id,
    const ObTabletID &tablet_id,
    const ObDatumRowkey &rowkey,
    const int64_t schema_column_count,
    const ObStorageDatumUtils &datum_utils)
  : base_(tenant_id, tablet_id, rowkey, schema_column_count, datum_utils),
    begin_version_(begin_version),
    end_version_(end_version)
{
}

int ObMultiVersionFuseRowCacheKey::equal(const ObIKVCacheKey &other, bool &equal) const
{
  int ret = OB_SUCCESS;
  const ObMultiVersionFuseRowCacheKey &other_key = reinterpret_cast<const ObMultiVersionFuseRowCacheKey &>(other);
  equal = begin_version_ == other_key.begin_version_ &&
          end_version_ == other_key.end_version_;
  if (equal && OB_FAIL(base_.equal(other_key.base_, equal))) {
    LOG_WARN("Failed to check if fuse row keys are equal", K(ret), K(*this), K(other_key));
  }
  return ret;
}

int ObMultiVersionFuseRowCacheKey::hash(uint64_t &hash_value) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(base_.hash(hash_value))) {
    LOG_WARN("Failed to hash base key", K(ret), K(*this));
  } else {
    hash_value = common::murmurhash(&begin_version_, sizeof(begin_version_), hash_value);
    hash_value = common::murmurhash(&end_version_, sizeof(end_version_), hash_value);
  }
  return ret;
}

uint64_t ObMultiVersionFuseRowCacheKey::get_tenant_id() const
{
  return base_.get_tenant_id();
}

int64_t ObMultiVersionFuseRowCacheKey::size() const
{
  return sizeof(*this) + base_.rowkey_size();
}

int ObMultiVersionFuseRowCacheKey::deep_copy(char *buf, const int64_t buf_len, ObIKVCacheKey *&key) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == buf || buf_len < size())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), K(buf_len), "request_size", size());
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid multi version fuse row cache key", K(ret), K(*this));
  } else {
    ObMultiVersionFuseRowCacheKey *pfuse_key = new (buf) ObMultiVersionFuseRowCacheKey();
    pfuse_key->begin_version_ = begin_version_;
    pfuse_key->end_version_ = end_version_;
    if (OB_FAIL(base_.deep_copy(buf + sizeof(ObMultiVersionFuseRowCacheKey), buf_len - sizeof(ObMultiVersionFuseRowCacheKey), pfuse_key->base_))) {
      LOG_WARN("fail to deep copy base key", K(ret));
    } else {
      key = pfuse_key;
    }
    if (OB_FAIL(ret)) {
      pfuse_key->~ObMultiVersionFuseRowCacheKey();
      pfuse_key = nullptr;
    }
  }
  return ret;
}

bool ObMultiVersionFuseRowCacheKey::is_valid() const
{
  return OB_LIKELY(begin_version_ >= 0 && end_version_ > begin_version_ && base_.is_valid());
}

int ObMultiVersionFuseRowCache::get_row(const ObMultiVersionFuseRowCacheKey &key, ObFuseRowValueHandle &handle)
{
  int ret = OB_SUCCESS;
  const ObFuseRowCacheValue *value = nullptr;
  if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(key));
  } else if (OB_FAIL(get(key, value, handle.handle_))) {
    if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
      LOG_WARN("fail to get key from row cache", K(ret));
    }
    EVENT_INC(ObStatEventIds::FUSE_ROW_CACHE_MISS);
    EVENT_INC(ObStatEventIds::MULTI_VERSION_FUSE_ROW_CACHE_MISS);
  } else {
    if (OB_ISNULL(value)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, the value must not be NULL", K(ret));
    } else {
      handle.value_ = const_cast<ObFuseRowCacheValue *>(value);
    }
  }
  return ret;
}

int ObMultiVersionFuseRowCache::put_row(const ObMultiVersionFuseRowCacheKey &key, const ObFuseRowCacheValue &value)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!key.is_valid() || !value.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(key), K(value));
  } else if (OB_FAIL(put(key, value, true/*overwrite*/))) {
    LOG_WARN("fail to put row to row cache", K(ret), K(key), K(value));
  }
  return ret;
}