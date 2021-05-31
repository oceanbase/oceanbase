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
#include "lib/stat/ob_diagnose_info.h"

using namespace oceanbase::blocksstable;
using namespace oceanbase::storage;

ObFuseRowCacheKey::ObFuseRowCacheKey() : table_id_(0), rowkey_size_(0), rowkey_()
{}

ObFuseRowCacheKey::ObFuseRowCacheKey(const uint64_t table_id, const ObStoreRowkey& rowkey) : table_id_(table_id)
{
  rowkey.hash();
  rowkey_ = rowkey;
  rowkey_size_ = rowkey.get_deep_copy_size();
}

uint64_t ObFuseRowCacheKey::get_tenant_id() const
{
  return extract_tenant_id(table_id_);
}

uint64_t ObFuseRowCacheKey::hash() const
{
  uint64_t hash_val = 0;
  hash_val = common::murmurhash(&table_id_, sizeof(table_id_), hash_val);
  if (nullptr != rowkey_.get_obj_ptr() && 0 < rowkey_.get_obj_cnt()) {
    hash_val = rowkey_.murmurhash(hash_val);
  }
  return hash_val;
}

bool ObFuseRowCacheKey::operator==(const ObIKVCacheKey& other) const
{
  bool bret = true;
  const ObFuseRowCacheKey& other_key = reinterpret_cast<const ObFuseRowCacheKey&>(other);
  bret = table_id_ == other_key.table_id_;
  bret &= (rowkey_size_ == other_key.rowkey_size_);
  if (bret && rowkey_size_ > 0) {
    if (nullptr != rowkey_.get_obj_ptr() && nullptr != other_key.rowkey_.get_obj_ptr()) {
      bret = rowkey_.simple_equal(other_key.rowkey_);
    } else {
      bret = false;
    }
  }
  return bret;
}

int64_t ObFuseRowCacheKey::size() const
{
  return sizeof(*this) + rowkey_size_;
}

int ObFuseRowCacheKey::deep_copy(char* buf, const int64_t buf_len, ObIKVCacheKey*& key) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == buf || buf_len < size())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), K(buf_len), "request_size", size());
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid fuse row cache key", K(ret), K(*this));
  } else {
    ObFuseRowCacheKey* pfuse_key = new (buf) ObFuseRowCacheKey();
    pfuse_key->table_id_ = table_id_;
    if (nullptr != rowkey_.get_obj_ptr() && rowkey_size_ > 0) {
      ObRawBufAllocatorWrapper tmp_buf(buf + sizeof(*this), rowkey_size_);
      if (OB_FAIL(rowkey_.deep_copy(pfuse_key->rowkey_, tmp_buf))) {
        LOG_WARN("fail to deep copy rowkey", K(ret));
      } else {
        pfuse_key->rowkey_size_ = rowkey_size_;
        key = pfuse_key;
      }
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
  return OB_LIKELY(table_id_ != 0 && rowkey_size_ > 0 && nullptr != rowkey_.get_obj_ptr());
}

ObFuseRowCacheValue::ObFuseRowCacheValue()
    : obj_array_(nullptr),
      size_(0),
      column_cnt_(0),
      flag_(-1),
      schema_version_(0),
      snapshot_version_(0),
      partition_id_(0),
      sstable_end_log_ts_(0),
      fq_ctx_()
{}

int ObFuseRowCacheValue::init(const ObStoreRow& row, const int64_t schema_version, const int64_t snapshot_version,
    const int64_t partition_id, const int64_t sstable_end_log_ts, const ObFastQueryContext& fq_ctx)
{
  int ret = OB_SUCCESS;
  if (schema_version < 0 || snapshot_version <= 0 || partition_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(schema_version), K(snapshot_version), K(partition_id));
  } else {
    column_cnt_ = row.row_val_.count_;
    obj_array_ = 0 == column_cnt_ ? nullptr : row.row_val_.cells_;
    schema_version_ = schema_version;
    flag_ = row.flag_;
    snapshot_version_ = snapshot_version;
    partition_id_ = partition_id;
    sstable_end_log_ts_ = sstable_end_log_ts;
    fq_ctx_ = fq_ctx;
    size_ = sizeof(ObObj) * column_cnt_;
    for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt_; ++i) {
      const ObObj& obj = obj_array_[i];
      if (obj.need_deep_copy()) {
        size_ += obj.get_deep_copy_size();
      }
    }
  }
  return ret;
}

int64_t ObFuseRowCacheValue::size() const
{
  return sizeof(*this) + size_;
}

void ObFuseRowCacheValue::set_snapshot_version(const int64_t snapshot_version)
{
  const int64_t old_snapshot_version = ATOMIC_LOAD(&snapshot_version_);
  if (snapshot_version > old_snapshot_version) {
    ATOMIC_VCAS(&snapshot_version_, old_snapshot_version, snapshot_version);
  }
}

int ObFuseRowCacheValue::deep_copy(char* buf, const int64_t buf_len, ObIKVCacheValue*& value) const
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
    ObFuseRowCacheValue* pfuse_value = new (buf) ObFuseRowCacheValue();
    if (nullptr == obj_array_) {
      pfuse_value->obj_array_ = nullptr;
    } else {
      char* tmp_buf = buf + sizeof(*this);
      MEMCPY(tmp_buf, obj_array_, sizeof(ObObj) * column_cnt_);
      pfuse_value->obj_array_ = reinterpret_cast<ObObj*>(tmp_buf);
    }
    pfuse_value->column_cnt_ = column_cnt_;
    pfuse_value->schema_version_ = schema_version_;
    pfuse_value->flag_ = flag_;
    pfuse_value->snapshot_version_ = snapshot_version_;
    pfuse_value->partition_id_ = partition_id_;
    pfuse_value->fq_ctx_ = fq_ctx_;
    pfuse_value->size_ = size_;
    pfuse_value->sstable_end_log_ts_ = sstable_end_log_ts_;
    pos = sizeof(*this) + sizeof(ObObj) * column_cnt_;
    for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt_; ++i) {
      ObObj& dst_obj = pfuse_value->obj_array_[i];
      const ObObj& src_obj = obj_array_[i];
      if (dst_obj.need_deep_copy()) {
        if (OB_FAIL(dst_obj.deep_copy(src_obj, buf, buf_len, pos))) {
          LOG_WARN("fail to deep copy obj", K(ret), K(src_obj), K(dst_obj));
        }
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

int ObFuseRowCache::get_row(const ObFuseRowCacheKey& key, const int64_t partition_id, ObFuseRowValueHandle& handle)
{
  int ret = OB_SUCCESS;
  const ObFuseRowCacheValue* value = nullptr;
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
    } else if (partition_id != value->get_partition_id()) {
      ret = OB_ENTRY_NOT_EXIST;
      EVENT_INC(ObStatEventIds::FUSE_ROW_CACHE_MISS);
    } else {
      handle.value_ = const_cast<ObFuseRowCacheValue*>(value);
      EVENT_INC(ObStatEventIds::FUSE_ROW_CACHE_HIT);
    }
  }
  return ret;
}

int ObFuseRowCache::put_row(const ObFuseRowCacheKey& key, const ObFuseRowCacheValue& value)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!key.is_valid() || !value.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(key), K(value));
  } else if (OB_FAIL(put(key, value, true /*overwrite*/))) {
    LOG_WARN("fail to put row to row cache", K(ret), K(key), K(value));
  }
  return ret;
}
