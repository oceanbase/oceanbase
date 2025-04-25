//Copyright (c) 2025 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#define USING_LOG_PREFIX STORAGE

#include "storage/ddl/ob_tablet_split_cache.h"

namespace oceanbase
{
using namespace common;
using namespace blocksstable;
namespace storage
{
/*
* ObTabletSplitCacheKey
* */
ObTabletSplitCacheKey::ObTabletSplitCacheKey()
  : tenant_id_(OB_INVALID_TENANT_ID), tablet_id_(), bucket_id_(0)
{
}

ObTabletSplitCacheKey::ObTabletSplitCacheKey(const uint64_t tenant_id, const ObTabletID &tablet_id, const uint8_t bucket_id)
  : tenant_id_(tenant_id), tablet_id_(tablet_id), bucket_id_(bucket_id)
{
}

bool ObTabletSplitCacheKey::is_valid() const
{
  return tenant_id_ > 0 && tablet_id_.is_valid();
}

int ObTabletSplitCacheKey::equal(const ObIKVCacheKey &other, bool &equal) const
{
  int ret = OB_SUCCESS;
  const ObTabletSplitCacheKey &other_key = reinterpret_cast<const ObTabletSplitCacheKey &>(other);
  if (OB_UNLIKELY(!is_valid() || !other_key.is_valid())) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid data", KR(ret), KPC(this), K(other_key));
  } else {
    equal = (tenant_id_ == other_key.tenant_id_ && tablet_id_ == other_key.tablet_id_ && bucket_id_ == other_key.bucket_id_);
  }
  return ret;
}

int ObTabletSplitCacheKey::hash(uint64_t &hash_val) const
{
  int ret = OB_SUCCESS;
  hash_val = murmurhash(&tenant_id_, sizeof(tenant_id_), 0);
  hash_val = murmurhash(&tablet_id_, sizeof(tablet_id_), hash_val);
  hash_val = murmurhash(&bucket_id_, sizeof(bucket_id_), hash_val);
  return ret;
}

int ObTabletSplitCacheKey::deep_copy(char *buf, const int64_t buf_len, ObIKVCacheKey *&key) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == buf || buf_len < size())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), K(buf_len), "request_size", size());
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid truncate info cache key", K(ret), K(*this));
  } else {
    key = new (buf) ObTabletSplitCacheKey(tenant_id_, tablet_id_, bucket_id_);
  }
  return ret;
}

/*
* ObTabletSplitCacheValue
* */
ObTabletSplitCacheValue::ObTabletSplitCacheValue()
  : deep_copy_size_(0),
    split_cnt_(0),
    split_type_(ObTabletSplitType::RANGE),
    partkey_is_rowkey_prefix_(false),
    start_partkey_(),
    end_partkey_()
{
}

ObTabletSplitCacheValue::~ObTabletSplitCacheValue()
{
}

int ObTabletSplitCacheValue::init(const ObTabletSplitTscInfo &split_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!split_info.is_split_dst_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("only support cache split dst info", K(ret), K(split_info));
  } else {
    deep_copy_size_ = sizeof(*this);
    split_cnt_ = split_info.split_cnt_;
    split_type_ = split_info.split_type_;
    partkey_is_rowkey_prefix_ = split_info.partkey_is_rowkey_prefix_;
  }

  if (OB_FAIL(ret)) {
  } else if (!split_info.start_partkey_.is_valid()) {
    start_partkey_.reset();
  } else if (OB_FAIL(start_partkey_.assign(split_info.start_partkey_.datums_, split_info.start_partkey_.get_datum_cnt()))) {
    LOG_WARN("failed to assign", K(ret), K(split_info.start_partkey_));
  } else {
    deep_copy_size_ += split_info.start_partkey_.get_deep_copy_size();
  }

  if (OB_FAIL(ret)) {
  } else if (!split_info.end_partkey_.is_valid()) {
    end_partkey_.reset();
  } else if (OB_FAIL(end_partkey_.assign(split_info.end_partkey_.datums_, split_info.end_partkey_.get_datum_cnt()))) {
    LOG_WARN("failed to assign", K(ret), K(split_info.end_partkey_));
  } else {
    deep_copy_size_ += split_info.end_partkey_.get_deep_copy_size();
  }
  return ret;
}

int ObTabletSplitCacheValue::deep_copy(ObTabletSplitTscInfo &split_info, ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  split_info.split_cnt_ = split_cnt_;
  split_info.split_type_ = split_type_;
  split_info.partkey_is_rowkey_prefix_ = partkey_is_rowkey_prefix_;
  split_info.is_split_dst_ = true;
  if (!start_partkey_.is_valid()) {
    split_info.start_partkey_.reset();
  } else if (OB_FAIL(start_partkey_.deep_copy(split_info.start_partkey_, allocator))) {
    LOG_WARN("failed to deep copy", K(ret), K(start_partkey_));
  }

  if (OB_FAIL(ret)) {
  } else if (!end_partkey_.is_valid()) {
    split_info.end_partkey_.reset();
  } else if (OB_FAIL(end_partkey_.deep_copy(split_info.end_partkey_, allocator))) {
    LOG_WARN("failed to deep copy", K(ret), K(end_partkey_));
  }
  return ret;
}

bool ObTabletSplitCacheValue::is_valid() const
{
  return deep_copy_size_ >= sizeof(*this) && split_cnt_ > 0;
}

int ObTabletSplitCacheValue::deep_copy(char *buf, const int64_t buf_len, ObIKVCacheValue *&value) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == buf || buf_len < size())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), K(buf_len), "request_size", size());
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid truncate info cache value", K(ret));
  } else {
    int64_t pos = 0;
    ObTabletSplitCacheValue *pvalue = new (buf) ObTabletSplitCacheValue();
    pos += sizeof(*this);
    pvalue->deep_copy_size_ = deep_copy_size_;
    pvalue->split_cnt_ = split_cnt_;
    pvalue->split_type_ = split_type_;
    pvalue->partkey_is_rowkey_prefix_ = partkey_is_rowkey_prefix_;
    if (OB_UNLIKELY(pos > buf_len)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("buffer overflow", K(ret), K(buf_len), KPC(this));
    } else if (start_partkey_.is_valid()) {
      if (OB_FAIL(start_partkey_.deep_copy(pvalue->start_partkey_, buf + pos, buf_len - pos))) {
        LOG_WARN("failed to deep copy", K(ret), K(start_partkey_));
      } else {
        pos += start_partkey_.get_deep_copy_size();
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(pos > buf_len)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("buffer overflow", K(ret), K(buf_len), KPC(this));
    } else if (end_partkey_.is_valid()) {
      if (OB_FAIL(end_partkey_.deep_copy(pvalue->end_partkey_, buf + pos, buf_len - pos))) {
        LOG_WARN("failed to deep copy", K(ret), K(end_partkey_));
      } else {
        pos += end_partkey_.get_deep_copy_size();
      }
    }

    if (OB_SUCC(ret)) {
      value = pvalue;
    }

    if (OB_FAIL(ret) && nullptr != pvalue) {
      pvalue->~ObTabletSplitCacheValue();
      pvalue = nullptr;
    }
  }
  return ret;
}

/**
 * -----------------------------------------------------ObTabletSplitCache------------------------------------------------------
 */
ObTabletSplitCache::ObTabletSplitCache()
{
}

ObTabletSplitCache::~ObTabletSplitCache()
{
}

int ObTabletSplitCache::get_split_cache(const ObTabletSplitCacheKey &key, ObTabletSplitCacheValueHandle &handle)
{
  int ret = OB_SUCCESS;
  const ObTabletSplitCacheValue *value = NULL;

  if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid tablet split cache key.", K(ret), K(key));
  } else if (OB_FAIL(get(key, value, handle.handle_))) {
    if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
      STORAGE_LOG(WARN, "Fail to get key from tablet split cache, ", K(ret));
    }
  } else {
    if (OB_ISNULL(value)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Unexpected error, the value is NULL, ", K(ret));
    } else {
      handle.row_value_ = const_cast<ObTabletSplitCacheValue*>(value);
    }
  }
  return ret;
}

int ObTabletSplitCache::put_split_cache(const ObTabletSplitCacheKey &key, const ObTabletSplitCacheValue &value)
{
  int ret = OB_SUCCESS;
  bool overwrite = true;
  if (OB_UNLIKELY(!key.is_valid() || !value.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid row cache input param.", K(key), K(value), K(ret));
  } else if (OB_FAIL(put(key, value, overwrite))) {
    STORAGE_LOG(WARN, "Fail to put row to row cache, ", K(ret));
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
