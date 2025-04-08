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
#include "storage/truncate_info/ob_truncate_info_kv_cache.h"
#include "storage/truncate_info/ob_truncate_info.h"
#include "storage/truncate_info/ob_truncate_info_array.h"
#include "storage/blocksstable/ob_storage_cache_suite.h"
namespace oceanbase
{
using namespace common;
using namespace blocksstable;
namespace storage
{
/*
* ObTruncateInfoCacheKey
* */
ObTruncateInfoCacheKey::ObTruncateInfoCacheKey(
  const uint64_t tenant_id,
  const ObTabletID &tablet_id,
  const int64_t schema_version,
  const int64_t last_major_snapshot)
  : tenant_id_(tenant_id),
    tablet_id_(tablet_id),
    schema_version_(schema_version),
    last_major_snapshot_(last_major_snapshot)
{
}

bool ObTruncateInfoCacheKey::is_valid() const
{
  return tenant_id_ > 0
    && tablet_id_.is_valid()
    && schema_version_ > 0
    && last_major_snapshot_ > 0;
}

int ObTruncateInfoCacheKey::equal(const ObIKVCacheKey &other, bool &equal) const
{
  int ret = OB_SUCCESS;
  const ObTruncateInfoCacheKey &other_key = reinterpret_cast<const ObTruncateInfoCacheKey &>(other);
  if (OB_UNLIKELY(!is_valid() || !other_key.is_valid())) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid data", KR(ret), KPC(this), K(other_key));
  } else {
    equal = (tenant_id_ == other_key.tenant_id_
      && tablet_id_ == other_key.tablet_id_
      && schema_version_ == other_key.schema_version_
      && last_major_snapshot_ == other_key.last_major_snapshot_);
  }
  return ret;
}

int ObTruncateInfoCacheKey::hash(uint64_t &hash_val) const
{
  int ret = OB_SUCCESS;
  hash_val = murmurhash(&tenant_id_, sizeof(tenant_id_), 0);
  hash_val = murmurhash(&tablet_id_, sizeof(tablet_id_), hash_val);
  hash_val = murmurhash(&schema_version_, sizeof(schema_version_), hash_val);
  hash_val = murmurhash(&last_major_snapshot_, sizeof(last_major_snapshot_), hash_val);
  return ret;
}

int ObTruncateInfoCacheKey::deep_copy(char *buf, const int64_t buf_len, ObIKVCacheKey *&key) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == buf || buf_len < size())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), K(buf_len), "request_size", size());
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid truncate info cache key", K(ret), K(*this));
  } else {
    key = new (buf) ObTruncateInfoCacheKey(tenant_id_, tablet_id_, schema_version_, last_major_snapshot_);
  }
  return ret;
}
/*
* ObTruncateInfoCacheValue
* */
ObTruncateInfoCacheValue::ObTruncateInfoCacheValue()
  : count_(0),
    truncate_info_array_(nullptr),
    deep_copy_size_(0)
{}

int ObTruncateInfoCacheValue::init(const int64_t count, ObTruncateInfo *truncate_info_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(count <= 0 || nullptr == truncate_info_array)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(count), KP(truncate_info_array));
  } else {
    deep_copy_size_ = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      if (OB_UNLIKELY(!truncate_info_array[i].is_valid())) {
        ret = OB_INVALID_DATA;
        LOG_WARN("invalid data to init truncate info cache value", KR(ret), K(i), K(truncate_info_array[i]));
      } else {
        deep_copy_size_ += truncate_info_array[i].get_deep_copy_size();
      }
    } // for
    if (OB_SUCC(ret)) {
      truncate_info_array_ = truncate_info_array;
      count_ = count;
      deep_copy_size_ += sizeof(ObTruncateInfo) * count_;
    }
  }
  return ret;
}

int ObTruncateInfoCacheValue::deep_copy(char *buf, const int64_t buf_len, ObIKVCacheValue *&value) const
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
    ObTruncateInfoCacheValue *pfuse_value = new (buf) ObTruncateInfoCacheValue();
    pos += sizeof(*this);
    pfuse_value->truncate_info_array_ = new (buf + pos) ObTruncateInfo[count_];
    pfuse_value->count_ = count_;
    pfuse_value->deep_copy_size_ = deep_copy_size_;
    pos += sizeof(ObTruncateInfo) * count_;
    for (int64_t i = 0; OB_SUCC(ret) && i < count_; ++i) {
      if (OB_FAIL(truncate_info_array_[i].deep_copy(buf, buf_len, pos, pfuse_value->truncate_info_array_[i]))) {
        STORAGE_LOG(WARN, "Failed to deep copy truncate info", K(ret), K(i), K(truncate_info_array_[i]));
      }
    }

    if (OB_SUCC(ret)) {
      value = pfuse_value;
    } else if (nullptr != pfuse_value) {
      pfuse_value->~ObTruncateInfoCacheValue();
      pfuse_value = nullptr;
    }
  }
  return ret;
}

/*
* ObTruncateInfoKVCache
* */
int ObTruncateInfoKVCache::init(const char *cache_name, const int64_t priority)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL((ObKVCache<ObTruncateInfoCacheKey, ObTruncateInfoCacheValue>::init(cache_name,  priority)))) {
    LOG_WARN("fail to init truncate info kv cache", K(ret), K(priority));
  }
  return ret;
}

int ObTruncateInfoKVCache::get_truncate_info_array(const ObTruncateInfoCacheKey &key, ObTruncateInfoValueHandle &handle)
{
  int ret = OB_SUCCESS;
  const ObTruncateInfoCacheValue *value = nullptr;
  if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(key));
  } else if (OB_FAIL(get(key, value, handle.handle_))) {
    if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
      LOG_WARN("fail to get key from truncate info cache", K(ret));
    }
    EVENT_INC(ObStatEventIds::TRUNCATE_INFO_CACHE_MISS);
  } else if (OB_ISNULL(value)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, the value must not be NULL", K(ret));
  } else {
    handle.value_ = value;
    EVENT_INC(ObStatEventIds::TRUNCATE_INFO_CACHE_HIT);
  }
  return ret;
}

int ObTruncateInfoKVCache::put_truncate_info_array(const ObTruncateInfoCacheKey &key, ObTruncateInfoCacheValue &value)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!key.is_valid() || !value.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(key), K(value));
  } else if (OB_FAIL(put(key, value, false/*overwrite*/))) {
    LOG_WARN("fail to put truncate into cache", K(ret), K(key), K(value));
  } else {
    LOG_INFO("success to put truncate info cache", KR(ret), K(key), K(value));
  }
  return ret;
}
/*
* ObTruncateInfoKVCacheUtil
* */
int ObTruncateInfoKVCacheUtil::get_truncate_info_array(
    ObIAllocator &allocator,
    const ObTruncateInfoCacheKey &cache_key,
    storage::ObTruncateInfoArray &input_array)
{
  int ret = OB_SUCCESS;
  storage::ObTruncateInfoKVCache &truncate_info_cache = ObStorageCacheSuite::get_instance().get_truncate_info_cache();
  ObTruncateInfoValueHandle cache_handle;
  if (OB_SUCC(truncate_info_cache.get_truncate_info_array(cache_key, cache_handle))) {
    if (OB_UNLIKELY(!cache_handle.is_valid())) {
      ret = OB_INVALID_DATA;
      LOG_WARN("invalid data from truncate info kv cache", KR(ret), K(cache_key), K(cache_handle));
    } else if (OB_FAIL(input_array.init_with_kv_cache_array(
        allocator, ObArrayWrap<ObTruncateInfo>(cache_handle.value_->get_truncate_info_array(), cache_handle.value_->get_count())))) {
      LOG_WARN("failed to init truncate info array", KR(ret), K(cache_key), K(cache_handle));
    }
  }
  return ret;
}

int ObTruncateInfoKVCacheUtil::put_truncate_info_array(
    const ObTruncateInfoCacheKey &cache_key,
    ObIArray<ObTruncateInfo *> &distinct_array)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator temp_allocator(ObMemAttr(MTL_ID(), "CopyTrunValue"));
  ObTruncateInfoCacheValue cache_value;
  if (OB_UNLIKELY(!cache_key.is_valid() || distinct_array.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(cache_key), K(distinct_array));
  } else if (1 == distinct_array.count()) {
    if (OB_FAIL(cache_value.init(distinct_array.count(), distinct_array.at(0)))) {
      LOG_WARN("failed to init truncate info value", KR(ret), K(distinct_array));
    }
  } else {
    const int64_t count = distinct_array.count();
    void *buf = nullptr;
    ObTruncateInfo *dst_array = nullptr;
    if (OB_ISNULL(buf = temp_allocator.alloc(count * sizeof(ObTruncateInfo)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory", KR(ret), K(count));
    } else {
      dst_array = new(buf) ObTruncateInfo[count];
      for (int64_t idx = 0; OB_SUCC(ret) && idx < count; ++idx) {
        ObTruncateInfo *info = distinct_array.at(idx);
        if (OB_ISNULL(info)) {
          ret = OB_INVALID_DATA;
          LOG_WARN("invalid truncate info", KR(ret), K(idx), KPC(info));
        } else if (OB_FAIL(info->shallow_copy(dst_array[idx]))) {
          LOG_WARN("failed to shallow copy truncate info", KR(ret), K(idx), KPC(info));
        }
      } // for
    }
    if (FAILEDx(cache_value.init(count, dst_array))) {
      LOG_WARN("failed to init truncate info value", KR(ret), K(distinct_array));
    }
  }
  storage::ObTruncateInfoKVCache &truncate_info_cache = ObStorageCacheSuite::get_instance().get_truncate_info_cache();
  if (FAILEDx(truncate_info_cache.put_truncate_info_array(cache_key, cache_value))) {
    if (OB_ENTRY_EXIST != ret) {
      LOG_WARN("failed to put truncate info cache", KR(ret), K(cache_key));
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
