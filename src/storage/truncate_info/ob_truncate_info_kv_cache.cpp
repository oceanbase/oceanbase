// Copyright (c) 2025 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.

#include "share/rc/ob_tenant_base.h"
#include "storage/blocksstable/ob_storage_cache_suite.h"
#include "storage/truncate_info/ob_truncate_info_kv_cache.h"
#include "storage/compaction_ttl/ob_ttl_filter_info_kv_cache.h"

#define USING_LOG_PREFIX STORAGE

namespace oceanbase
{
using namespace common;
using namespace blocksstable;
namespace storage
{

template <MDSInfoType type>
ObMDSInfoCacheKeyImpl<type>::ObMDSInfoCacheKeyImpl(const uint64_t tenant_id,
                                               const ObTabletID &tablet_id,
                                               const int64_t commit_version,
                                               const int64_t last_major_snapshot)
    : ObMDSInfoCacheKey(type),
      tenant_id_(tenant_id),
      tablet_id_(tablet_id),
      commit_version_(commit_version),
      last_major_snapshot_(last_major_snapshot)
{
}

template <MDSInfoType type>
bool ObMDSInfoCacheKeyImpl<type>::is_valid() const
{
  return tenant_id_ > 0 && tablet_id_.is_valid() && commit_version_ > 0 && last_major_snapshot_ > 0;
}

template <MDSInfoType type>
int ObMDSInfoCacheKeyImpl<type>::equal(const ObIKVCacheKey &other, bool &equal) const
{
  int ret = OB_SUCCESS;

  if (static_cast<const ObMDSInfoCacheKey &>(other).type_ != type) {
    equal = false;
  } else {
    const ObMDSInfoCacheKeyImpl<type> &other_key = static_cast<const ObMDSInfoCacheKeyImpl<type> &>(other);

    if (OB_UNLIKELY(!is_valid() || !other_key.is_valid())) {
      ret = OB_INVALID_DATA;
      LOG_WARN("invalid data", KR(ret), KPC(this), K(other_key));
    } else {
      equal = tenant_id_ == other_key.tenant_id_
            && tablet_id_ == other_key.tablet_id_
            && commit_version_ == other_key.commit_version_
            && last_major_snapshot_ == other_key.last_major_snapshot_;
    }
  }

  return ret;
}

template <MDSInfoType type>
int ObMDSInfoCacheKeyImpl<type>::hash(uint64_t &hash_val) const
{
  int ret = OB_SUCCESS;

  hash_val = murmurhash(&type_, sizeof(type_), 0);
  hash_val = murmurhash(&tenant_id_, sizeof(tenant_id_), hash_val);
  hash_val = murmurhash(&tablet_id_, sizeof(tablet_id_), hash_val);
  hash_val = murmurhash(&commit_version_, sizeof(commit_version_), hash_val);
  hash_val = murmurhash(&last_major_snapshot_, sizeof(last_major_snapshot_), hash_val);

  return ret;
}

template <MDSInfoType type>
int ObMDSInfoCacheKeyImpl<type>::deep_copy(char *buf, const int64_t buf_len, ObIKVCacheKey *&key) const
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(nullptr == buf || buf_len < size())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(buf), K(buf_len), "request_size", size());
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid truncate info cache key", KR(ret), K(*this));
  } else {
    key = new (buf) ObMDSInfoCacheKeyImpl<type>(tenant_id_, tablet_id_, commit_version_, last_major_snapshot_);
  }

  return ret;
}

template <typename T>
int ObMDSArrayCacheValue<T>::init(const int64_t count, T *mds_info_array)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(count <= 0 || nullptr == mds_info_array)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", KR(ret), K(count), KP(mds_info_array));
  } else {
    deep_copy_size_ = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < count; i++) {
      if (OB_UNLIKELY(!mds_info_array[i].is_valid())) {
        ret = OB_INVALID_DATA;
        LOG_WARN("Invalid data to init mds info array", KR(ret), K(i), K(mds_info_array[i]));
      } else {
        deep_copy_size_ += mds_info_array[i].get_deep_copy_size();
      }
    }
    if (OB_SUCC(ret)) {
      mds_info_array_ = mds_info_array;
      count_ = count;
      deep_copy_size_ += sizeof(T) * count_;
    }
  }

  return ret;
}

template <typename T>
int ObMDSArrayCacheValue<T>::deep_copy(char *buf, const int64_t buf_len, ObIKVCacheValue *&value) const
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(nullptr == buf || buf_len < size())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid arguments", KR(ret), KP(buf), K(buf_len), "request_size", size());
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_DATA;
    LOG_WARN("Invalid mds info array", KR(ret));
  } else {
    int64_t pos = 0;
    ObMDSArrayCacheValue<T> *pfuse_value = new (buf) ObMDSArrayCacheValue<T>();
    pos += sizeof(*this);
    pfuse_value->mds_info_array_ = new (buf + pos) T[count_];
    pfuse_value->count_ = count_;
    pfuse_value->deep_copy_size_ = deep_copy_size_;
    pos += sizeof(T) * count_;
    for (int64_t i = 0; OB_SUCC(ret) && i < count_; ++i) {
      if (OB_FAIL(mds_info_array_[i].deep_copy(buf, buf_len, pos, pfuse_value->mds_info_array_[i]))) {
        LOG_WARN("Fail to deep copy mds info", KR(ret), K(i), K(mds_info_array_[i]));
      }
    }

    if (OB_SUCC(ret)) {
      value = pfuse_value;
    } else if (nullptr != pfuse_value) {
      pfuse_value->~ObMDSArrayCacheValue<T>();
      pfuse_value = nullptr;
    }
  }

  return ret;
}

template <typename Key, typename MDSInfo>
int ObMDSInfoKVCacheUtil<Key, MDSInfo>::get_mds_info_array(ObIAllocator &allocator,
                                                           const Key &key,
                                                           MDSInfoArray &mds_info_array)
{
  int ret = OB_SUCCESS;

  CacheValueHandle cache_handle;
  if (OB_SUCC(get_mds_info_kvcache().get_mds_info_array(key, cache_handle))) {
    if (OB_UNLIKELY(!cache_handle.is_valid())) {
      ret = OB_INVALID_DATA;
      LOG_WARN("Invalid data from mds info kv cache", KR(ret), K(key), K(cache_handle));
    } else if (OB_FAIL(mds_info_array.init_with_kv_cache_array(
                   allocator,
                   ObArrayWrap<MDSInfo>(cache_handle.value_->get_array(),
                                        cache_handle.value_->get_count())))) {
      LOG_WARN("Fail to init mds info array", KR(ret), K(key), K(cache_handle));
    }
  }

  return ret;
}

template <typename Key, typename MDSInfo>
int ObMDSInfoKVCacheUtil<Key, MDSInfo>::put_mds_info_array(const Key &key,
                                                           ObIArray<MDSInfo *> &mds_info_array)
{
  int ret = OB_SUCCESS;

  ObArenaAllocator temp_allocator(ObMemAttr(MTL_ID(), "CopyMDSValue"));
  CacheValue cache_value;

  if (OB_UNLIKELY(!key.is_valid() || mds_info_array.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", KR(ret), K(key), K(mds_info_array));
  } else if (1 == mds_info_array.count()) {
    if (OB_FAIL(cache_value.init(mds_info_array.count(), mds_info_array.at(0)))) {
      LOG_WARN("Failed to init mds info value", KR(ret), K(mds_info_array));
    }
  } else {
    const int64_t count = mds_info_array.count();
    void *buf = nullptr;
    MDSInfo *dst_array = nullptr;
    if (OB_ISNULL(buf = temp_allocator.alloc(count * sizeof(MDSInfo)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Fail to alloc memory", KR(ret), K(count));
    } else {
      dst_array = new(buf) MDSInfo[count];
      for (int64_t idx = 0; OB_SUCC(ret) && idx < count; ++idx) {
        MDSInfo *info = mds_info_array.at(idx);
        if (OB_ISNULL(info)) {
          ret = OB_INVALID_DATA;
          LOG_WARN("Invalid mds info", KR(ret), K(idx), KPC(info));
        } else if (OB_FAIL(info->shallow_copy(dst_array[idx]))) {
          LOG_WARN("Fail to shallow copy mds info", KR(ret), K(idx), KPC(info));
        }
      }
    }
    if (FAILEDx(cache_value.init(count, dst_array))) {
      LOG_WARN("Fail to init mds info value", KR(ret), K(count), K(dst_array));
    }
  }

  if (FAILEDx(get_mds_info_kvcache().put_mds_info_array(key, cache_value))) {
    if (OB_ENTRY_EXIST != ret) {
      LOG_WARN("Fail to put mds info cache", KR(ret), K(key));
    }
  }

  return ret;
}

template <typename Key, typename MDSInfo>
ObMDSInfoKVCache<Key, MDSInfo> &ObMDSInfoKVCacheUtil<Key, MDSInfo>::get_mds_info_kvcache()
{
  ObMDSInfoKVCache<Key, MDSInfo> *mds_info_kv_cache = nullptr;

  if constexpr (std::is_same_v<MDSInfo, ObTruncateInfo>) {
    mds_info_kv_cache = &ObStorageCacheSuite::get_instance().get_truncate_info_cache();
  } else if constexpr (std::is_same_v<MDSInfo, ObTTLFilterInfo>) {
    mds_info_kv_cache = &ObStorageCacheSuite::get_instance().get_ttl_filter_info_cache();
  } else {
    static_assert(sizeof(MDSInfo) == 0, "Unsupported mds info type");
  }

  return *mds_info_kv_cache;
}

template class ObMDSInfoCacheKeyImpl<MDSInfoType::TRUNCATE_INFO>;
template class ObMDSArrayCacheValue<ObTruncateInfo>;
template class ObMDSArrayValueHandle<ObTruncateInfo>;
template class ObMDSInfoKVCache<ObTruncateInfoCacheKey, ObTruncateInfo>;
template class ObMDSInfoKVCacheUtil<ObTruncateInfoCacheKey, ObTruncateInfo>;

template class ObMDSInfoCacheKeyImpl<MDSInfoType::TTL_FILTER_INFO>;
template class ObMDSArrayCacheValue<ObTTLFilterInfo>;
template class ObMDSArrayValueHandle<ObTTLFilterInfo>;
template class ObMDSInfoKVCache<ObTTLFilterInfoCacheKey, ObTTLFilterInfo>;
template class ObMDSInfoKVCacheUtil<ObTTLFilterInfoCacheKey, ObTTLFilterInfo>;

} // namespace storage
} // namespace oceanbase
