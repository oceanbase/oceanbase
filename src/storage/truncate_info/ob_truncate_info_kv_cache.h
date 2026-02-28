// Copyright (c) 2025 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.

#ifndef OB_STORAGE_TRUNCATE_INFO_TRUNCATE_INFO_KV_CACHE_H_
#define OB_STORAGE_TRUNCATE_INFO_TRUNCATE_INFO_KV_CACHE_H_

#include "share/cache/ob_kv_storecache.h"
#include "share/cache/ob_kvcache_struct.h"
#include "storage/compaction_ttl/ob_ttl_filter_info.h"
#include "storage/truncate_info/ob_truncate_info.h"
#include "storage/truncate_info/ob_truncate_info_array.h"

namespace oceanbase
{
namespace storage
{

/**
 * How to add new multi-source data required by the storage layer and provide KV cache support capabilities?
 * 1. Add new MDSInfo classes (refer to ObTruncateInfoKey, ObTruncateInfo), both are base types that need to be manually implemented
 * 2. Add new CacheKey (refer to ObTruncateInfoCacheKey), need to inherit ObMDSInfoCacheKey and implement specific cache key implementation
 *    - Must add new MDSInfoType enum values, because all MDSInfo are in one KVCache system, different MDS types need to be distinguished by type
 * 3. The following types need to be explicitly instantiated in the corresponding cpp files (i.e., template class .....) (limited by circular dependency issues, can only be manually instantiated in cpp)
 *    - ObMDSArrayCacheValue<MDSInfo>
 *    - ObMDSArrayValueHandle<MDSInfo>
 *    - ObMDSInfoKVCache<CacheKey, MDSInfo>
 *    - ObMDSInfoKVCacheUtil<CacheKey, MDSInfo>
 *
 *    - ObMDSInfoDistinctMgrImpl<MDSInfo, MDSInfoArray, CacheKey> This is a class used for deduplication of MDSInfoArray, need to manually implement build_distinct_array method in the class
 *    - ObTabletMDSInfoReader<Key, MDSInfo> This is a helper class used to read MDSInfo on tablet
 *
 * Class Relationship Diagram:
 *
 *                    ┌─────────────────────────────────────────────────────────────┐
 *                    │                    MDSInfo (Data Information)               │
 *                    │              (ObTruncateInfo, TTLFilterInfo, etc.)          │
 *                    └─────────────────────────────────────────────────────────────┘
 *                                                           │
 *                                                           │ Contains
 *                                                           ▼
 * ┌─────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
 * │                                    MDS KV Cache System Architecture                                         │
 * │                                                                                                             │
 * │  ┌─────────────────-┐    ┌─────────────────────────────────────────────────────────────────────────────┐    │
 * │  │ ObMDSInfoCacheKey│    │                    ObMDSInfoKVCacheWrapper                                  │    │
 * │  │  (Cache Key Base)│    │                    (MDS KV Cache Wrapper Base Class)                        │    │
 * │  └────────────────-─┘    └─────────────────────────────────────────────────────────────────────────────┘    │
 * │           │                                    │                                                            │
 * │           │ Inherits                           │ Inherits                                                   │
 * │           ▼                                    ▼                                                            │
 * │  ┌─────────────────────────────────┐    ┌─────────────────────────────────────────────────────────────┐     │
 * │  │ ObMDSInfoCacheKeyImpl<Type>     │    │              ObMDSInfoKVCache<Key, MDSInfo>                 │     │
 * │  │   (Specific Cache Key Impl)     │    │                (Specific KV Cache Implementation)           │     │
 * │  │ - type                          │    │                                                             │     │
 * │  │ - tenant_id                     │    │                                                             │     │
 * │  │ - tablet_id                     │    │  ┌─────────────────────────────────────────────────────────┐│     │
 * │  │ - commit_version                │    │  │              ObMDSInfoKVCacheUtil<Key, MDSInfo>         ││     │
 * │  │ - last_major_snapshot           │    │  │                (Cache Utility Class)                    ││     │
 * │  └─────────────────────────────────┘    │  └─────────────────────────────────────────────────────────┘│     │
 * │                                         └─────────────────────────────────────────────────────────────┘     │
 * │                                                                                                             │
 * │  ┌─────────────────────────────────────────────────────────────────────────────────────────────────────┐    │
 * │  │                              Cache Value Related Classes                                            │    │
 * │  │                                                                                                     │    │
 * │  │  ┌─────────────────────────────────┐    ┌─────────────────────────────────────────────────────────┐ │    │
 * │  │  │ ObMDSArrayCacheValue<MDSInfo>   │    │              ObMDSArrayValueHandle<MDSInfo>             │ │    │
 * │  │  │   (Cache Value Wrapper Class)   │    │                (Cache Value Handle)                     │ │    │
 * │  │  │ - count_                        │    │ - value_ (points to CacheValue)                         │ │    │
 * │  │  │ - mds_info_array_               │    │ - handle_ (ObKVCacheHandle)                             │ │    │
 * │  │  │ - deep_copy_size_               │    └─────────────────────────────────────────────────────────┘ │    │
 * │  │  └─────────────────────────────────┘                                                                │    │
 * │  └─────────────────────────────────────────────────────────────────────────────────────────────────────┘    │
 * └─────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
 *
 * Usage Flow:
 * 1. Get cache instance through ObMDSInfoKVCacheUtil
 * 2. Create cache key using ObMDSInfoCacheKeyImpl
 * 3. Perform get/put operations through ObMDSInfoKVCache
 * 4. Return ObMDSArrayValueHandle wrapped cache result
 *
 */

enum class MDSInfoType : uint8_t
{
  TRUNCATE_INFO = 0,
  TTL_FILTER_INFO = 1,
};

class ObMDSInfoCacheKey : public common::ObIKVCacheKey
{
public:
  ObMDSInfoCacheKey(const MDSInfoType type) : type_(type) {}
  virtual ~ObMDSInfoCacheKey() = default;

  MDSInfoType type_;
};

/**
 * @brief a very common mds info cache key implementation,
 *        used for truncate info and ttl filter info
 */
template <MDSInfoType type>
class ObMDSInfoCacheKeyImpl final : public ObMDSInfoCacheKey
{
public:
  ObMDSInfoCacheKeyImpl(const uint64_t tenant_id,
                        const ObTabletID &tablet_id,
                        const int64_t commit_version,
                        const int64_t last_major_snapshot);

  virtual ~ObMDSInfoCacheKeyImpl() = default;

  bool is_valid() const;
  virtual int equal(const ObIKVCacheKey &other, bool &equal) const override;
  virtual int hash(uint64_t &hash_value) const override;
  virtual uint64_t get_tenant_id() const override { return tenant_id_; }
  virtual int64_t size() const override { return sizeof(*this); }
  virtual int deep_copy(char *buf, const int64_t buf_len, ObIKVCacheKey *&key) const override;
  TO_STRING_KV(K_(tenant_id), K_(tablet_id), K_(commit_version), K_(last_major_snapshot));

private:
  uint64_t tenant_id_;
  ObTabletID tablet_id_;
  int64_t commit_version_;
  int64_t last_major_snapshot_;
};

template <typename MDSInfo>
class ObMDSArrayCacheValue final : public common::ObIKVCacheValue
{
public:
  ObMDSArrayCacheValue() : count_(0), mds_info_array_(nullptr), deep_copy_size_(0) {}
  virtual ~ObMDSArrayCacheValue() = default;
  OB_INLINE bool is_valid() const { return count_ > 0 && nullptr != mds_info_array_; }
  virtual int64_t size() const override { return sizeof(*this) + deep_copy_size_; }
  int init(const int64_t count, MDSInfo *mds_info_array);
  OB_INLINE const MDSInfo *get_array() const { return mds_info_array_; }
  OB_INLINE int64_t get_count() const { return count_; }
  virtual int deep_copy(char *buf, const int64_t buf_len, ObIKVCacheValue *&value) const override;
  TO_STRING_KV(K_(count),
               "array",
               ObArrayWrap<MDSInfo>(mds_info_array_, count_),
               K_(deep_copy_size));

private:
  int64_t count_;
  MDSInfo *mds_info_array_;
  int64_t deep_copy_size_;
};

template <typename MDSInfo>
class ObMDSArrayValueHandle final
{
public:
  using CacheValue = ObMDSArrayCacheValue<MDSInfo>;

public:
  ObMDSArrayValueHandle() : value_(nullptr), handle_() {}

  ~ObMDSArrayValueHandle() = default;

  bool is_valid() const { return nullptr != value_ && value_->is_valid() && handle_.is_valid(); }

  void reset()
  {
    value_ = nullptr;
    handle_.reset();
  }

  TO_STRING_KV(KP_(value), K_(handle));

  const CacheValue *value_;
  ObKVCacheHandle handle_;
};

class ObMDSInfoKVCacheWrapper : public ObKVCache<ObMDSInfoCacheKey, ObIKVCacheValue>
{
public:
  ObMDSInfoKVCacheWrapper() = default;
  virtual ~ObMDSInfoKVCacheWrapper() = default;
};

template <typename Key, typename MDSInfo>
class ObMDSInfoKVCache final : public ObMDSInfoKVCacheWrapper
{
public:
  using CacheValue = ObMDSArrayCacheValue<MDSInfo>;
  using CacheValueHandle = ObMDSArrayValueHandle<MDSInfo>;

public:
  ObMDSInfoKVCache() = default;
  virtual ~ObMDSInfoKVCache() = default;

  int init(const char *cache_name, const int64_t priority)
  {
    int ret = OB_SUCCESS;

    if (OB_FAIL(ObMDSInfoKVCacheWrapper::init(cache_name, priority))) {
      STORAGE_LOG(WARN, "Fail to init mds info kv cache", KR(ret), K(cache_name), K(priority));
    }

    return ret;
  }

  int get_mds_info_array(const Key &key, CacheValueHandle &handle)
  {
    int ret = OB_SUCCESS;

    const ObIKVCacheValue *value = nullptr;
    if (OB_UNLIKELY(!key.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "Invalid arguments", KR(ret), K(key));
    } else if (OB_FAIL(this->get(key, value, handle.handle_))) {
      if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
        STORAGE_LOG(WARN, "Fail to get mds info from cache", KR(ret), K(key));
      } else {
        EVENT_INC(ObStatEventIds::MDS_FILTER_INFO_CACHE_MISS);
      }
    } else if (OB_ISNULL(value)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Unexpected error, the value must not be NULL", KR(ret));
    } else {
      handle.value_ = static_cast<const CacheValue *>(value);
      EVENT_INC(ObStatEventIds::MDS_FILTER_INFO_CACHE_HIT);
    }

    return ret;
  }

  int put_mds_info_array(const Key &key, CacheValue &value)
  {
    int ret = OB_SUCCESS;

    if (OB_UNLIKELY(!key.is_valid() || !value.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "Invalid arguments", KR(ret), K(key), K(value));
    } else if (OB_FAIL(this->put(key, value, false /*overwrite*/))) {
      if (OB_ENTRY_EXIST != ret) {
        STORAGE_LOG(WARN, "Fail to put mds info into cache", KR(ret), K(key), K(value));
      }
    }

    return ret;
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObMDSInfoKVCache);
};

template <typename Key, typename MDSInfo>
class ObMDSInfoKVCacheUtil final
{
public:
  using CacheValue = ObMDSArrayCacheValue<MDSInfo>;
  using CacheValueHandle = ObMDSArrayValueHandle<MDSInfo>;
  using MDSInfoArray = ObMDSInfoArray<MDSInfo>;

public:
  static int get_mds_info_array(ObIAllocator &allocator,
                                const Key &key,
                                MDSInfoArray &mds_info_array);

  static int put_mds_info_array(const Key &key, ObIArray<MDSInfo *> &mds_info_array);

  static ObMDSInfoKVCache<Key, MDSInfo> &get_mds_info_kvcache();
};

using ObTruncateInfoCacheKey = ObMDSInfoCacheKeyImpl<MDSInfoType::TRUNCATE_INFO>;
using ObTruncateInfoCacheValue = ObMDSArrayCacheValue<ObTruncateInfo>;
using ObTruncateInfoCacheValueHandle = ObMDSArrayValueHandle<ObTruncateInfo>;
using ObTruncateInfoKVCache = ObMDSInfoKVCache<ObTruncateInfoCacheKey, ObTruncateInfo>;
using ObTruncateInfoKVCacheUtil = ObMDSInfoKVCacheUtil<ObTruncateInfoCacheKey, ObTruncateInfo>;

} // namespace storage
} // namespace oceanbase

#endif // OB_STORAGE_TRUNCATE_INFO_TRUNCATE_INFO_KV_CACHE_H_
