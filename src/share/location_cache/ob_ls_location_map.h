/**
 * Copyright (c) 2021, 2022 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_SHARE_OB_LS_LOCATION_MAP
#define OCEANBASE_SHARE_OB_LS_LOCATION_MAP

#include "share/location_cache/ob_location_struct.h"
#include "lib/lock/ob_qsync_lock.h"

namespace oceanbase
{
namespace share
{
typedef common::hash::ObHashMap<ObLSLocationCacheKey, ObLSLocation> NoSwapCache;
typedef common::ObSEArray<ObLSLocation, 6> ObLSLocationArray;

struct ObTenantLSInfoKey
{
  ObTenantLSInfoKey() : tenant_id_(OB_INVALID_TENANT_ID), ls_id_() {}
  ObTenantLSInfoKey(const uint64_t tenant_id, const ObLSID &ls_id) : tenant_id_(tenant_id), ls_id_(ls_id) {}
  void reset()
  {
    tenant_id_ = OB_INVALID_TENANT_ID;
    ls_id_.reset();
  }
  bool operator==(const ObTenantLSInfoKey &key) const
  {
    return tenant_id_ == key.tenant_id_ && ls_id_ == key.ls_id_;
  }
  uint64_t hash() const
  {
    uint64_t hash_val = 0;
    hash_val = murmurhash(&tenant_id_, sizeof(tenant_id_), hash_val);
    hash_val = murmurhash(&ls_id_, sizeof(ls_id_), hash_val);
    return hash_val;
  }
  int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; }
  bool is_valid() const { return OB_INVALID_TENANT_ID != tenant_id_ && ls_id_.is_valid(); }
  TO_STRING_KV(K_(tenant_id), K_(ls_id));
  uint64_t tenant_id_;
  ObLSID ls_id_;
};

typedef common::hash::ObHashMap<ObTenantLSInfoKey, bool> ObTenantLsInfoHashMap;

class ObLSLocationMap
{
public:
  ObLSLocationMap()
      : is_inited_(false),
        size_(0),
        ls_buckets_(nullptr),
        buckets_lock_(nullptr)
  {
    destroy();
  }
  ~ObLSLocationMap() { destroy(); }
  void destroy();
  int init();
  int update(const bool from_rpc,
             const ObLSLocationCacheKey &key,
             ObLSLocation &ls_location);
  int get(const ObLSLocationCacheKey &key, ObLSLocation &location) const;
  int del(const ObLSLocationCacheKey &key, const int64_t safe_delete_time);
  int check_and_generate_dead_cache(ObLSLocationArray &arr);
  int get_all(ObLSLocationArray &arr);
  int64_t size() { return size_; }
private:
  static const int64_t MAX_ACCESS_TIME_UPDATE_THRESHOLD = 10000000;

private:
  bool is_inited_;
  int64_t size_;
  ObLSLocation **ls_buckets_;
  const static int64_t BUCKETS_CNT = 1 << 8;
  common::ObQSyncLock *buckets_lock_;
};


} // end namespace share
} // end namespace oceanbase
#endif
