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

#ifndef OCEANBASE_SHARE_OB_MAX_ID_CACHE_H_
#define OCEANBASE_SHARE_OB_MAX_ID_CACHE_H_

#include "deps/oblib/src/lib/hash/ob_hashmap.h"
#include "src/share/ob_max_id_fetcher.h"

namespace oceanbase
{
namespace share
{

class ObMaxIdCacheItem
{
public:
  ObMaxIdCacheItem(const ObMaxIdType &type, const uint64_t tenant_id);
  int fetch_max_id(const uint64_t tenant_id, const ObMaxIdType max_id_type, uint64_t &id,
      const uint64_t size, ObMySQLProxy *sql_proxy);
private:
  int fetch_ids_from_inner_table_(const uint64_t size, ObMySQLProxy *sql_proxy);
  int fetch_ids_by_cache_without_lock_(const uint64_t size, uint64_t &id);
  bool cached_id_valid_();
private:
  static const uint64_t CACHE_SIZE = 1024;
  static const int64_t FETCH_RETRY_INTERVAL_US = 10 * 1000; // 10ms retry interval
  static const int64_t MAX_FETCH_RETRY_CNT = 10;            // max retry count before bypass lock
private:
  // [min_id, min_id + size) is valid
  uint64_t min_id_;
  uint64_t size_;
  uint64_t tenant_id_;
  ObMaxIdType type_;
  common::ObLatch latch_;              // protect cache allocation
  common::ObLatch fetch_latch_;        // serialize inner table fetch
};

class ObMaxIdCache
{
public:
  explicit ObMaxIdCache(const uint64_t tenant_id);
  int fetch_max_id(const uint64_t tenant_id, const ObMaxIdType max_id_type, uint64_t &id,
      const uint64_t size, ObMySQLProxy *sql_proxy);
private:
  uint64_t tenant_id_;
  ObMaxIdCacheItem object_id_cache_;
  ObMaxIdCacheItem normal_rowid_table_tablet_id_cache_;
  ObMaxIdCacheItem extended_rowid_table_tablet_id_cache_;
};

class ObMaxIdCacheMgr
{
public:
  int init(ObMySQLProxy *sql_proxy);
  void reset();
  // return [id, id + size - 1)
  int fetch_max_id(const uint64_t tenant_id, const ObMaxIdType max_id_type, uint64_t &id,
      const uint64_t size, bool init_tenant_if_not_exist = true);
  ObMaxIdCacheMgr();
  ~ObMaxIdCacheMgr();
private:
  int add_tenant_(const uint64_t tenant_id);
  // switch rs will clean redundant tenant
  int remove_cache_(ObMaxIdCache *cache);
private:
  using hashmap = hash::ObHashMap<uint64_t, ObMaxIdCache *>;
  hashmap tenant_caches_; // tenant_id to cache
  ObMemAttr attr_;
  ObArenaAllocator allocator_;
  bool inited_;
  common::ObLatch latch_;
  ObMySQLProxy *sql_proxy_;
};

}
}


#endif // OCEANBASE_SHARE_OB_MAX_ID_CACHE_H_
