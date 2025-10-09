/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef _SHARE_CATALOG_HIVE_OB_HIVE_CLIENT_POOL_H
#define _SHARE_CATALOG_HIVE_OB_HIVE_CLIENT_POOL_H

#include "lib/alloc/alloc_assist.h"
#include "lib/list/ob_list.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/lock/ob_mutex.h"
#include "lib/rc/context.h"
#include "lib/task/ob_timer.h"
#include "lib/lock/ob_thread_cond.h"
#include "lib/atomic/ob_atomic.h"

namespace oceanbase
{
namespace observer
{
  class ObHMSClientPoolGetter;  // Forward declaration
}
namespace share
{

// Forward declaration to avoid header dependency
class ObHiveMetastoreClient;

// Using common namespace for ObThreadCond
using common::ObThreadCond;

static constexpr int64_t MAX_HMS_CATALOG_CONNECTIONS = 100;
static constexpr int64_t DEFAULT_HMS_CLIENT_POOL_SIZE = 20;
static constexpr int64_t DEFAULT_HMS_CLIENT_SOCKET_TIMEOUT_US = 10LL * 1000LL * 1000LL; // 10 seconds
static constexpr int64_t MAX_POOL_SIZE = 100;
static constexpr int64_t POOL_CLEANUP_INTERVAL_US = 5LL * 60LL * 1000LL * 1000LL; // 5 minutes
static constexpr int64_t POOL_IDLE_THRESHOLD_US = 5LL * 60LL * 1000LL * 1000LL; // 5 minutes
static constexpr int64_t POOL_REMOVAL_THRESHOLD_US = 10LL * 60LL * 1000LL * 1000LL;  // 10 minutes
static constexpr char HMS_CLIENT_POOL_ALLOCATOR[] = "HMSPoolAlloc";
static constexpr char HMS_CLIENT_POOL_MGR_ALLOCATOR[] = "HMSMgrAlloc";

typedef ObList<ObHiveMetastoreClient *, ObIAllocator> HMSClientList;

/* --------------------- start of ObHMSClientPoolKey ---------------------*/

struct ObHMSClientPoolKey final
{
public:
  ObHMSClientPoolKey() : tenant_id_(OB_INVALID_TENANT_ID), catalog_id_(OB_INVALID_ID) {}
  ObHMSClientPoolKey(const uint64_t &tenant_id, const uint64_t &catalog_id)
    : tenant_id_(tenant_id), catalog_id_(catalog_id) {}
  ~ObHMSClientPoolKey() {}

  uint64_t do_hash() const;
  int hash(uint64_t &hash_val) const;
  bool operator==(const ObHMSClientPoolKey &other) const;
  int assign(const ObHMSClientPoolKey &other);

  TO_STRING_KV(K_(tenant_id), K_(catalog_id));

private:
  uint64_t tenant_id_;
  uint64_t catalog_id_;
};

/* --------------------- end of ObHMSClientPoolKey ---------------------*/

class ObHMSClientPoolMgr;

class ObHMSClientPool
{
public:
  static const int64_t WAIT_INTERVAL_US = 100 * 1000; // 100ms

public:
  ObHMSClientPool();
  ~ObHMSClientPool();

  int init(const uint64_t &tenant_id,
           const uint64_t &catalog_id,
           const ObString &uri,
           const ObString &properties,
           ObIAllocator *allocator);

  int get_client(ObHiveMetastoreClient *&client);

  int return_client(ObHiveMetastoreClient *&client);

  void get_pool_stats(int64_t &total_clients, int64_t &in_use_clients, int64_t &idle_clients) const;

  int cleanup();
  int64_t get_waiting_threads() const;
  void refresh_last_access_ts();

  bool is_inited() const { return is_inited_; }

  uint64_t get_tenant_id() const { return tenant_id_; }

  uint64_t get_catalog_id() const { return catalog_id_; }

  int64_t get_last_access_ts() const { return last_access_ts_; }

private:
  // Note: this function is not thread safe, so we need to call it in a thread safe way.
  // Currently, we only use it in get_client, so we can use a global lock to protect it.
  bool is_pool_full_unlocked_();
  bool idle_clients_available_unlocked_();
  bool can_create_client_unlocked_();
  bool can_destroy_client_unlocked_();
  // Note: this function is not thread safe, so we need to call it in a thread safe way.
  // Currently, we only use it in get_client, so we can use a global lock to protect it.
  int create_new_client_unlocked_();

  // Helper methods for get_client
  int try_get_client_from_queue_unlocked_(ObHiveMetastoreClient *&client);
  int try_create_and_get_client_unlocked_(ObHiveMetastoreClient *&client);
  void destroy_invalid_client_unlocked_(ObHiveMetastoreClient *client);

private:
  ObIAllocator *allocator_;
  uint64_t tenant_id_;
  uint64_t catalog_id_;
  ObString uri_;
  ObString properties_;

  HMSClientList *client_queue_;

  int64_t pool_size_;
  int64_t socket_timeout_;    // us
  int64_t total_clients_;
  int64_t in_use_clients_;
  volatile int64_t waiting_threads_;     // Number of threads waiting for clients (atomic access)

  mutable ObThreadCond pool_cond_;    // Thread condition for synchronization

  bool is_inited_;
  int64_t last_access_ts_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObHMSClientPool);
};


class ObHMSClientPoolIdleClearTask : public common::ObTimerTask
{
public:
  ObHMSClientPoolIdleClearTask() : pool_mgr_(NULL) {}
  void runTimerTask(void);

public:
  ObHMSClientPoolMgr *pool_mgr_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObHMSClientPoolIdleClearTask);
};

class ObHMSClientPoolMgr
{
public:
  ObHMSClientPoolMgr();
  ~ObHMSClientPoolMgr();

public:
  static int mtl_init(ObHMSClientPoolMgr *&node_list);
  static void mtl_stop(ObHMSClientPoolMgr *&node_list);
  static void mtl_wait(ObHMSClientPoolMgr *&node_list);
  void destroy();

  int get_client(const uint64_t &tenant_id,
                 const uint64_t &catalog_id,
                 const ObString &uri,
                 const ObString &properties,
                 ObHiveMetastoreClient *&client);

  int is_inited() const { return is_inited_; }
  uint64_t get_tenant_id() const { return tenant_id_; }
  int cleanup_idle_pools();
  int generate_pool_stat_monitoring_info_rows(observer::ObHMSClientPoolGetter &getter);

private:
  int get_or_create_pool(const uint64_t &tenant_id,
                         const uint64_t &catalog_id,
                         const ObString &uri,
                         const ObString &properties,
                         ObHMSClientPool *&pool);
  int init(const uint64_t &tenant_id);
  int init_mem_context(const uint64_t &tenant_id);
  int create_pool_map(const uint64_t &tenant_id);
  void destroy_pool_map();

private:
  lib::MemoryContext mem_context_;
  ObIAllocator *inner_allocator_;
  ObHMSClientPoolIdleClearTask clear_task_;
  bool is_inited_;
  bool destroyed_;
  uint64_t tenant_id_;
  int tg_id_;
  int64_t last_access_ts_;
  hash::ObHashMap<ObHMSClientPoolKey, ObHMSClientPool *> pool_map_;
  SpinRWLock pool_mgr_lock_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObHMSClientPoolMgr);
};

} // namespace share
} // namespace oceanbase

#endif /* _SHARE_CATALOG_HIVE_OB_HIVE_CLIENT_POOL_H */
