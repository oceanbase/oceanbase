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

#ifndef OCEANBASE_SHARE_CATALOG_JDBC_CONNECTION_POOL_H_
#define OCEANBASE_SHARE_CATALOG_JDBC_CONNECTION_POOL_H_

#include "lib/hash/ob_hashmap.h"
#include "lib/lock/ob_thread_cond.h"
#include "lib/task/ob_timer.h"
#include "plugin/external_table/ob_jdbc_catalog_plugin.h"

namespace oceanbase
{
namespace share
{
struct ObJdbcConnectionPoolKey
{
  ObJdbcConnectionPoolKey() = default;
  ObJdbcConnectionPoolKey(uint64_t tenant_id, uint64_t catalog_id, int64_t schema_version)
      : tenant_id_(tenant_id),
        catalog_id_(catalog_id),
        catalog_schema_version_(schema_version)
  { }
  uint64_t hash() const;
  int hash(uint64_t &value) const
  {
    value = hash();
    return common::OB_SUCCESS;
  }
  bool operator==(const ObJdbcConnectionPoolKey &other) const;
  uint64_t tenant_id_ = OB_INVALID_TENANT_ID;
  uint64_t catalog_id_ = OB_INVALID_ID;
  int64_t catalog_schema_version_ = 0;
  TO_STRING_KV(K_(tenant_id), K_(catalog_id), K_(catalog_schema_version));
};

struct ObJdbcConnectionOptions
{
  ObJdbcConnectionPoolKey key_;
  common::ObString jdbc_url_;
  common::ObString plugin_name_;
  // Do not add TO_STRING: connection properties contain credentials.
};

struct ObJdbcConnectionPoolStats
{
  ObJdbcConnectionPoolKey key_;
  int64_t idle_ = 0;
  int64_t in_use_ = 0;
  int64_t creating_ = 0;
  int64_t closing_ = 0;
  int64_t waiters_ = 0;
  int64_t connected_ = 0;
  int64_t reused_ = 0;
  int64_t wait_timeouts_ = 0;
  int64_t evicted_ = 0;
  int64_t total() const { return idle_ + in_use_ + creating_ + closing_; }
  TO_STRING_KV(K_(key),
               K_(idle),
               K_(in_use),
               K_(creating),
               K_(closing),
               K_(waiters),
               K_(connected),
               K_(reused),
               K_(wait_timeouts),
               K_(evicted));
};

struct ObJdbcConnection
{
  plugin::ObJavaGlobalRef connection_;
  int64_t connection_id_ = 0;
  int64_t returned_at_ = 0;
};

class ObJdbcConnectionPool;
class ObJdbcConnectionGuard
{
public:
  ObJdbcConnectionGuard() = default;
  ~ObJdbcConnectionGuard() { release(); }
  bool is_valid() const { return OB_NOT_NULL(connection_); }
  jobject connection() const;
  plugin::ObJdbcCatalogPlugin *catalog_plugin() const;
  void mark_reusable() { reusable_ = true; }
  int release();
private:
  friend class ObJdbcConnectionPool;
  ObJdbcConnectionPool *pool_ = nullptr;
  ObJdbcConnection *connection_ = nullptr;
  bool reusable_ = false;
  DISABLE_COPY_ASSIGN(ObJdbcConnectionGuard);
};

class ObJdbcConnectionPool
{
public:
  static constexpr int64_t MAX_CONNECTIONS = 20;
  static constexpr int64_t CHECK_INTERVAL_US = 100 * 1000;
  static constexpr int64_t IDLE_CONNECTION_US = 5L * 60 * 1000 * 1000;
  static constexpr int64_t EMPTY_POOL_US = 10L * 60 * 1000 * 1000;
  ObJdbcConnectionPool() = default;
  ~ObJdbcConnectionPool();
  int init(const ObJdbcConnectionOptions &options);
  int acquire(const common::ObString &jdbc_url, int64_t deadline_ts, ObJdbcConnectionGuard &guard);
  void drain(bool stopping = false); // Changes state and wakes waiters; never calls JDBC.
  void cleanup();                   // Caller pins the pool while JDBC closes outside the lock.
  void snapshot(ObJdbcConnectionPoolStats &stats) const;
  bool removable() const;           // Called under the manager lock, which owns the last reference.
  void inc_ref() { ATOMIC_INC(&refs_); }
  void dec_ref() { ATOMIC_DEC(&refs_); }
  int64_t refs() const { return ATOMIC_LOAD(&refs_); }
  const ObJdbcConnectionPoolKey &key() const { return stats_.key_; }
  TO_STRING_KV(K(stats_.key_), "refs", refs());
private:
  friend class ObJdbcConnectionGuard;
  int release(ObJdbcConnection *connection, bool reusable);
  int close(ObJdbcConnection *connection, const char *reason);
  void refresh_empty_time(); // Requires cond_.
  enum State { ACTIVE, DRAINING, STOPPED };
  mutable common::ObThreadCond cond_;
  State state_ = ACTIVE;
  bool inited_ = false;
  int64_t refs_ = 1; // Manager reference; borrowers and maintenance pin independently.
  int64_t empty_since_ = 0;
  ObJdbcConnection *idle_[MAX_CONNECTIONS] = {};
  ObJdbcConnectionPoolStats stats_;
  plugin::ObJdbcCatalogPlugin catalog_plugin_;
  DISALLOW_COPY_AND_ASSIGN(ObJdbcConnectionPool);
};

class ObJdbcConnectionPoolMgr
{
public:
  ObJdbcConnectionPoolMgr();
  ~ObJdbcConnectionPoolMgr() { destroy(); }
  static int mtl_init(ObJdbcConnectionPoolMgr *&mgr);
  static void mtl_stop(ObJdbcConnectionPoolMgr *&mgr);
  static void mtl_wait(ObJdbcConnectionPoolMgr *&mgr);
  int acquire(const ObJdbcConnectionPoolKey &key, int64_t deadline_ts, ObJdbcConnectionGuard &guard);
  int get_stats(common::ObIArray<ObJdbcConnectionPoolStats> &stats);
  void cleanup_idle_pools();
  void stop();
  void wait();
  void destroy();
private:
  class ClearTask : public common::ObTimerTask
  {
  public:
    explicit ClearTask(ObJdbcConnectionPoolMgr &mgr) : mgr_(mgr) {}
    void runTimerTask() override { mgr_.cleanup_idle_pools(); }
  private:
    ObJdbcConnectionPoolMgr &mgr_;
  };
  int init(uint64_t tenant_id);
  int load_options(const ObJdbcConnectionPoolKey &key,
                   common::ObIAllocator &allocator,
                   ObJdbcConnectionOptions &options,
                   int64_t &current_version);
  int validate(const ObJdbcConnectionOptions &options, int64_t &current_version);
  int get_pool(const ObJdbcConnectionOptions &options, ObJdbcConnectionPool *&pool);
  int pin_pools(common::ObIArray<ObJdbcConnectionPool *> &pools);
  void retire_versions(const ObJdbcConnectionPoolKey &key, int64_t current_version);
  void finish_operation();
  common::ObThreadCond cond_;
  common::hash::ObHashMap<ObJdbcConnectionPoolKey, ObJdbcConnectionPool *> pools_;
  ClearTask clear_task_;
  uint64_t tenant_id_ = OB_INVALID_TENANT_ID;
  bool inited_ = false;
  bool stopped_ = false;
  int64_t operations_ = 0;
  int tg_id_ = -1;
  DISALLOW_COPY_AND_ASSIGN(ObJdbcConnectionPoolMgr);
};
} // namespace share
} // namespace oceanbase
#endif
