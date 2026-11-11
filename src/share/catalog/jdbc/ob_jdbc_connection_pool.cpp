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

#define USING_LOG_PREFIX SHARE
#include "share/catalog/jdbc/ob_jdbc_connection_pool.h"
#include "lib/alloc/alloc_assist.h"
#include "lib/thread/thread_mgr.h"
#include "lib/time/ob_time_utility.h"
#include "lib/worker.h"
#include "observer/ob_server_struct.h"
#include "share/catalog/ob_catalog_properties.h"
#include "share/rc/ob_tenant_base.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"

namespace oceanbase
{
namespace share
{
using namespace common;

uint64_t ObJdbcConnectionPoolKey::hash() const
{
  uint64_t value = murmurhash(&tenant_id_, sizeof(tenant_id_), 0);
  value = murmurhash(&catalog_id_, sizeof(catalog_id_), value);
  return murmurhash(&catalog_schema_version_, sizeof(catalog_schema_version_), value);
}

bool ObJdbcConnectionPoolKey::operator==(const ObJdbcConnectionPoolKey &other) const
{
  return tenant_id_ == other.tenant_id_ && catalog_id_ == other.catalog_id_
         && catalog_schema_version_ == other.catalog_schema_version_;
}

jobject ObJdbcConnectionGuard::connection() const
{
  return OB_NOT_NULL(connection_) ? connection_->connection_.handle() : nullptr;
}

plugin::ObJdbcCatalogPlugin *ObJdbcConnectionGuard::catalog_plugin() const
{
  return OB_NOT_NULL(pool_) ? &pool_->catalog_plugin_ : nullptr;
}

int ObJdbcConnectionGuard::release()
{
  int ret = OB_SUCCESS;
  ObJdbcConnectionPool *pool = pool_;
  ObJdbcConnection *connection = connection_;
  const bool reusable = reusable_;
  pool_ = nullptr;
  connection_ = nullptr;
  reusable_ = false;
  if (OB_NOT_NULL(pool)) {
    if (OB_NOT_NULL(connection)) {
      ret = pool->release(connection, reusable);
    }
    pool->dec_ref();
  }
  return ret;
}

ObJdbcConnectionPool::~ObJdbcConnectionPool()
{
  if (inited_) {
    drain(true);
    cleanup();
    OB_ASSERT(stats_.total() == 0 && refs() == 1);
  }
}

int ObJdbcConnectionPool::init(const ObJdbcConnectionOptions &options)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
  } else if (OB_FAIL(cond_.init(ObWaitEventIds::CATALOG_CLIENT_POOL_COND_WAIT))) {
  } else if (OB_FAIL(catalog_plugin_.init(options.plugin_name_))) {
    LOG_WARN("failed to initialize jdbc pool bridge", K(ret), K(options.key_));
  } else {
    stats_.key_ = options.key_;
    empty_since_ = ObTimeUtility::current_time();
    inited_ = true;
  }
  return ret;
}

int ObJdbcConnectionPool::acquire(const ObString &jdbc_url, int64_t deadline_ts, ObJdbcConnectionGuard &guard)
{
  int ret = OB_SUCCESS;
  ObJdbcConnection *connection = nullptr;
  bool creating = false;
  bool waited = false;
  inc_ref(); // Transfers to guard on success; covers waits and JNI creation on failure.
  if (!inited_) {
    ret = OB_NOT_INIT;
  } else if (guard.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    ObThreadCondGuard lock(cond_);
    while (OB_SUCC(ret) && OB_ISNULL(connection) && !creating) {
      if (STOPPED == state_) {
        ret = OB_SERVER_IS_STOPPING;
      } else if (DRAINING == state_) {
        ret = OB_SCHEMA_EAGAIN;
      } else if (ObTimeUtility::current_time() >= deadline_ts) {
        ret = OB_TIMEOUT;
      } else if (OB_FAIL(THIS_WORKER.check_status())) {
      } else if (stats_.idle_ > 0) {
        connection = idle_[--stats_.idle_];
        idle_[stats_.idle_] = nullptr;
        ++stats_.in_use_;
        ++stats_.reused_;
      } else if (stats_.total() < MAX_CONNECTIONS) {
        ++stats_.creating_;
        empty_since_ = 0;
        creating = true;
      } else {
        waited = true;
        ++stats_.waiters_;
        const int64_t remaining = deadline_ts - ObTimeUtility::current_time();
        if (remaining > 0) {
          const int wait_ret = cond_.wait_us(std::min(CHECK_INTERVAL_US, remaining));
          if (OB_SUCCESS != wait_ret && OB_TIMEOUT != wait_ret) {
            ret = wait_ret;
          }
        }
        --stats_.waiters_;
      }
    }
    if (OB_TIMEOUT == ret && waited) {
      ++stats_.wait_timeouts_;
    }
  }
  if (OB_SUCC(ret) && creating) {
    void *buf = ob_malloc(sizeof(ObJdbcConnection), ObMemAttr(key().tenant_id_, "JdbcConn"));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      connection = new (buf) ObJdbcConnection();
      static int64_t next_connection_id = 0;
      connection->connection_id_ = ATOMIC_AAF(&next_connection_id, 1);
      ret = catalog_plugin_.connect(jdbc_url, connection->connection_);
    }
    {
      ObThreadCondGuard lock(cond_);
      --stats_.creating_;
      if (OB_SUCC(ret)) {
        ++stats_.connected_;
        if (STOPPED == state_) {
          ret = OB_SERVER_IS_STOPPING;
        } else if (DRAINING == state_) {
          ret = OB_SCHEMA_EAGAIN;
        } else if (ObTimeUtility::current_time() >= deadline_ts) {
          ret = OB_TIMEOUT;
        } else {
          ret = THIS_WORKER.check_status();
        }
      } else if (ObTimeUtility::current_time() >= deadline_ts) {
        // A driver error can arrive after its own network timeout, later than the SQL deadline.
        ret = OB_TIMEOUT;
      }
      if (OB_SUCC(ret)) {
        ++stats_.in_use_;
      } else if (OB_NOT_NULL(connection)) {
        ++stats_.closing_;
      }
      refresh_empty_time();
      cond_.broadcast();
    }
    if (OB_FAIL(ret) && OB_NOT_NULL(connection)) {
      close(connection, "create failed, expired or draining");
      connection = nullptr;
    }
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(connection)) {
    guard.pool_ = this;
    guard.connection_ = connection;
    guard.reusable_ = false;
    LOG_INFO("jdbc pool acquire", K(stats_.key_), K(connection->connection_id_), K(creating));
  } else {
    dec_ref();
  }
  return ret;
}

int ObJdbcConnectionPool::release(ObJdbcConnection *connection, bool reusable)
{
  int ret = OB_SUCCESS;
  bool keep = false;
  if (OB_NOT_NULL(connection)) {
    {
      ObThreadCondGuard lock(cond_);
      --stats_.in_use_;
      keep = reusable && ACTIVE == state_;
      if (keep) {
        connection->returned_at_ = ObTimeUtility::current_time();
        idle_[stats_.idle_++] = connection;
        LOG_INFO("jdbc pool return", K(stats_.key_), K(connection->connection_id_));
      } else {
        ++stats_.closing_;
      }
      cond_.broadcast();
    }
    if (!keep) {
      ret = close(connection, reusable ? "draining" : "operation or cursor failure");
    }
  }
  return ret;
}

int ObJdbcConnectionPool::close(ObJdbcConnection *connection, const char *reason)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(connection)) {
    ret = catalog_plugin_.disconnect(connection->connection_);
    // The physical disconnect is attempted first, including failed connect/promotion paths.
    connection->connection_.clear();
    LOG_INFO("jdbc pool evict", K(ret), K(stats_.key_), K(connection->connection_id_), KCSTRING(reason));
    connection->~ObJdbcConnection();
    ob_free(connection);
    ObThreadCondGuard lock(cond_);
    --stats_.closing_;
    ++stats_.evicted_;
    refresh_empty_time();
    cond_.broadcast();
  }
  return ret;
}

void ObJdbcConnectionPool::refresh_empty_time()
{
  if (stats_.total() == 0 && empty_since_ == 0) {
    empty_since_ = ObTimeUtility::current_time();
  }
}

void ObJdbcConnectionPool::drain(bool stopping)
{
  if (inited_) {
    ObThreadCondGuard lock(cond_);
    if (stopping) {
      state_ = STOPPED;
    } else if (ACTIVE == state_) {
      state_ = DRAINING;
    }
    cond_.broadcast();
  }
}

void ObJdbcConnectionPool::cleanup()
{
  ObJdbcConnection *closing[MAX_CONNECTIONS] = {};
  int64_t count = 0;
  {
    ObThreadCondGuard lock(cond_);
    for (int64_t i = stats_.idle_ - 1; i >= 0; --i) {
      ObJdbcConnection *connection = idle_[i];
      if (OB_NOT_NULL(connection)
          && (ACTIVE != state_ || ObTimeUtility::current_time() - connection->returned_at_ >= IDLE_CONNECTION_US)) {
        closing[count++] = connection;
        idle_[i] = idle_[--stats_.idle_];
        idle_[stats_.idle_] = nullptr;
        ++stats_.closing_;
      }
    }
  }
  for (int64_t i = 0; i < count; ++i) {
    close(closing[i], "idle expiry or draining");
  }
}

void ObJdbcConnectionPool::snapshot(ObJdbcConnectionPoolStats &stats) const
{
  ObThreadCondGuard lock(cond_);
  stats = stats_;
}

bool ObJdbcConnectionPool::removable() const
{
  ObThreadCondGuard lock(cond_);
  return refs() == 1 && stats_.total() == 0 && stats_.waiters_ == 0
      && (ACTIVE != state_ || (empty_since_ > 0 && ObTimeUtility::current_time() - empty_since_ >= EMPTY_POOL_US));
}

ObJdbcConnectionPoolMgr::ObJdbcConnectionPoolMgr() : clear_task_(*this) {}

int ObJdbcConnectionPoolMgr::mtl_init(ObJdbcConnectionPoolMgr *&mgr)
{
  return OB_NOT_NULL(mgr) ? mgr->init(MTL_ID()) : OB_INVALID_ARGUMENT;
}

void ObJdbcConnectionPoolMgr::mtl_stop(ObJdbcConnectionPoolMgr *&mgr)
{
  if (OB_NOT_NULL(mgr)) {
    mgr->stop();
  }
}

void ObJdbcConnectionPoolMgr::mtl_wait(ObJdbcConnectionPoolMgr *&mgr)
{
  if (OB_NOT_NULL(mgr)) {
    mgr->wait();
  }
}

int ObJdbcConnectionPoolMgr::init(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  tenant_id_ = tenant_id;
  if (OB_FAIL(cond_.init(ObWaitEventIds::CATALOG_CLIENT_POOL_COND_WAIT))) {
  } else if (OB_FAIL(pools_.create(64, ObMemAttr(tenant_id, "JdbcPools"), ObMemAttr(tenant_id, "JdbcPools")))) {
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::ReqMemEvict, tg_id_))) {
  } else if (OB_FAIL(TG_START(tg_id_))) {
  } else if (OB_FAIL(TG_SCHEDULE(tg_id_, clear_task_, ObJdbcConnectionPool::IDLE_CONNECTION_US, true))) {
  } else {
    inited_ = true;
  }
  return ret;
}

int ObJdbcConnectionPoolMgr::load_options(const ObJdbcConnectionPoolKey &key, ObIAllocator &allocator,
                                         ObJdbcConnectionOptions &options, int64_t &current_version)
{
  int ret = OB_SUCCESS;
  schema::ObSchemaGetterGuard schema_guard;
  const schema::ObCatalogSchema *catalog = nullptr;
  ObJdbcCatalogProperties properties;
  ObString plugin_name;
  options = ObJdbcConnectionOptions();
  options.key_ = key;
  current_version = -1; // Unknown on service errors; never retire a valid pool on a transient error.
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id_, schema_guard))) {
  } else if (OB_FAIL(schema_guard.get_catalog_schema_by_id(tenant_id_, key.catalog_id_, catalog))) {
  } else if (OB_ISNULL(catalog)) {
    current_version = 0;
    ret = OB_SCHEMA_EAGAIN;
  } else {
    current_version = catalog->get_schema_version();
    if (current_version != key.catalog_schema_version_) {
      ret = OB_SCHEMA_EAGAIN;
    } else if (OB_FAIL(properties.load_from_string(catalog->get_catalog_properties(), allocator))) {
    } else if (OB_FAIL(properties.decrypt(allocator))) {
    } else if (OB_FAIL(ObJdbcCatalogProperties::resolve_plugin_name(properties.url_, plugin_name))) {
    } else {
      options.jdbc_url_ = properties.url_;
      options.plugin_name_ = plugin_name;
    }
  }
  return ret;
}

int ObJdbcConnectionPoolMgr::validate(const ObJdbcConnectionOptions &options, int64_t &current_version)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObMemAttr(tenant_id_, "JdbcValidate"));
  ObJdbcConnectionOptions current_options;
  if (OB_FAIL(load_options(options.key_, allocator, current_options, current_version))) {
  } else if (current_options.jdbc_url_ != options.jdbc_url_
             || 0 != current_options.plugin_name_.case_compare(options.plugin_name_)) {
    ret = OB_SCHEMA_EAGAIN;
  }
  return ret;
}

int ObJdbcConnectionPoolMgr::get_pool(const ObJdbcConnectionOptions &options, ObJdbcConnectionPool *&pool)
{
  int ret = OB_SUCCESS;
  pool = nullptr;
  {
    ObThreadCondGuard lock(cond_);
    if (stopped_) {
      ret = OB_SERVER_IS_STOPPING;
    } else if (OB_SUCCESS == (ret = pools_.get_refactored(options.key_, pool)) && OB_NOT_NULL(pool)) {
      pool->inc_ref();
    }
  }
  if (OB_HASH_NOT_EXIST == ret) {
    ret = OB_SUCCESS;
    ObJdbcConnectionPool *candidate = nullptr;
    void *buf = ob_malloc(sizeof(ObJdbcConnectionPool), ObMemAttr(tenant_id_, "JdbcPool"));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      candidate = new (buf) ObJdbcConnectionPool();
      // Factory/JNI initialization is outside both locks, and performs no physical connect.
      if (OB_FAIL(candidate->init(options))) {
      } else {
        ObThreadCondGuard lock(cond_);
        if (stopped_) {
          ret = OB_SERVER_IS_STOPPING;
        } else if (OB_HASH_NOT_EXIST == (ret = pools_.get_refactored(options.key_, pool))) {
          ret = pools_.set_refactored(options.key_, candidate);
          if (OB_SUCC(ret)) {
            pool = candidate;
            candidate = nullptr;
          }
        }
        if (OB_SUCC(ret) && OB_NOT_NULL(pool)) {
          pool->inc_ref();
        }
      }
    }
    if (OB_NOT_NULL(candidate)) {
      candidate->~ObJdbcConnectionPool();
      ob_free(candidate);
    }
  }
  return ret;
}

void ObJdbcConnectionPoolMgr::retire_versions(const ObJdbcConnectionPoolKey &key, int64_t current_version)
{
  if (current_version >= 0) {
    ObSEArray<ObJdbcConnectionPool *, 4> retired;
    {
      ObThreadCondGuard lock(cond_);
      for (auto it = pools_.begin(); it != pools_.end(); ++it) {
        ObJdbcConnectionPool *pool = it->second;
        if (OB_NOT_NULL(pool) && pool->key().catalog_id_ == key.catalog_id_
            && (current_version == 0 || pool->key().catalog_schema_version_ < current_version)) {
          pool->drain();
          if (OB_SUCCESS == retired.push_back(pool)) {
            pool->inc_ref();
          }
        }
      }
    }
    for (int64_t i = 0; i < retired.count(); ++i) {
      if (OB_NOT_NULL(retired.at(i))) {
        retired.at(i)->cleanup();
        retired.at(i)->dec_ref();
      }
    }
  }
}

int ObJdbcConnectionPoolMgr::acquire(const ObJdbcConnectionPoolKey &key, int64_t deadline_ts,
                                     ObJdbcConnectionGuard &guard)
{
  int ret = OB_SUCCESS;
  bool operation = false;
  {
    ObThreadCondGuard lock(cond_);
    if (!inited_) {
      ret = OB_NOT_INIT;
    } else if (stopped_) {
      ret = OB_SERVER_IS_STOPPING;
    } else if (guard.is_valid() || key.tenant_id_ != tenant_id_
             || key.catalog_id_ == OB_INVALID_ID || key.catalog_schema_version_ <= 0) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      ++operations_;
      operation = true;
    }
  }
  if (OB_SUCC(ret)) {
    ObArenaAllocator allocator(ObMemAttr(tenant_id_, "JdbcOptions"));
    ObJdbcConnectionOptions options;
    ObJdbcConnectionPool *pool = nullptr;
    int64_t version = -1;
    ret = load_options(key, allocator, options, version);
    retire_versions(key, version);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(get_pool(options, pool))) {
    } else if (OB_ISNULL(pool)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(pool->acquire(options.jdbc_url_, deadline_ts, guard))) {
    } else {
      // Schema may have advanced while waiting or connecting. Never expose that lease.
      ret = validate(options, version);
      if (OB_SUCC(ret) && ObTimeUtility::current_time() >= deadline_ts) {
        ret = OB_TIMEOUT;
      }
      if (OB_SUCC(ret)) {
        ret = THIS_WORKER.check_status();
      }
      if (OB_SUCC(ret)) {
        ObThreadCondGuard lock(cond_);
        if (stopped_) {
          ret = OB_SERVER_IS_STOPPING;
        }
      }
      if (OB_FAIL(ret)) {
        guard.release();
        retire_versions(options.key_, version);
      }
    }
    if (OB_NOT_NULL(pool)) {
      pool->dec_ref();
    }
  }
  if (operation) {
    finish_operation();
  }
  return ret;
}

void ObJdbcConnectionPoolMgr::finish_operation()
{
  ObThreadCondGuard lock(cond_);
  --operations_;
  cond_.broadcast();
}

int ObJdbcConnectionPoolMgr::pin_pools(ObIArray<ObJdbcConnectionPool *> &pools)
{
  int ret = OB_SUCCESS;
  {
    ObThreadCondGuard lock(cond_);
    for (auto it = pools_.begin(); OB_SUCC(ret) && it != pools_.end(); ++it) {
      if (OB_NOT_NULL(it->second) && OB_SUCC(ret = pools.push_back(it->second))) {
        it->second->inc_ref();
      }
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to pin all jdbc connection pools",
             K(ret),
             K_(tenant_id),
             "pinned_pool_count", pools.count());
  }
  return ret;
}

int ObJdbcConnectionPoolMgr::get_stats(ObIArray<ObJdbcConnectionPoolStats> &stats)
{
  int ret = OB_SUCCESS;
  ObThreadCondGuard lock(cond_);
  for (auto it = pools_.begin(); OB_SUCC(ret) && it != pools_.end(); ++it) {
    ObJdbcConnectionPoolStats snapshot;
    if (OB_NOT_NULL(it->second)) {
      it->second->snapshot(snapshot);
      ret = stats.push_back(snapshot);
    }
  }
  return ret;
}

void ObJdbcConnectionPoolMgr::cleanup_idle_pools()
{
  if (pools_.created()) {
    ObSEArray<ObJdbcConnectionPool *, 8> pools;
    IGNORE_RETURN pin_pools(pools);
    for (int64_t i = 0; i < pools.count(); ++i) {
      ObJdbcConnectionPool *pool = pools.at(i);
      if (OB_NOT_NULL(pool)) {
        schema::ObSchemaGetterGuard schema_guard;
        const schema::ObCatalogSchema *catalog = nullptr;
        // Also retire a dropped/altered catalog that has had no subsequent requests.
        if (OB_NOT_NULL(GCTX.schema_service_)
            && OB_SUCCESS == GCTX.schema_service_->get_tenant_schema_guard(tenant_id_, schema_guard)
            && OB_SUCCESS == schema_guard.get_catalog_schema_by_id(tenant_id_, pool->key().catalog_id_, catalog)
            && (OB_ISNULL(catalog) || catalog->get_schema_version() > pool->key().catalog_schema_version_)) {
          pool->drain();
        }
        pool->cleanup();
        pool->dec_ref();
      }
    }
    // Remove one at a time; destruction (including bridge GlobalRefs) is outside the manager lock.
    bool removed = true;
    while (removed) {
      ObJdbcConnectionPool *pool = nullptr;
      {
        ObThreadCondGuard lock(cond_);
        for (auto it = pools_.begin(); it != pools_.end(); ++it) {
          if (OB_NOT_NULL(it->second) && it->second->removable()) {
            int ret = OB_SUCCESS;
            pool = it->second;
            if (OB_FAIL(pools_.erase_refactored(it->first))) {
              LOG_WARN("failed to erase jdbc connection pool", K(ret), K_(tenant_id), K(pool->key()));
              pool = nullptr;
            }
            break;
          }
        }
      }
      removed = OB_NOT_NULL(pool);
      if (removed) {
        pool->~ObJdbcConnectionPool();
        ob_free(pool);
      }
    }
  }
}

void ObJdbcConnectionPoolMgr::stop()
{
  if (cond_.is_inited() && pools_.created()) {
    ObThreadCondGuard lock(cond_);
    stopped_ = true;
    for (auto it = pools_.begin(); it != pools_.end(); ++it) {
      if (OB_NOT_NULL(it->second)) {
        it->second->drain(true);
      }
    }
    cond_.broadcast();
  }
  if (tg_id_ >= 0) {
    TG_STOP(tg_id_);
  }
}

void ObJdbcConnectionPoolMgr::wait()
{
  if (tg_id_ >= 0) {
    TG_WAIT(tg_id_);
  }
  if (cond_.is_inited() && pools_.created()) {
    bool done = false;
    while (!done) {
      cleanup_idle_pools();
      ObThreadCondGuard lock(cond_);
      done = operations_ == 0 && pools_.size() == 0;
      if (!done) {
        cond_.wait_us(ObJdbcConnectionPool::CHECK_INTERVAL_US);
      }
    }
  }
}

void ObJdbcConnectionPoolMgr::destroy()
{
  stop();
  wait();
  if (tg_id_ >= 0) {
    TG_DESTROY(tg_id_);
    tg_id_ = -1;
  }
  pools_.destroy();
  inited_ = false;
}
} // namespace share
} // namespace oceanbase
