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

#define USING_LOG_PREFIX LIB_MYSQLC
#include "lib/oblog/ob_log_module.h"

#include "lib/mysqlclient/ob_mysql_connection_pool.h"
#include "lib/mysqlclient/ob_server_connection_pool.h"
#include "lib/mysqlclient/ob_mysql_server_provider.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/thread/thread_mgr.h"

namespace oceanbase {
namespace common {
namespace sqlclient {
const char* const ObMySQLConnectionPool::DEFAULT_DB_USER = OB_INTERNAL_USER;
const char* const ObMySQLConnectionPool::DEFAULT_DB_PASS = "";
const char* const ObMySQLConnectionPool::DEFAULT_DB_NAME = "default";

ObMySQLConnectionPool::ObMySQLConnectionPool()
    : is_updated_(false),
      is_stop_(false),
      mode_(ObMySQLConnection::OCEANBASE_MODE),
      tg_id_(-1),
      server_provider_(NULL),
      busy_conn_count_(0),
      config_(),
      get_lock_(obsys::WRITE_PRIORITY),
      allocator_(ObModIds::OB_SQL_CONNECTION_POOL),
      cluster_list_(allocator_)
{
  set_db_param(DEFAULT_DB_USER, DEFAULT_DB_PASS, DEFAULT_DB_NAME);
  init_sql_[0] = '\0';
  if (OB_UNLIKELY(0 != mysql_library_init(0, NULL, NULL))) {
    LOG_WARN("could not initialize MySQL library");
  }

  // set defult values
  config_.sqlclient_wait_timeout_ = 10;                  // 10s
  config_.long_query_timeout_ = 120 * 1000 * 1000;       // 120s
  config_.connection_refresh_interval_ = 200 * 1000;     // 200ms
  config_.connection_pool_warn_time_ = 1 * 1000 * 1000;  // 1s
  config_.sqlclient_per_observer_conn_limit_ = 10;
}

ObMySQLConnectionPool::~ObMySQLConnectionPool()
{
  ObServerConnectionPool* pool = NULL;
  obsys::CWLockGuard lock(get_lock_);
  for (ClusterList::iterator cluster_iter = cluster_list_.begin(); cluster_iter != cluster_list_.end();
       cluster_iter++) {
    ClusterServerList*& cluster_server_list = *cluster_iter;
    if (OB_NOT_NULL(cluster_server_list)) {
      ServerList& server_list = cluster_server_list->get_server_list();
      for (ServerList::iterator iter = server_list.begin(); iter != server_list.end(); iter++) {
        pool = *iter;
        // pool->~ObServerConnectionPool();
        // so that ObCachedAllocator can destroy and free the memory
        server_pool_.free(pool);
      }
      server_list.clear();
    }
  }
  // delete mysql_library_end() because not all thread exit when the pool deconstruct
  // mysql_library_end();

  stop();
  if (tg_id_ != -1) {
    int ret = OB_SUCCESS;
    if (OB_FAIL(TG_CANCEL_R(tg_id_, *this))) {
      LOG_ERROR("fail to cancel timer task", K(this), K(ret));
    }
    tg_id_ = -1;
  }
}

void ObMySQLConnectionPool::set_server_provider(ObMySQLServerProvider* provider)
{
  server_provider_ = provider;
}

int ObMySQLConnectionPool::set_db_param(const char* db_user, const char* db_pass, const char* db_name)
{
  int ret = OB_SUCCESS;
  int64_t w_len = 0;
  if (OB_ISNULL(db_user) || OB_ISNULL(db_pass) || OB_ISNULL(db_name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid db param", K(db_user), K(db_pass), K(db_name), K(ret));
  } else if (FALSE_IT(w_len = snprintf(db_user_, OB_MAX_USER_NAME_BUF_LENGTH, "%s", db_user))) {
    // impossible
  } else if (OB_UNLIKELY(w_len <= 0) || OB_UNLIKELY(w_len > OB_MAX_USER_NAME_LENGTH)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fill db user error", K(db_user), K(ret));
  } else if (FALSE_IT(w_len = snprintf(db_pass_, OB_MAX_PASS_WORD_LENGTH, "%s", db_pass))) {
    // impossible
  } else if (OB_UNLIKELY(w_len < 0) || OB_UNLIKELY(w_len >= OB_MAX_PASS_WORD_LENGTH)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fill db password error", K(db_pass), K(ret));
  } else if (FALSE_IT(w_len = snprintf(db_name_, OB_MAX_DATABASE_NAME_BUF_LENGTH, "%s", db_name))) {
    // impossible
  } else if (OB_UNLIKELY(w_len <= 0) || OB_UNLIKELY(w_len > OB_MAX_DATABASE_NAME_LENGTH)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fill db database name error", K(db_name), K(ret));
  }
  return ret;
}

int ObMySQLConnectionPool::set_db_param(const ObString& db_user, const ObString& db_pass, const ObString& db_name)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(db_user.length() > OB_MAX_USER_NAME_LENGTH - 1) ||
      OB_UNLIKELY(db_pass.length() > OB_MAX_PASS_WORD_LENGTH - 1) ||
      OB_UNLIKELY(db_name.length() > OB_MAX_DATABASE_NAME_LENGTH - 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("db param buffer is not enough", K(db_user), K(db_pass), K(db_name), K(ret));
  } else {
    MEMCPY(db_user_, db_user.ptr(), db_user.length());
    db_user_[db_user.length()] = '\0';
    MEMCPY(db_pass_, db_pass.ptr(), db_pass.length());
    db_pass_[db_pass.length()] = '\0';
    MEMCPY(db_name_, db_name.ptr(), db_name.length());
    db_name_[db_name.length()] = '\0';
  }
  return ret;
}

int ObMySQLConnectionPool::start(int tg_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!server_provider_ || -1 == tg_id)) {
    ret = OB_NOT_INIT;
    LOG_WARN("server_provider_ not inited", K(ret), K(server_provider_), K(tg_id));
  } else {
    is_stop_ = false;
    tg_id_ = tg_id;
    runTimerTask();
  }
  return ret;
}

void ObMySQLConnectionPool::stop()
{
  is_stop_ = true;
}

void ObMySQLConnectionPool::signal_refresh()
{
  TG_CANCEL(tg_id_, *this);
  LOG_INFO("signal_refresh");
  runTimerTask();
}

void ObMySQLConnectionPool::close_all_connection()
{
  int ret = OB_SUCCESS;
  obsys::CWLockGuard lock(get_lock_);
  ObServerConnectionPool* pool = NULL;
  for (ClusterList::iterator cluster_iter = cluster_list_.begin(); cluster_iter != cluster_list_.end();
       cluster_iter++) {
    ClusterServerList*& cluster_server_list = *cluster_iter;
    if (OB_ISNULL(cluster_server_list)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ptr should not be null", K(ret));
    } else {
      ServerList& server_list = cluster_server_list->get_server_list();
      for (ServerList::iterator iter = server_list.begin(); iter != server_list.end(); iter++) {
        if (OB_ISNULL(pool = *iter)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("unexpected error, pool is null", K(ret));
        } else {
          pool->close_all_connection();
        }
      }
    }
  }
}

void ObMySQLConnectionPool::mark_all_server_connection_gone()
{
  int ret = OB_SUCCESS;
  ObServerConnectionPool* pool = NULL;
  for (ClusterList::iterator cluster_iter = cluster_list_.begin(); cluster_iter != cluster_list_.end();
       cluster_iter++) {
    ClusterServerList*& cluster_server_list = *cluster_iter;
    if (OB_ISNULL(cluster_server_list)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ptr should not be null", K(ret));
    } else {
      ServerList& server_list = cluster_server_list->get_server_list();
      for (ServerList::iterator iter = server_list.begin(); iter != server_list.end(); iter++) {
        if (OB_ISNULL(pool = *iter)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("unexpected error, pool is null", K(ret));
        } else {
          pool->set_server_gone(true);
        }
      }
    }
  }
}

// need outter lock protection
int ObMySQLConnectionPool::renew_server_connection_pool(const int64_t cluster_id, common::ObAddr& server)
{
  int ret = OB_SUCCESS;
  ObServerConnectionPool* pool = NULL;
  ClusterServerList* cluster_server_list = NULL;
  if (OB_FAIL(get_cluster_server_list_(cluster_id, cluster_server_list))) {
    LOG_WARN("fail to get cluster_server_list", K(ret), K(cluster_id), K(server));
  } else if (OB_ISNULL(cluster_server_list)) {
    ret = OB_ITER_END;
    LOG_DEBUG("cluster_server_list maybe not created yet", K(ret), K(cluster_id), K(server));
  } else {
    ServerList& server_list = cluster_server_list->get_server_list();
    bool found = false;
    for (ServerList::iterator iter = server_list.begin(); OB_SUCC(ret) && !found && iter != server_list.end(); iter++) {
      if (OB_ISNULL(pool = *iter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pool is null", K(ret));
      } else if (pool->get_server() == server) {
        LOG_DEBUG("server pool renewed", K(cluster_id), K(server));
        pool->renew();
        found = true;
      }
    }
    if (OB_SUCC(ret) && false == found) {
      // not found this server in connection pool. should be a newly online one
      ret = OB_ITER_END;
    }
  }
  return ret;
}

int ObMySQLConnectionPool::create_server_connection_pool(const int64_t cluster_id, const common::ObAddr& server)
{
  int ret = OB_SUCCESS;
  ClusterServerList* cluster_server_list = NULL;
  if (OB_FAIL(get_cluster_server_list_(cluster_id, cluster_server_list))) {
    LOG_WARN("fail to get cluster_server_list", K(ret), K(cluster_id), K(server));
  } else if (OB_ISNULL(cluster_server_list)) {
    void* buf = NULL;
    if (NULL == (buf = allocator_.alloc(sizeof(ClusterServerList)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc cluster_server_list failed", K(ret), K(cluster_id), K(server));
    } else if (FALSE_IT(cluster_server_list = new (buf) ClusterServerList(allocator_))) {
    } else if (FALSE_IT(cluster_server_list->set_cluster_id(cluster_id))) {
    } else if (OB_FAIL(cluster_list_.push_back(cluster_server_list))) {
      LOG_WARN("fail to push back cluster_server_list", K(ret), K(cluster_id), K(server));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(cluster_server_list)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cluster_server_list is null", K(ret), K(cluster_id), K(server));
  } else {
    ServerList& server_list = cluster_server_list->get_server_list();
    ObServerConnectionPool* pool = server_pool_.alloc();
    if (OB_ISNULL(pool)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("out of memory", K(ret));
    } else if (OB_FAIL(pool->init(this, server, config_.sqlclient_per_observer_conn_limit_))) {
      LOG_WARN("fail to init connection pool", K(ret));
    } else if (OB_FAIL(server_list.push_back(pool))) {
      LOG_WARN("push pool to list fail. destroyed", K(ret));
    } else {
      LOG_INFO("new server pool created", K(server), K(config_.sqlclient_per_observer_conn_limit_));
    }

    if (OB_FAIL(ret) && OB_LIKELY(NULL != pool)) {
      server_pool_.free(pool);  // put back to cache. prevent memory leak
      pool = NULL;
    }
  }
  return ret;
}

// need outter lock protection
// TODO: remove unused cluster_server_list
int ObMySQLConnectionPool::purge_connection_pool()
{
  int ret = OB_SUCCESS;
  ObServerConnectionPool* pool = NULL;
  for (ClusterList::iterator cluster_iter = cluster_list_.begin(); cluster_iter != cluster_list_.end();
       cluster_iter++) {
    ClusterServerList*& cluster_server_list = *cluster_iter;
    if (OB_ISNULL(cluster_server_list)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ptr should not be null", K(ret));
    } else {
      ServerList& server_list = cluster_server_list->get_server_list();
      int64_t now = ::oceanbase::common::ObTimeUtility::current_time();
      ObArray<ObList<ObServerConnectionPool*, common::ObArenaAllocator>::iterator> to_delete;
      for (ServerList::iterator iter = server_list.begin(); OB_SUCC(ret) && iter != server_list.end(); iter++) {
        if (OB_ISNULL(pool = *iter)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("pool is null", K(ret));
        } else if (0 == pool->get_busy_count() && MAX_SERVER_GONE_INTERVAL < now - pool->last_renew_time()) {
          /* When can we release the ServerConnectionPool:
           *  - all connection of the pool have been released back to pool
           *  - and it has been a 'very long' time that the server has gone
           */
          pool->set_server_gone(true);  // defensive code, not required.
          // normally when it reachs here, it has been marked 'gone' already
          LOG_INFO("server pool removed", K(pool->get_server()));
          if (OB_FAIL(to_delete.push_back(iter))) {
            LOG_WARN("push iter to delete list fail", K(ret));
          }
        }
      }
      if (OB_SUCC(ret)) {
        for (int i = 0; OB_SUCC(ret) && i < to_delete.count(); i++) {
          pool = *to_delete.at(i);
          server_pool_.free(pool);
          if (OB_FAIL(server_list.erase(to_delete.at(i)))) {
            LOG_WARN("fail to delete pool from server_list", K(ret));
          }
        }
      }
    }
  }

  return ret;
}

// External caller lock
int ObMySQLConnectionPool::get_pool(const int64_t cluster_id, ObServerConnectionPool*& pool)
{
  // Note: Round-Robin while getting server from server list.
  static __thread int64_t cursor = 0;
  int ret = OB_SUCCESS;
  pool = NULL;
  ClusterServerList* cluster_server_list = NULL;
  if (OB_FAIL(get_cluster_server_list_(cluster_id, cluster_server_list))) {
    LOG_WARN("fail to get cluster_server_list", K(ret), K(cluster_id));
  } else if (OB_ISNULL(cluster_server_list)) {
    // skip
  } else {
    ServerList& server_list = cluster_server_list->get_server_list();
    if (server_list.size() > 0) {
      cursor = (cursor + 1) % server_list.size();
      int64_t pos = cursor;
      ObList<ObServerConnectionPool*, common::ObArenaAllocator>::iterator iter;
      for (iter = server_list.begin(); pos > 0 && iter != server_list.end(); iter++, pos--) {
        // nop
      }

      if (OB_UNLIKELY(iter == server_list.end())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("iter should not end.", K(pos), K(cursor), K(server_list.size()));
      } else if (OB_ISNULL(*iter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("pool iter must not null", K(ret));
      } else {
        pool = *iter;
      }
    } else {
      ret = OB_RESOURCE_OUT;
      LOG_ERROR("server list must not be empty", K(ret));
    }
  }
  return ret;
}

int ObMySQLConnectionPool::acquire(const int64_t cluster_id, ObMySQLConnection*& connection)
{
  int ret = OB_SUCCESS;
  const int64_t server_count = get_server_count(cluster_id);

  connection = NULL;
  bool acquire_succ = false;
  for (int64_t i = 0; !acquire_succ && i < server_count; ++i) {
    if (OB_FAIL(do_acquire(cluster_id, connection))) {
      LOG_WARN("fail to get connection", K(ret));
    } else if (OB_FAIL(try_connect(connection))) {
      LOG_WARN("failed to try connection, will release connection", K(ret));
      const bool succ = false;
      if (OB_SUCCESS != release(connection, succ)) {  // ignore ret
        LOG_WARN("failed to release connection, ignore ret");
      }
    } else {
      acquire_succ = true;
    }
  }
  if (OB_FAIL(ret)) {
    connection = NULL;
  }

  if (OB_ISNULL(connection)) {
    LOG_WARN("failed to acquire connection", K(cluster_id), K(this), K(server_count), K(busy_conn_count_), K(ret));
    obsys::CRLockGuard lock(get_lock_);
    for (ClusterList::iterator cluster_iter = cluster_list_.begin(); cluster_iter != cluster_list_.end();
         cluster_iter++) {
      ClusterServerList*& cluster_server_list = *cluster_iter;
      if (OB_NOT_NULL(cluster_server_list)) {
        LOG_INFO("dump server connection pool", "cluster_id", cluster_server_list->get_cluster_id());
        ServerList& server_list = cluster_server_list->get_server_list();
        for (ServerList::iterator iter = server_list.begin(); iter != server_list.end(); iter++) {
          if (OB_NOT_NULL(*iter)) {
            (*iter)->dump();
          }
        }
      }
    }
  } else {
    connection->set_mode(mode_);
  }
  return ret;
}

int64_t ObMySQLConnectionPool::get_server_count(const int64_t cluster_id) const
{
  int64_t count = 0;
  int ret = OB_SUCCESS;
  obsys::CRLockGuard lock(get_lock_);
  ClusterServerList* cluster_server_list = NULL;
  if (OB_FAIL(get_cluster_server_list_(cluster_id, cluster_server_list))) {
    LOG_WARN("fail to get cluster_server_list", K(ret), K(cluster_id));
  } else if (OB_ISNULL(cluster_server_list)) {
    // skip
  } else {
    count = cluster_server_list->get_server_list().size();
  }
  return count;
}

int ObMySQLConnectionPool::do_acquire(const int64_t cluster_id, ObMySQLConnection*& connection)
{
  int ret = OB_SUCCESS;
  obsys::CRLockGuard lock(get_lock_);
  ObServerConnectionPool* pool = NULL;
  if (OB_FAIL(get_pool(cluster_id, pool))) {
    LOG_WARN("failed to get pool", K(ret));
  } else if (OB_FAIL(pool->acquire(connection))) {
    LOG_WARN("failed to get connection", K(ret));
  } else if (OB_ISNULL(connection)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("connection must not be null", K(ret));
  } else if (OB_UNLIKELY(connection->is_busy())) {
    ret = OB_ERR_SYS;
    LOG_WARN("connection is busy, should not happen", K(ret));
  } else {
    ATOMIC_INC((uint64_t*)&busy_conn_count_);
    connection->set_busy(true);
    connection->set_timestamp(::oceanbase::common::ObTimeUtility::current_time());
    connection->set_cluster_id(cluster_id);
    LOG_DEBUG("connection acquire", K(this), K(busy_conn_count_), K(connection), K(pool));
  }
  return ret;
}

int ObMySQLConnectionPool::try_connect(ObMySQLConnection* connection)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(connection)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("connection must not null", K(ret));
  } else if (connection->is_closed()) {
    connection->set_timeout(config_.sqlclient_wait_timeout_);
    if (OB_FAIL(connection->connect(db_user_, db_pass_, db_name_))) {
      LOG_WARN("fail to connect to server", K(connection->get_server()), K(ret));
    } else if ('\0' != init_sql_[0]) {
      // TODO oushen, not support now
      // if (OB_SUCCESS != (ret = execute_init_sql(connection))) {
      //  LOG_WARN("failed to execute_init_sql, ret=%d", ret);
      //}
    }
    if (OB_SUCC(ret) && ObMySQLConnection::OCEANBASE_MODE == mode_) {
      if (OB_FAIL(connection->init_oceanbase_connection())) {
        LOG_WARN("fail to init oceanabse connection", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(connection->set_timeout_variable(config_.long_query_timeout_, DEFAULT_TRANSACTION_TIMEOUT_US))) {
        LOG_WARN("fail to set mysql timeout variablse", K(ret));
        if (ObMySQLConnection::DEBUG_MODE == mode_) {
          ret = OB_SUCCESS;
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    // connection->set_trace_id();
  }
  return ret;
}

int ObMySQLConnectionPool::execute_init_sql(ObMySQLConnection* connection)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(connection)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid connection", K(connection), K(ret));
  } else {
    ObMySQLStatement stmt;
    SMART_VAR(ObMySQLProxy::MySQLResult, rs)
    {
      int64_t affect_rows = 0;
      if (OB_FAIL(connection->create_statement(stmt, OB_SYS_TENANT_ID, init_sql_))) {
        LOG_WARN("create statement failed", K(connection), K(ret));
      } else if (OB_FAIL(stmt.execute_update(affect_rows))) {
        LOG_WARN("execute sql failed", K(connection), K(connection->get_server()), K(init_sql_), K(ret));
      }
    }
  }

  return ret;
}

int ObMySQLConnectionPool::release(ObMySQLConnection* connection, const bool succ)
{
  int ret = OB_SUCCESS;
  obsys::CRLockGuard lock(get_lock_);
  ObServerConnectionPool* pool = NULL;
  if (OB_ISNULL(connection)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("connection is null, cannot be released", K(ret));
  } else {
    const int64_t cost_time = ::oceanbase::common::ObTimeUtility::current_time() - connection->get_timestamp();
    if (cost_time > config_.connection_pool_warn_time_) {
      LOG_WARN("this connection cost too much time",
          K(this),
          K(cost_time),
          K(config_.connection_pool_warn_time_),
          K(connection->get_server()),
          "start time",
          time2str(connection->get_timestamp()));
    }
    // reset_trace_id(connection);//we just set a new one when acquire next time
    connection->set_busy(false);
    if (OB_ISNULL(pool = connection->get_root())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to release connection. connection pool not set", K(ret));
    } else if (OB_FAIL(pool->release(connection, succ))) {
      LOG_WARN("fail to release connection", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    ATOMIC_DEC((uint64_t*)&busy_conn_count_);
  }
  LOG_DEBUG("connection release", K(this), K(busy_conn_count_), K(connection), K(pool), K(ret));
  return ret;
}

void ObMySQLConnectionPool::runTimerTask()
{
  int ret = OB_SUCCESS;
  int64_t cluster_count = 0;
  int64_t refresh_period_ms = 0;
  common::ObAddr server;

  LOG_TRACE("start timer task for refresh connection pool");
  if (OB_ISNULL(server_provider_) || -1 == tg_id_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid connection pool", K(server_provider_), K(tg_id_), K(ret));
  } else {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = server_provider_->prepare_refresh())) {
      // Ignore the error code and does not affect the subsequent process
      LOG_WARN("failed to prepare refresh", K(ret), K(tmp_ret));
    }
    obsys::CWLockGuard lock(get_lock_);
    if (OB_FAIL(server_provider_->refresh_server_list())) {
      if (!is_updated_) {  // has never successfully updated, it is in startup time, should not print ERROR
        LOG_INFO("fail to refresh mysql server list in startup time, it's normal", K(ret));
      } else {
        LOG_WARN("fail to refresh mysql server list", K(ret));
      }
    } else if ((cluster_count = server_provider_->get_cluster_count()) == 0) {
      LOG_DEBUG("no alive ms found, skip refresh conncetion pool");
    } else {
      is_updated_ = true;
      mark_all_server_connection_gone();
      ObArray<int64_t> cluster_list;
      if (OB_FAIL(server_provider_->get_cluster_list(cluster_list))) {
        LOG_WARN("fail to get cluster list", K(ret));
      } else {
        for (int64_t i = 0; i < cluster_list.count(); i++) {
          int64_t cluster_id = cluster_list.at(i);
          int64_t server_count = 0;
          if ((server_count = server_provider_->get_server_count(cluster_id)) == 0) {
            LOG_DEBUG("no alive ms found, skip refresh conncetion pool");
          } else {
            for (int64_t j = 0; j < server_count; j++) {
              /* steps:
               * 1. renew old ms
               * 2. add new-born ms
               * 3. filter out dead or idle ms
               */
              if (OB_FAIL(server_provider_->get_server(cluster_id, j, server))) {
                LOG_ERROR("can't get mysql server", K(cluster_id), K(server_count), "cur_index", j, K(ret));
              } else if (OB_FAIL(renew_server_connection_pool(cluster_id, server))) {
                if (OB_UNLIKELY(OB_ITER_END != ret)) {
                  LOG_ERROR("fail to renew connection server", K(cluster_id), K(ret));
                } else if (OB_FAIL(create_server_connection_pool(cluster_id, server))) {
                  LOG_ERROR("fail to create new connection to server", K(cluster_id), K(server), K(ret));
                }
              }
            }
          }
        }
      }

      // update connection pool
      // - remove invalid server connection pool
      // - close long idle connection
      if (OB_FAIL(purge_connection_pool())) {
        LOG_ERROR("fail to update mysql connection pool", K(ret));
      }
    }
    // end LOCK BLOCK
    if (cluster_count > 0) {
      refresh_period_ms = config_.connection_refresh_interval_ * 50;  // default 10s
    } else if (server_provider_->need_refresh()) {
      refresh_period_ms = config_.connection_refresh_interval_;  // default 200ms=0.2s
    } else {
      refresh_period_ms = config_.connection_refresh_interval_ * 500;  // 100S;
    }
    if (!is_stop_) {
      if (OB_FAIL(TG_SCHEDULE(tg_id_, *this, refresh_period_ms, false))) {
        LOG_ERROR("fail to schedule timer again in mysql connection pool", K(ret));
      } else {
        LOG_TRACE("schedule timer task for refresh next time", K(refresh_period_ms));
      }
    } else {
      LOG_INFO("stop schedule the refresh connection pool task");
    }
  }
}

int ObMySQLConnectionPool::acquire(ObISQLConnection*& conn, ObISQLClient* client_addr)
{
  int64_t cluster_id = OB_INVALID_ID;
  return acquire(conn, cluster_id, client_addr);
}

int ObMySQLConnectionPool::acquire(ObISQLConnection*& conn, const int64_t cluster_id, ObISQLClient* client_addr)
{
  int ret = OB_SUCCESS;
  UNUSED(client_addr);
  ObMySQLConnection* mysql_conn = NULL;
  conn = NULL;
  if (OB_FAIL(acquire(cluster_id, mysql_conn))) {
    LOG_WARN("acquire connection failed", K(ret));
  } else {
    conn = mysql_conn;
    if (OB_NOT_NULL(conn) && conn->get_cluster_id() != cluster_id) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cluster_id not match", K(ret), K(cluster_id));
    }
  }
  return ret;
}

int ObMySQLConnectionPool::release(ObISQLConnection* conn, const bool success)
{
  int ret = OB_SUCCESS;
  if (NULL != conn) {
    // FIXME : add ObISQLConnection::revert() for connection release to avoid dynamic cast
    ObMySQLConnection* mysql_conn = dynamic_cast<ObMySQLConnection*>(conn);
    if (OB_ISNULL(mysql_conn)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ObMySQLConnectionPool only release ObMySQLConnection", K(ret));
    } else if (OB_FAIL(release(mysql_conn, success))) {
      LOG_WARN("release connection failed", K(ret));
    }
  }
  return ret;
}

int ObMySQLConnectionPool::escape(
    const char* from, const int64_t from_size, char* to, const int64_t to_size, int64_t& out_size)
{
  ObMySQLConnection* conn = NULL;
  int ret = OB_SUCCESS;
  int64_t cluster_id = OB_INVALID_ID;
  if (OB_FAIL(acquire(cluster_id, conn))) {
    LOG_WARN("acquire connection failed", K(ret));
  } else if (OB_ISNULL(conn)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("connection should not be NULL", K(ret));
  } else if (OB_FAIL(conn->escape(from, from_size, to, to_size, out_size))) {
    LOG_WARN("escape string failed", K(from), K(from_size), K(to), K(to_size), K(ret));
  }
  if (NULL != conn) {
    int tmp_ret = release(conn, OB_SUCCESS == ret);
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("release connection failed", K(tmp_ret));
    }
  }
  return ret;
}

// The caller needs to lock, this function does not lock
int ObMySQLConnectionPool::get_cluster_server_list_(
    const int64_t cluster_id, ClusterServerList*& cluster_server_list) const
{
  int ret = OB_SUCCESS;
  cluster_server_list = NULL;
  for (ClusterList::const_iterator cluster_iter = cluster_list_.begin(); cluster_iter != cluster_list_.end();
       cluster_iter++) {
    ClusterServerList* const& cur = *cluster_iter;
    if (OB_NOT_NULL(cur) && cluster_id == cur->get_cluster_id()) {
      cluster_server_list = cur;
      break;
    }
  }
  return ret;
}

int ObDbLinkConnectionPool::create_dblink_pool(uint64_t dblink_id, const ObAddr& server, const ObString& db_tenant,
    const ObString& db_user, const ObString& db_pass, const ObString& db_name)
{
  int ret = OB_SUCCESS;
  ObServerConnectionPool* dblink_pool = NULL;
  obsys::CRLockGuard lock(get_lock_);
  if (OB_FAIL(get_dblink_pool(dblink_id, dblink_pool))) {
    LOG_WARN("fail to get dblink connection pool", K(dblink_id));
  } else if (OB_NOT_NULL(dblink_pool)) {
    // nothing.
  } else {
    int64_t cluster_id = OB_INVALID_ID;
    ClusterServerList* cluster_server_list = NULL;
    if (OB_FAIL(get_cluster_server_list_(cluster_id, cluster_server_list))) {
      LOG_WARN("fail to get cluster_server_list", K(ret), K(cluster_id), K(server));
    } else if (OB_ISNULL(cluster_server_list)) {
      void* buf = NULL;
      if (NULL == (buf = allocator_.alloc(sizeof(ClusterServerList)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("alloc cluster_server_list failed", K(ret), K(cluster_id), K(server));
      } else if (FALSE_IT(cluster_server_list = new (buf) ClusterServerList(allocator_))) {
      } else if (FALSE_IT(cluster_server_list->set_cluster_id(cluster_id))) {
      } else if (OB_FAIL(cluster_list_.push_back(cluster_server_list))) {
        LOG_WARN("fail to push back cluster_server_list", K(ret), K(cluster_id), K(server));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(cluster_server_list)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cluster_server_list is null", K(ret), K(cluster_id), K(server));
    } else {
      ServerList& server_list = cluster_server_list->get_server_list();
      if (OB_ISNULL(dblink_pool = server_pool_.alloc())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("out of memory", K(ret));
      } else if (OB_FAIL(dblink_pool->init_dblink(dblink_id,
                     server,
                     db_tenant,
                     db_user,
                     db_pass,
                     db_name,
                     this,
                     config_.sqlclient_per_observer_conn_limit_))) {
        LOG_WARN("fail to init dblink connection pool", K(ret));
      } else if (OB_FAIL(server_list.push_back(dblink_pool))) {
        LOG_WARN("fail to push pool to list", K(ret));
      } else {
        LOG_INFO("new dblink pool created", K(server), K(config_.sqlclient_per_observer_conn_limit_));
      }
      if (OB_FAIL(ret) && OB_NOT_NULL(dblink_pool)) {
        server_pool_.free(dblink_pool);  // put back to cache. prevent memory leak
        dblink_pool = NULL;
      }
    }
  }
  return ret;
}

int ObDbLinkConnectionPool::acquire_dblink(uint64_t dblink_id, ObMySQLConnection*& dblink_conn)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(do_acquire_dblink(dblink_id, dblink_conn))) {
    LOG_WARN("fail to acquire dblink", K(ret), K(dblink_id));
  } else if (OB_FAIL(try_connect_dblink(dblink_conn))) {
    LOG_WARN("fail to try connect dblink", K(ret), K(dblink_id));
    int release_ret = release_dblink(dblink_conn);
    if (release_ret != OB_SUCCESS) {
      LOG_WARN("fail to release dblink conn", K(release_ret), K(dblink_id));
    }
    dblink_conn = NULL;
  }
  return ret;
}

int ObDbLinkConnectionPool::release_dblink(ObMySQLConnection* dblink_conn)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(release(dblink_conn, true))) {
    LOG_WARN("fail to release dblink conn", K(ret));
  }
  return ret;
}

int ObDbLinkConnectionPool::get_dblink_pool(uint64_t dblink_id, ObServerConnectionPool*& dblink_pool)
{
  int ret = OB_SUCCESS;
  ObServerConnectionPool* pool = NULL;
  dblink_pool = NULL;
  ClusterServerList* cluster_server_list = NULL;
  int64_t cluster_id = OB_INVALID_ID;
  if (OB_FAIL(get_cluster_server_list_(cluster_id, cluster_server_list))) {
    LOG_WARN("fail to get cluster_server_list", K(ret), K(cluster_id));
  } else if (OB_ISNULL(cluster_server_list)) {
    // skip
  } else {
    ServerList& server_list = cluster_server_list->get_server_list();
    for (ServerList::iterator iter = server_list.begin(); OB_SUCC(ret) && iter != server_list.end(); iter++) {
      if (OB_ISNULL(pool = *iter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pool is null", K(ret));
      } else if (dblink_id == pool->get_dblink_id()) {
        dblink_pool = pool;
        break;
      }
    }
  }
  return ret;
}

int ObDbLinkConnectionPool::do_acquire_dblink(uint64_t dblink_id, ObMySQLConnection*& dblink_conn)
{
  int ret = OB_SUCCESS;
  ObServerConnectionPool* dblink_pool = NULL;
  if (OB_FAIL(get_dblink_pool(dblink_id, dblink_pool))) {
    LOG_WARN("failed to get dblink pool", K(ret), K(dblink_id));
  } else if (OB_ISNULL(dblink_pool)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dblink pool is NULL", K(ret));
  } else if (OB_FAIL(dblink_pool->acquire(dblink_conn))) {
    LOG_WARN("failed to acquire dblink conn", K(ret));
  } else if (OB_ISNULL(dblink_conn)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dblink conn is NULL", K(ret));
  } else if (OB_UNLIKELY(dblink_conn->is_busy())) {
    ret = OB_ERR_SYS;
    LOG_WARN("dblink conn is busy, should not happen", K(ret));
  } else {
    ATOMIC_INC((uint64_t*)&busy_conn_count_);
    dblink_conn->set_busy(true);
    dblink_conn->set_timestamp(::oceanbase::common::ObTimeUtility::current_time());
    dblink_conn->set_cluster_id(OB_INVALID_ID);
    LOG_DEBUG("connection acquire", K(this), K(busy_conn_count_), K(dblink_conn), K(dblink_pool));
  }
  return ret;
}

int ObDbLinkConnectionPool::try_connect_dblink(ObMySQLConnection* dblink_conn)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(dblink_conn)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("dblink conn is NULL", K(ret));
  } else if (dblink_conn->is_closed()) {
    dblink_conn->set_timeout(config_.sqlclient_wait_timeout_);
    if (OB_FAIL(dblink_conn->connect_dblink())) {
      LOG_WARN("fail to connect dblink", K(dblink_conn->get_server()), K(ret));
    } else if (OB_FAIL(
                   dblink_conn->set_timeout_variable(config_.long_query_timeout_, DEFAULT_TRANSACTION_TIMEOUT_US))) {
      LOG_WARN("fail to set mysql timeout variablse", K(ret));
    }
  }
  return ret;
}

}  // end namespace sqlclient
}  // end namespace common
}  // end namespace oceanbase
