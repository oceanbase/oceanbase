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
#include "lib/mysqlclient/ob_isql_connection_pool.h"
#include "lib/oblog/ob_log_module.h"

#include "lib/mysqlclient/ob_mysql_connection_pool.h"
#include "lib/mysqlclient/ob_server_connection_pool.h"
#include "lib/mysqlclient/ob_mysql_server_provider.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/thread/thread_mgr.h"

namespace oceanbase
{
namespace common
{
namespace sqlclient
{
const char *const ObMySQLConnectionPool::DEFAULT_DB_USER = OB_INTERNAL_USER;
const char *const ObMySQLConnectionPool::DEFAULT_DB_PASS = "";
const char *const ObMySQLConnectionPool::DEFAULT_DB_NAME = "default";

ObTenantServerConnectionPool::ObTenantServerConnectionPool()
  : cursor_(0),
    server_pool_list_("ObMySQLConnPool", OB_MALLOC_NORMAL_BLOCK_SIZE)
{}

ObTenantServerConnectionPool::~ObTenantServerConnectionPool()
{
  reset();
}

void ObTenantServerConnectionPool::reset()
{
  cursor_ = 0;
  server_pool_list_.reset();
}

int ObTenantServerConnectionPool::get_server_pool(ObServerConnectionPool *&server_pool)
{
  int ret = OB_SUCCESS;
  server_pool = NULL;
  const int64_t tenant_server_cnt = server_pool_list_.count();

  if (OB_UNLIKELY(0 >= tenant_server_cnt)) {
    ret = OB_RESOURCE_OUT;
    LOG_WARN("tenant_server_list must not be empty", K(ret), K(tenant_server_cnt));
  } else {
    cursor_ = (cursor_ + 1) % tenant_server_cnt;

    if (OB_UNLIKELY(OB_FAIL(server_pool_list_.at(cursor_, server_pool)))) {
    } else if (OB_ISNULL(server_pool)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("server_conn_pool iter must not null", K(ret));
    }
  }

  return ret;
}

int ObTenantServerConnectionPool::renew(const TenantServerConnArray &new_server_conn_pool_list)
{
  int ret = OB_SUCCESS;
  cursor_ = 0;
  server_pool_list_.reset();

  if (OB_FAIL(server_pool_list_.assign(new_server_conn_pool_list))) {
    LOG_ERROR("assign new_server_conn_pool_list to tenant_server_conn_pool failed", K(ret), KPC(this));
  }

  return ret;
}

ObMySQLConnectionPool::ObMySQLConnectionPool()
  : is_updated_(false),
    is_stop_(false),
    is_use_ssl_(true),
    mode_(ObMySQLConnection::OCEANBASE_MODE),
    pool_type_(MySQLConnectionPoolType::SERVER_POOL),
    tg_id_(-1),
    server_provider_(NULL),
    busy_conn_count_(0),
    user_info_lock_(obsys::WRITE_PRIORITY),
    config_(),
    get_lock_(obsys::WRITE_PRIORITY),
    dblink_pool_lock_(obsys::WRITE_PRIORITY),
    allocator_(ObModIds::OB_SQL_CONNECTION_POOL),
    server_list_(allocator_),
    tenant_server_pool_map_(),
    server_pool_(),
    check_read_consistency_(true)
{
  int ret = OB_SUCCESS;
  set_db_param(DEFAULT_DB_USER, DEFAULT_DB_PASS, DEFAULT_DB_NAME);
  init_sql_[0] = '\0';

  if (OB_UNLIKELY(0 != mysql_library_init(0, NULL, NULL))) {
    LOG_WARN_RET(OB_ERROR, "could not initialize MySQL library");
  } else if (OB_FAIL(tenant_server_pool_map_.init(ObModIds::LIB_OBSQL))) {
    LOG_WARN("init tenant_server_pool_map_ failed", K(ret));
  }

  // set defult values
  config_.sqlclient_wait_timeout_ = 10;             // 10s
  config_.long_query_timeout_ = 120*1000*1000;      // 120s
  config_.connection_refresh_interval_ = 200*1000;  // 200ms
  config_.connection_pool_warn_time_ = 1*1000*1000; // 1s
  config_.sqlclient_per_observer_conn_limit_ = 256; // dblink connection limits is 256
}

ObMySQLConnectionPool::~ObMySQLConnectionPool()
{
  ObServerConnectionPool *pool = NULL;
  obsys::ObWLockGuard lock(get_lock_);
  tenant_server_pool_map_.destroy();

  for (ServerList::iterator iter = server_list_.begin();
      iter != server_list_.end(); iter++) {
    pool = *iter;
    // pool->~ObServerConnectionPool();
    // so that ObCachedAllocator can destroy and free the memory
    server_pool_.free(pool);
  }

  server_list_.clear();
  // delete mysql_library_end() because not all thread exit when the pool deconstruct
  // mysql_library_end();

  stop();

  if (tg_id_ != -1) {
    TG_DESTROY(tg_id_);
    tg_id_ = -1;
  }
}

void ObMySQLConnectionPool::set_server_provider(ObMySQLServerProvider *provider)
{
  server_provider_ = provider;
}

int ObMySQLConnectionPool::set_db_param(const char *db_user, const char *db_pass,
                                         const char *db_name)
{
  int ret = OB_SUCCESS;
  int64_t w_len = 0;
  obsys::ObWLockGuard lock(user_info_lock_);
  if (OB_ISNULL(db_user) || OB_ISNULL(db_pass) || OB_ISNULL(db_name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid db param", KP(db_user), KP(db_pass), KP(db_name), K(ret));
  } else if (FALSE_IT(w_len = snprintf(db_user_, OB_MAX_USER_NAME_BUF_LENGTH, "%s", db_user))) {
    // impossible
  } else if (OB_UNLIKELY(w_len <= 0) || OB_UNLIKELY(w_len > OB_MAX_USER_NAME_LENGTH)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fill db user error", KCSTRING(db_user), K(ret));
  } else if (FALSE_IT(w_len = snprintf(db_pass_, OB_MAX_PASSWORD_BUF_LENGTH, "%s", db_pass))) {
    // impossible
  } else if (OB_UNLIKELY(w_len < 0) || OB_UNLIKELY(w_len >= OB_MAX_PASSWORD_BUF_LENGTH)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fill db password error", KCSTRING(db_pass), K(ret));
  } else if (FALSE_IT(w_len = snprintf(db_name_, OB_MAX_DATABASE_NAME_BUF_LENGTH, "%s", db_name))) {
    // impossible
  } else if (OB_UNLIKELY(w_len <= 0) || OB_UNLIKELY(w_len > OB_MAX_DATABASE_NAME_LENGTH)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fill db database name error", KCSTRING(db_name), K(ret));
  }
  return ret;
}

int ObMySQLConnectionPool::set_db_param(const ObString &db_user, const ObString &db_pass,
                                         const ObString &db_name)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(db_user.length() > OB_MAX_USER_NAME_LENGTH)
      || OB_UNLIKELY(db_pass.length() > OB_MAX_PASSWORD_LENGTH)
      || OB_UNLIKELY(db_name.length() > OB_MAX_DATABASE_NAME_LENGTH)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("db param buffer is not enough", K(db_user), K(db_pass), K(db_name), K(ret));
  } else {
    obsys::ObWLockGuard lock(user_info_lock_);
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
  if (! is_stop_) {
    int ret = OB_SUCCESS;
    if (tg_id_ != -1) {
      int origin_tg_id = tg_id_;
      if (OB_FAIL(TG_CANCEL_ALL(tg_id_))) {
        LOG_ERROR("fail to cancel timer task", K(ret), K(tg_id_), K(is_stop_), K(this));
      } else {
        TG_STOP(tg_id_);
        TG_WAIT(tg_id_);
        tg_id_ = -1;
        LOG_INFO("ObMySQLConnectionPool stop succ", K(origin_tg_id));
      }
    }
    is_stop_ = true;
  }
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
  obsys::ObWLockGuard lock(get_lock_);
  ObServerConnectionPool *pool = NULL;
  for (ServerList::iterator iter = server_list_.begin();
      iter != server_list_.end(); iter++) {
    if (OB_ISNULL(pool = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected error, pool is null", K(ret));
    } else {
      pool->close_all_connection();
    }
  }
}

void ObMySQLConnectionPool::mark_all_server_connection_gone()
{
  int ret = OB_SUCCESS;
  ObServerConnectionPool *pool = NULL;
  for (ServerList::iterator iter = server_list_.begin();
      iter != server_list_.end(); iter++) {
    if (OB_ISNULL(pool = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected error, pool is null", K(ret));
    } else {
      pool->reset_idle_conn_to_sys_tenant();
      pool->set_server_gone(true);
    }
  }
}


// need outter lock protection
int ObMySQLConnectionPool::renew_server_connection_pool(common::ObAddr &server)
{
  int ret = OB_SUCCESS;
  ObServerConnectionPool *pool = NULL;
  bool found = false;
  for (ServerList::iterator iter = server_list_.begin();
      OB_SUCC(ret) && !found && iter != server_list_.end(); iter++) {
    if (OB_ISNULL(pool = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pool is null", K(ret));
    } else if (pool->get_server() == server) {
      LOG_TRACE("server pool renewed", K(server));
      pool->renew();
      found = true;
    }
  }

  if (OB_SUCC(ret) && false == found) {
    // not found this server in connection pool. should be a newly online one
    ret = OB_ITER_END;
  }
  return ret;
}

int ObMySQLConnectionPool::create_server_connection_pool(const common::ObAddr &server)
{
  int ret = OB_SUCCESS;
  ObServerConnectionPool *pool = server_pool_.alloc();
  if (OB_ISNULL(pool)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("out of memory", K(ret));
  } else if (OB_FAIL(pool->init(this, server, config_.sqlclient_per_observer_conn_limit_))) {
    LOG_WARN("fail to init connection pool", K(ret));
  } else if (OB_FAIL(server_list_.push_back(pool))) {
    LOG_WARN("push pool to list fail. destroyed", K(ret));
  } else {
    LOG_INFO("new server pool created", K(server), K(config_.sqlclient_per_observer_conn_limit_));
  }

  if (OB_FAIL(ret) && OB_LIKELY(NULL != pool)) {
    server_pool_.free(pool); // put back to cache. prevent memory leak
    pool = NULL;
  }
  return ret;
}

// need outter lock protection
int ObMySQLConnectionPool::purge_connection_pool()
{
  int ret = OB_SUCCESS;
  ObServerConnectionPool *pool = NULL;
  int64_t now = ::oceanbase::common::ObTimeUtility::current_time();
  ObArray<ObList<ObServerConnectionPool *, common::ObArenaAllocator>::iterator > to_delete;
  for (ServerList::iterator iter = server_list_.begin();
      OB_SUCC(ret) && iter != server_list_.end(); iter++) {
    if (OB_ISNULL(pool = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pool is null", K(ret));
    } else if (0 == pool->get_busy_count() &&
        MAX_SERVER_GONE_INTERVAL < now - pool->last_renew_time()
        ) {
      /* When can we release the ServerConnectionPool:
       *  - all connection of the pool have been released back to pool
       *  - and it has been a 'very long' time that the server has gone
       */
      pool->set_server_gone(true); // defensive code, not required.
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
      if (OB_FAIL(server_list_.erase(to_delete.at(i)))) {
        LOG_WARN("fail to delete pool from server_list", K(ret));
      }
    }
  }
  return ret;
}

// External caller lock
int ObMySQLConnectionPool::get_pool(const uint64_t tenant_id, ObServerConnectionPool *&pool)
{
  // Note: Round-Robin while getting server from server list.
  RLOCAL(int64_t, cursor);
  int ret = OB_SUCCESS;
  pool = NULL;
  if (server_list_.size() > 0) {
    // get pool from server_list_ if tenant_id is invalid or tenant_server_pool_map_ is empty
    // otherwise get pool from tenant_server_pool_map_
    if (MySQLConnectionPoolType::SERVER_POOL == pool_type_) {
      cursor = (cursor + 1) % server_list_.size();
      int64_t pos = cursor;
      ServerList::iterator iter;
      for (iter = server_list_.begin(); pos > 0 && iter != server_list_.end(); iter++, pos--) {
        // nop
      }

      if (OB_UNLIKELY(iter == server_list_.end())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("iter should not end.", K(pos), K(*(&cursor)),
            K(server_list_.size()));
      } else if (OB_ISNULL(*iter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("pool iter must not null", K(ret));
      } else {
        pool = *iter;
      }
    } else if (MySQLConnectionPoolType::TENANT_POOL == pool_type_) {
      ObTenantServerConnectionPool *tenant_server_pool = NULL;

      if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_ERROR("tenant_id should be valid in TENANT_POOL mode", K(ret), K(tenant_id));
      } else if (OB_FAIL(get_tenant_server_pool(tenant_id, tenant_server_pool))) {
        LOG_ERROR("get_tenant_server_pool failed", K(ret), K(tenant_id));
      } else if (OB_ISNULL(tenant_server_pool)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid tenant_server_pool", K(ret), K(tenant_id));
      } else if (OB_FAIL(tenant_server_pool->get_server_pool(pool))) {
        LOG_ERROR("get_server_pool from tenant_server_pool failed", K(ret), K(tenant_id));
      } else if (OB_ISNULL(pool)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("server_pool must not be null", K(ret), K(tenant_id));
      }

      if (OB_NOT_NULL(tenant_server_pool)) {
        tenant_server_pool_map_.revert(tenant_server_pool);
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid connection_pool_type", K(ret), K(pool_type_), K(tenant_id));
    }
  } else {
    ret = OB_RESOURCE_OUT;
    LOG_ERROR("server list must not be empty", K(ret));
  }

  return ret;
}

int ObMySQLConnectionPool::get_tenant_server_pool(const uint64_t tenant_id, ObTenantServerConnectionPool *&tenant_server_pool)
{
  int ret = OB_SUCCESS;
  tenant_server_pool = NULL;
  // use user_tenant_id for meta tenant.
  uint64_t usr_tenant_id = gen_user_tenant_id(tenant_id);

  if (OB_FAIL(tenant_server_pool_map_.get(usr_tenant_id, tenant_server_pool))) {
    LOG_ERROR("get tenant_server_pool from tenant_server_pool_map_ failed", K(ret), K(tenant_id), K(usr_tenant_id));
  } else if (OB_ISNULL(tenant_server_pool)) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_ERROR("tenant_server_pool is null", K(ret), K(tenant_id), K(usr_tenant_id));
  } else {
    LOG_TRACE("get tenant_server_pool succ", K(ret), K(this), K(tenant_id), K(usr_tenant_id), K(tenant_server_pool));
  }

  return ret;
}

int ObMySQLConnectionPool::acquire(const uint64_t tenant_id, ObMySQLConnection *&connection)
{
  int ret = OB_SUCCESS;
  const int64_t server_count = get_server_count();

  connection = NULL;
  bool acquire_succ = false;
  for (int64_t i = 0; !acquire_succ && i < server_count; ++i) {
    if (OB_FAIL(do_acquire(tenant_id, connection))) {
      LOG_WARN("fail to get connection", K(ret), K(tenant_id));
    } else if (OB_FAIL(try_connect(connection))) {
      LOG_WARN("failed to try connection, will release connection", K(ret), K(tenant_id));
      const bool succ = false;
      if (OB_SUCCESS != release(connection, succ)) { // ignore ret
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
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to acquire connection",
             K(this), K(tenant_id), K(server_count), K(busy_conn_count_), K(ret));
    obsys::ObRLockGuard lock(get_lock_);
    for (ServerList::iterator iter = server_list_.begin();
        iter != server_list_.end(); iter++) {
      if (OB_NOT_NULL(*iter)) {
        (*iter)->dump();
      }
    }
  } else {
    connection->set_mode(mode_);
  }
  return ret;
}

int64_t ObMySQLConnectionPool::get_server_count() const
{
  obsys::ObRLockGuard lock(get_lock_);
  return server_list_.size();
}

int ObMySQLConnectionPool::do_acquire(const uint64_t tenant_id, ObMySQLConnection *&connection)
{
  int ret = OB_SUCCESS;
  obsys::ObRLockGuard lock(get_lock_);
  ObServerConnectionPool *pool = NULL;
  if (OB_FAIL(get_pool(tenant_id, pool))) {
    LOG_WARN("failed to get pool", K(ret));
  } else if (OB_FAIL(pool->acquire(connection, 0))) {
    LOG_WARN("failed to get connection", K(ret));
  } else if (OB_ISNULL(connection)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("connection must not be null", K(ret));
  } else if (OB_UNLIKELY(connection->is_busy())) {
    ret =  OB_ERR_SYS;
    LOG_WARN("connection is busy, should not happen", K(ret));
  } else {
    ATOMIC_INC((uint64_t *)&busy_conn_count_);
    connection->set_busy(true);
    connection->set_timestamp(::oceanbase::common::ObTimeUtility::current_time());
    LOG_TRACE("connection acquire", K(this), K(tenant_id), K(busy_conn_count_), K(connection), K(pool));
  }
  return ret;
}

int ObMySQLConnectionPool::try_connect(ObMySQLConnection *connection)
{
  int ret = OB_SUCCESS;
  char db_user[OB_MAX_USER_NAME_BUF_LENGTH] = {0};
  char db_pass[OB_MAX_PASSWORD_BUF_LENGTH] = {0};
  char db_name[OB_MAX_DATABASE_NAME_BUF_LENGTH] = {0};
  // copy db_user infos within lock
  {
    obsys::ObRLockGuard lock(user_info_lock_);
    MEMCPY(db_user, db_user_, OB_MAX_USER_NAME_BUF_LENGTH);
    MEMCPY(db_pass, db_pass_, OB_MAX_PASSWORD_BUF_LENGTH);
    MEMCPY(db_name, db_name_, OB_MAX_DATABASE_NAME_BUF_LENGTH);
  }

  if (OB_ISNULL(connection)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("connection must not null", K(ret));
  } else if (connection->is_closed()) {
    connection->set_timeout(config_.sqlclient_wait_timeout_);
    if (OB_FAIL(connection->connect(db_user, db_pass, db_name, is_use_ssl_))) {
      LOG_WARN("fail to connect to server",
               K(connection->get_server()), K(ret));
    } else if ('\0' != init_sql_[0]) {
      // TODO oushen, not support now
      //if (OB_SUCCESS != (ret = execute_init_sql(connection))) {
      //  LOG_WARN("failed to execute_init_sql, ret=%d", ret);
      //}
    }
    if (OB_SUCC(ret) && ObMySQLConnection::OCEANBASE_MODE == mode_) {
      // bugfix:
      if (OB_FAIL(connection->init_oceanbase_connection())) {
        LOG_WARN("fail to init oceanabse connection", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (!check_read_consistency_) {
        // if connect to mysql, can not set ob_read_consistency
        // TODO, donglou, remove later
        connection->set_read_consistency_strong();
      } else {
        if (OB_FAIL(connection->set_timeout_variable(
                  config_.long_query_timeout_,
                  DEFAULT_TRANSACTION_TIMEOUT_US))) {
          LOG_WARN("fail to set mysql timeout variablse", K(ret));
          if (ObMySQLConnection::DEBUG_MODE == mode_) {
            ret = OB_SUCCESS;
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    //connection->set_trace_id();
  }
  return ret;
}

int ObMySQLConnectionPool::execute_init_sql(ObMySQLConnection *connection)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(connection)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid connection", K(connection), K(ret));
  } else {
    ObMySQLStatement stmt;
    int64_t affect_rows = 0;
    if (OB_FAIL(connection->create_statement(
        stmt, OB_SYS_TENANT_ID, init_sql_))) {
      LOG_WARN("create statement failed", K(connection), K(ret));
    } else if (OB_FAIL(stmt.execute_update(affect_rows))) {
      LOG_WARN("execute sql failed", K(connection),
               K(connection->get_server()), KCSTRING(init_sql_), K(ret));
    }
  }

  return ret;
}

int ObMySQLConnectionPool::release(ObMySQLConnection *connection, const bool succ)
{
  int ret = OB_SUCCESS;
  obsys::ObRLockGuard lock(get_lock_);
  ObServerConnectionPool *pool = NULL;
  if (OB_ISNULL(connection)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("connection is null, cannot be released", K(ret));
  } else {
    const int64_t cost_time = ::oceanbase::common::ObTimeUtility::current_time() - connection->get_timestamp();
    if (cost_time > config_.connection_pool_warn_time_) {
      LOG_WARN_RET(OB_ERR_TOO_MUCH_TIME, "this connection cost too much time", K(this), K(cost_time),
               K(config_.connection_pool_warn_time_), K(connection->get_server()),
               "start time", time2str(connection->get_timestamp()));
    }
    //reset_trace_id(connection);//we just set a new one when acquire next time
    if (OB_ISNULL(pool = connection->get_root())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to release connection. connection pool not set", K(ret));
    } else if (OB_FAIL(pool->release(connection, succ))) {
      LOG_WARN("fail to release connection", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    ATOMIC_DEC((uint64_t *)&busy_conn_count_);
  }
  LOG_TRACE("release connection to mysql connection pool", K(this), K(busy_conn_count_),
            KP(connection), K(connection->is_closed()), K(pool), K(ret), K(connection->get_sessid()), K(lbt()));
  return ret;
}

void ObMySQLConnectionPool::runTimerTask()
{
  int ret = OB_SUCCESS;
  int64_t count = 0;
  int64_t i = 0;
  int64_t refresh_period_ms = 0;
  common::ObAddr server;

  LOG_TRACE("start timer task for refresh connection pool");
  if (OB_ISNULL(server_provider_) || -1 == tg_id_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid connection pool", K(server_provider_), K(tg_id_), K(ret));
  } else {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = server_provider_->prepare_refresh())) {
      //Ignore the error code and does not affect the subsequent process
      LOG_WARN("failed to prepare refresh", K(ret), K(tmp_ret));
    }
    obsys::ObWLockGuard lock(get_lock_);
    if (OB_FAIL(server_provider_->refresh_server_list())) {
      if (!is_updated_) { // has never successfully updated, it is in startup time, should not print ERROR
        LOG_INFO("fail to refresh mysql server list in startup time, it's normal", K(ret));
      } else {
        LOG_WARN("fail to refresh mysql server list", K(ret));
      }
    } else if ((count = server_provider_->get_server_count()) == 0) {
      LOG_TRACE("no alive ms found, skip refresh conncetion pool");
    } else {
      is_updated_ = true;
      mark_all_server_connection_gone();
      for (i = 0; i < count; i++) {
        /* steps:
         * 1. renew old ms
         * 2. add new-born ms
         * 3. filter out dead or idle ms
         */
        if (OB_FAIL(server_provider_->get_server(i, server))) {
          if (OB_ENTRY_NOT_EXIST != ret) {
            LOG_ERROR("can't get mysql server",
                K(count), "cur_index", i, K(ret));
          } else {
            LOG_WARN("mysql server at specified pos of server_provider_ is not exist, skip", K(ret),
                K(count), "cur_index", i);
            ret = OB_SUCCESS;
          }
        } else if (OB_FAIL(renew_server_connection_pool(server))) {
          if (OB_UNLIKELY(OB_ITER_END != ret)) {
            LOG_ERROR("fail to renew connection server", K(ret));
          } else if (OB_FAIL(create_server_connection_pool(server))) {
            LOG_ERROR("fail to create new connection to server", K(server), K(ret));
          }
        }
      }

      // update connection pool
      // - remove invalid server connection pool
      // - close long idle connection
      // - renew tenant_server_conn_pool_map
      if (OB_FAIL(purge_connection_pool())) {
        LOG_ERROR("fail to update mysql connection pool", K(ret));
      }
    }

    // renew_tenant_server_pool_map should be after renew_server_connection_pool, and should not be
    // affetced by the renew result of server_conn_pool.
    if (MySQLConnectionPoolType::TENANT_POOL == pool_type_ && OB_FAIL(renew_tenant_server_pool_map())) {
      LOG_ERROR("renew_tenant_server_pool_map failed", K(ret));
    }

    if (OB_FAIL(server_provider_->end_refresh())) {
      LOG_WARN("server_provider_ end_refresh failed", K(ret), K(this));
    }
    // end LOCK BLOCK
    if (count > 0) {
      refresh_period_ms = config_.connection_refresh_interval_ * 50; // default 10s
    } else if (server_provider_->need_refresh()) {
      refresh_period_ms = config_.connection_refresh_interval_; // default 200ms=0.2s
    } else {
      refresh_period_ms = config_.connection_refresh_interval_ * 500; //100S;
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

int ObMySQLConnectionPool::acquire(const uint64_t tenant_id, ObISQLConnection *&conn, ObISQLClient *client_addr, const int32_t group_id)
{
  int ret = OB_SUCCESS;
  UNUSED(client_addr);
  UNUSED(group_id);
  ObMySQLConnection *mysql_conn = NULL;
  conn = NULL;
  if (OB_FAIL(acquire(tenant_id, mysql_conn))) {
    LOG_WARN("acquire connection failed", K(ret));
  } else {
    conn = mysql_conn;
  }
  return ret;
}

int ObMySQLConnectionPool::release(ObISQLConnection *conn, const bool success)
{
  int ret = OB_SUCCESS;
  if (NULL != conn) {
    // FIXME baihua: add ObISQLConnection::revert() for connection release to avoid dynamic cast
    ObMySQLConnection *mysql_conn = dynamic_cast<ObMySQLConnection *>(conn);
    if (OB_ISNULL(mysql_conn)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ObMySQLConnectionPool only release ObMySQLConnection", K(ret));
    } else if (OB_FAIL(release(mysql_conn, success))) {
      LOG_WARN("release connection failed", K(ret));
    }
  }
  return ret;
}

int ObMySQLConnectionPool::escape(const char *from, const int64_t from_size,
      char *to, const int64_t to_size, int64_t &out_size)
{
  ObMySQLConnection *conn = NULL;
  int ret = OB_SUCCESS;
  if (OB_FAIL(acquire(OB_SYS_TENANT_ID, conn))) {
    // use SYS_TENANT connection for escape
    LOG_WARN("acquire connection failed", K(ret));
  } else if (OB_ISNULL(conn)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("connection should not be NULL", K(ret));
  } else if (OB_FAIL(conn->escape(from, from_size, to, to_size, out_size))) {
    LOG_WARN("escape string failed", KCSTRING(from), K(from_size),
             KCSTRING(to), K(to_size), K(ret));
  }
  if (NULL != conn) {
    int tmp_ret = release(conn, OB_SUCCESS == ret);
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("release connection failed", K(tmp_ret));
    }
  }
  return ret;
}
int ObMySQLConnectionPool::create_dblink_pool(const dblink_param_ctx &param_ctx, const ObAddr &server,
                                              const ObString &db_tenant, const ObString &db_user,
                                              const ObString &db_pass, const ObString &db_name,
                                              const common::ObString &conn_str,
                                              const common::ObString &cluster_str)
{
  UNUSEDx(param_ctx);
  int ret = OB_SUCCESS;
  ObServerConnectionPool *dblink_pool = NULL;

  if (OB_FAIL(get_dblink_pool(param_ctx, dblink_pool))) {
    LOG_WARN("fail to get dblink connection pool", K(param_ctx));
  } else if (OB_NOT_NULL(dblink_pool)) {
    // nothing.
  } else {
    // can not use obsys::ObRLockGuard lock(get_lock_), it's useless
    // can not use obsys::ObWLockGuard lock(get_lock_), cause it will have dead lock
    // use a new lock for create_dblink_pool
    obsys::ObWLockGuard lock(dblink_pool_lock_);
    if (OB_FAIL(get_dblink_pool(param_ctx, dblink_pool))) { //get again
    LOG_WARN("fail to get dblink connection pool", K(param_ctx));
    } else if (OB_NOT_NULL(dblink_pool)) {
      // nothing.
    } else if (OB_ISNULL(dblink_pool = server_pool_.alloc())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("out of memory", K(ret));
    } else if (OB_FAIL(dblink_pool->init_dblink(param_ctx.tenant_id_, param_ctx.dblink_id_,
                                                server, db_tenant, db_user, db_pass,
                                                db_name, conn_str, cluster_str,
                                                this, config_.sqlclient_per_observer_conn_limit_))) {
      LOG_WARN("fail to init dblink connection pool", K(ret));
    } else if (OB_FAIL(server_list_.push_back(dblink_pool))) {
      LOG_WARN("fail to push pool to list", K(ret));
    } else {
      LOG_DEBUG("new dblink pool created", K(server), K(config_.sqlclient_per_observer_conn_limit_));
    }
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(dblink_pool)) {
    server_pool_.free(dblink_pool); // put back to cache. prevent memory leak
    dblink_pool = NULL;
  }
  return ret;
}

int ObMySQLConnectionPool::acquire_dblink(const dblink_param_ctx &param_ctx, ObISQLConnection *&dblink_conn)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(do_acquire_dblink(param_ctx, dblink_conn))) {
    LOG_WARN("fail to acquire dblink", K(ret), K(param_ctx));
  } else if (OB_FAIL(try_connect_dblink(dblink_conn, param_ctx.sql_request_level_))) {
    LOG_WARN("fail to try connect dblink", K(ret), K(param_ctx));
    int release_ret = release_dblink(dblink_conn);
    if (release_ret != OB_SUCCESS) {
      LOG_WARN("fail to release dblink conn", K(release_ret), K(param_ctx));
    }
    dblink_conn = NULL;
  }
  return ret;
}

int ObMySQLConnectionPool::release_dblink(ObISQLConnection *dblink_conn)
{
  int ret = OB_SUCCESS;
  const bool succ = OB_NOT_NULL(dblink_conn) ? dblink_conn->usable() : false;
  if (OB_FAIL(release(dynamic_cast<ObMySQLConnection *>(dblink_conn), succ))) {
    LOG_WARN("fail to release dblink conn", K(ret));
  }
  return ret;
}

int ObMySQLConnectionPool::get_dblink_pool(const dblink_param_ctx &param_ctx, ObServerConnectionPool *&dblink_pool)
{
  int ret = OB_SUCCESS;
  ObServerConnectionPool *pool = NULL;
  dblink_pool = NULL;
  for (ServerList::iterator iter = server_list_.begin();
      OB_SUCC(ret) && iter != server_list_.end(); iter++) {
    if (OB_ISNULL(pool = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pool is null", K(ret));
    } else if (param_ctx.dblink_id_ == pool->get_dblink_id() &&
               param_ctx.tenant_id_ == pool->get_tenant_id()) {
      dblink_pool = pool;
      break;
    }
  }
  return ret;
}

int ObMySQLConnectionPool::do_acquire_dblink(const dblink_param_ctx &param_ctx, ObISQLConnection *&dblink_conn)
{
  int ret = OB_SUCCESS;
  ObServerConnectionPool *dblink_pool = NULL;
  ObMySQLConnection *dblink_conn1 = NULL;
  if (OB_FAIL(get_dblink_pool(param_ctx, dblink_pool))) {
    LOG_WARN("failed to get dblink pool", K(ret), K(param_ctx));
  } else if (OB_ISNULL(dblink_pool)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dblink pool is NULL", K(ret));
  } else if (OB_FAIL(dblink_pool->acquire(dblink_conn1, param_ctx.sessid_))) {
    LOG_WARN("failed to acquire dblink conn", K(ret));
  } else if (OB_ISNULL(dblink_conn1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dblink conn is NULL", K(ret));
  } else if (OB_UNLIKELY(dblink_conn1->is_busy())) {
    ret =  OB_ERR_SYS;
    LOG_WARN("dblink conn is busy, should not happen", K(ret));
  } else {
    ATOMIC_INC((uint64_t *)&busy_conn_count_);
    dblink_conn1->set_busy(true);
    dblink_conn1->set_dblink_id(param_ctx.dblink_id_);
    dblink_conn1->set_timestamp(::oceanbase::common::ObTimeUtility::current_time());
    dblink_conn = static_cast<ObISQLConnection *>(dblink_conn1);
    LOG_TRACE("acquire connection from mysql connection pool", K(this), K(busy_conn_count_), KP(dblink_conn), K(dblink_conn1->is_closed()), K(dblink_pool), K(param_ctx));
  }
  return ret;
}

int ObMySQLConnectionPool::try_connect_dblink(ObISQLConnection *dblink_conn, int64_t sql_request_level)
{
  int ret = OB_SUCCESS;
  ObMySQLConnection *dblink_conn1 = static_cast<ObMySQLConnection *>(dblink_conn);
  if (OB_ISNULL(dblink_conn1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("dblink conn is NULL", K(ret));
  } else if (dblink_conn1->is_closed()) {
    dblink_conn1->set_timeout(config_.sqlclient_wait_timeout_);
    LOG_TRACE("set dblink timeout and sql request level", K(sql_request_level), K(config_.sqlclient_wait_timeout_), K(lbt()), K(ret));
    if (OB_FAIL(dblink_conn1->connect_dblink(is_use_ssl_, sql_request_level))) {
      LOG_WARN("fail to connect dblink", K(dblink_conn1->get_server()), K(ret));
    } else if (OB_FAIL(dblink_conn1->set_timeout_variable(config_.long_query_timeout_,
                                                         DEFAULT_TRANSACTION_TIMEOUT_US))) {
      LOG_WARN("fail to set mysql timeout variablse", K(ret));
      dblink_conn1->close();
    }
  }
  return ret;
}

// update tenant_server_conn_pool_map
// - get_tenant_id_list
// - get_tenant_server_addr_list
// - get_tenant_server_pool_list
// - renew tenant_server_pool_map
// - purge tenant_server_pool if tenant not serving
int ObMySQLConnectionPool::renew_tenant_server_pool_map()
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 16> tenant_array("OBMySQLConnPool", OB_MALLOC_NORMAL_BLOCK_SIZE);

  if (OB_ISNULL(server_provider_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("server_provider_ must be valid", K(ret));
  } else if (OB_FAIL((server_provider_->get_tenant_ids(tenant_array)))) {
    LOG_ERROR("get_tenant_ids by server_provider_ failed", K(ret));
  } else {
    // renew connection_pool for each tenant
    // skip error in case of on tenant renew fail affect others
    for (int64_t tenant_idx = 0; tenant_idx < tenant_array.count(); tenant_idx++) {
      uint64_t tenant_id = OB_INVALID_TENANT_ID;

      if (OB_FAIL(tenant_array.at(tenant_idx, tenant_id))) {
        LOG_ERROR("get_tenant_id failed", K(ret), K(tenant_idx), K(tenant_array));
      } else if (OB_FAIL(renew_tenant_server_pool_(tenant_id))) {
        LOG_WARN("renew_tenant_server_pool_ failed", K(ret), K(tenant_id), K(tenant_idx), K(tenant_array));
      }
    } // end for tenant_array

    if (OB_FAIL(purge_tenant_server_pool_map_(tenant_array))) {
      LOG_WARN("purge_tenant_server_pool_map_ failed, skip this error", K(ret), K(tenant_array));
      ret = OB_SUCCESS;
    } else {
      LOG_TRACE("renew tenant_server_conn_pool_map succ");
    }
  }


  return ret;
}

int ObMySQLConnectionPool::renew_tenant_server_pool_(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObAddr, 16> server_array("OBMySQLConnPool", OB_MALLOC_NORMAL_BLOCK_SIZE);
  TenantServerConnArray tenant_server_list("OBMySQLConnPool", OB_MALLOC_NORMAL_BLOCK_SIZE);

  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("tenant_id to refresh tenant_server_conn_pool must be valid", K(tenant_id));
  } else if (OB_FAIL(server_provider_->get_tenant_servers(tenant_id, server_array))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      LOG_WARN("tenant may already been dropped, skip build this tenant conn pool", K(tenant_id));
      ret = OB_SUCCESS;
    } else {
      LOG_ERROR("get_tenant_servers by server_provider_ failed", K(ret), K(tenant_id));
    }
  } else if (OB_UNLIKELY(server_array.count() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected can't find any server for tenant", K(ret), K(tenant_id));
  } else {
    for (int server_idx = 0; OB_SUCC(ret) && server_idx < server_array.count(); server_idx++) {
      ObAddr server_addr;
      ObServerConnectionPool *pool = NULL;

      if (OB_FAIL(server_array.at(server_idx, server_addr))) {
        LOG_ERROR("get server failed", K(ret), K(tenant_id), K(server_idx), K(server_array));
      } else if (OB_FAIL(get_server_pool_(server_addr, pool))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          LOG_WARN("can't find server in server_pool_list_, skip this server", K(ret), K(tenant_id), K(server_addr));
          ret = OB_SUCCESS;
        } else {
          LOG_ERROR("get_server_pool_ for specified server failed", K(ret), K(tenant_id), K(server_addr));
        }
      } else if (OB_ISNULL(pool)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("get invalid server_pool", K(ret), K(tenant_id), K(server_addr));
      } else if (OB_FAIL(tenant_server_list.push_back(pool))) {
        LOG_ERROR("push_back server_pool into tenant_server_list failed", K(ret),
            K(tenant_id), K(server_addr));
      }
    } // end for server_array

    if (OB_SUCC(ret) && tenant_server_list.count() > 0) {
      // try get TenantServerConnPoll from TenantServerConnMap by tenant_id
      // create if not exist
      ObTenantServerConnectionPool *tenant_server_conn_pool = NULL;

      if (OB_FAIL(tenant_server_pool_map_.get(tenant_id, tenant_server_conn_pool))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          if (OB_FAIL(tenant_server_pool_map_.alloc_value(tenant_server_conn_pool))) {
            LOG_ERROR("alloc tenant_server_conn_pool failed", K(ret), K(tenant_id));
          } else if (OB_FAIL(tenant_server_pool_map_.insert_and_get(tenant_id, tenant_server_conn_pool))) {
            LOG_ERROR("insert tenant_server_conn_pool failed", K(ret), K(tenant_id));
          } else {
            LOG_INFO("[STAT][TENANT_CONN_POOL][ADD] alloc tenant_server_conn_pool succ", K(tenant_id));
          }
        } else {
          LOG_ERROR("get_tenant_server_pool failed", K(ret), K(tenant_id));
        }
      }

      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(tenant_server_conn_pool)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("tenant_server_conn_pool is null", K(ret), K(tenant_id));
      } else if (OB_FAIL(tenant_server_conn_pool->renew(tenant_server_list))) {
        LOG_ERROR("refresh tenant_server_conn_pool failed", K(ret),
            K(tenant_id), K(tenant_server_list));
      } else {
        LOG_TRACE("[STAT][TENANT_CONN_POOL][RENEW_SVR]", K(tenant_id), K(tenant_server_list));
      }

      if (OB_NOT_NULL(tenant_server_conn_pool)) {
        tenant_server_pool_map_.revert(tenant_server_conn_pool);
      }
    }
  }

  return ret;
}

int ObMySQLConnectionPool::get_server_pool_(const ObAddr &addr, ObServerConnectionPool *&pool)
{
  int ret = OB_SUCCESS;
  bool found = false;

  if (OB_UNLIKELY(!addr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("server addr should be valid", K(ret), K(addr));
  } else {
    for(ServerList::iterator iter = server_list_.begin();
        OB_SUCC(ret) && ! found && iter != server_list_.end();
        iter++) {
      if (OB_ISNULL(pool = *iter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("pool is null", K(ret), K(addr));
      } else if (pool->get_server() == addr) {
        found = true;
      }
    }
  }

  if (OB_SUCC(ret) && ! found) {
    ret = OB_ENTRY_NOT_EXIST;
  }

  return ret;
}

int ObMySQLConnectionPool::purge_tenant_server_pool_map_(const ObIArray<uint64_t> &tenant_array)
{
  int ret = OB_SUCCESS;
  TenantServerConnPoolPurger purger(tenant_array, allocator_);
  if (OB_FAIL(tenant_server_pool_map_.remove_if(purger))) {
    LOG_ERROR("purge tenant_server_pool_map_ failed", K(ret), K(tenant_array));
  } else {
    _LOG_TRACE("[STAT][TENANT_CONN_POOL][PURGE] POOL=%p, PURGE_COUNT=%ld, CUR_TENANT_COUNT=%ld",
        this, purger.purge_count_, tenant_server_pool_map_.count());
  }

  return ret;
}

int ObMySQLConnectionPool::clean_dblink_connection(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObServerConnectionPool *pool = NULL;
  ObArray<ObList<ObServerConnectionPool *, common::ObArenaAllocator>::iterator > to_delete;
  for (ServerList::iterator iter = server_list_.begin();
      OB_SUCC(ret) && iter != server_list_.end(); iter++) {
    if (OB_ISNULL(pool = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pool is null", K(ret));
    } else if (pool->get_tenant_id() != tenant_id) {
    } else if (0 == pool->get_busy_count()) {
      pool->set_server_gone(true);
      LOG_INFO("dblink mysql server pool removed", K(pool->get_server()), K(pool->get_tenant_id()), K(pool->get_dblink_id()));
      if (OB_FAIL(to_delete.push_back(iter))) {
        LOG_WARN("push iter to delete list fail", K(ret));
      }
    } else {
      LOG_ERROR("dblink pool busy count should be zero", K(pool->get_server()),
                K(pool->get_tenant_id()), K(pool->get_dblink_id()));
    }
  }
  LOG_INFO("clean dblink mysql server pool", K(to_delete.count()), K(server_list_.size()));
  if (OB_SUCC(ret)) {
    for (int i = 0; OB_SUCC(ret) && i < to_delete.count(); i++) {
      pool = *to_delete.at(i);
      server_pool_.free(pool);
      if (OB_FAIL(server_list_.erase(to_delete.at(i)))) {
        LOG_WARN("fail to delete pool from server_list", K(ret));
      }
    }
  }
  return ret;
}

bool ObMySQLConnectionPool::TenantServerConnPoolPurger::operator()(
    const TenantMapKey &tenant_key,
    const ObTenantServerConnectionPool *tenant_server_pool)
{
  bool need_purge = false;

  if (OB_ISNULL(tenant_server_pool)) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "tenant_server_pool must not be null", K(tenant_key));
  } else if (is_tenant_not_serve_(tenant_key.tenant_id_)) {
    need_purge = true;
    purge_count_++;
    LOG_INFO("[STAT][TENANT_CONN_POOL][PURGE]", K(tenant_key));
  }

  return need_purge;
}

bool ObMySQLConnectionPool::TenantServerConnPoolPurger::is_tenant_not_serve_(const uint64_t tenant_id) const
{
  int ret = OB_SUCCESS;
  const uint64_t user_tenant_count = tenant_array_.count();
  bool is_serve = false;

  if (0 >= user_tenant_count) {
    is_serve = true;
  } else {
    for(int idx = 0; OB_SUCC(ret) && !is_serve && idx < user_tenant_count; idx++) {
      uint64_t user_tenant_id = 0;

      if (OB_FAIL(tenant_array_.at(idx, user_tenant_id))) {
        LOG_WARN("get tenant_id from tenant_array failed", K(ret), K(idx), K(user_tenant_count), K_(tenant_array));
        is_serve = true; // should not purge this tenant, retry in next refresh loop.
      } else if (tenant_id == user_tenant_id) {
        is_serve = true;
      }
    }
  }

  return ! is_serve;
}


} // end namespace sqlclient
} // end namespace common
} // end namespace oceanbase
