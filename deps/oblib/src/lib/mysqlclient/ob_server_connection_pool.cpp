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
#include "lib/mysqlclient/ob_server_connection_pool.h"
#include "lib/mysqlclient/ob_mysql_connection_pool.h"

namespace oceanbase
{
namespace common
{
namespace sqlclient
{
ObServerConnectionPool::ObServerConnectionPool() :
    ObCommonServerConnectionPool(),
    connection_pool_ptr_(NULL),
    root_(NULL),
    dblink_id_(OB_INVALID_ID),
    server_(),
    pool_lock_(common::ObLatchIds::INNER_CONN_POOL_LOCK),
    last_renew_timestamp_(0),
    connection_version_(0),
    max_allowed_conn_count_(0),
    server_not_available_(false)
{
  db_name_[0] = 0;
  db_user_[0] = 0;
  db_pass_[0] = 0;
}


ObServerConnectionPool::~ObServerConnectionPool()
{
}

uint64_t ObServerConnectionPool::get_busy_count() const
{
  return busy_conn_count_;
}

int ObServerConnectionPool::acquire(ObMySQLConnection *&conn, uint32_t sessid)
{
  int ret = OB_SUCCESS;
  ObMySQLConnection *connection = NULL;
  {
    ObSpinLockGuard lock(pool_lock_);
    if (server_not_available_) {
      ret = OB_RESOURCE_OUT;
    } else if (free_conn_count_ > 0) {
      if (OB_FAIL(connection_pool_ptr_->get_cached(connection, sessid))) {
        ATOMIC_DEC(&free_conn_count_);
        LOG_WARN("fail get conn", K(free_conn_count_), K(busy_conn_count_), K(ret));
      } else {
        connection->init(this);
        ATOMIC_INC(&busy_conn_count_);
        ATOMIC_DEC(&free_conn_count_);
      }
    } else if (busy_conn_count_ < max_allowed_conn_count_) {
      ret = connection_pool_ptr_->alloc(connection, sessid);
      if (OB_ERR_ALREADY_EXISTS == ret) {

        connection->init(this);
        ATOMIC_INC(&busy_conn_count_);
        ATOMIC_DEC(&free_conn_count_);
        ret = OB_SUCCESS;
      } else if (OB_SUCC(ret)) {
        connection->init(this);
        connection->set_connection_version(connection_version_);
        ATOMIC_INC(&busy_conn_count_);
      } else {
        LOG_ERROR("fail get conn", K(free_conn_count_), K(busy_conn_count_), K(ret));
      }
    } else {
      ret = OB_RESOURCE_OUT;
      LOG_WARN("fail to acquire connection from server pool",
               K(free_conn_count_), K(busy_conn_count_), K(max_allowed_conn_count_), K(server_), K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    conn = connection;
    if (conn->connection_version() != connection_version_) {
      conn->set_connection_version(connection_version_);
      conn->close();
    } else if (sessid != conn->get_sessid()) {
      LOG_TRACE("get connection from other session, close it", K(ret), K(sessid), K(conn->get_sessid()));
      conn->close();
    } else if (false == conn->is_closed()) {
      if (OB_SUCCESS != conn->ping()) {
        conn->close();
      }
    }
    conn->set_sessid(sessid);
  }
  LOG_TRACE("acquire connection from server conn pool", KP(this), K(busy_conn_count_), K(free_conn_count_), KP(connection), K(ret), K(sessid), K(lbt()));
  return ret;
}

int ObServerConnectionPool::release(common::sqlclient::ObISQLConnection *conn, const bool succ)
{
  int ret = OB_SUCCESS;
  ObMySQLConnection *connection = static_cast<ObMySQLConnection *>(conn);
  if (OB_ISNULL(connection)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid connection", K(connection), K(ret));
  } else {
    connection->set_busy(false);
    if (succ) {
      connection->succ_times_++;
      if (!get_dblink_reuse_connection_cfg()) {
        connection->close();
        LOG_TRACE("close dblink connection when release it", K(ret), K(succ), KP(conn));
      }
    } else {
      LOG_TRACE("release oci connection, close it caused by err", K(succ));
      connection->error_times_++;
      connection->close();
    }
    {
      // Do not perform any operations on the network after thread holding a lock
      ObSpinLockGuard lock(pool_lock_);
      if (OB_FAIL(connection_pool_ptr_->put_cached(connection, connection->get_sessid()))) {
        ATOMIC_DEC(&busy_conn_count_);
        LOG_WARN("connection object failed to put to cache. destroyed", K(ret));
      } else {
        ATOMIC_DEC(&busy_conn_count_);
        ATOMIC_INC(&free_conn_count_);
      }
    }
  }
  LOG_TRACE("release connection to server conn pool", KP(this),
                                                      K(busy_conn_count_),
                                                      K(free_conn_count_),
                                                      KP(connection),
                                                      K(connection->get_sessid()),
                                                      K(succ),
                                                      K(ret),
                                                      K(lbt()));
  return ret;
}


int ObServerConnectionPool::init(ObMySQLConnectionPool *root,
                                 const common::ObAddr &server,
                                 int64_t max_allowed_conn_count)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(root)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to init server connection pool. root=NULL", K(ret));
  } else if (OB_FAIL(dblink_connection_pool_.init())) {
    LOG_WARN("fail to init dblink_connection_pool_", K(ret));
  } else {
    LOG_DEBUG("init server for connection pool", K(server));
    this->root_ = root;
    this->server_ = server;
    this->last_renew_timestamp_ = ::oceanbase::common::ObTimeUtility::current_time();
    this->server_not_available_ = false;
    this->max_allowed_conn_count_ = max_allowed_conn_count;
    connection_pool_ptr_ = &connection_pool_;
  }
  return ret;
}

void ObServerConnectionPool::reset()
{
  root_ = NULL;
  dblink_id_ = OB_INVALID_ID;
  last_renew_timestamp_ = 0;
  server_not_available_ = true;
  // TODO:
  // close all ObMySQLConnections in connection_pool_
  close_all_connection();
}

void ObServerConnectionPool::reset_idle_conn_to_sys_tenant()
{
  ObSpinLockGuard lock(pool_lock_);
  auto fn = [](ObMySQLConnection &conn){ if (!conn.is_closed() && !conn.is_busy()) conn.switch_tenant(OB_SYS_TENANT_ID); };
  connection_pool_.for_each(fn);
}

void ObServerConnectionPool::close_all_connection()
{
  ObSpinLockGuard lock(pool_lock_);
  connection_version_++;
  return;
}
void ObServerConnectionPool::dump()
{
  LOG_INFO("ms", K(server_), "free_conn_count_", free_conn_count_, "busy_conn_count_", busy_conn_count_,
           "max_allowed_conn_count_", max_allowed_conn_count_, "server_not_available_", server_not_available_);
}

int ObServerConnectionPool::init_dblink(uint64_t tenant_id, uint64_t dblink_id, const ObAddr &server,
                                        const ObString &db_tenant, const ObString &db_user,
                                        const ObString &db_pass, const ObString &db_name,
                                        const common::ObString &conn_str,
                                        const common::ObString &cluster_str,
                                        ObMySQLConnectionPool *root, int64_t max_allowed_conn_count)
{
  UNUSED(conn_str);
  int ret = OB_SUCCESS;
  if (OB_FAIL(init(root, server, max_allowed_conn_count))) {
    LOG_WARN("fail to init", K(ret));
  } else if (OB_INVALID_ID == dblink_id
             || db_tenant.empty() || db_user.empty() || db_pass.empty()
             || (!lib::is_oracle_mode() && db_name.empty())
             || OB_UNLIKELY(cluster_str.length() >= OB_MAX_CLUSTER_NAME_LENGTH)
             || OB_UNLIKELY(db_tenant.length() >= OB_MAX_TENANT_NAME_LENGTH)
             || OB_UNLIKELY(db_user.length() >= OB_MAX_USER_NAME_LENGTH)
             || OB_UNLIKELY(db_pass.length() >= OB_MAX_PASSWORD_LENGTH)
             || OB_UNLIKELY(db_name.length() >= OB_MAX_DATABASE_NAME_LENGTH)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("db param buffer is not enough", K(ret),
             K(dblink_id), K(db_tenant), K(db_user), K(db_pass), K(db_name));
  } else {
    dblink_id_ = dblink_id;
    tenant_id_  = tenant_id;
    if (cluster_str.empty()) {
      (void)snprintf(db_user_, sizeof(db_user_), "%.*s@%.*s", db_user.length(), db_user.ptr(),
                    db_tenant.length(), db_tenant.ptr());
    } else {
      (void)snprintf(db_user_, sizeof(db_user_), "%.*s@%.*s#%.*s", db_user.length(), db_user.ptr(),
                    db_tenant.length(), db_tenant.ptr(),
                    cluster_str.length(), cluster_str.ptr());
    }
    (void)snprintf(db_pass_, sizeof(db_pass_), "%.*s", db_pass.length(), db_pass.ptr());
    // if db is NULL, the default database is used.
    if (!db_name.empty()) {
      (void)snprintf(db_name_, sizeof(db_name_), "%.*s", db_name.length(), db_name.ptr());
    }
    connection_pool_ptr_ = &dblink_connection_pool_;
  }
  return ret;
}

int ObServerConnectionPool::free_dblink_session(uint32_t sessid)
{
  int ret = OB_SUCCESS;
  int64_t fail_recycled_conn_count = 0;
  int64_t succ_recycled_conn_count = 0;
  if (OB_FAIL(dblink_connection_pool_.free_session_conn_array(sessid, fail_recycled_conn_count, succ_recycled_conn_count))) {
    LOG_WARN("drop dblink session failed, some connection of this seesion will be freed",
              K(fail_recycled_conn_count), K(succ_recycled_conn_count), K(sessid), K(ret));
  }
  LOG_TRACE("free_dblink_session",  KP(this), K(fail_recycled_conn_count), K(succ_recycled_conn_count), K(busy_conn_count_), K(free_conn_count_), K(sessid), K(ret));
  return ret;
};

} // end namespace sqlclient
} // end namespace common
} // end namespace oceanbase
