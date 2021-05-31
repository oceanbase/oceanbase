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
#include "lib/mysqlclient/ob_server_connection_pool.h"
#include "lib/mysqlclient/ob_mysql_connection_pool.h"

namespace oceanbase {
namespace common {
namespace sqlclient {
ObServerConnectionPool::ObServerConnectionPool()
    : free_conn_count_(0),
      busy_conn_count_(0),
      root_(NULL),
      dblink_id_(OB_INVALID_ID),
      server_(),
      pool_lock_(),
      last_renew_timestamp_(0),
      connection_version_(0),
      max_allowed_conn_count_(0),
      server_not_available_(false)
{}

ObServerConnectionPool::~ObServerConnectionPool()
{}

uint64_t ObServerConnectionPool::get_busy_count() const
{
  return busy_conn_count_;
}

int ObServerConnectionPool::acquire(ObMySQLConnection*& conn)
{
  int ret = OB_SUCCESS;
  ObMySQLConnection* connection = NULL;
  {
    ObSpinLockGuard lock(pool_lock_);
    if (server_not_available_) {
      ret = OB_RESOURCE_OUT;
    } else if (free_conn_count_ > 0) {
      if (OB_ISNULL(connection = connection_pool_.get_cached())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail get conn", K(free_conn_count_), K(busy_conn_count_), K(ret));
      } else {
        connection->init(this);
        ATOMIC_INC(&busy_conn_count_);
        ATOMIC_DEC(&free_conn_count_);
      }
    } else if (busy_conn_count_ < max_allowed_conn_count_) {
      if (OB_ISNULL(connection = connection_pool_.alloc())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail get conn", K(free_conn_count_), K(busy_conn_count_), K(ret));
      } else {
        connection->init(this);
        connection->set_connection_version(connection_version_);
        ATOMIC_INC(&busy_conn_count_);
      }
    } else {
      ret = OB_RESOURCE_OUT;
      LOG_WARN("fail to acquire connection from server pool",
          K(free_conn_count_),
          K(busy_conn_count_),
          K(max_allowed_conn_count_),
          K(server_),
          K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    conn = connection;
    if (conn->connection_version() != connection_version_) {
      conn->set_connection_version(connection_version_);
      conn->close();
    } else if (false == conn->is_closed()) {
      if (OB_SUCCESS != conn->ping()) {
        conn->close();
      }
    }
  }
  return ret;
}

int ObServerConnectionPool::release(ObMySQLConnection* connection, const bool succ)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(connection)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid connection", K(connection), K(ret));
  } else {
    ObSpinLockGuard lock(pool_lock_);
    if (succ) {
      connection->succ_times_++;
    } else {
      LOG_TRACE("release! err", K(succ));
      connection->error_times_++;
      connection->close();
    }
    ret = connection_pool_.put_cached(connection);
    if (OB_SUCC(ret)) {
      ATOMIC_DEC(&busy_conn_count_);
      ATOMIC_INC(&free_conn_count_);
    } else if (OB_ALLOCATE_MEMORY_FAILED == ret) {
      ATOMIC_DEC(&busy_conn_count_);
      LOG_WARN("connection object failed to put to cache. destroyed", K(ret));
    } else {
      LOG_ERROR("fail to release connection. force destructed. but error code unexpected", K(ret));
    }
  }
  return ret;
}

int ObServerConnectionPool::init(
    ObMySQLConnectionPool* root, const common::ObAddr& server, int64_t max_allowed_conn_count)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(root)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to init server connection pool. root=NULL", K(ret));
  } else {
    LOG_DEBUG("init server for connection pool", K(server));
    this->root_ = root;
    this->server_ = server;
    this->last_renew_timestamp_ = ::oceanbase::common::ObTimeUtility::current_time();
    this->server_not_available_ = false;
    this->max_allowed_conn_count_ = max_allowed_conn_count;
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

void ObServerConnectionPool::close_all_connection()
{
  ObSpinLockGuard lock(pool_lock_);
  connection_version_++;
  return;
}
void ObServerConnectionPool::dump()
{
  LOG_INFO("ms",
      K(server_),
      "free_conn_count_",
      free_conn_count_,
      "busy_conn_count_",
      busy_conn_count_,
      "max_allowed_conn_count_",
      max_allowed_conn_count_,
      "server_not_available_",
      server_not_available_);
}

int ObServerConnectionPool::init_dblink(uint64_t dblink_id, const ObAddr& server, const ObString& db_tenant,
    const ObString& db_user, const ObString& db_pass, const ObString& db_name, ObMySQLConnectionPool* root,
    int64_t max_allowed_conn_count)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init(root, server, max_allowed_conn_count))) {
    LOG_WARN("fail to init", K(ret));
  } else if (OB_INVALID_ID == dblink_id || db_tenant.empty() || db_user.empty() ||
             db_pass.empty() /*|| db_name.empty()*/
             || OB_UNLIKELY(db_tenant.length() >= OB_MAX_TENANT_NAME_LENGTH) ||
             OB_UNLIKELY(db_user.length() >= OB_MAX_USER_NAME_LENGTH) ||
             OB_UNLIKELY(db_pass.length() >= OB_MAX_PASSWORD_LENGTH) ||
             OB_UNLIKELY(db_name.length() >= OB_MAX_DATABASE_NAME_LENGTH)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("db param buffer is not enough", K(ret), K(dblink_id), K(db_tenant), K(db_user), K(db_pass), K(db_name));
  } else {
    dblink_id_ = dblink_id;
    (void)snprintf(db_user_, sizeof(db_user_), "%s@%s", db_user.ptr(), db_tenant.ptr());
    (void)snprintf(db_pass_, sizeof(db_pass_), "%s", db_pass.ptr());
    // https://baike.baidu.com/item/mysql_real_connect/4007597
    // if db is NULL, the default database is used.
    if (!db_name.empty()) {
      (void)snprintf(db_name_, sizeof(db_name_), "%s", db_name.ptr());
    }
  }
  return ret;
}

}  // end namespace sqlclient
}  // end namespace common
}  // end namespace oceanbase
