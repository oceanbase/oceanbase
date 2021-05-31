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

#define USING_LOG_PREFIX COMMON_MYSQLP
#include "ob_single_connection_proxy.h"
#include "lib/mysqlclient/ob_isql_connection.h"
#include "lib/mysqlclient/ob_isql_connection_pool.h"

using namespace oceanbase::common;
using namespace oceanbase::common::sqlclient;

ObSingleConnectionProxy::ObSingleConnectionProxy()
    : errno_(OB_SUCCESS), statement_count_(0), conn_(NULL), pool_(NULL), sql_client_(NULL), oracle_mode_(false)
{}

ObSingleConnectionProxy::~ObSingleConnectionProxy()
{
  (void)close();
}

int ObSingleConnectionProxy::connect(ObISQLClient* sql_client)
{
  int ret = OB_SUCCESS;
  if (NULL == sql_client || NULL == sql_client->get_pool()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(sql_client));
  } else if (NULL != pool_ || NULL != conn_) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("transaction can only be started once", K(pool_), K(conn_));
  } else {
    oracle_mode_ = sql_client->is_oracle_mode();
    pool_ = sql_client->get_pool();
    if (OB_FAIL(pool_->acquire(conn_, sql_client))) {
      LOG_WARN("acquire connection failed", K(ret), K(pool_));
    } else if (NULL == conn_) {
      ret = OB_INNER_STAT_ERROR;
      LOG_WARN("connection can not be NULL");
    } else if (!sql_client->is_active()) {  // check client active after connection acquired
      ret = OB_INACTIVE_SQL_CLIENT;
      LOG_WARN("inactive sql client", K(ret));
      int tmp_ret = pool_->release(conn_, OB_SUCCESS == ret);
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("release connection failed", K(tmp_ret));
      }
      conn_ = NULL;
    } else {
      sql_client_ = sql_client;
    }
    if (OB_FAIL(ret)) {
      conn_ = NULL;
      pool_ = NULL;
      sql_client_ = NULL;
    }
  }
  return ret;
}

int ObSingleConnectionProxy::read(ReadResult& res, const uint64_t tenant_id, const char* sql)
{
  int ret = OB_SUCCESS;
  res.reset();
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("check inner stat failed");
  } else if (OB_FAIL(conn_->execute_read(tenant_id, sql, res))) {
    errno_ = ret;
    const int ER_LOCK_WAIT_TIMEOUT = -1205;
    if (ER_LOCK_WAIT_TIMEOUT == ret) {
      LOG_INFO("execute query failed", K(ret), K(sql), K_(conn));
    } else {
      LOG_WARN("execute query failed", K(ret), K(sql), K_(conn));
    }
  }
  ++statement_count_;
  LOG_TRACE("execute sql", K(sql), K(ret));
  return ret;
}

int ObSingleConnectionProxy::write(const uint64_t tenant_id, const char* sql, int64_t& affected_rows)
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("check inner stat failed");
  } else if (NULL == sql_client_) {
    ret = OB_INACTIVE_SQL_CLIENT;
    LOG_WARN("sql_client_ is NULL", K(ret), K(sql));
  } else if (!sql_client_->is_active()) {
    ret = OB_INACTIVE_SQL_CLIENT;
    LOG_WARN("inactive sql client can't execute write sql", K(ret), K(sql));
  } else if (OB_FAIL(conn_->execute_write(tenant_id, sql, affected_rows))) {
    errno_ = ret;
    LOG_WARN("execute sql failed", K(ret), K(sql), K_(conn));
  }
  ++statement_count_;
  LOG_TRACE("execute sql", K(sql), K(ret));
  return ret;
}

void ObSingleConnectionProxy::close()
{
  if (NULL != pool_ && NULL != conn_) {
    pool_->release(conn_, OB_SUCCESS == errno_);
  }
  conn_ = NULL;
  pool_ = NULL;
  errno_ = OB_SUCCESS;
}

int ObSingleConnectionProxy::escape(
    const char* from, const int64_t from_size, char* to, const int64_t to_size, int64_t& out_size)
{
  int ret = OB_SUCCESS;
  if (NULL == pool_) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("transcation not started");
  } else if (OB_FAIL(pool_->escape(from, from_size, to, to_size, out_size))) {
    LOG_WARN("escape string failed",
        "from",
        ObString(from_size, from),
        K(from_size),
        "to",
        static_cast<void*>(to),
        K(to_size));
  }
  return ret;
}
