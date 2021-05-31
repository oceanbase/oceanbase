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
#include "ob_sql_client_decorator.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/ob_schema_status_proxy.h"
#include "observer/ob_server_struct.h"
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

int ObSQLClientRetry::escape(
    const char* from, const int64_t from_size, char* to, const int64_t to_size, int64_t& out_size)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sql_client_)) {
    ret = OB_INNER_STAT_ERROR;
  } else {
    ret = sql_client_->escape(from, from_size, to, to_size, out_size);
  }
  return ret;
}

int ObSQLClientRetry::read(ReadResult& res, const uint64_t tenant_id, const char* sql)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sql_client_)) {
    ret = OB_INNER_STAT_ERROR;
  } else {
    ret = sql_client_->read(res, tenant_id, sql);
    if (OB_FAIL(ret)) {
      for (int32_t retry = 0; retry < retry_limit_ && OB_SUCCESS != ret; retry++) {
        LOG_WARN("retry execute query when failed", K(ret), K(retry), K_(retry_limit), K(sql));
        ret = sql_client_->read(res, tenant_id, sql);
      }
    }
  }
  return ret;
}

int ObSQLClientRetry::write(const uint64_t tenant_id, const char* sql, int64_t& affected_rows)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sql_client_)) {
    ret = OB_INNER_STAT_ERROR;
  } else {
    ret = sql_client_->write(tenant_id, sql, affected_rows);
  }
  return ret;
}

sqlclient::ObISQLConnectionPool* ObSQLClientRetry::get_pool()
{
  sqlclient::ObISQLConnectionPool* pool = NULL;
  if (NULL != sql_client_) {
    pool = sql_client_->get_pool();
  }
  return pool;
}

////////////////////////////////////////////////////////////////
int ObSQLClientRetryWeak::escape(
    const char* from, const int64_t from_size, char* to, const int64_t to_size, int64_t& out_size)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sql_client_)) {
    ret = OB_INNER_STAT_ERROR;
  } else {
    ret = sql_client_->escape(from, from_size, to, to_size, out_size);
  }
  return ret;
}

// In the case of weakly consistent reads, the READ-COMMITTED isolation level is always used
//       Under the SERIALIZABLE isolation level, weakly consistent reading is not supported, nor is it supported to read
//       the specified snapshot version
int ObSQLClientRetryWeak::weak_read(ReadResult& res, const uint64_t tenant_id, const char* sql)
{
  int ret = OB_SUCCESS;
  ObSingleConnectionProxy single_conn_proxy;
  sqlclient::ObISQLConnection* conn = NULL;
  int64_t old_read_consistency = 0;
  ObString ob_read_consistency = ObString::make_string("ob_read_consistency");
  ObString read_snapshot_version_name = ObString::make_string("ob_read_snapshot_version");
  ObString tx_isolation = ObString::make_string("tx_isolation");
  int64_t old_tx_isolation = 0;
  int64_t read_committed = 1;  // ObTransIsolation::READ_COMMITTED
  if (OB_FAIL(single_conn_proxy.connect(sql_client_))) {
    LOG_WARN("failed to get mysql connect", K(ret));
  } else if (OB_ISNULL(conn = single_conn_proxy.get_connection())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null pointer", K(ret));
  } else if (OB_FAIL(conn->get_session_variable(ob_read_consistency, old_read_consistency))) {
    LOG_WARN("failed to get session variable ob_read_consistency", K(ret));
  } else if (OB_FAIL(conn->set_session_variable(ob_read_consistency, static_cast<int64_t>(WEAK)))) {
    LOG_WARN("failed to set session variable ob_read_consistency", K(ret));
  } else if (OB_FAIL(conn->get_session_variable(tx_isolation, old_tx_isolation))) {
    LOG_WARN("failed to get session variable tx_isolation", K(ret));
  }
  // Mandatory set to READ-COMMITTED isolation level
  else if (OB_FAIL(conn->set_session_variable(tx_isolation, read_committed))) {
    LOG_WARN("failed to set session variable ob_read_consistency", K(ret));
  } else if (snapshot_timestamp_ > 0 &&
             OB_FAIL(conn->set_session_variable(read_snapshot_version_name, snapshot_timestamp_))) {
    LOG_WARN("failed to set session variable ob_read_snapshot_version", K(ret), K_(snapshot_timestamp));
  } else {

    if (check_sys_variable_) {
      ret = single_conn_proxy.read(res, tenant_id, sql);
    } else {
      ret = read_without_check_sys_variable(single_conn_proxy, res, tenant_id, sql);
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to weak read",
          K(ret),
          K(sql),
          K(tenant_id),
          K_(did_use_weak),
          K_(did_use_retry),
          K_(snapshot_timestamp),
          K_(check_sys_variable),
          K_(snapshot_timestamp));
    } else {
      LOG_INFO("YZFDEBUG weak read succeeded!",
          K(ret),
          K(sql),
          K(tenant_id),
          K_(did_use_weak),
          K_(did_use_retry),
          K_(snapshot_timestamp),
          K_(check_sys_variable),
          K_(snapshot_timestamp),
          "old_read_consistency",
          old_read_consistency,
          K(old_tx_isolation));
    }

    // restore the old value
    int tmp_ret = conn->set_session_variable(ob_read_consistency, old_read_consistency);
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("failed to set session variable ob_read_consistency", K(ret), K(old_read_consistency));
    }
    ret = (OB_SUCCESS == ret) ? tmp_ret : ret;

    tmp_ret = conn->set_session_variable(tx_isolation, old_tx_isolation);
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("failed to set session variable tx_isolation", K(ret), K(old_tx_isolation));
    }
    ret = (OB_SUCCESS == ret) ? tmp_ret : ret;

    // reset ob_read_snapshot_version
    tmp_ret = conn->set_session_variable(read_snapshot_version_name, OB_INVALID_VERSION);
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("failed to set session variable ob_read_snapshot_version", K(ret));
    }
    ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
  }
  return ret;
}

int ObSQLClientRetryWeak::read_without_retry(ReadResult& res, const uint64_t tenant_id, const char* sql)
{
  int ret = OB_SUCCESS;
  ObSingleConnectionProxy single_conn_proxy;
  sqlclient::ObISQLConnection* conn = NULL;
  if (OB_FAIL(single_conn_proxy.connect(sql_client_))) {
    LOG_WARN("failed to get mysql connect", K(ret));
  } else if (OB_ISNULL(conn = single_conn_proxy.get_connection())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null pointer", K(ret));
  } else {
    bool old_value = conn->get_no_retry_on_rpc_error();
    conn->set_no_retry_on_rpc_error(true);
    if (check_sys_variable_) {
      ret = single_conn_proxy.read(res, tenant_id, sql);
    } else {
      ret = read_without_check_sys_variable(single_conn_proxy, res, tenant_id, sql);
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to read without retry", K(ret), K(sql), K(tenant_id));
    }
    // restore the old value
    conn->set_no_retry_on_rpc_error(old_value);
  }
  return ret;
}

int ObSQLClientRetryWeak::read_without_check_sys_variable(
    ObSingleConnectionProxy& single_conn_proxy, ReadResult& res, const uint64_t tenant_id, const char* sql)
{
  int ret = OB_SUCCESS;
  ObString check_sys_variable_name = ObString::make_string("ob_check_sys_variable");
  sqlclient::ObISQLConnection* conn = NULL;
  if (OB_ISNULL(conn = single_conn_proxy.get_connection())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null pointer", K(ret));
  } else if (OB_FAIL(conn->set_session_variable(check_sys_variable_name, static_cast<int64_t>(check_sys_variable_)))) {
    LOG_WARN("failed to set session variable ob_check_sys_variable", K(ret));
  } else {

    ret = single_conn_proxy.read(res, tenant_id, sql);
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to read without check sys variable",
          K(ret),
          K(sql),
          K(tenant_id),
          K_(did_use_weak),
          K_(did_use_retry),
          K_(check_sys_variable),
          K_(snapshot_timestamp));
    } else {
      LOG_INFO("read without check sys variable succeeded!",
          K(ret),
          K(sql),
          K(tenant_id),
          K_(did_use_weak),
          K_(did_use_retry),
          K_(check_sys_variable),
          K_(snapshot_timestamp));
    }

    int check_sys_variable = 1;
    int tmp_ret = conn->set_session_variable(check_sys_variable_name, check_sys_variable);
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("failed to set session variable ob_check_sys_variable", K(ret));
    }
    ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
  }
  return ret;
}

int ObSQLClientRetryWeak::read(ReadResult& res, const uint64_t tenant_id, const char* sql)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sql_client_)) {
    ret = OB_INNER_STAT_ERROR;
  } else if (OB_FAIL(update_weak_read_snapshot_timestamp(tenant_id))) {
    LOG_WARN("fail to update weak read snapshot timestamp", K(ret), K(tenant_id));
  } else {
    if (did_use_weak_) {
      if (did_use_retry_) {
        if (OB_FAIL(read_without_retry(res, tenant_id, sql))) {
          ret = weak_read(res, tenant_id, sql);
        }
      } else {
        ret = weak_read(res, tenant_id, sql);
      }
    } else {
      // normal read
      if (check_sys_variable_) {
        ret = sql_client_->read(res, tenant_id, sql);
      } else {
        ObSingleConnectionProxy single_conn_proxy;
        if (OB_FAIL(single_conn_proxy.connect(sql_client_))) {
          LOG_WARN("failed to get mysql connect", K(ret));
        } else {
          ret = read_without_check_sys_variable(single_conn_proxy, res, tenant_id, sql);
        }
      }
    }
  }
  return ret;
}

int ObSQLClientRetryWeak::write(const uint64_t tenant_id, const char* sql, int64_t& affected_rows)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sql_client_)) {
    ret = OB_INNER_STAT_ERROR;
  } else {
    ret = sql_client_->write(tenant_id, sql, affected_rows);
  }
  return ret;
}

int ObSQLClientRetryWeak::update_weak_read_snapshot_timestamp(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  bool need_weak_read = false;
  if (!is_auto_mode()) {
    // skip
  } else if (OB_INVALID_TENANT_ID == tenant_id_ || OB_INVALID_ID == table_id_ || tenant_id != tenant_id_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id_), K(table_id_), K(tenant_id));
  } else if (GCTX.is_standby_cluster() && OB_SYS_TENANT_ID != tenant_id_) {
    ObRefreshSchemaStatus schema_status;
    if (OB_FAIL(ObSysTableChecker::is_tenant_table_need_weak_read(table_id_, need_weak_read))) {
      LOG_WARN("fail to check if table need weak read", K(ret), K(tenant_id_), K(table_id_));
    } else if (!need_weak_read) {
      did_use_weak_ = false;
      snapshot_timestamp_ = OB_INVALID_TIMESTAMP;
    } else if (OB_ISNULL(GCTX.schema_status_proxy_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema_status_proxy is null", K(ret));
    } else if (OB_FAIL(GCTX.schema_status_proxy_->get_refresh_schema_status(tenant_id_, schema_status))) {
      LOG_WARN("fail to get schema status", K(ret), K(tenant_id_));
    } else if (0 == schema_status.snapshot_timestamp_) {
      // The weakly read version number is 0, the transaction will negotiate a readable version number, which does not
      // match expectations, here is a defense
      ret = OB_REPLICA_NOT_READABLE;
      LOG_WARN("snapshot_timestamp is invalid", K(ret), K(schema_status));
    } else if (OB_INVALID_TIMESTAMP == schema_status.snapshot_timestamp_ &&
               OB_INVALID_VERSION == schema_status.readable_schema_version_) {
      did_use_weak_ = false;
      snapshot_timestamp_ = OB_INVALID_TIMESTAMP;
    } else {
      did_use_weak_ = true;
      snapshot_timestamp_ = schema_status.snapshot_timestamp_;
    }
  } else {
    did_use_weak_ = false;
    snapshot_timestamp_ = OB_INVALID_TIMESTAMP;
  }
  return ret;
}

sqlclient::ObISQLConnectionPool* ObSQLClientRetryWeak::get_pool()
{
  sqlclient::ObISQLConnectionPool* pool = NULL;
  if (NULL != sql_client_) {
    pool = sql_client_->get_pool();
  }
  return pool;
}
