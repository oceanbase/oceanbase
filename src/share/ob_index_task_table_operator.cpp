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

#define USING_LOG_PREFIX SERVER

#include "ob_index_task_table_operator.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/inner_table/ob_inner_table_schema.h"

using namespace oceanbase::share;
using namespace oceanbase::common;

ObIndexTaskTableOperator::ObIndexTaskTableOperator()
{}

ObIndexTaskTableOperator::~ObIndexTaskTableOperator()
{}

int ObIndexTaskTableOperator::get_build_index_server(
    const uint64_t index_id, const int64_t partition_id, ObMySQLProxy& sql_proxy, ObAddr& addr)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    sqlclient::ObMySQLResult* result = NULL;
    if (OB_INVALID_ID == index_id || partition_id < 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arguments", K(ret), K(index_id), K(partition_id));
    } else if (OB_FAIL(sql.assign_fmt("SELECT svr_ip, svr_port FROM %s "
                                      "WHERE tenant_id = %ld AND index_table_id = %ld AND partition_id = %ld",
                   OB_ALL_INDEX_SCHEDULE_TASK_TNAME,
                   extract_tenant_id(index_id),
                   index_id,
                   partition_id))) {
      LOG_WARN("fail to assign sql", K(ret));
    } else if (OB_FAIL(sql_proxy.read(res, sql.ptr()))) {
      LOG_WARN("fail to execute sql", K(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, mysql result must not be NULL", K(ret));
    } else if (OB_FAIL(result->next())) {
      if (OB_LIKELY(OB_ITER_END == ret)) {
        ret = OB_ENTRY_NOT_EXIST;
      } else {
        LOG_WARN("fail to get next row", K(ret));
      }
    } else {
      int64_t tmp_real_str_len = 0;
      char ip[OB_MAX_SERVER_ADDR_SIZE] = "";
      int port = 0;
      EXTRACT_STRBUF_FIELD_MYSQL(*result, "svr_ip", ip, OB_MAX_SERVER_ADDR_SIZE, tmp_real_str_len);
      EXTRACT_INT_FIELD_MYSQL(*result, "svr_port", port, int);
      (void)tmp_real_str_len;
      if (!addr.set_ip_addr(ip, port)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to set ip addr", K(ret), K(ip), K(port));
      }
    }
  }
  return ret;
}

int ObIndexTaskTableOperator::get_build_index_version(const uint64_t index_table_id, const int64_t partition_id,
    const ObAddr& addr, ObMySQLProxy& sql_proxy, int64_t& snapshot_version, int64_t& frozen_version)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    sqlclient::ObMySQLResult* result = NULL;
    char ip[common::OB_MAX_SERVER_ADDR_SIZE] = "";
    snapshot_version = -1;
    frozen_version = -1;
    if (OB_INVALID_ID == index_table_id || partition_id < 0 || !addr.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arguments", K(ret), K(index_table_id), K(partition_id), K(addr));
    } else if (!addr.ip_to_string(ip, sizeof(ip))) {
      LOG_WARN("fail to convert ObAddr to ip", K(ret));
    } else if (OB_FAIL(sql.assign_fmt("SELECT snapshot_version, frozen_version from %s "
                                      "WHERE tenant_id = %ld AND index_table_id = %ld AND partition_id = %ld AND "
                                      "svr_ip = '%s' AND svr_port = %d",
                   OB_ALL_INDEX_SCHEDULE_TASK_TNAME,
                   extract_tenant_id(index_table_id),
                   index_table_id,
                   partition_id,
                   ip,
                   addr.get_port()))) {
      STORAGE_LOG(WARN, "fail to assign sql", K(ret));
    } else if (OB_FAIL(sql_proxy.read(res, sql.ptr()))) {
      LOG_WARN("fail to execute sql", K(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "error unexpected, query result must not be NULL", K(ret));
    } else if (OB_FAIL(result->next())) {
      if (OB_LIKELY(OB_ITER_END == ret)) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to get next row", K(ret));
      }
    } else {
      EXTRACT_INT_FIELD_MYSQL(*result, "snapshot_version", snapshot_version, int64_t);
      EXTRACT_INT_FIELD_MYSQL(*result, "frozen_version", frozen_version, int64_t);
    }
  }
  return ret;
}

int ObIndexTaskTableOperator::generate_new_build_index_record(const uint64_t index_table_id, const int64_t partition_id,
    const ObAddr& addr, const int64_t snapshot_version, const int64_t frozen_version, ObMySQLProxy& sql_proxy)
{
  int ret = OB_SUCCESS;
  char ip[common::OB_MAX_SERVER_ADDR_SIZE] = "";
  if (OB_INVALID_ID == index_table_id || partition_id < 0 || !addr.is_valid() || snapshot_version < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments",
        K(ret),
        K(index_table_id),
        K(partition_id),
        K(addr),
        K(snapshot_version),
        K(frozen_version));
  } else if (!addr.ip_to_string(ip, sizeof(ip))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to convert ip to string", K(ret), K(addr));
  } else {
    ObDMLSqlSplicer dml;
    if (OB_FAIL(dml.add_pk_column("tenant_id", extract_tenant_id(index_table_id))) ||
        OB_FAIL(dml.add_pk_column(K(index_table_id))) || OB_FAIL(dml.add_pk_column(K(partition_id))) ||
        OB_FAIL(dml.add_column("svr_ip", ip)) || OB_FAIL(dml.add_column("svr_port", addr.get_port())) ||
        OB_FAIL(dml.add_column(K(snapshot_version))) || OB_FAIL(dml.add_column(K(frozen_version)))) {
      LOG_WARN("fail to add column", K(ret));
    } else {
      ObDMLExecHelper exec(sql_proxy, OB_SYS_TENANT_ID);
      int64_t affected_rows = 0;
      if (OB_FAIL(exec.exec_replace(OB_ALL_INDEX_SCHEDULE_TASK_TNAME, dml, affected_rows))) {
        LOG_WARN("fail to exeucte dml", K(ret));
      }
    }
  }
  return ret;
}

int ObIndexTaskTableOperator::update_frozen_version(const uint64_t index_table_id, const int64_t partition_id,
    const ObAddr& addr, const int64_t frozen_version, common::ObMySQLProxy& sql_proxy)
{
  int ret = OB_SUCCESS;
  char ip[common::OB_MAX_SERVER_ADDR_SIZE];
  if (OB_UNLIKELY(OB_INVALID_ID == index_table_id || partition_id < 0 || !addr.is_valid() || frozen_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(index_table_id), K(partition_id), K(addr), K(frozen_version));
  } else if (!addr.ip_to_string(ip, sizeof(ip))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to convert ip to string", K(ret), K(addr));
  } else {
    ObDMLSqlSplicer dml;
    if (OB_FAIL(dml.add_pk_column("tenant_id", extract_tenant_id(index_table_id))) ||
        OB_FAIL(dml.add_pk_column(K(index_table_id))) || OB_FAIL(dml.add_pk_column(K(partition_id))) ||
        OB_FAIL(dml.add_column("svr_ip", ip)) || OB_FAIL(dml.add_column("svr_port", addr.get_port())) ||
        OB_FAIL(dml.add_column(K(frozen_version)))) {
      LOG_WARN("fail to add column", K(ret));
    } else {
      ObDMLExecHelper exec(sql_proxy, OB_SYS_TENANT_ID);
      int64_t affected_rows = 0;
      if (OB_FAIL(exec.exec_update(OB_ALL_INDEX_SCHEDULE_TASK_TNAME, dml, affected_rows))) {
        LOG_WARN("fail to exeucte dml", K(ret));
      } else if (affected_rows < 1) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "error unexpected, invalid affected row count", K(ret), K(index_table_id), K(affected_rows));
      }
    }
  }
  return ret;
}
