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

#include "ob_index_status_table_operator.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/inner_table/ob_inner_table_schema.h"

using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::common;

bool ObIndexStatusTableOperator::ObBuildIndexStatus::operator==(const ObBuildIndexStatus& other) const
{
  return index_status_ == other.index_status_ && ret_code_ == other.ret_code_ && role_ == other.role_;
}

bool ObIndexStatusTableOperator::ObBuildIndexStatus::operator!=(const ObBuildIndexStatus& other) const
{
  return !(operator==(other));
}

bool ObIndexStatusTableOperator::ObBuildIndexStatus::is_valid() const
{
  return index_status_ > ObIndexStatus::INDEX_STATUS_NOT_FOUND && index_status_ < ObIndexStatus::INDEX_STATUS_MAX;
}

ObIndexStatusTableOperator::ObIndexStatusTableOperator()
{}

ObIndexStatusTableOperator::~ObIndexStatusTableOperator()
{}

int ObIndexStatusTableOperator::get_build_index_status(const uint64_t index_table_id, const int64_t partition_id,
    const ObAddr& addr, ObMySQLProxy& sql_proxy, ObBuildIndexStatus& status)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    sqlclient::ObMySQLResult* result = NULL;
    char ip[common::OB_MAX_SERVER_ADDR_SIZE] = "";
    if (OB_INVALID_ID == index_table_id || partition_id < 0 || !addr.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arguments", K(ret), K(index_table_id), K(partition_id), K(addr));
    } else if (!addr.ip_to_string(ip, sizeof(ip))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("fail to convert ip to string", K(ret), K(addr));
    } else if (OB_FAIL(sql.assign_fmt("SELECT index_status, ret_code, role from %s "
                                      "WHERE tenant_id = %ld AND index_table_id = %ld AND partition_id = %ld AND "
                                      "svr_ip = '%s' AND svr_port = %d",
                   OB_ALL_LOCAL_INDEX_STATUS_TNAME,
                   extract_tenant_id(index_table_id),
                   index_table_id,
                   partition_id,
                   ip,
                   addr.get_port()))) {
      LOG_WARN("fail to assign sql", K(ret));
    } else if (OB_FAIL(sql_proxy.read(res, sql.ptr()))) {
      LOG_WARN("fail to execute sql", K(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, query result must not be NULL", K(ret));
    } else if (OB_FAIL(result->next())) {
      if (OB_LIKELY(OB_ITER_END == ret)) {
        ret = OB_ENTRY_NOT_EXIST;
      } else {
        LOG_WARN("fail to get next row", K(ret));
      }
    } else {
      int index_status = 0;
      EXTRACT_INT_FIELD_MYSQL(*result, "index_status", index_status, int);
      status.index_status_ = static_cast<ObIndexStatus>(index_status);
      EXTRACT_INT_FIELD_MYSQL(*result, "ret_code", status.ret_code_, int);
      EXTRACT_INT_FIELD_MYSQL(*result, "role", status.role_, common::ObRole);
    }
  }
  return ret;
}

int ObIndexStatusTableOperator::check_final_status_reported(
    const uint64_t index_table_id, const int64_t partition_id, ObMySQLProxy& sql_proxy, bool& is_reported)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    sqlclient::ObMySQLResult* result = NULL;
    if (OB_INVALID_ID == index_table_id || partition_id < 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arguments", K(ret), K(index_table_id), K(partition_id));
    } else if (OB_FAIL(
                   sql.assign_fmt("SELECT 1 from %s "
                                  "WHERE tenant_id = %ld AND index_table_id = %ld AND partition_id = %ld AND role = %d",
                       OB_ALL_LOCAL_INDEX_STATUS_TNAME,
                       extract_tenant_id(index_table_id),
                       index_table_id,
                       partition_id,
                       common::LEADER))) {
      LOG_WARN("fail to assign sql", K(ret));
    } else if (OB_FAIL(sql_proxy.read(res, sql.ptr()))) {
      LOG_WARN("fail to execute sql", K(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, query result must not be NULL", K(ret));
    } else if (OB_FAIL(result->next())) {
      if (OB_LIKELY(OB_ITER_END == ret)) {
        ret = OB_SUCCESS;
        is_reported = false;
      } else {
        LOG_WARN("fail to get next row", K(ret));
      }
    } else {
      is_reported = true;
    }
  }
  return ret;
}

int ObIndexStatusTableOperator::report_build_index_status(const uint64_t index_table_id, const int64_t partition_id,
    const ObAddr& addr, const ObBuildIndexStatus& status, ObMySQLProxy& sql_proxy)
{
  int ret = OB_SUCCESS;
  ObBuildIndexStatus report_status;
  bool need_report = false;
  if (OB_INVALID_ID == index_table_id || partition_id < 0 || !addr.is_valid() || !status.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(index_table_id), K(partition_id), K(addr), K(status));
  } else if (OB_FAIL(get_build_index_status(index_table_id, partition_id, addr, sql_proxy, report_status))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      need_report = true;
    } else {
      LOG_WARN("fail to get build index status", K(ret), K(index_table_id), K(partition_id));
    }
  } else {
    need_report = (report_status != status && !common::is_strong_leader(report_status.role_));
  }

  if (!need_report) {
    STORAGE_LOG(INFO,
        "process index status has already been reported before",
        K(report_status),
        K(status),
        K(index_table_id),
        K(partition_id));
  }

  if (OB_SUCC(ret) && need_report) {
    char ip[OB_MAX_SERVER_ADDR_SIZE] = "";
    if (!addr.ip_to_string(ip, sizeof(ip))) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "convert ip to string failed", K(ret), K(addr));
    } else {
      ObDMLSqlSplicer dml;
      if (OB_FAIL(dml.add_pk_column("tenant_id", extract_tenant_id(index_table_id))) ||
          OB_FAIL(dml.add_pk_column(K(index_table_id))) || OB_FAIL(dml.add_pk_column(K(partition_id))) ||
          OB_FAIL(dml.add_pk_column("svr_ip", ip)) || OB_FAIL(dml.add_pk_column("svr_port", addr.get_port())) ||
          OB_FAIL(dml.add_column("index_status", status.index_status_)) ||
          OB_FAIL(dml.add_column("ret_code", status.ret_code_)) || OB_FAIL(dml.add_column("role", status.role_)) ||
          OB_FAIL(dml.add_gmt_modified())) {
        STORAGE_LOG(WARN, "add column failed", K(ret));
      } else {
        ObDMLExecHelper exec(sql_proxy, OB_SYS_TENANT_ID);
        int64_t affected_rows = 0;
        if (OB_FAIL(exec.exec_insert_update(OB_ALL_LOCAL_INDEX_STATUS_TNAME, dml, affected_rows))) {
          STORAGE_LOG(WARN, "execute dml failed", K(ret));
        } else if (affected_rows > 2) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "unexpected affected rows", K(ret), K(affected_rows));
        }
        STORAGE_LOG(
            INFO, "process index status report", K(ret), K(partition_id), K(index_table_id), K(addr), K(status));
      }
    }
  }
  return ret;
}
