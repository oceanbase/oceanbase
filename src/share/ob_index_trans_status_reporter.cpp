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

#include "ob_index_trans_status_reporter.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/inner_table/ob_inner_table_schema.h"

using namespace oceanbase::share;
using namespace oceanbase::common;

ObIndexTransStatusReporter::ObIndexTransStatusReporter()
{}

ObIndexTransStatusReporter::~ObIndexTransStatusReporter()
{}

int ObIndexTransStatusReporter::report_wait_trans_status(const uint64_t index_table_id, const int svr_type,
    const int64_t partition_id, const ObIndexTransStatus &status, ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  char ip[common::OB_MAX_SERVER_ADDR_SIZE] = "";
  if (OB_INVALID_ID == index_table_id || INVALID_SERVER_TYPE == svr_type || !status.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(index_table_id), K(svr_type), K(partition_id), K(status));
  } else if (!status.server_.ip_to_string(ip, sizeof(ip))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to convert ip to string", K(ret), K(status.server_));
  } else {
    ObDMLSqlSplicer dml;
    if (OB_FAIL(dml.add_pk_column("tenant_id", extract_tenant_id(index_table_id))) ||
        OB_FAIL(dml.add_pk_column(K(index_table_id))) || OB_FAIL(dml.add_pk_column(K(svr_type))) ||
        OB_FAIL(dml.add_pk_column(K(partition_id))) || OB_FAIL(dml.add_column("svr_ip", ip)) ||
        OB_FAIL(dml.add_column("svr_port", status.server_.get_port())) ||
        OB_FAIL(dml.add_column("trans_status", status.trans_status_)) ||
        OB_FAIL(dml.add_column("snapshot_version", status.snapshot_version_)) ||
        OB_FAIL(dml.add_column("frozen_version", status.frozen_version_)) ||
        OB_FAIL(dml.add_column("schema_version", status.schema_version_))) {
      LOG_WARN("fail to add column", K(ret));
    } else {
      ObDMLExecHelper exec(sql_client, OB_SYS_TENANT_ID);
      int64_t affected_rows = 0;
      if (OB_FAIL(exec.exec_insert_update(OB_ALL_INDEX_WAIT_TRANSACTION_STATUS_TNAME, dml, affected_rows))) {
        LOG_WARN("fail to exeucte dml", K(ret));
      }
    }
  }
  return ret;
}

int ObIndexTransStatusReporter::delete_wait_trans_status(const uint64_t index_table_id, ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == index_table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(index_table_id));
  } else {
    ObDMLSqlSplicer dml;
    if (OB_FAIL(dml.add_pk_column("tenant_id", extract_tenant_id(index_table_id))) ||
        OB_FAIL(dml.add_pk_column(K(index_table_id)))) {
      LOG_WARN("fail to add column", K(ret), K(index_table_id));
    } else {
      ObDMLExecHelper exec(sql_client, OB_SYS_TENANT_ID);
      int64_t affected_rows = 0;
      if (OB_FAIL(exec.exec_delete(OB_ALL_INDEX_WAIT_TRANSACTION_STATUS_TNAME, dml, affected_rows))) {
        LOG_WARN("fail to exec delete", K(ret));
      }
    }
  }
  return ret;
}

int ObIndexTransStatusReporter::get_wait_trans_status(const uint64_t index_table_id, const int svr_type,
    const int64_t partition_id, ObMySQLProxy& sql_proxy, ObIndexTransStatus& status)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    sqlclient::ObMySQLResult* result = NULL;
    status.reset();
    if (OB_INVALID_ID == index_table_id || INVALID_SERVER_TYPE == svr_type) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arguments", K(ret), K(index_table_id), K(svr_type), K(partition_id));
    } else if (OB_FAIL(sql.assign_fmt(
                   "SELECT * from %s "
                   "WHERE tenant_id = %ld AND index_table_id = %ld AND svr_type = %d AND partition_id = %ld",
                   OB_ALL_INDEX_WAIT_TRANSACTION_STATUS_TNAME,
                   extract_tenant_id(index_table_id),
                   index_table_id,
                   svr_type,
                   partition_id))) {
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
      int64_t tmp_real_str_len = 0;  // It is only used to fill out the parameters and does not work. It is necessary to
                                     // ensure that there is no'\0' character in the corresponding string
      char ip[OB_MAX_SERVER_ADDR_SIZE] = "";
      int port = 0;
      EXTRACT_INT_FIELD_MYSQL(*result, "trans_status", status.trans_status_, int);
      EXTRACT_INT_FIELD_MYSQL(*result, "snapshot_version", status.snapshot_version_, int64_t);
      EXTRACT_INT_FIELD_MYSQL(*result, "frozen_version", status.frozen_version_, int64_t);
      EXTRACT_INT_FIELD_MYSQL(*result, "schema_version", status.schema_version_, int64_t);
      EXTRACT_INT_FIELD_MYSQL(*result, "svr_port", port, int);
      EXTRACT_STRBUF_FIELD_MYSQL(*result, "svr_ip", ip, OB_MAX_SERVER_ADDR_SIZE, tmp_real_str_len);
      if (!status.server_.set_ip_addr(ip, port)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, fail to set server addr", K(ret), K(ip), K(port));
      }
      (void)tmp_real_str_len;  // make compiler happy
    }
  }
  return ret;
}
