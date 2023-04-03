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

#include "share/ob_disk_usage_table_operator.h"
#include "lib/number/ob_number_v2.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "storage/ob_disk_usage_reporter.h"
#include "lib/container/ob_iarray.h"

namespace oceanbase
{
namespace share
{
using namespace common;
using namespace storage;

ObDiskUsageTableOperator::ObDiskUsageTableOperator()
  : is_inited_(false),
    proxy_(NULL)
{

}

ObDiskUsageTableOperator::~ObDiskUsageTableOperator()
{
  is_inited_ = false;
  proxy_ = NULL;
}

int ObDiskUsageTableOperator::init(common::ObMySQLProxy &proxy)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    SHARE_LOG(WARN, "init twice", K(ret));
  } else {
    proxy_ = &proxy;
    is_inited_ = true;
  }
  return ret;
}

int ObDiskUsageTableOperator::update_tenant_space_usage(const uint64_t tenant_id,
                                const char *svr_ip,
                                const int32_t svr_port,
                                const int64_t seq_num,
                                const storage::ObDiskReportFileType file_type,
                                const uint64_t data_size,
                                const uint64_t used_size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "not init", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
      || nullptr == svr_ip || svr_port <= 0 || seq_num <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid argument", K(ret), K(tenant_id), K(svr_ip), K(svr_port),
        K(file_type), K(data_size), K(used_size), K(seq_num));
  } else {
    ObDMLSqlSplicer dml;
    ObDMLExecHelper exec(*proxy_, common::OB_SYS_TENANT_ID);
    int64_t affected_rows = 0;
    const char *file_type_str = NULL;
    switch (file_type) {
      case ObDiskReportFileType::OB_DISK_REPORT_TENANT_DATA:
        file_type_str = "tenant file data";
        break;
      case ObDiskReportFileType::OB_DISK_REPORT_TENANT_META_DATA:
        file_type_str = "tenant meta data";
        break;
      case ObDiskReportFileType::OB_DISK_REPORT_TENANT_INDEX_DATA:
        file_type_str = "tenant index data";
        break;
      case ObDiskReportFileType::OB_DISK_REPORT_TENANT_TMP_DATA:
        file_type_str = "tenant tmp data";
        break;
      case ObDiskReportFileType::OB_DISK_REPORT_TENANT_SLOG_DATA:
        file_type_str = "tenant slog data";
        break;
      case ObDiskReportFileType::OB_DISK_REPORT_TENANT_CLOG_DATA:
        file_type_str = "tenant clog data";
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        SHARE_LOG(WARN, "unexpected", K(ret), K(file_type));
        break;
    }
    if  (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(dml.add_pk_column("tenant_id", tenant_id))
        ||  OB_FAIL(dml.add_pk_column("svr_ip", svr_ip))
        ||  OB_FAIL(dml.add_pk_column("svr_port", svr_port))
        ||  OB_FAIL(dml.add_pk_column("start_seq", seq_num))
        ||  OB_FAIL(dml.add_pk_column("file_type", file_type_str))
        ||  OB_FAIL(dml.add_column("data_size", data_size))
        ||  OB_FAIL(dml.add_column("used_size", used_size))) {
      SHARE_LOG(WARN, "dml fail to add column", K(ret));
    } else if (OB_FAIL(exec.exec_insert_update(OB_ALL_SPACE_USAGE_TNAME, dml, affected_rows))) {
      SHARE_LOG(WARN, "fail to exec insert", K(ret));
    } else if (affected_rows > 2) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_LOG(WARN, "affected rows unexpected", K(ret), K(affected_rows), K(tenant_id),
                                                  K(file_type), K(data_size));
    } else {
      SHARE_LOG(INFO, "insert successful ", K(ret), K(tenant_id), K(file_type), K(data_size));
    }
  }
  return ret;
}

int ObDiskUsageTableOperator::delete_tenant_space_usage(const uint64_t tenant_id,
                              const char *svr_ip,
                              const int32_t svr_port,
                              const int64_t seq_num,
                              const storage::ObDiskReportFileType file_type)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "not init", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
      || nullptr == svr_ip || svr_port <= 0 || seq_num <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid argument", K(ret), K(tenant_id), K(svr_ip), K(svr_port),
                                        K(file_type), K(seq_num));
  } else {
    ObDMLSqlSplicer dml;
    ObDMLExecHelper exec(*proxy_, common::OB_SYS_TENANT_ID);
    int64_t affected_rows = 0;
    const char *file_type_str = NULL;
    switch (file_type) {
      case ObDiskReportFileType::OB_DISK_REPORT_TENANT_DATA:
        file_type_str = "tenant file data";
        break;
      case ObDiskReportFileType::OB_DISK_REPORT_TENANT_META_DATA:
        file_type_str = "tenant file meta data";
        break;
      case ObDiskReportFileType::OB_DISK_REPORT_TENANT_INDEX_DATA:
        file_type_str = "tenant index data";
        break;
      case ObDiskReportFileType::OB_DISK_REPORT_TENANT_TMP_DATA:
        file_type_str = "tenant tmp data";
        break;
      case ObDiskReportFileType::OB_DISK_REPORT_TENANT_SLOG_DATA:
        file_type_str = "tenant slog data";
        break;
      case ObDiskReportFileType::OB_DISK_REPORT_TENANT_CLOG_DATA:
        file_type_str = "tenant clog data";
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        SHARE_LOG(WARN, "unexpected", K(ret), K(file_type));
        break;
    }
    if  (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(dml.add_pk_column("tenant_id", tenant_id))
          ||  OB_FAIL(dml.add_pk_column("svr_ip", svr_ip))
          ||  OB_FAIL(dml.add_pk_column("svr_port", svr_port))
          ||  OB_FAIL(dml.add_pk_column("start_seq", seq_num))
          ||  OB_FAIL(dml.add_pk_column("file_type", file_type_str))) {
      SHARE_LOG(WARN, "dml fail to add column", K(ret));
    } else if (OB_FAIL(exec.exec_delete(OB_ALL_SPACE_USAGE_TNAME, dml, affected_rows))) {
      SHARE_LOG(WARN, "fail to exec delete", K(ret));
    } else if (affected_rows > 1) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_LOG(WARN, "affected rows unexpected", K(ret), K(affected_rows), K(tenant_id),
                                                  K(file_type));
    } else {
      SHARE_LOG(INFO, "delete successful ", K(ret), K(tenant_id), K(file_type));
    }
  }
  return ret;
}

int ObDiskUsageTableOperator::delete_tenant_all(const uint64_t tenant_id,
                              const char *svr_ip,
                              const int32_t svr_port,
                              const int64_t seq_num)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "not init", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
      || nullptr == svr_ip || svr_port <= 0 || seq_num <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid argument", K(ret), K(tenant_id), K(svr_ip), K(svr_port), K(seq_num));
  } else {
    ObDMLSqlSplicer dml;
    ObDMLExecHelper exec(*proxy_, common::OB_SYS_TENANT_ID);
    int64_t affected_rows = 0;
    if (OB_FAIL(dml.add_pk_column("tenant_id", tenant_id))
          ||  OB_FAIL(dml.add_pk_column("svr_ip", svr_ip))
          ||  OB_FAIL(dml.add_pk_column("svr_port", svr_port))
          ||  OB_FAIL(dml.add_pk_column("start_seq", seq_num))) {
      SHARE_LOG(WARN, "dml fail to add column", K(ret));
    } else if (OB_FAIL(exec.exec_delete(OB_ALL_SPACE_USAGE_TNAME, dml, affected_rows))) {
      SHARE_LOG(WARN, "fail to exec delete", K(ret));
    } else {
      SHARE_LOG(INFO, "delete successful ", K(ret), K(tenant_id));
    }
  }
  return ret;
}

int ObDiskUsageTableOperator::delete_tenant_all(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "not init", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid argument", K(ret), K(tenant_id));
  } else {
    ObDMLSqlSplicer dml;
    ObDMLExecHelper exec(*proxy_, common::OB_SYS_TENANT_ID);
    int64_t affected_rows = 0;
    if (OB_FAIL(dml.add_pk_column("tenant_id", tenant_id))) {
      SHARE_LOG(WARN, "dml fail to add column", K(ret));
    } else if (OB_FAIL(exec.exec_delete(OB_ALL_SPACE_USAGE_TNAME, dml, affected_rows))) {
      SHARE_LOG(WARN, "fail to exec delete", K(ret));
    } else {
      SHARE_LOG(INFO, "delete successful ", K(ret), K(tenant_id));
    }
  }
  return ret;
}

int ObDiskUsageTableOperator::get_all_tenant_ids(const char *svr_ip,
                                                 const int32_t svr_port,
                                                 const int64_t seq_num,
                                                 common::ObIArray<uint64_t> &tenant_ids)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "not init", K(ret));
  } else if (OB_ISNULL(svr_ip) || OB_UNLIKELY(svr_port <= 0 || seq_num < 0)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid argument", K(ret), KP(svr_ip), K(svr_port), K(seq_num));
  } else {
    tenant_ids.reset();
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      common::sqlclient::ObMySQLResult *result = NULL;
      const char *sql_str = "SELECT DISTINCT tenant_id "
                            "FROM %s "
                            "WHERE svr_ip = '%s' and svr_port = %ld and start_seq = %lu "
                            "ORDER BY tenant_id";
      ObSqlString sql;
      if (OB_FAIL(sql.append_fmt(sql_str, OB_ALL_SPACE_USAGE_TNAME, svr_ip, svr_port, seq_num))) {
        SHARE_LOG(WARN, "fail to append sql", K(ret), K(sql_str), K(svr_ip), K(svr_port), K(seq_num));
      } else if (OB_ISNULL(proxy_)) {
        ret = OB_ERR_UNEXPECTED;
        SHARE_LOG(WARN, "disk usage table operator is not init", K(ret), KP(proxy_));
      } else if (OB_FAIL(proxy_->read(res, sql.ptr()))) {
        SHARE_LOG(WARN, "fail to read result", K(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        SHARE_LOG(WARN, "result set from read is NULL", K(ret));
      } else {/*do nothing*/}

      while (OB_SUCC(ret) && OB_SUCC(result->next())) {
        uint64_t tenant_id = OB_INVALID_ID;
        EXTRACT_INT_FIELD_MYSQL(*result, "tenant_id", tenant_id, uint64_t);
        if (OB_FAIL(tenant_ids.push_back(tenant_id))) {
          SHARE_LOG(WARN, "fail to push back tenant id", K(ret), K(tenant_id));
        }
      }

      if (OB_LIKELY(OB_ITER_END == ret)) {
        ret = OB_SUCCESS;
      } else {
        SHARE_LOG(WARN, "fail to get all tenant ids from __all_space_usage", K(ret));
      }
    }
  }
  return ret;
}

} // namespace share
} // namespace oceanbase

