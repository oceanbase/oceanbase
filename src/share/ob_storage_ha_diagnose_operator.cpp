/**
 * Copyright (c) 2022 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SHARE

#include "lib/mysqlclient/ob_mysql_proxy.h" // ObISqlClient, SMART_VAR
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/ob_storage_ha_diagnose_operator.h"
#include "share/ob_dml_sql_splicer.h" // ObDMLSqlSplicer
#include "share/config/ob_server_config.h"

namespace oceanbase
{

namespace share
{

ObStorageHADiagOperator::ObStorageHADiagOperator()
  : is_inited_(false),
    last_event_ts_(0) {}

int ObStorageHADiagOperator::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObStorageHADiagOperator is already init", K(ret));
  } else {
    last_event_ts_ = 0;
    is_inited_ = true;
  }
  return ret;
}

void ObStorageHADiagOperator::reset()
{
  is_inited_ = false;
  last_event_ts_ = 0;
}

int ObStorageHADiagOperator::get_batch_row_keys(
        common::ObISQLClient &sql_proxy,
        const uint64_t tenant_id,
        const ObStorageHADiagModule &module,
        const int64_t last_end_timestamp,
        ObIArray<int64_t> &timestamp_array) const
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const char* table_name = nullptr;
  timestamp_array.reset();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || ObStorageHADiagModule::MAX_MODULE == module)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(module));
  } else if (OB_FAIL(get_table_name_(module, table_name))) {
    LOG_WARN("fail to get table name", KR(ret), K(module));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      sqlclient::ObMySQLResult *result = nullptr;
      if (OB_FAIL(sql.assign_fmt(
          "select time_to_usec(gmt_create) as create_ts from %s where gmt_create > usec_to_time(%ld) order by gmt_create limit 100",
          table_name, last_end_timestamp))) {
        LOG_WARN("assign_fmt failed", K(ret), K(last_end_timestamp));
      } else if (OB_FAIL(sql_proxy.read(res, sql.ptr()))) {
        LOG_WARN("fail to execute sql", K(sql), K(ret), K(tenant_id));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is NULL", K(sql), K(ret));
      } else {
        while (OB_SUCC(ret)) {
          if (OB_FAIL(result->next())) {
            if (OB_ITER_END == ret) {
              ret = OB_SUCCESS;
              break;
            } else {
              LOG_WARN("fail to get next result", K(sql), K(ret));
            }
          } else if (OB_ISNULL(result)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("result is NULL", K(sql),K(ret));
          } else {
            int64_t timestamp = 0;
            EXTRACT_INT_FIELD_MYSQL(*result, "create_ts", timestamp, int64_t);
            if (OB_FAIL(ret)) {
              LOG_WARN("extract mysql failed", K(ret), K(sql));
            } else if (OB_FAIL(timestamp_array.push_back(timestamp))) {
              LOG_WARN("failed to push to array", K(ret), K(timestamp_array), K(timestamp));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObStorageHADiagOperator::do_batch_delete(
        common::ObISQLClient &sql_proxy,
        const uint64_t tenant_id,
        const ObStorageHADiagModule &module,
        const ObIArray<int64_t> &timestamp_array,
        const int64_t delete_timestamp,
        int64_t &delete_index) const
{
  int ret = OB_SUCCESS;
  const char* table_name = nullptr;
  ObSqlString sql;
  int64_t affected_rows = 0;
  delete_index = 0;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || ObStorageHADiagModule::MAX_MODULE == module)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(module));
  } else if (OB_FAIL(get_table_name_(module, table_name))) {
    LOG_WARN("fail to get table name", KR(ret), K(module));
  } else if (OB_FAIL(sql.assign_fmt("delete from %s where ",
                              table_name))) {
    LOG_WARN("assign_fmt failed", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < timestamp_array.count(); i++) {
      int64_t timestamp = timestamp_array.at(i);
      if (timestamp <= delete_timestamp) {
        if (i > 0 && OB_FAIL(sql.append_fmt(" or "))) {
          LOG_WARN("append_fmt failed", K(ret), K(i));
        } else if (OB_FAIL(sql.append_fmt("gmt_create = usec_to_time(%ld)", timestamp))) {
          LOG_WARN("append_fmt failed", K(ret), K(i), K(timestamp));
        }
      } else if (0 == i) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected timestamp", K(ret), K(i), K(timestamp), K(delete_timestamp));
      } else {
        delete_index = i - 1;
        break;
      }
    }
    if (OB_FAIL(ret)) {
      //do nothing
      LOG_WARN("fail to splice sql", K(ret), K(sql));
    } else if (OB_FAIL(sql_proxy.write(tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("fail to execute sql", K(sql), K(ret), K(tenant_id));
    }
  }
  return ret;
}

//TODO(zhixing.yh) batch insert row
int ObStorageHADiagOperator::insert_row(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const uint64_t report_tenant_id,
    const ObStorageHADiagInfo &info,
    const ObStorageHADiagModule &module)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
      || !info.is_valid()
      || ObStorageHADiagModule::MAX_MODULE == module)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(info));
  } else {
    ObSqlString sql;
    ObDMLSqlSplicer dml_splicer;
    int64_t affected_rows = 0;
    const char* table_name = nullptr;
    if (OB_FAIL(fill_dml_(tenant_id, report_tenant_id, info, dml_splicer))) {
      LOG_WARN("fail to fill dml splicer", KR(ret), K(tenant_id), K(info));
    } else if (OB_FAIL(get_table_name_(module, table_name))) {
      LOG_WARN("fail to get table name", KR(ret), K(tenant_id), K(info));
    } else if (OB_FAIL(dml_splicer.splice_insert_sql(table_name, sql))) {
      LOG_WARN("fail to splice insert sql", KR(ret), K(module));
    } else if (OB_FAIL(sql_proxy.write(tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("fail to execute sql", K(sql), K(ret), K(tenant_id));
    } else {
      LOG_DEBUG("insert storage ha diagnose info history success",
          K(tenant_id), K(affected_rows), K(info));
    }
  }
  return ret;
}

int ObStorageHADiagOperator::get_table_name_(const ObStorageHADiagModule &module, const char *&table_name) const
{
  int ret = OB_SUCCESS;
  switch(module) {
    case ObStorageHADiagModule::TRANSFER_ERROR_DIAGNOSE:
      table_name = OB_ALL_STORAGE_HA_ERROR_DIAGNOSE_HISTORY_TNAME;
      break;
    case ObStorageHADiagModule::TRANSFER_PERF_DIAGNOSE:
      table_name = OB_ALL_STORAGE_HA_PERF_DIAGNOSE_HISTORY_TNAME;
      break;
    default:
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid module", K(ret), K(module));
      break;
  }
  return ret;
}

int ObStorageHADiagOperator::gen_event_ts_(int64_t &event_ts)
{
  int ret = OB_SUCCESS;
  event_ts = 0;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    const int64_t now = ObTimeUtility::current_time();
    event_ts = (last_event_ts_ >= now ? last_event_ts_ + 1 : now);
    last_event_ts_ = event_ts;
  }
  return ret;
}

int ObStorageHADiagOperator::fill_dml_(
    const uint64_t tenant_id,
    const uint64_t report_tenant_id,
    const ObStorageHADiagInfo &input_info,
    ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  char diagnose_info[common::OB_DIAGNOSE_INFO_LENGTH] = { 0 };
  char task_id[common::OB_MAX_TRACE_ID_BUFFER_SIZE] = { 0 };
  char ip_buf[common::OB_IP_STR_BUFF] = { 0 };
  int64_t timestamp = 0;
  if (OB_FAIL(gen_event_ts_(timestamp))) {
    LOG_WARN("failed to gen event ts", K(ret), K(timestamp), K(last_event_ts_));
  } else if (OB_FAIL(dml.add_gmt_create(timestamp))) {
    LOG_WARN("failed to add gmt_create", K(ret), K(input_info));
  } else if (!ObServerConfig::get_instance().self_addr_.ip_to_string(ip_buf, sizeof(ip_buf))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to set svr_ip", K(ret));
  } else if (OB_FAIL(dml.add_column("svr_ip", ip_buf))) {
    LOG_WARN("failed to add svr_ip", K(ret));
  } else if (OB_FAIL(dml.add_column("svr_port", ObServerConfig::get_instance().self_addr_.get_port()))) {
    LOG_WARN("failed to add svr_port", K(ret));
  } else if (OB_FAIL(dml.add_column("tenant_id", report_tenant_id))) {
    LOG_WARN("failed to add tenant id", K(ret), K(report_tenant_id));
  } else if (OB_FAIL(dml.add_column("ls_id", input_info.ls_id_.id()))) {
    LOG_WARN("failed to add ls id", K(ret), K(input_info));
  } else if (OB_FAIL(dml.add_column("module", input_info.get_module_str()))) {
    LOG_WARN("failed to add module", K(ret), K(input_info));
  } else if (OB_FAIL(dml.add_column("type", input_info.get_type_str()))) {
    LOG_WARN("failed to add type", K(ret), K(input_info));
  } else if (OB_FAIL(input_info.get_task_id(task_id, sizeof(task_id)))) {
    LOG_WARN("failed to get task id", K(ret), K(input_info));
  } else if (OB_FAIL(dml.add_column("task_id", task_id))) {
    LOG_WARN("failed to add task id", K(ret), K(input_info));
  } else if (OB_FAIL(dml.add_column("retry_id", input_info.retry_id_))) {
    LOG_WARN("failed to add retry id", K(ret), K(input_info));
  } else if (OB_FAIL(dml.add_column("result_code", input_info.result_code_))) {
    LOG_WARN("failed to add result code", K(ret), K(input_info));
  } else if (OB_FAIL(dml.add_column("result_msg", input_info.get_transfer_error_diagnose_msg()))) {
    LOG_WARN("failed to add result msg", K(ret), K(input_info));
  } else if (OB_FAIL(input_info.get_info(diagnose_info, sizeof(diagnose_info)))) {
    LOG_WARN("failed to get diagnose info", K(ret), K(input_info));
  } else if (OB_FAIL(dml.add_column("info", diagnose_info))) {
    LOG_WARN("failed to add info", K(ret), K(diagnose_info));
  } else {
    switch (input_info.module_) {
      case ObStorageHADiagModule::TRANSFER_ERROR_DIAGNOSE: {
        if (OB_FAIL(dml.add_time_column("create_time", input_info.timestamp_))) {
          LOG_WARN("failed to add create_time", K(ret), K(input_info));
        }
        break;
      }
      case ObStorageHADiagModule::TRANSFER_PERF_DIAGNOSE: {
        const ObTransferPerfDiagInfo &transfer_perf_info = static_cast<const ObTransferPerfDiagInfo&>(input_info);
        if (OB_FAIL(dml.add_time_column("start_timestamp", input_info.timestamp_))) {
          LOG_WARN("failed to add start timestamp", K(ret), K(transfer_perf_info));
        } else if (OB_FAIL(dml.add_time_column("end_timestamp", transfer_perf_info.end_timestamp_))) {
          LOG_WARN("failed to add end timestamp", K(ret), K(transfer_perf_info));
        } else if (OB_FAIL(dml.add_column("tablet_id", transfer_perf_info.tablet_id_.id()))) {
          LOG_WARN("failed to add tablet id", K(ret), K(transfer_perf_info));
        } else if (OB_FAIL(dml.add_column("tablet_count", transfer_perf_info.tablet_count_))) {
          LOG_WARN("failed to add tablet count", K(ret), K(transfer_perf_info));
        }
        break;
      }
      default: {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("unknow module", K(ret), K(input_info.module_));
        break;
      }
    }
  }
  return ret;
}

int ObStorageHADiagOperator::parse_result_(
    common::sqlclient::ObMySQLResult &result,
    int64_t &result_count) const
{
  int ret = OB_SUCCESS;
  result_count = 0;
  EXTRACT_INT_FIELD_MYSQL(result, "count", result_count, int64_t);
  if (OB_FAIL(ret)) {
    LOG_WARN("extract mysql failed", KR(ret));
  }
  return ret;
}

int ObStorageHADiagOperator::check_transfer_task_exist(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const share::ObTransferTaskID &task_id,
    int64_t &result_count) const
{
  int ret = OB_SUCCESS;
  result_count = 0;
  if (!is_valid_tenant_id(tenant_id) || !task_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(task_id));
  } else {
    ObSqlString sql;
    SMART_VAR(ObISQLClient::ReadResult, res) {
      sqlclient::ObMySQLResult *result = nullptr;
      if (OB_FAIL(sql.assign_fmt(
          "SELECT COUNT(*) as count FROM %s WHERE task_id = %ld",
          OB_ALL_TRANSFER_TASK_TNAME, task_id.id()))) {
        LOG_WARN("fail to assign sql", KR(ret), K(task_id));
      } else if (OB_FAIL(sql_proxy.read(res, tenant_id, sql.ptr()))) {
        LOG_WARN("execute sql failed", KR(ret), K(tenant_id), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get mysql result failed", KR(ret), K(tenant_id), K(sql));
      } else {
        if (OB_FAIL(result->next())) {
          if (OB_ITER_END == ret) {
            ret = OB_ENTRY_NOT_EXIST;
          } else {
            LOG_WARN("fail to get next result", KR(ret), K(sql));
          }
        } else if (OB_FAIL(parse_result_(*result, result_count))) {
          LOG_WARN("failed to parse result", KR(ret), K(result_count));
        } else if (OB_FAIL(result->next())) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("get next result failed", KR(ret), K(result_count));
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("read more than one row", KR(ret), K(result_count));
        }
      }
    }
  }
  return ret;
}

} // end namespace share
} // end namespace oceanbase
