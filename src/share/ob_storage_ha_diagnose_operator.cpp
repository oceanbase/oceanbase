/**
 * Copyright (c) 2022 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include "share/inner_table/ob_inner_table_schema.h"
#include "share/ob_storage_ha_diagnose_operator.h"
#include "share/ob_dml_sql_splicer.h"
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

int ObStorageHADiagOperator::get_table_name_(const ObStorageHADiagModule &module, const char *&table_name) const
{
  int ret = OB_SUCCESS;
  if (ObStorageHADiagModule::TRANSFER_ERROR_DIAGNOSE == module) {
    table_name = OB_ALL_STORAGE_HA_ERROR_DIAGNOSE_HISTORY_TNAME;
  } else if (ObStorageHADiagModule::TRANSFER_PERF_DIAGNOSE == module) {
    table_name = OB_ALL_STORAGE_HA_PERF_DIAGNOSE_HISTORY_TNAME;
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid module", K(ret), K(module));
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

int ObStorageHADiagOperator::delete_expired_rows(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const ObStorageHADiagModule &module,
    const int64_t delete_timestamp,
    const int64_t batch_limit,
    int64_t &affected_rows) const
{
  int ret = OB_SUCCESS;
  const char *table_name = nullptr;
  ObSqlString sql;
  affected_rows = 0;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
      || ObStorageHADiagModule::MAX_MODULE == module
      || batch_limit <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(module), K(batch_limit));
  } else if (OB_FAIL(get_table_name_(module, table_name))) {
    LOG_WARN("fail to get table name", KR(ret), K(module));
  } else if (OB_FAIL(sql.assign_fmt(
      "DELETE FROM %s WHERE gmt_create <= usec_to_time(%ld) LIMIT %ld",
      table_name, delete_timestamp, batch_limit))) {
    LOG_WARN("assign_fmt failed", K(ret), K(delete_timestamp), K(batch_limit));
  } else if (OB_FAIL(sql_proxy.write(tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("fail to execute sql", K(sql), K(ret), K(tenant_id));
  }
  return ret;
}

int ObStorageHADiagOperator::append_inflight_row(
    const uint64_t report_tenant_id,
    const ObLSID &ls_id,
    const ObStorageHADiagModule &module,
    const ObStorageHADiagTaskType task_type,
    const ObHAInflightDiagState &state,
    const int32_t result_code,
    const int64_t retry_id,
    ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(report_tenant_id)
      || !ls_id.is_valid()
      || ObStorageHADiagModule::MAX_MODULE == module
      || !is_valid_ha_diag_task_type(task_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(report_tenant_id),
        K(ls_id), K(module), "task_type", ha_diag_task_type_str(task_type));
  } else if (OB_FAIL(fill_inflight_dml_(report_tenant_id, ls_id, module, task_type,
          state, result_code, retry_id, dml))) {
    LOG_WARN("fail to fill inflight dml splicer", KR(ret), K(report_tenant_id), K(module));
  } else if (OB_FAIL(dml.finish_row())) {
    LOG_WARN("fail to finish row", KR(ret), K(report_tenant_id), K(module));
  }
  return ret;
}

int ObStorageHADiagOperator::batch_write_inflight_rows(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const ObStorageHADiagModule &module,
    ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
      || ObStorageHADiagModule::MAX_MODULE == module)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(module));
  } else if (0 == dml.get_row_count()) {
    // nothing to flush
  } else {
    const char *table_name = nullptr;
    ObSqlString sql;
    int64_t affected_rows = 0;
    const int64_t row_count = dml.get_row_count();
    if (OB_FAIL(get_table_name_(module, table_name))) {
      LOG_WARN("fail to get table name", KR(ret), K(module));
    } else if (OB_FAIL(dml.splice_batch_insert_sql(table_name, sql))) {
      LOG_WARN("fail to splice batch insert sql", KR(ret), K(module));
    } else if (OB_FAIL(sql_proxy.write(tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("fail to execute sql", K(sql), K(ret), K(tenant_id));
    }
  }
  return ret;
}

int ObStorageHADiagOperator::fill_inflight_dml_(
    const uint64_t report_tenant_id,
    const ObLSID &ls_id,
    const ObStorageHADiagModule &module,
    const ObStorageHADiagTaskType task_type,
    const ObHAInflightDiagState &state,
    const int32_t result_code,
    const int64_t retry_id,
    ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  char diagnose_info[common::OB_DIAGNOSE_INFO_LENGTH] = { 0 };
  char task_id_buf[common::OB_MAX_TRACE_ID_BUFFER_SIZE] = { 0 };
  char ip_buf[common::OB_IP_STR_BUFF] = { 0 };
  int64_t timestamp = 0;
  const int64_t module_idx = static_cast<int64_t>(module);
  const int64_t msg_idx = static_cast<int64_t>(state.last_result_msg_);
  if (OB_FAIL(gen_event_ts_(timestamp))) {
    LOG_WARN("failed to gen event ts", K(ret));
  } else if (OB_FAIL(dml.add_gmt_create(timestamp))) {
    LOG_WARN("failed to add gmt_create", K(ret));
  } else if (!ObServerConfig::get_instance().self_addr_.ip_to_string(ip_buf, sizeof(ip_buf))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to set svr_ip", K(ret));
  } else if (OB_FAIL(dml.add_column("svr_ip", ip_buf))) {
    LOG_WARN("failed to add svr_ip", K(ret));
  } else if (OB_FAIL(dml.add_column("svr_port", ObServerConfig::get_instance().self_addr_.get_port()))) {
    LOG_WARN("failed to add svr_port", K(ret));
  } else if (OB_FAIL(dml.add_column("tenant_id", report_tenant_id))) {
    LOG_WARN("failed to add tenant id", K(ret), K(report_tenant_id));
  } else if (OB_FAIL(dml.add_column("ls_id", ls_id.id()))) {
    LOG_WARN("failed to add ls id", K(ret), K(ls_id));
  } else if (OB_FAIL(dml.add_column("module", ObStorageDiagModuleStr[module_idx]))) {
    LOG_WARN("failed to add module", K(ret), K(module));
  } else if (OB_FAIL(dml.add_column("type", ha_diag_task_type_str(task_type)))) {
    LOG_WARN("failed to add type", K(ret), "task_type", ha_diag_task_type_str(task_type));
  } else if (FALSE_IT(snprintf(task_id_buf, sizeof(task_id_buf), "%ld", state.task_id_.id()))) {
  } else if (OB_FAIL(dml.add_column("task_id", task_id_buf))) {
    LOG_WARN("failed to add task id", K(ret));
  } else if (OB_FAIL(dml.add_column("retry_id", retry_id))) {
    LOG_WARN("failed to add retry id", K(ret), K(retry_id));
  } else if (OB_FAIL(dml.add_column("result_code", result_code))) {
    LOG_WARN("failed to add result code", K(ret), K(result_code));
  } else if (OB_FAIL(dml.add_column("result_msg",
      (msg_idx >= 0 && msg_idx < static_cast<int64_t>(ObStorageHACostItemName::MAX_NAME))
        ? ObTransferErrorDiagMsg[msg_idx]
        : "Unstatistical errors"))) {
    LOG_WARN("failed to add result msg", K(ret), K(msg_idx));
  } else if (OB_FAIL(ObHAInflightDiag::serialize_info(state, diagnose_info, sizeof(diagnose_info)))) {
    LOG_WARN("failed to serialize info", K(ret));
  } else if (OB_FAIL(dml.add_column("info", diagnose_info))) {
    LOG_WARN("failed to add info", K(ret));
  } else {
    const bool is_perf = (ObStorageHADiagModule::TRANSFER_PERF_DIAGNOSE == module);
    if (is_perf) {
      if (OB_FAIL(dml.add_time_column("start_timestamp", state.start_ts_))) {
        LOG_WARN("failed to add start timestamp", K(ret));
      } else if (OB_FAIL(dml.add_time_column("end_timestamp", state.end_ts_))) {
        LOG_WARN("failed to add end timestamp", K(ret));
      } else if (OB_FAIL(dml.add_column("tablet_id", 0L))) {
        LOG_WARN("failed to add tablet id", K(ret));
      } else if (OB_FAIL(dml.add_column("tablet_count", 0L))) {
        LOG_WARN("failed to add tablet count", K(ret));
      }
    } else {
      if (OB_FAIL(dml.add_time_column("create_time", state.start_ts_))) {
        LOG_WARN("failed to add create_time", K(ret));
      }
    }
  }
  return ret;
}

} // end namespace share
} // end namespace oceanbase
