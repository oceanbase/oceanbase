//Copyright (c) 2025 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#define USING_LOG_PREFIX SHARE
#include "rootserver/compaction_ttl/ob_tenant_compaction_ttl_scheduler.h"
#include "rootserver/compaction_ttl/ob_compaction_ttl_service.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "lib/timezone/ob_time_convert.h"
#include "observer/omt/ob_tenant_timezone_mgr.h"
namespace oceanbase
{
using namespace rootserver;
using namespace common;
using namespace storage;
using namespace transaction;
namespace share
{


int ObTenantCompactionTTLScheduler::get_tenant_tz_info_wrap(
    ObFixedLengthString<common::OB_MAX_TIMESTAMP_TZ_LENGTH> &time_zone,
    ObTimeZoneInfoWrap &time_zone_info_wrap)
{
  int ret = OB_SUCCESS;
  ObMultiVersionSchemaService *schema_service = nullptr;
  ObSchemaGetterGuard schema_guard;
  ObTZMapWrap tz_map_wrap;
  const ObSysVarSchema *var_schema = nullptr;
  ObTimeZoneInfoManager *tz_info_mgr = nullptr;
  if (OB_ISNULL(schema_service = GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service must not be null", K(ret));
  } else if (OB_FAIL(schema_service->get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("failed to get_tenant_schema_guard", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(schema_guard.get_tenant_system_variable(tenant_id_, share::SYS_VAR_SYSTEM_TIME_ZONE, var_schema))) {
    LOG_WARN("fail to get tenant system variable", K(ret));
  } else if (OB_ISNULL(var_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("var schema must not be null", K(ret));
  } else if (OB_FAIL(OTTZ_MGR.get_tenant_timezone(tenant_id_, tz_map_wrap, tz_info_mgr))) {
    LOG_WARN("failed to get tenant timezone", K(ret));
  } else if (OB_FAIL(time_zone.assign(var_schema->get_value()))) {
    LOG_WARN("failed to assign timezone", K(ret));
  } else if (OB_FAIL(time_zone_info_wrap.init_time_zone(var_schema->get_value(), OB_INVALID_VERSION,
             *(const_cast<ObTZInfoMap *>(tz_map_wrap.get_tz_map()))))) {
    LOG_WARN("failed to init time zone", K(ret));
  }
  return ret;
}

int ObTenantCompactionTTLScheduler::in_active_time(bool &is_active_time)
{
  int ret = OB_SUCCESS;
  is_active_time = false;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
  if (!tenant_config.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant config is not valid", KR(ret), K_(tenant_id));
  } else if (tenant_config->ttl_duty_time.disable()) {
    if (REACH_THREAD_TIME_INTERVAL(10_min)) {
      LOG_INFO("ttl_duty_time is disabled, skip compaction ttl duty", K_(tenant_id));
    }
  } else {
    const int config_hour = tenant_config->ttl_duty_time.hour();
    const int config_minute = tenant_config->ttl_duty_time.minute();

    // Get tenant's timezone info
    ObTimeZoneInfoWrap time_zone_info_wrap;
    ObFixedLengthString<common::OB_MAX_TIMESTAMP_TZ_LENGTH> time_zone;
    if (OB_FAIL(get_tenant_tz_info_wrap(time_zone, time_zone_info_wrap))) {
      LOG_WARN("failed to get tenant sys time zone wrap", KR(ret), K_(tenant_id));
    } else {
      // Get current time in microseconds
      const int64_t cur_time_us = ObTimeUtility::current_time();

      // Convert current time to ObTime with tenant's timezone
      ObTime ob_time;
      const ObTimeZoneInfo *tz_info = time_zone_info_wrap.is_position_class() ?
                                      &time_zone_info_wrap.get_tz_info_pos() :
                                      time_zone_info_wrap.get_time_zone_info();

      if (OB_FAIL(ObTimeConverter::datetime_to_ob_time(cur_time_us, tz_info, ob_time))) {
        LOG_WARN("failed to convert datetime to ob_time", KR(ret), K(cur_time_us));
      } else {
        const int cur_hour = ob_time.parts_[DT_HOUR];
        const int cur_minute = ob_time.parts_[DT_MIN];
        if ((cur_hour == config_hour) && (cur_minute == config_minute)) {
          is_active_time = true;
        }
        LOG_DEBUG("check compaction ttl active time", K_(tenant_id), K(config_hour), K(config_minute),
                 K(cur_hour), K(cur_minute), K(is_active_time), K(time_zone));
      }
    }
  }
  return ret;
}

int ObTenantCompactionTTLScheduler::check_is_ttl_table(
  const ObTableSchema &table_schema,
  const uint64_t tenant_data_version,
  bool &is_ttl_table)
{
  int ret = OB_SUCCESS;
  is_ttl_table = false;
  if (!table_schema.is_user_table()) {
  } else if (OB_FAIL(ObTTLUtil::check_is_normal_ttl_table(table_schema, is_ttl_table, false/*check_recyclebin*/))) {
    LOG_ERROR("fail to check is ttl table", KR(ret), K(table_schema.get_table_name()));
  } else if (is_ttl_table && OB_FAIL(ObCompactionTTLUtil::is_compaction_ttl_schema(
      tenant_data_version, table_schema, is_ttl_table))) {
    LOG_WARN("fail to check is compaction ttl table", KR(ret), K(table_schema.get_table_name()));
  } else if (is_ttl_table) {
    if (OB_UNLIKELY(table_schema.get_ttl_flag().ttl_type_ != ObTTLDefinition::COMPACTION)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema is compaction ttl table but ttl_type is not COMPACTION", KR(ret), K(table_schema.get_table_name()),
        K(table_schema.get_ttl_flag().ttl_type_));
    }
  } else {
    if (OB_UNLIKELY(table_schema.get_ttl_flag().ttl_type_ == ObTTLDefinition::COMPACTION)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid COMPACTION ttl_type", KR(ret), K(is_ttl_table),
       "table_name", table_schema.get_table_name(),
       "table_id", table_schema.get_table_id(),
       "ttl_definition", table_schema.get_ttl_definition(),
       "ttl_type", table_schema.get_ttl_flag().ttl_type_);
    }
  }
  return ret;
}

int ObTenantCompactionTTLScheduler::check_all_table_finished(bool &all_finished)
{
  int ret = OB_SUCCESS;
  all_finished = true;
  uint64_t tenant_data_version = 0;
  ObSEArray<uint64_t, DEFAULT_TABLE_ARRAY_SIZE> table_id_array;
  if (OB_FAIL(ObTTLUtil::get_tenant_table_ids(tenant_id_, table_id_array))) {
    LOG_WARN("fail to get tenant table ids", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id_, tenant_data_version))) {
    LOG_WARN("fail to get min data version", K(ret));
  }

  // 2. get all ttl table ids
  int64_t start_idx = 0;
  int64_t end_idx = 0;
  while (OB_SUCC(ret) && start_idx < table_id_array.count() && all_finished) {
    // temp schema guard to loop tablet table ids
    ObSchemaGetterGuard schema_guard;
    start_idx = end_idx;
    end_idx = MIN(table_id_array.count(), start_idx + TBALE_CHECK_BATCH_SIZE);
    bool is_compaction_ttl_table = false;
    if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(tenant_id_, schema_guard))) {
      LOG_WARN("fail to get schema guard", KR(ret), K_(tenant_id));
    }
    for (int64_t idx = start_idx; OB_SUCC(ret) && idx < end_idx; ++idx) {
      const int64_t table_id = table_id_array.at(idx);
      const ObTableSchema *table_schema = nullptr;
      if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, table_id, table_schema))) {
        LOG_WARN("fail to get simple schema", KR(ret), K(table_id));
      } else if (OB_ISNULL(table_schema)) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("table schema is null", KR(ret), K(table_id));
      } else if (OB_FAIL(check_is_ttl_table(*table_schema, tenant_data_version, is_compaction_ttl_table))) {
        LOG_ERROR("fail to check is ttl table", KR(ret), K(table_id), "table_name", table_schema->get_table_name());
        // skip this error table to prevent one table from causing TTL unavailability.
        ret = OB_SUCCESS; // ignore error
      } else if (is_compaction_ttl_table) {
       if (OB_FAIL(deal_with_compaction_ttl_table(*table_schema, all_finished))) {
          LOG_WARN("fail to deal with compaction ttl table", KR(ret), K(table_id));
        }
      }
    } // for
  }
  if (OB_SUCC(ret) && unsync_compaction_ttl_task_infos_.count() > 0) {
    if (OB_FAIL(retry_to_sync_compaction_ttl_task_info())) {
      LOG_WARN("fail to retry to sync compaction ttl task info", KR(ret), K_(tenant_id));
    }
    if (all_finished && !unsync_compaction_ttl_task_infos_.empty()) { // retry failed, need to sync again
      all_finished = false;
      LOG_INFO("has unsync compaction ttl task infos, need retry", KR(ret), K_(tenant_id), K_(unsync_compaction_ttl_task_infos));
    }
  }
#ifdef ERRSIM
  if (!all_finished) {
    SERVER_EVENT_SYNC_ADD("ttl_errsim", "ttl_check_all_table_finished", "tenant_id", tenant_id_,
      "all_finished", all_finished, "ret", ret,
      "unsync_compaction_ttl_task_infos", unsync_compaction_ttl_task_infos_.count());
  }
#endif
  return ret;
}

int ObTenantCompactionTTLScheduler::deal_with_compaction_ttl_table(
  const ObTableSchema &table_schema,
  bool &all_finished)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const uint64_t table_id = table_schema.get_table_id();
  bool exists = false;
  if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else if (OB_FAIL(check_compaction_ttl_task_exists(table_id, exists))) {
    LOG_WARN("fail to check compaction ttl task exists", KR(ret), K(table_id));
  } else if (!exists && unsync_compaction_ttl_task_infos_.count() > 0) {
    for (int64_t i = 0; OB_SUCC(ret) && i < unsync_compaction_ttl_task_infos_.count(); i++) {
      if (table_id == unsync_compaction_ttl_task_infos_.at(i).table_id_) {
        exists = true;
        break;
      }
    } // for
  }
  if (OB_SUCC(ret) && !exists) {
    transaction::ObTransID tx_id;
    const int64_t start_ts = ObTimeUtility::current_time();
    ObCompactionTTLService compaction_ttl_service(tenant_id_, table_id);
    ObTTLTaskStatus task_status = ObTTLTaskStatus::OB_TTL_TASK_INVALID;
    int err_code = compaction_ttl_service.execute(tenant_task_.get_task_start_ts(), *sql_proxy_, tx_id, task_status);
    const int64_t end_ts = ObTimeUtility::current_time();
    if (OB_TABLE_IS_DELETED == err_code) {
    } else if (OB_TRANS_TIMEOUT == err_code || OB_TRANS_UNKNOWN == err_code) {
      int64_t state = 0;
      if (OB_TMP_FAIL(get_tx_state(ObLSID(ObLSID::SYS_LS_ID), tx_id, state))) {
        LOG_WARN("fail to get tx state", KR(tmp_ret), K(table_id));
      } else if (ObTxData::COMMIT == state) {
        err_code = OB_SUCCESS;
        task_status = ObTTLTaskStatus::OB_TTL_TASK_FINISH;
      } else {
        LOG_WARN("tx state is not commit", KR(tmp_ret), K(tx_id), K(table_id), K(state));
      }
    }
    if (OB_SUCC(ret) && OB_TABLE_IS_DELETED != err_code) {
      if (ObTTLTaskStatus::OB_TTL_TASK_FINISH != task_status && ObTTLTaskStatus::OB_TTL_TASK_SKIP != task_status) {
        ret = err_code;
        LOG_WARN("fail to execute compaction ttl service", KR(ret), K(table_id));
        all_finished = false; // failed to execute compaction ttl service, need to retry again
      }
       // record all result
      if (OB_TMP_FAIL(replace_compaction_ttl_task(table_id, err_code, task_status, start_ts, end_ts))) {
        LOG_WARN("fail to replace compaction ttl task into inner_table", KR(tmp_ret), K(table_id));
      }
      LOG_INFO("[COMPACTION TTL] success to execute compaction ttl service", K(table_id), K(err_code), K(tx_id),
        K(start_ts), K(end_ts), K(task_status));
    }
  }
  return ret;
}

int ObTenantCompactionTTLScheduler::replace_compaction_ttl_task(
  const uint64_t table_id,
  const int64_t err_code,
  const ObTTLTaskStatus task_status,
  const int64_t start_ts,
  const int64_t end_ts)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTTLStatus ttl_task;

  if (OB_FAIL(create_compaction_ttl_task_record(table_id, err_code, task_status, start_ts, end_ts, ttl_task))) {
    LOG_WARN("fail to create compaction TTL task record", KR(ret), K_(tenant_id), K(table_id));
  } else if (OB_FAIL(ObTTLUtil::replace_ttl_task(tenant_id_,
                                                share::OB_ALL_KV_TTL_TASK_TNAME,
                                                *sql_proxy_,
                                                ttl_task))) {
    LOG_WARN("fail to replace compaction TTL task", KR(ret), K_(tenant_id), K(table_id));
    if (ObTTLTaskStatus::OB_TTL_TASK_FINISH == task_status) {
      if (OB_TMP_FAIL(unsync_compaction_ttl_task_infos_.push_back(
        ObCompactionTTLTaskInfo(table_id, start_ts, end_ts)))) {
        LOG_WARN("fail to push back compaction ttl task info", KR(tmp_ret), K(table_id));
      } else {
        LOG_INFO("[COMPACTION TTL] success to unsync_compaction_ttl_task_infos", K_(tenant_id), K(table_id), K(task_status),
          K_(unsync_compaction_ttl_task_infos));
      }
    }
  } else {
    LOG_INFO("[COMPACTION TTL] success to replace compaction TTL task", K_(tenant_id), K(table_id));
  }

  return ret;
}

// Member function to check if compaction TTL task exists
// TODO(@lixia.yq) record table id in memory to avoid repeat check inner_table
int ObTenantCompactionTTLScheduler::check_compaction_ttl_task_exists(const uint64_t table_id, bool &exists)
{
  int ret = OB_SUCCESS;
  exists = false;
  ObSqlString sql;
  SMART_VAR(ObISQLClient::ReadResult, res) {
    sqlclient::ObMySQLResult* result = nullptr;
    if (OB_FAIL(sql.append_fmt(
        "SELECT count(*) as cnt FROM %s WHERE tenant_id = %ld AND table_id = %ld AND "
        "task_id = %ld AND tablet_id = %ld AND (status = %ld OR status = %ld) AND task_type = %ld",
        share::OB_ALL_KV_TTL_TASK_TNAME,
        tenant_id_,
        table_id,
        tenant_task_.ttl_status_.task_id_,
        ObTTLUtil::COMPACTION_TTL_TABLET_ID,
        static_cast<int64_t>(ObTTLTaskStatus::OB_TTL_TASK_FINISH),
        static_cast<int64_t>(ObTTLTaskStatus::OB_TTL_TASK_SKIP),
        static_cast<int64_t>(ObTTLType::COMPACTION_TTL)))) {
      LOG_WARN("fail to assign sql", KR(ret));
    } else if (OB_FAIL(sql_proxy_->read(res, gen_meta_tenant_id(tenant_id_), sql.ptr()))) {
      LOG_WARN("fail to execute sql", KR(ret), K_(tenant_id), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get mysql result failed", KR(ret), K(sql));
    } else if (OB_FAIL(result->next())) {
      LOG_WARN("fail to get next result", KR(ret), K(sql));
    } else {
      int64_t cnt = 0;
      EXTRACT_INT_FIELD_MYSQL_WITH_DEFAULT_VALUE(*result, "cnt", cnt, int64_t,
          true/*skip_null_error*/, false/*skip_column_error*/, 0/*default value*/);
      if (OB_SUCC(ret) && cnt > 0) {
        exists = true;
      }
    }
  }
  return ret;
}

int ObTenantCompactionTTLScheduler::retry_to_sync_compaction_ttl_task_info()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else {
    ObSEArray<ObCompactionTTLTaskInfo, 4> tmp_array;
    if (OB_FAIL(tmp_array.assign(unsync_compaction_ttl_task_infos_))) {
      LOG_WARN("fail to assign", KR(ret));
    }

    int64_t idx = 0;
    while (OB_SUCC(ret) && idx < tmp_array.count()) {
      const ObCompactionTTLTaskInfo &compaction_ttl_task_info = tmp_array.at(idx);
      if (OB_FAIL(replace_compaction_ttl_task(
        compaction_ttl_task_info.table_id_, OB_SUCCESS, ObTTLTaskStatus::OB_TTL_TASK_FINISH,
        compaction_ttl_task_info.start_ts_, compaction_ttl_task_info.end_ts_))) {
        LOG_WARN("fail to replace compaction ttl task", KR(ret), K(compaction_ttl_task_info.table_id_));
      } else {
        ++idx;
      }
    }
    if (OB_SUCC(ret) || idx > 0) {
      // OB_FAIL and idx = 0 means all replace tasks are failed, don't need to reset unsync_compaction_ttl_task_infos_
      unsync_compaction_ttl_task_infos_.reset();
      const int64_t succ_cnt = idx;
      while (OB_SUCCESS == tmp_ret && idx < tmp_array.count()) {
        if (OB_TMP_FAIL(unsync_compaction_ttl_task_infos_.push_back(tmp_array.at(idx)))) {
          LOG_WARN("fail to push back compaction ttl task info", KR(tmp_ret), K(tmp_array.at(idx)));
        }
        ++idx;
      } // while
      LOG_INFO("[COMPACTION TTL] success to replace compaction ttl task when retry", K_(tenant_id),
        K(succ_cnt), "failed_cnt", tmp_array.count() - succ_cnt, K_(unsync_compaction_ttl_task_infos));
    }
  }
  return ret;
}

// Helper function to create compaction TTL task record
int ObTenantCompactionTTLScheduler::create_compaction_ttl_task_record(
  const uint64_t table_id,
  const int64_t err_code,
  const ObTTLTaskStatus task_status,
  const int64_t start_ts,
  const int64_t end_ts,
  ObTTLStatus &ttl_task)
{
  int ret = OB_SUCCESS;
  ttl_task.tenant_id_ = tenant_id_;
  ttl_task.table_id_ = table_id;
  ttl_task.tablet_id_ = ObTTLUtil::COMPACTION_TTL_TABLET_ID;
  ttl_task.task_id_ = tenant_task_.ttl_status_.task_id_;
  ttl_task.task_start_time_ = start_ts;
  ttl_task.task_update_time_ = end_ts;
  ttl_task.trigger_type_ = tenant_task_.ttl_status_.trigger_type_;
  ttl_task.status_ = static_cast<int64_t>(task_status);
  ttl_task.ttl_del_cnt_ = -1;
  ttl_task.max_version_del_cnt_ = -1;
  ttl_task.scan_cnt_ = -1;
  ttl_task.row_key_.reset();
  ttl_task.ret_code_ = common::ob_error_name(err_code);
  ttl_task.task_type_ = ObTTLType::COMPACTION_TTL;

  return ret;
}

int ObTenantCompactionTTLScheduler::get_tx_state(
  const share::ObLSID &ls_id,
  const transaction::ObTransID &tx_id,
  int64_t &state)
{
  int ret = OB_SUCCESS;
  ObTxTableGuard tx_table_guard;
  ObLSHandle ls_handle;
  SCN commit_version;
  SCN recycled_scn;
  if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::TRANS_MOD))) {
    LOG_WARN("get ls handle fail", KR(ret), K(ls_id));
  } else if (OB_FAIL(ls_handle.get_ls()->get_tx_table()->get_tx_table_guard(tx_table_guard))) {
    LOG_WARN("get tx table guard failed", KR(ret), K(ls_id));
  } else if (OB_FAIL(tx_table_guard.try_get_tx_state(tx_id, state, commit_version, recycled_scn))) {
    LOG_WARN("get tx state failed", KR(ret), K(ls_id), K(tx_id));
  }
  return ret;
}


} // namespace share
} // namespace oceanbase
