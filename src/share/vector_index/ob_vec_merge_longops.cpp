/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include "share/vector_index/ob_vec_merge_longops.h"
#include "share/ob_ddl_common.h"
#include "observer/ob_server_struct.h"

namespace oceanbase
{
namespace share
{

int ObVecMergeLongopsKey::to_key_string()
{
  int ret = OB_SUCCESS;
  int64_t name_pos = 0;
  int64_t target_pos = 0;
  if (OB_FAIL(databuff_printf(name_, common::MAX_LONG_OPS_NAME_LENGTH, name_pos, "VEC MERGE TASK"))) {
    LOG_WARN("fail to set name string", K(ret));
  } else if (OB_FAIL(databuff_printf(target_, common::MAX_LONG_OPS_TARGET_LENGTH, target_pos, "task_id=%ld", task_id_))) {
    LOG_WARN("fail to set target string", K(ret));
  }
  return ret;
}

int ObVecMergeLongopsStat::init(
    const uint64_t tenant_id,
    const int64_t task_id,
    const common::ObCurTraceId::TraceId &trace_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || task_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(task_id));
  } else {
    tenant_id_ = tenant_id;
    task_id_ = task_id;
    trace_id_ = trace_id;
    key_.tenant_id_ = tenant_id;
    key_.sid_ = OB_INVALID_ID;
    key_.task_id_ = task_id;
    if (OB_FAIL(key_.to_key_string())) {
      LOG_WARN("failed to generate key string", K(ret));
    } else {
      start_time_ = ObTimeUtility::current_time();
      is_inited_ = true;
    }
  }
  return ret;
}

int ObVecMergeLongopsStat::get_longops_value(ObLongopsValue &value)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    value.reset();
    value.trace_id_ = trace_id_;
    value.sid_ = OB_INVALID_ID;
    value.tenant_id_ = tenant_id_;
    value.start_time_ = start_time_;
    value.elapsed_seconds_ = (ObTimeUtility::current_time() - start_time_);
    value.last_update_time_ = ObTimeUtility::current_time();
    MEMCPY(value.op_name_, key_.name_, common::MAX_LONG_OPS_NAME_LENGTH);
    MEMCPY(value.target_, key_.target_, common::MAX_LONG_OPS_TARGET_LENGTH);

    if (OB_FAIL(collect_from_plan_monitor(value))) {
      int64_t pos = 0;
      if (OB_SUCCESS != databuff_printf(value.message_, common::MAX_LONG_OPS_MESSAGE_LENGTH, pos,
                                        "STATUS: INSERTING, WAITING FOR PLAN MONITOR DATA")) {
        LOG_WARN("failed to printf fallback message");
      }
      value.percentage_ = 0;
      value.time_remaining_ = 0;
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObVecMergeLongopsStat::collect_from_plan_monitor(ObLongopsValue &value)
{
  int ret = OB_SUCCESS;
  SMART_VARS_3((ObSqlMonitorStatsCollector, collector),
               (ObDDLDiagnoseInfo, diagnose_info),
               (ObSqlMonitorStats, sql_monitor_stats)) {
    ObDDLTaskStatInfo stat_info;
    int64_t pos = 0;
    if (OB_FAIL(collector.scan_task_id_.push_back(task_id_))) {
      LOG_WARN("failed to push back task_id", K(ret));
    } else if (OB_FAIL(collector.scan_tenant_id_.push_back(tenant_id_))) {
      LOG_WARN("failed to push back tenant_id", K(ret));
    } else if (OB_FAIL(collector.init(GCTX.sql_proxy_))) {
      LOG_WARN("failed to init collector", K(ret));
    } else if (OB_FAIL(diagnose_info.init(tenant_id_, task_id_,
                   ObDDLType::DDL_CREATE_INDEX, 1/*execution_id*/))) {
      LOG_WARN("failed to init diagnose_info", K(ret));
    } else if (OB_FAIL(sql_monitor_stats.init(tenant_id_, task_id_,
                   ObDDLType::DDL_CREATE_INDEX))) {
      LOG_WARN("failed to init sql_monitor_stats", K(ret));
    } else if (OB_FAIL(collector.get_next_sql_plan_monitor_stat(sql_monitor_stats))) {
      LOG_WARN("failed to get plan monitor stat", K(ret));
    } else if (sql_monitor_stats.is_empty_) {
      ret = OB_EMPTY_RESULT;
    } else if (OB_FAIL(diagnose_info.process_sql_monitor_and_generate_longops_message(
                   sql_monitor_stats, 1/*target_cg_cnt*/, stat_info, pos))) {
      LOG_WARN("failed to process and generate message", K(ret));
    } else {
      value.time_remaining_ = stat_info.time_remaining_;
      value.percentage_ = stat_info.percentage_;
      MEMCPY(value.message_, stat_info.message_, common::MAX_LONG_OPS_MESSAGE_LENGTH);
    }
  }
  return ret;
}

} // end namespace share
} // end namespace oceanbase
