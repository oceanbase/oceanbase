/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX DATA_DICT

#include "pl/sys_package/ob_dbms_data_dict.h"
#include "share/ob_common_rpc_proxy.h"
#include "observer/ob_server_struct.h"  // GCTX
#include "common/ob_timeout_ctx.h"      // ObTimeoutCtx
#include "lib/utility/ob_tracepoint_def.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace observer;
using namespace obrpc;

namespace pl
{
int ObDBMSDataDict::trigger_dump_data_dict(
    sql::ObExecContext &ctx,
    sql::ParamStore &params,
    common::ObObj &result)
{
  int ret = OB_SUCCESS;
  UNUSED(result);
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  uint64_t data_version = 0;
  ObAddr leader;
  share::SCN base_scn;
  int64_t data_dict_dump_history_retention_sec = OB_INVALID_TIMESTAMP;
  int32_t data_dict_dump_history_retention_day = -1;
  ObTriggerDumpDataDictArg trigger_dump_data_dict_arg;
  int64_t errsim_code = EventTable::EN_DICT_MODIFY_ITEM_RETENION;
  const static int64_t SECONDS_OF_DAY = 24 * 3600;
  LOG_INFO("trigger dump data_dict begin", K(params));

  if (OB_FAIL(check_op_allowed_(ctx, tenant_id))) {
    LOG_WARN("check_op_allowed failed", KR(ret));
  } else if (OB_UNLIKELY(1 != params.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid params count for trigger_dump_data_dict", KR(ret), K(params));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "trigger_dump_data_dict");
  } else if (!params.at(0).is_null() && OB_FAIL(params.at(0).get_int32(data_dict_dump_history_retention_day))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get int failed", KR(ret), K(params.at(0)));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "data_dict_dump_history_retention_day of trigger_dump_data_dict");
  } else if (data_dict_dump_history_retention_day >= 0
      && FALSE_IT(data_dict_dump_history_retention_sec = data_dict_dump_history_retention_day * SECONDS_OF_DAY)) {
  } else if (OB_UNLIKELY(errsim_code != OB_SUCCESS) // errsim mode for test
      && FALSE_IT(data_dict_dump_history_retention_sec = data_dict_dump_history_retention_day)) {
  } else if (OB_FAIL(get_tenant_gts_(tenant_id, base_scn))) {
    LOG_WARN("get_tenant_gts failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(trigger_dump_data_dict_arg.init(base_scn, data_dict_dump_history_retention_sec))) {
    LOG_WARN("init trigger_dump_data_dict_arg failed", KR(ret), K(base_scn), K(data_dict_dump_history_retention_sec), K(params));
  } else if (OB_FAIL(send_trigger_dump_dict_rpc_(tenant_id, trigger_dump_data_dict_arg))) {
    LOG_WARN("send_trigger_dump_dict_rpc_ failed", KR(ret), K(tenant_id), K(trigger_dump_data_dict_arg));
  }

  if (OB_SUCC(ret)) {
    LOG_INFO("trigger dump data_dict succ", KR(ret), K(errsim_code), K(params));
  } else {
    LOG_WARN("trigger dump data_dict failed", KR(ret), K(errsim_code), K(params));
  }

  return ret;
}

int ObDBMSDataDict::enable_dump(
    sql::ObExecContext &ctx,
    sql::ParamStore &params,
    common::ObObj &result)
{
  int ret = OB_SUCCESS;
  UNUSED(result);
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  ObObj is_job_enable;
  is_job_enable.set_bool(true);

  if (OB_FAIL(check_op_allowed_(ctx, tenant_id))) {
    LOG_WARN("check_op_allowed failed", KR(ret));
  } else if (OB_FAIL(modify_schedule_job_(tenant_id, "enabled", is_job_enable))) {
    LOG_WARN("modify_schedule_job_ attr enable SCHEDULED_TRIGGER_DUMP_DATA_DICT failed",
        KR(ret), K(tenant_id), K(params), K(is_job_enable));
  }

  if (OB_SUCC(ret)) {
    LOG_INFO("enable dump data_dict succ", KR(ret), K(params));
  } else {
    LOG_WARN("enable dump data_dict failed", KR(ret), K(params));
  }

  return ret;
}

int ObDBMSDataDict::disable_dump(
    sql::ObExecContext &ctx,
    sql::ParamStore &params,
    common::ObObj &result)
{
  int ret = OB_SUCCESS;
  UNUSED(result);
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  ObObj is_job_enable;
  is_job_enable.set_bool(false);

  if (OB_FAIL(check_op_allowed_(ctx, tenant_id))) {
    LOG_WARN("check_op_allowed failed", KR(ret));
  } else if (OB_FAIL(modify_schedule_job_(tenant_id, "enabled", is_job_enable))) {
    LOG_WARN("modify_schedule_job_ attr enable SCHEDULED_TRIGGER_DUMP_DATA_DICT failed",
        KR(ret), K(tenant_id), K(params), K(is_job_enable));
  } else {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
    const int64_t dump_interval = tenant_config->dump_data_dictionary_to_log_interval;
    if (dump_interval > 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("can't disable dump_data_dict while dict still can dump by config dump_data_dictionary_to_log_interval",
          KR(ret), K(dump_interval));
      LOG_USER_ERROR(OB_ERR_UNEXPECTED, "please execute `alter system set dump_data_dictionary_to_log_interval = '0s'` then retry");
    }
  }

  if (OB_SUCC(ret)) {
    LOG_INFO("disable dump data_dict succ", KR(ret), K(params));
  } else {
    LOG_WARN("disable dump data_dict failed", KR(ret), K(params));
  }

  return ret;
}

int ObDBMSDataDict::modify_interval(
    sql::ObExecContext &ctx,
    sql::ParamStore &params,
    common::ObObj &result)
{
  int ret = OB_SUCCESS;
  UNUSED(result);
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  ObObj repeat_interval;

  if (OB_FAIL(check_op_allowed_(ctx, tenant_id))) {
    LOG_WARN("check_op_allowed failed", KR(ret));
  } else if (OB_UNLIKELY(1 != params.count()
      || params.at(0).is_null()
      || !params.at(0).is_string_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid params count for modify_interval", KR(ret), K(params));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "DBMS_DATA_DICT.MODIFY_DUMP_INTERVAL parameter");
  } else if (FALSE_IT(repeat_interval.set_varchar(params.at(0).get_string()))) {
  } else if (OB_FAIL(modify_schedule_job_(tenant_id, "repeat_interval", repeat_interval))) {
    LOG_WARN("modify_schedule_job_ attr repeat_interval SCHEDULED_TRIGGER_DUMP_DATA_DICT failed",
        KR(ret), K(tenant_id), K(params), K(repeat_interval));
  }

  if (OB_SUCC(ret)) {
    LOG_INFO("modify dump interval succ", KR(ret), K(params));
  } else {
    LOG_WARN("modify dump interval failed", KR(ret), K(params));
  }

  return ret;
}

int ObDBMSDataDict::modify_duration(
    sql::ObExecContext &ctx,
    sql::ParamStore &params,
    common::ObObj &result)
{
  int ret = OB_SUCCESS;
  UNUSED(result);
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  ObObj max_run_duration;

  if (OB_FAIL(check_op_allowed_(ctx, tenant_id))) {
    LOG_WARN("check_op_allowed failed", KR(ret));
  } else if (OB_UNLIKELY(1 != params.count()
      || params.at(0).is_null()
      || !params.at(0).is_integer_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid params count for modify_duration", KR(ret), K(params));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "DBMS_DATA_DICT.MODIFY_RUN_DURATION parameter");
  } else if (FALSE_IT(max_run_duration.set_int(params.at(0).get_int()))) {
  } else if (OB_FAIL(modify_schedule_job_(tenant_id, "max_run_duration", max_run_duration))) {
    LOG_WARN("modify_schedule_job_ attr max_run_duration SCHEDULED_TRIGGER_DUMP_DATA_DICT failed",
        KR(ret), K(tenant_id), K(params), K(max_run_duration));
  }

  if (OB_SUCC(ret)) {
    LOG_INFO("modify dump duration succ", KR(ret), K(params));
  } else {
    LOG_WARN("modify dump duration failed", KR(ret), K(params));
  }

  return ret;
}

int ObDBMSDataDict::modify_retention(
    sql::ObExecContext &ctx,
    sql::ParamStore &params,
    common::ObObj &result)
{
  int ret = OB_SUCCESS;
  UNUSED(result);
  const static int64_t SECONDS_OF_DAY = 24 * 3600;
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  ObSqlString job_action_str;
  ObObj job_action;
  share::SCN base_scn;
  bool is_scheduled_job_enabled = false;
  ObTriggerDumpDataDictArg trigger_dump_data_dict_arg;
  int64_t data_dict_dump_history_retention_sec = OB_INVALID_TIMESTAMP;
  int32_t data_dict_dump_history_retention_day = -1;
  int64_t errsim_code = EventTable::EN_DICT_MODIFY_ITEM_RETENION;

  if (OB_FAIL(check_op_allowed_(ctx, tenant_id))) {
    LOG_WARN("check_op_allowed failed", KR(ret));
  } else if (OB_UNLIKELY(1 != params.count()
      || params.at(0).is_null()
      || !params.at(0).is_integer_type()
      || params.at(0).get_int() < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid params count for modify_retention", KR(ret), K(params));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "DBMS_DATA_DICT.MODIFY_DICT_ITEM_RETENTION parameter");
  } else if (OB_FAIL(params.at(0).get_int32(data_dict_dump_history_retention_day)) || data_dict_dump_history_retention_day < 0) {
    LOG_WARN("invalid data_dict_dump_history_retention_day", K(data_dict_dump_history_retention_day));
  } else if (OB_FAIL(check_scheduled_job_enabled_(tenant_id, is_scheduled_job_enabled))) {
    LOG_WARN("check_scheduled_job_enabled_ failed", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(! is_scheduled_job_enabled)) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("scheduled job is not enabled, can not modify retention, please call DBMS_DATA_DICT.ENABLE_DUMP() then retry",
        KR(ret), K(tenant_id));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "DBMS_SCHEDULER JOB NOT ENABLE, please call DBMS_DATA_DICT.ENABLE() otherwise MODIFY_DICT_ITEM_RETENTION is");
  } else if (OB_FAIL(job_action_str.assign_fmt("DBMS_DATA_DICT.TRIGGER_DUMP(%d)", data_dict_dump_history_retention_day))) {
    LOG_WARN("build job_action failed", KR(ret), K(tenant_id), K(job_action_str));
  } else if (FALSE_IT(job_action.set_varchar(job_action_str.string()))) {
  } else if (OB_FAIL(modify_schedule_job_(tenant_id, "job_action", job_action))) {
    LOG_WARN("modify_schedule_job_ attr job_action SCHEDULED_TRIGGER_DUMP_DATA_DICT failed",
        KR(ret), K(tenant_id), K(params), K(job_action));
  } else if (FALSE_IT(data_dict_dump_history_retention_sec = data_dict_dump_history_retention_day * SECONDS_OF_DAY)) {
  } else if (OB_UNLIKELY(errsim_code != OB_SUCCESS) // errsim mode for test
      && FALSE_IT(data_dict_dump_history_retention_sec = data_dict_dump_history_retention_day)) {
  } else if (OB_FAIL(get_tenant_gts_(tenant_id, base_scn))) {
    LOG_WARN("get_tenant_gts_ failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(trigger_dump_data_dict_arg.init(base_scn, data_dict_dump_history_retention_sec))) {
    LOG_WARN("init trigger_dump_data_dict_arg failed", KR(ret), K(tenant_id), K(trigger_dump_data_dict_arg),
        K(base_scn), K(data_dict_dump_history_retention_sec));
  } else if (OB_FAIL(send_trigger_dump_dict_rpc_(tenant_id, trigger_dump_data_dict_arg))) {
    LOG_WARN("send_trigger_dump_dict_rpc_ failed", KR(ret), K(tenant_id), K(trigger_dump_data_dict_arg));
  }

  if (OB_SUCC(ret)) {
    LOG_INFO("modify dict_item retention succ", KR(ret), K(errsim_code), K(params));
  } else {
    LOG_WARN("modify dict_item retention failed", KR(ret), K(errsim_code), K(params));
  }

  return ret;
}

int ObDBMSDataDict::check_op_allowed_(const sql::ObExecContext &ctx, uint64_t &tenant_id)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  tenant_id = OB_INVALID_TENANT_ID;

  if (OB_ISNULL(ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null session info", KR(ret));
  } else if (FALSE_IT(tenant_id = ctx.get_my_session()->get_effective_tenant_id())) {
  } else if (OB_UNLIKELY(!is_user_tenant(tenant_id))) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("not user tenant, trigger_dump_data_dict is not allowed", KR(ret), K(tenant_id));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "not user tenant, trigger_dump_data_dict is");
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("fail to get tenant data version", KR(ret), K(tenant_id), K(data_version));
  } else if (OB_UNLIKELY(data_version < MOCK_DATA_VERSION_4_2_5_1 || (data_version >= DATA_VERSION_4_3_0_0 && data_version < DATA_VERSION_4_4_1_0))) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support to trigger partition balance", KR(ret), K(tenant_id), K(data_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "trigger_dump_data_dict on version below 4.2.5.1 or between 4.3.0 and 4.4.1(not included)");
  }

  return ret;
}

int ObDBMSDataDict::check_scheduled_job_enabled_(const uint64_t tenant_id, bool &is_enabled)
{
  int ret = OB_SUCCESS;
  common::ObISQLClient *sql_client = nullptr;
  const bool is_oracle_mode = lib::is_oracle_mode();
  ObArenaAllocator allocator(ObMemAttr(tenant_id, "dbms_data_dict"));
  dbms_scheduler::ObDBMSSchedJobInfo job_info;
  is_enabled = false;

  if (OB_ISNULL(sql_client = GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy is null", KR(ret));
  } else if (OB_FAIL(dbms_scheduler::ObDBMSSchedJobUtils::get_dbms_sched_job_info(
      *sql_client,
      tenant_id,
      is_oracle_mode,
      DATA_DICT_SCHEDULED_JOB_NAME,
      allocator,
      job_info))) {
        LOG_WARN("get_dbms_sched_job_info failed", KR(ret), K(tenant_id));
  } else {
    is_enabled = !job_info.is_disabled();
  }

  return ret;
}

int ObDBMSDataDict::get_tenant_gts_(const uint64_t tenant_id, share::SCN &gts)
{
  int ret = OB_SUCCESS;
  const transaction::MonotonicTs stc = transaction::MonotonicTs::current_time();
  transaction::MonotonicTs tmp_receive_gts_ts(0);
  int64_t retry_cnt = 0;
  const static int64_t GET_GTS_TIMEOUT = 10_s;
  const static int64_t PRINT_INTERVAL = 10;
  const int64_t timeout_ts = ObTimeUtility::current_time() + GET_GTS_TIMEOUT;

  do{
    if (ObTimeUtility::current_time() > timeout_ts) {
      ret = OB_TIMEOUT;
      LOG_WARN("get_tenant_gts timeout, wait retry by dbms_scheduler or user_manual", KR(ret), K(timeout_ts));
    } else if (OB_FAIL(OB_TS_MGR.get_gts(tenant_id, stc, NULL, gts, tmp_receive_gts_ts))) {
      if (OB_EAGAIN == ret) {
        ob_usleep(100_ms);
        if (++retry_cnt % PRINT_INTERVAL == 0) {
          LOG_WARN("retry get_gts for data_dict_service failed", KR(ret), K(retry_cnt));
        }
      } else {
        LOG_WARN("get_gts for data_dict_service failed", KR(ret));
      }
    } else if (OB_UNLIKELY(!gts.is_valid_and_not_min())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("gts invalid", KR(ret), K(tenant_id), K(gts));
    } else {
      LOG_TRACE("get gts", K(tenant_id), K(gts));
    }
  } while (OB_EAGAIN == ret);
  return ret;
}

int ObDBMSDataDict::send_trigger_dump_dict_rpc_(const uint64_t tenant_id, const obrpc::ObTriggerDumpDataDictArg &trigger_dump_data_dict_arg)
{
  int ret = OB_SUCCESS;
  ObAddr leader;
  static const int64_t DICT_RPC_TIMEOUT = 10_s;

  if (OB_UNLIKELY(! is_user_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else {
    const int64_t timeout_ts = ObTimeUtility::current_time() + DICT_RPC_TIMEOUT;
    do {
      leader.reset();
      // get ls_leader of SYS_LS
      if (OB_NOT_MASTER == ret || OB_EAGAIN == ret || OB_NEED_RETRY == ret) {
        ret = OB_SUCCESS;
        ob_usleep(1_s);
      }
      if (ObTimeUtility::current_time() > timeout_ts) {
        ret = OB_TIMEOUT;
        LOG_WARN("send_trigger_dump_dict_rpc_ timeout, wait retry by dbms_scheduler or user_manual", KR(ret), K(timeout_ts));
      } else if (OB_ISNULL(GCTX.location_service_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("location_service_ is invalid", KR(ret));
      } else if (OB_ISNULL(GCTX.srv_rpc_proxy_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("srv_rpc_proxy is invalid", KR(ret));
      } else if (OB_FAIL(GCTX.location_service_->get_leader_with_retry_until_timeout(
          GCONF.cluster_id,
          tenant_id,
          SYS_LS,
          leader))) { // default 1s timeout
        LOG_WARN("get sys_ls leader failed", KR(ret), "cluster_id", GCONF.cluster_id.get_value(),
            K(tenant_id), K(leader));
      } else if (OB_FAIL(GCTX.srv_rpc_proxy_->to(leader)
                                              .by(tenant_id)
                                              .timeout(DICT_RPC_TIMEOUT)
                                              .trigger_dump_data_dict(trigger_dump_data_dict_arg))) {
        LOG_WARN("trigger_dump_data_dict rpc failed", KR(ret), K(tenant_id), K(trigger_dump_data_dict_arg));
      } else {
        LOG_TRACE("trigger_dump_data_dict rpc success", KR(ret), K(tenant_id), K(leader), K(trigger_dump_data_dict_arg));
      }
    } while (OB_NOT_MASTER == ret || OB_EAGAIN == ret || OB_NEED_RETRY == ret);
  }
  return ret;
}

int ObDBMSDataDict::modify_schedule_job_(
    const uint64_t tenant_id,
    const ObString &job_attribute_name,
    const ObObj &job_attribute_value)
{
  int ret = OB_SUCCESS;
  common::ObISQLClient *sql_client = nullptr;
  const bool is_oracle_mode = lib::is_oracle_mode();
  ObArenaAllocator allocator(ObMemAttr(tenant_id, "dbms_data_dict"));
  dbms_scheduler::ObDBMSSchedJobInfo job_info;

  if (OB_ISNULL(sql_client = GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy is null", KR(ret));
  } else if (OB_FAIL(dbms_scheduler::ObDBMSSchedJobUtils::get_dbms_sched_job_info(
    *sql_client,
    tenant_id,
    is_oracle_mode,
    DATA_DICT_SCHEDULED_JOB_NAME,
    allocator,
    job_info))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      LOG_WARN("get_dbms_sched_job_info not exist", KR(ret), K(tenant_id));
    } else {
      LOG_WARN("get_dbms_sched_job_info failed", KR(ret), K(tenant_id));
    }
  } else if (OB_FAIL(dbms_scheduler::ObDBMSSchedJobUtils::update_dbms_sched_job_info(
      *sql_client,
      job_info,
      job_attribute_name,
      job_attribute_value))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      LOG_INFO("schedule job didn't change, just skip", K(tenant_id), K(job_attribute_name), K(job_attribute_value));
    } else {
      LOG_WARN("update_dbms_sched_job_info failed", KR(ret),
          KCSTRING(DATA_DICT_SCHEDULED_JOB_NAME), K(tenant_id), K(job_attribute_name), K(job_attribute_value));
    }
  } else {
    LOG_INFO("modify schedule job succ", K(tenant_id),
        KCSTRING(DATA_DICT_SCHEDULED_JOB_NAME), K(job_attribute_name), K(job_attribute_value));
  }

  if (OB_ENTRY_NOT_EXIST == ret) {
    ret = OB_SUCCESS;
  }

  return ret;
}

} // end of pl
} // end oceanbase
