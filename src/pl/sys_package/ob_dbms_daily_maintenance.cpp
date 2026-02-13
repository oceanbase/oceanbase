/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX PL
#include "pl/sys_package/ob_dbms_daily_maintenance.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "share/compaction/ob_schedule_daily_maintenance_window.h"
#include "rootserver/freeze/window/ob_window_compaction_helper.h"
#include "share/ob_cluster_version.h"

namespace oceanbase
{
namespace pl
{

int ObDBMSDailyMaintenance::trigger_window_compaction_proc(sql::ObExecContext &ctx,
                                                           sql::ParamStore &params,
                                                           common::ObObj &result)
{
  int ret = OB_SUCCESS;
  UNUSED(result);
  const uint64_t tenant_id = MTL_ID();
  const int64_t trigger_start_time_us = ObTimeUtility::current_time();
  bool is_daily_maintenance = false;
  int64_t window_start_time_us = 0;
  FLOG_INFO("[WIN-COMPACTION] start to trigger window compaction proc", K(trigger_start_time_us), K(params));

  if (OB_UNLIKELY(!ctx.is_valid() || params.count() != 1 || !params.at(0).is_tinyint())) { // tinyint type is boolean in mysql mode
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ctx), K(params));
  } else if (OB_FAIL(check_supported(tenant_id))) {
    LOG_WARN("fail to check window compaction compatbility", K(ret), K(tenant_id));
  } else if (OB_FAIL(params.at(0).get_bool(is_daily_maintenance))) {
    LOG_WARN("failed to get param", KR(ret), K(params.at(0)));
  } else {
    int64_t retry_sleep_time_duration = BASE_RETRY_SLEEP_DURATION_US;
    rootserver::ObWindowCompactionParam param(true /*with_start_ts*/, window_start_time_us);
    do {
      ret = OB_SUCCESS;
      // Reload tenant config and job info every time to catch up the latest config
      omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
      dbms_scheduler::ObDBMSSchedJobInfo job_info;
      int64_t retry_timeout = DEFAULT_RETRY_TIMEOUT_US;
      if (!tenant_config.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get tenant config", KR(ret));
      } else if (!tenant_config->enable_window_compaction) {
        ret = OB_MAJOR_FREEZE_NOT_ALLOW;
        LOG_WARN("window compaction is not allowed", KR(ret), K(tenant_id));
        LOG_USER_ERROR(OB_MAJOR_FREEZE_NOT_ALLOW, "window compaction is not allowed, please check parameter enable_window_compaction");
      } else if (OB_FAIL(ObScheduleDailyMaintenanceWindow::get_daily_maintenance_window_job_info(ctx, job_info))) {
        LOG_WARN("failed to get daily maintenance window job info", KR(ret));
      } else if (job_info.is_disabled()) {
        ret = OB_MAJOR_FREEZE_NOT_ALLOW;
        LOG_WARN("daily maintenance window job is disabled", KR(ret), K(tenant_id), K(job_info));
        LOG_USER_ERROR(OB_MAJOR_FREEZE_NOT_ALLOW, "daily maintenance window job is disabled, please enable DAILY_MAINTENANCE_WINDOW firstly");
      } else if (is_daily_maintenance) {
        // trigger by DAILY_MAINTENANCE_WINDOW
        window_start_time_us = job_info.start_date_;
      } else {
        window_start_time_us = trigger_start_time_us;
      }

      if (OB_FAIL(ret)) {
      } else if (FALSE_IT(param.window_start_time_us_ = window_start_time_us)) {
      } else if (FALSE_IT(retry_timeout = MIN(retry_timeout, job_info.max_run_duration_ * USES_PER_SEC))) {
      } else if (ObTimeUtility::current_time() - window_start_time_us >= retry_timeout) {
        ret = OB_TIMEOUT;
        LOG_WARN("window compaction duration timeout, no need to retry again", K(tenant_id), K(window_start_time_us), K(retry_timeout), K(job_info));
        LOG_USER_ERROR(OB_TIMEOUT, retry_timeout);
      } else if (OB_FAIL(rootserver::ObWindowCompactionHelper::trigger_window_compaction(tenant_id, param))) {
        LOG_WARN("failed to trigger window compaction", KR(ret), K(tenant_id), K(param));
      } else {
        LOG_INFO("succeed to trigger window compaction", K(tenant_id), K(param), K(window_start_time_us), K(job_info), K(is_daily_maintenance));
        SERVER_EVENT_SYNC_ADD("window_compaction", "trigger_window_compaction_proc", "tenant_id", tenant_id, "window_start_time_us", window_start_time_us, "is_daily_maintenance", is_daily_maintenance);
      }

      if (OB_EAGAIN == ret) {
        USLEEP(retry_sleep_time_duration);
        retry_sleep_time_duration = MIN(retry_sleep_time_duration * 2, MAX_RETRY_SLEEP_DURATION_US);
      }
    } while (OB_EAGAIN == ret);
  }
  const int64_t cost_time = ObTimeUtility::current_time() - trigger_start_time_us;
  FLOG_INFO("[WIN-COMPACTION] end to trigger window compaction proc", KR(ret), K(is_daily_maintenance), K(trigger_start_time_us), K(window_start_time_us), K(cost_time));
  return ret;
}

int ObDBMSDailyMaintenance::set_thread_count(sql::ObExecContext &ctx,
                                             sql::ParamStore &params,
                                             common::ObObj &result)
{
  int ret = OB_SUCCESS;
  UNUSED(result);
  const int64_t start_time_us = ObTimeUtility::current_time();
  uint64_t tenant_id = MTL_ID();
  number::ObNumber num_thread_cnt;
  int64_t thread_cnt = -1;
  if (OB_FAIL(check_supported(tenant_id))) {
    LOG_WARN("fail to check window compaction compatbility", K(ret), K(tenant_id));
  } else if (OB_UNLIKELY(!ctx.is_valid() || 1 != params.count() || params.at(0).is_null())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ctx), K(params));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "thread_count should be between 0 to 100, 0 means default thread count");
  } else if (lib::is_oracle_mode() && OB_FAIL(params.at(0).get_number(num_thread_cnt))) {
    LOG_WARN("failed to get number", KR(ret), K(params.at(0)));
  } else if (lib::is_oracle_mode() && OB_FAIL(num_thread_cnt.extract_valid_int64_with_trunc(thread_cnt))) {
    LOG_WARN("failed to extract valid int64 with trunc", KR(ret), K(num_thread_cnt));
  } else if (lib::is_mysql_mode() && FALSE_IT(thread_cnt = params.at(0).get_int())) {
  } else if (thread_cnt < 0 || thread_cnt > ObDailyWindowJobConfig::MAX_THREAD_CNT) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ctx), K(params));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "thread_count should be between 0 to 100, 0 means default thread count");
  } else if (OB_FAIL(ObScheduleDailyMaintenanceWindow::set_thread_count(ctx, tenant_id, thread_cnt))) {
    LOG_WARN("failed to set thread count", KR(ret), K(tenant_id), K(thread_cnt));
  }
  const int64_t cost_time = ObTimeUtility::current_time() - start_time_us;
  FLOG_INFO("[WIN-COMPACTION] end to set window thread count", KR(ret), K(thread_cnt), K(start_time_us), K(cost_time));
  return ret;
}

int ObDBMSDailyMaintenance::check_supported(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("fail to get data version", K(ret), K(tenant_id));
  } else if (data_version < DATA_VERSION_4_5_1_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("window compaction is not supported now, please upgrade observer first", KR(ret), K(tenant_id), K(data_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "window compaction in version < 4.5.1.0");
  } else if (!is_user_tenant(tenant_id)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("window compaction only supported in user tenant", KR(ret), K(tenant_id));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "window compaction in non-user tenant");
  }
  return ret;
}

} // end of pl
} // end of oceanbase