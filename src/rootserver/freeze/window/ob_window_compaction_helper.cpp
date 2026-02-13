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

#define USING_LOG_PREFIX RS_COMPACTION

#include "rootserver/freeze/window/ob_window_compaction_helper.h"
#include "rootserver/freeze/ob_major_freeze_service.h"
#include "share/compaction/ob_schedule_daily_maintenance_window.h"
#include "share/compaction/ob_compaction_resource_manager.h"
#include "share/location_cache/ob_location_service.h"
#include "share/ob_global_merge_table_operator.h"
#include "observer/ob_srv_network_frame.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/compaction/ob_tenant_status_cache.h"


namespace oceanbase
{
namespace rootserver
{

/*-------------------------------- ObWindowParameters --------------------------------*/
ObWindowParameters::ObWindowParameters()
  : tenant_id_(OB_INVALID_TENANT_ID),
    enable_window_compaction_(false),
    window_duration_us_(0),
    window_start_time_us_(0),
    merge_status_(ObZoneMergeInfo::MERGE_STATUS_MAX),
    merge_mode_(ObGlobalMergeInfo::MERGE_MODE_MAX)
{}

void ObWindowParameters::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  merge_status_ = ObZoneMergeInfo::MERGE_STATUS_MAX;
  merge_mode_ = ObGlobalMergeInfo::MERGE_MODE_MAX;
  window_start_time_us_ = 0;
  window_duration_us_ = 0;
  enable_window_compaction_ = false;
}

int ObWindowParameters::init(
  const uint64_t tenant_id,
  const ObGlobalMergeInfo &global_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!global_info.is_valid() || global_info.tenant_id_ != tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(global_info));
  } else if (FALSE_IT(tenant_id_ = tenant_id)) {
  } else if (OB_FAIL(refresh_enable_window_compaction())) {
    LOG_WARN("fail to refresh enable window compaction", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(refresh_window_duration_us())) {
    LOG_WARN("fail to refresh window duration us", KR(ret), K_(tenant_id));
  } else {
    window_start_time_us_ = global_info.merge_start_time();
    merge_status_ = static_cast<ObZoneMergeInfo::MergeStatus>(global_info.merge_status_.get_value());
    merge_mode_ = static_cast<ObGlobalMergeInfo::MergeMode>(global_info.merge_mode_.get_value());
    is_inited_ = true;
  }
  return ret;
}

int ObWindowParameters::refresh_enable_window_compaction()
{
  int ret = OB_SUCCESS;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
  if (tenant_config.is_valid()) {
    enable_window_compaction_ = tenant_config->enable_window_compaction;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tenant config", KR(ret), K_(tenant_id));
  }
  return ret;
}

int ObWindowParameters::refresh_window_duration_us()
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_arena("ReadDailyJob", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id_);
  dbms_scheduler::ObDBMSSchedJobInfo job_info;
  int64_t duration_us = 0;
  if (OB_FAIL(ObScheduleDailyMaintenanceWindow::check_supported(tenant_id_))) {
    LOG_WARN("check window compaction supported failed", KR(ret), K_(tenant_id));
  } else if (!enable_window_compaction_) {
    // window compaction is not enabled, do nothing
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is nullptr", KR(ret));
  } else if (OB_FAIL(ObScheduleDailyMaintenanceWindow::get_daily_maintenance_window_job_info(*GCTX.sql_proxy_, tenant_id_, tmp_arena, job_info))) {
    LOG_WARN("failed to get daily maintenance window job info", KR(ret), K_(tenant_id));
  } else if (job_info.is_disabled()) {
    window_duration_us_ = 0;
  } else if (FALSE_IT(duration_us = job_info.get_max_run_duration() * 1000 * 1000L)) {
  } else if (duration_us < 0 || duration_us > FULL_DAY_US) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid window duration, shoud be [0, 24h]", K(ret), K(duration_us));
  } else {
    window_duration_us_ = duration_us;
  }
  return ret;
}

int ObWindowParameters::check_need_do_window_compaction(bool &need_do_window_compaction) const
{
  int ret = OB_SUCCESS;
  need_do_window_compaction = false;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("window parameters is not inited", K(ret), KPC(this));
  } else if (is_window_compaction_stopped()) {
  } else if (is_idle_merge_status()) {
    if (is_window_compaction_forever()) {
      // need do window compaction and alter merge mode to window, and merge status to merging
      need_do_window_compaction = true;
      LOG_INFO("[WIN-COMPACTION] window duration is set 24h, need do window compaction all the time", K_(window_duration_us));
    }
  } else if (is_tenant_merge_mode()) {
    if (is_merging_merge_status() || is_merge_verifying()) {
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid merge status for tenant major merge", K(ret), K_(merge_status));
    }
  } else if (is_window_merge_mode()) {
    if (is_merging_merge_status()) {
      // sys ls leader check and alter __all_merge_info, other observer follow the status of __all_merge_info
      const int64_t current_time_us = ObTimeUtility::current_time_us();
      if (is_window_compaction_forever()) {
        need_do_window_compaction = true;
        LOG_INFO("[WIN-COMPACTION] window duration is set 24h, need do window compaction all the time", K_(window_duration_us));
      } else if (current_time_us >= window_start_time_us_ && current_time_us < (window_start_time_us_ + window_duration_us_)) {
        need_do_window_compaction = true;
      } else {
        LOG_INFO("[WIN-COMPACTION] window compaction time is up, need to finish", K_(window_start_time_us), K_(window_duration_us), K(current_time_us));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid merge status for tenant major merge", K(ret), K_(merge_status));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid merge mode", K(ret), K_(merge_mode));
  }
  return ret;
}

/*-------------------------------- ObWindowResourceCache --------------------------------*/
ObWindowResourceCache::ObWindowResourceCache():
  merge_start_time_us_(-1),
  finish_switch_to_window_(false),
  finish_switch_to_normal_(false)
{}

void ObWindowResourceCache::reset()
{
  merge_start_time_us_ = -1;
  finish_switch_to_window_ = false;
  finish_switch_to_normal_ = false;
}

bool ObWindowResourceCache::check_could_skip(const int64_t merge_start_time_us, const bool to_window) const
{
  bool bret = false;
  if (merge_start_time_us < 0 || merge_start_time_us_ < 0) {
  } else if (merge_start_time_us_ != merge_start_time_us) {
  } else if (to_window && finish_switch_to_window_) {
    bret = true;
  } else if (!to_window && finish_switch_to_normal_) {
    bret = true;
  }
  return bret;
}

void ObWindowResourceCache::update(const int64_t merge_start_time_us, const bool to_window)
{
  finish_switch_to_window_ = to_window;
  finish_switch_to_normal_ = !to_window;
  merge_start_time_us_ = merge_start_time_us;
}

/*-------------------------------- ObWindowCompactionHelper --------------------------------*/
int ObWindowCompactionHelper::check_window_compaction_could_start(
  const uint64_t tenant_id,
  const ObGlobalMergeInfo &global_info,
  bool &could_start_window_compaction)
{
  int ret = OB_SUCCESS;
  could_start_window_compaction = false;
  compaction::ObTenantStatusCache tenant_status;
  if (OB_UNLIKELY(!global_info.is_valid() || global_info.tenant_id_ != tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(global_info));
  } else if (global_info.suspend_merging_.get_value()) { // window compaction is not supported when suspend merging
  } else if (global_info.frozen_scn() > global_info.last_merged_scn()) { // tenant major merge is not finished
  } else if (OB_FAIL(tenant_status.init_or_refresh())) {
    LOG_WARN("fail to init tenant status", KR(ret), K(tenant_id));
  } else if (tenant_status.is_skip_window_compaction_tenant()) {
  } else {
    could_start_window_compaction = true;
  }
  LOG_TRACE("[WIN-COMPACTION] check window compaction could start", KR(ret), K(tenant_id), K(global_info), K(could_start_window_compaction), K(tenant_status));
  return ret;
}

int ObWindowCompactionHelper::check_and_alter_window_status_for_leader(
    const uint64_t tenant_id,
    const int64_t current_epoch,
    const share::ObGlobalMergeInfo &global_info,
    ObWindowResourceCache &resource_cache)
{
  int ret = OB_SUCCESS;
  bool could_start_window_compaction = false;
  bool need_do_window_compaction = false;
  bool is_switched = false;
  const int64_t merge_start_time_us = global_info.merge_start_time();
  ObWindowParameters params;
  if (current_epoch < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid current epoch", KR(ret), K(current_epoch));
  } else if (OB_FAIL(ObScheduleDailyMaintenanceWindow::check_supported(tenant_id))) {
    LOG_WARN("check window compaction compatbility failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(check_window_compaction_could_start(tenant_id, global_info, could_start_window_compaction))) {
    LOG_WARN("fail to check window compaction could start", KR(ret), K(tenant_id), K(global_info));
  } else if (!could_start_window_compaction) {
  } else if (OB_FAIL(params.init(tenant_id, global_info))) {
    LOG_WARN("fail to init params", KR(ret), K(tenant_id), K(global_info));
  } else if (OB_UNLIKELY(!params.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid window params", KR(ret), K(params));
  } else if (OB_FAIL(params.check_need_do_window_compaction(need_do_window_compaction))) {
    LOG_WARN("fail to check need do window compaction", KR(ret), K(params));
  }

  if (OB_FAIL(ret)) {
  } else if (need_do_window_compaction) {
    ObCompactionResourceManager rsrc_mgr;
    // 1. check current merge status and merge mode, if not match, need alter __all_merge_info
    if (!params.is_global_during_window_compaction()) {
      rootserver::ObWindowCompactionParam param(false /*with_start_ts*/, -1 /*meaningless*/);
      if (OB_FAIL(rootserver::ObWindowCompactionHelper::trigger_window_compaction(tenant_id, param))) {
        LOG_WARN("failed to trigger window compaction", K(ret), K(tenant_id), K(param));
      }
    }
    // 2. check current resource plan, if not match, need alter to window compaction plan
    if (FAILEDx(rsrc_mgr.check_and_switch(true /*to_window*/, merge_start_time_us, resource_cache, is_switched))) {
      LOG_WARN("failed to check and switch window compaction plan", K(ret), K(merge_start_time_us), K(resource_cache));
    }
  } else {
    ObCompactionResourceManager rsrc_mgr;
    // 1. check current resource plan, if it is window compaction plan, need alter merge_plan to normal plan
    if (OB_FAIL(rsrc_mgr.check_and_switch(false /*to_window*/, merge_start_time_us, resource_cache, is_switched))) {
      LOG_WARN("failed to check and switch normal compaction plan", K(ret), K(merge_start_time_us), K(resource_cache));
    }
    // 2. alter __all_merge_info to window merge mode and idle status
    if (OB_SUCC(ret) && params.is_window_merge_mode() && !params.is_idle_merge_status()) {
      if (OB_FAIL(rootserver::ObWindowCompactionHelper::finish_window_compaction(tenant_id))) {
        LOG_WARN("failed to finish window compaction", K(ret), K(tenant_id));
      }
    }
  }
  LOG_INFO("[WIN-COMPACTION] Finish check and alter window status for leader", KR(ret), K(tenant_id), K(current_epoch), K(merge_start_time_us), K(resource_cache),
           K(global_info), K(could_start_window_compaction), K(need_do_window_compaction), K(is_switched), K(params));
  return ret;
}

int ObWindowCompactionHelper::clean_before_major_merge(
    const int64_t merge_start_time_us,
    ObWindowResourceCache &resource_cache)
{
  int ret = OB_SUCCESS;
  ObCompactionResourceManager rsrc_mgr;
  if (OB_FAIL(rsrc_mgr.switch_to_normal_before_major_merge(merge_start_time_us, resource_cache))) {
    LOG_WARN("failed to switch to normal before major merge", KR(ret), K(merge_start_time_us), K(resource_cache));
  }
  return ret;
}

int ObWindowCompactionHelper::trigger_window_compaction(
    const uint64_t tenant_id,
    const ObWindowCompactionParam &param)
{
  int ret = OB_SUCCESS;
  const int64_t trigger_start_time = ObTimeUtility::current_time();
  obrpc::ObWindowCompactionRpcProxy proxy;
  ObAddr leader;
  obrpc::ObWindowCompactionRequest req;
  obrpc::ObWindowCompactionResponse resp;
  rpc::frame::ObReqTransport *transport = nullptr;

  if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(param));
  } else if (OB_FAIL(ObScheduleDailyMaintenanceWindow::check_supported(tenant_id))) {
    LOG_WARN("check window compaction compatbility failed", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(GCTX.net_frame_) || OB_ISNULL(GCTX.location_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid GCTX", KR(ret));
  } else if (OB_FAIL(req.init(tenant_id, param))) {
    LOG_WARN("fail to init request", KR(ret), K(tenant_id), K(param));
  } else if (OB_ISNULL(transport = GCTX.net_frame_->get_req_transport())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid transport", KR(ret));
  } else if (OB_FAIL(proxy.init(transport))) {
    LOG_WARN("fail to init proxy", KR(ret));
  } else {
    bool window_compaction_done = false;
    const int64_t timeout_us = MAX(GCONF.rpc_timeout * 5, MAX_PROCESS_TIME_US); // timeout >= 10s
    for (int64_t i = 0; OB_SUCC(ret) && !window_compaction_done && i < MAX_RPC_RETRY_COUNT; ++i) {
      if (OB_FAIL(GCTX.location_service_->get_leader_with_retry_until_timeout(GCONF.cluster_id, tenant_id, share::SYS_LS, leader))) {
        LOG_WARN("fail to get ls locaiton leader", KR(ret), K(tenant_id));
      } else if (OB_FAIL(proxy.to(leader)
                                .trace_time(true)
                                .timeout(timeout_us)
                                .by(tenant_id)
                                .dst_cluster_id(GCONF.cluster_id)
                                .window_compaction(req, resp))) {
        LOG_WARN("tenant window compaction rpc failed", KR(ret), K(tenant_id), K(leader), K(req), K(timeout_us));
      } else if (FALSE_IT(ret = resp.err_code_)) {
      } else if (OB_FAIL(ret)) {
        if (OB_LEADER_NOT_EXIST == ret || OB_EAGAIN == ret) {
          const int64_t RESERVED_TIME_US = 600 * 1000; // 600 ms
          const int64_t timeout_remain_us = THIS_WORKER.get_timeout_remain();
          const int64_t idle_time_us = 200 * 1000 * (i + 1);
          if (timeout_remain_us - idle_time_us > RESERVED_TIME_US) {
            LOG_WARN("leader may switch or ddl conflict, will retry", KR(ret), K(tenant_id), K(req),
              "ori_leader", leader, K(timeout_remain_us), K(idle_time_us), K(RESERVED_TIME_US));
            USLEEP(idle_time_us);
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("leader may switch or ddl conflict, will not retry cuz timeout_remain is not enough",
              KR(ret), K(tenant_id), K(req), "ori_leader", leader, K(timeout_remain_us), K(idle_time_us), K(RESERVED_TIME_US));
          }
        } else {
          LOG_WARN("fail to window compaction", KR(ret), K(tenant_id), K(leader), K(req));
        }
      } else {
        window_compaction_done = true;
      }
    }
    if (OB_SUCC(ret) && !window_compaction_done) {
      ret = OB_EAGAIN;
      LOG_WARN("fail to retry window compaction while switching role", KR(ret), K(MAX_RPC_RETRY_COUNT));
    }
  }

  const int64_t launch_cost_time = ObTimeUtility::current_time() - trigger_start_time;
  LOG_INFO("trigger window compaction", KR(ret), K(tenant_id), K(param), K(launch_cost_time));
  return ret;
}

int ObWindowCompactionHelper::finish_window_compaction(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  // don't need to send RPC, this function could only called by sys ls leader
  ObPrimaryMajorFreezeService *primary_major_freeze_service = nullptr;
  ObRestoreMajorFreezeService *restore_major_freeze_service = nullptr;
  ObMajorFreezeService *major_freeze_service = nullptr;
  bool is_primary_service = true;
  if (OB_FAIL(ObScheduleDailyMaintenanceWindow::check_supported(tenant_id))) {
    LOG_WARN("check window compaction compatbility failed", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(primary_major_freeze_service = MTL(ObPrimaryMajorFreezeService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("primary major freeze service is null", KR(ret));
  } else if (OB_ISNULL(restore_major_freeze_service = MTL(ObRestoreMajorFreezeService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("restore major freeze service is null", KR(ret));
  } else if (OB_FAIL(ObMajorFreezeUtil::get_major_freeze_service(primary_major_freeze_service,
                                                                 restore_major_freeze_service,
                                                                 major_freeze_service,
                                                                 is_primary_service))) {
    LOG_WARN("fail to get major freeze service", KR(ret));
  } else if (OB_UNLIKELY(major_freeze_service->get_tenant_id() != tenant_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant id does not match", KR(ret), K(tenant_id), "service_id", major_freeze_service->get_tenant_id());
  } else if (OB_FAIL(major_freeze_service->finish_window_compaction())) {
    LOG_WARN("fail to finish window compaction", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObWindowCompactionHelper::check_window_compaction_global_active(
    const uint64_t tenant_id,
    bool &is_active)
{
  int ret = OB_SUCCESS;
  ObGlobalMergeInfo global_merge_info;
  bool enable_window_compaction = false;
  is_active = false;
  if (!is_user_tenant(tenant_id)) {
    // do nothing
  } else {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
    if (tenant_config.is_valid()) {
      enable_window_compaction = tenant_config->enable_window_compaction;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid tenant config", KR(ret), K(tenant_id));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (!enable_window_compaction) {
  } else if (OB_FAIL(ObGlobalMergeTableOperator::load_global_merge_info(*GCTX.sql_proxy_, tenant_id, global_merge_info))) {
    LOG_WARN("failed to load global merge info", KR(ret), K(tenant_id));
  } else if (ObGlobalMergeInfo::MERGE_MODE_WINDOW == global_merge_info.merge_mode_.get_value()
         && ObZoneMergeInfo::MERGE_STATUS_MERGING == global_merge_info.merge_status_.get_value()) {
    is_active = true;
  }
  return ret;
}

} // end namespace rootserver
} // end namespace oceanbase
