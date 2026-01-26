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

#define USING_LOG_PREFIX STORAGE
#include "ob_compaction_resource_manager.h"
#include "rootserver/ob_rs_event_history_table_operator.h"
#include "rootserver/freeze/window/ob_window_compaction_helper.h"
#include "share/rc/ob_tenant_base.h"
#include "share/schema/ob_schema_utils.h"

namespace oceanbase
{
namespace share
{

const ObString ObCompactionResourceManager::window_plan_(INNER_DAILY_WINDOW_PLAN_NAME);
const ObString ObCompactionResourceManager::window_consumer_group_(INNER_DAILY_WINDOW_COMPACTION_LOW_GROUP_NAME);
const ObString ObCompactionResourceManager::attr_name_("FUNCTION");
const ObString ObCompactionResourceManager::attr_value_("COMPACTION_LOW");

ObCompactionResourceManager::ObCompactionResourceManager()
  : tenant_id_(MTL_ID()),
    allocator_(ObMemAttr(tenant_id_, "ObComRsrcMgr")),
    proxy_()
{
}

int ObCompactionResourceManager::check_and_switch(
    const bool to_window,
    const int64_t merge_start_time_us,
    rootserver::ObWindowResourceCache &resource_cache,
    bool &is_switched)
{
  int ret = OB_SUCCESS;
  ObSqlString current_plan;
  is_switched = false;
  if (OB_UNLIKELY(merge_start_time_us < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid merge start time us", KR(ret), K(merge_start_time_us));
  } else if (OB_FAIL(ObScheduleDailyMaintenanceWindow::check_supported(tenant_id_))) {
    LOG_WARN("check window compaction compatbility failed", KR(ret));
  } else if (resource_cache.check_could_skip(merge_start_time_us, to_window)) {
    LOG_TRACE("resource manager plan has already switched", K(merge_start_time_us), K(to_window), K(resource_cache));
  } else if (OB_FAIL(get_current_resource_manager_plan_(current_plan))) {
    LOG_WARN("failed to get current resource manager plan", K(ret));
  } else if (to_window && OB_FAIL(check_window_plan_and_consumer_group_exist_())) {
    LOG_WARN("failed to check window plan and consumer group exist", K(ret));
  } else if (OB_FAIL(switch_compaction_plan_(current_plan.string(), to_window, merge_start_time_us, resource_cache, is_switched))) {
    LOG_WARN("failed to switch compaction plan", K(ret), K(to_window), K(merge_start_time_us), K(resource_cache), K(current_plan));
  }
  return ret;
}

int ObCompactionResourceManager::switch_to_normal_before_major_merge(
    const int64_t merge_start_time_us,
    rootserver::ObWindowResourceCache &resource_cache)
{
  int ret = OB_SUCCESS;
  uint64_t min_data_version = 0;
  ObSqlString current_plan;
  bool is_switched = false;
  if (OB_UNLIKELY(merge_start_time_us < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid merge start time us", KR(ret), K(merge_start_time_us));
  } else if (!is_user_tenant(tenant_id_) || resource_cache.check_could_skip(merge_start_time_us, false /*to_window*/)) {
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id_, min_data_version))) {
    LOG_WARN("fail to get data version", KR(ret), K_(tenant_id));
  } else if (min_data_version < DATA_VERSION_4_5_1_0) {
  } else if (OB_FAIL(get_current_resource_manager_plan_(current_plan))) {
    LOG_WARN("failed to get current resource manager plan", K(ret));
  } else if (OB_FAIL(switch_compaction_plan_(current_plan.string(), false /*to_window*/, merge_start_time_us, resource_cache, is_switched))) {
    LOG_WARN("failed to switch compaction plan", K(ret), K(merge_start_time_us), K(resource_cache), K(current_plan));
  } else {
    LOG_INFO("finish switch to normal plan before major merge", K(merge_start_time_us), K(resource_cache), K(current_plan), K(is_switched));
  }
  return ret;
}

int ObCompactionResourceManager::get_window_compaction_thread_cnt(
    int64_t &thread_cnt,
    bool &from_job_config)
{
  int ret = OB_SUCCESS;
  thread_cnt = 0;
  from_job_config = false;
  dbms_scheduler::ObDBMSSchedJobInfo job_info;
  ObDailyWindowJobConfig job_cfg(allocator_);
  ObMySQLTransaction trans;
  ObResourceManagerProxy::TransGuard trans_guard(trans, tenant_id_, ret);
  if (OB_UNLIKELY(!trans_guard.ready())) {
    // warning is logged in trans_guard
  } else if (OB_ISNULL(GCTX.cgroup_ctrl_) || !GCTX.cgroup_ctrl_->is_valid()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("adaptive window compaction thread is only supported when cgroup is valid", KR(ret));
  } else if (OB_FAIL(check_window_resources_ready_(trans))) {
    LOG_WARN("failed to check window resources ready", K(ret));
  } else if (OB_FAIL(ObScheduleDailyMaintenanceWindow::get_daily_maintenance_window_job_info(trans, tenant_id_, allocator_, job_info))) {
    LOG_WARN("failed to get daily maintenance window job info", K(ret));
  } else if (OB_FAIL(job_cfg.parse_from_string(job_info.job_config_))) {
    LOG_WARN("failed to parse job config", K(ret), "job_config", job_info.job_config_);
  } else if (job_cfg.thread_cnt_ != 0) {
    thread_cnt = job_cfg.thread_cnt_;
    from_job_config = true;
    LOG_INFO("[WIN-COMPACTION] get window compaction thread cnt from job config", K(ret), K(thread_cnt));
  } else if (OB_FAIL(calculate_window_compaction_thread_cnt_(trans, thread_cnt))) {
    LOG_WARN("failed to calculate window compaction thread cnt", K(ret));
  }
  return ret;
}

#define GET_POSITIVE_INT_FLOOR(value) std::max(1L, static_cast<int64_t>(std::floor(value)))
int ObCompactionResourceManager::calculate_window_compaction_thread_cnt_(
    ObMySQLTransaction &trans,
    int64_t &thread_cnt)
{
  int ret = OB_SUCCESS;
  thread_cnt = 0;
  ObPlanDirective directive;
  const double global_background_cpu_quota = static_cast<double>(GCONF.global_background_cpu_quota);
  double min_cpu = 0.0;
  double max_cpu = 0.0;
  if (OB_FAIL(proxy_.get_plan_directive(trans, tenant_id_, window_plan_, window_consumer_group_, directive))) {
    LOG_WARN("failed to get plan directive", K(ret), K_(window_plan), K_(window_consumer_group));
  } else if (OB_UNLIKELY(!directive.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("plan directive is not valid", K(ret), K_(window_plan), K_(window_consumer_group));
  } else if (OB_ISNULL(GCTX.omt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("omt is null", KR(ret));
  } else if (OB_FAIL(GCTX.omt_->get_tenant_cpu(tenant_id_, min_cpu, max_cpu))) {
    LOG_WARN("failed to get tenant cpu", KR(ret));
  } else if (GCONF.enable_global_background_resource_isolation && global_background_cpu_quota > 0) {
    thread_cnt = GET_POSITIVE_INT_FLOOR(std::min(global_background_cpu_quota, max_cpu) * directive.utilization_limit_ / 100.0);
  } else {
    thread_cnt = GET_POSITIVE_INT_FLOOR(max_cpu * directive.utilization_limit_ / 100.0);
  }

  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(thread_cnt < 0 || thread_cnt > ObDailyWindowJobConfig::MAX_THREAD_CNT)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid thread cnt", KR(ret), K(thread_cnt), K(ObDailyWindowJobConfig::MAX_THREAD_CNT));
  } else {
    LOG_INFO("[WIN-COMPACTION] calculate window compaction thread cnt", K(ret), K(thread_cnt), K(global_background_cpu_quota), K(max_cpu), K(directive));
  }
  return ret;
}
#undef GET_POSITIVE_INT_FLOOR

int ObCompactionResourceManager::check_window_plan_and_consumer_group_exist_()
{
  int ret = OB_SUCCESS;
  bool plan_exist = false;
  bool consumer_group_exist = false;
  bool plan_directive_exist = false;
  ObMySQLTransaction trans;
  ObResourceManagerProxy::TransGuard trans_guard(trans, tenant_id_, ret);
  if (trans_guard.ready()) {
    if (OB_FAIL(proxy_.check_if_plan_exist(trans, tenant_id_, window_plan_, plan_exist))) {
      LOG_WARN("failed to check daily window plan exist", K(ret), K_(window_plan));
    } else if (OB_UNLIKELY(!plan_exist)) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("[WIN-COMPACTION] daily window plan not exist, please create it", K(ret), K_(window_plan));
    } else if (OB_FAIL(proxy_.check_if_consumer_group_exist(trans, tenant_id_, window_consumer_group_, consumer_group_exist))) {
      LOG_WARN("failed to check consumer group exist", K(ret), K_(window_consumer_group));
    } else if (OB_UNLIKELY(!consumer_group_exist)) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("[WIN-COMPACTION] daily window consumer group not exist, please create it", K(ret), K_(window_consumer_group));
    } else if (OB_FAIL(proxy_.check_if_plan_directive_exist(trans, tenant_id_, window_plan_, window_consumer_group_, plan_directive_exist))) {
      LOG_WARN("failed to check daily window plan directive exist", K(ret), K_(window_plan), K_(window_consumer_group));
    } else if (OB_UNLIKELY(!plan_directive_exist)) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("[WIN-COMPACTION] directive for window consumer group not exist, please create it", K(ret), K_(window_plan), K_(window_consumer_group));
    }

    // TODO:(chengkong) tmp usage, it is better to add into compaction diagnose info
    if (OB_ENTRY_NOT_EXIST == ret && TC_REACH_TIME_INTERVAL(ADD_RS_EVENT_INTERVAL)) {
      ROOTSERVICE_EVENT_ADD("window_compaction", "plan_or_consumer_group_not_exist",
                            K_(tenant_id), K(ret), K(plan_exist), K(consumer_group_exist), K(plan_directive_exist));
    }
  }
  return ret;
}

int ObCompactionResourceManager::check_window_resources_ready_(ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObSqlString current_plan;
  ObSqlString current_consumer_group;
  bool is_exist = false;
  if (OB_FAIL(get_current_resource_manager_plan_(current_plan))) {
    LOG_WARN("failed to get current resource plan", K(ret));
  } else if (!is_window_plan_(current_plan.string())) {
    ret = OB_EAGAIN;
    LOG_WARN("adaptive window compaction thread is only supported when resource manager plan is window plan", K(ret));
  } else if (OB_FAIL(get_current_compaction_low_consumer_group_(trans, current_consumer_group))) {
    LOG_WARN("failed to get current compaction low consumer group", K(ret));
  } else if (!is_window_consumer_group_(current_consumer_group.string())) {
    ret = OB_EAGAIN;
    LOG_WARN("adaptive window compaction thread is only supported when resource manager consumer group is window consumer group", K(ret));
  } else if (OB_FAIL(proxy_.check_if_plan_directive_exist(trans, tenant_id_, window_plan_, window_consumer_group_, is_exist))) {
    LOG_WARN("failed to check daily window plan directive exist", K(ret), K_(window_plan), K_(window_consumer_group));
  } else if (OB_UNLIKELY(!is_exist)) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("directive for window consumer group not exist", K(ret), K_(window_plan), K_(window_consumer_group));
  }
  return ret;
}

int ObCompactionResourceManager::get_current_resource_manager_plan_(ObSqlString &current_plan)
{
  int ret = OB_SUCCESS;
  char plan_name_data[OB_MAX_RESOURCE_PLAN_NAME_LENGTH];
  ObDataBuffer plan_allocator(plan_name_data, OB_MAX_RESOURCE_PLAN_NAME_LENGTH);
  ObString tmp_plan;
  if (OB_FAIL(schema::ObSchemaUtils::get_tenant_varchar_variable(tenant_id_, SYS_VAR_RESOURCE_MANAGER_PLAN, plan_allocator, tmp_plan))) {
    LOG_WARN("failed to get current resource plan", K(ret));
  } else if (tmp_plan.empty()) {
    // Default value of resource_manager_plan is empty, explicitly assign empty string("") instead of leaving it NULL.
    if (OB_FAIL(current_plan.assign(""))) {
      LOG_WARN("failed to assign empty plan", K(ret));
    } else {
      LOG_INFO("current resource plan is empty", K(tmp_plan), K(current_plan));
    }
  } else if (OB_FAIL(current_plan.assign(tmp_plan))) {
    LOG_WARN("failed to assign plan", K(ret), K(tmp_plan));
  }
  return ret;
}

int ObCompactionResourceManager::get_current_compaction_low_consumer_group_(
    ObMySQLTransaction &trans,
    ObSqlString &current_consumer_group)
{
  int ret = OB_SUCCESS;
  ObString tmp_consumer_group;
  bool is_exist = false;
  if (OB_FAIL(proxy_.get_function_mapping_info(trans, tenant_id_, allocator_, attr_value_, tmp_consumer_group))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      // mapping rule for COMPACTION_LOW may not exist, take it as "".
      if (OB_FAIL(current_consumer_group.assign(""))) {
        LOG_WARN("failed to assign empty consumer group", K(ret));
      }
    } else {
      LOG_WARN("failed to get current compaction low consumer group", K(ret), K_(attr_value));
    }
  } else if (OB_FAIL(proxy_.check_if_consumer_group_exist(trans, tenant_id_, tmp_consumer_group, is_exist))) {
    LOG_WARN("failed to check consumer group exist", K(ret), K(tmp_consumer_group));
  } else if (!is_exist) {
    // consumer group is deleted, but function mapping info not, take it as "".
    if (OB_FAIL(current_consumer_group.assign(""))) {
      LOG_WARN("failed to assign empty consumer group", K(ret));
    }
  } else if (OB_FAIL(current_consumer_group.assign(tmp_consumer_group))) {
    LOG_WARN("failed to assign consumer group", K(ret), K(tmp_consumer_group));
  }
  return ret;
}

int ObCompactionResourceManager::switch_compaction_plan_(
    const ObString &current_plan,
    const bool to_window,
    const int64_t merge_start_time_us,
    rootserver::ObWindowResourceCache &resource_cache,
    bool &is_switched)
{
  int ret = OB_SUCCESS;
  is_switched = false;
  ObFinishedStep finished_step = FINISH_STEP_MAX;
  if (OB_FAIL(switch_compaction_plan_by_step_(current_plan, to_window, merge_start_time_us, resource_cache, finished_step))) {
    LOG_WARN("failed to switch compaction plan by step", K(ret));
  } else if (SUBMIT_PREV_INFO == finished_step
          && OB_FAIL(switch_compaction_plan_by_step_(current_plan, to_window, merge_start_time_us, resource_cache, finished_step))) {
    LOG_WARN("failed to switch compaction plan by step", K(ret));
  } else if (APPLY_WINDOW_PLAN == finished_step) {
    is_switched = true;
    ROOTSERVICE_EVENT_ADD("window_compaction", "switch_to_window_compaction_plan", K_(tenant_id), K(ret));
  } else if (ROLLBACK_PREV_INFO == finished_step) {
    is_switched = true;
    ROOTSERVICE_EVENT_ADD("window_compaction", "switch_to_normal_compaction_plan", K_(tenant_id), K(ret));
  }
  return ret;
}

int ObCompactionResourceManager::switch_compaction_plan_by_step_(
    const ObString &current_plan,
    const bool to_window,
    const int64_t merge_start_time_us,
    rootserver::ObWindowResourceCache &resource_cache,
    ObFinishedStep &finished_step)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  dbms_scheduler::ObDBMSSchedJobInfo job_info;
  ObDailyWindowJobConfig job_cfg(allocator_);
  ObSqlString current_consumer_group;
  if (OB_UNLIKELY(merge_start_time_us < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid merge start time us", KR(ret), K(merge_start_time_us));
  } else {
    ObResourceManagerProxy::TransGuard trans_guard(trans, tenant_id_, ret);
    if (OB_UNLIKELY(!trans_guard.ready())) {
      // warning is logged in trans_guard
    } else if (to_window && OB_FAIL(get_current_compaction_low_consumer_group_(trans, current_consumer_group))) {
      LOG_WARN("failed to get current compaction low consumer group", K(ret));
    } else if (OB_FAIL(ObScheduleDailyMaintenanceWindow::get_daily_maintenance_window_job_info(trans, tenant_id_, allocator_, job_info))) {
      LOG_WARN("failed to get daily maintenance window job info", K(ret));
    } else if (OB_FAIL(job_cfg.parse_from_string(job_info.job_config_))) {
      LOG_WARN("failed to parse job config", K(ret), "job_config", job_info.job_config_);
    } else if (job_cfg.is_prev_info_empty()) {
      if (to_window && OB_FAIL(submit_prev_info_(trans, job_cfg, current_plan, current_consumer_group.string()))) {
        LOG_WARN("failed to submit prev info", K(ret), K(job_cfg), K(current_plan), K(current_consumer_group), K(job_info));
      } else {
        // prev info is empty, means the resource manager plan and consumer group have been rollback, or have not been altered
        finished_step = to_window ? SUBMIT_PREV_INFO : NONE;
      }
    } else if (job_cfg.is_prev_info_submitted()) {
      if (to_window && is_window_plan_(current_plan) && is_window_consumer_group_(current_consumer_group.string())) {
        finished_step = NONE; // already in window compaction plan
      } else if (OB_FAIL(switch_plan_and_consumer_group_(trans, job_cfg, current_plan, to_window))) {
        LOG_WARN("failed to step to altered stage", K(ret), K(job_cfg), K(current_plan), K(job_info));
      } else {
        finished_step = to_window ? APPLY_WINDOW_PLAN : ROLLBACK_PREV_INFO;
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected job config", K(ret), K(job_cfg));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (SUBMIT_PREV_INFO == finished_step) {
    LOG_INFO("[WIN-COMPACTION] Success to submit previous plan and consumer group", K(job_cfg), K(job_info));
    ROOTSERVICE_EVENT_ADD("window_compaction", "submit_prev_info", K_(tenant_id), K(ret),
                          "prev_group", job_cfg.prev_group_, "prev_plan", job_cfg.prev_plan_);
  } else if (APPLY_WINDOW_PLAN == finished_step) {
    resource_cache.update(merge_start_time_us, to_window);
    LOG_INFO("[WIN-COMPACTION] Success to apply window plan and consumer group", K(job_cfg), K(job_info), K(resource_cache));
    ROOTSERVICE_EVENT_ADD("window_compaction", "apply_window_plan", K_(tenant_id), K(ret),
                          "prev_group", job_cfg.prev_group_, "prev_plan", job_cfg.prev_plan_);
  } else if (ROLLBACK_PREV_INFO == finished_step) {
    resource_cache.update(merge_start_time_us, to_window);
    LOG_INFO("[WIN-COMPACTION] Success to switch to normal compaction plan", K(job_cfg), K(job_info), K(resource_cache));
  } else if (NONE == finished_step) {
    resource_cache.update(merge_start_time_us, to_window);
    LOG_INFO("[WIN-COMPACTION] Already switch to target plan and consumer group", K(job_cfg), K(job_info), K(resource_cache));
  }
  return ret;
}

int ObCompactionResourceManager::submit_prev_info_(
    ObMySQLTransaction &trans,
    ObDailyWindowJobConfig &job_cfg,
    const ObString &current_plan,
    const ObString &current_consumer_group)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(job_cfg.set_prev_plan(current_plan))) {
    LOG_WARN("failed to set prev plan", K(ret), K(current_plan));
  } else if (OB_FAIL(job_cfg.set_prev_group(current_consumer_group))) {
    LOG_WARN("failed to set prev group", K(ret), K(current_consumer_group));
  } else if (OB_FAIL(ObScheduleDailyMaintenanceWindow::set_job_config(trans, allocator_, tenant_id_, job_cfg))) {
    LOG_WARN("failed to set job config", K(ret), K(job_cfg));
  }
  return ret;
}

int ObCompactionResourceManager::switch_plan_and_consumer_group_(
    ObMySQLTransaction &trans,
    ObDailyWindowJobConfig &job_cfg,
    const ObString &current_plan,
    const bool to_window)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!job_cfg.is_prev_info_submitted())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("prev info should be submitted before apply or rollback", K(ret), K(job_cfg));
  } else if (OB_FAIL(switch_plan_(trans, job_cfg, current_plan, to_window))) {
    LOG_WARN("failed to switch to target plan", K(ret), K(job_cfg), K(current_plan));
  } else if (OB_FAIL(switch_consumer_group_(trans, job_cfg, to_window))) {
    LOG_WARN("failed to switch to target consumer group", K(ret), K(job_cfg));
  } else if (!to_window && FALSE_IT(job_cfg.reset_prev_info())) {
  } else if (OB_FAIL(ObScheduleDailyMaintenanceWindow::set_job_config(trans, allocator_, tenant_id_, job_cfg))) {
    LOG_WARN("failed to set job config", K(ret), K(job_cfg));
  }
  return ret;
}

int ObCompactionResourceManager::switch_plan_(
    ObMySQLTransaction &trans,
    const ObDailyWindowJobConfig &job_cfg,
    const ObString &current_plan,
    const bool to_window)
{
  int ret = OB_SUCCESS;
  bool exist = false;
  const ObString &target_plan = to_window ? window_plan_ : job_cfg.prev_plan_.string();
  ObSqlString update_plan_sql;
  int64_t unused_affected_rows = 0; // set global sys variable does not affect rows
  if (to_window == is_window_plan_(current_plan)) {
    LOG_INFO("current plan is already in target plan, don't need to switch", K(current_plan), K(job_cfg));
  } else if (OB_UNLIKELY(!job_cfg.is_prev_plan_submitted())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("prev plan should be submitted", K(ret), K(job_cfg));
  } else if (!target_plan.empty() && OB_FAIL(proxy_.check_if_plan_exist(trans, tenant_id_, target_plan, exist))) {
    LOG_WARN("failed to check plan exist", K(ret), K(target_plan));
  } else if (!target_plan.empty() && OB_UNLIKELY(!exist)) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_ERROR("target plan does not exist, failed to switch to target plan", K(ret), K(target_plan));
  } else if (OB_FAIL(update_plan_sql.assign_fmt("set global resource_manager_plan = '%.*s'", target_plan.length(), target_plan.ptr()))) {
    LOG_WARN("failed to assign update plan sql", K(ret));
  } else if (OB_FAIL(trans.write(tenant_id_, update_plan_sql.ptr(), unused_affected_rows))) {
    LOG_WARN("failed to update plan", K(ret), K(update_plan_sql));
  } else {
    LOG_INFO("[WIN-COMPACTION] Success to switch to target plan", K(job_cfg), K(target_plan));
  }
  return ret;
}

// If previous consumer group of COMPACTION_LOW doesn't exist, it means it use the default OTHER_GROUP.
// The prev_group would be "" in this case, and the mapping rule will be removed when the input consumer group is empty
// in ObResourceManagerProxy::replace_function_mapping_rule
int ObCompactionResourceManager::switch_consumer_group_(
    ObMySQLTransaction &trans,
    const ObDailyWindowJobConfig &job_cfg,
    const bool to_window)
{
  int ret = OB_SUCCESS;
  bool exist = false;
  const ObString &target_consumer_group = to_window ? window_consumer_group_ : job_cfg.prev_group_.string();
  if (OB_UNLIKELY(!job_cfg.is_prev_group_submitted())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("prev group should be submitted before apply or rollback", K(ret), K(job_cfg));
  } else if (!target_consumer_group.empty() && OB_FAIL(proxy_.check_if_consumer_group_exist(trans, tenant_id_, target_consumer_group, exist))) {
    LOG_WARN("failed to check consumer group exist", K(ret), K(target_consumer_group), K(job_cfg), K(to_window));
  } else if (!target_consumer_group.empty() && OB_UNLIKELY(!exist)) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_ERROR("target consumer group does not exist, failed to switch", K(ret), K(target_consumer_group), K(job_cfg), K(to_window));
  } else if (OB_FAIL(proxy_.replace_function_mapping_rule(trans, tenant_id_, attr_name_, attr_value_, target_consumer_group, true /* is_window_compaction_switch */))) {
    LOG_WARN("failed to replace function mapping rule", K(ret), K_(attr_name), K_(attr_value), K(job_cfg), K(to_window));
  } else {
    LOG_INFO("[WIN-COMPACTION] Success to switch to target consumer group", K(job_cfg), K(target_consumer_group), K(to_window));
  }
  return ret;
}

} // end of share
} // end of oceanbase
