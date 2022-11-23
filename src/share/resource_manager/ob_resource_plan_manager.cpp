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

#define USING_LOG_PREFIX SHARE
#include "ob_resource_plan_manager.h"
#include "lib/string/ob_string.h"
#include "share/resource_manager/ob_resource_manager_proxy.h"
#include "share/resource_manager/ob_cgroup_ctrl.h"
#include "observer/ob_server_struct.h"


using namespace oceanbase::common;
using namespace oceanbase::share;

int ObResourcePlanManager::init()
{
  int ret = OB_SUCCESS;
  LOG_INFO("resource plan manager init ok");
  return ret;
}

int ObResourcePlanManager::refresh_resource_plan(uint64_t tenant_id, ObString &plan_name)
{
  int ret = OB_SUCCESS;
  ObResourceManagerProxy proxy;
  // 目前每个租户最多只有 2 个 活跃 directive : interactive, batch
  ObPlanDirectiveSet directives;
  if (OB_ISNULL(GCTX.cgroup_ctrl_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cgroup ctrl is null", K(ret));
  } else if (!GCTX.cgroup_ctrl_->is_valid()) {
    ret = OB_EAGAIN;
    // cgroup ctrl 没有初始化成功，可能是没有 cgroup fs、没有权限等原因
    // 此时不再继续后继资源隔离操作
  } else if (OB_FAIL(proxy.get_all_plan_directives(tenant_id, plan_name, directives))) {
    LOG_WARN("fail get plan directive", K(tenant_id), K(plan_name), K(ret));
  } else {
    // directive => cgroup share/cfs_cpu_quota 转换。2 步:
    //   step1: 以 100 为总值做归一化
    //   step2: 将值转化成 cgroup 值 （utilization=>cfs_cpu_quota 的值和 cpu 核数等有关)
    //      - 如果 utilization = 100，那么 cfs_cpu_quota = -1
    if (OB_FAIL(create_cgroup_dir_if_not_exist(directives))) {
      LOG_WARN("fail create cgroup dir", K(directives), K(ret));
    } else if (OB_FAIL(normalize_directives(directives))) {
      LOG_WARN("fail normalize directive", K(ret));
    } else if (OB_FAIL(flush_directive_to_cgroup_fs(directives))) {
      LOG_WARN("fail flush directive to cgroup fs", K(ret));
    }
    LOG_INFO("refresh_resource_plan", K(tenant_id), K(plan_name), K(directives));
  }
  return ret;
}

int ObResourcePlanManager::normalize_directives(ObPlanDirectiveSet &directives)
{
  int ret = OB_SUCCESS;
  int64_t total_mgmt = 0;
  int64_t total_util = 0;
  for (int64_t i = 0; i < directives.count(); ++i) {
    const ObPlanDirective &d = directives.at(i);
    total_mgmt += d.mgmt_p1_;
    total_util += d.utilization_limit_;
  }

  // 计算：cfs_cpu_quota 值
  // 公式
  //  {
  //   cfs_cpu_quota = d.utilization_limit_percent_ * tenant.cfs_cpu_peroid * unit_min_cpu / 100
  //   unit_min_cpu = tenant.cpu.shares / 1024   # 租户的 shares 值是根据 unit 的 min_cpu 计算而得，
  //  }
  //  =>
  //  {
  //   cfs_cpu_quota = d.utilization_limit_percent_ * tenant.cfs_cpu_peroid * / 100 * tenant.cpu.shares / 1024
  //                 = d.utilization_limit_ * 100 / total * tenant.cfs_cpu_peroid * / 100 * tenant.cpu.shares / 1024
  //                 = (d.utilization_limit_ / total) * tenant.cfs_cpu_peroid *  (tenant.cpu.shares / 1024)
  //  }
  //
  // 特殊值考虑：
  //  如果 mgmt 设置为 0，意味着用户希望它的 share 尽可能小
  //  如果 limit 设置为 0，意味着用户希望它的 cpu quota 尽可能小
  //  注意：mgmt, limit 的默认值都不是 0
  for (int64_t i = 0; i < directives.count(); ++i) {
    ObPlanDirective &d = directives.at(i);
    if (0 == total_mgmt) {
      d.mgmt_p1_ = 0;
    } else {
      d.mgmt_p1_ = 100 * d.mgmt_p1_ / total_mgmt;
    }
    if (0 == total_util) {
      d.utilization_limit_ = 0;
    } else {
      int32_t tenant_cpu_shares = 0;
      int32_t cfs_period_us = 0;
      if (OB_FAIL(GCTX.cgroup_ctrl_->get_cpu_shares(tenant_cpu_shares, d.tenant_id_))) {
        LOG_WARN("fail get cpu shares", K(d), K(ret));
      } else if (OB_FAIL(GCTX.cgroup_ctrl_->get_cpu_cfs_period(
                  d.tenant_id_,
                  d.level_,
                  d.group_name_,
                  cfs_period_us))) {
        LOG_WARN("fail get cpu cfs period", K(d), K(ret));
      } else {
        if (d.utilization_limit_ == 100) {
          // 不限制
          d.utilization_limit_ = -1;
        } else {
          d.utilization_limit_ =
              (int64_t)cfs_period_us * tenant_cpu_shares *  d.utilization_limit_ / 100 / 1024;
        }
      }
    }
  }
  return ret;
}

int ObResourcePlanManager::create_cgroup_dir_if_not_exist(const ObPlanDirectiveSet &directives)
{
  int ret = OB_SUCCESS;
  share::ObCgroupCtrl *cgroup_ctrl = GCTX.cgroup_ctrl_;
  if (OB_ISNULL(cgroup_ctrl) || !cgroup_ctrl->is_valid()) {
    ret = OB_NOT_INIT;
  } else {
    for (int64_t i = 0; i < directives.count(); ++i) {
      const ObPlanDirective &d = directives.at(i);
      if (OB_FAIL(cgroup_ctrl->create_user_tenant_group_dir(
                  d.tenant_id_,
                  d.level_,
                  d.group_name_))) {
        LOG_WARN("fail init user tenant group", K(d), K(ret));
      }
    }
  }
  return ret;
}


/*
 * cpu share 比较简单，给相对值即可。
 * cfs_quota_us 相对复杂，需要考虑 cpu 数量。具体规则如下：
 *
 * https://www.kernel.org/doc/Documentation/scheduler/sched-bwc.txt
 *
 * 1. Limit a group to 1 CPU worth of runtime.
 *
 *   If period is 250ms and quota is also 250ms, the group will get
 *   1 CPU worth of runtime every 250ms.
 *
 *   # echo 250000 > cpu.cfs_quota_us // quota = 250ms
 *   # echo 250000 > cpu.cfs_period_us // period = 250ms
 *
 * 2. Limit a group to 2 CPUs worth of runtime on a multi-CPU machine.
 *
 *   With 500ms period and 1000ms quota, the group can get 2 CPUs worth of
 *   runtime every 500ms.
 *
 *   # echo 1000000 > cpu.cfs_quota_us // quota = 1000ms
 *   # echo 500000 > cpu.cfs_period_us // period = 500ms
 *
 *   The larger period here allows for increased burst capacity.
 *
 * 3. Limit a group to 20% of 1 CPU.
 *
 *   With 50ms period, 10ms quota will be equivalent to 20% of 1 CPU.
 *
 *   # echo 10000 > cpu.cfs_quota_us   // quota = 10ms
 *   # echo 50000 > cpu.cfs_period_us  // period = 50ms
 *
 *   By using a small period here we are ensuring a consistent latency
 *   response at the expense of burst capacity.
 */
int ObResourcePlanManager::flush_directive_to_cgroup_fs(ObPlanDirectiveSet &directives)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < directives.count(); ++i) {
    const ObPlanDirective &d = directives.at(i);
    if (OB_FAIL(GCTX.cgroup_ctrl_->set_cpu_shares(
                d.tenant_id_,
                d.level_,
                d.group_name_,
                static_cast<int32_t>(d.mgmt_p1_)))) {
      LOG_ERROR("fail set cpu shares. tenant isolation function may not functional!!",
                K(d), K(ret));
    } else if (OB_FAIL(GCTX.cgroup_ctrl_->set_cpu_cfs_quota(
                d.tenant_id_,
                d.level_,
                d.group_name_,
                static_cast<int32_t>(d.utilization_limit_)))) {
      LOG_ERROR("fail set cpu quota. tenant isolation function may not functional!!",
                K(d), K(ret));
    }
    // ignore ret, continue
  }
  return ret;
}
