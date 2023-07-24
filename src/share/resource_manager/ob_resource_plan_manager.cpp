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
#include "share/io/ob_io_manager.h"
#include "share/resource_manager/ob_resource_manager_proxy.h"
#include "share/resource_manager/ob_cgroup_ctrl.h"
#include "observer/ob_server_struct.h"


using namespace oceanbase::common;
using namespace oceanbase::share;

int ObResourcePlanManager::init()
{
  int ret = OB_SUCCESS;
  if (tenant_plan_map_.created()) {
    ret = OB_INIT_TWICE;
    LOG_WARN("mapping rule manager should not init multiple times", K(ret));
  } else if (OB_FAIL(tenant_plan_map_.create(7, "TENANT_PLAN_MAP"))) {
    LOG_WARN("fail create tenant_plan_map", K(ret));
  } else {
    LOG_INFO("resource plan manager init ok");
  }
  return ret;
}

int ObResourcePlanManager::switch_resource_plan(const uint64_t tenant_id, ObString &plan_name)
{
  int ret = OB_SUCCESS;
  ObResMgrVarcharValue origin_plan;
  ObResMgrVarcharValue cur_plan(plan_name);
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_TENANT_ID;
    LOG_WARN("invalid config", K(ret), K(tenant_id));
  } else if (OB_FAIL(tenant_plan_map_.get_refactored(tenant_id, origin_plan))) {
    if (OB_HASH_NOT_EXIST == ret) {
      // initialize
      if (OB_FAIL(tenant_plan_map_.set_refactored(tenant_id, cur_plan))) {
        LOG_WARN("set plan failed", K(ret), K(tenant_id));
      } else {
        LOG_INFO("add tenant id plan success", K(tenant_id), K(cur_plan));
      }
    } else {
      LOG_WARN("get plan failed", K(ret), K(tenant_id));
    }
  } else if (origin_plan != cur_plan) {
    // switch plan，reset 原来plan下对应directive的io资源
    ObResourceManagerProxy proxy;
    if (OB_FAIL(GCTX.cgroup_ctrl_->reset_all_group_iops(
                      tenant_id,
                      1))) {
      LOG_ERROR("reset old plan group directive failed", K(tenant_id), K(ret));
    }
    if (OB_SUCC(ret) && plan_name.empty()) {
      // reset user and function hashmap
      if (OB_FAIL(proxy.reset_all_mapping_rules())) {
        LOG_WARN("fail reset all group rules",K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(tenant_plan_map_.set_refactored(tenant_id, cur_plan, 1))) { //overrite
        LOG_WARN("set plan failed", K(ret), K(tenant_id));
      } else {
        LOG_INFO("switch resource plan success", K(tenant_id), K(origin_plan), K(cur_plan));
      }
    }
  }
  return ret;
}

int ObResourcePlanManager::refresh_resource_plan(const uint64_t tenant_id, ObString &plan_name)
{
  int ret = OB_SUCCESS;
  ObResourceManagerProxy proxy;
  ObPlanDirectiveSet directives;
  ObPlanDirective other_directive; // for OTHER_GROUPS
  other_directive.set_group_id(0);
  other_directive.set_tenant_id(tenant_id);
    // 首先check plan是否发生了切换，如果plan切换那么原plan中资源设置先清零
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_TENANT_ID;
    LOG_WARN("invalid config", K(ret), K(tenant_id));
  } else if (OB_FAIL(switch_resource_plan(tenant_id, plan_name))) {
    LOG_WARN("check resource plan failed", K(ret), K(tenant_id), K(plan_name));
  } else if (OB_FAIL(proxy.get_all_plan_directives(tenant_id, plan_name, directives))) {
    LOG_WARN("fail get plan directive", K(tenant_id), K(plan_name), K(ret));
  } else if (OB_FAIL(normalize_iops_directives(tenant_id, directives, other_directive))) {
    LOG_WARN("fail normalize directive", K(ret));
  } else if (OB_FAIL(flush_directive_to_iops_control(tenant_id, directives, other_directive))) { // for IOPS
    LOG_WARN("fail flush directive to io control", K(ret));
  } else {
    if (OB_ISNULL(GCTX.cgroup_ctrl_) || !(GCTX.cgroup_ctrl_->is_valid())) {
      // do nothing，cgroup ctrl 没有初始化成功，可能是没有 cgroup fs、没有权限等原因
      // cgroup不生效无法对CPU资源进行隔离，但上述io资源隔离可以继续


      // directive => cgroup share/cfs_cpu_quota 转换。2 步:
      //   step1: 以 100 为总值做归一化
      //   step2: 将值转化成 cgroup 值 （utilization=>cfs_cpu_quota 的值和 cpu 核数等有关)
      //      - 如果 utilization = 100，那么 cfs_cpu_quota = -1
    } else if (OB_FAIL(create_cgroup_dir_if_not_exist(directives))) {
      LOG_WARN("fail create cgroup dir", K(directives), K(ret));
    } else if (OB_FAIL(normalize_cpu_directives(directives))) {
      LOG_WARN("fail normalize directive", K(ret));
    } else if (OB_FAIL(flush_directive_to_cgroup_fs(directives))) { // for CPU
      LOG_WARN("fail flush directive to cgroup fs", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("refresh resource plan success", K(tenant_id), K(plan_name), K(directives));
  }
  return ret;
}

int ObResourcePlanManager::get_cur_plan(const uint64_t tenant_id, ObResMgrVarcharValue &plan_name)
{
  int ret = OB_SUCCESS;
  plan_name.reset();
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_TENANT_ID;
    LOG_WARN("invalid config", K(ret), K(tenant_id));
  } else if (OB_FAIL(tenant_plan_map_.get_refactored(tenant_id, plan_name))) {
    if (OB_HASH_NOT_EXIST == ret) {
      //plan只有被使用才会放到map里
      ret = OB_SUCCESS;
      LOG_INFO("delete plan success with no_releated_io_module", K(plan_name), K(tenant_id));
    } else {
      LOG_WARN("get plan failed", K(ret), K(tenant_id), K(plan_name));
    }
  }
  return ret;
}

int ObResourcePlanManager::normalize_cpu_directives(ObPlanDirectiveSet &directives)
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

int ObResourcePlanManager::normalize_iops_directives(const uint64_t tenant_id,
                                                     ObPlanDirectiveSet &directives,
                                                     ObPlanDirective &other_group_directive)
{
  int ret = OB_SUCCESS;
  // 在本版本中，用户无法指定OTHER_GROUPS及其他默认组的资源，OTHER资源是使用其他资源组算出来的
  // OTHER MIN_IOPS = 100-SUM; MAX_IOPS = 100; WEIGHT_IOPS = 100/SUM;
  // 需要在产品手册中告知，建议用户不要把所有组的min_iops总和设置成100%

  uint64_t total_weight = 0;
  uint64_t total_min = 0;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_TENANT_ID;
    LOG_WARN("invalid config", K(ret), K(tenant_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < directives.count(); ++i) {
      ObPlanDirective &cur_directive = directives.at(i);
      if (OB_UNLIKELY(!is_user_group(cur_directive.group_id_))) {
        ret = OB_ERR_UNEXPECTED;
        // 理论上不应该出现
        LOG_WARN("unexpected error!!!", K(cur_directive));
      } else if (OB_UNLIKELY(!cur_directive.is_valid())) {
        ret = OB_INVALID_CONFIG;
        LOG_WARN("invalid group io config", K(cur_directive));
      } else {
        total_weight += cur_directive.weight_iops_;
        total_min += cur_directive.min_iops_;
      }
    }
    total_weight += OTHER_GROUPS_IOPS_WEIGHT; //OTHER GROUPS WEIGHT

    if(OB_SUCC(ret)) {
      if (total_min > 100) {
        ret = OB_INVALID_CONFIG;
        LOG_WARN("invalid group io config", K(total_min));
      } else {
        for (int64_t i = 0; i < directives.count(); ++i) {
          ObPlanDirective &cur_directive = directives.at(i);
          cur_directive.weight_iops_ = 100 * cur_directive.weight_iops_ / total_weight;
        }
        other_group_directive.weight_iops_ = 100 * 100 / total_weight;
        other_group_directive.min_iops_ = 100 - total_min;
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

int ObResourcePlanManager::flush_directive_to_iops_control(const uint64_t tenant_id,
                                                           ObPlanDirectiveSet &directives,
                                                           ObPlanDirective &other_group_directive)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_TENANT_ID;
    LOG_WARN("invalid config", K(ret), K(tenant_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < directives.count(); ++i) {
      const ObPlanDirective &cur_directive = directives.at(i);
      share::OBGroupIOInfo cur_io_info;
      if (OB_FAIL(cur_io_info.init(cur_directive.min_iops_, cur_directive.max_iops_, cur_directive.weight_iops_))) {
        LOG_ERROR("fail init group io info", K(cur_directive), K(ret));
      } else if (OB_FAIL(GCTX.cgroup_ctrl_->set_group_iops(
                        cur_directive.tenant_id_,
                        cur_directive.level_,
                        cur_directive.group_id_,
                        cur_io_info))) {
        LOG_ERROR("fail set iops. tenant isolation function may not functional!!",
                  K(cur_directive), K(ret));
      }
      // ignore ret, continue
    }
    if (OB_SUCC(ret)) {
      share::OBGroupIOInfo other_io_info;
      if (OB_FAIL(other_io_info.init(other_group_directive.min_iops_,
                                    other_group_directive.max_iops_,
                                    other_group_directive.weight_iops_))) {
        LOG_ERROR("fail init other group io info", K(other_group_directive), K(ret));
      } else if (OB_FAIL(GCTX.cgroup_ctrl_->set_group_iops(
                        other_group_directive.tenant_id_,
                        other_group_directive.level_,
                        other_group_directive.group_id_,
                        other_io_info))) {
        LOG_ERROR("fail set iops. tenant isolation function may not functional!!",
                  K(other_group_directive), K(ret));
      } else if (OB_FAIL(refresh_tenant_group_io_config(tenant_id))) {
        LOG_WARN("refresh tenant io config failed", K(ret), K(tenant_id));
      }
    }
  }
  return ret;
}

int ObResourcePlanManager::refresh_tenant_group_io_config(const uint64_t tenant_id) {
  int ret = OB_SUCCESS;
  ObRefHolder<ObTenantIOManager> tenant_holder;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_TENANT_ID;
    LOG_WARN("invalid config", K(ret), K(tenant_id));
  } else if (OB_FAIL(OB_IO_MANAGER.get_tenant_io_manager(tenant_id, tenant_holder))) {
    LOG_WARN("get tenant io manager failed", K(ret), K(tenant_id));
  } else if (OB_FAIL(tenant_holder.get_ptr()->refresh_group_io_config())) {
    LOG_WARN("refresh group io config failed", K(ret), K(tenant_id));
  }
  return ret;
}