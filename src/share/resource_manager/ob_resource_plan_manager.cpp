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
#include "share/io/ob_io_manager.h"
#include "share/resource_manager/ob_resource_manager_proxy.h"
#include "share/resource_manager/ob_resource_manager.h"
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
    if (OB_SUCC(ret)) {
      if (OB_FAIL(tenant_plan_map_.set_refactored(tenant_id, cur_plan, 1))) {  // overrite
        LOG_WARN("set plan failed", K(ret), K(tenant_id));
      } else {
        LOG_INFO("switch resource plan success", K(tenant_id), K(origin_plan), K(cur_plan));
      }
    }
  }
  return ret;
}

int ObResourcePlanManager::refresh_global_background_cpu()
{
  int ret = OB_SUCCESS;
  if (GCONF.enable_global_background_resource_isolation && GCTX.cgroup_ctrl_->is_valid()) {
    double cpu = static_cast<double>(GCONF.global_background_cpu_quota);
    if (cpu <= 0) {
      cpu = -1;
    }
    if (cpu >= 0 && OB_FAIL(GCTX.cgroup_ctrl_->set_cpu_shares(  // set cgroup/background/cpu.shares
                        OB_INVALID_TENANT_ID,
                        cpu,
                        OB_INVALID_GROUP_ID,
                        true /* is_background */))) {
      LOG_WARN("fail to set background cpu shares", K(ret));
    }
    int compare_ret = 0;
    if (OB_SUCC(ret) && OB_SUCC(GCTX.cgroup_ctrl_->compare_cpu(background_quota_, cpu, compare_ret))) {
      if (0 == compare_ret) {
        // do nothing
      } else if (OB_FAIL(GCTX.cgroup_ctrl_->set_cpu_cfs_quota(  // set cgroup/background/cpu.cfs_quota_us
                     OB_INVALID_TENANT_ID,
                     cpu,
                     OB_INVALID_GROUP_ID,
                     true /* is_background */))) {
        LOG_WARN("fail to set background cpu cfs quota", K(ret));
      } else {
        if (compare_ret < 0) {
          const int64_t phy_cpu_cnt = sysconf(_SC_NPROCESSORS_ONLN);
          int tmp_ret = OB_SUCCESS;
          omt::TenantIdList ids;
          GCTX.omt_->get_tenant_ids(ids);
          for (uint64_t i = 0; i < ids.size(); i++) {
            uint64_t tenant_id = ids[i];
            if (is_sys_tenant(tenant_id) || is_meta_tenant(tenant_id)) {
              // do nothing
              // meta tenant and sys tenant are unlimited
            } else {
              double target_cpu = -1;
              if (OB_DTL_TENANT_ID == tenant_id) {
                target_cpu = (phy_cpu_cnt <= 4) ? 1.0 : OB_DTL_CPU;
              } else if (OB_DATA_TENANT_ID == tenant_id) {
                target_cpu = (phy_cpu_cnt <= 4) ? 1.0 : OB_DATA_CPU;
              } else if (!is_virtual_tenant_id(tenant_id)) {
                MTL_SWITCH(tenant_id)
                {
                  target_cpu = MTL_CTX()->unit_max_cpu();
                }
              }
              if (OB_TMP_FAIL(GCTX.cgroup_ctrl_->compare_cpu(target_cpu, cpu, compare_ret))) {
                LOG_WARN_RET(tmp_ret, "compare tenant cpu failed", K(tmp_ret), K(tenant_id));
              } else if (compare_ret > 0) {
                target_cpu = cpu;
              }
              if (OB_TMP_FAIL(GCTX.cgroup_ctrl_->set_cpu_cfs_quota(
                      tenant_id, target_cpu, OB_INVALID_GROUP_ID, true /* is_background */))) {
                LOG_WARN_RET(tmp_ret, "set tenant cpu cfs quota failed", K(tmp_ret), K(tenant_id));
              }
            }
          }
        }
      }
      if (OB_SUCC(ret) && 0 != compare_ret) {
        background_quota_ = cpu;
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
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_TENANT_ID;
    LOG_WARN("invalid config", K(ret), K(tenant_id));
  } else if (OB_FAIL(switch_resource_plan(tenant_id, plan_name))) {
    LOG_WARN("check resource plan failed", K(ret), K(tenant_id), K(plan_name));
  } else if (OB_FAIL(proxy.get_all_plan_directives(tenant_id, plan_name, directives))) {
    LOG_WARN("fail get plan directive", K(tenant_id), K(plan_name), K(ret));
  } else if (OB_FAIL(normalize_iops_directives(tenant_id, directives, other_directive))) {
    LOG_WARN("fail normalize iops directive", K(ret));
  } else if (OB_FAIL(normalize_net_bandwidth_directives(tenant_id, directives, other_directive))) {
    LOG_WARN("fail normalize net bandwidthdirective", K(ret));
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
    } else if (OB_FAIL(flush_directive_to_cgroup_fs(directives))) {  // for CPU
      LOG_WARN("fail flush directive to cgroup fs", K(ret));
    }
    (void) clear_deleted_directives(tenant_id, directives);
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

int64_t ObResourcePlanManager::to_string(char *buf, const int64_t len) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_SUCC(databuff_printf(buf, len, pos, "background_quota:%d, tenant_plan_map:", background_quota_))) {
    if (OB_SUCC(databuff_printf(buf, len, pos, "{"))) {
      common::hash::ObHashMap<uint64_t, ObResMgrVarcharValue>::PrintFunctor fn(buf, len, pos);
      if (OB_SUCC(tenant_plan_map_.foreach_refactored(fn))) {
        ret = databuff_printf(buf, len, pos, "}");
      }
    }
  }
  return pos;
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
      if (OB_UNLIKELY(!is_resource_manager_group(cur_directive.group_id_))) {
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

int ObResourcePlanManager::normalize_net_bandwidth_directives(const uint64_t tenant_id,
                                                              ObPlanDirectiveSet &directives,
                                                              ObPlanDirective &other_group_directive)
{
  int ret = OB_SUCCESS;

  uint64_t total_weight = 0;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_TENANT_ID;
    LOG_WARN("invalid config", K(ret), K(tenant_id));
  } else {
    // step 1. sum total net bandwidth weight
    for (int64_t i = 0; OB_SUCC(ret) && i < directives.count(); ++i) {
      ObPlanDirective &cur_directive = directives.at(i);
      if (OB_UNLIKELY(!is_resource_manager_group(cur_directive.group_id_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error!!!", K(cur_directive));
      } else if (OB_UNLIKELY(!cur_directive.is_valid())) {
        ret = OB_INVALID_CONFIG;
        LOG_WARN("invalid group net bandwidth config", K(cur_directive));
      } else {
        total_weight += cur_directive.net_bandwidth_weight_;
      }
    }
    total_weight += OTHER_GROUPS_NET_BANDWIDTH_WEIGHT; //OTHER GROUPS WEIGHT
    // step 2. compute real net bandwidth weight
    if(OB_SUCC(ret) && total_weight > 0) {
      for (int64_t i = 0; i < directives.count(); ++i) {
        ObPlanDirective &cur_directive = directives.at(i);
        cur_directive.net_bandwidth_weight_ = int64_t(100 * (double(cur_directive.net_bandwidth_weight_) / double(total_weight)));
      }
      other_group_directive.net_bandwidth_weight_ = int64_t(100 * (double(OTHER_GROUPS_NET_BANDWIDTH_WEIGHT) / double(total_weight)));
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
    if (OB_FAIL(GCTX.cgroup_ctrl_->set_cpu_shares(d.tenant_id_, d.mgmt_p1_ / 100, d.group_id_))) {
      LOG_ERROR("fail set cpu shares. tenant isolation function may not functional!!", K(d), K(ret));
    } else {
      double tenant_cpu_quota = 0;
      if (OB_FAIL(GCTX.cgroup_ctrl_->get_cpu_cfs_quota(d.tenant_id_, tenant_cpu_quota, OB_INVALID_GROUP_ID))) {
        LOG_WARN("fail get cpu quota", K(d), K(ret));
      } else if (OB_FAIL(GCTX.cgroup_ctrl_->set_cpu_cfs_quota_(d.tenant_id_,
                     -1 == tenant_cpu_quota ? -1 : tenant_cpu_quota * d.utilization_limit_ / 100,
                     d.group_id_))) {
        LOG_ERROR(
            "fail set cpu quota. tenant isolation function may not functional!!", K(ret), K(d), K(tenant_cpu_quota));
      }
      if (OB_SUCC(ret) && GCONF.enable_global_background_resource_isolation) {
        if (OB_FAIL(GCTX.cgroup_ctrl_->get_cpu_cfs_quota(
                d.tenant_id_, tenant_cpu_quota, OB_INVALID_GROUP_ID, true /* is_background */))) {
          LOG_WARN("fail get cpu quota", K(d), K(ret));
        } else if (OB_FAIL(GCTX.cgroup_ctrl_->set_cpu_cfs_quota_(d.tenant_id_,
                       -1 == tenant_cpu_quota ? -1 : tenant_cpu_quota * d.utilization_limit_ / 100,
                       d.group_id_,
                       true /* is_background */))) {
          LOG_ERROR(
              "fail set cpu quota. tenant isolation function may not functional!!", K(ret), K(d), K(tenant_cpu_quota));
        }
      }
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
  ObRefHolder<ObTenantIOManager> tenant_holder;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_TENANT_ID;
    LOG_WARN("invalid config", K(ret), K(tenant_id));
  } else if (OB_FAIL(OB_IO_MANAGER.get_tenant_io_manager(tenant_id, tenant_holder))) {
    LOG_WARN("get tenant io manager failed", K(ret), K(tenant_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < directives.count(); ++i) {
      const ObPlanDirective &cur_directive = directives.at(i);
      share::ObGroupIOInfo cur_io_info;
      if (OB_FAIL(cur_io_info.init(cur_directive.group_name_.get_value().ptr(), cur_directive.min_iops_, cur_directive.max_iops_, cur_directive.weight_iops_,
                                   cur_directive.max_net_bandwidth_, cur_directive.net_bandwidth_weight_))) {
        LOG_ERROR("fail init group io info", K(cur_directive), K(ret));
      } else if (OB_FAIL(tenant_holder.get_ptr()->modify_io_config(cur_directive.group_id_,
                     cur_io_info.group_name_,
                     cur_io_info.min_percent_,
                     cur_io_info.max_percent_,
                     cur_io_info.weight_percent_,
                     cur_io_info.max_net_bandwidth_percent_,
                     cur_io_info.net_bandwidth_weight_percent_))) {
        LOG_ERROR("fail set iops. tenant isolation function may not functional!!",
                  K(cur_directive), K(ret));
      } else {
        LOG_INFO("set group iops", K(ret), K(tenant_id), K(cur_directive.group_id_), K(cur_io_info));
      }
    }
    if (OB_SUCC(ret)) {
      share::ObGroupIOInfo other_io_info;
      if (OB_FAIL(other_io_info.init(other_group_directive.group_name_.get_value().ptr(),
                                     other_group_directive.min_iops_,
                                     other_group_directive.max_iops_,
                                     other_group_directive.weight_iops_,
                                     other_group_directive.max_net_bandwidth_, other_group_directive.net_bandwidth_weight_))) {
        LOG_ERROR("fail init other group io info", K(other_group_directive), K(ret));
      } else if (OB_FAIL(tenant_holder.get_ptr()->modify_io_config(other_group_directive.group_id_,
                     other_io_info.group_name_,
                     other_io_info.min_percent_,
                     other_io_info.max_percent_,
                     other_io_info.weight_percent_,
                     other_io_info.max_net_bandwidth_percent_,
                     other_io_info.net_bandwidth_weight_percent_))) {
        LOG_ERROR("fail set iops. tenant isolation function may not functional!!",
                  K(other_group_directive), K(ret));
      } else if (OB_FAIL(tenant_holder.get_ptr()->refresh_group_io_config())) {
        LOG_WARN("refresh tenant io config failed", K(ret), K(tenant_id));
      }
    }
  }
  return ret;
}


int ObResourcePlanManager::clear_deleted_directives(const uint64_t tenant_id,
                                                           ObPlanDirectiveSet &directives)
{
  int ret = OB_SUCCESS;

  ObRefHolder<ObTenantIOManager> tenant_holder;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_TENANT_ID;
    LOG_WARN("invalid config", K(ret), K(tenant_id));
  } else if (OB_FAIL(OB_IO_MANAGER.get_tenant_io_manager(tenant_id, tenant_holder))) {
    LOG_WARN("get tenant io manager failed", K(ret), K(tenant_id));
  }

  if (OB_SUCC(ret)) {
    common::ObSEArray<share::ObTenantGroupIdKey, 16> group_id_keys;
    common::ObSEArray<ObGroupName, 16> group_names;
        common::hash::ObHashMap<ObTenantGroupIdKey, ObGroupName> &group_id_name_map =
        G_RES_MGR.get_mapping_rule_mgr().get_group_id_name_map();
    ObResourceMappingRuleManager::GetTenantGroupIdNameFunctor functor(tenant_id, group_id_keys, group_names);
    if (OB_FAIL(group_id_name_map.foreach_refactored(functor))) {
      LOG_WARN("failed to do foreach", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < group_id_keys.count(); ++i) {
        bool is_group_id_found = false;
        for (int64_t j = 0; !is_group_id_found && j < directives.count(); ++j) {
          const share::ObPlanDirective &cur_directive = directives.at(j);
          if (cur_directive.group_id_ == group_id_keys.at(i).group_id_) {
            is_group_id_found = true;
          }
        }
        if (!is_group_id_found) {
          const uint64_t deleted_group_id = group_id_keys.at(i).group_id_;
          if (OB_FAIL(tenant_holder.get_ptr()->reset_consumer_group_config(deleted_group_id))) {
            LOG_WARN("reset consumer group config failed", K(ret), K(deleted_group_id));
          } else if (!GCTX.cgroup_ctrl_->is_valid()) {
            // do nothing
          } else if (OB_FAIL(GCTX.cgroup_ctrl_->set_cpu_shares(tenant_id, 1, deleted_group_id))) {
            LOG_WARN("fail to set cpu share", K(ret), K(tenant_id), K(deleted_group_id));
          } else if (OB_FAIL(GCTX.cgroup_ctrl_->set_cpu_cfs_quota(tenant_id, -1, deleted_group_id))) {
            LOG_WARN("fail to set cpu quota", K(ret), K(tenant_id), K(deleted_group_id));
          } else {
            LOG_INFO("directive cleared", K(tenant_id), K(deleted_group_id));
          }
        }
      }
    }
  }
  return ret;
}
