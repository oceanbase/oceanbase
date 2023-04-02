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
#include "ob_resource_mapping_rule_manager.h"
#include "lib/string/ob_string.h"
#include "share/resource_manager/ob_resource_manager_proxy.h"
#include "share/resource_manager/ob_cgroup_ctrl.h"
#include "observer/ob_server_struct.h"


using namespace oceanbase::common;
using namespace oceanbase::share;

int ObResourceMappingRuleManager::init()
{
  int ret = OB_SUCCESS;
  int rule_bucket_size = 4096;
  int group_bucket_size = 512;
  if (user_rule_map_.created() || group_id_name_map_.created() ||
      function_rule_map_.created() || group_name_id_map_.created()) {
    ret = OB_INIT_TWICE;
    LOG_WARN("mapping rule manager should not init multiple times", K(ret));
  } else if (OB_FAIL(user_rule_map_.create(rule_bucket_size, "UsrRuleMap", "UsrRuleMapNode"))) {
    // 整个集群的用户数，一般来说不会很大，4K 的空间足够
    LOG_WARN("fail create rule map", K(ret));
  } else if (OB_FAIL(group_id_name_map_.create(group_bucket_size, "GrpIdNameMap", "GrpIdNameNode"))) {
    // 整个集群的 group 数，一般来说不会很大，512 的空间足够
    LOG_WARN("fail create group map", K(ret));
  } else if (OB_FAIL(function_rule_map_.create(group_bucket_size, "FuncRuleMap", "FuncRuleNode"))) {
    LOG_WARN("fail create function rule map", K(ret));
  } else if (OB_FAIL(group_name_id_map_.create(group_bucket_size, "GrpNameIdMap", "GrpNameIdNode"))) {
    LOG_WARN("fail create name id map", K(ret));
  }
  LOG_INFO("resource mapping rule manager init ok");
  return ret;
}

int ObResourceMappingRuleManager::refresh_group_mapping_rule(const uint64_t tenant_id, const ObString &plan)
{
  int ret = OB_SUCCESS;
  ObResourceManagerProxy proxy;
  ObResourceUserMappingRuleSet rules;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_TENANT_ID;
    LOG_WARN("invalid config", K(ret), K(tenant_id));
  } else if (OB_FAIL(proxy.get_all_group_info(tenant_id, plan, rules))) {
    LOG_WARN("fail get group infos", K(tenant_id), K(ret));
  } else {
    for (int64_t i = 0; i < rules.count() && OB_SUCC(ret); ++i) {
      ObResourceUserMappingRule &rule = rules.at(i);
      if (OB_FAIL(group_id_name_map_.set_refactored(
                  combine_two_ids(rule.tenant_id_, rule.group_id_),
                  rule.group_name_,
                  0 /* don't overwrite */))) {
        if (OB_HASH_EXIST == ret) {
          // group_id 和 group_name 的映射是固定的，不会变化，所以无需 overwrite
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail set group mapping to group_map", K(rule), K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(group_name_id_map_.set_refactored(
                          share::ObTenantGroupKey(rule.tenant_id_, rule.group_name_),
                          rule.group_id_,
                          1 /* overwrite */))) {
          LOG_WARN("fail set group name mapping to group id", K(rule), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObResourceMappingRuleManager::refresh_resource_mapping_rule(
    const uint64_t tenant_id,
    const ObString &plan)
{
  int ret = OB_SUCCESS;
  ObResourceManagerProxy proxy;
  //下面这些读内部表的操作，不受cgroup是否开启影响
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_TENANT_ID;
    LOG_WARN("invalid config", K(ret), K(tenant_id));
  } else if (OB_FAIL(refresh_resource_user_mapping_rule(proxy, tenant_id, plan))) {
    LOG_WARN("fail refresh user mapping rule", K(tenant_id), K(plan), K(ret));
  } else if (OB_FAIL(refresh_resource_function_mapping_rule(proxy, tenant_id, plan))) {
    LOG_WARN("fail refresh function mapping rule", K(tenant_id), K(plan), K(ret));
  } else {
    LOG_INFO("refresh resource mapping rule success", K(tenant_id), K(plan));
  }
  return ret;
}

int ObResourceMappingRuleManager::reset_mapping_rules()
{
  int ret = OB_SUCCESS;
  for (common::hash::ObHashMap<sql::ObTenantUserKey, uint64_t>::const_iterator user_iter = user_rule_map_.begin();
      OB_SUCC(ret) && user_iter != user_rule_map_.end(); ++user_iter) {
    if (OB_FAIL(user_rule_map_.set_refactored(user_iter->first, 0, 1/*overwrite*/))) {
      LOG_WARN("failed to reset user map", K(ret), K(user_iter->first));
    }
  }
  if (OB_SUCC(ret)) {
    for (common::hash::ObHashMap<share::ObTenantFunctionKey, uint64_t>::const_iterator func_iter = function_rule_map_.begin();
        OB_SUCC(ret) && func_iter != function_rule_map_.end(); ++func_iter) {
      if (OB_FAIL(function_rule_map_.set_refactored(func_iter->first, 0, 1/*overwrite*/))) {
        LOG_WARN("failed to reset user map", K(ret), K(func_iter->first));
      }
    }
  }
  return ret;
}

int ObResourceMappingRuleManager::refresh_resource_user_mapping_rule(
    ObResourceManagerProxy &proxy,
    const uint64_t tenant_id,
    const ObString &plan)
{
  int ret = OB_SUCCESS;
  ObResourceUserMappingRuleSet user_rules;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_TENANT_ID;
    LOG_WARN("invalid config", K(ret), K(tenant_id));
  } else if (OB_FAIL(proxy.get_all_resource_mapping_rules_by_user(tenant_id, plan, user_rules))) {
    LOG_WARN("fail get resource mapping user_rules", K(tenant_id), K(ret));
  } else {
    for (int64_t i = 0; i < user_rules.count() && OB_SUCC(ret); ++i) {
      ObResourceUserMappingRule &rule = user_rules.at(i);
      // 建立 user_id => group_id 的映射
      // note: 这里没有处理删除用户名之后的情况，如果频繁地建用户，给用户设定 mapping，然后删用户
      //       会导致 rule_map 里垃圾内容堆积。但是考虑到这种情况在线上环境里很少出现，暂不处理。
      //       内存占用很小(key:value = uint64:uint64)，不会有太多浪费。
      uint64_t group_id = 0;
      bool map_changed = true;
      if (OB_SUCCESS == user_rule_map_.get_refactored(
              sql::ObTenantUserKey(rule.tenant_id_, rule.user_id_), group_id)) {
        if (group_id == rule.group_id_) {
          map_changed = false; // avoid set_refactor memory fragment
        }
      }
      if (map_changed && OB_FAIL(user_rule_map_.set_refactored(
                  sql::ObTenantUserKey(rule.tenant_id_, rule.user_id_),
                  rule.group_id_,
                  1 /* overwrite on dup key */))) {
        LOG_WARN("fail set user mapping rule to rule_map", K(rule), K(ret));
      }
    }
    LOG_INFO("refresh resource user mapping rule", K(tenant_id), K(plan), K(user_rules));
  }
  return ret;
}

int ObResourceMappingRuleManager::refresh_resource_function_mapping_rule(
    ObResourceManagerProxy &proxy,
    const uint64_t tenant_id,
    const ObString &plan)
{
  int ret = OB_SUCCESS;
  ObResourceMappingRuleSet rules;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_TENANT_ID;
    LOG_WARN("invalid config", K(ret), K(tenant_id));
  } else if (OB_FAIL(proxy.get_all_resource_mapping_rules_by_function(tenant_id, plan, rules))) {
    LOG_WARN("fail get resource mapping rules", K(tenant_id), K(ret));
  } else {
    for (int64_t i = 0; i < rules.count() && OB_SUCC(ret); ++i) {
      ObResourceMappingRule &rule = rules.at(i);
      // 建立 function_name => group_id 的映射
      uint64_t group_id = 0;
      bool map_changed = true;
      if (OB_SUCCESS == function_rule_map_.get_refactored(
              share::ObTenantFunctionKey(rule.tenant_id_, rule.value_), group_id)) {
        if (rule.group_id_ == group_id) {
          // no new function mapping, don't update the function_rule_map_ to avoid memory fragment
          map_changed = false;
        }
      }
      if (map_changed && OB_FAIL(function_rule_map_.set_refactored(
                  share::ObTenantFunctionKey(rule.tenant_id_, rule.value_), /* function name */
                  rule.group_id_, /* group id */
                  1 /* overwrite on dup key */))) {
        LOG_WARN("fail set user mapping rule to rule_map", K(rule), K(ret));
      }
    }
    LOG_INFO("refresh_resource_function_mapping_rule", K(tenant_id), K(plan), K(rules));
  }

  return ret;
}
