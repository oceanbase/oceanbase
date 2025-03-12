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
#include "share/resource_manager/ob_resource_manager_proxy.h"
#include "share/io/ob_io_manager.h"
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
                  ObTenantGroupIdKey(rule.tenant_id_, rule.group_id_),
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
    (void)clear_deleted_group(tenant_id, rules);
  }
  return ret;
}

int ObResourceMappingRuleManager::clear_deleted_group(
    const uint64_t tenant_id, const ObResourceUserMappingRuleSet &rules)
{
  int ret = OB_SUCCESS;

  ObRefHolder<ObTenantIOManager> tenant_holder;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_TENANT_ID;
    LOG_WARN("invalid config", K(ret), K(tenant_id));
  } else if (OB_FAIL(OB_IO_MANAGER.get_tenant_io_manager(tenant_id, tenant_holder))) {
    LOG_WARN("get tenant io manager failed", K(ret), K(tenant_id));
  } else {
    ObSEArray<share::ObTenantGroupIdKey, 16> group_id_keys;
    ObSEArray<ObGroupName, 16> group_names;
    GetTenantGroupIdNameFunctor functor(tenant_id, group_id_keys, group_names);
    if (OB_FAIL(group_id_name_map_.foreach_refactored(functor))) {
      LOG_WARN("failed to do foreach", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < group_id_keys.count(); ++i) {
        bool is_group_id_found = false;
        for (int64_t j = 0; !is_group_id_found && j < rules.count(); ++j) {
          const ObResourceUserMappingRule &rule = rules.at(j);
          if (share::ObTenantGroupIdKey(rule.tenant_id_, rule.group_id_) == group_id_keys.at(i)) {
            is_group_id_found = true;
          }
        }
        if (!is_group_id_found) {
          uint64_t deleted_group_id = group_id_keys.at(i).group_id_;
          ObGroupName deleted_group_name = group_names.at(i);
          LOG_INFO("group_id need to be cleared", K(tenant_id), K(deleted_group_id), K(deleted_group_name));
          if (GCTX.cgroup_ctrl_->is_valid()) {
            if (OB_FAIL(GCTX.cgroup_ctrl_->remove_cgroup(tenant_id, deleted_group_id))) {
              LOG_WARN("failed to remove cgroup", K(ret), K(tenant_id), K(deleted_group_id));
            }
          }
          if (OB_SUCC(ret)) {
            if (OB_FAIL(tenant_holder.get_ptr()->delete_consumer_group_config(deleted_group_id))) {
              LOG_WARN("delete consumer group config failed", K(ret), K(tenant_id), K(deleted_group_id));
            } else if (OB_FAIL(group_id_name_map_.erase_refactored(group_id_keys.at(i)))) {
              LOG_WARN("fail erase group mapping from group_map", K(deleted_group_id), K(ret));
            } else if (OB_FAIL(
                          group_name_id_map_.erase_refactored(share::ObTenantGroupKey(tenant_id, deleted_group_name)))) {
              LOG_WARN("fail erase group name mapping from group id", K(deleted_group_name), K(ret));
            }
          }
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
    (void)clear_resource_user_mapping_rule(tenant_id, user_rules);
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
    (void)clear_resource_function_mapping_rule(tenant_id, rules);
    LOG_INFO("refresh resource mapping rule", K(tenant_id), K(plan), K(rules));
  }
  return ret;
}


int ObResourceMappingRuleManager::clear_resource_function_mapping_rule(const uint64_t tenant_id,
    const ObResourceMappingRuleSet &rules)
{
  int ret = OB_SUCCESS;
  ObSEArray<share::ObTenantFunctionKey, 16> func_keys;
  ObSEArray<uint64_t, 16> group_ids;
  GetTenantFunctionRuleFunctor functor(tenant_id, func_keys, group_ids);
  if (OB_FAIL(function_rule_map_.foreach_refactored(functor))) {
    LOG_WARN("failed to do foreach", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < func_keys.count(); ++i) {
      bool is_group_id_found = false;
      for (int64_t j = 0; !is_group_id_found && j < rules.count(); ++j) {
        const ObResourceMappingRule &rule = rules.at(j);
        if (share::ObTenantFunctionKey(rule.tenant_id_, rule.value_) == func_keys.at(i)) {
          is_group_id_found = true;
        }
      }
      if (!is_group_id_found) {
        LOG_INFO("tenant function need to be cleared", "function", func_keys.at(i), "group_id", group_ids.at(i));
        if (OB_FAIL(function_rule_map_.erase_refactored(func_keys.at(i)))) {
          LOG_WARN("failed to reset user map", K(ret), K(func_keys.at(i)));
        }
      }
    }
  }
  return ret;
}

int ObResourceMappingRuleManager::clear_resource_user_mapping_rule(const uint64_t tenant_id,
    const ObResourceUserMappingRuleSet &rules)
{
  int ret = OB_SUCCESS;
  ObSEArray<sql::ObTenantUserKey, 16> user_keys;
  ObSEArray<uint64_t, 16> group_ids;
  GetTenantUserRuleFunctor functor(tenant_id, user_keys, group_ids);
  if (OB_FAIL(user_rule_map_.foreach_refactored(functor))) {
    LOG_WARN("failed to do foreach", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < user_keys.count(); ++i) {
      bool is_group_id_found = false;
      for (int64_t j = 0; !is_group_id_found && j < rules.count(); ++j) {
        const ObResourceUserMappingRule &rule = rules.at(j);
        if (sql::ObTenantUserKey(rule.tenant_id_, rule.user_id_) == user_keys.at(i)) {
          is_group_id_found = true;
        }
      }
      if (!is_group_id_found) {
        LOG_INFO("tenant user group need to be cleared", "user", user_keys.at(i), "group_id", group_ids.at(i));
        if (OB_FAIL(user_rule_map_.erase_refactored(user_keys.at(i)))) {
          LOG_WARN("failed to reset user map", K(ret), K(user_keys.at(i)));
        }
      }
    }
  }
  return ret;
}

int64_t ObResourceMappingRuleManager::to_string(char* buf, int64_t len) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_SUCC(ret)) {
    ret = databuff_printf(buf, len, pos, "user_rule_map:");
    if (OB_SUCC(ret)) {
      if (OB_SUCC(databuff_printf(buf, len, pos, "{"))) {
        common::hash::ObHashMap<sql::ObTenantUserKey, uint64_t>::PrintFunctor fn1(buf, len, pos);
        if (OB_SUCC(user_rule_map_.foreach_refactored(fn1))) {
          ret = databuff_printf(buf, len, pos, "} ");
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    ret = databuff_printf(buf, len, pos, "function_rule_map:");
    if (OB_SUCC(ret)) {
      if (OB_SUCC(databuff_printf(buf, len, pos, "{"))) {
        common::hash::ObHashMap<share::ObTenantFunctionKey, uint64_t>::PrintFunctor fn2(buf, len, pos);
        if (OB_SUCC(function_rule_map_.foreach_refactored(fn2))) {
          ret = databuff_printf(buf, len, pos, "} ");
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    ret = databuff_printf(buf, len, pos, "group_id_name_map:");
    if (OB_SUCC(ret)) {
      if (OB_SUCC(databuff_printf(buf, len, pos, "{"))) {
        common::hash::ObHashMap<ObTenantGroupIdKey, ObGroupName>::PrintFunctor fn3(buf, len, pos);
        if (OB_SUCC(group_id_name_map_.foreach_refactored(fn3))) {
          ret = databuff_printf(buf, len, pos, "} ");
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    ret = databuff_printf(buf, len, pos, "group_name_id_map:");
    if (OB_SUCC(ret)) {
      if (OB_SUCC(databuff_printf(buf, len, pos, "{"))) {
        common::hash::ObHashMap<share::ObTenantGroupKey, uint64_t>::PrintFunctor fn4(buf, len, pos);
        if (OB_SUCC(group_name_id_map_.foreach_refactored(fn4))) {
          ret = databuff_printf(buf, len, pos, "}");
        }
      }
    }
  }
  if (OB_SUCCESS != ret) {
    pos = 0;
    databuff_printf(buf, len, pos, "{...}");
  }

  return pos;
}
