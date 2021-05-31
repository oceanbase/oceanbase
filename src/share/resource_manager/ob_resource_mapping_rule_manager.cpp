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
#include "observer/omt/ob_cgroup_ctrl.h"
#include "observer/ob_server_struct.h"

using namespace oceanbase::common;
using namespace oceanbase::share;

int ObResourceMappingRuleManager::init()
{
  int ret = OB_SUCCESS;
  int rule_bucket_size = 4096;
  int group_bucket_size = 512;
  if (rule_map_.created() || group_map_.created()) {
    ret = OB_INIT_TWICE;
    LOG_WARN("mapping rule manager should not init multiple times", K(ret));
  } else if (OB_FAIL(rule_map_.create(rule_bucket_size, "UsrRuleMap", "UsrRuleMapNode"))) {
    LOG_WARN("fail create rule map", K(ret));
  } else if (OB_FAIL(group_map_.create(group_bucket_size, "GrpIdNameMap", "GrpIdNameNode"))) {
    LOG_WARN("fail create group map", K(ret));
  }
  LOG_INFO("resource plan manager init ok");
  return ret;
}

int ObResourceMappingRuleManager::refresh_resource_mapping_rule(uint64_t tenant_id, const ObString& plan)
{
  int ret = OB_SUCCESS;
  ObResourceManagerProxy proxy;
  ObResourceUserMappingRuleSet rules;
  if (OB_ISNULL(GCTX.cgroup_ctrl_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cgroup ctrl is null", K(ret));
  } else if (!GCTX.cgroup_ctrl_->is_valid()) {
    ret = OB_EAGAIN;
  } else if (OB_FAIL(proxy.get_all_resource_mapping_rules_by_user(tenant_id, plan, rules))) {
    LOG_WARN("fail get resource mapping rules", K(tenant_id), K(ret));
  } else {
    for (int64_t i = 0; i < rules.count() && OB_SUCC(ret); ++i) {
      ObResourceUserMappingRule& rule = rules.at(i);
      if (OB_FAIL(group_map_.set_refactored(
              combine_id(rule.tenant_id_, rule.group_id_), rule.group_name_, 0 /* don't overwrite */))) {
        if (OB_HASH_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail set group mapping to group_map", K(rule), K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(rule_map_.set_refactored(
                combine_id(rule.tenant_id_, rule.user_id_), rule.group_id_, 1 /* overwrite on dup key */))) {
          LOG_WARN("fail set user mapping rule to rule_map", K(rule), K(ret));
        }
      }
    }
    LOG_INFO("refresh_resource_mapping_rule", K(tenant_id), K(plan), K(rules));
  }
  return ret;
}
