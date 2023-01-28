// Copyright 2022 Alibaba Inc. All Rights Reserved.
// Author:
//  dachuan.sdc@antgroup.com
// This file is for resource mapping rule cache module.



#define USING_LOG_PREFIX SHARE
#include "ob_resource_col_mapping_rule_manager.h"
#include "lib/string/ob_string.h"
#include "share/resource_manager/ob_resource_manager_proxy.h"
#include "share/resource_manager/ob_cgroup_ctrl.h"
#include "observer/ob_server_struct.h"
#include "sql/ob_sql.h"


using namespace oceanbase::common;
using namespace oceanbase::share;

int ObResourceColMappingRuleManager::init()
{
  int ret = OB_SUCCESS;
  // number of columns with mapping rule.
  int rule_bucket_size = 4096;
  int rule_group_bucket_size = 40960;
  int tenant_bucket_size = 100;
  if (OB_UNLIKELY(rule_id_map_.created() || group_id_map_.created())) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(rule_id_map_.create(rule_bucket_size, "ColRuleMap", "ColRuleMapNode"))) {
    LOG_WARN("fail create rule map", K(ret));
  } else if (OB_FAIL(group_id_map_.create(rule_group_bucket_size, "RuleGrpIdMap", "RuleGrpIdNode"))) {
    LOG_WARN("fail create group map", K(ret));
  } else if (OB_FAIL(tenant_version_.create(tenant_bucket_size, "RuleVersionMap", "RuleVersionNode"))) {
    LOG_WARN("fail create group map", K(ret));
  }
  LOG_INFO("init resource column mapping rule manager", K(ret));
  return ret;
}

int ObResourceColMappingRuleManager::refresh_resource_column_mapping_rule(uint64_t tenant_id,
                                                                          sql::ObPlanCache *plan_cache,
                                                                          const common::ObString &plan)
{
  int ret = OB_SUCCESS;
  ObResourceManagerProxy proxy;
  int64_t inner_table_version = 0;
  int64_t cache_version = get_column_mapping_version(tenant_id);
  if (OB_FAIL(proxy.get_resource_mapping_version(tenant_id, inner_table_version))) {
    LOG_WARN("get resource mapping version failed", K(ret));
  } else if (cache_version >= inner_table_version) {
    LOG_TRACE("local version is latest, don't need refresh", K(tenant_id), K(cache_version), K(inner_table_version));
  } else if (OB_ISNULL(plan_cache)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get plan cache failed", K(ret), K(tenant_id));
  } else if (lock_.trylock()) {
    LOG_INFO("local version is not latest, need refresh", K(tenant_id), K(cache_version), K(inner_table_version));
    // update cache
    ObArray<ObResourceColumnMappingRule> rules;
    if (OB_ISNULL(GCTX.cgroup_ctrl_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cgroup ctrl is null", K(ret));
    } else if (!GCTX.cgroup_ctrl_->is_valid()) {
      ret = OB_EAGAIN;
      // cgroup ctrl 没有初始化成功，可能是没有 cgroup fs、没有权限等原因
      // 此时不再继续后继资源隔离操作
    } else if (OB_FAIL(proxy.get_all_resource_mapping_rules_by_column(tenant_id, plan, rules))) {
      LOG_WARN("fail get resource mapping rules by column", K(tenant_id), K(ret));
    } else {
      //TODO: remove elements not in the array which means not in inner table.
      uint64_t current_time = static_cast<uint64_t>(ObTimeUtility::current_time());
      for (int64_t i = 0; i < rules.count() && OB_SUCC(ret); ++i) {
        ObResourceColumnMappingRule &rule = rules.at(i);
        uint64_t rule_id = 0;
        if (OB_FAIL(rule_id_map_.get_refactored(
                  ColumnNameKey(tenant_id, rule.database_id_, rule.table_name_, rule.column_name_, rule.case_mode_),
                  rule_id))) {
          if (OB_HASH_NOT_EXIST == ret) {
            //use current_time as new rule id and insert into rule_id_map_.
            rule_id = current_time;
            current_time++;
            if (OB_FAIL(rule_id_map_.set_refactored(
                  ColumnNameKey(tenant_id, rule.database_id_, rule.table_name_, rule.column_name_, rule.case_mode_),
                  rule_id))) {
              rule.reset();
              LOG_WARN("rule id map set refactored failed", K(ret));
            } else if (OB_FAIL(plan_cache->evict_plan_by_table_name(rule.database_id_, rule.table_name_))) {
              ret = OB_SUCCESS;
              LOG_ERROR("evict plan by table name failed", K(ret), K(tenant_id),
                        K(rule.database_id_), K(rule.table_name_));
            }
          } else {
            rule.reset();
            LOG_WARN("get refactored from rule id map failed", K(ret));
          }
        } else {
          rule.reset_table_column_name();
        }
        RuleValue rule_value;
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(group_id_map_.get_refactored(
                      RuleValueKey(tenant_id, rule_id, rule.user_name_, rule.literal_value_),
                      rule_value))) {
          if (OB_HASH_NOT_EXIST == ret) {
            if (OB_FAIL(group_id_map_.set_refactored(
                      RuleValueKey(tenant_id, rule_id, rule.user_name_, rule.literal_value_),
                      RuleValue(rule.group_id_, rule.user_name_, rule.literal_value_)))) {
              rule.reset_user_name_literal();
              LOG_WARN("group id map set refactored failed", K(ret));
            }
          } else {
            rule.reset_user_name_literal();
            LOG_WARN("group id map set refactored failed", K(ret));
          }
        } else if (rule_value.group_id_ == rule.group_id_) {
          //not updated, just ignore.
          // release memory of user_name and literal_value in ObResourceColumnMappingRule
          rule.reset_user_name_literal();
        } else if (OB_FAIL(group_id_map_.set_refactored(
                      RuleValueKey(tenant_id, rule_id, rule.user_name_, rule.literal_value_),
                      RuleValue(rule.group_id_, rule.user_name_, rule.literal_value_),
                      1 /* overwrite*/, 0, 0 /* overwrite_key*/))) {
          LOG_WARN("group id map set refactored failed", K(ret));
        } else {
          // parameter overwrite_key of set_refactored is unused, key is always overwrited when value is overwrited.
          // So we record ptrs in value, and free ptrs recorded in original value when overwrite key and value.
          rule_value.release_memory();
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(tenant_version_.set_refactored(tenant_id, inner_table_version, 1))) {
          LOG_WARN("update tenant cache version failed", K(ret));
        }
      }
      LOG_INFO("refresh_resource_column_mapping_rule",
               K(ret), K(tenant_id), K(inner_table_version),
               K(plan), K(rules), "rule_count", group_id_map_.size());
    }
    lock_.unlock();
  } else {
    LOG_ERROR("get lock of resource mapping rule cache failed.", K(tenant_id));
  }
  return ret;
}

uint64_t ObResourceColMappingRuleManager::get_column_mapping_rule_id(uint64_t tenant_id,
                                                        uint64_t database_id,
                                                        const common::ObString &table_name,
                                                        const common::ObString &column_name,
                                                        common::ObNameCaseMode case_mode) const
{
  int ret = common::OB_SUCCESS;
  uint64_t rule_id = OB_INVALID_ID;
  if (OB_FAIL(rule_id_map_.get_refactored(
            ColumnNameKey(tenant_id, database_id, table_name, column_name, case_mode), rule_id))) {
    rule_id = OB_INVALID_ID;
  }
  LOG_TRACE("get_column_mapping_rule_id", K(tenant_id), K(database_id), K(table_name), K(column_name), K(case_mode), K(rule_id));
  return rule_id;
}
uint64_t ObResourceColMappingRuleManager::get_column_mapping_group_id(uint64_t tenant_id,
                                       uint64_t rule_id, const ObString &user_name,
                                       const common::ObString &literal_value)
{
  int ret = common::OB_SUCCESS;
  RuleValue rule_value;
  if (OB_FAIL(group_id_map_.get_refactored(
            RuleValueKey(tenant_id, rule_id, user_name, literal_value), rule_value))) {
    if (OB_HASH_NOT_EXIST == ret) {
      if (OB_FAIL(group_id_map_.get_refactored(
            RuleValueKey(tenant_id, rule_id, ObString(), literal_value), rule_value))) {
        // return invalid group id if not found, and then use current thread execute this query.
        rule_value.group_id_ = OB_INVALID_ID;
      }
    } else {
      rule_value.group_id_ = OB_INVALID_ID;
    }
  }
  LOG_TRACE("get_column_mapping_group_id", K(tenant_id), K(rule_id), K(user_name), K(literal_value), K(rule_value.group_id_));
  return rule_value.group_id_;
}

int64_t ObResourceColMappingRuleManager::get_column_mapping_version(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  int64_t version = 0;
  if (OB_FAIL(tenant_version_.get_refactored(tenant_id, version))) {
    LOG_WARN("get tenant column mapping version failed", K(tenant_id), K(ret));
    version = 0;
  }
  return version;
}
