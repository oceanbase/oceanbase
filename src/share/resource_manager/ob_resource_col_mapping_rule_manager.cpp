/**
 * Copyright (c) 2023 OceanBase
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
#include "ob_resource_col_mapping_rule_manager.h"
#include "lib/string/ob_string.h"
#include "share/resource_manager/ob_resource_manager_proxy.h"
#include "share/resource_manager/ob_cgroup_ctrl.h"
#include "observer/ob_server_struct.h"
#include "sql/ob_sql.h"


using namespace oceanbase::common;
using namespace oceanbase::share;

int ObTenantResColMappingInfo::init(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  // number of columns with mapping rule.
  int rule_bucket_size = 128;
  int rule_group_bucket_size = 128;
  tenant_id_ = tenant_id;
  allocator_.set_label("ResKeyName");
  allocator_.set_tenant_id(tenant_id);
  ObMemAttr attr1(tenant_id, "ResRuleIdMap");
  ObMemAttr attr2(tenant_id, "ResGrpIdMap");
  if (OB_UNLIKELY(rule_id_map_.created() || group_id_map_.created())) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(rule_id_map_.create(rule_bucket_size, attr1, attr1))) {
    LOG_WARN("fail create rule map", K(ret));
  } else if (OB_FAIL(group_id_map_.create(rule_group_bucket_size, attr2, attr2))) {
    LOG_WARN("fail create group map", K(ret));
  }
  return ret;
}

int ObTenantResColMappingInfo::refresh(sql::ObPlanCache *plan_cache, const common::ObString &plan)
{
  int ret = OB_SUCCESS;
  ObResourceManagerProxy proxy;
  int64_t inner_table_version = 0;
  if (OB_FAIL(proxy.get_resource_mapping_version(tenant_id_, inner_table_version))) {
    LOG_WARN("get resource mapping version failed", K(ret));
  } else if (version_ >= inner_table_version) {
    LOG_TRACE("local version is latest, don't need refresh", K(tenant_id_),
              K(version_), K(inner_table_version));
  } else if (OB_ISNULL(plan_cache)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get plan cache failed", K(ret));
  } else if (lock_.trylock()) {
    LOG_INFO("local version is not latest, need refresh", K(tenant_id_),
             K(version_), K(inner_table_version));
    // update cache
    ObArray<ObResourceColumnMappingRule> rules;
    if (OB_ISNULL(GCTX.cgroup_ctrl_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cgroup ctrl is null", K(ret));
    } else if (!GCTX.cgroup_ctrl_->is_valid()) {
      // cgroup ctrl 没有初始化成功，可能是没有 cgroup fs、没有权限等原因
      // 此时不再继续后继资源隔离操作
    } else if (OB_FAIL(proxy.get_all_resource_mapping_rules_by_column(tenant_id_, plan,
                                                                      allocator_, rules))) {
      LOG_WARN("fail get resource mapping rules by column", K(tenant_id_), K(ret));
    } else {
      //TODO: remove elements not in the array which means not in inner table.
      uint64_t current_time = static_cast<uint64_t>(ObTimeUtility::current_time());

      for (int64_t i = 0; i < rules.count() && OB_SUCC(ret); ++i) {
        ObResourceColumnMappingRule &rule = rules.at(i);
        uint64_t rule_id = 0;
        bool rule_id_map_inserted = false;
        bool group_id_map_inserted = false;
        if (OB_FAIL(rule_id_map_.get_refactored(ColumnNameKey(tenant_id_, rule.database_id_,
                  rule.table_name_, rule.column_name_, rule.case_mode_), rule_id))) {
          if (OB_HASH_NOT_EXIST == ret) {
            //use current_time as new rule id and insert into rule_id_map_.
            rule_id = current_time;
            current_time++;
            if (OB_FAIL(rule_id_map_.set_refactored(ColumnNameKey(tenant_id_, rule.database_id_,
                    rule.table_name_, rule.column_name_, rule.case_mode_), rule_id))) {
              LOG_WARN("rule id map set refactored failed", K(ret));
            } else {
              rule_id_map_inserted = true;
              if (OB_FAIL(plan_cache->evict_plan_by_table_name(rule.database_id_, rule.table_name_))) {
                ret = OB_SUCCESS;
                LOG_ERROR("evict plan by table name failed", K(ret), K(tenant_id_), K(rule));
              }
            }
          } else {
            LOG_WARN("get refactored from rule id map failed", K(ret));
          }
        }
        RuleValue rule_value;
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(group_id_map_.get_refactored(
                      RuleValueKey(rule_id, rule.user_name_, rule.literal_value_),
                      rule_value))) {
          if (OB_HASH_NOT_EXIST == ret) {
            if (OB_FAIL(group_id_map_.set_refactored(
                      RuleValueKey(rule_id, rule.user_name_, rule.literal_value_),
                      RuleValue(rule.group_id_, rule.user_name_, rule.literal_value_)))) {
              LOG_WARN("group id map set refactored failed", K(ret));
            } else {
              group_id_map_inserted = true;
            }
          } else {
            LOG_WARN("group id map set refactored failed", K(ret));
          }
        } else if (rule_value.group_id_ == rule.group_id_) {
          //not updated, just ignore.
        } else if (OB_FAIL(group_id_map_.set_refactored(
                      RuleValueKey(rule_id, rule.user_name_, rule.literal_value_),
                      RuleValue(rule.group_id_, rule.user_name_, rule.literal_value_),
                      1 /* overwrite*/, 0, 0 /* overwrite_key*/))) {
          LOG_WARN("group id map set refactored failed", K(ret));
        } else {
          group_id_map_inserted = true;
          // parameter overwrite_key of set_refactored is unused, key is always overwrited when value is overwrited.
          // So we record ptrs in value, and free ptrs recorded in original value when overwrite key and value.
          rule_value.release_memory(allocator_);
        }
        if (!rule_id_map_inserted) {
          rule.reset_table_column_name(allocator_);
        }
        if (!group_id_map_inserted) {
          rule.reset_user_name_literal(allocator_);
        }
      }
      if (OB_SUCC(ret)) {
        version_ = inner_table_version;
      }
      LOG_INFO("refresh_resource_column_mapping_rule",
               K(ret), K(tenant_id_), K(inner_table_version),
               K(plan), K(rules), "rule_count", group_id_map_.size());
    }
    lock_.unlock();
  } else {
    LOG_ERROR("get lock of resource mapping rule cache failed.", K(tenant_id_));
  }
  return ret;
}

int ObTenantResColMappingInfo::get_rule_id(uint64_t tenant_id,
                                           uint64_t database_id,
                                           const common::ObString &table_name,
                                           const common::ObString &column_name,
                                           common::ObNameCaseMode case_mode,
                                           uint64_t &rule_id)
{
  int ret = OB_SUCCESS;
  rule_id = OB_INVALID_ID;
  if (OB_FAIL(rule_id_map_.get_refactored(
            ColumnNameKey(tenant_id, database_id, table_name, column_name, case_mode), rule_id))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("get rule id failed", K(ret));
    } else {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObTenantResColMappingInfo::get_group_id(uint64_t rule_id, const ObString &user_name,
                                            const common::ObString &literal_value,
                                            uint64_t &group_id)
{
  int ret = OB_SUCCESS;
  RuleValue rule_value;
  group_id = OB_INVALID_ID;
  if (OB_FAIL(group_id_map_.get_refactored(
            RuleValueKey(rule_id, user_name, literal_value), rule_value))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = group_id_map_.get_refactored(
              RuleValueKey(rule_id, ObString(), literal_value), rule_value);
    }
  }
  if (OB_SUCC(ret)) {
    group_id = rule_value.group_id_;
  } else if (OB_HASH_NOT_EXIST == ret) {
    ret = OB_SUCCESS;
  } else {
    LOG_WARN("get group id failed", K(ret));
  }
  LOG_TRACE("get_column_mapping_group_id", K(tenant_id_), K(rule_id), K(user_name),
            K(literal_value), K(rule_value.group_id_));
  return ret;
}

int ObResourceColMappingRuleManager::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(tenant_rule_infos_.init("ResRuleInfoMap"))) {
    LOG_WARN("create map failed", K(ret));
  } else {
    inited_ = true;
  }
  return ret;
}

int ObResourceColMappingRuleManager::add_tenant(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObTenantResColMappingInfo *res_info = NULL;
  void *buf = NULL;
  ObMemAttr attr(tenant_id, "ResRuleInfo");
  ObResTenantId res_tenant_id(tenant_id);
  if (is_virtual_tenant_id(tenant_id)) {
    // do nothing
  } else if (OB_UNLIKELY(!inited_)) {
    LOG_ERROR("resource column mapping rule manager not init", K(ret));
  } else if (OB_FAIL(tenant_rule_infos_.contains_key(res_tenant_id))) {
    if (OB_ENTRY_EXIST == ret) {
      ret = OB_SUCCESS;
    } else if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("check map contain key failed", K(ret));
    } else if (OB_ISNULL(buf = ob_malloc(sizeof(ObTenantResColMappingInfo), attr))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret), K(sizeof(ObTenantResColMappingInfo)));
    } else {
      res_info = new (buf)ObTenantResColMappingInfo();
      if (OB_FAIL(res_info->init(tenant_id))) {
        LOG_WARN("init resource column mapping info failed", K(ret));
      } else if (OB_FAIL(tenant_rule_infos_.insert_and_get(res_tenant_id, res_info))) {
        LOG_WARN("insert failed", K(ret));
      } else {
        tenant_rule_infos_.revert(res_info);
      }
      if (OB_FAIL(ret)) {
        res_info->~ObTenantResColMappingInfo();
        ob_free(res_info);
        res_info = NULL;
      }
    }
  }
  LOG_INFO("add resource column mapping rule info", K(ret), K(tenant_id));
  return ret;
}

int ObResourceColMappingRuleManager::drop_tenant(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObResTenantId res_tenant_id(tenant_id);
  if (is_virtual_tenant_id(tenant_id)) {
    // do nothing
  } else if (OB_FAIL(tenant_rule_infos_.del(res_tenant_id))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("drop tenant failed", K(ret));
    } else {
      ret = OB_SUCCESS;
    }
  }
  tenant_rule_infos_.purge();
  LOG_INFO("drop resource column mapping rule info", K(ret), K(tenant_id));
  return ret;
}

int ObResourceColMappingRuleManager::refresh_resource_column_mapping_rule(uint64_t tenant_id,
                                                                      sql::ObPlanCache *plan_cache,
                                                                      const common::ObString &plan)
{
  int ret = OB_SUCCESS;
  ObTenantResColMappingInfo *res_info = NULL;
  ObResTenantId res_tenant_id(tenant_id);
  if (OB_FAIL(tenant_rule_infos_.get(res_tenant_id, res_info))) {
    LOG_WARN("get tenant rule infos failed", K(ret));
  } else if (OB_FAIL(res_info->refresh(plan_cache, plan))) {
    LOG_WARN("refresh res col mapping info failed", K(ret), K(tenant_id));
  }
  tenant_rule_infos_.revert(res_info);
  return ret;
}

uint64_t ObResourceColMappingRuleManager::get_column_mapping_rule_id(uint64_t tenant_id,
                                                        uint64_t database_id,
                                                        const common::ObString &table_name,
                                                        const common::ObString &column_name,
                                                        common::ObNameCaseMode case_mode)
{
  int ret = OB_SUCCESS;
  uint64_t rule_id = OB_INVALID_ID;
  ObTenantResColMappingInfo *res_info = NULL;
  ObResTenantId res_tenant_id(tenant_id);
  if (OB_FAIL(tenant_rule_infos_.get(res_tenant_id, res_info))) {
    LOG_WARN("get tenant  rule info failed", K(ret));
  } else if (OB_FAIL(res_info->get_rule_id(tenant_id, database_id, table_name, column_name,
                                           case_mode, rule_id))) {
    LOG_WARN("get rule id failed", K(ret));
  }
  tenant_rule_infos_.revert(res_info);
  LOG_TRACE("get_column_mapping_rule_id", K(tenant_id), K(database_id), K(table_name),
            K(column_name), K(case_mode), K(rule_id));
  return rule_id;
}

uint64_t ObResourceColMappingRuleManager::get_column_mapping_group_id(uint64_t tenant_id,
                                       uint64_t rule_id, const ObString &user_name,
                                       const common::ObString &literal_value)
{
  int ret = OB_SUCCESS;
  ObTenantResColMappingInfo *res_info = NULL;
  uint64_t group_id = OB_INVALID_ID;
  ObResTenantId res_tenant_id(tenant_id);
  if (OB_FAIL(tenant_rule_infos_.get(res_tenant_id, res_info))) {
    LOG_WARN("get tenant  rule info failed", K(ret));
  } else if (OB_FAIL(res_info->get_group_id(rule_id, user_name, literal_value, group_id))) {
    LOG_WARN("get group id failed", K(ret));
  }
  tenant_rule_infos_.revert(res_info);
  LOG_TRACE("get_column_mapping_group_id", K(tenant_id), K(rule_id), K(user_name), K(literal_value),
            K(group_id));
  return group_id;
}

int64_t ObResourceColMappingRuleManager::get_column_mapping_version(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  int64_t version = 0;
  ObResTenantId res_tenant_id(tenant_id);
  ObTenantResColMappingInfo *res_info = NULL;
  if (OB_FAIL(tenant_rule_infos_.get(res_tenant_id, res_info))) {
    LOG_TRACE("get tenant column mapping version failed", K(tenant_id), K(ret));
  } else {
    version = res_info->get_version();
  }
  tenant_rule_infos_.revert(res_info);
  return version;
}
