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

#define USING_LOG_PREFIX SHARE_SCHEMA

#include "share/schema/ob_ccl_rule_mgr.h"
#include "lib/allocator/ob_mod_define.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "share/schema/ob_schema_utils.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
const char *ObCCLRuleMgr::CCL_RULE_MGR = "CCL_RULE_MGR";
ObCCLRuleMgr::ObCCLRuleMgr()
  : is_inited_(false),
    local_allocator_(SET_USE_500(ObModIds::OB_SCHEMA_GETTER_GUARD, ObCtxIds::SCHEMA_SERVICE)),
    allocator_(local_allocator_),
    ccl_rules_(0, NULL, SET_USE_500(CCL_RULE_MGR, ObCtxIds::SCHEMA_SERVICE)),
    ccl_rules_specified_by_dml_(0, NULL, SET_USE_500(CCL_RULE_MGR, ObCtxIds::SCHEMA_SERVICE)),
    ccl_rules_specified_by_database_table_dml_(0, NULL, SET_USE_500(CCL_RULE_MGR, ObCtxIds::SCHEMA_SERVICE)),
    ccl_rule_name_map_(SET_USE_500(CCL_RULE_MGR, ObCtxIds::SCHEMA_SERVICE)),
    ccl_rule_id_map_(SET_USE_500(CCL_RULE_MGR, ObCtxIds::SCHEMA_SERVICE))
{
}

ObCCLRuleMgr::ObCCLRuleMgr(common::ObIAllocator &allocator)
  : is_inited_(false),
    local_allocator_(SET_USE_500(ObModIds::OB_SCHEMA_GETTER_GUARD, ObCtxIds::SCHEMA_SERVICE)),
    allocator_(allocator),
    ccl_rules_(0, NULL, SET_USE_500(CCL_RULE_MGR, ObCtxIds::SCHEMA_SERVICE)),
    ccl_rules_specified_by_dml_(0, NULL, SET_USE_500(CCL_RULE_MGR, ObCtxIds::SCHEMA_SERVICE)),
    ccl_rules_specified_by_database_table_dml_(0, NULL, SET_USE_500(CCL_RULE_MGR, ObCtxIds::SCHEMA_SERVICE)),
    ccl_rule_name_map_(SET_USE_500(CCL_RULE_MGR, ObCtxIds::SCHEMA_SERVICE)),
    ccl_rule_id_map_(SET_USE_500(CCL_RULE_MGR, ObCtxIds::SCHEMA_SERVICE))
{
}

ObCCLRuleMgr::~ObCCLRuleMgr()
{
}

ObCCLRuleMgr &ObCCLRuleMgr::operator=(const ObCCLRuleMgr &other)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ccl_rule manager not init", K(ret));
  } else if (OB_FAIL(assign(other))) {
    LOG_WARN("assign failed", K(ret));
  }
  return *this;
}

int ObCCLRuleMgr::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init private rls_group schema manager twice", K(ret));
  } else if (OB_FAIL(ccl_rule_name_map_.init())) {
    LOG_WARN("init rls_group name map failed", K(ret));
  } else if (OB_FAIL(ccl_rule_id_map_.init())) {
    LOG_WARN("init rls_group id map failed", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObCCLRuleMgr::reset()
{
  if (IS_NOT_INIT) {
    LOG_WARN_RET(OB_NOT_INIT, "ccl_rule manger not init");
  } else {
    ccl_rules_.clear();
    ccl_rules_specified_by_dml_.clear();
    ccl_rules_specified_by_database_table_dml_.clear();
    ccl_rule_name_map_.clear();
    ccl_rule_id_map_.clear();
  }
}

int ObCCLRuleMgr::assign(const ObCCLRuleMgr &other)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ccl_rule manager not init", K(ret));
  } else if (this != &other) {
    if (OB_FAIL(ccl_rule_name_map_.assign(other.ccl_rule_name_map_))) {
      LOG_WARN("assign rls_group name map failed", K(ret));
    } else if (OB_FAIL(ccl_rule_id_map_.assign(other.ccl_rule_id_map_))) {
      LOG_WARN("assign rls_group id map failed", K(ret));
    } else if (OB_FAIL(ccl_rules_.assign(other.ccl_rules_))) {
      LOG_WARN("assign rls_group schema vector failed", K(ret));
    } else if (OB_FAIL(ccl_rules_specified_by_dml_.assign(other.ccl_rules_specified_by_dml_))) {
      LOG_WARN("assign rls_group schema vector failed", K(ret));
    } else if (OB_FAIL(ccl_rules_specified_by_database_table_dml_.assign(other.ccl_rules_specified_by_database_table_dml_))) {
      LOG_WARN("assign rls_group schema vector failed", K(ret));
    }
  }
  return ret;
}

int ObCCLRuleMgr::deep_copy(const ObCCLRuleMgr &other)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("rls_group manager not init", K(ret));
  } else if (this != &other) {
    reset();
    ObSimpleCCLRuleSchema *schema = NULL;
    for (CCLRuleIter iter = other.ccl_rules_.begin();
         OB_SUCC(ret) && iter != other.ccl_rules_.end(); iter++) {
      if (OB_ISNULL(schema = *iter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", KP(schema), K(ret));
      } else if (OB_FAIL(add_ccl_rule(*schema, (*iter)->get_name_case_mode(), ccl_rules_))) {
        LOG_WARN("add ccl_rule failed", K(*schema), K(ret));
      }
    }
    for (CCLRuleIter iter = other.ccl_rules_specified_by_dml_.begin();
         OB_SUCC(ret) && iter != other.ccl_rules_specified_by_dml_.end(); iter++) {
      if (OB_ISNULL(schema = *iter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", KP(schema), K(ret));
      } else if (OB_FAIL(add_ccl_rule(*schema, (*iter)->get_name_case_mode(), ccl_rules_specified_by_dml_))) {
        LOG_WARN("add ccl_rule failed", K(*schema), K(ret));
      }
    }
    for (CCLRuleIter iter = other.ccl_rules_specified_by_database_table_dml_.begin();
         OB_SUCC(ret) && iter != other.ccl_rules_specified_by_database_table_dml_.end(); iter++) {
      if (OB_ISNULL(schema = *iter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", KP(schema), K(ret));
      } else if (OB_FAIL(add_ccl_rule(*schema, (*iter)->get_name_case_mode(), ccl_rules_specified_by_database_table_dml_))) {
        LOG_WARN("add ccl_rule failed", K(*schema), K(ret));
      }
    }
  }
  return ret;
}

int ObCCLRuleMgr::add_ccl_rule(const ObSimpleCCLRuleSchema &schema, const ObNameCaseMode mode, CCLRuleInfos &ccl_rule_infos)
{
  int ret = OB_SUCCESS;
  ObSimpleCCLRuleSchema *new_schema = NULL;
  ObSimpleCCLRuleSchema *old_schema = NULL;
  CCLRuleIter iter = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("the schema mgr is not init", K(ret));
  } else if (OB_UNLIKELY(!schema.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(schema));
  } else if (OB_FAIL(ObSchemaUtils::alloc_schema(allocator_,
                                                 schema,
                                                 new_schema))) {
    LOG_WARN("alloc schema failed", K(ret));
  } else if (OB_ISNULL(new_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("alloc schema is a NULL ptr", K(new_schema), K(ret));
  } else if (OB_FALSE_IT(new_schema->set_name_case_mode(mode))) {
  } else if (OB_FAIL(ccl_rule_infos.replace(new_schema, iter, compare_ccl_rule, equal_ccl_rule,
                                            old_schema))) {
    LOG_WARN("failed to add rls_group schema", K(ret));
  } else {
    if (OB_SUCC(ret)) {
      int over_write = 1;
      ObCCLRuleNameHashKey hash_wrapper(new_schema->get_tenant_id(),
                                        new_schema->get_name_case_mode(),
                                        new_schema->get_ccl_rule_name());
      if (OB_FAIL(ccl_rule_name_map_.set_refactored(hash_wrapper, new_schema, over_write))) {
        LOG_WARN("build ccl_rule hash map failed", K(ret));
      } else if (OB_FAIL(ccl_rule_id_map_.set_refactored(new_schema->get_ccl_rule_id(), new_schema,
                                                         over_write))) {
        LOG_WARN("build ccl_rule id hashmap failed", K(ret), "ccl_rule id",
                 new_schema->get_ccl_rule_id());
      }
    }
  }

  LOG_TRACE("add ccl_rule", K(schema), K(ccl_rule_infos.count()),
            K(ccl_rule_name_map_.item_count()), K(ccl_rule_id_map_.item_count()));
  return ret;
}

int ObCCLRuleMgr::add_ccl_rule(const ObSimpleCCLRuleSchema &schema, const ObNameCaseMode mode)
{
  int ret = OB_SUCCESS;
  CclRuleContainsInfo ccl_rule_contains_info = CclRuleContainsInfo::NONE;
  //so far we only can use affect_for_all_databases and affect_for_all_tables
  // to check whether ccl rule contains database and table info
  //Because affect_tables_ would be push_back a default 0 value
  if (!schema.affect_for_all_databases()) {
    ccl_rule_contains_info = CclRuleContainsInfo::DATABASE_AND_TABLE;
  } else if (schema.get_affect_dml() != ObCCLAffectDMLType::ALL) {
    ccl_rule_contains_info = CclRuleContainsInfo::DML;
  }
  CCLRuleInfos *target_ccl_rule_infos = get_ccl_rule_belong_ccl_rule_infos(ccl_rule_contains_info);
  if (OB_FAIL(add_ccl_rule(schema, mode, *target_ccl_rule_infos))) {
    LOG_WARN("fail to add ccl rule", K(ret));
  }
  return ret;
}

int ObCCLRuleMgr::del_ccl_rule_from_ccl_rule_infos(const ObTenantCCLRuleId &id,
                                                   CCLRuleInfos &ccl_rule_infos,
                                                   ObSimpleCCLRuleSchema *&schema)
{
  int ret = OB_SUCCESS;
  schema = NULL;
  if (OB_FAIL(ccl_rule_infos.remove_if(id, compare_with_tenant_ccl_rule_id,
                                       equal_with_tenant_ccl_rule_id, schema))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      // if item does not exist, regard it as succeeded, schema will be refreshed later
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to remove ccl_rule schema", K(ret));
    }
  } else if (OB_ISNULL(schema)) {
    // if item can be found, schema should not be null
    // defense code, should not happed
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("removed ccl_rule schema return NULL, ", "tenant_id", id.tenant_id_, "ccl_rule_id",
             id.ccl_rule_id_, K(ret));
  }

  return ret;
}

int ObCCLRuleMgr::del_ccl_rule(const ObTenantCCLRuleId &id)
{
  int ret = OB_SUCCESS;
  ObSimpleCCLRuleSchema *schema = NULL;
  if (OB_UNLIKELY(!id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(id));
  } else if (OB_FAIL(del_ccl_rule_from_ccl_rule_infos(id, ccl_rules_specified_by_database_table_dml_, schema))) {
    LOG_WARN("fail to del ccl rules from ccl_rules_specified_by_database_table_dml_", K(ret));
  } else if (OB_ISNULL(schema) && OB_FAIL(del_ccl_rule_from_ccl_rule_infos(id, ccl_rules_specified_by_dml_, schema))) {
    LOG_WARN("fail to del ccl rules from ccl_rules_specified_by_dml_", K(ret));
  } else if (OB_ISNULL(schema) && OB_FAIL(del_ccl_rule_from_ccl_rule_infos(id, ccl_rules_, schema))) {
    LOG_WARN("fail to del ccl rules from ccl_rules_specified_by_dml_", K(ret));
  }

  if (OB_SUCC(ret) && OB_NOT_NULL(schema)) {
    if (OB_FAIL(ccl_rule_id_map_.erase_refactored(id.ccl_rule_id_))) {
      if (OB_HASH_NOT_EXIST == ret) {
        //item does not exist, will ignore it
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to delete ccl_rule from hashmap", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(schema)) {
    if (OB_FAIL(ccl_rule_name_map_.erase_refactored(
        ObCCLRuleNameHashKey(schema->get_tenant_id(),
                             schema->get_name_case_mode(),
                             schema->get_ccl_rule_name())))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        //item does not exist, will ignore it
      } else {
        LOG_WARN("failed to delete ccl_rule from hashmap", K(ret));
      }
    }
  }

  LOG_TRACE("ccl_rule del", K(id), K(ccl_rules_.count()),
            K(ccl_rules_specified_by_dml_.count()),
            K(ccl_rules_specified_by_database_table_dml_.count()),
            K(ccl_rule_id_map_.item_count()),
            K(ccl_rule_name_map_.item_count()));
  return ret;
}

int ObCCLRuleMgr::get_schema_by_id(const uint64_t ccl_rule_id,
                                     const ObSimpleCCLRuleSchema *&schema) const
{
  int ret = OB_SUCCESS;
  schema = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == ccl_rule_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ccl_rule_id));
  } else {
    ObSimpleCCLRuleSchema *tmp_schema = NULL;
    int hash_ret = ccl_rule_id_map_.get_refactored(ccl_rule_id, tmp_schema);
    if (OB_LIKELY(OB_SUCCESS == hash_ret)) {
      if (OB_ISNULL(tmp_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(ret), K(tmp_schema));
      } else {
        schema = tmp_schema;
      }
    }
  }
  return ret;
}

int ObCCLRuleMgr::get_schema_by_name(const uint64_t tenant_id,
                                     const ObNameCaseMode mode,
                                     const common::ObString &name,
                                     const ObSimpleCCLRuleSchema *&schema) const
{
  int ret = OB_SUCCESS;
  schema = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id ||
             name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(name));
  } else {
    ObSimpleCCLRuleSchema *tmp_schema = NULL;
    ObCCLRuleNameHashKey hash_wrapper(tenant_id, mode, name);
    if (OB_FAIL(ccl_rule_name_map_.get_refactored(hash_wrapper, tmp_schema))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        // LOG_WARN("schema is not exist", K(tenant_id), K(name));
      } else {
        LOG_WARN("failed to get ccl_rule from hashmap", K(ret));
      }
    } else {
      schema = tmp_schema;
    }
  }
  return ret;
}

ObCCLRuleMgr::CCLRuleInfos *
ObCCLRuleMgr::get_ccl_rule_belong_ccl_rule_infos(CclRuleContainsInfo contians_info)
{
  CCLRuleInfos *target_ccl_rule_infos = nullptr;
  if (contians_info == CclRuleContainsInfo::DATABASE_AND_TABLE) {
    target_ccl_rule_infos = &ccl_rules_specified_by_database_table_dml_;
  } else if (contians_info == CclRuleContainsInfo::DML) {
    target_ccl_rule_infos = &ccl_rules_specified_by_dml_;
  } else {
    target_ccl_rule_infos = &ccl_rules_;
  }
  return target_ccl_rule_infos;
}

int ObCCLRuleMgr::get_schemas_in_tenant(const uint64_t tenant_id,
                                        common::ObIArray<const ObSimpleCCLRuleSchema *> &schemas,
                                        const CCLRuleInfos &ccl_rule_infos) const
{
  int ret = OB_SUCCESS;
  schemas.reset();
  ObTenantCCLRuleId id(tenant_id, OB_MIN_ID);
  ConstCCLRuleIter iter_begin = ccl_rule_infos.lower_bound(id, compare_with_tenant_ccl_rule_id);
  bool is_stop = false;
  for (ConstCCLRuleIter iter = iter_begin; OB_SUCC(ret) && iter != ccl_rule_infos.end() && !is_stop;
       ++iter) {
    const ObSimpleCCLRuleSchema *schema = NULL;
    if (OB_ISNULL(schema = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret), K(schema));
    } else if (tenant_id != schema->get_tenant_id()) {
      is_stop = true;
    } else if (OB_FAIL(schemas.push_back(schema))) {
      LOG_WARN("push back ccl_rule failed", K(ret));
    }
  }
  return ret;
}

int ObCCLRuleMgr::get_schemas_in_tenant(const uint64_t tenant_id,
                                        common::ObIArray<const ObSimpleCCLRuleSchema *> &schemas) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_schemas_in_tenant(tenant_id, schemas, ccl_rules_specified_by_database_table_dml_))) {
    LOG_WARN("fail to get ccl rule schemas in ccl_rules_specified_by_database_table_dml_", K(ret));
  } else if (OB_FAIL(get_schemas_in_tenant(tenant_id, schemas, ccl_rules_specified_by_dml_))) {
    LOG_WARN("fail to get ccl rule schemas in ccl_rules_specified_by_database_table_dml_", K(ret));
  } else if (OB_FAIL(get_schemas_in_tenant(tenant_id, schemas, ccl_rules_))) {
    LOG_WARN("fail to get ccl rule schemas in ccl_rules_specified_by_database_table_dml_", K(ret));
  }
  return ret;
}

int ObCCLRuleMgr::del_schemas_in_tenant(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else {
    ObArray<const ObSimpleCCLRuleSchema *> schemas;
    if (OB_FAIL(get_schemas_in_tenant(tenant_id, schemas))) {
      LOG_WARN("get rls_policy schemas failed", K(ret), K(tenant_id));
    } else {
      FOREACH_CNT_X(schema, schemas, OB_SUCC(ret)) {
        ObTenantCCLRuleId id(tenant_id, (*schema)->get_ccl_rule_id());
        if (OB_FAIL(del_ccl_rule(id))) {
          LOG_WARN("del ccl_rule failed",
                   "tenant_id", id.tenant_id_,
                   "ccl_rule_id", id.ccl_rule_id_,
                   K(ret));
        }
      }
    }
  }
  return ret;
}

int ObCCLRuleMgr::get_schema_count(int64_t &schema_count) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    schema_count = ccl_rules_specified_by_database_table_dml_.count()
                   + ccl_rules_specified_by_dml_.count() + ccl_rules_.count();
  }
  return ret;
}

int ObCCLRuleMgr::get_schema_statistics(ObSchemaStatisticsInfo &schema_info) const
{
  int ret = OB_SUCCESS;
  schema_info.reset();
  schema_info.schema_type_ = RLS_POLICY_SCHEMA;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(get_schema_count(schema_info.count_))) {
    LOG_WARN("fail to get ccl rule schema count", K(ret));
  } else {
    for (ConstCCLRuleIter iter = ccl_rules_specified_by_database_table_dml_.begin();
        OB_SUCC(ret) && iter != ccl_rules_specified_by_database_table_dml_.end(); ++iter) {
      if (OB_ISNULL(*iter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("schema is null", K(ret));
      } else {
        schema_info.size_ += (*iter)->get_convert_size();
      }
    }
    for (ConstCCLRuleIter iter = ccl_rules_specified_by_dml_.begin();
        OB_SUCC(ret) && iter != ccl_rules_specified_by_dml_.end(); ++iter) {
      if (OB_ISNULL(*iter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("schema is null", K(ret));
      } else {
        schema_info.size_ += (*iter)->get_convert_size();
      }
    }
    for (ConstCCLRuleIter iter = ccl_rules_.begin();
        OB_SUCC(ret) && iter != ccl_rules_.end(); ++iter) {
      if (OB_ISNULL(*iter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("schema is null", K(ret));
      } else {
        schema_info.size_ += (*iter)->get_convert_size();
      }
    }
  }
  return ret;
}

OB_INLINE bool ObCCLRuleMgr::compare_ccl_rule(const ObSimpleCCLRuleSchema *lhs, const ObSimpleCCLRuleSchema *rhs)
{
  return lhs->get_sort_key() < rhs->get_sort_key();
}

OB_INLINE bool ObCCLRuleMgr::equal_ccl_rule(const ObSimpleCCLRuleSchema *lhs, const ObSimpleCCLRuleSchema *rhs)
{
  return lhs->get_sort_key() == rhs->get_sort_key();
}

OB_INLINE bool ObCCLRuleMgr::compare_with_tenant_ccl_rule_id(const ObSimpleCCLRuleSchema *lhs, const ObTenantCCLRuleId &key)
{
  return NULL != lhs ? (lhs->get_sort_key() < key) : false;
}

OB_INLINE bool ObCCLRuleMgr::equal_with_tenant_ccl_rule_id(const ObSimpleCCLRuleSchema *lhs, const ObTenantCCLRuleId &key)
{
  return NULL != lhs ? (lhs->get_sort_key() == key) : false;
}

} // namespace schema
} // namespace share
} // namespace oceanbase
