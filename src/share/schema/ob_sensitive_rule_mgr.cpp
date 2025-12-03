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

#include "share/schema/ob_sensitive_rule_mgr.h"
#include "share/schema/ob_schema_utils.h"
#include "lib/allocator/ob_mod_define.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
using namespace std;
using namespace common;
using namespace hash;

ObSensitiveRuleMgr::ObSensitiveRuleMgr()
    : is_inited_(false),
      local_allocator_(SET_USE_500(ObModIds::OB_SCHEMA_GETTER_GUARD, ObCtxIds::SCHEMA_SERVICE)),
      allocator_(local_allocator_),
      schema_infos_(0, NULL, SET_USE_500("SchemaSensRule", ObCtxIds::SCHEMA_SERVICE)),
      name_map_(SET_USE_500("SchemaSensRule", ObCtxIds::SCHEMA_SERVICE)),
      id_map_(SET_USE_500("SchemaSensRule", ObCtxIds::SCHEMA_SERVICE)),
      column_map_(SET_USE_500("SchemaSensRule", ObCtxIds::SCHEMA_SERVICE))
{
}

ObSensitiveRuleMgr::ObSensitiveRuleMgr(ObIAllocator &allocator)
    : is_inited_(false),
      local_allocator_(SET_USE_500(ObModIds::OB_SCHEMA_GETTER_GUARD, ObCtxIds::SCHEMA_SERVICE)),
      allocator_(allocator),
      schema_infos_(0, NULL, SET_USE_500("SchemaSensRule", ObCtxIds::SCHEMA_SERVICE)),
      name_map_(SET_USE_500("SchemaSensRule", ObCtxIds::SCHEMA_SERVICE)),
      id_map_(SET_USE_500("SchemaSensRule", ObCtxIds::SCHEMA_SERVICE)),
      column_map_(SET_USE_500("SchemaSensRule", ObCtxIds::SCHEMA_SERVICE))
{
}

ObSensitiveRuleMgr::~ObSensitiveRuleMgr()
{
}

int ObSensitiveRuleMgr::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init private sensitive_rule schema manager twice", K(ret));
  } else if (OB_FAIL(name_map_.init())) {
    LOG_WARN("init hash map failed", K(ret));
  } else if (OB_FAIL(id_map_.init())) {
    LOG_WARN("init hash map failed", K(ret));
  } else if (OB_FAIL(column_map_.init())) {
    LOG_WARN("create sensitive_col hash map failed", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObSensitiveRuleMgr::reset()
{
  if (!is_inited_) {
    LOG_WARN_RET(OB_NOT_INIT, "sensitive_rule manger not init");
  } else {
    schema_infos_.clear();
    name_map_.clear();
    id_map_.clear();
    column_map_.clear();
  }
}

ObSensitiveRuleMgr &ObSensitiveRuleMgr::operator =(const ObSensitiveRuleMgr &other)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema manager not init", K(ret));
  } else if (OB_FAIL(assign(other))) {
    LOG_WARN("assign failed", K(ret));
  }
  return *this;
}

int ObSensitiveRuleMgr::assign(const ObSensitiveRuleMgr &other)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema manager not init", K(ret));
  } else if (this != &other) {
    if (OB_FAIL(name_map_.assign(other.name_map_))) {
      LOG_WARN("assign sensitive_rule name map failed", K(ret));
    } else if (OB_FAIL(id_map_.assign(other.id_map_))) {
      LOG_WARN("assign sensitive_rule id map failed", K(ret));
    } else if (OB_FAIL(schema_infos_.assign(other.schema_infos_))) {
      LOG_WARN("assign sensitive_rule schema vector failed", K(ret));
    } else if (OB_FAIL(column_map_.assign(other.column_map_))) {
      LOG_WARN("reuse sensitive_col hash map failed", K(ret));
    }
  }
  return ret;
}

int ObSensitiveRuleMgr::deep_copy(const ObSensitiveRuleMgr &other)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema manager not init", K(ret));
  } else if (this != &other) {
    reset();
    ObSensitiveRuleSchema *schema = NULL;
    for (SensitiveRuleIter iter = other.schema_infos_.begin();
        OB_SUCC(ret) && iter != other.schema_infos_.end(); iter++) {
      if (OB_ISNULL(schema = *iter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", KP(schema), K(ret));
      } else if (OB_FAIL(add_sensitive_rule(*schema))) {
        LOG_WARN("add outline failed", K(*schema), K(ret));
      }
    }
  }
  return ret;
}

int ObSensitiveRuleMgr::add_sensitive_rules(const common::ObIArray<ObSensitiveRuleSchema> &sensitive_rule_schemas)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < sensitive_rule_schemas.count() && OB_SUCC(ret); ++i) {
    if (OB_FAIL(add_sensitive_rule(sensitive_rule_schemas.at(i)))) {
      LOG_WARN("push schema failed", K(ret));
    }
  }
  return ret;
}

// erase sensitive columns from column map related to sensitive_rule_id
int ObSensitiveRuleMgr::erase_from_column_map(const uint64_t sensitive_rule_id)
{
  int ret = OB_SUCCESS;
  ObSensitiveRuleSchema *schema = NULL;
  if (OB_FAIL(id_map_.get_refactored(sensitive_rule_id, schema))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("schema is not exist", K(sensitive_rule_id),
               "map_cnt", id_map_.item_count());
    }
  } else if (OB_ISNULL(schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema is null", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < schema->get_sensitive_field_items().count(); ++i) {
      ObSensitiveFieldItem item = schema->get_sensitive_field_items().at(i);
      if (OB_FAIL(column_map_.erase_refactored(ObSensitiveColumnHashKey(schema->get_tenant_id(), 
                                                                        item.table_id_, 
                                                                        item.column_id_)))) {
        LOG_WARN("erase sensitive_col hash map failed", K(ret));
      }
    }
  }
  return ret;
}

// set sensitive columns for column map by new schema
int ObSensitiveRuleMgr::update_column_map(ObSensitiveRuleSchema *new_schema)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(new_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("new_schema is null", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < new_schema->get_sensitive_field_items().count(); ++i) {
    ObSensitiveFieldItem item = new_schema->get_sensitive_field_items().at(i);
    ObSensitiveColumnHashKey sensitive_col_hash_key(new_schema->get_tenant_id(), item.table_id_, item.column_id_);
    ObSensitiveColumnSchema *new_col_schema_ptr = NULL;
    ObSensitiveColumnSchema new_col_schema;
    new_col_schema.set_tenant_id(new_schema->get_tenant_id());
    new_col_schema.set_table_id(item.table_id_);
    new_col_schema.set_column_id(item.column_id_);
    new_col_schema.set_sensitive_rule_id(new_schema->get_sensitive_rule_id());
    new_col_schema.set_schema_version(new_schema->get_schema_version());
    if (OB_FAIL(ObSchemaUtils::alloc_schema(allocator_, new_col_schema, new_col_schema_ptr))) {
      LOG_WARN("alloc schema failed", K(ret));
    } else if (OB_ISNULL(new_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("alloc schema is a NULL ptr", K(new_schema), K(ret));
    } else if (OB_FAIL(column_map_.set_refactored(sensitive_col_hash_key, new_col_schema_ptr, 0))) {
      LOG_WARN("build sensitive_col hash map failed", K(ret));
    }
  }
  return ret;
}

int ObSensitiveRuleMgr::add_sensitive_rule(const ObSensitiveRuleSchema &schema)
{
  int ret = OB_SUCCESS;
  ObSensitiveRuleSchema *new_schema = NULL;
  ObSensitiveRuleSchema *old_schema = NULL;
  SensitiveRuleIter iter = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
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
  } else if (OB_FAIL(schema_infos_.replace(new_schema,
                                           iter,
                                           schema_cmp,
                                           schema_equal,
                                           old_schema))) {
    LOG_WARN("failed to add sensitive_rule schema", K(ret));
  } else if (OB_FAIL(erase_from_column_map(new_schema->get_sensitive_rule_id()))) {
    LOG_WARN("erase from column map failed", K(ret));
  } else {
    int overwrite = 1;
    ObSensitiveRuleNameHashKey sensitive_rule_name_hash_key(new_schema->get_tenant_id(),
                                                            new_schema->get_sensitive_rule_name_str());
    if (OB_FAIL(name_map_.set_refactored(sensitive_rule_name_hash_key, new_schema, overwrite))) {
      LOG_WARN("build sensitive_rule hash map failed", K(ret));
    } else if (OB_FAIL(id_map_.set_refactored(new_schema->get_sensitive_rule_id(), new_schema, overwrite))) {
      LOG_WARN("build sensitive_rule id hashmap failed", K(ret),
              "sensitive_rule_id", new_schema->get_sensitive_rule_id());
    } else if (OB_FAIL(update_column_map(new_schema))) {
      LOG_WARN("update column map failed", K(ret));
    }
  }
  if (OB_SUCC(ret) && (schema_infos_.count() != name_map_.item_count()
                      || schema_infos_.count() != id_map_.item_count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema is inconsistent with its map", K(ret),
            K(schema_infos_.count()),
            K(name_map_.item_count()),
            K(id_map_.item_count()));
  }
  LOG_DEBUG("sensitive_rule add", K(schema), K(schema_infos_.count()),
            K(name_map_.item_count()), K(id_map_.item_count()));
  return ret;
}

int ObSensitiveRuleMgr::get_schema_by_id(const uint64_t sensitive_rule_id, const ObSensitiveRuleSchema *&schema) const
{
  int ret = OB_SUCCESS;
  schema = NULL;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == sensitive_rule_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(sensitive_rule_id));
  } else {
    ObSensitiveRuleSchema *tmp_schema = NULL;
    if (OB_FAIL(id_map_.get_refactored(sensitive_rule_id, tmp_schema))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        LOG_INFO("schema is not exist", K(sensitive_rule_id),
                 "map_cnt", name_map_.item_count());
      }
    } else {
      schema = tmp_schema;
    }
  }
  return ret;
}

int ObSensitiveRuleMgr::get_schema_by_name(const uint64_t tenant_id,
                                           const ObString &name,
                                           const ObSensitiveRuleSchema *&schema) const
{
  int ret = OB_SUCCESS;
  schema = NULL;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id || name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(name));
  } else {
    ObSensitiveRuleSchema *tmp_schema = NULL;
    ObSensitiveRuleNameHashKey hash_wrap(tenant_id, name);
    if (OB_FAIL(name_map_.get_refactored(hash_wrap, tmp_schema))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        LOG_INFO("schema is not exist", K(tenant_id), K(name),
                 "map_cnt", name_map_.item_count());
      }
    } else {
      schema = tmp_schema;
    }
  }
  return ret;
}

int ObSensitiveRuleMgr::get_schema_by_column(const uint64_t tenant_id, 
                                             const uint64_t table_id,
                                             const uint64_t column_id,
                                             const ObSensitiveRuleSchema *&schema) const
{
  int ret = OB_SUCCESS;
  schema = NULL;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == table_id || OB_INVALID_ID == column_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(table_id), K(column_id));
  } else {
    ObSensitiveColumnSchema *col_schema = NULL;
    ObSensitiveRuleSchema *rule_schema = NULL;
    int hash_ret = column_map_.get_refactored(ObSensitiveColumnHashKey(tenant_id, table_id, column_id), col_schema);
    if (OB_SUCCESS != hash_ret) {
      if (OB_LIKELY(OB_HASH_NOT_EXIST == hash_ret)) {
        ret = OB_SUCCESS;
        LOG_INFO("sensitive rule schema is not exist", K(tenant_id), K(table_id), K(column_id));
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get sensitive column schema failed", K(ret), K(hash_ret));
      }
    } else if (OB_ISNULL(col_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL sensitive column schema", K(ret));
    } else if (OB_FAIL(id_map_.get_refactored(col_schema->get_sensitive_rule_id(), rule_schema))) {
      // get sensitive rule schema by id should not fail
      LOG_WARN("get sensitive rule schema failed", K(ret));
    } else if (OB_ISNULL(rule_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL sensitive rule schema", K(ret));
    } else {
      schema = rule_schema;
    }
  }
  return ret;
}

int ObSensitiveRuleMgr::del_schemas_in_tenant(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else {
    ObArray<const ObSensitiveRuleSchema *> schemas;
    if (OB_FAIL(get_schemas_in_tenant(tenant_id, schemas))) {
      LOG_WARN("get sensitive_rule schemas failed", K(ret), K(tenant_id));
    } else {
      FOREACH_CNT_X(schema, schemas, OB_SUCC(ret)) {
        ObTenantSensitiveRuleId id(tenant_id, (*schema)->get_sensitive_rule_id());
        if (OB_FAIL(del_sensitive_rule(id))) {
          LOG_WARN("del sensitive_rule failed",
                   "tenant_id", id.tenant_id_,
                   "sensitive_rule_id", id.schema_id_,
                   K(ret));
        }
      }
    }
  }
  return ret;
}

int ObSensitiveRuleMgr::del_sensitive_rule(const ObTenantSensitiveRuleId &id)
{
  int ret = OB_SUCCESS;
  ObSensitiveRuleSchema *schema = NULL;
  if (!id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(id));
  } else if (OB_FAIL(schema_infos_.remove_if(id,
                                             compare_with_tenant_sensitive_rule_id,
                                             equal_to_tenant_sensitive_rule_id,
                                             schema))) {
    LOG_WARN("failed to remove sensitive_rule schema", K(ret));
  } else if (OB_ISNULL(schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("removed sensitive_rule schema return NULL, ",
             "tenant_id",
             id.tenant_id_,
             "sensitive_rule_id",
             id.schema_id_,
             K(ret));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(erase_from_column_map(schema->get_sensitive_rule_id()))) {
      LOG_WARN("erase from column map failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(id_map_.erase_refactored(schema->get_sensitive_rule_id()))) {
      LOG_WARN("failed delete sensitive_rule from hashmap", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(name_map_.erase_refactored(ObSensitiveRuleNameHashKey(schema->get_tenant_id(),
                                                                      schema->get_sensitive_rule_name_str())))) {
      LOG_WARN("failed delete sensitive_rule from hashmap", K(ret));
    }
  }
  if (OB_SUCC(ret) && OB_UNLIKELY(schema_infos_.count() != name_map_.item_count()
                                  || schema_infos_.count() != id_map_.item_count())) {
    LOG_WARN("sensitive_rule schema is non-consistent",
             K(id), K(schema_infos_.count()), K(name_map_.item_count()), K(id_map_.item_count()));
  }
  LOG_DEBUG("sensitive_rule del", K(id), K(schema_infos_.count()),
            K(name_map_.item_count()), K(id_map_.item_count()));
  return ret;
}

bool ObSensitiveRuleMgr::compare_with_tenant_sensitive_rule_id(const ObSensitiveRuleSchema *lhs,
                                                               const ObTenantSensitiveRuleId &id)
{
  return NULL != lhs ? (lhs->get_tenant_sensitive_rule_id() < id) : false;
}

bool ObSensitiveRuleMgr::equal_to_tenant_sensitive_rule_id(const ObSensitiveRuleSchema *lhs,
                                                           const ObTenantSensitiveRuleId &id)
{
  return NULL != lhs ? (lhs->get_tenant_sensitive_rule_id() == id) : false;
}

int ObSensitiveRuleMgr::get_schemas_in_tenant(const uint64_t tenant_id,
    ObIArray<const ObSensitiveRuleSchema *> &schemas) const
{
  int ret = OB_SUCCESS;
  schemas.reset();
  ObTenantSensitiveRuleId id(tenant_id, OB_MIN_ID);
  ConstSensitiveRuleIter iter_begin = schema_infos_.lower_bound(id, compare_with_tenant_sensitive_rule_id);
  bool is_stop = false;
  for (ConstSensitiveRuleIter iter = iter_begin;
      OB_SUCC(ret) && iter != schema_infos_.end() && !is_stop; ++iter) {
    const ObSensitiveRuleSchema *schema = NULL;
    if (OB_ISNULL(schema = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret), K(schema));
    } else if (tenant_id != schema->get_tenant_id()) {
      is_stop = true;
    } else if (OB_FAIL(schemas.push_back(schema))) {
      LOG_WARN("push back sensitive_rule failed", K(ret));
    }
  }
  return ret;
}

int ObSensitiveRuleMgr::get_schema_count(int64_t &schema_count) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    schema_count = schema_infos_.count();
  }
  return ret;
}

int ObSensitiveRuleMgr::get_schema_statistics(ObSchemaStatisticsInfo &schema_info) const
{
  int ret = OB_SUCCESS;
  schema_info.reset();
  schema_info.schema_type_ = SENSITIVE_RULE_SCHEMA;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    schema_info.count_ = schema_infos_.size();
    for (ConstSensitiveRuleIter it = schema_infos_.begin(); OB_SUCC(ret) && it != schema_infos_.end(); it++) {
      if (OB_ISNULL(*it)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("schema is null", K(ret));
      } else {
        schema_info.size_ += (*it)->get_convert_size();
      }
    }
  }
  return ret;
}

} //end of namespace schema
} //end of namespace share
} //end of namespace oceanbase
