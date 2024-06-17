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
#include "share/schema/ob_label_se_policy_mgr.h"
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
using namespace std;
using namespace common;
using namespace hash;

ObLabelSePolicyMgr::ObLabelSePolicyMgr()
    : is_inited_(false),
      local_allocator_(SET_USE_500(ObModIds::OB_SCHEMA_GETTER_GUARD, ObCtxIds::SCHEMA_SERVICE)),
      allocator_(local_allocator_),
      schema_infos_(0, NULL, SET_USE_500(ObModIds::OB_SCHEMA_LABEL_SE_POLICY, ObCtxIds::SCHEMA_SERVICE)),
      policy_name_map_(SET_USE_500(ObModIds::OB_SCHEMA_LABEL_SE_POLICY, ObCtxIds::SCHEMA_SERVICE)),
      column_name_map_(SET_USE_500(ObModIds::OB_SCHEMA_LABEL_SE_POLICY, ObCtxIds::SCHEMA_SERVICE)),
      id_map_(SET_USE_500(ObModIds::OB_SCHEMA_LABEL_SE_POLICY, ObCtxIds::SCHEMA_SERVICE))

{
}

ObLabelSePolicyMgr::ObLabelSePolicyMgr(ObIAllocator &allocator)
    : is_inited_(false),
      local_allocator_(SET_USE_500(ObModIds::OB_SCHEMA_GETTER_GUARD, ObCtxIds::SCHEMA_SERVICE)),
      allocator_(allocator),
      schema_infos_(0, NULL, SET_USE_500(ObModIds::OB_SCHEMA_LABEL_SE_POLICY, ObCtxIds::SCHEMA_SERVICE)),
      policy_name_map_(SET_USE_500(ObModIds::OB_SCHEMA_LABEL_SE_POLICY, ObCtxIds::SCHEMA_SERVICE)),
      column_name_map_(SET_USE_500(ObModIds::OB_SCHEMA_LABEL_SE_POLICY, ObCtxIds::SCHEMA_SERVICE)),
      id_map_(SET_USE_500(ObModIds::OB_SCHEMA_LABEL_SE_POLICY, ObCtxIds::SCHEMA_SERVICE))
{
}

ObLabelSePolicyMgr::~ObLabelSePolicyMgr()
{
}

int ObLabelSePolicyMgr::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init private label security policy schema manager twice", K(ret));
  } else if (OB_FAIL(policy_name_map_.init())) {
    LOG_WARN("init hash map failed", K(ret));
  } else if (OB_FAIL(column_name_map_.init())) {
    LOG_WARN("init hash map failed", K(ret));
  } else if (OB_FAIL(id_map_.init())) {
    LOG_WARN("init hash map failed", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObLabelSePolicyMgr::reset()
{
  if (!is_inited_) {
    LOG_WARN_RET(OB_NOT_INIT, "label security policy manger not init");
  } else {
    schema_infos_.clear();
    policy_name_map_.clear();
    column_name_map_.clear();
    id_map_.clear();
  }
}

ObLabelSePolicyMgr &ObLabelSePolicyMgr::operator =(const ObLabelSePolicyMgr &other)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema manager not init", K(ret));
  } else  if (OB_FAIL(assign(other))) {
    LOG_WARN("assign failed", K(ret));
  }
  return *this;
}

int ObLabelSePolicyMgr::assign(const ObLabelSePolicyMgr &other)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema manager not init", K(ret));
  } else if (this != &other) {
    if (OB_FAIL(policy_name_map_.assign(other.policy_name_map_))) {
      LOG_WARN("assign label security policy name map failed", K(ret));
    } else if (OB_FAIL(column_name_map_.assign(other.column_name_map_))) {
      LOG_WARN("assign label security policy name map failed", K(ret));
    } else if (OB_FAIL(id_map_.assign(other.id_map_))) {
      LOG_WARN("assign label security policy id map failed", K(ret));
    } else if (OB_FAIL(schema_infos_.assign(other.schema_infos_))) {
      LOG_WARN("assign label security policy schema vector failed", K(ret));
    }
  }
  return ret;
}

int ObLabelSePolicyMgr::deep_copy(const ObLabelSePolicyMgr &other)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema manager not init", K(ret));
  } else if (this != &other) {
    reset();
    ObLabelSePolicySchema *schema = NULL;
    for (LabelSePolicyIter iter = other.schema_infos_.begin();
         OB_SUCC(ret) && iter != other.schema_infos_.end(); iter++) {
      if (OB_ISNULL(schema = *iter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", KP(schema), K(ret));
      } else if (OB_FAIL(add_label_se_policy(*schema))) {
        LOG_WARN("add outline failed", K(*schema), K(ret));
      }
    }
  }
  return ret;
}


int ObLabelSePolicyMgr::add_label_se_policy(const ObLabelSePolicySchema &schema)
{
  int ret = OB_SUCCESS;
  ObLabelSePolicySchema *new_schema = NULL;
  ObLabelSePolicySchema *old_schema = NULL;
  LabelSePolicyIter iter = NULL;
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
    LOG_WARN("failed to add label security policy schema", K(ret));
  } else {
    int over_write = 1;
    ObLabelSeNameHashKey policy_name_hash_key(new_schema->get_tenant_id(), new_schema->get_policy_name_str());
    ObLabelSeColumnNameHashKey column_name_hask_key(new_schema->get_tenant_id(), new_schema->get_column_name_str());
    if (OB_FAIL(policy_name_map_.set_refactored(policy_name_hash_key, new_schema, over_write))) {
      LOG_WARN("build label security policy hash map failed", K(ret));
    } else if (OB_FAIL(column_name_map_.set_refactored(column_name_hask_key, new_schema, over_write))) {
      LOG_WARN("build label security policy hash map failed", K(ret));
    } else if (OB_FAIL(id_map_.set_refactored(new_schema->get_label_se_policy_id(), new_schema, over_write))) {
      LOG_WARN("build label security policy id hashmap failed", K(ret),
               "label_se_policy_id", new_schema->get_label_se_policy_id());
    }
  }
  if (OB_SUCC(ret) && (schema_infos_.count() != policy_name_map_.item_count()
                       || schema_infos_.count() != column_name_map_.item_count()
                       || schema_infos_.count() != id_map_.item_count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema is inconsistent with its map", K(ret),
             K(schema_infos_.count()),
             K(policy_name_map_.item_count()),
             K(column_name_map_.item_count()),
             K(id_map_.item_count()));
  }

  LOG_DEBUG("label_se_policy add", K(schema), K(schema_infos_.count()),
            K(policy_name_map_.item_count()), K(column_name_map_.item_count()), K(id_map_.item_count()));

  return ret;
}

int ObLabelSePolicyMgr::add_label_se_policys(const ObIArray<ObLabelSePolicySchema> &schemas)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < schemas.count() && OB_SUCC(ret); ++i) {
    if (OB_FAIL(add_label_se_policy(schemas.at(i)))) {
      LOG_WARN("push schema failed", K(ret));
    }
  }
  return ret;
}

int ObLabelSePolicyMgr::get_schema_by_name(const uint64_t tenant_id,
                                           const ObString &name,
                                           const ObLabelSePolicySchema *&schema) const
{
  int ret = OB_SUCCESS;
  schema = NULL;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id
             || name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(name));
  } else {
    ObLabelSePolicySchema *tmp_schema = NULL;
    ObLabelSeNameHashKey hash_wrap(tenant_id, name);
    if (OB_FAIL(policy_name_map_.get_refactored(hash_wrap, tmp_schema))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        LOG_DEBUG("schema is not exist", K(tenant_id), K(name),
                 "map_cnt", policy_name_map_.item_count());
      }
    } else {
      schema = tmp_schema;
    }
  }
  return ret;
}

int ObLabelSePolicyMgr::get_schema_by_column_name(const uint64_t tenant_id,
                                                  const ObString &column_name,
                                                  const ObLabelSePolicySchema *&schema) const
{
  int ret = OB_SUCCESS;
  schema = NULL;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id
             || column_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(column_name));
  } else {
    ObLabelSePolicySchema *tmp_schema = NULL;
    ObLabelSeColumnNameHashKey hash_wrap(tenant_id, column_name);
    if (OB_FAIL(column_name_map_.get_refactored(hash_wrap, tmp_schema))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        LOG_DEBUG("schema is not exist", K(tenant_id), K(column_name),
                 "map_cnt", column_name_map_.item_count());
      }
    } else {
      schema = tmp_schema;
    }
  }
  return ret;
}

int ObLabelSePolicyMgr::get_schema_version_by_id(uint64_t label_se_policy_id, int64_t &schema_version) const
{
  int ret = OB_SUCCESS;
  schema_version = OB_INVALID_VERSION;
  const ObLabelSePolicySchema *schema = NULL;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == label_se_policy_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(label_se_policy_id));
  } else if (OB_FAIL(get_schema_by_id(label_se_policy_id, schema))) {
    LOG_WARN("get schema by id failed", K(ret), K(label_se_policy_id));
  } else if (OB_ISNULL(schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema is NULL", K(ret), K(label_se_policy_id));
  } else {
    schema_version = schema->get_schema_version();
  }
  return ret;
}

int ObLabelSePolicyMgr::del_schemas_in_tenant(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else {
    ObArray<const ObLabelSePolicySchema *> schemas;
    if (OB_FAIL(get_schemas_in_tenant(tenant_id, schemas))) {
      LOG_WARN("get label security policy schemas failed", K(ret), K(tenant_id));
    } else {
      FOREACH_CNT_X(schema, schemas, OB_SUCC(ret)) {
        ObTenantLabelSePolicyId id(tenant_id, (*schema)->get_label_se_policy_id());
        if (OB_FAIL(del_label_se_policy(id))) {
          LOG_WARN("del label security policy failed",
                   "tenant_id", id.tenant_id_,
                   "label_se_policy_id", id.label_se_policy_id_,
                   K(ret));
        }
      }
    }
  }
  return ret;
}

int ObLabelSePolicyMgr::del_label_se_policy(const ObTenantLabelSePolicyId &id)
{
  int ret = OB_SUCCESS;
  ObLabelSePolicySchema *schema = NULL;
  if (!id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(id));
  } else if (OB_FAIL(schema_infos_.remove_if(id,
                                             compare_with_tenant_label_se_policy_id,
                                             equal_to_tenant_label_se_policy_id,
                                             schema))) {
    LOG_WARN("failed to remove policy schema", K(ret));
  } else if (OB_ISNULL(schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("removed policy schema return NULL, ",
             "tenant_id",
             id.tenant_id_,
             "label_se_policy_id",
             id.label_se_policy_id_,
             K(ret));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(id_map_.erase_refactored(schema->get_label_se_policy_id()))) {
      LOG_WARN("failed delete label security policy from hashmap", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(policy_name_map_.erase_refactored(
                  ObLabelSeNameHashKey(schema->get_tenant_id(), schema->get_policy_name())))) {
      LOG_WARN("failed delete label security policy from hashmap", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(column_name_map_.erase_refactored(
                  ObLabelSeColumnNameHashKey(schema->get_tenant_id(), schema->get_column_name_str())))) {
      LOG_WARN("failed delete label security policy from hashmap", K(ret));
    }
  }

  if (OB_SUCC(ret) && OB_UNLIKELY(schema_infos_.count() != policy_name_map_.item_count()
                                  || schema_infos_.count() != column_name_map_.item_count()
                                  || schema_infos_.count() != id_map_.item_count())) {
    LOG_WARN("policy schema is non-consistent",K(id), K(schema_infos_.count()),
             K(policy_name_map_.item_count()), K(column_name_map_.item_count()), K(id_map_.item_count()));
  }

  LOG_DEBUG("label_se_policy del", K(id), K(schema_infos_.count()),
            K(policy_name_map_.item_count()), K(column_name_map_.item_count()), K(id_map_.item_count()));

  return ret;
}

bool ObLabelSePolicyMgr::compare_with_tenant_label_se_policy_id(const ObLabelSePolicySchema *lhs,
                                                         const ObTenantLabelSePolicyId &id)
{
  return NULL != lhs ? (lhs->get_tenant_label_se_policy_id() < id) : false;
}

bool ObLabelSePolicyMgr::equal_to_tenant_label_se_policy_id(const ObLabelSePolicySchema *lhs,
                                                     const ObTenantLabelSePolicyId &id)
{
  return NULL != lhs ? (lhs->get_tenant_label_se_policy_id() == id) : false;
}

int ObLabelSePolicyMgr::get_schemas_in_tenant(const uint64_t tenant_id,
    ObIArray<const ObLabelSePolicySchema *> &schemas) const
{
  int ret = OB_SUCCESS;
  schemas.reset();

  ObTenantLabelSePolicyId id(tenant_id, OB_MIN_ID);
  ConstLabelSePolicyIter iter_begin =
      schema_infos_.lower_bound(id, compare_with_tenant_label_se_policy_id);
  bool is_stop = false;
  for (ConstLabelSePolicyIter iter = iter_begin;
      OB_SUCC(ret) && iter != schema_infos_.end() && !is_stop; ++iter) {
    const ObLabelSePolicySchema *schema = NULL;
    if (OB_ISNULL(schema = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret), K(schema));
    } else if (tenant_id != schema->get_tenant_id()) {
      is_stop = true;
    } else if (OB_FAIL(schemas.push_back(schema))) {
      LOG_WARN("push back label security policy failed", K(ret));
    }
  }

  return ret;
}

int ObLabelSePolicyMgr::get_schema_by_id(const uint64_t label_se_policy_id, const ObLabelSePolicySchema *&schema) const
{
  int ret = OB_SUCCESS;
  schema = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == label_se_policy_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(label_se_policy_id));
  } else {
    ObLabelSePolicySchema *tmp_schema = NULL;
    int hash_ret = id_map_.get_refactored(label_se_policy_id, tmp_schema);
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

int ObLabelSePolicyMgr::get_schema_count(int64_t &schema_count) const
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

int ObLabelSePolicyMgr::get_schema_statistics(ObSchemaStatisticsInfo &schema_info) const
{
  int ret = OB_SUCCESS;
  schema_info.reset();
  schema_info.schema_type_ = LABEL_SE_POLICY_SCHEMA;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    schema_info.count_ = schema_infos_.size();
    for (ConstLabelSePolicyIter it = schema_infos_.begin(); OB_SUCC(ret) && it != schema_infos_.end(); it++) {
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

ObLabelSeCompMgr::ObLabelSeCompMgr()
    : is_inited_(false),
      local_allocator_(SET_USE_500(ObModIds::OB_SCHEMA_GETTER_GUARD, ObCtxIds::SCHEMA_SERVICE)),
      allocator_(local_allocator_),
      schema_infos_(0, NULL, SET_USE_500(ObModIds::OB_SCHEMA_LABEL_SE_COMPONENT, ObCtxIds::SCHEMA_SERVICE)),
      short_name_map_(SET_USE_500(ObModIds::OB_SCHEMA_LABEL_SE_COMPONENT, ObCtxIds::SCHEMA_SERVICE)),
      long_name_map_(SET_USE_500(ObModIds::OB_SCHEMA_LABEL_SE_COMPONENT, ObCtxIds::SCHEMA_SERVICE)),
      id_map_(SET_USE_500(ObModIds::OB_SCHEMA_LABEL_SE_COMPONENT, ObCtxIds::SCHEMA_SERVICE)),
      num_map_(SET_USE_500(ObModIds::OB_SCHEMA_LABEL_SE_COMPONENT, ObCtxIds::SCHEMA_SERVICE))

{
}

ObLabelSeCompMgr::ObLabelSeCompMgr(ObIAllocator &allocator)
    : is_inited_(false),
      local_allocator_(SET_USE_500(ObModIds::OB_SCHEMA_GETTER_GUARD, ObCtxIds::SCHEMA_SERVICE)),
      allocator_(allocator),
      schema_infos_(0, NULL, SET_USE_500(ObModIds::OB_SCHEMA_LABEL_SE_COMPONENT, ObCtxIds::SCHEMA_SERVICE)),
      short_name_map_(SET_USE_500(ObModIds::OB_SCHEMA_LABEL_SE_COMPONENT, ObCtxIds::SCHEMA_SERVICE)),
      long_name_map_(SET_USE_500(ObModIds::OB_SCHEMA_LABEL_SE_COMPONENT, ObCtxIds::SCHEMA_SERVICE)),
      id_map_(SET_USE_500(ObModIds::OB_SCHEMA_LABEL_SE_COMPONENT, ObCtxIds::SCHEMA_SERVICE)),
      num_map_(SET_USE_500(ObModIds::OB_SCHEMA_LABEL_SE_COMPONENT, ObCtxIds::SCHEMA_SERVICE))
{
}

ObLabelSeCompMgr::~ObLabelSeCompMgr()
{
}

int ObLabelSeCompMgr::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(long_name_map_.init())) {
    LOG_WARN("hash map init failed", K(ret));
  } else if (OB_FAIL(short_name_map_.init())) {
    LOG_WARN("hash map init failed", K(ret));
  } else if (OB_FAIL(id_map_.init())) {
    LOG_WARN("hash map init failed", K(ret));
  } else if (OB_FAIL(num_map_.init())) {
    LOG_WARN("hash map init failed", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObLabelSeCompMgr::reset()
{
  if (!is_inited_) {
    LOG_ERROR_RET(OB_NOT_INIT, "schema mgr not inited");
  } else {
    schema_infos_.clear();
    long_name_map_.clear();
    short_name_map_.clear();
    id_map_.clear();
    num_map_.clear();
  }
}

ObLabelSeCompMgr &ObLabelSeCompMgr::operator =(const ObLabelSeCompMgr &other)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema mgr not inited", K(ret));
  } else  if (OB_FAIL(assign(other))) {
    LOG_WARN("assign failed", K(ret));
  }
  return *this;
}

int ObLabelSeCompMgr::assign(const ObLabelSeCompMgr &other)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema mgr not inited", K(ret));
  } else if (this != &other) {
    if (OB_FAIL(long_name_map_.assign(other.long_name_map_))) {
      LOG_WARN("assign hash map failed", K(ret));
    } else if (OB_FAIL(short_name_map_.assign(other.short_name_map_))) {
      LOG_WARN("assign hash map failed", K(ret));
    } else if (OB_FAIL(id_map_.assign(other.id_map_))) {
      LOG_WARN("assign hash map failed", K(ret));
    } else if (OB_FAIL(num_map_.assign(other.num_map_))) {
      LOG_WARN("assign hash map failed", K(ret));
    } else if (OB_FAIL(schema_infos_.assign(other.schema_infos_))) {
      LOG_WARN("assign hash map failed", K(ret));
    }
  }
  return ret;
}

int ObLabelSeCompMgr::deep_copy(const ObLabelSeCompMgr &other)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema mgr not inited", K(ret));
  } else if (this != &other) {
    reset();
    ObLabelSeComponentSchema *schema = NULL;
    for (LabelSeCompIter iter = other.schema_infos_.begin();
         OB_SUCC(ret) && iter != other.schema_infos_.end(); iter++) {
      if (OB_ISNULL(schema = *iter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", KP(schema), K(ret));
      } else if (OB_FAIL(add_label_se_component(*schema))) {
        LOG_WARN("add schema failed", K(*schema), K(ret));
      }
    }
  }
  return ret;
}


int ObLabelSeCompMgr::add_label_se_component(const ObLabelSeComponentSchema &schema)
{
  int ret = OB_SUCCESS;
  ObLabelSeComponentSchema *new_schema = NULL;
  ObLabelSeComponentSchema *old_schema = NULL;
  LabelSeCompIter iter = NULL;
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
    LOG_WARN("failed to add schema", K(ret));
  } else {
    int over_write = 1;
    ObLabelSeCompLongNameHashKey new_long_name_key(new_schema->get_tenant_label_se_policy_id(),
                                                   new_schema->get_comp_type(),
                                                   new_schema->get_long_name_str());
    ObLabelSeCompShortNameHashKey new_short_name_key(new_schema->get_tenant_label_se_policy_id(),
                                                     new_schema->get_comp_type(),
                                                     new_schema->get_short_name_str());
    ObLabelSeCompNumHashKey new_num_key(new_schema->get_tenant_label_se_policy_id(),
                                        new_schema->get_comp_type(),
                                        new_schema->get_comp_num());

    if (OB_NOT_NULL(old_schema)) {
      ObLabelSeCompLongNameHashKey old_long_name_key(old_schema->get_tenant_label_se_policy_id(),
                                                     old_schema->get_comp_type(),
                                                     old_schema->get_long_name_str());
      ObLabelSeCompShortNameHashKey old_short_name_key(old_schema->get_tenant_label_se_policy_id(),
                                                       old_schema->get_comp_type(),
                                                       old_schema->get_short_name_str());
      ObLabelSeCompNumHashKey old_num_key(old_schema->get_tenant_label_se_policy_id(),
                                          old_schema->get_comp_type(),
                                          old_schema->get_comp_num());
      if (!(old_long_name_key == new_long_name_key)
          && OB_FAIL(long_name_map_.erase_refactored(old_long_name_key))) {
        LOG_WARN("fail to erase hash map", K(ret));
      } else if (!(old_short_name_key == new_short_name_key)
                 && OB_FAIL(short_name_map_.erase_refactored(old_short_name_key))) {
        LOG_WARN("fail to erase hash map", K(ret));
      } else if (!(old_num_key == new_num_key)
                 && OB_FAIL(num_map_.erase_refactored(old_num_key))) {
        LOG_WARN("fail to erase hash map", K(ret));
      }
    }

    if (FAILEDx(long_name_map_.set_refactored(new_long_name_key,
                                              new_schema, over_write))) {
      LOG_WARN("update hash map failed", K(ret));
    } else if (OB_FAIL(short_name_map_.set_refactored(new_short_name_key,
                                                      new_schema, over_write))) {
      LOG_WARN("update hash map failed", K(ret));
    } else if (OB_FAIL(id_map_.set_refactored(new_schema->get_label_se_component_id(),
                                              new_schema, over_write))) {
      LOG_WARN("update hash map failed", K(ret));
    } else if (OB_FAIL(num_map_.set_refactored(new_num_key, new_schema, over_write))) {
      LOG_WARN("update hash map failed", K(ret));
    } else {
      LOG_DEBUG("add label security component schema succ", K(*new_schema));
    }
  }

  if (OB_SUCC(ret) && (schema_infos_.count() != long_name_map_.item_count()
                       || schema_infos_.count() != short_name_map_.item_count()
                       || schema_infos_.count() != id_map_.item_count()
                       || schema_infos_.count() != num_map_.item_count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema is inconsistent with its map",
             K(schema_infos_.count()),
             K(long_name_map_.item_count()),
             K(short_name_map_.item_count()),
             K(id_map_.item_count()),
             K(num_map_.item_count()));
  }

  return ret;
}

int ObLabelSeCompMgr::add_label_se_components(const ObIArray<ObLabelSeComponentSchema> &schemas)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < schemas.count() && OB_SUCC(ret); ++i) {
    if (OB_FAIL(add_label_se_component(schemas.at(i)))) {
      LOG_WARN("push schema failed", K(ret));
    }
  }
  return ret;
}

int ObLabelSeCompMgr::get_schema_by_short_name(const ObTenantLabelSePolicyId &id,
                                               const int64_t comp_type,
                                               const ObString &name,
                                               const ObLabelSeComponentSchema *&schema) const
{
  int ret = OB_SUCCESS;
  schema = NULL;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!id.is_valid()
             || comp_type < 0
             || name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(id), K(comp_type), K(name));
  } else {
    ObLabelSeComponentSchema *tmp_schema = NULL;
    ObLabelSeCompShortNameHashKey hash_wrap(id, comp_type, name);
    if (OB_FAIL(short_name_map_.get_refactored(hash_wrap, tmp_schema))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        LOG_DEBUG("schema is not exist", K(ret));
      }
    } else {
      schema = tmp_schema;
    }
  }
  return ret;
}

int ObLabelSeCompMgr::get_schema_by_long_name(const ObTenantLabelSePolicyId &id,
                                              const int64_t comp_type,
                                              const ObString &name,
                                              const ObLabelSeComponentSchema *&schema) const
{
  int ret = OB_SUCCESS;
  schema = NULL;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!id.is_valid()
             || comp_type < 0
             || name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(id), K(comp_type), K(name));
  } else {
    ObLabelSeComponentSchema *tmp_schema = NULL;
    ObLabelSeCompLongNameHashKey hash_wrap(id, comp_type, name);
    if (OB_FAIL(long_name_map_.get_refactored(hash_wrap, tmp_schema))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        LOG_DEBUG("schema is not exist", K(ret));
      }
    } else {
      schema = tmp_schema;
    }
  }
  return ret;
}

int ObLabelSeCompMgr::get_schema_by_comp_num(const ObTenantLabelSePolicyId &id,
                                             const int64_t comp_type,
                                             const int64_t comp_num,
                                             const ObLabelSeComponentSchema *&schema) const
{
  int ret = OB_SUCCESS;
  schema = NULL;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!id.is_valid()
             || comp_type < 0
             || comp_num < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(id), K(comp_type), K(comp_num));
  } else {
    ObLabelSeComponentSchema *tmp_schema = NULL;
    ObLabelSeCompNumHashKey hash_wrap(id, comp_type, comp_num);
    if (OB_FAIL(num_map_.get_refactored(hash_wrap, tmp_schema))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        LOG_DEBUG("schema is not exist", K(ret));
      }
    } else {
      schema = tmp_schema;
    }
  }
  return ret;
}

int ObLabelSeCompMgr::get_schema_version_by_id(uint64_t label_se_comp_id, int64_t &schema_version) const
{
  int ret = OB_SUCCESS;
  schema_version = OB_INVALID_VERSION;
  const ObLabelSeComponentSchema *schema = NULL;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == label_se_comp_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(label_se_comp_id));
  } else if (OB_FAIL(get_schema_by_id(label_se_comp_id, schema))) {
    LOG_WARN("get schema by id failed", K(ret), K(label_se_comp_id));
  } else if (OB_ISNULL(schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema is NULL", K(ret), K(label_se_comp_id));
  } else {
    schema_version = schema->get_schema_version();
  }
  return ret;
}

int ObLabelSeCompMgr::del_schemas_in_tenant(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else {
    ObArray<const ObLabelSeComponentSchema *> schemas;
    if (OB_FAIL(get_schemas_in_tenant(tenant_id, schemas))) {
      LOG_WARN("get schemas failed", K(ret), K(tenant_id));
    } else {
      FOREACH_CNT_X(schema, schemas, OB_SUCC(ret)) {
        ObTenantLabelSeComponentId id((*schema)->get_tenant_label_se_component_id());
        if (OB_FAIL(del_label_se_component(id))) {
          LOG_WARN("del schema failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObLabelSeCompMgr::del_schemas_in_policy(const uint64_t tenant_id, const uint64_t policy_id)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == tenant_id
     || OB_INVALID_ID == policy_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(policy_id));
  } else {
    ObArray<const ObLabelSeComponentSchema *> schemas;
    if (OB_FAIL(get_schemas_in_policy(tenant_id, policy_id, schemas))) {
      LOG_WARN("get schemas failed", K(ret), K(tenant_id));
    } else {
      FOREACH_CNT_X(schema, schemas, OB_SUCC(ret)) {
        ObTenantLabelSeComponentId id((*schema)->get_tenant_label_se_component_id());
        if (OB_FAIL(del_label_se_component(id))) {
          LOG_WARN("del schema failed", K(ret));
        }
      }
    }
  }
  return ret;
}


int ObLabelSeCompMgr::del_label_se_component(const ObTenantLabelSeComponentId &id)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;
  ObLabelSeComponentSchema *schema = NULL;
  if (!id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(id));
  } else if (OB_FAIL(schema_infos_.remove_if(id,
                                             compare_with_tenant_label_se_comp_id,
                                             equal_to_tenant_label_se_comp_id,
                                             schema))) {
    LOG_WARN("failed to remove schema", K(ret));
  } else if (OB_ISNULL(schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("removed schema return NULL", K(ret));
  }

  if (OB_SUCC(ret)) {
    hash_ret = id_map_.erase_refactored(schema->get_label_se_component_id());
    if (OB_UNLIKELY(OB_SUCCESS != hash_ret)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("erase hash map failed", K(ret), K(hash_ret));
    }
  }

  if (OB_SUCC(ret)) {
    hash_ret = num_map_.erase_refactored(ObLabelSeCompNumHashKey(schema->get_tenant_label_se_policy_id(),
                                                                 schema->get_comp_type(),
                                                                 schema->get_comp_num()));
    if (OB_UNLIKELY(OB_SUCCESS != hash_ret)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("erase hash map failed", K(ret), K(hash_ret));
    }
  }

  if (OB_SUCC(ret)) {
    ObLabelSeCompShortNameHashKey the_hash_key(schema->get_tenant_label_se_policy_id(),
                                               schema->get_comp_type(),
                                               schema->get_short_name_str());
    hash_ret = short_name_map_.erase_refactored(the_hash_key);
    if (OB_SUCCESS != hash_ret) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("erase hash map failed", K(ret), K(hash_ret));
    }
  }

  if (OB_SUCC(ret)) {
    ObLabelSeCompLongNameHashKey the_hash_key(schema->get_tenant_label_se_policy_id(),
                                               schema->get_comp_type(),
                                               schema->get_long_name_str());
    hash_ret = long_name_map_.erase_refactored(the_hash_key);
    if (OB_SUCCESS != hash_ret) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("erase hash map failed", K(ret), K(hash_ret));
    }
  }

  if (OB_SUCC(ret) && (schema_infos_.count() != long_name_map_.item_count()
                       || schema_infos_.count() != short_name_map_.item_count()
                       || schema_infos_.count() != id_map_.item_count()
                       || schema_infos_.count() != num_map_.item_count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema is inconsistent with its map",
             K(schema_infos_.count()),
             K(long_name_map_.item_count()),
             K(short_name_map_.item_count()),
             K(id_map_.item_count()),
             K(num_map_.item_count()));
  }
  return ret;
}

bool ObLabelSeCompMgr::compare_with_tenant_label_se_comp_id(const ObLabelSeComponentSchema *lhs,
                                                            const ObTenantLabelSeComponentId &id)
{
  return NULL != lhs ? (lhs->get_tenant_label_se_component_id() < id) : false;
}

bool ObLabelSeCompMgr::equal_to_tenant_label_se_comp_id(const ObLabelSeComponentSchema *lhs,
                                                        const ObTenantLabelSeComponentId &id)
{
  return NULL != lhs ? (lhs->get_tenant_label_se_component_id() == id) : false;
}

int ObLabelSeCompMgr::get_schemas_in_tenant(const uint64_t tenant_id,
    ObIArray<const ObLabelSeComponentSchema *> &schemas) const
{
  int ret = OB_SUCCESS;
  schemas.reset();

  ObTenantLabelSeComponentId id(tenant_id, OB_MIN_ID);
  ConstLabelSeCompIter iter_begin =
      schema_infos_.lower_bound(id, compare_with_tenant_label_se_comp_id);
  bool is_stop = false;
  for (ConstLabelSeCompIter iter = iter_begin;
      OB_SUCC(ret) && iter != schema_infos_.end() && !is_stop; ++iter) {
    const ObLabelSeComponentSchema *schema = NULL;
    if (OB_ISNULL(schema = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret), K(schema));
    } else if (tenant_id != schema->get_tenant_id()) {
      is_stop = true;
    } else if (OB_FAIL(schemas.push_back(schema))) {
      LOG_WARN("push back schema failed", K(ret));
    }
  }

  return ret;
}

int ObLabelSeCompMgr::get_schemas_in_policy(const uint64_t tenant_id,
                                            const uint64_t policy_id,
                                            ObIArray<const ObLabelSeComponentSchema *> &schemas) const
{
  int ret = OB_SUCCESS;
  schemas.reset();

  ObTenantLabelSeComponentId id(tenant_id, OB_MIN_ID);
  ConstLabelSeCompIter iter_begin =
      schema_infos_.lower_bound(id, compare_with_tenant_label_se_comp_id);
  bool is_stop = false;
  for (ConstLabelSeCompIter iter = iter_begin;
      OB_SUCC(ret) && iter != schema_infos_.end() && !is_stop; ++iter) {
    const ObLabelSeComponentSchema *schema = NULL;
    if (OB_ISNULL(schema = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret), K(schema));
    } else if (tenant_id != schema->get_tenant_id()) {
      is_stop = true;
    } else if (policy_id == schema->get_label_se_policy_id()) {
      if (OB_FAIL(schemas.push_back(schema))) {
        LOG_WARN("push back schema failed", K(ret));
      }
    }
  }

  return ret;
}

int ObLabelSeCompMgr::get_schema_by_id(const uint64_t label_se_comp_id, const ObLabelSeComponentSchema *&schema) const
{
  int ret = OB_SUCCESS;
  schema = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == label_se_comp_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(label_se_comp_id));
  } else {
    ObLabelSeComponentSchema *tmp_schema = NULL;
    int hash_ret = id_map_.get_refactored(label_se_comp_id, tmp_schema);
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

int ObLabelSeCompMgr::get_schema_count(int64_t &schema_count) const
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

int ObLabelSeCompMgr::get_schema_statistics(ObSchemaStatisticsInfo &schema_info) const
{
  int ret = OB_SUCCESS;
  schema_info.reset();
  schema_info.schema_type_ = LABEL_SE_COMPONENT_SCHEMA;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    schema_info.count_ = schema_infos_.size();
    for (ConstLabelSeCompIter it = schema_infos_.begin(); OB_SUCC(ret) && it != schema_infos_.end(); it++) {
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

//label schema mgr


ObLabelSeLabelMgr::ObLabelSeLabelMgr()
    : is_inited_(false),
      local_allocator_(SET_USE_500(ObModIds::OB_SCHEMA_GETTER_GUARD, ObCtxIds::SCHEMA_SERVICE)),
      allocator_(local_allocator_),
      schema_infos_(0, NULL, SET_USE_500(ObModIds::OB_SCHEMA_LABEL_SE_LABEL, ObCtxIds::SCHEMA_SERVICE)),
      label_map_(SET_USE_500(ObModIds::OB_SCHEMA_LABEL_SE_LABEL, ObCtxIds::SCHEMA_SERVICE)),
      id_map_(SET_USE_500(ObModIds::OB_SCHEMA_LABEL_SE_LABEL, ObCtxIds::SCHEMA_SERVICE)),
      tag_map_(SET_USE_500(ObModIds::OB_SCHEMA_LABEL_SE_LABEL, ObCtxIds::SCHEMA_SERVICE))

{
}

ObLabelSeLabelMgr::ObLabelSeLabelMgr(ObIAllocator &allocator)
    : is_inited_(false),
      local_allocator_(SET_USE_500(ObModIds::OB_SCHEMA_GETTER_GUARD, ObCtxIds::SCHEMA_SERVICE)),
      allocator_(allocator),
      schema_infos_(0, NULL, SET_USE_500(ObModIds::OB_SCHEMA_LABEL_SE_LABEL, ObCtxIds::SCHEMA_SERVICE)),
      label_map_(SET_USE_500(ObModIds::OB_SCHEMA_LABEL_SE_LABEL, ObCtxIds::SCHEMA_SERVICE)),
      id_map_(SET_USE_500(ObModIds::OB_SCHEMA_LABEL_SE_LABEL, ObCtxIds::SCHEMA_SERVICE)),
      tag_map_(SET_USE_500(ObModIds::OB_SCHEMA_LABEL_SE_LABEL, ObCtxIds::SCHEMA_SERVICE))
{
}

ObLabelSeLabelMgr::~ObLabelSeLabelMgr()
{
}

int ObLabelSeLabelMgr::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init private label security policy schema manager twice", K(ret));
  } else if (OB_FAIL(label_map_.init())) {
    LOG_WARN("init private label security policy schema name map failed", K(ret));
  } else if (OB_FAIL(id_map_.init())) {
    LOG_WARN("init private label security policy schema id map failed", K(ret));
  } else if (OB_FAIL(tag_map_.init())) {
    LOG_WARN("init private label security policy schema id map failed", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObLabelSeLabelMgr::reset()
{
  if (!is_inited_) {
    LOG_WARN_RET(OB_NOT_INIT, "label security policy manger not init");
  } else {
    schema_infos_.clear();
    label_map_.clear();
    id_map_.clear();
    tag_map_.clear();
  }
}

ObLabelSeLabelMgr &ObLabelSeLabelMgr::operator =(const ObLabelSeLabelMgr &other)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema manager not init", K(ret));
  } else  if (OB_FAIL(assign(other))) {
    LOG_WARN("assign failed", K(ret));
  }
  return *this;
}

int ObLabelSeLabelMgr::assign(const ObLabelSeLabelMgr &other)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema manager not init", K(ret));
  } else if (this != &other) {
    if (OB_FAIL(label_map_.assign(other.label_map_))) {
      LOG_WARN("assign label security policy name map failed", K(ret));
    } else if (OB_FAIL(id_map_.assign(other.id_map_))) {
      LOG_WARN("assign label security policy id map failed", K(ret));
    } else if (OB_FAIL(tag_map_.assign(other.tag_map_))) {
      LOG_WARN("assign label security policy id map failed", K(ret));
    } else if (OB_FAIL(schema_infos_.assign(other.schema_infos_))) {
      LOG_WARN("assign label security policy schema vector failed", K(ret));
    }
  }
  return ret;
}

int ObLabelSeLabelMgr::deep_copy(const ObLabelSeLabelMgr &other)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema manager not init", K(ret));
  } else if (this != &other) {
    reset();
    ObLabelSeLabelSchema *schema = NULL;
    for (LabelSeLabelIter iter = other.schema_infos_.begin();
         OB_SUCC(ret) && iter != other.schema_infos_.end(); iter++) {
      if (OB_ISNULL(schema = *iter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", KP(schema), K(ret));
      } else if (OB_FAIL(add_label_se_label(*schema))) {
        LOG_WARN("add label security component failed", K(*schema), K(ret));
      }
    }
  }
  return ret;
}


int ObLabelSeLabelMgr::add_label_se_label(const ObLabelSeLabelSchema &schema)
{
  int ret = OB_SUCCESS;
  ObLabelSeLabelSchema *new_schema = NULL;
  ObLabelSeLabelSchema *old_schema = NULL;
  LabelSeLabelIter iter = NULL;
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
    LOG_WARN("failed to add label security policy schema", K(ret));
  } else {
    int overwrite = 1;
    ObLabelSeNameHashKey new_label_key(new_schema->get_tenant_id(),
                                       new_schema->get_label_str());
    ObLabelSeLabelTagHashKey new_tag_key(new_schema->get_tenant_id(),
                                         new_schema->get_label_tag());

    if (OB_NOT_NULL(old_schema)) {
      ObLabelSeNameHashKey old_label_key(old_schema->get_tenant_id(),
                                         old_schema->get_label_str());
      ObLabelSeLabelTagHashKey old_tag_key(old_schema->get_tenant_id(),
                                           old_schema->get_label_tag());
      if (!(old_label_key == new_label_key)
          && OB_FAIL(label_map_.erase_refactored(old_label_key))) {
        LOG_WARN("build label security policy hash map failed", K(ret));
      } else if (!(old_tag_key == new_tag_key)
                 && OB_FAIL(tag_map_.erase_refactored(old_tag_key))) {
        LOG_WARN("build label security policy id hashmap failed", K(ret));
      }
    }

    if (OB_FAIL(id_map_.set_refactored(new_schema->get_label_se_label_id(),
                                       new_schema, overwrite))) {
      LOG_WARN("build label security policy id hashmap failed", K(ret));
    } else if (OB_FAIL(label_map_.set_refactored(new_label_key, new_schema, overwrite))) {
      LOG_WARN("build label security policy hash map failed", K(ret));
    } else if (OB_FAIL(tag_map_.set_refactored(new_tag_key, new_schema, overwrite))) {
      LOG_WARN("build label security policy id hashmap failed", K(ret));
    } else {
      LOG_DEBUG("add schema pointer to maps", K(*new_schema));
    }
  }

  if (OB_SUCC(ret) && (schema_infos_.count() != label_map_.item_count()
                       || schema_infos_.count() != id_map_.item_count()
                       || schema_infos_.count() != tag_map_.item_count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema is inconsistent with its map",
             K(schema_infos_.count()),
             K(label_map_.item_count()),
             K(id_map_.item_count()),
             K(tag_map_.item_count()));
  }

  return ret;
}

int ObLabelSeLabelMgr::add_label_se_labels(const ObIArray<ObLabelSeLabelSchema> &schemas)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < schemas.count() && OB_SUCC(ret); ++i) {
    if (OB_FAIL(add_label_se_label(schemas.at(i)))) {
      LOG_WARN("push schema failed", K(ret));
    }
  }
  return ret;
}

int ObLabelSeLabelMgr::get_schema_by_label(uint64_t tenant_id,
                                           const ObString &label,
                                           const ObLabelSeLabelSchema *&schema) const
{
  int ret = OB_SUCCESS;
  schema = NULL;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id
             || label.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(label));
  } else {
    ObLabelSeLabelSchema *tmp_schema = NULL;
    ObLabelSeNameHashKey hash_wrap(tenant_id, label);
    if (OB_FAIL(label_map_.get_refactored(hash_wrap, tmp_schema))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        LOG_DEBUG("schema is not exist", K(tenant_id), K(label),
                 "map_cnt", label_map_.item_count());
      }
    } else {
      schema = tmp_schema;
    }
  }
  return ret;
}

int ObLabelSeLabelMgr::get_schema_by_label_tag(const uint64_t tenant_id,
                                               const int64_t label_tag,
                                               const ObLabelSeLabelSchema *&schema) const
{
  int ret = OB_SUCCESS;
  schema = NULL;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id
             || label_tag < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(label_tag));
  } else {
    ObLabelSeLabelSchema *tmp_schema = NULL;
    ObLabelSeLabelTagHashKey hash_wrap(tenant_id, label_tag);
    if (OB_FAIL(tag_map_.get_refactored(hash_wrap, tmp_schema))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        LOG_DEBUG("schema is not exist", K(tenant_id), K(label_tag),
                 "map_cnt", tag_map_.item_count());
      }
    } else {
      schema = tmp_schema;
    }
  }
  return ret;
}

int ObLabelSeLabelMgr::get_schema_version_by_id(uint64_t label_se_label_id, int64_t &schema_version) const
{
  int ret = OB_SUCCESS;
  schema_version = OB_INVALID_VERSION;
  const ObLabelSeLabelSchema *schema = NULL;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == label_se_label_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(label_se_label_id));
  } else if (OB_FAIL(get_schema_by_id(label_se_label_id, schema))) {
    LOG_WARN("get schema by id failed", K(ret), K(label_se_label_id));
  } else if (OB_ISNULL(schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema is NULL", K(ret), K(label_se_label_id));
  } else {
    schema_version = schema->get_schema_version();
  }
  return ret;
}

int ObLabelSeLabelMgr::del_schemas_in_tenant(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else {
    ObArray<const ObLabelSeLabelSchema *> schemas;
    if (OB_FAIL(get_schemas_in_tenant(tenant_id, schemas))) {
      LOG_WARN("get label security policy schemas failed", K(ret), K(tenant_id));
    } else {
      FOREACH_CNT_X(schema, schemas, OB_SUCC(ret)) {
        ObTenantLabelSeLabelId id((*schema)->get_tenant_label_se_label_id());
        if (OB_FAIL(del_label_se_label(id))) {
          LOG_WARN("del label security policy failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObLabelSeLabelMgr::del_schemas_in_policy(const uint64_t tenant_id, const uint64_t policy_id)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else {
    ObArray<const ObLabelSeLabelSchema *> schemas;
    if (OB_FAIL(get_schemas_in_policy(tenant_id, policy_id, schemas))) {
      LOG_WARN("get label security policy schemas failed", K(ret), K(tenant_id));
    } else {
      FOREACH_CNT_X(schema, schemas, OB_SUCC(ret)) {
        ObTenantLabelSeLabelId id((*schema)->get_tenant_label_se_label_id());
        if (OB_FAIL(del_label_se_label(id))) {
          LOG_WARN("del label security policy failed",
                   "tenant_id", id.tenant_id_,
                   "label_se_label_id", id.label_se_label_id_,
                   K(ret));
        }
      }
    }
  }
  return ret;
}


int ObLabelSeLabelMgr::del_label_se_label(const ObTenantLabelSeLabelId &id)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;
  ObLabelSeLabelSchema *schema = NULL;
  if (!id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(id));
  } else if (OB_FAIL(schema_infos_.remove_if(id,
                                             compare_with_tenant_label_se_label_id,
                                             equal_to_tenant_label_se_label_id,
                                             schema))) {
    LOG_WARN("failed to remove schema", K(ret));
  } else if (OB_ISNULL(schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("removed schema return NULL", K(ret));
  }

  if (OB_SUCC(ret)) {
    hash_ret = id_map_.erase_refactored(schema->get_label_se_label_id());
    if (OB_UNLIKELY(OB_SUCCESS != hash_ret)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to erase hash map", K(ret), K(hash_ret));
    }
  }

  if (OB_SUCC(ret)) {
    hash_ret = tag_map_.erase_refactored(ObLabelSeLabelTagHashKey(schema->get_tenant_id(),
                                                                  schema->get_label_tag()));
    if (OB_UNLIKELY(OB_SUCCESS != hash_ret)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("erase hash map failed", K(ret), K(hash_ret));
    }
  }

  if (OB_SUCC(ret)) {
    ObLabelSeNameHashKey the_hash_key(schema->get_tenant_id(), schema->get_label_str());
    hash_ret = label_map_.erase_refactored(the_hash_key);
    if (OB_SUCCESS != hash_ret) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("erase hash map failed", K(ret), K(hash_ret));
    }
  }

  if (OB_SUCC(ret) && (schema_infos_.count() != label_map_.item_count()
                       || schema_infos_.count() != id_map_.item_count()
                       || schema_infos_.count() != tag_map_.item_count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema is inconsistent with its map",
             K(schema_infos_.count()),
             K(label_map_.item_count()),
             K(id_map_.item_count()),
             K(tag_map_.item_count()));
  }
  return ret;
}

bool ObLabelSeLabelMgr::compare_with_tenant_label_se_label_id(const ObLabelSeLabelSchema *lhs,
                                                              const ObTenantLabelSeLabelId &id)
{
  return NULL != lhs ? (lhs->get_tenant_label_se_label_id() < id) : false;
}

bool ObLabelSeLabelMgr::equal_to_tenant_label_se_label_id(const ObLabelSeLabelSchema *lhs,
                                                          const ObTenantLabelSeLabelId &id)
{
  return NULL != lhs ? (lhs->get_tenant_label_se_label_id() == id) : false;
}

int ObLabelSeLabelMgr::get_schemas_in_tenant(const uint64_t tenant_id,
    ObIArray<const ObLabelSeLabelSchema *> &schemas) const
{
  int ret = OB_SUCCESS;
  schemas.reset();

  ObTenantLabelSeLabelId id(tenant_id, OB_MIN_ID);
  ConstLabelSeLabelIter iter_begin =
      schema_infos_.lower_bound(id, compare_with_tenant_label_se_label_id);
  bool is_stop = false;
  for (ConstLabelSeLabelIter iter = iter_begin;
      OB_SUCC(ret) && iter != schema_infos_.end() && !is_stop; ++iter) {
    const ObLabelSeLabelSchema *schema = NULL;
    if (OB_ISNULL(schema = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret), K(schema));
    } else if (tenant_id != schema->get_tenant_id()) {
      is_stop = true;
    } else if (OB_FAIL(schemas.push_back(schema))) {
      LOG_WARN("push back label security policy failed", K(ret));
    }
  }

  return ret;
}

int ObLabelSeLabelMgr::get_schemas_in_policy(const uint64_t tenant_id,
                                            const uint64_t policy_id,
                                            ObIArray<const ObLabelSeLabelSchema *> &schemas) const
{
  int ret = OB_SUCCESS;
  schemas.reset();

  ObTenantLabelSeLabelId id(tenant_id, OB_MIN_ID);
  ConstLabelSeLabelIter iter_begin =
      schema_infos_.lower_bound(id, compare_with_tenant_label_se_label_id);
  bool is_stop = false;
  for (ConstLabelSeLabelIter iter = iter_begin;
      OB_SUCC(ret) && iter != schema_infos_.end() && !is_stop; ++iter) {
    const ObLabelSeLabelSchema *schema = NULL;
    if (OB_ISNULL(schema = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret), K(schema));
    } else if (tenant_id != schema->get_tenant_id()) {
      is_stop = true;
    } else if (policy_id == schema->get_label_se_policy_id()) {
      if (OB_FAIL(schemas.push_back(schema))) {
        LOG_WARN("push back label security policy failed", K(ret));
      }
    }
  }

  return ret;
}

int ObLabelSeLabelMgr::get_schema_by_id(const uint64_t label_se_label_id, const ObLabelSeLabelSchema *&schema) const
{
  int ret = OB_SUCCESS;
  schema = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == label_se_label_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(label_se_label_id));
  } else {
    ObLabelSeLabelSchema *tmp_schema = NULL;
    int hash_ret = id_map_.get_refactored(label_se_label_id, tmp_schema);
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

int ObLabelSeLabelMgr::get_schema_count(int64_t &schema_count) const
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

int ObLabelSeLabelMgr::get_schema_statistics(ObSchemaStatisticsInfo &schema_info) const
{
  int ret = OB_SUCCESS;
  schema_info.reset();
  schema_info.schema_type_ = LABEL_SE_LABEL_SCHEMA;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    schema_info.count_ = schema_infos_.size();
    for (ConstLabelSeLabelIter it = schema_infos_.begin(); OB_SUCC(ret) && it != schema_infos_.end(); it++) {
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

// user level schema

ObLabelSeUserLevelMgr::ObLabelSeUserLevelMgr()
    : is_inited_(false),
      local_allocator_(SET_USE_500(ObModIds::OB_SCHEMA_GETTER_GUARD, ObCtxIds::SCHEMA_SERVICE)),
      allocator_(local_allocator_),
      schema_infos_(0, NULL, SET_USE_500(ObModIds::OB_SCHEMA_LABEL_SE_USER_LEVEL, ObCtxIds::SCHEMA_SERVICE)),
      user_level_map_(SET_USE_500(ObModIds::OB_SCHEMA_LABEL_SE_USER_LEVEL, ObCtxIds::SCHEMA_SERVICE)),
      id_map_(SET_USE_500(ObModIds::OB_SCHEMA_LABEL_SE_USER_LEVEL, ObCtxIds::SCHEMA_SERVICE))

{
}

ObLabelSeUserLevelMgr::ObLabelSeUserLevelMgr(ObIAllocator &allocator)
    : is_inited_(false),
      local_allocator_(SET_USE_500(ObModIds::OB_SCHEMA_GETTER_GUARD, ObCtxIds::SCHEMA_SERVICE)),
      allocator_(allocator),
      schema_infos_(0, NULL, SET_USE_500(ObModIds::OB_SCHEMA_LABEL_SE_USER_LEVEL, ObCtxIds::SCHEMA_SERVICE)),
      user_level_map_(SET_USE_500(ObModIds::OB_SCHEMA_LABEL_SE_USER_LEVEL, ObCtxIds::SCHEMA_SERVICE)),
      id_map_(SET_USE_500(ObModIds::OB_SCHEMA_LABEL_SE_USER_LEVEL, ObCtxIds::SCHEMA_SERVICE))
{
}

ObLabelSeUserLevelMgr::~ObLabelSeUserLevelMgr()
{
}

int ObLabelSeUserLevelMgr::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init private label security policy schema manager twice", K(ret));
  } else if (OB_FAIL(user_level_map_.init())) {
    LOG_WARN("init private label security policy schema map failed", K(ret));
  } else if (OB_FAIL(id_map_.init())) {
    LOG_WARN("init private label security policy schema id map failed", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObLabelSeUserLevelMgr::reset()
{
  if (!is_inited_) {
    LOG_WARN_RET(OB_NOT_INIT, "label security policy manger not init");
  } else {
    schema_infos_.clear();
    user_level_map_.clear();
    id_map_.clear();
  }
}

ObLabelSeUserLevelMgr &ObLabelSeUserLevelMgr::operator =(const ObLabelSeUserLevelMgr &other)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema manager not init", K(ret));
  } else  if (OB_FAIL(assign(other))) {
    LOG_WARN("assign failed", K(ret));
  }
  return *this;
}

int ObLabelSeUserLevelMgr::assign(const ObLabelSeUserLevelMgr &other)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema manager not init", K(ret));
  } else if (this != &other) {
    if (OB_FAIL(user_level_map_.assign(other.user_level_map_))) {
      LOG_WARN("assign label security user level map failed", K(ret));
    } else if (OB_FAIL(schema_infos_.assign(other.schema_infos_))) {
      LOG_WARN("assign label security policy schema vector failed", K(ret));
    } else if (OB_FAIL(id_map_.assign(other.id_map_))) {
      LOG_WARN("assign label security user level id map failed", K(ret));
    }
  }
  return ret;
}

int ObLabelSeUserLevelMgr::deep_copy(const ObLabelSeUserLevelMgr &other)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema manager not init", K(ret));
  } else if (this != &other) {
    reset();
    ObLabelSeUserLevelSchema *schema = NULL;
    for (LabelSeUserLevelIter iter = other.schema_infos_.begin();
         OB_SUCC(ret) && iter != other.schema_infos_.end(); iter++) {
      if (OB_ISNULL(schema = *iter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", KP(schema), K(ret));
      } else if (OB_FAIL(add_label_se_user_level(*schema))) {
        LOG_WARN("add label security component failed", K(*schema), K(ret));
      }
    }
  }
  return ret;
}


int ObLabelSeUserLevelMgr::add_label_se_user_level(const ObLabelSeUserLevelSchema &schema)
{
  int ret = OB_SUCCESS;
  ObLabelSeUserLevelSchema *new_schema = NULL;
  ObLabelSeUserLevelSchema *old_schema = NULL;
  LabelSeUserLevelIter iter = NULL;
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
    LOG_WARN("failed to add label security policy schema", K(ret));
  } else {
    int over_write = 1;
    ObLabelSeUserLevelHashKey hash_key(new_schema->get_tenant_id(),
                                       new_schema->get_user_id(),
                                       new_schema->get_label_se_policy_id());
    if (OB_FAIL(user_level_map_.set_refactored(hash_key, new_schema, over_write))) {
      LOG_WARN("build label security policy hashmap failed", K(ret));
    } else if (OB_FAIL(id_map_.set_refactored(new_schema->get_label_se_user_level_id(),
                                              new_schema, over_write))) {
      LOG_WARN("build label security policy id hashmap failed", K(ret),
               "label_se_user_level_id", new_schema->get_label_se_user_level_id());
    } else {
      LOG_DEBUG("add point of schema to maps", K(*new_schema));
    }
  }

  if (OB_SUCC(ret) && (schema_infos_.count() != user_level_map_.item_count()
                       || schema_infos_.count() != id_map_.item_count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema is inconsistent with its map",
             K(schema_infos_.count()),
             K(user_level_map_.item_count()),
             K(id_map_.item_count()));
  }

  return ret;
}

int ObLabelSeUserLevelMgr::add_label_se_user_levels(const ObIArray<ObLabelSeUserLevelSchema> &schemas)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < schemas.count() && OB_SUCC(ret); ++i) {
    if (OB_FAIL(add_label_se_user_level(schemas.at(i)))) {
      LOG_WARN("push schema failed", K(ret));
    }
  }
  return ret;
}

int ObLabelSeUserLevelMgr::get_schema_by_user_policy_id(const uint64_t tenant_id,
                                                        const uint64_t user_id,
                                                        const uint64_t label_se_policy_id,
                                                        const ObLabelSeUserLevelSchema *&schema) const
{
  int ret = OB_SUCCESS;
  schema = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id
             || OB_INVALID_ID == user_id
             || OB_INVALID_ID == label_se_policy_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(user_id), K(label_se_policy_id));
  } else {
    ObLabelSeUserLevelSchema *tmp_schema = NULL;
    ObLabelSeUserLevelHashKey hash_key(tenant_id, user_id, label_se_policy_id);
    if (OB_FAIL(user_level_map_.get_refactored(hash_key, tmp_schema))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        LOG_DEBUG("schema is not exist", K(ret));
      }
    } else {
      schema = tmp_schema;
    }
  }
  return ret;
}

int ObLabelSeUserLevelMgr::get_schema_version_by_id(uint64_t label_se_user_level_id, int64_t &schema_version) const
{
  int ret = OB_SUCCESS;
  schema_version = OB_INVALID_VERSION;
  const ObLabelSeUserLevelSchema *schema = NULL;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == label_se_user_level_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(label_se_user_level_id));
  } else if (OB_FAIL(get_schema_by_id(label_se_user_level_id, schema))) {
    LOG_WARN("get schema by id failed", K(ret), K(label_se_user_level_id));
  } else if (OB_ISNULL(schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema is NULL", K(ret), K(label_se_user_level_id));
  } else {
    schema_version = schema->get_schema_version();
  }
  return ret;
}

int ObLabelSeUserLevelMgr::del_schemas_in_tenant(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else {
    ObArray<const ObLabelSeUserLevelSchema *> schemas;
    if (OB_FAIL(get_schemas_in_tenant(tenant_id, schemas))) {
      LOG_WARN("get label security policy schemas failed", K(ret), K(tenant_id));
    } else {
      FOREACH_CNT_X(schema, schemas, OB_SUCC(ret)) {
        ObTenantLabelSeUserLevelId id((*schema)->get_tenant_label_se_user_level_id());
        if (OB_FAIL(del_label_se_user_level(id))) {
          LOG_WARN("del label security policy failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObLabelSeUserLevelMgr::del_schemas_in_policy(const uint64_t tenant_id, const uint64_t policy_id)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else {
    ObArray<const ObLabelSeUserLevelSchema *> schemas;
    if (OB_FAIL(get_schemas_in_policy(tenant_id, policy_id, schemas))) {
      LOG_WARN("get label security policy schemas failed", K(ret), K(tenant_id));
    } else {
      FOREACH_CNT_X(schema, schemas, OB_SUCC(ret)) {
        ObTenantLabelSeUserLevelId id((*schema)->get_tenant_label_se_user_level_id());
        if (OB_FAIL(del_label_se_user_level(id))) {
          LOG_WARN("del label security policy failed",
                   "tenant_id", id.tenant_id_,
                   "label_se_user_level_id", id.label_se_user_level_id_,
                   K(ret));
        }
      }
    }
  }
  return ret;
}

int ObLabelSeUserLevelMgr::del_schemas_in_user(const uint64_t tenant_id, const uint64_t user_id)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else {
    ObArray<const ObLabelSeUserLevelSchema *> schemas;
    if (OB_FAIL(get_schemas_in_user(tenant_id, user_id, schemas))) {
      LOG_WARN("get label security policy schemas failed", K(ret), K(tenant_id));
    } else {
      FOREACH_CNT_X(schema, schemas, OB_SUCC(ret)) {
        ObTenantLabelSeUserLevelId id((*schema)->get_tenant_label_se_user_level_id());
        if (OB_FAIL(del_label_se_user_level(id))) {
          LOG_WARN("del label security policy failed",
                   "tenant_id", id.tenant_id_,
                   "label_se_user_level_id", id.label_se_user_level_id_,
                   K(ret));
        }
      }
    }
  }
  return ret;
}

int ObLabelSeUserLevelMgr::del_label_se_user_level(const ObTenantLabelSeUserLevelId &id)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;
  ObLabelSeUserLevelSchema *schema = NULL;
  if (!id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(id));
  } else if (OB_FAIL(schema_infos_.remove_if(id,
                                             compare_with_tenant_label_se_user_level_id,
                                             equal_to_tenant_label_se_user_level_id,
                                             schema))) {
    LOG_WARN("failed to remove schema", K(ret));
  } else if (OB_ISNULL(schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("removed schema return NULL", K(ret));
  }

  if (OB_SUCC(ret)) {
    ObLabelSeUserLevelHashKey hash_key(schema->get_tenant_id(),
                                       schema->get_user_id(),
                                       schema->get_label_se_policy_id());
    hash_ret = user_level_map_.erase_refactored(hash_key);
    if (OB_UNLIKELY(OB_SUCCESS != hash_ret)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to erase hash map", K(ret), K(hash_ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(id_map_.erase_refactored(schema->get_label_se_user_level_id()))) {
      LOG_WARN("failed delete label security user level from hashmap", K(ret));
    }
  }

  if (OB_SUCC(ret) && (schema_infos_.count() != user_level_map_.item_count()
                       || schema_infos_.count() != id_map_.item_count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema is inconsistent with its map",
             K(schema_infos_.count()),
             K(user_level_map_.item_count()),
             K(id_map_.item_count()));
  }
  return ret;
}

bool ObLabelSeUserLevelMgr::compare_with_tenant_label_se_user_level_id(const ObLabelSeUserLevelSchema *lhs,
                                                              const ObTenantLabelSeUserLevelId &id)
{
  return NULL != lhs ? (lhs->get_tenant_label_se_user_level_id() < id) : false;
}

bool ObLabelSeUserLevelMgr::equal_to_tenant_label_se_user_level_id(const ObLabelSeUserLevelSchema *lhs,
                                                          const ObTenantLabelSeUserLevelId &id)
{
  return NULL != lhs ? (lhs->get_tenant_label_se_user_level_id() == id) : false;
}

int ObLabelSeUserLevelMgr::get_schemas_in_tenant(const uint64_t tenant_id,
    ObIArray<const ObLabelSeUserLevelSchema *> &schemas) const
{
  int ret = OB_SUCCESS;
  schemas.reset();

  ObTenantLabelSeUserLevelId id(tenant_id, OB_MIN_ID);
  ConstLabelSeUserLevelIter iter_begin =
      schema_infos_.lower_bound(id, compare_with_tenant_label_se_user_level_id);
  bool is_stop = false;
  for (ConstLabelSeUserLevelIter iter = iter_begin;
      OB_SUCC(ret) && iter != schema_infos_.end() && !is_stop; ++iter) {
    const ObLabelSeUserLevelSchema *schema = NULL;
    if (OB_ISNULL(schema = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret), K(schema));
    } else if (tenant_id != schema->get_tenant_id()) {
      is_stop = true;
    } else if (OB_FAIL(schemas.push_back(schema))) {
      LOG_WARN("push back label security policy failed", K(ret));
    }
  }

  return ret;
}

int ObLabelSeUserLevelMgr::get_schemas_in_policy(const uint64_t tenant_id,
                                                 const uint64_t policy_id,
                                                 ObIArray<const ObLabelSeUserLevelSchema *> &schemas) const
{
  int ret = OB_SUCCESS;
  schemas.reset();

  ObTenantLabelSeUserLevelId id(tenant_id, OB_MIN_ID);
  ConstLabelSeUserLevelIter iter_begin =
      schema_infos_.lower_bound(id, compare_with_tenant_label_se_user_level_id);
  bool is_stop = false;
  for (ConstLabelSeUserLevelIter iter = iter_begin;
      OB_SUCC(ret) && iter != schema_infos_.end() && !is_stop; ++iter) {
    const ObLabelSeUserLevelSchema *schema = NULL;
    if (OB_ISNULL(schema = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret), K(schema));
    } else if (tenant_id != schema->get_tenant_id()) {
      is_stop = true;
    } else if (policy_id == schema->get_label_se_policy_id()) {
      if (OB_FAIL(schemas.push_back(schema))) {
        LOG_WARN("push back label security policy failed", K(ret));
      }
    }
  }

  return ret;
}

int ObLabelSeUserLevelMgr::get_schemas_in_user(const uint64_t tenant_id,
                                               const uint64_t user_id,
                                               ObIArray<const ObLabelSeUserLevelSchema *> &schemas) const
{
  int ret = OB_SUCCESS;
  schemas.reset();

  ObTenantLabelSeUserLevelId id(tenant_id, OB_MIN_ID);
  ConstLabelSeUserLevelIter iter_begin =
      schema_infos_.lower_bound(id, compare_with_tenant_label_se_user_level_id);
  bool is_stop = false;
  for (ConstLabelSeUserLevelIter iter = iter_begin;
      OB_SUCC(ret) && iter != schema_infos_.end() && !is_stop; ++iter) {
    const ObLabelSeUserLevelSchema *schema = NULL;
    if (OB_ISNULL(schema = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret), K(schema));
    } else if (tenant_id != schema->get_tenant_id()) {
      is_stop = true;
    } else if (user_id == schema->get_label_se_user_level_id()) {
      if (OB_FAIL(schemas.push_back(schema))) {
        LOG_WARN("push back label security policy failed", K(ret));
      }
    }
  }

  return ret;
}


int ObLabelSeUserLevelMgr::get_schema_by_id(const uint64_t label_se_user_level_id,
                                            const ObLabelSeUserLevelSchema *&schema) const
{
  int ret = OB_SUCCESS;
  schema = NULL;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == label_se_user_level_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(label_se_user_level_id));
  } else {
    ObLabelSeUserLevelSchema *tmp_schema = NULL;
    int hash_ret = id_map_.get_refactored(label_se_user_level_id, tmp_schema);
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


int ObLabelSeUserLevelMgr::get_schema_count(int64_t &schema_count) const
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

int ObLabelSeUserLevelMgr::get_schema_statistics(ObSchemaStatisticsInfo &schema_info) const
{
  int ret = OB_SUCCESS;
  schema_info.reset();
  schema_info.schema_type_ = LABEL_SE_USER_LEVEL_SCHEMA;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    schema_info.count_ = schema_infos_.size();
    for (ConstLabelSeUserLevelIter it = schema_infos_.begin(); OB_SUCC(ret) && it != schema_infos_.end(); it++) {
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
