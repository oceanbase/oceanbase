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

#include "ob_java_policy_mgr.h"
#include "share/schema/ob_schema_utils.h"

namespace oceanbase
{
namespace share
{
namespace schema
{

int ObSimpleJavaPolicySchema::assign(const ObSimpleJavaPolicySchema &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    reset();
    tenant_id_ = other.tenant_id_;
    key_ = other.key_;
    schema_version_ = other.schema_version_;
    kind_ = other.kind_;
    grantee_ = other.grantee_;
    type_schema_ = other.type_schema_;
    status_ = other.status_;
    if (OB_FAIL(deep_copy_str(other.type_name_, type_name_))) {
      LOG_WARN("failed to deep_copy_str type_name", K(ret));
    } else if (OB_FAIL(deep_copy_str(other.name_, name_))) {
      LOG_WARN("failed to deep_copy_str name", K(ret));
    } else if (OB_FAIL(deep_copy_str(other.action_, action_))) {
      LOG_WARN("failed to deep_copy_str action", K(ret));
    }
  }
  return ret;
}

bool ObSimpleJavaPolicySchema::is_valid() const
{
  return ObSchema::is_valid()
      && OB_INVALID_ID != tenant_id_
      && OB_INVALID_ID != key_
      && OB_INVALID_VERSION != schema_version_
      && ObSimpleJavaPolicySchema::is_valid_java_policy_kind(kind_)
      && ObSimpleJavaPolicySchema::is_valid_java_policy_status(status_);
}

void ObSimpleJavaPolicySchema::reset()
{
  ObSchema::reset();
  tenant_id_ = OB_INVALID_ID;
  key_ = OB_INVALID_ID;
  schema_version_ = OB_INVALID_VERSION;
  kind_ = JavaPolicyKind::INVALID;
  grantee_ = OB_INVALID_ID;
  type_schema_ = OB_INVALID_ID;
  status_ = JavaPolicyStatus::INVALID;
  reset_string(type_name_);
  reset_string(name_);
  reset_string(action_);
}

ObJavaPolicyMgr::ObJavaPolicyMgr()
    : is_inited_(false),
      local_allocator_(SET_USE_500(ObModIds::OB_SCHEMA_GETTER_GUARD, ObCtxIds::SCHEMA_SERVICE)),
      allocator_(local_allocator_),
      java_policy_infos_(0, nullptr, SET_USE_500("SchJPol", ObCtxIds::SCHEMA_SERVICE)),
      java_policy_id_map_(SET_USE_500("SchJPol", ObCtxIds::SCHEMA_SERVICE))
{
}

ObJavaPolicyMgr::ObJavaPolicyMgr(common::ObIAllocator &allocator)
    : is_inited_(false),
      local_allocator_(SET_USE_500(ObModIds::OB_SCHEMA_GETTER_GUARD, ObCtxIds::SCHEMA_SERVICE)),
      allocator_(allocator),
      java_policy_infos_(0, nullptr, SET_USE_500("SchJPol", ObCtxIds::SCHEMA_SERVICE)),
      java_policy_id_map_(SET_USE_500("SchJPol", ObCtxIds::SCHEMA_SERVICE))
{
}

int ObJavaPolicyMgr::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(java_policy_id_map_.init())) {
    LOG_WARN("failed to init java_policy_id_map_", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObJavaPolicyMgr::reset()
{
  if (!is_inited_) {
    LOG_WARN_RET(OB_NOT_INIT, "not init");
  } else {
    java_policy_infos_.clear();
    java_policy_id_map_.clear();
  }
}

int ObJavaPolicyMgr::assign(const ObJavaPolicyMgr &other)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (this != &other) {
    if (OB_FAIL(java_policy_infos_.assign(other.java_policy_infos_))) {
      LOG_WARN("assign failed", K(ret));
    } else if (OB_FAIL(java_policy_id_map_.assign(other.java_policy_id_map_))) {
      LOG_WARN("assign java_policy_id_map_ failed", K(ret));
    }
  }
  return ret;
}

int ObJavaPolicyMgr::deep_copy(const ObJavaPolicyMgr &other)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (this != &other) {
    reset();
    for (JavaPolicyConstIter it = other.java_policy_infos_.begin(); OB_SUCC(ret) && it != other.java_policy_infos_.end(); ++it) {
      ObSimpleJavaPolicySchema *src_schema = *it;
      if (OB_ISNULL(src_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL schema", K(ret));
      } else if (OB_FAIL(add_java_policy(*src_schema))) {
        LOG_WARN("add_java_policy failed", K(ret));
      }
    }
  }
  return ret;
}

int ObJavaPolicyMgr::get_java_policy_schema_count(int64_t &count) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    count = java_policy_infos_.size();
  }
  return ret;
}

int ObJavaPolicyMgr::get_schema_statistics(ObSchemaStatisticsInfo &schema_info) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    schema_info.reset();
    schema_info.schema_type_ = JAVA_POLICY_SCHEMA;
    schema_info.count_ = java_policy_infos_.size();
    for (JavaPolicyConstIter it = java_policy_infos_.begin();
         OB_SUCC(ret) && it != java_policy_infos_.end(); ++it) {
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

int ObJavaPolicyMgr::add_java_policy(const ObSimpleJavaPolicySchema &schema)
{
  int ret = OB_SUCCESS;
  ObSimpleJavaPolicySchema *new_schema = nullptr;
  JavaPolicyIter it = java_policy_infos_.begin();
  ObSimpleJavaPolicySchema *replaced_schema = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!schema.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid schema", K(ret), K(schema));
  } else if (OB_FAIL(ObSchemaUtils::alloc_schema(allocator_, schema, new_schema))) {
    LOG_WARN("alloc_schema failed", K(ret));
  } else if (OB_ISNULL(new_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL new_schema", K(ret));
  } else if (OB_FAIL(java_policy_infos_.replace(new_schema, it, compare_java_policy, equal_java_policy, replaced_schema))) {
    LOG_WARN("insert failed", K(ret));
  } else {
    constexpr int overwrite = 1;
    if (OB_FAIL(java_policy_id_map_.set_refactored(new_schema->get_key(), new_schema, overwrite))) {
      LOG_WARN("failed to set_refactored to java_policy_id_map_", K(ret), KPC(new_schema));
    }
  }

  // defensive check: verify consistency between sorted vector and hash map
  // if inconsistent, try self-healing by rebuilding the hash map from the vector
  if (java_policy_infos_.count() != java_policy_id_map_.item_count()) {
    LOG_WARN("java policy schema is inconsistent",
             K(ret),
             K(java_policy_infos_.count()),
             K(java_policy_id_map_.item_count()),
             K(java_policy_infos_),
             K(schema));
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = rebuild_java_policy_hashmap())) {
      LOG_WARN("rebuild java_policy hashmap failed", K(ret), K(tmp_ret));
    }
  }

  if (OB_SUCC(ret) && nullptr != replaced_schema) {
    // Should generally not happen if adding unique keys
  }
  return ret;
}

int ObJavaPolicyMgr::add_java_policies(const common::ObIArray<ObSimpleJavaPolicySchema> &schemas)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < schemas.count(); ++i) {
    if (OB_FAIL(add_java_policy(schemas.at(i)))) {
      LOG_WARN("add_java_policy failed", K(ret), "index", i);
    }
  }
  return ret;
}

int ObJavaPolicyMgr::del_java_policy(const ObTenantJavaPolicyId &id)
{
  int ret = OB_SUCCESS;
  ObSimpleJavaPolicySchema schema_key;
  ObSimpleJavaPolicySchema *removed_schema = nullptr;
  ObSimpleJavaPolicySchema *found_schema = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(id));
  } else if (OB_FAIL(java_policy_id_map_.get_refactored(id.java_policy_id_, found_schema))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("get_refactored failed", K(ret), K(id));
    } else {
      ret = OB_ENTRY_NOT_EXIST;
    }
  } else if (OB_ISNULL(found_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL schema", K(ret), K(id));
  } else if (found_schema->get_tenant_id() != id.tenant_id_) {
    ret = OB_ENTRY_NOT_EXIST;
  } else {
    schema_key.set_tenant_id(found_schema->get_tenant_id());
    schema_key.set_grantee(found_schema->get_grantee());
    schema_key.set_key(found_schema->get_key());
    if (OB_FAIL(java_policy_infos_.remove_if(&schema_key, compare_java_policy, equal_java_policy, removed_schema))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("remove failed", K(ret), K(id));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(removed_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL schema removed", K(ret));
  } else {
    // try remove from map
    int hash_ret = OB_SUCCESS;
    if (OB_SUCCESS != (hash_ret = java_policy_id_map_.erase_refactored(removed_schema->get_key()))) {
      if (OB_HASH_NOT_EXIST != hash_ret) {
        LOG_WARN("failed to erase from java_policy_id_map_", K(hash_ret), "key", removed_schema->get_key());
      }
    }
  }
  return ret;
}

int ObJavaPolicyMgr::get_java_policy_schema(const uint64_t tenant_id,
                                            const uint64_t java_policy_id,
                                            const ObSimpleJavaPolicySchema *&schema) const
{
  int ret = OB_SUCCESS;
  schema = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObSimpleJavaPolicySchema *tmp_schema = nullptr;
    if (OB_FAIL(java_policy_id_map_.get_refactored(java_policy_id, tmp_schema))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to get_refactored from java_policy_id_map_", K(ret), K(java_policy_id), K(java_policy_infos_));
      }
    } else if (OB_ISNULL(tmp_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL schema", K(ret), K(java_policy_id), K(java_policy_infos_));
    } else {
      schema = tmp_schema;
    }
  }
  return ret;
}

int ObJavaPolicyMgr::get_java_policy_schemas_in_tenant(const uint64_t tenant_id,
                                                       common::ObIArray<const ObSimpleJavaPolicySchema *> &schemas) const
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(ret), K(tenant_id));
  } else {
    schemas.reuse();
    ObSimpleJavaPolicySchema key_schema;
    key_schema.set_tenant_id(tenant_id);
    // Set it to 0 (minimum possible ID) to scan all grantees within the tenant.
    key_schema.set_grantee(0);
    key_schema.set_key(OB_MIN_ID);
    JavaPolicyConstIter it = java_policy_infos_.lower_bound(&key_schema, compare_java_policy);
    bool is_stop = false;
    for (; OB_SUCC(ret) && !is_stop && it != java_policy_infos_.end(); ++it) {
      const ObSimpleJavaPolicySchema *schema = *it;
      if (OB_ISNULL(schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL schema", K(ret));
      } else if (schema->get_tenant_id() != tenant_id) {
        is_stop = true;
      } else if (OB_FAIL(schemas.push_back(schema))) {
        LOG_WARN("push_back failed", K(ret));
      }
    }
  }
  return ret;
}

int ObJavaPolicyMgr::get_java_policy_schemas_of_grantee(const uint64_t tenant_id,
                                                        const uint64_t grantee_id,
                                                        common::ObIArray<const ObSimpleJavaPolicySchema *> &schemas) const
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || OB_INVALID_ID == grantee_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(grantee_id));
  } else {
    schemas.reuse();
    ObSimpleJavaPolicySchema key_schema;
    key_schema.set_tenant_id(tenant_id);
    key_schema.set_grantee(grantee_id);
    key_schema.set_key(OB_MIN_ID);
    JavaPolicyConstIter it = java_policy_infos_.lower_bound(&key_schema, compare_java_policy);
    bool is_stop = false;
    for (; OB_SUCC(ret) && !is_stop && it != java_policy_infos_.end(); ++it) {
      const ObSimpleJavaPolicySchema *schema = *it;
      if (OB_ISNULL(schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL schema", K(ret));
      } else if (schema->get_tenant_id() != tenant_id || schema->get_grantee() != grantee_id) {
        is_stop = true;
      } else if (OB_FAIL(schemas.push_back(schema))) {
        LOG_WARN("push_back failed", K(ret));
      }
    }
  }
  return ret;
}

int ObJavaPolicyMgr::del_schemas_in_tenant(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(ret), K(tenant_id));
  } else {
    common::ObSEArray<const ObSimpleJavaPolicySchema *, 32> schemas;
    if (OB_FAIL(get_java_policy_schemas_in_tenant(tenant_id, schemas))) {
      LOG_WARN("get_java_policy_schemas_in_tenant failed", K(ret), K(tenant_id));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < schemas.count(); ++i) {
        const ObSimpleJavaPolicySchema *schema = schemas.at(i);
        if (OB_ISNULL(schema)) {
           ret = OB_ERR_UNEXPECTED;
           LOG_WARN("NULL schema", K(ret));
        } else if (OB_FAIL(del_java_policy(ObTenantJavaPolicyId(tenant_id, schema->get_key())))) {
        LOG_WARN("del_java_policy failed", K(ret), K(tenant_id), "key", schema->get_key());
        }
      }
    }
  }
  return ret;
}

bool ObJavaPolicyMgr::compare_java_policy(const ObSimpleJavaPolicySchema *lhs,
                                          const ObSimpleJavaPolicySchema *rhs)
{
  bool ret = false;
  if (lhs->get_tenant_id() != rhs->get_tenant_id()) {
    ret = lhs->get_tenant_id() < rhs->get_tenant_id();
  } else if (lhs->get_grantee() != rhs->get_grantee()) {
    ret = lhs->get_grantee() < rhs->get_grantee();
  } else {
    ret = lhs->get_key() < rhs->get_key();
  }
  return ret;
}

bool ObJavaPolicyMgr::equal_java_policy(const ObSimpleJavaPolicySchema *lhs,
                                        const ObSimpleJavaPolicySchema *rhs)
{
  return lhs->get_tenant_id() == rhs->get_tenant_id()
      && lhs->get_key() == rhs->get_key();
}

int ObJavaPolicyMgr::rebuild_java_policy_hashmap()
{
  int ret = OB_SUCCESS;

  constexpr int overwrite = 1;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("java policy manager not init", K(ret));
  } else {
    java_policy_id_map_.clear();

    for (JavaPolicyConstIter iter = java_policy_infos_.begin();
         OB_SUCC(ret) && iter != java_policy_infos_.end();
         ++iter) {
      ObSimpleJavaPolicySchema *schema = *iter;
      if (OB_ISNULL(schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected NULL schema", K(ret), K(java_policy_infos_));
      } else {
        if (OB_FAIL(java_policy_id_map_.set_refactored(schema->get_key(), schema, overwrite))) {
          LOG_WARN("failed to set_refactored to java_policy_id_map_", K(ret), KPC(schema));
        }
      }
    }
  }

  return ret;
}

} // namespace schema
} // namespace share
} // namespace oceanbase
