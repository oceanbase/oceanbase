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

#include "share/schema/ob_rls_mgr.h"
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
const char *ObRlsPolicyMgr::RLS_POLICY_MGR = "RLS_POLICY_MGR";
const char *ObRlsGroupMgr::RLS_GROUP_MGR = "RLS_GROUP_MGR";
const char *ObRlsContextMgr::RLS_CONTEXT_MGR = "RLS_CONTEXT_MGR";

ObRlsPolicyMgr::ObRlsPolicyMgr()
  : is_inited_(false),
    local_allocator_(SET_USE_500(ObModIds::OB_SCHEMA_GETTER_GUARD, ObCtxIds::SCHEMA_SERVICE)),
    allocator_(local_allocator_),
    rls_policy_infos_(0, NULL, SET_USE_500(RLS_POLICY_MGR, ObCtxIds::SCHEMA_SERVICE)),
    rls_policy_name_map_(SET_USE_500(RLS_POLICY_MGR, ObCtxIds::SCHEMA_SERVICE)),
    rls_policy_id_map_(SET_USE_500(RLS_POLICY_MGR, ObCtxIds::SCHEMA_SERVICE))
{
}

ObRlsPolicyMgr::ObRlsPolicyMgr(common::ObIAllocator &allocator)
  : is_inited_(false),
    local_allocator_(SET_USE_500(ObModIds::OB_SCHEMA_GETTER_GUARD, ObCtxIds::SCHEMA_SERVICE)),
    allocator_(allocator),
    rls_policy_infos_(0, NULL, SET_USE_500(RLS_POLICY_MGR, ObCtxIds::SCHEMA_SERVICE)),
    rls_policy_name_map_(SET_USE_500(RLS_POLICY_MGR, ObCtxIds::SCHEMA_SERVICE)),
    rls_policy_id_map_(SET_USE_500(RLS_POLICY_MGR, ObCtxIds::SCHEMA_SERVICE))
{
}

ObRlsPolicyMgr::~ObRlsPolicyMgr()
{
}

ObRlsPolicyMgr &ObRlsPolicyMgr::operator=(const ObRlsPolicyMgr &other)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("rls_policy manager not init", K(ret));
  } else if (OB_FAIL(assign(other))) {
    LOG_WARN("assign failed", K(ret));
  }
  return *this;
}

int ObRlsPolicyMgr::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init private rls_policy schema manager twice", K(ret));
  } else if (OB_FAIL(rls_policy_name_map_.init())) {
    LOG_WARN("init rls_policy name map failed", K(ret));
  } else if (OB_FAIL(rls_policy_id_map_.init())) {
    LOG_WARN("init rls_policy id map failed", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObRlsPolicyMgr::reset()
{
  if (IS_NOT_INIT) {
    LOG_WARN_RET(OB_NOT_INIT, "rls_policy manger not init");
  } else {
    rls_policy_infos_.clear();
    rls_policy_name_map_.clear();
    rls_policy_id_map_.clear();
  }
}

int ObRlsPolicyMgr::assign(const ObRlsPolicyMgr &other)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("rls_policy manager not init", K(ret));
  } else if (this != &other) {
    if (OB_FAIL(rls_policy_name_map_.assign(other.rls_policy_name_map_))) {
      LOG_WARN("assign rls_policy name map failed", K(ret));
    } else if (OB_FAIL(rls_policy_id_map_.assign(other.rls_policy_id_map_))) {
      LOG_WARN("assign rls_policy id map failed", K(ret));
    } else if (OB_FAIL(rls_policy_infos_.assign(other.rls_policy_infos_))) {
      LOG_WARN("assign rls_policy schema vector failed", K(ret));
    }
  }
  return ret;
}

int ObRlsPolicyMgr::deep_copy(const ObRlsPolicyMgr &other)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("rls_policy manager not init", K(ret));
  } else if (this != &other) {
    reset();
    ObRlsPolicySchema *schema = NULL;
    for (RlsPolicyIter iter = other.rls_policy_infos_.begin();
         OB_SUCC(ret) && iter != other.rls_policy_infos_.end(); iter++) {
      if (OB_ISNULL(schema = *iter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", KP(schema), K(ret));
      } else if (OB_FAIL(add_rls_policy(*schema))) {
        LOG_WARN("add rls_policy failed", K(*schema), K(ret));
      }
    }
  }
  return ret;
}

int ObRlsPolicyMgr::add_rls_policy(const ObRlsPolicySchema &schema)
{
  int ret = OB_SUCCESS;
  ObRlsPolicySchema *new_schema = NULL;
  ObRlsPolicySchema *old_schema = NULL;
  RlsPolicyIter iter = NULL;
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
  } else if (OB_FAIL(rls_policy_infos_.replace(new_schema,
                                              iter,
                                              schema_compare,
                                              schema_equal,
                                              old_schema))) {
    LOG_WARN("failed to add rls_policy schema", K(ret));
  } else {
    int over_write = 1;
    ObRlsPolicyNameHashKey hash_wrapper(new_schema->get_tenant_id(), new_schema->get_table_id(),
                                        new_schema->get_rls_group_id(), new_schema->get_policy_name());
    if (OB_FAIL(rls_policy_name_map_.set_refactored(hash_wrapper, new_schema, over_write))) {
      LOG_WARN("build rls_policy hash map failed", K(ret));
    } else if (OB_FAIL(rls_policy_id_map_.set_refactored(new_schema->get_rls_policy_id(), new_schema, over_write))) {
      LOG_WARN("build rls_policy id hashmap failed", K(ret),
               "rls_policy_id", new_schema->get_rls_policy_id());
    }
  }
  if ((rls_policy_infos_.count() != rls_policy_name_map_.item_count()
      || rls_policy_infos_.count() != rls_policy_id_map_.item_count())) {
    LOG_WARN("schema is inconsistent with its map", K(ret),
             K(rls_policy_infos_.count()),
             K(rls_policy_name_map_.item_count()),
             K(rls_policy_id_map_.item_count()));
    int rebuild_ret = OB_SUCCESS;
    if (OB_SUCCESS != (rebuild_ret = rebuild_rls_policy_hashmap())) {
      LOG_WARN("rebuild rls_policy hashmap failed", K(rebuild_ret));
    }
    if (OB_SUCC(ret)) {
      ret = rebuild_ret;
    }
  }
  LOG_TRACE("add rls_policy", K(schema), K(rls_policy_infos_.count()),
            K(rls_policy_name_map_.item_count()), K(rls_policy_id_map_.item_count()));
  return ret;
}

int ObRlsPolicyMgr::add_rls_policys(const common::ObIArray<ObRlsPolicySchema> &schemas)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < schemas.count() && OB_SUCC(ret); ++i) {
    if (OB_FAIL(add_rls_policy(schemas.at(i)))) {
      LOG_WARN("push rls_policy failed", K(ret));
    }
  }
  return ret;
}

int ObRlsPolicyMgr::del_rls_policy(const ObTenantRlsPolicyId &id)
{
  int ret = OB_SUCCESS;
  ObRlsPolicySchema *schema = NULL;
  if (OB_UNLIKELY(!id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(id));
  } else if (OB_FAIL(rls_policy_infos_.remove_if(id,
                                                 compare_with_sort_key,
                                                 equal_to_sort_key,
                                                 schema))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      // if item does not exist, regard it as succeeded, schema will be refreshed later
      ret = OB_SUCCESS;
      LOG_INFO("failed to remove rls_policy schema, item may not exist", K(ret));
    } else {
      LOG_WARN("failed to remove rls_policy schema", K(ret));
    }
  } else if (OB_ISNULL(schema)) {
    // if item can be found, schema should not be null
    // defense code, should not happed
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("removed rls_policy schema return NULL, ",
             "tenant_id", id.tenant_id_,
             "rls_policy_id", id.schema_id_,
             K(ret));
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(schema)) {
    if (OB_FAIL(rls_policy_id_map_.erase_refactored(schema->get_rls_policy_id()))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        LOG_INFO("item does not exist, will ignore it", K(ret),
            "rls_policy id", schema->get_rls_policy_id());
      } else {
        LOG_WARN("failed to delete rls_policy from hashmap", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(schema)) {
    ObRlsPolicyNameHashKey hash_wrapper(schema->get_tenant_id(), schema->get_table_id(),
                                        schema->get_rls_group_id(), schema->get_policy_name());
    if (OB_FAIL(rls_policy_name_map_.erase_refactored(hash_wrapper))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        LOG_INFO("item does not exist, will ignore it", K(ret),
            "rls_policy name", schema->get_policy_name());
      } else {
        LOG_WARN("failed to delete rls_policy from hashmap", K(ret));
      }
    }
  }

  if (rls_policy_infos_.count() != rls_policy_name_map_.item_count()
      || rls_policy_infos_.count() != rls_policy_id_map_.item_count()) {
    LOG_WARN("rls_policy schema is non-consistent", K(id), K(rls_policy_infos_.count()),
        K(rls_policy_name_map_.item_count()), K(rls_policy_id_map_.item_count()));
    int rebuild_ret = OB_SUCCESS;
    if (OB_SUCCESS != (rebuild_ret = rebuild_rls_policy_hashmap())) {
      LOG_WARN("rebuild rls_policy hashmap failed", K(rebuild_ret));
    }
    if (OB_SUCC(ret)) {
      ret = rebuild_ret;
    }
  }
  LOG_TRACE("rls_policy del", K(id), K(rls_policy_infos_.count()),
      K(rls_policy_name_map_.item_count()), K(rls_policy_id_map_.item_count()));
  return ret;
}

int ObRlsPolicyMgr::get_schema_by_id(const uint64_t rls_policy_id,
                                     const ObRlsPolicySchema *&schema) const
{
  int ret = OB_SUCCESS;
  schema = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == rls_policy_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(rls_policy_id));
  } else {
    ObRlsPolicySchema *tmp_schema = NULL;
    int hash_ret = rls_policy_id_map_.get_refactored(rls_policy_id, tmp_schema);
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

int ObRlsPolicyMgr::get_schema_by_name(const uint64_t tenant_id,
                                       const uint64_t table_id,
                                       const uint64_t rls_group_id,
                                       const common::ObString &name,
                                       const ObRlsPolicySchema *&schema) const
{
  int ret = OB_SUCCESS;
  schema = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || OB_INVALID_ID == table_id ||
             OB_INVALID_ID == rls_group_id || name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(table_id), K(rls_group_id), K(name));
  } else {
    ObRlsPolicySchema *tmp_schema = NULL;
    ObRlsPolicyNameHashKey hash_wrapper(tenant_id, table_id, rls_group_id, name);
    if (OB_FAIL(rls_policy_name_map_.get_refactored(hash_wrapper, tmp_schema))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        LOG_INFO("schema is not exist", K(tenant_id), K(table_id), K(rls_group_id), K(name),
                 "map_cnt", rls_policy_name_map_.item_count());
      } else {
        LOG_WARN("failed to get rls_policy from hashmap", K(ret));
      }
    } else {
      schema = tmp_schema;
    }
  }
  return ret;
}

int ObRlsPolicyMgr::get_schemas_in_tenant(const uint64_t tenant_id,
                                          common::ObIArray<const ObRlsPolicySchema *> &schemas) const
{
  int ret = OB_SUCCESS;
  schemas.reset();
  ObTenantRlsPolicyId id(tenant_id, OB_MIN_ID);
  ConstRlsPolicyIter iter_begin =
      rls_policy_infos_.lower_bound(id, compare_with_sort_key);
  bool is_stop = false;
  for (ConstRlsPolicyIter iter = iter_begin;
      OB_SUCC(ret) && iter != rls_policy_infos_.end() && !is_stop; ++iter) {
    const ObRlsPolicySchema *schema = NULL;
    if (OB_ISNULL(schema = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret), K(schema));
    } else if (tenant_id != schema->get_tenant_id()) {
      is_stop = true;
    } else if (OB_FAIL(schemas.push_back(schema))) {
      LOG_WARN("push back rls_policy failed", K(ret));
    }
  }
  return ret;
}

int ObRlsPolicyMgr::get_schemas_in_table(const uint64_t tenant_id,
                                         const uint64_t table_id,
                                         common::ObIArray<const ObRlsPolicySchema *> &schemas) const
{
  int ret = OB_SUCCESS;
  schemas.reset();
  ObTenantRlsPolicyId id(tenant_id, OB_MIN_ID);
  ConstRlsPolicyIter iter_begin =
      rls_policy_infos_.lower_bound(id, compare_with_sort_key);
  bool is_stop = false;
  for (ConstRlsPolicyIter iter = iter_begin;
      OB_SUCC(ret) && iter != rls_policy_infos_.end() && !is_stop; ++iter) {
    const ObRlsPolicySchema *schema = NULL;
    if (OB_ISNULL(schema = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret), K(schema));
    } else if (tenant_id != schema->get_tenant_id()) {
      is_stop = true;
    } else if (schema->get_table_id() != table_id) {
      // do nothing
    } else if (OB_FAIL(schemas.push_back(schema))) {
      LOG_WARN("push back rls_policy failed", K(ret));
    }
  }
  return ret;
}

int ObRlsPolicyMgr::get_schemas_in_group(const uint64_t tenant_id,
                                         const uint64_t table_id,
                                         const uint64_t rls_group_id,
                                         common::ObIArray<const ObRlsPolicySchema *> &schemas) const
{
  int ret = OB_SUCCESS;
  schemas.reset();
  ObTenantRlsPolicyId id(tenant_id, OB_MIN_ID);
  ConstRlsPolicyIter iter_begin =
      rls_policy_infos_.lower_bound(id, compare_with_sort_key);
  bool is_stop = false;
  for (ConstRlsPolicyIter iter = iter_begin;
      OB_SUCC(ret) && iter != rls_policy_infos_.end() && !is_stop; ++iter) {
    const ObRlsPolicySchema *schema = NULL;
    if (OB_ISNULL(schema = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret), K(schema));
    } else if (tenant_id != schema->get_tenant_id()) {
      is_stop = true;
    } else if (schema->get_table_id() != table_id || schema->get_rls_group_id() != rls_group_id) {
      // do nothing
    } else if (OB_FAIL(schemas.push_back(schema))) {
      LOG_WARN("push back rls_policy failed", K(ret));
    }
  }
  return ret;
}

int ObRlsPolicyMgr::del_schemas_in_tenant(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else {
    ObArray<const ObRlsPolicySchema *> schemas;
    if (OB_FAIL(get_schemas_in_tenant(tenant_id, schemas))) {
      LOG_WARN("get rls_policy schemas failed", K(ret), K(tenant_id));
    } else {
      FOREACH_CNT_X(schema, schemas, OB_SUCC(ret)) {
        ObTenantRlsPolicyId id(tenant_id, (*schema)->get_rls_policy_id());
        if (OB_FAIL(del_rls_policy(id))) {
          LOG_WARN("del rls_policy failed",
                   "tenant_id", id.tenant_id_,
                   "rls_policy_id", id.schema_id_,
                   K(ret));
        }
      }
    }
  }
  return ret;
}

int ObRlsPolicyMgr::get_schema_count(int64_t &schema_count) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    schema_count = rls_policy_infos_.count();
  }
  return ret;
}

int ObRlsPolicyMgr::get_schema_statistics(ObSchemaStatisticsInfo &schema_info) const
{
  int ret = OB_SUCCESS;
  schema_info.reset();
  schema_info.schema_type_ = RLS_POLICY_SCHEMA;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    schema_info.count_ = rls_policy_infos_.size();
    for (ConstRlsPolicyIter iter = rls_policy_infos_.begin();
        OB_SUCC(ret) && iter != rls_policy_infos_.end(); ++iter) {
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

int ObRlsPolicyMgr::rebuild_rls_policy_hashmap()
{
  int ret = OB_SUCCESS;
  int over_write = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    rls_policy_name_map_.clear();
    rls_policy_id_map_.clear();
  }

  for (ConstRlsPolicyIter iter = rls_policy_infos_.begin();
      OB_SUCC(ret) && iter != rls_policy_infos_.end(); ++iter) {
    ObRlsPolicySchema *rls_policy_schema = *iter;
    ObRlsPolicyNameHashKey hash_wrapper;
    if (OB_ISNULL(rls_policy_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rls_policy_schema is NULL", K(ret), K(rls_policy_schema));
    } else if (FALSE_IT(hash_wrapper.set_tenant_id(rls_policy_schema->get_tenant_id()))) {
      // do nothing
    } else if (FALSE_IT(hash_wrapper.set_table_id(rls_policy_schema->get_table_id()))) {
      // do nothing
    } else if (FALSE_IT(hash_wrapper.set_rls_group_id(rls_policy_schema->get_rls_group_id()))) {
      // do nothing
    } else if (FALSE_IT(hash_wrapper.set_name(rls_policy_schema->get_policy_name()))) {
      // do nothing
    } else if (OB_FAIL(rls_policy_name_map_.set_refactored(hash_wrapper,
        rls_policy_schema, over_write))) {
      LOG_WARN("build rls_policy name hashmap failed", K(ret),
          "rls_policy_name", rls_policy_schema->get_policy_name());
    } else if (OB_FAIL(rls_policy_id_map_.set_refactored(rls_policy_schema->get_rls_policy_id(),
        rls_policy_schema, over_write))) {
      LOG_WARN("build rls_policy id hashmap failed", K(ret),
          "rls_policy_id", rls_policy_schema->get_rls_policy_id());
    }
  }
  return ret;
}

ObRlsGroupMgr::ObRlsGroupMgr()
  : is_inited_(false),
    local_allocator_(SET_USE_500(ObModIds::OB_SCHEMA_GETTER_GUARD, ObCtxIds::SCHEMA_SERVICE)),
    allocator_(local_allocator_),
    rls_group_infos_(0, NULL, SET_USE_500(RLS_GROUP_MGR, ObCtxIds::SCHEMA_SERVICE)),
    rls_group_name_map_(SET_USE_500(RLS_GROUP_MGR, ObCtxIds::SCHEMA_SERVICE)),
    rls_group_id_map_(SET_USE_500(RLS_GROUP_MGR, ObCtxIds::SCHEMA_SERVICE))
{
}

ObRlsGroupMgr::ObRlsGroupMgr(common::ObIAllocator &allocator)
  : is_inited_(false),
    local_allocator_(SET_USE_500(ObModIds::OB_SCHEMA_GETTER_GUARD, ObCtxIds::SCHEMA_SERVICE)),
    allocator_(allocator),
    rls_group_infos_(0, NULL, SET_USE_500(RLS_GROUP_MGR, ObCtxIds::SCHEMA_SERVICE)),
    rls_group_name_map_(SET_USE_500(RLS_GROUP_MGR, ObCtxIds::SCHEMA_SERVICE)),
    rls_group_id_map_(SET_USE_500(RLS_GROUP_MGR, ObCtxIds::SCHEMA_SERVICE))
{
}

ObRlsGroupMgr::~ObRlsGroupMgr()
{
}

ObRlsGroupMgr &ObRlsGroupMgr::operator=(const ObRlsGroupMgr &other)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("rls_group manager not init", K(ret));
  } else if (OB_FAIL(assign(other))) {
    LOG_WARN("assign failed", K(ret));
  }
  return *this;
}

int ObRlsGroupMgr::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init private rls_group schema manager twice", K(ret));
  } else if (OB_FAIL(rls_group_name_map_.init())) {
    LOG_WARN("init rls_group name map failed", K(ret));
  } else if (OB_FAIL(rls_group_id_map_.init())) {
    LOG_WARN("init rls_group id map failed", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObRlsGroupMgr::reset()
{
  if (IS_NOT_INIT) {
    LOG_WARN_RET(OB_NOT_INIT, "rls_group manger not init");
  } else {
    rls_group_infos_.clear();
    rls_group_name_map_.clear();
    rls_group_id_map_.clear();
  }
}

int ObRlsGroupMgr::assign(const ObRlsGroupMgr &other)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("rls_group manager not init", K(ret));
  } else if (this != &other) {
    if (OB_FAIL(rls_group_name_map_.assign(other.rls_group_name_map_))) {
      LOG_WARN("assign rls_group name map failed", K(ret));
    } else if (OB_FAIL(rls_group_id_map_.assign(other.rls_group_id_map_))) {
      LOG_WARN("assign rls_group id map failed", K(ret));
    } else if (OB_FAIL(rls_group_infos_.assign(other.rls_group_infos_))) {
      LOG_WARN("assign rls_group schema vector failed", K(ret));
    }
  }
  return ret;
}

int ObRlsGroupMgr::deep_copy(const ObRlsGroupMgr &other)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("rls_group manager not init", K(ret));
  } else if (this != &other) {
    reset();
    ObRlsGroupSchema *schema = NULL;
    for (RlsGroupIter iter = other.rls_group_infos_.begin();
         OB_SUCC(ret) && iter != other.rls_group_infos_.end(); iter++) {
      if (OB_ISNULL(schema = *iter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", KP(schema), K(ret));
      } else if (OB_FAIL(add_rls_group(*schema))) {
        LOG_WARN("add rls_group failed", K(*schema), K(ret));
      }
    }
  }
  return ret;
}

int ObRlsGroupMgr::add_rls_group(const ObRlsGroupSchema &schema)
{
  int ret = OB_SUCCESS;
  ObRlsGroupSchema *new_schema = NULL;
  ObRlsGroupSchema *old_schema = NULL;
  RlsGroupIter iter = NULL;
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
  } else if (OB_FAIL(rls_group_infos_.replace(new_schema,
                                              iter,
                                              schema_compare,
                                              schema_equal,
                                              old_schema))) {
    LOG_WARN("failed to add rls_group schema", K(ret));
  } else {
    int over_write = 1;
    ObRlsGroupNameHashKey hash_wrapper(new_schema->get_tenant_id(), new_schema->get_table_id(),
                                       new_schema->get_policy_group_name());
    if (OB_FAIL(rls_group_name_map_.set_refactored(hash_wrapper, new_schema, over_write))) {
      LOG_WARN("build rls_group hash map failed", K(ret));
    } else if (OB_FAIL(rls_group_id_map_.set_refactored(new_schema->get_rls_group_id(), new_schema, over_write))) {
      LOG_WARN("build rls_group id hashmap failed", K(ret),
               "rls_group_id", new_schema->get_rls_group_id());
    }
  }
  if ((rls_group_infos_.count() != rls_group_name_map_.item_count()
      || rls_group_infos_.count() != rls_group_id_map_.item_count())) {
    LOG_WARN("schema is inconsistent with its map", K(ret),
             K(rls_group_infos_.count()),
             K(rls_group_name_map_.item_count()),
             K(rls_group_id_map_.item_count()));
    int rebuild_ret = OB_SUCCESS;
    if (OB_SUCCESS != (rebuild_ret = rebuild_rls_group_hashmap())) {
      LOG_WARN("rebuild rls_group hashmap failed", K(rebuild_ret));
    }
    if (OB_SUCC(ret)) {
      ret = rebuild_ret;
    }
  }
  LOG_TRACE("add rls_group", K(schema), K(rls_group_infos_.count()),
            K(rls_group_name_map_.item_count()), K(rls_group_id_map_.item_count()));
  return ret;
}

int ObRlsGroupMgr::add_rls_groups(const common::ObIArray<ObRlsGroupSchema> &schemas)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < schemas.count() && OB_SUCC(ret); ++i) {
    if (OB_FAIL(add_rls_group(schemas.at(i)))) {
      LOG_WARN("push rls_group failed", K(ret));
    }
  }
  return ret;
}

int ObRlsGroupMgr::del_rls_group(const ObTenantRlsGroupId &id)
{
  int ret = OB_SUCCESS;
  ObRlsGroupSchema *schema = NULL;
  if (OB_UNLIKELY(!id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(id));
  } else if (OB_FAIL(rls_group_infos_.remove_if(id,
                                                compare_with_sort_key,
                                                equal_to_sort_key,
                                                schema))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      // if item does not exist, regard it as succeeded, schema will be refreshed later
      ret = OB_SUCCESS;
      LOG_INFO("failed to remove rls_group schema, item may not exist", K(ret));
    } else {
      LOG_WARN("failed to remove rls_group schema", K(ret));
    }
  } else if (OB_ISNULL(schema)) {
    // if item can be found, schema should not be null
    // defense code, should not happed
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("removed rls_group schema return NULL, ",
             "tenant_id", id.tenant_id_,
             "rls_group_id", id.schema_id_,
             K(ret));
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(schema)) {
    if (OB_FAIL(rls_group_id_map_.erase_refactored(schema->get_rls_group_id()))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        LOG_INFO("item does not exist, will ignore it", K(ret),
            "rls_group id", schema->get_rls_group_id());
      } else {
        LOG_WARN("failed to delete rls_group from hashmap", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(schema)) {
    if (OB_FAIL(rls_group_name_map_.erase_refactored(
        ObRlsGroupNameHashKey(schema->get_tenant_id(), schema->get_table_id(),
                              schema->get_policy_group_name())))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        LOG_INFO("item does not exist, will ignore it", K(ret),
            "rls_group name", schema->get_policy_group_name());
      } else {
        LOG_WARN("failed to delete rls_group from hashmap", K(ret));
      }
    }
  }

  if (rls_group_infos_.count() != rls_group_name_map_.item_count()
      || rls_group_infos_.count() != rls_group_id_map_.item_count()) {
    LOG_WARN("rls_group schema is non-consistent", K(id), K(rls_group_infos_.count()),
        K(rls_group_name_map_.item_count()), K(rls_group_id_map_.item_count()));
    int rebuild_ret = OB_SUCCESS;
    if (OB_SUCCESS != (rebuild_ret = rebuild_rls_group_hashmap())) {
      LOG_WARN("rebuild rls_group hashmap failed", K(rebuild_ret));
    }
    if (OB_SUCC(ret)) {
      ret = rebuild_ret;
    }
  }
  LOG_TRACE("rls_group del", K(id), K(rls_group_infos_.count()),
      K(rls_group_name_map_.item_count()), K(rls_group_id_map_.item_count()));
  return ret;
}

int ObRlsGroupMgr::get_schema_by_id(const uint64_t rls_group_id,
                                     const ObRlsGroupSchema *&schema) const
{
  int ret = OB_SUCCESS;
  schema = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == rls_group_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(rls_group_id));
  } else {
    ObRlsGroupSchema *tmp_schema = NULL;
    int hash_ret = rls_group_id_map_.get_refactored(rls_group_id, tmp_schema);
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

int ObRlsGroupMgr::get_schema_by_name(const uint64_t tenant_id,
                                      const uint64_t table_id,
                                      const common::ObString &name,
                                      const ObRlsGroupSchema *&schema) const
{
  int ret = OB_SUCCESS;
  schema = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || OB_INVALID_ID == table_id ||
             name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(table_id), K(name));
  } else {
    ObRlsGroupSchema *tmp_schema = NULL;
    ObRlsGroupNameHashKey hash_wrapper(tenant_id, table_id, name);
    if (OB_FAIL(rls_group_name_map_.get_refactored(hash_wrapper, tmp_schema))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        LOG_INFO("schema is not exist", K(tenant_id), K(table_id), K(name),
                 "map_cnt", rls_group_name_map_.item_count());
      } else {
        LOG_WARN("failed to get rls_group from hashmap", K(ret));
      }
    } else {
      schema = tmp_schema;
    }
  }
  return ret;
}

int ObRlsGroupMgr::get_schemas_in_tenant(const uint64_t tenant_id,
                                          common::ObIArray<const ObRlsGroupSchema *> &schemas) const
{
  int ret = OB_SUCCESS;
  schemas.reset();
  ObTenantRlsGroupId id(tenant_id, OB_MIN_ID);
  ConstRlsGroupIter iter_begin =
      rls_group_infos_.lower_bound(id, compare_with_sort_key);
  bool is_stop = false;
  for (ConstRlsGroupIter iter = iter_begin;
      OB_SUCC(ret) && iter != rls_group_infos_.end() && !is_stop; ++iter) {
    const ObRlsGroupSchema *schema = NULL;
    if (OB_ISNULL(schema = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret), K(schema));
    } else if (tenant_id != schema->get_tenant_id()) {
      is_stop = true;
    } else if (OB_FAIL(schemas.push_back(schema))) {
      LOG_WARN("push back rls_group failed", K(ret));
    }
  }
  return ret;
}

int ObRlsGroupMgr::get_schemas_in_table(const uint64_t tenant_id,
                                         const uint64_t table_id,
                                         common::ObIArray<const ObRlsGroupSchema *> &schemas) const
{
  int ret = OB_SUCCESS;
  schemas.reset();
  ObTenantRlsGroupId id(tenant_id, OB_MIN_ID);
  ConstRlsGroupIter iter_begin =
      rls_group_infos_.lower_bound(id, compare_with_sort_key);
  bool is_stop = false;
  for (ConstRlsGroupIter iter = iter_begin;
      OB_SUCC(ret) && iter != rls_group_infos_.end() && !is_stop; ++iter) {
    const ObRlsGroupSchema *schema = NULL;
    if (OB_ISNULL(schema = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret), K(schema));
    } else if (tenant_id != schema->get_tenant_id()) {
      is_stop = true;
    } else if (schema->get_table_id() != table_id) {
      // do nothing
    } else if (OB_FAIL(schemas.push_back(schema))) {
      LOG_WARN("push back rls_group failed", K(ret));
    }
  }
  return ret;
}

int ObRlsGroupMgr::del_schemas_in_tenant(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else {
    ObArray<const ObRlsGroupSchema *> schemas;
    if (OB_FAIL(get_schemas_in_tenant(tenant_id, schemas))) {
      LOG_WARN("get rls_group schemas failed", K(ret), K(tenant_id));
    } else {
      FOREACH_CNT_X(schema, schemas, OB_SUCC(ret)) {
        ObTenantRlsGroupId id(tenant_id, (*schema)->get_rls_group_id());
        if (OB_FAIL(del_rls_group(id))) {
          LOG_WARN("del rls_group failed",
                   "tenant_id", id.tenant_id_,
                   "rls_group_id", id.schema_id_,
                   K(ret));
        }
      }
    }
  }
  return ret;
}

int ObRlsGroupMgr::get_schema_count(int64_t &schema_count) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    schema_count = rls_group_infos_.count();
  }
  return ret;
}

int ObRlsGroupMgr::get_schema_statistics(ObSchemaStatisticsInfo &schema_info) const
{
  int ret = OB_SUCCESS;
  schema_info.reset();
  schema_info.schema_type_ = RLS_GROUP_SCHEMA;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    schema_info.count_ = rls_group_infos_.size();
    for (ConstRlsGroupIter iter = rls_group_infos_.begin();
        OB_SUCC(ret) && iter != rls_group_infos_.end(); ++iter) {
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

int ObRlsGroupMgr::rebuild_rls_group_hashmap()
{
  int ret = OB_SUCCESS;
  int over_write = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    rls_group_name_map_.clear();
    rls_group_id_map_.clear();
  }

  for (ConstRlsGroupIter iter = rls_group_infos_.begin();
      OB_SUCC(ret) && iter != rls_group_infos_.end(); ++iter) {
    ObRlsGroupSchema *rls_group_schema = *iter;
    ObRlsGroupNameHashKey hash_wrapper;
    if (OB_ISNULL(rls_group_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rls_group_schema is NULL", K(ret), K(rls_group_schema));
    } else if (FALSE_IT(hash_wrapper.set_tenant_id(rls_group_schema->get_tenant_id()))) {
      // do nothing
    } else if (FALSE_IT(hash_wrapper.set_table_id(rls_group_schema->get_table_id()))) {
      // do nothing
    } else if (FALSE_IT(hash_wrapper.set_group_name(rls_group_schema->get_policy_group_name()))) {
      // do nothing
    } else if (OB_FAIL(rls_group_name_map_.set_refactored(hash_wrapper,
        rls_group_schema, over_write))) {
      LOG_WARN("build rls_group name hashmap failed", K(ret),
          "rls_group_name", rls_group_schema->get_policy_group_name());
    } else if (OB_FAIL(rls_group_id_map_.set_refactored(rls_group_schema->get_rls_group_id(),
        rls_group_schema, over_write))) {
      LOG_WARN("build rls_group id hashmap failed", K(ret),
          "rls_group_id", rls_group_schema->get_rls_group_id());
    }
  }
  return ret;
}

ObRlsContextMgr::ObRlsContextMgr()
  : is_inited_(false),
    local_allocator_(SET_USE_500(ObModIds::OB_SCHEMA_GETTER_GUARD, ObCtxIds::SCHEMA_SERVICE)),
    allocator_(local_allocator_),
    rls_context_infos_(0, NULL, SET_USE_500(RLS_CONTEXT_MGR, ObCtxIds::SCHEMA_SERVICE)),
    rls_context_name_map_(SET_USE_500(RLS_CONTEXT_MGR, ObCtxIds::SCHEMA_SERVICE)),
    rls_context_id_map_(SET_USE_500(RLS_CONTEXT_MGR, ObCtxIds::SCHEMA_SERVICE))
{
}

ObRlsContextMgr::ObRlsContextMgr(common::ObIAllocator &allocator)
  : is_inited_(false),
    local_allocator_(SET_USE_500(ObModIds::OB_SCHEMA_GETTER_GUARD, ObCtxIds::SCHEMA_SERVICE)),
    allocator_(allocator),
    rls_context_infos_(0, NULL, SET_USE_500(RLS_CONTEXT_MGR, ObCtxIds::SCHEMA_SERVICE)),
    rls_context_name_map_(SET_USE_500(RLS_CONTEXT_MGR, ObCtxIds::SCHEMA_SERVICE)),
    rls_context_id_map_(SET_USE_500(RLS_CONTEXT_MGR, ObCtxIds::SCHEMA_SERVICE))
{
}

ObRlsContextMgr::~ObRlsContextMgr()
{
}

ObRlsContextMgr &ObRlsContextMgr::operator=(const ObRlsContextMgr &other)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("rls_context manager not init", K(ret));
  } else if (OB_FAIL(assign(other))) {
    LOG_WARN("assign failed", K(ret));
  }
  return *this;
}

int ObRlsContextMgr::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init private rls_context schema manager twice", K(ret));
  } else if (OB_FAIL(rls_context_name_map_.init())) {
    LOG_WARN("init rls_context name map failed", K(ret));
  } else if (OB_FAIL(rls_context_id_map_.init())) {
    LOG_WARN("init rls_context id map failed", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObRlsContextMgr::reset()
{
  if (IS_NOT_INIT) {
    LOG_WARN_RET(OB_NOT_INIT, "rls_context manger not init");
  } else {
    rls_context_infos_.clear();
    rls_context_name_map_.clear();
    rls_context_id_map_.clear();
  }
}

int ObRlsContextMgr::assign(const ObRlsContextMgr &other)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("rls_context manager not init", K(ret));
  } else if (this != &other) {
    if (OB_FAIL(rls_context_name_map_.assign(other.rls_context_name_map_))) {
      LOG_WARN("assign rls_context name map failed", K(ret));
    } else if (OB_FAIL(rls_context_id_map_.assign(other.rls_context_id_map_))) {
      LOG_WARN("assign rls_context id map failed", K(ret));
    } else if (OB_FAIL(rls_context_infos_.assign(other.rls_context_infos_))) {
      LOG_WARN("assign rls_context schema vector failed", K(ret));
    }
  }
  return ret;
}

int ObRlsContextMgr::deep_copy(const ObRlsContextMgr &other)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("rls_context manager not init", K(ret));
  } else if (this != &other) {
    reset();
    ObRlsContextSchema *schema = NULL;
    for (RlsContextIter iter = other.rls_context_infos_.begin();
         OB_SUCC(ret) && iter != other.rls_context_infos_.end(); iter++) {
      if (OB_ISNULL(schema = *iter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", KP(schema), K(ret));
      } else if (OB_FAIL(add_rls_context(*schema))) {
        LOG_WARN("add rls_context failed", K(*schema), K(ret));
      }
    }
  }
  return ret;
}

int ObRlsContextMgr::add_rls_context(const ObRlsContextSchema &schema)
{
  int ret = OB_SUCCESS;
  ObRlsContextSchema *new_schema = NULL;
  ObRlsContextSchema *old_schema = NULL;
  RlsContextIter iter = NULL;
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
  } else if (OB_FAIL(rls_context_infos_.replace(new_schema,
                                              iter,
                                              schema_compare,
                                              schema_equal,
                                              old_schema))) {
    LOG_WARN("failed to add rls_context schema", K(ret));
  } else {
    int over_write = 1;
    ObRlsContextNameHashKey hash_wrapper(new_schema->get_tenant_id(),
                                         new_schema->get_table_id(),
                                         new_schema->get_context_name(),
                                         new_schema->get_attribute());
    if (OB_FAIL(rls_context_name_map_.set_refactored(hash_wrapper, new_schema, over_write))) {
      LOG_WARN("build rls_context hash map failed", K(ret));
    } else if (OB_FAIL(rls_context_id_map_.set_refactored(new_schema->get_rls_context_id(), new_schema, over_write))) {
      LOG_WARN("build rls_context id hashmap failed", K(ret),
               "rls_context_id", new_schema->get_rls_context_id());
    }
  }
  if ((rls_context_infos_.count() != rls_context_name_map_.item_count()
      || rls_context_infos_.count() != rls_context_id_map_.item_count())) {
    LOG_WARN("schema is inconsistent with its map", K(ret),
             K(rls_context_infos_.count()),
             K(rls_context_name_map_.item_count()),
             K(rls_context_id_map_.item_count()));
    int rebuild_ret = OB_SUCCESS;
    if (OB_SUCCESS != (rebuild_ret = rebuild_rls_context_hashmap())) {
      LOG_WARN("rebuild rls_context hashmap failed", K(rebuild_ret));
    }
    if (OB_SUCC(ret)) {
      ret = rebuild_ret;
    }
  }
  LOG_TRACE("add rls_context", K(schema), K(rls_context_infos_.count()),
            K(rls_context_name_map_.item_count()), K(rls_context_id_map_.item_count()));
  return ret;
}

int ObRlsContextMgr::add_rls_contexts(const common::ObIArray<ObRlsContextSchema> &schemas)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < schemas.count() && OB_SUCC(ret); ++i) {
    if (OB_FAIL(add_rls_context(schemas.at(i)))) {
      LOG_WARN("push rls_context failed", K(ret));
    }
  }
  return ret;
}

int ObRlsContextMgr::del_rls_context(const ObTenantRlsContextId &id)
{
  int ret = OB_SUCCESS;
  ObRlsContextSchema *schema = NULL;
  if (OB_UNLIKELY(!id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(id));
  } else if (OB_FAIL(rls_context_infos_.remove_if(id,
                                                  compare_with_sort_key,
                                                  equal_to_sort_key,
                                                  schema))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      // if item does not exist, regard it as succeeded, schema will be refreshed later
      ret = OB_SUCCESS;
      LOG_INFO("failed to remove rls_context schema, item may not exist", K(ret));
    } else {
      LOG_WARN("failed to remove rls_context schema", K(ret));
    }
  } else if (OB_ISNULL(schema)) {
    // if item can be found, schema should not be null
    // defense code, should not happed
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("removed rls_context schema return NULL, ",
             "tenant_id", id.tenant_id_,
             "rls_context_id", id.schema_id_,
             K(ret));
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(schema)) {
    if (OB_FAIL(rls_context_id_map_.erase_refactored(schema->get_rls_context_id()))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        LOG_INFO("item does not exist, will ignore it", K(ret),
            "rls_context id", schema->get_rls_context_id());
      } else {
        LOG_WARN("failed to delete rls_context from hashmap", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(schema)) {
    if (OB_FAIL(rls_context_name_map_.erase_refactored(
        ObRlsContextNameHashKey(schema->get_tenant_id(), schema->get_table_id(),
                                schema->get_context_name(), schema->get_attribute())))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        LOG_INFO("item does not exist, will ignore it", K(ret),
            "rls_context name", schema->get_context_name());
      } else {
        LOG_WARN("failed to delete rls_context from hashmap", K(ret));
      }
    }
  }

  if (rls_context_infos_.count() != rls_context_name_map_.item_count()
      || rls_context_infos_.count() != rls_context_id_map_.item_count()) {
    LOG_WARN("rls_context schema is non-consistent", K(id), K(rls_context_infos_.count()),
        K(rls_context_name_map_.item_count()), K(rls_context_id_map_.item_count()));
    int rebuild_ret = OB_SUCCESS;
    if (OB_SUCCESS != (rebuild_ret = rebuild_rls_context_hashmap())) {
      LOG_WARN("rebuild rls_context hashmap failed", K(rebuild_ret));
    }
    if (OB_SUCC(ret)) {
      ret = rebuild_ret;
    }
  }
  LOG_TRACE("rls_context del", K(id), K(rls_context_infos_.count()),
      K(rls_context_name_map_.item_count()), K(rls_context_id_map_.item_count()));
  return ret;
}

int ObRlsContextMgr::get_schema_by_id(const uint64_t rls_context_id,
                                     const ObRlsContextSchema *&schema) const
{
  int ret = OB_SUCCESS;
  schema = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == rls_context_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(rls_context_id));
  } else {
    ObRlsContextSchema *tmp_schema = NULL;
    int hash_ret = rls_context_id_map_.get_refactored(rls_context_id, tmp_schema);
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

int ObRlsContextMgr::get_schema_by_name(const uint64_t tenant_id,
                                        const uint64_t table_id,
                                        const common::ObString &name,
                                        const common::ObString &attribute,
                                        const ObRlsContextSchema *&schema) const
{
  int ret = OB_SUCCESS;
  schema = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || OB_INVALID_ID == table_id ||
             name.empty() || attribute.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(table_id), K(name), K(attribute));
  } else {
    ObRlsContextSchema *tmp_schema = NULL;
    ObRlsContextNameHashKey hash_wrapper(tenant_id, table_id, name, attribute);
    if (OB_FAIL(rls_context_name_map_.get_refactored(hash_wrapper, tmp_schema))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        LOG_INFO("schema is not exist", K(tenant_id), K(table_id), K(name), K(attribute),
                 "map_cnt", rls_context_name_map_.item_count());
      } else {
        LOG_WARN("failed to get rls_context from hashmap", K(ret));
      }
    } else {
      schema = tmp_schema;
    }
  }
  return ret;
}

int ObRlsContextMgr::get_schemas_in_tenant(const uint64_t tenant_id,
                                          common::ObIArray<const ObRlsContextSchema *> &schemas) const
{
  int ret = OB_SUCCESS;
  schemas.reset();
  ObTenantRlsContextId id(tenant_id, OB_MIN_ID);
  ConstRlsContextIter iter_begin =
      rls_context_infos_.lower_bound(id, compare_with_sort_key);
  bool is_stop = false;
  for (ConstRlsContextIter iter = iter_begin;
      OB_SUCC(ret) && iter != rls_context_infos_.end() && !is_stop; ++iter) {
    const ObRlsContextSchema *schema = NULL;
    if (OB_ISNULL(schema = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret), K(schema));
    } else if (tenant_id != schema->get_tenant_id()) {
      is_stop = true;
    } else if (OB_FAIL(schemas.push_back(schema))) {
      LOG_WARN("push back rls_context failed", K(ret));
    }
  }
  return ret;
}

int ObRlsContextMgr::get_schemas_in_table(const uint64_t tenant_id,
                                         const uint64_t table_id,
                                         common::ObIArray<const ObRlsContextSchema *> &schemas) const
{
  int ret = OB_SUCCESS;
  schemas.reset();
  ObTenantRlsContextId id(tenant_id, OB_MIN_ID);
  ConstRlsContextIter iter_begin =
      rls_context_infos_.lower_bound(id, compare_with_sort_key);
  bool is_stop = false;
  for (ConstRlsContextIter iter = iter_begin;
      OB_SUCC(ret) && iter != rls_context_infos_.end() && !is_stop; ++iter) {
    const ObRlsContextSchema *schema = NULL;
    if (OB_ISNULL(schema = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret), K(schema));
    } else if (tenant_id != schema->get_tenant_id()) {
      is_stop = true;
    } else if (schema->get_table_id() != table_id) {
      // do nothing
    } else if (OB_FAIL(schemas.push_back(schema))) {
      LOG_WARN("push back rls_context failed", K(ret));
    }
  }
  return ret;
}

int ObRlsContextMgr::del_schemas_in_tenant(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else {
    ObArray<const ObRlsContextSchema *> schemas;
    if (OB_FAIL(get_schemas_in_tenant(tenant_id, schemas))) {
      LOG_WARN("get rls_context schemas failed", K(ret), K(tenant_id));
    } else {
      FOREACH_CNT_X(schema, schemas, OB_SUCC(ret)) {
        ObTenantRlsContextId id(tenant_id, (*schema)->get_rls_context_id());
        if (OB_FAIL(del_rls_context(id))) {
          LOG_WARN("del rls_context failed",
                   "tenant_id", id.tenant_id_,
                   "rls_context_id", id.schema_id_,
                   K(ret));
        }
      }
    }
  }
  return ret;
}

int ObRlsContextMgr::get_schema_count(int64_t &schema_count) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    schema_count = rls_context_infos_.count();
  }
  return ret;
}

int ObRlsContextMgr::get_schema_statistics(ObSchemaStatisticsInfo &schema_info) const
{
  int ret = OB_SUCCESS;
  schema_info.reset();
  schema_info.schema_type_ = RLS_CONTEXT_SCHEMA;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    schema_info.count_ = rls_context_infos_.size();
    for (ConstRlsContextIter iter = rls_context_infos_.begin();
        OB_SUCC(ret) && iter != rls_context_infos_.end(); ++iter) {
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

int ObRlsContextMgr::rebuild_rls_context_hashmap()
{
  int ret = OB_SUCCESS;
  int over_write = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    rls_context_name_map_.clear();
    rls_context_id_map_.clear();
  }

  for (ConstRlsContextIter iter = rls_context_infos_.begin();
      OB_SUCC(ret) && iter != rls_context_infos_.end(); ++iter) {
    ObRlsContextSchema *rls_context_schema = *iter;
    ObRlsContextNameHashKey hash_wrapper;
    if (OB_ISNULL(rls_context_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rls_context_schema is NULL", K(ret), K(rls_context_schema));
    } else if (FALSE_IT(hash_wrapper.set_tenant_id(rls_context_schema->get_tenant_id()))) {
      // do nothing
    } else if (FALSE_IT(hash_wrapper.set_context_name(rls_context_schema->get_context_name()))) {
      // do nothing
    } else if (FALSE_IT(hash_wrapper.set_attribute(rls_context_schema->get_attribute()))) {
      // do nothing
    } else if (OB_FAIL(rls_context_name_map_.set_refactored(hash_wrapper,
        rls_context_schema, over_write))) {
      LOG_WARN("build rls_context name hashmap failed", K(ret),
          "rls_context_name", rls_context_schema->get_context_name());
    } else if (OB_FAIL(rls_context_id_map_.set_refactored(rls_context_schema->get_rls_context_id(),
        rls_context_schema, over_write))) {
      LOG_WARN("build rls_context id hashmap failed", K(ret),
          "rls_context_id", rls_context_schema->get_rls_context_id());
    }
  }
  return ret;
}

} // namespace schema
} // namespace share
} // namespace oceanbase
