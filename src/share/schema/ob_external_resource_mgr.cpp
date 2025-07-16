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

#include "ob_external_resource_mgr.h"

#include "share/schema/ob_schema_utils.h"

namespace oceanbase
{

namespace share
{

namespace schema
{

ObExternalResourceMgr::ObExternalResourceMgr()
  : is_inited_(false),
    local_allocator_(SET_USE_500(ObModIds::OB_SCHEMA_GETTER_GUARD, ObCtxIds::SCHEMA_SERVICE)),
    allocator_(local_allocator_),
    external_resource_infos_(0, nullptr, SET_USE_500("SchExtRes", ObCtxIds::SCHEMA_SERVICE)),
    external_resource_id_map_(SET_USE_500("SchExtRes", ObCtxIds::SCHEMA_SERVICE)),
    external_resource_name_map_(SET_USE_500("SchExtRes", ObCtxIds::SCHEMA_SERVICE))
{
  // do nothing
}

ObExternalResourceMgr::ObExternalResourceMgr(ObIAllocator &allocator)
  : is_inited_(false),
    local_allocator_(SET_USE_500(ObModIds::OB_SCHEMA_GETTER_GUARD, ObCtxIds::SCHEMA_SERVICE)),
    allocator_(allocator),
    external_resource_infos_(0, nullptr, SET_USE_500("SchExtRes", ObCtxIds::SCHEMA_SERVICE)),
    external_resource_id_map_(SET_USE_500("SchExtRes", ObCtxIds::SCHEMA_SERVICE)),
    external_resource_name_map_(SET_USE_500("SchExtRes", ObCtxIds::SCHEMA_SERVICE))
{
  // do nothing
}

int ObExternalResourceMgr::init()
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("external resource mgr init twice", K(ret), K(lbt()));
  } else if (OB_FAIL(external_resource_id_map_.init())) {
    LOG_WARN("failed to init external_resource_id_map_", K(ret));
  } else if (OB_FAIL(external_resource_name_map_.init())) {
    LOG_WARN("failed to init external_resource_name_map_", K(ret));
  } else {
    is_inited_ = true;
  }

  return ret;
}

void ObExternalResourceMgr::reset()
{
  if (!is_inited_) {
    LOG_WARN_RET(OB_NOT_INIT, "external resource manger not init");
  } else {
    external_resource_infos_.clear();
    external_resource_id_map_.clear();
    external_resource_name_map_.clear();
  }
}

int ObExternalResourceMgr::assign(const ObExternalResourceMgr &other)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("external resource manager not init", K(ret));
  } else if (this != &other) {
    if (OB_FAIL(external_resource_infos_.assign(other.external_resource_infos_))) {
      LOG_WARN("assign external resource id map failed", K(ret));
    } else if (OB_FAIL(external_resource_id_map_.assign(other.external_resource_id_map_))) {
      LOG_WARN("assign external resource name map failed", K(ret));
    } else if (OB_FAIL(external_resource_name_map_.assign(other.external_resource_name_map_))) {
      LOG_WARN("assign external resource infos vector failed", K(ret));
    }
  }

  return ret;
}

int ObExternalResourceMgr::deep_copy(const ObExternalResourceMgr &other)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("external resource manager not init", K(ret));
  } else if (this != &other) {
    reset();
    for (ExternalResourceIter iter = other.external_resource_infos_.begin();
         OB_SUCC(ret) && iter != other.external_resource_infos_.end();
         iter++) {
      ObSimpleExternalResourceSchema *resource_info = *iter;
      if (OB_ISNULL(resource_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected NULL resource_info", K(resource_info), K(ret));
      } else if (OB_FAIL(add_external_resource(*resource_info))) {
        LOG_WARN("failed to add_external_resource", K(*resource_info), K(ret));
      }
    }
  }

  return ret;
}

int ObExternalResourceMgr::get_external_resource_schema_count(int64_t &schema_count) const
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    LOG_WARN("external resource manger not init");
  } else {
    schema_count = external_resource_infos_.size();
  }

  return ret;
}

int ObExternalResourceMgr::get_schema_statistics(ObSchemaStatisticsInfo &schema_info) const
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("external resource manager not init", K(ret));
  } else {
    schema_info.reset();
    schema_info.schema_type_ = EXTERNAL_RESOURCE_SCHEMA;
    schema_info.count_ = external_resource_infos_.size();
    for (ExternalResourceConstIter it = external_resource_infos_.begin();
         OB_SUCC(ret) && it != external_resource_infos_.end();
         it++) {
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

int ObExternalResourceMgr::add_external_resource(const ObSimpleExternalResourceSchema &external_resource_schema)
{
  int ret = OB_SUCCESS;

  constexpr int overwrite = 1;

  ObSimpleExternalResourceSchema *new_schema = nullptr;
  ObSimpleExternalResourceSchema *replaced_schema = nullptr;
  ExternalResourceIter iter = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("external resource manager not init", K(ret));
  } else if (OB_UNLIKELY(!external_resource_schema.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid external_resource_schema", K(ret), K(external_resource_schema));
  } else if (OB_FAIL(ObSchemaUtils::alloc_schema(allocator_, external_resource_schema, new_schema))) {
    LOG_WARN("failed to alloc_schema", K(ret), K(external_resource_schema));
  } else if (OB_ISNULL(new_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL new_schema", K(ret), K(external_resource_schema));
  } else if (OB_FAIL(external_resource_infos_.replace(new_schema,
                                                      iter,
                                                      compare_external_resource,
                                                      equal_external_resource,
                                                      replaced_schema))) {
    LOG_WARN("failed to add external resource schema", K(ret), KPC(new_schema));
  } else {
    ObExternalResourceHashWrapper hash_wrapper(new_schema->get_tenant_id(),
                                               new_schema->get_database_id(),
                                               new_schema->get_name());

    if (OB_FAIL(external_resource_id_map_.set_refactored(new_schema->get_resource_id(), new_schema, overwrite))) {
      LOG_WARN("failed to set_refactored to external_resource_id_map_", K(ret), KPC(new_schema));
    } else if (OB_FAIL(external_resource_name_map_.set_refactored(hash_wrapper, new_schema, overwrite))) {
      LOG_WARN("failed to set_refactored to external_resource_name_map_", K(ret), KPC(new_schema));
    }
  }

  // always check
  if (external_resource_infos_.count() != external_resource_id_map_.item_count()
      || external_resource_infos_.count() != external_resource_name_map_.item_count()) {
    LOG_WARN("external resouce schema is inconsistent",
             K(ret),
             K(external_resource_infos_.count()),
             K(external_resource_id_map_.item_count()),
             K(external_resource_name_map_.item_count()),
             K(external_resource_infos_),
             K(external_resource_schema));
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = rebuild_external_resource_hashmap())) {
      LOG_WARN("rebuild external_resource hashmap failed", K(ret), K(tmp_ret));
    }
  }

  return ret;
}

int ObExternalResourceMgr::rebuild_external_resource_hashmap()
{
  int ret = OB_SUCCESS;

  constexpr int overwrite = 1;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("external resource manager not init", K(ret));
  } else {
    external_resource_id_map_.clear();
    external_resource_name_map_.clear();

    for (ExternalResourceConstIter iter = external_resource_infos_.begin();
         OB_SUCC(ret) && iter != external_resource_infos_.end();
         ++iter) {
      ObSimpleExternalResourceSchema *schema = *iter;
      if (OB_ISNULL(schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected NULL schema", K(ret), K(external_resource_infos_));
      } else {
        ObExternalResourceHashWrapper hash_wrapper(schema->get_tenant_id(),
                                                   schema->get_database_id(),
                                                   schema->get_name());

        if (OB_FAIL(external_resource_id_map_.set_refactored(schema->get_resource_id(), schema, overwrite))) {
          LOG_WARN("failed to set_refactored to external_resource_id_map_", K(ret), KPC(schema));
        } else if (OB_FAIL(external_resource_name_map_.set_refactored(hash_wrapper, schema, overwrite))) {
          LOG_WARN("failed to set_refactored to external_resource_name_map_", K(ret), KPC(schema));
        }
      }
    }
  }

  return ret;
}

int ObExternalResourceMgr::add_external_resources(const common::ObIArray<ObSimpleExternalResourceSchema> &external_resource_schemas)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("external resource manager not init", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < external_resource_schemas.count(); ++i) {
      if (OB_FAIL(add_external_resource(external_resource_schemas.at(i)))) {
        LOG_WARN("failed to add_external_resource", K(ret), K(i), K(external_resource_schemas.at(i)));
      }
    }
  }

  return ret;
}

int ObExternalResourceMgr::del_external_resource(const ObTenantExternalResourceId &external_resource)
{
  int ret = OB_SUCCESS;

  ObSimpleExternalResourceSchema *schema = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("external resource manager not init", K(ret));
  } else if (!external_resource.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid external_resource", K(ret), K(external_resource));
  } else if (OB_FAIL(external_resource_infos_.remove_if(external_resource,
                                                        compare_with_tenant_external_resource_id,
                                                        equal_to_tenant_external_resource_id,
                                                        schema))) {
    LOG_WARN("failed to remove external resource schema", K(ret), K(external_resource));
  } else if (OB_ISNULL(schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL schema removed", K(ret), K(external_resource));
  } else {
    int hash_ret = OB_SUCCESS;
    ObExternalResourceHashWrapper hash_wrapper(schema->get_tenant_id(),
                                               schema->get_database_id(),
                                               schema->get_name());

    if (OB_SUCCESS != (hash_ret = external_resource_id_map_.erase_refactored(schema->get_resource_id()))) {
      LOG_WARN("failed erase_refactored from id hashmap",
               K(ret),
               K(hash_ret),
               K(schema->get_resource_id()));
      ret = OB_HASH_NOT_EXIST != hash_ret ? hash_ret : ret;
    } else if (OB_SUCCESS != (hash_ret = external_resource_name_map_.erase_refactored(hash_wrapper))) {
      LOG_WARN("failed erase_refactored from name hashmap",
               K(ret),
               K(hash_ret),
               K(hash_wrapper));
      ret = OB_HASH_NOT_EXIST != hash_ret ? hash_ret : ret;
    }
  }

  // always check
  if (external_resource_infos_.count() != external_resource_id_map_.item_count()
      || external_resource_infos_.count() != external_resource_name_map_.item_count()) {
    LOG_WARN("external resouce schema is inconsistent",
             K(ret),
             K(external_resource_infos_.count()),
             K(external_resource_id_map_.item_count()),
             K(external_resource_name_map_.item_count()),
             K(external_resource_infos_),
             K(external_resource));
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = rebuild_external_resource_hashmap())) {
      LOG_WARN("rebuild external_resource hashmap failed", K(ret), K(tmp_ret));
    }
  }

  return ret;
}

int ObExternalResourceMgr::get_external_resource_schema(const uint64_t external_resource_id,
                                                        const ObSimpleExternalResourceSchema *&external_resource_schema) const
{
  int ret = OB_SUCCESS;

  external_resource_schema = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("external resource manager not init", K(ret));
  } else if (OB_INVALID_ID == external_resource_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid external_resource_id", K(ret), K(external_resource_id));
  } else {
    ObSimpleExternalResourceSchema *tmp_schema = nullptr;

    if (OB_FAIL(external_resource_id_map_.get_refactored(external_resource_id, tmp_schema))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to get_refactored from external_resource_id_map_", K(ret), K(external_resource_id), K(external_resource_infos_));
      }
    } else if (OB_ISNULL(tmp_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL schema", K(ret), K(external_resource_id), K(external_resource_infos_));
    } else {
      external_resource_schema = tmp_schema;
    }
  }

  return ret;
}

int ObExternalResourceMgr::get_external_resource_schema(const uint64_t tenant_id,
                                                        const uint64_t database_id,
                                                        const common::ObString &name,
                                                        const ObSimpleExternalResourceSchema *&external_resource_schema) const
{
  int ret = OB_SUCCESS;

  external_resource_schema = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("external resource manager not init", K(ret));
  } else if (!is_valid_tenant_id(tenant_id) || OB_INVALID_ID == database_id || name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(database_id), K(name));
  } else {
    ObSimpleExternalResourceSchema *tmp_schema = nullptr;

    ObExternalResourceHashWrapper hash_wrapper(tenant_id, database_id, name);

    if (OB_FAIL(external_resource_name_map_.get_refactored(hash_wrapper, tmp_schema))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to get_refactored from external_resource_name_map_", K(ret), K(hash_wrapper), K(external_resource_infos_));
      }
    } else if (OB_ISNULL(tmp_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL schema", K(ret), K(hash_wrapper), K(external_resource_infos_));
    } else {
      external_resource_schema = tmp_schema;
    }
  }

  return ret;
}

int ObExternalResourceMgr::get_external_resource_schemas_in_tenant(const uint64_t tenant_id,
                                                                   common::ObIArray<const ObSimpleExternalResourceSchema *> &external_resource_schemas) const
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("external resource manager not init", K(ret));
  } else if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(ret), K(tenant_id));
  } else {
    external_resource_schemas.reuse();

    ObTenantExternalResourceId tenant_id_lower(tenant_id, OB_MIN_ID);
    ExternalResourceConstIter lower_bound = external_resource_infos_.lower_bound(tenant_id_lower, compare_with_tenant_external_resource_id);
    bool is_stop = false;

    for (ExternalResourceConstIter iter = lower_bound;
        OB_SUCC(ret) && !is_stop && iter != external_resource_infos_.end();
        ++iter) {
      const ObSimpleExternalResourceSchema *schema = nullptr;

      if (OB_ISNULL(schema = *iter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected NULL schema", K(ret), K(schema), K(external_resource_infos_));
      } else if (tenant_id != schema->get_tenant_id()) {
        is_stop = true;
      } else if (OB_FAIL(external_resource_schemas.push_back(schema))) {
        LOG_WARN("failed to push_back", K(ret), KPC(schema));
      }
    }
  }

  return ret;
}

int ObExternalResourceMgr::get_external_resource_schemas_in_database(const uint64_t tenant_id,
                                                                     const uint64_t database_id,
                                                                     common::ObIArray<const ObSimpleExternalResourceSchema *> &external_resource_schemas) const
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("external resource manager not init", K(ret));
  } else if (!is_valid_tenant_id(tenant_id) || OB_INVALID_ID == database_id ) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(database_id));
  } else {
    external_resource_schemas.reuse();

    ObTenantExternalResourceId tenant_id_lower(tenant_id, OB_MIN_ID);
    ExternalResourceConstIter lower_bound = external_resource_infos_.lower_bound(tenant_id_lower, compare_with_tenant_external_resource_id);
    bool is_stop = false;

    for (ExternalResourceConstIter iter = lower_bound;
        OB_SUCC(ret) && !is_stop && iter != external_resource_infos_.end();
        ++iter) {
      const ObSimpleExternalResourceSchema *schema = nullptr;

      if (OB_ISNULL(schema = *iter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected NULL schema", K(ret), K(schema), K(external_resource_infos_));
      } else if (tenant_id != schema->get_tenant_id()) {
        is_stop = true;
      } else if (database_id != schema->get_database_id()) {
        // do nothing
      } else if (OB_FAIL(external_resource_schemas.push_back(schema))) {
        LOG_WARN("failed to push_back", K(ret), KPC(schema));
      }
    }
  }

  return ret;
}

int ObExternalResourceMgr::del_schemas_in_tenant(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("external resource manager not init", K(ret));
  } else if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(ret), K(tenant_id));
  } else {
    ObSEArray<const ObSimpleExternalResourceSchema *, 32> schemas;

    if (OB_FAIL(get_external_resource_schemas_in_tenant(tenant_id, schemas))) {
      LOG_WARN("failed to get_external_resource_schemas_in_tenant", K(ret), K(tenant_id));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < schemas.count(); ++i) {
        const ObSimpleExternalResourceSchema *curr = schemas.at(i);

        if (OB_ISNULL(curr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected NULL schema", K(ret), K(i), K(schemas));
        } else {
          ObTenantExternalResourceId tenant_schema_id(tenant_id, curr->get_resource_id());

          if (OB_FAIL(del_external_resource(tenant_schema_id))) {
            LOG_WARN("failed to del_external_resource", K(ret), K(tenant_schema_id));
          }
        }
      }
    }
  }

  return ret;
}

} // namespace schema
} // namespace share
} // namespace oceanbase