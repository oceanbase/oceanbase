/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE_SCHEMA

#include "ob_ai_provider_mgr.h"
#include "share/schema/ob_schema_utils.h"

namespace oceanbase
{

namespace share
{

namespace schema
{

OB_SERIALIZE_MEMBER(ObAIProviderSchema,
                    tenant_id_,
                    provider_id_,
                    name_,
                    protocol_,
                    base_url_,
                    access_key_,
                    schema_version_,
                    case_mode_);

int64_t ObAIProviderSchema::get_convert_size() const
{
  int64_t convert_size = sizeof(ObAIProviderSchema);

  convert_size += name_.length() + 1;
  convert_size += protocol_.length() + 1;
  convert_size += base_url_.length() + 1;
  convert_size += access_key_.length() + 1;

  return convert_size;
}

int ObAIProviderSchema::assign(const ObAIProviderSchema &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    reset();
    if (OB_FAIL(set_name(other.name_))) {
      LOG_WARN("failed to set name", K(ret), K(*this), K(other));
    } else if (OB_FAIL(set_protocol(other.protocol_))) {
      LOG_WARN("failed to set protocol", K(ret), K(*this), K(other));
    } else if (OB_FAIL(set_base_url(other.base_url_))) {
      LOG_WARN("failed to set base_url", K(ret), K(*this), K(other));
    } else if (OB_FAIL(set_access_key(other.access_key_))) {
      LOG_WARN("failed to set access_key", K(ret), K(*this), K(other));
    } else {
      set_tenant_id(other.tenant_id_);
      set_provider_id(other.provider_id_);
      set_schema_version(other.schema_version_);
      set_case_mode(other.case_mode_);
    }
  }
  return ret;
}


ObAIProviderMgr::ObAIProviderMgr()
  : is_inited_(false),
    local_allocator_(SET_USE_500(ObModIds::OB_SCHEMA_GETTER_GUARD, ObCtxIds::SCHEMA_SERVICE)),
    allocator_(local_allocator_),
    provider_infos_(0, nullptr, SET_USE_500("SchAIProvider", ObCtxIds::SCHEMA_SERVICE)),
    provider_id_map_(SET_USE_500("SchAIProvider", ObCtxIds::SCHEMA_SERVICE)),
    provider_name_map_(SET_USE_500("SchAIProvider", ObCtxIds::SCHEMA_SERVICE))
{
}

ObAIProviderMgr::ObAIProviderMgr(ObIAllocator &allocator)
  : is_inited_(false),
    local_allocator_(SET_USE_500(ObModIds::OB_SCHEMA_GETTER_GUARD, ObCtxIds::SCHEMA_SERVICE)),
    allocator_(allocator),
    provider_infos_(0, nullptr, SET_USE_500("SchAIProvider", ObCtxIds::SCHEMA_SERVICE)),
    provider_id_map_(SET_USE_500("SchAIProvider", ObCtxIds::SCHEMA_SERVICE)),
    provider_name_map_(SET_USE_500("SchAIProvider", ObCtxIds::SCHEMA_SERVICE))
{
}

int ObAIProviderMgr::init()
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ai provider mgr init twice", K(ret), K(lbt()));
  } else if (OB_FAIL(provider_id_map_.init())) {
    LOG_WARN("failed to init provider_id_map_", K(ret));
  } else if (OB_FAIL(provider_name_map_.init())) {
    LOG_WARN("failed to init provider_name_map_", K(ret));
  } else {
    is_inited_ = true;
  }

  return ret;
}

void ObAIProviderMgr::reset()
{
  if (!is_inited_) {
    LOG_WARN_RET(OB_NOT_INIT, "ai provider mgr not init");
  } else {
    provider_infos_.clear();
    provider_id_map_.clear();
    provider_name_map_.clear();
  }
}

int ObAIProviderMgr::assign(const ObAIProviderMgr &other)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ai provider mgr not init", K(ret));
  } else if (this != &other) {
    if (OB_FAIL(provider_infos_.assign(other.provider_infos_))) {
      LOG_WARN("assign provider infos failed", K(ret));
    } else if (OB_FAIL(provider_id_map_.assign(other.provider_id_map_))) {
      LOG_WARN("assign provider id map failed", K(ret));
    } else if (OB_FAIL(provider_name_map_.assign(other.provider_name_map_))) {
      LOG_WARN("assign provider name map failed", K(ret));
    }
  }

  return ret;
}

int ObAIProviderMgr::deep_copy(const ObAIProviderMgr &other)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ai provider mgr not init", K(ret));
  } else if (this != &other) {
    reset();
    for (ProviderIter iter = other.provider_infos_.begin();
         OB_SUCC(ret) && iter != other.provider_infos_.end();
         iter++) {
      ObAIProviderSchema *provider_schema = *iter;
      if (OB_ISNULL(provider_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected NULL provider_schema", K(provider_schema), K(ret));
      } else if (OB_FAIL(add_ai_provider(*provider_schema, provider_schema->get_case_mode()))) {
        LOG_WARN("failed to add_ai_provider_schema", K(*provider_schema), K(ret));
      }
    }
  }

  return ret;
}

int ObAIProviderMgr::get_ai_provider_schema_count(int64_t &schema_count) const
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ai provider mgr not init", K(ret));
  } else {
    schema_count = provider_infos_.size();
  }

  return ret;
}

int ObAIProviderMgr::get_schema_statistics(ObSchemaStatisticsInfo &schema_info) const
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ai provider mgr not init", K(ret));
  } else {
    schema_info.reset();
    schema_info.schema_type_ = AI_PROVIDER_SCHEMA;
    schema_info.count_ = provider_infos_.size();
    for (ProviderConstIter it = provider_infos_.begin();
         OB_SUCC(ret) && it != provider_infos_.end();
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

int ObAIProviderMgr::get_ai_provider_schema(const uint64_t provider_id, const ObAIProviderSchema *&provider_schema) const
{
  int ret = OB_SUCCESS;
  ObAIProviderSchema *tmp_schema = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ai provider mgr not init", K(ret));
  } else if (OB_INVALID_ID == provider_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid provider_id", K(ret), K(provider_id));
  } else if (OB_FAIL(provider_id_map_.get_refactored(provider_id, tmp_schema))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get provider_schema", K(ret), K(provider_id));
    }
  } else if (OB_ISNULL(tmp_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL provider_schema", K(ret), K(provider_id));
  } else {
    provider_schema = tmp_schema;
  }

  return ret;
}

int ObAIProviderMgr::get_ai_provider_schema(const uint64_t tenant_id, const common::ObString &name,
                                            const common::ObNameCaseMode case_mode,
                                            const ObAIProviderSchema *&provider_schema) const
{
  int ret = OB_SUCCESS;
  ObAIProviderSchema *tmp_schema = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ai provider mgr not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id || name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(name));
  } else {
    ObAIProviderHashWrapper hash_wrapper(tenant_id, name, case_mode);

    if (OB_FAIL(provider_name_map_.get_refactored(hash_wrapper, tmp_schema))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to get provider_schema", K(ret), K(name));
      }
    } else if (OB_ISNULL(tmp_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL provider_schema", K(ret), K(name));
    } else {
      provider_schema = tmp_schema;
    }
  }

  return ret;
}

int ObAIProviderMgr::del_schemas_in_tenant(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ai provider mgr not init", K(ret));
  } else if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(ret), K(tenant_id));
  } else {
    ObSEArray<const ObAIProviderSchema *, 32> schemas;

    if (OB_FAIL(get_ai_provider_schemas_in_tenant(tenant_id, schemas))) {
      LOG_WARN("failed to get_ai_provider_schemas_in_tenant", K(ret), K(tenant_id));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < schemas.count(); ++i) {
        const ObAIProviderSchema *curr = schemas.at(i);

        if (OB_ISNULL(curr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected NULL schema", K(ret), K(i), K(schemas));
        } else {
          if (OB_FAIL(del_ai_provider(ObTenantAIProviderId(tenant_id, curr->get_provider_id())))) {
            LOG_WARN("failed to del_ai_provider", K(ret), K(tenant_id), K(curr->get_provider_id()));
          }
        }
      }
    }
  }
  return ret;
}

int ObAIProviderMgr::add_ai_provider(const ObAIProviderSchema &provider_schema, common::ObNameCaseMode case_mode)
{
  int ret = OB_SUCCESS;

  constexpr int overwrite = 1;

  ObAIProviderSchema *new_schema = nullptr;
  ObAIProviderSchema *replaced_schema = nullptr;
  ProviderIter iter = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ai provider mgr not init", K(ret));
  } else if (OB_UNLIKELY(!provider_schema.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid provider_schema", K(ret), K(provider_schema));
  } else if (OB_FAIL(ObSchemaUtils::alloc_schema(allocator_, provider_schema, new_schema))) {
    LOG_WARN("failed to alloc_schema", K(ret), K(provider_schema));
  } else if (OB_ISNULL(new_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL new_schema", K(ret), K(provider_schema));
  } else if (OB_FALSE_IT(new_schema->set_case_mode(case_mode))) {
  } else if (OB_FAIL(provider_infos_.replace(new_schema,
                                             iter,
                                             compare_provider,
                                             equal_provider,
                                             replaced_schema))) {
    LOG_WARN("failed to add provider schema", K(ret), KPC(new_schema));
  } else {
    ObAIProviderHashWrapper hash_wrapper(new_schema->get_tenant_id(),
                                         new_schema->get_name(),
                                         new_schema->get_case_mode());

    if (OB_FAIL(provider_id_map_.set_refactored(new_schema->get_provider_id(), new_schema, overwrite))) {
      LOG_WARN("failed to set_refactored to provider_id_map_", K(ret), KPC(new_schema));
    } else if (OB_FAIL(provider_name_map_.set_refactored(hash_wrapper, new_schema, overwrite))) {
      LOG_WARN("failed to set_refactored to provider_name_map_", K(ret), KPC(new_schema));
    }
  }

  // always check, it may become inconsistent in some specific scenarios(e.g., error code -4013),
  // if not equal, rebuild the hashmap
  if (provider_infos_.count() != provider_id_map_.item_count()
      || provider_infos_.count() != provider_name_map_.item_count()) {
    LOG_WARN("ai provider schema is inconsistent",
             K(ret),
             K(provider_infos_.count()),
             K(provider_id_map_.item_count()),
             K(provider_name_map_.item_count()),
             K(provider_infos_));
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = rebuild_ai_provider_hashmap())) {
      LOG_WARN("rebuild ai provider hashmap failed", K(ret), K(tmp_ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }
  return ret;
}

int ObAIProviderMgr::del_ai_provider(const ObTenantAIProviderId &tenant_provider_id)
{
  int ret = OB_SUCCESS;
  ObAIProviderSchema *schema = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ai provider mgr not init", K(ret));
  } else if (!tenant_provider_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid provider_id", K(ret), K(tenant_provider_id));
  } else if (OB_FAIL(provider_infos_.remove_if(tenant_provider_id,
                                               compare_with_tenant_provider_id,
                                               equal_to_tenant_provider_id,
                                               schema))) {
    LOG_WARN("failed to remove provider_schema", K(ret), K(tenant_provider_id));
  } else if (OB_ISNULL(schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL provider_schema", K(ret), K(tenant_provider_id));
  } else {
    int hash_ret = OB_SUCCESS;
    ObAIProviderHashWrapper hash_wrapper(schema->get_tenant_id(),
                                         schema->get_name(),
                                         schema->get_case_mode());

    if (OB_SUCCESS != (hash_ret = provider_id_map_.erase_refactored(schema->get_provider_id()))) {
      LOG_WARN("failed erase_refactored from id hashmap",
               K(ret),
               K(hash_ret),
               K(schema->get_provider_id()));
      ret = OB_HASH_NOT_EXIST != hash_ret ? hash_ret : ret;
    } else if (OB_SUCCESS != (hash_ret = provider_name_map_.erase_refactored(hash_wrapper))) {
      LOG_WARN("failed erase_refactored from name hashmap",
               K(ret),
               K(hash_ret),
               K(hash_wrapper));
      ret = OB_HASH_NOT_EXIST != hash_ret ? hash_ret : ret;
    }
  }

  // always check
  if (provider_infos_.count() != provider_id_map_.item_count()
      || provider_infos_.count() != provider_name_map_.item_count()) {
    LOG_WARN("ai provider schema is inconsistent",
             K(ret),
             K(provider_infos_.count()),
             K(provider_id_map_.item_count()),
             K(provider_name_map_.item_count()),
             K(provider_infos_));
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = rebuild_ai_provider_hashmap())) {
      LOG_WARN("rebuild ai provider hashmap failed", K(ret), K(tmp_ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }

  return ret;
}

int ObAIProviderMgr::get_ai_provider_schemas_in_tenant(const uint64_t tenant_id, common::ObIArray<const ObAIProviderSchema *> &provider_schemas) const
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ai provider mgr not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(ret), K(tenant_id));
  } else {
    provider_schemas.reuse();

    ObTenantAIProviderId tenant_id_lower(tenant_id, OB_MIN_ID);
    ProviderConstIter lower_bound = provider_infos_.lower_bound(tenant_id_lower, compare_with_tenant_provider_id);
    bool is_stop = false;

    for (ProviderConstIter iter = lower_bound;
         OB_SUCC(ret) && !is_stop && iter != provider_infos_.end();
         iter++) {
      const ObAIProviderSchema *schema = *iter;
      if (OB_ISNULL(schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected NULL schema", K(ret), K(schema), K(provider_infos_));
      } else if (tenant_id != schema->get_tenant_id()) {
        is_stop = true;
      } else if (OB_FAIL(provider_schemas.push_back(schema))) {
        LOG_WARN("failed to push_back", K(ret), KPC(schema));
      }
    }
  }
  return ret;
}

int ObAIProviderMgr::rebuild_ai_provider_hashmap()
{
  int ret = OB_SUCCESS;
  constexpr int overwrite = 1;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ai provider mgr not init", K(ret));
  } else {
    provider_id_map_.clear();
    provider_name_map_.clear();

    for (ProviderIter iter = provider_infos_.begin();
         OB_SUCC(ret) && iter != provider_infos_.end();
         iter++) {
      ObAIProviderSchema *schema = *iter;
      if (OB_ISNULL(schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected NULL schema", K(ret), K(provider_infos_));
      } else {
        ObAIProviderHashWrapper hash_wrapper(schema->get_tenant_id(),
                                             schema->get_name(),
                                             schema->get_case_mode());

        if (OB_FAIL(provider_id_map_.set_refactored(schema->get_provider_id(), schema, overwrite))) {
          LOG_WARN("failed to set_refactored to provider_id_map_", K(ret), KPC(schema));
        } else if (OB_FAIL(provider_name_map_.set_refactored(hash_wrapper, schema, overwrite))) {
          LOG_WARN("failed to set_refactored to provider_name_map_", K(ret), KPC(schema));
        }
      }
    }
  }

  return ret;
}

} // namespace schema
} // namespace share
} // namespace oceanbase
