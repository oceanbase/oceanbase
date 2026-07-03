/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE_SCHEMA

#include "ob_ai_gateway_mgr.h"
#include "share/schema/ob_schema_utils.h"

namespace oceanbase
{

namespace share
{

namespace schema
{

OB_SERIALIZE_MEMBER(ObAIGatewaySchema,
                    tenant_id_,
                    gateway_id_,
                    name_,
                    endpoints_,
                    circuit_breaker_,
                    schema_version_,
                    case_mode_);

int64_t ObAIGatewaySchema::get_convert_size() const
{
  int64_t convert_size = sizeof(ObAIGatewaySchema);

  convert_size += name_.length() + 1;
  convert_size += endpoints_.length() + 1;
  convert_size += circuit_breaker_.length() + 1;

  return convert_size;
}

int ObAIGatewaySchema::assign(const ObAIGatewaySchema &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    reset();
    if (OB_FAIL(set_name(other.name_))) {
      LOG_WARN("failed to set name", K(ret), K(*this), K(other));
    } else if (OB_FAIL(set_endpoints(other.endpoints_))) {
      LOG_WARN("failed to set endpoints", K(ret), K(*this), K(other));
    } else if (OB_FAIL(set_circuit_breaker(other.circuit_breaker_))) {
      LOG_WARN("failed to set circuit_breaker", K(ret), K(*this), K(other));
    } else {
      set_tenant_id(other.tenant_id_);
      set_gateway_id(other.gateway_id_);
      set_schema_version(other.schema_version_);
      set_case_mode(other.case_mode_);
    }
  }
  return ret;
}


ObAIGatewayMgr::ObAIGatewayMgr()
  : is_inited_(false),
    local_allocator_(SET_USE_500(ObModIds::OB_SCHEMA_GETTER_GUARD, ObCtxIds::SCHEMA_SERVICE)),
    allocator_(local_allocator_),
    gateway_infos_(0, nullptr, SET_USE_500("SchAIGateway", ObCtxIds::SCHEMA_SERVICE)),
    gateway_id_map_(SET_USE_500("SchAIGateway", ObCtxIds::SCHEMA_SERVICE)),
    gateway_name_map_(SET_USE_500("SchAIGateway", ObCtxIds::SCHEMA_SERVICE))
{
}

ObAIGatewayMgr::ObAIGatewayMgr(ObIAllocator &allocator)
  : is_inited_(false),
    local_allocator_(SET_USE_500(ObModIds::OB_SCHEMA_GETTER_GUARD, ObCtxIds::SCHEMA_SERVICE)),
    allocator_(allocator),
    gateway_infos_(0, nullptr, SET_USE_500("SchAIGateway", ObCtxIds::SCHEMA_SERVICE)),
    gateway_id_map_(SET_USE_500("SchAIGateway", ObCtxIds::SCHEMA_SERVICE)),
    gateway_name_map_(SET_USE_500("SchAIGateway", ObCtxIds::SCHEMA_SERVICE))
{
}

int ObAIGatewayMgr::init()
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ai gateway mgr init twice", K(ret), K(lbt()));
  } else if (OB_FAIL(gateway_id_map_.init())) {
    LOG_WARN("failed to init gateway_id_map_", K(ret));
  } else if (OB_FAIL(gateway_name_map_.init())) {
    LOG_WARN("failed to init gateway_name_map_", K(ret));
  } else {
    is_inited_ = true;
  }

  return ret;
}

void ObAIGatewayMgr::reset()
{
  if (!is_inited_) {
    LOG_WARN_RET(OB_NOT_INIT, "ai gateway mgr not init");
  } else {
    gateway_infos_.clear();
    gateway_id_map_.clear();
    gateway_name_map_.clear();
  }
}

int ObAIGatewayMgr::assign(const ObAIGatewayMgr &other)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ai gateway mgr not init", K(ret));
  } else if (this != &other) {
    if (OB_FAIL(gateway_infos_.assign(other.gateway_infos_))) {
      LOG_WARN("assign gateway infos failed", K(ret));
    } else if (OB_FAIL(gateway_id_map_.assign(other.gateway_id_map_))) {
      LOG_WARN("assign gateway id map failed", K(ret));
    } else if (OB_FAIL(gateway_name_map_.assign(other.gateway_name_map_))) {
      LOG_WARN("assign gateway name map failed", K(ret));
    }
  }

  return ret;
}

int ObAIGatewayMgr::deep_copy(const ObAIGatewayMgr &other)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ai gateway mgr not init", K(ret));
  } else if (this != &other) {
    reset();
    for (GatewayIter iter = other.gateway_infos_.begin();
         OB_SUCC(ret) && iter != other.gateway_infos_.end();
         iter++) {
      ObAIGatewaySchema *gateway_schema = *iter;
      if (OB_ISNULL(gateway_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected NULL gateway_schema", K(gateway_schema), K(ret));
      } else if (OB_FAIL(add_ai_gateway(*gateway_schema, gateway_schema->get_case_mode()))) {
        LOG_WARN("failed to add_ai_gateway_schema", K(*gateway_schema), K(ret));
      }
    }
  }

  return ret;
}

int ObAIGatewayMgr::get_ai_gateway_schema_count(int64_t &schema_count) const
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ai gateway mgr not init", K(ret));
  } else {
    schema_count = gateway_infos_.size();
  }

  return ret;
}

int ObAIGatewayMgr::get_schema_statistics(ObSchemaStatisticsInfo &schema_info) const
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ai gateway mgr not init", K(ret));
  } else {
    schema_info.reset();
    schema_info.schema_type_ = AI_GATEWAY_SCHEMA;
    schema_info.count_ = gateway_infos_.size();
    for (GatewayConstIter it = gateway_infos_.begin();
         OB_SUCC(ret) && it != gateway_infos_.end();
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

int ObAIGatewayMgr::get_ai_gateway_schema(const uint64_t gateway_id, const ObAIGatewaySchema *&gateway_schema) const
{
  int ret = OB_SUCCESS;
  ObAIGatewaySchema *tmp_schema = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ai gateway mgr not init", K(ret));
  } else if (OB_INVALID_ID == gateway_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid gateway_id", K(ret), K(gateway_id));
  } else if (OB_FAIL(gateway_id_map_.get_refactored(gateway_id, tmp_schema))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get gateway_schema", K(ret), K(gateway_id));
    }
  } else if (OB_ISNULL(tmp_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL gateway_schema", K(ret), K(gateway_id));
  } else {
    gateway_schema = tmp_schema;
  }

  return ret;
}

int ObAIGatewayMgr::get_ai_gateway_schema(const uint64_t tenant_id, const common::ObString &name,
                                            const common::ObNameCaseMode case_mode,
                                            const ObAIGatewaySchema *&gateway_schema) const
{
  int ret = OB_SUCCESS;
  ObAIGatewaySchema *tmp_schema = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ai gateway mgr not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id || name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(name));
  } else {
    ObAIGatewayHashWrapper hash_wrapper(tenant_id, name, case_mode);

    if (OB_FAIL(gateway_name_map_.get_refactored(hash_wrapper, tmp_schema))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to get gateway_schema", K(ret), K(name));
      }
    } else if (OB_ISNULL(tmp_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL gateway_schema", K(ret), K(name));
    } else {
      gateway_schema = tmp_schema;
    }
  }

  return ret;
}

int ObAIGatewayMgr::del_schemas_in_tenant(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ai gateway mgr not init", K(ret));
  } else if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(ret), K(tenant_id));
  } else {
    ObSEArray<const ObAIGatewaySchema *, 32> schemas;

    if (OB_FAIL(get_ai_gateway_schemas_in_tenant(tenant_id, schemas))) {
      LOG_WARN("failed to get_ai_gateway_schemas_in_tenant", K(ret), K(tenant_id));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < schemas.count(); ++i) {
        const ObAIGatewaySchema *curr = schemas.at(i);

        if (OB_ISNULL(curr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected NULL schema", K(ret), K(i), K(schemas));
        } else {
          if (OB_FAIL(del_ai_gateway(ObTenantAIGatewayId(tenant_id, curr->get_gateway_id())))) {
            LOG_WARN("failed to del_ai_gateway", K(ret), K(tenant_id), K(curr->get_gateway_id()));
          }
        }
      }
    }
  }
  return ret;
}

int ObAIGatewayMgr::add_ai_gateway(const ObAIGatewaySchema &gateway_schema, common::ObNameCaseMode case_mode)
{
  int ret = OB_SUCCESS;

  constexpr int overwrite = 1;

  ObAIGatewaySchema *new_schema = nullptr;
  ObAIGatewaySchema *replaced_schema = nullptr;
  GatewayIter iter = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ai gateway mgr not init", K(ret));
  } else if (OB_UNLIKELY(!gateway_schema.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid gateway_schema", K(ret), K(gateway_schema));
  } else if (OB_FAIL(ObSchemaUtils::alloc_schema(allocator_, gateway_schema, new_schema))) {
    LOG_WARN("failed to alloc_schema", K(ret), K(gateway_schema));
  } else if (OB_ISNULL(new_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL new_schema", K(ret), K(gateway_schema));
  } else if (OB_FALSE_IT(new_schema->set_case_mode(case_mode))) {
  } else if (OB_FAIL(gateway_infos_.replace(new_schema,
                                             iter,
                                             compare_gateway,
                                             equal_gateway,
                                             replaced_schema))) {
    LOG_WARN("failed to add gateway schema", K(ret), KPC(new_schema));
  } else {
    ObAIGatewayHashWrapper hash_wrapper(new_schema->get_tenant_id(),
                                         new_schema->get_name(),
                                         new_schema->get_case_mode());

    if (OB_FAIL(gateway_id_map_.set_refactored(new_schema->get_gateway_id(), new_schema, overwrite))) {
      LOG_WARN("failed to set_refactored to gateway_id_map_", K(ret), KPC(new_schema));
    } else if (OB_FAIL(gateway_name_map_.set_refactored(hash_wrapper, new_schema, overwrite))) {
      LOG_WARN("failed to set_refactored to gateway_name_map_", K(ret), KPC(new_schema));
    }
  }

  // always check, it may become inconsistent in some specific scenarios(e.g., error code -4013),
  // if not equal, rebuild the hashmap
  if (gateway_infos_.count() != gateway_id_map_.item_count()
      || gateway_infos_.count() != gateway_name_map_.item_count()) {
    LOG_WARN("ai gateway schema is inconsistent",
             K(ret),
             K(gateway_infos_.count()),
             K(gateway_id_map_.item_count()),
             K(gateway_name_map_.item_count()),
             K(gateway_infos_));
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = rebuild_ai_gateway_hashmap())) {
      LOG_WARN("rebuild ai gateway hashmap failed", K(ret), K(tmp_ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }
  return ret;
}

int ObAIGatewayMgr::add_ai_gateways(const common::ObIArray<ObAIGatewaySchema> &gateway_schemas, common::ObNameCaseMode case_mode)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < gateway_schemas.count(); ++i) {
    if (OB_FAIL(add_ai_gateway(gateway_schemas.at(i), case_mode))) {
      LOG_WARN("failed to add ai gateway", K(ret), K(i));
    }
  }
  return ret;
}

int ObAIGatewayMgr::del_ai_gateway(const ObTenantAIGatewayId &tenant_gateway_id)
{
  int ret = OB_SUCCESS;
  ObAIGatewaySchema *schema = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ai gateway mgr not init", K(ret));
  } else if (!tenant_gateway_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid gateway_id", K(ret), K(tenant_gateway_id));
  } else if (OB_FAIL(gateway_infos_.remove_if(tenant_gateway_id,
                                               compare_with_tenant_gateway_id,
                                               equal_to_tenant_gateway_id,
                                               schema))) {
    LOG_WARN("failed to remove gateway_schema", K(ret), K(tenant_gateway_id));
  } else if (OB_ISNULL(schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL gateway_schema", K(ret), K(tenant_gateway_id));
  } else {
    int hash_ret = OB_SUCCESS;
    ObAIGatewayHashWrapper hash_wrapper(schema->get_tenant_id(),
                                         schema->get_name(),
                                         schema->get_case_mode());

    if (OB_SUCCESS != (hash_ret = gateway_id_map_.erase_refactored(schema->get_gateway_id()))) {
      LOG_WARN("failed erase_refactored from id hashmap",
               K(ret),
               K(hash_ret),
               K(schema->get_gateway_id()));
      ret = OB_HASH_NOT_EXIST != hash_ret ? hash_ret : ret;
    } else if (OB_SUCCESS != (hash_ret = gateway_name_map_.erase_refactored(hash_wrapper))) {
      LOG_WARN("failed erase_refactored from name hashmap",
               K(ret),
               K(hash_ret),
               K(hash_wrapper));
      ret = OB_HASH_NOT_EXIST != hash_ret ? hash_ret : ret;
    }
  }

  // always check, it may become inconsistent in some specific scenarios(e.g., error code -4013),
  // if not equal, rebuild the hashmap
  if (gateway_infos_.count() != gateway_id_map_.item_count()
      || gateway_infos_.count() != gateway_name_map_.item_count()) {
    LOG_WARN("ai gateway schema is inconsistent",
             K(ret),
             K(gateway_infos_.count()),
             K(gateway_id_map_.item_count()),
             K(gateway_name_map_.item_count()),
             K(gateway_infos_));
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = rebuild_ai_gateway_hashmap())) {
      LOG_WARN("rebuild ai gateway hashmap failed", K(ret), K(tmp_ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }

  return ret;
}

int ObAIGatewayMgr::get_ai_gateway_schemas_in_tenant(const uint64_t tenant_id, common::ObIArray<const ObAIGatewaySchema *> &gateway_schemas) const
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ai gateway mgr not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(ret), K(tenant_id));
  } else {
    gateway_schemas.reuse();

    ObTenantAIGatewayId tenant_id_lower(tenant_id, OB_MIN_ID);
    GatewayConstIter lower_bound = gateway_infos_.lower_bound(tenant_id_lower, compare_with_tenant_gateway_id);
    bool is_stop = false;

    for (GatewayConstIter iter = lower_bound;
         OB_SUCC(ret) && !is_stop && iter != gateway_infos_.end();
         iter++) {
      const ObAIGatewaySchema *schema = *iter;
      if (OB_ISNULL(schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected NULL schema", K(ret), K(schema), K(gateway_infos_));
      } else if (tenant_id != schema->get_tenant_id()) {
        is_stop = true;
      } else if (OB_FAIL(gateway_schemas.push_back(schema))) {
        LOG_WARN("failed to push_back", K(ret), KPC(schema));
      }
    }
  }
  return ret;
}

int ObAIGatewayMgr::rebuild_ai_gateway_hashmap()
{
  int ret = OB_SUCCESS;
  constexpr int overwrite = 1;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ai gateway mgr not init", K(ret));
  } else {
    gateway_id_map_.clear();
    gateway_name_map_.clear();

    for (GatewayIter iter = gateway_infos_.begin();
         OB_SUCC(ret) && iter != gateway_infos_.end();
         iter++) {
      ObAIGatewaySchema *schema = *iter;
      if (OB_ISNULL(schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected NULL schema", K(ret), K(gateway_infos_));
      } else {
        ObAIGatewayHashWrapper hash_wrapper(schema->get_tenant_id(),
                                             schema->get_name(),
                                             schema->get_case_mode());

        if (OB_FAIL(gateway_id_map_.set_refactored(schema->get_gateway_id(), schema, overwrite))) {
          LOG_WARN("failed to set_refactored to gateway_id_map_", K(ret), KPC(schema));
        } else if (OB_FAIL(gateway_name_map_.set_refactored(hash_wrapper, schema, overwrite))) {
          LOG_WARN("failed to set_refactored to gateway_name_map_", K(ret), KPC(schema));
        }
      }
    }
  }

  return ret;
}

} // namespace schema
} // namespace share
} // namespace oceanbase
