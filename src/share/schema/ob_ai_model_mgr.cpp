/**
 * Copyright (c) 2025 OceanBase
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

#include "ob_ai_model_mgr.h"
#include "share/schema/ob_schema_utils.h"

namespace oceanbase
{

namespace share
{

namespace schema
{

OB_SERIALIZE_MEMBER(ObAiModelSchema,
                    tenant_id_,
                    model_id_,
                    name_,
                    type_,
                    model_name_,
                    schema_version_,
                    case_mode_);

int64_t ObAiModelSchema::get_convert_size() const
{
  int64_t convert_size = sizeof(ObAiModelSchema);

  convert_size += name_.length() + 1;
  convert_size += model_name_.length() + 1;

  return convert_size;
}

int ObAiModelSchema::assign(const ObAiModelSchema &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    reset();
    if (OB_FAIL(set_name(other.name_))) {
      LOG_WARN("failed to set name", K(ret), K(*this), K(other));
    } else if (OB_FAIL(set_model_name(other.model_name_))) {
      LOG_WARN("failed to set model name", K(ret), K(*this), K(other));
    } else {
      set_tenant_id(other.tenant_id_);
      set_model_id(other.model_id_);
      set_type(other.type_);
      set_schema_version(other.schema_version_);
    }
  }
  return ret;
}

int ObAiModelSchema::assign(const uint64_t tenant_id, const ObAiServiceModelInfo &model_info)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_FAIL(set_name(model_info.get_name()))) {
    LOG_WARN("failed to set name", K(ret), K(model_info));
  } else if (OB_FAIL(set_model_name(model_info.get_model_name()))) {
    LOG_WARN("failed to set model name", K(ret), K(model_info));
  } else {
    set_tenant_id(tenant_id);
    set_model_id(OB_INVALID_ID);
    set_schema_version(OB_INVALID_VERSION);
    set_type(model_info.get_type());
  }
  return ret;
}


ObAiModelMgr::ObAiModelMgr()
  : is_inited_(false),
    local_allocator_(SET_USE_500(ObModIds::OB_SCHEMA_GETTER_GUARD, ObCtxIds::SCHEMA_SERVICE)),
    allocator_(local_allocator_),
    ai_model_infos_(0, nullptr, SET_USE_500("SchAiModel", ObCtxIds::SCHEMA_SERVICE)),
    ai_model_id_map_(SET_USE_500("SchAiModel", ObCtxIds::SCHEMA_SERVICE)),
    ai_model_name_map_(SET_USE_500("SchAiModel", ObCtxIds::SCHEMA_SERVICE))
{
}

ObAiModelMgr::ObAiModelMgr(ObIAllocator &allocator)
  : is_inited_(false),
    local_allocator_(SET_USE_500(ObModIds::OB_SCHEMA_GETTER_GUARD, ObCtxIds::SCHEMA_SERVICE)),
    allocator_(allocator),
    ai_model_infos_(0, nullptr, SET_USE_500("SchAiModel", ObCtxIds::SCHEMA_SERVICE)),
    ai_model_id_map_(SET_USE_500("SchAiModel", ObCtxIds::SCHEMA_SERVICE)),
    ai_model_name_map_(SET_USE_500("SchAiModel", ObCtxIds::SCHEMA_SERVICE))
{
}

int ObAiModelMgr::init()
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ai model mgr init twice", K(ret), K(lbt()));
  } else if (OB_FAIL(ai_model_id_map_.init())) {
    LOG_WARN("failed to init ai_model_id_map_", K(ret));
  } else if (OB_FAIL(ai_model_name_map_.init())) {
    LOG_WARN("failed to init ai_model_name_map_", K(ret));
  } else {
    is_inited_ = true;
  }

  return ret;
}

void ObAiModelMgr::reset()
{
  if (!is_inited_) {
    LOG_WARN_RET(OB_NOT_INIT, "ai model mgr not init");
  } else {
    ai_model_infos_.clear();
    ai_model_id_map_.clear();
    ai_model_name_map_.clear();
  }
}

int ObAiModelMgr::assign(const ObAiModelMgr &other)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ai model mgr not init", K(ret));
  } else if (this != &other) {
    if (OB_FAIL(ai_model_infos_.assign(other.ai_model_infos_))) {
      LOG_WARN("assign ai model infos failed", K(ret));
    } else if (OB_FAIL(ai_model_id_map_.assign(other.ai_model_id_map_))) {
      LOG_WARN("assign ai model id map failed", K(ret));
    } else if (OB_FAIL(ai_model_name_map_.assign(other.ai_model_name_map_))) {
      LOG_WARN("assign ai model name map failed", K(ret));
    }
  }

  return ret;
}

int ObAiModelMgr::deep_copy(const ObAiModelMgr &other)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ai model mgr not init", K(ret));
  } else if (this != &other) {
    reset();
    for (AiModelIter iter = other.ai_model_infos_.begin();
         OB_SUCC(ret) && iter != other.ai_model_infos_.end();
         iter++) {
      ObAiModelSchema *ai_model_schema = *iter;
      if (OB_ISNULL(ai_model_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected NULL ai_model_schema", K(ai_model_schema), K(ret));
      } else if (OB_FAIL(add_ai_model(*ai_model_schema, ai_model_schema->get_case_mode()))) {
        LOG_WARN("failed to add_ai_model_schema", K(*ai_model_schema), K(ret));
      }
    }
  }

  return ret;
}

int ObAiModelMgr::get_ai_model_schema_count(int64_t &schema_count) const
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    LOG_WARN("ai model mgr not init");
  } else {
    schema_count = ai_model_infos_.size();
  }

  return ret;
}

int ObAiModelMgr::get_schema_statistics(ObSchemaStatisticsInfo &schema_info) const
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ai model mgr not init", K(ret));
  } else {
    schema_info.reset();
    schema_info.schema_type_ = AI_MODEL_SCHEMA;
    schema_info.count_ = ai_model_infos_.size();
    for (AiModelConstIter it = ai_model_infos_.begin();
         OB_SUCC(ret) && it != ai_model_infos_.end();
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

int ObAiModelMgr::get_ai_model_schema(const uint64_t ai_model_id, const ObAiModelSchema *&ai_model_schema) const
{
  int ret = OB_SUCCESS;
  ObAiModelSchema *tmp_schema = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ai model mgr not init", K(ret));
  } else if (OB_INVALID_ID == ai_model_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ai_model_id", K(ret), K(ai_model_id));
  } else if (OB_FAIL(ai_model_id_map_.get_refactored(ai_model_id, tmp_schema))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get ai_model_schema", K(ret), K(ai_model_id));
    }
  } else if (OB_ISNULL(tmp_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL ai_model_schema", K(ret), K(ai_model_id));
  } else {
    ai_model_schema = tmp_schema;
  }

  return ret;
}

int ObAiModelMgr::get_ai_model_schema(const uint64_t tenant_id, const common::ObString &name,
                                      const common::ObNameCaseMode case_mode,
                                      const ObAiModelSchema *&ai_model_schema) const
{
  int ret = OB_SUCCESS;
  ObAiModelSchema *tmp_schema = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ai model mgr not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id || name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(name));
  }  else {
    ObAiModelHashWrapper hash_wrapper(tenant_id, name, case_mode);

    if (OB_FAIL(ai_model_name_map_.get_refactored(hash_wrapper, tmp_schema))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to get ai_model_schema", K(ret), K(name));
      }
    } else if (OB_ISNULL(tmp_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL ai_model_schema", K(ret), K(name));
    } else {
      ai_model_schema = tmp_schema;
    }
  }

  return ret;
}

int ObAiModelMgr::del_schemas_in_tenant(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ai model mgr not init", K(ret));
  } else if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(ret), K(tenant_id));
  } else {
    ObSEArray<const ObAiModelSchema *, 32> schemas;

    if (OB_FAIL(get_ai_model_schemas_in_tenant(tenant_id, schemas))) {
      LOG_WARN("failed to get_ai_model_schemas_in_tenant", K(ret), K(tenant_id));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < schemas.count(); ++i) {
        const ObAiModelSchema *curr = schemas.at(i);

        if (OB_ISNULL(curr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected NULL schema", K(ret), K(i), K(schemas));
        } else {
          if (OB_FAIL(del_ai_model(ObTenantAiModelId(tenant_id, curr->get_model_id())))) {
            LOG_WARN("failed to del_ai_model", K(ret), K(tenant_id), K(curr->get_model_id()));
          }
        }
      }
    }
  }
  return ret;
}

int ObAiModelMgr::add_ai_model(const ObAiModelSchema &ai_model_schema, common::ObNameCaseMode case_mode)
{
  int ret = OB_SUCCESS;

  constexpr int overwrite = 1;

  ObAiModelSchema *new_schema = nullptr;
  ObAiModelSchema *replaced_schema = nullptr;
  AiModelIter iter = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ai model mgr not init", K(ret));
  } else if (OB_UNLIKELY(!ai_model_schema.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ai_model_schema", K(ret), K(ai_model_schema));
  } else if (OB_FAIL(ObSchemaUtils::alloc_schema(allocator_, ai_model_schema, new_schema))) {
    LOG_WARN("failed to alloc_schema", K(ret), K(ai_model_schema));
  } else if (OB_ISNULL(new_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL new_schema", K(ret), K(ai_model_schema));
  } else if (OB_FALSE_IT(new_schema->set_case_mode(case_mode))) {
  } else if (OB_FAIL(ai_model_infos_.replace(new_schema,
                                             iter,
                                             compare_ai_model,
                                             equal_ai_model,
                                             replaced_schema))) {
    LOG_WARN("failed to add ai model schema", K(ret), KPC(new_schema));
  } else {
    ObAiModelHashWrapper hash_wrapper(new_schema->get_tenant_id(),
                                      new_schema->get_name(),
                                      new_schema->get_case_mode());

    if (OB_FAIL(ai_model_id_map_.set_refactored(new_schema->get_model_id(), new_schema, overwrite))) {
      LOG_WARN("failed to set_refactored to ai_model_id_map_", K(ret), KPC(new_schema));
    } else if (OB_FAIL(ai_model_name_map_.set_refactored(hash_wrapper, new_schema, overwrite))) {
      LOG_WARN("failed to set_refactored to ai_model_name_map_", K(ret), KPC(new_schema));
    }
  }

  // always check, it may become inconsistent in some specific scenarios(e.g., error code -4013),
  // if not equal, rebuild the hashmap
  if (ai_model_infos_.count() != ai_model_id_map_.item_count()
      || ai_model_infos_.count() != ai_model_name_map_.item_count()) {
    LOG_WARN("ai model schema is inconsistent",
             K(ret),
             K(ai_model_infos_.count()),
             K(ai_model_id_map_.item_count()),
             K(ai_model_name_map_.item_count()),
             K(ai_model_infos_));
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = rebuild_ai_model_hashmap())) {
      LOG_WARN("rebuild ai model hashmap failed", K(ret), K(tmp_ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }
  return ret;
}

int ObAiModelMgr::del_ai_model(const ObTenantAiModelId &tenant_ai_model_id)
{
  int ret = OB_SUCCESS;
  ObAiModelSchema *schema = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ai model mgr not init", K(ret));
  } else if (!tenant_ai_model_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ai_model_id", K(ret), K(tenant_ai_model_id));
  } else if (OB_FAIL(ai_model_infos_.remove_if(tenant_ai_model_id,
                                               compare_with_tenant_ai_model_id,
                                               equal_to_tenant_ai_model_id,
                                               schema))) {
    LOG_WARN("failed to remove ai_model_schema", K(ret), K(tenant_ai_model_id));
  } else if (OB_ISNULL(schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL ai_model_schema", K(ret), K(tenant_ai_model_id));
  } else {
    int hash_ret = OB_SUCCESS;
    ObAiModelHashWrapper hash_wrapper(schema->get_tenant_id(),
                                      schema->get_name(),
                                      schema->get_case_mode());

    if (OB_SUCCESS != (hash_ret = ai_model_id_map_.erase_refactored(schema->get_model_id()))) {
      LOG_WARN("failed erase_refactored from id hashmap",
               K(ret),
               K(hash_ret),
               K(schema->get_model_id()));
      ret = OB_HASH_NOT_EXIST != hash_ret ? hash_ret : ret;
    } else if (OB_SUCCESS != (hash_ret = ai_model_name_map_.erase_refactored(hash_wrapper))) {
      LOG_WARN("failed erase_refactored from name hashmap",
               K(ret),
               K(hash_ret),
               K(hash_wrapper));
      ret = OB_HASH_NOT_EXIST != hash_ret ? hash_ret : ret;
    }
  }

  // always check
  if (ai_model_infos_.count() != ai_model_id_map_.item_count()
      || ai_model_infos_.count() != ai_model_name_map_.item_count()) {
    LOG_WARN("ai model schema is inconsistent",
             K(ret),
             K(ai_model_infos_.count()),
             K(ai_model_id_map_.item_count()),
             K(ai_model_name_map_.item_count()),
             K(ai_model_infos_));
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = rebuild_ai_model_hashmap())) {
      LOG_WARN("rebuild ai model hashmap failed", K(ret), K(tmp_ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }

  return ret;
}

int ObAiModelMgr::get_ai_model_schemas_in_tenant(const uint64_t tenant_id, common::ObIArray<const ObAiModelSchema *> &ai_model_schemas) const
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ai model mgr not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(ret), K(tenant_id));
  } else {
    ai_model_schemas.reuse();

    ObTenantAiModelId tenant_id_lower(tenant_id, OB_MIN_ID);
    AiModelConstIter lower_bound = ai_model_infos_.lower_bound(tenant_id_lower, compare_with_tenant_ai_model_id);
    bool is_stop = false;

    for (AiModelConstIter iter = lower_bound;
         OB_SUCC(ret) && !is_stop && iter != ai_model_infos_.end();
         iter++) {
      const ObAiModelSchema *schema = *iter;
      if (OB_ISNULL(schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected NULL schema", K(ret), K(schema), K(ai_model_infos_));
      } else if (tenant_id != schema->get_tenant_id()) {
        is_stop = true;
      } else if (OB_FAIL(ai_model_schemas.push_back(schema))) {
        LOG_WARN("failed to push_back", K(ret), KPC(schema));
      }
    }
  }
  return ret;
}

int ObAiModelMgr::rebuild_ai_model_hashmap()
{
  int ret = OB_SUCCESS;
  constexpr int overwrite = 1;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ai model mgr not init", K(ret));
  } else {
    ai_model_id_map_.clear();
    ai_model_name_map_.clear();

    for (AiModelIter iter = ai_model_infos_.begin();
         OB_SUCC(ret) && iter != ai_model_infos_.end();
         iter++) {
      ObAiModelSchema *schema = *iter;
      if (OB_ISNULL(schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected NULL schema", K(ret), K(ai_model_infos_));
      } else {
        ObAiModelHashWrapper hash_wrapper(schema->get_tenant_id(),
                                          schema->get_name(),
                                          schema->get_case_mode());

        if (OB_FAIL(ai_model_id_map_.set_refactored(schema->get_model_id(), schema, overwrite))) {
          LOG_WARN("failed to set_refactored to ai_model_id_map_", K(ret), KPC(schema));
        } else if (OB_FAIL(ai_model_name_map_.set_refactored(hash_wrapper, schema, overwrite))) {
          LOG_WARN("failed to set_refactored to ai_model_name_map_", K(ret), KPC(schema));
        }
      }
    }
  }


  return ret;
}

} // namespace schema
} // namespace share
} // namespace oceanbase
