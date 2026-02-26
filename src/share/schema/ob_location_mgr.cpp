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

#include "share/schema/ob_location_mgr.h"
#include "share/schema/ob_schema_utils.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
const char *ObLocationMgr::LOCATION_MGR = "LOCATION_MGR";

ObLocationMgr::ObLocationMgr()
  : is_inited_(false),
    local_allocator_(SET_USE_500(ObModIds::OB_SCHEMA_GETTER_GUARD, ObCtxIds::SCHEMA_SERVICE)),
    allocator_(local_allocator_),
    location_infos_(0, NULL, SET_USE_500(LOCATION_MGR, ObCtxIds::SCHEMA_SERVICE)),
    location_name_map_(SET_USE_500(LOCATION_MGR, ObCtxIds::SCHEMA_SERVICE)),
    location_id_map_(SET_USE_500(LOCATION_MGR, ObCtxIds::SCHEMA_SERVICE))
{
}

ObLocationMgr::ObLocationMgr(common::ObIAllocator &allocator)
  : is_inited_(false),
    local_allocator_(SET_USE_500(ObModIds::OB_SCHEMA_GETTER_GUARD, ObCtxIds::SCHEMA_SERVICE)),
    allocator_(allocator),
    location_infos_(0, NULL, SET_USE_500(LOCATION_MGR, ObCtxIds::SCHEMA_SERVICE)),
    location_name_map_(SET_USE_500(LOCATION_MGR, ObCtxIds::SCHEMA_SERVICE)),
    location_id_map_(SET_USE_500(LOCATION_MGR, ObCtxIds::SCHEMA_SERVICE))
{
}

ObLocationMgr::~ObLocationMgr()
{
}

ObLocationMgr &ObLocationMgr::operator=(const ObLocationMgr &other)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("location manager not init", K(ret));
  } else if (OB_FAIL(assign(other))) {
    LOG_WARN("assign failed", K(ret));
  }
  return *this;
}

int ObLocationMgr::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init private location schema manager twice", K(ret));
  } else if (OB_FAIL(location_name_map_.init())) {
    LOG_WARN("init location name map failed", K(ret));
  } else if (OB_FAIL(location_id_map_.init())) {
    LOG_WARN("init location id map failed", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObLocationMgr::reset()
{
  if (IS_NOT_INIT) {
    LOG_WARN_RET(OB_NOT_INIT, "location manger not init");
  } else {
    location_infos_.clear();
    location_name_map_.clear();
    location_id_map_.clear();
  }
}

int ObLocationMgr::assign(const ObLocationMgr &other)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("location manager not init", K(ret));
  } else if (this != &other) {
    if (OB_FAIL(location_name_map_.assign(other.location_name_map_))) {
      LOG_WARN("assign location name map failed", K(ret));
    } else if (OB_FAIL(location_id_map_.assign(other.location_id_map_))) {
      LOG_WARN("assign location id map failed", K(ret));
    } else if (OB_FAIL(location_infos_.assign(other.location_infos_))) {
      LOG_WARN("assign location schema vector failed", K(ret));
    }
  }
  return ret;
}

int ObLocationMgr::deep_copy(const ObLocationMgr &other)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("location manager not init", K(ret));
  } else if (this != &other) {
    reset();
    ObLocationSchema *schema = NULL;
    for (LocationIter iter = other.location_infos_.begin();
        OB_SUCC(ret) && iter != other.location_infos_.end(); iter++) {
      if (OB_ISNULL(schema = *iter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", KP(schema), K(ret));
      } else if (OB_FAIL(add_location(*schema, schema->get_name_case_mode()))) {
        LOG_WARN("add location failed", K(*schema), K(ret));
      }
    }
  }
  return ret;
}

int ObLocationMgr::add_location(const ObLocationSchema &schema, const ObNameCaseMode mode)
{
  int ret = OB_SUCCESS;
  ObLocationSchema *new_schema = NULL;
  ObLocationSchema *old_schema = NULL;
  LocationIter iter = NULL;
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
  } else if (OB_FAIL(location_infos_.replace(new_schema,
                                              iter,
                                              schema_compare,
                                              schema_equal,
                                              old_schema))) {
    LOG_WARN("failed to add location schema", K(ret));
  } else {
    int over_write = 1;
    ObLocationNameHashKey hash_wrapper(new_schema->get_tenant_id(),
                                       new_schema->get_name_case_mode(),
                                       new_schema->get_location_name_str());
    if (OB_FAIL(location_name_map_.set_refactored(hash_wrapper, new_schema, over_write))) {
      LOG_WARN("build location hash map failed", K(ret));
    } else if (OB_FAIL(location_id_map_.set_refactored(new_schema->get_location_id(), new_schema, over_write))) {
      LOG_WARN("build location id hashmap failed", K(ret),
              "location_id", new_schema->get_location_id());
    }
  }
  if ((location_infos_.count() != location_name_map_.item_count()
      || location_infos_.count() != location_id_map_.item_count())) {
    LOG_WARN("schema is inconsistent with its map", K(ret),
            K(location_infos_.count()),
            K(location_name_map_.item_count()),
            K(location_id_map_.item_count()));
    int rebuild_ret = OB_SUCCESS;
    if (OB_SUCCESS != (rebuild_ret = rebuild_location_hashmap())) {
      LOG_WARN("rebuild location hashmap failed", K(rebuild_ret));
    }
    if (OB_SUCC(ret)) {
      ret = rebuild_ret;
    }
  }
  LOG_DEBUG("add location", K(schema), K(location_infos_.count()),
            K(location_name_map_.item_count()), K(location_id_map_.item_count()));
  return ret;
}

int ObLocationMgr::del_location(const ObTenantLocationId &id)
{
  int ret = OB_SUCCESS;
  ObLocationSchema *schema = NULL;
  if (OB_UNLIKELY(!id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(id));
  } else if (OB_FAIL(location_infos_.remove_if(id,
                                                compare_with_tenant_location_id,
                                                equal_to_tenant_location_id,
                                                schema))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      // if item does not exist, regard it as succeeded, schema will be refreshed later
      ret = OB_SUCCESS;
      LOG_INFO("failed to remove location schema, item may not exist", K(ret));
    } else {
      LOG_WARN("failed to remove location schema", K(ret));
    }
  } else if (OB_ISNULL(schema)) {
    // if item can be found, schema should not be null
    // defense code, should not happed
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("removed location schema return NULL, ",
            "tenant_id", id.tenant_id_,
            "location_id", id.schema_id_,
            K(ret));
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(schema)) {
    if (OB_FAIL(location_id_map_.erase_refactored(schema->get_location_id()))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        LOG_INFO("item does not exist, will igore it", K(ret),
            "location id", schema->get_location_id());
      } else {
        LOG_WARN("failed to delete location from hashmap", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(schema)) {
    if (OB_FAIL(location_name_map_.erase_refactored(
        ObLocationNameHashKey(schema->get_tenant_id(), schema->get_name_case_mode(), schema->get_location_name_str())))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        LOG_INFO("item does not exist, will igore it", K(ret),
            "location name", schema->get_location_name_str());
      } else {
        LOG_WARN("failed to delete location from hashmap", K(ret));
      }
    }
  }

  if (location_infos_.count() != location_name_map_.item_count()
      || location_infos_.count() != location_id_map_.item_count()) {
    LOG_WARN("location schema is non-consistent",K(id), K(location_infos_.count()),
        K(location_name_map_.item_count()), K(location_id_map_.item_count()));
    int rebuild_ret = OB_SUCCESS;
    if (OB_SUCCESS != (rebuild_ret = rebuild_location_hashmap())) {
      LOG_WARN("rebuild location hashmap failed", K(rebuild_ret));
    }
    if (OB_SUCC(ret)) {
      ret = rebuild_ret;
    }
  }
  LOG_DEBUG("location del", K(id), K(location_infos_.count()),
      K(location_name_map_.item_count()), K(location_id_map_.item_count()));
  return ret;
}

int ObLocationMgr::get_location_schema_by_id(const uint64_t location_id,
                                              const ObLocationSchema *&schema) const
{
  int ret = OB_SUCCESS;
  schema = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == location_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(location_id));
  } else {
    ObLocationSchema *tmp_schema = NULL;
    int hash_ret = location_id_map_.get_refactored(location_id, tmp_schema);
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

int ObLocationMgr::get_location_schema_by_name(const uint64_t tenant_id,
                                               const ObNameCaseMode mode,
                                               const common::ObString &name,
                                               const ObLocationSchema *&schema) const
{
  int ret = OB_SUCCESS;
  schema = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id) || OB_UNLIKELY(name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(name));
  } else {
    ObLocationSchema *tmp_schema = NULL;
    ObLocationNameHashKey hash_wrapper(tenant_id, mode, name);
    if (OB_FAIL(location_name_map_.get_refactored(hash_wrapper, tmp_schema))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        LOG_INFO("schema is not exist", K(tenant_id), K(name),
                "map_cnt", location_name_map_.item_count());
      }
    } else {
      schema = tmp_schema;
    }
  }
  return ret;
}

int ObLocationMgr::get_location_schema_by_prefix_match(const uint64_t tenant_id,
                                                       const common::ObString &access_path,
                                                       ObArray<const ObLocationSchema*> &match_schemas) const
{
  // todo 后面使用前缀树优化
  int ret = OB_SUCCESS;
  match_schemas.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < location_infos_.count(); i++) {
    const ObLocationSchema *tmp_schema = location_infos_.at(i);
    if (OB_ISNULL(tmp_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null schema", K(ret));
    } else if (tenant_id == tmp_schema->get_tenant_id()
               && access_path.prefix_match(tmp_schema->get_location_url_str())) {
      match_schemas.push_back(tmp_schema);
    }
  }
  return ret;
}

int ObLocationMgr::get_location_schemas_in_tenant(const uint64_t tenant_id,
                                                    common::ObIArray<const ObLocationSchema *> &schemas) const
{
  int ret = OB_SUCCESS;
  schemas.reset();
  ObTenantLocationId id(tenant_id, OB_MIN_ID);
  ConstLocationIter iter_begin =
      location_infos_.lower_bound(id, compare_with_tenant_location_id);
  bool is_stop = false;
  for (ConstLocationIter iter = iter_begin;
      OB_SUCC(ret) && iter != location_infos_.end() && !is_stop; ++iter) {
    const ObLocationSchema *schema = NULL;
    if (OB_ISNULL(schema = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret), K(schema));
    } else if (tenant_id != schema->get_tenant_id()) {
      is_stop = true;
    } else if (OB_FAIL(schemas.push_back(schema))) {
      LOG_WARN("push back location failed", K(ret));
    }
  }
  return ret;
}

int ObLocationMgr::del_location_schemas_in_tenant(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else {
    ObArray<const ObLocationSchema *> schemas;
    if (OB_FAIL(get_location_schemas_in_tenant(tenant_id, schemas))) {
      LOG_WARN("get location schemas failed", K(ret), K(tenant_id));
    } else {
      FOREACH_CNT_X(schema, schemas, OB_SUCC(ret)) {
        ObTenantLocationId id(tenant_id, (*schema)->get_location_id());
        if (OB_FAIL(del_location(id))) {
          LOG_WARN("del location failed",
                  "tenant_id",
                  id.tenant_id_,
                  "location_id",
                  id.schema_id_,
                  K(ret));
        }
      }
    }
  }
  return ret;
}

int ObLocationMgr::get_location_schema_count(int64_t &schema_count) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    schema_count = location_infos_.count();
  }
  return ret;
}

int ObLocationMgr::get_schema_statistics(ObSchemaStatisticsInfo &schema_info) const
{
  int ret = OB_SUCCESS;
  schema_info.reset();
  schema_info.schema_type_ = LOCATION_SCHEMA;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    schema_info.count_ = location_infos_.size();
    for (ConstLocationIter iter = location_infos_.begin();
        OB_SUCC(ret) && iter != location_infos_.end(); ++iter) {
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

int ObLocationMgr::rebuild_location_hashmap()
{
  int ret = OB_SUCCESS;
  int over_write = 1;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    location_name_map_.clear();
    location_id_map_.clear();
  }

  for (ConstLocationIter iter = location_infos_.begin();
      OB_SUCC(ret) && iter != location_infos_.end(); ++iter) {
    ObLocationSchema *location_schema = *iter;
    ObLocationNameHashKey hash_wrapper;
    if (OB_ISNULL(location_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("location_schema is NULL", K(ret), K(location_schema));
    } else if (FALSE_IT(hash_wrapper.set_tenant_id(location_schema->get_tenant_id()))) {
      // do nothing
    } else if (FALSE_IT(hash_wrapper.set_location_name(location_schema->get_location_name()))) {
      // do nothing
    } else if (OB_FAIL(location_name_map_.set_refactored(hash_wrapper,
        location_schema, over_write))) {
      LOG_WARN("build location name hashmap failed", K(ret),
          "location_name", location_schema->get_location_name());
    } else if (OB_FAIL(location_id_map_.set_refactored(location_schema->get_location_id(),
        location_schema, over_write))) {
      LOG_WARN("build location id hashmap failed", K(ret),
          "location_id", location_schema->get_location_id());
    }
  }
  return ret;
}
} // namespace schema
} // namespace share
} // namespace oceanbase
