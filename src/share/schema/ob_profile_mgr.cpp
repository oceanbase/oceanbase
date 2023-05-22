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

#include "share/schema/ob_profile_mgr.h"
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

ObProfileMgr::ObProfileMgr()
    : is_inited_(false),
      local_allocator_(SET_USE_500(ObModIds::OB_SCHEMA_GETTER_GUARD, ObCtxIds::SCHEMA_SERVICE)),
      allocator_(local_allocator_),
      schema_infos_(0, NULL, SET_USE_500(ObModIds::OB_SCHEMA_PROFILE, ObCtxIds::SCHEMA_SERVICE)),
      name_map_(SET_USE_500(ObModIds::OB_SCHEMA_PROFILE, ObCtxIds::SCHEMA_SERVICE)),
      id_map_(SET_USE_500(ObModIds::OB_SCHEMA_PROFILE, ObCtxIds::SCHEMA_SERVICE))
{
}
ObProfileMgr::ObProfileMgr(ObIAllocator &allocator)
    : is_inited_(false),
      local_allocator_(SET_USE_500(ObModIds::OB_SCHEMA_GETTER_GUARD, ObCtxIds::SCHEMA_SERVICE)),
      allocator_(allocator),
      schema_infos_(0, NULL, SET_USE_500(ObModIds::OB_SCHEMA_PROFILE, ObCtxIds::SCHEMA_SERVICE)),
      name_map_(SET_USE_500(ObModIds::OB_SCHEMA_PROFILE, ObCtxIds::SCHEMA_SERVICE)),
      id_map_(SET_USE_500(ObModIds::OB_SCHEMA_PROFILE, ObCtxIds::SCHEMA_SERVICE))
{
}
ObProfileMgr::~ObProfileMgr()
{
}
int ObProfileMgr::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init private profile schema manager twice", K(ret));
  } else if (OB_FAIL(name_map_.init())) {
    LOG_WARN("init hash map failed", K(ret));
  } else if (OB_FAIL(id_map_.init())) {
    LOG_WARN("init hash map failed", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}
void ObProfileMgr::reset()
{
  if (!is_inited_) {
    LOG_WARN_RET(OB_NOT_INIT, "profile manger not init");
  } else {
    schema_infos_.clear();
    name_map_.clear();
    id_map_.clear();
  }
}
ObProfileMgr &ObProfileMgr::operator =(const ObProfileMgr &other)
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
int ObProfileMgr::assign(const ObProfileMgr &other)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema manager not init", K(ret));
  } else if (this != &other) {
    if (OB_FAIL(name_map_.assign(other.name_map_))) {
      LOG_WARN("assign profile name map failed", K(ret));
    } else if (OB_FAIL(id_map_.assign(other.id_map_))) {
      LOG_WARN("assign profile id map failed", K(ret));
    } else if (OB_FAIL(schema_infos_.assign(other.schema_infos_))) {
      LOG_WARN("assign profile schema vector failed", K(ret));
    }
  }
  return ret;
}
int ObProfileMgr::deep_copy(const ObProfileMgr &other)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema manager not init", K(ret));
  } else if (this != &other) {
    reset();
    ObProfileSchema *schema = NULL;
    for (ProfileIter iter = other.schema_infos_.begin();
         OB_SUCC(ret) && iter != other.schema_infos_.end(); iter++) {
      if (OB_ISNULL(schema = *iter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", KP(schema), K(ret));
      } else if (OB_FAIL(add_profile(*schema))) {
        LOG_WARN("add outline failed", K(*schema), K(ret));
      }
    }
  }
  return ret;
}
int ObProfileMgr::add_profile(const ObProfileSchema &schema)
{
  int ret = OB_SUCCESS;
  ObProfileSchema *new_schema = NULL;
  ObProfileSchema *old_schema = NULL;
  ProfileIter iter = NULL;
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
    LOG_WARN("failed to add profile schema", K(ret));
  } else {
    int over_write = 1;
    ObProfileNameHashKey profile_name_hash_key(new_schema->get_tenant_id(), new_schema->get_profile_name_str());
    if (OB_FAIL(name_map_.set_refactored(profile_name_hash_key, new_schema, over_write))) {
      LOG_WARN("build profile hash map failed", K(ret));
    } else if (OB_FAIL(id_map_.set_refactored(new_schema->get_profile_id(), new_schema, over_write))) {
      LOG_WARN("build profile id hashmap failed", K(ret),
               "profile_id", new_schema->get_profile_id());
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
  LOG_DEBUG("profile add", K(schema), K(schema_infos_.count()),
            K(name_map_.item_count()), K(id_map_.item_count()));
  return ret;
}

int ObProfileMgr::add_profiles(const ObIArray<ObProfileSchema> &schemas)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < schemas.count() && OB_SUCC(ret); ++i) {
    if (OB_FAIL(add_profile(schemas.at(i)))) {
      LOG_WARN("push schema failed", K(ret));
    }
  }
  return ret;
}

int ObProfileMgr::get_schema_by_name(const uint64_t tenant_id,
                                     const ObString &name,
                                     const ObProfileSchema *&schema) const
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
    ObProfileSchema *tmp_schema = NULL;
    ObProfileNameHashKey hash_wrap(tenant_id, name);
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

int ObProfileMgr::get_schema_version_by_id(uint64_t profile_id, int64_t &schema_version) const
{
  int ret = OB_SUCCESS;
  schema_version = OB_INVALID_VERSION;
  const ObProfileSchema *schema = NULL;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == profile_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(profile_id));
  } else if (OB_FAIL(get_schema_by_id(profile_id, schema))) {
    LOG_WARN("fail to get profile schema version by id", KR(ret), K(profile_id));
  } else if (OB_NOT_NULL(schema)) {
    schema_version = schema->get_schema_version();
  }
  return ret;
}
int ObProfileMgr::del_schemas_in_tenant(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else {
    ObArray<const ObProfileSchema *> schemas;
    if (OB_FAIL(get_schemas_in_tenant(tenant_id, schemas))) {
      LOG_WARN("get profile schemas failed", K(ret), K(tenant_id));
    } else {
      FOREACH_CNT_X(schema, schemas, OB_SUCC(ret)) {
        ObTenantProfileId id(tenant_id, (*schema)->get_profile_id());
        if (OB_FAIL(del_profile(id))) {
          LOG_WARN("del profile failed",
                   "tenant_id", id.tenant_id_,
                   "profile_id", id.schema_id_,
                   K(ret));
        }
      }
    }
  }
  return ret;
}
int ObProfileMgr::del_profile(const ObTenantProfileId &id)
{
  int ret = OB_SUCCESS;
  ObProfileSchema *schema = NULL;
  if (!id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(id));
  } else if (OB_FAIL(schema_infos_.remove_if(id,
                                             compare_with_tenant_profile_id,
                                             equal_to_tenant_profile_id,
                                             schema))) {
    LOG_WARN("failed to remove profile schema", K(ret));
  } else if (OB_ISNULL(schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("removed profile schema return NULL, ",
             "tenant_id",
             id.tenant_id_,
             "profile_id",
             id.schema_id_,
             K(ret));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(id_map_.erase_refactored(schema->get_profile_id()))) {
      LOG_WARN("failed delete profile from hashmap", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(name_map_.erase_refactored(
                  ObProfileNameHashKey(schema->get_tenant_id(), schema->get_profile_name_str())))) {
      LOG_WARN("failed delete profile from hashmap", K(ret));
    }
  }
  if (OB_SUCC(ret) && OB_UNLIKELY(schema_infos_.count() != name_map_.item_count()
                                  || schema_infos_.count() != id_map_.item_count())) {
    LOG_WARN("profile schema is non-consistent",K(id), K(schema_infos_.count()),
             K(name_map_.item_count()), K(id_map_.item_count()));
  }
  LOG_DEBUG("profile del", K(id), K(schema_infos_.count()),
            K(name_map_.item_count()), K(id_map_.item_count()));
  return ret;
}

bool ObProfileMgr::compare_with_tenant_profile_id(const ObProfileSchema *lhs,
                                                  const ObTenantProfileId &id)
{
  return NULL != lhs ? (lhs->get_tenant_profile_id() < id) : false;
}

bool ObProfileMgr::equal_to_tenant_profile_id(const ObProfileSchema *lhs,
                                              const ObTenantProfileId &id)
{
  return NULL != lhs ? (lhs->get_tenant_profile_id() == id) : false;
}

int ObProfileMgr::get_schemas_in_tenant(const uint64_t tenant_id,
    ObIArray<const ObProfileSchema *> &schemas) const
{
  int ret = OB_SUCCESS;
  schemas.reset();
  ObTenantProfileId id(tenant_id, OB_MIN_ID);
  ConstProfileIter iter_begin =
      schema_infos_.lower_bound(id, compare_with_tenant_profile_id);
  bool is_stop = false;
  for (ConstProfileIter iter = iter_begin;
      OB_SUCC(ret) && iter != schema_infos_.end() && !is_stop; ++iter) {
    const ObProfileSchema *schema = NULL;
    if (OB_ISNULL(schema = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret), K(schema));
    } else if (tenant_id != schema->get_tenant_id()) {
      is_stop = true;
    } else if (OB_FAIL(schemas.push_back(schema))) {
      LOG_WARN("push back profile failed", K(ret));
    }
  }
  return ret;
}

int ObProfileMgr::get_schema_by_id(const uint64_t profile_id, const ObProfileSchema *&schema) const
{
  int ret = OB_SUCCESS;
  schema = NULL;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == profile_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(profile_id));
  } else {
    ObProfileSchema *tmp_schema = NULL;
    int hash_ret = id_map_.get_refactored(profile_id, tmp_schema);
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

int ObProfileMgr::get_schema_count(int64_t &schema_count) const
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

int ObProfileMgr::get_schema_statistics(ObSchemaStatisticsInfo &schema_info) const
{
  int ret = OB_SUCCESS;
  schema_info.reset();
  schema_info.schema_type_ = PROFILE_SCHEMA;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    schema_info.count_ = schema_infos_.size();
    for (ConstProfileIter it = schema_infos_.begin(); OB_SUCC(ret) && it != schema_infos_.end(); it++) {
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
