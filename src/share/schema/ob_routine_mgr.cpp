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
#include "ob_routine_mgr.h"
#include "lib/allocator/ob_mod_define.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "share/schema/ob_schema_utils.h"
namespace oceanbase
{
using namespace std;
using namespace common;
using namespace common::hash;
namespace share
{
namespace schema
{
ObSimpleRoutineSchema::ObSimpleRoutineSchema()
  : ObSchema()
{
  reset();
}

ObSimpleRoutineSchema::ObSimpleRoutineSchema(ObIAllocator *allocator)
  : ObSchema(allocator)
{
  reset();
}

ObSimpleRoutineSchema::~ObSimpleRoutineSchema()
{
}

int ObSimpleRoutineSchema::assign(const ObSimpleRoutineSchema &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    reset();
    error_ret_ = other.error_ret_;
    tenant_id_ = other.tenant_id_;
    database_id_ = other.database_id_;
    package_id_ = other.package_id_;
    routine_id_ = other.routine_id_;
    overload_ = other.overload_;
    schema_version_ = other.schema_version_;
    routine_type_ = other.routine_type_;
    if (OB_FAIL(deep_copy_str(other.routine_name_, routine_name_))) {
      LOG_WARN("Fail to deep copy routine name", K(ret));
    } else if (OB_FAIL(deep_copy_str(other.priv_user_, priv_user_))) {
      LOG_WARN("Fail to deep copy priv user name", K(ret));
    }
    if (OB_FAIL(ret)) {
      error_ret_ = ret;
    }
  }
  return ret;
}

bool ObSimpleRoutineSchema::operator ==(const ObSimpleRoutineSchema &other) const
{
  bool ret = false;
  if (tenant_id_ == other.tenant_id_
      && database_id_ == other.database_id_
      && package_id_ == other.package_id_
      && routine_id_ == other.routine_id_
      && schema_version_ == other.schema_version_
      && overload_ == other.overload_
      && routine_type_ == other.routine_type_
      && routine_name_ == other.routine_name_
      && priv_user_ == other.priv_user_) {
    ret = true;
  }

  return ret;
}

int64_t ObSimpleRoutineSchema::get_convert_size() const
{
  int64_t convert_size = 0;

  convert_size += sizeof(ObSimpleRoutineSchema);
  convert_size += routine_name_.length() + 1;
  convert_size += priv_user_.length() + 1;

  return convert_size;
}

ObRoutineMgr::ObRoutineMgr()
    : local_allocator_(SET_USE_500(ObModIds::OB_SCHEMA_GETTER_GUARD, ObCtxIds::SCHEMA_SERVICE)),
      allocator_(local_allocator_),
      routine_infos_(0, NULL, SET_USE_500(ObModIds::OB_SCHEMA_ROUTINE_INFO_VECTOR, ObCtxIds::SCHEMA_SERVICE)),
      routine_id_map_(SET_USE_500(ObModIds::OB_SCHEMA_ROUTINE_ID_MAP, ObCtxIds::SCHEMA_SERVICE)),
      routine_name_map_(SET_USE_500(ObModIds::OB_SCHEMA_ROUTINE_NAME_MAP, ObCtxIds::SCHEMA_SERVICE)),
      is_inited_(false)
{
}

ObRoutineMgr::ObRoutineMgr(ObIAllocator &allocator)
    : local_allocator_(SET_USE_500(ObModIds::OB_SCHEMA_GETTER_GUARD, ObCtxIds::SCHEMA_SERVICE)),
      allocator_(allocator),
      routine_infos_(0, NULL, SET_USE_500(ObModIds::OB_SCHEMA_ROUTINE_INFO_VECTOR, ObCtxIds::SCHEMA_SERVICE)),
      routine_id_map_(SET_USE_500(ObModIds::OB_SCHEMA_ROUTINE_ID_MAP, ObCtxIds::SCHEMA_SERVICE)),
      routine_name_map_(SET_USE_500(ObModIds::OB_SCHEMA_ROUTINE_NAME_MAP, ObCtxIds::SCHEMA_SERVICE)),
      is_inited_(false)
{
}

ObRoutineMgr::~ObRoutineMgr()
{
}

int ObRoutineMgr::init()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(routine_id_map_.init())) {
    LOG_WARN("init procedure id map failed", K(ret));
  } else if (OB_FAIL(routine_name_map_.init())) {
    LOG_WARN("init procedure name map failed", K(ret));
  } else {
    is_inited_ = true;
  }

  return ret;
}

void ObRoutineMgr::reset()
{
  int ret = OB_SUCCESS;

  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    // reset will not release memory for vector, use clear()
    routine_infos_.clear();
    routine_id_map_.clear();
    routine_name_map_.clear();
  }
}

ObRoutineMgr &ObRoutineMgr::operator =(const ObRoutineMgr &other)
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(assign(other))) {
    LOG_WARN("assign failed", K(ret));
  }

  return *this;
}

int ObRoutineMgr::assign(const ObRoutineMgr &other)
{
  int ret = OB_SUCCESS;

  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (this != &other) {
    reset();
    #define ASSIGN_FIELD(x)                        \
      if (OB_SUCC(ret)) {                          \
        if (OB_FAIL(x.assign(other.x))) {          \
          LOG_WARN("assign " #x "failed", K(ret)); \
        }                                          \
      }
    ASSIGN_FIELD(routine_infos_);
    ASSIGN_FIELD(routine_id_map_);
    ASSIGN_FIELD(routine_name_map_);
    #undef ASSIGN_FIELD
  }

  return ret;
}

int ObRoutineMgr::deep_copy(const ObRoutineMgr &other)
{
  int ret = OB_SUCCESS;

  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (this != &other) {
    reset();
    for (RoutineIter iter = other.routine_infos_.begin();
       OB_SUCC(ret) && iter != other.routine_infos_.end(); iter++) {
      ObSimpleRoutineSchema *routine = *iter;
      if (OB_ISNULL(routine)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(routine), K(ret));
      } else if (OB_FAIL(add_routine(*routine))) {
        LOG_WARN("add procedure failed", K(*routine), K(ret));
      }
    }
  }
  return ret;
}

bool ObRoutineMgr::check_inner_stat() const
{
  return is_inited_;
}

bool ObRoutineMgr::compare_routine(const ObSimpleRoutineSchema *lhs, const ObSimpleRoutineSchema *rhs)
{
  return lhs->get_routine_key() < rhs->get_routine_key();
}

bool ObRoutineMgr::equal_routine(const ObSimpleRoutineSchema *lhs, const ObSimpleRoutineSchema *rhs)
{
  return lhs->get_routine_key() == rhs->get_routine_key();
}

bool ObRoutineMgr::compare_with_tenant_routine_id(const ObSimpleRoutineSchema *lhs,
                                                 const ObTenantRoutineId &tenant_routine_id)
{
  return NULL != lhs ? (lhs->get_routine_key() < tenant_routine_id) : false;
}

bool ObRoutineMgr::equal_with_tenant_routine_id(const ObSimpleRoutineSchema *lhs,
                                                    const ObTenantRoutineId &tenant_routine_id)
{
  return NULL != lhs ? (lhs->get_routine_key() == tenant_routine_id) : false;
}

int ObRoutineMgr::add_routines(const ObIArray<ObSimpleRoutineSchema> &routine_schemas)
{
  int ret = OB_SUCCESS;

  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    FOREACH_CNT_X(routine_schema, routine_schemas, OB_SUCC(ret)) {
      if (OB_FAIL(add_routine(*routine_schema))) {
        LOG_WARN("add routine failed", K(ret), "routine_schema", *routine_schema);
      }
    }
  }

  return ret;
}

int ObRoutineMgr::del_routines(const ObIArray<ObTenantRoutineId> &tenant_routine_ids)
{
  int ret = OB_SUCCESS;

  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    FOREACH_CNT_X(tenant_routine_id, tenant_routine_ids, OB_SUCC(ret)) {
      if (OB_FAIL(del_routine(*tenant_routine_id))) {
        LOG_WARN("del routine failed", K(ret), K(*tenant_routine_id));
      }
    }
  }

  return ret;
}

int ObRoutineMgr::add_routine(const ObSimpleRoutineSchema &routine_schema)
{
  int ret = OB_SUCCESS;

  ObSimpleRoutineSchema *new_routine_schema = NULL;
  RoutineIter iter = NULL;
  ObSimpleRoutineSchema *replaced_routine = NULL;
  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!routine_schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(routine_schema));
  } else if (OB_FAIL(ObSchemaUtils::alloc_schema(allocator_,
                                                 routine_schema,
                                                 new_routine_schema))) {
    LOG_WARN("alloc schema failed", K(ret));
  } else if (OB_ISNULL(new_routine_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(ret), K(new_routine_schema));
  } else if (OB_FAIL(routine_infos_.replace(new_routine_schema,
                                            iter,
                                            compare_routine,
                                            equal_routine,
                                            replaced_routine))) {
    LOG_WARN("failed to add routine schema", K(ret));
  } else {
    int over_write = 1;
    int hash_ret = routine_id_map_.set_refactored(new_routine_schema->get_routine_id(),
                                                  new_routine_schema, over_write);
    if (OB_SUCCESS != hash_ret && OB_HASH_EXIST != hash_ret) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("build routine id hashmap failed", K(ret), K(hash_ret),
               "routine_id", new_routine_schema->get_routine_id());
    }
    if (OB_SUCC(ret)) {
      ObRoutineNameHashWrapper name_wrapper(new_routine_schema->get_tenant_id(),
                                            new_routine_schema->get_database_id(),
                                            new_routine_schema->get_package_id(),
                                            new_routine_schema->get_routine_name(),
                                            new_routine_schema->get_overload(),
                                            new_routine_schema->get_routine_type());
      hash_ret = routine_name_map_.set_refactored(name_wrapper, new_routine_schema, over_write);
      if (OB_SUCCESS != hash_ret && OB_HASH_EXIST != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("build routine name hashmap failed", K(ret), K(hash_ret),
                 "routine_id", new_routine_schema->get_routine_id(),
                 "routine_name", new_routine_schema->get_routine_name(),
                 "routine_type", new_routine_schema->get_routine_type());
      }
    }
  }
  // ignore ret
  if (routine_infos_.count() != routine_id_map_.item_count() ||
      routine_infos_.count() != routine_name_map_.item_count()) {
    LOG_WARN("routine info is non-consistent",
             "routine_infos_count", routine_infos_.count(),
             "routine_id_map_item_count", routine_id_map_.item_count(),
             "routine_name_map_item_count", routine_name_map_.item_count(),
             "routine_id", routine_schema.get_routine_id(),
             "routine_name", routine_schema.get_routine_name());
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = rebuild_routine_hashmap())) {
      LOG_WARN("rebuild routine hashmap failed", K(tmp_ret));
    }
  }
  return ret;
}

int ObRoutineMgr::check_user_reffered_by_definer(const ObString &user_name, bool &ref) const
{
  int ret = OB_SUCCESS;
  ref = false;
  for (ConstRoutineIter iter = routine_infos_.begin(); OB_SUCC(ret) && !ref && iter != routine_infos_.end(); iter++) {
    const ObSimpleRoutineSchema *routine = NULL;
    if (OB_ISNULL(routine = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret), K(routine));
    } else if (0 == user_name.compare(routine->get_priv_user())) {
      ref = true;
    }
  }
  return ret;
}

int ObRoutineMgr::del_routine(const ObTenantRoutineId &tenant_routine_id)
{
  int ret = OB_SUCCESS;

  ObSimpleRoutineSchema *schema_to_del = NULL;
  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!tenant_routine_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_routine_id));
  } else if (OB_FAIL(routine_infos_.remove_if(tenant_routine_id, compare_with_tenant_routine_id,
                                              equal_with_tenant_routine_id,
                                              schema_to_del))) {
    LOG_WARN("failed to remove routine schema, ", K(tenant_routine_id), K(ret));
  } else if (OB_ISNULL(schema_to_del)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("removed procedure schema return NULL, ", K(tenant_routine_id), K(ret));
  } else {
    int hash_ret = routine_id_map_.erase_refactored(schema_to_del->get_routine_id());
    if (OB_SUCCESS != hash_ret) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed delete routine from procedure id hashmap, ",
               "hash_ret", hash_ret, "routine_id", schema_to_del->get_routine_id());
    }
    if (OB_SUCC(ret)) {
      ObRoutineNameHashWrapper name_wrapper(schema_to_del->get_tenant_id(),
                                              schema_to_del->get_database_id(),
                                              schema_to_del->get_package_id(),
                                              schema_to_del->get_routine_name(),
                                              schema_to_del->get_overload(),
                                              schema_to_del->get_routine_type());
      hash_ret = routine_name_map_.erase_refactored(name_wrapper);
      if (OB_SUCCESS != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed delete procedure from procedure name hashmap, ",
                 K(ret), K(hash_ret),
                 "tenant_id", schema_to_del->get_tenant_id(),
                 "database_id", schema_to_del->get_database_id(),
                 "routine_name", schema_to_del->get_routine_name(),
                 "routine_type", schema_to_del->get_routine_type());
      }
    }
  }
  // ignore ret
  if (routine_infos_.count() != routine_id_map_.item_count() ||
      routine_infos_.count() != routine_name_map_.item_count()) {
    LOG_WARN("routine info is non-consistent",
             "routine_infos_count", routine_infos_.count(),
             "routine_id_map_item_count", routine_id_map_.item_count(),
             "routine_name_map_item_count", routine_name_map_.item_count(),
             "routine_id", tenant_routine_id.get_routine_id());
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = rebuild_routine_hashmap())){
      LOG_WARN("rebuild routine hashmap failed", K(tmp_ret));
    }
  }

  return ret;
}

int ObRoutineMgr::get_routine_schema(uint64_t routine_id, const ObSimpleRoutineSchema *&routine_schema) const
{
  int ret = OB_SUCCESS;
  routine_schema = NULL;

  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == routine_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(routine_id));
  } else {
    ObSimpleRoutineSchema *tmp_schema = NULL;
    int hash_ret = routine_id_map_.get_refactored(routine_id, tmp_schema);
    if (OB_SUCCESS == hash_ret) {
      if (OB_ISNULL(tmp_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(ret), K(tmp_schema));
      } else {
        routine_schema = tmp_schema;
      }
    }
  }

  return ret;
}

int ObRoutineMgr::get_routine_schema(
    uint64_t tenant_id, uint64_t database_id, uint64_t package_id,
    const common::ObString &routine_name, uint64_t overload,
    ObRoutineType routine_type, const ObSimpleRoutineSchema *&routine_schema) const
{
  int ret = OB_SUCCESS;
  routine_schema = NULL;

  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == database_id || routine_name.empty()
             || OB_INVALID_INDEX == overload || INVALID_ROUTINE_TYPE == routine_type) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(database_id), K(routine_name));
  } else {
    ObSimpleRoutineSchema *tmp_schema = NULL;
    ObRoutineNameHashWrapper name_wrapper(tenant_id, database_id, package_id, routine_name, overload, routine_type);
    int hash_ret = routine_name_map_.get_refactored(name_wrapper, tmp_schema);
    if (OB_SUCCESS == hash_ret) {
      if (OB_ISNULL(tmp_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(ret), K(tmp_schema));
      } else {
        routine_schema = tmp_schema;
      }
    }
  }

  return ret;
}

int ObRoutineMgr::get_routine_schemas_in_tenant(uint64_t tenant_id, ObIArray<const ObSimpleRoutineSchema *> &routine_schemas) const
{
  int ret = OB_SUCCESS;
  routine_schemas.reset();

  ObTenantRoutineId tenant_routine_id_lower(tenant_id, OB_MIN_ID);
  ConstRoutineIter tenant_routine_begin =
      routine_infos_.lower_bound(tenant_routine_id_lower, compare_with_tenant_routine_id);
  bool is_stop = false;
  for (ConstRoutineIter iter = tenant_routine_begin; OB_SUCC(ret) && iter != routine_infos_.end() && !is_stop; ++iter) {
    const ObSimpleRoutineSchema *routine = NULL;
    if (OB_ISNULL(routine = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret), K(routine));
    } else if (tenant_id != routine->get_tenant_id()) {
      is_stop = true;
    } else if (OB_FAIL(routine_schemas.push_back(routine))) {
      LOG_WARN("push back routine failed", K(ret));
    }
  }

  return ret;
}

int ObRoutineMgr::get_routine_schemas_in_database(uint64_t tenant_id, uint64_t database_id,
                                                  ObIArray<const ObSimpleRoutineSchema *> &routine_schemas) const
{
  int ret = OB_SUCCESS;
  routine_schemas.reset();

  ObTenantRoutineId tenant_outine_id_lower(tenant_id, OB_MIN_ID);
  ConstRoutineIter tenant_routine_begin =
      routine_infos_.lower_bound(tenant_outine_id_lower, compare_with_tenant_routine_id);
  bool is_stop = false;
  for (ConstRoutineIter iter = tenant_routine_begin;
      OB_SUCC(ret) && iter != routine_infos_.end() && !is_stop; ++iter) {
    const ObSimpleRoutineSchema *routine = NULL;
    if (OB_ISNULL(routine = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret), K(routine));
    } else if (tenant_id != routine->get_tenant_id()) {
      is_stop = true;
    } else if (routine->get_database_id() != database_id) {
      // do-nothing
    } else if (OB_FAIL(routine_schemas.push_back(routine))) {
      LOG_WARN("push back procedure failed", K(ret));
    }
  }

  return ret;
}

int ObRoutineMgr::get_routine_schemas_in_udt(
  uint64_t tenant_id, uint64_t udt_id,
  ObIArray<const ObSimpleRoutineSchema *> &routine_schemas) const
{
  int ret = OB_SUCCESS;
  routine_schemas.reset();

  ObTenantRoutineId tenant_outine_id_lower(tenant_id, OB_MIN_ID);
  ConstRoutineIter tenant_routine_begin =
      routine_infos_.lower_bound(tenant_outine_id_lower, compare_with_tenant_routine_id);
  bool is_stop = false;
  for (ConstRoutineIter iter = tenant_routine_begin;
      OB_SUCC(ret) && iter != routine_infos_.end() && !is_stop; ++iter) {
    const ObSimpleRoutineSchema *routine = NULL;
    if (OB_ISNULL(routine = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret), K(routine));
    } else if (tenant_id != routine->get_tenant_id()) {
      is_stop = true;
    } else if (routine->get_package_id() != udt_id
               || routine->get_routine_type() != ROUTINE_UDT_TYPE) {
      // do nothing
    } else if (OB_FAIL(routine_schemas.push_back(routine))) {
      LOG_WARN("push back procedure failed", K(ret));
    }
  }

  return ret;
}

int ObRoutineMgr::get_routine_schemas_in_package(uint64_t tenant_id, uint64_t package_id,
                                                 ObIArray<const ObSimpleRoutineSchema *> &routine_schemas) const
{
  int ret = OB_SUCCESS;
  routine_schemas.reset();

  ObTenantRoutineId tenant_outine_id_lower(tenant_id, OB_MIN_ID);
  ConstRoutineIter tenant_routine_begin =
      routine_infos_.lower_bound(tenant_outine_id_lower, compare_with_tenant_routine_id);
  bool is_stop = false;
  for (ConstRoutineIter iter = tenant_routine_begin;
      OB_SUCC(ret) && iter != routine_infos_.end() && !is_stop; ++iter) {
    const ObSimpleRoutineSchema *routine = NULL;
    if (OB_ISNULL(routine = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret), K(routine));
    } else if (tenant_id != routine->get_tenant_id()) {
      is_stop = true;
    } else if (routine->get_package_id() != package_id
               || routine->get_routine_type() != ROUTINE_PACKAGE_TYPE) {
      // do nothing
    } else if (OB_FAIL(routine_schemas.push_back(routine))) {
      LOG_WARN("push back procedure failed", K(ret));
    }
  }

  return ret;
}

int ObRoutineMgr::del_routine_schemas_in_tenant(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;

  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else {
    ObArray<const ObSimpleRoutineSchema *> schemas;
    if (OB_FAIL(get_routine_schemas_in_tenant(tenant_id, schemas))) {
      LOG_WARN("get routine schemas failed", K(ret), K(tenant_id));
    } else {
      FOREACH_CNT_X(schema, schemas, OB_SUCC(ret)) {
        ObTenantRoutineId tenant_routine_id(tenant_id, (*schema)->get_routine_id());
        if (OB_FAIL(del_routine(tenant_routine_id))) {
          LOG_WARN("del routine failed", K(tenant_routine_id), K(ret));
        }
      }
    }
  }

  return ret;
}

int ObRoutineMgr::del_routine_schemas_in_database(uint64_t tenant_id, uint64_t database_id)
{
  int ret = OB_SUCCESS;

  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id
             || OB_INVALID_ID == database_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(database_id));
  } else {
    ObArray<const ObSimpleRoutineSchema *> schemas;
    if (OB_FAIL(get_routine_schemas_in_database(tenant_id, database_id, schemas))) {
      LOG_WARN("get routine schemas failed", K(ret), K(tenant_id), K(database_id));
    } else {
      FOREACH_CNT_X(schema, schemas, OB_SUCC(ret)) {
        ObTenantRoutineId tenant_routine_id(tenant_id, (*schema)->get_routine_id());
        if (OB_FAIL(del_routine(tenant_routine_id))) {
          LOG_WARN("del routine failed", K(tenant_routine_id), K(ret));
        }
      }
    }
  }

  return ret;
}

int ObRoutineMgr::del_routine_schemas_in_package(uint64_t tenant_id, uint64_t package_id)
{
  int ret = OB_SUCCESS;

  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id
             || OB_INVALID_ID == package_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(package_id));
  } else {
    ObArray<const ObSimpleRoutineSchema *> schemas;
    if (OB_FAIL(get_routine_schemas_in_package(tenant_id, package_id, schemas))) {
      LOG_WARN("get routine schemas failed", K(ret), K(tenant_id), K(package_id));
    } else {
      FOREACH_CNT_X(schema, schemas, OB_SUCC(ret)) {
        ObTenantRoutineId tenant_routine_id(tenant_id, (*schema)->get_routine_id());
        if (OB_FAIL(del_routine(tenant_routine_id))) {
          LOG_WARN("del routine failed", K(tenant_routine_id), K(ret));
        }
      }
    }
  }

  return ret;
}

int ObRoutineMgr::rebuild_routine_hashmap()
{
  int ret = OB_SUCCESS;

  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    routine_id_map_.clear();
    routine_name_map_.clear();
    for (ConstRoutineIter iter = routine_infos_.begin();
        iter != routine_infos_.end() && OB_SUCC(ret); ++iter) {
      ObSimpleRoutineSchema *routine_schema = *iter;
      if (OB_ISNULL(routine_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("routine_schema is NULL", K(ret), K(routine_schema));
      } else {
        int over_write = 1;
        int hash_ret = routine_id_map_.set_refactored(routine_schema->get_routine_id(),
                                                      routine_schema, over_write);
        if (OB_SUCCESS != hash_ret) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("build routine id hashmap failed", K(ret), K(hash_ret),
                   "routine_id", routine_schema->get_routine_id());
        }
        if (OB_SUCC(ret)) {
          ObRoutineNameHashWrapper name_wrapper(routine_schema->get_tenant_id(),
                                                routine_schema->get_database_id(),
                                                routine_schema->get_package_id(),
                                                routine_schema->get_routine_name(),
                                                routine_schema->get_overload(),
                                                routine_schema->get_routine_type());
          hash_ret = routine_name_map_.set_refactored(name_wrapper, routine_schema,
                                                      over_write);
          if (OB_SUCCESS != hash_ret) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("build routine name hashmap failed", K(ret), K(hash_ret),
                     "routine_id", routine_schema->get_routine_id(),
                     "routine_name", routine_schema->get_routine_name());
          }
        }
      }
    }
  }

  return ret;
}

int ObRoutineMgr::get_routine_schema_count(int64_t &routine_schema_count) const
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    routine_schema_count = routine_infos_.size();
  }
  return ret;
}

int ObRoutineMgr::get_schema_statistics(ObSchemaStatisticsInfo &schema_info) const
{
  int ret = OB_SUCCESS;
  schema_info.reset();
  schema_info.schema_type_ = ROUTINE_SCHEMA;
  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    schema_info.count_ = routine_infos_.size();
    for (ConstRoutineIter it = routine_infos_.begin(); OB_SUCC(ret) && it != routine_infos_.end(); it++) {
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

}  // namespace schema
}  // namespace share
}  // namespace oceanbase
