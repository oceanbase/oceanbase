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
#include "ob_udt_mgr.h"
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
ObSimpleUDTSchema::ObSimpleUDTSchema()
  : ObSchema()
{
  reset();
}

ObSimpleUDTSchema::ObSimpleUDTSchema(ObIAllocator *allocator)
  : ObSchema(allocator)
{
  reset();
}

ObSimpleUDTSchema::ObSimpleUDTSchema(const ObSimpleUDTSchema &other)
  : ObSchema()
{
  reset();
  *this = other;
}

ObSimpleUDTSchema::~ObSimpleUDTSchema()
{
}

ObSimpleUDTSchema &ObSimpleUDTSchema::operator =(const ObSimpleUDTSchema &other)
{
  if (this != &other) {
    reset();
    int ret = OB_SUCCESS;
    error_ret_ = other.error_ret_;
    tenant_id_ = other.tenant_id_;
    database_id_ = other.database_id_;
    package_id_ = other.package_id_;
    type_id_ = other.type_id_;
    schema_version_ = other.schema_version_;
    typecode_ = other.typecode_;
    if (OB_FAIL(deep_copy_str(other.type_name_, type_name_))) {
      LOG_WARN("Fail to deep copy udt name", K(ret));
    }
    if (OB_FAIL(ret)) {
      error_ret_ = ret;
    }
  }

  return *this;
}

bool ObSimpleUDTSchema::operator ==(const ObSimpleUDTSchema &other) const
{
  bool ret = false;

  if (tenant_id_ == other.tenant_id_
      && database_id_ == other.database_id_
      && package_id_ == other.package_id_
      && type_id_ == other.type_id_
      && schema_version_ == other.schema_version_
      && typecode_ == other.typecode_
      && type_name_ == other.type_name_) {
    ret = true;
  }

  return ret;
}

int64_t ObSimpleUDTSchema::get_convert_size() const
{
  int64_t convert_size = 0;
  convert_size += sizeof(ObSimpleUDTSchema);
  convert_size += type_name_.length() + 1;
  return convert_size;
}

ObUDTMgr::ObUDTMgr()
    : local_allocator_(SET_USE_500(ObModIds::OB_SCHEMA_GETTER_GUARD, ObCtxIds::SCHEMA_SERVICE)),
      allocator_(local_allocator_),
      udt_infos_(0, NULL, SET_USE_500(ObModIds::OB_SCHEMA_UDT_INFO_VECTOR, ObCtxIds::SCHEMA_SERVICE)),
      type_id_map_(SET_USE_500(ObModIds::OB_SCHEMA_UDT_ID_MAP, ObCtxIds::SCHEMA_SERVICE)),
      type_name_map_(SET_USE_500(ObModIds::OB_SCHEMA_UDT_NAME_MAP, ObCtxIds::SCHEMA_SERVICE)),
      is_inited_(false)
{
}

ObUDTMgr::ObUDTMgr(ObIAllocator &allocator)
    : local_allocator_(SET_USE_500(ObModIds::OB_SCHEMA_GETTER_GUARD, ObCtxIds::SCHEMA_SERVICE)),
      allocator_(allocator),
      udt_infos_(0, NULL, SET_USE_500(ObModIds::OB_SCHEMA_UDT_INFO_VECTOR, ObCtxIds::SCHEMA_SERVICE)),
      type_id_map_(SET_USE_500(ObModIds::OB_SCHEMA_UDT_ID_MAP, ObCtxIds::SCHEMA_SERVICE)),
      type_name_map_(SET_USE_500(ObModIds::OB_SCHEMA_UDT_NAME_MAP, ObCtxIds::SCHEMA_SERVICE)),
      is_inited_(false)
{
}

ObUDTMgr::~ObUDTMgr()
{
}

int ObUDTMgr::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(type_id_map_.init())) {
    LOG_WARN("init procedure id map failed", K(ret));
  } else if (OB_FAIL(type_name_map_.init())) {
    LOG_WARN("init procedure name map failed", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObUDTMgr::reset()
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    // reset will not release memory for vector, use clear()
    udt_infos_.clear();
    type_id_map_.clear();
    type_name_map_.clear();
  }
}

ObUDTMgr &ObUDTMgr::operator =(const ObUDTMgr &other)
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

int ObUDTMgr::assign(const ObUDTMgr &other)
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
    ASSIGN_FIELD(udt_infos_);
    ASSIGN_FIELD(type_id_map_);
    ASSIGN_FIELD(type_name_map_);
    #undef ASSIGN_FIELD
  }

  return ret;
}

int ObUDTMgr::deep_copy(const ObUDTMgr &other)
{
  int ret = OB_SUCCESS;

  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (this != &other) {
    reset();
    for (UDTIter iter = other.udt_infos_.begin();
       OB_SUCC(ret) && iter != other.udt_infos_.end(); iter++) {
      ObSimpleUDTSchema *udt = *iter;
      if (OB_ISNULL(udt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(udt), K(ret));
      } else if (OB_FAIL(add_udt(*udt))) {
        LOG_WARN("add procedure failed", K(*udt), K(ret));
      }
    }
  }
  return ret;
}

bool ObUDTMgr::check_inner_stat() const
{
  return is_inited_;
}

bool ObUDTMgr::compare_udt(const ObSimpleUDTSchema *lhs, const ObSimpleUDTSchema *rhs)
{
  return lhs->get_udt_key() < rhs->get_udt_key();
}

bool ObUDTMgr::equal_udt(const ObSimpleUDTSchema *lhs, const ObSimpleUDTSchema *rhs)
{
  return lhs->get_udt_key() == rhs->get_udt_key();
}

bool ObUDTMgr::compare_with_tenant_type_id(const ObSimpleUDTSchema *lhs,
                                          const ObTenantUDTId &tenant_udt_id)
{
  return NULL != lhs ? (lhs->get_udt_key() < tenant_udt_id) : false;
}

bool ObUDTMgr::equal_with_tenant_type_id(const ObSimpleUDTSchema *lhs,
                                         const ObTenantUDTId &tenant_udt_id)
{
  return NULL != lhs ? (lhs->get_udt_key() == tenant_udt_id) : false;
}

int ObUDTMgr::add_udts(const ObIArray<ObSimpleUDTSchema> &udt_schemas)
{
  int ret = OB_SUCCESS;

  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    FOREACH_CNT_X(udt_schema, udt_schemas, OB_SUCC(ret)) {
      if (OB_FAIL(add_udt(*udt_schema))) {
        LOG_WARN("add udt failed", K(ret), "udt_schema", *udt_schema);
      }
    }
  }

  return ret;
}

int ObUDTMgr::del_udts(const ObIArray<ObTenantUDTId> &tenant_udt_ids)
{
  int ret = OB_SUCCESS;

  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    FOREACH_CNT_X(tenant_udt_id, tenant_udt_ids, OB_SUCC(ret)) {
      if (OB_FAIL(del_udt(*tenant_udt_id))) {
        LOG_WARN("del udt failed", K(ret), K(*tenant_udt_id));
      }
    }
  }

  return ret;
}

int ObUDTMgr::add_udt(const ObSimpleUDTSchema &udt_schema)
{
  int ret = OB_SUCCESS;

  ObSimpleUDTSchema *new_udt_schema = NULL;
  UDTIter iter = NULL;
  ObSimpleUDTSchema *replaced_udt = NULL;
  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!udt_schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(udt_schema));
  } else if (OB_FAIL(ObSchemaUtils::alloc_schema(allocator_, udt_schema, new_udt_schema))) {
    LOG_WARN("alloc schema failed", K(ret));
  } else if (OB_ISNULL(new_udt_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(ret), K(new_udt_schema));
  } else if (OB_FAIL(udt_infos_.replace(new_udt_schema,
                                        iter,
                                        compare_udt,
                                        equal_udt,
                                        replaced_udt))) {
    LOG_WARN("failed to add udt schema", K(ret));
  } else {
    int over_write = 1;
    int hash_ret = type_id_map_.set_refactored(new_udt_schema->get_type_id(),
                                               new_udt_schema, over_write);
    if (OB_SUCCESS != hash_ret && OB_HASH_EXIST != hash_ret) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("build udt id hashmap failed", K(ret), K(hash_ret),
               "udt_id", new_udt_schema->get_type_id());
    }
    if (OB_SUCC(ret)) {
      ObUDTNameHashWrapper name_wrapper(new_udt_schema->get_tenant_id(),
                                        new_udt_schema->get_database_id(),
                                        new_udt_schema->get_package_id(),
                                        new_udt_schema->get_type_name());
      hash_ret = type_name_map_.set_refactored(name_wrapper, new_udt_schema, over_write);
      if (OB_SUCCESS != hash_ret && OB_HASH_EXIST != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("build udt name hashmap failed", K(ret), K(hash_ret),
                 "udt_id", new_udt_schema->get_type_id(),
                 "udt_name", new_udt_schema->get_type_name(),
                 "udt_type", new_udt_schema->get_typecode());
      }
    }
  }
  // ignore ret
  if (udt_infos_.count() != type_id_map_.item_count() ||
      udt_infos_.count() != type_name_map_.item_count()) {
    LOG_WARN("udt info is non-consistent",
             "udt_infos_count", udt_infos_.count(),
             "type_id_map_item_count", type_id_map_.item_count(),
             "type_name_map_item_count", type_name_map_.item_count(),
             "udt_id", udt_schema.get_type_id(),
             "udt_name", udt_schema.get_type_name());
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = rebuild_udt_hashmap())) {
      LOG_WARN("rebuild udt hashmap failed", K(tmp_ret));
    }
  }
  return ret;
}

int ObUDTMgr::del_udt(const ObTenantUDTId &tenant_udt_id)
{
  int ret = OB_SUCCESS;

  ObSimpleUDTSchema *schema_to_del = NULL;
  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!tenant_udt_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_udt_id));
  } else if (OB_FAIL(udt_infos_.remove_if(tenant_udt_id, compare_with_tenant_type_id,
                                          equal_with_tenant_type_id,
                                          schema_to_del))) {
    LOG_WARN("failed to remove udt schema, ", K(tenant_udt_id), K(ret));
  } else if (OB_ISNULL(schema_to_del)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("removed procedure schema return NULL, ", K(tenant_udt_id), K(ret));
  } else {
    int hash_ret = type_id_map_.erase_refactored(schema_to_del->get_type_id());
    if (OB_SUCCESS != hash_ret) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed delete udt from procedure id hashmap, ",
               "hash_ret", hash_ret, "udt_id", schema_to_del->get_type_id());
    }
    if (OB_SUCC(ret)) {
      ObUDTNameHashWrapper name_wrapper(schema_to_del->get_tenant_id(),
                                        schema_to_del->get_database_id(),
                                        schema_to_del->get_package_id(),
                                        schema_to_del->get_type_name());
      hash_ret = type_name_map_.erase_refactored(name_wrapper);
      if (OB_SUCCESS != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed delete procedure from procedure name hashmap, ",
                 K(ret), K(hash_ret),
                 "tenant_id", schema_to_del->get_tenant_id(),
                 "database_id", schema_to_del->get_database_id(),
                 "udt_name", schema_to_del->get_type_name());
      }
    }
  }
  // ignore ret
  if (udt_infos_.count() != type_id_map_.item_count() ||
      udt_infos_.count() != type_name_map_.item_count()) {
    LOG_WARN("udt info is non-consistent",
             "udt_infos_count", udt_infos_.count(),
             "type_id_map_item_count", type_id_map_.item_count(),
             "type_name_map_item_count", type_name_map_.item_count(),
             "udt_id", tenant_udt_id.get_type_id());
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = rebuild_udt_hashmap())){
      LOG_WARN("rebuild udt hashmap failed", K(tmp_ret));
    }
  }

  return ret;
}

int ObUDTMgr::get_udt_schema(uint64_t udt_id, const ObSimpleUDTSchema *&udt_schema) const
{
  int ret = OB_SUCCESS;
  udt_schema = NULL;

  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == udt_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(udt_id));
  } else {
    ObSimpleUDTSchema *tmp_schema = NULL;
    int hash_ret = type_id_map_.get_refactored(udt_id, tmp_schema);
    if (OB_SUCCESS == hash_ret) {
      if (OB_ISNULL(tmp_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(ret), K(tmp_schema));
      } else {
        udt_schema = tmp_schema;
      }
    }
  }
  return ret;
}

int ObUDTMgr::get_udt_schema(
    uint64_t tenant_id, uint64_t database_id, uint64_t package_id,
    const common::ObString &udt_name, const ObSimpleUDTSchema *&udt_schema) const
{
  int ret = OB_SUCCESS;
  udt_schema = NULL;

  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id
             || OB_INVALID_ID == database_id
             || udt_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(database_id), K(udt_name));
  } else {
    ObSimpleUDTSchema *tmp_schema = NULL;
    ObUDTNameHashWrapper name_wrapper(tenant_id, database_id, package_id, udt_name);
    int hash_ret = type_name_map_.get_refactored(name_wrapper, tmp_schema);
    if (OB_SUCCESS == hash_ret) {
      if (OB_ISNULL(tmp_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(ret), K(tmp_schema));
      } else {
        udt_schema = tmp_schema;
      }
    }
  }

  return ret;
}

int ObUDTMgr::get_udt_schemas_in_tenant(uint64_t tenant_id, ObIArray<const ObSimpleUDTSchema *> &udt_schemas) const
{
  int ret = OB_SUCCESS;
  udt_schemas.reset();

  ObTenantUDTId tenant_type_id_lower(tenant_id, OB_MIN_ID);
  ConstUDTIter tenant_udt_begin =
      udt_infos_.lower_bound(tenant_type_id_lower, compare_with_tenant_type_id);
  bool is_stop = false;
  for (ConstUDTIter iter = tenant_udt_begin; OB_SUCC(ret) && iter != udt_infos_.end() && !is_stop; ++iter) {
    const ObSimpleUDTSchema *udt = NULL;
    if (OB_ISNULL(udt = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret), K(udt));
    } else if (tenant_id != udt->get_tenant_id()) {
      is_stop = true;
    } else if (OB_FAIL(udt_schemas.push_back(udt))) {
      LOG_WARN("push back udt failed", K(ret));
    }
  }

  return ret;
}

int ObUDTMgr::get_udt_schemas_in_database(uint64_t tenant_id, uint64_t database_id,
                                          ObIArray<const ObSimpleUDTSchema *> &udt_schemas) const
{
  int ret = OB_SUCCESS;
  udt_schemas.reset();

  ObTenantUDTId tenant_type_id_lower(tenant_id, OB_MIN_ID);
  ConstUDTIter tenant_udt_begin =
      udt_infos_.lower_bound(tenant_type_id_lower, compare_with_tenant_type_id);
  bool is_stop = false;
  for (ConstUDTIter iter = tenant_udt_begin;
      OB_SUCC(ret) && iter != udt_infos_.end() && !is_stop; ++iter) {
    const ObSimpleUDTSchema *udt = NULL;
    if (OB_ISNULL(udt = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret), K(udt));
    } else if (tenant_id != udt->get_tenant_id()) {
      is_stop = true;
    } else if (udt->get_database_id() != database_id) {
      // do-nothing
    } else if (OB_FAIL(udt_schemas.push_back(udt))) {
      LOG_WARN("push back procedure failed", K(ret));
    }
  }

  return ret;
}


int ObUDTMgr::get_udt_schemas_in_package(uint64_t tenant_id, uint64_t package_id,
                                                 ObIArray<const ObSimpleUDTSchema *> &udt_schemas) const
{
  int ret = OB_SUCCESS;
  udt_schemas.reset();

  ObTenantUDTId tenant_type_id_lower(tenant_id, OB_MIN_ID);
  ConstUDTIter tenant_udt_begin =
      udt_infos_.lower_bound(tenant_type_id_lower, compare_with_tenant_type_id);
  bool is_stop = false;
  for (ConstUDTIter iter = tenant_udt_begin;
      OB_SUCC(ret) && iter != udt_infos_.end() && !is_stop; ++iter) {
    const ObSimpleUDTSchema *udt = NULL;
    if (OB_ISNULL(udt = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret), K(udt));
    } else if (tenant_id != udt->get_tenant_id()) {
      is_stop = true;
    } else if (udt->get_package_id() != package_id) {
      // do nothing
    } else if (OB_FAIL(udt_schemas.push_back(udt))) {
      LOG_WARN("push back procedure failed", K(ret));
    }
  }

  return ret;
}

int ObUDTMgr::del_udt_schemas_in_tenant(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;

  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else {
    ObArray<const ObSimpleUDTSchema *> schemas;
    if (OB_FAIL(get_udt_schemas_in_tenant(tenant_id, schemas))) {
      LOG_WARN("get udt schemas failed", K(ret), K(tenant_id));
    } else {
      FOREACH_CNT_X(schema, schemas, OB_SUCC(ret)) {
        ObTenantUDTId tenant_udt_id(tenant_id, (*schema)->get_type_id());
        if (OB_FAIL(del_udt(tenant_udt_id))) {
          LOG_WARN("del udt failed", K(tenant_udt_id), K(ret));
        }
      }
    }
  }

  return ret;
}

int ObUDTMgr::del_udt_schemas_in_database(uint64_t tenant_id, uint64_t database_id)
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
    ObArray<const ObSimpleUDTSchema *> schemas;
    if (OB_FAIL(get_udt_schemas_in_database(tenant_id, database_id, schemas))) {
      LOG_WARN("get udt schemas failed", K(ret), K(tenant_id), K(database_id));
    } else {
      FOREACH_CNT_X(schema, schemas, OB_SUCC(ret)) {
        ObTenantUDTId tenant_udt_id(tenant_id, (*schema)->get_type_id());
        if (OB_FAIL(del_udt(tenant_udt_id))) {
          LOG_WARN("del udt failed", K(tenant_udt_id), K(ret));
        }
      }
    }
  }

  return ret;
}

int ObUDTMgr::del_udt_schemas_in_package(uint64_t tenant_id, uint64_t package_id)
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
    ObArray<const ObSimpleUDTSchema *> schemas;
    if (OB_FAIL(get_udt_schemas_in_package(tenant_id, package_id, schemas))) {
      LOG_WARN("get udt schemas failed", K(ret), K(tenant_id), K(package_id));
    } else {
      FOREACH_CNT_X(schema, schemas, OB_SUCC(ret)) {
        ObTenantUDTId tenant_udt_id(tenant_id, (*schema)->get_type_id());
        if (OB_FAIL(del_udt(tenant_udt_id))) {
          LOG_WARN("del udt failed", K(tenant_udt_id), K(ret));
        }
      }
    }
  }

  return ret;
}

int ObUDTMgr::rebuild_udt_hashmap()
{
  int ret = OB_SUCCESS;

  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    type_id_map_.clear();
    type_name_map_.clear();
    for (ConstUDTIter iter = udt_infos_.begin();
        iter != udt_infos_.end() && OB_SUCC(ret); ++iter) {
      ObSimpleUDTSchema *udt_schema = *iter;
      if (OB_ISNULL(udt_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("udt_schema is NULL", K(ret), K(udt_schema));
      } else {
        int over_write = 1;
        int hash_ret = type_id_map_.set_refactored(udt_schema->get_type_id(),
                                                   udt_schema, over_write);
        if (OB_SUCCESS != hash_ret) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("build udt id hashmap failed", K(ret), K(hash_ret),
                   "udt_id", udt_schema->get_type_id());
        }
        if (OB_SUCC(ret)) {
          ObUDTNameHashWrapper name_wrapper(udt_schema->get_tenant_id(),
                                            udt_schema->get_database_id(),
                                            udt_schema->get_package_id(),
                                            udt_schema->get_type_name());
          hash_ret = type_name_map_.set_refactored(name_wrapper,
                                                   udt_schema,
                                                   over_write);
          if (OB_SUCCESS != hash_ret) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("build udt name hashmap failed", K(ret), K(hash_ret),
                     "udt_id", udt_schema->get_type_id(),
                     "udt_name", udt_schema->get_type_name());
          }
        }
      }
    }
  }

  return ret;
}

int ObUDTMgr::get_udt_schema_count(int64_t &udt_schema_count) const
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    udt_schema_count = udt_infos_.size();
  }
  return ret;
}

int ObUDTMgr::get_schema_statistics(ObSchemaStatisticsInfo &schema_info) const
{
  int ret = OB_SUCCESS;
  schema_info.reset();
  schema_info.schema_type_ = UDT_SCHEMA;
  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    schema_info.count_ = udt_infos_.size();
    for (ConstUDTIter it = udt_infos_.begin(); OB_SUCC(ret) && it != udt_infos_.end(); it++) {
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
