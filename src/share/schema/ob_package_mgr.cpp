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
#include "share/schema/ob_package_mgr.h"
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
ObSimplePackageSchema &ObSimplePackageSchema::operator =(const ObSimplePackageSchema &other)
{
  if (this != &other) {
    reset();
    int ret = OB_SUCCESS;
    error_ret_ = other.error_ret_;
    tenant_id_ = other.get_tenant_id();
    package_id_ = other.get_package_id();
    schema_version_ = other.get_schema_version();
    database_id_ = other.get_database_id();
    type_ = other.get_type();
    comp_flag_ = other.get_comp_flag();
    if (OB_FAIL(deep_copy_str(other.package_name_, package_name_))) {
      LOG_WARN("Fail to deep copy package name", K(ret));
    }
    if (OB_FAIL(ret)) {
      error_ret_ = ret;
    }
  }

  return *this;
}

bool ObSimplePackageSchema::operator ==(const ObSimplePackageSchema &other) const
{
  bool ret = false;

  if (tenant_id_ == other.get_tenant_id() &&
      package_id_ == other.get_package_id() &&
      schema_version_ == other.get_schema_version() &&
      database_id_ == other.get_database_id() &&
      package_name_ == other.get_package_name() &&
      type_ == other.get_type() &&
      comp_flag_ == other.get_comp_flag()) {
    ret = true;
  }

  return ret;
}

int64_t ObSimplePackageSchema::get_convert_size() const
{
  int64_t convert_size = 0;

  convert_size += sizeof(ObSimplePackageSchema);
  convert_size += package_name_.length() + 1;

  return convert_size;
}

ObPackageMgr::ObPackageMgr()
    : local_allocator_(SET_USE_500(ObModIds::OB_SCHEMA_GETTER_GUARD, ObCtxIds::SCHEMA_SERVICE)),
      allocator_(local_allocator_),
      package_infos_(0, NULL, SET_USE_500(ObModIds::OB_SCHEMA_PACKAGE_INFO_VECTOR, ObCtxIds::SCHEMA_SERVICE)),
      package_id_map_(SET_USE_500(ObModIds::OB_SCHEMA_PACKAGE_ID_MAP, ObCtxIds::SCHEMA_SERVICE)),
      package_name_map_(SET_USE_500(ObModIds::OB_SCHEMA_PACKAGE_NAME_MAP, ObCtxIds::SCHEMA_SERVICE)),
      is_inited_(false)
{
}

ObPackageMgr::ObPackageMgr(ObIAllocator &allocator)
    : local_allocator_(SET_USE_500(ObModIds::OB_SCHEMA_GETTER_GUARD, ObCtxIds::SCHEMA_SERVICE)),
      allocator_(allocator),
      package_infos_(0, NULL, SET_USE_500(ObModIds::OB_SCHEMA_PACKAGE_INFO_VECTOR, ObCtxIds::SCHEMA_SERVICE)),
      package_id_map_(SET_USE_500(ObModIds::OB_SCHEMA_PACKAGE_ID_MAP, ObCtxIds::SCHEMA_SERVICE)),
      package_name_map_(SET_USE_500(ObModIds::OB_SCHEMA_PACKAGE_NAME_MAP, ObCtxIds::SCHEMA_SERVICE)),
      is_inited_(false)
{
}


int ObPackageMgr::init()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(package_id_map_.init())) {
    LOG_WARN("init package id map failed", K(ret));
  } else if (OB_FAIL(package_name_map_.init())) {
    LOG_WARN("init package name map failed", K(ret));
  } else {
    is_inited_ = true;
  }

  return ret;
}

void ObPackageMgr::reset()
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    // reset will not release memory for vector, use clear()
    package_infos_.clear();
    package_id_map_.clear();
    package_name_map_.clear();
  }
}

ObPackageMgr &ObPackageMgr::operator =(const ObPackageMgr &other)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(assign(other))) {
    LOG_WARN("assign failed", K(ret));
  }

  return *this;
}

int ObPackageMgr::assign(const ObPackageMgr &other)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
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
    ASSIGN_FIELD(package_infos_);
    ASSIGN_FIELD(package_id_map_);
    ASSIGN_FIELD(package_name_map_);
    #undef ASSIGN_FIELD
  }

  return ret;
}

int ObPackageMgr::deep_copy(const ObPackageMgr &other)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (this != &other) {
    reset();
    for (PackageIter iter = other.package_infos_.begin();
       OB_SUCC(ret) && iter != other.package_infos_.end(); iter++) {
      ObSimplePackageSchema *package = *iter;
      if (OB_ISNULL(package)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(package), K(ret));
      } else if (OB_FAIL(add_package(*package))) {
        LOG_WARN("add package failed", K(*package), K(ret));
      }
    }
  }
  return ret;
}

bool ObPackageMgr::compare_package(const ObSimplePackageSchema *lhs, const ObSimplePackageSchema *rhs)
{
  return lhs->get_tenant_package_id() < rhs->get_tenant_package_id();
}

bool ObPackageMgr::equal_package(const ObSimplePackageSchema *lhs, const ObSimplePackageSchema *rhs)
{
  return lhs->get_tenant_package_id() == rhs->get_tenant_package_id();
}

bool ObPackageMgr::compare_with_tenant_package_id(const ObSimplePackageSchema *lhs,
                                                 const ObTenantPackageId &tenant_package_id)
{
  return NULL != lhs ? (lhs->get_tenant_package_id() < tenant_package_id) : false;
}

bool ObPackageMgr::equal_with_tenant_package_id(const ObSimplePackageSchema *lhs,
                                                    const ObTenantPackageId &tenant_package_id)
{
  return NULL != lhs ? (lhs->get_tenant_package_id() == tenant_package_id) : false;
}

int ObPackageMgr::add_packages(const ObIArray<ObSimplePackageSchema> &package_schemas)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    FOREACH_CNT_X(package_schema, package_schemas, OB_SUCC(ret)) {
      if (OB_FAIL(add_package(*package_schema))) {
        LOG_WARN("add package failed", K(ret), "package_schema", *package_schema);
      }
    }
  }
  return ret;
}

int ObPackageMgr::del_packages(const ObIArray<ObTenantPackageId> &packages)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    FOREACH_CNT_X(package_id, packages, OB_SUCC(ret)) {
      if (OB_FAIL(del_package(*package_id))) {
        LOG_WARN("del package failed", K(ret), K(*package_id));
      }
    }
  }

  return ret;
}

int ObPackageMgr::add_package(const ObSimplePackageSchema &package_schema)
{
  int ret = OB_SUCCESS;

  ObSimplePackageSchema *new_package_schema = NULL;
  PackageIter iter = NULL;
  ObSimplePackageSchema *replaced_package = NULL;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!package_schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(package_schema));
  } else if (OB_FAIL(ObSchemaUtils::alloc_schema(allocator_,
                                                 package_schema,
                                                 new_package_schema))) {
    LOG_WARN("alloc schema failed", K(ret));
  } else if (OB_ISNULL(new_package_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(ret), K(new_package_schema));
  } else if (OB_FAIL(package_infos_.replace(new_package_schema,
                                           iter,
                                           compare_package,
                                           equal_package,
                                           replaced_package))) {
    LOG_WARN("failed to add package schema", K(ret));
  } else {
    int over_write = 1;
    int hash_ret = package_id_map_.set_refactored(new_package_schema->get_package_id(),
                                                  new_package_schema, over_write);
    if (OB_SUCCESS != hash_ret && OB_HASH_EXIST != hash_ret) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("build package id hashmap failed", K(ret), K(hash_ret),
               "package_id", new_package_schema->get_package_id());
    }
    if (OB_SUCC(ret)) {
      ObPackageNameHashWrapper name_wrapper(new_package_schema->get_tenant_id(),
                                            new_package_schema->get_database_id(),
                                            new_package_schema->get_package_name(),
                                            new_package_schema->get_type(),
                                            new_package_schema->get_comp_flag() & COMPATIBLE_MODE_BIT);
      hash_ret = package_name_map_.set_refactored(name_wrapper, new_package_schema, over_write);
      if (OB_SUCCESS != hash_ret && OB_HASH_EXIST != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("build package name hashmap failed", K(ret), K(hash_ret),
                 "package_id", new_package_schema->get_package_id(),
                 "package_name", new_package_schema->get_package_name(),
                 "package_type", new_package_schema->get_type(),
                 "comp_flag", new_package_schema->get_comp_flag());
      }
    }
  }

  return ret;
}

int ObPackageMgr::del_package(const ObTenantPackageId &package_id)
{
  int ret = OB_SUCCESS;

  ObSimplePackageSchema *schema_to_del = NULL;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!package_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(package_id));
  } else if (OB_FAIL(package_infos_.remove_if(package_id, compare_with_tenant_package_id,
                                                equal_with_tenant_package_id,
                                                schema_to_del))) {
    LOG_WARN("failed to remove package schema, ", K(package_id), K(ret));
  } else if (OB_ISNULL(schema_to_del)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("removed package schema return NULL, ", K(package_id), K(ret));
  } else {
    int hash_ret = package_id_map_.erase_refactored(schema_to_del->get_package_id());
    if (OB_SUCCESS != hash_ret) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed delete package from package id hashmap, ",
               "hash_ret", hash_ret, "package_id", schema_to_del->get_package_id());
    }
    if (OB_SUCC(ret)) {
      ObPackageNameHashWrapper name_wrapper(schema_to_del->get_tenant_id(),
                                            schema_to_del->get_database_id(),
                                            schema_to_del->get_package_name(),
                                            schema_to_del->get_type(),
                                            schema_to_del->get_comp_flag() & COMPATIBLE_MODE_BIT);
      hash_ret = package_name_map_.erase_refactored(name_wrapper);
      if (OB_SUCCESS != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed delete package from package name hashmap, ",
                 K(ret), K(hash_ret),
                 "tenant_id", schema_to_del->get_tenant_id(),
                 "database_id", schema_to_del->get_database_id(),
                 "package_name", schema_to_del->get_package_name(),
                 "package_type", schema_to_del->get_type(),
                 "comp_flag", schema_to_del->get_comp_flag());
      }
    }
  }

  return ret;
}

int ObPackageMgr::get_package_schema(uint64_t package_id, const ObSimplePackageSchema *&package_schema) const
{
  int ret = OB_SUCCESS;
  package_schema = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == package_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(package_id));
  } else {
    ObSimplePackageSchema *tmp_schema = NULL;
    int hash_ret = package_id_map_.get_refactored(package_id, tmp_schema);
    if (OB_SUCCESS == hash_ret) {
      if (OB_ISNULL(tmp_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(ret), K(tmp_schema));
      } else {
        package_schema = tmp_schema;
      }
    }
  }

  return ret;
}

int ObPackageMgr::get_package_schema(uint64_t tenant_id, uint64_t database_id,
                                     const ObString &package_name, ObPackageType package_type,
                                     int64_t compat_mode,
                                     const ObSimplePackageSchema *&package_schema) const
{
  int ret = OB_SUCCESS;
  package_schema = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == database_id || package_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(database_id), K(package_name));
  } else {
    ObSimplePackageSchema *tmp_schema = NULL;
    ObPackageNameHashWrapper name_wrapper(tenant_id, database_id, package_name, package_type, compat_mode);
    int hash_ret = package_name_map_.get_refactored(name_wrapper, tmp_schema);
    if (OB_SUCCESS == hash_ret) {
      if (OB_ISNULL(tmp_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(ret), K(tmp_schema));
      } else {
        package_schema = tmp_schema;
      }
    }
  }

  return ret;
}

int ObPackageMgr::get_package_schemas_in_tenant(uint64_t tenant_id, ObIArray<const ObSimplePackageSchema *> &package_schemas) const
{
  int ret = OB_SUCCESS;
  package_schemas.reset();

  ObTenantPackageId tenant_package_id_lower(tenant_id, OB_MIN_ID);
  ConstPackageIter tenant_package_begin =
      package_infos_.lower_bound(tenant_package_id_lower, compare_with_tenant_package_id);
  bool is_stop = false;
  for (ConstPackageIter iter = tenant_package_begin; OB_SUCC(ret) && iter != package_infos_.end() && !is_stop; ++iter) {
    const ObSimplePackageSchema *package = NULL;
    if (OB_ISNULL(package = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret), K(package));
    } else if (tenant_id != package->get_tenant_id()) {
      is_stop = true;
    } else if (OB_FAIL(package_schemas.push_back(package))) {
      LOG_WARN("push back package failed", K(ret));
    }
  }

  return ret;
}

int ObPackageMgr::del_package_schemas_in_tenant(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else {
    ObArray<const ObSimplePackageSchema *> schemas;
    if (OB_FAIL(get_package_schemas_in_tenant(tenant_id, schemas))) {
      LOG_WARN("get package schemas failed", K(ret), K(tenant_id));
    } else {
      FOREACH_CNT_X(schema, schemas, OB_SUCC(ret)) {
        ObTenantPackageId tenant_package_id(tenant_id, (*schema)->get_package_id());
        if (OB_FAIL(del_package(tenant_package_id))) {
          LOG_WARN("del package failed", K(tenant_package_id), K(ret));
        }
      }
    }
  }

  return ret;
}

int ObPackageMgr::get_package_schemas_in_database(uint64_t tenant_id, uint64_t database_id,
                                    common::ObIArray<const ObSimplePackageSchema *> &package_schemas) const
{
  int ret = OB_SUCCESS;
  package_schemas.reset();

  ObTenantPackageId tenant_package_id_lower(tenant_id, OB_MIN_ID);
  ConstPackageIter tenant_package_begin =
      package_infos_.lower_bound(tenant_package_id_lower, compare_with_tenant_package_id);
  bool is_stop = false;
  for (ConstPackageIter iter = tenant_package_begin; OB_SUCC(ret) && iter != package_infos_.end() && !is_stop; ++iter) {
    const ObSimplePackageSchema *package = NULL;
    if (OB_ISNULL(package = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret), K(package));
    } else if (tenant_id != package->get_tenant_id()) {
      is_stop = true;
    } else if (package->get_database_id() != database_id) {
      // do-nothing
    } else if (OB_FAIL(package_schemas.push_back(package))) {
      LOG_WARN("push back package failed", K(ret));
    }
  }

  return ret;
}

int ObPackageMgr::del_package_schemas_in_databae(uint64_t tenant_id, uint64_t database_id)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id
             || OB_INVALID_ID == database_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else {
    ObArray<const ObSimplePackageSchema *> schemas;
    if (OB_FAIL(get_package_schemas_in_database(tenant_id, database_id, schemas))) {
      LOG_WARN("get package schemas failed", K(ret), K(tenant_id));
    } else {
      FOREACH_CNT_X(schema, schemas, OB_SUCC(ret)) {
        ObTenantPackageId tenant_package_id(tenant_id, (*schema)->get_package_id());
        if (OB_FAIL(del_package(tenant_package_id))) {
          LOG_WARN("del package failed", K(tenant_package_id), K(ret));
        }
      }
    }
  }

  return ret;
}

int ObPackageMgr::get_package_schema_count(int64_t &package_schema_count) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    package_schema_count = package_infos_.size();
  }
  return ret;
}

int ObPackageMgr::get_schema_statistics(ObSchemaStatisticsInfo &schema_info) const
{
  int ret = OB_SUCCESS;
  schema_info.reset();
  schema_info.schema_type_ = PACKAGE_SCHEMA;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    schema_info.count_ = package_infos_.size();
    for (ConstPackageIter it = package_infos_.begin(); OB_SUCC(ret) && it != package_infos_.end(); it++) {
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




