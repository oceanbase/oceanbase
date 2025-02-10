/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX PL_DEPENDENCY
#include "lib/oblog/ob_log_module.h"
#include "ob_pl_dependency_util.h"
#include "ob_pl_resolver.h"
#include "src/sql/resolver/ob_resolver_utils.h"

namespace oceanbase
{
using namespace common;
using namespace sql;
using namespace share::schema;

namespace pl
{

int ObPLDependencyUtil::add_dependency_object_impl(const ObPLDependencyTable *dep_tbl,
                                                  const ObSchemaObjVersion &obj_version)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(dep_tbl)) {
    OZ (add_dependency_object_impl(const_cast<ObPLDependencyTable &>(*dep_tbl), obj_version));
  }
  return ret;
}
int ObPLDependencyUtil::add_dependency_object_impl(ObPLDependencyTable &dep_tbl,
                                                  const ObSchemaObjVersion &obj_version)
{
  int ret = OB_SUCCESS;
  bool exists = false;
  for (ObPLDependencyTable::iterator it = dep_tbl.begin();
                                 it < dep_tbl.end(); it++) {
    if (*it == obj_version) {
      exists = true;
      break;
    }
  }
  if (!exists) {
    OZ (dep_tbl.push_back(obj_version));
  }
  return ret;
}

int ObPLDependencyUtil::get_synonym_object(uint64_t tenant_id,
                                            uint64_t &owner_id,
                                            ObString &object_name,
                                            bool &exist,
                                            sql::ObSQLSessionInfo &session_info,
                                            share::schema::ObSchemaGetterGuard &schema_guard,
                                            ObIArray<ObSchemaObjVersion> *deps)
{
  int ret = OB_SUCCESS;
  ObSchemaChecker schema_checker;
  ObSynonymChecker synonym_checker;
  OZ (schema_checker.init(schema_guard, session_info.get_sessid()));
  OZ (ObResolverUtils::resolve_synonym_object_recursively(
    schema_checker, synonym_checker,
    tenant_id, owner_id, object_name, owner_id, object_name, exist));
  OZ (collect_synonym_deps(tenant_id, synonym_checker, schema_guard, deps));
  return ret;
}

int ObPLDependencyUtil::collect_synonym_deps(uint64_t tenant_id,
                                            ObSynonymChecker &synonym_checker,
                                            share::schema::ObSchemaGetterGuard &schema_guard,
                                            ObIArray<ObSchemaObjVersion> *deps)
{
  int ret = OB_SUCCESS;
  if (synonym_checker.has_synonym() && OB_NOT_NULL(deps)) {
    const ObIArray<uint64_t> &synonym_ids = synonym_checker.get_synonym_ids();
    for (int64_t i = 0; OB_SUCC(ret) && i < synonym_checker.get_synonym_ids().count(); ++i) {
      int64_t schema_version = OB_INVALID_VERSION;
      ObSchemaObjVersion obj_version;
      if (OB_FAIL(schema_guard.get_schema_version(SYNONYM_SCHEMA, tenant_id, synonym_ids.at(i), schema_version))) {
        LOG_WARN("failed to get schema version", K(ret), K(tenant_id), K(synonym_ids.at(i)));
      } else if (OB_UNLIKELY(OB_INVALID_VERSION == schema_version)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("schema version is invalid", K(ret), K(tenant_id), K(synonym_ids.at(i)));
      } else {
        obj_version.object_id_ = synonym_ids.at(i);
        obj_version.object_type_ = DEPENDENCY_SYNONYM;
        obj_version.version_ = schema_version;
        bool exists = false;
        for (int64_t i = 0; !exists && i < deps->count(); ++i) {
          const ObSchemaObjVersion &cur_obj_version = deps->at(i);
          if (cur_obj_version == obj_version) {
            exists = true;
          }
        }
        if (!exists && OB_FAIL(deps->push_back(obj_version))) {
          LOG_WARN("failed to push back obj version to array", K(ret), KPC(deps), K(obj_version));
        }
      }
    }
  }
  return ret;
}

int ObPLDependencyUtil::add_dependency_objects(const ObPLDependencyTable *dep_tbl,
                                               const ObIArray<ObSchemaObjVersion> &dependency_objects)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(dep_tbl)) {
    for (int64_t i = 0; i < dependency_objects.count() ; ++i) {
      OZ (add_dependency_object_impl(dep_tbl, dependency_objects.at(i)));
    }
  }
  return ret;
}

int ObPLDependencyUtil::add_dependency_objects(ObPLDependencyTable &dep_tbl,
                                              const ObPLResolveCtx &resolve_ctx,
                                              const ObPLDataType &type)
{
  int ret = OB_SUCCESS;

  if (type.is_user_type()) {
    ObSchemaObjVersion obj_version;

    if (type.is_package_type()) {
      const ObSimplePackageSchema *package_info = nullptr;
      const uint64_t package_id = extract_package_id(type.get_user_type_id());
      const uint64_t tenant_id = get_tenant_id_by_object_id(package_id);

      if (OB_INVALID_ID == package_id || ObTriggerInfo::is_trigger_package_id(package_id)) {
        // do nothing, may inside package of ddl stage
      } else if (OB_FAIL(resolve_ctx.schema_guard_.get_simple_package_info(tenant_id, package_id, package_info))) {
        LOG_WARN("failed to get_simple_package_info",
                 K(ret), K(type), K(tenant_id), K(package_id), KPC(package_info));
      } else if (OB_ISNULL(package_info)) {
        ret = OB_ERR_PACKAGE_DOSE_NOT_EXIST;
        LOG_WARN("unexpected NULL pacakge info", K(ret), K(type), K(tenant_id), K(package_id));
      } else {
        obj_version.object_id_ = package_id;
        obj_version.object_type_ = DEPENDENCY_PACKAGE;
        obj_version.version_ = package_info->get_schema_version();

        if (OB_FAIL(add_dependency_object_impl(dep_tbl, obj_version))) {
          LOG_WARN("failed to add_dependency_object", K(ret), K(type), KPC(package_info), K(obj_version));
        }
      }
    } else if (type.is_udt_type()) {
      const ObUDTTypeInfo *udt_info = nullptr;
      const uint64_t udt_id = type.get_user_type_id();
      const uint64_t tenant_id = get_tenant_id_by_object_id(udt_id);

      if (OB_FAIL(resolve_ctx.schema_guard_.get_udt_info(tenant_id, udt_id, udt_info))) {
        LOG_WARN("failed to get_udt_info", K(ret), K(type), K(tenant_id), K(udt_id), KPC(udt_info));
      } else if (OB_ISNULL(udt_info)) {
        ret = OB_ERR_PACKAGE_DOSE_NOT_EXIST;
        LOG_WARN("udt not exist", K(ret), K(type), K(tenant_id), K(udt_id));
      } else {
        obj_version.object_id_ = udt_id;
        obj_version.object_type_ = DEPENDENCY_TYPE;
        obj_version.version_ = udt_info->get_schema_version();

        if (OB_FAIL(add_dependency_object_impl(dep_tbl, obj_version))) {
          LOG_WARN("failed to add_dependency_object", K(ret), K(type), KPC(udt_info), K(obj_version));
        }
      }
    } else if (type.is_rowtype_type()) {
      const ObSimpleTableSchemaV2 *table_schema = nullptr;
      const uint64_t table_id = type.get_user_type_id();
      const uint64_t tenant_id = get_tenant_id_by_object_id(table_id);

      if (OB_FAIL(resolve_ctx.schema_guard_.get_simple_table_schema(tenant_id, table_id, table_schema))) {
        LOG_WARN("failed to get_simple_table_schema", K(ret), K(type), K(tenant_id), K(table_id), KPC(table_schema));
      } else if (OB_NOT_NULL(table_schema)) {
        obj_version.object_id_ = table_id;
        obj_version.object_type_ = DEPENDENCY_TABLE;
        obj_version.version_ = table_schema->get_schema_version();

        if (OB_FAIL(add_dependency_object_impl(dep_tbl, obj_version))) {
          LOG_WARN("failed to add_dependency_object", K(ret), K(type), KPC(table_schema), K(obj_version));
        }
      }
    } else {
      // do nothing
    }
  }

  return ret;
}

}
}