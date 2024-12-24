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

#define USING_LOG_PREFIX PL
#include "pl/ob_pl_type.h"
#include "observer/mysql/obsm_utils.h"
#include "pl/ob_pl.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/ob_spi.h"
#include "sql/engine/ob_exec_context.h"
#include "ob_pl_code_generator.h"
#include "share/schema/ob_routine_info.h"
#include "observer/mysql/obmp_stmt_execute.h"
#include "pl/ob_pl_resolver.h"
#include "pl/ob_pl_package.h"
#include "common/ob_smart_call.h"
#include "sql/resolver/expr/ob_raw_expr_copier.h"
#include "pl/ob_pl_user_type.h"
#include "dblink/ob_pl_dblink_guard.h"
#include "sql/resolver/ob_stmt_resolver.h"
namespace oceanbase
{
using namespace common;
using namespace sql;
using namespace share::schema;
using namespace observer;
using namespace obmysql;

namespace pl
{

DEF_TO_STRING(ObPLDataType)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(type));
  J_COMMA();
  J_KV(K_(type_from));
  J_COMMA();
  J_KV(K_(not_null));
  J_COMMA();
  J_KV(K_(pls_type));
  J_COMMA();
  J_KV(K_(type_info_id));
  J_COMMA();
  if (is_obj_type()) {
    J_KV(K_(obj_type));
  } else if (is_user_type()) {
    J_KV(K_(user_type_id));
  } else {
    const char *pl_type = "INVALID DATA TYPE";
    J_KV(K(pl_type));
  }
  return pos;
}

int ObPLDataType::get_udt_type_by_name(uint64_t tenant_id,
                                       uint64_t owner_id,
                                       const ObString &udt,
                                       sql::ObSQLSessionInfo &session_info,
                                       share::schema::ObSchemaGetterGuard &schema_guard,
                                       ObPLDataType &pl_type,
                                       ObIArray<ObSchemaObjVersion> *deps)
{
  int ret = OB_SUCCESS;
  const ObUDTTypeInfo *udt_info = NULL;
  ObPLType type = ObPLType::PL_INVALID_TYPE;
  OZ (schema_guard.get_udt_info(tenant_id, owner_id, OB_INVALID_ID, udt, udt_info));
  if (OB_SUCC(ret) && OB_ISNULL(udt_info)) {
    uint64_t object_owner_id = owner_id;
    ObString object_name = udt;
    bool exist = false;
    OZ (get_synonym_object(tenant_id, object_owner_id, object_name, exist, session_info, schema_guard, deps));
    if (OB_SUCC(ret) && exist) {
      OZ (schema_guard.get_udt_info(tenant_id, object_owner_id, OB_INVALID_ID, object_name, udt_info));
    }
    if (OB_SUCC(ret) && OB_ISNULL(udt_info)) {
      ret = OB_ERR_SP_UNDECLARED_TYPE;
      LOG_WARN("udt not exist", K(ret), K(tenant_id), K(owner_id), K(udt));
      LOG_USER_ERROR(OB_ERR_SP_UNDECLARED_TYPE, udt.length(), udt.ptr());
    }
  }
#ifdef OB_BUILD_ORACLE_PL
  OX (type = udt_info->is_opaque()
        ? PL_OPAQUE_TYPE :
          udt_info->is_collection()
            ? (udt_info->is_varray() ? PL_VARRAY_TYPE : PL_NESTED_TABLE_TYPE) : PL_RECORD_TYPE);
#else
  OX (type = PL_RECORD_TYPE);
#endif
  OX (pl_type.set_user_type_id(type, udt_info->get_type_id()));
  OX (pl_type.set_type_from(PL_TYPE_UDT));
  if (OB_SUCC(ret) && OB_NOT_NULL(deps)) {
    share::schema::ObSchemaObjVersion obj_version;
    new(&obj_version)ObSchemaObjVersion(udt_info->get_type_id(), udt_info->get_schema_version(), DEPENDENCY_TYPE);
    OZ (deps->push_back(obj_version));
  }
  return ret;
}

#ifdef OB_BUILD_ORACLE_PL
int ObPLDataType::get_pkg_type_by_name(uint64_t tenant_id,
                                       uint64_t owner_id,
                                       const ObString &pkg,
                                       const ObString &type,
                                       ObIAllocator &allocator,
                                       sql::ObSQLSessionInfo &session_info,
                                       share::schema::ObSchemaGetterGuard &schema_guard,
                                       common::ObMySQLProxy &sql_proxy,
                                       bool is_pkg_var, // pkg var or pkg type
                                       ObPLDataType &pl_type,
                                       ObIArray<ObSchemaObjVersion> *deps)
{
  int ret = OB_SUCCESS;
  const share::schema::ObPackageInfo *package_info = NULL;
  int64_t compatible_mode = lib::is_oracle_mode() ? COMPATIBLE_ORACLE_MODE
                                                  : COMPATIBLE_MYSQL_MODE;
  ObPLPackageManager *package_manager = NULL;
  pl::ObPLPackageGuard package_guard(session_info.get_effective_tenant_id());
  pl::ObPLResolveCtx resolve_ctx(allocator, session_info, schema_guard,
                                package_guard, sql_proxy, false);
  OZ (package_guard.init());
  CK (OB_NOT_NULL(session_info.get_pl_engine()));
  OZ (schema_guard.get_package_info(tenant_id, owner_id, pkg, share::schema::PACKAGE_TYPE,
                                    compatible_mode, package_info));
  if (OB_SUCC(ret) && OB_ISNULL(package_info)) {
    uint64_t object_owner_id = owner_id;
    ObString object_name = pkg;
    bool exist = false;
    OZ (get_synonym_object(tenant_id, object_owner_id, object_name, exist, session_info, schema_guard, deps));
    if (OB_SUCC(ret) && exist) {
      OZ (schema_guard.get_package_info(tenant_id, object_owner_id, object_name, share::schema::PACKAGE_TYPE,
                                        compatible_mode, package_info));
    }
    if (OB_SUCC(ret) && OB_ISNULL(package_info)) {
      ret = OB_ERR_PACKAGE_DOSE_NOT_EXIST;
      LOG_WARN("package not exist", K(ret), K(tenant_id), K(owner_id), K(pkg), K(type));
      {
        ObString db_name("");
        const ObDatabaseSchema *database_schema = NULL;
        if (OB_SUCCESS == schema_guard.get_database_schema(tenant_id, owner_id, database_schema)) {
          if (NULL != database_schema) {
            db_name =database_schema->get_database_name_str();
          }
        }
        LOG_USER_ERROR(OB_ERR_PACKAGE_DOSE_NOT_EXIST, "PACKAGE OR TABLE",
                                 db_name.length(), db_name.ptr(), pkg.length(), pkg.ptr());
      }
    }
  }
  OX (package_manager = &(session_info.get_pl_engine()->get_package_manager()));
  CK (OB_NOT_NULL(package_manager));
  if (is_pkg_var) {
    const ObPLVar *pkg_var = NULL;
    int64_t var_idx = OB_INVALID_ID;
    CK (OB_NOT_NULL(package_info));
    OZ (package_manager->get_package_var(resolve_ctx,
                                         package_info->get_package_id(),
                                         type,
                                         pkg_var,
                                         var_idx));
    if (OB_SUCC(ret) && OB_ISNULL(pkg_var)) {
      ret = OB_ERR_SP_UNDECLARED_TYPE;
      LOG_WARN("package variable is not exist", K(ret), K(tenant_id), K(owner_id), K(pkg), K(type));
      LOG_USER_ERROR(OB_ERR_SP_UNDECLARED_TYPE, type.length(), type.ptr());
    }
    if (OB_FAIL(ret) || OB_ISNULL(pkg_var)) {
    } else if (pl::PL_CURSOR_TYPE == pkg_var->get_type().get_type()) {
      OX (pl_type.set_user_type_id(PL_RECORD_TYPE, pkg_var->get_type().get_user_type_id()));
      OX (pl_type.set_type_from(PL_TYPE_PACKAGE));
    } else {
      OX (pl_type = pkg_var->get_type());
      OX (pl_type.set_type_from(PL_TYPE_ATTR_TYPE));
    }
  } else {
    const ObUserDefinedType *user_type = NULL;
    CK (OB_NOT_NULL(package_info));
    OZ (package_manager->get_package_type(resolve_ctx,
                                          package_info->get_package_id(),
                                          type,
                                          user_type));
    if (OB_SUCC(ret) && OB_ISNULL(user_type)) {
      ret = OB_ERR_SP_UNDECLARED_TYPE;
      LOG_WARN("package type is not exist", K(ret), K(tenant_id), K(owner_id), K(pkg), K(type));
      LOG_USER_ERROR(OB_ERR_SP_UNDECLARED_TYPE, type.length(), type.ptr());
    }
    if (OB_SUCC(ret)) {
      if (user_type->is_subtype()) {
        const ObUserDefinedSubType* subtype = static_cast<const ObUserDefinedSubType*>(user_type);
        CK (OB_NOT_NULL(subtype));
        OX (pl_type = *(subtype->get_base_type()));
      } else {
        OX (pl_type = *user_type);
      }
      if (!pl_type.is_sys_refcursor_type()) {
        OX (pl_type.set_type_from(PL_TYPE_PACKAGE));
      }
    }
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(deps)) {
    share::schema::ObSchemaObjVersion obj_version;
    new(&obj_version)ObSchemaObjVersion(package_info->get_package_id(),
                                        package_info->get_schema_version(),
                                        DEPENDENCY_PACKAGE);
    OZ (deps->push_back(obj_version));
  }
  return ret;
}
#endif

int ObPLDataType::collect_synonym_deps(uint64_t tenant_id,
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
        if (OB_FAIL(deps->push_back(obj_version))) {
          LOG_WARN("failed to push back obj version to array", K(ret), KPC(deps), K(obj_version));
        }
      }
    }
  }
  return ret;
}

int ObPLDataType::get_synonym_object(uint64_t tenant_id,
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

int ObPLDataType::get_table_type_by_name(uint64_t tenant_id,
                                         uint64_t owner_id,
                                         const ObString &table,
                                         const ObString &type,
                                         ObIAllocator &allocator,
                                         sql::ObSQLSessionInfo &session_info,
                                         share::schema::ObSchemaGetterGuard &schema_guard,
                                         bool is_rowtype,
                                         ObPLDataType &pl_type,
                                         ObIArray<ObSchemaObjVersion> *deps)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_info = NULL;
  OZ (schema_guard.get_table_schema(tenant_id, owner_id, table, false, table_info));
  if (OB_SUCC(ret) && OB_ISNULL(table_info)) {
    uint64_t object_owner_id = owner_id;
    ObString object_name = table;
    bool exist = false;
    OZ (get_synonym_object(tenant_id, object_owner_id, object_name, exist, session_info, schema_guard, deps));
    if (OB_SUCC(ret) && exist) {
      OZ (schema_guard.get_table_schema(tenant_id, object_owner_id, object_name, false, table_info));
    }
    if (OB_SUCC(ret) && OB_ISNULL(table_info)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("table is not exist", K(ret), K(tenant_id), K(owner_id), K(table), K(type), K(is_rowtype));
    }
  }
  if (is_rowtype) {
    CK (type.empty());
    CK (OB_NOT_NULL(table_info));
    OX (pl_type.set_user_type_id(PL_RECORD_TYPE, table_info->get_table_id()));
    OX (pl_type.set_type_from(PL_TYPE_ATTR_ROWTYPE));
  } else {
    ObPLPackageGuard dummy_guard(session_info.get_effective_tenant_id());
    ObMySQLProxy dummy_proxy;
    ObPLResolveCtx ctx(allocator, session_info, schema_guard, dummy_guard, dummy_proxy, false);
    ObRecordType *record_type = NULL;
    const ObPLDataType *member_type = NULL;
    CK (!type.empty());
    OZ (ObPLResolver::build_record_type_by_schema(ctx, table_info, record_type));
    CK (OB_NOT_NULL(record_type));
    int64_t i = 0;
    for (; OB_SUCC(ret) && i < record_type->get_member_count(); ++i) {
      const ObString *record_name = record_type->get_record_member_name(i);
      CK (OB_NOT_NULL(record_name));
      if (OB_SUCC(ret) && 0 == record_name->case_compare(type)) {
        CK (OB_NOT_NULL(member_type = record_type->get_record_member_type(i)));
        CK (OB_NOT_NULL(member_type->get_data_type()));
        OX (pl_type.set_data_type(*(member_type->get_data_type())));
        OX (pl_type.set_type_from(PL_TYPE_ATTR_TYPE));
        break;
      }
    }
    if (OB_SUCC(ret) && i == record_type->get_member_count()) {
      ret = OB_ERR_COLUMN_NOT_FOUND;
      LOG_WARN("table`s column not found!", K(ret), K(tenant_id), K(owner_id), K(table), K(type));
    }
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(deps)) {
    bool is_view = table_info->is_view_table() && !table_info->is_materialized_view();
    ObSchemaObjVersion obj_version;
    new(&obj_version)ObSchemaObjVersion(table_info->get_table_id(),
                                        table_info->get_schema_version(),
                                        is_view ? DEPENDENCY_VIEW : DEPENDENCY_TABLE);
    OZ (deps->push_back(obj_version));
  }
  return ret;
}

int ObPLDataType::transform_from_iparam(const ObRoutineParam *iparam,
                                        share::schema::ObSchemaGetterGuard &schema_guard,
                                        sql::ObSQLSessionInfo &session_info,
                                        ObIAllocator &allocator,
                                        common::ObMySQLProxy &sql_proxy,
                                        pl::ObPLDataType &pl_type,
                                        ObIArray<ObSchemaObjVersion> *deps,
                                        ObPLDbLinkGuard *dblink_guard)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(iparam));
  if (OB_FAIL(ret)) {
    // do nothing ...
  } else if (!iparam->is_extern_type()) {
    ObDataType *data_type = NULL;
    ObPLEnumSetCtx* enum_set_ctx_bk = pl_type.get_enum_set_ctx();
    OX (pl_type = iparam->get_pl_data_type());
    OX (pl_type.set_enum_set_ctx(enum_set_ctx_bk));
    OZ (pl_type.set_type_info(iparam->get_extended_type_info()));
    OX (data_type = pl_type.get_data_type());
    CK (OB_NOT_NULL(data_type));
    if (OB_SUCC(ret)) {
      ObObjMeta &meta = const_cast<ObObjMeta&>(data_type->get_meta_type());
      if (data_type->get_meta_type().is_bit()) {
        meta.set_scale(data_type->get_accuracy().get_precision());
      } else {
        meta.set_scale(data_type->get_accuracy().get_scale());
      }
      if (meta.is_string_or_lob_locator_type() ||
          meta.is_enum_or_set() ||
          meta.is_json() ||
          meta.is_geometry()) {
        meta.set_collation_level(CS_LEVEL_IMPLICIT);
      }
    }
  } else {
    ObParamExternType type = iparam->get_extern_type_flag();
    const uint64_t tenant_id = is_oceanbase_sys_database_id(iparam->get_type_owner()) ?
                               OB_SYS_TENANT_ID : session_info.get_effective_tenant_id();
    switch (type) {
      case SP_EXTERN_UDT: {
        OZ (get_udt_type_by_name(tenant_id,
                                 iparam->get_type_owner(),
                                 iparam->get_type_name(),
                                 session_info,
                                 schema_guard,
                                 pl_type,
                                 deps));
        break;
      }
#ifdef OB_BUILD_ORACLE_PL
      case SP_EXTERN_PKG: {
        OZ (get_pkg_type_by_name(tenant_id,
                                 iparam->get_type_owner(),
                                 iparam->get_type_subname(),
                                 iparam->get_type_name(),
                                 allocator,
                                 session_info,
                                 schema_guard,
                                 sql_proxy,
                                 false,
                                 pl_type,
                                 deps));
        break;
      }
      case SP_EXTERN_PKG_VAR: {
        OZ (get_pkg_type_by_name(tenant_id,
                                 iparam->get_type_owner(),
                                 iparam->get_type_subname(),
                                 iparam->get_type_name(),
                                 allocator,
                                 session_info,
                                 schema_guard,
                                 sql_proxy,
                                 true,
                                 pl_type,
                                 deps));
        break;
      }
      case SP_EXTERN_TAB_COL: {
        OZ (get_table_type_by_name(tenant_id,
                                   iparam->get_type_owner(),
                                   iparam->get_type_subname(),
                                   iparam->get_type_name(),
                                   allocator,
                                   session_info,
                                   schema_guard,
                                   false,
                                   pl_type,
                                   deps));
        if (OB_SUCC(ret) && iparam->is_in_param() && ob_is_numeric_type(pl_type.get_obj_type())) {
          const ObAccuracy &default_accuracy =  ObAccuracy::DDL_DEFAULT_ACCURACY2[lib::is_oracle_mode()][pl_type.get_obj_type()];
          // precision of decimal int must be equal to precision defined in schema.
          if (!pl_type.get_data_type()->get_meta_type().is_decimal_int()) {
            pl_type.get_data_type()->set_accuracy(default_accuracy);
          }
        }
        break;
      }
      case SP_EXTERN_PKGVAR_OR_TABCOL: {
        OZ (get_table_type_by_name(tenant_id,
                                   iparam->get_type_owner(),
                                   iparam->get_type_subname(),
                                   iparam->get_type_name(),
                                   allocator,
                                   session_info,
                                   schema_guard,
                                   false,
                                   pl_type,
                                   deps));
        if (OB_TABLE_NOT_EXIST == ret || OB_ERR_COLUMN_NOT_FOUND == ret) {
          ret = OB_SUCCESS;
          OZ (get_pkg_type_by_name(tenant_id,
                                   iparam->get_type_owner(),
                                   iparam->get_type_subname(),
                                   iparam->get_type_name(),
                                   allocator,
                                   session_info,
                                   schema_guard,
                                   sql_proxy,
                                   true,
                                   pl_type,
                                   deps));
        }
        break;
      }
      case SP_EXTERN_SYS_REFCURSOR: {
        pl_type.set_sys_refcursor_type();
      }
        break;
#endif
      case SP_EXTERN_TAB: {
        OZ (get_table_type_by_name(tenant_id,
                                   iparam->get_type_owner(),
                                   iparam->get_type_name(),
                                   ObString(""),
                                   allocator,
                                   session_info,
                                   schema_guard,
                                   true,
                                   pl_type,
                                   deps));
        break;
      }
      case SP_EXTERN_LOCAL_VAR : {
        ObDataType *data_type = NULL;
        OX (pl_type = iparam->get_pl_data_type());
        OX (data_type = pl_type.get_data_type());
        CK (OB_NOT_NULL(data_type));
        if (OB_SUCC(ret)) {
          ObObjMeta &meta = const_cast<ObObjMeta&>(data_type->get_meta_type());
          if (data_type->get_meta_type().is_bit()) {
            meta.set_scale(data_type->get_accuracy().get_precision());
          } else {
            meta.set_scale(data_type->get_accuracy().get_scale());
          }
        }
        break;
      }
#ifdef OB_BUILD_ORACLE_PL
      case SP_EXTERN_DBLINK :{
        const ObUserDefinedType *udt = NULL;
        const ObRoutineParam *param = static_cast<const ObRoutineParam*>(iparam);
        CK (OB_NOT_NULL(param));
        CK (OB_NOT_NULL(dblink_guard));
        CK (param->get_extended_type_info().count() > 0);
        OZ (dblink_guard->get_dblink_type_by_name(param->get_type_owner(), param->get_extended_type_info().at(0),
                                                  param->get_type_subname(), param->get_type_name(), udt));
        CK (OB_NOT_NULL(udt));
        OX (pl_type.set_user_type_id(udt->get_type(), udt->get_user_type_id()));
        OX (pl_type.set_type_from(ObPLTypeFrom::PL_TYPE_DBLINK));
      }
      break;
#endif
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected extern type", K(ret), K(type));
        break;
      }
    }
  }
  return ret;
}

int ObPLDataType::transform_and_add_routine_param(const pl::ObPLRoutineParam *param,
                                                  int64_t position,
                                                  int64_t level,
                                                  int64_t &sequence,
                                                  share::schema::ObRoutineInfo &routine_info)
{
  int ret = OB_SUCCESS;
  ObRoutineParam param_info;
  CK (OB_NOT_NULL(param));
  OX (param_info.set_tenant_id(routine_info.get_tenant_id()));
  OX (param_info.set_routine_id(routine_info.get_routine_id()));
  OX (param_info.set_sequence(sequence++));
  OX (param_info.set_subprogram_id(routine_info.get_subprogram_id()));
  OX (param_info.set_param_position(position));
  OX (param_info.set_param_level(level));
  OX (param_info.set_flag(param->get_mode()));
  OX (param->is_nocopy_param() ? param_info.set_nocopy_param() : void(NULL));
  OX (param->is_default_cast() ? param_info.set_default_cast() : void(NULL));
  OZ (param_info.set_param_name(param->get_name()));
  if (OB_FAIL(ret)) {
  } else if (param->get_extern_type() == ObParamExternType::SP_EXTERN_INVALID) {
    CK (param->get_pl_data_type().is_obj_type());
    CK (OB_NOT_NULL(param->get_pl_data_type().get_data_type()));
    OX (param_info.set_param_type(*(param->get_pl_data_type().get_data_type())));
  } else {
    OX (param_info.set_param_type(ObExtendType));
    OZ (param_info.set_type_name(param->get_type_name()));
    OZ (param_info.set_type_subname(param->get_type_subname()));
    OX (param_info.set_type_owner(param->get_type_owner()));
    OX (param_info.set_extern_type_flag(static_cast<ObParamExternType>(param->get_extern_type())));
  }
  OZ (routine_info.add_routine_param(param_info));
  return ret;
}

//is same enum_set_ctx, only need to copy type_info_id_
int ObPLDataType::deep_copy(common::ObIAllocator &alloc, const ObPLDataType &other)
{
  int ret = OB_SUCCESS;
  type_ = other.type_;
  type_from_ = other.type_from_;
  user_type_id_ = other.user_type_id_;
  obj_type_ = other.obj_type_;
  pls_type_ = other.pls_type_;
  not_null_ = other.not_null_;
  enum_set_ctx_ = other.enum_set_ctx_;
  type_info_id_ = other.type_info_id_;
  return ret;
}

//is not same enum_set_ctx, need to set type_info_id independently
int ObPLDataType::deep_copy(ObPLEnumSetCtx &enum_set_ctx, const ObPLDataType &other)
{
  int ret = OB_SUCCESS;
  ObIArray<ObString> *type_info = NULL;
  type_ = other.type_;
  type_from_ = other.type_from_;
  user_type_id_ = other.user_type_id_;
  obj_type_ = other.obj_type_;
  pls_type_ = other.pls_type_;
  not_null_ = other.not_null_;
  OZ (other.get_type_info(type_info));
  if (OB_NOT_NULL(type_info)) {
    enum_set_ctx_ = &enum_set_ctx;
    OZ (set_type_info(*type_info));
  }
  return ret;
}

bool ObPLDataType::operator==(const ObPLDataType &other) const
{
  return type_ == other.type_
      && type_from_ == other.type_from_
      && (is_obj_type() ? obj_type_ == other.obj_type_ : true)
      && not_null_ == other.not_null_
      && pls_type_ == other.pls_type_
      && (!is_obj_type() ? user_type_id_ == other.user_type_id_ : true)
      && (is_enum_or_set_type() //enum or set type maybe not in the same enum_set_ctx_, so check array is equal
          ? (enum_set_ctx_ != NULL && other.enum_set_ctx_ != NULL && is_array_equal(*(enum_set_ctx_->get_enum_type_info(type_info_id_)), *(other.enum_set_ctx_->get_enum_type_info(other.type_info_id_))))
          : true);
}

void ObPLDataType::set_data_type(const common::ObDataType &obj_type)
{
  type_ = PL_OBJ_TYPE;
  obj_type_ = obj_type;
  if (obj_type.get_meta_type().is_string_or_lob_locator_type()) {
    obj_type_.set_collation_level(common::CS_LEVEL_IMPLICIT);
  }
}

common::ObObjType ObPLDataType::get_obj_type() const
{
  common::ObObjType type = common::ObNullType;;
  if (is_obj_type()) {
    type = get_data_type()->get_obj_type();
  } else {
    type = common::ObExtendType;
  }
  return type;
}

uint64_t ObPLDataType::get_user_type_id() const
{
  uint64_t user_type_id = common::OB_INVALID_ID;
  if (is_user_type()) {
    user_type_id = user_type_id_;
  }
  return user_type_id;
}

//基本数据类型在LLVM里的全局符号表存储的数据类型
int ObPLDataType::get_llvm_type(common::ObObjType obj_type, jit::ObLLVMHelper& helper, ObPLADTService &adt_service, jit::ObLLVMType &type)
{
  return get_datum_type(obj_type, helper, adt_service, type);
}


//基本数据类型在sql里的数据的实际存储类型全部都是ObObj
int ObPLDataType::get_datum_type(common::ObObjType obj_type, jit::ObLLVMHelper& helper, ObPLADTService &adt_service, jit::ObLLVMType &type)
{
  UNUSED(obj_type); UNUSED(helper); UNUSED(adt_service);
  return adt_service.get_obj(type);
}

int ObPLDataType::generate_assign_with_null(ObPLCodeGenerator &generator,
                                            const ObPLINS &ns,
                                            jit::ObLLVMValue &allocator,
                                            jit::ObLLVMValue &dest) const
{
  int ret = OB_SUCCESS;
  if (is_obj_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected type to assign NULL", K(*this), K(ret));
  } else {
    ObArenaAllocator tmp_allocator(GET_PL_MOD_STRING(PL_MOD_IDX::OB_PL_ARENA), OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    const ObUserDefinedType *user_type = NULL;
    if (OB_FAIL(ns.get_user_type(get_user_type_id(), user_type, &tmp_allocator))) {
      LOG_WARN("failed to get user type", K(*this), K(ret));
    } else if (OB_ISNULL(user_type)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Cannot get valid user type", K(*this), K(ret));
    } else if (OB_FAIL(SMART_CALL(user_type->generate_assign_with_null(generator,
                                                                       ns,
                                                                       allocator,
                                                                       dest)))) {
      LOG_WARN("failed to generate assign with null", K(*this), K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObPLDataType::generate_default_value(ObPLCodeGenerator &generator,
                                         const ObPLINS &ns,
                                         const pl::ObPLStmt *stmt,
                                         jit::ObLLVMValue &value,
                                         jit::ObLLVMValue &llvm_allocator,
                                         bool is_top_level) const
{
  int ret = OB_SUCCESS;
  if (is_obj_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected type to assign NULL", K(*this), K(ret));
  } else {
    ObArenaAllocator allocator(GET_PL_MOD_STRING(PL_MOD_IDX::OB_PL_ARENA), OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    const ObUserDefinedType *user_type = NULL;
    if (OB_FAIL(ns.get_user_type(get_user_type_id(), user_type, &allocator))) {
      LOG_WARN("failed to get user type", K(*this), K(ret));
    } else if (OB_ISNULL(user_type)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Cannot get valid user type", K(*this), K(ret));
    } else if (OB_FAIL(SMART_CALL(user_type->generate_default_value(generator,
                                                                       ns,
                                                                       stmt,
                                                                       value,
                                                                       llvm_allocator,
                                                                       is_top_level)))) {
      LOG_WARN("failed to generate default value", K(*this), K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObPLDataType::generate_copy(ObPLCodeGenerator &generator,
                                const ObPLBlockNS &ns,
                                jit::ObLLVMValue &allocator,
                                jit::ObLLVMValue &src,
                                jit::ObLLVMValue &dest,
                                uint64_t location,
                                bool in_notfound,
                                bool in_warning,
                                uint64_t package_id) const
{
  UNUSED(ns);
  int ret = OB_SUCCESS;
  ObSEArray<jit::ObLLVMValue, 4> args;
  jit::ObLLVMValue llvm_value;
  jit::ObLLVMValue dest_type;

  OZ (args.push_back(generator.get_vars()[generator.CTX_IDX]));
  OZ (args.push_back(allocator));
  OZ (args.push_back(src));
  OZ (args.push_back(dest));
  if (is_composite_type()) {
    ObDataType obj_type;
    ObObjMeta meta;
    meta.set_ext();
    obj_type.set_meta_type(meta);
    obj_type.set_udt_id(user_type_id_);
    OZ (generator.generate_data_type(obj_type, dest_type));
  } else {
    OZ (generator.generate_data_type(obj_type_, dest_type));
  }
  OZ (args.push_back(dest_type));
  OZ (generator.get_helper().get_int64(package_id, llvm_value));
  OZ (args.push_back(llvm_value));
  if (OB_SUCC(ret)) {
    jit::ObLLVMValue ret_err;
    if (OB_FAIL(generator.get_helper().create_call(ObString("spi_copy_datum"), generator.get_spi_service().spi_copy_datum_, args, ret_err))) {
      LOG_WARN("failed to create call", K(ret));
    } else if (OB_FAIL(generator.check_success(ret_err, location, in_notfound, in_warning))) {
      LOG_WARN("failed to check success", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObPLDataType::generate_construct(ObPLCodeGenerator &generator,
                                     const ObPLINS &ns,
                                     jit::ObLLVMValue &value,
                                     jit::ObLLVMValue &llvm_allocator,
                                     bool is_top_level,
                                     const pl::ObPLStmt *stmt) const
{
  int ret = OB_SUCCESS;
  if (is_obj_type()) {
    ObObj obj(get_obj_type());
    OZ (generator.generate_obj(obj, value));
  } else {
    ObArenaAllocator allocator(GET_PL_MOD_STRING(PL_MOD_IDX::OB_PL_ARENA), OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    const ObUserDefinedType *user_type = NULL;
    OZ (ns.get_user_type(get_user_type_id(), user_type, &allocator));
    CK (OB_NOT_NULL(user_type));
    OZ (SMART_CALL(user_type->generate_construct(generator, ns, value, llvm_allocator, is_top_level, stmt)));
  }
  return ret;
}

int ObPLDataType::generate_new(ObPLCodeGenerator &generator,
                                     const ObPLINS &ns,
                                     jit::ObLLVMValue &value,
                                     jit::ObLLVMValue &llvm_allocator,
                                     bool is_top_level,
                                     const pl::ObPLStmt *stmt) const
{
  int ret = OB_SUCCESS;
  if (is_obj_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("NOoooooooo, please don't do that", K(ret));
  } else {
    ObArenaAllocator allocator(GET_PL_MOD_STRING(PL_MOD_IDX::OB_PL_ARENA), OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    const ObUserDefinedType *user_type = NULL;
    OZ (ns.get_user_type(get_user_type_id(), user_type, &allocator));
    CK (OB_NOT_NULL(user_type));
    OZ (SMART_CALL(user_type->generate_new(generator, ns, value, llvm_allocator, is_top_level, stmt)));
  }
  return ret;
}

int ObPLDataType::newx(common::ObIAllocator &allocator, const ObPLINS *ns, int64_t &ptr) const
{
  int ret = OB_SUCCESS;
  if (is_obj_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("NOoooooooo, please don't do that", K(type_), K(obj_type_), K(ret));
  } else {
    ObArenaAllocator tmp_allocator(GET_PL_MOD_STRING(PL_MOD_IDX::OB_PL_ARENA), OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    CK (OB_NOT_NULL(ns));
    const ObUserDefinedType *user_type = NULL;
    OZ (ns->get_user_type(get_user_type_id(), user_type, &tmp_allocator));
    CK (OB_NOT_NULL(user_type));
    OZ (SMART_CALL(user_type->newx(allocator, ns, ptr)));
  }
  return ret;
}

int ObPLDataType::get_size(ObPLTypeSize type, int64_t &size) const
{
  UNUSED(type);
  int ret = OB_SUCCESS;
//  if (is_obj_type()) {
    size += sizeof(ObObj);
//  } else {
//    ObArenaAllocator allocator;
//    OZ (ns.get_size(type, *this, size, &allocator));
//  }
  return ret;
}

int ObPLDataType::get_field_count(const ObPLINS& ns, int64_t &count) const
{
  int ret = OB_SUCCESS;
  count = 1;
  if (is_record_type()) { //只有record的field是member的个数，其他类型的field都是1
    ObArenaAllocator allocator(GET_PL_MOD_STRING(PL_MOD_IDX::OB_PL_ARENA), OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    const ObUserDefinedType *user_type = NULL;
    OZ (ns.get_user_type(get_user_type_id(), user_type, &allocator));
    CK (OB_NOT_NULL(user_type));
    if (OB_SUCC(ret)) {
      count = static_cast<const ObRecordType*>(user_type)->get_member_count();
    }
  }
  return ret;
}

int ObPLDataType::init_session_var(const ObPLResolveCtx &resolve_ctx,
                                   ObIAllocator &obj_allocator,
                                   ObExecContext &exec_ctx,
                                   const sql::ObSqlExpression *default_expr,
                                   bool default_construct,
                                   ObObj &obj) const
{
  int ret = OB_SUCCESS;
  if (is_obj_type()) {
    obj.set_meta_type(get_data_type()->get_meta_type());
    if (OB_ISNULL(default_expr)) {
      // do nothing ...
    } else {
      ObObj calc_obj;
      ObArenaAllocator tmp_allocator(GET_PL_MOD_STRING(PL_MOD_IDX::OB_PL_INIT_SESSION_VAR), OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
      if (OB_FAIL(ObSQLUtils::calc_sql_expression_without_row(exec_ctx,*default_expr,calc_obj, &tmp_allocator))) {
        LOG_WARN("calc expr failed", K(ret));
      } else if (calc_obj.need_deep_copy()) {
        char *copy_data = NULL;
        int64_t copy_size = calc_obj.get_deep_copy_size();
        int64_t copy_pos = 0;
        if (OB_ISNULL(copy_data = static_cast<char *>(obj_allocator.alloc(copy_size)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("memory allocate failed", K(ret));
        } else if (OB_FAIL(obj.deep_copy(calc_obj, copy_data, copy_size, copy_pos))) {
          LOG_WARN("obj deep copy failed", K(ret));
        } else {}
      } else {
        obj = calc_obj;
      }
    }
  } else if (is_cursor_type()) {
    OZ (ObSPIService::spi_cursor_alloc(obj_allocator, obj));
  } else {
    const ObUserDefinedType *user_type = NULL;
    OZ (get_external_user_type(resolve_ctx, user_type), KPC(this));
    CK (OB_NOT_NULL(user_type));
    OZ (user_type->init_session_var(resolve_ctx,
                                    obj_allocator,
                                    exec_ctx,
                                    default_expr,
                                    default_construct,
                                    obj), KPC(this));
  }
  return ret;
}

// -------------------- Start for Package Session Variable Serialize/DeSerialize ------

int ObPLDataType::get_serialize_size(
  const ObPLResolveCtx &resolve_ctx, common::ObObj &obj, int64_t &size) const
{
  int ret = OB_SUCCESS;
  char *src = reinterpret_cast<char*>(&obj);
  size = 8; // for MIN_CLUSTER_VERSION
  OZ (get_serialize_size(resolve_ctx, src, size));
  LOG_DEBUG("get serialize size", K(ret), K(obj), K(size));
  return ret;
}

int ObPLDataType::serialize(
  const ObPLResolveCtx &resolve_ctx, common::ObObj &obj, common::ObObj &result) const
{
  int ret = OB_SUCCESS;
  char *serialize_buff = NULL;
  int64_t serialize_size = 0;
  int64_t serialize_pos = 0;
  char *src = reinterpret_cast<char*>(&obj);
  OZ (get_serialize_size(resolve_ctx, obj, serialize_size));
  CK (serialize_size > 0);
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(
    serialize_buff = static_cast<char*>(resolve_ctx.allocator_.alloc(serialize_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocator memory for serialzie session variable buffer!",
             K(ret), K(serialize_size), K(obj));
  } else if (is_user_type()) {
    //add cluster version for complex type!
    OZ (serialization::encode_vi64(
      serialize_buff, serialize_size, serialize_pos, GET_MIN_CLUSTER_VERSION()));
  }
  OZ (serialize(resolve_ctx, src, serialize_buff, serialize_size, serialize_pos));
  CK (serialize_pos <= serialize_size);
  OX (result.set_hex_string(ObString(serialize_pos, serialize_buff)));
  LOG_DEBUG("serialize pl package variable obj",
            K(ret), K(result), K(serialize_pos), K(serialize_size));
  return ret;
}

int ObPLDataType::deserialize(
  const ObPLResolveCtx &resolve_ctx,
  common::ObIAllocator &allocator,
  const char* src, const int64_t src_len, common::ObObj &result) const
{
  int ret = OB_SUCCESS;
  int64_t version = OB_INVALID_VERSION;
  int64_t src_pos = 0;
  char *dst = reinterpret_cast<char*>(&result);
  if (is_user_type()) {
    //get cluster version first for complex type!
    OZ (serialization::decode_vi64(src, src_len, src_pos, &version));
  }
  OZ (deserialize(
    resolve_ctx, allocator, src, src_len, src_pos, dst));
  LOG_DEBUG("deserialize pl package variable obj", K(ret), K(result));
  return ret;
}

int ObPLDataType::get_serialize_size(
  const ObPLResolveCtx &resolve_ctx, char *&src, int64_t &size) const
{
  int ret = OB_SUCCESS;
  ObObj *obj = NULL;
  CK (OB_NOT_NULL(obj = reinterpret_cast<ObObj*>(src)));
  if (OB_FAIL(ret)) {
  } else if (is_obj_type()) {
    if (ObMaxType == obj->get_type()) {
      ObObj max = ObObj::make_max_obj();
      size += max.get_serialize_size();
    } else {
      size += obj->get_serialize_size();
    }
  } else {
    const ObUserDefinedType *user_type = NULL;
    char *type_src = NULL;
    if (obj->is_ext()) {
      CK (OB_NOT_NULL(type_src = reinterpret_cast<char*>(obj->get_ext())));
    } else {
      CK (OB_NOT_NULL(type_src = reinterpret_cast<char*>(obj)));
    }
    OZ (get_external_user_type(resolve_ctx, user_type), KPC(this));
    CK (OB_NOT_NULL(user_type));
    OZ (user_type->get_serialize_size(resolve_ctx, type_src, size), KPC(this));
  }
  OX (src += sizeof(ObObj));
  return ret;
}

int ObPLDataType::serialize(
  const ObPLResolveCtx &resolve_ctx,
  char *&src, char* dst, int64_t dst_len, int64_t &dst_pos) const
{
  int ret = OB_SUCCESS;
  ObObj *obj = NULL;
  CK (OB_NOT_NULL(obj = reinterpret_cast<ObObj*>(src)));
  if (OB_FAIL(ret)) {
  } else if (is_obj_type()) {
    if (ObMaxType == obj->get_type()) {
      ObObj max = ObObj::make_max_obj();
      OZ (max.serialize(dst, dst_len, dst_pos));
    } else {
      OZ (obj->serialize(dst, dst_len, dst_pos));
    }
  } else {
    const ObUserDefinedType *user_type = NULL;
    char *type_src = NULL;
    if (obj->is_ext()) {
      CK (OB_NOT_NULL(type_src = reinterpret_cast<char*>(obj->get_ext())));
    } else {
      CK (OB_NOT_NULL(type_src = reinterpret_cast<char*>(obj)));
    }
    OZ (get_external_user_type(resolve_ctx, user_type), KPC(this));
    CK (OB_NOT_NULL(user_type));
    OZ (user_type->serialize(resolve_ctx, type_src, dst, dst_len, dst_pos), KPC(this));
  }
  OX (src += sizeof(ObObj));
  return ret;
}

int ObPLDataType::deserialize(
  const ObPLResolveCtx &resolve_ctx,
  common::ObIAllocator &allocator,
  const char* src, const int64_t src_len, int64_t &src_pos, char *&dst) const
{
  int ret = OB_SUCCESS;
  ObObj *obj = NULL;
  CK (OB_NOT_NULL(src));
  CK (OB_NOT_NULL(dst));
  CK (src_pos <= src_len);
  CK (OB_NOT_NULL(obj = reinterpret_cast<ObObj*>(dst)));
  if (OB_FAIL(ret)) {
  } else if (is_obj_type()) {
    ObObj src_obj;
    OZ (src_obj.deserialize(src, src_len, src_pos));
    if (OB_FAIL(ret)) {
    } else if (src_obj.is_max_value()) {
      OX (new(obj)ObObj(ObMaxType));
    } else {
      OZ (deep_copy_obj(allocator, src_obj, *obj));
    }
  } else {
    const ObUserDefinedType *user_type = NULL;
    char *type_dst = NULL;
    if (obj->is_ext()) {
      CK (OB_NOT_NULL(type_dst = reinterpret_cast<char*>(obj->get_ext())));
    } else {
      CK (OB_NOT_NULL(type_dst = reinterpret_cast<char*>(obj)));
    }
    OZ (get_external_user_type(resolve_ctx, user_type), KPC(this));
    CK (OB_NOT_NULL(user_type));
    OZ (user_type->deserialize(resolve_ctx, allocator, src, src_len, src_pos, type_dst));
  }
  OX (dst += sizeof(ObObj));
  return ret;
}

// -------------------- End for Package Session Variable Serialize/DeSerialize ------

int ObPLDataType::serialize(share::schema::ObSchemaGetterGuard &schema_guard,
                            const ObTimeZoneInfo *tz_info,
                            MYSQL_PROTOCOL_TYPE type,
                            char *&src,
                            char *dst,
                            const int64_t dst_len,
                            int64_t &dst_pos) const
{
  int ret = OB_SUCCESS;
  if (is_obj_type()) {
    obmysql::EMySQLFieldType mysql_type = obmysql::EMySQLFieldType::MYSQL_TYPE_NOT_DEFINED;
    uint16_t flags;
    ObScale num_decimals;
    ObObj obj;
    ObField field;
    if (OB_FAIL(ObSMUtils::get_mysql_type(get_obj_type(), mysql_type, flags, num_decimals))) {
      LOG_WARN("get mysql type failed", K(ret), K(get_obj_type()));
    } else {
      obj = *(reinterpret_cast<ObObj *>(src));
      src += sizeof(ObObj);
      field.accuracy_ = get_data_type()->get_accuracy();
      field.flags_ = get_data_type()->is_zero_fill() ? ZEROFILL_FLAG : 0;
    }
    if (OB_SUCC(ret)
        && !obj.is_invalid_type() // deleted element not serialize.
        && !obj.is_null()) { // null already serialized into null map.
      if (obj.get_type() != get_obj_type()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to serialize pl data type, data type inconsistent with pl type",
                 K(get_obj_type()), K(obj.get_type()), K(obj), K(*this), K(ret));
      } else if (OB_FAIL(ObSMUtils::cell_str(dst, dst_len, obj, type, dst_pos, OB_INVALID_ID, NULL, tz_info, &field, NULL))) {
        LOG_WARN("failed to cell str", K(ret), K(obj), K(dst_len), K(dst_pos));
      } else {
        LOG_DEBUG("success serialize pl data type", K(*this), K(obj),
          K(reinterpret_cast<int64_t>(dst)), K(dst_len), K(type), K(dst_pos));
      }
    }
  } else {
    const ObUserDefinedType *user_type = NULL;
    const ObUDTTypeInfo *udt_info = NULL;
    ObArenaAllocator local_allocator;
    const uint64_t tenant_id = get_tenant_id_by_object_id(get_user_type_id());
    if (!is_udt_type()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not support other type except udt type", K(ret), K(get_type_from()));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "non-schema user defined type deserialize");
    } else if (OB_FAIL(schema_guard.get_udt_info(tenant_id, get_user_type_id(), udt_info))) {
      LOG_WARN("failed to get udt info", K(ret), K(tenant_id), K(get_user_type_id()));
    } else if (OB_ISNULL(udt_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("udt info is null", K(ret), K(get_user_type_id()));
    } else if (OB_FAIL(udt_info->transform_to_pl_type(local_allocator, user_type))) {
      LOG_WARN("failed to transform to pl type", K(ret), KPC(udt_info));
    } else if (OB_ISNULL(user_type)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("user type is null", K(ret), K(user_type));
    } else if (OB_FAIL(user_type->serialize(schema_guard, tz_info, type, src, dst, dst_len, dst_pos))) {
      LOG_WARN("failed to deserialize user type", K(ret));
    }
  }
  return ret;
}

int ObPLDataType::deserialize(ObSchemaGetterGuard &schema_guard,
                              common::ObIAllocator &allocator,
                              const ObCharsetType charset,
                              const ObCollationType cs_type,
                              const ObCollationType ncs_type,
                              const common::ObTimeZoneInfo *tz_info,
                              const char *&src,
                              char *dst,
                              const int64_t dst_len,
                              int64_t &dst_pos) const
{
  int ret = OB_SUCCESS;
  if (is_obj_type()) {
    int64_t init_size = 0;
    obmysql::EMySQLFieldType mysql_type = obmysql::EMySQLFieldType::MYSQL_TYPE_NOT_DEFINED;
    uint16_t flags;
    ObScale num_decimals;
    ObObj param;
    ObArenaAllocator local_allocator;
    if (OB_FAIL(get_size(PL_TYPE_INIT_SIZE, init_size))) {
      LOG_WARN("get base type init size failed", K(ret));
    } else if (OB_ISNULL(dst) || (dst_len - dst_pos < init_size)) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("data size overflow", K(ret));
    } else if (OB_FAIL(ObSMUtils::get_mysql_type(get_obj_type(), mysql_type, flags, num_decimals))) {
      LOG_WARN("get mysql type failed", K(ret));
    } else if (OB_FAIL(ObMPStmtExecute::parse_basic_param_value(
        local_allocator, (uint8_t)mysql_type, charset, ObCharsetType::CHARSET_INVALID, cs_type, ncs_type, src, tz_info, param, true, NULL,
        NULL == get_data_type() ? false : get_data_type()->get_meta_type().is_unsigned_integer()))) {
      // get_data_type() is null, its a extend type, unsigned need false.
      LOG_WARN("failed to parse basic param value", K(ret));
    } else {
      ObObj *obj = reinterpret_cast<ObObj *>(dst + dst_pos);
      OZ (deep_copy_obj(allocator, param, *obj));
      OX (dst_pos += sizeof(ObObj));
      LOG_DEBUG("deserialize ob pl data type success",
                K(*this), K(*obj), K(obj), K(dst_pos), K(dst), K(ret));
    }
  } else {
    const ObUserDefinedType *user_type = NULL;
    const ObUDTTypeInfo *udt_info = NULL;
    ObArenaAllocator local_allocator;
    const uint64_t tenant_id = get_tenant_id_by_object_id(get_user_type_id());
    ObObj *obj = reinterpret_cast<ObObj *>(dst + dst_pos);
    int64_t new_dst_len = 0;
    int64_t new_dst_pos = 0;
    if (!is_udt_type()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not support other type except udt type", K(ret), K(get_type_from()));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "non-schema user defined type deserialize");
    } else if (OB_FAIL(schema_guard.get_udt_info(tenant_id, get_user_type_id(), udt_info))) {
      LOG_WARN("failed to get udt info", K(ret), K(tenant_id));
    } else if (OB_ISNULL(udt_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("udt info is null", K(ret));
    } else if (OB_FAIL(udt_info->transform_to_pl_type(local_allocator, user_type))) {
      LOG_WARN("failed to transform to pl type", K(ret));
    } else if (OB_ISNULL(user_type)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("user type is null", K(ret), K(user_type));
    } else if (OB_FAIL(user_type->init_obj(schema_guard, allocator, *obj, new_dst_len))) {
      LOG_WARN("failed to init obj", K(ret));
    } else if (OB_FAIL(user_type->deserialize(schema_guard, allocator,
                  charset, cs_type, ncs_type, tz_info, src,
                  reinterpret_cast<char *>(obj->get_ext()), new_dst_len, new_dst_pos))) {
      LOG_WARN("failed to deserialize user type", K(ret));
    } else {
      dst_pos += sizeof(ObObj);
    }
  }
  return ret;
}

int ObPLDataType::convert(ObPLResolveCtx &ctx, ObObj *&src, ObObj *&dst) const
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(src));
  CK (OB_NOT_NULL(dst));
  if (is_obj_type()) {
    ObExprResType result_type;
    result_type.reset();
    CK (OB_NOT_NULL(get_data_type()));
    OX (result_type.set_meta(get_data_type()->get_meta_type()));
    OX (result_type.set_accuracy(get_data_type()->get_accuracy()));
    OZ (ObSPIService::spi_convert(ctx.session_info_, ctx.allocator_, *src, result_type, *dst));
    OX (src ++);
    OX (dst ++);
  } else {
    ObArenaAllocator allocator(GET_PL_MOD_STRING(PL_MOD_IDX::OB_PL_ARENA), OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    const ObUserDefinedType *user_type = NULL;
    OZ (ctx.get_user_type(get_user_type_id(), user_type, &allocator));
    CK (OB_NOT_NULL(user_type));
    OZ (user_type->convert(ctx, src, dst));
  }
  return ret;
}

int ObPLDataType::add_package_routine_schema_param(const ObPLResolveCtx &resolve_ctx,
                                                   const ObPLBlockNS &block_ns,
                                                   const ObString &package_name,
                                                   const ObString &param_name,
                                                   int64_t mode, int64_t position,
                                                   int64_t level, int64_t &sequence,
                                                   ObRoutineInfo &routine_info) const
{
  int ret = OB_SUCCESS;
  ObRoutineParam param_info;
  if (is_obj_type()) { // 基础数据类型
    param_info.set_tenant_id(routine_info.get_tenant_id());
    param_info.set_routine_id(routine_info.get_routine_id());
    param_info.set_sequence(sequence++);
    param_info.set_subprogram_id(routine_info.get_subprogram_id());
    param_info.set_param_position(position);
    param_info.set_param_level(level);
    param_info.set_flag(mode);
    OZ (param_info.set_param_name(param_name));
    OX (param_info.set_param_type(*get_data_type()));
    OZ (routine_info.add_routine_param(param_info));
  } else { // 复杂数据类型
    const ObUserDefinedType *user_type = NULL;
    ObString type_package_name;
    uint64_t type_package_id = OB_INVALID_ID;
    OZ (block_ns.get_pl_data_type_by_id(get_user_type_id(), user_type));
    CK (OB_NOT_NULL(user_type));
    CK (is_package_type() || is_local_type() || is_rowtype_type() || is_udt_type());
    if (OB_SUCC(ret) && is_package_type()) {
      const ObPackageInfo *package_info = NULL;
      type_package_id = extract_package_id(user_type->get_user_type_id());
      const uint64_t tenant_id = get_tenant_id_by_object_id(type_package_id);
      CK (OB_INVALID_ID != type_package_id);
      OZ (resolve_ctx.schema_guard_.get_package_info(tenant_id, type_package_id, package_info));
      CK (OB_NOT_NULL(package_info));
      OX (param_info.set_type_owner(package_info->get_database_id()));
      OZ (param_info.set_type_subname(package_info->get_package_name()));
    }
    if (OB_SUCC(ret) && is_local_type()) {
      OX (param_info.set_type_owner(resolve_ctx.session_info_.get_database_id()));
      OZ (param_info.set_type_subname(package_name));
    }
    if (OB_SUCC(ret) && is_rowtype_type()) {
      uint64_t table_id = user_type->get_user_type_id();
      const uint64_t tenant_id = resolve_ctx.session_info_.get_effective_tenant_id();
      const share::schema::ObTableSchema *table_schema = NULL;
      OZ (resolve_ctx.schema_guard_.get_table_schema(tenant_id, table_id, table_schema));
      CK (OB_NOT_NULL(table_schema));
      OX (param_info.set_type_owner(table_schema->get_database_id()));
    }
    if (OB_SUCC(ret) && is_udt_type()) {
      uint64_t udt_id = user_type->get_user_type_id();
      const uint64_t tenant_id = get_tenant_id_by_object_id(udt_id);
      const share::schema::ObUDTTypeInfo *udt_info = NULL;
      OZ (resolve_ctx.schema_guard_.get_udt_info(tenant_id, udt_id, udt_info));
      CK (OB_NOT_NULL(udt_info));
      OX (param_info.set_type_owner(udt_info->get_database_id()));
    }
    OX (param_info.set_tenant_id(routine_info.get_tenant_id()));
    OX (param_info.set_routine_id(routine_info.get_routine_id()));
    OX (param_info.set_sequence(sequence++));
    OX (param_info.set_subprogram_id(routine_info.get_subprogram_id()));
    OX (param_info.set_param_position(position));
    OX (param_info.set_param_level(level));
    OX (param_info.set_flag(mode));
    OZ (param_info.set_param_name(param_name));
    OZ (param_info.set_type_name(user_type->get_name()));
    OX (param_info.set_param_type(ObExtendType));
    OZ (routine_info.add_routine_param(param_info));
    // TODO: Oracle的实现会将复杂类型在Routine系统表中展开, 同时在Type系统表中记录一份, 暂时未看出这么做的用意,
    // Type中记录的信息已经足够使用, 因此这里不将复杂类型展开, 仅保留接口
    // OZ (user_type->add_package_routine_schema_param(resolve_ctx, block_ns, package_name,
    //              param_name, mode, position, level, sequence, routine_info));
  }
  return ret;
}

int ObPLDataType::get_external_user_type(const ObPLResolveCtx &resolve_ctx,
                                         const ObUserDefinedType *&user_type) const
{
  int ret = OB_SUCCESS;
  OZ (resolve_ctx.get_user_type(get_user_type_id(), user_type, &resolve_ctx.allocator_));
  CK (OB_NOT_NULL(user_type));
  OV (common::is_dblink_type_id(get_user_type_id())
      || is_package_type() == user_type->is_package_type()
      || is_udt_type() == user_type->is_udt_type()
      || is_rowtype_type() == user_type->is_rowtype_type()
      || is_type_type() == user_type->is_type_type(),
    OB_ERR_UNEXPECTED, KPC(this), KPC(user_type));
  return ret;
}

int ObPLDataType::get_all_depended_user_type(const ObPLResolveCtx &resolve_ctx,
                                             const ObPLBlockNS &current_ns) const
{
  int ret = OB_SUCCESS;
  if (is_obj_type()) {
    //do nothing
  } else {
    uint64_t user_type_id = get_user_type_id();
    const ObUserDefinedType *user_type = NULL;
    if (OB_ISNULL(user_type = current_ns.get_type_table()->get_type(user_type_id))) {
      if (OB_ISNULL(user_type = current_ns.get_type_table()->get_external_type(user_type_id))) {
        if (OB_ISNULL(current_ns.get_external_ns())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("external ns is null", K(ret), K(user_type_id), KPC(this));
        } else if (OB_ISNULL(current_ns.get_external_ns()->get_parent_ns())) {
          OZ (get_external_user_type(resolve_ctx, user_type));
          CK (OB_NOT_NULL(user_type));
        } else {
          const ObPLBlockNS *parent_ns = current_ns.get_external_ns()->get_parent_ns();
          if (OB_FAIL(parent_ns->get_pl_data_type_by_id(user_type_id, user_type))) {
            LOG_WARN("get user type failed", K(ret), K(user_type_id), KPC(user_type), KPC(this));
          } else if (OB_ISNULL(user_type)) {
            OZ (get_external_user_type(resolve_ctx, user_type));
            CK (OB_NOT_NULL(user_type));
          }
        }
        if (OB_SUCC(ret)) {
          ObSEArray<ObDataType, 8> types;
          if (OB_FAIL(current_ns.expand_data_type(user_type, types))) {
            LOG_WARN("failed to expand data type", K(ret), KPC(user_type));
          } else if (OB_FAIL(current_ns.get_type_table()->add_external_type(user_type))) {
            LOG_WARN("add user type table failed", K(ret), KPC(user_type), KPC(this));
          }
        }
      }
    }

    OZ (SMART_CALL(user_type->get_all_depended_user_type(resolve_ctx, current_ns)));
  }
  return ret;
}

int ObPLDataType::set_type_info(const ObIArray<common::ObString>& type_info)
{
  int ret = OB_SUCCESS;
  bool found = false;
  uint16_t type_info_id;
  if (0 == type_info.count()) { //does not need to set type_info_id
    type_info_id_ = OB_INVALID_ID;
  } else if (OB_ISNULL(enum_set_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("enum_set_ctx_ is null", K(ret), K(this), K(type_info));
  } else if ( !enum_set_ctx_->is_inited() && OB_FAIL(enum_set_ctx_->init())) {
    LOG_WARN("enum type info ctx init failed", K(ret));
  } else if (OB_FAIL(enum_set_ctx_->get_type_info_id(&type_info, type_info_id))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("failed to get enum type info id by type info", K(ret), K(type_info));
    } else { //set a new enum type info
      ret = OB_SUCCESS;
      ObIArray<common::ObString>* dst_type_info = NULL;
      if (OB_FAIL(enum_set_ctx_->deep_copy_type_info(enum_set_ctx_->get_allocator(),
                                                         dst_type_info,
                                                         type_info))) {
        LOG_WARN("failed to deep copy type info");
      } else if (OB_FAIL(enum_set_ctx_->get_new_enum_type_info_id(type_info_id))) {
        LOG_WARN("failed to get new enum type info id", K(ret));
      } else if (OB_FAIL(enum_set_ctx_->set_enum_type_info(type_info_id, dst_type_info))) {
        LOG_WARN("failed to set new enum type info", K(ret));
      } else {
        type_info_id_ = type_info_id;
      }
    }
  } else { // type_info already exists
    type_info_id_ = type_info_id;
  }
  return ret;
}

int ObPLDataType::get_type_info(ObIArray<common::ObString> *&type_info) const
{
  int ret = OB_SUCCESS;
  if (is_enum_or_set_type()) {
    if (NULL == enum_set_ctx_ || !enum_set_ctx_->is_inited()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("enum_set_ctx is null or not inited", K(ret), KPC(enum_set_ctx_));
    } else if (OB_INVALID_ID == type_info_id_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid type_info_id_", K(ret), K(type_info_id_));
    } else if (OB_FAIL(enum_set_ctx_->get_enum_type_info(type_info_id_, type_info))) {
      LOG_WARN("failed to get enum type info", K(ret), K(type_info_id_));
    }
  } else {
    type_info = NULL;
  }

  return ret;
}

int ObPLDataType::deep_copy_pl_type(
  ObIAllocator &allocator, const ObPLDataType &src, ObPLDataType *&dst)
{
  int ret = OB_SUCCESS;

#define COPY_COMMON \
  OZ (dst_type->deep_copy(allocator, *src_type));

#define DEEP_COPY_TYPE(type, class, copy_func)                          \
case type: {                                                            \
  class *dst_type = NULL;                                               \
  void *ptr = allocator.alloc(sizeof(class));                           \
  if (OB_ISNULL(ptr)) {                                                 \
    ret = OB_ALLOCATE_MEMORY_FAILED;                                    \
    LOG_WARN("failed to alloc memory for pl type", K(ret));             \
  } else {                                                              \
    const class *src_type = dynamic_cast<const class *>(&src);          \
    if (OB_ISNULL(src_type)) {                                          \
      ObPLDataType *dst_type = NULL;                                    \
      OX (dst_type = new (ptr) ObPLDataType());                         \
      OZ (dst_type->deep_copy(allocator, src));                        \
      OX (dst = dst_type);                                              \
    } else {                                                            \
      OX (dst_type = new(ptr)class());                                  \
      copy_func;                                                        \
      OX (dst = static_cast<ObPLDataType *>(dst_type));                 \
    }                                                                   \
  }                                                                     \
} break;

  switch (src.get_type())
  {
    DEEP_COPY_TYPE(PL_OBJ_TYPE, ObPLDataType, COPY_COMMON);
    DEEP_COPY_TYPE(PL_RECORD_TYPE, ObRecordType, COPY_COMMON);
    DEEP_COPY_TYPE(PL_CURSOR_TYPE, ObRefCursorType, COPY_COMMON);
    case PL_INTEGER_TYPE: /*do nothing*/ break;
#ifdef OB_BUILD_ORACLE_PL
    DEEP_COPY_TYPE(PL_NESTED_TABLE_TYPE, ObNestedTableType, COPY_COMMON);
    DEEP_COPY_TYPE(PL_ASSOCIATIVE_ARRAY_TYPE, ObAssocArrayType, COPY_COMMON);
    DEEP_COPY_TYPE(PL_VARRAY_TYPE, ObVArrayType, COPY_COMMON);
    DEEP_COPY_TYPE(PL_SUBTYPE, ObUserDefinedSubType, COPY_COMMON);
    DEEP_COPY_TYPE(PL_REF_CURSOR_TYPE, ObRefCursorType, COPY_COMMON);
    case PL_OPAQUE_TYPE: /*do nothing*/ break;
#endif
    default: {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("type for anytype is not supported", K(ret), K(src));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "type for anytype");
    } break;
  }

#undef DEEP_COPY_TYPE
#undef COPY_RECORD
#undef COPY_COMMON

  return ret;
}

int ObPLDataType::obj_is_null(ObObj& obj, bool &is_null)
{
  int ret = OB_SUCCESS;

  is_null = false;

  if (obj.is_null()) {
    is_null = true;
  } else if (obj.is_ext()) {
    uint64_t ext = obj.get_ext();
    switch (obj.get_meta().get_extend_type()) {
#ifdef OB_BUILD_ORACLE_PL
      case pl::PL_NESTED_TABLE_TYPE:
      case pl::PL_ASSOCIATIVE_ARRAY_TYPE:
      case pl::PL_VARRAY_TYPE: {
        pl::ObPLCollection *collection = reinterpret_cast<pl::ObPLCollection*>(ext);
        is_null = OB_ISNULL(collection) ? true : collection->is_collection_null();
        break;
      }
      case pl::PL_OPAQUE_TYPE: {
        pl::ObPLOpaque *opaque = reinterpret_cast<pl::ObPLOpaque *>(ext);
        is_null = OB_ISNULL(opaque) ? true : opaque->is_invalid();
        break;
      }
      case pl::PL_CURSOR_TYPE:
      case pl::PL_REF_CURSOR_TYPE: {
        is_null = obj.get_ext() == 0;
        break;
      }
  #endif
      case pl::PL_RECORD_TYPE: {
        pl::ObPLRecord *rec = reinterpret_cast<pl::ObPLRecord *>(ext);
        is_null = rec->is_null();
        break;
      }
      default: {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("check complex value is null not supported", K(ret), K(obj));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "check complex is null");
        break;
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("check obj is null unexcepted error", K(ret), K(obj));
  }
  return ret;
}

int ObPLDataType::datum_is_null(ObDatum* param, bool is_udt_type, bool &is_null)
{
  int ret = OB_SUCCESS;

  OV(OB_NOT_NULL(param), OB_ERR_UNEXPECTED);

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (!is_udt_type) {
    is_null = param->is_null();
  } else if (param->is_null() || param->extend_obj_->is_null()) {
    is_null = true;
  } else {
    if (OB_FAIL(ObPLDataType::obj_is_null(*(ObObj*)param->extend_obj_, is_null))) {
      LOG_WARN("check obj is null failed", K(ret), K(param->extend_obj_));
    }
  }
  return ret;
}

int ObPLEnumSetCtx::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("enum type info ctx already inited", K(ret), K(*this));
  } else {
    if (OB_FAIL(enum_type_info_reverse_map_.create(common::hash::cal_next_prime(32), ObModIds::OB_HASH_BUCKET, ObModIds::OB_HASH_NODE))) {
      LOG_WARN("failed to create enum_type_info_reverse_map_", K(ret));
    } else {
      is_inited_ = true;
      used_type_info_id_ = 0;
    }
  }
  return ret;
}

void ObPLEnumSetCtx::reset() {
  if (is_inited_) {
    enum_type_info_array_.destroy();
    enum_type_info_reverse_map_.destroy();
    is_inited_ = false;
    used_type_info_id_ = 0;
    LOG_DEBUG("enum type info ctx reset", KP(this), K(lbt()));
  }
}

int ObPLEnumSetCtx::get_new_enum_type_info_id(uint16_t &type_info_id)
{
  int ret = OB_SUCCESS;
  if (UINT16_MAX == used_type_info_id_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("more then 64K different enum type info", K(ret));
  } else {
    type_info_id = used_type_info_id_++;
  }
  return ret;
}

int ObPLEnumSetCtx::ensure_array_capacity(const uint16_t count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(count >= enum_type_info_array_.get_capacity()) &&
      OB_FAIL(enum_type_info_array_.reserve(next_pow2(count)))) {
    LOG_WARN("fail to reserve array capacity", K(ret), K(count), K(enum_type_info_array_));
  } else if (OB_FAIL(enum_type_info_array_.prepare_allocate(count))) {
    LOG_WARN("fail to prepare allocate array", K(ret), K(count), K(enum_type_info_array_));
  }
  return ret;
}

int ObPLEnumSetCtx::get_type_info_id(const ObIArray<common::ObString>* type_info, uint16_t &type_info_id)
{
  int ret = OB_SUCCESS;
  uint64_t temp_type_info_id;
  ObPLEnumSetCtx::ObPLTypeInfoKey enum_type_info_key((ObIArray<common::ObString>*)type_info);
  if (OB_FAIL(enum_type_info_reverse_map_.get_refactored(enum_type_info_key, temp_type_info_id))) {
    LOG_WARN("failed to get enum_type_info id", K(ret), K(type_info));
  } else {
    type_info_id = temp_type_info_id;
  }
  return ret;
}

int ObPLEnumSetCtx::set_enum_type_info(uint16_t type_info_id, ObIArray<common::ObString>* type_info)
{
  int ret = OB_SUCCESS;
  ObIArray<common::ObString>* temp_value;
  ObPLEnumSetCtx::ObPLTypeInfoKey enum_type_info_key(type_info);
  if (OB_FAIL(ensure_array_capacity(type_info_id + 1))) {
    LOG_WARN("failed to ensure array capacity", K(ret));
  }  else if (OB_FAIL(enum_type_info_reverse_map_.set_refactored(enum_type_info_key, type_info_id))) {
    if (OB_HASH_EXIST == ret) {
      ret = OB_SUCCESS;
      enum_type_info_array_.at(type_info_id) = type_info;
    } else {
      LOG_WARN("set enum type info reverse map failed", K(ret), K(type_info), K(type_info_id));
    }
  } else {
    enum_type_info_array_.at(type_info_id) = type_info;
  }
  return ret;
}

int ObPLEnumSetCtx::get_enum_type_info(uint16_t type_info_id, ObIArray<common::ObString> *&type_info) const
{
  int ret = OB_SUCCESS;
  CK (type_info_id < enum_type_info_array_.count());
  OX (type_info = enum_type_info_array_.at(type_info_id));
  return ret;
}

int ObPLEnumSetCtx::deep_copy_type_info(common::ObIAllocator &allocator,
                                           common::ObIArray<common::ObString>* &dst_type_info,
                                           const common::ObIArray<common::ObString>& type_info)
{
  int ret = OB_SUCCESS;
  void *mem = NULL;
  ObFixedArray<ObString, ObIAllocator> *type_info_value = NULL;
  if (OB_ISNULL(mem = allocator.alloc(sizeof(ObFixedArray<ObString, ObIAllocator>)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc type info", K(ret));
  } else {
    type_info_value = new(mem) ObFixedArray<ObString, ObIAllocator>(allocator);
    if (OB_FAIL(type_info_value->init(type_info.count()))) {
      LOG_WARN("fail to init array", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < type_info.count(); ++i) {
        const ObString &info = type_info.at(i);
        ObString dst_info;
        if OB_FAIL(ob_write_string(allocator, info, dst_info)) {
          LOG_WARN("failed to write string", K(info), K(ret));
        } else if (OB_FAIL(type_info_value->push_back(dst_info))) {
          LOG_WARN("fail to push back info", K(i), K(dst_info), K(ret));
        }
      }
    }
    OX (dst_type_info = type_info_value);
  }
  return ret;
}

ObObjAccessIdx::ObObjAccessIdx(const ObPLDataType &elem_type,
                               AccessType access_type,
                               const common::ObString &var_name,
                               const ObPLDataType &var_type,
                               int64_t value
                               )
  : elem_type_(elem_type),
    access_type_(access_type),
    var_name_(var_name),
    var_type_(var_type),
    var_index_(common::OB_INVALID_INDEX),
    routine_info_(NULL),
    get_sysfunc_(NULL)
{
  if (AccessType::IS_EXPR == access_type
      || AccessType::IS_UDF_NS == access_type) {
    get_sysfunc_ = reinterpret_cast<sql::ObRawExpr *>(value);
    var_index_ = var_type.get_user_type_id();
  } else if (AccessType::IS_INTERNAL_PROC == access_type
          || AccessType::IS_EXTERNAL_PROC == access_type
          || AccessType::IS_NESTED_PROC == access_type) {
    routine_info_ = reinterpret_cast<share::schema::ObIRoutineInfo *>(value);
  } else {
    var_index_ = value;
  }
}

int ObObjAccessIdx::deep_copy(common::ObIAllocator &allocator, sql::ObRawExprFactory &expr_factory, const ObObjAccessIdx &src)
{
  int ret = OB_SUCCESS;
  elem_type_ = src.elem_type_;
  access_type_ = src.access_type_;
  var_type_ = src.var_type_;
  var_index_ = src.var_index_;
  routine_info_ = src.routine_info_;
  type_method_params_ = src.type_method_params_;
  if (OB_FAIL(ob_write_string(allocator, src.var_name_, var_name_))) {
    PL_LOG(WARN, "failed to write string", K(var_name_), K(ret));
  } else if (OB_FAIL(ObPLExprCopier::copy_expr(expr_factory, src.get_sysfunc_, get_sysfunc_))) {
    PL_LOG(WARN, "failed to copy expr", K(var_name_), K(ret));
  } else { /*do nothing*/ }
  return ret;
}

void ObObjAccessIdx::reset()
{
  elem_type_.reset();
  access_type_ = IS_INVALID;
  var_name_.reset();
  var_type_.reset();
  var_index_ = common::OB_INVALID_INDEX;
  routine_info_ = NULL;
  type_method_params_.reset();
  get_sysfunc_ = NULL;
}

bool ObObjAccessIdx::operator==(const ObObjAccessIdx &other) const
{
  // udf deterministic default value is false, we need display setting check_ctx.need_check_deterministic_
  ObExprEqualCheckContext check_ctx;
  check_ctx.need_check_deterministic_ = false;
  return elem_type_ == other.elem_type_
      && access_type_ == other.access_type_
      && 0 == var_name_.case_compare(other.var_name_)
      && var_type_ == other.var_type_
      && var_index_ == other.var_index_
      && routine_info_ == other.routine_info_
      && is_array_equal(type_method_params_, other.type_method_params_)
      && (get_sysfunc_ == other.get_sysfunc_
          || (NULL != get_sysfunc_ && NULL != other.get_sysfunc_ && get_sysfunc_->same_as(*other.get_sysfunc_, &check_ctx)));
}

bool ObObjAccessIdx::is_table(const common::ObIArray<ObObjAccessIdx> &access_idxs)
{
  return !access_idxs.empty()
         && ((1 == access_idxs.count()
              && ObObjAccessIdx::IS_TABLE_NS == access_idxs.at(0).access_type_)
            || (2 == access_idxs.count()
              && ObObjAccessIdx::IS_DB_NS == access_idxs.at(0).access_type_
              && ObObjAccessIdx::IS_TABLE_NS == access_idxs.at(1).access_type_));
}

bool ObObjAccessIdx::is_table_column(const common::ObIArray<ObObjAccessIdx> &access_idxs)
{
  return !access_idxs.empty()
         && ((2 == access_idxs.count()
              && ObObjAccessIdx::IS_TABLE_NS == access_idxs.at(0).access_type_
              && ObObjAccessIdx::IS_TABLE_COL == access_idxs.at(1).access_type_)
            || (3 == access_idxs.count()
              && ObObjAccessIdx::IS_DB_NS == access_idxs.at(0).access_type_
              && ObObjAccessIdx::IS_TABLE_NS == access_idxs.at(1).access_type_
              && ObObjAccessIdx::IS_TABLE_COL == access_idxs.at(2).access_type_));
}

bool ObObjAccessIdx::is_dblink_table(const common::ObIArray<ObObjAccessIdx> &access_idxs)
{
  return !access_idxs.empty()
         && ((1 == access_idxs.count()
              && ObObjAccessIdx::IS_DBLINK_PKG_NS == access_idxs.at(0).access_type_)
            || (2 == access_idxs.count()
               && ObObjAccessIdx::IS_DB_NS == access_idxs.at(0).access_type_
               && ObObjAccessIdx::IS_DBLINK_PKG_NS == access_idxs.at(1).access_type_));
}

bool ObObjAccessIdx::is_dblink_table_column(const common::ObIArray<ObObjAccessIdx> &access_idxs)
{
  return !access_idxs.empty()
         && ((2 == access_idxs.count()
              && ObObjAccessIdx::IS_DBLINK_PKG_NS == access_idxs.at(0).access_type_
              && ObObjAccessIdx::IS_TABLE_COL == access_idxs.at(1).access_type_)
            || (3 == access_idxs.count()
              && ObObjAccessIdx::IS_DB_NS == access_idxs.at(0).access_type_
              && ObObjAccessIdx::IS_DBLINK_PKG_NS == access_idxs.at(1).access_type_
              && ObObjAccessIdx::IS_TABLE_COL == access_idxs.at(2).access_type_));
}

bool ObObjAccessIdx::is_local_variable(const common::ObIArray<ObObjAccessIdx> &access_idxs)
{
  bool is_local = false;
  if (!access_idxs.empty()) {
    int i = 0;
    while (ObObjAccessIdx::IS_LABEL_NS == access_idxs.at(i).access_type_
           && i < access_idxs.count() - 1) {
      ++i;
    }
    is_local = ObObjAccessIdx::IS_LOCAL == access_idxs.at(i).access_type_;
  }
  return is_local;
}

bool ObObjAccessIdx::is_function_return_variable(
        const common::ObIArray<ObObjAccessIdx> &access_idxs)
{
  bool is_func_ret = false;
  for (int64_t i = 0; !is_func_ret && i < access_idxs.count(); ++i) {
    is_func_ret = access_idxs.at(i).is_udf_type();
  }
  return is_func_ret;
}

int64_t ObObjAccessIdx::get_local_variable_idx(const common::ObIArray<ObObjAccessIdx> &access_idxs)
{

  int64_t local_var_idx = OB_INVALID_INDEX;
  if (!access_idxs.empty()) {
    int i = 0;
    while (ObObjAccessIdx::IS_LABEL_NS == access_idxs.at(i).access_type_
           && i < access_idxs.count() - 1) {
      ++i;
    }
    if (ObObjAccessIdx::IS_LOCAL == access_idxs.at(i).access_type_) {
      local_var_idx = i;
    }
  }
  return local_var_idx;
}

int64_t ObObjAccessIdx::get_subprogram_idx(const common::ObIArray<ObObjAccessIdx> &access_idxs)
{
  int64_t subprogram_var_idx = OB_INVALID_INDEX;
  if (!access_idxs.empty()) {
    int i = 0;
    while (ObObjAccessIdx::IS_LABEL_NS == access_idxs.at(i).access_type_
           && i < access_idxs.count() - 1) {
      ++i;
    }
    if (ObObjAccessIdx::IS_SUBPROGRAM_VAR == access_idxs.at(i).access_type_) {
      subprogram_var_idx = i;
    }
  }
  return subprogram_var_idx;
}

bool ObObjAccessIdx::is_subprogram_variable(const common::ObIArray<ObObjAccessIdx> &access_idxs)
{
  bool is_subprogram_var = false;
  if (!access_idxs.empty()) {
    int i = 0;
    while (ObObjAccessIdx::IS_LABEL_NS == access_idxs.at(i).access_type_
           && i < access_idxs.count() - 1) {
      ++i;
    }
    is_subprogram_var = ObObjAccessIdx::IS_SUBPROGRAM_VAR == access_idxs.at(i).access_type_;
  }
  return is_subprogram_var;
}

bool ObObjAccessIdx::is_package_variable(const common::ObIArray<ObObjAccessIdx> &access_idxs)
{
  return (!access_idxs.empty() && ObObjAccessIdx::IS_PKG == access_idxs.at(0).access_type_)
      || (access_idxs.count() > 1
          && (ObObjAccessIdx::IS_PKG_NS == access_idxs.at(0).access_type_
            || ObObjAccessIdx::IS_LABEL_NS == access_idxs.at(0).access_type_)
          && ObObjAccessIdx::IS_PKG == access_idxs.at(1).access_type_)
      || (access_idxs.count() > 2
          && ObObjAccessIdx::IS_DB_NS == access_idxs.at(0).access_type_
          && (ObObjAccessIdx::IS_PKG_NS == access_idxs.at(1).access_type_
            || ObObjAccessIdx::IS_LABEL_NS == access_idxs.at(1).access_type_)
          && ObObjAccessIdx::IS_PKG == access_idxs.at(2).access_type_);
}

bool ObObjAccessIdx::is_get_variable(const common::ObIArray<ObObjAccessIdx> &access_idxs)
{
  bool result = false;
  if (1 == access_idxs.count()) {
    result = ObObjAccessIdx::IS_USER == access_idxs.at(0).access_type_
        || ObObjAccessIdx::IS_SESSION == access_idxs.at(0).access_type_
        || ObObjAccessIdx::IS_GLOBAL == access_idxs.at(0).access_type_;
  }
  return result;
}

bool ObObjAccessIdx::is_local_baisc_variable(
    const common::ObIArray<ObObjAccessIdx> &access_idxs)
{
  int64_t local_idx = get_local_variable_idx(access_idxs);
  return is_local_variable(access_idxs)
          && (access_idxs.count() - 1) == local_idx
          && local_idx >= 0
          && local_idx < access_idxs.count()
          && access_idxs.at(local_idx).elem_type_.is_obj_type();
}

bool ObObjAccessIdx::is_subprogram_basic_variable(
    const common::ObIArray<ObObjAccessIdx> &access_idxs)
{
  return is_subprogram_variable(access_idxs)
          && (access_idxs.count() - 1) == get_subprogram_idx(access_idxs)
          && OB_INVALID_INDEX != get_subprogram_idx(access_idxs)
          && access_idxs.at(get_subprogram_idx(access_idxs)).elem_type_.is_obj_type();
}

bool ObObjAccessIdx::is_local_refcursor_variable(
    const common::ObIArray<ObObjAccessIdx> &access_idxs)
{
  return is_local_variable(access_idxs)
      && 1 == access_idxs.count()
      && access_idxs.at(0).elem_type_.is_cursor_type();
}

bool ObObjAccessIdx::is_local_cursor_variable(
  const common::ObIArray<ObObjAccessIdx> &access_idxs)
{
  return is_local_variable(access_idxs)
      && 1 == access_idxs.count()
      && access_idxs.at(0).elem_type_.is_cursor_type()
      && access_idxs.at(0).elem_type_.is_local_type();
}

bool ObObjAccessIdx::is_subprogram_cursor_variable(
  const common::ObIArray<ObObjAccessIdx> &access_idxs)
{
  return is_subprogram_variable(access_idxs)
      && 1 == access_idxs.count()
      && access_idxs.at(0).elem_type_.is_cursor_type();
}

bool ObObjAccessIdx::is_package_cursor_variable(
  const common::ObIArray<ObObjAccessIdx> &access_idxs)
{
  bool b_ret = false;
  if (!access_idxs.empty()
      && access_idxs.count() < 4
      && is_package_variable(access_idxs)
      && ObObjAccessIdx::IS_PKG == access_idxs.at(access_idxs.count() - 1).access_type_
      && access_idxs.at(access_idxs.count() - 1).elem_type_.is_cursor_type()) {
    //special case: table(var), table and var both pkg type
    if (access_idxs.count() <= 1
        || ObObjAccessIdx::IS_PKG != access_idxs.at(access_idxs.count() - 2).access_type_) {
      b_ret = true;
    }
  }
  return b_ret;
}

bool ObObjAccessIdx::is_package_baisc_variable(
    const common::ObIArray<ObObjAccessIdx> &access_idxs)
{
  bool b_ret = false;
  if (!access_idxs.empty()
      && access_idxs.count() < 4
      && is_package_variable(access_idxs)
      && ObObjAccessIdx::IS_PKG == access_idxs.at(access_idxs.count() - 1).access_type_
      && access_idxs.at(access_idxs.count() - 1).elem_type_.is_obj_type()) {
    //special case: table(var), table and var both pkg type
    if (access_idxs.count() <= 1
        || ObObjAccessIdx::IS_PKG != access_idxs.at(access_idxs.count() - 2).access_type_) {
      b_ret = true;
    }
  }
  return b_ret;
}

bool ObObjAccessIdx::is_local_type(
  const common::ObIArray<ObObjAccessIdx> &access_idxs)
{
  return access_idxs.count() > 0
          && access_idxs.at(access_idxs.count() - 1).is_local_type();
}

bool ObObjAccessIdx::is_pkg_type(
  const common::ObIArray<ObObjAccessIdx> &access_idxs)
{
  return access_idxs.count() > 0
          && access_idxs.at(access_idxs.count() - 1).is_pkg_type();
}

bool ObObjAccessIdx::is_udt_type(
  const common::ObIArray<ObObjAccessIdx> &access_idxs)
{
  return access_idxs.count() > 0
          && access_idxs.at(access_idxs.count() - 1).is_udt_type();
}

bool ObObjAccessIdx::is_external_type(
  const common::ObIArray<ObObjAccessIdx> &access_idxs)
{
  return is_pkg_type(access_idxs) || is_udt_type(access_idxs);
}

bool ObObjAccessIdx::is_type(
  const common::ObIArray<ObObjAccessIdx> &access_idxs)
{
  return is_local_type(access_idxs) || is_pkg_type(access_idxs) || is_udt_type(access_idxs);
}

bool ObObjAccessIdx::is_expr_type(
  const common::ObIArray<ObObjAccessIdx> &access_idxs)
{
  return access_idxs.count() > 0
          && access_idxs.at(access_idxs.count() - 1).is_expr();
}

const ObPLDataType &ObObjAccessIdx::get_final_type(const common::ObIArray<ObObjAccessIdx> &access_idxs)
{
  return access_idxs.at(access_idxs.count() - 1).elem_type_;
}

int ObObjAccessIdx::get_package_id(const ObIArray<ObObjAccessIdx>& access_idxs,
                                   uint64_t &package_id,
                                   uint64_t &var_idx)
{
  int ret = OB_SUCCESS;
#define GET_CONST_EXPR_VALUE(expr, val) \
do {  \
  const ObConstRawExpr *c_expr = static_cast<const ObConstRawExpr*>(expr); \
  CK (OB_NOT_NULL(c_expr)); \
  CK (c_expr->get_value().is_uint64() \
      || c_expr->get_value().is_int()); \
  OX (val = c_expr->get_value().is_uint64() ? c_expr->get_value().get_uint64() \
        : c_expr->get_value().get_int()); \
} while (0)

  CK (!access_idxs.empty());
  CK (is_package_variable(access_idxs));
  if (OB_FAIL(ret)) {
  } else if (ObObjAccessIdx::IS_PKG == access_idxs.at(0).access_type_) {
    const ObObjAccessIdx& access_idx = access_idxs.at(0);
    const ObSysFunRawExpr *f_expr = static_cast<const ObSysFunRawExpr  *>(access_idx.get_sysfunc_);
    CK (OB_NOT_NULL(f_expr) && f_expr->get_param_count() >= 2);
    CK (T_OP_GET_PACKAGE_VAR == f_expr->get_expr_type());
    GET_CONST_EXPR_VALUE(f_expr->get_param_expr(0), package_id);
    GET_CONST_EXPR_VALUE(f_expr->get_param_expr(1), var_idx);
  } else if (is_package_variable(access_idxs)) {
    for (int64_t i = 0; i < access_idxs.count(); ++i) {
      const ObObjAccessIdx& access_idx = access_idxs.at(i);
      if (ObObjAccessIdx::IS_PKG_NS == access_idx.access_type_) {
        package_id = access_idx.var_index_;
        LOG_DEBUG("success to get package id", K(package_id), K(access_idxs), K(i));
      } else if (ObObjAccessIdx::IS_LABEL_NS == access_idx.access_type_) {
        CK (OB_NOT_NULL(access_idx.label_ns_));
        OX (package_id = access_idx.label_ns_->get_package_id());
        LOG_DEBUG("success to get package id from label ns", K(package_id), K(access_idxs), K(i));
      } else if (ObObjAccessIdx::IS_PKG == access_idx.access_type_) {
        var_idx = access_idx.var_index_;
        LOG_DEBUG("success to get package variable index",
                  K(package_id), K(access_idxs), K(i));
        break;
      }
    }
  }

#undef GET_CONSTANT_EXPR_VALUE

  return ret;
}

int ObObjAccessIdx::get_package_id(
  const ObRawExpr *expr, uint64_t& package_id, uint64_t *p_var_idx)
{
  int ret = OB_SUCCESS;
  package_id = OB_INVALID_ID;
  if (expr->is_sys_func_expr() && T_OP_GET_PACKAGE_VAR == expr->get_expr_type()) {
    const ObSysFunRawExpr *f_expr = static_cast<const ObSysFunRawExpr  *>(expr);
    const ObConstRawExpr *c_expr1 = NULL;
    CK (OB_NOT_NULL(f_expr));
    CK (OB_NOT_NULL(f_expr->get_param_expr(0)));
    CK (f_expr->get_param_expr(0)->is_const_raw_expr());
    CK (OB_NOT_NULL(c_expr1 = static_cast<const ObConstRawExpr*>(f_expr->get_param_expr(0))));
    CK (T_UINT64 == c_expr1->get_expr_type());
    CK (c_expr1->get_value().is_uint64());
    OX (package_id = c_expr1->get_value().get_uint64());
    if (OB_SUCC(ret) && OB_NOT_NULL(p_var_idx)) {
      const ObConstRawExpr *c_expr2 = NULL;
      CK (OB_NOT_NULL(f_expr->get_param_expr(1)));
      CK (f_expr->get_param_expr(1)->is_const_raw_expr());
      CK (OB_NOT_NULL(c_expr2 = static_cast<const ObConstRawExpr*>(f_expr->get_param_expr(1))));
      CK (T_INT == c_expr2->get_expr_type());
      CK (c_expr2->get_value().is_int());
      OX (*p_var_idx = c_expr2->get_value().get_int());;
    }
    LOG_DEBUG("success to get package id", K(ret), K(package_id));
  } else if (expr->is_obj_access_expr()) {
    uint64_t var_idx = OB_INVALID_ID;
    const ObObjAccessRawExpr *access_expr = static_cast<const ObObjAccessRawExpr *>(expr);
    OZ (get_package_id(
      access_expr->get_access_idxs(), package_id, var_idx));
    OX (p_var_idx != NULL ? *p_var_idx = var_idx : (uint64_t)NULL);
  }
  if (OB_INVALID_ID == package_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get package id", K(ret), K(package_id), KPC(expr));
  }
  return ret;
}

bool ObObjAccessIdx::has_same_collection_access(const ObRawExpr *expr, const ObObjAccessRawExpr *access_expr)
{
  bool ret = false;
  if (expr->is_obj_access_expr()) {
    const ObIArray<ObObjAccessIdx> &left = static_cast<const ObObjAccessRawExpr *>(expr)->get_access_idxs();
    const ObIArray<ObObjAccessIdx> &right= access_expr->get_access_idxs();
    for (int64_t i = 0; i < left.count() && i < right.count(); ++i) {
      if (!(left.at(i) == right.at(i))) {
        break;
      } else if (left.at(i).elem_type_.is_collection_type()) {
        ret = true;
        break;
      }
    }
  }
  return ret;
}

bool ObObjAccessIdx::has_collection_access(const ObRawExpr *expr)
{
  bool ret = false;
  if (OB_NOT_NULL(expr)) {
    if (expr->is_obj_access_expr()) {
      const ObObjAccessRawExpr *access_expr = static_cast<const ObObjAccessRawExpr*>(expr);
      for (int64_t i = access_expr->get_access_idxs().count() - 1; i >= 0; --i) {
        if (access_expr->get_access_idxs().at(i).elem_type_.is_collection_type()) {
          ret = true;
          break;
        }
      }
    } else {
      for (int64_t i = 0; i < expr->get_param_count(); ++i) {
        if (has_collection_access(expr->get_param_expr(i))) {
          ret = true;
          break;
        }
      }
    }
  }
  return ret;
}

int ObObjAccessIdx::datum_need_copy(const ObRawExpr *into, const ObRawExpr *value, AccessType &alloc_scop)
{
  int ret = OB_SUCCESS;
  alloc_scop = IS_INVALID;
  if (OB_ISNULL(into)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid exor", K(into), K(value), K(ret));
  } else {
    /*
     * 如果我们的表达式计算能够保证计算的结果一定不会复用输入表达式的内存，那么其实不需要做任何拷贝。
     * 如果我们的表达式计算不保证这一点，例如如果CAST的类型和输入类型一致，那么其实返回结果还是原始表达式，那么需要拷贝内存。
     * 但是我们没有办法区分哪些表达式是重新申请了内存，哪些是用的原来的内存。简单点的办法，如果涉及到Collection那么就COPY？？？
     */

    //如果目的端是包变量，那么一定需要copy
    if (OB_SUCC(ret) && IS_INVALID == alloc_scop) {
      if (into->is_sys_func_expr() && T_OP_GET_PACKAGE_VAR == into->get_expr_type()) {
        alloc_scop = IS_PKG; //TODO: @ryan.ly PKG
      } else if (into->is_obj_access_expr()) {
        const ObObjAccessRawExpr *access_expr = static_cast<const ObObjAccessRawExpr*>(into);
        if (is_package_variable(access_expr->get_access_idxs())) {
          alloc_scop = IS_PKG;
        }
      } else { /*do nothing*/ }
    }

    //如果源数据来源于NestedTable，那么一定要重新copy
    if (OB_SUCC(ret)
        && IS_INVALID == alloc_scop
        && OB_NOT_NULL(value)
        && has_collection_access(value)) {
      alloc_scop = IS_LOCAL;
    }

    //如果目的端是NestedTable，那么一定要重新copy
    if (OB_SUCC(ret)
        && IS_INVALID == alloc_scop
        && into->is_obj_access_expr()
        && has_collection_access(into)) {
      alloc_scop = IS_LOCAL;
    }

    //如果源数据和目的端不同属于一个Allocator Scope，那么也要copy
    if (OB_SUCC(ret) && IS_INVALID == alloc_scop) {
      bool src_pkg = false;
      bool dest_local = false;
      if (OB_NOT_NULL(value) && value->is_obj_access_expr()) {
        const ObObjAccessRawExpr *access_value = static_cast<const ObObjAccessRawExpr*>(value);
        if (is_package_variable(access_value->get_access_idxs())) {
          src_pkg = true;
        }
      } else if (OB_NOT_NULL(value)
                 && value->is_sys_func_expr()
                 && T_OP_GET_PACKAGE_VAR == value->get_expr_type()) { //TODO: PKG
        src_pkg = true;
      } else { /*do nothing*/ }

      if (into->is_obj_access_expr()) {
        const ObObjAccessRawExpr *access_into = static_cast<const ObObjAccessRawExpr*>(into);
        if (is_local_variable(access_into->get_access_idxs())) {
          dest_local = true;
        }
      } else if (into->is_const_raw_expr()) {
        dest_local = true;
      } else { /*do nothing*/ }

      alloc_scop = src_pkg && dest_local ? IS_LOCAL : IS_INVALID;
    }
  }
  return ret;
}

bool ObObjAccessIdx::is_contain_object_type(const common::ObIArray<ObObjAccessIdx> &access_idxs)
{
  bool b_ret = false;
  for (int64_t i = 0; !b_ret && i < access_idxs.count(); ++i) {
    b_ret = access_idxs.at(i).elem_type_.is_object_type();
  }
  return b_ret;
}

int ObPLCursorInfo::set_and_register_snapshot(const transaction::ObTxReadSnapshot &snapshot)
{
  int ret = OB_SUCCESS;
  OZ (set_snapshot(snapshot));
  if (OB_SUCC(ret) && snapshot.is_valid()) {
    set_need_check_snapshot(true);
    OZ (MTL(transaction::ObTransService*)->register_tx_snapshot_verify(get_snapshot()));
  }
  return ret;
}

int ObPLCursorInfo::deep_copy(ObPLCursorInfo &src, common::ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  common::ObIAllocator *copy_allocator = allocator;
  if (NULL == copy_allocator) {
    copy_allocator = get_allocator();
  }
  if (is_streaming_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("streaming cursor can not be copy", K(src), K(ret));
  } else {
    ObSPICursor *src_cursor = src.get_spi_cursor();
    ObSPICursor *dest_cursor = NULL;
    CK (OB_NOT_NULL(copy_allocator));
    CK (OB_NOT_NULL(src_cursor));
    if (OB_SUCC(ret)) {
      id_ = src.id_;
      is_explicit_ = src.is_explicit_;
      for_update_ = src.for_update_;
      has_hidden_rowid_ = src.has_hidden_rowid_;
      is_streaming_ = src.is_streaming_;
      isopen_ = src.isopen_;
      fetched_ = src.fetched_;
      fetched_with_row_ = src.fetched_with_row_;
      rowcount_  = src.rowcount_;
      current_position_ = src.current_position_;
      in_forall_ = src.in_forall_;
      save_exception_ = src.save_exception_;
      forall_rollback_ = src.forall_rollback_;
      trans_id_ = src.trans_id_;
      is_scrollable_ = src.is_scrollable_;
      OZ (snapshot_.assign(src.snapshot_));
      is_need_check_snapshot_ = src.is_need_check_snapshot_;
      last_execute_time_ = src.last_execute_time_;
      sql_trace_id_ = src.sql_trace_id_;
      //these should not be copied ..
      //    lib::MemoryContext entity_;
      //    ObIAllocator *allocator_;
    }
    //copy row store
    if (OB_SUCC(ret)) {
      const ObNewRow *row = NULL;
      int64_t cur = 0;
      // it will happend not in ps cursor.
      OZ (prepare_spi_cursor(dest_cursor,
                              src_cursor->row_store_.get_tenant_id(),
                              src_cursor->row_store_.get_mem_limit(),
                              false,
                              src_cursor->session_info_));
      CK (OB_NOT_NULL(dest_cursor));
      if (OB_SUCC(ret)) {
        OZ (dest_cursor->row_desc_.assign(src_cursor->row_desc_));
    #ifdef OB_BUILD_ORACLE_PL
        if (OB_SUCC(ret) && src_cursor->fields_.count() > 0) {
          OZ (ObDbmsCursorInfo::deep_copy_field_columns(*copy_allocator,
                                                        &(src_cursor->fields_),
                                                        dest_cursor->fields_));
        }
    #endif
        OX (dest_cursor->cur_ = src_cursor->cur_);

        while (OB_SUCC(ret) && cur < src_cursor->row_store_.get_row_cnt()) {
          if (OB_FAIL(src_cursor->row_store_.get_row(cur, row))) {
            //do nothing
          } else if (OB_ISNULL(row)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("row is null", K(ret));
          } else {
            ObNewRow tmp_row = *row;
            for (int64_t i = 0; OB_SUCC(ret) && i < tmp_row.get_count(); ++i) {
              ObObj& obj = tmp_row.get_cell(i);
              ObObj tmp;
              if (obj.is_pl_extend()) {
                if (OB_FAIL(pl::ObUserDefinedType::deep_copy_obj(*(dest_cursor->allocator_), obj, tmp))) {
                  LOG_WARN("failed to copy pl extend", K(ret));
                } else {
                  obj = tmp;
                  dest_cursor->complex_objs_.push_back(tmp);
                }
              }
            }
            if (OB_SUCC(ret)) {
              if (OB_FAIL(dest_cursor->row_store_.add_row(tmp_row))) {
                LOG_WARN("failed to add row to row store", K(ret));
              } else {
                ++cur;
              }
            }
          }
        }

        OX (dest_cursor->row_store_.finish_add_row());

        OZ (ob_write_row(*copy_allocator, src.current_row_, current_row_));
        OZ (ob_write_row(*copy_allocator, src.first_row_, first_row_));
        OZ (ob_write_row(*copy_allocator, src.last_row_, last_row_));
        OZ (bulk_rowcount_.assign(src.bulk_rowcount_));
        OZ (bulk_exceptions_.assign(src.bulk_exceptions_));

        if (OB_FAIL(ret)) {
          common::ObIAllocator *old_allocator = get_allocator();
          dest_cursor->~ObSPICursor();
          this->~ObPLCursorInfo();
          new(this) ObPLCursorInfo(old_allocator);
          this->reset();
          this->set_ref_count(1);
        }
      }
    }
  }

  return ret;
}

int ObPLCursorInfo::close(sql::ObSQLSessionInfo &session, bool is_reuse)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("close cursor", K(isopen()), K(id_), K(this), K(*this), K(session.get_sessid()));
  if (isopen()) { //如果游标已经打开，需要释放资源
    if (is_streaming()) {
      ObSPIResultSet *spi_result = get_cursor_handler();
      if (OB_NOT_NULL(spi_result)) {
        if (OB_NOT_NULL(spi_result->get_result_set())) {
          OZ (spi_result->set_cursor_env(session));
          int close_ret = spi_result->close_result_set();
          if (OB_SUCCESS != close_ret) {
            LOG_WARN("close mysql result set failed", K(ret), K(close_ret));
          }
          ret = (OB_SUCCESS == ret ? close_ret : ret);
          spi_result->destruct_exec_params(session);
          //spi_result->get_mysql_result().reset();
          int reset_ret = spi_result->reset_cursor_env(session);
          ret = (OB_SUCCESS == ret ? reset_ret : ret);
        }
        spi_result->~ObSPIResultSet();
      }
    } else {
      CK (OB_NOT_NULL(get_allocator()));
      if (OB_SUCC(ret) && OB_NOT_NULL(get_spi_cursor())) {
        get_spi_cursor()->~ObSPICursor();
        get_allocator()->free(get_spi_cursor());
        spi_cursor_ = nullptr;
      }
    }
#ifdef OB_BUILD_ORACLE_PL
    if (lib::is_oracle_mode()) {
      /* unregiter snapshot whether ret is succ or not*/
      MTL(transaction::ObTransService*)->unregister_tx_snapshot_verify(get_snapshot());
      get_snapshot().reset();
    }
#endif
  } else {
    LOG_INFO("NOTICE: cursor is closed without openning", K(*this), K(ret));
  }
  is_reuse ? reuse() : reset();
  return ret;
}

int ObPLCursorInfo::get_found(bool &found, bool &isnull) const
{
  int ret = OB_SUCCESS;
  if (is_explicit_) {
    if (!isopen_) {
      ret = OB_ER_SP_CURSOR_NOT_OPEN;
      LOG_WARN("cursor is not open", K(ret));
    } else if (!fetched_) {
      isnull = true;
    } else {
      found = fetched_with_row_;
    }
  } else {
    // 对于隐式游标
    // 在PL开始执行时将session上的ObPLCursor变量初始化,
    // 并且在PL中第一个DML开始时open,
    // 在整个PL执行过程中不close
    if (!isopen_) { // 说明当前PL中还没有执行过DML
      isnull = true;
    } else {
      found = 0 != rowcount_;
    }
  }
  return ret;
}

int ObPLCursorInfo::get_notfound(bool &notfound, bool &isnull) const
{
  int ret = OB_SUCCESS;
  bool found = false;
  if (OB_FAIL(get_found(found, isnull))) {
    LOG_WARN("get not found error", K(ret));
  } else if (!isnull) {
    notfound = !found;
  }
  return ret;
}

int ObPLCursorInfo::get_rowcount(int64_t &rowcount, bool &isnull) const
{
  int ret = OB_SUCCESS;
  if (is_explicit_) {
    if (!isopen_) {
      ret = OB_ER_SP_CURSOR_NOT_OPEN;
      LOG_WARN("cursor is not open", K(ret));
    } else {
      rowcount = rowcount_;
    }
  } else {
    if (!isopen_) {
      isnull = true;
    } else {
      rowcount = rowcount_;
    }
  }
  return ret;
}

int ObPLCursorInfo::set_rowcount(int64_t rowcount)
{
  int ret = OB_SUCCESS;
  if (is_explicit_) {
    if (!isopen_) {
      ret = OB_ER_SP_CURSOR_NOT_OPEN;
      LOG_WARN("cursor is not open", K(ret));
    } else {
      rowcount_ = rowcount;
    }
  } else {
    if (in_forall_) {
      if (OB_FAIL(add_bulk_row_count(rowcount))) {
        LOG_WARN("faield to add bulk rowcount", K(ret), K(rowcount));
      } else {
        rowcount_ += rowcount;
      }
    } else {
      rowcount_ = rowcount;
    }
    isopen_ = true; // 隐式游标用这个值来判断是否有执行过dml，因此每次set_rowcount都设置这个值
  }
  return ret;
}

int ObPLCursorInfo::get_rowid(ObString &rowid) const
{
  int ret = OB_SUCCESS;
  if (is_explicit_) {
    if (!fetched_) {
      ret = OB_ER_SP_CURSOR_NOT_OPEN;
      LOG_WARN("cursor is not fetched", K(*this), K(ret));
    } else if (!has_hidden_rowid_ || nullptr == current_row_.cells_) {
      ret = OB_INVALID_ROWID;
      LOG_WARN("cursor has no rowid", K(*this), K(ret));
    } else {
      rowid = current_row_.get_cell(current_row_.get_count() - 1).get_string();
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("implicit cursor has no rowid", K(ret));
  }
  return ret;
}

int ObPLCursorInfo::set_bulk_exception(int64_t error)
{
  int ret = OB_SUCCESS;
  if (!in_forall_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("can not set bulk exception when not in forall", K(ret), K(in_forall_));
  } else if (OB_FAIL(set_rowcount(0))) { // 将失败语句的行设置为0
    LOG_WARN("failed to set rowcount for bulk exception", K(ret));
  } else if (OB_FAIL(add_bulk_exception(bulk_rowcount_.count(), error))) {
    LOG_WARN("failed to set exception for bulk exception", K(ret));
  }
  if (OB_FAIL(ret)) { // 设置bulk exception失败后将退出for_all_环境，为了避免隐式游标的值不完整重置隐式游标
    reset();
  }
  return ret;
}

int ObPLCursorInfo::get_bulk_rowcount(int64_t index, int64_t &rowcount) const
{
  int ret = OB_SUCCESS;
  if (index < 0 || index >= bulk_rowcount_.count()) {
    ret = OB_ARRAY_OUT_OF_RANGE;
    LOG_WARN("bulk rowcount index is invalid", K(ret), K(index), K(bulk_rowcount_.count()));
  } else {
    rowcount = bulk_rowcount_.at(index);
  }
  return ret;
}

int ObPLCursorInfo::get_bulk_exception(int64_t index, bool need_code, int64_t &result) const
{
  int ret = OB_SUCCESS;
  if (index < 0 || index >= bulk_exceptions_.count()) {
    ret = OB_ARRAY_OUT_OF_RANGE;
    LOG_WARN("bulk exceptions index is invalid",
      K(ret), K(index), K(need_code), K(bulk_exceptions_.count()));
  } else if (need_code) {
    result = bulk_exceptions_.at(index).error_code_;
  } else {
    result = bulk_exceptions_.at(index).index_;
  }
  return ret;
}

int ObPLCursorInfo::prepare_entity(ObSQLSessionInfo &session, 
                                   lib::MemoryContext &entity)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(entity)) {
    uint64_t eff_tenant_id = session.get_effective_tenant_id();
    lib::MemoryContext parent_entity = session.get_cursor_cache().mem_context_;
    lib::ContextParam param;
    param.set_mem_attr(eff_tenant_id, ObModIds::OB_PL_TEMP, ObCtxIds::DEFAULT_CTX_ID)
      .set_page_size(OB_MALLOC_NORMAL_BLOCK_SIZE);
    OV (OB_NOT_NULL(parent_entity));
    OZ (parent_entity->CREATE_CONTEXT(entity, param));
    CK (OB_NOT_NULL(entity), OB_ALLOCATE_MEMORY_FAILED);
  } else {
    entity->reuse();
  }
  return ret;
}

int ObPLCursorInfo::prepare_spi_result(ObPLExecCtx *ctx, ObSPIResultSet *&spi_result)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(ctx));
  CK (OB_NOT_NULL(ctx->exec_ctx_));
  CK (OB_NOT_NULL(ctx->exec_ctx_->get_my_session()));
  if (OB_ISNULL(spi_cursor_) || !last_stream_cursor_) {
    OV (OB_NOT_NULL(get_allocator()));
    if (OB_SUCC(ret) && OB_NOT_NULL(spi_cursor_) && OB_NOT_NULL(static_cast<ObSPICursor*>(spi_cursor_))) {
      static_cast<ObSPICursor*>(spi_cursor_)->~ObSPICursor();
      get_allocator()->free(spi_cursor_);
      spi_cursor_ = NULL;
    }
    OX (spi_cursor_ = get_allocator()->alloc(sizeof(ObSPIResultSet)));
    OV (OB_NOT_NULL(spi_cursor_), OB_ALLOCATE_MEMORY_FAILED);
  } else {
    CK (OB_NOT_NULL(static_cast<ObSPIResultSet*>(spi_cursor_)));
    OX ((static_cast<ObSPIResultSet*>(spi_cursor_))->~ObSPIResultSet());
  }
  OX (spi_result = new (spi_cursor_) ObSPIResultSet());
  OX (last_stream_cursor_ = true);
  OZ (spi_result->init(*ctx->exec_ctx_->get_my_session()));
  return ret;
}

int ObPLCursorInfo::prepare_spi_cursor(ObSPICursor *&spi_cursor,
                                        uint64_t tenant_id,
                                        uint64_t mem_limit,
                                        bool is_local_for_update,
                                        sql::ObSQLSessionInfo* session_info)
{
  int ret = OB_SUCCESS;
  ObIAllocator *spi_allocator = get_allocator();
  if (OB_ISNULL(spi_allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cursor allocator is null.", K(ret), K(spi_allocator), K(id_));
  } else if (OB_ISNULL(spi_cursor_)) {
    int64_t alloc_size = is_local_for_update
      ? (sizeof(ObSPICursor) > sizeof(ObSPIResultSet) ? sizeof(ObSPICursor) : sizeof(ObSPIResultSet))
      : sizeof(ObSPICursor);
    OX (spi_cursor_ = spi_allocator->alloc(alloc_size));
    OV (OB_NOT_NULL(spi_cursor_), OB_ALLOCATE_MEMORY_FAILED);
  } else {
    if (last_stream_cursor_) {
      CK (OB_NOT_NULL(static_cast<ObSPIResultSet*>(spi_cursor_)));
      OX (static_cast<ObSPIResultSet*>(spi_cursor_)->~ObSPIResultSet());
    } else {
      CK (OB_NOT_NULL(static_cast<ObSPICursor*>(spi_cursor_)));
      OX (static_cast<ObSPICursor*>(spi_cursor_)->~ObSPICursor());
    }
  }
  OX (spi_cursor = new (spi_cursor_) ObSPICursor(*spi_allocator, session_info));
  OX (last_stream_cursor_ = false);
  if (OB_SUCC(ret)) {
    if (OB_INVALID_SIZE == mem_limit) {
      mem_limit = GCONF._chunk_row_store_mem_limit;
    }
    OZ (spi_cursor->row_store_.init(mem_limit,
                                tenant_id,
                                common::ObCtxIds::DEFAULT_CTX_ID,
                                "PSCursorRowStore"));
  }
  return ret;
}

int ObPLCursorInfo::set_current_position(int64_t position) {
  int ret = OB_SUCCESS;
  if (!is_streaming()) {
    if (OB_NOT_NULL(get_spi_cursor())) {
      get_spi_cursor()->cur_ = position;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result set is null in unstreaming mode.", K(get_id()), K(ret));
    }
  }
  current_position_ = position;
  return ret;
}

}  // namespace pl
}  // namespace oceanbase
