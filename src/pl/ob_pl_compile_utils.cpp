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

#include "pl/ob_pl_compile_utils.h"
#include "pl/ob_pl_compile.h"
#include "src/sql/resolver/ob_resolver_utils.h"
#include "pl/ob_pl_code_generator.h"
#include "pl/ob_pl_package.h"
#include "pl/pl_cache/ob_pl_cache_mgr.h"
#ifdef OB_BUILD_ORACLE_PL
#include "pl/ob_pl_package_type.h"
#include "sql/resolver/ddl/ob_create_udt_resolver.h"
#include "pl/ob_pl_udt_object_manager.h"
#endif

namespace oceanbase {
using namespace common;
using namespace share;
using namespace schema;
using namespace sql;
namespace pl {

int ObPLCompilerUtils::compile(ObExecContext &ctx,
                               uint64_t tenant_id,
                               const ObString &database_name,
                               const ObString &object_name,
                               CompileType object_type,
                               int64_t schema_version,
                               bool is_recompile)
{
  int ret = OB_SUCCESS;
  const ObDatabaseSchema *db_schema = nullptr;
  CK (OB_NOT_NULL(ctx.get_sql_ctx()));
  CK (OB_NOT_NULL(ctx.get_sql_ctx()->schema_guard_));
  OZ (ctx.get_sql_ctx()->schema_guard_->get_database_schema(tenant_id, database_name, db_schema));
  CK (OB_NOT_NULL(db_schema));
  OZ (compile(ctx, tenant_id, db_schema->get_database_id(), object_name, object_type, schema_version, is_recompile));
  if (OB_FAIL(ret)) {
    LOG_WARN("fail to compile object",
              K(ret), K(tenant_id), K(object_type), K(database_name), K(object_name), K(schema_version));
    ret = OB_SUCCESS;
    common::ob_reset_tsi_warning_buffer();
    if (NULL != ctx.get_my_session()) {
      ctx.get_my_session()->reset_warnings_buf();
    }
  }
  return ret;
}

int ObPLCompilerUtils::compile(ObExecContext &ctx,
                               uint64_t tenant_id,
                               uint64_t database_id,
                               const ObString &object_name,
                               CompileType object_type,
                               int64_t schema_version,
                               bool is_recompile)
{
  int ret = OB_SUCCESS;
  switch (object_type) {
    case COMPILE_PROCEDURE: {
      OZ (compile_routine(ctx, tenant_id, database_id, object_name, ROUTINE_PROCEDURE_TYPE, schema_version, is_recompile));
    } break;
    case COMPILE_FUNCTION: {
      OZ (compile_routine(ctx, tenant_id, database_id, object_name, ROUTINE_FUNCTION_TYPE, schema_version, is_recompile));
    } break;
    case COMPILE_PACKAGE_SPEC: {
      OZ (compile_package(ctx, tenant_id, database_id, object_name, schema::ObPackageType::PACKAGE_TYPE, schema_version, is_recompile));
    } break;
    case COMPILE_PACKAGE_BODY: {
      OZ (compile_package(ctx, tenant_id, database_id, object_name, schema::ObPackageType::PACKAGE_BODY_TYPE, schema_version, is_recompile));
    } break;
    case COMPILE_TRIGGER: {
      OZ (compile_trigger(ctx, tenant_id, database_id, object_name, schema_version, is_recompile));
    } break;
#ifdef OB_BUILD_ORACLE_PL
    case COMPILE_UDT: {
      OZ (compile_udt(ctx, tenant_id, database_id, object_name, schema_version, is_recompile));
    } break;
#endif
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected compile type", K(ret), K(object_type));
    } break;
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("fail to compile object",
              K(ret), K(tenant_id), K(object_type), K(database_id), K(object_name), K(schema_version));
    ret = OB_SUCCESS;
    common::ob_reset_tsi_warning_buffer();
    if (NULL != ctx.get_my_session()) {
      ctx.get_my_session()->reset_warnings_buf();
    }
  }
  return ret;
}

int ObPLCompilerUtils::compile_routine(ObExecContext &ctx,
                                       uint64_t tenant_id,
                                       uint64_t database_id,
                                       const ObString &routine_name,
                                       ObRoutineType routine_type,
                                       int64_t schema_version,
                                       bool is_recompile)
{
  int ret = OB_SUCCESS;
  const ObRoutineInfo *routine_info = nullptr;
  share::schema::ObSchemaGetterGuard *schema_guard = nullptr;
  uint64_t db_id = OB_INVALID_ID;

  CK (OB_NOT_NULL(ctx.get_sql_ctx()));
  CK (OB_NOT_NULL(schema_guard = ctx.get_sql_ctx()->schema_guard_));

  if (ROUTINE_PROCEDURE_TYPE == routine_type) {
    OZ (schema_guard->get_standalone_procedure_info(tenant_id, database_id, routine_name, routine_info));
  } else {
    OZ (schema_guard->get_standalone_function_info(tenant_id, database_id, routine_name, routine_info));
  }
  OZ (ctx.get_my_session()->get_database_id(db_id));

  if (OB_SUCC(ret)
      && OB_NOT_NULL(routine_info)
      && !(is_recompile && routine_info->is_invoker_right())
      && (OB_INVALID_VERSION == schema_version || schema_version == routine_info->get_schema_version())) {
    ObCacheObjGuard cacheobj_guard(PL_ROUTINE_HANDLE);
    ObPLFunction* routine = nullptr;
    ObPLCacheCtx pc_ctx;

    pc_ctx.session_info_ = ctx.get_my_session();
    pc_ctx.schema_guard_ = ctx.get_sql_ctx()->schema_guard_;
    pc_ctx.key_.namespace_ = ObLibCacheNameSpace::NS_PRCR;
    pc_ctx.key_.db_id_ = db_id;
    pc_ctx.key_.key_id_ = routine_info->get_routine_id();
    pc_ctx.key_.sessid_ = ctx.get_my_session()->is_pl_debug_on() ? ctx.get_my_session()->get_server_sid() : 0;

    CK (OB_NOT_NULL(ctx.get_pl_engine()));
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(pl::ObPLCacheMgr::get_pl_cache(ctx.get_my_session()->get_plan_cache(), cacheobj_guard, pc_ctx))) {
      LOG_TRACE("get pl function from ol cache failed", K(ret), K(pc_ctx.key_));
      HANDLE_PL_CACHE_RET_VALUE(ret);
    } else {
      routine = static_cast<pl::ObPLFunction*>(cacheobj_guard.get_cache_obj());
    }
    if (OB_SUCC(ret) && OB_ISNULL(routine)) {
      OZ (ctx.get_pl_engine()->generate_pl_function(ctx, routine_info->get_routine_id(), cacheobj_guard));
      OX (routine = static_cast<pl::ObPLFunction*>(cacheobj_guard.get_cache_obj()));
      CK (OB_NOT_NULL(routine));
      // recompile do not add pl cache !
      if (OB_SUCC(ret) && routine->get_can_cached() && !is_recompile) {
        ObString sql;
        OZ (ObPLCacheCtx::assemble_format_routine_name(sql, routine));
        OZ (ObSQLUtils::md5(sql, pc_ctx.sql_id_, (int32_t)sizeof(pc_ctx.sql_id_)));
        OX (routine->get_stat_for_update().name_ = sql);
        OX (routine->get_stat_for_update().type_ = pl::ObPLCacheObjectType::STANDALONE_ROUTINE_TYPE);
        OZ (ctx.get_pl_engine()->add_pl_lib_cache(routine, pc_ctx));
      }
      OZ (pl::ObPLCompiler::update_schema_object_dep_info(routine->get_dependency_table(),
                                                          routine->get_tenant_id(),
                                                          routine->get_owner(),
                                                          routine_info->get_routine_id(),
                                                          routine_info->get_schema_version(),
                                                          routine_info->get_object_type()));
    }
  }
  return ret;
}

int ObPLCompilerUtils::compile_package(ObExecContext &ctx,
                                       uint64_t tenant_id,
                                       uint64_t database_id,
                                       const ObString &package_name,
                                       schema::ObPackageType package_type,
                                       int64_t schema_version,
                                       bool is_recompile)
{
  int ret = OB_SUCCESS;
  ObSchemaChecker schema_checker;
  const ObPackageInfo *package_info = nullptr;
  int64_t compatible_mode = lib::is_oracle_mode() ? COMPATIBLE_ORACLE_MODE
                                                  : COMPATIBLE_MYSQL_MODE;
  CK (OB_NOT_NULL(ctx.get_sql_ctx()->schema_guard_));
  OZ (schema_checker.init(*ctx.get_sql_ctx()->schema_guard_, ctx.get_my_session()->get_sessid_for_table()));
  OZ (ctx.get_sql_ctx()->schema_guard_->get_package_info(tenant_id, database_id, package_name, package_type, compatible_mode, package_info));
  CK (OB_NOT_NULL(package_info));
  CK (OB_NOT_NULL(ctx.get_sql_proxy()));
  CK (OB_NOT_NULL(ctx.get_pl_engine()));
  if (OB_SUCC(ret)
      && !(is_recompile && package_info->is_invoker_right())
      && (OB_INVALID_VERSION == schema_version || schema_version == package_info->get_schema_version())) {
    const ObPackageInfo *package_spec_info = NULL;
    const ObPackageInfo *package_body_info = NULL;
    pl::ObPLPackage *package_spec = nullptr;
    pl::ObPLPackage *package_body = nullptr;
    pl::ObPLPackageGuard package_guard(ctx.get_my_session()->get_effective_tenant_id());
    pl::ObPLResolveCtx resolve_ctx(ctx.get_allocator(),
                                    *ctx.get_my_session(),
                                    *ctx.get_sql_ctx()->schema_guard_,
                                    package_guard,
                                    *ctx.get_sql_proxy(),
                                    false, false, false, NULL, NULL, TgTimingEvent::TG_TIMING_EVENT_INVALID,
                                    false, is_recompile ? false : true);

    OZ (package_guard.init());
    OZ (ctx.get_pl_engine()->get_package_manager().get_package_schema_info(resolve_ctx.schema_guard_,
                                                                           package_info->get_package_id(),
                                                                           package_spec_info,
                                                                           package_body_info));
    // trigger compile package & add to disk & add to pl cache only has package body
    if (OB_SUCC(ret) && OB_NOT_NULL(package_body_info)) {
      OZ (ctx.get_pl_engine()->get_package_manager().get_cached_package(resolve_ctx, package_info->get_package_id(), package_spec, package_body));
      CK (OB_NOT_NULL(package_spec));
    }
  }
  return ret;
}

int ObPLCompilerUtils::compile_trigger(ObExecContext &ctx,
                                       uint64_t tenant_id,
                                       uint64_t database_id,
                                       const ObString &trigger_name,
                                       int64_t schema_version,
                                       bool is_recompile)
{
  int ret = OB_SUCCESS;
  const ObTriggerInfo *trigger_info = nullptr;
  const ObPackageInfo *package_spec_info = NULL;
  CK (OB_NOT_NULL(ctx.get_sql_proxy()));
  CK (OB_NOT_NULL(ctx.get_sql_ctx()->schema_guard_));
  OZ (ctx.get_sql_ctx()->schema_guard_->get_trigger_info(tenant_id, database_id, trigger_name, trigger_info));

  if (OB_SUCC(ret) && OB_ISNULL(trigger_info)) {
    ret = OB_ERR_TRIGGER_NOT_EXIST;
    LOG_WARN("trigger not exist", K(ret), K(database_id), K(trigger_name));
  }

  CK (OB_NOT_NULL(ctx.get_pl_engine()));
  CK (OB_NOT_NULL(package_spec_info = &trigger_info->get_package_spec_info()));
  if (OB_SUCC(ret)
      && !(is_recompile && package_spec_info->is_invoker_right())
      && (OB_INVALID_VERSION == schema_version || schema_version == trigger_info->get_schema_version())) {
    ObPLPackage *package_spec = nullptr;
    ObPLPackage *package_body = nullptr;
    pl::ObPLPackageGuard package_guard(ctx.get_my_session()->get_effective_tenant_id());
    pl::ObPLResolveCtx resolve_ctx(ctx.get_allocator(),
                                    *ctx.get_my_session(),
                                    *ctx.get_sql_ctx()->schema_guard_,
                                    package_guard,
                                    *ctx.get_sql_proxy(),
                                    false, false, false, NULL, NULL, TgTimingEvent::TG_TIMING_EVENT_INVALID,
                                    false, is_recompile ? false : true);

    OZ (package_guard.init());
    OZ (ctx.get_pl_engine()->get_package_manager().get_cached_package(resolve_ctx,
                                                                      package_spec_info->get_package_id(),
                                                                      package_spec,
                                                                      package_body));
    CK (OB_NOT_NULL(package_spec));
    CK (OB_NOT_NULL(package_body));
  }
  return ret;
}

#ifdef OB_BUILD_ORACLE_PL
int ObPLCompilerUtils::compile_udt(ObExecContext &ctx,
                                   uint64_t tenant_id,
                                   uint64_t database_id,
                                   const ObString &udt_name,
                                   int64_t schema_version,
                                   bool is_recompile)
{
  int ret = OB_SUCCESS;

  CK (OB_NOT_NULL(ctx.get_sql_ctx()));
  CK (OB_NOT_NULL(ctx.get_my_session()));
  CK (OB_NOT_NULL(ctx.get_sql_proxy()));
  CK (OB_NOT_NULL(ctx.get_sql_ctx()->schema_guard_));

  if (OB_SUCC(ret)) {
    const ObUDTTypeInfo *udt_info = nullptr;
    pl::ObPLUDTObject *obj_spec = nullptr;
    pl::ObPLUDTObject *obj_body = nullptr;
    uint64_t spec_id = OB_INVALID_ID;
    uint64_t body_id = OB_INVALID_ID;
    ObUDTObjectType *spec_info = NULL;
    ObPackageInfo package_info;
    const share::schema::ObDatabaseSchema *db = nullptr;
    pl::ObPLPackageGuard package_guard(ctx.get_my_session()->get_effective_tenant_id());
    pl::ObPLResolveCtx resolve_ctx(ctx.get_allocator(),
                                   *ctx.get_my_session(),
                                   *ctx.get_sql_ctx()->schema_guard_,
                                   package_guard,
                                   *ctx.get_sql_proxy(),
                                   false, false, false, NULL, NULL, TgTimingEvent::TG_TIMING_EVENT_INVALID,
                                   false, is_recompile ? false : true);
    OZ (package_guard.init());
    OZ (ctx.get_sql_ctx()->schema_guard_->get_udt_info(tenant_id, database_id, OB_INVALID_ID, udt_name, udt_info));
    CK (OB_NOT_NULL(udt_info));
    if (OB_SUCC(ret) && 0 < udt_info->get_object_type_infos().count()) {
      spec_info = udt_info->get_object_type_infos().at(0);
      if (OB_NOT_NULL(spec_info)) {
        OZ (spec_info->to_package_info(package_info));
      }
    }
    if (OB_SUCC(ret)
        && !(is_recompile && package_info.is_invoker_right())
        && (OB_INVALID_VERSION == schema_version || schema_version == udt_info->get_schema_version())) {
      OX (spec_id = udt_info->get_object_spec_id(tenant_id));
      if (OB_INVALID_ID == spec_id) { // means not object with body
        bool is_valid = false;
        sql::ObResolverParams resolver_params;
        sql::ObSchemaChecker schema_checker;
        OZ (schema_checker.init(*ctx.get_sql_ctx()->schema_guard_, ctx.get_my_session()->get_sessid_for_table()));
        OX (resolver_params.allocator_ = &ctx.get_allocator());
        OX (resolver_params.schema_checker_ = &schema_checker);
        OX (resolver_params.session_info_ = ctx.get_my_session());
        OX (resolver_params.expr_factory_ = ctx.get_expr_factory());
        OX (resolver_params.stmt_factory_ = ctx.get_stmt_factory());
        OX (resolver_params.sql_proxy_ = ctx.get_sql_proxy());
        CK (OB_NOT_NULL(resolver_params.query_ctx_ = ctx.get_stmt_factory()->get_query_ctx()));
        OZ (ctx.get_sql_ctx()->schema_guard_->get_database_schema(tenant_id, database_id, db));
        CK (OB_NOT_NULL(db));
        OZ (ObCreateUDTResolver::check_udt_validation(*ctx.get_my_session(),
                                                      *ctx.get_sql_ctx()->schema_guard_,
                                                      resolver_params,
                                                      db->get_database_name_str(),
                                                      *udt_info,
                                                      is_valid));
        if (OB_SUCC(ret) && is_valid) {
          share::schema::ObErrorInfo error_info;
          OZ (error_info.delete_error(udt_info));
        }
      } else {
        if (udt_info->has_type_body()) {
          OX (body_id = udt_info->get_object_body_id(tenant_id));
        }
        //persistent object only has object body
        OZ (pl::ObPLUDTObjectManager::compile_udt(resolve_ctx,
                                                  spec_id,
                                                  body_id,
                                                  udt_info,
                                                  obj_spec,
                                                  obj_body));
        if (body_id != OB_INVALID_ID) {
          CK (OB_NOT_NULL(obj_spec));
          CK (OB_NOT_NULL(obj_body));
        }
      }
    }
  }
  return ret;
}
#endif

} // end of namespace pl
} // end of namespace oceanbase
