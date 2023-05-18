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

#define USING_LOG_PREFIX SQL_RESV
#include "ob_alter_package_resolver.h"
#include "ob_alter_package_stmt.h"
#include "share/ob_rpc_struct.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "pl/parser/parse_stmt_item_type.h"
#include "pl/ob_pl_package.h"
#include "pl/ob_pl_compile.h"
#include "sql/resolver/ddl/ob_create_package_resolver.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;
using namespace oceanbase::pl;
namespace sql
{

int ObAlterPackageResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  const ParseNode *name_node = NULL;
  const ParseNode *alter_clause = NULL;
  ObString db_name;
  ObString package_name;
  ObAlterPackageStmt *alter_package_stmt = NULL;

  CK (OB_LIKELY(T_PACKAGE_ALTER == parse_tree.type_));
  CK (OB_LIKELY(2 == parse_tree.num_child_));
  CK (OB_NOT_NULL(name_node = parse_tree.children_[0]));
  CK (OB_NOT_NULL(alter_clause = parse_tree.children_[1]));
  OZ (ObResolverUtils::resolve_sp_name(*session_info_, *name_node, db_name, package_name));
  if (OB_SUCC(ret) && ObSchemaChecker::is_ora_priv_check()) {
    OZ (schema_checker_->check_ora_ddl_priv(session_info_->get_effective_tenant_id(),
                                            session_info_->get_priv_user_id(),
                                            db_name,
                                            stmt::T_ALTER_ROUTINE,
                                            session_info_->get_enable_role_array()));
  }
  OV (OB_NOT_NULL(alter_package_stmt = create_stmt<ObAlterPackageStmt>()), OB_ALLOCATE_MEMORY_FAILED);
  OX (alter_package_stmt->get_alter_package_arg().db_name_ = db_name);
  OX (alter_package_stmt->get_alter_package_arg().package_name_ = package_name);
  OZ (resolve_alter_clause(*alter_clause, db_name, package_name, alter_package_stmt->get_alter_package_arg()));
  return ret;
}

int ObAlterPackageResolver::resolve_alter_clause(const ParseNode &alter_clause,
                                                 const ObString &db_name,
                                                 const ObString &package_name,
                                                 obrpc::ObAlterPackageArg &pkg_arg)
{
  int ret = OB_SUCCESS;
  CK (OB_LIKELY(T_PACKAGE_ALTER_OPTIONS == alter_clause.type_));
  if (OB_FAIL(ret)) {
  } else if (PACKAGE_ALTER_EDITIONABLE == alter_clause.int16_values_[0]
             || PACKAGE_ALTER_NONEDITIONABLE == alter_clause.int16_values_[0]) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("alter editionable is not supported yet!", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "alter editionable");
  } else {
    OZ (resolve_alter_compile_clause(alter_clause, db_name, package_name, pkg_arg));
  }
  return ret;
}

int ObAlterPackageResolver::resolve_alter_compile_clause(const ParseNode &alter_clause,
                                                         const ObString &db_name,
                                                         const ObString &package_name,
                                                         obrpc::ObAlterPackageArg &pkg_arg)
{
  int ret = OB_SUCCESS;
  bool compile_spec = false;
  bool compile_body = false;
  CK (OB_LIKELY(T_PACKAGE_ALTER_OPTIONS == alter_clause.type_));
  CK (OB_LIKELY(PACKAGE_ALTER_COMPILE == alter_clause.int16_values_[0]));
  if (OB_FAIL(ret)) {
  } else if (1 == alter_clause.int16_values_[3]) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("alter package with reuse_setting not supported yet!", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "alter package with reuse setting");
  } else if (PACKAGE_UNIT_BODY == alter_clause.int16_values_[2]) {
    compile_body = true;
  } else if (PACKAGE_UNIT_SPECIFICATION == alter_clause.int16_values_[2]) {
    compile_spec = true;
  }
  OZ (compile_package(db_name, package_name, compile_spec, compile_body, pkg_arg));
  return ret;
}

int ObAlterPackageResolver::analyze_package(ObPLCompiler &compiler,
                                            const ObString &source,
                                            const ObPLBlockNS *parent_ns,
                                            ObPLPackageAST &package_ast,
                                            bool is_for_trigger,
                                            bool is_package,
                                            const ObString& db_name, 
                                            const ObString &package_name,
                                            const ObPackageInfo *package_info,
                                            share::schema::ObErrorInfo &error_info,
                                            bool &has_error)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_FAIL(compiler.analyze_package(source, parent_ns, package_ast, is_for_trigger))) {
    ObPL::insert_error_msg(ret);
    switch (ret) {
      case OB_ERR_PACKAGE_DOSE_NOT_EXIST:
        LOG_USER_WARN(OB_ERR_PACKAGE_DOSE_NOT_EXIST, is_package ? "PACKAGE" : "PACKAGE BODY",
                    db_name.length(), db_name.ptr(), package_name.length(), package_name.ptr());
        break;
      case OB_ERR_BAD_DATABASE:
        LOG_USER_WARN(OB_ERR_BAD_DATABASE, db_name.length(), db_name.ptr());
        break;
      default:
        LOG_USER_WARN(OB_ERR_PACKAGE_COMPILE_ERROR, is_package ? "PACKAGE" : "PACKAGE BODY",
                    db_name.length(), db_name.ptr(), package_name.length(), package_name.ptr());
        has_error = true;
        ret = OB_SUCCESS;
        break;
    }
  }
  tmp_ret = error_info.collect_error_info(package_info);
  ret = OB_SUCCESS == ret ? tmp_ret : ret;
  return ret;
}

int ObAlterPackageResolver::compile_package(const ObString& db_name,
                                            const ObString &package_name,
                                            bool compile_spec,
                                            bool compile_body,
                                            obrpc::ObAlterPackageArg &pkg_arg)
{
  int ret = OB_SUCCESS;
  const ObPackageInfo *package_spec_info = NULL;
  const ObPackageInfo *package_body_info = NULL;
  int64_t compatible_mode = lib::is_oracle_mode() ? COMPATIBLE_ORACLE_MODE
                                                  : COMPATIBLE_MYSQL_MODE;
  share::schema::ObErrorInfo &error_info = pkg_arg.error_info_;
  HEAP_VARS_2((ObPLPackageAST, package_spec_ast, *allocator_),
              (ObPLPackageAST, package_body_ast, *allocator_)) {
    ObPLPackageGuard package_guard(session_info_->get_effective_tenant_id());
    ObPLCompiler compiler(*allocator_,
                          *session_info_,
                          *(schema_checker_->get_schema_guard()),
                          package_guard,
                          *(params_.sql_proxy_));
    ObString source;
    bool has_error = false;
    OZ (package_guard.init());
    OZ (schema_checker_->get_package_info(session_info_->get_effective_tenant_id(),
                                          db_name,
                                          package_name,
                                          PACKAGE_TYPE,
                                          compatible_mode,
                                          package_spec_info));
    OZ (package_spec_ast.init(db_name,
                              package_spec_info->get_package_name(),
                              PL_PACKAGE_SPEC,
                              package_spec_info->get_database_id(),
                              package_spec_info->get_package_id(),
                              package_spec_info->get_schema_version(),
                              NULL));
    OX (source = package_spec_info->get_source());
    OZ (ObSQLUtils::convert_sql_text_from_schema_for_resolve(
          *allocator_, session_info_->get_dtc_params(), source));
    OZ (analyze_package(compiler, source, NULL, package_spec_ast, false, true, 
          db_name, package_name, package_spec_info, error_info, has_error));
    if (OB_FAIL(ret)) {
      // error msg has fixed in analyze_package
    } else if (1 == package_spec_ast.get_routine_table().get_count()
               && !compile_body && !compile_spec) {
      OZ (schema_checker_->get_package_info(session_info_->get_effective_tenant_id(),
                                            db_name,
                                            package_name,
                                            PACKAGE_BODY_TYPE,
                                            compatible_mode,
                                            package_body_info),
                                            package_spec_ast.get_routine_table().get_count(),
                                            compile_body);
      if (OB_SUCC(ret)) {
        ObString source;
        OZ (schema_checker_->get_package_info(session_info_->get_effective_tenant_id(),
                                            db_name,
                                            package_name,
                                            PACKAGE_BODY_TYPE,
                                            compatible_mode,
                                            package_body_info));
        OX (source = package_body_info->get_source());
        OZ (ObSQLUtils::convert_sql_text_from_schema_for_resolve(
              *allocator_, session_info_->get_dtc_params(), source));
        OZ (package_body_ast.init(db_name,
                                  package_name,
                                  PL_PACKAGE_BODY,
                                  OB_INVALID_ID,
                                  OB_INVALID_ID,
                                  OB_INVALID_VERSION,
                                  &package_spec_ast));
        OZ (analyze_package(compiler, source, &(package_spec_ast.get_body()->get_namespace()), 
          package_body_ast, false, false, db_name, package_name, package_body_info, error_info, has_error));
      } else if (OB_ERR_PACKAGE_DOSE_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        OZ (error_info.delete_error(package_spec_info));
      }
    } else if (package_spec_ast.get_routine_table().get_count() > 1 && !compile_body) {
      // 如果不需要compile body, 仅check body是否存在
      OZ (schema_checker_->get_package_info(session_info_->get_effective_tenant_id(),
                                            db_name,
                                            package_name,
                                            PACKAGE_BODY_TYPE,
                                            compatible_mode,
                                            package_body_info),
                                            package_spec_ast.get_routine_table().get_count(),
                                            compile_body);
      if (OB_ERR_PACKAGE_DOSE_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        LOG_USER_WARN(OB_ERR_PACKAGE_COMPILE_ERROR, "PACKAGE",
                      db_name.length(), db_name.ptr(),
                      package_name.length(), package_name.ptr());
      }
    }
    if (OB_FAIL(ret)) {
    } else if (compile_body || (!compile_body && !compile_spec)) {
      ObString source;
      has_error = false;
      OZ (schema_checker_->get_package_info(session_info_->get_effective_tenant_id(),
                                            db_name,
                                            package_name,
                                            PACKAGE_BODY_TYPE,
                                            compatible_mode,
                                            package_body_info));
      OX (source = package_body_info->get_source());
      OZ (ObSQLUtils::convert_sql_text_from_schema_for_resolve(
            *allocator_, session_info_->get_dtc_params(), source));
      OZ (package_body_ast.init(db_name,
                                package_name,
                                PL_PACKAGE_BODY,
                                OB_INVALID_ID,
                                OB_INVALID_ID,
                                OB_INVALID_VERSION,
                                &package_spec_ast));
      OZ (analyze_package(compiler, source, &(package_spec_ast.get_body()->get_namespace()), 
        package_body_ast, false, false, db_name, package_name, package_body_info, error_info, has_error));
      OX (pkg_arg.tenant_id_ = package_body_info->get_tenant_id());
      OX (pkg_arg.package_type_ = package_body_info->get_type());
      OX (pkg_arg.compatible_mode_ = package_body_info->get_compatibility_mode());
      if (OB_SUCC(ret) && !has_error) {
        // if has_error, don't need to update routine route sql
        ObArray<const ObRoutineInfo *> routine_infos;
        ObPLRoutineTable &spec_routine_table = package_spec_ast.get_routine_table();
        ObPLRoutineTable &body_routine_table = package_body_ast.get_routine_table();
        OZ (schema_checker_->get_schema_guard()->get_routine_infos_in_package(
            session_info_->get_effective_tenant_id(),
            package_spec_info->get_package_id(),
            routine_infos));
        OZ (ObCreatePackageBodyResolver::update_routine_route_sql(*allocator_,
                                                                  *session_info_,
                                                                  pkg_arg.public_routine_infos_,
                                                                  spec_routine_table,
                                                                  body_routine_table,
                                                                  routine_infos));
      }
    }
    if (OB_SUCC(ret)) {
      if (!(compile_body || (!compile_body && !compile_spec))) {
        OV (OB_NOT_NULL(package_spec_info), OB_INVALID_ARGUMENT);
        OX (pkg_arg.tenant_id_ = package_spec_info->get_tenant_id());
        OX (pkg_arg.package_type_ = package_spec_info->get_type());
        OX (pkg_arg.compatible_mode_ = package_spec_info->get_compatibility_mode());
      }
    }
  }
  // TODO: collect error info
  return ret;
}

} //namespace sql
} //namespace oceanbase
