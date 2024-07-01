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
  CK (OB_LIKELY(T_PACKAGE_ALTER_OPTIONS == alter_clause.type_));
  CK (OB_LIKELY(PACKAGE_ALTER_COMPILE == alter_clause.int16_values_[0]));
  if (OB_FAIL(ret)) {
  } else if (1 == alter_clause.int16_values_[3]) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("alter package with reuse_setting not supported yet!", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "alter package with reuse setting");
  }
  OZ (compile_package(db_name, package_name, alter_clause.int16_values_[2], pkg_arg));
  return ret;
}

int ObAlterPackageResolver::analyze_package(ObPLCompiler &compiler,
                                            const ObPLBlockNS *parent_ns,
                                            ObPLPackageAST &package_ast,
                                            const ObString& db_name,
                                            const ObPackageInfo *package_info,
                                            share::schema::ObErrorInfo &error_info,
                                            bool &has_error)
{
  int ret = OB_SUCCESS;
  ObString source;
  ObString package_name;

  CK (OB_NOT_NULL(package_info));
  CK (package_info->is_package() || package_info->is_package_body());
  OX (package_name = package_info->get_package_name());
  OX (source = package_info->get_source());
  OZ (ObSQLUtils::convert_sql_text_from_schema_for_resolve(
          *allocator_, session_info_->get_dtc_params(), source));

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(compiler.analyze_package(source, parent_ns, package_ast,
                                              false /* is_for_trigger */))) {
    ObPL::insert_error_msg(ret);
    switch (ret) {
    case OB_ERR_PACKAGE_DOSE_NOT_EXIST:
      LOG_USER_WARN(OB_ERR_PACKAGE_DOSE_NOT_EXIST,
                    package_info->is_package() ? "PACKAGE" : "PACKAGE BODY",
                    db_name.length(), db_name.ptr(), package_name.length(), package_name.ptr());
      break;
    case OB_ERR_BAD_DATABASE:
      LOG_USER_WARN(OB_ERR_BAD_DATABASE, db_name.length(), db_name.ptr());
      break;
    default:
      LOG_USER_WARN(OB_ERR_PACKAGE_COMPILE_ERROR,
                    package_info->is_package() ? "PACKAGE" : "PACKAGE BODY",
                    db_name.length(), db_name.ptr(), package_name.length(), package_name.ptr());
      has_error = true;
      ret = OB_SUCCESS;
      break;
    }
  }
  OZ (error_info.collect_error_info(package_info));
  return ret;
}

int ObAlterPackageResolver::compile_package(const ObString& db_name,
                                            const ObString &package_name,
                                            int16_t compile_flag,
                                            obrpc::ObAlterPackageArg &pkg_arg)
{
  int ret = OB_SUCCESS;
  const ObPackageInfo *package_spec_info = nullptr;
  const ObPackageInfo *package_body_info = nullptr;
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
    bool has_error = false;
    char buf[OB_MAX_PROC_ENV_LENGTH];
    int64_t pos = 0;
    OZ (ObExecEnv::gen_exec_env(*session_info_, buf, OB_MAX_PROC_ENV_LENGTH, pos));
    OZ (ob_write_string(*allocator_, ObString(pos, buf), pkg_arg.exec_env_));
    OZ (package_guard.init());
    OZ (schema_checker_->get_package_info(session_info_->get_effective_tenant_id(),
                                          db_name,
                                          package_name,
                                          share::schema::PACKAGE_TYPE,
                                          compatible_mode,
                                          package_spec_info));
    OZ (package_spec_ast.init(db_name,
                              package_spec_info->get_package_name(),
                              PL_PACKAGE_SPEC,
                              package_spec_info->get_database_id(),
                              package_spec_info->get_package_id(),
                              package_spec_info->get_schema_version(),
                              nullptr));
    OZ (analyze_package(compiler, nullptr, package_spec_ast, db_name, package_spec_info,
                        error_info, has_error));

    bool collect_package_body_info = false;
    if (OB_SUCC(ret)) {
      switch (compile_flag) {
      case PACKAGE_UNIT_SPECIFICATION: {
        collect_package_body_info = false;  // compile package spec
      } break;
      case PACKAGE_UNIT_PACKAGE: {
        OZ (schema_checker_->get_package_info(session_info_->get_effective_tenant_id(),
                                              db_name,
                                              package_name,
                                              share::schema::PACKAGE_BODY_TYPE,
                                              compatible_mode,
                                              package_body_info));
        if (OB_ERR_PACKAGE_DOSE_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          collect_package_body_info = false;  // compile package spec
        } else if (OB_SUCC(ret) && OB_NOT_NULL(package_body_info)) {
          collect_package_body_info = true;  // compile package body
        } else {
          LOG_WARN("failed to get package body info", K(ret), K(package_body_info));
        }
      } break;
      case PACKAGE_UNIT_BODY: {
        OZ (schema_checker_->get_package_info(session_info_->get_effective_tenant_id(),
                                              db_name,
                                              package_name,
                                              share::schema::PACKAGE_BODY_TYPE,
                                              compatible_mode,
                                              package_body_info));
        if (OB_ERR_PACKAGE_DOSE_NOT_EXIST == ret) {
          LOG_WARN("package body not found", K(ret), K(db_name), K(package_name));
        } else if (OB_SUCC(ret) && OB_NOT_NULL(package_body_info)) {
          collect_package_body_info = true;  // compile package body
        } else {
          LOG_WARN("failed to get package body info", K(ret), K(package_body_info));
        }
      } break;
      default: {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid alter package compile flag", K(ret), K(compile_flag));
      } break;
      }
    }

#define COLLECT_PACKAGE_INFO(alt_pkg_arg, package_info)                            \
  do {                                                                             \
    OV(OB_NOT_NULL(package_info), OB_INVALID_ARGUMENT);                            \
    OX(alt_pkg_arg.tenant_id_ = package_info->get_tenant_id());                    \
    OX(alt_pkg_arg.package_type_ = package_info->get_type());                      \
    OX(alt_pkg_arg.compatible_mode_ = package_info->get_compatibility_mode());     \
  } while (0)

    if (OB_FAIL(ret)) {
    } else if (!collect_package_body_info) {
      COLLECT_PACKAGE_INFO(pkg_arg, package_spec_info);
      if (OB_SUCC(ret) && !has_error) {
        share::schema::ObErrorInfo error_info;
        OZ (error_info.delete_error(package_spec_info));
      }
    } else {
      bool body_has_error = false;
      OZ (package_body_ast.init(db_name,
                                package_name,
                                PL_PACKAGE_BODY,
                                OB_INVALID_ID,
                                OB_INVALID_ID,
                                OB_INVALID_VERSION,
                                &package_spec_ast));
      OZ (analyze_package(compiler, &(package_spec_ast.get_body()->get_namespace()),
                          package_body_ast, db_name, package_body_info, error_info, body_has_error));
      if (OB_SUCC(ret)) {
        ObArray<const ObRoutineInfo *> routine_infos;
        OZ (schema_checker_->get_schema_guard()->get_routine_infos_in_package(
              session_info_->get_effective_tenant_id(),
              package_spec_info->get_package_id(),
              routine_infos));
        if (OB_FAIL(ret)) {
        } else if (!body_has_error) {
          // if has_error, don't need to update routine route sql
          ObSEArray<ObRoutineInfo, 2> routine_spec_infos;
          ObPLRoutineTable &spec_routine_table = package_spec_ast.get_routine_table();
          ObPLRoutineTable &body_routine_table = package_body_ast.get_routine_table();
          if (OB_SUCC(ret) && routine_infos.empty() && spec_routine_table.get_count() > 1) {
            OZ (ObCreatePackageResolver::resolve_functions_spec(
              *package_spec_info, routine_spec_infos, spec_routine_table));
            CK (routine_spec_infos.count() > 0);
            for (int64_t i = 0; OB_SUCC(ret) && i < routine_spec_infos.count(); ++i) {
              OZ (routine_infos.push_back(&routine_spec_infos.at(i)));
            }
          }
          OZ (ObCreatePackageBodyResolver::update_routine_route_sql(*allocator_,
                                                                    *session_info_,
                                                                    pkg_arg.public_routine_infos_,
                                                                    spec_routine_table,
                                                                    body_routine_table,
                                                                    routine_infos));
          if (OB_FAIL(ret)) {
            pkg_arg.public_routine_infos_.reset();
          } else {
            share::schema::ObErrorInfo error_info;
            if (!has_error) {
              OZ (error_info.delete_error(package_spec_info));
            }
            OZ (error_info.delete_error(package_body_info));
          }
        } else if (0 != package_body_info->get_exec_env().case_compare(pkg_arg.exec_env_)) {
          for (int64_t i = 0; OB_SUCC(ret) && i < routine_infos.count(); ++i) {
            OZ (pkg_arg.public_routine_infos_.push_back(*routine_infos.at(i)));
          }
        }
        // TODO:actually，“alter package compile” should replace package spec and package body
        // but if we will alter package body, it only send package body info rpc, so we can only collect package body dep
        if (OB_SUCC(ret)) {
          ObString dep_attr;
          OZ (ObDependencyInfo::collect_dep_infos(package_body_ast.get_dependency_table(),
                                                  pkg_arg.dependency_infos_,
                                                  ObObjectType::PACKAGE_BODY,
                                                  0, dep_attr, dep_attr));
        }
      }
      COLLECT_PACKAGE_INFO(pkg_arg, package_body_info);
    }

#undef COLLECT_PACKAGE_INFO
  }
  return ret;
}

} //namespace sql
} //namespace oceanbase
