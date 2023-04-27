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
#include "ob_create_package_resolver.h"
#include "ob_create_package_stmt.h"
#include "share/ob_rpc_struct.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "pl/ob_pl.h"
#include "pl/ob_pl_package.h"
#include "pl/ob_pl_compile.h"
#include "lib/charset/ob_charset.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;
using namespace pl;
namespace sql
{

int ObCreatePackageResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  CK (T_PACKAGE_CREATE == parse_tree.type_);
  CK (CREATE_PACKAGE_NODE_CHILD_COUNT == parse_tree.num_child_);
  CK (OB_NOT_NULL(parse_tree.children_));
  CK (OB_NOT_NULL(session_info_));
  if (OB_SUCC(ret) && lib::is_mysql_mode() &&
      OB_SYS_TENANT_ID != session_info_->get_effective_tenant_id()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported package in mysql mode", K(ret), K(lbt()));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "not supported package in mysql mode");
  }
  if (OB_SUCC(ret)) {
    bool resolve_success = true;
    HEAP_VAR(ObPLPackageAST, package_ast, *allocator_) {
      ObPLPackageGuard package_guard(params_.session_info_->get_effective_tenant_id());
      ObPLResolver resolver(*params_.allocator_,
                            *params_.session_info_,
                            *(params_.schema_checker_->get_schema_mgr()),
                            package_guard,
                            *params_.sql_proxy_,
                            *params_.expr_factory_,
                            NULL,
                            false);
      ObCreatePackageStmt *stmt = NULL;
      ParseNode *package_block_node = NULL ;
      ParseNode *sp_name_node = NULL;
      ParseNode *package_name_node = NULL;
      ParseNode *package_stmts_node = NULL;
      ParseNode *package_clause_node = NULL;
      ObString db_name;
      ObString package_name;
      bool is_invoker_right = false;
      bool has_accessible_by = false;
      CK (OB_NOT_NULL(package_block_node = parse_tree.children_[0]));
      CK (T_PACKAGE_BLOCK == package_block_node->type_);
      CK (OB_UNLIKELY(PACKAGE_BLOCK_NODE_CHILD_COUNT == package_block_node->num_child_));
      CK (OB_NOT_NULL(package_block_node->children_));
      CK (OB_NOT_NULL(sp_name_node = package_block_node->children_[0]));
      OX (package_clause_node = package_block_node->children_[1]);
      OX (package_stmts_node = package_block_node->children_[2]);
      OX (package_name_node = package_block_node->children_[3]);
      OZ (ObResolverUtils::resolve_sp_name(*session_info_,
                                                   *sp_name_node,
                                                   db_name,
                                                   package_name));
      CK (OB_NOT_NULL(schema_checker_));
      if (OB_SUCC(ret) && ObSchemaChecker::is_ora_priv_check()) {
        OZ (schema_checker_->check_ora_ddl_priv(
                              session_info_->get_effective_tenant_id(),
                              session_info_->get_priv_user_id(),
                              db_name,
                              stmt::T_CREATE_ROUTINE,
                              session_info_->get_enable_role_array()));
      }
      //Resolve package clause: invoker right and accessible by
      OZ (resolve_invoke_accessible(package_clause_node,
                                    is_invoker_right,
                                    has_accessible_by));
      //NOTE: null package stmts is possible
      if (OB_SUCC(ret) && OB_NOT_NULL(package_stmts_node)) {
        CK (T_PACKAGE_STMTS == package_stmts_node->type_);
        OZ (package_ast.init(db_name, package_name, PL_PACKAGE_SPEC,
                             OB_INVALID_ID, OB_INVALID_ID, OB_INVALID_VERSION, NULL));
        OZ (resolver.init(package_ast));
        if (OB_SUCC(ret)
            && OB_FAIL(resolver.resolve(package_stmts_node, package_ast))) {
          LOG_WARN("resolve package spec failed", K(ret));
          LOG_USER_WARN(OB_ERR_PACKAGE_COMPILE_ERROR, "PACKAGE",
                        db_name.length(), db_name.ptr(), package_name.length(), package_name.ptr());
          resolve_success = ObPLResolver::is_object_not_exist_error(ret) ? true : false;
          ObPL::insert_error_msg(ret);
          ret = OB_SUCCESS;
        }
      }
      //NOTE: null package name is possible
      if (OB_SUCC(ret) && OB_NOT_NULL(package_name_node)) {
        ObString opt_package_name(package_name_node->str_len_, package_name_node->str_value_);
        CK (T_IDENT == package_name_node->type_);
        if (OB_SUCC(ret)
            && OB_UNLIKELY(0 != package_name.case_compare(opt_package_name))) {
          ret = OB_ERR_SP_LILABEL_MISMATCH;
          LOG_WARN("end package name not match", K(ret), K(package_name), K(opt_package_name));
          LOG_USER_ERROR(OB_ERR_SP_LILABEL_MISMATCH, opt_package_name.length(), opt_package_name.ptr());
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_ISNULL(stmt = create_stmt<ObCreatePackageStmt>())) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate memory for create package stmt failed", K(ret));
        } else {
          common::ObCompatibilityMode compa_mode = lib::is_mysql_mode() ? common::MYSQL_MODE
                                                                        : common::ORACLE_MODE;
          obrpc::ObCreatePackageArg &create_package_arg = stmt->get_create_package_arg();
          ObPackageInfo &package_info = create_package_arg.package_info_;
          ObString package_block(static_cast<int32_t>(package_block_node->str_len_),
                                 package_block_node->str_value_);
          create_package_arg.is_replace_ = static_cast<bool>(parse_tree.int32_values_[0]);
          create_package_arg.is_editionable_ = !static_cast<bool>(parse_tree.int32_values_[1]);
          create_package_arg.db_name_ = db_name;
          package_info.set_tenant_id(session_info_->get_effective_tenant_id());
          package_info.set_owner_id(session_info_->get_user_id());
          package_info.set_type(PACKAGE_TYPE);
          package_info.set_compatibility_mode(compa_mode);
          if (is_invoker_right) {
            package_info.set_invoker_right();
          }
          if (has_accessible_by) {
            package_info.set_accessible_by_clause();
          }
          if (!create_package_arg.is_editionable_) {
            create_package_arg.package_info_.set_noneditionable();
          }
          if (OB_FAIL(ObSQLUtils::convert_sql_text_to_schema_for_storing(
                        *allocator_, session_info_->get_dtc_params(), package_block))) {
            LOG_WARN("fail to convert package block", K(ret));
          } else if (OB_FAIL(package_info.set_package_name(package_name))) {
            LOG_WARN("set package name failed", K(ret), K(package_name));
          } else if (OB_FAIL(package_info.set_source(package_block))) {
            LOG_WARN("set package source failed", K(ret));
          } else if (OB_SYS_TENANT_ID == session_info_->get_effective_tenant_id()) {
            // 系统租户在创建系统包, 环境变量使用Oracle租户默认的环境变量
            // sql_mode = "PIPES_AS_CONCAT,STRICT_ALL_TABLES,PAD_CHAR_TO_FULL_LENGTH"
            if (common::ORACLE_MODE == compa_mode) {
              if (OB_FAIL(package_info.set_exec_env(ObString("2151677954,45,46,46,")))) {
                LOG_WARN("failed to set system package exec env",
                          K(ret), K(session_info_->get_effective_tenant_id()), K(package_info));
              }
            } else {
              OZ (package_info.set_exec_env(ObString("4194304,45,45,45,")));
            }
          } else {
            char buf[OB_MAX_PROC_ENV_LENGTH];
            int64_t pos = 0;
            if (OB_FAIL(ObExecEnv::gen_exec_env(*session_info_, buf, OB_MAX_PROC_ENV_LENGTH, pos))) {
              LOG_WARN("failed to generate exec env", K(ret));
            } else if (OB_FAIL(package_info.set_exec_env(ObString(pos, buf)))) {
              LOG_WARN("set exec env failed", K(ret));
            } else {}
          }
          if (OB_SUCC(ret) && resolve_success) {
            OZ (resolve_functions_spec(package_info,
                                       create_package_arg.public_routine_infos_,
                                       package_ast.get_routine_table()));
          }
          if (OB_SUCC(ret)) {
            ObString dep_attr;
            OZ (ObDependencyInfo::collect_dep_infos(package_ast.get_dependency_table(),
                                                    create_package_arg.dependency_infos_,
                                                    ObObjectType::PACKAGE,
                                                    0, dep_attr, dep_attr));
          }
          if (OB_SUCC(ret)) {
            ObErrorInfo &error_info = create_package_arg.error_info_;
            ObPackageInfo &pkg_info = create_package_arg.package_info_;
            error_info.collect_error_info(&pkg_info);
          }
        }
      }
    }
    if (OB_NOT_NULL(session_info_)
        && OB_SYS_TENANT_ID == session_info_->get_effective_tenant_id()
        /*&& !session_info_->is_inner()*/) {
      // 低版本升级到2274, 老的升级脚本中包含了创建Package语句, 部分语句在2274 Server上会产生Warning
      // 比如: Create Package pack IS Procedure proc(x Boolean := 1); End;会报错Boolean表达式默认值非法的Warning
      // 2274的升级脚本还会用最新的Package脚本重建这个包, 为了避免产生的Warning使得升级失败, 这里把Warning清理掉
      common::ob_reset_tsi_warning_buffer();
    }
  }
  return ret;
}

int ObCreatePackageResolver::resolve_invoke_accessible(const ParseNode *package_clause_node,
                                       bool &is_invoker_right,
                                       bool &has_accessible_by)
{
  int ret = OB_SUCCESS;
  bool has_sp_invoker_clause = false;
  bool has_accessible_by_clause = false;
  if (OB_NOT_NULL(package_clause_node)) {
    CK (T_SP_CLAUSE_LIST == package_clause_node->type_);
    for (int64_t i = 0; OB_SUCC(ret) && i < package_clause_node->num_child_; ++i) {
      const ParseNode *node = package_clause_node->children_[i];
      if (OB_NOT_NULL(node)) {
        if (T_SP_INVOKE == node->type_) {
          if (has_sp_invoker_clause) {
            ret = OB_ERR_DECL_MORE_THAN_ONCE;
            LOG_WARN("PLS-00371: at most one declaration for 'string' is permitted",
                      K(ret), K(node->type_), K(has_sp_invoker_clause));
          } else {
            has_sp_invoker_clause = true;
            if (1 == node->value_) {
              is_invoker_right = true;
            }
          }
        } else if (T_SP_ACCESSIBLE_BY == node->type_) {
          if (has_accessible_by_clause) {
            ret = OB_ERR_DECL_MORE_THAN_ONCE;
            LOG_WARN("PLS-00371: at most one declaration for 'string' is permitted",
                      K(ret), K(node->type_), K(has_accessible_by_clause));
          } else {
            has_accessible_by_clause = true;
            has_accessible_by = true;
          }
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("not supported other clause yet", K(ret), K(node));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "specified invoker clause");
        }
      }
    }
  }

  return ret;
}

int ObCreatePackageResolver::check_overload_out_argument(const pl::ObPLRoutineTable &routine_table, int64_t idx)
{
  int64_t ret = OB_SUCCESS;
  const ObPLRoutineInfo *right_info = NULL;
  OZ (routine_table.get_routine_info(idx, right_info));
  CK (OB_NOT_NULL(right_info));
  for (int64_t i = idx - 1; OB_SUCC(ret) && i >= ObPLRoutineTable::NORMAL_ROUTINE_START_IDX; --i) {
    const ObPLRoutineInfo *left_info = NULL;
    OZ (routine_table.get_routine_info(i, left_info));
    OV (OB_NOT_NULL(left_info), OB_ERR_UNEXPECTED, i);
    if (OB_FAIL(ret)) {
    } else if (left_info->get_name() == right_info->get_name()
               && left_info->get_type() == right_info->get_type()) {
      int64_t left_param_cnt = left_info->get_param_count();
      int64_t right_param_cnt = right_info->get_param_count();
      int64_t param_cnt = left_param_cnt >= right_param_cnt ? left_param_cnt : right_param_cnt;
      for (int64_t j = 0; OB_SUCC(ret) && j < param_cnt; ++j) {
        ObPLRoutineParam *left_param = NULL;
        ObPLRoutineParam *right_param = NULL;
        if (j < left_param_cnt) {
          OZ (left_info->get_routine_param(j, left_param));
          CK (OB_NOT_NULL(left_param));
        }
        if (j < right_param_cnt) {
          OZ (right_info->get_routine_param(j, right_param));
          CK (OB_NOT_NULL(right_param));
        }
        if (OB_FAIL(ret)) {
        } else if (OB_NOT_NULL(left_param) && OB_NOT_NULL(right_param)) {
          bool left_out_mode = left_param->is_out_param() || left_param->is_inout_param();
          bool right_out_mode = right_param->is_out_param() || right_param->is_inout_param();
          if (left_out_mode != right_out_mode) {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("overloaded routine out parameter not in same position!",
                     K(ret), K(j), K(left_info->get_name()));
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "overloaded routine out parameter not in same position");
          }
        } else if (OB_NOT_NULL(left_param)
                  && (left_param->is_out_param() || left_param->is_inout_param())) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("overloaded routine out parameter not in same position!",
                   K(ret), K(j), K(left_info->get_name()));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "overloaded routine out parameter not in same position");
        } else if (OB_NOT_NULL(right_param)
                  && (right_param->is_out_param() || right_param->is_inout_param())) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("overloaded routine out parameter not in same position!",
                   K(ret), K(j), K(left_info->get_name()));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "overloaded routine out parameter not in same position");
        }
      }
      break;
    }
  }
  return ret;
}

int ObCreatePackageResolver::resolve_functions_spec(const ObPackageInfo &package_info,
                                                    ObIArray<ObRoutineInfo> &routine_list,
                                                    const ObPLRoutineTable &routine_table,
                                                    ObRoutineType routine_type)
{
  int ret = OB_SUCCESS;

  uint64_t routine_count = routine_table.get_count();
  ObRoutineInfo routine_info;
  const ObPLRoutineInfo *pl_routine_info = NULL;
  for (int64_t i = ObPLRoutineTable::NORMAL_ROUTINE_START_IDX; OB_SUCC(ret) && i<routine_count; i++) {
    routine_info.reset();
    //process basic info
    routine_info.set_tenant_id(package_info.get_tenant_id());
    routine_info.set_owner_id(package_info.get_owner_id());
    routine_info.set_database_id(package_info.get_database_id());
    routine_info.set_package_id(package_info.get_package_id());
    routine_info.set_routine_type(routine_type);
    routine_info.set_subprogram_id(i);
    routine_info.set_exec_env(package_info.get_exec_env());
    if (OB_FAIL(routine_table.get_routine_info(i, pl_routine_info))) {
      LOG_WARN("get package routine info failed", K(package_info.get_package_name()), K(ret));
    } else if (OB_FAIL(routine_info.set_routine_name(pl_routine_info->get_name()))) {
      LOG_WARN("set routine name failed", "routine name", pl_routine_info->get_name(), K(ret));
    } /*else if (i > ObPLRoutineTable::NORMAL_ROUTINE_START_IDX) {
               // && OB_FAIL(check_overload_out_argument(routine_table, i))) {
      LOG_WARN("failed to check overload out argument", K(ret));
    } */else {
      if (pl_routine_info->is_deterministic()) {
        routine_info.set_deterministic();
      }
      if (pl_routine_info->is_parallel_enable()) {
        routine_info.set_parallel_enable();
      }
      if (pl_routine_info->is_pipelined()) {
        routine_info.set_pipelined();
      }
      //set data access info
      if (pl_routine_info->is_no_sql()) {
        routine_info.set_no_sql();
      } else if (pl_routine_info->is_reads_sql_data()) {
        routine_info.set_reads_sql_data();
      } else if (pl_routine_info->is_modifies_sql_data()) {
        routine_info.set_modifies_sql_data();
      } else if (pl_routine_info->is_contains_sql()) {
        routine_info.set_contains_sql();
      }
      // udt type 相关信息设置
      if (pl_routine_info->is_udt_routine()) {
        routine_info.set_is_udt_udf();
        if (pl_routine_info->is_udt_static_routine()) {
          routine_info.set_is_static();
        }
        if (pl_routine_info->is_function()) {
          routine_info.set_is_udt_function();
        }
        if (pl_routine_info->is_udt_cons()) {
          routine_info.set_is_udt_cons();
        }
        if (pl_routine_info->is_udt_map()) {
          routine_info.set_is_udt_map();
        }
        if (pl_routine_info->is_udt_order()) {
          routine_info.set_is_udt_order();
        }
      }
      if (package_info.is_invoker_right()) {
        routine_info.set_invoker_right();
      }
      if (package_info.has_accessible_by_clause()
          || pl_routine_info->has_accessible_by_clause()) {
        routine_info.set_accessible_by_clause();
      }
      routine_info.set_overload(NO_OVERLOAD_IDX); //no overload
      for (int64_t k = routine_list.count(); OB_SUCC(ret) && k>0; k--) {
        ObRoutineInfo &tmp_routine_info = routine_list.at(k-1);
        if (ObCharset::case_insensitive_equal(routine_info.get_routine_name(),
                                              tmp_routine_info.get_routine_name())) {
          if (NO_OVERLOAD_IDX == tmp_routine_info.get_overload()) {
            tmp_routine_info.set_overload(OVERLOAD_START_IDX);
          }
          routine_info.set_overload(tmp_routine_info.get_overload()+1);
          break;
        }
      }
    }
    //process ret and param info
    if (OB_SUCC(ret)) {
      int64_t start_position = 1;
      int64_t start_sequence = 1;
      int64_t start_level = 0;
      if (OB_SUCC(ret) && OB_NOT_NULL(pl_routine_info->get_ret_info())) {
        const ObPLRoutineParam *ret_info =
          static_cast<const ObPLRoutineParam*>(pl_routine_info->get_ret_info());
        CK (OB_NOT_NULL(ret_info));
        OZ (ObPLDataType::transform_and_add_routine_param(ret_info,
                                                          0, // position
                                                          start_level,
                                                          start_sequence,
                                                          routine_info));
        /*if (OB_FAIL(ret_info->get_type().add_package_routine_schema_param(
            resolver.get_resolve_ctx(),
            resolver.get_current_namespace(), package_name, ret_info->get_name(),
            ret_info->get_mode(), 0, start_level, start_sequence, routine_info))) {
          LOG_WARN("add package routine return param failed", K(ret));
        } */
      }
      if (OB_SUCC(ret)) {
        uint64_t param_count = pl_routine_info->get_param_count();
        for (int64_t j = 0; OB_SUCC(ret) && j < param_count; j++) {
          ObPLRoutineParam *param = pl_routine_info->get_params().at(j);
          CK (OB_NOT_NULL(param));
          OZ (ObPLDataType::transform_and_add_routine_param(param,
                                                            start_position++,
                                                            start_level,
                                                            start_sequence,
                                                            routine_info));
          /* if (OB_FAIL(param->get_type().add_package_routine_schema_param(
              resolver.get_resolve_ctx(),
              resolver.get_current_namespace(), package_name, param->get_name(),
              param->get_mode(), start_position++, start_level, start_sequence, routine_info))) {
            LOG_WARN("add package routine param failed", K(ret));
          } else*/ if (OB_SUCC(ret)) {
            int64_t idx = routine_info.get_routine_params().count() - 1;
            ObRoutineParam* rountine_param = routine_info.get_routine_params().at(idx);
            if (OB_ISNULL(rountine_param)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("rountine param is null", K(ret), K(idx));
            } else if (OB_FAIL(rountine_param->set_default_value(param->get_default_value()))) {
              LOG_WARN("failed to set default value", K(ret));
            }
          }
        }
      }
    }
    //add routine info
    if (OB_SUCC(ret)) {
      routine_list.push_back(routine_info);
    }
  }
  return ret;
}

int ObCreatePackageBodyResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObCreatePackageStmt *stmt = NULL;
  ParseNode *package_body_block_node = NULL;
  ObString db_name;
  ObString package_name;

  CK (OB_NOT_NULL(parse_tree.children_));
  CK (OB_NOT_NULL(session_info_));
  CK (OB_NOT_NULL(schema_checker_));
  CK (OB_NOT_NULL(schema_checker_->get_schema_guard()));
  CK (OB_NOT_NULL(allocator_));
  CK (OB_NOT_NULL(params_.allocator_));
  CK (OB_NOT_NULL(params_.session_info_));
  CK (OB_NOT_NULL(params_.sql_proxy_));
  CK (OB_LIKELY(T_PACKAGE_CREATE_BODY == parse_tree.type_));
  CK (OB_LIKELY(CREATE_PACKAGE_BODY_NODE_CHILD_COUNT == parse_tree.num_child_));

  if (OB_SUCC(ret) && lib::is_mysql_mode() &&
      OB_SYS_TENANT_ID != session_info_->get_effective_tenant_id()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported package in mysql mode", K(ret), K(lbt()));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "package in mysql mode");
  }

  if (OB_SUCC(ret)
      && OB_ISNULL(stmt = create_stmt<ObCreatePackageStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory for create package stmt failed", K(ret));
  }

  if (OB_SUCC(ret)) {
    ParseNode *sp_name_node = NULL;
    ParseNode *package_name_node = NULL;
    CK (OB_NOT_NULL(package_body_block_node = parse_tree.children_[0]));
    CK (OB_LIKELY(T_PACKAGE_BODY_BLOCK == package_body_block_node->type_));
    CK (OB_LIKELY(PACKAGE_BODY_BLOCK_NODE_CHILD_COUNT == package_body_block_node->num_child_));
    CK (OB_NOT_NULL(package_body_block_node->children_));
    CK (OB_NOT_NULL(sp_name_node = package_body_block_node->children_[0]));
    OZ (ObResolverUtils::resolve_sp_name(*session_info_, *sp_name_node, db_name, package_name));

    // check end label
    if (OB_SUCC(ret)
        && OB_NOT_NULL(package_name_node = package_body_block_node->children_[3])) {
      ObString end_package_name;
      CK (OB_LIKELY(T_IDENT == package_name_node->type_));
      OX (end_package_name = ObString(package_name_node->str_len_, package_name_node->str_value_));
      if (OB_SUCC(ret)
          && OB_UNLIKELY(0 != package_name.case_compare(end_package_name))) {
        ret = OB_ERR_SP_LILABEL_MISMATCH;
        LOG_WARN("end package body name not match", K(ret), K(package_name), K(end_package_name));
        LOG_USER_ERROR(OB_ERR_SP_LILABEL_MISMATCH,
                       end_package_name.length(), end_package_name.ptr());
      }
    }

    //syntax and semantic analysis
    bool is_invoker_right = false;
    if (OB_SUCC(ret)
        && OB_NOT_NULL(package_body_block_node->str_value_)
        && OB_LIKELY(package_body_block_node->str_len_ > 0)) {
      ObString package_body_src(package_body_block_node->str_len_, package_body_block_node->str_value_);
      HEAP_VARS_2((ObPLPackageAST, package_spec_ast, *allocator_),
                  (ObPLPackageAST, package_body_ast, *allocator_)) {
        ObPLPackageGuard package_guard(params_.session_info_->get_effective_tenant_id());
        ObSchemaGetterGuard *schema_guard = schema_checker_->get_schema_mgr();
        ObPLCompiler compiler(*params_.allocator_,
                              *params_.session_info_,
                              *schema_guard,
                              package_guard,
                              *params_.sql_proxy_);
        const ObPackageInfo *package_spec_info = NULL;
        int64_t compatible_mode = lib::is_oracle_mode() ? COMPATIBLE_ORACLE_MODE
                                                        : COMPATIBLE_MYSQL_MODE;
        ObString source;
        OZ (schema_checker_->get_package_info(session_info_->get_effective_tenant_id(),
                                              db_name,
                                              package_name,
                                              PACKAGE_TYPE,
                                              compatible_mode,
                                              package_spec_info));
        if (OB_ERR_PACKAGE_DOSE_NOT_EXIST == ret) {
          ret = OB_ERR_SPEC_NOT_EXIST;
          LOG_USER_ERROR(OB_ERR_SPEC_NOT_EXIST, package_name.length(), package_name.ptr());
        }
        CK (OB_NOT_NULL(package_spec_info));
        OX (is_invoker_right = package_spec_info->is_invoker_right());
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

        OZ (compiler.analyze_package(source,
                                     NULL, package_spec_ast, false));
        OZ (package_body_ast.init(db_name,
                                  package_name,
                                  PL_PACKAGE_BODY,
                                  OB_INVALID_ID,
                                  OB_INVALID_ID,
                                  OB_INVALID_VERSION,
                                  &package_spec_ast));
        OZ (compiler.analyze_package(package_body_src,
                                  &(package_spec_ast.get_body()->get_namespace()),
                                  package_body_ast,
                                  false));
        if (OB_SUCC(ret)) {
          if (package_body_ast.get_serially_reusable()
              != package_spec_ast.get_serially_reusable()) {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("PLS-00709: pragma string must be declared in package specification and body",
                     K(ret),
                     K(package_body_ast.get_serially_reusable()),
                     K(package_spec_ast.get_serially_reusable()));
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "pragma string not declared in package specification and body");
          }
        }
        // update route sql of routine info
        if (OB_SUCC(ret)) {
          obrpc::ObCreatePackageArg &create_package_arg = stmt->get_create_package_arg();
          ObIArray<ObRoutineInfo> &routine_list = create_package_arg.public_routine_infos_;
          const ObPLRoutineTable &spec_routine_table = package_spec_ast.get_routine_table();
          const ObPLRoutineTable &body_routine_table = package_body_ast.get_routine_table();
          ObRoutineInfo routine_info;
          const ObPLRoutineInfo *pl_routine_info = NULL;
          ObSEArray<const ObRoutineInfo *, 2> routine_infos;
          ObSEArray<ObRoutineInfo, 2> routine_spec_infos;
          uint64_t database_id = OB_INVALID_ID;
          OZ (schema_checker_->get_schema_guard()->get_database_id(
            session_info_->get_effective_tenant_id(), db_name, database_id));
          OZ (schema_checker_->get_schema_guard()->get_routine_infos_in_package(
            session_info_->get_effective_tenant_id(), package_spec_info->get_package_id(),
            routine_infos));

          if (OB_SUCC(ret) && routine_infos.empty() && package_spec_ast.get_routine_table().get_count() > 1) {
            OZ (ObCreatePackageResolver::resolve_functions_spec(
              *package_spec_info, routine_spec_infos, package_spec_ast.get_routine_table()));
            CK (routine_spec_infos.count() > 0);
            for (int64_t i = 0; OB_SUCC(ret) && i < routine_spec_infos.count(); ++i) {
              OZ (routine_infos.push_back(&routine_spec_infos.at(i)));
            }
          }


          OZ (update_routine_route_sql(*allocator_, *session_info_, routine_list,
                                       spec_routine_table, body_routine_table, routine_infos));
        }
        if (OB_FAIL(ret) && ret != OB_ERR_UNEXPECTED && ret != OB_ERR_TOO_LONG_IDENT) {
          LOG_USER_WARN(OB_ERR_PACKAGE_COMPILE_ERROR, "PACKAGE BODY",
                        db_name.length(), db_name.ptr(),
                        package_name.length(), package_name.ptr());
          ObPL::insert_error_msg(ret);
          ret = OB_SUCCESS;
        }
        if (OB_SUCC(ret)) {
          ObString dep_attr;
          OZ (ObDependencyInfo::collect_dep_infos(package_body_ast.get_dependency_table(),
                                              stmt->get_create_package_arg().dependency_infos_,
                                              ObObjectType::PACKAGE_BODY,
                                              0, dep_attr, dep_attr));
        }
      }
    }

    //set package body common info
    if (OB_SUCC(ret)) {
      common::ObCompatibilityMode compa_mode = lib::is_mysql_mode() ? common::MYSQL_MODE
                                                                    : common::ORACLE_MODE;
      obrpc::ObCreatePackageArg &create_package_arg = stmt->get_create_package_arg();
      ObPackageInfo &package_info = create_package_arg.package_info_;
      ObString package_body_block(static_cast<int32_t>(package_body_block_node->str_len_),
                                      package_body_block_node->str_value_);

      create_package_arg.is_replace_ = static_cast<bool>(parse_tree.int32_values_[0]);
      create_package_arg.is_editionable_ = !static_cast<bool>(parse_tree.int32_values_[1]);
      create_package_arg.db_name_ = db_name;

      package_info.set_tenant_id(session_info_->get_effective_tenant_id());
      package_info.set_owner_id(session_info_->get_user_id());
      package_info.set_type(PACKAGE_BODY_TYPE);
      package_info.set_compatibility_mode(compa_mode);
      if (!create_package_arg.is_editionable_) {
        create_package_arg.package_info_.set_noneditionable();
      }
      if (is_invoker_right) {
        create_package_arg.package_info_.set_invoker_right();
      }
      OZ (package_info.set_package_name(package_name), package_name);
      OZ (ObSQLUtils::convert_sql_text_to_schema_for_storing(
            *allocator_, session_info_->get_dtc_params(), package_body_block));
      OZ (package_info.set_source(package_body_block), package_body_block);

      if (OB_SUCC(ret)) {
        if (OB_UNLIKELY(OB_SYS_TENANT_ID == session_info_->get_effective_tenant_id())) {
          // 系统租户在创建系统包, 环境变量使用Oracle租户默认的环境变量
          // sql_mode = "PIPES_AS_CONCAT,STRICT_ALL_TABLES,PAD_CHAR_TO_FULL_LENGTH"
          if (common::ORACLE_MODE == compa_mode) {
            OZ (package_info.set_exec_env(ObString("2151677954,45,46,46,")));
          } else {
            OZ (package_info.set_exec_env(ObString("4194304,45,45,45,")));
          }
        } else {
          char buf[OB_MAX_PROC_ENV_LENGTH];
          int64_t pos = 0;
          OZ (ObExecEnv::gen_exec_env(*session_info_, buf, OB_MAX_PROC_ENV_LENGTH, pos));
          OZ (package_info.set_exec_env(ObString(pos, buf)));
        }
      }
      if (OB_SUCC(ret)) {
        ObErrorInfo &error_info = create_package_arg.error_info_;
        ObPackageInfo &pkg_info = create_package_arg.package_info_;
        error_info.collect_error_info(&pkg_info);
      }
    }
    if (OB_NOT_NULL(session_info_)
        && OB_SYS_TENANT_ID == session_info_->get_effective_tenant_id()
        /*&& !session_info_->is_inner()*/) {
      /* NOTE: REMOVE IS_INNER
       * Some system package like dbms_utility may produce warings in create stage under system tenant.
       * It will failed upgrade OCEANBASE.
       * But package still work, It will recompile in normal tenant without warnings.
       * So here, we ignore warnings in system package create stage.
       */
      // 低版本升级到2274, 老的升级脚本中包含了创建Package语句, 部分语句在2274 Server上会产生Warning
      // 比如: Create Package pack IS Procedure proc(x Boolean := 1); End;会报错Boolean表达式默认值非法的Warning
      // 2274的升级脚本还会用最新的Package脚本重建这个包, 为了避免产生的Warning使得升级失败, 这里把Warning清理掉
      common::ob_reset_tsi_warning_buffer();
    }
  }
  return ret;
}

int ObCreatePackageBodyResolver::update_routine_route_sql(ObIAllocator &allocator,
                                                          const ObSQLSessionInfo &session_info,
                                                          ObIArray<ObRoutineInfo> &public_routine_list,
                                                          const ObPLRoutineTable &spec_routine_table,
                                                          const ObPLRoutineTable &body_routine_table,
                                                          ObIArray<const ObRoutineInfo *> &routine_infos)
{
  int ret = OB_SUCCESS;
  const ObPLRoutineInfo *pl_routine_info = NULL;
  ObRoutineInfo routine_info;
  for (int64_t i = ObPLRoutineTable::NORMAL_ROUTINE_START_IDX;
       OB_SUCC(ret) && i < spec_routine_table.get_count(); i++) {
    const ObRoutineInfo* tmp_routine_info = NULL;
    bool found = false;
    OX (routine_info.reset());
    OZ (body_routine_table.get_routine_info(i, pl_routine_info));
    for (int64_t j = 0; OB_SUCC(ret) && j < routine_infos.count(); ++j) {
      tmp_routine_info = routine_infos.at(j);
      if (tmp_routine_info->get_subprogram_id() == i) {
        ObString route_sql = pl_routine_info->get_route_sql();
        ObString routine_body = pl_routine_info->get_routine_body();
        CK (false == found);
        OX (found = true);
        OX (routine_info = *tmp_routine_info);
        OZ (ObSQLUtils::convert_sql_text_to_schema_for_storing(allocator, session_info.get_dtc_params(), route_sql));
        OX (routine_info.set_route_sql(route_sql));
        OZ (ObSQLUtils::convert_sql_text_to_schema_for_storing(allocator, session_info.get_dtc_params(), routine_body));
        OX (routine_info.set_routine_body(routine_body));
        if (OB_SUCC(ret)) {
          if (pl_routine_info->is_modifies_sql_data()) {
            routine_info.set_modifies_sql_data();
          } else if (pl_routine_info->is_reads_sql_data()) {
            routine_info.set_reads_sql_data();
          } else if (pl_routine_info->is_contains_sql()) {
            routine_info.set_contains_sql();
          } else if (pl_routine_info->is_no_sql()) {
            routine_info.set_no_sql();
          }
          if (pl_routine_info->is_wps()) {
            routine_info.set_wps();
          }
          if (pl_routine_info->is_rps()) {
            routine_info.set_rps();
          }
          if (pl_routine_info->is_has_sequence()) {
            routine_info.set_has_sequence();
          }
          if (pl_routine_info->is_has_out_param()) {
            routine_info.set_has_out_param();
          }
          if (pl_routine_info->is_external_state()) {
            routine_info.set_external_state();
          }
        }
      }
    }
    OZ (public_routine_list.push_back(routine_info));
  }
  return ret;
}

} //namespace sql
} //namespace oceanbase
