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
#include "ob_alter_routine_resolver.h"
#include "ob_alter_routine_stmt.h"
#include "share/ob_rpc_struct.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "pl/ob_pl.h"
#include "pl/ob_pl_compile.h"
#include "pl/ob_pl_package.h"
#include "pl/ob_pl_router.h"
#include "pl/ob_pl_stmt.h"
#include "pl/parser/parse_stmt_item_type.h"
#include "lib/utility/ob_defer.h"

#ifdef OB_BUILD_ORACLE_PL
#include "pl/debug/ob_pl_debugger_manager.h"
#endif
namespace oceanbase
{
using namespace common;
using namespace share::schema;
namespace sql
{

int ObAlterRoutineResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;

  CK (OB_NOT_NULL(session_info_));
  CK (OB_NOT_NULL(schema_checker_));
  CK (OB_NOT_NULL(allocator_));
  CK (OB_LIKELY((T_SP_ALTER == parse_tree.type_) || (T_SF_ALTER == parse_tree.type_)));
  CK (OB_LIKELY(2 == parse_tree.num_child_));
  CK (OB_NOT_NULL(parse_tree.children_));
  CK (OB_NOT_NULL(parse_tree.children_[0]));

  if (OB_SUCC(ret)) {
    ObAlterRoutineStmt *alter_routine_stmt = NULL;
    const share::schema::ObRoutineInfo *routine_info = NULL;
    ParseNode *name_node = parse_tree.children_[0];
    ObString db_name;
    ObString sp_name;
    //Step1: resolve routine name and check priv
    CK (OB_NOT_NULL(name_node));
    OZ (ObResolverUtils::resolve_sp_name(*session_info_, *name_node, db_name, sp_name));
    if (OB_SUCC(ret) && ObSchemaChecker::is_ora_priv_check()) { 
      OZ (schema_checker_->check_ora_ddl_priv(session_info_->get_effective_tenant_id(),
                                              session_info_->get_priv_user_id(),
                                              db_name,
                                              stmt::T_ALTER_ROUTINE,
                                              session_info_->get_enable_role_array()));
    }
    //Step2: create alter stmt
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(alter_routine_stmt = create_stmt<ObAlterRoutineStmt>())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory for ObAlterRoutineStmt", K(ret));
    } else {
      crt_resolver_->set_basic_stmt(alter_routine_stmt);
    }
    //Step3: got standalone routine info 
    if (OB_FAIL(ret)) {
    } else if (T_SP_ALTER == parse_tree.type_) {
      OZ (schema_checker_->get_standalone_procedure_info(
        session_info_->get_effective_tenant_id(), db_name, sp_name, routine_info));
    } else {
      OZ (schema_checker_->get_standalone_function_info(
       session_info_->get_effective_tenant_id(), db_name, sp_name, routine_info));
    }
    if (OB_SUCC(ret) && OB_ISNULL(routine_info)) {
      ret = OB_ERR_SP_DOES_NOT_EXIST;
      LOG_USER_ERROR(OB_ERR_SP_DOES_NOT_EXIST,
                     T_SP_ALTER == parse_tree.type_ ? "PROCEDURE" : "FUNCTION",
                     db_name.length(), db_name.ptr(),
                     sp_name.length(), sp_name.ptr());
    }
    //Step4: do real alter resolve
    if (OB_FAIL(ret)) {
    } else if (lib::is_mysql_mode()) {
      OX (alter_routine_stmt->get_routine_arg().routine_info_ = *routine_info);
      OZ (resolve_clause_list(parse_tree.children_[1], alter_routine_stmt->get_routine_arg()));
      OX (alter_routine_stmt->get_routine_arg().db_name_ = db_name);
      OX (alter_routine_stmt->get_routine_arg().routine_info_.set_tenant_id(routine_info->get_tenant_id()));
      OX (alter_routine_stmt->get_routine_arg().routine_info_.set_routine_id(routine_info->get_routine_id()));
      OX (alter_routine_stmt->get_routine_arg().is_need_alter_ = true);
    } else {
      CK (OB_NOT_NULL(parse_tree.children_[1]));
      OZ (resolve_impl(
        alter_routine_stmt->get_routine_arg(), *routine_info, *(parse_tree.children_[1])));
      OX (alter_routine_stmt->get_routine_arg()
        .routine_info_.set_tenant_id(routine_info->get_tenant_id()));
      OX (alter_routine_stmt->get_routine_arg()
        .routine_info_.set_routine_id(routine_info->get_routine_id()));

    }
    //Step5: collection error info
    if (OB_SUCC(ret)) {
      obrpc::ObCreateRoutineArg &crt_routine_arg = alter_routine_stmt->get_routine_arg();
      ObErrorInfo &error_info = crt_routine_arg.error_info_;
      error_info.collect_error_info(&(crt_routine_arg.routine_info_));
    }
  }
  return ret;
}

int ObAlterRoutineResolver::resolve_clause_list(
  const ParseNode *node, obrpc::ObCreateRoutineArg &crt_routine_arg)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(node)) {
    CK (T_SP_CLAUSE_LIST == node->type_);
    for (int64_t i = 0; OB_SUCC(ret) && i < node->num_child_; ++i) {
      const ObStmtNodeTree *child = node->children_[i];
      if (OB_NOT_NULL(child)) {
        if (T_SP_INVOKE == child->type_) {
          if (SP_INVOKER == child->value_) {
            crt_routine_arg.routine_info_.set_invoker_right();
          } else if (SP_DEFINER == child->value_) {
            crt_routine_arg.routine_info_.clear_invoker_right();
          }
        } else if (T_COMMENT == child->type_ && lib::is_mysql_mode()) {
          ObString routine_comment;
          OX (routine_comment = ObString(child->str_len_, child->str_value_));
          OZ (crt_routine_arg.routine_info_.set_comment(routine_comment));
        } else if (T_SP_DATA_ACCESS == child->type_ && lib::is_mysql_mode()) {
          if (SP_NO_SQL == child->value_) {
            crt_routine_arg.routine_info_.set_no_sql();
          } else if (SP_READS_SQL_DATA == child->value_) {
            crt_routine_arg.routine_info_.set_reads_sql_data();
          } else if (SP_MODIFIES_SQL_DATA == child->value_) {
            crt_routine_arg.routine_info_.set_modifies_sql_data();
          } else if (SP_CONTAINS_SQL == child->value_) {
            crt_routine_arg.routine_info_.set_contains_sql();
          }
        } else {
          // do nothing
          /* Currently, ob only support SQL SECURITY and LANGUAGE SQL opt clause,
             other clauses have no real meaning, they are advisory only.
             MYSQL server does not use them to constrain what kinds of statements
             a routine is permitted to execute. */
        }
      }
    }
  }
  return ret;
}

int ObAlterRoutineResolver::resolve_impl(
  obrpc::ObCreateRoutineArg &crt_routine_arg,
  const share::schema::ObRoutineInfo &routine_info, const ParseNode &alter_clause_node)
{
  int ret = OB_SUCCESS;
  if (T_SP_COMPILE_CLAUSE == alter_clause_node.type_) {
    OZ (resolve_compile_clause(crt_routine_arg, routine_info, alter_clause_node));
  } else if (T_SP_EDITIONABLE_CLAUSE == alter_clause_node.type_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported yet!", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "alter editionable");
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unknow alter clause node type", K(ret), K(alter_clause_node.type_));
  }
  return ret;
}

int ObAlterRoutineResolver::resolve_compile_clause(
  obrpc::ObCreateRoutineArg &crt_routine_arg,
  const share::schema::ObRoutineInfo &routine_info, const ParseNode &alter_clause_node)
{
  int ret = OB_SUCCESS;
  ObSEArray<std::pair<share::ObSysVarClassType, ObObj>, 1> params;
  bool reuse_setting = false;
  bool need_recreate = false;
  ObExecEnv old_env;
  ObExecEnv new_env;
  ObArenaAllocator tmp_allocator;

  CK (OB_LIKELY(T_SP_COMPILE_CLAUSE == alter_clause_node.type_));
  CK (OB_LIKELY(1 == alter_clause_node.num_child_));
  if (OB_FAIL(ret)) {
  } else if (OB_NOT_NULL(alter_clause_node.children_[0])) { // compiler parameters
    OZ (resolve_compile_parameters(alter_clause_node.children_[0], params));
  }
  OX (reuse_setting = (1==alter_clause_node.int32_values_[1]) ? true : false);
 
  if (OB_SUCC(ret)) {
    OZ (old_env.load(*session_info_, &tmp_allocator));
    if (reuse_setting) {
      OZ (new_env.init(routine_info.get_exec_env()));
    } else {
      OZ (new_env.load(*session_info_, &tmp_allocator));
    }
    if (params.count() > 0) {
      for (int64_t i = 0; OB_SUCC(ret) && i < params.count(); ++i) {
        if (share::SYS_VAR_PLSQL_CCFLAGS == params.at(i).first) {
          ObString plsql_ccflags = params.at(i).second.get_string();
          new_env.set_plsql_ccflags(plsql_ccflags); 
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected compile parameter", K(ret), K(params));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    ObExecEnv routine_env;
    OZ (routine_env.init(routine_info.get_exec_env()));
    OX (need_recreate = (routine_env == new_env) ? false : true);
  }
  if (OB_SUCC(ret)) {
    const ParseNode *parse_tree = NULL;
    OZ (new_env.store(*session_info_));
    OZ (parse_routine(routine_info.get_routine_body(), parse_tree));
    CK (OB_NOT_NULL(parse_tree));
    OZ (resolve_routine(crt_routine_arg, routine_info, need_recreate, parse_tree));
    if (OB_SUCC(ret)
          && 1 == alter_clause_node.int32_values_[0]  // ALTER COMPILE DEBUG
          && !need_recreate
          && session_info_->get_pl_attached_id() > 0
          && OB_INVALID_ID != session_info_->get_pl_attached_id()) {
      OZ (register_debug_info(routine_info));
    }
    OZ (old_env.store(*session_info_));
  }
  return ret;
}

int ObAlterRoutineResolver::resolve_compile_parameters(
  const ParseNode *compile_params_node,
  ObIArray<std::pair<share::ObSysVarClassType, ObObj> > &params)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(compile_params_node));
  CK (T_VARIABLE_SET == compile_params_node->type_);
  CK (OB_NOT_NULL(compile_params_node->children_));
  for (int64_t i = 0; OB_SUCC(ret) && i < compile_params_node->num_child_; ++i) {
    const ParseNode *param = compile_params_node->children_[i];
    const ParseNode *ident = NULL;
    const ParseNode *value = NULL;
    CK (OB_NOT_NULL(param));
    CK (T_VAR_VAL == param->type_);
    CK (2 == param->num_child_);
    CK (OB_NOT_NULL(param->children_));
    CK (OB_NOT_NULL(ident = param->children_[0]));
    CK (OB_NOT_NULL(value = param->children_[1]));
    OZ (resolve_compile_parameter(ident, value, params));
  }
  return ret;
}

int ObAlterRoutineResolver::resolve_compile_parameter(
  const ParseNode *ident, const ParseNode *value,
  ObIArray<std::pair<share::ObSysVarClassType, ObObj> > &params)
{
  int ret = OB_SUCCESS;
  ObString var_name;
  share::ObSysVarClassType var_type = share::SYS_VAR_INVALID;
  ObObj var_val;

  CK (T_IDENT == ident->type_);
  CK (T_INT == value->type_ || T_VARCHAR == value->type_);
  OX (var_name = ObString(ident->str_len_, ident->str_value_));
  CK (!var_name.empty());

  // In Oracle, All compile parameter as follow:
  // PLSCOPE_SETTINGS (In OB: not supported yet)
  // PLSQL_CCFLAGS (In OB: support as variable, used by condition compilation)
  // PLSQL_CODE_TYPE (In OB: support as parameter, not used)
  // PLSQL_OPTIMIZE_LEVEL (In OB: support as paramter, not used)
  // PLSQL_WARNINGS (In OB: support as variable, but not used)
  // NLS_LENGTH_SEMANTICS (In OB: support as variable, used by expr)
  // PERMIT_92_WRAP_FORMAT (In OB: not supported yet)

  if (0 == var_name.case_compare("PLSQL_CCFLAGS")) {
    var_type = share::SYS_VAR_PLSQL_CCFLAGS;
  } else if (0 == var_name.case_compare("PLSCOPE_SETTINGS")
      || 0 == var_name.case_compare("PLSQL_CODE_TYPE")
      || 0 == var_name.case_compare("PLSQL_OPTIMIZE_LEVEL")
      || 0 == var_name.case_compare("PLSQL_WARNINGS")
      || 0 == var_name.case_compare("NLS_LENGTH_SEMANTICS")
      || 0 == var_name.case_compare("PERMIT_92_WRAP_FORMAT")) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support compile parameter", K(ret), K(var_name));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "compile parameter");
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support compile parameter", K(ret), K(var_name));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "compile parameter");
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < params.count(); ++i) {
    if (params.at(i).first == var_type) {
      ret = OB_ERR_DUP_COMPILE_PARAM;
      LOG_WARN("ORA-39956: duplicate setting for PL/SQL compiler parameter string");
      LOG_USER_ERROR(OB_ERR_DUP_COMPILE_PARAM, var_name.length(), var_name.ptr());
    }
  }

  if (OB_FAIL(ret)) {
  } else if (T_INT == value->type_) {
    var_val.set_int(value->value_);
  } else if (T_VARCHAR == value->type_) {
    ObString str(value->str_len_, value->str_value_);
    var_val.set_varchar(str);
  }
  OZ (params.push_back(std::make_pair(var_type, var_val)));
  return ret;
}

int ObAlterRoutineResolver::parse_routine(
  const ObString &source, const ParseNode *&parse_tree)
{
  int ret = OB_SUCCESS;
  ObDataTypeCastParams dtc_params = session_info_->get_dtc_params();
  pl::ObPLParser parser(*(params_.allocator_), session_info_->get_charsets4parser(), session_info_->get_sql_mode());
  ParseResult parse_result;
  ObString body = source;
  MEMSET(&parse_result, 0, SIZEOF(ParseResult));
  OZ (ObSQLUtils::convert_sql_text_from_schema_for_resolve(
                                  *(params_.allocator_), dtc_params, body));
  OZ (parser.parse(body, body, parse_result));
  CK (OB_NOT_NULL(parse_result.result_tree_));
  CK (T_STMT_LIST == parse_result.result_tree_->type_);
  CK (1 == parse_result.result_tree_->num_child_);
  CK (OB_NOT_NULL(parse_result.result_tree_->children_));
  CK (OB_NOT_NULL(parse_tree = parse_result.result_tree_->children_[0]));

  if (OB_SUCC(ret) && T_SP_PRE_STMTS == parse_tree->type_) {
    OZ (pl::ObPLResolver::resolve_condition_compile(
      *(params_.allocator_),
      session_info_,
      params_.schema_checker_->get_schema_guard(),
      NULL,
      params_.sql_proxy_,
      NULL,
      parse_tree,
      parse_tree));
  }
  return ret;
}

int ObAlterRoutineResolver::resolve_routine(
  obrpc::ObCreateRoutineArg &crt_routine_arg,
  const share::schema::ObRoutineInfo &routine_info,
  bool need_recreate, const ParseNode *source_tree)
{
  int ret = OB_SUCCESS;
  ParseNode *crt_tree = NULL;
  CK (OB_NOT_NULL(source_tree));
  CK (T_SP_SOURCE == source_tree->type_
      || T_SF_SOURCE == source_tree->type_
      || T_SF_AGGREGATE_SOURCE == source_tree->type_);
  CK (lib::is_oracle_mode());
  if (OB_SUCC(ret)
      && OB_ISNULL(crt_tree = new_non_terminal_node(
          params_.allocator_,
          T_SP_SOURCE == source_tree->type_ ? T_SP_CREATE : T_SF_CREATE,
          1,
          source_tree))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed alloc memory for create routine node", K(ret));
  }
  OX (crt_tree->int32_values_[0] = need_recreate ? 1 : 0);
  OX (crt_tree->int32_values_[1] = routine_info.is_noneditionable() ? 1 : 0);
  CK (OB_NOT_NULL(crt_resolver_));
  OZ (crt_resolver_->resolve_impl(*crt_tree, &crt_routine_arg));
  return ret;
}

int ObAlterRoutineResolver::register_debug_info(const share::schema::ObRoutineInfo &routine_info)
{
  int ret = OB_SUCCESS;

#ifndef OB_BUILD_ORACLE_PL
  UNUSED(routine_info);
#else
  CK (OB_NOT_NULL(session_info_));
  CK (OB_NOT_NULL(session_info_->get_pl_engine()));

  if (OB_FAIL(ret)) {
    // do nothing
  } else {
    uint32_t id = session_info_->get_pl_attached_id();
    pl::ObPLDebuggerGuard guard(id);
    pl::debugger::ObPLDebugger *pl_debugger = nullptr;
    ObExecContext *exec_ctx = nullptr;

    if (OB_FAIL(guard.get(pl_debugger))) {
      LOG_WARN("failed get pl debugger", K(ret));
    } else if (OB_ISNULL(pl_debugger) || !pl_debugger->is_debug_on()) {
      // do nothing
    } else if (OB_ISNULL(exec_ctx = session_info_->get_cur_exec_ctx())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get cur exec context", K(session_info_));
    } else if (OB_ISNULL(exec_ctx->get_package_guard())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get package guard", K(session_info_), K(exec_ctx));
    } else {
      pl::ObPLFunction *routine = nullptr;
      pl::ObPLFunction *local_routine = nullptr;
      ObCacheObjGuard cacheobj_guard(PL_ROUTINE_HANDLE);
      ObArray<int64_t> subprogram_path;  // empty array

      ObSqlString const_name;
      common::ObIArray<jit::ObDWARFHelper*> *pl_dwarf_helpers = nullptr;

      pl::debugger::ObPLDebugger *old_debugger = session_info_->get_pl_debugger();
      session_info_->set_pl_debugger(pl_debugger);

      // always set pl debugger back to old debugger
      DEFER(session_info_->set_pl_debugger(old_debugger));

      if (OB_FAIL(session_info_
                    ->get_pl_engine()
                    ->get_pl_function(*exec_ctx,
                                      *exec_ctx->get_package_guard(),
                                      routine_info.get_package_id(),
                                      routine_info.get_routine_id(),
                                      subprogram_path,
                                      cacheobj_guard,
                                      local_routine))) {
        LOG_WARN("failed to compile pl function", K(ret), K(routine_info));
      } else if (nullptr != local_routine) {
        routine = local_routine;
      } else {
        routine = static_cast<pl::ObPLFunction*>(cacheobj_guard.get_cache_obj());
      }

      if (OB_FAIL(ret)) {
        // do nothing
      } else if (OB_ISNULL(routine)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected NULL routine", K(local_routine), K(cacheobj_guard));
      } else if (OB_ISNULL(pl_dwarf_helpers = pl_debugger->get_dwarf_helpers())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected NULL pl dwarf helpers", K(pl_debugger->get_dwarf_helpers()));
      } else if (OB_FAIL(const_name.append(routine->get_name_debuginfo().owner_name_))) {
        LOG_WARN("failed to append owner name", K(ret), K(routine->get_name_debuginfo().owner_name_));
      } else if (OB_FAIL(const_name.append("."))) {
        LOG_WARN("failed to append sep .", K(ret));
      } else if (OB_FAIL(const_name.append(routine->get_name_debuginfo().routine_name_))) {
        LOG_WARN("failed to append routine name", K(ret), K(routine->get_name_debuginfo().routine_name_));
      } else {
        int64_t line = 1;
        int64_t address = -1;
        for (int64_t i = 0; OB_SUCC(ret) && address == -1 && i < pl_dwarf_helpers->count(); ++i) {
          jit::ObDWARFHelper* dwarf_helper = pl_dwarf_helpers->at(i);
          if (OB_ISNULL(dwarf_helper)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected NULL dwarf helper", KPC(pl_dwarf_helpers));
          } else if (OB_FAIL(dwarf_helper->find_address_by_function_line(
                                            const_name.string(),
                                            line,
                                            address))) {
            LOG_WARN("failed to find address by function line", K(ret), K(const_name), K(line));
          }
        }

        if (OB_SUCC(ret) && address == -1) {
          char *copy = nullptr;
          jit::ObDIRawData di_raw_data = routine->get_debug_info();
          jit::ObDWARFHelper* dwarf_helper = nullptr;

          if (di_raw_data.empty()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected empty di raw data", K(di_raw_data.get_size()), K(di_raw_data.get_data()));
          } else if (OB_ISNULL(pl_debugger->get_allocator())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected NULL allocator in pl_debugger", K(pl_debugger));
          } else if (OB_ISNULL(copy = static_cast<char *>(
                                  pl_debugger->get_allocator()->alloc(di_raw_data.get_size())))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to alloc memory for debug info", K(di_raw_data.get_size()));
          } else if (OB_ISNULL(MEMCPY(copy, di_raw_data.get_data(), di_raw_data.get_size()))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to MEMCPY debug info",
                    "dst", copy,
                    K(di_raw_data.get_data()),
                    K(di_raw_data.get_size()));
          } else if (OB_ISNULL(dwarf_helper = static_cast<jit::ObDWARFHelper *>(
                                  pl_debugger->get_allocator()->alloc(sizeof(jit::ObDWARFHelper))))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to alloc memory for dwarf helper",K(sizeof(jit::ObDWARFHelper)));
          } else if (OB_ISNULL(
                        dwarf_helper = new (dwarf_helper)
                            jit::ObDWARFHelper(*pl_debugger->get_allocator(),
                                                copy,
                                                di_raw_data.get_size()))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to placement new ObDWARFHelper");
          } else if (OB_FAIL(dwarf_helper->init())) {
            LOG_WARN("failed to init dwarf helper", K(ret));
          } else if (OB_FAIL(pl_debugger->get_debugger_ctrl().register_debug_info(dwarf_helper))) {
            LOG_WARN("failed to register debug info", K(ret));
          }
        }
      }
    }
  }
#endif

  return ret;
}
} // namespace sql
} //namespace oceanbase


