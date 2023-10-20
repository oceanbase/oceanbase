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

#include "lib/worker.h"
#include "pl/ob_pl_resolver.h"
#include "pl/ob_pl_stmt.h"
#include "pl/ob_pl_router.h"
#include "common/sql_mode/ob_sql_mode_utils.h"
#include "pl/ob_pl_package.h"
#include "pl/parser/parse_stmt_item_type.h"
#include "pl/ob_pl_exception_handling.h"
#include "pl/ob_pl_interface_pragma.h"
#include "sql/parser/ob_parser.h"
#include "sql/parser/parse_malloc.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/resolver/expr/ob_raw_expr_printer.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "sql/resolver/ddl/ob_ddl_resolver.h"
#include "sql/resolver/expr/ob_raw_expr_resolver_impl.h"
#include "sql/resolver/dml/ob_select_resolver.h"
#include "sql/resolver/expr/ob_raw_expr_wrap_enum_set.h"
#include "observer/ob_server_struct.h"
#include "sql/rewrite/ob_transform_pre_process.h"
#include "share/schema/ob_trigger_info.h"
#include "sql/resolver/expr/ob_raw_expr_copier.h"
#ifdef OB_BUILD_ORACLE_PL
#include "pl/ob_pl_warning.h"
#include "pl/ob_pl_udt_object_manager.h"
#include "pl/sys_package/ob_json_pl_utils.h"
#include "pl/dblink/ob_pl_dblink_util.h"
#endif
namespace oceanbase
{
using namespace common;
using namespace share;
using namespace sql;
using namespace share::schema;

namespace pl
{

const char *ObPLResolver::ANONYMOUS_BLOCK   = "__anonymous_block__";
const char *ObPLResolver::ANONYMOUS_ARG     = "__anonymous_argument__";
const char *ObPLResolver::ANONYMOUS_SQL_ARG = "__anonymous_argument__for_static_sql__";
const char *ObPLResolver::ANONYMOUS_INOUT_ARG  = "__anonymous_argument__for_inout__";

int ObPLResolver::init(ObPLFunctionAST &func_ast)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(make_block(func_ast, NULL, current_block_, true))) {
    LOG_WARN("failed to make block", K(current_block_), K(ret));
  } else {
    current_level_ = 0;
    func_ast.set_body(current_block_);
    arg_cnt_ = func_ast.get_arg_count();
    question_mark_cnt_ = 0;
    external_ns_.set_dependency_table(&func_ast.get_dependency_table());
    if (!OB_ISNULL(external_ns_.get_parent_ns())) {
      uint64_t type_start_gen_id = external_ns_.get_parent_ns()->get_type_table()->get_type_start_gen_id();
      func_ast.get_user_type_table().set_type_start_gen_id(type_start_gen_id);
    }
    if (OB_SUCC(ret) && lib::is_oracle_mode() && !func_ast.is_udt_cons()) {
      OZ (current_block_->get_namespace().add_label(
        func_ast.get_name(), ObPLLabelTable::ObPLLabelType::LABEL_BLOCK, NULL));
    }
    if (OB_SUCC(ret)) {
      if (!resolve_ctx_.package_guard_.is_inited()) {
        if (OB_FAIL(resolve_ctx_.package_guard_.init())) {
          LOG_WARN("package guard init failed", K(ret));
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < func_ast.get_arg_count(); ++i) {
      if (OB_FAIL(current_block_->get_namespace().get_symbols().push_back(i))) {
        LOG_WARN("failed to add variable to symbol table", K(i),K(ret));
      } else {
        common::ObString name;
        ObPLDataType type;
        OZ (func_ast.get_argument(i, name, type));
        if (OB_SUCC(ret) && type.is_cursor_type()) {
          OZ (current_block_->get_namespace().get_cursors().push_back(
              current_block_->get_namespace().get_cursors().count()));
        }
        if (OB_SUCC(ret) && type.is_composite_type()) { // 将参数中的复杂类型在本ns中展开, 避免参数类型在符号表中不全
          if (type.is_local_type()
              && STANDALONE_ANONYMOUS == func_ast.get_proc_type()) {
            OZ (current_block_->get_namespace().get_types().push_back(
                current_block_->get_namespace().get_types().count()));
          } else {
            const ObUserDefinedType *user_type = NULL;
            OZ (current_block_->get_namespace().get_pl_data_type_by_id(type.get_user_type_id(), user_type), type, i);
            CK (OB_NOT_NULL(user_type));
            OZ (user_type->get_all_depended_user_type(resolve_ctx_, current_block_->get_namespace()), user_type);
          }
        }
      }
    }
    if (func_ast.is_function()) {
      const ObPLDataType &type = func_ast.get_ret_type();
      if (type.is_composite_type()) {
        const ObUserDefinedType *user_type = NULL;
        OZ (current_block_->get_namespace().get_pl_data_type_by_id(type.get_user_type_id(), user_type), type);
        CK (OB_NOT_NULL(user_type));
        OZ (user_type->get_all_depended_user_type(resolve_ctx_, current_block_->get_namespace()), user_type);
      }

    }
  }
  return ret;
}

int ObPLResolver::init_default_exprs(
  ObPLFunctionAST &func, const ObIArray<share::schema::ObRoutineParam *> &params)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0, idx = 0; OB_SUCC(ret) && i < params.count(); ++i) {
    if (params.at(i)->is_ret_param()) {
      // do nothing ...
    } else {
      OZ (init_default_expr(func, idx, *(params.at(i))));
      OX (idx++);
    }
  }
  return ret;
}

int ObPLResolver::init_default_exprs(
  ObPLFunctionAST &func, const ObIArray<ObPLRoutineParam *> &params)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < params.count(); ++i) {
    CK (OB_NOT_NULL(params.at(i)));
    OZ (init_default_expr(func, i, *(params.at(i))));
  }
  return ret;
}

int ObPLResolver::init_default_expr(
  ObPLFunctionAST &func_ast, int64_t idx, const share::schema::ObIRoutineParam &param)
{
  int ret = OB_SUCCESS;
  if (!param.get_default_value().empty()) {
    ObPLSymbolTable &symbol_table = func_ast.get_symbol_table();
    const ObPLVar *var = symbol_table.get_symbol(idx);
    CK (OB_NOT_NULL(var));
    OZ (init_default_expr(func_ast, param, var->get_type()));
    OX ((const_cast<ObPLVar *>(var))->set_default(func_ast.get_expr_count() - 1));
  }
  return ret;
}

int ObPLResolver::init_default_expr(ObPLFunctionAST &func_ast,
                                    const share::schema::ObIRoutineParam &param,
                                    const ObPLDataType &expected_type)
{
  int ret = OB_SUCCESS;
  if (!param.get_default_value().empty()) {
    const ParseNode *default_node = NULL;
    ObRawExpr *default_expr = NULL;
    ObString default_value = param.get_default_value();
    OZ (ObSQLUtils::convert_sql_text_from_schema_for_resolve(
          resolve_ctx_.allocator_, resolve_ctx_.session_info_.get_dtc_params(), default_value));
    OZ (ObRawExprUtils::parse_default_expr_from_str(
      default_value,
      resolve_ctx_.session_info_.get_charsets4parser(),
      resolve_ctx_.allocator_,
      default_node));
    CK (OB_NOT_NULL(default_node));
    OZ (resolve_expr(default_node, func_ast, default_expr,
         combine_line_and_col(default_node->stmt_loc_), true, &expected_type));
    CK (OB_NOT_NULL(default_expr));
  }
  return ret;
}

#define RESOLVE_CHILDREN(node, compile_unit) \
  do { \
    if (OB_NOT_NULL(node)) { \
      for (int64_t i = 0; OB_SUCC(ret) && i < node->num_child_; ++i) { \
        if (NULL != node->children_[i]) { \
          OZ (SMART_CALL(resolve(node->children_[i], compile_unit))); \
        } \
      } \
    } \
  } while (0)

#define RESOLVE_STMT(stmt_type, resolve_func, stmt_def) \
  do { \
    ObPLStmtType type = stmt_type; \
    if (OB_FAIL(stmt_factory_.allocate(type, current_block_, stmt))) { \
      LOG_WARN("failed to alloc stmt", K(ret)); \
    } else if (OB_FAIL(resolve_func(parse_tree, static_cast<stmt_def*>(stmt), func))) { \
      LOG_WARN("failed to resolve pl stmt", K(parse_tree->type_), K(parse_tree), K(stmt), K(ret)); \
    } else if (type != PL_SQL) { \
      func.set_is_all_sql_stmt(false); \
    } \
  } while (0)

#define NO_EXCEPTION_STMT(type) ( PL_BLOCK == type \
    || PL_USER_TYPE == type \
    || PL_LEAVE == type \
    || PL_ITERATE == type \
    || PL_LOOP == type \
    || PL_COND == type \
    || PL_HANDLER == type \
    || PL_CURSOR == type \
    || PL_ROUTINE_DEF == type \
    || PL_ROUTINE_DECL == type \
  )

int ObPLResolver::resolve(const ObStmtNodeTree *parse_tree, ObPLFunctionAST &func)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(parse_tree)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("parse_tree is NULL", K(parse_tree), K(ret));
  } else {
    ObPLStmt *stmt = NULL;
    ObString label;
    ObSEArray<ObString, 4> labels;
    ObString end_label;
    ObPLLabelTable *pl_label = NULL;
    int64_t label_idx = OB_INVALID_INDEX;
    if (T_SP_LABELED_BLOCK == parse_tree->type_ || T_SP_LABELED_CONTROL == parse_tree->type_) {
      // Oracle可以只有EndLabel,没有BeginLabel, 且EndLabel可以与BeginLabel不匹配
      // 如下用例在Oracle下是合法的
      // example 1: <<label>> BEGIN NULL; END label1;
      // example 2: BEGIN NULL; END label;
      ParseNode *labels_node = parse_tree->children_[0];
      if (OB_NOT_NULL(labels_node)) {
        if (T_LABEL_LIST == labels_node->type_) {
          for (int64_t i = 0; OB_SUCC(ret) && i < labels_node->num_child_; ++i) {
            label.reset();
            if (OB_FAIL(resolve_ident(labels_node->children_[i], label))) {
              LOG_WARN("failed to resolve ident", K(labels_node->children_[i]), K(ret));
            } else if (OB_FAIL(labels.push_back(label))) {
              LOG_WARN("failed to push labels", K(label), K(labels.count()), K(ret));
            } else {
              // do nothing
            }
          }
        } else if (OB_FAIL(resolve_ident(parse_tree->children_[0], label))) {
          LOG_WARN("failed to resolve ident",  K(parse_tree->children_[0]), K(ret));
        } else if (OB_FAIL(labels.push_back(label))) {
          LOG_WARN("failed to push labels", K(label), K(labels.count()), K(ret));
        }
      }
      if (OB_SUCC(ret) && NULL != parse_tree->children_[2]) {
        if (OB_FAIL(resolve_ident(parse_tree->children_[2], end_label))) {
          LOG_WARN("failed to resolve ident", K(parse_tree->children_[2]), K(ret));
        } else if (lib::is_mysql_mode()) {
          bool find_flag = false;
          for (int64_t i = 0; i < labels.count(); ++i) {
            if (0 == labels.at(i).case_compare(end_label)) {
              find_flag = true;
            }
          }
          if (!find_flag) {
            ret =  OB_ERR_SP_LILABEL_MISMATCH;
            LOG_WARN("begin label is not match with end label", K(label), K(end_label), K(ret));
          }
        } else { // Oracle的EndLabel行为, 仅对Procedure和Function结尾的EndLabel做匹配检查
          if (NULL != current_block_
              && NULL == current_block_->get_namespace().get_pre_ns()) {
            if (func.get_name().case_compare("__anonymous_block__") != 0
                && end_label.compare(func.get_name()) != 0) {
              ret =  OB_ERR_END_LABEL_NOT_MATCH;
              LOG_WARN("begin label is not match with end label", K(label), K(end_label), K(ret));
              LOG_USER_ERROR(OB_ERR_END_LABEL_NOT_MATCH,
                             end_label.length(), end_label.ptr(),
                             func.get_name().length(), func.get_name().ptr());
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (NULL != current_block_ && OB_NOT_NULL(parse_tree->children_[0])) {
          ObPLLabelTable::ObPLLabelType type =
            T_SP_LABELED_BLOCK == parse_tree->type_ ?
              ObPLLabelTable::ObPLLabelType::LABEL_BLOCK
            : ObPLLabelTable::ObPLLabelType::LABEL_CONTROL;
          for (int64_t i = 0; i < labels.count(); ++i) {
            OZ (current_block_->get_namespace().add_label(labels.at(i), type, NULL),
                                                                labels.at(i), type);
          }
          pl_label = current_block_->get_namespace().get_label_table();
          label_idx = pl_label->get_count() - 1;
        } else { /*do nothing*/ }
        parse_tree = parse_tree->children_[1];
      }
    }

    if (OB_SUCC(ret)) {
      switch (parse_tree->type_) {
      case T_SP_PROC_STMT_LIST:
      case T_SP_BLOCK_CONTENT: {
        ObPLStmtBlock *block = NULL;
        ++current_level_;
        if (OB_FAIL(resolve_stmt_list(parse_tree, block, func))) {
          LOG_WARN("failed to resolve stmt list", K(parse_tree->type_), K(ret));
        } else if (OB_FAIL(handler_analyzer_.reset_handlers(current_level_))) {
          LOG_WARN("failed to reset handlers", K(ret));
        } else {
          --current_level_;
          handler_analyzer_.reset_notfound_and_warning(current_level_);
          if (lib::is_oracle_mode() && current_level_ <= 1 && func.is_autonomous()) {
            block->set_is_autonomous();
          }
          stmt = block;
        }
      }
        break;
      case T_SP_DECL_LIST: {
        RESOLVE_CHILDREN(parse_tree, func);
      }
        break;
      case T_SP_DECL: {
        const ObStmtNodeTree *type_node = parse_tree->children_[1];
        ObPLDataType data_type;
        ObString ident_name;
        CK (OB_NOT_NULL(type_node));
        OZ (resolve_sp_data_type(type_node, ident_name, func, data_type));
        if (OB_SUCC(ret)) {
          if (!data_type.is_cursor_type() || data_type.is_rowtype_type()) {
            RESOLVE_STMT(PL_VAR, resolve_declare_var, ObPLDeclareVarStmt);
          } else {
#ifdef OB_BUILD_ORACLE_PL
            RESOLVE_STMT(PL_CURSOR, resolve_declare_ref_cursor, ObPLDeclareCursorStmt);
            func.set_modifies_sql_data();
#endif
          }
        }
      }
        break;
#ifdef OB_BUILD_ORACLE_PL
      case T_SP_DECL_USER_TYPE: {
        RESOLVE_STMT(PL_USER_TYPE, resolve_declare_user_type, ObPLDeclareUserTypeStmt);
      }
        break;
      case T_SP_DECL_USER_SUBTYPE: {
        RESOLVE_STMT(PL_USER_SUBTYPE, resolve_declare_user_subtype, ObPLDeclareUserTypeStmt);
      }
#endif
        break;
      case T_VARIABLE_SET: {
        RESOLVE_STMT(PL_ASSIGN, resolve_assign, ObPLAssignStmt);
      }
        break;
      case T_SP_IF: {
        RESOLVE_STMT(PL_IF, resolve_if, ObPLIfStmt);
      }
        break;
      case T_SP_CASE: {
        RESOLVE_STMT(PL_CASE, resolve_case, ObPLCaseStmt);
      }
        break;
      case T_SP_ITERATE: {
        RESOLVE_STMT(PL_ITERATE, resolve_iterate, ObPLIterateStmt);
      }
        break;
      case T_SP_LEAVE: {
        RESOLVE_STMT(PL_LEAVE, resolve_leave, ObPLLeaveStmt);
      }
        break;
      case T_SP_WHILE: {
        RESOLVE_STMT(PL_WHILE, resolve_while, ObPLWhileStmt);
      }
        break;
      case T_SP_FOR_LOOP: {
        RESOLVE_STMT(PL_FOR_LOOP, resolve_for_loop, ObPLForLoopStmt);
      }
        break;
      case T_SP_CURSOR_FOR_LOOP: {
        RESOLVE_STMT(PL_CURSOR_FOR_LOOP, resolve_cursor_for_loop, ObPLCursorForLoopStmt);
        if (!func.is_modifies_sql_data()) {
          func.set_reads_sql_data();
        }
      }
        break;
      case T_SP_FORALL: {
        RESOLVE_STMT(PL_FORALL, resolve_forall, ObPLForAllStmt);
        func.set_modifies_sql_data();
      }
        break;
      case T_SP_REPEAT: {
        RESOLVE_STMT(PL_REPEAT, resolve_repeat, ObPLRepeatStmt);
      }
        break;
      case T_SP_LOOP: {
        RESOLVE_STMT(PL_LOOP, resolve_loop, ObPLLoopStmt);
      }
        break;
      case T_SP_RETURN: {
        RESOLVE_STMT(PL_RETURN, resolve_return, ObPLReturnStmt);
        // OB_ERR_FUNCTION_UNKNOWN need set too.
        // case: CREATE FUNCTION f1() RETURNS INT RETURN f1()
        if (lib::is_mysql_mode()
            && (OB_SUCCESS == ret
                || OB_ERR_FUNCTION_UNKNOWN == ret
                || OB_ERR_SP_DOES_NOT_EXIST == ret)) {
          func.set_return();
        }
      }
        break;
      case T_TRANSACTION:
      case T_SQL_STMT: {
        RESOLVE_STMT(PL_SQL, resolve_sql, ObPLSqlStmt);
      }
        break;
      case T_SP_EXECUTE_IMMEDIATE: {
        RESOLVE_STMT(PL_EXECUTE, resolve_execute_immediate, ObPLExecuteStmt);
        func.set_modifies_sql_data();
      }
        break;
      case T_SP_DO: {
        RESOLVE_STMT(PL_DO, resolve_do, ObPLDoStmt);
        if (lib::is_mysql_mode()) {
          if (!func.is_reads_sql_data() && !func.is_modifies_sql_data()) {
            func.set_contains_sql();
          }
        }
      }
        break;
      case T_SP_EXTEND: {
        RESOLVE_STMT(PL_EXTEND, resolve_extend, ObPLExtendStmt);
      }
        break;
      case T_SP_INIT_PRAGMA:
      case T_SP_DECL_COND: {
        RESOLVE_STMT(PL_COND, resolve_declare_cond, ObPLDeclareCondStmt);
      }
        break;
      case T_SP_DECL_HANDLER: {
        if (OB_ISNULL(current_block_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("current block is NULL", K(current_block_), K(ret));
        } else {
          if (current_block_->has_eh()) {
            stmt = NULL;
          } else if (OB_FAIL(stmt_factory_.allocate(PL_HANDLER, current_block_, stmt))) {
            LOG_WARN("failed to alloc stmt", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(check_declare_order(PL_HANDLER))) {
            LOG_WARN("fail to check decalre order", K(ret));
          } else if (OB_FAIL(resolve_declare_handler(parse_tree, static_cast<ObPLDeclareHandlerStmt*>(NULL == stmt ? current_block_->get_eh() : stmt), func))) {
            LOG_WARN("failed to resolve declare handler stmt", K(parse_tree), K(stmt), K(ret));
          } else if (stmt != NULL && static_cast<ObPLDeclareHandlerStmt*>(stmt)->get_handlers().count() <= 0) {
            // continue handler will record into handler_analyzer_,
            // so here handlers may null, do not add null handler to current block.
            stmt = NULL;
          }
          if (OB_SUCC(ret)) {
            func.set_is_all_sql_stmt(false);
          }
        }
      }
        break;
      case T_SP_RESIGNAL: {
        RESOLVE_STMT(PL_SIGNAL, resolve_resignal, ObPLSignalStmt);
        if (lib::is_mysql_mode() && !func.is_reads_sql_data() && !func.is_modifies_sql_data()) {
          func.set_contains_sql();
        }
      }
        break;
      case T_SP_SIGNAL: {
        RESOLVE_STMT(PL_SIGNAL, resolve_signal, ObPLSignalStmt);
        if (lib::is_mysql_mode() && !func.is_reads_sql_data() && !func.is_modifies_sql_data()) {
          func.set_contains_sql();
        }
      }
        break;
      case T_SP_CALL_STMT: {
        RESOLVE_STMT(PL_CALL, resolve_call, ObPLCallStmt);
        func.set_external_state();
      }
        break;
      case T_SP_INNER_CALL_STMT: {
        if (OB_FAIL(resolve_inner_call(parse_tree, stmt, func))) {
          LOG_WARN("failed to resolve inner call", K(parse_tree), K(ret));
        } else {
          func.set_is_all_sql_stmt(false);
          func.set_external_state();
        }
      }
        break;
      case T_SP_DECL_CURSOR: {
        if (OB_FAIL(check_declare_order(PL_CURSOR))) {
          LOG_WARN("fail to check decalre order", K(ret));
        } else {
          RESOLVE_STMT(PL_CURSOR, resolve_declare_cursor, ObPLDeclareCursorStmt);
          if (!func.is_modifies_sql_data()) {
            func.set_reads_sql_data();
          }
        }
      }
        break;
      case T_SP_PROC_OPEN: {
        if (NULL == parse_tree->children_[2]) {
          RESOLVE_STMT(PL_OPEN, resolve_open, ObPLOpenStmt);
        } else {
          RESOLVE_STMT(PL_OPEN_FOR, resolve_open_for, ObPLOpenForStmt);
        }
      }
        break;
      case T_SP_PROC_FETCH: {
        RESOLVE_STMT(PL_FETCH, resolve_fetch, ObPLFetchStmt);
      }
        break;
      case T_SP_PROC_CLOSE: {
        RESOLVE_STMT(PL_CLOSE, resolve_close, ObPLCloseStmt);
      }
        break;
      case T_SP_NULL: {
        RESOLVE_STMT(PL_NULL, resolve_null, ObPLNullStmt);
      }
        break;
      case T_SP_PIPE_ROW: {
        RESOLVE_STMT(PL_PIPE_ROW, resolve_pipe_row, ObPLPipeRowStmt);
      }
        break;
      case T_STMT_LIST: //TODO: delete me ..@rudian
      case T_PACKAGE_BODY_STMTS: {
        RESOLVE_CHILDREN(parse_tree, func);
      }
        break;
      case T_SP_SOURCE: {
        if (OB_FAIL(resolve_root(parse_tree->children_[3], func))) {
          LOG_WARN("failed to resolve sp source", K(current_block_), K(ret));
        }
      }
        break;
      case T_SF_AGGREGATE_SOURCE:
      case T_SF_SOURCE: {
          if (OB_NOT_NULL(parse_tree) && OB_NOT_NULL(parse_tree->children_[4])) {
            OZ (SMART_CALL(resolve(parse_tree->children_[4], func)));
          }
          OZ (SMART_CALL(resolve_root(parse_tree->children_[5], func)));
        }
          break;
      case T_SP_CREATE:
      case T_SF_CREATE: {
        ret = OB_ER_SP_NO_RECURSIVE_CREATE;
        LOG_WARN("Can't create a routine from within another routine", K(ret));
        LOG_USER_ERROR(OB_ER_SP_NO_RECURSIVE_CREATE);
      }
        break;
      case T_SUB_FUNC_DECL: //fall through
      case T_SUB_PROC_DECL: {
        ObPLRoutineInfo *routine_info = NULL;
        if (OB_FAIL(resolve_routine_decl(parse_tree, func, routine_info, false))) {
          LOG_WARN("resolve routine declaration failed", K(parse_tree), K(ret));
        }
      }
        break;
      case T_SUB_FUNC_DEF: //fall through
      case T_SUB_PROC_DEF: {
        // must be nested routine, because udt member is process in package ast resolve
        if (OB_FAIL(resolve_routine_def(parse_tree, func, false))) {
          LOG_WARN("resolve procedure definition failed", K(parse_tree), K(ret));
        }
      }
        break;
      case T_SP_PRAGMA_INLINE: {
        // do nothing ...
      }
        break;
      case T_SP_PRAGMA_UDF: {
        if (OB_FAIL(resolve_udf_pragma(parse_tree, func))) {
          LOG_WARN("resolve udf pragma failed", K(ret), K(parse_tree), K(func));
        }
      }
        break;
      case T_SP_PRAGMA_INTERFACE: {
        RESOLVE_STMT(PL_INTERFACE, resolve_interface, ObPLInterfaceStmt);
      }
        break;
      case T_SP_PRAGMA_RESTRICT_REFERENCE:
      case T_SP_PRAGMA_SERIALLY_REUSABLE: {
        ret = OB_ERR_PARAM_IN_PACKAGE_SPEC;
        LOG_WARN("PLS-00708: Pragma string must be declared in a package specification", K(ret));
      }
        break;
      case T_SP_PRAGMA_AUTONOMOUS_TRANSACTION: {
        if (current_level_ > 1) {
          //oracle兼容：只能在第一个block中申明自治事务
          ret = OB_ERR_PRAGMA_ILLEGAL;
          LOG_USER_ERROR(OB_ERR_PRAGMA_ILLEGAL, "AUTONOMOUS_TRANSACTION");
        } else if (func.is_autonomous()) {
          ret = OB_ERR_PRAGMA_DECL_TWICE;
          LOG_USER_ERROR(OB_ERR_PRAGMA_DECL_TWICE,
                         ObString("AUTONOMOUS_TRANSACTION").length(),
                         ObString("AUTONOMOUS_TRANSACTION").ptr());
        } else {
          func.set_autonomous();
        }
      }
        break;
      case T_SP_GOTO: {
        RESOLVE_STMT(PL_GOTO, resolve_goto, ObPLGotoStmt);
      }
        break;
      case T_SP_PRE_STMTS: {
        const ParseNode *new_node = NULL;
        OZ (ObPLResolver::resolve_condition_compile(
          resolve_ctx_.allocator_,
          &(resolve_ctx_.session_info_),
          &(resolve_ctx_.schema_guard_),
          &(resolve_ctx_.package_guard_),
          &(resolve_ctx_.sql_proxy_),
          NULL,
          parse_tree,
          new_node,
          false,
          false,
          false,
          NULL,
          &(func.get_dependency_table())));
        CK (OB_NOT_NULL(new_node));
        OZ (SMART_CALL(resolve(new_node, func)));
      }
        break;
#define NOT_SUPPORT_IN_ROUTINE \
  ret = OB_NOT_SUPPORTED;       \
  LOG_WARN("Not support parser node", K(get_type_name(parse_tree->type_)), K(ret));
  LOG_USER_ERROR(OB_NOT_SUPPORTED, "parser node");
  LOG_USER_ERROR(OB_NOT_SUPPORTED, get_type_name(parse_tree->type_));

      case T_SF_ALTER:
      case T_SF_DROP: {
        ret = OB_ERR_SP_NO_DROP_SP;
        LOG_WARN("DDL SQL is not allowed in stored function", K(ret));
        LOG_USER_ERROR(OB_ERR_SP_NO_DROP_SP, "FUNCTION");
      }
        break;
      case T_SP_ALTER:
      case T_SP_DROP: {
        ret = OB_ERR_SP_NO_DROP_SP;
        LOG_WARN("DDL SQL is not allowed in stored function", K(ret));
        LOG_USER_ERROR(OB_ERR_SP_NO_DROP_SP, "PROCEDURE");
      }
        break;
      case T_TG_ALTER:
      case T_TG_DROP:
      case T_TG_CREATE: {
        if ((resolve_ctx_.session_info_.is_for_trigger_package() || func.is_function())) {
         ret = OB_ER_COMMIT_NOT_ALLOWED_IN_SF_OR_TRG;
         LOG_WARN("DDL SQL is not allowed in stored function", K(ret));
        } else {
          NOT_SUPPORT_IN_ROUTINE
        }
      }
        break;
      case T_SET_PASSWORD: {
        if ((resolve_ctx_.session_info_.is_for_trigger_package() || func.is_function())) {
          ret = OB_ER_SP_CANT_SET_AUTOCOMMIT;
          LOG_WARN("Not allowed to set autocommit from a stored function or trigger", K(ret));
        } else {
          NOT_SUPPORT_IN_ROUTINE
        }
      }
        break;
      default:
        NOT_SUPPORT_IN_ROUTINE
        break;
      }
#undef NOT_SUPPORT_IN_ROUTINE

      // MySQL兼容: 对于对象不存在的错误，resolve阶段不报错,这里替换为一个single语句，在执行阶段报错
      // 由于对象的创建可能在sp的创建之后，因此这里不将加了single语句的function放入cache
      if ((OB_ERR_FUNCTION_UNKNOWN == ret
           || OB_ERR_SP_WRONG_ARG_NUM == ret
           || OB_ERR_SP_DOES_NOT_EXIST == ret
           || OB_ERR_GET_STACKED_DIAGNOSTICS == ret
           || OB_ERR_RESIGNAL_WITHOUT_ACTIVE_HANDLER == ret)
          && lib::is_mysql_mode()) {
        ObPLSignalStmt *signal_stmt = NULL;
        int save_ret = ret;
        if (OB_FAIL(stmt_factory_.allocate(PL_SIGNAL, current_block_, stmt))) {
          LOG_WARN("failed to alloc stmt", K(ret));
        } else if (OB_ISNULL(signal_stmt = static_cast<ObPLSignalStmt*>(stmt))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("static cast failed", K(ret));
        } else {
          if (-1 == ob_mysql_errno(save_ret)) {
            signal_stmt->set_cond_type(SQL_STATE);
          } else {
            signal_stmt->set_cond_type(ERROR_CODE);
          }
          signal_stmt->set_error_code(ob_errpkt_errno(save_ret, lib::is_oracle_mode()));
          signal_stmt->set_ob_error_code(save_ret);
          signal_stmt->set_sql_state(ob_sqlstate(save_ret));
          signal_stmt->set_str_len(STRLEN(ob_sqlstate(save_ret)));
          func.set_can_cached(false);
          func.set_is_all_sql_stmt(false);
          if (lib::is_mysql_mode() && !func.is_reads_sql_data() && !func.is_modifies_sql_data()) {
            func.set_contains_sql();
          }
        }
      }

      if (OB_SUCC(ret) && handler_analyzer_.in_continue()  && NULL != stmt && !NO_EXCEPTION_STMT(stmt->get_type())) {
        // for continue handler, should add handler to every stmt, here, make a new block, consturct new block with handler and stmt.
        ObPLStmtBlock *block = NULL;
        if (OB_FAIL(make_block(func, current_block_, block))) {
          LOG_WARN("failed to make block", K(current_block_), K(ret));
        } else {
          ObPLStmt *stmt = NULL;
          ObPLDeclareHandlerStmt *declare_handler_stmt = NULL;
          if (OB_FAIL(stmt_factory_.allocate(PL_HANDLER, block, stmt))) {
            LOG_WARN("failed to alloc stmt", K(ret));
          } else if (OB_ISNULL(declare_handler_stmt = static_cast<ObPLDeclareHandlerStmt*>(stmt))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to allocate stmt", K(current_block_), K(ret));
          } else if (OB_FAIL(block->add_stmt(declare_handler_stmt))) {
            LOG_WARN("failed to add stmt", K(declare_handler_stmt), K(ret));
          } else {
            block->set_eh(declare_handler_stmt);
            ObPLDeclareHandlerStmt::DeclareHandler handler_info;
            for (int64_t i = handler_analyzer_.get_continue(); OB_SUCC(ret) && i < handler_analyzer_.get_stack_depth(); ++i) {
              if (OB_FAIL(handler_analyzer_.get_handler(i, handler_info))) {
                LOG_WARN("failed to get handler", K(i), K(ret));
              } else if (OB_FAIL(declare_handler_stmt->add_handler(handler_info))) {
                LOG_WARN("failed to add handler", K(handler_info), K(ret));
              } else { /*do nothing*/ }
            }
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(block->add_stmt(stmt))) {
            LOG_WARN("failed to add stmt", K(stmt), K(ret));
          } else {
            stmt->set_block(block);
            stmt = block;
          }
        }
      }

      if (OB_SUCC(ret) && NULL != stmt) {
        ObPLStmtBlock *pl_stmt_block = static_cast<ObPLStmtBlock *>(stmt);
        if (PL_BLOCK == stmt->get_type() && pl_stmt_block != NULL && pl_stmt_block->get_is_autonomous()) {
          for (int64_t i = 0; OB_SUCC(ret) && i < pl_stmt_block->get_stmts().count(); ++i) {
            ObPLStmt *sub_stmt = pl_stmt_block->get_stmts().at(i);
            if (PL_BLOCK == sub_stmt->get_type()) {
              static_cast<ObPLStmtBlock *>(sub_stmt)->clear_aotonomous();
            }
          }
        }
        stmt->set_level(current_level_);
        stmt->set_location(parse_tree->stmt_loc_.first_line_, parse_tree->stmt_loc_.first_column_);
        int64_t lbls_cnt = labels.count();
        // 这儿只能用label idx，而不是label_table的count(), 是因为这个函数resolve过程中会递归
        // 所以可能子过程里面会push很多labels进去，递归退出到这儿的时候，label_idx和count()就对不上了。
        if (OB_NOT_NULL(pl_label) && OB_INVALID_INDEX != label_idx) {
          for (int64_t i = 0; OB_SUCC(ret) && i < lbls_cnt; ++i) {
            CK (0 <= (label_idx - i));
            OX (stmt->set_label_idx(label_idx - i));
            OX (pl_label->set_end_flag(label_idx - i, true));
          }
        }
      }
      if (OB_FAIL(ret)) {
        if (NULL != stmt) {
          stmt->~ObPLStmt();
        }
      } else if (NULL != current_block_ && NULL != stmt) {
        if (OB_FAIL(current_block_->add_stmt(stmt))) {
          LOG_WARN("failed to add stmt", K(stmt), K(ret));
          if (NULL != stmt) {
            stmt->~ObPLStmt();
          }
        }
      }
    }
    if (OB_FAIL(ret)) {
      record_error_line(parse_tree, resolve_ctx_.session_info_);
    }
  }
  return ret;
}

int ObPLResolver::resolve_root(const ObStmtNodeTree *parse_tree, ObPLFunctionAST &func)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(resolve(parse_tree, func))) {
    LOG_WARN("failed to resolve pl.", K(ret));
  } else if (OB_FAIL(resolve_goto_stmts(func))) {
    LOG_WARN("failed to resolve goto stmts", K(ret));
  } else if (OB_FAIL(check_subprogram(func))) {
    LOG_WARN("faield to check subprogram", K(ret));
  }
  return ret;
}

int ObPLResolver::check_subprogram(ObPLFunctionAST &func)
{
  int ret = OB_SUCCESS;
  ObPLRoutineTable &routine_table = func.get_routine_table();
  common::ObIArray<ObPLRoutineInfo *> &routine_infos = routine_table.get_routine_infos();
  for (int64_t i = 0; OB_SUCC(ret) && i < routine_infos.count(); ++i) {
    ObPLFunctionAST *routine_ast = NULL;
    CK (OB_NOT_NULL(routine_infos.at(i)));
    OZ (routine_table.get_routine_ast(i, routine_ast));
    if (OB_SUCC(ret) && OB_ISNULL(routine_ast)) {
      ret = OB_ERR_ROUTINE_NOT_DEFINE;
      LOG_USER_ERROR(OB_ERR_ROUTINE_NOT_DEFINE,
                     routine_infos.at(i)->get_name().length(),
                     routine_infos.at(i)->get_name().ptr());
      LOG_WARN("A subprogram body must be defined for the forward declaration of string.",
               K(ret), K(i));
    }
  }
  return ret;
}

int ObPLResolver::resolve_goto_stmts(ObPLFunctionAST &func_ast)
{
  UNUSED(func_ast);
  int ret = OB_SUCCESS;
  int64_t cnt = get_goto_stmts().count();
  ObPLStmt *goto_stmt = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < cnt; ++i) {
      goto_stmt = get_goto_stmts().at(i);
      if (OB_ISNULL(goto_stmt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("goto stmt is null", K(ret));
      } else {
        switch (goto_stmt->get_type()) {
          case PL_GOTO: {
            if (OB_FAIL(check_goto(static_cast<ObPLGotoStmt *>(goto_stmt)))) {
              LOG_WARN("failed to resolve goto stmt", K(ret));
            }
          }
          break;
          default: {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("cann't resolve other stmt again expect goto stmt", K(goto_stmt->get_type()));
          }
          break;
        }
      }
    }
    while (OB_SUCC(ret) && 0 < get_goto_stmts().count()) {
      OX (pop_goto_stmts());
    }
  return ret;
}

int ObPLResolver::init(ObPLPackageAST &package_ast)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(make_block(package_ast, current_block_))) {
    LOG_WARN("failed to make block", K(current_block_), K(ret));
  } else {
    current_level_ = 0;
    package_ast.set_body(current_block_);
    external_ns_.set_dependency_table(&package_ast.get_dependency_table());
    current_block_->get_namespace().set_explicit_block();

    /*
     * NOTE: only set label for package spec.
     *   for self pacakge label variable(such as pkg.var), alreays search start at package spec.
     *   public and private variables are all different with names.
     */
    if (ObPLBlockNS::BLOCK_PACKAGE_SPEC == current_block_->get_namespace().get_block_type()) {
      OZ (current_block_->get_namespace().add_label(
        package_ast.get_name(), ObPLLabelTable::ObPLLabelType::LABEL_BLOCK, NULL));
    }
    if (!resolve_ctx_.package_guard_.is_inited()) {
      OZ (resolve_ctx_.package_guard_.init());
    }
  }
  return ret;
}

#define RESOLVE_PACKAGE_STMT(node, compile_unit) \
  do { \
    if (OB_NOT_NULL(node)) { \
      int save_ret = OB_SUCCESS; \
      for (int64_t i = 0; OB_SUCC(ret) && i < node->num_child_; ++i) { \
        if (NULL != node->children_[i]) { \
          if (OB_FAIL(SMART_CALL(resolve(node->children_[i], compile_unit)))) { \
            save_ret = ret; \
            ret = is_object_not_exist_error(ret) ? OB_SUCCESS : ret; \
            LOG_WARN("failed to resolve package stmts", K(ret), K(i)); \
          } \
        } \
      } \
      ret = OB_SUCCESS == ret ? save_ret : ret; \
    } \
  } while (0)


int ObPLResolver::resolve(const ObStmtNodeTree *parse_tree, ObPLPackageAST &package_ast)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(parse_tree)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("parse_tree is NULL or invalid", K(parse_tree), K(ret));
  } else {
    switch (parse_tree->type_) {
    case T_PACKAGE_BLOCK: {
      if (PACKAGE_BLOCK_NUM_CHILD != parse_tree->num_child_) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("package block parse tree invalid", K(ret));
      } else if (OB_NOT_NULL(parse_tree->children_[2])) {
        OZ (SMART_CALL(resolve(parse_tree->children_[2], package_ast)));
      }
    }
      break;
    case T_PACKAGE_STMTS: {
      RESOLVE_PACKAGE_STMT(parse_tree, package_ast);
    }
      break;
    case T_PACKAGE_BODY_BLOCK: {
      if (PACKAGE_BODY_BLOCK_NUM_CHILD != parse_tree->num_child_) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("package body block parse_tree invalid", K(ret));
      } else {
        if (OB_NOT_NULL(parse_tree->children_[1])) {
          OZ (SMART_CALL(resolve(parse_tree->children_[1], package_ast)));
        }
        if (OB_SUCC(ret) && OB_NOT_NULL(parse_tree->children_[2])) {
          OZ (SMART_CALL(resolve(parse_tree->children_[2], package_ast)));
        }
      }
    }
      break;
    case T_SP_CREATE_TYPE_BODY_SRC: {
      if (2 != parse_tree->num_child_) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("type body block invalid", K(ret));
      } else {
        if (OB_NOT_NULL(parse_tree->children_[1])) {
          OZ (SMART_CALL(resolve(parse_tree->children_[1], package_ast)));
        }
      }
    }
      break;
    case T_SP_OBJECT_BODY_DEF: {
      for (int64_t i = 0; OB_SUCC(ret) && i < parse_tree->num_child_; ++i) {
        OZ (SMART_CALL(resolve(parse_tree->children_[i], package_ast)));
      }
    }
      break;
    case T_PACKAGE_BODY_STMTS: {
      RESOLVE_CHILDREN(parse_tree, package_ast);
    }
      break;
    case T_SP_DECL: {
      OZ (resolve_declare_var(parse_tree, package_ast));
      break;
    }
#ifdef OB_BUILD_ORACLE_PL
    case T_SP_DECL_USER_TYPE: {
      OZ (resolve_declare_user_type(parse_tree, package_ast));
      break;
    }
    case T_SP_DECL_USER_SUBTYPE: {
      OZ (resolve_declare_user_subtype(parse_tree, NULL, package_ast));
      break;
    }
#endif
    case T_SP_INIT_PRAGMA:
    case T_SP_DECL_COND: {
      OZ (resolve_declare_cond(parse_tree, package_ast));
      break;
    }
    case T_SUB_FUNC_DECL: //fall through
    case T_SUB_PROC_DECL: {
      ObPLRoutineInfo *routine_info = NULL;
      if (OB_FAIL(resolve_routine_decl(parse_tree, package_ast, routine_info))) {
        LOG_WARN("resolve routine declaration failed", K(parse_tree), K(ret));
      }
    }
      break;
    case T_SUB_FUNC_DEF: //fall through
    case T_SUB_PROC_DEF: {
      bool is_udt_routine = (ObPackageType::PL_UDT_OBJECT_SPEC == package_ast.get_package_type()
                         || ObPackageType::PL_UDT_OBJECT_BODY == package_ast.get_package_type()) ?
                         true : false;
      if (OB_FAIL(resolve_routine_def(parse_tree, package_ast, is_udt_routine))) {
        LOG_WARN("resolve procedure definition failed", K(parse_tree), K(ret));
      }
    }
      break;
    case T_STMT_LIST: {
      RESOLVE_CHILDREN(parse_tree, package_ast);
    }
      break;
    case T_SP_PROC_STMT_LIST: {
      if (OB_FAIL(resolve_init_routine(parse_tree, package_ast))) {
        LOG_WARN("resolve init routine failed", K(parse_tree), K(ret));
      }
    }
      break;
    case T_SP_PRAGMA_INLINE: {
      // do nothing ...
    }
      break;
    case T_SP_PRAGMA_SERIALLY_REUSABLE: {
      if (OB_FAIL(resolve_serially_reusable_pragma(parse_tree, package_ast))) {
        LOG_WARN("resolve serially pragma failed", K(ret), K(parse_tree), K(package_ast));
      }
    }
      break;
    case T_SP_PRAGMA_RESTRICT_REFERENCE: {
      if (OB_FAIL(resolve_restrict_references_pragma(parse_tree, package_ast))) {
        LOG_WARN("resolve restrict references failed", K(ret), K(parse_tree), K(package_ast));
      }
    }
      break;
    case T_SP_PRAGMA_INTERFACE: {
      if (OB_FAIL(resolve_interface_pragma(parse_tree, package_ast))) {
        LOG_WARN("resolve restrict references failed", K(ret), K(parse_tree), K(package_ast));
      }
    }
      break;
    case T_SP_PRAGMA_UDF: {
      ret = OB_ERR_PRAGMA_ILLEGAL;
      LOG_USER_ERROR(OB_ERR_PRAGMA_ILLEGAL, "UDF");
    }
      break;
    case T_SP_DECL_CURSOR: {
      if (OB_FAIL(resolve_declare_cursor(parse_tree, package_ast))) {
        LOG_WARN("resovle package cursor declare failed", K(ret), K(parse_tree), K(package_ast));
      } else {
        if (!package_ast.is_modifies_sql_data()) {
          package_ast.set_reads_sql_data();
        }
      }
    }
      break;
#ifdef OB_BUILD_ORACLE_PL
    case T_SP_OBJ_ELEMENT_SPEC_LIST: {
      OZ (resolve_object_elem_spec_list(parse_tree, package_ast));
    }
      break;
    case T_SP_OBJ_ELEMENT_SPEC:
    case T_SP_OBJ_ELEM_CONSTRUCTOR : {
      OZ (resolve_object_elem_spec_def(parse_tree, package_ast));
    }
      break;
    case T_SP_CREATE_TYPE_SRC : {
      OZ (resolve_object_def(parse_tree, package_ast));
    }
      break;
#endif
    case T_SP_OBJ_CONSTR_IMPL : {
      CK (OB_NOT_NULL(parse_tree));
      OX (const_cast<ParseNode *>(parse_tree)->type_ = T_SUB_FUNC_DEF);
      CK (OB_NOT_NULL(parse_tree->children_[0]));
      OX (parse_tree->children_[0]->type_ = T_SUB_FUNC_DECL);
      OZ (resolve_routine_def(parse_tree, package_ast, true));
    }
      break;
    case T_SP_CONTRUCTOR_DEF_IN_TYPE : {
      CK (OB_NOT_NULL(parse_tree));
      CK (2 == parse_tree->num_child_);
      OZ (SMART_CALL(resolve(parse_tree->children_[1], package_ast)));
    }
      break;
    case T_SP_OBJ_MAP_ORDER: {
      CK (OB_NOT_NULL(parse_tree));
      CK (1 == parse_tree->num_child_);
      CK (2 == parse_tree->int16_values_[0]); // map define, not decl
      CK (OB_NOT_NULL(parse_tree->children_[0]));
      OZ (resolve_routine_def(parse_tree->children_[0], package_ast, true));
    }
      break;
#ifdef OB_BUILD_ORACLE_PL
    case T_SP_PRE_STMTS: {
      const ParseNode *new_node = NULL;
      OZ (ObPLResolver::resolve_condition_compile(
          resolve_ctx_.allocator_,
          &(resolve_ctx_.session_info_),
          &(resolve_ctx_.schema_guard_),
          &(resolve_ctx_.package_guard_),
          &(resolve_ctx_.sql_proxy_),
          NULL,
          parse_tree,
          new_node,
          false,
          false,
          false,
          NULL,
          &(package_ast.get_dependency_table())));
      CK (OB_NOT_NULL(new_node));
      OZ (ObPLUDTObjectManager::make_self_node(
        resolve_ctx_.allocator_, const_cast<ParseNode *>(new_node), package_ast.get_name()));
      OZ (SMART_CALL(resolve(new_node, package_ast)));
    }
      break;
#endif
    default: {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("Not support parser node", K(get_type_name(parse_tree->type_)), K(ret));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, get_type_name(parse_tree->type_));
      break;
    }
    }
    if (OB_FAIL(ret)) {
      record_error_line(parse_tree, resolve_ctx_.session_info_);
    }
  }
  return ret;
}

#ifdef OB_BUILD_ORACLE_PL
int ObPLResolver::resolve_object_def(const ParseNode *parse_tree, ObPLCompileUnitAST &package_ast)
{
  int ret = OB_SUCCESS;
  CK (OB_LIKELY(T_SP_CREATE_TYPE_SRC == parse_tree->type_));
  const ParseNode *spec_body_node = parse_tree;
  CK (OB_NOT_NULL(spec_body_node));
  if (OB_SUCC(ret)) {
    // bool replace = static_cast<bool>(parse_tree->int32_values_[0]);
    // bool noneditioable = static_cast<bool>(parse_tree->int32_values_[1]);
    const ParseNode *name_node = spec_body_node->children_[0];
    const ParseNode *oid_node = spec_body_node->children_[1];
    const ParseNode *object_type_node = spec_body_node->children_[2];

    UNUSED(oid_node);
    CK (OB_NOT_NULL(name_node));
    CK (OB_NOT_NULL(object_type_node));
    if (OB_SUCC(ret)) {
      ObString db_name, udt_name;
      if (OB_FAIL(ObResolverUtils::resolve_sp_name(get_resolve_ctx().session_info_,
                                                  *name_node,
                                                  db_name,
                                                  udt_name))) {
        LOG_WARN("failed to resolve type name", K(ret));
      } else {
        CK (T_SP_OBJECT_DEF == object_type_node->type_
          || T_SP_OPAQUE_DEF == object_type_node->type_);
        CK (OB_LIKELY(5 == object_type_node->num_child_));
        CK (OB_NOT_NULL(object_type_node->children_[3]));
        CK (OB_NOT_NULL(object_type_node->children_[1]));
        if (OB_SUCC(ret)) {
          // const ParseNode* invoke_accessby_node = object_type_node->children_[0];
          const ParseNode* object_or_under = object_type_node->children_[1];
          // const ParseNode* sqlj_node = object_type_node->children_[2];
          const ParseNode* attrs_node = object_type_node->children_[3];
          // const ParseNode* final_node = object_type_node->children_[4];
          CK (OB_NOT_NULL(object_or_under));
          CK (OB_NOT_NULL(attrs_node));
          CK (OB_LIKELY(T_SP_OBJ_ATTR_AND_ELEMENT_SPEC == attrs_node->type_));
          const ParseNode *elem_spec_list = attrs_node->children_[1];
          if (OB_SUCC(ret)) {
            if (T_SP_IS_UNDER_OBJECT == object_or_under->type_) {
              ret = OB_NOT_SUPPORTED;
              LOG_WARN("not support yet", K(ret));
              LOG_USER_ERROR(OB_NOT_SUPPORTED, "under object");
            } else if (OB_NOT_NULL(elem_spec_list)) {
              if (OB_FAIL(resolve_object_elem_spec_list(elem_spec_list, package_ast))){
                LOG_WARN("failed to resolve elem spec list", K(ret));
              }
            }
          }
        }
      }
    }
  }

  return ret;
}

int ObPLResolver::resolve_object_elem_spec_list(const ParseNode *parse_tree,
                                                ObPLCompileUnitAST &package_ast)
{
  int ret = OB_SUCCESS;
  const ParseNode* element_spec_list = parse_tree;
  if (OB_ISNULL(element_spec_list)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < element_spec_list->num_child_; ++i) {
      const ParseNode *element_spec = element_spec_list->children_[i];
      if (OB_FAIL(resolve_object_elem_spec_def(element_spec, package_ast))) {
        LOG_WARN("failed to resolve type spec", K(ret));
      }
    }
  }
  return ret;
}

int ObPLResolver::resolve_object_elem_spec_def(const ParseNode *parse_tree,
                                               ObPLCompileUnitAST &package_ast)
{
  int ret = OB_SUCCESS;

  CK (OB_NOT_NULL(parse_tree));
  if (OB_SUCC(ret)) {
    if (T_SP_OBJ_ELEMENT_SPEC == parse_tree->type_) {
      CK (3 == parse_tree->num_child_);
      // const ParseNode *inherit_node = parse_tree->children_[0];
      const ParseNode *subprog_node = parse_tree->children_[1];
      // const ParseNode *restrict_ref_node = parse_tree->children_[2];
      if (OB_NOT_NULL(subprog_node)) {
        if (T_SP_OBJ_ELEM_SUBPROG_SPEC == subprog_node->type_) {
          const ParseNode *routine_node = subprog_node->children_[0];
          CK (OB_NOT_NULL(routine_node));
          if (OB_SUCC(ret)) {
            int64_t member_or_static = static_cast<int64_t>(subprog_node->int16_values_[0]);
            ObPLRoutineInfo *routine_info = NULL;

            if (OB_FAIL(resolve_routine_decl(routine_node, package_ast, routine_info, true))) {
              LOG_WARN("failed to resolve object function", K(ret));
            } else if (OB_ISNULL(routine_info)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("udt routine info is null", K(ret));
            } else {
              if (UdtUdfType::UDT_UDF_STATIC == member_or_static) {
                routine_info->get_compile_flag().add_static();
              }
              routine_info->set_is_udt_routine();
              // check static and self
              if (routine_info->is_udt_static_routine() && routine_info->has_self_param()) {
                ret = OB_ERR_STATIC_METHOD_HAS_SELF;
                LOG_WARN("static member routine should not have self param");
              }
            }
          }
        } else if (T_SP_OBJ_ELEM_CONSTRUCTOR == subprog_node->type_) {
          ObPLRoutineInfo *routine_info = NULL;
          // 0: name, 1 : param, 2 : attr
          OZ (resolve_object_constructor(subprog_node, package_ast, routine_info));
          CK (OB_NOT_NULL(routine_info));
          if (OB_SUCC(ret)) {
            routine_info->set_is_udt_routine();
            routine_info->set_is_udt_cons();
          }
        } else if (T_SP_OBJ_MAP_ORDER == subprog_node->type_) {
          ObPLRoutineInfo *routine_info = NULL;
          const ParseNode *routine_node = subprog_node->children_[0];
          CK (OB_NOT_NULL(routine_node));
          OZ (resolve_routine_decl(routine_node, package_ast, routine_info, true));
          CK (OB_NOT_NULL(routine_info));
          OX (routine_info->set_is_udt_routine());
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unknown type subprogram type", K(ret));
        }
      }
    } else if (T_SP_OBJ_ELEM_CONSTRUCTOR == parse_tree->type_) {
      ObPLRoutineInfo *routine_info = NULL;
      OZ (resolve_object_constructor(parse_tree, package_ast, routine_info));
      OV (OB_NOT_NULL(routine_info), OB_ERR_UNEXPECTED);
      if (OB_SUCC(ret)) {
        routine_info->set_is_udt_routine();
        routine_info->set_is_udt_cons();
      }
    }
    // inherit and restrict ref not support yet
  }

  return ret;
}

int ObPLResolver::resolve_object_constructor(const ParseNode *parse_tree,
                                             ObPLCompileUnitAST &package_ast,
                                             ObPLRoutineInfo *&routine_info)
{
  int ret = OB_SUCCESS;
  ParseNode *subp = const_cast<ParseNode *>(parse_tree);
  CK (OB_NOT_NULL(parse_tree));
  CK (5 == parse_tree->num_child_);
  CK (T_SP_OBJ_ELEM_CONSTRUCTOR == parse_tree->type_);
  OX (subp->type_ = T_SUB_FUNC_DECL);
  OZ (resolve_routine_decl(subp, package_ast, routine_info, true));
  return ret;
}
#endif

int ObPLResolver::check_declare_order(ObPLStmtType type)
{
  int ret = OB_SUCCESS;
  CK(PL_VAR == type || PL_COND == type || PL_HANDLER == type || PL_CURSOR == type);
  if (is_oracle_mode()) {
    // oracle compatible, do nothing ...
  } else {
    ObPLStmtType pre_type = INVALID_PL_STMT;
    if (OB_NOT_NULL(current_block_) && (current_block_->get_stmts().count() != 0)) {
      pre_type =
        current_block_->get_stmts().at(current_block_->get_stmts().count() - 1)->get_type();
    }
    if (handler_analyzer_.get_stack_depth() > 0) {
      ObPLDeclareHandlerStmt::DeclareHandler info;
      if (OB_FAIL(handler_analyzer_.get_handler(handler_analyzer_.get_stack_depth() - 1, info))) {
        LOG_WARN("failed to get last handler", K(ret));
      } else if (info.get_level() == current_level_) {
        pre_type = PL_HANDLER;
      }
    }
    if (OB_FAIL(ret)) {
    } else if ((PL_VAR == type || PL_COND == type)
        && (PL_HANDLER == pre_type || PL_CURSOR == pre_type)) {
      ret = OB_ER_SP_VARCOND_AFTER_CURSHNDLR;
      LOG_USER_ERROR(OB_ER_SP_VARCOND_AFTER_CURSHNDLR);
      LOG_WARN("Variable or condition declaration after cursor or handler declaration", K(ret));
    } else if (PL_CURSOR == type && PL_HANDLER == pre_type) {
      ret = OB_ER_SP_CURSOR_AFTER_HANDLER;
      LOG_USER_ERROR(OB_ER_SP_CURSOR_AFTER_HANDLER);
      LOG_WARN("Cursor declaration after handler declaration", K(ret));
    }
  }
  return ret;
}

#ifdef OB_BUILD_ORACLE_PL
int ObPLResolver::resolve_declare_user_type(const ObStmtNodeTree *parse_tree, ObPLDeclareUserTypeStmt *stmt, ObPLFunctionAST &func)
{
  int ret = OB_SUCCESS;
  ObPLCompileUnitAST &unit_ast = static_cast<ObPLCompileUnitAST&>(func);
  ret = resolve_declare_user_type_comm(parse_tree, stmt, unit_ast);
  return ret;
}

int ObPLResolver::resolve_declare_user_type(const ObStmtNodeTree *parse_tree, ObPLPackageAST &package_ast)
{
  int ret = OB_SUCCESS;
  ObPLCompileUnitAST &unit_ast = static_cast<ObPLCompileUnitAST&>(package_ast);
  ret = resolve_declare_user_type_comm(parse_tree, NULL, unit_ast);
  return ret;
}
#endif

int ObPLResolver::get_number_literal_value(ObRawExpr *expr, int64_t &result)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(expr));

#define GET_CONST_EXPR_VAL(expr, result) \
  const ObConstRawExpr *const_expr = static_cast<const ObConstRawExpr*>(expr); \
  CK (OB_NOT_NULL(const_expr)); \
  CK (const_expr->get_value().is_integer_type() || const_expr->get_value().is_number()); \
  if (OB_FAIL(ret)) { \
  } else if (const_expr->get_value().is_integer_type()) { \
    result = const_expr->get_value().get_int(); \
  } else if (const_expr->get_value().is_number()) { \
    OZ (const_expr->get_value().get_number().extract_valid_int64_with_trunc(result)); \
  }

  if (OB_FAIL(ret)) {
  } else if ((T_INT != expr->get_expr_type()
       && T_OP_NEG != expr->get_expr_type())
      || (T_OP_NEG == expr->get_expr_type()
          && T_INT != expr->get_param_expr(0)->get_expr_type())) {
    ret = OB_ERR_NUMERIC_LITERAL_REQUIRED;
    LOG_WARN("PLS-00491: numeric literal required", K(ret), KPC(expr));
  } else if (T_INT == expr->get_expr_type()) {
    GET_CONST_EXPR_VAL(expr, result);
  } else if (T_OP_NEG == expr->get_expr_type()) {
    GET_CONST_EXPR_VAL(expr->get_param_expr(0), result);
    OX (result = (INT64_MIN == result) ? static_cast<uint64_t>(INT64_MAX + 1UL) : -result);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected expr type", K(ret), K(expr->get_expr_type()));
  }

  return ret;
}

int ObPLResolver::get_const_number_variable_literal_value(ObRawExpr *expr, int64_t &result)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(expr));
  CK (T_QUESTIONMARK == expr->get_expr_type());
  CK (OB_NOT_NULL(current_block_));
  CK (OB_NOT_NULL(current_block_->get_symbol_table()));

  if (OB_SUCC(ret)) {
    int64_t idx = static_cast<ObConstRawExpr*>(expr)->get_value().get_unknown();
    const ObPLVar *var = current_block_->get_symbol_table()->get_symbol(idx);

    const ObRawExpr *value_expr = nullptr;

    if (OB_ISNULL(var)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL symbol variable", K(ret), KPC(expr),
               K(current_block_->get_symbol_table()), K(idx));
    } else if (!var->is_readonly()
               || OB_ISNULL(value_expr = current_block_->get_expr(var->get_default()))
               || (T_NUMBER != value_expr->get_expr_type() && T_INT != value_expr->get_expr_type())) {
      ret = OB_ERR_NUMERIC_LITERAL_REQUIRED;
      LOG_WARN("PLS-00491: numeric literal required", K(ret), K(var), K(value_expr));
    } else {
      GET_CONST_EXPR_VAL(value_expr, result);
    }
  }

#undef GET_CONST_EXPR_VAL
  return ret;
}

int ObPLResolver::calc_subtype_range_bound(const ObStmtNodeTree *bound_node,
                                           ObPLCompileUnitAST &unit_ast,
                                           int32_t &bound)
{
  int ret = OB_SUCCESS;
  ObRawExpr *bound_expr = NULL;
  int64_t result = -1;
  CK (OB_NOT_NULL(bound_node));
  OZ (resolve_expr(bound_node, unit_ast, bound_expr, combine_line_and_col(bound_node->stmt_loc_)));
  CK (OB_NOT_NULL(bound_expr));
  if (T_QUESTIONMARK == bound_expr->get_expr_type()){
    OZ (get_const_number_variable_literal_value(bound_expr, result));
  } else if (T_OP_NEG == bound_expr->get_expr_type()
             && OB_NOT_NULL(bound_expr->get_param_expr(0))
             && T_QUESTIONMARK == bound_expr->get_param_expr(0)->get_expr_type()) {
    OZ (get_const_number_variable_literal_value(bound_expr->get_param_expr(0), result));
    OX (result = (INT64_MIN == result) ? static_cast<uint64_t>(INT64_MAX + 1UL) : -result);
  } else {
    OZ (get_number_literal_value(bound_expr, result));
  }

  if (OB_SUCC(ret)) {
    if (-2147483648 <= result && result <= 2147483647) {
      bound = result;
    } else {
      ret = OB_ERR_NON_INT_LITERAL;
      LOG_WARN("PLS-00325: non-integral numeric literal string is inappropriate in this context",
               K(ret), K(result));
    }
  }
  return ret;
}

#ifdef OB_BUILD_ORACLE_PL
int ObPLResolver::resolve_subtype_precision(const ObStmtNodeTree *precision_node,
                                            ObPLDataType &base_type)
{
  int ret = OB_SUCCESS;
  int16_t precision = -1;
  int16_t scale = 0;
  CK (OB_NOT_NULL(precision_node));
  CK (T_LINK_NODE == precision_node->type_);
  CK (1 == precision_node->param_num_ || 2 == precision_node->param_num_);
  OX (precision = precision_node->int16_values_[0]);
  OX (2 == precision_node->param_num_ ? scale = precision_node->int16_values_[1] : int16_t(0));
  CK (OB_NOT_NULL(base_type.get_data_type()));
  if (OB_SUCC(ret)) {
    ObObjTypeClass tclass = base_type.get_data_type()->get_type_class();
    if (2 == precision_node->param_num_) {
      if (ObNumberTC != tclass) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not supported yet", K(ret), K(tclass));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "precision of subtype is non-number type");
      }
    } else {
      if (ObFloatTC != tclass && ObDoubleTC != tclass && ObStringTC != tclass
          && ObNumberTC != tclass && ObOTimestampTC != tclass) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not supported yet", K(ret), K(tclass));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "the base type of subtype is not float/double/string/number/timestamp class");
      }
    }
    if (OB_SUCC(ret)) {
      if (ObOTimestampTC == tclass) {
        if (precision > OB_MAX_TIMESTAMP_TZ_PRECISION) {
          ret = OB_ERR_DATETIME_INTERVAL_PRECISION_OUT_OF_RANGE;
        } else {
          base_type.get_data_type()->set_precision(precision);
          base_type.get_data_type()->set_scale(scale);
        }
      } else if (ObNumberTC == tclass) {
        if (base_type.get_data_type()->get_meta_type().is_number_float()) {
          if ((OB_UNLIKELY(precision < OB_MIN_NUMBER_FLOAT_PRECISION)
                  || OB_UNLIKELY(precision > OB_MAX_NUMBER_FLOAT_PRECISION))) {
            ret = OB_FLOAT_PRECISION_OUT_RANGE;
            LOG_WARN("precision of float out of range", K(ret), K(precision));
          }
        } else if ((OB_UNLIKELY(precision < OB_MIN_NUMBER_PRECISION)
                    || OB_UNLIKELY(precision > OB_MAX_NUMBER_PRECISION))) {
          ret = OB_NUMERIC_PRECISION_OUT_RANGE;
          LOG_WARN("precision of number overflow", K(ret), K(scale), K(precision));
        } else if (OB_UNLIKELY(scale < OB_MIN_NUMBER_SCALE)
                   || OB_UNLIKELY(scale > OB_MAX_NUMBER_SCALE)) {
          ret = OB_NUMERIC_SCALE_OUT_RANGE;
          LOG_WARN("scale of number out of range", K(ret), K(scale));
        } else {
          base_type.get_data_type()->set_precision(precision);
          base_type.get_data_type()->set_scale(scale);
        }
      } else if (ObFloatTC == tclass || ObDoubleTC == tclass) {
        base_type.get_data_type()->set_precision(precision);
        base_type.get_data_type()->set_scale(scale);
      } else if (ObStringTC == tclass) {
        if (0 == precision) {
          ret = OB_ERR_ZERO_LEN_COL;
          LOG_WARN("Oracle not allowed zero length", K(ret));
          // @FIXME comparison is always false due to limited range of data type
_Pragma("GCC diagnostic push")
_Pragma("GCC diagnostic ignored \"-Wtype-limits\"")
        } else if (OB_MAX_ORACLE_VARCHAR_LENGTH < precision || precision < 0) {
_Pragma("GCC diagnostic pop")
          ret = OB_ERR_TOO_LONG_COLUMN_LENGTH;
          LOG_WARN("column data length is invalid", K(ret), K(precision), K(base_type));
        } else {
          base_type.get_data_type()->set_length(precision);
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("not supported yet", K(ret), K(base_type));
        LOG_USER_ERROR(OB_ERR_UNEXPECTED, "type class");
      }
    }
  }
  return ret;
}

int ObPLResolver::resolve_declare_user_subtype(const ObStmtNodeTree *parse_tree,
                                               ObPLDeclareUserTypeStmt *stmt,
                                               ObPLCompileUnitAST &unit_ast)
{
  int ret = OB_SUCCESS;
  ObString subtype_name;
  bool not_null = false;
  bool has_range = false;
  int32_t lower = -1;
  int32_t upper = -1;
  ObPLDataType base_type;
  const ObStmtNodeTree *name_node = NULL;
  const ObStmtNodeTree *type_node = NULL;
  const ObStmtNodeTree *constraint_node = NULL;
  // 合法性检查
  CK (OB_NOT_NULL(parse_tree));
  CK (OB_NOT_NULL(current_block_));
  CK (OB_LIKELY(T_SP_DECL_USER_SUBTYPE == parse_tree->type_));
  CK (OB_LIKELY(2 == parse_tree->num_child_));
  CK (OB_NOT_NULL(parse_tree->children_[0])
      && OB_LIKELY(T_IDENT == parse_tree->children_[0]->type_));
  CK (OB_NOT_NULL(name_node = parse_tree->children_[0]));
  CK (OB_NOT_NULL(type_node = parse_tree->children_[1]));
  CK (T_SP_USER_SUBTYPE_BASETYPE == type_node->type_);
  CK (2 == type_node->num_child_);
  OX (constraint_node = type_node->children_[1]);
  CK (OB_NOT_NULL(type_node = type_node->children_[0]));
  // 解析子类型名称
  OX (subtype_name = ObString(name_node->str_len_, name_node->str_value_));
  // 解析子类型的基础类型
  CK (OB_NOT_NULL(type_node));
  OZ (resolve_sp_data_type(type_node, subtype_name, unit_ast, base_type, NULL));
  // 解析子类型的RANGE子句
  if (OB_SUCC(ret) && OB_NOT_NULL(constraint_node)) {
    if (T_SP_USER_SUBTYPE_RANGE == constraint_node->type_) {
      CK (OB_LIKELY(2 == constraint_node->num_child_));
      CK (OB_NOT_NULL(constraint_node->children_[0]));
      CK (OB_NOT_NULL(constraint_node->children_[1]));
      OZ (calc_subtype_range_bound(constraint_node->children_[0], unit_ast, lower));
      OZ (calc_subtype_range_bound(constraint_node->children_[1], unit_ast, upper));
      if (OB_SUCC(ret)) {
        if (!base_type.is_pl_integer_type() || upper < lower) {
          ret = OB_ERR_IMPROPER_CONSTRAINT_FORM;
          LOG_WARN("PLS-00572: improper constraint form used", K(ret), K(base_type));
        }
      }
      OX (has_range = true);
    } else {
      CK (T_LINK_NODE == constraint_node->type_);
      OZ (resolve_subtype_precision(constraint_node, base_type));
    }
  }
  // 解析NOT NULL子句
  OX (not_null = (-1 == parse_tree->value_) ? false : static_cast<bool>(parse_tree->value_));
  // 构造并加入NameSpace
  if (OB_SUCC(ret)) {
    ObUserDefinedSubType *subtype = NULL;
    if (OB_ISNULL(subtype = static_cast<ObUserDefinedSubType *>
                  (resolve_ctx_.allocator_.alloc(sizeof(ObUserDefinedSubType))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory for user defiend sub type", K(ret));
    }
    OX (subtype = new(subtype)ObUserDefinedSubType());
    OX (subtype->set_name(subtype_name));
    CK (!base_type.is_subtype());

    if (OB_SUCC(ret)) {
      if (base_type.is_not_null() && -1 == parse_tree->value_) {
        ret = OB_ERR_SUBTYPE_NOTNULL_MISMATCH;
        LOG_WARN("PLS-00366: subtype of a not null type must also be not null", K(ret));
      } else {
        OX (base_type.set_not_null(not_null || base_type.get_not_null()));
      }
    }
    OX (has_range ? base_type.set_range(lower, upper) : void(NULL));
    OX (subtype->set_base_type(base_type));
    OZ (current_block_->get_namespace().add_type(subtype));
    OX (NULL != stmt ? stmt->set_user_type(subtype) : (void)NULL);
  }
  return ret;
}

int ObPLResolver::resolve_declare_user_type_comm(const ObStmtNodeTree *parse_tree,
                                                 ObPLDeclareUserTypeStmt *stmt,
                                                 ObPLCompileUnitAST &unit_ast)
{
  int ret = OB_SUCCESS;
  const ObStmtNodeTree *type_definition_node = NULL;
  if (OB_ISNULL(parse_tree) || OB_ISNULL(current_block_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(parse_tree), K(stmt));
  } else if (OB_ISNULL(type_definition_node = parse_tree->children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type definition node is null");
  } else {
    switch (type_definition_node->type_) {
    case T_SP_REF_CURSOR_TYPE: {
      if (OB_FAIL(resolve_ref_cursor_type(type_definition_node, stmt, unit_ast))) {
        LOG_WARN("resolve declare ref cursor type failed", K(ret));
      }
    }
      break;
    case T_SP_RECORD_TYPE: {
      //record type
      if (OB_FAIL(resolve_declare_record_type(type_definition_node, stmt, unit_ast))) {
        LOG_WARN("resolve declare record type failed", K(ret));
      }
    }
      break;
    case T_SP_COLLECTION_TYPE: {
      //collection type
      if (OB_FAIL(resolve_declare_collection_type(type_definition_node, stmt, unit_ast))) {
        LOG_WARN("resolve declare collection type failed", K(ret));
      }
    }
      break;
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid type node", K_(type_definition_node->type));
    }
      break;
    }
  }
  return ret;
}

int ObPLResolver::resolve_ref_cursor_type(const ParseNode *node,
                                              ObPLDeclareUserTypeStmt *stmt,
                                              ObPLCompileUnitAST &unit_ast)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node) || OB_ISNULL(current_block_)) { //如果是package，stmt为空，所以这里不检查stmt
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("parse_tree is NULL", K(node), K(stmt), K(current_block_), K(ret));
  } else {
    //解析name
    const ObStmtNodeTree *name_node = node->children_[0];
    const ObStmtNodeTree *type_node = node->children_[1];
    if (OB_ISNULL(name_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("parse_tree is NULL", K(name_node), K(ret));
    } else {
      ObString name;
      ObPLDataType return_type;
      if (OB_FAIL(resolve_cursor_common(name_node, type_node, unit_ast, name, return_type))) {
        LOG_WARN("failed to resolve cursor comm", K(ret), K(name));
      } else if (NULL != type_node && !return_type.is_record_type()) {
        ret = OB_ERR_INVALID_CURSOR_RETURN_TYPE;
        LOG_WARN("ref cursor must return record type", K(return_type), K(ret));
      } else {
        ObRefCursorType *ref_cursor_type = NULL;
        if (OB_ISNULL(ref_cursor_type = static_cast<ObRefCursorType*>(resolve_ctx_.allocator_.alloc(sizeof(ObRefCursorType))))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to alloc memory", K(ret));
        } else {
          ref_cursor_type = new(ref_cursor_type)ObRefCursorType();
          ref_cursor_type->set_name(name);
          if (unit_ast.is_package()) {
            ref_cursor_type->set_type_from(PL_TYPE_PACKAGE);
          }
          NULL == type_node ? (void)NULL : ref_cursor_type->set_return_type_id(return_type.get_user_type_id());
          if (OB_FAIL(current_block_->get_namespace().add_type(ref_cursor_type))) {
            LOG_WARN("failed to add record type to type table", K(*ref_cursor_type), K(ret));
          } else if (!OB_ISNULL(stmt)) {
            stmt->set_user_type(static_cast<ObUserDefinedType *>(ref_cursor_type));
          } else { /*do nothing*/ }
        }
      }
    }
  }
  return ret;
}
#endif

int ObPLResolver::resolve_declare_record_type(const ParseNode *type_node,
                                              ObPLDeclareUserTypeStmt *stmt,
                                              ObPLCompileUnitAST &unit_ast)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(type_node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("type_node is null");
  } else if (OB_UNLIKELY(type_node->num_child_ != 2)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type node children number is invalid", K_(type_node->num_child));
  } else {
    //resolve record name
    const ParseNode *name_node = type_node->children_[0];
    ObString record_name;
    if (OB_ISNULL(name_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("name node is null");
    } else {
      record_name.assign_ptr(name_node->str_value_, static_cast<int32_t>(name_node->str_len_));
    }
    //resolve record member list
    if (OB_SUCC(ret)) {
      ObRecordType *record_type = NULL;
      const ParseNode *record_member_list = NULL;
      void *ptr = NULL;
      if (OB_ISNULL(record_member_list = type_node->children_[1])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("record_member_list is null");
      } else if (OB_UNLIKELY(record_member_list->type_ != T_RECORD_MEMBER_LIST)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("record member list is invalid", K_(record_member_list->type));
      } else if (OB_ISNULL(ptr = resolve_ctx_.allocator_.alloc(sizeof(ObRecordType)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret));
      } else {
        record_type = new(ptr) ObRecordType();
        record_type->set_name(record_name);
        if (unit_ast.is_package()) {
          record_type->set_type_from(PL_TYPE_PACKAGE);
        }
      }

      ObPLStmtBlock *parent = current_block_;
      ObPLBlockNS *parent_namespace = resolve_ctx_.params_.secondary_namespace_;
      ObPLStmtBlock *block = NULL;
      if (unit_ast.is_routine()) {
        OZ (make_block(static_cast<ObPLFunctionAST&>(unit_ast), parent, block, false));
      } else {
        OZ (make_block(static_cast<ObPLPackageAST&>(unit_ast), block));
        OX (block->get_namespace().set_block_type(ObPLBlockNS::BLOCK_ROUTINE));
        OX (block->get_namespace().set_pre_ns(NULL == parent ? NULL : &parent->get_namespace()));
      }
      OX (set_current(*block));
      CK (OB_NOT_NULL(record_type));
      OZ (record_type->record_members_init(&resolve_ctx_.allocator_, record_member_list->num_child_));
      for (int64_t i = 0; OB_SUCC(ret) && i < record_member_list->num_child_; ++i) {
        const ParseNode *member_node = record_member_list->children_[i];
        if (OB_ISNULL(member_node)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("member_node is null");
        } else if (OB_UNLIKELY(member_node->type_ != T_RECORD_MEMBER)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("member node is null");
        } else if (OB_UNLIKELY(3 != member_node->num_child_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("member node children number is invalid", K(member_node->num_child_));
        } else {
          ObString member_name(static_cast<int32_t>(member_node->children_[0]->str_len_), member_node->children_[0]->str_value_);
          ObPLDataType data_type;
          ObRawExpr *default_expr = NULL;
          ParseNode *default_node = member_node->children_[2];
          bool member_not_null = (1 == member_node->int32_values_[0]);
          if (OB_FAIL(resolve_sp_data_type(member_node->children_[1], record_name, unit_ast, data_type))) {
            LOG_WARN("resolve sp data type failed", K(ret));
          } else if (data_type.is_cursor_type()) {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("Cursor type defined in record is not supported", K(ret));
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "attribute type of record is cursor type");
          } else if (OB_FAIL(current_block_->get_namespace().add_symbol(
                          member_name, data_type, NULL, false, false, false, true))) {
            LOG_WARN("failed to add symbol to namespace", K(ret), K(member_name));
          } else {
            // subtype may have not null declared, check it first, etc. subtype t1 is int not null;
            if (OB_SUCC(ret)) {
              if (data_type.is_not_null() && -1 == member_node->int32_values_[0]) {
                ret = OB_ERR_SUBTYPE_NOTNULL_MISMATCH;
                LOG_WARN("PLS-00366: subtype of a not null type must also be not null", K(ret));
              } else {
                member_not_null = data_type.get_not_null() ? true : member_not_null;
              }
            }
            if (OB_SUCC(ret)) {
              data_type.set_not_null(member_not_null);
              int64_t default_expr_idx = OB_INVALID_INDEX;
              // not null不能是空值，但是这个只能防住 a not null这种情况， a not null := null这种防不住
              // 所以在record的generate_default_value里面增加了null check
              if ((member_not_null) && OB_ISNULL(default_node)) {
                ret = OB_ERR_INIT_NOTNULL_ILLEGAL;
                LOG_WARN("not null member does not dafault value", K(ret));
              } else {
                if (OB_NOT_NULL(default_node)) {
                  ObString expr_str(default_node->str_len_, default_node->str_value_);
                  OZ (resolve_expr(default_node, unit_ast, default_expr,
                                  combine_line_and_col(default_node->stmt_loc_), true, &data_type));
                  OZ (check_param_default_expr_legal(default_expr, false));
                  OX (default_expr_idx = unit_ast.get_expr_count() - 1);
                } else {
                  /*
                  To define a RECORD type, specify its name and define its fields. To define a field,
                  specify its name and data type. By default, the initial value of a field is NULL
                  */
                  // 设置成invalid，后面赋值的时候发现是invalid，赋值为null
                  default_expr_idx = OB_INVALID_INDEX;
                }
              }
              if (OB_SUCC(ret)) {
                if (OB_FAIL(record_type->add_record_member(member_name,
                                                          data_type,
                                                          default_expr_idx,
                                                          default_expr))) {
                  LOG_WARN("add record member failed", K(ret));
                } else {
                  record_type->set_not_null(record_type->get_not_null());
                }
              }
            }
          }
        }
      }

      if (OB_SUCC(ret) && OB_NOT_NULL(block)) {
        OZ (block->get_namespace().delete_symbols());
      }
      if (OB_NOT_NULL(parent)) {
        set_current(*parent);
        resolve_ctx_.params_.secondary_namespace_ = parent_namespace;
      }

      if (OB_SUCC(ret)) {
        if (OB_ISNULL(current_block_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("current block is null");
        } else if (OB_FAIL(current_block_->get_namespace().add_type(record_type))) {
          LOG_WARN("failed to add record type to type table", K(*record_type), K(ret));
        } else {
          if (!OB_ISNULL(stmt)) {
            stmt->set_user_type(static_cast<ObUserDefinedType *>(record_type));
          }
        }
      }
    }
  }
  return ret;
}

#ifdef OB_BUILD_ORACLE_PL
int ObPLResolver::resolve_sp_subtype_precision(ObSQLSessionInfo &sesion_info,
                                               ObIArray<ObRawExpr*>& params,
                                               const ObUserDefinedType *user_type,
                                               ObPLDataType &pl_type)
{
  int ret = OB_SUCCESS;
  int64_t precision = -1;
  int64_t scale = -1;
  const ObUserDefinedSubType *sub_type = NULL;

  CK (OB_NOT_NULL(user_type));
  CK (params.count() > 0);

  if (OB_FAIL(ret)) {
  } else if (!user_type->is_subtype()) {
    ret = OB_ERR_TYPE_CANT_CONSTRAINED;
    LOG_WARN("PLS-00566: type name \"string\" cannot be constrained", K(ret));
  } else if (params.count() > 2) {
    ret = OB_ERR_IMPROPER_CONSTRAINT_FORM;
    LOG_WARN("PLS-00572: improper constraint form used", K(ret), K(params.count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < params.count(); ++i) {
    int64_t result = -1;
    OZ (get_number_literal_value(params.at(i), result));
    OX (0==i ? precision = result : scale = result);
  }
  CK (OB_NOT_NULL(sub_type = static_cast<const ObUserDefinedSubType *>(user_type)));
  OX (pl_type = *(sub_type->get_base_type()));
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(pl_type.get_data_type())) {
      ret = OB_ERR_TYPE_CANT_CONSTRAINED;
      LOG_WARN("PLS-00566: type name \"string\" cannot be constrained", K(ret), K(pl_type));
    } else if (2 == params.count()
              && pl_type.get_data_type()->get_type_class() != ObNumberTC) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("only can specific scale for number type class",
               K(ret), KPC(pl_type.get_data_type()));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "specific scale for non number type class");
    }
  }

  if (OB_SUCC(ret)) {
    //reference by ObResolverUtils::resolve_data_type
    ObDataType &data_type = (*pl_type.get_data_type());
    const ObAccuracy &default_accuracy
      = ObAccuracy::DDL_DEFAULT_ACCURACY2[true][data_type.get_obj_type()];
    switch (data_type.get_type_class()) {
      case ObIntTC:
      case ObUIntTC: {
        if (precision <= 0) {
          precision = default_accuracy.get_precision();
        }
        if (precision > OB_MAX_INTEGER_DISPLAY_WIDTH) {
          ret = OB_ERR_TOO_BIG_DISPLAYWIDTH;
        } else {
          data_type.set_precision(precision);
          data_type.set_scale(0);
        }
      } break;
      case ObFloatTC:
      case ObDoubleTC: {
        data_type.set_precision(precision);
        data_type.set_scale(scale);
        break;
      }
      case ObNumberTC: {
        if (data_type.get_meta_type().is_number_float()) {
          if (precision != PRECISION_UNKNOWN_YET
            && (OB_UNLIKELY(precision < OB_MIN_NUMBER_FLOAT_PRECISION)
                || OB_UNLIKELY(precision > OB_MAX_NUMBER_FLOAT_PRECISION))) {
            ret = OB_FLOAT_PRECISION_OUT_RANGE;
            LOG_WARN("precision of float out of range", K(ret), K(precision));
          } else {
            data_type.set_precision(precision);
            data_type.set_scale(ORA_NUMBER_SCALE_UNKNOWN_YET);
          }
        } else if (OB_UNLIKELY(precision < OB_MIN_NUMBER_PRECISION)
                   || OB_UNLIKELY(precision > OB_MAX_NUMBER_PRECISION)) {
          ret = OB_NUMERIC_PRECISION_OUT_RANGE;
          LOG_WARN("precision of number overflow", K(ret), K(scale), K(precision));
        } else if (2 == params.count()
                  && (OB_UNLIKELY(scale < OB_MIN_NUMBER_SCALE)
                      || OB_UNLIKELY(scale > OB_MAX_NUMBER_SCALE))) {
          ret = OB_NUMERIC_SCALE_OUT_RANGE;
          LOG_WARN("scale of number out of range", K(ret), K(scale));
        } else {
          data_type.set_precision(precision);
          if (2 == params.count()) {
            data_type.set_scale(scale);
          } else {
            data_type.set_scale(0);
          }
        }
      }  break;
      case ObOTimestampTC: {
        if (OB_UNLIKELY(precision > OB_MAX_TIMESTAMP_TZ_PRECISION)) {
          ret = OB_ERR_DATETIME_INTERVAL_PRECISION_OUT_OF_RANGE;
        } else {
          data_type.set_precision(
            static_cast<int16_t>(default_accuracy.get_precision() + precision));
          data_type.set_scale(precision);
        }
      } break;
      case ObStringTC: {
        int32_t length = precision;
        int64_t nchar_mbminlen = 0;
        const ObSessionNLSParams &nls_session_param = sesion_info.get_session_nls_params();
        data_type.set_length(length);
        if (0 == length) {
          ret = OB_ERR_ZERO_LEN_COL;
          LOG_WARN("Oracle not allowed zero length", K(ret));
        } else if (OB_FAIL(ObCharset::get_mbminlen_by_coll(
          nls_session_param.nls_nation_collation_, nchar_mbminlen))) {
          LOG_WARN("fail to get mbminlen of nchar", K(ret), K(nls_session_param));
        } else if (((ObVarcharType == data_type.get_obj_type()
                     || ObNVarchar2Type == data_type.get_obj_type())
                      && OB_MAX_ORACLE_VARCHAR_LENGTH < length)
                     || (ObCharType == data_type.get_obj_type()
                         && OB_MAX_ORACLE_CHAR_LENGTH_BYTE < length)
                     || (ObNCharType == data_type.get_obj_type()
                         && OB_MAX_ORACLE_CHAR_LENGTH_BYTE < length * nchar_mbminlen)) {
          ret = OB_ERR_TOO_LONG_COLUMN_LENGTH;
          LOG_WARN("column data length is invalid",
                   K(ret), K(length), K(data_type), K(nchar_mbminlen));
        }
      } break;
      case ObRawTC:
      case ObLobTC:
      case ObTextTC:
      case ObJsonTC:
      case ObGeometryTC: {
        data_type.set_length(precision);
      } break;
      case ObRowIDTC: {
        if (ob_is_urowid(data_type.get_obj_type())) {
          if (precision > OB_MAX_USER_ROW_KEY_LENGTH) {
            ret = OB_ERR_TOO_LONG_COLUMN_LENGTH;
            LOG_WARN("column data length is invalid", K(ret), K(precision), K(data_type));
          } else {
            data_type.set_length(precision);
          }
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("only support urowid type for now", K(ret), K(data_type));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "non-urowid type");
        }
      } break;
      default: {
        ret = OB_ERR_ILLEGAL_TYPE;
        LOG_WARN("Unsupported data type for subtype basetype", K(ret), K(data_type));
      } break;
    }
  }
  return ret;
}
#endif

int ObPLResolver::resolve_sp_composite_type(const ParseNode *sp_data_type_node,
                                            ObPLCompileUnitAST &func,
                                            ObPLDataType &data_type,
                                            ObPLExternTypeInfo *extern_type_info)
{
  int ret = OB_SUCCESS;

  ObArray<ObObjAccessIdent> obj_access_idents;
  ObArray<ObObjAccessIdx> access_idxs;
  const ObUserDefinedType *user_type = NULL;
  ObArray<ObRawExpr*> params;
  SET_LOG_CHECK_MODE();
  CK (OB_NOT_NULL(sp_data_type_node));
  CK (OB_NOT_NULL(current_block_));
  CK (T_SP_OBJ_ACCESS_REF == sp_data_type_node->type_);
  OZ (resolve_obj_access_idents(*(sp_data_type_node), obj_access_idents, func));
  CK (obj_access_idents.count() > 0);
  OX (obj_access_idents.at(obj_access_idents.count() - 1).has_brackets_ = false);
  for (int64_t i = 0; OB_SUCC(ret) && i < obj_access_idents.count(); ++i) {
    OZ (resolve_access_ident(obj_access_idents.at(i),
                             current_block_->get_namespace(),
                             expr_factory_,
                             &resolve_ctx_.session_info_,
                             access_idxs,
                             func));
  }
  if (OB_ERR_SP_UNDECLARED_VAR == ret) {
    if (2 == sp_data_type_node->num_child_ 
          && NULL != sp_data_type_node->children_[1]
          && T_SP_CPARAM_LIST == sp_data_type_node->children_[1]->type_) {
      ret = OB_ERR_TYPE_CANT_CONSTRAINED;
      LOG_USER_ERROR(OB_ERR_TYPE_CANT_CONSTRAINED, 
                    obj_access_idents.at(obj_access_idents.count()-1).access_name_.length(),
                    obj_access_idents.at(obj_access_idents.count()-1).access_name_.ptr());
    } else {
      ret = OB_ERR_SP_UNDECLARED_TYPE;
      LOG_WARN("failed to resolve composite type", K(ret), K(obj_access_idents));
      LOG_USER_ERROR(OB_ERR_SP_UNDECLARED_TYPE,
                    obj_access_idents.at(obj_access_idents.count()-1).access_name_.length(),
                    obj_access_idents.at(obj_access_idents.count()-1).access_name_.ptr());
    }
  }
#ifdef OB_BUILD_ORACLE_PL
  //所有Type表都没有找到, 看下是否是SYS_REFCURSOR
  if (OB_FAIL(ret)
      && 1 == obj_access_idents.count()
      && ObCharset::case_insensitive_equal(obj_access_idents.at(0).access_name_, "SYS_REFCURSOR")) {
    ret = OB_SUCCESS;
    ob_reset_tsi_warning_buffer();
    CK (OB_NOT_NULL(current_block_->get_namespace().get_type_table()));
    OX (user_type = &(current_block_->get_namespace().get_type_table()->get_sys_refcursor_type()));
    OX (data_type = *user_type);

  } else
#endif
  if (OB_SUCC(ret)) {
    if (!ObObjAccessIdx::is_type(access_idxs)) {
      ret = OB_ERR_SP_UNDECLARED_TYPE;
      LOG_WARN("type not exists", K(ret), K(access_idxs));
    }

    OZ (current_block_->get_namespace().get_pl_data_type_by_id(
      access_idxs.at(access_idxs.count() - 1).var_index_, user_type), access_idxs);
    CK (OB_NOT_NULL(user_type));
    if (OB_SUCC(ret)) {
      if (!obj_access_idents.at(obj_access_idents.count() - 1).params_.empty()) {
        ObArray<ObRawExpr*> params;
        OZ (obj_access_idents.at(obj_access_idents.count() -1 ).extract_params(0, params));
#ifdef OB_BUILD_ORACLE_PL
        OZ (resolve_sp_subtype_precision(resolve_ctx_.session_info_, params, user_type, data_type));
#endif
      } else {
        OX (data_type = *user_type);
      }
    }
  }

  if (OB_NOT_NULL(extern_type_info)) {
    if (is_object_not_exist_error(ret)
      && obj_access_idents.count() >= 1) {
      LOG_USER_WARN(OB_ERR_SP_UNDECLARED_TYPE,
                    obj_access_idents.at(obj_access_idents.count() - 1).access_name_.length(),
                    obj_access_idents.at(obj_access_idents.count() - 1).access_name_.ptr());
      record_error_line(sp_data_type_node, resolve_ctx_.session_info_);
      ObPL::insert_error_msg(ret);
      ret = OB_SUCCESS;
      OZ (resolve_extern_type_info(resolve_ctx_.schema_guard_,
                                   resolve_ctx_.session_info_,
                                   obj_access_idents,
                                   extern_type_info));
    } else if (OB_FAIL(ret) || data_type.is_obj_type()) {
#ifdef OB_BUILD_ORACLE_PL
    } else if (user_type->is_sys_refcursor_type()) {
      OX (extern_type_info->flag_ = ObParamExternType::SP_EXTERN_SYS_REFCURSOR);
      OX (extern_type_info->type_name_ = ObString("SYS_REFCURSOR"));
#endif
    } else if (ObObjAccessIdx::is_local_type(access_idxs)) {
      OX (extern_type_info->flag_ = ObParamExternType::SP_EXTERN_LOCAL_VAR);
      OX (extern_type_info->type_name_ = access_idxs.at(access_idxs.count()-1).var_name_);
      if (access_idxs.count() > 1) {
        OX (extern_type_info->type_subname_ = access_idxs.at(access_idxs.count()-2).var_name_);
      }
    } else if (ObObjAccessIdx::is_external_type(access_idxs)) {
      OZ (resolve_extern_type_info(resolve_ctx_.schema_guard_, access_idxs, extern_type_info));
    }
  }
  CANCLE_LOG_CHECK_MODE();
  return ret;
}

int ObPLResolver::build_pl_integer_type(ObPLIntegerType type, ObPLDataType &data_type)
{
  int ret = OB_SUCCESS;
  ObDataType scalar_data_type;
  const ObAccuracy &default_accuracy = ObAccuracy::DDL_DEFAULT_ACCURACY[ObInt32Type];
  scalar_data_type.set_obj_type(ObInt32Type);
  scalar_data_type.set_precision(default_accuracy.get_precision());
  scalar_data_type.set_scale(0);
  scalar_data_type.set_zero_fill(false);
  scalar_data_type.meta_.set_scale(scalar_data_type.get_scale());
  data_type.set_pl_integer_type(type, scalar_data_type);
  OZ (resolve_sp_integer_constraint(data_type), data_type);
  return ret;
}

int ObPLResolver::resolve_sp_integer_type(const ParseNode *sp_data_type_node,
                                          ObPLDataType &data_type)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(sp_data_type_node));
  CK (T_SP_INTEGER_TYPE == sp_data_type_node->type_);
  if (OB_SUCC(ret)) {
    int64_t type = sp_data_type_node->value_;
    switch (type)
    {
#define SET_PL_INTEGER_TYPE(type)                               \
  case SP_##type: {                                             \
    OZ (build_pl_integer_type(PL_##type, data_type));           \
    break;                                                      \
  }
      SET_PL_INTEGER_TYPE(PLS_INTEGER);
      SET_PL_INTEGER_TYPE(BINARY_INTEGER);
      SET_PL_INTEGER_TYPE(NATURAL);
      SET_PL_INTEGER_TYPE(NATURALN);
      SET_PL_INTEGER_TYPE(POSITIVE);
      SET_PL_INTEGER_TYPE(POSITIVEN);
      SET_PL_INTEGER_TYPE(SIGNTYPE);
      SET_PL_INTEGER_TYPE(SIMPLE_INTEGER);
#undef SET_PL_INTEGER_TYPE
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected pl integer type", K(type), K(ret));
      }
    }
  }
  return ret;
}

int ObPLResolver::resolve_sp_integer_constraint(ObPLDataType &pls_type)
{
  int ret = OB_SUCCESS;
  ObPLIntegerType type = pls_type.get_pl_integer_type();
  switch (type)
  {
  case PL_PLS_INTEGER:
  case PL_BINARY_INTEGER:
  case PL_SIMPLE_INTEGER: {
    pls_type.set_range(-2147483648, 2147483647);
    pls_type.set_not_null(PL_SIMPLE_INTEGER == type);
  }
    break;
  case PL_NATURAL:
  case PL_NATURALN: {
    pls_type.set_range(0, 2147483647);
    pls_type.set_not_null(PL_NATURALN == type);
  }
    break;
  case PL_POSITIVE:
  case PL_POSITIVEN: {
    pls_type.set_range(1, 2147483647);
    pls_type.set_not_null(PL_POSITIVEN == type);
  }
    break;
  case PL_SIGNTYPE: {
    pls_type.set_range(-1, 1);
  }
    break;
  default:
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected pl integer type", K(type), K(ret));
  }
  return ret;
}

int ObPLResolver::resolve_sp_scalar_type(ObIAllocator &allocator,
                                         const ParseNode *sp_data_type_node,
                                         const ObString &ident_name,
                                         const ObSQLSessionInfo &session_info,
                                         ObPLDataType &data_type,
                                         bool is_for_param_type,
                                         uint64_t package_id)
{
  int ret = OB_SUCCESS;
  CK(OB_NOT_NULL(sp_data_type_node));
  if (OB_FAIL(ret)) {
  } else if (T_SP_INTEGER_TYPE == sp_data_type_node->type_) {
    if (OB_FAIL(resolve_sp_integer_type(sp_data_type_node, data_type))) {
      LOG_WARN("fail to resolve sp integer data type", K(ret));
    }
  } else {
    ObDataType scalar_data_type;
    omt::ObTenantConfigGuard tcg(
        TENANT_CONF(session_info.get_effective_tenant_id()));
    bool convert_real_to_decimal =
        (tcg.is_valid() && tcg->_enable_convert_real_to_decimal);
    if (OB_FAIL(ObResolverUtils::resolve_data_type(*sp_data_type_node,
                                      ident_name,
                                      scalar_data_type,
                                      is_oracle_mode(),
                                      true,
                                      session_info.get_session_nls_params(),
                                      session_info.get_effective_tenant_id(),
                                      convert_real_to_decimal))) {
      LOG_WARN("resolve data type failed", K(ret));
    } else if (scalar_data_type.get_meta_type().is_string_or_lob_locator_type()
            || scalar_data_type.get_meta_type().is_enum_or_set()
            || scalar_data_type.get_meta_type().is_json()
            || scalar_data_type.get_meta_type().is_geometry()) {
      ObObjMeta tmp_meta = scalar_data_type.get_meta_type();
      if (ObLongTextType == tmp_meta.get_type() && is_oracle_mode()) {
        scalar_data_type.set_meta_type(tmp_meta);
        scalar_data_type.set_length(-1); // was set ObLobType
      }
      ObCharsetType charset_type = scalar_data_type.get_charset_type();
      ObCollationType collation_type = scalar_data_type.get_collation_type();
      if (CHARSET_ANY == charset_type) {
        if (!is_for_param_type) {
          ret = OB_ERR_ANY_CS_NOT_ALLOWED;
          LOG_WARN("PLS-00551: character set ANY_CS is only allowed on a subprogram parameter", K(ret));
        }
      } else if (CHARSET_INVALID == charset_type
                 && CS_TYPE_INVALID == collation_type) {
        if (lib::is_mysql_mode()) {
          OZ (session_info.get_character_set_connection(charset_type));
          OZ (session_info.get_collation_connection(collation_type));
        } else { // oracle mode
          if (OB_FAIL(ret)) {
            // do nothing
          } else if (OB_MIN_SYS_PL_OBJECT_ID < package_id
                       && package_id < OB_MAX_SYS_PL_OBJECT_ID) {
            // system package
            session_info.get_collation_database(collation_type);
          } else {
            collation_type = scalar_data_type.get_meta_type().is_nstring() ?
              session_info.get_nls_collation_nation()
            : session_info.get_nls_collation();
          }
          if (OB_SUCC(ret)) {
            charset_type = CS_TYPE_ANY == collation_type ?
                           CHARSET_ANY : ObCharset::charset_type_by_coll(collation_type);
          }
        }
      } else if (OB_FAIL(ObCharset::check_and_fill_info(charset_type, collation_type))) {
        LOG_WARN("fail to fill collation info", K(charset_type), K(collation_type), K(ret));
      }
      if (OB_SUCC(ret)) {
        scalar_data_type.set_charset_type(charset_type);
        scalar_data_type.set_collation_type(collation_type);
      }
      if (OB_SUCC(ret) &&
          (scalar_data_type.get_meta_type().is_lob()
            || scalar_data_type.get_meta_type().is_lob_locator()
            || scalar_data_type.get_meta_type().is_json()
            || scalar_data_type.get_meta_type().is_geometry())
          && CHARSET_ANY != scalar_data_type.get_charset_type()) {
        ObObjType type = scalar_data_type.get_obj_type();
        type = ob_is_lob_locator(type) ? ObLongTextType : type;
        int32_t length = scalar_data_type.get_length();
        if (OB_FAIL(ObDDLResolver::check_text_length(scalar_data_type.get_charset_type(),
                                                     scalar_data_type.get_collation_type(),
                                                     ident_name.ptr(),
                                                     type,
                                                     length,
                                                     true))) {
          LOG_WARN("failed to check text length", K(ret), K(scalar_data_type));
        } else {
          scalar_data_type.set_length(length);
          if (lib::is_mysql_mode()) {
            scalar_data_type.set_obj_type(type);
          }
        }
      }
      if (OB_SUCC(ret)) {
        scalar_data_type.set_charset_type(charset_type);
        scalar_data_type.set_collation_type(collation_type);
        scalar_data_type.set_length_semantics(scalar_data_type.get_length_semantics());
        scalar_data_type.meta_.set_collation_level(CS_LEVEL_IMPLICIT);
      }
    }
    if (OB_SUCC(ret) && scalar_data_type.get_meta_type().is_enum_or_set()) {
      ObArray<ObString> type_info_array;
      CK(OB_LIKELY(4 == sp_data_type_node->num_child_));
      CK(OB_NOT_NULL(sp_data_type_node->children_[3]));
      int32_t length = 0;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ObResolverUtils::resolve_extended_type_info(*(sp_data_type_node->children_[3]),
                                                                     type_info_array))) {
        LOG_WARN("fail to resolve extended type info", K(ret));
      } else if (OB_FAIL(ObResolverUtils::check_extended_type_info(
                  allocator,
                  type_info_array,
                  // the resolved type info array is session connection collation.
                  session_info.get_local_collation_connection(),
                  ident_name,
                  scalar_data_type.get_obj_type(),
                  scalar_data_type.get_collation_type(),
                  session_info.get_sql_mode()))) {
        LOG_WARN("fail to check extended type info", K(type_info_array), K(ret));
      } else if (OB_FAIL(data_type.set_type_info(type_info_array))) {
        LOG_WARN("fail to set type info", K(ret));
      } else if (OB_FAIL(ObDDLResolver::calc_enum_or_set_data_length(type_info_array,
                                                  scalar_data_type.get_collation_type(),
                                                  scalar_data_type.get_obj_type(), length))) {
        LOG_WARN("failed to calc enum or set data length", K(ret));
      } else {
        scalar_data_type.set_length(length);
      }
    }
    if (OB_SUCC(ret)) {
      // resolve_data_type会将scale设置在accuracy上,这里将meta_的scale设置下
      if (scalar_data_type.get_meta_type().is_bit()) { // 对于bit类型, scale存储的是长度信息
        scalar_data_type.meta_.set_scale(scalar_data_type.get_precision());
      } else {
        scalar_data_type.meta_.set_scale(scalar_data_type.get_scale());
        if (is_lob_storage(scalar_data_type.get_meta_type().get_type())) {
          scalar_data_type.meta_.set_has_lob_header();
        }
      }
      data_type.set_data_type(scalar_data_type);
    }
    if (OB_SUCC(ret)
        && ObRawType == scalar_data_type.get_meta_type().get_type()
        && lib::is_oracle_mode()) {
      const int64_t data_len = scalar_data_type.accuracy_.get_length();
      if (data_len < 0 || data_len > OB_MAX_ORACLE_RAW_PL_VAR_LENGTH) {
        ret = OB_ERR_TOO_LONG_COLUMN_LENGTH;
        LOG_WARN("raw data length is invalid",
                 K(ret), K(OB_MAX_ORACLE_RAW_PL_VAR_LENGTH), "real_data_length", data_len);
        LOG_USER_ERROR(OB_ERR_TOO_LONG_COLUMN_LENGTH,
                       "RAW TYPE", static_cast<int>(OB_MAX_ORACLE_RAW_PL_VAR_LENGTH));
      } else if (0 == data_len) {
        ret = OB_ERR_ZERO_LEN_COL;
        LOG_WARN("raw data length cannot be zero on oracle mode", K(ret), K(data_len));
      }
    }
  }
  LOG_DEBUG("resolve sp scalar type result", K(ret), K(data_type), K(lbt()));
  return ret;
}

int ObPLResolver::get_view_select_stmt(
  const ObPLResolveCtx &ctx, const ObTableSchema* view_schema, ObSelectStmt *&select_stmt)
{
  int ret = OB_SUCCESS;
  ObStmtFactory stmt_factory(ctx.allocator_);
  ObRawExprFactory expr_factory(ctx.allocator_);
  const ObDatabaseSchema *db_schema = NULL;
  ObSqlString select_sql;
  ParseResult parse_result;
  ObParser parser(ctx.allocator_, ctx.session_info_.get_sql_mode());
  ObSchemaChecker schema_checker;
  ObResolverParams resolver_ctx;
  ParseNode *select_stmt_node = NULL;

  OZ (ctx.schema_guard_.get_database_schema(view_schema->get_tenant_id(),
      view_schema->get_database_id(), db_schema));
  CK (OB_NOT_NULL(db_schema));
  OZ (select_sql.append_fmt(
    "select * from \"%.*s\".\"%.*s\"",
    db_schema->get_database_name_str().length(), db_schema->get_database_name_str().ptr(),
    view_schema->get_table_name_str().length(), view_schema->get_table_name_str().ptr()));
  OZ (parser.parse(select_sql.string(), parse_result));
  OZ (schema_checker.init(ctx.schema_guard_, ctx.session_info_.get_sessid()));

  OX (resolver_ctx.allocator_ = &(ctx.allocator_));
  OX (resolver_ctx.schema_checker_ = &schema_checker);
  OX (resolver_ctx.session_info_ = &(ctx.session_info_));
  OX (resolver_ctx.expr_factory_ = &expr_factory);
  OX (resolver_ctx.stmt_factory_ = &stmt_factory);
  CK (OB_NOT_NULL(resolver_ctx.query_ctx_ = stmt_factory.get_query_ctx()));
  OX (resolver_ctx.query_ctx_->question_marks_count_
        = static_cast<int64_t>(parse_result.question_mark_ctx_.count_));

  CK (OB_NOT_NULL(select_stmt_node = parse_result.result_tree_->children_[0]));
  CK (T_SELECT == select_stmt_node->type_);

  ObSelectResolver select_resolver(resolver_ctx);
  OZ (SMART_CALL(select_resolver.resolve(*select_stmt_node)));
  CK (OB_NOT_NULL(select_stmt = static_cast<ObSelectStmt*>(select_resolver.get_basic_stmt())));

  // CK (OB_LIKELY(1 == select_stmt->get_table_size()));
  // CK (OB_NOT_NULL(view_item = select_stmt->get_table_item(0)));
  // CK (OB_NOT_NULL(select_stmt = static_cast<ObSelectStmt*>(view_item->ref_query_)));
  return ret;
}

int ObPLResolver::fill_record_type(
  ObSchemaGetterGuard &schema_guard, ObIAllocator &allocator, ObSelectStmt *select_stmt, ObRecordType *&record_type)
{
  int ret = OB_SUCCESS;
  const SelectItem *select_item = NULL;
  const ObRawExpr *expr = NULL;
  CK (OB_NOT_NULL(select_stmt));
  OZ (record_type->record_members_init(&allocator, select_stmt->get_select_item_size()));
  for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt->get_select_item_size(); ++i) {
    ObString copy_name;
    ObDataType data_type;
    ObPLDataType pl_type;
    CK (OB_NOT_NULL(select_item = &(select_stmt->get_select_item(i))));
    CK (OB_NOT_NULL(expr = select_item->expr_));
    if (OB_FAIL(ret)) {
    } else if (expr->get_result_type().is_ext() || expr->get_result_type().is_user_defined_sql_type()) {
      const ObUDTTypeInfo *udt_info = NULL;
      const ObUserDefinedType *user_type = NULL;
      ObArenaAllocator allocator;
      uint64_t udt_id = expr->get_result_type().get_expr_udt_id();
      uint64_t tenant_id = get_tenant_id_by_object_id(udt_id);
      OZ (schema_guard.get_udt_info(tenant_id, udt_id, udt_info));
      CK (OB_NOT_NULL(udt_info));
      OZ (udt_info->transform_to_pl_type(allocator, user_type));
      OX (pl_type = *user_type);
    } else {
      OX (data_type.set_meta_type(expr->get_result_type()));
      OX (data_type.set_accuracy(expr->get_result_type().get_accuracy()));
      OX (pl_type.set_data_type(data_type));
    }
    OZ (ob_write_string(allocator, select_item->alias_name_, copy_name));
    OZ (record_type->add_record_member(copy_name, pl_type));
  }
  return ret;
}

int ObPLResolver::build_record_type_by_view_schema(const ObPLResolveCtx &ctx,
                                                   const ObTableSchema* view_schema,
                                                   ObRecordType *&record_type)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *select_stmt = NULL;
  ObSelectStmt *real_stmt = NULL;
  ObArenaAllocator alloc;
  ObStmtFactory stmt_factory(alloc);
  ObRawExprFactory expr_factory(alloc);
  const ObDatabaseSchema *db_schema = NULL;
  ObSqlString select_sql;
  ParseResult parse_result;
  ObParser parser(alloc, ctx.session_info_.get_sql_mode(),
                  ctx.session_info_.get_charsets4parser());
  ObSchemaChecker schema_checker;
  ObResolverParams resolver_ctx;
  ParseNode *select_stmt_node = NULL;

  OZ (ctx.schema_guard_.get_database_schema(view_schema->get_tenant_id(),
      view_schema->get_database_id(), db_schema));
  CK (OB_NOT_NULL(db_schema));
  OZ (select_sql.append_fmt(
    "select * from \"%.*s\".\"%.*s\"",
    db_schema->get_database_name_str().length(), db_schema->get_database_name_str().ptr(),
    view_schema->get_table_name_str().length(), view_schema->get_table_name_str().ptr()));
  OZ (parser.parse(select_sql.string(), parse_result));
  OZ (schema_checker.init(ctx.schema_guard_, ctx.session_info_.get_sessid()));

  OX (resolver_ctx.allocator_ = &(alloc));
  OX (resolver_ctx.schema_checker_ = &schema_checker);
  OX (resolver_ctx.session_info_ = &(ctx.session_info_));
  OX (resolver_ctx.expr_factory_ = &expr_factory);
  OX (resolver_ctx.stmt_factory_ = &stmt_factory);
  OX (resolver_ctx.sql_proxy_ = &(ctx.sql_proxy_));
  CK (OB_NOT_NULL(resolver_ctx.query_ctx_ = stmt_factory.get_query_ctx()));
  OX (resolver_ctx.query_ctx_->question_marks_count_
        = static_cast<int64_t>(parse_result.question_mark_ctx_.count_));

  CK (OB_NOT_NULL(select_stmt_node = parse_result.result_tree_->children_[0]));
  CK (T_SELECT == select_stmt_node->type_);

  ObSelectResolver select_resolver(resolver_ctx);
  OZ (SMART_CALL(select_resolver.resolve(*select_stmt_node)));
  CK (OB_NOT_NULL(select_stmt = static_cast<ObSelectStmt*>(select_resolver.get_basic_stmt())));
  // OZ (get_view_select_stmt(resolve_ctx, view_schema, select_stmt));
  CK (OB_NOT_NULL(real_stmt = select_stmt->get_real_stmt()));
  CK (OB_NOT_NULL(record_type));
  OZ (fill_record_type(ctx.schema_guard_, ctx.allocator_, real_stmt, record_type));
  return ret;
}

int ObPLResolver::build_record_type_by_table_schema(ObSchemaGetterGuard &schema_guard,
                                                    common::ObIAllocator &allocator,
                                                    const ObTableSchema* table_schema,
                                                    ObRecordType *&record_type,
                                                    bool with_rowid)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(table_schema));
  OZ (record_type->record_members_init(&allocator, table_schema->get_column_count() + 1));
  if (OB_SUCC(ret)) {
    ObTableSchema::const_column_iterator cs_iter = table_schema->column_begin();
    ObTableSchema::const_column_iterator cs_iter_end = table_schema->column_end();
    for (; OB_SUCC(ret) && cs_iter != cs_iter_end; cs_iter++) {
      const ObColumnSchemaV2 &column_schema = **cs_iter;
      if (!column_schema.is_hidden() && !(column_schema.is_invisible_column() && !with_rowid)) {
        ObPLDataType pl_type;
        if (column_schema.get_meta_type().is_user_defined_sql_type()) {
          const ObUDTTypeInfo *udt_info = NULL;
          const ObUserDefinedType *user_type = NULL;
          ObArenaAllocator allocator;
          uint64_t tenant_id = get_tenant_id_by_object_id(column_schema.get_sub_data_type());
          OZ (schema_guard.get_udt_info(tenant_id, column_schema.get_sub_data_type(), udt_info));
          CK (OB_NOT_NULL(udt_info));
          OZ (udt_info->transform_to_pl_type(allocator, user_type));
          CK (OB_NOT_NULL(user_type));
          OX (pl_type = *user_type);
        } else {
          ObDataType data_type;
          data_type.set_meta_type(column_schema.get_meta_type());
          data_type.set_accuracy(column_schema.get_accuracy());
          if (data_type.get_meta_type().is_bit()) { // 对于bit类型, scale存储的是长度信息
            data_type.meta_.set_scale(data_type.get_precision());
          } else {
            data_type.meta_.set_scale(data_type.get_scale());
          }
          if (column_schema.is_enum_or_set()) {
            OZ (pl_type.set_type_info(column_schema.get_extended_type_info()));
          }
          OX (pl_type.set_data_type(data_type));
        }
        if (OB_SUCC(ret)) {
          char *name_buf = NULL;
          ObString column_name = column_schema.get_column_name_str();
          if (OB_ISNULL(name_buf =
              static_cast<char*>(allocator.alloc(column_name.length() + 1)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to alloc column name buf", K(ret), K(column_name));
          } else {
            MEMCPY(name_buf, column_name.ptr(), column_name.length());
            ObString deep_copy_name(column_name.length(), name_buf);
            OZ (record_type->add_record_member(deep_copy_name, pl_type));
          }
        }
      }
    }
  }
  if (OB_SUCC(ret) && with_rowid && lib::is_oracle_mode()) {
    ObDataType data_type;
    ObPLDataType pl_type;
    data_type.set_obj_type(ObURowIDType);
    data_type.set_length(OB_MAX_USER_ROW_KEY_LENGTH);
    data_type.set_precision(-1);
    data_type.set_collation_type(CS_TYPE_BINARY);
    pl_type.set_data_type(data_type);
    char *name_buf = NULL;
    ObString column_name(OB_HIDDEN_LOGICAL_ROWID_COLUMN_NAME);
    if (OB_ISNULL(name_buf = static_cast<char*>(allocator.
        alloc(OB_HIDDEN_LOGICAL_ROWID_COLUMN_NAME_LENGTH + 1)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc column name buf", K(ret), K(column_name));
    } else {
      MEMCPY(name_buf, column_name.ptr(), column_name.length());
      ObString deep_copy_name(column_name.length(), name_buf);
      OZ (record_type->add_record_member(deep_copy_name, pl_type));
    }
  }
  return ret;
}

int ObPLResolver::build_record_type_by_schema(
  const ObPLResolveCtx &resolve_ctx, const ObTableSchema* table_schema,
  ObRecordType *&record_type, bool with_rowid)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(table_schema));
  OX (record_type = NULL);
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(record_type =
        static_cast<ObRecordType*>(resolve_ctx.allocator_.alloc(sizeof(ObRecordType))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory for ObRecordtype!", K(ret));
    } else {
      ObSqlString type_fmt_name;
      ObString type_name;
      OZ (type_fmt_name.append_fmt("%s%%ROWTYPE", table_schema->get_table_name()));
      OZ (ob_write_string(resolve_ctx.allocator_, type_fmt_name.string(), type_name));
      OX (record_type = new(record_type)ObRecordType())
      OX (record_type->set_name(type_name));
      OX (record_type->set_user_type_id(table_schema->get_table_id()));
      OX (record_type->set_type_from(PL_TYPE_ATTR_ROWTYPE));
    }
  }
  if (OB_SUCC(ret)) {
    if (table_schema->is_view_table() && !table_schema->is_materialized_view()) {
      OZ (build_record_type_by_view_schema(
        resolve_ctx, table_schema, record_type));
    } else {
      OZ (build_record_type_by_table_schema(
        resolve_ctx.schema_guard_, resolve_ctx.allocator_, table_schema, record_type, with_rowid));
    }
  }
  return ret;
}

int ObPLResolver::resolve_extern_type_info(ObSchemaGetterGuard &schema_guard,
                                           const ObSQLSessionInfo &session_info,
                                           const ObIArray<ObObjAccessIdent> &access_idents,
                                           ObPLExternTypeInfo *extern_type_info)
{
  int ret = OB_SUCCESS;
  SET_LOG_CHECK_MODE();
  CK (OB_NOT_NULL(extern_type_info));
  CK (access_idents.count() > 0 && access_idents.count() <= 3);
  if (OB_FAIL(ret)) {
  } else if (3 == access_idents.count()) { //db.pkg.type
    OZ (schema_guard.get_database_id(session_info.get_effective_tenant_id(),
      access_idents.at(0).access_name_, extern_type_info->type_owner_));
    OX (extern_type_info->type_subname_ = access_idents.at(1).access_name_);
    OX (extern_type_info->flag_ = ObParamExternType::SP_EXTERN_PKG);
  } else if (2 == access_idents.count()) { // pkg.type or db.type
    OZ (schema_guard.get_database_id(session_info.get_effective_tenant_id(),
      access_idents.at(0).access_name_, extern_type_info->type_owner_));
    if (OB_ERR_BAD_DATABASE == ret || OB_INVALID_ID == extern_type_info->type_owner_) {
      ret = OB_SUCCESS;
      OX (extern_type_info->type_subname_ = access_idents.at(0).access_name_);
      OX (extern_type_info->flag_ = ObParamExternType::SP_EXTERN_PKG);
    } else {
      OX (extern_type_info->flag_ = ObParamExternType::SP_EXTERN_UDT);
    }
  } else {
    OX (extern_type_info->flag_ = ObParamExternType::SP_EXTERN_UDT);
  }
  if (OB_SUCC(ret) && OB_INVALID_ID == extern_type_info->type_owner_) {
    OZ (session_info.get_database_id(extern_type_info->type_owner_));
  }
  OX (extern_type_info->type_name_ = access_idents.at(access_idents.count() - 1).access_name_);
  CANCLE_LOG_CHECK_MODE();
  return ret;
}

int ObPLResolver::resolve_extern_type_info(bool is_row_type,
                                           ObSchemaGetterGuard &schema_guard,
                                           const ObSQLSessionInfo &session_info,
                                           const ObIArray<ObObjAccessIdent> &access_idents,
                                           ObPLExternTypeInfo *extern_type_info)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(extern_type_info));
  if (OB_FAIL(ret)) {
  } else if (is_row_type) { // must be table%rowtype or dbname.table%rowtype
    CK (access_idents.count() <= 2 && access_idents.count() >= 1);
    if (OB_FAIL(ret)) {
    } else if (2 == access_idents.count()) {
      OZ (schema_guard.get_database_id(session_info.get_effective_tenant_id(),
        access_idents.at(0).access_name_, extern_type_info->type_owner_));
    } else {
      OZ (session_info.get_database_id(extern_type_info->type_owner_));
    }
    OX (extern_type_info->type_name_ = access_idents.at(access_idents.count() - 1).access_name_);
    OX (extern_type_info->flag_ = ObParamExternType::SP_EXTERN_TAB);
  } else { // dbname.table.col%type or table.col%type, dbname.pack.var%type, pack.var%type
    CK (access_idents.count() <= 3 && access_idents.count() >= 2);
    if (OB_FAIL(ret)) {
    } else if (3 == access_idents.count()) {
      OZ (schema_guard.get_database_id(session_info.get_effective_tenant_id(),
        access_idents.at(0).access_name_, extern_type_info->type_owner_));
    } else {
      OZ (session_info.get_database_id(extern_type_info->type_owner_));
    }
    OX (extern_type_info->type_name_ = access_idents.at(access_idents.count() - 1).access_name_);
    OX (extern_type_info->type_subname_ = access_idents.at(access_idents.count() - 2).access_name_);
    OX (extern_type_info->flag_ = ObParamExternType::SP_EXTERN_PKGVAR_OR_TABCOL);
  }
  return ret;
}

int ObPLResolver::resolve_extern_type_info(ObSchemaGetterGuard &guard,
                                           const ObIArray<ObObjAccessIdx> &access_idxs,
                                           ObPLExternTypeInfo *extern_type_info)
{
  int ret = OB_SUCCESS;
  CK (ObObjAccessIdx::is_table_column(access_idxs)
      || ObObjAccessIdx::is_package_variable(access_idxs)
      || ObObjAccessIdx::is_table(access_idxs)
      || ObObjAccessIdx::is_local_variable(access_idxs)
      || ObObjAccessIdx::is_external_type(access_idxs));
  if (OB_ISNULL(extern_type_info)) {
    // do nothing ...
  } else if (ObObjAccessIdx::is_table_column(access_idxs)) {
    CK (2 == access_idxs.count() || 3 == access_idxs.count());
    OX (extern_type_info->type_name_ = access_idxs.at(access_idxs.count() - 1).var_name_);
    OX (extern_type_info->type_subname_ = access_idxs.at(access_idxs.count() - 2).var_name_);
    OX (extern_type_info->flag_ = ObParamExternType::SP_EXTERN_TAB_COL);
    if (OB_FAIL(ret)) {
    } else if (3 == access_idxs.count()) {
      extern_type_info->type_owner_ = access_idxs.at(0).var_index_;
    } else {
      const ObTableSchema *table = NULL;
      const uint64_t tenant_id = resolve_ctx_.session_info_.get_effective_tenant_id();
      OZ (guard.get_table_schema(tenant_id, access_idxs.at(0).var_index_, table));
      CK (OB_NOT_NULL(table));
      OX (extern_type_info->type_owner_ = table->get_database_id());
    }
    OZ (fill_schema_obj_version(guard,
                                ObParamExternType::SP_EXTERN_TAB_COL,
                                access_idxs.at(access_idxs.count() - 2).var_index_,
                                *extern_type_info));
  } else if (ObObjAccessIdx::is_package_variable(access_idxs)) {
    ObObjAccessIdx::AccessType type = ObObjAccessIdx::IS_INVALID;
    uint64_t package_id = OB_INVALID_ID;
    CK (access_idxs.count() <= 3);
    OX (extern_type_info->flag_ = ObParamExternType::SP_EXTERN_PKG_VAR);
    OX (extern_type_info->type_name_ = access_idxs.at(access_idxs.count() - 1).var_name_);
    OX (access_idxs.count() > 1 ?
      extern_type_info->type_subname_ = access_idxs.at(access_idxs.count() - 2).var_name_
      // 这里处理特殊情况，当前ns为package，变量也是pacakge local var的情况下，ObParamExternType被强制修改为
      // SP_EXTERN_PKG_VAR(ns.resolve_symbol)，但是access_idxs只有一个，因为没有使用.这种feild access，
      // 因此也需要赋值pacakge name, type_owner_则会在函数底部赋值为当前的database_id.
      : extern_type_info->type_subname_ = current_block_->get_namespace().get_package_name());
    if (OB_FAIL(ret)) {
    } else if (3 == access_idxs.count()) {
      if (OB_SYS_TENANT_ID == get_tenant_id_by_object_id(access_idxs.at(1).var_index_)) {
        extern_type_info->type_owner_ = OB_SYS_DATABASE_ID;
      } else {
        extern_type_info->type_owner_ = access_idxs.at(0).var_index_;
      }
      OX (package_id = access_idxs.at(1).var_index_);
      OX (type = access_idxs.at(1).access_type_);
    } else if (2 == access_idxs.count()) {
      if (OB_SYS_TENANT_ID == get_tenant_id_by_object_id(access_idxs.at(0).var_index_)) { // 系统包中的Var
        extern_type_info->type_owner_ = OB_SYS_DATABASE_ID;
      } else {
        OZ(resolve_ctx_.session_info_.get_database_id(extern_type_info->type_owner_));
      }
      OX (package_id = access_idxs.at(0).var_index_);
      OX (type = access_idxs.at(0).access_type_);
    } else {
      OZ(resolve_ctx_.session_info_.get_database_id(extern_type_info->type_owner_));
      OX (package_id = current_block_->get_namespace().get_package_id());
      OX (type = ObObjAccessIdx::IS_PKG_NS);
    }
    if (ObObjAccessIdx::IS_LABEL_NS == type) {
      // do nothing
    } else {
      OZ (fill_schema_obj_version(guard,
                                  ObParamExternType::SP_EXTERN_PKG_VAR,
                                  package_id,
                                  *extern_type_info));
    }
  } else if (ObObjAccessIdx::is_table(access_idxs)) {
    CK (1 == access_idxs.count() || 2 == access_idxs.count());
    OX (extern_type_info->flag_ = ObParamExternType::SP_EXTERN_TAB);
    OX (extern_type_info->type_name_ = access_idxs.at(access_idxs.count() - 1).var_name_);
    if (OB_FAIL(ret)) {
    } else if (2 == access_idxs.count()) {
      extern_type_info->type_owner_ = access_idxs.at(0).var_index_;
    } else {
      const ObTableSchema *table = NULL;
      const uint64_t tenant_id = resolve_ctx_.session_info_.get_effective_tenant_id();
      OZ (guard.get_table_schema(tenant_id, access_idxs.at(0).var_index_, table));
      CK (OB_NOT_NULL(table));
      OX (extern_type_info->type_owner_ = table->get_database_id());
    }
    OZ (fill_schema_obj_version(guard,
                                ObParamExternType::SP_EXTERN_TAB,
                                access_idxs.at(access_idxs.count() - 1).var_index_,
                                *extern_type_info));
  } else if (ObObjAccessIdx::is_local_variable(access_idxs)) {
    OX (extern_type_info->flag_ = ObParamExternType::SP_EXTERN_LOCAL_VAR);
  } else if (ObObjAccessIdx::is_pkg_type(access_idxs)) {
    CK (access_idxs.count() <= 3);
    OX (extern_type_info->flag_ = ObParamExternType::SP_EXTERN_PKG);
    OX (extern_type_info->type_name_ = access_idxs.at(access_idxs.count() - 1).var_name_);
    if (OB_FAIL(ret)) {
    } else if (access_idxs.count() > 1) { // db.pkg.type or pkg.type
      extern_type_info->type_subname_ = access_idxs.at(access_idxs.count() - 2).var_name_;
      if (3 == access_idxs.count()) {
        if (OB_SYS_TENANT_ID == get_tenant_id_by_object_id(access_idxs.at(1).var_index_)) {
          extern_type_info->type_owner_ = OB_SYS_DATABASE_ID;
        } else {
          extern_type_info->type_owner_ = access_idxs.at(0).var_index_;
        }
      } else if (OB_SYS_TENANT_ID == get_tenant_id_by_object_id(access_idxs.at(0).var_index_)) {
        extern_type_info->type_owner_ = OB_SYS_DATABASE_ID;
      }
    } else if (extract_package_id(access_idxs.at(0).var_index_)
               != current_block_->get_namespace().get_package_id()) {
      // type (current package type or standard type)
      const ObPackageInfo *package_info = NULL;
      const uint64_t package_id = extract_package_id(access_idxs.at(0).var_index_);
      const uint64_t tenant_id = get_tenant_id_by_object_id(package_id);
      CK (1 == access_idxs.count());
      OZ (guard.get_package_info(tenant_id, package_id, package_info));
      CK (OB_NOT_NULL(package_info));
      OX (extern_type_info->type_subname_ = package_info->get_package_name());
      OX (extern_type_info->type_owner_ = package_info->get_database_id());
    } else {
      extern_type_info->type_subname_ = current_block_->get_namespace().get_package_name();
    }
    OZ (fill_schema_obj_version(guard,
                          ObParamExternType::SP_EXTERN_PKG,
                          extract_package_id(access_idxs.at(access_idxs.count() - 1).var_index_),
                          *extern_type_info));
  } else if (ObObjAccessIdx::is_udt_type(access_idxs)) {
  CK (access_idxs.count() <= 2);
    OX (extern_type_info->flag_ = ObParamExternType::SP_EXTERN_UDT);
    OX (extern_type_info->type_name_ = access_idxs.at(access_idxs.count() - 1).var_name_);
    if (OB_FAIL(ret)) {
    } else if (2 == access_idxs.count()) {
      if (OB_SYS_TENANT_ID == get_tenant_id_by_object_id(access_idxs.at(1).var_index_)) {
        extern_type_info->type_owner_ = OB_SYS_DATABASE_ID;
      } else {
        extern_type_info->type_owner_ = access_idxs.at(0).var_index_;
      }
    } else if (OB_SYS_TENANT_ID == get_tenant_id_by_object_id(access_idxs.at(0).var_index_)) {
      extern_type_info->type_owner_ = OB_SYS_DATABASE_ID;
    }
    OZ (fill_schema_obj_version(guard,
                                ObParamExternType::SP_EXTERN_UDT,
                                access_idxs.at(access_idxs.count() - 1).var_index_,
                                *extern_type_info));
  }
  if (OB_SUCC(ret)
      && OB_NOT_NULL(extern_type_info)
      && OB_INVALID_ID == extern_type_info->type_owner_) {
    OZ (resolve_ctx_.session_info_.get_database_id(extern_type_info->type_owner_));
  }
  return ret;
}

int ObPLResolver::fill_schema_obj_version(ObSchemaGetterGuard &guard,
                                          ObParamExternType type,
                                          uint64_t obj_id,
                                          ObPLExternTypeInfo &extern_type_info)
{
  int ret = OB_SUCCESS;

#define FILL(class, func, type) \
do { \
  const class *info = NULL; \
  OZ (guard.func(tenant_id, obj_id, info)); \
  CK (OB_NOT_NULL(info)); \
  OX (extern_type_info.obj_version_.object_id_ = obj_id); \
  OX (extern_type_info.obj_version_.version_ = info->get_schema_version()); \
  OX (extern_type_info.obj_version_.object_type_ = type); \
} while (0)

  switch (type) {
    case ObParamExternType::SP_EXTERN_TAB:
    case ObParamExternType::SP_EXTERN_TAB_COL: {
      const uint64_t tenant_id = resolve_ctx_.session_info_.get_effective_tenant_id();
      FILL(ObTableSchema, get_table_schema, DEPENDENCY_TABLE);
    } break;
    case ObParamExternType::SP_EXTERN_PKG:
    case ObParamExternType::SP_EXTERN_PKG_VAR: {
      const uint64_t tenant_id = get_tenant_id_by_object_id(obj_id);
      CK (OB_NOT_NULL(current_block_));
      if (OB_SUCC(ret)) {
        if (OB_INVALID_ID == obj_id
              || obj_id == current_block_->get_namespace().get_package_id()) {
          // same package, do nothing ...
        } else {
          FILL(ObPackageInfo, get_package_info, DEPENDENCY_PACKAGE);
        }
      }
    } break;
    case ObParamExternType::SP_EXTERN_UDT: {
      const uint64_t tenant_id = get_tenant_id_by_object_id(obj_id);
      FILL(ObUDTTypeInfo, get_udt_info, DEPENDENCY_TYPE);
    } break;
    default: {
      // do nothing ...
    }
  }
#undef FILL
  return ret;
}

bool ObPLResolver::is_data_type_name(const ObString &ident_name)
{
  bool is_same = false;
  if (0 == ident_name.case_compare("NUMERIC") ||
      0 == ident_name.case_compare("NUMBER") ||
      0 == ident_name.case_compare("FLOAT") ||
      0 == ident_name.case_compare("REAL") ||
      0 == ident_name.case_compare("TIMESTAMP") ||
      0 == ident_name.case_compare("SMALLINT") ||
      0 == ident_name.case_compare("INTEGER") ||
      0 == ident_name.case_compare("DECIMAL") ||
      0 == ident_name.case_compare("PLS_INTEGER") ||
      0 == ident_name.case_compare("BINARY_INTEGER") ||
      0 == ident_name.case_compare("NATURAL") ||
      0 == ident_name.case_compare("NATURALN") ||
      0 == ident_name.case_compare("POSITIVE") ||
      0 == ident_name.case_compare("POSITIVEN") ||
      0 == ident_name.case_compare("SIGNTYPE") ||
      0 == ident_name.case_compare("SIMPLE_INTEGER") ||
      0 == ident_name.case_compare("BOOLEAN") ||
      0 == ident_name.case_compare("BINARY_DOUBLE") ||
      0 == ident_name.case_compare("BINARY_FLOAT") ||
      0 == ident_name.case_compare("SIMPLE_DOUBLE") ||
      0 == ident_name.case_compare("SIMPLE_FLOAT") ||
      0 == ident_name.case_compare("DATE") ||
      0 == ident_name.case_compare("RAW") ||
      0 == ident_name.case_compare("NCHAR") ||
      0 == ident_name.case_compare("VARCHAR") ||
      0 == ident_name.case_compare("VARCHAR2") ||
      0 == ident_name.case_compare("NVARCHAR2") ||
      0 == ident_name.case_compare("CLOB") ||
      0 == ident_name.case_compare("UROWID") ||
      0 == ident_name.case_compare("ROWID")) {
    is_same = true;
  }
  return is_same;
}

int ObPLResolver::resolve_sp_row_type(const ParseNode *sp_data_type_node,
                                      ObPLCompileUnitAST &func,
                                      ObPLDataType &pl_type,
                                      ObPLExternTypeInfo *extern_type_info,
                                      bool with_rowid)
{
  int ret = OB_SUCCESS;

  ObArray<ObObjAccessIdent> obj_access_idents;
  SET_LOG_CHECK_MODE();
  CK (OB_NOT_NULL(sp_data_type_node),
      OB_LIKELY(T_SP_TYPE == sp_data_type_node->type_
                || T_SP_ROWTYPE == sp_data_type_node->type_),
      OB_LIKELY(1 == sp_data_type_node->num_child_),
      OB_NOT_NULL(sp_data_type_node->children_[0]),
      OB_LIKELY(T_SP_OBJ_ACCESS_REF == sp_data_type_node->children_[0]->type_));

  OZ (resolve_obj_access_idents(*(sp_data_type_node->children_[0]), obj_access_idents, func));
  CK (obj_access_idents.count() > 0);
  OX (obj_access_idents.at(obj_access_idents.count() - 1).has_brackets_ = false);
  if (OB_SUCC(ret)) {
    ObArray<ObObjAccessIdx> access_idxs;
    for (int64_t i = 0; OB_SUCC(ret) && i < obj_access_idents.count(); ++i) {
      OZ (resolve_access_ident(obj_access_idents.at(i),
                               current_block_->get_namespace(),
                               expr_factory_,
                               &resolve_ctx_.session_info_,
                               access_idxs,
                               func,
                               false,
                               true));
      if (OB_ERR_SP_UNDECLARED_VAR == ret && is_data_type_name(obj_access_idents.at(i).access_name_)) {
        ret  = OB_ERR_TYPE_DECL_ILLEGAL;
        LOG_USER_ERROR(OB_ERR_TYPE_DECL_ILLEGAL, obj_access_idents.at(i).access_name_.length(),
                                                 obj_access_idents.at(i).access_name_.ptr());
      }
    }
    // 创建package的时候可能依赖的外部类型还没有创建, 记录下外部类型的name信息, 执行的时候重新解析
    if (OB_NOT_NULL(extern_type_info)
        && is_object_not_exist_error(ret)
        && obj_access_idents.count() >= 1) {
      LOG_USER_WARN(OB_ERR_SP_UNDECLARED_TYPE,
                    obj_access_idents.at(obj_access_idents.count() - 1).access_name_.length(),
                    obj_access_idents.at(obj_access_idents.count() - 1).access_name_.ptr());
      record_error_line(sp_data_type_node, resolve_ctx_.session_info_);
      ObPL::insert_error_msg(ret);
      ret = OB_SUCCESS;
      CK (T_SP_ROWTYPE == sp_data_type_node->type_ || T_SP_TYPE == sp_data_type_node->type_);
      OZ (resolve_extern_type_info(T_SP_ROWTYPE == sp_data_type_node->type_,
                                   resolve_ctx_.schema_guard_,
                                   resolve_ctx_.session_info_,
                                   obj_access_idents,
                                   extern_type_info));
    } else if (OB_FAIL(ret)) {
      // do nothing ...
    } else {
      int64_t idx_cnt = access_idxs.count();
      CK (OB_LIKELY(idx_cnt != 0));
      if (T_SP_TYPE == sp_data_type_node->type_) {
        if (ObObjAccessIdx::is_table_column(access_idxs)
            || ObObjAccessIdx::is_local_variable(access_idxs)
            || ObObjAccessIdx::is_package_variable(access_idxs)
            || ObObjAccessIdx::is_subprogram_variable(access_idxs)) {
          OZ (pl_type.deep_copy(resolve_ctx_.allocator_,
                                ObObjAccessIdx::get_final_type(access_idxs)));
          OX (pl_type.set_type_from_orgin(pl_type.get_type_from()));
          OX (pl_type.set_type_from(PL_TYPE_ATTR_TYPE));
          OZ (resolve_extern_type_info(resolve_ctx_.schema_guard_, access_idxs, extern_type_info));
        } else {
          ret = OB_ERR_TYPE_DECL_ILLEGAL;
          LOG_WARN("PLS-00206: %TYPE must be applied to a variable, column, field or attribute",
                   K(ret), K(access_idxs));
        }
      } else {
        if (ObObjAccessIdx::is_table(access_idxs)) {
          ObSQLSessionInfo &session_info = resolve_ctx_.session_info_;
          ObSchemaGetterGuard &schema_guard = resolve_ctx_.schema_guard_;
          uint64_t db_id = OB_INVALID_ID;
          const ObTableSchema* table_schema = NULL;
          ObRecordType *record_type = NULL;
          ObSEArray<ObDataType, 8> types;
          const uint64_t tenant_id = session_info.get_effective_tenant_id();
          OZ (session_info.get_database_id(db_id));
          OZ (schema_guard.get_table_schema(tenant_id, access_idxs.at(idx_cnt - 1).var_index_, table_schema));
          CK (OB_NOT_NULL(table_schema));
          if (OB_FAIL(ret)) {
          } else if (with_rowid) {
            // with_rowid的情况只可能是oracle模式下的trigger
            // 将trigger package中routine的参数类型设置为 PL_TYPE_PACKAGE, 并加入到包头的type_table_中
            ObSqlString record_name;
            ObString record_name_str;
            char* name_buf = NULL;
            bool is_dup = false;
            const void *dup_type = NULL;
            const ObPLBlockNS *pre_ns = NULL;
            CK (OB_NOT_NULL(current_block_));
            CK (OB_NOT_NULL(pre_ns = current_block_->get_namespace().get_pre_ns()));
            CK (access_idxs.count() > 0);
            OZ (record_name.append_fmt(
                "__trigger_param_type_%.*s",
                access_idxs.at(0).var_name_.length(), access_idxs.at(0).var_name_.ptr()));
            if (access_idxs.count() >= 2) {
              OZ (record_name.append_fmt("_%.*s",
                access_idxs.at(1).var_name_.length(), access_idxs.at(1).var_name_.ptr()));
            }
            if (OB_FAIL(ret)) {
            } else if (OB_ISNULL(name_buf = static_cast<char*>(resolve_ctx_.allocator_.alloc(record_name.length() + 1)))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("failed to allocate record name", K(ret));
            } else {
              record_name.to_string(name_buf, record_name.length() + 1);
              record_name_str.assign_ptr(name_buf, record_name.length());
              if (OB_FAIL(pre_ns->check_dup_type(record_name_str, is_dup, dup_type))) {
                LOG_WARN("check dup type failed", K(record_name_str), K(is_dup), K(ret));
              } else if (is_dup) {
                if (OB_ISNULL(dup_type)) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("dup type is NULL", K(ret));
                } else if (OB_SUCC(ret) && is_dup) {
                  record_type = static_cast<ObRecordType *>(const_cast<void *>(dup_type));
                  OV (OB_NOT_NULL(record_type));
                  OX (pl_type.set_user_type_id(record_type->get_type(), record_type->get_user_type_id()));
                  OZ (func.add_dependency_object(ObSchemaObjVersion(table_schema->get_table_id(),
                      table_schema->get_schema_version(), ObDependencyTableType::DEPENDENCY_TABLE)));
                }
              } else {
                OZ (build_record_type_by_schema(resolve_ctx_, table_schema, record_type, with_rowid));
                CK (OB_NOT_NULL(record_type));
                OX (record_type->set_name(record_name_str));
                OZ (pre_ns->expand_data_type(record_type, types));
                OZ (const_cast<ObPLBlockNS *>(pre_ns)->add_type(record_type));
                OX (record_type->set_type_from(PL_TYPE_PACKAGE));
                OX (pl_type.set_user_type_id(record_type->get_type(), record_type->get_user_type_id()));
                OZ (pl_type.get_all_depended_user_type(resolve_ctx_, *pre_ns));
                OZ (func.add_dependency_object(ObSchemaObjVersion(table_schema->get_table_id(),
                        table_schema->get_schema_version(), ObDependencyTableType::DEPENDENCY_TABLE)));
              }
            }
            OX (pl_type.set_type_from(PL_TYPE_PACKAGE));
            OX (pl_type.set_type_from_orgin(PL_TYPE_PACKAGE));
          } else {
            OZ (build_record_type_by_schema(resolve_ctx_, table_schema, record_type, with_rowid));
            CK (OB_NOT_NULL(record_type));
            CK (OB_NOT_NULL(current_block_));
            CK (OB_NOT_NULL(current_block_->get_namespace().get_type_table()));
            OZ (current_block_->get_namespace().expand_data_type(record_type, types));
            OZ (current_block_->get_namespace().get_type_table()->add_external_type(record_type));
            OX (pl_type.set_user_type_id(record_type->get_type(), record_type->get_user_type_id()));
            OX (pl_type.set_type_from_orgin(pl_type.get_type_from()));
            OX (pl_type.set_type_from(PL_TYPE_ATTR_ROWTYPE));
            OZ (pl_type.get_all_depended_user_type(resolve_ctx_, current_block_->get_namespace()));
            OZ (resolve_extern_type_info(resolve_ctx_.schema_guard_, access_idxs, extern_type_info));
            OZ (func.add_dependency_object(ObSchemaObjVersion(table_schema->get_table_id(),
                      table_schema->get_schema_version(), ObDependencyTableType::DEPENDENCY_TABLE)));
          }
        } else if (ObObjAccessIdx::is_local_cursor_variable(access_idxs)
                   || ObObjAccessIdx::is_local_refcursor_variable(access_idxs)
                   || ObObjAccessIdx::is_subprogram_cursor_variable(access_idxs)
                   || ObObjAccessIdx::is_package_cursor_variable(access_idxs)) {
          const ObUserDefinedType* user_type = NULL;
          const ObRecordType *record_type = NULL;
          OX (pl_type.set_user_type_id(
            PL_RECORD_TYPE, ObObjAccessIdx::get_final_type(access_idxs).get_user_type_id()));
          CK (OB_NOT_NULL(current_block_));
          OZ (current_block_->
            get_namespace().get_pl_data_type_by_id(pl_type.get_user_type_id(), user_type));
          CK (OB_NOT_NULL(user_type));
          if (OB_SUCC(ret)) {
            if (user_type->is_cursor_type() || user_type->is_sys_refcursor_type()) {
              const ObRefCursorType *type = static_cast<const ObRefCursorType*>(user_type);
              CK (OB_NOT_NULL(type));
              OX (user_type = NULL);
              OZ (current_block_->get_namespace().get_pl_data_type_by_id(type->get_return_type_id(), user_type));
              CK (OB_NOT_NULL(user_type));
              OX (pl_type.set_user_type_id(PL_RECORD_TYPE, type->get_return_type_id()));
            }
          }
          CK (user_type->is_record_type());
          CK (OB_NOT_NULL(record_type = static_cast<const ObRecordType*>(user_type)));
          CK (OB_NOT_NULL(current_block_->get_namespace().get_type_table()));
          OX ((const_cast<ObRecordType*>(record_type))->set_type_from(PL_TYPE_ATTR_ROWTYPE));
          OZ (current_block_->get_namespace().get_type_table()->add_external_type(record_type));
          OX (pl_type.set_type_from_orgin(pl_type.get_type_from()));
          OX (pl_type.set_type_from(PL_TYPE_ATTR_ROWTYPE));
          OZ (pl_type.get_all_depended_user_type(resolve_ctx_, current_block_->get_namespace()));
          if (!ObObjAccessIdx::is_subprogram_cursor_variable(access_idxs)) {
            OZ (resolve_extern_type_info(resolve_ctx_.schema_guard_, access_idxs, extern_type_info));
          }
        } else {
          ret = OB_ERR_WRONG_ROWTYPE;
          LOG_USER_ERROR(OB_ERR_WRONG_ROWTYPE,
                         static_cast<int>(sp_data_type_node->str_len_), sp_data_type_node->str_value_);
          LOG_WARN("PLS-00310: with %ROWTYPE attribute, ident must name a table, cursor or cursor-variable",
                   K(ret), K(access_idxs));
        }
      }
    }
  }
  CANCLE_LOG_CHECK_MODE();
  return ret;
}

// 注意: 该函数参考ObResolverUtils::resolve_data_type进行实现, 调整类型精度信息到默认值
int ObPLResolver::adjust_routine_param_type(ObPLDataType &type)
{
  int ret = OB_SUCCESS;
  if (type.is_obj_type() && !type.is_pl_integer_type() && OB_NOT_NULL(type.get_data_type())) {
    ObDataType data_type = *(type.get_data_type());
    const ObAccuracy &default_accuracy
      = ObAccuracy::DDL_DEFAULT_ACCURACY2[true][data_type.get_obj_type()];
    switch (data_type.get_type_class()) {
      case ObIntTC:
      case ObUIntTC: {
        data_type.set_precision(default_accuracy.get_precision());
        data_type.set_scale(0);
      } break;
      case ObFloatTC: {
        data_type.set_precision(126);
        data_type.set_scale(-1);
      } break;
      case ObDoubleTC: {
        data_type.set_precision(-1);
        data_type.set_scale(-85);
      } break;
      case ObNumberTC: {
        data_type.set_precision(-1);
        data_type.set_scale(-85);
      } break;
      case ObOTimestampTC: {
        data_type.set_precision(
          static_cast<int16_t>(default_accuracy.get_precision() + default_accuracy.get_scale()));
        data_type.set_scale(default_accuracy.get_scale());
      } break;
      case ObDateTimeTC: {
        data_type.set_precision(static_cast<int16_t>(default_accuracy.get_precision()));
        data_type.set_scale(0);
      } break;
      case ObDateTC: {
        data_type.set_precision(default_accuracy.get_precision());
        data_type.set_scale(default_accuracy.get_scale());
      } break;
      case ObTimeTC: {
        data_type.set_precision(default_accuracy.get_precision());
        data_type.set_scale(default_accuracy.get_scale());
      } break;
      case ObYearTC: {
        data_type.set_precision(default_accuracy.get_precision());
        data_type.set_scale(default_accuracy.get_scale());
      } break;
      case ObStringTC: {
        data_type.set_length(-1);
      } break;
      case ObRawTC: {
        data_type.set_length(2000);
      } break;
      case ObLobTC:
      case ObTextTC:
      case ObJsonTC:
      case ObGeometryTC: {
        data_type.set_length(-1);
        data_type.set_scale(default_accuracy.get_scale());
      } break;
      case ObBitTC: {
        data_type.set_precision(default_accuracy.get_precision());
        data_type.set_scale(default_accuracy.get_scale());
      } break;
      case ObIntervalTC: {
        if (data_type.get_meta_type().is_interval_ym()) {
          ObScale scale = ObIntervalScaleUtil::interval_ym_scale_to_ob_scale(9);
          data_type.set_scale(scale);
        } else {
          ObScale scale = ObIntervalScaleUtil::interval_ds_scale_to_ob_scale(9, 9);
          data_type.set_scale(scale);
        }
      } break;
      case ObRowIDTC: {
        if (ob_is_urowid(data_type.get_obj_type())) {
          data_type.set_length(4000);
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("only support urowid type for now", K(ret), K(data_type));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "non-urowid type");
        }
      } break;
      case ObExtendTC:
      default: {
        // do nothing ...
      } break;
    }
    OX (type.set_data_type(data_type));
  }
  return ret;
}

int ObPLResolver::resolve_dblink_type(const ParseNode *node,
                                      ObPLCompileUnitAST &func,
                                      ObPLDataType &pl_type)
{
  int ret = OB_SUCCESS;
  ParseNode *access_node = NULL;
  ObString db_name;
  ObString pkg_name;
  ObString udt_name;
  ObString dblink_name;
  const ObUserDefinedType *udt = NULL;
  CK (OB_LIKELY(2 == node->num_child_));
  CK (OB_LIKELY(OB_NOT_NULL(node->children_[0])) && OB_LIKELY(OB_NOT_NULL(node->children_[1])));
  CK (OB_LIKELY(T_SP_OBJ_ACCESS_REF == node->children_[0]->type_));
  CK (OB_LIKELY(T_USER_VARIABLE_IDENTIFIER == node->children_[1]->type_));
  CK (2 == node->children_[0]->num_child_);
  CK (OB_NOT_NULL(access_node = node->children_[0]->children_[0]));
  CK (T_SP_ACCESS_NAME == access_node->type_);
  // access_node must be a package type.
  CK (3 == access_node->num_child_);
  CK (OB_NOT_NULL(access_node->children_[1]) && OB_NOT_NULL(access_node->children_[2]))
  if (OB_SUCC(ret)) {
    dblink_name.assign_ptr(node->children_[1]->str_value_,
                           static_cast<int32_t>(node->children_[1]->str_len_));
    pkg_name.assign_ptr(access_node->children_[1]->str_value_,
                        static_cast<int32_t>(access_node->children_[1]->str_len_));
    udt_name.assign_ptr(access_node->children_[2]->str_value_,
                        static_cast<int32_t>(access_node->children_[2]->str_len_));
    if (OB_NOT_NULL(access_node->children_[0])) {
      db_name.assign_ptr(access_node->children_[0]->str_value_,
                         static_cast<int32_t>(access_node->children_[0]->str_len_));
    }
  }

  OZ (resolve_dblink_type(dblink_name, db_name, pkg_name, udt_name, func, pl_type));
  return ret;
}

// for example :  'insert into tbl values (:NEW.rowid);'
// this stmt may inside a trigger mock function body, :NEW is a type like tbl%ROWTYPE
// in this case, we have to including the rowid psedocolumn in the result, regardless of
// whether rowid is actual accessed or not.
int ObPLResolver::resolve_sp_data_type(const ParseNode *sp_data_type_node,
                                       const ObString &ident_name,
                                       ObPLCompileUnitAST &func,
                                       ObPLDataType &data_type,
                                       ObPLExternTypeInfo *extern_type_info,
                                       bool with_rowid,
                                       bool is_for_param_type)
{
  int ret = OB_SUCCESS;

  SET_LOG_CHECK_MODE();

  bool need_adjust_type = false;
  CK (OB_NOT_NULL(sp_data_type_node));
  CK (OB_NOT_NULL(current_block_));
  if (OB_SUCC(ret)) {
    if (T_SP_OBJ_ACCESS_REF == sp_data_type_node->type_) {
      OZ (resolve_sp_composite_type(sp_data_type_node, func, data_type, extern_type_info));
    } else if (T_SP_TYPE == sp_data_type_node->type_
               || T_SP_ROWTYPE == sp_data_type_node->type_) {
      OZ (resolve_sp_row_type(sp_data_type_node, func, data_type, extern_type_info, with_rowid));
    } else if (T_SP_DBLINK_TYPE == sp_data_type_node->type_) {
      OZ (resolve_dblink_type(sp_data_type_node, func, data_type));
    } else {
      OZ (resolve_sp_scalar_type(resolve_ctx_.allocator_,
                                 sp_data_type_node,
                                 ident_name,
                                 resolve_ctx_.session_info_,
                                 data_type,
                                 is_for_param_type,
                                 get_current_namespace().get_package_id()));
    }
  }

  OX (need_adjust_type = data_type.is_subtype() || data_type.is_type_type());
#ifdef OB_BUILD_ORACLE_PL
  OZ (current_block_->get_namespace().get_subtype_actually_basetype(data_type));
#endif
  /*!
   * for number(38,0), always adjust presicion and scale. example:
   * declare
   *   subtype st is number(38,0);
   *   function func(x st) return st is begin dbms_output.put_line(x); return x; end;
   * begin
   *   dbms_output.put_line(func(.66));
   * end;
   * output is 0.66, 0.66
   */
  if (OB_SUCC(ret)
      && OB_NOT_NULL(extern_type_info)
      && ObNumberType == data_type.get_obj_type()
      && 38 == data_type.get_data_type()->get_precision()
      && 0 == data_type.get_data_type()->get_scale()) {
    OZ (adjust_routine_param_type(data_type));
  }
  // datatype is parameter type if extern_type_info is not null, need adjust presicion and scale etc...
  if (OB_SUCC(ret) && OB_NOT_NULL(extern_type_info) && need_adjust_type) {
    // it`s return type when ident_name is empty.
    // if return is numeric type, do not adjust it. (Compatible Oracle)
    if (ident_name.empty() && ob_is_numeric_type(data_type.get_obj_type())) {
      // do nothing ...
    } else {
      OZ (adjust_routine_param_type(data_type));
    }
  }

  CANCLE_LOG_CHECK_MODE();

  return ret;
}

#ifdef OB_BUILD_ORACLE_PL
int ObPLResolver::resolve_declare_collection_type(const ParseNode *type_node,
                                                  ObPLDeclareUserTypeStmt *stmt,
                                                  ObPLCompileUnitAST &unit_ast)
{
  UNUSED(unit_ast);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(type_node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("type node is null");
  } else if (OB_UNLIKELY(type_node->num_child_ != 2)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type node is invalid", K(type_node->num_child_));
  } else {
    const ParseNode *name_node = type_node->children_[0];
    const ParseNode *coll_type_def = type_node->children_[1];
    ObString table_name;
    ObPLDataType element_type;
    ObCollectionType *collection_type = NULL;
    void *ptr = NULL;
    if (OB_ISNULL(name_node) || OB_ISNULL(coll_type_def)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("parse node is invalid", K(name_node), K(coll_type_def));
    } else {
      table_name.assign_ptr(name_node->str_value_, static_cast<int32_t>(name_node->str_len_));
      const ParseNode *elem_type_node = coll_type_def->children_[0];
      if (OB_FAIL(resolve_sp_data_type(elem_type_node, table_name, unit_ast, element_type))) {
        LOG_WARN("resolve sp data type failed", K(ret));
      } else if (element_type.is_cursor_type()) {
        ret = OB_ERR_INDEX_TABLE_OF_CURSOR;
        LOG_WARN("Index Tables of Cursor Variables are disallowed", K(ret), K(element_type));
      }

#define ALLOC_COLLECTION_TYPE(TYPE) \
  do { \
     if (OB_SUCC(ret)) { \
       TYPE *table_type = NULL; \
       if (OB_ISNULL(ptr = resolve_ctx_.allocator_.alloc(sizeof(TYPE)))) { \
         ret = OB_ALLOCATE_MEMORY_FAILED; \
         LOG_WARN("no memory to allocate ObTableType", "size", sizeof(TYPE)); \
       } else { \
         table_type = new(ptr) TYPE(); \
         collection_type = table_type; \
       } \
     } \
  } while (0)

      switch (coll_type_def->type_) {
      case T_SP_NESTED_TABLE_TYPE: {

        ALLOC_COLLECTION_TYPE(ObNestedTableType);

      }
        break;
      case T_SP_ASSOC_ARRAY_TYPE: {

        ALLOC_COLLECTION_TYPE(ObAssocArrayType);

        if (OB_SUCC(ret)) {
          ObPLDataType index_type;
          const ParseNode *index_type_node = coll_type_def->children_[1];
          if (OB_FAIL(resolve_sp_data_type(index_type_node, table_name, unit_ast, index_type))) {
            LOG_WARN("resolve sp data type failed", K(ret));
          } else if (!index_type.is_pl_integer_type()
                     && index_type.get_obj_type() != ObVarcharType) {
              ret = OB_ERR_UNSUPPORTED_TABLE_INDEX_TYPE;
              LOG_WARN("PLS-00315: Implementation restriction: unsupported table index type",
                       K(index_type), K(ret));
          } else {
            static_cast<ObAssocArrayType*>(collection_type)->set_index_type(index_type);
          }
        }
      }
        break;
      case T_SP_VARRAY_TYPE: {

        ALLOC_COLLECTION_TYPE(ObVArrayType);

        if (OB_SUCC(ret)) {
          const ParseNode *size_node = coll_type_def->children_[1];
          CK (OB_NOT_NULL(size_node));
          CK (T_INT == size_node->type_);
          if (OB_SUCC(ret)) {
            if (size_node->value_ > 0) {
              static_cast<ObVArrayType*>(collection_type)->set_capacity(size_node->value_);
            } else {
              ret = OB_ERR_ARRAY_MUST_HAVE_POSITIVE_LIMIT;
              LOG_WARN("PLS-00537: A VARRAY must have a positive limit", K(size_node->value_), K(ret));
            }
          }
        }
      }
        break;
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("collection type is invalid", K_(coll_type_def->type));
      }
        break;
      }

      if (OB_SUCC(ret)) {
        if (element_type.is_not_null() && -1 == coll_type_def->value_) {
          ret = OB_ERR_SUBTYPE_NOTNULL_MISMATCH;
          LOG_WARN("PLS-00366: subtype of a not null type must also be not null", K(ret));
        } else {
          element_type.set_not_null(coll_type_def->value_ == 1 || element_type.get_not_null());
        }
      }

      if (OB_SUCC(ret)) {
        collection_type->set_element_type(element_type);
        collection_type->set_name(table_name);
        if (unit_ast.is_package()) {
          collection_type->set_type_from(PL_TYPE_PACKAGE);
        }
        if (OB_FAIL(current_block_->get_namespace().add_type(collection_type))) {
          LOG_WARN("failed to add table type to type table", K(*collection_type), K(ret));
        } else {
          if (!OB_ISNULL(stmt)) {
            stmt->set_user_type(collection_type);
          }
        }
      }
    }
  }
  return ret;
}

int ObPLResolver::check_collection_constructor(const ParseNode *node, const ObString &type_name, bool &is_constructor)
{
  int ret = OB_SUCCESS;
  is_constructor = false;
  if (OB_NOT_NULL(node)
      && T_OBJ_ACCESS_REF == node->type_
      && 2 == node->num_child_
      && OB_NOT_NULL(node->children_[0])
      && OB_ISNULL(node->children_[1])
      && T_FUN_SYS == node->children_[0]->type_
      && 1 == node->children_[0]->num_child_) {
    const ParseNode *name_node = node->children_[0]->children_[0];
    CK (OB_NOT_NULL(name_node));
    if (OB_SUCC(ret)) {
      ObString func_name(static_cast<int32_t>(name_node->str_len_), name_node->str_value_);
      is_constructor = ObCharset::case_insensitive_equal(func_name, type_name);
      if (is_constructor && node->children_[1] != NULL) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "Collection constructor with obj access");
      }
    }
  }
  return ret;
}
#endif

int ObPLResolver::resolve_declare_var(const ObStmtNodeTree *parse_tree, ObPLDeclareVarStmt *stmt, ObPLFunctionAST &func)
{
  int ret = OB_SUCCESS;
  ObPLCompileUnitAST &unit_ast = static_cast<ObPLCompileUnitAST&>(func);
  if (OB_FAIL(check_declare_order(PL_VAR))) {
    LOG_WARN("fail to check decalre order", K(ret));
  } else {
    ret = resolve_declare_var_comm(parse_tree, stmt, unit_ast);
  }
  return ret;
}

int ObPLResolver::resolve_declare_var(const ObStmtNodeTree *parse_tree, ObPLPackageAST &package_ast)
{
  int ret = OB_SUCCESS;
  ObPLCompileUnitAST &unit_ast = static_cast<ObPLCompileUnitAST&>(package_ast);
  ret = resolve_declare_var_comm(parse_tree, NULL, unit_ast);
  return ret;
}

int ObPLResolver::resolve_declare_var_comm(const ObStmtNodeTree *parse_tree,
                                           ObPLDeclareVarStmt *stmt,
                                           ObPLCompileUnitAST &unit_ast)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(parse_tree));
  CK (OB_LIKELY(3 == parse_tree->num_child_));
  CK (OB_NOT_NULL(current_block_));
  if (OB_SUCC(ret)) {
    bool not_null = parse_tree->int32_values_[0] == 1;
    bool constant = parse_tree->int32_values_[1] == 1;

    ObPLDataType data_type;
    ObString ident_name;
    ObRawExpr *default_expr = NULL;
    bool default_construct = false;
    const ObStmtNodeTree *name_node = parse_tree->children_[0];
    const ObStmtNodeTree *type_node = parse_tree->children_[1];
    const ObStmtNodeTree *default_node = parse_tree->children_[2];
    ObSEArray<ObString, 1> names;

    // resolve ident name first, for report error message.
    CK (OB_NOT_NULL(name_node));
    CK (name_node->num_child_ > 0);
    CK (OB_NOT_NULL(name_node->children_[0]));
    OZ (resolve_ident(name_node->children_[0], ident_name));

    // resolve variable type
    CK (OB_NOT_NULL(type_node));
    OZ (resolve_sp_data_type(type_node, ident_name, unit_ast, data_type));

    if (OB_SUCC(ret)) {
      if (data_type.is_not_null() && -1 == parse_tree->int32_values_[0]) {
        ret = OB_ERR_SUBTYPE_NOTNULL_MISMATCH;
        LOG_WARN("PLS-00366: subtype of a not null type must also be not null", K(ret));
      } else {
        OX (not_null = not_null || data_type.get_not_null());
      }
    }
    OX (data_type.set_not_null(not_null));

    if (OB_SUCC(ret) && data_type.is_cursor_type() && unit_ast.is_package()) {
      ret = OB_ERR_CURSOR_VAR_IN_PKG;
      LOG_WARN("PLS-00994: Cursor Variables cannot be declared as part of a package",
               K(ret), K(data_type));
    }

    // resolve defalut value
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(default_node)) {
        if (constant) {
          ret = OB_ERR_INIT_CONST_ILLEGAL;
          LOG_WARN("PLS-00322: Constant declarations should contain initialization assignments",
                   K(ret), K(constant), K(default_node));
          LOG_USER_ERROR(OB_ERR_INIT_CONST_ILLEGAL, ident_name.length(), ident_name.ptr());
        } else if (not_null) {
          ret = OB_ERR_INIT_NOTNULL_ILLEGAL;
          LOG_WARN("PLS-00218: a variable declared NOT NULL must have an initialization assignment",
                   K(ret), K(not_null), K(default_node));
        }
      }
      if (OB_NOT_NULL(default_node)) { // 默认值的not null检查在执行期做, 这里仅解析默认值表达式
        CK (OB_LIKELY(T_SP_DECL_DEFAULT == default_node->type_));
        CK (OB_NOT_NULL(default_node->children_[0]));

        if (OB_SUCC(ret) && not_null && T_NULL == default_node->children_[0]->type_) {
	        ret = OB_ERR_EXPRESSION_WRONG_TYPE;
          LOG_WARN("PLS-00382: expression is of wrong type", K(ret));
        }
        if (OB_SUCC(ret) && not_null && T_QUESTIONMARK == default_node->children_[0]->type_) {
          int64_t idx = default_node->children_[0]->value_;
          const ObPLVar* var = current_block_->get_symbol_table()->get_symbol(idx);
          if (NULL != var &&
              0 == var->get_name().case_compare(ObPLResolver::ANONYMOUS_ARG) &&
              NULL != var->get_pl_data_type().get_data_type() &&
              ObNullType == var->get_pl_data_type().get_data_type()->get_obj_type()) {
            ret = OB_ERR_EXPRESSION_WRONG_TYPE;
            LOG_WARN("PLS-00382: expression is of wrong type", K(ret));
          }
        }

        OZ (resolve_expr(default_node->children_[0], unit_ast, default_expr,
                         combine_line_and_col(default_node->children_[0]->stmt_loc_),
                         true, &data_type));
        if (OB_SUCC(ret)) {
          if (T_FUN_PL_COLLECTION_CONSTRUCT == default_expr->get_expr_type()
              && 0 == default_expr->get_param_count()) {
            if (OB_NOT_NULL(stmt)) {
              stmt->set_default(PL_CONSTRUCT_COLLECTION);
            }
            default_construct = true;
          } else {
            if (OB_NOT_NULL(stmt)) {
              stmt->set_default(unit_ast.get_expr_count() - 1);
            }
          }
        }
      } else if (OB_NOT_NULL(data_type.get_data_type())) { // 基础类型如果, 没有默认值, 设置为NULL
        OZ (ObRawExprUtils::build_null_expr(expr_factory_, default_expr));
        CK (OB_NOT_NULL(default_expr));
        OZ (ObRawExprUtils::build_column_conv_expr(&resolve_ctx_.session_info_,
                                                   expr_factory_,
                                                   data_type.get_data_type()->get_obj_type(),
                                                   data_type.get_data_type()->get_collation_type(),
                                                   data_type.get_data_type()->get_accuracy_value(),
                                                   true,
                                                   NULL, /*"db_name"."tb_name"."col_name"*/
                                                   &data_type.get_type_info(),
                                                   default_expr,
                                                   true /*is_in_pl*/));
        CK (OB_NOT_NULL(default_expr));
        OZ (formalize_expr(*default_expr));
        OZ (unit_ast.add_expr(default_expr));
        if (OB_SUCC(ret) && OB_NOT_NULL(stmt)) {
          stmt->set_default(unit_ast.get_expr_count() - 1);
        }
      } else {
        if (OB_SUCC(ret) && OB_NOT_NULL(stmt) && data_type.is_associative_array_type()) {
          stmt->set_default(PL_CONSTRUCT_COLLECTION);
        }
      }
    }

    if (OB_SUCC(ret)) {
      ObString name;
      CK (OB_NOT_NULL(name_node));
      for (int64_t i = 0; OB_SUCC(ret) && i < name_node->num_child_; ++i) {
        name.reset();
        if (OB_UNLIKELY(name_node->children_[i]->str_len_ > 
          (lib::is_oracle_mode() ? OB_MAX_PL_IDENT_LENGTH : OB_MAX_MYSQL_PL_IDENT_LENGTH))) {
          ret = OB_ERR_IDENTIFIER_TOO_LONG;
          LOG_USER_ERROR(OB_ERR_IDENTIFIER_TOO_LONG,
                         static_cast<int32_t>(name_node->children_[i]->str_len_),
                         name_node->children_[i]->str_value_);
          LOG_WARN("identifier too long", K(name_node->children_[i]->str_value_));
        }
        OZ (resolve_ident(name_node->children_[i], name));
        if (OB_SUCC(ret) && OB_NOT_NULL(stmt)) {
          OZ (stmt->add_index(unit_ast.get_symbol_table().get_count()));
        }
        OZ (current_block_->get_namespace().add_symbol(
              name, data_type, default_expr, constant, not_null, default_construct));
      }
    }
  }
  return ret;
}

int ObPLResolver::is_return_ref_cursor_type(const ObRawExpr *expr, bool &is_ref_cursor_type)
{
  int ret = OB_SUCCESS;
  is_ref_cursor_type = false;
  CK (OB_NOT_NULL(expr));
  if (OB_SUCC(ret)) {
    const ObPLDataType *src_type = NULL;
    ObPLDataType type_local;
    if (expr->is_const_raw_expr()) {
      const ObConstRawExpr *const_expr = static_cast<const ObConstRawExpr*>(expr);
      const ObPLSymbolTable* symbol_table = NULL;
      const ObPLVar* var = NULL;
      if (!const_expr->get_value().is_unknown()) {
        // ret = OB_ERR_UNEXPECTED;
        // do nothing, may be something like( return 1; )
        // LOG_WARN("const expr", K(const_expr->get_value()), K(current_block_), K(ret));
      } else if (OB_ISNULL(current_block_)
              || OB_ISNULL(symbol_table = current_block_->get_symbol_table())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(current_block_), K(ret));
      } else if (OB_ISNULL(var = symbol_table->get_symbol(const_expr->get_value().get_unknown()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get symble var is null", K(var),
                                           K(const_expr->get_value().get_unknown()), K(ret));
      } else if (var->get_name().case_compare("\"anonymous argument\"") != 0) {
        src_type = &(var->get_type());
        if (src_type->is_ref_cursor_type()) {
          is_ref_cursor_type = true;
        }
      }
    } else if (expr->is_obj_access_expr()) {
      OZ (static_cast<const ObObjAccessRawExpr*>(expr)->get_final_type(type_local));
      OX (src_type = &type_local);
      if (OB_SUCC(ret) && src_type->is_ref_cursor_type()) {
        is_ref_cursor_type = true;
      }
    } else if (expr->is_udf_expr()) {
      const ObUDFRawExpr *udf_expr = static_cast<const ObUDFRawExpr *>(expr);
      if (udf_expr->get_is_return_sys_cursor()) {
        is_ref_cursor_type = true;
      }
    }
  }
  return ret;
}

int ObPLResolver::check_assign_type(const ObPLDataType &dest_data_type, const ObRawExpr *right_expr)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(right_expr));
  if (OB_SUCC(ret)) {
    if (dest_data_type.is_cursor_var()) { // cursor var cant be left value
      ret = OB_ERR_CURSOR_LEFT_ASSIGN;
      LOG_WARN("local cursor cann't be assign left", K(ret));
    } else if (dest_data_type.is_cursor_type()) {
      bool is_ref_cursor_type = false;
      OZ (is_return_ref_cursor_type(right_expr, is_ref_cursor_type));
      if (OB_SUCC(ret) && !is_ref_cursor_type && T_NULL != right_expr->get_expr_type()) {
        ret = OB_ERR_EXP_NEED_SAME_DATATYPE;
        LOG_WARN("expression wrong type", K(ret));
      }
    } else {
      // do nothing
    }
  }
  return ret;
}

int ObPLResolver::resolve_question_mark_node(
  const ObStmtNodeTree *into_node, ObRawExpr *&into_expr)
{
  int ret = OB_SUCCESS;
  ObConstRawExpr *expr = NULL;
  const ObPLVar *var = NULL;
  CK (OB_NOT_NULL(current_block_->get_symbol_table()));
  CK (OB_NOT_NULL(var = current_block_->get_symbol_table()->get_symbol(into_node->value_)));
  CK (var->get_name().prefix_match(ANONYMOUS_ARG));
  if (NULL != var && var->is_readonly() && var->is_referenced()) {
    OX (const_cast<ObPLVar*>(var)->set_name(ANONYMOUS_INOUT_ARG));
  }
  OZ (expr_factory_.create_raw_expr(T_QUESTIONMARK, expr));
  CK (OB_NOT_NULL(expr));
  if (OB_SUCC(ret)) {
    ObObjParam val;
    ObExprResType type;
    val.set_unknown(into_node->value_);
    val.set_param_meta();
    expr->set_value(val);
    type.set_null();
    expr->set_result_type(type);
    OZ (expr->extract_info());
    OX (into_expr = expr);
  }
  return ret;
}

bool ObPLResolver::is_question_mark_value(ObRawExpr *into_expr)
{
  bool ret = false;
  if (OB_NOT_NULL(into_expr)
      && T_QUESTIONMARK == into_expr->get_expr_type()
      && (static_cast<ObConstRawExpr *>(into_expr))->get_value().is_unknown()
      && OB_NOT_NULL(current_block_)
      && OB_NOT_NULL(current_block_->get_symbol_table())) {
    const ObPLVar *var = NULL;
    int64_t idx = (static_cast<ObConstRawExpr *>(into_expr))->get_value().get_unknown();
    if (OB_NOT_NULL(var = current_block_->get_symbol_table()->get_symbol(idx))) {
      if (var->get_name().prefix_match(ANONYMOUS_ARG)) {
        ret = true;
      }
    }
  }
  return ret;
}

int ObPLResolver::set_question_mark_type(ObRawExpr *into_expr, const ObPLDataType *type)
{
  int ret = OB_SUCCESS;
  ObConstRawExpr *const_expr = NULL;
  const ObPLVar *var = NULL;
  CK (OB_NOT_NULL(into_expr));
  CK (OB_NOT_NULL(type));
  CK (T_QUESTIONMARK == into_expr->get_expr_type());
  CK (OB_NOT_NULL(const_expr = static_cast<ObConstRawExpr*>(into_expr)));
  CK (OB_NOT_NULL(current_block_->get_symbol_table()));
  CK (OB_NOT_NULL(var = current_block_
    ->get_symbol_table()->get_symbol(const_expr->get_value().get_unknown())));
  CK (var->get_name().prefix_match(ANONYMOUS_ARG));
  OX ((const_cast<ObPLVar*>(var))->set_type(*type));
  OX ((const_cast<ObPLVar*>(var))->set_readonly(false));
  return ret;
}

int ObPLResolver::resolve_assign(const ObStmtNodeTree *parse_tree, ObPLAssignStmt *stmt, ObPLFunctionAST &func)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(parse_tree) || OB_ISNULL(stmt) || OB_ISNULL(current_block_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("parse_tree is NULL", K(parse_tree), K(stmt), K(current_block_), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < parse_tree->num_child_; ++i) {
      if (OB_ISNULL(parse_tree->children_[i])) {
        LOG_WARN("invalid assign stmt", K(parse_tree->children_[i]), K(ret));
      } else if (T_VAR_VAL != parse_tree->children_[i]->type_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid assign stmt", K(parse_tree->children_[i]->type_), K(ret));
      } else {
        //解析into expr
        ObRawExpr *into_expr = NULL;
        const ObStmtNodeTree *into_node = parse_tree->children_[i]->children_[0];
        ObQualifiedName q_name;
        bool need_expect_type = true;
        if (T_SP_OBJ_ACCESS_REF == into_node->type_/*Oracle mode*/) {
          if (OB_FAIL(resolve_obj_access_idents(*into_node, q_name.access_idents_, func))) {
            LOG_WARN("resolve pl obj access ident failed", K(ret));
          }
        } else if (T_QUESTIONMARK == into_node->type_/*Oracle mode*/) {
          OZ (resolve_question_mark_node(into_node, into_expr));
        } else if (OB_FAIL(ObResolverUtils::resolve_obj_access_ref_node(expr_factory_,
                            into_node, q_name, resolve_ctx_.session_info_))) {
          LOG_WARN("resolve obj access ref node failed", K(ret));
        }
        if (OB_SUCC(ret)) {
          if (T_SYSTEM_VARIABLE == into_node->type_) {
            q_name.access_idents_.at(0).access_index_ =
                ObSetVar::SET_SCOPE_GLOBAL == static_cast<ObSetVar::SetScopeType>(parse_tree->children_[i]->value_)
                ? pl::ObPLExternalNS::GLOBAL_VAR : pl::ObPLExternalNS::SESSION_VAR;
          } else if (T_USER_VARIABLE_IDENTIFIER == into_node->type_) {
            q_name.access_idents_.at(0).access_index_ = pl::ObPLExternalNS::USER_VAR;
          }
        }
        if (OB_SUCC(ret) && OB_ISNULL(into_expr)) {
          OZ (resolve_var(q_name, func, into_expr, true/*for write*/));
          if (OB_ERR_VARIABLE_IS_READONLY == ret) {
            ret = OB_ERR_EXP_NOT_ASSIGNABLE;
            LOG_USER_ERROR(OB_ERR_EXP_NOT_ASSIGNABLE,
                           static_cast<int>(into_node->str_len_), into_node->str_value_);
          }
          CK (OB_NOT_NULL(into_expr));
        }
        if (OB_SUCC(ret)) {
          if (T_OP_GET_SYS_VAR == into_expr->get_expr_type()
              || T_OP_GET_USER_VAR == into_expr->get_expr_type()) {
            // 对于系统变量和用户变量的赋值, PL通过SPI构造SET @VAR = VALUE;语句, 然后交由SQL引擎执行
            // 虽然系统变量的类型是确定的，但是这里也不应该将VALUE的结果强转为系统变量的类型
            // 原因是系统变量支持类型SET AUTOCOMMIT=ON;这种写法，而AUTOCOMMIT的类型是BOOL
            // ON显然不支持转为BOOL类型
            // 对于用户变量的赋值，用户变量类型会随着赋值结果的类型而改变，因此这里也不能强转
            need_expect_type = false;
            if (T_OP_GET_SYS_VAR == into_expr->get_expr_type()) {
              ObString var_name(into_node->str_len_, into_node->str_value_);
              if (OB_UNLIKELY(0 == var_name.case_compare("autocommit"))) {
                if (func.is_function() || resolve_ctx_.session_info_.is_for_trigger_package()) {
                  ret = OB_ER_SP_CANT_SET_AUTOCOMMIT;
                  LOG_USER_ERROR(OB_ER_SP_CANT_SET_AUTOCOMMIT);
                  LOG_WARN("Not allowed to set autocommit from a stored function or trigger",
                          K(ret), K(var_name));
                } else {
                  func.set_has_set_autocommit_stmt();
                }
              }
            } 
          } else if (into_expr->is_obj_access_expr()) {
            OZ (func.add_obj_access_expr(into_expr), into_expr);
          }

          OZ (func.add_expr(into_expr), into_expr);
          OZ (stmt->add_into(func.get_expr_count() - 1));
        }

        // 解析value expr
        if (OB_SUCC(ret)) {
          ObRawExpr *value_expr = NULL;
          const ObStmtNodeTree *value_node = parse_tree->children_[i]->children_[1];
          if (OB_ISNULL(value_node)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("value node is NULL", K(value_node), K(ret));
          } else {
            const ObPLDataType *expected_type = NULL;
            ObPLDataType expected_type_local;
            uint64_t package_id = OB_INVALID_ID;
            uint64_t subprogram_id = OB_INVALID_ID;
            int64_t var_idx = OB_INVALID_INDEX;
            const ObPLVar* var = NULL;
            if (into_expr->is_const_raw_expr()) {
              const ObConstRawExpr *const_expr = static_cast<const ObConstRawExpr*>(into_expr);
              const ObPLSymbolTable* symbol_table = NULL;
              const ObPLVar* var = NULL;
              CK (const_expr->get_value().is_unknown());
              CK (OB_NOT_NULL(current_block_));
              CK (OB_NOT_NULL(symbol_table = current_block_->get_symbol_table()));
              CK (OB_NOT_NULL(var = symbol_table->get_symbol(const_expr->get_value().get_unknown())));
              if (OB_FAIL(ret)) {
                // do nothing
              } else if (var->get_name().prefix_match(ANONYMOUS_ARG)) {
                if (!var->is_readonly()) {
                  const_cast<ObPLVar*>(var)->set_name(ANONYMOUS_INOUT_ARG);
                }
              } else {
                expected_type = &(var->get_type());
              }
            } else if (into_expr->is_obj_access_expr()) {
              OZ (static_cast<const ObObjAccessRawExpr*>(into_expr)->get_final_type(expected_type_local));
              OX (expected_type = &expected_type_local);
            } else if (T_OP_GET_PACKAGE_VAR == into_expr->get_expr_type()) {
              CK (into_expr->get_param_count() >= 3);
              OX (package_id = static_cast<const ObConstRawExpr *>(into_expr->get_param_expr(0))->get_value().get_uint64());
              OX (var_idx = static_cast<const ObConstRawExpr *>(into_expr->get_param_expr(1))->get_value().get_int());
              OZ (current_block_->get_namespace().get_package_var(resolve_ctx_, package_id, var_idx, var));
              CK (OB_NOT_NULL(var));
              OX (expected_type = &(var->get_type()));
              OX (func.set_wps());
            } else if (T_OP_GET_SUBPROGRAM_VAR == into_expr->get_expr_type()) {
              CK (into_expr->get_param_count() >= 3);
              OX (subprogram_id = static_cast<const ObConstRawExpr *>(into_expr->get_param_expr(1))->get_value().get_uint64());
              OX (var_idx = static_cast<const ObConstRawExpr *>(into_expr->get_param_expr(2))->get_value().get_int());
              OZ (get_subprogram_var(current_block_->get_namespace(), subprogram_id, var_idx, var));
              CK (OB_NOT_NULL(var));
              OX (expected_type = &(var->get_type()));
            } else {
              ObDataType data_type;
              data_type.set_meta_type(into_expr->get_result_type());
              data_type.set_accuracy(into_expr->get_accuracy());
              expected_type_local.set_data_type(data_type);
              expected_type = &expected_type_local;
            }
            if (OB_SUCC(ret)) {
              if (lib::is_oracle_mode()) {
                CK (OB_LIKELY(T_SP_DECL_DEFAULT == value_node->type_));
                if (OB_SUCC(ret)) {
                  need_expect_type = T_DEFAULT == value_node->children_[0]->type_ ? false : need_expect_type;
                }
                OZ (resolve_expr(value_node->children_[0], func, value_expr,
                                 combine_line_and_col(value_node->children_[0]->stmt_loc_),
                                 true, expected_type));
                CK (OB_NOT_NULL(value_expr));
                // a cursor type cann't assign to a ref cursor type, just as int cann't assign to int*
                if (OB_SUCC(ret) && OB_NOT_NULL(expected_type)) {
                  OZ (check_assign_type(*expected_type, value_expr));
                }
                if (OB_SUCC(ret)) {
                  if (T_FUN_PL_COLLECTION_CONSTRUCT == value_expr->get_expr_type()
                      && 0 == value_expr->get_param_count()) {
                    stmt->add_value(PL_CONSTRUCT_COLLECTION);
                  } else {
                    stmt->add_value(func.get_expr_count() - 1);
                  }
                }
                // 目标是QuestionMark, 需要设置目标的类型, 因为QuestionMark默认是无类型的, 在赋值时确定类型
                if (OB_SUCC(ret) && is_question_mark_value(into_expr)) {
                  if (value_expr->get_result_type().is_ext()) {
                    const ObUserDefinedType *user_type = NULL;
                    OZ (current_block_->get_namespace().get_pl_data_type_by_id(
                      value_expr->get_result_type().get_udt_id(), user_type));
                    OZ (set_question_mark_type(into_expr, user_type));
                  } else {
                    ObDataType data_type;
                    ObPLDataType pl_type;
                    OX (data_type.set_meta_type(value_expr->get_result_type()));
                    OX (data_type.set_accuracy(value_expr->get_result_type().get_accuracy()));
                    OX (pl_type.set_data_type(data_type));
                    OX (set_question_mark_type(into_expr, &pl_type));
                  }
                }
              } else {
                OZ (resolve_expr(value_node, func, value_expr,
                                 combine_line_and_col(value_node->stmt_loc_),
                                 true, need_expect_type ? expected_type : NULL));
                if (OB_SUCC(ret)
                    && T_DEFAULT == value_node->type_
                    && into_expr->get_expr_type() != T_OP_GET_SYS_VAR) {
                  ret = OB_NOT_SUPPORTED;
                  LOG_WARN("default value only used by system variables");
                  LOG_USER_ERROR(OB_NOT_SUPPORTED, "default value not used by system variables");
                }
                OZ (stmt->add_value(func.get_expr_count() - 1));
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObPLResolver::resolve_if(const ObStmtNodeTree *parse_tree, ObPLIfStmt *stmt, ObPLFunctionAST &func)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(parse_tree) || OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("parse_tree is NULL", K(parse_tree), K(stmt), K(ret));
  } else {
    //解析expr then结构
    ObRawExpr *expr = NULL;
    ObPLStmtBlock *then_block = NULL;
    ObPLDataType data_type(ObTinyIntType);
    set_item_type(T_SP_IF);
    if (OB_FAIL(resolve_then(
        parse_tree, func, &data_type, expr, then_block, lib::is_mysql_mode()))) {
      LOG_WARN("failed to resolve then", K(ret));
    } else if (OB_FAIL(func.add_expr(expr))) {
      LOG_WARN("failed to add expr", K(*expr), K(ret));
    } else {
      stmt->set_cond(func.get_expr_count() - 1);
      stmt->set_then(then_block);
    }

    //解析else子句
    if (OB_SUCC(ret)) {
      const ObStmtNodeTree *else_node = parse_tree->children_[2];
      if (NULL == else_node) {
        //do nothing
      } else if (T_SP_ELSE != else_node->type_ || OB_ISNULL(else_node->children_[0])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Invalid else node type", K(else_node->type_), K(else_node->children_[0]), K(ret));
      } else if (T_SP_PROC_STMT_LIST != else_node->children_[0]->type_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Invalid else node", K(else_node->children_[0]->type_), K(ret));
      } else {
        ObPLStmtBlock *else_block = NULL;
        if (OB_FAIL(resolve_stmt_list(else_node->children_[0], else_block, func))) {
          LOG_WARN("failed to resolve stmt list", K(else_node->children_[0]->type_), K(ret));
        } else {
          stmt->set_else(else_block);
        }
      }
    }
  }
  return ret;
}

int ObPLResolver::resolve_case(const ObStmtNodeTree *parse_tree, ObPLCaseStmt *stmt, ObPLFunctionAST &func)
{
  int ret = OB_SUCCESS;

  ObConstRawExpr *case_var = nullptr;
  if (OB_ISNULL(parse_tree) || OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("parse_tree is NULL", K(parse_tree), K(stmt), K(ret));
  } else {
    //解析expr
    ObRawExpr *case_expr = NULL;
    const ObStmtNodeTree *expr_node = parse_tree->children_[0];
    set_item_type(T_SP_CASE);
    ObPLStmtBlock *current_block = current_block_;
    if (OB_FAIL(make_block(func, current_block, current_block_))) {
      LOG_WARN("failed to make block", K(ret));
    } else if (NULL == expr_node) {
      //没有case表达式
    } else if (OB_FAIL(resolve_expr(expr_node,
                                    func,
                                    case_expr,
                                    combine_line_and_col(expr_node->stmt_loc_)))) {
      LOG_WARN("failed to resolve expr", K(expr_node), K(ret));
    } else if (OB_FAIL(current_block_->get_namespace().add_symbol(
                   ObString(""), // anonymous variable for holding case expr value
                   case_expr->get_data_type()))) {
      LOG_WARN("failed to add CASE expr var to symbol table");
    } else {
      stmt->set_case_expr(func.get_expr_count() - 1);
      stmt->set_case_var(func.get_symbol_table().get_count() - 1);
      OZ(expr_factory_.create_raw_expr(T_QUESTIONMARK, case_var));
      CK(OB_NOT_NULL(case_var));
      if (OB_SUCC(ret)) {
        ObObjParam val;
        val.set_unknown(stmt->get_case_var());
        val.set_param_meta();
        case_var->set_value(val);
        case_var->set_result_type(case_expr->get_result_type());
        OZ (case_var->extract_info());
      }
    }

    //解析when子句
    const ObStmtNodeTree *when_list = parse_tree->children_[1];
    OZ (resolve_when(when_list, case_var, stmt, func));

    //解析else子句
    if (OB_SUCC(ret)) {
      const ObStmtNodeTree *else_node = parse_tree->children_[2];
      ObPLStmtBlock *else_block = NULL;
      if (NULL == else_node) {
        /*
         * Mysql在运行态发现没有when子句能够匹配，并且没有else子句，会报错。所以这里为else分支生成一个Signal语句。
         * If no when_value or search_condition matches the value tested and the CASE statement contains no ELSE clause,
         * a Case not found for CASE statement error results.
         * */
        if (OB_FAIL(make_block(func, current_block_, else_block))) {
          LOG_WARN("failed to make block", K(ret));
        } else {
          ObPLStmt *stmt = NULL;
          ObPLSignalStmt *signal_stmt = NULL;
          if (OB_FAIL(stmt_factory_.allocate(PL_SIGNAL, else_block, stmt))) {
            LOG_WARN("failed to alloc stmt", K(ret));
          } else if (OB_ISNULL(signal_stmt = static_cast<ObPLSignalStmt*>(stmt))) {
            LOG_WARN("failed to cast stmt", K(ret));
          } else {
            signal_stmt->set_cond_type(ERROR_CODE);
            signal_stmt->set_sql_state("20000");
            signal_stmt->set_str_len(5);
            if (lib::is_oracle_mode()) {
              signal_stmt->set_error_code(OB_ER_SP_CASE_NOT_FOUND); // ORACLE模式使用OB的错误码
            } else {
              signal_stmt->set_error_code(ER_SP_CASE_NOT_FOUND); // MySQL模式使用Mysql错误码
            }
            signal_stmt->set_ob_error_code(OB_ER_SP_CASE_NOT_FOUND);
            if (OB_FAIL(else_block->add_stmt(signal_stmt))) {
              LOG_WARN("failed to add stmt", K(stmt), K(ret));
            }
          }
        }
      } else {
        if (T_SP_ELSE != else_node->type_ || OB_ISNULL(else_node->children_[0])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Invalid else node type", K(else_node->type_), K(else_node->children_[0]), K(ret));
        } else if (T_SP_PROC_STMT_LIST != else_node->children_[0]->type_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Invalid else node", K(else_node->children_[0]->type_), K(ret));
        } else {
          if (OB_FAIL(resolve_stmt_list(else_node->children_[0], else_block, func))) {
            LOG_WARN("failed to resolve stmt list", K(else_node->children_[0]->type_), K(ret));
          }
        }
      }

      if (OB_SUCC(ret)) {
        stmt->set_else_clause(else_block);
      }
    }

    //恢复current_block_
    if (OB_SUCC(ret)) {
      set_current(*current_block);
    }
  }
  return ret;
}

int ObPLResolver::resolve_when(const ObStmtNodeTree *parse_tree, ObRawExpr *case_expr_var, ObPLCaseStmt *stmt, ObPLFunctionAST &func)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(parse_tree) || OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parse_tree of when list is NULL", K(parse_tree), K(stmt), K(ret));
  } else if (T_WHEN_LIST != parse_tree->type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid when node type", K(parse_tree->type_), K(ret));
  } else if (0 >= parse_tree->num_child_) {  // one when clause at least
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected when list number", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < parse_tree->num_child_; ++i) {
      ObRawExpr *expr = nullptr;
      ObPLStmtBlock *body = nullptr;
      ObPLDataType data_type(ObTinyIntType);
      bool is_bool_stmt = false;
      if (!case_expr_var && lib::is_mysql_mode()) {
        is_bool_stmt = true;
      }
      set_item_type(T_SP_WHEN);
      if (OB_FAIL(make_block(func, current_block_, body))) {
        LOG_WARN("failed to make block");
      } else if (OB_FAIL(resolve_then(parse_tree->children_[i],
                                      func,
                                      case_expr_var ? nullptr : &data_type,
                                      expr,
                                      body,
                                      is_bool_stmt))) {
        LOG_WARN("failed to resolve then", K(ret));
      }

      if (OB_SUCC(ret) && case_expr_var) {
        ObRawExpr *cmp_expr = nullptr;

        OZ (ObRawExprUtils::create_equal_expr(expr_factory_,
                                             &resolve_ctx_.session_info_,
                                             case_expr_var,
                                             expr,
                                             cmp_expr));
        OX (expr = cmp_expr);
      }

      OZ (func.add_expr(expr));
      OZ (expr->formalize(&resolve_ctx_.session_info_));
      OZ (stmt->add_when_clause(func.get_expr_count() - 1, body));
    }
  }
  return ret;
}

int ObPLResolver::resolve_then(const ObStmtNodeTree *parse_tree,
                               ObPLFunctionAST &func,
                               ObPLDataType *data_type,
                               ObRawExpr *&expr,
                               ObPLStmtBlock *&then_block,
                               bool is_add_bool_expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(parse_tree)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("parse_tree is NULL", K(parse_tree), K(ret));
  } else {
    //解析expr
    const ObStmtNodeTree *expr_node = parse_tree->children_[0];
    if (NULL == expr_node) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr node is NULL", K(expr_node), K(parse_tree->children_), K(ret));
    } else if (OB_FAIL(resolve_expr(expr_node, func, expr,
                                    combine_line_and_col(expr_node->stmt_loc_),
                                    false, data_type, false, is_add_bool_expr))) {
      LOG_WARN("failed to resolve expr", K(expr_node), K(ret));
    } else { /*do nothing*/ }

    //解析then子句
    if (OB_SUCC(ret)) {
      reset_item_type();
      const ObStmtNodeTree *then_node = parse_tree->children_[1];
      if (NULL == then_node) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("then node is NULL", K(then_node), K(parse_tree->children_), K(ret));
      } else if (T_SP_PROC_STMT_LIST != then_node->type_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Invalid then node type", K(then_node->type_), K(ret));
      } else if (OB_FAIL(resolve_stmt_list(then_node, then_block, func))) {
        LOG_WARN("failed to resolve stmt list", K(then_node->type_), K(ret));
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObPLResolver::resolve_loop_control(const ObStmtNodeTree *parse_tree, ObPLLoopControl *stmt, bool is_iterate_label, ObPLFunctionAST &func)
{
  int ret = OB_SUCCESS;
  UNUSED(func);
  if (OB_ISNULL(parse_tree) || OB_ISNULL(stmt) || OB_ISNULL(current_block_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("parse_tree is NULL", K(parse_tree), K(stmt), K(current_block_), K(ret));
  } else if (NULL != parse_tree->children_[0]) {
    //解析label
    ObString name;
    int64_t label = OB_INVALID_INDEX;
    const ObStmtNodeTree *label_node = parse_tree->children_[0];
    if (OB_FAIL(resolve_ident(label_node, name))) {
      LOG_WARN("failed to resolve ident", K(label_node), K(ret));
    } else if (OB_FAIL(resolve_label(name, current_block_->get_namespace(), label, is_iterate_label))) {
      if (OB_ERR_SP_LILABEL_MISMATCH == ret && lib::is_oracle_mode()) {
        ret = OB_ERR_NEED_A_LABEL;
        LOG_USER_ERROR(OB_ERR_NEED_A_LABEL, name.length(), name.ptr());
      } else {
        LOG_WARN("failed to resolve label", K(name), K(ret));
      }
    } else if (OB_INVALID_INDEX == label) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("can not resolve label", K(name), K(label), K(ret));
    } else {
      stmt->set_next_label(name);
    }
  } else if (!lib::is_oracle_mode()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("must have label name in iterate stme", K(resolve_ctx_.session_info_.get_compatibility_mode()), K(ret));
  } else { /*do nothing*/ }

  if (OB_SUCC(ret)) {
    //解析condition
    const ObStmtNodeTree *cond_node = parse_tree->children_[1];
    if (NULL != cond_node) {
      if (lib::is_oracle_mode()) {
        ObRawExpr *expr = NULL;
        ObPLDataType data_type(ObTinyIntType);
        if (OB_FAIL(resolve_expr(cond_node, func, expr, combine_line_and_col(cond_node->stmt_loc_),
             true, &data_type, false, lib::is_mysql_mode()))) {
          LOG_WARN("failed to resolve then", K(ret));
        } else {
          stmt->set_cond(func.get_expr_count() - 1);
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("condition loop is not support in mysql mode", K(ret));
      }
    }
  }
  return ret;
}

int ObPLResolver::resolve_iterate(const ObStmtNodeTree *parse_tree, ObPLIterateStmt *stmt, ObPLFunctionAST &func)
{
  return resolve_loop_control(parse_tree, stmt, true, func);
}

int ObPLResolver::resolve_leave(const ObStmtNodeTree *parse_tree, ObPLLeaveStmt *stmt, ObPLFunctionAST &func)
{
  return resolve_loop_control(parse_tree, stmt, false, func);
}

int ObPLResolver::resolve_while(const ObStmtNodeTree *parse_tree, ObPLWhileStmt *stmt, ObPLFunctionAST &func)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(parse_tree)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("parse_tree is NULL", K(parse_tree), K(ret));
  } else {
    const ObStmtNodeTree *expr_node = parse_tree->children_[0];
    const ObStmtNodeTree *body_node = parse_tree->children_[1];
    if (OB_FAIL(resolve_cond_loop(expr_node, body_node, stmt, func))) {
      LOG_WARN("failed to resolve while", K(parse_tree), K(stmt), K(ret));
    }
  }
  return ret;
}

int ObPLResolver::resolve_for_loop(const ObStmtNodeTree *parse_tree, ObPLForLoopStmt *stmt, ObPLFunctionAST &func)
{
  int ret = OB_SUCCESS;
  CK(OB_NOT_NULL(parse_tree),
     OB_NOT_NULL(stmt),
     T_SP_FOR_LOOP == parse_tree->type_,
     5 == parse_tree->num_child_,
     OB_NOT_NULL(parse_tree->children_[0]),
     OB_NOT_NULL(parse_tree->children_[1]),
     OB_NOT_NULL(parse_tree->children_[2]));
  if (OB_SUCC(ret)) {
    bool reverse = static_cast<bool>(parse_tree->value_);
    const ObStmtNodeTree *index_node = parse_tree->children_[0];
    const ObStmtNodeTree *lower_node = parse_tree->children_[1];
    const ObStmtNodeTree *upper_node = parse_tree->children_[2];
    const ObStmtNodeTree *body_node  = parse_tree->children_[3];
    const ObStmtNodeTree *label_node = parse_tree->children_[4];
    ObPLDataType data_type;
    ObPLStmtBlock *body_block = NULL;
    // 解析reverse
    stmt->set_reverse(reverse);
    // 解析 label
    if (OB_SUCC(ret)) {
      if (OB_NOT_NULL(label_node)) {
        ObString name;
        int64_t label = OB_INVALID_ID;
        if (OB_FAIL(resolve_ident(label_node, name))) {
          LOG_WARN("failed to resolve ident", K(ret), K(label_node));
        } else if (OB_FAIL(resolve_label(name, current_block_->get_namespace(), label, false))) {
          LOG_WARN("failed to resolve label", K(ret), K(name));
        } else if (OB_UNLIKELY(OB_INVALID_INDEX == label)) {
          ret = OB_ERR_SP_LILABEL_MISMATCH;
          LOG_WARN("no matching label", K(ret), K(name), K(label));
        }
      }
    }
    // 解析index
    if (OB_SUCC(ret)) {
      ObString index_name;
      if (OB_FAIL(build_pl_integer_type(PL_PLS_INTEGER, data_type))) {
        LOG_WARN("failed to build pl integer type", K(ret));
      } else if (OB_FAIL(make_block(func, current_block_, body_block, true))) {
        LOG_WARN("failed to make blocl", K(current_block_), K(ret));
      } else if (OB_ISNULL(body_block)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to allocate block", K(body_block), K(ret));
      } else if (OB_FAIL(resolve_ident(index_node, index_name))) {
        LOG_WARN("failed to resolve ident", K(index_node), K(ret));
      } else if (OB_FAIL(body_block->get_namespace().add_symbol(index_name,
                                                                data_type,
                                                                NULL,
                                                                true))) { /*index 只读，不可再循环体内赋值*/
        LOG_WARN("fail to add for loop index variable to symbol", K(ret));
      } else {
        stmt->set_ident(func.get_symbol_table().get_count() - 1);
      }
    }
    // 解析lower bound expr
    if (OB_SUCC(ret)) {
      ObRawExpr *lower_expr = NULL;
      if (OB_FAIL(resolve_expr(lower_node, static_cast<ObPLCompileUnitAST&>(func), lower_expr,
                               combine_line_and_col(lower_node->stmt_loc_), true, &data_type))) {
        LOG_WARN("failed to resolve expr", K(lower_node), K(ret));
      } else {
        stmt->set_lower(func.get_expr_count() - 1);
      }
    }
    // 解析upper bound expr
    if (OB_SUCC(ret)) {
      ObRawExpr *upper_expr = NULL;
      if (OB_FAIL(resolve_expr(upper_node, static_cast<ObPLCompileUnitAST&>(func), upper_expr,
                               combine_line_and_col(upper_node->stmt_loc_), true, &data_type))) {
        LOG_WARN("failed to resolve expr", K(upper_node), K(ret));
      } else {
        stmt->set_upper(func.get_expr_count() - 1);
      }
    }
    // 解析body
    if (OB_SUCC(ret)) {
      if (OB_FAIL(resolve_stmt_list(body_node, body_block, func))) {
        LOG_WARN("failed to resolve stmt list", K(body_node->type_), K(ret));
      } else {
        stmt->set_body(body_block);
      }
    }
  }
  return ret;
}

int ObPLResolver::resolve_cursor_for_loop(
  const ObStmtNodeTree *parse_tree, ObPLCursorForLoopStmt *stmt, ObPLFunctionAST &func)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(parse_tree));
  CK (OB_NOT_NULL(stmt));
  CK (T_SP_CURSOR_FOR_LOOP == parse_tree->type_);
  CK (4 == parse_tree->num_child_);
  CK (OB_NOT_NULL(parse_tree->children_[0]));
  CK (OB_NOT_NULL(parse_tree->children_[1]));
  if (OB_SUCC(ret)) {
    question_mark_cnt_ = parse_tree->value_;
    const ObStmtNodeTree* index_node = parse_tree->children_[0];
    const ObStmtNodeTree* cursor_node = parse_tree->children_[1];
    const ObStmtNodeTree* body_node = parse_tree->children_[2];
    const ObStmtNodeTree* label_node = parse_tree->children_[3];
    ObString index_name;
    ObPLStmtBlock *body_block = NULL;
    // 解析label
    if (OB_SUCC(ret)) {
      if (OB_NOT_NULL(label_node)) {
        ObString name;
        int64_t label = OB_INVALID_ID;
        if (OB_FAIL(resolve_ident(label_node, name))) {
          LOG_WARN("failed to resolve ident", K(ret), K(label_node));
        } else if (OB_FAIL(resolve_label(name, current_block_->get_namespace(), label, false))) {
          LOG_WARN("failed to resolve label", K(ret), K(name));
        } else if (OB_UNLIKELY(OB_INVALID_INDEX == label)) {
          ret = OB_ERR_SP_LILABEL_MISMATCH;
          LOG_WARN("no matching label", K(ret), K(name), K(label));
        }
      }
    }
    // 创建body block
    if (OB_SUCC(ret)) {
      if (OB_FAIL(make_block(func, current_block_, body_block, true))) {
        LOG_WARN("failed to make block", K(ret));
      } else if (OB_ISNULL(body_block)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to allocate body block", K(ret));
      }
    }
    // 解析cursor
    if (OB_SUCC(ret)) {
      int64_t cursor_index = OB_INVALID_ID;
      if (T_OBJ_ACCESS_REF == cursor_node->type_) {
        CK (cursor_node->num_child_ >= 1);
        CK (OB_NOT_NULL(cursor_node->children_[0]));
        OX (cursor_node = T_FUN_SYS == cursor_node->children_[0]->type_ ?
          cursor_node->children_[0] : cursor_node);

        // 解析Cursor
        OZ (resolve_cursor(
          (T_FUN_SYS == cursor_node->type_) ? cursor_node->children_[0] : cursor_node,
          current_block_->get_namespace(), cursor_index, func));
        OX (stmt->set_cursor_index(cursor_index));

        //解析实参
        if (OB_SUCC(ret)) {
          const ObStmtNodeTree *param_node = NULL;
          if (T_FUN_SYS == cursor_node->type_ && cursor_node->num_child_ > 1) {
            param_node = cursor_node->children_[1];
          } else if (T_OBJ_ACCESS_REF == cursor_node->type_
                     && cursor_node->children_[1] != NULL
                     && T_FUN_SYS == cursor_node->children_[1]->children_[0]->type_
                     && cursor_node->children_[1]->children_[0]->num_child_ > 1) {
            param_node = cursor_node->children_[1]->children_[0]->children_[1];
          }
          OZ (resolve_cursor_actual_params(param_node, stmt, func));
        }
        if (OB_SUCC(ret)) {
          const ObPLVar *var = NULL;
          const ObPLCursor *cursor = stmt->get_cursor();
          if (OB_FAIL(stmt->get_var(var))) {
            LOG_WARN("failed to get var", K(ret));
          } else if (OB_ISNULL(cursor)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("value is NULL", K(ret));
          } else if (ObPLCursor::DEFINED != cursor->get_state()) {
            if (OB_NOT_NULL(var) && var->get_pl_data_type().is_ref_cursor_type()) {
              ret = OB_ERR_UNDEFINED;
              LOG_WARN("ref cursor not defined", KPC(var), K(ret));
              LOG_USER_ERROR(OB_ERR_UNDEFINED, var->get_name().ptr());
            } else {
              ret = OB_ERR_TYPE_DECL_MALFORMED;
              LOG_WARN("cursor not defined", KPC(var), K(ret));
            }
          }
        }
      } else if (T_SQL_STMT == cursor_node->type_) {
        ObPLStmtBlock *parent = current_block_;
        set_current(*body_block);
        ObString cursor_name;
        ObPLDataType dummy_return_type;
        ObArray<int64_t> dummy_formal_params;
        if (OB_FAIL(resolve_cursor_def(cursor_name, cursor_node,  body_block->get_namespace(),
                                   dummy_return_type, dummy_formal_params, func, cursor_index))) {
          LOG_WARN("failed to resolve cursor comm", K(ret), K(cursor_name), K(cursor_node->type_));
        } else {
          stmt->set_cursor_index(cursor_index);
          stmt->set_need_declare(true);
          set_current(*parent);
        }
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("name cursro node is invalid", K(ret), K(cursor_node->type_));
      }
      if (OB_SUCC(ret)) {
        ObPLCursor *cursor = current_block_->get_cursor(cursor_index);
        if (OB_NOT_NULL(cursor) && cursor->is_dup_column()) {
          ret = OB_ERR_DUP_NAME_IN_CURSOR;
          LOG_WARN("alias required in SELECT list of cursor to avoid duplicate column names", K(ret), K(cursor_index));
        }
      }
    }
    // 解析index
    if (OB_SUCC(ret)) {
      const ObUserDefinedType *user_type = NULL;
      if (OB_FAIL(resolve_ident(index_node, index_name))) {
        LOG_WARN("failed to resolve ident", K(ret));
      } else if (OB_FAIL(body_block->get_namespace().add_symbol(index_name,
                                                                stmt->get_cursor()->get_cursor_type(),
                                                                NULL,
                                                                false))) { /*与ForLoop不同，CursorForLoop的index可以在循环体内赋值*/
        LOG_WARN("fail to add for loop index variable to symbol", K(ret));
      } else if (OB_FAIL(body_block->get_namespace().get_pl_data_type_by_id(
                  stmt->get_cursor()->get_cursor_type().get_user_type_id(), user_type))) {
        LOG_WARN("fail to get user type", K(ret));
      } else if (OB_ISNULL(user_type)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("user type is null", K(ret));
      } else {
        if (OB_SUCC(ret)) {
          stmt->set_user_type(user_type);
          stmt->set_index_index(func.get_symbol_table().get_count() - 1);
        }
      }
    }
    // 将index作为fetch into变量
    if (OB_SUCC(ret)) {
      ObRawExpr *expr = NULL;
      if (OB_FAIL(ObPLResolver::resolve_local_var(index_name,
                                                  body_block->get_namespace(),
                                                  expr_factory_,
                                                  &resolve_ctx_.session_info_,
                                                  &resolve_ctx_.schema_guard_,
                                                  expr))) {
        LOG_WARN("failed to resolve_local_var", K(ret));
      } else if (OB_FAIL(func.add_expr(expr))) {
        LOG_WARN("failed ot add expr", K(ret));
      } else if (OB_FAIL(stmt->add_into(func.get_expr_count() - 1, current_block_->get_namespace(), *expr))) {
        LOG_WARN("failed to add into", K(ret));
      } else if (!expr->is_obj_access_expr()) { // cursor 的返回值是一个record变量, 因此这里一定是个obj_access_expr
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("cursor for loop into expr not access expr", K(ret));
      } else if (OB_FAIL(func.add_obj_access_expr(expr))) {
          LOG_WARN("push_back error", K(ret));
      }
    }
    // 解析body
    if (OB_SUCC(ret)) {
      // 这儿做了一个block，这样一个for loop cursor会有两个block， 一个是定义的，一个是body
      // 这么做的原因是在code gen的时候，在block结尾出会close cursor，如果连个是连在一起，那么
      // 第一次for 循环结束后就会close cursor，导致后续的fetch，cursor都是close的。
      // 提取之后，cursor就定义在前一个block中，在循环体block不会去做close的动作。
      ObPLStmtBlock *body_body_block = NULL;
      if (OB_FAIL(make_block(func, body_block, body_body_block, true))) {
        LOG_WARN("failed to make block", K(ret));
      } else if (OB_ISNULL(body_body_block)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to allocate body block", K(ret));
      } else if (OB_FAIL(resolve_stmt_list(body_node, body_body_block, func))) {
        LOG_WARN("failed to resolve stmt list", K(body_node->type_), K(ret));
      } else {
        stmt->set_body(body_body_block);
      }
    }
  }
  return ret;
}

int ObPLResolver::check_use_idx_illegal(ObRawExpr* expr, int64_t idx)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(expr)) {
    if (T_QUESTIONMARK == expr->get_expr_type()) {
      ObConstRawExpr *const_expr = static_cast<ObConstRawExpr*>(expr);
      if (idx == const_expr->get_value().get_unknown()) {
        ret = OB_ERR_FORALL_ITER_NOT_ALLOWED;
        LOG_WARN("PLS-00430: FORALL iteration variable INDX is not allowed in this context", K(ret));
      }
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
        OZ (check_use_idx_illegal(expr->get_param_expr(i), idx));
      }
    }
  }
  return ret;
}

int ObPLResolver::check_raw_expr_in_forall(ObRawExpr* expr, int64_t idx, bool &need_modify, bool &can_array_binding)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(expr));
  if (OB_SUCC(ret)) {
    need_modify = false;
    if (expr->is_obj_access_expr()) {
      ObObjAccessRawExpr *obj_access_expr = static_cast<ObObjAccessRawExpr*>(expr);
      int64_t collection_index = 0;
      for (int64_t i = 0; i < obj_access_expr->get_access_idxs().count(); ++i) {
        if (obj_access_expr->get_access_idxs().at(i).elem_type_.is_collection_type()) {
          collection_index = i;
          break;
        }
      }
      if (obj_access_expr->get_access_idxs().count() > 1
          && obj_access_expr->get_access_idxs().at(collection_index).elem_type_.is_collection_type()) {
        if (obj_access_expr->get_access_idxs().at(collection_index + 1).is_local()) {
          int64_t var_idx = obj_access_expr->get_access_idxs().at(collection_index + 1).var_index_;
          if (var_idx < 0 || var_idx >= obj_access_expr->get_var_indexs().count()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("var index is invalid", K(var_idx), K(obj_access_expr->get_var_indexs()), K(ret));
          } else if (obj_access_expr->get_var_indexs().at(var_idx) == idx) {
            need_modify = true;
            if (obj_access_expr->get_var_indexs().count() != 2
                || !obj_access_expr->get_access_idxs().at(collection_index + 1).elem_type_.is_obj_type()) {
              can_array_binding = false;
            } else if (2 == obj_access_expr->get_access_idxs().count() - collection_index) {
              can_array_binding = (0 == collection_index);
            } else if (obj_access_expr->get_access_idxs().at(collection_index + 2).is_const()) {
              can_array_binding = false;
            } else {
              ret = OB_ERR_BULK_IN_BIND;
              LOG_WARN("PLS-00674: references to fields of BULK In-BIND table of records or objects must have the form A(I).F",
                       K(ret), K(obj_access_expr));
            }
          }
        } else if (obj_access_expr->get_access_idxs().at(1).is_expr()) {
          CK (OB_NOT_NULL(obj_access_expr->get_access_idxs().at(1).get_sysfunc_));
          if (OB_FAIL(ret)) {
          } else if (T_FUN_PL_ASSOCIATIVE_INDEX == obj_access_expr->get_access_idxs().at(1).get_sysfunc_->get_expr_type()) {
            ObPLAssocIndexRawExpr *result_expr = static_cast<ObPLAssocIndexRawExpr *>(
              obj_access_expr->get_access_idxs().at(1).get_sysfunc_);
            ObConstRawExpr *const_expr = NULL;
            CK (OB_NOT_NULL(result_expr));
            CK (OB_NOT_NULL(result_expr->get_param_expr(1)));
            if (OB_SUCC(ret) && result_expr->get_param_expr(1)->is_const_raw_expr()) {
              CK (OB_NOT_NULL(const_expr = static_cast<ObConstRawExpr*>(result_expr->get_param_expr(1))));
            }
            if (OB_SUCC(ret) && OB_NOT_NULL(const_expr)) {
              const ObObj &const_obj = const_expr->get_value();
              if (const_obj.is_unknown() && (idx == const_obj.get_unknown())) {
                need_modify = true;
                can_array_binding = false;
              }
            }
          } else {
            OZ (check_use_idx_illegal(obj_access_expr->get_access_idxs().at(1).get_sysfunc_, idx));
          }
        } else if (obj_access_expr->get_access_idxs().at(1).is_const() ||
                   obj_access_expr->get_access_idxs().at(1).is_pkg()) {
          // do nothing ...
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("not supported yet", K(ret), KPC(obj_access_expr));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "non-urowid type");
        }
      } else {
        can_array_binding = false;
      }
    } else if (T_QUESTIONMARK == expr->get_expr_type()) {
      ObConstRawExpr *const_expr = static_cast<ObConstRawExpr*>(expr);
      if (idx == const_expr->get_value().get_unknown()) {
        ret = OB_ERR_FORALL_ITER_NOT_ALLOWED;
        LOG_WARN("PLS-00430: FORALL iteration variable INDX is not allowed in this context", K(ret));
      }
      //这里控制包含非数组类型变量的forall语句直接以forloop实现, 避免forall先回退, 再forloop执行
      can_array_binding = true;
    } else {
      bool inner_modify = false;
      bool inner_can_array_binding = true;
      for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
        OZ (check_raw_expr_in_forall(expr->get_param_expr(i), idx, inner_modify, inner_can_array_binding));
        if (inner_modify) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("not supported yet", K(ret), K(expr));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "expression batch execution in forall statement");
        }
      }
    }
  }
  return ret;
}

int ObPLResolver::modify_raw_expr_in_forall(ObPLForAllStmt &stmt,
                                            ObPLFunctionAST &func,
                                            ObPLSqlStmt &sql_stmt,
                                            ObIArray<int64_t>& modify_exprs)
{
  int ret = OB_SUCCESS;
  // only allow x(idx); y(x(idx)),y(abs(x(idx))),y(x(idx) + 1) is invalid
  // in check_raw_expr_in_forall y(x(idx)) is valid, so here we check again
  for (int64_t i = 0; OB_SUCC(ret) && i < modify_exprs.count(); ++i) {
    ObRawExpr* expr = func.get_expr(sql_stmt.get_params().at(modify_exprs.at(i)));
    ObObjAccessRawExpr *obj_access_expr = NULL;
    CK (OB_NOT_NULL(expr));
    CK (expr->is_obj_access_expr());
    CK (OB_NOT_NULL(obj_access_expr = static_cast<ObObjAccessRawExpr*>(expr)));
    CK (2 == obj_access_expr->get_access_idxs().count());
    CK (2 == obj_access_expr->get_var_indexs().count());
    CK (obj_access_expr->get_access_idxs().at(0).elem_type_.is_collection_type());
    CK (1 == obj_access_expr->get_access_idxs().at(1).var_index_);
    OZ (modify_raw_expr_in_forall(stmt, func, sql_stmt, modify_exprs.at(i),
          obj_access_expr->get_var_indexs().at(0)));
  }
  return ret;
}

int ObPLResolver::modify_raw_expr_in_forall(ObPLForAllStmt &stmt,
                                            ObPLFunctionAST &func,
                                            ObPLSqlStmt &sql_stmt,
                                            int64_t modify_expr_id,
                                            int64_t table_idx)
{
  int ret = OB_SUCCESS;
  SET_LOG_CHECK_MODE();
  hash::ObHashMap<int64_t, int64_t> &tab_to_subtab = stmt.get_tab_to_subtab_map();
  int64_t sub_table_idx = OB_INVALID_INDEX;
  if (OB_FAIL(tab_to_subtab.get_refactored(table_idx, sub_table_idx))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("failed to get refactored in hash map", K(ret), K(table_idx));
    } else {
      const ObPLVar *table_var = func.get_symbol_table().get_symbol(table_idx);
      ret = OB_SUCCESS;
      CK (OB_NOT_NULL(table_var));
      CK (OB_NOT_NULL(stmt.get_body()));
      ObPLBlockNS &ns = const_cast<ObPLBlockNS&>(stmt.get_body()->get_namespace());
      OZ (ns.add_symbol(ObString(""), table_var->get_type(), NULL, true));
      if (OB_SUCC(ret)) {
        sub_table_idx = func.get_symbol_table().get_count() - 1;
        OZ (tab_to_subtab.set_refactored(table_idx, sub_table_idx));
      }
    }
  }
  if (OB_SUCC(ret)) {
    ObConstRawExpr* expr = NULL;
    ObObjParam val;
    ObExprResType res_type;
    OZ (expr_factory_.create_raw_expr(T_QUESTIONMARK, expr));
    CK (OB_NOT_NULL(expr));
    if (OB_SUCC(ret)) {
      val.set_unknown(sub_table_idx);
      val.set_param_meta();
      expr->set_value(val);
      res_type.set_ext();
      expr->set_result_type(res_type);
      OZ (expr->extract_info());
    }
    if (OB_SUCC(ret)) {
      func.add_expr(expr);
      sql_stmt.get_array_binding_params().at(modify_expr_id) = func.get_expr_count() - 1;
    }
  }
  CANCLE_LOG_CHECK_MODE();
  return ret;
}

int ObPLResolver::check_forall_sql_and_modify_params(ObPLForAllStmt &stmt, ObPLFunctionAST &func)
{
  /*
   * dml_statement
   * A static or dynamic INSERT, UPDATE, DELETE, or MERGE statement that references at least
   * one collection in its VALUES or WHERE clause. Performance benefits apply only to
   * collection references that use index as an index.
   * Every collection that dml_statement references must have indexes that match the
   * values of index. If you apply the DELETE, EXTEND, or TRIM method to one collection,
   * apply it to the other collections also, so that all collections have the same set of indexes.
   * If any collection lacks a referenced element, PL/SQL raises an exception.
   *
   * Restriction on dml_statement If dml_statement is a dynamic SQL statement, then
   * values in the USING clause (bind variables for the dynamic SQL statement) must be
   * simple references to the collection, not expressions. For example, collection(i) is
   * valid, but UPPER(collection(i)) is invalid.
   */
  int ret = OB_SUCCESS;
  const ObPLStmtBlock *body_block = NULL;
  CK (OB_NOT_NULL(body_block = stmt.get_body()));
  CK (OB_LIKELY(1 == body_block->get_child_size()));
  CK (OB_NOT_NULL(body_block->get_child_stmt(0)));
  CK (OB_LIKELY(PL_SQL == body_block->get_child_stmt(0)->get_type())
      || OB_LIKELY(PL_EXECUTE == body_block->get_child_stmt(0)->get_type()));
  if (PL_EXECUTE == body_block->get_child_stmt(0)->get_type()) {
    const ObPLExecuteStmt *execute_stmt
      = static_cast<const ObPLExecuteStmt *>(body_block->get_child_stmt(0));
    ObSEArray<int64_t, 3> params;
    const ObIArray<InOutParam> *using_params = NULL;
    CK (OB_NOT_NULL(execute_stmt));
    CK (OB_NOT_NULL(using_params = &(execute_stmt->get_using())));
    if (execute_stmt->get_into().count() && !execute_stmt->get_is_returning()) {
      ret= OB_ERR_FORALL_BULK_TOGETHER;
      LOG_WARN("PLS-00432: implementation restriction: cannot use FORALL and BULK COLLECT INTO"
               " together in SELECT statements", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < using_params->count(); ++i) {
      OZ (params.push_back(using_params->at(i).param_));
    }
    if (OB_SUCC(ret) && params.count() <= 0) {
        ret = OB_ERR_FORALL_DML_WITHOUT_BULK;
        LOG_WARN("PLS-00435: DML statement without BULK In-BIND cannot be used inside FORALL.",
                 K(ret), K(params.count()));
    }
    ObSEArray<int64_t, 16> need_modify_exprs;
    bool can_array_binding = true;
    for (int64_t i = 0; OB_SUCC(ret) && i < params.count(); ++i) {
      ObRawExpr* exec_param = func.get_expr(params.at(i));
      bool need_modify = false;
      CK (OB_NOT_NULL(exec_param));
      OZ (check_raw_expr_in_forall(exec_param, stmt.get_ident(), need_modify, can_array_binding));
      if (OB_SUCC(ret)) {
        if (need_modify) {
          OZ (need_modify_exprs.push_back(i));
        }
      }
    }
    if (OB_SUCC(ret) && 0 == need_modify_exprs.count()) {
      ret = OB_ERR_FORALL_DML_WITHOUT_BULK;
      LOG_WARN("PLS-00435: DML statement without BULK In-BIND cannot be used inside FORALL.",
               K(ret), K(params.count()));
    }
    stmt.set_binding_array(false);
  } else {
    const ObPLSqlStmt* const_sql_stmt = NULL;
    ObPLSqlStmt* sql_stmt = NULL;
    CK (OB_NOT_NULL(const_sql_stmt
      = static_cast<const ObPLSqlStmt*>(body_block->get_child_stmt(0))));
    CK (OB_NOT_NULL(sql_stmt = const_cast<ObPLSqlStmt*>(const_sql_stmt)));
    if (OB_SUCC(ret)
        && sql_stmt->get_stmt_type() != stmt::T_INSERT
        && sql_stmt->get_stmt_type() != stmt::T_SELECT
        && sql_stmt->get_stmt_type() != stmt::T_DELETE
        && sql_stmt->get_stmt_type() != stmt::T_UPDATE) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("forall support dml sql only", K(ret), K(sql_stmt->get_stmt_type()));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "not dml sql in forall");
    } else if (sql_stmt->has_link_table()) {
      ret = OB_ERR_FORALL_ON_REMOTE_TABLE;
      LOG_WARN("FORALL INSERT/UPDATE/DELETE not support on remote tables", K(ret), K(sql_stmt->get_stmt_type()));
    }
    if (OB_SUCC(ret)) {
      // Restriction:
      // PLS-00432: implementation restriction: cannot use FORALL and BULK COLLECT INTO together in SELECT statements
      // PLS-00435: DML statement without BULK In-BIND cannot be used inside FORALL
      // PLS-00430: FORALL iteration variable INDX is not allowed in this context
      const common::ObIArray<int64_t>& params = sql_stmt->get_params();
      if (OB_SUCC(ret) && params.count() <= 0) {
          ret = OB_ERR_FORALL_DML_WITHOUT_BULK;
          LOG_WARN("PLS-00435: DML statement without BULK In-BIND cannot be used inside FORALL.", K(ret), K(params.count()));
      }
      ObSEArray<int64_t, 16> need_modify_exprs;
      bool can_array_binding = true;
      for (int64_t i = 0; OB_SUCC(ret) && i < params.count(); ++i) {
        ObRawExpr* exec_param = func.get_expr(params.at(i));
        bool need_modify = false;
        bool is_array_binding = true;
        CK (OB_NOT_NULL(exec_param));
        OZ (check_raw_expr_in_forall(exec_param, stmt.get_ident(), need_modify, is_array_binding));
        if (OB_SUCC(ret)) {
          if (need_modify) {
            OZ (need_modify_exprs.push_back(i));
          }
          OZ (sql_stmt->get_array_binding_params().push_back(params.at(i)));
        }
        can_array_binding &= is_array_binding;
      }
      if (OB_SUCC(ret) && 0 == need_modify_exprs.count()) {
        ret = OB_ERR_FORALL_DML_WITHOUT_BULK;
        LOG_WARN("PLS-00435: DML statement without BULK In-BIND cannot be used inside FORALL.", K(ret), K(params.count()));
      }
      if (OB_FAIL(ret)) {
      } else if (stmt.is_values_bound() || stmt.is_indices_bound() || stmt.is_indices_with_between_bound()) {
        stmt.set_binding_array(false);
      } else if (can_array_binding) {
        stmt.set_binding_array(true);
      }
      if (OB_SUCC(ret) && StmtType::T_SELECT == sql_stmt->get_stmt_type()) {
        ret = OB_ERR_FORALL_BULK_TOGETHER;
        LOG_WARN("PLS-00432: implementation restriction: cannot use FORALL and BULK COLLECT INTO together in SELECT statements", K(ret));
      }
      if (OB_SUCC(ret) && stmt.get_binding_array()) {
        OZ (modify_raw_expr_in_forall(stmt, func, *sql_stmt, need_modify_exprs));
        if (OB_SUCC(ret)) {
          stmt.set_sql_stmt(sql_stmt);
        }
      }
    }
  }
  return ret;
}

int ObPLResolver::resolve_forall_collection_and_check(const ObStmtNodeTree *coll_node,
                                                      ObPLForAllStmt *stmt,
                                                      ObPLFunctionAST &func,
                                                      ObIArray<ObObjAccessIdx> &access_idxs)
{
  int ret = OB_SUCCESS;
  ObArray<ObObjAccessIdent> obj_access_idents;
  const ObUserDefinedType *user_type = NULL;
  CK (OB_NOT_NULL(coll_node));
  CK (OB_NOT_NULL(current_block_));
  CK (T_SP_OBJ_ACCESS_REF == coll_node->type_);
  OZ (resolve_obj_access_idents(*coll_node, obj_access_idents, func));
  for (int64_t i = 0; OB_SUCC(ret) && i < obj_access_idents.count(); ++i) {
    OZ (resolve_access_ident(obj_access_idents.at(i),
                             current_block_->get_namespace(),
                             expr_factory_,
                             &resolve_ctx_.session_info_,
                             access_idxs,
                             func));
  }
  if (OB_FAIL(ret)) {
  } else if (!ObObjAccessIdx::get_final_type(access_idxs).is_collection_type()) {
    ret = OB_ERR_SHOULD_COLLECTION_TYPE;
    LOG_WARN("PLS-00668: Type of expression should be a collection type",
             K(ret), K(ObObjAccessIdx::get_final_type(access_idxs)));
  } else {
    OZ (current_block_->get_namespace().get_pl_data_type_by_id(
      ObObjAccessIdx::get_final_type(access_idxs).get_user_type_id(), user_type));
    CK (OB_NOT_NULL(user_type));
  }
#ifdef OB_BUILD_ORACLE_PL
  if (OB_SUCC(ret)) {
    const ObCollectionType *coll_type = static_cast<const ObCollectionType*>(user_type);
    CK (OB_NOT_NULL(coll_type));
    if (OB_FAIL(ret)) {
    } else if (stmt->is_values_bound() && !coll_type->get_element_type().is_pl_integer_type()) {
      ret = OB_ERR_ASSOC_ELEM_TYPE;
      LOG_WARN(
        "PLS-00667: Element type of associative array should be pls_integer or binary_integer",
        K(ret), K(coll_type->get_element_type()));
    } else if (coll_type->is_associative_array_type()) {
      const ObAssocArrayType *assoc_type = static_cast<const ObAssocArrayType*>(user_type);
      CK (OB_NOT_NULL(assoc_type));
      if (OB_FAIL(ret)) {
      } else if (!assoc_type->get_index_type().is_pl_integer_type()) {
        ret = OB_ERR_UNSUPPORTED_TABLE_INDEX_TYPE;
        LOG_WARN("PLS-00315: Implementation restriction: unsupported table index type",
                 K(ret), K(assoc_type->get_index_type()));
      }
    }
  }
#endif
  return ret;
}

int ObPLResolver::build_forall_index_expr(ObPLForAllStmt *stmt,
                                          ObRawExprFactory &expr_factory,
                                          ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObConstRawExpr *c_expr = NULL;
  ObExprResType res_type;
  ObObjParam val;
  ObPLDataType data_type;
  OZ (expr_factory.create_raw_expr(T_QUESTIONMARK, c_expr));
  CK (OB_NOT_NULL(c_expr));
  OX (val.set_unknown(stmt->get_ident()));
  OX (val.set_param_meta());
  OX (c_expr->set_value(val));
  OZ (build_pl_integer_type(PL_PLS_INTEGER, data_type));
  CK (OB_NOT_NULL(data_type.get_data_type()));
  OX (res_type.set_meta(data_type.get_data_type()->get_meta_type()));
  OX (res_type.set_accuracy(data_type.get_data_type()->get_accuracy()));
  OX (c_expr->set_result_type(res_type));
  OZ (c_expr->add_flag(IS_DYNAMIC_PARAM));
  OZ (c_expr->extract_info());
  OX (expr = c_expr);
  return ret;
}

int ObPLResolver::build_collection_value_expr(ObIArray<ObObjAccessIdx> &access_idxs,
                                              ObPLFunctionAST &func,
                                              ObPLForAllStmt *stmt,
                                              ObPLDataType *expected_type)
{
  int ret = OB_SUCCESS;
  ObObjAccessIdx access_idx;
  ObRawExpr *expr = NULL;

  new (&access_idx) ObObjAccessIdx(*expected_type,
                                   ObObjAccessIdx::AccessType::IS_LOCAL,
                                   ObString(""),
                                   *expected_type,
                                   stmt->get_ident());
  OZ (access_idxs.push_back(access_idx));
  OZ (make_var_from_access(access_idxs,
                           expr_factory_,
                           &(resolve_ctx_.session_info_),
                           &(resolve_ctx_.schema_guard_),
                           current_block_->get_namespace(),
                           expr));
  CK (OB_NOT_NULL(expr));
  if (OB_SUCC(ret) && expr->is_obj_access_expr()) {
    OZ (func.add_obj_access_expr(expr));
  }
  OZ (func.add_expr(expr));
  OX (access_idxs.pop_back());
  OX (stmt->set_value(func.get_expr_count() - 1));
  return ret;
}

int ObPLResolver::build_collection_property_expr(const ObString &property_name,
                                                 ObIArray<ObObjAccessIdx> &access_idxs,
                                                 ObPLFunctionAST &func,
                                                 ObPLForAllStmt *stmt,
                                                 ObPLDataType *expected_type)
{
  int ret = OB_SUCCESS;
  const ObUserDefinedType *user_type = NULL;
  uint64_t package_id = OB_INVALID_ID;
  int64_t var_idx = OB_INVALID_INDEX;
  ObObjAccessIdx access_idx;
  ObObjAccessIdent access_ident;
  ObPLDataType type;
  ObRawExpr *expr = NULL;
  ObRawExpr *index_expr = NULL;
  const ObDataType *data_type = NULL;
  OZ (current_block_->get_namespace().get_pl_data_type_by_id(
    ObObjAccessIdx::get_final_type(access_idxs).get_user_type_id(), user_type));
  CK (OB_NOT_NULL(user_type));
  OX (access_ident.access_name_ = property_name);
  OZ (build_forall_index_expr(stmt, expr_factory_, index_expr));
  CK (OB_NOT_NULL(index_expr));
  OX (access_ident.params_.push_back(std::make_pair(index_expr, 0)));
  OZ (current_block_->get_namespace().find_sub_attr_by_name(
    *user_type,
    access_ident,
    resolve_ctx_.session_info_,
    expr_factory_,
    func,
    access_idx,
    type,
    package_id,
    var_idx));
  if (OB_SUCC(ret)
      && (0 == property_name.case_compare("next") || 0 == property_name.case_compare("exists"))) {
    if (user_type->is_associative_array_type()) {
      ObObjAccessIdx tmp_access_idx;
      ObPLAssocIndexRawExpr *index_expr = NULL;
      OZ (build_collection_attribute_access(expr_factory_,
                                            &(resolve_ctx_.session_info_),
                                            current_block_->get_namespace(),
                                            func,
                                            *user_type,
                                            access_idxs,
                                            OB_INVALID_INDEX,
                                            access_ident.params_.at(0).first,
                                            tmp_access_idx));
      CK (OB_NOT_NULL(index_expr =
            static_cast<ObPLAssocIndexRawExpr *>(tmp_access_idx.get_sysfunc_)));
      OX (index_expr->set_out_of_range_set_err(false));
      OX (index_expr->set_parent_type(
        0 == property_name.case_compare("next") ?
          parent_expr_type::EXPR_NEXT : parent_expr_type::EXPR_EXISTS));
      OX (access_idx.get_sysfunc_ = tmp_access_idx.get_sysfunc_);
    } else {
      access_idx.get_sysfunc_ = access_ident.params_.at(0).first;
    }
  }
  OZ (access_idxs.push_back(access_idx));
  OZ (make_var_from_access(access_idxs,
                           expr_factory_,
                           &(resolve_ctx_.session_info_),
                           &(resolve_ctx_.schema_guard_),
                           current_block_->get_namespace(),
                           expr));
  if (OB_SUCC(ret) && expr->is_obj_access_expr()) {
    OZ (func.add_obj_access_expr(expr));
  }
  if (property_name.case_compare("next")
      || property_name.case_compare("first")
      || property_name.case_compare("last")) {
    CK (OB_NOT_NULL(data_type = expected_type->get_data_type()));
    OZ (ObRawExprUtils::build_column_conv_expr(&resolve_ctx_.session_info_,
                                               expr_factory_,
                                               data_type->get_obj_type(),
                                               data_type->get_collation_type(),
                                               data_type->get_accuracy_value(),
                                               true,
                                               NULL, /*"db_name"."tb_name"."col_name"*/
                                               &expected_type->get_type_info(),
                                               expr,
                                               true /*is_in_pl*/));
  }
  OZ (func.add_expr(expr));
  OX (access_idxs.pop_back());

  return ret;
}

int ObPLResolver::resolve_forall_collection_node(const ObStmtNodeTree *collection_node,
                                                 ObPLForAllStmt *stmt,
                                                 ObPLFunctionAST &func,
                                                 ObPLDataType *expected_type)
{
  int ret = OB_SUCCESS;
  ObArray<ObObjAccessIdx> access_idxs;

  CK (OB_NOT_NULL(stmt));
  OZ (resolve_forall_collection_and_check(collection_node, stmt, func, access_idxs));

#define BUILD_PROPERTY_EXPR(name) \
do { \
  OZ (build_collection_property_expr(ObString(#name), access_idxs, func, stmt, expected_type)); \
  OX (stmt->set_##name(func.get_expr_count() - 1)); \
} while(0)

  if (OB_SUCC(ret)
      && (stmt->is_indices_bound() || stmt->is_values_bound())) {
    BUILD_PROPERTY_EXPR(first);
    BUILD_PROPERTY_EXPR(last);
  }
  BUILD_PROPERTY_EXPR(next);

  if (OB_FAIL(ret)) {
  } else if (stmt->is_values_bound()) {
    OZ (build_collection_value_expr(access_idxs, func, stmt, expected_type));
  } else if (stmt->is_indices_with_between_bound()) {
    BUILD_PROPERTY_EXPR(exists);
  }

#undef BUILD_PROPERTY_EXPR

  return ret;
}

int ObPLResolver::resolve_indices_bound_clause(const ObStmtNodeTree &indices_of_node,
                                               ObPLForAllStmt *stmt,
                                               ObPLFunctionAST &func,
                                               ObPLDataType *expected_type)
{
  int ret = OB_SUCCESS;
  const ObStmtNodeTree* collection_node = NULL;
  const ObStmtNodeTree* between_node = NULL;
  CK (OB_NOT_NULL(stmt));
  CK (T_SP_INDICES_OF_CALUSE == indices_of_node.type_);
  CK (2 == indices_of_node.num_child_);
  CK (OB_NOT_NULL(collection_node = indices_of_node.children_[0]));
  OX (between_node = indices_of_node.children_[1]);
  if (OB_SUCC(ret) && OB_NOT_NULL(between_node)) {
    OZ (resolve_normal_bound_clause(*between_node, stmt, func, expected_type));
  }
  OX (stmt->set_bound_type(OB_NOT_NULL(between_node)
                           ? ObPLForLoopStmt::BoundType::INDICES_WITH_BETWEEN
                           : ObPLForLoopStmt::BoundType::INDICES));
  OZ (resolve_forall_collection_node(collection_node, stmt, func, expected_type));
  return ret;
}

int ObPLResolver::resolve_values_bound_clause(const ObStmtNodeTree &values_of_node,
                                              ObPLForAllStmt *stmt,
                                              ObPLFunctionAST &func,
                                              ObPLDataType *expected_type)
{
  int ret = OB_SUCCESS;
  const ObStmtNodeTree* collection_node = NULL;
  CK (OB_NOT_NULL(stmt));
  CK (T_SP_VALUES_OF_CALUSE == values_of_node.type_);
  CK (1 == values_of_node.num_child_);
  CK (OB_NOT_NULL(collection_node = values_of_node.children_[0]));
  OX (stmt->set_bound_type(ObPLForLoopStmt::BoundType::VALUES));
  OZ (resolve_forall_collection_node(collection_node, stmt, func, expected_type));
  return ret;
}

int ObPLResolver::resolve_normal_bound_clause(const ObStmtNodeTree &bound_node,
                                              ObPLForAllStmt *stmt,
                                              ObPLFunctionAST &func,
                                              ObPLDataType *expected_type)
{
  int ret = OB_SUCCESS;
  const ObStmtNodeTree* lower_node = NULL;
  const ObStmtNodeTree* upper_node = NULL;
  ObRawExpr *lower_expr = NULL;
  ObRawExpr *upper_expr = NULL;
  CK (OB_NOT_NULL(stmt));
  CK (T_SP_BOUND_CLAUSE == bound_node.type_);
  CK (2 == bound_node.num_child_);
  CK (OB_NOT_NULL(bound_node.children_));
  CK (OB_NOT_NULL(lower_node = bound_node.children_[0]));
  CK (OB_NOT_NULL(upper_node = bound_node.children_[1]));
  OZ (resolve_expr(
    lower_node, static_cast<ObPLCompileUnitAST&>(func), lower_expr,
    combine_line_and_col(lower_node->stmt_loc_), true, expected_type));
  OZ (resolve_expr(
    upper_node, static_cast<ObPLCompileUnitAST&>(func), upper_expr,
    combine_line_and_col(upper_node->stmt_loc_), true, expected_type));
  OX (stmt->set_lower(func.get_expr_count() - 2));
  OX (stmt->set_upper(func.get_expr_count() - 1));
  OX (stmt->set_bound_type(ObPLForLoopStmt::BoundType::NORMAL));
  return ret;
}

int ObPLResolver::resolve_forall_bound_clause(const ObStmtNodeTree &parse_tree,
                                              ObPLForAllStmt *stmt,
                                              ObPLFunctionAST &func,
                                              ObPLDataType *expected_type)
{
  int ret = OB_SUCCESS;
  switch (parse_tree.type_) {
    case T_SP_BOUND_CLAUSE: {
      OZ (resolve_normal_bound_clause(parse_tree, stmt, func, expected_type));
    } break;
    case T_SP_INDICES_OF_CALUSE: {
      OZ (resolve_indices_bound_clause(parse_tree, stmt, func, expected_type));
    } break;
    case T_SP_VALUES_OF_CALUSE: {
      OZ (resolve_values_bound_clause(parse_tree, stmt, func, expected_type));
    } break;
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected bound clause type", K(ret), K(parse_tree.type_));
    }
  }
  return ret;
}

int ObPLResolver::resolve_forall(const ObStmtNodeTree *parse_tree,
                                 ObPLForAllStmt *stmt,
                                 ObPLFunctionAST &func)
{
  int ret = OB_SUCCESS;

  CK (OB_NOT_NULL(parse_tree),
      OB_NOT_NULL(stmt),
      3 == parse_tree->num_child_,
      OB_NOT_NULL(parse_tree->children_[0]),
      OB_NOT_NULL(parse_tree->children_[1]));

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(parse_tree->children_[2])) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("for all dml must be not null", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "null dml in forall");
  } else {
    const ObStmtNodeTree* index_node = parse_tree->children_[0];
    const ObStmtNodeTree* bound_node = parse_tree->children_[1];
    const ObStmtNodeTree* body_node = parse_tree->children_[2];
    bool save_exception = static_cast<bool>(parse_tree->value_);
    ObPLDataType data_type;
    ObPLStmtBlock *body_block = NULL;
    ObString index_name;

    // 解析svae_exception
    OX (stmt->set_save_exception(save_exception));

    // 解析index
    OZ (build_pl_integer_type(PL_PLS_INTEGER, data_type));
    OZ (make_block(func, current_block_, body_block, true));
    CK (OB_NOT_NULL(body_block));
    OZ (resolve_ident(index_node, index_name));
    OZ (body_block->get_namespace().add_symbol(index_name, data_type, NULL, true));
    OX (stmt->set_ident(func.get_symbol_table().get_count() - 1));

    // 解析bound
    OZ (resolve_forall_bound_clause(*bound_node, stmt, func, &data_type));

    // 解析body
    OZ (resolve_stmt_list(body_node, body_block, func));
    OX (stmt->set_body(body_block));

    // for ArrayBinding优化
    OZ (stmt->create_tab_to_subtab());

    // 检查sql语句以及Index使用的合法性
    OZ (check_forall_sql_and_modify_params(*stmt, func));
  }
  return ret;
}

int ObPLResolver::resolve_repeat(const ObStmtNodeTree *parse_tree, ObPLRepeatStmt *stmt, ObPLFunctionAST &func)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(parse_tree)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("parse_tree is NULL", K(parse_tree), K(ret));
  } else {
    const ObStmtNodeTree *expr_node = parse_tree->children_[1];
    const ObStmtNodeTree *body_node = parse_tree->children_[0];
    if (OB_FAIL(resolve_cond_loop(expr_node, body_node, stmt, func))) {
      LOG_WARN("failed to resolve while", K(parse_tree), K(stmt), K(ret));
    }
  }
  return ret;
}

int ObPLResolver::resolve_loop(const ObStmtNodeTree *parse_tree, ObPLLoopStmt *stmt, ObPLFunctionAST &func)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(parse_tree) || OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("parse_tree is NULL", K(parse_tree), K(stmt), K(ret));
  } else {
    //解析body
    if (OB_ISNULL(parse_tree->children_[0])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Invalid loop body", K(parse_tree->children_[0]->type_), K(ret));
    } else if (T_SP_PROC_STMT_LIST != parse_tree->children_[0]->type_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Invalid loop body", K(parse_tree->children_[0]->type_), K(ret));
    } else {
      ObPLStmtBlock *body_block = NULL;
      if (OB_FAIL(resolve_stmt_list(parse_tree->children_[0], body_block, func))) {
        LOG_WARN("failed to resolve stmt list", K(parse_tree->children_[0]->type_), K(ret));
      } else {
        stmt->set_body(body_block);
      }
    }
  }
  return ret;
}

int ObPLResolver::resolve_return(const ObStmtNodeTree *parse_tree, ObPLReturnStmt *stmt, ObPLFunctionAST &func)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(parse_tree));
  CK (OB_NOT_NULL(stmt));

  if (OB_SUCC(ret) && !func.is_function()) {
    if (lib::is_mysql_mode()) {
      ret = OB_ER_SP_BADRETURN;
      LOG_WARN("RETURN is only allowed in a FUNCTION", K(ret));
      LOG_USER_ERROR(OB_ER_SP_BADRETURN);
    }
  }

  if (OB_SUCC(ret)) {
    ObRawExpr *expr = NULL;
    ObStmtNodeTree *expr_node = parse_tree->children_[0];
#ifdef OB_BUILD_ORACLE_PL
    if (func.is_udt_cons()) {
      if (OB_NOT_NULL(expr_node)) {
        ret = OB_ERR_CONS_HAS_RET_NODE;
        LOG_WARN("udt constructor should not have return expr", K(ret));
        LOG_USER_ERROR(OB_ERR_CONS_HAS_RET_NODE);
      } else {
        ParseNode *child = NULL;
        ParseNode *parent = NULL;
        if (NULL == (child = new_terminal_node(&resolve_ctx_.allocator_, T_IDENT))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to alloc memory", K(ret));
        } else if (NULL == (parent = new_non_terminal_node(&resolve_ctx_.allocator_, T_OBJ_ACCESS_REF, 2, child, NULL))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to alloc memory", K(ret));
        } else {
          child->str_value_ = "SELF";
          child->str_len_ = 4;
          child->raw_text_ = "SELF";
          child->text_len_ = 4;
          expr_node = parent;
        }
      }
    }
#endif
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_ISNULL(expr_node)) {
      if (lib::is_mysql_mode()
          || (func.is_function() && !func.get_pipelined())) {
        ret = OB_ERR_RETURN_VALUE_REQUIRED;
        LOG_WARN("not allow return expr node is null in function", K(ret), K(expr_node));
      }
    } else if (!func.is_function()) {
      ret = OB_ER_SP_BADRETURN;
      LOG_WARN("not allow return expr node is not null in procedure", K(ret), K(func.is_function()));
    } else if (func.get_pipelined()) {
      ret = OB_ERR_RETURN_EXPR_ILLEGAL;
      LOG_WARN("PLS-00633: RETURN statement in a pipelined function cannot contain an expression",
               K(ret), K(func));
    } else if (OB_FAIL(resolve_expr(expr_node, func, expr,
                                    combine_line_and_col(expr_node->stmt_loc_),
                                    true, &func.get_ret_type()))) {
      LOG_WARN("failed to resolve expr", K(expr_node), K(ret));
    } else {
      stmt->set_ret(func.get_expr_count() - 1);
      bool is_ref_cursor_type = false;
      OZ (is_return_ref_cursor_type(expr, is_ref_cursor_type));
      OX (stmt->set_is_ref_cursor_type(is_ref_cursor_type));
      LOG_DEBUG("return ref cursor type: ", K(is_ref_cursor_type));
    }
  }

  return ret;
}

int ObPLResolver::check_and_record_stmt_type(ObPLFunctionAST &func,
                                             sql::ObSPIService::ObSPIPrepareResult &prepare_result)
{
  // FUNCTION 中不允许含有的SQL语句包括 PS, DDL, SELECT WITHOUT INTO
  int ret = OB_SUCCESS;
  stmt::StmtType type = prepare_result.type_;
  bool in_tg = resolve_ctx_.session_info_.is_for_trigger_package();
  switch (type) {
    case stmt::T_PREPARE:
    case stmt::T_EXECUTE:
    case stmt::T_DEALLOCATE: {
      if (func.is_function() || in_tg) {
        ret = OB_ER_STMT_NOT_ALLOWED_IN_SF_OR_TRG;
        LOG_WARN("Dynamic SQL is not allowed in stored function", K(ret));
        LOG_USER_ERROR(OB_ER_STMT_NOT_ALLOWED_IN_SF_OR_TRG, "Dynamic SQL");
      } else {
        func.set_contain_dynamic_sql();
      }
      break;
    }
    case stmt::T_SELECT:
    case stmt::T_EXPLAIN:
    case stmt::T_SHOW_COLUMNS:
    case stmt::T_SHOW_TABLES:
    case stmt::T_SHOW_DATABASES:
    case stmt::T_SHOW_TABLE_STATUS:
    case stmt::T_SHOW_SERVER_STATUS:
    case stmt::T_SHOW_VARIABLES:
    case stmt::T_SHOW_SCHEMA:
    case stmt::T_SHOW_CREATE_DATABASE:
    case stmt::T_SHOW_CREATE_TABLE:
    case stmt::T_SHOW_CREATE_VIEW:
    case stmt::T_SHOW_WARNINGS:
    case stmt::T_SHOW_ERRORS:
    case stmt::T_SHOW_GRANTS:
    case stmt::T_SHOW_CHARSET:
    case stmt::T_SHOW_COLLATION:
    case stmt::T_SHOW_PARAMETERS:
    case stmt::T_SHOW_INDEXES:
    case stmt::T_SHOW_PROCESSLIST:
    case stmt::T_SHOW_TABLEGROUPS:
    case stmt::T_HELP:
    case stmt::T_SHOW_RECYCLEBIN:
    case stmt::T_SHOW_RESTORE_PREVIEW:
    case stmt::T_SHOW_TENANT:
    case stmt::T_SHOW_SEQUENCES: {
      if (0 == prepare_result.into_exprs_.count()) {
        if (func.is_function() || in_tg) {
          ret = OB_ER_SP_NO_RETSET;
          LOG_WARN("Not allowed to return a result set in pl function", K(ret));
          if (in_tg) {
            LOG_USER_ERROR(OB_ER_SP_NO_RETSET, "trigger");
          } else {
            LOG_USER_ERROR(OB_ER_SP_NO_RETSET, "function");
          }
        } else {
          func.set_multi_results();
        }
      }
      break;
    }
    default: {
      if (ObStmt::is_ddl_stmt(type, false)
          || ObStmt::is_dcl_stmt(type)
          || ObStmt::is_tcl_stmt(type)) {
        if (func.is_function() || in_tg) {
          ret = OB_ER_COMMIT_NOT_ALLOWED_IN_SF_OR_TRG;
          LOG_WARN("DDL SQL is not allowed in stored function", K(ret));
          LOG_USER_ERROR(OB_ER_COMMIT_NOT_ALLOWED_IN_SF_OR_TRG);
        } else {
          func.set_has_commit_or_rollback();
        }
      }
    }
    break;
  }
  return ret;
}

int ObPLResolver::replace_plsql_line(
  ObIAllocator &allocator, const ObStmtNodeTree *node, ObString &sql, ObString &new_sql)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(node)) {
    if (T_PLSQL_VARIABLE_IDENTIFIER == node->type_) {
      int64_t plsql_line = node->int32_values_[0];
      int64_t first_column = node->int16_values_[2];
      int64_t last_column = node->int16_values_[3];

      CK (first_column < sql.length() && last_column < sql.length());

      if (OB_SUCC(ret)
          && 12 == (last_column - first_column + 1)
          && 0 == ObString(12, sql.ptr() + first_column - 1).case_compare("$$PLSQL_LINE")) {
        char buffer[13]; // $$PLSQL_LINE
        int64_t n = snprintf(buffer, 13, "%012ld", plsql_line);
        if (12 == n) {
          if (new_sql.empty()) {
            OZ (ob_write_string(allocator, sql, new_sql, true/*append \0*/));
          }
          // NOTICE: copy 12 bytes for ignore \0 tail.
          OX (MEMCPY(new_sql.ptr() + first_column - 1, buffer, 12));
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("plsql_line format line exceed buffer size 13!",
                   K(ret), K(plsql_line), K(n), K(sql), K(new_sql), K(buffer));
        }
      }
      LOG_DEBUG("replace plsql line",
                K(ObString(last_column - first_column + 1, sql.ptr() + first_column - 1)),
                K(plsql_line),
                K(first_column),
                K(last_column),
                K(sql),
                K(new_sql));

    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < node->num_child_; ++i) {
        if (OB_NOT_NULL(node->children_[i])) {
          OZ (replace_plsql_line(allocator, node->children_[i], sql, new_sql));
        }
      }
    }
  }
  return ret;
}

int ObPLResolver::resolve_static_sql(const ObStmtNodeTree *parse_tree, ObPLSql &static_sql, ObPLInto &static_into, bool is_cursor, ObPLFunctionAST &func)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(parse_tree) || OB_ISNULL(current_block_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("argument is NULL", K(parse_tree), K(current_block_), K(ret));
  } else {
    pl::ObRecordType *record_type = NULL;
    CK (T_SQL_STMT == parse_tree->type_ || T_TRANSACTION == parse_tree->type_);
    if (OB_SUCC(ret)
        && 1 == parse_tree->num_child_
        && NULL != parse_tree->children_[0]
        && T_SELECT == parse_tree->children_[0]->type_
        && (NULL == ObResolverUtils::get_select_into_node(*parse_tree->children_[0])
                || is_cursor)) {
      if (OB_ISNULL(record_type = static_cast<ObRecordType*>(resolve_ctx_.allocator_.alloc(sizeof(ObRecordType))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc memory", K(ret));
      } else {
        record_type = new(record_type)ObRecordType();
        record_type->set_type_from(PL_TYPE_ATTR_ROWTYPE);
      }
    }
    if (OB_SUCC(ret)
        && 1 == parse_tree->num_child_
        && NULL != parse_tree->children_[0]
        && T_DIAGNOSTICS == parse_tree->children_[0]->type_
        && 0 == parse_tree->children_[0]->children_[1]->value_
        && !current_block_->in_handler()) {
      ret = OB_ERR_GET_STACKED_DIAGNOSTICS;
      LOG_WARN("GET STACKED DIAGNOSTICS when handler not active", K(ret));
    }
    if (OB_SUCC(ret)) {
      sql::ObSPIService::ObSPIPrepareResult prepare_result;
      ObString name;
      prepare_result.record_type_ = record_type;
      prepare_result.tg_timing_event_ = 
                            static_cast<TgTimingEvent>(resolve_ctx_.params_.tg_timing_event_);
      question_mark_cnt_ = parse_tree->value_; // 更新解析到当前语句时question mark的数量(包含当前语句)
      ObString new_sql;
      ObString old_sql(parse_tree->str_value_);
      OZ (replace_plsql_line(resolve_ctx_.allocator_, parse_tree, old_sql, new_sql));
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ObSPIService::spi_prepare(resolve_ctx_.allocator_,
                                            resolve_ctx_.session_info_,
                                            resolve_ctx_.sql_proxy_,
                                            resolve_ctx_.schema_guard_,
                                            expr_factory_,
                                            new_sql.empty() ?
                                              parse_tree->str_value_ : new_sql.ptr(),
                                            is_cursor,
                                            &current_block_->get_namespace(),
                                            prepare_result))) {
        if (OB_ERR_TOO_BIG_DISPLAYWIDTH == ret) {
          LOG_WARN("%s is too big, max is 65", K(parse_tree->str_value_), K(ret));
          LOG_USER_ERROR(OB_ERR_TOO_BIG_DISPLAYWIDTH, parse_tree->str_value_, OB_MAX_BIT_LENGTH);
        } else {
          LOG_WARN("failed to prepare stmt", K(ret));
        }
      } else if (is_mysql_mode()
                 && OB_FAIL(check_and_record_stmt_type(func, prepare_result))) {
        LOG_WARN("sql stmt not support in pl function", K(parse_tree->str_value_), K(prepare_result.type_), K(ret));
      } else if (stmt::T_SELECT == prepare_result.type_
                 && 0 == prepare_result.into_exprs_.count()
                 && !is_cursor
                 && is_oracle_mode()) {
        ret = OB_ERR_INTO_CLAUSE_EXPECTED;
        LOG_WARN("PLS-00428: an INTO clause is expected in this SELECT statement", K(ret));
      } else if (is_mysql_mode() && stmt::T_END_TRANS == prepare_result.type_ 
                                 && func.is_function()) {
        name.assign_ptr("COMMIT", 6);
        if (0 != name.case_compare(parse_tree->str_value_)) {
          name.assign_ptr("ROLLBACK", 8);
        }
        ret = OB_ERR_STMT_NOT_ALLOW_IN_MYSQL_PROCEDRUE;
        LOG_WARN("%s is not allowed in stored procedure. ", K(name), K(ret));
        LOG_USER_ERROR(OB_ERR_STMT_NOT_ALLOW_IN_MYSQL_PROCEDRUE, name.length(), name.ptr());
      } else if (is_mysql_mode() && stmt::T_LOAD_DATA == prepare_result.type_) {
        name.assign_ptr("LOAD DATA", 9);
        ret = OB_ERR_STMT_NOT_ALLOW_IN_MYSQL_PROCEDRUE;
        LOG_WARN("%s is not allowed in stored procedure. ", K(name), K(ret));
        LOG_USER_ERROR(OB_ERR_STMT_NOT_ALLOW_IN_MYSQL_PROCEDRUE, name.length(), name.ptr());
      } else if (is_mysql_mode() && stmt::T_LOCK_TABLE == prepare_result.type_) {
        name.assign_ptr("LOCK TABLE", 10);
        ret = OB_ERR_STMT_NOT_ALLOW_IN_MYSQL_PROCEDRUE;
        LOG_WARN("%s is not allowed in stored procedure. ", K(name), K(ret));
        LOG_USER_ERROR(OB_ERR_STMT_NOT_ALLOW_IN_MYSQL_PROCEDRUE, name.length(), name.ptr());
      } else if (OB_FAIL(func.add_sql_exprs(prepare_result.exec_params_))) {
        LOG_WARN("failed to set precalc exprs", K(prepare_result.exec_params_), K(ret));
      } else if (OB_FAIL(func.add_sql_exprs(prepare_result.into_exprs_))) {
        LOG_WARN("failed to set precalc exprs", K(prepare_result.into_exprs_), K(ret));
      } else { /*do nothing*/ }

      if (OB_SUCC(ret) && lib::is_oracle_mode()) {
        if ((stmt::T_SELECT == prepare_result.type_
             && func.get_compile_flag().compile_with_rnds())
            || (ObStmt::is_write_stmt(prepare_result.type_, false)
                && func.get_compile_flag().compile_with_wnds())) {
          ret = OB_ERR_SUBPROGRAM_VIOLATES_PRAGMA;
          LOG_WARN("PLS-00452: Subprogram 'string' violates its associated pragma",
                   K(ret), K(prepare_result.type_), K(func.get_compile_flag()));
        }
      }
      if (OB_SUCC(ret)) {
        if (prepare_result.for_update_) {
          func.set_modifies_sql_data();
        } else if (stmt::T_SELECT == prepare_result.type_) {
          if (!func.is_modifies_sql_data()) {
            func.set_reads_sql_data();
          }
        } else if (ObStmt::is_dml_write_stmt(prepare_result.type_) ||
                   ObStmt::is_savepoint_stmt(prepare_result.type_) ||
                   ObStmt::is_tcl_stmt(prepare_result.type_)) {
          func.set_modifies_sql_data();
        } else if (!func.is_reads_sql_data() && !func.is_modifies_sql_data()) {
          func.set_contains_sql();
        }
      }

      if (OB_SUCC(ret)) {
        ObArray<int64_t> idxs;
        for (int64_t i = func.get_expr_count() - prepare_result.exec_params_.count() - prepare_result.into_exprs_.count();
            OB_SUCC(ret) && i < func.get_expr_count() - prepare_result.into_exprs_.count();
            ++i) {
          if (OB_FAIL(idxs.push_back(i))) {
            LOG_WARN("push back error", K(prepare_result.into_exprs_), K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(static_sql.set_params(idxs))) {
            LOG_WARN("failed to set params", K(prepare_result.exec_params_), K(idxs), K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          const ObPLSymbolTable *table = current_block_->get_symbol_table();
          CK (OB_NOT_NULL(table));
          for (int64_t i = 0; OB_SUCC(ret) && i < prepare_result.exec_params_.count(); ++i) {
            const ObRawExpr *expr = prepare_result.exec_params_.at(i);
            const ObConstRawExpr *const_expr = NULL;
            const ObPLVar *var = NULL;
            CK (OB_NOT_NULL(expr));
            if (OB_SUCC(ret) && T_QUESTIONMARK == expr->get_expr_type()) {
              CK (OB_NOT_NULL(const_expr = static_cast<const ObConstRawExpr *>(expr)));
              CK (OB_NOT_NULL(var = table->get_symbol(const_expr->get_value().get_unknown())));
              if (OB_SUCC(ret) && var->get_name().prefix_match(ANONYMOUS_ARG)) {
                (const_cast<ObPLVar*>(var))->set_name(ANONYMOUS_SQL_ARG);
              }
            }
          }
        }
        idxs.reset();
        for (int64_t i = func.get_expr_count() - prepare_result.into_exprs_.count();
             OB_SUCC(ret) && i < func.get_expr_count();
             ++i) {
          if (OB_FAIL(idxs.push_back(i))) {
            LOG_WARN("push back error", K(prepare_result.into_exprs_), K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          if (prepare_result.is_bulk_) {
            static_into.set_bulk();
          }
          if (OB_FAIL(static_into.set_into(idxs, current_block_->get_namespace(), prepare_result.into_exprs_))) {
            LOG_WARN("failed to set into exprs", K(prepare_result.into_exprs_), K(idxs), K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          static_sql.set_ps_sql(prepare_result.ps_sql_, prepare_result.type_);
          static_sql.set_sql(prepare_result.route_sql_);
          static_sql.set_for_update(prepare_result.for_update_);
          static_sql.set_hidden_rowid(prepare_result.has_hidden_rowid_);
          static_sql.set_link_table(prepare_result.has_link_table_);
        }
      }

      //检查Bulk合法性
      if (OB_SUCC(ret) && !prepare_result.into_exprs_.empty()) {
        if (OB_ISNULL(prepare_result.into_exprs_.at(0))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expr is NULL", K(prepare_result.into_exprs_), K(ret));
        } else if (prepare_result.into_exprs_.at(0)->is_obj_access_expr()) {
          const ObObjAccessRawExpr *access_expr = static_cast<const ObObjAccessRawExpr*>(prepare_result.into_exprs_.at(0));
          ObPLDataType type;
          OZ (access_expr->get_final_type(type));
          OZ (static_into.check_into(
            func, current_block_->get_namespace(), prepare_result.is_bulk_));
        } else {
          OZ (static_into.check_into(func, current_block_->get_namespace(), prepare_result.is_bulk_));
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(func.add_dependency_objects(prepare_result.ref_objects_))) {
          LOG_WARN("add dependency tables failed", K(ret));
        } else if (OB_FAIL(static_sql.set_ref_objects(prepare_result.ref_objects_))) {
          LOG_WARN("set ref objects failed", K(ret));
        } else {
          static_sql.set_row_desc(record_type);
        }
      }
    }
  }
  return ret;
}

int ObPLResolver::resolve_sql(const ObStmtNodeTree *parse_tree, ObPLSqlStmt *stmt, ObPLFunctionAST &func)
{
  int ret = OB_SUCCESS;
  CK(OB_NOT_NULL(stmt));
  OZ (resolve_static_sql(parse_tree, *stmt, *stmt, false/*not cursor*/, func));
  OZ (func.get_sql_stmts().push_back(stmt));
  return ret;
}

int ObPLResolver::resolve_using(const ObStmtNodeTree *using_node,
                                ObIArray<InOutParam> &using_params,
                                ObPLFunctionAST &func)
{
  int ret = OB_SUCCESS;
  if (NULL != using_node) {
    ObRawExpr *expr = NULL;
    int64_t out_idx = OB_INVALID_INDEX;
    const ObStmtNodeTree *using_param = NULL;
    ObPLRoutineParamMode using_param_mode = PL_PARAM_INVALID;
    for (int64_t i = 0; OB_SUCC(ret) && i < using_node->num_child_; ++i) {
      out_idx = OB_INVALID_INDEX;
      using_param = using_node->children_[i];
      if (OB_ISNULL(using_param)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("using param node is NULL", K(i), K(using_node->type_), K(ret));
      } else if (1 != using_param->num_child_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("using param node must have only 1 child", K(i), K(using_node->type_), K(using_param->num_child_), K(ret));
      } else if (OB_FAIL(resolve_expr(using_param->children_[0], func, expr,
                                   combine_line_and_col(using_param->children_[0]->stmt_loc_)))) {
        LOG_WARN("failed to resolve const", K(ret));
      } else {
        bool legal_extend = false;
        if (ObExtendType == expr->get_result_type().get_type()) {
          if (expr->get_result_type().get_udt_id() != OB_INVALID_ID) {
            const ObUserDefinedType *user_type = NULL;
            CK (OB_NOT_NULL(current_block_));
            OZ (current_block_->get_namespace().get_pl_data_type_by_id(
              expr->get_result_type().get_udt_id(), user_type));
            CK (OB_NOT_NULL(user_type));
            OX (legal_extend = user_type->is_udt_type()
                               || user_type->is_package_type()
                               || user_type->is_sys_refcursor_type()
                               || user_type->is_rowtype_type());
          } else {
            legal_extend = true; // for anonymous collection
          }
        }
        if (OB_SUCC(ret)
            && (T_NULL == using_param->children_[0]->type_
                || ObTinyIntType == expr->get_result_type().get_type()
                || IS_BOOL_OP(using_param->children_[0]->type_)
                || (ObExtendType == expr->get_result_type().get_type() && !legal_extend))) {
          ret = OB_ERR_EXPR_SQL_TYPE;
          LOG_WARN("PLS-00457: expressions have to be of SQL types",
                   K(ret), K(using_param->children_[0]->type_), K(expr->get_result_type()));
        }
        if (OB_SUCC(ret)) {
          switch (using_param->value_) {
          case MODE_IN: {
            using_param_mode = PL_PARAM_IN;
          }
            break;
          case MODE_OUT: { //fallthrough
            using_param_mode = PL_PARAM_OUT;
          }
          case MODE_INOUT: {
            if (MODE_INOUT == using_param->value_) {
              OX (using_param_mode = PL_PARAM_INOUT);
            }
            OZ (resolve_inout_param(expr, using_param_mode, out_idx));
          }
            break;
          default: {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("sp inout flag is invalid", K(using_param->value_));
          }
            break;
          }
        }
      }

      OZ (using_params.push_back(InOutParam(func.get_expr_count() - 1, using_param_mode, out_idx)));
    }
  }
  return ret;
}

int ObPLResolver::resolve_execute_immediate(
  const ObStmtNodeTree *parse_tree, ObPLExecuteStmt *stmt, ObPLFunctionAST &func)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(parse_tree) || OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("argument is NULL", K(parse_tree), K(stmt), K(ret));
  } else {
    //解析sql
    ObRawExpr *sql = NULL;
    const ObStmtNodeTree *sql_node = parse_tree->children_[0];
    if (OB_ISNULL(sql_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sql node is NULL", K(parse_tree->children_), K(sql_node), K(ret));
    } else if (OB_FAIL(resolve_expr(sql_node, func,
                                    sql, combine_line_and_col(sql_node->stmt_loc_)))) {
      LOG_WARN("failed to resolve sql expr", K(ret));
    } else if (!sql->get_result_type().is_string_or_lob_locator_type() && !sql->get_result_type().is_json()
               && !sql->get_result_type().is_geometry()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("dynamic sql must be string type", K(sql->get_result_type()), K(ret));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "non-string type dynamic sql");
    } else {
      stmt->set_sql(func.get_expr_count() - 1);
    }

    //解析into
    if (OB_SUCC(ret)) {
      const ObStmtNodeTree *into_node = parse_tree->children_[1];
      if (NULL != into_node) {
        if (OB_FAIL(resolve_into(into_node, *stmt, func))) {
          LOG_WARN("type node is NULL", K(parse_tree->children_), K(into_node), K(ret));
        }
      }
    }

    //解析returning into, 语法保证了INTO子句和RETURNING INTO子句不能并存, 因此共用一个INTO结构
    if (OB_SUCC(ret)) {
      const ObStmtNodeTree *returning_node = parse_tree->children_[3];
      if (NULL != returning_node) {
        OZ (resolve_into(returning_node, *stmt, func));
        OX (stmt->set_is_returning(true));
      }
    }

    //解析using
    if (OB_SUCC(ret)) {
      const ObStmtNodeTree *using_node = parse_tree->children_[2];
      if (NULL != using_node) {
        OZ (resolve_using(using_node, stmt->get_using(), func));
        if (OB_FAIL(ret)) {
        } else if (stmt->has_out() && (parse_tree->children_[3] || parse_tree->children_[1])) {
          ret = OB_ERR_INOUT_PARAM_PLACEMENT_NOT_PROPERLY;
          LOG_WARN("PLS-00254: OUT and IN/OUT modes cannot be used in this context Cause: \
              actual parameter mode (OUT, or IN/OUT) is not used properly in USING clause.\
              For USING clause in an OPEN statement, only IN mode is allowed.", K(ret));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_using().count(); ++i) {
            const ObRawExpr *expr = func.get_expr(stmt->get_using_index(i));
            if (OB_ISNULL(expr)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("expr is NULL", K(ret));
            } else if (expr->is_obj_access_expr()) {
              pl::ObPLDataType final_type;
              const ObObjAccessRawExpr *access_expr = static_cast<const ObObjAccessRawExpr *>(expr);
              OZ(access_expr->get_final_type(final_type));
              if (OB_SUCC(ret) && final_type.is_type_record() && stmt->is_out(i)) {
                ret =OB_ERR_EXPRESSION_WRONG_TYPE;
                LOG_WARN("expr is wrong type", K(ret));
              }
            }

            if (OB_SUCC(ret) && stmt->is_out(i)) {
              OZ (stmt->generate_into_variable_info(current_block_->get_namespace(), *expr));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObPLResolver::resolve_extend(const ObStmtNodeTree *parse_tree, ObPLExtendStmt *stmt, ObPLFunctionAST &func)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(parse_tree) || OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("parse_tree is NULL", K(parse_tree), K(stmt), K(ret));
  } else {
    //解析expr
    ObRawExpr *expr = NULL;
    const ObStmtNodeTree *expr_node = parse_tree->children_[0];
    if (OB_ISNULL(expr_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sql node is NULL", K(parse_tree->children_), K(expr_node), K(ret));
    } else if (OB_FAIL(resolve_expr(expr_node, func,
                                    expr, combine_line_and_col(expr_node->stmt_loc_)))) {
      LOG_WARN("failed to resolve sql expr", K(ret));
    } else if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is NULL", K(ret));
    } else if (expr->is_obj_access_expr()) {
      const ObObjAccessRawExpr *access_expr = static_cast<const ObObjAccessRawExpr*>(expr);
      ObPLDataType type;
      if (OB_FAIL(access_expr->get_final_type(type))) {
        LOG_WARN("failed to get final type", K(*access_expr), K(ret));
      } else if (type.is_collection_type()) {
        if (OB_FAIL(type.get_all_depended_user_type(get_resolve_ctx(), get_current_namespace()))) {
          LOG_WARN("get all depended user type failed", K(ret));
        } else {
          stmt->set_extend(func.get_expr_count() - 1);
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("extend method must be used for collcetion type", K(type), K(*expr), K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("extend method must be used for collcetion type", K(*expr), K(ret));
    }

    //解析ni
    if (OB_SUCC(ret)) {
      ObRawExpr *n_expr = NULL;
      ObRawExpr *i_expr = NULL;
      int64_t n = OB_INVALID_INDEX;
      int64_t i = OB_INVALID_INDEX;
      if (NULL == parse_tree->children_[1]) {
        ObConstRawExpr *expr = NULL;
        if (OB_FAIL(ObRawExprUtils::build_const_int_expr(expr_factory_, ObIntType, 1, expr))) {
          LOG_WARN("failed to build const expr", K(ret));
        } else if (OB_FAIL(func.add_expr(expr))) {
          LOG_WARN("failed to add expr", K(*expr), K(ret));
        } else {
          n = func.get_expr_count() - 1;
        }
      } else if (T_FUN_SYS != parse_tree->children_[1]->type_
          || NULL == parse_tree->children_[1]->children_[0]
          || T_IDENT != parse_tree->children_[1]->children_[0]->type_
          || 6 != parse_tree->children_[1]->children_[0]->str_len_
          || 0 != STRNCASECMP(parse_tree->children_[1]->children_[0]->str_value_, "EXTEND", 6)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected extend method", K(parse_tree->children_[1]->type_), K(ret));
      } else {
        const ObStmtNodeTree *arg_list = parse_tree->children_[1]->children_[1];
        if (T_EXPR_LIST != arg_list->type_ || 0 == arg_list->num_child_ || arg_list->num_child_ > 2) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected extend method", K(arg_list->type_), K(arg_list->num_child_), K(ret));
        } else if (OB_FAIL(resolve_expr(arg_list->children_[0], func, n_expr,
                                        combine_line_and_col(arg_list->children_[0]->stmt_loc_)))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected extend method", K(arg_list->children_[0]->type_), K(ret));
        } else {
          n = func.get_expr_count() - 1;
        }

        if (OB_SUCC(ret) && 2 == arg_list->num_child_) {
          if (OB_FAIL(resolve_expr(arg_list->children_[1], func, i_expr,
                                   combine_line_and_col(arg_list->children_[1]->stmt_loc_)))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Unexpected extend method", K(arg_list->children_[1]->type_), K(ret));
          } else {
            i = func.get_expr_count() - 1;
          }
        }
      }

      if (OB_SUCC(ret)) {
        stmt->set_ni(n, i);
      }
    }
  }
  return ret;
}

int ObPLResolver::resolve_udf_pragma(const ObStmtNodeTree *parse_tree,
                                     ObPLFunctionAST &unit_ast)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(parse_tree));
  CK (T_SP_PRAGMA_UDF == parse_tree->type_);
  if (OB_SUCC(ret)) {
    if (unit_ast.is_routine()) {
      if (unit_ast.get_compile_flag().compile_with_udf()) {
        ret = OB_ERR_PRAGMA_DECL_TWICE;
        LOG_WARN("PLS-00711: PRAGMA string cannot be declared twice", K(ret));
      } else {
        unit_ast.get_compile_flag().add_udf();
      }
    } else {
      ret = OB_ERR_PRAGMA_ILLEGAL;
      LOG_WARN("PLS-00710: Pragma string cannot be specified here", K(ret));
    }
  }
  return ret;
}

int ObPLResolver::resolve_serially_reusable_pragma(const ObStmtNodeTree *parse_tree,
                                                   ObPLPackageAST &unit_ast)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(parse_tree));
  CK (T_SP_PRAGMA_SERIALLY_REUSABLE == parse_tree->type_);
  if (OB_SUCC(ret)) {
    if (unit_ast.is_package()) {
      OX (unit_ast.set_serially_reusable());
    } else {
      ret = OB_ERR_PARAM_IN_PACKAGE_SPEC;
      LOG_WARN("PLS-00708: Pragma string must be declared in a package specification", K(ret));
    }
  }
  return ret;
}

int ObPLResolver::resolve_restrict_references_pragma(const ObStmtNodeTree *parse_tree,
                                                     ObPLPackageAST &ast)
{
  int ret = OB_SUCCESS;
  const ObStmtNodeTree *subprogram_node = NULL;
  const ObStmtNodeTree *assert_list_node = NULL;
  ObString subprogram;
  ObPLCompileFlag compile_flag;
  CK (OB_NOT_NULL(parse_tree));
  CK (T_SP_PRAGMA_RESTRICT_REFERENCE == parse_tree->type_);
  CK (2 == parse_tree->num_child_);
  CK (OB_NOT_NULL(subprogram_node = parse_tree->children_[0]));
  CK (OB_NOT_NULL(assert_list_node = parse_tree->children_[1]));
  if (OB_SUCC(ret) && ast.get_package_type() != ObPackageType::PL_PACKAGE_SPEC) {
    ret = OB_ERR_PARAM_IN_PACKAGE_SPEC;
    LOG_WARN("PLS-00708: Pragma string must be declared in a package specification",
             K(ret), K(ast.get_package_type()));
  }
  // resolve subprogram name
  CK (T_SP_NAME == subprogram_node->type_);
  if (OB_SUCC(ret)) {
    if (0 == subprogram_node->num_child_) {
      // subprogram is empty indicate default, do nothing ...
    } else {
      CK (1 == subprogram_node->num_child_);
      CK (OB_NOT_NULL(subprogram_node->children_[0]));
      CK (T_IDENT == subprogram_node->children_[0]->type_);
      OX (subprogram = ObString(subprogram_node->children_[0]->str_len_,
                                subprogram_node->children_[0]->str_value_));
      if (OB_SUCC(ret) && subprogram.empty()) {
        ret = OB_ERR_IDENT_EMPTY;
        LOG_WARN("Identifier cannot be an empty string", K(ret), K(subprogram));
      }
    }
  }
  // resolve assert list
  if (OB_SUCC(ret)) {
    CK (T_SP_ASSERT_ITEM_LIST == assert_list_node->type_);
    for (int64_t i = 0; OB_SUCC(ret) && i < assert_list_node->num_child_; ++i) {
      ObStmtNodeTree *assert_node = assert_list_node->children_[i];
      CK (OB_NOT_NULL(assert_node));
      CK (T_SP_ASSERT_ITEM == assert_node->type_);
      if (OB_SUCC(ret)
          && !(ObPLCompileFlag::TRUST <= assert_node->value_
               && assert_node->value_ <= ObPLCompileFlag::WNPS)) {
        ObString node(assert_node->str_len_, assert_node->str_value_);
        ret = OB_ERR_PRAGMA_STR_UNSUPPORT;
        LOG_USER_ERROR(OB_ERR_PRAGMA_STR_UNSUPPORT,
                       "RESTRICT_REFERENCES", node.length(), node.ptr());
      }
      OZ (compile_flag.add_compile_flag(assert_node->value_));
    }
  }
  if (OB_SUCC(ret)) {
    if (subprogram.empty() || 0 == subprogram.case_compare(ast.get_name())) {
      OX (ast.set_compile_flag(compile_flag));
    } else {
      ObPLRoutineTable &routine_table = ast.get_routine_table();
      const ObPLRoutineInfo *routine_info = NULL;
      for (int64_t i = routine_table.get_count() - 1; OB_SUCC(ret) && i >= 1; --i) {
        OZ (routine_table.get_routine_info(i, routine_info));
        CK (OB_NOT_NULL(routine_info));
        if (OB_SUCC(ret) && 0 == routine_info->get_name().case_compare(subprogram)) {
          (const_cast<ObPLRoutineInfo*>(routine_info))->set_compile_flag(compile_flag);
          break;
        }
        OX (routine_info = NULL;)
      }
      if (OB_SUCC(ret) && OB_ISNULL(routine_info)) {
        ret = OB_ERR_PRAGMA_FIRST_ARG;
        LOG_USER_ERROR(OB_ERR_PRAGMA_FIRST_ARG, "RESTRICT_REFERENCES");
      }
    }
  }
  return ret;
}

int ObPLResolver::resolve_interface_pragma(const ObStmtNodeTree *parse_tree, ObPLPackageAST &ast)
{
  int ret = OB_SUCCESS;
  const ObStmtNodeTree *name_node = NULL;
  ObString interface_name;
  CK (OB_NOT_NULL(parse_tree));
  CK (T_SP_PRAGMA_INTERFACE == parse_tree->type_);
  CK (1 == parse_tree->num_child_);
  CK (OB_NOT_NULL(name_node = parse_tree->children_[0]));
  if (OB_SUCC(ret)
      && ast.get_package_type() != ObPackageType::PL_PACKAGE_BODY
      && ast.get_package_type() != ObPackageType::PL_UDT_OBJECT_BODY) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("Ober Notice: This is not a public feature. Only allowed in package body now",
             K(ret), K(ast.get_package_type()));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "PRAGMA INTERFACE in package body");
  }
  // resolve interface name
  CK (T_IDENT == name_node->type_);
  if (OB_SUCC(ret)) {
    OX (interface_name = ObString(name_node->str_len_, name_node->str_value_));
    CK (!interface_name.empty());
  }

  if (OB_SUCC(ret)) {
    int64_t idx = OB_INVALID_INDEX;
    ObPLFunctionAST *routine_ast = NULL;
    ObPLRoutineInfo *routine_info = NULL;
    ObPLRoutineTable &routine_table = ast.get_routine_table();
    for (int64_t i = 0;
         OB_SUCC(ret) && OB_INVALID_INDEX == idx && i < routine_table.get_count();
         ++i) {
      //寻找Interface Pragma语句的上一个routine
      routine_ast = NULL;
      routine_info = NULL;
      OZ (routine_table.get_routine_info(i, routine_info));
      OZ (routine_table.get_routine_ast(i, routine_ast));
      if (NULL != routine_info && NULL == routine_ast) {
        //本Interface Pragma修饰的routine一定是只有声明而没有定义的状态
        idx = i;
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_INVALID_INDEX == idx) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN(
            "Ober Notice: This is not a public feature. Only allowed follow a routine declare",
            K(interface_name),
            K(ret));
      } else {
        CK(OB_NOT_NULL(routine_info));
        if (OB_SUCC(ret) && OB_NOT_NULL(routine_ast)) {
          ret = OB_ERR_SP_DUP_VAR;
          LOG_USER_ERROR(OB_ERR_SP_DUP_VAR,
              routine_ast->get_name().length(), routine_ast->get_name().ptr());
        }
        OZ (routine_info->add_compile_flag(ast.get_compile_flag()));
        OZ (routine_table.make_routine_ast(resolve_ctx_.allocator_,
                                           ast.get_db_name(),
                                           ast.is_package() ? ast.get_name() : ObString(),
                                           ast.get_version(),
                                           *routine_info,
                                           routine_ast));
        OZ (resolve_routine_block(parse_tree, *routine_info, *routine_ast));
        OZ (routine_table.set_routine_ast(idx, routine_ast));
        OX (ast.set_can_cached(routine_ast->get_can_cached()));
        if (OB_FAIL(ret) && OB_NOT_NULL(routine_ast)) {
          routine_ast->~ObPLFunctionAST();
          resolve_ctx_.allocator_.free(routine_ast);
          routine_ast = NULL;
        }
        OZ (const_cast<ObPLRoutineInfo*>(routine_info)->get_compile_flag().add_intf());
      }
    }
  }
  return ret;
}

int ObPLResolver::resolve_interface(const ObStmtNodeTree *parse_tree,
                                    ObPLInterfaceStmt *stmt,
                                    ObPLFunctionAST &ast)
{
  int ret = OB_SUCCESS;
  const ObStmtNodeTree *name_node = NULL;
  ObString interface_name;
  CK (OB_NOT_NULL(parse_tree));
  CK (T_SP_PRAGMA_INTERFACE == parse_tree->type_);
  CK (1 == parse_tree->num_child_);
  CK (OB_NOT_NULL(name_node = parse_tree->children_[0]));

  // resolve interface name
  CK (T_IDENT == name_node->type_);
  if (OB_SUCC(ret)) {
    OX (interface_name = ObString(name_node->str_len_, name_node->str_value_));
    CK (!interface_name.empty());
  }

  if (OB_SUCC(ret)) {
    PL_C_INTERFACE_t entry = nullptr;
    CK (OB_NOT_NULL(GCTX.pl_engine_));
    OX (entry = GCTX.pl_engine_->get_interface_service().get_entry(interface_name));
    if (OB_SUCC(ret) && OB_ISNULL(entry)) {
      ret = OB_ERR_PRAGMA_FOLLOW_DECL;
      LOG_USER_ERROR(OB_ERR_PRAGMA_FOLLOW_DECL, interface_name.length(), interface_name.ptr());
    }
    OX (stmt->set_entry(interface_name));
    OZ (ast.get_compile_flag().add_intf());
  }
  return ret;
}

int ObPLResolver::resolve_declare_cond(const ObStmtNodeTree *parse_tree,
                                       ObPLPackageAST &package_ast)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(resolve_declare_cond(parse_tree, NULL, package_ast))) {
    LOG_WARN("failed to resolve declare condition for package", K(ret));
  }
  return ret;
}

int ObPLResolver::resolve_declare_cond(const ObStmtNodeTree *parse_tree,
                                       ObPLDeclareCondStmt *stmt,
                                       ObPLCompileUnitAST &func)
{
  int ret = OB_SUCCESS;
  UNUSED(func);
  UNUSED(stmt);
  if (OB_ISNULL(parse_tree)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("parse_tree is NULL", K(parse_tree), K(ret));
  } else if (OB_FAIL(check_declare_order(PL_COND))) {
    LOG_WARN("fail to check declare order", K(ret));
  } else {
    //解析name
    const ObStmtNodeTree *name_node = (T_SP_DECL_COND == parse_tree->type_
      ? parse_tree->children_[0] : parse_tree->children_[0]->children_[0]);
    ObString name;
    if (T_IDENT != name_node->type_) {
      ret = OB_ERR_EX_NAME_ARG;
      LOG_WARN("expect a name for exception", K(name_node->type_), K(ret));
    } else if (OB_FAIL(resolve_ident(name_node, name))) {
      LOG_WARN("failed to resolve ident", K(name_node), K(ret));
    }

    //解析condition
    ObPLConditionValue value;
    if (OB_SUCC(ret)) {
      if ((T_SP_DECL_COND == parse_tree->type_ && 2 == parse_tree->num_child_)
          || T_SP_INIT_PRAGMA == parse_tree->type_) {
        // T_SP_DECL_COND is mysql mode
        // T_SP_INIT_PRAGMA is oracle mode
        const ObStmtNodeTree *condition_node = (T_SP_DECL_COND == parse_tree->type_ ?
          parse_tree->children_[1] : NULL);
        if (T_SP_INIT_PRAGMA == parse_tree->type_) {
          if (2 != parse_tree->children_[0]->num_child_) {
            ret = OB_ERR_EX_ARG_NUM;
            LOG_WARN("illegal number of arg", K(parse_tree->children_[0]->num_child_), K(ret));
          } else if (OB_ISNULL(parse_tree->children_[0]->children_[1])) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("param node is NULL", K(ret));
          } else {
            condition_node = parse_tree->children_[0]->children_[1];
          }
        }
        CK (OB_NOT_NULL(condition_node));
        OZ (resolve_condition_value(condition_node, value,
                                    is_sys_database_id(func.get_database_id())));
        OZ (current_block_->get_namespace().add_condition(
              name, value, lib::is_oracle_mode() ? true : false));
      } else { // ORACLE模式下 UserDefinedException
        CK (lib::is_oracle_mode());
        OX (value.type_ = ERROR_CODE);
        // package public or private exception need to combind package id.
        if (OB_FAIL(ret)) {
        } else if (func.is_package()) {
          ObPLPackageAST &pack = static_cast<ObPLPackageAST&>(func);
          OX (value.error_code_
            = (next_user_defined_exception_id_++) | (pack.get_id() << OB_PACKAGE_ID_SHIFT));
        } else {
          OX (value.error_code_ = next_user_defined_exception_id_++);
        }
        OX (value.sql_state_ = ob_sqlstate(static_cast<int>(value.error_code_)));
        OX (value.str_len_ = STRLEN(value.sql_state_));
        OZ (current_block_->get_namespace().add_condition(name, value));
      }
    }
  }
  return ret;
}

int ObPLResolver::resolve_declare_handler(const ObStmtNodeTree *parse_tree, ObPLDeclareHandlerStmt *stmt, ObPLFunctionAST &func)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(parse_tree) || OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("parse_tree is NULL", K(parse_tree), K(stmt), K(ret));
  } else if (OB_FAIL(check_declare_order(PL_HANDLER))) {
    LOG_WARN("fail to check declare order", K(ret));
  } else {
    ObPLDeclareHandlerStmt::DeclareHandler::HandlerDesc* desc =
            static_cast<ObPLDeclareHandlerStmt::DeclareHandler::HandlerDesc*>(resolve_ctx_.allocator_.alloc(sizeof(ObPLDeclareHandlerStmt::DeclareHandler::HandlerDesc)));
    if (OB_ISNULL(desc)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Invalid then node type", K(ret));
    } else {
      desc = new(desc)ObPLDeclareHandlerStmt::DeclareHandler::HandlerDesc(resolve_ctx_.allocator_);
      //解析Action
      desc->set_action(static_cast<ObPLDeclareHandlerStmt::DeclareHandler::Action>(parse_tree->value_));

      //解析body：这里必须先解析body后解析condition value，是因为handler的body解析过程不应受本handler自己的in_warning和in_notfound影响
      if (OB_SUCC(ret)) {
        if (OB_ISNULL(parse_tree->children_[1])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Invalid body node", K(parse_tree->children_[1]), K(ret));
        } else if (T_SP_PROC_STMT_LIST != parse_tree->children_[1]->type_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Invalid else node", K(parse_tree->children_[1]->type_), K(ret));
        } else {
          ObPLStmtBlock *body_block = NULL;
          int64_t top_continue = handler_analyzer_.get_continue();
          ++current_level_;
          if (handler_analyzer_.in_continue()) {
            ObPLDeclareHandlerStmt::DeclareHandler info;
            if (OB_FAIL(handler_analyzer_.get_handler(top_continue, info))) {
              LOG_WARN("failed to get top continue handler", K(ret), K(top_continue));
            } else if (info.get_level() == (current_level_ - 1)) {
              handler_analyzer_.set_continue(OB_INVALID_INDEX);
            }
          }
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(resolve_stmt_list(parse_tree->children_[1],
                                        body_block,
                                        func,
                                        true,/*stop scarch label*/
                                        true /*in exception handler scope*/))) {
            LOG_WARN("failed to resolve stmt list", K(parse_tree->children_[1]->type_), K(ret));
          } else if (OB_FAIL(handler_analyzer_.reset_handlers(current_level_))) {
            LOG_WARN("failed to reset handlers", K(ret), K(current_level_));
          } else {
            --current_level_;
            handler_analyzer_.reset_notfound_and_warning(current_level_);
            handler_analyzer_.set_continue(top_continue);
            desc->set_body(body_block);
          }
        }
      }

      //解析condition value
      if (OB_SUCC(ret)) {
        const ObStmtNodeTree *handler_list = parse_tree->children_[0];
        if (NULL == handler_list) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("then node is NULL", K(handler_list), K(parse_tree->children_), K(ret));
        } else if (T_SP_HCOND_LIST != handler_list->type_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Invalid then node type", K(handler_list->type_), K(ret));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < handler_list->num_child_; ++i) {
            ObPLConditionValue value;
            bool dup = false;
            ObPLConditionType actual_type = INVALID_TYPE;
            if (OB_FAIL(resolve_handler_condition(handler_list->children_[i], value, func))) {
              LOG_WARN("failed to resolve condition value", K(handler_list->children_[i]), K(ret));
            } else if (OB_FAIL(check_duplicate_condition(*stmt, value, dup, desc))) {
              LOG_WARN("failed to check duplication", K(value), K(ret));
            } else if (dup) {
              ret = OB_ERR_SP_DUP_HANDLER;
              LOG_USER_ERROR(OB_ERR_SP_DUP_HANDLER);
              LOG_WARN("Duplicate handler declared in the same block", K(value), K(dup), K(ret));
            } else if (OB_FAIL(ObPLResolver::analyze_actual_condition_type(value, actual_type))) {
              LOG_WARN("failed to analyze actual condition type", K(value), K(ret));
            } else if (lib::is_oracle_mode()
                       && desc->get_conditions().count() > 0
                       && ObPLConditionType::OTHERS
                        == desc->get_conditions().at(desc->get_conditions().count() - 1).type_) {
              ret = OB_ERR_NO_CHOICES;
              LOG_WARN("no choices may appear with choice OTHERS in an exception handler",
                        K(ret));
            } else if (OB_FAIL(desc->add_condition(value))) {
              LOG_WARN("failed to add condition for delcare handler desc", K(ret), K(value));
            } else {
              if (NOT_FOUND == actual_type && !handler_analyzer_.in_notfound()) {
                handler_analyzer_.set_notfound(current_level_);
                current_block_->set_notfound();
              } else if (SQL_WARNING == actual_type && !handler_analyzer_.in_warning()) {
                handler_analyzer_.set_warning(current_level_);
                current_block_->set_warning();
              }
            }
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (desc->is_continue() || handler_analyzer_.in_continue()) {
        //如果自己是continue或者已经在continue里，把自己压栈
        if (OB_FAIL(handler_analyzer_.set_handler(desc, current_level_))) {
          LOG_WARN("failed to set handler", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (desc->is_continue() && !handler_analyzer_.in_continue()) {
        //如果自己是top continue，需要追溯把自己平级的handler都压栈
        handler_analyzer_.set_continue();
        if (OB_FAIL(handler_analyzer_.set_handlers(stmt->get_handlers(), current_level_))) {
          LOG_WARN("failed to set handler", K(ret));
        }
      }
    }

    if (OB_SUCC(ret) && !desc->is_continue()) {
      ObPLDeclareHandlerStmt::DeclareHandler handler;
      handler.set_desc(desc);
      if (OB_FAIL(stmt->add_handler(handler))) {
        LOG_WARN("failed to add handler", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_ISNULL(current_block_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Symbol table is NULL", K(current_block_), K(ret));
      } else if (stmt->get_handlers().count() > 0) {
        current_block_->set_eh(stmt);
      }
    }
  }
  return ret;
}

int ObPLResolver::resolve_resignal(
  const ObStmtNodeTree *parse_tree, ObPLSignalStmt *stmt, ObPLFunctionAST &func)
{
  int ret = OB_SUCCESS;
  UNUSED(func);
  CK (OB_NOT_NULL(parse_tree));
  CK (OB_LIKELY(T_SP_RESIGNAL == parse_tree->type_));
  CK (OB_LIKELY(2 == parse_tree->num_child_));
  CK (OB_NOT_NULL(stmt));
  CK (OB_NOT_NULL(current_block_));
  OX (stmt->set_is_resignal_stmt());
  if (OB_FAIL(ret)) {
  } else if (!current_block_->in_handler()) {
    if (lib::is_oracle_mode()) {
      ret = OB_ERR_SP_COND_MISMATCH;
      LOG_WARN("PLS-00367: a RAISE statement with no exception name must be inside an exception handler", K(ret));
    } else if(OB_NOT_NULL(parse_tree->children_[0]) && OB_FAIL(resolve_signal(parse_tree, stmt, func))) {
      LOG_WARN("resolve resignal fail", K(ret));
    } else {
      ret = OB_ERR_RESIGNAL_WITHOUT_ACTIVE_HANDLER;
      LOG_WARN("RESIGNAL when handler not active", K(ret));
    }
  } else if (OB_ISNULL(parse_tree->children_[0]) && OB_ISNULL(parse_tree->children_[1])) {
    stmt->set_is_signal_null();
  } else {
    OZ (resolve_signal(parse_tree, stmt, func));
  }
  return ret;
}

int ObPLResolver::resolve_signal(const ObStmtNodeTree *parse_tree, ObPLSignalStmt *stmt, ObPLFunctionAST &func)
{
  int ret = OB_SUCCESS;
  UNUSED(func);
  CK (OB_NOT_NULL(parse_tree));
  CK (OB_NOT_NULL(stmt));
  CK (OB_NOT_NULL(current_block_));
  if (OB_SUCC(ret)) {
    //解析 value
    const ObStmtNodeTree *value_node = parse_tree->children_[0];
    if (OB_ISNULL(value_node)) {
      if (lib::is_oracle_mode()) {
        if (OB_SUCC(ret) && !current_block_->in_handler()) {
          ret = OB_ERR_RAISE_NOT_IN_HANDLER;
          LOG_WARN("PLS-00367: a RAISE statement with no exception name must be inside an exception handler", K(ret));
        }
        OX (stmt->set_is_signal_null());
      } else if (!stmt->is_resignal_stmt()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("signal stmt must specify signal value", K(ret));
      }
    } else if (T_IDENT == value_node->type_ // for mysql mode
               || T_SP_ACCESS_NAME == value_node->type_) { // for oracle mode
      const ObPLConditionValue *value = NULL;
      CK (OB_NOT_NULL(current_block_));
      OZ (resolve_condition(value_node,
                            current_block_->get_namespace(),
                            &value,
                            func));
      if (OB_SUCC(ret) && OB_ISNULL(value)) {
        ret = OB_ERR_SP_COND_MISMATCH;
        LOG_USER_ERROR(OB_ERR_SP_COND_MISMATCH, static_cast<int>(value_node->str_len_), value_node->str_value_);
        LOG_WARN("Undefined CONDITION: condition value is NULL", K(value), K(value_node->str_value_), K(ret));
      }
      if (OB_SUCC(ret)) {
        /*
         * Mysql不允许signal语句的condition是error code类型，只能是sqlstate类型，所以这里禁止掉。
         * If SIGNAL refers to a named condition that is defined with a MySQL error number rather than an SQLSTATE value,
         * a SIGNAL/RESIGNAL can only use a CONDITION defined with SQLSTATE error occurs.
         * */
        if (!lib::is_oracle_mode() && SQL_STATE != value->type_) {
          ret = OB_ERR_SP_BAD_CONDITION_TYPE;
          LOG_WARN("SIGNAL/RESIGNAL can only use a CONDITION defined with SQLSTATE", K(value->type_), K(ret));
        } else if (!is_sqlstate_valid(value->sql_state_, value->str_len_)
            || is_sqlstate_completion(value->sql_state_)) {
          ret = OB_ER_SP_BAD_SQLSTATE;
          LOG_USER_ERROR(OB_ER_SP_BAD_SQLSTATE, static_cast<int>(value->str_len_), value->sql_state_);
          LOG_WARN("Bad SQLSTATE", K(ret));
        } else {
          stmt->set_value(*value);
          stmt->set_ob_error_code(value->error_code_);
        }
      }
    } else if (T_SQL_STATE == value_node->type_) {
      stmt->set_cond_type(SQL_STATE);
      CK (OB_NOT_NULL(value_node->children_[0]));
      CK (OB_LIKELY(T_VARCHAR == value_node->children_[0]->type_));
      if (OB_SUCC(ret)) {
        if (!is_sqlstate_valid(value_node->children_[0]->str_value_, value_node->children_[0]->str_len_)
        || is_sqlstate_completion(value_node->children_[0]->str_value_)) {
          ret = OB_ER_SP_BAD_SQLSTATE;
          LOG_USER_ERROR(OB_ER_SP_BAD_SQLSTATE, static_cast<int>(value_node->children_[0]->str_len_),
                                                value_node->children_[0]->str_value_);
          LOG_WARN("Bad SQLSTATE", K(ret));
        } else {
          stmt->set_sql_state(value_node->children_[0]->str_value_);
          stmt->set_str_len(value_node->children_[0]->str_len_);
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid name node", K(value_node->type_), K(ret));
    }
  }
  if (OB_SUCC(ret) && lib::is_mysql_mode()) {
    const ObStmtNodeTree *info_node = parse_tree->children_[1];
    if (NULL != info_node) {
      CK (T_SP_SIGNAL_INFO_LIST == info_node->type_);
      OZ (stmt->create_item_to_expr_idx(info_node->num_child_));
      for (int64_t i = 0; OB_SUCC(ret) && i < info_node->num_child_; ++i) {
        ObRawExpr *value_expr = NULL;
        ParseNode *var = info_node->children_[i]->children_[0];
        if (T_USER_VARIABLE_IDENTIFIER == var->type_ ||
            T_SYSTEM_VARIABLE == var->type_) {
          ObQualifiedName q_name;
          OZ (ObResolverUtils::resolve_obj_access_ref_node(expr_factory_,
                            var, q_name, resolve_ctx_.session_info_));
          if (T_SYSTEM_VARIABLE == var->type_) {
            q_name.access_idents_.at(0).access_index_ =
                ObSetVar::SET_SCOPE_GLOBAL == static_cast<ObSetVar::SetScopeType>(info_node->children_[i]->value_)
                ? pl::ObPLExternalNS::GLOBAL_VAR : pl::ObPLExternalNS::SESSION_VAR;
          } else if (T_USER_VARIABLE_IDENTIFIER == var->type_) {
            q_name.access_idents_.at(0).access_index_ = pl::ObPLExternalNS::USER_VAR;
          }
          OZ (resolve_var(q_name, func, value_expr, false/*for write*/));
          CK (OB_NOT_NULL(value_expr));
          OZ (func.add_expr(value_expr), value_expr);
        } else {
          OZ (resolve_expr(var,
                          func, value_expr,
                          combine_line_and_col(var->stmt_loc_),
                          true, NULL));
        }
        OZ (stmt->get_item_to_expr_idx().set_refactored(info_node->children_[i]->value_,
                                                        func.get_expr_count() - 1));
        if (OB_HASH_EXIST == ret) {
          ObString item_name;
          switch (info_node->children_[i]->value_)
          {
          case SignalCondInfoItem::DIAG_CLASS_ORIGIN:
            item_name.assign_ptr("CLASS_ORIGIN", static_cast<int32_t>(STRLEN("CLASS_ORIGIN")));
            break;
          case SignalCondInfoItem::DIAG_SUBCLASS_ORIGIN:
            item_name.assign_ptr("SUBCLASS_ORIGIN", static_cast<int32_t>(STRLEN("SUBCLASS_ORIGIN")));
            break;
          case SignalCondInfoItem::DIAG_CONSTRAINT_CATALOG:
            item_name.assign_ptr("CONSTRAINT_CATALOG", static_cast<int32_t>(STRLEN("CONSTRAINT_CATALOG")));
            break;
          case SignalCondInfoItem::DIAG_CONSTRAINT_SCHEMA:
            item_name.assign_ptr("CONSTRAINT_SCHEMA", static_cast<int32_t>(STRLEN("CONSTRAINT_SCHEMA")));
            break;
          case SignalCondInfoItem::DIAG_CONSTRAINT_NAME:
            item_name.assign_ptr("CONSTRAINT_NAME", static_cast<int32_t>(STRLEN("CONSTRAINT_NAME")));
            break;
          case SignalCondInfoItem::DIAG_CATALOG_NAME:
            item_name.assign_ptr("CATALOG_NAME", static_cast<int32_t>(STRLEN("CATALOG_NAME")));
            break;
          case SignalCondInfoItem::DIAG_SCHEMA_NAME:
            item_name.assign_ptr("SCHEMA_NAME", static_cast<int32_t>(STRLEN("SCHEMA_NAME")));
            break;
          case SignalCondInfoItem::DIAG_TABLE_NAME:
            item_name.assign_ptr("TABLE_NAME", static_cast<int32_t>(STRLEN("TABLE_NAME")));
            break;
          case SignalCondInfoItem::DIAG_COLUMN_NAME:
            item_name.assign_ptr("COLUMN_NAME", static_cast<int32_t>(STRLEN("COLUMN_NAME")));
            break;
          case SignalCondInfoItem::DIAG_CURSOR_NAME:
            item_name.assign_ptr("CURSOR_NAME", static_cast<int32_t>(STRLEN("CURSOR_NAME")));
            break;
          case SignalCondInfoItem::DIAG_MESSAGE_TEXT:
            item_name.assign_ptr("MESSAGE_TEXT", static_cast<int32_t>(STRLEN("MESSAGE_TEXT")));
            break;
          case SignalCondInfoItem::DIAG_MYSQL_ERRNO:
            item_name.assign_ptr("MYSQL_ERRNO", static_cast<int32_t>(STRLEN("MYSQL_ERRNO")));
            break;
          default:
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("signal item is error", K(info_node->children_[i]->value_), K(ret));
            break;
          }
          if (OB_ERR_UNEXPECTED != ret) {
            ret = OB_ERR_DUP_SIGNAL_SET;
            LOG_WARN("duplicate condition information item", K(ret));
            LOG_USER_ERROR(OB_ERR_DUP_SIGNAL_SET, item_name.ptr());
          }
        }
      }
    }
  }
  return ret;
}

int ObPLResolver::resolve_call(const ObStmtNodeTree *parse_tree, ObPLCallStmt *stmt, ObPLFunctionAST &func)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(parse_tree));
  CK (OB_NOT_NULL(stmt));
  CK (OB_LIKELY(T_SP_CALL_STMT == parse_tree->type_));
  if (OB_SUCC(ret)) {
    ObSchemaChecker schema_checker;
    ParseNode *name_node = parse_tree->children_[0];
    ParseNode *params_node = parse_tree->children_[1];
    if (OB_ISNULL(name_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the children of parse tree is NULL", K(name_node), K(ret));
    } else if (OB_FAIL(schema_checker.init(resolve_ctx_.schema_guard_, resolve_ctx_.session_info_.get_sessid()))) {
      LOG_ERROR("schema checker init failed", K(ret));
    } else {
      ObString db_name;
      ObString package_name;
      ObString sp_name;
      ObArray<ObRawExpr *> expr_params;
      ObString dblink_name;
      if (T_SP_ACCESS_NAME != name_node->type_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Invalid procedure name node", K(name_node->type_), K(ret));
      } else if (OB_FAIL(ObResolverUtils::resolve_sp_access_name(schema_checker,
                            resolve_ctx_.session_info_.get_effective_tenant_id(),
                            resolve_ctx_.session_info_.get_database_name(),
                            *name_node, db_name, package_name, sp_name, dblink_name))) {
        LOG_WARN("resolve sp name failed", K(ret));
      }
      if (OB_SUCC(ret) && OB_NOT_NULL(params_node)) {
        CK (T_SP_CPARAM_LIST == params_node->type_ || T_EXPR_LIST == params_node->type_);
        for (int64_t param_idx = 0;
             OB_SUCC(ret) && param_idx < params_node->num_child_; ++param_idx) {
          ObRawExpr *expr = NULL;
          OZ (resolve_expr(params_node->children_[param_idx],
                           static_cast<ObPLCompileUnitAST&>(func), expr,
                           combine_line_and_col(params_node->children_[param_idx]->stmt_loc_),
                           false, NULL));
          CK (OB_NOT_NULL(expr));
          OX (expr_params.push_back(expr));
        }
      }
      if (OB_SUCC(ret)) {
        const ObIRoutineInfo *routine_info = NULL;
        ObProcType routine_type = STANDALONE_PROCEDURE;
        if (OB_FAIL(current_block_->get_namespace().resolve_routine(
                                                            resolve_ctx_,
                                                            db_name,
                                                            package_name,
                                                            sp_name,
                                                            expr_params,
                                                            routine_type,
                                                            routine_info))) {
          LOG_WARN("resolve routine failed", K(ret));
          if (OB_ERR_SP_DOES_NOT_EXIST == ret) {
            LOG_USER_ERROR(OB_ERR_SP_DOES_NOT_EXIST, "PROCEDURE",
                package_name.length(), package_name.ptr(), sp_name.length(), sp_name.ptr());
          }
        } else if (PACKAGE_PROCEDURE == routine_type || PACKAGE_FUNCTION == routine_type) {
          const ObPLRoutineInfo *package_routine_info = static_cast<const ObPLRoutineInfo *>(routine_info);
          stmt->set_proc_id(package_routine_info->get_id());
          stmt->set_package_id(func.get_package_id());
          if (package_routine_info->get_param_count() != 0) {
            if (OB_FAIL(resolve_call_param_list(params_node, package_routine_info->get_params(), stmt, func))) {
              LOG_WARN("failed to resolve call param list", K(ret));
            }
          }
        } else if (STANDALONE_PROCEDURE == routine_type || STANDALONE_FUNCTION == routine_type) {
          const share::schema::ObRoutineInfo *schema_routine_info = static_cast<const ObRoutineInfo *>(routine_info);;
          const common::ObIArray<share::schema::ObRoutineParam*> &routine_params = schema_routine_info->get_routine_params();
          stmt->set_package_id(schema_routine_info->get_package_id());
          if (OB_INVALID_ID == schema_routine_info->get_package_id()) {
            stmt->set_proc_id(schema_routine_info->get_routine_id());
          } else {
            stmt->set_proc_id(schema_routine_info->get_subprogram_id());
          }
          if (routine_params.count() != 0) {
            if (OB_FAIL(resolve_call_param_list(params_node, routine_params, stmt, func))) {
              LOG_WARN("failed to resolve call param list", K(ret));
            }
          }
        } else {}
      }
    }
  }
  return ret;
}

int ObPLResolver::resolve_cparams_expr(const ParseNode *params_node,
                                       ObPLFunctionAST &func,
                                       ObIArray<ObRawExpr*> &exprs)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(params_node));
  CK (OB_LIKELY(T_SP_CPARAM_LIST == params_node->type_ || T_EXPR_LIST == params_node->type_));
  for (int64_t i = 0; OB_SUCC(ret) && i < params_node->num_child_; ++i) {
    ObRawExpr *expr = NULL;
    CK (params_node->children_[i]);
    OZ (resolve_expr(params_node->children_[i], static_cast<ObPLCompileUnitAST&>(func), expr,
                     combine_line_and_col(params_node->children_[i]->stmt_loc_), true, NULL));
    CK (OB_NOT_NULL(expr));
    OZ (exprs.push_back(expr));
  }
  return ret;
}

int ObPLResolver::set_write_property(ObRawExpr *raw_expr,
                                     ObRawExprFactory &expr_factory,
                                     const ObSQLSessionInfo *session_info,
                                     ObSchemaGetterGuard *schema_guard,
                                     bool for_write)
{
  int ret = OB_SUCCESS;
  ObObjAccessRawExpr *obj_expr = static_cast<ObObjAccessRawExpr *>(raw_expr);
  ObString func_name;
  CK (OB_NOT_NULL(obj_expr));
  OX (obj_expr->set_write(for_write));
  OZ (build_obj_access_func_name(obj_expr->get_orig_access_idxs(),
                                 expr_factory,
                                 session_info,
                                 schema_guard,
                                 for_write,
                                 func_name));
  OX (obj_expr->set_func_name(func_name));
  OZ (obj_expr->formalize(session_info));
  for (int64_t i = 0; OB_SUCC(ret) && i < obj_expr->get_param_count(); ++i) {
    if (T_FUN_PL_ASSOCIATIVE_INDEX == obj_expr->get_param_expr(i)->get_expr_type()) {
      ObPLAssocIndexRawExpr* index_expr = static_cast<ObPLAssocIndexRawExpr*>(obj_expr->get_param_expr(i));
      CK (OB_NOT_NULL(index_expr));
      OX (index_expr->set_write(for_write));
      if (OB_SUCC(ret) && index_expr->get_param_expr(0)->is_obj_access_expr()) {
        ObObjAccessRawExpr *obj_access_expr = static_cast<ObObjAccessRawExpr*>(index_expr->get_param_expr(0));
        CK (OB_NOT_NULL(obj_access_expr));
        if (obj_access_expr->for_write() != for_write) {
          ObString func_name;
          OX (obj_access_expr->set_write(for_write));
          OZ (build_obj_access_func_name(obj_access_expr->get_orig_access_idxs(),
                                         expr_factory,
                                         session_info,
                                         schema_guard,
                                         for_write,
                                         func_name));
          OX (obj_access_expr->set_func_name(func_name));
          OZ (obj_access_expr->formalize(session_info));
        }
      }
    }
  }
  return ret;
}

int ObPLResolver::resolve_inout_param(ObRawExpr *param_expr, ObPLRoutineParamMode param_mode, int64_t &out_idx)
{
  int ret = OB_SUCCESS;
  out_idx = OB_INVALID_INDEX;
  CK (OB_NOT_NULL(param_expr));

  if (OB_SUCC(ret) && T_SP_CPARAM == param_expr->get_expr_type()) {
    ObCallParamRawExpr *call_expr = static_cast<ObCallParamRawExpr *>(param_expr);
    CK (OB_NOT_NULL(call_expr));
    CK (OB_NOT_NULL(call_expr->get_expr()));
    OX (param_expr = call_expr->get_expr());
  }
  // ObjAccessExpr有几种情况: 本地复杂变量做出参; 本地复杂变量的某个域做出参; Package复杂变量的某个属性做出参;
  if (OB_FAIL(ret)) {
  } else if (param_expr->is_obj_access_expr()) {
    ObObjAccessRawExpr *obj_expr = static_cast<ObObjAccessRawExpr *>(param_expr);
    ObIArray<pl::ObObjAccessIdx>& access_idxs = obj_expr->get_access_idxs();
    //本地复杂变量本身做出参
    if (ObObjAccessIdx::is_local_variable(access_idxs)
        && ObObjAccessIdx::get_local_variable_idx(access_idxs) == (access_idxs.count() - 1)) {
      CK (!obj_expr->get_var_indexs().empty());
      OX (out_idx = obj_expr->get_var_indexs().at(0));
    } else if (ObObjAccessIdx::is_local_variable(access_idxs)
               || ObObjAccessIdx::is_package_variable(access_idxs)
               || ObObjAccessIdx::is_subprogram_variable(access_idxs)) {
      if (ObObjAccessIdx::is_local_variable(access_idxs)) {
        int64_t var_idx
          = access_idxs.at(ObObjAccessIdx::get_local_variable_idx(access_idxs)).var_index_;
        CK (var_idx >= 0 && var_idx < obj_expr->get_var_indexs().count());
        OZ (check_local_variable_read_only(
          current_block_->get_namespace(),
          obj_expr->get_var_indexs().at(var_idx),
          PL_PARAM_INOUT == param_mode),
          K(access_idxs));
      } else {
        OZ (check_variable_accessible(current_block_->get_namespace(), access_idxs, true));
      }
    } else {
      ret = OB_ERR_EXP_NOT_ASSIGNABLE;
      LOG_WARN("expression cannot be used as an assignment", K(ret), K(access_idxs));
    }
    OZ (obj_expr->formalize(&get_resolve_ctx().session_info_));
    OZ (set_write_property(obj_expr, expr_factory_, &resolve_ctx_.session_info_, &resolve_ctx_.schema_guard_, true));
  } else if (param_expr->is_const_raw_expr()) { // 本地变量做出参
    const ObConstRawExpr *const_expr = static_cast<const ObConstRawExpr*>(param_expr);
    const ObPLSymbolTable *table = current_block_->get_symbol_table();
    const ObPLVar *var = NULL;
    bool is_anonymos_const_var = false;
    if (!resolve_ctx_.is_prepare_protocol_  &&
        T_QUESTIONMARK == const_expr->get_expr_type() &&
        OB_NOT_NULL(table) &&
        OB_NOT_NULL(var = table->get_symbol(const_expr->get_value().get_unknown())) &&
        0 == var->get_name().case_compare(pl::ObPLResolver::ANONYMOUS_ARG)) {
      is_anonymos_const_var = true;
    }
    if (T_QUESTIONMARK != const_expr->get_expr_type() || is_anonymos_const_var) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("procedure parameter expr type is wrong", K(ret), K(const_expr->get_expr_type()));
    } else {
      out_idx = const_expr->get_value().get_unknown();
    }
  } else if (T_OP_GET_USER_VAR == param_expr->get_expr_type() // 用户变量做出参
             || T_OP_GET_SYS_VAR == param_expr->get_expr_type()) {
    // 如果是用户变量和系统变量，是得不到out_index的，退出即可
  } else if (T_OP_GET_PACKAGE_VAR == param_expr->get_expr_type()
             || T_OP_GET_SUBPROGRAM_VAR == param_expr->get_expr_type()) {
    // PACKAGE变量做出参, 得不到OutIDX, 仅检查变量是否可读写
    OZ (check_variable_accessible(param_expr, true));
  } else {
    ret = OB_ERR_EXP_NOT_ASSIGNABLE;
    LOG_WARN("wrong param type with output routine param",
             K(ret), K(param_expr->get_expr_type()), KPC(param_expr));
  }
  if (OB_SUCC(ret) && out_idx != OB_INVALID_INDEX) {
    OZ (check_local_variable_read_only(
      current_block_->get_namespace(), out_idx, PL_PARAM_INOUT == param_mode));
  }
  return ret;
}

int ObPLResolver::check_in_param_type_legal(const ObIRoutineParam *param_info,
                                            const ObRawExpr* param)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(param_info));
  CK (OB_NOT_NULL(param));
  if (OB_SUCC(ret)) {
    ObPLDataType expected_type, actually_type;
    if (param->is_obj_access_expr()) {
      const ObObjAccessRawExpr *obj_access = NULL;
      CK (OB_NOT_NULL(obj_access = static_cast<const ObObjAccessRawExpr*>(param)));
      OZ (obj_access->get_final_type(actually_type));
    } else if (T_QUESTIONMARK == param->get_expr_type()) {
      int64_t var_idx = OB_INVALID_INDEX;
      const ObPLVar *var = NULL;
      CK (OB_NOT_NULL(current_block_));
      OX (var_idx = static_cast<const ObConstRawExpr*>(param)->get_value().get_unknown());
      CK (OB_NOT_NULL(var = current_block_->get_variable(var_idx)));
      OX (actually_type = var->get_pl_data_type());
    } else if (param->has_flag(IS_PL_MOCK_DEFAULT_EXPR)) {
      new(&actually_type)ObPLDataType(ObNullType);
    } else {
      new(&actually_type)ObPLDataType(param->get_data_type());
      actually_type.get_data_type()->set_udt_id(param->get_result_type().get_udt_id());
    }
    if (OB_SUCC(ret)) {
      if (param_info->is_schema_routine_param()) {
        const ObRoutineParam* iparam = static_cast<const ObRoutineParam*>(param_info);
        if (ObParamExternType::SP_EXTERN_DBLINK == iparam->get_extern_type_flag()) {
          const ObUserDefinedType *udt = NULL;
          CK (iparam->get_extended_type_info().count() > 0);
          OZ (resolve_ctx_.package_guard_.dblink_guard_.get_dblink_type_by_name(
              iparam->get_type_owner(), iparam->get_extended_type_info().at(0),
              iparam->get_type_subname(), iparam->get_type_name(), udt));
          CK (OB_NOT_NULL(udt));
          OX (expected_type.set_user_type_id(udt->get_type(), udt->get_user_type_id()));
          OX (expected_type.set_type_from(ObPLTypeFrom::PL_TYPE_DBLINK));
          OZ (expected_type.set_type_info(iparam->get_extended_type_info()));
        } else {
          OZ (pl::ObPLDataType::transform_from_iparam(iparam,
                                                      resolve_ctx_.schema_guard_,
                                                      resolve_ctx_.session_info_,
                                                      resolve_ctx_.allocator_,
                                                      resolve_ctx_.sql_proxy_,
                                                      expected_type));
        }
      } else {
        const ObPLRoutineParam* iparam = static_cast<const ObPLRoutineParam*>(param_info);
        OX (expected_type = iparam->get_type());
      }
    }
    if (OB_SUCC(ret) && !(actually_type == expected_type)) {
      bool is_legal = true;
      if (actually_type.is_cursor_type() && expected_type.is_cursor_type()) {
        // do nothing ...
      } else if (actually_type.is_obj_type() && ObNullType == actually_type.get_obj_type()) {
        // do nothing ...
      } else if (actually_type.is_composite_type() && expected_type.is_composite_type()) {
        uint64_t actual_udt_id = actually_type.get_user_type_id();
        if ((OB_INVALID_ID == actual_udt_id || OB_INVALID_ID == extract_package_id(actual_udt_id)) &&
            resolve_ctx_.is_prepare_protocol_ &&
            actually_type.is_collection_type() &&
            OB_NOT_NULL(resolve_ctx_.params_.param_list_) &&
            T_QUESTIONMARK == param->get_expr_type()) { // anony array
          int64_t index = static_cast<const ObConstRawExpr*>(param)->get_value().get_unknown();
          CK (resolve_ctx_.params_.param_list_->count() > index);
          if (OB_SUCC(ret)) {
            const ObObjParam &param = resolve_ctx_.params_.param_list_->at(index);
            const pl::ObPLComposite *src_composite = NULL;
            const pl::ObPLCollection *src_coll = NULL;
            const pl::ObUserDefinedType *pl_user_type = NULL;
            const pl::ObCollectionType *coll_type = NULL;
            CK (OB_NOT_NULL(src_composite = reinterpret_cast<const ObPLComposite *>(param.get_ext())));
            OZ (resolve_ctx_.get_user_type(expected_type.get_user_type_id(), pl_user_type));
            CK (OB_NOT_NULL(coll_type = static_cast<const ObCollectionType *>(pl_user_type)));
            CK (OB_NOT_NULL(src_coll = static_cast<const ObPLCollection *>(src_composite)));
            if (OB_FAIL(ret)) {
            } else if (coll_type->get_element_type().is_obj_type() ^
                      src_coll->get_element_desc().is_obj_type()) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("incorrect argument type, diff type",
                            K(ret), K(coll_type->get_element_type()), K(src_coll->get_element_desc()));
            } else if (coll_type->get_element_type().is_obj_type()) { // basic data type
              const ObDataType *src_data_type = &src_coll->get_element_desc();
              const ObDataType *dst_data_type = coll_type->get_element_type().get_data_type();
              if (dst_data_type->get_obj_type() == src_data_type->get_obj_type()) {
                // do nothing
              } else if (cast_supported(src_data_type->get_obj_type(),
                                        src_data_type->get_collation_type(),
                                        dst_data_type->get_obj_type(),
                                        dst_data_type->get_collation_type())) {
                // do nothing
              } else {
                ret = OB_INVALID_ARGUMENT;
                LOG_WARN("incorrect argument type, diff type", K(ret));
              }
            } else {
              // element is composite type
              uint64_t element_type_id = src_coll->get_element_desc().get_udt_id();
              is_legal = element_type_id == coll_type->get_element_type().get_user_type_id();
              if (!is_legal) {
                OZ (ObPLResolver::check_composite_compatible(
                  NULL == resolve_ctx_.params_.secondary_namespace_
                      ? static_cast<const ObPLINS&>(resolve_ctx_)
                      : static_cast<const ObPLINS&>(*resolve_ctx_.params_.secondary_namespace_),
                  element_type_id, coll_type->get_element_type().get_user_type_id(), is_legal));
              }
            }
          } else {
            if (actual_udt_id != expected_type.get_user_type_id()) {
              OZ (check_composite_compatible(current_block_->get_namespace(),
                                            actual_udt_id,
                                            expected_type.get_user_type_id(),
                                            is_legal));
            }
          }
        }
      } else if (actually_type.is_composite_type() || expected_type.is_composite_type()) {
        if (actually_type.is_obj_type()
             && ObExtendType == actually_type.get_data_type()->get_obj_type()) {
          is_legal =
              actually_type.get_data_type()->get_udt_id() == expected_type.get_user_type_id();
        } else if (expected_type.is_obj_type()
                    && ObExtendType == expected_type.get_data_type()->get_obj_type()) {
          is_legal =
            expected_type.get_data_type()->get_udt_id() == actually_type.get_user_type_id();
        } else {
          is_legal = false;
        }
      } else { /*do nothing*/ }
      if (OB_SUCC(ret) && !is_legal) {
        ret = OB_ERR_WRONG_TYPE_FOR_VAR;
        LOG_WARN("PLS-00306: wrong number or types of arguments in call stmt",
                 K(ret), K(actually_type), K(expected_type));
      }
    }
  }
  return ret;
}

int ObPLResolver::resolve_cparam_without_assign(ObRawExpr* expr,
                                                const int64_t position,
                                                ObPLFunctionAST &func,
                                                ObIArray<ObRawExpr*> &params,
                                                ObIArray<int64_t> &expr_idxs)
{
  int ret = OB_SUCCESS;
  int64_t expr_idx = OB_INVALID_INDEX;
  CK (OB_NOT_NULL(expr));
  CK (OB_LIKELY(expr_idxs.count() == params.count()));
  CK (OB_LIKELY(position >= 0 && position < params.count()));
  CK (OB_ISNULL(params.at(position)));
  if (OB_SUCC(ret)) {
    if (T_SP_CPARAM == expr->get_expr_type()) {
      ObCallParamRawExpr* call_expr = static_cast<ObCallParamRawExpr*>(expr);
      CK (OB_NOT_NULL(call_expr));
      CK (OB_NOT_NULL(call_expr->get_expr()));
      CK (has_exist_in_array(func.get_exprs(), call_expr->get_expr(), &expr_idx));
    } else {
      OV (has_exist_in_array(func.get_exprs(), expr, &expr_idx), OB_ERR_UNEXPECTED, KPC(expr));
    }
  }
  CK (OB_LIKELY(expr_idx != OB_INVALID_INDEX));
  OX (expr_idxs.at(position) = expr_idx);
  OX (params.at(position) = expr);
  return ret;
}

int ObPLResolver::resolve_cparam_with_assign(ObRawExpr* expr,
                                             const common::ObIArray<ObIRoutineParam*> &params_list,
                                             ObPLFunctionAST &func,
                                             ObIArray<ObRawExpr*> &params,
                                             ObIArray<int64_t> &expr_idx)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(expr));
  CK (OB_LIKELY(T_SP_CPARAM == expr->get_expr_type()));
  if (OB_SUCC(ret)) {
    ObString name;
    int64_t position = -1;
    ObCallParamRawExpr* call_expr = static_cast<ObCallParamRawExpr*>(expr);
    CK (OB_NOT_NULL(call_expr));
    CK (!call_expr->get_name().empty());
    OX (name = call_expr->get_name());
    for (int64_t i = 0; OB_SUCC(ret) && i < params_list.count(); ++i) {
      if (0 == params_list.at(i)->get_name().case_compare(name)) {
        position = i;
        break;
      }
    }
    if (OB_SUCC(ret) && -1 == position) {
      ret = OB_ERR_SP_UNDECLARED_VAR;
      LOG_WARN("can not find param in param list", K(ret), K(position), K(name));
    }
    OZ (resolve_cparam_without_assign(call_expr->get_expr(), position, func, params, expr_idx));
  }
  return ret;
}

int ObPLResolver::resolve_nocopy_params(const ObIRoutineInfo *routine_info,
                                        ObUDFInfo &udf_info)
{
  int ret = OB_SUCCESS;
  ObSEArray<const ObRawExpr*, 16> actual_params_list;
  ObSEArray<ObIRoutineParam*, 16> formal_params_list;
  ObUDFRawExpr *udf_raw_expr = udf_info.ref_expr_;
  CK (OB_NOT_NULL(routine_info));
  CK (OB_NOT_NULL(udf_raw_expr));
  for (int64_t i = 0; OB_SUCC(ret) && i < routine_info->get_param_count(); ++i) {
    ObIRoutineParam* param = NULL;
    OZ (routine_info->get_routine_param(i, param));
    CK (OB_NOT_NULL(param));
    OZ (formal_params_list.push_back(param));
  }
  CK (udf_raw_expr->get_param_count() == formal_params_list.count());
  for (int64_t i = 0; OB_SUCC(ret) && i < formal_params_list.count(); ++i) {
    CK (OB_NOT_NULL(udf_raw_expr->get_param_expr(i)));
    OZ (actual_params_list.push_back(udf_raw_expr->get_param_expr(i)));
  }
  OZ (resolve_nocopy_params(formal_params_list,
                            actual_params_list,
                            udf_raw_expr->get_nocopy_params()));
  return ret;
}

int ObPLResolver::resolve_nocopy_params(const ObIArray<ObIRoutineParam *> &formal_param_list,
                                        ObPLCallStmt *call_stmt)
{
  int ret = OB_SUCCESS;
  ObSEArray<const ObRawExpr*, 16> actual_params_list;
  for (int64_t i = 0; OB_SUCC(ret) && i < formal_param_list.count(); ++i) {
    OZ (actual_params_list.push_back(call_stmt->get_param_expr(i)));
  }
  OZ (resolve_nocopy_params(formal_param_list,
                            actual_params_list,
                            call_stmt->get_nocopy_params()));
  return ret;
}

int ObPLResolver::resolve_nocopy_params(const ObIArray<ObIRoutineParam *> &formal_params_list,
                                        const ObIArray<const ObRawExpr *> &actual_params_list,
                                        ObIArray<int64_t> &nocopy_params)
{
  int ret = OB_SUCCESS;
  CK (formal_params_list.count() == actual_params_list.count());
  for (int64_t i = 0; OB_SUCC(ret) && i < formal_params_list.count(); ++i) {
    nocopy_params.push_back(OB_INVALID_INDEX);
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < formal_params_list.count(); ++i) {
    ObIRoutineParam* formal_param = formal_params_list.at(i);
    pl::ObPLDataType pl_type = formal_param->get_pl_data_type();
    if (formal_param->is_nocopy_param()
        && pl_type.is_obj_type()
        && *(pl_type.get_meta_type()) == (actual_params_list.at(i)->get_result_type())) {
      nocopy_params.at(i) = i;
      for (int64_t j = 0; OB_SUCC(ret) && j < i; ++j) {
        ObIRoutineParam *formal_param = formal_params_list.at(j);
        pl::ObPLDataType tmp_pl_type = formal_param->get_pl_data_type();
        if (tmp_pl_type.is_obj_type()
            && *(pl_type.get_meta_type()) == *(tmp_pl_type.get_meta_type())
            && actual_params_list.at(i)->same_as(*(actual_params_list.at(j)))
            && (formal_param->is_in_param() || formal_param->is_nocopy_param())) {
            nocopy_params.at(j) = i;
        }
      }
    }
  }
  return ret;
}

int ObPLResolver::resolve_cparams(ObIArray<ObRawExpr*> &exprs,
                                  const common::ObIArray<ObIRoutineParam*> &params_list,
                                  ObPLStmt *stmt,
                                  ObPLFunctionAST &func)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(stmt));
  ObSEArray<ObRawExpr*, 32> params;
  ObSEArray<int64_t, 32> expr_idx;
  // Step 1: CHECK, input exprs count must be less than params_list.
  if (exprs.count() > params_list.count()) {
    ret = OB_ERR_SP_WRONG_ARG_NUM;
    LOG_WARN("routine param does not has default value",
              K(ret), K(exprs.count()), K(params_list.count()));
  }
  // Step 2: initilize params array, put all null.
  for (int64_t i = 0; OB_SUCC(ret) && i < params_list.count(); ++i) {
    OZ (params.push_back(NULL));
    OZ (expr_idx.push_back(OB_INVALID_INDEX));
  }
  // Step 3: resolve exprs, put actully param to right position.
  bool has_assign_param = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    if (OB_ISNULL(exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("param node is NULL", K(i), K(ret));
    } else if (T_SP_CPARAM == exprs.at(i)->get_expr_type()) {
      has_assign_param = true;
      OZ (resolve_cparam_with_assign(exprs.at(i), params_list, func, params, expr_idx));
    } else if (has_assign_param) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("can not set param without assign after param with assign", K(ret));
    } else {
      OZ (resolve_cparam_without_assign(exprs.at(i), i, func, params, expr_idx));
    }
  }
  // Step 4: process vacancy parameter, fill default expr otherwise report error.
  for (int64_t i = 0; OB_SUCC(ret) && i < params.count(); ++i) {
    ObIRoutineParam *formal_param = params_list.at(i);
    if (OB_ISNULL(params.at(i))) { // 空缺参数
      if (OB_ISNULL(formal_param)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("routine param is null", K(ret), K(i));
      } else {
        if (PL_CALL == stmt->get_type()) {
          if (formal_param->get_default_value().empty()) {
            ret = OB_ERR_SP_WRONG_ARG_NUM;
            LOG_WARN("routine param does not has default value",
                     K(ret), K(i), K(formal_param->get_default_value()), K(exprs.count()));
          } else {
            ObConstRawExpr *default_expr = NULL;
            OZ (ObRawExprUtils::build_const_int_expr(expr_factory_, ObIntType, 0, default_expr));
            CK (OB_NOT_NULL(default_expr));
            OZ (default_expr->add_flag(IS_PL_MOCK_DEFAULT_EXPR));
            OZ (func.add_expr(default_expr));
            OZ (resolve_cparam_without_assign(default_expr, i, func, params, expr_idx));
          }
        } else if (PL_OPEN == stmt->get_type()
                   || PL_OPEN_FOR == stmt->get_type()
                   || PL_CURSOR_FOR_LOOP == stmt->get_type()) {
          int64_t default_idx = static_cast<ObPLVar *>(formal_param)->get_default();
          if (OB_UNLIKELY(-1 == default_idx)) {
            ret = OB_ERR_SP_WRONG_ARG_NUM;
            LOG_WARN("actual param expr is null", KPC(formal_param), KPC(static_cast<ObPLVar *>(formal_param)), K(ret));
          } else {
            ObConstRawExpr *default_expr = NULL;
            OZ (ObRawExprUtils::build_const_int_expr(expr_factory_, ObIntType, default_idx, default_expr));
            CK (OB_NOT_NULL(default_expr));
            OZ (default_expr->add_flag(IS_PL_MOCK_DEFAULT_EXPR));
            OZ (func.add_expr(default_expr));
            OZ (resolve_cparam_without_assign(default_expr, i, func, params, expr_idx));
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("params using in invalid stmt", K(stmt->get_type()), K(ret));
        }
      }
    } else if (stmt->get_type() != PL_CALL) { // for cursor, need to add convert to param expr
      int64_t idx = OB_INVALID_INDEX;
      CK (PL_CURSOR_FOR_LOOP == stmt->get_type() || PL_OPEN == stmt->get_type());
      CK (OB_NOT_NULL(formal_param));
      OZ (convert_cursor_actual_params(
        params.at(i), formal_param->get_pl_data_type(), func, idx));
      if (OB_SUCC(ret) && idx != OB_INVALID_INDEX) {
        expr_idx.at(i) = idx;
      }
    }
  }
  // Step 5: add params to stmt.
  if (OB_SUCC(ret)) {
    if (PL_CALL == stmt->get_type()) {
      for (int64_t i = 0; OB_SUCC(ret) && i < params.count(); ++i) {
        const ObIRoutineParam* param_info = params_list.at(i);
        ObPLRoutineParamMode param_mode = PL_PARAM_INVALID;
        int64_t out_idx = OB_INVALID_INDEX;
        CK (OB_NOT_NULL(param_info));
        OX (param_mode = static_cast<ObPLRoutineParamMode>(param_info->get_mode()));
        CK (param_mode != PL_PARAM_INVALID);
        CK (OB_NOT_NULL(params.at(i)));
        if (OB_SUCC(ret)
            && (PL_PARAM_INOUT == param_mode || PL_PARAM_OUT == param_mode)) {
          OZ (resolve_inout_param(params.at(i), param_mode, out_idx), K(i), K(params), K(exprs));
        }
        if (OB_SUCC(ret)
            && OB_LIKELY(OB_INVALID_INDEX != expr_idx.at(i))) {
          OZ (static_cast<ObPLCallStmt*>(stmt)->add_param(expr_idx.at(i), param_mode, out_idx));
        }
      }
      OZ (resolve_nocopy_params(params_list, static_cast<ObPLCallStmt*>(stmt)));
    } else if (PL_CURSOR_FOR_LOOP == stmt->get_type()) {
      OZ (static_cast<ObPLCursorForLoopStmt*>(stmt)->set_params(expr_idx));
    } else { //must be Open Stmt
      OZ (static_cast<ObPLOpenStmt*>(stmt)->set_params(expr_idx));
    }
  }
  // Step 6: check input parameter legal
  if (OB_SUCC(ret) && PL_CALL == stmt->get_type()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < params.count(); ++i) {
      OZ (check_in_param_type_legal(params_list.at(i), params.at(i)));
    }
  }
  return ret;
}

int ObPLResolver::resolve_call_param_list(ObIArray<ObRawExpr*> &params,
                                          const common::ObIArray<ObPLRoutineParam *> &params_list,
                                          ObPLCallStmt *stmt,
                                          ObPLFunctionAST &func)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObIRoutineParam*, 16> iparams;
  CK (OB_NOT_NULL(stmt));
  for (int64_t i = 0; OB_SUCC(ret) && i < params_list.count(); ++i) {
    OZ (iparams.push_back(params_list.at(i)));
  }
  OZ (resolve_cparams(params, iparams, stmt, func));
  return ret;
}

int ObPLResolver::resolve_call_param_list(ObIArray<ObRawExpr*> &params,
                                          const ObIArray<ObRoutineParam*> &params_list,
                                          ObPLCallStmt *stmt,
                                          ObPLFunctionAST &func)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObIRoutineParam*, 16> iparams;
  CK (OB_NOT_NULL(stmt));
  for (int64_t i = 0; OB_SUCC(ret) && i < params_list.count(); ++i) {
    OZ (iparams.push_back(params_list.at(i)));
  }
  OZ (resolve_cparams(params, iparams, stmt, func));
  return ret;
}

int ObPLResolver::resolve_call_param_list(const ObStmtNodeTree *parse_tree,
                                          const common::ObIArray<ObPLRoutineParam *> &params_list,
                                          ObPLCallStmt *stmt,
                                          ObPLFunctionAST &func)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 32> exprs;
  ObSEArray<ObIRoutineParam*, 16> iparams;
  CK (OB_NOT_NULL(stmt));
  CK (OB_NOT_NULL(parse_tree));
  OZ (resolve_cparams_expr(parse_tree, func, exprs));
  for (int64_t i = 0; OB_SUCC(ret) && i < params_list.count(); ++i) {
    OZ (iparams.push_back(params_list.at(i)));
  }
  OZ (resolve_cparams(exprs, iparams, stmt, func));
  return ret;
}

int ObPLResolver::resolve_call_param_list(const ObStmtNodeTree *parse_tree,
                                          const ObIArray<ObRoutineParam*> &params_list,
                                          ObPLCallStmt *stmt,
                                          ObPLFunctionAST &func)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 32> exprs;
  ObSEArray<ObIRoutineParam*, 16> iparams;
  CK (OB_NOT_NULL(stmt));
  CK (OB_NOT_NULL(parse_tree));
  OZ (resolve_cparams_expr(parse_tree, func, exprs));
  for (int64_t i = 0; OB_SUCC(ret) && i < params_list.count(); ++i) {
    OZ (iparams.push_back(params_list.at(i)));
  }
  OZ (resolve_cparams(exprs, iparams, stmt, func));
  return ret;
}

int ObPLResolver::check_cursor_formal_params(
  const ObIArray<int64_t>& formal_params, ObPLCursor &cursor, bool &legal)
{
  int ret = OB_SUCCESS;

  const ObPLSymbolTable *left_symbol_table = NULL;
  const ObPLSymbolTable *right_symbol_table = NULL;
  const ObIArray<ObRawExpr *> *left_expr_table = NULL;
  const ObIArray<ObRawExpr *> *right_expr_table = NULL;
  OX (legal = true);
  CK (OB_NOT_NULL(current_block_));
  CK (OB_NOT_NULL(left_symbol_table = current_block_->get_namespace().get_symbol_table()));
  CK (OB_NOT_NULL(left_expr_table = current_block_->get_namespace().get_exprs()));
  if (OB_FAIL(ret)) {
  } else if (formal_params.count() != cursor.get_formal_params().count()) {
    legal = false;
  } else if (cursor.get_package_id() != current_block_->get_namespace().get_package_id()
      || cursor.get_routine_id() != current_block_->get_namespace().get_routine_id()) {
    // external cursor
    CK (OB_NOT_NULL(current_block_->get_namespace().get_external_ns()));
    CK (OB_NOT_NULL(current_block_->get_namespace().get_external_ns()->get_parent_ns()));
    CK (OB_NOT_NULL(right_symbol_table
      = current_block_->get_namespace().get_external_ns()->get_parent_ns()->get_symbol_table()));
    CK (OB_NOT_NULL(right_expr_table
      = current_block_->get_namespace().get_external_ns()->get_parent_ns()->get_exprs()));
  } else {
    // local cursor
    CK (OB_NOT_NULL(right_symbol_table = left_symbol_table));
    CK (OB_NOT_NULL(right_expr_table = left_expr_table));
  }
  for (int64_t i = 0; OB_SUCC(ret) && legal && i < formal_params.count(); ++i) {
    const ObPLVar* left_var = left_symbol_table->get_symbol(formal_params.at(i));
    const ObPLVar* right_var = right_symbol_table->get_symbol(cursor.get_formal_params().at(i));
    const ObRawExpr *left_default_expr = NULL;
    const ObRawExpr *right_default_expr = NULL;
    CK (OB_NOT_NULL(left_var));
    CK (OB_NOT_NULL(right_var));
    CK ((-1 == left_var->get_default()) || (left_var->get_default() < left_expr_table->count()));
    CK ((-1 == right_var->get_default()) || (right_var->get_default() < right_expr_table->count()));
    OX (left_default_expr
      = (-1 == left_var->get_default()) ? NULL : left_expr_table->at(left_var->get_default()));
    OX (right_default_expr
      = (-1 == right_var->get_default()) ? NULL : right_expr_table->at(right_var->get_default()));
    if (OB_FAIL(ret)) {
    } else if (left_var->get_name() != right_var->get_name()
        || !(left_var->get_type() == right_var->get_type())
        || left_var->is_readonly() != right_var->is_readonly()
        || left_var->is_not_null() != right_var->is_not_null()
        || left_var->is_default_construct() != right_var->is_default_construct()
        || left_var->is_formal_param() != right_var->is_formal_param()) {
      legal = false;
    }
    if (OB_SUCC(ret) && legal && left_default_expr != right_default_expr) {
      if (OB_ISNULL(left_default_expr) || OB_ISNULL(right_default_expr)) {
        legal = false;
      } else if (!left_default_expr->same_as(*right_default_expr)) {
        legal = false;
      }
    }
  }
  return ret;
}

int ObPLResolver::replace_cursor_formal_params(const ObIArray<int64_t> &src_formal_exprs,
                                               const ObIArray<int64_t> &dst_formal_exprs,
                                               uint64_t src_package_id,
                                               uint64_t dst_package_id,
                                               ObIArray<ObRawExpr *> &sql_params)
{
  int ret = OB_SUCCESS;
  if (src_package_id != dst_package_id) {
    for (int64_t i = 0; OB_SUCC(ret) && i < sql_params.count(); ++i) {
      ObRawExpr* raw_expr = sql_params.at(i);
      CK (OB_NOT_NULL(raw_expr));
      OZ (replace_cursor_formal_params(
        src_formal_exprs, dst_formal_exprs, src_package_id, dst_package_id, *raw_expr));
    }
  }
  return ret;
}

int ObPLResolver::replace_cursor_formal_params(const ObIArray<int64_t> &src_formal_exprs,
                                               const ObIArray<int64_t> &dst_formal_exprs,
                                               uint64_t src_package_id,
                                               uint64_t dst_package_id,
                                               ObRawExpr &expr)
{
  int ret = OB_SUCCESS;
  if (expr.get_expr_type() == T_OP_GET_PACKAGE_VAR) {
    ObSysFunRawExpr &f_expr = static_cast<ObSysFunRawExpr &>(expr);
    uint64_t package_id = OB_INVALID_ID;
    int64_t var_idx = OB_INVALID_ID;
    CK (f_expr.get_param_count() >= 2);
    OZ (ObRawExprUtils::get_package_var_ids(&f_expr, package_id, var_idx));
    if (OB_SUCC(ret) && package_id == src_package_id) {
      for (int64_t i = 0; OB_SUCC(ret) && i < src_formal_exprs.count(); ++i) {
        if (var_idx == src_formal_exprs.at(i)) {
          OZ (ObRawExprUtils::set_package_var_ids(
            &f_expr, dst_package_id, dst_formal_exprs.at(i)));
          break;
        }
      }
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr.get_param_count(); ++i) {
      ObRawExpr *param = expr.get_param_expr(i);
      CK (OB_NOT_NULL(param));
      OZ (replace_cursor_formal_params(
        src_formal_exprs, dst_formal_exprs, src_package_id, dst_package_id, *param));
    }
  }
  return ret;
}

int ObPLResolver::resolve_cursor_def(const ObString &cursor_name,
                                      const ObStmtNodeTree *sql_node,
                                      ObPLBlockNS &sql_ns,
                                      ObPLDataType &cursor_type,
                                      const ObIArray<int64_t> &formal_params,
                                      ObPLCompileUnitAST &func,
                                      int64_t &cursor_index) // CURSOR 在符号表中的下标
{
  int ret = OB_SUCCESS;
  ObRecordType *record_type = NULL;
  ObArray<int64_t> expr_idxs;
  int64_t index = common::OB_INVALID_INDEX;
  sql::ObSPIService::ObSPIPrepareResult prepare_result;
  CK (OB_NOT_NULL(sql_node));
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(record_type = static_cast<ObRecordType*>(resolve_ctx_.allocator_.alloc(sizeof(ObRecordType))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory", K(ret));
    } else {
      int64_t current_time = ObTimeUtility::current_time();
      ObSqlString record_name;
      char* name_buf = NULL;
      if (OB_FAIL(record_name.append_fmt("__for_loop_cursor_record_name_%ld", current_time))) {
        LOG_WARN("failed to generate cursor record name", K(ret));
      } else if (OB_ISNULL(name_buf = static_cast<char*>(resolve_ctx_.allocator_.alloc(record_name.length() + 1)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate record name", K(ret));
      } else {
        record_name.to_string(name_buf, record_name.length() + 1);
        record_type = new(record_type)ObRecordType();
        record_type->set_name(ObString(record_name.length(), name_buf));
        record_type->set_type_from(PL_TYPE_ATTR_ROWTYPE);
        prepare_result.record_type_ = record_type;
        prepare_result.tg_timing_event_ =
                                static_cast<TgTimingEvent>(resolve_ctx_.params_.tg_timing_event_);
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObSPIService::spi_prepare(resolve_ctx_.allocator_,
                                          resolve_ctx_.session_info_,
                                          resolve_ctx_.sql_proxy_,
                                          resolve_ctx_.schema_guard_,
                                          expr_factory_,
                                          sql_node->str_value_,
                                          true, /*is_cursor*/
                                          &sql_ns,
                                          prepare_result))) {
      LOG_WARN("failed to prepare stmt", K(ret));
    } else if (!prepare_result.into_exprs_.empty()
               && lib::is_mysql_mode()) { // oracle不报错,会忽略掉INTO
      ret = OB_ER_SP_BAD_CURSOR_SELECT;
      LOG_USER_ERROR(OB_ER_SP_BAD_CURSOR_SELECT);
      LOG_WARN("Sql with into clause should not in Declare cursor", K(prepare_result.route_sql_), K(ret));
    } else if (OB_FAIL(func.add_sql_exprs(prepare_result.exec_params_))) {
      LOG_WARN("failed to set precalc exprs", K(prepare_result.exec_params_), K(ret));
    }
    if (OB_SUCC(ret)) {
      if (prepare_result.for_update_) {
        func.set_modifies_sql_data();
      } else if (!func.is_modifies_sql_data()) {
        func.set_reads_sql_data();
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(func.add_dependency_objects(prepare_result.ref_objects_))) {
        LOG_WARN("failed to set ref object", K(prepare_result.ref_objects_), K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    for (int64_t i = func.get_expr_count() - prepare_result.exec_params_.count();
      OB_SUCC(ret) && i < func.get_expr_count();
      ++i) {
      if (OB_FAIL(expr_idxs.push_back(i))) {
        LOG_WARN("push_back error", K(i), K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(record_type)) {
    OZ (current_block_->get_namespace().add_type(record_type));
    if (OB_FAIL(ret)) {
    } else if (!cursor_type.is_valid_type()) {
      cursor_type.set_user_type_id(record_type->get_type(), record_type->get_user_type_id());
      cursor_type.set_type_from(record_type->get_type_from());
    } else {
      ObArenaAllocator allocator;
      const ObUserDefinedType *cursor_user_type = NULL;
      if (OB_FAIL(current_block_->get_namespace().get_user_type(cursor_type.get_user_type_id(),
                                                                cursor_user_type, &allocator))) {
        LOG_WARN("failed to get user type", K(cursor_type), K(ret));
      } else if (OB_ISNULL(cursor_user_type)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get cursor type", K(cursor_type), K(ret));
      } else if (!cursor_user_type->is_record_type()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("cursor must return record type", K(*cursor_user_type), K(ret));
      } else {
        bool is_compatible = false;
        const ObRecordType *return_type = static_cast<const ObRecordType*>(cursor_user_type);
        if (OB_FAIL(record_type->is_compatble(*return_type, is_compatible))) {
          LOG_WARN("failed to check compatible", K(*record_type), K(*return_type), K(ret));
        } else if (!is_compatible) {
          ret = OB_OBJ_TYPE_ERROR;
          LOG_WARN("type not compatible", K(*record_type), K(*return_type), K(ret));
        } else { /*do nothing*/ }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_INVALID_ID ==  cursor_index) {
      ObPLDataType type(PL_CURSOR_TYPE);
      type.set_user_type_id(PL_CURSOR_TYPE, record_type->get_user_type_id());
      if (OB_FAIL(current_block_->get_namespace().add_cursor(cursor_name,
                                                             type,
                                                             prepare_result.route_sql_,
                                                             expr_idxs,
                                                             prepare_result.ps_sql_,
                                                             prepare_result.type_,
                                                             prepare_result.for_update_,
                                                             prepare_result.has_hidden_rowid_,
                                                             prepare_result.rowid_table_id_,
                                                             prepare_result.ref_objects_,
                                                             record_type,
                                                             cursor_type,
                                                             formal_params,
                                                             ObPLCursor::DEFINED,
                                                             prepare_result.has_dup_column_name_,
                                                             index))) {
        LOG_WARN("failed to add cursor to symbol table",
                 K(cursor_name),
                 K(sql_node->str_value_),
                 K(prepare_result.exec_params_),
                 K(prepare_result.ps_sql_),
                 K(prepare_result.type_),
                 K(prepare_result.for_update_),
                 K(ret));
      } else {
        cursor_index = index;
      }
    } else {
      ObPLCursor *cursor = current_block_->get_cursor(cursor_index);
      if (OB_ISNULL(cursor)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("cursor NULL", K(cursor_index), K(cursor_name), K(ret));
      } else {
        const ObPLVar *var = nullptr;
        OZ (current_block_->get_namespace().get_cursor_var(
        cursor->get_package_id(), cursor->get_routine_id(), cursor->get_index(), var));
        CK (OB_NOT_NULL(var));
        if (OB_SUCC(ret)) {
          ObPLDataType type(PL_CURSOR_TYPE);
          type.set_user_type_id(PL_CURSOR_TYPE, record_type->get_user_type_id());
          const_cast<ObPLVar *>(var)->set_type(type);
        }
        /*
         * 检查Oracle模式游标是否有重定义：
         * Oracle在游标重定义方面的表现很奇怪，如果同一个游标名被先声明后定义的原型不同（参数或者返回值类型不同），
         * 甚至同一个游标名被定义了两次，只要这个游标没有被访问过，就不会报错。
         * 所以如果我们在定义的时候找到以前declare或define过，那么要先检查是否是合法的define。
         * 对于重复定义，或者声明和定义不一致，我们不能在这里报错，而必须在访问游标时才报错，所以在这里只设置游标状态，
         * 而等待在resolve_cursor的时候检查出来在报错。
         * */
        bool same_cursor_declare = false;
        if (ObPLCursor::DECLARED == cursor->get_state()
            && cursor_type == cursor->get_cursor_type()) {
          OZ (check_cursor_formal_params(formal_params, *cursor, same_cursor_declare));
        }
        if (OB_FAIL(ret)) {
        } else if (same_cursor_declare) {
          // 声明在包头，定义在包体，sql中引用的cursor参数符号在包体中，此时需要替换成包头中的符号
          OZ (replace_cursor_formal_params(formal_params,
                                           cursor->get_formal_params(),
                                           current_block_->get_namespace().get_package_id(),
                                           cursor->get_package_id(),
                                           prepare_result.exec_params_));
          //仅当原型和声明完全一致时，才是合法
          OZ (cursor->set(prepare_result.route_sql_,
                          expr_idxs,
                          prepare_result.ps_sql_,
                          prepare_result.type_,
                          prepare_result.for_update_,
                          record_type,
                          cursor_type,
                          ObPLCursor::DEFINED,
                          prepare_result.ref_objects_,
                          cursor->get_formal_params(),
                          prepare_result.has_dup_column_name_));
          if (OB_SUCC(ret)
          && (cursor->get_package_id() != current_block_->get_namespace().get_package_id()
              || cursor->get_routine_id() != current_block_->get_namespace().get_routine_id())) {
            const ObPLCursor *external_cursor = NULL;
            CK (OB_NOT_NULL(current_block_->get_namespace().get_external_ns()));
            CK (OB_NOT_NULL(current_block_->get_namespace().get_external_ns()->get_parent_ns()));
            OZ (current_block_->get_namespace().get_external_ns()->get_parent_ns()->get_cursor(
                cursor->get_package_id(),
                cursor->get_routine_id(),
                cursor->get_index(),
                external_cursor));
            if (OB_SUCC(ret)) {
              if (OB_ISNULL(external_cursor)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("cursor NULL", K(cursor_index), K(cursor_name), K(ret));
              } else {
                OZ (const_cast<ObPLCursor*>(external_cursor)->set(prepare_result.route_sql_,
                                          expr_idxs,
                                          prepare_result.ps_sql_,
                                          prepare_result.type_,
                                          prepare_result.for_update_,
                                          record_type,
                                          cursor_type,
                                          ObPLCursor::DEFINED,
                                          prepare_result.ref_objects_,
                                          external_cursor->get_formal_params(),
                                          prepare_result.has_dup_column_name_),
                                          K(formal_params),
                                          K(external_cursor->get_formal_params()));
              }
            }
          }
        } else if (ObPLCursor::DEFINED == cursor->get_state()) {
          // already defined, do not defined it agine.
          ret = OB_ERR_ATTR_FUNC_CONFLICT;
          LOG_USER_ERROR(OB_ERR_ATTR_FUNC_CONFLICT, cursor_name.length(), cursor_name.ptr());
        } else { //不合法的定义不用去做define，直接设置cursor状态
          cursor->set_state(ObPLCursor::DUP_DECL);
        }
      }
    }
  }
  return ret;
}

int ObPLResolver::resolve_declare_cursor(const ObStmtNodeTree *parse_tree, ObPLPackageAST &func)
{
  int ret = OB_SUCCESS;
  OZ (resolve_declare_cursor(parse_tree, NULL, func));
  return ret;
}

int ObPLResolver::resolve_declare_cursor(
  const ObStmtNodeTree *parse_tree, ObPLDeclareCursorStmt *stmt, ObPLCompileUnitAST &func)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(parse_tree) || OB_ISNULL(current_block_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("parse_tree is NULL", K(parse_tree), K(current_block_), K(ret));
  } else {
    //解析name
    const ObStmtNodeTree *name_node = parse_tree->children_[0];
    const ObStmtNodeTree *param_node = parse_tree->children_[1];
    const ObStmtNodeTree *type_node = parse_tree->children_[2];
    const ObStmtNodeTree *sql_node = parse_tree->children_[3];
    if (OB_ISNULL(name_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("parse_tree is NULL", K(name_node), K(ret));
    } else if (lib::is_mysql_mode() && (NULL != type_node || NULL == sql_node)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("cursor in mysql mode must has no type node and has a valid sql node", K(type_node), K(sql_node), K(ret));
    } else if (lib::is_oracle_mode() && NULL ==type_node && NULL == sql_node) {
      ret = OB_ERR_CURSOR_NO_RETURN_TYPE;
      LOG_WARN("cursor declare must has retrun type", K(type_node), K(sql_node), K(ret));
    } else {
      ObString name;
      ObPLDataType return_type;
      ObArray<int64_t> formal_params;
      int64_t cursor_index = common::OB_INVALID_INDEX;
      ObPLStmtBlock *current_block = current_block_;
      ObPLStmtBlock *cursor_block = current_block_;
      question_mark_cnt_ = parse_tree->value_;

      OZ (resolve_cursor_common(name_node, type_node, func, name, return_type));

      if (OB_SUCC(ret) && NULL != param_node) {
        if (func.is_routine()) {
          OZ (make_block(
            static_cast<ObPLFunctionAST&>(func), current_block, cursor_block));
          OX (set_current(*cursor_block));
        } else {
          OZ (make_block(static_cast<ObPLPackageAST&>(func), cursor_block));
          OX (cursor_block->get_namespace().set_pre_ns(
            NULL == current_block ? NULL : &current_block->get_namespace()));
          OX (set_current(*cursor_block));
        }
        OZ (resolve_cursor_formal_param(param_node, func, formal_params));
        OX (set_current(*current_block));
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(resolve_cursor(name,
                                   current_block_->get_namespace(),
                                   cursor_index,
                                   func,
                                   true/*check mode*/))) {
          LOG_WARN("failed to resolve cursor", K(ret), K(name));
        } else if (lib::is_mysql_mode() && OB_INVALID_INDEX != cursor_index) {
          ret = OB_ERR_SP_DUP_CURSOR;
          LOG_WARN("Duplicate cursor", K(name),K(ret));
          LOG_USER_ERROR(OB_ERR_SP_DUP_CURSOR, name.length(), name.ptr());
        } else if (NULL == sql_node) { //only declare
          if (OB_INVALID_INDEX == cursor_index) { //没有declare过，添加进符号表
            if (OB_FAIL(current_block_->get_namespace().add_cursor(name,
                                                                   ObPLDataType(PL_CURSOR_TYPE),
                                                                   ObString(),
                                                                   ObArray<int64_t>(),
                                                                   ObString(),
                                                                   stmt::T_NONE,
                                                                   false, //for update
                                                                   false, //hidden rowid
                                                                   OB_INVALID_ID,
                                                                   ObArray<ObSchemaObjVersion>(),
                                                                   NULL,
                                                                   return_type,
                                                                   formal_params,
                                                                   ObPLCursor::DECLARED,
                                                                   false,
                                                                   cursor_index))) {
              LOG_WARN("failed to add cursor to symbol table", K(name), K(ret));
            } else if (OB_NOT_NULL(stmt)) {
              stmt->set_cursor_index(cursor_index);
            } else { /*do nothing*/ }
          } else {
            ret = OB_ERR_SP_DUP_CURSOR;
            LOG_WARN("PLS-00305: previous use of cursor conflicts with this use", K(name),K(ret));
          }
        } else { //declare and define
          if (OB_FAIL(resolve_cursor_def(name, sql_node, cursor_block->get_namespace(),
                                         return_type, formal_params, func, cursor_index))) {
            LOG_WARN("failed to resolve cursor comm", K(ret), K(name));
          } else if (OB_NOT_NULL(stmt)) {
            stmt->set_cursor_index(cursor_index);
          } else { /*do nothing*/ }
        }
      }
    }
  }
  return ret;
}

int ObPLResolver::resolve_cursor_common(const ObStmtNodeTree *name_node,
                                        const ObStmtNodeTree *type_node,
                                        ObPLCompileUnitAST &func,
                                        ObString &name,
                                        ObPLDataType &return_type)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(resolve_ident(name_node, name))) {
    LOG_WARN("failed to resolve ident", K(name_node), K(ret));
  } else if (NULL != type_node) {
    if (OB_FAIL(resolve_sp_data_type(type_node, name, func, return_type))) {
      LOG_WARN("failed to resolve return type", K(type_node), K(name), K(ret));
    } else if (!return_type.is_record_type()) {
      ret = OB_ERR_TYPE_DECL_MALFORMED;
      LOG_WARN("PLS-00320: the declaration of the type of this expression is incomplete or malformed",
               K(ret), K(return_type));
    }
  } else { /*do nothing*/ }
  return ret;
}

int ObPLResolver::resolve_cursor_formal_param(
  const ObStmtNodeTree *param_list, ObPLCompileUnitAST &func, ObIArray<int64_t> &params)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(current_block_) || OB_ISNULL(param_list)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("argument invalid", K(current_block_), K(param_list), K(ret));
  } else if (param_list->type_ != T_SP_PARAM_LIST || OB_ISNULL(param_list->children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param list type is invalid", K(param_list->type_), K(param_list->children_), K(ret));
  } else {
    ObString param_name;
    ObPLDataType param_type;
    for (int64_t i = 0; OB_SUCC(ret) && i < param_list->num_child_; ++i) {
      const ParseNode *param_node = param_list->children_[i];
      const ParseNode *name_node = NULL;
      const ParseNode *type_node = NULL;
      param_name.reset();
      param_type.reset();
      if (OB_ISNULL(param_node)
          || OB_UNLIKELY(param_node->type_ != T_SP_PARAM)
          || OB_ISNULL(param_node->children_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("param node is invalide", K(param_node), K_(param_node->children));
      } else if (OB_ISNULL(name_node = param_node->children_[0])
          || OB_ISNULL(type_node = param_node->children_[1])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("name node or type node is null", K(name_node), K(type_node));
      } else {
        ObRawExpr *default_expr = NULL;
        param_name.assign_ptr(name_node->str_value_, static_cast<int32_t>(name_node->str_len_));
        if (OB_FAIL(resolve_sp_data_type(type_node, param_name, func, param_type))) {
          LOG_WARN("resolve data type failed", K(ret), K(param_name));
        } else if (MODE_IN != param_node->value_) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("param inout flag is invalid", K(param_node->value_), K(ret));
        } else if (OB_NOT_NULL(param_node->children_[2])) {
          ParseNode* default_node = param_node->children_[2];
          if (default_node->type_ != T_SP_DECL_DEFAULT) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("default node type is unexpected", K(ret));
          } else if (OB_FAIL(resolve_expr(default_node->children_[0],
                                          func, default_expr,
                                          combine_line_and_col(default_node->stmt_loc_),
                                          true /*need_add*/,
                                          &param_type))) {
            LOG_WARN("failed to resolve default expr", K(ret));
          } else if (OB_ISNULL(default_expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to resolve default expr", K(ret), K(default_expr));
          } else { /*do nothing*/ }
        }

        if (OB_SUCC(ret)) {
          if (OB_FAIL(current_block_->get_namespace().add_symbol(param_name, param_type, default_expr,
                                                                 false, false, false, true))) {
            LOG_WARN("failed to add symbol", K(param_name), K(param_type), K(default_expr), K(ret));
          } else if (OB_FAIL(params.push_back(current_block_->get_namespace().get_symbol_table()->get_count() - 1))) {
            LOG_WARN("push back error", K(params), K(param_type), K(default_expr), K(ret));
          } else { /*do nothing*/ }
        }
      }
    }
  }
  return ret;
}

#ifdef OB_BUILD_ORACLE_PL
int ObPLResolver::resolve_declare_ref_cursor(
  const ObStmtNodeTree *parse_tree, ObPLDeclareCursorStmt *stmt, ObPLFunctionAST &func)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(parse_tree) || OB_ISNULL(stmt) || OB_ISNULL(current_block_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("parse_tree is NULL", K(parse_tree), K(stmt), K(current_block_), K(ret));
  } else {
    const ObStmtNodeTree *name_node = parse_tree->children_[0];
    const ObStmtNodeTree *type_node = parse_tree->children_[1];
    ObPLDataType data_type;
    ObString ident_name;
    CK (OB_NOT_NULL(type_node));
    OZ (resolve_sp_data_type(type_node, ident_name, func, data_type));
    CK (data_type.is_cursor_type());
    CK (OB_NOT_NULL(name_node));
    if (OB_SUCC(ret)) {
      if (1 != name_node->num_child_) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("name list for ref cursor is not supported yet", K(name_node->num_child_), K(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "name list for ref cursor");
      } else {
        OZ (resolve_ident(name_node->children_[0], ident_name));
      }
    }

    if (OB_SUCC(ret)) {
      ObArenaAllocator allocator;
      const ObUserDefinedType *cursor_type = NULL;
      const ObUserDefinedType *return_type = NULL;
      int64_t index = OB_INVALID_INDEX;
      if (OB_FAIL(current_block_->get_namespace().get_user_type(data_type.get_user_type_id(),
          cursor_type, &allocator))) {
        LOG_WARN("failed to get user type", K(data_type), K(ret));
      } else if (OB_ISNULL(cursor_type)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get cursor type", K(data_type), K(ret));
      } else {
        uint64_t return_type_id = static_cast<const ObRefCursorType*>(cursor_type)->get_return_type_id();
        if (OB_INVALID_ID != return_type_id) {
          if (OB_FAIL(current_block_->get_namespace().get_user_type(return_type_id, return_type))) {
            LOG_WARN("failed to get user type", K(*cursor_type), K(ret));
          } else if (OB_ISNULL(return_type)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to get cursor type", K(*cursor_type), K(ret));
          } else { /*do nothing*/ }
        }
      }

      if (OB_SUCC(ret)) {
        ObString dummy_sql;
        ObArray<int64_t> dummy_params;
        sql::stmt::StmtType dummy_stmt_type = sql::stmt::T_NONE;
        bool dummy_for_update = false;
        bool dummy_hidden_rowid = false;
        common::ObArray<ObSchemaObjVersion> dummy_ref_objects;
        const ObPLDataType dummy_return_type;
        const ObArray<int64_t> dummy_formal_params;
        if (OB_FAIL(current_block_->get_namespace().add_cursor(ident_name,
                                                               data_type,
                                                               dummy_sql,
                                                               dummy_params,
                                                               dummy_sql,
                                                               dummy_stmt_type,
                                                               dummy_for_update,
                                                               dummy_hidden_rowid,
                                                               OB_INVALID_ID,
                                                               dummy_ref_objects,
                                                               NULL, /*ref cursor的row desc不确定*/
                                                               NULL == return_type ? dummy_return_type : *return_type,
                                                               dummy_formal_params,
                                                               ObPLCursor::DECLARED,
                                                               false,
                                                               index))) {
           LOG_WARN("failed to add cursor to symbol table", K(ident_name), K(return_type), K(index), K(ret));
        } else {
          stmt->set_cursor_index(index);
        }
      }
    }
  }
  return ret;
}
#endif

int ObPLResolver::resolve_open(
  const ObStmtNodeTree *parse_tree, ObPLOpenStmt *stmt, ObPLFunctionAST &func)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(parse_tree) || OB_ISNULL(stmt) || OB_ISNULL(current_block_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("parse_tree is NULL", K(parse_tree), K(stmt), K(current_block_), K(ret));
  } else {
    // 解析Cursor
    const ObStmtNodeTree *name_node = parse_tree->children_[0];
    int64_t index = OB_INVALID_INDEX;
    OZ (resolve_cursor(name_node, current_block_->get_namespace(), index, func));
    OX (stmt->set_cursor_index(index));
    if (OB_SUCC(ret)) {
      const ObPLVar *var = NULL;
      if (OB_FAIL(stmt->get_var(var))) {
        LOG_WARN("failed to get var", K(ret));
      } else if (NULL == var) {
        //非local cursor，一定不是只读的
      } else if (var->get_type().is_ref_cursor_type() && stmt->get_type() != PL_OPEN_FOR) {
        ret = OB_ERR_EXPRESSION_WRONG_TYPE;
        LOG_WARN("use open stmt with a ref cursor is not property.", K(ret), K(index));
      } else if (var->is_readonly()) {
        /* 也有可能是子过程的parent cursor，不应是in的cursor
         *create or replace procedure subproc is
          cur sys_refcursor;
          procedure subproc1 is
          begin
          open cur for select * from tbl_xxx;
          end;
          begin
          subproc1;
          close cur;
          end;
         */
        ret = OB_ERR_IN_CURSOR_OPEND;
        LOG_WARN("PLS-00361: IN cursor cannot be OPEN'ed", K(ret), KPC(var));
      } else {
        // 判断是否对外部cursor进行open，这儿会决定这个cursor的内存是否在session内存上
        ObPLCursor *cursor = NULL;
        cursor = current_block_->get_namespace().get_cursor_table()->get_cursor(index);
        if (OB_SUCC(ret) && OB_NOT_NULL(cursor)) {
          if (cursor->is_for_update()) {
            func.set_modifies_sql_data();
          }
          if (PL_REF_CURSOR_TYPE == cursor->get_cursor_type().get_type()) {
            if (cursor->get_package_id() != current_block_->get_namespace().get_package_id()
                || cursor->get_routine_id() != current_block_->get_namespace().get_routine_id()) {
              func.set_open_external_ref_cursor();
              func.set_external_state();
            }
          } else {
            if (!var->get_pl_data_type().is_ref_cursor_type()
                && ObPLCursor::DEFINED != cursor->get_state()) {
              ret = OB_ERR_TYPE_DECL_MALFORMED;
              LOG_WARN("cursor not defined", KPC(var), KPC(cursor), K(ret));
            }
          }
        }
      }
    }
  }

  //解析实参
  if (OB_SUCC(ret)) {
    const ObStmtNodeTree *param_node = parse_tree->children_[1];
    if (OB_FAIL(resolve_cursor_actual_params(param_node, stmt, func))) {
      LOG_WARN("failed to resolve cursor actual params", K(ret));
    }
  }
  return ret;
}

int ObPLResolver::convert_cursor_actual_params(
  ObRawExpr *expr, ObPLDataType pl_data_type, ObPLFunctionAST &func, int64_t &idx)
{
  int ret = OB_SUCCESS;
  ObDataType *data_type = pl_data_type.get_data_type();
  ObRawExpr *convert_expr = expr;
  if (T_SP_CPARAM == expr->get_expr_type()) {
    ObCallParamRawExpr *call_expr = static_cast<ObCallParamRawExpr *>(expr);
    CK (OB_NOT_NULL(call_expr));
    CK (OB_NOT_NULL(call_expr->get_expr()));
    OX (convert_expr = call_expr->get_expr());
  }
  CK (OB_NOT_NULL(convert_expr));
  if (OB_FAIL(ret)) {
  } else if (OB_NOT_NULL(data_type)) {
    OZ (ObRawExprUtils::build_column_conv_expr(&resolve_ctx_.session_info_,
                                               expr_factory_,
                                               data_type->get_obj_type(),
                                               data_type->get_collation_type(),
                                               data_type->get_accuracy_value(),
                                               true,
                                               NULL,
                                               NULL,
                                               convert_expr));
    OZ (func.add_expr(convert_expr));
    OX (idx = func.get_exprs().count() - 1);
  } else if (pl_data_type.is_cursor_type()) {
    if (convert_expr->get_result_type().get_extend_type() != PL_CURSOR_TYPE
        && convert_expr->get_result_type().get_extend_type() != PL_REF_CURSOR_TYPE) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_WARN("PLS-00382: expression is of wrong type",
                  K(ret), K(pl_data_type.is_obj_type()), KPC(convert_expr),
                  K(convert_expr->get_result_type().get_obj_meta().get_type()),
                  K(pl_data_type.get_user_type_id()),
                  K(convert_expr->get_result_type().get_udt_id()));
    }
  } else if (pl_data_type.get_user_type_id() != convert_expr->get_result_type().get_udt_id()) {
    bool is_compatible = false;
    if (convert_expr->get_result_type().is_null()) {
      is_compatible = true;
    } else if (convert_expr->get_result_type().is_ext()) {
      CK (OB_NOT_NULL(current_block_));
      OZ (check_composite_compatible(current_block_->get_namespace(),
                                     pl_data_type.get_user_type_id(),
                                     convert_expr->get_result_type().get_udt_id(),
                                     is_compatible));
    }
    if (OB_SUCC(ret) && !is_compatible) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_WARN("PLS-00382: expression is of wrong type",
                  K(ret), K(pl_data_type.is_obj_type()), KPC(convert_expr),
                  K(convert_expr->get_result_type().get_obj_meta().get_type()),
                  K(pl_data_type.get_user_type_id()),
                  K(convert_expr->get_result_type().get_udt_id()));
    }
  }
  return ret;
}

int ObPLResolver::resolve_cursor_actual_params(
  const ObStmtNodeTree *parse_tree, ObPLStmt *stmt, ObPLFunctionAST &func)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 32> exprs;
  ObSEArray<ObIRoutineParam*, 16> iparams;
  const ObPLCursor *cursor =
    (PL_OPEN == stmt->get_type() || PL_OPEN_FOR == stmt->get_type())
      ? static_cast<ObPLOpenStmt*>(stmt)->get_cursor()
        : static_cast<ObPLCursorForLoopStmt*>(stmt)->get_cursor();
  CK (OB_NOT_NULL(cursor));
  CK (OB_NOT_NULL(resolve_ctx_.session_info_.get_pl_engine()));
  CK (OB_NOT_NULL(stmt->get_namespace()));
  if (OB_SUCC(ret) && NULL != parse_tree) {
    OZ (resolve_cparams_expr(parse_tree, func, exprs));
  }
  if (OB_SUCC(ret)) {
    const ObIArray<int64_t> &params_list = cursor->get_formal_params();
    for (int64_t i = 0; OB_SUCC(ret) && i < params_list.count(); ++i) {
      const ObPLVar *var = NULL;
      if (cursor->is_package_cursor()) {
        OZ (stmt->get_namespace()->get_package_var(resolve_ctx_,
                                                   cursor->get_package_id(),
                                                   params_list.at(i),
                                                   var));
        CK (OB_NOT_NULL(var));
        OZ (iparams.push_back(const_cast<ObPLVar*>(var)));
      } else if (cursor->get_routine_id() != stmt->get_namespace()->get_routine_id()) {
        // not package cursor, not local cursor, must be subprogram cursor.
        OZ (stmt->get_namespace()->get_subprogram_var(cursor->get_package_id(),
                                                      cursor->get_routine_id(),
                                                      params_list.at(i),
                                                      var));
        CK (OB_NOT_NULL(var));
        OZ (iparams.push_back(const_cast<ObPLVar*>(var)));
      } else {
        OZ (iparams.push_back(const_cast<ObPLVar*>(stmt->get_variable((params_list.at(i))))));
      }
    }
  }
  OZ (resolve_cparams(exprs, iparams, stmt, func));
  return ret;
}

int ObPLResolver::resolve_open_for(
  const ObStmtNodeTree *parse_tree, ObPLOpenForStmt *stmt, ObPLFunctionAST &func)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(parse_tree) || OB_ISNULL(stmt) || OB_ISNULL(current_block_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("parse_tree is NULL", K(parse_tree), K(stmt), K(current_block_), K(ret));
  } else if (lib::is_mysql_mode()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mysql has no open for stmt", K(ret));
  } else if (OB_FAIL(resolve_open(parse_tree, stmt, func))) {
    LOG_WARN("failed to resolve open", K(parse_tree), K(stmt), K(current_block_), K(ret));
  } else { //解析for
    const ObStmtNodeTree *for_node = parse_tree->children_[2];
    if (OB_ISNULL(for_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("for clause is NULL", K(parse_tree->num_child_), K(ret));
    } else {
      if (T_SQL_STMT == for_node->type_) {
        ObPLInto dummy_into(resolve_ctx_.allocator_);
        if (OB_FAIL(resolve_static_sql(for_node, stmt->get_static_sql(), dummy_into, true/*is cursor*/, func))) {
          LOG_WARN("failed to resolve static sql",  K(ret));
        } else { //检查sql语句的结果集类型和Cursor的return类型是否兼容
          const ObPLCursor *cursor = stmt->get_cursor();
          CK (OB_NOT_NULL(cursor));
          if (OB_SUCC(ret) && cursor->get_cursor_type().is_valid_type()) {
            ObArenaAllocator allocator;
            const ObUserDefinedType *cursor_type = NULL;
            if (OB_FAIL(current_block_->get_namespace().get_user_type(cursor->get_cursor_type().get_user_type_id(),
                  cursor_type, &allocator))) {
              LOG_WARN("failed to get user type", K(cursor->get_cursor_type()), K(ret));
            } else if (OB_ISNULL(cursor_type)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("failed to get cursor type", K(cursor->get_cursor_type()), K(ret));
            } else if (!cursor_type->is_record_type()) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("cursor must return record type", K(*cursor_type), K(ret));
            } else if (OB_ISNULL(stmt->get_static_sql().get_row_desc())) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("static sql must has valid row desc", K(stmt->get_static_sql()), K(ret));
            } else {
              bool is_compatible = false;
              const ObRecordType *return_type = static_cast<const ObRecordType*>(cursor_type);
              if (OB_FAIL(stmt->get_static_sql().get_row_desc()->is_compatble(*return_type, is_compatible))) {
                LOG_WARN("failed to check compatible", K(*stmt->get_static_sql().get_row_desc()), K(*return_type), K(ret));
              } else if (!is_compatible) {
                ret = OB_OBJ_TYPE_ERROR;
                LOG_WARN("type not compatible", K(*stmt->get_static_sql().get_row_desc()), K(*return_type), K(ret));
              } else { /*do nothing*/ }
            }
          }
        }
      } else { //expr
        ObRawExpr *dynamic_sql_expr = NULL;
        const ObPLCursor *cursor = stmt->get_cursor();
        const ObPLVar *var = NULL;
        if (OB_FAIL(stmt->get_var(var))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get var fail.", K(ret));
        } else if (NULL == var || NULL == cursor) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("cursor or var is null.", K(ret));
        } else if (cursor->get_cursor_type().is_valid_type()) {
          ret = OB_ERR_CURSOR_IN_OPEN_DYNAMIC_SQL;
          LOG_WARN("dynamicly can not OPEN a REF CURSOR that has RETURN type.", K(ret), K(var->get_name()));
          LOG_USER_ERROR(OB_ERR_CURSOR_IN_OPEN_DYNAMIC_SQL, var->get_name().length(), var->get_name().ptr());
        } else if (OB_FAIL(resolve_expr(for_node, func, dynamic_sql_expr,
                                 combine_line_and_col(for_node->stmt_loc_)))) {
          LOG_WARN("failed to resolve sql expr", K(ret));
        } else if (!dynamic_sql_expr->get_result_type().is_string_or_lob_locator_type() && !dynamic_sql_expr->get_result_type().is_json()
                   && !dynamic_sql_expr->get_result_type().is_geometry()) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("dynamic sql must be string type", K(dynamic_sql_expr->get_result_type()), K(ret));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "non-string type dynamic sql");
        } else {
          stmt->set_dynamic_sql(func.get_expr_count() - 1);
          func.set_modifies_sql_data();
        }
      }
    }
  }

  //解析using
  if (OB_SUCC(ret)) {
    const ObStmtNodeTree *using_node = parse_tree->children_[3];
    if (NULL != using_node) {
      OZ (resolve_using(using_node, stmt->get_using(), func));
      if (OB_SUCC(ret) && stmt->has_out()) {
        ret = OB_ERR_INOUT_PARAM_PLACEMENT_NOT_PROPERLY;
        LOG_WARN("PLS-00254: OUT and IN/OUT modes cannot be used in this context Cause: \
            actual parameter mode (OUT, or IN/OUT) is not used properly in USING clause.\
            For USING clause in an OPEN statement, only IN mode is allowed.", K(ret));
      }
    }
  }
  return ret;
}

int ObPLResolver::resolve_fetch(
  const ObStmtNodeTree *parse_tree, ObPLFetchStmt *stmt, ObPLFunctionAST &func)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(parse_tree) || OB_ISNULL(stmt) || OB_ISNULL(current_block_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("parse_tree is NULL", K(parse_tree), K(stmt), K(current_block_), K(ret));
  } else {
    //解析name
    const ObStmtNodeTree *name_node = parse_tree->children_[0];
    const ObPLCursor* cursor = NULL;
    int64_t index = OB_INVALID_INDEX;
    OZ (resolve_cursor(name_node, current_block_->get_namespace(), index, func));
    CK (OB_NOT_NULL(current_block_->get_namespace().get_cursor_table()));
    CK (OB_NOT_NULL(
      cursor = current_block_->get_namespace().get_cursor_table()->get_cursor(index)));
    OX (stmt->set_index(cursor->get_package_id(), cursor->get_routine_id(), cursor->get_index()));
    if (OB_SUCC(ret)) {
      if (cursor->is_for_update()) {
        func.set_modifies_sql_data();
      } else if (!func.is_modifies_sql_data()) {
        func.set_reads_sql_data();
      }
    }

    //解析into
    if (OB_SUCC(ret)) {
      const ObStmtNodeTree *into_node = parse_tree->children_[1];
      if (OB_ISNULL(into_node)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Fetch statement must have a INTO clause", K(ret));
      } else if (OB_FAIL(resolve_into(into_node, *stmt, func))) {
        LOG_WARN("resolve into node failed", K(parse_tree->children_), K(into_node), K(ret));
      } else { /*do nothing*/ }
      //检查Cursor的return类型和INTO子句类型是否兼容
      if (OB_SUCC(ret) && lib::is_oracle_mode() && cursor->get_cursor_type().is_valid_type()) {
        const ObUserDefinedType *cursor_type = NULL;
        if (OB_FAIL(current_block_->get_namespace().get_user_type(
            cursor->get_cursor_type().get_user_type_id(), cursor_type))) {
          LOG_WARN("failed to get user type", K(cursor->get_cursor_type()), K(ret));
        } else if (OB_ISNULL(cursor_type)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to get cursor type", K(cursor->get_cursor_type()), K(ret));
        } else if (!cursor_type->is_record_type()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("cursor must return record type", K(*cursor_type), K(ret));
        } else {
          bool is_compatible = true;
          const ObRecordType *return_type = static_cast<const ObRecordType*>(cursor_type);
          stmt->set_user_type(cursor_type);
          if (return_type->get_record_member_count() != stmt->get_data_type().count()
              && return_type->get_record_member_count() != stmt->get_into().count()) {
            ret = OB_ERR_WRONG_FETCH_INTO_NUM;
            LOG_WARN("wrong number of values in the INTO list of a FETCH statement", K(ret));
          } else {
            //如果into后面只有一个变量且为record类型，
            bool has_type_record_type = false;
            const ObPLDataType *into_var_type = NULL;
            for (int64_t i = 0; OB_SUCC(ret) && !has_type_record_type && i < stmt->get_into_data_type().count(); ++i) {
              into_var_type = &stmt->get_into_data_type(i);
              //type定义的record
              if (into_var_type->is_type_record()) {
                has_type_record_type = true;
              }
            }
            if (OB_SUCC(ret) && has_type_record_type && stmt->get_into_data_type().count() != 1) {
              //后续新增494的错误码
              ret = OB_ERR_MULTI_RECORD;
              LOG_WARN("coercion into multiple record targets not supported", K(ret));
            }
            /* 走到这里如果没报错，有两种可能:
               1.into变量只有唯一一个type record.
               2.into变量无type record */

            if (OB_SUCC(ret)) {
              const ObUserDefinedType *into_user_type = NULL;
              const ObRecordType *into_record_type = NULL;
              int64_t into_data_type_count = 0;
              if (has_type_record_type) {
                CK (1 == stmt->get_into_data_type().count());
                OZ (current_block_->get_namespace().get_user_type(stmt->get_into_data_type(0).get_user_type_id(), into_user_type));
                CK (OB_NOT_NULL(into_user_type));
                CK (into_user_type->is_record_type());
                OX (into_record_type = static_cast<const ObRecordType*>(into_user_type));
                OX (into_data_type_count = into_record_type->get_record_member_count());
              } else {
                into_data_type_count = stmt->get_into_data_type().count();
              }
              // 依次比较cursor member和所有into变量
              const ObPLDataType *left = NULL;
              const ObPLDataType *right = NULL;
              CK (return_type->get_record_member_count() == into_data_type_count);
              for (int64_t i = 0; OB_SUCC(ret) && is_compatible && i < into_data_type_count; ++i) {
                left = return_type->get_record_member_type(i);
                if (has_type_record_type) {
                  right = into_record_type->get_record_member_type(i);
                } else {
                  right = &stmt->get_into_data_type(i);
                }
                CK (OB_NOT_NULL(left));
                if (OB_FAIL(ret)) {
                } else if (left->is_obj_type() &&
                           right->is_obj_type() &&
                           NULL != right->get_data_type() &&
                           !right->get_data_type()->get_meta_type().is_ext()) {
                  if (right->get_data_type()->get_meta_type().is_null() &&
                      stmt->get_into().count() > i &&
                      is_question_mark_value(func.get_expr(stmt->get_into(i)))) {
                    OZ (set_question_mark_type(func.get_expr(stmt->get_into(i)), left));
                  } else {
                    CK (OB_NOT_NULL(left->get_data_type()));
                    OX (is_compatible = cast_supported(left->get_data_type()->get_obj_type(),
                                                      left->get_data_type()->get_collation_type(),
                                                      right->get_data_type()->get_obj_type(),
                                                      right->get_data_type()->get_collation_type()));
                  }
                } else if ((!left->is_obj_type() ||
                            (left->get_data_type() != NULL && left->get_data_type()->get_meta_type().is_ext()))
                              &&
                            (!right->is_obj_type() ||
                            (right->get_data_type() != NULL && right->get_data_type()->get_meta_type().is_ext()))) {
                  uint64_t left_udt_id = (NULL == left->get_data_type()) ? left->get_user_type_id()
                                                                          : left->get_data_type()->get_udt_id();
                  uint64_t right_udt_id = (NULL == right->get_data_type()) ? right->get_user_type_id()
                                                                            : right->get_data_type()->get_udt_id();
                  if (left_udt_id != right_udt_id) {
                    is_compatible = false;
                  } else {
                    // same composite type, compatible is true, do nothing.
                  }
                } else {
                  is_compatible = false;
                }
              }

              if (OB_SUCC(ret) && !is_compatible) {
                ret = OB_ERR_TYPE_MISMATCH_IN_FETCH;
                LOG_WARN("type not compatible", K(ret), KPC(left), KPC(right));
              }
            }
          }

          if (OB_SUCC(ret) && !is_compatible) {
            ret = OB_ERR_TYPE_MISMATCH_IN_FETCH;
            LOG_WARN("type not compatible",
                     K(*return_type),
                     K(stmt->get_data_type()),
                     K(stmt->get_into()),
                     K(ret));
          }
        }
      }
    }

    //解析limit
    if (OB_SUCC(ret) && 3 == parse_tree->num_child_) {
      const ObStmtNodeTree *limit_node = parse_tree->children_[2];
      if (!stmt->is_bulk()) {
        ret = OB_ERR_LIMIT_ILLEGAL;
        LOG_WARN("PLS-00439:A LIMIT clause must be used within a BULK FETCH", K(stmt->is_bulk()));
      }
      if (OB_SUCC(ret) && OB_ISNULL(limit_node)) {
        stmt->set_limit(INT64_MAX);
      }
      if (OB_SUCC(ret) && OB_NOT_NULL(limit_node)) {
        ObRawExpr *limit_expr = NULL;
        ObPLDataType expected_type(ObIntType);
        OZ (resolve_expr(limit_node, func, limit_expr,
                         combine_line_and_col(limit_node->stmt_loc_), true, &expected_type, true));
        if (OB_ERR_LIMIT_CLAUSE == ret) {
          LOG_USER_ERROR(OB_ERR_LIMIT_CLAUSE, static_cast<int>(limit_node->str_len_),
                         limit_node->str_value_);
        } else if (OB_ERR_EXPRESSION_WRONG_TYPE == ret) {
          LOG_WARN("expression is of wrong type", K(ret), K(limit_node->str_value_));
        } else {
          // do nothing
        }
        CK (OB_NOT_NULL(limit_expr));
        OX (stmt->set_limit(func.get_expr_count() - 1));
      }
    }
  }
  return ret;
}

int ObPLResolver::resolve_close(
  const ObStmtNodeTree *parse_tree, ObPLCloseStmt *stmt, ObPLFunctionAST &func)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(parse_tree) || OB_ISNULL(stmt) || OB_ISNULL(current_block_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("parse_tree is NULL", K(parse_tree), K(stmt), K(current_block_), K(ret));
  } else {
    //解析name
    const ObStmtNodeTree *name_node = parse_tree->children_[0];
    int64_t index = OB_INVALID_INDEX;
    const ObPLCursor *cursor = NULL;
    OZ (resolve_cursor(name_node, current_block_->get_namespace(), index, func));
    CK (OB_NOT_NULL(current_block_->get_namespace().get_cursor_table()));
    CK (OB_NOT_NULL(
      cursor = current_block_->get_namespace().get_cursor_table()->get_cursor(index)));
    OX (stmt->set_index(
      cursor->get_package_id(), cursor->get_routine_id(), cursor->get_index()));
    if (OB_SUCC(ret)) {
      if (cursor->is_for_update()) {
        func.set_modifies_sql_data();
      } else if (!func.is_modifies_sql_data()) {
        func.set_reads_sql_data();
      }
    }
  }
  return ret;
}

int ObPLResolver::resolve_null(const ObStmtNodeTree *parse_tree, ObPLNullStmt *stmt, ObPLFunctionAST &func)
{
  UNUSEDx(parse_tree, stmt, func);
  return OB_SUCCESS;
}


int ObPLResolver::resolve_pipe_row(
  const ObStmtNodeTree *parse_tree, ObPLPipeRowStmt *stmt, ObPLFunctionAST &func)
{
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_ORACLE_PL
  ObRawExpr *row_expr = NULL;
  CK (OB_NOT_NULL(parse_tree));
  CK (T_SP_PIPE_ROW == parse_tree->type_);
  CK (1 == parse_tree->num_child_);
  CK (OB_NOT_NULL(parse_tree->children_[0]));
  if (OB_SUCC(ret) && !func.get_pipelined()) {
    ret = OB_ERR_PIPE_STMT_IN_NON_PIPELINED_FUNC;
    LOG_WARN("PLS-00629: PIPE statement cannot be used in non-pipelined functions", K(ret));
  }
  const ObPLDataType *return_type = &(func.get_ret_type());
  const ObUserDefinedType* user_type = NULL;
  const ObCollectionType* coll_type = NULL;
  CK (OB_NOT_NULL(return_type));
  CK (OB_NOT_NULL(current_block_));
  OZ (current_block_->
    get_namespace().get_pl_data_type_by_id(return_type->get_user_type_id(), user_type));
  CK (OB_NOT_NULL(user_type));
  CK (user_type->is_collection_type());
  CK (OB_NOT_NULL(coll_type = static_cast<const ObCollectionType*>(user_type)));
  CK (return_type->is_collection_type());
  OZ (resolve_expr(
    parse_tree->children_[0], func, row_expr,
    combine_line_and_col(parse_tree->children_[0]->stmt_loc_),
    true, &(coll_type->get_element_type())));
  CK (OB_NOT_NULL(row_expr));
  CK (OB_NOT_NULL(stmt));
  OX (stmt->set_row(func.get_expr_count() - 1));
  OX (stmt->set_type(func.get_ret_type()));
#else
  UNUSEDx(parse_tree, stmt, func);
#endif
  return ret;
}

int ObPLResolver::resolve_ident(const ParseNode *node, ObString &ident)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid Argument", K(node), K(ret));
  } else if (T_IDENT != node->type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid ident", K(node->type_), K(ret));
  } else if (OB_UNLIKELY(node->str_len_ > 
    (lib::is_oracle_mode() ? OB_MAX_PL_IDENT_LENGTH : OB_MAX_MYSQL_PL_IDENT_LENGTH))) {
    ret = OB_ERR_TOO_LONG_IDENT;
    LOG_WARN("identifier is too long", K(node->str_value_), K(ret));
  } else {
    ident.assign_ptr(node->str_value_, static_cast<int32_t>(node->str_len_));
  }
  return ret;
}

bool ObPLResolver::is_need_add_checker(const ObPLIntegerType &type, const ObRawExpr *expr)
{
  bool ret = true;
  // for simple_integer, if checker expr tree already has checker, do not add.
  // others always add.
  if (PL_SIMPLE_INTEGER == type && OB_NOT_NULL(expr)) {
    if (T_FUN_PL_INTEGER_CHECKER == expr->get_expr_type()) {
      ret = false;
    } else {
      for (int i = 0; ret && i < expr->get_param_count(); ++i) {
        ret = is_need_add_checker(type, expr->get_param_expr(i));
      }
    }
  }
  return ret;
}

int ObPLResolver::add_pl_integer_checker_expr(ObRawExprFactory &expr_factory,
                                              const ObPLIntegerType &type,
                                              int32_t lower,
                                              int32_t upper,
                                              ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  CK(OB_NOT_NULL(expr));
  if (OB_SUCC(ret)) {
    ObPLIntegerCheckerRawExpr *checker = NULL;
    if (expr_factory.create_raw_expr(T_FUN_PL_INTEGER_CHECKER, checker)) {
      LOG_WARN("create pl_integer_checker failed", K(ret));
    } else if (OB_ISNULL(checker)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("new raw expr is NULL", K(checker), K(ret));
    } else if (OB_FAIL(checker->add_param_expr(expr))) {
      LOG_WARN("add real param expr failed", K(ret));
    } else {
      checker->set_pl_integer_type(type);
      checker->set_range(lower, upper);
      expr = checker;
    }
  }
  return ret;
}

int ObPLResolver::check_expr_result_type(const ObRawExpr *expr, ObPLIntegerType &pl_integer_type, bool &is_anonymos_arg)
{
  int ret = OB_SUCCESS;
  CK(OB_NOT_NULL(expr));
  if (OB_SUCC(ret)) {
    pl_integer_type = PL_INTEGER_INVALID;
    if (T_FUN_PL_INTEGER_CHECKER == expr->get_expr_type()) {
      const ObPLIntegerCheckerRawExpr *checker = static_cast<const ObPLIntegerCheckerRawExpr*>(expr);
      CK (OB_NOT_NULL(checker));
      OX (pl_integer_type = checker->get_pl_integer_type());
    } else if (T_OBJ_ACCESS_REF == expr->get_expr_type()) {
      ObPLDataType pl_data_type;
      const ObObjAccessRawExpr *obj_expr = static_cast<const ObObjAccessRawExpr*>(expr);
      if (OB_FAIL(obj_expr->get_final_type(pl_data_type))) {
        LOG_WARN("failed to get obj access raw expr final type", K(ret));
      } else if (pl_data_type.is_pl_integer_type()) {
        pl_integer_type = pl_data_type.get_pl_integer_type();
      }
    } else if (expr->is_const_raw_expr()) {
      const ObConstRawExpr *const_expr = static_cast<const ObConstRawExpr*>(expr);
      const ObPLSymbolTable* symbol_table = NULL;
      const ObPLVar* var = NULL;
      if (T_QUESTIONMARK != const_expr->get_expr_type()) {
        /* Integer Literals in SIMPLE_INTEGER Range
         * Integer literals in the SIMPLE_INTEGER range have the data type SIMPLE_INTEGER.
         * However, to ensure backward compatibility, when all operands in an arithmetic
         * expression are integer literals, PL/SQL treats the integer literals as if they were cast to
         * PLS_INTEGER. */
        if (T_INT == const_expr->get_expr_type()) {
          const ObObj &obj = const_expr->get_value();
          int64_t v = 0;
          if (obj.is_integer_type()) {
            v = obj.get_int();
          } else if (obj.is_number()) {
            OZ (obj.get_number().extract_valid_int64_with_trunc(v));
          }
          if (OB_SUCC(ret) && v >= -2147483648 && v <= 2147483647) {
            pl_integer_type = PL_SIMPLE_INTEGER;
          }
        }
      } else if (!const_expr->get_value().is_unknown()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected const expr", K(const_expr->get_value()), K(current_block_), K(ret));
      } else if (OB_ISNULL(current_block_) || OB_ISNULL(symbol_table = current_block_->get_symbol_table())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(current_block_), K(ret));
      } else if (OB_ISNULL(var = symbol_table->get_symbol(const_expr->get_value().get_unknown()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get symble var is null", K(var), K(const_expr->get_value().get_unknown()), K(ret));
      } else {
        if (0 == var->get_name().case_compare(ObPLResolver::ANONYMOUS_ARG)) {
          is_anonymos_arg = true;
        }
        pl_integer_type = var->get_type().get_pl_integer_type();
      }
    } else if (expr->is_udf_expr()) {
      const ObUDFRawExpr *udf = static_cast<const ObUDFRawExpr *>(expr);
      CK (OB_NOT_NULL(udf));
      OX (pl_integer_type = udf->get_pls_type());
    }
  }
  return ret;
}

int ObPLResolver::add_pl_integer_checker_expr(ObRawExprFactory &expr_factory,
                                              ObRawExpr *&expr,
                                              bool &need_replace)
{
  int ret = OB_SUCCESS;
  ObRawExpr *child = NULL;
  need_replace = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
    child = expr->get_param_expr(i);
    if (OB_ISNULL(child)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("child expr is null", K(ret));
    } else if (OB_FAIL(add_pl_integer_checker_expr(expr_factory, child, need_replace))) {
      LOG_WARN("failed to add pl integer checker expr", K(ret));
    } else if (need_replace) {
      expr->get_param_expr(i) = child;
    }
  }
  #define LOGIC_EXPR(expr) \
  ( T_OP_IN == expr->get_expr_type() \
    || T_OP_EXISTS == expr->get_expr_type() \
    || T_OP_NOT_IN == expr->get_expr_type() \
    || T_OP_NOT_EXISTS == expr->get_expr_type() \
    || T_OP_ROW == expr->get_expr_type() \
   )

  #define CHECK_RES_TYPE(expr) \
  ( \
     expr->get_result_type().is_numeric_type()\
  )
  if (OB_SUCC(ret)) {
    // 对于溢出的检查只关心双目运算符的计算
    if (2 == expr->get_param_count() && !IS_COMMON_COMPARISON_OP(expr->get_expr_type())
             && !LOGIC_EXPR(expr) && expr->get_expr_type() != T_FUN_SYS_POWER && CHECK_RES_TYPE(expr)) {
      const ObRawExpr *left = ObRawExprUtils::skip_implicit_cast(expr->get_param_expr(0));
      const ObRawExpr *right = ObRawExprUtils::skip_implicit_cast(expr->get_param_expr(1));
      ObPLIntegerType left_pl_integer = PL_INTEGER_INVALID, right_pl_integer = PL_INTEGER_INVALID;
      bool is_anonymos_arg_left = false;
      bool is_anonymos_arg_right = false;
      if (OB_FAIL(check_expr_result_type(left, left_pl_integer, is_anonymos_arg_left))) {
        LOG_WARN("failed to check expr result type", K(ret));
      } else if (OB_FAIL(check_expr_result_type(right, right_pl_integer, is_anonymos_arg_right))) {
        LOG_WARN("failed to check expr result type", K(ret));
      } else if (PL_INTEGER_INVALID != left_pl_integer && PL_INTEGER_INVALID != right_pl_integer) {
        ObPLIntegerType type = PL_SIMPLE_INTEGER == left_pl_integer && PL_SIMPLE_INTEGER == right_pl_integer ?
                          PL_SIMPLE_INTEGER : PL_PLS_INTEGER;
        /* Integer Literals in SIMPLE_INTEGER Range
           Integer literals in the SIMPLE_INTEGER range have the data type SIMPLE_INTEGER.
           However, to ensure backward compatibility, when all operands in an arithmetic
           expression are integer literals, PL/SQL treats the integer literals as if they were cast to
           PLS_INTEGER. */
        if (PL_SIMPLE_INTEGER == type &&
            ((T_INT == left->get_expr_type() && T_INT == right->get_expr_type()) ||
             (is_anonymos_arg_left && is_anonymos_arg_right))) {
          type = PL_PLS_INTEGER;
        }
        OZ (add_pl_integer_checker_expr(expr_factory, type, -2147483648, 2147483647, expr));
        OX (need_replace = true);
      }
    }
  }
  #undef LOGIC_EXPR
  #undef CHECK_RES_TYPE
  return ret;
}

int ObPLResolver::resolve_condition_compile(
  ObIAllocator &allocator,
  ObSQLSessionInfo *session_info,
  ObSchemaGetterGuard *schema_guard,
  ObPLPackageGuard *package_guard,
  ObMySQLProxy *sql_proxy,
  const ObString *exec_env,
  const ParseNode *node,
  const ParseNode *&new_node,
  bool is_inner_parse,
  bool is_for_trigger,
  bool is_for_dynamic,
  bool *is_include_old_new_in_trigger,
  ObPLDependencyTable *dep_table)
{
  int ret = OB_SUCCESS;
  ObRawExprFactory expr_factory(allocator);
  CK (OB_NOT_NULL(node));
  CK (T_SP_PRE_STMTS == node->type_);
  CK (OB_NOT_NULL(schema_guard));
  if (OB_SUCC(ret) && OB_ISNULL(sql_proxy)) {
    CK (OB_NOT_NULL(sql_proxy = GCTX.sql_proxy_));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(session_info)) {
    ObPLPackageGuard local_pkg_guard(schema_guard->get_tenant_id());
    if (OB_SUCC(ret) && OB_ISNULL(package_guard)) {
      OZ (local_pkg_guard.init());
      OX (package_guard = &local_pkg_guard);
    }
    SMART_VAR(sql::ObSQLSessionInfo, session) {
      ObExecEnv env;
      CK (OB_NOT_NULL(exec_env));
      OZ (env.init(*exec_env));
      OZ (session.init(0, 0, &allocator));
      OZ (session.load_default_sys_variable(false, false));
      OZ (env.store(session));
      if (OB_SUCC(ret)) {
        ObPLResolver resolver(
          allocator, session,
          *schema_guard, *package_guard, *sql_proxy, expr_factory, NULL, false);
        int64_t question_mark_count = 0;
        OZ (resolver.resolve_condition_compile(
          node,
          new_node,
          question_mark_count,
          is_inner_parse,
          is_for_trigger,
          is_for_dynamic,
          is_include_old_new_in_trigger,
          dep_table));
      }
    }
  } else {
    ObPLPackageGuard local_pkg_guard(session_info->get_effective_tenant_id());
    if (OB_SUCC(ret) && OB_ISNULL(package_guard)) {
      OZ (local_pkg_guard.init());
      OX (package_guard = &local_pkg_guard);
    }
    ObPLResolver resolver(
      allocator, *session_info,
      *schema_guard, *package_guard, *sql_proxy, expr_factory, NULL, false);
    int64_t question_mark_count = 0;
    OZ (resolver.resolve_condition_compile(
      node,
      new_node,
      question_mark_count,
      is_inner_parse,
      is_for_trigger,
      is_for_dynamic,
      is_include_old_new_in_trigger,
      dep_table));
  }
  return ret;
}

int ObPLResolver::resolve_condition_compile(
  const ParseNode *node,
  const ParseNode *&new_node,
  int64_t &question_mark_count,
  bool is_inner_parse,
  bool is_for_trigger,
  bool is_for_dynamic,
  bool *is_include_old_new_in_trigger,
  ObPLDependencyTable *dep_table)
{
  int ret = OB_SUCCESS;
  ObString old_sql;
  ObString new_sql;
  ObPLParser pl_parser(
    resolve_ctx_.allocator_, resolve_ctx_.session_info_.get_charsets4parser(),
    resolve_ctx_.session_info_.get_sql_mode());
  ParseResult parse_result;

  HEAP_VAR(ObPLFunctionAST, unit_ast, resolve_ctx_.allocator_) {
    CK (OB_NOT_NULL(node));
    OZ (init(unit_ast));
    OZ (resolve_condition_compile(unit_ast, *node, old_sql, new_sql));
    CK (OB_LIKELY(old_sql.length() == new_sql.length()));

    OX (parse_result.is_for_trigger_ = is_for_trigger ? 1 : 0);
    OX (parse_result.is_dynamic_sql_ = is_for_dynamic ? 1 : 0);
    parse_result.mysql_compatible_comment_ = 0;
    OZ (pl_parser.parse(new_sql, old_sql, parse_result, is_inner_parse));
    if (OB_SUCC(ret) && OB_NOT_NULL(is_include_old_new_in_trigger)) {
      *is_include_old_new_in_trigger = parse_result.is_include_old_new_in_trigger_;
    }

    CK (1 == parse_result.result_tree_->num_child_);
    CK (OB_NOT_NULL(new_node = parse_result.result_tree_->children_[0]));
    OX (question_mark_count = parse_result.question_mark_ctx_.count_);
    if (OB_SUCC(ret) && T_SP_CREATE_TYPE == new_node->type_) {
      ret = OB_ERR_DIRECTIVE_CONTEXT;
      LOG_WARN("preprocessor directives are not supported in this context",
              K(ret), K(new_node->type_));
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(dep_table)) {
      ObPLDependencyTable &src_table = unit_ast.get_dependency_table();
      for (int i = 0; OB_SUCC(ret) && i < src_table.count(); ++i) {
        OZ (add_var_to_array_no_dup(*dep_table, src_table.at(i)));
      }
    }
  }
  return ret;
}

int ObPLResolver::replace_source_string(
  const ObString &old_sql, ParseNode *new_node)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(new_node)) {
    if (new_node->str_len_ > 0 && new_node->pl_str_off_ >= 0) {
      new_node->str_value_ = parse_strndup(old_sql.ptr() + new_node->pl_str_off_,
                                           new_node->str_len_,
                                           &resolve_ctx_.allocator_);
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < new_node->num_child_; ++i) {
      OZ (replace_source_string(old_sql, new_node->children_[i]));
    }
  }
  return ret;
}

int ObPLResolver::resolve_condition_compile(
  ObPLFunctionAST &unit_ast, const ParseNode &node, ObString &old_sql, ObString &new_sql)
{
  int ret = OB_SUCCESS;
  int64_t start = -1;
  int64_t end = -1;
  char* ptr = NULL;
  const ParseNode *child = NULL;

  CK (OB_NOT_NULL(child = &node));
  CK (OB_LIKELY(T_SP_PRE_STMTS == child->type_));
  OX (old_sql.assign_ptr(child->str_value_, child->str_len_));
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(
        ptr = static_cast<char*>(resolve_ctx_.allocator_.alloc(old_sql.length() + 1)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory for new sql string", K(ret));
  } else {
    MEMSET(ptr, ' ', old_sql.length());
    ptr[old_sql.length()] = '\0';
  }
  OX (new_sql.assign_buffer(ptr, old_sql.length() + 1));
  OX (new_sql.set_length(old_sql.length()));

  LOG_INFO("resolve condition compile : old_sql is ", K(old_sql));
  OZ (resolve_preprocess_stmt(unit_ast, *child, old_sql, new_sql, start, end));
  if (OB_FAIL(ret)) {
  } else if ((end + 1) < old_sql.length()) {
    OZ (append_sql(old_sql, new_sql, end + 1, old_sql.length()));
  }

  LOG_INFO("resolve condition compile : after condition compile",
           K(ret), K(old_sql), K(new_sql), K(start), K(end));
  return ret;
}

int ObPLResolver::append_sql(
  ObString &old_sql, ObString &new_sql, int64_t start, int64_t end, bool fill_blank)
{
  int ret = OB_SUCCESS;
  if (end < start) {
    // do nothing ...
  } else {
    for (int64_t i = start; OB_SUCC(ret) && i <= end; ++i) {
      if (!fill_blank) {
        new_sql.ptr()[i] = old_sql[i];
      } else if (old_sql[i] == '\t' || old_sql[i] == '\n') {
        new_sql.ptr()[i] = old_sql[i];
      } else {
        new_sql.ptr()[i] = ' ';
      }
    }
  }
  LOG_INFO("append_sql done!",
           K(old_sql), K(new_sql), K(start), K(end), K(fill_blank),
           K(old_sql.length()));
  return ret;
}

int ObPLResolver::resolve_preprocess_stmt(
  ObPLFunctionAST &unit_ast,
  const ParseNode &node, ObString &old_sql, ObString &new_sql, int64_t &start, int64_t &end)
{
  int ret = OB_SUCCESS;
  CK (OB_LIKELY(T_SP_PRE_STMTS == node.type_));
  for (int64_t i = 0; OB_SUCC(ret) && i < node.num_child_; ++i) {
    const ParseNode* child = node.children_[i];
    if (OB_ISNULL(child)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("child node is null!", K(ret), K(child), K(i));
    } else if (T_IDENT == child->type_) {
      if (-1 == start) {
        start = child->int32_values_[0];
        end = child->int32_values_[1];
        OZ (append_sql(old_sql, new_sql, child->int32_values_[0], child->int32_values_[1]));
      } else {
        CK ((end + 1) <= child->int32_values_[0]);
        OZ (append_sql(old_sql, new_sql, end + 1, child->int32_values_[0], true));
        OZ (append_sql(old_sql, new_sql, child->int32_values_[0], child->int32_values_[1]));
        OX (start = child->int32_values_[0]);
        OX (end = child->int32_values_[1]);
      }
    } else if (T_SP_ERROR == child->type_) {
      OZ (SMART_CALL(resolve_error_stmt(unit_ast, *child)));
    } else if (T_SP_PRE_IF == child->type_) {
      OZ (SMART_CALL(resolve_pre_if_stmt(unit_ast, *child, old_sql, new_sql, start, end)));
    } else if (T_SP_PRE_STMTS == child->type_) {
      OZ (SMART_CALL(resolve_preprocess_stmt(unit_ast, *child, old_sql, new_sql, start, end)));
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected node type!", K(ret), K(child->type_));
    }
  }
  return ret;
}

int ObPLResolver::resolve_error_stmt(ObPLFunctionAST &unit_ast, const ParseNode &node)
{
  int ret = OB_SUCCESS;
  ObObj result;
  ObString result_msg;
  CK (OB_LIKELY(T_SP_ERROR == node.type_));
  CK (OB_LIKELY(1 == node.num_child_));
  CK (OB_NOT_NULL(node.children_[0]));

  OZ (resolve_and_calc_static_expr(unit_ast, *(node.children_[0]), ObVarcharType, result));
  OZ (result.get_string(result_msg));
  if (OB_SUCC(ret)) {
    ret = OB_ERR_DIRECTIVE_ERROR;
    LOG_WARN("$ERROR directive was used. This is an expected error message.", K(ret), K(result));
    LOG_USER_ERROR(OB_ERR_DIRECTIVE_ERROR, result_msg.length(), result_msg.ptr());
  }
  return ret;
}

int ObPLResolver::resolve_pre_if_stmt(ObPLFunctionAST &unit_ast,
                                      const ParseNode &node,
                                      ObString &old_sql,
                                      ObString &new_sql,
                                      int64_t &start,
                                      int64_t &end)
{
  int ret = OB_SUCCESS;

  const ParseNode *bool_expr_node = NULL;
  const ParseNode *then_expr_node = NULL;
  const ParseNode *else_expr_node = NULL;
  ObObj result;

  CK (T_SP_PRE_IF == node.type_);
  CK (3 == node.num_child_);
  CK (OB_NOT_NULL(bool_expr_node = node.children_[0]));
  CK (OB_NOT_NULL(then_expr_node = node.children_[1]));
  OX (else_expr_node = node.children_[2]);

  // Step1. resolve bool expr and calc result.
  OZ (resolve_and_calc_static_expr(unit_ast, *bool_expr_node, ObTinyIntType, result));

  // Step2. concat sql string acording bool result.
  if (OB_FAIL(ret)) {
  } else if (result.is_true()) {
    OZ (resolve_preprocess_stmt(unit_ast, *then_expr_node, old_sql, new_sql, start, end));
  } else if (OB_NOT_NULL(else_expr_node)) {
    OZ (resolve_pre_else_stmt(unit_ast, *else_expr_node, old_sql, new_sql, start, end));
  }
  return ret;
}

int ObPLResolver::resolve_pre_else_stmt(ObPLFunctionAST &unit_ast,
                                        const ParseNode &node,
                                        ObString &old_sql,
                                        ObString &new_sql,
                                        int64_t &start,
                                        int64_t &end)
{
  int ret = OB_SUCCESS;
  CK (OB_LIKELY(1 == node.num_child_));
  CK (OB_NOT_NULL(node.children_));
  CK (OB_NOT_NULL(node.children_[0]));
  CK (OB_LIKELY(T_SP_PRE_IF == node.children_[0]->type_
                || T_SP_PRE_STMTS == node.children_[0]->type_));
  if (OB_FAIL(ret)) {
  } else if (T_SP_PRE_IF == node.children_[0]->type_) {
    OZ (resolve_pre_if_stmt(unit_ast, *(node.children_[0]), old_sql, new_sql, start, end));
  } else {
    CK (T_SP_PRE_STMTS == node.children_[0]->type_);
    OZ (resolve_preprocess_stmt(unit_ast, *(node.children_[0]), old_sql, new_sql, start, end));
  }
  return ret;
}

// ----------------- check boolean static expr -------------

int ObPLResolver::is_bool_literal_expr(const ObRawExpr *expr, bool &is_bool_literal_expr)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(expr));
  OX (is_bool_literal_expr
        = (T_NULL == expr->get_expr_type()
            || T_BOOL == expr->get_expr_type()
            || (T_FUN_PLSQL_VARIABLE == expr->get_expr_type()
                && ObNullType == expr->get_result_type().get_type())));
  return ret;
}

int ObPLResolver::is_static_expr(const ObRawExpr *expr, bool &is_static_expr)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(expr));
  if (OB_FAIL(ret)) {
  } else if (T_OP_GET_PACKAGE_VAR == expr->get_expr_type()) {
    const ObSysFunRawExpr *f_expr = static_cast<const ObSysFunRawExpr *>(expr);
    uint64_t package_id = OB_INVALID_ID;
    uint64_t var_idx = OB_INVALID_ID;
    CK (OB_NOT_NULL(f_expr) && f_expr->get_param_count() >= 2);
    OZ (get_const_expr_value(f_expr->get_param_expr(0), package_id));
    OZ (get_const_expr_value(f_expr->get_param_expr(1), var_idx));
    OZ (check_package_variable_read_only(package_id, var_idx));
    if (OB_ERR_VARIABLE_IS_READONLY == ret) {
      ret = OB_SUCCESS;
      is_static_expr = true;
    }
  } else if (T_FUN_PLSQL_VARIABLE == expr->get_expr_type()) {
    is_static_expr = true;
  }
  return ret;
}

int ObPLResolver::is_static_bool_expr(const ObRawExpr *expr, bool &is_static_bool_expr)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(expr));
  OX (is_static_bool_expr = false);
  if (OB_SUCC(ret)
      && ObTinyIntType == expr->get_result_type().get_type()) {
    OZ (is_static_expr(expr, is_static_bool_expr));
  }
  return ret;
}

int ObPLResolver::is_pls_literal_expr(const ObRawExpr *expr, bool &is_pls_literal_expr)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(expr));
  OX (is_pls_literal_expr = (T_NULL == expr->get_expr_type()));
  if (OB_SUCC(ret)
      && !is_pls_literal_expr
      && T_INT == expr->get_expr_type()) {
    const ObConstRawExpr *const_expr = static_cast<const ObConstRawExpr *>(expr);
    const ObObj &obj = const_expr->get_value();
    int64_t v = 0;
    if (obj.is_integer_type()) {
      v = obj.get_int();
    } else if (obj.is_number()) {
      OZ (obj.get_number().extract_valid_int64_with_trunc(v));
    }
    if (OB_SUCC(ret) && v >= -2147483648 && v <= 2147483647) {
      is_pls_literal_expr = true;
    }
  }
  return ret;
}

int ObPLResolver::is_static_pls_expr(const ObRawExpr *expr, bool &is_static_pls_expr)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(expr));
  OX (is_static_pls_expr = false);
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(is_pls_literal_expr(expr, is_static_pls_expr))) {
    LOG_WARN("failed to call is_pls_literal_expr", K(ret), KPC(expr));
  } else if (!is_static_pls_expr) {
    if (OB_FAIL(is_static_expr(expr, is_static_pls_expr))) {
      LOG_WARN("failed to call is_static_expr", K(ret), KPC(expr));
    } else if (is_static_pls_expr && ObInt32Type != expr->get_result_type().get_type()) {
      is_static_pls_expr = false;
    }
  }
  return ret;
}

int ObPLResolver::is_static_pls_or_bool_expr(
  const ObRawExpr *expr, bool &static_pls_or_bool_expr)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(expr));
  if (OB_FAIL(ret)) {
  } else {
    bool is_static_pls_int_expr = false;
    bool is_static_bool_expr = false;
    OZ (is_static_pls_expr(expr, is_static_pls_int_expr));
    OZ (!is_static_pls_int_expr ? check_static_bool_expr(expr, is_static_bool_expr) : OB_SUCCESS);
    OX (static_pls_or_bool_expr = (is_static_pls_int_expr || is_static_bool_expr));
  }
  return ret;
}

int ObPLResolver::is_static_relation_expr(const ObRawExpr *expr, bool &is_static_relation_expr)
{
  int ret = OB_SUCCESS;
  OX (is_static_relation_expr = false);
  CK (OB_NOT_NULL(expr));
  if (OB_FAIL(ret)) {
  } else if (T_OP_EQ == expr->get_expr_type() // =
             || T_OP_LE == expr->get_expr_type() // <
             || T_OP_LT == expr->get_expr_type() // >
             || T_OP_GE == expr->get_expr_type() // <=
             || T_OP_GT == expr->get_expr_type() // >=
             || T_OP_NE == expr->get_expr_type()) { // <>
    bool static_pls_or_bool_expr = false;
    const ObRawExpr *left = NULL;
    const ObRawExpr *right = NULL;
    CK (2 == expr->get_param_count());
    CK (OB_NOT_NULL(left = ObRawExprUtils::skip_implicit_cast(expr->get_param_expr(0))));
    CK (OB_NOT_NULL(right = ObRawExprUtils::skip_implicit_cast(expr->get_param_expr(1))));
    OZ (is_static_pls_or_bool_expr(left, static_pls_or_bool_expr));
    OZ (static_pls_or_bool_expr
      ? is_static_pls_or_bool_expr(right, static_pls_or_bool_expr) : OB_SUCCESS);
    OX (is_static_relation_expr = static_pls_or_bool_expr);
  } else if (T_OP_NOT == expr->get_expr_type()) {
    const ObRawExpr *child = NULL;
    CK (1 == expr->get_param_count());
    CK (OB_NOT_NULL(child = ObRawExprUtils::skip_implicit_cast(expr->get_param_expr(0))));
    OZ (check_static_bool_expr(child, is_static_relation_expr));
  } else if (T_OP_AND == expr->get_expr_type()
          || T_OP_OR == expr->get_expr_type()) {
    const ObRawExpr *left = NULL;
    const ObRawExpr *right = NULL;
    CK (2 == expr->get_param_count());
    CK (OB_NOT_NULL(left = ObRawExprUtils::skip_implicit_cast(expr->get_param_expr(0))));
    CK (OB_NOT_NULL(right = ObRawExprUtils::skip_implicit_cast(expr->get_param_expr(1))));
    OZ (check_static_bool_expr(left, is_static_relation_expr));
    OZ (is_static_relation_expr
      ? check_static_bool_expr(right, is_static_relation_expr) : OB_SUCCESS);
  } else if (T_OP_IS == expr->get_expr_type()
            || T_OP_IS_NOT == expr->get_expr_type()) {
    const ObRawExpr *child = NULL;
    const ObConstRawExpr *con_expr = NULL;
    CK (2 == expr->get_param_count());
    CK (OB_NOT_NULL(child = ObRawExprUtils::skip_implicit_cast(expr->get_param_expr(0))));
    CK (OB_NOT_NULL(con_expr = static_cast<const ObConstRawExpr *>
                    (ObRawExprUtils::skip_implicit_cast(expr->get_param_expr(1)))));
    if (OB_SUCC(ret) && ObNullType == con_expr->get_value().get_type()) {
      OZ (is_static_pls_or_bool_expr(child, is_static_relation_expr));
      OZ (!is_static_relation_expr
        ? check_static_bool_expr(child, is_static_relation_expr) : OB_SUCCESS);
    }
  }
  return ret;
}

/*!
 * BOOLEAN Static Expressions BOOLEAN static expressions are:
 *   ■ BOOLEAN literals (TRUE, FALSE, or NULL)
 *   ■ BOOLEAN static constants
 *   ■ Where x and y are PLS_INTEGER static expressions:
 *     – x > y
 *     – x < y
 *     – x >= y
 *     – x <= y
 *     – x = y
 *     – x <> y
 *   ■ Where x and y are BOOLEAN static expressions:
 *     – NOT y
 *     – x AND y
 *     – x OR y
 *     – x > y
 *     - x < y
 *     – x >= y
 *     – x = y
 *     – x <= y
 *     – x <> y
 *   ■ Where x is a static expression:
 *     – x IS NULL
 *     – x IS NOT NULL
 */
int ObPLResolver::check_static_bool_expr(const ObRawExpr *expr, bool &static_bool_expr)
{
  int ret = OB_SUCCESS;
  OX (static_bool_expr = false);
  CK (OB_NOT_NULL(expr));
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(is_bool_literal_expr(expr, static_bool_expr))) {
    LOG_WARN("failed to call is_bool_literal_expr", K(ret), KPC(expr));
  } else if (!static_bool_expr
    && OB_FAIL(is_static_bool_expr(expr, static_bool_expr))) {
      LOG_WARN("failed to call is_static_bool_expr", K(ret), KPC(expr));
  } else if (!static_bool_expr
    && OB_FAIL(is_static_relation_expr(expr, static_bool_expr))) {
      LOG_WARN("failed to call is_static_relation_expr", K(ret), KPC(expr));
  }
  return ret;
}

// ----------------- check boolean static expr end -------------

int ObPLResolver::resolve_and_calc_static_expr(
  ObPLFunctionAST &unit_ast, const ParseNode &node, ObObjType expect_type, ObObj &result_obj)
{
  int ret = OB_SUCCESS;
  ObRawExpr *expr = NULL;
  OZ (resolve_expr(&node, unit_ast, expr));
  CK (OB_NOT_NULL(expr));
  if (OB_FAIL(ret)) {
  } else if (ObTinyIntType == expect_type) {
    bool is_static_bool_expr = false;
    if (OB_FAIL(check_static_bool_expr(expr, is_static_bool_expr))) {
      LOG_WARN("failed to check static bool expr", K(ret), KPC(expr));
    } else if (!is_static_bool_expr) {
      ret = OB_ERR_STATIC_BOOL_EXPR;
      LOG_WARN("not a bool expr for condition compile", K(ret), KPC(expr));
    }
  } else if (ObVarcharType == expect_type) {
    if (!ob_is_string_tc(expr->get_result_type().get_type())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("not a varchar expr for condition compile", K(ret), KPC(expr));
    }
  }
  OZ (ObSPIService::spi_calc_raw_expr(
    &(resolve_ctx_.session_info_), &(resolve_ctx_.allocator_), expr, &result_obj));
  return ret;
}

int ObPLResolver::build_raw_expr(const ParseNode &node,
                                 ObRawExpr *&expr,
                                 ObIArray<ObQualifiedName> &columns,
                                 ObIArray<ObVarInfo> &sys_vars,
                                 ObIArray<ObAggFunRawExpr*> &aggr_exprs,
                                 ObIArray<ObWinFunRawExpr*> &win_exprs,
                                 ObIArray<ObSubQueryInfo> &sub_query_info,
                                 ObIArray<ObUDFInfo> &udf_info,
                                 ObIArray<ObOpRawExpr*> &op_exprs,
                                 bool is_prepare_protocol/*= false*/)
{
  int ret = OB_SUCCESS;
  CK(current_block_);
  CK(OB_NOT_NULL(current_block_->get_namespace().get_external_ns()));
  if (OB_SUCC(ret)) {
    const pl::ObPLResolveCtx &ctx = current_block_->get_namespace().get_external_ns()->get_resolve_ctx();
    ObSchemaChecker schema_checker;
    TgTimingEvent tg = static_cast<TgTimingEvent>(resolve_ctx_.params_.tg_timing_event_);
    if (OB_FAIL(schema_checker.init(ctx.schema_guard_,
                                    ctx.session_info_.get_sessid()))) {
      LOG_WARN("failed to init schema checker", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_raw_expr(expr_factory_,
                               ctx.session_info_,
                               &schema_checker,
                               &current_block_->get_namespace(),
                               T_PL_SCOPE,
                               NULL/*ObStmt*/,
                               resolve_ctx_.params_.param_list_,
                               NULL/*external_param_info*/,
                               node,
                               expr,
                               columns,
                               sys_vars,
                               aggr_exprs,
                               win_exprs,
                               sub_query_info,
                               udf_info,
                               op_exprs,
                               is_prepare_protocol,
                               tg))) {
      LOG_WARN("failed to build raw expr", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObPLResolver::build_raw_expr(const ParseNode *node,
                                 ObPLCompileUnitAST &unit_ast,
                                 ObRawExpr *&expr,
                                 const ObPLDataType *expected_type)
{
  int ret = OB_SUCCESS;
  ObArray<ObQualifiedName> columns;
  ObArray<ObVarInfo> sys_vars;
  ObArray<ObAggFunRawExpr*> aggr_exprs;
  ObArray<ObWinFunRawExpr*> win_exprs;
  ObArray<ObSubQueryInfo> sub_query_info;
  ObArray<ObUDFInfo> udf_info;
  ObArray<ObOpRawExpr*> op_exprs;
  CK (OB_NOT_NULL(node));
  OZ (build_raw_expr(*node,
                     expr,
                     columns,
                     sys_vars,
                     aggr_exprs,
                     win_exprs,
                     sub_query_info,
                     udf_info,
                     op_exprs,
                     resolve_ctx_.is_prepare_protocol_));
  if (OB_FAIL(ret)) {
    LOG_WARN("build raw expr failed", K(ret));
  } else if (aggr_exprs.count() > 0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("agg expr in pl assign stmt not allowed", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "agg expr in pl assign stmt");
  } else if (sub_query_info.count() > 0) {
    if (lib::is_mysql_mode()) {
      OZ (transform_subquery_expr(node, expr, expected_type, unit_ast));
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "use subquery in pl/sql expression");
    }
  } else {
    OZ (resolve_columns(expr, columns, unit_ast));
  }
  OV (udf_info.count() <= 0, OB_ERR_UNEXPECTED, K(udf_info));
  if (OB_SUCC(ret) && op_exprs.count() > 0) {
    if (OB_FAIL(ObRawExprUtils::resolve_op_exprs_for_oracle_implicit_cast(expr_factory_,
                                        &resolve_ctx_.session_info_, op_exprs))) {
      LOG_WARN("implicit cast faild", K(ret));
    }
  }

  // record 只能被定义在两个地方，local和package
  if (OB_SUCC(ret) && !OB_ISNULL(expr) &&
      (T_OP_IS == expr->get_expr_type() || T_OP_IS_NOT == expr->get_expr_type())
      && columns.count() > 0) {
    uint64_t parent_id = OB_INVALID_INDEX;
    int64_t var_index = OB_INVALID_INDEX;
    ObPLExternalNS::ExternalType type = ObPLExternalNS::INVALID_VAR;
    ObPLDataType pl_data_type;
    int64_t ident_cnt = columns.at(0).access_idents_.count();
    ObPLBlockNS &ns = current_block_->get_namespace();
    int64_t tmp_ret = OB_SUCCESS;
    if (1 < ident_cnt) {
        // try record type inside package
        type = ObPLExternalNS::PKG_NS;
        const ObString &pkg_name = columns.at(0).access_idents_.at(0).access_name_;
        if (OB_SUCCESS != (tmp_ret =resolve_ctx_.session_info_.get_database_id(parent_id))) {
          LOG_WARN("get database id failed.", K(tmp_ret));
        } else if (OB_SUCCESS != (tmp_ret = ns.resolve_symbol(pkg_name,
                                                                  type,
                                                                  pl_data_type,
                                                                  parent_id,
                                                                  var_index))) {
          LOG_WARN("failed to get var index", K(pl_data_type), K(tmp_ret));
        } else {
          parent_id = var_index;
          type = ObPLExternalNS::PKG_VAR;
          var_index = OB_INVALID_INDEX;
        }
    } else {
      // do nothing
    }
    if (OB_SUCCESS == tmp_ret && !columns.at(0).col_name_.empty()) {
    // we don't want the ret code to be cover intendedly
      if (OB_SUCCESS != (tmp_ret = ns.resolve_symbol(columns.at(0).col_name_,
                                          type,
                                          pl_data_type,
                                          parent_id,
                                          var_index))) {
        LOG_WARN("failed to get var index", K(pl_data_type), K(ret));
      } else {
        if (pl_data_type.is_record_type() && !pl_data_type.is_object_type()) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "record type nullable test");
          LOG_WARN("record type is not allowed for nullable test.",
                   K(ret),
                   K(columns.at(0).col_name_),
                   K(parent_id),
                   K(var_index),
                   K(pl_data_type));
        }
      }
    }
  }
  OZ (ObRawExprUtils::set_call_in_pl(expr));
  return ret;
}

bool ObPLResolver::is_json_type_compatible(const ObUserDefinedType *left, const ObUserDefinedType *right)
{
#ifdef OB_BUILD_ORACLE_PL
  return (ObPlJsonUtil::is_pl_json_element_type(left->get_user_type_id())
          && ObPlJsonUtil::is_pl_json_object_type(right->get_user_type_id())) ;
#else
  return false;
#endif
}

int ObPLResolver::check_composite_compatible(const ObPLINS &ns,
                                             uint64_t left_type_id,
                                             uint64_t right_type_id,
                                             bool &is_compatible)
{
  int ret = OB_SUCCESS;
  const ObUserDefinedType *left_type = NULL;
  const ObUserDefinedType *right_type = NULL;
  ObArenaAllocator allocator;
  is_compatible = false;
  //NOTICE: do not call this function when left_type_id equal to right_type_id
  CK (left_type_id != right_type_id);
  OZ (ns.get_user_type(left_type_id, left_type, &allocator));
  OZ (ns.get_user_type(right_type_id, right_type, &allocator));
  CK (OB_NOT_NULL(left_type) && OB_NOT_NULL(right_type));
  // Assigning One Record Variable to Another
  // You can assign the value of one record variable to another record variable only in these cases:
  // The two variables have the same RECORD type.
  // The target variable is declared with a RECORD type, the source variable is declared with %ROWTYPE,
  // their fields match in number and order, and corresponding fields have the same data type.
  // For record components of composite variables, the types of the composite variables need not match.
  if (OB_FAIL(ret)) {
  } else if (is_json_type_compatible(left_type, right_type)) {
    is_compatible = true;
  } else if (left_type->is_cursor_type() && right_type->is_cursor_type()) {
    is_compatible = true;
  } else if (right_type->is_generic_type()) {
    if ((right_type->is_generic_adt_type()
         || right_type->is_generic_record_type())
        && left_type->is_record_type()) {
      is_compatible = true;
    } else if ((right_type->is_generic_varray_type()
               || right_type->is_generic_v2_table_type()
               || right_type->is_generic_table_type()
               || right_type->is_generic_collection_type())
              && left_type->is_collection_type()) {
      is_compatible = true;
    } else if (right_type->is_generic_ref_cursor_type()
               && left_type->is_cursor_type()) {
      is_compatible = true;
    }
  } else if (left_type->is_record_type()
      && right_type->is_record_type()
      && !left_type->is_udt_type()
      && !right_type->is_udt_type()
      && (left_type->is_rowtype_type() || right_type->is_rowtype_type())) {
    const ObRecordType *left_r_type = static_cast<const ObRecordType *>(left_type);
    const ObRecordType *right_r_type = static_cast<const ObRecordType *>(right_type);
    CK (OB_NOT_NULL(left_r_type) && OB_NOT_NULL(right_r_type));
    OZ (left_r_type->is_compatble(*right_r_type, is_compatible));
  }
  return ret;
}

int ObPLResolver::check_collection_expr_illegal(const ObRawExpr *expr, bool &is_obj_acc)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else {
    is_obj_acc = false;
    const ObOpRawExpr *op_cmp = static_cast<const ObOpRawExpr *>(expr);
    const ObRawExpr *left = op_cmp->get_param_expr(0);
    const ObRawExpr *right = op_cmp->get_param_expr(1);
    // record and collection must be obj access
    if (OB_ISNULL(left) || OB_ISNULL(right)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr left or right children is null", K(ret));
    } else {
      if (T_OBJ_ACCESS_REF == left->get_expr_type()
          && T_OBJ_ACCESS_REF == right->get_expr_type()) {
        const ObObjAccessRawExpr *l = static_cast<const ObObjAccessRawExpr *>(left);
        const ObObjAccessRawExpr *r = static_cast<const ObObjAccessRawExpr *>(right);
        is_obj_acc = true;
        if ((ObObjAccessIdx::get_final_type(l->get_access_idxs()).is_varray_type()
            || ObObjAccessIdx::get_final_type(r->get_access_idxs()).is_varray_type())
            || (ObObjAccessIdx::get_final_type(l->get_access_idxs()).is_associative_array_type()
            || ObObjAccessIdx::get_final_type(r->get_access_idxs()).is_associative_array_type())) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("not supported varray or associative array compared.", K(ret));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "varray or associative array compared");
        }
      }
    }
  }
  return ret;
}

int ObPLResolver::formalize_expr(ObRawExpr &expr)
{
  int ret = OB_SUCCESS;
  stmt::StmtType stmt_type_bak = resolve_ctx_.session_info_.get_stmt_type();
  resolve_ctx_.session_info_.set_stmt_type(stmt::T_NONE);
  OZ (expr.formalize(&resolve_ctx_.session_info_));
  OZ (formalize_expr(expr, &resolve_ctx_.session_info_, current_block_->get_namespace()));
  resolve_ctx_.session_info_.set_stmt_type(stmt_type_bak);
  return ret;
}

int ObPLResolver::formalize_expr(ObRawExpr &expr,
                                 const ObSQLSessionInfo *session_info,
                                 const ObPLINS &ns)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ret)
      && expr.get_result_type().is_ext()
      && OB_INVALID_ID != expr.get_udt_id()) {
    int64_t size = 0;
    const ObUserDefinedType *user_type = NULL;
    OZ (ns.get_user_type(expr.get_udt_id(), user_type, NULL));
    CK (OB_NOT_NULL(user_type));
    OZ (user_type->get_size(ns, PL_TYPE_INIT_SIZE, size));
    OX (const_cast<ObExprResType&>(expr.get_result_type()).set_extend_size(size));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < expr.get_param_count(); ++i) {
    ObRawExpr *child_expr = expr.get_param_expr(i);
    if (OB_NOT_NULL(child_expr)) {
      OZ (formalize_expr(*child_expr, session_info, ns));
      if (OB_SUCC(ret) && expr.is_udf_expr()) {
        ObUDFRawExpr &udf_expr = static_cast<ObUDFRawExpr&>(expr);
        ObIArray<ObExprResType>& params_type = udf_expr.get_params_type();
        if (params_type.at(i).is_ext()) {
          params_type.at(i).set_udt_id(child_expr->get_udt_id());
        }
      }
    }
  }
  return ret;
}

int ObPLResolver::set_cm_warn_on_fail(ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(expr));
  for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
    OZ (set_cm_warn_on_fail(expr->get_param_expr(i)));
  }
  if (OB_SUCC(ret)) {
    if (T_FUN_SYS_CAST == expr->get_expr_type()) {
      expr->set_extra(expr->get_extra() | CM_WARN_ON_FAIL);
    } else if (T_FUN_SUBQUERY == expr->get_expr_type()) {
      ObPlQueryRefRawExpr *subquery_expr = static_cast<ObPlQueryRefRawExpr *>(expr);
      subquery_expr->set_ignore_fail();
    }
  }
  return ret;
}

int ObPLResolver::analyze_expr_type(ObRawExpr *&expr,
                                    ObPLCompileUnitAST &unit_ast)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(expr));
  if (OB_FAIL(ret)) {
  } else if (T_OP_GET_PACKAGE_VAR == expr->get_expr_type()) {
    OX (unit_ast.set_rps());
  } else if (T_FUN_UDF == expr->get_expr_type() ||
             T_OP_GET_SYS_VAR == expr->get_expr_type() ||
             T_OP_GET_USER_VAR == expr->get_expr_type() ||
             (T_FUN_SUBQUERY == expr->get_expr_type() && lib::is_mysql_mode())) { // user var expr has been rewrite to subquery expr in mysql mode
    OX (unit_ast.set_external_state());
  } else {
    for (int64_t i = 0;
         OB_SUCC(ret) &&
         (!unit_ast.is_rps() || !unit_ast.is_external_state()) &&
         i < expr->get_param_count();
         ++i) {
      OZ (analyze_expr_type(expr->get_param_expr(i), unit_ast));
    }
  }
  return ret;
}

int ObPLResolver::resolve_expr(const ParseNode *node,
                               ObPLCompileUnitAST &unit_ast,
                               ObRawExpr *&expr,
                               uint64_t line_number,
                               bool need_add,
                               const ObPLDataType *expected_type,
                               bool is_behind_limit,
                               bool is_add_bool)
{
  int ret = OB_SUCCESS;
  // Step 1: resolve parse node to raw expr
  OZ (build_raw_expr(node, unit_ast, expr, expected_type));
  CK (OB_NOT_NULL(expr));
  OZ (replace_object_compare_expr(expr, unit_ast));
  OZ (analyze_expr_type(expr, unit_ast));
  // check op illegal
  if (OB_SUCC(ret) && OB_NOT_NULL(expr)
      && (T_OP_EQ == expr->get_expr_type() || T_OP_NE == expr->get_expr_type())) {
    bool is_obj_acc = false;
    OZ (check_collection_expr_illegal(expr, is_obj_acc));
  }

  CK (OB_NOT_NULL(current_block_));
#ifdef OB_BUILD_ORACLE_PL
  OZ (current_block_->get_namespace().get_subtype_actually_basetype(expected_type, expected_type));
#endif

  // check op illegal
  if (OB_SUCC(ret) && !OB_ISNULL(expr) && (T_OP_MULTISET == expr->get_expr_type())) {
    bool is_obj_acc = false;
    if (OB_FAIL(check_collection_expr_illegal(expr, is_obj_acc))) {
      LOG_WARN("failed to check collection expr illegal", K(ret));
    } else if (!is_obj_acc) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("multiset op should apply to nested tables", K(ret));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "multiset op is applied to non-nested tables");
    } else {
      // do nothing
    }
  }

  // Step 2: process call param raw expr
  ObCallParamRawExpr *call_expr = NULL;
  if (OB_SUCC(ret) && T_SP_CPARAM == expr->get_expr_type()) {
    CK (OB_NOT_NULL(call_expr = static_cast<ObCallParamRawExpr*>(expr)));
    CK (OB_NOT_NULL(expr = call_expr->get_expr()));
  }

  // Step 3: add pls integer checker
  OZ (formalize_expr(*expr));
  bool need_replace = false;
  if (OB_SUCC(ret) && lib::is_oracle_mode()) {
    OZ (add_pl_integer_checker_expr(expr_factory_, expr, need_replace));
  }
  if (OB_SUCC(ret) && expr->is_udf_expr()) {
    ObUDFRawExpr *udf_expr = static_cast<ObUDFRawExpr *>(expr);
    udf_expr->set_loc(line_number);
  }
  if (is_mysql_mode()) {
    ObRawExprWrapEnumSet enum_set_wrapper(expr_factory_, &resolve_ctx_.session_info_);
    OZ (enum_set_wrapper.analyze_expr(expr));
  }
  OZ (formalize_expr(*expr));

  // Step 4: check complex cast legal
  // 原来是step2，移动这里是因为result type需要deduce一把才能出来结果。
  // 放在step是有问题，因为类似T_SP_CPARAM是没有对应的expr执行实体，需要展开. 没展开deduce就会出问题。
  bool pl_sql_format_convert = false;
  if (OB_SUCC(ret) && OB_NOT_NULL(expected_type)) {
    if (T_OP_ROW == expr->get_expr_type()) {
      ret = OB_ERR_EXPRESSION_WRONG_TYPE;
      LOG_WARN("PLS-00382: expression is of wrong type",
               K(ret), K(expected_type->is_obj_type()), KPC(expr));
    } else if (ObNullType == expr->get_result_type().get_obj_meta().get_type()) {
      // do nothing
    } else if (expected_type->is_opaque_type()
               && expected_type->get_user_type_id() == static_cast<uint64_t>(T_OBJ_XML)
               && expr->get_result_type().is_xml_sql_type()) {
      pl_sql_format_convert = true;
    } else if ((!expected_type->is_obj_type()
                 && expr->get_result_type().get_obj_meta().get_type() != ObExtendType)
               || (expected_type->is_obj_type()
                   && expr->get_result_type().get_obj_meta().get_type() == ObExtendType)) {
      ret = OB_ERR_EXPRESSION_WRONG_TYPE;
      LOG_WARN("PLS-00382: expression is of wrong type",
               K(ret), K(expected_type->is_obj_type()), K(expr->get_result_type().get_obj_meta().get_type()));
    } else if (expected_type->is_composite_type()
               && expr->get_result_type().get_obj_meta().is_ext()
               && expected_type->get_user_type_id() != expr->get_result_type().get_udt_id()
               && expr->get_expr_type() != T_FUN_SYS_PDB_GET_RUNTIME_INFO) {
      bool is_compatible = false;
      OZ (check_composite_compatible(current_block_->get_namespace(),
                                     expected_type->get_user_type_id(),
                                     expr->get_result_type().get_udt_id(),
                                     is_compatible));
      if (OB_FAIL(ret)) {
      } else if (!is_compatible) {
        ret = OB_ERR_INVALID_TYPE_FOR_OP;
#ifdef OB_BUILD_ORACLE_PL
        // error code compiltable with oracle
        if (ObPlJsonUtil::is_pl_json_object_type(expected_type->get_user_type_id())
            && ObPlJsonUtil::is_pl_json_element_type(expr->get_result_type().get_udt_id())) {
          ret = OB_ERR_EXPRESSION_WRONG_TYPE;
        }
#endif
        LOG_WARN("PLS-00382: expression is of wrong type",
                  K(ret), K(expected_type->is_obj_type()), KPC(expr),
                  K(expr->get_result_type().get_obj_meta().get_type()),
                  K(expected_type->get_user_type_id()),
                  K(expr->get_result_type().get_udt_id()));
      }
    }
  }

  // Step 5: check can simple pls integer opt
  bool simple_pls_integer = false;
  if (OB_SUCC(ret)
      && lib::is_oracle_mode()
      && OB_NOT_NULL(expected_type)
      && expected_type->is_pl_integer_type()
      && expected_type->is_default_pls_range()
      && expr->is_const_raw_expr()) {
    //Int类型的Const向PLS_INTEGER赋值，可忽略pl_integer_checker和column_conv
    if (static_cast<ObConstRawExpr*>(expr)->get_value().is_integer_type()
        && static_cast<ObConstRawExpr*>(expr)->get_value().get_int() >= INT32_MIN
        && static_cast<ObConstRawExpr*>(expr)->get_value().get_int() <= INT32_MAX) {
      static_cast<ObConstRawExpr*>(expr)->get_value().set_type(ObInt32Type);
      static_cast<ObConstRawExpr*>(expr)->
                   set_expr_obj_meta(static_cast<ObConstRawExpr*>(expr)->get_value().get_meta());
      expr->set_data_type(ObInt32Type);
      simple_pls_integer = true;
    } else if (static_cast<ObConstRawExpr*>(expr)->get_value().is_number()) {
      int64_t int_value = 0;
      if (static_cast<ObConstRawExpr*>(expr)->get_value().get_number().is_valid_int64(int_value)
          && int_value >= INT32_MIN
          && int_value <= INT32_MAX) {
        static_cast<ObConstRawExpr*>(expr)->get_value().set_int32(static_cast<int32_t>(int_value));
        static_cast<ObConstRawExpr*>(expr)->set_expr_obj_meta(
                                        static_cast<ObConstRawExpr*>(expr)->get_value().get_meta());
        expr->set_data_type(ObInt32Type);
        simple_pls_integer = true;
      }
    } else { /*do nothing*/ }
  } else { /*do nothing*/ }

  // Step 6: check need to cast
  bool need_cast = false;
  const ObDataType *data_type =
    OB_NOT_NULL(expected_type) ? expected_type->get_data_type() : NULL;
  if (OB_SUCC(ret) && !simple_pls_integer && OB_NOT_NULL(data_type) && !pl_sql_format_convert) {
    need_cast = (ob_is_enum_or_set_type(data_type->get_obj_type())
                || expr->get_result_type().get_obj_meta() != data_type->get_meta_type()
                || expr->get_result_type().get_accuracy() != data_type->get_accuracy());
    need_cast = need_cast
      || (expr->get_result_type().get_obj_meta() == data_type->get_meta_type()
          && data_type->get_meta_type().is_integer_type());
  }

  // check boolean expr legal
  if (OB_SUCC(ret) && need_cast && lib::is_oracle_mode()) {
    // expected expr is boolean expr
    if (data_type->get_obj_type() == ObTinyIntType) {
      if (IS_BOOL_OP(expr->get_expr_type())
          || T_NULL == expr->get_expr_type()
          || ObTinyIntType == expr->get_result_type().get_type()) {
        // do nothing ...
      } else if (T_QUESTIONMARK == expr->get_expr_type() &&
                 ObNullType == expr->get_data_type()) {
        // do nothing ...
      } else {
        ret = OB_ERR_INVALID_TYPE_FOR_OP;
        LOG_WARN("PLS-00382: expression is of wrong type",
                  K(ret), KPC(data_type), KPC(expr));
      }
    } else if ((IS_BOOL_OP(expr->get_expr_type()) || ObTinyIntType == expr->get_result_type().get_type())
               && ObTinyIntType != data_type->get_obj_type()) {
      ret = OB_ERR_EXPRESSION_WRONG_TYPE;
      LOG_WARN("PLS-00382: expression is of wrong type", K(ret), KPC(data_type), KPC(expr));
    }
  }

  // check limit for oracle
  if (OB_SUCC(ret) && is_behind_limit && lib::is_oracle_mode()) {
    ObObjMeta limit_meta = expr->get_result_type();
    if (limit_meta.is_numeric_type()) {
      //do nothing
    } else if (limit_meta.is_character_type()) {
      ret = OB_ERR_LIMIT_CLAUSE;
      LOG_WARN("value in LIMIT clause is invalid", K(ret), KPC(data_type), KPC(expr));
    } else {
      ret = OB_ERR_EXPRESSION_WRONG_TYPE;
      LOG_WARN("expression is of wrong type", K(ret), KPC(data_type), KPC(expr));
    }
  }

  // Step 7: do actually cast
  if (OB_FAIL(ret)) {
  } else if (is_add_bool) {
    ObOpRawExpr *bool_expr = NULL;
    if (OB_FAIL(expr_factory_.create_raw_expr(T_OP_BOOL, bool_expr))) {
      LOG_WARN("create bool expr failed", K(ret));
    } else if (OB_ISNULL(bool_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("bool_expr is NULL", K(ret));
    } else {
      OZ(bool_expr->add_flag(IS_INNER_ADDED_EXPR));
      OZ(bool_expr->add_param_expr(expr));
      OX(expr = bool_expr);
      OZ (formalize_expr(*expr));
      OZ (set_cm_warn_on_fail(expr));
    }
  } else if (need_cast) {
    bool need_wrap = false;
    OZ (ObRawExprUtils::need_wrap_to_string(expr->get_result_type().get_type(),
                                            data_type->get_obj_type(),
                                            true,
                                            need_wrap));
    if (OB_SUCC(ret) && need_wrap) {
      ObSysFunRawExpr *out_expr = NULL;
      OZ (ObRawExprUtils::create_type_to_str_expr(expr_factory_,
                                                  expr,
                                                  out_expr,
                                                  &resolve_ctx_.session_info_,
                                                  true));
      CK (OB_NOT_NULL(out_expr));
      OX (expr = out_expr);
    }
    OZ (ObRawExprUtils::build_column_conv_expr(&resolve_ctx_.session_info_,
                                               expr_factory_,
                                               data_type->get_obj_type(),
                                               data_type->get_collation_type(),
                                               data_type->get_accuracy_value(),
                                               true,
                                               NULL, /*"db_name"."tb_name"."col_name"*/
                                               &expected_type->get_type_info(),
                                               expr,
                                               true /*is_in_pl*/));
  }

  // Step 8: add pls integer checker again
  if (OB_SUCC(ret)
     && !simple_pls_integer
     && OB_NOT_NULL(expected_type)
     && expected_type->is_pl_integer_type()
     && is_need_add_checker(expected_type->get_pl_integer_type(), expr)) {
    OZ (add_pl_integer_checker_expr(expr_factory_,
                                    PL_SIMPLE_INTEGER == expected_type->get_pl_integer_type()
                                      ? PL_PLS_INTEGER : expected_type->get_pl_integer_type(),
                                    expected_type->get_lower(),
                                    expected_type->get_upper(),
                                    expr));
    OZ (formalize_expr(*expr));
  }

  // Step 9: const folding opt.
  if (OB_SUCC(ret) && OB_FAIL(replace_to_const_expr_if_need(expr))) {
    // 兼容MySQL,Oralce, 计算过程中如果出错不在此阶段报, 执行阶段报
    ret = OB_SUCCESS;
  }

  //in static typing engine, we wont do implict cast at stage of execution,
  //for some expr like ObExprIn/ObExprArgCase, if type of left is not match with right,
  //we rewrite "a in (b,c)" to "a in b or a in c"
  //"case a when b xx when c xx" to "case when a == b then xx case when a == c then xx"
  if (OB_SUCC(ret)) {
    bool transformed = false;
    OZ(ObTransformPreProcess::transform_expr(unit_ast.get_expr_factory(),
                                              resolve_ctx_.session_info_, expr,
                                              transformed));
  }


  // Step 10: add expr to ast if needed
  OZ (need_add ? unit_ast.add_expr(expr, simple_pls_integer) : OB_SUCCESS);
  // When raw expr calls formalize for type deduce in the new engine, if the src type is
  // inconsistent with the dst type, an implicit cast will be added to ensure that the new engine
  // does not make a mistake in cg. but add_expr calls check_simple_calc_expr to rewrite the
  // type in some special cases, and this may happen after formalize.
  // Therefore, we need to call formalize again after add expr to ensure
  // that the types are inconsistent with an implicit cast
  if (OB_SUCC(ret)) {
    OZ (formalize_expr(*expr));
  }

  // Step 11: process call param raw expr
  if (OB_SUCC(ret) && OB_NOT_NULL(call_expr)) {
    call_expr->set_expr(expr);
    expr = call_expr;
  }
  OZ (ObRawExprUtils::set_call_in_pl(expr));
  if (OB_FAIL(ret)) {
    record_error_line(node, resolve_ctx_.session_info_);
  }
  return ret;
}


int ObPLResolver::transform_subquery_expr(const ParseNode *node,
                                          ObRawExpr *&expr,
                                          const ObPLDataType *expected_type,
                                          ObPLCompileUnitAST &func)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(expr));

  if (OB_SUCC(ret)) {
    pl::ObRecordType *record_type = nullptr;
    if (OB_ISNULL(record_type =
              static_cast<ObRecordType*>(resolve_ctx_.allocator_.alloc(sizeof(ObRecordType))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory", K(ret));
    } else {
      record_type = new(record_type)ObRecordType();
      record_type->set_type_from(PL_TYPE_ATTR_ROWTYPE);
    }

    sql::ObSPIService::ObSPIPrepareResult prepare_result;

    prepare_result.record_type_ = record_type;
    prepare_result.tg_timing_event_ =
                        static_cast<TgTimingEvent>(resolve_ctx_.params_.tg_timing_event_);
    question_mark_cnt_ = node->value_;
    int64_t total_size = 7 + node->str_len_ + strlen(" as 'subquery'") + 1;
    char *sql_str = static_cast<char *>(resolve_ctx_.allocator_.alloc(total_size));
    int64_t sql_len = 0;
    MEMMOVE(sql_str, "SELECT ", 7);
    sql_len += 7;
    MEMMOVE(sql_str + sql_len, node->str_value_, node->str_len_);
    sql_len += node->str_len_;
    MEMMOVE(sql_str + sql_len, " as 'subquery'", strlen(" as 'subquery'"));
    sql_len += strlen(" as 'subquery'");
    sql_str[sql_len] = '\0';

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObSPIService::spi_prepare(resolve_ctx_.allocator_,
                              resolve_ctx_.session_info_,
                              resolve_ctx_.sql_proxy_,
                              resolve_ctx_.schema_guard_,
                              expr_factory_,
                              sql_str,
                              false,
                              &current_block_->get_namespace(),
                              prepare_result))) {
      LOG_WARN("failed to prepare stmt", K(ret));
    } else if (0 != prepare_result.into_exprs_.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected into expr", K(ret));
    } else if (OB_FAIL(func.add_sql_exprs(prepare_result.exec_params_))) {
      LOG_WARN("failed to set precalc exprs", K(prepare_result.exec_params_), K(ret));
    } else if (OB_FAIL(func.add_dependency_objects(prepare_result.ref_objects_))) {
      LOG_WARN("failed to add dependency objects", K(prepare_result.ref_objects_), K(ret));
    } else {
      if (!func.is_modifies_sql_data()) {
        func.set_reads_sql_data();
      }
    }

    if (OB_SUCC(ret)) {
      ObPlQueryRefRawExpr *subquery_expr = nullptr;
      OZ (expr_factory_.create_raw_expr(T_FUN_SUBQUERY, subquery_expr));

      OX (subquery_expr->set_ps_sql(prepare_result.ps_sql_));
      OX (subquery_expr->set_stmt_type(prepare_result.type_));
      OX (subquery_expr->set_route_sql(prepare_result.route_sql_));

      for (int64_t i = 0; OB_SUCC(ret) && i < prepare_result.exec_params_.count(); ++i) {
        OZ (subquery_expr->add_param_expr(prepare_result.exec_params_.at(i)));
      }

      ObExprResType result_type;
      // todo: subquery expression result type need optimizate in mysql mode
      ObPLDataType data_type(ObVarcharType);
      const ObDataType *subquery_data_type = nullptr;
      if (OB_SUCC(ret)) {
        if (nullptr == expected_type || nullptr == expected_type->get_meta_type() ||
            nullptr == expected_type->get_data_type()) {
          expected_type = &data_type;
        }
        subquery_data_type = expected_type->get_data_type();

        OX (result_type.set_accuracy(subquery_data_type->get_accuracy()));
        OX (result_type.set_type(subquery_data_type->get_obj_type()));
        OX (result_type.set_meta(subquery_data_type->get_meta_type()));
        OX (result_type.set_length(subquery_data_type->get_length()));

        ObCollationType collation_type = result_type.get_collation_type();
        if (OB_SUCC(ret) && CS_TYPE_INVALID == collation_type) {
          OZ (resolve_ctx_.session_info_.get_collation_connection(collation_type));

          OX (result_type.set_collation_type(collation_type));
          OX (result_type.set_collation_level(CS_LEVEL_IMPLICIT));
        }

        OX (subquery_expr->set_subquery_result_type(result_type));
        OX (expr = subquery_expr);
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("inner error", K(ret));
      }
    }
  }
  return ret;
}

int ObPLResolver::check_expr_can_pre_calc(ObRawExpr *expr, bool &pre_calc)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(expr));
  // 在SQL下可以提前计算的表达式,在PL下则不一定能提前计算, 如ROW_COUNT, ROW%COUNT
  // 暂时没有统一的规则计算可以在PL端提前计算的表达式,因此暂时仅放开部分表达式
  if (!(
    (IS_CONST_TYPE(expr->get_expr_type()) && T_QUESTIONMARK != expr->get_expr_type())
    || T_FUN_SYS_STR_TO_DATE == expr->get_expr_type()
    || T_FUN_SYS_TO_DATE == expr->get_expr_type()
    || T_FUN_COLUMN_CONV == expr->get_expr_type())) {
    pre_calc = false;
  }
  for (int64_t i = 0; OB_SUCC(ret) && pre_calc && i < expr->get_param_count(); ++i) {
    OZ (check_expr_can_pre_calc(expr->get_param_expr(i), pre_calc));
  }
  LOG_DEBUG("check_expr_can_pre_calc", K(pre_calc), K(ret), KPC(expr));
  return ret;
}

int ObPLResolver::replace_to_const_expr_if_need(ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  bool pre_calc = false;
  CK (OB_NOT_NULL(expr));
  LOG_DEBUG("start replaceto const expr if need", K(ret), KPC(expr));
  if (OB_SUCC(ret) && expr->is_const_expr()) {
    pre_calc = true;
    OZ (check_expr_can_pre_calc(expr, pre_calc));
  }
  if (OB_SUCC(ret) && pre_calc && !expr->is_const_raw_expr()) {
    ObObj result;
    ObConstRawExpr *const_expr = NULL;
    OZ (ObSPIService::spi_calc_raw_expr(&(resolve_ctx_.session_info_),
                                        &(resolve_ctx_.allocator_),
                                        expr,
                                        &result));
    OZ (expr_factory_.create_raw_expr(static_cast<ObItemType>(result.get_meta().get_type()),
                                      const_expr));
    CK (OB_NOT_NULL(const_expr));
    OX (const_expr->set_value(result));
    OX (expr = const_expr);
  }
  LOG_DEBUG("end replaceto const expr if need", K(ret), KPC(expr));
  return ret;
}

int ObPLResolver::resolve_columns(ObRawExpr *&expr, ObArray<ObQualifiedName> &columns, ObPLCompileUnitAST &unit_ast)
{
  int ret = OB_SUCCESS;
  ObArray<ObRawExpr*> real_exprs;
  for(int64_t i = 0; OB_SUCC(ret) && i < columns.count(); i++) {
    ObQualifiedName &q_name = columns.at(i);
    ObRawExpr *ref_expr = NULL;
    OZ (resolve_qualified_identifier(q_name, columns, real_exprs, unit_ast, ref_expr),
                                     K(q_name), K(columns), K(real_exprs), K(unit_ast), K(ref_expr));
    CK (OB_NOT_NULL(ref_expr));
    OZ (real_exprs.push_back(ref_expr), ref_expr);
    OZ (ObRawExprUtils::replace_ref_column(expr, q_name.ref_expr_, ref_expr));
    if (OB_SUCC(ret) && T_SP_CPARAM == expr->get_expr_type()) {
      ObCallParamRawExpr* call_expr = static_cast<ObCallParamRawExpr*>(expr);
      CK (ref_expr != expr);
      CK (ref_expr->get_expr_type() != T_SP_CPARAM);
      CK (OB_NOT_NULL(call_expr));
      if (OB_SUCC(ret)) {
        ObRawExpr *param_child = call_expr->get_expr();
        if (OB_NOT_NULL(param_child)) {
          if (param_child == q_name.ref_expr_) {
            OX (call_expr->set_expr(ref_expr));
          } else {
            OZ (ObRawExprUtils::replace_ref_column(param_child, q_name.ref_expr_, ref_expr));
          }
        }
      }
    }
    if (OB_SUCC(ret) && ref_expr->is_obj_access_expr()) { //ObObjAccessrawExpr需额外存储一份
      OZ (unit_ast.add_obj_access_expr(ref_expr), q_name, ref_expr);
    }
  }
  return ret;
}

int ObPLResolver::resolve_raw_expr(const ParseNode &node,
                                   sql::ObResolverParams &params,
                                   ObRawExpr *&expr,
                                   bool for_write,
                                   const ObPLDataType *expected_type)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(params.allocator_));
  CK (OB_NOT_NULL(params.session_info_));
  CK (OB_NOT_NULL(params.schema_checker_));
  CK (OB_NOT_NULL(params.schema_checker_->get_schema_guard()));
  CK (OB_NOT_NULL(params.sql_proxy_));
  CK (OB_NOT_NULL(params.expr_factory_));
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(params.secondary_namespace_)) {
    HEAP_VAR(pl::ObPLFunctionAST, func_ast, *(params.allocator_)) {
      ObPLStmtBlock *null_block = NULL;
      ObPLPackageGuard package_guard(params.session_info_->get_effective_tenant_id());
      ObPLResolver resolver(*(params.allocator_),
                            *(params.session_info_),
                            *(params.schema_checker_->get_schema_guard()),
                            package_guard,
                            *(params.sql_proxy_),
                            *(params.expr_factory_),
                            NULL,/*parent ns*/
                            params.is_prepare_protocol_,
                            false, /*check mode*/
                            true, /*sql mode*/
                            params.param_list_);
      OZ (package_guard.init());
      OZ (resolver.init(func_ast));
      // build first namespace
      OZ (resolver.make_block(func_ast, NULL, null_block));
      OZ (resolver.resolve_expr(&node, func_ast, expr,
                                resolver.combine_line_and_col(node.stmt_loc_),
                                false, expected_type));
      OZ (resolver.check_variable_accessible(expr, for_write));
    }
  } else {
    HEAP_VAR(pl::ObPLFunctionAST, func_ast, *(params.allocator_)) {
      ObPLBlockNS &ns = *(params.secondary_namespace_);
      ObPLResolver resolver(*(params.allocator_),
                            ns.get_external_ns()->get_resolve_ctx().session_info_,
                            ns.get_external_ns()->get_resolve_ctx().schema_guard_,
                            ns.get_external_ns()->get_resolve_ctx().package_guard_,
                            ns.get_external_ns()->get_resolve_ctx().sql_proxy_,
                            *(params.expr_factory_),
                            ns.get_external_ns()->get_parent_ns(),
                            params.is_prepare_protocol_,
                            false, /*check mode*/
                            true, /*sql mode*/
                            params.param_list_);
      OZ (resolver.init(func_ast));
      OX (resolver.get_current_namespace() = ns);
      OZ (resolver.resolve_expr(&node, func_ast, expr,
                                resolver.combine_line_and_col(node.stmt_loc_),
                                false, expected_type));
      OZ (resolver.check_variable_accessible(expr, for_write));
    }
  }
  return ret;
}

int ObPLResolver::resolve_raw_expr(const ParseNode &node,
                                   common::ObIAllocator &allocator,
                                   ObRawExprFactory &expr_factory,
                                   ObPLBlockNS &ns,
                                   bool is_prepare_protocol,
                                   ObRawExpr *&expr,
                                   bool for_write)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(ns.get_external_ns()));
  if (OB_SUCC(ret)) {
    ObArray<ObQualifiedName> columns;
    ObArray<ObVarInfo> sys_vars;
    ObArray<ObAggFunRawExpr*> aggr_exprs;
    ObArray<ObWinFunRawExpr*> win_exprs;
    ObArray<ObSubQueryInfo> sub_query_info;
    ObArray<ObUDFInfo> udf_info;
    ObArray<ObOpRawExpr*> op_exprs;

    ObPLResolver resolver(allocator,
                          ns.get_external_ns()->get_resolve_ctx().session_info_,
                          ns.get_external_ns()->get_resolve_ctx().schema_guard_,
                          ns.get_external_ns()->get_resolve_ctx().package_guard_,
                          ns.get_external_ns()->get_resolve_ctx().sql_proxy_,
                          expr_factory,
                          ns.get_external_ns()->get_parent_ns(),
                          is_prepare_protocol,
                          false,
                          false,
                          ns.get_external_ns()->get_resolve_ctx().params_.param_list_);
    HEAP_VAR(pl::ObPLFunctionAST, func_ast, allocator) {
      OC( (resolver.init)(func_ast) );
      if (OB_SUCC(ret)) {
        resolver.get_current_namespace() = ns;

        OZ (resolver.build_raw_expr(node,
                                    expr,
                                    columns,
                                    sys_vars,
                                    aggr_exprs,
                                    win_exprs,
                                    sub_query_info,
                                    udf_info,
                                    op_exprs,
                                    is_prepare_protocol));

        for(int64_t i = 0; OB_SUCC(ret) && i < columns.count(); ++i) {
          OZ (resolver.resolve_columns(expr, columns, static_cast<ObPLCompileUnitAST &>(func_ast)));
        }

        // 该接口调用栈:
        // resolve_into_variable_node -> resolve_external_expr -> resolve_raw_expr
        // into variable 不支持udf， 这里报错处理
        if (OB_SUCC(ret) && udf_info.count() > 0) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("into variable is not support udf", K(ret));
        }
      }
      CK (OB_NOT_NULL(expr));
      OZ (expr->extract_info());
      OZ (expr->deduce_type(&ns.get_external_ns()->get_resolve_ctx().session_info_));

      OZ (resolver.check_variable_accessible(expr, for_write));
    }
  }

  if (OB_SUCC(ret) && expr->is_obj_access_expr()) {
    OZ(expr->formalize(&ns.get_external_ns()->get_resolve_ctx().session_info_));
    OZ (set_write_property(expr,
                           expr_factory,
                           &ns.get_external_ns()->get_resolve_ctx().session_info_,
                           &ns.get_external_ns()->get_resolve_ctx().schema_guard_,
                           for_write));
  }
  return ret;
}

int ObPLResolver::init_udf_info_of_accessident(ObObjAccessIdent &access_ident)
{
  int ret = OB_SUCCESS;
  ObUDFRawExpr *func_expr = NULL;
  OX (new (&access_ident.udf_info_) ObUDFInfo());
  OZ (expr_factory_.create_raw_expr(T_FUN_UDF, func_expr));
  CK (OB_NOT_NULL(func_expr));
  for (int64_t i = 0; OB_SUCC(ret) && i < access_ident.params_.count(); ++i) {
    std::pair<ObRawExpr*, int64_t> &param = access_ident.params_.at(i);
    if (0 == param.second) {
      OZ (func_expr->add_param_expr(param.first));
      OX (access_ident.udf_info_.udf_param_num_++);
    } else {
      break;
    }
  }
  OX (func_expr->set_func_name(access_ident.access_name_));
  OX (access_ident.udf_info_.ref_expr_ = func_expr);
  return ret;
}

int ObPLResolver::init_udf_info_of_accessidents(ObIArray<ObObjAccessIdent> &access_idents)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < access_idents.count(); ++i) {
    OZ (init_udf_info_of_accessident(access_idents.at(i)));
  }
  return ret;
}

int ObPLResolver::mock_self_param(bool need_rotate,
                                  ObIArray<ObObjAccessIdent> &obj_access_idents,
                                  ObIArray<ObObjAccessIdx> &self_access_idxs,
                                  ObPLFunctionAST &func)
{
  int ret = OB_SUCCESS;
  uint64_t acc_cnt = obj_access_idents.count();
  ObRawExpr *self_arg = NULL;
  if (!(self_access_idxs.count() > 0 &&
        ObObjAccessIdx::IS_UDT_NS == self_access_idxs.at(self_access_idxs.count() - 1).access_type_)) {
    if (self_access_idxs.at(self_access_idxs.count() - 1).is_udf_type()) {
      OX (self_arg = self_access_idxs.at(self_access_idxs.count() - 1).get_sysfunc_);
      CK (OB_NOT_NULL(self_arg));
    } else {
      OZ (make_var_from_access(self_access_idxs,
                               expr_factory_,
                               &resolve_ctx_.session_info_,
                               &resolve_ctx_.schema_guard_,
                               current_block_->get_namespace(),
                               self_arg), K(obj_access_idents), K(self_access_idxs));
      OZ (func.add_obj_access_expr(self_arg));
    }
    OZ (func.add_expr(self_arg));
    OZ (obj_access_idents.at(acc_cnt - 1).params_.push_back(std::make_pair(self_arg, 0)));
    if (OB_SUCC(ret) && need_rotate) {
      std::rotate(obj_access_idents.at(acc_cnt - 1).params_.begin(),
                  obj_access_idents.at(acc_cnt - 1).params_.begin()
                    + obj_access_idents.at(acc_cnt - 1).params_.count() - 1,
                  obj_access_idents.at(acc_cnt - 1).params_.end());
    }
  }
  return ret;
}

int ObPLResolver::resolve_inner_call(
  const ParseNode *parse_tree, ObPLStmt *&stmt, ObPLFunctionAST &func)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(parse_tree));
  CK (OB_LIKELY(T_SP_INNER_CALL_STMT == parse_tree->type_));
  CK (OB_LIKELY(1 == parse_tree->num_child_) || (3 == parse_tree->num_child_));
  if (OB_SUCC(ret)) {
    ObArray<ObObjAccessIdent> obj_access_idents;
    ObArray<ObObjAccessIdx> access_idxs;
    ObArray<ObObjAccessIdx> self_access_idxs;
    ObArray<ObRawExpr *> expr_params;
    if (1 == parse_tree->num_child_) {
      OZ (resolve_obj_access_idents(*parse_tree->children_[0], obj_access_idents, func));
      OZ (init_udf_info_of_accessidents(obj_access_idents));
      for (int64_t i = 0; OB_SUCC(ret) && i < obj_access_idents.count(); ++i) {
        // TODO: distinguish coll(idx).proc() and func(arg1).proc()
        bool is_routine = obj_access_idents.at(i).is_pl_udf()
          || (obj_access_idents.at(i).params_.count() != 1 && obj_access_idents.at(i).has_brackets_);
        if (i == obj_access_idents.count() - 1) {
          if (access_idxs.count() > 0) {
            if (access_idxs.at(access_idxs.count() - 1).access_type_ == ObObjAccessIdx::IS_DB_NS
                || access_idxs.at(access_idxs.count() - 1).access_type_ == ObObjAccessIdx::IS_PKG_NS
                || access_idxs.at(access_idxs.count() - 1).access_type_ == ObObjAccessIdx::IS_LABEL_NS
                || access_idxs.at(access_idxs.count() - 1).access_type_ == ObObjAccessIdx::IS_UDT_NS
                || access_idxs.at(access_idxs.count() - 1).access_type_ == ObObjAccessIdx::IS_DBLINK_PKG_NS) {
              is_routine = true;
            }
          } else {
            is_routine = true;
          }
        } else {
          obj_access_idents.at(i).set_pl_udf();
        }
        int64_t idx_cnt = access_idxs.count();
        OZ (resolve_access_ident(obj_access_idents.at(i),
                                current_block_->get_namespace(),
                                expr_factory_,
                                &resolve_ctx_.session_info_,
                                access_idxs,
                                func,
                                is_routine),
                                K(access_idxs), K(i));
        OZ (obj_access_idents.at(i).extract_params(0, expr_params));
        OX (idx_cnt = (idx_cnt >= access_idxs.count()) ? 0 : idx_cnt);
        if (OB_SUCC(ret)
            && (expr_params.count() > 0 || obj_access_idents.at(i).has_brackets_)
            && !access_idxs.at(idx_cnt).is_procedure()
            && !access_idxs.at(idx_cnt).is_udf_type()
            && !access_idxs.at(idx_cnt).is_system_procedure()
            && !access_idxs.at(idx_cnt).is_type_method()
            && (!access_idxs.at(idx_cnt).var_type_.is_composite_type() || 0 == expr_params.count())) {
          ret = OB_ERR_OUT_OF_SCOPE;
          LOG_WARN("PLS-00225: subprogram or cursor reference is out of scope", K(ret), K(access_idxs.at(access_idxs.count()-1)));
          LOG_USER_ERROR(OB_ERR_OUT_OF_SCOPE, obj_access_idents.at(i).access_name_.length(), obj_access_idents.at(i).access_name_.ptr());
        }
        if (OB_SUCC(ret)
            && obj_access_idents.count() >= 2
            && i == (obj_access_idents.count() - 2)) {
          OZ (self_access_idxs.assign(access_idxs));
        }
      }
    } else {
      OZ (resolve_dblink_idents(*parse_tree, obj_access_idents, func, access_idxs));
      // PLFunction with dblink call statement cannot be added to plan cache
      OX (func.set_can_cached(false));
    }

    if (OB_SUCC(ret)) {
      int64_t idx_cnt = access_idxs.count();
      int64_t idents_cnt = obj_access_idents.count();
      CK (OB_LIKELY(idx_cnt != 0));
      if (OB_FAIL(ret)) {
      } else if (access_idxs.at(idx_cnt - 1).is_system_procedure()) {
        if (0 == access_idxs.at(idx_cnt - 1).var_name_.case_compare("RAISE_APPLICATION_ERROR")) {
          ObPLRaiseAppErrorStmt *raise_stmt = NULL;
          ObArray<ObRawExpr*> params;
          OZ (stmt_factory_.allocate(PL_RAISE_APPLICATION_ERROR, current_block_, stmt));
          CK (OB_NOT_NULL(raise_stmt = static_cast<ObPLRaiseAppErrorStmt *>(stmt)));
          OZ (obj_access_idents.at(idents_cnt-1).extract_params(0, params));
          for (int64_t  i = 0; OB_SUCC(ret) && i < params.count(); ++i) {
            int64_t expr_idx = OB_INVALID_ID;
            CK (OB_NOT_NULL(params.at(i)));
            if (OB_SUCC(ret) && params.at(i)->get_expr_type() == T_SP_CPARAM) {
              ret = OB_NOT_SUPPORTED;
              LOG_WARN("not supported assign parameter yet", K(ret));
              LOG_USER_ERROR(OB_NOT_SUPPORTED, "assign parameter in RAISE_APPLICATION_ERROR");
            }
            CK (has_exist_in_array(func.get_exprs(), params.at(i), &expr_idx));
            OZ (raise_stmt->add_param(expr_idx));
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("only support raise application error as system procedure for now",
                   K(ret), K(access_idxs), K(idx_cnt));
        }
      } else if (access_idxs.at(idx_cnt - 1).is_procedure()) {
        ObPLCallStmt *call_stmt = NULL;
        if (OB_FAIL(stmt_factory_.allocate(PL_CALL, current_block_, stmt))) {
          LOG_WARN("failed to alloc stmt", K(ret));
        } else if (OB_ISNULL(call_stmt = static_cast<ObPLCallStmt *>(stmt))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to cast stmt", K(ret));
        } else if (access_idxs.at(idx_cnt-1).is_internal_procedure()) {
          ObArray<ObRawExpr*> params;
          const ObPLRoutineInfo *package_routine_info = static_cast<const ObPLRoutineInfo *>(access_idxs.at(idx_cnt-1).routine_info_);
          CK (OB_NOT_NULL(package_routine_info));
          if (OB_SUCC(ret)) {
            call_stmt->set_proc_id(package_routine_info->get_id());
            call_stmt->set_package_id(func.get_package_id());
            OX (call_stmt->set_route_sql(package_routine_info->get_route_sql()));
            if (package_routine_info->has_accessible_by_clause()) {
              OZ (check_package_accessible(
                current_block_, resolve_ctx_.schema_guard_, func.get_package_id()));
            }
            if (OB_SUCC(ret) && package_routine_info->is_udt_routine()) {
              call_stmt->set_is_object_udf();
            }
            // mock udt udf's self param
            if (OB_SUCC(ret)
                && package_routine_info->is_udt_routine()
                && !package_routine_info->is_udt_static_routine()) {
              if (idents_cnt > 0 && obj_access_idents.at(idents_cnt - 1).udf_info_.is_udt_udf_) {
                call_stmt->set_is_object_udf();
              }
              const ObIArray<ObPLRoutineParam *> &routine_params
                                  = package_routine_info->get_params();
              const ObPLRoutineParam *self_param = NULL;
              int64_t self_param_pos = OB_INVALID_INDEX;
              for (int64_t i = 0; i < routine_params.count(); ++i) {
                if (routine_params.at(i)->is_self_param()) {
                  self_param = routine_params.at(i);
                  self_param_pos = i;
                  break;
                }
              }
              if (OB_NOT_NULL(self_param)) {
                const ObIRoutineInfo* routine_info = NULL;
                CK (0 == self_param_pos || self_param_pos == routine_params.count() - 1);
                CK (OB_NOT_NULL(routine_info = access_idxs.at(idx_cnt - 1).routine_info_));

                if (OB_FAIL(ret)) {
                } else if (routine_info->is_udt_routine()
                           && !(routine_info->is_udt_static_routine() || routine_info->is_udt_cons())) {
                  if (idx_cnt > 1 && idents_cnt > 1) {
                    OZ (mock_self_param(0 == self_param_pos, obj_access_idents, self_access_idxs, func));
                  } else {
                    ObConstRawExpr *question_expr = NULL;
                    OZ (expr_factory_.create_raw_expr(T_QUESTIONMARK, question_expr));
                    CK (OB_NOT_NULL(question_expr));
                    if (OB_SUCC(ret)) {
                      ObObjParam val;
                      ObExprResType type;
                      val.set_unknown(0);
                      val.set_param_meta();
                      question_expr->set_value(val);
                      type.set_ext();
                      question_expr->set_result_type(type);
                      OZ (question_expr->extract_info());
                      OZ (question_expr->add_flag(IS_UDT_UDF_SELF_PARAM));
                      OZ (obj_access_idents.at(idents_cnt - 1)
                            .params_.push_back(std::make_pair(question_expr, 0)));
                      OZ (func.add_expr(question_expr));
                      if (OB_SUCC(ret) && 0 == self_param_pos) {
                        std::rotate(obj_access_idents.at(idents_cnt-1).params_.begin(),
                                obj_access_idents.at(idents_cnt-1).params_.begin()
                                  + obj_access_idents.at(idents_cnt-1).params_.count() - 1,
                                obj_access_idents.at(idents_cnt-1).params_.end());
                      }
                    }
                  }
                } else {
                  ObConstRawExpr *null_expr = NULL;
                  OZ (expr_factory_.create_raw_expr(T_NULL, null_expr));
                  CK (OB_NOT_NULL(null_expr));
                  OZ (null_expr->extract_info());
                  OZ (null_expr->add_flag(IS_UDT_UDF_SELF_PARAM));
                  OZ (obj_access_idents.at(idents_cnt - 1)
                        .params_.push_back(std::make_pair(null_expr, 0)));
                  OZ (func.add_expr(null_expr));
                  if (OB_SUCC(ret) && 0 == self_param_pos) {
                    std::rotate(obj_access_idents.at(idents_cnt-1).params_.begin(),
                                obj_access_idents.at(idents_cnt-1).params_.begin()
                                  + obj_access_idents.at(idents_cnt-1).params_.count() - 1,
                                obj_access_idents.at(idents_cnt-1).params_.end());
                  }
                }
              }
            }
            // end mock self param
            OZ (obj_access_idents.at(idents_cnt-1).extract_params(0, params));
            if (OB_FAIL(ret)){
            } else if (package_routine_info->get_param_count() != 0) {
              OZ (resolve_call_param_list(params, package_routine_info->get_params(), call_stmt, func));
            } else if (params.count() != 0) {
              ret = OB_INVALID_ARGUMENT_NUM;
              LOG_WARN("ORA-06553:PLS-306:wrong number or types of arguments in call procedure",
                       K(ret), K(params.count()), K(package_routine_info->get_param_count()));
            }
          }
        } else if (access_idxs.at(idx_cnt - 1).is_external_procedure()) {
          ObArray<ObRawExpr*> params;
          const share::schema::ObRoutineInfo *schema_routine_info
                     = static_cast<const ObRoutineInfo *>(access_idxs.at(idx_cnt - 1).routine_info_);
          CK (OB_NOT_NULL(schema_routine_info));
          OX (call_stmt->set_package_id(schema_routine_info->get_package_id()));
          OX (call_stmt->set_dblink_id(schema_routine_info->get_dblink_id()));
          if (OB_FAIL(ret)) {
          } else if (OB_INVALID_ID == schema_routine_info->get_package_id()) {
            call_stmt->set_proc_id(schema_routine_info->get_routine_id());
          } else {
            call_stmt->set_proc_id(schema_routine_info->get_subprogram_id());
          }
          OX (call_stmt->set_route_sql(schema_routine_info->get_route_sql()));
          OZ (check_routine_accessible(
            current_block_, resolve_ctx_.schema_guard_, *schema_routine_info));
          if (OB_SUCC(ret)) {
            const common::ObIArray<share::schema::ObRoutineParam*> &routine_params
                                                     = schema_routine_info->get_routine_params();
            int64_t idents_cnt = obj_access_idents.count();
            if (schema_routine_info->is_udt_routine()) {
              call_stmt->set_is_object_udf();
            }
            if (OB_SUCC(ret) && !schema_routine_info->is_udt_static_routine()) {
              if (idents_cnt > 1 && idx_cnt > 1) {
                // call_stmt->set_is_object_udf();
                const ObRoutineParam *self_param = NULL;
                int64_t self_param_pos = OB_INVALID_INDEX;
                for (int64_t i = 0; i < routine_params.count(); ++i) {
                  if (routine_params.at(i)->is_self_param()) {
                    self_param = routine_params.at(i);
                    self_param_pos = i;
                    break;
                  }
                }
                // member procedure can be used as static procedure, if pass correct argument
                if (OB_NOT_NULL(self_param)) {
                  CK (0 == self_param_pos || self_param_pos == routine_params.count() - 1);
                  OZ(mock_self_param(0 == self_param_pos, obj_access_idents, self_access_idxs, func));
                }
              }
            }
            OZ (obj_access_idents.at(idents_cnt - 1).extract_params(0, params));
            if (routine_params.count() != 0) {
              OZ (resolve_call_param_list(params, routine_params, call_stmt, func));
            } else if (params.count() != 0) {
              ret = OB_INVALID_ARGUMENT_NUM;
              LOG_WARN("ORA-06553:PLS-306:wrong number or types of arguments in call procedure",
                       K(ret), K(params.count()), K(schema_routine_info->get_param_count()));
            }
          }
        } else if (access_idxs.at(idx_cnt - 1).is_nested_procedure()) {
          ObArray<ObRawExpr*> params;
          const ObPLRoutineInfo *root_routine_info
              = static_cast<const ObPLRoutineInfo *>(access_idxs.at(idx_cnt-1).routine_info_);
          CK (OB_NOT_NULL(root_routine_info));
          if (OB_SUCC(ret)) {
            call_stmt->set_package_id(func.get_package_id());
            call_stmt->set_proc_id(root_routine_info->get_parent_id());
            OX (call_stmt->set_route_sql(root_routine_info->get_route_sql()));
            OZ (call_stmt->set_subprogram_path(root_routine_info->get_subprogram_path()));
            OZ (obj_access_idents.at(idx_cnt-1).extract_params(0, params));
            if (root_routine_info->get_param_count() != 0) {
              OZ (resolve_call_param_list(params, root_routine_info->get_params(), call_stmt, func));
            } else if (params.count() != 0) {
              ret = OB_INVALID_ARGUMENT_NUM;
              LOG_WARN("ORA-06553:PLS-306:wrong number or types of arguments in call procedure",
                       K(ret), K(params.count()), K(root_routine_info->get_param_count()));
            }
          }
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("not support procedure type", K(access_idxs.at(idx_cnt - 1)), K(ret));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "procedure type");
          LOG_USER_ERROR(OB_NOT_SUPPORTED, const_cast<char *> (func.get_name().ptr()));
        }
      } else if (access_idxs.at(idx_cnt - 1).is_type_method()) {
        ObRawExpr *expr = NULL;
        ObPLDataType *type = NULL;
        int64_t expr_idx = OB_INVALID_INDEX, x = OB_INVALID_INDEX, y = OB_INVALID_INDEX;
        // for now, support extend,delete only, need check readonly prop
        OZ (check_variable_accessible(current_block_->get_namespace(), access_idxs, true));
        OZ (make_var_from_access(access_idxs, expr_factory_,
                     &resolve_ctx_.session_info_, &resolve_ctx_.schema_guard_,
                     get_current_namespace(), expr, false));
        OZ (func.add_obj_access_expr(expr));
        OZ (func.add_expr(expr));
        CK (OB_NOT_NULL(type = &(access_idxs.at(access_idxs.count() - 2).elem_type_)));
        CK (type->is_collection_type());
        OZ (type->get_all_depended_user_type(get_resolve_ctx(), get_current_namespace()));
        CK (OB_INVALID_ID != (expr_idx = func.get_expr_count()-1));
        if (OB_SUCC(ret)) {
          if (type->is_associative_array_type()
              && 0 == access_idxs.at(access_idxs.count()-1).var_name_.case_compare("extend")) {
            ret = OB_ERR_CALL_WRONG_ARG;
            LOG_USER_ERROR(OB_ERR_CALL_WRONG_ARG,
                           ObString("EXTEND").length(), ObString("EXTEND").ptr());
          } else if (!access_idxs.at(access_idxs.count()-1).type_method_params_.empty()) {
            x = access_idxs.at(access_idxs.count()-1).type_method_params_.at(0);
            if (2 == access_idxs.at(access_idxs.count()-1).type_method_params_.count()) {
              y = access_idxs.at(access_idxs.count()-1).type_method_params_.at(1);
            }
          } else if (0 == access_idxs.at(idx_cnt-1).var_name_.case_compare("extend")) {
            ObConstRawExpr *expr = NULL;
            OZ (ObRawExprUtils::build_const_int_expr(expr_factory_, ObIntType, 1, expr));
            CK (OB_NOT_NULL(expr));
            OZ (func.add_expr(expr));
            if (OB_SUCC(ret)) {
              x = func.get_expr_count() - 1;
            }
          }
        }
        if (OB_SUCC(ret)) {
          CK (0 == access_idxs.at(idx_cnt-1).var_name_.case_compare("extend")
              || 0 == access_idxs.at(idx_cnt-1).var_name_.case_compare("delete")
              || 0 == access_idxs.at(idx_cnt-1).var_name_.case_compare("trim"));
          if (0 == access_idxs.at(idx_cnt-1).var_name_.case_compare("extend")) {
            ObPLExtendStmt *extend_stmt = NULL;
            OZ (stmt_factory_.allocate(PL_EXTEND, current_block_, stmt));
            CK (OB_NOT_NULL(extend_stmt = static_cast<ObPLExtendStmt *>(stmt)));
            if (OB_SUCC(ret)) {
              extend_stmt->set_extend(expr_idx);
              extend_stmt->set_ni(x, y);
            }
          } else if (0 == access_idxs.at(idx_cnt-1).var_name_.case_compare("delete")){
            ObPLDeleteStmt *delete_stmt = NULL;
            OZ (stmt_factory_.allocate(PL_DELETE, current_block_, stmt));
            CK (OB_NOT_NULL(delete_stmt = static_cast<ObPLDeleteStmt *>(stmt)));
            if (OB_SUCC(ret)) {
              delete_stmt->set_delete(expr_idx);
              delete_stmt->set_mn(x, y);
            }
          } else {
            ObPLTrimStmt *trim_stmt = NULL;
            OZ (stmt_factory_.allocate(PL_TRIM, current_block_, stmt));
            CK (OB_NOT_NULL(trim_stmt = static_cast<ObPLTrimStmt *>(stmt)));
            if (OB_SUCC(ret)) {
              trim_stmt->set_trim(expr_idx);
              trim_stmt->set_n(x);
            }
          }
        }
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not support inner call function type", K(ret), K(access_idxs.at(idx_cnt - 1)));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "inner call function type");
        LOG_USER_ERROR(OB_NOT_SUPPORTED, const_cast<char *> (func.get_name().ptr()));
      }
    }
  }

  return ret;
}

int ObPLResolver::resolve_obj_access_node(const ParseNode &node,
                                          ObSQLSessionInfo &session_info,
                                          ObRawExprFactory &expr_factory,
                                          ObSchemaGetterGuard &schema_guard,
                                          ObMySQLProxy &sql_proxy,
                                          ObIArray<ObObjAccessIdent> &obj_access_idents,
                                          ObIArray<ObObjAccessIdx>& access_idxs)
{
  int ret = OB_SUCCESS;
  pl::ObPLPackageGuard package_guard(session_info.get_effective_tenant_id());
  ObArenaAllocator allocator;
  // fake resolve_ctx, we only use session_info, schema_guard
  ObPLResolveCtx resolve_ctx(
    allocator, session_info, schema_guard, package_guard, *(GCTX.sql_proxy_), false);
  ObPLExternalNS external_ns(resolve_ctx, NULL);
  CK (T_SP_OBJ_ACCESS_REF == node.type_);
  OZ (package_guard.init());
  OZ (ObPLResolver::resolve_obj_access_idents(node, expr_factory, obj_access_idents, session_info));
  for (int64_t i = 0; OB_SUCC(ret) && i < obj_access_idents.count(); ++i) {
    OZ (ObPLResolver::resolve_access_ident(obj_access_idents.at(i),
                                           external_ns,
                                           access_idxs));
  }
  if (OB_FAIL(ret)) {
    record_error_line(const_cast<const ObStmtNodeTree*>(&node), session_info);
  }
  return ret;
}

int ObPLResolver::resolve_cparam_list_simple(const ParseNode &node,
                                             ObRawExprFactory &expr_factory,
                                             ObIArray<ObObjAccessIdent> &obj_access_idents)
{
  int ret = OB_SUCCESS;
  CK (T_SP_CPARAM_LIST == node.type_);
  for (int64_t i = 0; OB_SUCC(ret) && i < node.num_child_; ++i) {
    const ParseNode* child_node = node.children_[i];
    CK (OB_NOT_NULL(child_node));
    if (OB_FAIL(ret)) {
    } else if (child_node->type_ != T_INT) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not supported T_SP_CPARAM_LIST in resolve_obj_access_idents_sample",
               K(ret), K(child_node->type_));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "T_SP_CPARAM_LIST in resolve_obj_access_idents_sample");
    } else {
      ObConstRawExpr *expr = NULL;
      OZ (ObRawExprUtils::build_const_int_expr(
        expr_factory, ObIntType, child_node->value_, expr));
      CK (OB_NOT_NULL(expr));
      OZ (obj_access_idents.at(obj_access_idents.count()-1)
      .params_.push_back(std::make_pair(static_cast<ObRawExpr*>(expr), 0)));
    }
  }
  return ret;
}

// resolve obj access idents without T_SP_CPARAM_LIST
int ObPLResolver::resolve_obj_access_idents(const ParseNode &node,
                                            ObRawExprFactory &expr_factory,
                                            ObIArray<ObObjAccessIdent> &obj_access_idents,
                                            ObSQLSessionInfo &session_info)
{
  int ret = OB_SUCCESS;
  CK (OB_LIKELY(node.type_ == T_SP_OBJ_ACCESS_REF),
      OB_LIKELY(node.num_child_ == 2),
      OB_NOT_NULL(node.children_[0]),
      OB_LIKELY(T_SP_ACCESS_NAME == node.children_[0]->type_
                || T_IDENT == node.children_[0]->type_
                || T_SP_CPARAM_LIST == node.children_[0]->type_),
      OB_LIKELY((0 == obj_access_idents.count() && T_SP_ACCESS_NAME == node.children_[0]->type_)
                || obj_access_idents.count() != 0));
#define PROCESS_IDENT_NODE(node) \
  if (OB_SUCC(ret) && T_IDENT == node->type_) { \
    ObString ident_name(static_cast<int32_t>(node->str_len_), \
                        node->str_value_); \
    ObObjAccessIdent access_ident(ident_name); \
    OZ(obj_access_idents.push_back(access_ident)); \
  }
#define PROCESS_CPARAM_LIST_NODE(node) \
  if (OB_SUCC(ret) && T_SP_CPARAM_LIST == node->type_) { \
    OZ (resolve_cparam_list_simple(*node, expr_factory, obj_access_idents)); \
  }
#define PROCESS_ACCESS_NAME(node) \
  if (OB_SUCC(ret) && T_SP_ACCESS_NAME == node->type_) { \
    for (int i = 0; OB_SUCC(ret) && i < node->num_child_; i++) { \
      if (OB_NOT_NULL(node->children_[i])) { \
        ObString ident_name(static_cast<int32_t>(node->children_[i]->str_len_), \
                            node->children_[i]->str_value_); \
        ObObjAccessIdent access_ident(ident_name); \
        OZ (obj_access_idents.push_back(access_ident)); \
      } \
    } \
  }
  PROCESS_IDENT_NODE(node.children_[0]);
  PROCESS_CPARAM_LIST_NODE(node.children_[0]);
  PROCESS_ACCESS_NAME(node.children_[0]);

  if (OB_SUCC(ret) && OB_NOT_NULL(node.children_[1])) {
    CK (T_IDENT == node.children_[1]->type_
        || T_SP_CPARAM_LIST == node.children_[1]->type_
        || T_SP_OBJ_ACCESS_REF == node.children_[1]->type_);
    PROCESS_IDENT_NODE(node.children_[1]);
    PROCESS_CPARAM_LIST_NODE(node.children_[1]);

    if (OB_SUCC(ret) && T_SP_OBJ_ACCESS_REF == node.children_[1]->type_) {
      OZ (resolve_obj_access_idents(*(node.children_[1]), expr_factory, obj_access_idents, session_info));
    }
  }
  if (OB_FAIL(ret)) {
    record_error_line(const_cast<const ObStmtNodeTree*>(&node), session_info);
  }
#undef PROCESS_IDENT_NODE
#undef PROCESS_CPARAM_LIST_NODE
#undef PROCESS_ACCESS_NAME
  return ret;
}

int ObPLResolver::resolve_obj_access_idents(const ParseNode &node,
                                            ObIArray<ObObjAccessIdent> &obj_access_idents,
                                            ObPLCompileUnitAST &func)
{
  int ret = OB_SUCCESS;
  CK (OB_LIKELY(T_SP_OBJ_ACCESS_REF == node.type_));
  CK (OB_LIKELY(2 == node.num_child_));
  if (OB_SUCC(ret) && OB_ISNULL(node.children_[0])) {
    ret = OB_ERR_OUT_OF_SCOPE;
    LOG_WARN("PLS-00225: subprogram or cursor reference is out of scope", K(ret));
  }
  if (OB_SUCC(ret)) {
    if (0 == obj_access_idents.count()
        && T_SP_ACCESS_NAME != node.children_[0]->type_
        && T_QUESTIONMARK != node.children_[0]->type_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("node type invalid", K(node.children_[0]->type_), K(ret));
    } else if (T_IDENT == node.children_[0]->type_) {
      ObString ident_name(static_cast<int32_t>(node.children_[0]->str_len_), node.children_[0]->str_value_);
      ObObjAccessIdent access_ident(ident_name);
      if (OB_FAIL(obj_access_idents.push_back(access_ident))) {
        LOG_WARN("push back access ident failed", K(ret), K(access_ident));
      }
    } else if (T_QUESTIONMARK == node.children_[0]->type_) {
      if (obj_access_idents.count() > 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("quesitonmark in obj access ref muse be top node.", K(ret), K(obj_access_idents));
      } else {
        ObObjAccessIdent ident(ObString(""), node.children_[0]->value_);
        OX (ident.set_pl_var());
        OZ (obj_access_idents.push_back(ident));
      }
    } else if (T_SP_ACCESS_NAME == node.children_[0]->type_) {
      for (int i = 0; OB_SUCC(ret) && i < node.children_[0]->num_child_; i++) {
        if (OB_NOT_NULL(node.children_[0]->children_[i])) {
          ObString ident_name(static_cast<int32_t>(node.children_[0]->children_[i]->str_len_), node.children_[0]->children_[i]->str_value_);
          ObObjAccessIdent access_ident(ident_name);
          if (OB_FAIL(obj_access_idents.push_back(access_ident))) {
            LOG_WARN("push back access ident failed", K(ret));
          }
        }
      }
    } else if (T_SP_CPARAM_LIST == node.children_[0]->type_) {
      ParseNode *params_node = node.children_[0];
      ObObjAccessIdent param_access;
      bool current_params = obj_access_idents.count() > 0
              && !obj_access_idents.at(obj_access_idents.count()-1).access_name_.empty()
              && obj_access_idents.at(obj_access_idents.count()-1).params_.empty();
      obj_access_idents.at(obj_access_idents.count() -1).has_brackets_ = true;
      for (int64_t param_idx = 0; OB_SUCC(ret) && param_idx < params_node->num_child_; ++param_idx) {
        ObRawExpr *expr = NULL;
        if (OB_FAIL(resolve_expr(params_node->children_[param_idx], func, expr,
                            combine_line_and_col(params_node->children_[param_idx]->stmt_loc_)))) {
          LOG_WARN("failed to resolve expr", K(ret));
        } else {
          std::pair<ObRawExpr*, int64_t> param(expr, 0);
          if (current_params) {
            if (OB_FAIL(obj_access_idents.at(obj_access_idents.count()-1).params_.push_back(param))) {
              LOG_WARN("push back expr failed", K(ret));
            }
          } else {
            if (OB_FAIL(param_access.params_.push_back(param))) {
              LOG_WARN("push back access ident failed", K(ret));
            }
          }
        }
      }
      if (OB_SUCC(ret) && !param_access.params_.empty()) {
        if (OB_FAIL(obj_access_idents.push_back(param_access))) {
          LOG_WARN("push back access ident failed", K(ret));
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Invalid node type", K(node.children_[0]->type_), K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_NOT_NULL(node.children_[1])) {
      if (T_IDENT == node.children_[1]->type_) {
        ObString ident_name(static_cast<int32_t>(node.children_[1]->str_len_), node.children_[1]->str_value_);
        ObObjAccessIdent access_ident(ident_name);
        if (OB_FAIL(obj_access_idents.push_back(access_ident))) {
          LOG_WARN("push back access ident failed", K(ret));
        }
      } else if (T_SP_CPARAM_LIST == node.children_[1]->type_) {
        ParseNode *params_node = node.children_[1];
        ObObjAccessIdent param_access;
        ObIArray<std::pair<ObRawExpr*, int64_t>> &current_params
            = obj_access_idents.at(obj_access_idents.count()-1).params_;
        int64_t current_level = current_params.empty()
            ? 0 : current_params.at(current_params.count() - 1).second + 1;
        obj_access_idents.at(obj_access_idents.count() - 1).has_brackets_ = true;
        for (int64_t param_idx = 0; OB_SUCC(ret) && param_idx < params_node->num_child_; ++param_idx) {
          ObRawExpr *expr = NULL;
          if (OB_FAIL(resolve_expr(params_node->children_[param_idx], func, expr,
                            combine_line_and_col(params_node->children_[param_idx]->stmt_loc_)))) {
            LOG_WARN("failed to resolve expr", K(ret));
          } else {
            std::pair<ObRawExpr*, int64_t> param(expr, current_level);
            if (T_IDENT == node.children_[0]->type_
                || T_SP_ACCESS_NAME == node.children_[0]->type_
                || T_SP_CPARAM_LIST == node.children_[0]->type_
                || T_QUESTIONMARK == node.children_[0]->type_) {
              if (OB_FAIL(obj_access_idents.at(obj_access_idents.count()-1).params_.push_back(param))) {
                LOG_WARN("push back access ident failed", K(ret));
              }
            } else {
              if (OB_FAIL(param_access.params_.push_back(param))) {
                LOG_WARN("push back access ident failed", K(ret));
              }
            }
          }
        }
        if (OB_SUCC(ret) && !param_access.params_.empty()) {
          if (OB_FAIL(obj_access_idents.push_back(param_access))) {
            LOG_WARN("push back access ident failed", K(ret));
          }
        }
      } else if (T_SP_OBJ_ACCESS_REF == node.children_[1]->type_) {
        if (OB_FAIL(resolve_obj_access_idents(*(node.children_[1]), obj_access_idents, func))) {
          LOG_WARN("resolve obj access idents failed", K(ret));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Invalid node type", K(node.children_[1]->type_), K(ret));
      }
    }
  }
  return ret;
}

int ObPLResolver::resolve_dblink_idents(const ParseNode &node,
                                        common::ObIArray<ObObjAccessIdent> &obj_access_idents,
                                        ObPLCompileUnitAST &func,
                                        common::ObIArray<ObObjAccessIdx> &access_idexs)
{
  int ret = OB_SUCCESS;
  UNUSED(func);
  ObString dblink_name;
  CK (OB_LIKELY(3 == node.num_child_));
  CK (OB_LIKELY(OB_NOT_NULL(node.children_[0])) && OB_LIKELY(OB_NOT_NULL(node.children_[1])));
  CK (OB_LIKELY(T_SP_ACCESS_NAME == node.children_[0]->type_));
  CK (OB_LIKELY(T_DBLINK_NAME == node.children_[1]->type_));
  for (int64_t i = 0; OB_SUCC(ret) && i < node.children_[0]->num_child_; i++) {
    if (OB_NOT_NULL(node.children_[0]->children_[i])) {
        ObString ident_name(static_cast<int32_t>(node.children_[0]->children_[i]->str_len_),
                            node.children_[0]->children_[i]->str_value_);
        ObObjAccessIdent access_ident(ident_name);
        if (OB_FAIL(obj_access_idents.push_back(access_ident))) {
          LOG_WARN("push back access ident failed", K(ret));
      }
    }
  }
  CK (obj_access_idents.count() >= 1 && obj_access_idents.count() <= 3);
  CK (1 == node.children_[1]->num_child_ && OB_NOT_NULL(node.children_[1]->children_[0]));
  OX (dblink_name.assign_ptr(node.children_[1]->children_[0]->str_value_,
                             static_cast<int32_t>(node.children_[1]->children_[0]->str_len_)));
  if (OB_SUCC(ret) && OB_NOT_NULL(node.children_[2])) {
    ParseNode *params_node = node.children_[2];
    ObObjAccessIdent param_access;
    for (int64_t param_idx = 0; OB_SUCC(ret) && param_idx < params_node->num_child_; ++param_idx) {
      ObRawExpr *expr = NULL;
      if (OB_FAIL(resolve_expr(params_node->children_[param_idx], func, expr,
                               combine_line_and_col(params_node->children_[param_idx]->stmt_loc_)))) {
        LOG_WARN("failed to resolve expr", K(ret));
      } else {
        std::pair<ObRawExpr*, int64_t> param(expr, 0);
        if (T_IDENT == node.children_[0]->type_
            || T_SP_ACCESS_NAME == node.children_[0]->type_) {
          if (OB_FAIL(obj_access_idents.at(obj_access_idents.count()-1).params_.push_back(param))) {
            LOG_WARN("push back access ident failed", K(ret));
          }
        } else {
          if (OB_FAIL(param_access.params_.push_back(param))) {
            LOG_WARN("push back access ident failed", K(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret) && !param_access.params_.empty()) {
      if (OB_FAIL(obj_access_idents.push_back(param_access))) {
        LOG_WARN("push back access ident failed", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    uint64_t dblink_id;
    uint64_t cnt = obj_access_idents.count();
    uint64_t tenant_id = resolve_ctx_.session_info_.get_effective_tenant_id();
    common::ObArray<ObRawExpr *> expr_params;
    const ObIRoutineInfo *routine_info = NULL;
    ObString empty_str("");
    ObString &routine_name = obj_access_idents.at(cnt - 1).access_name_;
    ObString &pkg_name = (cnt >= 2 ? obj_access_idents.at(cnt - 2).access_name_ : empty_str);
    ObString &db_name = (cnt == 3 ? obj_access_idents.at(0).access_name_ : empty_str);
    OZ (obj_access_idents.at(cnt - 1).extract_params(0, expr_params));
    // OZ (resolve_ctx_.schema_guard_.get_dblink_id(tenant_id, dblink_name, dblink_id));
    // OV (OB_INVALID_ID != dblink_id, OB_DBLINK_NOT_EXIST_TO_ACCESS, dblink_id);
    // OZ (resolve_ctx_.schema_guard_.get_dblink_user(tenant_id, dblink_name, db_name, resolve_ctx_.allocator_));
    OZ (resolve_dblink_routine(resolve_ctx_, dblink_name, db_name, pkg_name,
                                routine_name, expr_params, routine_info));
    if (OB_SUCC(ret) && OB_NOT_NULL(routine_info)) {
      ObPLDataType proc_pl_data_type;
      ObObjAccessIdx::AccessType proc_type = ObObjAccessIdx::AccessType::IS_EXTERNAL_PROC;
      int64_t access_value = reinterpret_cast<int64_t>(routine_info);
      ObObjAccessIdx access_idx;
      new (&access_idx)ObObjAccessIdx(proc_pl_data_type, proc_type, routine_name, proc_pl_data_type, access_value);
      OZ (access_idexs.push_back(access_idx));
    }
  }

  return ret;
}

int ObPLResolver::resolve_dblink_routine(ObPLResolveCtx &resolve_ctx,
                                         const ObString &dblink_name,
                                         const ObString &db_name,
                                         const ObString &pkg_name,
                                         const ObString &routine_name,
                                         const common::ObIArray<sql::ObRawExpr *> &expr_params,
                                         const ObIRoutineInfo *&routine_info)
{
  int ret = OB_SUCCESS;
  ObSEArray<const ObIRoutineInfo*, 8> routine_infos;
  const share::schema::ObIRoutineInfo *i_routine_info = NULL;
  OZ (resolve_ctx.package_guard_.dblink_guard_.get_routine_infos_with_synonym(resolve_ctx.session_info_,
        resolve_ctx.schema_guard_, dblink_name, db_name, pkg_name, routine_name, routine_infos));

  if (OB_SUCC(ret) && routine_infos.count() > 0) {
    OZ (ObResolverUtils::pick_routine(resolve_ctx, expr_params, routine_infos, i_routine_info));
    // OX (routine_info = static_cast<const share::schema::ObRoutineInfo*>(i_routine_info));
    LOG_DEBUG("debug for pick routine info", K(ret), K(db_name), K(pkg_name),
              K(routine_name), K(expr_params), KPC(routine_info));
    if (OB_SUCC(ret)) {
      routine_info = i_routine_info;
    }
  }
  if (OB_SUCC(ret) && OB_ISNULL(routine_info)) {
    ret = OB_ERR_NOT_VALID_ROUTINE_NAME;
    LOG_WARN("not a valid function or procedure name", K(ret), K(dblink_name),
              K(db_name), K(pkg_name), K(routine_name));
  }
  return ret;
}

int ObPLResolver::resolve_dblink_routine_with_synonym(ObPLResolveCtx &resolve_ctx,
                                                      const uint64_t pkg_syn_id,
                                                      const ObString &routine_name,
                                                      const common::ObIArray<sql::ObRawExpr *> &expr_params,
                                                      const ObIRoutineInfo *&routine_info)
{
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_ORACLE_PL
  ObString dblink_name;
  ObString db_name;
  ObString pkg_name;
  OZ (ObPLDblinkUtil::separate_name_from_synonym(resolve_ctx.schema_guard_, resolve_ctx.allocator_,
                                                 resolve_ctx.session_info_.get_effective_tenant_id(),
                                                 pkg_syn_id, dblink_name, db_name, pkg_name));
  OZ (ObPLResolver::resolve_dblink_routine(resolve_ctx, dblink_name, db_name, pkg_name,
                                           routine_name, expr_params, routine_info),
                                           dblink_name, db_name, pkg_name);
#endif
  return ret;
}

int ObPLResolver::resolve_dblink_type_with_synonym(const uint64_t pkg_syn_id,
                                                   const ObString &type_name,
                                                   ObPLCompileUnitAST &func,
                                                   ObPLDataType &pl_type)
{
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_ORACLE_PL
  ObString dblink_name;
  ObString db_name;
  ObString pkg_name;
  OZ (ObPLDblinkUtil::separate_name_from_synonym(resolve_ctx_.schema_guard_, resolve_ctx_.allocator_,
                                                 resolve_ctx_.session_info_.get_effective_tenant_id(),
                                                 pkg_syn_id, dblink_name, db_name, pkg_name));
  OZ (resolve_dblink_type(dblink_name, db_name, pkg_name, type_name, func, pl_type));
#endif
  return ret;
}

int ObPLResolver::resolve_dblink_type(const ObString &dblink_name,
                                      const ObString &db_name,
                                      const ObString &pkg_name,
                                      const ObString &udt_name,
                                      ObPLCompileUnitAST &func,
                                      ObPLDataType &pl_type)
{
  int ret = OB_SUCCESS;
  const ObUserDefinedType *udt = NULL;
  OZ (resolve_ctx_.package_guard_.dblink_guard_.get_dblink_type_with_synonym(resolve_ctx_.session_info_,
        resolve_ctx_.schema_guard_, dblink_name, db_name, pkg_name, udt_name, udt));
  OZ (current_block_->get_namespace().get_type_table()->add_external_type(udt));
  OX (func.set_can_cached(false));
  if (OB_SUCC(ret) && udt->is_collection_type()) {
    const ObCollectionType *coll_type = static_cast<const ObCollectionType *>(udt);
    if (OB_NOT_NULL(coll_type) && coll_type->get_element_type().is_record_type()) {
      const ObUserDefinedType *udt2 = NULL;
      uint64_t user_type_id = coll_type->get_element_type().get_user_type_id();
      OZ (resolve_ctx_.package_guard_.dblink_guard_.get_dblink_type_by_id(
                                        extract_package_id(user_type_id), user_type_id, udt2));
      OZ (current_block_->get_namespace().get_type_table()->add_external_type(udt2));
    }
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(udt)) {
    pl_type.set_user_type_id(udt->get_type(), udt->get_user_type_id());
    pl_type.set_type_from(PL_TYPE_PACKAGE);
    pl_type.set_type_from_orgin(PL_TYPE_PACKAGE);
  }
  return ret;
}


int ObPLResolver::resolve_qualified_identifier(ObQualifiedName &q_name,
                                               ObIArray<ObQualifiedName> &columns,
                                               ObIArray<ObRawExpr*> &real_exprs,
                                               ObPLCompileUnitAST &unit_ast,
                                               ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  OZ (resolve_qualified_name(q_name, columns, real_exprs, unit_ast, expr));
  //因为obj access的参数拉平处理，a(b,c)在columns会被存储为b,c,a，所以解释完一个ObQualifiedName，
  //都要把他前面的ObQualifiedName拿过来尝试替换一遍参数
  for (int64_t i = 0; OB_SUCC(ret) && i < real_exprs.count(); ++i) {
    OZ (ObRawExprUtils::replace_ref_column(expr, columns.at(i).ref_expr_, real_exprs.at(i)));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < columns.count(); ++i) {
    OZ (columns.at(i).replace_access_ident_params(q_name.ref_expr_, expr));
  }
  CK (OB_NOT_NULL(expr));
  OZ (formalize_expr(*expr));
  return ret;
}

int ObPLResolver::resolve_sqlcode_or_sqlerrm(ObQualifiedName &q_name,
                                             ObPLCompileUnitAST &unit_ast,
                                             ObRawExpr *&expr)
{
  // 走到这里说明一定是无参的SQLCODE或者SQLERRM，带参数的SQLERRM已经在is_sysfunc处理过了
  int ret = OB_SUCCESS;
  UNUSED(unit_ast);
  if (1 == q_name.access_idents_.count()
      && q_name.access_idents_.at(0).get_type() == AccessNameType::UNKNOWN
      && (0 == q_name.access_idents_.at(0).access_name_.case_compare("SQLCODE")
          || 0 == q_name.access_idents_.at(0).access_name_.case_compare("SQLERRM"))) {
    ObPLSQLCodeSQLErrmRawExpr *c_expr = NULL;
    OZ (expr_factory_.create_raw_expr(T_FUN_PL_SQLCODE_SQLERRM, c_expr));
    CK (OB_NOT_NULL(c_expr));
    OX (c_expr->set_is_sqlcode(0 == q_name.access_idents_.at(0).access_name_.case_compare("SQLCODE")));
    OZ (c_expr->extract_info());
    OX (expr = c_expr);
  } else {
    ret = OB_ERR_SP_UNDECLARED_VAR;
    LOG_WARN("failed to resolve sqlcode or sqlerrm", K(ret), K(q_name));
  }
  return ret;
}

int ObPLResolver::resolve_construct(const ObQualifiedName &q_name,
                                    const ObUDFInfo &udf_info,
                                    const ObUserDefinedType &user_type,
                                    ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  if (user_type.is_nested_table_type() || user_type.is_varray_type()) {
    OZ (resolve_collection_construct(q_name, udf_info, &user_type, expr));
  } else if (user_type.is_object_type() && user_type.is_udt_type()) {
    OZ (resolve_object_construct(q_name, udf_info, &user_type, expr));
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("only allow collection construct and user define record construct", K(ret), K(user_type));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "Constructs other than collection constructs and user-defined record constructs");
  }
  return ret;
}

int ObPLResolver::resolve_construct(const ObQualifiedName &q_name,
                                   const ObUDFInfo &udf_info,
                                   ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  const ObUserDefinedType *user_type = NULL;
  CK (OB_NOT_NULL(current_block_));
  ObString package_name
    = udf_info.is_udf_udt_cons()
      ? ObString("")
        : udf_info.udf_package_ != udf_info.udf_name_ ? udf_info.udf_package_ : ObString(""); 
  OZ (current_block_->get_namespace().get_pl_data_type_by_name(
                      resolve_ctx_, udf_info.udf_database_, package_name,
                      udf_info.udf_name_, user_type));
  CK (OB_NOT_NULL(user_type));
  // reset the error code, we have to try udf.
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to get type", K(ret), K(package_name), K(udf_info), K(q_name));
    ret = OB_ERR_SP_UNDECLARED_TYPE;
  }
  if (OB_SUCC(ret)) {
    if (user_type->is_nested_table_type()
        || user_type->is_varray_type()) {
      OZ (resolve_collection_construct(q_name, udf_info, user_type, expr));
    } else if (user_type->is_object_type() && user_type->is_udt_type()) {
      OZ (resolve_object_construct(q_name, udf_info, user_type, expr));
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("only allow collection construct and user define record construct", K(ret), KPC(user_type));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "Constructs other than collection constructs and user-defined record constructs");
    }
  }
  return ret;
}

int ObPLResolver::resolve_object_construct(const sql::ObQualifiedName &q_name,
                                           const sql::ObUDFInfo &udf_info,
                                           const ObUserDefinedType *user_type,
                                           ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  uint64_t type_id = OB_INVALID_ID;
  OX (type_id = user_type->get_user_type_id());
  if (OB_SUCC(ret)) {
    ObUDFInfo &uinfo = const_cast<ObUDFInfo &>(udf_info);
    if (uinfo.udf_package_.empty()) {
      // object type name should same as constructor name
      uinfo.udf_package_ = uinfo.udf_name_;
    } else if (0 != uinfo.udf_package_.case_compare(uinfo.udf_name_)) {
      if (0 == uinfo.udf_package_.case_compare(OB_ORA_SYS_SCHEMA_NAME)
          && lib::is_oracle_mode()) { // oracle sys udt
        // do nothing
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("type name is not same as constructor name", K(uinfo), K(ret));
      }
    } else {
      // do nothing
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(uinfo.ref_expr_) && uinfo.ref_expr_->get_udf_id() == OB_INVALID_ID) {
      SMART_VAR(ObPLFunctionAST, dummy_ast, resolve_ctx_.allocator_) {
        ObSEArray<ObObjAccessIdx, 1> access_idxs;
        OZ (resolve_udf_info(uinfo, access_idxs, dummy_ast));
      }
    }
    //
    // check is use default constructor
    // for example: object(a number,  constructor object(a varchar));
    // begin v object := object(4); resolve_udf will pick the user defined cons,
    // actually, here we need to use the default construtor, that is: reolsve record construct
    // on the other side: object(a number, constructor(a number) constructor object(a varchar));
    // resolve_udf will pick the right one, we dont need to resolve record_construct.

    bool use_buildin_default_constructor = false;
    if (OB_SUCC(ret)
        && !uinfo.is_udt_overload_default_cons()
        && !user_type->is_opaque_type()) { // opaque type has no member, do not check
      const ObRecordType *object_type = NULL;
      CK (OB_NOT_NULL(object_type = dynamic_cast<const ObRecordType *>(user_type)));

      // must have same attribute and param, exclude self param
      if (udf_info.ref_expr_->get_param_exprs().count() - 1 == object_type->get_member_count()) {
        use_buildin_default_constructor = true;

        for (int64_t i = 1; OB_SUCC(ret) && i < udf_info.ref_expr_->get_param_exprs().count(); ++i) {
          const ObRawExpr *param_expr = udf_info.ref_expr_->get_param_exprs().at(i);
          if (OB_ISNULL(param_expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("null param expr", K(ret));
          } else {
            const ObExprResType &param_res_type = param_expr->get_result_type();
            const ObPLDataType *pl_type = object_type->get_record_member_type(i - 1);
            if (OB_NOT_NULL(pl_type)
                && OB_NOT_NULL(pl_type->get_meta_type())
                && (param_res_type.get_type() == pl_type->get_meta_type()->get_type())) {
                // do nothing
            } else {
              use_buildin_default_constructor = false;
              break;
            }
          }
        }
      }
    }
    OX (expr = udf_info.ref_expr_);
    // if cant find user define construtor, try default construct
    if ((OB_SUCCESS == ret && use_buildin_default_constructor)
     || OB_ERR_SP_WRONG_ARG_NUM == ret
     || OB_ERR_FUNCTION_UNKNOWN == ret
     || OB_ERR_SP_UNDECLARED_VAR == ret
     || OB_ERR_INVALID_TYPE_FOR_OP == ret
     || OB_ERR_PACKAGE_DOSE_NOT_EXIST == ret
     || OB_ERR_SP_DOES_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      pl_reset_warning_buffer();
      OZ (resolve_record_construct(q_name, udf_info, user_type, expr));
    }
  }
  return ret;
}

int ObPLResolver::resolve_record_construct(const ObQualifiedName &q_name,
                                           const ObUDFInfo &udf_info,
                                           const ObUserDefinedType *user_type,
                                           ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  const ObRecordType *object_type = NULL;
  ObObjectConstructRawExpr *object_expr = NULL;
  ObExprResType res_type;
  int64_t rowsize = 0;
  const ObUDTTypeInfo *udt_info = NULL;
  uint64_t tenant_id = OB_INVALID_ID;
  bool is_udt_type = false;
  CK (OB_NOT_NULL(user_type));
  OX (is_udt_type = user_type->is_udt_type());
  CK (OB_NOT_NULL(udf_info.ref_expr_));
  CK (OB_NOT_NULL(object_type = static_cast<const ObRecordType *>(user_type)));
  if (OB_SUCC(ret) && udf_info.param_names_.count() > 0) { // 构造函数暂时不允许使用=>赋值
    ret = OB_ERR_CALL_WRONG_ARG;
    LOG_WARN("PLS-00306: wrong number or types of arguments in call procedure", K(ret));
  }
  if (OB_SUCC(ret)) {
    int64_t param_cnt = udf_info.ref_expr_->get_param_exprs().count();
    int64_t member_cnt = object_type->get_member_count();
    bool is_opaque_cons_and_no_self_param
      = object_type->is_opaque_type() && (param_cnt - 2) == member_cnt && udf_info.is_udf_udt_cons();

    if (OB_SUCC(ret)
        && ((!udf_info.is_udf_udt_cons() && param_cnt != member_cnt)
              || (udf_info.is_udf_udt_cons() && param_cnt - 1 != member_cnt))
        && !is_opaque_cons_and_no_self_param) {
      ret = OB_ERR_CALL_WRONG_ARG;
      LOG_WARN("PLS-00306: wrong number or types of arguments in call",
              K(ret),
              K(q_name),
              K(udf_info.ref_expr_->get_param_exprs().count()),
              K(object_type->get_member_count()));
    }
  }
  OZ (expr_factory_.create_raw_expr(T_FUN_PL_OBJECT_CONSTRUCT, object_expr));
  CK (OB_NOT_NULL(object_expr));
  OZ (user_type->get_size(current_block_->get_namespace(), pl::PL_TYPE_ROW_SIZE, rowsize));
  OX (object_expr->set_rowsize(rowsize));
  OX (res_type.set_type(ObExtendType));
  OX (res_type.set_extend_type(PL_RECORD_TYPE));
  OX (res_type.set_udt_id(user_type->get_user_type_id()));
  OX (object_expr->set_udt_id(user_type->get_user_type_id()));
  OX (object_expr->set_result_type(res_type));
  if (is_udt_type) {
    OX (tenant_id = get_tenant_id_by_object_id(user_type->get_user_type_id()));
    OZ (resolve_ctx_.schema_guard_.get_udt_info(
        tenant_id, user_type->get_user_type_id(), udt_info));
    CK (OB_NOT_NULL(udt_info));
    OX (object_expr->set_database_id(udt_info->get_database_id()));
    OX (object_expr->set_coll_schema_version(udt_info->get_schema_version()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < object_type->get_member_count(); ++i) {
    const ObPLDataType *pl_type = object_type->get_record_member_type(i);
    ObExprResType elem_type;
    const ObDataType *data_type = NULL;
    CK (OB_NOT_NULL(pl_type));
    if (OB_FAIL(ret)) {
    } else if (!pl_type->is_obj_type()) {
      OX (elem_type.set_ext());
      OX (elem_type.set_extend_type(pl_type->get_type()));
      OX (elem_type.set_udt_id(pl_type->get_user_type_id()));
    } else {
      CK (OB_NOT_NULL(data_type = pl_type->get_data_type()));
      OX (elem_type.set_meta(data_type->get_meta_type()));
      OX (elem_type.set_accuracy(data_type->get_accuracy()));
    }
    OX (object_expr->add_elem_type(elem_type));
  }
  OZ (object_expr->set_access_names(q_name.access_idents_));
  OX (object_expr->set_func_name(object_type->get_name()));
  int64_t i = udf_info.is_udf_udt_cons() && udf_info.is_contain_self_param_ ? 1 : 0; // ignore the self param
  for (; OB_SUCC(ret) && i < udf_info.ref_expr_->get_param_exprs().count(); ++i) {
    OZ (object_expr->add_param_expr(udf_info.ref_expr_->get_param_exprs().at(i)));
  }
  OX (expr = object_expr);
  return ret;
}

int ObPLResolver::resolve_collection_construct(const ObQualifiedName &q_name,
                                               const ObUDFInfo &udf_info,
                                               const ObUserDefinedType *user_type,
                                               ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_ORACLE_PL
  const ObCollectionType *coll_type = NULL;
  ObCollectionConstructRawExpr *coll_expr = NULL;
  ObExprResType res_type;
  const ObUDTTypeInfo *udt_info = NULL;
  uint64_t tenant_id = OB_INVALID_ID;
  bool is_udt_type = false;
  CK (OB_NOT_NULL(user_type));
  OX (is_udt_type = user_type->is_udt_type());
  OZ (expr_factory_.create_raw_expr(T_FUN_PL_COLLECTION_CONSTRUCT, coll_expr));
  CK (OB_NOT_NULL(coll_expr));
  if (OB_SUCC(ret) && udf_info.param_names_.count() > 0) { // 构造函数不允许使用=>赋值
    ret = OB_ERR_CALL_WRONG_ARG;
    LOG_WARN("PLS-00306: wrong number or types of arguments in call to", K(ret));
  }
  CK (OB_NOT_NULL(coll_type = static_cast<const ObCollectionType *>(user_type)));
  OX (coll_expr->set_type(user_type->get_type()));
  OX (coll_expr->set_capacity(user_type->is_varray_type() ? static_cast<const ObVArrayType*>(user_type)->get_capacity() : OB_INVALID_SIZE));
  OX (coll_expr->set_udt_id(user_type->get_user_type_id()));
  OX (coll_expr->set_elem_type(coll_type->get_element_type()));
  OZ (coll_expr->set_access_names(q_name.access_idents_));
  OX (coll_expr->set_func_name(coll_type->get_name()));
  if (is_udt_type) {
    OX (tenant_id = get_tenant_id_by_object_id(user_type->get_user_type_id()));
    OZ (resolve_ctx_.schema_guard_.get_udt_info(
        tenant_id, user_type->get_user_type_id(), udt_info));
    CK (OB_NOT_NULL(udt_info));
    OX (coll_expr->set_database_id(udt_info->get_database_id()));
    OX (coll_expr->set_coll_schema_version(udt_info->get_schema_version()));
  }
  OX (res_type.set_type(ObExtendType));
  OX (res_type.set_extend_type(user_type->get_type()));
  OX (res_type.set_udt_id(user_type->get_user_type_id()));
  OX (coll_expr->set_result_type(res_type));
  CK (OB_NOT_NULL(udf_info.ref_expr_));
  for (int64_t i = 0; OB_SUCC(ret) && i < udf_info.ref_expr_->get_param_exprs().count(); ++i) {
    if (coll_type->get_element_type().is_obj_type()) {
      ObRawExpr *child = udf_info.ref_expr_->get_param_exprs().at(i);
      const ObDataType *data_type = coll_type->get_element_type().get_data_type();
      CK (OB_NOT_NULL(data_type));
      OZ (ObRawExprUtils::build_column_conv_expr(&resolve_ctx_.session_info_,
                                                 expr_factory_,
                                                 data_type->get_obj_type(),
                                                 data_type->get_collation_type(),
                                                 data_type->get_accuracy_value(),
                                                 true,
                                                 NULL,
                                                 NULL,
                                                 child,
                                                 true));
      OZ (coll_expr->add_param_expr(child));
    } else {
      OZ (coll_expr->add_param_expr(udf_info.ref_expr_->get_param_exprs().at(i)));
    }
  }
  OX (expr = coll_expr);
#else
  UNUSEDx(q_name, udf_info, user_type, expr);
#endif
  return ret;
}

int ObPLResolver::resolve_udf_without_brackets(
  ObQualifiedName &q_name, ObPLCompileUnitAST &unit_ast, ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObUDFRawExpr *udf_expr = NULL;
  ObObjAccessIdent &access_ident = q_name.access_idents_.at(q_name.access_idents_.count() - 1);
  ObUDFInfo &udf_info = access_ident.udf_info_;
  ObSEArray<ObObjAccessIdx, 4> access_idxs;
  OX (access_ident.set_pl_udf());
  OZ (expr_factory_.create_raw_expr(T_FUN_UDF, udf_expr), K(q_name));
  CK (OB_NOT_NULL(udf_expr));
  OX (udf_expr->set_func_name(access_ident.access_name_));
  OX (udf_info.ref_expr_ = udf_expr);
  OX (udf_info.udf_name_ = access_ident.access_name_);
  OZ (resolve_name(q_name, current_block_->get_namespace(), expr_factory_, &resolve_ctx_.session_info_, access_idxs, unit_ast),
    K(access_idxs), K(q_name));
  OV (access_idxs.at(access_idxs.count() - 1).is_udf_type());
  OX (expr = access_idxs.at(access_idxs.count() - 1).get_sysfunc_);
  CK (OB_NOT_NULL(expr));
  if (OB_FAIL(ret) && ret != OB_ERR_INSUFFICIENT_PRIVILEGE) {
    ret = OB_ERR_SP_UNDECLARED_VAR;
  }
  return ret;
}

int ObPLResolver::replace_udf_param_expr(
  ObQualifiedName &q_name, ObIArray<ObQualifiedName> &columns, ObIArray<ObRawExpr*> &real_exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < q_name.access_idents_.count(); ++i) {
    ObObjAccessIdent &access_ident = q_name.access_idents_.at(i);
    if (access_ident.is_pl_udf()) {
      OZ (replace_udf_param_expr(access_ident, columns, real_exprs));
    } else if (access_ident.is_sys_func()) {
      // cases like : xmlparse(document expr).getclobval()
      ObRawExpr *expr = static_cast<ObRawExpr *>(access_ident.sys_func_expr_);
      for (int64_t i = 0; OB_SUCC(ret) && i < real_exprs.count(); ++i) {
        if (OB_FAIL(ObRawExprUtils::replace_ref_column(expr, columns.at(i).ref_expr_, real_exprs.at(i)))) {
          LOG_WARN("replace column ref expr failed", K(ret));
        }
      }
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObPLResolver::replace_udf_param_expr(
  ObObjAccessIdent &access_ident, ObIArray<ObQualifiedName> &columns, ObIArray<ObRawExpr*> &real_exprs)
{
  int ret = OB_SUCCESS;
  ObUDFInfo &udf_info = access_ident.udf_info_;
  ObRawExpr *expr = static_cast<ObRawExpr *>(udf_info.ref_expr_);
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("udf expr is null", K(ret));
  } else {
    //如果是UDF，要先把UDF INFO里的参数替换掉，否则重载匹配的时候会失败
    for (int64_t i = 0; OB_SUCC(ret) && i < real_exprs.count(); ++i) {
      OZ (ObRawExprUtils::replace_ref_column(expr, columns.at(i).ref_expr_, real_exprs.at(i)));
      for (int64_t j = 0; OB_SUCC(ret) && j < udf_info.param_exprs_.count(); ++j) {
        CK (OB_NOT_NULL(udf_info.param_exprs_.at(j)));
        if (OB_FAIL(ret)) {
        } else if (udf_info.param_exprs_.at(j) == columns.at(i).ref_expr_) {
          udf_info.param_exprs_.at(j) = real_exprs.at(i);
        } else if (T_SP_CPARAM == udf_info.param_exprs_.at(j)->get_expr_type()) {
          ObCallParamRawExpr* call_expr =
            static_cast<ObCallParamRawExpr*>(udf_info.param_exprs_.at(j));
          CK (OB_NOT_NULL(call_expr));
          if (OB_SUCC(ret) && call_expr->get_expr() == columns.at(i).ref_expr_) {
            call_expr->set_expr(real_exprs.at(i));
          }
        } else if (udf_info.param_exprs_.at(j)->get_param_count() > 0) {
          OZ (recursive_replace_expr(udf_info.param_exprs_.at(j), columns.at(i), real_exprs.at(i)));
        }
      }
    }
  }
  return ret;
}

int ObPLResolver::resolve_qualified_name(ObQualifiedName &q_name,
                                         ObIArray<ObQualifiedName> &columns,
                                         ObIArray<ObRawExpr*> &real_exprs,
                                         ObPLCompileUnitAST &unit_ast,
                                         ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;

  SET_LOG_CHECK_MODE();
  if (!q_name.dblink_name_.empty()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("dblink sequence and udf not support in PL", K(ret), K(q_name));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "dblink sequence and udf in PL");
  }
  OZ (replace_udf_param_expr(q_name, columns, real_exprs));
  if (OB_FAIL(ret)) {
  } else if (q_name.is_sys_func()) {
    if (OB_FAIL(q_name.access_idents_.at(0).sys_func_expr_->check_param_num())) {
      LOG_WARN("sys func param number not match", K(ret));
    } else {
      expr = static_cast<ObRawExpr *>(q_name.access_idents_.at(0).sys_func_expr_);
    }
  } else if (q_name.is_pl_udf() || q_name.is_udf_return_access()) {
    /*
     * in pl context, we should consider four types of function:
     * 1  standalone function, which is defined by create function ddl
     * 2  package public function, which is declared in package spec and defined in package body
     * 3  package private function, which is declared and defined in package body
     * 4  system function, which is define by database system ,such as concat
     *
     * in oracle, function resolve precedence in pl is package private function, then sys function,
     * then package public function or standalone function
     *
     * here we use
     * is_udf to represent package private/public and standalone function
     * is sys_func to represent sys function
     * is_external_udf to represent package public and standalone function
     *
     * record dependency info in resolve_routine() function already!
     */

    //try to resolve as udf
    ObObjAccessIdent &access_ident = q_name.access_idents_.at(q_name.access_idents_.count() - 1);
    ObUDFInfo &udf_info = access_ident.udf_info_;
    expr = static_cast<ObRawExpr *>(udf_info.ref_expr_);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("udf expr is null", K(ret));
    } else {
      if (OB_SUCC(ret)) {
        if (q_name.is_pl_udf()) {
          ObSEArray<ObObjAccessIdx, 4> access_idxs;
          OZ (resolve_name(q_name, current_block_->get_namespace(), expr_factory_, &resolve_ctx_.session_info_, access_idxs, unit_ast));
          if (OB_FAIL(ret)) {
          } else if (access_idxs.at(access_idxs.count() - 1).is_udf_type()) {
            OX (expr = reinterpret_cast<ObRawExpr*>(access_idxs.at(access_idxs.count() - 1).get_sysfunc_));
          } else {
            OZ (make_var_from_access(access_idxs,
                                     expr_factory_,
                                     &(resolve_ctx_.session_info_),
                                     &(resolve_ctx_.schema_guard_),
                                     current_block_->get_namespace(),
                                     expr));
          }
          CK (OB_NOT_NULL(expr));
        } else { // 如果是udf return access，需要当做var解析
          if (OB_FAIL(resolve_var(q_name, unit_ast, expr))) {
            LOG_WARN("failed to resolve var", K(q_name), K(ret));
          }
        }
      }
    }
  } else {
    if (OB_FAIL(resolve_var(q_name, unit_ast, expr))) {
      if (OB_ERR_SP_UNDECLARED_VAR == ret) {
        pl_reset_warning_buffer();
        if (OB_FAIL(resolve_sequence_object(q_name, unit_ast, expr))) {
          LOG_IN_CHECK_MODE("failed to sequence object", K(q_name), K(ret));
        }
      } else {
        LOG_IN_CHECK_MODE("failed to resolve var", K(q_name), K(ret));
      }
    }
    if (OB_ERR_SP_UNDECLARED_VAR == ret) {
      pl_reset_warning_buffer();
      if (OB_FAIL(resolve_sqlcode_or_sqlerrm(q_name, unit_ast, expr))) {
        LOG_IN_CHECK_MODE("failed to resolve sqlcode or sqlerrm",  K(ret), K(q_name));
      }
    }
    if (OB_ERR_SP_UNDECLARED_VAR == ret) {
      pl_reset_warning_buffer();
      if (OB_FAIL(resolve_udf_without_brackets(q_name, unit_ast, expr))) {
        LOG_IN_CHECK_MODE("failed to resolve udf without bracks", K(ret), K(q_name));
      }
    }
    if (OB_ERR_SP_UNDECLARED_VAR == ret) {
      if ((T_SP_IF == item_type_ || T_SP_CASE == item_type_ || T_SP_WHEN == item_type_)
          && lib::is_oracle_mode()) {
        pl_reset_warning_buffer();
        ret = check_undeclared_var_type(q_name);
      }
      if (OB_ERR_SP_UNDECLARED_VAR == ret) {
        LOG_USER_ERROR(OB_ERR_SP_UNDECLARED_VAR, q_name.col_name_.length(), q_name.col_name_.ptr());
      }
    }
  }
  //in static typing engine, we wont do implict cast at stage of execution,
  //for some expr like ObExprIn/ObExprArgCase, if type of left is not match with right,
  //we rewrite "a in (b,c)" to "a in b or a in c"
  //"case a when b xx when c xx" to "case when a == b then xx case when a == c then xx"
  if (OB_SUCC(ret)) {
    bool transformed = false;
    OZ(ObTransformPreProcess::transform_expr(unit_ast.get_expr_factory(),
                                              resolve_ctx_.session_info_, expr,
                                              transformed));
  }

  CANCLE_LOG_CHECK_MODE();
  return ret;
}

int ObPLResolver::resolve_var(ObQualifiedName &q_name, ObPLCompileUnitAST &func, ObRawExpr *&expr,
                              bool for_write)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(current_block_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Symbol table is NULL", K(current_block_), K(expr), K(ret));
  } else if (OB_FAIL(resolve_var(q_name, current_block_->get_namespace(), expr_factory_,
                                 &resolve_ctx_.session_info_, func, expr, for_write))) {
    LOG_IN_CHECK_MODE("failed to resolve var", K(q_name), K(expr), K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObPLResolver::make_self_symbol_expr(ObPLCompileUnitAST &func, ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObObjAccessIdent access_ident;
  ObArray<ObObjAccessIdx> access_idxs;
  new (&access_ident) ObObjAccessIdent(ObString("SELF"), OB_INVALID_INDEX);
  OX (expr = NULL);
  CK (OB_NOT_NULL(current_block_));
  OZ (resolve_access_ident(access_ident,
                           current_block_->get_namespace(),
                           expr_factory_,
                           &resolve_ctx_.session_info_,
                           access_idxs,
                           func));
  OZ (make_var_from_access(access_idxs,
                           expr_factory_,
                           &(resolve_ctx_.session_info_),
                           &(resolve_ctx_.schema_guard_),
                           current_block_->get_namespace(),
                           expr));
  CK (OB_NOT_NULL(expr));
  return ret;
}

#ifdef OB_BUILD_ORACLE_PL
int ObPLResolver::add_udt_self_argument(const ObIRoutineInfo *routine_info,
                                        ObIArray<ObRawExpr*> &expr_params,
                                        ObIArray<ObObjAccessIdx> &access_idxs,
                                        ObUDFInfo &udf_info,
                                        ObPLCompileUnitAST &func)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(routine_info)
      && routine_info->is_udt_routine() // Add Self Argument For UDT Routine.
      && !routine_info->is_udt_static_routine() // Static Routine Has Not Self Argument.
      && expr_params.count() < routine_info->get_param_count()
      && (0 == expr_params.count()
           || (expr_params.count() >= 1
                && !expr_params.at(0)->has_flag(IS_UDT_UDF_SELF_PARAM)
                && !(access_idxs.count() > 0
                      && ObObjAccessIdx::IS_UDT_NS == access_idxs.at(access_idxs.count() - 1).access_type_
                      && expr_params.at(0)->get_result_type().get_expr_udt_id()
                        == access_idxs.at(access_idxs.count() - 1).var_index_)))) {
    ObRawExpr *self_argument = NULL;
    CK (OB_NOT_NULL(udf_info.ref_expr_));
    if (OB_FAIL(ret)) {
    } else if (routine_info->is_udt_cons()) { // UDT Construct Self Argument.
      OZ (ObPLUDTObjectManager::make_constructor_self_expr(
                                    resolve_ctx_,
                                    udf_info.udf_database_,
                                    udf_info.udf_name_,
                                    resolve_ctx_.session_info_.get_effective_tenant_id(),
                                    expr_factory_,
                                    current_block_->get_namespace(),
                                    self_argument));
      CK (OB_NOT_NULL(self_argument));
      OZ (self_argument->formalize(&resolve_ctx_.session_info_));
      OX (udf_info.set_is_udf_udt_cons());
      OZ (func.add_expr(self_argument));
    } else if (access_idxs.count() > 0) { // Member Self Argument With Prefix.
      if (access_idxs.at(access_idxs.count() - 1).is_udf_type()) {
        OX (self_argument = access_idxs.at(access_idxs.count() - 1).get_sysfunc_);
        OX (access_idxs.reset());
      } else {
        OZ (make_var_from_access(access_idxs,
                                 expr_factory_,
                                 &(resolve_ctx_.session_info_),
                                 &resolve_ctx_.schema_guard_,
                                 current_block_->get_namespace(),
                                 self_argument));
        OX (access_idxs.reset()); // Erase Pre Access. Start New Begin with UDF.
      }
    } else { // Member Self Argument Without Prefix.
      OZ (make_self_symbol_expr(func, self_argument));
    }
    CK (OB_NOT_NULL(self_argument));
    OZ (self_argument->add_flag(IS_UDT_UDF_SELF_PARAM));
    if (OB_SUCC(ret) && self_argument->is_obj_access_expr()) {
      OZ (func.add_obj_access_expr(self_argument));
    }
    OZ (udf_info.ref_expr_->add_param_expr(self_argument));
    OX (udf_info.udf_param_num_++);
    OZ (expr_params.push_back(self_argument));
    for(int64_t i = udf_info.ref_expr_->get_children_count() - 1; OB_SUCC(ret) && i > 0; --i) {
      OZ (udf_info.ref_expr_->replace_param_expr(i, expr_params.at(i - 1)));
    }
    OZ (udf_info.ref_expr_->replace_param_expr(0, self_argument));
    OX (udf_info.is_contain_self_param_ = true);
  }
  return ret;
}
#endif

int ObPLResolver::resolve_udf_info(
  ObUDFInfo &udf_info, ObIArray<ObObjAccessIdx> &access_idxs, ObPLCompileUnitAST &func)
{
  int ret = OB_SUCCESS;
  ObString db_name = udf_info.udf_database_;
  ObString package_name = udf_info.udf_package_;
  ObString udf_name = udf_info.udf_name_;
  ObSchemaChecker schema_checker;
  const ObIRoutineInfo *routine_info = NULL;
  ObProcType routine_type = STANDALONE_FUNCTION;
  ObSEArray<ObRawExpr*, 4> expr_params;

  CK (OB_NOT_NULL(udf_info.ref_expr_));
  CK (OB_NOT_NULL(current_block_));
  OX (func.set_external_state());
  OZ (schema_checker.init(resolve_ctx_.schema_guard_, resolve_ctx_.session_info_.get_sessid()));
  OZ (ObRawExprUtils::rebuild_expr_params(udf_info, &expr_factory_, expr_params), K(udf_info), K(access_idxs));
  {
    ObPLMockSelfArg self(access_idxs, expr_params, expr_factory_, resolve_ctx_.session_info_);;
    OZ (self.mock());
    OZ (current_block_->get_namespace().resolve_routine(resolve_ctx_,
                                                        udf_info.udf_database_,
                                                        udf_info.udf_package_,
                                                        udf_info.udf_name_,
                                                        expr_params,
                                                        routine_type,
                                                        routine_info), K(udf_info));
  }

#ifdef OB_BUILD_ORACLE_PL
  OZ (add_udt_self_argument(routine_info, expr_params, access_idxs, udf_info, func),
    K(access_idxs), K(expr_params));
#endif

  // adjust routine database name, will set to ObUDFRawExpr later.
  if (OB_SUCC(ret)
      && db_name.empty()
      && OB_NOT_NULL(routine_info)
      && routine_info->get_database_id() != OB_INVALID_ID) {
    if (routine_info->get_database_id() != resolve_ctx_.session_info_.get_database_id()) {
      const ObDatabaseSchema *database_schema = NULL;
      OZ (resolve_ctx_.schema_guard_.get_database_schema(
        resolve_ctx_.session_info_.get_effective_tenant_id(), routine_info->get_database_id(), database_schema));
      CK (OB_NOT_NULL(database_schema));
      OX (db_name = database_schema->get_database_name_str());
    }
    if (OB_SUCC(ret) && routine_info->get_package_id() != OB_INVALID_ID) {
      if (routine_info->is_udt_routine()) {
        const share::schema::ObUDTTypeInfo *udt_info = NULL;
        OZ (resolve_ctx_.schema_guard_.get_udt_info(
          routine_info->get_tenant_id(), routine_info->get_package_id(), udt_info));
        CK (OB_NOT_NULL(udt_info));
        OX (package_name = udt_info->get_type_name());
      } else {
        const share::schema::ObPackageInfo *package_info = NULL;
        OZ (resolve_ctx_.schema_guard_.get_package_info(
          routine_info->get_tenant_id(), routine_info->get_package_id(), package_info));
        CK (OB_NOT_NULL(package_info));
        OX (package_name = package_info->get_package_name());
      }
    }
    if (OB_SUCC(ret) &&
        OB_NOT_NULL(udf_info.ref_expr_) &&
        udf_info.ref_expr_->get_func_name().case_compare(routine_info->get_routine_name()) != 0) {
      OX (udf_info.ref_expr_->set_func_name(routine_info->get_routine_name()));
    }
  }

  if (OB_SUCC(ret)) {
    if (PACKAGE_PROCEDURE == routine_type
        || PACKAGE_FUNCTION == routine_type
        || UDT_PROCEDURE == routine_type
        || UDT_FUNCTION == routine_type) {

      const ObPLRoutineInfo *package_routine_info = static_cast<const ObPLRoutineInfo *>(routine_info);

      CK (OB_NOT_NULL(package_routine_info));

      OZ (check_package_accessible(
        current_block_, resolve_ctx_.schema_guard_, *package_routine_info));

      if (OB_SUCC(ret)
          && (ObPLBlockNS::BLOCK_PACKAGE_SPEC == current_block_->get_namespace().get_block_type()
              || ObPLBlockNS::BLOCK_OBJECT_SPEC == current_block_->get_namespace().get_block_type())
          && package_routine_info->get_pkg_id() == current_block_->get_namespace().get_package_id()) {
        ret = OB_ERR_REFER_SAME_PACKAGE;
        LOG_WARN("variable or constant initialization may not refer to functions"
                 "declared in the same package",
                 K(ret), KPC(package_routine_info));
      }

      if (OB_SUCC(ret)
          && resolve_ctx_.is_sql_scope_
          && package_routine_info->is_private_routine()) {
        ret = OB_ERR_PRIVATE_UDF_USE_IN_SQL;
        LOG_WARN("function 'string' may not be used in SQL", K(ret), K(udf_name));
        LOG_USER_ERROR(OB_ERR_PRIVATE_UDF_USE_IN_SQL, udf_name.length(), udf_name.ptr());
      }

#ifdef OB_BUILD_ORACLE_PL
      if (OB_SUCC(ret) && package_routine_info->is_udt_cons()) {
        bool is_overloaded = false;
        const ObUDTTypeInfo *udt_info = NULL;
        const uint64_t tenant_id = package_routine_info->get_tenant_id();
        OZ (resolve_ctx_.schema_guard_.get_udt_info(
          tenant_id, package_routine_info->get_pkg_id(), udt_info));
        CK (OB_NOT_NULL(udt_info));
        OZ (ObPLUDTObjectManager::check_overload_default_cons(package_routine_info,
                                                              udt_info,
                                                              is_overloaded));
        if (is_overloaded) {
          OX (udf_info.set_is_udt_overload_default_cons());
        }
      }
#endif

      OX (package_name = package_name.empty()
          ? current_block_->get_namespace().get_package_name() : package_name);

      bool is_package_body_udf
        = !package_routine_info->is_udt_routine()
          && package_routine_info->get_pkg_id() == current_block_->get_namespace().get_package_id()
          && (ObPLBlockNS::BlockType::BLOCK_PACKAGE_BODY == current_block_->get_namespace().get_block_type()
              || ObPLBlockNS::BlockType::BLOCK_OBJECT_BODY == current_block_->get_namespace().get_block_type()
              || ObPLBlockNS::BlockType::BLOCK_ROUTINE == current_block_->get_namespace().get_block_type());
      int64_t cur_pkg_version = current_block_->get_namespace().get_package_version();
      if (OB_SUCC(ret)
          && OB_INVALID_ID != package_routine_info->get_pkg_id()
          && package_routine_info->get_pkg_id() != current_block_->get_namespace().get_package_id()) {
        share::schema::ObSchemaType schema_type = OB_MAX_SCHEMA;
        schema_type = package_routine_info->is_udt_routine() ? UDT_SCHEMA : PACKAGE_SCHEMA;
        OZ (resolve_ctx_.schema_guard_.get_schema_version(schema_type,
                                                          package_routine_info->get_tenant_id(),
                                                          package_routine_info->get_pkg_id(),
                                                          cur_pkg_version));
      }
      OZ (ObRawExprUtils::resolve_udf_common_info(db_name,
                                                  package_name,
                                                  package_routine_info->get_id(),
                                                  package_routine_info->get_pkg_id(),
                                                  package_routine_info->get_subprogram_path(),
                                                  common::OB_INVALID_VERSION, /*udf_schema_version*/
                                                  cur_pkg_version,
                                                  package_routine_info->is_deterministic(),
                                                  package_routine_info->is_parallel_enable(),
                                                  is_package_body_udf,
                                                  false,
                                                  common::OB_INVALID_ID,
                                                  udf_info));
      OZ (ObRawExprUtils::resolve_udf_param_types(package_routine_info,
                                                  resolve_ctx_.schema_guard_,
                                                  resolve_ctx_.session_info_,
                                                  resolve_ctx_.allocator_,
                                                  resolve_ctx_.sql_proxy_,
                                                  udf_info), udf_info);
      OZ (ObRawExprUtils::resolve_udf_param_exprs(package_routine_info,
                                                  current_block_->get_namespace(),
                                                  schema_checker,
                                                  resolve_ctx_.session_info_,
                                                  resolve_ctx_.allocator_,
                                                  resolve_ctx_.is_prepare_protocol_,
                                                  expr_factory_,
                                                  resolve_ctx_.sql_proxy_,
                                                  resolve_ctx_.extern_param_info_,
                                                  udf_info), udf_info);
    } else if (STANDALONE_PROCEDURE == routine_type
               || STANDALONE_FUNCTION == routine_type) {

      const share::schema::ObRoutineInfo *schema_routine_info = static_cast<const ObRoutineInfo *>(routine_info);
      const ObPackageInfo* package_info = NULL;
      const ObUDTTypeInfo *udt_info = NULL;
      int64_t schema_version = OB_INVALID_VERSION;
      uint64_t routine_id = OB_INVALID_ID;

      CK (OB_NOT_NULL(schema_routine_info));

      OZ (check_routine_accessible(
        current_block_, resolve_ctx_.schema_guard_, *schema_routine_info));

      OX (routine_id = (OB_INVALID_ID == schema_routine_info->get_package_id())
          ? schema_routine_info->get_routine_id() : schema_routine_info->get_subprogram_id());

      OX (udf_info.is_udt_udf_ = schema_routine_info->is_udt_routine());

      if (OB_SUCC(ret) && routine_id == schema_routine_info->get_subprogram_id()) {
        if (!udf_info.is_udt_udf_) {
          OZ (resolve_ctx_.schema_guard_.get_package_info(
              schema_routine_info->get_tenant_id(), schema_routine_info->get_package_id(), package_info));
          CK (OB_NOT_NULL(package_info));
          OX (schema_version = package_info->get_schema_version());
        }
#ifdef OB_BUILD_ORACLE_PL
        else {
          OZ (resolve_ctx_.schema_guard_.get_udt_info(
              schema_routine_info->get_tenant_id(), schema_routine_info->get_package_id(), udt_info));
          CK (OB_NOT_NULL(udt_info));
          OX (schema_version = udt_info->get_schema_version());

          // to check is this overload the default constructor
          if (OB_SUCC(ret) && schema_routine_info->is_udt_cons()) {
            bool is_overloaded = false;
            OZ (ObPLUDTObjectManager::check_overload_default_cons(schema_routine_info,
                                                                  udt_info,
                                                                  is_overloaded));
            if (is_overloaded) {
              OX (udf_info.set_is_udt_overload_default_cons());
            }
          }
        }
#endif
      }
      OZ (ObRawExprUtils::resolve_udf_common_info(db_name,
                                                  package_name,
                                                  routine_id,
                                                  schema_routine_info->get_package_id(),
                                                  ObArray<int64_t>(),
                                                  routine_id == schema_routine_info->get_subprogram_id()
                                                    ? common::OB_INVALID_VERSION
                                                      : schema_routine_info->get_schema_version(),
                                                  routine_id == schema_routine_info->get_subprogram_id()
                                                    ? schema_version
                                                      : common::OB_INVALID_VERSION, /*pkg_schema_version*/
                                                  schema_routine_info->is_deterministic(),
                                                  schema_routine_info->is_parallel_enable(),
                                                  false, /*is_pkg_body_udf*/
                                                  schema_routine_info->is_aggregate(),
                                                  schema_routine_info->get_type_id(),
                                                  udf_info));
      OZ (ObRawExprUtils::resolve_udf_param_types(schema_routine_info,
                                                  resolve_ctx_.schema_guard_,
                                                  resolve_ctx_.session_info_,
                                                  resolve_ctx_.allocator_,
                                                  resolve_ctx_.sql_proxy_,
                                                  udf_info), udf_info);
      OZ (ObRawExprUtils::resolve_udf_param_exprs(schema_routine_info,
                                                  current_block_->get_namespace(),
                                                  schema_checker,
                                                  resolve_ctx_.session_info_,
                                                  resolve_ctx_.allocator_,
                                                  resolve_ctx_.is_prepare_protocol_,
                                                  expr_factory_,
                                                  resolve_ctx_.sql_proxy_,
                                                  resolve_ctx_.extern_param_info_,
                                                  udf_info), udf_info);
    } else if (NESTED_PROCEDURE == routine_type || NESTED_FUNCTION == routine_type) {
      const ObPLRoutineInfo *sub_routine_info = static_cast<const ObPLRoutineInfo *>(routine_info);

      CK (OB_NOT_NULL(sub_routine_info));

      OZ (ObRawExprUtils::resolve_udf_common_info(db_name,
                                                  package_name,
                                                  sub_routine_info->get_parent_id(),
                                                  current_block_->get_namespace().get_package_id(),
                                                  sub_routine_info->get_subprogram_path(),
                                                  common::OB_INVALID_VERSION, /*udf_schema_version*/
                                                  common::OB_INVALID_VERSION, /*pkg_schema_version*/
                                                  sub_routine_info->is_deterministic(),
                                                  sub_routine_info->is_parallel_enable(),
                                                  pl::ObPLBlockNS::BlockType::BLOCK_PACKAGE_BODY
                                                    == current_block_->get_namespace().get_block_type(),
                                                  false,
                                                  common::OB_INVALID_ID,
                                                  udf_info));
      OZ (ObRawExprUtils::resolve_udf_param_types(sub_routine_info,
                                                  resolve_ctx_.schema_guard_,
                                                  resolve_ctx_.session_info_,
                                                  resolve_ctx_.allocator_,
                                                  resolve_ctx_.sql_proxy_,
                                                  udf_info), udf_info);
      OZ (ObRawExprUtils::resolve_udf_param_exprs(sub_routine_info,
                                                  current_block_->get_namespace(),
                                                  schema_checker,
                                                  resolve_ctx_.session_info_,
                                                  resolve_ctx_.allocator_,
                                                  resolve_ctx_.is_prepare_protocol_,
                                                  expr_factory_,
                                                  resolve_ctx_.sql_proxy_,
                                                  resolve_ctx_.extern_param_info_,
                                                  udf_info), udf_info);
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected routine type",
          K(routine_type), K(db_name), K(package_name), K(udf_name),
          K(ret));
    }
    if (OB_SUCC(ret) && !resolve_ctx_.is_sql_scope_) {
      ObUDFRawExpr *udf_raw_expr = NULL;
      const ObPLSymbolTable *table = current_block_->get_symbol_table();
      CK (OB_NOT_NULL(table));
      CK (OB_NOT_NULL(udf_raw_expr = udf_info.ref_expr_));
      for (int64_t i = 0; OB_SUCC(ret) && i < udf_raw_expr->get_params_desc().count(); ++i) {
        int64_t position = udf_raw_expr->get_param_position(i);
        if (position != OB_INVALID_INDEX
            && udf_raw_expr->get_params_desc().at(i).is_local_out()) {
          const ObPLVar *var = NULL;
          ObExprResType result_type;
          CK (OB_NOT_NULL(var = table->get_symbol(position)));
          if (OB_SUCC(ret) && var->is_readonly()) {
            if (var->get_name().prefix_match(ANONYMOUS_ARG)) {
              ObPLVar* shadow_var = const_cast<ObPLVar*>(var);
              ObIRoutineParam *iparam = NULL;
              OX (shadow_var->set_readonly(false));
              CK (OB_NOT_NULL(routine_info));
              OZ (routine_info->get_routine_param(i, iparam));
              if (OB_SUCC(ret) && iparam->is_inout_param()) {
                shadow_var->set_name(ANONYMOUS_INOUT_ARG);
              }
            } else {
              ret = OB_ERR_VARIABLE_IS_READONLY;
              LOG_WARN("variable is read only", K(ret), K(position), KPC(var));
            }
          }
          if (OB_SUCC(ret)) {
            const ObPLDataType &pl_type = var->get_type();
            if (pl_type.is_obj_type()
                && pl_type.get_data_type()->get_obj_type() != ObNullType) {
              ObExprResTypes &params_type
                = static_cast<ObExprResTypes&>(udf_raw_expr->get_params_type());
              CK (OB_NOT_NULL(pl_type.get_data_type()));
              OX (result_type = params_type.at(i));
              OX (result_type.set_meta(pl_type.get_data_type()->get_meta_type()));
              OX (result_type.set_accuracy(pl_type.get_data_type()->get_accuracy()));
              OX (params_type[i] = result_type);
            }
            if (OB_SUCC(ret)) {
              LOG_DEBUG("rewrite params type",
                     K(ret), K(i), KPC(pl_type.get_data_type()),
                     K(udf_raw_expr->get_params_type().at(i)),
                     K(udf_raw_expr->get_params_type().at(i).get_accuracy()),
                     K(result_type));
            } else {
              LOG_WARN("rewrite params type failed", K(ret), K(i), K(pl_type));
            }
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      ObUDFRawExpr *udf_raw_expr = NULL;
      CK (OB_NOT_NULL(udf_raw_expr = udf_info.ref_expr_));
      OX (udf_raw_expr->set_is_udt_cons(udf_info.is_udf_udt_cons()));
      OX (udf_raw_expr->set_is_udt_udf(routine_info->is_udt_routine()));
      OX (udf_raw_expr->set_is_deterministic(routine_info->is_deterministic()));
    }
  }
  return ret;
}

int ObPLResolver::check_local_variable_read_only(
  const ObPLBlockNS &ns, uint64_t var_idx, bool is_for_inout_param)
{
  int ret = OB_SUCCESS;
  const ObPLVar *var = NULL;
  const ObPLSymbolTable *symbol_table = NULL;
  CK (OB_NOT_NULL(symbol_table = ns.get_symbol_table()));
  OV (OB_NOT_NULL(var = symbol_table->get_symbol(var_idx)), OB_ERR_UNEXPECTED, K(var_idx));
  if (OB_SUCC(ret)) {
#define GET_TRIGGER_INFO  \
  ObSchemaChecker schema_checker; \
  const ObTriggerInfo *trg_info = NULL; \
  const uint64_t tenant_id = resolve_ctx_.session_info_.get_effective_tenant_id();  \
  OZ (schema_checker.init(resolve_ctx_.schema_guard_, resolve_ctx_.session_info_.get_sessid())); \
  OZ (schema_checker.get_trigger_info(tenant_id, ns.get_db_name(), ns.get_package_name(), trg_info)); \
  CK (OB_NOT_NULL(trg_info));

    // check type udf member function attr readable. etc: a := 5; when a is type object attr
    // and this stmt is inside a object member function, it is not applicable.
    // note: this statement is avaiable inside a member procedure
    if (ns.get_symbol_table()->get_self_param_idx() == var_idx
      && ns.function_block()
      && ns.is_udt_routine()
      && var->is_readonly()) {
      ret = OB_ERR_EXP_NOT_ASSIGNABLE;
      LOG_WARN("udt function attribute is read only", K(var_idx), K(ret),
                                                      K(ns.function_block()),
                                                      K(ns.is_udt_routine()));
      LOG_USER_ERROR(OB_ERR_EXP_NOT_ASSIGNABLE, var->get_name().length(), var->get_name().ptr());
    } else if(var->is_readonly()) {
      // 匿名块的参数是可以写的, 记录下当前匿名块的参数被写过
      if (var->get_name().prefix_match(ANONYMOUS_ARG)) {
        ObPLVar *shadow_var = const_cast<ObPLVar*>(var);
        shadow_var->set_readonly(false);
        if (is_for_inout_param || var->is_referenced()) {
          shadow_var->set_name(ANONYMOUS_INOUT_ARG);
        }
      } else {
        if (lib::is_mysql_mode()) {
          if (0 == var->get_name().case_compare("NEW")
              && (TgTimingEvent::TG_AFTER_DELETE == resolve_ctx_.params_.tg_timing_event_
                  || TgTimingEvent::TG_BEFORE_DELETE == resolve_ctx_.params_.tg_timing_event_)) {
            ret = OB_ERR_TRIGGER_NO_SUCH_ROW;
            LOG_WARN("There is no NEW row in on DELETE trigger", K(ret), K(resolve_ctx_.params_.tg_timing_event_));
            LOG_USER_ERROR(OB_ERR_TRIGGER_NO_SUCH_ROW, "NEW", "DELETE");
          } else {
            ret = OB_ERR_TRIGGER_CANT_CHANGE_ROW;
            if (0 == var->get_name().case_compare("NEW")) {
              LOG_WARN("can not update NEW row in after trigger", K(var->get_name()), K(ret));
              LOG_MYSQL_USER_ERROR(OB_ERR_TRIGGER_CANT_CHANGE_ROW, "NEW", "after ");
            } else {
              LOG_WARN("can not update OLD row in trigger", K(var->get_name()), K(ret));
              LOG_MYSQL_USER_ERROR(OB_ERR_TRIGGER_CANT_CHANGE_ROW, "OLD", "");
            }
          }
        } else if (resolve_ctx_.session_info_.is_for_trigger_package()
                   && lib::is_oracle_mode()
                   && ObTriggerInfo::is_trigger_body_package_id(ns.get_package_id())) {
          GET_TRIGGER_INFO;
          if (OB_FAIL(ret)) {
          } else if (var->get_name().prefix_match(":")) {
            ObString tmp(var->get_name().length() - 1, var->get_name().ptr() + 1);
            if (0 == trg_info->get_ref_new_name().case_compare(tmp)) {
              ret = OB_ERR_TRIGGER_CANT_CHANGE_ROW;
              LOG_WARN("can not change NEW row in trigger", K(var->get_name()), K(ret));
            } else if (0 == trg_info->get_ref_old_name().case_compare(tmp)) {
              ret = OB_ERR_TRIGGER_CANT_CHANGE_OLD_ROW;
              LOG_WARN("can not change OLD row in trigger", K(var->get_name()), K(ret));
            }
          }
        } else {
          ret = OB_ERR_VARIABLE_IS_READONLY;
          LOG_WARN("variable is read only", K(ret), K(var_idx), KPC(var));
        }
      }
    } else if (resolve_ctx_.session_info_.is_for_trigger_package()) {
      if (ObTriggerInfo::is_trigger_body_package_id(ns.get_package_id()) && lib::is_oracle_mode()) {
        GET_TRIGGER_INFO;
        if (OB_SUCC(ret)
            && (trg_info->has_delete_event() && !trg_info->has_update_event() && !trg_info->has_insert_event())) {
          if (var->get_name().prefix_match(":")) {
            ObString tmp(var->get_name().length() - 1, var->get_name().ptr() + 1);
            if (0 == trg_info->get_ref_new_name().case_compare(tmp)) {
              ret = OB_ERR_TRIGGER_CANT_CHANGE_ROW;
              LOG_WARN("cannot change NEW values for this trigger type", K(var->get_name()), K(ret));
            }
          }
        }
      } else if (0 == var->get_name().case_compare("NEW") && lib::is_mysql_mode()
                 && (TgTimingEvent::TG_AFTER_DELETE == resolve_ctx_.params_.tg_timing_event_
                     || TgTimingEvent::TG_BEFORE_DELETE == resolve_ctx_.params_.tg_timing_event_)) {
        ret = OB_ERR_TRIGGER_NO_SUCH_ROW;
        LOG_WARN("there is no NEW row in on DELETE trigger", K(ret), K(resolve_ctx_.params_.tg_timing_event_));
        LOG_USER_ERROR(OB_ERR_TRIGGER_NO_SUCH_ROW, "NEW", "DELETE");
      }
    }
#undef GET_TRIGGER_INFO
  }
  return ret;
}

int ObPLResolver::restriction_on_result_cache(ObIRoutineInfo *routine_info)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(routine_info));
  /*
   * RESULT_CACHE is disallowed on functions with OUT or IN OUT parameters
   * RESULT_CACHE is disallowed on functions with IN or RETURN parameter of (or
   *  containing) these types:
   *   – BLOB
   *   – CLOB
   *   – NCLOB
   *   – REF CURSOR
   *   – Collection
   *   – Object
   *   – Record or PL/SQL collection that contains an unsupported return type
   */
#define RESTRICTION_ON_TYPE(type) \
  if (OB_FAIL(ret)) { \
  } else if (type.is_obj_type() && (ob_is_text_tc(type.get_obj_type())  \
              || ob_is_lob_tc(type.get_obj_type()))) { \
    ret = OB_ERR_IMPL_RESTRICTION; \
    LOG_USER_ERROR(OB_ERR_IMPL_RESTRICTION, \
             "RESULT_CACHE is disallowed on subprograms with IN/RETURN" \
             " parameter of (or containing) LOB type"); \
  } else if (type.is_cursor_type()) { \
    ret = OB_ERR_IMPL_RESTRICTION; \
    LOG_USER_ERROR(OB_ERR_IMPL_RESTRICTION, \
             "RESULT_CACHE is disallowed on subprograms with IN/RETURN" \
             " parameter of (or containing) RefCursor type"); \
  } else if (type.is_collection_type() && !type.is_udt_type()) { \
    ret = OB_ERR_IMPL_RESTRICTION; \
    LOG_USER_ERROR(OB_ERR_IMPL_RESTRICTION, \
             "RESULT_CACHE is disallowed on subprograms with IN/RETURN" \
             " parameter of (or containing) Collection type"); \
  } else if (type.is_record_type() && !type.is_udt_type() && !type.is_rowtype_type()) { \
    ret = OB_ERR_IMPL_RESTRICTION; \
    LOG_USER_ERROR(OB_ERR_IMPL_RESTRICTION, \
             "RESULT_CACHE is disallowed on subprograms with IN/RETURN" \
             " parameter of (or containing) Record type"); \
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < routine_info->get_param_count(); ++i) {
    ObIRoutineParam *param = NULL;
    ObPLRoutineParamMode mode;
    OZ (routine_info->get_routine_param(i, param));
    CK (OB_NOT_NULL(param));
    OX (mode = static_cast<ObPLRoutineParamMode>(param->get_mode()));
    if (OB_SUCC(ret)) {
      if (param->is_out_param() || param->is_inout_param()) {
        ret = OB_ERR_IMPL_RESTRICTION;
        LOG_USER_ERROR(OB_ERR_IMPL_RESTRICTION,
                 "RESULT_CACHE is disallowed on subprograms with OUT or IN OUT parameters");
      } else {
        RESTRICTION_ON_TYPE(param->get_pl_data_type());
      }
    }
  }
  if (OB_SUCC(ret)) {
    const ObIRoutineParam *ret_param = routine_info->get_ret_info();
    CK (OB_NOT_NULL(ret_param));
    RESTRICTION_ON_TYPE(ret_param->get_pl_data_type());
  }
#undef RESTRICTION_ON_TYPE

  return ret;
}

int ObPLResolver::get_caller_accessor_item(const ObPLStmtBlock *caller, AccessorItem &caller_item)
{
  int ret = OB_SUCCESS;
  const ObPLBlockNS *block_ns = NULL;
  CK (OB_NOT_NULL(caller));
  CK (OB_NOT_NULL(block_ns = &(caller->get_namespace())));
  if (OB_SUCC(ret)) {
    if (block_ns->get_package_id() != OB_INVALID_ID) {
      if (ObTriggerInfo::is_trigger_package_id(block_ns->get_package_id())) { //trigger
        caller_item.kind_ = AccessorItemKind::PL_ACCESSOR_TRIGGER;
        caller_item.schema_ = block_ns->get_db_name();
        caller_item.name_ = block_ns->get_package_name();
      } else { //package
        caller_item.kind_ = AccessorItemKind::PL_ACCESSOR_PACKAGE;
        caller_item.name_ = block_ns->get_package_name();
      }
    } else if (block_ns->function_block()) {
      caller_item.kind_ = AccessorItemKind::PL_ACCESSOR_FUNCTION;
      caller_item.schema_ = block_ns->get_db_name();
      caller_item.name_ = block_ns->get_routine_name();
    } else {
      caller_item.kind_ = AccessorItemKind::PL_ACCESSOR_PROCEDURE;
      caller_item.schema_ = block_ns->get_db_name();
      caller_item.name_ = block_ns->get_routine_name();
    }
    if (caller_item.schema_.empty()) {
      caller_item.schema_ = resolve_ctx_.session_info_.get_database_name();
    }
  }
  return ret;
}

int ObPLResolver::check_package_accessible(
  const ObPLStmtBlock *caller, ObSchemaGetterGuard &guard, uint64_t package_id)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(caller));
  if (OB_SUCC(ret)
      && OB_INVALID_ID != package_id
      && package_id != caller->get_namespace().get_package_id()) {
    const ObPackageInfo *pkg_info = NULL;
    AccessorItem caller_item;
    const uint64_t tenant_id = get_tenant_id_by_object_id(package_id);
    OZ (guard.get_package_info(tenant_id, package_id, pkg_info));
    CK (OB_NOT_NULL(pkg_info));
    if (OB_SUCC(ret) && pkg_info->has_accessible_by_clause()) {
      OZ (get_caller_accessor_item(caller, caller_item));
      OZ (check_package_accessible(caller_item, pkg_info->get_source()));
    }
  }
  return ret;
}

int ObPLResolver::check_package_accessible(
  const ObPLStmtBlock *caller, ObSchemaGetterGuard &guard, const ObPLRoutineInfo &routine_info)
{
  int ret = OB_SUCCESS;
  if (routine_info.has_accessible_by_clause()) {
    AccessorItem caller_item;
    CK (OB_NOT_NULL(caller));
    OZ (check_package_accessible(caller, guard, routine_info.get_pkg_id()));
    OZ (get_caller_accessor_item(caller, caller_item));
    OZ (check_routine_accessible(caller_item, routine_info.get_routine_body()));
  }
  return ret;
}

int ObPLResolver::check_routine_accessible(
  const ObPLStmtBlock *caller, ObSchemaGetterGuard &guard, const ObRoutineInfo& routine_info)
{
  int ret = OB_SUCCESS;
  if (routine_info.has_accessible_by_clause()) {
    AccessorItem caller_item;
    CK (OB_NOT_NULL(caller));
    if (OB_SUCC(ret) && routine_info.get_package_id() != OB_INVALID_ID) {
      OZ (check_package_accessible(caller, guard, routine_info.get_package_id()));
    }
    OZ (get_caller_accessor_item(caller, caller_item));
    OZ (check_routine_accessible(caller_item, routine_info.get_routine_body()));
  }
  return ret;
}

int ObPLResolver::check_package_accessible(
  AccessorItem &caller, const ObString &package_body)
{
  int ret = OB_SUCCESS;
  ObSEArray<AccessorItem, 4> accessors;
  OZ (resolve_package_accessible_by(package_body, accessors));
  OZ (check_common_accessible(caller, accessors));
  return ret;
}

int ObPLResolver::check_routine_accessible(
  AccessorItem &caller, const ObString &routine_body)
{
  int ret = OB_SUCCESS;
  ObSEArray<AccessorItem, 4> accessors;
  OZ (resolve_routine_accessible_by(routine_body, accessors));
  OZ (check_common_accessible(caller, accessors));
  return ret;
}

int ObPLResolver::check_common_accessible(
  AccessorItem &caller, ObIArray<AccessorItem> &accessors)
{
  int ret = OB_SUCCESS;
  bool found = false;
  for (int64_t i = 0; OB_SUCC(ret) && !found && i < accessors.count(); ++i) {
    AccessorItem &accessor = accessors.at(i);
    if (0 == accessor.name_.case_compare(caller.name_)
        && 0 == accessor.schema_.case_compare(caller.schema_)
        && (accessor.kind_ == caller.kind_
            || AccessorItemKind::PL_ACCESSOR_ALL == accessor.kind_)) {
      found = true;
    }
  }
  if (!found && accessors.count() > 0) {
    ret = OB_ERR_INSUFFICIENT_PRIVILEGE;
    LOG_WARN("PLS-00904: insufficient privilege to access object string",
             K(ret), K(caller), K(accessors));
  }
  return ret;
}

int ObPLResolver::resolve_accessible_by(
  const ObStmtNodeTree *accessor_list, ObIArray<AccessorItem> &result)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(accessor_list));
  for (int64_t i = 0; OB_SUCC(ret) && i < accessor_list->num_child_; ++i) {
    const ObStmtNodeTree *accessor = accessor_list->children_[i];
    const ObStmtNodeTree *kind = NULL;
    const ObStmtNodeTree *name = NULL;
    CK (OB_NOT_NULL(accessor));
    CK (T_SP_ACCESSOR == accessor->type_);
    CK (2 == accessor->num_child_);
    OX (kind = accessor->children_[0]);
    CK (OB_NOT_NULL(name = accessor->children_[1]));
    CK (2 == name->num_child_);
    CK (OB_NOT_NULL(name->children_[1]));
    if (OB_SUCC(ret)) {
      ObString schema_name;
      ObString item_name;
      OZ (ob_write_string(resolve_ctx_.allocator_,
                          ObString(name->children_[1]->str_len_, name->children_[1]->str_value_),
                          item_name));
      if (OB_NOT_NULL(name->children_[0])) {
        OZ (ob_write_string(resolve_ctx_.allocator_,
                          ObString(name->children_[0]->str_len_, name->children_[0]->str_value_),
                          schema_name));
      } else {
        schema_name = resolve_ctx_.session_info_.get_database_name();
      }
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(kind)) {
        OZ (result.push_back(
          AccessorItem(AccessorItemKind::PL_ACCESSOR_ALL, schema_name, item_name)));
      } else {
        AccessorItemKind item_kind = PL_ACCESSOR_INVALID;
        switch(kind->value_) {
          case SP_FUNCTION: {
            item_kind = AccessorItemKind::PL_ACCESSOR_FUNCTION;
            break;
          }
          case SP_PROCEDURE: {
            item_kind = AccessorItemKind::PL_ACCESSOR_PROCEDURE;
            break;
          }
          case SP_PACKAGE: {
            item_kind = AccessorItemKind::PL_ACCESSOR_PACKAGE;
            break;
          }
          case SP_TRIGGER: {
            item_kind = AccessorItemKind::PL_ACCESSOR_TRIGGER;
            break;
          }
          case SP_TYPE: {
            item_kind = AccessorItemKind::PL_ACCESSOR_TYPE;
            break;
          }
          default: {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected accessor kind node", K(ret), K(kind));
          }
        }
        OZ (result.push_back(AccessorItem(item_kind, schema_name, item_name)));
      }
    }
  }
  return ret;
}

int ObPLResolver::resolve_package_accessible_by(
  const ObString source, ObIArray<AccessorItem> &result)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObPLParser parser(allocator, ObCharsets4Parser(), resolve_ctx_.session_info_.get_sql_mode());
  ObStmtNodeTree *parse_tree = NULL;
  const ObStmtNodeTree *package_node = NULL;
  const ObStmtNodeTree *clause_node = NULL;
  CK (lib::is_oracle_mode());
  OZ (parser.parse_package(source, parse_tree, resolve_ctx_.session_info_.get_dtc_params(), NULL, false));
  CK (OB_NOT_NULL(parse_tree));
  CK (T_STMT_LIST == parse_tree->type_);
  CK (1 == parse_tree->num_child_);
  CK (OB_NOT_NULL(package_node = parse_tree->children_[0]));
  if (OB_SUCC(ret) && T_SP_PRE_STMTS == parse_tree->type_) {
    OZ (ObPLResolver::resolve_condition_compile(
      resolve_ctx_.allocator_,
      &(resolve_ctx_.session_info_),
      &(resolve_ctx_.schema_guard_),
      &(resolve_ctx_.package_guard_),
      &(resolve_ctx_.sql_proxy_),
      NULL,
      package_node,
      package_node,
      true /*inner_parse*/));
  }
  CK (OB_NOT_NULL(package_node));
  CK (T_PACKAGE_BLOCK == package_node->type_);
  CK (4 == package_node->num_child_);
  OX (clause_node = package_node->children_[1]);
  for (int64_t i = 0;
      OB_SUCC(ret) && OB_NOT_NULL(clause_node) && i < clause_node->num_child_; ++i) {
    const ObStmtNodeTree *child = clause_node->children_[i];
    if (OB_NOT_NULL(child) && T_SP_ACCESSIBLE_BY == child->type_) {
      const ObStmtNodeTree *accessor_list = NULL;
      CK (1 == child->num_child_);
      OX (accessor_list = child->children_[0]);
      CK (OB_NOT_NULL(accessor_list));
      CK (T_SP_ACCESSOR_LIST == accessor_list->type_);
      OZ (resolve_accessible_by(accessor_list, result));
    }
  }
  return ret;
}

int ObPLResolver::resolve_routine_accessible_by(
  const ObString source, ObIArray<AccessorItem> &result)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObPLParser parser(allocator, ObCharsets4Parser(), resolve_ctx_.session_info_.get_sql_mode());
  ObStmtNodeTree *parse_tree = NULL;
  const ObStmtNodeTree *routine_node = NULL;
  const ObStmtNodeTree *clause_node = NULL;
  CK (lib::is_oracle_mode());
  OZ (parser.parse_routine_body(source, parse_tree, false), source);
  CK (OB_NOT_NULL(parse_tree->children_));
  CK (1 == parse_tree->num_child_);
  CK (OB_NOT_NULL(parse_tree->children_[0]));
  OX (routine_node = parse_tree->children_[0]);
  if (OB_SUCC(ret) && T_SP_PRE_STMTS == routine_node->type_) {
    OZ (ObPLResolver::resolve_condition_compile(
      resolve_ctx_.allocator_,
      &(resolve_ctx_.session_info_),
      &(resolve_ctx_.schema_guard_),
      &(resolve_ctx_.package_guard_),
      &(resolve_ctx_.sql_proxy_),
      NULL,
      routine_node,
      routine_node,
      true /*inner_parse*/));
  }
  CK (OB_NOT_NULL(routine_node));
  CK (T_SF_SOURCE == routine_node->type_ || T_SP_SOURCE == routine_node->type_);
  if (OB_SUCC(ret)) {
    if (T_SF_SOURCE == routine_node->type_) {
      clause_node = routine_node->children_[3];
    } else {
      clause_node = routine_node->children_[2];
    }
  }
  for (int64_t i = 0;
       OB_SUCC(ret) && OB_NOT_NULL(clause_node) && i < clause_node->num_child_; ++i) {
    const ObStmtNodeTree *child = clause_node->children_[i];
    if (OB_NOT_NULL(child) && T_SP_ACCESSIBLE_BY == child->type_) {
      const ObStmtNodeTree *accessor_list = NULL;
      CK (1 == child->num_child_);
      OX (accessor_list = child->children_[0]);
      CK (OB_NOT_NULL(accessor_list));
      CK (T_SP_ACCESSOR_LIST == accessor_list->type_);
      OZ (resolve_accessible_by(accessor_list, result));
    }
  }
  return ret;
}

int ObPLResolver::resolve_sf_clause(
  const ObStmtNodeTree *node, ObIRoutineInfo *routine_info, ObProcType &routine_type, const ObPLDataType &ret_type)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(node));
  CK (OB_NOT_NULL(routine_info));
  CK (T_SP_CLAUSE_LIST == node->type_);
  for (int64_t i = 0; OB_SUCC(ret) && i < node->num_child_; ++i) {
    const ObStmtNodeTree *child = node->children_[i];
    bool has_invoker_clause = false;
    if (OB_NOT_NULL(child)) {
      if (T_SP_DETERMINISTIC == child->type_) {
        if (routine_info->is_deterministic()) {
          ret = OB_ERR_DECL_MORE_THAN_ONCE;
          LOG_WARN("PLS-00371: at most one declaration for 'string' is permitted",
                   K(ret), K(child->type_));
        } else {
          routine_info->set_deterministic();
        }
      } else if (T_SP_PARALLEL_ENABLE == child->type_) {
        if (routine_info->is_parallel_enable()) {
          ret = OB_ERR_DECL_MORE_THAN_ONCE;
          LOG_WARN("PLS-00371: at most one declaration for 'string' is permitted",
                   K(ret), K(child->type_));
        } else if (child->num_child_ > 0) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("not support partition by clause in parallel enable clause", K(ret), K(child));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "partition by clause in parallel enable clause");
        } else if (ObProcType::NESTED_FUNCTION == routine_type
                   || ObProcType::NESTED_PROCEDURE == routine_type) {
          ret = OB_ERR_ILLEGAL_OPTION;
          LOG_WARN("PLS-00712: illegal option for subprogram string", K(ret));
        } else {
          routine_info->set_parallel_enable();
        }
      } else if (T_SP_INVOKE == child->type_) {
        if (has_invoker_clause) {
          ret = OB_ERR_DECL_MORE_THAN_ONCE;
          LOG_WARN("PLS-00371: at most one declaration for 'string' is permitted",
                   K(ret), K(child->type_));
        } else if (ObProcType::STANDALONE_FUNCTION != routine_type
                   && ObProcType::STANDALONE_PROCEDURE != routine_type) {
          ret = OB_ERR_ONLY_SCHEMA_LEVEL_ALLOW;
          LOG_USER_ERROR(OB_ERR_ONLY_SCHEMA_LEVEL_ALLOW, "AUTHID");
        } else {
          has_invoker_clause = true;
          if (lib::is_oracle_mode() && SP_CURRENT_USER == child->value_) {
            routine_info->set_invoker_right();
          } else if (lib::is_mysql_mode() && SP_INVOKER == child->value_) {
            routine_info->set_invoker_right();
          }
        }
      } else if (T_SP_RESULT_CACHE == child->type_) {
        /* This RELIES_ON clause is deprecated. As of Oracle Database 12c, the database
         * detects all data sources that are queried while a result-cached function is
         * running, and RELIES_ON clause does nothing. */
        if (routine_info->is_result_cache()) {
          ret = OB_ERR_DECL_MORE_THAN_ONCE;
          LOG_WARN("PLS-00371: at most one declaration for 'string' is permitted",
                   K(ret), K(child->type_));
        } else if (ObProcType::NESTED_FUNCTION == routine_type
                   || ObProcType::NESTED_PROCEDURE == routine_type) {
          ret = OB_ERR_IMPL_RESTRICTION;
          LOG_USER_ERROR(OB_ERR_IMPL_RESTRICTION,
                         "RESULT_CACHE on subprograms in anonymous blocks is");
        } else {
          OZ (restriction_on_result_cache(routine_info));
          OX (routine_info->set_result_cache());
        }
      } else if (T_SP_ACCESSIBLE_BY == child->type_) {
        if (routine_info->has_accessible_by_clause()) {
          ret = OB_ERR_DECL_MORE_THAN_ONCE;
          LOG_WARN("PLS-00371: at most one declaration for 'string' is permitted",
                   K(ret), K(child->type_));
        } else if (ObProcType::NESTED_FUNCTION == routine_type
                   || ObProcType::NESTED_PROCEDURE == routine_type) {
          ret = OB_ERR_MISMATCH_SUBPROGRAM;
          LOG_WARN("PLS-00263: mismatch between string on a subprogram specification and body",
                   K(ret), K(child->type_));
        } else {
          routine_info->set_accessible_by_clause();
        }
      } else if (T_SP_PIPELINED == child->type_) {
        CK (OB_NOT_NULL(routine_info->get_ret_info()));
        if (OB_SUCC(ret)) {
          if (PACKAGE_FUNCTION != routine_type && NESTED_FUNCTION != routine_type
              && STANDALONE_FUNCTION != routine_type && UDT_FUNCTION != routine_type) {
            ret = OB_ERR_ONLY_FUNC_CAN_PIPELINED;
            LOG_WARN("only functions can be declared as PIPELINED", K(ret));
          } else if (!ret_type.is_nested_table_type() && !ret_type.is_varray_type()) {
            ret = OB_ERR_PIPE_RETURN_NOT_COLL;
            LOG_WARN("pipelined functions must have a supported collection return type",
                     K(ret_type.get_type()), K(ret));
          }
          OX (routine_info->set_pipelined());
        }
      } else if (T_COMMENT == child->type_) {
        if (lib::is_mysql_mode()) {
          ObString routine_comment;
          CK (OB_NOT_NULL(dynamic_cast<ObRoutineInfo*>(routine_info)));
          OX (routine_comment = ObString(child->str_len_, child->str_value_));
          OZ (dynamic_cast<ObRoutineInfo*>(routine_info)->set_comment(routine_comment));
        }
      } else if (T_SP_DATA_ACCESS == child->type_) {
        if (lib::is_mysql_mode()) {
          if (SP_NO_SQL == child->value_) {
            routine_info->set_no_sql();
          } else if (SP_READS_SQL_DATA == child->value_) {
            routine_info->set_reads_sql_data();
          } else if (SP_MODIFIES_SQL_DATA == child->value_) {
            routine_info->set_modifies_sql_data();
          } else if (SP_CONTAINS_SQL == child->value_) {
            routine_info->set_contains_sql();
          }
        }
      }
    }
  }
  return ret;
}

int ObPLResolver::get_local_variable_constraint(
  const ObPLBlockNS &ns, int64_t var_idx, bool &not_null, ObPLIntegerRange &range)
{
  int ret = OB_SUCCESS;
  const ObPLVar *var = NULL;
  CK (OB_NOT_NULL(ns.get_symbol_table()));
  CK (OB_NOT_NULL(var = ns.get_symbol_table()->get_symbol(var_idx)));
  CK (OB_NOT_NULL(var));
  OX (not_null = var->is_not_null());
  OX (var->get_type().is_pl_integer_type() ?
        range.set_range(var->get_type().get_range()) : void(NULL));
  return ret;
}

int ObPLResolver::get_subprogram_ns(
  ObPLBlockNS &current_ns, uint64_t subprogram_id, ObPLBlockNS *&subprogram_ns)
{
  int ret = OB_SUCCESS;
  if (current_ns.get_routine_id() == subprogram_id) {
    subprogram_ns = &current_ns;
  } else if (OB_NOT_NULL(current_ns.get_external_ns())
             && OB_NOT_NULL(current_ns.get_external_ns()->get_parent_ns())) {
    OZ (get_subprogram_ns(
      *(const_cast<ObPLBlockNS *>(current_ns.get_external_ns()->get_parent_ns())),
      subprogram_id, subprogram_ns));
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("can not found subprogram namespace",
             K(ret), K(subprogram_id), K(current_ns.get_routine_id()));
  }
  return ret;
}

int ObPLResolver::get_subprogram_var(
  ObPLBlockNS &ns, uint64_t subprogram_id, int64_t var_idx, const ObPLVar *&var)
{
  int ret = OB_SUCCESS;
  ObPLBlockNS *subprogram_ns = NULL;
  const ObPLSymbolTable *symbol_table = NULL;
  OZ (get_subprogram_ns(ns, subprogram_id, subprogram_ns));
  CK (OB_NOT_NULL(subprogram_ns));
  CK (OB_NOT_NULL(symbol_table = subprogram_ns->get_symbol_table()));
  OV (OB_NOT_NULL(var = symbol_table->get_symbol(var_idx)),
    OB_ERR_UNEXPECTED, K(var_idx), K(subprogram_id), K(symbol_table->get_count()));
  return ret;
}

int ObPLResolver::check_subprogram_variable_read_only(
  ObPLBlockNS &ns, uint64_t subprogram_id, int64_t var_idx)
{
  int ret = OB_SUCCESS;
  ObPLBlockNS *subprogram_ns = NULL;
  OZ (get_subprogram_ns(ns, subprogram_id, subprogram_ns));
  CK (OB_NOT_NULL(subprogram_ns));
  OZ (check_local_variable_read_only(*subprogram_ns, var_idx));
  return ret;
}

int ObPLResolver::check_package_variable_read_only(uint64_t package_id, uint64_t var_idx)
{
  int ret = OB_SUCCESS;
  bool is_local = false;

  const ObPLBlockNS *ns = &(current_block_->get_namespace());
  do {
    if (ns->get_block_type() != ObPLBlockNS::BLOCK_ROUTINE
      && ns->get_package_id() == package_id) {
      break;
    } else {
      const ObPLBlockNS *pre_ns = ns->get_pre_ns();
      if (OB_NOT_NULL(ns->get_pre_ns())) {
        ns = ns->get_pre_ns();
      } else if (OB_NOT_NULL(ns->get_external_ns())) {
        ns = ns->get_external_ns()->get_parent_ns();
      } else {
        ns = NULL;
      }
    }
  } while (OB_NOT_NULL(ns));

  if (OB_NOT_NULL(ns)) {
    is_local = true;
    OZ (check_local_variable_read_only(*ns, var_idx));
  }

  if (OB_SUCC(ret) && !is_local) {
    const ObPLVar *var = NULL;
    ObPLPackageManager &package_manager =
        resolve_ctx_.session_info_.get_pl_engine()->get_package_manager();
    OZ (package_manager.get_package_var(resolve_ctx_, package_id, var_idx, var));
    CK (OB_NOT_NULL(var));
    if (OB_SUCC(ret) && var->is_readonly()) {
      ret = OB_ERR_VARIABLE_IS_READONLY;
      LOG_WARN("variable is read only", K(ret), K(package_id), K(var_idx));
    }
  }
  return ret;
}

int ObPLResolver::check_variable_accessible(
        const ObPLBlockNS &ns, const ObIArray<ObObjAccessIdx>& access_idxs, bool for_write)
{
  int ret = OB_SUCCESS;
  CK (!access_idxs.empty());
  if (OB_FAIL(ret)) {
  } else if (for_write && OB_FAIL(check_update_column(ns, access_idxs))) {
    LOG_WARN("check update column failed", K(ret));
  } else if (ObObjAccessIdx::is_local_variable(access_idxs)) {
    if (for_write) {
      OZ (check_local_variable_read_only(
        ns, access_idxs.at(ObObjAccessIdx::get_local_variable_idx(access_idxs)).var_index_
        /*access_idxs.at(access_idxs.count() - 1).var_index_*/), access_idxs);
    } else {
      ObPLVar *var = NULL;
      const ObPLSymbolTable *symbol_table = NULL;
      int64_t idx = access_idxs.at(ObObjAccessIdx::get_local_variable_idx(access_idxs)).var_index_;
      CK (OB_NOT_NULL(symbol_table = ns.get_symbol_table()));
      OV (OB_NOT_NULL(var = const_cast<ObPLVar *>(symbol_table->get_symbol(idx))), OB_ERR_UNEXPECTED, K(idx));
      OX (var->set_is_referenced(true));
    }
  } else if (ObObjAccessIdx::is_package_variable(access_idxs)) {
    if ((for_write && ns.get_compile_flag().compile_with_wnps())
        || (!for_write && ns.get_compile_flag().compile_with_rnps())) {
      ret = OB_ERR_SUBPROGRAM_VIOLATES_PRAGMA;
      LOG_WARN("PLS-00452: Subprogram 'string' violates its associated pragma",
               K(ret), K(ns.get_compile_flag()));
    } else {
      uint64_t package_id = OB_INVALID_ID;
      uint64_t var_idx = OB_INVALID_ID;
      OZ (ObObjAccessIdx::get_package_id(access_idxs, package_id, var_idx));
      if (OB_SUCC(ret) && for_write) {
        const ObPLBlockNS *iter_ns = &ns;
        while (OB_NOT_NULL(iter_ns)) {
          if ((ObPLBlockNS::BlockType::BLOCK_PACKAGE_SPEC == iter_ns->get_block_type()
                || ObPLBlockNS::BlockType::BLOCK_PACKAGE_BODY == iter_ns->get_block_type())
              && iter_ns->get_package_id() == package_id) {
            break;
          } else {
            iter_ns = OB_NOT_NULL(iter_ns->get_external_ns())
                        ? iter_ns->get_external_ns()->get_parent_ns() : NULL;
          }
        }
        if (OB_NOT_NULL(iter_ns)) {
          OZ (check_local_variable_read_only(*iter_ns, var_idx));
        } else {
          OZ (check_package_variable_read_only(package_id, var_idx));
        }
      }
      OZ (check_package_accessible(current_block_, resolve_ctx_.schema_guard_, package_id));
    }
  } else if (ObObjAccessIdx::is_subprogram_variable(access_idxs)) {
    if (for_write) {
      const ObPLBlockNS *subprogram_ns = NULL;
      const ObRawExpr *f_expr = NULL;
      int64_t subprogram_idx = OB_INVALID_INDEX;
      OX (subprogram_idx = ObObjAccessIdx::get_subprogram_idx(access_idxs));
      CK (subprogram_idx != OB_INVALID_INDEX
          && subprogram_idx >= 0 && subprogram_idx < access_idxs.count());
      OX (subprogram_ns = access_idxs.at(subprogram_idx).var_ns_);
      CK (OB_NOT_NULL(subprogram_ns));
      OX (f_expr = access_idxs.at(subprogram_idx).get_sysfunc_);
      CK (OB_NOT_NULL(f_expr));
      if (T_OP_GET_SUBPROGRAM_VAR == f_expr->get_expr_type()) {
        uint64_t actual_var_idx = OB_INVALID_INDEX;
        OZ (get_const_expr_value(f_expr->get_param_expr(2), actual_var_idx));
        OZ (check_local_variable_read_only(*subprogram_ns, actual_var_idx));
      } else {
        OZ (check_local_variable_read_only(
          *subprogram_ns, access_idxs.at(subprogram_idx).var_index_));
      }
    }
  } else if (ObObjAccessIdx::is_get_variable(access_idxs)) {
    // do nothing ...
  } else if (ObObjAccessIdx::is_function_return_variable(access_idxs)) {
    if (for_write) {
      ret = OB_ERR_VARIABLE_IS_READONLY;
      LOG_WARN("function return variable is read only", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unknow variable type", K(ret), K(access_idxs));
  }
  return ret;
}

int ObPLResolver::get_const_expr_value(const ObRawExpr *expr, uint64_t &val)
{
  int ret = OB_SUCCESS;
  const ObConstRawExpr *c_expr = static_cast<const ObConstRawExpr*>(expr);
  CK (OB_NOT_NULL(c_expr));
  CK (c_expr->get_value().is_uint64()
      || c_expr->get_value().is_int()
      || c_expr->get_value().is_unknown());
  OX (val = c_expr->get_value().is_uint64() ? c_expr->get_value().get_uint64()
        : c_expr->get_value().is_int() ? c_expr->get_value().get_int()
        : c_expr->get_value().get_unknown());
  return ret;
}

int ObPLResolver::check_variable_accessible(ObRawExpr *expr, bool for_write)
{
  int ret = OB_SUCCESS;

#define GET_CONST_EXPR_VALUE(expr, val) get_const_expr_value(expr, val)

  CK (OB_NOT_NULL(expr));
  CK (OB_NOT_NULL(current_block_));
  if (OB_FAIL(ret)) {
  } else if (expr->is_obj_access_expr()) {
    ObObjAccessRawExpr *obj_access = static_cast<ObObjAccessRawExpr*>(expr);
    CK (OB_NOT_NULL(obj_access));
    if (OB_FAIL(ret)) {
    } else if (ObObjAccessIdx::is_local_variable(obj_access->get_access_idxs())) {
      if (for_write) {
        ObIArray<ObObjAccessIdx> &access_idxs = obj_access->get_access_idxs();
        int64_t var_idx =
          access_idxs.at(ObObjAccessIdx::get_local_variable_idx(access_idxs)).var_index_;
        CK (var_idx >= 0 && var_idx < obj_access->get_var_indexs().count());
        OZ (check_local_variable_read_only(
          current_block_->get_namespace(), obj_access->get_var_indexs().at(var_idx)));
      }
    } else if (ObObjAccessIdx::is_subprogram_variable(obj_access->get_access_idxs()) && for_write) {
      const ObPLBlockNS *subprogram_ns = NULL;
      const ObRawExpr *f_expr = NULL;
      ObIArray<ObObjAccessIdx> &access_idxs = obj_access->get_access_idxs();
      int64_t subprogram_idx = access_idxs.at(ObObjAccessIdx::get_subprogram_idx(access_idxs)).var_index_;
      CK (subprogram_idx != OB_INVALID_INDEX
          && subprogram_idx >= 0 && subprogram_idx < access_idxs.count());
      OX (subprogram_ns = access_idxs.at(subprogram_idx).var_ns_);
      CK (OB_NOT_NULL(subprogram_ns));
      OX (f_expr = obj_access->get_param_expr(subprogram_idx));
      CK (OB_NOT_NULL(f_expr));
      if (T_OP_GET_SUBPROGRAM_VAR == f_expr->get_expr_type()) {
        uint64_t actual_var_idx = OB_INVALID_INDEX;
        GET_CONST_EXPR_VALUE(f_expr->get_param_expr(2), actual_var_idx);
        OZ (check_local_variable_read_only(*subprogram_ns, actual_var_idx));
      } else {
        OZ (check_variable_accessible(current_block_->get_namespace(),
                                  obj_access->get_access_idxs(),
                                  for_write),
                                  obj_access->get_access_idxs(), expr);
      }
    } else {
      OZ (check_variable_accessible(current_block_->get_namespace(),
                                   obj_access->get_access_idxs(),
                                   for_write),
                                   obj_access->get_access_idxs(), expr);
    }
  } else if (expr->is_const_raw_expr()) {
    if (for_write) {
      uint64_t var_idx = OB_INVALID_ID;
      GET_CONST_EXPR_VALUE(expr, var_idx);
      OZ (check_local_variable_read_only(current_block_->get_namespace(), var_idx));
    }
  } else if (expr->is_sys_func_expr()
             && T_OP_GET_PACKAGE_VAR == expr->get_expr_type()) {
    const ObSysFunRawExpr *f_expr = static_cast<const ObSysFunRawExpr *>(expr);
    uint64_t package_id = OB_INVALID_ID;
    uint64_t var_idx = OB_INVALID_ID;
    CK (OB_NOT_NULL(f_expr) && f_expr->get_param_count() >= 2);
    GET_CONST_EXPR_VALUE(f_expr->get_param_expr(0), package_id);
    GET_CONST_EXPR_VALUE(f_expr->get_param_expr(1), var_idx);
    if (OB_SUCC(ret) && for_write) {
      OZ (check_package_variable_read_only(package_id, var_idx));
    }
    OZ (check_package_accessible(current_block_, resolve_ctx_.schema_guard_, package_id));
  } else if (expr->is_sys_func_expr()
             && T_OP_GET_SUBPROGRAM_VAR == expr->get_expr_type()
             && for_write) {
    const ObSysFunRawExpr *f_expr = static_cast<const ObSysFunRawExpr *>(expr);
    uint64_t subprogram_id = OB_INVALID_ID;
    uint64_t var_idx = OB_INVALID_ID;
    CK (OB_NOT_NULL(f_expr) && f_expr->get_param_count() >= 3);
    GET_CONST_EXPR_VALUE(f_expr->get_param_expr(1), subprogram_id);
    GET_CONST_EXPR_VALUE(f_expr->get_param_expr(2), var_idx);
    OZ (check_subprogram_variable_read_only(
      current_block_->get_namespace(), subprogram_id, var_idx));
  }

#undef GET_CONST_EXPR_VALUE

  return ret;
}

int ObPLResolver::resolve_var(ObQualifiedName &q_name, ObPLBlockNS &ns,
                              ObRawExprFactory &expr_factory, const ObSQLSessionInfo *session_info,
                              ObPLCompileUnitAST &func, ObRawExpr *&expr, bool for_write)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObObjAccessIdx, 8> access_idxs;
  if (OB_FAIL(resolve_name(q_name, ns, expr_factory, session_info, access_idxs, func))) {
    LOG_IN_CHECK_MODE("failed to resolve symbol", K(q_name), K(ret));
    ret = (OB_ERR_SP_DOES_NOT_EXIST == ret
            || OB_ERR_FUNCTION_UNKNOWN == ret
            || OB_ERR_SP_WRONG_ARG_NUM == ret) ? OB_ERR_SP_UNDECLARED_VAR : ret;
  } else if (!ObObjAccessIdx::is_local_variable(access_idxs)
             && !ObObjAccessIdx::is_function_return_variable(access_idxs)
             && !ObObjAccessIdx::is_package_variable(access_idxs)
             && !ObObjAccessIdx::is_get_variable(access_idxs)
             && !ObObjAccessIdx::is_subprogram_variable(access_idxs)) {
    ret = OB_ERR_SP_UNDECLARED_VAR;
    LOG_WARN("failed to resolve var", K(q_name), K(access_idxs));
    LOG_USER_ERROR(OB_ERR_SP_UNDECLARED_VAR,
                   access_idxs.at(access_idxs.count()-1).var_name_.length(),
                   access_idxs.at(access_idxs.count()-1).var_name_.ptr());
  } else if (OB_FAIL(check_variable_accessible(ns, access_idxs, for_write))) {
    LOG_WARN("failed to check variable read only", K(ret), K(q_name), K(access_idxs));
  } else if (OB_FAIL(make_var_from_access(access_idxs, expr_factory, session_info,
                                          &resolve_ctx_.schema_guard_, ns, expr, for_write))) {
    LOG_WARN("failed to make var from access", K(ret), K(q_name), K(access_idxs));
  } else { /*do nothing*/ }

  if (OB_SUCC(ret)) {
    if (ObObjAccessIdx::is_package_variable(access_idxs)) {
      if (for_write) {
        OX (func.set_wps());
      } else {
        OX (func.set_rps());
      }
    } else if (ObObjAccessIdx::is_get_variable(access_idxs)) {
      OX (func.set_external_state());
    }
  }
  return ret;
}

int ObPLResolver::resolve_local_var(const ObString &var_name,
                                    ObPLBlockNS &ns,
                                    ObRawExprFactory &expr_factory,
                                    const ObSQLSessionInfo *session_info,
                                    ObSchemaGetterGuard *schema_guard,
                                    ObRawExpr *&expr,
                                    bool for_write)
{
  int ret = OB_SUCCESS;
  uint64_t parent_id = OB_INVALID_INDEX;
  int64_t var_index = OB_INVALID_INDEX;
  ObPLExternalNS::ExternalType type = ObPLExternalNS::INVALID_VAR;
  ObPLDataType pl_data_type;
  ObObjAccessIdx access_idx;
  if (OB_FAIL(ns.resolve_symbol(var_name, type, pl_data_type, parent_id, var_index))) {
    LOG_WARN("failed to get var index", K(var_name), K(ret));
  } else if (ObPLExternalNS::LOCAL_VAR != type) {
    ret = OB_ERR_SP_UNDECLARED_VAR;
    LOG_USER_ERROR(OB_ERR_SP_UNDECLARED_VAR, var_name.length(), var_name.ptr());
  } else {
    new(&access_idx)ObObjAccessIdx(pl_data_type,
                                   static_cast<ObObjAccessIdx::AccessType>(type),
                                   var_name,
                                   pl_data_type,
                                   var_index);
  }
  if (OB_SUCC(ret) && for_write) {
    const ObPLVar *var = NULL;
    const ObPLSymbolTable *symbol_table = NULL;
    CK (OB_NOT_NULL(symbol_table))
    CK (OB_NOT_NULL(var = symbol_table->get_symbol(var_index)));
    if (OB_SUCC(ret) && var->is_readonly()) {
      ret = OB_ERR_VARIABLE_IS_READONLY;
      LOG_WARN("variable is read only", K(ret), K(access_idx));
    }
  }
  if (OB_SUCC(ret)) {
    ObSEArray<ObObjAccessIdx, 8> access_idxs;
    if (OB_FAIL(access_idxs.push_back(access_idx))) {
      LOG_WARN("failed to resolve symbol", K(var_name), K(ret));
    } else if (OB_FAIL(make_var_from_access(access_idxs, expr_factory,
                                            session_info, schema_guard,
                                            ns, expr, for_write))) {
      LOG_WARN("failed to make var from access", K(var_name));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObPLResolver::resolve_local_var(const ParseNode &node,
                                    ObPLBlockNS &ns,
                                    ObRawExprFactory &expr_factory,
                                    const ObSQLSessionInfo *session_info,
                                    ObSchemaGetterGuard *schema_guard,
                                    ObRawExpr *&expr,
                                    bool for_write)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(resolve_local_var(ObString(node.str_len_, node.str_value_),
                                ns, expr_factory, session_info,
                                schema_guard, expr, for_write))) {
    LOG_WARN("failed to resolve_local_var", K(ret));
  }
  return ret;
}

int ObPLResolver::build_obj_access_func_name(const ObIArray<ObObjAccessIdx> &access_idxs,
                                             ObRawExprFactory &expr_factory,
                                             const ObSQLSessionInfo *session_info,
                                             ObSchemaGetterGuard *schema_guard,
                                             bool for_write,
                                             ObString &result)
{
  int ret = OB_SUCCESS;
  //函数名称格式为get_attr_idx_a
  ObSqlString buf;
  OZ (buf.append_fmt("%s", "get_attr"));
  if (for_write /*&& ObObjAccessIdx::is_contain_object_type(access_idxs)*/) {
    OZ (buf.append_fmt("%s", "_for_write"));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < access_idxs.count(); ++i) {
    if (ObObjAccessIdx::IS_TYPE_METHOD == access_idxs.at(i).access_type_) {
      continue;
    } else if (!access_idxs.at(i).var_name_.empty()) {
      OZ (buf.append_fmt("_var_name_%.*s",
                          access_idxs.at(i).var_name_.length(),
                          access_idxs.at(i).var_name_.ptr()),
                          i, access_idxs);
      if (OB_INVALID_INDEX != access_idxs.at(i).var_index_) {
        //如果按名字编码需要把idx也编进去，防止出现同名的情况
        OZ (buf.append_fmt("_%ld", access_idxs.at(i).var_index_),
                           i, access_idxs);
      }
    } else if (OB_INVALID_INDEX != access_idxs.at(i).var_index_) {
      OZ (buf.append_fmt("_var_index_%ld", access_idxs.at(i).var_index_),
                         i, access_idxs);
    } else if (NULL == access_idxs.at(i).get_sysfunc_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Cannot generate function name for Unexpected ObjAccess",
               K(access_idxs), K(ret));
    }
    if (NULL != access_idxs.at(i).get_sysfunc_) {
      OZ (buf.append_fmt("_get_sysfunc_"), K(i), K(access_idxs));
      if (OB_SUCC(ret)) {
        HEAP_VAR(char[OB_MAX_DEFAULT_VALUE_LENGTH], expr_str_buf) {
          MEMSET(expr_str_buf, 0, sizeof(expr_str_buf));
          int64_t pos = 0;
          ObRawExprPrinter expr_printer(
            expr_str_buf, OB_MAX_DEFAULT_VALUE_LENGTH, &pos, schema_guard, session_info->get_timezone_info());
          OZ (expr_printer.do_print(access_idxs.at(i).get_sysfunc_, T_NONE_SCOPE, true));
          OZ (buf.append_fmt("%.*s", static_cast<int32_t>(pos), expr_str_buf));
        }
        if (OB_SUCC(ret) && access_idxs.at(i).get_sysfunc_->is_udf_expr()) {
          //如果是UDF需要把id也编进去，防止出现同名的情况
          ObUDFRawExpr* udf_expr = static_cast<ObUDFRawExpr*>(access_idxs.at(i).get_sysfunc_);
          OZ (buf.append_fmt("_%ld_%ld", udf_expr->get_pkg_id(), udf_expr->get_udf_id()),
              i, access_idxs);
          for (int64_t j = 0; OB_SUCC(ret) && j < udf_expr->get_subprogram_path().count(); ++j) {
            OZ (buf.append_fmt("_%ld", udf_expr->get_subprogram_path().at(j)),
                               i, j, access_idxs);
          }
        }
      }
    }
  }
  OZ (ob_write_string(expr_factory.get_allocator(), buf.string(), result));
  return ret;
}

int ObPLResolver::make_var_from_access(const ObIArray<ObObjAccessIdx> &access_idxs,
                                       ObRawExprFactory &expr_factory,
                                       const ObSQLSessionInfo *session_info,
                                       ObSchemaGetterGuard *schema_guard,
                                       const ObPLBlockNS &ns,
                                       ObRawExpr *&expr,
                                       bool for_write)
{
  int ret = OB_SUCCESS;
  CK (!access_idxs.empty());
  if (OB_FAIL(ret)) {
  } else if (OB_LIKELY(ObObjAccessIdx::is_local_baisc_variable(access_idxs)
      || ObObjAccessIdx::is_local_refcursor_variable(access_idxs))) {
    ObConstRawExpr *c_expr = NULL;
    ObExprResType res_type;
    ObObjParam val;
    int pos = access_idxs.count() - 1;
    OZ (expr_factory.create_raw_expr(T_QUESTIONMARK, c_expr));
    CK (OB_NOT_NULL(c_expr));
    OX (val.set_unknown(access_idxs.at(pos).var_index_));
    OX (c_expr->set_value(val));
    if (OB_SUCC(ret)) {
      if (ObObjAccessIdx::is_local_baisc_variable(access_idxs)) {
        CK (OB_NOT_NULL(access_idxs.at(pos).elem_type_.get_data_type()));
        OX (res_type.set_meta(access_idxs.at(pos).elem_type_.get_data_type()->get_meta_type()));
        OX (res_type.set_accuracy(access_idxs.at(pos).elem_type_.get_data_type()->get_accuracy()));
      } else {
        OX (res_type.set_type(access_idxs.at(pos).elem_type_.get_obj_type()));
        OX (res_type.set_udt_id(access_idxs.at(pos).elem_type_.get_user_type_id()));
        OX (res_type.set_extend_type(access_idxs.at(pos).elem_type_.get_type()));
      }
    }
    OX (c_expr->set_result_type(res_type));
    OX (c_expr->set_enum_set_values(access_idxs.at(pos).elem_type_.get_type_info()));
    OZ (c_expr->add_flag(IS_DYNAMIC_PARAM));
    if (OB_SUCC(ret) && ob_is_enum_or_set_type(res_type.get_type())) {
      c_expr->add_flag(IS_ENUM_OR_SET);
    }
    OZ (c_expr->extract_info());
    OX (expr = c_expr);
  } else if (ObObjAccessIdx::is_package_baisc_variable(access_idxs)
             || ObObjAccessIdx::is_package_cursor_variable(access_idxs)
             || ObObjAccessIdx::is_subprogram_basic_variable(access_idxs)
             || ObObjAccessIdx::is_subprogram_cursor_variable(access_idxs)
             || ObObjAccessIdx::is_get_variable(access_idxs)) {
    CK (OB_NOT_NULL(expr = access_idxs.at(access_idxs.count() - 1).get_sysfunc_));
    OZ (expr->formalize(session_info));
    OZ (formalize_expr(*expr, session_info, ns), expr, access_idxs);
  } else {
    ObObjAccessRawExpr *obj_access_ref = NULL;

    OZ (expr_factory.create_raw_expr(T_OBJ_ACCESS_REF, obj_access_ref));
    CK (OB_NOT_NULL(obj_access_ref));
    OZ (obj_access_ref->add_access_indexs(access_idxs), K(access_idxs));

    OX (obj_access_ref->set_enum_set_values(access_idxs.at(access_idxs.count() - 1).elem_type_.get_type_info()));
    OZ (set_write_property(obj_access_ref, expr_factory, session_info, schema_guard, for_write));
    OZ (obj_access_ref->formalize(session_info));
    OZ (formalize_expr(*obj_access_ref, session_info, ns));
    OX (expr = obj_access_ref);
  }
  return ret;
}

int ObPLResolver::resolve_name(ObQualifiedName &q_name,
                               const ObPLBlockNS &ns,
                               ObRawExprFactory &expr_factory,
                               const ObSQLSessionInfo *session_info,
                               ObIArray<ObObjAccessIdx> &access_idxs,
                               ObPLCompileUnitAST &func)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(q_name.access_idents_.empty())) {
    if (lib::is_oracle_mode()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("qualified name is empty", K(ret));
    } else { //mysql兼容性
      if (!q_name.tbl_name_.empty()) {
        ret = OB_ERR_UNKNOWN_TABLE;
        LOG_USER_ERROR(OB_ERR_UNKNOWN_TABLE,
                       static_cast<int32_t>(q_name.tbl_name_.length()), q_name.tbl_name_.ptr(),
                       static_cast<int32_t>(q_name.database_name_.length()), q_name.database_name_.ptr());
      } else {
        ret = OB_ERR_BAD_FIELD_ERROR;
        LOG_USER_ERROR(OB_ERR_BAD_FIELD_ERROR,
                       static_cast<int32_t>(q_name.col_name_.length()), q_name.col_name_.ptr(),
                       static_cast<int32_t>(q_name.tbl_name_.length()), q_name.tbl_name_.ptr());
      }
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < q_name.access_idents_.count(); ++i) {
      ObObjAccessIdent &access_ident = q_name.access_idents_.at(i);
      if (OB_FAIL(resolve_access_ident(access_ident,
                                       ns,
                                       expr_factory,
                                       session_info,
                                       access_idxs,
                                       func,
                                       access_ident.is_pl_udf()))) {
        LOG_IN_CHECK_MODE("failed to resolve access ident", K(ret), K(i), K(q_name.access_idents_));
      }
    }
  }
  return ret;
}

// resolve external symbol direct
int ObPLResolver::resolve_access_ident(const ObObjAccessIdent &access_ident,
                                       ObPLExternalNS &external_ns,
                                       ObIArray<ObObjAccessIdx> &access_idxs)
{
  int ret = OB_SUCCESS;

  SET_LOG_CHECK_MODE();

  ObObjAccessIdx access_idx;
  uint64_t parent_id = OB_INVALID_INDEX;
  int64_t var_index = OB_INVALID_INDEX;
  ObPLExternalNS::ExternalType type = static_cast<ObPLExternalNS::ExternalType>(access_ident.access_index_);
  ObPLDataType pl_data_type;
  int64_t cnt = access_idxs.count();
  if (0 == cnt // 当前为根节点
      || ObObjAccessIdx::IS_DB_NS == access_idxs.at(cnt - 1).access_type_ // 父节点是DB Name
      || ObObjAccessIdx::IS_PKG_NS == access_idxs.at(cnt - 1).access_type_  // 父节点是Package Name
      || ObObjAccessIdx::IS_TABLE_NS == access_idxs.at(cnt - 1).access_type_) {
    if (cnt != 0) {
      if (ObObjAccessIdx::IS_DB_NS == access_idxs.at(cnt - 1).access_type_) {
        type = ObPLExternalNS::INVALID_VAR; // 父节点是DB Name, 子节点可能是Package Name或者Table Name
      } else if (ObObjAccessIdx::IS_PKG_NS == access_idxs.at(cnt - 1).access_type_) {
        type = ObPLExternalNS::PKG_VAR; // 父节点是PackageName, 子节点尝试���析为Package Var
      } else if (ObObjAccessIdx::IS_TABLE_NS == access_idxs.at(cnt - 1).access_type_) {
        type = ObPLExternalNS::TABLE_COL; // 父节点是TableName, 子节点尝试解析为ColumnName
      }
      parent_id = access_idxs.at(cnt - 1).var_index_;
    }
    OZ (external_ns.resolve_external_symbol(access_ident.access_name_,
                                            type,
                                            pl_data_type,
                                            parent_id,
                                            var_index), K(access_ident));
    if (ObPLExternalNS::INVALID_VAR == type) {
      ret = OB_ERR_SP_UNDECLARED_VAR;
      LOG_WARN("failed to resolve access ident",
               K(ret), K(access_ident), K(access_idxs));
    } else {
      new(&access_idx)ObObjAccessIdx(pl_data_type,
                                     static_cast<ObObjAccessIdx::AccessType>(type),
                                     access_ident.access_name_,
                                     pl_data_type,
                                     var_index);
    }
    OZ (access_idxs.push_back(access_idx));
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported condition in resovle_access_ident",
             K(ret), K(cnt), K(access_idxs.at(cnt - 1).access_type_));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "access type");
  }

  CANCLE_LOG_CHECK_MODE();

  return ret;
}

int ObPLResolver::check_is_udt_routine(const ObObjAccessIdent &access_ident, // 当前正在resolve的ident
                                       const ObPLBlockNS &ns,
                                       ObIArray<ObObjAccessIdx> &access_idxs,
                                       bool &is_routine)
{
  int ret = OB_SUCCESS;
  is_routine = false;
  uint64_t udt_id = OB_INVALID_ID;
  uint64_t cnt = access_idxs.count();
  if (cnt > 0 && access_idxs.at(cnt - 1).elem_type_.is_object_type()) {
    const ObPLDataType &parent_type = access_idxs.at(cnt - 1).elem_type_;
    const ObUserDefinedType *user_type = NULL;
    common::ObArray<ObRawExpr *> expr_params;
    ObProcType proc_type = INVALID_PROC_TYPE;
    bool is_exist = false;
    ObString database_name;
    ObString object_name;
    ObString routine_name = access_ident.access_name_;

    OZ (ns.get_pl_data_type_by_id(parent_type.get_user_type_id(), user_type));
    CK (OB_NOT_NULL(user_type));
    CK (user_type->is_udt_type() && (user_type->is_record_type() || user_type->is_opaque_type()));
    OZ (get_udt_database_name(resolve_ctx_.schema_guard_, parent_type.get_user_type_id(), database_name));
    OX (object_name = user_type->get_name());
    OZ (access_ident.extract_params(0, expr_params));
    OZ (ns.check_routine_exists(database_name, object_name, routine_name, ROUTINE_FUNCTION_TYPE, is_exist, proc_type, udt_id));
    OZ (!is_exist ? ns.check_routine_exists(database_name, object_name, routine_name, ROUTINE_PROCEDURE_TYPE, is_exist, proc_type, udt_id) : OB_SUCCESS);
    OX (is_routine = is_exist);
  }
  return ret;
}

int ObPLResolver::convert_pltype_to_restype(ObIAllocator &alloc,
                                            const ObPLDataType &pl_type,
                                            ObExprResType *&result_type)
{
  int ret = OB_SUCCESS;
  result_type = static_cast<ObExprResType *>(alloc.alloc(sizeof(ObExprResType)));
  const ObDataType *data_type = pl_type.get_data_type();
  if (OB_ISNULL(result_type)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  } else {
    new (result_type) ObExprResType(alloc);
    if (OB_ISNULL(data_type)) {
      result_type->set_ext();
      result_type->set_udt_id(pl_type.get_user_type_id());
      result_type->set_extend_type(pl_type.get_type());
    } else {
      result_type->set_type(data_type->get_obj_type());
      if (ob_is_string_tc(result_type->get_type())
          || ob_is_raw_tc(result_type->get_type())) {
        result_type->set_length(data_type->get_length());
        result_type->set_length_semantics(data_type->get_length_semantics());
        result_type->set_collation_type(data_type->get_collation_type());
        result_type->set_collation_level(data_type->get_collation_level());
      } else if (ob_is_number_tc(result_type->get_type()) ||
                 ob_is_interval_tc(result_type->get_type())) {
        result_type->set_precision(data_type->get_precision());
        result_type->set_scale(data_type->get_scale());
      } else if (ob_is_text_tc(result_type->get_type())
                || ob_is_lob_tc(result_type->get_type())) {
        result_type->set_length(data_type->get_length());
        result_type->set_collation_type(data_type->get_collation_type());
        result_type->set_collation_level(data_type->get_collation_level());
        result_type->set_scale(data_type->get_scale());
      }
    }
  }
  return ret;
}

int ObPLResolver::get_names_by_access_ident(ObObjAccessIdent &access_ident,
                                            ObIArray<ObObjAccessIdx> &access_idxs,
                                            ObString &database_name,
                                            ObString &package_name,
                                            ObString &routine_name)
{
  int ret = OB_SUCCESS;
  int64_t cnt = access_idxs.count();
  routine_name = access_ident.access_name_;
  if (cnt <= 0) {
    // do nothing ...
  } else if (ObObjAccessIdx::IS_PKG_NS == access_idxs.at(cnt - 1).access_type_
             || ObObjAccessIdx::IS_UDT_NS == access_idxs.at(cnt - 1).access_type_
             || ObObjAccessIdx::IS_LABEL_NS == access_idxs.at(cnt - 1).access_type_
             || ObObjAccessIdx::IS_DBLINK_PKG_NS == access_idxs.at(cnt-1).access_type_) {
    package_name = access_idxs.at(cnt - 1).var_name_;
    if (cnt >= 2) {
      OV (2 == cnt, OB_ERR_UNEXPECTED, K(cnt));
      OV (ObObjAccessIdx::IS_DB_NS == access_idxs.at(cnt - 2).access_type_, OB_ERR_UNEXPECTED, K(access_idxs.at(cnt - 2)));
      OX (database_name = access_idxs.at(cnt - 2).var_name_);
    }
  } else if (ObObjAccessIdx::IS_DB_NS == access_idxs.at(cnt - 1).access_type_) {
    database_name = access_idxs.at(cnt - 1).var_name_;
    OV (1 == cnt, OB_ERR_UNEXPECTED, K(cnt));
  } else if (access_idxs.at(cnt - 1).var_type_.is_object_type()) {
    OZ (get_udt_names(
      resolve_ctx_.schema_guard_, access_idxs.at(cnt - 1).var_type_.get_user_type_id(), database_name, package_name));
  } else {
    ret = OB_ERR_FUNCTION_UNKNOWN;
    LOG_WARN("unknow function invoke", K(ret), K(access_idxs), K(access_ident));
  }
  return ret;
}

int ObPLResolver::construct_name(ObString &database_name,
                                 ObString &package_name,
                                 ObString &routine_name,
                                 ObSqlString &object_name)
{
  int ret = OB_SUCCESS;
  if (!database_name.empty()) {
    OZ (object_name.append_fmt("%.*s.", database_name.length(), database_name.ptr()));
  }
  if (!package_name.empty()) {
    OZ (object_name.append_fmt("%.*s.", package_name.length(), package_name.ptr()));
  }
  CK (!routine_name.empty());
  OZ (object_name.append(routine_name));
  return ret;
}

int ObPLMockSelfArg::mock()
{
  int ret = OB_SUCCESS;
  if (access_idxs_.count() > 0 && expr_params_.count() > 0) {
    if (expr_params_.at(0)->get_expr_type() != T_SP_CPARAM) {
      // for compatible, here only try deduce, if has error, will report at later logic.
      IGNORE_RETURN expr_params_.at(0)->formalize(&session_info_);
    }
    if (expr_params_.at(0)->has_flag(IS_UDT_UDF_SELF_PARAM)) {
      // already has self argument, do nothing ...
    } else if (ObObjAccessIdx::IS_UDT_NS == access_idxs_.at(access_idxs_.count() - 1).access_type_
        && expr_params_.at(0)->get_result_type().get_expr_udt_id()
              == access_idxs_.at(access_idxs_.count() - 1).var_index_) {
      expr_params_.at(0)->add_flag(IS_UDT_UDF_SELF_PARAM);
      mocked_ = true;
      mark_only_ = true;
    } else if (access_idxs_.at(access_idxs_.count() - 1).elem_type_.is_composite_type()
                && expr_params_.at(0)->get_result_type().get_expr_udt_id()
                    == access_idxs_.at(access_idxs_.count() - 1).elem_type_.get_user_type_id()) {
      ObConstRawExpr *null_expr = NULL;
      OZ (expr_factory_.create_raw_expr(T_NULL, null_expr));
      CK (OB_NOT_NULL(null_expr));
      OZ (null_expr->add_flag(IS_UDT_UDF_SELF_PARAM));
      OZ (expr_params_.push_back(null_expr));
      OX (std::rotate(expr_params_.begin(), expr_params_.begin() + expr_params_.count() - 1, expr_params_.end()));
      OX (mocked_ = true);
    }
  }
  return ret;
}

ObPLMockSelfArg::~ObPLMockSelfArg()
{
  int ret = OB_SUCCESS;
  if (mocked_) {
    if (mark_only_) {
      if (OB_FAIL(expr_params_.at(0)->clear_flag(IS_UDT_UDF_SELF_PARAM))) {
        LOG_WARN("failed to clear flag", K(ret));
      }
    } else {
      std::rotate(expr_params_.begin(), expr_params_.begin() + 1, expr_params_.end());
      if (!expr_params_.at(expr_params_.count() - 1)->has_flag(IS_UDT_UDF_SELF_PARAM)) {
        LOG_ERROR("rotate failed", K(expr_params_));
      }
      expr_params_.pop_back();
    }
  }
}

int ObPLResolver::resolve_routine(ObObjAccessIdent &access_ident,
                                  const ObPLBlockNS &ns,
                                  ObIArray<ObObjAccessIdx> &access_idxs,
                                  ObPLCompileUnitAST &func)
{
  int ret = OB_SUCCESS;

  ObString database_name, package_name, routine_name;
  const ObIRoutineInfo *routine_info = NULL;
  ObSEArray<ObRawExpr*, 4> expr_params;
  ObProcType routine_type = access_ident.is_pl_udf() ? STANDALONE_FUNCTION : STANDALONE_PROCEDURE;
  bool is_dblink_pkg_ns = false;
  if (access_idxs.count() > 0) {
    is_dblink_pkg_ns = (ObObjAccessIdx::IS_DBLINK_PKG_NS == access_idxs.at(access_idxs.count()-1).access_type_);
  }
  OZ (get_names_by_access_ident(
    access_ident, access_idxs, database_name, package_name, routine_name));

  if (access_ident.is_pl_udf()) {
    OZ (ObRawExprUtils::rebuild_expr_params(access_ident.udf_info_, &expr_factory_, expr_params),
      K(access_ident), K(access_idxs));
  } else {
    OZ (access_ident.extract_params(0, expr_params));
  }

  if (database_name.empty() && package_name.empty() && 0 == routine_name.case_compare("RAISE_APPLICATION_ERROR")) {
    ObObjAccessIdx access_idx;
    if (expr_params.count() != 2 && expr_params.count() != 3) {
      ret = OB_ERR_WRONG_TYPE_FOR_VAR;
      LOG_WARN("PLS-00306: wrong number or types of arguments in call to 'RAISE_APPLICATION_ERROR'", K(ret));;
      LOG_USER_ERROR(OB_ERR_WRONG_TYPE_FOR_VAR, routine_name.length(), routine_name.ptr());
    } else {
      ObPLDataType invalid_pl_data_type;
      new(&access_idx)ObObjAccessIdx(invalid_pl_data_type,
                                     ObObjAccessIdx::AccessType::IS_SYSTEM_PROC,
                                     routine_name,
                                     invalid_pl_data_type);
      OZ (access_idxs.push_back(access_idx));
    }
  } else {

    {
      ObPLMockSelfArg self(access_idxs, expr_params, expr_factory_, resolve_ctx_.session_info_);
      OZ (self.mock());
      if (!is_dblink_pkg_ns) {
        OZ (ns.resolve_routine(resolve_ctx_,
                              database_name,
                              package_name,
                              routine_name,
                              expr_params,
                              routine_type,
                              routine_info));
        if (OB_SUCC(ret) && OB_INVALID_ID != routine_info->get_dblink_id()) {
          func.set_can_cached(false);
        }
      } else {
        OZ (ObPLResolver::resolve_dblink_routine_with_synonym(resolve_ctx_,
                                static_cast<uint64_t>(access_idxs.at(access_idxs.count()-1).var_index_),
                                routine_name, expr_params, routine_info));
        OX (func.set_can_cached(false));
      }
    }

    if (OB_FAIL(ret)
        && OB_ERR_UNEXPECTED != ret
        && OB_ERR_SP_WRONG_ARG_NUM != ret
        && OB_ERR_CALL_WRONG_ARG != ret
        && OB_ERR_FUNC_DUP != ret
        && OB_ERR_POSITIONAL_FOLLOW_NAME != ret
        && OB_ALLOCATE_MEMORY_FAILED != ret) {
      // Not A Routine, try compostie access again.
      if (access_idxs.count() > 0
          && (access_idxs.at(access_idxs.count() - 1)).elem_type_.is_composite_type()
          && OB_FAIL(resolve_composite_access(access_ident, access_idxs, ns, func))) {
        LOG_WARN("failed to access composite access", K(ret), K(access_ident), K(access_idxs));
      }
      if (OB_FAIL(ret)) {
        LOG_INFO("failed to resolve routine",
          K(ret), K(database_name), K(package_name), K(routine_name), K(routine_type), K(access_ident), K(access_idxs));
        ret = OB_ERR_FUNCTION_UNKNOWN;
        ObSqlString object_name;
        construct_name(database_name, package_name, routine_name, object_name);
        LOG_USER_ERROR(OB_ERR_FUNCTION_UNKNOWN,
                       access_ident.is_pl_udf() ? "FUNCTION" : "PROCEDURE",
                       object_name.string().length(), object_name.string().ptr());
      }
    } else { // find A routine, resolve it.
      CK (OB_NOT_NULL(routine_info));
#ifdef OB_BUILD_ORACLE_PL
      OZ (check_routine_callable(ns, access_idxs, expr_params, *routine_info));
#endif
      if (OB_FAIL(ret)) {
      } else if (OB_NOT_NULL(routine_info->get_ret_info())) {
        CK (access_ident.is_pl_udf());
        OZ (resolve_function(access_ident, access_idxs, routine_info, func));
      } else {
        OZ (resolve_procedure(access_ident, access_idxs, routine_info, routine_type));
      }
    }
  }
  return ret;
}

#ifdef OB_BUILD_ORACLE_PL
int ObPLResolver::check_routine_callable(const ObPLBlockNS &ns,
                                         ObIArray<ObObjAccessIdx> &access_idxs,
                                         ObIArray<ObRawExpr*> &expr_params,
                                         const ObIRoutineInfo &routine_info)
{
  int ret = OB_SUCCESS;
  if (routine_info.is_udt_static_routine()) {
    if (access_idxs.empty()) {
      uint64_t udt_id = OB_INVALID_ID;
      OZ (ObPLUDTObjectManager::check_routine_callable(ns, true, udt_id));
    } else {
      if (access_idxs.at(access_idxs.count() - 1).access_type_ != ObObjAccessIdx::IS_UDT_NS) {
        ret = OB_ERR_INVOKE_STATIC_BY_INSTANCE;
        LOG_WARN("invoke static udt function with instance", K(ret), K(access_idxs));
        LOG_USER_ERROR(OB_ERR_INVOKE_STATIC_BY_INSTANCE);
      }
    }
  } else if (routine_info.is_udt_routine()
             && !routine_info.is_udt_static_routine()
             && !routine_info.is_udt_cons()
             && !access_idxs.empty()) {
    if (ObObjAccessIdx::IS_UDT_NS == access_idxs.at(access_idxs.count() - 1).access_type_) {
      if (expr_params.count() > 0
          && expr_params.at(0)->get_result_type().get_udt_id()
              == access_idxs.at(access_idxs.count() - 1).var_index_) {
        if (OB_FAIL(expr_params.at(0)->clear_flag(IS_UDT_UDF_SELF_PARAM))) {
          LOG_WARN("failed to clear flag", K(ret));
        }
      } else if (expr_params.count() > 0
                 && expr_params.at(0)->get_result_type().is_xml_sql_type()
                 && (T_OBJ_XML == access_idxs.at(access_idxs.count() - 1).var_index_)) {
        // select 'head' || xmlparse(document '<a>123</a>').getclobval() into a from dual;
        if (OB_FAIL(expr_params.at(0)->clear_flag(IS_UDT_UDF_SELF_PARAM))) {
          LOG_WARN("failed to clear flag", K(ret));
        }
      } /*else if (expr_params.count() > 0
                 && expr_params.at(0)->get_expr_type() == T_QUESTIONMARK) {
        // do nothing ...
      } */ else {
        ret = OB_ERR_SP_WRONG_ARG_NUM;
        LOG_WARN("Incorrect number of arguments", K(ret));
      }
    }
  }
  return ret;
}
#endif

int ObPLResolver::resolve_function(ObObjAccessIdent &access_ident,
                                   ObIArray<ObObjAccessIdx> &access_idxs,
                                   const ObIRoutineInfo *routine_info,
                                   ObPLCompileUnitAST &func)
{
  int ret = OB_SUCCESS;

  ObPLDataType return_type;
  ObObjAccessIdx access_idx;
  CK (OB_NOT_NULL(routine_info));
  CK (OB_NOT_NULL(routine_info->get_ret_info()));
  if (OB_FAIL(ret)) {
  } else if (routine_info->get_ret_info()->is_schema_routine_param()) {
    const ObRoutineParam *iparam = static_cast<const ObRoutineParam*>(routine_info->get_ret_info());
    CK (OB_NOT_NULL(iparam));
    OZ (pl::ObPLDataType::transform_from_iparam(iparam,
                                                resolve_ctx_.schema_guard_,
                                                resolve_ctx_.session_info_,
                                                resolve_ctx_.allocator_,
                                                resolve_ctx_.sql_proxy_,
                                                return_type));
  } else {
    OX (return_type = routine_info->get_ret_info()->get_pl_data_type());
  }
  if (OB_SUCC(ret)
      && access_ident.udf_info_.ref_expr_->get_udf_id() == OB_INVALID_ID) {
    OZ (get_names_by_access_ident(access_ident,
                                  access_idxs,
                                  access_ident.udf_info_.udf_database_,
                                  access_ident.udf_info_.udf_package_,
                                  access_ident.udf_info_.udf_name_));
    OZ (resolve_udf_info(access_ident.udf_info_, access_idxs, func), K(access_ident));
    if (OB_SUCC(ret)
        && access_ident.udf_info_.is_new_keyword_used_
        && !access_ident.udf_info_.is_udf_udt_cons()) {
      ret = OB_ERR_PARSER_SYNTAX;
      LOG_WARN("NEW key word is only allowed for constructors", K(ret), K(access_ident));
    }
  }
  OX (new(&access_idx)ObObjAccessIdx(return_type,
                                     ObObjAccessIdx::AccessType::IS_UDF_NS,
                                     access_ident.udf_info_.udf_name_,
                                     return_type,
                                     reinterpret_cast<int64_t>(access_ident.udf_info_.ref_expr_)));
  OZ (access_idxs.push_back(access_idx));

  if (OB_SUCC(ret)
      && access_ident.is_pl_udf()
      && access_ident.params_.count() > access_ident.udf_info_.param_exprs_.count()) {
    OZ (build_return_access(access_ident, access_idxs, func));
  }
  return ret;
}

int ObPLResolver::resolve_procedure(ObObjAccessIdent &access_ident,
                                    ObIArray<ObObjAccessIdx> &access_idxs,
                                    const ObIRoutineInfo *routine_info,
                                    ObProcType routine_type)
{
  int ret = OB_SUCCESS;
  ObObjAccessIdx access_idx;
  ObObjAccessIdx::AccessType access_type;
  ObPLDataType invalid_type;
  switch (routine_type) {
    case STANDALONE_PROCEDURE: {
      access_type = ObObjAccessIdx::AccessType::IS_EXTERNAL_PROC;
    } break;
    case NESTED_PROCEDURE: {
      access_type = ObObjAccessIdx::AccessType::IS_NESTED_PROC;
    } break;
    default: {
      access_type = ObObjAccessIdx::AccessType::IS_INTERNAL_PROC;
    } break;
  }
  CK (OB_NOT_NULL(routine_info));
  OX (new (&access_idx) ObObjAccessIdx(invalid_type,
                                       access_type,
                                       access_ident.access_name_,
                                       invalid_type,
                                       reinterpret_cast<int64_t>(routine_info)));
  OZ (access_idxs.push_back(access_idx));
  return ret;
}

int ObPLResolver::resolve_construct(ObObjAccessIdent &access_ident,
                                    const ObPLBlockNS &ns,
                                    ObIArray<ObObjAccessIdx> &access_idxs,
                                    uint64_t user_type_id,
                                    ObPLCompileUnitAST &func)
{
  int ret = OB_SUCCESS;
  ObQualifiedName q_name;
  ObRawExpr* expr = NULL;
  const ObUserDefinedType *user_type = NULL;
  ObObjAccessIdx access_idx;
  OV (access_ident.is_pl_udf(), OB_ERR_UNEXPECTED, K(access_ident));
  OZ (ns.get_pl_data_type_by_id(user_type_id, user_type));
  CK (OB_NOT_NULL(user_type));
  if (OB_FAIL(ret)) {
  } else if (access_idxs.count() > 0) {
    OZ (get_names_by_access_ident(access_ident,
                                  access_idxs,
                                  access_ident.udf_info_.udf_database_,
                                  access_ident.udf_info_.udf_package_,
                                  access_ident.udf_info_.udf_name_));
    if (OB_SUCC(ret) &&
        !access_ident.udf_info_.udf_database_.empty() &&
        access_ident.udf_info_.udf_database_.case_compare(OB_SYS_DATABASE_NAME) != 0) {
      OZ (q_name.access_idents_.push_back(access_ident.udf_info_.udf_database_));
    }
    if (OB_SUCC(ret) && !access_ident.udf_info_.udf_package_.empty()) {
      OZ (q_name.access_idents_.push_back(access_ident.udf_info_.udf_package_));
    }
  } else {
    access_ident.udf_info_.udf_name_ = user_type->get_name();
  }
  OZ (q_name.access_idents_.push_back(access_ident));
  OZ (resolve_construct(q_name, access_ident.udf_info_, *user_type, expr));
  CK (OB_NOT_NULL(expr));
  OZ (func.add_expr(expr));
  OX (new(&access_idx)ObObjAccessIdx(*user_type,
                                     ObObjAccessIdx::AccessType::IS_UDF_NS,
                                     access_ident.access_name_,
                                     *user_type,
                                     reinterpret_cast<int64_t>(expr)));
  OZ (access_idxs.push_back(access_idx));
  if (OB_SUCC(ret)
      && access_ident.is_pl_udf()
      && access_ident.params_.count() > access_ident.udf_info_.param_exprs_.count()) {
    OZ (build_return_access(access_ident, access_idxs, func));
  }
  return ret;
}

int ObPLResolver::resolve_self_element_access(ObObjAccessIdent &access_ident,
                                              const ObPLBlockNS &ns,
                                              ObIArray<ObObjAccessIdx> &access_idxs,
                                              ObPLCompileUnitAST &func)
{
  int ret = OB_SUCCESS;
  const ObPLBlockNS *udt_routine_ns = ns.get_udt_routine_ns();

  if (0 == access_idxs.count() // Element Access Without Prefix [SELF.]
      && OB_NOT_NULL(udt_routine_ns) // In UDT Routine Namepspace
      && OB_NOT_NULL(udt_routine_ns->get_symbol_table()->get_self_param())) {

    ObObjAccessIdx self_access_idx;
    ObObjAccessIdx elem_access_idx;

    const ObUserDefinedType *self_user_type = NULL;
    const ObPLVar *self_var = udt_routine_ns->get_symbol_table()->get_self_param();
    ObPLDataType self_data_type = self_var->get_type();
    int64_t self_index = udt_routine_ns->get_symbol_table()->get_self_param_idx();
    uint64_t user_type_id = udt_routine_ns->get_package_id();

    // Construct A Self AccessIdx
    new (&self_access_idx) ObObjAccessIdx(self_data_type,
                                          udt_routine_ns == &ns
                                            ? ObObjAccessIdx::AccessType::IS_LOCAL
                                              : ObObjAccessIdx::AccessType::IS_SUBPROGRAM_VAR,
                                          self_var->get_name(),
                                          self_data_type,
                                          self_index);

    if (self_access_idx.is_subprogram_var()) {
      ObExprResType *result_type = NULL;
      CK (OB_NOT_NULL(udt_routine_ns));
      OZ (convert_pltype_to_restype(expr_factory_.get_allocator(), self_data_type, result_type));
      OX (self_access_idx.var_ns_ = udt_routine_ns);
      OZ (ObRawExprUtils::build_get_subprogram_var(expr_factory_,
                                                   udt_routine_ns->get_package_id(),
                                                   udt_routine_ns->get_routine_id(),
                                                   self_index,
                                                   result_type,
                                                   self_access_idx.get_sysfunc_,
                                                   &resolve_ctx_.session_info_));
    }
    OZ (ns.get_pl_data_type_by_id(self_data_type.get_user_type_id(), self_user_type));
    CK (OB_NOT_NULL(self_user_type));
    OZ (self_user_type->get_all_depended_user_type(resolve_ctx_, ns));

    // Resolve Self AccessIdx with AccessIdent
    OZ (udt_routine_ns->find_sub_attr_by_name(*self_user_type,
                                              access_ident,
                                              resolve_ctx_.session_info_,
                                              expr_factory_,
                                              func,
                                              elem_access_idx,
                                              self_data_type,
                                              user_type_id,
                                              self_index));
    OZ (access_idxs.push_back(self_access_idx));
    OZ (access_idxs.push_back(elem_access_idx));

    if (OB_FAIL(ret)) {
    } else if (elem_access_idx.elem_type_.is_composite_type() && !access_ident.params_.empty()) { //collection type
      OZ (build_collection_access(access_ident, access_idxs, func));
    } else if (elem_access_idx.elem_type_.is_obj_type() && !access_ident.params_.empty()) {
      ret = OB_ERR_NO_FUNCTION_EXIST;
      LOG_WARN("PLS-00222: no function with name 'string' exists in this scope", K(ret), K(access_ident));
    }
  } else {
    ret = OB_ERR_SP_UNDECLARED_VAR;
    LOG_IN_CHECK_MODE("undeclared var", K(access_ident), K(ret));
    if (lib::is_mysql_mode() || !resolve_ctx_.is_check_mode_) {
      LOG_USER_ERROR(OB_ERR_SP_UNDECLARED_VAR,
                     access_ident.access_name_.length(), access_ident.access_name_.ptr());
    }
  }
  return ret;
}

int ObPLResolver::build_access_idx_sys_func(uint64_t parent_id, ObObjAccessIdx &access_idx)
{
  int ret = OB_SUCCESS;
  if (access_idx.is_pkg() || access_idx.is_subprogram_var()) {
    ObExprResType *result_type = NULL;
    OZ (convert_pltype_to_restype(expr_factory_.get_allocator(), access_idx.elem_type_, result_type));
    if (OB_FAIL(ret)) {
    } else if (access_idx.is_pkg()) {
      OZ (ObRawExprUtils::build_get_package_var(expr_factory_,
                                                resolve_ctx_.schema_guard_,
                                                parent_id,
                                                access_idx.var_index_,
                                                result_type,
                                                access_idx.get_sysfunc_,
                                                &resolve_ctx_.session_info_));
    } else if (access_idx.is_subprogram_var()) {
      const ObPLBlockNS *ns = reinterpret_cast<const ObPLBlockNS*>(parent_id);;
      CK (OB_NOT_NULL(ns));
      OX (access_idx.var_ns_ = ns);
      OZ (ObRawExprUtils::build_get_subprogram_var(expr_factory_,
                                                   ns->get_package_id(),
                                                   ns->get_routine_id(),
                                                   access_idx.var_index_,
                                                   result_type,
                                                   access_idx.get_sysfunc_,
                                                   &resolve_ctx_.session_info_));
    }
  } else if (access_idx.is_user_var()) {
    OZ (ObRawExprUtils::build_get_user_var(expr_factory_,
                                           access_idx.var_name_,
                                           access_idx.get_sysfunc_,
                                           &resolve_ctx_.session_info_), K(access_idx));
  } else if (access_idx.is_session_var()) {
    OZ (ObRawExprUtils::build_get_sys_var(expr_factory_,
                                          access_idx.var_name_,
                                          ObObjAccessIdx::IS_GLOBAL == access_idx.access_type_
                                            ? ObSetVar::SET_SCOPE_GLOBAL : ObSetVar::SET_SCOPE_SESSION,
                                          access_idx.get_sysfunc_,
                                          &resolve_ctx_.session_info_), K(access_idx));
  }
  return ret;
}

int ObPLResolver::build_current_access_idx(uint64_t parent_id,
                                           ObObjAccessIdx &access_idx,
                                           ObObjAccessIdent &access_ident,
                                           const ObPLBlockNS &ns,
                                           ObIArray<ObObjAccessIdx> &access_idxs,
                                           ObPLCompileUnitAST &func)
{
  int ret = OB_SUCCESS;

  OZ (build_access_idx_sys_func(parent_id, access_idx));
  OZ (access_idxs.push_back(access_idx), K(access_idx));
  if (OB_FAIL(ret)) {
  } else if (access_idx.elem_type_.is_composite_type() && !access_ident.params_.empty()) { //collection type
    OZ (build_collection_access(access_ident, access_idxs, func));
  } else if (access_idx.elem_type_.is_obj_type() && !access_ident.params_.empty()) {
    ret = OB_ERR_NO_FUNCTION_EXIST;
    LOG_WARN("PLS-00222: no function with name 'string' exists in this scope",
               K(ret), K(access_idx.access_type_), K(access_ident));
  }
  return ret;
}

int ObPLResolver::build_collection_index_expr(ObObjAccessIdent &access_ident,
                                              ObObjAccessIdx &access_idx,
                                              const ObPLBlockNS &ns,
                                              ObIArray<ObObjAccessIdx> &access_idxs,
                                              const ObUserDefinedType &user_type,
                                              ObPLCompileUnitAST &func)
{
  int ret = OB_SUCCESS;
  if (!access_ident.params_.empty()
      && user_type.is_associative_array_type()) {
    ObObjAccessIdx index_access_idx;
    // associate array 会构造一个额外的表达式。所以调用build函数。例如aa('a');
    // 这里使用一个tmp,而不是传access_idx的原因是，
    // build会改变它的类型为IS_EXPR，这里预期需要的类型是IS_PROPERTY，
    // 否则的话，objaccess表达式获取的就是expr计算结果，没有collection地址了。
    OZ (build_collection_attribute_access(expr_factory_,
                                          &resolve_ctx_.session_info_,
                                          ns,
                                          func,
                                          user_type,
                                          access_idxs,
                                          OB_INVALID_INDEX,
                                          access_ident.params_.at(0).first, // parameter level
                                          index_access_idx));
    if (OB_SUCC(ret)) {
      ObPLAssocIndexRawExpr *index_expr = static_cast<ObPLAssocIndexRawExpr *>(index_access_idx.get_sysfunc_);
      CK (OB_NOT_NULL(index_expr));
      OX (index_expr->set_out_of_range_set_err(false));
      OX (access_idx.get_sysfunc_ = index_access_idx.get_sysfunc_);
      if (OB_FAIL(ret)) {
      } else if (0 == access_idx.var_name_.case_compare("PRIOR")) {
        index_expr->set_parent_type(parent_expr_type::EXPR_PRIOR);
      } else if (0 == access_idx.var_name_.case_compare("NEXT")) {
        index_expr->set_parent_type(parent_expr_type::EXPR_NEXT);
      } else if (0 == access_idx.var_name_.case_compare("EXISTS")) {
        index_expr->set_parent_type(parent_expr_type::EXPR_EXISTS);
      } else {
        index_expr->set_parent_type(parent_expr_type::EXPR_UNKNOWN);
      }
    }
  } else {
    // 这里暂时不调用build，因为a(1)这种会被build优化掉，
    // 没有param，get_attr的时候直接拿这个常量值，不符合next,prior的预期
    for (int64_t i = 0; OB_SUCC(ret) && i < access_idx.type_method_params_.count(); ++i) {
      uint64_t expr_idx = access_idx.type_method_params_.at(i);
      OV (expr_idx >= 0 && expr_idx < func.get_exprs().count(),
            OB_ERR_UNEXPECTED, K(expr_idx), K(func.get_exprs().count()));
      OX (access_idx.get_sysfunc_ = func.get_expr(expr_idx));
    }
  }
  return ret;
}


int ObPLResolver::resolve_composite_access(ObObjAccessIdent &access_ident,
                                           ObIArray<ObObjAccessIdx> &access_idxs,
                                           const ObPLBlockNS &ns,
                                           ObPLCompileUnitAST &func)
{
  int ret = OB_SUCCESS;

  const ObPLDataType &parent_type = access_idxs.at(access_idxs.count() - 1).elem_type_;
  const ObUserDefinedType *user_type = NULL;
  ObObjAccessIdx access_idx;
  ObPLDataType pl_data_type;
  uint64_t parent_id;
  int64_t var_index;

  if (!parent_type.is_composite_type()) {
    ret = OB_ERR_OUT_OF_SCOPE;
    LOG_WARN("PLS-00225: subprogram or cursor reference is out of scope", K(ret), K(parent_type), K(access_ident));
  }

  OZ (ns.get_pl_data_type_by_id(parent_type.get_user_type_id(), user_type));
  CK (OB_NOT_NULL(user_type));
  OZ (user_type->get_all_depended_user_type(resolve_ctx_, ns));

  if (OB_FAIL(ret)) {
  } else if (access_ident.access_name_.empty()) { // no element name, must be collection access.
    OV (!access_ident.params_.empty(), OB_ERR_UNEXPECTED, K(access_ident), K(access_idxs));
    OZ (build_collection_access(access_ident, access_idxs, func));
  } else { // record element access
    OZ (ns.find_sub_attr_by_name(*user_type,
                                 access_ident,
                                 resolve_ctx_.session_info_,
                                 expr_factory_,
                                 func,
                                 access_idx,
                                 pl_data_type,
                                 parent_id,
                                 var_index));

    OZ (build_access_idx_sys_func(parent_id, access_idx));

    if (OB_FAIL(ret)) {
    } else if (0 == access_idx.var_name_.case_compare("NEXT")
              || 0 == access_idx.var_name_.case_compare("PRIOR")
              || 0 == access_idx.var_name_.case_compare("EXISTS")) {

      CK (access_idx.type_method_params_.count() <= 1); // parameter count must be 0 or 1

      OZ (build_collection_index_expr(access_ident, access_idx, ns, access_idxs, *user_type, func));
    }

    OZ (access_idxs.push_back(access_idx));

    if (OB_SUCC(ret)
        && !access_idx.is_property()
        && !access_idx.is_type_method()
        && !access_ident.params_.empty()) {
      // second level collection access. "X(1)(2)", here continue resolve "(2)"
      OZ (build_collection_access(access_ident, access_idxs, func));
    }
  }
  return ret;
}

int ObPLResolver::resolve_sys_func_access(ObObjAccessIdent &access_ident,
                                          ObIArray<ObObjAccessIdx> &access_idxs,
                                          const ObSQLSessionInfo *session_info,
                                          const ObPLBlockNS &ns)
{
  int ret = OB_SUCCESS;
  ObObjAccessIdx access_idx;
  const ObUserDefinedType *user_type = NULL;
  CK (OB_NOT_NULL(access_ident.sys_func_expr_));
  if (OB_SUCC(ret)) {
    if (access_ident.sys_func_expr_->get_result_type().get_type() == ObMaxType) {
      if (OB_FAIL(access_ident.sys_func_expr_->formalize(session_info))) {
        LOG_WARN("deduce type failed for sys func ident", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (!access_ident.sys_func_expr_->get_result_type().is_xml_sql_type()
                && !(access_ident.sys_func_expr_->get_result_type().is_ext()
                     && access_ident.sys_func_expr_->get_result_type().get_udt_id() == T_OBJ_XML)) {
      ret = OB_ERR_NOT_OBJ_REF;
      LOG_WARN("unsupported sys func ident",
        K(ret), K(access_ident), K(access_ident.sys_func_expr_->get_result_type()),
        K(access_ident.sys_func_expr_->get_result_type().get_udt_id()));
    } else { // only xmltype is supported
      OZ (ns.get_pl_data_type_by_id(T_OBJ_XML, user_type));
    }
  }
  CK (OB_NOT_NULL(user_type));
  OX (new (&access_idx) ObObjAccessIdx(*user_type,
                                        ObObjAccessIdx::AccessType::IS_UDF_NS,
                                        access_ident.access_name_,
                                        *user_type,
                                        reinterpret_cast<int64_t>(access_ident.sys_func_expr_)));
  OZ (access_idxs.push_back(access_idx));
  return ret;
}


int ObPLResolver::resolve_access_ident(ObObjAccessIdent &access_ident, // 当前正在resolve的ident
                                       const ObPLBlockNS &ns,
                                       ObRawExprFactory &expr_factory,
                                       const ObSQLSessionInfo *session_info,
                                       ObIArray<ObObjAccessIdx> &access_idxs, // 已经resolve过的ident信息, 作为当前ident的父节点
                                       ObPLCompileUnitAST &func,
                                       bool is_routine,
                                       bool is_resolve_rowtype)
{
  int ret = OB_SUCCESS;

  SET_LOG_CHECK_MODE();

  ObObjAccessIdx access_idx;
  uint64_t parent_id = OB_INVALID_INDEX;
  int64_t var_index = OB_INVALID_INDEX;
  ObPLExternalNS::ExternalType type = static_cast<ObPLExternalNS::ExternalType>(access_ident.access_index_);
  ObPLDataType pl_data_type;
  int64_t cnt = access_idxs.count();

  if (!is_routine) {
    OZ (check_is_udt_routine(access_ident, ns, access_idxs, is_routine));
  }

  if (0 == cnt && access_ident.is_sys_func()) {
    OZ (resolve_sys_func_access(access_ident, access_idxs, session_info, ns));
  } else if (0 == cnt
      || ObObjAccessIdx::IS_DB_NS == access_idxs.at(cnt - 1).access_type_
      || ObObjAccessIdx::IS_PKG_NS == access_idxs.at(cnt - 1).access_type_
      || ObObjAccessIdx::IS_TABLE_NS == access_idxs.at(cnt - 1).access_type_
      || ObObjAccessIdx::IS_LABEL_NS == access_idxs.at(cnt - 1).access_type_
      || ObObjAccessIdx::IS_UDT_NS == access_idxs.at(cnt - 1).access_type_
      || ObObjAccessIdx::IS_DBLINK_PKG_NS == access_idxs.at(cnt-1).access_type_
      || is_routine) {
    bool label_symbol = false;
    if (cnt != 0) {
      if (ObObjAccessIdx::IS_DB_NS == access_idxs.at(cnt - 1).access_type_) {
        type = ObPLExternalNS::INVALID_VAR; // 父节点是DB Name, 子节点可能是Package Name或者Table Name
      } else if (ObObjAccessIdx::IS_PKG_NS == access_idxs.at(cnt - 1).access_type_) {
        type = ObPLExternalNS::PKG_VAR; // 父节点是PackageName, 子节点尝试解析为Package Var
      } else if (ObObjAccessIdx::IS_TABLE_NS == access_idxs.at(cnt - 1).access_type_ && !is_routine) {
        type = ObPLExternalNS::TABLE_COL; // 父节点是TableName, 子节点尝试解析为ColumnName
      } else if (ObObjAccessIdx::IS_LABEL_NS == access_idxs.at(cnt - 1).access_type_) {
        label_symbol = true;
      }
      if (!label_symbol) {
        parent_id = access_idxs.at(cnt - 1).var_index_;
      }
    }
    if (label_symbol) {
      OZ (ns.resolve_label_symbol(access_ident.access_name_,
                                  type,
                                  pl_data_type,
                                  access_idxs.at(cnt - 1).label_ns_,
                                  var_index), K(access_ident.access_name_));
      OX (parent_id =
        ObPLExternalNS::ExternalType::SUBPROGRAM_VAR == type
          ? reinterpret_cast<uint64_t>(access_idxs.at(cnt - 1).label_ns_)
            : access_idxs.at(cnt - 1).label_ns_->get_package_id());
    } else if (0 != cnt
               && ObObjAccessIdx::IS_DBLINK_PKG_NS == access_idxs.at(cnt-1).access_type_ && !is_routine) {
      OZ (resolve_dblink_type_with_synonym(parent_id, access_ident.access_name_, func, pl_data_type));
      OX (type = ObPLExternalNS::PKG_TYPE);
      OX (var_index = pl_data_type.get_user_type_id());
    } else {
      if (access_ident.is_pl_var() && access_ident.access_name_.empty()) { // questionmark variable
        const ObPLSymbolTable *sym_tbl = ns.get_symbol_table();
        int64_t var_idx = access_ident.access_index_;
        const ObPLVar *local_var = NULL;
        CK (OB_NOT_NULL(sym_tbl));
        if (OB_SUCC(ret) && var_idx >= 0 && var_idx < sym_tbl->get_count()) {
          CK (OB_NOT_NULL(local_var = sym_tbl->get_symbol(var_idx)));
          OX (pl_data_type = local_var->get_type());
          OX (type = ObPLExternalNS::LOCAL_VAR);
          OX (var_index = var_idx);
        }
      } else {
        OZ (ns.resolve_symbol(access_ident.access_name_,
                              type,
                              pl_data_type,
                              parent_id,
                              var_index), K(access_ident));
      }
      if (OB_SUCC(ret) && is_resolve_rowtype && ObPLExternalNS::LOCAL_VAR == type) {
        const ObPLSymbolTable &sym_table = func.get_symbol_table();
        const ObPLVar *var = sym_table.get_symbol(var_index);
        if (OB_NOT_NULL(var) && var->is_formal_param()) {
          ret = OB_ERR_TYPE_DECL_ILLEGAL;
          LOG_WARN("row type illegal, should not be formal parameter", K(ret), KPC(var), K(access_ident), K(var_index));
          LOG_USER_ERROR(OB_ERR_TYPE_DECL_ILLEGAL, access_ident.access_name_.length(), access_ident.access_name_.ptr());
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if ((ObPLExternalNS::LOCAL_TYPE == type || ObPLExternalNS::PKG_TYPE == type || ObPLExternalNS::UDT_NS == type)
                && (is_routine || (access_ident.has_brackets_))) {
      OZ (resolve_construct(access_ident, ns, access_idxs, var_index, func),
        K(is_routine), K(is_resolve_rowtype), K(type),
        K(pl_data_type), K(var_index), K(access_ident), K(access_idxs));
    } else if (ObPLExternalNS::INVALID_VAR == type
               || (ObPLExternalNS::SELF_ATTRIBUTE == type)
               || (ObPLExternalNS::LOCAL_VAR == type && is_routine)
               || (ObPLExternalNS::TABLE_NS == type && is_routine)
               || (ObPLExternalNS::LABEL_NS == type && is_routine)
               || (ObPLExternalNS::DB_NS == type && is_routine)
               || (ObPLExternalNS::DBLINK_PKG_NS == type && is_routine)) {
      if (is_routine) {
        OZ (resolve_routine(access_ident, ns, access_idxs, func),
          K(access_ident), K(access_idxs));
      } else { // [self.]element, user can access element without self prefix, handle it in here.
        OZ (resolve_self_element_access(access_ident, ns, access_idxs, func),
          K(access_ident), K(access_idxs));
      }
    } else { // current symbol resolve success!
      ObObjAccessIdx access_idx;
      new(&access_idx)ObObjAccessIdx(pl_data_type,
                                     static_cast<ObObjAccessIdx::AccessType>(type),
                                     access_ident.access_name_,
                                     pl_data_type,
                                     var_index);
      OZ (build_access_idx_sys_func(parent_id, access_idx));
      OZ (access_idxs.push_back(access_idx), K(access_idx));
      if (OB_FAIL(ret)) {
      } else if (access_idx.elem_type_.is_composite_type() && !access_ident.params_.empty()) { //collection type
        OZ (build_collection_access(access_ident, access_idxs, func));
      } else if (access_idx.elem_type_.is_obj_type() && !access_ident.params_.empty()) {
        ret = OB_ERR_NO_FUNCTION_EXIST;
        LOG_WARN("PLS-00222: no function with name 'string' exists in this scope",
                   K(ret), K(access_idx.access_type_), K(access_ident));
      }
    }
  } else {
    // not top node and parent not a namespace, it must be composite access. handle it here.
    OZ (resolve_composite_access(access_ident, access_idxs, ns, func), K(access_ident), K(access_idxs));
  }

  CANCLE_LOG_CHECK_MODE();

  return ret;
}

int ObPLResolver::build_collection_attribute_access(ObRawExprFactory &expr_factory,
                                                    const ObSQLSessionInfo *session_info,
                                                    const ObPLBlockNS &ns,
                                                    ObPLCompileUnitAST &func,
                                                    const ObUserDefinedType &user_type,
                                                    ObIArray<ObObjAccessIdx> &access_idxs,
                                                    int64_t attr_index,
                                                    ObRawExpr *func_expr,
                                                    ObObjAccessIdx &access_idx)
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_ORACLE_PL
  UNUSEDx(expr_factory, session_info, ns, func, user_type, access_idxs, attr_index, func_expr, access_idx);
#else
  if (user_type.is_nested_table_type() || user_type.is_varray_type()) {
    const ObCollectionType &table_type = static_cast<const ObCollectionType &>(user_type);
    ObString empty_name;
    new(&access_idx)ObObjAccessIdx(table_type.get_element_type(),
                                   NULL == func_expr ?
                                   ObObjAccessIdx::AccessType::IS_CONST : ObObjAccessIdx::AccessType::IS_EXPR,
                                   empty_name,
                                   table_type.get_element_type(),
                                   NULL == func_expr ? attr_index : reinterpret_cast<int64_t>(func_expr));
    if (NULL != func_expr && func_expr->is_const_raw_expr()) {
      //如果是NestedTable，且括号里的参数是Const，那么不把这个Const作为param_expr处理，这是一个优化
      const ObConstRawExpr* const_expr = static_cast<const ObConstRawExpr*>(func_expr);
      CK (OB_NOT_NULL(const_expr));
      if (OB_FAIL(ret)) {
      } else if (func_expr->get_expr_type() != T_QUESTIONMARK) {
        const ObObj &const_obj = const_expr->get_value();
        int64_t var_index = OB_INVALID_INDEX;
        if (const_obj.is_integer_type()) {
          var_index = const_obj.get_int();
        } else if (const_obj.is_number()) {
          int64_t int_value  = OB_INVALID_INDEX;
          if (!const_obj.get_number().is_valid_int64(int_value)) {
            number::ObNumber number = const_obj.get_number();
            if (OB_FAIL(number.round(0))) {
              LOG_WARN("failed to round number value", K(ret), K(number));
            } else if (!number.is_valid_int64(int_value)) {
              ret = OB_ARRAY_OUT_OF_RANGE;
              LOG_WARN("invalid const type for array index", K(const_obj), K(ret));
            } else {
              var_index = int_value;
            }
          } else {
            var_index = int_value;
          }
        }
        if (OB_SUCC(ret) && var_index > 0) {
          access_idx.access_type_ = ObObjAccessIdx::AccessType::IS_CONST;
          access_idx.var_index_  = var_index;
          access_idx.get_sysfunc_ = NULL;
        }
      } else {
        const ObPLVar *var = NULL;
        CK (OB_NOT_NULL(ns.get_symbol_table()));
        CK (OB_NOT_NULL(var = ns.get_symbol_table()->get_symbol(const_expr->get_value().get_unknown())));
        if (OB_SUCC(ret)
            && (ob_is_int_tc(var->get_type().get_obj_type())
                || ob_is_uint_tc(var->get_type().get_obj_type())
                || ob_is_number_tc(var->get_type().get_obj_type()))) {
          access_idx.access_type_ = ObObjAccessIdx::AccessType::IS_LOCAL;
          access_idx.var_index_ = const_expr->get_value().get_unknown();
          access_idx.var_name_ = ns.get_symbol_table()->get_symbol(access_idx.var_index_)->get_name();
        }
      }
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(access_idx.get_sysfunc_)) {
      OZ (formalize_expr(*(access_idx.get_sysfunc_)));
      if (OB_FAIL(ret)) {
      } else if (access_idx.get_sysfunc_->get_result_type().is_ext()) {
        ret = OB_ERR_EXPRESSION_WRONG_TYPE;
        LOG_WARN("PLS-00382: expression is of wrong type",
                 K(ret), K(access_idx.get_sysfunc_->get_result_type()));
      } else {
        OZ (ObRawExprUtils::build_column_conv_expr(
        &resolve_ctx_.session_info_,
        expr_factory_,
        ObInt32Type,
        CS_TYPE_INVALID,
        (ObAccuracy::DDL_DEFAULT_ACCURACY2[lib::is_oracle_mode()][ObInt32Type]).accuracy_,
        true,
        NULL, /*"db_name."tb_name"."col_name"*/
        NULL,
        access_idx.get_sysfunc_,
        true/*+is_in_pl*/));
        CK (OB_NOT_NULL(access_idx.get_sysfunc_));
        OZ (formalize_expr(*(access_idx.get_sysfunc_)));
      }
    }
  } else if (user_type.is_associative_array_type()) {
    const ObAssocArrayType &assoc_type = static_cast<const ObAssocArrayType&>(user_type);
    ObPLAssocIndexRawExpr *result_expr = NULL;
    if (OB_FAIL(expr_factory.create_raw_expr(T_FUN_PL_ASSOCIATIVE_INDEX, result_expr))) {
      LOG_WARN("create ObOpRawExpr failed", K(ret));
    } else if (OB_ISNULL(result_expr)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else {
      ObRawExpr *collection_expr = NULL;
      if (OB_FAIL(make_var_from_access(access_idxs, expr_factory, session_info,
                                       &resolve_ctx_.schema_guard_, ns, collection_expr))) {
        LOG_WARN("failed to make var from access", K(access_idxs), K(ret));
      } else if (OB_ISNULL(collection_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to make_var_from_access", K(user_type), K(access_idxs), K(ret));
      } else if (!collection_expr->is_obj_access_expr()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid collection expr", K(*collection_expr), K(user_type), K(access_idxs), K(ret));
      } else if (OB_FAIL(func.add_obj_access_expr(collection_expr))) {
        LOG_WARN("push back error", K(*collection_expr));
      } else if (OB_FAIL(result_expr->add_param_expr(collection_expr))) {
        LOG_WARN("failed to add param expr", K(ret));
      } else {
        ObRawExpr *key_expr = NULL;
        int64_t idx_value  = OB_INVALID_INDEX;
        if (NULL == func_expr) {
          idx_value = attr_index;
        } else if (func_expr->is_const_raw_expr()) {
          const ObObj &const_obj = static_cast<const ObConstRawExpr*>(func_expr)->get_value();
          if (const_obj.is_integer_type()) {
            idx_value = const_obj.get_int();
          } else if (const_obj.is_number()) {
            if (!const_obj.get_number().is_valid_int64(idx_value)) {
              idx_value  = OB_INVALID_INDEX;
            }
          } else { /*do nothing*/ }
        } else { /*do nothing*/ }

        if (OB_SUCC(ret)) {
          if (OB_INVALID_INDEX == idx_value) {
            key_expr = func_expr;
          } else {
            ObConstRawExpr *const_expr = NULL;
            if (OB_FAIL(ObRawExprUtils::build_const_int_expr(expr_factory, ObIntType, idx_value, const_expr))) {
              LOG_WARN("failed to build_const_int_expr", K(ret));
            } else {
              key_expr = const_expr;
            }
          }
          if (OB_SUCC(ret)) {
            CK (OB_NOT_NULL(key_expr));
            CK (OB_NOT_NULL(assoc_type.get_index_type().get_data_type()));
            CK (OB_NOT_NULL(assoc_type.get_index_type().get_meta_type()));
            if (OB_FAIL(ret)) {
            } else if (OB_FAIL(key_expr->formalize(&(resolve_ctx_.session_info_)))) {
              LOG_WARN("failed to formalize key expr", K(ret), KPC(key_expr));
            } else if (T_NULL == key_expr->get_expr_type()) {
              // do nothing
            } else if (key_expr->get_result_type().get_obj_meta()
                          == *assoc_type.get_index_type().get_meta_type()
                       && key_expr->get_result_type().get_accuracy()
                          == assoc_type.get_index_type().get_data_type()->get_accuracy()) {
              //do nothing
            } else if (key_expr->get_result_type().get_obj_meta()
                          == *assoc_type.get_index_type().get_meta_type()
                       && assoc_type.get_index_type().get_meta_type()->is_integer_type()) {
              //如果是integer类型，不做类型转换，只检查值域
              if (lib::is_oracle_mode()
                  && assoc_type.get_index_type().is_pl_integer_type()
                  && is_need_add_checker(
                      assoc_type.get_index_type().get_pl_integer_type(),
                      key_expr)
                  && OB_FAIL(add_pl_integer_checker_expr(
                            expr_factory_,
                            PL_SIMPLE_INTEGER == assoc_type.get_index_type().get_pl_integer_type()
                              ? PL_PLS_INTEGER : assoc_type.get_index_type().get_pl_integer_type(),
                            assoc_type.get_index_type().get_lower(),
                            assoc_type.get_index_type().get_upper(),
                            key_expr))) {
                LOG_WARN("failed to add pl integer checker expr", K(ret), KPC(key_expr));
              } else if (OB_FAIL(key_expr->formalize(&(resolve_ctx_.session_info_)))) {
                LOG_WARN("failed to formalize expr", K(ret));
              }
            } else if (!cast_supported(key_expr->get_result_type().get_type(),
                          key_expr->get_result_type().get_collation_type(),
                          assoc_type.get_index_type().get_data_type()->get_obj_type(),
                          assoc_type.get_index_type().get_data_type()->get_collation_type())) {
              ret = OB_ERR_INVALID_TYPE_FOR_OP;
              LOG_WARN("transition does not support",
                       K(ret),
                       K(key_expr->get_result_type()),
                       K(ob_obj_type_str(assoc_type.get_index_type().get_obj_type())),
                       KPC(key_expr));
            } else {
              if (OB_FAIL(ObRawExprUtils::build_column_conv_expr(
                          &resolve_ctx_.session_info_,
                          expr_factory_,
                          assoc_type.get_index_type().get_obj_type(),
                          assoc_type.get_index_type().get_data_type()->get_collation_type(),
                          assoc_type.get_index_type().get_data_type()->get_accuracy_value(),
                          true,
                          NULL, /*"db_name"."tb_name"."col_name"*/
                          &assoc_type.get_index_type().get_type_info(),
                          key_expr,
                          true /*is_in_pl*/))) {
                LOG_WARN("fail to build column conv expr", K(ret));
              } else if (lib::is_oracle_mode()
                        && assoc_type.get_index_type().is_pl_integer_type()
                        && OB_FAIL(add_pl_integer_checker_expr(
                                                expr_factory_,
                                                assoc_type.get_index_type().get_pl_integer_type(),
                                                assoc_type.get_index_type().get_lower(),
                                                assoc_type.get_index_type().get_upper(),
                                                key_expr))) {
                LOG_WARN("failed to add pl integer checker expr", K(ret));
              } else if (OB_FAIL(key_expr->formalize(&(resolve_ctx_.session_info_)))) {
                LOG_WARN("failed to formalize expr", K(ret));
              } else { /*do nothing*/ }
            }
          }
        }

        if (OB_SUCC(ret)) {
          if (OB_FAIL(result_expr->add_param_expr(key_expr))) {
            LOG_WARN("failed to add param expr", K(ret));
          } else if (assoc_type.get_index_type().get_meta_type()->is_string_type()) {
            result_expr->set_is_index_by_varchar(true);
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      ObString empty_name;
      new(&access_idx)ObObjAccessIdx(assoc_type.get_element_type(),
                                     ObObjAccessIdx::AccessType::IS_EXPR,
                                     empty_name,
                                     assoc_type.get_element_type(),
                                     reinterpret_cast<int64_t>(result_expr));
      access_idx.var_index_ = attr_index;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid user type", K(user_type), K(ret));
  }
#endif
  return ret;
}

int ObPLResolver::build_collection_access(ObObjAccessIdent &access_ident,
                                        ObIArray<ObObjAccessIdx> &access_idxs,
                                        ObPLCompileUnitAST &func,
                                        int64_t start_level)
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_ORACLE_PL
  UNUSEDx(access_ident, access_idxs, func, start_level);
#else
  CK (OB_NOT_NULL(current_block_));
  if (OB_SUCC(ret)) {
    const ObSQLSessionInfo &session_info = resolve_ctx_.session_info_;
    ObPLBlockNS &ns = current_block_->get_namespace();
    ObObjAccessIdx access_idx;
    ObPLDataType parent_type;
    int64_t param_level = start_level - 1;
    OX (parent_type = ObObjAccessIdx::get_final_type(access_idxs));
    for (int64_t i = 0; OB_SUCC(ret) && i < access_ident.params_.count(); ++i) {
      if (access_ident.params_.at(i).second >= start_level) {
        if (param_level == access_ident.params_.at(i).second) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("cannot access Collection by more than 1 index", K(access_ident), K(ret));
        } else if (!parent_type.is_collection_type()) {
          ret = OB_ERR_PL_COMMON;
          LOG_WARN("cannot access non collection with parameter", K(ret));
          LOG_USER_ERROR(OB_ERR_PL_COMMON, "can not access non collection with paremeter");
        } else {
          CK (param_level + 1 == access_ident.params_.at(i).second);
          if (OB_SUCC(ret)) {
            param_level = access_ident.params_.at(i).second;
            const ObUserDefinedType *user_type = NULL;
            OZ (ns.get_user_type(parent_type.get_user_type_id(), user_type));
            CK (OB_NOT_NULL(user_type));
            OZ (user_type->get_all_depended_user_type(resolve_ctx_, ns));
            OX (parent_type = static_cast<const ObCollectionType*>(user_type)->get_element_type());
            OZ (build_collection_attribute_access(expr_factory_,
                                                          &session_info,
                                                          ns,
                                                          func,
                                                          *user_type,
                                                          access_idxs,
                                                          OB_INVALID_INDEX,
                                                          access_ident.params_.at(i).first,
                                                          access_idx));
            OZ (access_idxs.push_back(access_idx));
          }
        }
      }
    }
  }
#endif
  return ret;
}

int ObPLResolver::build_return_access(ObObjAccessIdent &access_ident,
                                          ObIArray<ObObjAccessIdx> &access_idxs,
                                          ObPLCompileUnitAST &func)
{
  return build_collection_access(access_ident, access_idxs, func, 1/*start level = 1*/);
}

int ObPLResolver::resolve_into(const ParseNode *into_node, ObPLInto &into, ObPLFunctionAST &func)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(into_node));
  CK (OB_NOT_NULL(current_block_));
  CK (OB_LIKELY(T_INTO_VARIABLES == into_node->type_));
  CK (OB_LIKELY(1 == into_node->num_child_));
  CK (OB_NOT_NULL(into_node->children_[0]));
  CK (OB_LIKELY(T_SP_INTO_LIST == into_node->children_[0]->type_));
  if (OB_SUCC(ret)) {
    const ParseNode *into_list = into_node->children_[0];
    for (int64_t i = 0; OB_SUCC(ret) && i < into_list->num_child_; ++i) {
      ObQualifiedName q_name;
      ObRawExpr* expr = NULL;
      CK (OB_NOT_NULL(into_list->children_[i]))
      if (OB_FAIL(ret)) {
      } else if (T_SP_OBJ_ACCESS_REF == into_list->children_[i]->type_/*Oracle mode*/) {
        OZ (resolve_obj_access_idents(*into_list->children_[i], q_name.access_idents_, func));
      } else if (T_QUESTIONMARK == into_list->children_[i]->type_ /*Oracle mode*/) {
        OZ (resolve_question_mark_node(into_list->children_[i], expr));
        CK (OB_NOT_NULL(expr));
      } else { /*Mysql mode*/
        OZ (ObResolverUtils::resolve_obj_access_ref_node(expr_factory_,
                                into_list->children_[i], q_name, resolve_ctx_.session_info_));
      }
      if (OB_SUCC(ret) && OB_ISNULL(expr)) {
        OZ (resolve_var(q_name, func, expr, true/*fo write*/));
      }
      if (OB_ERR_VARIABLE_IS_READONLY == ret) {
        ret = OB_ERR_EXP_NOT_INTO_TARGET;
        LOG_USER_ERROR(OB_ERR_EXP_NOT_INTO_TARGET,
                       static_cast<int>(into_list->children_[i]->str_len_),
                       into_list->children_[i]->str_value_);
      }
      if (OB_SUCC(ret) && 1 == into_node->value_) {
        into.set_bulk();
      }
      CK (OB_NOT_NULL(expr));
      OZ (func.add_expr(expr));
      OZ (into.add_into(func.get_expr_count() - 1, current_block_->get_namespace(), *expr));
      if (OB_FAIL(ret)) {
      } else if (expr->is_obj_access_expr()) {
        OZ (func.add_obj_access_expr(expr));
      } else if (T_OP_GET_PACKAGE_VAR == expr->get_expr_type()) {
        OX (func.set_wps());
      }
    }

    // 检查[bulk] into var的var合法性，var不能是collection
    OZ (into.check_into(func, current_block_->get_namespace(), 1 == into_node->value_));
  }
  return ret;
}

struct ObPredefinedException {
  ObPredefinedException(const char* name, ObPLConditionValue value) :
    name_(name), value_(value) {}
  const char* name_;
  ObPLConditionValue value_;
};

#define DEFINED_EXCEPTION(name, code) \
  ObPredefinedException(name, ObPLConditionValue(ERROR_CODE, code))

static ObPredefinedException PREDEFINED_EXCEPTIONS[] =
{
  DEFINED_EXCEPTION("ACCESS_INTO_NULL", OB_ERR_ACCESS_INTO_NULL), // ORA-6530
  DEFINED_EXCEPTION("CASE_NOT_FOUND", OB_ER_SP_CASE_NOT_FOUND), // ORA-6592
  DEFINED_EXCEPTION("COLLECTION_IS_NULL", OB_ERR_COLLECION_NULL), // ORA-6531
  DEFINED_EXCEPTION("CURSOR_ALREADY_OPEN", OB_ER_SP_CURSOR_ALREADY_OPEN), // ORA-6511
  DEFINED_EXCEPTION("DUP_VAL_ON_INDEX", OB_ERR_PRIMARY_KEY_DUPLICATE), // ORA-1

  DEFINED_EXCEPTION("INVALID_CURSOR", OB_ERR_INVALID_CURSOR), // ORA-1001
  DEFINED_EXCEPTION("INVALID_NUMBER", OB_INVALID_NUMERIC), // ORA-1722
  DEFINED_EXCEPTION("LOGIN_DENIED", OB_ERR_LOGIN_DENIED), // ORA-1017
  DEFINED_EXCEPTION("NO_DATA_FOUND", OB_READ_NOTHING), // ORA-+100
  DEFINED_EXCEPTION("NO_DATA_NEEDED", OB_ERR_NO_DATA_NEEDED), // ORA-6548

  DEFINED_EXCEPTION("NOT_LOGGED_ON", OB_ERR_NOT_LOGGED_ON), // ORA-1012
  DEFINED_EXCEPTION("PROGRAM_ERROR", OB_ERR_PROGRAM_ERROR), // ORA-6501
  DEFINED_EXCEPTION("ROWTYPE_MISMATCH", OB_ERR_ROWTYPE_MISMATCH), // ORA-6504
  DEFINED_EXCEPTION("SELF_IS_NULL", OB_ERR_SELF_IS_NULL), // ORA-30625
  DEFINED_EXCEPTION("STORAGE_ERROR", OB_ERR_STORAGE_ERROR), // ORA-6500

  DEFINED_EXCEPTION("SUBSCRIPT_BEYOND_COUNT", OB_ERR_SUBSCRIPT_BEYOND_COUNT), // ORA-6533
  DEFINED_EXCEPTION("SUBSCRIPT_OUTSIDE_LIMIT", OB_ERR_SUBSCRIPT_OUTSIDE_LIMIT), // ORA-6532
  DEFINED_EXCEPTION("SYS_INVALID_ROWID", OB_INVALID_ROWID), // ORA-1410
  DEFINED_EXCEPTION("TIMEOUT_ON_RESOURCE", OB_ERR_TIMEOUT_ON_RESOURCE), // ORA-51
  DEFINED_EXCEPTION("TOO_MANY_ROWS", OB_ERR_TOO_MANY_ROWS), // ORA-1422

  DEFINED_EXCEPTION("VALUE_ERROR", OB_ERR_NUMERIC_OR_VALUE_ERROR), // ORA-6502
  DEFINED_EXCEPTION("ZERO_DIVIDE", OB_ERR_DIVISOR_IS_ZERO), // ORA-1476
};

int ObPLResolver::resolve_pre_condition(const ObString &name, const ObPLConditionValue **value)
{
  int ret = OB_SUCCESS;
  *value = NULL;
  for (int64_t i = 0;
       i < sizeof(PREDEFINED_EXCEPTIONS) / sizeof(PREDEFINED_EXCEPTIONS[0]);
       ++i) {
    if (0 == name.case_compare(PREDEFINED_EXCEPTIONS[i].name_)) {
      *value = &(PREDEFINED_EXCEPTIONS[i].value_);
      break;
    }
  }
  return ret;
}

int ObPLResolver::resolve_condition(const ObStmtNodeTree *parse_tree,
                                    const ObPLBlockNS &ns,
                                    const ObPLConditionValue **value,
                                    ObPLCompileUnitAST &func)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(parse_tree));
  if (OB_FAIL(ret)) {
  } else if (T_SP_ACCESS_NAME == parse_tree->type_) {
    ObSchemaChecker schema_checker;
    ObString db_name;
    ObString package_name;
    ObString condition_name;
    ObString dblink_name;
    OZ (schema_checker.init(resolve_ctx_.schema_guard_, resolve_ctx_.session_info_.get_sessid()));
    OZ (ObResolverUtils::resolve_sp_access_name(schema_checker,
                                               resolve_ctx_.session_info_.get_effective_tenant_id(),
                                               resolve_ctx_.session_info_.get_database_name(),
                                               *parse_tree,
                                               db_name,
                                               package_name,
                                               condition_name,
                                               dblink_name));
    if (OB_FAIL(ret)) {
    } else if (package_name.empty()) {
      if (!db_name.empty()
          && db_name != resolve_ctx_.session_info_.get_database_name()) {
        ret = OB_ERR_SP_COND_MISMATCH;
        LOG_WARN("can not found continue",
                 K(resolve_ctx_.session_info_.get_database_name()),
                 K(db_name), K(package_name), K(condition_name));
        LOG_USER_ERROR(OB_ERR_SP_COND_MISMATCH, condition_name.length(), condition_name.ptr());
      } else {
        OZ (resolve_condition(condition_name, ns, value));
      }
    } else if (db_name == resolve_ctx_.session_info_.get_database_name()
               && package_name == ns.get_package_name()) {
      // package_name is not null and equal to current ns, search local
      OZ (resolve_condition(condition_name, ns, value));
    } else {
      // package name is not null and not equal to current ns, search global
      uint64_t database_id = OB_INVALID_ID;
      int64_t compatible_mode = lib::is_oracle_mode() ? COMPATIBLE_ORACLE_MODE
                                                      : COMPATIBLE_MYSQL_MODE;
      uint64_t tenant_id = resolve_ctx_.session_info_.get_effective_tenant_id();
      const ObPackageInfo *package_info = NULL;
      ObPLPackageManager &package_manager =
        resolve_ctx_.session_info_.get_pl_engine()->get_package_manager();
      db_name = db_name.empty() ? resolve_ctx_.session_info_.get_database_name() : db_name;
      const ObPLCondition *condition = NULL;
      OZ (resolve_ctx_.schema_guard_.get_database_id(tenant_id, db_name, database_id));
      OZ (resolve_ctx_.schema_guard_.get_package_info(
          tenant_id, database_id, package_name, PACKAGE_TYPE, compatible_mode, package_info));
      if (OB_SUCC(ret)
          && OB_ISNULL(package_info)
          && 0 == db_name.case_compare(OB_SYS_DATABASE_NAME)) {
        OZ (resolve_ctx_.schema_guard_.get_package_info(OB_SYS_TENANT_ID,
                                                OB_SYS_DATABASE_ID,
                                                package_name, PACKAGE_TYPE,
                                                compatible_mode, package_info));
      }
      if (OB_SUCC(ret) && OB_ISNULL(package_info)) {
        ret = OB_ERR_PACKAGE_DOSE_NOT_EXIST;
        LOG_WARN("package not exist", K(ret), K(package_name), K(db_name));
        LOG_USER_ERROR(OB_ERR_PACKAGE_DOSE_NOT_EXIST, "PACKAGE",
                       db_name.length(), db_name.ptr(),
                       package_name.length(), package_name.ptr());
      }
      OZ (package_manager.get_package_condition(resolve_ctx_,
                                                package_info->get_package_id(),
                                                condition_name,
                                                condition));
      if (OB_SUCC(ret) && OB_ISNULL(condition)) {
        ret = OB_ERR_SP_COND_MISMATCH;
        LOG_WARN("can not found continue",
                 K(resolve_ctx_.session_info_.get_database_name()),
                 K(db_name), K(package_name), K(condition_name));
        LOG_USER_ERROR(OB_ERR_SP_COND_MISMATCH, condition_name.length(), condition_name.ptr());
      }
      if (OB_SUCC(ret) && OB_NOT_NULL(condition) && condition->get_duplicate()) {
        ret = OB_ERR_SP_DUP_CONDITION;
        LOG_WARN("duplicate condition declare", K(ret), K(condition_name));
        LOG_USER_ERROR(OB_ERR_SP_DUP_CONDITION, condition_name.length(), condition_name.ptr());
      }
      if (OB_SUCC(ret)) {
        ObSchemaObjVersion obj_version;
        obj_version.object_id_ = package_info->get_package_id();
        obj_version.object_type_ = DEPENDENCY_PACKAGE;
        obj_version.version_ = package_info->get_schema_version();
        OZ (func.add_dependency_object(obj_version));
        OX (func.set_rps());
      }
      OX (*value = &(condition->get_value()));
    }
  } else if (T_IDENT == parse_tree->type_) {
    OZ (resolve_condition(ObString(parse_tree->str_len_, parse_tree->str_value_), ns, value));
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected continue node type", K(ret), K(parse_tree->type_));
  }
  return ret;
}

int ObPLResolver::resolve_condition(const ObString &name, const ObPLBlockNS &ns, const ObPLConditionValue **value)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(value)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Argument is NULL", K(value), K(ret));
  } else {
    *value = NULL;
    const ObPLConditionTable *condition_table = NULL;
    if (OB_ISNULL(condition_table = ns.get_condition_table())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Symbol table is NULL", K(condition_table), K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < ns.get_conditions().count(); ++i) {
        const ObPLCondition *condition = condition_table->get_condition(ns.get_conditions().at(i));
        if (OB_ISNULL(condition)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Get a invalid var", K(i), K(ns.get_conditions().at(i)), K(ret));
        } else if (0 == name.case_compare(condition->get_name())) {
          if (condition->get_duplicate()) {
            ret = OB_ERR_SP_DUP_CONDITION;
            LOG_WARN("duplicate condition declare", K(ret), K(name));
            LOG_USER_ERROR(OB_ERR_SP_DUP_CONDITION, name.length(), name.ptr());
          } else {
            *value = &condition->get_value();
          }
        }
      }

      if (OB_SUCC(ret) && NULL == *value) {
        if (NULL == ns.get_pre_ns()) {
          if (lib::is_oracle_mode()) {
            if (OB_NOT_NULL(ns.get_external_ns())
                && OB_NOT_NULL(ns.get_external_ns()->get_parent_ns())) {
              OZ (SMART_CALL(resolve_condition(
                name, *(ns.get_external_ns()->get_parent_ns()), value)));
            } else {
              OZ (resolve_pre_condition(name, value)); // check predefine exception
            }
          }
          if (OB_SUCC(ret) && OB_ISNULL(*value)) {
            ret = OB_ERR_SP_COND_MISMATCH;
            LOG_USER_ERROR(OB_ERR_SP_COND_MISMATCH, name.length(), name.ptr());
          }
        } else {
          OZ (SMART_CALL(resolve_condition(name, *ns.get_pre_ns(), value)));
        }
      }
    }
  }
  return ret;
}

int ObPLResolver::resolve_handler_condition(const ObStmtNodeTree *parse_tree,
                                            ObPLConditionValue &condition,
                                            ObPLCompileUnitAST &func)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(parse_tree)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid Argument", K(parse_tree), K(ret));
  } else if (T_SP_EXCEPTION_OTHERS == parse_tree->type_
          || T_SQL_WARNING == parse_tree->type_
          || T_SQL_NOT_FOUND == parse_tree->type_
          || T_SQL_EXCEPTION == parse_tree->type_) {
    condition.type_ = static_cast<ObPLConditionType>(parse_tree->type_ - T_SQL_EXCEPTION + SQL_EXCEPTION);
  } else if (T_SP_CONDITION == parse_tree->type_) {
    if (OB_FAIL(resolve_condition_value(parse_tree, condition,
                                        is_sys_database_id(func.get_database_id())))) {
      LOG_WARN("failed to resolve condition", K(parse_tree), K(ret));
    }
  } else if (T_IDENT == parse_tree->type_ // for mysql mode
            || T_SP_ACCESS_NAME == parse_tree->type_) { // for oracle mode
    const ObPLConditionValue *value = NULL;
    CK (OB_NOT_NULL(current_block_));
    OZ (resolve_condition(parse_tree, current_block_->get_namespace(), &value, func));
    CK (OB_NOT_NULL(value));
    OX (condition = *value);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid condition type", K(parse_tree->type_), K(ret));
  }
  return ret;
}

int ObPLResolver::resolve_condition_value(const ObStmtNodeTree *parse_tree,
                                          ObPLConditionValue &value,
                                          const bool is_sys_db)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(parse_tree)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid Argument", K(parse_tree), K(ret));
  } else if (lib::is_mysql_mode()) {
    if (T_SP_CONDITION != parse_tree->type_ || OB_ISNULL(parse_tree->children_[0])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Invalid condition type", K(parse_tree->type_), K(ret));
    } else if (T_SQL_STATE == parse_tree->children_[0]->type_) {
      value.type_ = SQL_STATE;
      if (OB_ISNULL(parse_tree->children_[0]->children_[0]) || T_VARCHAR != parse_tree->children_[0]->children_[0]->type_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Invalid condition type", K(parse_tree->children_[0]->children_[0]), K(ret));
      } else {
        value.sql_state_ = parse_tree->children_[0]->children_[0]->str_value_;
        value.str_len_ = parse_tree->children_[0]->children_[0]->str_len_;
      }
    } else if (T_INT == parse_tree->children_[0]->type_) {
      value.type_ = ERROR_CODE;
      value.error_code_ = parse_tree->children_[0]->value_;
      if (lib::is_oracle_mode()) {
        if (value.error_code_ >= 0) {
          ret = OB_ERR_ILLEGAL_ERROR_NUM;
          LOG_WARN("PLS-00701: PRAGMA EXCEPTION_INIT 的非法 ORACLE 错误号", K(ret));
        }
      }
      OX (value.sql_state_ = ob_sqlstate(static_cast<int>(value.error_code_)));
      OX (value.str_len_ = STRLEN(value.sql_state_));
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Invalid condition type", K(parse_tree->children_[0]->type_), K(ret));
    }
  } else {
    if (T_INT == parse_tree->type_
        || (T_VARCHAR == parse_tree->type_ && is_sys_db)) {
      value.error_code_ = (T_INT == parse_tree->type_ 
        ? parse_tree->value_ : static_cast<int64_t>(strtoll(parse_tree->str_value_, NULL, 10)));
      if (value.error_code_ >= 0) {
        ret = OB_ERR_ILLEGAL_ERROR_NUM;
        LOG_WARN("illega error number for PRAGMA EXCEPTION_INIT", K(ret));
      }
      OX (value.type_ = ERROR_CODE);
      OX (value.sql_state_ = ob_sqlstate(static_cast<int>(value.error_code_)));
      OX (value.str_len_ = STRLEN(value.sql_state_));
    } else {
      ret = OB_ERR_EX_SECOND_ARG;
      LOG_WARN("second argument to PRAGMA EXCEPTION_INIT must be a numeric literal", K(ret));
    }
  }
  return ret;
}

int ObPLResolver::check_duplicate_condition(const ObPLDeclareHandlerStmt &stmt,
                                            const ObPLConditionValue &value,
                                            bool &dup,
                                            ObPLDeclareHandlerStmt::DeclareHandler::HandlerDesc* cur_desc)
{
  int ret = OB_SUCCESS;
  dup = false;
  for (int64_t i = 0; OB_SUCC(ret) && !dup && i < stmt.get_handlers().count(); ++i) {
    ObPLDeclareHandlerStmt::DeclareHandler::HandlerDesc *desc = stmt.get_handler(i).get_desc();
    if (OB_ISNULL(desc)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Handler is NULL", K(i), K(stmt.get_handler(i)), K(ret));
    } else {
      for (int64_t j = 0; !dup && j < desc->get_conditions().count(); ++j) {
        if (value.type_ == desc->get_condition(j).type_ &&
            value.error_code_ == desc->get_condition(j).error_code_ &&
            value.str_len_ == desc->get_condition(j).str_len_ &&
            0 == STRNCMP(value.sql_state_, desc->get_condition(j).sql_state_, value.str_len_)) {
          dup = true;
        }
      }
    }
  }
  for (int64_t i = handler_analyzer_.get_stack_depth() - 1; OB_SUCC(ret) && !dup && i >= 0; --i) {
    ObPLDeclareHandlerStmt::DeclareHandler handler;
    if (OB_FAIL(handler_analyzer_.get_handler(i, handler))) {
      LOG_WARN("failed to get handler from handler analyzer", K(ret), K(i));
    } else if (handler.get_level() == current_level_) {
      ObPLDeclareHandlerStmt::DeclareHandler::HandlerDesc *desc = handler.get_desc();
      if (OB_ISNULL(desc)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Handler desc is NULL", K(ret), K(i), K(handler));
      } else if (desc->is_continue()) {
        for (int64_t j = 0; !dup && j < desc->get_conditions().count(); ++j) {
          if (value.type_ == desc->get_condition(j).type_ &&
              value.error_code_ == desc->get_condition(j).error_code_ &&
              value.str_len_ == desc->get_condition(j).str_len_ &&
              0 == STRNCMP(value.sql_state_, desc->get_condition(j).sql_state_, value.str_len_)) {
            dup = true;
          }
        }
      }
    } else {
      break;
    }
  }
  if (OB_NOT_NULL(cur_desc)) {
    for (int64_t j = 0; !dup && j < cur_desc->get_conditions().count(); ++j) {
      if (value.type_ == cur_desc->get_condition(j).type_ &&
          value.error_code_ == cur_desc->get_condition(j).error_code_ &&
          value.str_len_ == cur_desc->get_condition(j).str_len_ &&
          0 == STRNCMP(value.sql_state_, cur_desc->get_condition(j).sql_state_, value.str_len_)) {
        dup = true;
      }
    }
  }
  return ret;
}

// build oracle sequence_object.currval, sequence_object.nextval expr
int ObPLResolver::build_seq_value_expr(ObRawExpr *&expr,
                                       const ObQualifiedName &q_name,
                                       uint64_t seq_id)
{
  int ret = OB_SUCCESS;
  ObSequenceRawExpr *func_expr = NULL;
  ObConstRawExpr *col_id_expr = NULL;
  ObSQLSessionInfo& session_info = resolve_ctx_.session_info_;

  if (0 == q_name.col_name_.case_compare("CURRVAL")) {
    if (OB_FAIL(expr_factory_.create_raw_expr(T_FUN_SYS_SEQ_NEXTVAL, func_expr))) {
      LOG_WARN("create currval failed", K(ret));
    } else if (OB_FAIL(expr_factory_.create_raw_expr(T_UINT64, col_id_expr))) {
      LOG_WARN("create const raw expr failed", K(ret));
    } else {
      if (OB_ISNULL(func_expr) || OB_ISNULL(col_id_expr)){
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected to get a null func expr", K(func_expr), K(col_id_expr), K(ret));
      } else {
        ObObj col_id;
        col_id.set_uint64(seq_id);
        col_id_expr->set_value(col_id);
        if (OB_FAIL(func_expr->set_sequence_meta(q_name.database_name_, q_name.tbl_name_, q_name.col_name_, seq_id))) {
          LOG_WARN("failed to set sequence meta", K(ret));
        } else if (OB_FAIL(func_expr->add_flag(IS_SEQ_EXPR))) {
          LOG_WARN("failed to add flag", K(ret));
        } else if (OB_FAIL(func_expr->add_param_expr(col_id_expr))) {
          LOG_WARN("set funcation param expr failed", K(ret));
        } else if (OB_FAIL(func_expr->formalize(&session_info))) {
          LOG_WARN("failed to extract info", K(ret));
        } else {
          expr = func_expr;
        }
      }
    }
  } else if (0 == q_name.col_name_.case_compare("NEXTVAL")) {
    if (OB_FAIL(expr_factory_.create_raw_expr(T_FUN_SYS_PL_SEQ_NEXT_VALUE, func_expr))) {
      LOG_WARN("create nextval failed", K(ret));
    } else if (OB_FAIL(expr_factory_.create_raw_expr(T_UINT64, col_id_expr))) {
      LOG_WARN("create const raw expr failed", K(ret));
    } else {
      if (OB_ISNULL(func_expr) || OB_ISNULL(col_id_expr)){
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected to get a null func expr", K(func_expr), K(col_id_expr), K(ret));
      } else {
        ObObj col_id;
        func_expr->set_sequence_meta(q_name.database_name_, q_name.tbl_name_, q_name.col_name_, seq_id);
        col_id.set_uint64(seq_id);
        col_id_expr->set_value(col_id);
        if (OB_FAIL(func_expr->add_param_expr(col_id_expr))) {
          LOG_WARN("set funcation param expr failed", K(ret));
        } else if (OB_FAIL(func_expr->formalize(&session_info))) {
          LOG_WARN("failed to extract info", K(ret));
        } else {
          expr = func_expr;
        }
      }
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("resolve failed, invalid sequence field name.", K(ret));
  }
  return ret;
}


int ObPLResolver::resolve_sequence_object(const ObQualifiedName &q_name,
                                          ObPLCompileUnitAST &unit_ast,
                                          ObRawExpr *&real_ref_expr)
{
  int ret = OB_SUCCESS;
  uint64_t seq_id = OB_INVALID_ID;
  ObSynonymChecker syn_checker;
  ObSchemaChecker sc;
  if (0 == q_name.col_name_.case_compare("NEXTVAL") ||
      0 == q_name.col_name_.case_compare("CURRVAL")) {
    if (OB_FAIL(sc.init(resolve_ctx_.schema_guard_))) {
      LOG_WARN("init schemachecker failed.");
    } else {
      // check if sequence is created. will also check synonym
      if (OB_FAIL(ob_sequence_ns_checker_.check_sequence_namespace(q_name,
                                                                  syn_checker,
                                                                  &resolve_ctx_.session_info_,
                                                                  &sc,
                                                                  seq_id))) {
        LOG_WARN_IGNORE_COL_NOTFOUND(ret, "check basic column namespace failed", K(ret), K(q_name));
      } else if(OB_FAIL(build_seq_value_expr(real_ref_expr, q_name, seq_id))) {
        LOG_WARN("failed to resolve seq.", K(ret));
      } else {
        int64_t schema_version = OB_INVALID_VERSION;
        ObSchemaObjVersion obj_version;
        const uint64_t tenant_id = resolve_ctx_.session_info_.get_effective_tenant_id();
        OZ (resolve_ctx_.schema_guard_.get_schema_version(SEQUENCE_SCHEMA,
                                                          tenant_id,
                                                          seq_id,
                                                          schema_version));
        CK (schema_version != OB_INVALID_VERSION);
        OX (obj_version.object_id_ = seq_id);
        OX (obj_version.object_type_ = DEPENDENCY_SEQUENCE);
        OX (obj_version.version_ = schema_version);
        OZ (unit_ast.add_dependency_object(obj_version));
      }
      if (OB_SUCC(ret)) {
        unit_ast.set_has_sequence();
      }
    }
  } else {
    ret = OB_ERR_SP_UNDECLARED_VAR;
    LOG_WARN("sequence access bad field.", K(ret));
  }
  return ret;
}

/**
  Sanity check for SQLSTATEs. The function does not check if it's really an
  existing SQL-state (there are just too many), it just checks string length and
  looks for bad characters.

  @param sqlstate the condition SQLSTATE.

  @retval true if it's ok.
  @retval false if it's bad.
*/
bool ObPLResolver::is_sqlstate_valid(const char* sql_state, const int64_t &str_len)
{
  bool ret = true;
  if (str_len != 5 || NULL == sql_state) {
    ret = false;
  } else {
    for (int i = 0 ; i < 5; ++i) {
      char c = sql_state[i];
      if ((c < '0' || '9' < c)
          && (c < 'A' || 'Z' < c)) {
        ret = false;
        break;
      }
    }
  }
  return ret;
}

/**
  Checks if the specified SQL-state-string defines COMPLETION condition.
  This function assumes that the given string contains a valid SQL-state.

  @param s the condition SQLSTATE.

  @retval true if the given string defines COMPLETION condition.
  @retval false otherwise.
*/
bool ObPLResolver::is_sqlstate_completion(const char *sql_state)
{
  return sql_state[0] == '0' && sql_state[1] == '0';
}

int ObPLResolver::analyze_actual_condition_type(const ObPLConditionValue &value, ObPLConditionType &type)
{
  int ret = OB_SUCCESS;
  type = INVALID_TYPE;
  switch (value.type_) {
  case ERROR_CODE: {
    const char* state = ob_sqlstate(static_cast<int>(value.error_code_));
    if (OB_UNLIKELY(NULL == state || STRLEN(state) < 5)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Invalid condition type", K(value), K(ret));
      } else {
        type = ObPLEH::eh_classify_exception(state);
      }
  }
    break;
  case SQL_STATE: {
    /*
      An error is triggered:
        - if the specified string is not a valid SQLSTATE,
        - or if it represents the completion condition -- it is not
          allowed to SIGNAL, or declare a handler for the completion
          condition.
    */
    if (!is_sqlstate_valid(value.sql_state_, value.str_len_)
        || is_sqlstate_completion(value.sql_state_)) {
      ret = OB_ER_SP_BAD_SQLSTATE;
      LOG_USER_ERROR(OB_ER_SP_BAD_SQLSTATE, static_cast<int>(value.str_len_), value.sql_state_);
      LOG_WARN("Bad SQLSTATE", K(value), K(ret));
    } else {
      type = ObPLEH::eh_classify_exception(value.sql_state_);
    }
  }
    break;
  case SQL_EXCEPTION:
  case SQL_WARNING:
  case NOT_FOUND:
  case OTHERS: {
    type = value.type_;
  }
    break;
  default:{
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid condition type", K(value), K(ret));
  }
    break;
  }
  return ret;
}

int ObPLResolver::HandlerAnalyzer::set_handlers(const common::ObIArray<ObPLDeclareHandlerStmt::DeclareHandler> &handlers, int64_t level)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < handlers.count(); ++i) {
    if (OB_FAIL(set_handler(handlers.at(i).get_desc(), level))) {
      LOG_WARN("failed to set handler", K(i), K(ret));
    }
  }
  return ret;
}

int ObPLResolver::HandlerAnalyzer::reset_handlers(int64_t level)
{
  int ret = OB_SUCCESS;
  for (int64_t i = handler_stack_.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
    if (handler_stack_.at(i).get_level() == level) {
      if (i != handler_stack_.count() -1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to reset handlers with level", K(ret), K(level), K(i), K(handler_stack_.at(i)));
      } else {
        handler_stack_.pop_back();
      }
    }
  }
  if (OB_SUCC(ret) && handler_stack_.count() - 1 < top_continue_) {
    top_continue_ = OB_INVALID_INDEX;
  }
  return ret;
}

int ObPLResolver::resolve_external_types_from_expr(ObRawExpr &expr)
{
  int ret = OB_SUCCESS;
  if (expr.is_obj_access_expr()) {
    ObObjAccessRawExpr &obj_access_expr = static_cast<ObObjAccessRawExpr&>(expr);
    for (int64_t i = 0; OB_SUCC(ret) && i < obj_access_expr.get_access_idxs().count(); ++i) {
      OZ (obj_access_expr.get_access_idxs().at(i).elem_type_.get_all_depended_user_type(
          get_resolve_ctx(),
          get_current_namespace()));
      if (NULL != obj_access_expr.get_access_idxs().at(i).get_sysfunc_) {
        OZ (resolve_external_types_from_expr(*obj_access_expr.get_access_idxs().at(i).get_sysfunc_));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < expr.get_param_count(); ++i) {
      CK (OB_NOT_NULL(expr.get_param_expr(i)));
      OZ (resolve_external_types_from_expr(*expr.get_param_expr(i)));
    }
  }
  return ret;
}

int ObPLResolver::add_external_cursor(ObPLBlockNS &ns,
                                      const ObPLBlockNS *external_ns,
                                      const ObPLCursor &cursor,
                                      int64_t &index,
                                      ObPLCompileUnitAST &func)
{
  int ret = OB_SUCCESS;
  OX (index = OB_INVALID_INDEX);
  CK (OB_NOT_NULL(ns.get_cursor_table()));
  for (int64_t i = 0; OB_SUCC(ret) && i < ns.get_cursor_table()->get_count(); ++i) {
    const ObPLCursor *tmp = ns.get_cursor_table()->get_cursor(i);
    CK (OB_NOT_NULL(tmp));
    if (OB_SUCC(ret)
        && tmp->get_package_id() == cursor.get_package_id()
        && tmp->get_routine_id() == cursor.get_routine_id()
        && tmp->get_index() == cursor.get_index()) {
      index = i;
      break;
    }
  }
  if (OB_SUCC(ret) && OB_INVALID_INDEX == index) {
    ObIAllocator &allocator = resolve_ctx_.allocator_;
    ObString sql;
    ObString ps_sql;
    ObRecordType *row_desc = NULL;
    ObPLDataType cursor_type;
    if (OB_NOT_NULL(cursor.get_row_desc())) {
      row_desc = static_cast<ObRecordType *>(allocator.alloc(sizeof(ObRecordType)));
      if (OB_ISNULL(row_desc)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc record type for package cursor", K(ret));
      }
      row_desc = new(row_desc)ObRecordType();
      OZ (row_desc->deep_copy(allocator, *(cursor.get_row_desc())));
    }
    OX (index = OB_INVALID_INDEX);
    OZ (ob_write_string(allocator, cursor.get_sql(), sql));
    OZ (ob_write_string(allocator, cursor.get_ps_sql(), ps_sql));
    OZ (cursor_type.deep_copy(allocator, cursor.get_cursor_type()));
    ObSEArray<int64_t, 4> sql_params;

    for (int64_t i = 0; OB_SUCC(ret) && i < cursor.get_sql_params().count(); ++i) {
      ObRawExpr *expr = NULL;
      ObRawExpr *org_expr = NULL;
      if (NULL == external_ns) {
        org_expr = reinterpret_cast<ObRawExpr*>(cursor.get_sql_params().at(i));
      } else {
        CK (OB_NOT_NULL(external_ns->get_exprs()));
        CK (cursor.get_sql_params().at(i) >= 0
            && cursor.get_sql_params().at(i) < external_ns->get_exprs()->count());
        OX (org_expr = external_ns->get_exprs()->at(cursor.get_sql_params().at(i)));
      }
      OZ (ObPLExprCopier::copy_expr(expr_factory_, org_expr, expr));
      CK (OB_NOT_NULL(expr));
      OZ (resolve_external_types_from_expr(*expr));
      OZ (func.add_expr(expr));
      if (OB_SUCC(ret) && expr->is_obj_access_expr()) {
        OZ (func.add_obj_access_expr(expr));
      }
      OZ (sql_params.push_back(func.get_expr_count() - 1));
    }
    CK (OB_NOT_NULL(ns.get_cursor_table()));
    OZ (ns.get_cursor_table()->add_cursor(cursor.get_package_id(),
                                          cursor.get_routine_id(),
                                          cursor.get_index(),
                                          sql,
                                          sql_params,
                                          ps_sql,
                                          cursor.get_stmt_type(),
                                          cursor.is_for_update(),
                                          cursor.has_hidden_rowid(),
                                          cursor.get_rowid_table_id(),
                                          cursor.get_ref_objects(),
                                          row_desc,
                                          cursor_type,
                                          cursor.get_formal_params(),
                                          cursor.get_state(),
                                          cursor.is_dup_column()));
    OZ (ns.get_cursors().push_back(ns.get_cursor_table()->get_count() - 1));
    OX (index = ns.get_cursor_table()->get_count() - 1);
  }
  return ret;
}

int ObPLResolver::resolve_cursor(ObPLCompileUnitAST &func,
                                 const ObString &database_name,
                                 const ObString &package_name,
                                 const ObString &cursor_name,
                                 const ObPLCursor *&cursor)
{
  int ret = OB_SUCCESS;
  // package name is not null and not equal to current ns, search global
  uint64_t tenant_id = resolve_ctx_.session_info_.get_effective_tenant_id();
  int64_t compatible_mode = lib::is_oracle_mode() ? COMPATIBLE_ORACLE_MODE
                                                  : COMPATIBLE_MYSQL_MODE;
  const ObPackageInfo *package_info = NULL;
  ObPLPackageManager &package_manager =
    resolve_ctx_.session_info_.get_pl_engine()->get_package_manager();
  ObString db_name =
    database_name.empty() ? resolve_ctx_.session_info_.get_database_name() : database_name;
  uint64_t database_id = OB_INVALID_ID;
  int64_t idx = OB_INVALID_INDEX;
  CK (!package_name.empty());
  CK (!cursor_name.empty());
  OX (cursor = NULL);
  OZ (resolve_ctx_.schema_guard_.get_database_id(tenant_id, db_name, database_id));
  OZ (resolve_ctx_.schema_guard_.get_package_info(
      tenant_id, database_id, package_name, PACKAGE_TYPE, compatible_mode, package_info));
  if (OB_SUCC(ret)
      && OB_ISNULL(package_info) && 0 == db_name.case_compare(OB_SYS_DATABASE_NAME)) {
    OZ (resolve_ctx_.schema_guard_.get_package_info(
      OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID,
      package_name, PACKAGE_TYPE, compatible_mode, package_info));
  }
  if (OB_SUCC(ret) && OB_ISNULL(package_info)) {
    ObSchemaChecker checker;
    ObSEArray<uint64_t, 4> syn_id_array;
    ObString new_package_name;
    OZ (checker.init(resolve_ctx_.schema_guard_));
    OZ (checker.get_obj_info_recursively_with_synonym(
      tenant_id, database_id, package_name, database_id, new_package_name, syn_id_array, true));
    OZ (resolve_ctx_.schema_guard_.get_package_info(
      tenant_id, database_id, new_package_name, PACKAGE_TYPE, compatible_mode, package_info));
    if (OB_SUCC(ret)
        && OB_ISNULL(package_info) && OB_SYS_DATABASE_ID == database_id) {
      OZ (resolve_ctx_.schema_guard_.get_package_info(
        OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID, new_package_name, PACKAGE_TYPE, compatible_mode, package_info));
    }
  }
  if ((OB_SUCC(ret) && OB_ISNULL(package_info)) || OB_SYNONYM_NOT_EXIST == ret) {
    ret = OB_ERR_PACKAGE_DOSE_NOT_EXIST;
    LOG_WARN("package not exist", K(ret), K(package_name), K(db_name));
    LOG_USER_ERROR(OB_ERR_PACKAGE_DOSE_NOT_EXIST, "PACKAGE",
                           db_name.length(), db_name.ptr(),
                           package_name.length(), package_name.ptr());
  }
  OZ (package_manager.get_package_cursor(
        resolve_ctx_, package_info->get_package_id(), cursor_name, cursor, idx));
  if (OB_SUCC(ret) && OB_ISNULL(cursor)) {
    ret = OB_ERR_SP_CURSOR_MISMATCH;
    LOG_WARN("can not found cursor",
             K(resolve_ctx_.session_info_.get_database_name()),
             K(db_name), K(package_name), K(cursor_name));
    LOG_USER_ERROR(OB_ERR_SP_CURSOR_MISMATCH, cursor_name.length(), cursor_name.ptr());
  }
  OZ (func.add_dependency_objects(cursor->get_value().get_ref_objects()));
  if (OB_SUCC(ret)) {
    ObSchemaObjVersion obj_version;
    obj_version.object_id_ = package_info->get_package_id();
    obj_version.object_type_ = DEPENDENCY_PACKAGE;
    obj_version.version_ = package_info->get_schema_version();
    OZ (func.add_dependency_object(obj_version));
    OX (func.set_rps());
  }
  return ret;
}

int ObPLResolver::resolve_cursor(
  const ObStmtNodeTree *parse_tree, const ObPLBlockNS &ns,
  int64_t &index, ObPLCompileUnitAST &func)
{
  int ret = OB_SUCCESS;
  OX (index = OB_INVALID_INDEX);
  CK (OB_NOT_NULL(parse_tree));
  if (OB_FAIL(ret)) {
  } else if (T_SP_ACCESS_NAME == parse_tree->type_) {
    ObSchemaChecker schema_checker;
    ObString db_name;
    ObString package_name;
    ObString cursor_name;
    ObString dblink_name;
    OZ (schema_checker.init(resolve_ctx_.schema_guard_, resolve_ctx_.session_info_.get_sessid()));
    OZ (ObResolverUtils::resolve_sp_access_name(
      schema_checker, resolve_ctx_.session_info_.get_effective_tenant_id(),
      resolve_ctx_.session_info_.get_database_name(),
      *parse_tree, db_name, package_name, cursor_name, dblink_name));
    if (OB_FAIL(ret)) {
    } else if (package_name.empty()) {
      if (!db_name.empty()
          && db_name != resolve_ctx_.session_info_.get_database_name()) {
        ret = OB_ERR_SP_CURSOR_MISMATCH;
        LOG_WARN("can not found cursor",
                 K(resolve_ctx_.session_info_.get_database_name()),
                 K(db_name), K(package_name), K(cursor_name));
        LOG_USER_ERROR(OB_ERR_SP_CURSOR_MISMATCH, cursor_name.length(), cursor_name.ptr());
      } else {
        OZ (resolve_cursor(cursor_name, ns, index, func), cursor_name);
      }
    } else if (db_name == resolve_ctx_.session_info_.get_database_name()
               && package_name == ns.get_package_name()) {
      // package_name is not null and equal to current ns, search local
      OZ (resolve_cursor(cursor_name, ns, index, func, false, true), cursor_name);
    } else {
      // search global cursor
      const ObPLCursor *cursor = NULL;
      OZ (resolve_cursor(
        func, db_name, package_name, cursor_name, cursor));
      CK (OB_NOT_NULL(cursor));
      CK (OB_NOT_NULL(current_block_));
      OZ (add_external_cursor(current_block_->get_namespace(), NULL, *cursor, index, func));
    }
  } else if (T_OBJ_ACCESS_REF == parse_tree->type_) {
    CK (2 == parse_tree->num_child_);
    if (parse_tree->children_[1] != nullptr) {
      ObString db_name;
      ObString package_name;
      ObString cursor_name;
      const ObStmtNodeTree *package_node = parse_tree->children_[0];
      const ObStmtNodeTree *cursor_node = parse_tree->children_[1]->children_[0];
      package_name.assign_ptr(package_node->str_value_, static_cast<int32_t>(package_node->str_len_));
      if (T_FUN_SYS == cursor_node->type_
          && OB_NOT_NULL(cursor_node->children_[0])
          && T_IDENT == cursor_node->children_[0]->type_) {
        cursor_name.assign_ptr(cursor_node->children_[0]->str_value_, static_cast<int32_t>(cursor_node->children_[0]->str_len_));
      } else {
        cursor_name.assign_ptr(cursor_node->str_value_, static_cast<int32_t>(cursor_node->str_len_));
      }
      db_name = resolve_ctx_.session_info_.get_database_name();
      if (OB_FAIL(ret)) {
      } else if (package_name.empty()) {
        ret = OB_ERR_SP_CURSOR_MISMATCH;
        LOG_WARN("can not found cursor",
                 K(resolve_ctx_.session_info_.get_database_name()),
                 K(db_name), K(package_name), K(cursor_name));
        LOG_USER_ERROR(OB_ERR_SP_CURSOR_MISMATCH, cursor_name.length(), cursor_name.ptr());
      } else if (package_name == ns.get_package_name()) {
        OZ (resolve_cursor(cursor_name, ns, index, func, false, true), cursor_name);
      } else {
        // search global cursor
        const ObPLCursor *cursor = NULL;
        OZ (resolve_cursor(
          func, db_name, package_name, cursor_name, cursor));
        CK (OB_NOT_NULL(cursor));
        CK (OB_NOT_NULL(current_block_));
        OZ (add_external_cursor(current_block_->get_namespace(), NULL, *cursor, index, func));
      }
    } else {
      const ObStmtNodeTree *cursor_name = parse_tree->children_[0];
      if (T_IDENT == cursor_name->type_) {
        OZ (resolve_cursor(ObString(cursor_name->str_len_, cursor_name->str_value_), ns, index, func));
      } else if (T_QUESTIONMARK == cursor_name->type_) {
        OZ (resolve_questionmark_cursor(cursor_name->value_, current_block_->get_namespace(), index));
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected cursor node type", K(ret), K(cursor_name->type_));
      }
    }
  } else if (T_IDENT == parse_tree->type_) {
    OZ (resolve_cursor(ObString(parse_tree->str_len_, parse_tree->str_value_), ns, index, func));
  } else if (T_QUESTIONMARK == parse_tree->type_) {
    OZ (resolve_questionmark_cursor(parse_tree->value_, current_block_->get_namespace(), index));
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected cursor node type", K(ret), K(parse_tree->type_));
  }
  return ret;
}

int ObPLResolver::resolve_questionmark_cursor(
  const int64_t symbol_idx, ObPLBlockNS &ns, int64_t &cursor)
{
  // question mark 一定是当前 ns, 里的，所以不需要去别的 ns 找
  int ret = OB_SUCCESS;
  cursor = OB_INVALID_INDEX;
  const ObPLSymbolTable *symbol_table = ns.get_symbol_table();
  const ObPLCursorTable *cursor_table = ns.get_cursor_table();
  CK (OB_NOT_NULL(symbol_table));
  CK (OB_NOT_NULL(cursor_table));
  for (int64_t i = 0;
       OB_SUCC(ret) && OB_INVALID_INDEX == cursor && i < ns.get_cursors().count(); ++i) {
    const ObPLVar *var = NULL;
    const ObPLCursor *cur = cursor_table->get_cursor(ns.get_cursors().at(i));
    CK (OB_NOT_NULL(cur));
    if (ns.get_package_id() != cur->get_package_id()
        || ns.get_routine_id() != cur->get_routine_id()) {
      //外部cursor，跳过即可
    } else {
      CK (OB_NOT_NULL(var = symbol_table->get_symbol(cur->get_index())));
      if (OB_SUCC(ret) && symbol_idx == cur->get_index()) {
        if (ns.get_block_type() != ObPLBlockNS::BLOCK_ROUTINE
            || ns.get_symbol_table() != current_block_->get_namespace().get_symbol_table()) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("questionmark cursor only resolve at itself namespace.", K(symbol_idx));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "resolve questionmark cursor at not its own namespace");
        } else {
          cursor = ns.get_cursors().at(i);
        }
        if (OB_SUCC(ret) && ObPLCursor::DUP_DECL == cur->get_state()) {
          ret = OB_ERR_SP_DUP_CURSOR;
          LOG_WARN("too many declarations of cursor match this call", K(ret), K(symbol_idx));
        }
        break;
      }
    }
  }
  if (OB_SUCC(ret) && OB_INVALID_INDEX == cursor) {
    if (OB_FAIL(ns.add_questionmark_cursor(symbol_idx))) {
      LOG_WARN("failed to add condition", K(ret));
    } else {
      ObPLVar *var = const_cast<ObPLVar *>(ns.get_symbol_table()->get_symbol(symbol_idx));
      if (OB_ISNULL(var)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid symbol.", K(symbol_idx));
      } else {
        var->set_readonly(false);
        var->set_type(PL_CURSOR_TYPE);
        var->get_type().set_sys_refcursor_type();
        cursor = ns.get_cursors().at(ns.get_cursors().count() - 1);
      }
    }
  }
  return ret;
}

int ObPLResolver::resolve_cursor(
  const ObString &name, const ObPLBlockNS &ns,
  int64_t &cursor, ObPLCompileUnitAST &func, bool check_mode, bool for_external_cursor)
{
  int ret = OB_SUCCESS;
  cursor = OB_INVALID_INDEX;
  const ObPLSymbolTable *symbol_table = ns.get_symbol_table();
  const ObPLCursorTable *cursor_table = ns.get_cursor_table();
  CK (OB_NOT_NULL(symbol_table));
  CK (OB_NOT_NULL(cursor_table));
  for (int64_t i = 0;
       OB_SUCC(ret)
       && (for_external_cursor ? ns.get_block_type() != ObPLBlockNS::BLOCK_ROUTINE : true)
       && OB_INVALID_INDEX == cursor
       && i < ns.get_cursors().count();
       ++i) {
    const ObPLVar *var = NULL;
    const ObPLCursor *cur = cursor_table->get_cursor(ns.get_cursors().at(i));
    CK (OB_NOT_NULL(cur));
    if (ns.get_package_id() != cur->get_package_id()
        || ns.get_routine_id() != cur->get_routine_id()) {
      // external cursor
      if (cur->is_package_cursor()) { // package cursor
        OZ (ns.get_package_var(
          resolve_ctx_, cur->get_package_id(), cur->get_index(), var),
          K(ns.get_package_id()), K(cur->get_package_id()),
          K(ns.get_routine_id()), K(cur->get_routine_id()));
        OV (OB_NOT_NULL(var),
          OB_ERR_UNEXPECTED, KPC(cur), K(ns.get_package_id()), K(ns.get_routine_id()));
      } else {
        OZ (ns.get_subprogram_var(
          cur->get_package_id(), cur->get_routine_id(), cur->get_index(), var),
          K(ns.get_package_id()), K(ns.get_routine_id()),
          K(cur->get_package_id()), K(cur->get_routine_id()), K(cur->get_index()));
        OV (OB_NOT_NULL(var),
          OB_ERR_UNEXPECTED, KPC(cur), K(ns.get_package_id()), K(ns.get_routine_id()));
      }
    } else {
      OV (OB_NOT_NULL(var = symbol_table->get_symbol(cur->get_index())),
        OB_ERR_UNEXPECTED, KPC(cur), K(ns.get_package_id()), K(ns.get_routine_id()));
    }
    if (OB_SUCC(ret) && 0 == name.case_compare(var->get_name())) {
      if (ns.get_block_type() != ObPLBlockNS::BLOCK_ROUTINE
          || ns.get_symbol_table() != current_block_->get_namespace().get_symbol_table()) {
        CK (OB_NOT_NULL(current_block_));
        OZ (add_external_cursor(current_block_->get_namespace(), &ns, *cur, cursor, func));
      } else {
        cursor = ns.get_cursors().at(i);
      }
      if (OB_SUCC(ret) && !check_mode && ObPLCursor::DUP_DECL == cur->get_state()) {
        ret = OB_ERR_SP_DUP_CURSOR;
        LOG_WARN("too many declarations of cursor match this call", K(ret), K(name));
      }
      break;
    }
  }

  if (OB_SUCC(ret)
      && OB_INVALID_INDEX == cursor
      && (!check_mode || ns.get_block_type() != ObPLBlockNS::BLOCK_ROUTINE)) {
    if (OB_NOT_NULL(ns.get_pre_ns())) {
      OZ (SMART_CALL(resolve_cursor(name,
                                    *ns.get_pre_ns(),
                                    cursor,
                                    func,
                                    check_mode,
                                    for_external_cursor)), K(name));
    } else if (OB_NOT_NULL(ns.get_external_ns())
               && OB_NOT_NULL(ns.get_external_ns()->get_parent_ns())) {
      OZ (SMART_CALL(resolve_cursor(name,
                                    *(ns.get_external_ns()->get_parent_ns()),
                                    cursor,
                                    func,
                                    check_mode,
                                    for_external_cursor)), K(name));
    } else if (check_mode) {
      LOG_DEBUG("can not found cursor", K(name));
    } else {
      ret = OB_ERR_SP_CURSOR_MISMATCH;
      LOG_WARN("can not found cursor", K(ret), K(name));
      LOG_USER_ERROR(OB_ERR_SP_CURSOR_MISMATCH, name.length(), name.ptr());
    }
  }
  return ret;
}

int ObPLResolver::resolve_label(const ObString &name,
                                const ObPLBlockNS &ns,
                                int64_t &label,
                                bool is_iterate_label)
{
  int ret = OB_SUCCESS;
  label = OB_INVALID_INDEX;
  const ObPLLabelTable *label_table = ns.get_label_table();
  if (OB_ISNULL(label_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Symbol table is NULL", K(label_table), K(label), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && OB_INVALID_INDEX == label && i < ns.get_labels().count(); ++i) {
      const ObString *label_name = label_table->get_label(ns.get_labels().at(i));
      if (OB_ISNULL(label_name)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("label is NULL", K(label_name), K(label), K(ret));
      } else if (0 == name.case_compare(*label_name)) {
        if (is_iterate_label) {
          ObPLLabelTable::ObPLLabelType type = label_table->get_label_type(ns.get_labels().at(i));
          if (ObPLLabelTable::ObPLLabelType::LABEL_CONTROL == type) {
            label = ns.get_labels().at(i);
          }
        } else {
          label = ns.get_labels().at(i);
        }
        break;
      } else { /*do nothing*/ }
    }

    if (OB_SUCC(ret) && OB_INVALID_INDEX == label) {
      if (ns.stop_search_label() || NULL == ns.get_pre_ns()) {
        ret = OB_ERR_SP_LILABEL_MISMATCH;
        LOG_WARN("label is not declared in this scope", K(name), K(label), K(ret));
        LOG_USER_ERROR(OB_ERR_SP_LILABEL_MISMATCH, name.length(), name.ptr());
      } else {
        OZ (SMART_CALL(resolve_label(name, *ns.get_pre_ns(), label, is_iterate_label)));
      }
    }
  }
  return ret;
}

int ObPLResolver::resolve_cond_loop(const ObStmtNodeTree *expr_node, const ObStmtNodeTree *body_node, ObPLCondLoop *stmt, ObPLFunctionAST &func)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr_node) || OB_ISNULL(body_node) || OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("parse_tree is NULL", K(expr_node), K(body_node), K(stmt), K(ret));
  } else {
    //解析expr
    ObRawExpr *expr = NULL;
    ObPLDataType data_type(ObTinyIntType);
    if (OB_FAIL(resolve_expr(expr_node, func, expr, combine_line_and_col(expr_node->stmt_loc_),
                             true, &data_type, false, lib::is_mysql_mode()))) {
      LOG_WARN("failed to resolve expr", K(expr_node), K(ret));
    } else {
      stmt->set_cond(func.get_expr_count() - 1);
    }

    //解析body
    if (OB_SUCC(ret)) {
      if (T_SP_PROC_STMT_LIST != body_node->type_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Invalid loop body", K(body_node->type_), K(ret));
      } else {
        ObPLStmtBlock *body_block = NULL;
        if (OB_FAIL(resolve_stmt_list(body_node, body_block, func))) {
          LOG_WARN("failed to resolve stmt list", K(body_node->type_), K(ret));
        } else {
          stmt->set_body(body_block);
        }
      }
    }
  }
  return ret;
}

int ObPLResolver::resolve_package_stmt_list(const ObStmtNodeTree *node, ObPLStmtBlock *&block, ObPLPackageAST &package)
{
  int ret = OB_SUCCESS;
  ObPLStmt *stmt = NULL;
  ObPLStmtBlock *parent = current_block_;
  block = NULL;
  if (OB_FAIL(stmt_factory_.allocate(PL_BLOCK, parent, stmt))) {
    LOG_WARN("failed to alloc stmt", K(ret));
  } else if (OB_ISNULL(block = static_cast<ObPLStmtBlock*>(stmt))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to static cast", K(ret));
  } else if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to allocate block", K(node), K(block), K(ret));
  } else {
    block->get_namespace().set_symbol_table(&package.get_symbol_table());
    block->get_namespace().set_type_table(&package.get_user_type_table());
    block->get_namespace().set_condition_table(&package.get_condition_table());
    block->get_namespace().set_cursor_table(&package.get_cursor_table());
    block->get_namespace().set_routine_table(&package.get_routine_table());
    block->get_namespace().set_external_ns(&get_external_ns());
    block->get_namespace().set_pre_ns(NULL == parent ? NULL : &parent->get_namespace());
    set_current(*block);

//    for (int64_t i = 0; OB_SUCC(ret) && i < node->num_child_; ++i) {
//      if (NULL != node->children_[i] && OB_FAIL(resolve(node->children_[i], package))) {
//        LOG_WARN("failed to resolve child stmt", K(i), K(node), K(ret));
//      }
//    }

    if (OB_SUCC(ret)) {
      set_current(*parent);
    }
  }
  return ret;
}

int ObPLResolver::resolve_stmt_list(const ObStmtNodeTree *node,
                                    ObPLStmtBlock *&block,
                                    ObPLFunctionAST &func,
                                    bool stop_search_label,
                                    bool in_handler_scope)
{
  int ret = OB_SUCCESS;
  ObPLStmtBlock *parent = current_block_;
  if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt list node is null", K(ret), K(node));
  } else if (OB_ISNULL(block)
            && OB_FAIL(make_block(func, parent, block, T_SP_BLOCK_CONTENT == node->type_))) {
    LOG_WARN("failed to make block", K(current_block_), K(ret));
  } else if (OB_ISNULL(block)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to allocate block", K(block), K(ret));
  } else if (stop_search_label && !is_oracle_mode()) {
    block->get_namespace().set_stop_search_label();
  }
  if (OB_SUCC(ret) && in_handler_scope) {
    block->set_handler();
  }

  if (OB_SUCC(ret)) {
    set_current(*block);
    if (T_SP_BLOCK_CONTENT == node->type_) {
      CK (node->num_child_ <= 2 && node->num_child_ >= 1);
      if (OB_SUCC(ret)) {
        RESOLVE_CHILDREN(node->children_[0], func);
      }
      if (OB_SUCC(ret) && 2 == node->num_child_) {
        RESOLVE_CHILDREN(node->children_[1], func);
      }
    } else {
      RESOLVE_CHILDREN(node, func);
    }
    if (OB_SUCC(ret)) {
      set_current(*parent);
    } else if (block != NULL) {
      block->reset();
    }
  }
  return ret;
}

int ObPLResolver::make_block(
  ObPLFunctionAST &func, const ObPLStmtBlock *parent, ObPLStmtBlock *&block, bool explicit_block)
{
  int ret = OB_SUCCESS;
  bool replace_current = block == current_block_;
  ObPLStmt *stmt = NULL;
  if (OB_FAIL(stmt_factory_.allocate(PL_BLOCK, parent, stmt))) {
    LOG_WARN("failed to alloc stmt", K(ret));
  } else if (OB_ISNULL(block = static_cast<ObPLStmtBlock*>(stmt))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to static cast", K(block), K(ret));
  } else {
    block->get_namespace().set_symbol_table(&func.get_symbol_table());
    block->get_namespace().set_label_table(&func.get_label_table());
    block->get_namespace().set_type_table(&func.get_user_type_table());
    block->get_namespace().set_condition_table(&func.get_condition_table());
    block->get_namespace().set_cursor_table(&func.get_cursor_table());
    block->get_namespace().set_routine_table(&func.get_routine_table());
    block->get_namespace().set_exprs(&func.get_exprs());
    block->get_namespace().set_obj_access_exprs(&func.get_obj_access_exprs());
    block->get_namespace().set_external_ns(&get_external_ns());
    block->get_namespace().set_pre_ns(NULL == parent ? NULL : &parent->get_namespace());
    block->get_namespace().set_block_type(ObPLBlockNS::BLOCK_ROUTINE);
    block->get_namespace().set_db_name(func.get_db_name());
    block->get_namespace().set_database_id(func.get_database_id());
    block->get_namespace().set_package_name(func.get_package_name());
    block->get_namespace().set_package_id(func.get_package_id());
    block->get_namespace().set_package_version(func.get_package_version());
    block->get_namespace().set_routine_id(func.get_subprogram_id());
    block->get_namespace().set_routine_name(func.get_name());
    block->get_namespace().set_compile_flag(func.get_compile_flag());
    if (explicit_block) {
      block->get_namespace().set_explicit_block();
    }
    if (func.is_function()) {
      block->get_namespace().set_function_block();
    }
    if (func.is_udt_routine()) {
      block->get_namespace().set_is_udt_routine();
    }
    if (replace_current) {
      resolve_ctx_.params_.secondary_namespace_ = &block->get_namespace();
    }
    if (NULL != parent) {
      parent->in_notfound() ? block->set_notfound() :  (void)NULL;
      parent->in_warning() ? block->set_warning() :  (void)NULL;
      parent->in_handler() ? block->set_handler() : (void)NULL;
    }
    if (lib::is_oracle_mode()
        && NULL != parent
        && NULL == parent->get_namespace().get_pre_ns()
        && func.get_arg_count() > 0) {
      OV (func.get_arg_count() == block->get_namespace().get_symbol_table()->get_count(),
          OB_ERR_UNEXPECTED, func.get_arg_count(),
          block->get_namespace().get_symbol_table()->get_count());
      for (int i = 0; OB_SUCC(ret) && i < func.get_arg_count(); ++i) {
        OZ (block->get_namespace().get_symbols().push_back(i));
      }
    }
  }
  return ret;
}

int ObPLResolver::make_block(ObPLPackageAST &package_ast, ObPLStmtBlock *&block)
{
  int ret = OB_SUCCESS;
  ObPLStmt *stmt = NULL;
  if (OB_FAIL(stmt_factory_.allocate(PL_BLOCK, NULL, stmt))) {
    LOG_WARN("failed to alloc stmt", K(ret));
  } else if (OB_ISNULL(block = static_cast<ObPLStmtBlock*>(stmt))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to allocate block", K(block), K(ret));
  } else {
    block->get_namespace().set_symbol_table(&package_ast.get_symbol_table());
    block->get_namespace().set_label_table(&package_ast.get_label_table());
    block->get_namespace().set_type_table(&package_ast.get_user_type_table());
    block->get_namespace().set_condition_table(&package_ast.get_condition_table());
    block->get_namespace().set_cursor_table(&package_ast.get_cursor_table());
    block->get_namespace().set_routine_table(&package_ast.get_routine_table());
    block->get_namespace().set_exprs(&package_ast.get_exprs());
    block->get_namespace().set_obj_access_exprs(&package_ast.get_obj_access_exprs());
    block->get_namespace().set_external_ns(&get_external_ns());
    block->get_namespace().set_block_type(NULL == get_external_ns().get_parent_ns() ?
    package_ast.is_package() ? ObPLBlockNS::BLOCK_PACKAGE_SPEC : ObPLBlockNS::BLOCK_OBJECT_SPEC :
    package_ast.is_package() ? ObPLBlockNS::BLOCK_PACKAGE_BODY : ObPLBlockNS::BLOCK_OBJECT_BODY);
    block->get_namespace().set_db_name(package_ast.get_db_name());
    block->get_namespace().set_database_id(package_ast.get_database_id());
    block->get_namespace().set_package_name(package_ast.get_name());
    block->get_namespace().set_package_id(package_ast.get_id());
    block->get_namespace().set_package_version(package_ast.get_version());
  }
  return ret;
}

int ObPLResolver::check_param_default_expr_legal(ObRawExpr *expr, bool is_subprogram_expr)
{
  int ret = OB_SUCCESS;
  int64_t symbol_idx = OB_INVALID_INDEX;
  CK (OB_NOT_NULL(current_block_));
  CK (OB_NOT_NULL(expr));

  if (OB_FAIL(ret)) {
  } else if (T_QUESTIONMARK == expr->get_expr_type()) {
    ObConstRawExpr *const_expr = static_cast<ObConstRawExpr *>(expr);
    CK (OB_NOT_NULL(const_expr));
    OX (symbol_idx = const_expr->get_value().get_unknown());
  } else if (expr->is_obj_access_expr()) {
    ObObjAccessRawExpr *obj_expr = static_cast<ObObjAccessRawExpr *>(expr);
    ObIArray<ObObjAccessIdx> &access_idxs = obj_expr->get_access_idxs();
    if (ObObjAccessIdx::is_local_variable(access_idxs)) {
      int64_t var_idx = OB_INVALID_INDEX;
      CK (ObObjAccessIdx::get_local_variable_idx(access_idxs) >= 0
          && ObObjAccessIdx::get_local_variable_idx(access_idxs) < access_idxs.count());
      OX (var_idx = access_idxs.at(ObObjAccessIdx::get_local_variable_idx(access_idxs)).var_index_);
      CK (var_idx >= 0 && var_idx < obj_expr->get_var_indexs().count());
      OX (symbol_idx = obj_expr->get_var_indexs().at(var_idx));
    }
  }
  if (OB_SUCC(ret) && symbol_idx != OB_INVALID_INDEX) {
    ObIArray<int64_t> &symbols = current_block_->get_namespace().get_symbols();
    ObString var_name;
    int64_t i = 0;
    for (; i < symbols.count(); ++i) {
      if (symbol_idx == symbols.at(i)) {
        const ObPLVar *var = current_block_->get_namespace().get_symbol_table()->get_symbol(symbol_idx);
        if (OB_NOT_NULL(var)) {
          var_name = var->get_name();
        }
        break;
      }
    }
    if (i == symbols.count()) {
      // not found, do nothing ...
    } else if (i == (symbols.count() - 1)) {
      // default expr refrence self parameter.
      ret = OB_ERR_TYPE_DECL_MALFORMED;
      LOG_WARN(
        "PLS-00320: the declaration of the type of this expression is incomplete or malformed",
        K(symbols), K(symbol_idx), K(ret));
    } else if (is_subprogram_expr) {
      ret = OB_ERR_IN_FORMAL_NOT_DENOTABLE;
      LOG_WARN(
        "PLS-00227: subprogram 'in' formal X is not yet denotable",
        K(symbols), K(symbol_idx), K(ret));
      LOG_USER_ERROR(OB_ERR_IN_FORMAL_NOT_DENOTABLE, var_name.length(), var_name.ptr());
    } else {
      ret = OB_ERR_FIELD_NOT_DENOTABLE;
      LOG_WARN(
        "PLS-00742: field string is not yet denotable", K(symbols), K(symbol_idx), K(ret));
      LOG_USER_ERROR(OB_ERR_FIELD_NOT_DENOTABLE, var_name.length(), var_name.ptr());
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
    OZ (check_param_default_expr_legal(expr->get_param_expr(i), is_subprogram_expr));
  }

  return ret;
}

int ObPLResolver::resolve_routine_decl_param_list(const ParseNode *param_list,
                                                  ObPLCompileUnitAST &unit_ast,
                                                  ObPLRoutineInfo &routine_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(param_list)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param list invalid", K(ret));
  } else if (param_list->type_ != T_SP_PARAM_LIST || OB_ISNULL(param_list->children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param list type is invalid", K(param_list->type_), K(param_list->children_), K(ret));
  } else if (param_list->num_child_ > OB_MAX_PROC_PARAM_COUNT) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("too many formal parameters, max number of formal parameters"
             "in an explicit cursor, function, or procedure is 65536!",
             K(ret), K(OB_MAX_PROC_PARAM_COUNT));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "number of formal parameters large than 65536");
  } else {
    ObString param_name, default_value;
    ObPLDataType param_type;
    ObPLRoutineParamMode param_mode = PL_PARAM_INVALID;
    ObPLExternTypeInfo extern_type_info;
    bool default_cast = false;

    ObPLStmtBlock *parent = current_block_;
    ObPLBlockNS *parent_namespace = resolve_ctx_.params_.secondary_namespace_;
    ObPLStmtBlock *block = NULL;
    if (unit_ast.is_routine()) {
      OZ (make_block(static_cast<ObPLFunctionAST&>(unit_ast), parent, block, false));
    } else {
      OZ (make_block(static_cast<ObPLPackageAST&>(unit_ast), block));
      OX (block->get_namespace().set_block_type(ObPLBlockNS::BLOCK_ROUTINE));
      OX (block->get_namespace().set_pre_ns(NULL == parent ? NULL : &parent->get_namespace()));
    }
    OX (set_current(*block));

    for (int64_t i = 0; OB_SUCC(ret) && i < param_list->num_child_; ++i) {
      const ParseNode *param_node = param_list->children_[i];
      const ParseNode *name_node = NULL;
      const ParseNode *type_node = NULL;
      param_name.reset();
      default_value.reset();
      default_cast = false;
      param_type.reset();
      extern_type_info.reset();
      if (OB_ISNULL(param_node)
          || OB_UNLIKELY(param_node->type_ != T_SP_PARAM)
          || OB_ISNULL(param_node->children_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("param node is invalide", K(param_node), K_(param_node->children));
      } else if (OB_ISNULL(name_node = param_node->children_[0])
          || OB_ISNULL(type_node = param_node->children_[1])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("name node or type node is null", K(name_node), K(type_node));
      } else {
        param_name.assign_ptr(name_node->str_value_, static_cast<int32_t>(name_node->str_len_));
        bool with_rowid = check_with_rowid(routine_info.get_name(),
                                           resolve_ctx_.session_info_.is_for_trigger_package());
        if (OB_FAIL(resolve_sp_data_type(type_node, param_name, unit_ast,
                                         param_type, &extern_type_info, with_rowid, true))) {
          LOG_WARN("resolve data type failed", K(ret), K(param_name));
        } else {
          switch (param_node->int32_values_[0]) {
          case MODE_IN:
            param_mode = PL_PARAM_IN;
            break;
          case MODE_OUT:
            param_mode = PL_PARAM_OUT;
            if (!unit_ast.is_package()) {
              unit_ast.set_has_out_param();
            }
            break;
          case MODE_INOUT:
            param_mode = PL_PARAM_INOUT;
            if (!unit_ast.is_package()) {
              unit_ast.set_has_out_param();
            }
            break;
          default:
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("param inout flag is invalid", K(param_node->value_));
            break;
          }
        }
        // clear warning info while type is self, only in object spec resolve
        if (OB_SUCC(ret) && routine_info.is_udt_routine()
          && 0 == current_block_->get_namespace().get_package_name().case_compare(extern_type_info.type_name_)) {
          OX (ob_reset_tsi_warning_buffer());
        }
        OZ (current_block_->get_namespace().add_symbol(param_name, param_type, NULL,
                                                      false, false, false, true));
        if (OB_ERR_SP_DUP_VAR == ret) {
          ret = OB_ERR_DUPLICATE_FILED;
        }
        if (OB_SUCC(ret)  // mysql mode 没有param_node->children_[2]
            && OB_NOT_NULL(param_node->children_[2])
            && (PL_PARAM_OUT == param_mode || PL_PARAM_INOUT == param_mode)) {
          ret = OB_ERR_OUT_PARAM_HAS_DEFAULT;
          LOG_WARN("PLS-00230: OUT and IN OUT formal parameters may not have default expressions",
                   K(ret));
        }
        if (OB_SUCC(ret) && OB_NOT_NULL(param_node->children_[2])) {
          ParseNode* default_node = param_node->children_[2];
          ObString default_v(static_cast<int32_t>(default_node->str_len_),
                             default_node->str_value_);
          ObRawExpr *default_expr = NULL;
          ObRoutineMatchInfo::MatchInfo match_info;
          ObObjType src_type = ObMaxType;
          uint64_t src_type_id = OB_INVALID_ID;
          CK (T_SP_DECL_DEFAULT == default_node->type_);
          CK (!default_v.empty());
          OZ (resolve_expr(default_node->children_[0], unit_ast, default_expr,
                           combine_line_and_col(default_node->children_[0]->stmt_loc_), false));
          OZ (ObResolverUtils::get_type_and_type_id(default_expr, src_type, src_type_id));
          OZ (ObResolverUtils::check_type_match(
            resolve_ctx_, match_info, default_expr, src_type, src_type_id, param_type));
          CK (OB_NOT_NULL(default_expr));
          OZ (check_param_default_expr_legal(default_expr));
          OX (default_value = default_v);
          OX (default_cast = match_info.need_cast_);
        }
        if (OB_SUCC(ret)) {
          ObPLRoutineParam *param = NULL;
          bool is_nocopy = is_mysql_mode() ? false : (1 == param_node->int32_values_[1]);
          OZ (routine_info.make_routine_param(resolve_ctx_.allocator_,
                                              resolve_ctx_.session_info_.get_dtc_params(),
                                              param_name,
                                              param_type,
                                              param_mode,
                                              is_nocopy, //nocopy
                                              default_value,
                                              default_cast,
                                              extern_type_info,
                                              param));
          if (OB_SUCC(ret) && OB_NOT_NULL(param)) {
            // make sure self param' type is object's type
            if (param->is_self_param()) {
              const ObPLBlockNS *top_ns = &(current_block_->get_namespace());
              CK (OB_NOT_NULL(top_ns));
              int64_t cnt = 0;
              while (OB_SUCC(ret) && OB_NOT_NULL(top_ns->get_pre_ns())) {
                top_ns = top_ns->get_pre_ns();
                ++cnt;
                if (10000 < cnt) {
                  break;
                }
              };
              if (10000 == cnt) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("seems infinite recursive loop, loop iterations: ", K(cnt));
              }
              if (OB_SUCC(ret)) {
                if (ObPLBlockNS::BlockType::BLOCK_OBJECT_SPEC == top_ns->get_block_type()
                    || ObPLBlockNS::BlockType::BLOCK_OBJECT_BODY == top_ns->get_block_type()) {
                  const ObString &obj_name = top_ns->get_package_name();
                  if (0 != obj_name.case_compare(param->get_type_name())) {
                    ret = OB_ERR_EXPRESSION_WRONG_TYPE;
                    LOG_WARN("PLS-00382: expression is of wrong type", K(ret), K(obj_name), K(param->get_type_name()));
                    LOG_USER_ERROR(OB_ERR_EXPRESSION_WRONG_TYPE);
                  }
                  if (OB_FAIL(ret)) {
                  } else if (PL_PARAM_OUT == param_mode) {
                    ret = OB_ERR_SELF_PARAM_NOT_OUT;
                    LOG_USER_ERROR(OB_ERR_SELF_PARAM_NOT_OUT);
                  } else if (routine_info.is_udt_cons() && param_mode != PL_PARAM_INOUT) {
                    ret = OB_ERR_SELF_PARAM_NOT_INOUT;
                    LOG_USER_ERROR(OB_ERR_SELF_PARAM_NOT_INOUT);
                  }
                } else {
                  param->set_is_self_param(false);
                }
              }
            }
          }
          OZ (routine_info.add_param(param));
          if (OB_FAIL(ret) && OB_NOT_NULL(param)) {
            param->~ObPLRoutineParam();
            resolve_ctx_.allocator_.free(param);
            param = NULL;
          }
          // bool is_readonly = false;
          // OX (is_readonly = (param->is_self_param() && PL_PARAM_IN == param_mode) ? true : false);
          if (OB_SUCC(ret) && param->is_self_param()) {
            current_block_->get_namespace().get_symbol_table()->set_self_param_idx();
          }
          if (OB_SUCC(ret) && (param->is_self_param() && PL_PARAM_IN == param_mode)) {
            // is_readonly = true;
            const ObPLVar *var=current_block_->get_namespace().get_symbol_table()->get_self_param();
            if (OB_NOT_NULL(var)) {
              const_cast<ObPLVar *>(var)->set_readonly(true);
            }
          }
        }
      }
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(block)) {
      OZ (block->get_namespace().delete_symbols());
    }
    if (OB_NOT_NULL(parent)) {
      set_current(*parent);
      resolve_ctx_.params_.secondary_namespace_ = parent_namespace;
    }
  }
  return ret;
}

int ObPLResolver::check_params_legal_in_body_routine(ObPLFunctionAST &routine_ast,
                                                     const ObPLRoutineInfo *parent_routine_info,
                                                     const ObPLRoutineInfo *body_routine_info)
{
  // 包头中有默认值,包体中可以有默认值也可以没有
  // 如果包体中有默认值, 必须与包头中的默认值一致
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(parent_routine_info));
  CK (OB_NOT_NULL(body_routine_info));
  if (OB_SUCC(ret)) {
    const common::ObIArray<ObPLRoutineParam *> &parent_params = parent_routine_info->get_params();
    const common::ObIArray<ObPLRoutineParam *> &body_params = body_routine_info->get_params();
    CK (OB_LIKELY(parent_params.count() == body_params.count()));
    ObArenaAllocator allocator;
    SMART_VAR(ObPLFunctionAST, parent_ast, allocator) {
      for (int64_t i = 0; OB_SUCC(ret) && i < parent_params.count(); ++i) {
        ObPLRoutineParam* parent_param = parent_params.at(i);
        ObPLRoutineParam* body_param = body_params.at(i);
        CK (OB_NOT_NULL(parent_param));
        CK (OB_NOT_NULL(body_param));
        if (OB_SUCC(ret)) {
          const ObPLVar *var = routine_ast.get_symbol_table().get_symbol(i);
          ObPLDataType expected_type(parent_param->get_type().get_obj_type());
          OZ (init_default_expr(parent_ast,
                                *parent_param,
                                var->get_type()));
          if (OB_FAIL(ret)) {
          } else if (!parent_param->get_default_value().empty()) {
            if (body_param->get_default_value().empty()) {
              body_param->set_default_value(parent_param->get_default_value());
              OZ (init_default_expr(routine_ast, i, *body_param));
            } else {
              ObRawExpr* body_default = routine_ast.get_expr(var->get_default());
              ObRawExpr* parent_default = parent_ast.get_expr(parent_ast.get_expr_count()-1);
              ObExprEqualCheckContext check_ctx;
              check_ctx.need_check_deterministic_ = false;
              if (NULL == parent_default || NULL == body_default) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("default expr is null.", K(ret), K(parent_default), K(body_default));
              } else if (!body_default->same_as(*parent_default, &check_ctx)) {
                ret = OB_ERR_DEFAULT_NOT_MATCH;
                LOG_WARN("PLS-00593:"
                        " default value of parameter 'string' in body must match that of spec",
                        K(ret));
              } else { }
            }
          } else if (!body_param->get_default_value().empty()) {
            ret = OB_ERR_DEFAULT_NOT_MATCH;
            LOG_WARN("PLS-00593:"
                    "default value of parameter 'string' in body must match that of spec",
                    K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObPLResolver::resolve_routine_decl(const ObStmtNodeTree *parse_tree,
                                       ObPLCompileUnitAST &unit_ast,
                                       ObPLRoutineInfo *&routine_info,
                                       bool is_udt_routine,
                                       bool resolve_routine_def)
{
  int ret = OB_SUCCESS;
  CK (T_SUB_PROC_DECL == parse_tree->type_ || T_SUB_FUNC_DECL == parse_tree->type_);
  CK (OB_NOT_NULL(parse_tree->str_value_),
      OB_NOT_NULL(parse_tree->children_),
      OB_LIKELY(parse_tree->num_child_ <= 5));
  routine_info = NULL;
  if (OB_SUCC(ret)) {
    ObString routine_name;
    ObArray<int64_t> dummy_path;
    ObPLDataType ret_type;
    /* A subprogram created inside a package is a packaged subprogram .
     *  A subprogram created inside a PL/SQL block is a nested subprogram */
    ObProcType routine_type = unit_ast.is_package() || unit_ast.is_object()
        ? ((T_SUB_PROC_DECL == parse_tree->type_) ? PACKAGE_PROCEDURE : PACKAGE_FUNCTION)
            : ((T_SUB_PROC_DECL == parse_tree->type_) ? NESTED_PROCEDURE : NESTED_FUNCTION);
    const ObPLRoutineTable *routine_table = NULL;
    ObString routine_decl_str(static_cast<int32_t>(parse_tree->str_len_), parse_tree->str_value_);
    CK (OB_NOT_NULL(parse_tree->children_[0]));
    CK (OB_LIKELY(T_IDENT == parse_tree->children_[0]->type_));
    OX (routine_name.assign_ptr(parse_tree->children_[0]->str_value_,
                                static_cast<int32_t>(parse_tree->children_[0]->str_len_)));
    if (OB_UNLIKELY(routine_name.length() > 
      (lib::is_oracle_mode() ? OB_MAX_PL_IDENT_LENGTH : OB_MAX_MYSQL_PL_IDENT_LENGTH))) {
      ret = OB_ERR_IDENTIFIER_TOO_LONG;
      LOG_USER_ERROR(OB_ERR_IDENTIFIER_TOO_LONG, routine_name.length(), routine_name.ptr());
      LOG_WARN("identifier too long", K(routine_name), K(ret));
    }
    CK (OB_NOT_NULL(routine_table = current_block_->get_namespace().get_routine_table()));
    OZ (routine_table->make_routine_info(resolve_ctx_.allocator_,
                                         routine_name,
                                         routine_type,
                                         routine_decl_str,
                                         unit_ast.get_database_id(),
                                         unit_ast.is_package() || unit_ast.is_object() ? unit_ast.get_id() : static_cast<ObPLFunctionAST&>(unit_ast).get_package_id(),
                                         unit_ast.is_package() || unit_ast.is_object() ? OB_INVALID_ID : static_cast<ObPLFunctionAST&>(unit_ast).get_id(),
                                         unit_ast.is_package() || unit_ast.is_object()
                                           ? dummy_path : static_cast<ObPLFunctionAST&>(unit_ast).get_subprogram_path(),
                                         routine_info));
    if (OB_SUCC(ret) && unit_ast.get_priv_user().length() != 0) {
      routine_info->set_priv_user(unit_ast.get_priv_user());
    }
    if (OB_SUCC(ret) && is_udt_routine) {
      routine_info->set_is_udt_routine();
      int64_t udt_udf_modifier = static_cast<int64_t>(parse_tree->int16_values_[0]);
      if (UdtUdfType::UDT_UDF_STATIC == udt_udf_modifier) {
        routine_info->get_compile_flag().add_static();
      } else if (UdtUdfType::UDT_UDF_MAP == udt_udf_modifier) {
        if (routine_info->is_function()) {
          routine_info->get_compile_flag().add_map();
        } else {
          ret = OB_ERR_ORDER_MAP_NEED_BE_FUNC;
          LOG_WARN("Only a function may be a MAP, ORDER or CONSTRUCTOR method",
                       K(ret), KPC(routine_info));
        }
      } else if (UdtUdfType::UDT_UDF_ORDER == udt_udf_modifier) {
        if (routine_info->is_function()) {
          routine_info->get_compile_flag().add_order();
        } else {
          ret = OB_ERR_ORDER_MAP_NEED_BE_FUNC;
          LOG_WARN("Only a function may be a MAP, ORDER or CONSTRUCTOR method",
                       K(ret), KPC(routine_info));
        }
      } else if (UdtUdfType::UDT_UDF_CONS == udt_udf_modifier) {
        routine_info->set_is_udt_cons();
        if (0 != routine_info->get_name().case_compare(
          current_block_->get_namespace().get_package_name())) {
          ret = OB_ERR_CONS_NAME_ILLEGAL;
          LOG_USER_ERROR(OB_ERR_CONS_NAME_ILLEGAL);
        }
      } else {
        // do nothing
        if (!routine_info->is_udt_cons()
        && 0 == routine_info->get_name().case_compare(
           current_block_->get_namespace().get_package_name())) {
          ret = OB_ERR_FUNC_NAME_SAME_WITH_CONS;
          LOG_WARN("not construtor but name is same as type name", K(ret));
          LOG_USER_ERROR(OB_ERR_FUNC_NAME_SAME_WITH_CONS, routine_info->get_name().length(),
                                                          routine_info->get_name().ptr());
        }
      }
    }
    if (OB_SUCC(ret) && (PACKAGE_FUNCTION == routine_type || NESTED_FUNCTION == routine_type)) {
      ParseNode *type_node = parse_tree->children_[2];
      ObPLRoutineParam *param = NULL;
      ObPLExternTypeInfo extern_type_info;
      CK (OB_NOT_NULL(type_node));
      OZ (resolve_sp_data_type(type_node, ObString(""), unit_ast, ret_type, &extern_type_info));
      // clear warning info while type is self, only in object spec resolve
      if (OB_SUCC(ret) && routine_info->is_udt_routine()
        && 0 == current_block_->get_namespace().get_package_name().case_compare(extern_type_info.type_name_)) {
        OX (ob_reset_tsi_warning_buffer());
      }
      OZ (routine_info->make_routine_param(resolve_ctx_.allocator_,
                                           resolve_ctx_.session_info_.get_dtc_params(),
                                           ObString(""),
                                           ret_type,
                                           PL_PARAM_OUT,
                                           false, //nocopy
                                           ObString(""),
                                           false, //default cast
                                           extern_type_info,
                                           param));
      OX (routine_info->set_ret_info(param));
      OX ((ObPLBlockNS::BlockType::BLOCK_PACKAGE_BODY == current_block_->get_namespace().get_block_type()
          || ObPLBlockNS::BlockType::BLOCK_OBJECT_BODY == current_block_->get_namespace().get_block_type())
            ? routine_info->set_is_private_routine() : void());
    }
    if (OB_SUCC(ret)) {
      const ObObjMeta *meta = ret_type.get_meta_type();
      // check map return type
      if (routine_info->is_udt_map()
      && (ret_type.is_rowtype_type()
          || ret_type.is_type_type()
          || ret_type.is_boolean_type()
          || ret_type.is_urowid_type()
          // || ret_type.is_long_type()
          || ret_type.is_lob_type()
          || ret_type.is_lob_storage_type()))
      {
        ret = OB_ERR_MAP_RET_SCALAR_TYPE;
        LOG_USER_ERROR(OB_ERR_MAP_RET_SCALAR_TYPE);
      }
      if (routine_info->is_udt_order()
      && (ret_type.is_rowtype_type()
         || ret_type.is_type_type()
         || ret_type.is_boolean_type()
         || ret_type.is_real_type())) {
        ret = OB_ERR_ORDER_RET_INT_TYPE;
        LOG_USER_ERROR(OB_ERR_ORDER_RET_INT_TYPE);
      }
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(parse_tree->children_[1])) {
      CK (OB_LIKELY(T_SP_PARAM_LIST == parse_tree->children_[1]->type_));
      OZ (resolve_routine_decl_param_list(parse_tree->children_[1], unit_ast, *routine_info));
    }
    const ObStmtNodeTree *clause_node = NULL;
    if (OB_SUCC(ret)
        && T_SUB_FUNC_DECL == parse_tree->type_
        && 5 == parse_tree->num_child_
        && OB_NOT_NULL(parse_tree->children_[3])) {
      OZ (resolve_sf_clause(
        parse_tree->children_[3], routine_info, routine_type, ret_type));
      OX (clause_node = parse_tree->children_[3]);
    }
    if (OB_SUCC(ret)
        && T_SUB_PROC_DECL == parse_tree->type_
        && 3 == parse_tree->num_child_
        && OB_NOT_NULL(parse_tree->children_[2])) {
      OZ (resolve_sf_clause(
        parse_tree->children_[2], routine_info, routine_type, ret_type));
      OX (clause_node = parse_tree->children_[2]);
    }
    if (OB_SUCC(ret) && routine_info->has_accessible_by_clause()) {
      for (int64_t i = 0;
           OB_SUCC(ret) && OB_NOT_NULL(clause_node) && i < clause_node->num_child_; ++i) {
        const ObStmtNodeTree *child = clause_node->children_[i];
        if (OB_NOT_NULL(child) && T_SP_ACCESSIBLE_BY == child->type_) {
          const ObStmtNodeTree *accessor_list = NULL;
          CK (1 == child->num_child_);
          OX (accessor_list = child->children_[0]);
          CK (OB_NOT_NULL(accessor_list));
          CK (T_SP_ACCESSOR_LIST == accessor_list->type_);
          OZ (resolve_accessible_by(accessor_list, routine_info->get_accessors()));
        }
      }
    }
    if (OB_SUCC(ret) && routine_info->has_generic_type() && resolve_routine_def) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("NOTICE: Routine use Generic Type not Implement with Interface not supported",
                K(ret), K(resolve_routine_def), K(routine_info->has_generic_type()));
      LOG_USER_ERROR(OB_NOT_SUPPORTED,
        "NOTICE: Routine use Generic Type not Implement with Interface");
    }
    const ObPLRoutineInfo *exist = NULL;
    OZ (current_block_->get_namespace().get_routine_info(routine_info, exist));
    if (OB_SUCC(ret) && resolve_routine_def) {
      for (int64_t i = 0; OB_SUCC(ret) && i <routine_info->get_param_count(); ++i) {
        if (OB_NOT_NULL(routine_info->get_params().at(i)->get_type().get_data_type())
            && routine_info->get_params().at(i)->get_type().get_data_type()->get_charset_type()
            == CHARSET_ANY) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("character set ANY_CS is only supported defined by pragma interface", K(ret));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "character set ANY_CS defined using method other than pragma interface");
        }
      }
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(exist)) {
      if (resolve_routine_def) {
        ObPLFunctionAST *routine_ast = NULL;
        int64_t idx = OB_INVALID_INDEX;
        OZ (exist->get_idx(idx));
        OZ (routine_table->get_routine_ast(idx, routine_ast));
        if (OB_SUCC(ret) && OB_NOT_NULL(routine_ast)) { // 已经定义过函数体,不可以重复定义
          ret = OB_ERR_EXIST_OBJECT;
          LOG_WARN("already has same routine in package", K(ret));
        }
      } else { // 已经声明过函数,不可以重复声明
        ret = OB_ERR_ATTR_FUNC_CONFLICT;
        LOG_WARN("already has same routine in package", K(ret));
        LOG_USER_ERROR(OB_ERR_ATTR_FUNC_CONFLICT,
                       exist->get_name().length(), exist->get_name().ptr());
      }
    }
    if (OB_SUCC(ret) && NULL == exist) { // 如果已经声明过, 不需要重复加入TABLE
      const ObPLRoutineInfo *parent_routine_info = NULL;
      // NOTICE: only package or object body need search parent_routine_info
      if ((ObPLBlockNS::BlockType::BLOCK_PACKAGE_BODY
            == current_block_->get_namespace().get_block_type()
          && OB_NOT_NULL(external_ns_.get_parent_ns())
          && ObPLBlockNS::BlockType::BLOCK_PACKAGE_SPEC
              == external_ns_.get_parent_ns()->get_block_type())
          || (ObPLBlockNS::BlockType::BLOCK_OBJECT_BODY
            == current_block_->get_namespace().get_block_type()
          && OB_NOT_NULL(external_ns_.get_parent_ns())
          && ObPLBlockNS::BlockType::BLOCK_OBJECT_SPEC
              == external_ns_.get_parent_ns()->get_block_type())) {
        OZ (external_ns_.get_parent_ns()->get_routine_info(routine_info, parent_routine_info));
      }
      if (OB_FAIL(ret)) {
      } else if (NULL != parent_routine_info) {  // public routine
        if (parent_routine_info->is_pipelined() != routine_info->is_pipelined()
            || (routine_info->is_deterministic() && !parent_routine_info->is_deterministic())
            || (routine_info->is_parallel_enable() && !parent_routine_info->is_parallel_enable())
            || (routine_info->is_result_cache() && !parent_routine_info->is_result_cache())) {
          ret = OB_ERR_ITEM_NOT_IN_BODY;
          LOG_WARN("PLS-00323: subprogram or cursor is declared"
                   "in a package specification and must be defined in the package body",
                   K(ret), KPC(routine_info), KPC(exist));
        }
        if (OB_SUCC(ret)
            && (routine_info->has_accessible_by_clause()
                  != parent_routine_info->has_accessible_by_clause()
                || !is_array_equal(
                  routine_info->get_accessors(), parent_routine_info->get_accessors()))) {
          ret = OB_ERR_MISMATCH_SUBPROGRAM;
          LOG_WARN("PLS-00263: mismatch between string on a subprogram specification and body",
                   K(ret), KPC(routine_info), KPC(parent_routine_info));
        }
        OX (routine_info->set_compile_flag(parent_routine_info->get_compile_flag()));
        OZ (current_block_->get_namespace().set_routine_info(parent_routine_info->get_id(), routine_info));
      } else { // private routine
        if (resolve_routine_def && routine_info->has_accessible_by_clause()) {
          ret = OB_ERR_MISMATCH_SUBPROGRAM;
          LOG_WARN("PLS-00263: mismatch between string on a subprogram specification and body",
                   K(ret));
        }
        // need set line info
        OX (routine_info->set_loc(combine_line_and_col(parse_tree->stmt_loc_)));
        OZ (current_block_->get_namespace().add_routine_info(routine_info));
      }
    }
    if ((OB_FAIL(ret) && OB_NOT_NULL(routine_info))
        || OB_NOT_NULL(exist)) {
      routine_info->~ObPLRoutineInfo();
      resolve_ctx_.allocator_.free(routine_info);
      routine_info = const_cast<ObPLRoutineInfo*>(exist);
    }
  }
  if (OB_FAIL(ret)) {
    record_error_line(parse_tree, resolve_ctx_.session_info_);
  }
  return ret;
}

int ObPLResolver::resolve_routine_block(const ObStmtNodeTree *parse_tree,
                                        const ObPLRoutineInfo &routine_info,
                                        ObPLFunctionAST &routine_ast)
{
  int ret = OB_SUCCESS;
  const ParseNode *routine_block = parse_tree;
  if (OB_ISNULL(routine_block)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("routine_block is invalid", K(ret));
  } else {
    ObPLResolver resolver(resolve_ctx_.allocator_, resolve_ctx_.session_info_, resolve_ctx_.schema_guard_,
                          resolve_ctx_.package_guard_, resolve_ctx_.sql_proxy_, expr_factory_,
                          &current_block_->get_namespace(), resolve_ctx_.is_prepare_protocol_,
                          false/*is_check_mode_ = false*/, false/*bool is_sql_scope_ = false*/,
                          resolve_ctx_.params_.param_list_);
    // note: init函数中引用了resolver的external_ns_, 而resolver是一个栈变量，使用的时候需要小心
    if (OB_FAIL(resolver.init(routine_ast))) {
      LOG_WARN("routine init failed ", K(ret));
    } else if (OB_FAIL(resolver.init_default_exprs(routine_ast, routine_info.get_params()))) {
      LOG_WARN("routine init default exprs failed", K(ret));
    } else {
      const ObPLRoutineInfo *parent_routine_info = NULL;
      // NOTICE: only package body need search parent_routine_info
      if ((ObPLBlockNS::BlockType::BLOCK_PACKAGE_BODY
            == current_block_->get_namespace().get_block_type()
          && OB_NOT_NULL(external_ns_.get_parent_ns())
          && ObPLBlockNS::BlockType::BLOCK_PACKAGE_SPEC
              == external_ns_.get_parent_ns()->get_block_type())
          || (ObPLBlockNS::BlockType::BLOCK_OBJECT_BODY
            == current_block_->get_namespace().get_block_type()
          && OB_NOT_NULL(external_ns_.get_parent_ns())
          && ObPLBlockNS::BlockType::BLOCK_OBJECT_SPEC
              == external_ns_.get_parent_ns()->get_block_type())) {
        OZ (external_ns_.get_parent_ns()->get_routine_info(&routine_info, parent_routine_info));
      }
      if (NULL != parent_routine_info &&
            OB_FAIL(resolver.check_params_legal_in_body_routine(routine_ast,
                                                                parent_routine_info,
                                                                &routine_info))) {
        LOG_WARN("param ilegal.", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(resolver.resolve_root(routine_block, routine_ast))) {
        LOG_WARN("resolve routine block failed", K(routine_block->type_), K(ret));
      } else {
        OX (const_cast<ObPLBlockNS &>(routine_ast.get_body()->get_namespace()).set_external_ns(NULL));
      }
    }
    if (resolve_ctx_.session_info_.is_pl_debug_on()) {
      if (OB_FAIL(routine_ast.generate_symbol_debuginfo())) {
        LOG_WARN("failed to generate symbol debuginfo", K(ret));
      }
    }
  }
  return ret;
}

int ObPLResolver::resolve_routine_def(const ObStmtNodeTree *parse_tree,
                                      ObPLCompileUnitAST &unit_ast,
                                      bool is_udt_routine)
{
  int ret = OB_SUCCESS;
  CK (OB_LIKELY(T_SUB_PROC_DEF == parse_tree->type_ || T_SUB_FUNC_DEF == parse_tree->type_));
  CK (OB_LIKELY(2 == parse_tree->num_child_));
  CK (OB_NOT_NULL(parse_tree->children_));
  CK (OB_NOT_NULL(parse_tree->children_[1]));
  if (OB_SUCC(ret)) {
    ParseNode *routine_decl = parse_tree->children_[0];
    ObPLRoutineInfo *routine_info = NULL;
    ObPLRoutineTable &routine_table = unit_ast.get_routine_table();
    ObPLFunctionAST *routine_ast = NULL;
    ObString route_sql;
    ObString routine_body(
      parse_tree->children_[1]->str_len_, parse_tree->children_[1]->str_value_);
    int64_t idx = OB_INVALID_INDEX;
    OZ (resolve_routine_decl(routine_decl, unit_ast, routine_info, is_udt_routine, true));
    CK (OB_NOT_NULL(routine_info));
    OZ (routine_info->get_idx(idx));
    OZ (routine_table.get_routine_ast(idx, routine_ast));
    if (OB_SUCC(ret) && OB_NOT_NULL(routine_ast)) {
      ret = OB_ERR_SP_DUP_VAR;
      LOG_USER_ERROR(OB_ERR_SP_DUP_VAR,
          routine_ast->get_name().length(), routine_ast->get_name().ptr());
    }
    if (OB_SUCC(ret)) {
      OZ (routine_info->add_compile_flag(unit_ast.get_compile_flag()));
    }
    OZ (routine_table.make_routine_ast(resolve_ctx_.allocator_,
                                       unit_ast.get_db_name(),
                                       (unit_ast.is_package() || unit_ast.is_object())
                                        ? unit_ast.get_name()
                                          : current_block_->get_namespace().get_package_name(),
                                       unit_ast.get_version(),
                                       *routine_info,
                                       routine_ast));
    if (OB_SUCC(ret)) {
      if (unit_ast.is_object()) {
        routine_ast->set_is_udt_routine();
      }
      if (routine_info->is_udt_cons()) {
        routine_ast->set_is_udt_cons();
      }
    }
    OZ (resolve_routine_block(parse_tree->children_[1], *routine_info, *routine_ast));
    OX (routine_ast->get_body()->set_location(
      parse_tree->stmt_loc_.first_line_, parse_tree->stmt_loc_.first_column_));
    OZ (ObPLRouter::analyze_stmt(routine_ast->get_body(), route_sql));
    if (OB_FAIL(ret)) {
    } else if ((ObPLBlockNS::BlockType::BLOCK_PACKAGE_BODY
        == current_block_->get_namespace().get_block_type()
        && OB_NOT_NULL(external_ns_.get_parent_ns())
        && ObPLBlockNS::BlockType::BLOCK_PACKAGE_SPEC
          == external_ns_.get_parent_ns()->get_block_type())) {
        const ObPLRoutineInfo *decl_routine_info = nullptr;
        OZ (external_ns_.get_parent_ns()->get_routine_info(routine_info, decl_routine_info));
        if (OB_FAIL(ret)) {
        } else if (decl_routine_info != nullptr) {
          OX (const_cast<ObPLRoutineInfo*>(decl_routine_info)->set_route_sql(route_sql));
        }
    }
    OX (routine_info->set_route_sql(route_sql));
    OX (routine_info->set_routine_body(routine_body));
    OZ (routine_table.set_routine_ast(idx, routine_ast));
    OX (unit_ast.add_dependency_objects(routine_ast->get_dependency_table()));
    if (OB_SUCC(ret) && unit_ast.get_can_cached()) {
      OX (unit_ast.set_can_cached(routine_ast->get_can_cached()));
    }
    if (OB_SUCC(ret)) {
      if (unit_ast.is_modifies_sql_data()) {
        // do nothing
      } else if (routine_ast->is_modifies_sql_data()) {
        unit_ast.set_modifies_sql_data();
      } else if (unit_ast.is_reads_sql_data()) {
        // do nothing
      } else if (routine_ast->is_reads_sql_data()) {
        unit_ast.set_reads_sql_data();
      } else if (unit_ast.is_contains_sql()) {
        // do nothing
      } else if (routine_ast->is_contains_sql()) {
        unit_ast.set_contains_sql();
      } else if (routine_ast->is_no_sql()) {
        unit_ast.set_no_sql();
      }
      if (routine_ast->is_wps()) {
        unit_ast.set_wps();
      }
      if (routine_ast->is_rps()) {
        unit_ast.set_rps();
      }
      if (routine_ast->is_has_sequence()) {
        unit_ast.set_has_sequence();
      }
      /*if (routine_ast->is_has_out_param()) {
        unit_ast.set_has_out_param();
      }*/
      if (routine_ast->is_external_state()) {
        unit_ast.set_external_state();
      }
    }
    OX (routine_info->set_analyze_flag(routine_ast->get_analyze_flag()));
    // 将routine param中的外部类型加入到当前namespace的type table中。
    for (int64_t i = 0; OB_SUCC(ret) && i < routine_info->get_param_count(); ++i) {
      ObPLRoutineParam *param = routine_info->get_params().at(i);
      CK (OB_NOT_NULL(param));
      if (OB_SUCC(ret) && OB_INVALID_ID != param->get_type().get_user_type_id()) {
        const ObUserDefinedType *user_type = NULL;
        OZ (unit_ast.get_body()->get_namespace().get_pl_data_type_by_id(param->get_type().get_user_type_id(), user_type));
        CK (OB_NOT_NULL(user_type));
        ObSEArray<ObDataType, 8> types;
        OZ (routine_ast->get_body()->get_namespace().expand_data_type(user_type, types));
        OZ (routine_ast->get_body()->get_namespace().get_type_table()->add_external_type(user_type));
      }
    }
    if (OB_FAIL(ret) && OB_NOT_NULL(routine_ast)) {
      routine_ast->~ObPLFunctionAST();
      resolve_ctx_.allocator_.free(routine_ast);
      routine_ast = NULL;
    }
  }
  return ret;
}

int ObPLResolver::resolve_init_routine(const ObStmtNodeTree *parse_tree, ObPLPackageAST &package_ast)
{
  int ret = OB_SUCCESS;
  ObPLRoutineTable &routine_table = package_ast.get_routine_table();
  ObString name("__init__");
  ObString empty_decl_str;
  ObArray<ObPLRoutineParam *> empty_params;
  ObPLRoutineInfo *routine_info = NULL;
  ObPLFunctionAST *routine_ast = NULL;
  if (OB_FAIL(routine_table.make_routine_info(resolve_ctx_.allocator_,
                                                     name,
                                                     PACKAGE_PROCEDURE,
                                                     empty_decl_str,
                                                     package_ast.get_database_id(),
                                                     package_ast.get_id(),
                                                     ObPLRoutineTable::INIT_ROUTINE_IDX,
                                                     ObArray<int64_t>(),
                                                     routine_info))) {
    LOG_WARN("make routine info failed", K(ret));
  } else if (OB_FAIL(routine_table.make_routine_ast(resolve_ctx_.allocator_,
                                                    package_ast.get_db_name(),
                                                    package_ast.get_name(),
                                                    package_ast.get_version(),
                                                    *routine_info,
                                                    routine_ast))) {
    LOG_WARN("make routine ast failed", K(ret));
  } else if (OB_FAIL(resolve_routine_block(parse_tree, *routine_info, *routine_ast))) {
    LOG_WARN("resolve routine block failed", K(ret));
  } else if (OB_FAIL(package_ast.add_dependency_objects(routine_ast->get_dependency_table()))) {
    LOG_WARN("add dependency table failed", K(ret));
  } else {
    routine_ast->get_body()->set_location(
      parse_tree->stmt_loc_.first_line_, parse_tree->stmt_loc_.first_column_);
    routine_table.set_init_routine_info(routine_info);
    routine_table.set_init_routine_ast(routine_ast);
  }
  return ret;
}

int ObPLResolver::resolve_goto(const ObStmtNodeTree *parse_tree,
                               ObPLGotoStmt *stmt,
                               ObPLCompileUnitAST &unit_ast)
{
  UNUSED(unit_ast);
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(parse_tree)) {
    const ObStmtNodeTree *dst_label = parse_tree->children_[0];
    ObString label;
    if (OB_FAIL(resolve_ident(dst_label, label))) {
      LOG_WARN("failed to resolve ident", K(ret));
    } else {
      stmt->set_dst_label(label);
      current_block_->set_is_contain_goto_stmt(true);
      OX (push_goto_stmts(stmt));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("goto stmt node is null", K(ret));
  }
  return ret;
}

int ObPLResolver::check_goto(ObPLGotoStmt *stmt)
{
  int ret = OB_SUCCESS;
  int64_t label_idx = OB_INVALID_INDEX;
  bool is_dup_label = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null goto stmt", K(ret));
  } else if (OB_FAIL(resolve_label(stmt->get_dst_label(),
                                   stmt->get_block()->get_namespace(),
                                   label_idx, false))) {
    if (ret == OB_ERR_SP_LILABEL_MISMATCH) {
      const ObPLBlockNS& ns = stmt->get_block()->get_namespace();
      const ObPLLabelTable *label_table = ns.get_label_table();
      const ObString& name = stmt->get_dst_label();
      bool found = false;
      int iret = OB_SUCCESS;
      // search the label in global label table
      for (int64_t i = 0; OB_SUCCESS == iret && i < label_table->get_count(); ++i) {
        const ObString *label_name = label_table->get_label(i);
        if (OB_ISNULL(label_name)) {
          iret = OB_ERR_UNEXPECTED;
          LOG_WARN("label is NULL", K(label_name), K(label_idx), K(ret));
        } else if (0 == name.case_compare(*label_name)) {
          found = true;
          break;
        } else { /*do nothing*/ }
      }
      if (OB_SUCCESS == iret && found) {
        // errno for illeagal goto label
        ret = OB_ERR_GOTO_BRANCH_ILLEGAL;
        LOG_WARN("goto label is illeagal", K(name), K(label_idx), K(ret));
        LOG_USER_ERROR(OB_ERR_GOTO_BRANCH_ILLEGAL, name.length(), name.ptr());
      } else {
        // errno for non-existing label
        ret = OB_ERR_INVALID_GOTO;
        LOG_WARN("Invalid GOTO to non-label", K(name), K(label_idx), K(ret));
        LOG_USER_ERROR(OB_ERR_INVALID_GOTO, name.length(), name.ptr());
      }
    }
  } else if (OB_INVALID_INDEX == label_idx) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cann't find goto destination label", K(ret));
  } else if (OB_FAIL(stmt->get_block()->get_namespace().check_dup_goto_label(stmt->get_dst_label(), is_dup_label))) {
    LOG_WARN("failed to check goto label dup", K(stmt->get_dst_label()), K(ret));
  } else if (is_dup_label) {
    ret = OB_ERR_DECL_MORE_THAN_ONCE;
    LOG_WARN("label is declared duplicately in this scope", K(stmt->get_dst_label()), K(label_idx), K(ret));
    LOG_USER_ERROR(OB_ERR_DECL_MORE_THAN_ONCE, stmt->get_dst_label().length(), stmt->get_dst_label().ptr());
  } else {
    ObPLLabelTable *pl_label = current_block_->get_namespace().get_label_table();
    if (OB_NOT_NULL(pl_label)) {
      enum GotoRestrictionType restrict_type = RESTRICTION_NO_RESTRICT;
      ObPLStmt *ls = pl_label->get_next_stmt(label_idx);
      if (OB_ISNULL(ls)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get destination stmt", K(ret));
      } else if (OB_FAIL(verify_goto_stmt_restriction(*stmt, *ls, restrict_type))) {
        LOG_WARN("failed to verify goto stmt", K(ret));
      } else if (RESTRICTION_NO_RESTRICT != restrict_type) {
        ret = OB_ERR_GOTO_BRANCH_ILLEGAL;
        LOG_WARN("illegal GOTO statement; this GOTO cannot branch to label",
                                                   K(*(pl_label->get_label(label_idx))));
        LOG_USER_ERROR(OB_ERR_GOTO_BRANCH_ILLEGAL, pl_label->get_label(label_idx)->length(),
                                                   pl_label->get_label(label_idx)->ptr());
      } else if (OB_FAIL(check_goto_cursor_stmts(*stmt, *ls))) {
        LOG_WARN("failed to check goto cursor stmts", K(ret));
      } else if (RESTRICTION_NO_RESTRICT == restrict_type) {
        pl_label->set_is_goto_dst(label_idx ,true);
        // 多标签的场景下，在resolve函数中给goto 目的stmt赋值的可能是其它标签，所以这里需要更新一下
        // <<lab1>> <<lab2>> stmt; goto lab2 类似这种，stmt的标签idx指向lab1，所以这里更新为lab2的idx.
        // 但是lab1 和lab2的关联stmt都是一样的，所以能通过lab2找到对应的stmt.
        ls->update_label_index(label_idx);
        stmt->set_dst_stmt(ls);
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("cann't branch to the destination", K(*ls), K(*stmt));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("labe talbe is not found", K(ret));
    }
  }
  return ret;
}

// 这个函数主要解决goto跳出cursor for loop的时候，需要关闭对应的cursor，这里记录跳出的相关cursor stmts
// 因为这个for loop可以多层嵌套，所以需要记录跳出了多少层。
int ObPLResolver::check_goto_cursor_stmts(ObPLGotoStmt &goto_stmt, const ObPLStmt &dst_stmt)
{
  int ret = OB_SUCCESS;

  const ObPLStmtBlock *goto_block = goto_stmt.get_block();
  const ObPLStmtBlock *dst_block = dst_stmt.get_block();
  const ObPLStmtBlock *parent_blk = NULL;
  if (OB_ISNULL(goto_block) || OB_ISNULL(dst_block)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("goto or dst block is null", K(ret), K(goto_block), K(dst_block));
  } else if (goto_block == dst_block) {
    // do nothing
  } else {
    // 这里一定是包含关系，dst_block包含了goto_blk，因为前面verify过了。
    parent_blk = goto_block;
    bool exit_flag = false;
    const ObPLCursorForLoopStmt *cur_level_forloop = NULL;
    int loopcnt = 0;
    do {
      exit_flag = parent_blk == dst_block;
      const ObIArray<ObPLStmt *> &stmts = parent_blk->get_cursor_stmts();
      for (int64_t i = 0; OB_SUCC(ret) && i < stmts.count(); ++i) {
        const ObPLCursorForLoopStmt *cfl_stmt = static_cast<ObPLCursorForLoopStmt *>(stmts.at(i));
        CK (OB_NOT_NULL(cfl_stmt));
        // 这里有一个隐含的不变量就是，多层for loop cursor，每层有且只有1或0个for loop cursor需要关闭cursor
        if (OB_FAIL(ret)) {
        } else if (cfl_stmt->is_contain_goto_stmt() || cfl_stmt->get_body()->is_contain_stmt(cur_level_forloop)) {
          if (OB_FAIL(goto_stmt.push_cursor_stmt(cfl_stmt))) {
            LOG_WARN("failed to push stmt", K(ret));
            break;
          }
          cur_level_forloop = cfl_stmt;
          break;
        } else if (OB_NOT_NULL(cur_level_forloop)) {
          bool is_contain = false;
          OZ (check_contain_cursor_loop_stmt(cfl_stmt->get_body(), cur_level_forloop, is_contain));
          if (OB_SUCC(ret) && is_contain) {
            if (OB_FAIL(goto_stmt.push_cursor_stmt(cfl_stmt))) {
              LOG_WARN("failed to push stmt", K(ret));
              break;
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        parent_blk = parent_blk->get_block();
        loopcnt++;
      }
    } while(OB_SUCC(ret) && OB_NOT_NULL(parent_blk) && !exit_flag);
    // loop invarant
    CK (goto_stmt.get_cursor_stmt_count() <= loopcnt);
  }
  return ret;
}

int ObPLResolver::check_contain_cursor_loop_stmt(const ObPLStmtBlock *stmt_block, 
                                                 const ObPLCursorForLoopStmt *cur_loop_stmt, 
                                                 bool &is_contain)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObPLStmt *> &cursor_stmts = stmt_block->get_cursor_stmts();
  const ObIArray<ObPLStmt*> &stmts = stmt_block->get_stmts();
  bool stop = false;
  CK (OB_NOT_NULL(stmt_block));
  CK (OB_NOT_NULL(cur_loop_stmt));
  OX (stop = (stmt_block->get_level() == cur_loop_stmt->get_level()));
  for (int64_t i = 0; OB_SUCC(ret) && !is_contain && i < cursor_stmts.count(); i++) {
    const ObPLCursorForLoopStmt *cfl_stmt = static_cast<ObPLCursorForLoopStmt *>(cursor_stmts.at(i));
    CK (OB_NOT_NULL(cfl_stmt));
    if (OB_SUCC(ret) && cfl_stmt->get_stmt_id() == cur_loop_stmt->get_stmt_id()) {
      is_contain = true;
      break;
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && !stop && !is_contain && i < stmts.count(); i++) {
    CK (OB_NOT_NULL(stmts.at(i)));
    if (OB_SUCC(ret) && PL_BLOCK == stmts.at(i)->get_type()) {
      OZ (check_contain_cursor_loop_stmt(static_cast<ObPLStmtBlock *>(stmts.at(i)), cur_loop_stmt, is_contain));
      if (OB_SUCC(ret) && is_contain) {
        break;
      }
    }
  }
  return ret;
}

int ObPLResolver::verify_goto_stmt_restriction(const ObPLStmt &goto_stmt,
                                               const ObPLStmt &dst_stmt,
                                               GotoRestrictionType &result)
{
  int ret = OB_SUCCESS;
  result = RESTRICTION_NO_RESTRICT;
  // A GOTO statement cannot transfer control into an IF statement, CASE statement,
  // LOOP statement, or sub-block.
  // namespace已经做了控制，resolve label的时候，会找不到label

  // A GOTO statement cannot transfer control from one IF statement clause to
  // another, or from one CASE statement WHEN clause to another.
  // namespace已经做了控制，resolve label的时候，会找不到label

  // A GOTO statement cannot transfer control out of a subprogram.
  // namespace已经做了控制，resolve label的时候，会找不到label

  // A GOTO statement cannot transfer control into an exception handler.
  // namespace已经做了控制，resolve label的时候，会找不到label

  // A GOTO statement cannot transfer control from an exception handler back into the
  // current block (but it can transfer control from an exception handler into an
  // enclosing block).
  // for example
  /*
  * case1
  *
  * begin
  *   begin
  *     <<aa>>
  *     some instr
  *   exception
  *     when no_dat_found
  *       goto <<aa>>
  *   end;
  * end;
  *
  * * * *
  * case2
  *
  * begin
  *   begin
  *     <<aa>>
  *     some instr
  *   exception
  *     when no_dat_found
  *       goto <<bb>>
  *   end;
  * <<bb>>
  *   begin
  *     some instr
  *   end;
  * end;
  *
  * * * *
  * case3
  *
  * begin
  * <<cc>>
  *   instr
  *   begin
  *     <<aa>>
  *     some instr
  *   exception
  *     when no_dat_found
  *       goto <<cc>>
  *   end;
  * <<bb>>
  *   begin
  *     some instr
  *   end;
  * end;
  */
  // case1 is illegal, case2, case3 is legal
  const ObPLStmtBlock *blk = goto_stmt.get_block();
  if (OB_ISNULL(blk)) {
    // do nothing
  } else if (blk->in_handler()) {
    if (!blk->is_contain_stmt(&dst_stmt)) {
      // result = RESTRICTION_JUMP_OUT_EXCEPTION;
      bool is_hit = false;
      // tracking back to parent recursively,
      // to see if this goto stmt is transfer control to other encolsing block
      const ObPLStmtBlock *parent = blk->get_block();
      while (OB_NOT_NULL(parent)) {
        if (parent->is_contain_stmt(&dst_stmt) && !parent->in_handler()) {
          is_hit = true;
          break;
        }
        if (parent->get_namespace().explicit_block()) {
          break;
        } else {
          parent = parent->get_block();
        }
      }
      if (is_hit) {
        result = RESTRICTION_JUMP_OUT_EXCEPTION;
      }
    } else {
      // do nothing ...
    }
  } else {
    // do nothing ...
  }
  return ret;
}

int64_t ObPLResolver::combine_line_and_col(const ObStmtLoc &loc)
{
  int64_t bloc = static_cast<int64_t>(loc.first_line_);
  bloc = (bloc<<32) + static_cast<int64_t>(loc.first_column_);
  return bloc;
}

int ObPLResolver::replace_map_or_order_expr(
  uint64_t udt_id, ObRawExpr *&expr, ObPLCompileUnitAST &func)
{
  int ret = OB_SUCCESS;
  ObSEArray<const ObRoutineInfo *, 2> routine_infos;
  const ObRoutineInfo* routine_info = NULL;
  ObRawExpr *left = expr->get_param_expr(0);
  ObRawExpr *right = expr->get_param_expr(1);
  const ObUserDefinedType *user_type = NULL;
  OZ (resolve_ctx_.schema_guard_.get_routine_infos_in_udt(
                  get_tenant_id_by_object_id(udt_id), udt_id, routine_infos));
  OZ (resolve_ctx_.get_user_type(udt_id, user_type));
  CK (OB_NOT_NULL(user_type));
  for (int64_t i = 0; OB_SUCC(ret) && i < routine_infos.count(); ++i) {
    if (routine_infos.at(i)->is_udt_order()) {
      CK (OB_ISNULL(routine_info));
      OX (routine_info = routine_infos.at(i));
    } else if (routine_infos.at(i)->is_udt_map()) {
      CK (OB_ISNULL(routine_info));
      OX (routine_info = routine_infos.at(i));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_NOT_NULL(routine_info)) {
    const ObString &routine_name = routine_info->get_routine_name();
    ObObjAccessIdent left_access_ident(routine_name);
    ObSEArray<ObObjAccessIdx, 1> left_idxs;
    OZ (left_idxs.push_back(ObObjAccessIdx(*user_type,
                                           ObObjAccessIdx::AccessType::IS_EXPR,
                                           ObString(""),
                                           *user_type,
                                           reinterpret_cast<int64_t>(left))));
    OX (left_access_ident.set_pl_udf());
    OX (left_access_ident.access_name_ = routine_name);
    if (routine_info->is_udt_order()) {
      OZ (left_access_ident.params_.push_back(std::make_pair(right, 0)));
    }
    CK (OB_NOT_NULL(current_block_));
    OZ (init_udf_info_of_accessident(left_access_ident));
    OZ (resolve_access_ident(left_access_ident, current_block_->get_namespace(), expr_factory_, &resolve_ctx_.session_info_, left_idxs, func, true));
    CK (left_idxs.at(left_idxs.count() - 1).is_udf_type());
    OX (left = left_idxs.at(left_idxs.count() - 1).get_sysfunc_);
    if (OB_FAIL(ret)) {
    } else if (routine_info->is_udt_order()) {
      ObObjType result_type = left->get_result_type().get_type();
      ObConstRawExpr *const_expr = NULL;
      if (left->get_result_type().get_type() == ObNumberType) {
        number::ObNumber zero;
        OZ (zero.from(static_cast<int64_t>(0), resolve_ctx_.allocator_));
        OZ (ObRawExprUtils::build_const_number_expr(expr_factory_, ObNumberType, zero, const_expr));
      } else {
        OZ (ObRawExprUtils::build_const_int_expr(expr_factory_, left->get_result_type().get_type(), 0, const_expr));
      }
      OX (right = const_expr);
    } else {
      ObObjAccessIdent right_access_ident(routine_name);
      ObSEArray<ObObjAccessIdx, 1> right_idxs;
      OX (right_access_ident.set_pl_udf());
      OX (right_access_ident.access_name_ = routine_name);
      OZ (init_udf_info_of_accessident(right_access_ident));
      OZ (right_idxs.push_back(ObObjAccessIdx(*user_type,
                               ObObjAccessIdx::AccessType::IS_EXPR,
                               ObString(""),
                               *user_type,
                               reinterpret_cast<int64_t>(right))));
      OZ (resolve_access_ident(right_access_ident, current_block_->get_namespace(), expr_factory_, &resolve_ctx_.session_info_, right_idxs, func, true));
      CK (right_idxs.at(right_idxs.count() - 1).is_udf_type());
      OX (right = right_idxs.at(right_idxs.count() - 1).get_sysfunc_);
    }
    CK (OB_NOT_NULL(static_cast<ObOpRawExpr*>(expr)));
    OZ (static_cast<ObOpRawExpr*>(expr)->replace_param_expr(0, left));
    OZ (static_cast<ObOpRawExpr*>(expr)->replace_param_expr(1, right));
    OZ (expr->formalize(&resolve_ctx_.session_info_));
  } else {
    ret = OB_ERR_NO_ORDER_MAP;
    LOG_WARN("A MAP or ORDER function is required for comparing objects in PL/SQL", K(ret));
  }
  return ret;
}


int ObPLResolver::replace_object_compare_expr(ObRawExpr *&expr, ObPLCompileUnitAST &unit_ast)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(expr));
  for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
    OZ (SMART_CALL(replace_object_compare_expr(expr->get_param_expr(i),unit_ast)));
  }
  if (OB_FAIL(ret)) {
  } else if (IS_COMMON_COMPARISON_OP(expr->get_expr_type())) {
    CK (2 == expr->get_param_count());
    CK (OB_NOT_NULL(expr->get_param_expr(0)));
    CK (OB_NOT_NULL(expr->get_param_expr(1)));
    if (OB_FAIL(ret)) {
    } else if (expr->get_param_expr(0)->get_result_type().is_ext()
              && expr->get_param_expr(0)->get_result_type().get_udt_id() != OB_INVALID_ID
              && expr->get_param_expr(1)->get_result_type().is_ext()
              && expr->get_param_expr(1)->get_result_type().get_udt_id() != OB_INVALID_ID) {
      const ObUserDefinedType *l_type = NULL;
      const ObUserDefinedType *r_type = NULL;
      CK (OB_NOT_NULL(current_block_));
      OZ (current_block_->get_namespace().get_user_type(expr->get_param_expr(0)->get_result_type().get_udt_id(), l_type));
      OZ (current_block_->get_namespace().get_user_type(expr->get_param_expr(1)->get_result_type().get_udt_id(), r_type));
      CK (OB_NOT_NULL(l_type));
      CK (OB_NOT_NULL(r_type));
      if (OB_FAIL(ret)) {
      } else if (l_type->get_user_type_id() != r_type->get_user_type_id()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("compare between two different udt type", K(ret), K(l_type), K(r_type));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "compare between two different composite type");
      } else if (l_type->is_object_type() && r_type->is_object_type()) {
        OZ (replace_map_or_order_expr(l_type->get_user_type_id(), expr, unit_ast));
      }
    }
  }
  return ret;
}

int ObPLResolver::record_error_line(const ObStmtNodeTree *parse_tree,
                                    ObSQLSessionInfo &session_info)
{
  int ret = OB_SUCCESS;
  if (NULL != parse_tree &&
      (0 < parse_tree->stmt_loc_.first_line_ || 0 < parse_tree->stmt_loc_.first_column_)) {
    ret = record_error_line(session_info,
                            parse_tree->stmt_loc_.first_line_,
                            parse_tree->stmt_loc_.first_column_);
  }
  return ret;
}

int ObPLResolver::record_error_line(ObSQLSessionInfo &session_info,
                                    int32_t line,
                                    int32_t col)
{
  int ret = OB_SUCCESS;
  // comp oracle, oracle line is begin with 1 while ob is begin with 0
  line++;
  col++;
  const ObWarningBuffer *buf = common::ob_get_tsi_warning_buffer();
  if (NULL != buf && buf->get_error_line() == 0) {
      ObSqlString &err_msg = session_info.get_pl_exact_err_msg();
      err_msg.reset();
      if (OB_FAIL(err_msg.append_fmt("\nat "))) {
        LOG_WARN("fail to get call stack name.", K(ret));
      } else if (OB_FAIL(err_msg.append_fmt(" line : %d, col : %d", line, col))) {
        LOG_WARN("fail to append line info.", K(ret), K(line), K(col));
      } else {
        LOG_DEBUG("exact error msg: ", K(line), K(col));
      }
      LOG_USER_ERROR_LINE_COLUMN(line, col);
  }
  return ret;
}

int ObPLResolveCtx::get_user_type(uint64_t type_id, const ObUserDefinedType *&user_type, ObIAllocator *allocator) const
{
  int ret = OB_SUCCESS;

  SET_LOG_CHECK_MODE();

  UNUSED(allocator);
  // 首先尝试下是不是UDT Type
  const ObUDTTypeInfo *udt_info = NULL;
  const uint64_t tenant_id = get_tenant_id_by_object_id(type_id);
  OZ (schema_guard_.get_udt_info(tenant_id, type_id, udt_info), type_id);
  if (OB_NOT_NULL(udt_info)) {
    OZ (udt_info->transform_to_pl_type(allocator_, user_type), type_id);
  } else { // 其次尝试下是不是Table Type
    ret = OB_SUCCESS;
    const ObTableSchema* table_schema = NULL;
    const uint64_t tenant_id = session_info_.get_effective_tenant_id();
    OZ (schema_guard_.get_table_schema(tenant_id, type_id, table_schema), type_id);
    if (OB_NOT_NULL(table_schema)) {
      ObRecordType* record_type = NULL;
      OZ (ObPLResolver::build_record_type_by_schema(*this, table_schema, record_type), type_id);
      CK (OB_NOT_NULL(record_type));
      OX (user_type = record_type);
    } else if (type_id != OB_INVALID_ID
              && extract_package_id(type_id) != OB_INVALID_ID) { // 最后尝试下是不是PackageType
      ret = OB_SUCCESS;
      const ObUserDefinedType *package_user_type = NULL;
      if (!common::is_dblink_type_id(type_id)) {
        ObPLPackageManager &package_manager = session_info_.get_pl_engine()->get_package_manager();
        ObPLDataType *copy_pl_type = NULL;
        OZ (package_manager.get_package_type(*this, extract_package_id(type_id), type_id, package_user_type), K(type_id));
        CK (OB_NOT_NULL(user_type = static_cast<const ObUserDefinedType *>(package_user_type)));
      } else {
        OZ (package_guard_.dblink_guard_.get_dblink_type_by_id(extract_package_id(type_id), type_id,
            package_user_type), type_id);
        CK (OB_NOT_NULL(user_type = static_cast<const ObUserDefinedType *>(package_user_type)));
      }
    }
  }

  CANCLE_LOG_CHECK_MODE();

  return ret;
}

int ObPLResolver::check_undeclared_var_type(ObQualifiedName &q_name)
{
  // ret must be OB_ERR_SP_UNDECLARED_VAR before entering this function 
  int ret = OB_SUCCESS;
  const char *empty = "";
  const char *dot = ".";
#define LOG_USER_ERROR_WRONG_SCHEMA_REF_1(name1) \
        ret = OB_ERR_WRONG_SCHEMA_REF; \
        LOG_USER_ERROR(OB_ERR_WRONG_SCHEMA_REF, name1.ptr(), empty, empty, empty, empty)
#define LOG_USER_ERROR_WRONG_SCHEMA_REF_2(name1, name2) \
        ret = OB_ERR_WRONG_SCHEMA_REF; \
        LOG_USER_ERROR(OB_ERR_WRONG_SCHEMA_REF, name1.ptr(), dot, name2.ptr(), empty, empty)
#define LOG_USER_ERROR_WRONG_SCHEMA_REF_3(name1, name2, name3) \
        ret = OB_ERR_WRONG_SCHEMA_REF; \
        LOG_USER_ERROR(OB_ERR_WRONG_SCHEMA_REF, name1.ptr(), dot, name2.ptr(), dot, name3.ptr())
  uint64_t tenant_id = resolve_ctx_.session_info_.get_effective_tenant_id();
  uint64_t db_id = resolve_ctx_.session_info_.get_database_id();
  ObSchemaGetterGuard &schema_guard = resolve_ctx_.schema_guard_;
  const ObSimpleTableSchemaV2 *table_schema = NULL;
  ObSchemaType schema_type = OB_MAX_SCHEMA;
  switch (q_name.access_idents_.count()) {
    case 1: {
      ObString name = q_name.access_idents_.at(0).access_name_;
      if (OB_FAIL(check_var_type(name, db_id, schema_type, table_schema, true))) {
        LOG_WARN("failed to check_var_type", K(q_name.access_idents_.at(0).access_name_),
                 K(db_id), K(ret));
      } else if (TABLE_SCHEMA == schema_type || SEQUENCE_SCHEMA == schema_type) {
        LOG_USER_ERROR_WRONG_SCHEMA_REF_1(q_name.access_idents_.at(0).access_name_);
      }
    }
      break;
    case 2: {
      if (OB_FAIL(check_var_type(q_name.access_idents_.at(0).access_name_,
                                 q_name.access_idents_.at(1).access_name_, db_id))) {
        LOG_WARN("failed to check_var_type", K(q_name.access_idents_.at(0).access_name_),
                 K(q_name.access_idents_.at(1).access_name_), K(db_id), K(ret));
        if (OB_ERR_WRONG_SCHEMA_REF == ret) {
          LOG_USER_ERROR_WRONG_SCHEMA_REF_2(q_name.access_idents_.at(0).access_name_,
                                            q_name.access_idents_.at(1).access_name_);
        }
      } else {
        db_id = OB_INVALID_ID;
        if (OB_FAIL(schema_guard.get_database_id(tenant_id, 
                                                q_name.access_idents_.at(0).access_name_, db_id))) {
          LOG_WARN("failed to get_database_id", K(tenant_id),
                   K(q_name.access_idents_.at(0).access_name_), K(ret));
        } else if (OB_INVALID_ID == db_id) {
          // return when not find a db_id
        } else if (OB_FAIL(check_var_type(q_name.access_idents_.at(1).access_name_, db_id,
                                          schema_type, table_schema, true))) {
          LOG_WARN("failed to check_var_type", K(q_name.access_idents_.at(1).access_name_),
                   K(db_id), K(ret));
        } else if (TABLE_SCHEMA == schema_type || SEQUENCE_SCHEMA == schema_type) {
          LOG_USER_ERROR_WRONG_SCHEMA_REF_2(q_name.access_idents_.at(0).access_name_,
                                            q_name.access_idents_.at(1).access_name_);
        }
      }
    }
      break;
    case 3: {
      db_id = OB_INVALID_ID;
      if (OB_FAIL(schema_guard.get_database_id(tenant_id, q_name.access_idents_.at(0).access_name_, 
                                               db_id))) {
        LOG_WARN("failed to get_database_id", K(tenant_id), 
                 K(q_name.access_idents_.at(0).access_name_), K(ret));
      } else if (OB_INVALID_ID == db_id) {
        // return when not find a db_id
      } else if (OB_FAIL(check_var_type(q_name.access_idents_.at(1).access_name_,
                                        q_name.access_idents_.at(2).access_name_,
                                        db_id))) {
        LOG_WARN("failed to check_var_type", K(q_name.access_idents_.at(1).access_name_),
                 K(q_name.access_idents_.at(2).access_name_), K(db_id), K(ret));
        if (OB_ERR_WRONG_SCHEMA_REF == ret) {
          LOG_USER_ERROR_WRONG_SCHEMA_REF_3(q_name.access_idents_.at(0).access_name_,
                                            q_name.access_idents_.at(1).access_name_,
                                            q_name.access_idents_.at(2).access_name_);
        }
      }
    }
      break;
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the impossible", K(q_name), K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    ret = OB_ERR_SP_UNDECLARED_VAR;
  }
  return ret;
}

int ObPLResolver::check_var_type(ObString &name, uint64_t db_id, ObSchemaType &schema_type,
                                 const ObSimpleTableSchemaV2 *&schema, bool find_synonym)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = resolve_ctx_.session_info_.get_effective_tenant_id();
  ObSchemaGetterGuard &schema_guard = resolve_ctx_.schema_guard_;
  bool is_exist = false;
  uint64_t seq_id = OB_INVALID_ID;
  bool is_sys_generated = false;
  if (OB_FAIL(schema_guard.get_simple_table_schema(tenant_id, db_id, name, false, schema))) {
    LOG_WARN("failed to get table schema", K(tenant_id), K(db_id), K(name), K(ret));
  } else if (OB_NOT_NULL(schema)) {
    // return when find a table schema
    schema_type = TABLE_SCHEMA;
  } else if (OB_FAIL(schema_guard.check_sequence_exist_with_name(tenant_id, db_id, name,
                                                                 is_exist, seq_id,
                                                                 is_sys_generated))) {
    LOG_WARN("fail to check sequence exist", K(tenant_id), K(db_id), K(name), K(ret));
  } else if (is_exist) {
    // return when find a seq id
    schema_type = SEQUENCE_SCHEMA;
  } else if (find_synonym) {
    ObSchemaChecker schema_checker;
    ObSynonymChecker synonym_checker;
    uint64_t object_db_id = OB_INVALID_ID;
    ObString object_name;
    if (OB_FAIL(schema_checker.init(schema_guard, resolve_ctx_.session_info_.get_sessid()))) {
      LOG_WARN("failed to init schema_checker", K(ret));
    } else if (OB_FAIL(ObResolverUtils::resolve_synonym_object_recursively(
                           schema_checker, synonym_checker, tenant_id,
                           db_id, name, object_db_id, object_name, is_exist))) {
      LOG_WARN("failed to find synonym", K(tenant_id), K(db_id), K(name), K(ret));
    } else if (is_exist) {
      if (OB_FAIL(SMART_CALL(check_var_type(object_name, object_db_id,
                                            schema_type, schema, false)))) {
        LOG_WARN("failed check_var_type", K(object_name), K(object_db_id), K(ret));
      }
    }
  }
  return ret;
}

int ObPLResolver::check_var_type(ObString &name1, ObString &name2, uint64_t db_id) {
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard &schema_guard = resolve_ctx_.schema_guard_;
  const ObSimpleTableSchemaV2 *table_schema = NULL;
  ObSchemaType schema_type = OB_MAX_SCHEMA;
  const ObTableSchema *t_schema = NULL;
  OZ (check_var_type(name1, db_id, schema_type, table_schema, true), name1, db_id);
  if (TABLE_SCHEMA == schema_type) {
    CK (OB_NOT_NULL(table_schema));
    OZ (schema_guard.get_table_schema(table_schema->get_tenant_id(),
                                      table_schema->get_table_id(),
                                      t_schema));
    if (OB_SUCC(ret)) {
      bool has = false;
      CK (OB_NOT_NULL(t_schema));
      OZ (t_schema->has_column(name2, has));
      if (OB_SUCC(ret)) {
        if (has) {
          ret = OB_ERR_WRONG_SCHEMA_REF;
          LOG_WARN("Table,View Or Sequence reference not allowed in this context", K(ret));
        } else {
          ret = OB_ERR_COMPONENT_UNDECLARED;
          LOG_WARN("component must be declared", K(ret));
          LOG_USER_ERROR(OB_ERR_COMPONENT_UNDECLARED, name2.length(), name2.ptr());
        }
      }
    }
  } else if (SEQUENCE_SCHEMA == schema_type) {
    ret = OB_ERR_COMPONENT_UNDECLARED;
    LOG_WARN("component must be declared", K(ret));
    LOG_USER_ERROR(OB_ERR_COMPONENT_UNDECLARED, name2.length(), name2.ptr());
  }
  return ret;
}

int ObPLResolver::check_update_column(const ObPLBlockNS &ns, const ObIArray<ObObjAccessIdx>& access_idxs)
{
  int ret = OB_SUCCESS;
  if (resolve_ctx_.session_info_.is_for_trigger_package()
      && 2 == access_idxs.count()
      && 0 != access_idxs.at(1).var_name_.case_compare(OB_HIDDEN_LOGICAL_ROWID_COLUMN_NAME)
      && access_idxs.at(0).var_name_.prefix_match(":")
      && lib::is_oracle_mode()) {
    ObSchemaChecker schema_checker;
    const ObTriggerInfo *trg_info = NULL;
    const uint64_t tenant_id = resolve_ctx_.session_info_.get_effective_tenant_id();
    OZ (schema_checker.init(resolve_ctx_.schema_guard_, resolve_ctx_.session_info_.get_sessid()));
    OZ (schema_checker.get_trigger_info(tenant_id,
                                        ns.get_db_name(),
                                        ns.get_package_name(),
                                        trg_info));
    CK (OB_NOT_NULL(trg_info));
    if (OB_SUCC(ret)) {
      ObString var_name = access_idxs.at(0).var_name_;
      ++var_name;
      if (0 == trg_info->get_ref_new_name().case_compare(var_name)
          || 0 == trg_info->get_ref_old_name().case_compare(var_name)) {
        const uint64_t table_id = trg_info->get_base_object_id();
        const ObString column_name = access_idxs.at(1).var_name_;
        const ObTableSchema *table_schema = NULL;
        const ObColumnSchemaV2 *col_schema = NULL;
        OZ (resolve_ctx_.schema_guard_.get_table_schema(tenant_id, table_id, table_schema));
        CK (OB_NOT_NULL(table_schema));
        OX (col_schema = table_schema->get_column_schema(column_name));
        CK (OB_NOT_NULL(col_schema));
        if (OB_SUCC(ret)) {
          if (col_schema->is_generated_column()) {
            ret = OB_ERR_CANNOT_UPDATE_VIRTUAL_COL_IN_TRG;
            LOG_WARN("virtual column cannot be updated in trigger body", K(col_schema->get_column_name_str()), K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObPLResolver::resolve_do(const ObStmtNodeTree *parse_tree, ObPLDoStmt *stmt,
                             ObPLFunctionAST &func)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(parse_tree) || OB_ISNULL(stmt) || OB_ISNULL(current_block_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parse_tree is NULL", K(parse_tree), K(stmt), K(current_block_), K(ret));
  } else if (OB_ISNULL(parse_tree->children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid do stmt", K(parse_tree->children_[0]), K(ret));
  } else if (T_EXPR_LIST != parse_tree->children_[0]->type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid do stmt", K(parse_tree->children_[0]->type_), K(ret));
  } else {
    const ObStmtNodeTree *current_node = parse_tree->children_[0];
    for (int64_t i = 0; OB_SUCC(ret) && i < current_node->num_child_; ++i) {
      ObRawExpr *value_expr = NULL;
      const ObStmtNodeTree *value_node = current_node->children_[i];
      if (OB_ISNULL(value_node)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr node is NULL", K(value_node), K(ret));
      } else if (OB_FAIL(resolve_expr(value_node, func, value_expr,
                        combine_line_and_col(value_node->stmt_loc_),
                        true, NULL))) {
          LOG_WARN("failed to resolve expr ", K(value_node), K(ret));
      } else if (OB_FAIL(stmt->add_value(func.get_expr_count() - 1))) {
        LOG_WARN("failed to add_value ", K(stmt), K(ret));
      }
    }
  }
  
  return ret;
}

int ObPLResolver::get_udt_names(
  ObSchemaGetterGuard &schema_guard, const uint64_t udt_id, ObString &database_name, ObString &udt_name)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID != udt_id) {
    uint64_t db_id = OB_INVALID_ID;
    const ObUDTTypeInfo *udt_info = NULL;
    const ObSimpleDatabaseSchema *db_schema = NULL;
    const uint64_t tenant_id = get_tenant_id_by_object_id(udt_id);
    OZ (schema_guard.get_udt_info(tenant_id, udt_id, udt_info));
    CK (OB_NOT_NULL(udt_info));
    OX (db_id = udt_info->get_database_id());
    CK (OB_INVALID_ID != db_id);
    OZ (schema_guard.get_database_schema(tenant_id, db_id, db_schema));
    CK (OB_NOT_NULL(db_schema));
    if (OB_SUCC(ret) && resolve_ctx_.session_info_.get_database_name().case_compare(db_schema->get_database_name_str()) != 0) {
      OX (database_name = db_schema->get_database_name_str());
    }
    OX (udt_name = udt_info->get_type_name());
  }
  return ret;
}

int ObPLResolver::get_udt_database_name(ObSchemaGetterGuard &schema_guard, const uint64_t udt_id, ObString &db_name)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID != udt_id) {
    uint64_t db_id = OB_INVALID_ID;
    const ObUDTTypeInfo *udt_info = NULL;
    const ObSimpleDatabaseSchema *db_schema = NULL;
    const uint64_t tenant_id = get_tenant_id_by_object_id(udt_id);
    OZ (schema_guard.get_udt_info(tenant_id, udt_id, udt_info));
    CK (OB_NOT_NULL(udt_info));
    OX (db_id = udt_info->get_database_id());
    CK (OB_INVALID_ID != db_id);
    OZ (schema_guard.get_database_schema(tenant_id, db_id, db_schema));
    CK (OB_NOT_NULL(db_schema));
    OX (db_name = db_schema->get_database_name_str());
  }
  return ret;
}

bool ObPLResolver::check_with_rowid(const ObString &routine_name, bool is_for_trigger)
{
  bool with_rowid = false;
  if (is_for_trigger && lib::is_oracle_mode()) {
    if (0 == routine_name.case_compare("calc_when") ||
        0 == routine_name.case_compare("before_row") ||
        0 == routine_name.case_compare("after_row")) {
      with_rowid = true;
    }
  }
  return with_rowid;
}

int ObPLResolver::recursive_replace_expr(ObRawExpr *expr,
                                         ObQualifiedName &qualified_name,
                                         ObRawExpr *real_expr)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(expr));
  CK (OB_NOT_NULL(real_expr));
  for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
    CK (OB_NOT_NULL(expr->get_param_expr(i)));
    if (OB_FAIL(ret)) {
    } else if (expr->get_param_expr(i) == qualified_name.ref_expr_) {
      expr->get_param_expr(i) = real_expr;
    } else if (expr->get_param_expr(i)->get_param_count() > 0) {
      OZ (recursive_replace_expr(expr->get_param_expr(i), qualified_name, real_expr));
    }
  }
  return ret;
}

}
}

