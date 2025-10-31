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

#include "pl/ob_pl_router.h"
#include "pl/ob_pl_package.h"
#include "sql/resolver/ddl/ob_ddl_resolver.h"
#include "pl/ob_pl_dependency_util.h"

namespace oceanbase {
using namespace common;
using namespace share;
using namespace schema;
using namespace sql;

namespace pl {

int ObPLRouter::check_error_in_resolve(int code)
{
  int ret = OB_SUCCESS;
  switch (code) {
    case OB_ER_STMT_NOT_ALLOWED_IN_SF_OR_TRG:
    case OB_ER_COMMIT_NOT_ALLOWED_IN_SF_OR_TRG:
    case OB_ER_SP_NO_RETSET:
    case OB_ERR_SP_COND_MISMATCH:
    case OB_ERR_SP_LILABEL_MISMATCH:
    case OB_ERR_SP_CURSOR_MISMATCH:
    case OB_ERR_SP_DUP_VAR:
    case OB_ERR_SP_DUP_TYPE:
    case OB_ERR_SP_DUP_CONDITION:
    case OB_ERR_SP_DUP_LABEL:
    case OB_ERR_SP_DUP_CURSOR:
    case OB_ERR_SP_DUP_HANDLER:
    case OB_ER_SP_NO_RECURSIVE_CREATE:
    case OB_ER_SP_BADRETURN:
    case OB_ER_SP_BAD_CURSOR_SELECT:
    case OB_ER_SP_VARCOND_AFTER_CURSHNDLR:
    case OB_ER_SP_CURSOR_AFTER_HANDLER:
    case OB_ER_SP_BAD_SQLSTATE:
    case OB_ERR_UNKNOWN_TABLE:
    case OB_ER_SP_CANT_SET_AUTOCOMMIT:
    case OB_ERR_GOTO_BRANCH_ILLEGAL:
    case OB_ERR_UNEXPECTED:
    case OB_ERR_RETURN_VALUE_REQUIRED:
    case OB_ERR_END_LABEL_NOT_MATCH:
    case OB_ERR_TOO_LONG_IDENT:
    case OB_ERR_PL_JSONTYPE_USAGE: {
      ret = code;
    }
    break;
    case OB_ERR_SP_UNDECLARED_VAR: {
      //某些错误码oracle模式需要覆盖，mysql模式不能覆盖
      if (lib::is_oracle_mode()) {
        LOG_WARN("resolver error", K(ret), K(code));
        ObPL::insert_error_msg(code);
      } else {
        ret = code;
      }
    }
    break;
    case OB_ERR_NO_RETURN_IN_FUNCTION:
    case OB_ERR_STMT_NOT_ALLOW_IN_MYSQL_FUNC_TRIGGER:
    case OB_ERR_TOO_LONG_STRING_TYPE:
    case OB_ERR_WIDTH_OUT_OF_RANGE:
    case OB_ERR_REDEFINE_LABEL:
    case OB_ERR_STMT_NOT_ALLOW_IN_MYSQL_PROCEDRUE:
    case OB_ERR_TOO_BIG_PRECISION:
    case OB_ERR_TOO_BIG_SCALE:
    case OB_ERR_TOO_BIG_DISPLAYWIDTH:
    case OB_ERR_TOO_LONG_COLUMN_LENGTH:
    case OB_ERR_TRIGGER_CANT_CHANGE_ROW:
    case OB_ERR_SET_USAGE:
    case OB_ERR_COLUMN_SPEC:
    case OB_ERR_TRIGGER_NO_SUCH_ROW:
    case OB_ERR_SP_NO_DROP_SP:
    case OB_ERR_SP_BAD_CONDITION_TYPE:
    case OB_ERR_DUP_SIGNAL_SET:
    case OB_ERR_WRONG_PARAMETERS_TO_NATIVE_FCT:
    case OB_ERR_CANNOT_UPDATE_VIRTUAL_COL_IN_TRG:
    case OB_ERR_VIEW_SELECT_CONTAIN_QUESTIONMARK:
    case OB_ERR_VIEW_SELECT_CONTAIN_INTO: {
      if (lib::is_mysql_mode()) {
        ret = code;
        break;
      }
      // oracle mode go to default
    }
    default: {
      LOG_WARN("resolver error", K(ret), K(code));
      // do not recover the ret intentionaly.
      ObPL::insert_error_msg(code);
    }
    break;
  }
  return ret;
}

int ObPLRouter::analyze(ObString &route_sql, ObIArray<ObDependencyInfo> &dep_info, ObRoutineInfo &routine_info, obrpc::ObDDLArg *ddl_arg)
{
  int ret = OB_SUCCESS;
  HEAP_VAR(ObPLFunctionAST, func_ast, inner_allocator_) {
    if (routine_info_.is_pipelined()) {
      func_ast.set_pipelined();
    }
    if (OB_FAIL(simple_resolve(func_ast))) {
      // 兼容mysql，部分resolve阶段错误需要在创建时抛出
      if (OB_FAIL(check_error_in_resolve(ret))) {
        LOG_WARN("resolve error with error code", K(ret));
      } else {
        LOG_USER_WARN(OB_ERR_RESOLVE_SQL);
      }
    } else if (OB_FAIL(analyze_stmt(func_ast.get_body(), route_sql))) {
      LOG_WARN("failed to analyze stmt", K(ret));
    } else if (OB_FAIL(mark_sql_transpiler_eligible(func_ast, routine_info))) {
      LOG_WARN("failed to mark sql transpiler eligible", K(ret));
    } else {
      ObString dep_attr;
      OZ (ObDependencyInfo::collect_dep_infos(func_ast.get_dependency_table(),
                                            dep_info,
                                            routine_info_.get_object_type(),
                                            0, dep_attr, dep_attr));
      OZ (ObDDLResolver::ob_add_ddl_dependency(func_ast.get_dependency_table(),
                                               *ddl_arg));
    }
    if (OB_SUCC(ret)) {
      if (func_ast.is_modifies_sql_data()) {
        routine_info.set_modifies_sql_data();
      } else if (func_ast.is_reads_sql_data()) {
        routine_info.set_reads_sql_data();
      } else if (func_ast.is_contains_sql()) {
        routine_info.set_contains_sql();
      } else if (func_ast.is_no_sql()) {
        routine_info.set_no_sql();
      }
      if (func_ast.is_wps()) {
        routine_info.set_wps();
      }
      if (func_ast.is_rps()) {
        routine_info.set_rps();
      }
      if (func_ast.is_has_sequence()) {
        routine_info.set_has_sequence();
      }
      if (func_ast.is_has_out_param()) {
        routine_info.set_has_out_param();
      }
      if (func_ast.is_external_state()) {
        routine_info.set_external_state();
      }
    }
  }
  return ret;
}

int ObPLRouter::simple_resolve(ObPLFunctionAST &func_ast)
{
  int ret = OB_SUCCESS;
  func_ast.set_name(routine_info_.get_routine_name());
  if (ROUTINE_PROCEDURE_TYPE == routine_info_.get_routine_type()) {
    func_ast.set_proc_type(STANDALONE_PROCEDURE);
  } else if (ROUTINE_FUNCTION_TYPE == routine_info_.get_routine_type()) {
    func_ast.set_proc_type(STANDALONE_FUNCTION);
  }
  if (routine_info_.is_udt_routine()) {
      func_ast.set_is_udt_routine();
  }
  //添加参数列表
  for (int64_t i = 0; OB_SUCC(ret) && i < routine_info_.get_routine_params().count(); ++i) {
    ObRoutineParam *param = routine_info_.get_routine_params().at(i);
    ObPLDataType param_type;
    ObSEArray<ObSchemaObjVersion, 4> deps;
    CK (OB_NOT_NULL(param));
    OX (param_type.set_enum_set_ctx(&func_ast.get_enum_set_ctx()));
    OZ (pl::ObPLDataType::transform_from_iparam(param,
                                                schema_guard_,
                                                session_info_,
                                                sql_proxy_,
                                                param_type,
                                                &deps));
    if (OB_SUCC(ret)) {
      if (param->is_ret_param()) {
        func_ast.set_ret_type(param_type);
        if (OB_FAIL(func_ast.set_ret_type_info(param->get_extended_type_info(), &func_ast.get_enum_set_ctx()))) {
          LOG_WARN("fail to set type info", K(ret));
        }
      } else if (OB_FAIL(func_ast.add_argument(param->get_param_name(),
                                               param_type,
                                               NULL,
                                               &param->get_extended_type_info(),
                                               param->is_in_sp_param(),
                                               param->is_self_param()))) { //输入参数的default值在编译时候没用
        LOG_WARN("failed to add argument", K(param->get_param_name()), K(param->get_param_type()), K(ret));
      } else {
        // do nothing
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(ObPLDependencyUtil::add_dependency_objects(&func_ast.get_dependency_table(), deps))) {
          LOG_WARN("fail to add dependency objects", K(ret));
        }
      }
    }
  }
  //Parser
  ObStmtNodeTree *parse_tree = NULL;
  bool is_wrap = false;
  if (OB_SUCC(ret)) {
    ObString body = routine_info_.get_routine_body(); //获取body字符串
    ObPLParser parser(inner_allocator_, session_info_.get_charsets4parser(), session_info_.get_sql_mode());
    CHECK_COMPATIBILITY_MODE(&session_info_);

    if (OB_FAIL(ObSQLUtils::convert_sql_text_from_schema_for_resolve(
                  inner_allocator_, session_info_.get_dtc_params(), body))) {
      LOG_WARN("fail to get routine body", K(ret));
    } else if (OB_FAIL(parser.parse_routine_body(body, parse_tree, session_info_.is_for_trigger_package(), is_wrap))) {
      LOG_WARN("parse routine body failed", K(ret), K(body));
    }
  }

  //Resolver
  if (OB_SUCC(ret)) {
    const bool is_prepare_protocol = false;
    ObPLPackageGuard package_guard(session_info_.get_effective_tenant_id());
    ObPLResolver resolver(inner_allocator_,
                          session_info_,
                          schema_guard_,
                          package_guard,
                          sql_proxy_,
                          expr_factory_,
                          NULL,
                          is_prepare_protocol,
                          false,
                          false,
                          NULL,
                          NULL,
                          routine_info_.get_tg_timing_event());
    if (OB_ISNULL(parse_tree)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pl body is NULL", K(parse_tree), K(ret));
    } else if (OB_FAIL(resolver.init(func_ast))) {
      LOG_WARN("failed to init resolver", K(routine_info_), K(ret));
    } else if (OB_FAIL(resolver.resolve(parse_tree, func_ast))) {
      LOG_WARN("failed to analyze pl body", K(routine_info_), K(ret));
    } else if (lib::is_mysql_mode() && func_ast.is_function() && !func_ast.has_return()) {
      ret = OB_ERR_NO_RETURN_IN_FUNCTION;
      LOG_WARN("mysql func need return. ", K(ret));
      LOG_USER_ERROR(OB_ERR_NO_RETURN_IN_FUNCTION, func_ast.get_name().length(), 
                                                   func_ast.get_name().ptr());
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObPLRouter::analyze_stmt(const ObPLStmt *stmt, ObString &route_sql)
{
  int ret = OB_SUCCESS;
  if (NULL != stmt && route_sql.empty()) {
    switch (stmt->get_type()) {
    case PL_SQL: {
      const ObPLSqlStmt *sql_stmt = static_cast<const ObPLSqlStmt*>(stmt);
      if (OB_FAIL(check_route_sql(sql_stmt, route_sql))) {
        LOG_WARN("failed to check route sql", K(ret));
      }
    }
    break;
    case PL_OPEN:
    case PL_OPEN_FOR: {
      const ObPLOpenStmt *open_stmt = static_cast<const ObPLOpenStmt*>(stmt);
      const ObPLCursor *cursor = open_stmt->get_cursor();
      if (OB_ISNULL(cursor)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("cursor is NULL", K(cursor), K(ret));
      } else {
        if (OB_FAIL(check_route_sql(&cursor->get_value(), route_sql))) {
          LOG_WARN("failed to check route sql", K(ret));
        }
      }
    }
    break;
    case PL_HANDLER: {
      // skip
    }
    break;
    case PL_CALL: {
      const ObPLCallStmt *inner_call_stmt = static_cast<const ObPLCallStmt*>(stmt);
      route_sql = inner_call_stmt->get_route_sql();
    }
    break;
    default: {
      for (int64_t i = 0; OB_SUCC(ret) && route_sql.empty() && i < stmt->get_child_size(); ++i) {
        if (OB_FAIL(analyze_stmt(stmt->get_child_stmt(i), route_sql))) {
          LOG_WARN("failed to analyze stmt", K(i), K(ret));
        }
      }
    }
    break;
    }
  }
  return ret;
}

int ObPLRouter::check_route_sql(const ObPLSql *pl_sql, ObString &route_sql)
{
  int ret = OB_SUCCESS;
  bool has_table = false;
  for (int64_t i = 0; OB_SUCC(ret) && !has_table && i < pl_sql->get_ref_objects().count(); ++i) {
    if (pl_sql->get_ref_objects().at(i).is_base_table()) {
      has_table = true;
    }
  }

  if (OB_SUCC(ret) && has_table) {
    route_sql = pl_sql->get_sql();
  }
  return ret;
}

int ObPLRouter::mark_sql_transpiler_eligible(const ObPLFunctionAST &func_ast, ObRoutineInfo &routine_info)
{
  int ret = OB_SUCCESS;

  bool eligible = false;
  const ObPLReturnStmt *return_stmt = nullptr;

  // check structure is eligible
  if (OB_FAIL(try_extract_sql_transpiler_expr(func_ast, eligible, return_stmt))) {
    LOG_WARN("failed to check structure sql transpiler eligible", K(ret));
  }

  // check param types are eligible
  if (OB_SUCC(ret) && eligible) {
    for (int64_t i = 0; OB_SUCC(ret) && eligible && i < func_ast.get_symbol_table().get_count(); ++i) {
      const ObPLVar *var = func_ast.get_symbol_table().get_symbol(i);
      const ObRoutineParam *param = nullptr;

      if (OB_ISNULL(var)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected NULL param", K(ret), K(var), K(func_ast), K(i));
      } else if (i >= routine_info.get_param_count()) {
        eligible = false;
      } else if (OB_ISNULL(param = routine_info.get_routine_params().at(i + routine_info.get_param_start_idx()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected NULL param", K(ret), K(var), K(func_ast), K(i), K(routine_info));
      } else if (!param->is_in_param()) {
        eligible = false;
      } else if (OB_FAIL(check_type_sql_transpiler_eligible(var->get_type(), eligible))) {
        LOG_WARN("failed to check type sql transpiler eligible", K(ret), K(var), K(i));;
      }
    }
  }

  // check ret type is eligible
  if (OB_SUCC(ret) && eligible) {
    if (OB_FAIL(check_type_sql_transpiler_eligible(func_ast.get_ret_type(), eligible))) {
      LOG_WARN("failed to check type sql transpiler eligible", K(ret), K(func_ast.get_ret_type()));
    }
  }

  // check exprs are eligible
  if (OB_SUCC(ret) && eligible) {
    const ObRawExpr *ret_expr = nullptr;

    CK (OB_NOT_NULL(return_stmt));
    CK (OB_NOT_NULL(ret_expr = return_stmt->get_ret_expr()));

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(check_expr_sql_transpiler_eligible(*ret_expr, eligible))) {
      LOG_WARN("failed to check expr sql transpiler eligible", K(ret), K(*ret_expr));
    }
  }

  if (OB_SUCC(ret) && eligible) {
    routine_info.set_sql_transpiler_eligible();
  }

  return ret;
}

int ObPLRouter::try_extract_sql_transpiler_expr(const ObPLFunctionAST &func_ast, bool &success, const ObPLReturnStmt *&return_stmt)
{
  int ret = OB_SUCCESS;

  success = false;
  return_stmt = nullptr;

  const ObPLStmtBlock *body = func_ast.get_body();

  CK (OB_NOT_NULL(body));
  CK (PL_BLOCK == body->get_type());

  // check structure contains only one return statement
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (0 < func_ast.get_routine_table().get_count()) {
    // do nothing
  } else if (1 == body->get_child_size()) {
    const ObPLStmt *child_stmt = body->get_child_stmt(0);

    CK (OB_NOT_NULL(child_stmt));

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (PL_BLOCK == child_stmt->get_type()) {
      const ObPLStmtBlock *child_block = static_cast<const ObPLStmtBlock *>(child_stmt);
      const ObPLStmt *grandchild = nullptr;

      if (1 == child_block->get_child_size()
            && OB_NOT_NULL(grandchild = child_block->get_child_stmt(0))
            && PL_RETURN == grandchild->get_type()) {
        return_stmt = static_cast<const ObPLReturnStmt*>(grandchild);
        success = true;
      }
    } else if (PL_RETURN == child_stmt->get_type()) {
      return_stmt = static_cast<const ObPLReturnStmt*>(child_stmt);
      success = true;
    }
  }

  return ret;
}

int ObPLRouter::check_type_sql_transpiler_eligible(const ObPLDataType &type, bool &eligible)
{
  int ret = OB_SUCCESS;

  ObObjType obj_type = type.get_obj_type();

  if (PL_OBJ_TYPE != type.get_type()) {
    eligible = false;
  } else if (ob_is_user_defined_type(obj_type)) {
    eligible = false;
  } else if (ob_is_json(obj_type) || ob_is_geometry(obj_type) || ob_is_roaringbitmap(obj_type)) {
    eligible = false;
  } else if (PL_TYPE_LOCAL != type.get_type_from()
               || PL_TYPE_LOCAL != type.get_type_from_origin()) {
    eligible = false;
  } else if (ob_is_enum_or_set_type(obj_type) || ob_is_enum_or_set_inner_type(obj_type)) {
    eligible = false;
  }

  return ret;
}

int ObPLRouter::check_expr_sql_transpiler_eligible(const ObRawExpr &expr, bool &eligible)
{
  int ret = OB_SUCCESS;

  if (expr.has_flag(IS_PL_UDF) || expr.has_flag(CNT_PL_UDF)) {
    eligible = false;
  } else if (ObRawExpr::EXPR_PL_QUERY_REF == expr.get_expr_class()) {
    eligible = false;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && eligible && i < expr.get_param_count(); ++i) {
      if (OB_ISNULL(expr.get_param_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected NULL param expr", K(ret), K(expr), K(i));
      } else if (OB_FAIL(SMART_CALL(check_expr_sql_transpiler_eligible(*expr.get_param_expr(i), eligible)))) {
        LOG_WARN("failed to check expr sql transpiler eligible", K(ret), K(expr.get_param_expr(i)));
      }
    }
  }

  return ret;
}

}  // namespace pl
}  // namespace oceanbase
