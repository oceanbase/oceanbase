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
#include "share/schema/ob_routine_info.h"
#include "parser/ob_pl_parser.h"
#include "pl/ob_pl_resolver.h"
#include "pl/ob_pl_package.h"
#include "share/ob_errno.h"
#include "sql/ob_sql_utils.h"

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
    case OB_ERR_VIEW_SELECT_CONTAIN_QUESTIONMARK: {
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

int ObPLRouter::analyze(ObString &route_sql, ObIArray<ObDependencyInfo> &dep_info, ObRoutineInfo &routine_info)
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
    } else {
      ObString dep_attr;
      OZ (ObDependencyInfo::collect_dep_infos(func_ast.get_dependency_table(),
                                            dep_info,
                                            routine_info_.get_object_type(),
                                            0, dep_attr, dep_attr));
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
    CK (OB_NOT_NULL(param));
    OZ (pl::ObPLDataType::transform_from_iparam(param,
                                                schema_guard_,
                                                session_info_,
                                                inner_allocator_,
                                                sql_proxy_,
                                                param_type));
    if (OB_SUCC(ret)) {
      if (param->is_ret_param()) {
        func_ast.set_ret_type(param_type);
        if (OB_FAIL(func_ast.set_ret_type_info(param->get_extended_type_info()))) {
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
    }
  }
  //Parser
  ObStmtNodeTree *parse_tree = NULL;
  if (OB_SUCC(ret)) {
    ObString body = routine_info_.get_routine_body(); //获取body字符串
    ObPLParser parser(inner_allocator_, session_info_.get_charsets4parser(), session_info_.get_sql_mode());
    CHECK_COMPATIBILITY_MODE(&session_info_);

    if (OB_FAIL(ObSQLUtils::convert_sql_text_from_schema_for_resolve(
                  inner_allocator_, session_info_.get_dtc_params(), body))) {
      LOG_WARN("fail to get routine body", K(ret));
    } else if (OB_FAIL(parser.parse_routine_body(body, parse_tree, session_info_.is_for_trigger_package()))) {
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

}
}
