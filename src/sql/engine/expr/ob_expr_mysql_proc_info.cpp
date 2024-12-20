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

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/expr/ob_expr_mysql_proc_info.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/ob_spi.h"
#include "pl/ob_pl_stmt.h"
#include "share/schema/ob_schema_printer.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

ObExprMysqlProcInfo::ObExprMysqlProcInfo(ObIAllocator &alloc)
  : ObStringExprOperator(alloc, T_FUNC_SYS_MYSQL_PROC_INFO, N_MYSQL_PROC_INFO, 2, NOT_VALID_FOR_GENERATED_COL, INTERNAL_IN_MYSQL_MODE)
{
}

ObExprMysqlProcInfo::~ObExprMysqlProcInfo()
{
}

int ObExprMysqlProcInfo::calc_result_type2(ObExprResType &type,
                                           ObExprResType &type1,
                                           ObExprResType &type2,
                                           ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  UNUSED(type1);
  UNUSED(type2);

  int ret = OB_SUCCESS;
  type.set_blob();
  type.set_collation_type(CS_TYPE_BINARY);
  type.set_length(OB_MAX_LONGTEXT_LENGTH);
  return ret;
}

int ObExprMysqlProcInfo::set_return_result(const ObExpr &expr,
                                           ObEvalCtx &ctx,
                                           ObDatum &expr_datum,
                                           ObString &value_str)
{
  int ret = OB_SUCCESS;

  ObTextStringDatumResult str_result(expr.datum_meta_.type_, &expr, &ctx, &expr_datum);
  if (OB_FAIL(str_result.init(value_str.length()))) {
    LOG_WARN("init lob result failed", K(ret));
  } else if (OB_FAIL(str_result.append(value_str.ptr(), value_str.length()))) {
    LOG_WARN("append lob result failed");
  } else {
    str_result.set_result();
  }

  return ret;
}

int ObExprMysqlProcInfo::extract_create_node_from_routine_info(ObIAllocator &alloc, const ObRoutineInfo &routine_info, const sql::ObExecEnv &exec_env, ParseNode *&create_node) {
  int ret = OB_SUCCESS;

  ParseResult parse_result;
  ObString routine_stmt;
  ObSQLMode sql_mode = exec_env.get_sql_mode();
  sql_mode &= ~SMO_ORACLE;
  pl::ObPLParser parser(alloc, sql::ObCharsets4Parser(), sql_mode);
  const ObString &routine_body = routine_info.get_routine_body();
  const char prefix[] = "CREATE\n";
  int64_t prefix_len = STRLEN(prefix);
  int64_t buf_sz = prefix_len + routine_body.length();
  char *stmt_buf = static_cast<char *>(alloc.alloc(buf_sz));
  if (OB_ISNULL(stmt_buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SERVER_LOG(WARN, "failed to allocate memory for routine body buffer",
               K(buf_sz));
  } else {
    MEMCPY(stmt_buf, prefix, prefix_len);
    MEMCPY(stmt_buf + prefix_len, routine_body.ptr(), routine_body.length());
    routine_stmt.assign_ptr(stmt_buf, buf_sz);
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(parser.parse(routine_stmt, routine_stmt, parse_result, true))) {
    SERVER_LOG(WARN, "failed to parse mysql routine body",
               K(ret), K(routine_info), K(routine_body));
  }

  if OB_SUCC(ret) {
    if (OB_NOT_NULL(parse_result.result_tree_) &&
        T_STMT_LIST == parse_result.result_tree_->type_ &&
        1 == parse_result.result_tree_->num_child_) {
      create_node = parse_result.result_tree_->children_[0];
    } else {
      create_node = nullptr;
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "unexpected parse node of mysql routine body", K(routine_info), K(routine_body), K(parse_result.result_tree_));
    }
  }

  return ret;
}

int ObExprMysqlProcInfo::get_routine_info(ObSQLSessionInfo *session,
                                          uint64_t routine_id,
                                          const ObRoutineInfo *&routine_info)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = OB_INVALID_ID;
ObSchemaGetterGuard schema_guard;

  CK (OB_NOT_NULL(session));
  CK (OB_NOT_NULL(GCTX.schema_service_));
  OX (tenant_id = pl::get_tenant_id_by_object_id(routine_id));
  OZ (GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard));
  OZ (schema_guard.get_routine_info(tenant_id, routine_id, routine_info));

  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(OB_ISNULL(routine_info))) { //refresh schema try again
    OZ (ObSPIService::force_refresh_schema(tenant_id));
    OZ (GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard));
    OZ (schema_guard.get_routine_info(tenant_id, routine_id, routine_info));
  }
  CK (OB_NOT_NULL(routine_info));
  return ret;
}

int ObExprMysqlProcInfo::get_param_list_info(const ObExpr &expr,
                                             ObEvalCtx &ctx,
                                             ObDatum &expr_datum,
                                             uint64_t routine_id)
{
  int ret = OB_SUCCESS;
  const ObRoutineInfo *routine_info = NULL;
  ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  ObSchemaGetterGuard &schema_guard = *ctx.exec_ctx_.get_sql_ctx()->schema_guard_;

  sql::ObExecEnv exec_env;
  ObEvalCtx::TempAllocGuard alloc_guard(ctx);
  ObIAllocator &calc_alloc = alloc_guard.get_allocator();
  ParseNode *create_node = nullptr;

  OZ (get_routine_info(session, routine_id, routine_info));
  OZ (exec_env.init(routine_info->get_exec_env()));

  if (OB_FAIL(ret)) {
  } else if (routine_info->get_routine_body().prefix_match_ci("procedure")
              || routine_info->get_routine_body().prefix_match_ci("function")) {
    if (OB_FAIL(extract_create_node_from_routine_info(
                  calc_alloc, *routine_info, exec_env, create_node))) {
      SERVER_LOG(WARN, "failed to extract create node from routine info",
                  K(ret), K(*routine_info), K(exec_env), K(create_node));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (nullptr != create_node) {
    if (T_SP_CREATE != create_node->type_ && T_SF_CREATE != create_node->type_ && OB_ISNULL(create_node->children_[2])) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "unexpected parse node type of routine body", K(create_node->type_));
    } else {
      ParseNode *param_node = create_node->children_[2];
      ObString value_str;
      if (param_node != nullptr) {
        if (OB_FAIL(ob_write_string(
                        calc_alloc,
                        ObString(min(OB_MAX_VARCHAR_LENGTH, param_node->str_len_),
                        param_node->str_value_),
                        value_str))) {
          SERVER_LOG(WARN, "failed to ob_write_string",
                      K(ret),
                      K(param_node->str_len_),
                      K(param_node->str_value_),
                      K(value_str));
        }
      }
      if (OB_SUCC(ret)) {
        OZ (set_return_result(expr, ctx, expr_datum, value_str));
      }
    }
  } else {
    char *param_list_buf = NULL;
    if (NULL == (param_list_buf = static_cast<char *>(calc_alloc.alloc(OB_MAX_VARCHAR_LENGTH)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SERVER_LOG(WARN, "fail to alloc param_list_buf", K(ret));
    } else {
      ObSchemaPrinter schema_printer(schema_guard);
      int64_t pos = 0;
      if (OB_FAIL(schema_printer.print_routine_definition_param_v1(*routine_info,
                                                                   NULL,
                                                                   param_list_buf,
                                                                   OB_MAX_VARCHAR_LENGTH,
                                                                   pos,
                                                                   TZ_INFO(session)))) {
        LOG_WARN("failed to print routine definition param", K(ret));
      } else {
        ObString value_str(static_cast<int32_t>(pos), static_cast<int32_t>(pos), param_list_buf);
        OZ (set_return_result(expr, ctx, expr_datum, value_str));
      }
    }
  }

  return ret;
}

int ObExprMysqlProcInfo::get_returns_info(const ObExpr &expr,
                                          ObEvalCtx &ctx,
                                          ObDatum &expr_datum,
                                          uint64_t routine_id)
{
  int ret = OB_SUCCESS;
  char *returns_buf = NULL;
  int64_t pos = 0;
  const ObRoutineInfo *routine_info = NULL;
  ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();

  ObEvalCtx::TempAllocGuard alloc_guard(ctx);
  ObIAllocator &calc_alloc = alloc_guard.get_allocator();

  OZ (get_routine_info(session, routine_id, routine_info));

  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(NULL == (returns_buf = static_cast<char *>(calc_alloc.alloc(OB_MAX_VARCHAR_LENGTH))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SERVER_LOG(WARN, "fail to alloc returns_buf", K(ret));
  } else {
    if (routine_info->is_function()) {
      if (OB_FAIL(ob_sql_type_str_with_coll(returns_buf,
                                            OB_MAX_VARCHAR_LENGTH,
                                            pos,
                                            routine_info->get_ret_type()->get_obj_type(),
                                            routine_info->get_ret_type()->get_length(),
                                            routine_info->get_ret_type()->get_precision(),
                                            routine_info->get_ret_type()->get_scale(),
                                            routine_info->get_ret_type()->get_collation_type()))) {
        SHARE_SCHEMA_LOG(WARN, "fail to get data type str with coll", KPC(routine_info->get_ret_type()));
      }
    } else {
      // proc no returns, fill empty.
    }

    if (OB_SUCC(ret)) {
      ObString value_str(static_cast<int32_t>(pos), static_cast<int32_t>(pos), returns_buf);
      OZ (set_return_result(expr, ctx, expr_datum, value_str));
    }
  }

  return ret;
}

int ObExprMysqlProcInfo::get_body_info(const ObExpr &expr,
                                       ObEvalCtx &ctx,
                                       ObDatum &expr_datum,
                                       uint64_t routine_id)
{
  int ret = OB_SUCCESS;
  const ObRoutineInfo *routine_info = NULL;
  ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();

  sql::ObExecEnv exec_env;
  ParseNode *create_node = nullptr;
  ObEvalCtx::TempAllocGuard alloc_guard(ctx);
  ObIAllocator &calc_alloc = alloc_guard.get_allocator();

  OZ (get_routine_info(session, routine_id, routine_info));
  OZ (exec_env.init(routine_info->get_exec_env()));

  if (OB_FAIL(ret)) {
  } else if (routine_info->get_routine_body().prefix_match_ci("procedure")
              || routine_info->get_routine_body().prefix_match_ci("function")) {
    if (OB_FAIL(extract_create_node_from_routine_info(
                  calc_alloc, *routine_info, exec_env, create_node))) {
      SERVER_LOG(WARN, "failed to extract create node from routine info",
                  K(ret), K(*routine_info), K(exec_env), K(create_node));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (nullptr != create_node) {
    ParseNode *body_node = nullptr;
    if (T_SP_CREATE != create_node->type_ && T_SF_CREATE != create_node->type_) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "unexpected parse node type of routine body", K(create_node->type_));
    } else if (FALSE_IT(body_node = create_node->type_ == T_SP_CREATE ? create_node->children_[4] : create_node->children_[5])) {
      // do nothing
    } else if (OB_ISNULL(body_node) || OB_ISNULL(body_node->raw_text_)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "unexpected empty routine body", K(routine_info->get_routine_body()));
    } else {
      ObString value_str;
      if (OB_FAIL(ob_write_string(calc_alloc,
                                  ObString(min(OB_MAX_VARCHAR_LENGTH, body_node->text_len_), body_node->raw_text_),
                                  value_str))) {
        SERVER_LOG(WARN, "failed to ob_write_string", K(ret), K(ObString(body_node->text_len_, body_node->raw_text_)));
      } else {
        OZ (set_return_result(expr, ctx, expr_datum, value_str));
      }
    }
  } else {
    ObString &value_str = (ObString&)routine_info->get_routine_body();
    OZ(set_return_result(expr, ctx, expr_datum, value_str));
  }

  return ret;
}

int ObExprMysqlProcInfo::get_info_by_field_id(const ObExpr &expr,
                                                    ObEvalCtx &ctx,
                                                    ObDatum &expr_datum,
                                                    uint64_t routine_id,
                                                    uint64_t field_id)
{
  int ret = OB_SUCCESS;
  const ObRoutineInfo *routine_info = NULL;
  ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();

  sql::ObExecEnv exec_env;

  OZ (get_routine_info(session, routine_id, routine_info));
  OZ (exec_env.init(routine_info->get_exec_env()));

  if (OB_SUCC(ret)) {
    ObString value_str;
    switch (field_id) {
      case SQL_MODE: {
        ObObj int_value;
        ObObj str_value;
        int_value.set_int(exec_env.get_sql_mode());
        if (OB_FAIL(ob_sql_mode_to_str(int_value, str_value, &ctx.get_expr_res_alloc()))) {
          LOG_WARN("fail to convert sqlmode to string", K(int_value), K(ret));
        } else {
          value_str = str_value.get_string();
          OZ (set_return_result(expr, ctx, expr_datum, value_str));
        }
        break;
      }
      case CHARACTER_SET_CLIENT: {
        value_str = ObString(ObCharset::charset_name(exec_env.get_charset_client()));
        OZ (set_return_result(expr, ctx, expr_datum, value_str));
        break;
      }
      case COLLATION_CONNECTION: {
        value_str = ObString(ObCharset::collation_name(exec_env.get_collation_connection()));
        OZ (set_return_result(expr, ctx, expr_datum, value_str));
        break;
      }
      case DB_COLLATION: {
        value_str = ObString(ObCharset::collation_name(exec_env.get_collation_database()));
        OZ (set_return_result(expr, ctx, expr_datum, value_str));
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected field id", K(ret), K(field_id));
      }
    }
  }
  return ret;
}

int ObExprMysqlProcInfo::eval_mysql_proc_info(const ObExpr &expr,
                                                 ObEvalCtx &ctx,
                                                 ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum  *arg1 = NULL;
  ObDatum  *arg2 = NULL;
  uint64_t routine_id;
  ObString info_name;

  if (OB_FAIL(expr.args_[0]->eval(ctx, arg1))) {
    LOG_WARN("eval arg1 failed", K(ret));
  } else if (arg1->is_null()) {
    expr_datum.set_null();
  } else {
    routine_id = arg1->get_uint64();
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(expr.args_[1]->eval(ctx, arg2))) {
    LOG_WARN("eval arg2 failed", K(ret));
  } else if (arg2->is_null()) {
    expr_datum.set_null();
  } else {
    info_name = arg2->get_string();
  }

  if (OB_FAIL(ret) || arg1->is_null() || arg2->is_null()) {
    // do nothing
  } else if (0 == info_name.case_compare("PARAM_LIST")) {
    if (OB_FAIL(get_param_list_info(expr, ctx, expr_datum, routine_id))) {
      LOG_WARN("get param_list info failed", K(ret), K(routine_id));
    }
  } else if (0 == info_name.case_compare("RETURNS")) {
    if (OB_FAIL(get_returns_info(expr, ctx, expr_datum, routine_id))) {
      LOG_WARN("get returns info failed", K(ret), K(routine_id));
    }
  } else if (0 == info_name.case_compare("BODY")) {
    if (OB_FAIL(get_body_info(expr, ctx, expr_datum, routine_id))) {
      LOG_WARN("get body info failed", K(ret), K(routine_id));
    }
  } else if (0 == info_name.case_compare("SQL_MODE")) {
    if (OB_FAIL(get_info_by_field_id(expr, ctx, expr_datum, routine_id, SQL_MODE))) {
      LOG_WARN("get sql_mode info failed", K(ret), K(routine_id));
    }
  } else if (0 == info_name.case_compare("CHARACTER_SET_CLIENT")) {
    if (OB_FAIL(get_info_by_field_id(expr, ctx, expr_datum, routine_id, CHARACTER_SET_CLIENT))) {
      LOG_WARN("get character_set_client info failed", K(ret), K(routine_id));
    }
  } else if (0 == info_name.case_compare("COLLATION_CONNECTION")) {
    if (OB_FAIL(get_info_by_field_id(expr, ctx, expr_datum, routine_id, COLLATION_CONNECTION))) {
      LOG_WARN("get collation_connection info failed", K(ret), K(routine_id));
    }
  } else if (0 == info_name.case_compare("DB_COLLATION")) {
    if (OB_FAIL(get_info_by_field_id(expr, ctx, expr_datum, routine_id, DB_COLLATION))) {
      LOG_WARN("get db_collation info failed", K(ret), K(routine_id));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argment", K(ret), K(arg2));
  }

  return ret;
}

int ObExprMysqlProcInfo::cg_expr(ObExprCGCtx &op_cg_ctx,
                                   const ObRawExpr &raw_expr,
                                   ObExpr &rt_expr) const
{
  UNUSED(raw_expr);
  UNUSED(op_cg_ctx);
  rt_expr.eval_func_ = ObExprMysqlProcInfo::eval_mysql_proc_info;
  return OB_SUCCESS;
}

}
}
