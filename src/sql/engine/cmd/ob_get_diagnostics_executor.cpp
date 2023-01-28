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

#define USING_LOG_PREFIX  SQL_ENG
#include <utility>
#include "sql/engine/cmd/ob_get_diagnostics_executor.h"
#include "sql/resolver/cmd/ob_get_diagnostics_stmt.h"
#include "observer/ob_inner_sql_connection_pool.h"
#include "observer/ob_inner_sql_connection.h"
#include "observer/ob_inner_sql_result.h"
#include "share/ob_define.h"
#include "sql/session/ob_session_val_map.h"
#include "pl/ob_pl.h"
#include "share/ob_lob_access_utils.h"
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
namespace oceanbase
{
namespace sql
{

#define SET_OBJ_VAR(expect_type, integer_val, str_val, has_lob_header) do {             \
    ObObj obj;                                         \
    if (ObTinyIntType == expect_type) {                \
      obj.set_tinyint(integer_val);                    \
    } else if (ObSmallIntType == expect_type) {        \
      obj.set_smallint(integer_val);                   \
    } else if (ObMediumIntType == expect_type) {       \
      obj.set_mediumint(integer_val);                  \
    } else if (ObInt32Type == expect_type) {           \
      obj.set_int32(integer_val);                      \
    } else if (ObIntType == expect_type) {             \
      obj.set_int(integer_val);                        \
    } else if (ObUTinyIntType == expect_type) {        \
      obj.set_utinyint(integer_val);                   \
    } else if (ObUSmallIntType == expect_type) {       \
      obj.set_usmallint(integer_val);                  \
    } else if (ObUMediumIntType == expect_type) {      \
      obj.set_umediumint(integer_val);                 \
    } else if (ObUInt32Type == expect_type) {          \
      obj.set_uint32(integer_val);                     \
    } else if (ObUInt64Type == expect_type) {          \
      obj.set_uint64(integer_val);                     \
    } else if (ObFloatType == expect_type) {           \
      obj.set_float(integer_val);                      \
    } else if (ObDoubleType == expect_type) {          \
      obj.set_double(integer_val);                     \
    } else if (ObUFloatType == expect_type) {          \
      obj.set_ufloat(integer_val);                     \
    } else if (ObUDoubleType == expect_type) {         \
      obj.set_udouble(integer_val);                    \
    } else if (ObNumberType == expect_type) {          \
      number::ObNumber num;                            \
      num.from(integer_val, ctx.get_allocator());      \
      obj.set_number(num);                             \
    } else if (ObUNumberType == expect_type) {         \
      number::ObNumber num;                            \
      num.from(integer_val, ctx.get_allocator());      \
      obj.set_unumber(num);                            \
    } else if (ObVarcharType == expect_type) {         \
      obj.set_varchar(str_val);                        \
      obj.set_collation_level(CS_LEVEL_COERCIBLE);     \
      obj.set_collation_type(collation_type);          \
    } else if (ObCharType == expect_type) {            \
      obj.set_char(str_val);                           \
      obj.set_collation_level(CS_LEVEL_COERCIBLE);     \
      obj.set_collation_type(collation_type);          \
    } else if (ObTinyTextType == expect_type ||        \
               ObTextType == expect_type ||            \
               ObMediumTextType == expect_type ||      \
               ObLongTextType == expect_type ||        \
               ObJsonType == expect_type) {            \
      ObTextStringResult lob(expect_type, has_lob_header, &ctx.get_allocator());         \
      if (OB_FAIL(lob.init(str_val.length()))) {                         \
        LOG_WARN("fail to init lob", K(ret), K(lob));                    \
      } else if (OB_FAIL(lob.append(str_val))) {                         \
        LOG_WARN("fail to append str", K(ret), K(lob), K(str_val));      \
      } else {                                                           \
        ObString lob_str;                                                \
        lob.get_result_buffer(lob_str);                                  \
        obj.set_string(expect_type, lob_str);                            \
        obj.set_collation_level(CS_LEVEL_COERCIBLE);                     \
        obj.set_collation_type(collation_type);                          \
        if (lob.has_lob_header()) {                                      \
          obj.set_has_lob_header();                                      \
        }                                                                \
      }                                                                  \
    } else {                                           \
      ret = OB_NOT_SUPPORTED;                          \
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "not supported pl var type");  \
    }                                                  \
    if (OB_SUCC(ret)) {                                \
      ObObjParam value(obj);                           \
      value.set_param_meta();                          \
      result = value;                                  \
    }                                                  \
  } while (0)

ObGetDiagnosticsExecutor::ObGetDiagnosticsExecutor() {}

ObGetDiagnosticsExecutor::~ObGetDiagnosticsExecutor() {}

int ObGetDiagnosticsExecutor::assign_condition_val(ObExecContext &ctx, ObGetDiagnosticsStmt &stmt,
                          ObSQLSessionInfo *session_info,
                          sqlclient::ObISQLConnection *conn,
                          int64_t err_ret, ObString err_msg_c, ObString sql_state_c)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(session_info));
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_info_argument().count(); ++i) {
    int64_t affected_rows = 0;
    ObString var;
    ObSqlString set_sql;
    const ObString &val = stmt.get_info_argument().at(i);
    const ObRawExpr* var_expr = stmt.get_params().at(i + 1);
    CK (OB_NOT_NULL(var_expr));
    if (OB_FAIL(ret)) {
    } else if (T_OP_GET_USER_VAR == var_expr->get_expr_type()) {
      const ObSysFunRawExpr *func_expr = static_cast<const ObSysFunRawExpr*>(var_expr);
      if (OB_ISNULL(func_expr->get_param_expr(0))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sys var is NULL", K(*func_expr), K(ret));
      } else if (OB_UNLIKELY(!func_expr->get_param_expr(0)->is_const_raw_expr()
        || !static_cast<const ObConstRawExpr*>(func_expr->get_param_expr(0))->get_value().is_varchar())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid sys var", K(*func_expr->get_param_expr(0)), K(ret));
      } else {
        var = static_cast<const ObConstRawExpr*>(func_expr->get_param_expr(0))->get_value().get_varchar();
      }

      DIAG_INFO_TYPE info_type;
      OZ (stmt.get_diag_info_type_by_name(val, info_type));
      if (OB_SUCC(ret)) {
        switch (info_type) {
          case MYSQL_ERRNO_TYPE:
            OZ(set_sql.assign_fmt("set %s%s=\"%d\";", "@", var.ptr(), ob_errpkt_errno(err_ret, lib::is_oracle_mode())));
            break;
          case MESSAGE_TEXT_TYPE:
            OZ(set_sql.assign_fmt("set %s%s=\"%s\";", "@", var.ptr(), err_msg_c.ptr()));
            break;
          case RETURNED_SQLSTATE_TYPE:
            OZ(set_sql.assign_fmt("set %s%s=\"%s\";", "@", var.ptr(),
                                  err_ret > 0 ? sql_state_c.ptr() : ob_sqlstate(err_ret)));
            break;
          case CLASS_ORIGIN_TYPE:
          case SUBCLASS_ORIGIN_TYPE:
            OZ(set_sql.assign_fmt("set %s%s=\"%s\";", "@", var.ptr(), "ISO 9075"));
            break;
          case TABLE_NAME_TYPE:
          case COLUMN_NAME_TYPE:
          case CONSTRAINT_CATALOG_TYPE:
          case CONSTRAINT_SCHEMA_TYPE:
          case CONSTRAINT_NAME_TYPE:
          case CATALOG_NAME_TYPE:
          case SCHEMA_NAME_TYPE:
          case CURSOR_NAME_TYPE:
            /* do nothing */
            break;
          default:
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected type", K(ret), K(info_type));
            break;
        }
      }

      CK (OB_NOT_NULL(conn));
      if (OB_SUCC(ret) && set_sql.length() != 0) {
        if (OB_FAIL(conn->execute_write(session_info->get_effective_tenant_id(), set_sql.ptr(), affected_rows))) {
          LOG_WARN("execute write failed", K(ret), K(set_sql), K(affected_rows));
        }
      }
    } else if (OB_LIKELY(var_expr->is_const_raw_expr())) {
      const ObConstRawExpr *const_expr = static_cast<const ObConstRawExpr *>(var_expr);
      if (T_QUESTIONMARK == const_expr->get_expr_type()) {
        ObCollationType collation_type;
        ObObjParam result;
        pl::ObPLContext *pl_context = nullptr;
        pl::ObPLExecState *pl_state = nullptr;
        char str[50] = {0};
        CK (OB_NOT_NULL(pl_context = session_info->get_pl_context()));
        CK (OB_NOT_NULL(pl_state = pl_context->get_current_state()));
        OZ (session_info->get_collation_connection(collation_type));
        DIAG_INFO_TYPE info_type;
        OZ (stmt.get_diag_info_type_by_name(val, info_type));
        int64_t idx = const_expr->get_value().get_unknown();
        idx = stmt.get_origin_param_index(idx);
        CK (idx != OB_INVALID_INDEX);
        CK (idx < pl_state->get_function().get_variables().count());
        if (OB_SUCC(ret)) {
          ObObj obj;
          pl::ObPLDataType pl_data_type = pl_state->get_function().get_variables().at(idx);
          CK (nullptr != pl_data_type.get_data_type());
          if (OB_SUCC(ret)) {
            ObObjType expect_type = pl_data_type.get_data_type()->get_obj_type();
            bool has_lob_header = pl_data_type.get_data_type()->get_meta_type().has_lob_header();
            switch (info_type) {
              case MYSQL_ERRNO_TYPE:
                {
                  int64_t errnum = ob_errpkt_errno(err_ret, lib::is_oracle_mode());
                  sprintf(str, "%ld", errnum);
                  SET_OBJ_VAR(expect_type, errnum, ObString(str), has_lob_header);
                }
                break;
              case MESSAGE_TEXT_TYPE:
                {
                  SET_OBJ_VAR(expect_type, (int64_t)0, err_msg_c, has_lob_header);
                }
                break;
              case RETURNED_SQLSTATE_TYPE:
                {
                  ObString sqlstate(ob_sqlstate(err_ret));
                  if (err_ret > 0) {
                    SET_OBJ_VAR(expect_type, (int64_t)0, sql_state_c, has_lob_header);
                  } else {
                    SET_OBJ_VAR(expect_type, (int64_t)0, sqlstate, has_lob_header);
                  }
                }
                break;
              case CLASS_ORIGIN_TYPE:
              case SUBCLASS_ORIGIN_TYPE:
                {
                  SET_OBJ_VAR(expect_type, (int64_t)0, ObString("ISO 9075"), has_lob_header);
                }
                break;
              case COLUMN_NAME_TYPE:
              case CONSTRAINT_CATALOG_TYPE:
              case CONSTRAINT_SCHEMA_TYPE:
              case CONSTRAINT_NAME_TYPE:
              case CATALOG_NAME_TYPE:
              case SCHEMA_NAME_TYPE:
              case CURSOR_NAME_TYPE:
                /* do nothing */
                break;
              default:
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("unexpected type", K(ret), K(info_type));
                break;
            }
          }
        }

        if (OB_SUCC(ret)) {
          OZ (pl_state->set_var(idx, result));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected expr type", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected expr type", K(ret));
    }
  }
  return ret;
}

int ObGetDiagnosticsExecutor::get_condition_num(ObExecContext &ctx, ObGetDiagnosticsStmt &stmt, int64_t &num)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session_info = ctx.get_my_session();
  pl::ObPLContext *pl_context = nullptr;
  pl::ObPLExecState *pl_state = nullptr;
  const ObRawExpr* cond_argument = nullptr;

  CK(OB_NOT_NULL(session_info));
  if (0 == stmt.get_params().count()) {
    /* 发生了OB_ERR_BAD_FIELD_ERROR错误，但是语句能够正常执行 */
    ret = OB_ERR_BAD_FIELD_ERROR;
  } else {
    CK (stmt.get_params().count() == stmt.get_info_argument().count() + 1);
    OX (cond_argument = stmt.get_params().at(0));
    CK (OB_NOT_NULL(cond_argument));
    if (OB_SUCC(ret)) {
      if (OB_LIKELY(cond_argument->is_const_raw_expr())) {
        const ObConstRawExpr *const_expr = static_cast<const ObConstRawExpr *>(cond_argument);
        if (T_QUESTIONMARK == const_expr->get_expr_type()) {
          CK (OB_NOT_NULL(pl_context = session_info->get_pl_context()));
          CK (OB_NOT_NULL(pl_state = pl_context->get_current_state()));
          ObObjParam value;
          int64_t idx = const_expr->get_value().get_unknown();
          idx = stmt.get_origin_param_index(idx);
          CK (idx != OB_INVALID_INDEX);
          OZ (pl_state->get_var(idx, value));
          OX (num = value.get_int());
          if (OB_SUCC(ret)) {
            ret = (num <= 0 || num >= MAX_BUFFER_SIZE) ? OB_ERR_INVALID_CONDITION_NUMBER : ret;
          }
        } else if (T_INT == const_expr->get_expr_type() && const_expr->get_value().is_integer_type()) {
          num = const_expr->get_value().get_int();
          if (OB_SUCC(ret)) {
            ret = (num <= 0 || num >= MAX_BUFFER_SIZE) ? OB_ERR_INVALID_CONDITION_NUMBER : ret;
          }
        } else if (T_VARCHAR == cond_argument->get_expr_type()) {
          //to make '12321' string be a number, 'as2da3' string still be a string.
          ObString arg_str = const_expr->get_value().get_string();
          if (arg_str.ptr() == NULL) {
            ret = OB_ERR_INVALID_CONDITION_NUMBER;
          } else {
            int64_t number = strtoll(arg_str.ptr(), NULL, 10);
            if (INT64_MAX == number || number <= 0 || number >= MAX_BUFFER_SIZE) {
              ret = OB_ERR_INVALID_CONDITION_NUMBER;
            } else {
              num = number;
            }
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("condition argument is invalid", K(ret));
        }
      } else if (T_OP_GET_USER_VAR == cond_argument->get_expr_type()) {
        const ObSysFunRawExpr *func_expr = static_cast<const ObSysFunRawExpr*>(cond_argument);
        if (OB_ISNULL(func_expr->get_param_expr(0))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("sys var is NULL", K(*func_expr), K(ret));
        } else if (OB_UNLIKELY(!func_expr->get_param_expr(0)->is_const_raw_expr()
          || !static_cast<const ObConstRawExpr*>(func_expr->get_param_expr(0))->get_value().is_varchar())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid sys var", K(*func_expr->get_param_expr(0)), K(ret));
        } else {
          const ObString var_name = static_cast<const ObConstRawExpr*>(func_expr->get_param_expr(0))->get_value().get_varchar();
          ObObj value;
          int64_t number = 0;
          if (OB_FAIL(session_info->get_user_variable_value(var_name, value))) {
            LOG_WARN("get user variable failed", K(var_name), K(ret));
          } else if (value.is_integer_type()) {
            OZ (value.get_int(num));
          } else if (value.is_string_type()) {
            ObString str;
            if (OB_FAIL(value.get_varchar(str))) {
              LOG_WARN("get varchar failed", K(ret));
            } else if (FALSE_IT(number = strtoll(str.ptr(), NULL, 10))) {
            } else if (INT64_MAX == number || number <= 0 || number >= MAX_BUFFER_SIZE) {
              ret = OB_ERR_INVALID_CONDITION_NUMBER;
            } else {
              num = number;
            }
          }
        }
      } else if (T_OP_EXCEPT == cond_argument->get_expr_type()) {
        ret = OB_ERR_USER_VARIABLE_UNKNOWN;
      }
    }
  }

  return ret;
}
 
int ObGetDiagnosticsExecutor::execute(ObExecContext &ctx, ObGetDiagnosticsStmt &stmt) {
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session_info = ctx.get_my_session();
  ObPhysicalPlanCtx *plan_ctx = ctx.get_physical_plan_ctx();
  sqlclient::ObISQLConnection *conn = NULL;
  observer::ObInnerSQLConnectionPool *pool = NULL;
  ObMySQLProxy *sql_proxy = ctx.get_sql_proxy();
  uint64_t tenant_id = 0; 
  int64_t warning_count = 0;
  ObSqlString query_virtual;
  if (OB_ISNULL(session_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(session_info));
  } else if (OB_ISNULL(sql_proxy = GCTX.sql_proxy_) || OB_ISNULL(sql_proxy->get_pool())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy must not null", K(ret), KP(GCTX.sql_proxy_));
  } else if (OB_UNLIKELY(sqlclient::INNER_POOL != sql_proxy->get_pool()->get_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pool type must be inner", K(ret), "type", sql_proxy->get_pool()->get_type());
  } else if (OB_ISNULL(pool = static_cast<observer::ObInnerSQLConnectionPool*>(sql_proxy->get_pool()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pool must not null", K(ret));
  } else if (OB_FAIL(pool->acquire(session_info, conn))) {
    LOG_WARN("failed to get conn", K(ret));
  } else if (OB_FAIL(query_virtual.assign_fmt("select count(*) from %s.%s", 
                      OB_SYS_DATABASE_NAME, OB_TENANT_VIRTUAL_WARNING_TNAME))) {
    LOG_WARN("assign format failed", K(ret));
  } else {
    SMART_VAR(ObISQLClient::ReadResult, res) {
      tenant_id = session_info->get_effective_tenant_id();
      common::sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(conn->execute_read(tenant_id, query_virtual.ptr(), res))) {
        LOG_WARN("Failed to spi_query", K(query_virtual), K(ret));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get result", K(ret));
      } else if (OB_SUCC(result->next())) {
        EXTRACT_INT_FIELD_MYSQL(*result, "count(*)", warning_count, int64_t);
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (stmt.get_diagnostics_type() == DiagnosticsType::GET_CURRENT_COND) {
    int64_t restored_arg;
    OZ (get_condition_num(ctx, stmt, restored_arg));
    if (OB_ERR_INVALID_CONDITION_NUMBER == ret) {
      ret = OB_SUCCESS;
      LOG_USER_WARN(OB_ERR_INVALID_CONDITION_NUMBER);
    } else if (OB_ERR_USER_VARIABLE_UNKNOWN == ret || OB_OBJ_TYPE_ERROR == ret) {
      ret = OB_SUCCESS;
      LOG_TRACE("condition num is invalid");
      LOG_USER_WARN(OB_ERR_INVALID_CONDITION_NUMBER);
    } else if (OB_ERR_BAD_FIELD_ERROR == ret) {
      ret = OB_SUCCESS;
      ObString scope_name = ObString::make_string("field_list");
      ObString invalid_condition_name = stmt.get_invalid_condition_name();
      LOG_USER_WARN(OB_ERR_BAD_FIELD_ERROR,
                    invalid_condition_name.length(), invalid_condition_name.ptr(),
                    static_cast<int32_t>(scope_name.length()), scope_name.ptr());
    } else if (OB_FAIL(ret)) {
      LOG_WARN("unexpected error", K(ret));
    } else if (warning_count < restored_arg || restored_arg < 1) {
      LOG_TRACE("condition num is invalid");
      LOG_USER_WARN(OB_ERR_INVALID_CONDITION_NUMBER);
    } else {
      int err_ret;
      ObString err_msg, err_msg_c, sqlstate, sqlstate_c;
      ObSqlString query_virtual;
      if (OB_FAIL(query_virtual.assign_fmt(
        "select message, ori_code, sql_state from %s.%s limit %ld, 1", 
        OB_SYS_DATABASE_NAME, OB_TENANT_VIRTUAL_WARNING_TNAME, restored_arg - 1))) {
        LOG_WARN("assign fmt failed", K(ret));
      } else {
        SMART_VAR(ObISQLClient::ReadResult, res) {
          common::sqlclient::ObMySQLResult *result = NULL;
          if (OB_FAIL(conn->execute_read(tenant_id, query_virtual.ptr(), res))) {
            LOG_WARN("Failed to spi_query", K(query_virtual), K(ret));
          } else if (OB_ISNULL(result = res.get_result())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to get result", K(ret));
          } else if (OB_SUCC(result->next())) {
            EXTRACT_VARCHAR_FIELD_MYSQL(*result, "message", err_msg);
            EXTRACT_INT_FIELD_MYSQL(*result, "ori_code", err_ret, int);
            EXTRACT_VARCHAR_FIELD_MYSQL(*result, "sql_state", sqlstate);
            if (OB_FAIL(ret)) {
            } else if (OB_FAIL(ob_write_string(ctx.get_allocator(), err_msg, err_msg_c, true))) {
              //when using ptr(), char *'s end should be '\0'
              LOG_WARN("ob write string failed", K(ret));
            } else if (OB_FAIL(ob_write_string(ctx.get_allocator(), sqlstate, sqlstate_c, true))) { 
              //when using ptr(), char *'s end should be '\0'
              LOG_WARN("ob write string failed", K(ret));
            }
          }
        }
      }
      OZ (assign_condition_val(ctx, stmt, session_info, conn, err_ret, err_msg_c, sqlstate_c));
    }
  } else if (stmt.get_diagnostics_type() == DiagnosticsType::GET_STACKED_COND) {
    int64_t restored_arg = 0;
    OZ (get_condition_num(ctx, stmt, restored_arg));
    if (OB_ERR_INVALID_CONDITION_NUMBER == ret) {
      ret = OB_SUCCESS;
      LOG_USER_WARN(OB_ERR_INVALID_CONDITION_NUMBER);
    } else if (OB_ERR_USER_VARIABLE_UNKNOWN == ret || OB_OBJ_TYPE_ERROR == ret) {
      ret = OB_SUCCESS;
      LOG_TRACE("condition num is invalid");
      LOG_USER_WARN(OB_ERR_INVALID_CONDITION_NUMBER);
    } else if (OB_ERR_BAD_FIELD_ERROR == ret) {
      ret = OB_SUCCESS;
      ObString scope_name = ObString::make_string("field_list");
      ObString invalid_condition_name = stmt.get_invalid_condition_name();
      LOG_USER_WARN(OB_ERR_BAD_FIELD_ERROR,
                    invalid_condition_name.length(), invalid_condition_name.ptr(),
                    static_cast<int32_t>(scope_name.length()), scope_name.ptr());
    } else if (OB_FAIL(ret)) {
      LOG_WARN("unexpected error", K(ret));
    } else if (restored_arg > 1) { /* todo:hr-当前stack诊断区只支持存一条信息 */
      LOG_TRACE("type ok but out of range", K(restored_arg));
      LOG_USER_WARN(OB_ERR_INVALID_CONDITION_NUMBER);
    } else {
      int err_ret;
      int mysql_errcode;
      ObString err_msg_c, sqlstate_c;
      pl::ObPLSqlCodeInfo *sqlcode_info = NULL;
      int64_t idx;
      CK (OB_NOT_NULL(session_info->get_pl_sqlcode_info()));
      OX (sqlcode_info = session_info->get_pl_sqlcode_info());
      CK (sqlcode_info->get_stack_warning_buf().count() > 0);
      OX (idx = sqlcode_info->get_stack_warning_buf().count() - 1);
      if (OB_SUCC(ret)) {
        err_ret = sqlcode_info->get_stack_warning_buf().at(idx).get_err_code();
        err_msg_c = sqlcode_info->get_stack_warning_buf().at(idx).get_err_msg();
        sqlstate_c = sqlcode_info->get_stack_warning_buf().at(idx).get_sql_state();
      }
      OZ (assign_condition_val(ctx, stmt, session_info, conn, err_ret, err_msg_c, sqlstate_c));
    }
  } else if (stmt.get_diagnostics_type() == DiagnosticsType::GET_CURRENT_INFO ||
             stmt.get_diagnostics_type() == DiagnosticsType::GET_STACKED_INFO) {
    CK (stmt.get_params().count() == stmt.get_info_argument().count());
    int64_t old_affected_rows = session_info->get_affected_rows();
    int64_t number;
    if (stmt.get_diagnostics_type() == DiagnosticsType::GET_CURRENT_INFO) {
      number = warning_count;
    } else {
      int64_t idx;
      pl::ObPLSqlCodeInfo *sqlcode_info = NULL;
      CK (OB_NOT_NULL(session_info->get_pl_sqlcode_info()));
      OX (sqlcode_info = session_info->get_pl_sqlcode_info());
      CK (sqlcode_info->get_stack_warning_buf().count() > 0);
      OX (idx = sqlcode_info->get_stack_warning_buf().count() - 1);
      OX (number = 1 + sqlcode_info->get_stack_warning_buf().at(idx).get_readable_warning_count());
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_info_argument().count(); ++i) {
      ObString var;
      const ObString &val = stmt.get_info_argument().at(i);
      int64_t affected_rows = 0;
      ObSqlString set_sql;
      const ObRawExpr* var_expr = stmt.get_params().at(i);
      CK (OB_NOT_NULL(var_expr));
      if (OB_FAIL(ret)) {
      } else if (T_OP_GET_USER_VAR == var_expr->get_expr_type()) {
        const ObSysFunRawExpr *func_expr = static_cast<const ObSysFunRawExpr*>(var_expr);
        if (OB_ISNULL(func_expr->get_param_expr(0))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("sys var is NULL", K(*func_expr), K(ret));
        } else if (OB_UNLIKELY(!func_expr->get_param_expr(0)->is_const_raw_expr()
          || !static_cast<const ObConstRawExpr*>(func_expr->get_param_expr(0))->get_value().is_varchar())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid sys var", K(*func_expr->get_param_expr(0)), K(ret));
        } else {
          var = static_cast<const ObConstRawExpr*>(func_expr->get_param_expr(0))->get_value().get_varchar();
        }
        if (OB_FAIL(ret)) {
        } else if (val == "NUMBER") {
          if (OB_FAIL(set_sql.assign_fmt("set %s%s=%ld;", "@", var.ptr(), number))) {
            LOG_WARN("assign fmt failed", K(ret));
          }
        } else if (val == "ROW_COUNT") {
          if (OB_FAIL(set_sql.assign_fmt("set %s%s=%ld;", "@", var.ptr(), old_affected_rows))) {
            LOG_WARN("assign fmt failed", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(conn->execute_write(tenant_id, set_sql.ptr(), affected_rows))) {
            LOG_WARN("execute write failed", K(ret), K(set_sql), K(affected_rows));
          }
        }
      } else if (OB_LIKELY(var_expr->is_const_raw_expr())) {
        const ObConstRawExpr *const_expr = static_cast<const ObConstRawExpr *>(var_expr);
        if (T_QUESTIONMARK == const_expr->get_expr_type()) {
          pl::ObPLContext *pl_context = NULL;
          pl::ObPLExecState *pl_state = NULL;
          CK (OB_NOT_NULL(pl_context = session_info->get_pl_context()));
          CK (OB_NOT_NULL(pl_state = pl_context->get_current_state()));

          ObObjParam result;
          ObObjParam origin_obj;
          ObCollationType collation_type;
          char str[50] = {0};
          int64_t idx = const_expr->get_value().get_unknown();
          idx = stmt.get_origin_param_index(idx);
          CK (idx != OB_INVALID_INDEX);
          CK (idx < pl_state->get_function().get_variables().count());
          OZ (session_info->get_collation_connection(collation_type));
          if (OB_SUCC(ret)) {
            pl::ObPLDataType pl_data_type = pl_state->get_function().get_variables().at(idx);
            CK (nullptr != pl_data_type.get_data_type());
            if (OB_SUCC(ret)) {
              ObObjType expect_type = pl_data_type.get_data_type()->get_obj_type();
              bool has_lob_header = pl_data_type.get_data_type()->get_meta_type().has_lob_header();
              if (val == "NUMBER") {
                sprintf(str, "%ld", number);
                SET_OBJ_VAR(expect_type, number, ObString(str), has_lob_header);
              } else if (val == "ROW_COUNT") {
                sprintf(str, "%ld", old_affected_rows);
                SET_OBJ_VAR(expect_type, old_affected_rows, ObString(str), has_lob_header);
              } else {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("unexpected", K(val), K(ret));
              }
            }
          }

          if (OB_SUCC(ret)) {
            OZ (pl_state->set_var(idx, result));
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected expr type", K(ret));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected expr type", K(ret));
      }
    }
  }
  if (OB_FAIL(pool->release(conn, true))) {
    LOG_WARN("release failed", K(ret));
  }
  return ret;
}
#undef SET_OBJ_VAR
}/* ns sql*/
}/* ns oceanbase */
