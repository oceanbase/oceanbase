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
#include "sql/engine/expr/ob_expr_inner_table_option_printer.h"
#include <string.h>
#include "share/object/ob_obj_cast.h"
#include "objit/common/ob_item_type.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/session/ob_sql_session_info.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_schema_printer.h"
//#include "sql/engine/expr/ob_expr_promotion_util.h"


namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprInnerTableOptionPrinter::ObExprInnerTableOptionPrinter(ObIAllocator &alloc)
    : ObStringExprOperator(alloc, T_FUN_SYS_INNER_TABLE_OPTION_PRINTER, N_INNER_TABLE_OPTION_PRINTER, 3,
                           VALID_FOR_GENERATED_COL, INTERNAL_IN_MYSQL_MODE, INTERNAL_IN_ORACLE_MODE)
{
  need_charset_convert_ = false;
}

ObExprInnerTableOptionPrinter::~ObExprInnerTableOptionPrinter()
{
}

inline int ObExprInnerTableOptionPrinter::calc_result_type3(ObExprResType &type,
                                                            ObExprResType &type1,
                                                            ObExprResType &type2,
                                                            ObExprResType &type3,
                                                            common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  type1.set_calc_type(ObIntType);
  type2.set_calc_type(ObIntType);
  type3.set_calc_type(ObIntType);
  type.set_type(ObVarcharType);
  type.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
  type.set_collation_level(common::CS_LEVEL_IMPLICIT);
  return ret;
}

int ObExprInnerTableOptionPrinter::cg_expr(ObExprCGCtx &, const ObRawExpr &, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  CK(3 == rt_expr.arg_cnt_);
  rt_expr.eval_func_ = &ObExprInnerTableOptionPrinter::eval_inner_table_option_printer;
  return ret;
}

int ObExprInnerTableOptionPrinter::eval_inner_table_option_printer(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  share::schema::ObSchemaGetterGuard schema_guard;
  const ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  ObDatum *tenant_id = nullptr;
  ObDatum *database_id = nullptr;
  ObDatum *table_id = nullptr;
  const ObTableSchema *table_schema = nullptr;
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get my session", K(ret));
  } else if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get schema_service", K(ret));
  } else if (OB_FAIL(expr.eval_param_value(ctx, tenant_id, database_id, table_id))) {
    LOG_WARN("failed to eval table id", K(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(session->get_effective_tenant_id(), schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id->get_int(), table_id->get_int(), table_schema))) {
    LOG_WARN("failed to get table schema", K(ret), K(table_id->get_int()));
  } else if (nullptr == table_schema) {
    expr_datum.set_null();
  } else {
    ObSchemaPrinter schema_printer(schema_guard);
    int64_t pos = 0;
    char *buf = expr.get_str_res_mem(ctx, MAX_TABLE_STATUS_CREATE_OPTION_LENGTH);
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc buf", K(ret));
    } else if (FALSE_IT(MEMSET(buf, 0, MAX_TABLE_STATUS_CREATE_OPTION_LENGTH))) {
    } else if (OB_FAIL(schema_printer.print_table_definition_table_options(*table_schema,
                                                                    buf,
                                                                    MAX_TABLE_STATUS_CREATE_OPTION_LENGTH,
                                                                    pos,
                                                                    true))) {
      LOG_WARN("print table definition table options failed", K(ret));
    } else {
      int64_t len = strlen(buf);
      if (len > 0) {
        expr_datum.set_string(ObString(len, buf));
      } else {
        expr_datum.set_null();
      }
    }
  }
  return ret;
}

DEF_SET_LOCAL_SESSION_VARS(ObExprInnerTableOptionPrinter, raw_expr) {
  int ret = OB_SUCCESS;
  if (lib::is_mysql_mode()) {
    SET_LOCAL_SYSVAR_CAPACITY(1);
    EXPR_ADD_LOCAL_SYSVAR(SYS_VAR_COLLATION_CONNECTION);
  }
  return ret;
}

ObExprInnerTableSequenceGetter::ObExprInnerTableSequenceGetter(ObIAllocator &alloc)
    : ObExprOperator(alloc, T_FUN_SYS_INNER_TABLE_SEQUENCE_GETTER, N_INNER_TABLE_SEQUENCE_GETTER, 3,
                           VALID_FOR_GENERATED_COL, INTERNAL_IN_MYSQL_MODE, INTERNAL_IN_ORACLE_MODE)
{
}

ObExprInnerTableSequenceGetter::~ObExprInnerTableSequenceGetter()
{
}

inline int ObExprInnerTableSequenceGetter::calc_result_type3(ObExprResType &type,
                                                             ObExprResType &type1,
                                                             ObExprResType &type2,
                                                             ObExprResType &type3,
                                                             common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  type1.set_calc_type(ObIntType);
  type2.set_calc_type(ObIntType);
  type3.set_calc_type(ObIntType);
  type.set_type(ObUInt64Type);
  return ret;
}

int ObExprInnerTableSequenceGetter::cg_expr(ObExprCGCtx &, const ObRawExpr &, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  CK(3 == rt_expr.arg_cnt_);
  rt_expr.eval_func_ = &ObExprInnerTableSequenceGetter::eval_inner_table_sequence_getter;
  return ret;
}

int ObExprInnerTableSequenceGetter::eval_inner_table_sequence_getter(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  share::schema::ObSchemaGetterGuard schema_guard;
  const ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  ObDatum *tenant_id = nullptr;
  ObDatum *table_id = nullptr;
  ObDatum *auto_inc_col_id = nullptr;
  const ObTableSchema *table_schema = nullptr;
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get my session", K(ret));
  } else if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get schema_service", K(ret));
  } else if (OB_FAIL(expr.eval_param_value(ctx, tenant_id, table_id, auto_inc_col_id))) {
    LOG_WARN("failed to eval table id", K(ret));
  } else if (auto_inc_col_id->is_null() || 0 == auto_inc_col_id->get_int()) {
    expr_datum.set_null();
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(session->get_effective_tenant_id(), schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id->get_int(), table_id->get_int(), table_schema))) {
    LOG_WARN("failed to get table schema", K(ret), K(table_id->get_int()));
  } else if (nullptr == table_schema) {
    expr_datum.set_null();
  } else {
    uint64_t auto_increment = 0;
    if (OB_FAIL(share::ObAutoincrementService::get_instance().get_sequence_value(
          table_schema->get_tenant_id(), table_schema->get_table_id(),
          table_schema->get_autoinc_column_id(), table_schema->is_order_auto_increment_mode(),
          table_schema->get_truncate_version(), auto_increment))) {
      SHARE_SCHEMA_LOG(WARN, "fail to get auto_increment value", K(ret));
    } else if (auto_increment > 0) {
      if (table_schema->get_auto_increment() > auto_increment) {
        auto_increment = table_schema->get_auto_increment();
      }
      expr_datum.set_uint(auto_increment);
    } else {
      expr_datum.set_null();
    }
  }
  return ret;
}

DEF_SET_LOCAL_SESSION_VARS(ObExprInnerTableSequenceGetter, raw_expr) {
  int ret = OB_SUCCESS;
  if (lib::is_mysql_mode()) {
    SET_LOCAL_SYSVAR_CAPACITY(1);
    EXPR_ADD_LOCAL_SYSVAR(share::SYS_VAR_COLLATION_CONNECTION);
  }
  return ret;
}

}
}