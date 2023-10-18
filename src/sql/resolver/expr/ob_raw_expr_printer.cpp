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

#define USING_LOG_PREFIX SQL
#include "sql/resolver/expr/ob_raw_expr_printer.h"
#include "lib/oblog/ob_log_module.h"
#include "sql/ob_select_stmt_printer.h"
#include "sql/engine/expr/ob_expr_column_conv.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "lib/string/ob_sql_string.h"
#include "lib/worker.h"
#include "pl/ob_pl_user_type.h"
#include "pl/ob_pl_stmt.h"

namespace oceanbase
{
using namespace common;
namespace sql
{
ObRawExprPrinter::ObRawExprPrinter()
  : buf_(NULL),
      buf_len_(0),
      pos_(NULL),
      scope_(T_NONE_SCOPE),
      only_column_namespace_(false),
      tz_info_(NULL),
      param_store_(NULL),
      schema_guard_(NULL),
      print_cte_(false)
{
}

ObRawExprPrinter::ObRawExprPrinter(char *buf, int64_t buf_len, int64_t *pos, ObSchemaGetterGuard *schema_guard,
                                   ObObjPrintParams print_params, const ParamStore *param_store)
    : buf_(buf),
      buf_len_(buf_len),
      pos_(pos),
      scope_(T_NONE_SCOPE),
      only_column_namespace_(false),
      tz_info_(NULL),
      print_params_(print_params),
      param_store_(param_store),
      schema_guard_(schema_guard),
      print_cte_(false)
{
}

ObRawExprPrinter::~ObRawExprPrinter()
{
}

void ObRawExprPrinter::init(char *buf, int64_t buf_len, int64_t *pos, ObSchemaGetterGuard *schema_guard,
                            ObObjPrintParams print_params, const ParamStore *param_store)
{
  buf_ = buf;
  buf_len_ = buf_len;
  pos_ = pos;
  scope_ = T_NONE_SCOPE;
  schema_guard_ = schema_guard;
  print_params_ = print_params;
  param_store_ = param_store;
}

int ObRawExprPrinter::do_print(ObRawExpr *expr, ObStmtScope scope, bool only_column_namespace, bool print_cte)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr should not be NULL", K(ret));
  } else {
    scope_ = scope;
    only_column_namespace_ = only_column_namespace;
    print_cte_ = print_cte;
    PRINT_EXPR(expr);
  }
  return ret;
}

int ObRawExprPrinter::print(ObRawExpr *expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf_) || OB_ISNULL(pos_) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_ is NULL of buf_ is NULL or pos_ is NULL or expr is NULL", K(ret));
  } else if (!expr->get_alias_column_name().empty()
      && !expr->is_column_ref_expr()
      && !expr->is_aggr_expr()
      && !expr->is_pseudo_column_expr()
     // quertionmark 是一个const expr，如果是prepare，需要打印成:0这样的东西，不能用alias来代替
      && T_QUESTIONMARK != expr->get_expr_type()
      && scope_ != T_DBLINK_SCOPE
      && scope_ != T_FIELD_LIST_SCOPE
      && scope_ != T_GROUP_SCOPE
      && scope_ != T_WHERE_SCOPE
      && scope_ != T_NONE_SCOPE
      && scope_ != T_ORDER_SCOPE
      && (scope_ == T_HAVING_SCOPE && lib::is_mysql_mode())) {
    //expr is a alias column ref
    //alias column target list
    PRINT_IDENT_WITH_QUOT(expr->get_alias_column_name());
  } else {
    switch (expr->get_expr_class()) {
    case ObRawExpr::EXPR_CONST: {
      ObConstRawExpr *con_expr = static_cast<ObConstRawExpr*>(expr);
      PRINT_EXPR(con_expr);
      break;
    }
    case ObRawExpr::EXPR_EXEC_PARAM: {
      ObExecParamRawExpr *exec_expr = static_cast<ObExecParamRawExpr*>(expr);
      if (print_params_.for_dblink_) {
        bool print_ref = exec_expr->is_ref_same_dblink();
        if ((OB_E(EventTable::EN_GENERATE_PLAN_WITH_RECONSTRUCT_SQL) OB_SUCCESS) == OB_SUCCESS) {
          //do nothing
        } else {
          print_ref = true;
        }
        if (OB_ISNULL(exec_expr->get_ref_expr())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null", K(ret));
        } else if (exec_expr->get_ref_expr()->is_query_ref_expr()) {
          print_ref = true;
        }
        if (print_ref) {
          PRINT_EXPR(exec_expr->get_ref_expr());
        } else {
          PRINT_EXPR(exec_expr);
        }
      } else {
        PRINT_EXPR(exec_expr->get_ref_expr());
      }
      break;
    }
    case ObRawExpr::EXPR_QUERY_REF: {
      ObQueryRefRawExpr *una_expr = static_cast<ObQueryRefRawExpr*>(expr);
      PRINT_EXPR(una_expr);
      break;
    }
    case ObRawExpr::EXPR_PL_QUERY_REF: {
      ObPlQueryRefRawExpr *pl_subquery_expr = static_cast<ObPlQueryRefRawExpr*>(expr);
      PRINT_EXPR(pl_subquery_expr);
      break;
    }
    case ObRawExpr::EXPR_COLUMN_REF: {
      ObColumnRefRawExpr *bin_expr = static_cast<ObColumnRefRawExpr*>(expr);
      PRINT_EXPR(bin_expr);
      break;
    }
    case ObRawExpr::EXPR_OPERATOR: {
      ObOpRawExpr *op_expr = static_cast<ObOpRawExpr*>(expr);
      PRINT_EXPR(op_expr);
      break;
    }
    case ObRawExpr::EXPR_CASE_OPERATOR: {
      ObCaseOpRawExpr *case_expr = static_cast<ObCaseOpRawExpr*>(expr);
      PRINT_EXPR(case_expr);
      break;
    }
    case ObRawExpr::EXPR_AGGR: {
      ObAggFunRawExpr *agg_expr = static_cast<ObAggFunRawExpr*>(expr);
      PRINT_EXPR(agg_expr);
      break;
    }
    case ObRawExpr::EXPR_SYS_FUNC: {
      ObSysFunRawExpr *sys_expr = static_cast<ObSysFunRawExpr*>(expr);
      PRINT_EXPR(sys_expr);
      break;
    }
    case ObRawExpr::EXPR_UDF: {
      ObUDFRawExpr *udf_expr = static_cast<ObUDFRawExpr *>(expr);
      PRINT_EXPR(udf_expr);
      break;
    }
    case ObRawExpr::EXPR_WINDOW: {
      ObWinFunRawExpr *win_expr = static_cast<ObWinFunRawExpr *>(expr);
      PRINT_EXPR(win_expr);
      break;
    }
    case ObRawExpr::EXPR_PSEUDO_COLUMN: {
      ObPseudoColumnRawExpr *pse_expr = static_cast<ObPseudoColumnRawExpr *>(expr);
      PRINT_EXPR(pse_expr);
      break;
    }
    case ObRawExpr::EXPR_SET_OP: {
      ObSetOpRawExpr *set_op_expr = static_cast<ObSetOpRawExpr *>(expr);
      PRINT_EXPR(set_op_expr);
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unknown expr class", K(ret), K(expr->get_expr_class()));
      break;
    }
    }
  }

  return ret;
}

int ObRawExprPrinter::print(ObConstRawExpr *expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf_) || OB_ISNULL(pos_) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_ is NULL of buf_ is NULL or pos_ is NULL or expr is NULL", K(ret));
  } else if (print_params_.for_dblink_ && T_QUESTIONMARK == expr->get_expr_type()) {
    int64_t idx = expr->get_value().get_unknown();
    bool is_bool_expr = false;
    if (expr->is_exec_param_expr()) {
      ObExecParamRawExpr *exec_expr = static_cast<ObExecParamRawExpr*>(expr);
      if (OB_FAIL(ObRawExprUtils::check_is_bool_expr(exec_expr->get_ref_expr(), is_bool_expr))) {
        LOG_WARN("failed to check is bool expr", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (is_bool_expr && OB_FAIL(databuff_printf(buf_, buf_len_, *pos_, "(1 = "))) {
      /**
       * For SQL like "select * from T1 where C1 = 1 and C1 = 2",
       * because the where clause is always false,
       * the optimizer will replace the filter with startup_filter.
       * Therefore, dblink needs to handle this special case
       * by rewriting startup_filter as "0 = 1" or "1 = 1".
       *
       */
      LOG_WARN("fail to print 1 =", K(ret));
    } else if (OB_NOT_NULL(param_store_) && 0 <= idx && idx < param_store_->count()) {
      OZ (param_store_->at(idx).print_sql_literal(buf_, buf_len_, *pos_, print_params_));
    } else if (OB_FAIL(ObLinkStmtParam::write(buf_, buf_len_, *pos_,
                                              expr->get_value().get_unknown(),
                                              expr->get_data_type()))) {
      LOG_WARN("fail to write param to buf", K(expr->get_value().get_unknown()), K(expr->get_expr_obj_meta()), K(ret));
    }
    if (is_bool_expr){
      DATA_PRINTF(")");
    }
  } else if (OB_NOT_NULL(param_store_) && T_QUESTIONMARK == expr->get_expr_type()) {
    int64_t idx = expr->get_value().get_unknown();
    CK (0 <= idx && idx < param_store_->count());
    if (OB_FAIL(ret)) {
    } else if (param_store_->at(idx).is_datetime()) {
      int32_t tmp_date = 0;
      if (OB_FAIL(databuff_printf(buf_, buf_len_, *pos_, "%s '", LITERAL_PREFIX_DATE))) {
        LOG_WARN("fail to print literal prefix", K(ret));
      } else if (OB_FAIL(ObTimeConverter::datetime_to_date(param_store_->at(idx).get_datetime(), NULL, tmp_date))) {
        LOG_WARN("fail to datetime_to_date", "datetime", param_store_->at(idx).get_datetime(), K(ret));
      } else if (OB_FAIL(ObTimeConverter::date_to_str(tmp_date, buf_, buf_len_, *pos_))) {
        LOG_WARN("fail to date_to_str", K(tmp_date), K(ret));
      } else if (OB_FAIL(databuff_printf(buf_, buf_len_, *pos_, "'"))) {
        LOG_WARN("fail to print single quote", K(ret));
      }
    } else {
      //timestamp time zone type need print prefix.
      if (param_store_->at(idx).is_timestamp_tz()) {
        DATA_PRINTF(" timestamp ");
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(param_store_->at(idx).print_sql_literal(buf_, buf_len_, *pos_, print_params_))) {
          LOG_WARN("failed to print sql literal", K(ret));
        }
      }
    }
  } else if (expr->get_literal_prefix().empty()) {
    //for empty string in Oracle mode , we should use char/nchar-type obj to print
    if (expr->get_value().is_null() && (ObCharType == expr->get_expr_obj_meta().get_type()
                                        || ObNCharType == expr->get_expr_obj_meta().get_type())) {
      ObObj empty_string = expr->get_value();
      empty_string.set_meta_type(expr->get_expr_obj_meta());
      if (OB_FAIL(empty_string.print_sql_literal(buf_, buf_len_, *pos_, print_params_))) {
        LOG_WARN("fail to print sql literal", K(ret));
      }
    } else if (expr->get_expr_type() == T_DATE &&
               OB_FAIL(databuff_printf(buf_, buf_len_, *pos_, "date "))) {
      LOG_WARN("fail to print date string", K(ret));
    } else if (T_BOOL == expr->get_expr_type()) {
      /**
       * For SQL like "select * from T1 where C1 = 1 and C1 = 2",
       * because the where clause is always false, 
       * the optimizer will replace the filter with startup_filter.
       * Therefore, dblink needs to handle this special case 
       * by rewriting startup_filter as "0 = 1" or "1 = 1".
       * 
       */
      if (OB_FAIL(databuff_printf(buf_, buf_len_, *pos_, expr->get_value().get_bool() ? "(1 = 1)" : "(0 = 1)"))) {
        LOG_WARN("fail to print startup filter", K(ret));
      }
    } else if (OB_FAIL(expr->get_value().print_sql_literal(buf_, buf_len_, *pos_, print_params_))) {
      LOG_WARN("fail to print sql literal", K(ret));
    }
  } else if (expr->get_literal_prefix() == LITERAL_PREFIX_DATE && expr->get_value().is_datetime()) {
    int32_t tmp_date = 0;
    if (OB_FAIL(databuff_printf(buf_, buf_len_, *pos_, "%s '", LITERAL_PREFIX_DATE))) {
      LOG_WARN("fail to print literal prefix", K(ret));
    } else if (OB_FAIL(ObTimeConverter::datetime_to_date(expr->get_value().get_datetime(), NULL, tmp_date))) {
      LOG_WARN("fail to datetime_to_date", "datetime", expr->get_value().get_datetime(), K(ret));
    } else if (OB_FAIL(ObTimeConverter::date_to_str(tmp_date, buf_, buf_len_, *pos_))) {
      LOG_WARN("fail to date_to_str", K(tmp_date), K(ret));
    } else if (OB_FAIL(databuff_printf(buf_, buf_len_, *pos_, "'"))) {
      LOG_WARN("fail to print single quote", K(ret));
    }
  } else if (expr->get_literal_prefix().prefix_match_ci(ORACLE_LITERAL_PREFIX_INTERVAL)
            || (expr->get_value().is_oracle_decimal())) {
    if (OB_FAIL(databuff_printf(buf_, buf_len_, *pos_, "%.*s",
                                expr->get_literal_prefix().length(), expr->get_literal_prefix().ptr()))) {
      LOG_WARN("fail to print literal suffix", K(ret));
    }
  } else {
    if (OB_FAIL(databuff_printf(buf_, buf_len_, *pos_, "%.*s ", LEN_AND_PTR(expr->get_literal_prefix())))) {
       LOG_WARN("fail to print literal prefix", K(ret));
    } else if (!expr->is_date_unit()
            && OB_FAIL(expr->get_value().print_sql_literal(buf_, buf_len_, *pos_, print_params_))) {
      LOG_WARN("fail to print sql literal", K(ret));
    }
  }
  return ret;
}

int ObRawExprPrinter::print(ObQueryRefRawExpr *expr)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf_) || OB_ISNULL(pos_) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_ is NULL of buf_ is NULL or pos_ is NULL or expr is NULL", K(ret));
  } else {
    if (expr->is_cursor()) {
      DATA_PRINTF("CURSOR");
    } else if (expr->is_multiset()) {
      DATA_PRINTF("MULTISET");
    }
    if (OB_SUCC(ret)) {
      ObStmt *stmt = expr->get_ref_stmt();
      if (OB_ISNULL(stmt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("stmt is NULL", K(ret));
      } else {
        if (stmt->is_select_stmt()) {
          DATA_PRINTF("(");
          ObSelectStmtPrinter stmt_printer(buf_,
                                           buf_len_,
                                           pos_,
                                           static_cast<ObSelectStmt*>(stmt),
                                           schema_guard_,
                                           print_params_,
                                           param_store_);
          if (print_cte_) {
            stmt_printer.enable_print_temp_table_as_cte();
          }
          if (OB_FAIL(stmt_printer.do_print())) {
            LOG_WARN("fail to print ref query", K(ret));
          }
          DATA_PRINTF(")");
        }
      }
    }
  }

  return ret;
}

int ObRawExprPrinter::print(ObColumnRefRawExpr *expr)
{
  ObArenaAllocator allocator("PrintRefColumn");
  int ret = OB_SUCCESS;
  bool is_oracle_mode = lib::is_oracle_mode();
  if (OB_ISNULL(buf_) || OB_ISNULL(pos_) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_ is NULL of buf_ is NULL or pos_ is NULL or expr is NULL", K(ret));
  } else if (expr->is_generated_column() && expr->is_hidden_column()
             && OB_NOT_NULL(expr->get_dependant_expr())) {
    PRINT_EXPR(expr->get_dependant_expr());
  } else {
    ObArenaAllocator arena_alloc;
    ObString col_name = expr->get_column_name();
    if (is_oracle_mode &&
          OB_FAIL(ObSelectStmtPrinter::remove_double_quotation_for_string(col_name, arena_alloc))) {
      LOG_WARN("failed to remove double quotation for string", K(ret));
    } else if (print_params_.for_dblink_) {
      if (!expr->is_cte_generated_column() &&
          !expr->get_database_name().empty()) {
        PRINT_IDENT_WITH_QUOT(expr->get_database_name());
        DATA_PRINTF(".");
      }
      if (!expr->get_table_name().empty()) {
        ObString table_name = expr->get_table_name();
        PRINT_IDENT_WITH_QUOT(table_name);
        DATA_PRINTF(".");
      }
      PRINT_IDENT_WITH_QUOT(col_name);
    } else if (expr->is_cte_generated_column()) {
      ObString table_name = expr->get_synonym_name().empty() ?
                                                  expr->get_table_name() : expr->get_synonym_name();
      if (OB_SUCC(ret)) {
        // note: expr's table_name is equal to alias if table's alias is not empty,
        PRINT_IDENT_WITH_QUOT(table_name);
        DATA_PRINTF(".");
        PRINT_IDENT_WITH_QUOT(col_name);
      }
    } else if (OB_UNLIKELY(only_column_namespace_)) {
      PRINT_IDENT_WITH_QUOT(col_name);
    } else if (expr->is_from_alias_table()) {
      PRINT_IDENT_WITH_QUOT(expr->get_table_name());
      DATA_PRINTF(".");
      PRINT_IDENT_WITH_QUOT(col_name);
    } else {
      if (!expr->get_synonym_name().empty() && !expr->get_synonym_db_name().empty()) {
        ObString synonyn_db_name = expr->get_synonym_db_name();
        PRINT_IDENT_WITH_QUOT(synonyn_db_name);
        DATA_PRINTF(".");
      } else if (!expr->get_synonym_name().empty() && expr->get_synonym_db_name().empty()) {
        // do nothing, synonym database name is not explicit
      } else if (expr->get_database_name().length() > 0) {
        ObString database_name = expr->get_database_name();
        PRINT_IDENT_WITH_QUOT(database_name);
        DATA_PRINTF(".");
      }
      ObString table_name = expr->get_synonym_name().empty() ?
                                  expr->get_table_name() : expr->get_synonym_name();
      if (OB_SUCC(ret)) {
        // note: expr's table_name is equal to alias if table's alias is not empty,
        if (!table_name.empty()) {
          PRINT_IDENT_WITH_QUOT(table_name);
          DATA_PRINTF(".");
          PRINT_IDENT_WITH_QUOT(col_name);
        } else {
          // oracle allow derived table without alias name, table_name is empty here.
          // e.g.:  select * from (select 1 from dual)
          PRINT_IDENT_WITH_QUOT(col_name);
        }
      }
    }
  }
  return ret;
}

int ObRawExprPrinter::print(ObOpRawExpr *expr)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf_) || OB_ISNULL(pos_) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_ is NULL of buf_ is NULL or pos_ is NULL or expr is NULL", K(ret));
  } else {
    ObString symbol("");
    ObItemType type = expr->get_expr_type();
    switch (type) {
    case T_OP_PRIOR :
      SET_SYMBOL_IF_EMPTY("prior");
    case T_OP_CONNECT_BY_ROOT :
      SET_SYMBOL_IF_EMPTY("connect_by_root");
    case T_OP_NOT_EXISTS:
      SET_SYMBOL_IF_EMPTY("not exists");
    case T_OP_BIT_NEG:
      SET_SYMBOL_IF_EMPTY("~");
    case T_OP_EXISTS:
      SET_SYMBOL_IF_EMPTY("exists");
    case T_OP_NOT: {
      if (lib::is_mysql_mode()) {
        SET_SYMBOL_IF_EMPTY("(not");
      } else {
        SET_SYMBOL_IF_EMPTY("not");
      }
      if (1 != expr->get_param_count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr param count should be equal 1 ", K(ret), K(expr->get_param_count()));
      } else {
        DATA_PRINTF("%.*s", LEN_AND_PTR(symbol));
        DATA_PRINTF("(");
        PRINT_EXPR(expr->get_param_expr(0));
        DATA_PRINTF(")");
      }
      if (type == T_OP_NOT && lib::is_mysql_mode()) {
        DATA_PRINTF(")");
      }
      break;
    }
    case T_OP_NEG: {
      if (1 != expr->get_param_count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr param count should be equal 1 ", K(ret), K(expr->get_param_count()));
      } else {
        DATA_PRINTF("-");
        DATA_PRINTF("(");
        PRINT_EXPR(expr->get_param_expr(0));
        DATA_PRINTF(")");
      }
      break;
    }
    case T_OP_AND:
      SET_SYMBOL_IF_EMPTY("and");
    case T_OP_OR: {
      SET_SYMBOL_IF_EMPTY("or");
      // 这里孩子不一定为2, 比如a or (b or c) 会被改写为一个or含三个孩子
      if (expr->get_param_count() < 2) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr param count should be greater than or equal 2", K(ret), K(expr->get_param_count()));
      } else {
        DATA_PRINTF("(");
        for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
          PRINT_EXPR(expr->get_param_expr(i));
          DATA_PRINTF(" %.*s ", LEN_AND_PTR(symbol));
        }
        if (OB_SUCC(ret)) {
          *pos_ -= 2 + symbol.length();
          DATA_PRINTF(")");
        }
      }
      break;
    }
    case T_OP_ADD: // a op b
    case T_OP_AGG_ADD:
      SET_SYMBOL_IF_EMPTY("+");
    case T_OP_MINUS:
    case T_OP_AGG_MINUS:
      SET_SYMBOL_IF_EMPTY("-");
    case T_OP_MUL:
    case T_OP_AGG_MUL:
      SET_SYMBOL_IF_EMPTY("*");
    case T_OP_DIV:
    case T_OP_AGG_DIV:
      SET_SYMBOL_IF_EMPTY("/");
    case T_OP_POW:
      SET_SYMBOL_IF_EMPTY("pow");
    case T_OP_MOD:
      if (lib::is_oracle_mode()) {
        SET_SYMBOL_IF_EMPTY("mod");
      } else {
        SET_SYMBOL_IF_EMPTY("%");
      }
    case T_OP_INT_DIV:
      SET_SYMBOL_IF_EMPTY("div");
    case T_OP_LE:
    case T_OP_SQ_LE:               // subquery
      SET_SYMBOL_IF_EMPTY("<=");
    case T_OP_LT:
    case T_OP_SQ_LT:
      SET_SYMBOL_IF_EMPTY("<");
    case T_OP_EQ:
    case T_OP_SQ_EQ:
      SET_SYMBOL_IF_EMPTY("=");
    case T_OP_NSEQ:
    case T_OP_SQ_NSEQ:
      SET_SYMBOL_IF_EMPTY("<=>");
    case T_OP_GE:
    case T_OP_SQ_GE:
      SET_SYMBOL_IF_EMPTY(">=");
    case T_OP_GT:
    case T_OP_SQ_GT:
      SET_SYMBOL_IF_EMPTY(">");
    case T_OP_BIT_LEFT_SHIFT:
      SET_SYMBOL_IF_EMPTY("<<");
    case T_OP_BIT_RIGHT_SHIFT:
      SET_SYMBOL_IF_EMPTY(">>");
    case T_OP_NE:
    case T_OP_SQ_NE:
      SET_SYMBOL_IF_EMPTY("<>");
    case T_OP_IN: // in sub-query will be rewrited as expr = ANY(sub-query)
      SET_SYMBOL_IF_EMPTY("in");
    case T_OP_NOT_IN:
      SET_SYMBOL_IF_EMPTY("not in"); // not in sub-query will be rewrited as expr != all(sub-query)
    case T_OP_BIT_OR:
      SET_SYMBOL_IF_EMPTY("|");
    case T_OP_BIT_XOR:
      SET_SYMBOL_IF_EMPTY("^");
    case T_OP_XOR:
      if (lib::is_mysql_mode()) {
        SET_SYMBOL_IF_EMPTY("xor");
      } else {
        SET_SYMBOL_IF_EMPTY("^");
      }
    case T_OP_BIT_AND:
      SET_SYMBOL_IF_EMPTY("&");
    case T_OP_REGEXP:
      SET_SYMBOL_IF_EMPTY("regexp");
    case T_OP_CNN: {
      SET_SYMBOL_IF_EMPTY("||");
      //case T_OP_DATE_ADD: {
      if (OB_UNLIKELY(2 != expr->get_param_count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr param count should be equal 2", "count", expr->get_param_count(), K(ret));
      } else {
        if (T_OP_MOD == type && lib::is_oracle_mode()) {
          DATA_PRINTF(" %.*s(", LEN_AND_PTR(symbol));
          PRINT_EXPR(expr->get_param_expr(0));
          DATA_PRINTF(", ");
          PRINT_EXPR(expr->get_param_expr(1));
          DATA_PRINTF(")");
        } else {
          DATA_PRINTF("(");
          PRINT_EXPR(expr->get_param_expr(0));
          DATA_PRINTF(" %.*s ", LEN_AND_PTR(symbol));
          if (OB_SUCC(ret)) {
            // any, all
            if (T_WITH_ANY == expr->get_subquery_key()) {
              DATA_PRINTF("any ");
            } else if (T_WITH_ALL == expr->get_subquery_key()) {
              DATA_PRINTF("all ");
            }
          }
          PRINT_EXPR(expr->get_param_expr(1));
          DATA_PRINTF(")");
        }
      }
      break;
    }
      // is bool, is not bool, is null, is not null, isnull()
    case T_OP_IS:
      SET_SYMBOL_IF_EMPTY("is");
    case T_OP_IS_NOT: {
      SET_SYMBOL_IF_EMPTY("is not");
      if (2 != expr->get_param_count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr param count should be equal 2", K(ret), K(expr->get_param_count()));
      } else {
        DATA_PRINTF("(");
        PRINT_EXPR(expr->get_param_expr(0));
        DATA_PRINTF(" %.*s ", LEN_AND_PTR(symbol));
        if (OB_SUCC(ret)) {
          ObConstRawExpr *con_expr = static_cast<ObConstRawExpr*>(expr->get_param_expr(1));
          if (OB_ISNULL(con_expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("con_expr should not be NULL", K(ret));
          } else {
            ObObj &obj = con_expr->get_value();
            ObObjType type = obj.get_type();
            if (ObNullType == type) {
              DATA_PRINTF("null");
            } else if (ObTinyIntType == type) {
              DATA_PRINTF("%s", obj.get_bool() ? "true" : "false");
            } else if (ObDoubleType == type) {
              if (isnan(obj.get_double())) {
                DATA_PRINTF("nan");
              } else {
                DATA_PRINTF("infinite");
              }
            } else {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected obj type", K(ret), K(type));
            }
          }
        }
        DATA_PRINTF(")");
      }
      break;
    }
      // between, not between
    case T_OP_BTW:
      SET_SYMBOL_IF_EMPTY("between");
    case T_OP_NOT_BTW: {
      SET_SYMBOL_IF_EMPTY("not between");
      if (3 != expr->get_param_count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr param count should be equal 3", K(ret), K(expr->get_param_count()));
      } else {
        DATA_PRINTF("(");
        PRINT_EXPR(expr->get_param_expr(0));
        DATA_PRINTF(" %.*s ", LEN_AND_PTR(symbol));
        PRINT_EXPR(expr->get_param_expr(1));
        DATA_PRINTF(" and ");
        PRINT_EXPR(expr->get_param_expr(2));
        DATA_PRINTF(")");
      }
      break;
    }
      // expr list
    case T_OP_ROW: {
      if (expr->get_param_count() < 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("param count should be greater than or equal 1", K(ret), K(expr->get_param_count()));
      } else {
        DATA_PRINTF("(");
        for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
          PRINT_EXPR(expr->get_param_expr(i));
          DATA_PRINTF(",");
        }
        if (OB_SUCC(ret)) {
          --*pos_;
          DATA_PRINTF(")");
        }
      }
      break;
    }
    case T_OP_LIKE:
      SET_SYMBOL_IF_EMPTY("like");
    case T_OP_NOT_LIKE: {
      SET_SYMBOL_IF_EMPTY("not like");
      if (3 != expr->get_param_count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("param count should be equal 3", K(ret), K(expr->get_param_count()));
      } else {
        DATA_PRINTF("(");
        PRINT_EXPR(expr->get_param_expr(0));
        DATA_PRINTF(" %.*s ", LEN_AND_PTR(symbol));
        PRINT_EXPR(expr->get_param_expr(1));
        DATA_PRINTF(" escape ");
        PRINT_EXPR(expr->get_param_expr(2));
        DATA_PRINTF(")");
      }
      break;
    }
    case T_OBJ_ACCESS_REF: {
      ObObjAccessRawExpr *obj_access_expr = static_cast<ObObjAccessRawExpr*>(expr);
      bool parent_is_table = false;
      ObIArray<pl::ObObjAccessIdx> &access_idxs = obj_access_expr->get_orig_access_idxs();
      int64_t start = access_idxs.count() - 1;
      for (;start > 0; --start) {
        if (OB_NOT_NULL(access_idxs.at(start).get_sysfunc_)) {
          break;
        }
      }
      for (int64_t i = start; OB_SUCC(ret) && i < access_idxs.count(); ++i) {
        pl::ObObjAccessIdx &current_idx = access_idxs.at(i);
        if (parent_is_table) {
          DATA_PRINTF("(");
        } else if (i > start) {
          DATA_PRINTF(".");
        }
        if (OB_NOT_NULL(current_idx.get_sysfunc_)) {
          PRINT_EXPR(current_idx.get_sysfunc_);
        } else if (!current_idx.var_name_.empty()) {
          DATA_PRINTF("%.*s", current_idx.var_name_.length(), current_idx.var_name_.ptr());
        } else {
          DATA_PRINTF("%ld", current_idx.var_index_);
        }
        if (parent_is_table) {
          DATA_PRINTF(")");
        }
        parent_is_table = current_idx.elem_type_.is_nested_table_type();
      }
      break;
    }
    case T_FUN_PL_ASSOCIATIVE_INDEX: {
      ObPLAssocIndexRawExpr *assoc_index_expr = static_cast<ObPLAssocIndexRawExpr*>(expr);
      PRINT_EXPR(assoc_index_expr->get_param_expr(1));
      break;
    }
    case T_FUN_PL_INTEGER_CHECKER: {
      ObPLIntegerCheckerRawExpr *checker = static_cast<ObPLIntegerCheckerRawExpr*>(expr);
      PRINT_EXPR(checker->get_param_expr(0));
      break;
    }
    case T_OP_ASSIGN: {
      SET_SYMBOL_IF_EMPTY(":=");
      if (OB_UNLIKELY(2 != expr->get_param_count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("param count should be equal 2", K(ret), K(expr->get_param_count()));
      } else {
        DATA_PRINTF("(");
        if (OB_ISNULL(expr->get_param_expr(0)) || !expr->get_param_expr(0)->is_const_raw_expr()
            || !static_cast<ObConstRawExpr*>(expr->get_param_expr(0))->get_value().is_varchar()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid user variable name", K(ret), K(expr->get_param_expr(0)));
        } else {
          ObString func_name = static_cast<ObConstRawExpr*>(expr->get_param_expr(0))
                                                            ->get_value().get_varchar();
          DATA_PRINTF("@");
          DATA_PRINTF("%.*s", LEN_AND_PTR(func_name));
          DATA_PRINTF("%.*s", LEN_AND_PTR(symbol));
          PRINT_EXPR(expr->get_param_expr(1));
        }
        DATA_PRINTF(")");
      }
      break;
    }
    case T_OP_MULTISET: {
      SET_SYMBOL_IF_EMPTY("MULTISET");
      break;
    }
    case T_OP_BOOL:{
      CK(1 == expr->get_param_count());
      if (print_params_.for_dblink_ && lib::is_mysql_mode()) {
        DATA_PRINTF("!!(");
        PRINT_EXPR(expr->get_param_expr(0));
        DATA_PRINTF(")");
      } else if (expr->has_flag(IS_INNER_ADDED_EXPR)) {
        // ignore print inner added expr
        PRINT_EXPR(expr->get_param_expr(0));
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("bool expr have to be inner expr for now", K(ret), K(*expr));
      }
      break;
    }
    case T_FUN_SYS_REMOVE_CONST: {
      if (expr->has_flag(IS_INNER_ADDED_EXPR)) {
        // ignore print inner added expr
        CK(1 == expr->get_param_count());
        PRINT_EXPR(expr->get_param_expr(0));
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("bool expr have to be inner expr for now", K(ret), K(*expr));
      }
      break;
    }
    case T_FUN_SYS_WRAPPER_INNER: {
      if (expr->has_flag(IS_INNER_ADDED_EXPR)) {
        // ignore print inner added expr
        CK(expr->get_param_count() == 1);
        if (OB_SUCC(ret)) {
          PRINT_EXPR(expr->get_param_expr(0));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("wrapper expr have to be inner expr for now", K(ret), K(*expr));
      }
      break;
    }
    case T_OP_COLL_PRED: {
      SET_SYMBOL_IF_EMPTY("collection predicate");
      break;
    }
    case T_FUN_PL_GET_CURSOR_ATTR: {
      ObPLGetCursorAttrRawExpr *cursor_attr_expr = static_cast<ObPLGetCursorAttrRawExpr*>(expr);
      if (cursor_attr_expr->get_pl_get_cursor_attr_info().is_rowid()) {
        DATA_PRINTF("%s", "%ROWID");
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("only rowid for cursor is supported",
                 K(ret), "type", get_type_name(type),
                 K(cursor_attr_expr->get_pl_get_cursor_attr_info()));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "only rowid for cursor is supported");
      }
      break;
    }
    case T_FUN_SYS_POINT: {
      SET_SYMBOL_IF_EMPTY("point");
      break;
    }
    case T_FUN_SYS_LINESTRING: {
      SET_SYMBOL_IF_EMPTY("linestring");
      break;
    }
    case T_FUN_SYS_MULTIPOINT: {
      SET_SYMBOL_IF_EMPTY("multipoint");
      break;
    }
    case T_FUN_SYS_MULTILINESTRING: {
      SET_SYMBOL_IF_EMPTY("multilinestring");
      break;
    }
    case T_FUN_SYS_POLYGON: {
      SET_SYMBOL_IF_EMPTY("polygon");
      break;
    }
    case T_FUN_SYS_MULTIPOLYGON: {
      SET_SYMBOL_IF_EMPTY("multipolygon");
      break;
    }
    case T_FUN_SYS_GEOMCOLLECTION: {
      SET_SYMBOL_IF_EMPTY("geomcollection");
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unknown expr type", K(ret), "type", get_type_name(type));
      break;
    }
    } // end switch
  }

  return ret;
}

int ObRawExprPrinter::print(ObSetOpRawExpr *expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is NULL", K(ret));
  } else {
    switch (expr->get_expr_type()) {
      case T_OP_UNION:
        DATA_PRINTF("union");
        break;
      case T_OP_INTERSECT:
        DATA_PRINTF("intersect");
        break;
      case T_OP_EXCEPT:
        if (lib::is_oracle_mode()) {
          DATA_PRINTF("minus");
        } else {
          DATA_PRINTF("except");
        }
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unknown expr type", K(ret), K(expr->get_expr_type()));
        break;
    }
    DATA_PRINTF("[%ld]", expr->get_idx() + 1);
  }
  return ret;
}

int ObRawExprPrinter::print(ObCaseOpRawExpr *expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf_) || OB_ISNULL(pos_) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_ is NULL of buf_ is NULL or pos_ is NULL or expr is NULL", K(ret));
  } else {
    DATA_PRINTF("(case");
    if (OB_SUCC(ret)) {
      ObRawExpr *arg_expr = expr->get_arg_param_expr();
      if (NULL != arg_expr) {
        DATA_PRINTF(" ");
        PRINT_EXPR(arg_expr);
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_when_expr_size(); ++i) {
        DATA_PRINTF(" when ");
        PRINT_EXPR(expr->get_when_param_expr(i));
        DATA_PRINTF(" then ");
        PRINT_EXPR(expr->get_then_param_expr(i));
      }
      DATA_PRINTF(" else");
      if (OB_SUCC(ret)) {
        ObRawExpr *default_expr = expr->get_default_param_expr();
        if (NULL != default_expr) {
          DATA_PRINTF(" ");
          PRINT_EXPR(default_expr);
        }
      }
      DATA_PRINTF(" end)");
    }
  }

  return ret;
}

int ObRawExprPrinter::print_ora_json_arrayagg(ObAggFunRawExpr *expr)
{
  int ret = OB_SUCCESS;
  if (expr->get_real_param_count() != 5) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get unexpected error", K(ret), K(expr->get_param_count()),
                                      K(expr->get_real_param_count()), K(expr));
  } else {
    DATA_PRINTF("json_arrayagg(");
    PRINT_EXPR(expr->get_param_expr(0));
    // format json
    if (OB_SUCC(ret)
        && static_cast<ObConstRawExpr *>(expr->get_param_expr(1))->get_value().get_int() == 1) {
      DATA_PRINTF(" format json");
    }
    // order by
    if (OB_SUCC(ret)) {
      const ObIArray<OrderItem> &order_items = expr->get_order_items();
      int64_t order_item_size = order_items.count();
      if (order_item_size > 0) {
        DATA_PRINTF(" order by ");
        for (int64_t i = 0; OB_SUCC(ret) && i < order_item_size; ++i) {
          const OrderItem &order_item = order_items.at(i);
          PRINT_EXPR(order_item.expr_);
          if (OB_SUCC(ret)) {
            if (lib::is_mysql_mode()) {
              if (is_descending_direction(order_item.order_type_)) {
                DATA_PRINTF(" desc ");
              }
            } else if (order_item.order_type_ == NULLS_FIRST_ASC) {
              DATA_PRINTF(" asc nulls first ");
            } else if (order_item.order_type_ == NULLS_LAST_ASC) {//use default value
              /*do nothing*/
            } else if (order_item.order_type_ == NULLS_FIRST_DESC) {//use default value
              DATA_PRINTF(" desc ");
            } else if (order_item.order_type_ == NULLS_LAST_DESC) {
              DATA_PRINTF(" desc nulls last ");
            } else {/*do nothing*/}
          }
          DATA_PRINTF(",");
        }
        if (OB_SUCC(ret)) {
          --*pos_;
        }
      }
    }
    // on null
    if (OB_SUCC(ret)
        && static_cast<ObConstRawExpr *>(expr->get_param_expr(2))->get_value().get_int() == 2) {
      DATA_PRINTF(" null on null");
    }
    // returning
    if (OB_SUCC(ret)) {
      if (OB_FAIL(print_json_return_type(expr->get_param_expr(3)))) {
        LOG_WARN("fail to print cast_type", K(ret));
      }
    }
    // strict
    if (OB_SUCC(ret)
        && static_cast<ObConstRawExpr *>(expr->get_param_expr(4))->get_value().get_int() == 1) {
      DATA_PRINTF(" strict");
    }
    DATA_PRINTF(")");
  }
  return ret;
}

int ObRawExprPrinter::print(ObAggFunRawExpr *expr)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf_) || OB_ISNULL(pos_) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_ is NULL of buf_ is NULL or pos_ is NULL or expr is NULL", K(ret));
  } else {
    ObString database_name("");
    ObString package_name("");
    ObString symbol("");
    ObItemType type = expr->get_expr_type();
    switch (type) {
    case T_FUN_COUNT:
      SET_SYMBOL_IF_EMPTY("count");
    case T_FUN_MAX:
      SET_SYMBOL_IF_EMPTY("max");
    case T_FUN_MIN:
      SET_SYMBOL_IF_EMPTY("min");
    case T_FUN_SUM:
      SET_SYMBOL_IF_EMPTY("sum");
    case T_FUN_MEDIAN:
      SET_SYMBOL_IF_EMPTY("median");
    case T_FUN_APPROX_COUNT_DISTINCT:
      SET_SYMBOL_IF_EMPTY("approx_count_distinct");
    case T_FUN_GROUPING:
      SET_SYMBOL_IF_EMPTY("grouping");
    case T_FUN_GROUPING_ID:
      SET_SYMBOL_IF_EMPTY("grouping_id");
    case T_FUN_GROUP_ID:
      SET_SYMBOL_IF_EMPTY("group_id");
    case T_FUN_VARIANCE:
      SET_SYMBOL_IF_EMPTY("variance");
    case T_FUN_STDDEV:
      SET_SYMBOL_IF_EMPTY("stddev");
    case T_FUN_CORR:
      SET_SYMBOL_IF_EMPTY("corr");
    case T_FUN_COVAR_POP:
      SET_SYMBOL_IF_EMPTY("covar_pop");
    case T_FUN_COVAR_SAMP:
      SET_SYMBOL_IF_EMPTY("covar_samp");
    case T_FUN_VAR_POP:
      SET_SYMBOL_IF_EMPTY("var_pop");
    case T_FUN_VAR_SAMP:
      SET_SYMBOL_IF_EMPTY("var_samp");
    case T_FUN_REGR_SLOPE:
      SET_SYMBOL_IF_EMPTY("regr_slope");
    case T_FUN_REGR_INTERCEPT:
      SET_SYMBOL_IF_EMPTY("regr_intercept");
    case T_FUN_REGR_COUNT:
      SET_SYMBOL_IF_EMPTY("regr_count");
    case T_FUN_REGR_R2:
      SET_SYMBOL_IF_EMPTY("regr_r2");
    case T_FUN_REGR_AVGX:
      SET_SYMBOL_IF_EMPTY("regr_avgx");
    case T_FUN_REGR_AVGY:
      SET_SYMBOL_IF_EMPTY("regr_avgy");
    case T_FUN_REGR_SXX:
      SET_SYMBOL_IF_EMPTY("regr_sxx");
    case T_FUN_REGR_SYY:
      SET_SYMBOL_IF_EMPTY("regr_syy");
    case T_FUN_REGR_SXY:
      SET_SYMBOL_IF_EMPTY("regr_sxy");
    case T_FUN_AVG:
      SET_SYMBOL_IF_EMPTY("avg");
    case T_FUN_STDDEV_POP:
      SET_SYMBOL_IF_EMPTY("stddev_pop");
    case T_FUN_STDDEV_SAMP:
      SET_SYMBOL_IF_EMPTY("stddev_samp");
    case T_FUN_WM_CONCAT:
      SET_SYMBOL_IF_EMPTY("wm_concat");
    case T_FUN_TOP_FRE_HIST:
      SET_SYMBOL_IF_EMPTY("top_k_fre_hist");
    case T_FUN_HYBRID_HIST:
      SET_SYMBOL_IF_EMPTY("hybrid_hist");
    case T_FUN_SYS_BIT_AND:
      SET_SYMBOL_IF_EMPTY("bit_and");
    case T_FUN_SYS_BIT_OR:
      SET_SYMBOL_IF_EMPTY("bit_or");
    case T_FUN_SYS_BIT_XOR:
      SET_SYMBOL_IF_EMPTY("bit_xor");
    case T_FUN_JSON_ARRAYAGG:
      SET_SYMBOL_IF_EMPTY("json_arrayagg");
    case T_FUN_JSON_OBJECTAGG:
      SET_SYMBOL_IF_EMPTY("json_objectagg");
    case T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS:
      SET_SYMBOL_IF_EMPTY("approx_count_distinct_synopsis");
    case T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS_MERGE:
      SET_SYMBOL_IF_EMPTY("approx_count_distinct_synopsis_merge");
    case T_FUN_PL_AGG_UDF:{
      if (type == T_FUN_PL_AGG_UDF) {
        if (OB_ISNULL(expr->get_pl_agg_udf_expr()) ||
            OB_UNLIKELY(!expr->get_pl_agg_udf_expr()->is_udf_expr())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else {
          database_name = static_cast<ObUDFRawExpr*>(expr->get_pl_agg_udf_expr())->get_database_name();
          package_name = static_cast<ObUDFRawExpr*>(expr->get_pl_agg_udf_expr())->get_package_name();
          symbol = static_cast<ObUDFRawExpr*>(expr->get_pl_agg_udf_expr())->get_func_name();
        }
      }
      if (OB_SUCC(ret)) {
        if (!database_name.empty()) {
          DATA_PRINTF("%.*s.", LEN_AND_PTR(database_name));
        }
        if (!package_name.empty()) {
          DATA_PRINTF("%.*s.", LEN_AND_PTR(package_name));
        }
        DATA_PRINTF("%.*s(", LEN_AND_PTR(symbol));
      }
      // distinct, default 'all', not print
      if (OB_SUCC(ret)) {
        if (expr->is_param_distinct()) {
          DATA_PRINTF("distinct ");
        }
      }
      if (OB_SUCC(ret)) {
        if (0 == expr->get_param_count()) {
          if (T_FUN_COUNT == type) {
            // count(*) -> count(0)
            // but do not transform group_id() to group_id(0);
            DATA_PRINTF("0");
          } else if (T_FUN_GROUP_ID == type) {
            // do nothing.
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("expr type should be T_FUN_COUNT or group_id", K(ret), K(type));
          }
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_real_param_count(); ++i) {
            PRINT_EXPR(expr->get_param_expr(i));
            DATA_PRINTF(",");
          }
          if (OB_SUCC(ret)) {
            --*pos_;
          }
        }
      }

      // on null
      DATA_PRINTF(")");
      break;
    }
    case T_FUN_ORA_JSON_ARRAYAGG: {
      if (OB_FAIL(print_ora_json_arrayagg(expr))) {
        LOG_WARN("fail to print oracle json_arrayagg.", K(ret));
      }
      break;
    }
    case T_FUN_ORA_XMLAGG: {
      if (OB_FAIL(print_xml_agg_expr(expr))) {
        LOG_WARN("fail to print oracle xmlagg.", K(ret));
      }
      break;
    }
    case T_FUN_ORA_JSON_OBJECTAGG: {
      if (OB_UNLIKELY(expr->get_param_count() != 7)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected param count of expr to type", K(ret), KPC(expr));
      } else {
        // json_objectagg
        DATA_PRINTF("json_objectagg(");
        if (OB_SUCC(ret) && OB_FAIL(print_ora_json_objectagg(expr))) {
          LOG_WARN("bool expr have to be inner expr for now", K(ret), K(*expr));
        }
      }
      break;
    }
    case T_FUN_GROUP_RANK:
      SET_SYMBOL_IF_EMPTY("rank");
    case T_FUN_GROUP_DENSE_RANK:
      SET_SYMBOL_IF_EMPTY("dense_rank");
    case T_FUN_GROUP_PERCENT_RANK:
      SET_SYMBOL_IF_EMPTY("percent_rank");
    case T_FUN_GROUP_CUME_DIST:
      SET_SYMBOL_IF_EMPTY("cume_dist");
    case T_FUN_GROUP_PERCENTILE_CONT:
      SET_SYMBOL_IF_EMPTY("percentile_cont");
    case T_FUN_GROUP_PERCENTILE_DISC:
      SET_SYMBOL_IF_EMPTY("percentile_disc");
    case T_FUN_GROUP_CONCAT: {
      // mysql: group_concat(distinct c1,c2+1 order by c1 desc separator ',')
      // oracle: listagg(c1,',') within group(order by c1);
      if (lib::is_oracle_mode()) {
        SET_SYMBOL_IF_EMPTY("listagg");
      } else {
        SET_SYMBOL_IF_EMPTY("group_concat");
      }
      DATA_PRINTF("%.*s(", LEN_AND_PTR(symbol));
      // distinct
      if (OB_SUCC(ret)) {
        if (expr->is_param_distinct()) {
          DATA_PRINTF("distinct ");
        }
      }
      // expr list
      for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_real_param_count(); ++i) {
        PRINT_EXPR(expr->get_real_param_exprs().at(i));
        DATA_PRINTF(",");
      }
      if (OB_SUCC(ret)) {
        --*pos_;
      }
      if (lib::is_oracle_mode() && type == T_FUN_GROUP_CONCAT
          && 0 == expr->get_order_items().count()) { // oracle 模式 listagg 支持无 within group
        /* do nothing */
      } else {
        if (lib::is_oracle_mode()) {
          DATA_PRINTF(") within group (");
        }
        // order by
        if (OB_SUCC(ret)) {
          const ObIArray<OrderItem> &order_items = expr->get_order_items();
          int64_t order_item_size = order_items.count();
          if (order_item_size > 0) {
            DATA_PRINTF(" order by ");
            for (int64_t i = 0; OB_SUCC(ret) && i < order_item_size; ++i) {
              const OrderItem &order_item = order_items.at(i);
              PRINT_EXPR(order_item.expr_);
              if (OB_SUCC(ret)) {
                if (lib::is_mysql_mode()) {
                  if (is_descending_direction(order_item.order_type_)) {
                    DATA_PRINTF(" desc ");
                  }
                } else if (order_item.order_type_ == NULLS_FIRST_ASC) {
                  DATA_PRINTF(" asc nulls first ");
                } else if (order_item.order_type_ == NULLS_LAST_ASC) {//use default value
                  /*do nothing*/
                } else if (order_item.order_type_ == NULLS_FIRST_DESC) {//use default value
                  DATA_PRINTF(" desc ");
                } else if (order_item.order_type_ == NULLS_LAST_DESC) {
                  DATA_PRINTF(" desc nulls last ");
                } else {/*do nothing*/}
              }
              DATA_PRINTF(",");
            }
            if (OB_SUCC(ret)) {
              --*pos_;
            }
          }
        }
        // separator
        if (OB_SUCC(ret)) {
          if (expr->get_separator_param_expr()) {
            DATA_PRINTF(" separator ");
            PRINT_EXPR(expr->get_separator_param_expr());
          }
        }
      }
      DATA_PRINTF(")");
      break;
    }
    case T_FUN_KEEP_MAX:
      SET_SYMBOL_IF_EMPTY("max");
    case T_FUN_KEEP_MIN:
      SET_SYMBOL_IF_EMPTY("min");
    case T_FUN_KEEP_SUM:
      SET_SYMBOL_IF_EMPTY("sum");
    case T_FUN_KEEP_COUNT:
      SET_SYMBOL_IF_EMPTY("count");
    case T_FUN_KEEP_AVG:
      SET_SYMBOL_IF_EMPTY("avg");
    case T_FUN_KEEP_STDDEV:
      SET_SYMBOL_IF_EMPTY("stddev");
    case T_FUN_KEEP_VARIANCE:
      SET_SYMBOL_IF_EMPTY("variance");
    case T_FUN_KEEP_WM_CONCAT: {
      SET_SYMBOL_IF_EMPTY("wm_concat");
      DATA_PRINTF("%.*s(", LEN_AND_PTR(symbol));
      if (0 == expr->get_real_param_count()) {//count(*) keep(...)
        if (T_FUN_KEEP_COUNT != type) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expr type should be T_FUN_KEEP_COUNT ", K(ret), K(type));
        } else {
          DATA_PRINTF("*");
        }
      } else if (OB_UNLIKELY(T_FUN_KEEP_WM_CONCAT != type && 1 != expr->get_real_param_count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(ret), K(type));
      } else {
        PRINT_EXPR(expr->get_real_param_exprs().at(0));
      }
      if (OB_SUCC(ret)) {
        DATA_PRINTF(")");
        const ObIArray<OrderItem> &order_items = expr->get_order_items();
        if (order_items.count() > 0) {
          DATA_PRINTF("keep(dense_rank first order by ");
          for (int64_t i = 0; OB_SUCC(ret) && i < order_items.count(); ++i) {
            const OrderItem &order_item = order_items.at(i);
            PRINT_EXPR(order_item.expr_);
            if (OB_SUCC(ret)) {
              if (lib::is_mysql_mode()) {
                if (is_descending_direction(order_item.order_type_)) {
                  DATA_PRINTF(" desc ");
                }
              } else if (order_item.order_type_ == NULLS_FIRST_ASC) {
                DATA_PRINTF(" asc nulls first ");
              } else if (order_item.order_type_ == NULLS_LAST_ASC) {//use default value
                /*do nothing*/
              } else if (order_item.order_type_ == NULLS_FIRST_DESC) {//use default value
                DATA_PRINTF(" desc ");
              } else if (order_item.order_type_ == NULLS_LAST_DESC) {
                DATA_PRINTF(" desc nulls last ");
              } else {/*do nothing*/}
            }
            DATA_PRINTF(",");
          }
          if (OB_SUCC(ret)) {
            --*pos_;
            DATA_PRINTF(")");
          }
        }
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unknown expr type", K(ret), K(type));
      break;
    }
    } // end switch
  }

  return ret;
}

int ObRawExprPrinter::print_ora_json_objectagg(ObAggFunRawExpr *expr)
{
  INIT_SUCC(ret);
  int num_para = expr->get_real_param_count();
  if (OB_SUCC(ret)) {
    for (int i = 0; i < num_para - 4; i += 3) {
      PRINT_EXPR(expr->get_param_expr(i));
      DATA_PRINTF(" VALUE ");
      PRINT_EXPR(expr->get_param_expr(i + 1));
      if (!static_cast<ObConstRawExpr*>(expr->get_param_expr(i + 2))->get_value().is_int()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("type value isn't int value");
      } else {
        int64_t type = static_cast<ObConstRawExpr*>(expr->get_param_expr(i + 2))->get_value().get_int();
        switch (type) {
          case 0:
            break;
          case 1:
            DATA_PRINTF(" format json");
            break;
          default:
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid type value.", K(type));
            break;
        }
      }
      if (i != num_para - 7) {
        DATA_PRINTF(",");
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (!static_cast<ObConstRawExpr*>(expr->get_param_expr(3))->get_value().is_int()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("type value isn't int value");
    } else {
      int64_t type = static_cast<ObConstRawExpr*>(expr->get_param_expr(3))->get_value().get_int();
      switch (type) {
        case 2:
          DATA_PRINTF(" null");
          break;
        case 0:
          DATA_PRINTF(" absent");
          break;
        case 1:
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid type value.", K(type));
          break;
      }
      if (OB_SUCC(ret) && type != 1) {
        DATA_PRINTF(" on null");
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(print_json_return_type(expr->get_param_expr(4)))) {
      LOG_WARN("fail to print cast_type", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (!static_cast<ObConstRawExpr*>(expr->get_param_expr(5))->get_value().is_int()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("type value isn't int value");
    } else {
      int64_t type = static_cast<ObConstRawExpr*>(expr->get_param_expr(5))->get_value().get_int();
      switch (type) {
        case 0:
          DATA_PRINTF("");
          break;
        case 1:
          DATA_PRINTF(" strict");
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid type value.", K(type));
          break;
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (!static_cast<ObConstRawExpr*>(expr->get_param_expr(6))->get_value().is_int()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("type value isn't int value");
    } else {
      int64_t type = static_cast<ObConstRawExpr*>(expr->get_param_expr(6))->get_value().get_int();
      switch (type) {
        case 0:
          DATA_PRINTF("");
          break;
        case 1:
          DATA_PRINTF(" with unique keys");
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid type value.", K(type));
          break;
      }
    }
  }
  if (OB_SUCC(ret)) {
    DATA_PRINTF(")");
  }
  return ret;
}

int ObRawExprPrinter::print_json_object(ObSysFunRawExpr *expr)
{
  INIT_SUCC(ret);
  int num_para = expr->get_param_count();
  if (OB_SUCC(ret)) {
    for (int i = 0; i < num_para - 4; i += 3) {
      PRINT_EXPR(expr->get_param_expr(i));
      DATA_PRINTF(" VALUE ");
      PRINT_EXPR(expr->get_param_expr(i + 1));
      if (!static_cast<ObConstRawExpr*>(expr->get_param_expr(i + 2))->get_value().is_int()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("type value isn't int value");
      } else {
        int64_t type = static_cast<ObConstRawExpr*>(expr->get_param_expr(i + 2))->get_value().get_int();
        switch (type) {
          case 0:
            break;
          case 1:
            DATA_PRINTF(" format json");
            break;
          default:
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid type value.", K(type));
            break;
        }
      }
      if (i != num_para - 7 && OB_SUCC(ret)) {
        DATA_PRINTF(",");
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (!static_cast<ObConstRawExpr*>(expr->get_param_expr(num_para - 4))->get_value().is_int()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("type value isn't int value");
    } else {
      int64_t type = static_cast<ObConstRawExpr*>(expr->get_param_expr(num_para - 4))->get_value().get_int();
      switch (type) {
        case 0:
        case 1:
          DATA_PRINTF(" null");
          break;
        case 2:
          DATA_PRINTF(" absent");
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid type value.", K(type));
          break;
      }
      if (OB_SUCC(ret)) {
        DATA_PRINTF(" on null");
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(print_json_return_type(expr->get_param_expr(num_para - 3)))) {
      LOG_WARN("fail to print cast_type", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (!static_cast<ObConstRawExpr*>(expr->get_param_expr(num_para - 2))->get_value().is_int()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("type value isn't int value");
    } else {
      int64_t type = static_cast<ObConstRawExpr*>(expr->get_param_expr(num_para - 2))->get_value().get_int();
      switch (type) {
        case 0:
          DATA_PRINTF("");
          break;
        case 1:
          DATA_PRINTF(" strict");
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid type value.", K(type));
          break;
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (!static_cast<ObConstRawExpr*>(expr->get_param_expr(num_para - 1))->get_value().is_int()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("type value isn't int value");
    } else {
      int64_t type = static_cast<ObConstRawExpr*>(expr->get_param_expr(num_para - 1))->get_value().get_int();
      switch (type) {
        case 0:
          DATA_PRINTF("");
          break;
        case 1:
          DATA_PRINTF(" with unique keys");
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid type value.", K(type));
          break;
      }
    }
  }
  DATA_PRINTF(")");
  return ret;
}

int ObRawExprPrinter::print_json_return_type(ObRawExpr *expr)
{
  INIT_SUCC(ret);
  const int32_t DEFAULT_VARCHAR_LEN = 4000;
  ObScale scale = static_cast<ObConstRawExpr *>(expr)->get_accuracy().get_scale();
  if (OB_ISNULL(buf_) || OB_ISNULL(pos_) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_ is NULL of buf_ is NULL or pos_ is NULL or expr is NULL", K(ret));
  } else if (scale == 1) { // scale == 1 is default returning
  } else if (ObRawExpr::EXPR_CONST != expr->get_expr_class()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr class should be EXPR_CONST ", K(ret), K(expr->get_expr_class()));
  } else {
    ObConstRawExpr *con_expr = static_cast<ObConstRawExpr*>(expr);
    const ObLengthSemantics length_semantics = con_expr->get_accuracy().get_length_semantics();
    const ObScale scale = con_expr->get_accuracy().get_scale();
    ParseNode parse_node;
    if (OB_FAIL(con_expr->get_value().get_int(parse_node.value_))) {
      LOG_WARN("get int value failed", K(ret));
    } else if (parse_node.value_ == 0) {
    } else {
      int16_t cast_type = parse_node.int16_values_[OB_NODE_CAST_TYPE_IDX];

      switch (static_cast<ObItemType>(cast_type)) {
        case T_LONGTEXT: {
          int16_t collation = parse_node.int16_values_[OB_NODE_CAST_COLL_IDX];
          DATA_PRINTF(" RETURNING");
          if (BINARY_COLLATION == collation) {
            DATA_PRINTF(" BLOB");
          } else {
            DATA_PRINTF(" CLOB");
          }
          break;
        }
        case T_VARCHAR: {
          int32_t len = parse_node.int32_values_[OB_NODE_CAST_C_LEN_IDX];
          if ((length_semantics == LS_BYTE && len == -1) || len == DEFAULT_VARCHAR_LEN) { // special case :returning type is varchar2 without len (VARCHAR2)
            DATA_PRINTF(" RETURNING");
            DATA_PRINTF(" VARCHAR2");
            break;
          }
          // varchar2 with len should use normal function VARCHAR2(100)
        }
        default: {
          DATA_PRINTF(" RETURNING ");
          if (OB_FAIL(print_cast_type(expr))) {
            LOG_WARN("fail to print cast_type", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObRawExprPrinter::print_json_mergepatch(ObSysFunRawExpr *expr)
{
  INIT_SUCC(ret);
  DATA_PRINTF("JSON_MERGEPATCH(");

  PRINT_EXPR(expr->get_param_expr(0));
  DATA_PRINTF(" ,");
  PRINT_EXPR(expr->get_param_expr(1));
  if (OB_SUCC(ret) && OB_FAIL(print_json_return_type(expr->get_param_expr(2)))) {
    LOG_WARN("fail to print cast_type", K(ret));
  }

  if (OB_SUCC(ret)) {
    if (!static_cast<ObConstRawExpr*>(expr->get_param_expr(3))->get_value().is_int()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("type value isn't int value");
    } else {
      int64_t type = static_cast<ObConstRawExpr*>(expr->get_param_expr(3))->get_value().get_int();
      switch (type) {
        case 0:
          break;
        case 1:
          DATA_PRINTF(" PRETTY");
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid type value.", K(type));
          break;
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (!static_cast<ObConstRawExpr*>(expr->get_param_expr(4))->get_value().is_int()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("type value isn't int value");
    } else {
      int64_t type = static_cast<ObConstRawExpr*>(expr->get_param_expr(4))->get_value().get_int();
      switch (type) {
        case 0:
          break;
        case 1:
          DATA_PRINTF(" ASCII");
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid type value.", K(type));
          break;
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (!static_cast<ObConstRawExpr*>(expr->get_param_expr(5))->get_value().is_int()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("type value isn't int value");
    } else {
      int64_t type = static_cast<ObConstRawExpr*>(expr->get_param_expr(5))->get_value().get_int();
      switch (type) {
        case 0:
          break;
        case 1:
          DATA_PRINTF(" TRUNCATE");
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid type value.", K(type));
          break;
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (!static_cast<ObConstRawExpr*>(expr->get_param_expr(6))->get_value().is_int()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("type value isn't int value");
    } else {
      int64_t type = static_cast<ObConstRawExpr*>(expr->get_param_expr(6))->get_value().get_int();
      switch (type) {
        case 0:
          DATA_PRINTF(" ERROR ON ERROR");
          break;
        case 1:
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid type value.", K(type));
          break;
      }
    }
  }

  DATA_PRINTF(")");
  return ret;
}

int ObRawExprPrinter::print_json_array(ObSysFunRawExpr *expr)
{
  INIT_SUCC(ret);
  DATA_PRINTF("json_array(");
  size_t param_count = expr->get_param_count();
  size_t i = 0;
  for (; i < param_count - 3 && OB_SUCC(ret); i += 2) {
    PRINT_EXPR(expr->get_param_expr(i));
    if (!static_cast<ObConstRawExpr*>(expr->get_param_expr(i + 1))->get_value().is_int()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("type value isn't int value");
    } else {
      int64_t type = static_cast<ObConstRawExpr*>(expr->get_param_expr(i + 1))->get_value().get_int();
      switch (type) {
        case 0:
          break;
        case 1:
          DATA_PRINTF(" FORMAT JSON");
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid type value.", K(type));
          break;
      }
      if (i + 1 == param_count - 4) {
        DATA_PRINTF(" ");
      } else {
        DATA_PRINTF(", ");
      }
    }
  }

  if (OB_SUCC(ret)) {
    i = param_count - 3;
    if (!static_cast<ObConstRawExpr*>(expr->get_param_expr(i))->get_value().is_int()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("type value isn't int value");
    } else {
      int64_t type = static_cast<ObConstRawExpr*>(expr->get_param_expr(i))->get_value().get_int();
      switch (type) {
        case 0:
          DATA_PRINTF(" null on null");
          break;
        case 1:
          DATA_PRINTF(" absent on null");
          break;
        case 3:
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid type value.", K(type));
          break;
      }
    }
  }
  if (OB_SUCC(ret)) {
    ++i;
    if (OB_FAIL(print_json_return_type(expr->get_param_expr(i)))) {
      LOG_WARN("fail to print cast_type", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    ++i;
    if (!static_cast<ObConstRawExpr*>(expr->get_param_expr(i))->get_value().is_int()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("type value isn't int value");
    } else {
      int64_t type = static_cast<ObConstRawExpr*>(expr->get_param_expr(i))->get_value().get_int();
      switch (type) {
        case 0:
          DATA_PRINTF(" ");
          break;
        case 1:
          DATA_PRINTF(" strict");
          break;
        case 3:
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid type value.", K(type));
          break;
      }
    }
  }
  DATA_PRINTF(")");
  return ret;
}

int ObRawExprPrinter::print_json_value(ObSysFunRawExpr *expr)
{
  INIT_SUCC(ret);
  if (OB_SUCC(ret)) {
    if (OB_FAIL(print_json_return_type(expr->get_param_expr(2)))) {
      LOG_WARN("fail to print cast_type", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (!static_cast<ObConstRawExpr*>(expr->get_param_expr(3))->get_value().is_int()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("truncate value isn't int value");
    } else {
      int64_t type = static_cast<ObConstRawExpr*>(expr->get_param_expr(3))->get_value().get_int();
      switch (type) {
        case 0:
          break;
        case 1:
          DATA_PRINTF(" TRUNCATE");
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid type value.", K(type));
          break;
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (!static_cast<ObConstRawExpr*>(expr->get_param_expr(4))->get_value().is_int()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ascii value isn't int value");
    } else {
      int64_t type = static_cast<ObConstRawExpr*>(expr->get_param_expr(4))->get_value().get_int();
      switch (type) {
        case 0:
          DATA_PRINTF("");
          break;
        case 1:
          DATA_PRINTF(" ASCII");
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid type value.", K(type));
          break;
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (!static_cast<ObConstRawExpr*>(expr->get_param_expr(5))->get_value().is_int()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("type value isn't int value");
    } else {
      int64_t type = static_cast<ObConstRawExpr*>(expr->get_param_expr(5))->get_value().get_int();
      switch (type) {
        case 0:
          DATA_PRINTF(" error");
          break;
        case 1:
        case 3:
          if (lib::is_mysql_mode() || type == 1) {
            DATA_PRINTF(" null");
          }
          break;
        case 2:
          DATA_PRINTF(" default ");
          PRINT_EXPR(expr->get_param_expr(6));
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid type value.", K(type));
          break;
      }
      if (OB_SUCC(ret) && (lib::is_mysql_mode() || type < 3)) {
        DATA_PRINTF(" on empty");
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (!static_cast<ObConstRawExpr*>(expr->get_param_expr(8))->get_value().is_int()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("type value isn't int value");
    } else {
      int64_t type = static_cast<ObConstRawExpr*>(expr->get_param_expr(8))->get_value().get_int();
      switch (type) {
        case 0:
          DATA_PRINTF(" error");
          break;
        case 1:
        case 3:
          DATA_PRINTF(" null");
          break;
        case 2:
          DATA_PRINTF(" default ");
          PRINT_EXPR(expr->get_param_expr(9));
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid type value.", K(type));
          break;
      }
      if (OB_SUCC(ret)) {
        DATA_PRINTF(" on error");
      }
    }
  }
  if (lib::is_oracle_mode()) {
    if (OB_SUCC(ret)) {
      bool not_first_node = false;
      for (size_t i = 11;  OB_SUCC(ret) && i < expr->get_param_count(); i++) {
        if (!static_cast<ObConstRawExpr*>(expr->get_param_expr(i))->get_value().is_int()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("type value isn't int value");
        } else {
          int64_t type = static_cast<ObConstRawExpr*>(expr->get_param_expr(i))->get_value().get_int();
          switch (type) {
            case 0:
              if (not_first_node) {
                DATA_PRINTF(")");
              }
              DATA_PRINTF(" error");
              not_first_node = false;
              break;
            case 1:
              if (not_first_node) {
                DATA_PRINTF(")");
              }
              DATA_PRINTF(" null");
              not_first_node = false;
              break;
            case 2:
              if (not_first_node) {
                DATA_PRINTF(")");
              }
              DATA_PRINTF(" ignore ");
              not_first_node = false;
              break;
            case 3:
              break;
            case 4:
              if (not_first_node) {
                DATA_PRINTF(", ");
              } else {
                DATA_PRINTF("(");
              }
              DATA_PRINTF("missing data");
              not_first_node = true;
              break;
            case 5:
              if (not_first_node) {
                DATA_PRINTF(", ");
              } else {
                DATA_PRINTF("(");
              }
              DATA_PRINTF("extra data");
              not_first_node = true;
              break;
            case 6:
              if (not_first_node) {
                DATA_PRINTF(", ");
              } else {
                DATA_PRINTF("(");
              }
              DATA_PRINTF("type error");
              not_first_node = true;
              break;
            case 7:
              DATA_PRINTF("");
              not_first_node = true;
              break;
            default:
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("invalid type value.", K(type));
              break;
          }
          if (OB_SUCC(ret) && type < 3) {
            DATA_PRINTF(" on mismatch ");
          }
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    DATA_PRINTF(")");
  }
  return ret;
}

int ObRawExprPrinter::print_json_query(ObSysFunRawExpr *expr)
{
  INIT_SUCC(ret);
  if (OB_SUCC(ret)) {
    if (!static_cast<ObConstRawExpr*>(expr->get_param_expr(3))->get_value().is_int()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("truncate value isn't int value");
    } else {
      int64_t type = static_cast<ObConstRawExpr*>(expr->get_param_expr(3))->get_value().get_int();
      switch (type) {
        case 1:
          DATA_PRINTF(" TRUNCATE");
          break;
        case 0:
          break;
        default:
          break;
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (!static_cast<ObConstRawExpr*>(expr->get_param_expr(4))->get_value().is_int()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("scalar value isn't int value");
    } else {
      int64_t type = static_cast<ObConstRawExpr*>(expr->get_param_expr(4))->get_value().get_int();
      switch (type) {
        case 0:
          DATA_PRINTF(" ALLOW SCALARS");
          break;
        case 1:
          DATA_PRINTF(" DISALLOW SCALARS");
          break;
        case 2:
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid type value.", K(type));
          break;
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (!static_cast<ObConstRawExpr*>(expr->get_param_expr(5))->get_value().is_int()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pretty value isn't int value");
    } else {
      int64_t type = static_cast<ObConstRawExpr*>(expr->get_param_expr(5))->get_value().get_int();
      switch (type) {
        case 0:
          DATA_PRINTF("");
          break;
        case 1:
          DATA_PRINTF(" PRETTY");
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid type value.", K(type));
          break;
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (!static_cast<ObConstRawExpr*>(expr->get_param_expr(6))->get_value().is_int()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ascii value isn't int value");
    } else {
      int64_t type = static_cast<ObConstRawExpr*>(expr->get_param_expr(6))->get_value().get_int();
      switch (type) {
        case 0:
          DATA_PRINTF("");
          break;
        case 1:
          DATA_PRINTF(" ASCII");
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid type value.", K(type));
          break;
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (!static_cast<ObConstRawExpr*>(expr->get_param_expr(7))->get_value().is_int()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("wrapper value isn't int value");
    } else {
      int64_t type = static_cast<ObConstRawExpr*>(expr->get_param_expr(7))->get_value().get_int();
      switch (type) {
        case 0:
          DATA_PRINTF(" WITHOUT WRAPPER");
          break;
        case 1:
          DATA_PRINTF(" WITHOUT ARRAY WRAPPER");
          break;
        case 2:
          DATA_PRINTF(" WITH WRAPPER");
          break;
        case 3:
          DATA_PRINTF(" WITH ARRAY WRAPPER");
          break;
        case 4:
          DATA_PRINTF(" WITH UNCONDITIONAL WRAPPER");
          break;
        case 5:
          DATA_PRINTF(" WITH CONDITIONAL WRAPPER");
          break;
        case 6:
          DATA_PRINTF(" WITH UNCONDITIONAL ARRAY WRAPPER");
          break;
        case 7:
          DATA_PRINTF(" WITH CONDITIONAL ARRAY WRAPPER");
          break;
        case 8:
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid type value.", K(type));
          break;
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (!static_cast<ObConstRawExpr*>(expr->get_param_expr(8))->get_value().is_int()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("type value isn't int value");
    } else {
      int64_t type = static_cast<ObConstRawExpr*>(expr->get_param_expr(8))->get_value().get_int();
      switch (type) {
        case 0:
          DATA_PRINTF(" error on error");
          break;
        case 1:
          DATA_PRINTF(" null on error");
          break;
        case 2:
          DATA_PRINTF(" EMPTY on error");
          break;
        case 3:
          DATA_PRINTF(" EMPTY ARRAY on error");
          break;
        case 4:
          DATA_PRINTF(" EMPTY OBJECT on error");
          break;
        case 5:
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid type value.", K(type));
          break;
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (!static_cast<ObConstRawExpr*>(expr->get_param_expr(9))->get_value().is_int()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("type value isn't int value");
    } else {
      int64_t type = static_cast<ObConstRawExpr*>(expr->get_param_expr(9))->get_value().get_int();
      switch (type) {
        case 0:
          DATA_PRINTF(" error on empty");
          break;
        case 1:
          DATA_PRINTF(" null on empty");
          break;
        case 2:
          DATA_PRINTF(" EMPTY on empty");
          break;
        case 3:
          DATA_PRINTF(" EMPTY ARRAY on empty");
          break;
        case 4:
          DATA_PRINTF(" EMPTY OBJECT on empty");
          break;
        case 5:
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid type value.", K(type));
          break;
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (!static_cast<ObConstRawExpr*>(expr->get_param_expr(10))->get_value().is_int()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("type value isn't int value");
    } else {
      int64_t type = static_cast<ObConstRawExpr*>(expr->get_param_expr(10))->get_value().get_int();
      switch (type) {
        case 0:
          DATA_PRINTF(" error on mismatch");
          break;
        case 1:
        case 2:
          DATA_PRINTF(" null on mismatch");
          break;
        case 3:
          DATA_PRINTF(" dot on mismatch");
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid type value.", K(type));
          break;
      }
    }
  }
  if (OB_SUCC(ret)) {
    DATA_PRINTF(")");
  }
  return ret;
}

int ObRawExprPrinter::print_json_exists(ObSysFunRawExpr *expr)
{
  INIT_SUCC(ret);

  uint32_t param_num = expr->get_param_count();
  if (param_num > 5) {
    uint32_t passing_end = param_num - 3;
    DATA_PRINTF(" PASSING ");
    for (uint32_t i = 2; i < passing_end; i += 2) {
      PRINT_EXPR(expr->get_param_expr(i));
      DATA_PRINTF(" AS \"");
      ObString keyname = static_cast<ObConstRawExpr*>(expr->get_param_expr(i + 1))->get_value().get_string();
      DATA_PRINTF("%.*s", LEN_AND_PTR(keyname));
      DATA_PRINTF("\"");
      if (i + 2 < passing_end) {
        DATA_PRINTF(" , ");
      }
    }
  }

  // on error
  if (OB_SUCC(ret)) {
    if (!static_cast<ObConstRawExpr*>(expr->get_param_expr(param_num - 2))->get_value().is_int()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("type value isn't int value");
    } else {
      int64_t type = static_cast<ObConstRawExpr*>(expr->get_param_expr(param_num - 2))->get_value().get_int();
      switch (type) {
        case 0:
          DATA_PRINTF(" false on error");
          break;
        case 1:
          DATA_PRINTF(" true on error");
          break;
        case 2:
          DATA_PRINTF(" error on error");
          break;
        case 3:
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid type value.", K(type));
          break;
      }
    }
  }

  // on empty
  if (OB_SUCC(ret)) {
    if (!static_cast<ObConstRawExpr*>(expr->get_param_expr(param_num - 1))->get_value().is_int()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("type value isn't int value");
    } else {
      int64_t type = static_cast<ObConstRawExpr*>(expr->get_param_expr(param_num - 1))->get_value().get_int();
      switch (type) {
        case 0:
          DATA_PRINTF(" false on empty");
          break;
        case 1:
          DATA_PRINTF(" true on empty");
          break;
        case 2:
          DATA_PRINTF(" error on empty");
          break;
        case 3:
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid type value.", K(type));
          break;
      }
    }
  }
  return ret;
}

int ObRawExprPrinter::print_is_json(ObSysFunRawExpr *expr)
{
  INIT_SUCC(ret);
  if (OB_UNLIKELY(expr->get_param_count() != 5)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected param count of expr to type", K(ret), KPC(expr), K(expr->get_param_count()));
  } else {
    // 'json_data' is json strict_opt scalar_opt unique_opt;
    // json_data
    PRINT_EXPR(expr->get_param_expr(0));
    // is json or is not json
    if (!static_cast<ObConstRawExpr*>(expr->get_param_expr(1))->get_value().is_int()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("type value isn't int value");
    } else {
      int64_t is_json_type = static_cast<ObConstRawExpr*>(expr->get_param_expr(1))->get_value().get_int();
      if (is_json_type == 0) {
        DATA_PRINTF(" is not json ");
      } else if (is_json_type == 1) {
        DATA_PRINTF(" is json ");
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected param value", K(ret), KPC(expr), K(is_json_type));
      }
    }

    // lax or strict
    if (OB_SUCC(ret)) {
      if (!static_cast<ObConstRawExpr*>(expr->get_param_expr(2))->get_value().is_int()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("type value isn't int value");
      } else {
        int64_t type = static_cast<ObConstRawExpr*>(expr->get_param_expr(2))->get_value().get_int();
        switch (type) {
          case 0:
            DATA_PRINTF("lax ");
            break;
          case 1:
            DATA_PRINTF("strict ");
            break;
          case 2:
            break;
          default:
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid type value.", K(type));
            break;
        }
      }
    }

    // scalars
    if (OB_SUCC(ret)) {
      if (!static_cast<ObConstRawExpr*>(expr->get_param_expr(3))->get_value().is_int()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("type value isn't int value");
      } else {
        int64_t type = static_cast<ObConstRawExpr*>(expr->get_param_expr(3))->get_value().get_int();
        switch (type) {
          case 0:
            DATA_PRINTF("allow scalars ");
            break;
          case 1:
            DATA_PRINTF("disallow scalars ");
            break;
          case 2:
            break;
          default:
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid type value.", K(type));
            break;
        }
      }
    }

    // unique
    if (OB_SUCC(ret)) {
      if (!static_cast<ObConstRawExpr*>(expr->get_param_expr(4))->get_value().is_int()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("type value isn't int value");
      } else {
        int64_t type = static_cast<ObConstRawExpr*>(expr->get_param_expr(4))->get_value().get_int();
        switch (type) {
          case 0:
            DATA_PRINTF("without unique keys");
            break;
          case 1:
            DATA_PRINTF("with unique keys");
            break;
          case 2:
            break;
          default:
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid type value.", K(type));
            break;
        }
      }
    }

  }
  return ret;
}

int ObRawExprPrinter::print_json_expr(ObSysFunRawExpr *expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf_) || OB_ISNULL(pos_) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_ is NULL of buf_ is NULL or pos_ is NULL or expr is NULL", K(ret));
  } else {
    ObString func_name = expr->get_func_name();
    switch (expr->get_expr_type()) {
      case T_FUN_SYS_JSON_VALUE: {
        // json value parameter count more than 12, because mismatch is multi-val and default value.
        // json_value(expr(0), expr(1) returning cast_type ascii xxx on empty(default value) xxx on error(default value) xxx on mismatch (xxx))
        if (OB_UNLIKELY(expr->get_param_count() < 12)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected param count of expr to type", K(ret), KPC(expr));
        } else {
          DATA_PRINTF("json_value(");
          PRINT_EXPR(expr->get_param_expr(0));
          DATA_PRINTF(",");
          PRINT_EXPR(expr->get_param_expr(1));
          if (OB_SUCC(ret) && OB_FAIL(print_json_value(expr))) {
            LOG_WARN("bool expr have to be inner expr for now", K(ret), K(*expr));
          }
        }
        break;
      }
      case T_FUN_SYS_JSON_QUERY: {
        // json query (json doc, json path, (returning cast_type) opt_scalars opt_pretty opt_ascii opt_wrapper on_error on_empty on_mismatch).
        if (OB_UNLIKELY(expr->get_param_count() != 11)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected param count of expr to type", K(ret), KPC(expr), K(expr->get_param_count()));
        } else {
          DATA_PRINTF("json_query(");
          PRINT_EXPR(expr->get_param_expr(0));
          DATA_PRINTF(",");
          PRINT_EXPR(expr->get_param_expr(1));
          if (OB_SUCC(ret)) {
            if (expr->get_param_expr(2)->get_expr_type() == T_NULL) {
              // do nothing
            } else if (OB_FAIL(print_json_return_type(expr->get_param_expr(2)))) {
              LOG_WARN("fail to print cast_type", K(ret));
            }
          }
          if (OB_SUCC(ret) && OB_FAIL(print_json_query(expr))) {
            LOG_WARN("bool expr have to be inner expr for now", K(ret), K(*expr));
          }
        }
        break;
      }
      case T_FUN_SYS_JSON_OBJECT: {
        // json_object( (key , value) * n, xxx on null returning cast_type (strict) (with unique keys))
        if (OB_UNLIKELY(expr->get_param_count() < 4)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected param count of expr to type", K(ret), KPC(expr));
        } else {
          DATA_PRINTF("json_object(");
          if (OB_SUCC(ret) && OB_FAIL(print_json_object(expr))) {
            LOG_WARN("bool expr have to be inner expr for now", K(ret), K(*expr));
          }
        }
        break;
      }
      case T_FUN_SYS_JSON_EQUAL: {
        if (OB_SUCC(ret) && OB_FAIL(print_json_equal(expr))) {
          LOG_WARN("bool expr have to be inner expr for now", K(ret), K(*expr));
        }
        break;
      }
      case T_FUN_SYS_IS_JSON: {
        if (OB_SUCC(ret) && OB_FAIL(print_is_json(expr))) {
          LOG_WARN("bool expr have to be inner expr for now", K(ret), K(*expr));
        }
        break;
      }
      case T_FUN_SYS_JSON_EXISTS: {
        if (OB_UNLIKELY(expr->get_param_count() < 5)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected param count of expr to type", K(ret), KPC(expr), K(expr->get_param_count()));
        } else {
          // json_exists('json_data', 'json_path' PASSING AS ON ERROR ON EMPTY)
          DATA_PRINTF("json_exists(");
          PRINT_EXPR(expr->get_param_expr(0));
          DATA_PRINTF(",");
          PRINT_EXPR(expr->get_param_expr(1));
          if (OB_SUCC(ret) && OB_FAIL(print_json_exists(expr))) {
            LOG_WARN("bool expr have to be inner expr for now", K(ret), K(*expr));
          } else {
            DATA_PRINTF(")");
          }
        }
        break;
      }
      case T_FUN_SYS_JSON_ARRAY: {
        if (OB_UNLIKELY(expr->get_param_count() < 4)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected param count of expr to type", K(ret), KPC(expr), K(expr->get_param_count()));
        } else if (OB_FAIL(print_json_array(expr))) {
          LOG_WARN("fail to print json_array raw expr", K(ret), K(*expr));
        }
        break;
      }
      case T_FUN_SYS_JSON_MERGE_PATCH: {
        if (OB_UNLIKELY(expr->get_param_count() != 7)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected param count of expr to type", K(ret), KPC(expr), K(expr->get_param_count()));
        } else if (OB_FAIL(print_json_mergepatch(expr))) {
          LOG_WARN("fail to print json_array raw expr", K(ret), K(*expr));
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("should be json_expr", K(ret), K(expr->get_expr_type()));
      }
    }
  }
  return ret;
}

int ObRawExprPrinter::print_json_equal(ObSysFunRawExpr *expr)
{
  INIT_SUCC(ret);
  if (OB_UNLIKELY(expr->get_param_count() > 3) || OB_UNLIKELY(expr->get_param_count() < 2)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected param count of expr to type", K(ret), KPC(expr), K(expr->get_param_count()));
  } else {
    // json_equal('json_data1', 'json_data1' ON ERROR)
    DATA_PRINTF("json_equal(");
    PRINT_EXPR(expr->get_param_expr(0));
    DATA_PRINTF(",");
    PRINT_EXPR(expr->get_param_expr(1));
    if (OB_SUCC(ret) && expr->get_param_count() == 3) {
      if (!static_cast<ObConstRawExpr*>(expr->get_param_expr(2))->get_value().is_int()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("type value isn't int value");
      } else {
        int64_t type = static_cast<ObConstRawExpr*>(expr->get_param_expr(2))->get_value().get_int();
        switch (type) {
          case 0:
            DATA_PRINTF(" false on error");
            break;
          case 1:
            DATA_PRINTF(" true on error");
            break;
          case 2:
            DATA_PRINTF(" error on error");
            break;
          case 3:
            break;
          default:
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid type value.", K(type));
            break;
        }
      }
    }
    DATA_PRINTF(")");
  }
  return ret;
}

int ObRawExprPrinter::inner_print_fun_params(ObSysFunRawExpr &expr)
{
  int ret = OB_SUCCESS;
  int64_t param_count = expr.get_param_count();
  int64_t i = 0;
  DATA_PRINTF("(");
  for (; OB_SUCC(ret) && i < param_count; ++i) {
    PRINT_EXPR(expr.get_param_expr(i));
    DATA_PRINTF(",");
  }
  if (OB_SUCC(ret)) {
    if (i > 0) {
      --*pos_;
    }
    DATA_PRINTF(")");
  }
  return ret;
}

int ObRawExprPrinter::print(ObSysFunRawExpr *expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf_) || OB_ISNULL(pos_) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_ is NULL of buf_ is NULL or pos_ is NULL or expr is NULL", K(ret));
  } else {
    ObString func_name = expr->get_func_name();
    ObExprOperatorType expr_type = expr->get_expr_type();
    switch (expr_type) {
      case T_FUN_SYS_ALIGN_DATE4CMP: {
        CK(3 == expr->get_param_count());
        PRINT_EXPR(expr->get_param_expr(0));
        break;
      }
      case T_FUN_SYS_UTC_TIMESTAMP:
      case T_FUN_SYS_UTC_TIME: {
        const int16_t scale = static_cast<int16_t>(expr->get_result_type().get_scale());
        if (scale > 0) {
          DATA_PRINTF("%.*s(%d)", LEN_AND_PTR(func_name), scale);
        } else {
          DATA_PRINTF("%.*s()", LEN_AND_PTR(func_name));
        }
        break;
      }
      case T_FUN_SYS_XMLCAST:
      case T_FUN_SYS_TREAT:
      case T_FUN_SYS_CAST: {
        if (2 != expr->get_param_count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("param count should be equal 2", K(ret), K(expr->get_param_count()));
        } else if (expr->has_flag(IS_INNER_ADDED_EXPR)) {
          PRINT_EXPR(expr->get_param_expr(0));
        } else {
          if (OB_SUCC(ret)) {
            if (T_FUN_SYS_TREAT == expr_type) {
              DATA_PRINTF("treat(");
            } else if (T_FUN_SYS_XMLCAST == expr_type) {
              DATA_PRINTF("xmlcast(");
            } else {
              DATA_PRINTF("cast(");
            }
          }
          if (OB_SUCC(ret)) {
            PRINT_EXPR(expr->get_param_expr(0));
            DATA_PRINTF(" as ");
            if (OB_SUCC(ret) && OB_FAIL(print_cast_type(expr->get_param_expr(1)))) {
              LOG_WARN("fail to print cast_type", K(ret));
            }
          }
          DATA_PRINTF(")");
        }
        break;
      }
      case T_FUN_SYS_SET_COLLATION: {
        ObConstRawExpr *coll_expr = NULL;
        if (2 != expr->get_param_count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("param count should be 2", K(expr->get_param_count()));
        } else if (OB_ISNULL(coll_expr = static_cast<ObConstRawExpr*>(expr->get_param_expr(1)))
            || !coll_expr->is_const_raw_expr()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("coll_expr is invalid", "coll_expr", PC(coll_expr));
        } else if (!static_cast<ObConstRawExpr*>(coll_expr)->get_value().is_int()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("collation value isn't int value");
        } else {
          ObString collation_name;
          ObCollationType cs_type = static_cast<ObCollationType>(coll_expr->get_value().get_int());
          PRINT_EXPR(expr->get_param_expr(0));
          DATA_PRINTF(" collate ");
          if (OB_SUCC(ret) && OB_FAIL(ObCharset::collation_name(cs_type, collation_name))) {
            LOG_WARN("get collation name failed", K(cs_type));
          }
          DATA_PRINTF("%.*s", LEN_AND_PTR(collation_name));
        }
        break;
      }
      case T_FUN_SYS_CONVERT: {
        if (lib::is_mysql_mode()) {
          if (2 != expr->get_param_count()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("param count should be equal 2", K(ret), K(expr->get_param_count()));
          } else {
            DATA_PRINTF("convert(");
            PRINT_EXPR(expr->get_param_expr(0));
            DATA_PRINTF(" using ");
            PRINT_EXPR(expr->get_param_expr(1));
            DATA_PRINTF(")");
          }
        } else {
          DATA_PRINTF("%.*s", LEN_AND_PTR(func_name));
          OZ(inner_print_fun_params(*expr));
        }
        break;
      }
      case T_FUN_SYS_INTERVAL: {
        if (expr->get_param_count() < 2) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("param count should be greater than 1", K(ret), K(expr->get_param_count()));
        } else {
          DATA_PRINTF("interval");
          OZ(inner_print_fun_params(*expr));
        }
        break;
      }
      case T_FUN_SYS_DATE_ADD:
      case T_FUN_SYS_DATE_SUB: {
        if (3 != expr->get_param_count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("param count should be equal 3", K(ret), K(expr->get_param_count()));
        } else {
          DATA_PRINTF("(");
          // expr1
          PRINT_EXPR(expr->get_param_expr(0));
          // '+' or '-'
          if (OB_SUCC(ret)) {
            if (ObString("date_add") == func_name) {
              DATA_PRINTF(" + ");
            } else {
              DATA_PRINTF(" - ");
            }
          }
          DATA_PRINTF("interval ");
          // expr2
          PRINT_EXPR(expr->get_param_expr(1));
          DATA_PRINTF(" ");
          if (OB_SUCC(ret)) {
            if (OB_FAIL(print_date_unit(expr->get_param_expr(2)))) {
              LOG_WARN("fail to print date unit", K(ret));
            }
          }
          DATA_PRINTF(")");
        }
        break;
      }
      case T_FUN_SYS_TIME_STAMP_DIFF: {
        if (3 != expr->get_param_count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("param count should be equal 3", K(ret), K(expr->get_param_count()));
        } else {
          DATA_PRINTF("timestampdiff(");
          // date_unit
          if (OB_SUCC(ret)) {
            if (OB_FAIL(print_date_unit(expr->get_param_expr(0)))) {
              LOG_WARN("fail to print date unit", K(ret));
            }
          }
          DATA_PRINTF(",");
          // expr1
          PRINT_EXPR(expr->get_param_expr(1));
          DATA_PRINTF(",");
          // expr2
          PRINT_EXPR(expr->get_param_expr(2));
          DATA_PRINTF(")");
        }
        break;
      }
      case T_FUN_SYS_EXTRACT: {
        if (2 != expr->get_param_count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("param count should be equal 2", K(ret), K(expr->get_param_count()));
        } else {
          DATA_PRINTF("extract(");
          // date_unit
          if (OB_SUCC(ret)) {
            if (OB_FAIL(print_date_unit(expr->get_param_expr(0)))) {
              LOG_WARN("fail to print date unit", K(ret));
            }
          }
          DATA_PRINTF(" from ");
          PRINT_EXPR(expr->get_param_expr(1));
          DATA_PRINTF(")");
        }
        break;
      }
      case T_FUN_SYS_TRIM: {
        int64_t param_num = expr->get_param_count();
        if (param_num > 3 || param_num <= 0) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid param count", K(ret), K(expr->get_param_count()));
        } else {
          DATA_PRINTF("trim(");
          if (1 == param_num) {
            PRINT_EXPR(expr->get_param_expr(0));
          } else if (2 == param_num || 3 == param_num) {
            if (OB_SUCC(ret)) {
              //type
              int64_t default_type = -1;
              ObConstRawExpr *con_expr = static_cast<ObConstRawExpr*>(expr->get_param_expr(0));
              if (OB_ISNULL(con_expr)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("con_expr should not be NULL", K(ret));
              } else {
                con_expr->get_value().get_int(default_type);
                if (0 == default_type) {
                  DATA_PRINTF("both ");
                } else if (1 == default_type) {
                  DATA_PRINTF("leading ");
                } else if (2 == default_type) {
                  DATA_PRINTF("trailing ");
                } else {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("unknown default type", K(ret), K(default_type));
                }
              }
              if (OB_SUCC(ret)) {
                if (2 == param_num) {
                  DATA_PRINTF(" from ");
                  // expr
                  PRINT_EXPR(expr->get_param_expr(1));
                } else if (3 == param_num) {
                  // default_operand
                  PRINT_EXPR(expr->get_param_expr(1));
                  DATA_PRINTF(" from ");
                  // expr
                  PRINT_EXPR(expr->get_param_expr(2));
                }
              }
            }
          }//end 2 || 3
          DATA_PRINTF(")");
        }
        break;
      }
      case T_OP_GET_USER_VAR: {
        int64_t param_num = expr->get_param_count();
        if (1 != param_num || OB_ISNULL(expr->get_param_expr(0))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid param count", K(ret), K(expr->get_param_count()), K(expr->get_param_expr(0)));
        } else if (!expr->get_param_expr(0)->is_const_raw_expr()
            || !static_cast<ObConstRawExpr*>(expr->get_param_expr(0))->get_value().is_varchar()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid user variable name", K(ret), K(*expr->get_param_expr(0)));
        } else {
          DATA_PRINTF("@");
          func_name = static_cast<ObConstRawExpr*>(expr->get_param_expr(0))->get_value().get_varchar();
          DATA_PRINTF("%.*s", LEN_AND_PTR(func_name));
        }
        break;
      }
      case T_FUN_COLUMN_CONV: {
        int64_t param_num = expr->get_param_count();
        if ((param_num != ObExprColumnConv::PARAMS_COUNT_WITH_COLUMN_INFO
            && param_num != ObExprColumnConv::PARAMS_COUNT_WITHOUT_COLUMN_INFO)
            || OB_ISNULL(expr->get_param_expr(4))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid param count", K(ret), K(expr->get_param_count()), K(expr->get_param_expr(4)));
        } else {
          PRINT_EXPR(expr->get_param_expr(4));
        }
        break;
      }
      case T_OP_GET_SUBPROGRAM_VAR: //fallthrough
      case T_OP_GET_PACKAGE_VAR: {
        DATA_PRINTF("?");
        break;
      }
      case T_FUN_SYS_SYSDATE: //fallthrough
      case T_FUN_SYS_UID:
      case T_FUN_SYS_SESSIONTIMEZONE:
      case T_FUN_SYS_DBTIMEZONE:
      case T_FUN_SYS_ROWNUM:
      case T_FUN_SYS_USER: {
        DATA_PRINTF("%.*s", LEN_AND_PTR(expr->get_func_name()));
        if (lib::is_mysql_mode()) {
          DATA_PRINTF("()");
        }
        break;
      }
      case T_FUN_SYS_CUR_DATE: {
        if (lib::is_oracle_mode()) {
          DATA_PRINTF(N_CURRENT_DATE);
        } else {
          DATA_PRINTF("curdate()");
        }
        break;
      }
      case T_FUN_SYS_CUR_TIMESTAMP:
        // now(), current_timestamp(), local_time(), local_timestamp()
        if (lib::is_oracle_mode()) {
          func_name = "current_timestamp";
        } else {
          DATA_PRINTF("now(");
          if (OB_SUCC(ret) && expr->get_param_count() > 0) {
            // accuracy
            if (1 != expr->get_param_count()) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("param count should be equal 1", K(ret), K(expr->get_param_count()));
            } else {
              PRINT_EXPR(expr->get_param_expr(0));
            }
          }
          DATA_PRINTF(")");
          break;
        }
      case T_FUN_SYS_SYSTIMESTAMP:
      case T_FUN_SYS_LOCALTIMESTAMP: {
        DATA_PRINTF("%.*s", LEN_AND_PTR(func_name));
        if (OB_SUCC(ret) && expr->get_param_count() > 0) {
          // accuracy
          if (1 != expr->get_param_count()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("param count should be equal 1", K(ret), K(expr->get_param_count()));
          } else {
            PRINT_EXPR(expr->get_param_expr(0));
          }
          DATA_PRINTF(")");
        }
        break;
      }
      case T_FUN_SYS_SEQ_NEXTVAL: {
        ObSequenceRawExpr *seq_expr= static_cast<ObSequenceRawExpr*>(expr);
        if (1 != seq_expr->get_param_count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("param count should be equal 1", K(ret), K(seq_expr->get_param_count()));
        } else {
          if (!seq_expr->get_database_name().empty()) {
            PRINT_IDENT_WITH_QUOT(seq_expr->get_database_name());
            DATA_PRINTF(".");
          }
          if (!seq_expr->get_name().empty() && !seq_expr->get_action().empty()) {
            PRINT_IDENT_WITH_QUOT(seq_expr->get_name());
            DATA_PRINTF(".");
            PRINT_IDENT_WITH_QUOT(seq_expr->get_action());
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("sequence should specify format as seqname.action", K(ret));
          }
          if (OB_SUCC(ret) && seq_expr->is_dblink_sys_func()) {
            DATA_PRINTF("@%.*s", LEN_AND_PTR(seq_expr->get_dblink_name()));
          }
        }
        break;
      }
      case T_FUN_PL_SQLCODE_SQLERRM: {
        ObPLSQLCodeSQLErrmRawExpr *sql_expr = static_cast<ObPLSQLCodeSQLErrmRawExpr*>(expr);
        if (sql_expr->get_is_sqlcode()) {
          DATA_PRINTF("SQLCODE");
        } else {
          DATA_PRINTF("SQLERRM");
          if (1 == sql_expr->get_param_exprs().count()) {
            DATA_PRINTF("(");
            PRINT_EXPR(sql_expr->get_param_exprs().at(0));
            DATA_PRINTF(")");
          }
        }
        break;
      }
      case T_FUN_PLSQL_VARIABLE: {
        ObPLSQLVariableRawExpr *sql_expr = static_cast<ObPLSQLVariableRawExpr *>(expr);
        DATA_PRINTF("$$");
        DATA_PRINTF("%.*s", LEN_AND_PTR(sql_expr->get_plsql_variable()));
        break;
      }
      case T_FUN_PL_COLLECTION_CONSTRUCT: {
        ObCollectionConstructRawExpr *coll = static_cast<ObCollectionConstructRawExpr*>(expr);
        const common::ObIArray<common::ObString> &names = coll->get_access_names();
        int64_t i = 0;
        for (; OB_SUCC(ret) && i < names.count(); ++i) {
          DATA_PRINTF("%.*s.", LEN_AND_PTR(names.at(i)));
        }
        if (OB_SUCC(ret)) {
          --*pos_;
        }
        OZ(inner_print_fun_params(*expr));
        break;
      }
      case T_FUN_PL_OBJECT_CONSTRUCT: {
        ObObjectConstructRawExpr *object = static_cast<ObObjectConstructRawExpr*>(expr);
        const common::ObIArray<common::ObString> &names = object->get_access_names();
        int64_t i = 0;
        for (; OB_SUCC(ret) && i < names.count(); ++i) {
          DATA_PRINTF("%.*s.", LEN_AND_PTR(names.at(i)));
        }
        if (OB_SUCC(ret)) {
          --*pos_;
        }
        OZ(inner_print_fun_params(*expr));
        break;
      }
      case T_FUN_SYS_DEFAULT: {
        if (lib::is_oracle_mode()) {
          DATA_PRINTF("default");  
        } else {
          DATA_PRINTF("default(");
          if (5 != expr->get_param_count()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("param count should be equal 1", K(ret), K(expr->get_param_count()));
          } else {
            PRINT_EXPR(expr->get_param_expr(0));
            DATA_PRINTF(")");
          }
        }
        break;
      }
      case T_FUN_SYS_LNNVL: {
        if (lib::is_oracle_mode()) {
          DATA_PRINTF("(%.*s", LEN_AND_PTR(func_name));
        } else {
          DATA_PRINTF("%.*s(", LEN_AND_PTR(func_name));
        }
        if (1 != expr->get_param_count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("param count should be equal 1", K(ret), K(expr->get_param_count()));
        } else {
          PRINT_EXPR(expr->get_param_expr(0));
          DATA_PRINTF(")");
        }
        break;
      }
      case T_FUN_SYS_TRANSLATE: {
        if (OB_FAIL(print_translate(expr))) {
          LOG_WARN("failed to print translate", K(ret));
        }
        break;
      }
      case T_FUN_SYS_POSITION: {
        DATA_PRINTF("%.*s", LEN_AND_PTR(func_name));
        if (2 != expr->get_param_count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("param count should be equal 2", K(ret), K(expr->get_param_count()));
        } else {
          DATA_PRINTF("(");
          PRINT_EXPR(expr->get_param_expr(0));
          DATA_PRINTF(" in ");
          PRINT_EXPR(expr->get_param_expr(1));
          DATA_PRINTF(")");
        }
        break;
      }
      case T_FUN_SYS_CHAR: {
        DATA_PRINTF("%.*s(", LEN_AND_PTR(func_name));
        if (OB_SUCC(ret)) {
          if (expr->get_param_count() < 2) {
            ret = OB_ERR_UNEXPECTED;
          LOG_WARN("param count should be great or equ 2", K(ret), K(expr->get_param_count()));
          } else {
            int64_t i = 0;
            for (; OB_SUCC(ret) && i < expr->get_param_count() - 1; ++i) {
              PRINT_EXPR(expr->get_param_expr(i));
              DATA_PRINTF(",");
            }
            if (OB_SUCC(ret)) {
              --*pos_;
              DATA_PRINTF(" using ");
              PRINT_EXPR(expr->get_param_expr(expr->get_param_count() - 1));
              DATA_PRINTF(")");
            }
          }
        }
        break;
      }
      case T_FUN_SYS_GET_FORMAT: {
        if (2 != expr->get_param_count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("param count should be equal 2", K(ret), K(expr->get_param_count()));
        } else {
          DATA_PRINTF("get_format(");
          // temporal_unit
          if (OB_SUCC(ret)) {
            if (OB_FAIL(print_get_format_unit(expr->get_param_expr(0)))) {
              LOG_WARN("fail to print date unit", K(ret));
            }
          }
          DATA_PRINTF(", ");
          PRINT_EXPR(expr->get_param_expr(1));
          DATA_PRINTF(")");
        }
        break;
      }
      case T_FUN_SYS_TO_TYPE: {
        if (OB_UNLIKELY(1 != expr->get_param_count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected param count of expr to type", K(ret), KPC(expr));
        } else {
          PRINT_EXPR(expr->get_param_expr(0));
        }
        break;
      }
      case T_FUN_RETURNING_LOB: {
        PRINT_EXPR(expr->get_param_expr(0));
        break;
      }
      case T_FUN_SYS_REMOVE_CONST: {
        if (expr->has_flag(IS_INNER_ADDED_EXPR)) {
          // ignore print inner added expr
          CK(1 == expr->get_param_count());
          PRINT_EXPR(expr->get_param_expr(0));
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("bool expr have to be inner expr for now", K(ret), K(*expr));
        }
        break;
      }
      case T_FUN_SYS_CALC_UROWID: {
        ObColumnRefRawExpr *sub_pk_expr = NULL;
        if (expr->get_param_count() < 2) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected param count of expr to type", K(ret), KPC(expr));
        } else if (!expr->get_param_expr(1)->is_column_ref_expr()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected param type of expr", K(ret), KPC(expr));
        } else {
          sub_pk_expr = static_cast<ObColumnRefRawExpr*>(expr->get_param_expr(1));
          // Mock a rowid ColumnRefExpr temporarily
          ObColumnRefRawExpr tmp_rowid_expr;
          tmp_rowid_expr.set_expr_type(sub_pk_expr->get_expr_type());
          tmp_rowid_expr.assign(*sub_pk_expr);
          tmp_rowid_expr.set_column_name(ObString(OB_HIDDEN_LOGICAL_ROWID_COLUMN_NAME));
          PRINT_EXPR(&tmp_rowid_expr);
        }
        break;
      }
      case T_FUN_SYS_IS_JSON:
      case T_FUN_SYS_JSON_VALUE:
      case T_FUN_SYS_JSON_QUERY:
      case T_FUN_SYS_JSON_OBJECT:
      case T_FUN_SYS_JSON_EQUAL:
      case T_FUN_SYS_JSON_ARRAY:
      case T_FUN_SYS_JSON_MERGE_PATCH:
      case T_FUN_SYS_JSON_EXISTS: {
        if (lib::is_mysql_mode() && (expr_type == T_FUN_SYS_JSON_ARRAY || expr_type == T_FUN_SYS_JSON_MERGE_PATCH)) {
          DATA_PRINTF("%.*s", LEN_AND_PTR(func_name));
          OZ(inner_print_fun_params(*expr));
        } else if(lib::is_oracle_mode() || T_FUN_SYS_JSON_VALUE == expr_type) {
          if (OB_FAIL(print_json_expr(expr))) {
            LOG_WARN("fail to print json expr", K(ret), K(*expr));
          }
        } else { // mysql default
          DATA_PRINTF("%.*s", LEN_AND_PTR(func_name));
          OZ(inner_print_fun_params(*expr));
        }
        break;
      }
      case T_FUN_PAD: {
        if (print_params_.for_dblink_) {
          // Oracle do not have function pad,
          // but resolver will add pad above some expr.
          // So, only print the first param
          PRINT_EXPR(expr->get_param_expr(0));
        } else {
          DATA_PRINTF("%.*s", LEN_AND_PTR(func_name));
          OZ(inner_print_fun_params(*expr));
        }
        break;
      }
      case T_FUN_SYS_XMLPARSE : {
        if (OB_FAIL(print_xml_parse_expr(expr))) {
          LOG_WARN("print xml_parse expr failed", K(ret));
        }
        break;
      }
      case T_FUN_SYS_XML_ELEMENT : {
        if (OB_FAIL(print_xml_element_expr(expr))) {
          LOG_WARN("print xml_element expr failed", K(ret));
        }
        break;
      }
      case T_FUN_SYS_XML_ATTRIBUTES : {
        if (OB_FAIL(print_xml_attributes_expr(expr))) {
          LOG_WARN("print xml_attributes expr failed", K(ret));
        }
        break;
      }
      case T_FUN_SYS_XML_SERIALIZE: {
        if (OB_FAIL(print_xml_serialize_expr(expr))) {
          LOG_WARN("print xmlserialize expr failed", K(ret));
        }
        break;
      }
      case T_FUN_SYS_REGEXP_LIKE: {
        if (OB_UNLIKELY(expr->get_param_count() < 2 || expr->get_param_count() > 3)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expr param count should be equal 2 or 3", "count", expr->get_param_count(), K(ret));
        } else {
          DATA_PRINTF("(");
          DATA_PRINTF("%.*s(", LEN_AND_PTR(func_name));
          PRINT_EXPR(expr->get_param_expr(0));
          DATA_PRINTF(",");
          PRINT_EXPR(expr->get_param_expr(1));
          if (OB_SUCC(ret) && expr->get_param_count() == 3) {
            DATA_PRINTF(",");
            PRINT_EXPR(expr->get_param_expr(2));
          }
          DATA_PRINTF("))");
        }
        break;
      }
      case T_FUN_SYS_XML_EXTRACT: {
        DATA_PRINTF("extract");
        OZ(inner_print_fun_params(*expr));
        break;
      }
      case T_FUN_ENUM_TO_STR:
      case T_FUN_SET_TO_STR:
      case T_FUN_ENUM_TO_INNER_TYPE:
      case T_FUN_SET_TO_INNER_TYPE: {
        if (expr->get_param_count() < 2) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("param count should be greater than 1", K(ret), K(*expr));
        } else {
          PRINT_EXPR(expr->get_param_expr(1));
        }
        break;
      }
      case T_OP_CONV: {
        DATA_PRINTF("%.*s(", LEN_AND_PTR(func_name));
        int64_t param_count = expr->get_param_count();
        int64_t i = 0;
        for (; OB_SUCC(ret) && i < param_count; ++i) {
          if (i == 0) {
            PRINT_EXPR(expr->get_param_expr(i));
            DATA_PRINTF(",");
          } else if (func_name.case_compare("BIN") == 0 || func_name.case_compare("OCT") == 0) {
            // do nothing
          } else {
            PRINT_EXPR(expr->get_param_expr(i));
            DATA_PRINTF(",");
          }
        }
        if (OB_SUCC(ret)) {
          if (i > 0) {
            --*pos_;
          }
          DATA_PRINTF(")");
        }
        break;
      }
      case T_FUN_SYS_ORA_DECODE: {
        //同一个函数 在Oracle下名为decode， 在MySQL下名为ora_decode
        // for
        // 保证SQL反拼不会出错
        if (lib::is_oracle_mode()) {
          DATA_PRINTF("decode");
        } else {
          DATA_PRINTF("ora_decode");
        }
        OZ(inner_print_fun_params(*expr));
        break;
      }
      case T_FUN_UDF: {
        PRINT_IDENT_WITH_QUOT(func_name);
        OZ(inner_print_fun_params(*expr));
        break;
      }
      default: {
        DATA_PRINTF("%.*s", LEN_AND_PTR(func_name));
        OZ(inner_print_fun_params(*expr));
        break;
      }
    }
  }

  return ret;
}

int ObRawExprPrinter::print_translate(ObSysFunRawExpr *expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf_) || OB_ISNULL(pos_) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_ is NULL of buf_ is NULL or pos_ is NULL or expr is NULL", K(ret));
  } else {
    ObString func_name = expr->get_func_name();
    if (2 == expr->get_param_count()) {
      ObRawExpr *first_param = expr->get_param_expr(0);
      ObRawExpr *second_param = expr->get_param_expr(1);
      DATA_PRINTF("translate(");
      PRINT_EXPR(first_param);
      if (OB_ISNULL(second_param)
          || OB_UNLIKELY(ObRawExpr::EXPR_CONST != second_param->get_expr_class())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("second param of translate is null or not const", K(ret), KPC(second_param));
      } else {
        ObConstRawExpr *const_param = static_cast<ObConstRawExpr *>(second_param);
        int64_t char_cs;
        if (OB_FAIL(const_param->get_value().get_int(char_cs))) {
          LOG_WARN("expect int value", K(ret), K(const_param->get_value()));
        } else if (0 == char_cs) {
          DATA_PRINTF(" using char_cs)");
        } else if (OB_LIKELY(1 == char_cs)) {
          DATA_PRINTF(" using nchar_cs)");
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expect 0 or 1 for second param", K(ret), K(char_cs));
        }
      }
    } else {
      DATA_PRINTF("%.*s", LEN_AND_PTR(func_name));
      OZ(inner_print_fun_params(*expr));
    }
  }
  return ret;
}

int ObRawExprPrinter::print(ObUDFRawExpr *expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf_) || OB_ISNULL(pos_) || OB_ISNULL(expr) || OB_ISNULL(schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_ is NULL of buf_ is NULL or pos_ is NULL or expr is NULL", K(ret));
  } else {
    if (!print_params_.for_dblink_) {
      if (!expr->get_database_name().empty()) {
        if (expr->get_database_name().case_compare("oceanbase") != 0) {
          PRINT_IDENT_WITH_QUOT(expr->get_database_name());
          DATA_PRINTF(".");
        }
      } else if (OB_ISNULL(schema_guard_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("schema_guard for print raw expr is null", K(ret));
      } else {

#define PRINT_IMPLICIT_DATABASE_NAME(OBJECT, object_id, get_object_info_func, get_name_func) \
do { \
  const uint64_t tenant_id = pl::get_tenant_id_by_object_id(object_id); \
  const share::schema::ObDatabaseSchema *database_schema = NULL; \
  const share::schema::OBJECT *object_info = NULL; \
  ObSchemaChecker checker; \
  bool exist = false; \
  if (OB_SYS_TENANT_ID == tenant_id) { \
  } else if (OB_FAIL(schema_guard_->get_object_info_func(tenant_id, object_id, object_info))) { \
    LOG_WARN("failed to get udt info", K(ret), KPC(expr), K(tenant_id)); \
  } else if (OB_ISNULL(object_info)) { \
    ret = OB_ERR_UNEXPECTED; \
    LOG_WARN("object info is null", K(ret), KPC(expr), K(tenant_id)); \
  } else if (OB_FAIL(schema_guard_->get_database_schema(tenant_id, object_info->get_database_id(), database_schema))) { \
    LOG_WARN("failed to get database schema", K(ret), KPC(expr), K(tenant_id)); \
  } else if (OB_ISNULL(database_schema)) { \
    ret = OB_ERR_UNEXPECTED; \
    LOG_WARN("database schema info is null", K(ret), K(database_schema), KPC(expr), K(tenant_id)); \
  } else if (OB_FAIL(checker.init(*schema_guard_, schema_guard_->get_session_id()))) { \
    LOG_WARN("failed to init schema checker", K(ret)); \
  } else if (OB_FAIL(checker.check_exist_same_name_object_with_synonym(tenant_id, \
                                                                       database_schema->get_database_id(), \
                                                                       database_schema->get_database_name_str(), \
                                                                       exist))) { \
    LOG_WARN("failed to check exist same name object with database name", K(ret), KPC(database_schema)); \
  } else if (!exist) { \
    PRINT_IDENT_WITH_QUOT(database_schema->get_database_name_str()); \
    DATA_PRINTF("."); \
  } \
} while (0)

        if (expr->get_pkg_id() != OB_INVALID_ID) { // package or udt udf
          if (expr->get_is_udt_udf()) {
            PRINT_IMPLICIT_DATABASE_NAME(ObUDTTypeInfo, expr->get_pkg_id(), get_udt_info, get_type_name);
          } else if (!expr->is_pkg_body_udf()) {
            PRINT_IMPLICIT_DATABASE_NAME(ObPackageInfo, expr->get_pkg_id(), get_package_info, get_package_name);
          }
        } else if (expr->get_udf_id() != OB_INVALID_ID && 0 == expr->get_subprogram_path().count()) { // standalone udf
          PRINT_IMPLICIT_DATABASE_NAME(ObRoutineInfo, expr->get_udf_id(), get_routine_info, get_routine_name);
        }
      }

#undef PRINT_IMPLICIT_DATABASE_NAME
    }

    if (!expr->get_package_name().empty() &&
        !expr->get_is_udt_cons()) {
      PRINT_IDENT_WITH_QUOT(expr->get_package_name());
      DATA_PRINTF(".");
    }
    PRINT_IDENT_WITH_QUOT(expr->get_func_name());
    DATA_PRINTF("(");

    ObIArray<ObExprResType> &params_type = expr->get_params_type();
    ObIArray<ObString> &params_name = expr->get_params_name();
    bool last_is_comma = false;
    CK (params_type.count() == expr->get_param_count());

    for (int64_t  i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (params_type.at(i).is_null()) { // default parameter, do not print
        // do nothing ...
      } else if (0 == i && expr->get_is_udt_cons()) {
        // do not print construct null self argument
      } else {
        if (!params_name.at(i).empty()) {
          PRINT_IDENT_WITH_QUOT(params_name.at(i));
          DATA_PRINTF("=>");
        }
        PRINT_EXPR(expr->get_param_expr(i));
        DATA_PRINTF(",");
        last_is_comma = true;
      }
    }
    if (OB_SUCC(ret)) {
      if (last_is_comma) {
        --*pos_;
      }
      DATA_PRINTF(")");
    }
    // PRINT_EXPR(static_cast<ObSysFunRawExpr*>(expr));
  }
  return ret;
}

int ObRawExprPrinter::print(ObWinFunRawExpr *expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf_) || OB_ISNULL(pos_) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_ is NULL of buf_ is NULL or pos_ is NULL or expr is NULL", K(ret));
  } else {
    ObString symbol("");
    ObItemType type = expr->get_func_type();
    switch (type) {
      case T_WIN_FUN_ROW_NUMBER:
        SET_SYMBOL_IF_EMPTY("row_number");
      case T_WIN_FUN_RANK:
        SET_SYMBOL_IF_EMPTY("rank");
      case T_WIN_FUN_DENSE_RANK:
        SET_SYMBOL_IF_EMPTY("dense_rank");
      case T_WIN_FUN_PERCENT_RANK: {
        SET_SYMBOL_IF_EMPTY("percent_rank");
        DATA_PRINTF("%.*s(", LEN_AND_PTR(symbol));
        DATA_PRINTF(")");
        DATA_PRINTF(" over(");
        if (OB_FAIL(print_partition_exprs(expr))) {
          LOG_WARN("failed to print partition exprs.", K(ret));
        } else if (OB_FAIL(print_order_items(expr))) {
          LOG_WARN("failed to print order items.", K(ret));
        } else {/* do nothing. */  }
        DATA_PRINTF(")");
        break;
      }
      case T_FUN_MAX:
        SET_SYMBOL_IF_EMPTY("max");
      case T_FUN_SUM:
        SET_SYMBOL_IF_EMPTY("sum");
      case T_FUN_MIN:
        SET_SYMBOL_IF_EMPTY("min");
      case T_FUN_COUNT:
        SET_SYMBOL_IF_EMPTY("count");
      case T_FUN_AVG:
        SET_SYMBOL_IF_EMPTY("avg");
      case T_FUN_CORR:
        SET_SYMBOL_IF_EMPTY("corr");
      case T_FUN_COVAR_POP:
        SET_SYMBOL_IF_EMPTY("covar_pop");
      case T_FUN_COVAR_SAMP:
        SET_SYMBOL_IF_EMPTY("covar_samp");
      case T_FUN_VAR_POP:
        SET_SYMBOL_IF_EMPTY("var_pop");
      case T_FUN_VAR_SAMP:
        SET_SYMBOL_IF_EMPTY("var_samp");
      case T_FUN_REGR_SLOPE:
        SET_SYMBOL_IF_EMPTY("regr_slope");
      case T_FUN_REGR_INTERCEPT:
        SET_SYMBOL_IF_EMPTY("regr_intercept");
      case T_FUN_REGR_COUNT:
        SET_SYMBOL_IF_EMPTY("regr_count");
      case T_FUN_REGR_R2:
        SET_SYMBOL_IF_EMPTY("regr_r2");
      case T_FUN_REGR_AVGX:
        SET_SYMBOL_IF_EMPTY("regr_avgx");
      case T_FUN_REGR_AVGY:
        SET_SYMBOL_IF_EMPTY("regr_avgy");
      case T_FUN_REGR_SXX:
        SET_SYMBOL_IF_EMPTY("regr_sxx");
      case T_FUN_REGR_SYY:
        SET_SYMBOL_IF_EMPTY("regr_syy");
      case T_FUN_REGR_SXY:
        SET_SYMBOL_IF_EMPTY("regr_sxy");
      case T_FUN_VARIANCE:
        SET_SYMBOL_IF_EMPTY("variance");
      case T_FUN_STDDEV:
        SET_SYMBOL_IF_EMPTY("stddev");
      case T_FUN_STDDEV_POP:
        SET_SYMBOL_IF_EMPTY("stddev_pop");
      case T_FUN_STDDEV_SAMP:
        SET_SYMBOL_IF_EMPTY("stddev_samp");
      case T_FUN_WM_CONCAT:
        SET_SYMBOL_IF_EMPTY("wm_concat");
      case T_FUN_TOP_FRE_HIST:
        SET_SYMBOL_IF_EMPTY("top_k_fre_hist");
      case T_FUN_HYBRID_HIST:
        SET_SYMBOL_IF_EMPTY("hybrid_hist");
      case T_FUN_SYS_BIT_AND:
        SET_SYMBOL_IF_EMPTY("bit_and");
      case T_FUN_SYS_BIT_OR:
        SET_SYMBOL_IF_EMPTY("bit_or");
      case T_FUN_SYS_BIT_XOR:
        SET_SYMBOL_IF_EMPTY("bit_xor");
      case T_FUN_JSON_ARRAYAGG:
        SET_SYMBOL_IF_EMPTY("json_arrayagg");
      case T_FUN_JSON_OBJECTAGG:
        SET_SYMBOL_IF_EMPTY("json_objectagg");
      case T_FUN_PL_AGG_UDF: {
        ObString database;
        if (OB_ISNULL(expr->get_agg_expr())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret), KPC(expr));
        } else if (type == T_FUN_PL_AGG_UDF) {
          ObRawExpr *udf_expr = expr->get_agg_expr()->get_pl_agg_udf_expr();
          if (OB_ISNULL(udf_expr) ||
              OB_UNLIKELY(!udf_expr->is_udf_expr())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null", K(ret), KPC(expr));
          } else {
            ObUDFRawExpr* udf = static_cast<ObUDFRawExpr*>(udf_expr);
            database = udf->get_database_name();
            symbol = udf->get_func_name();
          }
        }
        if (OB_SUCC(ret)) {
          if(!database.empty()){
            PRINT_IDENT_WITH_QUOT(database);
            DATA_PRINTF(".");
          }
          if (T_FUN_PL_AGG_UDF == type) {
            PRINT_IDENT_WITH_QUOT(symbol);
          } else {
            DATA_PRINTF("%.*s", LEN_AND_PTR(symbol));
          }
          DATA_PRINTF("(");
        }
        // distinct, default 'all', not print
        if (OB_SUCC(ret)) {
          if (expr->get_agg_expr()->is_param_distinct()) {
            DATA_PRINTF("distinct ");
          }
        }
        if (OB_SUCC(ret)) {
          int64_t N = expr->get_agg_expr()->get_real_param_count();
          if (0 == N) {
            if (T_FUN_COUNT != type) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("expr type should be T_FUN_COUNT ", K(ret), K(type));
            } else {
              DATA_PRINTF("0");
            }
          } else {
            for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
              PRINT_EXPR(expr->get_agg_expr()->get_param_expr(i));
              DATA_PRINTF(",");
            }
            if (OB_SUCC(ret)) {
              --*pos_;
            }
          }
        }
        DATA_PRINTF(")");
        DATA_PRINTF(" over(");
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(print_partition_exprs(expr))) {
          LOG_WARN("failed to print partition exprs.", K(ret));
        } else if (OB_FAIL(print_order_items(expr))) {
          LOG_WARN("failed to print order items.", K(ret));
        } else if (OB_FAIL(print_window_clause(expr))) {
          LOG_WARN("failed to print window clause.", K(ret));
        } else {/* do nothing. */  }
        DATA_PRINTF(")");
        break;
      }
      case T_WIN_FUN_NTILE: {
        SET_SYMBOL_IF_EMPTY("ntile");
        DATA_PRINTF("%.*s(", LEN_AND_PTR(symbol));
        if (OB_SUCC(ret)) {
          if (0 == expr->get_param_count()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("expr type should be T_FUN_COUNT ", K(ret), K(type));
          } else {
            int64_t N = expr->get_func_params().count();
            for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
              PRINT_EXPR(expr->get_func_params().at(i));
            }
          }
        }
        DATA_PRINTF(")");
        DATA_PRINTF(" over(");
        if (OB_FAIL(print_partition_exprs(expr))) {
          LOG_WARN("failed to print partition exprs.", K(ret));
        } else if (OB_FAIL(print_order_items(expr))) {
          LOG_WARN("failed to print order items.", K(ret));
        } else {/* do nothing. */  }
        DATA_PRINTF(")");
        break;
      }
      case T_WIN_FUN_CUME_DIST: {
        SET_SYMBOL_IF_EMPTY("cume_dist");
        DATA_PRINTF("%.*s(", LEN_AND_PTR(symbol));
        DATA_PRINTF(")");
        DATA_PRINTF(" over(");
        if (OB_FAIL(print_partition_exprs(expr))) {
          LOG_WARN("failed to print partition exprs.", K(ret));
        } else if (OB_FAIL(print_order_items(expr))) {
          LOG_WARN("failed to print order items.", K(ret));
        } else {/* do nothing. */  }
        DATA_PRINTF(")");
        break;
      }
      case T_WIN_FUN_LEAD:
        SET_SYMBOL_IF_EMPTY("lead");
      case T_WIN_FUN_LAG:{
        SET_SYMBOL_IF_EMPTY("lag");
        DATA_PRINTF("%.*s(", LEN_AND_PTR(symbol));
        if (OB_SUCC(ret)) {
          if (0 == expr->get_param_count()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("expr count should not be 0 ", K(ret), K(type));
          } else {
            int64_t N = expr->get_func_params().count();
            for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
              PRINT_EXPR(expr->get_func_params().at(i));
              DATA_PRINTF(",");
            }
            if (OB_SUCC(ret)) {
              --*pos_;
            }
          }
        }
        if (OB_FAIL(ret)) {
          if (expr->is_ignore_null()) {
            DATA_PRINTF(" ignore nulls");
          } else {
            DATA_PRINTF(" respect nulls");
          }
        }
        DATA_PRINTF(")");
        DATA_PRINTF(" over(");
        if (OB_FAIL(print_partition_exprs(expr))) {
          LOG_WARN("failed to print partition exprs.", K(ret));
        } else if (OB_FAIL(print_order_items(expr))) {
          LOG_WARN("failed to print order items.", K(ret));
        } else {/* do nothing. */  }
        DATA_PRINTF(")");
        break;
      }
      case T_WIN_FUN_RATIO_TO_REPORT:{
        SET_SYMBOL_IF_EMPTY("ratio_to_report");
        DATA_PRINTF("%.*s(", LEN_AND_PTR(symbol));
        if (OB_SUCC(ret)) {
          if (0 == expr->get_param_count()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("expr count should not be 0 ", K(ret), K(type));
          } else {
            int64_t N = expr->get_func_params().count();
            for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
              PRINT_EXPR(expr->get_func_params().at(i));
              DATA_PRINTF(",");
            }
            if (OB_SUCC(ret)) {
              --*pos_;
            }
          }
        }
        DATA_PRINTF(")");
        DATA_PRINTF(" over(");
        if (OB_FAIL(print_partition_exprs(expr))) {
          LOG_WARN("failed to print partition exprs.", K(ret));
        } else {/* do nothing. */  }
        DATA_PRINTF(")");
        break;
      }
      case T_WIN_FUN_FIRST_VALUE:
        SET_SYMBOL_IF_EMPTY("first_value");
      case T_WIN_FUN_LAST_VALUE:
        SET_SYMBOL_IF_EMPTY("last_value");
      case T_WIN_FUN_NTH_VALUE: {
       SET_SYMBOL_IF_EMPTY("nth_value");
       DATA_PRINTF("%.*s(", LEN_AND_PTR(symbol));
       if (OB_SUCC(ret)) {
         if (0 == expr->get_param_count()) {
           ret = OB_ERR_UNEXPECTED;
           LOG_WARN("expr count should not be 0 ", K(ret), K(type));
         } else {
           int64_t N = expr->get_func_params().count();
           for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
             PRINT_EXPR(expr->get_func_params().at(i));
             DATA_PRINTF(",");
           }
           if (OB_SUCC(ret)) {
             --*pos_;
           }
         }
       }
       DATA_PRINTF(")");
       if (OB_SUCC(ret)) {
         if (expr->is_from_first()) {
           DATA_PRINTF(" from first");
         } else {
           DATA_PRINTF(" from last");
         }
         if (expr->is_ignore_null()) {
           DATA_PRINTF(" ignore nulls");
         } else {
           DATA_PRINTF(" respect nulls");
         }
       }
       DATA_PRINTF(" over(");
       if (OB_FAIL(print_partition_exprs(expr))) {
         LOG_WARN("failed to print partition exprs.", K(ret));
       } else if (OB_FAIL(print_order_items(expr))) {
         LOG_WARN("failed to print order items.", K(ret));
       } else if (OB_FAIL(print_window_clause(expr))) {
         LOG_WARN("failed to print window clause.", K(ret));
       } else {/* do nothing. */  }
       DATA_PRINTF(")");
       break;
      }
      case T_FUN_GROUP_CONCAT: {
        if (OB_FAIL(print(expr->get_agg_expr()))) {
          LOG_WARN("failed to print agg expr", K(ret));
        }
        if (OB_SUCC(ret)) {
          DATA_PRINTF(" over(");
          if (OB_FAIL(print_partition_exprs(expr))) {
            LOG_WARN("failed to print partition exprs.", K(ret));
          } else if (OB_FAIL(print_order_items(expr))) {
            LOG_WARN("failed to print order items.", K(ret));
          } else if (OB_FAIL(print_window_clause(expr))) {
            LOG_WARN("failed to print window clause.", K(ret));
          } else {
            DATA_PRINTF(")");
          }
        }
        break;
      }
      case T_FUN_KEEP_MAX:
        SET_SYMBOL_IF_EMPTY("max");
      case T_FUN_KEEP_MIN:
        SET_SYMBOL_IF_EMPTY("min");
      case T_FUN_KEEP_SUM:
        SET_SYMBOL_IF_EMPTY("sum");
      case T_FUN_KEEP_COUNT:
        SET_SYMBOL_IF_EMPTY("count");
      case T_FUN_KEEP_AVG:
        SET_SYMBOL_IF_EMPTY("avg");
      case T_FUN_KEEP_STDDEV:
        SET_SYMBOL_IF_EMPTY("stddev");
      case T_FUN_KEEP_VARIANCE:
        SET_SYMBOL_IF_EMPTY("variance");
      case T_FUN_APPROX_COUNT_DISTINCT:
        SET_SYMBOL_IF_EMPTY("approx_count_distinct");
      case T_FUN_KEEP_WM_CONCAT: {
        SET_SYMBOL_IF_EMPTY("wm_concat");
        DATA_PRINTF("%.*s(", LEN_AND_PTR(symbol));
        if (0 == expr->get_agg_expr()->get_real_param_count()) {//count(*) keep(...)
          if (T_FUN_KEEP_COUNT != type) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("expr type should be T_FUN_KEEP_COUNT ", K(ret), K(type));
          } else {
            DATA_PRINTF("*");
          }
        } else if (OB_UNLIKELY(T_FUN_KEEP_WM_CONCAT != type &&
                               1 != expr->get_agg_expr()->get_real_param_count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected error", K(ret), K(type));
        } else {
          PRINT_EXPR(expr->get_agg_expr()->get_real_param_exprs().at(0));
        }
        if (OB_SUCC(ret)) {
          DATA_PRINTF(")");
          const ObIArray<OrderItem> &order_items = expr->get_agg_expr()->get_order_items();
          if (order_items.count() > 0) {
            DATA_PRINTF("keep(dense_rank first order by ");
            for (int64_t i = 0; OB_SUCC(ret) && i < order_items.count(); ++i) {
              const OrderItem &order_item = order_items.at(i);
              if (OB_ISNULL(order_item.expr_)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("get unexpected null", K(ret));
              } else {
                PRINT_EXPR(order_item.expr_);
                if (OB_SUCC(ret)) {
                  if (lib::is_mysql_mode()) {
                    if (is_descending_direction(order_item.order_type_)) {
                      DATA_PRINTF(" desc ");
                    }
                  } else if (order_item.order_type_ == NULLS_FIRST_ASC) {
                    DATA_PRINTF(" asc nulls first ");
                  } else if (order_item.order_type_ == NULLS_LAST_ASC) {//use default value
                    /*do nothing*/
                  } else if (order_item.order_type_ == NULLS_FIRST_DESC) {//use default value
                    DATA_PRINTF(" desc ");
                  } else if (order_item.order_type_ == NULLS_LAST_DESC) {
                    DATA_PRINTF(" desc nulls last ");
                  } else {/*do nothing*/}
                }
              }
              DATA_PRINTF(",");
            }
            if (OB_SUCC(ret)) {
              --*pos_;
              DATA_PRINTF(")");
            }
          }
          if (OB_SUCC(ret)) {
            DATA_PRINTF(" over(");
            if (OB_FAIL(print_partition_exprs(expr))) {
              LOG_WARN("failed to print partition exprs.", K(ret));
            } else if (OB_FAIL(print_order_items(expr))) {
              LOG_WARN("failed to print order items.", K(ret));
            } else if (OB_FAIL(print_window_clause(expr))) {
              LOG_WARN("failed to print window clause.", K(ret));
            } else {
              DATA_PRINTF(")");
            }
          }
        }
        break;
      }
      case T_FUN_MEDIAN:
        SET_SYMBOL_IF_EMPTY("median");
      case T_FUN_GROUP_PERCENTILE_DISC:
        SET_SYMBOL_IF_EMPTY("percentile_disc");
      case T_FUN_GROUP_PERCENTILE_CONT: {
        SET_SYMBOL_IF_EMPTY("percentile_cont");
        if (OB_UNLIKELY(1 != expr->get_agg_expr()->get_real_param_count())
            || OB_UNLIKELY(1 != expr->get_agg_expr()->get_order_items().count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected error", K(ret), K(type));
        } else {
          DATA_PRINTF("%.*s(", LEN_AND_PTR(symbol));
          PRINT_EXPR(expr->get_agg_expr()->get_param_expr(0));
          DATA_PRINTF(")");
          if (OB_UNLIKELY(T_FUN_MEDIAN != type)) {
            DATA_PRINTF(" within group ( order by ");
            const OrderItem &order_item = expr->get_agg_expr()->get_order_items().at(0);
            PRINT_EXPR(order_item.expr_);
            if (OB_SUCC(ret) && is_descending_direction(order_item.order_type_)) {
              DATA_PRINTF(" desc ");
            }
            DATA_PRINTF(")");
          }
        }
        DATA_PRINTF(" over(");
        if (OB_FAIL(print_partition_exprs(expr))) {
          LOG_WARN("failed to print partition exprs.", K(ret));
        } else {/* do nothing. */  }
        DATA_PRINTF(")");
        break;
      }
      default: {
       ret = OB_ERR_UNEXPECTED;
       LOG_WARN("unknown expr type", K(ret), K(type));
       break;
      }
    }
  }
  return ret;
}

int ObRawExprPrinter::print(ObPseudoColumnRawExpr *expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf_) || OB_ISNULL(pos_) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_ is NULL of buf_ is NULL or pos_ is NULL or expr is NULL", K(ret));
  } else {
    ObString symbol("");
    ObItemType type = expr->get_expr_type();
    switch (type) {
      case T_LEVEL :
        SET_SYMBOL_IF_EMPTY("level");
      case T_CONNECT_BY_ISCYCLE :
        SET_SYMBOL_IF_EMPTY("connect_by_iscycle");
      case T_ORA_ROWSCN :
        SET_SYMBOL_IF_EMPTY("ora_rowscn");
      case T_CONNECT_BY_ISLEAF : {
        SET_SYMBOL_IF_EMPTY("connect_by_isleaf");
        if (0 != expr->get_param_count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expr param count should be 0", K(ret), K(expr->get_param_count()));
        } else {
          DATA_PRINTF("%.*s", LEN_AND_PTR(symbol));
        }
        break;
      }
      case T_PSEUDO_EXTERNAL_FILE_COL: {
        if (!expr->get_table_name().empty()) {
          PRINT_IDENT(expr->get_table_name());
          DATA_PRINTF(".");
        }
        PRINT_IDENT(expr->get_expr_name());
        break;
      }
      default : {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected pseudo column type", K(type));
      }
    }
  }
  return ret;
}

int ObRawExprPrinter::print_partition_exprs(ObWinFunRawExpr *expr)
{
  int ret = OB_SUCCESS;
  int64_t N = expr->get_partition_exprs().count();
  if (N == 0) { /* do nothing. */
  } else if (N > 0) {
    DATA_PRINTF(" PARTITION BY ");
    for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
      if (OB_ISNULL(expr->get_partition_exprs().at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get partition by exprs.", K(ret));
      } else {
        PRINT_EXPR(expr->get_partition_exprs().at(i));
        if (i < N - 1) {
          DATA_PRINTF(", ");
        } else { /* Do nothing */ }
      }
    }
  } else { /* do nothing. */ }
  return ret;
}

int ObRawExprPrinter::print_order_items(ObWinFunRawExpr *expr)
{
  int ret = OB_SUCCESS;
  int64_t N = expr->get_order_items().count();
  if (N == 0) { /* do nothing. */
  } else if (N > 0) {
    DATA_PRINTF(" ORDER BY ");
    for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
      if (OB_ISNULL(expr->get_order_items().at(i).expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get order items exprs.", K(ret));
      } else {
        PRINT_EXPR(expr->get_order_items().at(i).expr_);
        ObOrderDirection order_type = expr->get_order_items().at(i).order_type_;
        if (lib::is_oracle_mode()) {
          if (ObOrderDirection::NULLS_FIRST_ASC == order_type) {
            DATA_PRINTF(" ASC NULLS FIRST ");
          } else if (ObOrderDirection::NULLS_FIRST_DESC == order_type) {
            DATA_PRINTF(" DESC NULLS FIRST ");
          } else if (ObOrderDirection::NULLS_LAST_ASC == order_type) {
            DATA_PRINTF(" ASC NULLS LAST ");
          } else if (ObOrderDirection::NULLS_LAST_DESC == order_type) {
            DATA_PRINTF(" DESC NULLS LAST ");
          } else { /* do nothing. */ }
        } else {
          if (is_ascending_direction(order_type)) {
            DATA_PRINTF(" ASC ");
          } else if (is_descending_direction(order_type)) {
            DATA_PRINTF(" DESC ");
          } else { /* do nothing. */ }
        }
      }
      if (i < N - 1) {
        DATA_PRINTF(", ");
      } else { /* Do nothing */ }
    }
  }
  return ret;
}

int ObRawExprPrinter::print_window_clause(ObWinFunRawExpr *expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf_) || OB_ISNULL(pos_) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_ is NULL of buf_ is NULL or pos_ is NULL or expr is NULL", K(ret));
  } else if (ObRawExpr::EXPR_WINDOW != expr->get_expr_class()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected expr type.", K(ret));
  } else if (expr->get_order_items().empty()) {
    // do nothing.
  } else {
    if (WindowType::WINDOW_MAX == expr->get_window_type()) {
      //do nothing.
    } else if (WindowType::WINDOW_ROWS == expr->get_window_type()) {
      DATA_PRINTF(" ROWS");
    } else if (WindowType::WINDOW_RANGE == expr->get_window_type()) {
      DATA_PRINTF(" RANGE");
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected expr type.", K(ret));
    }
    if (OB_FAIL(ret)) {
    } else if (expr->is_between()) {
      DATA_PRINTF(" BETWEEN ");
      if (BoundType::BOUND_UNBOUNDED == expr->get_upper().type_) {
        DATA_PRINTF(" UNBOUNDED ");
        if (expr->get_upper().is_preceding_) {
          DATA_PRINTF(" PRECEDING ");
        } else {
          DATA_PRINTF(" FOLLOWING ");
        }
      } else if (BoundType::BOUND_CURRENT_ROW == expr->get_upper().type_) {
        DATA_PRINTF(" CURRENT ROW ");
      } else if (BoundType::BOUND_INTERVAL == expr->get_upper().type_) {
        if (!expr->get_upper().is_nmb_literal_) {
          DATA_PRINTF(" INTERVAL ");
        }
        PRINT_EXPR(expr->get_upper().interval_expr_);
        if (!expr->get_upper().is_nmb_literal_) {
          DATA_PRINTF(" ");
          PRINT_EXPR(expr->get_upper().date_unit_expr_);
        }
        if (expr->get_upper().is_preceding_) {
          DATA_PRINTF(" PRECEDING ");
        } else {
          DATA_PRINTF(" FOLLOWING ");
        }
      } else { /* do nothing. */ }
      DATA_PRINTF(" AND ");
      if (BoundType::BOUND_UNBOUNDED == expr->get_lower().type_) {
        DATA_PRINTF(" UNBOUNDED ");
        if (expr->get_lower().is_preceding_) {
          DATA_PRINTF(" PRECEDING ");
        } else {
          DATA_PRINTF(" FOLLOWING ");
        }
      } else if (BoundType::BOUND_CURRENT_ROW == expr->get_lower().type_) {
        DATA_PRINTF(" CURRENT ROW ");
      } else if (BoundType::BOUND_INTERVAL == expr->get_lower().type_) {
        if (!expr->get_lower().is_nmb_literal_) {
          DATA_PRINTF(" INTERVAL ");
        }
        PRINT_EXPR(expr->get_lower().interval_expr_);
        if (!expr->get_lower().is_nmb_literal_) {
          DATA_PRINTF(" ");
          PRINT_EXPR(expr->get_lower().date_unit_expr_);
        }
        if (expr->get_lower().is_preceding_) {
          DATA_PRINTF(" PRECEDING ");
        } else {
          DATA_PRINTF(" FOLLOWING ");
        }
      } else { /* do nothing. */ }
    }
  }
  return ret;
}

int ObRawExprPrinter::print_date_unit(ObRawExpr *expr)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf_) || OB_ISNULL(pos_) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_ is NULL of buf_ is NULL or pos_ is NULL or expr is NULL", K(ret));
  } else {
    if (ObRawExpr::EXPR_CONST != expr->get_expr_class()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr class should be EXPR_CONST", K(ret), K(expr->get_expr_class()));
    } else {
      int64_t date_unit_type = DATE_UNIT_MAX;
      ObConstRawExpr *con_expr = static_cast<ObConstRawExpr*>(expr);
      if (OB_ISNULL(con_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("con_expr should not be NULL", K(ret));
      } else {
        con_expr->get_value().get_int(date_unit_type);
        DATA_PRINTF("%s", ob_date_unit_type_str(static_cast<ObDateUnitType>(date_unit_type)));
      }
    }
  }

  return ret;
}

int ObRawExprPrinter::print_get_format_unit(ObRawExpr *expr)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf_) || OB_ISNULL(pos_) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_ is NULL of buf_ is NULL or pos_ is NULL or expr is NULL", K(ret));
  } else {
    if (ObRawExpr::EXPR_CONST != expr->get_expr_class()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr class should be EXPR_CONST", K(ret), K(expr->get_expr_class()));
    } else {
      int64_t get_format_type = GET_FORMAT_MAX;
      ObConstRawExpr *con_expr = static_cast<ObConstRawExpr*>(expr);
      if (OB_ISNULL(con_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("con_expr should not be NULL", K(ret));
      } else {
        con_expr->get_value().get_int(get_format_type);
        DATA_PRINTF("%s", ob_get_format_unit_type_str(static_cast<ObGetFormatUnitType>(get_format_type)));
      }
    }
  }

  return ret;
}

int ObRawExprPrinter::pre_check_treat_opt(ObRawExpr *expr, bool &is_treat)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(buf_) || OB_ISNULL(pos_) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_ is NULL of buf_ is NULL or pos_ is NULL or expr is NULL", K(ret));
  } else if (ObRawExpr::EXPR_CONST != expr->get_expr_class()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr class should be EXPR_CONST ", K(ret), K(expr->get_expr_class()));
  } else {
    ObConstRawExpr *con_expr = static_cast<ObConstRawExpr*>(expr);
    ParseNode parse_node;
    if (OB_FAIL(con_expr->get_value().get_int(parse_node.value_))) {
      LOG_WARN("get int value failed", K(ret));
    } else {
      int16_t cast_type = parse_node.int16_values_[OB_NODE_CAST_TYPE_IDX];
      switch (cast_type) {
        case T_JSON: {
          is_treat = true;
          break;
        }
        default: {
          break;
        }
      }
    }
  }
  return ret;
}

int ObRawExprPrinter::print_type(const ObExprResType &dst_type)
{
  int ret = OB_SUCCESS;
  ObConstRawExpr *type_expr = NULL;
  ObArenaAllocator allocator("PrintType");
  ObRawExprFactory expr_factory(allocator);


  if (OB_FAIL(ObRawExprUtils::create_type_expr(expr_factory, type_expr,
                                               dst_type, /*avoid_zero_len*/true))) {
    LOG_WARN("create type expr failed", K(ret));
  } else if (OB_FAIL(print_cast_type(type_expr))) {
    LOG_WARN("failed to print cast type", K(ret));
  }
  return ret;
}

int ObRawExprPrinter::print_cast_type(ObRawExpr *expr)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf_) || OB_ISNULL(pos_) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_ is NULL of buf_ is NULL or pos_ is NULL or expr is NULL", K(ret));
  } else if (ObRawExpr::EXPR_CONST != expr->get_expr_class()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr class should be EXPR_CONST ", K(ret), K(expr->get_expr_class()));
  } else {
    ObConstRawExpr *con_expr = static_cast<ObConstRawExpr*>(expr);
    const ObLengthSemantics length_semantics = con_expr->get_accuracy().get_length_semantics();
    ParseNode parse_node;
    if (OB_FAIL(con_expr->get_value().get_int(parse_node.value_))) {
      LOG_WARN("get int value failed", K(ret));
    } else {
      int16_t cast_type = parse_node.int16_values_[OB_NODE_CAST_TYPE_IDX];
      switch (cast_type) {
      case T_CHAR: {
        int16_t collation = parse_node.int16_values_[OB_NODE_CAST_COLL_IDX];
        int32_t len = parse_node.int32_values_[OB_NODE_CAST_C_LEN_IDX];
        if (lib::is_oracle_mode()) {
          DATA_PRINTF("char(%d %s)", len, get_length_semantics_str(length_semantics));
        } else {
          if (len >= 0) {
            DATA_PRINTF("char(%d) charset %s", len, ObCharset::charset_name(
                        static_cast<ObCollationType>(collation)));
          } else {
            DATA_PRINTF("char charset %s", ObCharset::charset_name(
                        static_cast<ObCollationType>(collation)));
          }
        }
        break;
      }
      case T_VARCHAR: {
        int16_t collation = parse_node.int16_values_[OB_NODE_CAST_COLL_IDX];
        int32_t len = parse_node.int32_values_[OB_NODE_CAST_C_LEN_IDX];
        if (BINARY_COLLATION == collation) {
          // BINARY
          if (lib::is_oracle_mode()) {
            DATA_PRINTF("varbinary(%d)", len);
          } else {
            if (len >= 0) {
              DATA_PRINTF("binary(%d)", len);
            } else {
              DATA_PRINTF("binary");
            }
          }
        } else {
          // CHARACTER
          if (lib::is_oracle_mode()) {
            DATA_PRINTF("varchar2(%d %s)", len, get_length_semantics_str(length_semantics));
          } else {
            if (len > 0) {
              DATA_PRINTF("varchar(%d)", len);
            } else {
              DATA_PRINTF("character");
              LOG_WARN("varchar's length is zero, use character instead", K(len));
            }
          }
        }
        break;
      }
      case T_NVARCHAR2: {
        DATA_PRINTF("nvarchar2(%d)", parse_node.int32_values_[OB_NODE_CAST_C_LEN_IDX]);
        break;
      }
      case T_NCHAR: {
        DATA_PRINTF("nchar(%d)", parse_node.int32_values_[OB_NODE_CAST_C_LEN_IDX]);
        break;
      }
      case T_DATETIME: {
        //oracle mode treate date as datetime
        if (lib::is_oracle_mode()) {
          DATA_PRINTF("date");
        } else {
          int16_t scale = parse_node.int16_values_[OB_NODE_CAST_N_SCALE_IDX];
          if (scale >= 0) {
            DATA_PRINTF("datetime(%d)", scale);
          } else {
            DATA_PRINTF("datetime");
          }
        }
        break;
      }
      case T_DATE: {
        DATA_PRINTF("date");
        break;
      }
      case T_YEAR: {
        int16_t scale = parse_node.int16_values_[OB_NODE_CAST_N_SCALE_IDX];
        if (scale >= 0) {
          DATA_PRINTF("year(%d)", scale);
        } else {
          DATA_PRINTF("year");
        }
        break;
      }
      case T_TIME: {
        int16_t scale = parse_node.int16_values_[OB_NODE_CAST_N_SCALE_IDX];
        if (scale >= 0) {
          DATA_PRINTF("time(%d)", scale);
        } else {
          DATA_PRINTF("time");
        }
        break;
      }
      case T_NUMBER: {
        int16_t precision = parse_node.int16_values_[OB_NODE_CAST_N_PREC_IDX];
        int16_t scale = parse_node.int16_values_[OB_NODE_CAST_N_SCALE_IDX];
	      if (print_params_.for_dblink_ && lib::is_oracle_mode()) {
          // The numeric precision range of ob and oracle are different,
          // -1 and -85 are not supported for oracle
          if (PRECISION_UNKNOWN_YET == precision &&
              ORA_NUMBER_SCALE_UNKNOWN_YET == scale) {
            DATA_PRINTF("number");
          } else if (PRECISION_UNKNOWN_YET == precision &&
                     0 == scale) {
            DATA_PRINTF("int");
          } else {
            DATA_PRINTF("number(%d,%d)", precision, scale);
          }
        } else {
          DATA_PRINTF("number(%d,%d)", precision, scale);
        }
        break;
      }
      case T_NUMBER_FLOAT: {
        int16_t precision = parse_node.int16_values_[OB_NODE_CAST_N_PREC_IDX];
        DATA_PRINTF("float(%d)", precision);
        break;
      }
      case T_TINYINT:
      case T_SMALLINT:
      case T_MEDIUMINT:
      case T_INT32:
      case T_INT: {
        DATA_PRINTF("signed");
        break;
      }
      case T_UTINYINT:
      case T_USMALLINT:
      case T_UMEDIUMINT:
      case T_UINT32:
      case T_UINT64: {
        DATA_PRINTF("unsigned");
        break;
      }
      case T_INTERVAL_YM: {
        int16_t year_scale = parse_node.int16_values_[OB_NODE_CAST_N_SCALE_IDX];
        DATA_PRINTF("interval year(%d) to month", year_scale);
        break;
      }
      case T_INTERVAL_DS: {
        int16_t day_scale = parse_node.int16_values_[OB_NODE_CAST_N_PREC_IDX]; //day scale
        int16_t fs_scale = parse_node.int16_values_[OB_NODE_CAST_N_SCALE_IDX]; //second scale
        DATA_PRINTF("interval day(%d) to second(%d)", day_scale, fs_scale);
        break;
      }
      case T_TIMESTAMP_TZ: {
        int16_t scale = parse_node.int16_values_[OB_NODE_CAST_N_SCALE_IDX];
        if (scale >= 0) {
          DATA_PRINTF("timestamp(%d) with time zone", scale);
        } else {
          DATA_PRINTF("timestamp with time zone");
        }
        break;
      }
      case T_TIMESTAMP_LTZ: {
        int16_t scale = parse_node.int16_values_[OB_NODE_CAST_N_SCALE_IDX];
        if (scale >= 0) {
          DATA_PRINTF("timestamp(%d) with local time zone", scale);
        } else {
          DATA_PRINTF("timestamp with local time zone");
        }
        break;
      }
      case T_TIMESTAMP_NANO: {
        int16_t scale = parse_node.int16_values_[OB_NODE_CAST_N_SCALE_IDX];
        if (scale >= 0) {
          DATA_PRINTF("timestamp(%d)", scale);
        } else {
          DATA_PRINTF("timestamp");
        }
        break;
      }
      case T_RAW: {
        int32_t len = parse_node.int32_values_[OB_NODE_CAST_C_LEN_IDX];
        DATA_PRINTF("raw(%d)", len);
        break;
      }
      case T_FLOAT: {
        const char *type_str = lib::is_oracle_mode() ? "binary_float" : "float";
        DATA_PRINTF("%s", type_str);
        break;
      }
      case T_DOUBLE: {
        const char *type_str = lib::is_oracle_mode() ? "binary_double" : "double";
        DATA_PRINTF("%s", type_str);
        break;
      }
      case T_UROWID: {
        DATA_PRINTF("urowid(%d)", parse_node.int32_values_[OB_NODE_CAST_C_LEN_IDX]);
        break;
      }
      case T_LOB: {
        int16_t collation = parse_node.int16_values_[OB_NODE_CAST_COLL_IDX];
        if (BINARY_COLLATION == collation) {
          DATA_PRINTF("blob");
        } else {
          DATA_PRINTF("clob");
        }
        break;
      }
      case T_JSON: {
        DATA_PRINTF("json");
        break;
      }
      case T_LONGTEXT: {
        int16_t collation = parse_node.int16_values_[OB_NODE_CAST_COLL_IDX];
        if (BINARY_COLLATION == collation) {
          DATA_PRINTF("blob");
        } else {
          DATA_PRINTF("clob");
        }
        break;
      }
      case T_GEOMETRY: {
        ObGeoType geo_type = static_cast<ObGeoType>(parse_node.int16_values_[OB_NODE_CAST_GEO_TYPE_IDX]);
        switch (geo_type) {
          case ObGeoType::GEOMETRY: {
            DATA_PRINTF("geometry");
            break;
          }
          case ObGeoType::POINT: {
            DATA_PRINTF("point");
            break;
          }
          case ObGeoType::LINESTRING: {
            DATA_PRINTF("linestring");
            break;
          }
          case ObGeoType::POLYGON: {
            DATA_PRINTF("polygon");
            break;
          }
          case ObGeoType::MULTIPOINT: {
            DATA_PRINTF("multipoint");
            break;
          }
          case ObGeoType::MULTILINESTRING: {
            DATA_PRINTF("multilinestring");
            break;
          }
          case ObGeoType::MULTIPOLYGON: {
            DATA_PRINTF("multipolygon");
            break;
          }
          case ObGeoType::GEOMETRYCOLLECTION: {
            DATA_PRINTF("geometrycollection");
            break;
          }
          case ObGeoType::GEOTYPEMAX: {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid cast geo sub type", K(ret), K(cast_type), K(geo_type));
            break;
          }
          default: {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unknown cast geo sub type", K(ret), K(cast_type), K(geo_type));
            break;
          }
        }
        break;
      }
      case T_EXTEND: {
        const int udt_id = con_expr->get_udt_id();
        const uint64_t dest_tenant_id = pl::get_tenant_id_by_object_id(udt_id);
        const share::schema::ObUDTTypeInfo *dest_info = NULL;
        if (OB_ISNULL(schema_guard_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null", K(ret), K(lbt()));
        } else if (OB_FAIL(schema_guard_->get_udt_info(dest_tenant_id, udt_id, dest_info))) {
          LOG_WARN("failed to get udt info", K(ret));
        } else {
          PRINT_IDENT_WITH_QUOT(dest_info->get_type_name());
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unknown cast type", K(ret), K(cast_type));
        break;
      }
      } // end switch
    }
  }

  return ret;
}

int ObRawExprPrinter::print_xml_parse_expr(ObSysFunRawExpr *expr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(4 != expr->get_param_count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected param count of expr to type", K(ret), KPC(expr));
  } else {
    DATA_PRINTF("xmlparse(");
    if (!static_cast<ObConstRawExpr*>(expr->get_param_expr(0))->get_value().is_int()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("doc type value isn't int value");
    } else {
      int64_t type = static_cast<ObConstRawExpr*>(expr->get_param_expr(0))->get_value().get_int();
      switch (type) {
        case 0:
          DATA_PRINTF("DOCUMENT ");
          break;
        case 1:
          DATA_PRINTF("CONTENT ");
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid doc type value", K(type), K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      PRINT_EXPR(expr->get_param_expr(1));
      if (!static_cast<ObConstRawExpr*>(expr->get_param_expr(2))->get_value().is_int()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("doc type value isn't int value");
      } else {
        int64_t format_type = static_cast<ObConstRawExpr*>(expr->get_param_expr(2))->get_value().get_int();
        switch (format_type) {
          case 0:
            // do nothing
            break;
          case 1:
            DATA_PRINTF(" WELLFORMED");
            break;
          default:
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid format type value", K(format_type), K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        DATA_PRINTF(")");
      }
    }
  }
  return ret;
}

int ObRawExprPrinter::print_xml_serialize_expr(ObSysFunRawExpr *expr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(10 != expr->get_param_count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unexpected param count of expr", K(ret), KPC(expr));
  } else {
    DATA_PRINTF("xmlserialize(");
    // doc_type
    if (!static_cast<ObConstRawExpr*>(expr->get_param_expr(0))->get_value().is_int()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("doc type value isn't int value");
    } else {
      int64_t type = static_cast<ObConstRawExpr*>(expr->get_param_expr(0))->get_value().get_int();
      switch (type) {
        case 0:
          DATA_PRINTF("DOCUMENT ");
          break;
        case 1:
          DATA_PRINTF("CONTENT ");
          break;
        default:
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid doc type value", K(type), K(ret));
      }
    }
    // value_expr
    PRINT_EXPR(expr->get_param_expr(1));
    // [as datatype]
    if (OB_SUCC(ret)) {
      DATA_PRINTF(" as ");
      if (OB_SUCC(ret)) {
        if (OB_FAIL(print_cast_type(expr->get_param_expr(2)))) {
          LOG_WARN("fail to print cast_type", K(ret));
        }
      }
    }
    // [encoding charset]
    if (OB_SUCC(ret)) {
      ObExprOperatorType expr_type = expr->get_param_expr(3)->get_expr_type();
      int64_t encoding_opt = static_cast<ObConstRawExpr*>(expr->get_param_expr(3))->get_value().get_int();
      if (expr_type == T_INT) {
        if (encoding_opt == 0) {
        } else if (encoding_opt == 1) {
          DATA_PRINTF(" encoding ");
          PRINT_EXPR(expr->get_param_expr(4));
        } else {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid encoding opt value", K(encoding_opt), K(ret));
        }
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid expr type", K(ret), K(expr_type), K(encoding_opt));
      }
    }
    // [version string literal]
    if (OB_SUCC(ret)) {
      ObExprOperatorType expr_type = expr->get_param_expr(5)->get_expr_type();
      int64_t version_opt = static_cast<ObConstRawExpr*>(expr->get_param_expr(5))->get_value().get_int();
      if (expr_type == T_INT) {
        if (version_opt == 0) {
        } else if (version_opt == 1) {
          DATA_PRINTF(" version ");
          PRINT_EXPR(expr->get_param_expr(6));
        } else {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid version opt value", K(version_opt), K(ret));
        }
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid expr type", K(ret), K(expr_type), K(version_opt));
      }
    }
    // [indent]
    if (OB_SUCC(ret)) {
      ObExprOperatorType expr_type_1 = expr->get_param_expr(7)->get_expr_type();
      ObExprOperatorType expr_type_2 = expr->get_param_expr(8)->get_expr_type();
      int64_t indent_type = static_cast<ObConstRawExpr*>(expr->get_param_expr(7))->get_value().get_int();
      int64_t value = static_cast<ObConstRawExpr*>(expr->get_param_expr(8))->get_value().get_int();
      if (expr_type_1 == T_INT && expr_type_2 == T_INT) {
        if (indent_type == 4) {
        } else if (indent_type == 1) {
          DATA_PRINTF(" no indent ");
        } else if (indent_type == 2) {
          DATA_PRINTF(" indent ");
        } else if (indent_type == 3) {
          DATA_PRINTF(" indent size = %ld", value);
        } else {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid indent value", K(value), K(ret));
        }
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid indent type", K(ret), K(expr_type_1), K(expr_type_2));
      }
    }

    // [hide|show] defaults
    if (OB_SUCC(ret)) {
      ObExprOperatorType expr_type = expr->get_param_expr(9)->get_expr_type();
      int64_t value = static_cast<ObConstRawExpr*>(expr->get_param_expr(9))->get_value().get_int();
      if (expr_type == T_INT) {
        if (value == 0) {
        } else if (value == 1) {
          DATA_PRINTF(" hide defaults ");
        } else if (value == 2) {
          DATA_PRINTF(" show defaults ");
        } else {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid indent value", K(value), K(ret));
        }
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid expr type", K(expr_type), K(ret));
      }
    }
    DATA_PRINTF(")");
  }

  return ret;
}


int ObRawExprPrinter::print_xml_element_expr(ObSysFunRawExpr *expr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(3 > expr->get_param_count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected param count of expr", K(ret), KPC(expr));
  } else {
    DATA_PRINTF("xmlelement(");
    if (!static_cast<ObConstRawExpr*>(expr->get_param_expr(0))->get_value().is_int()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("doc type value isn't int value");
    } else {
      int64_t type = static_cast<ObConstRawExpr*>(expr->get_param_expr(0))->get_value().get_int();
      switch (type) {
        case 0:
          DATA_PRINTF("NOENTITYESCAPING ");
          break;
        case 1:
          DATA_PRINTF("ENTITYESCAPING ");
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid doc type value", K(type), K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      int64_t format_type = 0;
      if (!static_cast<ObConstRawExpr*>(expr->get_param_expr(1))->get_value().is_int()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("doc type value isn't int value");
      } else {
        format_type = static_cast<ObConstRawExpr*>(expr->get_param_expr(1))->get_value().get_int();
        switch (format_type) {
          case 0:
            DATA_PRINTF("NAME ");
            break;
          case 1:
            DATA_PRINTF("EVALNAME ");
            break;
          default:
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid format type value", K(format_type), K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        int64_t cur_pos = *pos_;
        PRINT_EXPR(expr->get_param_expr(2));
        int64_t new_pos = *pos_;

        if (OB_SUCC(ret) && format_type == 0) {
          if (buf_[cur_pos] == '\'') {
             buf_[cur_pos] = '"';
          }
          if (buf_[new_pos - 1] == '\'') {
             buf_[new_pos - 1] = '"';
          }
        }

        for (int i = 3; i < expr->get_param_count() && OB_SUCC(ret); i++) {
          DATA_PRINTF(",");
          PRINT_EXPR(expr->get_param_expr(i));
        }
        DATA_PRINTF(")");
      }
    }
  }
  return ret;
}

int ObRawExprPrinter::print_xml_agg_expr(ObAggFunRawExpr *expr)
{
  INIT_SUCC(ret);
  if (OB_UNLIKELY(2 == expr->get_real_param_count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected param count of expr", K(ret), KPC(expr));
  } else {
    DATA_PRINTF("xmlagg(");
    PRINT_EXPR(expr->get_param_expr(0));
    if (OB_NOT_NULL(expr->get_param_expr(1))) {
      const ObIArray<OrderItem> &order_items = expr->get_order_items();
      int64_t order_item_size = order_items.count();
      if (order_item_size > 0) {
        DATA_PRINTF(" order by ");
        for (int64_t i = 0; OB_SUCC(ret) && i < order_item_size; ++i) {
          const OrderItem &order_item = order_items.at(i);
          PRINT_EXPR(order_item.expr_);
          if (OB_SUCC(ret)) {
            if (lib::is_mysql_mode()) {
              if (is_descending_direction(order_item.order_type_)) {
                DATA_PRINTF(" desc ");
              }
            } else if (order_item.order_type_ == NULLS_FIRST_ASC) {
              DATA_PRINTF(" asc nulls first ");
            } else if (order_item.order_type_ == NULLS_LAST_ASC) {//use default value
              /*do nothing*/
            } else if (order_item.order_type_ == NULLS_FIRST_DESC) {//use default value
              DATA_PRINTF(" desc ");
            } else if (order_item.order_type_ == NULLS_LAST_DESC) {
              DATA_PRINTF(" desc nulls last ");
            } else {/*do nothing*/}
          }
          DATA_PRINTF(",");
        }
        if (OB_SUCC(ret)) {
          --*pos_;
        }
      }
    }
    DATA_PRINTF(")");
  }
  return ret;
}

int ObRawExprPrinter::print_xml_attributes_expr(ObSysFunRawExpr *expr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(3 > expr->get_param_count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected param count of expr", K(ret), KPC(expr));
  } else if (OB_UNLIKELY(expr->get_param_count() % 2 != 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected param count of expr", K(ret), KPC(expr));
  } else {
    DATA_PRINTF("xmlattributes(");
    if (!static_cast<ObConstRawExpr*>(expr->get_param_expr(0))->get_value().is_int()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("doc type value isn't int value");
    } else {
      int64_t type = static_cast<ObConstRawExpr*>(expr->get_param_expr(0))->get_value().get_int();
      switch (type) {
        case 0:
          DATA_PRINTF("NOENTITYESCAPING ");
          break;
        case 1:
          DATA_PRINTF("ENTITYESCAPING ");
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid doc type value", K(type), K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (!static_cast<ObConstRawExpr*>(expr->get_param_expr(1))->get_value().is_int()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("doc type value isn't int value");
      } else {
        int64_t format_type = static_cast<ObConstRawExpr*>(expr->get_param_expr(1))->get_value().get_int();
        switch (format_type) {
          case 0:
            DATA_PRINTF("NOSCHEMACHECK ");
            break;
          case 1:
            DATA_PRINTF("SCHEMACHECK ");
            break;
          default:
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid format type value", K(format_type), K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        for (int i = 2; i < expr->get_param_count() && OB_SUCC(ret); i += 2) {
          if (i > 2) {
            DATA_PRINTF(",");
          }
          PRINT_EXPR(expr->get_param_expr(i));
          ObObj attr_key_obj = static_cast<ObConstRawExpr*>(expr->get_param_expr(i + 1))->get_value();
          ObItemType expr_type = expr->get_param_expr(i + 1)->get_expr_type();
          if ((T_VARCHAR != expr_type && T_CHAR != expr_type) ||
              attr_key_obj.get_type() == ObObjType::ObUnknownType) {
            DATA_PRINTF(" as evalname ");
            PRINT_EXPR(expr->get_param_expr(i + 1));
          } else if (!attr_key_obj.get_string().empty()) {
            // While the result obtained during anti-spelling has been parsed,
            // so adding all double quotes can achieve the desired result
            DATA_PRINTF(" as \"%.*s\"", LEN_AND_PTR(attr_key_obj.get_string()));
          }
        }
        DATA_PRINTF(")");
      }
    }
  }
  return ret;
}

} //end of namespace sql
} //end of namespace oceanbase
