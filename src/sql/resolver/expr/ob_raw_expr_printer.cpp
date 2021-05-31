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
#include "lib/string/ob_sql_string.h"

namespace oceanbase {
using namespace common;
namespace sql {

ObRawExprPrinter::ObRawExprPrinter()
    : buf_(NULL),
      buf_len_(0),
      pos_(NULL),
      scope_(T_NONE_SCOPE),
      only_column_namespace_(false),
      is_inited_(false),
      tz_info_(NULL),
      print_params_()
{}

ObRawExprPrinter::ObRawExprPrinter(char* buf, int64_t buf_len, int64_t* pos, ObObjPrintParams print_params)
    : buf_(buf),
      buf_len_(buf_len),
      pos_(pos),
      scope_(T_NONE_SCOPE),
      only_column_namespace_(false),
      is_inited_(true),
      tz_info_(NULL),
      print_params_(print_params)
{}

ObRawExprPrinter::~ObRawExprPrinter()
{}

void ObRawExprPrinter::init(char* buf, int64_t buf_len, int64_t* pos, ObObjPrintParams print_params)
{
  buf_ = buf;
  buf_len_ = buf_len;
  pos_ = pos;
  scope_ = T_NONE_SCOPE;
  print_params_ = print_params;
  is_inited_ = true;
}

int ObRawExprPrinter::do_print(ObRawExpr* expr, ObStmtScope scope, bool only_column_namespace)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited!", K(ret));
  } else if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr should not be NULL", K(ret));
  } else {
    scope_ = scope;
    only_column_namespace_ = only_column_namespace;
    PRINT_EXPR(expr);
    //    if (!expr->get_alias_column_name().empty() && T_FIELD_LIST_SCOPE == scope_) {
    //      const ObString &alias_name = expr->get_alias_column_name();
    //      DATA_PRINTF(" AS ");
    //      DATA_PRINTF("`%.*s`", LEN_AND_PTR(alias_name));
    //    }
  }
  return ret;
}

int ObRawExprPrinter::print(ObRawExpr* expr)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf_) || OB_ISNULL(pos_) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_ is NULL of buf_ is NULL or pos_ is NULL or expr is NULL", K(ret));
  } else if (!expr->get_alias_column_name().empty() && !expr->is_column_ref_expr() && !expr->is_aggr_expr() &&
             !expr->is_pseudo_column_expr() && scope_ != T_DBLINK_SCOPE && scope_ != T_FIELD_LIST_SCOPE &&
             scope_ != T_GROUP_SCOPE && scope_ != T_WHERE_SCOPE) {
    // expr is a alias column ref
    // alias column target list
    DATA_PRINTF(lib::is_oracle_mode() ? "\"%.*s\"" : "`%.*s`", LEN_AND_PTR(expr->get_alias_column_name()));
  } else if (scope_ == T_DBLINK_SCOPE && OB_NOT_NULL(expr->get_orig_expr())) {
    if (OB_FAIL(print(expr->get_orig_expr()))) {
      LOG_WARN("fail to print orig expr", K(ret));
    }
  } else {
    switch (expr->get_expr_class()) {
      case ObRawExpr::EXPR_CONST: {
        ObConstRawExpr* con_expr = static_cast<ObConstRawExpr*>(expr);
        PRINT_EXPR(con_expr);
        break;
      }
      case ObRawExpr::EXPR_QUERY_REF: {
        ObQueryRefRawExpr* una_expr = static_cast<ObQueryRefRawExpr*>(expr);
        PRINT_EXPR(una_expr);
        break;
      }
      case ObRawExpr::EXPR_COLUMN_REF: {
        ObColumnRefRawExpr* bin_expr = static_cast<ObColumnRefRawExpr*>(expr);
        PRINT_EXPR(bin_expr);
        break;
      }
      case ObRawExpr::EXPR_OPERATOR: {
        ObOpRawExpr* op_expr = static_cast<ObOpRawExpr*>(expr);
        PRINT_EXPR(op_expr);
        break;
      }
      case ObRawExpr::EXPR_CASE_OPERATOR: {
        ObCaseOpRawExpr* case_expr = static_cast<ObCaseOpRawExpr*>(expr);
        PRINT_EXPR(case_expr);
        break;
      }
      case ObRawExpr::EXPR_AGGR: {
        ObAggFunRawExpr* agg_expr = static_cast<ObAggFunRawExpr*>(expr);
        PRINT_EXPR(agg_expr);
        break;
      }
      case ObRawExpr::EXPR_SYS_FUNC: {
        ObSysFunRawExpr* sys_expr = static_cast<ObSysFunRawExpr*>(expr);
        PRINT_EXPR(sys_expr);
        break;
      }
      case ObRawExpr::EXPR_UDF: {
        ret = OB_NOT_SUPPORTED;
        break;
      }
      case ObRawExpr::EXPR_WINDOW: {
        ObWinFunRawExpr* win_expr = static_cast<ObWinFunRawExpr*>(expr);
        PRINT_EXPR(win_expr);
        break;
      }
      case ObRawExpr::EXPR_DOMAIN_INDEX: {
        ObFunMatchAgainst* ma_expr = static_cast<ObFunMatchAgainst*>(expr);
        PRINT_EXPR(ma_expr);
        break;
      }
      case ObRawExpr::EXPR_PSEUDO_COLUMN: {
        ObPseudoColumnRawExpr* pse_expr = static_cast<ObPseudoColumnRawExpr*>(expr);
        PRINT_EXPR(pse_expr);
        break;
      }
      case ObRawExpr::EXPR_SET_OP: {
        ObSetOpRawExpr* set_op_expr = static_cast<ObSetOpRawExpr*>(expr);
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

int ObRawExprPrinter::print(ObConstRawExpr* expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf_) || OB_ISNULL(pos_) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_ is NULL of buf_ is NULL or pos_ is NULL or expr is NULL", K(ret));
  } else if (T_DBLINK_SCOPE == scope_ && T_QUESTIONMARK == expr->get_expr_type()) {
    if (OB_FAIL(ObLinkStmtParam::write(buf_, buf_len_, *pos_, expr->get_value().get_unknown()))) {
      LOG_WARN("fail to write param to buf", K(ret));
    }
  } else if (expr->get_literal_prefix().empty()) {
    // for empty string in Oracle mode , we should use char-type obj to print
    if (expr->get_value().is_null() && ObCharType == expr->get_expr_obj_meta().get_type()) {
      ObObj empty_string = expr->get_value();
      empty_string.set_meta_type(expr->get_expr_obj_meta());
      if (OB_FAIL(empty_string.print_sql_literal(buf_, buf_len_, *pos_, print_params_))) {
        LOG_WARN("fail to print sql literal", K(ret));
      }
    } else if (OB_FAIL(expr->get_value().print_sql_literal(buf_, buf_len_, *pos_, print_params_))) {
      LOG_WARN("fail to print sql literal", K(ret));
    }
  } else if (expr->get_literal_prefix() == ORALCE_LITERAL_PREFIX_DATE) {
    int32_t tmp_date = 0;
    if (OB_FAIL(databuff_printf(buf_, buf_len_, *pos_, "%s '", ORALCE_LITERAL_PREFIX_DATE))) {
      LOG_WARN("fail to print literal prefix", K(ret));
    } else if (OB_FAIL(ObTimeConverter::datetime_to_date(expr->get_value().get_datetime(), NULL, tmp_date))) {
      LOG_WARN("fail to datetime_to_date", "datetime", expr->get_value().get_datetime(), K(ret));
    } else if (OB_FAIL(ObTimeConverter::date_to_str(tmp_date, buf_, buf_len_, *pos_))) {
      LOG_WARN("fail to date_to_str", K(tmp_date), K(ret));
    } else if (OB_FAIL(databuff_printf(buf_, buf_len_, *pos_, "'"))) {
      LOG_WARN("fail to print single quote", K(ret));
    }
  } else if (expr->get_literal_prefix().prefix_match_ci(ORACLE_LITERAL_PREFIX_INTERVAL) ||
             (expr->get_value().is_oracle_decimal())) {
    if (OB_FAIL(databuff_printf(
            buf_, buf_len_, *pos_, "%.*s", expr->get_literal_prefix().length(), expr->get_literal_prefix().ptr()))) {
      LOG_WARN("fail to print literal suffix", K(ret));
    }
  } else {
    if (OB_FAIL(databuff_printf(buf_, buf_len_, *pos_, "%.*s ", LEN_AND_PTR(expr->get_literal_prefix())))) {
      LOG_WARN("fail to print literal prefix", K(ret));
    } else if (!expr->is_date_unit() &&
               OB_FAIL(expr->get_value().print_sql_literal(buf_, buf_len_, *pos_, print_params_))) {
      LOG_WARN("fail to print sql literal", K(ret));
    }
  }
  return ret;
}

int ObRawExprPrinter::print(ObQueryRefRawExpr* expr)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf_) || OB_ISNULL(pos_) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_ is NULL of buf_ is NULL or pos_ is NULL or expr is NULL", K(ret));
  } else {
    if (expr->is_cursor()) {
      DATA_PRINTF("CURSOR");
    }
    if (OB_SUCC(ret)) {
      ObStmt* stmt = expr->get_ref_stmt();
      if (OB_ISNULL(stmt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("stmt is NULL", K(ret));
      } else {
        if (stmt->is_select_stmt()) {
          ObIArray<ObString>* column_list = NULL;
          bool is_set_subquery = false;
          ObSelectStmtPrinter stmt_printer(
              buf_, buf_len_, pos_, static_cast<ObSelectStmt*>(stmt), print_params_, column_list, is_set_subquery);
          if (OB_FAIL(stmt_printer.do_print())) {
            LOG_WARN("fail to print ref query", K(ret));
          }
        }
      }
    }
  }

  return ret;
}

int ObRawExprPrinter::print(ObColumnRefRawExpr* expr)
{
  ObArenaAllocator allocator("PrintRefColumn");
  int ret = OB_SUCCESS;
  bool is_oracle_mode = lib::is_oracle_mode();
  if (OB_ISNULL(buf_) || OB_ISNULL(pos_) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_ is NULL of buf_ is NULL or pos_ is NULL or expr is NULL", K(ret));
  } else if (expr->is_generated_column() && expr->is_hidden_column() && OB_NOT_NULL(expr->get_dependant_expr())) {
    PRINT_EXPR(expr->get_dependant_expr());
  } else {
    ObArenaAllocator arena_alloc;
    ObString col_name;
    if (OB_FAIL(ObSQLUtils::generate_new_name_with_escape_character(
            allocator, expr->get_column_name(), col_name, is_oracle_mode))) {
      LOG_WARN("fail to generate new name with escape character", K(ret));
    } else if (is_oracle_mode &&
               OB_FAIL(ObSelectStmtPrinter::remove_double_quotation_for_string(col_name, arena_alloc))) {
      LOG_WARN("failed to remove double quotation for string", K(ret));
    } else if (OB_FAIL(ObCharset::charset_convert(
                   allocator, col_name, CS_TYPE_UTF8MB4_BIN, print_params_.cs_type_, col_name))) {
      LOG_WARN("fail to convert charset", K(ret));
    } else if (expr->is_cte_generated_column()) {
      ObString table_name = expr->get_synonym_name().empty() ? expr->get_table_name() : expr->get_synonym_name();
      // note: expr's table_name is equal to alias if table's alias is not empty,
      CONVERT_CHARSET_FOR_RPINT(allocator, table_name);
      DATA_PRINTF(
          is_oracle_mode ? "\"%.*s\".\"%.*s\"" : "`%.*s`.`%.*s`", LEN_AND_PTR(table_name), LEN_AND_PTR(col_name));
    } else if (OB_UNLIKELY(only_column_namespace_)) {
      DATA_PRINTF(is_oracle_mode ? "\"%.*s\"" : "`%.*s`", LEN_AND_PTR(col_name));
    } else {
      if (!expr->get_synonym_name().empty() && !expr->get_synonym_db_name().empty()) {
        ObString synonyn_db_name = expr->get_synonym_db_name();
        CONVERT_CHARSET_FOR_RPINT(allocator, synonyn_db_name);
        DATA_PRINTF(is_oracle_mode ? "\"%.*s\"." : "`%.*s`.", LEN_AND_PTR(synonyn_db_name));
      } else if (expr->get_database_name().length() > 0) {
        ObString database_name = expr->get_database_name();
        CONVERT_CHARSET_FOR_RPINT(allocator, database_name);
        DATA_PRINTF(is_oracle_mode ? "\"%.*s\"." : "`%.*s`.", LEN_AND_PTR(database_name));
      }
      ObString table_name = expr->get_synonym_name().empty() ? expr->get_table_name() : expr->get_synonym_name();
      CONVERT_CHARSET_FOR_RPINT(allocator, table_name);
      // note: expr's table_name is equal to alias if table's alias is not empty,
      if (!table_name.empty()) {
        DATA_PRINTF(
            is_oracle_mode ? "\"%.*s\".\"%.*s\"" : "`%.*s`.`%.*s`", LEN_AND_PTR(table_name), LEN_AND_PTR(col_name));
      } else {
        // oracle allow derived table without alias name, table_name is empty here.
        // e.g.:  select * from (select 1 from dual)
        DATA_PRINTF(is_oracle_mode ? "\"%.*s\"" : "`%.*s`", LEN_AND_PTR(col_name));
      }
    }
  }

  return ret;
}

int ObRawExprPrinter::print(ObOpRawExpr* expr)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf_) || OB_ISNULL(pos_) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_ is NULL of buf_ is NULL or pos_ is NULL or expr is NULL", K(ret));
  } else {
    ObString symbol("");
    ObItemType type = expr->get_expr_type();
    switch (type) {
      case T_OP_PRIOR:
        SET_SYMBOL_IF_EMPTY("prior");
      case T_OP_CONNECT_BY_ROOT:
        SET_SYMBOL_IF_EMPTY("connect_by_root");
      case T_OP_NOT:
        SET_SYMBOL_IF_EMPTY("not");
      case T_OP_NOT_EXISTS:
        SET_SYMBOL_IF_EMPTY("not exists");
      case T_OP_EXISTS: {
        SET_SYMBOL_IF_EMPTY("exists");
        if (1 != expr->get_param_count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expr param count should be equal 1 ", K(ret), K(expr->get_param_count()));
        } else {
          DATA_PRINTF("%.*s", LEN_AND_PTR(symbol));
          DATA_PRINTF("(");
          PRINT_EXPR(expr->get_param_expr(0));
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
      case T_OP_ADD:  // a op b
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
      case T_OP_SQ_LE:  // subquery
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
      case T_OP_NE:
      case T_OP_SQ_NE:
        SET_SYMBOL_IF_EMPTY("<>");
      case T_OP_IN:  // in sub-query wille be rewrited as expr = ANY(sub-query)
        SET_SYMBOL_IF_EMPTY("in");
      case T_OP_NOT_IN:
        SET_SYMBOL_IF_EMPTY("not in");  // not in sub-query wille be rewrited as expr != all(sub-query)
      case T_OP_BIT_OR:
        SET_SYMBOL_IF_EMPTY("|");
      case T_OP_BIT_XOR:
      case T_OP_XOR:
        SET_SYMBOL_IF_EMPTY("^");
      case T_OP_BIT_AND:
        SET_SYMBOL_IF_EMPTY("&");
      case T_OP_REGEXP: {
        SET_SYMBOL_IF_EMPTY("regexp");
        case T_OP_CNN:
          SET_SYMBOL_IF_EMPTY("||");
          // case T_OP_DATE_ADD: {
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
        if (3 != expr->get_param_count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expr param count should be equal 3", K(ret), K(expr->get_param_count()));
        } else {
          DATA_PRINTF("(");
          PRINT_EXPR(expr->get_param_expr(0));
          DATA_PRINTF(" %.*s ", LEN_AND_PTR(symbol));
          if (OB_SUCC(ret)) {
            ObConstRawExpr* con_expr = static_cast<ObConstRawExpr*>(expr->get_param_expr(1));
            if (OB_ISNULL(con_expr)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("con_expr should not be NULL", K(ret));
            } else {
              ObObj& obj = con_expr->get_value();
              ObObjType type = obj.get_type();
              if (ObNullType == type) {
                DATA_PRINTF("null");
              } else if (ObTinyIntType == type) {
                DATA_PRINTF("%s", obj.get_bool() ? "true" : "false");
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
          DATA_PRINTF(")");
        }
        break;
      }
      case T_OBJ_ACCESS_REF: {
        ret = OB_NOT_SUPPORTED;
        break;
      }
      case T_FUN_PL_ASSOCIATIVE_INDEX: {
        ret = OB_NOT_SUPPORTED;
        break;
      }
      case T_FUN_PL_INTEGER_CHECKER: {
        ret = OB_NOT_SUPPORTED;
        break;
      }
      case T_OP_ASSIGN: {
        SET_SYMBOL_IF_EMPTY(":=");
        if (OB_UNLIKELY(2 != expr->get_param_count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("param count should be equal 2", K(ret), K(expr->get_param_count()));
        } else {
          DATA_PRINTF("(");
          if (OB_ISNULL(expr->get_param_expr(0)) || !expr->get_param_expr(0)->is_const_expr() ||
              !static_cast<ObConstRawExpr*>(expr->get_param_expr(0))->get_value().is_varchar()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid user variable name", K(ret), K(expr->get_param_expr(0)));
          } else {
            ObString func_name = static_cast<ObConstRawExpr*>(expr->get_param_expr(0))->get_value().get_varchar();
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
      case T_OP_BOOL:
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
      case T_OP_COLL_PRED: {
        SET_SYMBOL_IF_EMPTY("collection predicate");
        break;
      }
      case T_FUN_PL_GET_CURSOR_ATTR: {
        ret = OB_NOT_SUPPORTED;
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unknown expr type", K(ret), "type", get_type_name(type));
        break;
      }
    }  // end switch
  }

  return ret;
}

int ObRawExprPrinter::print(ObSetOpRawExpr* expr)
{
  if (OB_ISNULL(expr)) {
    LOG_WARN("expr is NULL", K(expr));
  } else {
    LOG_WARN("set op expr must be alias", K(*expr));
  }
  return OB_ERR_UNEXPECTED;
}

int ObRawExprPrinter::print(ObCaseOpRawExpr* expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf_) || OB_ISNULL(pos_) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_ is NULL of buf_ is NULL or pos_ is NULL or expr is NULL", K(ret));
  } else {
    DATA_PRINTF("(case");
    if (OB_SUCC(ret)) {
      ObRawExpr* arg_expr = expr->get_arg_param_expr();
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
        ObRawExpr* default_expr = expr->get_default_param_expr();
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

int ObRawExprPrinter::print(ObAggFunRawExpr* expr)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf_) || OB_ISNULL(pos_) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_ is NULL of buf_ is NULL or pos_ is NULL or expr is NULL", K(ret));
  } else {
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
      case T_FUN_WM_CONCAT: {
        SET_SYMBOL_IF_EMPTY("wm_concat");
        DATA_PRINTF("%.*s(", LEN_AND_PTR(symbol));
        // distinct, default 'all', not print
        if (OB_SUCC(ret)) {
          if (expr->is_param_distinct()) {
            DATA_PRINTF("distinct ");
          }
        }
        if (OB_SUCC(ret)) {
          if (0 == expr->get_param_count()) {
            if (T_FUN_COUNT != type) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("expr type should be T_FUN_COUNT ", K(ret), K(type));
            } else {
              // count(*) -> count(0)
              DATA_PRINTF("0");
            }
          } else if (T_FUN_MEDIAN == type) {
            // median add an order_item_expr when resolve, here not need to do this
            PRINT_EXPR(expr->get_param_expr(0));
          } else {
            for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
              PRINT_EXPR(expr->get_param_expr(i));
              DATA_PRINTF(",");
            }
            if (OB_SUCC(ret)) {
              --*pos_;
            }
          }
        }
        DATA_PRINTF(")");
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
        if (share::is_oracle_mode()) {
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
        if (share::is_oracle_mode() && type == T_FUN_GROUP_CONCAT && 0 == expr->get_order_items().count()) {
          /* do nothing */
        } else {
          if (share::is_oracle_mode()) {
            DATA_PRINTF(") within group (");
          }
          // order by
          if (OB_SUCC(ret)) {
            const ObIArray<OrderItem>& order_items = expr->get_order_items();
            int64_t order_item_size = order_items.count();
            if (order_item_size > 0) {
              DATA_PRINTF(" order by ");
              for (int64_t i = 0; OB_SUCC(ret) && i < order_item_size; ++i) {
                const OrderItem& order_item = order_items.at(i);
                PRINT_EXPR(order_item.expr_);
                if (OB_SUCC(ret)) {
                  if (share::is_mysql_mode()) {
                    if (is_descending_direction(order_item.order_type_)) {
                      DATA_PRINTF(" desc ");
                    }
                  } else if (order_item.order_type_ == NULLS_FIRST_ASC) {
                    DATA_PRINTF(" asc nulls first ");
                  } else if (order_item.order_type_ == NULLS_LAST_ASC) {  // use default value
                    /*do nothing*/
                  } else if (order_item.order_type_ == NULLS_FIRST_DESC) {  // use default value
                    DATA_PRINTF(" desc ");
                  } else if (order_item.order_type_ == NULLS_LAST_DESC) {
                    DATA_PRINTF(" desc nulls last ");
                  } else { /*do nothing*/
                  }
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
        if (0 == expr->get_real_param_count()) {  // count(*) keep(...)
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
          const ObIArray<OrderItem>& order_items = expr->get_order_items();
          if (order_items.count() > 0) {
            DATA_PRINTF("keep(dense_rank first order by ");
            for (int64_t i = 0; OB_SUCC(ret) && i < order_items.count(); ++i) {
              const OrderItem& order_item = order_items.at(i);
              PRINT_EXPR(order_item.expr_);
              if (OB_SUCC(ret)) {
                if (share::is_mysql_mode()) {
                  if (is_descending_direction(order_item.order_type_)) {
                    DATA_PRINTF(" desc ");
                  }
                } else if (order_item.order_type_ == NULLS_FIRST_ASC) {
                  DATA_PRINTF(" asc nulls first ");
                } else if (order_item.order_type_ == NULLS_LAST_ASC) {  // use default value
                  /*do nothing*/
                } else if (order_item.order_type_ == NULLS_FIRST_DESC) {  // use default value
                  DATA_PRINTF(" desc ");
                } else if (order_item.order_type_ == NULLS_LAST_DESC) {
                  DATA_PRINTF(" desc nulls last ");
                } else { /*do nothing*/
                }
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
    }  // end switch
  }

  return ret;
}

int ObRawExprPrinter::print(ObSysFunRawExpr* expr)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf_) || OB_ISNULL(pos_) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_ is NULL of buf_ is NULL or pos_ is NULL or expr is NULL", K(ret));
  } else {
    ObString func_name = expr->get_func_name();
    switch (expr->get_expr_type()) {
      case T_FUN_SYS_UTC_TIMESTAMP: {
        const int16_t scale = static_cast<int16_t>(expr->get_result_type().get_scale());
        if (scale > 0) {
          DATA_PRINTF("%.*s(%d)", LEN_AND_PTR(func_name), scale);
        } else {
          DATA_PRINTF("%.*s()", LEN_AND_PTR(func_name));
        }
        break;
      }
      case T_FUN_SYS_CAST: {
        if (2 != expr->get_param_count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("param count should be equal 2", K(ret), K(expr->get_param_count()));
        } else if (expr->has_flag(IS_INNER_ADDED_EXPR)) {
          PRINT_EXPR(expr->get_param_expr(0));
        } else {
          DATA_PRINTF("cast(");
          PRINT_EXPR(expr->get_param_expr(0));
          DATA_PRINTF(" as ");
          if (OB_SUCC(ret)) {
            if (OB_FAIL(print_cast_type(expr->get_param_expr(1)))) {
              LOG_WARN("fail to print cast_type", K(ret));
            }
          }
          DATA_PRINTF(")");
        }
        break;
      }
      case T_FUN_SYS_SET_COLLATION: {
        ObConstRawExpr* coll_expr = NULL;
        if (2 != expr->get_param_count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("param count should be 2", K(expr->get_param_count()));
        } else if (OB_ISNULL(coll_expr = static_cast<ObConstRawExpr*>(expr->get_param_expr(1))) ||
                   !coll_expr->is_const_expr()) {
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
              // type
              int64_t default_type = -1;
              ObConstRawExpr* con_expr = static_cast<ObConstRawExpr*>(expr->get_param_expr(0));
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
          }  // end 2 || 3
          DATA_PRINTF(")");
        }
        break;
      }
      case T_OP_GET_USER_VAR: {
        int64_t param_num = expr->get_param_count();
        if (1 != param_num || OB_ISNULL(expr->get_param_expr(0))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid param count", K(ret), K(expr->get_param_count()), K(expr->get_param_expr(0)));
        } else if (!expr->get_param_expr(0)->is_const_expr() ||
                   !static_cast<ObConstRawExpr*>(expr->get_param_expr(0))->get_value().is_varchar()) {
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
        if ((param_num != ObExprColumnConv::PARAMS_COUNT_WITH_COLUMN_INFO &&
                param_num != ObExprColumnConv::PARAMS_COUNT_WITHOUT_COLUMN_INFO) ||
            OB_ISNULL(expr->get_param_expr(4))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid param count", K(ret), K(expr->get_param_count()), K(expr->get_param_expr(4)));
        } else {
          PRINT_EXPR(expr->get_param_expr(4));
        }
        break;
      }
      case T_OP_GET_SUBPROGRAM_VAR:  // fallthrough
      case T_OP_GET_PACKAGE_VAR: {
        DATA_PRINTF("?");
        break;
      }
      case T_FUN_SYS_SYSDATE:  // fallthrough
      case T_FUN_SYS_UID:
      case T_FUN_SYS_SESSIONTIMEZONE:
      case T_FUN_SYS_DBTIMEZONE:
      case T_FUN_SYS_ROWNUM:
      case T_FUN_SYS_USER: {
        DATA_PRINTF("%.*s", LEN_AND_PTR(expr->get_func_name()));
        if (share::is_mysql_mode()) {
          DATA_PRINTF("()");
        }
        break;
      }
      case T_FUN_SYS_CUR_DATE: {
        if (share::is_oracle_mode()) {
          DATA_PRINTF(N_CURRENT_DATE);
        } else {
          DATA_PRINTF("curdate()");
        }
        break;
      }
      case T_FUN_SYS_CUR_TIMESTAMP:
        // now(), current_timestamp(), local_time(), local_timestamp()
        if (share::is_oracle_mode()) {
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
        ObSequenceRawExpr* seq_expr = static_cast<ObSequenceRawExpr*>(expr);
        if (1 != seq_expr->get_param_count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("param count should be equal 1", K(ret), K(seq_expr->get_param_count()));
        } else if (!seq_expr->get_name().empty() && !seq_expr->get_action().empty()) {
          DATA_PRINTF("%.*s.%.*s", LEN_AND_PTR(seq_expr->get_name()), LEN_AND_PTR(seq_expr->get_action()));
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("sequence should sepcify format as seqname.action", K(ret));
        }
        break;
      }
      case T_FUN_PL_SQLCODE_SQLERRM: {
        ObPLSQLCodeSQLErrmRawExpr* sql_expr = static_cast<ObPLSQLCodeSQLErrmRawExpr*>(expr);
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
      case T_FUN_PL_COLLECTION_CONSTRUCT: {
        ret = OB_NOT_SUPPORTED;
        break;
      }
      case T_FUN_PL_OBJECT_CONSTRUCT: {
        ret = OB_NOT_SUPPORTED;
      }
      case T_FUN_SYS_DEFAULT: {
        DATA_PRINTF("default");
        break;
      }
      case T_FUN_SYS_LNNVL: {
        DATA_PRINTF("%.*s", LEN_AND_PTR(func_name));
        if (1 != expr->get_param_count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("param count should be equal 1", K(ret), K(expr->get_param_count()));
        } else {
          PRINT_EXPR(expr->get_param_expr(0));
        }
        break;
      }
      case T_FUN_SYS_TRANSLATE: {
        if (2 == expr->get_param_count()) {
          ObRawExpr* first_param = expr->get_param_expr(0);
          ObRawExpr* second_param = expr->get_param_expr(1);
          DATA_PRINTF("translate(");
          PRINT_EXPR(first_param);
          if (OB_ISNULL(second_param) || OB_UNLIKELY(ObRawExpr::EXPR_CONST != second_param->get_expr_class())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("second param of translate is null or not const", K(ret), KPC(second_param));
          } else {
            ObConstRawExpr* const_param = static_cast<ObConstRawExpr*>(second_param);
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
          DATA_PRINTF("%.*s(", LEN_AND_PTR(func_name));
          if (OB_SUCC(ret)) {
            int64_t i = 0;
            for (; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
              PRINT_EXPR(expr->get_param_expr(i));
              DATA_PRINTF(",");
            }
            if (OB_SUCC(ret)) {
              if (i > 0) {
                --*pos_;
              }
              DATA_PRINTF(")");
            }
          }
        }
        break;
      }
      default: {
        // substr
        // date, month
        // cur_date, current_date, cur_time, current_time
        // func_name
        if (T_FUN_SYS_ORA_DECODE == expr->get_expr_type()) {
          // The same function is named decode under Oracle and ora_decode under MySQL
          // Ensure that SQL reverse spelling will not go wrong
          if (share::is_oracle_mode()) {
            func_name = "decode";
          } else {
            func_name = "ora_decode";
          }
        }
        if (T_FUN_UDF == expr->get_expr_type()) {
          DATA_PRINTF(share::is_oracle_mode() ? "\"%.*s\"(" : "`%.*s`(", LEN_AND_PTR(func_name));
        } else {
          DATA_PRINTF("%.*s(", LEN_AND_PTR(func_name));
        }
        if (OB_SUCC(ret)) {
          int64_t i = 0;
          for (; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
            PRINT_EXPR(expr->get_param_expr(i));
            DATA_PRINTF(",");
          }
          if (OB_SUCC(ret)) {
            if (i > 0) {
              --*pos_;
            }
            DATA_PRINTF(")");
          }
        }
        break;
      }
    }
  }

  return ret;
}

int ObRawExprPrinter::print(ObWinFunRawExpr* expr)
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
        } else { /* do nothing. */
        }
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
      case T_FUN_WM_CONCAT: {
        SET_SYMBOL_IF_EMPTY("wm_concat");
        DATA_PRINTF("%.*s(", LEN_AND_PTR(symbol));
        // distinct, default 'all', not print
        if (OB_SUCC(ret)) {
          if (expr->is_distinct()) {
            DATA_PRINTF("distinct ");
          }
        }
        if (OB_SUCC(ret)) {
          int64_t N = expr->get_agg_expr()->get_param_count();
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
        if (OB_FAIL(print_partition_exprs(expr))) {
          LOG_WARN("failed to print partition exprs.", K(ret));
        } else if (OB_FAIL(print_order_items(expr))) {
          LOG_WARN("failed to print order items.", K(ret));
        } else if (OB_FAIL(print_window_clause(expr))) {
          LOG_WARN("failed to print window clause.", K(ret));
        } else { /* do nothing. */
        }
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
        } else { /* do nothing. */
        }
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
        } else { /* do nothing. */
        }
        DATA_PRINTF(")");
        break;
      }
      case T_WIN_FUN_LEAD:
        SET_SYMBOL_IF_EMPTY("lead");
      case T_WIN_FUN_LAG: {
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
        } else { /* do nothing. */
        }
        DATA_PRINTF(")");
        break;
      }
      case T_WIN_FUN_RATIO_TO_REPORT: {
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
        } else { /* do nothing. */
        }
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
        } else { /* do nothing. */
        }
        DATA_PRINTF(")");
        break;
      }
      case T_FUN_GROUP_CONCAT: {
        // mysql: group_concat(distinct c1,c2+1 order by c1 desc separator ',')
        // oracle: listagg(c1,',') within group(order by c1);
        if (share::is_oracle_mode()) {
          SET_SYMBOL_IF_EMPTY("listagg");
        } else {
          SET_SYMBOL_IF_EMPTY("group_concat");
        }
        DATA_PRINTF("%.*s(", LEN_AND_PTR(symbol));
        // distinct
        if (OB_SUCC(ret)) {
          if (expr->is_distinct()) {
            DATA_PRINTF("distinct ");
          }
        }
        // expr list
        for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_agg_expr()->get_real_param_count(); ++i) {
          PRINT_EXPR(expr->get_agg_expr()->get_real_param_exprs().at(i));
          DATA_PRINTF(",");
        }
        if (OB_SUCC(ret)) {
          --*pos_;
        }
        if (share::is_oracle_mode() && 0 == expr->get_order_items().count()) {
          /* do nothing */
        } else if (share::is_oracle_mode()) {
          DATA_PRINTF(") within group (");
          // order by
          if (OB_SUCC(ret)) {
            const ObIArray<OrderItem>& order_items = expr->get_agg_expr()->get_order_items();
            int64_t order_item_size = order_items.count();
            if (order_item_size > 0) {
              DATA_PRINTF(" order by ");
              for (int64_t i = 0; OB_SUCC(ret) && i < order_item_size; ++i) {
                const OrderItem& order_item = order_items.at(i);
                PRINT_EXPR(order_item.expr_);
                if (OB_SUCC(ret)) {
                  if (share::is_mysql_mode()) {
                    if (is_descending_direction(order_item.order_type_)) {
                      DATA_PRINTF(" desc ");
                    }
                  } else if (order_item.order_type_ == NULLS_FIRST_ASC) {
                    DATA_PRINTF(" asc nulls first ");
                  } else if (order_item.order_type_ == NULLS_LAST_ASC) {  // use default value
                    /*do nothing*/
                  } else if (order_item.order_type_ == NULLS_FIRST_DESC) {  // use default value
                    DATA_PRINTF(" desc ");
                  } else if (order_item.order_type_ == NULLS_LAST_DESC) {
                    DATA_PRINTF(" desc nulls last ");
                  } else { /*do nothing*/
                  }
                }
                DATA_PRINTF(",");
              }
              if (OB_SUCC(ret)) {
                --*pos_;
              }
            }
          }
        }
        // separator
        if (OB_SUCC(ret)) {
          if (expr->get_agg_expr()->get_separator_param_expr()) {
            DATA_PRINTF(" separator ");
            PRINT_EXPR(expr->get_agg_expr()->get_separator_param_expr());
          }
        }
        DATA_PRINTF(")");
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
      case T_FUN_KEEP_WM_CONCAT: {
        SET_SYMBOL_IF_EMPTY("wm_concat");
        DATA_PRINTF("%.*s(", LEN_AND_PTR(symbol));
        if (0 == expr->get_agg_expr()->get_real_param_count()) {  // count(*) keep(...)
          if (T_FUN_KEEP_COUNT != type) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("expr type should be T_FUN_KEEP_COUNT ", K(ret), K(type));
          } else {
            DATA_PRINTF("*");
          }
        } else if (OB_UNLIKELY(T_FUN_KEEP_WM_CONCAT != type && 1 != expr->get_agg_expr()->get_real_param_count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected error", K(ret), K(type));
        } else {
          PRINT_EXPR(expr->get_agg_expr()->get_real_param_exprs().at(0));
        }
        if (OB_SUCC(ret)) {
          DATA_PRINTF(")");
          const ObIArray<OrderItem>& order_items = expr->get_agg_expr()->get_order_items();
          if (order_items.count() > 0) {
            DATA_PRINTF("keep(dense_rank first order by ");
            for (int64_t i = 0; OB_SUCC(ret) && i < order_items.count(); ++i) {
              const OrderItem& order_item = order_items.at(i);
              if (OB_ISNULL(order_item.expr_)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("get unexpected null", K(ret));
              } else {
                PRINT_EXPR(order_item.expr_);
                if (OB_SUCC(ret)) {
                  if (share::is_mysql_mode()) {
                    if (is_descending_direction(order_item.order_type_)) {
                      DATA_PRINTF(" desc ");
                    }
                  } else if (order_item.order_type_ == NULLS_FIRST_ASC) {
                    DATA_PRINTF(" asc nulls first ");
                  } else if (order_item.order_type_ == NULLS_LAST_ASC) {  // use default value
                    /*do nothing*/
                  } else if (order_item.order_type_ == NULLS_FIRST_DESC) {  // use default value
                    DATA_PRINTF(" desc ");
                  } else if (order_item.order_type_ == NULLS_LAST_DESC) {
                    DATA_PRINTF(" desc nulls last ");
                  } else { /*do nothing*/
                  }
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
        if (OB_UNLIKELY(1 != expr->get_agg_expr()->get_real_param_count()) ||
            OB_UNLIKELY(1 != expr->get_agg_expr()->get_order_items().count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected error", K(ret), K(type));
        } else {
          DATA_PRINTF("%.*s(", LEN_AND_PTR(symbol));
          PRINT_EXPR(expr->get_agg_expr()->get_param_expr(0));
          DATA_PRINTF(")");
          if (OB_UNLIKELY(T_FUN_MEDIAN != type)) {
            DATA_PRINTF(" within group ( order by ");
            const OrderItem& order_item = expr->get_agg_expr()->get_order_items().at(0);
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
        } else { /* do nothing. */
        }
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

int ObRawExprPrinter::print(ObFunMatchAgainst* expr)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf_) || OB_ISNULL(pos_) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_ is NULL of buf_ is NULL or pos_ is NULL or expr is NULL", K(ret));
  } else {
    DATA_PRINTF("MATCH(");
    if (OB_ISNULL(expr->get_match_columns())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("match columns is NULL", K(ret));
    } else {
      int64_t i = 0;
      for (; OB_SUCC(ret) && i < expr->get_match_columns()->get_param_count(); ++i) {
        ObRawExpr* column_ref = expr->get_match_columns()->get_param_expr(i);
        if (OB_ISNULL(column_ref)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("match columns is NULL", K(ret));
        } else {
          PRINT_EXPR(column_ref);
          DATA_PRINTF(",");
        }
      }
      if (OB_SUCC(ret)) {
        if (i > 0) {
          --*pos_;
        }
        DATA_PRINTF(") AGAINST(");
      }

      if (OB_SUCC(ret)) {
        PRINT_EXPR(expr->get_search_key());
        if (NATURAL_LANGUAGE_MODE == expr->get_mode_flag()) {
          DATA_PRINTF(" IN NATURAL LANGUAGE MODE)");
        } else {
          DATA_PRINTF(" IN BOOLEAN MODE)");
        }
      }
    }
  }

  return ret;
}

int ObRawExprPrinter::print(ObPseudoColumnRawExpr* expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf_) || OB_ISNULL(pos_) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_ is NULL of buf_ is NULL or pos_ is NULL or expr is NULL", K(ret));
  } else {
    ObString symbol("");
    ObItemType type = expr->get_expr_type();
    switch (type) {
      case T_LEVEL:
        SET_SYMBOL_IF_EMPTY("level");
      case T_CONNECT_BY_ISCYCLE:
        SET_SYMBOL_IF_EMPTY("connect_by_iscycle");
      case T_CONNECT_BY_ISLEAF: {
        SET_SYMBOL_IF_EMPTY("connect_by_isleaf");
        if (0 != expr->get_param_count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expr param count should be 0", K(ret), K(expr->get_param_count()));
        } else {
          DATA_PRINTF("%.*s", LEN_AND_PTR(symbol));
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected pseudo column type", K(type));
      }
    }
  }
  return ret;
}

int ObRawExprPrinter::print_partition_exprs(ObWinFunRawExpr* expr)
{
  int ret = OB_SUCCESS;
  int64_t N = expr->get_partition_exprs().count();
  if (N == 0) { /* do nothing. */
  } else if (N > 0) {
    DATA_PRINTF(" PARTITION BY ");
    for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
      if (OB_ISNULL(expr->get_partition_exprs().at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get partiton by exprs.", K(ret));
      } else {
        PRINT_EXPR(expr->get_partition_exprs().at(i));
        if (i < N - 1) {
          DATA_PRINTF(", ");
        } else { /* Do nothing */
        }
      }
    }
  } else { /* do nothing. */
  }
  return ret;
}

int ObRawExprPrinter::print_order_items(ObWinFunRawExpr* expr)
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
          } else { /* do nothing. */
          }
        } else {
          if (is_ascending_direction(order_type)) {
            DATA_PRINTF(" ASC ");
          } else if (is_descending_direction(order_type)) {
            DATA_PRINTF(" DESC ");
          } else { /* do nothing. */
          }
        }
      }
      if (i < N - 1) {
        DATA_PRINTF(", ");
      } else { /* Do nothing */
      }
    }
  }
  return ret;
}

int ObRawExprPrinter::print_window_clause(ObWinFunRawExpr* expr)
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
      // do nothing.
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
        PRINT_EXPR(expr->get_upper().interval_expr_);
        if (expr->get_upper().is_preceding_) {
          DATA_PRINTF(" PRECEDING ");
        } else {
          DATA_PRINTF(" FOLLOWING ");
        }
      } else { /* do nothing. */
      }
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
        PRINT_EXPR(expr->get_lower().interval_expr_);
        if (expr->get_lower().is_preceding_) {
          DATA_PRINTF(" PRECEDING ");
        } else {
          DATA_PRINTF(" FOLLOWING ");
        }
      } else { /* do nothing. */
      }
    }
  }
  return ret;
}

int ObRawExprPrinter::print_date_unit(ObRawExpr* expr)
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
      ObConstRawExpr* con_expr = static_cast<ObConstRawExpr*>(expr);
      if (OB_ISNULL(con_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("con_expr should not be NULL", K(ret));
      } else {
        con_expr->get_value().get_int(date_unit_type);
        DATA_PRINTF(ob_date_unit_type_str(static_cast<ObDateUnitType>(date_unit_type)));
      }
    }
  }

  return ret;
}

// 1. ob not support cast(expr as char charset xxx)
int ObRawExprPrinter::print_cast_type(ObRawExpr* expr)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf_) || OB_ISNULL(pos_) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_ is NULL of buf_ is NULL or pos_ is NULL or expr is NULL", K(ret));
  } else if (ObRawExpr::EXPR_CONST != expr->get_expr_class()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr class should be EXPR_CONST ", K(ret), K(expr->get_expr_class()));
  } else {
    ObConstRawExpr* con_expr = static_cast<ObConstRawExpr*>(expr);
    const ObLengthSemantics length_semantics = con_expr->get_accuracy().get_length_semantics();
    const ObScale scale = con_expr->get_accuracy().get_scale();
    ParseNode parse_node;
    if (OB_FAIL(con_expr->get_value().get_int(parse_node.value_))) {
      LOG_WARN("get int value failed", K(ret));
    } else {
      int16_t cast_type = parse_node.int16_values_[OB_NODE_CAST_TYPE_IDX];
      switch (cast_type) {
        case T_CHAR: {
          // ob not support cast(expr as char charset xxx)
#if 0
        int16_t collation = parse_node.int16_values_[OB_NODE_CAST_COLL_IDX];
        int32_t len = parse_node.int32_values_[OB_NODE_CAST_C_LEN_IDX];
        DATA_PRINTF("char");
        DATA_PRINTF("(%d) ", len);
        DATA_PRINTF("charset ");
        if (OB_SUCC(ret)) {
          if (BINARY_COLLATION == collation) {
            // BINARY
            DATA_PRINTF("binary");
          } else {
            // CHARACTER
            DATA_PRINTF("utf8mb4");
          }
        }
#else
          int16_t collation = parse_node.int16_values_[OB_NODE_CAST_COLL_IDX];
          int32_t len = parse_node.int32_values_[OB_NODE_CAST_C_LEN_IDX];
          if (BINARY_COLLATION == collation) {
            // BINARY
            if (len >= 0) {
              DATA_PRINTF("binary(%d)", len);
            } else {
              DATA_PRINTF("binary");
            }
          } else {
            // CHARACTER
            if (share::is_oracle_mode()) {
              DATA_PRINTF("char(%d %s)", len, get_length_semantics_str(length_semantics));
            } else {
              if (len > 0) {
                DATA_PRINTF("character(%d)", len);
              } else {
                DATA_PRINTF("character");
              }
            }
          }
#endif
          break;
        }
        case T_VARCHAR: {
          int16_t collation = parse_node.int16_values_[OB_NODE_CAST_COLL_IDX];
          int32_t len = parse_node.int32_values_[OB_NODE_CAST_C_LEN_IDX];
          if (BINARY_COLLATION == collation) {
            // BINARY
            if (share::is_oracle_mode()) {
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
            if (share::is_oracle_mode()) {
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
          // oracle mode treate date as datetime
          const char* type_str = lib::is_oracle_mode() ? "date" : "datetime";
          DATA_PRINTF(type_str);
          break;
        }
        case T_DATE: {
          DATA_PRINTF("date");
          break;
        }
        case T_TIME: {
          DATA_PRINTF("time");
          break;
        }
        case T_NUMBER: {
          int16_t precision = parse_node.int16_values_[OB_NODE_CAST_N_PREC_IDX];
          int16_t scale = parse_node.int16_values_[OB_NODE_CAST_N_SCALE_IDX];
          DATA_PRINTF("number(%d,%d)", precision, scale);
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
          int year_scale = ObIntervalScaleUtil::ob_scale_to_interval_ym_year_scale(static_cast<int8_t>(scale));
          DATA_PRINTF("interval year(%d) to month", year_scale);
          break;
        }
        case T_INTERVAL_DS: {
          int day_scale = ObIntervalScaleUtil::ob_scale_to_interval_ds_day_scale(static_cast<int8_t>(scale));
          int fs_scale = ObIntervalScaleUtil::ob_scale_to_interval_ds_second_scale(static_cast<int8_t>(scale));
          DATA_PRINTF("interval day(%d) to second(%d)", day_scale, fs_scale);
          break;
        }
        case T_TIMESTAMP_TZ: {
          DATA_PRINTF("timestamp with time zone");
          break;
        }
        case T_TIMESTAMP_LTZ: {
          DATA_PRINTF("timestamp with local time zone");
          break;
        }
        case T_TIMESTAMP_NANO: {
          DATA_PRINTF("timestamp");
          break;
        }
        case T_RAW: {
          int32_t len = parse_node.int32_values_[OB_NODE_CAST_C_LEN_IDX];
          DATA_PRINTF("raw(%d)", len);
          break;
        }
        case T_FLOAT: {
          const char* type_str = lib::is_oracle_mode() ? "binary_float" : "float";
          DATA_PRINTF(type_str);
          break;
        }
        case T_DOUBLE: {
          const char* type_str = lib::is_oracle_mode() ? "binary_double" : "double";
          DATA_PRINTF(type_str);
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
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unknown cast type", K(ret), K(cast_type));
          break;
        }
      }  // end switch
    }
  }

  return ret;
}
}  // end of namespace sql
}  // end of namespace oceanbase
