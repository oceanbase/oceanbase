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
#include "sql/ob_dml_stmt_printer.h"
#include "sql/ob_select_stmt_printer.h"
#include "sql/ob_sql_context.h"
#include "sql/resolver/dml/ob_del_upd_stmt.h"
#include "common/ob_smart_call.h"
#include "lib/charset/ob_charset.h"
#include "sql/optimizer/ob_log_plan.h"
namespace oceanbase
{
using namespace common;
namespace sql
{

ObDMLStmtPrinter::ObDMLStmtPrinter()
  : buf_(NULL),
    buf_len_(0),
    pos_(NULL),
    stmt_(NULL),
    schema_guard_(NULL),
    print_params_(),
    expr_printer_(),
    param_store_(NULL),
    is_inited_(false)
{
}

ObDMLStmtPrinter::ObDMLStmtPrinter(char *buf, int64_t buf_len, int64_t *pos, const ObDMLStmt *stmt,
                                   ObSchemaGetterGuard *schema_guard,
                                   ObObjPrintParams print_params,
                                   const ParamStore *param_store)
  : buf_(buf),
    buf_len_(buf_len),
    pos_(pos),
    stmt_(stmt),
    schema_guard_(schema_guard),
    print_params_(print_params),
    expr_printer_(),
    param_store_(param_store),
    is_inited_(true)
{
}

ObDMLStmtPrinter::~ObDMLStmtPrinter()
{
}

void ObDMLStmtPrinter::init(char *buf, int64_t buf_len, int64_t *pos, ObDMLStmt *stmt)
{
  buf_ = buf;
  buf_len_ = buf_len;
  pos_ = pos;
  stmt_ = stmt;
  is_inited_ = true;
}

int ObDMLStmtPrinter::print_hint()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt_) || OB_ISNULL(stmt_->get_query_ctx()) || OB_ISNULL(buf_) || OB_ISNULL(pos_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_ is NULL or buf_ is NULL or pos_ is NULL", K(ret));
  } else {
    const char *hint_begin = "/*+";
    const char *hint_end = " */";
    DATA_PRINTF("%s", hint_begin);
    if (OB_SUCC(ret)) {
      const ObQueryHint &query_hint = stmt_->get_query_ctx()->get_query_hint();
      // just for print hint, ExplainType set as invalid type
      planText plan_text(buf_, buf_len_, ExplainType::EXPLAIN_UNINITIALIZED);
      plan_text.pos = *pos_;
      plan_text.is_oneline_ = true;
      if (OB_FAIL(query_hint.print_stmt_hint(plan_text, *stmt_))) {
        LOG_WARN("failed to print stmt hint", K(ret));
      } else if (plan_text.pos == *pos_) {
        // no hint, roolback buffer!
        *pos_ -= strlen(hint_begin);
      } else {
        *pos_ = plan_text.pos;
        DATA_PRINTF("%s", hint_end);
      }
    }
  }

  return ret;
}

int ObDMLStmtPrinter::print_from(bool need_from)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(stmt_) || OB_ISNULL(buf_) || OB_ISNULL(pos_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_ is NULL or buf_ is NULL or pos_ is NULL", K(ret));
  } else {
    int64_t from_item_size = stmt_->get_from_item_size();
    if (from_item_size > 0) {
      if (need_from) {
        DATA_PRINTF(" from ");
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < from_item_size; ++i) {
        const FromItem &from_item = stmt_->get_from_item(i);
        const TableItem *table_item = NULL;
        if (from_item.is_joined_) {
          table_item = stmt_->get_joined_table(from_item.table_id_);
        } else {
          table_item = stmt_->get_table_item_by_id(from_item.table_id_);
        }
        if (OB_ISNULL(table_item)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("table_item should not be NULL", K(ret));
        } else if (OB_FAIL(print_table(table_item))) {
          LOG_WARN("fail to print table", K(ret), K(*table_item));
        } else {
          DATA_PRINTF(",");
        }
      }
      if (OB_SUCC(ret)) {
        --*pos_;
      }
    } else if (0 != stmt_->get_condition_exprs().count()) {
      // create view v as select 1 from dual where 1;
      DATA_PRINTF(" from DUAL");
    } else if (lib::is_oracle_mode() && 0 == from_item_size) {
      // select 1 from dual
      // in oracle mode
      DATA_PRINTF(" from DUAL");
    }
  }

  return ret;
}

int ObDMLStmtPrinter::print_table(const TableItem *table_item,
                                  bool expand_cte_table,
                                  bool no_print_alias/*default false*/)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret), K(is_stack_overflow));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_stack_overflow));
  } else if (OB_ISNULL(stmt_) || OB_ISNULL(buf_) || OB_ISNULL(pos_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_ is NULL or buf_ is NULL or pos_ is NULL", K(ret));
  } else {
    switch (table_item->type_) {
    case TableItem::BASE_TABLE: {
        if (OB_FAIL(print_base_table(table_item))) {
          LOG_WARN("failed to print base table", K(ret), K(*table_item));
        }
        break;
      }
    case TableItem::ALIAS_TABLE: {
        if (OB_FAIL(print_base_table(table_item))) {
          LOG_WARN("failed to print base table", K(ret), K(*table_item));
        //table in insert all can't print alias(bug:https://work.aone.alibaba-inc.com/issue/31941210)
        } else if (!no_print_alias) {
          DATA_PRINTF(lib::is_oracle_mode() ? " \"%.*s\"" : " `%.*s`",
                      LEN_AND_PTR(table_item->alias_name_));
        }
        break;
      }
    case TableItem::JOINED_TABLE: {
        const JoinedTable *join_table = static_cast<const JoinedTable*>(table_item);
        if (OB_ISNULL(join_table)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("join_table should not be NULL", K(ret));
        } else {
          // left table
          const TableItem *left_table = join_table->left_table_;
          if (OB_ISNULL(left_table)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("left_table should not be NULL", K(ret));
          } else {
            DATA_PRINTF("(");
            if (OB_SUCC(ret)) {
              if (OB_FAIL(SMART_CALL(print_table(left_table)))) {
                LOG_WARN("fail to print left table", K(ret), K(*left_table));
              }
            }
            // join type
            if (OB_SUCC(ret)) {
              // not support cross join and natural join
              ObString type_str("");
              switch (join_table->joined_type_) {
              case FULL_OUTER_JOIN: {
                  type_str = "full join";
                  break;
                }
              case LEFT_OUTER_JOIN: {
                  type_str = "left join";
                  break;
                }
              case RIGHT_OUTER_JOIN: {
                  type_str = "right join";
                  break;
                }
              case INNER_JOIN: {
                  if (join_table->join_conditions_.count() == 0) {//cross join
                    type_str = "cross join";
                  } else {
                    type_str = "join";
                  }
                  break;
                }
              default: {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("unknown join type", K(ret), K(join_table->joined_type_));
                  break;
                }
              }
              DATA_PRINTF(" %.*s ", LEN_AND_PTR(type_str));
            }
            // right table
            if (OB_SUCC(ret)) {
              const TableItem *right_table = join_table->right_table_;
              if (OB_ISNULL(right_table)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("right_table should not be NULL", K(ret));
              } else if (OB_FAIL(SMART_CALL(print_table(right_table)))) {
                LOG_WARN("fail to print right table", K(ret), K(*right_table));
              } else {
                // join conditions
                const ObIArray<ObRawExpr*> &join_conditions =
                    join_table->join_conditions_;
                int64_t join_conditions_size = join_conditions.count();
                if (join_conditions_size > 0) {
                  DATA_PRINTF(" on (");
                  for (int64_t i = 0; OB_SUCC(ret) && i < join_conditions_size;
                       ++i) {
                    if (OB_FAIL(expr_printer_.do_print(join_conditions.at(i), T_NONE_SCOPE))) {
                      LOG_WARN("fail to print join condition", K(ret));
                    }
                    DATA_PRINTF(" and ");
                  }
                  if (OB_SUCC(ret)) {
                    *pos_ -= 5; // strlen(" and ")
                    DATA_PRINTF(")");
                  }
                }
                DATA_PRINTF(")");
              }
            }
          }
        }
        break;
      }
    case TableItem::GENERATED_TABLE: {
        if (OB_ISNULL(table_item->ref_query_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("table item ref query is null", K(ret));
        // generated_table cannot appear in view_definition
        // view在resolver阶段被转换为生成表, 需要当做基表处理
        } else if (table_item->is_view_table_) {
          PRINT_TABLE_NAME(table_item);
          if (OB_SUCC(ret)) {
            if (table_item->alias_name_.length() > 0) {
              DATA_PRINTF(" %.*s", LEN_AND_PTR(table_item->alias_name_));
            }
          }
        } else if (!expand_cte_table && table_item->cte_type_ != TableItem::NOT_CTE) {
        	DATA_PRINTF(" %.*s", LEN_AND_PTR(table_item->table_name_));
        	if (table_item->alias_name_.length() > 0) {
        		DATA_PRINTF(" %.*s", LEN_AND_PTR(table_item->alias_name_));
        	}
    		} else {
          // 这里打印生成表是为了方便OB内部使用, 如rewrite调试用
          bool print_bracket = ! expand_cte_table || table_item->ref_query_->is_root_stmt();
          if (print_bracket) {
            DATA_PRINTF("(");
          }
          if (OB_SUCC(ret)) {
            ObIArray<ObString> *column_list = NULL;
            bool is_set_subquery = false;
            bool is_generated_table = true;
            // force print alias name for select item in generated table. Otherwise,
            // create view v as select 1 from dual where 1 > (select `abs(c1)` from (select abs(c1) from t));
            // Definition of v would be:
            // CREATE VIEW `v` AS select 1 AS `1` from DUAL where (1 > (select `abs(c1)` from ((select abs(`test`.`t`.`c1`) from `test`.`t`)) ))
            // `abs(c1)` would be an unknown column.
            const bool force_col_alias = stmt_->is_select_stmt();
            ObSelectStmtPrinter stmt_printer(buf_, buf_len_, pos_,
                                             static_cast<ObSelectStmt*>(table_item->ref_query_),
                                             schema_guard_,
                                             print_params_,
                                             column_list,
                                             is_set_subquery,
                                             is_generated_table,
                                             force_col_alias);
            if (OB_FAIL(stmt_printer.do_print())) {
              LOG_WARN("fail to print generated table", K(ret));
            }
            if (print_bracket) {
              DATA_PRINTF(")");
            }
            if (table_item->cte_type_ == TableItem::NOT_CTE) {
            	DATA_PRINTF(" %.*s", LEN_AND_PTR(table_item->alias_name_));
            }
          }
        }
        break;
      }
    case TableItem::CTE_TABLE: {
        PRINT_TABLE_NAME(table_item);
        if (! table_item->alias_name_.empty()) {
          DATA_PRINTF(lib::is_oracle_mode() ? " \"%.*s\"" : " `%.*s`",
                      LEN_AND_PTR(table_item->alias_name_));
        }
        break;
      }
    case TableItem::FUNCTION_TABLE: {
      CK (lib::is_oracle_mode());
      DATA_PRINTF("TABLE(");
      OZ (expr_printer_.do_print(table_item->function_table_expr_, T_FROM_SCOPE));
      DATA_PRINTF(")");
      DATA_PRINTF(" \"%.*s\"", LEN_AND_PTR(table_item->alias_name_));
      break;
    }
    case TableItem::TEMP_TABLE: {
      PRINT_TABLE_NAME(table_item);
      if (! table_item->alias_name_.empty()) {
        DATA_PRINTF(lib::is_oracle_mode() ? " \"%.*s\"" : " `%.*s`",
                    LEN_AND_PTR(table_item->alias_name_));
      }
      break;
    }
    default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unknown table type", K(ret), K(table_item->type_));
        break;
      }
    }
  }

  return ret;
}

int ObDMLStmtPrinter::print_base_table(const TableItem *table_item)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(stmt_) || OB_ISNULL(buf_) || OB_ISNULL(pos_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_ is NULL or buf_ is NULL or pos_ is NULL", K(ret));
  } else if (OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table_item should not be NULL", K(ret));
  } else if (TableItem::BASE_TABLE != table_item->type_
      && TableItem::ALIAS_TABLE != table_item->type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table_type should be BASE_TABLE or ALIAS_TABLE", K(ret),
             K(table_item->type_));
  } else {
    PRINT_TABLE_NAME(table_item);
    if (OB_SUCC(ret)) {
      // partition
      if (!table_item->access_all_part()) {
        const ObIArray<ObString> &part_names = table_item->part_names_;
        DATA_PRINTF(" partition(");
        for (int64_t i = 0; OB_SUCC(ret) && i < part_names.count(); ++i) {
          DATA_PRINTF("%.*s,", LEN_AND_PTR(part_names.at(i)));
        }
        if (OB_SUCC(ret)) {
          --*pos_;
          DATA_PRINTF(")");
        }
      }
      // flashback query
      if (OB_SUCC(ret)) {
        if (OB_NOT_NULL(table_item->flashback_query_expr_)) {
          if (table_item->flashback_query_type_ == TableItem::USING_TIMESTAMP) {
            DATA_PRINTF(" as of timestamp "); 
            if (OB_FAIL(expr_printer_.do_print(table_item->flashback_query_expr_, T_NONE_SCOPE))) {
              LOG_WARN("fail to print where expr", K(ret));
            }
          } else if (table_item->flashback_query_type_ == TableItem::USING_SCN) {
            if (lib::is_oracle_mode()) {
              DATA_PRINTF(" as of scn "); 
            } else {
              DATA_PRINTF(" as of snapshot "); 
            }
            if (OB_FAIL(expr_printer_.do_print(table_item->flashback_query_expr_, T_NONE_SCOPE))) {
              LOG_WARN("fail to print where expr", K(ret));
            }
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected type", K(ret), K(table_item->flashback_query_type_));
          }
        }
      }
    }
  }

  return ret;
}

int ObDMLStmtPrinter::print_where()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt_) || OB_ISNULL(buf_) || OB_ISNULL(pos_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_ is NULL or buf_ is NULL or pos_ is NULL", K(ret));
  } else {
    const ObIArray<ObRawExpr*> &condition_exprs = stmt_->get_condition_exprs();
    int64_t condition_exprs_size = condition_exprs.count();
    if (condition_exprs_size > 0) {
      DATA_PRINTF(" where ");
      for (int64_t i = 0; OB_SUCC(ret) && i < condition_exprs_size; ++i) {
        if (OB_FAIL(expr_printer_.do_print(condition_exprs.at(i), T_WHERE_SCOPE))) {
          LOG_WARN("fail to print where expr", K(ret));
        }
        DATA_PRINTF(" and ");
      }
      if (OB_SUCC(ret)) {
        *pos_ -= 5; // strlen(" and ")
      }
    }
  }

  return ret;
}

int ObDMLStmtPrinter::print_order_by()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(stmt_) || OB_ISNULL(buf_) || OB_ISNULL(pos_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_ is NULL or buf_ is NULL or pos_ is NULL", K(ret));
  } else {
    ObArenaAllocator alloc;
    ObConstRawExpr expr(alloc);
    int64_t order_item_size = stmt_->get_order_item_size();
    if (order_item_size > 0) {
      DATA_PRINTF(" order by ");
      for (int64_t i = 0; OB_SUCC(ret) && i < order_item_size; ++i) {
        const OrderItem &order_item = stmt_->get_order_item(i);
        if (OB_FAIL(expr_printer_.do_print(order_item.expr_, T_ORDER_SCOPE))) {
          LOG_WARN("fail to print order by expr", K(ret));
        } else if (lib::is_mysql_mode()) {
          if (is_descending_direction(order_item.order_type_)) {
            DATA_PRINTF("desc");
          }
        } else if (order_item.order_type_ == NULLS_FIRST_ASC) {
          DATA_PRINTF("asc nulls first");
        } else if (order_item.order_type_ == NULLS_LAST_ASC) {//use default value
          /*do nothing*/
        } else if (order_item.order_type_ == NULLS_FIRST_DESC) {//use default value
          DATA_PRINTF("desc");
        } else if (order_item.order_type_ == NULLS_LAST_DESC) {
          DATA_PRINTF("desc nulls last");
        } else {/*do nothing*/}
        DATA_PRINTF(",");
      }
      if (OB_SUCC(ret)) {
        --*pos_;
      }
    }
  }

  return ret;
}

int ObDMLStmtPrinter::print_limit()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(stmt_) || OB_ISNULL(buf_) || OB_ISNULL(pos_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_ is NULL or buf_ is NULL or pos_ is NULL", K(ret));
  } else if (stmt_->has_fetch()) {
    /*有fetch,说明是oracle mode下的fetch填充的limit,这里不应该打印 */
  } else {
    if (NULL != stmt_->get_offset_expr() || NULL != stmt_->get_limit_expr()) {
      DATA_PRINTF(" limit ");
    }
    // offset
    if (OB_SUCC(ret)) {
      if (NULL != stmt_->get_offset_expr()) {
        if (OB_FAIL(expr_printer_.do_print(stmt_->get_offset_expr(), T_NONE_SCOPE))) {
          LOG_WARN("fail to print order offset expr", K(ret));
        }
        DATA_PRINTF(",");
      }
    }
    // limit
    if (OB_SUCC(ret)) {
      if (NULL != stmt_->get_limit_expr()) {
        if (OB_FAIL(expr_printer_.do_print(stmt_->get_limit_expr(), T_NONE_SCOPE))) {
          LOG_WARN("fail to print order limit expr", K(ret));
        }
      }
    }
  }

  return ret;
}

int ObDMLStmtPrinter::print_fetch()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(stmt_) || OB_ISNULL(buf_) || OB_ISNULL(pos_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_ is NULL or buf_ is NULL or pos_ is NULL", K(ret));
  } else if (stmt_->has_fetch()) {
    // offset
    if (OB_SUCC(ret)) {
      if (NULL != stmt_->get_offset_expr()) {
        DATA_PRINTF(" offset ");
        if (OB_FAIL(expr_printer_.do_print(stmt_->get_offset_expr(), T_NONE_SCOPE))) {
          LOG_WARN("fail to print order offset expr", K(ret));
        }
        DATA_PRINTF(" rows");
      }
    }
    // fetch
    if (OB_SUCC(ret)) {
      if (NULL != stmt_->get_limit_expr()) {
        DATA_PRINTF(" fetch next ");
        if (OB_FAIL(expr_printer_.do_print(stmt_->get_limit_expr(), T_NONE_SCOPE))) {
          LOG_WARN("fail to print order limit expr", K(ret));
        }
        DATA_PRINTF(" rows");
      }
    }
    //fetch percent
    if (OB_SUCC(ret)) {
      if (NULL != stmt_->get_limit_percent_expr()) {
        DATA_PRINTF(" fetch next ");
        if (OB_FAIL(expr_printer_.do_print(stmt_->get_limit_percent_expr(), T_NONE_SCOPE))) {
          LOG_WARN("fail to print order limit expr", K(ret));
        }
        DATA_PRINTF(" percent rows");
      }
    }
    //fetch only/with ties
    if (OB_SUCC(ret) && 
        (NULL != stmt_->get_limit_expr() || NULL != stmt_->get_limit_percent_expr()) ) {
      if (stmt_->is_fetch_with_ties()) {
        DATA_PRINTF(" with ties");
      } else {
        DATA_PRINTF(" only");
      }
    }

  }
  return ret;
}

int ObDMLStmtPrinter::print_returning()
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(stmt_),
      OB_NOT_NULL(buf_),
      OB_NOT_NULL(pos_));
  CK (stmt_->is_insert_stmt() || stmt_->is_update_stmt() || stmt_->is_delete_stmt());
  if (OB_SUCC(ret)) {
    const ObDelUpdStmt &dml_stmt = static_cast<const ObDelUpdStmt&>(*stmt_);
    const ObIArray<ObRawExpr*> &returning_exprs = dml_stmt.get_returning_exprs();
    if (returning_exprs.count() > 0) {
      DATA_PRINTF(" returning ");
      OZ (expr_printer_.do_print(returning_exprs.at(0), T_NONE_SCOPE));
      for (uint64_t i = 1; OB_SUCC(ret) && i < returning_exprs.count(); ++i) {
        DATA_PRINTF(",");
        OZ (expr_printer_.do_print(returning_exprs.at(i), T_NONE_SCOPE));
      }
    }
  }
  return ret;
}

} //end of namespace sql
} //end of namespace oceanbase



