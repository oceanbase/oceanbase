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
#include "sql/ob_select_stmt_printer.h"
#include "sql/ob_sql_context.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "lib/allocator/page_arena.h"
namespace oceanbase
{
using namespace common;
namespace sql
{

void ObSelectStmtPrinter::init(char *buf, int64_t buf_len, int64_t *pos,
                               ObSelectStmt *stmt,
                               ObIArray<ObString> *column_list)
{
  ObDMLStmtPrinter::init(buf, buf_len, pos, stmt);
  column_list_ = column_list;
}

int ObSelectStmtPrinter::do_print()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt should not be NULL", K(ret));
  } else if (!stmt_->is_select_stmt()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Not a valid select stmt", K(stmt_->get_stmt_type()), K(ret));
  } else {
    const ObSelectStmt *select_stmt = static_cast<const ObSelectStmt *>(stmt_);
    if (OB_UNLIKELY(NULL != column_list_
        && column_list_->count() != select_stmt->get_select_item_size())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column_list size should be equal select_item size", K(ret),
          K(column_list_->count()), K(select_stmt->get_select_item_size()));
    } else {
      expr_printer_.init(buf_,
                        buf_len_,
                        pos_,
                        schema_guard_,
                        print_params_,
                        param_store_);
      if (stmt_->is_unpivot_select()) {
        if (OB_FAIL(print_unpivot())) {
          LOG_WARN("fail to print_unpivot",
                   KPC(stmt_->get_transpose_item()), K(ret));
        }
      } else if (OB_FAIL(print())) {
        LOG_WARN("fail to print stmt", KPC(stmt_), K(ret));
      }
    }
  }
  return ret;
}

int ObSelectStmtPrinter::print()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_ should not be NULL", K(ret));
  } else if (!stmt_->is_select_stmt()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Not a valid select stmt", K(stmt_->get_stmt_type()), K(ret));
  } else {
    const ObSelectStmt *select_stmt = static_cast<const ObSelectStmt*>(stmt_);
    if (OB_FAIL(print_with())) {
      LOG_WARN("print with failed");
    } else if (OB_FAIL(print_temp_table_as_cte())) {
      LOG_WARN("failed to print cte", K(ret));
    } else if (select_stmt->is_set_stmt()) {
      if (select_stmt->is_recursive_union() &&
          !print_params_.print_origin_stmt_) {
        // for dblink, print a embeded recursive union query block
        if (OB_FAIL(print_recursive_union_stmt())) {
          LOG_WARN("failed to print recursive union stmt", K(ret));
        }
      } else if (OB_FAIL(print_set_op_stmt())) {
        LOG_WARN("fail to print set_op stmt", K(ret), K(*stmt_));
      }
    } else if (OB_FAIL(print_basic_stmt())) {
      LOG_WARN("fail to print basic stmt", K(ret), K(*stmt_));
    }
  }

  return ret;
}

int ObSelectStmtPrinter::print_unpivot()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_ should not be NULL", K(ret));
  } else if (OB_UNLIKELY(!stmt_->is_select_stmt())
             || OB_UNLIKELY(!stmt_->is_unpivot_select())
             || OB_UNLIKELY(stmt_->get_table_items().count() != 1)
             || OB_ISNULL(stmt_->get_table_item(0)->ref_query_)
             || OB_UNLIKELY(stmt_->get_table_item(0)->ref_query_->get_table_items().count() != 1)
             ) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Not a valid select stmt", K(stmt_->get_stmt_type()),
             K(stmt_->get_table_items().count()), K(ret));
  } else {
    const ObSelectStmt *select_stmt = static_cast<const ObSelectStmt*>(stmt_);

    if (OB_FAIL(print_temp_table_as_cte())) {
      LOG_WARN("failed to print cte", K(ret));
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(print_select())) {
        LOG_WARN("fail to print select", K(ret), K(*stmt_));
      }
    }

    DATA_PRINTF(" FROM ");

    if (OB_SUCC(ret)) {
      if (OB_FAIL(print_table(stmt_->get_table_item(0)->ref_query_->get_table_item(0)))) {
        LOG_WARN("fail to print table", K(ret));
      }
    }

    const TransposeItem &transpose_item = *select_stmt->get_transpose_item();

    if (OB_SUCC(ret)) {
      DATA_PRINTF(" UNPIVOT %s (",
          (transpose_item.is_include_null() ? "INCLUDE NULLS" : "EXCLUDE NULLS"));
    }

    if (OB_SUCC(ret)) {
      if (transpose_item.unpivot_columns_.count() == 1) {
        DATA_PRINTF("%.*s", transpose_item.unpivot_columns_[0].length(),
                            transpose_item.unpivot_columns_[0].ptr());
      } else {
        DATA_PRINTF("(");
        for (int64_t i = 0; i < transpose_item.unpivot_columns_.count() && OB_SUCC(ret); ++i) {
          DATA_PRINTF("%.*s,", transpose_item.unpivot_columns_[i].length(),
                              transpose_item.unpivot_columns_[i].ptr());
        }
        if (OB_SUCC(ret)) {
          --(*pos_);
          DATA_PRINTF(")");
        }
      }
    }

    DATA_PRINTF(" FOR ");

    if (OB_SUCC(ret)) {
      if (transpose_item.for_columns_.count() == 1) {
        DATA_PRINTF("%.*s", transpose_item.for_columns_[0].length(),
                            transpose_item.for_columns_[0].ptr());
      } else {
        DATA_PRINTF("(");
        for (int64_t i = 0; i < transpose_item.for_columns_.count() && OB_SUCC(ret); ++i) {
          DATA_PRINTF("%.*s,", transpose_item.for_columns_[i].length(),
                               transpose_item.for_columns_[i].ptr());
        }
        if (OB_SUCC(ret)) {
          --(*pos_);
          DATA_PRINTF(")");
        }
      }
    }

    DATA_PRINTF(" IN (");

    if (OB_SUCC(ret)) {
      for (int64_t i = 0; i < transpose_item.in_pairs_.count() && OB_SUCC(ret); ++i) {
        const TransposeItem::InPair &in_pair = transpose_item.in_pairs_[i];
        if (in_pair.column_names_.count() == 1) {
          DATA_PRINTF("%.*s", in_pair.column_names_[0].length(),
                               in_pair.column_names_[0].ptr());
        } else {
          DATA_PRINTF("(");
          for (int64_t j = 0; j < in_pair.column_names_.count() && OB_SUCC(ret); ++j) {
            DATA_PRINTF("%.*s,", in_pair.column_names_[j].length(),
                                 in_pair.column_names_[j].ptr());
          }
          if (OB_SUCC(ret)) {
            --(*pos_);
            DATA_PRINTF(")");
          }
        }

        if (OB_SUCC(ret) && !in_pair.exprs_.empty()) {
          DATA_PRINTF(" AS ");
          if (OB_SUCC(ret)) {
            HEAP_VAR(char[OB_MAX_DEFAULT_VALUE_LENGTH], expr_str_buf) {
              MEMSET(expr_str_buf, 0 , sizeof(expr_str_buf));
              if (in_pair.exprs_.count() == 1) {
                ObRawExpr *expr = in_pair.exprs_.at(0);
                int64_t pos = 0;
                ObRawExprPrinter expr_printer(expr_str_buf, OB_MAX_DEFAULT_VALUE_LENGTH, &pos,
                                              schema_guard_, stmt_->get_query_ctx()->get_timezone_info());
                if (OB_FAIL(expr_printer.do_print(expr, T_NONE_SCOPE, true))) {
                  LOG_WARN("print expr definition failed", KPC(expr), K(ret));
                } else {
                  DATA_PRINTF("%.*s", static_cast<int32_t>(pos), expr_str_buf);
                }
              } else {
                DATA_PRINTF("(");
                for (int64_t j = 0; j < in_pair.exprs_.count() && OB_SUCC(ret); ++j) {
                  ObRawExpr *expr = in_pair.exprs_.at(j);
                  int64_t pos = 0;
                  ObRawExprPrinter expr_printer(expr_str_buf, OB_MAX_DEFAULT_VALUE_LENGTH, &pos,
                                                schema_guard_, stmt_->get_query_ctx()->get_timezone_info());
                  if (OB_FAIL(expr_printer.do_print(expr, T_NONE_SCOPE, true))) {
                    LOG_WARN("print expr definition failed", KPC(expr), K(ret));
                  } else {
                    DATA_PRINTF("%.*s,", static_cast<int32_t>(pos), expr_str_buf);
                  }
                }
                if (OB_SUCC(ret)) {
                  --(*pos_);
                  DATA_PRINTF(")");
                }
              }
            }
          }
        }
        if (i != transpose_item.in_pairs_.count() - 1) {
          DATA_PRINTF(",");
        }
      }
      DATA_PRINTF(" )");
    }

    DATA_PRINTF(" )");
  }

  return ret;
}
int ObSelectStmtPrinter::print_set_op_stmt()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_ should not be NULL", K(ret));
  } else if (!stmt_->is_select_stmt()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Not a valid select stmt", K(stmt_->get_stmt_type()), K(ret));
  } else {
    const ObSelectStmt *select_stmt = static_cast<const ObSelectStmt*>(stmt_);
    // todo: 目前 union 展平后无法保证与原始 sql 相同
    ObSEArray<ObSelectStmt*, 2> child_stmts;
    if (!select_stmt->is_set_stmt() || 2 > select_stmt->get_set_query().count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("stmt_ should have set_op", K(ret), K(select_stmt->is_set_stmt()),
                                           K(select_stmt->get_set_query().count()));
    } else if (OB_FAIL(child_stmts.assign(select_stmt->get_set_query()))) {
      LOG_WARN("failed to assign stmts", K(ret));
    } else if (OB_ISNULL(child_stmts.at(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("child_stmt should not be NULL", K(ret));
    } else {
      if (select_stmt->get_children_swapped()) {
        std::swap(child_stmts.at(0), child_stmts.at(1));
      }
      DATA_PRINTF("(");
      ObSelectStmtPrinter stmt_printer(buf_,
                                       buf_len_,
                                       pos_,
                                       child_stmts.at(0),
                                       schema_guard_,
                                       print_params_,
                                       param_store_,
                                       /*force_col_alias*/true);
      stmt_printer.set_column_list(column_list_);
      stmt_printer.set_is_first_stmt_for_hint(is_first_stmt_for_hint_);
      ObString set_op_str = ObString::make_string(
                                ObSelectStmt::set_operator_str(select_stmt->get_set_op()));
      if (OB_FAIL(stmt_printer.do_print())) {
        LOG_WARN("fail to print left stmt", K(ret), K(*child_stmts.at(0)));
      } else {
        stmt_printer.set_is_first_stmt_for_hint(false);
        DATA_PRINTF(")");
      }
      for (int64_t i = 1; OB_SUCC(ret) && i < child_stmts.count(); ++i) {
        DATA_PRINTF(" %.*s ", LEN_AND_PTR(set_op_str)); // print set_op
        if (!select_stmt->is_set_distinct()) {
          DATA_PRINTF(" all ");
        }
        if (OB_FAIL(ret)) {
        } else if (OB_ISNULL(child_stmts.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("child_stmt should not be NULL", K(ret));
        } else {
          DATA_PRINTF("(");
          stmt_printer.init(buf_, buf_len_, pos_, child_stmts.at(i), column_list_);
          if (OB_FAIL(stmt_printer.do_print())) {
            LOG_WARN("fail to print child stmt", K(ret));
          } else {
            DATA_PRINTF(")");
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(print_order_by())) {
        LOG_WARN("fail to print order by",K(ret));
      } else if (OB_FAIL(print_limit())) {
        LOG_WARN("fail to print limit", K(ret));
      } else if (OB_FAIL(print_fetch())) {
        LOG_WARN("fail to print fetch", K(ret));
      } else if (OB_FAIL(print_with_check_option())) {
        LOG_WARN("fail to print with check option", K(ret));
      }
    }
  }
  return ret;
}

int ObSelectStmtPrinter::print_recursive_union_stmt()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_ should not be NULL", K(ret));
  } else if (!stmt_->is_select_stmt()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Not a valid select stmt", K(stmt_->get_stmt_type()), K(ret));
  } else {
    const ObSelectStmt *select_stmt = static_cast<const ObSelectStmt*>(stmt_);
    TableItem *table = NULL;
    if (OB_FAIL(find_recursive_cte_table(select_stmt, table))) {
      LOG_WARN("failed to find recursive cte table", K(ret));
    } else if (OB_ISNULL(table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null table item", K(ret));
    } else  {
      DATA_PRINTF(is_oracle_mode() ? "WITH " : "WITH RECURSIVE ");
      DATA_PRINTF("%.*s", LEN_AND_PTR(table->table_name_));
      if (OB_FAIL(print_cte_define_title(select_stmt))) {
        LOG_WARN("failed to printf cte title", K(ret));
      } else {
        DATA_PRINTF("(");
        if (OB_FAIL(print_set_op_stmt())) {
          LOG_WARN("failed to print", K(ret));
        } else if (OB_FAIL(print_search_and_cycle(select_stmt))) {
          LOG_WARN("print search and cycle failed", K(ret));
        } else {
          DATA_PRINTF(")");
          DATA_PRINTF(" select * from ");
          DATA_PRINTF("%.*s", LEN_AND_PTR(table->table_name_));
        }
      }
    }
  }
  return ret;
}

int ObSelectStmtPrinter::print_basic_stmt()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_ should not be NULL", K(ret));
  } else if (OB_FAIL(print_select())) {
    LOG_WARN("fail to print select", K(ret), K(*stmt_));
  } else if (OB_FAIL(print_from())) {
    LOG_WARN("fail to print from", K(ret), K(*stmt_));
  } else if (OB_FAIL(print_where())) {
    LOG_WARN("fail to print where", K(ret), K(*stmt_));
  } else if (OB_FAIL(print_start_with())) {
    LOG_WARN("fail to print start with", K(ret), K(*stmt_));
  } else if (OB_FAIL(print_connect_by())) {
    LOG_WARN("fail to print connect by", K(ret), K(*stmt_));
  } else if (OB_FAIL(print_group_by())) {
    LOG_WARN("fail to print group by", K(ret), K(*stmt_));
  } else if (OB_FAIL(print_having())) {
    LOG_WARN("fail to print having", K(ret), K(*stmt_));
  } else if (OB_FAIL(print_order_by())) {
    LOG_WARN("fail to print order by", K(ret), K(*stmt_));
  } else if (OB_FAIL(print_limit())) {
    LOG_WARN("fail to print limit", K(ret), K(*stmt_));
  } else if (OB_FAIL(print_fetch())) {
    LOG_WARN("fail to print fetch", K(ret), K(*stmt_));
  } else if (OB_FAIL(print_with_check_option())) {
    LOG_WARN("fail to print with check option", K(ret));
  } else if (OB_FAIL(print_for_update())) {
    LOG_WARN("fail to print for update", K(ret), K(*stmt_));
  } else {
    // do-nothing
  }

  return ret;
}

int ObSelectStmtPrinter::print_select()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(stmt_) || OB_ISNULL(buf_) || OB_ISNULL(pos_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_ is NULL or buf_ is NULL or pos_ is NULL", K(ret));
  } else if (!stmt_->is_select_stmt()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Not a valid select stmt", K(stmt_->get_stmt_type()), K(ret));
  } else {
    const ObSelectStmt *select_stmt = static_cast<const ObSelectStmt*>(stmt_);
    DATA_PRINTF("select ");

    if (OB_SUCC(ret)) {
      bool is_oracle_mode = lib::is_oracle_mode();
      if (OB_FAIL(print_hint())) { // hint
        LOG_WARN("fail to print hint", K(ret), K(*select_stmt));
      } else if (select_stmt->is_unpivot_select()) {
        DATA_PRINTF(" * ");
      } else {
        if (select_stmt->has_distinct()) { // distinct
          DATA_PRINTF("distinct ");
        }
        if (select_stmt->get_select_item_size() == 0) {
          DATA_PRINTF("1 ");
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt->get_select_item_size(); ++i) {
          const SelectItem &select_item = select_stmt->get_select_item(i);
          OZ (set_synonym_name_recursively(select_item.expr_, stmt_));
          if (select_item.is_implicit_added_ || select_item.implicit_filled_) {
            continue;
          }
          ObRawExpr *expr = select_item.expr_;
          // mysql会对view_definition最顶层stmt的select_item添加别名(仅最顶层)
          // 别名规则见case：
          // create view(a,b) v as select c1,c2 as alias from t1;
          // 替换后：              select c1 as a, c2 as b from t1
          // 因此需要将最顶层stmt中相应别名表达式的func_name替换成column_name
          bool need_add_alias = need_print_alias() || select_item.is_real_alias_;
          if (OB_SUCC(ret)) {
            ObRawExpr *tmp_expr = expr;
            if (OB_ISNULL(expr)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("expr is null", K(ret), K(expr));
            } else if (OB_FAIL(ObRawExprUtils::erase_inner_added_exprs(tmp_expr, expr))) {
              LOG_WARN("erase inner cast expr failed", K(ret));
            } else if (OB_ISNULL(expr)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("expr is null");
            } else if (need_add_alias && NULL != column_list_ && select_item.is_real_alias_) {
              expr->set_alias_column_name(column_list_->at(i));
            }
          }
          if (OB_SUCC(ret)) {
            if (OB_FAIL(expr_printer_.do_print(expr, T_FIELD_LIST_SCOPE))) {
              LOG_WARN("fail to print select expr", K(ret));
            }
          }

          if (OB_SUCC(ret) && need_add_alias) {
            ObString alias_string;
            if (NULL != column_list_) {
              alias_string = column_list_->at(i);
            } else if (!select_item.alias_name_.empty()) {
              alias_string = select_item.alias_name_;
            } else {
              alias_string = select_item.expr_name_;
            }
            /* oracle模式下，由于部分函数的别名可能出现双引号“”，将导致二次解析出错，因此需要将这些双引号去掉
            *
            */
            ObArenaAllocator arena_alloc;
            if (is_oracle_mode && OB_FAIL(remove_double_quotation_for_string(alias_string,
                                                                             arena_alloc))) {
              LOG_WARN("failed to remove double quotation for string", K(ret));
            } else {
              DATA_PRINTF(" AS ");
              PRINT_IDENT_WITH_QUOT(alias_string);
            }
          }
          DATA_PRINTF(",");
        }
        if (OB_SUCC(ret)) {
          --*pos_;
        }
      }
      // select_items
    }
  }
  return ret;
}

int ObSelectStmtPrinter::remove_double_quotation_for_string(ObString &alias_string,
                                                            ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  const char *str = alias_string.ptr();
  int32_t length = alias_string.length();
  if (OB_ISNULL(str) && OB_UNLIKELY(length > 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(str), K(length), K(ret));
  } else if (length > 0) {
    char *buff = static_cast<char *>(allocator.alloc(length));
    if (OB_ISNULL(buff)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory failed.", K(buff), K(length), K(alias_string), K(ret));
    } else {
      int32_t len_str = 0;
      for (int32_t i = 0; len_str < length && i < length; ++i) {
        if (str[i] == '\"') {
          /*do nothing*/
        } else {
          buff[len_str++] = str[i];
        }
      }
      alias_string.assign_ptr(buff, static_cast<int64_t>(len_str));
    }
  }
  return ret;
}

int ObSelectStmtPrinter::print_start_with()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(stmt_) || OB_ISNULL(buf_) || OB_ISNULL(pos_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_ is NULL or buf_ is NULL or pos_ is NULL", K(ret));
  } else if (!stmt_->is_select_stmt()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Not a valid select stmt", K(stmt_->get_stmt_type()), K(ret));
  } else {
    const ObSelectStmt *select_stmt = static_cast<const ObSelectStmt*>(stmt_);
    const ObIArray<ObRawExpr*> &start_with_exprs = select_stmt->get_start_with_exprs();
    int64_t start_with_exprs_size = start_with_exprs.count();
    if (start_with_exprs_size > 0) {
      DATA_PRINTF(" start with ");
      for (int64_t i = 0; OB_SUCC(ret) && i < start_with_exprs_size; ++i) {
        if (OB_FAIL(expr_printer_.do_print(start_with_exprs.at(i), T_NONE_SCOPE))) {
          LOG_WARN("fail to print having expr", K(ret));
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

int ObSelectStmtPrinter::print_connect_by()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(stmt_) || OB_ISNULL(buf_) || OB_ISNULL(pos_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_ is NULL or buf_ is NULL or pos_ is NULL", K(ret));
  } else if (!stmt_->is_select_stmt()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Not a valid select stmt", K(stmt_->get_stmt_type()), K(ret));
  } else {
    const ObSelectStmt *select_stmt = static_cast<const ObSelectStmt*>(stmt_);
    const ObIArray<ObRawExpr*> &connect_by_exprs = select_stmt->get_connect_by_exprs();
    int64_t connect_by_exprs_size = connect_by_exprs.count();
    if (connect_by_exprs_size > 0) {
      DATA_PRINTF(" connect by ");
      if (select_stmt->is_nocycle()) {
        DATA_PRINTF("nocycle ");
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < connect_by_exprs_size; ++i) {
        if (OB_FAIL(expr_printer_.do_print(connect_by_exprs.at(i), T_NONE_SCOPE))) {
          LOG_WARN("fail to print having expr", K(ret));
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

int ObSelectStmtPrinter::print_group_by()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt_) || OB_ISNULL(buf_) || OB_ISNULL(pos_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_ is NULL or buf_ is NULL or pos_ is NULL", K(ret));
  } else if (!stmt_->is_select_stmt()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Not a valid select stmt", K(stmt_->get_stmt_type()), K(ret));
  } else {
    const ObSelectStmt *select_stmt = static_cast<const ObSelectStmt*>(stmt_);
    const ObIArray<ObRawExpr*> &group_exprs = select_stmt->get_group_exprs();
    const ObIArray<ObRawExpr*> &rollup_exprs = select_stmt->get_rollup_exprs();
    const ObIArray<ObGroupingSetsItem> &groupingsets_items = select_stmt->get_grouping_sets_items();
    const ObIArray<ObRollupItem> &rollup_items = select_stmt->get_rollup_items();
    const ObIArray<ObCubeItem> &cube_items = select_stmt->get_cube_items();
    int64_t group_exprs_size = group_exprs.count();
    int64_t rollup_exprs_size = rollup_exprs.count();
    int64_t grouping_sets_size = groupingsets_items.count();
    int64_t rollup_size = rollup_items.count();
    int64_t cube_size = cube_items.count();
    if (group_exprs_size + rollup_exprs_size + grouping_sets_size + rollup_size + cube_size > 0) {
      DATA_PRINTF(" group by ");
      if (OB_SUCC(ret) && grouping_sets_size > 0) {
        for (int64_t i = 0; OB_SUCC(ret) && i < grouping_sets_size; ++i) {
          const ObIArray<ObGroupbyExpr> &grouping_sets_exprs =
                                                      groupingsets_items.at(i).grouping_sets_exprs_;
          DATA_PRINTF(" grouping sets (");
          for (int64_t j = 0; OB_SUCC(ret) && j < grouping_sets_exprs.count(); ++j) {
            const ObIArray<ObRawExpr*> &groupby_exprs = grouping_sets_exprs.at(j).groupby_exprs_;
            DATA_PRINTF("(");
            for (int64_t k = 0; OB_SUCC(ret) && k < groupby_exprs.count(); ++k) {
              if (OB_FAIL(expr_printer_.do_print(groupby_exprs.at(k), T_GROUP_SCOPE))) {
                LOG_WARN("fail to print group expr", K(ret));
              } else if (k < groupby_exprs.count() - 1) {
                DATA_PRINTF(",");
              }
            }
            DATA_PRINTF("),");
          }
          if (OB_SUCC(ret)) {
            if (OB_FAIL(print_rollup_items(groupingsets_items.at(i).rollup_items_))) {
              LOG_WARN("failed to print rollup items", K(ret));
            }
          }
          if (OB_SUCC(ret)) {
            if (OB_FAIL(print_cube_items(groupingsets_items.at(i).cube_items_))) {
              LOG_WARN("failed to print cube items", K(ret));
            }
          }
          if (OB_SUCC(ret)) {
            --*pos_;
          }
          DATA_PRINTF("),");
        }
      }
      // print cube
      if (OB_SUCC(ret) && cube_size > 0) {
        if (OB_FAIL(print_cube_items(cube_items))) {
          LOG_WARN("failed to print cube items", K(ret));
        }
      }
      // print exprs
      for (int64_t i = 0; OB_SUCC(ret) && i < group_exprs_size; ++i) {
        if (OB_FAIL(print_expr_except_const_number(group_exprs.at(i), T_GROUP_SCOPE))) {
          LOG_WARN("fail to print group expr", K(ret));
        }
        DATA_PRINTF(",");
      }
      // print rollup
      if (OB_SUCC(ret)) {
        if (lib::is_mysql_mode() && rollup_exprs_size > 0) {
          for (int64_t i = 0; OB_SUCC(ret) && i < rollup_exprs_size; ++i) {
            if (OB_FAIL(print_expr_except_const_number(rollup_exprs.at(i), T_GROUP_SCOPE))) {
              LOG_WARN("fail to print group expr", K(ret));
            }
            DATA_PRINTF(",");
          }
          if (OB_SUCC(ret)) {
            --*pos_;
          }
          DATA_PRINTF(" with rollup ");
        } else if (lib::is_oracle_mode() && (rollup_size > 0 || rollup_exprs_size > 0)) {
          if (OB_FAIL(print_rollup_items(rollup_items))) {
            LOG_WARN("failed to print rollup items", K(ret));
          } else if (rollup_exprs_size > 0) {
            DATA_PRINTF(" rollup( ");
            for (int64_t i = 0; OB_SUCC(ret) && i < rollup_exprs_size; ++i) {
              if (OB_FAIL(print_expr_except_const_number(rollup_exprs.at(i), T_GROUP_SCOPE))) {
                LOG_WARN("fail to print group expr", K(ret));
              }
              DATA_PRINTF(",");
            }
            if (OB_SUCC(ret)) {
              --*pos_;
            }
            DATA_PRINTF("),");
          }
        }
      }
      // remove ","
      if (OB_SUCC(ret)) {
        --*pos_;
      }
    }
  }

  return ret;
}

int ObSelectStmtPrinter::print_rollup_items(const ObIArray<ObRollupItem> &rollup_items)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < rollup_items.count(); ++i) {
    const ObIArray<ObGroupbyExpr> &rollup_list_exprs = rollup_items.at(i).rollup_list_exprs_;
    DATA_PRINTF(" rollup (");
    for (int64_t j = 0; OB_SUCC(ret) && j < rollup_list_exprs.count(); ++j) {
      const ObIArray<ObRawExpr*> &groupby_exprs = rollup_list_exprs.at(j).groupby_exprs_;
      DATA_PRINTF("(");
      for (int64_t k = 0; OB_SUCC(ret) && k < groupby_exprs.count(); ++k) {
        if (OB_FAIL(expr_printer_.do_print(groupby_exprs.at(k), T_GROUP_SCOPE))) {
          LOG_WARN("fail to print group expr", K(ret));
        } else if (k < groupby_exprs.count() - 1) {
          DATA_PRINTF(",");
        }
      }
      if (j < rollup_list_exprs.count() - 1) {
        DATA_PRINTF("),");
      } else {
        DATA_PRINTF(")");
      }
    }
    DATA_PRINTF("),");
  }
  return ret;
}

int ObSelectStmtPrinter::print_cube_items(const ObIArray<ObCubeItem> &cube_items)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < cube_items.count(); ++i) {
    const ObIArray<ObGroupbyExpr> &cube_list_exprs = cube_items.at(i).cube_list_exprs_;
    DATA_PRINTF(" cube (");
    for (int64_t j = 0; OB_SUCC(ret) && j < cube_list_exprs.count(); ++j) {
      const ObIArray<ObRawExpr*> &groupby_exprs = cube_list_exprs.at(j).groupby_exprs_;
      DATA_PRINTF("(");
      for (int64_t k = 0; OB_SUCC(ret) && k < groupby_exprs.count(); ++k) {
        if (OB_FAIL(expr_printer_.do_print(groupby_exprs.at(k), T_GROUP_SCOPE))) {
          LOG_WARN("fail to print group expr", K(ret));
        } else if (k < groupby_exprs.count() - 1) {
          DATA_PRINTF(",");
        }
      }
      if (j < cube_list_exprs.count() - 1) {
        DATA_PRINTF("),");
      } else {
        DATA_PRINTF(")");
      }
    }
    DATA_PRINTF("),");
  }
  return ret;
}

int ObSelectStmtPrinter::print_having()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(stmt_) || OB_ISNULL(buf_) || OB_ISNULL(pos_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_ is NULL or buf_ is NULL or pos_ is NULL", K(ret));
  } else if (!stmt_->is_select_stmt()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Not a valid select stmt", K(stmt_->get_stmt_type()), K(ret));
  } else {
    const ObSelectStmt *select_stmt = static_cast<const ObSelectStmt*>(stmt_);
    const ObIArray<ObRawExpr*> &having_exprs = select_stmt->get_having_exprs();
    int64_t having_exprs_size = having_exprs.count();
    if (having_exprs_size > 0) {
      DATA_PRINTF(" having ");
      for (int64_t i = 0; OB_SUCC(ret) && i < having_exprs_size; ++i) {
        if (OB_FAIL(expr_printer_.do_print(having_exprs.at(i), T_HAVING_SCOPE))) {
          LOG_WARN("fail to print having expr", K(ret));
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

int ObSelectStmtPrinter::print_order_by()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(stmt_) || OB_ISNULL(buf_) || OB_ISNULL(pos_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_ is NULL or buf_ is NULL or pos_ is NULL", K(ret));
  } else if (!stmt_->is_select_stmt()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Not a valid select stmt", K(stmt_->get_stmt_type()), K(ret));
  } else {
    const ObSelectStmt *select_stmt = static_cast<const ObSelectStmt*>(stmt_);
    ObArenaAllocator alloc;
    ObConstRawExpr expr(alloc);
    int64_t order_item_size = select_stmt->get_order_item_size();
    if (order_item_size > 0) {
      if (select_stmt->is_order_siblings()) {
        DATA_PRINTF(" order siblings by");
      } else {
        DATA_PRINTF(" order by");
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < order_item_size; ++i) {
        const OrderItem &order_item = select_stmt->get_order_item(i);
        ObRawExpr *order_expr = order_item.expr_;
        int64_t sel_item_pos = -1;
        if (OB_ISNULL(order_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null", K(ret));
        } else if (T_FUN_SYS_CAST == order_expr->get_expr_type() &&
                   CM_IS_IMPLICIT_CAST(order_expr->get_extra())) {
          order_expr = order_expr->get_param_expr(0);
        }
        for (int64_t j = 0; OB_SUCC(ret) && j < select_stmt->get_select_item_size(); ++j) {
          ObRawExpr *select_expr = select_stmt->get_select_item(j).expr_;
          if (OB_ISNULL(select_expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected null", K(ret));
          } else if (T_FUN_SYS_CAST == select_expr->get_expr_type() &&
                     CM_IS_IMPLICIT_CAST(select_expr->get_extra())) {
            select_expr = select_expr->get_param_expr(0);
          }
          if (order_expr == select_expr) {
            sel_item_pos = j + 1;
            break;
          }
        }
        if (sel_item_pos != -1) {
          DATA_PRINTF(" %ld", sel_item_pos);
        } else {
          DATA_PRINTF(" ");
          if (OB_FAIL(print_expr_except_const_number(order_item.expr_, T_ORDER_SCOPE))) {
            LOG_WARN("fail to print order by expr", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          if (lib::is_mysql_mode()) {
            if (is_descending_direction(order_item.order_type_)) {
              DATA_PRINTF(" desc");
            }
          } else if (order_item.order_type_ == NULLS_FIRST_ASC) {
            DATA_PRINTF(" asc nulls first");
          } else if (order_item.order_type_ == NULLS_LAST_ASC) {//use default value
            /*do nothing*/
          } else if (order_item.order_type_ == NULLS_FIRST_DESC) {//use default value
            DATA_PRINTF(" desc");
          } else if (order_item.order_type_ == NULLS_LAST_DESC) {
            DATA_PRINTF(" desc nulls last");
          } else {/*do nothing*/}
          DATA_PRINTF(",");
        }
      }
      if (OB_SUCC(ret)) {
        --*pos_;
      }
    }
  }

  return ret;
}

int ObSelectStmtPrinter::print_for_update()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(stmt_) || OB_ISNULL(buf_) || OB_ISNULL(pos_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_ is NULL or buf_ is NULL or pos_ is NULL", K(ret));
  } else if (!stmt_->is_select_stmt()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Not a valid select stmt", K(stmt_->get_stmt_type()), K(ret));
  } else if (is_oracle_mode()) {
    if (is_root_stmt()) {
      const ObSelectStmt *select_stmt = static_cast<const ObSelectStmt*>(stmt_);
      bool has_for_update_ = false;
      int64_t wait_time = -1;
      bool skip_locked = false;
      for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt->get_table_size(); ++i) {
        const TableItem *table = NULL;
        if (OB_ISNULL(table = select_stmt->get_table_item(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("table item is null", K(ret));
        } else if (table->for_update_) {
          has_for_update_ = true;
          wait_time = table->for_update_wait_us_;
          skip_locked = table->skip_locked_;
          break;
        }
      }
      if (OB_SUCC(ret) && has_for_update_) {
        DATA_PRINTF(" for update");
        const ObIArray<ObColumnRefRawExpr *> &for_update_cols =
            select_stmt->get_for_update_columns();
        if (OB_SUCC(ret) && !select_stmt->get_for_update_columns().empty()) {
          DATA_PRINTF(" of");
          for (int64_t i = 0; OB_SUCC(ret) && i < for_update_cols.count(); ++i) {
            DATA_PRINTF(" ");
            if (OB_FAIL(ret)) {
              // do nothing
            } else if (OB_FAIL(expr_printer_.do_print(for_update_cols.at(i), T_NONE_SCOPE))) {
              LOG_WARN("fail to print for update column", K(ret));
            } else if (i != for_update_cols.count() - 1) {
              DATA_PRINTF(",");
            }
          }
        }
        if (OB_SUCC(ret) && wait_time >= 0) {
          if (wait_time > 0) {
            DATA_PRINTF(" wait %lld", wait_time / 1000000LL);
          } else if (skip_locked) {
            DATA_PRINTF(" skip locked");
          } else {
            DATA_PRINTF(" nowait");
          }
        }
      }
    }
  } else {
    const ObSelectStmt *select_stmt = static_cast<const ObSelectStmt*>(stmt_);
    if (select_stmt->get_table_size() > 0) {
      const TableItem *table_item = select_stmt->get_table_item(0);
      if (OB_ISNULL(table_item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table item is NULL", K(ret));
      } else if (table_item->for_update_) {
        DATA_PRINTF(" for update");
        if (OB_SUCC(ret) && table_item->for_update_wait_us_ > 0) {
          DATA_PRINTF(" wait %lld", table_item->for_update_wait_us_ / 1000000LL);
        }
      } else { /*do nothing*/ }
    }
  }

  return ret;
}

int ObSelectStmtPrinter::print_with_check_option()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt_) || OB_ISNULL(buf_) || OB_ISNULL(pos_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_ is NULL or buf_ is NULL or pos_ is NULL", K(ret));
  } else if (!stmt_->is_select_stmt()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Not a valid select stmt", K(stmt_->get_stmt_type()), K(ret));
  } else {
    const ObSelectStmt *select_stmt = static_cast<const ObSelectStmt*>(stmt_);
    if (select_stmt->is_view_stmt()) {
      /* only print 'with check option' of subquery.
       * with check option of view is printed in ObSchemaPrinter::print_view_definiton.
       * otherwise it may cause a syntax error when ViewResolver resolve definition of a view.
       * case: create view v as select * from t with check option.
       * if we print 'with check option' of a view here, definition of the view is
       * 'select * from t with check option', which will cause a syntax error in both mysql and oracle mode.
      */
    } else {
      ViewCheckOption view_check_option = select_stmt->get_check_option();
      if (VIEW_CHECK_OPTION_CASCADED == view_check_option) {
        DATA_PRINTF(" with check option");
      } else if (VIEW_CHECK_OPTION_LOCAL == view_check_option) {
        DATA_PRINTF(" with local check option");
      }
    }
  }
  return ret;
}

int ObSelectStmtPrinter::find_recursive_cte_table(const ObSelectStmt* stmt, TableItem* &table)
{
  int ret = OB_SUCCESS;
  table = NULL;
  ObSelectStmt* set_query = NULL;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  } else if (!stmt->is_recursive_union() ||
             OB_ISNULL(set_query=stmt->get_set_query(1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expect recurisve cte stmt", K(ret));
  }
  for (int i = 0; OB_SUCC(ret) && !table && i < set_query->get_table_items().count(); ++i) {
    TableItem *table_item = set_query->get_table_item(i);
    if (OB_ISNULL(table_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null table item", K(ret));
    } else if (!table_item->is_fake_cte_table()) {
      //do nothing
    } else {
      table = table_item;
    }
  }
  return ret;
}

} //end of namespace sql
} //end of namespace oceanbase
