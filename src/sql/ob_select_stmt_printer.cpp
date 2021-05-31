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
namespace oceanbase {
using namespace common;
namespace sql {

void ObSelectStmtPrinter::init(
    char* buf, int64_t buf_len, int64_t* pos, ObSelectStmt* stmt, ObIArray<ObString>* column_list, bool is_set_subquery)
{
  ObDMLStmtPrinter::init(buf, buf_len, pos, stmt);
  column_list_ = column_list;
  is_set_subquery_ = is_set_subquery;
}

int ObSelectStmtPrinter::do_print()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited!", K(ret));
  } else if (OB_ISNULL(stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt should not be NULL", K(ret));
  } else if (!stmt_->is_select_stmt()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Not a valid select stmt", K(stmt_->get_stmt_type()), K(ret));
  } else {
    const ObSelectStmt* select_stmt = static_cast<const ObSelectStmt*>(stmt_);
    if (OB_UNLIKELY(NULL != column_list_ && column_list_->count() != select_stmt->get_select_item_size())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column_list size should be equal select_item size",
          K(ret),
          K(column_list_->count()),
          K(select_stmt->get_select_item_size()));
    } else {
      expr_printer_.init(buf_, buf_len_, pos_, print_params_);
      if (stmt_->is_unpivot_select()) {
        if (OB_FAIL(print_unpivot())) {
          LOG_WARN("fail to print_unpivot", KPC(stmt_->get_transpose_item()), K(ret));
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
    const ObSelectStmt* select_stmt = static_cast<const ObSelectStmt*>(stmt_);
    if (!select_stmt->is_root_stmt() || is_set_subquery_) {
      DATA_PRINTF("(");
    }
    if (OB_SUCC(ret)) {
      if (select_stmt->get_generated_cte_count() != 0 && OB_FAIL(print_with())) {
        LOG_WARN("print with failed");
      }
    }
    if (OB_SUCC(ret)) {
      if (select_stmt->has_set_op()) {
        // union, intersect, except
        if (OB_FAIL(print_set_op_stmt())) {
          LOG_WARN("fail to print set_op stmt", K(ret), K(*stmt_));
        }
      } else if (OB_FAIL(print_basic_stmt())) {
        LOG_WARN("fail to print basic stmt", K(ret), K(*stmt_));
      }
    }

    if (OB_SUCC(ret)) {
      if (!select_stmt->is_root_stmt() || is_set_subquery_) {
        DATA_PRINTF(")");
      }
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
  } else if (OB_UNLIKELY(!stmt_->is_select_stmt()) || OB_UNLIKELY(!stmt_->is_unpivot_select()) ||
             OB_UNLIKELY(stmt_->get_table_items().count() != 1) || OB_ISNULL(stmt_->get_table_item(0)->ref_query_) ||
             OB_UNLIKELY(stmt_->get_table_item(0)->ref_query_->get_table_items().count() != 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Not a valid select stmt", K(stmt_->get_stmt_type()), K(stmt_->get_table_items().count()), K(ret));
  } else {
    const ObSelectStmt* select_stmt = static_cast<const ObSelectStmt*>(stmt_);
    if (!select_stmt->is_root_stmt()) {
      DATA_PRINTF("(");
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

    const TransposeItem& transpose_item = *select_stmt->get_transpose_item();

    if (OB_SUCC(ret)) {
      DATA_PRINTF(" UNPIVOT %s (", (transpose_item.is_include_null() ? "INCLUDE NULLS" : "EXCLUDE NULLS"));
    }

    if (OB_SUCC(ret)) {
      if (transpose_item.unpivot_columns_.count() == 1) {
        DATA_PRINTF("%.*s", transpose_item.unpivot_columns_[0].length(), transpose_item.unpivot_columns_[0].ptr());
      } else {
        DATA_PRINTF("(");
        for (int64_t i = 0; i < transpose_item.unpivot_columns_.count() && OB_SUCC(ret); ++i) {
          DATA_PRINTF("%.*s,", transpose_item.unpivot_columns_[i].length(), transpose_item.unpivot_columns_[i].ptr());
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
        DATA_PRINTF("%.*s", transpose_item.for_columns_[0].length(), transpose_item.for_columns_[0].ptr());
      } else {
        DATA_PRINTF("(");
        for (int64_t i = 0; i < transpose_item.for_columns_.count() && OB_SUCC(ret); ++i) {
          DATA_PRINTF("%.*s,", transpose_item.for_columns_[i].length(), transpose_item.for_columns_[i].ptr());
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
        const TransposeItem::InPair& in_pair = transpose_item.in_pairs_[i];
        if (in_pair.column_names_.count() == 1) {
          DATA_PRINTF("%.*s", in_pair.column_names_[0].length(), in_pair.column_names_[0].ptr());
        } else {
          DATA_PRINTF("(");
          for (int64_t j = 0; j < in_pair.column_names_.count() && OB_SUCC(ret); ++j) {
            DATA_PRINTF("%.*s,", in_pair.column_names_[j].length(), in_pair.column_names_[j].ptr());
          }
          if (OB_SUCC(ret)) {
            --(*pos_);
            DATA_PRINTF(")");
          }
        }

        if (OB_SUCC(ret) && !in_pair.exprs_.empty()) {
          DATA_PRINTF(" AS ");
          if (OB_SUCC(ret)) {
            HEAP_VAR(char[OB_MAX_DEFAULT_VALUE_LENGTH], expr_str_buf)
            {
              MEMSET(expr_str_buf, 0, sizeof(expr_str_buf));
              if (in_pair.exprs_.count() == 1) {
                ObRawExpr* expr = in_pair.exprs_.at(0);
                int64_t pos = 0;
                ObRawExprPrinter expr_printer(expr_str_buf, OB_MAX_DEFAULT_VALUE_LENGTH, &pos, stmt_->tz_info_);
                if (OB_FAIL(expr_printer.do_print(expr, T_NONE_SCOPE, true))) {
                  LOG_WARN("print expr definition failed", KPC(expr), K(ret));
                } else {
                  DATA_PRINTF("%.*s", static_cast<int32_t>(pos), expr_str_buf);
                }
              } else {
                DATA_PRINTF("(");
                for (int64_t j = 0; j < in_pair.exprs_.count() && OB_SUCC(ret); ++j) {
                  ObRawExpr* expr = in_pair.exprs_.at(j);
                  int64_t pos = 0;
                  ObRawExprPrinter expr_printer(expr_str_buf, OB_MAX_DEFAULT_VALUE_LENGTH, &pos, stmt_->tz_info_);
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

    if (OB_SUCC(ret)) {
      if (!select_stmt->is_root_stmt()) {
        DATA_PRINTF(")");
      }
    }
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
    const ObSelectStmt* select_stmt = static_cast<const ObSelectStmt*>(stmt_);
    ObSEArray<ObSelectStmt*, 2> child_stmts;
    if (!select_stmt->has_set_op() || 2 > select_stmt->get_set_query().count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN(
          "stmt_ should have set_op", K(ret), K(select_stmt->has_set_op()), K(select_stmt->get_set_query().count()));
    } else if (OB_FAIL(child_stmts.assign(select_stmt->get_set_query()))) {
      LOG_WARN("failed to assign stmts", K(ret));
    } else if (OB_ISNULL(child_stmts.at(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("child_stmt should not be NULL", K(ret));
    } else {
      if (select_stmt->get_children_swapped()) {
        std::swap(child_stmts.at(0), child_stmts.at(1));
      }
      bool is_set_subquery = true;
      ObSelectStmtPrinter stmt_printer(
          buf_, buf_len_, pos_, child_stmts.at(0), print_params_, column_list_, is_set_subquery);
      ObString set_op_str = ObString::make_string(ObSelectStmt::set_operator_str(select_stmt->get_set_op()));
      if (OB_FAIL(stmt_printer.do_print())) {
        LOG_WARN("fail to print left stmt", K(ret), K(*child_stmts.at(0)));
      }
      for (int64_t i = 1; OB_SUCC(ret) && i < child_stmts.count(); ++i) {
        DATA_PRINTF(" %.*s ", LEN_AND_PTR(set_op_str));  // print set_op
        if (!select_stmt->is_set_distinct()) {
          DATA_PRINTF(" all ");
        }
        if (OB_FAIL(ret)) {
        } else if (OB_ISNULL(child_stmts.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("child_stmt should not be NULL", K(ret));
        } else {
          stmt_printer.init(buf_, buf_len_, pos_, child_stmts.at(i), column_list_, is_set_subquery);
          if (OB_FAIL(stmt_printer.do_print())) {
            LOG_WARN("fail to print child stmt", K(ret));
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(print_order_by())) {
        LOG_WARN("fail to print order by", K(ret));
      } else if (OB_FAIL(print_limit())) {
        LOG_WARN("fail to print limit", K(ret));
      } else if (OB_FAIL(print_fetch())) {
        LOG_WARN("fail to print fetch", K(ret));
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
    const ObSelectStmt* select_stmt = static_cast<const ObSelectStmt*>(stmt_);
    DATA_PRINTF("select ");

    if (OB_SUCC(ret)) {
      bool is_oracle_mode = share::is_oracle_mode();
      if (OB_FAIL(print_hint())) {  // hint
        LOG_WARN("fail to print hint", K(ret), K(*select_stmt));
      } else if (select_stmt->is_unpivot_select()) {
        DATA_PRINTF(" * ");
      } else {
        if (select_stmt->has_distinct()) {  // distinct
          DATA_PRINTF("distinct ");
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt->get_select_item_size(); ++i) {
          const SelectItem& select_item = select_stmt->get_select_item(i);
          if (select_item.is_implicit_added_)
            continue;
          ObRawExpr* expr = select_item.expr_;
          bool need_add_alias = force_col_alias_;
          if (OB_ISNULL(expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("select item expr is null");
          } else if (select_item.is_real_alias_) {
            need_add_alias = true;
          } else if (select_stmt->is_root_stmt()) {
            if (print_params_.is_show_create_view_) {
              need_add_alias = true;
            }
          } else if (OB_ISNULL(select_stmt->get_parent_namespace_stmt())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("upper_scope_stmt is NULL", K(ret));
          } else if (select_stmt->get_parent_namespace_stmt()->is_delete_stmt() ||
                     select_stmt->get_parent_namespace_stmt()->is_update_stmt() ||
                     (select_stmt->get_parent_namespace_stmt()->is_root_stmt() && is_set_subquery_)) {
            need_add_alias = true;
          }
          if (OB_SUCC(ret)) {
            ObRawExpr* tmp_expr = expr;
            if (OB_FAIL(ObRawExprUtils::erase_inner_added_exprs(tmp_expr, expr))) {
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
            bool have_double_quotation = false;
            if (NULL != column_list_) {
              alias_string = column_list_->at(i);
            } else if (!select_item.alias_name_.empty()) {
              alias_string = select_item.alias_name_;
            } else {
              alias_string = select_item.expr_name_;
            }
            ObArenaAllocator arena_alloc;
            if (is_oracle_mode && OB_FAIL(remove_double_quotation_for_string(alias_string, arena_alloc))) {
              LOG_WARN("failed to remove double quotation for string", K(ret));
            } else {
              DATA_PRINTF(is_oracle_mode ? " AS \"%.*s\"" : " AS `%.*s`", LEN_AND_PTR(alias_string));
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

int ObSelectStmtPrinter::remove_double_quotation_for_string(ObString& alias_string, ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;
  const char* str = alias_string.ptr();
  int32_t length = alias_string.length();
  if (OB_ISNULL(str) && OB_UNLIKELY(length > 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(str), K(length), K(ret));
  } else if (length > 0) {
    char* buff = static_cast<char*>(allocator.alloc(length));
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
    const ObSelectStmt* select_stmt = static_cast<const ObSelectStmt*>(stmt_);
    const ObIArray<ObRawExpr*>& start_with_exprs = select_stmt->get_start_with_exprs();
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
        *pos_ -= 5;  // strlen(" and ")
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
    const ObSelectStmt* select_stmt = static_cast<const ObSelectStmt*>(stmt_);
    const ObIArray<ObRawExpr*>& connect_by_exprs = select_stmt->get_connect_by_exprs();
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
        *pos_ -= 5;  // strlen(" and ")
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
    const ObSelectStmt* select_stmt = static_cast<const ObSelectStmt*>(stmt_);
    const ObIArray<ObRawExpr*>& group_exprs = select_stmt->get_group_exprs();
    const ObIArray<ObRawExpr*>& rollup_exprs = select_stmt->get_rollup_exprs();
    const ObIArray<ObGroupingSetsItem>& groupingsets_items = select_stmt->get_grouping_sets_items();
    const ObIArray<ObMultiRollupItem>& multi_rollup_items = select_stmt->get_multi_rollup_items();
    int64_t group_exprs_size = group_exprs.count();
    int64_t rollup_exprs_size = rollup_exprs.count();
    int64_t grouping_sets_size = groupingsets_items.count();
    int64_t multi_rollup_size = multi_rollup_items.count();
    if (group_exprs_size + rollup_exprs_size + grouping_sets_size + multi_rollup_size > 0) {
      DATA_PRINTF(" group by ");
      if (OB_SUCC(ret) && grouping_sets_size > 0) {
        for (int64_t i = 0; OB_SUCC(ret) && i < grouping_sets_size; ++i) {
          const ObIArray<ObGroupbyExpr>& grouping_sets_exprs = groupingsets_items.at(i).grouping_sets_exprs_;
          DATA_PRINTF(" grouping sets (");
          for (int64_t j = 0; OB_SUCC(ret) && j < grouping_sets_exprs.count(); ++j) {
            const ObIArray<ObRawExpr*>& groupby_exprs = grouping_sets_exprs.at(j).groupby_exprs_;
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
            if (OB_FAIL(print_multi_rollup_items(groupingsets_items.at(i).multi_rollup_items_))) {
              LOG_WARN("failed to print multi rollup items", K(ret));
            }
          }
          if (OB_SUCC(ret)) {
            --*pos_;
          }
          DATA_PRINTF("),");
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < group_exprs_size; ++i) {
        if (OB_FAIL(expr_printer_.do_print(group_exprs.at(i), T_GROUP_SCOPE))) {
          LOG_WARN("fail to print group expr", K(ret));
        }
        DATA_PRINTF(",");
      }
      if (OB_SUCC(ret)) {
        if (rollup_exprs_size > 0) {
          if (lib::is_oracle_mode()) {
            DATA_PRINTF(" rollup( ");
          } else { /* do nothing. */
          }
          for (int64_t i = 0; OB_SUCC(ret) && i < rollup_exprs_size; ++i) {
            if (OB_FAIL(expr_printer_.do_print(rollup_exprs.at(i), T_GROUP_SCOPE))) {
              LOG_WARN("fail to print group expr", K(ret));
            }
            DATA_PRINTF(",");
          }
        } else if (multi_rollup_size > 0) {
          if (OB_FAIL(print_multi_rollup_items(multi_rollup_items))) {
            LOG_WARN("failed to print multi rollup items", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          --*pos_;
        }
        if (rollup_exprs_size > 0) {
          if (lib::is_oracle_mode()) {
            DATA_PRINTF(" ) ");
          } else {
            DATA_PRINTF(" with rollup ");
          }
        } else { /* do nothing. */
        }
      }
    } else { /* do nothing. */
    }
  }

  return ret;
}

int ObSelectStmtPrinter::print_multi_rollup_items(const ObIArray<ObMultiRollupItem>& rollup_items)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < rollup_items.count(); ++i) {
    const ObIArray<ObGroupbyExpr>& rollup_list_exprs = rollup_items.at(i).rollup_list_exprs_;
    DATA_PRINTF(" rollup (");
    for (int64_t j = 0; OB_SUCC(ret) && j < rollup_list_exprs.count(); ++j) {
      const ObIArray<ObRawExpr*>& groupby_exprs = rollup_list_exprs.at(j).groupby_exprs_;
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
    const ObSelectStmt* select_stmt = static_cast<const ObSelectStmt*>(stmt_);
    const ObIArray<ObRawExpr*>& having_exprs = select_stmt->get_having_exprs();
    int64_t having_exprs_size = having_exprs.count();
    if (having_exprs_size > 0) {
      DATA_PRINTF(" having ");
      for (int64_t i = 0; OB_SUCC(ret) && i < having_exprs_size; ++i) {
        if (OB_FAIL(expr_printer_.do_print(having_exprs.at(i), T_NONE_SCOPE))) {
          LOG_WARN("fail to print having expr", K(ret));
        }
        DATA_PRINTF(" and ");
      }
      if (OB_SUCC(ret)) {
        *pos_ -= 5;  // strlen(" and ")
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
    const ObSelectStmt* select_stmt = static_cast<const ObSelectStmt*>(stmt_);
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
        const OrderItem& order_item = select_stmt->get_order_item(i);
        ObRawExpr* order_expr = order_item.expr_;
        int64_t sel_item_pos = -1;
        for (int64_t j = 0; j < select_stmt->get_select_item_size(); ++j) {
          if (order_item.expr_ == select_stmt->get_select_item(j).expr_) {
            sel_item_pos = j + 1;
            break;
          }
        }
        if (sel_item_pos != -1) {
          ObObjParam val;
          val.set_int(sel_item_pos);
          val.set_scale(0);
          val.set_param_meta();
          expr.set_expr_type(T_INT);
          expr.set_value(val);
          order_expr = &expr;
        }
        if (OB_SUCC(ret)) {
          DATA_PRINTF(" ");
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(expr_printer_.do_print(order_expr, T_ORDER_SCOPE))) {
            LOG_WARN("fail to print order by expr", K(ret));
          } else if (share::is_mysql_mode()) {
            if (is_descending_direction(order_item.order_type_)) {
              DATA_PRINTF(" desc");
            }
          } else if (order_item.order_type_ == NULLS_FIRST_ASC) {
            DATA_PRINTF(" asc nulls first");
          } else if (order_item.order_type_ == NULLS_LAST_ASC) {  // use default value
            /*do nothing*/
          } else if (order_item.order_type_ == NULLS_FIRST_DESC) {  // use default value
            DATA_PRINTF(" desc");
          } else if (order_item.order_type_ == NULLS_LAST_DESC) {
            DATA_PRINTF(" desc nulls last");
          } else { /*do nothing*/
          }
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
    if (is_generated_table_ || stmt_->get_parent_namespace_stmt() != NULL) {
      // do nothing
    } else {
      const ObSelectStmt* select_stmt = static_cast<const ObSelectStmt*>(stmt_);
      bool has_for_update_ = false;
      int64_t wait_time = -1;
      for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt->get_table_size(); ++i) {
        const TableItem* table = NULL;
        if (OB_ISNULL(table = select_stmt->get_table_item(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("table item is null", K(ret));
        } else if (table->for_update_) {
          has_for_update_ = true;
          wait_time = table->for_update_wait_us_;
          break;
        }
      }
      if (OB_SUCC(ret) && has_for_update_) {
        DATA_PRINTF(" for update");
        const ObIArray<ObColumnRefRawExpr*>& for_update_cols = select_stmt->get_for_update_columns();
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
          } else {
            DATA_PRINTF(" nowait");
          }
        }
      }
    }
  } else {
    const ObSelectStmt* select_stmt = static_cast<const ObSelectStmt*>(stmt_);
    if (select_stmt->get_table_size() > 0) {
      const TableItem* table_item = select_stmt->get_table_item(0);
      if (OB_ISNULL(table_item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table item is NULL", K(ret));
      } else if (table_item->for_update_) {
        DATA_PRINTF(" for update");
        if (OB_SUCC(ret) && table_item->for_update_wait_us_ > 0) {
          DATA_PRINTF(" wait %lld", table_item->for_update_wait_us_ / 1000000LL);
        }
      } else { /*do nothing*/
      }
    }
  }

  return ret;
}

// restore CTE's complete definition
int ObSelectStmtPrinter::print_with()
{
  int ret = OB_SUCCESS;
  DATA_PRINTF("WITH ");
  if (OB_SUCC(ret)) {
    const ObSelectStmt* select_stmt = static_cast<const ObSelectStmt*>(stmt_);
    const common::ObIArray<TableItem*>& cte_tables = select_stmt->get_CTE_table_items();
    int64_t this_stmt_generated_cte_count = select_stmt->get_generated_cte_count();

    for (int64_t i = 0; i < this_stmt_generated_cte_count && OB_SUCC(ret); ++i) {
      TableItem* cte_table = cte_tables.at(i);
      // print definition
      if (OB_ISNULL(cte_table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the cte tableitem can not be null", K(ret));
      } else if (TableItem::NORMAL_CTE == cte_table->cte_type_ || TableItem::RECURSIVE_CTE == cte_table->cte_type_) {
        if (OB_ISNULL(cte_table->node_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("the parser node of a cte table can not be null", K(ret));
        } else if (OB_FAIL(print_cte_define_title(cte_table))) {
          LOG_WARN("print column name failed", K(ret));
        } else if (OB_FAIL(print_table(cte_table, true))) {
          LOG_WARN("print table failed", K(ret));
        } else if ((TableItem::RECURSIVE_CTE == cte_table->cte_type_) && OB_FAIL(print_search_and_cycle(cte_table))) {
          LOG_WARN("print search and cycle failed", K(ret));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected cte type", K(ret), K(cte_table->cte_type_));
      }
      if (OB_FAIL(ret)) {
        // do nothing
      } else if (i == this_stmt_generated_cte_count - 1) {
        DATA_PRINTF(" ");
      } else {
        DATA_PRINTF(", ");
      }
    }
  }
  return ret;
}

int ObSelectStmtPrinter::print_cte_define_title(TableItem* cte_table)
{
  int ret = OB_SUCCESS;
  ObSelectStmt* sub_select_stmt = NULL;
  PRINT_TABLE_NAME(cte_table);
  if (OB_ISNULL(cte_table->node_->children_[1]) && (TableItem::RECURSIVE_CTE == cte_table->cte_type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the recursive cte must have the colume definition", K(ret));
  } else if (OB_NOT_NULL(cte_table->node_->children_[1])) {
    DATA_PRINTF("(");
    sub_select_stmt = cte_table->ref_query_;
    const ObIArray<SelectItem>& sub_select_items = sub_select_stmt->get_select_items();
    // print column
    for (int64_t i = 0; i < sub_select_items.count() && OB_SUCC(ret); ++i) {
      if (T_CTE_SEARCH_COLUMN == sub_select_items.at(i).expr_->get_expr_type() ||
          T_CTE_CYCLE_COLUMN == sub_select_items.at(i).expr_->get_expr_type()) {
        // pseudo-column no need to print
      } else {
        DATA_PRINTF("%.*s", LEN_AND_PTR(sub_select_items.at(i).alias_name_));
        if (i != sub_select_items.count() - 1 &&
            T_CTE_SEARCH_COLUMN != sub_select_items.at(i + 1).expr_->get_expr_type() &&
            T_CTE_CYCLE_COLUMN != sub_select_items.at(i + 1).expr_->get_expr_type()) {
          DATA_PRINTF(", ");
        }
      }
    }
    DATA_PRINTF(")");
  } else {
    // do nothing, the normal cte without column alias is OK
  }
  DATA_PRINTF(" as ");
  return ret;
}

int ObSelectStmtPrinter::print_search_and_cycle(TableItem* cte_table)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("PrintSelectStmt");
  ObSelectStmt* sub_select_stmt = NULL;
  if (OB_ISNULL(sub_select_stmt = cte_table->ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the cte table must have ref sub query", K(ret));
  } else if (OB_ISNULL(cte_table->node_->children_[1])) {
    // do nothing
  } else {
    const ObIArray<SelectItem>& sub_select_items = sub_select_stmt->get_select_items();
    const ObIArray<OrderItem>& search_items = sub_select_stmt->get_search_by_items();
    if (!search_items.empty()) {
      DATA_PRINTF(" search ");
      if (sub_select_stmt->is_breadth_search()) {
        DATA_PRINTF("breadth first by ");
      } else {
        DATA_PRINTF("depth first by ");
      }
    }

    for (int64_t i = 0; i < search_items.count() && OB_SUCC(ret); ++i) {
      allocator.reuse();
      ObString column_name = ((ObColumnRefRawExpr*)(search_items.at(i).expr_))->get_column_name();
      CONVERT_CHARSET_FOR_RPINT(allocator, column_name);
      DATA_PRINTF(" %.*s ", LEN_AND_PTR(column_name));
      if (share::is_mysql_mode()) {
        if (is_descending_direction(search_items.at(i).order_type_)) {
          DATA_PRINTF("DESC ");
        }
      } else if (search_items.at(i).order_type_ == NULLS_FIRST_ASC) {
        DATA_PRINTF("ASC NULLS FIRST ");
      } else if (search_items.at(i).order_type_ == NULLS_LAST_ASC) {  // use default value
        /*do nothing*/
      } else if (search_items.at(i).order_type_ == NULLS_FIRST_DESC) {  // use default value
        DATA_PRINTF("DESC ");
      } else if (search_items.at(i).order_type_ == NULLS_LAST_DESC) {
        DATA_PRINTF("DESC NULLS LAST ");
      }
      if (i != search_items.count() - 1) {
        DATA_PRINTF(", ");
      } else {
        DATA_PRINTF(" ");
      }
    }

    for (int64_t i = 0; i < sub_select_items.count() && OB_SUCC(ret); ++i) {
      if (T_CTE_SEARCH_COLUMN == sub_select_items.at(i).expr_->get_expr_type()) {
        DATA_PRINTF("set ");
        DATA_PRINTF(" %.*s ", LEN_AND_PTR(sub_select_items.at(i).alias_name_));
      }
    }

    const ObIArray<ColumnItem>& cycle_items = sub_select_stmt->get_cycle_items();
    if (!cycle_items.empty()) {
      DATA_PRINTF("cycle ");
    }

    for (int64_t i = 0; i < cycle_items.count() && OB_SUCC(ret); ++i) {
      allocator.reuse();
      ObString column_name = ((ObColumnRefRawExpr*)(cycle_items.at(i).expr_))->get_column_name();
      CONVERT_CHARSET_FOR_RPINT(allocator, column_name);
      DATA_PRINTF("%.*s ", LEN_AND_PTR(column_name));
      if (i != cycle_items.count() - 1) {
        DATA_PRINTF(", ");
      } else {
        DATA_PRINTF(" ");
      }
    }

    for (int64_t i = 0; i < sub_select_items.count() && OB_SUCC(ret); ++i) {
      if (T_CTE_CYCLE_COLUMN == sub_select_items.at(i).expr_->get_expr_type()) {
        ObRawExpr* v;
        ObRawExpr* d_v;
        DATA_PRINTF("set ");
        DATA_PRINTF("%.*s ", LEN_AND_PTR(sub_select_items.at(i).alias_name_));
        DATA_PRINTF("to ");
        ((ObPseudoColumnRawExpr*)(sub_select_items.at(i).expr_))->get_cte_cycle_value(v, d_v);
        if (OB_ISNULL(v) || OB_ISNULL(d_v) || !v->is_const_expr() || !d_v->is_const_expr()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected const expr", K(ret), KPC(v), KPC(d_v));
        } else {
          ObObj non_cycle_vale = ((ObConstRawExpr*)v)->get_value();
          ObObj cycle_vale = ((ObConstRawExpr*)d_v)->get_value();
          DATA_PRINTF("'%.*s' ", LEN_AND_PTR(non_cycle_vale.get_string()));
          DATA_PRINTF("default ");
          DATA_PRINTF("'%.*s' ", LEN_AND_PTR(cycle_vale.get_string()));
        }
      }
    }
  }
  return ret;
}

}  // end of namespace sql
}  // end of namespace oceanbase
