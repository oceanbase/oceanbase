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
namespace oceanbase {
using namespace common;
namespace sql {

ObDMLStmtPrinter::ObDMLStmtPrinter()
    : buf_(NULL), buf_len_(0), pos_(NULL), stmt_(NULL), print_params_(), expr_printer_(), is_inited_(false)
{}

ObDMLStmtPrinter::ObDMLStmtPrinter(
    char* buf, int64_t buf_len, int64_t* pos, const ObDMLStmt* stmt, ObObjPrintParams print_params)
    : buf_(buf),
      buf_len_(buf_len),
      pos_(pos),
      stmt_(stmt),
      print_params_(print_params),
      expr_printer_(),
      is_inited_(true)
{}

ObDMLStmtPrinter::~ObDMLStmtPrinter()
{}

void ObDMLStmtPrinter::init(char* buf, int64_t buf_len, int64_t* pos, ObDMLStmt* stmt)
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

  if (OB_ISNULL(stmt_) || OB_ISNULL(buf_) || OB_ISNULL(pos_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_ is NULL or buf_ is NULL or pos_ is NULL", K(ret));
  } else {
    const char* hint_begin = "/*+";
    const char* hint_end = " */";
    DATA_PRINTF("%s", hint_begin);
    if (OB_SUCC(ret)) {
      // mark hint info is empty or not, so that we can rollback buffer at last
      const int64 start_pos = *pos_;

      const ObStmtHint& hint = stmt_->get_stmt_hint();
      // no_rewrite
      if (hint.no_rewrite_) {
        DATA_PRINTF(" NO_REWRITE");
      }
      // use_late_mat
      if (OB_SUCC(ret)) {
        if (OB_USE_LATE_MATERIALIZATION == hint.use_late_mat_) {
          DATA_PRINTF(" USE_LATE_MATERIALIZATION");
        } else if (OB_NO_USE_LATE_MATERIALIZATION == hint.use_late_mat_) {
          DATA_PRINTF(" NO_USE_LATE_MATERIALIZATION");
        } else { /*do nothing*/
        }
      }
      // use_jit
      if (OB_SUCC(ret)) {
        if (OB_USE_JIT_AUTO == hint.use_jit_policy_) {
          DATA_PRINTF(" USE_JIT_AUTO");
        } else if (OB_USE_JIT_FORCE == hint.use_jit_policy_) {
          DATA_PRINTF(" USE_JIT_FORCE");
        } else if (OB_NO_USE_JIT == hint.use_jit_policy_) {
          DATA_PRINTF(" NO_USE_JIT");
        } else { /*do nothing*/
        }
      }
      // read_consistency
      if (OB_SUCC(ret)) {
        if (INVALID_CONSISTENCY != hint.read_consistency_) {
          DATA_PRINTF(" READ_CONSISTENCY(%s)", get_consistency_level_str(hint.read_consistency_));
        }
      }
      // query_timeout
      if (OB_SUCC(ret)) {
        if (-1 != hint.query_timeout_) {
          DATA_PRINTF(" QUERY_TIMEOUT(%ld)", hint.query_timeout_);
        }
      }
      // enable_early_lock_release
      if (OB_SUCC(ret)) {
        if (hint.enable_lock_early_release_) {
          DATA_PRINTF(" TRANS_PARAM('ENABLE_EARLY_LOCK_RELEASE' 'TRUE') ");
        }
      }
      // force refresh location cache
      if (OB_SUCC(ret)) {
        if (hint.force_refresh_lc_) {
          DATA_PRINTF("FORCE_REFRESH_LOCATION_CACHE");
        }
      }
      // use_plan_cache
      if (OB_SUCC(ret)) {
        if (OB_USE_PLAN_CACHE_INVALID != hint.plan_cache_policy_) {
          DATA_PRINTF(" USE_PLAN_CACHE");
          if (OB_SUCC(ret)) {
            if (OB_USE_PLAN_CACHE_NONE == hint.plan_cache_policy_) {
              DATA_PRINTF("(none)");
            } else if (OB_USE_PLAN_CACHE_DEFAULT == hint.plan_cache_policy_) {
              DATA_PRINTF("(default)");
            } else {
            }
          }
        }
      }
      // ordered
      if (OB_SUCC(ret)) {
        if (hint.join_ordered_) {
          DATA_PRINTF(" ORDERED");
        }
      }

      // PX
      if (OB_SUCC(ret) && hint.has_px_hint_) {
        if (hint.enable_use_px()) {
          DATA_PRINTF(" USE_PX");
        } else if (hint.disable_use_px()) {
          DATA_PRINTF(" NO_USE_PX");
        }
      }

      // PARALLEL
      if (OB_SUCC(ret) && hint.parallel_ > 0) {
        DATA_PRINTF(" PARALLEL(%ld)", hint.parallel_);
      }

      // ENABLE_PARALLEL_DML
      if (OB_SUCC(ret) && hint.pdml_option_ == ObPDMLOption::ENABLE) {
        DATA_PRINTF(" ENABLE_PARALLEL_DML");
      }

      // top stmt, print hint with qb_name
      if (OB_SUCC(ret)) {
        if (stmt_->is_root_stmt()) {
          const ObQueryCtx* query_ctx = NULL;
          if (OB_ISNULL(query_ctx = stmt_->get_query_ctx())) {
            ret = OB_NOT_INIT;
            LOG_WARN("Stmt's query ctx should not be NULL", K(ret));
          } else if (OB_FAIL(print_index_hints_with_qb(query_ctx->org_indexes_))) {
            LOG_WARN("Failed to print index hints with qb", K(ret));
          } else if (OB_FAIL(print_tables_hints(N_LEADING_HINT, query_ctx->join_order_))) {
            LOG_WARN("Failed to print leading table hints", K(ret));
          } else if (OB_FAIL(print_tables_hints(N_USE_MERGE_HINT, query_ctx->use_merge_))) {
            LOG_WARN("Failed to print use merge table hints", K(ret));
          } else if (OB_FAIL(print_tables_hints(N_NO_USE_MERGE_HINT, query_ctx->no_use_merge_))) {
            LOG_WARN("Failed to print no use merge table hints", K(ret));
          } else if (OB_FAIL(print_tables_hints(N_USE_HASH_HINT, query_ctx->use_hash_))) {
            LOG_WARN("Failed to print use hash table hints", K(ret));
          } else if (OB_FAIL(print_tables_hints(N_NO_USE_HASH_HINT, query_ctx->no_use_hash_))) {
            LOG_WARN("Failed to print no use hash table hints", K(ret));
          } else if (OB_FAIL(print_tables_hints(N_USE_NL_HINT, query_ctx->use_nl_))) {
            LOG_WARN("Failed to print use nl table hints", K(ret));
          } else if (OB_FAIL(print_tables_hints(N_NO_USE_NL_HINT, query_ctx->no_use_nl_))) {
            LOG_WARN("Failed to print no use nl table hints", K(ret));
          } else if (OB_FAIL(print_tables_hints(N_USE_BNL_HINT, query_ctx->use_bnl_))) {
            LOG_WARN("Failed to print use bnl table hints", K(ret));
          } else if (OB_FAIL(print_tables_hints(N_NO_USE_BNL_HINT, query_ctx->no_use_bnl_))) {
            LOG_WARN("Failed to print no use bnl table hints", K(ret));
          } else if (OB_FAIL(print_tables_hints(N_USE_NL_MATERIALIZATION_HINT, query_ctx->use_nl_materialization_))) {
            LOG_WARN("Failed to print use material nl table hints", K(ret));
          } else if (OB_FAIL(
                         print_tables_hints(N_NO_USE_NL_MATERIALIZATION_HINT, query_ctx->no_use_nl_materialization_))) {
            LOG_WARN("Failed to print no use material nl table hints", K(ret));
          } else if (OB_FAIL(print_tables_hints(N_NO_PX_JOIN_FILTER, query_ctx->no_px_join_filter_))) {
            LOG_WARN("Failed to print no px join filter table hints", K(ret));
          } else if (OB_FAIL(print_tables_hints(N_PX_JOIN_FILTER, query_ctx->px_join_filter_))) {
            LOG_WARN("Failed to print px join filter table hints", K(ret));
          } else {
          }  // do nothing
        }
      }

      // print hint without qb_name
      if (OB_SUCC(ret)) {
        if (OB_FAIL(print_index_hints(hint.org_indexes_))) {
          LOG_WARN("Failed to print index hint", K(ret));
        } else if (OB_FAIL(print_tables_hint(N_LEADING_HINT, hint.join_order_, &hint.join_order_pairs_))) {
          LOG_WARN("Failed to print leading table hints", K(ret));
        } else if (OB_FAIL(print_tables_hint(N_USE_MERGE_HINT, hint.use_merge_, &hint.use_merge_order_pairs_))) {
          LOG_WARN("Failed to print use merges table hints", K(ret));
        } else if (OB_FAIL(
                       print_tables_hint(N_NO_USE_MERGE_HINT, hint.no_use_merge_, &hint.no_use_merge_order_pairs_))) {
          LOG_WARN("Failed to print no use merges table hints", K(ret));
        } else if (OB_FAIL(print_tables_hint(N_USE_HASH_HINT, hint.use_hash_, &hint.use_hash_order_pairs_))) {
          LOG_WARN("Failed to print use hashs table hints", K(ret));
        } else if (OB_FAIL(print_tables_hint(N_NO_USE_HASH_HINT, hint.no_use_hash_, &hint.no_use_hash_order_pairs_))) {
          LOG_WARN("Failed to print no use hashs table hints", K(ret));
        } else if (OB_FAIL(print_tables_hint(N_USE_NL_HINT, hint.use_nl_, &hint.use_nl_order_pairs_))) {
          LOG_WARN("Failed to print use nls table hints", K(ret));
        } else if (OB_FAIL(print_tables_hint(N_NO_USE_NL_HINT, hint.no_use_nl_, &hint.no_use_nl_order_pairs_))) {
          LOG_WARN("Failed to print use no nls table hints", K(ret));
        } else if (OB_FAIL(print_tables_hint(N_USE_BNL_HINT, hint.use_bnl_, &hint.use_bnl_order_pairs_))) {
          LOG_WARN("Failed to print use bnls table hints", K(ret));
        } else if (OB_FAIL(print_tables_hint(N_NO_USE_BNL_HINT, hint.no_use_bnl_, &hint.no_use_bnl_order_pairs_))) {
          LOG_WARN("Failed to print use no bnls table hints", K(ret));
        } else if (OB_FAIL(print_tables_hint(N_USE_NL_MATERIALIZATION_HINT,
                       hint.use_nl_materialization_,
                       &hint.use_nl_materialization_order_pairs_))) {
          LOG_WARN("Failed to print use material nl table hints", K(ret));
        } else if (OB_FAIL(print_tables_hint(N_NO_USE_NL_MATERIALIZATION_HINT,
                       hint.no_use_nl_materialization_,
                       &hint.no_use_nl_materialization_order_pairs_))) {
          LOG_WARN("Failed to print no use material nl table hints", K(ret));
        } else if (OB_FAIL(print_tables_hint(
                       N_NO_PX_JOIN_FILTER, hint.no_px_join_filter_, &hint.no_px_join_filter_order_pairs_))) {
          LOG_WARN("Failed to print no px join filter table hints", K(ret));
        } else if (OB_FAIL(
                       print_tables_hint(N_PX_JOIN_FILTER, hint.px_join_filter_, &hint.px_join_filter_order_pairs_))) {
          LOG_WARN("Failed to print px join filter table hints", K(ret));
        } else if (OB_FAIL(print_rewrite_hints(
                       ObStmtHint::NO_EXPAND_HINT, ObUseRewriteHint::NO_EXPAND, hint.use_expand_))) {
          LOG_WARN("Failed to print no expand hint", K(ret));
        } else if (OB_FAIL(print_rewrite_hints(
                       ObStmtHint::USE_CONCAT_HINT, ObUseRewriteHint::USE_CONCAT, hint.use_expand_))) {
          LOG_WARN("Failed to print use concat hint", K(ret));
        } else if (OB_FAIL(print_rewrite_hints(
                       ObStmtHint::VIEW_MERGE_HINT, ObUseRewriteHint::V_MERGE, hint.use_view_merge_))) {
          LOG_WARN("Failed to print view merge hints", K(ret));
        } else if (OB_FAIL(print_rewrite_hints(
                       ObStmtHint::NO_VIEW_MERGE_HINT, ObUseRewriteHint::NO_V_MERGE, hint.use_view_merge_))) {
          LOG_WARN("Failed to print no view merge hints", K(ret));
        } else if (OB_FAIL(print_rewrite_hints(ObStmtHint::UNNEST_HINT, ObUseRewriteHint::UNNEST, hint.use_unnest_))) {
          LOG_WARN("Failed to print unnest hints", K(ret));
        } else if (OB_FAIL(print_rewrite_hints(
                       ObStmtHint::NO_UNNEST_HINT, ObUseRewriteHint::NO_UNNEST, hint.use_unnest_))) {
          LOG_WARN("Failed to print no unnest hints", K(ret));
        } else if (OB_FAIL(print_rewrite_hints(
                       ObStmtHint::PLACE_GROUP_BY_HINT, ObUseRewriteHint::PLACE_GROUPBY, hint.use_place_groupby_))) {
          LOG_WARN("Failed to print place group by hints", K(ret));
        } else if (OB_FAIL(print_rewrite_hints(ObStmtHint::NO_PLACE_GROUP_BY_HINT,
                       ObUseRewriteHint::NO_PLACE_GROUPBY,
                       hint.use_place_groupby_))) {
          LOG_WARN("Failed to print no place group by hints", K(ret));
        } else if (OB_FAIL(print_rewrite_hints(
                       ObStmtHint::NO_PRED_DEDUCE_HINT, ObUseRewriteHint::NO_PRED_DEDUCE, hint.use_pred_deduce_))) {
          LOG_WARN("Faield to print no predicate deduce hints", K(ret));
        } else {
        }
      }

      if (OB_SUCC(ret)) {
        if (start_pos == *pos_) {
          // no hint, roolback buffer!
          *pos_ -= strlen(hint_begin);
        } else {
          DATA_PRINTF("%s", hint_end);
        }
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
        const FromItem& from_item = stmt_->get_from_item(i);
        const TableItem* table_item = NULL;
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
    } else if (share::is_oracle_mode() && 0 == from_item_size) {
      // select 1 from dual
      // in oracle mode
      DATA_PRINTF(" from DUAL");
    }
  }

  return ret;
}

int ObDMLStmtPrinter::print_table(
    const TableItem* table_item, bool expand_cte_table, bool no_print_alias /*default false*/)
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
        } else if (!no_print_alias) {
          DATA_PRINTF(share::is_oracle_mode() ? " \"%.*s\"" : " `%.*s`", LEN_AND_PTR(table_item->alias_name_));
        }
        break;
      }
      case TableItem::JOINED_TABLE: {
        const JoinedTable* join_table = static_cast<const JoinedTable*>(table_item);
        if (OB_ISNULL(join_table)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("join_table should not be NULL", K(ret));
        } else {
          // left table
          const TableItem* left_table = join_table->left_table_;
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
                  if (join_table->join_conditions_.count() == 0) {  // cross join
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
              const TableItem* right_table = join_table->right_table_;
              if (OB_ISNULL(right_table)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("right_table should not be NULL", K(ret));
              } else if (OB_FAIL(SMART_CALL(print_table(right_table)))) {
                LOG_WARN("fail to print right table", K(ret), K(*right_table));
              } else {
                // join conditions
                const ObIArray<ObRawExpr*>& join_conditions = join_table->join_conditions_;
                int64_t join_conditions_size = join_conditions.count();
                if (join_conditions_size > 0) {
                  DATA_PRINTF(" on (");
                  for (int64_t i = 0; OB_SUCC(ret) && i < join_conditions_size; ++i) {
                    if (OB_FAIL(expr_printer_.do_print(join_conditions.at(i), T_NONE_SCOPE))) {
                      LOG_WARN("fail to print join condition", K(ret));
                    }
                    DATA_PRINTF(" and ");
                  }
                  if (OB_SUCC(ret)) {
                    *pos_ -= 5;  // strlen(" and ")
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
          bool print_bracket = !expand_cte_table || table_item->ref_query_->is_root_stmt();
          if (print_bracket) {
            DATA_PRINTF("(");
          }
          int64_t old_pos = *pos_;
          if (OB_SUCC(ret)) {
            ObIArray<ObString>* column_list = NULL;
            bool is_set_subquery = false;
            bool is_generated_table = true;
            const bool force_col_alias =
                stmt_->is_select_stmt() && (static_cast<const ObSelectStmt*>(stmt_)->is_star_select());
            ObSelectStmtPrinter stmt_printer(buf_,
                buf_len_,
                pos_,
                static_cast<ObSelectStmt*>(table_item->ref_query_),
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
              DATA_PRINTF(" %.*s", LEN_AND_PTR(table_item->table_name_));
            }
          }
        }
        break;
      }
      case TableItem::CTE_TABLE: {
        PRINT_TABLE_NAME(table_item);
        if (!table_item->alias_name_.empty()) {
          DATA_PRINTF(share::is_oracle_mode() ? " \"%.*s\"" : " `%.*s`", LEN_AND_PTR(table_item->alias_name_));
        }
        break;
      }
      case TableItem::FUNCTION_TABLE: {
        CK(share::is_oracle_mode());
        DATA_PRINTF("TABLE(");
        OZ(expr_printer_.do_print(table_item->function_table_expr_, T_FROM_SCOPE));
        DATA_PRINTF(")");
        DATA_PRINTF(" \"%.*s\"", LEN_AND_PTR(table_item->alias_name_));
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

int ObDMLStmtPrinter::print_base_table(const TableItem* table_item)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(stmt_) || OB_ISNULL(buf_) || OB_ISNULL(pos_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_ is NULL or buf_ is NULL or pos_ is NULL", K(ret));
  } else if (OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table_item should not be NULL", K(ret));
  } else if (TableItem::BASE_TABLE != table_item->type_ && TableItem::ALIAS_TABLE != table_item->type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table_type should be BASE_TABLE or ALIAS_TABLE", K(ret), K(table_item->type_));
  } else {
    PRINT_TABLE_NAME(table_item);
    if (OB_SUCC(ret)) {
      const ObStmtHint& stmt_hint = stmt_->get_stmt_hint();
      // partition
      const ObPartHint* part_hint = stmt_hint.get_part_hint(table_item->table_id_);
      if (NULL != part_hint) {
        int64_t part_cnt = part_hint->part_names_.count();
        if (part_cnt <= 0) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("part count should be greater than 0", K(ret), K(part_cnt));
        } else {
          DATA_PRINTF(" partition(");
          for (int64_t i = 0; OB_SUCC(ret) && i < part_cnt; ++i) {
            DATA_PRINTF("%.*s,", LEN_AND_PTR(part_hint->part_names_.at(i)));
          }
          if (OB_SUCC(ret)) {
            --*pos_;
            DATA_PRINTF(")");
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
    const ObIArray<ObRawExpr*>& condition_exprs = stmt_->get_condition_exprs();
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
        *pos_ -= 5;  // strlen(" and ")
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
        const OrderItem& order_item = stmt_->get_order_item(i);
        if (OB_FAIL(expr_printer_.do_print(order_item.expr_, T_ORDER_SCOPE))) {
          LOG_WARN("fail to print order by expr", K(ret));
        } else if (share::is_mysql_mode()) {
          if (is_descending_direction(order_item.order_type_)) {
            DATA_PRINTF("desc");
          }
        } else if (order_item.order_type_ == NULLS_FIRST_ASC) {
          DATA_PRINTF("asc nulls first");
        } else if (order_item.order_type_ == NULLS_LAST_ASC) {  // use default value
          /*do nothing*/
        } else if (order_item.order_type_ == NULLS_FIRST_DESC) {  // use default value
          DATA_PRINTF("desc");
        } else if (order_item.order_type_ == NULLS_LAST_DESC) {
          DATA_PRINTF("desc nulls last");
        } else { /*do nothing*/
        }
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
    // fetch percent
    if (OB_SUCC(ret)) {
      if (NULL != stmt_->get_limit_percent_expr()) {
        DATA_PRINTF(" fetch next ");
        if (OB_FAIL(expr_printer_.do_print(stmt_->get_limit_percent_expr(), T_NONE_SCOPE))) {
          LOG_WARN("fail to print order limit expr", K(ret));
        }
        DATA_PRINTF(" percent rows");
      }
    }
    // fetch only/with ties
    if (OB_SUCC(ret)) {
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
  CK(OB_NOT_NULL(stmt_), OB_NOT_NULL(buf_), OB_NOT_NULL(pos_));
  CK(stmt_->is_insert_stmt() || stmt_->is_update_stmt() || stmt_->is_delete_stmt());
  if (OB_SUCC(ret)) {
    const ObDelUpdStmt& dml_stmt = static_cast<const ObDelUpdStmt&>(*stmt_);
    const ObIArray<ObRawExpr*>& returning_exprs = dml_stmt.get_returning_exprs();
    if (returning_exprs.count() > 0) {
      DATA_PRINTF(" returning ");
      OZ(expr_printer_.do_print(returning_exprs.at(0), T_NONE_SCOPE));
      for (uint64_t i = 1; OB_SUCC(ret) && i < returning_exprs.count(); ++i) {
        DATA_PRINTF(",");
        OZ(expr_printer_.do_print(returning_exprs.at(i), T_NONE_SCOPE));
      }
    }
  }
  return ret;
}

int ObDMLStmtPrinter::print_index_hints(const ObIArray<ObOrgIndexHint>& index_hints)
{
  int ret = OB_SUCCESS;
  if (index_hints.count() > 0) {
    for (int64_t idx = 0; OB_SUCC(ret) && idx < index_hints.count(); ++idx) {
      if (OB_FAIL(print_index_hint(index_hints.at(idx)))) {
        LOG_WARN("Failed to print org index hint", K(ret));
      }
    }
  }
  return ret;
}

int ObDMLStmtPrinter::print_index_hints_with_qb(const ObIArray<ObQNameIndexHint>& index_hints)
{
  int ret = OB_SUCCESS;
  if (index_hints.count() > 0) {
    for (int64_t idx = 0; OB_SUCC(ret) && idx < index_hints.count(); ++idx) {
      if (OB_FAIL(print_index_hint(index_hints.at(idx).index_hint_, index_hints.at(idx).qb_name_))) {
        LOG_WARN("Failed to print org index hint with qb", K(ret));
      }
    }
  }
  return ret;
}

int ObDMLStmtPrinter::print_tables_hints(const char* hint_name, const ObIArray<ObTablesInHint>& tables_hints)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(hint_name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("hint name should not be NULL", K(ret));
  } else if (tables_hints.count() > 0) {
    if (0 == STRCMP(hint_name, N_LEADING_HINT)) {
      for (int64_t idx = 0; OB_SUCC(ret) && idx < tables_hints.count(); ++idx) {
        if (OB_FAIL(print_tables_hint(hint_name,
                tables_hints.at(idx).tables_,
                &tables_hints.at(idx).join_order_pairs_,
                tables_hints.at(idx).qb_name_))) {
          LOG_WARN("Failed to print tables hint", K(ret));
        } else { /* do nothing */
        }
      }
    } else {
      for (int64_t idx = 0; OB_SUCC(ret) && idx < tables_hints.count(); ++idx) {
        if (OB_FAIL(print_tables_hint(hint_name, tables_hints.at(idx).tables_, NULL, tables_hints.at(idx).qb_name_))) {
          LOG_WARN("Failed to print tables hint", K(ret));
        } else { /* do nothing */
        }
      }
    }
  } else {
  }  // do nothing
  return ret;
}

int ObDMLStmtPrinter::print_rewrite_hints(
    const char* hint_name, const ObUseRewriteHint::Type hint_type, const ObUseRewriteHint::Type hint_value)
{
  int ret = OB_SUCCESS;
  if (hint_type == hint_value) {
    DATA_PRINTF(" %s", hint_name);
  }
  return ret;
}

int ObDMLStmtPrinter::print_parens_for_leading_hint(
    int64_t cur_idx, const ObIArray<std::pair<uint8_t, uint8_t>>* join_order_pairs, bool is_left_parent)
{
  int ret = OB_SUCCESS;
  int n_parens = 0;
  char paren_char = ' ';
  if (!OB_ISNULL(join_order_pairs)) {
    if (false == is_left_parent) {
      paren_char = ')';
      for (int64_t idx = 0; idx < join_order_pairs->count(); ++idx) {
        if (join_order_pairs->at(idx).second == cur_idx) {
          ++n_parens;
        } else { /* do nothing */
        }
      }
    } else {
      paren_char = '(';
      for (int64_t idx = 0; idx < join_order_pairs->count(); ++idx) {
        if (join_order_pairs->at(idx).first == cur_idx) {
          ++n_parens;
        } else if (join_order_pairs->at(idx).first > cur_idx) {
          break;
        } else { /* do nothing */
        }
      }
    }
    for (int i = 0; OB_SUCC(ret) && i < n_parens; ++i) {
      DATA_PRINTF("%c", paren_char);
    }
  } else { /* do nothing */
  }
  return ret;
}

int ObDMLStmtPrinter::print_tables_hint(const char* hint_name, const ObIArray<ObTableInHint>& table_hints,
    const ObIArray<std::pair<uint8_t, uint8_t>>* join_order_pairs, const ObString& qb_name)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(hint_name) || OB_ISNULL(pos_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Hint name should not be NULL", K(ret));
  } else if (table_hints.count() > 0) {
    if (qb_name.empty()) {
      DATA_PRINTF(" %s(", hint_name);
    } else {
      DATA_PRINTF(" %s(@%.*s ", hint_name, LEN_AND_PTR(qb_name));
    }
    if (OB_SUCC(ret)) {
      for (int64_t idx = 0; OB_SUCC(ret) && idx < table_hints.count(); ++idx) {
        if (OB_FAIL(print_parens_for_leading_hint(idx, join_order_pairs, true))) {
          LOG_WARN("Failed to print table in hint", K(ret));
        } else if (OB_FAIL(print_table_in_hint(table_hints.at(idx)))) {
          LOG_WARN("Failed to print table in hint", K(ret));
        } else if (OB_FAIL(print_parens_for_leading_hint(idx, join_order_pairs, false))) {
          LOG_WARN("Failed to print table in hint", K(ret));
        }
        if (OB_SUCC(ret)) {
          DATA_PRINTF(" ");
        }
      }
    }
    if (OB_SUCC(ret)) {
      --*pos_;
      DATA_PRINTF(")");
    }
  } else {
  }  // do nothing
  return ret;
}

int ObDMLStmtPrinter::print_table_in_hint(const ObTableInHint& hint_table)
{
  int ret = OB_SUCCESS;
  if (!hint_table.db_name_.empty()) {
    DATA_PRINTF("%.*s.", LEN_AND_PTR(hint_table.db_name_));
  }
  DATA_PRINTF("%.*s", LEN_AND_PTR(hint_table.table_name_));
  if (!hint_table.qb_name_.empty()) {
    DATA_PRINTF("@%.*s", LEN_AND_PTR(hint_table.qb_name_));
  }
  return ret;
}

int ObDMLStmtPrinter::print_index_hint(const ObOrgIndexHint& index_hint, const ObString& qb_name)
{
  int ret = OB_SUCCESS;
  bool primary_index = (0 == index_hint.index_name_.case_compare(N_PRIMARY));
  if (qb_name.empty()) {
    if (primary_index) {
      DATA_PRINTF(" FULL(");
    } else {
      DATA_PRINTF(" INDEX(");
    }
  } else {
    if (primary_index) {
      DATA_PRINTF(" FULL(@%.*s ", LEN_AND_PTR(qb_name));
    } else {
      DATA_PRINTF(" INDEX(@%.*s ", LEN_AND_PTR(qb_name));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(print_table_in_hint(index_hint.table_))) {
      LOG_WARN("Failed to print table in hint", K(ret));
    } else if (!primary_index) {
      DATA_PRINTF(" %.*s)", LEN_AND_PTR(index_hint.index_name_));
    } else {
      DATA_PRINTF(")");
    }
  }
  return ret;
}

}  // end of namespace sql
}  // end of namespace oceanbase
