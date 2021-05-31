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

#define USING_LOG_PREFIX SQL_REWRITE

#include "sql/rewrite/ob_transform_simplify.h"
#include "sql/rewrite/ob_transformer_impl.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/oblog/ob_log_module.h"
#include "common/ob_common_utility.h"
#include "share/ob_unit_getter.h"
#include "share/schema/ob_column_schema.h"
#include "sql/ob_sql_context.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "sql/code_generator/ob_expr_generator_impl.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/session/ob_sql_session_info.h"
#include "share/config/ob_server_config.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "common/ob_smart_call.h"
#include "sql/resolver/dml/ob_group_by_checker.h"
#include "sql/rewrite/ob_stmt_comparer.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
namespace oceanbase {
namespace sql {
int ObTransformSimplify::transform_one_stmt(
    common::ObIArray<ObParentDMLStmt>& parent_stmts, ObDMLStmt*& stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  UNUSED(parent_stmts);
  trans_happened = false;
  bool is_happened = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt is NULL", K(ret));
  } else {
    if (OB_SUCC(ret)) {
      if (OB_FAIL(remove_stmt_order_by(stmt, is_happened))) {
        LOG_WARN("remove stmt order by failed", K(ret));
      } else {
        trans_happened |= is_happened;
        LOG_TRACE("succeed to remove stmt order by", K(is_happened));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(remove_stmt_win(stmt, is_happened))) {
        LOG_WARN("remove stmt group by failed", K(ret));
      } else {
        trans_happened |= is_happened;
        LOG_TRACE("succeed to stmt remove group by", K(is_happened));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(remove_order_by_duplicates(stmt, is_happened))) {
        LOG_WARN("failed to do simplification of order by items", K(ret));
      } else {
        trans_happened |= is_happened;
        LOG_TRACE("succeed to remove order by duplicates", K(is_happened));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(remove_stmt_group_by(stmt, is_happened))) {
        LOG_WARN("remove stmt group by failed", K(ret));
      } else {
        trans_happened |= is_happened;
        LOG_TRACE("succeed to stmt remove group by", K(is_happened));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(remove_group_by_duplicates(stmt, is_happened))) {
        LOG_WARN("failed to remove group by duplicates", K(ret));
      } else {
        trans_happened |= is_happened;
        LOG_TRACE("succeed to remove group by duplicates", K(is_happened));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(remove_distinct_before_const(stmt, is_happened))) {
        LOG_WARN("failed to remove distinct before const", K(ret));
      } else {
        trans_happened |= is_happened;
        LOG_TRACE("succeed to remove const distinct", K(is_happened));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(remove_stmt_distinct(stmt, is_happened))) {
        LOG_WARN("failed to remove stmt distinct", K(ret));
      } else {
        trans_happened |= is_happened;
        LOG_TRACE("succeed to remove stmt distinct", K(is_happened));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(replace_is_null_condition(stmt, is_happened))) {
        LOG_WARN("failed to replace is null condition", K(is_happened));
      } else {
        trans_happened |= is_happened;
        LOG_TRACE("succeed to replace is null condition", K(is_happened));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(push_down_outer_join_condition(stmt, is_happened))) {
        LOG_WARN("failed to push down outer join condition", K(is_happened));
      } else {
        trans_happened |= is_happened;
        LOG_TRACE("succeed to push down outer join condition", K(is_happened));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(replace_op_null_condition(stmt, is_happened))) {
        LOG_WARN("failed to replace op null condition", K(is_happened));
      } else {
        trans_happened |= is_happened;
        LOG_TRACE("succeed to replace op null condition", K(is_happened));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(convert_preds_vector_to_scalar(stmt, is_happened))) {
        LOG_WARN("failed to convert vector predicate to scalar", K(ret));
      } else {
        trans_happened |= is_happened;
        LOG_TRACE("succeed to convert vector predicate to scalar", K(is_happened));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(transform_subquery_as_expr(stmt, is_happened))) {
        LOG_WARN("failed to transform subquery to expr", K(ret));
      } else {
        trans_happened |= is_happened;
        LOG_TRACE("succeed to transform subquery to expr", K(is_happened));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(remove_dummy_exprs(stmt, is_happened))) {
        LOG_WARN("failed to remove dummy exprs", K(ret));
      } else {
        trans_happened |= is_happened;
        LOG_TRACE("succeed to remove dummy exprs", K(is_happened), K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(simplify_win_exprs(stmt, is_happened))) {
        LOG_WARN("failed to simplify win expr", K(ret));
      } else {
        trans_happened |= is_happened;
        LOG_TRACE("succeed to simplify win expr", K(is_happened));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(remove_redundent_select(stmt, is_happened))) {
        LOG_WARN("failed to remove simple select", K(ret));
      } else {
        trans_happened |= is_happened;
        LOG_TRACE("succeed to remove simple select", K(is_happened));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(remove_aggr_distinct(stmt, is_happened))) {
        LOG_WARN("failed to remove aggr distinct", K(ret));
      } else {
        trans_happened |= is_happened;
        LOG_TRACE("succeed to remove aggr distinct", K(is_happened));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(pushdown_limit_offset(stmt, is_happened))) {
        LOG_WARN("failed to pushdown limit offset", K(ret));
      } else {
        trans_happened |= is_happened;
        LOG_TRACE("succeed to pushdown limit offset", K(is_happened));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(remove_redundent_group_by(stmt, is_happened))) {
        LOG_WARN("failed to remove redundent group by", K(ret));
      } else {
        trans_happened |= is_happened;
        LOG_TRACE("succeed to remove redundent group by", K(is_happened));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(stmt->formalize_stmt(ctx_->session_info_))) {
        LOG_WARN("failed to formalize stmt", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformSimplify::push_down_outer_join_condition(ObDMLStmt* stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is NULL", K(ret), K(stmt));
  } else if (OB_FAIL(ObTransformUtils::right_join_to_left(stmt))) {
    LOG_WARN("failed to change right join to left", K(ret));
  } else {
    ObIArray<JoinedTable*>& join_tables = stmt->get_joined_tables();
    for (int64_t i = 0; OB_SUCC(ret) && i < join_tables.count(); ++i) {
      bool is_happened = false;
      if (OB_FAIL(push_down_outer_join_condition(stmt, join_tables.at(i), is_happened))) {
        LOG_WARN("failed to push down outer join condition", K(ret));
      } else {
        trans_happened |= is_happened;
      }
    }
  }
  return ret;
}

int ObTransformSimplify::push_down_outer_join_condition(ObDMLStmt* stmt, TableItem* join_table, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  bool left_happend = false;
  bool right_happend = false;
  JoinedTable* cur_joined = NULL;
  if (OB_ISNULL(stmt) || OB_ISNULL(join_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL", K(ret));
  } else if (!join_table->is_joined_table()) {
    /*do nothing*/
  } else if (FALSE_IT(cur_joined = static_cast<JoinedTable*>(join_table))) {
    /*do nothing*/
  } else if (OB_FAIL(SMART_CALL(push_down_outer_join_condition(stmt, cur_joined->left_table_, left_happend)))) {
    LOG_WARN("failed to push down outer join condition", K(ret));
  } else if (OB_FAIL(SMART_CALL(push_down_outer_join_condition(stmt, cur_joined->right_table_, right_happend)))) {
    LOG_WARN("failed to push down outer join condition", K(ret));
  } else if (OB_FAIL(try_push_down_outer_join_conds(stmt, cur_joined, trans_happened))) {
    LOG_WARN("fail to get outer join push down conditions", K(ret));
  } else {
    trans_happened |= left_happend || right_happend;
  }
  return ret;
}

int ObTransformSimplify::try_push_down_outer_join_conds(ObDMLStmt* stmt, JoinedTable* join_table, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  ObSqlBitSet<> right_table_ids;
  if (OB_ISNULL(stmt) || OB_ISNULL(join_table) || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL", K(ret));
  } else if (!join_table->is_left_join()) {
    /*do nothing*/
  } else if (OB_ISNULL(join_table->right_table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL", K(ret));
  } else if (!join_table->right_table_->is_basic_table() && !join_table->right_table_->is_generated_table() &&
             !join_table->right_table_->is_joined_table()) {
    /*do nothing*/
  } else if (OB_FAIL(ObTransformUtils::get_table_rel_ids(*stmt, *join_table->right_table_, right_table_ids))) {
    LOG_WARN("failed to get target table rel ids", K(ret));
  } else {
    ObSEArray<ObRawExpr*, 16> push_down_conds;
    ObIArray<ObRawExpr*>& join_conds = join_table->get_join_conditions();
    for (int64_t i = 0; OB_SUCC(ret) && i < join_conds.count(); ++i) {
      if (OB_ISNULL(join_conds.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected NULL", K(ret));
      } else if (join_conds.at(i)->get_relation_ids().is_subset(right_table_ids) &&
                 join_conds.at(i)->has_flag(CNT_SUB_QUERY)) {
        if (OB_FAIL(push_down_conds.push_back(join_conds.at(i)))) {
          LOG_WARN("failed to push back expr", K(ret));
        }
      }
    }

    TableItem* view_item = NULL;
    if (OB_FAIL(ret) || push_down_conds.empty()) {
      /*do nothing*/
    } else if (OB_FAIL(ObTransformUtils::create_view_with_table(stmt, ctx_, join_table->right_table_, view_item))) {
      LOG_WARN("failed to create view with table", K(ret));
    } else if (OB_FAIL(push_down_on_condition(stmt, join_table, push_down_conds))) {
      LOG_WARN("failed to push down on condition", K(ret));
    } else {
      trans_happened = true;
    }
  }
  return ret;
}

int ObTransformSimplify::push_down_on_condition(ObDMLStmt* stmt, JoinedTable* join_table, ObIArray<ObRawExpr*>& conds)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 16> old_column_exprs;
  ObSEArray<ObRawExpr*, 16> new_column_exprs;
  ObSelectStmt* child_stmt = NULL;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_) || OB_ISNULL(join_table) || OB_ISNULL(join_table->right_table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL", K(ret));
  } else if (!join_table->is_left_join() || !join_table->right_table_->is_generated_table() ||
             OB_ISNULL(child_stmt = join_table->right_table_->ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected join table", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::remove_item(join_table->get_join_conditions(), conds))) {
    LOG_WARN("failed to remove item", K(ret));
  } else if (OB_FAIL(child_stmt->add_condition_exprs(conds))) {
    LOG_WARN("failed to add condotion exprs", K(ret));
  } else if (OB_FAIL(ObTransformUtils::extract_query_ref_expr(conds, child_stmt->get_subquery_exprs()))) {
    LOG_WARN("failed to adjust subquery list", K(ret));
  } else if (OB_FAIL(stmt->adjust_subquery_list())) {
    LOG_WARN("failed to adjust subquery list", K(ret));
  } else if (OB_FAIL(child_stmt->adjust_subquery_stmt_parent(stmt, child_stmt))) {
    LOG_WARN("failed to adjust subquery stmt parent", K(ret));
  } else if (OB_FAIL(stmt->get_column_exprs(join_table->right_table_->table_id_, old_column_exprs))) {
    LOG_WARN("failed to get column exprs", K(ret));
  } else if (OB_FAIL(ObTransformUtils::convert_column_expr_to_select_expr(
                 old_column_exprs, *child_stmt, new_column_exprs))) {
    LOG_WARN("failed to conver column exprs", K(ret));
  } else if (OB_FAIL(child_stmt->replace_inner_stmt_expr(old_column_exprs, new_column_exprs))) {
    LOG_WARN("failed to replace stmt exprs", K(ret));
  } else if (OB_FAIL(child_stmt->formalize_stmt(ctx_->session_info_))) {
    LOG_WARN("failed to formalize stmt", K(ret));
  }
  return ret;
}

//     select sum(sc2) from (select sum(c2) sc2 from t ... group by c1);
// --> select sum(c2) from (select c2 from t ...);
int ObTransformSimplify::remove_redundent_group_by(ObDMLStmt* stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  bool is_valid = false;
  ObArray<ObSelectStmt*> valid_child_stmts;
  ObSelectStmt* select_stmt = NULL;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (!stmt->is_select_stmt() || !stmt->is_single_table_stmt()) {
    /*do nothing*/
  } else if (FALSE_IT(select_stmt = static_cast<ObSelectStmt*>(stmt))) {
    /*do nothing*/
  } else if (OB_ISNULL(select_stmt->get_table_item(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table item is null", K(ret));
  } else if (!select_stmt->get_table_item(0)->is_generated_table()) {
    /*do nothing*/
  } else if (OB_FAIL(check_upper_stmt_validity(select_stmt, is_valid))) {
    LOG_WARN("failed to check upper stmt validity", K(ret));
  } else if (!is_valid) {
    /*do nothing*/
  } else if (OB_FAIL(
                 get_valid_child_stmts(select_stmt, select_stmt->get_table_item(0)->ref_query_, valid_child_stmts))) {
    LOG_WARN("failed to get valid child stmts", K(ret));
  } else if (valid_child_stmts.empty()) {
    /*do nothing*/
  } else if (OB_FAIL(remove_child_stmts_group_by(valid_child_stmts))) {
    LOG_WARN("failed to remove child stmts group by", K(ret));
  } else {
    trans_happened = true;
  }
  return ret;
}

int ObTransformSimplify::check_upper_stmt_validity(ObSelectStmt* upper_stmt, bool& is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  if (!upper_stmt->has_group_by()) {
    is_valid = false;
  }
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < upper_stmt->get_condition_size(); ++i) {
    ObRawExpr* cond_expr = NULL;
    if (OB_ISNULL(cond_expr = upper_stmt->get_condition_expr(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null expr", K(ret));
    } else if (cond_expr->has_flag(CNT_ROWNUM)) {
      is_valid = false;
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < upper_stmt->get_aggr_item_size(); ++i) {
    ObAggFunRawExpr* aggr_expr = NULL;
    if (OB_ISNULL(aggr_expr = upper_stmt->get_aggr_item(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null aggr", K(ret));
    } else if (T_FUN_MIN != aggr_expr->get_expr_type() && T_FUN_MAX != aggr_expr->get_expr_type() &&
               T_FUN_SUM != aggr_expr->get_expr_type()) {
      is_valid = false;
    } else if (T_FUN_SUM == aggr_expr->get_expr_type() && aggr_expr->is_param_distinct()) {
      is_valid = false;
    }
  }
  if (OB_SUCC(ret) && is_valid && lib::is_mysql_mode()) {
    int ret_tmp = ObGroupByChecker::check_group_by(upper_stmt);
    if (OB_ERR_WRONG_FIELD_WITH_GROUP == ret_tmp) {
      is_valid = false;
    } else if (OB_FAIL(ret_tmp)) {
      LOG_WARN("failed to check group by", K(ret));
    }
  }
  return ret;
}

int ObTransformSimplify::get_valid_child_stmts(
    ObSelectStmt* upper_stmt, ObSelectStmt* stmt, ObArray<ObSelectStmt*>& valid_child_stmts)
{
  int ret = OB_SUCCESS;
  bool is_valid = true;
  bool has_rownum = false;
  ObArray<ObRawExpr*> aggr_column_exprs;
  ObArray<ObRawExpr*> no_aggr_column_exprs;
  ObArray<ObRawExpr*> child_aggr_exprs;
  if (OB_ISNULL(upper_stmt) || OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (stmt->is_set_stmt()) {
    if (ObSelectStmt::UNION != stmt->get_set_op() || stmt->is_set_distinct() || stmt->has_limit()) {
      is_valid = false;
    } else {
      ObIArray<ObSelectStmt*>& child_stmts = stmt->get_set_query();
      for (int64_t i = 0; OB_SUCC(ret) && i < child_stmts.count(); ++i) {
        ret = SMART_CALL(get_valid_child_stmts(upper_stmt, child_stmts.at(i), valid_child_stmts));
      }
    }
  } else if (stmt->has_rollup() || stmt->has_window_function() || stmt->has_having() || stmt->has_sequence() ||
             stmt->has_limit()) {
    is_valid = false;
  } else if (!((stmt->has_distinct() && !stmt->has_group_by()) || (!stmt->has_distinct() && stmt->has_group_by()))) {
    // two scenarios will be allowed:
    // distinct without group by & group by without distinct.
    // cases with both distinct and group by, expected to be handled
    // by remove_stmt_distinct rule before.
    is_valid = false;
  } else if (OB_FAIL(stmt->has_rownum(has_rownum))) {
    LOG_WARN("failed to check has rownum", K(ret));
  } else if (has_rownum) {
    is_valid = false;
  } else if (OB_FAIL(get_upper_column_exprs(
                 *upper_stmt, *stmt, aggr_column_exprs, no_aggr_column_exprs, child_aggr_exprs, is_valid))) {
    LOG_WARN("failed to get upper column exprs", K(ret));
  } else if (!is_valid) {
    /*do nothing*/
  } else if (OB_FAIL(check_upper_group_by(*upper_stmt, no_aggr_column_exprs, is_valid))) {
    LOG_WARN("failed to check upper group by", K(ret));
  } else if (!is_valid) {
    /*do nothing*/
  } else if (OB_FAIL(check_aggrs_matched(
                 upper_stmt->get_aggr_items(), aggr_column_exprs, no_aggr_column_exprs, child_aggr_exprs, is_valid))) {
    LOG_WARN("failed to check aggrs matched", K(ret));
  } else if (!is_valid) {
    /*do nothing*/
  } else if (OB_FAIL(check_upper_condition(upper_stmt->get_condition_exprs(), aggr_column_exprs, is_valid))) {
    LOG_WARN("failed to check upper condition", K(ret));
  } else if (!is_valid) {
    /*do nothing*/
  } else if (OB_FAIL(valid_child_stmts.push_back(stmt))) {
    LOG_WARN("failed to push back child stmt", K(ret));
  } else {
    /*do nothing*/
  }
  return ret;
}

int ObTransformSimplify::remove_child_stmts_group_by(ObArray<ObSelectStmt*>& child_stmts)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->session_info_) || OB_ISNULL(ctx_->expr_factory_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ctx_));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < child_stmts.count(); ++i) {
    ObSelectStmt* stmt = NULL;
    if (OB_ISNULL(stmt = child_stmts.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else {
      stmt->get_aggr_items().reset();
      stmt->get_group_exprs().reset();
      stmt->reassign_rollup();
      stmt->get_rollup_exprs().reset();
      stmt->get_rollup_dirs().reset();
      stmt->get_order_items().reset();
      stmt->assign_all();
      for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_select_item_size(); ++i) {
        ObRawExpr* expr = stmt->get_select_item(i).expr_;
        if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null", K(ret));
        } else if (!expr->is_aggr_expr()) {
          /*do nothing*/
        } else if (T_FUN_MAX == expr->get_expr_type() || T_FUN_MIN == expr->get_expr_type() ||
                   T_FUN_SUM == expr->get_expr_type()) {
          ObRawExpr* cast_expr;
          if (OB_FAIL(ObRawExprUtils::try_add_cast_expr_above(ctx_->expr_factory_,
                  ctx_->session_info_,
                  *expr->get_param_expr(0),
                  expr->get_result_type(),
                  cast_expr))) {
            LOG_WARN("try add cast expr above failed", K(ret));
          } else {
            stmt->get_select_item(i).expr_ = cast_expr;
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected aggr", K(ret), K(expr->get_expr_type()));
        }
      }
    }
  }
  return ret;
}

int ObTransformSimplify::get_upper_column_exprs(ObSelectStmt& upper_stmt, ObSelectStmt& stmt,
    ObIArray<ObRawExpr*>& aggr_column_exprs, ObIArray<ObRawExpr*>& no_aggr_column_exprs,
    ObIArray<ObRawExpr*>& child_aggr_exprs, bool& is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  TableItem* table_item;
  ObSelectStmt* ref_query;
  if (OB_UNLIKELY(!upper_stmt.is_single_table_stmt()) || OB_ISNULL(table_item = upper_stmt.get_table_item(0)) ||
      OB_UNLIKELY(!table_item->is_generated_table()) || OB_ISNULL(ref_query = table_item->ref_query_) ||
      OB_UNLIKELY(stmt.get_select_item_size() != ref_query->get_select_item_size())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected upper stmt", K(ret), K(upper_stmt));
  }
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < stmt.get_select_item_size(); ++i) {
    ObRawExpr* stmt_select_expr = NULL;
    ColumnItem* column_item = NULL;
    if (OB_ISNULL(stmt_select_expr = stmt.get_select_item(i).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_ISNULL(
                   column_item = upper_stmt.get_column_item_by_id(table_item->table_id_, i + OB_APP_MIN_COLUMN_ID))) {
      // disable these aggr expr which can not be found in upper stmt,
      // otherwise, following elimination will meet unexpected, such as
      // inner stmt count(1) cnt with upper stmt max.
      if (stmt_select_expr->is_aggr_expr()) {
        is_valid = false;
      }
    } else if (OB_ISNULL(column_item->expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null expr", K(ret));
    } else if (stmt_select_expr->is_aggr_expr()) {  // is aggr
      int64_t idx = OB_INVALID_INDEX;
      if (!ObOptimizerUtil::find_item(stmt.get_aggr_items(), static_cast<ObAggFunRawExpr*>(stmt_select_expr), &idx)) {
        is_valid = false;
        LOG_DEBUG("cannot find aggr in stmt aggr item", K(ret), K(stmt), K(*stmt_select_expr));
      } else if (OB_FAIL(aggr_column_exprs.push_back(column_item->expr_)) ||
                 OB_FAIL(child_aggr_exprs.push_back(stmt.get_aggr_items().at(idx)))) {
        LOG_WARN("failed to push back aggr", K(ret));
      }
    } else if (ObOptimizerUtil::find_item(stmt.get_group_exprs(), stmt_select_expr) ||
               stmt_select_expr->has_const_or_const_expr_flag() || (stmt.has_distinct() && !stmt.has_group_by())) {
      // in group by or is const or distinct without group by
      if (OB_FAIL(no_aggr_column_exprs.push_back(column_item->expr_))) {
        LOG_WARN("failed to push back expr", K(ret));
      }
    } else {
      is_valid = false;
    }
  }
  return ret;
}

int ObTransformSimplify::check_upper_group_by(
    ObSelectStmt& upper_stmt, ObIArray<ObRawExpr*>& no_aggr_column_exprs, bool& is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  if (!ObOptimizerUtil::subset_exprs(upper_stmt.get_rollup_exprs(), no_aggr_column_exprs)) {
    /*do nothing*/
  } else if (!ObOptimizerUtil::subset_exprs(upper_stmt.get_group_exprs(), no_aggr_column_exprs)) {
    /*do nothing*/
  } else {
    is_valid = true;
  }
  return ret;
}

int ObTransformSimplify::check_aggrs_matched(ObIArray<ObAggFunRawExpr*>& upper_aggrs,
    ObIArray<ObRawExpr*>& aggr_column_exprs, ObIArray<ObRawExpr*>& no_aggr_column_exprs,
    ObIArray<ObRawExpr*>& child_aggr_exprs, bool& is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  if (aggr_column_exprs.count() != child_aggr_exprs.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected array count", K(ret), K(aggr_column_exprs), K(child_aggr_exprs));
  }
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < upper_aggrs.count(); ++i) {
    ObAggFunRawExpr* upper_aggr = NULL;
    ObRawExpr* aggr_param = NULL;
    int64_t idx = OB_INVALID_INDEX;
    if (OB_ISNULL(upper_aggr = upper_aggrs.at(i)) || OB_ISNULL(aggr_param = upper_aggr->get_param_expr(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if (ObOptimizerUtil::find_item(aggr_column_exprs, aggr_param, &idx)) {
      const ObAggFunRawExpr* child_aggr = static_cast<ObAggFunRawExpr*>(child_aggr_exprs.at(idx));
      if (upper_aggr->get_expr_type() != child_aggr->get_expr_type()) {
        is_valid = false;
      } else if (T_FUN_SUM == child_aggr->get_expr_type() && child_aggr->is_param_distinct()) {
        is_valid = false;
      } else {
        /*do nothing*/
      }
    } else if (ObOptimizerUtil::find_item(no_aggr_column_exprs, aggr_param) ||
               aggr_param->has_const_or_const_expr_flag()) {
      is_valid = T_FUN_MAX == upper_aggr->get_expr_type() || T_FUN_MIN == upper_aggr->get_expr_type();
    } else {
      is_valid = false;
    }
  }
  return ret;
}

// check upper stmt condition does not contain stmt aggr result
int ObTransformSimplify::check_upper_condition(
    ObIArray<ObRawExpr*>& cond_exprs, ObIArray<ObRawExpr*>& aggr_column_exprs, bool& is_valid)
{
  int ret = OB_SUCCESS;
  bool find_expr = false;
  int64_t N = cond_exprs.count();
  for (int64_t i = 0; OB_SUCC(ret) && !find_expr && i < N; ++i) {
    ret = exist_exprs_in_expr(cond_exprs.at(i), aggr_column_exprs, find_expr);
  }
  is_valid = !find_expr;
  return ret;
}

int ObTransformSimplify::exist_exprs_in_expr(const ObRawExpr* src_expr, ObIArray<ObRawExpr*>& dst_exprs, bool& is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = false;
  if (OB_ISNULL(src_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL", K(ret));
  } else if (dst_exprs.empty()) {
    /*do nothing*/
  } else if (ObOptimizerUtil::find_item(dst_exprs, src_expr)) {
    is_exist = true;
  } else {
    int64_t N = src_expr->get_param_count();
    for (int64_t i = 0; OB_SUCC(ret) && !is_exist && i < N; ++i) {
      const ObRawExpr* tmp_expr = src_expr->get_param_expr(i);
      bool is_correlated = false;
      if (OB_ISNULL(tmp_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected NULL", K(ret));
      } else if (tmp_expr->is_query_ref_expr()) {
        if (OB_FAIL(ObTransformUtils::is_correlated_expr(tmp_expr, dst_exprs.at(0)->get_expr_level(), is_correlated))) {
          LOG_WARN("failed to check correlated expr", K(ret));
        } else if (is_correlated) {
          is_exist = true;
        }
      } else if (OB_FAIL(SMART_CALL(exist_exprs_in_expr(tmp_expr, dst_exprs, is_exist)))) {
        LOG_WARN("failed to smart call", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformSimplify::replace_is_null_condition(ObDMLStmt* stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  bool is_happened = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data member or parameter is NULL", K(stmt), K(ctx_));
  } else if (stmt->is_sel_del_upd()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_condition_size(); ++i) {
      ObRawExpr* cond = NULL;
      if (OB_ISNULL(cond = stmt->get_condition_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("condition expr is null", K(ret));
      } else if (OB_FAIL(inner_replace_is_null_condition(stmt, stmt->get_condition_exprs().at(i), is_happened))) {
        LOG_WARN("failed to replace is null expr", K(ret));
      } else {
        trans_happened |= is_happened;
      }
    }
    if (OB_SUCC(ret) && stmt->is_select_stmt()) {
      ObSelectStmt* sel_stmt = static_cast<ObSelectStmt*>(stmt);
      for (int64_t i = 0; OB_SUCC(ret) && i < sel_stmt->get_having_expr_size(); ++i) {
        if (OB_FAIL(inner_replace_is_null_condition(sel_stmt, sel_stmt->get_having_exprs().at(i), is_happened))) {
          LOG_WARN("failed to replace is null expr", K(ret));
        } else {
          trans_happened |= is_happened;
        }
      }
    }
  }
  return ret;
}

int ObTransformSimplify::inner_replace_is_null_condition(ObDMLStmt* stmt, ObRawExpr*& expr, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  bool is_happened = false;
  trans_happened = false;
  bool is_stack_overflow = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null expr", K(expr), K(ret));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret));
  } else if (expr->is_op_expr()) {
    // do transformation for child exprs first
    ObOpRawExpr* op_expr = static_cast<ObOpRawExpr*>(expr);
    for (int64_t i = 0; OB_SUCC(ret) && i < op_expr->get_param_count(); i++) {
      ObRawExpr* temp = NULL;
      if (OB_ISNULL(temp = op_expr->get_param_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null expr", K(ret));
      } else if (OB_FAIL(SMART_CALL(inner_replace_is_null_condition(stmt, temp, is_happened)))) {
        LOG_WARN("failed to replace is null expr", K(ret));
      } else {
        trans_happened |= is_happened;
        op_expr->get_param_expr(i) = temp;
      }
    }
  }

  if (OB_SUCC(ret) && (T_OP_IS == expr->get_expr_type() || T_OP_IS_NOT == expr->get_expr_type())) {
    // do transforamtion for its own exprs
    if (OB_FAIL(do_replace_is_null_condition(stmt, expr, is_happened))) {
      LOG_WARN("failed to replace is null condition", K(ret));
    } else {
      trans_happened |= is_happened;
    }
  }

  if (OB_SUCC(ret) && trans_happened) {
    if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->session_info_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ctx_), K(ret));
    } else if (OB_FAIL(expr->formalize(ctx_->session_info_))) {
      LOG_WARN("failed to formalize expr", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObTransformSimplify::do_replace_is_null_condition(ObDMLStmt* stmt, ObRawExpr*& expr, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  if (OB_ISNULL(expr) || OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(stmt), K(expr), K(ret));
  } else if (T_OP_IS == expr->get_expr_type() || T_OP_IS_NOT == expr->get_expr_type()) {
    const ObOpRawExpr* op_expr = static_cast<ObOpRawExpr*>(expr);
    if (OB_UNLIKELY(op_expr->get_param_count() != 3)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected param numm", K(op_expr->get_param_count()), K(ret));
    } else {
      const ObRawExpr* child_0 = op_expr->get_param_expr(0);
      const ObRawExpr* child_1 = op_expr->get_param_expr(1);
      const ObRawExpr* child_2 = op_expr->get_param_expr(2);
      if (OB_ISNULL(child_0) || OB_ISNULL(child_1) || OB_ISNULL(child_2)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpecte null", K(child_0), K(child_1), K(child_2), K(ret));
      } else if (child_1->get_expr_type() == T_NULL && child_2->get_expr_type() == T_BOOL &&
                 !static_cast<const ObConstRawExpr*>(child_2)->get_value().get_bool()) {
        bool is_expected = false;
        if (child_0->get_expr_type() == T_FUN_COUNT) {
          is_expected = true;
        } else if (child_0->is_column_ref_expr()) {
          bool sql_auto_is_null = false;
          const ObColumnRefRawExpr* col_expr = static_cast<const ObColumnRefRawExpr*>(child_0);
          uint64_t table_id = col_expr->get_table_id();
          if (OB_FAIL(ctx_->session_info_->get_sql_auto_is_null(sql_auto_is_null))) {
            LOG_WARN("fail to get sql_auto_is_null system variable", K(ret));
          } else if (col_expr->is_not_null()) {
            if (T_OP_IS_NOT == expr->get_expr_type() ||
                (T_OP_IS == expr->get_expr_type() && !col_expr->get_result_type().is_datetime() &&
                    !col_expr->get_result_type().is_date() && !(sql_auto_is_null && col_expr->is_auto_increment()))) {
              if (OB_FAIL(is_expected_table_for_replace(stmt, table_id, is_expected))) {
                LOG_WARN("fail to judge expected table", K(ret), K(table_id), K(is_expected));
              }
            }
          }
        }
        if (OB_SUCC(ret) && is_expected) {
          bool b_value = (T_OP_IS_NOT == expr->get_expr_type());
          if (OB_FAIL(ObRawExprUtils::build_const_bool_expr(ctx_->expr_factory_, expr, b_value))) {
            LOG_WARN("create const bool expr failed", K(ret));
          } else {
            trans_happened = true;
          }
        }
      }
    }
  }
  return ret;
}

// rewrite null-reject conditions
int ObTransformSimplify::replace_op_null_condition(ObDMLStmt* stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  bool is_happened = false;
  trans_happened = false;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->exec_ctx_) ||
      OB_ISNULL(plan_ctx = ctx_->exec_ctx_->get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is NULL", K(ret), K(stmt), K(ctx_), K(plan_ctx));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_condition_size(); ++i) {
    ObRawExpr* cond = NULL;
    if (OB_ISNULL(cond = stmt->get_condition_expr(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("condition expr is null", K(ret));
    } else if (OB_FAIL(replace_cmp_null_condition(
                   stmt->get_condition_exprs().at(i), *stmt, plan_ctx->get_param_store(), is_happened))) {
      LOG_WARN("function replace_cmp_null_condition is failure", K(ret));
    } else {
      trans_happened |= is_happened;
    }
  }
  if (OB_SUCC(ret) && stmt->is_select_stmt()) {
    ObSelectStmt* sel_stmt = static_cast<ObSelectStmt*>(stmt);
    for (int64_t i = 0; OB_SUCC(ret) && i < sel_stmt->get_having_expr_size(); ++i) {
      if (OB_FAIL(replace_cmp_null_condition(
              sel_stmt->get_having_exprs().at(i), *stmt, plan_ctx->get_param_store(), is_happened))) {
        LOG_WARN("function replace_cmp_null_condition is failure", K(ret));
      } else {
        trans_happened |= is_happened;
      }
    }
  }
  return ret;
}

int ObTransformSimplify::replace_cmp_null_condition(
    ObRawExpr*& expr, const ObDMLStmt& stmt, const ParamStore& param_store, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  bool is_null_reject = false;
  bool is_happened = false;
  ObSEArray<const ObRawExpr*, 4> null_expr_lists;
  bool is_stack_overflow = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null expr", K(ret), K(expr));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_stack_overflow));
  } else if (T_OP_OR == expr->get_expr_type()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(replace_cmp_null_condition(expr->get_param_expr(i), stmt, param_store, is_happened)))) {
        LOG_WARN("or type execute failure", K(expr), K(ret));
      } else {
        trans_happened |= is_happened;
      }
    }
  } else if (OB_FAIL(extract_null_expr(expr, stmt, param_store, null_expr_lists))) {
    LOG_WARN("failed to use extract_null_expr funciton", K(ret), K(ctx_));
  } else if (0 != null_expr_lists.count()) {
    if (OB_FAIL(ObTransformUtils::is_null_reject_condition(expr, null_expr_lists, is_null_reject))) {
      LOG_WARN("failed to use is_null_reject_condition function", K(ret));
    } else if (is_null_reject) {
      bool b_value = false;
      if (OB_FAIL(ObRawExprUtils::build_const_bool_expr(ctx_->expr_factory_, expr, b_value))) {
        LOG_WARN("create const bool expr failed", K(ret));
      } else {
        trans_happened = true;
      }
    }
  } else {
    /*do nothing*/
  }
  return ret;
}

int ObTransformSimplify::extract_null_expr(
    ObRawExpr* expr, const ObDMLStmt& stmt, const ParamStore& param_store, ObIArray<const ObRawExpr*>& null_expr_lists)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null expr", K(expr), K(ret));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_stack_overflow));
  } else if (T_NULL == expr->get_expr_type()) {
    if (OB_FAIL(null_expr_lists.push_back(expr))) {
      LOG_WARN("null expr", K(expr), K(ret));
    } else {
      /*do notiing*/
    }
  } else if (T_QUESTIONMARK == expr->get_expr_type()) {
    const ObObj& value = static_cast<ObConstRawExpr*>(expr)->get_value();
    bool is_pre_param = false;
    if (OB_FAIL(ObTransformUtils::is_question_mark_pre_param(stmt, value.get_unknown(), is_pre_param))) {
      LOG_WARN("failed to check is question mark pre param", K(ret));
    } else if (!is_pre_param) {
      // do nothing
    } else if (param_store.at(value.get_unknown()).is_null()) {
      if (OB_FAIL(null_expr_lists.push_back(expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      }
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      ObRawExpr* temp = NULL;
      if (OB_ISNULL(temp = expr->get_param_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null expr", K(ret));
      } else if (OB_FAIL(SMART_CALL(extract_null_expr(temp, stmt, param_store, null_expr_lists)))) {
        LOG_WARN("failed to use extract_null_expr function", K(ret));
      } else {
        /*do nothing*/
      }
    }
  }
  return ret;
}

int ObTransformSimplify::is_expected_table_for_replace(ObDMLStmt* stmt, uint64_t table_id, bool& is_expected)
{
  int ret = OB_SUCCESS;
  is_expected = true;
  const TableItem* table_item = NULL;
  bool is_stack_overflow = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid parameter", K(stmt));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_stack_overflow));
  } else if (NULL == (table_item = stmt->get_table_item_by_id(table_id))) {
    // table from upper stmt
    ObStmt* upper_stmt = NULL;
    if (OB_ISNULL(upper_stmt = stmt->get_parent_namespace_stmt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("upper stmt shouldn't be NULL", K(ret));
    } else if (OB_UNLIKELY(!upper_stmt->is_dml_stmt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("upper stmt must be dml", K(upper_stmt->get_stmt_type()));
    } else if (OB_FAIL(SMART_CALL(
                   is_expected_table_for_replace(static_cast<ObDMLStmt*>(upper_stmt), table_id, is_expected)))) {
      LOG_WARN("fail to judge expected table", K(ret));
    } else { /*do nothing*/
    }
  } else {
    // table from current stmt
    if (table_item->is_basic_table() || table_item->is_generated_table()) {
      bool is_on_null_side = false;
      if (OB_FAIL(ObOptimizerUtil::is_table_on_null_side(stmt, table_item->table_id_, is_on_null_side))) {
        LOG_WARN("check is table on null side failed", K(ret), K(*table_item));
      } else {
        is_expected = !is_on_null_side;
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected table item type", K_(table_item->type));
    }
  }
  return ret;
}

/*
 * select * from t1 where c1 in (select c1 from t2 order by c2);
 * ==>
 * select * from t1 where c1 in (select c1 from t2);
 *
 * select distinct c1 from (select c1 from t1 order by c2);
 * ==>
 * select distinct c1 from (select c1 from t1);
 *
 * select  * from (select * from t1 order by c1) order by c2;
 * ==>
 * select  * from (select * from t1) order by c2;
 *
 * select c1 from (select c1 from t1 order by c2) group by c1;
 * ==>
 * select c1 from (select c1 from t1) group by c1;
 *
 * select c1 from t1 where c2 in (select c2 from t2 order by c1);
 * ==>
 * select c1 from t1 where c2 in (select c2 from t2);
 */
int ObTransformSimplify::remove_stmt_order_by(ObDMLStmt*& stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  bool subquery_happened = false;
  bool view_happened = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid parameter", K(stmt));
  } else if (OB_FAIL(remove_order_by_for_subquery(stmt, subquery_happened))) {
    LOG_WARN("remove order by for subquery failed");
  } else if (OB_FAIL(remove_order_by_for_view_stmt(stmt, view_happened))) {
    LOG_WARN("remove order by for subquery failed");
  } else {
    trans_happened = subquery_happened | view_happened;
  }
  return ret;
}

int ObTransformSimplify::remove_order_by_for_subquery(ObDMLStmt*& stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null");
  } else {
    ObIArray<ObQueryRefRawExpr*>& subquery_exprs = stmt->get_subquery_exprs();
    ObSelectStmt* subquery = NULL;
    ObArray<OrderItem> new_order_items;
    bool happened = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < subquery_exprs.count(); ++i) {
      ObQueryRefRawExpr* query_ref = subquery_exprs.at(i);
      if (OB_ISNULL(query_ref) || OB_ISNULL(subquery = query_ref->get_ref_stmt())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("subquery reference is invalid", K(query_ref));
      } else if (!subquery->has_limit() && !subquery->is_contains_assignment() && !subquery->is_hierarchical_query() &&
                 subquery->has_order_by() && OB_FAIL(do_remove_stmt_order_by(subquery, happened))) {
        LOG_WARN("do remove stmt order by failed", K(ret));
      } else {
        trans_happened |= happened;
      }
    }
  }
  return ret;
}

int ObTransformSimplify::remove_order_by_for_view_stmt(ObDMLStmt*& stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (!stmt->is_sel_del_upd()) {
    /*do nothing*/
  } else if (stmt->is_select_stmt() && static_cast<ObSelectStmt*>(stmt)->has_window_function()) {
    /*do nothing*/
  } else {
    ObSelectStmt* view_stmt = NULL;
    ObSelectStmt* select_stmt = NULL;
    bool happened = false;
    ObIArray<TableItem*>& table_items = stmt->get_table_items();
    for (int64_t i = 0; OB_SUCC(ret) && i < table_items.count(); ++i) {
      const TableItem* table_item = table_items.at(i);
      if (OB_ISNULL(table_item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("joined table is null", K(ret));
      } else if (!table_item->is_generated_table()) {
        /*do nothing*/
      } else if (OB_ISNULL(view_stmt = table_item->ref_query_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("view stmt is null", K(ret));
      } else if (view_stmt->is_contains_assignment() || view_stmt->is_hierarchical_query()) {
        // do nothing
      } else if (1 == table_items.count()) {
        if (stmt->is_select_stmt()) {
          bool has_rownum = false;
          select_stmt = static_cast<ObSelectStmt*>(stmt);
          if (OB_FAIL(select_stmt->has_rownum(has_rownum))) {
            LOG_WARN("failed to has rownum", K(ret));
          } else if (has_rownum) {
            /*do nothing*/
          } else if ((select_stmt->has_group_by() || select_stmt->has_distinct() || select_stmt->has_order_by()) &&
                     !view_stmt->has_limit() && view_stmt->has_order_by() &&
                     OB_FAIL(do_remove_stmt_order_by(view_stmt, happened))) {
            LOG_WARN("do remove stmt order by failed", K(ret));
          } else {
            trans_happened |= happened;
          }
        }
      } else if (!view_stmt->has_limit() && view_stmt->has_order_by() &&
                 OB_FAIL(do_remove_stmt_order_by(view_stmt, happened))) {
        LOG_WARN("do remove stmt order by failed", K(ret));
      } else {
        trans_happened |= happened;
      }
    }
  }
  return ret;
}

int ObTransformSimplify::do_remove_stmt_order_by(ObSelectStmt* select_stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  if (OB_ISNULL(select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("select stmt is null", K(select_stmt), K(ret));
  } else {
    ObArray<OrderItem> new_order_items;
    for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt->get_order_item_size(); ++i) {
      const OrderItem& order_item = select_stmt->get_order_item(i);
      if (OB_ISNULL(order_item.expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("order expr is null");
      } else if (order_item.expr_->has_flag(CNT_SUB_QUERY)) {
        // in case the subquery returns more than one rows,
        // we should keep the subquery which may returns error
        if (OB_FAIL(new_order_items.push_back(order_item))) {
          LOG_WARN("store new order item failed", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (select_stmt->get_order_items().count() != new_order_items.count()) {
        trans_happened = true;
      }
      select_stmt->get_order_items().reuse();
      if (OB_FAIL(select_stmt->get_order_items().assign(new_order_items))) {
        LOG_WARN("assign new order items failed", K(ret));
      }
    }
  }
  return ret;
}

/* if winfunc's partition exprs are unique, we can simplify the window function
create table t(pk int primary key, c1 int);
select max(c1) over(partition by pk) from t;
=>
select c1 from t;
*/
int ObTransformSimplify::remove_stmt_win(ObDMLStmt* stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  ObSelectStmt* select_stmt = NULL;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (!stmt->is_select_stmt()) {
    /*do nothing */
  } else {
    select_stmt = static_cast<ObSelectStmt*>(stmt);
    ObSEArray<ObRawExpr*, 4> win_exprs;
    bool can_be = false;
    ObWinFunRawExpr* win_expr = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt->get_window_func_count(); ++i) {
      if (OB_ISNULL(win_expr = select_stmt->get_window_func_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null window func", K(ret));
      } else if (OB_FAIL(check_stmt_win_can_be_removed(select_stmt, win_expr, can_be))) {
        LOG_WARN("check stmt window func can be removed failed", K(ret));
      } else if (!can_be) {
        /*do nothing*/
      } else if (OB_FAIL(win_exprs.push_back(win_expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      }
    }
    if (OB_FAIL(ret) || win_exprs.empty()) {
      /*do nothing*/
    } else if (OB_FAIL(do_remove_stmt_win(select_stmt, win_exprs))) {
      LOG_WARN("do transform remove group by failed", K(ret));
    } else {
      trans_happened = true;
    }
  }
  return ret;
}

int ObTransformSimplify::check_aggr_win_can_be_removed(const ObDMLStmt* stmt, ObRawExpr* expr, bool& can_remove)
{
  can_remove = false;
  int ret = OB_SUCCESS;
  ObItemType func_type = T_INVALID;
  ObAggFunRawExpr* aggr = NULL;
  ObWinFunRawExpr* win_func = NULL;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (expr->is_aggr_expr()) {
    aggr = static_cast<ObAggFunRawExpr*>(expr);
    func_type = aggr->get_expr_type();
  } else if (expr->is_win_func_expr()) {
    win_func = static_cast<ObWinFunRawExpr*>(expr);
    func_type = win_func->get_func_type();
    aggr = win_func->get_agg_expr();
    func_type = NULL == aggr ? win_func->get_func_type() : aggr->get_expr_type();
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected expr", K(ret), K(expr));
  }
  if (OB_SUCC(ret)) {
    switch (func_type) {
      // aggr func
      case T_FUN_COUNT:  // case when 1 or 0
      case T_FUN_MAX:    // return expr
      case T_FUN_MIN:
      // case T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS_MERGE:
      case T_FUN_AVG:  // return expr
      case T_FUN_COUNT_SUM:
      case T_FUN_SUM:
      // case T_FUN_APPROX_COUNT_DISTINCT: // return 1 or 0
      // case T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS:
      case T_FUN_GROUP_CONCAT:  // return expr
      // case T_FUN_GROUP_RANK:// return 1 or 2
      // case T_FUN_GROUP_DENSE_RANK:
      // case T_FUN_GROUP_PERCENT_RANK:// return 1 or 0
      // case T_FUN_GROUP_CUME_DIST:// return 1 or 0.5
      case T_FUN_MEDIAN:  // return expr
      case T_FUN_GROUP_PERCENTILE_CONT:
      case T_FUN_GROUP_PERCENTILE_DISC:
      case T_FUN_KEEP_MAX:
      case T_FUN_KEEP_MIN:
      case T_FUN_KEEP_COUNT:  // return 1 or 0
      case T_FUN_KEEP_SUM:    // return expr
      // ObExpandAggregateUtils::expand_aggr_expr
      // ObExpandAggregateUtils::expand_window_aggr_expr

      // window func
      case T_WIN_FUN_ROW_NUMBER:      // return 1
      case T_WIN_FUN_RANK:            // return 1
      case T_WIN_FUN_DENSE_RANK:      // return 1
      case T_WIN_FUN_PERCENT_RANK: {  // return 0
        can_remove = true;
        break;
      }
      case T_WIN_FUN_NTILE: {  // return 1
        // need check invalid param: ntile(-1)
        can_remove = false;
        ObRawExpr* expr = NULL;
        int64_t bucket_num = 0;
        bool is_valid = false;
        if (OB_ISNULL(win_func) || OB_UNLIKELY(win_func->get_func_params().empty()) ||
            OB_ISNULL(expr = win_func->get_func_params().at(0))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected func", K(ret));
        } else if (OB_FAIL(get_param_value(stmt, expr, is_valid, bucket_num))) {
          LOG_WARN("failed to get param value", K(ret));
        } else if (!is_valid) {
          can_remove = false;
        } else if (OB_UNLIKELY(bucket_num <= 0)) {
          if (is_oracle_mode()) {
            ret = OB_DATA_OUT_OF_RANGE;
            LOG_WARN("bucket_num out of range", K(ret), K(bucket_num));
          } else {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("bucket_num is invalid", K(ret), K(bucket_num));
          }
        } else {
          can_remove = true;
        }
        break;
      }
      case T_WIN_FUN_NTH_VALUE: {  // nth_value(expr,1) return expr, else return null
        // need check invalid param: nth_value(expr, -1)
        can_remove = false;
        ObRawExpr* nth_expr = NULL;
        int64_t value = 0;
        bool is_valid = false;
        if (OB_ISNULL(win_func) || OB_UNLIKELY(2 > win_func->get_func_params().count()) ||
            OB_ISNULL(nth_expr = win_func->get_func_params().at(1))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected func", K(ret));
        } else if (OB_FAIL(get_param_value(stmt, nth_expr, is_valid, value))) {
          LOG_WARN("failed to get param value", K(ret));
        } else if (!is_valid) {
          can_remove = false;
        } else if (OB_UNLIKELY(value <= 0)) {
          ret = OB_DATA_OUT_OF_RANGE;
          LOG_WARN("invalid argument", K(ret), K(value));
        } else {
          can_remove = true;
        }
        break;
      }
      case T_WIN_FUN_FIRST_VALUE:   // return expr (respect or ignore nulls)
      case T_WIN_FUN_LAST_VALUE: {  // return expr (respect or ignore nulls)
        // first_value && last_value has been converted to nth_value when resolving
        can_remove = false;
        break;
      }
      case T_WIN_FUN_CUME_DIST: {  // return 1
        can_remove = true;
        break;
      }
      case T_WIN_FUN_LEAD:   // return null or default value
      case T_WIN_FUN_LAG: {  // return null or default value
        can_remove = false;
        ObRawExpr* expr = NULL;
        int64_t value = 0;
        bool is_valid = false;
        if (OB_ISNULL(win_func) || OB_UNLIKELY(win_func->get_func_params().empty())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected func", K(ret));
        } else if (1 == win_func->get_func_params().count()) {
          can_remove = true;
        } else if (OB_ISNULL(expr = win_func->get_func_params().at(1))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected NULL", K(ret));
        } else if (OB_FAIL(get_param_value(stmt, expr, is_valid, value))) {
          LOG_WARN("failed to get param value", K(ret));
        } else if (!is_valid) {
          can_remove = false;
        } else if (OB_UNLIKELY(value < 0)) {
          ret = OB_ERR_ARGUMENT_OUT_OF_RANGE;
          LOG_USER_ERROR(OB_ERR_ARGUMENT_OUT_OF_RANGE, value);
          LOG_WARN("lead/lag argument is out of range", K(ret), K(value));
        } else {
          can_remove = true;
        }
        break;
      }
      case T_WIN_FUN_RATIO_TO_REPORT: {
        can_remove = true;
        break;
      }
      case T_WIN_FUN_SUM:
      case T_WIN_FUN_MAX:
      case T_WIN_FUN_AVG:
      default: {
        can_remove = false;
        break;
      }
    }
  }
  return ret;
}

/**
 *   count(x) --> case when x is not null then 1 esle 0 end
 *   count(*) --> 1
 **/
int ObTransformSimplify::transform_aggr_win_to_common_expr(
    ObSelectStmt* select_stmt, ObRawExpr* expr, ObRawExpr*& new_expr)
{
  int ret = OB_SUCCESS;
  new_expr = NULL;
  ObRawExpr* param_expr = NULL;
  ObItemType func_type = T_INVALID;
  ObAggFunRawExpr* aggr = NULL;
  ObWinFunRawExpr* win_func = NULL;
  if (OB_ISNULL(select_stmt) || OB_ISNULL(expr) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_) ||
      OB_ISNULL(ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (expr->is_aggr_expr()) {
    aggr = static_cast<ObAggFunRawExpr*>(expr);
    func_type = aggr->get_expr_type();
  } else if (expr->is_win_func_expr()) {
    win_func = static_cast<ObWinFunRawExpr*>(expr);
    func_type = win_func->get_func_type();
    aggr = win_func->get_agg_expr();
    func_type = NULL == aggr ? win_func->get_func_type() : aggr->get_expr_type();
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected expr", K(ret), K(expr));
  }
  if (OB_SUCC(ret)) {
    switch (func_type) {
      // aggr
      case T_FUN_MAX:  // return expr
      case T_FUN_MIN:
      case T_FUN_AVG:
      case T_FUN_SUM:
      case T_FUN_GROUP_CONCAT:
      case T_FUN_MEDIAN:
      case T_FUN_KEEP_MAX:
      case T_FUN_KEEP_MIN:
      case T_FUN_KEEP_SUM:
      case T_FUN_COUNT_SUM: {
        if (OB_ISNULL(aggr) || OB_ISNULL(param_expr = aggr->get_param_expr(0))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        }
        break;
      }
      case T_FUN_GROUP_PERCENTILE_CONT:  // return expr
      case T_FUN_GROUP_PERCENTILE_DISC: {
        if (OB_ISNULL(aggr) || OB_UNLIKELY(aggr->get_order_items().empty()) ||
            OB_ISNULL(param_expr = aggr->get_order_items().at(0).expr_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected func", K(ret));
        }
        break;
      }
      case T_FUN_COUNT:
      case T_FUN_KEEP_COUNT: {  // return 1 or 0
        ObConstRawExpr* const_one = NULL;
        ObConstRawExpr* const_zero = NULL;
        if (OB_ISNULL(aggr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (lib::is_oracle_mode()) {  // oracle mode return number 1 or 0
          if (OB_FAIL(ObRawExprUtils::build_const_number_expr(
                  *ctx_->expr_factory_, ObNumberType, number::ObNumber::get_positive_one(), const_one))) {
            LOG_WARN("failed to build const number expr", K(ret));
          } else if (0 == aggr->get_real_param_count()) {  // count(*) --> 1
            param_expr = const_one;
          } else if (OB_FAIL(ObRawExprUtils::build_const_number_expr(
                         *ctx_->expr_factory_, ObNumberType, number::ObNumber::get_zero(), const_zero))) {
            LOG_WARN("failed to build const number expr", K(ret));
          }
        } else {  // mysql mode return int 1 or 0
          if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*ctx_->expr_factory_, ObIntType, 1, const_one))) {
            LOG_WARN("failed to build const int expr", K(ret));
          } else if (0 == aggr->get_real_param_count()) {  // count(*) --> 1
            param_expr = const_one;
          } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*ctx_->expr_factory_, ObIntType, 0, const_zero))) {
            LOG_WARN("failed to build const int expr", K(ret));
          }
        }

        if (OB_SUCC(ret) && 0 != aggr->get_real_param_count()  // count(c1) --> case when
            && OB_FAIL(ObTransformUtils::build_case_when_expr(
                   *select_stmt, expr->get_param_expr(0), const_one, const_zero, param_expr, ctx_))) {
          LOG_WARN("failed to build case when expr", K(ret));
        }
        break;
      }
      case T_WIN_FUN_ROW_NUMBER:  // return int 1
      case T_WIN_FUN_RANK:
      case T_WIN_FUN_DENSE_RANK: {
        ObConstRawExpr* const_one = NULL;
        if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*ctx_->expr_factory_, ObIntType, 1, const_one))) {
          LOG_WARN("failed to build const int expr", K(ret));
        } else {
          param_expr = const_one;
        }
        break;
      }
      case T_WIN_FUN_NTILE: {  // return int 1 and add constraint
        ObConstRawExpr* const_one = NULL;
        if (OB_ISNULL(win_func) || OB_UNLIKELY(win_func->get_func_params().empty())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected func", K(ret), K(win_func));
        } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*ctx_->expr_factory_, ObIntType, 1, const_one))) {
          LOG_WARN("failed to build const int expr", K(ret));
        } else if (OB_FAIL(ObTransformUtils::add_const_param_constraints(
                       win_func->get_func_params().at(0), select_stmt->get_query_ctx(), ctx_))) {
          LOG_WARN("failed to add const param constraints", K(ret));
        } else {
          param_expr = const_one;
        }
        break;
      }
      case T_WIN_FUN_CUME_DIST: {  // return number 1
        ObConstRawExpr* const_one = NULL;
        if (OB_FAIL(ObRawExprUtils::build_const_number_expr(
                *ctx_->expr_factory_, ObNumberType, number::ObNumber::get_positive_one(), const_one))) {
          LOG_WARN("failed to build const number expr", K(ret));
        } else {
          param_expr = const_one;
        }
        break;
      }
      case T_WIN_FUN_PERCENT_RANK: {  // return number 0
        ObConstRawExpr* const_zero = NULL;
        if (OB_FAIL(ObRawExprUtils::build_const_number_expr(
                *ctx_->expr_factory_, ObNumberType, number::ObNumber::get_zero(), const_zero))) {
          LOG_WARN("failed to build const number expr", K(ret));
        } else {
          param_expr = const_zero;
        }
        break;
      }
      case T_WIN_FUN_NTH_VALUE: {  // nth_value(expr,1) return expr, else return null
        // need add constraint for nth_expr
        ObRawExpr* nth_expr = NULL;
        int64_t value = 0;
        bool is_valid = false;
        if (OB_ISNULL(win_func) || OB_UNLIKELY(2 > win_func->get_func_params().count()) ||
            OB_ISNULL(nth_expr = win_func->get_func_params().at(1))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected func", K(ret));
        } else if (OB_FAIL(get_param_value(select_stmt, nth_expr, is_valid, value))) {
          LOG_WARN("failed to get param value", K(ret));
        } else if (OB_UNLIKELY(!is_valid || value <= 0)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected func", K(ret), K(*win_func));
        } else if (OB_FAIL(
                       ObTransformUtils::add_const_param_constraints(nth_expr, select_stmt->get_query_ctx(), ctx_))) {
          LOG_WARN("failed to add const param constraints", K(ret));
        } else if (1 == value) {  // return expr
          param_expr = win_func->get_func_params().at(0);
        } else {  // return null
          ret = ObRawExprUtils::build_null_expr(*ctx_->expr_factory_, param_expr);
        }
        break;
      }
      case T_WIN_FUN_LEAD:  // return null or default value
      case T_WIN_FUN_LAG: {
        ObRawExpr* expr = NULL;
        int64_t value = 1;
        bool is_valid = false;
        if (OB_ISNULL(win_func) || OB_UNLIKELY(win_func->get_func_params().empty())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected func", K(ret));
        } else if (1 == win_func->get_func_params().count()) {
          value = 1;
        } else if (OB_ISNULL(expr = win_func->get_func_params().at(1))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected NULL", K(ret));
        } else if (OB_FAIL(get_param_value(select_stmt, expr, is_valid, value))) {
          LOG_WARN("failed to get param value", K(ret));
        } else if (OB_UNLIKELY(!is_valid || value < 0)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected func", K(ret), K(*win_func));
        } else if (OB_FAIL(ObTransformUtils::add_const_param_constraints(expr, select_stmt->get_query_ctx(), ctx_))) {
          LOG_WARN("failed to add const param constraints", K(ret));
        }

        if (OB_FAIL(ret)) {
        } else if (0 == value) {  // return current value
          param_expr = win_func->get_func_params().at(0);
        } else if (2 < win_func->get_func_params().count()) {  // return default value
          param_expr = win_func->get_func_params().at(2);
        } else if (OB_FAIL(ObRawExprUtils::build_null_expr(*ctx_->expr_factory_, param_expr))) {
          LOG_WARN("failed to build null expr", K(ret));
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected func", K(ret), K(*expr));
        break;
      }
    }

    if (OB_FAIL(ret)) {
      /*do nothing*/
    } else if (OB_ISNULL(param_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::try_add_cast_expr_above(
                   ctx_->expr_factory_, ctx_->session_info_, *param_expr, expr->get_result_type(), new_expr))) {
      LOG_WARN("try add cast expr above failed", K(ret));
    }
  }
  return ret;
}

int ObTransformSimplify::get_param_value(const ObDMLStmt* stmt, ObRawExpr* param, bool& is_valid, int64_t& value)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  ObObj obj_value;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  const ParamStore* param_store = NULL;
  if (OB_ISNULL(stmt) || OB_ISNULL(param) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->allocator_) ||
      OB_ISNULL(ctx_->exec_ctx_) || OB_ISNULL(plan_ctx = ctx_->exec_ctx_->get_physical_plan_ctx()) ||
      OB_ISNULL(param_store = &plan_ctx->get_param_store())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null", K(ret), K(stmt), K(param), K(ctx_), K(plan_ctx));
  } else if (T_INT == param->get_expr_type() || T_NUMBER == param->get_expr_type()) {
    obj_value = static_cast<const ObConstRawExpr*>(param)->get_value();
  } else if (T_QUESTIONMARK == param->get_expr_type()) {
    int64_t idx = static_cast<const ObConstRawExpr*>(param)->get_value().get_unknown();
    if (idx < 0 || idx >= param_store->count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Idx is invalid", K(idx), K(param_store->count()), K(ret));
    } else {
      obj_value = param_store->at(idx);
    }
  } else if ((param->has_flag(IS_CALCULABLE_EXPR) || param->is_const_expr()) &&
             (param->get_result_type().is_integer_type() || param->get_result_type().is_number())) {
    if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(
            stmt->get_stmt_type(), ctx_->session_info_, param, obj_value, param_store, *ctx_->allocator_))) {
      LOG_WARN("Failed to get const or calculable expr value", K(ret));
    }
  } else {
    is_valid = false;
  }
  if (OB_SUCC(ret) && is_valid) {
    number::ObNumber number;
    if (obj_value.is_null()) {
      is_valid = false;
    } else if (obj_value.is_integer_type()) {
      value = obj_value.get_int();
    } else if (!obj_value.is_numeric_type()) {
      is_valid = false;
    } else if (OB_FAIL(obj_value.get_number(number))) {
      LOG_WARN("unexpected value type", K(ret), K(obj_value));
    } else if (OB_UNLIKELY(!number.is_valid_int64(value))) {
      is_valid = false;
    }
  }
  return ret;
}

int ObTransformSimplify::check_stmt_win_can_be_removed(
    ObSelectStmt* select_stmt, ObWinFunRawExpr* win_expr, bool& can_be)
{
  int ret = OB_SUCCESS;
  can_be = false;
  bool is_unique = false;
  bool can_remove = false;
  bool contain = false;
  if (OB_ISNULL(select_stmt) || OB_ISNULL(win_expr) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->schema_checker_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL pointer error", K(ret));
  } else if (OB_FAIL(ObTransformUtils::check_stmt_unique(select_stmt,
                 ctx_->session_info_,
                 ctx_->schema_checker_,
                 win_expr->get_partition_exprs(),
                 true,
                 is_unique,
                 FLAGS_IGNORE_DISTINCT))) {
    LOG_WARN("failed to check stmt unique", K(ret));
  } else if (!is_unique) {
    /*do nothing*/
  } else if (BoundType::BOUND_CURRENT_ROW != win_expr->get_upper().type_ &&
             BoundType::BOUND_CURRENT_ROW != win_expr->get_upper().type_ &&
             win_expr->get_upper().is_preceding_ == win_expr->get_lower().is_preceding_) {
  } else if (OB_FAIL(check_aggr_win_can_be_removed(select_stmt, win_expr, can_remove))) {
    LOG_WARN("failed to check win can be removed", K(ret));
  } else if (!can_remove) {
    can_be = false;
  } else if (select_stmt->is_scala_group_by() && OB_FAIL(check_window_contain_aggr(win_expr, contain))) {
    LOG_WARN("failed to check window contain aggr", K(ret));
  } else if (contain) {
    can_be = false;
  } else {
    can_be = true;
  }
  return ret;
}

int ObTransformSimplify::check_window_contain_aggr(ObWinFunRawExpr* win_expr, bool& contain)
{
  int ret = OB_SUCCESS;
  contain = false;
  if (OB_ISNULL(win_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL pointer error", K(ret), K(win_expr));
  } else {
    ObRawExpr* expr = NULL;
    ObIArray<OrderItem>& win_order = win_expr->get_order_items();
    for (int64_t i = 0; !contain && OB_SUCC(ret) && i < win_order.count(); ++i) {
      if (OB_ISNULL(expr = win_order.at(i).expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret));
      } else if (expr->has_flag(CNT_AGG)) {
        contain = true;
      }
    }
    ObIArray<ObRawExpr*>& win_partition = win_expr->get_partition_exprs();
    for (int64_t i = 0; !contain && OB_SUCC(ret) && i < win_partition.count(); ++i) {
      if (OB_ISNULL(expr = win_partition.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret));
      } else if (expr->has_flag(CNT_AGG)) {
        contain = true;
      }
    }
    if (OB_SUCC(ret) && !contain) {
      if (NULL != win_expr->get_upper().interval_expr_ && win_expr->get_upper().interval_expr_->has_flag(CNT_AGG)) {
        contain = true;
      } else if (NULL != win_expr->get_lower().interval_expr_ &&
                 win_expr->get_lower().interval_expr_->has_flag(CNT_AGG)) {
        contain = true;
      }
    }
  }
  return ret;
}

int ObTransformSimplify::do_remove_stmt_win(ObSelectStmt* select_stmt, ObIArray<ObRawExpr*>& exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL pointer error", K(ret), K(select_stmt));
  } else {
    ObRawExpr* new_expr = NULL;
    ObSEArray<ObRawExpr*, 4> new_exprs;
    for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
      ObSysFunRawExpr* cast_expr = NULL;
      if (OB_ISNULL(exprs.at(i)) || !exprs.at(i)->is_win_func_expr()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected win expr", K(ret), K(exprs.at(i)));
      } else if (OB_FAIL(transform_aggr_win_to_common_expr(select_stmt, exprs.at(i), new_expr))) {
        LOG_WARN("transform aggr to common expr failed", K(ret));
      } else if (OB_FAIL(new_exprs.push_back(new_expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else if (OB_FAIL(select_stmt->remove_window_func_expr(static_cast<ObWinFunRawExpr*>(exprs.at(i))))) {
        LOG_WARN("failed to remove window func expr", K(ret));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(select_stmt->replace_inner_stmt_expr(exprs, new_exprs))) {
      LOG_WARN("select_stmt replace inner stmt expr failed", K(ret), K(select_stmt));
    }
  }
  return ret;
}

int ObTransformSimplify::remove_order_by_duplicates(ObDMLStmt*& stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null pointer passed to transform", K(ret));
  } else if (!stmt->is_sel_del_upd()) {
    // do nothing
  } else {
    ObIArray<OrderItem>& order_items = stmt->get_order_items();
    ObIArray<ObRawExpr*>& cond_exprs = stmt->get_condition_exprs();
    if (order_items.count() > 1) {
      ObArray<OrderItem> new_order_items;
      int64_t N = order_items.count();
      int64_t M = cond_exprs.count();
      bool is_exist = false;
      for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
        OrderItem& cur_item = order_items.at(i);
        is_exist = false;
        if (OB_FAIL(exist_item_by_expr(cur_item.expr_, new_order_items, is_exist))) {
          LOG_WARN("fail to adjust exist order item", K(ret), K(cur_item), K(new_order_items), K(is_exist));
        } else if (!is_exist) {
          for (int64_t j = 0; OB_SUCC(ret) && !is_exist && j < M; ++j) {
            ObRawExpr* op_expr = cond_exprs.at(j);
            if (OB_ISNULL(op_expr)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("null pointer in condtion exprs", K(ret));
            } else if (T_OP_EQ == op_expr->get_expr_type()) {
              if (op_expr->get_param_count() != 2) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("eq expr should have two param", K(ret));
              } else {
                ObRawExpr* left_param = op_expr->get_param_expr(0);
                ObRawExpr* right_param = op_expr->get_param_expr(1);
                if (OB_ISNULL(left_param) || OB_ISNULL(right_param)) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("null pointer in condtion exprs", K(ret));
                } else {
                  bool is_consistent = false;
                  if (right_param == cur_item.expr_) {
                    if (OB_FAIL(ObRawExprUtils::expr_is_order_consistent(left_param, right_param, is_consistent))) {
                      LOG_WARN("check expr is order consistent failed", K(ret), K(*left_param), K(*right_param));
                    } else if (is_consistent) {
                      if (OB_FAIL(exist_item_by_expr(left_param, new_order_items, is_exist))) {
                        LOG_WARN("fail to find expr in items", K(ret));
                      }
                    }
                  } else if (left_param == cur_item.expr_) {
                    if (OB_FAIL(ObRawExprUtils::expr_is_order_consistent(right_param, left_param, is_consistent))) {
                      LOG_WARN("check expr is order consistent failed", K(ret), K(*left_param), K(*right_param));
                    } else if (is_consistent) {
                      if (OB_FAIL(exist_item_by_expr(right_param, new_order_items, is_exist))) {
                        LOG_WARN("fail to find expr in items", K(ret));
                      }
                    }
                  } else { /* do nothing */
                  }
                }
              }
            } else { /* do nothing */
            }
          }
        } else { /*do nothing*/
        }
        if (OB_SUCC(ret) && !is_exist) {
          if (OB_FAIL(new_order_items.push_back(cur_item))) {
            LOG_WARN("failed to push back order item", K(ret), K(cur_item));
          }
        }
      }  // for
      if (OB_SUCC(ret)) {
        if (N != new_order_items.count()) {
          trans_happened = true;
        }
        if (OB_FAIL(order_items.assign(new_order_items))) {
          LOG_WARN("failed to reset order items", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTransformSimplify::remove_stmt_group_by(ObDMLStmt*& stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  ObSelectStmt* select_stmt = NULL;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (!stmt->is_select_stmt()) {
    // do nothing
  } else if (FALSE_IT(select_stmt = static_cast<ObSelectStmt*>(stmt))) {
    /*do nothing*/
  } else if (select_stmt->get_group_expr_size() > 0 && !select_stmt->has_rollup()) {
    // check stmt group by can be removed
    bool can_be = false;
    if (OB_FAIL(check_stmt_group_by_can_be_removed(select_stmt, can_be))) {
      LOG_WARN("check stmt group by can be removed failed", K(ret), K(*select_stmt));
    } else if (!can_be) {
      /*do nothing*/
    } else if (OB_FAIL(inner_remove_stmt_group_by(select_stmt, trans_happened))) {
      LOG_WARN("do transform remove group by failed", K(ret), K(*select_stmt));
    } else {
      /*do nothing*/
    }
  }
  return ret;
}

int ObTransformSimplify::check_stmt_group_by_can_be_removed(ObSelectStmt* select_stmt, bool& can_be)
{
  int ret = OB_SUCCESS;
  can_be = false;
  bool has_rownum = false;
  bool is_unique = false;
  if (OB_ISNULL(select_stmt) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->schema_checker_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL pointer error", K(ret), K(ctx_), K(ctx_->schema_checker_));
  } else if (OB_FAIL(select_stmt->has_rownum(has_rownum))) {
    LOG_WARN("check has_rownum error", K(ret));
  } else if (has_rownum && select_stmt->has_having()) {
    /*do nothing */
  } else if (OB_FAIL(ObTransformUtils::check_stmt_unique(select_stmt,
                 ctx_->session_info_,
                 ctx_->schema_checker_,
                 select_stmt->get_group_exprs(),
                 true /* strict */,
                 is_unique,
                 FLAGS_IGNORE_DISTINCT | FLAGS_IGNORE_GROUP))) {
    LOG_WARN("failed to check stmt unique", K(ret));
  } else if (is_unique) {
    can_be = true;
    for (int64_t i = 0; can_be && i < select_stmt->get_aggr_item_size(); ++i) {
      ObRawExpr* expr = static_cast<ObRawExpr*>(select_stmt->get_aggr_item(i));
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL pointer error", K(ret));
      } else if (OB_FAIL(check_aggr_win_can_be_removed(select_stmt, expr, can_be))) {
        LOG_WARN("fialed to check aggr can be removed", K(ret), K(*expr));
      }
    }
  } else {
    // do nothing
  }
  return ret;
}

int ObTransformSimplify::inner_remove_stmt_group_by(ObSelectStmt* select_stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(select_stmt) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_) || OB_ISNULL(ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL pointer error", K(ret), K(select_stmt), K(ctx_), K(ctx_->expr_factory_), K(ctx_->session_info_));
  } else {
    ObArray<ObRawExpr*> new_exprs;
    ObArray<ObRawExpr*> old_exprs;
    for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt->get_aggr_item_size(); ++i) {
      ObRawExpr* new_expr = NULL;
      ObRawExpr* cast_expr = NULL;
      ObRawExpr* expr = static_cast<ObRawExpr*>(select_stmt->get_aggr_item(i));
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL pointer error", K(ret));
      } else if (OB_FAIL(old_exprs.push_back(expr))) {
        LOG_WARN("old exprs push back failed", K(ret));
      } else if (OB_FAIL(transform_aggr_win_to_common_expr(select_stmt, expr, new_expr))) {
        LOG_WARN("transform aggr to common expr failed", K(ret), K(expr));
      }
      if (OB_SUCC(ret)) {
        if (OB_ISNULL(new_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret), K(new_expr));
        } else if (OB_FAIL(new_exprs.push_back(new_expr))) {
          LOG_WARN("new exprs push back failed", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(select_stmt->replace_inner_stmt_expr(old_exprs, new_exprs))) {
        LOG_WARN("select_stmt replace inner stmt expr failed", K(ret), K(select_stmt));
      } else if (OB_FAIL(append(select_stmt->get_condition_exprs(), select_stmt->get_having_exprs()))) {
        LOG_WARN("failed append having exprs to condition exprs", K(ret));
      } else {
        select_stmt->get_having_exprs().reset();
        select_stmt->get_aggr_items().reset();
        select_stmt->get_group_exprs().reset();
        trans_happened = true;
      }
    }
  }
  return ret;
}

// remove duplicate group by expr
// select * from t1 group by c1,c1,c1
//==>
// select * from t1 group by c1
int ObTransformSimplify::remove_group_by_duplicates(ObDMLStmt*& stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  ObSelectStmt* select_stmt = NULL;
  trans_happened = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null pointer", K(ret));
  } else if (!stmt->is_select_stmt()) {
    // do nothing
  } else if (FALSE_IT(select_stmt = static_cast<ObSelectStmt*>(stmt))) {
    /* do nothing */
  } else if (select_stmt->get_group_expr_size() > 0 && !select_stmt->has_rollup()) {
    ObArray<ObRawExpr*> new_group_exprs;
    ObIArray<ObRawExpr*>& group_exprs = select_stmt->get_group_exprs();
    for (int64_t i = 0; OB_SUCC(ret) && i < group_exprs.count(); ++i) {
      if (ObOptimizerUtil::find_equal_expr(new_group_exprs, group_exprs.at(i))) {
        /*do nothing*/
      } else if (OB_FAIL(new_group_exprs.push_back(group_exprs.at(i)))) {
        LOG_WARN("new group exprs push back failed", K(ret), K(group_exprs.at(i)));
      } else {
        /*do nothing*/
      }
    }
    if (OB_SUCC(ret)) {
      select_stmt->get_group_exprs().reset();
      if (OB_FAIL(select_stmt->get_group_exprs().assign(new_group_exprs))) {
        LOG_WARN("failed to assign a new group exprs", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformSimplify::exist_item_by_expr(ObRawExpr* expr, ObIArray<OrderItem>& order_items, bool& is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = false;
  int64_t N = order_items.count();
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("current order item expr is NULL", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < N && !is_exist; ++i) {
      OrderItem& this_item = order_items.at(i);
      if (OB_ISNULL(this_item.expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("order item expr is NULL", K(ret));
      } else if (this_item.expr_ == expr) {
        is_exist = true;
      } else if (this_item.expr_->same_as(*expr)) {
        is_exist = true;
      }
    }
  }
  return ret;
}

// for select
int ObTransformSimplify::remove_distinct_before_const(ObDMLStmt* stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is unexpected null", K(ret), K(stmt), K(ctx_));
  } else if (!stmt->is_select_stmt()) {
    // do nothing
  } else {
    bool can_be_eliminated = false;
    if (OB_FAIL(distinct_can_be_eliminated(stmt, can_be_eliminated))) {
      LOG_WARN("distinct_can_be_eliminated() fails unexpectedly", K(ret));
    } else if (can_be_eliminated) {
      // Eliminate DISTINCT and create a `LIMIT 1`
      ObConstRawExpr* limit_count_expr = NULL;
      std::pair<int64_t, ObRawExpr*> init_expr;
      if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*ctx_->expr_factory_, ObIntType, 1, limit_count_expr))) {
        LOG_WARN("Failed to create expr", K(ret));
      } else if (OB_ISNULL(limit_count_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("limit_count_expr is null");
      } else if (OB_FAIL(limit_count_expr->formalize(ctx_->session_info_))) {
        LOG_WARN("failed to formalize a new expr", K(ret));
      } else {
        stmt->set_limit_offset(limit_count_expr, NULL);
        static_cast<ObSelectStmt*>(stmt)->assign_all();
        trans_happened = true;
      }
    } else { /* Do nothing */
    }
  }

  return ret;
}

int ObTransformSimplify::remove_stmt_distinct(ObDMLStmt* stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  ObSelectStmt* select_stmt = NULL;
  trans_happened = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(stmt), K(ctx_));
  } else if (!stmt->is_select_stmt()) {
    // do nothing
  } else if (FALSE_IT(select_stmt = static_cast<ObSelectStmt*>(stmt))) {
    /* Do nothing*/
  } else if (select_stmt->has_distinct()) {
    bool is_unique = false;
    ObSEArray<ObRawExpr*, 16> select_exprs;
    if (OB_FAIL(select_stmt->get_select_exprs(select_exprs))) {
      LOG_WARN("failed to get select exprs", K(ret));
    } else if (OB_FAIL(ObTransformUtils::check_stmt_unique(select_stmt,
                   ctx_->session_info_,
                   ctx_->schema_checker_,
                   select_exprs,
                   true /* strict */,
                   is_unique,
                   true))) {
      LOG_WARN("failed to check stmt unique", K(ret));
    } else if (is_unique) {
      select_stmt->assign_all();
      trans_happened = true;
    }
  } else if (select_stmt->is_set_stmt() && OB_FAIL(remove_child_stmt_distinct(select_stmt, trans_happened))) {
    LOG_WARN("failed to remove child stmt distinct", K(ret));
  } else {
    /*do nothing*/
  }
  return ret;
}

int ObTransformSimplify::remove_child_stmt_distinct(ObSelectStmt* set_stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  if (OB_ISNULL(set_stmt) || !set_stmt->is_set_stmt()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected stmt", K(ret));
  } else if (!set_stmt->is_set_distinct() || ObSelectStmt::RECURSIVE == set_stmt->get_set_op()) {
    /*do nothing*/
  } else {
    bool child_happended = false;
    ObIArray<ObSelectStmt*>& child_stmts = set_stmt->get_set_query();
    for (int64_t i = 0; OB_SUCC(ret) && i < child_stmts.count(); ++i) {
      if (OB_FAIL(try_remove_child_stmt_distinct(child_stmts.at(i), child_happended))) {
        LOG_WARN("failed to try remove child stmt distinct", K(ret));
      } else {
        trans_happened |= child_happended;
      }
    }
  }
  return ret;
}

int ObTransformSimplify::try_remove_child_stmt_distinct(ObSelectStmt* stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(stmt));
  } else if (!stmt->is_set_stmt()) {
    bool has_rownum = false;
    if (!stmt->has_distinct() || stmt->has_sequence() || stmt->has_limit()) {
      /*do nothing*/
    } else if (OB_FAIL(stmt->has_rownum(has_rownum))) {
      LOG_WARN("failed to check has rownum", K(ret));
    } else if (!has_rownum) {
      stmt->assign_all();
      trans_happened = true;
    }
  } else if (ObSelectStmt::RECURSIVE == stmt->get_set_op() || stmt->has_limit()) {
    /*do nothing*/
  } else {
    bool child_happended = false;
    ObIArray<ObSelectStmt*>& child_stmts = stmt->get_set_query();
    for (int64_t i = 0; OB_SUCC(ret) && i < child_stmts.count(); ++i) {
      if (OB_FAIL(SMART_CALL(try_remove_child_stmt_distinct(child_stmts.at(i), child_happended)))) {
        LOG_WARN("failed to try remove child stmt distinct", K(ret));
      } else {
        trans_happened |= child_happended;
      }
    }
  }
  return ret;
}

int ObTransformSimplify::distinct_can_be_eliminated(ObDMLStmt* stmt, bool& can_be)
{
  int ret = OB_SUCCESS;
  can_be = false;
  ObSelectStmt* select_stmt = NULL;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(stmt), K(ctx_));
  } else if (!stmt->is_select_stmt() || stmt->is_contains_assignment() || stmt->is_calc_found_rows()) {
    // Do nothing for non-select query.
    // When there are `@var := ` assignment, don't eliminate distinct.
  } else if (FALSE_IT(select_stmt = static_cast<ObSelectStmt*>(stmt))) {
    /* Do nothing */
  } else if (select_stmt->has_distinct() && !select_stmt->has_set_op() && select_stmt->get_from_item_size() > 0) {
    // Only try to eliminate DISTINCT for plain SELECT
    int64_t limit_count = 0;
    const ObConstRawExpr* limit_expr = static_cast<ObConstRawExpr*>(select_stmt->get_limit_expr());
    const ObRawExpr* limit_offset_expr = select_stmt->get_offset_expr();
    if (limit_expr != NULL) {
      limit_count = limit_expr->get_value().get_int();
    }
    if (!select_stmt->has_limit() || (limit_offset_expr == NULL && limit_count > 0)) {
      bool contain_only = true;
      EqualSets& equal_sets = ctx_->equal_sets_;
      ObArenaAllocator alloc;
      ObSEArray<ObRawExpr*, 4> const_exprs;
      const ObIArray<ObRawExpr*>& conditions = select_stmt->get_condition_exprs();
      if (OB_FAIL(select_stmt->get_stmt_equal_sets(equal_sets, alloc, true, EQUAL_SET_SCOPE::SCOPE_ALL))) {
        LOG_WARN("failed to get stmt equal sets", K(ret));
      } else if (OB_FAIL(ObOptimizerUtil::compute_const_exprs(conditions, const_exprs))) {
        LOG_WARN("failed to compute const equivalent exprs", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && contain_only && i < select_stmt->get_select_item_size(); ++i) {
        ObRawExpr* expr = select_stmt->get_select_item(i).expr_;
        bool is_const = false;
        if (OB_FAIL(ObOptimizerUtil::is_const_expr(expr, equal_sets, const_exprs, is_const))) {
          LOG_WARN("check expr whether const expr failed", K(ret));
        } else {
          contain_only = is_const;
        }
      }
      equal_sets.reuse();
      if (OB_SUCC(ret) && contain_only) {
        can_be = true;
      } else { /* Do nothing */
      }
    } else { /* Do nothing */
    }
  } else { /* Do nothing */
  }
  return ret;
}

int ObTransformSimplify::convert_preds_vector_to_scalar(ObDMLStmt* stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 16> new_cond;
  bool is_happened = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL stmt", K(ret));
  } else if (!stmt->is_sel_del_upd()) {
    // do nothing
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_condition_size(); i++) {
      if (OB_FAIL(inner_convert_preds_vector_to_scalar(stmt->get_condition_expr(i), new_cond, is_happened))) {
        LOG_WARN("failed to convert predicate vector to scalar", K(ret));
      }
    }
    if (OB_SUCC(ret) && is_happened) {
      trans_happened = true;
      stmt->get_condition_exprs().reset();
      if (OB_FAIL(append(stmt->get_condition_exprs(), new_cond))) {
        LOG_WARN("failed to append join conditions", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_joined_tables().count(); i++) {
        if (OB_FAIL(convert_join_preds_vector_to_scalar(stmt->get_joined_tables().at(i), trans_happened))) {
          LOG_WARN("failed to convert join preds vector to scalar", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTransformSimplify::convert_join_preds_vector_to_scalar(TableItem* table_item, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL table item", K(ret));
  } else if (table_item->is_joined_table()) {
    JoinedTable* joined_table = reinterpret_cast<JoinedTable*>(table_item);
    ObSEArray<ObRawExpr*, 16> new_join_cond;
    bool is_happened = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < joined_table->get_join_conditions().count(); i++) {
      if (OB_FAIL(inner_convert_preds_vector_to_scalar(
              joined_table->get_join_conditions().at(i), new_join_cond, is_happened))) {
        LOG_WARN("failed to convert predicate vector to scalar", K(ret));
      }
    }
    if (OB_SUCC(ret) && is_happened) {
      trans_happened = true;
      joined_table->get_join_conditions().reset();
      if (OB_FAIL(append(joined_table->get_join_conditions(), new_join_cond))) {
        LOG_WARN("failed to append join conditions", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformSimplify::inner_convert_preds_vector_to_scalar(
    ObRawExpr* expr, ObIArray<ObRawExpr*>& exprs, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  bool need_push = true;
  ObRawExprFactory* factory = NULL;
  ObSQLSessionInfo* session = NULL;
  bool is_stack_overflow = false;
  if (OB_ISNULL(expr) || OB_ISNULL(ctx_) || OB_ISNULL(factory = ctx_->expr_factory_) ||
      OB_ISNULL(session = ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL param", K(ret), K(expr), K_(ctx), K(factory), K(session));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_stack_overflow));
  } else if ((expr->get_expr_type() == T_OP_EQ) || (expr->get_expr_type() == T_OP_NSEQ)) {
    ObOpRawExpr* op_expr = reinterpret_cast<ObOpRawExpr*>(expr);
    ObRawExpr* param_expr1 = expr->get_param_expr(0);
    ObRawExpr* param_expr2 = expr->get_param_expr(1);

    if (OB_UNLIKELY(2 != op_expr->get_param_count()) || OB_ISNULL(param_expr1) || OB_ISNULL(param_expr2)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("param expr is wrong", K(op_expr->get_param_count()), K(param_expr1), K(param_expr2));
    } else if (T_OP_ROW == param_expr1->get_expr_type() && T_OP_ROW == param_expr2->get_expr_type()) {
      need_push = false;
      trans_happened = true;
      if (OB_UNLIKELY(!is_oracle_mode() && param_expr1->get_param_count() != param_expr2->get_param_count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN(
            "param number not equal", K(ret), K(param_expr1->get_param_count()), K(param_expr2->get_param_count()));

      } else if (OB_UNLIKELY(is_oracle_mode() && 1 > param_expr2->get_param_count() &&
                             param_expr1->get_param_count() != param_expr2->get_param_expr(0)->get_param_count() &&
                             param_expr1->get_param_count() != param_expr2->get_param_count())) {
        ret = OB_ERR_INVALID_COLUMN_NUM;
        LOG_WARN("invalid relational operator on oracle mode", K(ret), K(param_expr2->get_param_count()));
      } else {
        if (is_oracle_mode() && T_OP_ROW == param_expr2->get_param_expr(0)->get_expr_type()) {
          param_expr2 = param_expr2->get_param_expr(0);
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < param_expr1->get_param_count(); i++) {
          ObOpRawExpr* new_op_expr = NULL;
          if (OB_FAIL(factory->create_raw_expr(expr->get_expr_type(), new_op_expr))) {
            LOG_WARN("failed to create raw expr", K(ret));
          } else if (OB_ISNULL(new_op_expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("NULL new op expr", K(ret));
          } else if (OB_FAIL(new_op_expr->set_param_exprs(
                         param_expr1->get_param_expr(i), param_expr2->get_param_expr(i)))) {
            LOG_WARN("failed to set param expr", K(ret));
          } else if (OB_FAIL(new_op_expr->formalize(session))) {
            LOG_WARN("failed to formalize expr", K(ret));
          } else if (OB_FAIL(SMART_CALL(inner_convert_preds_vector_to_scalar(
                         reinterpret_cast<ObRawExpr*>(new_op_expr), exprs, trans_happened)))) {
            LOG_WARN("failed to call inner convert recursive", K(ret));
          }
        }
      }
    }
  }
  if (OB_SUCC(ret) && need_push && OB_FAIL(exprs.push_back(expr))) {
    LOG_WARN("failed to push back expr", K(ret));
  }
  return ret;
}

int ObTransformSimplify::transform_subquery_as_expr(ObDMLStmt* stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  bool is_happened = false;
  ObSEArray<ObRawExprPointer, 16> relation_expr_pointers;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret), K(stmt));
  } else if (OB_FAIL(stmt->get_relation_exprs(relation_expr_pointers))) {
    LOG_WARN("failed to get_relation_exprs", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < relation_expr_pointers.count(); ++i) {
    ObRawExpr* expr = NULL;
    if (OB_FAIL(relation_expr_pointers.at(i).get(expr))) {
      LOG_WARN("failed to get relation expr", K(ret));
    } else if (OB_FAIL(try_trans_subquery_in_expr(stmt, expr, is_happened))) {
      LOG_WARN("failed to transform expr", K(ret));
    } else {
      trans_happened |= is_happened;
      if (OB_FAIL(relation_expr_pointers.at(i).set(expr))) {
        LOG_WARN("failed to set relation expr pointer", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformSimplify::try_trans_subquery_in_expr(ObDMLStmt* stmt, ObRawExpr*& expr, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  bool is_happened = false;
  trans_happened = false;
  bool is_stack_overflow = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(expr) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parameters have null", K(stmt), K(expr), K(ctx_));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_stack_overflow));
  } else if (IS_SUBQUERY_COMPARISON_OP(expr->get_expr_type()) || T_OP_EXISTS == expr->get_expr_type() ||
             T_OP_NOT_EXISTS == expr->get_expr_type()) {
    // do nothing
  } else if (expr->is_query_ref_expr()) {
    if (OB_FAIL(do_trans_subquery_as_expr(stmt, expr, is_happened))) {
      LOG_WARN("failed to do_trans_subquery_as_expr", K(ret));
    } else if (is_happened) {
      trans_happened = true;
    }
  } else if (expr->has_flag(CNT_SUB_QUERY)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(try_trans_subquery_in_expr(stmt, expr->get_param_expr(i), is_happened)))) {
        LOG_WARN("failed to trans param expr", K(ret));
      } else if (is_happened) {
        trans_happened = true;
      }
    }
    if (OB_SUCC(ret) && trans_happened) {
      if (OB_FAIL(expr->formalize(ctx_->session_info_))) {
        LOG_WARN("failed to formalize expr", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformSimplify::do_trans_subquery_as_expr(ObDMLStmt* stmt, ObRawExpr*& expr, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  bool is_expr = false;
  trans_happened = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parameters have null", K(ret), K(stmt), K(expr));
  } else if (expr->is_query_ref_expr()) {
    ObQueryRefRawExpr* query_ref = static_cast<ObQueryRefRawExpr*>(expr);
    const ObSelectStmt* sub_stmt = query_ref->get_ref_stmt();
    ObRawExpr* sub_expr = NULL;
    if (query_ref->is_cursor()) {
      /*do nothing*/
    } else if (OB_FAIL(ObTransformUtils::is_expr_query(sub_stmt, is_expr))) {
      LOG_WARN("fail to check is_expr_query", K(ret));
    } else if (!is_expr) {
      /*do nothing*/
    } else if (OB_UNLIKELY(1 != sub_stmt->get_select_item_size()) ||
               OB_ISNULL(sub_expr = sub_stmt->get_select_item(0).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sub stmt has invalid select item", K(ret), K(sub_stmt->get_select_item_size()), K(sub_expr));
    } else if (sub_expr->has_flag(CNT_ROWNUM)) {
      /*do nothing*/
    } else if (OB_FAIL(ObOptimizerUtil::remove_item(stmt->get_subquery_exprs(), query_ref))) {
      LOG_WARN("failed to remove child stmt", K(ret));
    } else {
      expr = sub_expr;
      trans_happened = true;
    }
  }
  return ret;
}

/**
 * Remove dummy filters
 * 1 false or filter -> filter
 * 2 true or filter -> true
 * 3 false and filter -> false
 * 4 true and filter -> filter
 */
int ObTransformSimplify::remove_dummy_exprs(ObDMLStmt* stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  bool is_happened = false;
  trans_happened = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null stmt", K(ret));
  } else {
    // remove dummy exprs in where clause
    if (OB_FAIL(remove_dummy_filter_exprs(stmt->get_condition_exprs(), is_happened))) {
      LOG_WARN("failed to post process filter exprs", K(ret));
    } else {
      trans_happened |= is_happened;
    }
    // remove dummy exprs in joined tables conditions
    ObIArray<JoinedTable*>& joined_table = stmt->get_joined_tables();
    for (int64_t i = 0; OB_SUCC(ret) && i < joined_table.count(); i++) {
      if (OB_ISNULL(joined_table.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null joined table", K(joined_table.at(i)), K(ret));
      } else if (OB_FAIL(remove_dummy_join_condition_exprs(*joined_table.at(i), is_happened))) {
        LOG_WARN("failed to post process join condition exprs", K(ret));
      } else {
        trans_happened |= is_happened;
      }
    }
    // remove dummy exprs in having clause
    if (OB_SUCC(ret) && stmt->is_select_stmt()) {
      ObSelectStmt* select_stmt = static_cast<ObSelectStmt*>(stmt);
      if (OB_FAIL(remove_dummy_filter_exprs(select_stmt->get_having_exprs(), is_happened))) {
        LOG_WARN("failed to post process filter exprs", K(ret));
      } else {
        trans_happened |= is_happened;
      }
    }
    // remove dummy case when
    if (OB_SUCC(ret)) {
      if (OB_FAIL(remove_dummy_case_when(stmt, is_happened))) {
        LOG_WARN("failed to remvoe dummy case when", K(ret));
      } else {
        trans_happened |= is_happened;
      }
    }
  }
  return ret;
}

int ObTransformSimplify::remove_dummy_filter_exprs(common::ObIArray<ObRawExpr*>& exprs, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  bool is_happened = false;
  bool is_valid_type = true;
  trans_happened = false;
  for (int64_t i = 0; OB_SUCC(ret) && is_valid_type && i < exprs.count(); i++) {
    ObRawExpr* temp = NULL;
    if (OB_ISNULL(temp = exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null expr", K(ret));
    } else {
      is_valid_type &= temp->get_result_type().is_integer_type();
    }
  }
  if (OB_FAIL(ret)) {
    /*do nothing*/
  } else if (!is_valid_type) {
    LOG_TRACE("expr list is not valid for removing dummy exprs", K(is_valid_type));
  } else {
    LOG_TRACE("expr list is valid for removing dummy exprs", K(is_valid_type));
    for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
      ObRawExpr* temp = NULL;
      if (OB_ISNULL(temp = exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null expr", K(ret));
      } else if (OB_FAIL(inner_remove_dummy_expr(temp, is_happened))) {
        LOG_WARN("failed to remove dummy filter", K(ret));
      } else if (OB_ISNULL(temp)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null expr", K(ret));
      } else {
        exprs.at(i) = temp;
        trans_happened |= is_happened;
      }
    }
    if (OB_SUCC(ret)) {
      ObSEArray<int64_t, 2> true_exprs;
      ObSEArray<int64_t, 2> false_exprs;
      if (OB_FAIL(ObTransformUtils::flatten_expr(exprs))) {
        LOG_WARN("failed to flatten exprs", K(ret));
      } else if (exprs.count() <= 1) {
        /*do nothing*/
      } else if (OB_FAIL(extract_dummy_expr_info(exprs, true_exprs, false_exprs))) {
        LOG_WARN("failed to extract exprs info", K(ret));
      } else if (true_exprs.empty() && false_exprs.empty()) {
        /*do nothing*/
      } else if (OB_FAIL(adjust_dummy_expr(false_exprs, true_exprs, exprs))) {
        LOG_WARN("failed to adjust dummy exprs", K(ret));
      } else {
        trans_happened |= true;
      }
    }
  }
  return ret;
}

int ObTransformSimplify::remove_dummy_join_condition_exprs(JoinedTable& join_table, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  bool is_happened = false;
  trans_happened = false;
  bool is_stack_overflow = false;
  if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_stack_overflow));
  } else if (OB_FAIL(remove_dummy_filter_exprs(join_table.join_conditions_, is_happened))) {
    LOG_WARN("failed to remove dummy exprs", K(ret));
  } else if (FALSE_IT(trans_happened = is_happened)) {
    /*do nothing*/
  } else {
    if (NULL != join_table.left_table_ && join_table.left_table_->is_joined_table()) {
      JoinedTable& left_table = static_cast<JoinedTable&>(*join_table.left_table_);
      if (OB_FAIL(SMART_CALL(remove_dummy_join_condition_exprs(left_table, is_happened)))) {
        LOG_WARN("failed to remove dummy join conditions", K(ret));
      } else {
        trans_happened |= is_happened;
      }
    }
    if (OB_SUCC(ret) && join_table.right_table_ != NULL && join_table.right_table_->is_joined_table()) {
      JoinedTable& right_table = static_cast<JoinedTable&>(*join_table.right_table_);
      if (OB_FAIL(SMART_CALL(remove_dummy_join_condition_exprs(right_table, is_happened)))) {
        LOG_WARN("failed to remove dummy join conditions", K(ret));
      } else {
        trans_happened |= is_happened;
      }
    }
  }
  return ret;
}

int ObTransformSimplify::inner_remove_dummy_expr(ObRawExpr*& expr, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  bool is_happened = false;
  bool is_valid_type = false;
  bool is_stack_overflow = false;
  trans_happened = false;
  if (OB_ISNULL(expr) || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ctx_), K(ret));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_stack_overflow));
  } else if (OB_FAIL(is_valid_transform_type(expr, is_valid_type))) {
    LOG_WARN("failed to check is valid transform type", K(ret));
  } else if (!is_valid_type) {
    LOG_TRACE("expr is not valid for removing dummy exprs", K(*expr));
  } else {
    // remove dummy filter for children exprs first
    LOG_TRACE("expr is valid for removing dummy exprs", K(*expr));
    ObOpRawExpr* op_expr = static_cast<ObOpRawExpr*>(expr);
    for (int64_t i = 0; OB_SUCC(ret) && i < op_expr->get_param_count(); i++) {
      ObRawExpr* temp = NULL;
      if (OB_ISNULL(temp = op_expr->get_param_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null expr", K(ret));
      } else if (OB_FAIL(SMART_CALL(inner_remove_dummy_expr(temp, is_happened)))) {
        LOG_WARN("failed to remove dummy filter", K(ret));
      } else if (OB_ISNULL(temp)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null expr", K(ret));
      } else {
        op_expr->get_param_expr(i) = temp;
        trans_happened |= is_happened;
      }
    }
    if (OB_SUCC(ret) && op_expr->get_param_count() > 1) {
      ObSEArray<int64_t, 2> true_exprs;
      ObSEArray<int64_t, 2> false_exprs;
      ObIArray<int64_t>* remove_exprs = NULL;
      ObIArray<int64_t>* return_exprs = NULL;
      if (OB_FAIL(extract_dummy_expr_info(op_expr->get_param_exprs(), true_exprs, false_exprs))) {
        LOG_WARN("failed to extract filters info", K(ret));
      } else if (T_OP_AND == expr->get_expr_type()) {
        remove_exprs = &true_exprs;
        return_exprs = &false_exprs;
      } else if (T_OP_OR == expr->get_expr_type()) {
        remove_exprs = &false_exprs;
        return_exprs = &true_exprs;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected expr type", K(expr->get_expr_type()), K(ret));
      }
      if (OB_SUCC(ret) && OB_NOT_NULL(remove_exprs) && OB_NOT_NULL(return_exprs) &&
          (!remove_exprs->empty() || !return_exprs->empty())) {
        if (OB_FAIL(adjust_dummy_expr(*return_exprs, *remove_exprs, op_expr->get_param_exprs()))) {
          LOG_WARN("failed to adjust dummy filter", K(ret));
        } else if (OB_UNLIKELY(op_expr->get_param_count() < 1)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("should have at least one param", K(op_expr->get_param_count()), K(ret));
        } else if (FALSE_IT(trans_happened = true)) {
          /*do nothing*/
        } else if (1 == op_expr->get_param_count()) {
          expr = op_expr->get_param_expr(0);
        } else { /*do nothing*/
        }
      }
    }
    if (OB_SUCC(ret) && trans_happened) {
      if (OB_FAIL(expr->formalize(ctx_->session_info_))) {
        LOG_WARN("failed to formalize expr", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObTransformSimplify::is_valid_transform_type(ObRawExpr* expr, bool& is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null expr", K(expr), K(ret));
  } else if (T_OP_AND == expr->get_expr_type() || T_OP_OR == expr->get_expr_type()) {
    is_valid = expr->get_result_type().is_integer_type();
    for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < expr->get_param_count(); i++) {
      ObRawExpr* temp = NULL;
      if (OB_ISNULL(temp = expr->get_param_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null expr", K(ret));
      } else {
        is_valid &= temp->get_result_type().is_integer_type();
      }
    }
  } else { /*do nothing*/
  }
  return ret;
}

int ObTransformSimplify::adjust_dummy_expr(const common::ObIArray<int64_t>& return_exprs,
    const common::ObIArray<int64_t>& remove_exprs, common::ObIArray<ObRawExpr*>& adjust_exprs)
{
  int ret = OB_SUCCESS;
  if (!return_exprs.empty()) {
    if (return_exprs.at(0) < 0 || return_exprs.at(0) >= adjust_exprs.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid array pos", K(return_exprs.at(0)), K(adjust_exprs.count()), K(ret));
    } else {
      ObRawExpr* temp = NULL;
      if (OB_ISNULL(temp = adjust_exprs.at(return_exprs.at(0)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null expr", K(ret));
      } else if (FALSE_IT(adjust_exprs.reset())) {
        /*do nothing*/
      } else if (OB_FAIL(adjust_exprs.push_back(temp))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else { /*do nothing*/
      }
    }
  } else {
    for (int64_t i = remove_exprs.count() - 1; OB_SUCC(ret) && i >= 0; i--) {
      if (remove_exprs.at(i) < 0 || remove_exprs.at(i) >= adjust_exprs.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid array pos", K(remove_exprs.at(i)), K(adjust_exprs.count()), K(ret));
      } else if (1 == adjust_exprs.count()) {
        /*do nothing*/
      } else if (OB_FAIL(adjust_exprs.remove(remove_exprs.at(i)))) {
        LOG_WARN("failed to remove expr", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObTransformSimplify::extract_dummy_expr_info(const common::ObIArray<ObRawExpr*>& exprs,
    common::ObIArray<int64_t>& true_exprs, common::ObIArray<int64_t>& false_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->allocator_) || OB_ISNULL(ctx_->exec_ctx_) ||
      OB_ISNULL(ctx_->exec_ctx_->get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ctx_), K(ret));
  } else {
    ObObj result;
    bool is_true = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
      ObRawExpr* temp = NULL;
      if (OB_ISNULL(temp = exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null expr", K(ret));
      } else if (!temp->is_const_expr()) {
        /*do nothing*/
      } else if (OB_FAIL(ObSQLUtils::calc_const_expr(
                     temp, &ctx_->exec_ctx_->get_physical_plan_ctx()->get_param_store(), result, is_true))) {
        LOG_WARN("failed to compute const expr", K(ret), K(*temp));
      } else if ((!is_true) || (!result.is_integer_type())) {
        // do nothing if plan cache does not check the bool value, or the result is not integer type
        LOG_TRACE("plan cache does not check this bool value, ignore it", K(is_true), K(result.get_type()));
      } else if (OB_FAIL(ObObjEvaluator::is_true(result, is_true))) {
        LOG_WARN("failed to get bool value", K(ret));
      } else if (!is_true && OB_FAIL(false_exprs.push_back(i))) {
        LOG_WARN("failed to push back into array", K(ret));
      } else if (is_true && OB_FAIL(true_exprs.push_back(i))) {
        LOG_WARN("failed to push back into array", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObTransformSimplify::simplify_win_exprs(ObDMLStmt* stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  bool is_happened = false;
  trans_happened = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (stmt->is_select_stmt()) {
    ObSelectStmt* sel_stmt = static_cast<ObSelectStmt*>(stmt);
    for (int64_t i = 0; OB_SUCC(ret) && i < sel_stmt->get_window_func_count(); ++i) {
      if (OB_FAIL(simplify_win_expr(sel_stmt->get_window_func_expr(i), is_happened))) {
        LOG_WARN("failed to simplify win expr", K(ret));
      } else {
        trans_happened |= is_happened;
      }
    }
  }
  return ret;
}

int ObTransformSimplify::simplify_win_expr(ObRawExpr* expr, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (expr->is_win_func_expr()) {
    ObWinFunRawExpr* win_expr = static_cast<ObWinFunRawExpr*>(expr);
    ObIArray<ObRawExpr*>& partition_exprs = win_expr->get_partition_exprs();
    ObIArray<OrderItem>& order_items = win_expr->get_order_items();
    ObSEArray<ObRawExpr*, 4> new_partition_exprs;
    ObSEArray<OrderItem, 4> new_order_items;
    common::hash::ObPlacementHashSet<ObRawExpr*, 32> expr_set;

    for (int64_t i = 0; OB_SUCC(ret) && i < partition_exprs.count(); ++i) {
      ObRawExpr* part_expr = partition_exprs.at(i);
      int hash_ret = expr_set.exist_refactored(part_expr);
      if (OB_HASH_NOT_EXIST == hash_ret) {
        if (OB_FAIL(expr_set.set_refactored(part_expr))) {
          LOG_WARN("failed to add expr into hash set", K(ret));
        } else if (OB_FAIL(new_partition_exprs.push_back(part_expr))) {
          LOG_WARN("failed to push back expr", K(ret));
        }
      } else if (OB_HASH_EXIST != hash_ret) {
        ret = hash_ret;
        LOG_WARN("failed to get expr from hash set", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < order_items.count(); ++i) {
      ObRawExpr* order_expr = order_items.at(i).expr_;
      int hash_ret = expr_set.exist_refactored(order_expr);
      if (OB_HASH_NOT_EXIST == hash_ret) {
        if (OB_FAIL(expr_set.set_refactored(order_expr))) {
          LOG_WARN("failed to add expr into hash set", K(ret));
        } else if (OB_FAIL(new_order_items.push_back(order_items.at(i)))) {
          LOG_WARN("failed to push back expr", K(ret));
        }
      } else if (OB_HASH_EXIST != hash_ret) {
        ret = hash_ret;
        LOG_WARN("failed to get expr from hash set", K(ret));
      }
    }
    if (OB_SUCC(ret) && new_order_items.count() == 0 && order_items.count() > 0) {
      // for computing range frame
      // at least one order item when executing
      if (OB_FAIL(new_order_items.push_back(order_items.at(0)))) {
        LOG_WARN("failed to push back order items", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      trans_happened =
          (new_partition_exprs.count() < partition_exprs.count() || new_order_items.count() < order_items.count());
      if (OB_FAIL(partition_exprs.assign(new_partition_exprs))) {
        LOG_WARN("failed to assign partition exprs", K(ret));
      } else if (OB_FAIL(order_items.assign(new_order_items))) {
        LOG_WARN("failed to assign order items", K(ret));
      }
    }
  }
  return ret;
}

/**
 *
 * @brief ObTransformSimplify::pushdown_limit_offset
 * select rownum rn, v.* from (select subquery1 from t) v offset 10 rows;
 * => select rownum + 10 rn, v.* from (select subquery1 from t offset 10 rows) v;
 */
int ObTransformSimplify::pushdown_limit_offset(ObDMLStmt* stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  ObSelectStmt* sel_stmt = NULL;
  ObSelectStmt* view_stmt = NULL;
  bool is_valid = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret), K(ctx_));
  } else if (!is_oracle_mode() || !stmt->is_select_stmt()) {
    /*do nothing*/
  } else if (FALSE_IT(sel_stmt = static_cast<ObSelectStmt*>(stmt))) {
  } else if (OB_FAIL(check_pushdown_limit_offset_validity(sel_stmt, view_stmt, is_valid))) {
    LOG_WARN("failed to check pushdown limit offset validity", K(ret));
  } else if (!is_valid) {
    /*do nothing*/
  } else if (OB_FAIL(do_pushdown_limit_offset(sel_stmt, view_stmt))) {
    LOG_WARN("failed to do pushdown limit offset", K(ret));
  } else {
    trans_happened = true;
  }
  return ret;
}

int ObTransformSimplify::check_pushdown_limit_offset_validity(
    ObSelectStmt* upper_stmt, ObSelectStmt*& view_stmt, bool& is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  view_stmt = NULL;
  TableItem* table = NULL;
  if (OB_ISNULL(upper_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret));
  } else if (!upper_stmt->is_single_table_stmt() || OB_ISNULL(table = upper_stmt->get_table_item(0)) ||
             !table->is_generated_table() || OB_ISNULL(view_stmt = table->ref_query_) ||
             view_stmt->is_hierarchical_query()) {
    is_valid = false;
  } else if (!upper_stmt->has_limit() || NULL == upper_stmt->get_offset_expr() ||
             NULL != upper_stmt->get_limit_percent_expr() || NULL != view_stmt->get_limit_percent_expr() ||
             upper_stmt->is_fetch_with_ties() || view_stmt->is_fetch_with_ties()) {
    is_valid = false;
  } else if (0 != upper_stmt->get_condition_size() || upper_stmt->has_group_by() || upper_stmt->has_rollup() ||
             upper_stmt->has_window_function() || upper_stmt->has_distinct() || upper_stmt->has_sequence() ||
             upper_stmt->has_order_by()) {
    is_valid = false;
  } else {
    is_valid = true;
  }
  return ret;
}

int ObTransformSimplify::do_pushdown_limit_offset(ObSelectStmt* upper_stmt, ObSelectStmt* view_stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(upper_stmt) || OB_ISNULL(view_stmt) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_) ||
      OB_ISNULL(ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else {
    ObRawExpr* view_limit = view_stmt->get_limit_expr();
    ObRawExpr* upper_limit = upper_stmt->get_limit_expr();
    ObRawExpr* view_offset = view_stmt->get_offset_expr();
    ObRawExpr* upper_offset = upper_stmt->get_offset_expr();
    ObRawExpr* limit_expr = NULL;
    ObRawExpr* offset_expr = NULL;
    if (OB_FAIL(ObTransformUtils::merge_limit_offset(
            ctx_, view_limit, upper_limit, view_offset, upper_offset, limit_expr, offset_expr))) {
      LOG_WARN("failed to merge limit offset", K(ret));
    } else {
      ObRawExpr* rownum_expr = NULL;
      ObOpRawExpr* add_expr = NULL;
      ObSEArray<ObRawExpr*, 1> old_expr;
      ObSEArray<ObRawExpr*, 1> new_expr;
      upper_stmt->set_limit_offset(upper_limit, NULL);
      upper_stmt->set_fetch_with_ties(false);
      upper_stmt->set_limit_percent_expr(NULL);
      upper_stmt->set_has_fetch(false);
      view_stmt->set_limit_offset(limit_expr, offset_expr);
      if (OB_FAIL(upper_stmt->get_rownum_expr(rownum_expr))) {
        LOG_WARN("failed to get rownum expr", K(ret));
      } else if (NULL == rownum_expr || NULL == upper_offset) {
        /*do nothing*/
      } else if (OB_FAIL(ctx_->expr_factory_->create_raw_expr(T_OP_ADD, add_expr))) {
        LOG_WARN("create add op expr failed", K(ret));
      } else if (OB_ISNULL(add_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("add expr is null");
      } else if (OB_FAIL(old_expr.push_back(rownum_expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else if (OB_FAIL(new_expr.push_back(add_expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else if (OB_FAIL(upper_stmt->replace_inner_stmt_expr(old_expr, new_expr))) {
        LOG_WARN("failed to replace expr", K(ret));
      } else if (OB_FAIL(add_expr->set_param_exprs(rownum_expr, upper_offset))) {
        LOG_WARN("set param exprs failed", K(ret));
      } else if (OB_FAIL(add_expr->formalize(ctx_->session_info_))) {
        LOG_WARN("formalize add operator failed", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformSimplify::remove_redundent_select(ObDMLStmt*& stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  bool is_valid = false;
  ObSelectStmt* new_stmt = NULL;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (stmt->is_select_stmt()) {
    ObSelectStmt* sel_stmt = static_cast<ObSelectStmt*>(stmt);
    if (OB_FAIL(try_remove_redundent_select(*sel_stmt, new_stmt))) {
      LOG_WARN("failed to check can remove simple select", K(ret));
    } else if (NULL != new_stmt) {
      stmt = new_stmt;
      trans_happened = true;
    }
  }
  return ret;
}

int ObTransformSimplify::try_remove_redundent_select(ObSelectStmt& stmt, ObSelectStmt*& new_stmt)
{
  int ret = OB_SUCCESS;
  new_stmt = NULL;
  ObRawExpr* sel_expr = NULL;
  ObQueryRefRawExpr* query_expr = NULL;
  ObSelectStmt* subquery = NULL;
  bool is_valid = false;
  if (!stmt.has_set_op() && !stmt.is_hierarchical_query() && 1 == stmt.get_select_item_size() &&
      0 == stmt.get_from_item_size() && 0 == stmt.get_condition_size() && 0 == stmt.get_aggr_item_size() &&
      0 == stmt.get_having_expr_size() && 0 == stmt.get_window_func_count() && !stmt.has_limit()) {
    if (OB_ISNULL(sel_expr = stmt.get_select_item(0).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null expr", K(ret));
    } else if (!sel_expr->is_query_ref_expr()) {
      // do nothing
    } else if (FALSE_IT(query_expr = static_cast<ObQueryRefRawExpr*>(sel_expr))) {
      // never reach
    } else if (OB_ISNULL(subquery = query_expr->get_ref_stmt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null stmt", K(ret));
    } else if (OB_FAIL(check_subquery_valid(*subquery, is_valid))) {
      LOG_WARN("failed to check subquery valid", K(ret));
    } else if (!is_valid) {
      // do nothing
    } else if (OB_FAIL(subquery->pullup_stmt_level())) {
      LOG_WARN("failed to pullup stmt level", K(ret));
    } else if (OB_FAIL(subquery->adjust_view_parent_namespace_stmt(stmt.get_parent_namespace_stmt()))) {
      LOG_WARN("failed to adjust view parent namespace stmt", K(ret));
    } else {
      new_stmt = subquery;
    }
  }
  return ret;
}

/**
 * @brief check_subquery_valid
 * check subquery return equal or less than one row
 * subquery should in format of:
 * 1. select ... from dual;
 * 2. select aggr() ...;  <- no group by
 * 3. select ... limit 0/1;
 * 4. select ... where rownum < 2;
 * case 3 and 4 is not ignored at the present
 */
int ObTransformSimplify::check_subquery_valid(ObSelectStmt& stmt, bool& is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  int64_t limit = -1;
  if (stmt.has_set_op() || stmt.is_hierarchical_query()) {
    // do nothing
  } else if (0 == stmt.get_from_item_size()) {
    is_valid = true;
  } else if (OB_FAIL(ObTransformUtils::get_stmt_limit_value(stmt, limit))) {
    LOG_WARN("failed to get stmt limit value", K(ret));
  } else if (0 == limit || 1 == limit) {
    is_valid = true;
  } else if (0 == stmt.get_group_expr_size()) {
    ObRawExpr* sel_expr = NULL;
    if (OB_UNLIKELY(1 != stmt.get_select_item_size()) || OB_ISNULL(sel_expr = stmt.get_select_item(0).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected subquery", K(ret), K(stmt.get_select_item_size()), K(sel_expr));
    } else if (sel_expr->has_flag(CNT_AGG)) {
      is_valid = true;
    }
  }
  return ret;
}

// try to remove distinct in aggr(distinct) and window_function(distinct)
int ObTransformSimplify::remove_aggr_distinct(ObDMLStmt* stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  ObAggFunRawExpr* aggr_expr = NULL;
  ObWinFunRawExpr* win_expr = NULL;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(stmt), K(ctx_));
  } else if (stmt->is_select_stmt()) {
    ObSelectStmt* select_stmt = static_cast<ObSelectStmt*>(stmt);
    // remove distinct in aggr
    for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt->get_aggr_item_size(); ++i) {
      if (OB_ISNULL(aggr_expr = select_stmt->get_aggr_item(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null aggr expr", K(ret));
      } else if (!aggr_expr->is_param_distinct()) {
        // do nothing
      } else if (T_FUN_MAX == aggr_expr->get_expr_type() || T_FUN_MIN == aggr_expr->get_expr_type()) {
        // max/min(distinct) remove distinct
        aggr_expr->set_param_distinct(false);
        trans_happened = true;
      } else if (T_FUN_SUM == aggr_expr->get_expr_type() || T_FUN_COUNT == aggr_expr->get_expr_type() ||
                 T_FUN_GROUP_CONCAT == aggr_expr->get_expr_type()) {
        // sum/count/group_concat(distinct) require param expr to be unique
        ObSEArray<ObRawExpr*, 4> aggr_param_exprs;
        bool is_unique = false;
        if (OB_FAIL(aggr_param_exprs.assign(aggr_expr->get_real_param_exprs()))) {
          LOG_WARN("failed to push back aggr param expr", K(ret));
        } else if (OB_FAIL(append(aggr_param_exprs, select_stmt->get_group_exprs()))) {
          LOG_WARN("failed to append group by expr", K(ret));
        } else if (OB_FAIL(ObTransformUtils::check_stmt_unique(select_stmt,
                       ctx_->session_info_,
                       ctx_->schema_checker_,
                       aggr_param_exprs,
                       false,
                       is_unique,
                       FLAGS_IGNORE_DISTINCT | FLAGS_IGNORE_GROUP))) {
          LOG_WARN("failed to check stmt unique", K(ret));
        } else if (is_unique) {
          aggr_expr->set_param_distinct(false);
          trans_happened = true;
        }
      } else {
        // do nothing
      }
    }
    if (OB_SUCC(ret) && trans_happened) {
      if (OB_FAIL(remove_aggr_duplicates(select_stmt))) {
        LOG_WARN("failed to remove aggr item duplicates", K(ret));
      } else { /*do nothing*/
      }
    }
    // remove distinct in window function
    bool win_happened = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt->get_window_func_count(); ++i) {
      if (OB_ISNULL(win_expr = select_stmt->get_window_func_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null window function expr", K(ret));
      } else if (NULL == (aggr_expr = win_expr->get_agg_expr()) || !aggr_expr->is_param_distinct()) {
        // do nothing if window function not has aggr expr(such as rownum)
        // or aggr expr not contain distinct
      } else if (T_FUN_MAX == aggr_expr->get_expr_type() || T_FUN_MIN == aggr_expr->get_expr_type()) {
        // max/min(distinct) remove distinct
        aggr_expr->set_param_distinct(false);
        trans_happened = true;
        win_happened = true;
      } else if (T_FUN_SUM == aggr_expr->get_expr_type() || T_FUN_COUNT == aggr_expr->get_expr_type()) {
        // sum/count(distinct) require param expr to be unique
        ObSEArray<ObRawExpr*, 4> aggr_param_exprs;
        bool is_unique = false;
        if (OB_FAIL(aggr_param_exprs.assign(aggr_expr->get_real_param_exprs()))) {
          LOG_WARN("failed to push back aggr param expr", K(ret));
        } else if (OB_FAIL(ObTransformUtils::check_stmt_unique(select_stmt,
                       ctx_->session_info_,
                       ctx_->schema_checker_,
                       aggr_param_exprs,
                       false,
                       is_unique,
                       FLAGS_IGNORE_DISTINCT))) {
          LOG_WARN("failed to check stmt unique", K(ret));
        } else if (is_unique) {
          aggr_expr->set_param_distinct(false);
          trans_happened = true;
          win_happened = true;
        }
      } else {
        // do nothing
      }
    }
    if (OB_SUCC(ret) && win_happened) {
      if (OB_FAIL(remove_win_func_duplicates(select_stmt))) {
        LOG_WARN("failed to remove win func duplicates", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObTransformSimplify::remove_aggr_duplicates(ObSelectStmt* select_stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(select_stmt));
  } else {
    ObSEArray<ObRawExpr*, 4> old_aggr_exprs;
    ObSEArray<ObRawExpr*, 4> new_aggr_exprs;
    ObSEArray<ObAggFunRawExpr*, 4> new_aggr_items;
    ObAggFunRawExpr* old_aggr_expr = NULL;
    ObAggFunRawExpr* new_aggr_expr = NULL;
    ObSqlBitSet<> removed_items;
    for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt->get_aggr_item_size(); ++i) {
      if (OB_ISNULL(new_aggr_expr = select_stmt->get_aggr_item(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null aggr expr", K(ret));
      } else if (removed_items.has_member(i)) {
        /*do nothing */
      } else if (OB_FAIL(new_aggr_items.push_back(new_aggr_expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else {
        for (int64_t j = i + 1; OB_SUCC(ret) && j < select_stmt->get_aggr_item_size(); ++j) {
          if (OB_ISNULL(old_aggr_expr = select_stmt->get_aggr_item(j))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get null aggr expr", K(ret));
          } else if (new_aggr_expr->same_as(*old_aggr_expr)) {
            if (OB_FAIL(removed_items.add_member(j))) {
              LOG_WARN("failed to add member", K(ret));
            } else if (OB_FAIL(new_aggr_exprs.push_back(new_aggr_expr))) {
              LOG_WARN("failed to push back expr", K(ret));
            } else if (OB_FAIL(old_aggr_exprs.push_back(old_aggr_expr))) {
              LOG_WARN("failed to push back expr", K(ret));
            } else { /*do nothing*/
            }
          } else { /*do nothing*/
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (!old_aggr_exprs.empty()) {
        if (OB_UNLIKELY(old_aggr_exprs.count() != new_aggr_exprs.count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("replace expr item size not equal", K(ret), K(old_aggr_exprs.count()), K(new_aggr_exprs.count()));
        } else if (OB_FAIL(select_stmt->replace_inner_stmt_expr(old_aggr_exprs, new_aggr_exprs))) {
          LOG_WARN("failed to replace inner stmt expr", K(ret));
        } else if (OB_FAIL(select_stmt->get_aggr_items().assign(new_aggr_items))) {
          LOG_WARN("failed to assign aggr items failed", K(ret));
        }
      } else { /*do nothing */
      }
    }
  }
  return ret;
}

int ObTransformSimplify::remove_win_func_duplicates(ObSelectStmt* select_stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(select_stmt));
  } else {
    ObSEArray<ObRawExpr*, 4> old_win_func_exprs;
    ObSEArray<ObRawExpr*, 4> new_win_func_exprs;
    ObSEArray<ObWinFunRawExpr*, 4> new_win_func_items;
    ObWinFunRawExpr* old_win_func_expr = NULL;
    ObWinFunRawExpr* new_win_func_expr = NULL;
    ObSqlBitSet<> removed_items;
    for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt->get_window_func_count(); ++i) {
      if (OB_ISNULL(new_win_func_expr = select_stmt->get_window_func_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null win func expr", K(ret));
      } else if (removed_items.has_member(i)) {
        /*do nothing */
      } else if (OB_FAIL(new_win_func_items.push_back(new_win_func_expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else {
        for (int64_t j = i + 1; OB_SUCC(ret) && j < select_stmt->get_window_func_count(); ++j) {
          if (OB_ISNULL(old_win_func_expr = select_stmt->get_window_func_expr(j))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get null aggr expr", K(ret));
          } else if (new_win_func_expr->same_as(*old_win_func_expr)) {
            if (OB_FAIL(removed_items.add_member(j))) {
              LOG_WARN("failed to add member", K(ret));
            } else if (OB_FAIL(new_win_func_exprs.push_back(new_win_func_expr))) {
              LOG_WARN("failed to push back expr", K(ret));
            } else if (OB_FAIL(old_win_func_exprs.push_back(old_win_func_expr))) {
              LOG_WARN("failed to push back expr", K(ret));
            } else { /*do nothing*/
            }
          } else { /*do nothing*/
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (!old_win_func_exprs.empty()) {
        if (OB_UNLIKELY(old_win_func_exprs.count() != new_win_func_exprs.count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN(
              "replace expr item size not equal", K(ret), K(old_win_func_exprs.count()), K(new_win_func_exprs.count()));
        } else if (OB_FAIL(select_stmt->replace_inner_stmt_expr(old_win_func_exprs, new_win_func_exprs))) {
          LOG_WARN("failed to replace inner stmt expr", K(ret));
        } else if (OB_FAIL(select_stmt->get_window_func_exprs().assign(new_win_func_items))) {
          LOG_WARN("failed to assign win func items failed", K(ret));
        }
      } else { /*do nothing */
      }
    }
  }
  return ret;
}

int ObTransformSimplify::remove_dummy_case_when(ObDMLStmt* stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  ObSEArray<ObRawExpr*, 8> relation_exprs;
  ObSEArray<ObCaseOpRawExpr*, 8> case_when_exprs;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (OB_FAIL(stmt->get_relation_exprs(relation_exprs))) {
    LOG_WARN("failed to get relation exprs", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < relation_exprs.count(); ++i) {
      ObRawExpr* expr = relation_exprs.at(i);
      if (OB_FAIL(remove_dummy_case_when(stmt->get_query_ctx(), expr, trans_happened))) {
        LOG_WARN("failed to remove dummy case when", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformSimplify::remove_dummy_case_when(ObQueryCtx* query_ctx, ObRawExpr* expr, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr) || OB_ISNULL(query_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param has null", K(ret));
  } else if (T_OP_CASE == expr->get_expr_type()) {
    ObCaseOpRawExpr* case_expr = static_cast<ObCaseOpRawExpr*>(expr);
    if (OB_FAIL(inner_remove_dummy_case_when(query_ctx, case_expr, trans_happened))) {
      LOG_WARN("failed to remove dummy case when", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
    if (OB_FAIL(SMART_CALL(remove_dummy_case_when(query_ctx, expr->get_param_expr(i), trans_happened)))) {
      LOG_WARN("failed to remove dummy case when", K(ret));
    }
  }
  return ret;
}

int ObTransformSimplify::inner_remove_dummy_case_when(
    ObQueryCtx* query_ctx, ObCaseOpRawExpr* case_expr, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  ObStmtCompareContext context;
  if (OB_ISNULL(case_expr) || OB_ISNULL(query_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param has null", K(query_ctx), K(case_expr), K(ret));
  } else if (case_expr->get_when_expr_size() != case_expr->get_then_expr_size()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("incorrect case when expr", K(*case_expr), K(ret));
  } else if (OB_FAIL(context.init(query_ctx))) {
    LOG_WARN("init stmt compare context failed", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < case_expr->get_when_expr_size(); ++i) {
    ObRawExpr* when = case_expr->get_when_param_expr(i);
    ObRawExpr* then = case_expr->get_then_param_expr(i);
    ObCaseOpRawExpr* child_case_expr = NULL;
    ObRawExpr* child_when = NULL;
    context.equal_param_info_.reset();
    if (OB_ISNULL(when) || OB_ISNULL(then)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null case when expr", K(when), K(then), K(ret));
    } else if (T_OP_CASE != then->get_expr_type()) {
      // do nothing
    } else if (OB_FALSE_IT(child_case_expr = static_cast<ObCaseOpRawExpr*>(then))) {
    } else if (child_case_expr->get_when_expr_size() <= 0) {
      // do nothing
    } else if (OB_ISNULL(child_when = child_case_expr->get_when_param_expr(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null case when expr", K(ret));
    } else if (when->same_as(*child_when, &context)) {
      ObRawExpr* child_then = child_case_expr->get_then_param_expr(0);
      if (OB_FAIL(case_expr->replace_then_param_expr(i, child_then))) {
        LOG_WARN("failed to replace then param expr", K(ret));
      } else if (OB_FAIL(append(query_ctx->all_equal_param_constraints_, context.equal_param_info_))) {
        LOG_WARN("append equal param info failed", K(ret));
      } else {
        trans_happened = true;
      }
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
