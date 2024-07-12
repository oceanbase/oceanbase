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

#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/resolver/dml/ob_update_stmt.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "sql/rewrite/ob_transform_const_propagate.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/rewrite/ob_stmt_comparer.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"
#include "common/ob_smart_call.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace sql
{
/**
 * @brief ObTransformConstPropagate::transform_one_stmt
 * collect const info and do propagation for:
 * - condition expr
 * - semi-condition expr
 * - select expr(including subquery)
 * - set op expr
 * - orderby/groupby expr
 * - having expr
 * - update assignment expr
 * NOTE: folding (pre-calculation) is not included in this rule.
 * 
 * supports pushing down & pull up constant for subquery and generated table.
 * supports DML.
 * 
 * following select expr will NOT be replaced:
 * 1. all select exprs will NOT be replaced, including:
 *  a. select expr of null side table item
 * 2. col ref select expr will NOT be replaced, including:
 *  a. col ref select expr of top level select stmt (no benefit)
 *  b. col ref select expr of updated view (not allowed)
 */
int ObTransformConstPropagate::transform_one_stmt(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                                  ObDMLStmt *&stmt,
                                                  bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  ObDMLStmt *parent_stmt = parent_stmts.empty() ? NULL : parent_stmts.at(parent_stmts.count() - 1).stmt_;
  bool is_on_null_side = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parameter", K(ret));
  } else if (!stmt_map_.created() && OB_FAIL(stmt_map_.create(20, ObModIds::OB_SQL_COMPILE))) {
    LOG_WARN("failed to create stmt map", K(ret));
  } else if (OB_NOT_NULL(parent_stmt) && stmt->is_select_stmt() &&
             OB_FAIL(is_parent_null_side(parent_stmt, stmt, is_on_null_side))) {
    LOG_WARN("failed to check null side", K(ret));
  } else if (OB_FAIL(do_transform(stmt,
                                  is_on_null_side,
                                  trans_happened))) {
    LOG_WARN("failed to do transformation for const propagation", K(ret));
  }
  if (OB_SUCC(ret) && trans_happened) {
    if (OB_FAIL(add_transform_hint(*stmt))) {
      LOG_WARN("failed to add transform hint", K(ret));
    }
  }
  return ret;
}

ObTransformConstPropagate::~ObTransformConstPropagate()
{
  for (int64_t i = 0; i < stmt_pullup_const_infos_.count(); ++i) {
    if (NULL != stmt_pullup_const_infos_.at(i)) {
      stmt_pullup_const_infos_.at(i)->~PullupConstInfos();
      stmt_pullup_const_infos_.at(i) = NULL;
    }
  }
}

int ObTransformConstPropagate::ExprConstInfo::merge_complex(ExprConstInfo &other)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(column_expr_ != other.column_expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column expr mismatch when merge cosnt info", K(ret));
  } else if (!other.is_complex_const_info_) {
    if (OB_FAIL(multi_const_exprs_.push_back(other.const_expr_))) {
      LOG_WARN("failed to push back const expr", K(ret));
    } else if (OB_FAIL(multi_need_add_constraints_.push_back(other.need_add_constraint_))) {
      LOG_WARN("failed to push back pre calc index", K(ret));
    }
  } else {
    if (OB_FAIL(append(multi_const_exprs_, other.multi_const_exprs_))) {
      LOG_WARN("failed to append const expr", K(ret));
    } else if (OB_FAIL(append(multi_need_add_constraints_, other.multi_need_add_constraints_))) {
      LOG_WARN("failed to append pre calc index", K(ret));
    }
  }
  return ret;
}

int ObTransformConstPropagate::ConstInfoContext::add_const_infos(ObIArray<ExprConstInfo> &const_infos)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < const_infos.count(); ++i) {
    if (OB_FAIL(add_const_info(const_infos.at(i)))) {
      LOG_WARN("failed to add const info", K(ret));
    }
  }
  return ret;
}

int ObTransformConstPropagate::ConstInfoContext::add_const_info(ExprConstInfo &const_info)
{
  int ret = OB_SUCCESS;
  bool found = false;
  for (int64_t i = 0; OB_SUCC(ret) && !found && i < active_const_infos_.count(); ++i) {
    ExprConstInfo &cur_info = active_const_infos_.at(i);
    if (const_info.column_expr_ == cur_info.const_expr_) {
      found = true;
      if (const_info.is_used_) {
        if (OB_FAIL(expired_const_infos_.push_back(const_info))) {
          LOG_WARN("failed to push back", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret) && !found) {
    if (OB_FAIL(active_const_infos_.push_back(const_info))) {
      LOG_WARN("failed to push back", K(ret));
    }
  }
  return ret;
}

int ObTransformConstPropagate::ConstInfoContext::merge_expired_const_infos(ConstInfoContext &other,
                                                                           bool can_pull_up)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < other.active_const_infos_.count(); ++i) {
    if (can_pull_up) {
      if (OB_FAIL(add_const_info(other.active_const_infos_.at(i)))) {
        LOG_WARN("failed to push back", K(ret));
      }
    } else if (other.active_const_infos_.at(i).is_used_) {
      if (OB_FAIL(expired_const_infos_.push_back(other.active_const_infos_.at(i)))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(append(expired_const_infos_, other.expired_const_infos_))) {
      LOG_WARN("failed to append expired const infos", K(ret));
    }
  }
  return ret;
}

int ObTransformConstPropagate::ConstInfoContext::find_exclude_expr(const ObRawExpr *expr,
                                                                   bool &found)
{
  int ret = OB_SUCCESS;
  found = false;
  for (int64_t i = 0; OB_SUCC(ret) && !found && i < active_const_infos_.count(); ++i) {
    if (active_const_infos_.at(i).exclude_expr_ == expr) {
      found = true;
    }
  }
  if (OB_SUCC(ret) && !found) {
    found = ObOptimizerUtil::find_item(extra_excluded_exprs_, expr);
  }
  return ret;
}

int ObTransformConstPropagate::ConstInfoContext::expire_const_infos()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < active_const_infos_.count(); ++i) {
    if (!active_const_infos_.at(i).is_used_) {
      // do nothing
    } else if (OB_FAIL(expired_const_infos_.push_back(active_const_infos_.at(i)))) {
      LOG_WARN("failed to push back", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    active_const_infos_.reuse();
    extra_excluded_exprs_.reuse();
  }
  return ret;
}

int ObTransformConstPropagate::check_hint_status(const ObDMLStmt &stmt, bool &need_trans)
{
  int ret = OB_SUCCESS;
  const ObQueryHint *query_hint = NULL;
  const ObHint *trans_hint = NULL;
  need_trans = false;
  if (OB_ISNULL(ctx_) ||
      OB_ISNULL(query_hint = stmt.get_stmt_hint().query_hint_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(ctx_), K(query_hint));
  } else if (!query_hint->has_outline_data()) {
    need_trans = true;
  } else if (NULL != (trans_hint = query_hint->get_outline_trans_hint(ctx_->trans_list_loc_))
             && trans_hint->get_hint_type() == get_hint_type()) {
    // next trans hint is REPLACE_CONST, allowed collect equal pair
    need_trans = true;
  }
  return ret;
}

int ObTransformConstPropagate::do_transform(ObDMLStmt *stmt,
                                            bool ignore_all_select_exprs,
                                            bool &trans_happened)
{
  int ret = OB_SUCCESS;
  ObSharedExprChecker shared_expr_checker;
  bool allow_trans = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parameter", K(ret));
  } else if (OB_FAIL(check_allow_trans(stmt, allow_trans))) {
    LOG_WARN("failed to check trans allowed", K(ret));
  } else if (!stmt->is_insert_stmt() && OB_FAIL(shared_expr_checker.init(*stmt))) {
    LOG_WARN("failed to init shared expr checker", K(ret));
  } else {
    ConstInfoContext const_ctx(shared_expr_checker, allow_trans);
    bool has_rollup_or_groupingsets = false;
    bool is_happened = false;
    if (OB_SUCC(ret)) {
      if (OB_FAIL(collect_equal_pair_from_condition(stmt,
                                                    stmt->get_condition_exprs(),
                                                    const_ctx,
                                                    is_happened))) {
        LOG_WARN("failed to collect const info from where condition", K(ret));
      } else {
        trans_happened |= is_happened;
        LOG_TRACE("succeed to do const propagation while collect from condition", K(is_happened));
      }
    }

    if (OB_SUCC(ret)) {
      is_happened = false;
      if (OB_FAIL(collect_equal_pair_from_tables(stmt,
                                                 const_ctx,
                                                 is_happened))) {
        LOG_WARN("failed to collect const info from tables", K(ret));
      } else {
        trans_happened |= is_happened;
        LOG_TRACE("succeed to do const propagation while collect from tables", K(is_happened));
      }
    }

    if (OB_SUCC(ret)) {
      is_happened = false;
      if (OB_FAIL(collect_equal_pair_from_semi_infos(stmt,
                                                     const_ctx,
                                                     is_happened))) {
        LOG_WARN("failed to collect const info from semi info", K(ret));
      } else {
        trans_happened |= is_happened;
        LOG_TRACE("succeed to do const propagation while collect from semi info", K(is_happened));
      }
    }

    if (OB_SUCC(ret)) {
      is_happened = false;
      if (OB_FAIL(replace_join_conditions(stmt,
                                          const_ctx,
                                          is_happened))) {
        LOG_WARN("failed to replace join conditions", K(ret));
      } else {
        trans_happened |= is_happened;
        LOG_TRACE("succeed to do const propagation for join conditions", K(is_happened));
      }
    }

    if (OB_SUCC(ret) && !const_ctx.active_const_infos_.empty()) {
      is_happened = false;
      if (OB_FAIL(replace_common_exprs(stmt->get_condition_exprs(),
                                       const_ctx,
                                       is_happened))) {
        LOG_WARN("failed to replace condition exprs", K(ret));
      } else {
        trans_happened |= is_happened;
        LOG_TRACE("succeed to do const propagation for condition expr", K(is_happened));
      }
    }

    if (OB_SUCC(ret) && !const_ctx.active_const_infos_.empty() &&
        (stmt->is_insert_stmt() || stmt->is_merge_stmt())) {
      is_happened = false;
      ObDelUpdStmt *insert = static_cast<ObDelUpdStmt *>(stmt);
      if (OB_FAIL(collect_equal_pair_from_condition(stmt,
                                                    insert->get_sharding_conditions(),
                                                    const_ctx,
                                                    is_happened))) {
        LOG_WARN("failed to collect const info from sharding condition", K(ret));
      } else {
        trans_happened |= is_happened;
        LOG_TRACE("succeed to do const propagation while collect from sharding", K(is_happened));
      }
    }

    if (OB_SUCC(ret) && !const_ctx.active_const_infos_.empty() &&
        (stmt->is_insert_stmt() || stmt->is_merge_stmt())) {
      ObDelUpdStmt *insert = static_cast<ObDelUpdStmt *>(stmt);
      is_happened = false;
      if (OB_FAIL(replace_common_exprs(insert->get_sharding_conditions(),
                                       const_ctx,
                                       is_happened))) {
        LOG_WARN("failed to repalce condition exprs", K(ret));
      } else {
        trans_happened |= is_happened;
        LOG_TRACE("succeed to do const propagation for sharding expr", K(is_happened));
      }
    }

    if (OB_SUCC(ret) && !const_ctx.active_const_infos_.empty()) {
      is_happened = false;
      if (OB_FAIL(replace_semi_conditions(stmt,
                                          const_ctx,
                                          is_happened))) {
        LOG_WARN("failed to replace semi conditions", K(ret));
      } else {
        trans_happened |= is_happened;
        LOG_TRACE("succeed to do const propagation for semi conditions", K(is_happened));
      }
    }

    // replace groupby exprs using common const info
    if (OB_SUCC(ret) && stmt->is_select_stmt() && (static_cast<ObSelectStmt*>(stmt)->has_rollup() ||
        static_cast<ObSelectStmt*>(stmt)->get_grouping_sets_items_size() != 0)) {
      has_rollup_or_groupingsets = true;
    }

    if (OB_SUCC(ret) && !const_ctx.active_const_infos_.empty() && stmt->is_select_stmt() && !has_rollup_or_groupingsets) {
      is_happened = false;
      if (OB_FAIL(replace_group_exprs(static_cast<ObSelectStmt*>(stmt),
                                      const_ctx,
                                      ignore_all_select_exprs,
                                      is_happened))) {
        LOG_WARN("failed to replace groupby exprs", K(ret));
      } else {
        trans_happened |= is_happened;
        LOG_TRACE("succeed to do const propagation for groupby expr", K(is_happened));
      }
    }

    if (OB_SUCC(ret) && stmt->is_select_stmt() &&
        (has_rollup_or_groupingsets || static_cast<ObSelectStmt*>(stmt)->is_scala_group_by())) {
      if (OB_FAIL(const_ctx.expire_const_infos())) {
        LOG_WARN("failed to expire const infos ", K(ret));
      }
    }


    if (OB_SUCC(ret) && stmt->is_select_stmt()) {
      if (!const_ctx.active_const_infos_.empty() && !ignore_all_select_exprs && static_cast<ObSelectStmt*>(stmt)->get_aggr_item_size() > 0) {
        is_happened = false;
        if (OB_FAIL(replace_select_exprs(static_cast<ObSelectStmt*>(stmt),
                                         const_ctx,
                                         is_happened))) {
          LOG_WARN("failed to replace select exprs", K(ret));
        } else {
          trans_happened |= is_happened;
          LOG_TRACE("succeed to do const propagation replace select exprs before get having exprs", K(is_happened));
        }
      }
    }

    if (OB_SUCC(ret) && stmt->is_select_stmt()) {
      is_happened = false;
      if (OB_FAIL(collect_equal_pair_from_condition(stmt,
                                                    static_cast<ObSelectStmt*>(stmt)->get_having_exprs(),
                                                    const_ctx,
                                                    is_happened))) {
        LOG_WARN("failed to collect const info from having condition", K(ret));
      } else {
        trans_happened |= is_happened;
        LOG_TRACE("succeed to do const propagation while collect from having", K(is_happened));
      }
    }

    if (OB_SUCC(ret) && stmt->is_select_stmt()) {
      is_happened = false;
      if (OB_FAIL(replace_common_exprs(static_cast<ObSelectStmt*>(stmt)->get_having_exprs(),
                                       const_ctx,
                                       is_happened))) {
        LOG_WARN("failed to replace having exprs", K(ret));
      } else {
        trans_happened |= is_happened;
        LOG_TRACE("succeed to do const propagation for having expr", K(is_happened));
      }
    }

    if (OB_SUCC(ret) && !const_ctx.active_const_infos_.empty()) {
      is_happened = false;
      if (OB_FAIL(replace_orderby_exprs(stmt->get_order_items(),
                                        const_ctx,
                                        is_happened))) {
        LOG_WARN("failed to replace orderby exprs", K(ret));
      } else {
        trans_happened |= is_happened;
        LOG_TRACE("succeed to do const propagation for orderby expr", K(is_happened));
      }
    }

    // replace select exprs using common const info and post-gby const info
    if (OB_SUCC(ret) && !const_ctx.active_const_infos_.empty() && stmt->is_select_stmt() && !ignore_all_select_exprs) {
      is_happened = false;
      if (static_cast<ObSelectStmt*>(stmt)->get_aggr_item_size() > 0) {
        if (OB_FAIL(replace_select_exprs_skip_agg(static_cast<ObSelectStmt*>(stmt),
                                       const_ctx,
                                       is_happened))) {
          LOG_WARN("failed to replace select exprs", K(ret));
        } else {
          trans_happened |= is_happened;
          LOG_TRACE("succeed to do const propagation for select expr", K(is_happened));
        }
      } else if (OB_FAIL(replace_select_exprs(static_cast<ObSelectStmt*>(stmt),
                                       const_ctx,
                                       is_happened))) {
        LOG_WARN("failed to replace select exprs", K(ret));
      } else {
        trans_happened |= is_happened;
        LOG_TRACE("succeed to do const propagation for select expr", K(is_happened));
      }
    }

    // replace update assignment list
    if (OB_SUCC(ret) && !const_ctx.active_const_infos_.empty() && stmt->is_update_stmt()) {
      is_happened = false;
      ObUpdateStmt *upd_stmt = static_cast<ObUpdateStmt *>(stmt);
       for (int64_t i = 0; OB_SUCC(ret) && i < upd_stmt->get_update_table_info().count(); ++i) {
        ObUpdateTableInfo* table_info = upd_stmt->get_update_table_info().at(i);
        if (OB_ISNULL(table_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get null table info", K(ret));
        } else if (OB_FAIL(replace_assignment_exprs(table_info->assignments_,
                                                    const_ctx,
                                                    is_happened))) {
          LOG_WARN("failed to replace assignment exprs", K(ret));
        } else {
          trans_happened |= is_happened;
          LOG_TRACE("succeed to do const propagation for assignment expr", K(is_happened));
        }
      }
    }

    // replace condition exprs from check constraint exprs.
    if (OB_SUCC(ret) && !const_ctx.active_const_infos_.empty()) {
      is_happened = false;
      if (OB_FAIL(replace_check_constraint_exprs(stmt,
                                                 const_ctx,
                                                 is_happened))) {
        LOG_WARN("failed to replace condition exprs", K(ret));
      } else {
        trans_happened |= is_happened;
        LOG_TRACE("succeed to do const propagation for check constraint expr", K(is_happened));
      }
    }

    if (OB_SUCC(ret) && trans_happened) {
      if (OB_FAIL(collect_equal_param_constraints(const_ctx.active_const_infos_))) {
        LOG_WARN("failed to append active equal params constraints", K(ret));
      } else if (OB_FAIL(collect_equal_param_constraints(const_ctx.expired_const_infos_))) {
        LOG_WARN("failed to append expired equal params constraints", K(ret));
      } else if (OB_FAIL(remove_const_exec_param(stmt))) {
        LOG_WARN("failed to remove const exec param", K(ret));
      } else if (OB_FAIL(stmt->formalize_stmt(ctx_->session_info_))) {
        LOG_WARN("failed to formalize stmt info", K(ret));
      } else {
        LOG_TRACE("succeed to do replacement internal", KPC(stmt));
      }
    }

    if (OB_SUCC(ret) && OB_FAIL(shared_expr_checker.destroy())) {
      LOG_WARN("failed to destroy shared expr checker", K(ret));
    }
  }
  return ret;
}

// const info is always collected even if transform is not allowed
int ObTransformConstPropagate::check_allow_trans(ObDMLStmt *stmt, bool &allow_trans)
{
  int ret = OB_SUCCESS;
  bool hint_allowed_trans = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parameter", K(ret));
  } else if (OB_FAIL(ObTransformRule::check_hint_status(*stmt, hint_allowed_trans))) {
    LOG_WARN("failed to check_hint_status", K(ret));
  } else {
    allow_trans = hint_allowed_trans && !stmt->is_contains_assignment();
  }
  return ret;
}

int ObTransformConstPropagate::exclude_redundancy_join_cond(ObIArray<ObRawExpr*> &condition_exprs,
                                                            ObIArray<ExprConstInfo> &expr_const_infos,
                                                            ObIArray<ObRawExpr*> &excluded_exprs)
{
  int ret = OB_SUCCESS;
  ObRawExpr *l_const_expr = NULL;
  ObRawExpr *r_const_expr = NULL;
  ObExprEqualCheckContext equal_ctx;
  equal_ctx.override_const_compare_ = true;
  for (int64_t i = 0; OB_SUCC(ret) && i < condition_exprs.count(); ++i) {
    ObRawExpr *expr = condition_exprs.at(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null expr", K(ret));
    } else if (!expr->has_flag(IS_JOIN_COND) ||
               2 != expr->get_param_count()) {
      //do nothing
    } else if (!find_const_expr(expr_const_infos, expr->get_param_expr(0), l_const_expr) || 
               !find_const_expr(expr_const_infos, expr->get_param_expr(1), r_const_expr)) {
      //do nothing
    } else if (OB_ISNULL(l_const_expr) || OB_ISNULL(r_const_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null expr", K(ret));
    } else if (!l_const_expr->same_as(*r_const_expr, &equal_ctx)) {
      //do nothing
    } else if (OB_FAIL(excluded_exprs.push_back(expr))) {
      LOG_WARN("failed to push back expr", K(ret));
    }
  }
  return ret;
}

bool ObTransformConstPropagate::find_const_expr(ObIArray<ExprConstInfo> &expr_const_infos, 
                                                ObRawExpr *expr, 
                                                ObRawExpr* &const_expr)
{
  const_expr = NULL;
  bool found = false;
  for (int64_t i = 0; !found && i < expr_const_infos.count(); ++i) {
    if (!expr_const_infos.at(i).is_complex_const_info_) {
      if (expr == expr_const_infos.at(i).column_expr_) {
        const_expr = expr_const_infos.at(i).const_expr_;
        found = true;
      }
    }
  }
  return found;
}

int ObTransformConstPropagate::collect_equal_pair_from_condition(ObDMLStmt *stmt,
                                                                 ObIArray<ObRawExpr*> &condition_exprs,
                                                                 ConstInfoContext &const_ctx,
                                                                 bool &trans_happened)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < condition_exprs.count(); ++i) {
    ObRawExpr *cur_expr = condition_exprs.at(i);
    if (OB_ISNULL(cur_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid condition exprs", K(ret));
    } else if (OB_FAIL(recursive_collect_equal_pair_from_condition(stmt,
                                                                   cur_expr,
                                                                   const_ctx,
                                                                   trans_happened))) {
      LOG_WARN("failed to recursive collect const info from condition", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(exclude_redundancy_join_cond(condition_exprs,
                                             const_ctx.active_const_infos_,
                                             const_ctx.extra_excluded_exprs_))) {
      LOG_WARN("failed to exclude redundancy join cond", K(ret));
    }
  }
  return ret;
}

int ObTransformConstPropagate::collect_equal_pair_from_tables(ObDMLStmt *stmt,
                                                              ConstInfoContext &const_ctx,
                                                              bool &trans_happened)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parameter", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_from_item_size(); ++i) {
    bool is_happened = false;
    FromItem &from_item = stmt->get_from_item(i);
    TableItem *table_item = stmt->get_table_item(from_item);
    if (OB_ISNULL(table_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid parameter", K(ret));
    } else if (OB_FAIL(recursive_collect_const_info_from_table(stmt,
                                                               table_item,
                                                               const_ctx,
                                                               false,
                                                               is_happened))) {
      LOG_WARN("failed to collect const info from table", K(ret));
    } else {
      trans_happened |= is_happened;
    }
  }
  return ret;
}

int ObTransformConstPropagate::collect_equal_pair_from_semi_infos(ObDMLStmt *stmt,
                                                                  ConstInfoContext &const_ctx,
                                                                  bool &trans_happened)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parameter", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_semi_info_size(); ++i) {
    bool is_happened = false;
    SemiInfo *semi_info = stmt->get_semi_infos().at(i);
    TableItem *right_table = NULL;
    if (OB_ISNULL(semi_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid semi info", K(ret));
    } else if (OB_ISNULL(right_table = stmt->get_table_item_by_id(semi_info->right_table_id_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected semi right table", KPC(semi_info), K(ret));
    } else if (OB_FAIL(recursive_collect_const_info_from_table(stmt,
                                                               right_table,
                                                               const_ctx,
                                                               true,
                                                               is_happened))) {
      LOG_WARN("failed to collect const info from table", K(ret));
    } else {
      trans_happened |= is_happened;
    }
  }
  return ret;
}

/* collect const info from table
 * 1. As left-join table(also right-join table
 *    const infos from not_null_side and on_condition can be pulled up to high level const ctx.
 *    const infos from null_side can only used on self level join table.
 * 2. inner-join table, const infos can be pulled up to high level const ctx.
*/
int ObTransformConstPropagate::recursive_collect_const_info_from_table(ObDMLStmt *stmt,
                                                                       TableItem *table_item,
                                                                       ConstInfoContext &const_ctx,
                                                                       bool is_null_side,
                                                                       bool &trans_happened)
{
  int ret = OB_SUCCESS;
  JoinedTable *joined_table = NULL;
  if (OB_ISNULL(stmt) || OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parameter", K(ret));
  } else if (table_item->is_generated_table()) {
    if (OB_FAIL(collect_equal_pair_from_pullup(stmt, table_item, const_ctx, is_null_side))) {
      LOG_WARN("failed to collect const info from pullup", K(ret));
    }
  } else if (!table_item->is_joined_table()) {
  } else if (FALSE_IT(joined_table = static_cast<JoinedTable *>(table_item))) {
  } else if (LEFT_OUTER_JOIN == joined_table->joined_type_ ||
             RIGHT_OUTER_JOIN == joined_table->joined_type_) {
    // FULL_OUT_JOIN is not transformed because may eliminate all equal join conditions
    TableItem *left_table = LEFT_OUTER_JOIN == joined_table->joined_type_ ?
                            joined_table->left_table_ : joined_table->right_table_;
    TableItem *right_table = LEFT_OUTER_JOIN == joined_table->joined_type_ ?
                              joined_table->right_table_ : joined_table->left_table_;
    ConstInfoContext tmp_ctx(const_ctx.shared_expr_checker_, const_ctx.allow_trans_);
    bool left_happened = false;
    bool right_happened = false;
    bool condition_happened = false;
    bool replace_join_left = false;
    bool replace_join_right = false;
    if (OB_FAIL(SMART_CALL(recursive_collect_const_info_from_table(stmt, left_table,
                                                                   tmp_ctx, false,
                                                                   left_happened)))) {
      LOG_WARN("failed to recursive collect const info from table", K(ret));
    } else if (OB_FAIL(SMART_CALL(recursive_replace_join_conditions(joined_table, tmp_ctx,
                                                                    replace_join_left)))) {
      LOG_WARN("failed to replace exprs in joined table", K(ret));
    } else if (OB_FAIL(const_ctx.merge_expired_const_infos(tmp_ctx, true))) {
      LOG_WARN("failed to merge expired const infos", K(ret));
    } else if (FALSE_IT(tmp_ctx.reset())) {
    } else if (OB_FAIL(SMART_CALL(recursive_collect_const_info_from_table(stmt, right_table,
                                                                          tmp_ctx, true,
                                                                          right_happened)))) {
      LOG_WARN("failed to recursive collect const info from table", K(ret));
    } else if (OB_FAIL(collect_equal_pair_from_condition(stmt, joined_table->get_join_conditions(),
                                                         tmp_ctx, condition_happened))) {
      LOG_WARN("failed to collect const info from join condition", K(ret));
    } else if (OB_FAIL(SMART_CALL(recursive_replace_join_conditions(joined_table, tmp_ctx,
                                                                    replace_join_right)))) {
      LOG_WARN("failed to replace exprs in joined table", K(ret));
    } else if (OB_FAIL(const_ctx.merge_expired_const_infos(tmp_ctx, false))) {
      LOG_WARN("failed to merge expired const infos", K(ret));
    } else {
      trans_happened |= left_happened || right_happened || condition_happened ||
                        replace_join_left || replace_join_right;
      LOG_TRACE("collect const info from table", K(trans_happened), K(left_happened),
                K(right_happened), K(condition_happened), K(replace_join_left),
                K(replace_join_right));
    }
  } else if (joined_table->is_inner_join()) {
    bool left_happened = false;
    bool right_happened = false;
    bool condition_happened = false;
    if (OB_FAIL(SMART_CALL(recursive_collect_const_info_from_table(stmt,
                                                                   joined_table->left_table_,
                                                                   const_ctx,
                                                                   false,
                                                                   left_happened)))) {
      LOG_WARN("failed to recursive collect const info from table", K(ret));
    } else if (OB_FAIL(SMART_CALL(recursive_collect_const_info_from_table(stmt,
                                                                         joined_table->right_table_,
                                                                         const_ctx,
                                                                         false,
                                                                         right_happened)))) {
      LOG_WARN("failed to recursive collect const info from table", K(ret));
    } else if (OB_FAIL(collect_equal_pair_from_condition(stmt,
                                                         joined_table->get_join_conditions(),
                                                         const_ctx,
                                                         condition_happened))) {
      LOG_WARN("failed to collect const info from join condition", K(ret));
    } else {
      trans_happened |= left_happened || right_happened || condition_happened;
      LOG_TRACE("collect const info from table", K(trans_happened), K(left_happened),
                K(right_happened), K(condition_happened));
    }
  }
  return ret;
}

int ObTransformConstPropagate::collect_equal_pair_from_pullup(ObDMLStmt *stmt,
                                                              TableItem *table,
                                                              ConstInfoContext &const_ctx,
                                                              bool is_null_side)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *child_stmt = NULL;
  if (OB_ISNULL(stmt) || OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parameter", K(ret));
  } else if (!table->is_generated_table()) {
    // do nothing
  } else if (OB_ISNULL(child_stmt = table->ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid target table ref query", K(ret));
  } else {
    for (int64_t j = 0; OB_SUCC(ret) && j < child_stmt->get_select_item_size(); ++j) {
      ObRawExpr *select_expr = child_stmt->get_select_items().at(j).expr_;
      ExprConstInfo equal_info;
      equal_info.mem_equal_ = true;
      const uint64_t column_id = j + OB_APP_MIN_COLUMN_ID;
      if (OB_ISNULL(select_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid selet expr", K(ret));
      } else if (OB_ISNULL(equal_info.column_expr_ =
                            stmt->get_column_expr_by_id(table->table_id_, column_id))) {
        // do nothing
      } else if (select_expr->is_const_expr()) {
        // handle generated_table const pull up.
        // case1: select * from (select 1 as a, 2 as b from t1) v, t2 where v.a = t2.c1 and v.b = t2.c2; =>
        //        select * from (select 1 as a, 2 as b from t1) v, t2 where 1   = t2.c1 and 2   = t2.c2;
        equal_info.const_expr_ = select_expr;
      } else if (select_expr->is_set_op_expr()) {
        // handle const set_op expr pull up.
        // NOTE: before replace the set_op const expr, it needs to check the unique equal constraint and record
        // it into equal_param_constraints_ in transform context to makes plan cache correct.  
        // case2: select c1 from (select 1 as c1 from t1 union select 1 as c1 from t2); =>
        //        select 1  from (select 1 as c1 from t1 union select 1 as c1 from t2);
        if (OB_FAIL(check_set_op_expr_const(child_stmt,
                                            static_cast<ObSetOpRawExpr*>(select_expr),
                                            equal_info.const_expr_,
                                            equal_info.equal_infos_))) {
          LOG_WARN("failed to check set op expr const", K(ret));
        }
      } else if (NULL == equal_info.const_expr_) {
        // handle const info pull up
        // case3: select * from (select c1 as a from t1 where c1 = '1') v, t2 where v.a = t2.c1; =>
        //        select * from (select c1 as a from t1 where c1 = '1') v, t2 where '1' = t2.c1;
        equal_info.mem_equal_ = false;
        if (OB_FAIL(collect_from_pullup_const_infos(child_stmt, select_expr, equal_info))) {
          LOG_WARN("failed to get expr from pullup const info", K(ret));
        }
      }
      if (OB_SUCC(ret) && equal_info.column_expr_ != NULL && equal_info.const_expr_ != NULL) {
        bool is_valid = false;
        if (OB_FAIL(check_const_expr_validity(*child_stmt,
                                              equal_info,
                                              is_valid))) {
          LOG_WARN("failed to check const expr validity", K(ret));
        } else if (!is_valid) {
          ObObjType col_type = equal_info.column_expr_->get_result_type().get_type();
          if (!(ob_is_text_tc(col_type) || ob_is_lob_tc(col_type) || ob_is_json_tc(col_type)) &&
              equal_info.const_expr_->is_static_scalar_const_expr()) {
            ObObj result;
            bool got_result = false;
            if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(ctx_->exec_ctx_,
                                                                  equal_info.const_expr_,
                                                                  result,
                                                                  got_result,
                                                                  *ctx_->allocator_))) {
              LOG_WARN("failed to calc const or caculable expr", K(ret));
            } else if (got_result && (result.is_null()
                      || (lib::is_oracle_mode() && result.is_null_oracle()))) {
              is_valid = true;
              if (!equal_info.const_expr_ -> is_const_raw_expr()) {
                equal_info.need_add_constraint_ = PRE_CALC_RESULT_NULL;
              }
            }
          }
        }
        if (OB_SUCC(ret) && is_valid) {
          if (OB_FAIL(check_cast_const_expr(equal_info, is_valid))) {
            LOG_WARN("failed to check need cast constraint", K(ret));
          }
        }
        if (OB_SUCC(ret) && is_valid) {
          if (OB_FAIL(const_ctx.add_const_info(equal_info))) {
            LOG_WARN("failed to push back equal info", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObTransformConstPropagate::replace_common_exprs(ObIArray<ObRawExpr *> &exprs,
                                                    ConstInfoContext &const_ctx,
                                                    bool &trans_happened,
                                                    bool used_in_compare)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    ObRawExpr *&cur_expr = exprs.at(i);
    bool internal_trans_happened = false;
    if (OB_ISNULL(cur_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid parameter", K(ret));
    } else if (OB_FAIL(replace_expr_internal(cur_expr,
                                             const_ctx,
                                             internal_trans_happened,
                                             used_in_compare))) {
      LOG_WARN("failed to replace exprs based on equal_pair", K(ret));
    } else {
      trans_happened |= internal_trans_happened;
    }
  }
  return ret;
}

int ObTransformConstPropagate::replace_group_exprs(ObSelectStmt *stmt,
                                                   ConstInfoContext &const_ctx,
                                                   bool ignore_all_select_exprs,
                                                   bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parameter", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_group_expr_size(); ++i) {
    ObRawExpr *group_expr = stmt->get_group_exprs().at(i);
    bool can_replace = true;
    bool internal_trans_happened = false;
    if (OB_ISNULL(group_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid parameter", K(ret));
    } else if (OB_FAIL(check_can_replace_in_select(stmt, group_expr, ignore_all_select_exprs,
                                                   can_replace))) {
      LOG_WARN("failed to check can replace in select", K(ret));
    } else if (!can_replace) {
      // group expr can not be transfromed when the corresponding select item can not be transfromed
    } else if (OB_FAIL(replace_expr_internal(stmt->get_group_exprs().at(i),
                                             const_ctx,
                                             internal_trans_happened,
                                             false))) {
      LOG_WARN("failed to replace select exprs based on equal_pair", K(ret));
    } else {
      trans_happened |= internal_trans_happened;
    }
  }
  return ret;
}

int ObTransformConstPropagate::replace_select_exprs(ObSelectStmt *stmt,
                                                    ConstInfoContext &const_ctx,
                                                    bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parameter", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_select_items().count(); ++i) {
    ObRawExpr *&cur_expr = stmt->get_select_items().at(i).expr_;
    bool internal_trans_happened = false;
    if (OB_ISNULL(cur_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid parameter", K(ret));
    } else if (cur_expr->is_column_ref_expr()) {
      if (OB_FAIL(generate_pullup_const_info(stmt, cur_expr, const_ctx))) {
        LOG_WARN("failed to generate pullup const info", K(ret));
      }
    } else if (OB_FAIL(replace_expr_internal(cur_expr,
                                             const_ctx,
                                             internal_trans_happened,
                                             false))) {
      LOG_WARN("failed to replace select exprs based on equal_pair", K(ret));
    } else {
      trans_happened |= internal_trans_happened;
    }
  }
  return ret;
}

int ObTransformConstPropagate::replace_orderby_exprs(ObIArray<OrderItem> &order_items,
                                                     ConstInfoContext &const_ctx,
                                                     bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < order_items.count(); ++i) {
    ObRawExpr *&cur_expr = order_items.at(i).expr_;
    bool internal_trans_happened = false;
    if (OB_ISNULL(cur_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid parameter", K(ret));
    } else if (OB_FAIL(replace_expr_internal(cur_expr,
                                             const_ctx,
                                             internal_trans_happened))) {
      LOG_WARN("failed to replace order by exprs based on equal_pair", K(ret));
    } else {
      trans_happened |= internal_trans_happened;
    }
  }
  return ret;
}

int ObTransformConstPropagate::replace_assignment_exprs(ObIArray<ObAssignment> &assignments,
                                                        ConstInfoContext &const_ctx,
                                                        bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  for (int64_t j = 0; OB_SUCC(ret) && j < assignments.count(); ++j) {
    ObAssignment &assign = assignments.at(j);
    bool internal_trans_happened = false;
    if (OB_ISNULL(assign.expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("assgin expr is null", K(ret));
    } else if (OB_FAIL(replace_expr_internal(assign.expr_,
                                             const_ctx,
                                             internal_trans_happened,
                                             false))) {
      LOG_WARN("failed to replace assignment exprs based on equal_pair", K(ret));
    } else {
      trans_happened |= internal_trans_happened;
    }
  }
  
  return ret;
}

int ObTransformConstPropagate::replace_join_conditions(ObDMLStmt *stmt,
                                                       ConstInfoContext &const_ctx,
                                                       bool &trans_happened)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parameter", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_table_size(); ++i) {
    TableItem *table = stmt->get_table_items().at(i);
    bool is_happened = false;
    if (OB_ISNULL(table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid parameter", K(ret));
    } else if (OB_FAIL(recursive_replace_join_conditions(table, const_ctx, is_happened))) {
      LOG_WARN("failed to replace exprs in joined table", K(ret));
    } else {
      trans_happened |= is_happened;
    }
  }
  return ret;
}

// replace semi condition exprs using common const info.
// if right only filters in semi info found after replacement, pushing them
// down into right table.
// formalize stmt before pushing down and generate view for right table if needed.
int ObTransformConstPropagate::replace_semi_conditions(ObDMLStmt *stmt,
                                                       ConstInfoContext &const_ctx,
                                                       bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parameter", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_semi_info_size(); ++i) {
    SemiInfo *semi_info = stmt->get_semi_infos().at(i);
    bool is_happened = false;
    if (OB_ISNULL(semi_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid semi info", K(ret));
    } else if (OB_FAIL(exclude_redundancy_join_cond(semi_info->semi_conditions_,
                                                    const_ctx.active_const_infos_,
                                                    const_ctx.extra_excluded_exprs_))) {
      LOG_WARN("failed to exclude redundancy join cond", K(ret));
    } else if (OB_FAIL(replace_common_exprs(semi_info->semi_conditions_,
                                            const_ctx,
                                            is_happened))) {
      LOG_WARN("failed to replace semi condition exprs", K(ret));
    } else if (OB_FAIL(stmt->formalize_stmt(ctx_->session_info_))) {
      LOG_WARN("formalize child stmt failed", K(ret));
    } else {
      trans_happened |= is_happened;
      LOG_TRACE("succeed to do const propagation for semi condition expr", K(is_happened));
    }
  }
  return ret;
}

int ObTransformConstPropagate::recursive_replace_join_conditions(TableItem *table_item,
                                                                 ConstInfoContext &const_ctx,
                                                                 bool &trans_happened)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parameter", K(ret));
  } else if (!table_item->is_joined_table()) {
    // do nothing
  } else {
    JoinedTable *joined_table = static_cast<JoinedTable *>(table_item);
    bool is_happened = false;
    if (OB_SUCC(ret) && (joined_table->is_right_join() || joined_table->is_inner_join())) {
      if (OB_FAIL(SMART_CALL(recursive_replace_join_conditions(joined_table->left_table_,
                                                               const_ctx,
                                                               is_happened)))) {
        LOG_WARN("failed to replace exprs in joined table", K(ret));
      } else {
        trans_happened |= is_happened;
      }
    }
    if (OB_SUCC(ret) && (joined_table->is_left_join() || joined_table->is_inner_join())) {
      if (OB_FAIL(SMART_CALL(recursive_replace_join_conditions(joined_table->right_table_,
                                                               const_ctx,
                                                               is_happened)))) {
        LOG_WARN("failed to replace exprs in joined table", K(ret));
      } else {
        trans_happened |= is_happened;
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(SMART_CALL(replace_common_exprs(joined_table->get_join_conditions(),
                                                  const_ctx,
                                                  is_happened)))) {
        LOG_WARN("failed to replace join condition exprs", K(ret));
      } else {
        trans_happened |= is_happened;
      }
    }
  }
  return ret;
}

int ObTransformConstPropagate::replace_expr_internal(ObRawExpr *&cur_expr,
                                                     ConstInfoContext &const_ctx,
                                                     bool &trans_happened,
                                                     bool used_in_compare)
{
  int ret = OB_SUCCESS;
  if (const_ctx.allow_trans_) {
    ObSEArray<ObRawExpr *, 8> parent_exprs;
    if (OB_FAIL(recursive_replace_expr(cur_expr,
                                      parent_exprs,
                                      const_ctx,
                                      used_in_compare,
                                      trans_happened))) {
      LOG_WARN("failed to recursive");
    }
  }
  return ret;
}

int ObTransformConstPropagate::recursive_replace_expr(ObRawExpr *&cur_expr,
                                                      ObIArray<ObRawExpr *> &parent_exprs,
                                                      ConstInfoContext &const_ctx,
                                                      bool used_in_compare,
                                                      bool &trans_happened)
{
  int ret = OB_SUCCESS;
  bool found = false;
  bool is_shared = false;
  trans_happened = false;
  bool can_replace_child = true;
  if (OB_ISNULL(cur_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parameter", K(ret));
  } else if (OB_FAIL(const_ctx.find_exclude_expr(cur_expr, found))) {
    LOG_WARN("failed to find exclude expr", K(ret));
  } else if (found || cur_expr->has_flag(IS_ROWID)) {
    // do nothing
    //because the column expr in rowid will be used to extract query range. So, we can't replace.
    //see the issue:
  } else if (OB_FAIL(const_ctx.shared_expr_checker_.is_shared_expr(cur_expr, is_shared))) {
    LOG_WARN("failed to check is shared expr", K(ret));
  } else if (is_shared) {
    /* do nothing */
  } else if (OB_FAIL(replace_internal(cur_expr,
                                      parent_exprs,
                                      const_ctx.active_const_infos_,
                                      used_in_compare,
                                      trans_happened))) {
    LOG_WARN("failed to replace expr internal", K(ret));
  } else if (trans_happened || cur_expr->get_param_count() < 1) {
    // do nothing
  } else if (cur_expr->get_expr_type() == T_OP_ROW &&
             OB_FAIL(check_can_replace_child_of_row(const_ctx, cur_expr, can_replace_child))) {
    LOG_WARN("failed to check can replace in row", K(ret));
  } else if (!can_replace_child) {
    // do nothing
  } else {
    int64_t N = cur_expr->get_param_count();
    if (OB_FAIL(parent_exprs.push_back(cur_expr))) {
      LOG_WARN("failed to push back", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
      bool param_trans_happened = false;
      ObRawExpr *&child_expr = cur_expr->get_param_expr(i);
      if (OB_FAIL(SMART_CALL(recursive_replace_expr(child_expr,
                                                    parent_exprs,
                                                    const_ctx,
                                                    used_in_compare,
                                                    param_trans_happened)))) {
        LOG_WARN("replace reference column failed", K(ret));
      } else {
        trans_happened |= param_trans_happened;
      }
    }
    if (OB_SUCC(ret)) {
      parent_exprs.pop_back();
    }
  }
  return ret;
}

int ObTransformConstPropagate::replace_internal(ObRawExpr *&cur_expr,
                                                ObIArray<ObRawExpr *> &parent_exprs,
                                                ObIArray<ExprConstInfo> &expr_const_infos,
                                                bool used_in_compare,
                                                bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  if (OB_ISNULL(cur_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid param", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && !trans_happened && i < expr_const_infos.count(); ++i) {
    ObRawExpr *column_expr = expr_const_infos.at(i).column_expr_;
    ObRawExpr *const_expr = expr_const_infos.at(i).const_expr_;
    bool can_replace = false;
    bool need_cast = true;
    if (expr_const_infos.at(i).is_complex_const_info_) {
      //do nothing
    } else if (OB_ISNULL(column_expr) || OB_ISNULL(const_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid expr", K(ret));
    } else if (cur_expr != column_expr) {
      // do nothing
    } else if (expr_const_infos.at(i).mem_equal_) {
      can_replace = true;
    } else if (OB_FAIL(ObTransformUtils::check_can_replace(cur_expr, parent_exprs,
                                                           used_in_compare, can_replace))) {
      LOG_WARN("failed to check can replace", K(ret));
    }

    if (OB_FAIL(ret)) {
    } else if (!can_replace) {
      // do nothing
    } else if (OB_FAIL(check_need_cast_when_replace(cur_expr, const_expr, parent_exprs, need_cast))) {
      LOG_WARN("failed to check need cast", K(ret));
    } else if (need_cast && OB_FAIL(prepare_new_expr(expr_const_infos.at(i)))) {
      LOG_WARN("failed to prepare new expr", K(ret));
    } else {
      cur_expr = need_cast ? expr_const_infos.at(i).new_expr_ : const_expr;
      expr_const_infos.at(i).is_used_ = true;
      trans_happened = true;
      LOG_TRACE("succeed to replace expr", KPC(column_expr), KPC(cur_expr));
    }
  }
  return ret;
}

int ObTransformConstPropagate::prepare_new_expr(ExprConstInfo &const_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(const_info.column_expr_) || OB_ISNULL(const_info.const_expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null expr", K(ret));
  } else if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_) || OB_ISNULL(ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parameter", K(ret));
  } else if (NULL == const_info.new_expr_) {
    ObRawExpr *cast_expr = const_info.const_expr_;
    if (const_info.const_expr_->get_result_type() == const_info.column_expr_->get_result_type()) {
      const_info.new_expr_ = const_info.const_expr_;
    } else if (OB_FAIL(ObTransformUtils::add_cast_for_replace(*ctx_->expr_factory_,
                                                            const_info.column_expr_,
                                                            cast_expr,
                                                            ctx_->session_info_))) {
      LOG_WARN("failed to create cast expr", K(ret));
    } else {
      const_info.new_expr_ = cast_expr;
    }
  }
  return ret;
}

int ObTransformConstPropagate::remove_const_exec_param(ObDMLStmt *stmt)
{
  int ret = OB_SUCCESS;
  bool trans_happened = false;
  ObArray<ObSelectStmt*> child_stmts;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parameter", K(ret));
  } else if (OB_FAIL(stmt->do_formalize_query_ref_exprs_pre())) {
    LOG_WARN("failed to do formalize query ref exprs", K(ret));
  } else if (OB_FAIL(remove_const_exec_param_exprs(stmt,
                                                   trans_happened))) {
    LOG_WARN("replace reference column failed", K(ret));
  } else if (OB_FAIL(stmt->get_child_stmts(child_stmts))) {
    LOG_WARN("failed to get child stmts", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < child_stmts.count(); ++i) {
      ObSelectStmt *child_stmt = child_stmts.at(i);
      if (OB_ISNULL(child_stmt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid child stmt", K(ret));
      } else if (OB_FAIL(SMART_CALL(remove_const_exec_param(child_stmt)))) {
        LOG_WARN("replace reference column failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(stmt->do_formalize_query_ref_exprs_post())) {
    LOG_WARN("failed to do formalize query ref exprs post", K(ret));
  }
  return ret;
}

int ObTransformConstPropagate::remove_const_exec_param_exprs(ObDMLStmt *stmt,
                                                             bool &trans_happened)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExprPointer, 4> exprs;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret), K(stmt));
  } else if (OB_FAIL(stmt->get_relation_exprs(exprs))) {
    LOG_WARN("failed to get exprs", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    bool is_happened = false;
    ObRawExprPointer &expr_ptr = exprs.at(i);
    ObRawExpr *expr = NULL;
    ObSEArray<ObRawExpr *, 8> parent_exprs;
    if (OB_FAIL(expr_ptr.get(expr))) {
      LOG_WARN("failed to get expr", K(ret));
    } else if (OB_FAIL(do_remove_const_exec_param(expr, parent_exprs, is_happened))) {
      LOG_WARN("failed to remove const exec param", K(ret));
    } else if (!is_happened) {
      // do nothing
    } else if (OB_FAIL(expr->formalize(ctx_->session_info_))) {
      LOG_WARN("failed to formalize expr", K(ret));
    } else if (OB_FAIL(expr_ptr.set(expr))) {
      LOG_WARN("failed to update expr", K(ret));
    } else {
      trans_happened = true;
    }
  }
  return ret;
}

int ObTransformConstPropagate::do_remove_const_exec_param(ObRawExpr *&expr,
                                                          ObIArray<ObRawExpr *> &parent_exprs,
                                                          bool &trans_happened)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_)
      || OB_ISNULL(ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (expr->is_exec_param_expr()) {
    ObExecParamRawExpr *exec_param = static_cast<ObExecParamRawExpr *>(expr);
    ObRawExpr *ref_expr = exec_param->get_ref_expr();
    if (OB_ISNULL(ref_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ref expr is null", K(ret));
    } else if (ref_expr->is_static_const_expr()) {
      ObRawExpr *cast_expr = ref_expr;
      bool need_cast = false;
      trans_happened = true;
      if (OB_FAIL(check_need_cast_when_replace(expr, ref_expr, parent_exprs, need_cast))) {
        LOG_WARN("failed to check need cast", K(ret));
      } else if (!need_cast && parent_exprs.count() != 0) {
        expr = ref_expr;
      } else if (OB_FAIL(ObTransformUtils::add_cast_for_replace(*ctx_->expr_factory_,
                                                                expr,
                                                                cast_expr,
                                                                ctx_->session_info_))) {
        LOG_WARN("failed to create cast expr", K(ret));
      } else {
        expr = cast_expr;
      }
    } else if (OB_FAIL(parent_exprs.push_back(exec_param))) {
      LOG_WARN("failed to push back", K(ret));
    } else if (OB_FAIL(SMART_CALL(do_remove_const_exec_param(ref_expr,
                                                             parent_exprs,
                                                             trans_happened)))) {
      LOG_WARN("failed to do remove const exec param", K(ret));
    } else if (OB_FALSE_IT(parent_exprs.pop_back())) {
    } else if (ref_expr->is_const_expr() &&
               !ref_expr->has_flag(CNT_ONETIME)) {
      expr = ref_expr;
      trans_happened = true;
    } else {
      exec_param->set_explicited_reference();
      exec_param->set_ref_expr(ref_expr);
    }
  } else if (expr->has_flag(CNT_DYNAMIC_PARAM)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(parent_exprs.push_back(expr))) {
        LOG_WARN("failed to push back", K(ret));
      } else if (OB_FAIL(do_remove_const_exec_param(expr->get_param_expr(i), parent_exprs,
                                                    trans_happened))) {
        LOG_WARN("failed to remove const exec param", K(ret));
      } else {
        parent_exprs.pop_back();
      }
    }
  }
  return ret;
}

int ObTransformConstPropagate::check_need_cast_when_replace(ObRawExpr *expr,
                                                            ObRawExpr *const_expr,
                                                            ObIArray<ObRawExpr *> &parent_exprs,
                                                            bool &need_cast)
{
  int ret = OB_SUCCESS;
  ObRawExpr *parent_expr = NULL;
  bool is_in_param = false;
  need_cast = true;
  if (parent_exprs.empty()) {
    need_cast = false;
  } else if (OB_ISNULL(parent_expr = parent_exprs.at(parent_exprs.count() - 1)) ||
             OB_ISNULL(const_expr) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (check_is_in_or_notin_param(parent_exprs, is_in_param)) {
    LOG_WARN("failed to check in or notin param", K(ret));
  } else if (is_in_param) {
    // for static engine, all params in rigth side of in or not in should have same type
    need_cast = true;
  } else if (const_expr->get_expr_type() == T_NULL &&
             ObDatumFuncs::is_null_aware_hash_type(expr->get_result_type().get_type())) {
    // To adapt to the behavior of casting NULL values for hash compare
    // cast need to be added above NULL for null aware hash type.
    need_cast = true;
  } else if (parent_expr->is_win_func_expr()) {
    ObWinFunRawExpr *win_expr = static_cast<ObWinFunRawExpr*>(parent_expr);
    ObAggFunRawExpr *agg_expr = win_expr->get_agg_expr();
    ObSEArray<ObRawExpr*,4> agg_params;
    if (OB_NOT_NULL(agg_expr) && OB_FAIL(agg_expr->get_param_exprs(agg_params))) {
      // Type deduction of some aggr funcs relies on not only real params, e.g. MEDIAN, GROUP_PERCENTILE_CONT
      LOG_WARN("failed to get agg params", K(ret));
    } else if (OB_NOT_NULL(agg_expr) && ObOptimizerUtil::find_item(agg_params, expr)) {
      need_cast = true;
    } else if (ObOptimizerUtil::find_item(win_expr->get_func_params(), expr)) {
      need_cast = true;
    } else {
      need_cast = false;
    }
  } else {
    need_cast = !(IS_COMPARISON_OP(parent_expr->get_expr_type()) ||
                  T_OP_ROW == parent_expr->get_expr_type() ||
                  parent_expr->is_query_ref_expr());
  }
  return ret;
}

int ObTransformConstPropagate::check_is_in_or_notin_param(ObIArray<ObRawExpr *> &parent_exprs,
                                                          bool &is_right_param)
{
  int ret = OB_SUCCESS;
  bool found = false;
  is_right_param = false;
  for (int i = parent_exprs.count() - 1; OB_SUCC(ret) && !found && i >= 0; --i) {
    ObRawExpr *cur_expr = parent_exprs.at(i);
    if (OB_ISNULL(cur_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if (T_OP_ROW == cur_expr->get_expr_type()) {
      // check next parent expr
    } else if (T_OP_IN == cur_expr->get_expr_type() || T_OP_NOT_IN == cur_expr->get_expr_type()) {
      found = true;
      if (OB_UNLIKELY(2 != cur_expr->get_param_count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid param count of in expr", KPC(cur_expr), K(ret));
      } else if (i < parent_exprs.count() - 1) {
        is_right_param = cur_expr->get_param_expr(1) == parent_exprs.at(i + 1);
      }
    } else {
      found = true;
    }
  }
  return ret;
}

int ObTransformConstPropagate::check_const_expr_validity(const ObDMLStmt &stmt,
                                                         ExprConstInfo &const_info,
                                                         bool &is_valid)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_const_expr_not_null(stmt, const_info, is_valid))) {
    LOG_WARN("failed to check const expr not null", K(ret));
  } else if (!is_valid) {
    // do nothing
  } else if (OB_FAIL(check_cast_const_expr(const_info, is_valid))) {
    LOG_WARN("failed to check cast const expr", K(ret));
  }

  return ret;
}

int ObTransformConstPropagate::check_const_expr_not_null(const ObDMLStmt &stmt,
                                                         ExprConstInfo &const_info,
                                                         bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  ObRawExpr *const_expr = const_info.const_expr_;
  if (OB_ISNULL(const_expr) ||
      OB_UNLIKELY(!const_expr->is_const_expr()) ||
      OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params are invalid", K(ret), K(const_expr), K(ctx_));
  } else if (const_expr->has_flag(CNT_DYNAMIC_PARAM)) {
    // do nothing
  } else {
    ObNotNullContext not_null_ctx(*ctx_, &stmt);
    ObArray<ObRawExpr *> constraints;
    bool is_not_null = false;
    if (OB_FAIL(ObTransformUtils::is_expr_not_null(not_null_ctx, 
                                                   const_expr, 
                                                   is_not_null, 
                                                   &constraints))) {
      LOG_WARN("failed to check expr not null", K(ret));
    } else if (is_not_null) {
      is_valid = true;
      if (!constraints.empty()) {
        const_info.need_add_constraint_ = PRE_CALC_RESULT_NOT_NULL;
      }
    }
  }
  return ret;
}

int ObTransformConstPropagate::check_cast_const_expr(ExprConstInfo &const_info,
                                                     bool &is_valid)
{
  int ret = OB_SUCCESS;
  bool need_cast = false;
  bool is_scale_adjust_cast = false;
  ObObj value;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->allocator_) || OB_ISNULL(ctx_->session_info_) ||
      OB_ISNULL(const_info.column_expr_) || OB_ISNULL(const_info.const_expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (!const_info.const_expr_->is_static_scalar_const_expr()) {
    is_valid = false;
  } else if (const_info.need_add_constraint_ == PRE_CALC_RESULT_NULL ||
             ObTransformUtils::is_const_null(*const_info.const_expr_)) {
    // do nothing
  } else if (OB_FAIL(ObRawExprUtils::check_need_cast_expr(const_info.const_expr_->get_result_type(),
                                                        const_info.column_expr_->get_result_type(),
                                                        need_cast,
                                                        is_scale_adjust_cast))) {
    LOG_WARN("failed to check need cast expr", K(ret));
    if (OB_ERR_INVALID_TYPE_FOR_OP == ret) {  // rewrite should not happened instead of failed a error
      ret = OB_SUCCESS;
      is_valid = false;
    } else {
      LOG_WARN("failed to check need cast expr", K(ret));
    }
  } else if (!need_cast) {
    // do nothing
  } else if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(ctx_->exec_ctx_,
                                                               const_info.const_expr_,
                                                               value,
                                                               is_valid,
                                                               *ctx_->allocator_))) {
    LOG_WARN("failed to calc const or caculable expr", K(ret));
  } else if (!is_valid) {
    // do nothing
  } else {
    const ObObj *dest_val = NULL;
    const ObExprResType &dst_type = const_info.column_expr_->get_result_type();
    ObDataTypeCastParams dtc_params = ctx_->session_info_->get_dtc_params();
    ObCastCtx cast_ctx(ctx_->allocator_,
                       &dtc_params,
                       CM_WARN_ON_FAIL,
                       dst_type.get_collation_type());
    EXPR_CAST_OBJ_V2(dst_type.get_type(), value, dest_val);
    ObObjType cmp_type = ObMaxType;
    int64_t eq_cmp = 0;
    if (OB_FAIL(ret)) {
      if (OB_ERR_INVALID_DATATYPE == ret) {  // rewrite should not happened instead of failed a error
        ret = OB_SUCCESS;
        is_valid = false;
      } else {
        LOG_WARN("failed to cast obj to dest type", K(ret), K(value), K(dst_type.get_type()));
      }
    } else if (OB_FAIL(ObExprResultTypeUtil::get_relational_cmp_type(cmp_type,
                                                                      value.get_type(),
                                                                      dest_val->get_type()))) {
      LOG_WARN("failed to get compare type", K(ret));
    } else if (OB_FAIL(ObRelationalExprOperator::compare_nullsafe(eq_cmp,
                                                                  value,
                                                                  *dest_val,
                                                                  cast_ctx,
                                                                  cmp_type,
                                                                  dst_type.get_collation_type()))) {
      LOG_WARN("failed to compare obj value", K(ret));
    } else if (eq_cmp != 0) {
      is_valid = false;
    } else {
      is_valid = true;
      // lossless cast constraint contains not null constraint, just overwrite constraint type
      const_info.need_add_constraint_ = PRE_CALC_LOSSLESS_CAST;
    }
  }
  return ret;
}

int ObTransformConstPropagate::check_can_replace_in_select(ObSelectStmt *stmt,
                                                           ObRawExpr *target_expr,
                                                           bool ignore_all_select_exprs,
                                                           bool &can_replace)
{
  int ret = OB_SUCCESS;
  can_replace = true;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parameter", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && can_replace && i < stmt->get_select_items().count(); ++i) {
    ObSEArray<ObRawExpr *, 8> parent_exprs;
    ObRawExpr *&cur_expr = stmt->get_select_items().at(i).expr_;
    if (OB_ISNULL(cur_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid parameter", K(ret));
    } else if (OB_FAIL(recursive_check_can_replace_in_select(cur_expr,
                                                             target_expr,
                                                             parent_exprs,
                                                             ignore_all_select_exprs,
                                                             can_replace))) {
      LOG_WARN("failed to replace select exprs based on equal_pair", K(ret));
    }
  }

  return ret;
}

int ObTransformConstPropagate::recursive_check_can_replace_in_select(ObRawExpr *expr,
                                                            ObRawExpr *target_expr,
                                                            ObIArray<ObRawExpr *> &parent_exprs,
                                                            bool ignore_all_select_exprs,
                                                            bool &can_replace)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr) || OB_ISNULL(target_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid param", K(ret));
  } else if (expr == target_expr) {
    if (ignore_all_select_exprs || parent_exprs.empty()) {
      can_replace = false;
    } else if (OB_FAIL(ObTransformUtils::check_can_replace(expr, parent_exprs,
                                                           false, can_replace))) {
      LOG_WARN("failed to check can replace", K(ret));
    }
  } else if (can_replace && expr->get_param_count() > 0) {
    int64_t N = expr->get_param_count();
    if (OB_FAIL(parent_exprs.push_back(expr))) {
      LOG_WARN("failed to push back", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && can_replace && i < N; ++i) {
      if (OB_FAIL(SMART_CALL(recursive_check_can_replace_in_select(expr->get_param_expr(i),
                                                                   target_expr,
                                                                   parent_exprs,
                                                                   ignore_all_select_exprs,
                                                                   can_replace)))) {
        LOG_WARN("replace reference column failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      parent_exprs.pop_back();
    }
  }
  return ret;
}

int ObTransformConstPropagate::check_set_op_expr_const(ObSelectStmt *stmt,
                                                       const ObSetOpRawExpr* set_op_expr,
                                                       ObRawExpr *&const_expr,
                                                       ObIArray<ObPCParamEqualInfo> &equal_infos)
{
  int ret = OB_SUCCESS;
  int64_t idx = -1;
  const_expr = NULL;
  bool is_valid = true;
  ObStmtCompareContext context;
  if (OB_ISNULL(stmt) || OB_ISNULL(stmt->get_query_ctx())
      || OB_ISNULL(set_op_expr) || OB_UNLIKELY(!stmt->is_set_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid set op expr", K(ret));
  } else if (OB_FALSE_IT(idx = set_op_expr->get_idx())) {
  } else if (idx < 0 || idx >= stmt->get_select_item_size()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("union expr param count is wrong", K(ret), K(idx), K(stmt->get_select_item_size()));
  } else {
    context.init(&stmt->get_query_ctx()->calculable_items_);
    ObIArray<ObSelectStmt*> &child_stmts = stmt->get_set_query();
    ObRawExpr* first_expr = NULL;
    if (T_OP_UNION == set_op_expr->get_expr_type()) {
      // for union, const of children needs to have unique value
      for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < child_stmts.count(); ++i) {
        ObSelectStmt *child = child_stmts.at(i);
        if (OB_ISNULL(child)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid child expr", K(ret));
        } else if (idx >= child->get_select_item_size()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid param for child stmt ", K(ret), K(idx), K(child->get_select_item_size()));
        } else {
          ObRawExpr* expr = child->get_select_item(idx).expr_;
          if (OB_ISNULL(expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid expr", K(ret));
          } else if (!expr->is_const_expr()) {
            is_valid = false;
          } else if (i == 0) {
            first_expr = expr;
          } else if (OB_NOT_NULL(first_expr) && !expr->same_as(*first_expr, &context)) {
            is_valid = false;
          } else if (OB_FAIL(append(equal_infos, context.equal_param_info_))) {
            LOG_WARN("failed to append equal param map", K(ret));
          } else {
            context.equal_param_info_.reset();
          }
        }
      }
    } else if (T_OP_INTERSECT == set_op_expr->get_expr_type()) {
      // for intersect, only need to check any child is const and no need to maintain constriant
      for (int64_t i = 0; OB_SUCC(ret) && NULL == first_expr && i < child_stmts.count(); ++i) {
        ObSelectStmt *child = child_stmts.at(i);
        if (OB_ISNULL(child)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid child expr", K(ret));
        } else if (idx >= child->get_select_item_size()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid param for child stmt ", K(ret), K(idx), K(child->get_select_item_size()));
        } else {
          ObRawExpr* expr = child->get_select_item(idx).expr_;
          if (OB_ISNULL(expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid expr", K(ret));
          } else if (!expr->is_const_expr()) {
            // do nothing
          } else {
            first_expr = expr;
          }
        }
      }
    } else if (T_OP_EXCEPT == set_op_expr->get_expr_type()) {
      // for except, only need to check the first child is const and no need to maintain constriant
      ObSelectStmt *child = child_stmts.at(0);
      if (OB_ISNULL(child)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid child expr", K(ret));
      } else if (idx >= child->get_select_item_size()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid param for child stmt ", K(ret), K(idx), K(child->get_select_item_size()));
      } else {
        ObRawExpr* expr = child->get_select_item(idx).expr_;
        if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid expr", K(ret));
        } else if (!expr->is_const_expr()) {
          // do nothing
        } else {
          first_expr = expr;
        }
      }
    }
    if (OB_SUCC(ret) && is_valid) {
      const_expr = first_expr;
    }
  }
  return ret;
}

int ObTransformConstPropagate::is_parent_null_side(ObDMLStmt *&parent_stmt,
                                                   ObDMLStmt *&stmt,
                                                   bool &is_on_null_side)
{
  int ret = OB_SUCCESS;
  is_on_null_side = false;
  if (OB_ISNULL(parent_stmt) || OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parameter", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !is_on_null_side && i < parent_stmt->get_table_size(); ++i) {
      TableItem *table_item = NULL;
      if (OB_ISNULL(table_item = parent_stmt->get_table_item(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table item is null", K(ret), K(i), K(table_item));
      } else if (!table_item->is_generated_table() ||
                 table_item->ref_query_ != stmt) {
        // do nothing
      } else if (OB_FAIL(ObOptimizerUtil::is_table_on_null_side(parent_stmt,
                                                                table_item->table_id_,
                                                                is_on_null_side))) {
        LOG_WARN("failed to check null side", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformConstPropagate::collect_equal_param_constraints(ObIArray<ExprConstInfo> &expr_const_infos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid stmt", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < expr_const_infos.count(); ++i) {
    if (!expr_const_infos.at(i).is_used_) {
      // do nothing
    } else if (OB_FAIL(append(ctx_->equal_param_constraints_,
                              expr_const_infos.at(i).equal_infos_))) {
      LOG_WARN("failed to append equal param constraint", K(ret));
    } else if (expr_const_infos.at(i).is_complex_const_info_) {
      if (OB_UNLIKELY(expr_const_infos.at(i).multi_need_add_constraints_.count() !=
                      expr_const_infos.at(i).multi_const_exprs_.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(ret), K(expr_const_infos));
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < expr_const_infos.at(i).multi_const_exprs_.count(); ++j) {
        if (OB_FAIL(add_equal_param_constraint(expr_const_infos.at(i).column_expr_,
                                      expr_const_infos.at(i).multi_const_exprs_.at(j),
                                      expr_const_infos.at(i).multi_need_add_constraints_.at(j)))) {
          LOG_WARN("failed to add equal param constraint", K(ret));
        }
      }
    } else if (OB_FAIL(add_equal_param_constraint(expr_const_infos.at(i).column_expr_,
                                                  expr_const_infos.at(i).const_expr_,
                                                  expr_const_infos.at(i).need_add_constraint_))) {
      LOG_WARN("failed to add equal param constraint", K(ret));
    }
  }
  return ret;
}

int ObTransformConstPropagate::add_equal_param_constraint(ObRawExpr *column_expr,
                                                          ObRawExpr *const_expr,
                                                          PreCalcExprExpectResult expect_result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(column_expr) || OB_ISNULL(const_expr) || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (PRE_CALC_RESULT_NONE == expect_result) {
    // do nothing
  } else if (PRE_CALC_RESULT_NULL == expect_result) {
    if (OB_FAIL(ObTransformUtils::add_param_null_constraint(*ctx_, const_expr))) {
      LOG_WARN(" fail to add param null constraint");
    }
  } else if (PRE_CALC_RESULT_NOT_NULL == expect_result) {
    if (OB_FAIL(ObTransformUtils::add_param_not_null_constraint(*ctx_, const_expr))) {
      LOG_WARN("failed to add param not null constraint", K(ret));
    }
  } else if (PRE_CALC_LOSSLESS_CAST == expect_result) {
    if (OB_FAIL(ObTransformUtils::add_param_lossless_cast_constraint(*ctx_,
                                                                     const_expr,
                                                                     column_expr))) {
      LOG_WARN("failed to add param not null constraint", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected expect result type", K(expect_result), K(ret));
  }
  return ret;
}

int ObTransformConstPropagate::recursive_collect_equal_pair_from_condition(ObDMLStmt *stmt,
                                                                           ObRawExpr *expr,
                                                                           ConstInfoContext &const_ctx,
                                                                           bool &trans_happened)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(stmt), K(expr));
  } else if (T_OP_EQ == expr->get_expr_type()) {
    // todo: support general expr const info instead of column expr
    ObRawExpr *param_1 = expr->get_param_expr(0);
    ObRawExpr *param_2 = expr->get_param_expr(1);
    ObRawExpr *column_expr = NULL;
    ObRawExpr *const_expr = NULL;
    bool is_valid = true;
    if (OB_ISNULL(param_1) || OB_ISNULL(param_2)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid param", K(ret));
    } else if (param_1->is_column_ref_expr() &&
                param_2->is_const_expr()) {
      column_expr = param_1;
      const_expr = param_2;
    } else if (param_2->is_column_ref_expr() &&
                param_1->is_const_expr()) {
      column_expr = param_2;
      const_expr = param_1;
    } else {
      is_valid = false;
    }
    if (OB_SUCC(ret) && is_valid) {
      ExprConstInfo new_info;
      new_info.column_expr_ = column_expr;
      new_info.const_expr_ = const_expr;
      if (OB_FAIL(check_const_expr_validity(*stmt, new_info, is_valid))) {
        LOG_WARN("failed to check const expr validity", K(ret));
      } else if (!is_valid) {
        // do nothing
      } else {
        new_info.exclude_expr_ = expr;
        if (OB_FAIL(const_ctx.add_const_info(new_info))) {
          LOG_WARN("failed to push back", K(ret));
        }
      }
    }
  } else if (T_OP_AND == expr->get_expr_type()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      bool is_happened = false;
      if (OB_FAIL(SMART_CALL(recursive_collect_equal_pair_from_condition(stmt,
                                                                         expr->get_param_expr(i),
                                                                         const_ctx,
                                                                         trans_happened)))) {
        LOG_WARN("failed to recursive collect const info from condition", K(ret));
      } else {
        trans_happened |= is_happened;
      }
    }
  } else if (T_OP_IN == expr->get_expr_type()) {
    ObRawExpr *l_expr = NULL;
    ObRawExpr *r_expr = NULL;
    if (OB_UNLIKELY(2 != expr->get_param_count()) ||
        OB_ISNULL(l_expr = expr->get_param_expr(0)) ||
        OB_ISNULL(r_expr = expr->get_param_expr(1))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(l_expr), K(r_expr), K(ret));
    } else if (l_expr->is_column_ref_expr() && T_OP_ROW == r_expr->get_expr_type()) {
      bool is_valid = true;
      ExprConstInfo new_info;
      new_info.column_expr_ = l_expr;
      new_info.is_complex_const_info_ = true;
      for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < r_expr->get_param_count(); ++i) {
        new_info.const_expr_ = r_expr->get_param_expr(i);
        if (OB_ISNULL(r_expr->get_param_expr(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret), K(r_expr));
        } else if (!r_expr->get_param_expr(i)->is_const_expr()) {
          is_valid = false;
        } else if (OB_FAIL(check_const_expr_validity(*stmt,
                                                     new_info,
                                                     is_valid))) {
          LOG_WARN("failed to check const expr validity", K(ret));
        } else if (!is_valid) {
          /*do nothing*/
        } else if (OB_FAIL(new_info.multi_const_exprs_.push_back(r_expr->get_param_expr(i)))) {
          LOG_WARN("failed to push back", K(ret));
        } else if (OB_FAIL(new_info.multi_need_add_constraints_.push_back(new_info.need_add_constraint_))) {
          LOG_WARN("failed to push back", K(ret));
        }
      }
      if (OB_SUCC(ret) && is_valid) {
        if (OB_FAIL(const_ctx.add_const_info(new_info))) {
          LOG_WARN("failed to push back", K(ret));
        }
      }
    }
  } else if (T_OP_OR == expr->get_expr_type()) {
    ObArray<ExprConstInfo> complex_infos;
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      ConstInfoContext tmp_ctx(const_ctx.shared_expr_checker_, const_ctx.allow_trans_);
      bool child_happened = false;
      bool current_happened = false;
      if (OB_FAIL(SMART_CALL(recursive_collect_equal_pair_from_condition(stmt,
                                                                         expr->get_param_expr(i),
                                                                         tmp_ctx,
                                                                         child_happened)))) {
        LOG_WARN("failed to recursive collect const info from condition", K(ret));
      } else if (OB_FAIL(replace_expr_internal(expr->get_param_expr(i),
                                               tmp_ctx,
                                               current_happened))) {
        LOG_WARN("failed to replace expr", K(ret));
      } else {
        trans_happened |= child_happened || current_happened;
      }
      if (OB_SUCC(ret)) {
        if (0 == i) {
          if (OB_FAIL(complex_infos.assign(tmp_ctx.active_const_infos_))) {
            LOG_WARN("failed to assign complex infos", K(ret));
          }
        } else if (OB_FAIL(merge_complex_const_infos(complex_infos, tmp_ctx.active_const_infos_))) {
          LOG_WARN("failed to merge complex const infos", K(ret));
        }
      }
      if (OB_SUCC(ret) && (child_happened || current_happened)) {
        if (OB_FAIL(const_ctx.merge_expired_const_infos(tmp_ctx, false))) {
          LOG_WARN("failed to merge expired const infos", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(const_ctx.add_const_infos(complex_infos))) {
        LOG_WARN("failed to append complex infos", K(ret));
      }
    }
  } else if (T_OP_IS == expr->get_expr_type()) {
    ObRawExpr *param_1 = expr->get_param_expr(0);
    ObRawExpr *param_2 = expr->get_param_expr(1);
    if (OB_ISNULL(param_1) || OB_ISNULL(param_2)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null ptr", K(param_1), K(param_2), K(ret));
    } else if (param_1->is_column_ref_expr() &&
                ObNullType == param_2->get_result_type().get_type()) {
      ExprConstInfo new_info;
      new_info.column_expr_ = param_1;
      new_info.const_expr_ = param_2;
      new_info.exclude_expr_ = expr;
      if (OB_FAIL(const_ctx.add_const_info(new_info))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformConstPropagate::merge_complex_const_infos(ObIArray<ExprConstInfo> &cur_const_infos,
                                                         ObIArray<ExprConstInfo> &new_const_infos)
{
  int ret = OB_SUCCESS;
  ObSEArray<ExprConstInfo, 4> tmp_const_infos;
  for (int64_t i = 0; OB_SUCC(ret) && i < cur_const_infos.count(); ++i) {
    bool found = false;
    ObRawExpr *param_expr = cur_const_infos.at(i).column_expr_;
    if (OB_ISNULL(param_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    }
    for (int64_t j = 0; OB_SUCC(ret) && !found && j < new_const_infos.count(); ++j) {
      if (param_expr == new_const_infos.at(j).column_expr_) {
        found = true;
        ExprConstInfo new_info;
        if (OB_FAIL(merge_const_info(cur_const_infos.at(i), new_const_infos.at(j), new_info))) {
          LOG_WARN("failed to merge const info", K(ret));
        } else if (OB_FAIL(tmp_const_infos.push_back(new_info))) {
          LOG_WARN("failed to push back", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(cur_const_infos.assign(tmp_const_infos))) {
      LOG_WARN("failed to assign const infos", K(ret));
    }
  }

  return ret;
}

int ObTransformConstPropagate::merge_const_info(ExprConstInfo &const_info_l,
                                                ExprConstInfo &const_info_r,
                                                ExprConstInfo &new_info)
{
  int ret = OB_SUCCESS;
  new_info.column_expr_ = const_info_l.column_expr_;
  new_info.is_complex_const_info_ = true;
  if (OB_FAIL(new_info.merge_complex(const_info_l))) {
    LOG_WARN("failed to merge const info left", K(ret));
  } else if (OB_FAIL(new_info.merge_complex(const_info_r))) {
    LOG_WARN("failed to merge const info right", K(ret));
  }
  return ret;
}

int ObTransformConstPropagate::replace_check_constraint_exprs(ObDMLStmt *stmt,
                                                              ConstInfoContext &const_ctx,
                                                              bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(stmt));
  } else if (!const_ctx.allow_trans_) {
    /* do nothing */
  } else {
    LOG_TRACE("begin replace check constraint exprs", K(const_ctx), K(stmt->get_check_constraint_items()));
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_check_constraint_items().count(); ++i) {
      ObDMLStmt::CheckConstraintItem &item = stmt->get_check_constraint_items().at(i);
      for (int64_t j = 0; OB_SUCC(ret) && j < item.check_constraint_exprs_.count(); ++j) {
        ObRawExpr *check_constraint_expr = item.check_constraint_exprs_.at(j);
        bool is_valid = false;
        ObSEArray<ObRawExpr*, 16> old_column_exprs;
        ObSEArray<ObRawExpr*, 16> new_const_exprs;
        ObRawExpr *part_column_expr = NULL;
        int64_t complex_cst_info_idx = -1;
        if (OB_ISNULL(check_constraint_expr) ||
            OB_UNLIKELY(item.check_constraint_exprs_.count() != item.check_flags_.count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret), K(item));
        } else if (!(item.check_flags_.at(j) & ObDMLStmt::CheckConstraintFlag::IS_VALIDATE_CHECK) &&
                   !(item.check_flags_.at(j) & ObDMLStmt::CheckConstraintFlag::IS_RELY_CHECK)) {
          //do nothing
        } else if (OB_FAIL(check_constraint_expr_validity(check_constraint_expr,
                                                          stmt->get_part_exprs(),
                                                          const_ctx.active_const_infos_,
                                                          part_column_expr,
                                                          old_column_exprs,
                                                          new_const_exprs,
                                                          complex_cst_info_idx,
                                                          is_valid))) {
          LOG_WARN("failed to check constraint expr validity", K(ret));
        } else if (!is_valid) {
          //do nothing
        } else if (OB_FAIL(do_replace_check_constraint_expr(stmt,
                                                            check_constraint_expr,
                                                            const_ctx.active_const_infos_,
                                                            part_column_expr,
                                                            old_column_exprs,
                                                            new_const_exprs,
                                                            complex_cst_info_idx,
                                                            trans_happened))) {
          LOG_WARN("failed to do replace check constraint expr", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTransformConstPropagate::check_constraint_expr_validity(ObRawExpr *check_constraint_expr,
                                                              const ObIArray<ObDMLStmt::PartExprItem> &part_items,
                                                              ObIArray<ExprConstInfo> &expr_const_infos,
                                                              ObRawExpr *&part_column_expr,
                                                              ObIArray<ObRawExpr*> &old_column_exprs,
                                                              ObIArray<ObRawExpr*> &new_const_exprs,
                                                              int64_t &complex_cst_info_idx,
                                                              bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  if (OB_ISNULL(check_constraint_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(check_constraint_expr));
  } else if (T_OP_EQ == check_constraint_expr->get_expr_type()) {
    ObRawExpr *l_expr = check_constraint_expr->get_param_expr(0);
    ObRawExpr *r_expr = check_constraint_expr->get_param_expr(1);
    bool is_happened = false;
    if (OB_ISNULL(l_expr) || OB_ISNULL(r_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(l_expr), K(r_expr));
    } else if (l_expr->is_column_ref_expr() &&
               OB_FAIL(do_check_constraint_param_expr_vaildity(l_expr,
                                                               r_expr,
                                                               part_items,
                                                               expr_const_infos,
                                                               old_column_exprs,
                                                               new_const_exprs,
                                                               complex_cst_info_idx,
                                                               is_valid))) {
      LOG_WARN("failed to do check constraint param expr vaildity", K(ret));
    } else if (is_valid) {
      part_column_expr = l_expr;
    } else if (r_expr->is_column_ref_expr() &&
               OB_FAIL(do_check_constraint_param_expr_vaildity(r_expr,
                                                               l_expr,
                                                               part_items,
                                                               expr_const_infos,
                                                               old_column_exprs,
                                                               new_const_exprs,
                                                               complex_cst_info_idx,
                                                               is_valid))) {
      LOG_WARN("failed to do check constraint param expr vaildity", K(ret));
    } else if (is_valid) {
      part_column_expr = r_expr;
    }
    LOG_TRACE("Succeed check constraint expr validity", KPC(check_constraint_expr), K(is_valid));
  }
  return ret;
}
/*if the check constraint expr is valid:
 * 1. equal expr which the column is in left or right param expr and the column is partition column;
 * 2. the column which in the non-partition column expr should be can replaced const expr;
 * 3. if the non-partition column expr is string sys func, it‘s insensitive to collation.
*/
int ObTransformConstPropagate::do_check_constraint_param_expr_vaildity(
                                                              ObRawExpr *column_param_expr,
                                                              ObRawExpr *non_column_param_expr,
                                                              const ObIArray<ObDMLStmt::PartExprItem> &part_items,
                                                              ObIArray<ExprConstInfo> &expr_const_infos,
                                                              ObIArray<ObRawExpr*> &old_column_exprs,
                                                              ObIArray<ObRawExpr*> &new_const_exprs,
                                                              int64_t &complex_cst_info_idx,
                                                              bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  if (OB_ISNULL(column_param_expr) || OB_ISNULL(non_column_param_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(column_param_expr), K(non_column_param_expr), K(ret));
  } else {
    //check rule 1
    for (int64_t i = 0; !is_valid && i < part_items.count(); ++i) {
      is_valid = column_param_expr == part_items.at(i).part_expr_ ||
                 column_param_expr == part_items.at(i).subpart_expr_;
    }
    if (is_valid) {
      bool is_found = false;
      for (int64_t i = 0; !is_found && i < expr_const_infos.count(); ++i) {
        is_found = (column_param_expr == expr_const_infos.at(i).column_expr_);
      }
      is_valid = !is_found;
    }
    //check rule 2
    if (is_valid) {
      ObSEArray<ObRawExpr *, 8> parent_exprs;
      if (OB_FAIL(ObRawExprUtils::extract_column_exprs(non_column_param_expr, old_column_exprs))) {
        LOG_WARN("failed to extract column exprs", K(ret));
      } else if (OB_FAIL(check_all_const_propagate_column(old_column_exprs,
                                                          expr_const_infos,
                                                          new_const_exprs,
                                                          complex_cst_info_idx,
                                                          is_valid))) {
        LOG_WARN("failed to check all const propagate column", K(ret));
      } else if (!is_valid) {
        //do nothing
      } else if (complex_cst_info_idx >= 0
              && expr_const_infos.at(complex_cst_info_idx).multi_const_exprs_.count() > 10) {
        is_valid = false; //do not deduce big in
      //check rule 3
      } else if (OB_FAIL(recursive_check_non_column_param_expr_validity(non_column_param_expr,
                                                                        parent_exprs,
                                                                        is_valid))) {
        LOG_WARN("failed to recursive check non column param expr validity", K(ret));
      } else {
        LOG_TRACE("succeed to do check constraint param expr vaildity", KPC(column_param_expr),
                KPC(non_column_param_expr), K(expr_const_infos), K(part_items), K(old_column_exprs),
                K(old_column_exprs), K(new_const_exprs), K(complex_cst_info_idx), K(is_valid));
      }
    }
  }
  return ret;
}

int ObTransformConstPropagate::check_constraint_value_validity(ObRawExpr *value_expr, bool &reject)
{
  int ret = OB_SUCCESS;
  reject = false;
  ObObj result;
  bool got_result = false;
  if (OB_ISNULL(value_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", K(ret));
  } else if (OB_FAIL(value_expr->extract_info())) {
    LOG_WARN("extract info failed", K(ret));
  } else if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(ctx_->exec_ctx_,
                                                        value_expr,
                                                        result,
                                                        got_result,
                                                        *ctx_->allocator_))) {
    LOG_WARN("calc const or calculable expr failed", K(ret));
  } else if (!got_result) {
    reject = true;
    //uncalculable
  } else if (result.is_null()) {
    reject = true;
    //violate null reject, can not add new condition
  }
  return ret;
}

int ObTransformConstPropagate::check_all_const_propagate_column(ObIArray<ObRawExpr*> &column_exprs,
                                                                ObIArray<ExprConstInfo> &expr_const_infos,
                                                                ObIArray<ObRawExpr*> &new_const_exprs,
                                                                int64_t &const_info_idx,
                                                                bool &is_all)
{
  int ret = OB_SUCCESS;
  const_info_idx = -1;
  is_all = !column_exprs.empty();
  int64_t remove_id = -1;
  for (int64_t i = 0; OB_SUCC(ret) && is_all && i < column_exprs.count(); ++i) {
    bool is_found = false;
    for (int64_t j = 0; OB_SUCC(ret) && !is_found && j < expr_const_infos.count(); ++j) {
      if (column_exprs.at(i) == expr_const_infos.at(j).column_expr_) {
        if (expr_const_infos.at(j).is_complex_const_info_) {
          if (const_info_idx != -1) {
            is_found = (const_info_idx == j);
          } else {
            is_found = true;
            const_info_idx = j;
            remove_id = i;
          }
        } else if (OB_FAIL(prepare_new_expr(expr_const_infos.at(j)))) {
          LOG_WARN("failed to create new expr", K(ret));
        } else if (OB_FAIL(new_const_exprs.push_back(expr_const_infos.at(j).new_expr_))) {
          LOG_WARN("failed to push back", K(ret));
        } else {
          is_found = true;
        }
      }
    }
    is_all = is_found;
  }
  if (const_info_idx != -1 && remove_id != -1) {
    if (OB_FAIL(column_exprs.remove(remove_id))) {
      LOG_WARN("failed to remove", K(ret));
    }
  }
  LOG_TRACE("check all const propagate column", K(column_exprs), K(const_info_idx),
                                                K(expr_const_infos), K(new_const_exprs), K(is_all));
  return ret;
}

int ObTransformConstPropagate::recursive_check_non_column_param_expr_validity(ObRawExpr *expr,
                                                                ObIArray<ObRawExpr *> &parent_exprs,
                                                                bool &is_valid)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(expr), K(ret));
  } else if (expr->is_column_ref_expr()) {
    if (OB_FAIL(ObTransformUtils::check_can_replace(expr, parent_exprs, true, is_valid))) {
      LOG_WARN("failed to check can replace", K(ret));
    }
  } else if (expr->get_param_count() > 0) {
    if (OB_FAIL(parent_exprs.push_back(expr))) {
      LOG_WARN("failed to push back", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(recursive_check_non_column_param_expr_validity(expr->get_param_expr(i),
                                                                            parent_exprs,
                                                                            is_valid)))) {
        LOG_WARN("replace reference column failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      parent_exprs.pop_back();
    }
  }
  return ret;
}

int ObTransformConstPropagate::do_replace_check_constraint_expr(ObDMLStmt *stmt,
                                                                ObRawExpr *check_constraint_expr,
                                                                ObIArray<ExprConstInfo> &expr_const_infos,
                                                                ObRawExpr *part_column_expr,
                                                                ObIArray<ObRawExpr*> &old_column_exprs,
                                                                ObIArray<ObRawExpr*> &new_const_exprs,
                                                                int64_t &complex_cst_info_idx,
                                                                bool &trans_happened)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt) || OB_ISNULL(check_constraint_expr) || OB_ISNULL(part_column_expr) ||
      OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_) ||
      OB_UNLIKELY(old_column_exprs.count() != new_const_exprs.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(check_constraint_expr), K(stmt), K(part_column_expr),
                                     K(ctx_), K(old_column_exprs), K(new_const_exprs));
  } else {
    bool reject = false;
    ObRawExpr *new_check_cst_expr = NULL;
    ObRawExpr *value_expr = NULL;
    ObRawExprCopier copier(*ctx_->expr_factory_);
    ObSEArray<ObRawExpr *, 4> not_null_values;
    if (complex_cst_info_idx >= 0) {//need generate in condition for complex const(or/in expr)
      if (OB_UNLIKELY(complex_cst_info_idx > expr_const_infos.count() ||
                      !expr_const_infos.at(complex_cst_info_idx).is_complex_const_info_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(ret), K(complex_cst_info_idx), K(expr_const_infos));
      } else if (OB_FAIL(build_new_in_condition_expr(check_constraint_expr,
                                                     expr_const_infos.at(complex_cst_info_idx),
                                                     part_column_expr,
                                                     old_column_exprs,
                                                     new_const_exprs,
                                                     new_check_cst_expr,
                                                     not_null_values,
                                                     reject))) {
        LOG_WARN("failed to build new in condition expr", K(ret));
      } else if (reject) {
      } else {
        expr_const_infos.at(complex_cst_info_idx).is_used_ = true;
      }
    } else if (OB_FAIL(copier.add_replaced_expr(old_column_exprs, new_const_exprs))) {
      LOG_WARN("failed to add replace pair", K(ret));
    } else if (OB_FAIL(copier.add_skipped_expr(part_column_expr))) {
      LOG_WARN("failed to add skipped expr", K(ret));
    } else if (OB_FAIL(copier.copy(check_constraint_expr, new_check_cst_expr))) {
      LOG_WARN("failed to copy expr", K(ret));
    } else if (OB_ISNULL(new_check_cst_expr) || OB_UNLIKELY(new_check_cst_expr->get_param_count() < 2)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error", K(ret), KPC(new_check_cst_expr));
    } else if (OB_ISNULL(value_expr = (part_column_expr == new_check_cst_expr->get_param_expr(0) ?
                                                          new_check_cst_expr->get_param_expr(1) :
                                                          new_check_cst_expr->get_param_expr(0)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("value expr is null", K(ret));
    } else if (OB_FAIL(not_null_values.push_back(value_expr))) {
      LOG_WARN("push back failed", K(ret));
    } else if (OB_FAIL(check_constraint_value_validity(value_expr, reject))) {
      LOG_WARN("ensure check cst failed", K(ret));
    } else {/*do nothing*/}

    if (OB_SUCC(ret) && !reject) {
      ObRawExpr *or_expr = NULL;
      ObRawExpr *part_col_is_null = NULL;
      ObSEArray<ObRawExpr *, 2> or_expr_children;
      if (OB_FAIL(ObRawExprUtils::build_is_not_null_expr(*ctx_->expr_factory_, part_column_expr, false, part_col_is_null))) {
        LOG_WARN("build is null failed", K(ret));
      } else if (OB_FAIL(or_expr_children.push_back(part_col_is_null))) {
        LOG_WARN("push back failed", K(ret));
      } else if (OB_FAIL(or_expr_children.push_back(new_check_cst_expr))) {
        LOG_WARN("push back failed", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::build_or_exprs(*ctx_->expr_factory_, or_expr_children, or_expr))) {
        LOG_WARN("build or exprs failed", K(ret));
      } else {
        new_check_cst_expr = or_expr;
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(batch_mark_expr_const_infos_used(old_column_exprs, expr_const_infos))) {
        LOG_WARN("failed to batch mark_expr_const_infos_used", K(ret));
      } else if (OB_FAIL(stmt->get_condition_exprs().push_back(new_check_cst_expr))) {
        LOG_WARN("failed to push back", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < not_null_values.count(); i++) {
          if (OB_FAIL(ObTransformUtils::add_param_not_null_constraint(*ctx_, not_null_values.at(i)))) {
            LOG_WARN("add not null cst failed", K(ret));
          }
        }
        trans_happened = true;
        LOG_TRACE("Succeed to do replace check constraint expr", KPC(new_check_cst_expr));
      }
    }
  }
  return ret;
}

int ObTransformConstPropagate::build_new_in_condition_expr(ObRawExpr *check_constraint_expr,
                                                           ExprConstInfo &expr_const_info,
                                                           ObRawExpr *part_column_expr,
                                                           ObIArray<ObRawExpr*> &old_column_exprs,
                                                           ObIArray<ObRawExpr*> &new_const_exprs,
                                                           ObRawExpr *&new_condititon_expr,
                                                           ObIArray<ObRawExpr*> &not_null_values,
                                                           bool &reject)
{
  int ret = OB_SUCCESS;
  new_condititon_expr = NULL;
  ObRawExpr *non_part_column_expr = NULL;
  if (OB_ISNULL(check_constraint_expr) || OB_ISNULL(part_column_expr) || OB_ISNULL(ctx_) ||
      OB_ISNULL(ctx_->expr_factory_) || OB_ISNULL(ctx_->session_info_) ||
      OB_UNLIKELY(!expr_const_info.is_complex_const_info_ ||
                  expr_const_info.multi_const_exprs_.empty() ||
                  check_constraint_expr->get_param_count() != 2)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), KPC(check_constraint_expr), K(part_column_expr),
                                    K(ctx_), K(expr_const_info));
  } else {
    ObOpRawExpr *in_expr = NULL;
    ObOpRawExpr *row_expr = NULL;
    ObRawExpr *non_part_column_expr = NULL;
    if (part_column_expr == check_constraint_expr->get_param_expr(0)) {
      non_part_column_expr = check_constraint_expr->get_param_expr(1);
    } else if (part_column_expr == check_constraint_expr->get_param_expr(1)) {
      non_part_column_expr = check_constraint_expr->get_param_expr(0);
    }
    if (OB_ISNULL(non_part_column_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(ret), K(non_part_column_expr), KPC(check_constraint_expr));
    } else if (OB_FAIL(ctx_->expr_factory_->create_raw_expr(T_OP_IN, in_expr)) ||
               OB_FAIL(ctx_->expr_factory_->create_raw_expr(T_OP_ROW, row_expr))) {
      LOG_WARN("create add expr failed", K(ret), K(in_expr), K(row_expr));
    } else if (OB_ISNULL(in_expr) || OB_ISNULL(row_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("add expr is null", K(ret), K(in_expr), K(row_expr));
    } else if (OB_FAIL(in_expr->add_param_expr(part_column_expr))) {
      LOG_WARN("failed to add param expr", K(ret));
    } else if (OB_FAIL(in_expr->add_param_expr(row_expr))) {
      LOG_WARN("failed to add param expr", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && !reject && i < expr_const_info.multi_const_exprs_.count(); ++i) {
        ObRawExpr *new_param_expr = NULL;
        ObRawExprCopier copier(*ctx_->expr_factory_);
        if (OB_FAIL(old_column_exprs.push_back(expr_const_info.column_expr_))) {
          LOG_WARN("failed to push back", K(ret));
        } else if (OB_FAIL(new_const_exprs.push_back(expr_const_info.multi_const_exprs_.at(i)))) {
          LOG_WARN("failed to push back", K(ret));
        } else if (OB_FAIL(copier.add_replaced_expr(old_column_exprs, new_const_exprs))) {
          LOG_WARN("failed to add replace pair", K(ret));
        } else if (OB_FAIL(copier.copy(non_part_column_expr, new_param_expr))) {
          LOG_WARN("failed to copy expr", K(ret));
        } else if (OB_FAIL(row_expr->add_param_expr(new_param_expr))) {
          LOG_WARN("failed to add param expr", K(ret));
        } else if (OB_FAIL(not_null_values.push_back(new_param_expr))) {
          LOG_WARN("failed to add param expr", K(ret));
        } else if (OB_FAIL(check_constraint_value_validity(new_param_expr, reject))) {
          LOG_WARN("ensure check cst failed", K(ret));
        } else {
          old_column_exprs.pop_back();
          new_const_exprs.pop_back();
        }
      }
      if (OB_SUCC(ret) && !reject) {
        if (OB_FAIL(in_expr->formalize(ctx_->session_info_))) {
          LOG_WARN("failed to formalize", K(ret));
        } else {
          new_condititon_expr = in_expr;
          LOG_TRACE("Succeed to build new in condition expr", KPC(new_condititon_expr));
        }
      }
    }
  }
  return ret;
}

int ObTransformConstPropagate::batch_mark_expr_const_infos_used(ObIArray<ObRawExpr*> &column_exprs,
                                                                ObIArray<ExprConstInfo> &expr_const_infos)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < column_exprs.count(); ++i) {
    bool is_found = false;
    for (int64_t j = 0; OB_SUCC(ret) && !is_found && j < expr_const_infos.count(); ++j) {
      if (column_exprs.at(i) == expr_const_infos.at(j).column_expr_) {
        if (expr_const_infos.at(j).is_complex_const_info_) {
          is_found = true;
        } else {
          expr_const_infos.at(j).is_used_ = true;
          is_found = true;
        }
      }
    }
    if (OB_UNLIKELY(!is_found)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(ret), K(column_exprs), K(expr_const_infos));
    }
  }
  return ret;
}

int ObTransformConstPropagate::generate_pullup_const_info(ObSelectStmt *stmt,
                                                          ObRawExpr *expr,
                                                          ConstInfoContext &const_ctx)
{
  int ret = OB_SUCCESS;
  bool found = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parameter", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && !found && i < const_ctx.active_const_infos_.count(); ++i) {
    ExprConstInfo &cur_info = const_ctx.active_const_infos_.at(i);
    ObRawExpr *column_expr = cur_info.column_expr_;
    ObRawExpr *const_expr = cur_info.const_expr_;
    if (cur_info.is_complex_const_info_) {
      //do nothing
    } else if (OB_ISNULL(column_expr) || OB_ISNULL(const_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid expr", K(ret));
    } else if (expr == cur_info.column_expr_) {
      found = true;
      ObIArray<PullupConstInfo> *pullup_const_infos = NULL;
      PullupConstInfo pullup_info;
      pullup_info.column_expr_ = cur_info.column_expr_;
      pullup_info.const_expr_ = cur_info.const_expr_;
      pullup_info.need_add_constraint_ = cur_info.is_used_ ? cur_info.need_add_constraint_ :
                                                             PRE_CALC_RESULT_NONE;
      if (OB_FAIL(pullup_info.equal_infos_.assign(cur_info.equal_infos_))) {
        LOG_WARN("failed to assign equal infos", K(ret));
      } else if (OB_FAIL(acquire_pullup_infos(stmt, pullup_const_infos))) {
        LOG_WARN("failed to acquire pullup const infos", K(ret));
      } else if (OB_ISNULL(pullup_const_infos)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to acquire pullup const infos", K(ret));
      } else if (OB_FAIL(pullup_const_infos->push_back(pullup_info))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformConstPropagate::acquire_pullup_infos(ObDMLStmt *stmt,
                                                    ObIArray<PullupConstInfo> *&pullup_infos)
{
  int ret = OB_SUCCESS;
  int64_t index = -1;
  const uint64_t key = reinterpret_cast<const uint64_t>(stmt);
  pullup_infos = NULL;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parameter", K(ret));
  } else if (OB_SUCCESS != (ret = stmt_map_.get_refactored(key, index))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get stmt index", K(ret));
    }
  } else if (OB_UNLIKELY(index >= stmt_pullup_const_infos_.count() || index < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid index", K(index), K(ret));
  } else {
    pullup_infos = stmt_pullup_const_infos_.at(index);
  }
  if (OB_SUCC(ret) && NULL == pullup_infos) {
    PullupConstInfos *new_infos = NULL;
    index = stmt_pullup_const_infos_.count();
    if (OB_ISNULL(new_infos = (PullupConstInfos *) allocator_.alloc(sizeof(PullupConstInfos)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate pullup predicates array", K(ret));
    } else {
      new_infos = new (new_infos) PullupConstInfos();
      if (OB_FAIL(stmt_pullup_const_infos_.push_back(new_infos))) {
        LOG_WARN("failed to push back predicates", K(ret));
      } else if (OB_FAIL(stmt_map_.set_refactored(key, index))) {
        LOG_WARN("failed to add entry info hash map", K(ret));
      } else {
        pullup_infos = new_infos;
      }
    }
  }
  return ret;
}

int ObTransformConstPropagate::collect_from_pullup_const_infos(ObDMLStmt *stmt,
                                                               ObRawExpr *expr,
                                                               ExprConstInfo &equal_info)
{
  int ret = OB_SUCCESS;
  int64_t index = -1;
  const uint64_t key = reinterpret_cast<const uint64_t>(stmt);
  ObIArray<PullupConstInfo> *pullup_infos = NULL;
  if (OB_ISNULL(stmt) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parameter", K(ret));
  } else if (OB_SUCCESS != (ret = stmt_map_.get_refactored(key, index))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get stmt index", K(ret));
    }
  } else if (OB_UNLIKELY(index >= stmt_pullup_const_infos_.count() || index < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid index", K(index), K(ret));
  } else {
    pullup_infos = stmt_pullup_const_infos_.at(index);
  }
  if (OB_SUCC(ret) && NULL != pullup_infos) {
    bool found = false;
    for (int64_t i = 0; OB_SUCC(ret) && !found && i < pullup_infos->count(); ++i) {
      PullupConstInfo &pullup_info = pullup_infos->at(i);
      if (expr == pullup_info.column_expr_) {
        found = true;
        equal_info.const_expr_ = pullup_info.const_expr_;
        equal_info.need_add_constraint_ = pullup_info.need_add_constraint_;
        if (OB_FAIL(equal_info.equal_infos_.assign(pullup_info.equal_infos_))) {
          LOG_WARN("failed to assign equal infos", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTransformConstPropagate::replace_select_exprs_skip_agg(ObSelectStmt *stmt,
                                                    ConstInfoContext &const_ctx,
                                                    bool &trans_happened)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 8> parent_exprs;
  trans_happened = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parameter", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_select_items().count(); ++i) {
    ObRawExpr *&cur_expr = stmt->get_select_items().at(i).expr_;
    parent_exprs.reuse();
    bool internal_trans_happened = false;
    if (OB_ISNULL(cur_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid parameter", K(ret));
    } else if (cur_expr->is_column_ref_expr()) {
      //do nothing, const infos have been pulluped befored collect having const infos
    } else if (OB_FAIL(replace_select_exprs_skip_agg_internal(cur_expr,
                                                          const_ctx,
                                                          parent_exprs,
                                                          internal_trans_happened,
                                                          false))) {
      LOG_WARN("failed to replace select exprs based on equal_pair", K(ret));
    } else {
      trans_happened |= internal_trans_happened;
    }
  }
  return ret;
}

int ObTransformConstPropagate::replace_select_exprs_skip_agg_internal(ObRawExpr *&cur_expr,
                                                                      ConstInfoContext &const_ctx,
                                                                      ObIArray<ObRawExpr *> &parent_exprs,
                                                                      bool &trans_happened,
                                                                      bool used_in_compare)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  if (OB_ISNULL(cur_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parameter", K(ret));
  } else if (!cur_expr->has_flag(CNT_AGG)) {
    bool is_happened = false;
    if (OB_FAIL(recursive_replace_expr(cur_expr,
                                      parent_exprs,
                                      const_ctx,
                                      used_in_compare,
                                      is_happened))) {
      LOG_WARN("failed to recursive");
    } else {
      trans_happened |= is_happened;
    }
  } else if (cur_expr->is_aggr_expr()) {
    // do nothing
  } else {
    int64_t N = cur_expr->get_param_count();
    if (OB_FAIL(parent_exprs.push_back(cur_expr))) {
      LOG_WARN("failed to push back", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
      bool param_trans_happened = false;
      if (OB_FAIL(SMART_CALL(replace_select_exprs_skip_agg_internal(cur_expr->get_param_expr(i),
                                                                    const_ctx,
                                                                    parent_exprs,
                                                                    param_trans_happened,
                                                                    used_in_compare)))) {
        LOG_WARN("fail to replace scalar select exprs internal", K(ret));
      } else {
        trans_happened |= param_trans_happened;
      }
    }
    if (OB_SUCC(ret)) {
      parent_exprs.pop_back();
    }
  }
  return ret;
}

// Check if all non-consts in T_OP_ROW can be replaced to consts.
int ObTransformConstPropagate::check_can_replace_child_of_row(ConstInfoContext &const_ctx,
                                                              ObRawExpr *&cur_expr,
                                                              bool &can_replace_child)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cur_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (cur_expr->is_const_expr() || !const_ctx.allow_trans_) {
    can_replace_child = false;
  } else {
    ObSEArray<ObRawExpr*, 4> const_cols;
    bool is_const_recursively = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < const_ctx.active_const_infos_.count(); i++) {
      if (OB_FAIL(const_cols.push_back(const_ctx.active_const_infos_.at(i).column_expr_))) {
        LOG_WARN("failed to push back expr", K(ret));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(ObOptimizerUtil::is_const_expr_recursively(cur_expr,
                                                                           const_cols,
                                                                           is_const_recursively))) {
      LOG_WARN("failed to check const expr recursively", K(ret));
    } else {
      can_replace_child &= is_const_recursively;
    }
  }
  return ret;
}

}// namespace sql
} // namespace oceanbase
