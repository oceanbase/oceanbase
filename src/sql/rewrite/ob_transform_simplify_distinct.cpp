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
#include "sql/rewrite/ob_transform_simplify_distinct.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "common/ob_smart_call.h"

using namespace oceanbase::sql;

int ObTransformSimplifyDistinct::transform_one_stmt(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                                    ObDMLStmt *&stmt,
                                                    bool &trans_happened)
{
  int ret = OB_SUCCESS;
  bool is_const_distinct = false;
  bool is_unique_distinct = false;
  ObSelectStmt *sel_stmt = static_cast<ObSelectStmt *>(stmt);
  UNUSED(parent_stmts);
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (!stmt->is_select_stmt()) {
    // do nothing
    OPT_TRACE("not select stmt");
  } else if (OB_FAIL(remove_distinct_on_const_exprs(sel_stmt, is_const_distinct))) {
    LOG_WARN("failed to remove distinct for const exprs", K(ret));
  } else if (OB_FAIL(remove_distinct_on_unique_exprs(sel_stmt, is_unique_distinct))) {
    LOG_WARN("failed to remove distinct for unique exprs", K(ret));
  } else {
    trans_happened = is_const_distinct || is_unique_distinct;
  }
  if (OB_SUCC(ret) && trans_happened) {
    if (OB_FAIL(add_transform_hint(*stmt))) {
      LOG_WARN("failed to add transform hint", K(ret));
    }
  }
  return ret;
}

int ObTransformSimplifyDistinct::remove_distinct_on_const_exprs(ObSelectStmt *stmt, bool &trans_happened)
{
  int ret = OB_SUCCESS;
  bool is_valid = false;
  trans_happened = false;
  ObConstRawExpr *limit_count_expr = NULL;
  OPT_TRACE("try to remove distinct on const exprs");
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is unexpected null", K(ret), K(stmt), K(ctx_));
  } else if (OB_FAIL(distinct_can_be_eliminated(stmt, is_valid))) {
    LOG_WARN("distinct_can_be_eliminated() fails unexpectedly", K(ret));
  } else if (!is_valid) {
    // do nothing
    OPT_TRACE("can not eliminate");
  } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*ctx_->expr_factory_,
                                                          ObIntType,
                                                          1L,
                                                          limit_count_expr))) {
    LOG_WARN("Failed to create expr", K(ret));
  } else if (OB_ISNULL(limit_count_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("limit_count_expr is null", K(ret));
  } else if (OB_FAIL(limit_count_expr->formalize(ctx_->session_info_))) {
    LOG_WARN("failed to formalize a new expr", K(ret));
  } else {
    // Eliminate DISTINCT and create a `LIMIT 1`
    stmt->set_limit_offset(limit_count_expr, NULL);
    stmt->assign_all();
    trans_happened = true;
  }
  return ret;
}

int ObTransformSimplifyDistinct::distinct_can_be_eliminated(ObSelectStmt *stmt, bool &can_be)
{
  int ret = OB_SUCCESS;
  can_be = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->exec_ctx_) ||
      OB_ISNULL(ctx_->exec_ctx_->get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(stmt), K(ctx_));
  } else if (stmt->is_contains_assignment() ||
             stmt->is_calc_found_rows()) {
    // Do nothing for non-select query.
    // When there are `@var := ` assignment, don't eliminate distinct.
    OPT_TRACE("stmt has assignment or calc found rows");
  } else if (stmt->has_distinct() && !stmt->is_set_stmt() && stmt->get_from_item_size() > 0 &&
             !stmt->has_rollup()) {
    // Only try to eliminate DISTINCT for plain SELECT
    int64_t limit_count = 0;
    const ObRawExpr *limit_offset_expr = stmt->get_offset_expr();
    bool is_null_value = false;
    if (OB_FAIL(ObTransformUtils::get_expr_int_value(stmt->get_limit_expr(),
                                                    &ctx_->exec_ctx_->get_physical_plan_ctx()->get_param_store(),
                                                    ctx_->exec_ctx_,
                                                    ctx_->allocator_,
                                                    limit_count,
                                                    is_null_value))) {
      LOG_WARN("failed to get limit int value", K(ret));
    } else if (!stmt->has_limit() || (limit_offset_expr == NULL && limit_count > 0)) {
      bool contain_only = true;
      EqualSets &equal_sets = ctx_->equal_sets_;
      ObArenaAllocator alloc;
      ObSEArray<ObRawExpr *, 4> const_exprs;
      const ObIArray<ObRawExpr *> &conditions = stmt->get_condition_exprs();
      if (OB_FAIL(stmt->get_stmt_equal_sets(equal_sets, alloc, true, true))) {
        LOG_WARN("failed to get stmt equal sets", K(ret));
      } else if (OB_FAIL(ObOptimizerUtil::compute_const_exprs(conditions, const_exprs))) {
        LOG_WARN("failed to compute const equivalent exprs", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && contain_only && i < stmt->get_select_item_size(); ++i) {
        ObRawExpr *expr = stmt->get_select_item(i).expr_;
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
      } else { /* Do nothing */ }
    } else { /* Do nothing */ }
  } else { /* Do nothing */ }
  return ret;
}

int ObTransformSimplifyDistinct::remove_distinct_on_unique_exprs(ObSelectStmt *stmt, bool &trans_happened)
{
  int ret = OB_SUCCESS;
  bool is_unique = false;
  ObSEArray<ObRawExpr *, 16> select_exprs;
  OPT_TRACE("try to remove distinct on unique exprs");
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(stmt), K(ctx_));
  } else if (!stmt->has_distinct()) {
    // do nothing
    OPT_TRACE("stmt do not has distinct");
  } else if (OB_FAIL(stmt->get_select_exprs(select_exprs))) {
    LOG_WARN("failed to get select exprs", K(ret));
  } else if (OB_FAIL(ObTransformUtils::check_stmt_unique(stmt, ctx_->session_info_,
                                                         ctx_->schema_checker_,
                                                         select_exprs,
                                                         true /* strict */,
                                                         is_unique, true))) {
    LOG_WARN("failed to check stmt unique", K(ret));
  } else if (is_unique) {
    stmt->assign_all();
    trans_happened = true;
  } else {
    OPT_TRACE("select expr is not unique, can not eliminate");
  }

  if (OB_SUCC(ret) && stmt->is_set_stmt()) {
    OPT_TRACE("try to remove child stmt`s distinct for set stmt");
    if (OB_FAIL(remove_child_stmt_distinct(stmt, trans_happened))) {
      LOG_WARN("failed to remove child stmt distinct", K(ret));
    }
  }
  return ret;
}

//消除distinct set op left/right query中的distinct
int ObTransformSimplifyDistinct::remove_child_stmt_distinct(ObSelectStmt *set_stmt,
                                                            bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  if (OB_ISNULL(set_stmt) || !set_stmt->is_set_stmt()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected stmt", K(ret));
  } else if (!set_stmt->is_set_distinct() ||
             ObSelectStmt::RECURSIVE == set_stmt->get_set_op()) {
    /*do nothing*/
    OPT_TRACE("union all or recurisve union can not remove distinct");
  } else {
    bool child_happended = false;
    ObIArray<ObSelectStmt*> &child_stmts = set_stmt->get_set_query();
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

//递归消除disticnt条件：
//  1.stmt 为非 recursive 的 set op, 无 limit, 尝试向下递归删除
//  2.stmt 非 set, 无 sequence/limit/rownum, 可消除distinct
int ObTransformSimplifyDistinct::try_remove_child_stmt_distinct(ObSelectStmt *stmt,
                                                                bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(stmt));
  } else if (!stmt->is_set_stmt()) {
    bool has_rownum = false;
    if (!stmt->has_distinct()
        || stmt->has_sequence()
        || stmt->has_limit()) {
      /*do nothing*/
      OPT_TRACE("stmt do not has distinct or has sequence/limit");
    } else if (OB_FAIL(stmt->has_rownum(has_rownum))) {
      LOG_WARN("failed to check has rownum", K(ret));
    } else if (!has_rownum) {
      stmt->assign_all();
      trans_happened = true;
    } else {
      OPT_TRACE("stmt has rownum");
    }
  } else if (ObSelectStmt::RECURSIVE == stmt->get_set_op() || stmt->has_limit()) {
    /*do nothing*/
    OPT_TRACE("is recursive union or stmt has limit");
  } else {
    bool child_happended = false;
    ObIArray<ObSelectStmt*> &child_stmts = stmt->get_set_query();
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
