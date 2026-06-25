/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_REWRITE
#include "ob_transform_subquery_unnest.h"

#include "sql/ob_sql_utils.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "sql/resolver/dml/ob_update_stmt.h"
#include "sql/resolver/expr/ob_raw_expr_info_extractor.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "sql/rewrite/ob_transform_utils.h"

namespace oceanbase
{
using namespace common;

namespace sql
{

// Choose traversal strategy based on optimizer compat version:
//   >= 4.6.1: PRE_ORDER  — unnest parent first (better nested subquery unnest)
//   <  4.6.1: POST_ORDER — visit children first (legacy behavior)
int ObTransformSubqueryUnnest::transform(ObDMLStmt *&stmt, uint64_t &transform_types)
{
  if (is_enable_pre_order_unnest(stmt)) {
    set_transform_method(TransMethod::PRE_ORDER);
  } else {
    set_transform_method(TransMethod::POST_ORDER);
  }
  return ObTransformRule::transform(stmt, transform_types);
}

/*
 * Main entry — two independent phases, run in sequence:
 *
 *   stmt ──► anyall_transform_subquery    scans WHERE conditions only
 *        │     EXISTS / NOT EXISTS / =ANY / =ALL  →  semi / anti / inner join
 *        │
 *        ──► singlerow_transform_subquery scans WHERE + post-join exprs
 *              scalar subquery            →  inner / outer join
 *
 */
int ObTransformSubqueryUnnest::transform_one_stmt(ObIArray<ObParentDMLStmt> &parent_stmts,
                                                  ObDMLStmt *&stmt,
                                                  bool &trans_happened)
{
  int ret = OB_SUCCESS;
  UNUSED(parent_stmts);
  bool is_anyall_happened = false;
  bool is_single_row_happened = false;
  trans_happened = false;
  ObSEArray<ObSelectStmt *, 4> unnest_stmts;
  stmt_ = stmt;
  if (OB_FAIL(anyall_transform_subquery(unnest_stmts, is_anyall_happened))) {
    LOG_WARN("failed to transform any all query", K(ret));
  } else if (OB_FAIL(singlerow_transform_subquery(unnest_stmts, is_single_row_happened))) {
    LOG_WARN("failed to transform single row query", K(ret));
  }
  if (OB_SUCC(ret)) {
    trans_happened = is_anyall_happened || is_single_row_happened;
    LOG_TRACE("succeed to transformer where subquery",
              K(is_anyall_happened),
              K(is_single_row_happened),
              K(trans_happened),
              K(get_trans_happened()));
    if (trans_happened && OB_FAIL(add_transform_hint(*stmt, &unnest_stmts))) {
      LOG_WARN("failed to add transform hint", K(ret));
    }
  }
  return ret;
}

int ObTransformSubqueryUnnest::transform_one_stmt_with_outline(
  ObIArray<ObParentDMLStmt> &parent_stmts,
  ObDMLStmt *&stmt,
  bool &trans_happened)
{
  int ret = OB_SUCCESS;
  UNUSED(parent_stmts);
  trans_happened = false;
  bool is_happened = false;
  stmt_ = stmt;
  if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(ctx_));
  } else {
    do {
      is_happened = false;
      ObSEArray<ObSelectStmt *, 4> unnest_stmts;
      if (OB_SUCC(ret) && !is_happened
          && OB_FAIL(anyall_transform_subquery(unnest_stmts, is_happened))) {
        LOG_WARN("failed to transform any all query", K(ret));
      }
      if (OB_SUCC(ret) && !is_happened
          && OB_FAIL(singlerow_transform_subquery(unnest_stmts, is_happened))) {
        LOG_WARN("failed to transform single row query", K(ret));
      }
      if (OB_SUCC(ret)) {
        if (is_happened) {
          ctx_->trans_list_loc_ += unnest_stmts.count();
          trans_happened = true;
          if (OB_FAIL(add_transform_hint(*stmt, &unnest_stmts))) {
            LOG_WARN("failed to add transform hint", K(ret));
          }
          LOG_TRACE("succeed to do subquery pullup with outline", K(ctx_->src_qb_name_));
        } else {
          LOG_TRACE("can not do subquery pullup with outline", K(ctx_->src_qb_name_));
        }
      }
    } while (OB_SUCC(ret) && is_happened);
  }
  return ret;
}

int ObTransformSubqueryUnnest::check_hint_status(const ObDMLStmt &stmt, bool &need_trans)
{
  int ret = OB_SUCCESS;
  need_trans = false;
  const ObQueryHint *query_hint = NULL;
  const ObHint *cur_trans_hint = NULL;
  if (OB_ISNULL(ctx_) || OB_ISNULL(query_hint = stmt.get_stmt_hint().query_hint_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(ctx_), K(query_hint));
  } else if (!query_hint->has_outline_data()) {
    need_trans = true;
  } else if (NULL == (cur_trans_hint = query_hint->get_outline_trans_hint(ctx_->trans_list_loc_))
             || !cur_trans_hint->is_unnest_hint()) {
    /*do nothing*/
  } else {
    ObQueryRefRawExpr *subquery_expr = NULL;
    ObSelectStmt *select_stmt = NULL;
    for (int64_t i = 0; !need_trans && OB_SUCC(ret) && i < stmt.get_subquery_expr_size(); ++i) {
      if (OB_ISNULL(subquery_expr = stmt.get_subquery_exprs().at(i))
          || OB_ISNULL(select_stmt = subquery_expr->get_ref_stmt())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(subquery_expr), K(select_stmt));
      } else {
        need_trans = query_hint->is_valid_outline_transform(ctx_->trans_list_loc_,
                                                            get_hint(select_stmt->get_stmt_hint()));
      }
    }
  }
  return ret;
}

int ObTransformSubqueryUnnest::construct_transform_hint(ObDMLStmt &stmt, void *trans_params)
{
  int ret = OB_SUCCESS;
  ObIArray<ObSelectStmt *> *unnested_stmts = NULL;
  UNUSED(stmt);
  if (OB_ISNULL(ctx_)
      || OB_ISNULL(ctx_->allocator_)
      || OB_ISNULL(trans_params)
      || OB_ISNULL(unnested_stmts = static_cast<ObIArray<ObSelectStmt *> *>(trans_params))
      || OB_UNLIKELY(unnested_stmts->empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(ctx_), K(trans_params), K(unnested_stmts));
  } else {
    ObHint *hint = NULL;
    ObDMLStmt *child_stmt = NULL;
    ObString child_qb_name;
    const ObHint *myhint = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < unnested_stmts->count(); ++i) {
      if (OB_ISNULL(child_stmt = unnested_stmts->at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret), K(child_stmt));
      } else if (OB_FAIL(ObQueryHint::create_hint(ctx_->allocator_, T_UNNEST, hint))) {
        LOG_WARN("failed to create hint", K(ret));
      } else if (OB_FAIL(child_stmt->get_qb_name(child_qb_name))) {
        LOG_WARN("failed to get qb name", K(ret), K(child_stmt->get_stmt_id()));
      } else if (OB_FAIL(ctx_->add_src_hash_val(child_qb_name))) {
        LOG_WARN("failed to add src hash val", K(ret));
      } else if (OB_FAIL(ctx_->outline_trans_hints_.push_back(hint))) {
        LOG_WARN("failed to push back hint", K(ret));
      } else if (NULL != (myhint = get_hint(child_stmt->get_stmt_hint())) && myhint->is_enable_hint()
                 && OB_FAIL(ctx_->add_used_trans_hint(myhint))) {
        LOG_WARN("failed to add used trans hint", K(ret));
      } else {
        hint->set_qb_name(child_qb_name);
      }
    }
  }
  return ret;
}

int ObTransformSubqueryUnnest::check_hint_allowed_unnest(const ObDMLStmt &stmt,
                                                         const ObSelectStmt &subquery,
                                                         const int64_t outline_loc,
                                                         bool &allowed)
{
  int ret = OB_SUCCESS;
  allowed = true;
  const ObQueryHint *query_hint = NULL;
  const ObHint *myhint = get_hint(subquery.get_stmt_hint());
  bool is_disable = NULL != myhint && myhint->is_disable_hint();
  const ObHint *no_rewrite1 = stmt.get_stmt_hint().get_no_rewrite_hint();
  const ObHint *no_rewrite2 = subquery.get_stmt_hint().get_no_rewrite_hint();
  if (OB_ISNULL(ctx_) || OB_ISNULL(query_hint = stmt.get_stmt_hint().query_hint_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(ctx_), K(query_hint));
  } else if (query_hint->has_outline_data()) {
    allowed = query_hint->is_valid_outline_transform(outline_loc, myhint);
  } else if (NULL != myhint && myhint->is_enable_hint()) {
    allowed = true;
  } else if (is_disable || NULL != no_rewrite1 || NULL != no_rewrite2) {
    allowed = false;
    if (OB_FAIL(ctx_->add_used_trans_hint(no_rewrite1))) {
      LOG_WARN("failed to add used trans hint", K(ret));
    } else if (OB_FAIL(ctx_->add_used_trans_hint(no_rewrite2))) {
      LOG_WARN("failed to add used trans hint", K(ret));
    } else if (is_disable && OB_FAIL(ctx_->add_used_trans_hint(myhint))) {
      LOG_WARN("failed to add used trans hint", K(ret));
    }
  }
  return ret;
}

// All transformations of SubQueryUnnest must check basic validity for stmt, do not rewrite if:
// 1. stmt has no subquery
// 2. stmt has no from item
// 3. stmt is hierarchical for update
// 4. stmt is unpivot select
int ObTransformSubqueryUnnest::check_basic_validity_for_stmt(const ObDMLStmt *stmt,
                                                             bool &is_valid) const
{
  int ret = OB_SUCCESS;
  is_valid = true;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(stmt));
  } else if (!stmt->has_subquery()) {
    is_valid = false;
    OPT_TRACE("stmt has no subquery");
  } else if (0 == stmt->get_from_item_size()) {
    // do not unnest subquery if stmt has no from item
    is_valid = false;
    OPT_TRACE("stmt has no from item");
  } else if (bool is_hsfu = false; OB_FAIL(stmt->is_hierarchical_for_update(is_hsfu))) {
    LOG_WARN("failed to check hierarchical for update", K(ret), K(stmt));
  } else if (is_hsfu) {
    is_valid = false;
    OPT_TRACE("stmt is hierarchical for update");
  } else if (stmt->is_unpivot_select()) {
    is_valid = false;
    OPT_TRACE("stmt is unpivot select");
  }
  return ret;
}

int ObTransformSubqueryUnnest::check_subquery_unique(ObSelectStmt *subquery, bool &is_unique)
{
  ObSEArray<ObRawExpr *, 4> const_exprs;
  return check_subquery_unique(subquery, const_exprs, is_unique);
}

// Check whether the subquery output is unique, considering exprs whose values are determined.
//
// @param[in]     subquery   The subquery to check.
// @param[in,out] const_exprs On input, exprs the caller already knows that are consts
//                            (e.g. IN-subquery select items that will become equi-join keys).
//                            On output, also includes const exprs determined by where conds.
//                            The combined set is passed to check_stmt_unique to help prove uniqueness.
// @param[out]    is_unique  true if the subquery output is unique after accounting for all
//                           determined exprs; false otherwise.
int ObTransformSubqueryUnnest::check_subquery_unique(ObSelectStmt *subquery,
                                                     ObIArray<ObRawExpr *> &const_exprs,
                                                     bool &is_unique)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(subquery)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("subquery is null", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::compute_const_exprs(subquery->get_condition_exprs(), const_exprs))) {
    LOG_WARN("failed to compute const exprs", K(ret));
  } else if (OB_FAIL(ObTransformUtils::check_stmt_unique(subquery,
                                                         ctx_->session_info_,
                                                         ctx_->schema_checker_,
                                                         const_exprs,
                                                         true, /* is_strict */
                                                         is_unique))) {
    LOG_WARN("failed to check stmt unique", K(ret));
  }
  return ret;
}

/*
 * Merge a subquery's tables, columns, conditions, semi infos into the parent stmt.
 */
int ObTransformSubqueryUnnest::pullup_subquery_to_parent_stmt(ObSelectStmt *subquery,
                                                              bool use_outer_join)
{
  int ret = OB_SUCCESS;
  // 1. pullup table items & column items
  if (OB_ISNULL(stmt_) || OB_ISNULL(subquery)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL pointer error", K(ret), K_(stmt), K(subquery));
  } else if (OB_FAIL(append(stmt_->get_table_items(), subquery->get_table_items()))) {
    LOG_WARN("failed to append table items", K(ret));
  } else if (OB_FAIL(append(stmt_->get_joined_tables(), subquery->get_joined_tables()))) {
    LOG_WARN("failed to append joined tables", K(ret));
  } else if (OB_FAIL(append(stmt_->get_column_items(), subquery->get_column_items()))) {
    LOG_WARN("failed to append column items", K(ret));
  } else if (OB_FAIL(stmt_->rebuild_tables_hash())) {
    LOG_WARN("failed to rebuild table hash", K(ret));
  } else if (OB_FAIL(stmt_->update_column_item_rel_id())) {
    LOG_WARN("failed to update column item rel id", K(ret));
  }

  // 2. pullup from items & condition exprs (if use inner join)
  if (OB_FAIL(ret)) {
  } else if (!use_outer_join) {
    if (OB_FAIL(append(stmt_->get_from_items(), subquery->get_from_items()))) {
      LOG_WARN("failed to append from items", K(ret));
    } else if (OB_FAIL(append(stmt_->get_condition_exprs(), subquery->get_condition_exprs()))) {
      LOG_WARN("failed to append condition exprs", K(ret));
    }
  } else {  // use_outer_join (LEFT_OUTER_JOIN)
    TableItem *left_table = NULL;
    TableItem *right_table = NULL;
    TableItem *joined_table = NULL;
    if (OB_FAIL(ObTransformUtils::merge_from_items_as_inner_join(ctx_, *stmt_, left_table))) {
      LOG_WARN("failed to merge from items ad inner join", K(ret));
    } else if (OB_FAIL(ObTransformUtils::merge_from_items_as_inner_join(ctx_,
                                                                        *subquery,
                                                                        right_table))) {
      LOG_WARN("failed to merge from items as inner join", K(ret));
    } else if (OB_FAIL(ObTransformUtils::add_new_joined_table(ctx_,
                                                              *stmt_,
                                                              LEFT_OUTER_JOIN,
                                                              left_table,
                                                              right_table,
                                                              subquery->get_condition_exprs(),
                                                              joined_table))) {
      LOG_WARN("failed to add new joined table", K(ret));
    } else if (FALSE_IT(stmt_->get_from_items().reset())) {
    } else if (OB_FAIL(stmt_->add_from_item(joined_table->table_id_, true))) {
      LOG_WARN("failed to add from items", K(ret));
    }
  }

  // 3. pullup semi infos
  //    The subquery being pulled up may contain semi infos
  //    whose semi conds can reference outer tables through exec-params,
  //    which have now been decorrelated into real column refs just now。
  //    We should update semi info's left_table_ids_ to include those outer tables after unnest.
  for (int64_t i = 0; OB_SUCC(ret) && i < subquery->get_semi_info_size(); ++i) {
    SemiInfo *semi = NULL;
    ObSEArray<ObRawExpr *, 4> columns;
    if (OB_ISNULL(semi = subquery->get_semi_infos().at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("semi info is null", K(ret), K(semi));
    } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(semi->semi_conditions_, columns))) {
      LOG_WARN("failed to extract column exprs", K(ret));
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < columns.count(); ++j) {
      uint64_t table_id = OB_INVALID_ID;
      if (OB_ISNULL(columns.at(j)) || OB_UNLIKELY(!columns.at(j)->is_column_ref_expr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column is null", K(ret));
      } else if (FALSE_IT(table_id = static_cast<ObColumnRefRawExpr *>(columns.at(j))->get_table_id())) {
      // table id already marked as semi left/right table, do nothing
      } else if (semi->right_table_id_ == table_id || ObOptimizerUtil::find_item(semi->left_table_ids_, table_id)) {
      // new table id (must belong to upper stmt), add into left_table_ids_
      } else if (OB_FAIL(semi->left_table_ids_.push_back(table_id))) {
        LOG_WARN("failed to push back left table id", K(ret));
      }
    }
  }
  if (FAILEDx(append(stmt_->get_semi_infos(), subquery->get_semi_infos()))) {
    LOG_WARN("failed to append semi infos", K(ret));
  }

  // 4. pullup other necessary infos
  if (FAILEDx(append(stmt_->get_part_exprs(), subquery->get_part_exprs()))) {
    LOG_WARN("failed to append part expr", K(ret));
  } else if (OB_FAIL(append(stmt_->get_check_constraint_items(), subquery->get_check_constraint_items()))) {
    LOG_WARN("failed to append check constraint items", K(ret));
  } else if (OB_FAIL(stmt_->get_stmt_hint().merge_stmt_hint(subquery->get_stmt_hint(), LEFT_HINT_DOMINATED))) {
    LOG_WARN("failed to merge subquery stmt hint", K(ret));
  }
  return ret;
}

// ============================== begin of anyall subquery ==============================

/*
 * Anyall entry — scan WHERE conditions for EXISTS / IN / =ANY / =ALL subqueries.
 *
 * For each condition:
 *   1. anyall_gather_transform_params  — classify and validate
 *   2. anyall_do_unnest_subquery       — rewrite
 *
 * Examples of supported rewrites:
 *
 *   -- EXISTS → left semi join
 *   WHERE EXISTS (SELECT 1 FROM t2 WHERE t1.c1 = t2.c1)
 *   →  t1 LEFT SEMI JOIN t2 ON t1.c1 = t2.c1
 *
 *   -- NOT IN → left anti join
 *   WHERE c1 NOT IN (SELECT c2 FROM t2)
 *   →  t1 LEFT ANTI JOIN (SELECT c2 FROM t2) ON c1 != c2 OR c1 IS NULL OR c2 IS NULL
 *
 *   -- OR expansion (see anyall_collect_or_branch_infos for details)
 *   WHERE c1 IN (SELECT c2 FROM t2 WHERE ...) OR c1 = 100
 *   →  t1 LEFT SEMI JOIN (SELECT c2 FROM t2 ... UNION ALL SELECT 100) v ON t1.c1 = v.c2
 */
int ObTransformSubqueryUnnest::anyall_transform_subquery(ObIArray<ObSelectStmt *> &unnest_stmts,
                                                         bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  ObSEArray<ObRawExpr *, 16> conditions;
  if (OB_ISNULL(stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_ is null", K(ret), K_(stmt));
  } else if (bool is_valid = false; OB_FAIL(check_basic_validity_for_stmt(stmt_, is_valid))) {
    LOG_WARN("failed to check preliminary validity", K(ret));
  } else if (!is_valid) {
    // do nothing
  } else if (OB_FAIL(conditions.assign(stmt_->get_condition_exprs()))) {
    LOG_WARN("failed to assign a new condition exprs", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < conditions.count(); ++i) {
      AnyAllSQParam trans_param;
      trans_param.outline_loc_ = ctx_->trans_list_loc_ + unnest_stmts.count();
      ObSelectStmt *orig_subquery = NULL; // for constructing UNNEST(@"SEL$N") hint
      if (OB_FAIL(anyall_gather_transform_params(conditions.at(i), trans_param))) {
        LOG_WARN("failed to check can be pulled up ", K(conditions.at(i)), K_(stmt), K(ret));
      } else if (!trans_param.can_transform_) {
        // do nothing
      } else if (OB_ISNULL(orig_subquery = trans_param.get_subquery())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("subquery is null", K(ret));
      } else if (OB_FAIL(anyall_do_unnest_subquery(conditions.at(i), trans_param))) {
        LOG_WARN("failed to unnest anyall subquery", K(ret));
      } else if (!trans_param.or_subquery_stmts_.empty()) {
        // OR expansion: push original branch subquery stmts (not the UNION ALL stmt)
        // so that construct_transform_hint generates per-branch UNNEST(@"SEL$N") hints
        if (OB_FAIL(append(unnest_stmts, trans_param.or_subquery_stmts_))) {
          LOG_WARN("failed to append OR branch stmts", K(ret));
        } else {
          trans_happened = true;
        }
      } else if (OB_FAIL(unnest_stmts.push_back(orig_subquery))) {
        LOG_WARN("failed to push back", K(ret));
      } else {
        trans_happened = true;
      }
    }
  }
  return ret;
}

int ObTransformSubqueryUnnest::anyall_gather_transform_params(ObRawExpr *expr,
                                                              AnyAllSQParam &trans_param)
{
  int ret = OB_SUCCESS;
  OPT_TRACE("try to pullup subquery expr:", expr);
  if (OB_ISNULL(stmt_) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL pointer error", K_(stmt), K(expr), K(ret));
  } else if (is_enable_anyall_unnest_homogeneous_or_cond(stmt_)
             && T_OP_OR == expr->get_expr_type() && expr->has_flag(CNT_SUB_QUERY)) {
    ObSEArray<OrBranchInfo, 4> branches;
    ObRawExpr *expanded_expr = NULL;
    if (OB_FAIL(anyall_collect_or_branch_infos(expr, trans_param, branches))) {
      LOG_WARN("failed to classify OR branches", K(ret));
    } else if (OB_FAIL(anyall_check_validity_for_or_branches(expr, trans_param, branches))) {
      LOG_WARN("failed to check OR branches validity", K(ret));
    } else if (!trans_param.can_transform_) {
      // do nothing
    } else if (OB_FAIL(anyall_expand_or_cond(branches, expanded_expr))) {
      LOG_WARN("failed to build expanded or cond", K(ret));
    } else if (OB_ISNULL(expanded_expr)
               || OB_UNLIKELY(!expanded_expr->has_flag(IS_WITH_ANY))
               || OB_ISNULL(expanded_expr->get_param_expr(0))
               || OB_ISNULL(expanded_expr->get_param_expr(1))
               || OB_UNLIKELY(!expanded_expr->get_param_expr(1)->is_query_ref_expr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected expanded_expr", K(ret), K(expanded_expr));
    } else {
      trans_param.expr_ = static_cast<ObOpRawExpr *>(expanded_expr);
      trans_param.subquery_expr_ = static_cast<ObQueryRefRawExpr *>(expanded_expr->get_param_expr(1));
    }
  } else {
    if (T_OP_EXISTS == expr->get_expr_type() || T_OP_NOT_EXISTS == expr->get_expr_type()) {
      if (OB_ISNULL(expr->get_param_expr(0))
          || OB_UNLIKELY(!expr->get_param_expr(0)->is_query_ref_expr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid (not) exist predicate", K(ret), K(*expr));
      } else {
        trans_param.expr_ = static_cast<ObOpRawExpr *>(expr);
        trans_param.subquery_expr_ = static_cast<ObQueryRefRawExpr *>(expr->get_param_expr(0));
      }
    } else if (expr->has_flag(IS_WITH_ALL) || expr->has_flag(IS_WITH_ANY)) {
      if (OB_ISNULL(expr->get_param_expr(0))
          || OB_ISNULL(expr->get_param_expr(1))
          || OB_UNLIKELY(!expr->get_param_expr(1)->is_query_ref_expr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid anyall predicate", K(ret), K(*expr));
      } else if (OB_UNLIKELY(expr->get_param_expr(0)->has_flag(CNT_SUB_QUERY))) {
        // do not transform `subquery in subquery`, `subquery = all subquery`
        // or else the left-hand subquery will be unnested as a lateral subquery
        trans_param.can_transform_ = false;
        OPT_TRACE("anyall unnest: lhs contains subquery", expr);
      } else {
        trans_param.expr_ = static_cast<ObOpRawExpr *>(expr);
        trans_param.subquery_expr_ = static_cast<ObQueryRefRawExpr *>(expr->get_param_expr(1));
      }
    } else {
      trans_param.can_transform_ = false;
      OPT_TRACE("anyall unnest: not supported expr type", expr);
    }
    if (OB_FAIL(ret) || !trans_param.can_transform_) {
    } else if (OB_ISNULL(trans_param.subquery_expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("subquery expr is null", K(ret));
    } else if (OB_FAIL(anyall_check_subquery_validity(trans_param))) {
      LOG_WARN("failed to check can be pulled up ", K(ret), K(trans_param));
    } else {
      LOG_TRACE("finish to check subquery unnest validity", K(ret), K(trans_param));
    }
  }
  return ret;
}

/*
 * Validity check for a single **(non-OR)** subquery.  For correlated subqueries,
 * picks one of three strategies:
 *
 *   correlated subquery
 *    │
 *    ├─ has nested correlated subquery in WHERE/HAVING?
 *    │    Must use **INNER JOIN** because a semi-join would leave the nested subquery
 *    │   inside the ON clause, leading to poor plans. Unnest only if:
 *    │     - expr is EXISTS or =ANY (NOT EXISTS / =ALL cannot become INNER JOIN)
 *    │     - can_unnest_directly without creating a view
 *    │     - subquery output is unique (INNER JOIN must not produce extra rows)
 *    │
 *    │   e.g.:  WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.c1 = t1.c1
 *    │                        AND t2.c2 >= ANY(SELECT c2 FROM t3 WHERE t3.c1 = t1.c2))
 *    │   →  t1 INNER JOIN t2 ON t2.c1 = t1.c1 and t2.c2 >= ANY(SELECT c2 FROM t3 WHERE t3.c1 = t1.c2)
 *    │
 *    ├─ can unnest directly?
 *    │   Standard semi/anti join — the common path for most subqueries.
 *    │
 *    └─ cannot unnest directly (has GROUP BY / DISTINCT / LIMIT / etc.)?
 *        Try wrapping in SPJ to isolate non-pull-able parts.
 *        e.g.:  WHERE c1 IN (SELECT MAX(c2) FROM t2 WHERE t2.c3 = t1.c3 GROUP BY c3, c4)
 *        →  wrap as: SELECT max_c2 FROM (SELECT MAX(c2) max_c2, c3 FROM t2 GROUP BY c3, c4) v
 *                     WHERE v.c3 = t1.c3
 *        then standard semi join on the wrapper.
 */
int ObTransformSubqueryUnnest::anyall_check_subquery_validity(AnyAllSQParam &trans_param)
{
  int ret = OB_SUCCESS;
  ObRawExpr *expr = trans_param.expr_;
  ObQueryRefRawExpr *query_ref_expr = trans_param.subquery_expr_;
  ObSelectStmt *subquery = trans_param.get_subquery();
  if (OB_ISNULL(expr) || OB_ISNULL(query_ref_expr) || OB_ISNULL(subquery)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL", K(ret), K(expr), K(query_ref_expr), K(subquery));
  } else if (expr->has_flag(CNT_ROWNUM)) {
    trans_param.can_transform_ = false;
    OPT_TRACE("expr contains ROWNUM, do not unnest");
  } else if (query_ref_expr->is_shared_reference()) {
    trans_param.can_transform_ = false;
    OPT_TRACE("subquery is shared reference, do not unnest");
  } else if ((T_OP_EXISTS == expr->get_expr_type() || T_OP_NOT_EXISTS == expr->get_expr_type())
             && !trans_param.is_correlated()) {
    // non-correlated exists subquery would be handled in `SimplifySubQuery`
    trans_param.can_transform_ = false;
    OPT_TRACE("not correlated exists subquery no need pullup");
  } else if (OB_FAIL(check_hint_allowed_unnest(*stmt_,
                                               *subquery,
                                               trans_param.outline_loc_,
                                               trans_param.can_transform_))) {
    LOG_WARN("failed to check hint", K(ret));
  } else if (subquery->is_set_stmt()) {
    for (int64_t i = 0; trans_param.can_transform_ && OB_SUCC(ret) && i < subquery->get_set_query().count(); ++i) {
      // skip hint check for child stmts of set stmt, this is INTENTIONAL
      // hence NO_UNNEST/NO_REWRITE hint in child stmts of set stmt cannot disable unnesting the whole set stmt
      if (OB_FAIL(anyall_check_subquery_validity_inner(trans_param, query_ref_expr, subquery->get_set_query(i)))) {
        LOG_WARN("failed to check subquery valid", K(ret));
      }
    }
  } else if (0 == subquery->get_from_item_size()) {
    trans_param.can_transform_ = false;
    OPT_TRACE("subquery has no from item, do not unnest");
  } else if (OB_FAIL(anyall_check_subquery_validity_inner(trans_param, query_ref_expr, subquery))) {
    LOG_WARN("failed to check subquery valid", K(ret));
  }
  if (OB_FAIL(ret) || !trans_param.can_transform_) {
    // do nothing
  } else if (!trans_param.is_correlated()) {
    // the following checks are against correlated subquery
  } else {
    bool can_unnest_directly = true;
    bool has_unremovable_limit = false;
    if (subquery->has_distinct()
        || subquery->is_hierarchical_query()
        || subquery->has_group_by()
        || subquery->has_window_function()
        || subquery->is_set_stmt()
        || subquery->is_unpivot_select()) {
      can_unnest_directly = false;
    } else if (bool has_rownum = false; OB_FAIL(subquery->has_rownum(has_rownum))) {
      LOG_WARN("failed to check subquery has rownum", K(ret));
    } else if (has_rownum) {
      can_unnest_directly = false;
    } else if (OB_FAIL(anyall_check_limit(expr->get_expr_type(),
                                          subquery,
                                          has_unremovable_limit,
                                          trans_param.need_add_limit_constraint_))) {
      LOG_WARN("failed to check if subquery has unremovable limit", K(ret));
    } else if (has_unremovable_limit) {
      can_unnest_directly = false;
    }
    if (OB_FAIL(ret)) {
    } else if (trans_param.has_nested_correlated_subquery_) {
      // A subquery with nested correlated subquery should be unnest using INNER JOIN,
      // otherwise a semi-join with correlated subquery inside `on` condition may lead to a worse plan.
      // To perform such rewrite, the subquery should fulfill the following conditions:
      if (!is_enable_anyall_unnest_nested_subquery(stmt_)) {
        trans_param.can_transform_ = false;
        OPT_TRACE("nested correlated subquery unnest is disabled by tenant config");
      } else if (expr->get_expr_type() != T_OP_EXISTS && !expr->has_flag(IS_WITH_ANY)) {
        // 1. The subquery can only be EXISTS or IN subquery
        //    otherwise it cannot be rewritten as INNER JOIN
        trans_param.can_transform_ = false;
        OPT_TRACE("nested correlated subquery must be EXISTS or IN to unnest as inner join");
      } else if (!can_unnest_directly) {
        // 2. The subquery can only be unnested directly.
        //    Because unnesting by creating a view would introduce new output columns to the subquery,
        //    hence the uniqueness of the original output columns can not be assured,
        //    which must be against condition 3.
        trans_param.can_transform_ = false;
        OPT_TRACE("nested correlated subquery cannot be unnested directly");
      } else {
        // 3. The subquery's output must be unique against the new join conditions,
        //    otherwise rewrite it as an INNER JOIN introduces extra rows
        ObSEArray<ObRawExpr *, 1> const_exprs;
        if (expr->has_flag(IS_WITH_ANY) && expr->get_expr_type() == T_OP_SQ_EQ) {
          // output exprs of the IN(=any) subquery can be treated as const exprs,
          // because they will be converted as equal join conditions after pulled up.
          for (int64_t i = 0; OB_SUCC(ret) && i < subquery->get_select_item_size(); ++i) {
            if (OB_FAIL(const_exprs.push_back(subquery->get_select_item(i).expr_))) {
              LOG_WARN("failed to push back", K(ret));
            }
          }
        }
        if (FAILEDx(check_subquery_unique(subquery,
                                          const_exprs,
                                          trans_param.can_transform_))) {
          LOG_WARN("failed to check subquery unique", K(ret));
        } else if (!trans_param.can_transform_) {
          OPT_TRACE("nested correlated subquery output is not unique");
        }
      }
    } else if (can_unnest_directly) { // not trans_param.has_nested_correlated_subquery_
      trans_param.can_transform_ = true;
    } else if (OB_FAIL(ObTransformUtils::check_correlated_exprs_can_pullup(
                                         query_ref_expr->get_exec_params(),
                                         *subquery,
                                         trans_param.can_transform_))) {
      LOG_WARN("failed to check can unnest with spj", K(ret));
    } else if (trans_param.can_transform_) {
      trans_param.need_create_spj_ = true;
    } else {
      OPT_TRACE("correlated exprs in subquery cannot be pulled up");
    }
  }
  return ret;
}

int ObTransformSubqueryUnnest::anyall_check_subquery_validity_inner(AnyAllSQParam &trans_param,
                                                                    ObQueryRefRawExpr *query_ref_expr,
                                                                    ObSelectStmt *subquery)
{
  int ret = OB_SUCCESS;
  bool reject_transform = false;
  bool is_correlated = false;
  if (OB_ISNULL(query_ref_expr) || OB_ISNULL(subquery)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL", K(ret), K(query_ref_expr), K(subquery));
  } else if (FALSE_IT(is_correlated = query_ref_expr->has_exec_param())) {
  } else if (OB_FAIL(ObTransformUtils::is_select_item_contain_subquery(subquery, reject_transform))) {
    LOG_WARN("failed to check select item contain subquery", K(subquery), K(ret));
  } else if (reject_transform) {
    trans_param.can_transform_ = false;
    OPT_TRACE("subquery`s select expr contain subquery");
  } else if (OB_FAIL(subquery->has_ref_assign_user_var(reject_transform))) {
    LOG_WARN("failed to check subquery has assignment ref user var", K(ret));
  } else if (reject_transform) {
    trans_param.can_transform_ = false;
    OPT_TRACE("subquery has user var");
  } else if (!is_correlated) {
    // the following checks only apply to correlated subquery
  } else if (OB_FAIL(ObTransformUtils::is_from_item_correlated(query_ref_expr->get_exec_params(),
                                                               *subquery,
                                                               reject_transform))) {
    LOG_WARN("failed to check if from item contains correlated subquery", K(ret));
  } else if (reject_transform) {
    trans_param.can_transform_ = false;
    OPT_TRACE("subquery`s table item is correlated");
  } else if (!trans_param.has_nested_correlated_subquery_) {
    // mark if subquery's where/having conditions contained exprs with nested correlated subquery
    ObSEArray<ObRawExpr*, 4> conds;
    if (OB_FAIL(conds.assign(subquery->get_condition_exprs()))) {
      LOG_WARN("failed to assign condition exprs", K(ret));
    } else if (OB_FAIL(append(conds, subquery->get_having_exprs()))) {
      LOG_WARN("failed to append having exprs", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && !trans_param.has_nested_correlated_subquery_ && i < conds.count(); ++i) {
      if (OB_ISNULL(conds.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL pointer error", K(ret), K(conds.at(i)));
      } else if (!conds.at(i)->has_flag(CNT_SUB_QUERY)) {
        // do nothing
      } else if (OB_FAIL(ObTransformUtils::is_correlated_expr(query_ref_expr->get_exec_params(),
                                                              conds.at(i),
                                                              trans_param.has_nested_correlated_subquery_))) {
        LOG_WARN("failed to check is correlated expr", K(ret));
      }
    }
  }
  return ret;
}

/*
 * @brief Check whether the LIMIT clause of a correlated subquery can be safely removed after unnesting.
 *
 * Background: after unnesting a correlated subquery into a semi/anti join, the original LIMIT
 * semantics no longer apply — a semi join only cares whether a matching row exists, not how many
 * rows are produced. LIMIT can therefore be dropped when certain conditions are met; otherwise
 * it must be retained and unnesting is rejected.
 *
 * LIMIT is removable when either of the following holds:
 *   1. The outer predicate is EXISTS / NOT EXISTS:
 *      the semi join only tests row existence, so LIMIT does not affect semantics.
 *   2. The outer predicate is an ANY/ALL comparison AND all SELECT items of the subquery are
 *      constant expressions: the result is independent of row count, so LIMIT is irrelevant.
 *
 * When LIMIT is removable but its value is a runtime parameter (`LIMIT ?`), a constraint must be
 * added to guarantee the parameter >= 1. This prevents a plan with LIMIT already removed from
 * being reused with LIMIT 0 (which would make the subquery always return an empty set).
 *
 * @param[in]  op_type                   outer predicate type (EXISTS / NOT EXISTS / ANY / ALL comparison)
 * @param[in]  subquery                  the subquery to check
 * @param[out] has_unremovable_limit     true  → LIMIT cannot be removed; unnesting is rejected
 *                                       false → no LIMIT present, or LIMIT can be safely removed
 * @param[out] need_add_limit_constraint true  → LIMIT is removable but a constraint on its value is required
 */
int ObTransformSubqueryUnnest::anyall_check_limit(const ObItemType op_type,
                                                  const ObSelectStmt *subquery,
                                                  bool &has_unremovable_limit,
                                                  bool &need_add_limit_constraint) const
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = NULL;
  has_unremovable_limit = false;
  need_add_limit_constraint = false;
  if (OB_ISNULL(subquery)
      || OB_ISNULL(ctx_)
      || OB_ISNULL(ctx_->exec_ctx_)
      || OB_ISNULL(plan_ctx = ctx_->exec_ctx_->get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(subquery), K(ctx_), K(plan_ctx));
  } else if (bool is_const_select = false;
             OB_FAIL(ObTransformUtils::check_const_select(ctx_, subquery, is_const_select))) {
    LOG_WARN("failed to check const select", K(ret));
  } else if (op_type != T_OP_EXISTS && op_type != T_OP_NOT_EXISTS && !is_const_select) {
    // ANY/ALL comparison with non-constant SELECT items:
    // LIMIT changes the result set size and cannot be removed without altering semantics
    has_unremovable_limit = subquery->has_limit();
  } else if (!subquery->has_limit()) {
    // no LIMIT clause, nothing to check
    has_unremovable_limit = false;
  } else if (subquery->get_offset_expr() != NULL || subquery->get_limit_percent_expr() != NULL) {
    // OFFSET or percent-based LIMIT cannot be equivalently expressed in a semi join
    has_unremovable_limit = true;
  } else {
    // plain `LIMIT N`
    bool is_null_value = false;
    int64_t limit_value = 0;
    if (OB_FAIL(ObTransformUtils::get_limit_value(subquery->get_limit_expr(),
                                                  &plan_ctx->get_param_store(),
                                                  ctx_->exec_ctx_,
                                                  ctx_->allocator_,
                                                  limit_value,
                                                  is_null_value))) {
      LOG_WARN("failed to get_limit_value", K(ret));
    } else if (!is_null_value && limit_value >= 1) {
      has_unremovable_limit = false;
      // LIMIT N (N>=1) can be removed, but if N is a parameterized value (`LIMIT ?`),
      // a constraint must be added to prevent plan cache from reusing this plan when
      // N is later bound to 0 (which would make the subquery always return empty)
      need_add_limit_constraint = true;
    } else {
      // limit_value = 0 or NULL: the subquery always returns an empty set, cannot remove
      has_unremovable_limit = true;
    }
  }
  return ret;
}

/*
 * Execute the unnesting.  Join type is decided by anyall_prepare_for_unnest:
 *
 *   EXISTS / =ANY ─── has nested correlated subquery? ─── yes → INNER JOIN
 *                 └── no  → LEFT SEMI JOIN
 *   NOT EXISTS / =ALL ───────────────────────────────────── LEFT ANTI JOIN
 *
 *   join_type
 *    ├─ SEMI / ANTI → build semi conds, wrap subquery as view, create SemiInfo
 *    └─ INNER       → build join conds, decorrelate, pullup tables directly
 *
 * NOTE: `expr` is the *original* WHERE condition — it may differ from trans_param.expr_
 *       when OR-expansion synthesized a new =ANY(UNION ALL ...) expression.
 */
int ObTransformSubqueryUnnest::anyall_do_unnest_subquery(ObRawExpr *expr, AnyAllSQParam &trans_param)
{
  int ret = OB_SUCCESS;
  ObJoinType join_type = UNKNOWN_JOIN;
  ObSEArray<TableItem *, 4> right_tables;
  if (OB_ISNULL(stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K_(stmt));
  } else if (OB_FAIL(anyall_prepare_for_unnest(trans_param, join_type))) {
    LOG_WARN("failed to prepare for unnest", K(ret));
  } else if (LEFT_SEMI_JOIN == join_type || LEFT_ANTI_JOIN == join_type) {
    // unnest as SEMI JOIN
    ObSEArray<ObSEArray<ObRawExpr*, 4>, 4> final_semi_conds; // a 2d array because semi info may split
    if (OB_FAIL(anyall_build_semi_conds(trans_param, join_type, right_tables, final_semi_conds))) {
      LOG_WARN("failed to generate semi conds", K(ret));
    } else if (OB_FAIL(anyall_build_semi_infos(trans_param, join_type, right_tables, final_semi_conds))) {
      LOG_WARN("failed to build new semi info", K(ret));
    }
  } else if (INNER_JOIN == join_type) {
    // unnest as INNER JOIN
    ObSEArray<ObRawExpr*, 4> join_conds;
    ObQueryRefRawExpr *query_ref_expr = trans_param.subquery_expr_;
    ObSelectStmt *subquery = trans_param.get_subquery();
    ObSEArray<ObRawExpr*, 4> right_exprs;
    if (OB_ISNULL(query_ref_expr) || OB_ISNULL(subquery)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret), K(query_ref_expr), K(subquery));
    } else if (OB_FAIL(subquery->get_select_exprs(right_exprs))) {
      LOG_WARN("failed to get select exprs", K(ret));
    } else if (OB_FAIL(anyall_build_join_conds(trans_param, INNER_JOIN, right_exprs, join_conds))) {
      LOG_WARN("failed to generate join conds", K(ret));
    } else if (trans_param.is_correlated()) {
      // de-correlation must performed after building join conditions
      // otherwise correlated conds can not be correctly identified
      if (OB_FAIL(ObTransformUtils::decorrelate(join_conds, query_ref_expr->get_exec_params()))) {
        LOG_WARN("failed to decorrelate subquery", K(ret));
      } else if (OB_FAIL(ObTransformUtils::decorrelate(subquery, query_ref_expr->get_exec_params()))) {
        LOG_WARN("failed to decorrelate subquery", K(ret));
      }
    }
    if (FAILEDx(append(stmt_->get_condition_exprs(), join_conds))) {
      LOG_WARN("failed to append join conds", K(ret));
    } else if (OB_FAIL(pullup_subquery_to_parent_stmt(subquery, false))) {
      LOG_WARN("failed to pull up tables and columns", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected join type", K(ret), K(join_type));
  }
  // NOTICE: `expr` refers to the original condition expr
  // which can be different from `trans_param.expr_` if it was a `OR` expr
  // here we should remove the original condition expr from the stmt
  if (FAILEDx(ObOptimizerUtil::remove_item(stmt_->get_condition_exprs(), expr))) {
    LOG_WARN("failed to remove condition expr", K(ret));
  } else if (OB_FAIL(stmt_->adjust_subquery_list())) {
    LOG_WARN("failed to adjust subquery list", K(ret));
  } else if (OB_FAIL(stmt_->formalize_stmt(ctx_->session_info_, false))) {
    LOG_WARN("failed to formalize stmt", K(ret));
  }
  return ret;
}

int ObTransformSubqueryUnnest::anyall_prepare_for_unnest(AnyAllSQParam &trans_param,
                                                         ObJoinType &join_type)
{
  int ret = OB_SUCCESS;
  ObRawExpr *expr = trans_param.expr_;
  ObQueryRefRawExpr *query_ref_expr = trans_param.subquery_expr_;
  ObSelectStmt *subquery = trans_param.get_subquery();

  if (OB_ISNULL(expr)
      || OB_ISNULL(query_ref_expr)
      || OB_ISNULL(subquery)
      || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(expr), K(query_ref_expr), K(subquery), K_(ctx));
  }
  // 1. get join type
  if (OB_SUCC(ret)) {
    if (T_OP_EXISTS == expr->get_expr_type() || expr->has_flag(IS_WITH_ANY)) {
      join_type = trans_param.has_nested_correlated_subquery_ ? INNER_JOIN : LEFT_SEMI_JOIN;
    } else if (T_OP_NOT_EXISTS == expr->get_expr_type() || expr->has_flag(IS_WITH_ALL)) {
      join_type = LEFT_ANTI_JOIN;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected expr type", K(ret), K(expr->get_expr_type()));
    }
  }
  // 2. add limit constraint if needed
  if (OB_SUCC(ret) && trans_param.need_add_limit_constraint_) {
    if (OB_FAIL(ObTransformUtils::add_const_param_constraints(subquery->get_limit_expr(), ctx_))) {
      LOG_WARN("failed to add const param constraints", K(ret));
    }
  }
  // 3. create spj if needed
  if (OB_SUCC(ret) && trans_param.need_create_spj_) {
    bool skip_const_in_select = T_OP_EXISTS == expr->get_expr_type() || T_OP_NOT_EXISTS == expr->get_expr_type();
    if (OB_FAIL(ObTransformUtils::create_spj_and_pullup_correlated_exprs(query_ref_expr->get_exec_params(),
                                                                         subquery,
                                                                         ctx_,
                                                                         false, // is_set_child_stmt
                                                                         skip_const_in_select))) {
      LOG_WARN("failed to create spj and pullup correlated exprs", K(ret));
    } else {
      query_ref_expr->set_ref_stmt(subquery);
    }
  }
  // 4. remove redundant limit and order-by for correlated subquery
  if (OB_SUCC(ret) && trans_param.is_correlated()) {
    // limit and orderby in correlated subquery can be safely removed once it passed `anyall_check_limit`. e.g.
    // from: select * from t1 where t1.c1 in (select 1 from t2 where t1.c2 = c2 order by c3 limit 1);
    // to:   select * from t1 semi join (select 1 from t2) on t1.c2 = t2.c2 and t1.c1 = 1;
    subquery->set_limit_offset(NULL, NULL);
    subquery->get_order_items().reuse();
  }
  return ret;
}

// Build join/semi conditions.  Two sources:
//   1. Correlated WHERE predicates pulled out of the subquery.
//   2. The original =ANY/=ALL comparison, converted to scalar comparison(s).
int ObTransformSubqueryUnnest::anyall_build_join_conds(AnyAllSQParam &trans_param,
                                                       ObJoinType join_type,
                                                       ObIArray<ObRawExpr *> &right_exprs,
                                                       ObIArray<ObRawExpr *> &join_conds)
{
  int ret = OB_SUCCESS;
  ObRawExpr *expr = trans_param.expr_;
  ObQueryRefRawExpr *query_ref_expr = trans_param.subquery_expr_;
  ObSelectStmt *subquery = trans_param.get_subquery();
  if (OB_ISNULL(expr) || OB_ISNULL(query_ref_expr) || OB_ISNULL(subquery)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(expr), K(query_ref_expr), K(subquery));
  }
  // 1. for correlated subquery, its correlated conditions can be pulled up as join condition
  if (OB_SUCC(ret) && trans_param.is_correlated()) {
    if (OB_FAIL(ObTransformUtils::get_correlated_conditions(query_ref_expr->get_exec_params(),
                                                            subquery->get_condition_exprs(),
                                                            join_conds))) {
      LOG_WARN("failed to get semi conditions", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::remove_item(subquery->get_condition_exprs(), join_conds))) {
      LOG_WARN("failed to remove condition expr", K(ret));
    }
  }
  // 2. convert original anyall condition to a join condition (or subquery`s where condition)
  if (OB_SUCC(ret) && (expr->has_flag(IS_WITH_ALL) || expr->has_flag(IS_WITH_ANY))) {
    if (OB_FAIL(anyall_convert_anyall_to_cmp_cond(trans_param,
                           /*+ force_join_cond */ (INNER_JOIN == join_type) || !trans_param.is_correlated(),
                                                  right_exprs,
                                                  join_conds))) {
      LOG_WARN("failed to generate new condition exprs", K(ret), K(trans_param));
    }
  }
  return ret;
}

/*
 * Build semi conditions and right-table(s) for the SEMI/ANTI path.
 *
 *   candidate semi conds ──► can split cartesian tables in subquery?
 *     ├─ yes → split subquery's disconnected table groups into separate semi-joins
 *     │        e.g. WHERE c1 IN (SELECT 1 FROM t2, t3 WHERE t2.c2 = t1.c1 AND t3.c3 = t1.c3)
 *     │        t2 and t3 are not connected by any join predicate within the subquery,
 *     │        so they can be split into: semi(t1, t2) ON ...  +  semi(t1, t3) ON ...
 *     │
 *     └─ no  → wrap the entire subquery as one generated-table view
 *              non-correlated: simple column-ref replacement via copier
 *              correlated: rebuild conditions with de-correlation awareness
 */
int ObTransformSubqueryUnnest::anyall_build_semi_conds(
  AnyAllSQParam &trans_param,
  ObJoinType join_type,
  ObIArray<TableItem *> &right_tables,
  ObIArray<ObSEArray<ObRawExpr *, 4>> &final_semi_conds)
{
  int ret = OB_SUCCESS;
  ObRawExpr *expr = trans_param.expr_;
  ObQueryRefRawExpr *query_ref_expr = trans_param.subquery_expr_;
  ObSelectStmt *subquery = trans_param.get_subquery();
  ObSEArray<ObRawExpr *, 4> candi_semi_conds;
  ObSEArray<ObRawExpr *, 4> right_exprs;
  if (OB_ISNULL(stmt_)
      || OB_ISNULL(expr)
      || OB_ISNULL(query_ref_expr)
      || OB_ISNULL(subquery)
      || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K_(stmt), K(expr), K(query_ref_expr), K(subquery), K_(ctx));
  }

  // 1. generate new semi conditions
  if (FAILEDx(subquery->get_select_exprs(right_exprs))) {
    LOG_WARN("failed to get select exprs", K(ret));
  } else if (OB_FAIL(anyall_build_join_conds(trans_param, join_type, right_exprs, candi_semi_conds))) {
    LOG_WARN("failed to build join conds", K(ret));
  // 2. create view and rebuild semi conditions
  } else if (OB_FAIL(anyall_check_can_split_semi_join(subquery,
                                                      candi_semi_conds,
                                                      join_type,
                                                      trans_param))) {
    LOG_WARN("fail to check can split", K(ret));
  } else if (trans_param.need_split_semi_info_) {
    // 2.1. if need split cartesian tables:
    //      split into multiple <semi conditions, right table> pairs
    //      semi conditions will be rebuilt in this function
    // Clear select items before split: the semi conditions share expression tree objects
    // (e.g. T_OP_ADD nodes) with the subquery's select items via right_exprs.
    // create_inline_view → generate_select_list → replace_relation_exprs mutates
    // origin_subquery's expression trees, which would corrupt the semi conditions
    // through the shared pointers. Clearing select items prevents extract_shared_exprs
    // from finding them, so no mutation occurs. rebuild_columns_for_view_and_conds
    // will properly reconstruct the view's select list from the semi conditions.
    subquery->get_select_items().reset();
    if (OB_FAIL(ObTransformUtils::do_split_cartesian_tables(ctx_,
                                                            stmt_,
                                                            subquery,
                                                            candi_semi_conds,
                                                            trans_param.connected_tables_,
                                                            right_tables,
                                                            final_semi_conds))) {
      LOG_WARN("fail to split cartesian tables", K(ret));
    }
  } else {
    // 2.2. if no need to split cartesian tables:
    //      create view for subquery as a right table, and rebuild semi conditions
    TableItem *view_table = NULL;
    if (OB_FAIL(ObTransformUtils::add_new_table_item(ctx_, stmt_, subquery, view_table))) {
      LOG_WARN("failed to add new view_table", K(ret));
    } else if (OB_ISNULL(view_table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table item should not be null", K(ret));
    } else {
      if (!trans_param.is_correlated()) {
        ObSEArray<ObRawExpr *, 4> view_columns;
        if (OB_FAIL(ObTransformUtils::create_columns_for_view(ctx_, *view_table, stmt_, view_columns))) {
          LOG_WARN("failed to create columns for view", K(ret));
        } else if (!is_opt_version_ge_461(stmt_)) {
          // replicate the legacy behavior for lower optimizer version or else the plan may change
          candi_semi_conds.reuse();
          if (OB_FAIL(anyall_build_join_conds(trans_param, join_type, view_columns, candi_semi_conds))) {
            LOG_WARN("failed to rebuild join conds with view columns", K(ret));
          }
        } else {
          // version >= 4.6.1: replace original select exprs → view columns via copier
          ObRawExprCopier copier(*ctx_->expr_factory_);
          if (OB_FAIL(copier.add_replaced_expr(right_exprs, view_columns))) {
            LOG_WARN("failed to add replaced expr", K(ret));
          } else if (OB_FAIL(copier.copy_on_replace(candi_semi_conds, candi_semi_conds))) {
            LOG_WARN("failed to copy on replace", K(ret));
          }
        }
      } else if (OB_FAIL(ObTransformUtils::rebuild_columns_for_view_and_conds(ctx_,
                                                                              stmt_,
                                                                              view_table,
                                                                              candi_semi_conds,
                                                                              !is_opt_version_ge_461(stmt_)))) {
        // version < 4.6.1: keep_orig_select_columns = true → preserves full SPJ select list (legacy behavior)
        // version >= 4.6.1: keep_orig_select_columns = false → minimal select list (new behavior)
        LOG_WARN("failed to rebuild conditions for view", K(ret));
      }
    }
    if (FAILEDx(right_tables.push_back(view_table))) {
      LOG_WARN("fail to push back table item", K(ret));
    } else if (OB_FAIL(final_semi_conds.push_back(candi_semi_conds))) {
      LOG_WARN("fail to push back semi conditions", K(ret));
    }
  }
  return ret;
}

/*
 * Convert =ANY / =ALL predicate into scalar comparisons.
 *
 *   | original             | join type  | generated condition             |
 *   |----------------------|------------|---------------------------------|
 *   | c1 =ANY (select c2)  | semi join  | c1 = c2                        |
 *   | c1 >ALL (select c2)  | anti join  | c1 <= c2 OR c1 IS NULL OR ...  |
 *   | (a,b) !=ANY (c,d)    | semi join  | (a,b) != (c,d)                 |
 *   | (a,b) =ALL  (c,d)    | anti join  | lnnvl((a,b) = (c,d))           |
 *
 * For =ALL, each condition is wrapped with null-checks via anyall_build_anti_conds.
 *
 * `force_join_cond` controls where the generated condition lands:
 *   true  (INNER JOIN, or non-correlated) → always a join/semi condition
 *   false (correlated semi/anti)          → if lhs is const, push into subquery WHERE
 *                                           (evaluated early, before the join)
 *     e.g.  WHERE 5 IN (SELECT c2 FROM t2 WHERE t1.c1 = t2.c1)
 *     →  semi cond: t1.c1 = t2.c1;  subquery WHERE: 5 = c2  (pushed down)
 */
int ObTransformSubqueryUnnest::anyall_convert_anyall_to_cmp_cond(AnyAllSQParam &trans_param,
                                                                 bool force_join_cond,
                                                                 ObIArray<ObRawExpr *> &right_exprs,
                                                                 ObIArray<ObRawExpr *> &join_conds)
{
  int ret = OB_SUCCESS;
  int64_t col_cnt = 0;
  ObRawExpr *expr = trans_param.expr_;
  ObSelectStmt *subquery = trans_param.get_subquery();
  ObRawExpr *left_expr = NULL;
  ObItemType cmp_op_type = T_INVALID;
  if (OB_ISNULL(stmt_)
      || OB_ISNULL(expr)
      || OB_ISNULL(subquery)
      || OB_ISNULL(ctx_)
      || OB_ISNULL(ctx_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL pointer error", K_(stmt), K(expr), K(ctx_));
  } else if (!expr->has_flag(IS_WITH_ALL) && !expr->has_flag(IS_WITH_ANY)) {
    // only convert any/all condition
  } else if (OB_UNLIKELY(T_INVALID == (cmp_op_type = anyall_get_cmp_op_type(*expr)))) {
    ret = OB_ERR_ILLEGAL_TYPE;
    LOG_WARN("invalid semi cond cmp op type", K(ret), K(expr->get_expr_type()));
  } else if (OB_UNLIKELY((col_cnt = subquery->get_select_item_size()) <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid select item size", K(ret), K(col_cnt));
  } else if (OB_ISNULL(left_expr = expr->get_param_expr(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("left param is null", K(ret));
  } else if (col_cnt > 1
             && OB_UNLIKELY(T_OP_ROW != left_expr->get_expr_type()
                            || col_cnt != left_expr->get_param_count()
                            || col_cnt != right_exprs.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid vector comparison", K(ret), K(col_cnt), K(right_exprs.count()), K(left_expr));
  } else if (col_cnt == 1 || cmp_op_type == T_OP_EQ) {
    // a cmp any(select b) -> a cmp b
    // (a,b) =any (select c,d) -> (a,b) = (c,d)
    if (OB_FAIL(anyall_convert_anyall_to_scalar_cmp_conds(trans_param, force_join_cond, cmp_op_type,
                                                          left_expr, right_exprs, join_conds))) {
      LOG_WARN("failed to build scalar cmp conds", K(ret));
    }
  } else {
    // vector comparison with non-EQ operator reaches here (e.g. !=ANY, >ANY, =ALL, <ALL, ...)
    // (a,b) !=ANY (select c,d) -> (a,b) != (c,d)
    // (a,b) =ALL (select c,d) -> lnnvl((a,b) = (c,d))
    if (OB_FAIL(anyall_convert_anyall_to_vector_cmp_cond(trans_param, force_join_cond,
                                                         left_expr, right_exprs, join_conds))) {
      LOG_WARN("failed to build vector cmp cond", K(ret));
    }
  }
  return ret;
}

// create scalar comparison conditions for each cmp column
int ObTransformSubqueryUnnest::anyall_convert_anyall_to_scalar_cmp_conds(
  AnyAllSQParam &trans_param,
  bool force_join_cond,
  ObItemType cmp_op_type,
  ObRawExpr *left_expr,
  ObIArray<ObRawExpr *> &right_exprs,
  ObIArray<ObRawExpr *> &join_conds)
{
  int ret = OB_SUCCESS;
  ObRawExpr *expr = trans_param.expr_;
  ObSelectStmt *subquery = trans_param.get_subquery();
  ObRawExprFactory *expr_factory = ctx_->expr_factory_;
  int64_t col_cnt = subquery->get_select_item_size();
  for (int64_t i = 0; OB_SUCC(ret) && i < col_cnt; ++i) {
    ObRawExpr *left_arg = (col_cnt == 1) ? left_expr : left_expr->get_param_expr(i);
    ObRawExpr *right_arg = right_exprs.at(i);
    ObRawExpr *cmp_expr = NULL;
    if (OB_ISNULL(left_arg) || OB_ISNULL(right_arg)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("left or right arg is null", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::create_double_op_expr(*expr_factory,
                                                             ctx_->session_info_,
                                                             cmp_op_type,
                                                             cmp_expr,
                                                             left_arg,
                                                             right_arg))) {
      LOG_WARN("failed to create comparison expr", K(ret));
    } else if (!expr->has_flag(IS_WITH_ALL)) {
      // do nothing
    } else if (OB_FAIL(anyall_build_anti_conds(subquery,
                                               cmp_expr,
                                               left_arg,
                                               right_arg,
                                               cmp_expr))) {
      LOG_WARN("failed to create anti join condition", K(ret));
    } else if (OB_ISNULL(cmp_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cmp expr is null", K(ret));
    } else if (OB_FAIL(cmp_expr->formalize(ctx_->session_info_))) {
      LOG_WARN("failed to formalize expr", K(ret));
    }
    if (OB_SUCC(ret)) {
      if (!force_join_cond && left_arg->is_const_expr() && !right_arg->has_flag(CNT_DYNAMIC_PARAM)) {
        if (OB_FAIL(subquery->get_condition_exprs().push_back(cmp_expr))) {
          LOG_WARN("failed to push back right table conditions", K(ret));
        }
      } else if (OB_FAIL(join_conds.push_back(cmp_expr))) {
        LOG_WARN("failed to push back semi join conditions", K(ret));
      }
    }
  }
  return ret;
}

/*
 * Vector comparison with non-EQ operator: build a T_OP_ROW right-hand side + apply lnnvl for ALL.
 *
 * | --- original cond --- | - join type - |   --- semi cond ---   |
 * |   (a,b) !=any (c,d)   |   semi join   |    (a,b) != (c,d)     |
 * |   (a,b)  =all (c,d)   |   anti join   | lnnvl((a,b) = (c,d))  |
 *
 * lnnvl is used because expr like `T_OP_ROW IS NULL` is invalid.
 */
int ObTransformSubqueryUnnest::anyall_convert_anyall_to_vector_cmp_cond(
  AnyAllSQParam &trans_param,
  bool force_join_cond,
  ObRawExpr *left_expr,
  ObIArray<ObRawExpr *> &right_exprs,
  ObIArray<ObRawExpr *> &join_conds)
{
  int ret = OB_SUCCESS;
  ObRawExpr *expr = trans_param.expr_;
  ObSelectStmt *subquery = trans_param.get_subquery();
  ObRawExprFactory *expr_factory = ctx_->expr_factory_;
  ObRawExpr *left_arg = left_expr;
  ObOpRawExpr *right_arg = NULL;
  ObRawExpr *cmp_expr = NULL;
  ObItemType cmp_op_type = expr->has_flag(IS_WITH_ALL) ? T_OP_EQ : T_OP_NE;
  if (OB_FAIL(expr_factory->create_raw_expr(T_OP_ROW, right_arg))) {
    LOG_WARN("failed to create a new expr", K(ret));
  } else if (OB_ISNULL(right_arg)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("right arg is null", K(ret));
  } else if (OB_FAIL(right_arg->set_param_exprs(right_exprs))) {
    LOG_WARN("failed to add param expr", K(ret));
  } else if (lib::is_oracle_mode()) {
    ObOpRawExpr *row_expr = NULL;
    if (OB_FAIL(expr_factory->create_raw_expr(T_OP_ROW, row_expr))) {
      LOG_WARN("failed to create a new expr", K(cmp_op_type), K(ret));
    } else if (OB_FAIL(row_expr->set_param_expr(right_arg))) {
      LOG_WARN("fail to set param expr", K(ret), K(right_arg), K(row_expr));
    } else {
      right_arg = row_expr;
    }
  }
  if (FAILEDx(ObRawExprUtils::create_double_op_expr(*expr_factory,
                                                    ctx_->session_info_,
                                                    cmp_op_type,
                                                    cmp_expr,
                                                    left_arg,
                                                    right_arg))) {
    LOG_WARN("failed to create comparison expr", K(ret));
  } else if (!expr->has_flag(IS_WITH_ALL)) {
    // do nothing
  } else if (OB_FAIL(ObRawExprUtils::build_lnnvl_expr(*expr_factory, cmp_expr, cmp_expr))) {
    LOG_WARN("failed to build lnnvl expr", K(ret));
  } else if (OB_ISNULL(cmp_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cmp expr is null", K(ret));
  } else if (OB_FAIL(cmp_expr->formalize(ctx_->session_info_))) {
    LOG_WARN("failed to formalize expr", K(ret));
  }
  if (OB_SUCC(ret)) {
    if (!force_join_cond && left_arg->is_const_expr() && !right_arg->has_flag(CNT_DYNAMIC_PARAM)) {
      if (OB_FAIL(subquery->get_condition_exprs().push_back(cmp_expr))) {
        LOG_WARN("failed to push back right table conditions", K(ret));
      }
    } else if (OB_FAIL(join_conds.push_back(cmp_expr))) {
      LOG_WARN("failed to push back semi conditions", K(ret));
    }
  }
  return ret;
}

// Map T_OP_SQ_* → T_OP_*.  For ALL the op is negated because ALL becomes ANTI JOIN:
//   =ALL → !=,  <=ALL → >,  <ALL → >=,  etc.
ObItemType ObTransformSubqueryUnnest::anyall_get_cmp_op_type(ObRawExpr &expr)
{
  ObItemType cmp_op_type = T_INVALID;
  if (expr.has_flag(IS_WITH_ALL)) {
    switch (expr.get_expr_type()) {
      case T_OP_SQ_EQ: cmp_op_type = T_OP_NE; break; // =  to !=
      case T_OP_SQ_LE: cmp_op_type = T_OP_GT; break; // <= to >
      case T_OP_SQ_LT: cmp_op_type = T_OP_GE; break; // <  to >=
      case T_OP_SQ_GE: cmp_op_type = T_OP_LT; break; // >= to <
      case T_OP_SQ_GT: cmp_op_type = T_OP_LE; break; // >  to <=
      case T_OP_SQ_NE: cmp_op_type = T_OP_EQ; break; // != to =
      default: break;
    }
  } else {
    switch (expr.get_expr_type()) {
      case T_OP_SQ_EQ: cmp_op_type = T_OP_EQ; break;
      case T_OP_SQ_LE: cmp_op_type = T_OP_LE; break;
      case T_OP_SQ_LT: cmp_op_type = T_OP_LT; break;
      case T_OP_SQ_GE: cmp_op_type = T_OP_GE; break;
      case T_OP_SQ_GT: cmp_op_type = T_OP_GT; break;
      case T_OP_SQ_NE: cmp_op_type = T_OP_NE; break;
      default: break;
    }
  }
  return cmp_op_type;
}

/*
 * @brief Wrap a scalar anti-join comparison with null-checks for NOT IN semantics.
 *
 * For NOT IN, the comparison `c1 =ALL (SELECT c2 ...)` is negated to `c1 != c2`.
 * But SQL NOT IN treats NULL specially: if either side is NULL, the row must be
 * kept by the anti join (i.e., the condition evaluates to unknown).  So we OR in
 * null-checks for each nullable side:
 *
 *   SELECT c1 FROM t1 WHERE c1 NOT IN (SELECT c2 FROM t2 WHERE t1.c3 != t2.c4)
 *   anti-join condition for the NOT IN comparison (c1 vs c2):
 *     c1 != c2 OR c1 IS NULL OR c2 IS NULL
 *   (the correlated predicate t1.c3 != t2.c4 is a separate semi condition,
 *    handled by anyall_build_join_conds, not by this function)
 *
 * This function is not suitable for vector comparisons (T_OP_ROW),
 * because left_arg and right_arg cannot be null-tested directly as rows.
 */
int ObTransformSubqueryUnnest::anyall_build_anti_conds(const ObSelectStmt *subquery,
                                                       ObRawExpr *cond_expr,
                                                       ObRawExpr *left_arg,
                                                       ObRawExpr *right_arg,
                                                       ObRawExpr *&anti_expr)
{
  int ret = OB_SUCCESS;
  ObRawExprFactory *expr_factory = NULL;
  bool left_is_not_null = false;
  bool right_is_not_null = false;
  ObOpRawExpr *new_cond_expr = NULL;
  ObSEArray<ObRawExpr *, 4> left_constraints;
  ObSEArray<ObRawExpr *, 4> right_constraints;
  const ObDMLStmt *right_stmt = subquery;
  if (OB_ISNULL(stmt_)
      || OB_ISNULL(subquery)
      || OB_ISNULL(cond_expr)
      || OB_ISNULL(left_arg)
      || OB_ISNULL(right_arg)
      || OB_ISNULL(ctx_)
      || OB_ISNULL(expr_factory = ctx_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret), K_(stmt), K(subquery), K(cond_expr), K(left_arg),
                                 K(right_arg), K_(ctx), K(expr_factory));
  } else {
    // Determine statement to use for right-arg null check because
    // right_arg may be a VIEW column belonging to stmt_ rather than the subquery.
    // Detect by extracting column refs from right_arg and checking their table_id ownership.
    ObSEArray<ObRawExpr *, 1> right_col_exprs;
    if (OB_FAIL(ObRawExprUtils::extract_column_exprs(right_arg, right_col_exprs))) {
      LOG_WARN("failed to extract column exprs", K(ret));
    } else if (!right_col_exprs.empty()) {
      const ObColumnRefRawExpr *col = static_cast<const ObColumnRefRawExpr *>(right_col_exprs.at(0));
      if (NULL == subquery->get_table_item_by_id(col->get_table_id())) {
        // this column does not belong to subquery
        // it must be a column of subquery view table which belongs to stmt_
        right_stmt = stmt_;
      }
    }
  }
  if (FAILEDx(ObTransformUtils::is_expr_not_null(ctx_,
                                                 stmt_,
                                                 left_arg,
                                                 NULLABLE_SCOPE::NS_WHERE,
                                                 left_is_not_null,
                                                 &left_constraints))) {
    LOG_WARN("fail to check need make null with left arg", K(ret));
  } else if (left_is_not_null &&
             OB_FAIL(ObTransformUtils::add_param_not_null_constraint(*ctx_, left_constraints))) {
    LOG_WARN("failed to add left constraints", K(ret));
  } else if (OB_FAIL(ObTransformUtils::is_expr_not_null(ctx_,
                                                        right_stmt,
                                                        right_arg,
                                                        NULLABLE_SCOPE::NS_TOP,
                                                        right_is_not_null,
                                                        &right_constraints))) {
    LOG_WARN("fail to check need make null with right arg", K(ret));
  } else if (right_is_not_null &&
             OB_FAIL(ObTransformUtils::add_param_not_null_constraint(*ctx_, right_constraints))) {
    LOG_WARN("failed to add param not null constraint", K(ret));
  } else if (left_is_not_null && right_is_not_null) {
    anti_expr = cond_expr;
  } else if (OB_FAIL(expr_factory->create_raw_expr(T_OP_OR, new_cond_expr))) {
    LOG_WARN("failed to create a new expr", K(ret));
  } else if (OB_ISNULL(anti_expr = new_cond_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("new_expr is null", K(new_cond_expr), K(ret));
  } else if (OB_FAIL(new_cond_expr->init_param_exprs(3))) {
    LOG_WARN("failed to init param exprs", K(ret));
  } else if (OB_FAIL(new_cond_expr->add_param_expr(cond_expr))) {
    LOG_WARN("failed to add param expr", K(ret));
  }
  if (OB_SUCC(ret) && !left_is_not_null) {
    ObRawExpr *left_null_check_expr = NULL;
    if (OB_FAIL(ObRawExprUtils::build_is_not_null_expr(*expr_factory, left_arg, false, left_null_check_expr))) {
      LOG_WARN("failed to create is null expr", K(ret));
    } else if (OB_FAIL(left_null_check_expr->add_relation_ids(left_arg->get_relation_ids()))) {
      LOG_WARN("failed to add relation ids", K(ret));
    } else if (OB_FAIL(new_cond_expr->add_param_expr(left_null_check_expr))) {
      LOG_WARN("failed to add param expr", K(ret));
    }
  }
  if (OB_SUCC(ret) && !right_is_not_null) {
    ObRawExpr *right_null_check_expr = NULL;
    if (OB_FAIL(ObRawExprUtils::build_is_not_null_expr(*expr_factory, right_arg, false, right_null_check_expr))) {
      LOG_WARN("failed to create is null expr", K(ret));
    } else if (OB_FAIL(right_null_check_expr->add_relation_ids(right_arg->get_relation_ids()))) {
      LOG_WARN("failed to add relation ids", K(ret));
    } else if (OB_FAIL(new_cond_expr->add_param_expr(right_null_check_expr))) {
      LOG_WARN("failed to add param expr", K(ret));
    }
  }
  return ret;
}

/**
 * @brief ObTransformSubqueryUnnest::anyall_check_can_split_semi_join
 * Check whether the semi join can be split.
 *
 * Check Rules:
 *   1. subquery is a spj query;
 *   2. subquery should not contains having;
 *   3. subquery should not contains subquery;
 *   4. join type is SEMI JOIN (ANTI JOIN can be support later).
 *
 *  Besides, pre split subquery and check the subquery can be split into
 *  more than ONE view tables.
 */
int ObTransformSubqueryUnnest::anyall_check_can_split_semi_join(
  ObSelectStmt *subquery,
  ObIArray<ObRawExpr *> &semi_conditions,
  ObJoinType join_type,
  AnyAllSQParam &trans_param)
{
  int ret = OB_SUCCESS;
  bool can_split = true;
  bool is_contain = false;
  if (OB_ISNULL(subquery) || OB_ISNULL(subquery->get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(subquery));
  } else if (!is_enable_anyall_split_semi_join(stmt_)) {
    can_split = false;
    OPT_TRACE("can not split cartesian tables, optimizer features version lower than 4.3.2");
  } else if (join_type != LEFT_SEMI_JOIN) {
    can_split = false;
    OPT_TRACE("can not split cartesian tables, not a semi join");
  } else if (!subquery->is_spj()) {
    can_split = false;
    OPT_TRACE("can not split cartesian tables, it is not a spj");
  } else if (subquery->has_having()) {
    can_split = false;
    OPT_TRACE("can not split cartesian tables, contain having");
  } else if (subquery->has_subquery()) {
    can_split = false;
    OPT_TRACE("can not split cartesian tables, contain subquery");
  } else if (OB_FAIL(ObTransformUtils::check_contain_lost_deterministic_expr(semi_conditions, is_contain))) {
    LOG_WARN("failed to check contain can not duplicate expr", K(ret));
  } else if (is_contain) {
    can_split = false;
    OPT_TRACE("can not split cartesian tables, contain can not duplicate function");
  } else if (OB_FAIL(ObTransformUtils::check_contain_correlated_table(subquery, is_contain))) {
    LOG_WARN("failed to check contain correlated table", K(ret));
  } else if (is_contain) {
    can_split = false;
    OPT_TRACE("can not split cartesian tables, contain correlated derived table");
  } else if (OB_FAIL(ObTransformUtils::cartesian_tables_pre_split(subquery,
                                                                  semi_conditions,
                                                                  trans_param.connected_tables_))) {
    LOG_WARN("fail to pre split cartesian tables", K(ret));
  } else if (trans_param.connected_tables_.count() <= 1) {
    can_split = false;
    OPT_TRACE("can not split cartesian tables, all tables related");
  }
  if (OB_SUCC(ret)) {
    trans_param.need_split_semi_info_ = can_split;
  }
  return ret;
}

int ObTransformSubqueryUnnest::anyall_build_semi_infos(
  AnyAllSQParam &trans_param,
  ObJoinType join_type,
  ObIArray<TableItem *> &right_tables,
  ObIArray<ObSEArray<ObRawExpr *, 4>> &final_semi_conds)
{
  int ret = OB_SUCCESS;
  ObQueryRefRawExpr *query_ref_expr = trans_param.subquery_expr_;
  if (OB_ISNULL(stmt_) || OB_ISNULL(query_ref_expr) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL", K(ret), K_(stmt), K(query_ref_expr), K_(ctx));
  } else if (OB_UNLIKELY(right_tables.count() != final_semi_conds.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unequal tables and semi conds count",
             K(ret), K(right_tables.count()), K(final_semi_conds.count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < right_tables.count(); ++i) {
    ObIArray<ObRawExpr *> &semi_conds = final_semi_conds.at(i);
    TableItem *right_table = right_tables.at(i);
    ObSelectStmt *subquery = NULL;
    SemiInfo *info = NULL;
    if (OB_ISNULL(right_table) || OB_ISNULL(subquery = right_table->ref_query_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret), K(right_table), K(subquery));
    } else if (OB_UNLIKELY(!right_table->is_generated_table())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected right table type in semi info", K(ret), K(right_table->type_));
    } else if (trans_param.is_correlated()
               && OB_FAIL(ObTransformUtils::decorrelate(semi_conds,
                                                        query_ref_expr->get_exec_params()))) {
      LOG_WARN("failed to decorrelate semi conditions", K(ret));
    } else if (OB_ISNULL(info = OB_NEWx(SemiInfo, ctx_->allocator_, *ctx_->allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to alloc semi info", K(ret));
    } else if (OB_FAIL(info->semi_conditions_.assign(semi_conds))) {
      LOG_WARN("failed to assign semi condition exprs", K(ret));
    } else {
      info->right_table_id_ = right_table->table_id_;
      info->join_type_ = join_type;
      info->semi_id_ = stmt_->get_query_ctx()->available_tb_id_--;
    }
    // fill left_table_ids_
    if (OB_SUCC(ret)) {
      info->left_table_ids_.reuse();
      ObSqlBitSet<> left_rel_ids;
      int32_t right_idx = stmt_->get_table_bit_index(info->right_table_id_);
      TableItem *table = NULL;
      ObRawExpr *cond_expr = NULL;
      for (int64_t j = 0; OB_SUCC(ret) && j < info->semi_conditions_.count(); ++j) {
        if (OB_ISNULL(cond_expr = info->semi_conditions_.at(j))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null", K(ret));
        } else if (OB_FAIL(cond_expr->pull_relation_id())) {
          LOG_WARN("pull expr relation ids failed", K(ret), K(*cond_expr));
        } else if (OB_FAIL(cond_expr->formalize(ctx_->session_info_))) {
          LOG_WARN("failed to formalize expr", K(ret));
        } else if (OB_FAIL(left_rel_ids.add_members(cond_expr->get_relation_ids()))) {
          LOG_WARN("failed to add members", K(ret));
        }
      }
      // remove right table relid from left_rel_ids. after that, if left_rel_ids is:
      // 1. not empty: convert relids to table ids, and assign to left_table_ids_
      // 2. empty: get the first table id from stmt_->get_from_item(0), and assign to left_table_ids_
      if (FAILEDx(left_rel_ids.del_member(right_idx))) {
        LOG_WARN("failed to delete members", K(ret));
      } else if (!left_rel_ids.is_empty()) {
        if (OB_FAIL(stmt_->relids_to_table_ids(left_rel_ids, info->left_table_ids_))) {
          LOG_WARN("failed to relids to table ids", K(ret));
        }
      } else if (OB_UNLIKELY(0 == stmt_->get_from_item_size())
                  || OB_ISNULL(table = stmt_->get_table_item(stmt_->get_from_item(0)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("stmt from item is empty", K(ret), K(table));
      } else if (table->is_joined_table()) {
        if (OB_FAIL(append(info->left_table_ids_, static_cast<JoinedTable *>(table)->single_table_ids_))) {
          LOG_WARN("failed to append table ids", K(ret));
        }
      } else if (OB_FAIL(info->left_table_ids_.push_back(table->table_id_))) {
        LOG_WARN("failed to push back table id", K(ret));
      }
    }
    if (FAILEDx(stmt_->add_semi_info(info))) {
      LOG_WARN("failed to add semi info", K(ret));
    } else if (OB_FAIL(subquery->adjust_subquery_list())) {
      LOG_WARN("failed to adjust subquery list", K(ret));
    } else if (OB_FAIL(subquery->formalize_stmt(ctx_->session_info_, false))) {
      LOG_WARN("formalize child stmt failed", K(ret));
    }
  }

  return ret;
}

// ============================== begin of OR subquery expansion ==============================

/*
 * Flatten an OR tree and classify each leaf into one of three branch types:
 *
 *   WHERE c1 IN (SELECT c2 FROM t2 WHERE ...) OR c1 = 100 OR c1 IN (1,2,3)
 *         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^   ^^^^^^^^^   ^^^^^^^^^^^^^
 *         ANY_SUBQUERY                            SCALAR_CMP   INLIST
 *
 * Nested ORs are recursively flattened.  Aborts if total branches > MAX_STMT_NUM_FOR_OR_EXPANSION.
 */
int ObTransformSubqueryUnnest::anyall_collect_or_branch_infos(ObRawExpr *expr,
                                                              AnyAllSQParam &trans_param,
                                                              ObIArray<OrBranchInfo> &branches)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null expr", K(ret));
  } else if (!trans_param.can_transform_) { // stop early once any branch proves the OR is not expandable
  } else if (T_OP_OR == expr->get_expr_type()) {  // flatten nested OR
    if (expr->get_param_count() + branches.count() > MAX_STMT_NUM_FOR_OR_EXPANSION) {
      trans_param.can_transform_ = false;
      OPT_TRACE("OR subquery expansion: too many branches");
    }
    for (int64_t i = 0; OB_SUCC(ret) && trans_param.can_transform_ && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(anyall_collect_or_branch_infos(expr->get_param_expr(i),
                                                            trans_param,
                                                            branches)))) {
        LOG_WARN("failed to classify or branch", K(ret));
      }
    }
  } else if (expr->get_param_count() != 2) {
    trans_param.can_transform_ = false;
    OPT_TRACE("OR subquery expansion: unsupported branch type", expr);
  } else {
    OrBranchInfo info;
    ObRawExpr *left = expr->get_param_expr(0);
    ObRawExpr *right = expr->get_param_expr(1);
    if (OB_ISNULL(left) || OB_ISNULL(right)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL", K(ret), K(left), K(right));
    } else if (expr->has_flag(IS_WITH_ANY)) {
      ObItemType scalar_cmp_type = T_INVALID;
      ObQueryRefRawExpr *query_ref_expr = NULL;
      ObSelectStmt *subquery = NULL;
      if (OB_UNLIKELY(!right->is_query_ref_expr())
          || OB_ISNULL(query_ref_expr = static_cast<ObQueryRefRawExpr *>(right))
          || OB_ISNULL(subquery = query_ref_expr->get_ref_stmt())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected NULL", K(ret), K(right), K(subquery));
      } else if (OB_FAIL(ObTransformUtils::query_cmp_to_value_cmp(expr->get_expr_type(),
                                                                  scalar_cmp_type))) {
        LOG_WARN("failed to convert sq cmp type", K(ret));
      } else {
        info.type_ = OrBranchInfo::ANY_SUBQUERY;
        info.left_expr_ = left;
        info.right_expr_ = query_ref_expr;
        info.scalar_cmp_type_ = scalar_cmp_type;
        info.output_column_cnt_ = subquery->get_select_item_size();
      }
    } else if (IS_COMMON_COMPARISON_OP(expr->get_expr_type()) && expr->get_expr_type() != T_OP_NSEQ) {
      if (T_OP_ROW == left->get_expr_type()
          && (T_OP_ROW != right->get_expr_type()
              || left->get_param_count() != right->get_param_count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected row expr", K(ret), K(left), K(right));
      } else {
        // Normalize so that the const operand is on the right, to align the left
        // expr across OR branches with an ANY_SUBQUERY anchor.
        bool is_right_const = right->is_const_expr();
        info.type_ = OrBranchInfo::SCALAR_CMP;
        info.left_expr_ = is_right_const ? left : right;
        info.right_expr_ = is_right_const ? right : left;
        info.scalar_cmp_type_ = is_right_const ? expr->get_expr_type() : get_opposite_compare_type(expr->get_expr_type());
        info.output_column_cnt_ = (T_OP_ROW == info.left_expr_->get_expr_type())
                                  ? info.left_expr_->get_param_count() : 1;
      }
    } else if (T_OP_IN == expr->get_expr_type()) {
      if (T_OP_ROW != right->get_expr_type()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected row expr", K(ret), K(right));
      } else {
        info.type_ = OrBranchInfo::INLIST;
        info.left_expr_ = left;
        info.right_expr_ = right;
        info.scalar_cmp_type_ = T_OP_EQ;
        info.output_column_cnt_ = (T_OP_ROW == left->get_expr_type()) ? left->get_param_count() : 1;
      }
    } else {
      trans_param.can_transform_ = false;
      OPT_TRACE("OR subquery expansion: unsupported branch type", expr);
    }
    if (OB_SUCC(ret) && trans_param.can_transform_ && OB_FAIL(branches.push_back(info))) {
      LOG_WARN("failed to push back branch info", K(ret));
    }
  }
  return ret;
}

/*
 * Check that all OR branches are "homogeneous" so they can merge into =ANY(UNION ALL ...).
 *
 * The first ANY_SUBQUERY branch is the anchor.  Every other branch must match:
 *   - same comparison operator, same column count, same left-hand expr
 *   - ANY_SUBQUERY branches: correlated conditions must be isomorphic
 *     (same structure & correlation pattern — needed for UNION ALL de-correlation)
 *   - SCALAR_CMP / INLIST branches: rejected if anchor is correlated
 *     (can't mix correlated subquery + constant in a single UNION ALL de-correlation)
 *
 * Valid:   c1 IN (SELECT c2 FROM t2) OR c1 = 100 OR c1 IN (1,2,3)
 *          all non-correlated, same lhs, same op → merged into =ANY(UNION ALL ...)
 *
 * Invalid: c1 IN (SELECT c2 FROM t2 WHERE t2.c3 = t1.c3) OR c1 = 100
 *          anchor is correlated but `c1 = 100` is scalar — rejected
 */
int ObTransformSubqueryUnnest::anyall_check_validity_for_or_branches(ObRawExpr *expr,
                                                                     AnyAllSQParam &trans_param,
                                                                     ObIArray<OrBranchInfo> &branches)
{
  int ret = OB_SUCCESS;
  int64_t anchor_idx = -1;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null expr", K(ret));
  } else if (!trans_param.can_transform_) {
    // do nothing
  } else if (branches.count() < 2) {
    trans_param.can_transform_ = false;
  } else {
    // set the first ANY subquery branch as the anchor branch
    for (int64_t i = 0; OB_SUCC(ret) && -1 == anchor_idx && i < branches.count(); ++i) {
      OrBranchInfo &info = branches.at(i);
      if (OrBranchInfo::ANY_SUBQUERY == info.type_) {
        anchor_idx = i;
      }
    }
  }
  if (OB_FAIL(ret) || !trans_param.can_transform_) {
  } else if (anchor_idx < 0 || anchor_idx >= branches.count()) {
    trans_param.can_transform_ = false;
    OPT_TRACE("OR subquery expansion: no any subquery branch");
  } else {
    OrBranchInfo &anchor_info = branches.at(anchor_idx);
    if (OB_ISNULL(anchor_info.left_expr_)
        || OB_ISNULL(anchor_info.right_expr_)
        || OB_UNLIKELY(!anchor_info.right_expr_->is_query_ref_expr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected anchor info", K(ret), K(anchor_info));
    }
    for (int64_t i = 0; OB_SUCC(ret) && trans_param.can_transform_ && i < branches.count(); ++i) {
      OrBranchInfo &info = branches.at(i);
      if (OB_FAIL(anyall_check_single_or_branch_validity(expr, info, trans_param))) {
        LOG_WARN("failed to check single or branch", K(ret));
      } else if (!trans_param.can_transform_ || i == anchor_idx) {
        // skip isomorphic check for invalid or anchor branch
      } else if (OB_FAIL(anyall_check_or_branches_isomorphic(anchor_info, info, trans_param))) {
        LOG_WARN("failed to check or branch isomorphic", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformSubqueryUnnest::anyall_check_single_or_branch_validity(ObRawExpr *expr,
                                                                      OrBranchInfo &info,
                                                                      AnyAllSQParam &trans_param)
{
  int ret = OB_SUCCESS;
  ObRawExpr *left = info.left_expr_;
  ObRawExpr *right = info.right_expr_;
  if (OB_ISNULL(left) || OB_ISNULL(right)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL", K(ret), K(left), K(right));
  } else if (left->has_flag(CNT_SUB_QUERY)) {
    trans_param.can_transform_ = false;
    OPT_TRACE("OR subquery expansion: lhs contains subquery", expr);
  } else if (OrBranchInfo::SCALAR_CMP == info.type_) {
    if (!right->is_const_expr()) {
      trans_param.can_transform_ = false;
      OPT_TRACE("OR subquery expansion: rhs of cmp contains non-const value", expr);
    }
  } else if (OrBranchInfo::INLIST == info.type_) {
    if (!right->is_const_expr()) {
      trans_param.can_transform_ = false;
      OPT_TRACE("OR subquery expansion: inlist contains non-const value", expr);
    }
  } else if (OrBranchInfo::ANY_SUBQUERY == info.type_) {
    ObQueryRefRawExpr *query_ref_expr = static_cast<ObQueryRefRawExpr *>(right);
    ObSelectStmt *subquery = query_ref_expr->get_ref_stmt();
    if (OB_ISNULL(subquery)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null subquery stmt", K(ret));
    } else if (subquery->is_set_stmt()) {
      trans_param.can_transform_ = false;
      OPT_TRACE("OR subquery expansion: unsupported nested set stmt");
    } else if (OB_FAIL(check_hint_allowed_unnest(*stmt_,
                                                 *subquery,
                                                 trans_param.outline_loc_ + trans_param.or_subquery_stmts_.count(),
                                                 trans_param.can_transform_))) {
      LOG_WARN("failed to check hint allowed unnest", K(ret));
    } else if (!trans_param.can_transform_) {
      OPT_TRACE("OR subquery expansion: hint not allowed");
    } else if (OB_FAIL(anyall_check_subquery_validity_inner(trans_param,
                                                            query_ref_expr,
                                                            subquery))) {
      LOG_WARN("failed to check subquery validity", K(ret));
    } else if (trans_param.has_nested_correlated_subquery_) {
      trans_param.can_transform_ = false;
      OPT_TRACE("OR subquery expansion: nested correlated subquery");
    } else if (!query_ref_expr->has_exec_param()) {
      // non-correlated subquery, push to or_subquery_stmts_ directly
      if (OB_FAIL(trans_param.or_subquery_stmts_.push_back(subquery))) {
        LOG_WARN("failed to push back subquery stmt", K(ret));
      }
    } else if (OB_FAIL(ObTransformUtils::check_correlated_exprs_can_pullup(query_ref_expr->get_exec_params(),
                                                                           *subquery,
                                                                           trans_param.can_transform_))) {
      LOG_WARN("failed to check correlated exprs can pullup", K(ret));
    } else if (!trans_param.can_transform_) {
      OPT_TRACE("OR subquery expansion: correlated exprs can't pullup");
    } else if (OB_FAIL(trans_param.or_subquery_stmts_.push_back(subquery))) {
      LOG_WARN("failed to push back subquery stmt", K(ret));
    } else {
      trans_param.need_create_spj_ = true;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected branch type", K(ret), K(info.type_));
  }
  return ret;
}

// Isomorphic comparison of a non-anchor branch against the anchor branch.
// the anchor branch should be an any subquery
int ObTransformSubqueryUnnest::anyall_check_or_branches_isomorphic(const OrBranchInfo &anchor,
                                                                   const OrBranchInfo &branch,
                                                                   AnyAllSQParam &trans_param)
{
  int ret = OB_SUCCESS;
  ObQueryRefRawExpr *anchor_query_ref_expr = NULL;  // NOT NULL, anchor must be an any subquery
  ObSelectStmt *anchor_subquery = NULL;             // NOT NULL, anchor must be an any subquery
  ObQueryRefRawExpr *branch_query_ref_expr = NULL;  // NOT NULL only if branch is an any subquery
  ObSelectStmt *branch_subquery = NULL;             // NOT NULL only if branch is an any subquery
  if (OB_ISNULL(anchor.left_expr_) || OB_ISNULL(anchor.right_expr_)
      || OB_UNLIKELY(anchor.type_ != OrBranchInfo::ANY_SUBQUERY)
      || OB_UNLIKELY(!anchor.right_expr_->is_query_ref_expr())
      || OB_ISNULL(branch.left_expr_) || OB_ISNULL(branch.right_expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected param", K(ret),
             K(anchor.left_expr_), K(anchor.right_expr_), K(anchor.type_),
             K(branch.left_expr_), K(branch.right_expr_));
  } else if (FALSE_IT(anchor_query_ref_expr = static_cast<ObQueryRefRawExpr *>(anchor.right_expr_))) {
  } else if (OB_ISNULL(anchor_subquery = anchor_query_ref_expr->get_ref_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null anchor subquery", K(ret));
  } else if (anchor.scalar_cmp_type_ != branch.scalar_cmp_type_) {
    trans_param.can_transform_ = false;
    OPT_TRACE("OR subquery expansion: comparison operators differ");
  } else if (anchor.output_column_cnt_ != branch.output_column_cnt_) {
    trans_param.can_transform_ = false;
    OPT_TRACE("OR subquery expansion: select item counts differ");
  } else if (!ObRawExprUtils::is_same_raw_expr(anchor.left_expr_, branch.left_expr_)) {
    trans_param.can_transform_ = false;
    OPT_TRACE("OR subquery expansion: left exprs differ");
  } else if (OrBranchInfo::ANY_SUBQUERY == branch.type_) {
    if (OB_UNLIKELY(!branch.right_expr_->is_query_ref_expr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected branch right expr", K(ret), K(branch.right_expr_));
    } else if (FALSE_IT(branch_query_ref_expr = static_cast<ObQueryRefRawExpr *>(branch.right_expr_))) {
    } else if (OB_ISNULL(branch_subquery = branch_query_ref_expr->get_ref_stmt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null branch subquery", K(ret));
    } else if (anchor_query_ref_expr->has_exec_param() != branch_query_ref_expr->has_exec_param()) {
      trans_param.can_transform_ = false;
      OPT_TRACE("OR subquery expansion: mixed correlated and non-correlated exprs");
    }
  } else if (anchor_query_ref_expr->has_exec_param()) {
    // INLIST/SCALAR CMP must be non-correlated
    trans_param.can_transform_ = false;
    OPT_TRACE("OR subquery expansion: mixed correlated and non-correlated exprs");
  }
  // Ensure output column types are equal-transitive so they can be safely UNION-ed
  if (OB_SUCC(ret) && trans_param.can_transform_) {
    if (OrBranchInfo::ANY_SUBQUERY == branch.type_ && branch_query_ref_expr->has_exec_param()) {
      // For correlated ANY_SUBQUERY,
      // use `check_correlated_condition_isomorphic` to check between anchor and current subquery:
      // 1. if correlated conditions are isomorphic
      // 2. if result types of output columns (after pulled-up) are equal transitive
      //    (use equal-transitive check to align with non-correlated scenario)
      ObSEArray<ObRawExpr*, 4> dummy_left_select_exprs;
      ObSEArray<ObRawExpr*, 4> dummy_right_select_exprs;
      if (OB_FAIL(ObTransformUtils::check_correlated_condition_isomorphic(anchor_subquery,
                                                                          branch_subquery,
                                                                          anchor_query_ref_expr->get_exec_params(),
                                                                          branch_query_ref_expr->get_exec_params(),
                                                                          trans_param.can_transform_,
                                                                          dummy_left_select_exprs,
                                                                          dummy_right_select_exprs,
                                                                          true,      /* skip_const_in_select */
                                                                          true,      /* skip_const_in_where */
                                                                          false))) { /* strict_res_type_check */
        LOG_WARN("failed to check correlated subquery isomorphic", K(ret));
      } else if (!trans_param.can_transform_) {
        OPT_TRACE("OR subquery expansion: correlated subquery not isomorphic");
      }
    } else {
      // Only non-correlated cases reach here:
      // - SCALAR_CMP / INLIST branches (which must be non-correlated per the check above)
      // - non-correlated ANY_SUBQUERY branches
      // For correlated ANY_SUBQUERY, equal-transitive check has been done inside
      // check_correlated_condition_isomorphic above.
      for (int64_t i = 0; OB_SUCC(ret) && trans_param.can_transform_ && i < anchor.output_column_cnt_; ++i) {
        ObRawExpr *anchor_select_expr = anchor_subquery->get_select_item(i).expr_;
        ObRawExpr *branch_column_expr = NULL;
        if (OrBranchInfo::SCALAR_CMP == branch.type_) {
          branch_column_expr = (1 == anchor.output_column_cnt_)
                               ? branch.right_expr_
                               : branch.right_expr_->get_param_expr(i);
        } else if (OrBranchInfo::INLIST == branch.type_) {
          if (OB_ISNULL(branch.right_expr_->get_param_expr(0))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected null first row in inlist", K(ret));
          } else {
            branch_column_expr = (1 == anchor.output_column_cnt_)
                                 ? branch.right_expr_->get_param_expr(0)
                                 : branch.right_expr_->get_param_expr(0)->get_param_expr(i);
          }
        } else if (OrBranchInfo::ANY_SUBQUERY == branch.type_) {
          branch_column_expr = branch_subquery->get_select_item(i).expr_;
        }
        if (OB_FAIL(ret)) {
        } else if (OB_ISNULL(branch_column_expr) || OB_ISNULL(anchor_select_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null expr", K(ret), K(i), K(branch_column_expr), K(anchor_select_expr));
        } else if (OB_FAIL(ObRelationalExprOperator::is_equal_transitive(anchor_select_expr->get_result_type(),
                                                                         branch_column_expr->get_result_type(),
                                                                         trans_param.can_transform_))) {
          LOG_WARN("failed to check is_equal_transitive", K(ret));
        } else if (!trans_param.can_transform_) {
          OPT_TRACE("OR subquery expansion: rhs exprs not equal transitive");
        }
      }
    }
  }
  return ret;
}

/*
 * Build the expanded =ANY(UNION ALL ...) expression from the classified branches.
 *
 * For each branch:
 *   SCALAR_CMP → create a FROM-less SELECT with the value expr as select item
 *   INLIST     → create a VALUES-table SELECT
 *   ANY_SUBQUERY → use the existing subquery stmt directly
 *
 * Then wrap all child stmts in a UNION ALL and create a new ObQueryRefRawExpr.
 */
int ObTransformSubqueryUnnest::anyall_expand_or_cond(ObIArray<OrBranchInfo> &branches,
                                                     ObRawExpr *&expanded_expr)
{
  int ret = OB_SUCCESS;
  expanded_expr = NULL;
  ObSEArray<ObSelectStmt *, 4> child_stmts;
  int64_t select_item_count = OB_INVALID;
  ObRawExpr *left_expr = NULL;
  ObItemType sq_cmp_type = T_INVALID;
  ObSEArray<ObExecParamRawExpr *, 4> exec_params;

  if (OB_ISNULL(ctx_)
      || OB_ISNULL(ctx_->expr_factory_)
      || OB_ISNULL(ctx_->stmt_factory_)
      || OB_ISNULL(ctx_->session_info_)
      || OB_ISNULL(stmt_)
      || OB_UNLIKELY(0 == branches.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K_(ctx), K_(stmt), K(branches.count()));
  } else {
    const OrBranchInfo &anchor_info = branches.at(0);
    select_item_count = anchor_info.output_column_cnt_;
    left_expr = anchor_info.left_expr_;
    sq_cmp_type = ObRawExprInfoExtractor::get_subquery_comparison_type(anchor_info.scalar_cmp_type_);
    if (OB_ISNULL(left_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null left expr", K(ret));
    } else if (OB_UNLIKELY(T_INVALID == sq_cmp_type)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected sq_cmp_type", K(ret), K(sq_cmp_type));
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < branches.count(); ++i) {
    const OrBranchInfo &branch = branches.at(i);
    ObSelectStmt *child_stmt = NULL;
    if (OrBranchInfo::ANY_SUBQUERY == branch.type_) {
      ObQueryRefRawExpr *subquery_expr = static_cast<ObQueryRefRawExpr *>(branch.right_expr_);
      if (OB_ISNULL(subquery_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null subquery expr", K(ret));
      } else if (OB_FAIL(append_array_no_dup(exec_params, subquery_expr->get_exec_params()))) {
        LOG_WARN("failed to append exec param exprs", K(ret));
      } else {
        child_stmt = subquery_expr->get_ref_stmt();
      }
    } else if (OrBranchInfo::SCALAR_CMP == branch.type_) {
      if (OB_FAIL(anyall_expand_or_create_stmt_for_scalar_cmp(branch, child_stmt))) {
        LOG_WARN("failed to create stmt for scalar cmp branch", K(ret));
      }
    } else if (OrBranchInfo::INLIST == branch.type_) {
      if (OB_FAIL(anyall_expand_or_create_stmt_for_inlist(branch, select_item_count, child_stmt))) {
        LOG_WARN("failed to create stmt for inlist branch", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unsupported branch type", K(ret), K(branch.type_));
    }
    if (FAILEDx(child_stmts.push_back(child_stmt))) {
      LOG_WARN("failed to push back child stmt", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    ObQueryRefRawExpr *union_query_ref = NULL;
    if (OB_FAIL(anyall_expand_or_create_union_query_ref(child_stmts,
                                                        exec_params,
                                                        select_item_count,
                                                        union_query_ref))) {
      LOG_WARN("failed to create union query ref", K(ret));
    } else {
      ObOpRawExpr *new_anyall_expr = NULL;
      ObSubQueryKey any_key = T_WITH_ANY;
      if (OB_FAIL(ctx_->expr_factory_->create_raw_expr(sq_cmp_type, new_anyall_expr))) {
        LOG_WARN("failed to create anyall expr", K(ret), K(sq_cmp_type));
      } else if (OB_ISNULL(new_anyall_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null anyall expr", K(ret));
      } else if (FALSE_IT(new_anyall_expr->set_subquery_key(any_key))) {
      } else if (OB_FAIL(new_anyall_expr->set_param_exprs(left_expr, union_query_ref))) {
        LOG_WARN("failed to set params for anyall expr", K(ret));
      } else if (OB_FAIL(new_anyall_expr->formalize(ctx_->session_info_))) {
        LOG_WARN("failed to formalize anyall expr", K(ret));
      } else {
        expanded_expr = new_anyall_expr;
      }
    }
  }

  return ret;
}

// Build a FROM-less SELECT stmt with the scalar value(s) from a SCALAR_CMP branch as select items.
int ObTransformSubqueryUnnest::anyall_expand_or_create_stmt_for_scalar_cmp(const OrBranchInfo &branch,
                                                                           ObSelectStmt *&child_stmt)
{
  int ret = OB_SUCCESS;
  child_stmt = NULL;
  if (OB_FAIL(ctx_->stmt_factory_->create_stmt(child_stmt))) {
    LOG_WARN("failed to create stmt for scalar branch", K(ret));
  } else if (OB_ISNULL(child_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null child_stmt", K(ret));
  } else {
    child_stmt->set_query_ctx(stmt_->get_query_ctx());
    if (T_OP_ROW == branch.left_expr_->get_expr_type()) {
      ObRawExpr *right = branch.right_expr_;
      if (OB_ISNULL(right) || T_OP_ROW != right->get_expr_type()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected right expr for multi-col scalar cmp", K(ret));
      }
      for (int64_t col = 0; OB_SUCC(ret) && col < right->get_param_count(); ++col) {
        ObRawExpr *col_val = right->get_param_expr(col);
        if (OB_ISNULL(col_val)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("null col val expr", K(ret));
        } else if (OB_FAIL(ObTransformUtils::create_select_item(*ctx_->allocator_, col_val, child_stmt))) {
          LOG_WARN("failed to add select item", K(ret));
        }
      }
    } else if (OB_FAIL(ObTransformUtils::create_select_item(*ctx_->allocator_, branch.right_expr_, child_stmt))) {
      LOG_WARN("failed to add select item", K(ret));
    }
    if (OB_FAIL(child_stmt->adjust_statement_id(ctx_->allocator_,
                                                ctx_->src_qb_name_,
                                                ctx_->src_hash_val_))) {
      LOG_WARN("failed to adjust statement id for scalar branch", K(ret));
    } else if (OB_FAIL(child_stmt->formalize_stmt(ctx_->session_info_, false))) {
      LOG_WARN("failed to formalize scalar branch stmt", K(ret));
    }
  }
  return ret;
}

// Flatten IN-list rows and build a VALUES-table SELECT stmt for an INLIST branch.
int ObTransformSubqueryUnnest::anyall_expand_or_create_stmt_for_inlist(const OrBranchInfo &branch,
                                                                       int64_t select_item_count,
                                                                       ObSelectStmt *&child_stmt)
{
  int ret = OB_SUCCESS;
  child_stmt = NULL;
  ObSEArray<ObRawExpr *, 8> flat_vals;
  ObOpRawExpr *inlist_row_expr = static_cast<ObOpRawExpr *>(branch.right_expr_);
  if (OB_ISNULL(inlist_row_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null inlist row expr", K(ret));
  }
  for (int64_t row = 0; OB_SUCC(ret) && row < inlist_row_expr->get_param_count(); ++row) {
    ObRawExpr *row_expr = inlist_row_expr->get_param_expr(row);
    if (OB_ISNULL(row_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null row expr", K(ret));
    } else if (1 == select_item_count) {
      if (OB_FAIL(flat_vals.push_back(row_expr))) {
        LOG_WARN("failed to push back flat val", K(ret));
      }
    } else if (T_OP_ROW == row_expr->get_expr_type() && select_item_count == row_expr->get_param_count()) {
      for (int64_t col = 0; OB_SUCC(ret) && col < select_item_count; ++col) {
        ObRawExpr *col_expr = row_expr->get_param_expr(col);
        if (OB_ISNULL(col_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null col expr", K(ret));
        } else if (OB_FAIL(flat_vals.push_back(col_expr))) {
          LOG_WARN("failed to push back flat val", K(ret));
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected inlist row expr", K(ret), K(select_item_count), K(row_expr));
    }
  }
  if (FAILEDx(anyall_expand_or_create_values_table_for_inlist(flat_vals, select_item_count, child_stmt))) {
    LOG_WARN("failed to create values table for inlist", K(ret));
  }
  return ret;
}

/*
 * Build a VALUES-table subquery for an IN-list's value expressions.
 * Each element of value_exprs is one row; column_cnt is 1 for single-column,
 * or >1 for multi-column IN.
 */
int ObTransformSubqueryUnnest::anyall_expand_or_create_values_table_for_inlist(
  ObIArray<ObRawExpr *> &value_exprs,
  int64_t column_cnt,
  ObSelectStmt *&values_stmt)
{
  int ret = OB_SUCCESS;
  values_stmt = NULL;
  ObValuesTableDef *table_def = NULL;

  if (OB_UNLIKELY(column_cnt <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid column_cnt", K(ret), K(column_cnt));
  } else if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->allocator_) || OB_ISNULL(ctx_->expr_factory_)
      || OB_ISNULL(ctx_->stmt_factory_) || OB_ISNULL(ctx_->session_info_)
      || OB_ISNULL(stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null ctx", K(ret));
  } else if (OB_ISNULL(table_def = OB_NEWx(ObValuesTableDef, ctx_->allocator_, *ctx_->allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate ObValuesTableDef", K(ret));
  } else if (OB_FAIL(ctx_->stmt_factory_->create_stmt(values_stmt))) {
    LOG_WARN("failed to create select stmt", K(ret));
  } else if (OB_ISNULL(values_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null values_stmt", K(ret));
  } else {
    // fill information of table_def
    table_def->access_type_ = ObValuesTableDef::ACCESS_EXPR;
    table_def->is_const_ = true;
    const int64_t row_cnt = value_exprs.count() / column_cnt;
    table_def->column_cnt_ = column_cnt;
    table_def->row_cnt_ = row_cnt;
    if (OB_FAIL(table_def->access_exprs_.assign(value_exprs))) {
      LOG_WARN("failed to assign access exprs", K(ret));
    }
    // this function is mainly for filling table_def->column_types_ rather than adding cast
    // the validity check ensures that exprs in each column shares the same result type
    // so no CAST should be added
    if (FAILEDx(ObOptimizerUtil::try_add_cast_to_select_list(ctx_->allocator_,
                                                             ctx_->session_info_,
                                                             ctx_->expr_factory_,
                                                             column_cnt,
                                                             false, /* is_set_distinct */
                                                             false, /* need_merge_type */
                                                             table_def->access_exprs_,
                                                             &table_def->column_types_))) {
      LOG_WARN("failed to add cast to select list", K(ret));
    }
    // according to ObDMLResolver::estimate_values_table_stats, for values table of ACCESS_EXPR
    // column_ndvs_ = row_cnt
    // num_null = 0.0
    for (int64_t col = 0; OB_SUCC(ret) && col < column_cnt; ++col) {
      if (OB_ISNULL(value_exprs.at(col))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null expr when reading types", K(ret));
      } else if (OB_FAIL(table_def->column_ndvs_.push_back(row_cnt))) {
        // use row_cnt as a conservative ndv estimate for const value lists
        LOG_WARN("failed to push back column ndv", K(ret));
      } else if (OB_FAIL(table_def->column_nnvs_.push_back(0))) {
        // assume no nulls in const value lists
        LOG_WARN("failed to push back column nnv", K(ret));
      }
    }
    // create values stmt by table_def
    if (OB_SUCC(ret)) {
      values_stmt->set_query_ctx(stmt_->get_query_ctx());
      if (OB_FAIL(ObResolverUtils::create_values_table_query(ctx_->session_info_,
                                                             ctx_->allocator_,
                                                             ctx_->expr_factory_,
                                                             stmt_->get_query_ctx(),
                                                             values_stmt,
                                                             table_def))) {
        LOG_WARN("failed to create values table query", K(ret));
      } else if (OB_FAIL(values_stmt->adjust_statement_id(ctx_->allocator_,
                                                          ctx_->src_qb_name_,
                                                          ctx_->src_hash_val_))) {
        LOG_WARN("failed to adjust statement id for values table stmt", K(ret));
      } else if (OB_FAIL(values_stmt->formalize_stmt(ctx_->session_info_, false))) {
        LOG_WARN("failed to formalize values table stmt", K(ret));
      }
    }
  }
  return ret;
}

// Wrap child_stmts in a UNION ALL stmt and create a QueryRef expr with column_types populated.
int ObTransformSubqueryUnnest::anyall_expand_or_create_union_query_ref(
  ObIArray<ObSelectStmt *> &child_stmts,
  ObIArray<ObExecParamRawExpr *> &exec_params,
  int64_t select_item_count,
  ObQueryRefRawExpr *&union_query_ref)
{
  int ret = OB_SUCCESS;
  union_query_ref = NULL;
  ObSelectStmt *union_stmt = NULL;
  if (OB_FAIL(ObTransformUtils::create_set_stmt(ctx_,
                                                ObSelectStmt::UNION,
                                                false /* not distinct (UNION ALL) */,
                                                child_stmts,
                                                union_stmt))) {
    LOG_WARN("failed to create UNION ALL stmt", K(ret));
  } else if (OB_ISNULL(union_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null union_stmt", K(ret));
  } else if (OB_FAIL(ctx_->expr_factory_->create_raw_expr(T_REF_QUERY, union_query_ref))) {
    LOG_WARN("failed to create query ref raw expr", K(ret));
  } else if (OB_ISNULL(union_query_ref)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null query_ref", K(ret));
  } else if (OB_FAIL(ObTransformUtils::deduplicate_exec_params_for_set_stmt(union_stmt, exec_params))) {
    LOG_WARN("failed to deduplicate exec params", K(ret));
  } else if (OB_FAIL(union_query_ref->get_exec_params().assign(exec_params))) {
    LOG_WARN("failed to append exec param exprs", K(ret));
  /*
   * `gen_set_target_list` would merge result types for UNION stmt and may add necessary cast to each branch
   * In previous validity check phase we already ensured that
   * the result types of output columns of each branch are equal-transitive
   * so the type promotion and CAST added here should cause no side-effect
   * pass `false` to need_merge_type to prohibit type merge for char/varchar types
   * otherwise the comparison result of before & after rewrite may differ in oracle mode
   */
  } else if (OB_FAIL(ObOptimizerUtil::gen_set_target_list(ctx_->allocator_,
                                                          ctx_->session_info_,
                                                          ctx_->expr_factory_,
                                                          union_stmt,
                                                          false))) { /* need_merge_type */
    LOG_WARN("failed to gen set target list", K(ret));
  } else {
    union_query_ref->set_ref_stmt(union_stmt);
    union_query_ref->set_output_column(select_item_count);
    union_query_ref->set_is_set(true);
    // column_types_ must be populated before formalize(); deduce them from
    // the UNION ALL stmt's select items (which have already been formalized)
    ObIArray<SelectItem> &union_sel = union_stmt->get_select_items();
    for (int64_t col = 0; OB_SUCC(ret) && col < select_item_count; ++col) {
      if (col >= union_sel.count() || OB_ISNULL(union_sel.at(col).expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("union stmt select item out of range or null", K(ret), K(col));
      } else if (OB_FAIL(union_query_ref->add_column_type(union_sel.at(col).expr_->get_result_type()))) {
        LOG_WARN("failed to add column type to query ref", K(ret));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(union_query_ref->formalize(ctx_->session_info_))) {
      LOG_WARN("failed to formalize query ref expr", K(ret));
    }
  }
  return ret;
}

// ============================== end of OR subquery expansion ==============================

// ============================== begin of singlerow subquery ==============================

/*
 * Singlerow entry — unnest scalar subqueries that return at most one row
 * (guaranteed by uniqueness check).
 *
 * Scans two sources:
 *   1. WHERE conditions             (in_where_cond = true)
 *   2. post-join expressions        (in_where_cond = false)
 *      - SELECT list (when no GROUP BY)
 *      - GROUP BY / aggregate exprs (when has GROUP BY)
 *      - UPDATE SET assignment values
 *
 *   -- inner join (WHERE + null-rejecting, e.g. `c1 = (subq)`):
 *   SELECT * FROM t1 WHERE c1 = (SELECT c2 FROM t2 WHERE t2.pk = t1.pk)
 *   →  SELECT * FROM t1, t2 WHERE t2.pk = t1.pk AND t1.c1 = t2.c2
 *
 *   -- outer join (SELECT list, or WHERE + non-null-rejecting like `c1 <=> (subq)`):
 *   SELECT (SELECT c2 FROM t2 WHERE t2.pk = t1.pk) FROM t1
 *   →  SELECT t2.c2 FROM t1 LEFT JOIN t2 ON t2.pk = t1.pk
 *
 *   -- multi-column in WHERE (4.6.1+):
 *   WHERE (a, b) = (SELECT c, d FROM t2 WHERE t2.pk = t1.pk)
 *   →  WHERE (a, b) = (t2.c, t2.d)  (with t2 inner-joined, subquery ref replaced by T_OP_ROW)
 *
 *   -- UPDATE vector assignment:
 *   UPDATE t1 SET (c1, c2) = (SELECT a, b FROM t2 WHERE t2.pk = t1.pk)
 *   →  UPDATE t1 LEFT JOIN t2 ON t2.pk = t1.pk SET c1 = t2.a, c2 = t2.b
 */
int ObTransformSubqueryUnnest::singlerow_transform_subquery(ObIArray<ObSelectStmt *> &unnest_stmts,
                                                            bool &trans_happened)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 4> cond_exprs;
  ObSEArray<ObRawExpr *, 4> post_join_exprs;
  // single-row subquery can be shared-referenced across multiple root exprs;
  // trans_params accumulates across calls so already-collected query refs are
  // naturally deduped by the per-expr validity check, avoiding duplicate unnest.
  ObSEArray<SingleRowSQParam, 4> trans_params;
  trans_happened = false;
  if (OB_ISNULL(stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (0 == stmt_->get_from_item_size() || !stmt_->has_subquery()) {
    /*do nothing*/
  } else if (OB_FAIL(cond_exprs.assign(stmt_->get_condition_exprs()))) {
    LOG_WARN("failed to get non semi conditions", K(ret));
  } else if (OB_FAIL(ObTransformUtils::get_post_join_exprs(stmt_, post_join_exprs, true))) {
    LOG_WARN("failed to get post join exprs", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < cond_exprs.count(); ++i) {
    if (OB_FAIL(singlerow_transform_subquery_inner(cond_exprs.at(i),
                                                   true,
                                                   trans_params,
                                                   unnest_stmts,
                                                   trans_happened))) {
      LOG_WARN("failed to transform single row subquery", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < post_join_exprs.count(); ++i) {
    if (OB_FAIL(singlerow_transform_subquery_inner(post_join_exprs.at(i),
                                                   false,
                                                   trans_params,
                                                   unnest_stmts,
                                                   trans_happened))) {
      LOG_WARN("failed to transform single row subquery", K(ret));
    }
  }
  return ret;
}

int ObTransformSubqueryUnnest::singlerow_transform_subquery_inner(
  ObRawExpr *expr,
  const bool in_where_cond,
  ObIArray<SingleRowSQParam> &trans_params,
  ObIArray<ObSelectStmt *> &unnest_stmts,
  bool &trans_happened)
{
  int ret = OB_SUCCESS;
  bool is_vector_assign = false;
  const int64_t start_idx = trans_params.count();
  if (OB_ISNULL(expr) || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (OB_FAIL(singlerow_gather_transform_params(expr,
                                                       expr,
                                                       in_where_cond,
                                                       trans_params,
                                                       ctx_->trans_list_loc_))) {
    // outline_loc base: anyall and singlerow are mutually exclusive within one call
    // of transform_one_stmt_with_outline, so unnest_stmts is always 0 when entering
    // singlerow here. The relative position within this call is expressed by
    // trans_params.count() at check time.
    LOG_WARN("failed to gather single row subquery params", K(ret));
  } else {
    is_vector_assign = in_where_cond ? false : expr->has_flag(CNT_ALIAS);
  }
  for (int64_t i = start_idx; OB_SUCC(ret) && i < trans_params.count(); ++i) {
    ObSelectStmt *subquery = NULL;
    SingleRowSQParam &param = trans_params.at(i);
    ObQueryRefRawExpr *query_ref_expr = param.query_ref_expr_;
    if (OB_ISNULL(query_ref_expr) || OB_ISNULL(subquery = query_ref_expr->get_ref_stmt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(singlerow_do_unnest_subquery(param, is_vector_assign))) {
      LOG_WARN("failed to unnest single row subquery", K(ret));
    } else if (OB_FAIL(unnest_stmts.push_back(subquery))) {
      LOG_WARN("failed to push back", K(ret));
    } else {
      trans_happened = true;
    }
  }
  if (OB_FAIL(ret) || !trans_happened) {
  } else if (OB_FAIL(singlerow_adjust_subquery_comparison_expr(expr))) {
    LOG_WARN("failed to adjust subquery comparison operator", K(ret));
  } else if (OB_FAIL(stmt_->formalize_stmt(ctx_->session_info_, false))) {
    LOG_WARN("failed to formalize stmt", K(ret));
  }
  return ret;
}

int ObTransformSubqueryUnnest::singlerow_gather_transform_params(
  ObRawExpr *root_expr,
  ObRawExpr *expr,
  bool in_where_cond,
  ObIArray<SingleRowSQParam> &trans_params,
  const int64_t outline_loc)
{
  int ret = OB_SUCCESS;
  bool is_valid = true;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (OB_FAIL(singlerow_check_transform_validity_for_expr(root_expr,
                                                                 expr,
                                                                 in_where_cond,
                                                                 trans_params,
                                                                 outline_loc,
                                                                 is_valid))) {
    LOG_WARN("failed to check subquery basic validity", K(ret));
  } else if (!is_valid) {
  } else if (!expr->is_query_ref_expr()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(singlerow_gather_transform_params(root_expr,
                                                               expr->get_param_expr(i),
                                                               in_where_cond,
                                                               trans_params,
                                                               outline_loc)))) {
        LOG_WARN("failed to get single row query", K(ret));
      }
    }
  } else {
    ObQueryRefRawExpr *query = static_cast<ObQueryRefRawExpr *>(expr);
    bool is_valid = false;
    SingleRowSQParam param;
    if (OB_FAIL(singlerow_gather_one_transform_param(root_expr,
                                                     query,
                                                     in_where_cond,
                                                     param,
                                                     is_valid))) {
      LOG_WARN("failed to build single row subquery param", K(ret));
    } else if (!is_valid) {
    } else if (OB_FAIL(trans_params.push_back(param))) {
      LOG_WARN("failed to push back single row query param", K(ret));
    }
  }
  return ret;
}

// Expr-level validity gate.  Rejects non-subquery exprs, hierarchical queries, then
// for query-ref exprs checks: SPJ, hint, output uniqueness, correlated from-items,
// and context-specific rules (multi-column in WHERE, select-expr without hint, etc.).
int ObTransformSubqueryUnnest::singlerow_check_transform_validity_for_expr(
  ObRawExpr *root_expr,
  ObRawExpr *expr,
  bool in_where_cond,
  ObIArray<SingleRowSQParam> &trans_params,
  const int64_t outline_loc,
  bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  if (OB_ISNULL(expr) || OB_ISNULL(stmt_)) {
    is_valid = false;
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(expr), K(stmt_));
  } else if (!expr->has_flag(CNT_SUB_QUERY)
             || expr->has_flag(IS_WITH_ANY)
             || expr->has_flag(IS_WITH_ALL)
             || T_OP_EXISTS == expr->get_expr_type()
             || T_OP_NOT_EXISTS == expr->get_expr_type()) {
    is_valid = false;
    OPT_TRACE("expr is not a subquery, or is not required to be single-row");
  } else if (expr->has_hierarchical_query_flag()) {
    // if expr is a hierarchical query
    // 1. it's difficult to check its uniqueness
    // 2. it may cause execution issues after predicate pushdown
    //  e.g. select * from t1 where prior 3 != (select c2 from t2 where c1 = 2)
    //       start with c1 = 1 connect by prior c1 = c2;
    is_valid = false;
    OPT_TRACE("expr is a hierarchical query, do not rewrite");
  } else if (!expr->is_query_ref_expr()) {
    // the following checks are for query ref expr only
  } else {
    bool is_from_item_correlated = false;
    ObQueryRefRawExpr *query_ref_expr = static_cast<ObQueryRefRawExpr *>(expr);
    ObSelectStmt *subquery = query_ref_expr->get_ref_stmt();
    const ObQueryCtx *query_ctx = stmt_->get_query_ctx();
    if (OB_ISNULL(subquery) || OB_ISNULL(query_ctx)) {
      is_valid = false;
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret), K(subquery), K(query_ctx));
    }
    // invalid if the subquery has already been gathered
    for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < trans_params.count(); ++i) {
      if (trans_params.at(i).query_ref_expr_ == query_ref_expr) {
        is_valid = false;
      }
    }
    // exclusive checks for subquery in where-cond or post-join expr
    if (OB_SUCC(ret) && is_valid) {
      if (in_where_cond) {
        if (subquery->get_select_item_size() > 1 && !is_enable_singlerow_unnest_multi_col_subquery(stmt_)) {
          is_valid = false;
          OPT_TRACE("subquery has multiple select items");
        }
      } else {
        bool is_vector_assign = root_expr->has_flag(CNT_ALIAS);
        bool is_select_expr = stmt_->is_select_stmt()
                              && static_cast<ObSelectStmt *>(stmt_)->check_is_select_item_expr(root_expr);
        if (is_select_expr && !subquery->get_stmt_hint().has_enable_hint(T_UNNEST) && !ctx_->force_subquery_unnest_) {
          is_valid = false; // rewrite select expr may lead to a worse plan, disable unless hint forced
          OPT_TRACE("subquery is select expr, do not rewrite unless hint forced");
        } else if (!is_vector_assign && subquery->get_select_item_size() > 1) {
          // except for vector assignment, a post-join expr should not be a subquery with more-than-one select item
          // usually should not reach here, disable the rewrite for safety concern
          is_valid = false;
          OPT_TRACE("subquery has multiple select items and not for vector assignment");
        }
      }
    }
    // common checks
    if (OB_SUCC(ret) && is_valid) {
      if (!subquery->is_spj()) {
        is_valid = false;
        OPT_TRACE("subquery is not spj");
      } else if (subquery->has_subquery() && !is_enable_singlerow_unnest_nested_subquery(stmt_)) {
        is_valid = false;
        OPT_TRACE("subquery contains subquery");
      } else if (subquery->is_values_table_query() &&
                !ObTransformUtils::is_enable_values_table_rewrite(query_ctx->optimizer_features_enable_version_)) {
        is_valid = false;
        OPT_TRACE("subquery is values table query and reject values table rewrite");
      } else if (OB_FAIL(check_hint_allowed_unnest(*stmt_,
                                                   *subquery,
                                                   outline_loc + trans_params.count(),
                                                   is_valid))) {
        LOG_WARN("failed to check hint", K(ret));
      } else if (!is_valid) {
        // do nothing
        OPT_TRACE("hint reject transform");
      } else if (OB_FAIL(ObTransformUtils::is_from_item_correlated(query_ref_expr->get_exec_params(),
                                                                   *subquery,
                                                                   is_from_item_correlated))) {
        LOG_WARN("failed to check if from item contains correlated subquery", K(ret));
      } else if (is_from_item_correlated) {
        is_valid = false;
        OPT_TRACE("subquery contains correlated from item, cannot be decorrelated");
      } else if (bool is_subquery_unique = false;
                 OB_FAIL(check_subquery_unique(subquery, is_subquery_unique))) {
        LOG_WARN("failed to check stmt unique", K(ret));
      } else if (!is_subquery_unique) {
        is_valid = false;
        OPT_TRACE("subquery is not unique");
      } else if (query_ref_expr->has_exec_param()) {
        // If all exec_params of a subquery contain subquery expr,
        // rewriting such subquery would lead to a nest-loop join w/o conditions, which is less efficient
        // e.g. select (...) subq1, (select c1 from t2 where pk = subq1) subq2 from t1 having subq2 > 0;
        //   => select (...) subq1, c1 from t1 join t2 on t2.pk = (...) subq1 where t2.c1 > 0;
        // Remove this check after we can generate a better plan for subqueries in join conditions.
        bool is_all_exec_params_cnt_sq = true;
        for (int64_t i = 0; OB_SUCC(ret) && is_all_exec_params_cnt_sq && i < query_ref_expr->get_exec_params().count(); i++) {
          const ObExecParamRawExpr *exec_param = query_ref_expr->get_exec_params().at(i);
          const ObRawExpr *ref_expr = NULL;
          if (OB_ISNULL(exec_param) || OB_ISNULL(ref_expr = exec_param->get_ref_expr())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected param", K(ret), KPC(ref_expr));
          } else {
            is_all_exec_params_cnt_sq &= ref_expr->has_flag(CNT_SUB_QUERY);
          }
        }
        if (OB_SUCC(ret)
            && is_all_exec_params_cnt_sq
            && !ctx_->force_subquery_unnest_
            && !subquery->get_stmt_hint().has_enable_hint(T_UNNEST)) {
          is_valid = false;
          OPT_TRACE("all exec params of subquery contain subquery, do not rewrite unless hint or parameter forced");
        }
      }
    }
  }
  return ret;
}

/*
 * Build SingleRowSQParam for one subquery.  Decides:
 *
 *   use_outer_join_?
 *    ├─ in WHERE + null-rejecting (e.g. `c1 = (subq)`) → inner join
 *    └─ otherwise (SELECT list, or `c1 <=> (subq)`)    → outer join
 *
 *   null_reject_select_idx_:
 *     Bitmap of select items that are NOT null-propagating (e.g. `COUNT(*)`, constants).
 *     These need CASE WHEN wrapping under outer join — see singlerow_wrap_case_when.
 */
int ObTransformSubqueryUnnest::singlerow_gather_one_transform_param(ObRawExpr *root_expr,
                                                                    ObQueryRefRawExpr *query_ref_expr,
                                                                    bool in_where_cond,
                                                                    SingleRowSQParam &param,
                                                                    bool &is_valid)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 4> columns;
  ObSelectStmt *subquery = NULL;
  OPT_TRACE("try to pullup single row subquery:", query_ref_expr);
  if (OB_ISNULL(root_expr) || OB_ISNULL(query_ref_expr) ||
      OB_ISNULL(subquery = query_ref_expr->get_ref_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("subquery is null", K(ret), K(subquery));
  } else if (OB_FAIL(subquery->get_column_exprs(columns))) {
    LOG_WARN("failed to get column exprs", K(ret));
  }
  // 1. check if using outer join
  if (OB_SUCC(ret)) {
    bool is_null_reject = false;
    ObSEArray<const ObRawExpr *, 1> tmp;
    param.query_ref_expr_ = query_ref_expr;
    if (!in_where_cond) {
      param.use_outer_join_ = true;
    } else if (OB_FAIL(tmp.push_back(query_ref_expr))) {
      LOG_WARN("failed to push back query", K(ret));
    } else if (OB_FAIL(ObTransformUtils::is_null_reject_condition(root_expr, tmp, is_null_reject))) {
      LOG_WARN("failed to check is null reject condition", K(ret));
    } else {
      param.use_outer_join_ = !is_null_reject;
    }
  }
  // 2. if subquery has null-propagating select exprs (e.g. COUNT(*), constants) and
  //    rewrite would use outer join, mark them so we can wrap CASE WHEN later
  if (OB_SUCC(ret) && param.use_outer_join_) {
    for (int64_t i = 0; OB_SUCC(ret) && i < subquery->get_select_item_size(); ++i) {
      bool is_null_propagate = true;
      if (OB_FAIL(ObTransformUtils::is_null_propagate_expr(subquery->get_select_item(i).expr_,
                                                           columns,
                                                           is_null_propagate))) {
        LOG_WARN("failed to check is null propagate expr", K(ret));
      } else if (is_null_propagate) {
        //do nothing
      } else if (OB_FAIL(param.null_reject_select_idx_.add_member(i))) {
        LOG_WARN("failed to push back select idx", K(ret));
      }
    }
  }
  // 3. final check whether the transformation is valid for the SingleRowSQParam
  if (OB_SUCC(ret)) {
    is_valid = true;
    if (OB_FAIL(singlerow_check_transform_validity_for_param(param, in_where_cond, is_valid))) {
      LOG_WARN("failed to check transform validity for param", K(ret));
    } else if (is_valid) {
      OPT_TRACE("subquery will be pull-ed up");
    } else {
      OPT_TRACE("subquery can not be pull-ed up");
    }
  }
  return ret;
}

// Param-level validity.  Outer-join specific rejections: semi-info in subquery,
// missing not-null column for CASE WHEN, exec params with aggregates/window-func/subquery.
// Under outer join, also rejects multi-table-index-matching subqueries (efficient enough without rewrite).
int ObTransformSubqueryUnnest::singlerow_check_transform_validity_for_param(SingleRowSQParam &param,
                                                                            bool in_where_cond,
                                                                            bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  ObQueryRefRawExpr *query_ref_expr = param.query_ref_expr_;
  ObSelectStmt *subquery = NULL;
  if (OB_ISNULL(ctx_) || OB_ISNULL(query_ref_expr) || OB_ISNULL(subquery = query_ref_expr->get_ref_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K_(ctx), K(query_ref_expr), K(subquery));
  }
  // 1. reject rewrite using outer-join for certain scenarios
  if (OB_SUCC(ret) && is_valid && param.use_outer_join_) {
    if (subquery->get_semi_info_size() > 0) {
      // 1.1 outer join does not support pulling up semi info, otherwise there will be correctness issues
      is_valid = false;
      OPT_TRACE("subquery has semi info");
    } else if (!param.null_reject_select_idx_.is_empty()) {
      // 1.2 Single-row subquery need to find not-null column to construct case when expr.
      if (OB_FAIL(ObTransformUtils::find_not_null_expr(*subquery, param.not_null_column_, is_valid, ctx_))) {
        LOG_WARN("failed to find not null expr", K(ret));
      } else if (!is_valid) {
        OPT_TRACE("subquery has null-reject select expr, but not found not-null column");
      }
    }
    // 1.3 it's dangerous to rewrite subquery if it's exec_params containing certain types of exprs
    for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < query_ref_expr->get_param_count(); ++i) {
      ObRawExpr *param_expr = NULL;
      if (OB_ISNULL(param_expr = query_ref_expr->get_param_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("param expr is null", K(ret));
      } else {
        is_valid = !param_expr->has_flag(CNT_AGG) &&
                   !param_expr->has_flag(CNT_WINDOW_FUNC) &&
                   !param_expr->has_flag(CNT_SUB_QUERY);
        if (!is_valid) {
          OPT_TRACE("exec param contains aggregate/window func/subquery, cannot rewrite with outer join");
        }
      }
    }
  }
  // 2. reject rewrite if subquery matches indexes on multiple tables
  //    such subquery can be efficient enough and rewrite it may lead to a worse plan
  if (OB_SUCC(ret) && is_valid) {
    bool is_hint_enabled = subquery->get_stmt_hint().has_enable_hint(T_UNNEST);
    bool subq_match_idx = false;
    if (OB_FAIL(ObTransformUtils::check_subquery_match_index(ctx_, query_ref_expr, subquery, subq_match_idx))) {
      LOG_WARN("fail to check subquery match index", K(ret));
    } else if (param.use_outer_join_ && subq_match_idx && subquery->get_table_items().count() > 1
               && !is_hint_enabled && !ctx_->force_subquery_unnest_) {
      is_valid = false;
      OPT_TRACE("subquery matches indexes on multiple tables, skip rewrite under outer join unless hint or parameter forced");
      LOG_TRACE("subquery match index and not hint enabled, do not rewrite unless hint or parameter forced");
    }
  }
  return ret;
}

/*
 * Execute single-row unnesting:
 *   1. decorrelate
 *   2. wrap CASE WHEN for non-null-propagating select items (outer join only)
 *   3. pullup tables/columns to parent stmt
 *   4. replace query-ref with select exprs
 *
 * Multi-column example:
 *   WHERE (a, b) = (SELECT c, d FROM t2 WHERE t2.pk = t1.pk)
 *   →  WHERE (a, b) = (t2.c, t2.d)    with t2 joined
 *   The select exprs are wrapped in T_OP_ROW to match the lhs (a, b).
 */
int ObTransformSubqueryUnnest::singlerow_do_unnest_subquery(SingleRowSQParam &param,
                                                            const bool is_vector_assign)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *subquery = NULL;
  ObSEArray<ObRawExpr *, 4> query_refs;
  ObSEArray<ObRawExpr *, 4> select_exprs;
  ObQueryRefRawExpr *query_ref_expr = param.query_ref_expr_;
  if (OB_ISNULL(ctx_)
      || OB_ISNULL(ctx_->session_info_)
      || OB_ISNULL(ctx_->expr_factory_)
      || OB_ISNULL(query_ref_expr)
      || OB_ISNULL(subquery = query_ref_expr->get_ref_stmt())
      || OB_ISNULL(stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (is_vector_assign && !stmt_->is_update_stmt()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("update stmt is expected for vector assign", K(ret));
  } else if (is_vector_assign
             && OB_FAIL(static_cast<ObUpdateStmt *>(stmt_)->get_vector_assign_values(query_ref_expr, query_refs))) {
    LOG_WARN("failed to get vector assign values", K(ret));
  } else if (!is_vector_assign && OB_FAIL(query_refs.push_back(query_ref_expr))) {
    LOG_WARN("failed to push back query refs", K(ret));
  } else if (OB_FAIL(ObTransformUtils::decorrelate(subquery, query_ref_expr->get_exec_params()))) {
    LOG_WARN("failed to decorrelate subquery", K(ret));
  } else if (OB_FAIL(subquery->get_select_exprs(select_exprs))) {
    LOG_WARN("failed to get select exprs", K(ret));
  } else if (param.use_outer_join_ && !param.null_reject_select_idx_.is_empty()
             && OB_FAIL(singlerow_wrap_case_when_for_select_expr(param, select_exprs))) {
    LOG_WARN("failed to wrap case when for select expr", K(ret));
  } else if (OB_FAIL(pullup_subquery_to_parent_stmt(subquery, param.use_outer_join_))) {
    LOG_WARN("failed to pullup tables and columns to parent stmt", K(ret));
  } else if (query_refs.count() == select_exprs.count()) {
    if (OB_FAIL(stmt_->replace_relation_exprs(query_refs, select_exprs))) {
      LOG_WARN("failed to replace inner stmt expr", K(ret));
    }
  } else if (is_vector_assign || query_refs.count() != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("query_refs size does not match select_item size",
             K(ret), K(query_refs.count()), K(select_exprs.count()), K(is_vector_assign));
  } else {
    // (t1.c1, t1.c2) = (select c3, c4 from t2 where xxx) -> (t1.c1, t1.c2) = (t2.c3, t2.c4)
    // need to wrap select exprs of subquery by a `T_OP_ROW` before replacement
    ObSEArray<ObRawExpr *, 1> row_expr_arr;
    ObOpRawExpr *row_expr = NULL;
    if (OB_FAIL(ObTransformUtils::build_row_expr(*ctx_->expr_factory_, select_exprs, row_expr))) {
      LOG_WARN("failed to build row expr", K(ret));
    } else if (OB_FAIL(row_expr_arr.push_back(row_expr))) {
      LOG_WARN("failed to push back row expr", K(ret));
    } else if (OB_FAIL(stmt_->replace_relation_exprs(query_refs, row_expr_arr))) {
      LOG_WARN("failed to replace inner stmt expr", K(ret));
    }
  }
  if (FAILEDx(stmt_->formalize_stmt(ctx_->session_info_, false))) {
    LOG_WARN("failed to formalize stmt", K(ret));
  }
  return ret;
}

int ObTransformSubqueryUnnest::singlerow_wrap_case_when_for_select_expr(
  SingleRowSQParam &param,
  ObIArray<ObRawExpr *> &select_exprs)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *subquery = NULL;
  ObQueryRefRawExpr *query_ref_expr = param.query_ref_expr_;
  if (OB_ISNULL(query_ref_expr) || OB_ISNULL(subquery = query_ref_expr->get_ref_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < select_exprs.count(); i++) {
    if (!param.null_reject_select_idx_.has_member(i)) {
      // do nothing
    } else if (OB_FAIL(singlerow_wrap_case_when(*subquery, param.not_null_column_, select_exprs.at(i)))) {
      LOG_WARN("failed to wrap case when", K(ret));
    } else {
      // do nothing
    }
  }
  return ret;
}

/*
 * Wrap select expr:  CASE WHEN not_null_col IS NOT NULL THEN expr ELSE NULL END
 *
 * Why: under outer join, "no matching row" produces NULLs for all right-side columns.
 * For a column ref like `t2.c1`, NULL already means "no match" — no ambiguity.
 * But for `COUNT(*)` or `1`, the result is never NULL by itself, so without this
 * wrapper the caller cannot tell "no match" apart from "matched and got 1".
 *
 *   SELECT (SELECT 1 FROM t2 WHERE t2.pk = t1.pk) FROM t1
 *   →  SELECT CASE WHEN t2.pk IS NOT NULL THEN 1 ELSE NULL END
 *      FROM t1 LEFT JOIN t2 ON t2.pk = t1.pk
 */
int ObTransformSubqueryUnnest::singlerow_wrap_case_when(ObSelectStmt &child_stmt,
                                                        ObRawExpr *not_null_column,
                                                        ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx is null", K(ctx_), K(ret));
  } else if (OB_ISNULL(not_null_column)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null column expr", K(ret));
  } else {
    ObRawExpr *null_expr = NULL;
    ObRawExpr *cast_expr = NULL;
    ObRawExpr *case_when_expr = NULL;
    ObRawExprFactory *factory = ctx_->expr_factory_;
    if (OB_FAIL(ObRawExprUtils::build_null_expr(*factory, null_expr))) {
      LOG_WARN("failed to build null expr", K(ret));
    } else if (OB_ISNULL(null_expr) || OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::try_add_cast_expr_above(ctx_->expr_factory_,
                                                               ctx_->session_info_,
                                                               *null_expr,
                                                               expr->get_result_type(),
                                                               cast_expr))) {
      LOG_WARN("try add cast expr above failed", K(ret));
    } else if (OB_FAIL(ObTransformUtils::build_case_when_expr(child_stmt,
                                                              not_null_column,
                                                              expr,
                                                              cast_expr,
                                                              case_when_expr,
                                                              ctx_))) {
      LOG_WARN("failed to build case when expr", K(ret));
    } else if (OB_ISNULL(case_when_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("case when expr is null", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::try_add_cast_expr_above(ctx_->expr_factory_,
                                                               ctx_->session_info_,
                                                               *case_when_expr,
                                                               expr->get_result_type(),
                                                               expr))) {
      LOG_WARN("failed to add cast expr above", K(ret));
    }
  }
  return ret;
}

// Convert subquery comparison op to scalar comparison op after unnest.
// This function is only applicable for multi-col subquery.
// For the single-col subquery, the cmp op was already a scalar one after resolver
// For example:
//  a = (SELECT a from t2)      (a, b) = (SELECT a, b from t2)
//    ^                                ^
//  T_OP_EQ                         T_OP_SQ_EQ
int ObTransformSubqueryUnnest::singlerow_adjust_subquery_comparison_expr(ObRawExpr *expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (IS_SUBQUERY_COMPARISON_OP(expr->get_expr_type())) {
    if (OB_NOT_NULL(expr->get_param_expr(0)) && !expr->get_param_expr(0)->is_query_ref_expr()
        && OB_NOT_NULL(expr->get_param_expr(1)) && !expr->get_param_expr(1)->is_query_ref_expr()) {
      ObItemType cmp_op = T_INVALID;
      ObOpRawExpr *new_expr = NULL;
      if (OB_FAIL(ObTransformUtils::query_cmp_to_value_cmp(expr->get_expr_type(), cmp_op))) {
        LOG_WARN("failed to query cmp to value cmp", K(ret));
      } else if (OB_FAIL(ctx_->expr_factory_->create_raw_expr(cmp_op, new_expr))) {
        LOG_WARN("create row expr failed", K(ret));
      } else if (OB_ISNULL(new_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("row expr is null", K(ret));
      } else if (expr->get_param_count() != 2) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr param count != 2", K(ret));
      } else if (OB_FAIL(new_expr->set_param_exprs(expr->get_param_expr(0),
                                                   expr->get_param_expr(1)))) {
        LOG_WARN("failed to set param exprs", K(ret));
      } else if (OB_FAIL(new_expr->formalize(ctx_->session_info_))) { // type deduction is needed
        LOG_WARN("failed to formalize new comparison expr", K(ret));
      } else {
        ObSEArray<ObRawExpr *, 1> exprs;
        ObSEArray<ObRawExpr *, 1> new_exprs;
        if (OB_FAIL(exprs.push_back(expr))) {
          LOG_WARN("failed to push back", K(ret));
        } else if (OB_FAIL(new_exprs.push_back(new_expr))) {
          LOG_WARN("failed to push back", K(ret));
        } else if (OB_FAIL(stmt_->replace_relation_exprs(exprs, new_exprs))) {
          LOG_WARN("failed to replace expr", K(ret));
        }
      }
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(singlerow_adjust_subquery_comparison_expr(expr->get_param_expr(i))))) {
        LOG_WARN("failed to adjust subquery comparison expr", K(ret));
      }
    }
  }
  return ret;
}
// ============================== end of singlerow subquery ==============================

} /* namespace sql */
} /* namespace oceanbase */