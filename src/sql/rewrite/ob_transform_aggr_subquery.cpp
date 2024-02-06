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

#include "ob_transform_aggr_subquery.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/oblog/ob_log_module.h"
#include "share/ob_errno.h"
#include "common/ob_common_utility.h"
#include "common/ob_smart_call.h"
#include "ob_transformer_impl.h"
#include "objit/common/ob_item_type.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/resolver/dml/ob_dml_stmt.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "sql/ob_sql_context.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/resolver/dml/ob_update_stmt.h"
#include "sql/rewrite/ob_stmt_comparer.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

/**
 *    select * from A where c1 > (select min(B.c1) from B where A.c2 = B.c2);
 * => select * from A, (select min(B.c1) as x, B.c2 as y from B group by B.c2) v where c1 > x and c2 = y;
 *
 *    select * from A where c1 > (select count(B.c1) from B where A.c2 = B.c2);
 * => select * from A left join
 *                 (select count(B.c1) as x, B.c2 as y from B group by B.c2) v on c2 = y
 *             where c1 > (case when y is not null then x else 0 end);

 *    select A.c1, (select sum(B.c2) from B where A.c1 = B.c1 ) as sum_e from A where A.c1 > 0;
 * => select A.c1, temp.sum_e from B left join
 *                              (select A.c1, sum(B.c2) as sum_e from B group by B.c1) temp on A.c1 = temp.c1
 *                            where A.c1 > 0;
 *
 *    select A.c1, (select count(B.c2) from B where A.c1 = B.c2) as sum_e from A where A.c1 > 0;
 * => select A.c1, (case when y is not null then temp.sum_e else 0 end) from B left join
 *                              (select A.c1 as y, sum(B.c2) as sum_e from B group by B.c1) temp on A.c1 = temp.c1
 *                            where A.c1 > 0;
 */
int ObTransformAggrSubquery::do_transform(ObDMLStmt *&stmt, bool &trans_happened)
{
  int ret = OB_SUCCESS;
  bool aggr_first_happened = false;
  bool join_first_happened = false;
  bool aggr_first_for_having_happened = false;
  trans_happened = false;
  /**
   * transform_with_aggregation_first unnest a subquery by transform the subquery as view 
   * with aggregation, then join origin tables with view.
   * 
   * transform_with_join_first unnest a subquery by join subquery's table with origin tables
   * first, then aggregate on origin tables' unique set.
   */
  if (OB_FAIL(extract_no_rewrite_select_exprs(stmt))) {
    LOG_WARN("failed to extract no rewrite select exprs", K(ret));
  } else if (OB_FAIL(transform_with_aggregation_first(stmt, aggr_first_happened))) {
    LOG_WARN("failed to transfrom with aggregation first", K(ret));
  } else if (OB_FAIL(transform_with_aggr_first_for_having(stmt, aggr_first_for_having_happened))) {
    LOG_WARN("failed to transfrom with aggregation first", K(ret));
  } else if (OB_FAIL(transform_with_join_first(stmt, join_first_happened))) {
    LOG_WARN("failed to transform stmt with join first ja", K(ret));
  } else {
    trans_happened = aggr_first_happened | join_first_happened | aggr_first_for_having_happened;
  }
  return ret;
}

int ObTransformAggrSubquery::transform_one_stmt(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                                ObDMLStmt *&stmt,
                                                bool &trans_happened)
{
  int ret = OB_SUCCESS;
  UNUSED(parent_stmts);
  trans_stmt_infos_.reset();
  join_first_happened_ = false;
  if (OB_FAIL(do_transform(stmt, trans_happened))) {
    LOG_WARN("failed to do transform", K(ret));
  } else if (trans_happened && OB_FAIL(add_transform_hint(*stmt, &trans_stmt_infos_))) {
    LOG_WARN("failed to add transform hint", K(ret));
  }
  return ret;
}

int ObTransformAggrSubquery::check_hint_status(const ObDMLStmt &stmt, bool &need_trans)
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
  } else if (NULL == (cur_trans_hint = query_hint->get_outline_trans_hint(ctx_->trans_list_loc_)) ||
             !(cur_trans_hint->is_aggr_first_unnest_hint() ||
               cur_trans_hint->is_join_first_unnest_hint())) {
    /*do nothing*/
  } else {
    ObQueryRefRawExpr* subquery_expr = nullptr;
    ObSelectStmt* select_stmt = nullptr;
    for (int64_t i = 0; !need_trans && OB_SUCC(ret) && i < stmt.get_subquery_expr_size(); ++i) {
      if (OB_ISNULL(subquery_expr = stmt.get_subquery_exprs().at(i)) ||
          OB_ISNULL(select_stmt = subquery_expr->get_ref_stmt())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(subquery_expr), K(select_stmt));
      } else if (query_hint->is_valid_outline_transform(ctx_->trans_list_loc_,
                                                   get_sub_unnest_hint(*select_stmt, AGGR_FIRST))) {
        need_trans = true;
      } else if (query_hint->is_valid_outline_transform(ctx_->trans_list_loc_,
                                                   get_sub_unnest_hint(*select_stmt, JOIN_FIRST))) {
        need_trans = true;
      } else if (query_hint->is_valid_outline_transform(ctx_->trans_list_loc_,
                                          select_stmt->get_stmt_hint().get_normal_hint(T_UNNEST))) {
        need_trans = true;
      }
    }
  }
  if (!need_trans) {
    OPT_TRACE("outline reject transform");
  }
  return ret;
}

int ObTransformAggrSubquery::construct_transform_hint(ObDMLStmt &stmt, void *trans_params)
{
  int ret = OB_SUCCESS;
  ObIArray<TransStmtInfo> *trans_stmt_infos = NULL;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->allocator_) || OB_ISNULL(trans_params)
      || OB_ISNULL(trans_stmt_infos = static_cast<ObIArray<TransStmtInfo>*>(trans_params))
      || OB_UNLIKELY(trans_stmt_infos->empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(ctx_), K(trans_params), K(trans_stmt_infos));
  } else {
    ObTransHint *hint = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < trans_stmt_infos->count(); ++i) {
      TransStmtInfo& info = trans_stmt_infos->at(i);
      if (OB_FAIL(ObQueryHint::create_hint(ctx_->allocator_,
                                           get_unnest_strategy(info.pullup_strategy_),
                                           hint))) {
        LOG_WARN("failed to create hint", K(ret));
      } else if (OB_FAIL(ctx_->add_src_hash_val(info.qb_name_))) {
        LOG_WARN("failed to add src hash val", K(ret));
      } else if (OB_FAIL(ctx_->outline_trans_hints_.push_back(hint))) {
        LOG_WARN("failed to push back hint", K(ret));
      } else if (nullptr != info.unnest_ && OB_FAIL(ctx_->add_used_trans_hint(info.unnest_))) {
        LOG_WARN("failed to add used trans hint", K(ret));
      } else {
        hint->set_qb_name(info.qb_name_);
      }
    }
  }
  return ret;
}

int ObTransformAggrSubquery::transform_with_aggregation_first(ObDMLStmt *&stmt,
                                                              bool &trans_happened)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 4> exprs;
  const bool with_vector_assgin = true;
  const ObQueryHint* query_hint = nullptr;
  OPT_TRACE("try aggregation first");
  if (OB_ISNULL(stmt) || OB_ISNULL(query_hint = stmt->get_stmt_hint().query_hint_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(ctx_), K(query_hint));
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid params", K(ret), K(stmt));
  } else if (stmt->is_hierarchical_query() || stmt->is_set_stmt() || !stmt->is_sel_del_upd()) {
    OPT_TRACE("hierarchical/set/insert/merge query can not transform");
  } else if (OB_FAIL(exprs.assign(stmt->get_condition_exprs()))) {
    LOG_WARN("failed to assign conditions", K(ret));
  } else if (OB_FAIL(ObTransformUtils::get_post_join_exprs(stmt, exprs, with_vector_assgin))) {
    LOG_WARN("failed to get post join exprs", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    if (OB_FAIL(do_aggr_first_transform(stmt, exprs.at(i), trans_happened))) {
      LOG_WARN("failed to transform one expr", K(ret));
    }
  }
  return ret;
}

int ObTransformAggrSubquery::transform_with_aggr_first_for_having(ObDMLStmt *&stmt, bool &trans_happened) 
{
  int ret = OB_SUCCESS;
  ObSelectStmt *view_stmt = NULL; 
  bool need_spj = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid params", K(ret), K(stmt));
  } else if (!stmt->is_select_stmt()) {
    // do nothing
  } else if (OB_FAIL(check_need_spj(stmt, need_spj))) {
    LOG_WARN("check need spj failed", K(ret));
  } else if (!need_spj) {
    //do nothing
  } else if (OB_FAIL(ObTransformUtils::create_simple_view(ctx_, stmt, view_stmt, true, true, true))) {
    LOG_WARN("create simple view failed", K(ret));
  } else if (OB_FAIL(transform_with_aggregation_first(stmt, trans_happened))) {
    LOG_WARN("transform with aggr first failed", K(ret));
  }
  return ret;
}

int ObTransformAggrSubquery::check_need_spj(ObDMLStmt *stmt, bool &is_valid)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *sel_stmt = NULL;
  bool has_rand = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (!stmt->is_select_stmt()) {
    is_valid = false;
  } else if (OB_ISNULL(sel_stmt = static_cast<ObSelectStmt *>(stmt))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (sel_stmt->get_having_exprs().count() == 0 ||
             sel_stmt->has_rollup()) {
    is_valid = false;
  } else if (OB_FAIL(sel_stmt->has_rand(has_rand))) {
    LOG_WARN("sel stmt has rand failed", K(ret));
  } else if (has_rand) {
    is_valid = false;
  } else {
    bool exist_valid_subquery = false;
    ObSEArray<TransformParam, 4> transform_params;
    for (int64_t i = 0; OB_SUCC(ret) && !exist_valid_subquery && i < sel_stmt->get_having_exprs().count(); i++) {
      transform_params.reuse();
      ObRawExpr *expr = sel_stmt->get_having_exprs().at(i);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
      } else if (!expr->has_flag(CNT_SUB_QUERY)) {
        //do nothing
      } else if (OB_FAIL(gather_transform_params(*stmt, expr, expr, AGGR_FIRST, false, transform_params))) {
        LOG_WARN("gather transform param failed", K(ret));
      } else if (transform_params.count() > 0) {
        exist_valid_subquery = true;
      }
    }
    is_valid = exist_valid_subquery;
  }
  return ret;
}

/**
 * @brief ObTransformAggrSubquery::do_aggr_first_transform
 * STEP 1: 找到一个可改写的子查询
 * STEP 2: 根据子查询的特点，决定改写的方式（OUTER JOIN/INNER JOIN）
 * STEP 3: 执行改写
 * @return
 */
int ObTransformAggrSubquery::do_aggr_first_transform(ObDMLStmt *&stmt,
                                                     ObRawExpr *expr,
                                                     bool &trans_happened)
{
  int ret = OB_SUCCESS;
  ObSEArray<TransformParam, 4> transform_params;
  ObSEArray<ObRawExpr *, 4> filters;
  bool is_select_item_expr = false;
  const ObQueryHint* query_hint = nullptr;
  if (OB_ISNULL(expr) || OB_ISNULL(stmt) ||
      OB_ISNULL(query_hint = stmt->get_stmt_hint().query_hint_)) {
    ret= OB_ERR_UNEXPECTED;
    LOG_WARN("condition is null", K(ret), K(expr), K(stmt), K(query_hint));
  } else if (stmt->is_select_stmt() && FALSE_IT(is_select_item_expr = 
              static_cast<ObSelectStmt*>(stmt)->check_is_select_item_expr(expr))) {
    // never reach
  } else if (OB_FAIL(gather_transform_params(*stmt, expr, expr, AGGR_FIRST, is_select_item_expr, transform_params))) {
    LOG_WARN("failed to check the condition is valid for transformation", K(ret));
  } else if (OB_FAIL(get_filters(*stmt, expr, filters))) {
    LOG_WARN("failed to get filters for the expr", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < transform_params.count(); ++i) {
    TransformParam &trans_param = transform_params.at(i);
    ObQueryRefRawExpr *query_ref = NULL;
    ObSelectStmt *subquery = NULL;
    if (OB_ISNULL(query_ref = trans_param.ja_query_ref_) ||
        OB_ISNULL(subquery = trans_param.ja_query_ref_->get_ref_stmt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid transform params", K(ret), K(trans_param.ja_query_ref_));
    } else if (!ObOptimizerUtil::find_item(stmt->get_subquery_exprs(), query_ref)) {
      // skip, the subquery has been pullup
    } else if (OB_FAIL(choose_pullup_method(filters, trans_param))) {
      LOG_WARN("failed to choose pullup method", K(ret));
    } else if (use_outer_join(trans_param.pullup_flag_) && stmt->get_table_size() == 0) {
      // skip
    } else if (!trans_param.limit_for_exists_ && trans_param.limit_value_ > 0 &&
               OB_FAIL(convert_limit_as_aggr(subquery, trans_param))) {
      LOG_WARN("failed to transform subquery with limit");
    } else if (OB_FAIL(fill_query_refs(stmt, expr, trans_param))) {
      LOG_WARN("failed to fill query refs", K(ret));
    } else if (OB_FAIL(transform_child_stmt(stmt, *subquery, trans_param))) {
      LOG_WARN("failed to transform subquery", K(ret));
    } else if (OB_FAIL(transform_upper_stmt(*stmt, trans_param))) {
      LOG_WARN("failed to transform upper stmt", K(ret));
    } else if (OB_FAIL(add_trans_stmt_info(*subquery, AGGR_FIRST))) {
      LOG_WARN("failed add trans stmt info", K(ret));
    } else {
      trans_happened = true;
    }
  }
  return ret;
}

/**
 * find valid JA-subquery in the root expr and select item expr.
 * @param is_select_item_expr: is root expr in select item, only used in aggr first rewrite
 **/
int ObTransformAggrSubquery::gather_transform_params(ObDMLStmt &stmt,
                                                     ObRawExpr *root_expr,
                                                     ObRawExpr *child_expr,
                                                     int64_t pullup_strategy,
                                                     const bool is_select_item_expr,
                                                     ObIArray<TransformParam> &transform_params)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(child_expr) || OB_ISNULL(root_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret), K(child_expr), K(root_expr));
  } else if (!child_expr->has_flag(CNT_SUB_QUERY) || (IS_SUBQUERY_COMPARISON_OP(child_expr->get_expr_type()) &&
            (child_expr->has_flag(IS_WITH_ANY) || child_expr->has_flag(IS_WITH_ALL) || aggr_first(pullup_strategy)))) {
     // do nothing
  } else if (child_expr->is_query_ref_expr()) {
    TransformParam trans_param(pullup_strategy);
    trans_param.ja_query_ref_ = static_cast<ObQueryRefRawExpr*>(child_expr);
    ObSelectStmt *subquery = trans_param.ja_query_ref_->get_ref_stmt();
    bool is_valid = false;
    bool hint_allowed = false;
    int64_t limit_value = 0;
    OPT_TRACE("try to pullup JA subquery", child_expr);
    if (ObOptimizerUtil::find_item(no_rewrite_exprs_, child_expr)) {
      LOG_TRACE("subquery in select expr and can use index");
      OPT_TRACE("subquery in select expr and can use index, no need transfrom");
    } else if (OB_FAIL(check_hint_allowed_unnest(stmt, *subquery,
                                                 ctx_->trans_list_loc_ + transform_params.count(),
                                                 pullup_strategy,
                                                 hint_allowed))) {
      LOG_WARN("failed to check hint allowed unnest", K(ret));
    } else if (!hint_allowed) {
      OPT_TRACE("hint reject transform");
    } else if (aggr_first(pullup_strategy)) {
      OPT_TRACE("try aggregation first transform");
      if (OB_FAIL(check_aggr_first_validity(*trans_param.ja_query_ref_,
                                            root_expr->has_flag(CNT_ALIAS),
                                            trans_param.nested_conditions_,
                                            is_select_item_expr,
                                            is_valid,
                                            limit_value,
                                            trans_param.equal_param_info_))) {
        LOG_WARN("failed to check subquery validity for aggr first", K(ret));
      } else {
        trans_param.limit_for_exists_ = false;
        trans_param.limit_value_ = limit_value;
      }
    } else if (join_first(pullup_strategy) && trans_param.ja_query_ref_->has_exec_param()) {
      OPT_TRACE("try join first transform");
      if (OB_FAIL(ObTransformUtils::find_parent_expr(root_expr, child_expr, trans_param.parent_expr_of_query_ref))) {
        LOG_WARN("failed to find parent expr of subquery expr", K(ret));
      } else if (OB_ISNULL(trans_param.parent_expr_of_query_ref)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("find no parent expr of subquery expr", K(ret));
      } else if (OB_FAIL(check_join_first_validity(*trans_param.ja_query_ref_,
                                            root_expr->has_flag(CNT_ALIAS),
                                            is_exists_op(root_expr->get_expr_type()),
                                            IS_SUBQUERY_COMPARISON_OP(trans_param.parent_expr_of_query_ref->get_expr_type()),
                                            trans_param.not_null_const_,
                                            trans_param.limit_for_exists_,
                                            limit_value,
                                            is_valid,
                                            trans_param.equal_param_info_))) {
        LOG_WARN("failed to check subquery validity for join first", K(ret));
      } else {
        trans_param.limit_value_ = limit_value;
      }
    }
    if (OB_SUCC(ret) && is_valid) {
      if (OB_FAIL(transform_params.push_back(trans_param))) {
        LOG_WARN("failed to push back transform parameters", K(ret));
      }
    } else {
      OPT_TRACE("expr can not be transformed");
    }
  } else {
    // join-first ja can rewrite exists predicate (must be a root relation expr in where/having)
    bool is_exists_filter = is_exists_op(child_expr->get_expr_type()) &&
                            root_expr == child_expr && join_first(pullup_strategy);
    if (is_exists_filter || !is_exists_op(child_expr->get_expr_type())) {
      for (int64_t i = 0; OB_SUCC(ret) && i < child_expr->get_param_count(); ++i) {
        if (OB_FAIL(SMART_CALL(gather_transform_params(stmt, root_expr,
                                                       child_expr->get_param_expr(i),
                                                       pullup_strategy,
                                                       is_select_item_expr,
                                                       transform_params)))) {
          LOG_WARN("failed to gather transform params", K(ret));
        }
      }
    }
  }
  return ret;
}

/**
 * @brief ObTransformAggrSubquery::check_aggr_first_validity
 *  check the valiaidty of the subquery
 */
int ObTransformAggrSubquery::check_aggr_first_validity(ObQueryRefRawExpr &query_ref,
                                                       const bool vector_assign,
                                                       ObIArray<ObRawExpr *> &nested_conditions,
                                                       const bool is_select_item_expr,
                                                       bool &is_valid,
                                                       int64_t &limit_value,
                                                       ObIArray<ObPCParamEqualInfo> &equal_param_info)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *subquery = NULL;
  bool has_rownum = false;
  bool has_ref_assign_user_var = false;
  bool limit_to_aggr = false;
  bool has_equal_correlation = false;
  bool is_correlated = false;
  nested_conditions.reuse();
  is_valid = true;
  limit_value = -1;
  if (OB_ISNULL(subquery = query_ref.get_ref_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("subquery is null", K(ret));
    // 0. check single set
  } else if (OB_FAIL(check_single_set_subquery(*subquery,
                                               is_valid,
                                               limit_to_aggr,
                                               limit_value))) {
    LOG_WARN("failed to check is single set query", K(ret));
  } else if (!is_valid) {
    OPT_TRACE("not single set query");
    // 1. check stmt components
  } else if (subquery->has_rollup() ||
             subquery->has_having() ||
             NULL != subquery->get_limit_percent_expr() ||
             NULL != subquery->get_offset_expr() ||
             subquery->has_window_function() ||
             subquery->has_sequence() ||
             subquery->is_set_stmt() ||
             subquery->is_hierarchical_query()) {
    is_valid = false;
    LOG_TRACE("invalid subquery", K(is_valid), K(*subquery));
    OPT_TRACE("subquery has rollup/having/limit offset/limit percent/win_func/sequence");
  } else if (OB_FAIL(subquery->has_rownum(has_rownum))) {
    LOG_WARN("failed to check subquery has rownum", K(ret));
  } else if (has_rownum) {
    is_valid = false;
    LOG_TRACE("has rownum expr", K(is_valid));
    OPT_TRACE("subquery has rownum");
  } else if (OB_FAIL(check_subquery_aggr_item(*subquery, is_valid))) {
    LOG_WARN("failed to check subquery select item", K(ret));
  } else if (!is_valid) {
    OPT_TRACE("has AVG aggregation");
  } else if (OB_FAIL(subquery->has_ref_assign_user_var(has_ref_assign_user_var))) {
    LOG_WARN("failed to check stmt has assignment ref user var", K(ret));
  } else if (has_ref_assign_user_var) {
    is_valid = false;
    LOG_TRACE("has assignment ref user variable", K(is_valid));
    OPT_TRACE("has assignment ref user variable");
    // 2. check the select item
  } else if (!vector_assign && subquery->get_select_item_size() > 1) {
    is_valid = false;
    OPT_TRACE("select item more than one");
    LOG_TRACE("select item size is invalid",
              K(vector_assign), K(subquery->get_select_item_size()));
  } else if (OB_FAIL(check_subquery_select(query_ref, is_valid))) {
    LOG_WARN("failed to check select validity", K(ret));
  } else if (!is_valid) {
    LOG_TRACE("select list is invalid", K(is_valid));
    OPT_TRACE("subquery select item contain subquery");
    // 3. check from list is not correlated
  } else if (OB_FAIL(ObTransformUtils::is_table_item_correlated(query_ref,
                                                                *subquery,
                                                                is_correlated))) {
    LOG_WARN("failed to check table item correlated or not", K(ret));
  } else if (is_correlated) {
    is_valid = false;
    OPT_TRACE("subquery`s table item is correlated");
    // 4. check correlated join on contiditons
    // 5. check correlated semi contiditons
  } else if (OB_FAIL(ObTransformUtils::is_join_conditions_correlated(query_ref,
                                                                     subquery,
                                                                     is_correlated))) {
    LOG_WARN("failed to check is join condition correlated", K(ret));
  } else if (is_correlated) {
    is_valid = false;
    OPT_TRACE("subquery`s outer/semi join condition is correlated");
    // 6. check correlated join contiditons
  } else if (OB_FAIL(check_subquery_conditions(query_ref, *subquery, nested_conditions,
                                               is_select_item_expr || limit_to_aggr,
                                               is_valid, has_equal_correlation))) {
    LOG_WARN("failed to check subquery conditions", K(ret));
  } else if (!is_valid) {
    OPT_TRACE("subquery's condition is not valid");
  } else if (limit_to_aggr && !has_equal_correlation) {
    // transform limit 1 to aggr only if subquery has equal correlation
    is_valid = false;
    OPT_TRACE("subquery does not have euqal correlation condition");
  } else if (limit_to_aggr && OB_FAIL(check_subquery_select_for_limit_1(*subquery,
                                                                        is_valid,
                                                                        equal_param_info))) {
    LOG_WARN("failed to check select validity for limit 1", K(ret));
  } else if (!is_valid) {
    OPT_TRACE("select item is lob or const");
  } else if (OB_FAIL(check_subquery_orderby(query_ref, is_valid))) {
    LOG_WARN("failed to check order_by validity", K(ret));
  } else if (!is_valid) {
    LOG_TRACE("order by item is invalid", K(is_valid));
    OPT_TRACE("subquery order by item contain correlated subquery");
  }
  return ret;
}

int ObTransformAggrSubquery::check_subquery_select(const ObQueryRefRawExpr &query_ref,
                                                   bool &is_valid)
{
  int ret = OB_SUCCESS;
  const ObSelectStmt *subquery = NULL;
  if (OB_ISNULL(subquery = query_ref.get_ref_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("subquery stmt is null", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < subquery->get_select_item_size(); ++i) {
    const ObRawExpr *select_expr = NULL;
    bool bret = false;
    if (OB_ISNULL(select_expr = subquery->get_select_item(i).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("select expr is null", K(ret));
    } else if (select_expr->has_flag(CNT_SUB_QUERY)) {
      is_valid = false;
    } else if (OB_FAIL(ObTransformUtils::is_correlated_expr(query_ref.get_exec_params(), select_expr, bret))) {
      LOG_WARN("failed to check is correlated", K(ret));
    } else if (bret) {
      is_valid = false;
    }
  }
  return ret;
}

// whether 'order by' subquery in current subquery has any correlated expr.
int ObTransformAggrSubquery::check_subquery_orderby(const ObQueryRefRawExpr &query_ref,
                                                    bool &is_valid)
{
  int ret = OB_SUCCESS;
  const ObSelectStmt *subquery = NULL;
  is_valid = true;
  if (OB_ISNULL(subquery = query_ref.get_ref_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("subquery stmt is null", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < subquery->get_order_item_size(); ++i) {
    const ObRawExpr *expr = NULL;
    bool is_correlated = false;
    if (OB_ISNULL(expr = subquery->get_order_item(i).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("select expr is null", K(ret));
    } else if (OB_FAIL(ObTransformUtils::is_correlated_expr(query_ref.get_exec_params(),
                                                            expr,
                                                            is_correlated))) {
      LOG_WARN("failed to check is correlated", K(ret));
    } else if (is_correlated) {
      is_valid = false;
    }
  }
  return ret;
}


int ObTransformAggrSubquery::check_subquery_select_for_limit_1(ObSelectStmt &subquery,
                                                               bool &is_valid,
                                                               ObIArray<ObPCParamEqualInfo> &equal_param_info)
{
  int ret = OB_SUCCESS;
  // check select item for transform limit 1 to group by
  //  1. only one select item
  //  2. not lob or const
  //  3. same as the first expr in order by
  is_valid = (1 == subquery.get_select_item_size());
  if (is_valid) {
    ObRawExpr *select_expr = NULL;
    if (OB_ISNULL(select_expr = subquery.get_select_item(0).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("select expr is null", K(ret));
    } else if (ObLobType == select_expr->get_data_type()
               || select_expr->is_const_expr()) {
      is_valid = false;
    } else if (subquery.has_order_by()) {
      ObStmtCompareContext context;
      OrderItem& first_order = subquery.get_order_item(0);
      if (OB_ISNULL(first_order.expr_) || OB_ISNULL(subquery.get_query_ctx())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret));
      } else if (FALSE_IT(context.init(&subquery.get_query_ctx()->calculable_items_))) {
        // never reach
      } else if (!select_expr->same_as(*first_order.expr_, &context)) {
        is_valid = false;
        OPT_TRACE("order does not match");
      } else if (NULLS_LAST_ASC == first_order.order_type_ ||
                 NULLS_LAST_DESC == first_order.order_type_){
        if (OB_FAIL(equal_param_info.assign(context.equal_param_info_))) {
          LOG_WARN("failed to assign equal param info", K(ret));
        } else {
          is_valid = true;
        }
      } else {
        is_valid = false;
      }
    }
  }
  return ret;
}

int ObTransformAggrSubquery::check_join_first_condition_for_limit_1(ObQueryRefRawExpr &query_ref,
                                                                    ObSelectStmt &subquery,
                                                                    bool &is_valid)
{
  int ret = OB_SUCCESS;
  EqualSets &equal_sets = ctx_->equal_sets_;
  ObSEArray<ObRawExpr *, 4> const_exprs;
  ObArenaAllocator alloc;
  bool has_equal_correlation = false;
  is_valid = true;
  if (OB_FAIL(subquery.get_stmt_equal_sets(equal_sets, alloc, true))) {
    LOG_WARN("failed to get stmt equal sets", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::compute_const_exprs(subquery.get_condition_exprs(),
                                                          const_exprs))) {
    LOG_WARN("failed to compute const equivalent exprs", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < subquery.get_condition_size(); ++i) {
    ObRawExpr *cond = NULL;
    bool is_eq_correlation = false;

    // transform limit 1 to group by only if subquery has equal correlation
    if (OB_ISNULL(cond = subquery.get_condition_expr(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("condition expr is null", K(ret));
    } else if (has_equal_correlation) {
      // do nothing
    } else if (OB_FAIL(ObTransformUtils::is_equal_correlation(query_ref,
                                                              cond,
                                                              is_eq_correlation))) {
      LOG_WARN("failed to check is equal correlation", K(ret));
    } else if (is_eq_correlation) {
      has_equal_correlation = true;
    }

    // correlation condition should not match index
    if (OB_SUCC(ret) && IS_COMMON_COMPARISON_OP(cond->get_expr_type())) {
      ObColumnRefRawExpr *column_expr = NULL;
      ObRawExpr *const_expr = NULL;
      bool is_match = false;
      if (OB_FAIL(ObTransformUtils::is_simple_correlated_pred(query_ref.get_exec_params(),
                                                              cond,
                                                              column_expr,
                                                              const_expr))) {
        LOG_WARN("failed to check is simple correlated pred", K(ret));
      } else if (column_expr == NULL) {
        // do nothing
      } else if (OB_FAIL(ObTransformUtils::is_match_index(ctx_->sql_schema_guard_,
                                                          &subquery,
                                                          column_expr,
                                                          is_match,
                                                          &equal_sets,
                                                          &const_exprs))) {
        LOG_WARN("failed to check is match index", K(ret));
      } else if (is_match) {
        LOG_TRACE("condition match index", KPC(cond));
        OPT_TRACE("subquery`s correlated condition match index");
        is_valid = false;
      }
    }
  }
  if (OB_SUCC(ret) && !has_equal_correlation) {
    is_valid = false;
    OPT_TRACE("subquery does not have euqal correlation condition");
  }
  equal_sets.reuse();
  return ret;
}


int ObTransformAggrSubquery::check_subquery_conditions(ObQueryRefRawExpr &query_ref,
                                                       ObSelectStmt &subquery,
                                                       ObIArray<ObRawExpr *> &nested_conds,
                                                       const bool check_idx,
                                                       bool &is_valid,
                                                       bool &has_equal_correlation)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  has_equal_correlation = false;
  ObArenaAllocator alloc;
  EqualSets &equal_sets = ctx_->equal_sets_;
  ObSEArray<ObRawExpr *, 4> const_exprs;
  if (OB_FAIL(subquery.get_stmt_equal_sets(equal_sets, alloc, true))) {
    LOG_WARN("failed to get stmt equal sets", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::compute_const_exprs(subquery.get_condition_exprs(),
                                                          const_exprs))) {
    LOG_WARN("failed to compute const equivalent exprs", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < subquery.get_condition_size(); ++i) {
      ObRawExpr *cond = NULL;
      ObRawExpr *outer_expr = NULL;
      ObRawExpr *inner_expr = NULL;
      bool is_correlated = false;
      if (OB_ISNULL(cond = subquery.get_condition_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("condition expr is null", K(ret));
      } else if (OB_FAIL(ObTransformUtils::is_correlated_expr(query_ref.get_exec_params(),
                                                              cond,
                                                              is_correlated))) {
        LOG_WARN("failed to check is correlated expr", K(ret));
      } else if (!is_correlated) {
        // do nothing
      } else if (!cond->has_flag(CNT_COLUMN) || cond->has_flag(CNT_SUB_QUERY)) {
        is_valid = false;
        OPT_TRACE("subquery`s condition has subquery");
      } else if (OB_FAIL(ObTransformUtils::is_equal_correlation(query_ref,
                                                                cond,
                                                                is_valid,
                                                                &outer_expr,
                                                                &inner_expr))) {
        LOG_WARN("failed to check is equal correlation", K(ret));
      } else if (!is_valid) {
        OPT_TRACE("subquery`s correlated condition is not equal cond");
      } else if (OB_FAIL(nested_conds.push_back(cond))) {
        LOG_WARN("failed to push back nested conditions", K(ret));
      } else if (check_idx && inner_expr->is_column_ref_expr()) {
        bool is_match = false;
        if (OB_FAIL(ObTransformUtils::is_match_index(ctx_->sql_schema_guard_,
                                                    &subquery,
                                                    static_cast<ObColumnRefRawExpr*>(inner_expr),
                                                    is_match,
                                                    &equal_sets, &const_exprs))) {
          LOG_WARN("failed to check is match index", K(ret));
        } else if (is_match) {
          LOG_TRACE("inner expr match index", K(*inner_expr));
          is_valid = false;
          OPT_TRACE("subquery`s correlated condition match index");
        } else {
          has_equal_correlation = true;
        }
      } else {
        has_equal_correlation = true;
      }
    }
  }
  equal_sets.reuse();
  return ret;
}

/**
 * @brief check the select item, and the subquery comparison operator
 *        determine the join method
 *  filters: filters on the ja query refs
 **/
int ObTransformAggrSubquery::choose_pullup_method(ObIArray<ObRawExpr *> &filters,
                                                  TransformParam &trans_param)
{
  int ret = OB_SUCCESS;
  ObQueryRefRawExpr *query_ref = trans_param.ja_query_ref_;
  ObIArray<bool> &is_null_prop = trans_param.is_null_prop_;
  ObSelectStmt *subquery = NULL;
  ObSEArray<const ObRawExpr*, 4> vars;
  bool is_null_propagate = true;
  bool is_null_reject = false;
  bool is_scalar_aggr = false;
  if (OB_ISNULL(query_ref) || OB_ISNULL(subquery = query_ref->get_ref_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params are invalid", K(ret), K(query_ref), K(subquery));
  } else if (OB_FAIL(is_null_prop.prepare_allocate(subquery->get_select_item_size()))) {
    LOG_WARN("failed to prepare allocate case when array", K(ret));
  } else {
    is_scalar_aggr = subquery->is_scala_group_by();
  }
  // 1. scalar group by must return one row
  //    if join result is empty, we need deduce results for the scalar aggr items
  //    if join result is not empty, the scalar aggr items can be computed normally
  // 2. normal group by, return zero or one row
  //    if return zero, the main query gots null from the subquery
  //    if return one, the main query gots real values from the subquery
  for (int64_t i = 0; OB_SUCC(ret) && i < subquery->get_select_item_size(); ++i) {
    ObRawExpr *expr = NULL;
    is_null_prop.at(i) = false;
    if (!is_scalar_aggr) {
      is_null_prop.at(i) = true;
    } else if (OB_ISNULL(expr = subquery->get_select_item(i).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("select expr is null", K(ret), K(expr));
    } else if (OB_FAIL(extract_nullable_exprs(expr, vars))) {
      LOG_WARN("failed to extract nullable exprs", K(ret));
    } else if (vars.count() <= 0) {
      // do nothing
    } else if (OB_FAIL(ObTransformUtils::is_null_propagate_expr(expr, vars, is_null_prop.at(i)))) {
      LOG_WARN("failed to check is null propagate expr", K(ret));
    }
    if (OB_SUCC(ret) && !is_null_prop.at(i)) {
      is_null_propagate = false;
    }
  }
  if (OB_SUCC(ret) && is_null_propagate) {
    if (OB_FAIL(ObTransformUtils::has_null_reject_condition(filters,
                                                            query_ref,
                                                            is_null_reject))) {
      LOG_WARN("failed to check has null reject condition", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (aggr_first(trans_param.pullup_flag_) &&
        trans_param.nested_conditions_.empty() &&
        is_scalar_aggr) {
      // use inner join for non-correlated subquery
      OPT_TRACE("use inner join for aggr first");
    } else if (!is_null_propagate || !is_null_reject) {
      trans_param.pullup_flag_ |= USE_OUTER_JOIN;
      OPT_TRACE("use outer join for none null propagate expr");
    } else {
      OPT_TRACE("use inner join for null propagate expr");
    }
  }
  return ret;
}

int ObTransformAggrSubquery::transform_child_stmt(ObDMLStmt *stmt,
                                                  ObSelectStmt &subquery,
                                                  TransformParam &param)
{
  int ret = OB_SUCCESS;
  ObIArray<ObRawExpr *> &nested_conditions = param.nested_conditions_;
  ObSEArray<ObRawExpr *, 4> old_group_exprs;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->allocator_) || OB_ISNULL(param.ja_query_ref_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("transform context is invalid", K(ret), K(ctx_), K(param.ja_query_ref_));
  } else if (OB_FAIL(old_group_exprs.assign(subquery.get_group_exprs()))) {
    LOG_WARN("failed to assign group exprs", K(ret));
  } else {
    subquery.get_group_exprs().reset();
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < nested_conditions.count(); ++i) {
    ObRawExpr *cond_expr = nested_conditions.at(i);
    bool left_is_correlated = false;
    if (OB_ISNULL(cond_expr) || OB_UNLIKELY(cond_expr->get_expr_type() != T_OP_EQ) ||
        OB_ISNULL(cond_expr->get_param_expr(0)) ||
        OB_ISNULL(cond_expr->get_param_expr(1))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("nested expr is invalid", K(ret), K(cond_expr));
    } else if (OB_FAIL(ObOptimizerUtil::remove_item(subquery.get_condition_exprs(),
                                                    cond_expr))) {
      LOG_WARN("failed to remove expr", K(ret));
    } else if (OB_FAIL(ObTransformUtils::is_correlated_expr(param.ja_query_ref_->get_exec_params(),
                                                            cond_expr->get_param_expr(0),
                                                            left_is_correlated))) {
      LOG_WARN("failed to check is left correlated", K(ret));
    } else {
      // construct a pullup condition from a nested condition
      int64_t inner_param_id = left_is_correlated ? 1 : 0;
      ObRawExpr *group_expr = cond_expr->get_param_expr(inner_param_id);
      if (group_expr->has_flag(IS_CONST)) {
        // do nothing for const expr
      } else if (ObOptimizerUtil::find_item(subquery.get_group_exprs(),
                                            group_expr)) {
        // do nothing
      } else if (OB_FAIL(subquery.add_group_expr(group_expr))) {
        LOG_WARN("failed to add group expr", K(ret));
      } else if (OB_FAIL(ObTransformUtils::create_select_item(*ctx_->allocator_,
                                                              group_expr,
                                                              &subquery))) {
        LOG_WARN("failed to create select item", K(ret));
      } else if (param.not_null_expr_ == NULL) {
        param.not_null_expr_ = group_expr;
      }
    }
  }
  if (OB_SUCC(ret) &&
      subquery.get_group_exprs().empty() &&
      !old_group_exprs.empty()) {
    ObConstRawExpr *const_one = NULL;
    if (is_oracle_mode()) {
      if (OB_FAIL(ObRawExprUtils::build_const_number_expr(*ctx_->expr_factory_,
                                                          ObNumberType,
                                                          number::ObNumber::get_positive_one(),
                                                          const_one))) {
        LOG_WARN("failed to build const one", K(ret));
      }
    } else {
      if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*ctx_->expr_factory_, ObIntType, 1 ,const_one))) {
        LOG_WARN("failed to build const int expr", K(ret));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(subquery.get_group_exprs().push_back(const_one))) {
      /// select * from t1 where c1 > (select count(d1) from t2 group by 1.0);
      /// => select * from t1, (select count(d1) as aggr from t2 group by 1.0) V where c1 > V.aggr
      LOG_WARN("failed to assign group exprs", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(subquery.formalize_stmt(ctx_->session_info_))) {
      LOG_WARN("failed to formalize subquery", K(ret));
    }
  }
  return ret;
}

/**
 * @brief ObTransformAggrSubquery::transform_upper_stmt
 * 1. 把子查询作为视图添加到主查询中
 * 2. 根据视图的输出列，推导原始子查询结果的表达式 real_values, 用 real_values 替换掉原始子查询表达式的引用
 * 3. 根据 pullup 的相关条件构造连接条件，使用恰当的方式连接提升产生的视图。
 * @return
 */
int ObTransformAggrSubquery::transform_upper_stmt(ObDMLStmt &stmt, TransformParam &param)
{
  int ret = OB_SUCCESS;
  TableItem *view_table = NULL;
  ObSEArray<ObRawExpr *, 4> view_columns;
  ObSEArray<ObRawExpr *, 4> select_exprs;
  ObSEArray<ObRawExpr *, 4> real_values;
  ObQueryRefRawExpr *query_expr = param.ja_query_ref_;
  ObIArray<ObRawExpr *> &pullup_conds = param.nested_conditions_;
  ObSelectStmt *subquery = NULL;
  int64_t idx = OB_INVALID_INDEX;

  // 1. add the subquery as view
  if (OB_ISNULL(ctx_) || OB_ISNULL(query_expr) ||
      OB_ISNULL(subquery = query_expr->get_ref_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params are invalid", K(ret), K(ctx_), K(subquery));
  } else if (OB_FAIL(ObOptimizerUtil::remove_item(stmt.get_subquery_exprs(), query_expr))) {
    LOG_WARN("failed to remove subquery expr", K(ret));
  } else if (OB_FAIL(ObTransformUtils::add_new_table_item(ctx_, &stmt, subquery, view_table))) {
    LOG_WARN("failed to add new table item", K(ret));
  } else if (OB_ISNULL(view_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table item is invalid", K(ret));
  } else if (OB_FAIL(ObTransformUtils::create_columns_for_view(
                       ctx_, *view_table, &stmt, view_columns))) {
    LOG_WARN("failed to create columns for view stmt", K(ret));
  } else if (OB_FAIL(subquery->get_select_exprs(select_exprs))) {
    LOG_WARN("failed to get select exprs", K(ret));
  } else if (ObOptimizerUtil::find_item(select_exprs, param.not_null_expr_, &idx)) {
    param.not_null_expr_ = view_columns.at(idx);
  }

  if (OB_SUCC(ret)) {
    // 2. 推导原始的子查询计算结果
    if (OB_FAIL(deduce_query_values(stmt, param, select_exprs, view_columns, real_values))) {
      LOG_WARN("failed to deduce subquery output", K(ret));
    } else if (OB_FAIL(stmt.replace_relation_exprs(param.query_refs_, real_values))) {
      LOG_WARN("failed to replace inner stmt expr", K(ret));
      // 3. 构造连接条件，连接视图
    } else if (OB_FAIL(ObTransformUtils::replace_exprs(select_exprs, view_columns, pullup_conds))) {
      LOG_WARN("failed to replace pullup conditions", K(ret));
    } else if (OB_FAIL(ObTransformUtils::decorrelate(pullup_conds, query_expr->get_exec_params()))) {
      LOG_WARN("failed to decorrelation", K(ret));
    } else if (OB_FAIL(transform_from_list(stmt, view_table, pullup_conds, param.pullup_flag_))) {
      LOG_WARN("failed to transform from list", K(ret));
      // 4. post process
    } else if (OB_FAIL(stmt.formalize_stmt(ctx_->session_info_))) {
      LOG_WARN("failed to formalize stmt", K(ret));
    }
  }
  return ret;
}

class ReplaceColumnsAndAggrs : public ObIRawExprReplacer
{
public:
  int generate_new_expr(ObRawExprFactory &expr_factory,
                        ObRawExpr *old_expr,
                        ObRawExpr *&new_expr)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(old_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is null", K(ret), K(old_expr));
    } else if (old_expr->get_expr_type() == T_FUN_COUNT) {
      ObConstRawExpr *const_zero = NULL;
      if (OB_FAIL(ObTransformUtils::build_const_expr_for_count(expr_factory, 0, const_zero))) {
        LOG_WARN("failed to replace count with 0", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::try_add_cast_expr_above(&expr_factory,
                                                                 session_,
                                                                 *const_zero,
                                                                 old_expr->get_result_type(),
                                                                 new_expr))) {
        LOG_WARN("failed to add cast expr above", K(ret));
      }
    } else if (old_expr->is_column_ref_expr() || old_expr->is_aggr_expr()) {
      // replace with NULL
      ObRawExpr *null_expr = NULL;
      if (OB_FAIL(ObRawExprUtils::build_null_expr(expr_factory, null_expr))) {
        LOG_WARN("failed to replace variable with null", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::try_add_cast_expr_above(&expr_factory,
                                                                 session_,
                                                                 *null_expr,
                                                                 old_expr->get_result_type(),
                                                                 new_expr))) {
        LOG_WARN("failed to add cast expr above", K(ret));
      }
    } else if (old_expr->is_const_or_param_expr()) {
      if (OB_FAIL(expr_factory.create_raw_expr(old_expr->get_expr_class(),
                                               old_expr->get_expr_type(),
                                               new_expr))) {
        LOG_WARN("failed to create raw expr", K(ret));
      } else if (OB_FAIL(new_expr->assign(*old_expr))) {
        LOG_WARN("failed to assign old expr", K(ret));
      }
    }
    return ret;
  }

  ObSQLSessionInfo *session_;
};

int ObTransformAggrSubquery::deduce_query_values(ObDMLStmt &stmt,
                                                 TransformParam &param,
                                                 ObIArray<ObRawExpr *> &view_select,
                                                 ObIArray<ObRawExpr *> &view_columns,
                                                 ObIArray<ObRawExpr *> &real_values)
{
  int ret = OB_SUCCESS;
  ObRawExpr *not_null_expr = param.not_null_expr_;
  ObIArray<bool> &is_null_prop = param.is_null_prop_;
  const bool is_outer_join = use_outer_join(param.pullup_flag_);
  if (OB_ISNULL(ctx_) ||
      OB_ISNULL(ctx_->session_info_) ||
      OB_ISNULL(ctx_->expr_factory_) ||
      OB_UNLIKELY(is_null_prop.count() > view_select.count()) ||
      OB_UNLIKELY(is_null_prop.count() > view_columns.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid session or invalid array size", K(ret));
  } else {
    ObRawExprCopier copier(*ctx_->expr_factory_);
    ReplaceColumnsAndAggrs replacer;
    replacer.session_ = ctx_->session_info_;
    for (int64_t i = 0; OB_SUCC(ret) && i < is_null_prop.count(); ++i) {
      // replace_columns_and_aggrs() may change expr result type, e.g.: sum() from ObNumberType
      // to ObNullType. This may cause operand implicit cast be added twice, so we erase it first.
      ObRawExpr *default_expr = NULL;
      ObRawExpr *case_when_expr = NULL;
      if (OB_FAIL(real_values.push_back(view_columns.at(i)))) {
        LOG_WARN("failed to push back view columns", K(ret));
      } else if (is_null_prop.at(i) || !is_outer_join) {
        continue;
      } else if (OB_ISNULL(not_null_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("not null expr is null", K(ret));
      } else if (OB_FAIL(copier.copy_on_replace(view_select.at(i),
                                                default_expr,
                                                &replacer))) {
        LOG_WARN("failed to generate default expr", K(ret));
        // link.zt, shall we add a cast here
      } else if (OB_FAIL(default_expr->formalize(ctx_->session_info_))) {
        LOG_WARN("failed to formalize default expr", K(ret));
      } else if (OB_FAIL(ObTransformUtils::build_case_when_expr(stmt,
                                                                not_null_expr,
                                                                view_columns.at(i),
                                                                default_expr,
                                                                case_when_expr,
                                                                ctx_))) {
        LOG_WARN("failed to build case when expr", K(ret));
      } else if (OB_ISNULL(case_when_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("case when expr is null", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::try_add_cast_expr_above(ctx_->expr_factory_,
                                                                 ctx_->session_info_,
                                                                 *case_when_expr,
                                                                 view_columns.at(i)->get_result_type(),
                                                                 real_values.at(i)))) {
        LOG_WARN("failed to add cast expr", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformAggrSubquery::transform_from_list(ObDMLStmt &stmt,
                                                 TableItem *view_table_item,
                                                 const ObIArray<ObRawExpr *> &joined_conds,
                                                 const int64_t pullup_flag)
{
  int ret = OB_SUCCESS;
  if (!use_outer_join(pullup_flag)) {
    // create inner join for null-reject condition
    if (OB_FAIL(stmt.add_from_item(view_table_item->table_id_, false))) {
      LOG_WARN("failed to push back from item", K(ret));
    } else if (OB_FAIL(stmt.add_condition_exprs(joined_conds))) {
      LOG_WARN("failed to add condition exprs", K(ret));
    }
  } else {
    // create outer join
    TableItem *joined_table = NULL;
    if (OB_FAIL(ObTransformUtils::merge_from_items_as_inner_join(
                  ctx_, stmt, joined_table))) {
      LOG_WARN("failed to merge from items as inner join", K(ret));
    } else if (OB_FAIL(ObTransformUtils::add_new_joined_table(ctx_,
                                                              stmt,
                                                              LEFT_OUTER_JOIN,
                                                              joined_table,
                                                              view_table_item,
                                                              joined_conds,
                                                              joined_table))) {
      LOG_WARN("failed to add new joined table", K(ret));
    } else if (OB_ISNULL(joined_table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("joined table is null", K(ret));
    } else if (OB_FAIL(stmt.add_from_item(joined_table->table_id_, true))) {
      LOG_WARN("failed to add from item", K(ret));
    }
  }
  return ret;
}

int ObTransformAggrSubquery::extract_nullable_exprs(
    const ObRawExpr *expr, ObIArray<const ObRawExpr *> &vars)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (expr->is_column_ref_expr() ||
             (expr->is_aggr_expr() && expr->get_expr_type() != T_FUN_COUNT)) {
    if (OB_FAIL(vars.push_back(expr))) {
      LOG_WARN("failed to push back expr", K(ret));
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(extract_nullable_exprs(expr->get_param_expr(i), vars)))) {
        LOG_WARN("failed to extract nullable expr", K(ret));
      }
    }
  }
  return ret;
}

/**
 * @brief ObTransformAggrSubquery::do_join_first_transform
 * STEP 1: 找到一个可改写的子查询 (同时确定改写的策略 INNER JON/ OUTER JOIN)
 * STEP 2: 从 STMT 上构造一个 SPJ 查询
 * STEP 3: 在 SPJ 查询上执行改写
 * @return
 */
int ObTransformAggrSubquery::transform_with_join_first(ObDMLStmt *&stmt,
                                                       bool &trans_happened)
{
  int ret = OB_SUCCESS;
  bool is_valid = false;
  int64_t query_num = 0;
  ObDMLStmt *target_stmt = stmt;
  const ObQueryHint* query_hint = nullptr;
  OPT_TRACE("try join first");
  if (OB_ISNULL(stmt) || OB_ISNULL(query_hint = stmt->get_stmt_hint().query_hint_) ||
      OB_ISNULL(ctx_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null stmt", K(ret), K(stmt), K(query_hint));
  } else if (OB_FAIL(check_stmt_valid(*stmt, is_valid))) {
    LOG_WARN("failed to check stmt valid", K(ret));
  } else if (is_valid) {
    query_num = stmt->get_subquery_expr_size();
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < query_num; ++i) {
    TransformParam param;
    ObSelectStmt *view_stmt = NULL;
    ObRawExpr *root_expr = NULL;
    bool post_group_by = false;
    if (OB_FAIL(get_trans_param(*target_stmt, param, root_expr, post_group_by))) {
      LOG_WARN("failed to find trans params", K(ret));
    } else if (NULL == root_expr) {
      break;
    } else if (!param.limit_for_exists_ &&
               param.limit_value_ > 0 &&
               OB_FAIL(convert_limit_as_aggr(param.ja_query_ref_->get_ref_stmt(),
                                             param))) {
      LOG_WARN("fail to transform for limit");
    } else if (OB_FAIL(fill_query_refs(target_stmt, root_expr, param))) {
      LOG_WARN("failed to fill query refs", K(ret));
    } else if (OB_FAIL(get_trans_view(*target_stmt,
                                      view_stmt,
                                      root_expr,
                                      post_group_by))) {
      LOG_WARN("failed to get transform view", K(ret));
    } else if (OB_UNLIKELY(!ObOptimizerUtil::find_item(view_stmt->get_subquery_exprs(),
                                                       param.ja_query_ref_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the subquery is not found in view stmt", K(ret), KPC(view_stmt), KPC(param.ja_query_ref_));
    } else if (OB_FAIL(do_join_first_transform(*view_stmt, param, root_expr, !join_first_happened_))) {
      LOG_WARN("failed to do join first transform", K(ret));
    } else if (OB_FAIL(ObTransformUtils::add_param_not_null_constraint(*ctx_, param.not_null_const_))) {
      LOG_WARN("failed to add param not null constraints", K(ret));
    } else {
      target_stmt = view_stmt;
      trans_happened = true;
      join_first_happened_ = true;
      if (OB_FAIL(add_constraints_for_limit(param))) {
        LOG_WARN("add constraints failed", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformAggrSubquery::add_constraints_for_limit(TransformParam &param)
{
  int ret = OB_SUCCESS;
  ObConstRawExpr *value_expr = NULL;
  ObConstRawExpr *zero_expr= NULL;
  ObOpRawExpr *true_expr= NULL;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null", K(ret));
  } else if (!param.limit_for_exists_) {
  } else if (OB_FAIL(ctx_->expr_factory_->create_raw_expr(T_INT, value_expr)) ||
              OB_FAIL(ctx_->expr_factory_->create_raw_expr(T_INT, zero_expr)) ||
              OB_FAIL(ctx_->expr_factory_->create_raw_expr(T_OP_GT, true_expr))) {
    LOG_WARN("create raw expr fail", K(ret));
  } else if (OB_ISNULL(value_expr) ||
              OB_ISNULL(zero_expr) ||
              OB_ISNULL(true_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null expr", K(ret));
  } else if (OB_FAIL(true_expr->set_param_exprs(value_expr, zero_expr))) {
    LOG_WARN("set param expr fail", K(ret));
  } else {
    ObObj obj_zero;
    obj_zero.set_int(ObIntType, 0);
    zero_expr->set_value(obj_zero);

    ObObj obj_value;
    obj_value.set_int(ObIntType, param.limit_value_);
    value_expr->set_value(obj_value);
  }
  if (OB_SUCC(ret) && param.limit_for_exists_) {
    if (OB_FAIL(true_expr->formalize(ctx_->exec_ctx_->get_my_session()))) {
      LOG_WARN("fail to formalize expr", K(ret));
    } else if (OB_FAIL(ObTransformUtils::add_param_bool_constraint(ctx_, true_expr, true))) {
      LOG_WARN("fail to add is true constraint", K(ret));
    }
  }
  return ret;
}

/**
 * @brief ObTransformAggrSubquery::get_trans_param
 * process subqueries from `where, group by, having, select, update assign`
 * @return
 */
int ObTransformAggrSubquery::get_trans_param(ObDMLStmt &stmt,
                                             TransformParam &param,
                                             ObRawExpr *&root_expr,
                                             bool &post_group_by)
{
  int ret = OB_SUCCESS;
  ObSEArray<TransformParam, 4> params;
  ObSEArray<ObRawExpr *, 4> pre_group_by_exprs;  // where, group exprs
  ObSEArray<ObRawExpr *, 4> post_group_by_exprs; // having, select, assign values exprs
  ObSEArray<ObRawExpr *, 4> invalid_list;
  bool has_rownum = false;
  if (OB_FAIL(pre_group_by_exprs.assign(stmt.get_condition_exprs()))) {
    LOG_WARN("failed to assign conditions", K(ret));
  } else if (OB_FAIL(stmt.has_rownum(has_rownum))) {
    LOG_WARN("failed to check has rownum", K(ret));
  } else if (has_rownum) {
    // do nothing
  } else if (stmt.is_select_stmt()) {
    ObSelectStmt &sel_stmt = static_cast<ObSelectStmt &>(stmt);
    if (sel_stmt.has_rollup()) {
      // we can create spj view with group-by pushed down for normal group by
    } else if (OB_FAIL(append(pre_group_by_exprs, sel_stmt.get_group_exprs()))) {
      LOG_WARN("failed to append group by exprs", K(ret));
    } else if (OB_FAIL(append(pre_group_by_exprs, sel_stmt.get_aggr_items()))) {
      LOG_WARN("failed to append aggr items", K(ret));
    } else if (sel_stmt.is_scala_group_by()) {
      // there is no group by clause, we can directly pullup subqueries
    } else if (OB_FAIL(append(post_group_by_exprs, sel_stmt.get_having_exprs()))) {
      LOG_WARN("failed to append having exprs", K(ret));
    } else if (OB_FAIL(sel_stmt.get_select_exprs(post_group_by_exprs))) {
      LOG_WARN("failed to get select exprs", K(ret));
    }
  } else if (stmt.is_update_stmt()) {
    ObUpdateStmt &upd_stmt = static_cast<ObUpdateStmt &>(stmt);
    if (OB_FAIL(upd_stmt.get_assign_values(pre_group_by_exprs, true))) {
      LOG_WARN("failed to get assign values", K(ret));
    }
  }
  int64_t pre_count = pre_group_by_exprs.count();
  int64_t post_count = post_group_by_exprs.count();
  for (int64_t i = 0; OB_SUCC(ret) && NULL == root_expr && i < pre_count + post_count; ++i) {
    params.reset();
    post_group_by = (i >= pre_count);
    ObRawExpr *expr = !post_group_by ? pre_group_by_exprs.at(i) :
                                       post_group_by_exprs.at(i - pre_count);
    ObSEArray<ObRawExpr *, 4> filters;
    bool is_filter = check_is_filter(stmt, expr);
    if (is_exists_op(expr->get_expr_type()) && !is_filter) {
      // only rewrite [not] exists in where or having
    } else if (OB_FAIL(gather_transform_params(stmt, expr, expr, JOIN_FIRST, 
                                               false, // is_select_item_expr, for aggr first only
                                               params))) {
      LOG_WARN("failed to gather transform params", K(ret));
    } else if (OB_FAIL(get_filters(stmt, expr, filters))) {
      LOG_WARN("failed to get filters", K(ret));
    }
    for (int64_t j = 0; OB_SUCC(ret) && NULL == root_expr && j < params.count(); ++j) {
      bool is_valid = true;
      if (ObOptimizerUtil::find_item(invalid_list, params.at(j).ja_query_ref_)) {
        is_valid = false;
      } else if (is_exists_op(expr->get_expr_type())) {
        if (OB_FAIL(choose_pullup_method_for_exists(params.at(j).ja_query_ref_,
                                                    params.at(j).pullup_flag_,
                                                    T_OP_NOT_EXISTS == expr->get_expr_type(),
                                                    is_valid))) {
          LOG_WARN("failed to choose pullup method for exists", K(ret));
        }
      } else {
        if (OB_FAIL(choose_pullup_method(filters, params.at(j)))) {
          LOG_WARN("failed to choose pullup method", K(ret));
        }
      }
      if (OB_SUCC(ret) && is_valid && use_outer_join(params.at(j).pullup_flag_)) {
        if (OB_FAIL(check_can_use_outer_join(params.at(j), is_valid))) {
          LOG_WARN("failed to check use outer join", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (!is_valid) {
          if (OB_FAIL(invalid_list.push_back(params.at(j).ja_query_ref_))) {
            LOG_WARN("failed to push back invalid subquery", K(ret));
          }
        } else {
          root_expr = expr;
          post_group_by = (i >= pre_count);
          param = params.at(j);
        }
      }
    }
  }
  return ret;
}

/**
 * @brief ObTransformAggrSubquery::get_trans_view
 * @param stmt 待改写的查询
 * @param view_stmt 分离后可改写的视图
 * @param root_expr 包含子查询的表达式
 * @param post_group_by 当前表达式是否需要在 group-by 之后处理
 * @return
 */
int ObTransformAggrSubquery::get_trans_view(ObDMLStmt &stmt,
                                            ObSelectStmt *&view_stmt,
                                            ObRawExpr *root_expr,
                                            bool post_group_by)
{
  int ret = OB_SUCCESS;
  bool has_rownum = false;
  bool has_groupby = false;
  if (OB_FAIL(stmt.has_rownum(has_rownum))) {
    LOG_WARN("failed to check has rownum", K(ret));
  } else if (stmt.is_select_stmt() && static_cast<ObSelectStmt &>(stmt).has_group_by()) {
    has_groupby = true;
  }
  if (OB_SUCC(ret)) {
    TableItem *table = NULL;
    ObAliasRefRawExpr *alias = NULL;
    bool push_group_by = stmt.is_select_stmt() && post_group_by && has_groupby;
    bool push_vector_assign = stmt.is_update_stmt() && root_expr->has_flag(CNT_ALIAS);
    if (!has_rownum && !has_groupby && stmt.is_select_stmt()) {
      view_stmt = static_cast<ObSelectStmt *>(&stmt);
    } else if (push_vector_assign &&
               OB_FAIL(ObRawExprUtils::find_alias_expr(root_expr, alias))) {
      LOG_WARN("failed to find alias expr", K(ret));
    } else if (OB_FAIL(ObTransformUtils::create_simple_view(ctx_, &stmt, view_stmt,
                                                            true, true, push_group_by,
                                                            alias))) {
      LOG_WARN("failed to create simple view", K(ret));
    } else if (push_group_by) {
      view_stmt = static_cast<ObSelectStmt *>(&stmt);
    }
  }
  return ret;
}

int ObTransformAggrSubquery::check_stmt_valid(ObDMLStmt &stmt, bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  bool can_set_unique = false;
  if (stmt.is_set_stmt() || stmt.is_hierarchical_query() || !stmt.is_sel_del_upd()) {
    is_valid = false;
  } else if (OB_FAIL(ObTransformUtils::check_can_set_stmt_unique(&stmt, can_set_unique))) {
    LOG_WARN("failed to check can set stmt unque", K(ret));
  } else if (!can_set_unique) {
    is_valid = false;
  }
  return ret;
}

/**
 * @brief ObTransformAggrSubquery::check_join_first_validity
 *  check the valiaidty of the subquery
 */
int ObTransformAggrSubquery::check_join_first_validity(ObQueryRefRawExpr &query_ref,
                                                       const bool is_vector_assign,
                                                       const bool in_exists,
                                                       const bool is_vector_cmp,
                                                       ObIArray<ObRawExpr *> &constraints,
                                                       bool &add_limit_constraints,
                                                       int64_t &limit_value,
                                                       bool &is_valid,
                                                       ObIArray<ObPCParamEqualInfo> &equal_param_info)
{
  int ret = OB_SUCCESS;
  bool has_rownum = false;
  bool has_correlated_cond = false;
  bool has_ref_assign_user_var = false;
  bool is_correlated = false;
  bool limit_to_aggr = false;
  ObSEArray<ObRawExpr *, 4> nested_conditions;
  ObSelectStmt *subquery = NULL;
  is_valid = true;
  if (OB_ISNULL(subquery = query_ref.get_ref_stmt()) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->exec_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("subquery is null", K(ret));
    // 0. check single set
  } else if (OB_FAIL(check_single_set_subquery(*subquery,
                                               is_valid,
                                               limit_to_aggr,
                                               limit_value,
                                               !in_exists))) {
    LOG_WARN("failed to check is single set query", K(ret));
  } else if (!is_valid) {
    OPT_TRACE("not single set query");
    // 1. check stmt components
  } else if (subquery->has_rollup() ||
             subquery->has_window_function() ||
             subquery->has_sequence() ||
             subquery->is_set_stmt() ||
             subquery->is_hierarchical_query()) {
    is_valid = false;
    LOG_TRACE("invalid subquery", K(is_valid), K(*subquery));
    OPT_TRACE("subquery has rollup/win_func/sequence");
  } else if (in_exists) {
    if (!subquery->has_having()) {
      is_valid = false;
      LOG_TRACE("invalid [not] exists subquery", K(is_valid), K(*subquery));
      OPT_TRACE("subquery do not has having")
    } else if (OB_FAIL(check_subquery_having(query_ref, *subquery, is_valid))) {
      LOG_WARN("failed to check subquery having", K(ret));
    } else if (!is_valid) {
      LOG_TRACE("invalid having clause", K(is_valid));
      OPT_TRACE("having condition has subquery / do not has aggr");
    } else if (!subquery->has_limit()) {
      /*do nothing*/
    } else if (OB_FAIL(check_limit_validity(*subquery, 
                                            add_limit_constraints,
                                            limit_value, 
                                            is_valid))) {
      LOG_WARN("check limit failed", K(ret));
    }
  } else {
    if (subquery->has_having() ||
        NULL != subquery->get_limit_percent_expr() ||
        NULL != subquery->get_offset_expr()) {
      is_valid = false;
      LOG_TRACE("invalid subquery", K(is_valid), K(*subquery));
      OPT_TRACE("subquery has having or limit percent or limit offset");
    } else if (OB_FAIL(check_subquery_select(query_ref, is_valid))) {
      LOG_WARN("failed to check subquery select", K(ret));
    } else if (!is_valid) {
      OPT_TRACE("subquery select item has subquery");
    }
  }
  if (OB_FAIL(ret) || !is_valid) {
  } else if (OB_FAIL(subquery->has_rownum(has_rownum))) {
    LOG_WARN("failed to check subquery has rownum", K(ret));
  } else if (has_rownum) {
    is_valid = false;
    LOG_TRACE("has rownum expr", K(is_valid));
    OPT_TRACE("has rownum expr");
  } else if (OB_FAIL(subquery->has_ref_assign_user_var(has_ref_assign_user_var))) {
    LOG_WARN("failed to check stmt has assignment ref user var", K(ret));
  } else if (has_ref_assign_user_var) {
    is_valid = false;
    LOG_TRACE("has assignment ref user variable", K(is_valid));
    OPT_TRACE("has assignment ref user variable");
  } else if (!is_vector_assign && !in_exists && !is_vector_cmp && subquery->get_select_item_size() > 1) {
    is_valid = false;
    OPT_TRACE("more than one select item");
    LOG_TRACE("select item size is invalid",
              K(is_vector_assign), K(subquery->get_select_item_size()));
  // 2. check the aggr item
  } else if (OB_FAIL(check_subquery_aggr_item(*subquery, is_valid))) {
    LOG_WARN("failed to check subquery aggregation item", K(ret));
  } else if (!is_valid) {
    // do nothing
    OPT_TRACE("subquery has AVG aggregation");
  } else if (OB_FAIL(check_count_const_validity(*subquery, constraints, is_valid))) {
    LOG_WARN("failed to check is valid count const", K(ret));
  } else if (!is_valid) {
    LOG_TRACE("aggr item is invalid", K(is_valid));
    OPT_TRACE("exists COUNT(NULL)");
    // never reach
  // 3. check from list is not correlated
  } else if (OB_FAIL(ObTransformUtils::is_table_item_correlated(
                       query_ref, *subquery, is_correlated))) {
    LOG_WARN("failed to check subquery table item is correlated", K(ret));
  } else if (is_correlated) {
    is_valid = false;
    OPT_TRACE("subquery`s table item is correlated");
    // 4. check correlated join on contiditons
    // 5. check correlated semi contiditons
  } else if (OB_FAIL(ObTransformUtils::is_join_conditions_correlated(query_ref, subquery, is_correlated))) {
    LOG_WARN("failed to check join condition correlated", K(ret));
  } else if (is_correlated) {
    is_valid = false;
    OPT_TRACE("subquery`s outer/semi join on condition is correlated");
  }
  // 5. check correlated join contiditons
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < subquery->get_condition_size(); ++i) {
    ObRawExpr *cond = subquery->get_condition_expr(i);
    if (OB_ISNULL(cond)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("condition expr is null", K(ret));
    } else if (!IS_COMMON_COMPARISON_OP(cond->get_expr_type()) ||
               T_OP_NSEQ == cond->get_expr_type() ||
               !cond->has_flag(CNT_COLUMN)) {
      // do nothing
    } else if (OB_FAIL(ObTransformUtils::is_correlated_expr(query_ref.get_exec_params(),
                                                            cond,
                                                            is_correlated))) {
      LOG_WARN("failed to check is correlated expr", K(ret));
    } else if (!is_correlated || !cond->has_flag(CNT_COLUMN)) {
      // do nothing
    } else if (cond->has_flag(CNT_SUB_QUERY)) {
      is_valid = false;
      LOG_TRACE("is not valid equal or common comparsion correlation", K(i), K(*cond));
      OPT_TRACE("is not valid equal or common comparsion correlation");
    } else if (OB_FAIL(nested_conditions.push_back(cond))) {
      LOG_WARN("failed to push back expr", K(ret));
    } else {
      has_correlated_cond = true;
    }
  }
  /**
   * for count(const), transfrom is valid if one of following condition is satisfied
   *  1. from item only has one basic table
   *  2. one side of correlated condition only has subquery's expr
   */
  if (OB_FAIL(ret) || !is_valid) {
  } else if (!has_correlated_cond) {
    is_valid = false;
    LOG_TRACE("no correlated common comparsion condition found", K(is_valid), K(has_correlated_cond));
    OPT_TRACE("no correlated common comparsion condition found");
  } else if (limit_to_aggr && OB_FAIL(check_join_first_condition_for_limit_1(query_ref, *subquery, is_valid))) {
    LOG_WARN("failed to check condition for limit 1", K(ret));
  } else if (!is_valid) {
    // do nothing
  } else if (limit_to_aggr &&
             OB_FAIL(check_subquery_select_for_limit_1(*subquery, is_valid, equal_param_info))) {
    LOG_WARN("failed to check select validity for limit 1", K(ret));
  }
  return ret;
}


int ObTransformAggrSubquery::check_limit_validity(ObSelectStmt &subquery, 
                                                  bool &add_limit_constraints,
                                                  int64_t &limit_value, 
                                                  bool &is_valid)
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = NULL;
  ObRawExpr *limit_expr = NULL;
  add_limit_constraints = false;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->exec_ctx_) ||
      OB_ISNULL(plan_ctx = ctx_->exec_ctx_->get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("subquery is null", K(ret));
  } else if (OB_ISNULL(limit_expr = subquery.get_limit_expr())
            || OB_UNLIKELY(T_INT != limit_expr->get_expr_type() 
                           && T_QUESTIONMARK != limit_expr->get_expr_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected limit expr", K(ret));
  } else if (T_INT == limit_expr->get_expr_type() && 
              0 == static_cast<ObConstRawExpr *>(limit_expr)->get_value().get_int()) {
    is_valid = false;
    LOG_TRACE("get limit 0 in [not] exists expr", K(is_valid));
  } else if (T_QUESTIONMARK == limit_expr->get_expr_type()) {
    int64_t const_value = OB_INVALID;
    ObObj value;
    int64_t idx = static_cast<ObConstRawExpr *>(limit_expr)->get_value().get_unknown();
    const ParamStore &param_store = plan_ctx->get_param_store();
      
    if (idx < 0 || idx >= param_store.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("idx is invalid", K(idx), K(param_store.count()), K(ret));
    } else {
      number::ObNumber number;
      value = param_store.at(idx);
      if (value.is_integer_type()) {
        const_value = value.get_int();
      } else if (value.is_number()) {
        if (OB_FAIL(value.get_number(number))) {
          LOG_WARN("failed to get number", K(ret));
        } else if (OB_UNLIKELY(!number.is_valid_int64(const_value))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("number is not valid int64", K(ret), K(value), K(number));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (FALSE_IT(limit_value = const_value)) {
      } else if (const_value == 0) {
        is_valid = false;
        add_limit_constraints = true;
        LOG_TRACE("get limit 0 in [not] exists expr", K(is_valid));
      } 
    }
  }
  return ret;
}
/**
 * @brief check the select item, and the subquery comparison operator
 *        determine the join method
 **/
int ObTransformAggrSubquery::choose_pullup_method_for_exists(ObQueryRefRawExpr *query_ref,
                                                             int64_t &pullup_flag,
                                                             const bool with_not,
                                                             bool &is_valid)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *subquery = NULL;
  ObSEArray<const ObRawExpr*, 4> vars;
  is_valid = true;
  if (OB_ISNULL(query_ref) || OB_ISNULL(subquery = query_ref->get_ref_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params are invalid", K(ret), K(query_ref), K(subquery));
  } else if (with_not) {
    pullup_flag |= USE_OUTER_JOIN;
  } else {
    // use outer join if all having expr is not null reject
    bool all_not_null_reject = true;
    for (int64_t i = 0; OB_SUCC(ret) && all_not_null_reject && i < subquery->get_having_expr_size(); ++i) {
      bool is_null_reject = false;
      vars.reuse();
      ObRawExpr *cur_expr = subquery->get_having_exprs().at(i);
      if (OB_ISNULL(cur_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("having expr is null", K(ret));
      } else if (!(IS_COMMON_COMPARISON_OP(cur_expr->get_expr_type())
                  && T_OP_NSEQ != cur_expr->get_expr_type())) {
        is_valid = false;
      } else if (OB_FAIL(extract_nullable_exprs(cur_expr, vars))) {
        LOG_WARN("failed to extract nullable exprs", K(ret));
      } else if (vars.count() <= 0) {
        // do nothing
      } else if (OB_FAIL(ObTransformUtils::is_null_reject_condition(cur_expr, vars, is_null_reject))) {
        LOG_WARN("failed to check is null reject condition", K(ret));
      }
      if (OB_SUCC(ret) && is_null_reject) {
        all_not_null_reject = false;
      }
    }
    if (OB_SUCC(ret) && all_not_null_reject) {
      pullup_flag |= USE_OUTER_JOIN;
    }
  }
  return ret;
}

int ObTransformAggrSubquery::do_join_first_transform(ObSelectStmt &select_stmt,
                                                    TransformParam &trans_param,
                                                    ObRawExpr *root_expr,
                                                    const bool is_first_trans)
{
  int ret = OB_SUCCESS;
  ObQueryRefRawExpr *query_ref_expr = NULL;
  ObSelectStmt *subquery = NULL;
  ObRawExprFactory *expr_factory = NULL;
  ObRawExpr *parent_expr_of_query_ref = NULL;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->session_info_) || OB_ISNULL(root_expr)
      || OB_ISNULL(expr_factory = ctx_->expr_factory_)
      || OB_ISNULL(query_ref_expr = trans_param.ja_query_ref_)
      || OB_ISNULL(subquery = trans_param.ja_query_ref_->get_ref_stmt())
      || OB_ISNULL(parent_expr_of_query_ref = trans_param.parent_expr_of_query_ref)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret), K(ctx_), K(root_expr), K(query_ref_expr), K(subquery));
  } else if (OB_FAIL(get_unique_keys(select_stmt, select_stmt.get_group_exprs(), is_first_trans))) {
    LOG_WARN("failed to get unique exprs", K(ret));
  }

  // modify aggr param expr to deal with empty-set-selected situation
  if (OB_SUCC(ret) && use_outer_join(trans_param.pullup_flag_)) {
    if (OB_FAIL(modify_aggr_param_expr_for_outer_join(trans_param))) {
      LOG_WARN("failed to modify subquery aggr param expr", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObOptimizerUtil::remove_item(select_stmt.get_subquery_exprs(),
                                             query_ref_expr))) {
      LOG_WARN("failed to remove query ref expr", K(ret));
    } else if (is_exists_op(root_expr->get_expr_type())) {
      bool need_lnnvl = root_expr->get_expr_type() == T_OP_NOT_EXISTS;
      if (OB_FAIL(ObOptimizerUtil::remove_item(select_stmt.get_condition_exprs(), root_expr))) {
        LOG_WARN("failed to remove exprs", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < subquery->get_having_expr_size(); ++i) {
        ObRawExpr *cond = subquery->get_having_exprs().at(i);
        if (need_lnnvl && OB_FAIL(ObRawExprUtils::build_lnnvl_expr(*expr_factory, cond, cond))) {
          LOG_WARN("failed to build lnnvl expr", K(ret));
        } else if (OB_FAIL(select_stmt.add_having_expr(cond))) {
          LOG_WARN("failed to add having condition", K(ret));
        }
      }
    } else {
      ObSEArray<ObRawExpr *, 4> select_exprs;
      if (OB_FAIL(subquery->get_select_exprs(select_exprs))) {
        LOG_WARN("failed to get select exprs", K(ret));
      } else if (OB_FAIL(modify_vector_comparison_expr_if_necessary(select_stmt, expr_factory, select_exprs, parent_expr_of_query_ref))) {
        LOG_WARN("failed to modify vector comparison expr", K(ret));
      } else if (OB_FAIL(select_stmt.replace_relation_exprs(trans_param.query_refs_,
                                                            select_exprs))) {
        LOG_WARN("failed to update query ref value expr", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    // pullup subquery component
    if (OB_FAIL(append(select_stmt.get_table_items(), subquery->get_table_items()))) {
      LOG_WARN("failed to append table items", K(ret));
    } else if (OB_FAIL(transform_from_list(select_stmt, *subquery, trans_param.pullup_flag_))) {
      LOG_WARN("failed to transform from list",K(ret));
    } else if (OB_FAIL(append(select_stmt.get_column_items(), subquery->get_column_items()))) {
      LOG_WARN("failed to append column items", K(ret));
    } else if (OB_FAIL(append(select_stmt.get_part_exprs(), subquery->get_part_exprs()))) {
      LOG_WARN("failed to append part exprs", K(ret));
    } else if (OB_FAIL(append(select_stmt.get_check_constraint_items(),
                              subquery->get_check_constraint_items()))) {
      LOG_WARN("failed to append check constraint items", K(ret));
    } else if (OB_FAIL(append(select_stmt.get_aggr_items(), subquery->get_aggr_items()))) {
      LOG_WARN("failed to append aggr items", K(ret));
    } else if (OB_FAIL(append(select_stmt.get_subquery_exprs(), subquery->get_subquery_exprs()))) {
      LOG_WARN("failed to append subquery exprs", K(ret));
    } else if (OB_FAIL(select_stmt.rebuild_tables_hash())) {
      LOG_WARN("failed to rebuild table hash", K(ret));
    } else if (OB_FAIL(select_stmt.update_column_item_rel_id())) {
      LOG_WARN("failed to update colun item rel id", K(ret));
    } else if (OB_FAIL(append(select_stmt.get_semi_infos(), subquery->get_semi_infos()))) {
      LOG_WARN("failed to append semi infos", K(ret));
    } else if (OB_FAIL(ObTransformUtils::decorrelate(&select_stmt, query_ref_expr->get_exec_params()))) {
      LOG_WARN("failed to decorrelate stmt", K(ret));
    } else if (OB_FAIL(select_stmt.formalize_stmt(ctx_->session_info_))) {
      LOG_WARN("failed to formalize stmt", K(ret));
    } else if (OB_FAIL(rebuild_conditon(select_stmt, *subquery))) {
      LOG_WARN("failed to rebuild condition", K(ret));
    } else if (OB_FAIL(add_trans_stmt_info(*subquery, JOIN_FIRST))) {
      LOG_WARN("failed to add trans stmt info", K(ret));
    }
  }
  return ret;
}


/**
 * @brief when subquery returns vector results and is used in comparison expr, two modifications are needed
 *   1. select exprs of subquery need to be integrated into a T_OP_ROW expr
 *   2. parent expr of subquery should be replaced to a new one with common comparision
 */
int ObTransformAggrSubquery::modify_vector_comparison_expr_if_necessary(
  ObSelectStmt &select_stmt, ObRawExprFactory *expr_factory, ObSEArray<ObRawExpr*, 4> &select_exprs, ObRawExpr *parent_expr_of_query_ref) {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr_factory) || OB_ISNULL(parent_expr_of_query_ref) || select_exprs.count() == 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret), K(expr_factory), K(parent_expr_of_query_ref), K(select_exprs));
  } else if (IS_SUBQUERY_COMPARISON_OP(parent_expr_of_query_ref->get_expr_type())) {
    // construct a T_OP_ROW expr through vector projection term of subquery
    ObOpRawExpr* op_row_expr;
    ObSEArray<ObRawExpr*, 4> temp_expr;
    if (OB_FAIL(ObTransformUtils::build_row_expr(*expr_factory, select_exprs, op_row_expr))) {
      LOG_WARN("failed to create op row expr", K(ret));
    } else if (OB_FAIL(temp_expr.push_back(op_row_expr))) {
      LOG_WARN("failed to push back op row expr", K(ret));
    }
    select_exprs.assign(temp_expr);
    // replace parent_expr_of_query_ref with subquery comparison operator to the one of common comparison operator
    ObItemType value_cmp_type = T_INVALID;
    if (OB_FAIL(ObTransformUtils::query_cmp_to_value_cmp(parent_expr_of_query_ref->get_expr_type(), value_cmp_type))) {
      LOG_WARN("unexpected root_expr type", K(ret), K(value_cmp_type));
    } else {
      ObOpRawExpr *new_parent_expr = NULL;
      ObSEArray<ObRawExpr*, 1> old_exprs;
      ObSEArray<ObRawExpr*, 1> new_exprs;

      if (OB_FAIL(expr_factory->create_raw_expr(value_cmp_type, new_parent_expr))) {
        LOG_WARN("failed to build expr", K(ret), K(new_parent_expr));
      } else {
        for (int64_t i=0; i < parent_expr_of_query_ref->get_param_count(); i++) {
          if (OB_FAIL(new_parent_expr->add_param_expr(parent_expr_of_query_ref->get_param_expr(i)))) {
            LOG_WARN("failed to add param expr", K(ret));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(old_exprs.push_back(parent_expr_of_query_ref)) ||
                   OB_FAIL(new_exprs.push_back(new_parent_expr))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to push back expr", K(ret));
        } else if (OB_FAIL(select_stmt.replace_relation_exprs(old_exprs, new_exprs))) {
          LOG_WARN("failed to replace expr in stmt", K(ret));
        }
      }
    }
  }
  return ret;
}

/**
 * 获取from list中所有基表的primary key
 */
int ObTransformAggrSubquery::get_unique_keys(ObDMLStmt &stmt,
                                             ObIArray<ObRawExpr *> &pkeys,
                                             const bool is_first_trans)
{
  int ret = OB_SUCCESS;
  ObSqlBitSet<> empty_ignore_tables;
  TableItem *cur_table = NULL;
  const ObQueryHint *query_hint = NULL;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_) ||
      OB_ISNULL(query_hint = stmt.get_stmt_hint().query_hint_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("transform context is invalid", K(ret), K(ctx_), K(query_hint));
  } else if (is_first_trans) {
    // 第一次改写时需要生成所有from table的unique 可以
    if (OB_FAIL(ObTransformUtils::generate_unique_key(ctx_, &stmt, empty_ignore_tables, pkeys))) {
      LOG_WARN("failed to generate unique key", K(ret));
    }
  } else if ((query_hint->has_outline_data() && stmt.get_table_items().count() < 1) ||
             (!query_hint->has_outline_data() && 1 != stmt.get_table_items().count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected table itemc", K(ret), K(stmt.get_table_items().count()));
  } else if (OB_ISNULL(cur_table = stmt.get_table_item(0)) ||
             OB_UNLIKELY(!cur_table->is_generated_table())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected table itemc", K(ret), K(cur_table));
  } else {
    // 已经改写过了，可以直接取改写视图中的group by列
    ObSelectStmt *view_stmt = NULL;
    ObSEArray<ObRawExpr *, 4> select_list;
    ObSEArray<ObRawExpr *, 4> column_list;
    ObSEArray<ObRawExpr *, 4> group_keys;
    ObRawExprCopier copier(*ctx_->expr_factory_);
    if (OB_ISNULL(view_stmt = cur_table->ref_query_) ||
        OB_UNLIKELY(0 == view_stmt->get_group_expr_size())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("view is not a normal group by", K(ret), K(view_stmt));
    } else if (OB_FAIL(stmt.get_view_output(*cur_table, select_list, column_list))) {
      LOG_WARN("failed to get view output", K(ret));
    } else if (OB_FAIL(copier.add_replaced_expr(select_list, column_list))) {
      LOG_WARN("failed to replace pair", K(ret));
    } else if (OB_FAIL(copier.copy_on_replace(view_stmt->get_group_exprs(), group_keys))) {
      LOG_WARN("failed to copy on replace exprs", K(ret));
    } else if (OB_FAIL(append(pkeys, group_keys))) {
      LOG_WARN("failed to append group keys", K(ret));
    }
  }
  return ret;
}

/*
 *  merge from table of parent stmt and subquery stmt 
 * */
int ObTransformAggrSubquery::transform_from_list(ObDMLStmt &stmt,
                                                 ObSelectStmt &subquery,
                                                 const int64_t pullup_flag)
{
  int ret = OB_SUCCESS;
  if (!use_outer_join(pullup_flag)) {
    // create inner join for null-reject condition
    if (OB_FAIL(append(stmt.get_joined_tables(), subquery.get_joined_tables()))) {
      LOG_WARN("failed to append joined tables",K(ret));
    } else if (OB_FAIL(append(stmt.get_from_items(), subquery.get_from_items()))) {
      LOG_WARN("failed to push back from items", K(ret));
    } else if (OB_FAIL(append(stmt.get_condition_exprs(), subquery.get_condition_exprs()))) {
      LOG_WARN("failed to push back condition exprs", K(ret));
    }
  } else {
    // create outer join
    TableItem *joined_table_main = NULL;
    TableItem *joined_table_sub = NULL;
    // 会生成一个新的joined table, 不需要append subquery的joined table
    if (OB_FAIL(ObTransformUtils::merge_from_items_as_inner_join(
                  ctx_, stmt, joined_table_main))) {
      LOG_WARN("failed to merge from items as inner join", K(ret));
    } else if (OB_FAIL(ObTransformUtils::merge_from_items_as_inner_join(
                  ctx_, subquery, joined_table_sub))) {
      LOG_WARN("failed to merge from items as inner join", K(ret));
    } else if (OB_FAIL(ObTransformUtils::add_new_joined_table(ctx_,
                                                              stmt,
                                                              LEFT_OUTER_JOIN,
                                                              joined_table_main,
                                                              joined_table_sub,
                                                              subquery.get_condition_exprs(),
                                                              joined_table_main))) {
      LOG_WARN("failed to add new joined table", K(ret));
    } else if (OB_ISNULL(joined_table_main)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("joined table is null", K(ret));
    } else if (OB_FAIL(stmt.add_from_item(joined_table_main->table_id_, true))) {
      LOG_WARN("failed to add from item", K(ret));
    }
  }
  return ret;
}

int ObTransformAggrSubquery::rebuild_conditon(ObSelectStmt &stmt, ObSelectStmt &subquery)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 8> where_conditions;
  ObIArray<ObRawExpr *> *target_array = NULL;
  if (OB_FAIL(where_conditions.assign(stmt.get_condition_exprs()))) {
    LOG_WARN("failed to assign where conditions", K(ret));
  }
  stmt.get_condition_exprs().reuse();
  for (int64_t i = 0; OB_SUCC(ret) && i < where_conditions.count(); ++i) {
    ObRawExpr *cur_expr = NULL;
    target_array = NULL;
    if (OB_ISNULL(cur_expr = where_conditions.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("condition expr is null", K(ret));
    } else if (cur_expr->has_flag(CNT_AGG)) {
      target_array = &stmt.get_having_exprs();
    } else if (cur_expr->has_flag(CNT_SUB_QUERY)) {
      if (ObOptimizerUtil::find_item(subquery.get_condition_exprs(), cur_expr)) {
        target_array = &stmt.get_condition_exprs();
      } else {
        target_array = &stmt.get_having_exprs();
      }
    } else {
      target_array = &stmt.get_condition_exprs();
    }

    if (OB_SUCC(ret) && OB_NOT_NULL(target_array)) {
      if (OB_FAIL(target_array->push_back(cur_expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformAggrSubquery::check_subquery_having(const ObQueryRefRawExpr &query_ref,
                                                   const ObSelectStmt &subquery,
                                                   bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  // check having expr valid for [not] exists subquery
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < subquery.get_having_expr_size(); ++i) {
    const ObRawExpr *cur_expr = subquery.get_having_exprs().at(i);
    bool bret = false;
    if (OB_ISNULL(cur_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null expr", K(ret));
    } else if (!cur_expr->has_flag(CNT_AGG) || cur_expr->has_flag(CNT_SUB_QUERY)) {
      is_valid = false;
      LOG_TRACE("invalid select item", K(is_valid));
    } else if (OB_FAIL(ObTransformUtils::is_correlated_expr(query_ref.get_exec_params(),
                                                            cur_expr,
                                                            bret))) {
      LOG_WARN("failed to check is correlated expr", K(ret));
    } else if (bret) {
      is_valid = false;
      LOG_TRACE("having condition is correlated", K(ret));
    }
  }
  return ret;
}

int ObTransformAggrSubquery::check_single_set_subquery(const ObSelectStmt &subquery,
                                                       bool &is_valid,
                                                       bool &limit_to_aggr,
                                                       int64_t &limit_value,
                                                       bool check_limit /*= true*/)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  limit_to_aggr = false;
  bool valid_group_by = false;
  bool is_null_value = true;
  bool has_limit = false;
  if (OB_FAIL(is_valid_group_by(subquery, valid_group_by))) {
    LOG_WARN("failed to check is valid group by", K(ret));
  } else if (check_limit && NULL != subquery.get_limit_expr()) {
    has_limit = true;
    ObPhysicalPlanCtx* plan_ctx = NULL;
    ObRawExpr* limit_expr = subquery.get_limit_expr();
    if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->exec_ctx_) ||
        OB_ISNULL(plan_ctx = ctx_->exec_ctx_->get_physical_plan_ctx()) ||
        OB_ISNULL(ctx_->allocator_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if (OB_FAIL(ObTransformUtils::get_expr_int_value(limit_expr,
                                                            &plan_ctx->get_param_store(),
                                                            ctx_->exec_ctx_,
                                                            ctx_->allocator_,
                                                            limit_value,
                                                            is_null_value))) {
      LOG_WARN("failed to get limit int value", K(ret));
    }
  }

  // is_valid is true iff
  if (OB_FAIL(ret)) {
  } else if (valid_group_by) {
    // 1. subquery is a valid group by (with no limit or limit > 0); or
    is_valid = !check_limit || !has_limit || (!is_null_value && limit_value >= 1);
  } else if (check_limit && !subquery.has_group_by()) {
    // 2. subquery is not group by but has limit 1 and is not with tie
    limit_to_aggr = !is_null_value && limit_value == 1 &&
                    !subquery.is_fetch_with_ties();
    is_valid = limit_to_aggr;
    OPT_TRACE("limit 1 can be converted to aggr");
  }
  return ret;
}

int ObTransformAggrSubquery::check_subquery_aggr_item(const ObSelectStmt &subquery,
                                                      bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i< subquery.get_aggr_item_size(); i++) {
    const ObAggFunRawExpr *agg_expr = subquery.get_aggr_item(i);
    if (OB_ISNULL(agg_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("aggr expr is null", K(ret));
    } else {
      const ObItemType type = agg_expr->get_expr_type();
      is_valid = (type == T_FUN_SUM || type == T_FUN_COUNT ||
                  type == T_FUN_MIN || type == T_FUN_MAX);
    }
  }
  return ret;
}

/**
 * @brief ObTransformAggrSubquery::check_count_const_validity
 * @param subquery should not contain const(null) for join-first pullup
 * @param is_valid
 * @return
 */
int ObTransformAggrSubquery::check_count_const_validity(const ObSelectStmt &subquery,
                                                        ObIArray<ObRawExpr *> &constraints,
                                                        bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  ObPhysicalPlanCtx *plan_ctx = NULL;
  ObNotNullContext not_null_ctx;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->allocator_) || OB_ISNULL(ctx_->exec_ctx_) ||
      OB_ISNULL(plan_ctx = ctx_->exec_ctx_->get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i< subquery.get_aggr_item_size(); i++) {
    bool is_count_const = false;
    bool is_not_null = false;
    ObAggFunRawExpr *agg_expr = subquery.get_aggr_item(i);
    if (OB_ISNULL(agg_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("aggr expr is null", K(ret));
    } else if (OB_FAIL(is_count_const_expr(agg_expr, is_count_const))) {
      LOG_WARN("failed to check is count const expr", K(ret));
    } else if (!is_count_const || agg_expr->get_param_count() == 0) {
      // do nothing
    } else if (OB_FAIL(ObTransformUtils::is_expr_not_null(not_null_ctx,
                                                          agg_expr->get_param_expr(0),
                                                          is_not_null,
                                                          &constraints))) {
      LOG_WARN("failed to check expr not null", K(ret));
    } else if (!is_not_null) {
      is_valid = false;
    }
  }
  return ret;
}

int ObTransformAggrSubquery::is_count_const_expr(const ObRawExpr *expr, bool &is_count_const)
{
  int ret = OB_SUCCESS;
  is_count_const = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null expr", K(ret));
  } else if (T_FUN_COUNT != expr->get_expr_type()) {
    // do nothing
  } else if (0 == expr->get_param_count()) {
    is_count_const = true;
    LOG_TRACE("expr is count(*)", K(*expr));
  } else if (OB_ISNULL(expr->get_param_expr(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null expr", K(ret));
  } else if (!expr->get_param_expr(0)->has_flag(CNT_COLUMN) 
            && !expr->get_param_expr(0)->has_flag(CNT_SUB_QUERY)) {
    is_count_const = true;
    LOG_TRACE("expr is count(const)", K(*expr));
  }
  return ret;
}

int ObTransformAggrSubquery::replace_count_const(ObAggFunRawExpr *agg_expr, ObRawExpr *not_null_expr)
{
  int ret = OB_SUCCESS;
  bool is_count_const = false;
  if (OB_ISNULL(agg_expr) || OB_ISNULL(not_null_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null expr", K(ret), K(agg_expr), K(not_null_expr));
  } else if (OB_FAIL(is_count_const_expr(agg_expr, is_count_const))) {
    LOG_WARN("failed to check is count const expr", K(ret));
  } else if (!is_count_const) {
    // do nothing
  } else if (0 == agg_expr->get_param_count()) {
    if (OB_FAIL(agg_expr->add_real_param_expr(not_null_expr))) {
      LOG_WARN("failed to add real param expr", K(ret));
    }
  } else {
    agg_expr->get_param_expr(0) = not_null_expr;
  }
  return ret;
}


/**
 * @brief modify_aggr_param_expr_for_outer_join
 * rules for modifying aggr param expr:
 *  1. when pullup strategy is determined to use INNER JOIN, aggr param expr in subquery should NOT be modified
 *  2. when aggr param expr has null-propagate property, it should NOT be modified
 *  3. when aggr item matches COUNT(CONST) pattern, the CONST param expr should be modified to a not-null-column-expr
 *  4. In the remaining cases, aggr param expr should be modified as
 *    "CASE WHEN not-null-column-expr is not null THEN original-agg ELSE null END"
 */
int ObTransformAggrSubquery::modify_aggr_param_expr_for_outer_join(TransformParam& trans_param)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *subquery = NULL;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_)
      || OB_ISNULL(trans_param.ja_query_ref_)
      || OB_ISNULL(subquery = trans_param.ja_query_ref_->get_ref_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret), K(ctx_), K(trans_param), K(subquery));
  }

  if (OB_SUCC(ret) && use_outer_join(trans_param.pullup_flag_)) {
    // collect related columns for null propagate check
    ObSEArray<const ObRawExpr*, 4> columns;
    ObSEArray<ObRawExpr*, 4> tmp;
    if (OB_FAIL(subquery->get_column_exprs(tmp))) {
      LOG_WARN("failed to get column exprs", K(ret));
    } else if (OB_FAIL(append(columns, tmp))) {
      LOG_WARN("failed to convert column array", K(ret));
    }

    // check && modify param expr for each aggr item
    for (int64_t i = 0; OB_SUCC(ret) && i < subquery->get_aggr_item_size(); ++i) {
      ObRawExpr* aggr = subquery->get_aggr_item(i);
      // check null-propagate && count const
      bool is_null_propagate = false;
      bool is_count_const = false;
      if (OB_ISNULL(aggr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("aggregation expr is invalid", K(ret));
      } else if (OB_FAIL(is_count_const_expr(aggr, is_count_const))) {
        LOG_WARN("failed to check is count const expr", K(ret));
      } else if (0 == aggr->get_param_count()) {
        // do nothing
      } else if (OB_FAIL(ObTransformUtils::is_null_propagate_expr(aggr->get_param_expr(0), columns, is_null_propagate))) {
        LOG_WARN("failed to check is null propagate expr", K(ret));
      }

      // modify aggr param expr
      if (is_null_propagate) {
        // do nothing
      } else if (is_count_const) {
        if (OB_FAIL(replace_count_const(static_cast<ObAggFunRawExpr*>(aggr), trans_param.not_null_expr_))) {
          LOG_WARN("failed to replace count const expr", K(ret));
        }
      } else {
        // replace aggr param expr with case when expr
        ObRawExpr* case_when_expr = NULL;
        ObRawExpr* not_null_column = trans_param.not_null_expr_;
        ObRawExpr* then_expr = aggr->get_param_expr(0);
        ObRawExpr* else_expr = NULL;
        if (OB_FAIL(ObRawExprUtils::build_null_expr(*ctx_->expr_factory_, else_expr))) {
          LOG_WARN("failed to create const null expr", K(ret));
        } else if (OB_FAIL(ObRawExprUtils::try_add_cast_expr_above(ctx_->expr_factory_, ctx_->session_info_, *else_expr, then_expr->get_result_type(), else_expr))) {
          LOG_WARN("failed to modify result type of null expr", K(ret));
        } else if (OB_ISNULL(not_null_column)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("not null column is invalid", K(ret));
        } else if (OB_FAIL(ObTransformUtils::build_case_when_expr(*subquery, not_null_column, then_expr, else_expr, case_when_expr, ctx_))) {
          LOG_WARN("failed to build case when expr", K(ret));
        } else if (OB_FAIL(ObTransformUtils::replace_expr(then_expr, case_when_expr, aggr))) {
          LOG_WARN("failed to build case when expr", K(ret));
        }
      }
    }
  }
  return ret;
}

/**
 * @brief check_can_use_outer_join
 * check if can use outer join for join fisrt ja rewrite
 * following case is not valid:
 * 1. child_stmt has semi join or subquery in where condition
 * 2. aggr item's parameter is not null propagate && there is no not null column found
 */
int ObTransformAggrSubquery::check_can_use_outer_join(TransformParam &param,
                                                      bool &is_valid)
{
  int ret = OB_SUCCESS;
  ObQueryRefRawExpr *query_ref = param.ja_query_ref_;
  ObRawExpr *&not_null_expr = param.not_null_expr_;
  ObSelectStmt *stmt = NULL;
  bool has_nested = false;
  bool limit_to_aggr = false;
  if (OB_ISNULL(param.ja_query_ref_) ||
      OB_ISNULL(stmt = param.ja_query_ref_->get_ref_stmt()) ||
      OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid transform param", K(ret));
  } else if (FALSE_IT(limit_to_aggr = !stmt->has_group_by() &&
                                      !param.limit_for_exists_ &&
                                      param.limit_value_ == 1)) {
    //never reach
  } else if (OB_FAIL(ObTransformUtils::has_nested_subquery(query_ref, has_nested))) {
    LOG_WARN("failed to check has nested subquery", K(ret));
  } else {
    is_valid = stmt->get_semi_info_size() == 0 && !has_nested;
    if (!is_valid) {
      OPT_TRACE("subquery has semi/anti join or has nested subquery, can not transform");
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < stmt->get_condition_size(); ++i) {
    if (OB_ISNULL(stmt->get_condition_expr(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("condition expr is null", K(ret));
    } else {
      is_valid = !stmt->get_condition_expr(i)->has_flag(CNT_SUB_QUERY);
      if (!is_valid) {
        OPT_TRACE("subquery`s where condition contain subquery, can not transform");
      }
    }
  }
  // reject outer join rewrite if any agg item satisfy: agg-param-expr not null-propagate && find no not-null-column
  if (OB_SUCC(ret) && is_valid) {
    if (not_null_expr != NULL) {
      // do nothing
    } else if (OB_FAIL(ObTransformUtils::find_not_null_expr(*stmt, not_null_expr, is_valid, ctx_))) {
      LOG_WARN("failed to find not null expr", K(ret));
    } else if (!is_valid) {
      // collect related columns
      ObSEArray<const ObRawExpr*, 4> columns;
      if (OB_SUCC(ret) && is_valid) {
        ObSEArray<ObRawExpr*, 4> tmp;
        if (OB_FAIL(stmt->get_column_exprs(tmp))) {
          LOG_WARN("failed to get column exprs", K(ret));
        } else if (OB_FAIL(append(columns, tmp))) {
          LOG_WARN("failed to convert column array", K(ret));
        }
      }
      // check null-propagate property for all agg-param-expr
      bool all_agg_param_null_propagate = true;
      for (int64_t i = 0; OB_SUCC(ret) && all_agg_param_null_propagate && i < stmt->get_aggr_item_size(); ++i) {
        ObRawExpr* aggr = stmt->get_aggr_item(i);
        bool is_null_propagate = false;
        if (OB_ISNULL(aggr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("aggregation expr is invalid", K(ret));
        } else if (0 == aggr->get_param_count()) {
          all_agg_param_null_propagate = false;
        } else if (OB_FAIL(ObTransformUtils::is_null_propagate_expr(aggr->get_param_expr(0), columns, is_null_propagate))) {
          LOG_WARN("failed to check is null propagate expr", K(ret));
        } else {
          all_agg_param_null_propagate = is_null_propagate;
        }
      }
      if (OB_SUCC(ret) && all_agg_param_null_propagate && limit_to_aggr) {
        if (OB_UNLIKELY(stmt->get_select_item_size() != 1)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpeced select item size", K(ret));
        } else if (OB_FAIL(ObTransformUtils::is_null_propagate_expr(stmt->get_select_item(0).expr_,
                                                                    columns,
                                                                    all_agg_param_null_propagate))) {
          LOG_WARN("failed to check is null propagate expr", K(ret));
        }
      }
      is_valid = all_agg_param_null_propagate;
    }
  }
  return ret;
}

int ObTransformAggrSubquery::fill_query_refs(ObDMLStmt *stmt,
                                             ObRawExpr *expr,
                                             TransformParam &param)
{
  int ret = OB_SUCCESS;
  param.query_refs_.reuse();
  if (OB_ISNULL(stmt) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params are invalid" ,K(ret), K(stmt), K(expr));
  } else if (expr->has_flag(CNT_ALIAS)) {
    if (stmt->is_update_stmt()) {
      if (OB_FAIL(static_cast<ObUpdateStmt*>(stmt)->get_vector_assign_values(
                         param.ja_query_ref_,
                         param.query_refs_))) {
        LOG_WARN("failed to get vector assign values", K(ret));
      }
    } else if (stmt->is_select_stmt()) {
      // select stmt can also contain alias ref exprs
      ObSEArray<ObRawExpr *, 8> sel_exprs;
      if (OB_FAIL(static_cast<ObSelectStmt *>(stmt)->get_select_exprs(sel_exprs))) {
        LOG_WARN("failed to get select exprs", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < sel_exprs.count(); ++i) {
          ObRawExpr *sel_expr = NULL;
          if (OB_ISNULL(sel_expr = sel_exprs.at(i))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null expr", K(ret));
          } else if (sel_expr->has_flag(CNT_ALIAS)) {
            if (OB_FAIL(param.query_refs_.push_back(sel_expr))) {
              LOG_WARN("failed to push back alias ref expr", K(ret));
            }
          }
        }
      }
    }
  } else if (OB_FAIL(param.query_refs_.push_back(param.ja_query_ref_))) {
    LOG_WARN("failed to push back query refs", K(ret));
  }
  return ret;
}

// 1. scalar group by
// 2. group by const expr
int ObTransformAggrSubquery::is_valid_group_by(const ObSelectStmt &subquery, bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  if (subquery.is_scala_group_by()) {
    is_valid = true;
  } else if (subquery.get_group_expr_size() > 0 &&
             subquery.get_rollup_expr_size() == 0) {
    // check is group by const
    is_valid = true;
    ObSEArray<ObRawExpr *, 4> const_exprs;
    if (OB_FAIL(ObOptimizerUtil::compute_const_exprs(subquery.get_condition_exprs(),
                                                     const_exprs))) {
      LOG_WARN("failed to compute const exprs", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < subquery.get_group_expr_size(); ++i) {
      if (subquery.get_group_exprs().at(i)->is_const_expr()) {
        // do nothing
      } else if (ObOptimizerUtil::find_item(const_exprs, subquery.get_group_exprs().at(i))) {
        // do nothing
      } else {
        is_valid = false;
      }
    }
  }
  LOG_TRACE("check single set query", K(is_valid), K(subquery.is_scala_group_by()));
  return ret;
}

bool ObTransformAggrSubquery::check_is_filter(const ObDMLStmt &stmt, const ObRawExpr *expr)
{
  bool bret = false;
  bret = ObOptimizerUtil::find_item(stmt.get_condition_exprs(), expr);
  if (!bret && stmt.is_select_stmt()) {
    bret = ObOptimizerUtil::find_item(static_cast<const ObSelectStmt&>(stmt).get_having_exprs(),
                                      expr);
  }
  return bret;
}

int ObTransformAggrSubquery::get_filters(ObDMLStmt &stmt,
                                         ObRawExpr *expr,
                                         ObIArray<ObRawExpr *> &filters)
{
  int ret = OB_SUCCESS;
  if (ObOptimizerUtil::find_item(stmt.get_condition_exprs(), expr)) {
    // is where
    if (OB_FAIL(filters.assign(stmt.get_condition_exprs()))) {
      LOG_WARN("failed to assign condition exprs", K(ret));
    }
  } else if (stmt.is_select_stmt()) {
    ObSelectStmt &sel_stmt = static_cast<ObSelectStmt &>(stmt);
    if (ObOptimizerUtil::find_item(sel_stmt.get_group_exprs(), expr) ||
        ObOptimizerUtil::find_item(sel_stmt.get_aggr_items(), expr) ||
        ObOptimizerUtil::find_item(sel_stmt.get_having_exprs(), expr)) {
      if (OB_FAIL(filters.assign(sel_stmt.get_having_exprs()))) {
        LOG_WARN("failed to find items", K(ret));
      }
    }
  }
  return ret;
}

/**
 * 获取不需要改写的子查询
 * 目前暂时不支持join reorder, select item中的子查询如果做了JA改写可能会出现无法走nest loop 条件下推的问题
 * 因此如果select item中子查询的相关连接条件中内表的列上有索引可用, 就不做JA改写了。
 */
int ObTransformAggrSubquery::extract_no_rewrite_select_exprs(ObDMLStmt *&stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (stmt->is_select_stmt() && stmt->get_subquery_expr_size() > 0) {
    ObSelectStmt *sel_stmt = static_cast<ObSelectStmt *>(stmt);
    for (int64_t i = 0; OB_SUCC(ret) && i < sel_stmt->get_select_item_size(); ++i) {
      if (OB_FAIL(extract_no_rewrite_expr(sel_stmt->get_select_item(i).expr_))) {
        LOG_WARN("failed to extract no rewrite expr", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformAggrSubquery::extract_no_rewrite_expr(ObRawExpr *expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr) || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));   
  } else if (expr->is_query_ref_expr()) {
    ObSelectStmt *subquery = static_cast<ObQueryRefRawExpr *>(expr)->get_ref_stmt();
    bool is_match = false;
    if (OB_ISNULL(subquery)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (subquery->get_stmt_hint().has_enable_hint(T_UNNEST) ||
               subquery->get_stmt_hint().has_enable_hint(T_AGGR_FIRST_UNNEST) ||
               subquery->get_stmt_hint().has_enable_hint(T_JOIN_FIRST_UNNEST)) {
      //do nothing
    } else if (OB_FAIL(ObTransformUtils::check_subquery_match_index(ctx_,
                                                                    static_cast<ObQueryRefRawExpr *>(expr),
                                                                    subquery,
                                                                    is_match))) {
      LOG_WARN("failed to check subquery match index", K(ret));
    } else if (!is_match) {
      //do nothing
    } else if (OB_FAIL(add_var_to_array_no_dup(no_rewrite_exprs_, expr))) {
      LOG_WARN("failed to add var to array no dup", K(ret));
    }
  } else if (expr->has_flag(CNT_SUB_QUERY)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(extract_no_rewrite_expr(expr->get_param_expr(i)))) {
        LOG_WARN("failed to extract no rewrite expr", K(ret));
      }
    }
  }
  return ret;
}
ObHint* ObTransformAggrSubquery::get_sub_unnest_hint(ObSelectStmt &subquery,
                                                     int64_t pullup_strategy)
{
  ObHint *myhint = NULL;
  if (aggr_first(pullup_strategy)) {
    myhint = subquery.get_stmt_hint().get_normal_hint(T_AGGR_FIRST_UNNEST);
  } else if (join_first(pullup_strategy)) {
    myhint = subquery.get_stmt_hint().get_normal_hint(T_JOIN_FIRST_UNNEST);
  }
  return myhint;
}

ObItemType ObTransformAggrSubquery::get_unnest_strategy(int64_t pullup_strategy) {
  ObItemType hint = T_INVALID;
  if (aggr_first(pullup_strategy)) {
    hint = T_AGGR_FIRST_UNNEST;
  } else if (join_first(pullup_strategy)) {
    hint = T_JOIN_FIRST_UNNEST;
  }
  return hint;
}
int ObTransformAggrSubquery::check_hint_allowed_unnest(ObDMLStmt &stmt,
                                                       ObSelectStmt &subquery,
                                                       const int64_t hint_loc,
                                                       const int64_t pullup_strategy,
                                                       bool &allowed)
{
  int ret = OB_SUCCESS;
  allowed = true;
  const ObQueryHint *query_hint = stmt.get_stmt_hint().query_hint_;
  const ObHint *unnest_hint = subquery.get_stmt_hint().get_normal_hint(T_UNNEST);
  const ObHint *subhint = get_sub_unnest_hint(subquery, pullup_strategy);
  bool is_enable = false;
  bool is_disable = false;
  bool is_unnest_enable = false;
  bool is_unnest_disable = false;
  if (NULL != unnest_hint) {
    is_unnest_enable = unnest_hint->is_enable_hint();
    is_unnest_disable = unnest_hint->is_disable_hint();
  }
  if (NULL != subhint) {
    is_enable = subhint->is_enable_hint();
    is_disable = subhint->is_disable_hint();
  }
  const ObHint *no_rewrite1 = stmt.get_stmt_hint().get_no_rewrite_hint();
  const ObHint *no_rewrite2 = subquery.get_stmt_hint().get_no_rewrite_hint();
  if (OB_ISNULL(ctx_) || OB_ISNULL(query_hint)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(ctx_), K(query_hint));
  } else if (query_hint->has_outline_data()) {
    // outline data allowed merge
    allowed = query_hint->is_valid_outline_transform(hint_loc, subhint);
  } else {
    if (is_enable || (!is_disable && is_unnest_enable)) {
      // enable hint
      allowed = true;
    } else if (NULL != no_rewrite1 || NULL != no_rewrite2 || is_disable || is_unnest_disable) {
      // add disable transform hint here
      allowed = false;
      if (OB_FAIL(ctx_->add_used_trans_hint(no_rewrite1))) {
        LOG_WARN("failed to add used trans hint", K(ret));
      } else if (OB_FAIL(ctx_->add_used_trans_hint(no_rewrite2))) {
        LOG_WARN("failed to add used trans hint", K(ret));
      } else if (is_disable && OB_FAIL(ctx_->add_used_trans_hint(subhint))) {
        LOG_WARN("failed to add used trans hint", K(ret));
      } else if (is_unnest_disable && OB_FAIL(ctx_->add_used_trans_hint(unnest_hint))) {
        LOG_WARN("failed to add used trans hint", K(ret));
      }
    } else { /* default enable */ }
  }
  return ret;
}

int ObTransformAggrSubquery::add_trans_stmt_info(ObSelectStmt &subquery, int64_t flag)
{
  int ret = OB_SUCCESS;
  TransStmtInfo info;
  info.pullup_strategy_ = flag;
  info.unnest_ = get_sub_unnest_hint(subquery, flag);
  if (NULL == info.unnest_) {
    info.unnest_ = get_hint(subquery.get_stmt_hint());
  }
  if (OB_FAIL(subquery.get_qb_name(info.qb_name_))) {
    LOG_WARN("failed to get qb name", K(ret));
  } else if (OB_FAIL(trans_stmt_infos_.push_back(info))) {
    LOG_WARN("failed to push back trans stmt info", K(ret), K(info));
  } else {
    ctx_->trans_list_loc_++;
  }
  return ret;
}

/**
 *    select B.c1 from B where A.c2 = B.c2 limit 1;
 * => select min(B.c1) from B where A.c2 = B.c2;
 *
 *    MySQL
 *    select B.c1 from B where A.c2 = B.c2 order by B.c1 desc limit 1;
 * => select max(B.c1) from B where A.c2 = B.c2;
 *
 *    Oracle
 *    select B.c1 from B where A.c2 = B.c2 order by B.c1 asc fetch next 1 row only;
 * => select min(B.c1) from B where A.c2 = B.c2;
 */
int ObTransformAggrSubquery::convert_limit_as_aggr(ObSelectStmt *subquery,
                                                   TransformParam &trans_param)
{
  int ret = OB_SUCCESS;
  ObRawExpr* expr = NULL;
  if (OB_ISNULL(subquery)
      || OB_ISNULL(ctx_->expr_factory_)
      || OB_ISNULL(ctx_->session_info_)
      || OB_UNLIKELY(!subquery->has_group_by() && 1 != trans_param.limit_value_)
      || OB_UNLIKELY(!subquery->has_group_by() && 1 != subquery->get_select_item_size())
      || OB_ISNULL(expr = subquery->get_select_item(0).expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected param", K(ret), KPC(subquery));
  } else {
    // If there is already aggr, do not add new aggr
    // If there is limit 1, add MIN to a column
    bool already_has_group = subquery->has_group_by();
    if (!already_has_group) {
      ObAggFunRawExpr* aggr_expr = NULL;
      ObItemType aggr_type = T_FUN_MIN;
      if (subquery->has_order_by()) {
        OrderItem& first_order = subquery->get_order_item(0);
        if (NULLS_LAST_ASC == first_order.order_type_) {
          aggr_type = T_FUN_MIN;
        } else if (NULLS_LAST_DESC == first_order.order_type_) {
          aggr_type = T_FUN_MAX;
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected order type", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ObRawExprUtils::build_common_aggr_expr(*ctx_->expr_factory_,
                                                                ctx_->session_info_,
                                                                aggr_type,
                                                                expr,
                                                                aggr_expr))) {
        LOG_WARN("failed to build aggr expr", K(ret));
      } else if (OB_FAIL(subquery->add_agg_item(*aggr_expr))) {
        LOG_WARN("failed to add aggr expr", K(ret));
      } else {
        subquery->get_select_item(0).expr_ = aggr_expr;
      }
    }

    // Add constraints and remove limit
    if (OB_SUCC(ret)) {
      ObConstRawExpr* one_expr = NULL;
      ObRawExpr* cmp_expr = NULL;
      ObRawExpr* limit_expr = NULL;
      if (OB_ISNULL(limit_expr = subquery->get_limit_expr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*ctx_->expr_factory_,
                                                              ObIntType, 1, one_expr))) {
        LOG_WARN("fail to build int expr", K(ret));
      } else if (already_has_group &&
        OB_FAIL(ObRawExprUtils::create_double_op_expr(*ctx_->expr_factory_,
                                                      ctx_->session_info_,
                                                      T_OP_GE,
                                                      cmp_expr,
                                                      limit_expr,
                                                      one_expr))) {
        LOG_WARN("fail to build cmp expr");
      } else if (!already_has_group &&
        OB_FAIL(ObRawExprUtils::create_double_op_expr(*ctx_->expr_factory_,
                                                      ctx_->session_info_,
                                                      T_OP_EQ,
                                                      cmp_expr,
                                                      limit_expr,
                                                      one_expr))) {
        LOG_WARN("fail to build cmp expr");
      } else if (
        OB_FAIL(ObTransformUtils::add_param_bool_constraint(ctx_,
                                                            cmp_expr,
                                                            true/*is_true*/))) {
        LOG_WARN("failed to add constraints", K(ret));
      } else if (OB_FAIL(append(ctx_->equal_param_constraints_, trans_param.equal_param_info_))) {
        LOG_WARN("append equal param info failed", K(ret));
      } else {
        subquery->set_limit_offset(NULL, NULL);
      }
    }
  }
  return ret;
}
