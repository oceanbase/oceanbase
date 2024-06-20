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
#include "ob_transform_where_subquery_pullup.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "sql/ob_sql_context.h"
#include "common/ob_smart_call.h"
#include "sql/resolver/dml/ob_update_stmt.h"

namespace oceanbase
{
using namespace common;
using share::schema::ObTableSchema;

namespace sql
{
//遍历每一个cond_expr进行如下处理:
//  1. 递归处理含subquery的表达式(eliminate_subquery()):
//     1) 如果是[NOT] EXISTS:
//         判断是否可以消除subquery
//           a. 如果可以，则将subquery消除
//           b. 如果不可以：
//              * 消除select list -->常量select 1
//              * 消除group by
//              * 消除order by
//     2) 如果是ANY/ALL：
//         a. 消除group by
//         b. 非相关any子查询如果select item为const item，则添加limit 1
//            eg: select * from t1 where c1 in (select 1 from t2);
//                ==> select * from t1 where c1 in (select 1 from t2 limit 1);
//         c. 消除相关子查询中的distinct
//  2. 判断是否可以pull up
//  3. pull up subquery
//     a. IN (SELECT ...) --> semi join
//     b. NOT IN (SELECT ...) --> anti join
int ObWhereSubQueryPullup::transform_one_stmt(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                              ObDMLStmt *&stmt,
                                              bool &trans_happened)
{
  int ret = OB_SUCCESS;
  UNUSED(parent_stmts);
  bool is_anyall_happened = false;
  bool is_single_set_happened = false;
  trans_happened = false;
  ObSEArray<ObSelectStmt*, 4> unnest_stmts;
  if (OB_FAIL(transform_anyall_query(stmt, unnest_stmts, is_anyall_happened))) {
    LOG_WARN("failed to transform any all query", K(ret));
  } else if (OB_FAIL(transform_single_set_query(stmt, unnest_stmts, is_single_set_happened))) {
    LOG_WARN("failed to transform single set query", K(ret));
  } else {
    trans_happened = is_anyall_happened | is_single_set_happened;
    LOG_TRACE("succeed to transformer where subquery",
              K(is_anyall_happened),
              K(is_single_set_happened),
              K(trans_happened),
              K(get_trans_happened()));
    if (trans_happened && OB_FAIL(add_transform_hint(*stmt, &unnest_stmts))) {
      LOG_WARN("failed to add transform hint", K(ret));
    }
  }
  return ret;
}

int ObWhereSubQueryPullup::transform_one_stmt_with_outline(ObIArray<ObParentDMLStmt> &parent_stmts,
                                                           ObDMLStmt *&stmt,
                                                           bool &trans_happened)
{
  int ret = OB_SUCCESS;
  UNUSED(parent_stmts);
  trans_happened = false;
  bool is_happened = false;
  ObSEArray<ObSelectStmt*, 4> unnest_stmts;
  do {
    is_happened = false;
    if (OB_FAIL(transform_anyall_query(stmt, unnest_stmts, is_happened))) {
      LOG_WARN("failed to transform any all query", K(ret));
    } else if (!is_happened && OB_FAIL(transform_single_set_query(stmt, unnest_stmts, is_happened))) {
      LOG_WARN("failed to transform single set query", K(ret));
    } else if (!is_happened) {
      LOG_TRACE("can not do subquery pullup with outline", K(ctx_->src_qb_name_));
    } else {
      ++ctx_->trans_list_loc_;
      trans_happened = true;
      LOG_TRACE("succeed to do subquery pullup with outline", K(ctx_->src_qb_name_));
    }
  } while (OB_SUCC(ret) && is_happened);
  if (OB_SUCC(ret) && trans_happened && OB_FAIL(add_transform_hint(*stmt, &unnest_stmts))) {
    LOG_WARN("failed to add transform hint", K(ret));
  }
  return ret;
}

int ObWhereSubQueryPullup::transform_anyall_query(ObDMLStmt *stmt,
                                                  ObIArray<ObSelectStmt*> &unnest_stmts,
                                                  bool &trans_happened)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 16> conditions;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret), K(stmt));
  } else if (!stmt->has_subquery()) {
    //do nothing
  } else if (OB_FAIL(conditions.assign(stmt->get_condition_exprs()))) {
    LOG_WARN("failed to assign a new condition exprs", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < conditions.count(); ++i) {
      bool is_happened = false;
      if (OB_FAIL(transform_one_expr(stmt, conditions.at(i), unnest_stmts, is_happened))) {
        LOG_WARN("failed to transform one subquery expr", K(ret));
      } else {
        trans_happened |= is_happened;
      }
    }
  }
  return ret;
}

int ObWhereSubQueryPullup::transform_one_expr(ObDMLStmt *stmt,
                                              ObRawExpr *expr,
                                              ObIArray<ObSelectStmt*> &unnest_stmts,
                                              bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  TransformParam trans_param;
  bool is_hsfu = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL pointer error", K(ret));
  //bug:
  } else if (expr->has_flag(CNT_ROWNUM)) {
    /*do nothing */
  } else if (OB_FAIL(stmt->is_hierarchical_for_update(is_hsfu))) {
    LOG_WARN("failed to check hierarchical for update", K(ret), KPC(stmt));
  } else if (is_hsfu) {
    // do nothing
  } else if (OB_FAIL(gather_transform_params(stmt, expr, trans_param))) {
    LOG_WARN("failed to check can be pulled up ", K(expr), K(stmt), K(ret));
  } else if (!trans_param.can_be_transform_) {
    // do nothing
  } else if (OB_FAIL(do_transform_pullup_subquery(stmt, expr, trans_param, trans_happened))) {
    LOG_WARN("failed to pull up subquery", K(ret));
  } else if (trans_happened && OB_FAIL(unnest_stmts.push_back(trans_param.subquery_))) {
    LOG_WARN("failed to push back", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObWhereSubQueryPullup::gather_transform_params(ObDMLStmt *stmt,
                                                   ObRawExpr *expr,
                                                   TransformParam &trans_param)
{
  int ret = OB_SUCCESS;
  trans_param.can_be_transform_ = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL pointer error", K(stmt), K(expr), K(ret));
  } else if (T_OP_EXISTS == expr->get_expr_type() || T_OP_NOT_EXISTS == expr->get_expr_type()) {
    if (OB_ISNULL(expr->get_param_expr(0)) ||
        OB_UNLIKELY(!expr->get_param_expr(0)->is_query_ref_expr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid [not] exist predicate", K(*expr), K(ret));
    } else {
      trans_param.can_be_transform_ = true;
      trans_param.subquery_expr_ = static_cast<ObQueryRefRawExpr *>(expr->get_param_expr(0));
      trans_param.subquery_ = trans_param.subquery_expr_->get_ref_stmt();
      trans_param.is_correlated_ = trans_param.subquery_expr_->has_exec_param();
      trans_param.op_ = static_cast<ObOpRawExpr*>(expr);
    }
  } else if (expr->has_flag(IS_WITH_ALL) || expr->has_flag(IS_WITH_ANY)) {
    if (OB_ISNULL(expr->get_param_expr(0)) || OB_ISNULL(expr->get_param_expr(1)) ||
        OB_UNLIKELY(!expr->get_param_expr(1)->is_query_ref_expr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid anyall predicate", K(*expr), K(ret));
    } else if (OB_UNLIKELY(expr->get_param_expr(0)->has_flag(CNT_SUB_QUERY))) {
      // subquery in subquery, subquery = all subquery do not transform
    } else {
      trans_param.can_be_transform_ = true;
      trans_param.left_hand_ = expr->get_param_expr(0);
      trans_param.subquery_expr_ = static_cast<ObQueryRefRawExpr *>(expr->get_param_expr(1));
      trans_param.subquery_ = trans_param.subquery_expr_->get_ref_stmt();
      trans_param.is_correlated_ = trans_param.subquery_expr_->has_exec_param();
      trans_param.op_ = static_cast<ObOpRawExpr*>(expr);
    }
  }
  if (OB_SUCC(ret) && trans_param.can_be_transform_) {
    OPT_TRACE("try to pullup subquery expr:", expr);
    if (OB_FAIL(check_transform_validity(stmt, expr, trans_param))) {
      LOG_WARN("failed to check can be pulled up ", K(*expr), K(ret));
    } else {
      LOG_TRACE("finish to check where subquery pull up", K(trans_param), K(ret));
    }
  }
  return ret;
}

/**
 * @brief ObWhereSubQueryPullup::can_be_unnested
 * check subquery can merge into stmt
 */
int ObWhereSubQueryPullup::can_be_unnested(const ObItemType op_type,
                                           const ObSelectStmt *subquery,
                                           bool &can_be,
                                           bool &need_add_limit_constraint)
{
  int ret = OB_SUCCESS;
  bool has_rownum = false;
  bool has_limit = false;
  can_be = true;
  need_add_limit_constraint = false;
  if (OB_ISNULL(subquery)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(subquery));
  } else if (subquery->has_distinct()
             || subquery->is_hierarchical_query()
             || subquery->has_group_by()
             || subquery->has_window_function()
             || subquery->is_set_stmt()) {
    can_be = false;
  } else if (OB_FAIL(subquery->has_rownum(has_rownum))) {
    LOG_WARN("failed to check has rownum expr", K(ret));
  } else if (has_rownum) {
    can_be = false;
  } else if (OB_FAIL(check_limit(op_type, subquery, has_limit, need_add_limit_constraint))) {
    LOG_WARN("failed to check subquery has unremovable limit", K(ret));
  } else if (has_limit) {
    can_be = false;
  }
  return ret;
}

int ObWhereSubQueryPullup::check_limit(const ObItemType op_type,
                                       const ObSelectStmt *subquery,
                                       bool &has_limit,
                                       bool &need_add_limit_constraint) const
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = NULL;
  bool is_const_select = false;
  has_limit = false;
  need_add_limit_constraint = false;
  if (OB_ISNULL(subquery) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->exec_ctx_) ||
      OB_ISNULL(plan_ctx = ctx_->exec_ctx_->get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(subquery), K(ctx_), K(ctx_->exec_ctx_), K(plan_ctx));
  } else if (OB_FAIL(check_const_select(*subquery, is_const_select))) {
    LOG_WARN("failed to check const select", K(ret));
  } else if (op_type != T_OP_EXISTS && op_type != T_OP_NOT_EXISTS && !is_const_select) {
    has_limit = subquery->has_limit();
  } else if (!subquery->has_limit() ||
             subquery->get_offset_expr() != NULL ||
             subquery->get_limit_percent_expr() != NULL) {
    has_limit = subquery->has_limit();
  } else {
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
      has_limit = false;
      //Just in case different parameters hit same plan, firstly we need add const param constraint
      need_add_limit_constraint = true;
    } else {
      has_limit = true;
    }
  }
  return ret;
}


/*@brief check transform validity
*1.basic validity
*2.check need create spj for direct correlated subquery
*/
int ObWhereSubQueryPullup::check_transform_validity(ObDMLStmt *stmt,
                                                    const ObRawExpr* expr,
                                                    TransformParam &trans_param)
{
  int ret = OB_SUCCESS;
  bool can_unnest = false;
  if (OB_ISNULL(expr) || OB_ISNULL(stmt) || OB_ISNULL(trans_param.subquery_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL pointer error", K(expr), K(stmt), K(trans_param.subquery_), K(ret));
  } else if (OB_FAIL(check_basic_validity(stmt, expr, trans_param))) {//check 1
    LOG_WARN("check basic valid failed", K(ret));
  } else if (!trans_param.can_be_transform_) {
    /*do nothing*/
    OPT_TRACE("subquery can not pullup");
  } else if (!trans_param.is_correlated_) {//check 2
    /*do nothing*/
  } else if (OB_FAIL(can_be_unnested(expr->get_expr_type(),
                                     trans_param.subquery_,
                                     can_unnest,
                                     trans_param.need_add_limit_constraint_))) {
    LOG_WARN("failed to check subquery can be unnested", K(ret));
  } else if (can_unnest) {
    /*do nothing*/
  } else if (OB_FAIL(ObTransformUtils::check_correlated_exprs_can_pullup(
                       trans_param.subquery_expr_->get_exec_params(),
                       *trans_param.subquery_,
                       trans_param.can_be_transform_))) {
    LOG_WARN("failed to check can unnest with spj", K(ret));
  } else if (!trans_param.can_be_transform_) {
    /*do nothing*/
    OPT_TRACE("subquery can not unnest, but subquery has none pullup correlated expr, will not transform");
  } else {
    trans_param.need_create_spj_ = true;
  }
  return ret;
}

/***@brief check basic validity

**/
/**
 * @brief ObWhereSubQueryPullup::check_basic_validity
 * 1.has no unnest hint do not transform
 * 2.stmt/subquery is select from dual, do not transform
 * 3.check set-op: any/all direct correlated do not transform
 * 4.expr is exists(or not exists) and subquery is not direct correlated
 * 5.condition of joined table/semi info in subquery is correlated
 * 6.where condition in subquery contain correlated subquery do not transform
 * 7.select item contains subquery
 * 8.generated table in subquery is correlated:
 *   eg:select * from t1
 *      where c2 in (select c2 from (select c2 from t2 where t2.c3 = t1.c3))
 */
int ObWhereSubQueryPullup::check_basic_validity(ObDMLStmt *stmt,
                                                const ObRawExpr* expr,
                                                TransformParam &trans_param)
{
  int ret = OB_SUCCESS;
  bool &is_valid = trans_param.can_be_transform_;
  ObSelectStmt *subquery = trans_param.subquery_;
  if (OB_ISNULL(expr) || OB_ISNULL(stmt) || OB_ISNULL(subquery)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL pointer error", K(ret), K(expr), K(stmt), K(subquery));
  } else if (!is_valid) {
    // do nothing
  } else if (OB_FAIL(check_hint_allowed_unnest(*stmt, *subquery, is_valid))) {
    LOG_WARN("failed to check hint", K(ret));
  } else if (!is_valid) {
    // do nothing
    OPT_TRACE("hint reject transform");
  } else if (stmt->get_table_items().empty()) {
    // do not transform if the main query is from dual, eg:
    // select 1 from dual where 1 >any (select c1 from t1);
    is_valid = false;
    OPT_TRACE("stmt do not have table item");
  } else if ((T_OP_EXISTS == expr->get_expr_type() || T_OP_NOT_EXISTS == expr->get_expr_type())
             && !trans_param.is_correlated_) {
    is_valid = false;
    OPT_TRACE("not correlated exists subquery no need pullup");
  } else if (subquery->is_set_stmt()) {
    if (!trans_param.is_correlated_) {
      is_valid = true;
    } else {
      ObIArray<ObSelectStmt*> &set_queries = subquery->get_set_query();
      for (int64_t i = 0; is_valid && OB_SUCC(ret) && i < set_queries.count(); ++i) {
        if (OB_FAIL(check_subquery_validity(trans_param.subquery_expr_,
                                            set_queries.at(i),
                                            trans_param.is_correlated_, 
                                            is_valid))) {
          LOG_WARN("failed to check subquery valid", K(ret));
        }
      }
    }
  } else if (OB_FAIL(check_subquery_validity(trans_param.subquery_expr_,
                                             subquery,
                                             trans_param.is_correlated_,
                                             is_valid))) {
    LOG_WARN("failed to check subquery valid", K(ret));
  }
  return ret;
}

int ObWhereSubQueryPullup::check_subquery_validity(ObQueryRefRawExpr *query_ref,
                                                   ObSelectStmt *subquery,
                                                   bool is_correlated,
                                                   bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  bool check_status = false;
  if (OB_ISNULL(query_ref) || OB_ISNULL(subquery)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret), K(query_ref), K(subquery));
  } else if (0 == subquery->get_from_item_size()) {
    is_valid = false;
    OPT_TRACE("subquery do not have table item");
  } else if (OB_FAIL(subquery->has_ref_assign_user_var(check_status))) {
    LOG_WARN("failed to check stmt has assignment ref user var", K(ret));
  } else if (check_status) {
    is_valid = false;
    OPT_TRACE("subquery has user var");
  } else if (OB_FAIL(ObTransformUtils::is_select_item_contain_subquery(subquery, check_status))) {
    LOG_WARN("failed to check select item contain subquery", K(subquery), K(ret));
  } else if (check_status) {
    is_valid = false;
    OPT_TRACE("subquery`s select expr contain subquery");
  } else if (!is_correlated) {
    // do nothing
  } else if (OB_FAIL(ObTransformUtils::is_join_conditions_correlated(query_ref->get_exec_params(),
                                                                     subquery,
                                                                     check_status))) {
    LOG_WARN("failed to is joined table conditions correlated", K(ret));
  } else if (check_status) {
    is_valid = false;
    OPT_TRACE("subquery`s on condition is correlated");
  } else if (OB_FAIL(is_where_having_subquery_correlated(query_ref->get_exec_params(), *subquery, check_status))) {
    LOG_WARN("failed to check select item contain subquery", K(subquery), K(ret));
  } else if (check_status) {
    is_valid = false;
    OPT_TRACE("subquery`s where condition contain correlated subquery");
  } else if (OB_FAIL(ObTransformUtils::is_table_item_correlated(
                       query_ref->get_exec_params(), *subquery, check_status))) {
    LOG_WARN("failed to check if subquery contain correlated subquery", K(ret));
  } else if (check_status) {
    is_valid = false;
    OPT_TRACE("subquery`s table item is correlated");
  }
  return ret;
}

/*@brief check if where subquery and having subquery in subquery is correlated
* semi/anti 的condition中不能有包含上层相关变量的子查询，暂时不支持这种行为
*   eg:select c2 from t1 where c2 not in
*                       (select c2 from t2 where t2.c2 not in (select 1 from t3 where t1.c3 = 1));
*/
int ObWhereSubQueryPullup::is_where_having_subquery_correlated(const ObIArray<ObExecParamRawExpr *> &exec_params,
                                                               const ObSelectStmt &subquery,
                                                               bool &is_correlated)
{
  int ret = OB_SUCCESS;
  is_correlated = false;
  ObSEArray<ObRawExpr*, 4> conds;
  if (OB_FAIL(conds.assign(subquery.get_condition_exprs()))) {
    LOG_WARN("failed to assign condition exprs", K(ret));
  } else if (OB_FAIL(append(conds, subquery.get_having_exprs()))) {
    LOG_WARN("failed to append having exprs", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && !is_correlated && i < conds.count(); ++i) {
    if (OB_ISNULL(conds.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL pointer error", K(conds.at(i)), K(ret));
    } else if (!conds.at(i)->has_flag(CNT_SUB_QUERY)) {
      // do nothing
    } else if (OB_FAIL(ObTransformUtils::is_correlated_expr(exec_params, conds.at(i), is_correlated))) {
      LOG_WARN("failed to check is correlated expr", K(ret));
    }
  }
  return ret;
}

int ObWhereSubQueryPullup::do_transform_pullup_subquery(ObDMLStmt *stmt,
                                                        ObRawExpr *expr,
                                                        TransformParam &trans_param,
                                                        bool &trans_happened)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *subquery = trans_param.subquery_;
  ObQueryRefRawExpr *query_ref = trans_param.subquery_expr_;
  if (OB_ISNULL(stmt) || OB_ISNULL(expr) ||
      OB_ISNULL(query_ref) || OB_ISNULL(subquery) ||
      OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL pointer error", K(stmt), K(expr), K(subquery), K(ctx_), K(query_ref), K(ret));
    //Just in case different parameters hit same plan, firstly we need add const param constraint.
    //In pullup_correlated_subquery_as_view limit expr will be set as null, so add constraint first.
  } else if (trans_param.need_add_limit_constraint_ &&
             OB_FAIL(ObTransformUtils::add_const_param_constraints(subquery->get_limit_expr(),
                                                                   ctx_))) {
    LOG_WARN("failed to add const param constraints", K(ret));
  } else if (trans_param.need_create_spj_) {
    if (OB_FAIL(ObTransformUtils::create_spj_and_pullup_correlated_exprs(query_ref->get_exec_params(),
                                                                         subquery,
                                                                         ctx_))) {
      LOG_WARN("failed to create spj and pullup correlated exprs", K(ret));
    } else {
      query_ref->set_ref_stmt(subquery);
    }
  }
  if (OB_FAIL(ret)) {
  } else if (trans_param.is_correlated_ &&
             OB_FAIL(pullup_correlated_subquery_as_view(stmt, subquery, expr,
                                                        query_ref))) {
    LOG_WARN("failed to pullup subquery as view", K(ret));
  } else if (!trans_param.is_correlated_ &&
             OB_FAIL(pullup_non_correlated_subquery_as_view(stmt, subquery, expr,
                                                            query_ref))) {
    LOG_WARN("failed to pullup subquery as view", K(ret));
  } else {
    trans_happened = true;
  }
  return ret;
}

// create new conds to subquery
// pullup conds and subquery
int ObWhereSubQueryPullup::pullup_correlated_subquery_as_view(ObDMLStmt *stmt,
                                                              ObSelectStmt *subquery,
                                                              ObRawExpr *expr,
                                                              ObQueryRefRawExpr *query_ref)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt) || OB_ISNULL(subquery) || OB_ISNULL(expr) || OB_ISNULL(ctx_)
      || OB_ISNULL(ctx_->allocator_) || OB_ISNULL(ctx_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(stmt), K(subquery), K(expr));
  } else if (OB_FAIL(ObOptimizerUtil::remove_item(stmt->get_condition_exprs(), expr))) {
    LOG_WARN("failed to remove condition expr", K(ret));
  } else {
    // select * from t1 where exists (select 1 from t2 where t1.c1 = c1 and c2 = 2 limit 1);
    // for the query contains correlated subquery above, limit 1 should be removed after transform.
    // select * from t1 semi join (select c1 from t2 where c2 = 2) v on t1.c1 = v.c1;
    subquery->set_limit_offset(NULL, NULL);
    // after ObWhereSubQueryPullup::check_limit, only any/all subquery with const select expr allowed unnest with order by and limit clause:
    // select * from t1 where t1.c1 in (select 1 from t2 where t1.c2 = c2 order by c3 limit 1);
    // just reset useless order items here.
    subquery->get_order_items().reuse();
  }

  if (OB_SUCC(ret)) {
    ObSEArray<ObRawExpr*, 4> right_hand_exprs;
    ObSEArray<ObRawExpr*, 4> candi_semi_conds;
    ObSEArray<ObRawExpr*, 4> column_exprs;
    ObSEArray<TableItem*, 4> right_tables;
    ObSEArray<ObSEArray<ObRawExpr*, 4>, 4> semi_conds;
    ObJoinType join_type = UNKNOWN_JOIN;
    SemiInfoSplitHelper split_helper;
    bool can_split = false;

    if (OB_FAIL(ObTransformUtils::get_correlated_conditions(query_ref->get_exec_params(),
                                                            subquery->get_condition_exprs(),
                                                            candi_semi_conds))) {
      LOG_WARN("failed to get semi conditions", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(candi_semi_conds,
                                                            column_exprs))) {
      LOG_WARN("failed to extract column exprs", K(ret));
    } else if (OB_FAIL(subquery->get_select_exprs(right_hand_exprs))) {
      LOG_WARN("failed to get select exprs", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(right_hand_exprs,
                                                            column_exprs))) {
      LOG_WARN("failed to extract column exprs", K(ret));
    } else if (OB_FAIL(generate_conditions(stmt, right_hand_exprs, subquery,
                                           expr, candi_semi_conds,
                                           subquery->get_condition_exprs()))) {
      // create conditions with left_hand and subquery's original targetlist
      LOG_WARN("failed to generate new condition exprs", K(ret), K(*subquery));
    } else if (OB_FAIL(ObOptimizerUtil::remove_item(subquery->get_condition_exprs(),
                                                    candi_semi_conds))) {
      LOG_WARN("failed to remove condition expr", K(ret));
    } else if (OB_FALSE_IT(subquery->get_select_items().reset())) {
    } else if (T_OP_EXISTS == expr->get_expr_type() || expr->has_flag(IS_WITH_ANY)) {
      join_type = LEFT_SEMI_JOIN;
      // add as LEFT_SEMI/ANTI_JOIN,
      // RIGHT_SEMI/ANTI_JOIN path is added in ObJoinOrder::generate_join_paths()
    } else if (T_OP_NOT_EXISTS == expr->get_expr_type() || expr->has_flag(IS_WITH_ALL)) {
      join_type = LEFT_ANTI_JOIN;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected expr type", K(ret), K(expr->get_expr_type()));
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(check_can_split(subquery, candi_semi_conds, join_type, split_helper))) {
      LOG_WARN("fail to check can split", K(ret));
    } else if (!split_helper.can_split_) {
      TableItem *table_item = NULL;
      ObSEArray<ObRawExpr*, 4> upper_column_exprs;
      ObSEArray<ObRawExpr*, 4> final_semi_conds;
      ObRawExprCopier copier(*ctx_->expr_factory_);
      if (OB_FAIL(ObTransformUtils::add_new_table_item(ctx_, stmt, subquery, table_item))) {
        LOG_WARN("failed to add new table_item", K(ret));
      } else if (OB_ISNULL(table_item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table item should not be null", K(ret));
      } else if (column_exprs.empty()) {
        if (OB_FAIL(ObTransformUtils::create_dummy_select_item(*subquery, ctx_))) {
          LOG_WARN("failed to create dummy select item", K(ret));
        } else if (OB_FAIL(final_semi_conds.assign(candi_semi_conds))) {
          LOG_WARN("failed to assign semi conditions", K(ret));
        }
      } else if (OB_FAIL(ObTransformUtils::create_select_item(*ctx_->allocator_, column_exprs,
                                                              subquery))) {
        LOG_WARN("failed to create select item", K(ret));
      } else if (OB_FAIL(ObTransformUtils::create_columns_for_view(ctx_, *table_item, stmt,
                                                                  upper_column_exprs))) {
        LOG_WARN("failed to create columns for view", K(ret));
      } else if (OB_FAIL(copier.add_replaced_expr(column_exprs, upper_column_exprs))) {
        LOG_WARN("failed to add replace pair", K(ret));
      } else if (OB_FAIL(copier.copy_on_replace(candi_semi_conds, final_semi_conds))) {
        LOG_WARN("failed to copy on replace expr", K(ret));
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(right_tables.push_back(table_item))) {
        LOG_WARN("fail to push back table item", K(ret));
      } else if (OB_FAIL(semi_conds.push_back(final_semi_conds))) {
        LOG_WARN("fail to push back semi conditions", K(ret));
      }
    } else if (OB_FAIL(ObTransformUtils::do_split_cartesian_tables(ctx_, stmt, subquery, candi_semi_conds,
                                                  split_helper.connected_tables_, right_tables, semi_conds))) {
      LOG_WARN("fail to split cartesian tables", K(ret));
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_UNLIKELY(right_tables.count() != semi_conds.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unequal tables and semi conds count", K(ret), K(right_tables.count()), K(semi_conds.count()));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < right_tables.count(); ++i) {
        SemiInfo *info = NULL;
        TableItem *split_right_table = right_tables.at(i);
        ObIArray<ObRawExpr*> &split_semi_conditions = semi_conds.at(i);
        ObSelectStmt *split_subquery = NULL;
        if (OB_ISNULL(split_right_table)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("split right table is NULL", K(ret));
        } else if (OB_ISNULL(split_subquery = split_right_table->ref_query_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("split ref query is NULL", K(ret));
        } else if (OB_FAIL(ObTransformUtils::decorrelate(split_semi_conditions, query_ref->get_exec_params()))) {
          LOG_WARN("failed to decorrelate semi conditions", K(ret));
        } else if (OB_FAIL(generate_semi_info(stmt, split_right_table, split_semi_conditions, join_type, info))) {
          LOG_WARN("failed to generate semi info", K(ret));
        } else if (OB_FAIL(split_subquery->adjust_subquery_list())) {
          LOG_WARN("failed to adjust subquery list", K(ret));
        } else if (OB_FAIL(split_subquery->formalize_stmt(ctx_->session_info_))) {
          LOG_WARN("formalize child stmt failed", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(stmt->formalize_stmt(ctx_->session_info_))) {
      LOG_WARN("failed to formalize stmt", K(ret));
  }
  return ret;
}

// create semi info with a generate table
int ObWhereSubQueryPullup::generate_semi_info(ObDMLStmt *stmt,
                                              TableItem *right_table,
                                              ObIArray<ObRawExpr*> &semi_conditions,
                                              ObJoinType join_type,
                                              SemiInfo *&semi_info)
{
  int ret = OB_SUCCESS;
  semi_info = NULL;
  SemiInfo *info = NULL;
  ObIAllocator *alloc = NULL;
  if (OB_ISNULL(stmt) || OB_ISNULL(right_table) ||
      OB_ISNULL(ctx_) || OB_ISNULL(alloc = ctx_->allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL pointer error", K(stmt), K(right_table), K(alloc), K(ret));
  } else if (OB_UNLIKELY(!right_table->is_generated_table())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected right table type in semi info", K(ret), K(right_table->type_));
  } else if (OB_ISNULL(info = static_cast<SemiInfo *>(alloc->alloc(sizeof(SemiInfo))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to alloc semi info", K(ret));
  } else if (OB_FALSE_IT(info = new (info) SemiInfo())) {
  } else if (OB_FAIL(info->semi_conditions_.assign(semi_conditions))) {
    LOG_WARN("failed to assign semi condition exprs", K(ret));
  } else if (OB_FALSE_IT(info->right_table_id_ = right_table->table_id_)) {
  } else if (OB_FAIL(fill_semi_left_table_ids(stmt, info))) {
    LOG_WARN("failed to fill semi left table ids", K(ret));
  } else if (OB_FAIL(stmt->add_semi_info(info))) {
    LOG_WARN("failed to add semi info", K(ret));
  } else {
    info->join_type_ = join_type;
  }

  if (OB_SUCC(ret)) {
    info->semi_id_ = stmt->get_query_ctx()->available_tb_id_--;
    semi_info = info;
  }
  return ret;
}

int ObWhereSubQueryPullup::fill_semi_left_table_ids(ObDMLStmt *stmt,
                                                    SemiInfo *info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt) || OB_ISNULL(info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL pointer error", K(ret), K(stmt), K(info));
  } else {
    info->left_table_ids_.reuse();
    ObSqlBitSet<> left_rel_ids;
    int32_t right_idx = stmt->get_table_bit_index(info->right_table_id_);
    TableItem *table = NULL;
    ObRawExpr *cond_expr = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < info->semi_conditions_.count(); ++i) {
      if (OB_ISNULL(cond_expr = info->semi_conditions_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret));
      } else if (OB_FAIL(cond_expr->pull_relation_id())) {
        LOG_WARN("pull expr relation ids failed", K(ret), K(*cond_expr));
      } else if (OB_FAIL(left_rel_ids.add_members(cond_expr->get_relation_ids()))) {
        LOG_WARN("failed to add members", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(left_rel_ids.del_member(right_idx))) {
      LOG_WARN("failed to delete members", K(ret));
    } else if (!left_rel_ids.is_empty()) {
      ret = stmt->relids_to_table_ids(left_rel_ids, info->left_table_ids_);
    } else if (OB_UNLIKELY(0 == stmt->get_from_item_size()) ||
               OB_ISNULL(table = stmt->get_table_item(stmt->get_from_item(0)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("stmt from item is empty", K(ret), K(table));
    } else if (table->is_joined_table()) {
      ret = append(info->left_table_ids_, static_cast<JoinedTable*>(table)->single_table_ids_);
    } else if (OB_FAIL(info->left_table_ids_.push_back(table->table_id_))) {
      LOG_WARN("failed to push back table id", K(ret));
    }
  }
  return ret;
}

int ObWhereSubQueryPullup::get_semi_oper_type(ObRawExpr *op, ObItemType &oper_type)
{
  int ret = OB_SUCCESS;
  oper_type = T_INVALID;
  if (OB_ISNULL(op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL pointer error", K(op), K(ret));
  } else if (op->has_flag(IS_WITH_ALL)) {
    switch (op->get_expr_type()) {
    case T_OP_SQ_EQ:
      oper_type = T_OP_NE;
      break;
    case T_OP_SQ_LE:
      oper_type = T_OP_GT;
      break;
    case T_OP_SQ_LT:
      oper_type = T_OP_GE;
      break;
    case T_OP_SQ_GE:
      oper_type = T_OP_LT;
      break;
    case T_OP_SQ_GT:
      oper_type = T_OP_LE;
      break;
    case T_OP_SQ_NE:
      oper_type = T_OP_EQ;
      break;
    default:
      break;
    }
  } else {
    switch (op->get_expr_type()) {
    case T_OP_SQ_EQ:
       oper_type = T_OP_EQ;
       break;
    case T_OP_SQ_LE:
       oper_type = T_OP_LE;
       break;
    case T_OP_SQ_LT:
       oper_type = T_OP_LT;
       break;
    case T_OP_SQ_GE:
       oper_type = T_OP_GE;
       break;
    case T_OP_SQ_GT:
       oper_type = T_OP_GT;
       break;
    case T_OP_SQ_NE:
       oper_type = T_OP_NE;
       break;
     default:
       break;
    }
  }
  return ret;
}

int ObWhereSubQueryPullup::generate_anti_condition(ObDMLStmt *stmt,
                                                   const ObSelectStmt *subquery,
                                                   ObRawExpr *cond_expr,
                                                   ObRawExpr *subq_select_expr,
                                                   ObRawExpr *left_arg,
                                                   ObRawExpr *right_arg,
                                                   ObRawExpr *&anti_expr)
{
  int ret = OB_SUCCESS;
  ObRawExprFactory *expr_factory = NULL;
  bool left_is_not_null = false;
  bool right_is_not_null = false;
  ObRawExpr *left_null = NULL;
  ObRawExpr *right_null = NULL;
  ObOpRawExpr *new_cond_expr = NULL;
  ObArray<ObRawExpr *> left_constraints;
  ObArray<ObRawExpr *> right_constraints;
  if (OB_ISNULL(stmt) || OB_ISNULL(subquery) || OB_ISNULL(cond_expr) ||
      OB_ISNULL(left_arg) || OB_ISNULL(right_arg) || OB_ISNULL(subq_select_expr) ||
      OB_ISNULL(ctx_) || OB_ISNULL(expr_factory = ctx_->expr_factory_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(stmt), K(subquery), K(cond_expr), K(left_arg),
             K(right_arg), K(ctx_), K(expr_factory));
  } else if (OB_FAIL(ObTransformUtils::is_expr_not_null(ctx_, stmt, left_arg, 
                                                        NULLABLE_SCOPE::NS_WHERE, 
                                                        left_is_not_null, &left_constraints))) {
    LOG_WARN("fail to check need make null with left arg", K(ret));
  } else if (left_is_not_null && 
             OB_FAIL(ObTransformUtils::add_param_not_null_constraint(*ctx_, left_constraints))) {
    LOG_WARN("failed to add left constraints", K(ret));
  } else if (OB_FAIL(ObTransformUtils::is_expr_not_null(ctx_,  subquery, subq_select_expr,
                                                        NULLABLE_SCOPE::NS_TOP,
                                                        right_is_not_null, &right_constraints))) {
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
  } else if (OB_FAIL(new_cond_expr->add_param_expr(cond_expr))) {
    LOG_WARN("failed to add param expr", K(ret));
  }
  if (OB_SUCC(ret) && !left_is_not_null) {
    if (OB_FAIL(make_null_test(stmt, left_arg, left_null))) {
      LOG_WARN("failed to make null test", K(ret));
    } else if (OB_FAIL(new_cond_expr->add_param_expr(left_null))) {
      LOG_WARN("failed to add param expr", K(ret));
    }
  }
  if (OB_SUCC(ret) && !right_is_not_null) {
    if (OB_FAIL(make_null_test(stmt, right_arg, right_null))) {
      LOG_WARN("faield to make null test", K(ret));
    } else if (OB_FAIL(new_cond_expr->add_param_expr(right_null))) {
      LOG_WARN("failed to add param expr", K(ret));
    }
  }
  return ret;
}

int ObWhereSubQueryPullup::make_null_test(ObDMLStmt *stmt, ObRawExpr *in_expr, ObRawExpr *&out_expr)
{
  int ret = OB_SUCCESS;
  ObRawExprFactory *expr_factory = NULL;
  if (OB_ISNULL(stmt) || OB_ISNULL(in_expr) || OB_ISNULL(ctx_) || OB_ISNULL(expr_factory = ctx_->expr_factory_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(stmt), K(in_expr), KP_(ctx), KP(expr_factory));
  } else if (ObRawExprUtils::build_is_not_null_expr(*expr_factory,
                                                    in_expr,
                                                    false/*is_not_null*/,
                                                    out_expr)) {
    LOG_WARN("failed to create is null expr", K(ret));
  } else if (OB_FAIL(out_expr->add_relation_ids(in_expr->get_relation_ids()))) {
    LOG_WARN("failed to add relation ids", K(ret));
  }
  return ret;
}

/**
 * @brief  将子查询提升上来后，将原来包含子查询的谓词转化为新的条件
 */
int ObWhereSubQueryPullup::generate_conditions(ObDMLStmt *stmt,
                                               ObIArray<ObRawExpr *> &subq_exprs,
                                               ObSelectStmt *subquery,
                                               ObRawExpr *expr,
                                               ObIArray<ObRawExpr*> &semi_conds,
                                               ObIArray<ObRawExpr *> &right_conds)
{
  int ret = OB_SUCCESS;
  ObRawExprFactory *expr_factory = NULL;
  if (OB_ISNULL(stmt) || OB_ISNULL(expr) || OB_ISNULL(subquery)
      || OB_ISNULL(ctx_) || OB_ISNULL(expr_factory = ctx_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL pointer error", K(stmt), K(expr), K(ctx_), K(expr_factory));
  } else if (expr->has_flag(IS_WITH_ALL) || expr->has_flag(IS_WITH_ANY)) {
    ObItemType oper_type = T_INVALID;
    int64_t N = subquery->get_select_item_size();
    ObRawExpr *cmp_expr = NULL;
    ObRawExpr *left_hand = expr->get_param_expr(0);
    if (OB_ISNULL(left_hand)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("left param is null", K(ret));
    } else if (N > 1 &&
               OB_UNLIKELY(left_hand->get_expr_type() != T_OP_ROW ||
                           left_hand->get_param_count() != N ||
                           subq_exprs.count() != N)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid vector comparison",
               K(ret), K(N), K(subq_exprs.count()), K(*left_hand));
    } else if (OB_FAIL(get_semi_oper_type(expr, oper_type))) {
      LOG_WARN("failed to get oper type", K(ret));
    } else if (T_INVALID == oper_type) {
      ret = OB_ERR_ILLEGAL_TYPE;
      LOG_WARN("Invalid oper type in subquery", K(ret), K(expr->get_expr_type()));
    } else if (N == 1 || oper_type == T_OP_EQ) {
      // a = b, a > b, a !=b, (a,b) = (c,d) ...
      for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
        ObRawExpr *left_arg = (N == 1) ? left_hand : left_hand->get_param_expr(i);
        ObRawExpr *right_arg = subq_exprs.at(i);
        ObRawExpr *select_expr = subquery->get_select_item(i).expr_;
        if (OB_SUCC(ret) && OB_FAIL(ObRawExprUtils::create_double_op_expr(
                            *expr_factory, ctx_->session_info_, oper_type,
                            cmp_expr, left_arg, right_arg))) {
          LOG_WARN("failed to create comparison expr", K(ret));
        } else if (!expr->has_flag(IS_WITH_ALL)) {
          // do nothing
        } else if (OB_FAIL(generate_anti_condition(
                                   stmt, subquery, cmp_expr, select_expr, left_arg, right_arg, cmp_expr))) {
          LOG_WARN("failed to create anti join condition", K(ret));
        }
        if (OB_SUCC(ret)) {
          if (left_arg->is_const_expr() && !right_arg->has_flag(CNT_DYNAMIC_PARAM)) {
            if (OB_FAIL(right_conds.push_back(cmp_expr))) {
              LOG_WARN("failed to push back right table conditions", K(ret));
            }
          } else {
            if (OB_FAIL(semi_conds.push_back(cmp_expr))) {
              LOG_WARN("failed to push back semi join conditions", K(ret));
            }
          }
        }
      }
    } else {
      // (a,b) != any (c,d) => (a,b) != (c,d) semi
      // (a,b)  = all (c,d) => lnnvl( (a,b) == (c,d) ) anit
      ObOpRawExpr *right_vector = NULL;
      ObRawExpr *lnnvl_expr = NULL;
      oper_type = expr->has_flag(IS_WITH_ALL)? T_OP_EQ : T_OP_NE;
      if (OB_FAIL(expr_factory->create_raw_expr(T_OP_ROW, right_vector))) {
        LOG_WARN("failed to create a new expr", K(ret));
      } else if (OB_FAIL(right_vector->get_param_exprs().assign(subq_exprs))) {
        LOG_WARN("failed to add param expr", K(ret));
      } else if (lib::is_oracle_mode()) {
        ObOpRawExpr *row_expr = NULL;
        if (OB_FAIL(expr_factory->create_raw_expr(T_OP_ROW, row_expr))) {
          LOG_WARN("failed to create a new expr", K(oper_type), K(ret));
        } else if (OB_FAIL(row_expr->add_param_expr(right_vector))) {
          LOG_WARN("fail to set param expr", K(ret), K(right_vector), K(row_expr));
        }
        right_vector = row_expr;
      }
      if (OB_SUCC(ret) && OB_FAIL(ObRawExprUtils::create_double_op_expr(
                           *expr_factory, ctx_->session_info_, oper_type,
                           cmp_expr, left_hand, right_vector))) {
        LOG_WARN("failed to create comparison expr", K(ret));
      } else if (!expr->has_flag(IS_WITH_ALL)) {
        // do nothing
      } else if (OB_FAIL(ObRawExprUtils::build_lnnvl_expr(
                           *expr_factory, cmp_expr, lnnvl_expr))) {
        LOG_WARN("failed to build lnnvl expr", K(ret));
      } else {
        cmp_expr = lnnvl_expr;
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(cmp_expr->formalize(ctx_->session_info_))) {
          LOG_WARN("failed to formalize expr", K(ret));
        } else if (left_hand->is_const_expr() &&
                   !right_vector->has_flag(CNT_DYNAMIC_PARAM)) {
          if (OB_FAIL(right_conds.push_back(cmp_expr))) {
            LOG_WARN("failed to push back right table conditions", K(ret));
          }
        } else {
          if (OB_FAIL(semi_conds.push_back(cmp_expr))) {
            LOG_WARN("failed to push back semi conditions", K(ret));
          }
        }
      }
    }

  }
  return ret;
}

/** @brief 将和外查询无关的子查询提升为一个view
 *         并和原来的表做join，根据子查询和外查询的相关特性可能转化为：
 *         II . IN (SELECT ...)     -> semi join
 *         III. NOT IN (SELECT ...) -> anti join
 *  NOTE: We use the term 'view' to describe a sub-query block appearing in the FROM clause,
 *        but also NOTE THAT IN OB we don't actually put Semi Join Tables in FromItems.
 *        Instead, we put them into a special data structure called SemiInfo.
 *
 *  e.g.
 *  I
 *  SELECT * FROM T1 WHERE T1.id IN (SELECT min(c1) FROM T2 GROUP BY c2);
 *  ->
 *  SELECT * FROM T1 SEMI-JOIN (SELECT min(c1) FROM T2 GROUP BY c2) AS view
 *  WHERE T1.id = view.`min(c1)`
 *
 *  II
 *  SELECT * FROM T1 WHERE T1.id NOT IN (SELECT min(c1) FROM T2 GROUP BY c2);
 *  ->
 *  SELECT * FROM T1 ANTI-JOIN (SELECT min(c1) FROM T2 GROUP BY c2) AS view
 *  WHERE T1.id = view.`min(c1)`
 *  OR T1.id IS NULL
 *  OR view.`min(c1)` IS NULL
 *
 *
 *  改写条件：
 *  I 改为semi join:
 *     1. IN(subquery)或者等价的=ANY(subquery)
 *     2. subquery和外查询无关联
 *     3. subquery不是集合操作(UNION / EXCEPT等)
 *
 *  II 改为anti join:
 *     1. NOT IN(subquery)或者等价的<>ALL(subquery)
 *     2. subquery和外查询无关联
 *     4. subquery不是集合操作(UNION / EXCEPT等)
 *
 *  执行流程：
 *         1. 生成一个generated_table，指向子查询；过程中需要填充generated_table中的列
 *         2. 生成等值连接条件，并加入到本查询的WHERE子句中
 *         3. 对于III改写anti join，如果需要，需要添加额外NOT NULL条件
 *         4. 移除原来的IN / NOT IN表达式
 *         5. 递归提升子查询和它内部表达式的层级
 */
int ObWhereSubQueryPullup::pullup_non_correlated_subquery_as_view(ObDMLStmt *stmt,
                                                                  ObSelectStmt *subquery,
                                                                  ObRawExpr *expr,
                                                                  ObQueryRefRawExpr *subquery_expr)
{
  int ret = OB_SUCCESS;
  ObRawExprFactory *expr_factory = NULL;
  ObSEArray<ObRawExpr*, 4> right_exprs;
  ObSEArray<ObRawExpr *, 4> new_conditions;
  ObSEArray<TableItem*, 4> right_tables;
  ObSEArray<ObSEArray<ObRawExpr*, 4>, 4> semi_conds;
  ObJoinType join_type = UNKNOWN_JOIN;
  SemiInfoSplitHelper split_helper;
  bool can_split;
  if (OB_ISNULL(stmt) || OB_ISNULL(subquery) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt or subquery, query ctx should not be null",
             K(ret), K(stmt), K(subquery), K(expr));
  } else if (OB_ISNULL(ctx_) || OB_ISNULL(expr_factory = ctx_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx or expr_factory is null", K(ret), K(ctx_), K(expr_factory));
  } else if (expr->has_flag(IS_WITH_ANY)) {
    join_type = LEFT_SEMI_JOIN;
    // add as LEFT_SEMI/ANTI_JOIN,
    // RIGHT_SEMI/ANTI_JOIN path is added in ObJoinOrder::generate_join_paths()
  } else if (expr->has_flag(IS_WITH_ALL)) {
    join_type = LEFT_ANTI_JOIN;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected expr type", K(ret), K(expr->get_expr_type()));
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(ObOptimizerUtil::remove_item(stmt->get_condition_exprs(), expr))) {
    LOG_WARN("failed to remove condition expr", K(ret));
  } else if (OB_FAIL(subquery->get_select_exprs(right_exprs))) {
    LOG_WARN("failed to get select exprs", K(ret));
  } else if (OB_FAIL(generate_conditions(stmt, right_exprs, subquery, expr,
                                         new_conditions, new_conditions))) {
    LOG_WARN("failed to generate new condition exprs", K(ret));
  } else if (OB_FAIL(check_can_split(subquery, new_conditions, join_type, split_helper))) {
    LOG_WARN("fail to check can split", K(ret));
  } else if (!split_helper.can_split_) {
    TableItem *table_item = NULL;
    right_exprs.reuse();
    new_conditions.reuse();
    if (OB_FAIL(ObTransformUtils::add_new_table_item(ctx_, stmt, subquery, table_item))) {
      LOG_WARN("failed to add new table_item", K(ret));
    } else if (OB_ISNULL(table_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table_item should not be null", K(ret));
    } else if (OB_FAIL(ObTransformUtils::create_columns_for_view(ctx_, *table_item,
                                                               stmt, right_exprs))) {
      LOG_WARN("failed to create columns for view", K(ret));
    } else if (OB_FAIL(generate_conditions(stmt, right_exprs, subquery, expr,
                                           new_conditions, new_conditions))) {
      LOG_WARN("failed to generate new condition exprs", K(ret));
    } else if (OB_FAIL(right_tables.push_back(table_item))) {
      LOG_WARN("fail to push back table item", K(ret));
    } else if (OB_FAIL(semi_conds.push_back(new_conditions))) {
      LOG_WARN("fail to push back semi conditions", K(ret));
    }
  } else if (OB_FALSE_IT(subquery->get_select_items().reset())) {
  } else if (OB_FAIL(ObTransformUtils::do_split_cartesian_tables(ctx_, stmt, subquery, new_conditions,
                                                split_helper.connected_tables_, right_tables, semi_conds))) {
    LOG_WARN("fail to split cartesian tables", K(ret));
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_UNLIKELY(right_tables.count() != semi_conds.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unequal tables and semi conds count", K(ret), K(right_tables.count()), K(semi_conds.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < right_tables.count(); ++i) {
      SemiInfo *info = NULL;
      TableItem *split_right_table = right_tables.at(i);
      ObIArray<ObRawExpr*> &split_semi_conditions = semi_conds.at(i);
      ObSelectStmt *split_subquery = NULL;
      if (OB_ISNULL(split_right_table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("split right table is NULL", K(ret));
      } else if (OB_ISNULL(split_subquery = split_right_table->ref_query_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("split ref query is NULL", K(ret));
      } else if (OB_FAIL(generate_semi_info(stmt, split_right_table, split_semi_conditions, join_type, info))) {
        LOG_WARN("failed to generate semi info", K(ret));
      } else if (OB_FAIL(split_subquery->adjust_subquery_list())) {
        LOG_WARN("failed to adjust subquery list", K(ret));
      } else if (OB_FAIL(split_subquery->formalize_stmt(ctx_->session_info_))) {
        LOG_WARN("formalize child stmt failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(stmt->formalize_stmt(ctx_->session_info_))) {
      LOG_WARN("failed to formalize stmt", K(ret));
  }
  return ret;
}

/**
 * @brief ObWhereSubQueryPullup::transform_single_set_query
 * 1. 检查子查询是不是 single-set spj，只要是 single-set spj 逻辑上就可以改造
 * 2. 子查询构造一个 outer join，然后替换所有的引用
 * @return
 */
int ObWhereSubQueryPullup::transform_single_set_query(ObDMLStmt *stmt,
                                                      ObIArray<ObSelectStmt*> &unnest_stmts,
                                                      bool &trans_happened)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 4> cond_exprs;
  ObSEArray<ObRawExpr *, 4> post_join_exprs;
  ObSEArray<ObRawExpr *, 4> select_exprs;
  ObSEArray<ObQueryRefRawExpr*, 4> transformed_subqueries;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (0 == stmt->get_from_item_size()) {
    /*do dothing*/
  } else if (OB_FAIL(cond_exprs.assign(stmt->get_condition_exprs()))) {
    LOG_WARN("failed to get non semi conditions", K(ret));
  } else if (OB_FAIL(ObTransformUtils::get_post_join_exprs(stmt, post_join_exprs, true))) {
    LOG_WARN("failed to get post join exprs", K(ret));
  } else if (stmt->is_select_stmt()) {
    ObSelectStmt *sel_stmt = static_cast<ObSelectStmt*>(stmt);
    if (OB_FAIL(sel_stmt->get_select_exprs(select_exprs))) {
      LOG_WARN("failed to get select exprs", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < cond_exprs.count(); ++i) {
    ObSEArray<SingleSetParam, 4> queries;
    if (OB_FAIL(get_single_set_subquery(*stmt,
                                        cond_exprs.at(i),
                                        cond_exprs.at(i),
                                        true,
                                        queries))) {
      LOG_WARN("failed to get single set subquery", K(ret));
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < queries.count(); ++j) {
      ObSelectStmt *subquery = NULL;
      ObQueryRefRawExpr *query_expr = queries.at(j).query_ref_expr_;
      bool subq_match_idx = false;
      if (OB_ISNULL(query_expr) || OB_ISNULL(subquery = query_expr->get_ref_stmt())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(ObTransformUtils::check_subquery_match_index(ctx_, query_expr, subquery, subq_match_idx))) {
        LOG_WARN("fail to check subquery match index", K(ret));
      } else if (queries.at(j).use_outer_join_ && subq_match_idx && subquery->get_table_items().count() > 1 &&
                 !subquery->get_stmt_hint().has_enable_hint(T_UNNEST)) {
        // do nothing
      } else if (subquery->get_select_item_size() >= 2) {
        // do nothing
      } else if (has_exist_in_array(transformed_subqueries, query_expr)) {
        //do nothing
      } else if (OB_FAIL(transformed_subqueries.push_back(query_expr))) {
        LOG_WARN("fail to push back", K(ret));
      } else if (OB_FAIL(unnest_single_set_subquery(stmt, queries.at(j), false))) {
        LOG_WARN("failed to unnest single set subquery", K(ret));
      } else if (OB_FAIL(unnest_stmts.push_back(subquery))) {
        LOG_WARN("failed to push back", K(ret));
      } else {
        trans_happened = true;
      }
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < post_join_exprs.count(); ++i) {
    ObSEArray<SingleSetParam, 4> queries;
    bool is_vector_assign = false;
    bool is_select_expr = false;
    if (OB_FAIL(get_single_set_subquery(*stmt,
                                        post_join_exprs.at(i),
                                        post_join_exprs.at(i),
                                        false,
                                        queries))) {
      LOG_WARN("failed to get single set subquery", K(ret));
    } else {
      is_vector_assign = post_join_exprs.at(i)->has_flag(CNT_ALIAS);
      if (ObOptimizerUtil::find_item(select_exprs, post_join_exprs.at(i))) {
        is_select_expr = true;
      }
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < queries.count(); ++j) {
      ObSelectStmt *subquery = NULL;
      ObQueryRefRawExpr *query_expr = queries.at(j).query_ref_expr_;
      bool subq_match_idx = false;
      if (OB_ISNULL(query_expr) || 
          OB_ISNULL(subquery = query_expr->get_ref_stmt())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(ObTransformUtils::check_subquery_match_index(ctx_, query_expr, subquery, subq_match_idx))) {
        LOG_WARN("fail to check subquery match index", K(ret));
      } else if (queries.at(j).use_outer_join_ && subq_match_idx && subquery->get_table_items().count() > 1 &&
                 !subquery->get_stmt_hint().has_enable_hint(T_UNNEST)) {
        // do nothing
      } else if (is_select_expr && !subquery->get_stmt_hint().has_enable_hint(T_UNNEST)) {
        //do nothing
      } else if (has_exist_in_array(transformed_subqueries, query_expr) ||
                 (subquery->get_select_item_size() > 1 && !is_vector_assign)) {
        //do nothing
      } else if (OB_FAIL(transformed_subqueries.push_back(query_expr))) {
        LOG_WARN("fail to push back", K(ret));
      } else if (OB_FAIL(unnest_single_set_subquery(stmt,
                                                  queries.at(j),
                                                  is_vector_assign))) {
        LOG_WARN("failed to unnest single set subquery", K(ret));
      } else if (OB_FAIL(unnest_stmts.push_back(subquery))) {
        LOG_WARN("failed to push back", K(ret));
      } else {
        trans_happened = true;
      }
    }
  }
  return ret;
}

bool ObWhereSubQueryPullup::is_vector_query(ObQueryRefRawExpr *query)
{
  bool bret = false;
  if (NULL == query || NULL == query->get_ref_stmt()) {
    // do nothing
  } else {
    bret = query->get_ref_stmt()->get_select_item_size() >= 2;
  }
  return bret;
}

int ObWhereSubQueryPullup::get_single_set_subquery(ObDMLStmt &stmt,
                                                   ObRawExpr *root_expr,
                                                   ObRawExpr *expr,
                                                   bool in_where_cond,
                                                   ObIArray<SingleSetParam> &queries)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (expr->has_flag(IS_WITH_ANY) ||
             expr->has_flag(IS_WITH_ALL) ||
             T_OP_EXISTS == expr->get_expr_type() ||
             T_OP_NOT_EXISTS == expr->get_expr_type() ||
             expr->has_hierarchical_query_flag() ||
             !expr->has_flag(CNT_SUB_QUERY)) {
    //expr中含有层次查询相关flag两种情况:
    //1. subquery为层次查询, 此时一般难以保证subquery unique, 直接禁掉
    //2. expr含有prior,为避免改写后谓词下压引起执行问题，现有层次查询方案中暂时禁掉
    //  select * from t1 where prior 3 != (select c2 from t2 where c1 = 2)
    //  start with c1 = 1 connect by prior c1 = c2;
  } else if (!expr->is_query_ref_expr()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(get_single_set_subquery(stmt,
                                                     root_expr,
                                                     expr->get_param_expr(i),
                                                     in_where_cond,
                                                     queries)))) {
        LOG_WARN("failed to get single set query", K(ret));
      }
    }
  } else {
    bool existed = false;
    ObQueryRefRawExpr *query = static_cast<ObQueryRefRawExpr *>(expr);
    for (int64_t i = 0; OB_SUCC(ret) && i < queries.count(); ++i) {
      existed = (queries.at(i).query_ref_expr_ == query);
    }
    if (existed) {
      // do nothing
    } else if (OB_FAIL(check_subquery_validity(stmt,
                                               root_expr,
                                               query,
                                               in_where_cond,
                                               queries))) {
      LOG_WARN("failed to check subquery validity", K(ret));
    }
  }
  return ret;
}

int ObWhereSubQueryPullup::check_subquery_validity(ObDMLStmt &stmt,
                                                   ObRawExpr *root_expr,
                                                   ObQueryRefRawExpr *query_ref,
                                                   bool in_where_cond,
                                                   ObIArray<SingleSetParam> &queries)
{
  int ret = OB_SUCCESS;
  ObSEArray<const ObRawExpr *, 4> targets;
  ObSEArray<ObRawExpr *, 4> columns;
  ObSEArray<ObRawExpr *, 4> const_columns;
  ObSEArray<ObRawExpr *, 4> const_exprs;
  ObSelectStmt *subquery = NULL;
  SingleSetParam param;
  bool is_valid = true;
  OPT_TRACE("try to pullup single set subquery:", query_ref);
  if (OB_ISNULL(root_expr) || OB_ISNULL(query_ref) ||
      OB_ISNULL(subquery = query_ref->get_ref_stmt()) ||
      OB_ISNULL(stmt.get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("subquery is null", K(ret), K(subquery));
  } else if (OB_FAIL(check_hint_allowed_unnest(stmt, *subquery, is_valid))) {
    LOG_WARN("failed to check hint", K(ret));
  } else if (!is_valid) {
    // do nothing
    OPT_TRACE("hint reject transform");
  } else if (!subquery->is_spj() || subquery->has_subquery() ||
             (subquery->is_values_table_query() &&
              !ObTransformUtils::is_enable_values_table_rewrite(stmt.get_query_ctx()->optimizer_features_enable_version_))) {
    is_valid = false;
    OPT_TRACE("subquery is not spj or has subquery");
  } else if (OB_FAIL(subquery->get_column_exprs(columns))) {
    LOG_WARN("failed to get column exprs", K(ret));
  } else if (OB_FAIL(append(targets, columns))) {
    LOG_WARN("failed to append targets", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::compute_const_exprs(subquery->get_condition_exprs(),
                                                          const_exprs))) {
    LOG_WARN("failed to compute const exprs", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < columns.count(); ++i) {
    if (!ObOptimizerUtil::find_item(const_exprs, columns.at(i))) {
      // do nothing
    } else if (OB_FAIL(const_columns.push_back(columns.at(i)))) {
      LOG_WARN("failed to push back const column", K(ret));
    }
  }

  //1.检查是否是single set query
  if (OB_SUCC(ret) && is_valid) {
    if (OB_FAIL(ObTransformUtils::check_stmt_unique(subquery, ctx_->session_info_,
                                                    ctx_->schema_checker_, const_columns,
                                                    true /* strict */, is_valid))) {
      LOG_WARN("failed to check stmt unique", K(ret));
    } else if (!is_valid) {
      OPT_TRACE("not single set subquery");
    }
  }
  //2.check from item correlated
  if (OB_SUCC(ret) && is_valid) {
    bool is_correlated = false;
    if (OB_FAIL(ObTransformUtils::is_join_conditions_correlated(query_ref->get_exec_params(),
                                                                subquery,
                                                                is_correlated))) {
      LOG_WARN("failed to is joined table conditions correlated", K(ret));
    } else if (is_correlated) {
      is_valid = false;
      OPT_TRACE("subquery contain correlated on condition");
    } else if (OB_FAIL(ObTransformUtils::is_table_item_correlated(query_ref->get_exec_params(),
                                                                  *subquery,
                                                                  is_correlated))) {
      LOG_WARN("failed to check if subquery contain correlated subquery", K(ret));
    } else if (is_correlated) {
      is_valid = false;
      OPT_TRACE("subquery contain correlated table item");
    }
  }
  //3.检查是否以outer join上拉子查询
  if (OB_SUCC(ret) && is_valid) {
    bool is_null_reject = false;
    ObSEArray<const ObRawExpr *, 1> tmp;
    param.query_ref_expr_ = query_ref;
    if (!in_where_cond) {
      param.use_outer_join_ = true;
    } else if (OB_FAIL(tmp.push_back(query_ref))) {
      LOG_WARN("failed to push back query", K(ret));
    } else if (OB_FAIL(ObTransformUtils::is_null_reject_condition(root_expr,
                                                                  tmp,
                                                                  is_null_reject))) {
      LOG_WARN("failed to check is null reject condition", K(ret));
    } else {
      param.use_outer_join_ = !is_null_reject;
    }
    //outer join不支持上拉semi info，否则有正确性问题
    if (param.use_outer_join_ && subquery->get_semi_info_size() > 0) {
      is_valid = false;
      OPT_TRACE("subquery has semi info");
    }
  }
  if (OB_SUCC(ret) && is_valid && param.use_outer_join_) {
    for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < query_ref->get_param_count(); ++i) {
      ObRawExpr *param = NULL;
      if (OB_ISNULL(param = query_ref->get_param_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("param expr is null", K(ret));
      } else {
        is_valid = !param->has_flag(CNT_SUB_QUERY) &&
                   !param->has_flag(CNT_AGG) &&
                   !param->has_flag(CNT_WINDOW_FUNC);
      }
    }
  }

  //4.检查子查询是否有空拒绝表达式，如果有，需要找到not null column构造case when
  if (OB_SUCC(ret) && is_valid && param.use_outer_join_) {
    //检查是否有空值拒绝表达式
    bool find = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < subquery->get_select_item_size(); ++i) {
      bool is_null_propagate = true;
      if (OB_FAIL(ObTransformUtils::is_null_propagate_expr(subquery->get_select_item(i).expr_,
                                                           targets,
                                                           is_null_propagate))) {
        LOG_WARN("failed to check is null propagate expr", K(ret));
      } else if (is_null_propagate) {
        //do nothing
      } else if (OB_FAIL(param.null_reject_select_idx_.add_member(i))) {
        LOG_WARN("failed to push back select idx", K(ret));
      } else {
        find = true;
      }
    }
    if (OB_SUCC(ret) && find) {
      if (OB_ISNULL(ctx_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid input", K(ret));
      } else if (OB_FAIL(ObTransformUtils::find_not_null_expr(*subquery,
                                                              param.not_null_column_,
                                                              is_valid,
                                                              ctx_))) {
        LOG_WARN("failed to find not null expr", K(ret));
      } else if (is_valid) {
        //find not null column, do nothing
      } else {
        is_valid = false;
        OPT_TRACE("subquery has null propagate select expr, but not found not null column");
      }
    }
  }
  if (OB_SUCC(ret) && is_valid) {
    if (OB_FAIL(queries.push_back(param))) {
      LOG_WARN("failed to push back single set query param", K(ret));
    } else {
      OPT_TRACE("subquery will pullup");
    }
  } else {
    OPT_TRACE("subquery can not pullup");
  }
  return ret;
}

int ObWhereSubQueryPullup::unnest_single_set_subquery(ObDMLStmt *stmt,
                                                      SingleSetParam& param,
                                                      const bool is_vector_assign)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *subquery = NULL;
  ObSEArray<ObRawExpr *, 4> query_refs;
  ObSEArray<ObRawExpr *, 4> select_list;
  ObQueryRefRawExpr *query_expr = param.query_ref_expr_;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->session_info_) ||
      OB_ISNULL(query_expr) || OB_ISNULL(subquery = query_expr->get_ref_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (is_vector_assign) {
    if (OB_UNLIKELY(!stmt->is_update_stmt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("update stmt is expected here", K(ret));
    } else if (OB_FAIL(static_cast<ObUpdateStmt *>(stmt)->get_vector_assign_values(
                         query_expr, query_refs))) {
      LOG_WARN("failed to get vector assign values", K(ret));
    }
  } else {
    if (OB_FAIL(query_refs.push_back(query_expr))) {
      LOG_WARN("failed to push back query refs", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObTransformUtils::decorrelate(subquery, query_expr->get_exec_params()))) {
      LOG_WARN("failed to decorrelate subquery", K(ret));
    } else if (OB_FAIL(subquery->get_select_exprs(select_list))) {
      LOG_WARN("failed to get select exprs", K(ret));
    } else if (OB_UNLIKELY(query_refs.count() != select_list.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("size does not match",
               K(ret), K(query_refs.count()), K(select_list.count()), K(is_vector_assign));
    } else if (param.use_outer_join_ && 
               !param.null_reject_select_idx_.is_empty() && 
               OB_FAIL(wrap_case_when_for_select_expr(param, select_list))) {
      LOG_WARN("failed to wrap case when for select expr", K(ret));
    } else if (OB_FAIL(pull_up_tables_and_columns(stmt, subquery))) {
      LOG_WARN("failed to merge tables to parent stmt", K(ret));
    } else if (OB_FAIL(trans_from_list(stmt, subquery, param.use_outer_join_))) {
      LOG_WARN("failed to transform from list", K(ret));
    } else if (OB_FAIL(append(stmt->get_part_exprs(), subquery->get_part_exprs()))) {
      LOG_WARN("failed to append part expr", K(ret));
    } else if (OB_FAIL(append(stmt->get_check_constraint_items(),
                              subquery->get_check_constraint_items()))) {
      LOG_WARN("failed to append check constraint items", K(ret));
    } else if (OB_FAIL(pull_up_semi_info(stmt, subquery))) {
      LOG_WARN("failed to merge others to parent stmt", K(ret));
    } else if (OB_FAIL(stmt->get_stmt_hint().merge_stmt_hint(subquery->get_stmt_hint(),
                                                             LEFT_HINT_DOMINATED))) {
      LOG_WARN("failed to merge subquery stmt hint", K(ret));
    } else if (OB_FAIL(stmt->replace_relation_exprs(query_refs, select_list))) {
      LOG_WARN("failed to replace inner stmt expr", K(ret));
    } else if (OB_FAIL(stmt->formalize_stmt(ctx_->session_info_))) {
      LOG_WARN("failed to formalize stmt", K(ret));
    }
  }
  return ret;
}


int ObWhereSubQueryPullup::wrap_case_when_for_select_expr(SingleSetParam& param,
                                                         ObIArray<ObRawExpr *> &select_exprs)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *subquery = NULL;
  ObQueryRefRawExpr *query_expr = param.query_ref_expr_;
  if (OB_ISNULL(query_expr) || OB_ISNULL(subquery = query_expr->get_ref_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } 
  for (int64_t i = 0; OB_SUCC(ret) && i < select_exprs.count(); i++) {
    if (!param.null_reject_select_idx_.has_member(i)) {
      //do nothing
    } else if (OB_FAIL(wrap_case_when(*subquery,
                                      param.not_null_column_,
                                      select_exprs.at(i)))) {
      LOG_WARN("failed to wrap case when", K(ret));
    } else {
      //do nothing
    }
  }
  return ret;
}


int ObWhereSubQueryPullup::wrap_case_when(ObSelectStmt &child_stmt,
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
    } else if (OB_FAIL(ObRawExprUtils::try_add_cast_expr_above(
                              ctx_->expr_factory_,
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


int ObWhereSubQueryPullup::pull_up_tables_and_columns(ObDMLStmt *stmt, ObSelectStmt *subquery)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt) || OB_ISNULL(subquery)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL pointer error", K(stmt), K(subquery), K(ret));
  } else if (OB_FAIL(append(stmt->get_table_items(), subquery->get_table_items()))) {
    LOG_WARN("failed to append table items", K(ret));
  } else if (OB_FAIL(append(stmt->get_joined_tables(), subquery->get_joined_tables()))) {
    LOG_WARN("failed to append joined tables", K(ret));
  } else if (OB_FAIL(append(stmt->get_column_items(), subquery->get_column_items()))) {
    LOG_WARN("failed to append column items", K(ret));
  } else if (OB_FAIL(stmt->rebuild_tables_hash())) {
    LOG_WARN("failed to rebuild table hash", K(ret));
  } else if (OB_FAIL(stmt->update_column_item_rel_id())) {
    LOG_WARN("failed to update column item rel id", K(ret));
  }
  return ret;
}

/*
 *如果condition中有和上层相关的table item，需要将其放到semi join的左枝,如:
 *select c1 from t1 where c1 in (select c1 from t2 where c2 >= some(select c2 from t3 where t1.c2=t3.c1));
*/
int ObWhereSubQueryPullup::pull_up_semi_info(ObDMLStmt* stmt,
                                             ObSelectStmt* subquery)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt) || OB_ISNULL(subquery)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(stmt), K(subquery), K(ret));
  }
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
      if (OB_ISNULL(columns.at(j)) || OB_UNLIKELY(!columns.at(j)->is_column_ref_expr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column is null", K(ret));
      } else {
        uint64_t table_id = static_cast<ObColumnRefRawExpr *>(columns.at(j))->get_table_id();
        if (table_id == semi->right_table_id_ ||
            ObOptimizerUtil::find_item(semi->left_table_ids_, table_id)) {
          // do nothing
        } else if (OB_FAIL(semi->left_table_ids_.push_back(table_id))) {
          LOG_WARN("failed to push back left table id", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObWhereSubQueryPullup::trans_from_list(ObDMLStmt *stmt,
                                           ObSelectStmt *subquery,
                                           const bool use_outer_join)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt) || OB_ISNULL(subquery)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(stmt), K(subquery));
  } else if (use_outer_join) {
    TableItem *left_table = NULL;
    TableItem *right_table = NULL;
    TableItem *joined_table = NULL;
    if (OB_FAIL(ObTransformUtils::merge_from_items_as_inner_join(
                  ctx_, *stmt, left_table))) {
      LOG_WARN("failed to merge from items ad inner join", K(ret));
    } else if (OB_FAIL(ObTransformUtils::merge_from_items_as_inner_join(
                         ctx_, *subquery, right_table))) {
      LOG_WARN("failed to merge from items as inner join", K(ret));
    } else if (OB_FAIL(ObTransformUtils::add_new_joined_table(ctx_,
                                                              *stmt,
                                                              LEFT_OUTER_JOIN,
                                                              left_table,
                                                              right_table,
                                                              subquery->get_condition_exprs(),
                                                              joined_table))) {
      LOG_WARN("failed to add new joined table", K(ret));
    } else if (FALSE_IT(stmt->get_from_items().reset())) {
      // do nothing
    } else if (OB_FAIL(stmt->add_from_item(joined_table->table_id_, true))) {
      LOG_WARN("failed to add from items", K(ret));
    }
  } else {
    if (OB_FAIL(append(stmt->get_from_items(), subquery->get_from_items()))) {
      LOG_WARN("failed to append from items", K(ret));
    } else if (OB_FAIL(append(stmt->get_condition_exprs(), subquery->get_condition_exprs()))) {
      LOG_WARN("failed to append condition exprs", K(ret));
    }
  }
  return ret;
}

int ObWhereSubQueryPullup::check_const_select(const ObSelectStmt &stmt,
                                              bool &is_const_select) const
{
  int ret = OB_SUCCESS;
  is_const_select = true;
  for (int64_t i = 0; OB_SUCC(ret) && is_const_select && i < stmt.get_select_item_size(); ++i) {
    if (OB_ISNULL(stmt.get_select_item(i).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is null", K(ret));
    } else {
      is_const_select = stmt.get_select_item(i).expr_->is_const_expr();
    }
  }
  return ret;
}

int ObWhereSubQueryPullup::check_hint_status(const ObDMLStmt &stmt, bool &need_trans)
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
             !cur_trans_hint->is_unnest_hint()) {
    /*do nothing*/
  } else {
    ObQueryRefRawExpr* subquery_expr = nullptr;
    ObSelectStmt* select_stmt = nullptr;
    for (int64_t i = 0; !need_trans && OB_SUCC(ret) && i < stmt.get_subquery_expr_size(); ++i) {
      if (OB_ISNULL(subquery_expr = stmt.get_subquery_exprs().at(i)) ||
          OB_ISNULL(select_stmt = subquery_expr->get_ref_stmt())) {
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

int ObWhereSubQueryPullup::check_hint_allowed_unnest(const ObDMLStmt &stmt,
                                                     const ObSelectStmt &subquery,
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
    // outline data allowed unnest
    allowed = query_hint->is_valid_outline_transform(ctx_->trans_list_loc_, myhint);
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

int ObWhereSubQueryPullup::construct_transform_hint(ObDMLStmt &stmt, void *trans_params)
{
  int ret = OB_SUCCESS;
  ObIArray<ObSelectStmt*> *unnest_stmts = NULL;
  UNUSED(stmt);
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->allocator_) || OB_ISNULL(trans_params)
      || OB_ISNULL(unnest_stmts = static_cast<ObIArray<ObSelectStmt*>*>(trans_params))
      || OB_UNLIKELY(unnest_stmts->empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(ctx_), K(trans_params), K(unnest_stmts));
  } else {
    ObHint *hint = NULL;
    ObDMLStmt *child_stmt = NULL;
    ObString child_qb_name;
    const ObHint *myhint = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < unnest_stmts->count(); ++i) {
      if (OB_ISNULL(child_stmt = unnest_stmts->at(i))) {
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
      } else if (NULL != (myhint = get_hint(child_stmt->get_stmt_hint()))
                 && myhint->is_enable_hint()
                 && OB_FAIL(ctx_->add_used_trans_hint(myhint))) {
        LOG_WARN("failed to add used trans hint", K(ret));
      } else {
        hint->set_qb_name(child_qb_name);
      }
    }
  }
  return ret;
}

/**
 * @brief ObWhereSubQueryPullup::check_can_split
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
int ObWhereSubQueryPullup::check_can_split(ObSelectStmt *subquery,
                                           ObIArray<ObRawExpr*> &semi_conditions,
                                           ObJoinType join_type,
                                           SemiInfoSplitHelper &helper)
{
  int ret = OB_SUCCESS;
  bool can_split = true;
  bool is_contain = false;
  if (OB_ISNULL(subquery) || OB_ISNULL(subquery->get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(subquery));
  } else if (subquery->get_query_ctx()->optimizer_features_enable_version_ < COMPAT_VERSION_4_2_3
             || (subquery->get_query_ctx()->optimizer_features_enable_version_ >= COMPAT_VERSION_4_3_0
                 && subquery->get_query_ctx()->optimizer_features_enable_version_ < COMPAT_VERSION_4_3_2)) {
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
  } else if (OB_FAIL(ObTransformUtils::check_contain_cannot_duplicate_expr(semi_conditions,
                                                                           is_contain))) {
    LOG_WARN("failed to check contain can not duplicate expr", K(ret));
  } else if (is_contain) {
    can_split = false;
    OPT_TRACE("can not split cartesian tables, contain can not duplicate function");
  } else if (OB_FAIL(ObTransformUtils::check_contain_correlated_function_table(subquery,
                                                                               is_contain))) {
    LOG_WARN("failed to check contain correlated function table", K(ret));
  } else if (is_contain) {
    can_split = false;
    OPT_TRACE("can not split cartesian tables, contain correlated function table");
  } else if (OB_FAIL(ObTransformUtils::check_contain_correlated_json_table(subquery,
                                                                           is_contain))) {
    LOG_WARN("failed to check contain correlated json table", K(ret));
  } else if (is_contain) {
    can_split = false;
    OPT_TRACE("can not split cartesian tables, contain correlated json table");
  } else if (OB_FAIL(ObTransformUtils::check_contain_correlated_lateral_table(subquery,
                                                                              is_contain))) {
    LOG_WARN("failed to check contain correlated lateral table", K(ret));
  } else if (is_contain) {
    can_split = false;
    OPT_TRACE("can not split cartesian tables, contain correlated lateral derived table");
  } else if (OB_FAIL(ObTransformUtils::cartesian_tables_pre_split(subquery,
                                    semi_conditions, helper.connected_tables_))){
    LOG_WARN("fail to pre split cartesian tables", K(ret));
  } else if (helper.connected_tables_.count() <= 1) {
    can_split = false;
    OPT_TRACE("can not split cartesian tables, all tables releated");
  }
  if (OB_SUCC(ret)) {
    helper.can_split_ = can_split;
  }
  return ret;
}

}
}
