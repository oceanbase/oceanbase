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
#include "sql/rewrite/ob_transform_semi_to_inner.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "sql/optimizer/ob_log_table_scan.h"
#include "sql/optimizer/ob_log_join.h"
#include "common/ob_smart_call.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;

/**
 * @brief ObTransformSemiToInner::transform_one_stmt
 * @param parent_stmts
 * @param stmt
 * @param trans_happened
 * @return
 */
int ObTransformSemiToInner::transform_one_stmt(
    common::ObIArray<ObParentDMLStmt> &parent_stmts, ObDMLStmt *&stmt, bool &trans_happened)
{
  int ret = OB_SUCCESS;
  ObDMLStmt *root_stmt = NULL;
  bool accepted = false;
  ObSEArray<SemiInfo*, 4> semi_infos;
  ObSEArray<TableItem*, 4> trans_right_table_items;
  ObCostBasedRewriteCtx ctx;
  ObTryTransHelper try_trans_helper;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param has null", K(ret), K(stmt), K(ctx_));
  } else if (OB_FAIL(semi_infos.assign(stmt->get_semi_infos()))) {
    LOG_WARN("failed to assign semi infos", K(ret));
  } else {
    bool cost_based_trans_tried = cost_based_trans_tried_;
    for (int64_t i = 0; OB_SUCC(ret) && i < semi_infos.count(); ++i) {
      SemiInfo *semi_info = semi_infos.at(i);
      TableItem *table_item = NULL;
      ObDMLStmt *trans_stmt = NULL;
      bool need_check_cost = false;
      bool happened = false;
      bool force_trans = false;
      bool force_no_trans = false;
      if (!parent_stmts.empty()) {
        root_stmt = parent_stmts.at(parent_stmts.count()-1).stmt_;
      } else {
        root_stmt = stmt;
      }
      if (OB_ISNULL(semi_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null semi info", K(ret));
      } else if (semi_info->is_anti_join()) {
        //do nothing
      } else if (OB_ISNULL(table_item = stmt->get_table_item_by_id(semi_info->right_table_id_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null table item", K(ret));
      } else if (OB_FAIL(check_hint_valid(*stmt, 
                                          *table_item, 
                                          force_trans,
                                          force_no_trans))) {
        LOG_WARN("failed to check hint valid", K(ret));
      } else if (force_no_trans) {
        //do nothing
      } else if (OB_FALSE_IT(ctx.hint_force_ = force_trans)) {
      } else if (OB_FAIL(try_trans_helper.fill_helper(stmt->get_query_ctx()))) {
        LOG_WARN("failed to fill try trans helper", K(ret));
      } else if (OB_FAIL(transform_semi_to_inner(root_stmt,
                                                stmt,
                                                semi_info,
                                                trans_stmt,
                                                ctx,
                                                need_check_cost,
                                                happened))) {
        LOG_WARN("failed to transform semi join to inner join", K(ret));
      } else if (!happened) {
        LOG_TRACE("semi join can not transform to inner join", K(*semi_info));
      } else if (OB_FAIL(accept_transform(parent_stmts, stmt, trans_stmt,
                                          !need_check_cost || ctx.hint_force_,
                                          accepted, &ctx))) {
        LOG_WARN("failed to accept transform", K(ret));
      } else if (!accepted) {
        if (OB_FAIL(try_trans_helper.recover(stmt->get_query_ctx()))) {
          LOG_WARN("failed to recover params", K(ret));
        } else if (OB_FAIL(add_ignore_semi_info(semi_info->semi_id_))) {
          LOG_WARN("failed to add ignore semi info", K(ret));
        } else {
          LOG_TRACE("semi join can not transform to inner join due to cost", K(*semi_info));
        }
      } else {
        if (!need_check_cost || ctx.hint_force_) {
          cost_based_trans_tried_ = cost_based_trans_tried;
        } else {
          cost_based_trans_tried = cost_based_trans_tried_;
          add_trans_type(ctx_->happened_cost_based_trans_, SEMI_TO_INNER);
        }
        if (OB_FAIL(trans_right_table_items.push_back(table_item))) {
          LOG_WARN("failed to add trans right table item", K(ret));
        } else {
          trans_happened = true;
          LOG_TRACE("succeed to transform one semi join to inner join", K(need_check_cost),
                                      K(*stmt), K(*semi_info));
        }
      }
    }
    if (OB_FAIL(ret) || !trans_happened) {
    } else if (OB_FAIL(add_transform_hint(*stmt, &trans_right_table_items))) {
      LOG_WARN("failed to add transform hint", K(ret));
    } 
  }
  return ret;
}

/**
 * @brief transform_semi_to_inner
 * 基于代价将semi join改写为inner join
 * 规则：
 * 1、如果SEMI JOIN的右表输出唯一时，可以直接改写。
 * 2、如果SEMI JOIN的右表输出不唯一时，需要检查两种情况
 *    semi join左表是否可以生成有效的inner path（连接条件下推match index)
 *    semi join右表是否可以生成有效的inner path（连接条件下推match index)
 */
int ObTransformSemiToInner::transform_semi_to_inner(ObDMLStmt *root_stmt,
                                                    ObDMLStmt *stmt,
                                                    const SemiInfo *pre_semi_info,
                                                    ObDMLStmt *&trans_stmt,
                                                    ObCostBasedRewriteCtx &ctx,
                                                    bool &need_check_cost,
                                                    bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  bool is_valid = false;
  SemiInfo *semi_info = NULL;
  bool need_add_distinct = false;
  bool ignore = false;
  bool need_add_limit_constraint = false;
  bool right_table_need_add_limit = false;
  trans_stmt = NULL;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->stmt_factory_) ||
      OB_ISNULL(ctx_->expr_factory_) || OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("has null param", K(ret));
  } else if (OB_FALSE_IT(semi_info = stmt->get_semi_info_by_id(pre_semi_info->semi_id_))) {
  } else if (NULL == semi_info) {
    /* do nothing */
  } else if (OB_FAIL(check_basic_validity(root_stmt,
                                          *stmt,
                                          *semi_info,
                                          ctx,
                                          is_valid,
                                          need_add_distinct,
                                          need_check_cost,
                                          need_add_limit_constraint,
                                          right_table_need_add_limit))) {
    LOG_WARN("failed to check basic validity", K(ret));
  } else if (!is_valid) {
    //只有确定可以改写后才会去深拷贝stmt
  } else if (!need_check_cost &&
             OB_FALSE_IT(trans_stmt = stmt)) {
    //如果右表不需要添加distinct算子，则基于规则改写，不会考虑代价，所以不需要深拷贝stmt
  } else if (need_check_cost && OB_FAIL(is_ignore_semi_info(pre_semi_info->semi_id_, ignore))) {
    LOG_WARN("failed to check is ignore semi info", K(ret));
  } else if (ignore) {
    LOG_TRACE("semi info has check cost", K(*semi_info));
  } else if (need_check_cost &&
             OB_FAIL(ObTransformUtils::deep_copy_stmt(*ctx_->stmt_factory_,
                                                      *ctx_->expr_factory_,
                                                      stmt,
                                                      trans_stmt))) {
    LOG_WARN("failed to deep copy stmt", K(ret));
  } else if (OB_ISNULL(trans_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  } else if (OB_ISNULL(semi_info = trans_stmt->get_semi_info_by_id(pre_semi_info->semi_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null semi info", K(ret));
  } else if (OB_FAIL(do_transform(*trans_stmt, semi_info, ctx, need_add_distinct,
                                  right_table_need_add_limit))) {
    LOG_WARN("failed to do transform semi to inner", K(ret));
  //Just in case different parameters hit same plan, firstly we need add const param constraint
  } else if (need_add_limit_constraint &&
             OB_FAIL(ObTransformUtils::add_const_param_constraints(stmt->get_limit_expr(),
                                                                   ctx_))) {
    LOG_WARN("failed to add const param constraints", K(ret));
  } else {
    trans_happened = true;
  }
  return ret;
}

/**
 * @brief check_basic_validity
 * semi join转inner join的条件
 * 1. 如果有left_expr = right_column形式的condition，并且
 *   right_column在semi右表的输出是唯一的，这种情况下我们直接转inner，
 *   并且不需要加distinct
 * 2. 如果右表输出不唯一，我们需要检查：
 *   a. 所有的semi condition是否都是left_expr = right_expr形式
 *      如果不是则不能改写。例如select * from l where exists (select 1 from r where l.a > r.b)
 *      不能改写，或者select * from l where exists (select 1 from r where l.a + r.b = r.c)
 *      我们也不能改写
 *   b. 所有的right_expr是否是加distinct类型安全的
 *      如果需要cast(left_expr)-->right_expr,则不能加distinct
 *      如果需要cast(right_expr)-->left_expr,则需要为右表的expr包裹cast后，才能加distinct
 *      如果左右expr的类型一致，直接加distinct
 *   c. semi condtion是否overlap左右表的索引
 *   如果上面的条件都满足，则改写为inner
 * 3.出现在嵌套子查询中的含有semi info信息的stmt，如果子查询的输出结果是否存在重复值不影响上层查询的输出结果，
 *   那么可以直接将semi join改为inner join，不需要添加distinct
 *   eg: select * from T1 where exists (select 1 from T2 where T2.c2 in (select T3.c2 from T3 where T1.c1 = T3.c1));
 */
int ObTransformSemiToInner::check_basic_validity(ObDMLStmt *root_stmt,
                                                 ObDMLStmt &stmt,
                                                 SemiInfo &semi_info,
                                                 ObCostBasedRewriteCtx &ctx,
                                                 bool &is_valid,
                                                 bool &need_add_distinct,
                                                 bool &need_check_cost,
                                                 bool &need_add_limit_constraint,
                                                 bool &right_table_need_add_limit)
{
  int ret = OB_SUCCESS;
  bool is_unique = false;
  bool is_all_equal_cond = false;
  bool is_all_left_filter = false;
  bool is_one_row = false;
  bool is_non_sens_dup_vals = false;
  bool is_multi_join_cond = false;
  TableItem *right_table = NULL;
  ObSEArray<ObRawExpr*, 4> left_exprs;
  ObSEArray<ObRawExpr*, 4> right_exprs;
  is_valid = false;
  need_add_distinct = true;
  need_check_cost = false;
  need_add_limit_constraint = false;
  right_table_need_add_limit = false;
  if (OB_ISNULL(right_table = stmt.get_table_item_by_id(semi_info.right_table_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get table items", K(ret), K(semi_info));
  } else if (OB_FAIL(check_semi_join_condition(stmt, semi_info, left_exprs, right_exprs,
                                               is_all_equal_cond, is_all_left_filter,
                                               is_multi_join_cond))) {
    LOG_WARN("failed to check semi join condition", K(ret));
  } else if (OB_FAIL(check_right_table_output_one_row(*right_table, is_one_row))) {
    LOG_WARN("failed to check right tables output one row", K(ret));
  } else if (is_one_row) {
    is_valid = true;
    need_add_distinct = false;
  } else if (OB_FAIL(ObTransformUtils::check_stmt_is_non_sens_dul_vals(ctx_, root_stmt, &stmt,
                                                                  is_non_sens_dup_vals,
                                                                  need_add_limit_constraint))) {
    LOG_WARN("failed to check stmt is non sens dul vals", K(ret));
  } else if (is_non_sens_dup_vals) {
    is_valid = true;
    need_add_distinct = false;
    LOG_TRACE("stmt isn't sensitive to result of subquery has duplicated values");
  } else if (OB_FAIL(check_right_exprs_unique(stmt, right_table, right_exprs, is_unique))) {
    LOG_WARN("failed to check exprs unique on table items", K(ret));
  } else if (is_unique) {
    is_valid = true;
    need_add_distinct = false;
    LOG_TRACE("semi right table output is unique");
  } else if (is_all_left_filter && (NULL == right_table->ref_query_ ||
                                    NULL == right_table->ref_query_->get_limit_percent_expr())) {
    is_valid = true;
    need_add_distinct = false;
    right_table_need_add_limit = true;
    LOG_TRACE("semi conditions are all left filters");
  } else if (!is_all_equal_cond) {
    is_valid = false;
    LOG_TRACE("semi right table output is not unique and semi condition is not all equal");
  } else if (OB_FAIL(check_can_add_distinct(left_exprs, right_exprs, is_valid))) {
    LOG_WARN("failed to check can add distinct", K(ret));
  } else if (!is_valid) {
    LOG_TRACE("semi right table output can not add distinct");
  } else if (is_multi_join_cond) {
    need_check_cost = true;
    ctx.is_multi_join_cond_ = true;
  } else if (!ctx.hint_force_ &&
             OB_FAIL(check_join_condition_match_index(root_stmt, stmt, semi_info,
                                                      semi_info.semi_conditions_,
                                                      is_valid))) {
    LOG_WARN("failed to check join condition match index", K(ret));
  } else {
    need_check_cost = is_valid | need_add_distinct;
    LOG_TRACE("semi condition match index", K(is_valid), K(need_add_distinct), K(need_check_cost));
  }
  return ret;
}

// check right_exprs is unique on right_table.
// if right_table is generate table, check right_table ref_query unique.
int ObTransformSemiToInner::check_right_exprs_unique(ObDMLStmt &stmt,
                                                     TableItem *right_table,
                                                     ObIArray<ObRawExpr*> &right_exprs,
                                                     bool &is_unique)
{
  int ret = OB_SUCCESS;
  is_unique = false;
  ObSelectStmt *ref_query = NULL;
  if (OB_ISNULL(right_table) || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null ctx", K(ret));
  } else if (!right_table->is_generated_table() && !right_table->is_temp_table()) {
    // baisc table
    ObSEArray<TableItem*, 1> right_tables;
    ObSEArray<ObRawExpr*, 1> dummy_conds;
    if (OB_FAIL(right_tables.push_back(right_table))) {
      LOG_WARN("failed to push back table", K(ret));
    } else if (OB_FAIL(ObTransformUtils::check_exprs_unique_on_table_items(&stmt,
                                                        ctx_->session_info_, ctx_->schema_checker_,
                                                        right_tables, right_exprs, dummy_conds,
                                                        false, is_unique))) {
      LOG_WARN("failed to check exprs unique on table items", K(ret));
    }
  } else if (OB_ISNULL(ref_query = right_table->ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null ref query", K(ret));
  } else {
    ObSEArray<ObRawExpr*, 4> right_cols;
    ObSEArray<ObRawExpr*, 4> right_select_exprs;
    for (int64_t i = 0; OB_SUCC(ret) && i < right_exprs.count(); ++i) {
      if (OB_ISNULL(right_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null expr", K(ret));
      } else if (right_exprs.at(i)->is_column_ref_expr()) {
        ret = right_cols.push_back(right_exprs.at(i));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObTransformUtils::convert_column_expr_to_select_expr(right_cols, *ref_query,
                                                                            right_select_exprs))) {
      LOG_WARN("failed to convert column expr to select expr", K(ret));
    } else if (OB_FAIL(ObTransformUtils::check_stmt_unique(ref_query, ctx_->session_info_,
                                                           ctx_->schema_checker_,
                                                           right_select_exprs, false,
                                                           is_unique))) {
      LOG_WARN("failed to check ref query unique", K(ret));
    }
  }
  return ret;
}

/**
 * @brief check_semi_join_condition
 * 检查semi condition，输出以下结果：
 * left_exprs: 所有EQ表达式的属于左表的expr
 * right_exprs: 所有EQ表达式的属于右表的expr
 * right_columns: 如果有lef_expr = right_column，保存column expr
 * is_all_euqal_cond:是否所有的表达式都是left_expr = right_expr形式
 */
int ObTransformSemiToInner::check_semi_join_condition(ObDMLStmt &stmt,
                                                      SemiInfo &semi_info,
                                                      ObIArray<ObRawExpr*> &left_exprs,
                                                      ObIArray<ObRawExpr*> &right_exprs,
                                                      bool &is_all_equal_cond,
                                                      bool &is_all_left_filter,
                                                      bool &is_multi_join_cond)
{
  int ret = OB_SUCCESS;
  ObSqlBitSet<> left_table_set;
  ObSqlBitSet<> right_table_set;
  ObSqlBitSet<> join_cond_table_ids;
  ObIArray<ObRawExpr*> &semi_conditions = semi_info.semi_conditions_;
  is_all_equal_cond = true;
  is_all_left_filter = true;
  is_multi_join_cond = false;
  if (OB_FAIL(stmt.get_table_rel_ids(semi_info.left_table_ids_, left_table_set))) {
    LOG_WARN("failed to get table rel ids", K(ret));
  } else if (OB_FAIL(stmt.get_table_rel_ids(semi_info.right_table_id_, right_table_set))) {
    LOG_WARN("failed to get table rel ids", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < semi_conditions.count(); ++i) {
    ObRawExpr *expr = semi_conditions.at(i);
    ObRawExpr *left = NULL;
    ObRawExpr *right = NULL;
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null expr", K(ret));
    } else if (left_table_set.is_superset(expr->get_relation_ids())) {
      // do nothing for left filters
    } else if (OB_FALSE_IT(is_all_left_filter = false)) {
    } else if (T_OP_EQ != expr->get_expr_type()) {
      is_all_equal_cond = false;
    } else if (OB_ISNULL(left = expr->get_param_expr(0)) || 
               OB_ISNULL(right = expr->get_param_expr(1))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null param", K(*expr), K(ret));
    } else if (!right_table_set.overlap(left->get_relation_ids()) &&
               !left_table_set.overlap(right->get_relation_ids())) {
      if (OB_FAIL(left_exprs.push_back(left))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else if (OB_FAIL(right_exprs.push_back(right))) {
        LOG_WARN("failed to push back expr", K(ret));
      }
    } else if (!right_table_set.overlap(right->get_relation_ids()) &&
               !left_table_set.overlap(left->get_relation_ids())) {
      if (OB_FAIL(left_exprs.push_back(right))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else if (OB_FAIL(right_exprs.push_back(left))) {
        LOG_WARN("failed to push back expr", K(ret));
      }
    } else {
      is_all_equal_cond = false;
    }
    if (OB_SUCC(ret) && !is_multi_join_cond && expr->get_relation_ids().num_members() > 1) {
      if (join_cond_table_ids.is_empty()) {
        if (OB_FAIL(join_cond_table_ids.add_members(expr->get_relation_ids()))) {
          LOG_WARN("failed to add members", K(ret));
        }
      } else if (!expr->get_relation_ids().equal(join_cond_table_ids)){
        is_multi_join_cond = true;        
      }
    }
  }
  return ret;
}

/**
 * 如果右表有limit 1或者unique_key = const表达式
 * 说明右边输出至多一行
 */
int ObTransformSemiToInner::check_right_table_output_one_row(TableItem &right_table,
                                                             bool &is_one_row)
{
  int ret = OB_SUCCESS;
  is_one_row = false;
  if (right_table.is_generated_table()) {
    ObPCConstParamInfo const_param_info;
    ObPhysicalPlanCtx *plan_ctx = NULL;
    if (OB_ISNULL(right_table.ref_query_) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->exec_ctx_) ||
        OB_ISNULL(plan_ctx = ctx_->exec_ctx_->get_physical_plan_ctx())){
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null param", K(right_table), K(ret));
    } else if (OB_FAIL(ObTransformUtils::check_limit_value(*right_table.ref_query_,
                                                           ctx_->exec_ctx_,
                                                           ctx_->allocator_,
                                                           1,
                                                           is_one_row,
                                                           const_param_info))) {
      LOG_WARN("failed to check limit value", K(ret));
    } else if (!const_param_info.const_idx_.empty() &&
               OB_FAIL(ctx_->plan_const_param_constraints_.push_back(const_param_info))) {
      LOG_WARN("failed to push back const param info", K(ret));
    }
  }
  return ret;
}

int ObTransformSemiToInner::check_can_add_distinct(const ObIArray<ObRawExpr*> &left_exprs,
                                                  const ObIArray<ObRawExpr*> &right_exprs,
                                                  bool &is_valid)
{
  int ret = OB_SUCCESS;
  bool need_add_cast = false;
  is_valid = true;
  if (left_exprs.count() != right_exprs.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect expr count", K(left_exprs), K(right_exprs), K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < left_exprs.count(); ++i) {
    ObRawExpr *left = left_exprs.at(i);
    ObRawExpr *right = right_exprs.at(i);
    if (OB_ISNULL(left) || OB_ISNULL(right)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null expr", K(ret));
    } else if (OB_FAIL(check_need_add_cast(left,
                                          right,
                                          need_add_cast,
                                          is_valid))) {
      LOG_WARN("failed to check need add cast", K(ret));
    }
  }
  return ret;
}

int ObTransformSemiToInner::check_need_add_cast(const ObRawExpr *left_arg,
                                                const ObRawExpr *right_arg,
                                                bool &need_add_cast,
                                                bool &is_valid)
{
  int ret = OB_SUCCESS;
  need_add_cast = false;
  is_valid = false;
  bool is_equal = false;
  if (OB_ISNULL(left_arg) || OB_ISNULL(right_arg)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("left arg and right arg should not be NULL", K(left_arg), K(right_arg), K(ret));
  } else if (OB_FAIL(ObRelationalExprOperator::is_equivalent(left_arg->get_result_type(),
                                                             left_arg->get_result_type(),
                                                             right_arg->get_result_type(),
                                                             is_valid))) {
    LOG_WARN("failed to check expr is equivalent", K(ret));
  } else if (!is_valid) {
    LOG_TRACE("can not use left expr type as the (left, right) compare type", K(is_valid));
  } else if (OB_FAIL(ObRelationalExprOperator::is_equivalent(left_arg->get_result_type(),
                                                             right_arg->get_result_type(),
                                                             right_arg->get_result_type(),
                                                             is_equal))) {
    LOG_WARN("failed to check expr is equivalent", K(ret));
  } else if (!is_equal) {
    need_add_cast = true;
  }
  return ret;
}

int ObTransformSemiToInner::check_join_condition_match_index(ObDMLStmt *root_stmt,
                                                             ObDMLStmt &stmt,
                                                             SemiInfo &semi_info,
                                                             const ObIArray<ObRawExpr*> &semi_conditions,
                                                             bool &is_match_index)
{
  int ret = OB_SUCCESS;
  is_match_index = false;
  if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx is null", K(ret), K(ctx_));
  }
  // check semi condition is match left table
  for (int64_t i = 0; OB_SUCC(ret) && !is_match_index && i < semi_conditions.count(); ++i) {
    ObSEArray<ObRawExpr*, 8> column_exprs;
    if (OB_FAIL(ObRawExprUtils::extract_column_exprs(semi_conditions.at(i), column_exprs))) {
      LOG_WARN("failed to extract column exprs", K(ret));
    }
    for (int64_t j = 0; OB_SUCC(ret) && !is_match_index && j < column_exprs.count(); ++j) {
      ObRawExpr *e = column_exprs.at(j);
      ObColumnRefRawExpr *col_expr = NULL;
      if (OB_ISNULL(e) || OB_UNLIKELY(!e->is_column_ref_expr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null expr", K(ret));
      } else if (OB_FALSE_IT(col_expr = static_cast<ObColumnRefRawExpr*>(e))) {
      } else if (!ObOptimizerUtil::find_item(semi_info.left_table_ids_, col_expr->get_table_id())) {
        // do nothing
      } else if (OB_FAIL(ObTransformUtils::check_column_match_index(root_stmt,
                                                                    &stmt,
                                                                    ctx_->sql_schema_guard_,
                                                                    col_expr,
                                                                    is_match_index))) {
        LOG_WARN("failed to check column expr is match index", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformSemiToInner::do_transform(ObDMLStmt &stmt,
                                         SemiInfo *semi_info,
                                         ObCostBasedRewriteCtx &ctx,
                                         bool need_add_distinct,
                                         bool right_table_need_add_limit)
{
  int ret = OB_SUCCESS;
  TableItem *view_table = NULL;
  ObSelectStmt *ref_query = NULL;
  bool is_all_equal_cond = false;
  bool is_all_left_filter = false;
  bool is_multi_join_cond = false;
  ObSEArray<ObRawExpr*, 4> left_exprs;
  ObSEArray<ObRawExpr*, 4> right_exprs;
  ObSEArray<ObRawExpr *, 2> dummy_filters;
  if (OB_ISNULL(ctx_) || OB_ISNULL(semi_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null ctx", K(ret));
  } else if (need_add_distinct && OB_FAIL(check_semi_join_condition(stmt, *semi_info,
                                                                    left_exprs, right_exprs,
                                                                    is_all_equal_cond,
                                                                    is_all_left_filter,
                                                                    is_multi_join_cond))) {
    LOG_WARN("failed to check semi join condition", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::remove_item(stmt.get_semi_infos(), semi_info))) {
    LOG_WARN("failed to remove semi info", K(ret));
  } else if (OB_FAIL(append(stmt.get_condition_exprs(), semi_info->semi_conditions_))) {
    LOG_WARN("failed to append semi conditions", K(ret));
  } else if (!need_add_distinct) {
    if (OB_FAIL(stmt.add_from_item(semi_info->right_table_id_, false))) {
      LOG_WARN("failed to add from items", K(ret));
    } else if (!right_table_need_add_limit) {
      /* do nothing */
    } else if (OB_FAIL(ObTransformUtils::add_limit_to_semi_right_table(&stmt, ctx_, semi_info))) {
      LOG_WARN("failed to add limit to semi right table", K(ret), K(stmt));
    }
  } else if (OB_FAIL(ObTransformUtils::create_view_with_from_items(&stmt, ctx_,
                                              stmt.get_table_item_by_id(semi_info->right_table_id_),
                                              right_exprs, dummy_filters, view_table))) {
    LOG_WARN("failed to merge from items as inner join", K(ret));
  } else if (OB_ISNULL(view_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null table item", K(ret));
  } else if (!view_table->is_generated_table()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expect generated table item", K(*view_table), K(ret));
  } else if (OB_ISNULL(ref_query = view_table->ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null ref query", K(ret));
  } else if (OB_FAIL(add_distinct(*ref_query, left_exprs, right_exprs))) {
    LOG_WARN("failed to add distinct exprs", K(ret));
  } else if (OB_FAIL(ref_query->add_from_item(semi_info->right_table_id_, false))) {
    LOG_WARN("failed to add from items", K(ret));
  } else if (OB_FAIL(stmt.add_from_item(view_table->table_id_, false))) {
    LOG_WARN("failed to add from items", K(ret));
  } else if (OB_FAIL(find_basic_table(ref_query, ctx.table_id_))) {
    LOG_WARN("failed to find basic table", K(ret));
  } else {
    ctx.view_table_id_ = view_table->table_id_;
  }
  return ret;
}

int ObTransformSemiToInner::find_basic_table(ObSelectStmt* stmt, uint64_t &table_id)
{
  int ret = OB_SUCCESS;
  bool find = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && !find && i < stmt->get_table_items().count(); ++i) {
    TableItem *table = stmt->get_table_item(i);
    if (OB_ISNULL(table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null table item", K(ret));
    } else if (!table->is_generated_table()) {
      table_id = table->table_id_;
      find = true;
    } else if (OB_FAIL(SMART_CALL(find_basic_table(table->ref_query_, table_id)))) {
      LOG_WARN("failed to find basic table item", K(ret));
    }
  }
  return ret;
}

int ObTransformSemiToInner::add_distinct(ObSelectStmt &view,
                                         const ObIArray<ObRawExpr*> &left_exprs,
                                         const ObIArray<ObRawExpr*> &right_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null ctx", K(ret));
  } else if (left_exprs.count() != right_exprs.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect expr count", K(left_exprs), K(right_exprs), K(ret));
  }
  view.assign_distinct();
  for (int64_t i = 0; OB_SUCC(ret) && i < view.get_select_item_size(); ++i) {
    SelectItem &item = view.get_select_item(i);
    ObRawExpr *left = NULL;
    ObRawExpr *right = NULL;
    ObSysFunRawExpr *cast_expr = NULL;
    bool need_add_cast = false;
    for (int64_t j = 0; OB_SUCC(ret) && !need_add_cast && j < left_exprs.count(); ++j) {
      left = left_exprs.at(j);
      right = right_exprs.at(j);
      bool is_valid = false;
      if (OB_ISNULL(left) || OB_ISNULL(right)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null expr", K(ret));
      } else if (right != item.expr_) {
        //do nothing
      } else if (OB_FAIL(check_need_add_cast(left,
                                            right,
                                            need_add_cast,
                                            is_valid))) {
        LOG_WARN("failed to check need add cast", K(ret));
      } else if (!is_valid) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expect valid cast expr", K(ret));
      } else if (need_add_cast) {
        LOG_TRACE("need cast expr", K(*left), K(*right));
      }
    }
    if (OB_FAIL(ret)) {
      //do nothing
    } else if (!need_add_cast) {
      //do nothing
    } else if (OB_ISNULL(left) || OB_ISNULL(right)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::create_cast_expr(*ctx_->expr_factory_,
                                                        right,
                                                        left->get_result_type(),
                                                        cast_expr,
                                                        ctx_->session_info_))) {
      LOG_WARN("failed to create cast expr", K(ret));
    } else {
      item.expr_ = cast_expr;
    }
  }
  return ret;
}

int ObTransformSemiToInner::add_ignore_semi_info(const uint64_t semi_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx has null param", K(ret));
  } else if (OB_FAIL(ctx_->ignore_semi_infos_.push_back(semi_id))) {
    LOG_WARN("failed to push back ignore semi info", K(ret));
  }
  return ret;
}

int ObTransformSemiToInner::is_ignore_semi_info(const uint64_t semi_id, bool &ignore)
{
  int ret = OB_SUCCESS;
  ignore = false;
  if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx has null param", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && !ignore && i < ctx_->ignore_semi_infos_.count(); ++i) {
    if (ctx_->ignore_semi_infos_.at(i) == semi_id) {
      ignore = true;    
    }
  }
  return ret;
}

int ObTransformSemiToInner::is_expected_plan(ObLogPlan *plan, void *check_ctx, bool &is_valid)
{
  int ret = OB_SUCCESS;
  ObCostBasedRewriteCtx *ctx = static_cast<ObCostBasedRewriteCtx *>(check_ctx);
  ObSEArray<ObLogicalOperator*, 4> parents;
  ObLogicalOperator* table_op = NULL;
  is_valid = false;
  if (OB_ISNULL(ctx) || OB_ISNULL(plan)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null param", K(ret));
  } else if (ctx->is_multi_join_cond_) {
    is_valid = true;
  } else if (OB_FAIL(find_operator(plan->get_plan_root(), 
                                   parents,
                                   ctx->table_id_,
                                   table_op))) {
    LOG_WARN("failed to get join operator", K(ret));
  } else if (NULL == table_op || parents.empty()) {
    //do nothing
  } else {
    ObLogicalOperator *child = table_op;
    ObLogicalOperator *parent = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && !is_valid && i < parents.count(); child = parent, ++i) {
      parent = parents.at(i);
      if (OB_ISNULL(parent)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null operator", K(ret));
      } else if (log_op_def::LOG_JOIN == parent->get_type()) {
        ObLogJoin *join_op = static_cast<ObLogJoin*>(parent);
        //After semi to inner, it needs to be used as a driving table and 
        //semi condition generates a conditional down pressure path
        if (!join_op->is_nlj_with_param_down() || 
            child != join_op->get_left_table()) {
          //do nothing
        } else if (OB_FAIL(check_is_semi_condition(join_op->get_nl_params(),
                                                   ctx->view_table_id_,
                                                   is_valid))) {
          LOG_WARN("failed to check is semi condition", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTransformSemiToInner::find_operator(ObLogicalOperator* root,
                                          ObIArray<ObLogicalOperator*> &parents,
                                          uint64_t table_id,
                                          ObLogicalOperator *&table_op)
{
  int ret = OB_SUCCESS;
  table_op = NULL;
  if (OB_ISNULL(root)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null logical operator", K(ret));
  } else if (log_op_def::LOG_TABLE_SCAN == root->get_type()) {
    ObLogTableScan *scan = static_cast<ObLogTableScan *>(root);
    if (scan->get_table_id() == table_id) {
      table_op = scan;
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && NULL == table_op && i < root->get_num_of_child(); ++i) {
      ObLogicalOperator *child = root->get_child(i);
      if (OB_FAIL(SMART_CALL(find_operator(child, parents, table_id, table_op)))) {
        LOG_WARN("failed to find operator", K(ret));
      } else if (NULL == table_op) {
        //do nothing
      } else if (OB_FAIL(parents.push_back(root))) {
        LOG_WARN("failed to push back operator", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformSemiToInner::check_is_semi_condition(ObIArray<ObExecParamRawExpr *> &nl_params,
                                                    uint64_t table_id,
                                                    bool &is_valid)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> param_exprs;
  ObBitSet<> column_ids;
  is_valid = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < nl_params.count(); ++i) {
    if (OB_ISNULL(nl_params.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("nl param is null", K(ret));
    } else if (OB_FAIL(param_exprs.push_back(nl_params.at(i)->get_ref_expr()))) {
      LOG_WARN("failed to push back expr", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObOptimizerUtil::extract_column_ids(param_exprs, table_id, column_ids))) {
      LOG_WARN("failed to extract colulmn ids", K(ret));
    } else if (!column_ids.is_empty()) {
      is_valid = true;
    }
  }
  return ret;
}

int ObTransformSemiToInner::construct_transform_hint(ObDMLStmt &stmt, void *trans_params)
{
  int ret = OB_SUCCESS;
  ObSemiToInnerHint *hint = NULL;
  ObIArray<TableItem*> *trans_right_table_items = NULL;
  const ObQueryHint *query_hint = NULL;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->allocator_) ||
      OB_ISNULL(query_hint = stmt.get_stmt_hint().query_hint_) ||
      OB_ISNULL(trans_right_table_items = static_cast<ObIArray<TableItem*> *>(trans_params))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(ctx_), K(query_hint));
  } else if (OB_FAIL(ObQueryHint::create_hint(ctx_->allocator_, T_SEMI_TO_INNER, hint))) {
    LOG_WARN("failed to create hint", K(ret));
  } else {
    TableItem *table_item = NULL;
    ObTableInHint table_hint;
    bool use_hint = false;
    const ObSemiToInnerHint *myhint = static_cast<const ObSemiToInnerHint*>(get_hint(stmt.get_stmt_hint()));
    for (int64_t i = 0; OB_SUCC(ret) && i < trans_right_table_items->count(); ++i) {
      if (OB_ISNULL(table_item = trans_right_table_items->at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret), K(table_item));
      } else if (OB_FALSE_IT(table_hint.set_table(*table_item))) {
      } else if (OB_FAIL(hint->get_tables().push_back(table_hint))) {
        LOG_WARN("failed to push back table hint", K(ret));
      } else if (OB_FAIL(ctx_->add_src_hash_val(table_item->get_table_name()))) {
        LOG_WARN("failed to add src hash val", K(ret));
      } else if (NULL != myhint && myhint->enable_semi_to_inner(query_hint->cs_type_, *table_item)) {
        use_hint = true;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ctx_->outline_trans_hints_.push_back(hint))) {
      LOG_WARN("failed to push back hint", K(ret));
    } else if (use_hint && OB_FAIL(ctx_->add_used_trans_hint(myhint))) {
      LOG_WARN("failed to add used trans hint", K(ret));
    } else {
      hint->set_qb_name(ctx_->src_qb_name_);
    }
  }
  return ret;
}

int ObTransformSemiToInner::check_hint_valid(const ObDMLStmt &stmt, 
                                            const TableItem &table,
                                            bool &force_trans,
                                            bool &force_no_trans) const
{
  int ret = OB_SUCCESS;
  force_trans = false;
  force_no_trans = false;
  const ObQueryHint *query_hint = NULL;
  const ObSemiToInnerHint *myhint = static_cast<const ObSemiToInnerHint*>(get_hint(stmt.get_stmt_hint()));
  if (OB_ISNULL(query_hint = stmt.get_stmt_hint().query_hint_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(query_hint));
  } else {
    force_trans = NULL != myhint && myhint->enable_semi_to_inner(query_hint->cs_type_, table);
    force_no_trans = !force_trans && query_hint->has_outline_data();
  }
  return ret;
}