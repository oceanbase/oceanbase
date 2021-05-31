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
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/rewrite/ob_transform_subquery_coalesce.h"
#include "sql/rewrite/ob_stmt_comparer.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/ob_sql_context.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

int ObTransformSubqueryCoalesce::transform_one_stmt(
    common::ObIArray<ObParentDMLStmt>& parent_stmts, ObDMLStmt*& stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  bool is_happened = false;
  trans_happened = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt is NULL", K(ret));
  } else if (OB_FAIL(transform_same_exprs(stmt, stmt->get_condition_exprs(), trans_happened))) {
    LOG_WARN("failed to transform where conditions", K(ret));
  } else if (!stmt->is_select_stmt()) {
    // do nothing
  } else if (OB_FAIL(transform_same_exprs(stmt, static_cast<ObSelectStmt*>(stmt)->get_having_exprs(), is_happened))) {
    LOG_WARN("failed to transform same exprs", K(ret));
  } else {
    trans_happened = trans_happened || is_happened;
  }
  if (OB_SUCC(ret)) {
    ObSEArray<ObPCParamEqualInfo, 4> rule_based_equal_infos;
    ObSEArray<ObPCParamEqualInfo, 4> cost_based_equal_infos;
    do {
      is_happened = false;
      ObDMLStmt* trans_stmt = NULL;
      rule_based_equal_infos.reset();
      cost_based_equal_infos.reset();
      bool rule_based_trans_happened = false;
      if (OB_FAIL(transform_diff_exprs(
              stmt, trans_stmt, rule_based_equal_infos, cost_based_equal_infos, rule_based_trans_happened))) {
        LOG_WARN("failed to transform exprs", K(ret));
      } else if (OB_FAIL(append(stmt->get_query_ctx()->all_equal_param_constraints_, rule_based_equal_infos))) {
        LOG_WARN("failed to append equal infos", K(ret));
      } else if (OB_ISNULL(trans_stmt)) {
        // do nothing
      } else if (OB_FAIL(accept_transform(parent_stmts, stmt, trans_stmt, is_happened))) {
        LOG_WARN("failed to accept transform", K(ret));
      } else if (!is_happened) {
        // do nothing
      } else if (OB_FAIL(append(stmt->get_query_ctx()->all_equal_param_constraints_, cost_based_equal_infos))) {
        LOG_WARN("failed to append equal infos", K(ret));
      }
      if (OB_SUCC(ret)) {
        trans_happened = trans_happened || rule_based_trans_happened || is_happened;
      }
    } while (OB_SUCC(ret) && is_happened);
  }
  return ret;
}

int ObTransformSubqueryCoalesce::transform_same_exprs(ObDMLStmt* stmt, ObIArray<ObRawExpr*>& conds, bool& is_happened)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> copy_exprs;
  ObSEArray<ObRawExpr*, 4> same_exprs;
  ObSEArray<ObRawExpr*, 4> same_all_exprs;
  ObSEArray<ObRawExpr*, 4> validity_exprs;
  ObSEArray<ObRawExpr*, 4> remove_exprs;
  is_happened = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (OB_FAIL(classify_conditions(conds, validity_exprs))) {
    LOG_WARN("failed to check conditions validity", K(ret));
  } else {
    bool coalesce_happened = false;
    bool happened = false;
    bool all_happened = false;
    ObItemType item_type[8] = {
        T_OP_EXISTS, T_OP_NOT_EXISTS, T_OP_SQ_EQ, T_OP_SQ_NE, T_OP_SQ_GT, T_OP_SQ_GE, T_OP_SQ_LT, T_OP_SQ_LE};
    for (int32_t i = 0; OB_SUCC(ret) && i < 8; ++i) {
      same_all_exprs.reset();
      same_exprs.reset();
      if (item_type[i] == T_OP_EXISTS || item_type[i] == T_OP_NOT_EXISTS) {
        if (OB_FAIL(get_same_classify_exprs(validity_exprs, same_exprs, item_type[i], CNT_SUB_QUERY))) {
          LOG_WARN("get the same classify exprs failed", K(ret));
        } else if (OB_FAIL(copy_exprs.assign(same_exprs))) {
          LOG_WARN("failed to assign exprs", K(ret));
        } else if (OB_FAIL(coalesce_same_exists_exprs(stmt, item_type[i], same_exprs, happened))) {
          LOG_WARN("failed to coalesce exists exprs", K(ret));
        } else if (OB_FAIL(get_remove_exprs(copy_exprs, same_exprs, remove_exprs))) {
          LOG_WARN("failed to get remove exprs", K(ret));
        } else {
          coalesce_happened |= happened;
        }
      } else if (OB_FAIL(get_same_classify_exprs(validity_exprs, same_exprs, item_type[i], IS_WITH_ANY))) {
        LOG_WARN("get the same classify exprs failed", K(ret));
      } else if (OB_FAIL(copy_exprs.assign(same_exprs))) {
        LOG_WARN("failed to assign exprs", K(ret));
      } else if (OB_FAIL(coalesce_same_any_all_exprs(stmt, T_ANY, same_exprs, happened))) {
        LOG_WARN("failed to coalesce any exprs", K(ret));
      } else if (OB_FAIL(get_remove_exprs(copy_exprs, same_exprs, remove_exprs))) {
        LOG_WARN("failed to get remove exprs", K(ret));
      } else if (OB_FAIL(get_same_classify_exprs(validity_exprs, same_all_exprs, item_type[i], IS_WITH_ALL))) {
        LOG_WARN("get the same classify exprs failed", K(ret));
      } else if (OB_FAIL(copy_exprs.assign(same_all_exprs))) {
        LOG_WARN("failed to assign exprs", K(ret));
      } else if (OB_FAIL(coalesce_same_any_all_exprs(stmt, T_ALL, same_all_exprs, all_happened))) {
        LOG_WARN("failed to coalesce all exprs", K(ret));
      } else if (OB_FAIL(get_remove_exprs(copy_exprs, same_all_exprs, remove_exprs))) {
        LOG_WARN("failed to get remove exprs", K(ret));
      } else {
        coalesce_happened = coalesce_happened | happened | all_happened;
      }
    }
    if (OB_SUCC(ret) && coalesce_happened) {
      if (OB_FAIL(ObOptimizerUtil::remove_item(conds, remove_exprs))) {
        LOG_WARN("failed to remove exprs", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformSubqueryCoalesce::get_same_classify_exprs(ObIArray<ObRawExpr*>& validity_exprs,
    ObIArray<ObRawExpr*>& same_classify_exprs, ObItemType ctype, ObExprInfoFlag flag)
{
  int ret = OB_SUCCESS;
  ObRawExpr* expr = NULL;
  for (int32_t i = 0; OB_SUCC(ret) && i < validity_exprs.count(); ++i) {
    expr = validity_exprs.at(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is null", K(ret));
    } else if (expr->has_flag(flag) && expr->get_expr_type() == ctype && OB_FAIL(same_classify_exprs.push_back(expr))) {
      LOG_WARN("failed to push back same classify exprs", K(ret));
    } else {
      /*do nothing*/
    }
  }
  return ret;
}

int ObTransformSubqueryCoalesce::adjust_transform_types(uint64_t& transform_types)
{
  int ret = OB_SUCCESS;
  if (cost_based_trans_tried_) {
    transform_types &= (~transformer_type_);
  }
  return ret;
}

int ObTransformSubqueryCoalesce::classify_conditions(
    ObIArray<ObRawExpr*>& conditions, ObIArray<ObRawExpr*>& validity_exprs)
{
  int ret = OB_SUCCESS;
  ObOpRawExpr* op = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < conditions.count(); ++i) {
    ObRawExpr* cond = NULL;
    bool is_valid = false;
    if (OB_ISNULL(cond = conditions.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is null", K(ret), K(i));
    } else if (cond->get_expr_type() != T_OP_EXISTS && cond->get_expr_type() != T_OP_NOT_EXISTS &&
               !cond->has_flag(IS_WITH_ANY) && !cond->has_flag(IS_WITH_ALL)) {
      // do nothing
    } else if ((cond->get_expr_type() == T_OP_EXISTS || cond->get_expr_type() == T_OP_NOT_EXISTS) &&
               OB_FAIL(check_query_ref_validity(cond->get_param_expr(0), is_valid))) {
      LOG_WARN("failed to check query ref validity", K(ret));
    } else if (is_valid) {
      ret = validity_exprs.push_back(cond);
    } else if ((cond->has_flag(IS_WITH_ANY) || cond->has_flag(IS_WITH_ALL)) &&
               OB_FAIL(check_query_ref_validity(cond->get_param_expr(1), is_valid))) {
      LOG_WARN("failed to check query ref validity", K(ret));
    } else if (is_valid) {
      ret = validity_exprs.push_back(cond);
    } else {
      // do nothing
    }
  }
  return ret;
}

int ObTransformSubqueryCoalesce::get_remove_exprs(
    ObIArray<ObRawExpr*>& ori_exprs, ObIArray<ObRawExpr*>& remain_exprs, ObIArray<ObRawExpr*>& remove_exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < ori_exprs.count(); ++i) {
    if (ObOptimizerUtil::find_item(remain_exprs, ori_exprs.at(i))) {
      // do nothing
    } else if (OB_FAIL(remove_exprs.push_back(ori_exprs.at(i)))) {
      LOG_WARN("failed to push back first expr", K(ret));
    }
  }
  return ret;
}

int ObTransformSubqueryCoalesce::check_query_ref_validity(ObRawExpr* expr, bool& is_valid)
{
  int ret = OB_SUCCESS;
  ObQueryRefRawExpr* query_ref = NULL;
  is_valid = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret), K(expr));
  } else if (expr->is_query_ref_expr()) {
    ObSelectStmt* sub_stmt = NULL;
    query_ref = static_cast<ObQueryRefRawExpr*>(expr);
    if (OB_ISNULL(sub_stmt = query_ref->get_ref_stmt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("query ref is null", K(ret));
    } else if (sub_stmt->is_spj() && sub_stmt->get_semi_infos().empty() && sub_stmt->get_subquery_exprs().empty()) {
      is_valid = true;
    }
  }
  return ret;
}

int ObTransformSubqueryCoalesce::coalesce_same_exists_exprs(
    ObDMLStmt* stmt, const ObItemType type, ObIArray<ObRawExpr*>& filters, bool& is_happened)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObPCParamEqualInfo, 4> equal_infos;
  ObStmtMapInfo map_info;
  QueryRelation relation;
  ObQueryRefRawExpr* first_query_ref = NULL;
  ObQueryRefRawExpr* second_query_ref = NULL;
  ObSqlBitSet<> removed_items;
  int64_t remove_index = -1;
  is_happened = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < filters.count(); ++i) {
    first_query_ref = get_exists_query_expr(filters.at(i));
    for (int64_t j = i + 1; OB_SUCC(ret) && !removed_items.has_member(i) && j < filters.count(); ++j) {
      map_info.reset();
      remove_index = -1;
      second_query_ref = get_exists_query_expr(filters.at(j));
      if (removed_items.has_member(j)) {
        // do nothing
      } else if (OB_ISNULL(first_query_ref) || OB_ISNULL(second_query_ref)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("query ref is invalid", K(ret));
      } else if (OB_FAIL(ObStmtComparer::check_stmt_containment(
                     first_query_ref->get_ref_stmt(), second_query_ref->get_ref_stmt(), map_info, relation))) {
        LOG_WARN("failed to check stmt containment", K(ret));
      } else if (relation == QueryRelation::LEFT_SUBSET || relation == QueryRelation::EQUAL) {
        remove_index = (type == T_OP_EXISTS ? j : i);
      } else if (relation == QueryRelation::RIGHT_SUBSET) {
        remove_index = (type == T_OP_EXISTS ? i : j);
      }
      if (OB_SUCC(ret) && remove_index != -1) {
        if (OB_FAIL(removed_items.add_member(remove_index))) {
          LOG_WARN("failed to add member", K(ret));
        } else if (OB_FAIL(append(equal_infos, map_info.equal_param_map_))) {
          LOG_WARN("failed to append equal param map", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret) && (!removed_items.is_empty())) {
    ObSEArray<ObRawExpr*, 4> new_filters;
    for (int64_t i = 0; OB_SUCC(ret) && i < filters.count(); ++i) {
      if (!removed_items.has_member(i)) {
        // the filter is not removed
        if (OB_FAIL(new_filters.push_back(filters.at(i)))) {
          LOG_WARN("failed to push back filter", K(ret));
        }
      } else if (OB_FAIL(
                     ObOptimizerUtil::remove_item(stmt->get_subquery_exprs(), get_exists_query_expr(filters.at(i))))) {
        LOG_WARN("failed to remove subquery expr", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(filters.assign(new_filters))) {
        LOG_WARN("failed to assign new filters", K(ret));
      } else if (OB_FAIL(append(stmt->get_query_ctx()->all_equal_param_constraints_, equal_infos))) {
        LOG_WARN("failed to append equal infos", K(ret));
      } else {
        is_happened = true;
      }
    }
  }
  return ret;
}

int ObTransformSubqueryCoalesce::coalesce_same_any_all_exprs(
    ObDMLStmt* stmt, const ObItemType type, ObIArray<ObRawExpr*>& filters, bool& is_happened)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObPCParamEqualInfo, 4> equal_infos;
  ObStmtMapInfo map_info;
  QueryRelation relation;
  ObQueryRefRawExpr* first_query_ref = NULL;
  ObRawExpr* first_left_expr = NULL;
  ObQueryRefRawExpr* second_query_ref = NULL;
  ObRawExpr* second_left_expr = NULL;
  ObSqlBitSet<> removed_items;
  int64_t remove_index = -1;
  is_happened = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < filters.count(); ++i) {
      first_left_expr = get_any_all_left_hand_expr(filters.at(i));
      first_query_ref = get_any_all_query_expr(filters.at(i));
      for (int64_t j = i + 1; OB_SUCC(ret) && !removed_items.has_member(i) && j < filters.count(); ++j) {
        second_left_expr = get_any_all_left_hand_expr(filters.at(j));
        second_query_ref = get_any_all_query_expr(filters.at(j));
        map_info.reset();
        remove_index = -1;
        if (removed_items.has_member(j)) {
          // do nothing
        } else if (OB_ISNULL(first_left_expr) || OB_ISNULL(first_query_ref) || OB_ISNULL(second_left_expr) ||
                   OB_ISNULL(second_query_ref)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("query ref is invalid", K(ret));
        } else if (!first_left_expr->same_as(*second_left_expr)) {
          /*do nothing*/
        } else if (OB_FAIL(ObStmtComparer::check_stmt_containment(
                       first_query_ref->get_ref_stmt(), second_query_ref->get_ref_stmt(), map_info, relation))) {
          LOG_WARN("failed to check stmt containment", K(ret));
        } else if (relation == QueryRelation::LEFT_SUBSET || relation == QueryRelation::EQUAL) {
          remove_index = (type == T_ANY ? j : i);
        } else if (relation == QueryRelation::RIGHT_SUBSET) {
          remove_index = (type == T_ANY ? i : j);
        }
        if (OB_SUCC(ret) && remove_index != -1) {
          if (OB_FAIL(removed_items.add_member(remove_index))) {
            LOG_WARN("failed to add member", K(ret));
          } else if (OB_FAIL(append(equal_infos, map_info.equal_param_map_))) {
            LOG_WARN("failed to append equal param map", K(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret) && (!removed_items.is_empty())) {
      ObSEArray<ObRawExpr*, 4> new_filters;
      ObOpRawExpr* temp_op = NULL;
      for (int64_t i = 0; OB_SUCC(ret) && i < filters.count(); ++i) {
        temp_op = static_cast<ObOpRawExpr*>(filters.at(i));
        if (!removed_items.has_member(i)) {
          // the filter is not removed
          if (OB_FAIL(new_filters.push_back(filters.at(i)))) {
            LOG_WARN("failed to push back filter", K(ret));
          }
        } else if (OB_FAIL(ObOptimizerUtil::remove_item(
                       stmt->get_subquery_exprs(), static_cast<ObQueryRefRawExpr*>(temp_op->get_param_expr(1))))) {
          LOG_WARN("failed to remove subquery expr", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(filters.assign(new_filters))) {
          LOG_WARN("failed to assign new filters", K(ret));
        } else if (OB_FAIL(append(stmt->get_query_ctx()->all_equal_param_constraints_, equal_infos))) {
          LOG_WARN("failed to append equal infos", K(ret));
        } else {
          is_happened = true;
        }
      }
    }
  }
  return ret;
}

int ObTransformSubqueryCoalesce::transform_diff_exprs(ObDMLStmt* stmt, ObDMLStmt*& trans_stmt,
    ObIArray<ObPCParamEqualInfo>& rule_based_equal_infos, ObIArray<ObPCParamEqualInfo>& cost_based_equal_infos,
    bool& rule_based_trans_happened)
{
  int ret = OB_SUCCESS;
  ObSEArray<TransformParam, 4> where_params;
  ObSEArray<TransformParam, 4> having_params;
  bool where_is_false = false;
  bool having_is_false = false;
  ObSelectStmt* select_stmt = NULL;
  ObSelectStmt* select_trans_stmt = NULL;
  rule_based_trans_happened = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->stmt_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(stmt), K(ctx_));
  } else if (stmt->is_select_stmt() && OB_ISNULL(select_stmt = static_cast<ObSelectStmt*>(stmt))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(stmt), K(select_stmt));
  } else if (OB_FAIL(check_conditions_validity(stmt->get_condition_exprs(), where_params, where_is_false))) {
    LOG_WARN("failed to check condition validity", K(ret));
  } else if (OB_NOT_NULL(select_stmt) &&
             OB_FAIL(check_conditions_validity(select_stmt->get_having_exprs(), having_params, having_is_false))) {
    LOG_WARN("failed to check having validity", K(ret));
  } else if (where_is_false && OB_FAIL(make_false(stmt->get_condition_exprs()))) {
    LOG_WARN("failed to make condition false", K(ret));
  } else if (having_is_false && OB_FAIL(make_false(select_stmt->get_having_exprs()))) {
    LOG_WARN("failed to make condition false", K(ret));
  } else if ((where_is_false || where_params.empty()) && (having_is_false || having_params.empty())) {
    // do nothing
  } else if (OB_FAIL(ObTransformUtils::copy_stmt(*ctx_->stmt_factory_, stmt, trans_stmt))) {
    LOG_WARN("failed to copy stmt", K(ret));
  } else if (OB_FAIL(trans_stmt->adjust_statement_id())) {
    LOG_WARN("failed to adjust stmt id", K(ret));
  } else if (trans_stmt->is_select_stmt() && OB_ISNULL(select_trans_stmt = static_cast<ObSelectStmt*>(trans_stmt))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(trans_stmt), K(select_trans_stmt));
  } else if (!where_is_false &&
             OB_FAIL(coalesce_diff_exists_exprs(trans_stmt, trans_stmt->get_condition_exprs(), where_params))) {
    LOG_WARN("failed to do coalesce diff where conditions", K(ret));
  } else if (!where_is_false &&
             OB_FAIL(coalesce_diff_any_all_exprs(trans_stmt, trans_stmt->get_condition_exprs(), where_params))) {
    LOG_WARN("failed to do coalesce diff where conditions", K(ret));
  } else if (OB_NOT_NULL(select_trans_stmt) && !having_is_false &&
             OB_FAIL(coalesce_diff_any_all_exprs(trans_stmt, select_trans_stmt->get_having_exprs(), having_params))) {
    LOG_WARN("failed to do coalesce diff having conditions", K(ret));
  }
  if (OB_SUCC(ret)) {
    rule_based_trans_happened = where_is_false || having_is_false;
    ObIArray<ObPCParamEqualInfo>* where_equal_infos =
        where_is_false ? &rule_based_equal_infos : &cost_based_equal_infos;
    ObIArray<ObPCParamEqualInfo>* having_equal_infos =
        having_is_false ? &rule_based_equal_infos : &cost_based_equal_infos;
    for (int64_t i = 0; OB_SUCC(ret) && i < where_params.count(); ++i) {
      if (OB_FAIL(append(*where_equal_infos, where_params.at(i).map_info_.equal_param_map_))) {
        LOG_WARN("failed to append equal infos", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < having_params.count(); ++i) {
      if (OB_FAIL(append(*having_equal_infos, having_params.at(i).map_info_.equal_param_map_))) {
        LOG_WARN("failed to append equal infos", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformSubqueryCoalesce::check_conditions_validity(
    ObIArray<ObRawExpr*>& conds, ObIArray<TransformParam>& trans_params, bool& has_false_conds)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> validity_exprs;
  TransformParam param;
  ObSqlBitSet<> removed;
  QueryRelation relation = QueryRelation::UNCOMPARABLE;
  has_false_conds = false;
  bool is_used = false;
  if (OB_FAIL(classify_conditions(conds, validity_exprs))) {
    LOG_WARN("failed to check conditions validity", K(ret));
  } else {
    ObSEArray<ObRawExpr*, 4> left_exprs;
    ObSEArray<ObRawExpr*, 4> right_exprs;
    ObItemType left_type[7] = {T_OP_EXISTS, T_OP_SQ_EQ, T_OP_SQ_NE, T_OP_SQ_LT, T_OP_SQ_LE, T_OP_SQ_GT, T_OP_SQ_GE};
    ObItemType right_type[7] = {
        T_OP_NOT_EXISTS, T_OP_SQ_NE, T_OP_SQ_EQ, T_OP_SQ_GE, T_OP_SQ_GT, T_OP_SQ_LE, T_OP_SQ_LT};
    for (int64_t k = 0; OB_SUCC(ret) && !has_false_conds && k < 7; ++k) {
      left_exprs.reset();
      right_exprs.reset();
      if (T_OP_EXISTS == left_type[k]) {  // exists
        if (OB_FAIL(get_same_classify_exprs(validity_exprs, left_exprs, left_type[k], CNT_SUB_QUERY))) {
          LOG_WARN("get the same classify exprs failed", K(ret));
        } else if (OB_FAIL(get_same_classify_exprs(validity_exprs, right_exprs, right_type[k], CNT_SUB_QUERY))) {
          LOG_WARN("get the same classify exprs failed", K(ret));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && !has_false_conds && i < left_exprs.count(); ++i) {
            ObQueryRefRawExpr* exists_query = get_exists_query_expr(left_exprs.at(i));
            is_used = false;
            for (int64_t j = 0; OB_SUCC(ret) && !is_used && !has_false_conds && j < right_exprs.count(); ++j) {
              ObQueryRefRawExpr* not_exists_query = get_exists_query_expr(right_exprs.at(j));
              if (!removed.has_member(j)) {
                param.exists_expr_ = left_exprs.at(i);
                param.not_exists_expr_ = right_exprs.at(j);
                param.trans_flag_ = EXISTS_NOT_EXISTS;
                param.map_info_.reset();
                if (OB_ISNULL(exists_query) || OB_ISNULL(not_exists_query)) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("query ref exprs are null", K(ret));
                } else if (OB_FAIL(ObStmtComparer::check_stmt_containment(not_exists_query->get_ref_stmt(),
                               exists_query->get_ref_stmt(),
                               param.map_info_,
                               relation))) {
                  LOG_WARN("failed to check stmt containment", K(ret));
                } else if (relation == QueryRelation::RIGHT_SUBSET || relation == QueryRelation::EQUAL) {
                  has_false_conds = true;
                  is_used = true;
                  trans_params.reset();
                  if (OB_FAIL(trans_params.push_back(param))) {
                    LOG_WARN("failed to push back transform param", K(ret));
                  }
                } else if (relation == QueryRelation::LEFT_SUBSET) {
                  if (OB_FAIL(removed.add_member(j))) {
                    LOG_WARN("failed to add member into bit set", K(ret));
                  } else if (OB_FAIL(trans_params.push_back(param))) {
                    LOG_WARN("failed to push back transform param", K(ret));
                  } else {
                    is_used = true;
                  }
                } else {
                }
              }
            }
          }
        }
      } else if (!has_false_conds) {  // any, all
        if (OB_FAIL(get_same_classify_exprs(validity_exprs, left_exprs, left_type[k], IS_WITH_ANY))) {
          LOG_WARN("get the same classify exprs failed", K(ret));
        } else if (OB_FAIL(get_same_classify_exprs(validity_exprs, right_exprs, right_type[k], IS_WITH_ALL))) {
          LOG_WARN("get the same classify exprs failed", K(ret));
        } else if ((T_OP_SQ_LT == left_type[k] || T_OP_SQ_GT == left_type[k]) &&
                   OB_FAIL(get_same_classify_exprs(
                       validity_exprs, right_exprs, right_type[k + 1], IS_WITH_ALL))) {  //>any vs <all, <=all...
          LOG_WARN("get the same classify exprs failed", K(ret));
        } else {
          removed.reset();
          bool can_coalesce = (left_type[k] == T_OP_SQ_EQ) ? true : false;
          for (int64_t i = 0; OB_SUCC(ret) && !has_false_conds && i < left_exprs.count(); ++i) {
            param.any_expr_ = left_exprs.at(i);
            param.trans_flag_ = ANY_ALL;
            is_used = false;
            for (int64_t j = 0; OB_SUCC(ret) && !is_used && !has_false_conds && j < right_exprs.count(); ++j) {
              if (!removed.has_member(j)) {
                param.all_expr_ = right_exprs.at(j);
                if (OB_FAIL(compare_any_all_subqueries(param, trans_params, has_false_conds, is_used, can_coalesce))) {
                  LOG_WARN("compare two subqueries failed", K(ret));
                } else if (!has_false_conds && is_used && OB_FAIL(removed.add_member(j))) {
                  LOG_WARN("failed to add member into bit set", K(ret));
                } else {
                  /*do nothing */
                }
              }
            }
          }
        }
      } else {
        /*do nothing*/
      }
    }
  }
  return ret;
}

int ObTransformSubqueryCoalesce::compare_any_all_subqueries(TransformParam& param,
    ObIArray<TransformParam>& trans_params, bool& has_false_conds, bool& is_used, bool can_coalesce)
{
  int ret = OB_SUCCESS;
  has_false_conds = false;
  is_used = false;
  QueryRelation relation = QueryRelation::UNCOMPARABLE;
  ObRawExpr* first_left_expr = get_any_all_left_hand_expr(param.any_expr_);
  ObQueryRefRawExpr* first_query_ref = get_any_all_query_expr(param.any_expr_);
  ObRawExpr* second_left_expr = get_any_all_left_hand_expr(param.all_expr_);
  ObQueryRefRawExpr* second_query_ref = get_any_all_query_expr(param.all_expr_);
  if (OB_ISNULL(first_left_expr) || OB_ISNULL(first_query_ref) || OB_ISNULL(second_left_expr) ||
      OB_ISNULL(second_query_ref)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("query ref exprs are null", K(ret));
  } else if (!first_left_expr->same_as(*second_left_expr)) {
    /*do nothing*/
  } else if (OB_FAIL(ObStmtComparer::check_stmt_containment(
                 second_query_ref->get_ref_stmt(), first_query_ref->get_ref_stmt(), param.map_info_, relation))) {
    LOG_WARN("failed to check stmt containment", K(ret));
  } else if (relation == QueryRelation::RIGHT_SUBSET || relation == QueryRelation::EQUAL) {
    has_false_conds = true;
    trans_params.reset();
    is_used = true;
    if (OB_FAIL(trans_params.push_back(param))) {
      LOG_WARN("failed to push back transform param", K(ret));
    }
  } else if (can_coalesce && relation == QueryRelation::LEFT_SUBSET) {
    if (OB_FAIL(trans_params.push_back(param))) {
      LOG_WARN("failed to push back transform param", K(ret));
    } else {
      is_used = true;
    }
  }
  return ret;
}

/**
 * @brief ObTransformSubqueryCoalesce::coalesce_diff_exists_exprs
 * 1. remove merged exprs from cond_exprs
 * 2. merge exists and not exists
 * 3. add a new expr into cond_exprs
 * @return
 */
int ObTransformSubqueryCoalesce::coalesce_diff_exists_exprs(
    ObDMLStmt* stmt, ObIArray<ObRawExpr*>& cond_exprs, ObIArray<TransformParam>& trans_params)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < trans_params.count(); ++i) {
    ObRawExpr* new_exists_expr = NULL;
    TransformParam& param = trans_params.at(i);
    if (param.trans_flag_ == EXISTS_NOT_EXISTS) {
      ObQueryRefRawExpr* exist_query = get_exists_query_expr(param.exists_expr_);
      ObQueryRefRawExpr* not_exist_query = get_exists_query_expr(param.not_exists_expr_);
      ObQueryRefRawExpr* new_exists_query = NULL;
      if (OB_ISNULL(exist_query) || OB_ISNULL(not_exist_query)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("params are invalid", K(ret), K(exist_query), K(not_exist_query));
      } else if (OB_FAIL(merge_exists_subqueries(param, new_exists_expr))) {
        LOG_WARN("failed to merge subqueries", K(ret));
      } else if (OB_FAIL(ObOptimizerUtil::remove_item(cond_exprs, param.exists_expr_))) {
        LOG_WARN("failed to remove item", K(ret));
      } else if (OB_FAIL(ObOptimizerUtil::remove_item(cond_exprs, param.not_exists_expr_))) {
        LOG_WARN("failed to remove item", K(ret));
      } else if (OB_FAIL(cond_exprs.push_back(new_exists_expr))) {
        LOG_WARN("failed to push back new exists expr", K(ret));
      } else if (OB_FAIL(ObOptimizerUtil::remove_item(stmt->get_subquery_exprs(), exist_query))) {
        LOG_WARN("failed to remove subquery ref expr", K(ret));
      } else if (OB_FAIL(ObOptimizerUtil::remove_item(stmt->get_subquery_exprs(), not_exist_query))) {
        LOG_WARN("failed to remove subquery ref expr", K(ret));
      } else if (OB_ISNULL(new_exists_query = get_exists_query_expr(new_exists_expr))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the new exists expr is invalid", K(ret));
      } else if (OB_FAIL(stmt->get_subquery_exprs().push_back(new_exists_query))) {
        LOG_WARN("failed to push back new exist query", K(ret));
      } else {
        /*do nothing*/
      }
    } else {
      /*do nothing*/
    }
  }
  return ret;
}

/**
 * @brief ObTransformSubqueryCoalesce::coalesce_diff_any_all_exprs
 * 1. remove merged exprs from cond_exprs
 * 2. merge any_subquery and all_subquery
 * 3. add a new expr into cond_exprs
 * @return
 */
int ObTransformSubqueryCoalesce::coalesce_diff_any_all_exprs(
    ObDMLStmt* stmt, ObIArray<ObRawExpr*>& cond_exprs, ObIArray<TransformParam>& trans_params)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null ptr", K(stmt), K(ret));
  } else {
    ObQueryRefRawExpr* any_ref_expr = NULL;
    ObQueryRefRawExpr* all_ref_expr = NULL;
    ObRawExpr* old_any_expr = NULL;
    ObRawExpr* old_all_expr = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < trans_params.count(); ++i) {
      ObRawExpr* new_any_all_expr = NULL;
      TransformParam& param = trans_params.at(i);
      if (param.trans_flag_ == ANY_ALL) {
        any_ref_expr = get_any_all_query_expr(param.any_expr_);
        all_ref_expr = get_any_all_query_expr(param.all_expr_);
        old_any_expr = param.any_expr_;
        old_all_expr = param.all_expr_;
        ObQueryRefRawExpr* new_any_all_query = NULL;
        if (OB_ISNULL(any_ref_expr) || OB_ISNULL(all_ref_expr) || OB_ISNULL(old_any_expr) || OB_ISNULL(old_all_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("params are invalid", K(ret), K(any_ref_expr), K(all_ref_expr), K(old_any_expr), K(old_all_expr));
        } else if (OB_FAIL(merge_any_all_subqueries(any_ref_expr, all_ref_expr, param, new_any_all_expr))) {
          LOG_WARN("failed to merge subqueries", K(ret));
        } else if (OB_FAIL(ObOptimizerUtil::remove_item(cond_exprs, old_any_expr))) {
          LOG_WARN("failed to remove item", K(ret));
        } else if (OB_FAIL(ObOptimizerUtil::remove_item(cond_exprs, old_all_expr))) {
          LOG_WARN("failed to remove item", K(ret));
        } else if (OB_FAIL(cond_exprs.push_back(new_any_all_expr))) {
          LOG_WARN("failed to push back new any all expr", K(ret));
        } else if (OB_FAIL(ObOptimizerUtil::remove_item(stmt->get_subquery_exprs(), any_ref_expr))) {
          LOG_WARN("failed to remove subquery ref expr", K(ret));
        } else if (OB_FAIL(ObOptimizerUtil::remove_item(stmt->get_subquery_exprs(), all_ref_expr))) {
          LOG_WARN("failed to remove subquery ref expr", K(ret));
        } else if (OB_ISNULL(new_any_all_query = get_any_all_query_expr(new_any_all_expr))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("the new any all expr is invalid", K(ret));
        } else if (OB_FAIL(stmt->get_subquery_exprs().push_back(new_any_all_query))) {
          LOG_WARN("failed to push back new any all query", K(ret));
        } else if (OB_FAIL(stmt->pull_all_expr_relation_id_and_levels())) {
          LOG_WARN("failed to form pull up expr id and level", K(ret));
        } else if (OB_FAIL(stmt->formalize_stmt(ctx_->session_info_))) {
          LOG_WARN("formalize stmt failed", K(ret));
        } else {
          /*do nothing */
        }
      }
    }
  }
  return ret;
}

int ObTransformSubqueryCoalesce::merge_exists_subqueries(TransformParam& trans_param, ObRawExpr*& new_exist_expr)
{
  int ret = OB_SUCCESS;
  ObRawExprFactory* expr_factory = NULL;
  ObStmtFactory* stmt_factory = NULL;
  ObSEArray<ObRawExpr*, 4> old_cols;
  ObSEArray<ObRawExpr*, 4> new_cols;
  ObSEArray<ObRawExpr*, 4> extra_conds;
  ObQueryRefRawExpr* exist_query_ref = NULL;
  ObQueryRefRawExpr* not_exist_query_ref = NULL;
  ObQueryRefRawExpr* new_query_ref = NULL;
  ObSelectStmt* exist_stmt = NULL;
  ObSelectStmt* not_exist_stmt = NULL;
  ObSelectStmt* new_exist_stmt = NULL;
  const ObStmtMapInfo& map_info = trans_param.map_info_;
  if (OB_ISNULL(ctx_) || OB_ISNULL(expr_factory = ctx_->expr_factory_) ||
      OB_ISNULL(stmt_factory = ctx_->stmt_factory_) ||
      OB_ISNULL(exist_query_ref = get_exists_query_expr(trans_param.exists_expr_)) ||
      OB_ISNULL(not_exist_query_ref = get_exists_query_expr(trans_param.not_exists_expr_)) ||
      OB_ISNULL(exist_stmt = exist_query_ref->get_ref_stmt()) ||
      OB_ISNULL(not_exist_stmt = not_exist_query_ref->get_ref_stmt()) ||
      OB_UNLIKELY(map_info.cond_map_.count() != not_exist_stmt->get_condition_size()) ||
      OB_UNLIKELY(map_info.table_map_.count() != not_exist_stmt->get_table_size())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("exist exprs are not valid",
        K(ret),
        K(expr_factory),
        K(stmt_factory),
        K(exist_query_ref),
        K(not_exist_query_ref),
        K(exist_stmt),
        K(not_exist_stmt));
  } else if (OB_FAIL(ObRawExprUtils::copy_expr(
                 *expr_factory, trans_param.exists_expr_, new_exist_expr, COPY_REF_DEFAULT))) {
    LOG_WARN("failed to copy exist expr", K(ret));
  } else if (OB_FAIL(expr_factory->create_raw_expr(T_INVALID, new_query_ref))) {
    LOG_WARN("failed to create query ref expr", K(ret));
  } else if (OB_FAIL(new_query_ref->assign(*exist_query_ref))) {
    LOG_WARN("failed to assign query ref expr", K(ret));
  } else if (OB_FAIL(ObTransformUtils::copy_stmt(
                 *stmt_factory, exist_stmt, reinterpret_cast<ObDMLStmt*&>(new_exist_stmt)))) {
    LOG_WARN("failed to copy exist stmt", K(ret));
  } else if (OB_FAIL(new_exist_stmt->adjust_statement_id())) {
    LOG_WARN("failed to adjust stmt id", K(ret));
  } else {
    new_query_ref->set_ref_stmt(new_exist_stmt);
    new_exist_expr->get_param_expr(0) = new_query_ref;
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < not_exist_stmt->get_condition_size(); ++i) {
    ObRawExpr* new_cond_expr = NULL;
    if (map_info.cond_map_.at(i) >= 0 && map_info.cond_map_.at(i) < new_exist_stmt->get_condition_size()) {
      // both stmt has the condition
    } else if (OB_FAIL(ObRawExprUtils::copy_expr(
                   *expr_factory, not_exist_stmt->get_condition_expr(i), new_cond_expr, COPY_REF_DEFAULT))) {
      LOG_WARN("failed to copy condition", K(ret));
    } else if (OB_FAIL(extra_conds.push_back(new_cond_expr))) {
      LOG_WARN("failed to push back condition", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < not_exist_stmt->get_column_size(); ++i) {
    ColumnItem& col_item = not_exist_stmt->get_column_items().at(i);
    ObColumnRefRawExpr* col_expr = not_exist_stmt->get_column_items().at(i).expr_;
    ObRawExpr* new_expr = NULL;
    TableItem* new_table_item = NULL;
    int64_t idx = not_exist_stmt->get_table_bit_index(col_item.table_id_) - 1;
    if (idx < 0 || idx >= map_info.table_map_.count() || map_info.table_map_.at(idx) < 0 ||
        map_info.table_map_.at(idx) >= new_exist_stmt->get_table_size() ||
        OB_ISNULL(new_table_item = new_exist_stmt->get_table_item(map_info.table_map_.at(idx)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table is not mapped", K(ret), K(idx));
    } else if (OB_NOT_NULL(
                   new_expr = new_exist_stmt->get_column_expr_by_id(new_table_item->table_id_, col_item.column_id_))) {
      // do nothing
    } else if (OB_FAIL(ObRawExprUtils::copy_expr(*expr_factory, col_item.expr_, new_expr, COPY_REF_SHARED))) {
      LOG_WARN("failed to copy expr", K(ret));
    } else {
      ColumnItem new_col_item;
      new_col_item.table_id_ = new_table_item->table_id_;
      new_col_item.column_id_ = col_item.column_id_;
      new_col_item.column_name_ = col_item.column_name_;
      new_col_item.expr_ = static_cast<ObColumnRefRawExpr*>(new_expr);
      new_col_item.expr_->set_table_id(new_table_item->table_id_);
      new_col_item.expr_->set_table_name(new_table_item->table_name_);
      if (OB_FAIL(new_exist_stmt->add_column_item(new_col_item))) {
        LOG_WARN("failed to add column item", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(old_cols.push_back(col_expr))) {
        LOG_WARN("failed to append column expr", K(ret));
      } else if (OB_FAIL(new_cols.push_back(new_expr))) {
        LOG_WARN("failed to append column expr", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    // create having exprs
    // having sum(case when conds then 1 else 0 end) = 0
    ObCaseOpRawExpr* case_expr = NULL;
    ObConstRawExpr* one_expr = NULL;
    ObConstRawExpr* zero_expr = NULL;
    ObConstRawExpr* equal_value = NULL;
    ObAggFunRawExpr* sum_expr = NULL;
    ObRawExpr* equal_expr = NULL;
    ObRawExpr* when_expr = NULL;
    if (OB_FAIL(expr_factory->create_raw_expr(T_OP_CASE, case_expr))) {
      LOG_WARN("failed to create case expr", K(ret));
    } else if (OB_FAIL(create_and_expr(extra_conds, when_expr))) {
      LOG_WARN("failed to create and expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*expr_factory, ObIntType, 1L, one_expr))) {
      LOG_WARN("failed to build const int expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*expr_factory, ObIntType, 0L, zero_expr))) {
      LOG_WARN("failed to build const int expr", K(ret));
    } else if (OB_FAIL(case_expr->add_when_param_expr(when_expr))) {
      LOG_WARN("failed to add when param expr", K(ret));
    } else if (OB_FAIL(case_expr->add_then_param_expr(one_expr))) {
      LOG_WARN("failed to add one expr", K(ret));
    } else if (FALSE_IT(case_expr->set_default_param_expr(zero_expr))) {
      // do nothing
    } else if (OB_FAIL(expr_factory->create_raw_expr(T_FUN_SUM, sum_expr))) {
      LOG_WARN("failed to create new aggr expr", K(ret));
    } else if (OB_ISNULL(sum_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sum expr is null", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*expr_factory, ObIntType, 0L, equal_value))) {
      LOG_WARN("faield to build const int expr", K(ret));
    } else if (OB_FAIL(sum_expr->add_real_param_expr(case_expr))) {
      LOG_WARN("failed to add real param expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::create_double_op_expr(
                   *expr_factory, ctx_->session_info_, T_OP_EQ, equal_expr, sum_expr, equal_value))) {
      LOG_WARN("failed to create equal expr", K(ret));
    } else if (OB_FAIL(equal_expr->formalize(ctx_->session_info_))) {
      LOG_WARN("failed to formalize equal expr", K(ret));
    } else if (OB_FAIL(equal_expr->replace_expr(old_cols, new_cols))) {
      LOG_WARN("failed to replace expr", K(ret));
    } else if (OB_FAIL(new_exist_stmt->add_agg_item(*sum_expr))) {
      LOG_WARN("failed to add aggr item", K(ret));
    } else if (OB_FAIL(new_exist_stmt->add_having_expr(equal_expr))) {
      LOG_WARN("failed to add having expr", K(ret));
    }
  }
  return ret;
}

int ObTransformSubqueryCoalesce::merge_any_all_subqueries(ObQueryRefRawExpr* any_query_ref,
    ObQueryRefRawExpr* all_query_ref, TransformParam& trans_param, ObRawExpr*& new_any_all_query)
{
  int ret = OB_SUCCESS;
  ObRawExprFactory* expr_factory = NULL;
  ObStmtFactory* stmt_factory = NULL;
  ObSEArray<ObRawExpr*, 4> old_cols;
  ObSEArray<ObRawExpr*, 4> new_cols;
  ObSEArray<ObRawExpr*, 4> extra_conds;
  ObQueryRefRawExpr* new_query_ref = NULL;
  ObSelectStmt* any_stmt = NULL;
  ObSelectStmt* all_stmt = NULL;
  ObSelectStmt* new_any_stmt = NULL;
  const ObStmtMapInfo& map_info = trans_param.map_info_;
  if (OB_ISNULL(ctx_) || OB_ISNULL(expr_factory = ctx_->expr_factory_) ||
      OB_ISNULL(stmt_factory = ctx_->stmt_factory_) || OB_ISNULL(any_query_ref) || OB_ISNULL(all_query_ref) ||
      OB_ISNULL(any_stmt = any_query_ref->get_ref_stmt()) || OB_ISNULL(all_stmt = all_query_ref->get_ref_stmt()) ||
      OB_UNLIKELY(map_info.cond_map_.count() != all_stmt->get_condition_size()) ||
      OB_UNLIKELY(map_info.table_map_.count() != all_stmt->get_table_size())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("any/all exprs are not valid",
        K(ret),
        K(expr_factory),
        K(stmt_factory),
        K(any_query_ref),
        K(all_query_ref),
        K(any_stmt),
        K(all_stmt));
  } else if (OB_FAIL(ObRawExprUtils::copy_expr(
                 *expr_factory, trans_param.any_expr_, new_any_all_query, COPY_REF_DEFAULT))) {
    LOG_WARN("failed to copy any expr", K(ret));
  } else if (OB_FAIL(expr_factory->create_raw_expr(T_INVALID, new_query_ref))) {
    LOG_WARN("failed to create query ref expr", K(ret));
  } else if (OB_FAIL(new_query_ref->assign(*any_query_ref))) {
    LOG_WARN("failed to assign query ref expr", K(ret));
  } else if (OB_FAIL(
                 ObTransformUtils::copy_stmt(*stmt_factory, any_stmt, reinterpret_cast<ObDMLStmt*&>(new_any_stmt)))) {
    LOG_WARN("failed to copy any stmt", K(ret));
  } else if (OB_FAIL(new_any_stmt->adjust_statement_id())) {
    LOG_WARN("failed to adjust stmt id", K(ret));
  } else {
    new_query_ref->set_ref_stmt(new_any_stmt);
    new_any_all_query->get_param_expr(1) = new_query_ref;
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < all_stmt->get_condition_size(); ++i) {
    ObRawExpr* new_cond_expr = NULL;
    if (map_info.cond_map_.at(i) >= 0 && map_info.cond_map_.at(i) < new_any_stmt->get_condition_size()) {
      // both stmt has the condition
    } else if (OB_FAIL(ObRawExprUtils::copy_expr(
                   *expr_factory, all_stmt->get_condition_expr(i), new_cond_expr, COPY_REF_DEFAULT))) {
      LOG_WARN("failed to copy condition", K(ret));
    } else if (OB_FAIL(extra_conds.push_back(new_cond_expr))) {
      LOG_WARN("failed to push back condition", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < all_stmt->get_column_size(); ++i) {
    ColumnItem& col_item = all_stmt->get_column_items().at(i);
    ObColumnRefRawExpr* col_expr = all_stmt->get_column_items().at(i).expr_;
    ObRawExpr* new_expr = NULL;
    TableItem* new_table_item = NULL;
    int64_t idx = all_stmt->get_table_bit_index(col_item.table_id_) - 1;
    if (OB_UNLIKELY(idx < 0 || idx >= map_info.table_map_.count() || map_info.table_map_.at(idx) < 0 ||
                    map_info.table_map_.at(idx) >= new_any_stmt->get_table_size() ||
                    OB_ISNULL(new_table_item = new_any_stmt->get_table_item(map_info.table_map_.at(idx))))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table is not mapped", K(ret), K(idx));
    } else if (OB_NOT_NULL(
                   new_expr = new_any_stmt->get_column_expr_by_id(new_table_item->table_id_, col_item.column_id_))) {
      // do nothing
    } else if (OB_FAIL(ObRawExprUtils::copy_expr(*expr_factory, col_item.expr_, new_expr, COPY_REF_SHARED))) {
      LOG_WARN("failed to copy expr", K(ret));
    } else {
      ColumnItem new_col_item;
      new_col_item.table_id_ = new_table_item->table_id_;
      new_col_item.column_id_ = col_item.column_id_;
      new_col_item.column_name_ = col_item.column_name_;
      new_col_item.expr_ = static_cast<ObColumnRefRawExpr*>(new_expr);
      new_col_item.expr_->set_table_id(new_table_item->table_id_);
      new_col_item.expr_->set_table_name(new_table_item->table_name_);
      if (OB_FAIL(new_any_stmt->add_column_item(new_col_item))) {
        LOG_WARN("failed to add column item", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(old_cols.push_back(col_expr))) {
        LOG_WARN("failed to append column expr", K(ret));
      } else if (OB_FAIL(new_cols.push_back(new_expr))) {
        LOG_WARN("failed to append column expr", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    // create lnnvl exprs
    ObRawExpr* and_expr = NULL;
    ObRawExpr* lnnvl_expr = NULL;
    if (OB_FAIL(create_and_expr(extra_conds, and_expr))) {
      LOG_WARN("failed to create and expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_lnnvl_expr(*expr_factory, and_expr, lnnvl_expr))) {
      LOG_WARN("failed to build lnnvl expr", K(ret));
    } else if (OB_FAIL(lnnvl_expr->formalize(ctx_->session_info_))) {
      LOG_WARN("failed to formalize lnnvl  expr expr", K(ret));
    } else if (OB_FAIL(lnnvl_expr->replace_expr(old_cols, new_cols))) {
      LOG_WARN("failed to replace expr", K(ret));
    } else if (OB_FAIL(new_any_stmt->add_condition_expr(lnnvl_expr))) {
      LOG_WARN("failed to add having expr", K(ret));
    }
  }
  return ret;
}

int ObTransformSubqueryCoalesce::create_and_expr(const ObIArray<ObRawExpr*>& params, ObRawExpr*& ret_expr)
{
  int ret = OB_SUCCESS;
  ObRawExprFactory* factory = NULL;
  ObOpRawExpr* and_expr = NULL;
  ret_expr = NULL;
  if (OB_ISNULL(ctx_) || OB_ISNULL(factory = ctx_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(ctx_), K(factory));
  } else if (params.count() == 0) {
    // do nothing
  } else if (params.count() == 1) {
    ret_expr = params.at(0);
  } else if (OB_FAIL(factory->create_raw_expr(T_OP_AND, and_expr))) {
    LOG_WARN("failed to create and expr", K(ret));
  } else if (OB_ISNULL(and_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("and expr is null", K(ret));
  } else if (OB_FAIL(and_expr->get_param_exprs().assign(params))) {
    LOG_WARN("failed to assign params", K(ret));
  } else {
    ret_expr = and_expr;
  }
  return ret;
}

ObQueryRefRawExpr* ObTransformSubqueryCoalesce::get_exists_query_expr(ObRawExpr* expr)
{
  ObQueryRefRawExpr* ret_expr = NULL;
  if (OB_ISNULL(expr) ||
      OB_UNLIKELY(expr->get_expr_type() != T_OP_EXISTS && expr->get_expr_type() != T_OP_NOT_EXISTS) ||
      OB_ISNULL(expr->get_param_expr(0)) || OB_UNLIKELY(!expr->get_param_expr(0)->is_query_ref_expr())) {
    // do nothing
  } else {
    ret_expr = static_cast<ObQueryRefRawExpr*>(expr->get_param_expr(0));
  }
  return ret_expr;
}

ObQueryRefRawExpr* ObTransformSubqueryCoalesce::get_any_all_query_expr(ObRawExpr* expr)
{
  ObQueryRefRawExpr* ret_expr = NULL;
  ObOpRawExpr* subquey_op = NULL;
  if (OB_ISNULL(expr) || OB_UNLIKELY(!expr->has_flag(IS_WITH_ANY) && !expr->has_flag(IS_WITH_ALL))) {
    // do nothing
  } else {
    subquey_op = static_cast<ObOpRawExpr*>(expr);
    if (OB_ISNULL(subquey_op->get_param_expr(1)) || OB_UNLIKELY(!subquey_op->get_param_expr(1)->is_query_ref_expr())) {
      /*do nothing*/
    } else {
      ret_expr = static_cast<ObQueryRefRawExpr*>(subquey_op->get_param_expr(1));
    }
  }
  return ret_expr;
}

ObRawExpr* ObTransformSubqueryCoalesce::get_any_all_left_hand_expr(ObRawExpr* expr)
{
  ObRawExpr* left_hand = NULL;
  ObOpRawExpr* subquey_op = NULL;
  if (OB_ISNULL(expr) || OB_UNLIKELY(!expr->has_flag(IS_WITH_ANY) && !expr->has_flag(IS_WITH_ALL))) {
    // do nothing
  } else {
    subquey_op = static_cast<ObOpRawExpr*>(expr);
    if (OB_ISNULL(subquey_op->get_param_expr(0))) {
      /*do nothing*/
    } else {
      left_hand = subquey_op->get_param_expr(0);
    }
  }
  return left_hand;
}

int ObTransformSubqueryCoalesce::make_false(ObIArray<ObRawExpr*>& conds)
{
  int ret = OB_SUCCESS;
  ObRawExpr* false_expr = NULL;
  conds.reset();
  if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("transform context is null", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_const_bool_expr(ctx_->expr_factory_, false_expr, false))) {
    LOG_WARN("failed to build const bool expr", K(ret));
  } else if (OB_FAIL(conds.push_back(false_expr))) {
    LOG_WARN("failed to push back false expr", K(ret));
  }
  return ret;
}
