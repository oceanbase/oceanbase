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
#include "sql/resolver/dml/ob_update_stmt.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

ObTransformSubqueryCoalesce::~ObTransformSubqueryCoalesce()
{
  for (int64_t i = 0; i < coalesce_stmts_.count(); ++i) {
    if (NULL != coalesce_stmts_.at(i)) {
      coalesce_stmts_.at(i)->~CoalesceStmts();
      coalesce_stmts_.at(i) = NULL;
    }
  }
}

int ObTransformSubqueryCoalesce::transform_one_stmt(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                                    ObDMLStmt *&stmt,
                                                    bool &trans_happened)
{
  int ret = OB_SUCCESS;
  bool is_happened = false;
  trans_happened = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt is NULL", K(ret), K(stmt), K(ctx_));
  } else if (OB_FAIL(transform_same_exprs(stmt, stmt->get_condition_exprs(), trans_happened))) {
    LOG_WARN("failed to transform where conditions", K(ret));
  } else if (!stmt->is_select_stmt()) {
    if (OB_FAIL(coalesce_update_assignment(stmt, is_happened))) {
      LOG_WARN("failed to transform for coalesce update set.", K(ret));
    } else {
      trans_happened |= is_happened;
      LOG_TRACE("succeed to transform for coalesce update set", K(is_happened), K(ret));
    }
  } else if (OB_FAIL(transform_same_exprs(
                      stmt, static_cast<ObSelectStmt*>(stmt)->get_having_exprs(), is_happened))) {
    LOG_WARN("failed to transform same exprs", K(ret));
  } else {
    trans_happened = trans_happened || is_happened;
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(transform_or_exprs(stmt, stmt->get_condition_exprs(), is_happened))) {
      LOG_WARN("failed to transform where conditions", K(ret));
    } else {
      trans_happened |= is_happened;
    }
  }
  if (OB_SUCC(ret)) {
    /* 将能够合并的尽量合并,如:select * from t1 where t1.c1 in (select 1 from t2 where t2.c2 = t1.c2) 
    * and c1 not in (select 1 from t2 where t2.c2 = t1.c2 and t2.c2 > 3) and c1 in (select 1 from t2 where t2.c2 = t1.c2);
    * 本来这条语句可以为恒fasle,但是第一次合并可能无法判断出，因此需要第二次合并才能判断出。
    */
    ObSEArray<ObPCParamEqualInfo, 4> rule_based_equal_infos;
    ObSEArray<ObPCParamEqualInfo, 4> cost_based_equal_infos;
    ObTryTransHelper try_trans_helper;
    do {
      is_happened = false;
      ObDMLStmt *trans_stmt = NULL;
      rule_based_equal_infos.reset();
      cost_based_equal_infos.reset();
      bool rule_based_trans_happened = false;
      if (OB_FAIL(try_trans_helper.fill_helper(stmt->get_query_ctx()))) {
        LOG_WARN("failed to fill try trans helper", K(ret));
      } else if (OB_FAIL(transform_diff_exprs(stmt, trans_stmt,
                                              rule_based_equal_infos,
                                              cost_based_equal_infos,
                                              rule_based_trans_happened))) {
        LOG_WARN("failed to transform exprs", K(ret));
      } else if (OB_FAIL(append(ctx_->equal_param_constraints_, rule_based_equal_infos))) {
        LOG_WARN("failed to append equal infos", K(ret));
      } else if (OB_ISNULL(trans_stmt)) {
        // do nothing
      } else if (OB_FAIL(accept_transform(parent_stmts, stmt, trans_stmt, false, false, is_happened))) {
        LOG_WARN("failed to accept transform", K(ret), KPC(trans_stmt));
      } else if (!is_happened) {
        // do nothing
      } else if (OB_FAIL(append(ctx_->equal_param_constraints_, cost_based_equal_infos))) {
        LOG_WARN("failed to append equal infos", K(ret));
      }
      if (OB_FAIL(ret)) {
      } else if (rule_based_trans_happened || is_happened) {
        trans_happened = true;
      } else if (OB_FAIL(try_trans_helper.recover(stmt->get_query_ctx()))) {
        LOG_WARN("failed to recover params", K(ret));
      }
    } while(OB_SUCC(ret) && is_happened);
  }
  if (OB_SUCC(ret) && trans_happened && !coalesce_stmts_.empty()) {
    if (OB_FAIL(add_transform_hint(*stmt, &coalesce_stmts_))) {
      LOG_WARN("failed to add hint", K(ret));
    }
  }
  return ret;
}

int ObTransformSubqueryCoalesce::transform_same_exprs(ObDMLStmt *stmt,
                                                      ObIArray<ObRawExpr *> &conds,
                                                      bool &is_happened)
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
    ObItemType item_type[8] = {T_OP_EXISTS, T_OP_NOT_EXISTS, T_OP_SQ_EQ, T_OP_SQ_NE,
                               T_OP_SQ_GT, T_OP_SQ_GE, T_OP_SQ_LT, T_OP_SQ_LE};
    for (int32_t i = 0; OB_SUCC(ret) && i < 8; ++i) {
      same_all_exprs.reset();
      same_exprs.reset();
      if (item_type[i] == T_OP_EXISTS || item_type[i] == T_OP_NOT_EXISTS) {
        if (OB_FAIL(get_same_classify_exprs(validity_exprs, same_exprs, 
                                            item_type[i], CNT_SUB_QUERY))) {
          LOG_WARN("get the same classify exprs failed", K(ret));
        } else if (OB_FAIL(copy_exprs.assign(same_exprs))) {
          LOG_WARN("failed to assign exprs", K(ret));
        } else if (OB_FAIL(coalesce_same_exists_exprs(stmt, item_type[i], 
                                                      same_exprs, happened))) {
          LOG_WARN("failed to coalesce exists exprs", K(ret));
        } else if (OB_FAIL(get_remove_exprs(copy_exprs, same_exprs, remove_exprs))) {
          LOG_WARN("failed to get remove exprs", K(ret));
        } else {
          coalesce_happened |= happened;
        }
      } else if (OB_FAIL(get_same_classify_exprs(validity_exprs, same_exprs, 
                                                 item_type[i], IS_WITH_ANY))) {
        LOG_WARN("get the same classify exprs failed", K(ret));
      } else if (OB_FAIL(copy_exprs.assign(same_exprs))) {
        LOG_WARN("failed to assign exprs", K(ret));
      } else if (OB_FAIL(coalesce_same_any_all_exprs(stmt, T_ANY, same_exprs, happened))) {
        LOG_WARN("failed to coalesce any exprs", K(ret));
      } else if (OB_FAIL(get_remove_exprs(copy_exprs, same_exprs, remove_exprs))) {
        LOG_WARN("failed to get remove exprs", K(ret));
      } else if (OB_FAIL(get_same_classify_exprs(validity_exprs, same_all_exprs,
                                                 item_type[i], IS_WITH_ALL))) {
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
      is_happened = true;
      if (OB_FAIL(ObOptimizerUtil::remove_item(conds, remove_exprs))) {
        LOG_WARN("failed to remove exprs", K(ret));
      }
    }
  } 
  return ret;
} 

int ObTransformSubqueryCoalesce::get_same_classify_exprs(ObIArray<ObRawExpr *> &validity_exprs,
                                                         ObIArray<ObRawExpr *> &same_classify_exprs,
                                                         ObItemType ctype,
                                                         ObExprInfoFlag flag) 
{   
  int ret = OB_SUCCESS;
  ObRawExpr *expr = NULL;
  for (int32_t i = 0; OB_SUCC(ret) && i < validity_exprs.count(); ++i) {
    expr = validity_exprs.at(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is null", K(ret));
    } else if (expr->has_flag(flag) && expr->get_expr_type() == ctype 
               && OB_FAIL(same_classify_exprs.push_back(expr))) {
      LOG_WARN("failed to push back same classify exprs", K(ret));
    } else {
      /*do nothing*/
    }
  }
  return ret;
}

int ObTransformSubqueryCoalesce::adjust_transform_types(uint64_t &transform_types)
{
  int ret = OB_SUCCESS;
  if(cost_based_trans_tried_) {
    transform_types &= (~(1 << transformer_type_));
  }
  return ret;
}

int ObTransformSubqueryCoalesce::classify_conditions(ObIArray<ObRawExpr *> &conditions,
                                                     ObIArray<ObRawExpr *> &validity_exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < conditions.count(); ++i) {
    ObRawExpr *cond = NULL;
    bool is_valid = false;
    if (OB_ISNULL(cond = conditions.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is null", K(ret), K(i));
    } else if (cond->get_expr_type() != T_OP_EXISTS &&
               cond->get_expr_type() != T_OP_NOT_EXISTS && 
               !cond->has_flag(IS_WITH_ANY) && 
               !cond->has_flag(IS_WITH_ALL)) {
      //do nothing
    } else if ((cond->get_expr_type() == T_OP_EXISTS 
                || cond->get_expr_type() == T_OP_NOT_EXISTS) 
               && OB_FAIL(check_query_ref_validity(cond->get_param_expr(0), is_valid))) {
      LOG_WARN("failed to check query ref validity", K(ret));
    } else if (is_valid) {
      ret = validity_exprs.push_back(cond);
    } else if ((cond->has_flag(IS_WITH_ANY) 
                || cond->has_flag(IS_WITH_ALL)) 
               && OB_FAIL(check_query_ref_validity(cond->get_param_expr(1), is_valid))) {
      LOG_WARN("failed to check query ref validity", K(ret));
    } else if (is_valid) {
      ret = validity_exprs.push_back(cond);
    } else {
      //do nothing
    }
  }
  return ret;
}

int ObTransformSubqueryCoalesce::get_remove_exprs(ObIArray<ObRawExpr *> &ori_exprs,
                                                  ObIArray<ObRawExpr *> &remain_exprs,
                                                  ObIArray<ObRawExpr *> &remove_exprs)
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

int ObTransformSubqueryCoalesce::check_query_ref_validity(ObRawExpr *expr,
                                                          bool &is_valid)
{
  int ret = OB_SUCCESS;
  ObQueryRefRawExpr *query_ref = NULL;
  is_valid = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret), K(expr));
  } else if (expr->is_query_ref_expr()) {
    ObSelectStmt *sub_stmt = NULL;
    query_ref = static_cast<ObQueryRefRawExpr *>(expr);
    if (OB_ISNULL(sub_stmt = query_ref->get_ref_stmt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("query ref is null", K(ret));
    } else if (sub_stmt->is_spj() &&
               sub_stmt->get_semi_infos().empty() &&
               sub_stmt->get_subquery_exprs().empty()) {
      is_valid = true;
    }
  }
  return ret;
}

int ObTransformSubqueryCoalesce::coalesce_same_exists_exprs(ObDMLStmt *stmt,
                                                            const ObItemType type,
                                                            ObIArray<ObRawExpr *> &filters,
                                                            bool &is_happened)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObPCParamEqualInfo, 4> equal_infos;
  ObStmtMapInfo map_info;
  QueryRelation relation;
  ObQueryRefRawExpr *first_query_ref = NULL;
  ObQueryRefRawExpr *second_query_ref = NULL;
  ObSqlBitSet<> removed_items;
  int64_t remove_index = -1;
  is_happened = false;
  bool force_trans = false;
  bool force_no_trans = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(stmt), K(ctx_));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < filters.count(); ++i) {
    first_query_ref = get_exists_query_expr(filters.at(i));
    for (int64_t j = i + 1; OB_SUCC(ret) && !removed_items.has_member(i) && j < filters.count(); ++j) {
      map_info.reset();
      remove_index = -1;
      second_query_ref = get_exists_query_expr(filters.at(j));
      OPT_TRACE("try to coalesce same exists exprs");
      OPT_TRACE("left:", filters.at(i));
      OPT_TRACE("right:", filters.at(j));
      if (removed_items.has_member(j)) {
        // do nothing
      } else if (OB_ISNULL(first_query_ref) || OB_ISNULL(second_query_ref) ||
                 OB_ISNULL(first_query_ref->get_ref_stmt()) || OB_ISNULL(second_query_ref->get_ref_stmt())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("query ref is invalid", K(ret));
      } else if (OB_FAIL(check_hint_valid(*stmt, 
                                          *first_query_ref->get_ref_stmt(),
                                          *second_query_ref->get_ref_stmt(), 
                                          force_trans,
                                          force_no_trans))) {
        LOG_WARN("failed to check hint valid", K(ret));
      } else if (force_no_trans) {
        //do nothing
        OPT_TRACE("hint reject transform");
      } else if (OB_FAIL(ObStmtComparer::check_stmt_containment(first_query_ref->get_ref_stmt(),
                                                                second_query_ref->get_ref_stmt(),
                                                                map_info, relation))) {
        LOG_WARN("failed to check stmt containment", K(ret));
      } else if (relation == QUERY_LEFT_SUBSET || relation == QUERY_EQUAL) {
        remove_index = (type == T_OP_EXISTS ? j : i);
        OPT_TRACE("right query contain left query, will coalesce subquery");
      } else if (relation == QUERY_RIGHT_SUBSET) {
        remove_index = (type == T_OP_EXISTS ? i : j);
        OPT_TRACE("left query contain right query, will coalesce subquery");
      } else {
        OPT_TRACE("stmt do not contain each other, will not coalesce");
      }
      if (OB_SUCC(ret) && remove_index != -1) {
        if (OB_FAIL(removed_items.add_member(remove_index))) {
          LOG_WARN("failed to add member", K(ret));
        } else if (OB_FAIL(append(equal_infos, map_info.equal_param_map_))) {
          LOG_WARN("failed to append equal param map", K(ret));
        } else if (OB_FAIL(add_coalesce_stmt(first_query_ref->get_ref_stmt(), 
                                             second_query_ref->get_ref_stmt()))) {
          LOG_WARN("failed to add coalesce stmts", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret) && (!removed_items.is_empty())) {
    ObSEArray<ObRawExpr *, 4> new_filters;
    for (int64_t i = 0; OB_SUCC(ret) && i < filters.count(); ++i) {
      if (!removed_items.has_member(i)) {
        // the filter is not removed
        if (OB_FAIL(new_filters.push_back(filters.at(i)))) {
          LOG_WARN("failed to push back filter", K(ret));
        }
      } else if (OB_FAIL(ObOptimizerUtil::remove_item(stmt->get_subquery_exprs(),
                                                      get_exists_query_expr(filters.at(i))))) {
        LOG_WARN("failed to remove subquery expr", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(filters.assign(new_filters))) {
        LOG_WARN("failed to assign new filters", K(ret));
      } else if (OB_FAIL(append(ctx_->equal_param_constraints_, equal_infos))) {
        LOG_WARN("failed to append equal infos", K(ret));
      } else {
        is_happened = true;
      }
    }
  }
  return ret;
}

int ObTransformSubqueryCoalesce::coalesce_same_any_all_exprs(ObDMLStmt *stmt,
                                                             const ObItemType type,
                                                             ObIArray<ObRawExpr *> &filters,
                                                             bool &is_happened)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObPCParamEqualInfo, 4> equal_infos;
  ObStmtMapInfo map_info;
  QueryRelation relation;
  ObQueryRefRawExpr *first_query_ref = NULL;
  ObRawExpr *first_left_expr = NULL;
  ObQueryRefRawExpr *second_query_ref = NULL;
  ObRawExpr *second_left_expr = NULL;
  ObSqlBitSet<> removed_items;
  int64_t remove_index = -1;
  is_happened = false;
  bool force_trans = false;
  bool force_no_trans = false;
  bool is_select_same = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(stmt), K(ctx_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < filters.count(); ++i) {
      first_left_expr = get_any_all_left_hand_expr(filters.at(i));
      first_query_ref = get_any_all_query_expr(filters.at(i));
      for (int64_t j = i + 1; OB_SUCC(ret) && !removed_items.has_member(i) && j < filters.count(); ++j) {
        second_left_expr = get_any_all_left_hand_expr(filters.at(j));
        second_query_ref = get_any_all_query_expr(filters.at(j));
        map_info.reset();
        remove_index = -1;
        OPT_TRACE("try to coalesce same any/all exprs");
        OPT_TRACE("left:", filters.at(i));
        OPT_TRACE("right:", filters.at(j));
        if (removed_items.has_member(j)) {
          // do nothing
        } else if (OB_ISNULL(first_left_expr) || OB_ISNULL(first_query_ref)
                  || OB_ISNULL(second_left_expr) || OB_ISNULL(second_query_ref)
                  || OB_ISNULL(first_query_ref->get_ref_stmt()) || OB_ISNULL(second_query_ref->get_ref_stmt())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("query ref is invalid", K(ret));
        } else if (!first_left_expr->same_as(*second_left_expr)) {
          /*do nothing*/
          OPT_TRACE("left param expr not same, can not coalesce");
        } else if (OB_FAIL(check_hint_valid(*stmt, 
                                            *first_query_ref->get_ref_stmt(), 
                                            *second_query_ref->get_ref_stmt(), 
                                            force_trans, 
                                            force_no_trans))) {
          LOG_WARN("failed to check hint valid", K(ret));
        } else if (force_no_trans) {
          //do nothing
          OPT_TRACE("hint reject transform");
        } else if (OB_FAIL(ObStmtComparer::check_stmt_containment(first_query_ref->get_ref_stmt(),
                                                                  second_query_ref->get_ref_stmt(),
                                                                  map_info, relation))) {
          LOG_WARN("failed to check stmt containment", K(ret));
        } else if (!map_info.is_select_item_equal_) {
          OPT_TRACE("stmts have different select items, can not coalesce");
        } else if (OB_FAIL(check_select_items_same(first_query_ref->get_ref_stmt(),
                                           second_query_ref->get_ref_stmt(),
                                           map_info,
                                           is_select_same))) {
          LOG_WARN("check select items failed", K(ret));
        } else if (!is_select_same) {
          OPT_TRACE("The order of select items in two stmts is different, can not coalesce");
        } else if (relation == QUERY_LEFT_SUBSET || relation == QUERY_EQUAL) {
          remove_index = (type == T_ANY ? j : i);
          OPT_TRACE("right query contain left query, will coalesce suqbeury");
        } else if (relation == QUERY_RIGHT_SUBSET) {
          remove_index = (type == T_ANY ? i : j);
          OPT_TRACE("left query contain right query, will coalesce suqbeury");
        } else {
          OPT_TRACE("stmt not contain each other, can not coalesce");
        }
        if (OB_SUCC(ret) && remove_index != -1) {
          if (OB_FAIL(removed_items.add_member(remove_index))) {
            LOG_WARN("failed to add member", K(ret));
          } else if (OB_FAIL(append(equal_infos, map_info.equal_param_map_))) {
            LOG_WARN("failed to append equal param map", K(ret));
          } else if (OB_FAIL(add_coalesce_stmt(first_query_ref->get_ref_stmt(), 
                                              second_query_ref->get_ref_stmt()))) {
            LOG_WARN("failed to add coalesce stmts", K(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret) && (!removed_items.is_empty())) {
      ObSEArray<ObRawExpr *, 4> new_filters;
      ObOpRawExpr *temp_op = NULL;
      for (int64_t i = 0; OB_SUCC(ret) && i < filters.count(); ++i) {
        temp_op = static_cast<ObOpRawExpr*>(filters.at(i));
        if (!removed_items.has_member(i)) {
          // the filter is not removed
          if (OB_FAIL(new_filters.push_back(filters.at(i)))) {
            LOG_WARN("failed to push back filter", K(ret));
          }
        } else if (OB_FAIL(ObOptimizerUtil::remove_item(stmt->get_subquery_exprs(),
                                                        static_cast<ObQueryRefRawExpr *>(temp_op->get_param_expr(1))))) {
          LOG_WARN("failed to remove subquery expr", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(filters.assign(new_filters))) {
          LOG_WARN("failed to assign new filters", K(ret));
        } else if (OB_FAIL(append(ctx_->equal_param_constraints_, equal_infos))) {
          LOG_WARN("failed to append equal infos", K(ret));
        } else {
          is_happened = true;
        }
      }
    }
  }
  return ret;
}

int ObTransformSubqueryCoalesce::transform_diff_exprs(
    ObDMLStmt *stmt,
    ObDMLStmt *&trans_stmt,
    ObIArray<ObPCParamEqualInfo> &rule_based_equal_infos,
    ObIArray<ObPCParamEqualInfo> &cost_based_equal_infos,
    bool &rule_based_trans_happened)
{
  int ret = OB_SUCCESS;
  ObSEArray<TransformParam, 1> where_params;
  ObSEArray<TransformParam, 1> having_params;
  bool where_is_false = false;
  bool having_is_false = false;
  bool hint_force_trans = false;
  ObSelectStmt *select_stmt = NULL;
  ObSelectStmt *select_trans_stmt = NULL;
  rule_based_trans_happened = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->stmt_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(stmt), K(ctx_));
  } else if (stmt->is_select_stmt() && OB_ISNULL(select_stmt = static_cast<ObSelectStmt*>(stmt))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(stmt), K(select_stmt));
  } else if (OB_FAIL(check_conditions_validity(stmt, 
                                               stmt->get_condition_exprs(), 
                                               where_params, 
                                               where_is_false,
                                               hint_force_trans))) {
    LOG_WARN("failed to check condition validity", K(ret));
  } else if (OB_NOT_NULL(select_stmt) &&
             OB_FAIL(check_conditions_validity(stmt, 
                                               select_stmt->get_having_exprs(),
                                               having_params,
                                               having_is_false,
                                               hint_force_trans))) {
    LOG_WARN("failed to check having validity", K(ret));
  } else if (where_is_false && OB_FAIL(make_false(stmt->get_condition_exprs())))  {
    LOG_WARN("failed to make condition false", K(ret));
  } else if (having_is_false && OB_FAIL(make_false(select_stmt->get_having_exprs()))) {
    LOG_WARN("failed to make condition false", K(ret));
  } else if ((where_is_false || where_params.empty()) &&
             (having_is_false || having_params.empty())) {
    // do nothing
  } else if (!hint_force_trans && 
             OB_FAIL(ObTransformUtils::copy_stmt(*ctx_->stmt_factory_, stmt, trans_stmt))) {
    LOG_WARN("failed to copy stmt", K(ret));
  } else if (hint_force_trans &&
             OB_FALSE_IT(trans_stmt = stmt)) {
  } else if (trans_stmt->is_select_stmt() &&
             OB_ISNULL(select_trans_stmt = static_cast<ObSelectStmt*>(trans_stmt))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(trans_stmt), K(select_trans_stmt));
  } else if (!where_is_false && OB_FAIL(coalesce_diff_exists_exprs(trans_stmt, trans_stmt->get_condition_exprs(), where_params))) {
    LOG_WARN("failed to do coalesce diff where conditions", K(ret));
  //bug:
  // } else if (OB_NOT_NULL(select_trans_stmt) && !having_is_false &&
  //            OB_FAIL(coalesce_diff_exists_exprs(trans_stmt,
  //                                               select_trans_stmt->get_having_exprs(),
  //                                               having_params))) {
  //   LOG_WARN("failed to do coalesce diff having conditions", K(ret));
  } else if (!where_is_false && OB_FAIL(coalesce_diff_any_all_exprs(trans_stmt, trans_stmt->get_condition_exprs(), where_params))) {
    LOG_WARN("failed to do coalesce diff where conditions", K(ret));
  } else if (OB_NOT_NULL(select_trans_stmt) && !having_is_false &&
             OB_FAIL(coalesce_diff_any_all_exprs(trans_stmt,
                                                 select_trans_stmt->get_having_exprs(),
                                                 having_params))) {
    LOG_WARN("failed to do coalesce diff having conditions", K(ret));
  }
  if (OB_SUCC(ret)) {
    rule_based_trans_happened = where_is_false || having_is_false || hint_force_trans;
    ObIArray<ObPCParamEqualInfo> *where_equal_infos = where_is_false ?
        &rule_based_equal_infos : &cost_based_equal_infos;
    ObIArray<ObPCParamEqualInfo> *having_equal_infos = having_is_false ?
        &rule_based_equal_infos : &cost_based_equal_infos;
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

int ObTransformSubqueryCoalesce::check_conditions_validity(ObDMLStmt *stmt,
                                                           ObIArray<ObRawExpr *> &conds,
                                                           ObIArray<TransformParam> &trans_params,
                                                           bool &has_false_conds,
                                                           bool &hint_force_trans)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> validity_exprs;
  TransformParam param;
  ObSqlBitSet<> removed;
  QueryRelation relation = QueryRelation::QUERY_UNCOMPARABLE;
  has_false_conds = false;
  bool is_used = false;//用于标记一个子查询已经与一个子查询结合,防止一个子查询二次结合,从而出错
  bool force_trans = false;
  bool force_no_trans = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  } else if (OB_FAIL(classify_conditions(conds, validity_exprs))) {
    LOG_WARN("failed to check conditions validity", K(ret));
  } else {
    ObSEArray<ObRawExpr*, 4> left_exprs;
    ObSEArray<ObRawExpr*, 4> right_exprs;
    ObItemType left_type[7] = {T_OP_EXISTS, T_OP_SQ_EQ, T_OP_SQ_NE, T_OP_SQ_LT, T_OP_SQ_LE,
                               T_OP_SQ_GT, T_OP_SQ_GE};
    ObItemType right_type[7] = {T_OP_NOT_EXISTS, T_OP_SQ_NE, T_OP_SQ_EQ, T_OP_SQ_GE, T_OP_SQ_GT,
                                T_OP_SQ_LE, T_OP_SQ_LT};
    for (int64_t k = 0; OB_SUCC(ret) && !has_false_conds && k < 7; ++k) {
      left_exprs.reset();
      right_exprs.reset();
      if (T_OP_EXISTS == left_type[k]) {//exists
        if (OB_FAIL(get_same_classify_exprs(validity_exprs, left_exprs,
                                            left_type[k], CNT_SUB_QUERY))) {
          LOG_WARN("get the same classify exprs failed", K(ret));
        } else if (OB_FAIL(get_same_classify_exprs(validity_exprs, right_exprs,
                                                   right_type[k], CNT_SUB_QUERY))) {
          LOG_WARN("get the same classify exprs failed", K(ret));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && !has_false_conds && i < left_exprs.count(); ++i) {
            ObQueryRefRawExpr *exists_query = get_exists_query_expr(left_exprs.at(i));
            is_used = false;
            for (int64_t j = 0; OB_SUCC(ret) && !is_used && !has_false_conds &&
                 j < right_exprs.count(); ++j) {
              ObQueryRefRawExpr *not_exists_query = get_exists_query_expr(right_exprs.at(j));
              if (!removed.has_member(j)) {
                OPT_TRACE("try to coalesce exists subquery:");
                OPT_TRACE("exists expr:", left_exprs.at(i));
                OPT_TRACE("not exists expr:", right_exprs.at(j));
                param.exists_expr_ = left_exprs.at(i);
                param.not_exists_expr_ = right_exprs.at(j);
                param.trans_flag_ = EXISTS_NOT_EXISTS;
                param.map_info_.reset();
                if (OB_ISNULL(exists_query) || OB_ISNULL(not_exists_query) ||
                    OB_ISNULL(exists_query->get_ref_stmt()) || OB_ISNULL(not_exists_query->get_ref_stmt())) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("query ref exprs are null", K(ret));
                } else if (OB_FAIL(check_hint_valid(*stmt, 
                                                    *not_exists_query->get_ref_stmt(), 
                                                    *exists_query->get_ref_stmt(), 
                                                    force_trans, 
                                                    force_no_trans))) {
                  LOG_WARN("failed to check hint valid", K(ret));
                } else if (force_no_trans) {
                  //do nothing
                  OPT_TRACE("hint reject transform");
                } else if (OB_FALSE_IT(hint_force_trans |= force_trans)) {
                } else if (OB_FAIL(ObStmtComparer::check_stmt_containment(
                                   not_exists_query->get_ref_stmt(),
                                   exists_query->get_ref_stmt(),
                                   param.map_info_,
                                   relation))) {
                  LOG_WARN("failed to check stmt containment", K(ret));
                } else if (relation == QueryRelation::QUERY_RIGHT_SUBSET ||
                           relation == QueryRelation::QUERY_EQUAL) {
                  has_false_conds = true;
                  is_used = true;
                  trans_params.reset();
                  if (OB_FAIL(trans_params.push_back(param))) {
                    LOG_WARN("failed to push back transform param", K(ret));
                  } else if (OB_FAIL(add_coalesce_stmt(not_exists_query->get_ref_stmt(), 
                                                       exists_query->get_ref_stmt()))) {
                    LOG_WARN("failed to add coalesce stmts", K(ret));
                  } else {
                    OPT_TRACE("left stmt contain right stmt, will coalesce");
                  }
                } else if (relation == QueryRelation::QUERY_LEFT_SUBSET) {
                  if (OB_FAIL(removed.add_member(j))) {
                    LOG_WARN("failed to add member into bit set", K(ret));
                  } else if (OB_FAIL(trans_params.push_back(param))) {
                    LOG_WARN("failed to push back transform param", K(ret));
                  } else if (OB_FAIL(add_coalesce_stmt(not_exists_query->get_ref_stmt(), 
                                                       exists_query->get_ref_stmt()))) {
                    LOG_WARN("failed to add coalesce stmts", K(ret));
                  } else {
                    is_used = true;
                    OPT_TRACE("right stmt contain left stmt, will coalesce");
                  }
                } else {
                  OPT_TRACE("stmt not contain each other, will not coalesce");
                }
              }
            }
          }
        }
      } else if (!has_false_conds) {//any、all
       if (OB_FAIL(get_same_classify_exprs(validity_exprs, left_exprs,
                                           left_type[k], IS_WITH_ANY))) {
          LOG_WARN("get the same classify exprs failed", K(ret));
        } else if (OB_FAIL(get_same_classify_exprs(validity_exprs, right_exprs,
                                                   right_type[k], IS_WITH_ALL))) {
          LOG_WARN("get the same classify exprs failed", K(ret));
        } else if ((T_OP_SQ_LT == left_type[k] || T_OP_SQ_GT == left_type[k]) && k + 1 < 7 &&
                   OB_FAIL(get_same_classify_exprs(validity_exprs, right_exprs,
                                                   right_type[k + 1], IS_WITH_ALL))) {//>any vs <all、<=all...
          LOG_WARN("get the same classify exprs failed", K(ret));
        } else {
          removed.reset();
          bool can_coalesce = (left_type[k] == T_OP_SQ_EQ) ? true : false;//仅仅in于not in可以合并为lnnvl这种情形
          for (int64_t i = 0; OB_SUCC(ret) && !has_false_conds && i < left_exprs.count(); ++i) {
            param.any_expr_ = left_exprs.at(i);
            param.trans_flag_ = ANY_ALL;
            is_used = false;
            for (int64_t j = 0; OB_SUCC(ret) && !is_used && !has_false_conds && j < right_exprs.count(); ++j) {
              if (!removed.has_member(j)) {
                param.all_expr_ = right_exprs.at(j);
                if (OB_FAIL(compare_any_all_subqueries(stmt,
                                                       param,
                                                       trans_params,
                                                       has_false_conds,
                                                       is_used,
                                                       can_coalesce,
                                                       hint_force_trans))) {
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

int ObTransformSubqueryCoalesce::compare_any_all_subqueries(ObDMLStmt *stmt,
                                                            TransformParam &param, 
                                                            ObIArray<TransformParam> &trans_params,
                                                            bool &has_false_conds,
                                                            bool &is_used,
                                                            bool can_coalesce,
                                                            bool &hint_force_trans)
{
  int ret = OB_SUCCESS;
  bool force_trans = false;
  bool force_no_trans = false;
  has_false_conds = false;
  is_used = false;
  QueryRelation relation = QueryRelation::QUERY_UNCOMPARABLE;
  ObRawExpr* first_left_expr = get_any_all_left_hand_expr(param.any_expr_);
  ObQueryRefRawExpr* first_query_ref = get_any_all_query_expr(param.any_expr_);
  ObRawExpr* second_left_expr = get_any_all_left_hand_expr(param.all_expr_);
  ObQueryRefRawExpr* second_query_ref = get_any_all_query_expr(param.all_expr_);
  bool is_select_same = false;
  OPT_TRACE("try to coalesce any/all subquery:");
  OPT_TRACE("any expr:", param.any_expr_);
  OPT_TRACE("all expr:", param.all_expr_);
  if (OB_ISNULL(first_left_expr) || OB_ISNULL(first_query_ref)
      || OB_ISNULL(second_left_expr) || OB_ISNULL(second_query_ref)
      || OB_ISNULL(first_query_ref->get_ref_stmt()) 
      || OB_ISNULL(second_query_ref->get_ref_stmt())
      || OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("query ref exprs are null", K(ret));
  } else if (!first_left_expr->same_as(*second_left_expr)) {
    /*do nothing*/
    OPT_TRACE("left param expr not same, can not transform");
  } else if (OB_FAIL(check_hint_valid(*stmt, 
                                      *first_query_ref->get_ref_stmt(), 
                                      *second_query_ref->get_ref_stmt(), 
                                      force_trans, 
                                      force_no_trans))) {
    LOG_WARN("failed to check hint valid", K(ret));
  } else if (force_no_trans) {
    //do nothing
    OPT_TRACE("hint reject transform");
  } else if (OB_FALSE_IT(hint_force_trans |= force_trans)) {
  } else if (OB_FAIL(ObStmtComparer::check_stmt_containment(second_query_ref->get_ref_stmt(),
                                                            first_query_ref->get_ref_stmt(),
                                                            param.map_info_,
                                                            relation))) {
    LOG_WARN("failed to check stmt containment", K(ret));
  } else if (!param.map_info_.is_select_item_equal_) {
    OPT_TRACE("stmts have different select items, can not coalesce");
  } else if (OB_FAIL(check_select_items_same(first_query_ref->get_ref_stmt(),
                                      second_query_ref->get_ref_stmt(),
                                      param.map_info_,
                                      is_select_same))) {
    LOG_WARN("check select items failed", K(ret));
  } else if (!is_select_same) {
    OPT_TRACE("The order of select items in two stmts is different, can not coalesce");
  } else if (relation == QueryRelation::QUERY_RIGHT_SUBSET ||
             relation == QueryRelation::QUERY_EQUAL) {
    has_false_conds = true;
    trans_params.reset();
    is_used = true;
    if (OB_FAIL(trans_params.push_back(param))) {
      LOG_WARN("failed to push back transform param", K(ret));
    } else if (OB_FAIL(add_coalesce_stmt(first_query_ref->get_ref_stmt(), 
                                          second_query_ref->get_ref_stmt()))) {
      LOG_WARN("failed to add coalesce stmts", K(ret));
    } else {
      OPT_TRACE("left stmt contain right stmt, will coalesce");
    }
  } else if (can_coalesce && relation == QUERY_LEFT_SUBSET) {
    if (OB_FAIL(trans_params.push_back(param))) {
      LOG_WARN("failed to push back transform param", K(ret));
    } else if (OB_FAIL(add_coalesce_stmt(first_query_ref->get_ref_stmt(), 
                                          second_query_ref->get_ref_stmt()))) {
      LOG_WARN("failed to add coalesce stmts", K(ret));
    } else {
      is_used = true;
      OPT_TRACE("right stmt contain left stmt, will coalesce");
    }
  } else {
    OPT_TRACE("stmt not contain each other, will not coalesce");
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
int ObTransformSubqueryCoalesce::coalesce_diff_exists_exprs(ObDMLStmt *stmt,
                                                            ObIArray<ObRawExpr *> &cond_exprs,
                                                            ObIArray<TransformParam> &trans_params)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < trans_params.count(); ++i) {
    ObRawExpr *new_exists_expr = NULL;
    TransformParam &param = trans_params.at(i);
    if (param.trans_flag_ == EXISTS_NOT_EXISTS) {
      ObQueryRefRawExpr *exist_query = get_exists_query_expr(param.exists_expr_);
      ObQueryRefRawExpr *not_exist_query = get_exists_query_expr(param.not_exists_expr_);
      ObQueryRefRawExpr *new_exists_query = NULL;
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
int ObTransformSubqueryCoalesce::coalesce_diff_any_all_exprs(ObDMLStmt *stmt,
                                                             ObIArray<ObRawExpr *> &cond_exprs,
                                                             ObIArray<TransformParam> &trans_params)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null ptr", K(stmt), K(ret));
  } else {
    ObQueryRefRawExpr *any_ref_expr = NULL;
    ObQueryRefRawExpr *all_ref_expr = NULL;
    ObRawExpr *old_any_expr = NULL;
    ObRawExpr *old_all_expr = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < trans_params.count(); ++i) {
      ObRawExpr *new_any_all_expr = NULL;
      TransformParam &param = trans_params.at(i);
      if (param.trans_flag_ == ANY_ALL) {
          any_ref_expr = get_any_all_query_expr(param.any_expr_);
          all_ref_expr = get_any_all_query_expr(param.all_expr_);
          old_any_expr = param.any_expr_;
          old_all_expr = param.all_expr_;
        ObQueryRefRawExpr *new_any_all_query = NULL;
        if (OB_ISNULL(any_ref_expr) || OB_ISNULL(all_ref_expr) 
            || OB_ISNULL(old_any_expr) || OB_ISNULL(old_all_expr)) {
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
        } else if (OB_FAIL(stmt->pull_all_expr_relation_id())) {
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

int ObTransformSubqueryCoalesce::merge_exists_subqueries(TransformParam &trans_param,
                                                         ObRawExpr *&new_exist_expr)
{
  int ret = OB_SUCCESS;
  ObRawExprFactory *expr_factory = NULL;
  ObStmtFactory *stmt_factory = NULL;
  ObSEArray<ObRawExpr *, 4> old_cols;
  ObSEArray<ObRawExpr *, 4> new_cols;
  ObSEArray<ObRawExpr *, 4> extra_conds;
  ObQueryRefRawExpr *exist_query_ref = NULL;
  ObQueryRefRawExpr *not_exist_query_ref = NULL;
  ObQueryRefRawExpr *new_query_ref = NULL;
  ObSelectStmt *exist_stmt = NULL;
  ObSelectStmt *not_exist_stmt = NULL;
  ObSelectStmt *new_exist_stmt = NULL;
  ObSEArray<ObRawExpr *, 4> old_exec_params;
  ObSEArray<ObRawExpr *, 4> new_exec_params;
  const ObStmtMapInfo &map_info = trans_param.map_info_;
  if (OB_ISNULL(ctx_) ||
      OB_ISNULL(expr_factory = ctx_->expr_factory_) ||
      OB_ISNULL(stmt_factory = ctx_->stmt_factory_) ||
      OB_ISNULL(exist_query_ref = get_exists_query_expr(trans_param.exists_expr_)) ||
      OB_ISNULL(not_exist_query_ref = get_exists_query_expr(trans_param.not_exists_expr_)) ||
      OB_ISNULL(exist_stmt = exist_query_ref->get_ref_stmt()) ||
      OB_ISNULL(not_exist_stmt = not_exist_query_ref->get_ref_stmt()) ||
      OB_UNLIKELY(map_info.cond_map_.count() != not_exist_stmt->get_condition_size()) ||
      OB_UNLIKELY(map_info.table_map_.count() != not_exist_stmt->get_table_size())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("exist exprs are not valid", K(ret), K(expr_factory), K(stmt_factory),
             K(exist_query_ref), K(not_exist_query_ref), K(exist_stmt), K(not_exist_stmt));
  } else if (OB_FAIL(expr_factory->create_raw_expr(trans_param.exists_expr_->get_expr_class(),
                                                   trans_param.exists_expr_->get_expr_type(),
                                                   new_exist_expr))) {
    LOG_WARN("failed to create raw expr", K(ret));
  } else if (OB_FAIL(new_exist_expr->assign(*trans_param.exists_expr_))) {
    LOG_WARN("failed to assign expr", K(ret));
  } else if (OB_FAIL(expr_factory->create_raw_expr(T_REF_QUERY, new_query_ref))) {
    LOG_WARN("failed to create query ref expr", K(ret));
  } else if (OB_FAIL(new_query_ref->assign(*exist_query_ref))) {
    LOG_WARN("failed to assign query ref expr", K(ret));
  } else if (OB_FAIL(ObTransformUtils::copy_stmt(*stmt_factory,
                                                 exist_stmt,
                                                 reinterpret_cast<ObDMLStmt *&>(new_exist_stmt)))) {
    LOG_WARN("failed to copy exist stmt", K(ret));
  } else if (OB_FAIL(new_exist_stmt->adjust_statement_id(ctx_->allocator_,
                                                         ctx_->src_qb_name_,
                                                         ctx_->src_hash_val_))) {
    LOG_WARN("failed to adjust statement id", K(ret));
  } else {
    new_query_ref->set_ref_stmt(new_exist_stmt);
    new_exist_expr->get_param_expr(0) = new_query_ref;
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(merge_exec_params(not_exist_query_ref,
                                  new_query_ref,
                                  old_exec_params,
                                  new_exec_params))) {
      LOG_WARN("failed to merge exec params", K(ret));
    }
  }
  if (OB_SUCC(ret)) {

  }
  for (int64_t i = 0; OB_SUCC(ret) && i < not_exist_stmt->get_column_size(); ++i) {
    ColumnItem &col_item = not_exist_stmt->get_column_items().at(i);
    ObColumnRefRawExpr *col_expr = not_exist_stmt->get_column_items().at(i).expr_;
    ObRawExpr *new_expr = NULL;
    TableItem *new_table_item = NULL;
    ObRawExpr *error_expr = NULL;
    ObRawExpr *empty_expr = NULL;
    int64_t idx = not_exist_stmt->get_table_bit_index(col_item.table_id_) - 1;
    if (idx < 0 || idx >= map_info.table_map_.count() ||
        map_info.table_map_.at(idx) < 0 ||
        map_info.table_map_.at(idx) >= new_exist_stmt->get_table_size() ||
        OB_ISNULL(new_table_item = new_exist_stmt->get_table_item(map_info.table_map_.at(idx)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table is not mapped", K(ret), K(idx));
    } else if (OB_NOT_NULL(new_expr = new_exist_stmt->get_column_expr_by_id(new_table_item->table_id_,
                                                                            col_item.column_id_))) {
      // do nothing
    } else if (OB_FAIL(ObRawExprCopier::copy_expr_node(*expr_factory, col_item.expr_, new_expr))) {
      LOG_WARN("failed to copy expr node", K(ret));
    } else if (OB_NOT_NULL(col_item.default_value_expr_)
               && OB_FAIL(ObRawExprCopier::copy_expr_node(*expr_factory, col_item.default_value_expr_, error_expr))) {
      LOG_WARN("failed to error expr node", K(ret));
    } else if (OB_NOT_NULL(col_item.default_empty_expr_)
               && OB_FAIL(ObRawExprCopier::copy_expr_node(*expr_factory, col_item.default_empty_expr_, empty_expr))) {
      LOG_WARN("failed to empty expr node", K(ret));
    } else {
      ColumnItem new_col_item;
      new_col_item.table_id_ = new_table_item->table_id_;
      new_col_item.column_id_ = col_item.column_id_;
      new_col_item.column_name_ = col_item.column_name_;
      new_col_item.default_value_expr_ = error_expr;
      new_col_item.default_empty_expr_ = empty_expr;
      new_col_item.expr_ = static_cast<ObColumnRefRawExpr *>(new_expr);
      new_col_item.expr_->set_table_id(new_table_item->table_id_);
      new_col_item.expr_->set_table_name(new_table_item->table_name_);
      new_col_item.is_geo_ = col_item.is_geo_;
      new_col_item.col_idx_= col_item.col_idx_;
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
    ObRawExprCopier copier(*expr_factory);
    if (OB_FAIL(copier.add_replaced_expr(old_exec_params, new_exec_params))) {
      LOG_WARN("failed to add replace pair", K(ret));
    } else if (OB_FAIL(copier.add_replaced_expr(old_cols, new_cols))) {
      LOG_WARN("failed to add replace pair", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < not_exist_stmt->get_condition_size(); ++i) {
      ObRawExpr *new_cond_expr = NULL;
      if (map_info.cond_map_.at(i) >= 0 &&
          map_info.cond_map_.at(i) < new_exist_stmt->get_condition_size()) {
        // both stmt has the condition
      } else if (OB_FAIL(copier.copy_on_replace(not_exist_stmt->get_condition_expr(i),
                                                new_cond_expr))) {
        LOG_WARN("failed to copy on replace condition expr", K(ret));
      } else if (OB_FAIL(extra_conds.push_back(new_cond_expr))) {
        LOG_WARN("failed to push back condition", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    // create having exprs
    // having sum(case when conds then 1 else 0 end) = 0
    ObCaseOpRawExpr *case_expr = NULL;
    ObConstRawExpr *one_expr = NULL;
    ObConstRawExpr *zero_expr = NULL;
    ObConstRawExpr *equal_value = NULL;
    ObAggFunRawExpr *sum_expr = NULL;
    ObRawExpr *equal_expr = NULL;
    ObRawExpr *when_expr = NULL;
    if (OB_FAIL(expr_factory->create_raw_expr(T_OP_CASE, case_expr))) {
      LOG_WARN("failed to create case expr", K(ret));
    } else if (OB_FAIL(create_and_expr(extra_conds, when_expr))) {
      LOG_WARN("failed to create and expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(
                         *expr_factory, ObIntType, 1L, one_expr))) {
      LOG_WARN("failed to build const int expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(
                         *expr_factory, ObIntType, 0L, zero_expr))) {
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
    } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(
                         *expr_factory, ObIntType, 0L, equal_value))) {
      LOG_WARN("faield to build const int expr", K(ret));
    } else if (OB_FAIL(sum_expr->add_real_param_expr(case_expr))) {
      LOG_WARN("failed to add real param expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::create_double_op_expr(
                         *expr_factory,
                         ctx_->session_info_,
                         T_OP_EQ,
                         equal_expr,
                         sum_expr,
                         equal_value))) {
      LOG_WARN("failed to create equal expr", K(ret));
    } else if (OB_FAIL(equal_expr->formalize(ctx_->session_info_))) {
      LOG_WARN("failed to formalize equal expr", K(ret));
    } else if (OB_FAIL(new_exist_stmt->add_agg_item(*sum_expr))) {
      LOG_WARN("failed to add aggr item", K(ret));
    } else if (OB_FAIL(new_exist_stmt->add_having_expr(equal_expr))) {
      LOG_WARN("failed to add having expr", K(ret));
    }
  }
  return ret;
}

int ObTransformSubqueryCoalesce::merge_any_all_subqueries(ObQueryRefRawExpr *any_query_ref,
                                                          ObQueryRefRawExpr *all_query_ref,
                                                          TransformParam &trans_param,
                                                          ObRawExpr *&new_any_all_query)
{
  int ret = OB_SUCCESS;
  ObRawExprFactory *expr_factory = NULL;
  ObStmtFactory *stmt_factory = NULL;
  ObSEArray<ObRawExpr *, 4> old_cols;
  ObSEArray<ObRawExpr *, 4> new_cols;
  ObSEArray<ObRawExpr *, 4> extra_conds;
  ObSEArray<ObRawExpr *, 4> old_exec_params;
  ObSEArray<ObRawExpr *, 4> new_exec_params;
  ObQueryRefRawExpr *new_query_ref = NULL;
  ObSelectStmt *any_stmt = NULL;
  ObSelectStmt *all_stmt = NULL;
  ObSelectStmt *new_any_stmt = NULL;
  const ObStmtMapInfo &map_info = trans_param.map_info_;
  if (OB_ISNULL(ctx_) ||
      OB_ISNULL(expr_factory = ctx_->expr_factory_) ||
      OB_ISNULL(stmt_factory = ctx_->stmt_factory_) ||
      OB_ISNULL(any_query_ref) ||
      OB_ISNULL(all_query_ref) ||
      OB_ISNULL(any_stmt = any_query_ref->get_ref_stmt()) ||
      OB_ISNULL(all_stmt = all_query_ref->get_ref_stmt()) ||
      OB_UNLIKELY(map_info.cond_map_.count() != all_stmt->get_condition_size()) ||
      OB_UNLIKELY(map_info.table_map_.count() != all_stmt->get_table_size())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("any/all exprs are not valid", K(ret), K(expr_factory), K(stmt_factory),
             K(any_query_ref), K(all_query_ref), K(any_stmt), K(all_stmt));
  } else if (OB_FAIL(ObRawExprCopier::copy_expr_node(*expr_factory,
                                             trans_param.any_expr_,
                                             new_any_all_query))) {
    LOG_WARN("failed to copy expr node", K(ret));
  } else if (OB_FAIL(expr_factory->create_raw_expr(T_REF_QUERY, new_query_ref))) {
    LOG_WARN("failed to create query ref expr", K(ret));
  } else if (OB_FAIL(new_query_ref->assign(*any_query_ref))) {
    LOG_WARN("failed to assign query ref expr", K(ret));
  } else if (OB_FAIL(ObTransformUtils::copy_stmt(*stmt_factory,
                                                 any_stmt,
                                                 reinterpret_cast<ObDMLStmt *&>(new_any_stmt)))) {
    LOG_WARN("failed to copy any stmt", K(ret));
  } else if (OB_FAIL(new_any_stmt->adjust_statement_id(ctx_->allocator_,
                                                       ctx_->src_qb_name_,
                                                       ctx_->src_hash_val_))) {
    LOG_WARN("failed to adjust statement id", K(ret));
  } else {
    new_query_ref->set_ref_stmt(new_any_stmt);
    new_any_all_query->get_param_expr(1) = new_query_ref;
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(merge_exec_params(all_query_ref,
                                  any_query_ref,
                                  old_exec_params,
                                  new_exec_params))) {
      LOG_WARN("failed to merge exec params", K(ret));
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < all_stmt->get_column_size(); ++i) {
    ColumnItem &col_item = all_stmt->get_column_items().at(i);
    ObColumnRefRawExpr *col_expr = all_stmt->get_column_items().at(i).expr_;
    ObRawExpr *new_expr = NULL;
    TableItem *new_table_item = NULL;
    ObRawExpr *error_expr = NULL;
    ObRawExpr *empty_expr = NULL;
    int64_t idx = all_stmt->get_table_bit_index(col_item.table_id_) - 1;
    if (OB_UNLIKELY(idx < 0 || idx >= map_info.table_map_.count() ||
        map_info.table_map_.at(idx) < 0 ||
        map_info.table_map_.at(idx) >= new_any_stmt->get_table_size() ||
        OB_ISNULL(new_table_item = new_any_stmt->get_table_item(map_info.table_map_.at(idx))))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table is not mapped", K(ret), K(idx));
    } else if (OB_NOT_NULL(new_expr = new_any_stmt->get_column_expr_by_id(new_table_item->table_id_,
                                                                            col_item.column_id_))) {
      // do nothing
    } else if (OB_FAIL(ObRawExprCopier::copy_expr_node(*expr_factory, col_item.expr_, new_expr))) {
      LOG_WARN("failed to copy expr node", K(ret));
    } else if (OB_NOT_NULL(col_item.default_value_expr_)
               && OB_FAIL(ObRawExprCopier::copy_expr_node(*expr_factory, col_item.default_value_expr_, error_expr))) {
      LOG_WARN("failed to error expr node", K(ret));
    } else if (OB_NOT_NULL(col_item.default_empty_expr_)
               && OB_FAIL(ObRawExprCopier::copy_expr_node(*expr_factory, col_item.default_empty_expr_, empty_expr))) {
      LOG_WARN("failed to empty expr node", K(ret));
    } else {
      ColumnItem new_col_item;
      new_col_item.table_id_ = new_table_item->table_id_;
      new_col_item.column_id_ = col_item.column_id_;
      new_col_item.column_name_ = col_item.column_name_;
      new_col_item.expr_ = static_cast<ObColumnRefRawExpr *>(new_expr);
      new_col_item.expr_->set_table_id(new_table_item->table_id_);
      new_col_item.expr_->set_table_name(new_table_item->table_name_);
      new_col_item.is_geo_ = col_item.is_geo_;
      new_col_item.default_value_expr_ = error_expr;
      new_col_item.default_empty_expr_ = empty_expr;
      new_col_item.col_idx_= col_item.col_idx_;
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
    ObRawExprCopier copier(*expr_factory);
    if (OB_FAIL(copier.add_replaced_expr(old_exec_params, new_exec_params))) {
      LOG_WARN("failed to add replace pair", K(ret));
    } else if (OB_FAIL(copier.add_replaced_expr(old_cols, new_cols))) {
      LOG_WARN("failed to add replace pair", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < all_stmt->get_condition_size(); ++i) {
      ObRawExpr *new_cond_expr = NULL;
      if (map_info.cond_map_.at(i) >= 0 &&
          map_info.cond_map_.at(i) < new_any_stmt->get_condition_size()) {
        // both stmt has the condition
      } else if (OB_FAIL(copier.copy_on_replace(all_stmt->get_condition_expr(i),
                                                new_cond_expr))) {
        LOG_WARN("failed to copy on replace expr", K(ret));
      } else if (OB_FAIL(extra_conds.push_back(new_cond_expr))) {
        LOG_WARN("failed to push back condition", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    // create lnnvl exprs
    ObRawExpr *and_expr = NULL;
    ObRawExpr *lnnvl_expr = NULL;
    if (OB_FAIL(create_and_expr(extra_conds, and_expr))) {
      LOG_WARN("failed to create and expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_lnnvl_expr(*expr_factory, and_expr, lnnvl_expr))) {
      LOG_WARN("failed to build lnnvl expr", K(ret));
    } else if (OB_FAIL(lnnvl_expr->formalize(ctx_->session_info_))) {
      LOG_WARN("failed to formalize lnnvl  expr expr", K(ret));
    } else if (OB_FAIL(new_any_stmt->add_condition_expr(lnnvl_expr))) {
      LOG_WARN("failed to add having expr", K(ret));
    }
  }
  return ret;
}

int ObTransformSubqueryCoalesce::create_and_expr(const ObIArray<ObRawExpr *> &params, ObRawExpr *&ret_expr)
{
  int ret = OB_SUCCESS;
  ObRawExprFactory *factory = NULL;
  ObOpRawExpr *and_expr = NULL;
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

ObQueryRefRawExpr* ObTransformSubqueryCoalesce::get_exists_query_expr(ObRawExpr *expr)
{
  ObQueryRefRawExpr *ret_expr = NULL;
  if (OB_ISNULL(expr) ||
      OB_UNLIKELY(expr->get_expr_type() != T_OP_EXISTS &&
                  expr->get_expr_type() != T_OP_NOT_EXISTS) ||
      OB_ISNULL(expr->get_param_expr(0)) ||
      OB_UNLIKELY(!expr->get_param_expr(0)->is_query_ref_expr())) {
    // do nothing
  } else {
    ret_expr = static_cast<ObQueryRefRawExpr *>(expr->get_param_expr(0));
  }
  return ret_expr;
}

ObQueryRefRawExpr* ObTransformSubqueryCoalesce::get_any_all_query_expr(ObRawExpr *expr)
{
  ObQueryRefRawExpr *ret_expr = NULL;
  ObOpRawExpr *subquey_op = NULL;
  if (OB_ISNULL(expr) || OB_UNLIKELY(!expr->has_flag(IS_WITH_ANY) && !expr->has_flag(IS_WITH_ALL))) {
    // do nothing
  } else {
    subquey_op = static_cast<ObOpRawExpr*>(expr);
    if (OB_ISNULL(subquey_op->get_param_expr(1)) || OB_UNLIKELY(!subquey_op->get_param_expr(1)->is_query_ref_expr())) {
      /*do nothing*/
    } else {  
      ret_expr = static_cast<ObQueryRefRawExpr *>(subquey_op->get_param_expr(1));
    }
  }
  return ret_expr;
}

ObRawExpr* ObTransformSubqueryCoalesce::get_any_all_left_hand_expr(ObRawExpr *expr)
{
  ObRawExpr *left_hand = NULL;
  ObOpRawExpr *subquey_op = NULL;
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

int ObTransformSubqueryCoalesce::make_false(ObIArray<ObRawExpr *> &conds)
{
  int ret = OB_SUCCESS;
  ObRawExpr *false_expr = NULL;
  conds.reset();
  if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("transform context is null", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_const_bool_expr(
                       ctx_->expr_factory_, false_expr, false))) {
    LOG_WARN("failed to build const bool expr", K(ret));
  } else if (OB_FAIL(conds.push_back(false_expr))) {
    LOG_WARN("failed to push back false expr", K(ret));
  }
  return ret;
}

int ObTransformSubqueryCoalesce::merge_exec_params(ObQueryRefRawExpr *source_query_ref,
                                                   ObQueryRefRawExpr *target_query_ref,
                                                   ObIArray<ObRawExpr *> &old_params,
                                                   ObIArray<ObRawExpr *> &new_params)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < source_query_ref->get_param_count(); ++i) {
    bool found = false;
    ObExecParamRawExpr *source_param = NULL;
    ObExecParamRawExpr *target_param = NULL;
    if (OB_ISNULL(source_param = source_query_ref->get_exec_param(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("exec param is null", K(ret));
    }
    // find whether source_exec_params is existed
    for (int64_t j = 0; OB_SUCC(ret) && !found && j < target_query_ref->get_param_count(); ++j) {
      if (OB_ISNULL(target_param = target_query_ref->get_exec_param(j))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("exec param is null", K(ret));
      } else if (target_param->get_ref_expr() == source_param->get_ref_expr()) {
        found = true;
      }
    }
    if (OB_SUCC(ret)) {
      if (found) {
        if (OB_FAIL(old_params.push_back(source_param)) ||
            OB_FAIL(new_params.push_back(target_param))) {
          LOG_WARN("failed to push back exec param", K(ret));
        }
      } else {
        if (OB_FAIL(target_query_ref->add_exec_param_expr(source_param))) {
          LOG_WARN("failed to add new exec param", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTransformSubqueryCoalesce::transform_or_exprs(ObDMLStmt *stmt,
                                                    ObIArray<ObRawExpr *> &conds,
                                                    bool &is_happened)
{
  int ret = OB_SUCCESS;
  is_happened = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < conds.count(); ++i) {
    if (OB_FAIL(transform_or_expr(stmt, conds.at(i), is_happened))) {
      LOG_WARN("failed to transform or expr", K(ret));
    }
  }
  return ret;
}

int ObTransformSubqueryCoalesce::transform_or_expr(ObDMLStmt *stmt,
                                                  ObRawExpr * &expr,
                                                  bool &is_happened)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr) || OB_ISNULL(stmt) || OB_ISNULL(stmt->get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null expr", K(ret));
  } else if (T_OP_OR == expr->get_expr_type()) {
    bool can_be_transform = true;
    ObRawExpr *first_expr_param = NULL;
    ObQueryRefRawExpr *first_subquery_expr = NULL;
    ObSEArray<ObSelectStmt*, 8> subqueries;
    ObSEArray<ObExecParamRawExpr *, 8> exec_params;
    ObStmtCompareContext compare_ctx(&stmt->get_query_ctx()->calculable_items_);
    //Check whether each independent or condition can pull up related conditions
    for (int64_t i = 0; OB_SUCC(ret) && can_be_transform && i < expr->get_param_count(); ++i) {
      ObRawExpr *expr_param = expr->get_param_expr(i);
      if (OB_ISNULL(expr_param)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null expr", K(ret));
      } else if (T_OP_EXISTS != expr_param->get_expr_type() &&
                 !expr_param->has_flag(IS_WITH_ANY)) {
        can_be_transform = false;
      } else if (0 == i) {
        first_expr_param = expr_param;
      } else if (OB_FAIL(check_expr_can_be_coalesce(stmt, 
                                                    first_expr_param, 
                                                    expr_param,
                                                    compare_ctx,
                                                    can_be_transform))) {
        LOG_WARN("failed to check expr can be coalesce", K(ret));                                          
      }
      //collect subquery
      if (OB_SUCC(ret) && can_be_transform) {
        ObQueryRefRawExpr *subquery_expr = NULL;
        ObSelectStmt *subquery = NULL;
        if (T_OP_EXISTS == expr_param->get_expr_type()) {
          if (OB_ISNULL(expr_param->get_param_expr(0)) || 
              OB_UNLIKELY(!expr_param->get_param_expr(0)->is_query_ref_expr())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid exist predicate", K(*expr_param), K(ret));
          } else {
            subquery_expr = static_cast<ObQueryRefRawExpr *>(expr_param->get_param_expr(0));
          }
        } else {
          if (OB_ISNULL(expr_param->get_param_expr(1)) || 
              OB_UNLIKELY(!expr_param->get_param_expr(1)->is_query_ref_expr())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid anyall predicate", K(*expr_param), K(ret));
          } else {
            subquery_expr = static_cast<ObQueryRefRawExpr *>(expr_param->get_param_expr(1));
          }
        }
        if (OB_SUCC(ret)) {
          subquery = subquery_expr->get_ref_stmt();
          if (OB_FAIL(subqueries.push_back(subquery))) {
            LOG_WARN("failed to push back subquery", K(ret));
          } else if (OB_FAIL(append(exec_params, subquery_expr->get_exec_params()))) {
            LOG_WARN("failed to append exec params", K(ret));
          } else if (0 == i) {
            first_subquery_expr = subquery_expr;
          }
        }
      }
    }
    if (OB_SUCC(ret) && can_be_transform) {
      bool force_trans = false;
      bool force_no_trans = false;
      if (OB_FAIL(check_hint_valid(*stmt, 
                                   subqueries, 
                                   force_trans, 
                                   force_no_trans))) {
        LOG_WARN("failed to check hint valid", K(ret));
      } else if (force_no_trans) {
        can_be_transform = false;
        OPT_TRACE("hint reject transform");
      }
    }
    if (OB_SUCC(ret) && can_be_transform) {
      ObSelectStmt *union_stmt = NULL;
      if (OB_FAIL(append(stmt->get_query_ctx()->all_equal_param_constraints_,
                         compare_ctx.equal_param_info_))) {
        LOG_WARN("append equal param info failed", K(ret));
      } else if (OB_FAIL(ObTransformUtils::create_set_stmt(ctx_,
                                                          ObSelectStmt::UNION,
                                                          false, 
                                                          subqueries, 
                                                          union_stmt))) {
        LOG_WARN("failed to create set stmt", K(ret));
      } else if (OB_FAIL(first_subquery_expr->get_exec_params().assign(exec_params))) {
        LOG_WARN("failed to assign exec params", K(ret));
      } else if (OB_ISNULL(first_subquery_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null expr", K(ret));
      } else if (OB_FAIL(add_coalesce_stmts(subqueries))) {
        LOG_WARN("failed to append stmts", K(ret));
      } else {
        //reuse or condition`s first expr param
        first_subquery_expr->set_ref_stmt(union_stmt);
        expr = first_expr_param;
        is_happened = true;
      }
    }
  }
  return ret;
}

int ObTransformSubqueryCoalesce::check_expr_can_be_coalesce(ObDMLStmt *stmt,
                                                            ObRawExpr *l_expr,
                                                            ObRawExpr *r_expr,
                                                            ObStmtCompareContext &compare_ctx,
                                                            bool &can_be)
{
  int ret = OB_SUCCESS;
  can_be = false;
  ObQueryRefRawExpr *l_subquery_expr = NULL;
  ObQueryRefRawExpr *r_subquery_expr = NULL;
  OPT_TRACE("try to coalesce subquery in or expr");
  OPT_TRACE("left:", l_expr);
  OPT_TRACE("right:", r_expr);
  if (OB_ISNULL(l_expr) || OB_ISNULL(r_expr) || OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null expr", K(ret));
  } else if (l_expr->get_expr_type() != r_expr->get_expr_type()) {
    can_be = false;
  } else if (T_OP_EXISTS == l_expr->get_expr_type()) {
    if (OB_ISNULL(l_expr->get_param_expr(0)) || OB_ISNULL(r_expr->get_param_expr(0)) ||
        OB_UNLIKELY(!l_expr->get_param_expr(0)->is_query_ref_expr()) || 
        OB_UNLIKELY(!r_expr->get_param_expr(0)->is_query_ref_expr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid exist predicate", K(*l_expr), K(ret));
    } else {
      l_subquery_expr = static_cast<ObQueryRefRawExpr *>(l_expr->get_param_expr(0));
      r_subquery_expr = static_cast<ObQueryRefRawExpr *>(r_expr->get_param_expr(0));
      can_be = l_subquery_expr->has_exec_param();
      if (!can_be) {
        OPT_TRACE("correlated subquery can not be coalesced");
      }
    }
  } else {
    if (OB_ISNULL(l_expr->get_param_expr(0)) || OB_ISNULL(l_expr->get_param_expr(1)) ||
        OB_UNLIKELY(!l_expr->get_param_expr(1)->is_query_ref_expr()) ||
        OB_ISNULL(r_expr->get_param_expr(0)) || OB_ISNULL(r_expr->get_param_expr(1)) ||
        OB_UNLIKELY(!r_expr->get_param_expr(1)->is_query_ref_expr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid anyall predicate", K(*r_expr), K(ret));
    } else if (OB_UNLIKELY(l_expr->get_param_expr(0)->is_query_ref_expr()) ||
                OB_UNLIKELY(r_expr->get_param_expr(0)->is_query_ref_expr())) {
      // subquery in subquery, subquery = all subquery do not transform
    } else {
      ObRawExpr *left_hand = l_expr->get_param_expr(0);
      ObRawExpr *right_hand = r_expr->get_param_expr(0);
      l_subquery_expr = static_cast<ObQueryRefRawExpr *>(l_expr->get_param_expr(1));
      r_subquery_expr = static_cast<ObQueryRefRawExpr *>(r_expr->get_param_expr(1));
      if (OB_ISNULL(left_hand) || OB_ISNULL(right_hand)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null expr", K(ret));
      } else {
        can_be = left_hand->same_as(*right_hand, &compare_ctx);
        if (!can_be) {
          OPT_TRACE("left param expr not same, can not be coalesced");
        }
      }
    }
  }
  if (OB_SUCC(ret) && can_be) {
    ObSelectStmt *l_subquery = l_subquery_expr->get_ref_stmt();
    ObSelectStmt *r_subquery = r_subquery_expr->get_ref_stmt();
    ObSEArray<ObRawExpr*, 4> left_exprs;
    ObSEArray<ObRawExpr*, 4> right_exprs;
    if (OB_ISNULL(l_subquery) || OB_ISNULL(r_subquery)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null stmt", K(ret));
    } else if (OB_FAIL(check_subquery_validity(l_subquery_expr, l_subquery, can_be))) {
      LOG_WARN("failed to check subquery validity", K(ret));
    } else if (!can_be) {
      //do nothing
    } else if (OB_FAIL(check_subquery_validity(r_subquery_expr, r_subquery, can_be))) {
      LOG_WARN("failed to check subquery validity", K(ret));
    } else if (!can_be) {
      //do nothing
    } else if (l_subquery_expr->has_exec_param() ^ r_subquery_expr->has_exec_param()) {
      can_be = false;
      OPT_TRACE("correlated subquery is not isomorphic, can not be coalesced");
    } else if (!l_subquery_expr->has_exec_param()) {
      can_be = true;
    } else if (OB_FAIL(ObTransformUtils::check_correlated_condition_isomorphic(l_subquery,
                                                                               r_subquery,
                                                                               *l_subquery_expr,
                                                                               *r_subquery_expr,
                                                                               can_be,
                                                                               left_exprs,
                                                                               right_exprs))) {
      LOG_WARN("failed to check correlated subquery isomorphic", K(ret));
    } else if (!can_be) {
      OPT_TRACE("correlated subquery is not isomorphic, can not be coalesced");
    }
  }
  return ret;
}

int ObTransformSubqueryCoalesce::check_subquery_validity(ObQueryRefRawExpr *query_ref,
                                                         ObSelectStmt *subquery,
                                                         bool &valid)
{
  int ret = OB_SUCCESS;
  bool has_user_var = false;
  bool contain_subquery = false;
  valid = false;
  if (OB_ISNULL(subquery) || OB_ISNULL(query_ref)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  } else if (0 == subquery->get_from_item_size()) {
    //do nothing
    OPT_TRACE("from dual query, can not be coalesced");
  } else if (OB_FAIL(subquery->has_ref_assign_user_var(has_user_var))) {
    LOG_WARN("failed to check stmt has assignment ref user var", K(ret));
  } else if (has_user_var) {
    //do nothing
    OPT_TRACE("stmt has user var, can not be coalesced");
  } else if (OB_FAIL(ObTransformUtils::is_select_item_contain_subquery(subquery, contain_subquery))) {
    LOG_WARN("failed to check select item contain subquery", K(subquery), K(ret));
  } else if (contain_subquery) {
    //do nothing
    LOG_WARN("select item contain subquery, can not be coalesced");
  } else if (OB_FAIL(ObTransformUtils::check_correlated_exprs_can_pullup(*query_ref, *subquery, valid))) {
    LOG_WARN("failed to check correlated expr can be pullup", K(ret));
  } else if (!valid) {
    OPT_TRACE("correlated exprs can not pullup, will not coalesce subquery");
  }
  return ret;
}
/**
 * coalesce_update_assignment
 * Combining isomorphic subqueries in a set
 * update t1 set
 * c1 = (select c1 from t2),
 * c2 = (select c2 from t2);
 * ==>
 * update t1 set
 * (c1,c2) = (select c1,c2 from t2);
 **/
int ObTransformSubqueryCoalesce::coalesce_update_assignment(ObDMLStmt *stmt, bool &trans_happened)
{
  int ret = OB_SUCCESS;
  ObQueryCtx *query_ctx = NULL;
  if (OB_ISNULL(stmt) ||
      OB_ISNULL(query_ctx = stmt->get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  } else if (!stmt->is_update_stmt()) {
    //do nothing
  } else {
    ObDelUpdStmt *dml_stmt = static_cast<ObDelUpdStmt*>(stmt);
    ObUpdateStmt *upd_stmt = static_cast<ObUpdateStmt*>(dml_stmt);
    //common subquery infos
    ObSEArray<StmtCompareHelper*, 4> coalesce_infos;
    ObSEArray<ObRawExpr*, 4> assign_exprs;
    ObSEArray<ObSelectStmt*, 4> subqueries;
    ObSEArray<ObRawExpr*, 4> select_exprs;
    ObSEArray<int64_t, 4> index_map;
    ObSelectStmt *coalesce_query = NULL;
    if (OB_FAIL(upd_stmt->get_assignments_exprs(assign_exprs))) {
      LOG_WARN("failed to get assignment exprs", K(ret));
    } else if (OB_FAIL(get_subquery_assign_exprs(assign_exprs, subqueries))) {
      LOG_WARN("failed to get subquery exprs", K(ret));
    } else if (OB_FAIL(get_coalesce_infos(*stmt, subqueries, coalesce_infos))) {
      LOG_WARN("failed to get coalesce infos", K(ret));
    } else if (OB_FAIL(remove_invalid_coalesce_info(coalesce_infos))) {
      LOG_WARN("failed to remove invalid infos", K(ret));
    } else {
      LOG_TRACE("succeed to get coalesce infos", K(subqueries), K(coalesce_infos));
    }
    //coalesce earch group subquery
    for (int64_t i = 0; OB_SUCC(ret) && i < coalesce_infos.count(); ++i) {
      StmtCompareHelper *helper = coalesce_infos.at(i);
      //Original select expr
      select_exprs.reuse();
      //The index map of the original select expr 
      //corresponding to the new select expr
      index_map.reuse();
      //Merged subquery
      coalesce_query = NULL;
      if (OB_ISNULL(helper)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null helper", K(ret));
      } else if (helper->similar_stmts_.count() < 2) {
        //do nothing
      } else if (OB_FAIL(coalesce_subquery(*helper, 
                                           query_ctx,
                                           select_exprs, 
                                           index_map, 
                                           coalesce_query))) {
        LOG_WARN("failed to create temp table", K(ret));
      } else if (OB_FAIL(adjust_assign_exprs(upd_stmt, 
                                             helper,
                                             select_exprs, 
                                             index_map, 
                                             coalesce_query))) {
        LOG_WARN("failed to adjust assign exprs", K(ret));
      } else {
        trans_happened = true;
      }
    }
    if (OB_SUCC(ret) && trans_happened) {
      if (OB_FAIL(upd_stmt->adjust_subquery_list())) {
        LOG_WARN("failed to adjust subquery list", K(ret));
      }
    }
    for (int64_t i = 0; i < coalesce_infos.count(); i++) {
      if (coalesce_infos.at(i) != NULL) {
        coalesce_infos.at(i)->~StmtCompareHelper();
        coalesce_infos.at(i) = NULL;
      }
    }
  }
  return ret;
}

int ObTransformSubqueryCoalesce::get_subquery_assign_exprs(ObIArray<ObRawExpr*> &assign_exprs, 
                                                           ObIArray<ObSelectStmt*> &subqueries)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObQueryRefRawExpr*, 4> query_ref_exprs;
  ObSEArray<ObAliasRefRawExpr*, 4> alias_exprs;
  for (int64_t i = 0; OB_SUCC(ret) && i < assign_exprs.count(); ++i) {
    ObRawExpr *expr = assign_exprs.at(i);
    ObSelectStmt *stmt = NULL;
    query_ref_exprs.reuse();
    alias_exprs.reuse();
    bool is_valid = true;
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null expr", K(ret));
    } else if (OB_FAIL(ObTransformUtils::extract_alias_expr(expr, alias_exprs))) {
      LOG_WARN("failed to extract expr", K(ret));
    } else if (OB_FAIL(ObTransformUtils::extract_query_ref_expr(expr, query_ref_exprs))) {
      LOG_WARN("failed to extract expr", K(ret));
    } else if (alias_exprs.count() > 1 || query_ref_exprs.count() > 1) {
      //disable subquery coalescing in this scenes
      is_valid = false;
    }
    for (int64_t j = 0; OB_SUCC(ret) && is_valid && j < query_ref_exprs.count(); ++j) {
      ObQueryRefRawExpr *query_ref_expr = query_ref_exprs.at(j);
      if (OB_ISNULL(query_ref_expr) || OB_ISNULL(stmt = query_ref_expr->get_ref_stmt())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null stmt", K(ret));
      } else if (!query_ref_expr->is_scalar()) {
        //do nothing
      } else if (stmt->has_limit() || stmt->has_distinct() || stmt->is_set_stmt()) {
        //stmt can not coalesce,do nothing
      } else if (ObOptimizerUtil::find_item(subqueries, stmt)) {
        //do nothing
      } else if (OB_FAIL(subqueries.push_back(stmt))) {
        LOG_WARN("failed to push back expr", K(ret));
      }
    }
    for (int64_t j = 0; OB_SUCC(ret) && is_valid && j < alias_exprs.count(); ++j) {
      ObAliasRefRawExpr *alias_expr = alias_exprs.at(j);
      if (OB_ISNULL(alias_expr) || OB_ISNULL(alias_expr->get_ref_expr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null expr", K(ret));
      } else if (alias_expr->get_ref_expr()->is_query_ref_expr()) {
        ObQueryRefRawExpr *query_ref_expr = static_cast<ObQueryRefRawExpr*>(alias_expr->get_ref_expr());
        stmt = query_ref_expr->get_ref_stmt();
        if (OB_ISNULL(stmt)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpect null stmt", K(ret));
        } else if (stmt->has_limit() || stmt->has_distinct() || stmt->is_set_stmt()) {
          //stmt can not coalesce,do nothing
        } else if (ObOptimizerUtil::find_item(subqueries, stmt)) {
          //do nothing
        } else if (OB_FAIL(subqueries.push_back(stmt))) {
          LOG_WARN("failed to push back expr", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTransformSubqueryCoalesce::get_coalesce_infos(ObDMLStmt &parent_stmt,
                                                    ObIArray<ObSelectStmt*> &subqueries, 
                                                    ObIArray<StmtCompareHelper*> &coalesce_infos)
{
  int ret = OB_SUCCESS;
  ObStmtMapInfo map_info;
  QueryRelation relation;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null param", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < subqueries.count(); ++i) {
    bool find_similar = false;
    ObSelectStmt *stmt = subqueries.at(i);
    if (OB_ISNULL(stmt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null stmt ", K(ret));
    }
    //find isomorphic subqueries grouping
    for (int64_t j = 0; OB_SUCC(ret) && !find_similar && j < coalesce_infos.count(); ++j) {
      map_info.reset();
      StmtCompareHelper *helper = coalesce_infos.at(j);
      if (OB_ISNULL(helper)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null compare helper", K(ret));
      } else {
        OPT_TRACE("try to coalesce subquery");
        OPT_TRACE("left:", stmt);
        OPT_TRACE("right:", helper->stmt_);
      }
      if (OB_FAIL(ret)) {
      } else if (!helper->hint_force_stmt_set_.empty() &&
                 !helper->hint_force_stmt_set_.has_qb_name(stmt)) {
        //hint forbid，do nothing
        OPT_TRACE("hint reject transform");
      } else if (OB_FAIL(ObStmtComparer::check_stmt_containment(helper->stmt_,
                                                                stmt,
                                                                map_info,
                                                                relation))) {
        LOG_WARN("failed to check stmt containment", K(ret));
      } else if (!check_subquery_can_coalesce(map_info)) {
        //do nothing
        OPT_TRACE("not same suqbuery, can not coalesce");
      } else if (OB_FAIL(helper->similar_stmts_.push_back(stmt))) {
        LOG_WARN("failed to push back stmt", K(ret));
      } else if (OB_FAIL(helper->stmt_map_infos_.push_back(map_info))) {
        LOG_WARN("failed to push back map info", K(ret));
      } else {
        find_similar = true;
      }
    }
    if (OB_SUCC(ret) && !find_similar) {
      //If no isomorphic subquery group is found, compare it with self
      //and create new group with self
      map_info.reset();
      StmtCompareHelper *helper = NULL;
      bool force_no_trans = false;
      QbNameList qb_names;
      if (OB_FAIL(get_hint_force_set(parent_stmt,
                                     *stmt, 
                                     qb_names, 
                                     force_no_trans))) {
        LOG_WARN("failed to get hint set", K(ret));
      } else if (force_no_trans) {
        //do nothing
        OPT_TRACE("hint reject transform");
      } else if (OB_FAIL(StmtCompareHelper::alloc_compare_helper(*ctx_->allocator_, helper))) {
        LOG_WARN("failed to alloc compare helper", K(ret));
      } else if (OB_ISNULL(helper)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null compare helper", K(ret));
      } else if (OB_FAIL(ObStmtComparer::check_stmt_containment(stmt,
                                                                stmt,
                                                                map_info,
                                                                relation))) {
        LOG_WARN("failed to check stmt containment", K(ret));
      } else if (OB_FAIL(helper->similar_stmts_.push_back(stmt))) {
        LOG_WARN("failed to push back stmt", K(ret));
      } else if (OB_FAIL(helper->stmt_map_infos_.push_back(map_info))) {
        LOG_WARN("failed to push back map info", K(ret));
      } else if (OB_FAIL(helper->hint_force_stmt_set_.assign(qb_names))) {
        LOG_WARN("failed to assign qb names", K(ret));
      } else if (OB_FALSE_IT(helper->stmt_ = stmt)) {
      } else if (OB_FAIL(coalesce_infos.push_back(helper))) {
        LOG_WARN("failed to push back merge info", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformSubqueryCoalesce::remove_invalid_coalesce_info(ObIArray<StmtCompareHelper*> &coalesce_infos)
{
  int ret = OB_SUCCESS;
  ObSEArray<StmtCompareHelper*, 4> new_coalesce_infos;
  for (int i = 0; OB_SUCC(ret) && i < coalesce_infos.count(); ++i) {
    StmtCompareHelper *helper = coalesce_infos.at(i);
    if (OB_ISNULL(helper)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null helper", K(ret));
    } else if (!helper->hint_force_stmt_set_.empty() &&
               !helper->hint_force_stmt_set_.is_equal(helper->similar_stmts_)) {
      //do nothing
    } else if (OB_FAIL(new_coalesce_infos.push_back(helper))) {
      LOG_WARN("failed to push back helper", K(ret));
    } else if (OB_FAIL(add_coalesce_stmts(helper->similar_stmts_))) {
      LOG_WARN("failed to push back stmts", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(coalesce_infos.assign(new_coalesce_infos))) {
      LOG_WARN("failed to assign array", K(ret));
    }
  }
  return ret;
}

bool ObTransformSubqueryCoalesce::check_subquery_can_coalesce(const ObStmtMapInfo &map_info)
{
  return map_info.is_table_equal_ &&
         map_info.is_from_equal_ &&
         map_info.is_semi_info_equal_ &&
         map_info.is_cond_equal_ &&
         map_info.is_group_equal_ &&
         map_info.is_having_equal_;
}

int ObTransformSubqueryCoalesce::coalesce_subquery(StmtCompareHelper &helper, 
                                             ObQueryCtx *query_ctx,
                                             ObIArray<ObRawExpr*> &select_exprs, 
                                             ObIArray<int64_t> &index_map, 
                                             ObSelectStmt* &coalesce_query)
{
  int ret = OB_SUCCESS;
  coalesce_query = helper.stmt_;
  if (OB_ISNULL(query_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null param", K(ret));
  } else if (helper.stmt_map_infos_.count() != helper.similar_stmts_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect stmt map info size", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < helper.similar_stmts_.count(); ++i) {
    ObSelectStmt *stmt = helper.similar_stmts_.at(i);
    if (OB_FAIL(inner_coalesce_subquery(stmt, 
                                        query_ctx,
                                        helper.stmt_map_infos_.at(i),
                                        select_exprs, 
                                        index_map, 
                                        coalesce_query,
                                        i == 0))) {
      LOG_WARN("failed to inner coalesce subquery", K(ret));
    } else if (OB_FAIL(append(query_ctx->all_equal_param_constraints_, 
                              helper.stmt_map_infos_.at(i).equal_param_map_))) {
      LOG_WARN("failed to append equal param constraints", K(ret));
    }
  }
  return ret;
}

int ObTransformSubqueryCoalesce::inner_coalesce_subquery(ObSelectStmt *subquery, 
                                                  ObQueryCtx *query_ctx,
                                                  ObStmtMapInfo &map_info,
                                                  ObIArray<ObRawExpr*> &select_exprs, 
                                                  ObIArray<int64_t> &index_map, 
                                                  ObSelectStmt *coalesce_query,
                                                  const bool is_first_subquery)
{
  int ret = OB_SUCCESS;
  //select items in subquery
  ObSEArray<ObRawExpr*, 16> subquery_select_list;
  //select items in coalesce query
  ObSEArray<ObRawExpr*, 16> coalesce_select_list;
  //column items in subquery
  ObSEArray<ObRawExpr*, 16> subquery_column_list;
  //column items in coalesce query
  ObSEArray<ObRawExpr*, 16> coalesce_column_list;
  //column items in subquery trans to column items in coalesce query
  ObSEArray<ObRawExpr*, 16> new_column_list;
  ObSEArray<ColumnItem, 16> new_column_items;

  // if the select items have same udf, need check if they are deterministic
  // eg: select func(c1), func(c1) from t1;
  bool need_check_deterministic = true;
  ObStmtCompareContext context(coalesce_query,
                               subquery,
                               map_info,
                               &query_ctx->calculable_items_,
                               need_check_deterministic);
  if (OB_ISNULL(subquery) || OB_ISNULL(coalesce_query) || 
      OB_ISNULL(ctx_) || OB_ISNULL(ctx_->allocator_) ||
      OB_ISNULL(query_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null param", K(ret));
  } else if (OB_FAIL(subquery->get_select_exprs(subquery_select_list))) {
    LOG_WARN("failed to get select exprs", K(ret));
  } else if (OB_FAIL(subquery->get_column_exprs(subquery_column_list))) {
    LOG_WARN("failed to get column exprs", K(ret));
  } else if (OB_FAIL(coalesce_query->get_select_exprs(coalesce_select_list))) {
    LOG_WARN("failed to get select exprs", K(ret));
  } else if (OB_FAIL(coalesce_query->get_column_exprs(coalesce_column_list))) {
    LOG_WARN("failed to get column exprs", K(ret));
  }
  //check column item
  for (int64_t i = 0; OB_SUCC(ret) && i < subquery_column_list.count(); ++i) {
    ObRawExpr *subquery_column = subquery_column_list.at(i);
    bool find = false;
    if (OB_ISNULL(subquery_column)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null column expr", K(ret));
    }
    //Check if the column is already in the combined subquery
    for (int64_t j = 0; OB_SUCC(ret) && !find && j < coalesce_column_list.count(); ++j) {
      ObRawExpr *coalesce_column = coalesce_column_list.at(j);
      if (OB_ISNULL(coalesce_column)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null column expr", K(ret));
      } else if (!coalesce_column->same_as(*subquery_column, &context)) {
        //do nothing
      } else if (OB_FAIL(new_column_list.push_back(coalesce_column))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else {
        find = true;
      }
    }
    //If not, it needs to be added to the combined subquery
    if (OB_SUCC(ret) && !find) {
      ColumnItem *column_item = NULL;
      ObColumnRefRawExpr *col_ref = static_cast<ObColumnRefRawExpr*>(subquery_column);
      uint64_t table_id = OB_INVALID_ID;
      if (!subquery_column->is_column_ref_expr()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expect column ref expr", KPC(subquery_column), K(ret));
      } else if (OB_ISNULL(column_item = subquery->get_column_item_by_id(col_ref->get_table_id(),
                                                                         col_ref->get_column_id()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null column item", K(ret));
      } else if (OB_FAIL(get_map_table_id(subquery,
                                          coalesce_query,
                                          map_info,
                                          col_ref->get_table_id(),
                                          table_id))) {
        LOG_WARN("failed to get map table id", K(ret));
      } else if (OB_FALSE_IT(column_item->table_id_ = table_id)) {
      } else if (OB_FALSE_IT(col_ref->set_table_id(table_id))) {
      } else if (OB_FAIL(new_column_items.push_back(*column_item))) {
        LOG_WARN("failed to push back column item", K(ret));
      } else if (OB_FAIL(new_column_list.push_back(subquery_column))) {
        LOG_WARN("failed to push back expr", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && !new_column_items.empty()) {
    if (OB_FAIL(coalesce_query->add_column_item(new_column_items))) {
      LOG_WARN("failed to add table item", K(ret));
    }
  }
  //check select item
  for (int64_t i = 0; OB_SUCC(ret) && i < subquery_select_list.count(); ++i) {
    ObRawExpr *subquery_select = subquery_select_list.at(i);
    bool find = false;
    if (OB_ISNULL(subquery_select)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null select expr", K(ret));
    } else if (OB_FAIL(select_exprs.push_back(subquery_select))) {
      LOG_WARN("failed to push back expr", K(ret));
    }
    for (int64_t j = 0; OB_SUCC(ret) && !find && j < coalesce_select_list.count(); ++j) {
      ObRawExpr *coalesce_select = coalesce_select_list.at(j);
      if (OB_ISNULL(coalesce_select)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null select expr", K(ret));
      } else if (!coalesce_select->same_as(*subquery_select, &context)) {
        // do nothing
      } else if (!is_first_subquery &&
                  OB_FAIL(ObTransformUtils::create_select_item(*ctx_->allocator_,
                                                               coalesce_select,
                                                               coalesce_query))) {
        LOG_WARN("failed to create column for subquery", K(ret));
      } else if (OB_FAIL(index_map.push_back(is_first_subquery ? j : coalesce_query->get_select_item_size() - 1))) {
        LOG_WARN("failed to push back index", K(ret));
      } else {
        find = true;
      }
    }
    if (OB_SUCC(ret) && !find) {
      ObSEArray<ObAggFunRawExpr*, 8> aggr_items;
      ObSEArray<ObWinFunRawExpr*, 8> win_func_exprs;
      if (ObTransformUtils::replace_expr(subquery_column_list, new_column_list, subquery_select)) {
        LOG_WARN("failed to replace expr", K(ret));
      } else if (OB_FAIL(ObTransformUtils::extract_aggr_expr(subquery_select,
                                                             aggr_items))) {
        LOG_WARN("failed to extract aggr expr", K(ret));
      } else if (OB_FAIL(append(coalesce_query->get_aggr_items(), aggr_items))) {
        LOG_WARN("failed to append aggr items", K(ret));
      } else if (OB_FAIL(ObTransformUtils::extract_winfun_expr(subquery_select, win_func_exprs))) {
        LOG_WARN("failed to extract win func exprs", K(ret));
      } else if (OB_FAIL(append(coalesce_query->get_window_func_exprs(), win_func_exprs))) {
        LOG_WARN("failed to append win func exprs", K(ret));
      } else if (OB_FAIL(ObTransformUtils::create_select_item(*ctx_->allocator_,
                                                              subquery_select,
                                                              coalesce_query))) {
        LOG_WARN("failed to create column for subquery", K(ret));
      } else if (OB_FAIL(index_map.push_back(coalesce_query->get_select_item_size() - 1))) {
        LOG_WARN("failed to push back index", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(coalesce_query->adjust_subquery_list())) {
      LOG_WARN("failed to adjust subquery list", K(ret));
    } else if (OB_FAIL(append(query_ctx->all_equal_param_constraints_, 
                              context.equal_param_info_))) {
      LOG_WARN("failed to append equal param constraints", K(ret));
    }
  }
  return ret;
}

int ObTransformSubqueryCoalesce::get_map_table_id(ObSelectStmt *subquery,
                                            ObSelectStmt *coalesce_subquery,
                                            ObStmtMapInfo& map_info,
                                            const uint64_t &subquery_table_id,
                                            uint64_t &table_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(subquery) || OB_ISNULL(coalesce_subquery)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  }
  bool find = false;
  int64_t idx = OB_INVALID_ID;
  for (int64_t i = 0; OB_SUCC(ret) && !find && i < subquery->get_table_size(); ++i) {
    TableItem *table = subquery->get_table_item(i);
    if (OB_ISNULL(table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null table item", K(ret));
    } else if (subquery_table_id == table->table_id_) {
      find =  true;
      idx = i;
    }
  }
  if (OB_SUCC(ret) && (!find || OB_INVALID_ID == idx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table shoud be found in subquery" ,K(subquery_table_id), K(ret));
  }
  find = false;
  for (int64_t i = 0; OB_SUCC(ret) && !find && i < map_info.table_map_.count(); ++i) {
    if (idx == map_info.table_map_.at(i)) {
      idx = i;
      find = true;
    }
  }
  if (OB_SUCC(ret) && (!find || OB_INVALID_ID == idx || 
      idx < 0 || idx > coalesce_subquery->get_table_size())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("incorrect table idx" ,K(idx), K(ret));
  }
  if (OB_SUCC(ret)) {
    TableItem *table = coalesce_subquery->get_table_item(idx);
    if (OB_ISNULL(table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null table item", K(ret));
    } else {
      table_id = table->table_id_;
    }
  }
  return ret;
}

int ObTransformSubqueryCoalesce::adjust_assign_exprs(ObUpdateStmt *upd_stmt,
                                              StmtCompareHelper *helper, 
                                              ObIArray<ObRawExpr*> &select_exprs, 
                                              ObIArray<int64_t> &index_map, 
                                              ObSelectStmt *coalesce_query)
{
  int ret = OB_SUCCESS;
  ObQueryRefRawExpr *coalesce_query_expr = NULL;
  ObArray<ObExecParamRawExpr *> all_params;
  ObSEArray<ObRawExpr*, 4> old_exprs;
  ObSEArray<ObRawExpr*, 4> new_exprs;
  if (OB_ISNULL(upd_stmt) || OB_ISNULL(helper) || OB_ISNULL(coalesce_query)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null param", K(ret));
  } else if (OB_FAIL(ctx_->expr_factory_->create_raw_expr(T_REF_QUERY, coalesce_query_expr))) {
    LOG_WARN("fail to create raw expr", K(ret));
  } else if (OB_ISNULL(coalesce_query_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null expr", K(ret));
  } else if (OB_FAIL(get_exec_params(upd_stmt, all_params))) {
    LOG_WARN("failed to get all params", K(ret));
  } else {
    coalesce_query_expr->set_ref_stmt(coalesce_query);
    if (OB_FAIL(ObTransformUtils::inherit_exec_params(all_params, coalesce_query_expr))) {
      LOG_WARN("failed to inherit exec params", K(ret));
    } else if (OB_FAIL(coalesce_query_expr->formalize(ctx_->session_info_))) {
      LOG_WARN("failed to formalize coalesce query expr", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < upd_stmt->get_update_table_info().count(); ++i) {
    ObUpdateTableInfo* table_info = upd_stmt->get_update_table_info().at(i);
    if (OB_ISNULL(table_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null table info", K(ret), K(i));
    } else {
      for (int64_t j = 0; OB_SUCC(ret) && j < table_info->assignments_.count(); ++j) {
        ObAssignment &assign = table_info->assignments_.at(j);
        if (OB_ISNULL(assign.expr_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpect null expr", K(ret));
        } else if (OB_FAIL(assign.expr_->extract_info())) {
          LOG_WARN("failed to extract expr info", K(ret));
        } else if (assign.expr_->has_flag(CNT_ALIAS)) {
          if (OB_FAIL(adjust_alias_assign_exprs(assign.expr_, 
                                              helper, 
                                              select_exprs, 
                                              index_map, 
                                              coalesce_query_expr, 
                                              coalesce_query,
                                              old_exprs,
                                              new_exprs))) {
            LOG_WARN("failed to extract expr", K(ret));
          }
        } else if (assign.expr_->has_flag(CNT_SUB_QUERY)) {
          if (OB_FAIL(adjust_query_assign_exprs(assign.expr_, 
                                                helper, 
                                                select_exprs, 
                                                index_map, 
                                                coalesce_query_expr, 
                                                coalesce_query,
                                                old_exprs,
                                                new_exprs))) {
            LOG_WARN("failed to extract expr", K(ret));
          }
        }
      }
    }
  }
  if (OB_SUCC(ret) && !old_exprs.empty()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < upd_stmt->get_update_table_info().count(); ++i) {
      ObUpdateTableInfo* table_info = upd_stmt->get_update_table_info().at(i);
      if (OB_ISNULL(table_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null table info", K(ret), K(i));
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && j < table_info->assignments_.count(); ++j) {
          ObAssignment &assign = table_info->assignments_.at(j);
          if (OB_ISNULL(assign.expr_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpect null expr", K(ret));
          } else if (!assign.expr_->has_flag(CNT_ALIAS) &&
                    !assign.expr_->has_flag(CNT_SUB_QUERY)) {
            // do nothing
          } else if (OB_FAIL(ObTransformUtils::replace_expr(old_exprs,
                                                            new_exprs,
                                                            assign.expr_))) {
            LOG_WARN("failed to replace expr", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObTransformSubqueryCoalesce::adjust_alias_assign_exprs(ObRawExpr* &assign_expr,
                                                    StmtCompareHelper *helper, 
                                                    ObIArray<ObRawExpr*> &select_exprs, 
                                                    ObIArray<int64_t> &index_map, 
                                                    ObQueryRefRawExpr *coalesce_query_expr,
                                                    ObSelectStmt *coalesce_query,
                                                    ObIArray<ObRawExpr*> &old_exprs,
                                                    ObIArray<ObRawExpr*> &new_exprs)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObAliasRefRawExpr*, 4> alias_exprs;
  ObRawExpr *new_expr = NULL;
  if (OB_ISNULL(assign_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null expr", K(ret));
  } else if (OB_FAIL(ObTransformUtils::extract_alias_expr(assign_expr, alias_exprs))) {
    LOG_WARN("failed to extract expr", K(ret));
  }
  for (int64_t j = 0; OB_SUCC(ret) && j < alias_exprs.count(); ++j) {
    ObAliasRefRawExpr *alias_expr = alias_exprs.at(j);
    if (OB_ISNULL(alias_expr) || OB_ISNULL(alias_expr->get_param_expr(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null expr", K(ret));
    } else if (ObOptimizerUtil::find_item(old_exprs, alias_expr)) {
      // do noting
    } else if (alias_expr->is_ref_query_output()) {
      ObQueryRefRawExpr *query_ref_expr = static_cast<ObQueryRefRawExpr*>(alias_expr->get_param_expr(0));
      int64_t select_idx = alias_expr->get_project_index();
      if (OB_FAIL(inner_adjust_assign_exprs(query_ref_expr->get_ref_stmt(), 
                                            select_idx, 
                                            helper, 
                                            select_exprs, 
                                            index_map, 
                                            coalesce_query_expr, 
                                            coalesce_query, 
                                            new_expr))) {
        LOG_WARN("failed to adjust assign exprs", K(ret));
      } else if (NULL == new_expr) {
        //do nothing
      } else if (OB_FAIL(old_exprs.push_back(alias_expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else if (OB_FAIL(new_exprs.push_back(new_expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformSubqueryCoalesce::adjust_query_assign_exprs(ObRawExpr* &assign_expr,
                                                    StmtCompareHelper *helper, 
                                                    ObIArray<ObRawExpr*> &select_exprs, 
                                                    ObIArray<int64_t> &index_map, 
                                                    ObQueryRefRawExpr *coalesce_query_expr,
                                                    ObSelectStmt *coalesce_query,
                                                    ObIArray<ObRawExpr*> &old_exprs,
                                                    ObIArray<ObRawExpr*> &new_exprs)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObQueryRefRawExpr*, 4> query_ref_exprs;
  ObRawExpr *new_expr = NULL;
  if (OB_ISNULL(assign_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null expr", K(ret));
  } else if (OB_FAIL(ObTransformUtils::extract_query_ref_expr(assign_expr, query_ref_exprs))) {
    LOG_WARN("failed to extract expr", K(ret));
  }
  for (int64_t j = 0; OB_SUCC(ret) && j < query_ref_exprs.count(); ++j) {
    ObQueryRefRawExpr *query_ref_expr = query_ref_exprs.at(j);
    if (OB_ISNULL(query_ref_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null stmt", K(ret));
    } else if (ObOptimizerUtil::find_item(old_exprs, query_ref_expr)) {
      // do noting
    } else if (OB_FAIL(inner_adjust_assign_exprs(query_ref_expr->get_ref_stmt(), 
                                                  0, 
                                                  helper, 
                                                  select_exprs, 
                                                  index_map, 
                                                  coalesce_query_expr, 
                                                  coalesce_query, 
                                                  new_expr))) {
      LOG_WARN("failed to adjust assign exprs", K(ret));
    } else if (NULL == new_expr) {
      //do nothing
    } else if (OB_FAIL(old_exprs.push_back(query_ref_expr))) {
      LOG_WARN("failed to push back expr", K(ret));
    } else if (OB_FAIL(new_exprs.push_back(new_expr))) {
      LOG_WARN("failed to push back expr", K(ret));
    }
  }
  return ret;
}

int ObTransformSubqueryCoalesce::inner_adjust_assign_exprs(ObSelectStmt *stmt,
                                                    const int64_t select_idx,
                                                    StmtCompareHelper *helper, 
                                                    ObIArray<ObRawExpr*> &select_exprs, 
                                                    ObIArray<int64_t> &index_map, 
                                                    ObQueryRefRawExpr *coalesce_query_expr,
                                                    ObSelectStmt *coalesce_query,
                                                    ObRawExpr* &new_expr)
{
  int ret = OB_SUCCESS;
  ObAliasRefRawExpr *alias_expr = NULL;
  ObRawExpr *select_expr = NULL;
  new_expr = NULL;
  int64_t new_idx = OB_INVALID_INDEX;
  if (OB_ISNULL(stmt) || OB_ISNULL(helper) || 
      OB_ISNULL(coalesce_query_expr) || OB_ISNULL(coalesce_query)|| 
      OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null param", K(ret));
  } else if (select_exprs.count() != index_map.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect select expr size" ,K(ret));
  } else if (!ObOptimizerUtil::find_item(helper->similar_stmts_, stmt)) {
    //do nothing
  } else if (select_idx < 0 || select_idx >= stmt->get_select_item_size()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect select index", K(select_idx), K(ret));
  } else if (OB_FALSE_IT(select_expr = stmt->get_select_item(select_idx).expr_)) {
  } else if (!ObOptimizerUtil::find_item(select_exprs, select_expr, &new_idx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("select expr not found", K(ret));
  } else if (0 > index_map.at(new_idx) || 
            index_map.at(new_idx) >= coalesce_query->get_select_item_size()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect select index", K(index_map.at(new_idx)), K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_query_output_ref(*ctx_->expr_factory_, 
                                                            coalesce_query_expr, 
                                                            index_map.at(new_idx), 
                                                            alias_expr))) {
    LOG_WARN("failed to build query output ref", K(ret));
  } else if (OB_FAIL(alias_expr->formalize(ctx_->session_info_))) {
    LOG_WARN("formalize like expr failed", K(ret));
  } else {
    new_expr = alias_expr;
  }
  return ret;
}

int ObTransformSubqueryCoalesce::get_exec_params(ObDMLStmt *stmt, ObIArray<ObExecParamRawExpr *> &all_params)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_subquery_expr_size(); ++i) {
    ObQueryRefRawExpr *query_ref = stmt->get_subquery_exprs().at(i);
    if (OB_ISNULL(query_ref)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("query ref is null", K(ret));
    } else if (OB_FAIL(append_array_no_dup(all_params, query_ref->get_exec_params()))) {
      LOG_WARN("failed to append array no dup", K(ret));
    }
  }
  return ret;
}

int ObTransformSubqueryCoalesce::construct_transform_hint(ObDMLStmt &stmt, void *trans_params)
{
  int ret = OB_SUCCESS;
  ObCoalesceSqHint *hint = NULL;
  Ob2DArray<CoalesceStmts *> *all_subqueries = NULL;
  all_subqueries = static_cast<Ob2DArray<CoalesceStmts *> * >(trans_params);
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->allocator_) ||
      OB_ISNULL(all_subqueries)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(ctx_));
  } else if (OB_FAIL(ObQueryHint::create_hint(ctx_->allocator_, T_COALESCE_SQ, hint))) {
    LOG_WARN("failed to create hint", K(ret));
  } else if (OB_FAIL(sort_coalesce_stmts(*all_subqueries))) {
    LOG_WARN("failed to sort stmts", K(ret));
  } else {
    bool use_hint = false;
    const ObCoalesceSqHint *myhint = static_cast<const ObCoalesceSqHint*>(get_hint(stmt.get_stmt_hint()));
    for (int64_t i = 0; OB_SUCC(ret) && i < all_subqueries->count(); ++i) {
      CoalesceStmts *subqueries = all_subqueries->at(i);
      QbNameList qb_names;
      if (OB_ISNULL(subqueries)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null stmts", K(ret));
      }
      for (int j = 0; OB_SUCC(ret) && j < subqueries->count(); ++j) {
        ObString subquery_qb_name;
        ObSelectStmt *subquery = NULL;
        if (OB_ISNULL(subquery = subqueries->at(j))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null", K(ret), K(subquery));
        } else if (OB_FAIL(subquery->get_qb_name(subquery_qb_name))) {
          LOG_WARN("failed to get qb name", K(ret), K(stmt.get_stmt_id()));
        } else if (OB_FAIL(qb_names.qb_names_.push_back(subquery_qb_name))) {
          LOG_WARN("failed to push back qb name", K(ret));
        } else if (OB_FAIL(ctx_->add_src_hash_val(subquery_qb_name))) {
          LOG_WARN("failed to add src hash val", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(hint->add_qb_name_list(qb_names))) {
        LOG_WARN("failed to add qb names", K(ret));
      } else if (NULL != myhint && myhint->enable_coalesce_sq(qb_names.qb_names_)) {
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

int ObTransformSubqueryCoalesce::check_hint_valid(const ObDMLStmt &stmt, 
                                                  ObSelectStmt &subquery1, 
                                                  ObSelectStmt &subquery2, 
                                                  bool &force_trans,
                                                  bool &force_no_trans) const
{
  int ret = OB_SUCCESS;
  ObSEArray<ObSelectStmt*, 4> queries;
  if (OB_FAIL(queries.push_back(&subquery1))) {
    LOG_WARN("failed to push subquery", K(ret));
  } else if (OB_FAIL(queries.push_back(&subquery2))) {
    LOG_WARN("failed to push subquery", K(ret));
  } else if (OB_FAIL(check_hint_valid(stmt, 
                                      queries, 
                                      force_trans, 
                                      force_no_trans))) {
    LOG_WARN("failed to check hint valid", K(ret));
  }
  return ret;
}

int ObTransformSubqueryCoalesce::check_hint_valid(const ObDMLStmt &stmt, 
                                                  const ObIArray<ObSelectStmt*> &queries,
                                                  bool &force_trans,
                                                  bool &force_no_trans) const
{
  int ret = OB_SUCCESS;
  force_trans = false;
  force_no_trans = false;
  const ObQueryHint *query_hint = NULL;
  ObSEArray<ObString, 4> qb_names;
  for (int i = 0; OB_SUCC(ret) && i < queries.count(); ++i) {
    ObSelectStmt *subquery = queries.at(i);
    ObString qb_name;
    if (OB_ISNULL(subquery)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null stmt", K(ret));
    } else if (OB_FAIL(subquery->get_qb_name(qb_name))) {
      LOG_WARN("failed to get qb name", K(ret));
    } else if (OB_FAIL(qb_names.push_back(qb_name))) {
      LOG_WARN("failed to push back qb_name", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(query_hint = stmt.get_stmt_hint().query_hint_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(query_hint));
  } else {
    const ObCoalesceSqHint *myhint = static_cast<const ObCoalesceSqHint*>(get_hint(stmt.get_stmt_hint()));
    force_trans = NULL != myhint && myhint->enable_coalesce_sq(qb_names);
    force_no_trans = !force_trans && query_hint->has_outline_data();
  }
  return ret;
}

int ObTransformSubqueryCoalesce::add_coalesce_stmt(ObSelectStmt *subquery1,  ObSelectStmt *subquery2)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObSelectStmt*, 8> stmts;
  if (OB_FAIL(stmts.push_back(subquery1))) {
    LOG_WARN("failed to push back subquery", K(ret));
  } else if (OB_FAIL(stmts.push_back(subquery2))) {
    LOG_WARN("failed to push back subquery", K(ret));
  } else if (OB_FAIL(add_coalesce_stmts(stmts))) {
    LOG_WARN("failed to push back stmts", K(ret));
  }
  return ret;
}

int ObTransformSubqueryCoalesce::get_hint_force_set(const ObDMLStmt &stmt, 
                                                    const ObSelectStmt &subquery,
                                                    QbNameList &qb_names,
                                                    bool &hint_force_no_trans)

{
  int ret = OB_SUCCESS;
  hint_force_no_trans = false;
  const ObQueryHint *query_hint = NULL;
  ObString qb_name;
  if (OB_ISNULL(ctx_) ||
      OB_ISNULL(query_hint = stmt.get_stmt_hint().query_hint_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(ctx_), K(query_hint));
  } else if (OB_FAIL(subquery.get_qb_name(qb_name))) {
    LOG_WARN("failed to get qb name", K(ret));
  } else {
    const ObHint *myhint = get_hint(stmt.get_stmt_hint());
    const ObCoalesceSqHint *hint = static_cast<const ObCoalesceSqHint*>(myhint);
    if (!query_hint->has_outline_data()) {
      const ObCoalesceSqHint *hint = static_cast<const ObCoalesceSqHint*>(myhint);
      if (NULL != myhint && OB_FAIL(hint->get_qb_name_list(qb_name, qb_names))) {
        LOG_WARN("failed to get qb name list", K(ret));
      }
    } else if (OB_ISNULL(myhint)) { // has outline data, myhint can not be null
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null hint", K(ret), K(myhint));
    } else if (OB_FAIL(hint->get_qb_name_list(qb_name, qb_names))) {
      LOG_WARN("failed to get qb name list", K(ret));
    } else if (qb_names.empty()) {
      hint_force_no_trans = true;
    }
  }
  return ret;
}

int ObTransformSubqueryCoalesce::add_coalesce_stmts(const ObIArray<ObSelectStmt*> &stms)
{
  int ret = OB_SUCCESS;
  CoalesceStmts *new_stmts = NULL;
  if (OB_ISNULL(new_stmts = (CoalesceStmts *) allocator_.alloc(sizeof(CoalesceStmts)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to allocate stmts array", K(ret));
  } else {
    new_stmts = new (new_stmts) CoalesceStmts();
    if (OB_FAIL(new_stmts->assign(stms))) {
      LOG_WARN("failed to assign array", K(ret));
    } else if (OB_FAIL(coalesce_stmts_.push_back(new_stmts))) {
      LOG_WARN("failed to push back stmts", K(ret));
    }
  }
  return ret;
}

int ObTransformSubqueryCoalesce::sort_coalesce_stmts(Ob2DArray<CoalesceStmts *> &coalesce_stmts)
{
  int ret = OB_SUCCESS;
  ObSEArray<std::pair<int, int>, 4> index_map;
  Ob2DArray<CoalesceStmts *> new_stmts;
  auto cmp_func1 = [](ObSelectStmt* l_stmt, ObSelectStmt* r_stmt){
    if (OB_ISNULL(l_stmt) || OB_ISNULL(r_stmt)) {
      return false;
    } else {
      return l_stmt->get_stmt_id() < r_stmt->get_stmt_id();
    }
  };
  auto cmp_func2 = [](std::pair<int,int> &lhs, std::pair<int,int> &rhs){
    return lhs.second < rhs.second;
  };
  for (int64_t i = 0; OB_SUCC(ret) && i < coalesce_stmts.count(); ++i) {
    CoalesceStmts *subqueries = coalesce_stmts.at(i);
    if (OB_ISNULL(subqueries)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null stmts", K(ret));
    } else {
      std::sort(subqueries->begin(), subqueries->end(), cmp_func1);
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < coalesce_stmts.count(); ++i) {
    CoalesceStmts *subqueries = coalesce_stmts.at(i);
    if (OB_ISNULL(subqueries)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null stmts", K(ret));
    } else if (subqueries->empty() || OB_ISNULL(subqueries->at(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null stmts", K(ret));
    } else if (OB_FAIL(index_map.push_back(std::pair<int,int>(i, subqueries->at(0)->get_stmt_id())))) {
      LOG_WARN("failed to push back index", K(ret));
    }
  }
  std::sort(index_map.begin(), index_map.end(), cmp_func2);
  for (int64_t i = 0; OB_SUCC(ret) && i < index_map.count(); ++i) {
    int index = index_map.at(i).first;
    if (index < 0 || index >= coalesce_stmts.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("index out of range", K(ret));
    } else if (OB_FAIL(new_stmts.push_back(coalesce_stmts.at(index)))) {
      LOG_WARN("failed to push back stmts", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(coalesce_stmts.assign(new_stmts))) {
      LOG_WARN("failed to assign array", K(ret));
    }
  }
  return ret;
}

int ObTransformSubqueryCoalesce::check_select_items_same(const ObDMLStmt *first,
                                                         const ObDMLStmt *second,
                                                         ObStmtMapInfo &map_info,
                                                         bool &is_same)
{
  int ret = OB_SUCCESS;
  is_same = true;
  if (OB_ISNULL(first) || OB_ISNULL(second)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), KP(first), KP(second));
  } else if (first->is_select_stmt() && second->is_select_stmt()) {
    const ObSelectStmt *first_sel = static_cast<const ObSelectStmt *>(first);
    const ObSelectStmt *second_sel = static_cast<const ObSelectStmt *>(second);
    ObStmtCompareContext context(first, second, map_info, &first->get_query_ctx()->calculable_items_);
    if (first_sel->get_select_item_size() != second_sel->get_select_item_size()) {
      is_same = false;
    }
    for (int64_t i = 0; OB_SUCC(ret) && is_same && i < first_sel->get_select_item_size(); ++i) {
      ObRawExpr *first_expr = first_sel->get_select_item(i).expr_;
      ObRawExpr *second_expr = second_sel->get_select_item(i).expr_;
      if (OB_ISNULL(first_expr) || OB_ISNULL(second_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret), KP(first_expr), KP(second_expr));
      } else if (!first_expr->same_as(*second_expr, &context)) {
        is_same = false;
      }
    }
  } else {
    //do nothing
  }
  return ret;
}
