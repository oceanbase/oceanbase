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
#include "ob_transform_simplify_groupby.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "common/ob_smart_call.h"

using namespace oceanbase::sql;

int ObTransformSimplifyGroupby::transform_one_stmt(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                                   ObDMLStmt *&stmt,
                                                   bool &trans_happened)
{
  int ret = OB_SUCCESS;
  bool is_happened = false;
  if (OB_FAIL(convert_count_aggr_contain_const(stmt, is_happened))) {
    LOG_WARN("convert count aggr contain const failed", K(ret));
  } else {
    trans_happened |= is_happened;
    OPT_TRACE("convert count aggr contain const:", is_happened);
    LOG_TRACE("succeed to convert count aggr contain const", K(is_happened));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(remove_stmt_group_by(stmt, is_happened))) {
      LOG_WARN("remove stmt group by failed", K(ret));
    } else {
      trans_happened |= is_happened;
      OPT_TRACE("remove group by:", is_happened);
      LOG_TRACE("succeed to stmt remove group by", K(is_happened));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(remove_group_by_duplicates(stmt, is_happened))) {
      LOG_WARN("failed to remove group by duplicates", K(ret));
    } else {
      trans_happened |= is_happened;
      OPT_TRACE("remove group by duplicates:", is_happened);
      LOG_TRACE("succeed to remove group by duplicates", K(is_happened));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(remove_aggr_distinct(stmt, is_happened))) {
      LOG_WARN("failed to remove aggr distinct", K(ret));
    } else {
      trans_happened |= is_happened;
      OPT_TRACE("remove aggr distinct:", is_happened);
      LOG_TRACE("succeed to remove aggr distinct", K(is_happened));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(remove_redundant_group_by(stmt, is_happened))) {
      LOG_WARN("failed to remove redundent group by", K(ret));
    } else {
      trans_happened |= is_happened;
      OPT_TRACE("remove redundent group by:", is_happened);
      LOG_TRACE("succeed to remove redundent group by", K(is_happened));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(transform_const_aggr(stmt, is_happened))) {
      LOG_WARN("failed to transform const aggr", K(ret));
    } else {
      trans_happened |= is_happened;
      OPT_TRACE("transform const aggr:", is_happened);
      LOG_TRACE("succeed to transform const aggr", K(is_happened));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(prune_group_by_rollup(parent_stmts, stmt, is_happened))) {
      LOG_WARN("failed to prune rollup", K(ret));
    } else {
      trans_happened |= is_happened;
      OPT_TRACE("prune group by rollup:", is_happened);
      LOG_TRACE("succeed to prune group by rollup", K(is_happened));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(convert_group_by_to_distinct(stmt, is_happened))) {
      LOG_WARN("failed to convert group by to distinct", K(ret));
    } else {
      trans_happened |= is_happened;
      OPT_TRACE("convert group by to distinct:", is_happened);
      LOG_TRACE("succeed to convert group by to distinct", K(is_happened));
    }
  }

  if (OB_SUCC(ret) && trans_happened) {
    if (OB_FAIL(add_transform_hint(*stmt))) {
      LOG_WARN("failed to add transform hint", K(ret));
    }
  }
  return ret;
}

//当存在多层group by时尝试对下层group by进行消除:
//select sum(sc2) from (select sum(c2) sc2 from t ... group by c1);
// --> select sum(c2) from (select c2 from t ...);
int ObTransformSimplifyGroupby::remove_redundant_group_by(ObDMLStmt *stmt, bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  bool is_valid = false;
  ObArray<ObSelectStmt*> valid_child_stmts;
  ObSelectStmt *select_stmt = NULL;
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
  } else if (OB_FAIL(get_valid_child_stmts(select_stmt,
                                           select_stmt->get_table_item(0)->ref_query_,
                                           valid_child_stmts))) {
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

//upper stmt pre check条件:
//  1.upper stmt 为单表查询, from table 为 generate table
//  2.包含group by
//  3.condition不存在rownum
//  4.仅存在min/max/sum/bit_and/bit_or/bit_xor aggr, sum aggr无distinct
//  5.是标准group by,无非聚合输出
int ObTransformSimplifyGroupby::check_upper_stmt_validity(ObSelectStmt *upper_stmt, bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  if (OB_ISNULL(upper_stmt) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (!upper_stmt->has_group_by()) {
    is_valid = false;
  } else if (!is_only_full_group_by_on(ctx_->session_info_->get_sql_mode())) {
    is_valid = false;
  }
  //check condition不存在rownum
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < upper_stmt->get_condition_size(); ++i) {
    ObRawExpr *cond_expr = NULL;
    if (OB_ISNULL(cond_expr = upper_stmt->get_condition_expr(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null expr", K(ret));
    } else if (cond_expr->has_flag(CNT_ROWNUM)) {
      is_valid = false;
    }
  }
  //check aggr仅存在 min/max/sum/bit_and/bit_or/bit_xor, sum且无 distinct, bit_xor的expr本身就不支持distinct
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < upper_stmt->get_aggr_item_size(); ++i) {
    ObAggFunRawExpr *aggr_expr = NULL;
    if (OB_ISNULL(aggr_expr = upper_stmt->get_aggr_item(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null aggr", K(ret));
    } else if (T_FUN_MIN != aggr_expr->get_expr_type() &&
               T_FUN_MAX != aggr_expr->get_expr_type() &&
               T_FUN_SUM != aggr_expr->get_expr_type() &&
               T_FUN_SYS_BIT_AND != aggr_expr->get_expr_type() &&
               T_FUN_SYS_BIT_OR != aggr_expr->get_expr_type() &&
               T_FUN_SYS_BIT_XOR != aggr_expr->get_expr_type()) {
      is_valid = false;
    } else if (T_FUN_SUM == aggr_expr->get_expr_type() && aggr_expr->is_param_distinct()) {
      is_valid = false;
    }
  }
  return ret;
}

//获取可消除group by的child stmt, 对union all查找所有分支, 判断条件:
//  1.child stmt 不存在 window function/having/distinct/rownum/limit/rollup
//  2.upper stmt group by 不能包含 child stmt aggr
//  3.upper stmt aggr 与 child aggr 满足消除匹配关系
//  4.upper stmt condition 没有使用 child stmt aggr
int ObTransformSimplifyGroupby::get_valid_child_stmts(ObSelectStmt *upper_stmt,
                                                      ObSelectStmt *stmt,
                                                      ObArray<ObSelectStmt*> &valid_child_stmts)
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
    if (ObSelectStmt::UNION != stmt->get_set_op()
        || stmt->is_set_distinct()
        || stmt->has_limit()) {//判断条件1
      is_valid = false;
    } else {
      ObIArray<ObSelectStmt*> &child_stmts = stmt->get_set_query();
      for (int64_t i = 0; OB_SUCC(ret) && i < child_stmts.count(); ++i) {
        ret = SMART_CALL(get_valid_child_stmts(upper_stmt, child_stmts.at(i), valid_child_stmts));
      }
    }
  } else if (!stmt->has_group_by()
             || stmt->has_rollup()
             || stmt->has_window_function()//判断条件1
             || stmt->has_having()
             || stmt->has_distinct()
             || stmt->has_sequence()
             || stmt->has_limit()) {
    is_valid = false;
  } else if (OB_FAIL(stmt->has_rownum(has_rownum))) {
    LOG_WARN("failed to check has rownum", K(ret));
  } else if (has_rownum) {
    is_valid = false;
  } else if (OB_FAIL(get_upper_column_exprs(*upper_stmt, *stmt,//获取upper stmt column item中的child aggr列与非aggr列
                                            aggr_column_exprs,
                                            no_aggr_column_exprs,
                                            child_aggr_exprs,
                                            is_valid))) {
    LOG_WARN("failed to get upper column exprs", K(ret));
  } else if (!is_valid) {
    /*do nothing*/
  } else if (OB_FAIL(check_upper_group_by(*upper_stmt, no_aggr_column_exprs, is_valid))) {//判断条件2
    LOG_WARN("failed to check upper group by", K(ret));
  } else if (!is_valid) {
    /*do nothing*/
  } else if (OB_FAIL(check_aggrs_matched(upper_stmt->get_aggr_items(),//判断条件3
                                         aggr_column_exprs, no_aggr_column_exprs,
                                         child_aggr_exprs, is_valid))) {
    LOG_WARN("failed to check aggrs matched", K(ret));
  } else if (!is_valid) {
    /*do nothing*/
  } else if (OB_FAIL(check_upper_condition(upper_stmt->get_condition_exprs(),//判断条件4
                                           aggr_column_exprs, is_valid))) {
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

//移除child_stmts中的group by/order by/aggr, 对原select中的aggr进行处理:
//  1. max/min/sum使用aggr参数添加cast后替换aggr,
//  2. count替换为is not, not null时结果为与count结果类型相同的const
int ObTransformSimplifyGroupby::remove_child_stmts_group_by(ObArray<ObSelectStmt*> &child_stmts)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->session_info_) || OB_ISNULL(ctx_->expr_factory_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ctx_));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < child_stmts.count(); ++i) {
    ObSelectStmt *stmt = NULL;
    if (OB_ISNULL(stmt = child_stmts.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else {
      stmt->get_aggr_items().reset();
      stmt->get_group_exprs().reset();
      stmt->get_rollup_exprs().reset();
      stmt->get_rollup_dirs().reset();
      stmt->get_order_items().reset();
      for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_select_item_size(); ++i) {
        ObRawExpr *expr = stmt->get_select_item(i).expr_;
        if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null", K(ret));
        } else if (!expr->is_aggr_expr()) {
          /*do nothing*/
        } else if (T_FUN_MAX == expr->get_expr_type() ||
                   T_FUN_MIN == expr->get_expr_type() ||
                   T_FUN_SUM == expr->get_expr_type() ||
                   T_FUN_SYS_BIT_AND == expr->get_expr_type() ||
                   T_FUN_SYS_BIT_OR == expr->get_expr_type() ||
                   T_FUN_SYS_BIT_XOR == expr->get_expr_type()) {
          ObRawExpr *cast_expr;
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

//从stmt select中匹配group by/aggr到upper stmt column item
int ObTransformSimplifyGroupby::get_upper_column_exprs(ObSelectStmt &upper_stmt,
                                                       ObSelectStmt &stmt,
                                                       ObIArray<ObRawExpr*> &aggr_column_exprs,
                                                       ObIArray<ObRawExpr*> &no_aggr_column_exprs,
                                                       ObIArray<ObRawExpr*> &child_aggr_exprs,
                                                       bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  TableItem *table_item = NULL;
  ObSelectStmt *ref_query = NULL;
  if (OB_UNLIKELY(!upper_stmt.is_single_table_stmt())
      || OB_ISNULL(table_item = upper_stmt.get_table_item(0))
      || OB_UNLIKELY(!table_item->is_generated_table())
      || OB_ISNULL(ref_query = table_item->ref_query_)
      || OB_UNLIKELY(stmt.get_select_item_size() != ref_query->get_select_item_size())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected upper stmt", K(ret), K(upper_stmt));
  }
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < stmt.get_select_item_size(); ++i) {
    ObRawExpr *stmt_select_expr = NULL;
    ColumnItem *column_item = NULL;
    if (OB_ISNULL(stmt_select_expr = stmt.get_select_item(i).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_ISNULL(column_item = upper_stmt.get_column_item_by_id(table_item->table_id_,
                                                                        i + OB_APP_MIN_COLUMN_ID))) {
      // a select expr contains aggr function, it's not used by upper stmt.
      // project pruning will remove it, do not transform here.
      is_valid = !stmt_select_expr->has_flag(CNT_AGG);
    } else if (OB_ISNULL(column_item->expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null expr", K(ret));
    } else if (stmt_select_expr->is_aggr_expr()) {//is aggr
      int64_t idx = OB_INVALID_INDEX;
      if (!ObOptimizerUtil::find_item(stmt.get_aggr_items(),
                                      static_cast<ObAggFunRawExpr*>(stmt_select_expr),
                                      &idx)) {
        is_valid = false;
        LOG_DEBUG("cannot find aggr in stmt aggr item", K(ret), K(stmt), K(*stmt_select_expr));
      } else if (OB_FAIL(aggr_column_exprs.push_back(column_item->expr_))
                 || OB_FAIL(child_aggr_exprs.push_back(stmt.get_aggr_items().at(idx)))) {
        LOG_WARN("failed to push back aggr", K(ret));
      }
    } else if (ObOptimizerUtil::find_item(stmt.get_group_exprs(), stmt_select_expr)
               || stmt_select_expr->is_const_expr()) {//in group by or is const
      if (OB_FAIL(no_aggr_column_exprs.push_back(column_item->expr_))) {
        LOG_WARN("failed to push back expr", K(ret));
      }
    } else {
      //可能允许转化的场景：
      //  1.select 中为 cast(aggr fun())
      //  2.select 包含 subquery
      is_valid = false;
    }
  }
  return ret;
}

//check upper stmt group by/rollup 不包含 stmt aggr 项
int ObTransformSimplifyGroupby::check_upper_group_by(ObSelectStmt &upper_stmt,
                                                     ObIArray<ObRawExpr*> &no_aggr_column_exprs,
                                                     bool &is_valid)
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

//check upper stmt aggr 与 child aggr 满足消除条件, aggr匹配条件:
//  1.upper aggr min/max/sum 参数为 child min/max/sum 结果, child aggr 为 sum 不能 distinct
//  2.upper aggr min/max 参数为 child stmt 非 aggr 列
int ObTransformSimplifyGroupby::check_aggrs_matched(ObIArray<ObAggFunRawExpr*> &upper_aggrs,
                                                    ObIArray<ObRawExpr*> &aggr_column_exprs,
                                                    ObIArray<ObRawExpr*> &no_aggr_column_exprs,
                                                    ObIArray<ObRawExpr*> &child_aggr_exprs,
                                                    bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  if (aggr_column_exprs.count() != child_aggr_exprs.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected array count", K(ret), K(aggr_column_exprs), K(child_aggr_exprs));
  }
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < upper_aggrs.count(); ++i) {
    ObAggFunRawExpr *upper_aggr = NULL;
    ObRawExpr *aggr_param = NULL;
    int64_t idx = OB_INVALID_INDEX;
    if (OB_ISNULL(upper_aggr = upper_aggrs.at(i))
        || OB_ISNULL(aggr_param = upper_aggr->get_param_expr(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if (ObOptimizerUtil::find_item(aggr_column_exprs, aggr_param, &idx)) {
      const ObAggFunRawExpr *child_aggr = static_cast<ObAggFunRawExpr*>(child_aggr_exprs.at(idx));
      if (upper_aggr->get_expr_type() != child_aggr->get_expr_type()) {//匹配条件1
        is_valid = false;
      } else if (T_FUN_SUM == child_aggr->get_expr_type()
                 && child_aggr->is_param_distinct()) {
        is_valid = false;
      } else {
        /*do nothing*/
      }
    } else if (ObOptimizerUtil::find_item(no_aggr_column_exprs, aggr_param)) {//匹配条件2
      is_valid = (T_FUN_MAX == upper_aggr->get_expr_type() ||
                  T_FUN_MIN == upper_aggr->get_expr_type() ||
                  T_FUN_SYS_BIT_AND == upper_aggr->get_expr_type() ||
                  T_FUN_SYS_BIT_OR == upper_aggr->get_expr_type() ||
                  T_FUN_SYS_BIT_XOR == upper_aggr->get_expr_type());
    } else {
      is_valid = false;
    }
  }
  return ret;
}

//check upper stmt condition 不包含 stmt aggr result
int ObTransformSimplifyGroupby::check_upper_condition(ObIArray<ObRawExpr*> &cond_exprs,
                                                      ObIArray<ObRawExpr*> &aggr_column_exprs,
                                                      bool &is_valid)
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

int ObTransformSimplifyGroupby::exist_exprs_in_expr(const ObRawExpr *src_expr,
                                                    ObIArray<ObRawExpr*> &dst_exprs,
                                                    bool &is_exist)
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
      if (OB_FAIL(SMART_CALL(exist_exprs_in_expr(src_expr->get_param_expr(i),
                                                 dst_exprs,
                                                 is_exist)))) {
        LOG_WARN("failed to smart call", K(ret));
      }
    }
  }
  return ret;
}

/* 当 group by 的表达式唯一的时候，可以做以下改写:
1. 去除 group by
2. 改写可以改写的聚合函数表达式，并删除原来的聚合函数。
*/
int ObTransformSimplifyGroupby::remove_stmt_group_by(ObDMLStmt *&stmt,
                                                     bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  ObSelectStmt *select_stmt = NULL;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (!stmt->is_select_stmt()) {
    //do nothing
  } else if (FALSE_IT(select_stmt = static_cast<ObSelectStmt *>(stmt))) {
    /*do nothing*/
  } else if (select_stmt->get_group_expr_size() > 0 && !select_stmt->has_rollup()) {
    //check stmt group by can be removed
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

int ObTransformSimplifyGroupby::check_stmt_group_by_can_be_removed(ObSelectStmt *select_stmt,
                                                                   bool &can_be)
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
  } else if (OB_FAIL(ObTransformUtils::check_stmt_unique(select_stmt, ctx_->session_info_,
                                                         ctx_->schema_checker_,
                                                         select_stmt->get_group_exprs(),
                                                         true /* strict */,
                                                         is_unique,
                                                         FLAGS_IGNORE_DISTINCT | FLAGS_IGNORE_GROUP))) {
    LOG_WARN("failed to check stmt unique", K(ret));
  } else if (is_unique) {
    can_be = true;
    for (int64_t i = 0; can_be && i < select_stmt->get_aggr_item_size(); ++i) {
      ObRawExpr *expr = static_cast<ObRawExpr* >(select_stmt->get_aggr_item(i));
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL pointer error", K(ret));
      } else if (OB_FAIL(check_aggr_win_can_be_removed(select_stmt, expr, can_be))) {
        LOG_WARN("fialed to check aggr can be removed", K(ret), K(*expr));
      }
    }
  } else {
    //do nothing
  }
  return ret;
}

int ObTransformSimplifyGroupby::inner_remove_stmt_group_by(ObSelectStmt *select_stmt, bool &trans_happened)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(select_stmt) || OB_ISNULL(ctx_) ||
      OB_ISNULL(ctx_->expr_factory_) || OB_ISNULL(ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL pointer error", K(ret), K(select_stmt), K(ctx_),
             K(ctx_->expr_factory_), K(ctx_->session_info_));
  } else {
    ObArray<ObRawExpr*> new_exprs;
    ObArray<ObRawExpr*> old_exprs;
    for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt->get_aggr_item_size(); ++i) {
      ObRawExpr *new_expr = NULL;
      ObRawExpr *expr = static_cast<ObRawExpr* >(select_stmt->get_aggr_item(i));
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
      select_stmt->get_aggr_items().reset();
      select_stmt->get_group_exprs().reset();
      if (OB_FAIL(select_stmt->replace_relation_exprs(old_exprs, new_exprs))) {
        LOG_WARN("select_stmt replace inner stmt expr failed", K(ret), K(select_stmt));
      } else if (OB_FAIL(append(select_stmt->get_condition_exprs(), select_stmt->get_having_exprs()))) {
        LOG_WARN("failed append having exprs to condition exprs", K(ret));
      } else {
        select_stmt->get_having_exprs().reset();
        trans_happened = true;
      }
    }
  }
  return ret;
}

//移除group by重复列
//select * from t1 group by c1,c1,c1
//==>
//select * from t1 group by c1
int ObTransformSimplifyGroupby::remove_group_by_duplicates(ObDMLStmt *&stmt, bool &trans_happened)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *select_stmt = NULL;
  trans_happened = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null pointer", K(ret));
  } else if (!stmt->is_select_stmt()) {
    //do nothing
  } else if (FALSE_IT(select_stmt = static_cast<ObSelectStmt *>(stmt))) {
    /* do nothing */
  } else if (select_stmt->get_group_expr_size() > 0 && !select_stmt->has_rollup()) {
    ObArray<ObRawExpr*> new_group_exprs;
    ObIArray<ObRawExpr*> &group_exprs = select_stmt->get_group_exprs();
    for (int64_t i = 0; OB_SUCC(ret) && i < group_exprs.count(); ++i) {
      if (ObOptimizerUtil::find_item(new_group_exprs, group_exprs.at(i))) {
        /*do nothing*/
      } else if (OB_FAIL(new_group_exprs.push_back(group_exprs.at(i)))) {
        LOG_WARN("new group exprs push back failed", K(ret), K(group_exprs.at(i)));
      } else {
        /*do nothing*/
      }
    }
    if (OB_SUCC(ret)) {
      if (new_group_exprs.count() == group_exprs.count()) {
        //do nothing
      } else {
        select_stmt->get_group_exprs().reset();
        if (OB_FAIL(select_stmt->get_group_exprs().assign(new_group_exprs))) {
          LOG_WARN("failed to assign a new group exprs", K(ret));
        } else {
          trans_happened = true;
        }
      }
    }
  }
  return ret;
}

// try to remove distinct in aggr(distinct) and window_function(distinct)
int ObTransformSimplifyGroupby::remove_aggr_distinct(ObDMLStmt *stmt, bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  ObAggFunRawExpr *aggr_expr = NULL;
  ObWinFunRawExpr *win_expr = NULL;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(stmt), K(ctx_));
  } else if (stmt->is_select_stmt()) {
    ObSelectStmt *select_stmt = static_cast<ObSelectStmt *>(stmt);
    // remove distinct in aggr
    for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt->get_aggr_item_size(); ++i) {
      if (OB_ISNULL(aggr_expr = select_stmt->get_aggr_item(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null aggr expr", K(ret));
      } else if (!aggr_expr->is_param_distinct()) {
        // do nothing
      } else if (T_FUN_MAX == aggr_expr->get_expr_type() ||
                 T_FUN_MIN == aggr_expr->get_expr_type()) {
        // max/min(distinct) 可以直接消掉distinct
        aggr_expr->set_param_distinct(false);
        trans_happened = true;
      } else if (T_FUN_SUM == aggr_expr->get_expr_type() ||
                 T_FUN_COUNT == aggr_expr->get_expr_type() ||
                 T_FUN_GROUP_CONCAT == aggr_expr->get_expr_type()) {
        // sum/count/group_concat(distinct) 要求param在做group by之前是非严格unique的
        ObSEArray<ObRawExpr *, 4> aggr_param_exprs;
        bool is_unique = false;
        if (OB_FAIL(aggr_param_exprs.assign(aggr_expr->get_real_param_exprs()))) {
          LOG_WARN("failed to push back aggr param expr", K(ret));
        } else if (OB_FAIL(append(aggr_param_exprs, select_stmt->get_group_exprs()))) {
          LOG_WARN("failed to append group by expr", K(ret));
        } else if (OB_FAIL(ObTransformUtils::check_stmt_unique(select_stmt, ctx_->session_info_,
                                                               ctx_->schema_checker_,
                                                               aggr_param_exprs,
                                                               false,
                                                               is_unique,
                                                               FLAGS_IGNORE_DISTINCT|FLAGS_IGNORE_GROUP))) {
          LOG_WARN("failed to check stmt unique", K(ret));
        } else if (is_unique) {
          aggr_expr->set_param_distinct(false);
          trans_happened = true;
        }
      } else {
        // do nothing
      }
    }
    // 整合消除distinct之后重复出现的aggr item:select min(distinct c), min(c) from t1;
    if (OB_SUCC(ret) && trans_happened) {
      if (OB_FAIL(remove_aggr_duplicates(select_stmt))) {
        LOG_WARN("failed to remove aggr item duplicates", K(ret));
      } else {/*do nothing*/}
    }
    // remove distinct in window function
    bool win_happened = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt->get_window_func_count(); ++i) {
      if (OB_ISNULL(win_expr = select_stmt->get_window_func_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null window function expr", K(ret));
      } else if (NULL == (aggr_expr = win_expr->get_agg_expr()) ||
                 !aggr_expr->is_param_distinct()) {
        // do nothing if window function not has aggr expr(such as rownum)
        // or aggr expr not contain distinct
      } else if (T_FUN_MAX == aggr_expr->get_expr_type() ||
                 T_FUN_MIN == aggr_expr->get_expr_type()) {
        // max/min(distinct) 可以直接消掉distinct
        aggr_expr->set_param_distinct(false);
        trans_happened = true;
        win_happened = true;
      } else if (T_FUN_SUM == aggr_expr->get_expr_type() ||
                 T_FUN_COUNT == aggr_expr->get_expr_type()) {
        // sum/count(distinct) 要求param在做group by之后是非严格unique的
        ObSEArray<ObRawExpr *, 4> aggr_param_exprs;
        bool is_unique = false;
        if (OB_FAIL(aggr_param_exprs.assign(aggr_expr->get_real_param_exprs()))) {
          LOG_WARN("failed to push back aggr param expr", K(ret));
        } else if (OB_FAIL(ObTransformUtils::check_stmt_unique(select_stmt, ctx_->session_info_,
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
      } else {/*do nothing*/}
    }

  }
  return ret;
}

int ObTransformSimplifyGroupby::remove_aggr_duplicates(ObSelectStmt *select_stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(select_stmt));
  } else {
    ObSEArray<ObRawExpr *, 4> old_aggr_exprs;
    ObSEArray<ObRawExpr *, 4> new_aggr_exprs;
    ObSEArray<ObAggFunRawExpr *, 4> new_aggr_items;
    ObAggFunRawExpr *old_aggr_expr = NULL;
    ObAggFunRawExpr *new_aggr_expr = NULL;
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
            } else {/*do nothing*/}
          } else {/*do nothing*/}
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (!old_aggr_exprs.empty()) {
        if (OB_UNLIKELY(old_aggr_exprs.count() != new_aggr_exprs.count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("replace expr item size not equal", K(ret), K(old_aggr_exprs.count()),
                   K(new_aggr_exprs.count()));
        } else if (OB_FAIL(select_stmt->get_aggr_items().assign(new_aggr_items))) {
          LOG_WARN("failed to assign aggr items failed", K(ret));
        } else if (OB_FAIL(select_stmt->replace_relation_exprs(old_aggr_exprs,
                                                               new_aggr_exprs))) {
          LOG_WARN("failed to replace inner stmt expr", K(ret));
        }
      } else { /*do nothing */}
    }
  }
  return ret;
}

int ObTransformSimplifyGroupby::remove_win_func_duplicates(ObSelectStmt *select_stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(select_stmt));
  } else {
    ObSEArray<ObRawExpr *, 4> old_win_func_exprs;
    ObSEArray<ObRawExpr *, 4> new_win_func_exprs;
    ObSEArray<ObWinFunRawExpr *, 4> new_win_func_items;
    ObWinFunRawExpr *old_win_func_expr = NULL;
    ObWinFunRawExpr *new_win_func_expr = NULL;
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
            } else {/*do nothing*/}
          } else {/*do nothing*/}
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (!old_win_func_exprs.empty()) {
        if (OB_UNLIKELY(old_win_func_exprs.count() != new_win_func_exprs.count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("replace expr item size not equal", K(ret), K(old_win_func_exprs.count()),
                   K(new_win_func_exprs.count()));
        } else if (OB_FAIL(select_stmt->get_window_func_exprs().assign(new_win_func_items))) {
          LOG_WARN("failed to assign win func items failed", K(ret));
        } else if (OB_FAIL(select_stmt->replace_relation_exprs(old_win_func_exprs,
                                                               new_win_func_exprs))) {
          LOG_WARN("failed to replace inner stmt expr", K(ret));
        }
      } else { /*do nothing */}
    }
  }
  return ret;
}



// convert count(1) --> count(*)
// convert count([distinct] null) --> 0
// if is scaler group by and count is the only aggr, do not transform
int ObTransformSimplifyGroupby::convert_count_aggr_contain_const(ObDMLStmt *stmt,
                                                                 bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  ObSelectStmt *select_stmt = NULL;
  ObSEArray<ObAggFunRawExpr*, 2> count_const;
  ObSEArray<ObAggFunRawExpr*, 2> count_null;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (!stmt->is_select_stmt() ||
             FALSE_IT(select_stmt = static_cast<ObSelectStmt *>(stmt))) {
    /*do nothing*/
  } else if (select_stmt->has_rollup()) {
    /*do nothing*/
  } else if (OB_FAIL(get_valid_count_aggr(select_stmt, count_null, count_const))) {
    LOG_WARN("failed to get calid count exprs", K(ret));
  } else if (count_null.empty() && count_const.empty()) {
    /*do nothing*/
  } else if (select_stmt->is_scala_group_by() &&
             count_null.count() == select_stmt->get_aggr_items().count()) {
    /* scaler group by and count is the only aggr, do not transform */
  } else if (OB_FAIL(convert_valid_count_aggr(select_stmt, count_null, count_const))) {
    LOG_WARN("failed to convert valid count aggr", K(ret));
  } else {
    trans_happened = true;
  }
  return ret;
}

int ObTransformSimplifyGroupby::convert_valid_count_aggr(ObSelectStmt *select_stmt,
                                                         ObIArray<ObAggFunRawExpr*> &count_null,
                                                         ObIArray<ObAggFunRawExpr*> &count_const)
{
  int ret = OB_SUCCESS;
  ObAggFunRawExpr *aggr = NULL;
  ObSEArray<ObRawExpr*, 2> count_null_exprs;
  ObSEArray<ObRawExpr*, 2> const_zeros;
  ObConstRawExpr *const_zero = NULL;
  if (OB_ISNULL(select_stmt) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null", K(ret), K(select_stmt), K(ctx_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < count_null.count(); ++i) {
      if (OB_ISNULL(aggr = count_null.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null", K(ret), K(aggr));
      } else if (OB_FAIL(ObTransformUtils::add_const_param_constraints(aggr->get_param_expr(0),
                                                                       ctx_))) {
        LOG_WARN("failed to add const param constraints", K(ret));
      } else if (OB_FAIL(count_null_exprs.push_back(aggr))) {
        LOG_WARN("failed to push back exprs", K(ret));
      } else if (OB_FAIL(ObTransformUtils::build_const_expr_for_count(*ctx_->expr_factory_, 0,
                                                                      const_zero))) {
        LOG_WARN("failed to build const expr for count", K(ret));
      } else if (OB_FAIL(const_zeros.push_back(const_zero))) {
        LOG_WARN("failed to push back exprs", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < count_const.count(); ++i) {
      if (OB_ISNULL(aggr = count_const.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null", K(ret), K(aggr));
      } else if (OB_FAIL(ObTransformUtils::add_const_param_constraints(aggr->get_param_expr(0),
                                                                       ctx_))) {
        LOG_WARN("failed to add const param constraints", K(ret));
      } else {
        aggr->get_real_param_exprs_for_update().reuse();
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObOptimizerUtil::remove_item(select_stmt->get_aggr_items(), count_null))) {
      LOG_WARN("failed to remove items", K(ret));
    } else if (OB_FAIL(select_stmt->replace_relation_exprs(count_null_exprs, const_zeros))) {
      LOG_WARN("failed to replace inner stmt exprs", K(ret));
    }
  }
  return ret;
}

int ObTransformSimplifyGroupby::get_valid_count_aggr(ObSelectStmt *select_stmt,
                                                     ObIArray<ObAggFunRawExpr*> &count_null,
                                                     ObIArray<ObAggFunRawExpr*> &count_const)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(select_stmt) || OB_ISNULL(ctx_) ||
      OB_ISNULL(ctx_->allocator_) || OB_ISNULL(ctx_->exec_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null", K(ret), K(select_stmt), K(ctx_));
  } else {
    ObAggFunRawExpr *aggr = NULL;
    ObRawExpr *param = NULL;
    ObObj obj_value;
    bool got_result = false;
    ObIArray<ObAggFunRawExpr*> &aggrs = select_stmt->get_aggr_items();
    for (int64_t i = 0; OB_SUCC(ret) && i < aggrs.count(); ++i) {
      if (OB_ISNULL(aggr = aggrs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null", K(ret), K(aggr));
      } else if (T_FUN_COUNT != aggr->get_expr_type() || 1 != aggr->get_real_param_exprs().count()) {
        /* do nothing */
        /* count(distinct 1, null) can not convert, count(1, null) do not convert now */
      } else if (OB_ISNULL(param = aggr->get_param_expr(0))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null", K(ret), K(param));
      } else if (!param->is_static_scalar_const_expr()) {
        /* do nothing */
      } else if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(ctx_->exec_ctx_,
                                                                   param, obj_value,
                                                                   got_result,
                                                                   *ctx_->allocator_))) {
        LOG_WARN("failed to calc const or calculable expr", K(ret));
      } else if (!got_result) {
        /* do nonthing */
      } else if (obj_value.is_null()) {
        ret = count_null.push_back(aggr);
      } else if (aggr->is_param_distinct()) {
        /* do nothing */
      } else {
        ret = count_const.push_back(aggr);
      }
    }
  }
  return ret;
}

int ObTransformSimplifyGroupby::check_aggr_win_can_be_removed(const ObDMLStmt *stmt,
                                                              ObRawExpr *expr,
                                                              bool &can_remove)
{
  can_remove = false;
  int ret = OB_SUCCESS;
  ObItemType func_type = T_INVALID;
  ObAggFunRawExpr *aggr = NULL;
  ObWinFunRawExpr *win_func = NULL;
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
    case T_FUN_COUNT: //case when 1 or 0
    case T_FUN_MAX: //return expr
    case T_FUN_MIN:
      //case T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS_MERGE: //不进行改写
    case T_FUN_AVG: //return expr
    case T_FUN_COUNT_SUM:
    case T_FUN_SUM:
      //case T_FUN_APPROX_COUNT_DISTINCT: // return 1 or 0 //不进行改写
      //case T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS://不进行改写
      // case T_FUN_GROUP_RANK:// return 1 or 2    需要考虑order desc、nulls first、多列条件，暂不改写
      // case T_FUN_GROUP_DENSE_RANK:
      // case T_FUN_GROUP_PERCENT_RANK:// return 1 or 0
      // case T_FUN_GROUP_CUME_DIST:// return 1 or 0.5
    case T_FUN_MEDIAN: // return expr
    case T_FUN_SYS_BIT_AND: // return expr or UINT_MAX_VAL[ObUInt64Type]
    case T_FUN_SYS_BIT_OR:  // return expr or 0
    case T_FUN_SYS_BIT_XOR: // return expr or 0
    case T_FUN_GROUP_PERCENTILE_CONT:
    case T_FUN_GROUP_PERCENTILE_DISC:
    case T_FUN_KEEP_MAX:
    case T_FUN_KEEP_MIN:
    case T_FUN_KEEP_COUNT: // return 1 or 0
    case T_FUN_KEEP_SUM: // return expr
      // 部分数学分析函数会在改写阶段进行展开:
      // ObExpandAggregateUtils::expand_aggr_expr
      // ObExpandAggregateUtils::expand_window_aggr_expr

      // window func
    case T_WIN_FUN_ROW_NUMBER: // return 1
    case T_WIN_FUN_RANK: // return 1
    case T_WIN_FUN_DENSE_RANK: // return 1
    case T_WIN_FUN_PERCENT_RANK: { // return 0
      can_remove = true;
      break;
    }
    case T_FUN_GROUP_CONCAT:{
      if (OB_ISNULL(aggr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected func", K(ret));
      } else {
        can_remove = aggr->get_real_param_count() == 1;
      }
      break;
    }
    case T_WIN_FUN_NTILE: {//return 1
      //need check invalid param: ntile(-1)
      can_remove = false;
      ObRawExpr *expr = NULL;
      int64_t bucket_num = 0;
      bool is_valid = false;
      if (OB_ISNULL(win_func) || OB_UNLIKELY(win_func->get_func_params().empty())
          || OB_ISNULL(expr = win_func->get_func_params().at(0))) {
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
    case T_WIN_FUN_NTH_VALUE: { // nth_value(expr,1) return expr, else return null
      //need check invalid param: nth_value(expr, -1)
      can_remove = false;
      ObRawExpr *nth_expr = NULL;
      int64_t value = 0;
      bool is_valid = false;
      if (OB_ISNULL(win_func) || OB_UNLIKELY(2 > win_func->get_func_params().count())
          || OB_ISNULL(nth_expr = win_func->get_func_params().at(1))) {
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
    case T_WIN_FUN_FIRST_VALUE: // return expr (respect or ignore nulls)
    case T_WIN_FUN_LAST_VALUE: { // return expr (respect or ignore nulls)
      // first_value && last_value has been converted to nth_value when resolving
      can_remove = false;
      break;
    }
    case T_WIN_FUN_CUME_DIST: { // return 1
      can_remove = true;
      break;
    }
    case T_WIN_FUN_LEAD: // return null or default value
    case T_WIN_FUN_LAG: { // return null or default value
      can_remove = false;
      ObRawExpr *expr = NULL;
      int64_t value = 0;
      bool is_valid = false;
      if (OB_ISNULL(win_func)|| OB_UNLIKELY(win_func->get_func_params().empty())) {
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
    case T_WIN_FUN_RATIO_TO_REPORT: { //resolver 阶段被转化为 expr/sum（expr）
      can_remove = true;
      break;
    }
    case T_WIN_FUN_SUM: //无效
    case T_WIN_FUN_MAX: //无效
    case T_WIN_FUN_AVG: //无效
    default: {
      can_remove = false;
      break;
    }
    }
  }
  return ret;
}

int ObTransformSimplifyGroupby::transform_aggr_win_to_common_expr(ObSelectStmt *select_stmt,
                                                                  ObRawExpr *expr,
                                                                  ObRawExpr *&new_expr)
{
  int ret = OB_SUCCESS;
  new_expr = NULL;
  ObRawExpr *param_expr = NULL;
  ObItemType func_type = T_INVALID;
  ObAggFunRawExpr *aggr = NULL;
  ObWinFunRawExpr *win_func = NULL;
  if (OB_ISNULL(select_stmt) || OB_ISNULL(expr) || OB_ISNULL(ctx_)
      || OB_ISNULL(ctx_->expr_factory_) || OB_ISNULL(ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (expr->is_aggr_expr()) {
    aggr = static_cast<ObAggFunRawExpr*>(expr);
    func_type = aggr->get_expr_type();
  } else if (expr->is_win_func_expr()) {
    win_func = static_cast<ObWinFunRawExpr*>(expr);
    func_type = win_func->get_func_type();
    aggr = win_func->get_agg_expr();
    // to fix bug: win magic 可能导致 func type 与 aggr 不一致
    func_type = NULL == aggr ? win_func->get_func_type() : aggr->get_expr_type();
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected expr", K(ret), K(expr));
  }
  if (OB_SUCC(ret)) {
    switch (func_type) {
    // aggr
    case T_FUN_MAX:// return expr
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
    case T_FUN_GROUP_PERCENTILE_CONT:// return expr
    case T_FUN_GROUP_PERCENTILE_DISC: {
      if (OB_ISNULL(aggr) || OB_UNLIKELY(aggr->get_order_items().empty())
          || OB_ISNULL(param_expr = aggr->get_order_items().at(0).expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected func", K(ret));
      }
      break;
    }
    case T_FUN_COUNT:
    case T_FUN_KEEP_COUNT: { // return 1 or 0
      ObConstRawExpr *const_one = NULL;
      ObConstRawExpr *const_zero = NULL;
      if (OB_ISNULL(aggr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(ObTransformUtils::build_const_expr_for_count(*ctx_->expr_factory_, 1,
                                                                      const_one))) {
        LOG_WARN("failed to build const expr for count", K(ret));
      } else if (0 == aggr->get_real_param_count()) { // count(*) --> 1
        param_expr = const_one;
      } else if (OB_FAIL(ObTransformUtils::build_const_expr_for_count(*ctx_->expr_factory_, 0,
                                                                      const_zero))) {
        LOG_WARN("failed to build const expr for count", K(ret));
        // count(c1) --> case when
      } else if (OB_FAIL(ObTransformUtils::build_case_when_expr(*select_stmt,
                                                                expr->get_param_expr(0),
                                                                const_one, const_zero,
                                                                param_expr, ctx_))) {
        LOG_WARN("failed to build case when expr", K(ret));
      }
      break;
    }
    case T_WIN_FUN_ROW_NUMBER: // return int 1
    case T_WIN_FUN_RANK:
    case T_WIN_FUN_DENSE_RANK: {
      ObConstRawExpr *const_one = NULL;
      if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*ctx_->expr_factory_, ObIntType,
                                                       1, const_one))) {
        LOG_WARN("failed to build const int expr", K(ret));
      } else {
        param_expr = const_one;
      }
      break;
    }
    case T_WIN_FUN_NTILE: { // return int 1 and add constraint
      ObConstRawExpr *const_one = NULL;
      if (OB_ISNULL(win_func) || OB_UNLIKELY(win_func->get_func_params().empty())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected func", K(ret), K(win_func));
      } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*ctx_->expr_factory_, ObIntType,
                                                              1, const_one))) {
        LOG_WARN("failed to build const int expr", K(ret));
      } else if (OB_FAIL(ObTransformUtils::add_const_param_constraints(
                           win_func->get_func_params().at(0),
                           ctx_))) {
        LOG_WARN("failed to add const param constraints", K(ret));
      } else {
        param_expr = const_one;
      }
      break;
    }
    case T_WIN_FUN_CUME_DIST: { // return number 1
      ObConstRawExpr *const_one = NULL;
      if (OB_FAIL(ObRawExprUtils::build_const_number_expr(*ctx_->expr_factory_, ObNumberType,
                                                          number::ObNumber::get_positive_one(),
                                                          const_one))) {
        LOG_WARN("failed to build const number expr", K(ret));
      } else {
        param_expr = const_one;
      }
      break;
    }
    case T_WIN_FUN_PERCENT_RANK: { // return number 0
      ObConstRawExpr *const_zero = NULL;
      if (OB_FAIL(ObRawExprUtils::build_const_number_expr(*ctx_->expr_factory_, ObNumberType,
                                                          number::ObNumber::get_zero(),
                                                          const_zero))) {
        LOG_WARN("failed to build const number expr", K(ret));
      } else {
        param_expr = const_zero;
      }
      break;
    }
    case T_WIN_FUN_NTH_VALUE: { // nth_value(expr,1) return expr, else return null
      // need add constraint for nth_expr
      ObRawExpr *nth_expr = NULL;
      int64_t value = 0;
      bool is_valid = false;
      if (OB_ISNULL(win_func) || OB_UNLIKELY(2 > win_func->get_func_params().count())
          || OB_ISNULL(nth_expr = win_func->get_func_params().at(1))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected func", K(ret));
      } else if (OB_FAIL(get_param_value(select_stmt, nth_expr, is_valid, value))) {
        LOG_WARN("failed to get param value", K(ret));
      } else if (OB_UNLIKELY(!is_valid || value <= 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected func", K(ret), K(*win_func));
      } else if (OB_FAIL(ObTransformUtils::add_const_param_constraints(nth_expr, ctx_))) {
        LOG_WARN("failed to add const param constraints", K(ret));
      } else if (1 == value) { // return expr
        param_expr = win_func->get_func_params().at(0);
      } else { //return null
        ret = ObRawExprUtils::build_null_expr(*ctx_->expr_factory_, param_expr);
      }
      break;
    }
    case T_WIN_FUN_LEAD: // return null or default value
    case T_WIN_FUN_LAG: {
      ObRawExpr *expr = NULL;
      int64_t value = 1;
      bool is_valid = false;
      if (OB_ISNULL(win_func)|| OB_UNLIKELY(win_func->get_func_params().empty())) {
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
      } else if (OB_FAIL(ObTransformUtils::add_const_param_constraints(expr, ctx_))) {
        LOG_WARN("failed to add const param constraints", K(ret));
      }

      if (OB_FAIL(ret)) {
      } else if (0 == value) { // return current value
        param_expr = win_func->get_func_params().at(0);
      } else if (2 < win_func->get_func_params().count()) { // return default value
        param_expr = win_func->get_func_params().at(2);
      } else if (OB_FAIL(ObRawExprUtils::build_null_expr(*ctx_->expr_factory_, param_expr))) {
        LOG_WARN("failed to build null expr", K(ret));
      }
      break;
    }
    case T_FUN_SYS_BIT_AND:
    case T_FUN_SYS_BIT_OR:
    case T_FUN_SYS_BIT_XOR: {
      if (OB_FAIL(ObTransformUtils::transform_bit_aggr_to_common_expr(*select_stmt, aggr, ctx_, param_expr))) {
        LOG_WARN("transform bit aggr to common expr failed", KR(ret), K(*aggr), K(func_type), K(*select_stmt));
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected func", K(ret), K(*expr));
      break;
    }
    }

    //尝试添加类型转化
    if (OB_FAIL(ret)) {
      /*do nothing*/
    } else if (OB_ISNULL(param_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::try_add_cast_expr_above(ctx_->expr_factory_,
                                                               ctx_->session_info_,
                                                               *param_expr,
                                                               expr->get_result_type(),
                                                               new_expr))) {
      LOG_WARN("try add cast expr above failed", K(ret));
    }
  }
  return ret;
}

int ObTransformSimplifyGroupby::get_param_value(const ObDMLStmt *stmt,
                                                ObRawExpr *param,
                                                bool &is_valid,
                                                int64_t &value)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  ObObj obj_value;
  bool got_result = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(param) || OB_ISNULL(ctx_) ||
      OB_ISNULL(ctx_->allocator_) || OB_ISNULL(ctx_->exec_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null", K(ret), K(stmt), K(param), K(ctx_));
  } else if (param->is_static_scalar_const_expr() &&
             (param->get_result_type().is_integer_type() ||
              param->get_result_type().is_number())) {
    if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(ctx_->exec_ctx_,
                                                          param, obj_value,
                                                          got_result,
                                                          *ctx_->allocator_))) {
      LOG_WARN("Failed to calc const or calculable expr", K(ret));
    } else if (!got_result) {
      is_valid = false;
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
    } else if (!obj_value.is_number()) {
      is_valid = false;
    } else if (OB_FAIL(obj_value.get_number(number))) {
      LOG_WARN("unexpected value type", K(ret), K(obj_value));
    } else if (OB_UNLIKELY(!number.is_valid_int64(value))) {
      is_valid = false;
    }
  }
  return ret;
}

// 1. With group by
//    select max(1) from t1 group by c1; -> select 1 from t1 group by c1;
// 2. Without group by && not from dual
//    select max(1) from t1; -> select max(t.a) from (select 1 as a from t1 limit 1) t;
int ObTransformSimplifyGroupby::transform_const_aggr(ObDMLStmt *stmt, bool &trans_happened)
{
  int ret = OB_SUCCESS;
  bool is_valid = false;
  ObRawExpr *expr = NULL;
  TableItem *table = NULL;
  trans_happened = false;
  ObSelectStmt *select_stmt = NULL;
  ObSelectStmt *view_stmt = NULL;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is NULL", K(ret), K(stmt));
  } else if (!stmt->is_select_stmt() ||
             FALSE_IT(select_stmt = static_cast<ObSelectStmt *>(stmt))) {
    //do nothing
  } else if (OB_FAIL(is_valid_const_aggregate(select_stmt, is_valid))) {
    LOG_WARN("failed to check is valid const aggregate", K(ret));
  } else if (!is_valid) {
    // do nothing
  } else if (OB_UNLIKELY(1 != select_stmt->get_select_item_size())
             || OB_ISNULL(expr = select_stmt->get_select_item(0).expr_)
             || OB_UNLIKELY(!expr->is_aggr_expr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("select item is invalid", K(ret));
  } else if (select_stmt->get_group_expr_size() > 0) {
    // with group by
    if (T_FUN_MAX == expr->get_expr_type() || T_FUN_MIN == expr->get_expr_type()) {
      select_stmt->get_select_item(0).expr_ = expr->get_param_expr(0);
    } else if (OB_FAIL(ObTransformUtils::transform_bit_aggr_to_common_expr(*select_stmt,
                                                                           expr,
                                                                           ctx_,
                                                                           expr))) {
      LOG_WARN("transform bit aggr to common expr failed", KR(ret), K(*expr), K(expr->get_expr_type()), K(*select_stmt));
    } else if (OB_FAIL(ObRawExprUtils::try_add_cast_expr_above(ctx_->expr_factory_,
                                                               ctx_->session_info_,
                                                               *expr,
                                                               expr->get_result_type(),
                                                               select_stmt->get_select_item(0).expr_))) {
      LOG_WARN("try add cast expr above failed", K(ret));
    }

    if (OB_SUCC(ret)) {
      select_stmt->get_aggr_items().reset();
      select_stmt->get_order_items().reset();
      trans_happened = true;
    }
  } else if (0 == select_stmt->get_from_items().count()) {
    //from dual
  } else if (select_stmt->is_single_table_stmt()
             && OB_NOT_NULL(table = select_stmt->get_table_item(0))
             && table->is_generated_table()) {
    ObSelectStmt *ref_query = NULL;
    if (OB_ISNULL(ref_query = table->ref_query_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if (ref_query->has_limit() ||
               ref_query->is_scala_group_by() ||
               ref_query->get_table_size() == 0) {
      /*do nothing*/
    } else if (OB_FAIL(ObTransformUtils::set_limit_expr(ref_query, ctx_))) {
      LOG_WARN("fail to set child limit item", K(ret));
    } else {
      trans_happened = true;
    }
  } else if (OB_FAIL(ObTransformUtils::create_simple_view(ctx_, select_stmt, view_stmt))) {
    LOG_WARN("failed to create simple view", K(ret));
  } else if (OB_FAIL(ObTransformUtils::set_limit_expr(view_stmt, ctx_))) {
    LOG_WARN("failed to set limit expr", K(ret));
  } else {
    trans_happened = true;
  }
  return ret;
}

int ObTransformSimplifyGroupby::is_valid_const_aggregate(ObSelectStmt *stmt, bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is NULL", K(ret));
  } else if (1 == stmt->get_select_item_size() &&
             !stmt->has_having() &&
             1 == stmt->get_aggr_item_size()) {
    SelectItem &select_item = stmt->get_select_item(0);
    if (OB_FAIL(is_const_aggr(stmt, select_item.expr_, is_valid))) {
      LOG_WARN("is_min_max_const() fails", K(ret), K(is_valid));
    }
  }
  return ret;
}

int ObTransformSimplifyGroupby::is_const_aggr(ObSelectStmt *stmt,
                                                 ObRawExpr *expr,
                                                 bool &is_const)
{
  int ret = OB_SUCCESS;
  is_const = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr from select_item is NULL", K(ret), K(expr));
  } else if (T_FUN_MAX == expr->get_expr_type() ||
             T_FUN_MIN == expr->get_expr_type() ||
             T_FUN_SYS_BIT_AND == expr->get_expr_type() ||
             T_FUN_SYS_BIT_OR == expr->get_expr_type()) {
    if (OB_UNLIKELY(expr->get_param_count() != 1) ||
        OB_ISNULL(expr->get_param_expr(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(ret), KPC(expr));
    } else {
      is_const = expr->get_param_expr(0)->is_const_expr();
    }
  } else { /* Do nothing */ }
  return ret;
}

int ObTransformSimplifyGroupby::prune_group_by_rollup(ObIArray<ObParentDMLStmt> &parent_stmts,
                                                      ObDMLStmt *stmt,
                                                      bool &trans_happened)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *select_stmt = NULL;
  int64_t pruned_expr_idx = -1;
  trans_happened = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (!stmt->is_select_stmt()) {
    // do nothing
  } else if (FALSE_IT(select_stmt = static_cast<ObSelectStmt *>(stmt))) {
    // do nothing
  } else if (!select_stmt->has_rollup()) {
    // do nothing
  } else if (OB_FAIL(check_can_prune_rollup(parent_stmts, select_stmt, pruned_expr_idx))) {
    LOG_WARN("failed to check rollup expr", K(ret));
  } else if (-1 == pruned_expr_idx) {
    // do nothing
  } else if (OB_FAIL(do_prune_rollup(select_stmt, pruned_expr_idx))) {
    LOG_WARN("failed to do prune rollup", K(ret));
  } else {
    trans_happened = true;
  }
  return ret;
}

int ObTransformSimplifyGroupby::check_can_prune_rollup(ObIArray<ObParentDMLStmt> &parent_stmts,
                                                       ObSelectStmt *stmt,
                                                       int64_t &pruned_expr_idx)
{
  int ret = OB_SUCCESS;
  int64_t pruned_by_self_idx = -1;
  int64_t pruned_by_parent_idx = -1;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (OB_FAIL(check_rollup_pruned_by_self(stmt, pruned_by_self_idx))) {
    LOG_WARN("failed to check rollup expr by self", K(ret));
  } else if (OB_FAIL(check_rollup_pruned_by_parent(parent_stmts, stmt, pruned_by_parent_idx))) {
    LOG_WARN("failed to check rollup expr by parent", K(ret));
  } else {
    pruned_expr_idx = MAX(pruned_by_self_idx, pruned_by_parent_idx);
  }
  return ret;
}

int ObTransformSimplifyGroupby::check_rollup_pruned_by_self(ObSelectStmt *stmt,
                                                            int64_t &pruned_expr_idx)
{
  int ret = OB_SUCCESS;
  bool is_found = false;
  ObSEArray<ObRawExpr*, 4> valid_having_exprs;
  pruned_expr_idx = -1;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (OB_FAIL(get_valid_having_exprs_contain_aggr(stmt->get_having_exprs(),
                                                         valid_having_exprs))) {
    LOG_WARN("failed to check contain aggr", K(ret));
  } else if (valid_having_exprs.empty()) {
    //do nothing
  } else {
    for (int64_t i = stmt->get_rollup_exprs().count() - 1; OB_SUCC(ret) && !is_found && i >= 0; --i) {
      bool has_null_reject = false;
      bool is_first = false;
      if (OB_FAIL(ObTransformUtils::has_null_reject_condition(valid_having_exprs,
                                                              stmt->get_rollup_exprs().at(i),
                                                              has_null_reject))) {
        LOG_WARN("failed to check has null reject condition", K(ret));
      } else if (!has_null_reject) {
        //do nothing
      } else if (OB_FAIL(is_first_rollup_with_duplicates(stmt, i, is_first))) {
        LOG_WARN("failed to check is first rollup", K(ret));
      } else if (!is_first) {
        // do nothing
      } else {
        is_found = true;
        pruned_expr_idx = i;
      }
    }
  }
  return ret;
}

int ObTransformSimplifyGroupby::check_rollup_pruned_by_parent(ObIArray<ObParentDMLStmt> &parent_stmts,
                                                              ObSelectStmt *stmt,
                                                              int64_t &pruned_expr_idx)
{
  int ret = OB_SUCCESS;
  pruned_expr_idx = -1;
  ObDMLStmt *parent_stmt = NULL;
  TableItem *table_item = NULL;
  if (parent_stmts.empty()) {
    // do nothing
  } else if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_ISNULL(parent_stmt = parent_stmts.at(parent_stmts.count() - 1).stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(ObTransformUtils::get_generated_table_item(*parent_stmt, stmt, table_item))) {
    LOG_WARN("failed to get table_item", K(ret));
  } else if (OB_NOT_NULL(table_item)) {
    bool found = false;
    ObSEArray<ObRawExpr *, 16> conditions;
    if (OB_FAIL(ObTransformUtils::get_table_related_condition(*parent_stmt,
                                                              table_item,
                                                              conditions))) {
      LOG_WARN("failed to get table related condition", K(ret));
    }
    for (int64_t i = stmt->get_rollup_exprs().count() - 1; OB_SUCC(ret) && !found && i >= 0; --i) {
      bool has_null_reject = false;
      bool is_first = false;
      ObSEArray<ObRawExpr *, 2> select_exprs;
      ObSEArray<ObRawExpr *, 2> targets;
      if (OB_FAIL(find_null_propagate_select_exprs(stmt,
                                                    stmt->get_rollup_exprs().at(i),
                                                    select_exprs))) {
        LOG_WARN("failed to find null propagate select expr", K(ret));
      } else if (OB_FAIL(ObTransformUtils::convert_select_expr_to_column_expr(select_exprs,
                                                                              *stmt,
                                                                              *parent_stmt,
                                                                              table_item->table_id_,
                                                                              targets))) {
        LOG_WARN("failed to convert expr to column epxr", K(ret));
      } else if (OB_FAIL(ObTransformUtils::has_null_reject_condition(conditions,
                                                                      targets,
                                                                      has_null_reject))) {
        LOG_WARN("failed to check has null reject condition", K(ret));
      } else if (!has_null_reject) {
        //do nothing
      } else if (OB_FAIL(is_first_rollup_with_duplicates(stmt, i, is_first))) {
        LOG_WARN("failed to check is first rollup", K(ret));
      } else if (!is_first) {
        // do nothing
      } else {
        found = true;
        pruned_expr_idx = i;
      }
    }
  }
  return ret;
}

int ObTransformSimplifyGroupby::find_null_propagate_select_exprs(ObSelectStmt *stmt,
                                                                 const ObRawExpr *expr,
                                                                 ObIArray<ObRawExpr *> &select_exprs)
{
  int ret = OB_SUCCESS;
  ObSEArray<const ObRawExpr *, 1> dummy_exprs;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (OB_FAIL(dummy_exprs.push_back(expr))) {
    LOG_WARN("failed to push back expr", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_select_item_size(); ++i) {
    bool is_null_propagate = false;
    ObRawExpr *select_expr = stmt->get_select_item(i).expr_;
    if (OB_FAIL(ObTransformUtils::is_null_propagate_expr(select_expr,
                                                         dummy_exprs,
                                                         is_null_propagate))) {
      LOG_WARN("failed to check null propagate expr", K(ret));                                                    
    } else if (!is_null_propagate) {
      // do nothing
    } else if (OB_FAIL(select_exprs.push_back(select_expr))) {
      LOG_WARN("failed to push back expr", K(ret));
    }
  }
  return ret;
}

int ObTransformSimplifyGroupby::is_first_rollup_with_duplicates(ObSelectStmt *stmt,
                                                                const int64_t rollup_expr_idx,
                                                                bool &is_first)
{
  int ret = OB_SUCCESS;
  int64_t idx = OB_INVALID_INDEX;
  is_first = false;
  ObRawExpr *rollup_expr = NULL;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (OB_UNLIKELY(rollup_expr_idx < 0)
             || OB_UNLIKELY(rollup_expr_idx >= stmt->get_rollup_exprs().count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid rollup expr idx", K(rollup_expr_idx), K(ret));
  } else if (FALSE_IT(rollup_expr = stmt->get_rollup_exprs().at(rollup_expr_idx))) {
    // do nothing
  } else if (ObOptimizerUtil::find_item(stmt->get_group_exprs(), rollup_expr)) {
    is_first = false;
  } else if (OB_UNLIKELY(!ObOptimizerUtil::find_item(stmt->get_rollup_exprs(),
                                                     rollup_expr,
                                                     &idx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to find rollup expr in rollup exprs", K(ret));
  } else {
    is_first = idx == rollup_expr_idx;
  }
  return ret;
}

int ObTransformSimplifyGroupby::do_prune_rollup(ObSelectStmt *stmt, const int64_t pruned_expr_idx)
{
  int ret = OB_SUCCESS;
  bool found = false;
  ObArray<ObRawExpr *> new_rollup_exprs;
  ObArray<ObOrderDirection> new_rollup_dirs;
  bool has_rollup_dir = stmt->has_rollup_dir();
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (OB_UNLIKELY(has_rollup_dir
                         && (stmt->get_rollup_dir_size() != stmt->get_rollup_exprs().count()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to check rollup exprs and directions count", K (ret));
  } else if (OB_UNLIKELY(pruned_expr_idx < 0)
             || OB_UNLIKELY(pruned_expr_idx >= stmt->get_rollup_exprs().count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid pruned expr idx", K(pruned_expr_idx), K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_rollup_exprs().count(); ++i) {
    ObRawExpr *rollup_expr = stmt->get_rollup_exprs().at(i);
    if (i <= pruned_expr_idx) {
      if (OB_FAIL(stmt->add_group_expr(rollup_expr))) {
        LOG_WARN("failed to push back rollup expr", K(ret));
      }
    } else {
      if (OB_FAIL(new_rollup_exprs.push_back(rollup_expr))) {
        LOG_WARN("failed to push back rollup expr", K(ret));
      } else if (has_rollup_dir &&
                 OB_FAIL(new_rollup_dirs.push_back(stmt->get_rollup_dirs().at(i)))) {
        LOG_WARN("failed to push back rollup expr", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    stmt->get_rollup_exprs().reset();
    stmt->get_rollup_dirs().reset();
    if (OB_FAIL(stmt->get_rollup_exprs().assign(new_rollup_exprs))) {
      LOG_WARN("failed to assign new rollup exprs", K(ret));
    } else if (OB_FAIL(stmt->get_rollup_dirs().assign(new_rollup_dirs))) {
      LOG_WARN("failed to assign new rollup dirs", K(ret));
    }
  }
  return ret;
}

//bug:
// create table t1(c1 int, c2 int);
// select sum(c1) from t1 group by c1,c2 with rollup having sum(c1) > 1 ==> can't prune
int ObTransformSimplifyGroupby::get_valid_having_exprs_contain_aggr(
    const ObIArray<ObRawExpr *> &having_exprs,
    ObIArray<ObRawExpr *> &vaild_having_exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < having_exprs.count(); ++i) {
    if (OB_ISNULL(having_exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(having_exprs.at(i)));
    } else if (having_exprs.at(i)->has_flag(CNT_AGG)) {
      //do nothing
    } else if (OB_FAIL(vaild_having_exprs.push_back(having_exprs.at(i)))) {
      LOG_WARN("faield to push back", K(ret));
    } else {/*do nothing*/}
  }
  return ret;
}

/*A group by clause can be converted to distinct when all of the following conditions are met:
1. There is no aggregate function or window function or rownum
2. having_exprs don't contain subquery exprs
2. The group by expr is a subset of the select expr
3. A fd relationship exists between the select expr and the group by expr
   Only consider the case that the group by expr is not unique.
   The case thar group by expr is unique has been included in remove_stmt_group_by.
e.g.
select c1,c2,c1+c2 from t group by c1,c2;
=> select distinct c1,c2,c1+c2 from t;
*/
int ObTransformSimplifyGroupby::convert_group_by_to_distinct(ObDMLStmt *stmt,
                                                      bool &trans_happened) {
  int ret = OB_SUCCESS;
  ObSelectStmt *select_stmt = NULL;
  bool can_convert = true;
  trans_happened = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (!stmt->is_select_stmt() ||
             FALSE_IT(select_stmt = static_cast<ObSelectStmt *>(stmt))) {
    /*do nothing*/
  } else if (OB_FAIL(check_can_convert_to_distinct(select_stmt, can_convert))) {
    LOG_WARN("check stmt group by can be removed failed", K(ret), K(*select_stmt));
  } else if (!can_convert) {
    /*do nothing*/
  } else {
    select_stmt->assign_distinct();
    if (OB_FAIL(append(select_stmt->get_condition_exprs(), select_stmt->get_having_exprs()))) {
      LOG_WARN("failed append having exprs to condition exprs", K(ret));
    } else {
      select_stmt->get_having_exprs().reset();
      select_stmt->get_group_exprs().reset();
      trans_happened = true;
    }
  }
  return ret;
}

int ObTransformSimplifyGroupby::check_can_convert_to_distinct(ObSelectStmt *stmt, bool &can_convert) {
  int ret = OB_SUCCESS;
  bool has_rownum = false;
  can_convert = false;
  ObSEArray<ObRawExpr*, 4> select_exprs;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (stmt->get_group_expr_size() == 0 ||
             stmt->has_rollup() ||
             stmt->has_grouping_sets() ||
             stmt->get_aggr_item_size() > 0 ||
             stmt->has_window_function()) {
    /* do nothing */
  } else if (OB_FAIL(stmt->has_rownum(has_rownum))){
    LOG_WARN("failed to check rownum info", K(ret));
  } else if (has_rownum) {
    /*do nothing */
  } else if (OB_FAIL(stmt->get_select_exprs(select_exprs))) {
    LOG_WARN("fail to get select exprs", K(ret));
  } else if (!ObOptimizerUtil::subset_exprs(stmt->get_group_exprs(), select_exprs)) {
  } else {
    //check if having_expr has subquery
    bool has_subquery = false;
    for (int64_t i = 0; OB_SUCC(ret) && !has_subquery && i < stmt->get_having_expr_size(); ++i) {
      if (OB_ISNULL(stmt->get_having_exprs().at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret), K(stmt->get_having_exprs().at(i)));
      } else if (stmt->get_having_exprs().at(i)->has_flag(CNT_SUB_QUERY)) {
        has_subquery = true;
      }
    }
    can_convert = !has_subquery;
    if (can_convert) {
      //check fd
      bool is_calculable = true;
      for (int64_t i = 0; OB_SUCC(ret) && is_calculable && i < select_exprs.count(); ++i) {
        if (OB_ISNULL(select_exprs.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null", K(ret), K(select_exprs.at(i)));
        } else if (OB_FAIL(ObOptimizerUtil::expr_calculable_by_exprs(select_exprs.at(i),
                                             stmt->get_group_exprs(),
                                             is_calculable))) {
          LOG_WARN("fail to check if select expr is const or exist", K(ret));
        } else {
          //do nothing
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && is_calculable && i < stmt->get_having_expr_size(); ++i) {
        if (OB_ISNULL(stmt->get_having_exprs().at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null", K(ret), K(stmt->get_having_exprs().at(i)));
        } else if (OB_FAIL(ObOptimizerUtil::expr_calculable_by_exprs(stmt->get_having_exprs().at(i),
                                                            stmt->get_group_exprs(),
                                                            is_calculable))) {
          LOG_WARN("fail to check if having expr is const or exist", K(ret));
        } else {
          //do nothing
        }
      }
      can_convert = is_calculable;
    }
  }
  return ret;
}