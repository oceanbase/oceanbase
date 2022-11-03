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

#define USING_LOG_PREFIX SQL_RESV

#include "sql/resolver/dml/ob_aggr_expr_push_up_analyzer.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/resolver/dml/ob_select_resolver.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "common/ob_smart_call.h"

namespace oceanbase
{
using namespace common;
namespace sql
{
int ObAggrExprPushUpAnalyzer::analyze_and_push_up_aggr_expr(ObRawExprFactory &expr_factory,
                                                            ObAggFunRawExpr *aggr_expr,
                                                            ObRawExpr *&final_aggr)
{
  int ret = OB_SUCCESS;
  cur_level_ = cur_resolver_.get_current_level();
  if (OB_ISNULL(aggr_expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("aggr expr is null");
  } else if (OB_FAIL(level_exprs_checker_.init(AGGR_HASH_BUCKET_SIZE))) {
    LOG_WARN("init level exprs checker failed", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < aggr_expr->get_param_count(); ++i) {
    ObRawExpr *&param_expr = aggr_expr->get_param_expr(i);
    if (OB_FAIL(analyze_aggr_param_expr(param_expr))) {
      LOG_WARN("analyze aggr param expr failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    //push up aggr to final aggr stmt
    bool has_param_aggr = false;
    ObSelectResolver *final_aggr_resolver = fetch_final_aggr_resolver(&cur_resolver_,
                                                                      get_final_aggr_level());
    ObArray<ObExecParamRawExpr *> final_exec_params;
    if (NULL == final_aggr_resolver) {
      ret = OB_ERR_INVALID_GROUP_FUNC_USE;
      LOG_WARN("no resolver can produce aggregate function");
    } else if (final_aggr_resolver == &cur_resolver_) {
      // do nothing
    } else if (OB_FAIL(get_exec_params(final_aggr_resolver->get_current_level(),
                                       final_exec_params))) {
      LOG_WARN("failed to get final exec params", K(ret));
    } else if (OB_FAIL(check_param_aggr(final_exec_params, has_param_aggr))) {
      LOG_WARN("failed to check param expr level", K(ret));
    } else if (has_param_aggr) {
      ret = OB_ERR_INVALID_GROUP_FUNC_USE;
      LOG_WARN("no resolver can produce aggregate function", K(final_exec_params));
    } else if (OB_FAIL(push_up_aggr_column(final_aggr_resolver->get_current_level()))) {
      LOG_WARN("push up aggr column failed", K(ret));
    } else if (OB_FAIL(adjust_query_level(final_aggr_resolver->get_current_level()))) {
      LOG_WARN("adjust query level failed", K(ret));
    } else if (OB_FAIL(push_up_subquery_in_aggr(*final_aggr_resolver,
                                                final_exec_params))) {
      LOG_WARN("push up subquery in aggr failed", K(ret));
    } else if (OB_FAIL(ObTransformUtils::decorrelate(reinterpret_cast<ObRawExpr *&>(aggr_expr),
                                                     final_aggr_resolver->get_current_level()))) {
      LOG_WARN("failed to decorrelate expr", K(ret));
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(final_aggr_resolver->add_aggr_expr(aggr_expr))) {
        LOG_WARN("add aggr to final resolver failed", K(ret));
      } else if (final_aggr_resolver == &cur_resolver_) {
        final_aggr = aggr_expr;
      } else if (OB_FAIL(ObRawExprUtils::get_exec_param_expr(expr_factory,
                                                             final_aggr_resolver->get_subquery(),
                                                             aggr_expr,
                                                             final_aggr))) {
        LOG_WARN("failed to get exec param expr", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else if(OB_FAIL(remove_alias_exprs())) {
      LOG_WARN("failed to remove alias exprs", K(ret));
    } else if (final_aggr_resolver == &cur_resolver_) {
      // do nothing
    } else if(OB_FAIL(remove_alias_exprs(reinterpret_cast<ObRawExpr *&>(aggr_expr)))) {
      LOG_WARN("failed to remove alias exprs", K(ret));
    }
  }
  return ret;
}

int ObAggrExprPushUpAnalyzer::analyze_aggr_param_exprs(ObIArray<ObRawExpr*> &param_exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < param_exprs.count(); ++i) {
    ObRawExpr *&param_expr = param_exprs.at(i);
    if (OB_FAIL(analyze_aggr_param_expr(param_expr))) {
      LOG_WARN("analyze aggr param expr failed", K(ret));
    }
  }
  return ret;
}

/**
 * @brief ObAggrExprPushUpAnalyzer::analyze_aggr_param_expr
 * @param param_expr
 * if a aggr expr does not use any values of the current stmt,
 * the aggr expr maybe pulled up into outer stmt (both Oracle and MySQL mode)
 * @return
 */
int ObAggrExprPushUpAnalyzer::analyze_aggr_param_expr(ObRawExpr *&param_expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(param_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param expr in aggregate is null");
  } else if (param_expr->get_expr_level() == cur_level_ &&
             (param_expr->is_column_ref_expr() ||
              T_REF_ALIAS_COLUMN == param_expr->get_expr_type())) {
    has_cur_layer_column_ = true;
  }
  while (OB_SUCC(ret) && T_REF_ALIAS_COLUMN == param_expr->get_expr_type()) {
    if (OB_ISNULL(param_expr = static_cast<ObAliasRefRawExpr*>(param_expr)->get_ref_expr())) {
      //去掉alias expr
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("alias ref expr is null", K(ret));
    }
  }
  if (OB_SUCC(ret) && param_expr->is_exec_param_expr()) {
    if (OB_FAIL(exec_columns_.push_back(static_cast<ObExecParamRawExpr *>(param_expr)))) {
      LOG_WARN("failed to push back exec columns", K(ret));
    }
  }

  // the following checks whether an aggr expr takes another aggr as its param.
  if (OB_SUCC(ret) &&
      param_expr->is_aggr_expr() &&
      param_expr->get_expr_level() == cur_level_) {
    //在聚集函数中含有同层级的聚集函数，这个对于mysql不允许的
    //select count(select count(t1.c1) from t) from t1;
    //在上面的例子中，count(t1.c1)推上去了，和最外层的count()处于同一级
    //在分析子查询的聚集函数上推的时候不会报错
    //在分析外层的聚集函数上推的时候报错发现了含有聚集函数中出现了同层嵌套，需要报错
    ret = OB_ERR_INVALID_GROUP_FUNC_USE;
    LOG_WARN("aggregate nested in the same level", K(*param_expr));
  }

  if (OB_SUCC(ret) &&
      param_expr->get_expr_level() >= 0 &&
      !param_expr->is_exec_param_expr()) {
    if (OB_FAIL(level_exprs_checker_.add_expr(param_expr))) {
      LOG_WARN("add expr to level exprs failed", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < param_expr->get_param_count(); ++i) {
    ObRawExpr *&param = param_expr->get_param_expr(i);
    if (OB_ISNULL(param)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("param expr is null");
    } else if (OB_FAIL(SMART_CALL(analyze_aggr_param_expr(param)))) {
      LOG_WARN("analyze child expr failed", K(ret));
    }
  }
  if (OB_SUCC(ret) && param_expr->is_query_ref_expr()) {
    ObQueryRefRawExpr *query_ref = static_cast<ObQueryRefRawExpr*>(param_expr);
    if (OB_FAIL(analyze_child_stmt(query_ref->get_ref_stmt()))) {
      LOG_WARN("analyze child stmt failed", K(ret));
    }
  }
  return ret;
}

int ObAggrExprPushUpAnalyzer::analyze_child_stmt(ObSelectStmt *child_stmt)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExprPointer, 4> relation_exprs;
  if (OB_ISNULL(child_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child stmt is null");
  } else if (OB_FAIL(child_stmts_.push_back(child_stmt))) {
    LOG_WARN("add child stmt failed", K(ret));
  } else if (OB_FAIL(child_stmt->get_relation_exprs(relation_exprs))) {
    LOG_WARN("failed to get relation exprs", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < relation_exprs.count(); ++i) {
    ObRawExpr *expr = NULL;
    if (OB_FAIL(relation_exprs.at(i).get(expr))) {
      LOG_WARN("failed to get expr", K(ret));
    } else if (OB_FAIL(analyze_aggr_param_expr(expr))) {
      LOG_WARN("failed to analyze aggr param expr", K(ret));
    } else if (OB_FAIL(relation_exprs.at(i).set(expr))) {
      LOG_WARN("failed to set expr", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < child_stmt->get_table_size(); ++i) {
    TableItem *table_item = child_stmt->get_table_item(i);
    if (OB_ISNULL(table_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table item is null");
    } else if (!table_item->is_generated_table()) {
      //do nothing
    } else if (OB_FAIL(analyze_child_stmt(table_item->ref_query_))) {
      LOG_WARN("analyze child stmt failed", K(ret));
    }
  }
  return ret;
}

/**
 * @brief ObAggrExprPushUpAnalyzer::get_final_aggr_level
 * if the aggr expr uses current_level value, then it belongs to the current stmt
 * otherwise, check whether aggr expr uses any outer stmt value, it belongs to the the most-inner outer stmt.
 * otherwise, it still belongs to the current stmt
 * @return
 */
int32_t ObAggrExprPushUpAnalyzer::get_final_aggr_level() const
{
  int32_t cur_aggr_level = cur_resolver_.get_current_level();
  int32_t final_aggr_level = has_cur_layer_column_ ? cur_aggr_level : -1;
  //根据MySQL聚集函数上推的原理，聚集函数的位置决定于查询中引用到的column的位置，上推的层次为
  //距离当前位置最近的上层位置
  //select c1 from t1 having c1>(select c1 from t2 having c1 >(select count(t1.c1) from t3))
  //count(t1.c1)位于第一层查询
  //select c1 from t1 having c1>(select c1 from t2 having c1 >(select count(t1.c1 + t2.c1) from t3))
  //count(t1.c1)位于第二层查询
  for (int32_t i = 0; i < exec_columns_.count(); ++i) {
    ObExecParamRawExpr *aggr_column = exec_columns_.at(i);
    int32_t column_level = -1;
    if (NULL != aggr_column &&
        NULL != aggr_column->get_ref_expr() &&
        (aggr_column->get_ref_expr()->is_column_ref_expr() ||
         (cur_resolver_.get_current_scope() != T_FIELD_LIST_SCOPE && aggr_column->get_ref_expr()->get_expr_type() == T_REF_ALIAS_COLUMN))) {
      column_level = aggr_column->get_expr_level();
    }
    if (column_level >= 0 && column_level <= cur_aggr_level && column_level > final_aggr_level) {
      final_aggr_level = column_level;
    }
  }
  if (lib::is_oracle_mode() && final_aggr_level < 0) {
    //count(const_expr)的情况
    final_aggr_level = cur_aggr_level;
  }
  // in mysql mode if the final_aggr_level < 0, the aggr's params are const. e.g., count(const_expr);
  return final_aggr_level;
}

ObSelectResolver *ObAggrExprPushUpAnalyzer::fetch_final_aggr_resolver(ObDMLResolver *cur_resolver,
                                                                      int32_t final_aggr_level)
{
  ObSelectResolver *final_resolver = NULL;
  if (cur_resolver != NULL) {
    /*
     * For mysql mode, it always pull subquery up to compute
     * For oracle mode, if it is in having scope, it will pull subquery up to compute
     * if it is in order scope (and subquery does not appear in where scope), it will pull subquery up to compute
     */
    if (final_aggr_level >= 0 && cur_resolver->get_current_level() > final_aggr_level
        && NULL != cur_resolver->get_parent_namespace_resolver()
        && (lib::is_mysql_mode()
            || T_HAVING_SCOPE == cur_resolver->get_parent_namespace_resolver()->get_current_scope()
            || (T_ORDER_SCOPE == cur_resolver->get_parent_namespace_resolver()->get_current_scope()
                && T_WHERE_SCOPE != cur_resolver->get_current_scope()))) {
      if (cur_resolver->is_select_resolver() && static_cast<ObSelectResolver*>(cur_resolver)->is_in_set_query()) {
        //当前resolver是位于union等关键字标识的set query中，aggr function不再铺上给union上层的查询
        //例如：SELECT (SELECT MAX(t1.b) from t2 union select 1 from t2 where 12 < 3) FROM t1 GROUP BY t1.a;
        //MAX(t1.b)中引用了第一层的属性，但是MAX(t1.b)整个表达式保留在union的左支中
      } else {
        ObDMLResolver *next_resolver = cur_resolver->get_parent_namespace_resolver();
        final_resolver = fetch_final_aggr_resolver(next_resolver, final_aggr_level);
      }
    }
    if (NULL == final_resolver && cur_resolver->is_select_resolver()) {
      ObSelectResolver *select_resolver = static_cast<ObSelectResolver*>(cur_resolver);
      if (select_resolver->can_produce_aggr()) {
        final_resolver = select_resolver;
      } else if (lib::is_mysql_mode() && final_aggr_level < 0) {
        /* bugfix: https://work.aone.alibaba-inc.com/issue/36773892
        * in mysql, a const aggr_expr(e.g., count(const_expr)), belongs to the nearest legal level.
        * 
        * select 1 from t1 where  (select 1 from t1 group by pk having  (select 1 from t1 where count(1)));
        * --> count(1)'s level is 1; it belongs to select 1 from t1 group by pk having xxx;
        * 
        * select 1 from t1 group by pk having  (select 1 from dual where (select 1 from t1 where count(1)));
        * --> count(1)'s level is 0;
        */
        ObDMLResolver *next_resolver = cur_resolver->get_parent_namespace_resolver();
        if (NULL != next_resolver) {
          final_resolver = fetch_final_aggr_resolver(next_resolver, final_aggr_level);
        }
      }
    }
  }
  return final_resolver;
}

int ObAggrExprPushUpAnalyzer::push_up_aggr_column(int32_t final_aggr_level)
{
  int ret = OB_SUCCESS;
  //在没有上推前的aggregate function所在的level,在这个层的本身或者父查询都的列都需要被推回到standard_group_checker_中
  int32_t old_aggr_level = cur_resolver_.get_current_level();
  for (int32_t i = 0; OB_SUCC(ret) && i < exec_columns_.count(); ++i) {
    ObExecParamRawExpr *upper_column = exec_columns_.at(i);
    ObRawExpr *ref_expr = NULL;
    if (OB_ISNULL(upper_column) ||
        OB_ISNULL(ref_expr = upper_column->get_ref_expr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("aggr_column is null", K(ret), K(upper_column));
    } else if (ref_expr->is_column_ref_expr()) {
      ObColumnRefRawExpr *column_ref = static_cast<ObColumnRefRawExpr*>(ref_expr);
      int32_t column_level = column_ref->get_expr_level();
      if (column_level <= old_aggr_level && column_level != final_aggr_level) {
        //聚集函数中如果引用了和聚集同层的column，这些column是不受only full group by约束和having约束的
        ObDMLResolver *cur_resolver = &cur_resolver_;
        //get the real resolver that produce this column
        for (; OB_SUCC(ret) && cur_resolver != NULL && cur_resolver->get_current_level() > column_level;
            cur_resolver = cur_resolver->get_parent_namespace_resolver()) {}
        if (cur_resolver != NULL && cur_resolver->is_select_resolver()) {
          ObSelectResolver *select_resolver = static_cast<ObSelectResolver*>(cur_resolver);
          if (OB_FAIL(select_resolver->add_unsettled_column(column_ref))) {
            LOG_WARN("add unsettled column to select resolver failed", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObAggrExprPushUpAnalyzer::push_up_subquery_in_aggr(
    ObSelectResolver &final_resolver,
    const ObIArray<ObExecParamRawExpr *> &exec_params)
{
  int ret = OB_SUCCESS;
  //当aggr expr被上推后, aggr expr中的subquery也跟随发生了变化，被提升到了上推后的stmt中
  //在本层只存在上推后的aggr expr的引用
  ObSelectStmt *cur_stmt = cur_resolver_.get_select_stmt();
  ObSelectStmt *final_stmt = final_resolver.get_select_stmt();

  if (OB_ISNULL(cur_stmt) || OB_ISNULL(final_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cur_stmt or final_stmt is null", K(cur_stmt), K(final_stmt));
  } else if (&cur_resolver_ != &final_resolver) {
    //发生了提升
    ObIArray<ObQueryRefRawExpr*> &query_refs = cur_stmt->get_subquery_exprs();
    ObArray<ObQueryRefRawExpr*> remain_exprs;
    for (int64_t i = 0; OB_SUCC(ret) && i < query_refs.count(); ++i) {
      ObQueryRefRawExpr *query_ref = query_refs.at(i);
      if (OB_ISNULL(query_ref) || OB_ISNULL(query_ref->get_ref_stmt())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("query_ref is null", K(query_ref), K(i));
      } else if (query_ref->get_expr_level() != cur_stmt->get_current_level()
          && query_ref->get_expr_level() != final_stmt->get_current_level()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("query_ref expr level is invalid", K(query_ref->get_expr_level()),
                 K(cur_stmt->get_current_level()), K(final_stmt->get_current_level()));
      } else if (query_ref->get_expr_level() == final_stmt->get_current_level()) {
        //current stmt中的query ref被上推了，从当前stmt的索引中移除，并加入到上推后的stmt中
        if (OB_FAIL(final_stmt->add_subquery_ref(query_ref))) {
          LOG_WARN("add subquery reference to final stmt failed", K(ret));
        } else if (OB_FAIL(query_ref->get_ref_stmt()->adjust_view_parent_namespace_stmt(final_stmt))) {
          LOG_WARN("adjust view parent namespace stmt failed", K(ret));
        } else if (OB_FAIL(query_ref->add_exec_param_exprs(exec_params))) {
          LOG_WARN("failed to add exec param exprs", K(ret));
        }
      } else if (OB_FAIL(remain_exprs.push_back(query_ref))) {
        LOG_WARN("store query ref expr failed", K(ret));
      }
    }
    if (OB_SUCC(ret) && remain_exprs.count() < query_refs.count()) {
      //保留下来的query ref减少，所以发生了subquery跟随aggr上推的问题，那么需要调整cur_stmt中的序列
      cur_stmt->get_subquery_exprs().reset();
      for (int64_t i = 0; OB_SUCC(ret) && i < remain_exprs.count(); ++i) {
        if (OB_FAIL(cur_stmt->add_subquery_ref(remain_exprs.at(i)))) {
          LOG_WARN("add subquery reference failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObAggrExprPushUpAnalyzer::adjust_query_level(int32_t final_aggr_level)
{
  int ret = OB_SUCCESS;
  int32_t old_aggr_level = cur_resolver_.get_current_level();
  int32_t ascending_level = final_aggr_level - old_aggr_level;
  //聚集函数发生了上推，aggregate function中的子查询的等级也会被提升，所以需要对level进行调整
  for (int64_t i = 0; OB_SUCC(ret) && ascending_level < 0 && i < level_exprs_.count(); ++i) {
    if (OB_ISNULL(level_exprs_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("level expr is null");
    } else if (level_exprs_.at(i)->get_expr_level() >= 0 && level_exprs_.at(i)->get_expr_level() >= old_aggr_level) {
      level_exprs_.at(i)->set_expr_level(level_exprs_.at(i)->get_expr_level() + ascending_level);
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && ascending_level < 0 && i < child_stmts_.count(); ++i) {
    //TODO: (yuming)对于子查询被提升了，除了要调整child_stmt的level，还要将child stmt从当前stmt中移除，并加入到提升后的stmt中
    if (OB_ISNULL(child_stmts_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("child stmt is null");
    } else if (child_stmts_.at(i)->get_current_level() > old_aggr_level) {
      child_stmts_.at(i)->set_current_level(child_stmts_.at(i)->get_current_level() + ascending_level);
    }
  }
  return ret;
}

int ObAggrExprPushUpAnalyzer::get_exec_params(const int32_t expr_level,
                                              ObIArray<ObExecParamRawExpr *> &exec_params)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exec_columns_.count(); ++i) {
    if (OB_ISNULL(exec_columns_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("exec param is null", K(ret));
    } else if (exec_columns_.at(i)->get_expr_level() != expr_level) {
      // do nothing
    } else if (OB_FAIL(exec_params.push_back(exec_columns_.at(i)))) {
      LOG_WARN("failed to push back exec params", K(ret));
    }
  }
  return ret;
}

int ObAggrExprPushUpAnalyzer::check_param_aggr(const ObIArray<ObExecParamRawExpr *> &exec_params,
                                               bool &has_aggr)
{
  int ret = OB_SUCCESS;
  has_aggr = false;
  for (int64_t i = 0; OB_SUCC(ret) && !has_aggr && i < exec_params.count(); ++i) {
    if (OB_ISNULL(exec_params.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("exec param expr is null", K(ret));
    } else if (OB_FAIL(has_aggr_expr(exec_params.at(i)->get_ref_expr(), has_aggr))) {
      LOG_WARN("failed to check has aggr expr", K(ret));
    }
  }
  return ret;
}

int ObAggrExprPushUpAnalyzer::has_aggr_expr(const ObRawExpr *expr, bool &has)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret), K(expr));
  } else if (expr->is_aggr_expr()) {
    has = true;
  }
  for (int64_t i = 0; OB_SUCC(ret) && !has && i < expr->get_param_count(); ++i) {
    if (OB_FAIL(has_aggr_expr(expr->get_param_expr(i), has))) {
      LOG_WARN("failed to check has aggr expr", K(ret));
    }
  }
  return ret;
}

int ObAggrExprPushUpAnalyzer::remove_alias_exprs()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exec_columns_.count(); ++i) {
    ObRawExpr *ref_expr = NULL;
    if (OB_ISNULL(exec_columns_.at(i)) ||
        OB_ISNULL(ref_expr = exec_columns_.at(i)->get_ref_expr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("exec column is null", K(ret));
    }
    while (OB_SUCC(ret) && ref_expr->get_expr_type() == T_REF_ALIAS_COLUMN) {
      if (OB_ISNULL(ref_expr = static_cast<ObAliasRefRawExpr *>(ref_expr)->get_ref_expr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("alias ref expr is null", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      exec_columns_.at(i)->set_ref_expr(ref_expr);
    }
  }
  return ret;
}

int ObAggrExprPushUpAnalyzer::remove_alias_exprs(ObRawExpr* &expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (expr->get_expr_type() == T_REF_ALIAS_COLUMN) {
    while (OB_SUCC(ret) && expr->get_expr_type() == T_REF_ALIAS_COLUMN) {
      if (OB_ISNULL(expr = static_cast<ObAliasRefRawExpr *>(expr)->get_ref_expr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("alias ref expr is null", K(ret));
      }
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
    if (OB_FAIL(SMART_CALL(remove_alias_exprs(expr->get_param_expr(i))))) {
      LOG_WARN("failed to decorrelate expr", K(ret));
    }
  }

  return ret;
}

}  // namespace sql
}  // namespace oceanbase
