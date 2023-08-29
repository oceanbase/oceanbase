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
  bool has_param_aggr = false;
  ObArray<ObExecParamRawExpr *> final_exec_params;
  ObArray<ObQueryRefRawExpr *> param_query_refs;
  ObSelectResolver *min_level_resolver = NULL;
  ObSelectResolver *final_aggr_resolver = NULL;
  ObRawExpr *root_expr = aggr_expr;
  if (OB_ISNULL(aggr_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("aggr expr is null", K(ret));
  } else if (OB_FAIL(analyze_aggr_param_expr(root_expr, true))) {
    LOG_WARN("failed to analyze aggr param expr", K(ret), K(*root_expr));
  } else if (OB_FAIL(get_min_level_resolver(min_level_resolver))) {
    LOG_WARN("failed to get min level resolver", K(ret));
  } else if (OB_ISNULL(final_aggr_resolver = fetch_final_aggr_resolver(&cur_resolver_,
                                                                       min_level_resolver))) {
    ret = OB_ERR_INVALID_GROUP_FUNC_USE;
    LOG_WARN("no resolver can produce aggregate function", K(ret));
  } else if (final_aggr_resolver == &cur_resolver_) {
    // do nothing
  } else if (OB_FAIL(get_exec_params(final_aggr_resolver, exec_columns_, final_exec_params))) {
    LOG_WARN("failed to get final exec params", K(ret));
  } else if (OB_FAIL(check_param_aggr(final_exec_params, has_param_aggr))) {
    LOG_WARN("failed to check param expr level", K(ret));
  } else if (has_param_aggr) {
    ret = OB_ERR_INVALID_GROUP_FUNC_USE;
    LOG_WARN("no resolver can produce aggregate function", K(ret));
  } else if (OB_FAIL(push_up_aggr_column(final_aggr_resolver))) {
    LOG_WARN("push up aggr column failed", K(ret));
  } else if (OB_FAIL(ObTransformUtils::extract_query_ref_expr(aggr_expr, param_query_refs))) {
    LOG_WARN("failed to extract query ref exprs", K(ret));
  } else if (OB_FAIL(push_up_subquery_in_aggr(*final_aggr_resolver,
                                              param_query_refs))) {
    LOG_WARN("push up subquery in aggr failed", K(ret));
  } else if (OB_FAIL(ObTransformUtils::decorrelate(reinterpret_cast<ObRawExpr *&>(aggr_expr),
                                                   final_exec_params))) {
    LOG_WARN("failed to decorrelate exec params", K(ret));
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
  return ret;
}

/**
 * @brief ObAggrExprPushUpAnalyzer::analyze_aggr_param_expr
 * @param param_expr
 * if a aggr expr does not use any values of the current stmt,
 * the aggr expr maybe pulled up into outer stmt (both Oracle and MySQL mode)
 * @return
 */
int ObAggrExprPushUpAnalyzer::analyze_aggr_param_expr(ObRawExpr *&param_expr,
                                                      bool is_root /* = false*/,
                                                      bool is_child_stmt /* = false*/)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(param_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param expr in aggregate is null");
  } else if (!is_root) {
    if (!is_child_stmt) {
      if (param_expr->is_column_ref_expr() ||
          T_REF_ALIAS_COLUMN == param_expr->get_expr_type()) {
        has_cur_layer_column_ = true;
      }
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
        !is_child_stmt &&
        param_expr->is_aggr_expr() &&
        !static_cast<ObAggFunRawExpr *>(param_expr)->is_nested_aggr()) {
      //在聚集函数中含有同层级的聚集函数，这个对于mysql不允许的
      //select count(select count(t1.c1) from t) from t1;
      //在上面的例子中，count(t1.c1)推上去了，和最外层的count()处于同一级
      //在分析子查询的聚集函数上推的时候不会报错
      //在分析外层的聚集函数上推的时候报错发现了含有聚集函数中出现了同层嵌套，需要报错
      ret = OB_ERR_INVALID_GROUP_FUNC_USE;
      LOG_WARN("aggregate nested in the same level", K(*param_expr));
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < param_expr->get_param_count(); ++i) {
    ObRawExpr *&param = param_expr->get_param_expr(i);
    if (OB_FAIL(SMART_CALL(analyze_aggr_param_expr(param, false, is_child_stmt)))) {
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
  } else if (OB_FAIL(child_stmt->get_relation_exprs(relation_exprs))) {
    LOG_WARN("failed to get relation exprs", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < relation_exprs.count(); ++i) {
    ObRawExpr *expr = NULL;
    if (OB_FAIL(relation_exprs.at(i).get(expr))) {
      LOG_WARN("failed to get expr", K(ret));
    } else if (OB_FAIL(analyze_aggr_param_expr(expr, false, true))) {
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

int ObAggrExprPushUpAnalyzer::get_min_level_resolver(ObSelectResolver *&resolver)
{
  int ret = OB_SUCCESS;
  bool has_column = has_cur_layer_column_;
  bool is_field_list_scope = (T_FIELD_LIST_SCOPE == cur_resolver_.get_current_scope());
  ObArray<ObExecParamRawExpr *> my_exec_params;
  resolver = &cur_resolver_;

  while (OB_SUCC(ret) && !has_column && NULL != resolver) {
    if (NULL != resolver->get_parent_namespace_resolver() &&
        resolver->get_parent_namespace_resolver()->is_select_resolver()) {
      resolver = static_cast<ObSelectResolver *>(resolver->get_parent_namespace_resolver());
      if (OB_FAIL(get_exec_params(resolver, exec_columns_, my_exec_params))) {
        LOG_WARN("failed to get my exec params", K(ret));
      }
    } else {
      resolver = NULL;
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < my_exec_params.count(); ++i) {
      ObRawExpr *ref_expr = NULL;
      if (OB_ISNULL(my_exec_params.at(i)) ||
          OB_ISNULL(ref_expr = my_exec_params.at(i)->get_ref_expr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("exec param is null", K(ret), K(my_exec_params.at(i)), K(ref_expr));
      } else if (ref_expr->is_column_ref_expr() ||
                 (!is_field_list_scope && ref_expr->is_alias_ref_expr())) {
        has_column = true;
      }
    }
    if (OB_SUCC(ret) && !has_column) {
      my_exec_params.reuse();
    }
  }
  if (OB_SUCC(ret) && lib::is_oracle_mode() && resolver == NULL) {
    resolver = &cur_resolver_;
  }
  return ret;
}

ObSelectResolver *ObAggrExprPushUpAnalyzer::fetch_final_aggr_resolver(ObDMLResolver *cur_resolver,
                                                                      ObSelectResolver *min_level_resolver)
{
  ObSelectResolver *final_resolver = NULL;
  if (cur_resolver != NULL) {
    /*
     * For mysql mode, it always pull subquery up to compute
     * For oracle mode, if it is in having scope, it will pull subquery up to compute
     * if it is in order scope (and subquery does not appear in where scope), it will pull subquery up to compute
     */
    if (min_level_resolver != NULL && cur_resolver != min_level_resolver
        && NULL != cur_resolver->get_parent_namespace_resolver()
        && (lib::is_mysql_mode()
            || T_HAVING_SCOPE == cur_resolver->get_parent_namespace_resolver()->get_current_scope())) {
      /*
        * bug fix:
        *
        * For mysql, aggr func belongs to the upper level, whether there is a "union" or not.
        *
        * For oracle, aggr func not in "HAVING" belongs to the subquery, does not need to
        * push up.
        *
        * SELECT (SELECT COUNT(t1.a) FROM dual) FROM t1 GROUP BY t1.a;
        *                  *
        * SELECT (SELECT COUNT(t1.a) union select 1 where 1>2) FROM t1 GROUP BY t1.a;
        *                  *
        * SELECT 1 FROM t1 HAVING 1 in (SELECT MAX(t1.n1) FROM dual);
        *                                       *
        * Here, for oracle mode, COUNT belongs to the subquery, but MAX belongs to the
        * upper query.
        */
      ObDMLResolver *next_resolver = cur_resolver->get_parent_namespace_resolver();
      final_resolver = fetch_final_aggr_resolver(next_resolver, min_level_resolver);
    }
    if (NULL == final_resolver && cur_resolver->is_select_resolver()) {
      ObSelectResolver *select_resolver = static_cast<ObSelectResolver*>(cur_resolver);
      if (select_resolver->can_produce_aggr()) {
        final_resolver = select_resolver;
      } else if (lib::is_mysql_mode() && min_level_resolver == NULL) {
        /* bugfix:
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
          final_resolver = fetch_final_aggr_resolver(next_resolver, min_level_resolver);
        }
      }
    }
  }
  return final_resolver;
}

int ObAggrExprPushUpAnalyzer::push_up_aggr_column(ObSelectResolver *final_resolver)
{
  int ret = OB_SUCCESS;
  ObSelectResolver *resolver = final_resolver;
  ObArray<ObExecParamRawExpr *> exec_params;
  if (OB_ISNULL(final_resolver)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("final resolver is null", K(ret), K(final_resolver));
  }
  while (OB_SUCC(ret) && NULL != resolver) {
    if (NULL != resolver->get_parent_namespace_resolver() &&
        resolver->get_parent_namespace_resolver()->is_select_resolver()) {
      resolver = static_cast<ObSelectResolver *>(resolver->get_parent_namespace_resolver());
    } else {
      resolver = NULL;
    }
    if (NULL != resolver) {
      exec_params.reuse();
      if (OB_FAIL(get_exec_params(resolver, exec_columns_, exec_params))) {
        LOG_WARN("failed to get exec params", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < exec_params.count(); ++i) {
        ObExecParamRawExpr *upper_column = exec_params.at(i);
        ObRawExpr *ref_expr = NULL;
        if (OB_ISNULL(upper_column) || OB_ISNULL(ref_expr = upper_column->get_ref_expr())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("upper column is null", K(ret), K(upper_column));
        } else if (!ref_expr->is_column_ref_expr()) {
          // do nothing
        } else if (OB_FAIL(resolver->add_unsettled_column(static_cast<ObColumnRefRawExpr *>(ref_expr)))) {
          LOG_WARN("failed to add unsettle column", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObAggrExprPushUpAnalyzer::push_up_subquery_in_aggr(
    ObSelectResolver &final_resolver,
    const ObIArray<ObQueryRefRawExpr *> &query_refs)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *cur_stmt = cur_resolver_.get_select_stmt();
  ObSelectStmt *final_stmt = final_resolver.get_select_stmt();
  if (OB_ISNULL(cur_stmt) || OB_ISNULL(final_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cur_stmt or final_stmt is null", K(ret), K(cur_stmt), K(final_stmt));
  } else if (&cur_resolver_ != &final_resolver) {
    for (int64_t i = 0; OB_SUCC(ret) && i < query_refs.count(); ++i) {
      if (OB_FAIL(cur_stmt->remove_subquery_expr(query_refs.at(i)))) {
        LOG_WARN("failed to remove subquery expr", K(ret));
      } else if (OB_FAIL(final_stmt->add_subquery_ref(query_refs.at(i)))) {
        LOG_WARN("failed to add query ref", K(ret));
      }
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

int ObAggrExprPushUpAnalyzer::get_exec_params(ObDMLResolver *resolver,
                                              ObIArray<ObExecParamRawExpr *> &all_exec_params,
                                              ObIArray<ObExecParamRawExpr *> &my_exec_params)
{
  int ret = OB_SUCCESS;
  ObQueryRefRawExpr *query_ref = NULL;
  if (OB_ISNULL(resolver) || OB_ISNULL(query_ref = resolver->get_subquery())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(resolver), K(query_ref));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < all_exec_params.count(); ++i) {
    if (!ObRawExprUtils::find_expr(query_ref->get_exec_params(), all_exec_params.at(i))) {
      // do nothing
    } else if (OB_FAIL(my_exec_params.push_back(all_exec_params.at(i)))) {
      LOG_WARN("failed to push back exec param", K(ret));
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
