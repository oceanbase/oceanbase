/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_RESV
#include "sql/resolver/mv/ob_mv_checker_utils.h"

#include "sql/engine/expr/ob_expr_result_type_util.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "sql/resolver/mv/ob_mv_checker.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
using namespace common;

namespace sql
{

bool ObMVCheckerUtils::is_group_recalculate_aggr(const ObRawExpr &aggr)
{
  if (!aggr.is_aggr_expr() || !IS_AGGR_FUN(aggr.get_expr_type())) {
    return false;
  }
  return !is_basic_aggr(aggr);
}

bool ObMVCheckerUtils::is_basic_count(const ObRawExpr &aggr)
{ return is_basic_aggr(aggr) && T_FUN_COUNT == aggr.get_expr_type(); }

bool ObMVCheckerUtils::is_basic_sum(const ObRawExpr &aggr)
{ return is_basic_aggr(aggr) && T_FUN_SUM == aggr.get_expr_type(); }

bool ObMVCheckerUtils::is_count_star(const ObRawExpr &aggr)
{ return is_basic_count(aggr) && 0 == static_cast<const ObAggFunRawExpr &>(aggr).get_real_param_count(); }

bool ObMVCheckerUtils::is_basic_aggr(const ObRawExpr &aggr)
{
  bool is_basic = false;
  if (!aggr.is_aggr_expr()
      || static_cast<const ObAggFunRawExpr &>(aggr).is_param_distinct()) {
    is_basic = false;
  } else if (T_FUN_COUNT == aggr.get_expr_type() || T_FUN_SUM == aggr.get_expr_type()) {
    is_basic = true;
  } else {
    is_basic = false;
  }
  return is_basic;
}

int ObMVCheckerUtils::build_sum_contribution_param(ObRawExprFactory &expr_factory,
                                                   ObSQLSessionInfo &session_info,
                                                   ObRawExpr *param,
                                                   ObRawExpr *&result)
{
  // For parameters of unsigned integer types, we need to cast them to signed type,
  // otherwise the contribution of SUM of the deleted rows: `-1 * unsigned`
  // would raise an 'value is out-of-range' error
  int ret = OB_SUCCESS;
  const ObRawExpr *orig_param = NULL;
  result = NULL;
  if (OB_ISNULL(param) || OB_ISNULL(orig_param = ObRawExprUtils::skip_implicit_cast(param))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null sum parameter", K(ret), KP(param));
  } else if (!ob_is_unsigned_type(orig_param->get_data_type())) {
    // for signed type, no additional processing is needed
    result = param;
  } else if (ObUNumberType == orig_param->get_data_type()) {
    // `-1 * UNUMBER` would be promoted to `NUMBER` so no additional cast is needed
    result = param;
  } else {
    const ObObjType type = orig_param->get_data_type();
    ObObjType sum_type = ObMaxType;
    ObRawExprResType cast_type;
    if (OB_FAIL(ObExprResultTypeUtil::get_sum_result_type(sum_type, type))) {
      LOG_WARN("failed to get sum result type", K(ret), K(type));
    } else if (FALSE_IT(cast_type.set_type(sum_type))) {
    } else if (ObNumberType == sum_type) {
      // BIT's ObAccuracy is measured in bits; its numeric range is equivalent to UINT64.
      const ObObjType accuracy_type = ObBitType == type ? ObUInt64Type : type;
      cast_type.set_accuracy(ObAccuracy::MAX_ACCURACY2[lib::is_oracle_mode()][accuracy_type]);
    }
    if (OB_SUCC(ret)) {
      ObSysFunRawExpr *cast_expr = NULL;
      if (OB_FAIL(ObRawExprUtils::create_cast_expr(expr_factory,
                                                   param,
                                                   cast_type,
                                                   cast_expr,
                                                   &session_info,
                                                   false,
                                                   CM_EXPLICIT_CAST))) {
        LOG_WARN("failed to cast unsigned sum parameter", K(ret));
      // clear the inner-added flag to preserve the explicit cast expr in printing
      } else if (OB_FAIL(cast_expr->clear_flag(IS_INNER_ADDED_EXPR))) {
        LOG_WARN("failed to preserve explicit cast", K(ret));
      } else {
        result = cast_expr;
      }
    }
  }
  return ret;
}

const char *ObMVCheckerUtils::get_table_type_str(const TableItem::TableType type)
{
  const char *type_str = NULL;
  switch (type) {
  case TableItem::BASE_TABLE: type_str = "BASE TABLE"; break;
  case TableItem::ALIAS_TABLE: type_str = "ALIAS TABLE"; break;
  case TableItem::GENERATED_TABLE: type_str = "GENERATED TABLE"; break;
  case TableItem::JOINED_TABLE: type_str = "JOIN TABLE"; break;
  case TableItem::CTE_TABLE: type_str = "CTE TABLE"; break;
  case TableItem::FUNCTION_TABLE: type_str = "FUNCTION TABLE"; break;
  case TableItem::TRANSPOSE_TABLE: type_str = "TRANSPOSE TABLE"; break;
  case TableItem::TEMP_TABLE: type_str = "TEMP TABLE"; break;
  case TableItem::LINK_TABLE: type_str = "LINK TABLE"; break;
  case TableItem::JSON_TABLE: type_str = "JSON TABLE"; break;
  case TableItem::VALUES_TABLE: type_str = "VALUES TABLE"; break;
  case TableItem::LATERAL_TABLE: type_str = "LATERAL TABLE"; break;
  default: type_str = "UNKNOWN TABLE TYPE"; break;
  }

  return type_str;
}

// Collects column references from compound SELECT exprs that are not projected directly
// so outer-join refresh can rebuild null-extended output rows.
int ObMVCheckerUtils::collect_outer_join_output_dep_cols(const ObSelectStmt &stmt,
                                                         const int64_t stmt_idx,
                                                         ObMVCheckerResult &result)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 8> single_columns_in_select;
  ObSEArray<ObRawExpr *, 8> all_columns;
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_select_item_size(); ++i) {
    ObRawExpr *expr = NULL;
    if (OB_ISNULL(expr = stmt.get_select_items().at(i).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null select expr", K(ret), K(i), K(stmt.get_select_items()));
    } else if (expr->is_column_ref_expr()) {
      if (OB_FAIL(single_columns_in_select.push_back(expr))) {
        LOG_WARN("failed to push back select expr", K(ret));
      }
    } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(expr, all_columns))) {
      LOG_WARN("failed to extract column exprs", K(ret));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(ObOptimizerUtil::remove_item(all_columns, single_columns_in_select))) {
    LOG_WARN("failed to remove single column exprs", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < all_columns.count(); ++i) {
    if (OB_FAIL(result.add_dependent_column(all_columns.at(i), stmt_idx))) {
      LOG_WARN("failed to add dependent column", K(ret), K(i));
    }
  }
  return ret;
}

int ObMVCheckerUtils::collect_aggr_dep_cols(const ObSelectStmt &stmt,
                                            ObRawExprFactory &expr_factory,
                                            ObSQLSessionInfo &session_info,
                                            const int64_t stmt_idx,
                                            const bool enable_simplify_aggr_dep,
                                            ObMVCheckerResult &result)
{
  int ret = OB_SUCCESS;
  // for GROUP BY stmt: add count(*) to dependent columns
  if (stmt.is_scala_group_by()) {
    // do not add count(*) for scalar group by here for compatibility reasons
    // if a count(*) is indeed necessary, it would be added later
  } else if (OB_FAIL(collect_default_count_expr(stmt, expr_factory, session_info, stmt_idx, result))) {
    LOG_WARN("failed to collect default count expression", K(ret));
  }
  // for supported aggr(x): add aggr(x) and count(x) to dependent columns
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_aggr_item_size(); ++i) {
    ObAggFunRawExpr *aggr_expr = stmt.get_aggr_item(i);
    ObRawExpr *param_expr = NULL;
    if (OB_ISNULL(aggr_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("aggregate expression is unexpected null", K(ret), K(i));
    } else if (!is_basic_aggr(*aggr_expr)) {
      // Group-recalculate aggregates do not need basic aggregate dependencies.
    } else if (is_basic_count(*aggr_expr)) {
      if (0 == aggr_expr->get_real_param_count()) {
        // complement necessary count(*) for scalar group by
        if (!stmt.is_scala_group_by() || stmt.check_is_select_item_expr(aggr_expr)) {
          // do nothing
        } else if (OB_FAIL(result.add_dependent_column(aggr_expr, stmt_idx))) {
          LOG_WARN("failed to add scalar count dependent column", K(ret), KPC(aggr_expr));
        }
      } else if (OB_UNLIKELY(1 != aggr_expr->get_real_param_count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("count expression has unexpected parameters", K(ret), KPC(aggr_expr));
      } else if (stmt.check_is_select_item_expr(aggr_expr)) {
      } else if (OB_FAIL(collect_equivalent_count_aggr(stmt,
                                                       aggr_expr->get_param_expr(0),
                                                       expr_factory,
                                                       session_info,
                                                       enable_simplify_aggr_dep,
                                                       stmt_idx,
                                                       result))) {
        LOG_WARN("failed to collect count dependent column", K(ret), KPC(aggr_expr));
      }
    } else if (is_basic_sum(*aggr_expr)) {
      if (OB_UNLIKELY(1 != aggr_expr->get_real_param_count()) || OB_ISNULL(param_expr = aggr_expr->get_param_expr(0))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sum expression has unexpected parameters", K(ret), KPC(aggr_expr));
      } else if (!stmt.check_is_select_item_expr(aggr_expr) && OB_FAIL(result.add_dependent_column(aggr_expr, stmt_idx))) {
        LOG_WARN("failed to add sum dependent column", K(ret), KPC(aggr_expr));
      } else if (OB_FAIL(collect_equivalent_count_aggr(stmt,
                                                       param_expr,
                                                       expr_factory,
                                                       session_info,
                                                       enable_simplify_aggr_dep,
                                                       stmt_idx,
                                                       result))) {
        LOG_WARN("failed to collect count dependent column for sum", K(ret), KPC(aggr_expr));
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_TRACE("aggr type is not supported", K(ret), K(aggr_expr->get_expr_type()));
    }
  }
  return ret;
}

int ObMVCheckerUtils::collect_default_count_expr(const ObSelectStmt &stmt,
                                                 ObRawExprFactory &expr_factory,
                                                 ObSQLSessionInfo &session_info,
                                                 const int64_t stmt_idx,
                                                 ObMVCheckerResult &result)
{
  int ret = OB_SUCCESS;
  int64_t select_idx = OB_INVALID_INDEX;
  ObAggFunRawExpr *count_expr = NULL;
  if (OB_FAIL(ObMVCheckerUtils::find_default_count_expr(stmt, select_idx))) {
    LOG_WARN("failed to find default count expression", K(ret));
  } else if (OB_INVALID_INDEX != select_idx) {
    // COUNT(*) already exists in the select list.
  } else if (OB_FAIL(build_count_expr(expr_factory, session_info, NULL, count_expr))) {
    LOG_WARN("failed to build default count expression", K(ret));
  } else if (OB_FAIL(result.add_dependent_column(count_expr, stmt_idx))) {
    LOG_WARN("failed to add default count dependent column", K(ret), K(stmt_idx));
  }
  return ret;
}

int ObMVCheckerUtils::collect_equivalent_count_aggr(const ObSelectStmt &stmt,
                                                    ObRawExpr *count_param,
                                                    ObRawExprFactory &expr_factory,
                                                    ObSQLSessionInfo &session_info,
                                                    const bool enable_simplify_aggr_dep,
                                                    const int64_t stmt_idx,
                                                    ObMVCheckerResult &result)
{
  int ret = OB_SUCCESS;
  int64_t select_idx = OB_INVALID_INDEX;
  ObRawExpr *check_param = NULL;
  ObAggFunRawExpr *count_expr = NULL;
  if (OB_FAIL(ObMVCheckerUtils::get_equivalent_count_aggr(stmt,
                                                          count_param,
                                                          &session_info,
                                                          enable_simplify_aggr_dep,
                                                          select_idx))) {
    LOG_WARN("failed to get equivalent count aggregate", K(ret), KPC(count_param));
  } else if (OB_INVALID_INDEX != select_idx) {
    if (select_idx == stmt.get_select_item_size()
        && stmt.is_scala_group_by()
        && OB_FAIL(collect_default_count_expr(stmt, expr_factory, session_info, stmt_idx, result))) {
      LOG_WARN("failed to collect scalar default count expression", K(ret));
    }
  } else if (OB_FAIL(ObMVCheckerUtils::get_equivalent_null_check_param(count_param, check_param))) {
    LOG_WARN("failed to get equivalent null check parameter", K(ret), KPC(count_param));
  } else if (OB_FAIL(build_count_expr(expr_factory, session_info, check_param, count_expr))) {
    LOG_WARN("failed to build equivalent count aggregate", K(ret), KPC(check_param));
  } else if (OB_FAIL(result.add_dependent_column(count_expr, stmt_idx))) {
    LOG_WARN("failed to add equivalent count dependent column", K(ret), K(stmt_idx));
  }
  return ret;
}

/**
 * @brief ObMVCheckerUtils::get_equivalent_count_aggr
 *
 * Find the equivalent count expr of COUNT(count_param) from the select list.
 * 1. if the count_param is not NULL, it is equivalent to COUNT(*).
 * 2. COUNT(c1*c1) is equivalent to COUNT(c1).
 *
 * Return the select_idx (index in the stmt's select list) of the equivalent
 * count expr.
 * select_idx == -1:
 *     the equivalent count expr is not found.
 * select_idx in [0, stmt.get_select_item_size() - 1]:
 *     the select_idx is the select_idx of the equivalent count expr.
 * select_idx == stmt.get_select_item_size():
 *     equivalent to COUNT(*), and the COUNT(*) is not exists in the
 *     current select list.
 */
int ObMVCheckerUtils::get_equivalent_count_aggr(const ObSelectStmt &stmt,
                                                ObRawExpr *count_param,
                                                ObSQLSessionInfo *session_info,
                                                const bool enable_simplify_aggr_dep,
                                                int64_t &select_idx)
{
  int ret = OB_SUCCESS;
  bool is_not_null = false;
  ObRawExpr *check_param = NULL;
  select_idx = -1;
  if (OB_FAIL(check_count_param_not_null(stmt, count_param, session_info, is_not_null))) {
    LOG_WARN("failed to check count parameter nullability", K(ret));
  } else if (is_not_null) {
    // equivalent to count(*)
    if (OB_FAIL(find_default_count_expr(stmt, select_idx))) {
      LOG_WARN("failed to find default count expr", K(ret));
    } else if (-1 == select_idx) {
      // COUNT(*) is not exists in the select list
      // If the stmt is normal group by
      //     For creating materialized view, COUNT(*) already has been added
      //     into the dependent columns list.
      //     For refreshing materialized view, it is unexpected and will be
      //     handled in the MVPrinter (ObSimpleMAVPrinter).
      // If the stmt is scalar group by
      //     COUNT(*) may not be added into the dependent columns list, check
      //     whether there has other equivalent first (set select_idx later).
      if (!stmt.is_scala_group_by()) {
        select_idx = stmt.get_select_item_size();
      }
    }
  }
  if (OB_FAIL(ret) || -1 != select_idx) {
    // do nothing
  } else if (OB_FAIL(get_equivalent_null_check_param(count_param, check_param))) {
    LOG_WARN("failed to get null check param", K(ret));
  } else if (OB_ISNULL(check_param)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), KPC(count_param), K(check_param));
  }
  // try to find equivalent count expr in select list
  for (int64_t i = 0; OB_SUCC(ret) && -1 == select_idx && i < stmt.get_select_item_size(); ++i) {
    ObRawExpr *expr = NULL;
    ObAggFunRawExpr *aggr_expr = NULL;
    ObRawExpr *aggr_param = NULL;
    if (OB_ISNULL(expr = stmt.get_select_item(i).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null select expr", K(ret));
    } else if (!expr->is_aggr_expr()) {
      // do nothing
    } else if (OB_FALSE_IT(aggr_expr = static_cast<ObAggFunRawExpr *>(expr))) {
    } else if (T_FUN_COUNT != aggr_expr->get_expr_type()
               || aggr_expr->is_param_distinct()
               || 1 != aggr_expr->get_real_param_count()) {
      // do nothing
    } else if (OB_FAIL(get_equivalent_null_check_param(aggr_expr->get_param_expr(0), aggr_param))) {
      LOG_WARN("failed to get null check param", K(ret));
    } else if (OB_ISNULL(aggr_param)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), KPC(aggr_expr), K(aggr_param));
    } else if (aggr_param->same_as(*check_param)) {
      select_idx = i;
    }
  }
  if (OB_SUCC(ret) && -1 == select_idx && is_not_null) {
    // For not null param in scalar group by and there is not other
    // equivalent count, will build COUNT(*) as a dependent column.
    select_idx = stmt.get_select_item_size();
  }
  if (OB_SUCC(ret) && -1 == select_idx && T_OP_CASE == count_param->get_expr_type() && enable_simplify_aggr_dep) {
    /* for case when expr, temporarily use count(*) as the dependent column */
    if (OB_FAIL(find_default_count_expr(stmt, select_idx))) {
      LOG_WARN("failed to find default count expr", K(ret));
    } else if (-1 == select_idx) {
      select_idx = stmt.get_select_item_size();
    }
  }
  return ret;
}

int ObMVCheckerUtils::check_count_param_not_null(const ObSelectStmt &stmt,
                                                 ObRawExpr *count_param,
                                                 ObSQLSessionInfo *session_info,
                                                 bool &is_not_null)
{
  int ret = OB_SUCCESS;
  is_not_null = false;
  if (OB_ISNULL(count_param) || OB_ISNULL(session_info) || OB_ISNULL(session_info->get_cur_exec_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(count_param), K(session_info));
  } else {
    // Prove nullability after joins and WHERE, including outer-join null extension.
    ObNotNullContext not_null_ctx(session_info->get_cur_exec_ctx(),
                                  &session_info->get_cur_exec_ctx()->get_allocator(),
                                  &stmt);
    if (OB_FAIL(not_null_ctx.generate_stmt_context(NULLABLE_SCOPE::NS_WHERE))) {
      LOG_WARN("failed to generate stmt context", K(ret));
    } else if (OB_FAIL(ObTransformUtils::is_expr_not_null(not_null_ctx, count_param, is_not_null, NULL))) {
      LOG_WARN("failed to check expr not null", K(ret));
    }
  }
  return ret;
}


int ObMVCheckerUtils::find_default_count_expr(const ObSelectStmt &stmt, int64_t &select_idx)
{
  int ret = OB_SUCCESS;
  select_idx = -1;
  for (int64_t i = 0; OB_SUCC(ret) && -1 == select_idx && i < stmt.get_select_item_size(); ++i) {
    const ObRawExpr *expr = NULL;
    const ObAggFunRawExpr *aggr_expr = NULL;
    if (OB_ISNULL(expr = stmt.get_select_item(i).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null select expr", K(ret));
    } else if (!expr->is_aggr_expr()) {
      // do nothing
    } else if (OB_FALSE_IT(aggr_expr = static_cast<const ObAggFunRawExpr *>(expr))) {
    } else if (T_FUN_COUNT == aggr_expr->get_expr_type()
               && !aggr_expr->is_param_distinct()
               && 0 == aggr_expr->get_real_param_count()) {
      select_idx = i;
    }
  }
  return ret;
}

// We need calculate sum(c1*c1) to get the value of stddev(c1).
// To refresh sum(c1*c1) and avoid calculate count(c1*c1), count(c1) can also used to refresh sum(c1*c1).
// Here try to get c1 as equivalent aggr param of c1*c1 only.
int ObMVCheckerUtils::get_equivalent_null_check_param(ObRawExpr *param_expr, ObRawExpr *&check_param)
{
  int ret = OB_SUCCESS;
  check_param = NULL;
  if (OB_ISNULL(param_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(param_expr));
  } else if (T_FUN_SYS_CAST == param_expr->get_expr_type()) {
    // sum(cast(c1_varchar as int)) can use count(c1_varchar)
    if (OB_FAIL(SMART_CALL(get_equivalent_null_check_param(param_expr->get_param_expr(0), check_param)))) {
      LOG_WARN("failed to smart call get null check param", K(ret));
    }
  } else if (T_OP_MUL == param_expr->get_expr_type()) {
    // sum(c1*c1) can use count(c1)
    ObRawExpr *l_expr = NULL;
    ObRawExpr *r_expr = NULL;
    if (OB_ISNULL(l_expr = param_expr->get_param_expr(0)) || OB_ISNULL(r_expr = param_expr->get_param_expr(1))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret), K(l_expr), K(r_expr));
    } else if (!l_expr->same_as(*r_expr)) {
      check_param = param_expr;
    } else if (OB_FAIL(SMART_CALL(get_equivalent_null_check_param(l_expr, check_param)))) {
      LOG_WARN("failed to smart call get null check param", K(ret));
    }
  } else {
    check_param = param_expr;
  }
  return ret;
}

int ObMVCheckerUtils::build_count_expr(ObRawExprFactory &expr_factory,
                                       ObSQLSessionInfo &session_info,
                                       ObRawExpr *param_expr,
                                       ObAggFunRawExpr *&count_expr)
{
  int ret = OB_SUCCESS;
  count_expr = NULL;
  if (OB_FAIL(expr_factory.create_raw_expr(T_FUN_COUNT, count_expr))) {
    LOG_WARN("failed to create count expression", K(ret));
  } else if (OB_ISNULL(count_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("count expression is unexpected null", K(ret));
  } else if (OB_NOT_NULL(param_expr) && OB_FAIL(count_expr->add_real_param_expr(param_expr))) {
    LOG_WARN("failed to add count parameter", K(ret), KPC(param_expr));
  } else if (OB_FAIL(count_expr->formalize(&session_info))) {
    LOG_WARN("failed to formalize count expression", K(ret), KPC(count_expr));
  }
  return ret;
}

int ObMVCheckerUtils::find_union_all_marker_column(const ObSelectStmt &stmt, int64_t &marker_idx)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObSelectStmt *> &set_queries = stmt.get_set_query();
  ObSEArray<ObRawExpr *, 4> marker_exprs;
  marker_idx = OB_INVALID_INDEX;
  if (OB_UNLIKELY(!stmt.is_set_stmt() || set_queries.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected non-set or empty set stmt", K(ret), K(stmt.is_set_stmt()), K(set_queries.count()));
  }
  // A marker must isolate branches under the comparison semantics of the persisted set output.
  for (int64_t i = 0; OB_SUCC(ret) && OB_INVALID_INDEX == marker_idx && i < stmt.get_select_item_size(); ++i) {
    bool is_valid = true;
    ObRawExpr *set_expr = stmt.get_select_item(i).expr_;
    marker_exprs.reuse();
    if (OB_ISNULL(set_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected union all set select item", K(ret), K(i), K(stmt));
    }
    for (int64_t j = 0; OB_SUCC(ret) && is_valid && j < set_queries.count(); ++j) {
      ObRawExpr *expr = NULL;
      if (OB_ISNULL(set_queries.at(j))
          || OB_UNLIKELY(i >= set_queries.at(j)->get_select_item_size())
          || OB_ISNULL(expr = set_queries.at(j)->get_select_item(i).expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected union all branch select item", K(ret), K(i), K(j), K(stmt));
      } else if (OB_FAIL(check_union_all_marker_candidate(*set_expr, *expr, marker_exprs, is_valid))) {
        LOG_WARN("failed to check union all marker candidate", K(ret), K(i), K(j));
      } else if (is_valid && OB_FAIL(marker_exprs.push_back(expr))) {
        LOG_WARN("failed to record union all marker expression", K(ret));
      }
    }
    if (OB_SUCC(ret) && is_valid) {
      marker_idx = i;
    }
  }
  return ret;
}

int ObMVCheckerUtils::check_union_all_marker_candidate(
    const ObRawExpr &set_expr,
    const ObRawExpr &branch_expr,
    const ObIArray<ObRawExpr *> &marker_exprs,
    bool &is_valid)
{
  int ret = OB_SUCCESS;
  if (!branch_expr.is_const_raw_expr()) {
    is_valid = false;
  } else {
    const ObObj &value = static_cast<const ObConstRawExpr &>(branch_expr).get_value();
    is_valid = !value.is_null();
    for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < marker_exprs.count(); ++i) {
      ObRawExpr *existing_expr = marker_exprs.at(i);
      int cmp = 0;
      if (OB_ISNULL(existing_expr) || !existing_expr->is_const_raw_expr()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("existing marker expr is unexpected", K(ret), K(i), KP(existing_expr));
      } else {
        const ObObj &existing_value = static_cast<ObConstRawExpr *>(existing_expr)->get_value();
        if (value.get_type() != existing_value.get_type()) {
          is_valid = false;
        } else if (OB_FAIL(value.compare(existing_value,
                                         set_expr.get_result_type().get_collation_type(),
                                         cmp))) {
          LOG_WARN("failed to compare union all marker values", K(ret), K(value), K(existing_value));
        } else if (0 == cmp) {
          is_valid = false;
        }
      }
    }
  }
  return ret;
}

int ObMVCheckerUtils::build_union_all_marker_expr(ObRawExprFactory &expr_factory,
                                                  ObSQLSessionInfo &session_info,
                                                  ObConstRawExpr *&marker_expr)
{
  int ret = OB_SUCCESS;
  marker_expr = NULL;
  if (!lib::is_oracle_mode()
      && OB_FAIL(ObRawExprUtils::build_const_int_expr(expr_factory, ObIntType, 0, marker_expr))) {
    LOG_WARN("failed to build union all marker", K(ret));
  } else if (lib::is_oracle_mode()
             && OB_FAIL(ObRawExprUtils::build_const_number_expr(expr_factory,
                                                                ObNumberType,
                                                                number::ObNumber::get_zero(),
                                                                marker_expr))) {
    LOG_WARN("failed to build union all marker", K(ret));
  } else if (OB_ISNULL(marker_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("union all marker is unexpected null", K(ret));
  } else {
    if (lib::is_oracle_mode()) {
      marker_expr->set_accuracy(ObAccuracy::DDL_DEFAULT_ACCURACY2[ORACLE_MODE][ObNumberType]);
    }
    if (OB_FAIL(marker_expr->formalize(&session_info))) {
      LOG_WARN("failed to formalize union all marker", K(ret));
    }
  }
  return ret;
}

int ObMVCheckerUtils::get_table_rowkey_ids(const ObTableSchema *table_schema,
                                           ObSchemaGetterGuard *schema_guard,
                                           ObIArray<uint64_t> &rowkey_ids)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 4> ori_rowkey_ids;
  bool has_logic_pk = false;
  if (OB_ISNULL(table_schema) || OB_ISNULL(schema_guard) || OB_UNLIKELY(table_schema->is_mlog_table())) {
    // For the partition table without primary key, the PK (M_ROW$$, SEQUENCE$$)
    // of its mlog table is not unique (missing the partition key, only partition
    // level unique). So should NOT use this function to get the PK of an mlog table.
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), KPC(table_schema), K(schema_guard));
  } else if (OB_FAIL(table_schema->is_table_with_logic_pk(*schema_guard, has_logic_pk))) {
    LOG_WARN("failed to check table with logic pk", K(ret), KPC(table_schema));
  } else if (has_logic_pk) {
    if (OB_FAIL(table_schema->get_logic_pk_column_ids(schema_guard, ori_rowkey_ids))) {
      LOG_WARN("failed to get logic pk column ids", K(ret));
    }
  } else if (OB_FAIL(table_schema->get_rowkey_column_ids(ori_rowkey_ids))) {
    LOG_WARN("failed to get table rowkey ids", K(ret), K(has_logic_pk), KPC(table_schema));
  } else if (table_schema->get_partition_key_info().is_valid()
             && OB_FAIL(table_schema->get_partition_key_info().get_column_ids(ori_rowkey_ids))) {
    LOG_WARN("failed to add part column ids", K(ret));
  } else if (table_schema->get_subpartition_key_info().is_valid()
             && OB_FAIL(table_schema->get_subpartition_key_info().get_column_ids(ori_rowkey_ids))) {
    LOG_WARN("failed to add subpart column ids", K(ret));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(ori_rowkey_ids.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected empty rowkey id array", K(ret));
  } else if (OB_FAIL(append_array_no_dup(rowkey_ids, ori_rowkey_ids))) {
    LOG_WARN("failed to append column ids no dup", K(ret));
  }
  return ret;
}

int ObMVCheckerUtils::get_missing_table_key_column_ids(const ObSelectStmt &stmt,
                                                       const TableItem &table_item,
                                                       const ObTableSchema *table_schema,
                                                       ObSchemaGetterGuard *schema_guard,
                                                       ObIArray<uint64_t> &missing_column_ids)
{
  int ret = OB_SUCCESS;
  missing_column_ids.reuse();
  if (OB_UNLIKELY(!table_item.is_basic_table()) || OB_ISNULL(table_schema) || OB_ISNULL(schema_guard)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected table or schema", K(ret), K(table_item), KPC(table_schema), K(schema_guard));
  } else if (OB_FAIL(get_table_rowkey_ids(table_schema, schema_guard, missing_column_ids))) {
    LOG_WARN("failed to get table rowkey ids", K(ret), KPC(table_schema));
  }
  for (int64_t i = 0; OB_SUCC(ret) && !missing_column_ids.empty() && i < stmt.get_select_item_size(); ++i) {
    const ObColumnRefRawExpr *col_expr = NULL;
    if (OB_FAIL(get_column_ref_through_implicit_lossless_cast(stmt.get_select_item(i).expr_, col_expr))) {
      LOG_WARN("failed to get column ref through implicit lossless casts", K(ret), K(i));
    } else if (NULL != col_expr) {
      if (col_expr->get_table_id() == table_item.table_id_
          && OB_FAIL(ObOptimizerUtil::remove_item(missing_column_ids, col_expr->get_column_id()))) {
        LOG_WARN("failed to remove selected table key column", K(ret), KPC(col_expr));
      }
    }
  }
  return ret;
}

int ObMVCheckerUtils::check_has_rowid_exprs(const ObDMLStmt *stmt, bool &has_rowid)
{
  int ret = OB_SUCCESS;
  has_rowid = false;
  ObSEArray<ObRawExpr *, 16> exprs;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (OB_FAIL(stmt->get_relation_exprs(exprs))) {
    LOG_WARN("failed to get relation exprs", K(ret));
  }
  while (OB_SUCC(ret) && !has_rowid && !exprs.empty()) {
    ObRawExpr *expr = NULL;
    if (OB_FAIL(exprs.pop_back(expr))) {
      LOG_WARN("failed to pop back", K(ret));
    } else if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null expr", K(ret));
    } else if (expr->has_flag(IS_ROWID)) {
      has_rowid = true;
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
        if (OB_FAIL(exprs.push_back(expr->get_param_expr(i)))) {
          LOG_WARN("failed to push back param expr", K(ret), K(i));
        }
      }
    }
  }
  return ret;
}

// If `expr` is a column ref expr, set `column` to `expr`.
// If `expr` is a column ref expr with implicit lossless casts, set `column` to the column reference.
// Otherwise, set `column` to NULL.
int ObMVCheckerUtils::get_column_ref_through_implicit_lossless_cast(const ObRawExpr *expr,
                                                                    const ObColumnRefRawExpr *&column)
{
  int ret = OB_SUCCESS;
  column = NULL;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null expr", K(ret));
  } else if (!ObRawExprUtils::is_column_ref_skip_implicit_cast(expr)) {
    // Only column refs and internal CAST chains are eligible.
  } else if (OB_FAIL(ObOptimizerUtil::get_expr_without_lossless_cast(expr, expr))) {
    LOG_WARN("failed to get expr without lossless cast", K(ret));
  } else if (expr->is_column_ref_expr()) {
    column = static_cast<const ObColumnRefRawExpr *>(expr);
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
