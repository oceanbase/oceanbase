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

#define USING_LOG_PREFIX SQL_OPT
#include "ob_optimizer_util.h"
#include "share/partition_table/ob_partition_location_cache.h"
#include "share/schema/ob_column_schema.h"
#include "share/schema/ob_table_schema.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/optimizer/ob_logical_operator.h"
#include "sql/optimizer/ob_log_plan.h"
#include "sql/optimizer/ob_opt_est_sel.h"
#include "sql/engine/table/ob_table_scan.h"
#include "sql/ob_sql_mock_schema_utils.h"
#include "lib/ob_name_def.h"
#include "common/ob_smart_call.h"
#include "sql/engine/expr/ob_expr_version.h"
#include "sql/rewrite/ob_transform_utils.h"

using namespace oceanbase;
using namespace sql;
using namespace oceanbase::common;
using namespace oceanbase::share::schema;
using namespace oceanbase::share;

int ObOptimizerUtil::is_prefix_ordering(const ObIArray<OrderItem>& pre, const ObIArray<OrderItem>& full,
    const EqualSets& equal_sets, const ObIArray<ObRawExpr*>& const_exprs, bool& is_prefix)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 5> pre_exprs;
  ObSEArray<ObOrderDirection, 5> pre_directions;
  if (OB_FAIL(split_expr_direction(pre, pre_exprs, pre_directions))) {
    LOG_WARN("Failed to construct direction", K(ret));
  } else if (OB_FAIL(is_prefix_ordering(pre_exprs, full, equal_sets, const_exprs, is_prefix, &pre_directions))) {
    LOG_WARN("Failed to check is prefix ordering", K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObOptimizerUtil::is_prefix_ordering(const ObIArray<ObRawExpr*>& pre, const ObIArray<OrderItem>& full,
    const EqualSets& equal_sets, const ObIArray<ObRawExpr*>& const_exprs, bool& is_prefix,
    const ObIArray<ObOrderDirection>* pre_directions)
{
  int ret = OB_SUCCESS;
  is_prefix = false;
  int64_t left_match_count = 0;
  int64_t right_match_count = 0;
  if (OB_FAIL(find_common_prefix_ordering(
          full, pre, equal_sets, const_exprs, left_match_count, right_match_count, pre_directions))) {
    LOG_WARN("failed to find common prefix", K(ret));
  } else if (right_match_count == pre.count()) {
    is_prefix = true;
  } else { /*do nothing*/
  }
  return ret;
}

int ObOptimizerUtil::is_prefix_ordering(const ObIArray<OrderItem>& pre, const ObIArray<ObRawExpr*>& full,
    const EqualSets& equal_sets, const ObIArray<ObRawExpr*>& const_exprs, bool& is_prefix,
    const ObIArray<ObOrderDirection>* full_directions)
{
  int ret = OB_SUCCESS;
  is_prefix = false;
  int64_t left_match_count = 0;
  int64_t right_match_count = 0;
  if (OB_FAIL(find_common_prefix_ordering(
          pre, full, equal_sets, const_exprs, left_match_count, right_match_count, full_directions))) {
    LOG_WARN("failed to find common prefix", K(ret));
  } else if (left_match_count == pre.count()) {
    is_prefix = true;
  } else { /*do nothing*/
  }
  return ret;
}

int ObOptimizerUtil::find_common_prefix_ordering(const ObIArray<OrderItem>& left_side,
    const ObIArray<OrderItem>& right_side, const EqualSets& equal_sets, const ObIArray<ObRawExpr*>& const_exprs,
    int64_t& left_match_count, int64_t& right_match_count)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObOrderDirection, 6> directions;
  ObSEArray<ObRawExpr*, 6> order_by_exprs;
  if (OB_FAIL(split_expr_direction(right_side, order_by_exprs, directions))) {
    LOG_WARN("failed to split expr and directions", K(ret));
  } else if (OB_FAIL(find_common_prefix_ordering(left_side,
                 order_by_exprs,
                 equal_sets,
                 const_exprs,
                 left_match_count,
                 right_match_count,
                 &directions))) {
    LOG_WARN("failed to find common prefix ordering", K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObOptimizerUtil::find_common_prefix_ordering(const ObIArray<OrderItem>& left_side,
    const ObIArray<ObRawExpr*>& right_side, const EqualSets& equal_sets, const ObIArray<ObRawExpr*>& const_exprs,
    int64_t& left_match_count, int64_t& right_match_count, const ObIArray<ObOrderDirection>* right_directions)
{
  int ret = OB_SUCCESS;
  left_match_count = 0;
  right_match_count = 0;
  bool left_is_const = false;
  bool right_is_const = false;
  int64_t N = left_side.count();
  int64_t M = right_side.count();
  int64_t i = 0;
  int64_t j = 0;
  bool is_prefix = true;
  while (OB_SUCCESS == ret && is_prefix && i < N && j < M) {
    if ((NULL != right_directions ? left_side.at(i).order_type_ != right_directions->at(j) : false) ||
        (!is_expr_equivalent(right_side.at(j), left_side.at(i).expr_, equal_sets))) {
      if (OB_FAIL(is_const_or_equivalent_expr(left_side, equal_sets, const_exprs, i, left_is_const))) {
        LOG_WARN("failed to check is const or equivalent exprs", K(ret));
      } else if (OB_FAIL(is_const_or_equivalent_expr(right_side, equal_sets, const_exprs, j, right_is_const))) {
        LOG_WARN("failed to check is const or equivalent exprs", K(ret));
      } else if (!left_is_const && !right_is_const) {
        is_prefix = false;
      } else {
        if (left_is_const) {
          ++i;
        }
        if (right_is_const) {
          ++j;
        }
      }
    } else {
      ++i;
      ++j;
    }
  }
  for (; OB_SUCC(ret) && is_prefix && i < N; i++) {
    if (OB_FAIL(is_const_or_equivalent_expr(left_side, equal_sets, const_exprs, i, left_is_const))) {
      LOG_WARN("failed to check is const or equivlent expr", K(ret));
    } else if (!left_is_const) {
      break;
    }
  }
  for (; OB_SUCC(ret) && is_prefix && j < M; j++) {
    if (OB_FAIL(is_const_or_equivalent_expr(right_side, equal_sets, const_exprs, j, right_is_const))) {
      LOG_WARN("failed to check is const or equivlent expr", K(ret));
    } else if (!right_is_const) {
      break;
    }
  }
  if (OB_SUCC(ret)) {
    left_match_count = i;
    right_match_count = j;
    LOG_TRACE("succeed to find common prefix", K(left_side), K(right_side), K(left_match_count), K(right_match_count));
  }
  return ret;
}

int ObOptimizerUtil::is_const_or_equivalent_expr(const common::ObIArray<ObRawExpr*>& exprs, const EqualSets& equal_sets,
    const common::ObIArray<ObRawExpr*>& const_exprs, const int64_t& pos, bool& is_const)
{
  int ret = OB_SUCCESS;
  is_const = false;
  if (OB_UNLIKELY(pos < 0 || pos >= exprs.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid array pos", K(pos), K(exprs.count()), K(ret));
  } else if (OB_ISNULL(exprs.at(pos))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexected null", K(ret));
  } else if (OB_FAIL(is_const_expr(exprs.at(pos), equal_sets, const_exprs, is_const))) {
    LOG_WARN("failed to check const expr", K(exprs.at(pos)), K(ret));
  } else if (is_const) {
    /*do nothing*/
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !is_const && i < pos; i++) {
      if (is_expr_equivalent(exprs.at(i), exprs.at(pos), equal_sets)) {
        is_const = true;
      }
    }
  }
  return ret;
}

int ObOptimizerUtil::is_const_or_equivalent_expr(const common::ObIArray<OrderItem>& order_items,
    const EqualSets& equal_sets, const common::ObIArray<ObRawExpr*>& const_exprs, const int64_t& pos, bool& is_const)
{
  int ret = OB_SUCCESS;
  is_const = false;
  if (OB_UNLIKELY(pos < 0 || pos >= order_items.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid array pos", K(pos), K(order_items.count()), K(ret));
  } else if (OB_ISNULL(order_items.at(pos).expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexected null", K(ret));
  } else if (OB_FAIL(is_const_expr(order_items.at(pos).expr_, equal_sets, const_exprs, is_const))) {
    LOG_WARN("failed to check const expr", K(order_items.at(pos).expr_), K(ret));
  } else if (is_const) {
    /*do nothing*/
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !is_const && i < pos; i++) {
      if (is_expr_equivalent(order_items.at(i).expr_, order_items.at(pos).expr_, equal_sets)) {
        is_const = true;
      }
    }
  }
  return ret;
}

int ObOptimizerUtil::prefix_subset_ids(const ObIArray<uint64_t>& pre, const ObIArray<uint64_t>& full, bool& is_prefix)
{
  int ret = OB_SUCCESS;
  is_prefix = true;
  int64_t N = pre.count();
  int64_t M = full.count();
  if (N > M) {
    is_prefix = false;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && is_prefix && i < N; ++i) {
      bool find = false;
      for (int64_t j = 0; OB_SUCC(ret) && is_prefix && !find && j < N; ++j) {
        if (pre.at(i) == full.at(j)) {
          find = true;
        } else {
          is_prefix = false;
        }
      }
    }
  }
  return ret;
}

int ObOptimizerUtil::prefix_subset_exprs(const ObIArray<ObRawExpr*>& exprs, const ObIArray<ObRawExpr*>& ordering,
    const EqualSets& equal_sets, const ObIArray<ObRawExpr*>& const_exprs, bool& is_covered, int64_t* match_count)
{
  int ret = OB_SUCCESS;
  is_covered = false;
  bool is_break = false;
  int64_t covered_count = 0;
  ObBitSet<64> expr_idxs;
  int64_t key_covered_count = 0;
  for (int64_t i = 0; OB_SUCC(ret) && !is_break && i < ordering.count(); ++i) {
    bool is_found = false;
    for (int64_t j = 0; OB_SUCC(ret) && !is_found && j < exprs.count(); ++j) {
      if (expr_idxs.has_member(j)) {
      } else if (is_expr_equivalent(ordering.at(i), exprs.at(j), equal_sets)) {
        is_found = true;
        if (OB_FAIL(expr_idxs.add_member(j))) {
          LOG_WARN("add expr_idxs member", K(ret));
        }
      }
    }
    if (OB_SUCC(ret) && is_found) {
      ++covered_count;
      key_covered_count = i + 1;
    }
    if (OB_SUCC(ret) && !is_found) {
      bool is_const = false;
      if (OB_FAIL(is_const_expr(ordering.at(i), equal_sets, const_exprs, is_const))) {
        LOG_WARN("check expr is const expr failed", K(ret));
      } else if (!is_const) {
        is_break = true;
      }
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    if (expr_idxs.has_member(i)) {
      for (int64_t j = 0; OB_SUCC(ret) && j < exprs.count(); ++j) {
        if (expr_idxs.has_member(j)) {
        } else if (is_expr_equivalent(exprs.at(i), exprs.at(j), equal_sets)) {
          if (OB_FAIL(expr_idxs.add_member(j))) {
            LOG_WARN("add expr_idxs member", K(ret));
          } else {
            ++covered_count;
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (covered_count == exprs.count()) {
      is_covered = true;
    }
    if (NULL != match_count) {
      *match_count = key_covered_count;
    }
  }
  return ret;
}

int ObOptimizerUtil::adjust_exprs_by_ordering(ObIArray<ObRawExpr*>& exprs, const ObIArray<OrderItem>& ordering,
    const EqualSets& equal_sets, const ObIArray<ObRawExpr*>& const_exprs, bool& ordering_used,
    ObIArray<ObOrderDirection>& directions, ObIArray<int64_t>* match_map /*=NULL*/)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 8> adjusted_exprs;
  ObSEArray<ObOrderDirection, 8> order_types;
  ObSEArray<int64_t, 8> expr_map;
  ObBitSet<64> expr_idxs;
  for (int64_t i = 0; OB_SUCC(ret) && i < ordering.count(); ++i) {
    const OrderItem& sort_key = ordering.at(i);
    bool is_found = false;
    for (int64_t j = 0; OB_SUCC(ret) && !is_found && j < exprs.count(); ++j) {
      if (expr_idxs.has_member(j)) {
      } else if (is_expr_equivalent(sort_key.expr_, exprs.at(j), equal_sets)) {
        is_found = true;
        ordering_used = true;
        if (OB_FAIL(adjusted_exprs.push_back(exprs.at(j)))) {
          LOG_WARN("store ordered expr failed", K(ret), K(i), K(j));
        } else if (OB_FAIL(expr_map.push_back(j))) {
          LOG_WARN("failed to push back expr index", K(ret));
        } else if (OB_FAIL(order_types.push_back(sort_key.order_type_))) {
          LOG_WARN("failed to push back order type");
        } else if (OB_FAIL(expr_idxs.add_member(j))) {
          LOG_WARN("add expr idxs member failed", K(ret), K(j));
        }
      }
    }
    if (OB_SUCC(ret) && !is_found) {
      bool is_const = false;
      if (OB_FAIL(is_const_or_equivalent_expr(ordering, equal_sets, const_exprs, i, is_const))) {
        LOG_WARN("failed to check is const or equivalent expr", K(ret));
      } else if (is_const) {
        /*do nothing*/
      } else {
        break;
      }
    }
  }
  for (int64_t j = 0; OB_SUCC(ret) && j < exprs.count(); ++j) {
    if (expr_idxs.has_member(j)) {
    } else if (OB_FAIL(adjusted_exprs.push_back(exprs.at(j)))) {
      LOG_WARN("store ordered expr failed", K(ret), K(j));
    } else if (OB_FAIL(expr_map.push_back(j))) {
      LOG_WARN("failed to push back expr index", K(ret));
    } else if (OB_FAIL(order_types.push_back(directions.at(j)))) {
      LOG_WARN("failed to push back order type", K(ret));
    } else if (OB_FAIL(expr_idxs.add_member(j))) {
      LOG_WARN("add expr idxs member failed", K(ret), K(j));
    }
  }
  if (OB_SUCC(ret)) {
    if (adjusted_exprs.count() != exprs.count() || order_types.count() != exprs.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("exprs don't covered completely by ordering",
          K(adjusted_exprs.count()),
          K(exprs.count()),
          K(order_types.count()));
    } else {
      exprs.reuse();
      if (OB_FAIL(exprs.assign(adjusted_exprs))) {
        LOG_WARN("assign adjusted exprs failed", K(ret));
      } else if (OB_FAIL(directions.assign(order_types))) {
        LOG_WARN("failed to assign order types", K(ret));
      } else if (match_map != NULL && OB_FAIL(match_map->assign(expr_map))) {
        LOG_WARN("failed to assign expr indexs", K(ret));
      }
    }
  }
  return ret;
}

int ObOptimizerUtil::adjust_exprs_by_mapping(const common::ObIArray<ObRawExpr*>& exprs,
    const common::ObIArray<int64_t>& match_map, common::ObIArray<ObRawExpr*>& adjusted_exprs)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> tmp_array;
  if (OB_UNLIKELY(exprs.count() != match_map.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("size does not match", K(ret), K(exprs.count()), K(match_map.count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < match_map.count(); ++i) {
    int64_t index = match_map.at(i);
    if (index < 0 || index >= exprs.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid index", K(ret), K(index));
    } else if (OB_FAIL(tmp_array.push_back(exprs.at(index)))) {
      LOG_WARN("failed to add expr", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(adjusted_exprs.assign(tmp_array))) {
      LOG_WARN("failed to assign expr array", K(ret));
    }
  }
  return ret;
}

bool ObOptimizerUtil::is_same_ordering(const common::ObIArray<OrderItem>& ordering1,
    const common::ObIArray<OrderItem>& ordering2, const EqualSets& equal_sets)
{
  bool is_same = true;
  int64_t N = ordering1.count();
  int64_t M = ordering2.count();
  if (N != M) {
    is_same = false;
  } else {
    for (int64_t i = 0; is_same && i < N; ++i) {
      if (!(ordering1.at(i).order_type_ == ordering2.at(i).order_type_ &&
              is_expr_equivalent(ordering1.at(i).expr_, ordering2.at(i).expr_, equal_sets) &&
              is_expr_equivalent(ordering2.at(i).expr_, ordering1.at(i).expr_, equal_sets))) {
        is_same = false;
      }
    }
  }
  return is_same;
}

bool ObOptimizerUtil::is_expr_equivalent(const ObRawExpr* from, const ObRawExpr* to, const EqualSets& equal_sets)
{
  bool found = false;
  bool is_consistent = false;
  if (OB_ISNULL(from) || OB_ISNULL(to)) {
    // do nothing
  } else if (from == to) {
    found = true;
  } else if (!from->is_generalized_column() && from->same_as(*to)) {
    found = true;
  } else if (ObRawExprUtils::expr_is_order_consistent(from, to, is_consistent) != OB_SUCCESS) {
    LOG_WARN("check expr is order consist ent failed");
  } else if (is_consistent) {
    int64_t N = equal_sets.count();
    for (int64_t i = 0; !found && i < N; ++i) {
      if (OB_ISNULL(equal_sets.at(i))) {
        LOG_WARN("get null equal set");
      } else if (find_equal_expr(*equal_sets.at(i), from) && find_equal_expr(*equal_sets.at(i), to)) {
        found = true;
      }
    }
  }
  return found;
}

int ObOptimizerUtil::extract_target_level_query_ref_expr(ObIArray<ObQueryRefRawExpr*>& exprs, const int64_t level,
    const ObIArray<ObQueryRefRawExpr*>& ignore_exprs, ObIArray<ObRawExpr*>& subqueries)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    ret = SMART_CALL(extract_target_level_query_ref_expr(exprs.at(i), level, ignore_exprs, subqueries));
  }
  return ret;
}

int ObOptimizerUtil::extract_target_level_query_ref_expr(ObIArray<ObRawExpr*>& exprs, const int64_t level,
    const ObIArray<ObQueryRefRawExpr*>& ignore_exprs, ObIArray<ObRawExpr*>& subqueries)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    ret = SMART_CALL(extract_target_level_query_ref_expr(exprs.at(i), level, ignore_exprs, subqueries));
  }
  return ret;
}

int ObOptimizerUtil::extract_target_level_query_ref_expr(ObRawExpr* expr, const int64_t level,
    const ObIArray<ObQueryRefRawExpr*>& ignore_exprs, ObIArray<ObRawExpr*>& subqueries)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (!expr->has_flag(CNT_SUB_QUERY)) {
    /*do nothing*/
  } else if (!expr->is_query_ref_expr()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      ret = SMART_CALL(extract_target_level_query_ref_expr(expr->get_param_expr(i), level, ignore_exprs, subqueries));
    }
  } else if (level == expr->get_expr_level() && !find_item(ignore_exprs, expr)) {
    ret = add_var_to_array_no_dup(subqueries, expr);
  } else {
    ObSelectStmt* ref_query = static_cast<ObQueryRefRawExpr*>(expr)->get_ref_stmt();
    ObSEArray<ObRawExpr*, 4> exprs;
    if (OB_ISNULL(ref_query)) {
      /*do nothing*/
    } else if (OB_FAIL(ref_query->get_relation_exprs(exprs))) {
      LOG_WARN("failed to get relation exprs", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
      ret = SMART_CALL(extract_target_level_query_ref_expr(exprs.at(i), level, ignore_exprs, subqueries));
    }
  }
  return ret;
}

bool ObOptimizerUtil::is_sub_expr(const ObRawExpr* sub_expr, const ObRawExpr* expr)
{
  bool found = false;
  if (NULL == sub_expr || NULL == expr) {
    /* do nothing */
  } else {
    if (sub_expr == expr) {
      found = true;
    } else if (ObRawExprUtils::is_same_raw_expr(sub_expr, expr)) {
      found = true;
    } else { /* do nothing. */
    }
    for (int64_t i = 0; !found && i < expr->get_param_count(); ++i) {
      found = is_sub_expr(sub_expr, expr->get_param_expr(i));
    }
  }
  return found;
}

bool ObOptimizerUtil::is_sub_expr(const ObRawExpr* sub_expr, const ObIArray<ObRawExpr*>& exprs)
{
  bool found = false;
  for (int64_t i = 0; !found && i < exprs.count(); i++) {
    found = is_sub_expr(sub_expr, exprs.at(i));
  }
  return found;
}

bool ObOptimizerUtil::is_sub_expr(const ObRawExpr* sub_expr, ObRawExpr*& expr, ObRawExpr**& addr_matched_expr)
{
  bool found = false;
  if (NULL == sub_expr || NULL == expr) {
    /* do nothing */
  } else if (sub_expr == expr) {
    found = true;
    addr_matched_expr = &expr;
  } else {
    for (int64_t i = 0; !found && i < expr->get_param_count(); ++i) {
      found = is_sub_expr(sub_expr, expr->get_param_expr(i), addr_matched_expr);
    }
  }
  return found;
}

bool ObOptimizerUtil::is_point_based_sub_expr(const ObRawExpr* sub_expr, const ObRawExpr* expr)
{
  bool found = false;
  if (NULL == sub_expr || NULL == expr) {
    /* do nothing */
  } else if (sub_expr == expr) {
    found = true;
  } else {
    for (int64_t i = 0; !found && i < expr->get_param_count(); ++i) {
      found = is_point_based_sub_expr(sub_expr, expr->get_param_expr(i));
    }
  }
  return found;
}

int ObOptimizerUtil::is_const_expr(
    const ObRawExpr* expr, const EqualSets& equal_sets, const ObIArray<ObRawExpr*>& const_exprs, bool& is_const)
{
  int ret = OB_SUCCESS;
  is_const = false;
  bool is_consistent = false;
  if (OB_ISNULL(expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("expr passed in should not be NULL", K(expr), K(ret));
  } else {
    ObArray<const EqualSet*> found_sets;
    if (OB_FAIL(find_equal_set(expr, equal_sets, found_sets))) {
      LOG_WARN("find equal set failed", K(ret));
    } else if (found_sets.count() <= 0) {
      if (OB_FAIL(is_const_expr(expr, const_exprs, is_const))) {
        LOG_WARN("failed to check if is const expr", K(expr), K(ret));
      }
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && !is_const && i < found_sets.count(); ++i) {
        const EqualSet* equal_set = found_sets.at(i);
        if (OB_ISNULL(equal_set)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("equal set is null");
        }
        for (int64_t j = 0; OB_SUCC(ret) && !is_const && j < equal_set->count(); ++j) {
          const ObRawExpr* cur_expr = equal_set->at(j);
          if (OB_ISNULL(cur_expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("expr passed in should not be NULL", K(j), K(ret));
          } else if (OB_FAIL(ObRawExprUtils::expr_is_order_consistent(cur_expr, expr, is_consistent))) {
            LOG_WARN("check expr is order consistent failed", K(ret));
          } else if (is_consistent && OB_FAIL(is_const_expr(cur_expr, const_exprs, is_const))) {
            LOG_WARN("failed to check const expr", K(cur_expr), K(ret));
          } else { /*do nothing*/
          }
        }
      }
    }
  }
  return ret;
}

bool ObOptimizerUtil::is_const_expr(const ObRawExpr* expr, const int64_t stmt_level)
{
  bool bret = false;
  if (OB_ISNULL(expr)) {
    // do nothing
  } else if (expr->has_const_or_const_expr_flag()) {
    bret = true;
  } else if (stmt_level < 0) {
    // do nothing
  } else if (!expr->get_expr_levels().has_member(stmt_level) && !expr->has_flag(CNT_RAND_FUNC) &&
             !expr->has_flag(CNT_PSEUDO_COLUMN)) {
    bret = true;
  }
  return bret;
}

int ObOptimizerUtil::is_const_expr(const ObRawExpr* expr, const ObIArray<ObRawExpr*>& const_exprs, bool& is_const)
{
  int ret = OB_SUCCESS;
  is_const = false;
  if (OB_ISNULL(expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("expr passed in should not be NULL", K(expr), K(ret));
  } else if (expr->has_const_or_const_expr_flag()) {
    is_const = true;
  } else if (find_item(const_exprs, expr)) {
    is_const = true;
  }
  return ret;
}

int ObOptimizerUtil::compute_const_exprs(
    const ObIArray<ObRawExpr*>& condition_exprs, const int64_t stmt_level, ObIArray<ObRawExpr*>& const_exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < condition_exprs.count(); ++i) {
    ObRawExpr* cur_expr = condition_exprs.at(i);
    if (OB_ISNULL(cur_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr passed in should not be NULL", K(i), K(ret));
    } else if (T_OP_EQ == cur_expr->get_expr_type() || T_OP_IS == cur_expr->get_expr_type()) {
      ObRawExpr* param_1 = cur_expr->get_param_expr(0);
      ObRawExpr* param_2 = cur_expr->get_param_expr(1);
      bool l_is_lossless = false;
      bool r_is_lossless = false;
      if (OB_ISNULL(param_1) || OB_ISNULL(param_2)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null ptr", K(param_1), K(param_2), K(ret));
      } else if (OB_FAIL(is_lossless_column_cast(param_1, l_is_lossless))) {
        LOG_WARN("failed to check is lossless column cast", K(ret));
      } else if (OB_FAIL(is_lossless_column_cast(param_2, r_is_lossless))) {
        LOG_WARN("failed to check is lossless column cast", K(ret));
      } else if (l_is_lossless && FALSE_IT(param_1 = param_1->get_param_expr(0))) {
      } else if (r_is_lossless && FALSE_IT(param_2 = param_2->get_param_expr(0))) {
      } else {
        bool left_const = is_const_expr(param_1, stmt_level);
        bool right_const = is_const_expr(param_2, stmt_level);
        if (left_const || right_const) {
          ObRawExpr* const_expr = left_const ? param_1 : param_2;
          ObRawExpr* common_expr = left_const ? param_2 : param_1;
          if (T_OP_EQ == cur_expr->get_expr_type()) {
            bool is_const = true;
            if (!ob_is_valid_obj_tc(const_expr->get_type_class()) ||
                !ob_is_valid_obj_tc(const_expr->get_type_class())) {
              // (a, a) = (1, 1);
              is_const = false;
            } else if (OB_FAIL(ObObjCaster::is_const_consistent(const_expr->get_result_type().get_obj_meta(),
                           common_expr->get_result_type().get_obj_meta(),
                           cur_expr->get_result_type().get_calc_type(),
                           cur_expr->get_result_type().get_calc_meta().get_collation_type(),
                           is_const))) {
              LOG_WARN("check expr type is strict monotonic failed", K(ret));
            } else if (is_const && OB_FAIL(add_var_to_array_no_dup(const_exprs, common_expr))) {
              LOG_WARN("failed to push back expr", K(ret));
            } else { /*do nothing*/
            }
          } else if (T_BOOL == const_expr->get_expr_type()) {
            // is true/false will not be regarded as const
          } else if (OB_FAIL(add_var_to_array_no_dup(const_exprs, common_expr))) {
            LOG_WARN("failed to push back expr", K(ret));
          } else { /*do nothing*/
          }
        }
      }
    }
  }
  return ret;
}

bool ObOptimizerUtil::overlap_exprs(const ObIArray<ObRawExpr*>& exprs1, const ObIArray<ObRawExpr*>& exprs2)
{
  bool overlap = false;
  int64_t N = exprs2.count();
  for (int64_t i = 0; !overlap && i < N; ++i) {
    if (find_equal_expr(exprs1, exprs2.at(i))) {
      overlap = true;
    }
  }
  return overlap;
}

bool ObOptimizerUtil::subset_exprs(const ObIArray<OrderItem>& sort_keys, const ObIArray<ObRawExpr*>& exprs)
{
  bool subset = true;
  int64_t M = sort_keys.count();
  int64_t N = exprs.count();
  if (M > N) {
    subset = false;
  } else {
    for (int64_t i = 0; subset && i < M; ++i) {
      if (!find_equal_expr(exprs, sort_keys.at(i).expr_)) {
        subset = false;
      }
    }
  }
  return subset;
}

bool ObOptimizerUtil::subset_exprs(
    const ObIArray<ObRawExpr*>& sub_exprs, const ObIArray<ObRawExpr*>& exprs, const EqualSets& equal_sets)
{
  bool subset = true;
  for (int64_t i = 0; subset && i < sub_exprs.count(); ++i) {
    if (!find_equal_expr(exprs, sub_exprs.at(i), equal_sets)) {
      subset = false;
    }
  }
  return subset;
}

bool ObOptimizerUtil::subset_exprs(const ObIArray<ObRawExpr*>& sub_exprs, const ObIArray<ObRawExpr*>& exprs)
{
  bool subset = true;
  for (int64_t i = 0; subset && i < sub_exprs.count(); ++i) {
    if (!find_equal_expr(exprs, sub_exprs.at(i))) {
      subset = false;
    }
  }
  return subset;
}

int ObOptimizerUtil::prefix_subset_exprs(const ObIArray<ObRawExpr*>& sub_exprs, const uint64_t subexpr_prefix_count,
    const ObIArray<ObRawExpr*>& exprs, const uint64_t expr_prefix_count, bool& is_subset)
{
  int ret = OB_SUCCESS;
  is_subset = false;
  if (subexpr_prefix_count > sub_exprs.count() || expr_prefix_count > exprs.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(subexpr_prefix_count), K(sub_exprs.count()), K(expr_prefix_count), K(exprs.count()));
  } else {
    ObSEArray<ObRawExpr*, 4> prefix_keys;
    for (int64_t i = 0; OB_SUCC(ret) && i < expr_prefix_count; ++i) {
      if (OB_FAIL(prefix_keys.push_back(exprs.at(i)))) {
        LOG_WARN("failed to push back exprs", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      is_subset = true;
      for (int64_t i = 0; is_subset && i < subexpr_prefix_count; ++i) {
        if (!find_equal_expr(prefix_keys, sub_exprs.at(i))) {
          is_subset = false;
        }
      }
    }
  }
  return ret;
}

bool ObOptimizerUtil::subset_exprs(
    const ObIArray<OrderItem>& sort_keys, const ObIArray<ObRawExpr*>& exprs2, const EqualSets& equal_sets)
{
  bool subset = true;
  for (int64_t i = 0; subset && i < sort_keys.count(); ++i) {
    if (!find_equal_expr(exprs2, sort_keys.at(i).expr_, equal_sets)) {
      subset = false;
    }
  }
  return subset;
}

int ObOptimizerUtil::intersect_exprs(
    ObIArray<ObRawExpr*>& first, ObIArray<ObRawExpr*>& right, ObIArray<ObRawExpr*>& result)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> tmp;
  for (int64_t i = 0; OB_SUCC(ret) && i < first.count(); ++i) {
    if (!find_equal_expr(right, first.at(i))) {
      // do nothing
    } else if (OB_FAIL(tmp.push_back(first.at(i)))) {
      LOG_WARN("failed to push back first expr", K(ret));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(result.assign(tmp))) {
    LOG_WARN("failed to assign expr array", K(ret));
  }
  return ret;
}

int ObOptimizerUtil::except_exprs(
    ObIArray<ObRawExpr*>& first, ObIArray<ObRawExpr*>& right, ObIArray<ObRawExpr*>& result)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> tmp;
  for (int64_t i = 0; OB_SUCC(ret) && i < first.count(); ++i) {
    if (find_equal_expr(right, first.at(i))) {
      // do nothing
    } else if (OB_FAIL(tmp.push_back(first.at(i)))) {
      LOG_WARN("failed to push back first expr", K(ret));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(result.assign(tmp))) {
    LOG_WARN("failed to assign expr array", K(ret));
  }
  return ret;
}

int ObOptimizerUtil::copy_exprs(
    ObRawExprFactory& expr_factory, const ObIArray<ObRawExpr*>& src, ObIArray<ObRawExpr*>& dst)
{
  int ret = OB_SUCCESS;
  dst.reset();
  int64_t N = src.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
    ObRawExpr* src_expr = src.at(i);
    if (OB_ISNULL(src_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("src_expr is NULL", K(ret));
    } else if (src_expr->has_flag(CNT_COLUMN) || src_expr->has_flag(CNT_AGG) || src_expr->has_flag(CNT_SUB_QUERY)) {
      if (OB_FAIL(dst.push_back(src_expr))) {
        LOG_WARN("push back src expr failed", K(ret));
      }
    } else {
      ObRawExpr* dst_expr = NULL;
      if (OB_FAIL(ObRawExprUtils::copy_expr(expr_factory, src_expr, dst_expr, COPY_REF_DEFAULT))) {
        LOG_WARN("copy expr failed", K(ret));
      } else if (OB_FAIL(dst.push_back(dst_expr))) {
        LOG_WARN("push back src expr failed", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObOptimizerUtil::clone_expr_for_topk(ObRawExprFactory& expr_factory, ObRawExpr* src, ObRawExpr*& dest)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_ISNULL(src)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid src is NULL", K(ret));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("check stack overflow failed", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret));
  } else {
    ObRawExpr::ExprClass expr_class = src->get_expr_class();
    switch (expr_class) {
      case ObRawExpr::EXPR_CONST:
      case ObRawExpr::EXPR_COLUMN_REF:
      case ObRawExpr::EXPR_AGGR: {
        dest = src;
        break;
      }
      case ObRawExpr::EXPR_OPERATOR: {
        ObOpRawExpr* dest_op = NULL;
        if (OB_FAIL(expr_factory.create_raw_expr(src->get_expr_type(), dest_op))) {
          LOG_WARN("failed to allocate raw expr", K(ret));
        } else if (OB_ISNULL(dest = dest_op)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("dest operator expr is null", K(ret));
        } else {
          ObOpRawExpr* src_op = static_cast<ObOpRawExpr*>(src);
          if (OB_ISNULL(src_op)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("casted src_op is NULL", K(ret));
          } else if (OB_FAIL(dest_op->assign(*src_op))) {
            LOG_WARN("failed to assign expr", K(ret));
          } else {
            dest_op->clear_child();
            int64_t count = src_op->get_param_count();
            for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
              ObRawExpr* param_expr = src_op->get_param_expr(i);
              ObRawExpr* new_param_expr = NULL;
              if (OB_FAIL(SMART_CALL(clone_expr_for_topk(expr_factory, param_expr, new_param_expr)))) {
                LOG_WARN("fail to copy_expr", K(ret));
              } else if (OB_FAIL(dest_op->add_param_expr(new_param_expr))) {
                LOG_WARN("fail to add param expr", K(ret));
              } else { /*do nothing*/
              }
            }
          }
        }
        break;
      }
      case ObRawExpr::EXPR_CASE_OPERATOR: {
        ObCaseOpRawExpr* dest_case = NULL;
        if (OB_FAIL(expr_factory.create_raw_expr(src->get_expr_type(), dest_case))) {
          LOG_WARN("failed to allocate raw expr", K(ret));
        } else if (OB_ISNULL(dest = dest_case)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("dest case expr is null", K(ret));
        } else {
          ObCaseOpRawExpr* src_case = static_cast<ObCaseOpRawExpr*>(src);
          if (OB_ISNULL(src_case)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("casted src_case is NULL", K(ret));
          } else if (OB_FAIL(dest_case->assign(*src_case))) {
            LOG_WARN("failed to assign expr", K(ret));
          } else {
            dest_case->clear_child();
            ObRawExpr* origin_arg = src_case->get_arg_param_expr();
            ObRawExpr* origin_default = src_case->get_default_param_expr();
            ObRawExpr* dest_arg = NULL;
            ObRawExpr* dest_default = NULL;
            if (OB_FAIL(SMART_CALL(clone_expr_for_topk(expr_factory, origin_arg, dest_arg)))) {
              LOG_WARN("fail to copy raw expr", K(ret));
            } else if (OB_FAIL(SMART_CALL(clone_expr_for_topk(expr_factory, origin_default, dest_default)))) {
              LOG_WARN("fail to copy raw expr", K(ret));
            } else {
              dest_case->set_arg_param_expr(dest_arg);
              dest_case->set_default_param_expr(dest_default);
              int64_t count = src_case->get_when_expr_size();
              for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
                ObRawExpr* origin_when = src_case->get_when_param_expr(i);
                ObRawExpr* origin_then = src_case->get_then_param_expr(i);
                ObRawExpr* dest_when = NULL;
                ObRawExpr* dest_then = NULL;
                if (OB_FAIL(SMART_CALL(clone_expr_for_topk(expr_factory, origin_when, dest_when)))) {
                  LOG_WARN("fail to copy raw expr", K(ret));
                } else if (OB_FAIL(dest_case->add_when_param_expr(dest_when))) {
                  LOG_WARN("fail to add raw expr", K(ret));
                } else if (OB_FAIL(SMART_CALL(clone_expr_for_topk(expr_factory, origin_then, dest_then)))) {
                  LOG_WARN("fail to copy raw expr", K(ret));
                } else if (OB_FAIL(dest_case->add_then_param_expr(dest_then))) {
                  LOG_WARN("fail to add raw expr", K(ret), K(dest_then));
                } else { /*do nothing*/
                }
              }
            }
          }
        }
        break;
      }
      case ObRawExpr::EXPR_SYS_FUNC: {
        ObSysFunRawExpr* dest_sys = NULL;
        if (OB_FAIL(expr_factory.create_raw_expr(src->get_expr_type(), dest_sys))) {
          LOG_WARN("failed to allocate raw expr", K(ret));
        } else if (OB_ISNULL(dest = dest_sys)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("dest sys func is null", K(ret));
        } else {
          ObSysFunRawExpr* src_sys = static_cast<ObSysFunRawExpr*>(src);
          if (OB_ISNULL(src_sys)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("casted src sys func is null", K(ret));
          } else if (OB_FAIL(dest_sys->assign(*src_sys))) {
            LOG_WARN("failed to assign expr", K(ret));
          } else {
            dest_sys->clear_child();
            int64_t count = src_sys->get_param_count();
            for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
              ObRawExpr* param_expr = src_sys->get_param_expr(i);
              ObRawExpr* new_param_expr = NULL;
              if (OB_FAIL(SMART_CALL(clone_expr_for_topk(expr_factory, param_expr, new_param_expr)))) {
                LOG_WARN("fail to copy raw expr", K(ret));
              } else if (OB_FAIL(dest_sys->add_param_expr(new_param_expr))) {
                LOG_WARN("fail to push raw expr", K(ret));
              } else { /*do nothing*/
              }
            }
          }
        }
        break;
      }
      case ObRawExpr::EXPR_UDF: {
        ret = OB_NOT_SUPPORTED;
        break;
      }
      case ObRawExpr::EXPR_QUERY_REF:
      case ObRawExpr::EXPR_WINDOW:
      case ObRawExpr::EXPR_DOMAIN_INDEX: {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not support expr class", K(expr_class), K(ret));
        break;
      }
      default: {
        // should not reach here
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid expr class", K(expr_class), K(ret));
        break;
      }
    }
  }
  return ret;
}

int ObOptimizerUtil::copy_sort_keys(const ObIArray<ObRawExpr*>& src, ObIArray<OrderItem>& dst)
{
  int ret = OB_SUCCESS;
  dst.reuse();
  int64_t N = src.count();
  OrderItem key;
  for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
    key.expr_ = src.at(i);
    ret = dst.push_back(key);
  }
  return ret;
}

bool ObOptimizerUtil::find_equal_expr(
    const ObIArray<ObRawExpr*>& exprs, const ObRawExpr* expr, const EqualSets& equal_sets, int64_t& idx)
{
  bool found = false;
  int64_t N = exprs.count();
  for (int64_t i = 0; !found && i < N; ++i) {
    if (is_expr_equivalent(exprs.at(i), expr, equal_sets)) {
      found = true;
      idx = i;
    }
  }
  return found;
}

bool ObOptimizerUtil::find_equal_expr(
    const common::ObIArray<ObRawExpr*>& exprs, const ObRawExpr* expr, ObExprParamCheckContext& context, int64_t& idx)
{
  int ret = OB_SUCCESS;
  bool found = false;
  int64_t N = exprs.count();
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
  }
  for (int64_t i = 0; OB_SUCC(ret) && !found && i < N; ++i) {
    if (OB_ISNULL(exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
    } else if (exprs.at(i)->same_as(*expr, &context)) {
      found = true;
      idx = i;
    } else { /*do nothing*/
    }
  }
  return found;
}

int ObOptimizerUtil::find_stmt_expr_direction(const ObDMLStmt& stmt, const common::ObIArray<ObRawExpr*>& exprs,
    const EqualSets& equal_sets, common::ObIArray<ObOrderDirection>& directions)
{
  int ret = OB_SUCCESS;
  ObOrderDirection dir;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
    if (OB_FAIL(find_stmt_expr_direction(stmt, exprs.at(i), equal_sets, dir))) {
      LOG_WARN("failed to find stmt expr direction", K(ret));
    } else if (OB_FAIL(directions.push_back(dir))) {
      LOG_WARN("failed to push back direction", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObOptimizerUtil::find_stmt_expr_direction(
    const ObDMLStmt& stmt, const ObRawExpr* expr, const EqualSets& equal_sets, ObOrderDirection& direction)
{
  int ret = OB_SUCCESS;
  bool is_find = false;
  direction = default_asc_direction();
  if (stmt.is_select_stmt()) {
    const ObSelectStmt& select_stmt = static_cast<const ObSelectStmt&>(stmt);
    // find direction in window function exprs
    for (int64_t i = 0; OB_SUCC(ret) && !is_find && i < select_stmt.get_window_func_count(); ++i) {
      const ObWinFunRawExpr* win_expr = NULL;
      if (OB_ISNULL(win_expr = select_stmt.get_window_func_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("window function is null", K(ret));
      } else {
        is_find = find_expr_direction(win_expr->get_order_items(), expr, equal_sets, direction);
      }
    }
    LOG_TRACE("succeed to check expr direction in window function exprs", K(direction), K(is_find));
  }
  if (OB_SUCC(ret) && !is_find) {
    // find direction in order by exprs
    is_find = find_expr_direction(stmt.get_order_items(), expr, equal_sets, direction);
    LOG_TRACE("succeed to check expr direction in order by exprs", K(direction), K(is_find));
  }
  return ret;
}

bool ObOptimizerUtil::find_expr_direction(
    const ObIArray<OrderItem>& exprs, const ObRawExpr* expr, const EqualSets& equal_sets, ObOrderDirection& direction)
{
  bool found = false;
  int64_t N = exprs.count();
  for (int64_t i = 0; !found && i < N; ++i) {
    if (is_expr_equivalent(exprs.at(i).expr_, expr, equal_sets)) {
      found = true;
      direction = exprs.at(i).order_type_;
    }
  }
  return found;
}

uint64_t ObOptimizerUtil::hash_array(uint64_t seed, const ObIArray<uint64_t>& data_array)
{
  uint64_t hash_value = seed;
  int64_t N = data_array.count();
  for (int64_t i = 0; i < N; ++i) {
    hash_value = common::do_hash(data_array.at(i), hash_value);
  }
  return hash_value;
}

bool ObOptimizerUtil::find_expr(ObIArray<ExprProducer>* ctx, const ObRawExpr& expr)
{
  bool found = false;
  ExprProducer* producer = NULL;
  found = find_expr(ctx, expr, producer);
  return found;
}

bool ObOptimizerUtil::find_expr(ObIArray<ExprProducer>* ctx, const ObRawExpr& expr, ExprProducer*& producer)
{
  bool found = false;
  producer = NULL;
  int64_t N = NULL == ctx ? 0 : ctx->count();
  for (int64_t i = 0; !found && i < N; ++i) {
    /**
     *  We rely on the assumption that we can compare the two expressions by
     *  comparing their physical addresses. This pointer-based comparison is only
     *  possible if it is guaranteed that the duplicate expressions will not be
     *  allocated again.
     */
    if (ctx->at(i).expr_ == &expr) {
      found = true;
      producer = &ctx->at(i);
    }
  }
  return found;
}

int ObOptimizerUtil::classify_equal_conds(
    const ObIArray<ObRawExpr*>& conds, ObIArray<ObRawExpr*>& normal_conds, ObIArray<ObRawExpr*>& nullsafe_conds)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < conds.count(); ++i) {
    if (OB_ISNULL(conds.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("condition is null", K(ret), K(conds.at(i)));
    } else if (conds.at(i)->get_expr_type() == T_OP_EQ || conds.at(i)->get_expr_type() == T_OP_SQ_EQ) {
      if (OB_FAIL(normal_conds.push_back(conds.at(i)))) {
        LOG_WARN("failed to push back normal equal condition", K(ret));
      }
    } else if (conds.at(i)->get_expr_type() == T_OP_NSEQ || conds.at(i)->get_expr_type() == T_OP_SQ_NSEQ) {
      if (OB_FAIL(nullsafe_conds.push_back(conds.at(i)))) {
        LOG_WARN("failed to push back nullsafe equal condition", K(ret));
      }
    }
  }
  return ret;
}

int ObOptimizerUtil::get_equal_keys(const ObIArray<ObRawExpr*>& exprs, const ObRelIds& left_table_sets,
    ObIArray<ObRawExpr*>& left_keys, ObIArray<ObRawExpr*>& right_keys)
{
  int ret = OB_SUCCESS;
  ObRawExpr* temp_expr = NULL;
  ObRawExpr* left_expr = NULL;
  ObRawExpr* right_expr = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    if (OB_ISNULL(temp_expr = exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (!temp_expr->has_flag(IS_JOIN_COND)) {
      /*do nothing*/
    } else if (!temp_expr->get_relation_ids().overlap(left_table_sets)) {
      /*do nothing*/
    } else if (OB_ISNULL(left_expr = temp_expr->get_param_expr(0)) ||
               OB_ISNULL(right_expr = temp_expr->get_param_expr(1))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(left_expr), K(right_expr), K(ret));
    } else {
      if (!left_expr->get_relation_ids().is_subset(left_table_sets)) {
        std::swap(left_expr, right_expr);
      }
      if (OB_FAIL(left_keys.push_back(left_expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else if (OB_FAIL(right_keys.push_back(right_expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

ObRawExpr* ObOptimizerUtil::find_exec_param(
    const ObIArray<std::pair<int64_t, ObRawExpr*>>& params, const int64_t param_num)
{
  ObRawExpr* expr = NULL;
  bool found = false;
  for (int64_t i = 0; !found && i < params.count(); ++i) {
    if (params.at(i).first == param_num) {
      expr = params.at(i).second;
      found = true;
    }
  }
  return expr;
}

int64_t ObOptimizerUtil::find_exec_param(const ObIArray<std::pair<int64_t, ObRawExpr*>>& params, const ObRawExpr* expr)
{
  int64_t param_num = -1;
  bool found = false;
  for (int64_t i = 0; !found && i < params.count(); ++i) {
    if (params.at(i).second == expr) {
      param_num = params.at(i).first;
      found = true;
    }
  }
  return param_num;
}

ObRawExpr* ObOptimizerUtil::find_param_expr(const ObIArray<ObRawExpr*>& exprs, const int64_t param_num)
{
  ObRawExpr* expr = NULL;
  bool found = false;
  for (int64_t i = 0; !found && i < exprs.count(); ++i) {
    if (OB_UNLIKELY(NULL == exprs.at(i))) {
      // NULL expr means not found, just continue
    } else if (exprs.at(i)->has_flag(IS_PARAM) &&
               static_cast<ObConstRawExpr*>(exprs.at(i))->get_value().get_unknown() == param_num) {
      expr = exprs.at(i);
      found = true;
    }
  }
  return expr;
}

int ObOptimizerUtil::extract_params(ObRawExpr* expr, ObIArray<ObRawExpr*>& params)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_ISNULL(expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("expr passed in is NULL", K(ret));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("check stack overflow failed", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret));
  } else {
    ObRawExpr::ExprClass expr_class = expr->get_expr_class();
    switch (expr_class) {
      case ObRawExpr::EXPR_CONST: {
        if (expr->has_flag(IS_PARAM)) {
          ret = params.push_back(expr);
        } else { /*do nothing*/
        }
        break;
      }
      case ObRawExpr::EXPR_SET_OP:
      case ObRawExpr::EXPR_QUERY_REF:
      case ObRawExpr::EXPR_COLUMN_REF: {
        break;
      }
      case ObRawExpr::EXPR_OPERATOR:       // fallthrough
      case ObRawExpr::EXPR_CASE_OPERATOR:  // fallthrough
      case ObRawExpr::EXPR_AGGR:           // fallthrough
      case ObRawExpr::EXPR_SYS_FUNC:
      case ObRawExpr::EXPR_UDF: {
        for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
          ret = SMART_CALL(extract_params(expr->get_param_expr(i), params));
        }
        break;
      }

      case ObRawExpr::EXPR_INVALID_CLASS:
      default:
        // should not reach here
        break;
    }
  }
  return ret;
}

int ObOptimizerUtil::extract_params(common::ObIArray<ObRawExpr*>& exprs, common::ObIArray<ObRawExpr*>& params)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    if (OB_FAIL(extract_params(exprs.at(i), params))) {
      LOG_WARN("failed to extract params", K(ret));
    }
  }
  return ret;
}

int ObOptimizerUtil::extract_equal_exec_params(const ObIArray<ObRawExpr*>& exprs,
    const ObIArray<std::pair<int64_t, ObRawExpr*>>& params, ObIArray<ObRawExpr*>& left_key,
    ObIArray<ObRawExpr*>& right_key)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    ObRawExpr* cur_expr = exprs.at(i);
    if (OB_ISNULL(cur_expr)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("expr passed in is NULL", K(ret));
    } else if (T_OP_EQ == cur_expr->get_expr_type() || T_OP_NSEQ == cur_expr->get_expr_type()) {
      ObRawExpr* expr = NULL;
      if (OB_ISNULL(cur_expr->get_param_expr(0)) || OB_ISNULL(cur_expr->get_param_expr(1))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Invalid op expr", K(*cur_expr), K(ret));
      } else if (cur_expr->get_param_expr(0)->has_flag(IS_PARAM)) {
        int64_t param_value = static_cast<ObConstRawExpr*>(cur_expr->get_param_expr(0))->get_value().get_unknown();
        if (NULL == (expr = find_exec_param(params, param_value))) {

        } else if (OB_FAIL(left_key.push_back(expr))) {
          LOG_WARN("push back error", K(ret));
        } else if (OB_FAIL(right_key.push_back(cur_expr->get_param_expr(1)))) {
          LOG_WARN("push back error", K(ret));
        } else { /*do nothing*/
        }
      } else if (cur_expr->get_param_expr(1)->has_flag(IS_PARAM)) {
        int64_t param_value = static_cast<ObConstRawExpr*>(cur_expr->get_param_expr(1))->get_value().get_unknown();
        if (NULL == (expr = find_exec_param(params, param_value))) {
        } else if (OB_FAIL(left_key.push_back(expr))) {
          LOG_WARN("push back error", K(ret));
        } else if (OB_FAIL(right_key.push_back(cur_expr->get_param_expr(0)))) {
          LOG_WARN("push back error", K(ret));
        } else { /*do nothing*/
        }
      } else { /*do nothing*/
      }
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObOptimizerUtil::add_col_ids_to_set(const ObIArray<ObRawExpr*>& exprs, ObBitSet<>& bitset)
{
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    if (OB_FAIL(add_col_ids_to_set(exprs.at(i), bitset, true))) {
      LOG_WARN("Failed to extract col expr", K(ret));
    }
  }
  return ret;
}

int ObOptimizerUtil::add_col_ids_to_set(
    const ObRawExpr* expr, ObBitSet<>& bitset, bool check_single_col, bool restrict_table_id, uint64_t target_table_id)
{
  int ret = OB_SUCCESS;
  ObArray<ObRawExpr*> cur_col_refs;
  if (OB_FAIL(ObRawExprUtils::extract_column_exprs(const_cast<ObRawExpr*>(expr), cur_col_refs))) {
    LOG_WARN("Failed to extract column expr", K(ret));
  } else if (check_single_col && cur_col_refs.count() != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Expect one col ref.", "count", cur_col_refs.count());
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < cur_col_refs.count(); ++i) {
      ObRawExpr* cur_expr = cur_col_refs.at(i);
      if (OB_ISNULL(cur_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL pointer error", K(cur_expr), K(i), K(ret));
      } else if (cur_expr->get_expr_type() != T_REF_COLUMN) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Expr type error", K(ret));
      } else {
        ObColumnRefRawExpr* col_ref = reinterpret_cast<ObColumnRefRawExpr*>(cur_expr);
        if (!restrict_table_id || (restrict_table_id && col_ref->get_table_id() == target_table_id)) {
          if (OB_FAIL(bitset.add_member(static_cast<int32_t>(col_ref->get_column_id())))) {
            LOG_WARN("failed to add member", K(bitset), K(col_ref->get_column_id()), K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObOptimizerUtil::check_if_column_ids_covered(const ObIArray<ObRawExpr*>& exprs, const ObBitSet<>& colset,
    bool restrict_table_id, uint64_t target_table_id, ObIArray<bool>& result)
{
  int ret = OB_SUCCESS;
  result.reset();
  for (int i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    bool single_result = false;
    if (OB_FAIL(check_if_column_ids_covered(exprs.at(i), colset, restrict_table_id, target_table_id, single_result))) {
      LOG_WARN("Failed to check expr", K(ret));
    } else {
      ret = result.push_back(single_result);
    }
  }
  return ret;
}

int ObOptimizerUtil::check_if_column_ids_covered(
    const ObRawExpr* exprs, const ObBitSet<>& colset, bool restrict_table_id, uint64_t target_table_id, bool& result)
{
  int ret = OB_SUCCESS;
  result = false;
  ObBitSet<> colSetOfThisExpr;
  if (OB_FAIL(add_col_ids_to_set(exprs, colSetOfThisExpr, false, restrict_table_id, target_table_id))) {
    LOG_WARN("Failed to extract col expr", K(ret));
  } else {
    if (colSetOfThisExpr.is_subset(colset)) {
      result = true;
    } else {
      result = false;
    }
  }
  return ret;
}

int ObOptimizerUtil::generate_rowkey_column_items(ObDMLStmt* stmt, ObRawExprFactory& expr_factory, uint64_t table_id,
    const share::schema::ObTableSchema& index_table_schema, common::ObIArray<ColumnItem>& index_columns)
{
  int ret = OB_SUCCESS;
  // get all the index keys
  const ObRowkeyInfo* rowkey_info = NULL;
  const ObColumnSchemaV2* column_schema = NULL;
  uint64_t column_id = OB_INVALID_ID;
  if (index_table_schema.is_index_table() && is_virtual_table(index_table_schema.get_data_table_id()) &&
      !index_table_schema.is_ordered()) {
    // for virtual table and its hash index
    rowkey_info = &index_table_schema.get_index_info();
  } else {
    rowkey_info = &index_table_schema.get_rowkey_info();
  }
  // if rowid_index is used ,range_column should just be [rowid]
  bool found_rowid_col = false;
  for (int col_idx = 0; !found_rowid_col && OB_SUCC(ret) && col_idx < rowkey_info->get_size(); ++col_idx) {
    if (OB_FAIL(rowkey_info->get_column_id(col_idx, column_id))) {
      LOG_WARN("Failed to get column id", K(ret));
    } else if (OB_ISNULL(column_schema = (index_table_schema.get_column_schema(column_id)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get column schema", K(column_id), K(ret));
    } else {
      ObColumnRefRawExpr* expr = NULL;
      ColumnItem* col_item = NULL;
      if (NULL != (col_item = stmt->get_column_item_by_id(table_id, column_id))) {
        if (OB_FAIL(index_columns.push_back(*col_item))) {
          LOG_WARN("Failed to add col item", K(ret));
        }
      } else {
        if (OB_FAIL(generate_rowkey_expr(stmt, expr_factory, table_id, *column_schema, expr, &index_columns))) {
          LOG_WARN("failed to get row key expr", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (column_schema->get_column_id() == OB_HIDDEN_LOGICAL_ROWID_COLUMN_ID) {
          found_rowid_col = true;
        }
      }
    }
  }  // for end
  if (OB_SUCC(ret)) {
    LOG_TRACE("get range columns", K(index_columns));
  }
  return ret;
}

int ObOptimizerUtil::generate_rowkey_exprs(ObDMLStmt* stmt, ObOptimizerContext& opt_ctx, const uint64_t table_id,
    const uint64_t ref_table_id, ObIArray<ObRawExpr*>& keys, ObIArray<ObRawExpr*>& ordering)
{
  int ret = OB_SUCCESS;
  const ObTableSchema* table_schema = NULL;
  ObSqlSchemaGuard* schema_guard = NULL;
  if (OB_ISNULL(schema_guard = opt_ctx.get_sql_schema_guard())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(schema_guard), K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(ref_table_id, table_schema))) {
    LOG_WARN("fail to get table schema", K(ref_table_id), K(table_schema), K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index schema should not be null", K(table_schema), K(ret));
  } else if (OB_FAIL(
                 generate_rowkey_exprs(stmt, opt_ctx.get_expr_factory(), table_id, *table_schema, keys, ordering))) {
    LOG_WARN("failed to get rowkeys raw expr", K(table_id), K(ref_table_id), K(ret));
  } else { /*do nothing*/
  }

  return ret;
}

int ObOptimizerUtil::generate_rowkey_exprs(ObDMLStmt* stmt, ObRawExprFactory& expr_factory, uint64_t table_id,
    const ObTableSchema& index_table_schema, ObIArray<ObRawExpr*>& index_keys, ObIArray<ObRawExpr*>& index_ordering)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(stmt), K(ret));
  } else {
    // get all the index keys
    const ObRowkeyInfo& rowkey_info = index_table_schema.get_rowkey_info();
    const ObColumnSchemaV2* column_schema = NULL;
    ObColumnRefRawExpr* expr = NULL;
    for (int col_idx = 0; OB_SUCC(ret) && col_idx < rowkey_info.get_size(); ++col_idx) {
      uint64_t column_id = OB_INVALID_ID;
      if (OB_FAIL(rowkey_info.get_column_id(col_idx, column_id))) {
        LOG_WARN("Failed to get column_id from rowkey_info", K(ret));
      } else if (OB_ISNULL(column_schema = (index_table_schema.get_column_schema(column_id)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get column schema", K(column_id), K(ret));
      } else {
        ObRawExpr* raw_expr = NULL;
        //!!!Dangerous. Different Unique indexes' hidden columns have the same column ids.
        // Now, we haven't thought the trouble this may cause. But, if we do multi-index scan
        // or other opt need to use different indexes in one logical plan, remember this problem.
        if (NULL != (raw_expr = stmt->get_column_expr_by_id(table_id, column_id))) {
          expr = static_cast<ObColumnRefRawExpr*>(raw_expr);
        } else if (OB_FAIL(generate_rowkey_expr(stmt, expr_factory, table_id, *column_schema, expr))) {
          LOG_WARN("failed to get row key expr", K(ret));
        } else { /*do nothing*/
        }

        if (OB_SUCC(ret)) {
          if (OB_FAIL(index_keys.push_back(expr))) {
            LOG_WARN("failed to add row key expr", K(ret));
          } else if (index_table_schema.is_ordered() && OB_FAIL(index_ordering.push_back(expr))) {
            // for virtual table, we have HASH index which offers no ordering on index keys
            LOG_WARN("failed to push back index ordering expr", K(ret));
          } else {
            LOG_TRACE("add index key expr", K(expr), K(column_id));
          }
        }
      }
    }
  }
  return ret;
}

int ObOptimizerUtil::generate_column_exprs(ObDMLStmt* stmt, ObOptimizerContext& opt_ctx, const uint64_t table_id,
    const uint64_t ref_table_id, const ObIArray<uint64_t>& column_ids, ObIArray<ObRawExpr*>& column_exprs)
{
  int ret = OB_SUCCESS;
  const ObTableSchema* table_schema = NULL;
  ObSqlSchemaGuard* schema_guard = NULL;
  if (OB_ISNULL(stmt) || OB_ISNULL(schema_guard = opt_ctx.get_sql_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(stmt), K(schema_guard), K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(ref_table_id, table_schema))) {
    LOG_WARN("failed to get table schema", K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    ObColumnRefRawExpr* column_expr = NULL;
    const ObColumnSchemaV2* column_schema = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < column_ids.count(); i++) {
      if (is_shadow_column(column_ids.at(i))) {
        continue;
      } else if (NULL != (column_expr = stmt->get_column_expr_by_id(table_id, column_ids.at(i)))) {
        /*do nothing*/
      } else if (OB_ISNULL(column_schema = table_schema->get_column_schema(column_ids.at(i)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(column_ids.at(i)), K(ret));
      } else if (OB_FAIL(
                     generate_rowkey_expr(stmt, opt_ctx.get_expr_factory(), table_id, *column_schema, column_expr))) {
        LOG_WARN("failed to generate rowkey expr", K(ret));
      } else { /*do nothing*/
      }

      if (OB_FAIL(ret)) {
        /*do nothing*/
      } else if (OB_ISNULL(column_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(column_exprs.push_back(column_expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObOptimizerUtil::build_range_columns(
    const ObDMLStmt* stmt, ObIArray<ObRawExpr*>& rowkeys, ObIArray<ColumnItem>& range_columns)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("NULL pointer error", K(stmt), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkeys.count(); ++i) {
      if (OB_ISNULL(rowkeys.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("rowkey expr passed in is NULL", K(rowkeys), K(i), K(ret));
      } else if (!rowkeys.at(i)->is_column_ref_expr()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("rowkey expr passed in is not a column", K(*rowkeys.at(i)), K(i), K(ret));
      } else {
        ObColumnRefRawExpr* expr = static_cast<ObColumnRefRawExpr*>(rowkeys.at(i));
        const ColumnItem* column_item = stmt->get_column_item_by_id(expr->get_table_id(), expr->get_column_id());
        //@notice: be careful, range column only use the attribute that table_id, column_id and column type
        // other attributes do not guarantee the correctness
        if (NULL != column_item) {
          ret = range_columns.push_back(*column_item);
        } else {
          ColumnItem new_column_item;
          new_column_item.expr_ = expr;
          new_column_item.table_id_ = expr->get_table_id();
          new_column_item.column_id_ = expr->get_column_id();
          ret = range_columns.push_back(new_column_item);
        }
      }
    }
  }
  return ret;
}

int ObOptimizerUtil::check_filter_before_indexback(uint64_t index_id, ObSchemaGetterGuard* schema_guard,
    const ObIArray<ObRawExpr*>& filters, bool restrict_table_id, uint64_t target_table_id,
    ObIArray<bool>& filter_before_ib)
{
  int ret = OB_SUCCESS;
  const ObTableSchema* index_schema = NULL;
  ObSEArray<ObColDesc, 4> column_ids;
  ObBitSet<> index_column_set;
  if (OB_ISNULL(schema_guard)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get unexpected null", K(schema_guard), K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(index_id, index_schema))) {
    LOG_WARN("failed to get index schema", K(ret));
  } else if (OB_ISNULL(index_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null index schema", K(ret));
  } else if (OB_FAIL(index_schema->get_column_ids(column_ids))) {
    LOG_WARN("failed to get column ids", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < column_ids.count(); i++) {
      if (OB_FAIL(index_column_set.add_member(column_ids.at(i).col_id_))) {
        LOG_WARN("failed to add column ids", K(ret));
      } else { /*do nothing*/
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(check_if_column_ids_covered(
              filters, index_column_set, restrict_table_id, target_table_id, filter_before_ib))) {
        LOG_WARN("Failed to check filter index back");
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObOptimizerUtil::generate_rowkey_expr(ObDMLStmt* stmt, ObRawExprFactory& expr_factory, const uint64_t& table_id,
    const ObColumnSchemaV2& column_schema, ObColumnRefRawExpr*& rowkey, ObIArray<ColumnItem>* column_items)
{
  int ret = OB_SUCCESS;
  /* Now, let's create the raw expr */
  const TableItem* table_item = NULL;
  if (OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument passed in", K(stmt), K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_column_expr(expr_factory, column_schema, rowkey))) {
    LOG_WARN("build column expr failed", K(ret));
  } else if (OB_ISNULL(rowkey)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to create raw expr for dummy output", K(ret));
  } else if (OB_ISNULL(table_item = stmt->get_table_item_by_id(table_id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get table item by id failed", K(table_id));
  } else {
    ColumnItem dummy_col_item;
    rowkey->set_ref_id(table_id, column_schema.get_column_id());
    rowkey->set_column_attr(table_item->get_table_name(), column_schema.get_column_name_str());
    dummy_col_item.table_id_ = rowkey->get_table_id();
    dummy_col_item.column_id_ = rowkey->get_column_id();
    dummy_col_item.base_tid_ = rowkey->get_table_id();
    dummy_col_item.base_cid_ = rowkey->get_column_id();
    dummy_col_item.column_name_ = rowkey->get_column_name();
    dummy_col_item.set_default_value(column_schema.get_cur_default_value());
    dummy_col_item.expr_ = rowkey;
    if (OB_FAIL(stmt->add_column_item(dummy_col_item))) {
      LOG_WARN("add column item to stmt failed", K(ret));
    } else if (FALSE_IT(rowkey->clear_explicited_referece())) {
      /*do nothing*/
    } else if (OB_FAIL(rowkey->formalize(NULL))) {
      LOG_WARN("formalize rowkey failed", K(ret));
    } else if (OB_FAIL(rowkey->pull_relation_id_and_levels(stmt->get_current_level()))) {
      LOG_WARN("failed to pullup relation ids", K(ret));
    } else if (NULL != column_items) {
      if (OB_FAIL(column_items->push_back(dummy_col_item))) {
        LOG_WARN("Failed to add dummy column item", K(ret));
      }
    } else {
    }  // do nothing
  }
  return ret;
}

int ObOptimizerUtil::find_equal_set(
    const ObRawExpr* ordering, const EqualSets& equal_sets, ObIArray<const EqualSet*>& found_sets)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(ordering)) {
    int64_t N = equal_sets.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
      if (OB_ISNULL(equal_sets.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null equal set", K(ret));
      } else if (find_equal_expr(*equal_sets.at(i), ordering)) {
        if (OB_FAIL(found_sets.push_back(equal_sets.at(i)))) {
          LOG_WARN("store found set failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObOptimizerUtil::extract_row_col_idx_for_in(const common::ObIArray<uint64_t>& column_ids,
    const int64_t index_col_pos, const uint64_t table_id, const ObRawExpr& l_expr, const ObRawExpr& r_expr,
    common::ObBitSet<>& col_idxs, int64_t& min_col_idx, bool& is_table_filter)
{
  int ret = OB_SUCCESS;
  common::ObBitSet<> init_col_idxs = col_idxs;
  int64_t init_min_col_idx = min_col_idx;
  bool init_is_table_filter = false;

  bool equal = true;
  for (int64_t i = 0; OB_SUCC(ret) && equal && i < r_expr.get_param_count(); ++i) {
    const ObRawExpr* expr = r_expr.get_param_expr(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr should not be NULL", K(ret));
    } else {
      bool tmp_is_table_filter = false;
      common::ObBitSet<> tmp_col_idxs = col_idxs;
      int64_t tmp_min_col_idx = min_col_idx;
      if (i == 0) {
        if (OB_FAIL(extract_row_col_idx(column_ids,
                index_col_pos,
                table_id,
                l_expr,
                *expr,
                init_col_idxs,
                init_min_col_idx,
                init_is_table_filter))) {
          LOG_WARN("extract_row_col_idx for vector IN-expr failed", K(ret));
        }
      } else if (OB_FAIL(extract_row_col_idx(column_ids,
                     index_col_pos,
                     table_id,
                     l_expr,
                     *expr,
                     tmp_col_idxs,
                     tmp_min_col_idx,
                     tmp_is_table_filter))) {
        LOG_WARN("extract_row_col_idx for vector IN-expr failed", K(ret));
      } else if (!(tmp_col_idxs == init_col_idxs) || tmp_min_col_idx != init_min_col_idx ||
                 tmp_is_table_filter != init_is_table_filter) {
        equal = false;
      } else {
        // do nothing
      }
    }
  }
  if (OB_SUCC(ret) && equal) {
    col_idxs = init_col_idxs;
    min_col_idx = init_min_col_idx;
    is_table_filter = init_is_table_filter;
  }
  return ret;
}

int ObOptimizerUtil::extract_row_col_idx(const ObIArray<uint64_t>& column_ids, const int64_t index_col_pos,
    const uint64_t table_id, const ObRawExpr& l_expr, const ObRawExpr& r_expr, ObBitSet<>& col_idxs,
    int64_t& min_col_idx, bool& is_table_filter)
{
  int ret = OB_SUCCESS;
  if (l_expr.get_param_count() != r_expr.get_param_count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param count is invalid", K(ret), K(l_expr.get_param_count()), K(r_expr.get_param_count()));
  } else if (T_OP_ROW == l_expr.get_expr_type() && T_OP_ROW == r_expr.get_expr_type()) {
    const ObRawExpr* column_expr = NULL;
    bool check_next = true;
    int64_t cur_col_idx = INITED_VALUE;
    int64_t last_idx = INITED_VALUE;
    int64_t i = 0;
    is_table_filter = true;
    for (i = 0; check_next && OB_SUCC(ret) && i < l_expr.get_param_count(); ++i) {
      column_expr = NULL;
      if (OB_ISNULL(l_expr.get_param_expr(i)) || OB_ISNULL(r_expr.get_param_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("param expr i is null", K(ret));
      } else if (l_expr.get_param_expr(i)->is_column_ref_expr() && r_expr.get_param_expr(i)->has_flag(IS_CONST)) {
        column_expr = l_expr.get_param_expr(i);
      } else if (r_expr.get_param_expr(i)->is_column_ref_expr() && l_expr.get_param_expr(i)->has_flag(IS_CONST)) {
        column_expr = r_expr.get_param_expr(i);
      } else {
        check_next = false;
      }
      // add col_idx to bitset
      if (OB_SUCC(ret) && check_next) {
        if (OB_FAIL(extract_column_idx(column_ids, index_col_pos, table_id, column_expr, cur_col_idx, col_idxs))) {
          LOG_WARN("extract column idx failed", K(ret));
        } else if (cur_col_idx < 0 || (last_idx >= 0 && last_idx + 1 != cur_col_idx)) {
          check_next = false;
        } else if (OB_FAIL(col_idxs.add_member(cur_col_idx))) {
          LOG_WARN("failed to add idx to ObBitSet", K(ret), K(cur_col_idx));
        } else {
          last_idx = cur_col_idx;
          if (cur_col_idx < min_col_idx) {
            min_col_idx = cur_col_idx;
          }
          is_table_filter = false;
        }
      }
    }
    if (is_table_filter) {
      if (cur_col_idx > TABLE_RELATED) {
        is_table_filter = false;
      }
    }
  }
  return ret;
}

int ObOptimizerUtil::extract_column_idx(const ObIArray<uint64_t>& column_ids, const int64_t index_col_pos,
    const uint64_t table_id, const ObRawExpr* raw_expr, int64_t& col_idx, ObBitSet<>& col_idxs,
    const bool is_org_filter)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_ISNULL(raw_expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid raw expr", K(raw_expr), K(ret));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("check stack overflow failed", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret));
  } else if (T_REF_COLUMN == raw_expr->get_expr_type()) {
    const ObColumnRefRawExpr* col_ref = static_cast<const ObColumnRefRawExpr*>(raw_expr);
    const uint64_t col_id = col_ref->get_column_id();
    if (col_ref->get_table_id() != table_id) {  // considered as const value
    } else {
      bool found = false;
      const int64_t N = column_ids.count();
      for (int64_t idx = 0; OB_SUCC(ret) && !found && idx < N; ++idx) {
        if (col_id == column_ids.at(idx)) {
          if (idx > index_col_pos) {
            col_idx = INDEX_STORE_RELATED;
          } else if (col_idx >= 0) {
            if (col_idx != idx) {
              col_idx = MUL_INDEX_COL;
            }
          } else if (INITED_VALUE == col_idx) {
            col_idx = is_org_filter ? static_cast<int64_t>(MUL_INDEX_COL) : idx;
          } else { /*do nothing*/
          }
          found = true;
        }
      }
      if (!found) {
        col_idx = TABLE_RELATED;
      }
    }
  } else {
    int64_t N = raw_expr->get_param_count();
    ObItemType type = raw_expr->get_expr_type();
    if (N > 0 && !is_query_range_op(type)) {
      col_idx = MUL_INDEX_COL;  // cannot be prefix filter
    }
    if ((IS_BASIC_CMP_OP(type) || T_OP_IN == type) && N == 2 &&
        (NULL == raw_expr->get_param_expr(0) || NULL == raw_expr->get_param_expr(1))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Invalid raw expr", K(*raw_expr), K(ret));
    } else if ((IS_BASIC_CMP_OP(type) || T_OP_IN == type) && N == 2 &&
               T_OP_ROW == raw_expr->get_param_expr(0)->get_expr_type()) {
      const ObRawExpr& l_expr = *(raw_expr->get_param_expr(0));
      const ObRawExpr& r_expr = *(raw_expr->get_param_expr(1));
      int64_t min_col_idx = column_ids.count();
      bool is_table_filter = false;
      if (T_OP_IN == type) {
        if (OB_FAIL(extract_row_col_idx_for_in(
                column_ids, index_col_pos, table_id, l_expr, r_expr, col_idxs, min_col_idx, is_table_filter))) {
          LOG_WARN("Extract colum idx error", K(ret));
        }
      } else {
        if (OB_FAIL(extract_row_col_idx(
                column_ids, index_col_pos, table_id, l_expr, r_expr, col_idxs, min_col_idx, is_table_filter))) {
          LOG_WARN("Extract colum idx error", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (min_col_idx == column_ids.count()) {
          col_idx = TABLE_RELATED;
        } else if (min_col_idx >= 0) {
          col_idx = min_col_idx;
        } else {
          col_idx = is_table_filter ? TABLE_RELATED : INDEX_STORE_RELATED;
        }
      }
    } else {
      bool is_related = false;
      for (int64_t i = 0; OB_SUCC(ret) && !is_related && i < N; ++i) {
        if (OB_FAIL(SMART_CALL(extract_column_idx(
                column_ids, index_col_pos, table_id, raw_expr->get_param_expr(i), col_idx, col_idxs)))) {
          LOG_WARN("Extrac column idx error", K(ret));
        } else if (TABLE_RELATED == col_idx) {
          is_related = true;
        } else { /*do nothing*/
        }
      }
    }
  }
  return ret;
}

bool ObOptimizerUtil::is_query_range_op(const ObItemType type)
{
  bool b_ret = false;
  if (IS_BASIC_CMP_OP(type) || T_OP_LIKE == type || T_OP_IS == type || T_OP_BTW == type || T_OP_NOT_BTW == type ||
      T_OP_IN == type || T_OP_AND == type || T_OP_OR == type || T_OP_ROW == type) {
    b_ret = true;
  }
  return b_ret;
}

int ObOptimizerUtil::get_child_corresponding_exprs(const ObDMLStmt* upper_stmt, const ObSelectStmt* stmt,
    const ObIArray<ObRawExpr*>& exprs, ObIArray<ObRawExpr*>& corr_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(upper_stmt) || OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid stmt", K(upper_stmt), K(stmt), K(ret));
  } else {
    uint64_t subquery_id = OB_INVALID_ID;
    if (OB_FAIL(get_subquery_id(upper_stmt, stmt, subquery_id))) {
      LOG_WARN("failed to get select exprs", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
        if (OB_ISNULL(exprs.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Invalid expr", K(exprs.at(i)), K(i), K(ret));
        } else if (!exprs.at(i)->is_column_ref_expr()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("not a column expr", K(exprs.at(i)), K(i), K(ret));
        } else if (static_cast<ObColumnRefRawExpr*>(exprs.at(i))->get_table_id() != subquery_id) {
          ret = corr_exprs.push_back(NULL);
        } else {
          int64_t idx = static_cast<int64_t>(
              static_cast<ObColumnRefRawExpr*>(exprs.at(i))->get_column_id() - OB_APP_MIN_COLUMN_ID);
          ret = corr_exprs.push_back(stmt->get_select_item(idx).expr_);
        }
      }
    }
  }
  return ret;
}

int ObOptimizerUtil::get_child_corresponding_exprs(
    const TableItem* table, const ObIArray<ObRawExpr*>& exprs, ObIArray<ObRawExpr*>& corr_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table) || OB_ISNULL(table->ref_query_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid stmt", KPC(table), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
      if (OB_ISNULL(exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Invalid expr", K(exprs.at(i)), K(i), K(ret));
      } else if (!exprs.at(i)->is_column_ref_expr()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("not a column expr", K(exprs.at(i)), K(i), K(ret));
      } else if (static_cast<ObColumnRefRawExpr*>(exprs.at(i))->get_table_id() != table->table_id_) {
        ret = corr_exprs.push_back(NULL);
      } else {
        int64_t idx =
            static_cast<int64_t>(static_cast<ObColumnRefRawExpr*>(exprs.at(i))->get_column_id() - OB_APP_MIN_COLUMN_ID);
        ret = corr_exprs.push_back(table->ref_query_->get_select_item(idx).expr_);
      }
    }
  }
  return ret;
}

int ObOptimizerUtil::get_subquery_id(const ObDMLStmt* upper_stmt, const ObSelectStmt* stmt, uint64_t& id)
{
  int ret = OB_SUCCESS;
  id = OB_INVALID_ID;
  if (OB_ISNULL(upper_stmt) || OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid stmt", K(upper_stmt), K(stmt), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && OB_INVALID_ID == id && i < upper_stmt->get_table_size(); ++i) {
      if (OB_ISNULL(upper_stmt->get_table_item(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Invalid table item", K(upper_stmt->get_table_item(i)), K(ret));
      } else if (upper_stmt->get_table_item(i)->is_generated_table() &&
                 upper_stmt->get_table_item(i)->ref_query_ == stmt) {
        id = upper_stmt->get_table_item(i)->table_id_;
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObOptimizerUtil::is_table_on_null_side(const ObDMLStmt* stmt, uint64_t table_id, bool& is_on_null_side)
{
  int ret = OB_SUCCESS;
  is_on_null_side = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("upper_stmt is null", K(ret));
  } else {
    bool is_in_joined_table = false;
    for (int64_t i = 0; OB_SUCC(ret) && !is_in_joined_table && i < stmt->get_joined_tables().count(); i++) {
      JoinedTable* joined_table = stmt->get_joined_tables().at(i);
      if (OB_FAIL(is_table_on_null_side_recursively(joined_table, table_id, is_in_joined_table, is_on_null_side))) {
        LOG_WARN("Check for generated table on null side recursively fails", K(is_on_null_side), K(i), K(ret));
      }
    }
  }
  return ret;
}

int ObOptimizerUtil::is_table_on_null_side_recursively(
    const TableItem* table_item, uint64_t table_id, bool& found, bool& is_on_null_side)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  found = false;
  is_on_null_side = false;
  if (OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Get unexpected null", K(ret), K(table_id), K(table_item), K(is_on_null_side));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("check stack overflow failed", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret));
  } else if (OB_FAIL(find_table_item(table_item, table_id, found))) {
    LOG_WARN("Find table item fails", K(ret), K(table_id), K(found));
  } else {
    if (table_item->is_joined_table() && found) {
      TableItem* left_table = NULL;
      TableItem* right_table = NULL;
      const JoinedTable* joined_table = static_cast<const JoinedTable*>(table_item);
      ObJoinType join_type = joined_table->joined_type_;
      if (OB_ISNULL(left_table = joined_table->left_table_) || OB_ISNULL(right_table = joined_table->right_table_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Get unexpected null",
            K(ret),
            K(table_id),
            K(table_item),
            K(left_table),
            K(right_table),
            K(is_on_null_side));
      } else if (FULL_OUTER_JOIN == join_type) {
        is_on_null_side = true;
      } else if (LEFT_OUTER_JOIN == join_type) {
        if (OB_FAIL(find_table_item(right_table, table_id, found))) {
          LOG_WARN("Find in joined table fails", K(ret));
        } else if (found) {
          is_on_null_side = true;
        } else {
          if (OB_FAIL(SMART_CALL(is_table_on_null_side_recursively(left_table, table_id, found, is_on_null_side)))) {
            LOG_WARN("Checking for table on null side recursively fails", K(ret));
          }
        }
      } else if (RIGHT_OUTER_JOIN == join_type) {
        if (OB_FAIL(find_table_item(left_table, table_id, found))) {
          LOG_WARN("Find in joined table fails", K(ret));
        } else if (found) {
          is_on_null_side = true;
        } else {
          if (OB_FAIL(SMART_CALL(is_table_on_null_side_recursively(right_table, table_id, found, is_on_null_side)))) {
            LOG_WARN("Checking for table on null side recursively fails", K(ret));
          }
        }
      } else {  // Other join type
        if (OB_FAIL(SMART_CALL(is_table_on_null_side_recursively(left_table, table_id, found, is_on_null_side)))) {
          LOG_WARN("Checking for table on null side recursively fails", K(ret));
        }
        if (OB_SUCC(ret) && !found) {
          if (OB_FAIL(SMART_CALL(is_table_on_null_side_recursively(right_table, table_id, found, is_on_null_side)))) {
            LOG_WARN("Checking for table on null side recursively fails", K(ret));
          }
        }
      }
    }
  }

  return ret;
}

int ObOptimizerUtil::find_table_item(const TableItem* table_item, uint64_t table_id, bool& found)
{
  int ret = OB_SUCCESS;
  found = false;
  if (OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("joined_table is null", K(ret), K(table_item));
  } else if (table_item->is_joined_table()) {
    const JoinedTable* joined_table = static_cast<const JoinedTable*>(table_item);
    found = find_item(joined_table->single_table_ids_, table_id);
  } else {
    found = table_id == table_item->table_id_;
  }
  return ret;
}

int ObOptimizerUtil::get_referenced_columns(const ObDMLStmt* stmt, const uint64_t table_id,
    const common::ObIArray<ObRawExpr*>& keys, common::ObIArray<ObRawExpr*>& columns)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get_stmt() returns null", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_column_size(); ++i) {
      const ColumnItem* col_item = stmt->get_column_item(i);
      if (OB_ISNULL(col_item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("col_item is null", K(i), K(ret));
      } else if (OB_ISNULL(col_item->expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null", K(ret));
      } else if (col_item->table_id_ == table_id && col_item->expr_->is_explicited_reference() &&
                 find_item(keys, col_item->expr_)) {
        ret = columns.push_back(col_item->expr_);
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObOptimizerUtil::get_non_referenced_columns(const ObDMLStmt* stmt, const uint64_t table_id,
    const common::ObIArray<ObRawExpr*>& keys, common::ObIArray<ObRawExpr*>& columns)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get_stmt() returns null", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_column_size(); ++i) {
      const ColumnItem* col_item = stmt->get_column_item(i);
      if (OB_ISNULL(col_item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("col_item is null", K(i), K(ret));
      } else if (OB_ISNULL(col_item->expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null", K(ret));
      } else if (col_item->table_id_ == table_id && col_item->expr_->is_explicited_reference() &&
                 !find_item(keys, col_item->expr_)) {
        ret = columns.push_back(col_item->expr_);
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObOptimizerUtil::is_contain_nl_params(
    const ObIArray<ObRawExpr*>& filters, const int64_t max_param_num, bool& is_contain)
{
  int ret = OB_SUCCESS;
  is_contain = false;
  for (int64_t i = 0; OB_SUCC(ret) && !is_contain && i < filters.count(); i++) {
    ObArray<ObRawExpr*> params;
    if (OB_FAIL(extract_params(filters.at(i), params))) {
      LOG_WARN("failed to extract_params", K(i), K(ret));
    } else {
      for (int64_t j = 0; OB_SUCC(ret) && !is_contain && j < params.count(); ++j) {
        int64_t param_value = -1;
        if (OB_ISNULL(params.at(j))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("params.at(j) returns null", K(ret), K(j));
        } else {
          param_value = static_cast<ObConstRawExpr*>(params.at(j))->get_value().get_unknown();
          if (OB_SUCC(ret) && param_value >= max_param_num) {
            is_contain = true;
          }
        }
      }
    }
  }

  return ret;
}

int ObOptimizerUtil::extract_parameterized_correlated_filters(const ObIArray<ObRawExpr*>& filters,
    const int64_t max_param_num, ObIArray<ObRawExpr*>& correlated_filters, ObIArray<ObRawExpr*>& uncorrelated_filters)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < filters.count(); ++i) {
    ObArray<ObRawExpr*> params;
    if (OB_FAIL(extract_params(filters.at(i), params))) {
      LOG_WARN("failed to extract_params", K(i), K(ret));
    } else {
      bool correlated = false;
      for (int64_t j = 0; OB_SUCC(ret) && !correlated && j < params.count(); ++j) {
        int64_t param_value = -1;
        if (OB_ISNULL(params.at(j))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("params.at(j) returns null", K(ret), K(j));
        } else {
          param_value = static_cast<ObConstRawExpr*>(params.at(j))->get_value().get_unknown();
        }
        if (OB_SUCC(ret) && param_value >= max_param_num) {
          correlated = true;
          if (OB_FAIL(correlated_filters.push_back(filters.at(i)))) {
            LOG_WARN("failed to push back uncorrelated_filters", K(i), K(j), K(ret));
          }
        } else { /* Do nothing */
        }
      }

      if (OB_SUCC(ret) && !correlated) {
        if (OB_FAIL(uncorrelated_filters.push_back(filters.at(i)))) {
          LOG_WARN("failed to push back uncorrelated_filters", K(i), K(ret));
        }
      } else { /* Do nothing */
      }
    }
  }  // end for
  return ret;
}

int ObOptimizerUtil::add_parameterized_expr(
    ObRawExpr*& target_expr, ObRawExpr* orig_expr, ObRawExpr* child_expr, int64_t child_idx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(target_expr) || OB_ISNULL(child_expr) || OB_ISNULL(orig_expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null pointer passed in", K(ret));
  } else {
    switch (target_expr->get_expr_class()) {
      case ObRawExpr::EXPR_CASE_OPERATOR: {
        ObCaseOpRawExpr* new_expr = static_cast<ObCaseOpRawExpr*>(target_expr);
        ObCaseOpRawExpr* origin_expr = static_cast<ObCaseOpRawExpr*>(orig_expr);
        if (0 > child_idx || child_idx >= origin_expr->get_param_count()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid index for case op operator", K(ret));
        } else if (T_OP_ARG_CASE == orig_expr->get_expr_type()) {
          // param vector: arg expr, when expr, then expr ... default expr
          if (child_idx == 0) {
            new_expr->set_arg_param_expr(child_expr);
          } else if (child_idx == origin_expr->get_when_expr_size() + origin_expr->get_then_expr_size() + 1) {
            new_expr->set_default_param_expr(child_expr);
          } else if (child_idx & 1) {
            ret = new_expr->add_when_param_expr(child_expr);
          } else {
            ret = new_expr->add_then_param_expr(child_expr);
          }
        } else {
          // param vector: when expr, then expr ... default expr
          if (child_idx == origin_expr->get_when_expr_size() + origin_expr->get_then_expr_size()) {
            new_expr->set_default_param_expr(child_expr);
          } else if (child_idx & 1) {
            ret = new_expr->add_then_param_expr(child_expr);
          } else {
            ret = new_expr->add_when_param_expr(child_expr);
          }
        }
        break;
      }

      case ObRawExpr::EXPR_AGGR: {
        if (child_idx != 0 && T_FUN_GROUP_CONCAT != target_expr->get_expr_type() &&
            T_FUN_COUNT != target_expr->get_expr_type()) {
          LOG_WARN("invalid index for agg expr except group_concat and count", K(ret), K(*orig_expr));
        } else {
          ObAggFunRawExpr* cur_expr = static_cast<ObAggFunRawExpr*>(target_expr);
          // cur_expr->set_param_expr(to_add_expr);
          if (0 != cur_expr->get_real_param_count() && T_FUN_GROUP_CONCAT != cur_expr->get_expr_type() &&
              T_FUN_COUNT != target_expr->get_expr_type()) {
            LOG_WARN("except group_concat and count, now, agg expr real param count must be 0", K(ret), K(*cur_expr));
          } else if (OB_FAIL(cur_expr->add_real_param_expr(child_expr))) {
            LOG_WARN("failed to add expr to param expr", K(ret));
          }
        }
        break;
      }

      case ObRawExpr::EXPR_OPERATOR:  // fall through
      case ObRawExpr::EXPR_SYS_FUNC:
      case ObRawExpr::EXPR_UDF: {
        ObOpRawExpr* cur_expr = static_cast<ObOpRawExpr*>(target_expr);
        if (OB_FAIL(cur_expr->add_param_expr(child_expr))) {
          LOG_WARN("failed to add expr to param expr", K(ret));
        }
        break;
      }
      default:
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid expr type passed in", K(ret));
    }  // switch case end
  }
  return ret;
}

int ObOptimizerUtil::extract_column_ids(const ObRawExpr* expr, const uint64_t table_id, ObIArray<uint64_t>& column_ids)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 16> columns;
  if (OB_FAIL(ObRawExprUtils::extract_column_exprs(expr, columns))) {
    LOG_WARN("failed to extract column exprs", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < columns.count(); ++i) {
      if (OB_ISNULL(columns.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column expr is NULL", K(i), K(columns.at(i)), K(ret));
      } else if (!columns.at(i)->is_column_ref_expr()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Invalid column expr", K(i), K(*columns.at(i)), K(ret));
      } else {
        ObColumnRefRawExpr* column = static_cast<ObColumnRefRawExpr*>(columns.at(i));
        if (column->get_table_id() == table_id) {
          if (OB_FAIL(column_ids.push_back(column->get_column_id()))) {
            LOG_WARN("push back error", K(table_id), K(column->get_column_id()), K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObOptimizerUtil::extract_column_ids(const ObRawExpr* expr, const uint64_t table_id, ObBitSet<>& column_ids)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 16> id_array;
  if (OB_FAIL(extract_column_ids(expr, table_id, id_array))) {
    LOG_WARN("failed to extract column ids", K(expr), K(table_id), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < id_array.count(); ++i) {
      if (OB_FAIL(column_ids.add_member(id_array.at(i)))) {
        LOG_WARN("add member error", K(id_array.at(i)), K(ret));
      }
    }
  }
  return ret;
}

int ObOptimizerUtil::extract_column_ids(
    const ObIArray<ObRawExpr*>& exprs, const uint64_t table_id, ObIArray<uint64_t>& column_ids)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 16> ids;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    if (OB_FAIL(extract_column_ids(exprs.at(i), table_id, ids))) {
      LOG_WARN("failed to extract column ids", K(exprs.at(i)), K(table_id), K(ret));
    } else if (OB_FAIL(append(column_ids, ids))) {
      LOG_WARN("add members error", K(ids), K(column_ids), K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObOptimizerUtil::extract_column_ids(
    const ObIArray<ObRawExpr*>& exprs, const uint64_t table_id, ObBitSet<>& column_ids)
{
  int ret = OB_SUCCESS;
  ObBitSet<> ids;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    if (OB_FAIL(extract_column_ids(exprs.at(i), table_id, ids))) {
      LOG_WARN("failed to extract column ids", K(exprs.at(i)), K(table_id), K(ret));
    } else if (OB_FAIL(column_ids.add_members(ids))) {
      LOG_WARN("add members error", K(ids), K(column_ids), K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObOptimizerUtil::is_same_table(const ObIArray<OrderItem>& exprs, uint64_t& table_id, bool& is_same)
{
  int ret = OB_SUCCESS;
  table_id = UINT64_MAX;
  is_same = false;
  if (exprs.count() > 0) {
    is_same = true;
    for (int64_t i = 0; OB_SUCC(ret) && is_same && i < exprs.count(); ++i) {
      if (!exprs.at(i).expr_->is_column_ref_expr()) {
        is_same = false;
      } else {
        ObColumnRefRawExpr* column = static_cast<ObColumnRefRawExpr*>(exprs.at(i).expr_);
        if (UINT64_MAX == table_id) {
          table_id = column->get_table_id();
        } else if (column->get_table_id() != table_id) {
          is_same = false;
        }
      }
    }
  }
  return ret;
}

int ObOptimizerUtil::make_sort_keys(
    const ObIArray<ObRawExpr*>& sort_exprs, const ObOrderDirection direction, ObIArray<OrderItem>& sort_keys)
{
  int ret = OB_SUCCESS;
  for (int64_t idx = 0; OB_SUCC(ret) && idx < sort_exprs.count(); ++idx) {
    if (OB_FAIL(sort_keys.push_back(OrderItem(sort_exprs.at(idx), direction)))) {
      LOG_WARN("Failed to add sort key", K(ret));
    }
  }
  return ret;
}

int ObOptimizerUtil::make_sort_keys(const ObIArray<OrderItem>& candi_sort_keys,
    const ObIArray<ObRawExpr*>& need_sort_exprs, ObIArray<OrderItem>& sort_keys)
{
  int ret = OB_SUCCESS;
  sort_keys.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < candi_sort_keys.count() && sort_keys.count() < need_sort_exprs.count(); ++i) {
    if (candi_sort_keys.at(i).expr_ == need_sort_exprs.at(sort_keys.count())) {
      ret = sort_keys.push_back(candi_sort_keys.at(i));
    }
  }
  if (OB_SUCC(ret) && sort_keys.count() != need_sort_exprs.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected candi sort key/sort exprs", K(ret), K(candi_sort_keys), K(need_sort_exprs));
  }
  return ret;
}

int ObOptimizerUtil::make_sort_keys(const ObIArray<ObRawExpr*>& candi_sort_exprs,
    const ObIArray<ObOrderDirection>& candi_directions, const ObIArray<ObRawExpr*>& need_sort_exprs,
    ObIArray<OrderItem>& sort_keys)
{
  int ret = OB_SUCCESS;
  sort_keys.reset();
  if (candi_directions.count() != candi_sort_exprs.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected candi sort exprs/directions count", K(ret), K(candi_sort_exprs), K(candi_directions));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < candi_sort_exprs.count() && sort_keys.count() < need_sort_exprs.count();
       ++i) {
    if (candi_sort_exprs.at(i) == need_sort_exprs.at(sort_keys.count())) {
      ret = sort_keys.push_back(OrderItem(candi_sort_exprs.at(i), candi_directions.at(i)));
    }
  }
  if (sort_keys.count() != need_sort_exprs.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected candi sort key/sort exprs", K(ret), K(candi_sort_exprs), K(need_sort_exprs));
  }
  return ret;
}

int ObOptimizerUtil::make_sort_keys(const ObIArray<ObRawExpr*>& sort_exprs,
    const ObIArray<ObOrderDirection>& directions, ObIArray<OrderItem>& sort_keys)
{
  int ret = OB_SUCCESS;
  if (sort_exprs.count() != directions.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("exprs number dismatches directions number", K(ret), K(sort_exprs.count()), K(directions.count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < sort_exprs.count(); ++i) {
    OrderItem sort_key(sort_exprs.at(i), directions.at(i));
    if (OB_FAIL(sort_keys.push_back(sort_key))) {
      LOG_WARN("failed to add sort key", K(ret));
    }
  }
  return ret;
}

int ObOptimizerUtil::split_expr_direction(
    const ObIArray<OrderItem>& order_items, ObIArray<ObRawExpr*>& raw_exprs, ObIArray<ObOrderDirection>& directions)
{
  int ret = OB_SUCCESS;
  for (int64_t idx = 0; OB_SUCC(ret) && idx < order_items.count(); ++idx) {
    if (OB_FAIL(raw_exprs.push_back(order_items.at(idx).expr_))) {
      LOG_WARN("Failed to add expr", K(ret));
    } else if (OB_FAIL(directions.push_back(order_items.at(idx).order_type_))) {
      LOG_WARN("Failed to add direction", K(ret));
    } else {
    }  // do nothing
  }
  return ret;
}

int ObOptimizerUtil::check_equal_query_ranges(
    const ObIArray<ObNewRange*>& ranges, const int64_t prefix_len, bool& all_prefix_equal, bool& all_full_equal)
{
  int ret = OB_SUCCESS;
  all_prefix_equal = false;
  all_full_equal = false;
  if (prefix_len < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(prefix_len), K(ret));
  } else if (0 == ranges.count()) {
    // do nothing
  } else {
    int64_t rowkey_count = -1;
    int64_t equal_prefix_count = -1;
    int64_t equal_prefix_null_count = 0;
    int64_t range_prefix_count = -1;
    bool contain_always_false = false;
    if (OB_ISNULL(ranges.at(0)) || OB_UNLIKELY((rowkey_count = ranges.at(0)->start_key_.length()) <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error", K(rowkey_count), K(ranges.at(0)), K(ret));
    } else if (OB_FAIL(check_prefix_ranges_count(
                   ranges, equal_prefix_count, equal_prefix_null_count, range_prefix_count, contain_always_false))) {
      LOG_WARN("failed to check ranges prefix count", K(ret));
    } else {
      if (prefix_len <= equal_prefix_count) {
        all_prefix_equal = true;
      }
      if (prefix_len == rowkey_count) {
        all_full_equal = true;
      }
    }
  }
  return ret;
}

int ObOptimizerUtil::check_prefix_ranges_count(const ObIArray<common::ObNewRange*>& ranges, int64_t& equal_prefix_count,
    int64_t& equal_prefix_null_count, int64_t& range_prefix_count, bool& contain_always_false)
{
  int ret = OB_SUCCESS;
  equal_prefix_count = 0;
  equal_prefix_null_count = 0;
  range_prefix_count = 0;
  contain_always_false = false;
  if (ranges.count() > 0) {
    equal_prefix_count = OB_USER_MAX_ROWKEY_COLUMN_NUMBER;
    range_prefix_count = OB_USER_MAX_ROWKEY_COLUMN_NUMBER;
    for (int64_t i = 0; OB_SUCC(ret) && i < ranges.count(); ++i) {
      ObNewRange* range = ranges.at(i);
      int64_t temp_equal_prefix_count = 0;
      int64_t temp_range_prefix_count = 0;
      if (OB_ISNULL(range)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null query range", K(ret));
      } else if (range->start_key_.length() != range->end_key_.length()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid start and end range key", K(range->start_key_.length()), K(range->end_key_.length()), K(ret));
      } else if (range->start_key_.ptr()[0].is_max_value() && range->end_key_.ptr()[0].is_min_value()) {
        contain_always_false = true;
        equal_prefix_count = 0;
        range_prefix_count = 0;
      } else if (OB_FAIL(check_prefix_range_count(range, temp_equal_prefix_count, temp_range_prefix_count))) {
        LOG_WARN("failed to check range prefix", K(ret));
      } else {
        equal_prefix_count = std::min(equal_prefix_count, temp_equal_prefix_count);
        range_prefix_count = std::min(range_prefix_count, temp_range_prefix_count);
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < ranges.count(); ++i) {
      const ObNewRange* range = ranges.at(i);
      int64_t temp_equal_prefix_null_count = 0;
      if (OB_FAIL(check_equal_prefix_null_count(range, equal_prefix_count, temp_equal_prefix_null_count))) {
        LOG_WARN("failed to check range prefix", K(ret));
      } else {
        equal_prefix_null_count = std::max(equal_prefix_null_count, temp_equal_prefix_null_count);
      }
    }
  }
  return ret;
}

int ObOptimizerUtil::check_prefix_ranges_count(const ObIArray<common::ObNewRange>& ranges, int64_t& equal_prefix_count,
    int64_t& equal_prefix_null_count, int64_t& range_prefix_count)
{
  int ret = OB_SUCCESS;
  equal_prefix_count = 0;
  equal_prefix_null_count = 0;
  range_prefix_count = 0;
  if (ranges.count() > 0) {
    equal_prefix_count = OB_USER_MAX_ROWKEY_COLUMN_NUMBER;
    range_prefix_count = OB_USER_MAX_ROWKEY_COLUMN_NUMBER;
    for (int64_t i = 0; OB_SUCC(ret) && i < ranges.count(); ++i) {
      const ObNewRange& range = ranges.at(i);
      int64_t temp_equal_prefix_count = 0;
      int64_t temp_range_prefix_count = 0;
      if (range.start_key_.length() != range.end_key_.length()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid start and end range key", K(range.start_key_.length()), K(range.end_key_.length()), K(ret));
      } else if (OB_FAIL(check_prefix_range_count(&range, temp_equal_prefix_count, temp_range_prefix_count))) {
        LOG_WARN("failed to check range prefix", K(ret));
      } else {
        equal_prefix_count = std::min(equal_prefix_count, temp_equal_prefix_count);
        range_prefix_count = std::min(range_prefix_count, temp_range_prefix_count);
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < ranges.count(); ++i) {
      const ObNewRange& range = ranges.at(i);
      int64_t temp_equal_prefix_null_count = 0;
      if (OB_FAIL(check_equal_prefix_null_count(&range, equal_prefix_count, temp_equal_prefix_null_count))) {
        LOG_WARN("failed to check range prefix", K(ret));
      } else {
        equal_prefix_null_count = std::max(equal_prefix_null_count, temp_equal_prefix_null_count);
      }
    }
  }
  return ret;
}

int ObOptimizerUtil::check_equal_prefix_null_count(
    const common::ObNewRange* range, const int64_t equal_prefix_count, int64_t& equal_prefix_null_count)
{
  int ret = OB_SUCCESS;
  equal_prefix_null_count = 0;
  if (OB_ISNULL(range)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null query range", K(ret));
  } else if (range->start_key_.length() != range->end_key_.length() ||
             equal_prefix_count > range->start_key_.length()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid start and end range key",
        K(ret),
        K(equal_prefix_count),
        K(range->start_key_.length()),
        K(range->end_key_.length()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < equal_prefix_count; ++i) {
    if (range->start_key_.ptr()[i] != range->end_key_.ptr()[i]) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected equal range", K(ret), K(range->start_key_.ptr()[i]), K(range->end_key_.ptr()[i]));
    } else if ((is_oracle_mode() && range->start_key_.ptr()[i].is_null_oracle()) ||
               range->start_key_.ptr()[i].is_null()) {
      ++equal_prefix_null_count;
    } else { /* do nothing */
    }
  }
  return ret;
}

int ObOptimizerUtil::check_prefix_range_count(
    const common::ObNewRange* range, int64_t& equal_prefix_count, int64_t& range_prefix_count)
{
  int ret = OB_SUCCESS;
  equal_prefix_count = 0;
  range_prefix_count = 0;
  if (OB_ISNULL(range)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null query range", K(ret));
  } else if (range->start_key_.length() != range->end_key_.length()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid start and end range key", K(range->start_key_.length()), K(range->end_key_.length()), K(ret));
  } else {
    equal_prefix_count = range->start_key_.length();
    for (int64_t i = 0;
         OB_SUCC(ret) && i < range->start_key_.length() && equal_prefix_count == range->start_key_.length();
         ++i) {
      if (range->start_key_.ptr()[i].is_min_value() || range->start_key_.ptr()[i].is_max_value() ||
          range->end_key_.ptr()[i].is_min_value() || range->end_key_.ptr()[i].is_max_value()) {
        equal_prefix_count = i;
      } else if (range->start_key_.ptr()[i] != range->end_key_.ptr()[i]) {
        equal_prefix_count = i;
      } else { /* do nothing */
      }
    }
    range_prefix_count = range->start_key_.length();
    for (int64_t i = 0;
         OB_SUCC(ret) && i < range->start_key_.length() && range_prefix_count == range->start_key_.length();
         ++i) {
      if ((range->start_key_.ptr()[i].is_min_value() || range->start_key_.ptr()[i].is_max_value()) &&
          (range->end_key_.ptr()[i].is_min_value() || range->end_key_.ptr()[i].is_max_value())) {
        range_prefix_count = i;
      } else if (range->start_key_.ptr()[i].is_min_value() || range->start_key_.ptr()[i].is_max_value() ||
                 range->end_key_.ptr()[i].is_min_value() || range->end_key_.ptr()[i].is_max_value() ||
                 range->start_key_.ptr()[i] != range->end_key_.ptr()[i]) {
        range_prefix_count = i + 1;
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

bool ObOptimizerUtil::same_partition_exprs(const common::ObIArray<ObRawExpr*>& l_exprs,
    const common::ObIArray<ObRawExpr*>& r_exprs, const EqualSets& equal_sets)
{
  bool same = true;

  for (int64_t i = 0; i < l_exprs.count(); ++i) {
    same = find_equal_expr(r_exprs, l_exprs.at(i), equal_sets);
    if (!same) {
      break;
    }
  }
  if (same) {
    for (int64_t i = 0; i < r_exprs.count(); ++i) {
      same = find_equal_expr(l_exprs, r_exprs.at(i), equal_sets);
      if (!same) {
        break;
      }
    }
  }

  return same;
}

int ObOptimizerUtil::classify_subquery_exprs(
    const ObIArray<ObRawExpr*>& exprs, ObIArray<ObRawExpr*>& subquery_exprs, ObIArray<ObRawExpr*>& non_subquery_exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
    ObRawExpr* temp_expr = exprs.at(i);
    if (OB_ISNULL(temp_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null expr", K(ret), K(temp_expr), K(ret));
    } else if (temp_expr->has_flag(CNT_SUB_QUERY)) {
      // used to allocate subplan filter
      if (OB_FAIL(subquery_exprs.push_back(temp_expr))) {
        LOG_WARN("failed to push back subquery exprs", K(ret));
      } else { /*do nothing*/
      }
    } else {
      if (OB_FAIL(non_subquery_exprs.push_back(temp_expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObOptimizerUtil::get_subquery_exprs(const ObIArray<ObRawExpr*>& exprs, ObIArray<ObRawExpr*>& subquery_exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
    if (OB_ISNULL(exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null expr", K(ret));
    } else if (exprs.at(i)->has_flag(CNT_SUB_QUERY) && OB_FAIL(subquery_exprs.push_back(exprs.at(i)))) {
      LOG_WARN("failed to push back exprs", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObOptimizerUtil::check_is_onetime_expr(const ObIArray<std::pair<int64_t, ObRawExpr*>>& onetime_exprs,
    ObRawExpr* expr, ObDMLStmt* stmt, bool& is_onetime_expr)
{
  int ret = OB_SUCCESS;
  bool contains = false;
  is_onetime_expr = false;
  if (OB_ISNULL(expr) || OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null expr", K(expr), K(stmt), K(ret));
  } else if (expr->get_expr_levels().has_member(stmt->get_current_level())) {
    // do nothing
  } else if (OB_FAIL(check_expr_contain_sharable_subquery(
                 expr, stmt->get_current_level(), onetime_exprs, true, contains))) {
    LOG_WARN("failed to check expr contain onetime expr", K(ret));
  } else if (contains) {
    /*do nothing*/
  } else if (has_psedu_column(*expr) || has_hierarchical_expr(*expr)) {
    // do nothing
  } else if (expr->has_flag(IS_SUB_QUERY)) {
    is_onetime_expr = expr->get_output_column() == 1 && !static_cast<ObQueryRefRawExpr*>(expr)->is_set();
  } else if (expr->has_flag(CNT_SUB_QUERY)) {
    if (T_FUN_COLUMN_CONV != expr->get_expr_type() && expr->get_expr_type() != T_OP_ROW && !expr->is_win_func_expr()) {
      is_onetime_expr = true;
    }
  }
  if (is_onetime_expr) {
    bool has_ref_assign_user_var = false;
    if (OB_FAIL(check_subquery_has_ref_assign_user_var(expr, has_ref_assign_user_var))) {
      LOG_WARN("failed to check expr on onetime valid", K(ret));
    } else if (has_ref_assign_user_var) {
      is_onetime_expr = false;
    } else {
      is_onetime_expr = true;
    }
  }
  LOG_TRACE("succeed to check if expr is onetime",
      K(stmt->get_current_level()),
      K(expr->get_expr_levels()),
      K(*expr),
      K(contains),
      K(is_onetime_expr));
  return ret;
}

int ObOptimizerUtil::get_non_const_expr_size(const ObIArray<ObRawExpr*>& exprs, const EqualSets& equal_sets,
    const ObIArray<ObRawExpr*>& const_exprs, int64_t& number)
{
  int ret = OB_SUCCESS;
  bool is_const = false;
  number = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    if (OB_FAIL(is_const_expr(exprs.at(i), equal_sets, const_exprs, is_const))) {
      LOG_WARN("failed to check is const expr", K(ret));
    } else if (!is_const) {
      ++number;
    }
  }
  return ret;
}

int ObOptimizerUtil::classify_get_scan_ranges(const common::ObIArray<ObNewRange>& input_ranges,
    common::ObIAllocator& allocator, common::ObIArray<ObNewRange>& get_ranges,
    common::ObIArray<ObNewRange>& scan_ranges, const ObTableSchema* table_schema /* NULL */,
    const bool should_trans_rowid /* false */)
{
  int ret = OB_SUCCESS;
  ObArray<ObNewRange> used_input_ranges;
  for (int i = 0; OB_SUCC(ret) && i < input_ranges.count(); i++) {
    bool contain_rowid_range = false;
    if (!should_trans_rowid) {
      // do nothing
    } else if (!input_ranges.at(i).start_key_.is_min_row() && !input_ranges.at(i).start_key_.is_max_row() &&
               input_ranges.at(i).is_valid()) {
      contain_rowid_range = ob_is_urowid(input_ranges.at(i).start_key_.get_obj_ptr()[0].get_type());
    } else if (!input_ranges.at(i).end_key_.is_min_row() && !input_ranges.at(i).end_key_.is_max_row() &&
               input_ranges.at(i).end_key_.is_valid()) {
      contain_rowid_range = ob_is_urowid(input_ranges.at(i).end_key_.get_obj_ptr()[0].get_type());
    }
    if (contain_rowid_range) {
      LOG_TRACE("start to transform rowid range to pk range", K(input_ranges.at(i)));
      ObNewRange tmp_range;
      ObSEArray<ObColDesc, 4> rowkey_descs;
      if (OB_ISNULL(table_schema)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("table_schema is NULL", K(ret));
      } else if (OB_FAIL(deep_copy_range(allocator, input_ranges.at(i), tmp_range))) {
        LOG_WARN("failed to deep copy range", K(ret));
      } else if (OB_FAIL(table_schema->get_rowkey_column_ids(rowkey_descs))) {
        LOG_WARN("get rowkey column desc failed", K(ret));
      } else if (OB_FAIL(ObTableScan::transform_rowid_range(allocator, rowkey_descs, tmp_range))) {
        LOG_WARN("failed to transform rowid range", K(ret));
      } else if (OB_FAIL(used_input_ranges.push_back(tmp_range))) {
        LOG_WARN("failed to push back range", K(ret));
      }
    } else if (OB_FAIL(used_input_ranges.push_back(input_ranges.at(i)))) {
      LOG_WARN("failed to push back range", K(ret));
    }
    if (OB_SUCC(ret)) {
      LOG_TRACE("used input range", K(used_input_ranges.at(used_input_ranges.count() - 1)));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < used_input_ranges.count(); i++) {
    if (used_input_ranges.at(i).is_single_rowkey()) {
      if (OB_FAIL(get_ranges.push_back(used_input_ranges.at(i)))) {
        LOG_WARN("failed to push back ranges", K(ret));
      } else { /*do nothing*/
      }
    } else {
      if (OB_FAIL(scan_ranges.push_back(used_input_ranges.at(i)))) {
        LOG_WARN("failed to push back scan ranges", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObOptimizerUtil::is_exprs_unique(const ObIArray<OrderItem>& ordering, const ObIArray<ObFdItem*>& fd_item_set,
    const EqualSets& equal_sets, const ObIArray<ObRawExpr*>& const_exprs, bool& order_unique)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 6> order_exprs;
  ObSEArray<ObOrderDirection, 6> order_directions;
  order_unique = false;
  if (OB_FAIL(split_expr_direction(ordering, order_exprs, order_directions))) {
    LOG_WARN("failed to split expr and direction", K(ret));
  } else if (OB_FAIL(is_exprs_unique(order_exprs, fd_item_set, equal_sets, const_exprs, order_unique))) {
    LOG_WARN("failed to check is order unique", K(ret));
  }
  return ret;
}

int ObOptimizerUtil::is_exprs_unique(const ObIArray<OrderItem>& ordering, const ObRelIds& all_tables,
    const ObIArray<ObFdItem*>& fd_item_set, const EqualSets& equal_sets, const ObIArray<ObRawExpr*>& const_exprs,
    bool& order_unique)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 6> order_exprs;
  ObSEArray<ObOrderDirection, 6> order_directions;
  order_unique = false;
  if (OB_FAIL(split_expr_direction(ordering, order_exprs, order_directions))) {
    LOG_WARN("failed to split expr and direction", K(ret));
  } else if (OB_FAIL(is_exprs_unique(order_exprs, all_tables, fd_item_set, equal_sets, const_exprs, order_unique))) {
    LOG_WARN("failed to check is order unique", K(ret));
  }
  return ret;
}

int ObOptimizerUtil::is_exprs_unique(const ObIArray<ObRawExpr*>& exprs, const ObRelIds& all_tables,
    const ObIArray<ObFdItem*>& fd_item_set, const EqualSets& equal_sets, const ObIArray<ObRawExpr*>& const_exprs,
    bool& is_unique)
{
  int ret = OB_SUCCESS;
  is_unique = false;
  ObSEArray<ObRawExpr*, 8> fd_set_parent_exprs;
  ObSEArray<ObRawExpr*, 8> extend_exprs;
  if (0 == all_tables.num_members()) {
    // select RANK() OVER() from dual
    if (OB_FAIL(is_exprs_unique(exprs, fd_item_set, equal_sets, const_exprs, is_unique))) {
      LOG_WARN("failed to check is exprs unique", K(ret));
    }
  } else if (OB_FAIL(extend_exprs.assign(exprs))) {
    LOG_WARN("failed to assign exprs", K(ret));
  } else if (OB_FAIL(get_fd_set_parent_exprs(fd_item_set, fd_set_parent_exprs))) {
    LOG_WARN("failed to get fd set parent exprs ", K(ret));
  } else if (OB_FAIL(remove_item(fd_set_parent_exprs, extend_exprs))) {
    LOG_WARN("failed to get fd set parent exprs ", K(ret));
  } else {
    ObRelIds remain_tables = all_tables;
    ObSqlBitSet<> skip_fd;
    int64_t exprs_count = -1;
    for (int64_t i = 0; OB_SUCC(ret) && !is_unique && i < 10 && extend_exprs.count() != exprs_count; ++i) {
      exprs_count = extend_exprs.count();
      if (OB_FAIL(is_exprs_unique(extend_exprs,
              remain_tables,
              fd_item_set,
              fd_set_parent_exprs,
              skip_fd,
              equal_sets,
              const_exprs,
              is_unique))) {
        LOG_WARN("failed to get fd set parent exprs ", K(ret));
      }
    }
  }
  return ret;
}

int ObOptimizerUtil::is_exprs_unique(ObIArray<ObRawExpr*>& extend_exprs, ObRelIds& remain_tables,
    const ObIArray<ObFdItem*>& fd_item_set, ObIArray<ObRawExpr*>& fd_set_parent_exprs, ObSqlBitSet<>& skip_fd,
    const EqualSets& equal_sets, const ObIArray<ObRawExpr*>& const_exprs, bool& is_unique)
{
  int ret = OB_SUCCESS;
  bool is_contain = false;
  ObFdItem* fd_item = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && !is_unique && i < fd_item_set.count(); ++i) {
    is_contain = false;
    if (OB_ISNULL(fd_item = fd_item_set.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null fd item", K(ret));
    } else if (skip_fd.has_member(i)) {
      /*do nothing*/
    } else if (fd_item->is_unique()) {  // unique fd item
      if (OB_FAIL(is_exprs_contain_fd_parent(extend_exprs, *fd_item, equal_sets, const_exprs, is_contain))) {
        LOG_WARN("failed to check is order unique", K(ret));
      } else if (is_contain) {
        is_unique = true;
      }
    } else if (fd_item->is_table_fd_item()) {  // not unique table fd item
      ObTableFdItem* table_fd_item = static_cast<ObTableFdItem*>(fd_item);
      if (!remain_tables.overlap(table_fd_item->get_child_tables())) {
        /*do nothing*/
      } else if (OB_FAIL(is_exprs_contain_fd_parent(extend_exprs, *fd_item, equal_sets, const_exprs, is_contain))) {
        LOG_WARN("failed to check is order unique", K(ret));
      } else if (!is_contain) {
        /*do nothing*/
      } else if (OB_FAIL(remain_tables.del_members(table_fd_item->get_child_tables()))) {
        LOG_WARN("failed to delete members", K(ret));
      } else if (OB_FAIL(split_child_exprs(fd_item, equal_sets, fd_set_parent_exprs, extend_exprs))) {
        LOG_WARN("failed to delete members", K(ret));
      }
      is_unique = (0 == remain_tables.num_members());
    } else {  // not unique expr fd item
      if (OB_FAIL(is_exprs_contain_fd_parent(extend_exprs, *fd_item, equal_sets, const_exprs, is_contain))) {
        LOG_WARN("failed to check is order unique", K(ret));
      } else if (is_contain && OB_FAIL(split_child_exprs(fd_item, equal_sets, fd_set_parent_exprs, extend_exprs))) {
        LOG_WARN("failed to delete members", K(ret));
      }
    }
    if (OB_SUCC(ret) && is_contain && OB_FAIL(skip_fd.add_member(i))) {
      LOG_WARN("failed to add member", K(ret));
    }
  }
  return ret;
}

int ObOptimizerUtil::is_exprs_unique(const ObIArray<ObRawExpr*>& exprs, const ObIArray<ObFdItem*>& fd_item_set,
    const EqualSets& equal_sets, const ObIArray<ObRawExpr*>& const_exprs, bool& is_unique)
{
  int ret = OB_SUCCESS;
  is_unique = false;
  for (int64_t i = 0; OB_SUCC(ret) && !is_unique && i < fd_item_set.count(); ++i) {
    if (OB_ISNULL(fd_item_set.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (!fd_item_set.at(i)->is_unique()) {
      // do nothing
    } else if (OB_FAIL(is_exprs_contain_fd_parent(exprs, *fd_item_set.at(i), equal_sets, const_exprs, is_unique))) {
      LOG_WARN("failed to check is order unique", K(ret));
    }
  }
  return ret;
}

int ObOptimizerUtil::get_fd_set_parent_exprs(
    const ObIArray<ObFdItem*>& fd_item_set, ObIArray<ObRawExpr*>& fd_set_parent_exprs)
{
  int ret = OB_SUCCESS;
  ObFdItem* fd_item = NULL;
  const ObRawExprSet* parent_exprs = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < fd_item_set.count(); ++i) {
    if (OB_ISNULL(fd_item = fd_item_set.at(i)) || OB_ISNULL(parent_exprs = fd_item->get_parent_exprs())) {
      LOG_WARN("unexpected null", K(ret));
    } else {
      for (int64_t j = 0; OB_SUCC(ret) && j < parent_exprs->count(); ++j) {
        if (add_var_to_array_no_dup(fd_set_parent_exprs, parent_exprs->at(j))) {
          LOG_WARN("failed to append array no dup", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObOptimizerUtil::split_child_exprs(const ObFdItem* fd_item, const EqualSets& equal_sets,
    ObIArray<ObRawExpr*>& exprs, ObIArray<ObRawExpr*>& child_exprs)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 8> other_exprs;
  bool is_in_child = false;
  if (OB_ISNULL(fd_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null fd item", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    if (OB_FAIL(fd_item->check_expr_in_child(exprs.at(i), equal_sets, is_in_child))) {
      LOG_WARN("failed to check expr in child", K(ret));
    } else if (is_in_child) {
      ret = child_exprs.push_back(exprs.at(i));
    } else {
      ret = other_exprs.push_back(exprs.at(i));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(exprs.assign(other_exprs))) {
    LOG_WARN("failed to assign exprs", K(ret));
  }
  return ret;
}

int ObOptimizerUtil::is_exprs_contain_fd_parent(const ObIArray<ObRawExpr*>& exprs, const ObFdItem& fd_item,
    const EqualSets& equal_sets, const ObIArray<ObRawExpr*>& const_exprs, bool& is_contain)
{
  int ret = OB_SUCCESS;
  is_contain = true;
  bool is_const = false;
  const ObRawExprSet* parent_exprs = NULL;
  if (OB_ISNULL(parent_exprs = fd_item.get_parent_exprs())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null parent exprs", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && is_contain && i < parent_exprs->count(); ++i) {
      if (find_equal_expr(exprs, parent_exprs->at(i), equal_sets)) {
        // do nothing
      } else if (OB_FAIL(is_const_expr(parent_exprs->at(i), equal_sets, const_exprs, is_const))) {
        LOG_WARN("failed to check is const expr", K(ret));
      } else if (!is_const) {
        is_contain = false;
      }
    }
  }
  return ret;
}

// n to 1 join, if left side fd item is unique or can function dependent join exprs,
// then left side fd item can function dependent right side's table set
int ObOptimizerUtil::add_fd_item_set_for_n21_join(ObFdItemFactory& fd_factory, ObIArray<ObFdItem*>& target,
    const ObIArray<ObFdItem*>& source, const ObIArray<ObRawExpr*>& join_exprs, const EqualSets& equal_sets,
    const ObRelIds& right_table_set)
{
  int ret = OB_SUCCESS;
  ObLogPlan* plan = NULL;
  bool is_in_child = false;
  // if join unique, matched fd item must be table level fd item
  for (int64_t i = 0; OB_SUCC(ret) && i < source.count(); ++i) {
    ObFdItem* fd_item = source.at(i);
    bool fd_other_side = false;
    if (OB_ISNULL(fd_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null fd item", K(ret));
    } else if (fd_item->is_unique()) {
      fd_other_side = true;
    } else if (OB_FAIL(fd_item->check_exprs_in_child(join_exprs, equal_sets, is_in_child))) {
      LOG_WARN("failed to check exprs in child", K(ret));
    } else if (is_in_child) {
      fd_other_side = true;
    }

    if (OB_SUCC(ret)) {
      if (fd_other_side) {
        ObTableFdItem* new_fd_item = NULL;
        if (fd_item->is_table_fd_item()) {
          // source fd item and matched fd item are both table level
          if (OB_FAIL(fd_factory.create_table_fd_item(new_fd_item, *static_cast<ObTableFdItem*>(fd_item)))) {
            LOG_WARN("failed to copy fd item", K(ret));
          }
          // source fd item is expr level but matched fd item is table level
        } else if (OB_FAIL(fd_factory.create_table_fd_item(
                       new_fd_item, fd_item->is_unique(), fd_item->get_parent_exprs(), fd_item->get_stmt_level()))) {
          LOG_WARN("failed to create fd item", K(ret));
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(new_fd_item->get_child_tables().add_members(right_table_set))) {
            LOG_WARN("failed to add members to child tables", K(ret));
          } else if (OB_FAIL(target.push_back(new_fd_item))) {
            LOG_WARN("failed to push back fd item", K(ret));
          }
        }
      } else if (OB_FAIL(target.push_back(fd_item))) {
        LOG_WARN("failed to push back fd item", K(ret));
      }
    }
  }
  return ret;
}

// n to n join, source fd item not unique anymore
int ObOptimizerUtil::add_fd_item_set_for_n2n_join(
    ObFdItemFactory& fd_factory, ObIArray<ObFdItem*>& target, const ObIArray<ObFdItem*>& source)
{
  int ret = OB_SUCCESS;
  ObFdItem* new_fd_item = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < source.count(); ++i) {
    const ObFdItem* fd_item = source.at(i);
    if (OB_ISNULL(fd_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null fd item", K(ret));
    } else if (!fd_item->is_unique()) {
      new_fd_item = const_cast<ObFdItem*>(fd_item);
    } else if (OB_FAIL(fd_factory.copy_fd_item(new_fd_item, *fd_item))) {
      LOG_WARN("failed to copy fd item", K(ret));
    } else {
      new_fd_item->set_is_unique(false);
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(target.push_back(new_fd_item))) {
        LOG_WARN("failed to push back fd item", K(ret));
      }
    }
  }
  return ret;
}

int ObOptimizerUtil::enhance_fd_item_set(const ObIArray<ObRawExpr*>& quals, ObIArray<ObFdItem*>& candi_fd_item_set,
    ObIArray<ObFdItem*>& fd_item_set, ObIArray<ObRawExpr*>& not_null_columns)
{
  int ret = OB_SUCCESS;
  bool is_happend = false;
  ObFdItem* fd_item = NULL;
  ObRawExprSet* parent_exprs = NULL;
  ObSEArray<ObFdItem*, 8> unused;
  for (int64_t i = 0; OB_SUCC(ret) && i < candi_fd_item_set.count(); ++i) {
    if (OB_ISNULL(fd_item = candi_fd_item_set.at(i)) || OB_ISNULL(parent_exprs = fd_item->get_parent_exprs())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null fd item", K(ret));
    } else {
      bool all_not_null = true;
      bool contain_not_null = false;
      for (int64_t j = 0; OB_SUCC(ret) && j < parent_exprs->count(); ++j) {
        ObRawExpr* cur_expr = parent_exprs->at(j);
        bool has_null_reject = false;
        if (ObOptimizerUtil::find_item(not_null_columns, cur_expr)) {
          contain_not_null = true;
        } else if (OB_FAIL(ObTransformUtils::has_null_reject_condition(quals, cur_expr, has_null_reject))) {
          LOG_WARN("failed to check has null reject condition", K(ret));
        } else if (!has_null_reject) {
          all_not_null = false;
        } else if (OB_FAIL(not_null_columns.push_back(cur_expr))) {
          LOG_WARN("failed to push back expr", K(ret));
        } else {
          contain_not_null = true;
        }
      }
      if (OB_SUCC(ret)) {
        if (all_not_null || (share::is_oracle_mode() && contain_not_null)) {
          is_happend = true;
          if (OB_FAIL(fd_item_set.push_back(fd_item))) {
            LOG_WARN("failed to push back fd item", K(ret));
          }
        } else if (OB_FAIL(unused.push_back(fd_item))) {
          LOG_WARN("failed to push back fd item", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret) && is_happend) {
    if (OB_FAIL(candi_fd_item_set.assign(unused))) {
      LOG_WARN("failed to assign candi fd item set", K(ret));
    }
  }
  return ret;
}

int ObOptimizerUtil::try_add_fd_item(ObDMLStmt* stmt, ObFdItemFactory& fd_factory, const uint64_t table_id,
    ObRelIds& tables, const ObTableSchema* index_schema, const ObIArray<ObRawExpr*>& quals,
    ObIArray<ObRawExpr*>& not_null_columns, ObIArray<ObFdItem*>& fd_item_set, ObIArray<ObFdItem*>& candi_fd_item_set)
{
  int ret = OB_SUCCESS;
  bool all_columns_used = true;
  bool all_not_null = true;
  bool contain_not_null = false;
  TableItem* table = NULL;
  ObSEArray<uint64_t, 8> column_ids;
  ObSEArray<ObRawExpr*, 8> unique_exprs;
  if (OB_ISNULL(stmt) || OB_ISNULL(index_schema) || OB_ISNULL(table = stmt->get_table_item_by_id(table_id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(stmt), K(index_schema));
  } else if (is_virtual_table(table->ref_id_)) {
  } else if (!index_schema->get_rowkey_info().is_valid()) {
    // do nothing
  } else if (OB_FAIL(index_schema->get_rowkey_info().get_column_ids(column_ids))) {
    LOG_WARN("failed to get index cols", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && all_columns_used && i < column_ids.count(); ++i) {
      ObRawExpr* col_expr = NULL;
      bool is_not_null = false;
      if (is_shadow_column(column_ids.at(i))) {
        continue;
      } else if (column_ids.at(i) == OB_HIDDEN_PK_PARTITION_COLUMN_ID ||
                 column_ids.at(i) == OB_HIDDEN_PK_CLUSTER_COLUMN_ID) {
        continue;  // do nothing
      } else if (OB_ISNULL(col_expr = stmt->get_column_expr_by_id(table_id, column_ids.at(i)))) {
        // column is not used by current stmt, skip
        all_columns_used = false;
      } else if (OB_FAIL(unique_exprs.push_back(col_expr))) {
        LOG_WARN("failed to push back unique col", K(ret));
      } else if (ObOptimizerUtil::find_item(not_null_columns, col_expr)) {
        // col_expr is not null
        contain_not_null = true;
        continue;
      } else if ((is_not_null = col_expr->is_not_null())) {
        // do nothing
      } else if (OB_FAIL(ObTransformUtils::has_null_reject_condition(quals, col_expr, is_not_null))) {
        LOG_WARN("failed to check whether has null reject condition", K(ret));
      }

      if (OB_FAIL(ret)) {
      } else if (!is_not_null) {
        all_not_null = false;
      } else if (OB_FAIL(not_null_columns.push_back(col_expr))) {
        LOG_WARN("failed to add not null col", K(ret));
      } else {
        contain_not_null = true;
      }
    }
    if (OB_SUCC(ret) && all_columns_used && unique_exprs.count() > 0) {
      ObTableFdItem* fd_item = NULL;
      if (OB_FAIL(fd_factory.create_table_fd_item(fd_item, true, unique_exprs, stmt->get_current_level(), tables))) {
        LOG_WARN("failed to create fd item", K(ret));
      } else if (all_not_null || (share::is_oracle_mode() && contain_not_null) ||
                 index_schema->get_table_id() == table->ref_id_) {
        if (OB_FAIL(fd_item_set.push_back(fd_item))) {
          LOG_WARN("failed to push back fd item", K(ret));
        }
      } else if (OB_FAIL(candi_fd_item_set.push_back(fd_item))) {
        LOG_WARN("failed to push back fd item", K(ret));
      }
    }
  }
  return ret;
}

int ObOptimizerUtil::convert_subplan_scan_equal_sets(ObIAllocator* allocator, ObRawExprFactory& expr_factory,
    const uint64_t table_id, ObDMLStmt& parent_stmt, const ObSelectStmt& child_stmt, const EqualSets& input_equal_sets,
    EqualSets& output_equal_sets)
{
  int ret = OB_SUCCESS;
  EqualSets dummy_equal_sets;
  ObSEArray<ObRawExpr*, 8> raw_eset;
  for (int64_t i = 0; OB_SUCC(ret) && i < input_equal_sets.count(); ++i) {
    const EqualSet* input_eset = input_equal_sets.at(i);
    raw_eset.reuse();
    if (OB_ISNULL(input_eset)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::convert_subplan_scan_expr(
                   expr_factory, dummy_equal_sets, table_id, parent_stmt, child_stmt, true, *input_eset, raw_eset))) {
      LOG_WARN("failed to convert subplan scan expr", K(ret));
    } else if (raw_eset.count() > 1 &&
               OB_FAIL(ObRawExprSetUtils::add_expr_set(allocator, raw_eset, output_equal_sets))) {
      LOG_WARN("failed to push back equal set", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObOptimizerUtil::convert_subplan_scan_fd_item_sets(ObFdItemFactory& fd_factory, ObRawExprFactory& expr_factory,
    const EqualSets& equal_sets, const uint64_t table_id, ObDMLStmt& parent_stmt, const ObSelectStmt& child_stmt,
    const ObFdItemSet& input_fd_item_sets, ObFdItemSet& output_fd_item_sets)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 8> new_parent_exprs;
  int32_t stmt_level = parent_stmt.get_current_level();
  ObRelIds tables;
  if (OB_FAIL(tables.add_member(parent_stmt.get_table_bit_index(table_id)))) {
    LOG_WARN("failed to add relid", K(ret), K(parent_stmt), K(table_id));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < input_fd_item_sets.count(); ++i) {
    ObRawExprSet* parent_set = NULL;
    const ObFdItem* old_fd_item = NULL;
    new_parent_exprs.reuse();
    if (OB_ISNULL(old_fd_item = input_fd_item_sets.at(i)) || OB_ISNULL(old_fd_item->get_parent_exprs())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(old_fd_item));
    } else if (OB_FAIL(ObOptimizerUtil::convert_subplan_scan_expr(expr_factory,
                   equal_sets,
                   table_id,
                   parent_stmt,
                   child_stmt,
                   false,
                   *old_fd_item->get_parent_exprs(),
                   new_parent_exprs))) {
      LOG_WARN("failed to convert subplan scan expr", K(ret));
    } else if (new_parent_exprs.empty()) {
      /*do nothing*/
    } else if (old_fd_item->is_unique()) {
      ObTableFdItem* table_fd_item = NULL;
      if (OB_FAIL(fd_factory.create_table_fd_item(table_fd_item, true, new_parent_exprs, stmt_level, tables))) {
        LOG_WARN("failed to create fd item", K(ret));
      } else if (OB_FAIL(output_fd_item_sets.push_back(table_fd_item))) {
        LOG_WARN("failed to push back fd item", K(ret));
      }
    } else {
      ObRawExpr* temp_expr = NULL;
      ObSEArray<ObRawExpr*, 8> child_select_exprs;
      ObSEArray<ObRawExpr*, 8> new_child_exprs;
      ObExprFdItem* expr_fd_item = NULL;
      if (OB_FAIL(child_stmt.get_select_exprs(child_select_exprs))) {
        LOG_WARN("failed to get select exprs", K(ret));
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && j < child_select_exprs.count(); ++j) {
          bool is_in_child = false;
          if (OB_FAIL(old_fd_item->check_expr_in_child(child_select_exprs.at(j), equal_sets, is_in_child))) {
            LOG_WARN("failed to check expr in child");
          } else if (!is_in_child) {
            /*do nothing*/
          } else if (NULL == (temp_expr = parent_stmt.get_column_expr_by_id(table_id, OB_APP_MIN_COLUMN_ID + j))) {
            /*do nothing*/
          } else if (OB_FAIL(new_child_exprs.push_back(temp_expr))) {
            LOG_WARN("failed to push back exprs", K(ret));
          } else { /*do nothing*/
          }
        }
        if (OB_SUCC(ret) && !new_child_exprs.empty()) {
          if (OB_FAIL(
                  fd_factory.create_expr_fd_item(expr_fd_item, false, new_parent_exprs, stmt_level, new_child_exprs))) {
            LOG_WARN("failed to create fd item", K(ret));
          } else if (OB_FAIL(output_fd_item_sets.push_back(expr_fd_item))) {
            LOG_WARN("failed to push back fd item", K(ret));
          } else { /*do nothing*/
          }
        }
      }
    }
  }
  return ret;
}

int ObOptimizerUtil::add_fd_item_set_for_left_join(ObFdItemFactory& fd_factory, const ObRelIds& right_tables,
    const ObIArray<ObRawExpr*>& right_join_exprs, const ObIArray<ObRawExpr*>& right_const_exprs,
    const EqualSets& right_equal_sets, const ObIArray<ObFdItem*>& right_fd_item_sets,
    const ObIArray<ObRawExpr*>& all_left_join_exprs, const EqualSets& left_equal_sets,
    const ObIArray<ObFdItem*>& left_fd_item_sets, const ObIArray<ObFdItem*>& left_candi_fd_item_sets,
    ObIArray<ObFdItem*>& fd_item_sets, ObIArray<ObFdItem*>& candi_fd_item_sets)
{
  int ret = OB_SUCCESS;
  bool right_is_unique = false;
  if (OB_FAIL(is_exprs_unique(
          right_join_exprs, right_tables, right_fd_item_sets, right_equal_sets, right_const_exprs, right_is_unique))) {
    LOG_WARN("failed to check is order unique", K(ret));
  } else if (right_is_unique) {
    if (OB_FAIL(add_fd_item_set_for_n21_join(
            fd_factory, fd_item_sets, left_fd_item_sets, all_left_join_exprs, left_equal_sets, right_tables))) {
      LOG_WARN("failed to add fd item set for n21 join", K(ret));
    } else if (OB_FAIL(add_fd_item_set_for_n21_join(fd_factory,
                   candi_fd_item_sets,
                   left_candi_fd_item_sets,
                   all_left_join_exprs,
                   left_equal_sets,
                   right_tables))) {
      LOG_WARN("failed to add fd item set for n21 join", K(ret));
    }
  } else if (OB_FAIL(add_fd_item_set_for_n2n_join(fd_factory, fd_item_sets, left_fd_item_sets))) {
    LOG_WARN("failed to add fd item set for n2n join", K(ret));
  } else if (OB_FAIL(add_fd_item_set_for_n2n_join(fd_factory, candi_fd_item_sets, left_candi_fd_item_sets))) {
    LOG_WARN("failed to add fd item set for n2n join", K(ret));
  }
  return ret;
}

int ObOptimizerUtil::check_need_sort(const ObIArray<OrderItem>& expected_order_items,
    const ObIArray<OrderItem>& input_ordering, const ObFdItemSet& fd_item_set, const EqualSets& equal_sets,
    const ObIArray<ObRawExpr*>& const_exprs, const bool is_at_most_one_row, bool& need_sort)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 6> expected_order_exprs;
  ObSEArray<ObOrderDirection, 6> expected_order_directions;
  if (OB_FAIL(split_expr_direction(expected_order_items, expected_order_exprs, expected_order_directions))) {
    LOG_WARN("failed to split expr and expected_order_directions", K(ret));
  } else if (OB_FAIL(check_need_sort(expected_order_exprs,
                 &expected_order_directions,
                 input_ordering,
                 fd_item_set,
                 equal_sets,
                 const_exprs,
                 is_at_most_one_row,
                 need_sort))) {
    LOG_WARN("failed to check need sort", K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObOptimizerUtil::check_need_sort(const ObIArray<ObRawExpr*>& expected_order_exprs,
    const ObIArray<ObOrderDirection>* expected_order_directions, const ObIArray<OrderItem>& input_ordering,
    const ObFdItemSet& fd_item_set, const EqualSets& equal_sets, const ObIArray<ObRawExpr*>& const_exprs,
    const bool is_at_most_one_row, bool& need_sort)
{
  int ret = OB_SUCCESS;
  need_sort = true;
  int64_t left_count = input_ordering.count();
  int64_t right_count = expected_order_exprs.count();
  int64_t l_idx = 0;
  int64_t r_idx = 0;
  ObSqlBitSet<> left_set;
  ObSqlBitSet<> right_set;
  ObSqlBitSet<> used_fd;
  ObSEArray<ObRawExpr*, 8> extend_exprs;
  ObSEArray<ObRawExpr*, 8> fd_set_parent_exprs;
  bool find_unique = false;
  bool is_match = true;
  bool left_is_const = false;
  bool right_is_const = false;
  if (is_at_most_one_row) {
    need_sort = false;
  } else {
    if (OB_FAIL(get_fd_set_parent_exprs(fd_item_set, fd_set_parent_exprs))) {
      LOG_WARN("failed to get fd set parent exprs ", K(ret));
    }
    while (OB_SUCC(ret) && is_match && !find_unique && r_idx < right_count) {
      if (l_idx < left_count &&
          (NULL == expected_order_directions ||
              input_ordering.at(l_idx).order_type_ == expected_order_directions->at(r_idx)) &&
          is_expr_equivalent(expected_order_exprs.at(r_idx), input_ordering.at(l_idx).expr_, equal_sets)) {
        ret = extend_exprs.push_back(expected_order_exprs.at(r_idx));
        ++l_idx;
        ++r_idx;
      } else if (l_idx < left_count &&
                 OB_FAIL(is_const_or_equivalent_expr(input_ordering, equal_sets, const_exprs, l_idx, left_is_const))) {
        LOG_WARN("failed to check is const or equivalent exprs", K(ret));
      } else if (OB_FAIL(is_const_or_equivalent_expr(
                     expected_order_exprs, equal_sets, const_exprs, r_idx, right_is_const))) {
        LOG_WARN("failed to check is const or equivalent exprs", K(ret));
      } else if (right_is_const || (l_idx < left_count && left_is_const)) {
        if (l_idx < left_count && left_is_const) {
          ++l_idx;
        }
        if (right_is_const) {
          ++r_idx;
        }
      } else {
        ObFdItem* fd_item = NULL;
        int64_t extend_exprs_count = -1;
        while (OB_SUCC(ret) && !find_unique && extend_exprs.count() > extend_exprs_count) {
          extend_exprs_count = extend_exprs.count();
          for (int64_t fd_idx = 0; OB_SUCC(ret) && !find_unique && fd_idx < fd_item_set.count(); ++fd_idx) {
            bool is_contain = false;
            if (OB_ISNULL(fd_item = fd_item_set.at(fd_idx))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("get null fd item", K(ret));
            } else if (used_fd.has_member(fd_idx)) {
              // do nothing
            } else if (OB_FAIL(
                           is_exprs_contain_fd_parent(extend_exprs, *fd_item, equal_sets, const_exprs, is_contain))) {
              LOG_WARN("faield to check is order unique", K(ret));
            } else if (!is_contain) {
              // do nothing
            } else if (fd_item->is_unique()) {
              find_unique = true;
            } else if (used_fd.add_member(fd_idx)) {
              LOG_WARN("failed to add member to set", K(ret));
            } else if (OB_FAIL(split_child_exprs(fd_item, equal_sets, fd_set_parent_exprs, extend_exprs))) {
              LOG_WARN("failed to delete members", K(ret));
            } else if (OB_FAIL(fd_item->check_exprs_in_child(expected_order_exprs, equal_sets, r_idx, right_set))) {
              LOG_WARN("failed to check exprs in child", K(ret));
            } else if (OB_FAIL(fd_item->check_exprs_in_child(input_ordering, equal_sets, l_idx, left_set))) {
              LOG_WARN("failed to check exprs in child", K(ret));
            }
          }
        }
        if (!find_unique && !left_set.has_member(l_idx) && !right_set.has_member(r_idx)) {
          is_match = false;
        }
      }
      while (left_set.has_member(l_idx)) {
        ++l_idx;
      };
      while (right_set.has_member(r_idx)) {
        ++r_idx;
      };
    }

    if (OB_SUCC(ret)) {
      if (!is_match) {
        need_sort = true;
      } else if (find_unique || expected_order_exprs.count() == r_idx) {
        need_sort = false;
      }
    }
  }
  LOG_TRACE("succeed to check need sort",
      K(need_sort),
      K(fd_item_set),
      K(equal_sets),
      K(const_exprs),
      K(is_at_most_one_row),
      K(expected_order_exprs),
      K(input_ordering),
      K(l_idx),
      K(r_idx),
      K(find_unique));
  return ret;
}

int ObOptimizerUtil::decide_sort_keys_for_merge_style_op(const ObIArray<OrderItem>& input_ordering,
    const ObFdItemSet& fd_item_set, const EqualSets& equal_sets, const ObIArray<ObRawExpr*>& const_exprs,
    const bool is_at_most_one_row, const ObIArray<ObRawExpr*>& merge_exprs,
    const ObIArray<ObOrderDirection>& merge_directions, MergeKeyInfo& merge_key)
{
  int ret = OB_SUCCESS;
  bool dummy_ordering_used = false;
  if (OB_FAIL(merge_key.order_exprs_.assign(merge_exprs))) {
    LOG_WARN("failed to assign exprs", K(ret));
  } else if (OB_FAIL(merge_key.order_directions_.assign(merge_directions))) {
    LOG_WARN("failed to assign exprs", K(ret));
  } else if (OB_FAIL(adjust_exprs_by_ordering(merge_key.order_exprs_,
                 input_ordering,
                 equal_sets,
                 const_exprs,
                 dummy_ordering_used,
                 merge_key.order_directions_,
                 &merge_key.map_array_))) {
    LOG_WARN("failed to adjust expr by ordering", K(ret));
  } else if (OB_FAIL(check_need_sort(merge_key.order_exprs_,
                 &merge_key.order_directions_,
                 input_ordering,
                 fd_item_set,
                 equal_sets,
                 const_exprs,
                 is_at_most_one_row,
                 merge_key.need_sort_))) {
    LOG_WARN("failed to check need sort", K(ret));
  } else if (OB_FAIL(make_sort_keys(merge_key.order_exprs_, merge_key.order_directions_, merge_key.order_items_))) {
    LOG_WARN("failed to make sort keys", K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObOptimizerUtil::flip_op_type(const ObItemType expr_type, ObItemType& rotated_expr_type)
{
  int ret = OB_SUCCESS;
  rotated_expr_type = T_INVALID;
  if (expr_type == T_OP_LE) {
    rotated_expr_type = T_OP_GE;
  } else if (expr_type == T_OP_LT) {
    rotated_expr_type = T_OP_GT;
  } else if (expr_type == T_OP_GE) {
    rotated_expr_type = T_OP_LE;
  } else if (expr_type == T_OP_GT) {
    rotated_expr_type = T_OP_LT;
  } else if (expr_type == T_OP_EQ || expr_type == T_OP_NE) {
    rotated_expr_type = expr_type;
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unsupported input expr type", K(ret));
  }
  return ret;
}

int ObOptimizerUtil::get_rownum_filter_info(ObRawExpr *rownum_cond,
                                            ObItemType &expr_type,
                                            ObRawExpr *&rownum_expr,
                                            ObRawExpr *&const_expr,
                                            bool &is_const_filter)
{
  int ret = OB_SUCCESS;
  is_const_filter = false;
  ObRawExpr *first_param = NULL;
  ObRawExpr *second_param = NULL;
  if (OB_ISNULL(rownum_cond)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (rownum_cond->is_op_expr() && 2 == rownum_cond->get_param_count() &&
             (IS_RANGE_CMP_OP(rownum_cond->get_expr_type())
              || T_OP_EQ == rownum_cond->get_expr_type())) {
    first_param = rownum_cond->get_param_expr(0);
    second_param = rownum_cond->get_param_expr(1);
    if (OB_ISNULL(first_param) || OB_ISNULL(second_param)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null expr", K(ret), K(first_param), K(second_param));
    } else if (first_param->has_flag(IS_ROWNUM) &&
               second_param->has_flag(IS_CONST) &&
               (second_param->get_result_type().is_int()
                || second_param->get_result_type().is_number())) {
      expr_type = rownum_cond->get_expr_type();
      rownum_expr = first_param;
      const_expr = second_param;
      is_const_filter = true;
    } else if (first_param->has_flag(IS_CONST) &&
               second_param->has_flag(IS_ROWNUM) &&
               (first_param->get_result_type().is_int()
                || first_param->get_result_type().is_number())) {
      if (OB_FAIL(flip_op_type(rownum_cond->get_expr_type(), expr_type))) {
        LOG_WARN("failed to retate rownum_expr type", K(ret));
      } else {
        rownum_expr = second_param;
        const_expr = first_param;
        is_const_filter = true;
      }
    }
  }
  return ret;
}

int ObOptimizerUtil::convert_rownum_filter_as_offset(ObRawExprFactory& expr_factory, ObSQLSessionInfo* session_info,
    const ObItemType filter_type, ObRawExpr* const_expr, ObRawExpr*& offset_int_expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(const_expr) || OB_ISNULL(session_info) ||
      OB_UNLIKELY(filter_type != T_OP_GE && filter_type != T_OP_GT) ||
      OB_UNLIKELY(!const_expr->get_result_type().is_integer_type() && !const_expr->get_result_type().is_number())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(const_expr), K(session_info), K(filter_type));
  } else if (T_OP_GT == filter_type) {
    if (OB_FAIL(floor_number_as_limit_offset_value(expr_factory, session_info, const_expr, offset_int_expr))) {
      LOG_WARN("failed to floor number as offset value", K(ret));
    }
  } else if (T_OP_GE == filter_type) {
    if (OB_FAIL(ceil_number_as_limit_offset_value(expr_factory, session_info, const_expr, offset_int_expr))) {
      LOG_WARN("failed to ceil number as offset value", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(offset_int_expr->formalize(session_info))) {
      LOG_WARN("failed to formalize offset int expr", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (session_info->use_static_typing_engine()) {
      // cast to int value in static typing engine.
      ObExprResType dst_type;
      dst_type.set_int();
      ObSysFunRawExpr* cast_expr = NULL;
      OZ(ObRawExprUtils::create_cast_expr(expr_factory, offset_int_expr, dst_type, cast_expr, session_info));
      CK(NULL != cast_expr);
      if (OB_SUCC(ret)) {
        offset_int_expr = cast_expr;
      }
    }
  }
  return ret;
}

int ObOptimizerUtil::convert_rownum_filter_as_limit(ObRawExprFactory& expr_factory, ObSQLSessionInfo* session_info,
    const ObItemType filter_type, ObRawExpr* const_expr, ObRawExpr*& limit_int_expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(const_expr) || OB_ISNULL(session_info) ||
      OB_UNLIKELY(filter_type != T_OP_LE && filter_type != T_OP_LT) ||
      OB_UNLIKELY(!const_expr->get_result_type().is_integer_type() && !const_expr->get_result_type().is_number() &&
                  !const_expr->get_result_type().is_null())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rownum expr is null", K(ret), K(const_expr), K(session_info), K(filter_type));
  } else if (filter_type == T_OP_LE) {
    if (OB_FAIL(floor_number_as_limit_offset_value(expr_factory, session_info, const_expr, limit_int_expr))) {
      LOG_WARN("failed to floor number as limit value", K(ret));
    }
  } else if (filter_type == T_OP_LT) {
    if (OB_FAIL(ceil_number_as_limit_offset_value(expr_factory, session_info, const_expr, limit_int_expr))) {
      LOG_WARN("failed to ceil number as limit value", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(limit_int_expr->formalize(session_info))) {
      LOG_WARN("failed to formalize limit int expr", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (session_info->use_static_typing_engine()) {
      // cast to int value in static typing engine.
      ObExprResType dst_type;
      dst_type.set_int();
      ObSysFunRawExpr* cast_expr = NULL;
      OZ(ObRawExprUtils::create_cast_expr(expr_factory, limit_int_expr, dst_type, cast_expr, session_info));
      CK(NULL != cast_expr);
      if (OB_SUCC(ret)) {
        limit_int_expr = cast_expr;
      }
    }
  }
  return ret;
}

int ObOptimizerUtil::floor_number_as_limit_offset_value(
    ObRawExprFactory& expr_factory, ObSQLSessionInfo* session_info, ObRawExpr* num_expr, ObRawExpr*& limit_int)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session_info) || OB_ISNULL(num_expr) ||
      OB_UNLIKELY(!num_expr->get_result_type().is_integer_type() && !num_expr->get_result_type().is_number() &&
                  !num_expr->get_result_type().is_null())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params are invalid", K(ret), K(session_info), K(num_expr));
  } else if (num_expr->get_result_type().is_integer_type()) {
    limit_int = num_expr;
  } else {
    ObSysFunRawExpr* floor_expr = NULL;
    if (OB_FAIL(expr_factory.create_raw_expr(T_FUN_SYS_FLOOR, floor_expr))) {
      LOG_WARN("failed to create fun sys floor", K(ret));
    } else if (OB_ISNULL(floor_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("floor expr is null", K(ret));
    } else if (OB_FAIL(floor_expr->set_param_expr(num_expr))) {
      LOG_WARN("failed to set param expr", K(ret));
    } else {
      floor_expr->set_func_name(ObString::make_string("FLOOR"));
      limit_int = floor_expr;
    }
  }
  return ret;
}

int ObOptimizerUtil::ceil_number_as_limit_offset_value(
    ObRawExprFactory& expr_factory, ObSQLSessionInfo* session_info, ObRawExpr* num_expr, ObRawExpr*& limit_int)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session_info) || OB_ISNULL(num_expr) ||
      OB_UNLIKELY(!num_expr->get_result_type().is_integer_type() && !num_expr->get_result_type().is_number() &&
                  !num_expr->get_result_type().is_null())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params are invalid", K(ret), K(session_info), K(num_expr));
  } else if (num_expr->get_result_type().is_number()) {
    ObSysFunRawExpr* ceil_expr = NULL;
    if (OB_FAIL(expr_factory.create_raw_expr(T_FUN_SYS_CEIL, ceil_expr))) {
      LOG_WARN("failed to create fun sys floor", K(ret));
    } else if (OB_ISNULL(ceil_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("floor expr is null", K(ret));
    } else if (OB_FAIL(ceil_expr->set_param_expr(num_expr))) {
      LOG_WARN("failed to set param expr", K(ret));
    } else {
      ceil_expr->set_func_name(ObString::make_string("CEIL"));
      num_expr = ceil_expr;
    }
  }

  if (OB_SUCC(ret) && OB_NOT_NULL(num_expr)) {
    ObRawExpr* minus_expr = NULL;
    ObConstRawExpr* const_one = NULL;
    if (OB_FAIL(ObRawExprUtils::build_const_int_expr(expr_factory, ObIntType, 1L, const_one))) {
      LOG_WARN("failed to build int expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::create_double_op_expr(
                   expr_factory, session_info, T_OP_MINUS, minus_expr, num_expr, const_one))) {
      LOG_WARN("failed to decrease const expr value", K(ret));
    } else {
      limit_int = minus_expr;
    }
  }
  return ret;
}

int ObOptimizerUtil::get_type_safe_join_exprs(const ObIArray<ObRawExpr*>& join_quals, const ObRelIds& left_tables,
    const ObRelIds& right_tables, ObIArray<ObRawExpr*>& left_exprs, ObIArray<ObRawExpr*>& right_exprs,
    ObIArray<ObRawExpr*>& all_left_exprs, ObIArray<ObRawExpr*>& all_right_exprs)
{
  int ret = OB_SUCCESS;
  ObRawExpr* cur_expr = NULL;
  ObRawExpr* first_expr = NULL;
  ObRawExpr* second_expr = NULL;
  ObRawExpr* left_expr = NULL;
  ObRawExpr* right_expr = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < join_quals.count(); ++i) {
    cur_expr = join_quals.at(i);
    if (OB_ISNULL(cur_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("join qual should not be NULL", K(cur_expr), K(ret));
    } else if (!cur_expr->has_flag(IS_JOIN_COND)) {
      // do nothing
    } else if (OB_ISNULL(first_expr = cur_expr->get_param_expr(0)) ||
               OB_ISNULL(second_expr = cur_expr->get_param_expr(1))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("join qual should not be NULL", K(*cur_expr), K(first_expr), K(second_expr), K(ret));
    } else if (T_OP_ROW == first_expr->get_expr_type() || T_OP_ROW == second_expr->get_expr_type()) {
      // do nothing
    } else if (first_expr->get_relation_ids().is_subset(left_tables) &&
               second_expr->get_relation_ids().is_subset(right_tables)) {
      left_expr = first_expr;
      right_expr = second_expr;
    } else if (first_expr->get_relation_ids().is_subset(right_tables) &&
               second_expr->get_relation_ids().is_subset(left_tables)) {
      right_expr = first_expr;
      left_expr = second_expr;
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(left_expr) && OB_NOT_NULL(right_expr)) {
      bool is_right_valid = false;
      bool is_left_valid = false;
      if (OB_FAIL(ObRelationalExprOperator::is_equivalent(left_expr->get_result_type(),
              right_expr->get_result_type(),
              right_expr->get_result_type(),
              is_right_valid))) {
        LOG_WARN("failed to check the compare type is the same", K(ret));
      } else if (OB_FAIL(ObRelationalExprOperator::is_equivalent(left_expr->get_result_type(),
                     left_expr->get_result_type(),
                     right_expr->get_result_type(),
                     is_left_valid))) {
        LOG_WARN("failed to check the compare type is the same", K(ret));
      } else if (is_left_valid && OB_FAIL(left_exprs.push_back(left_expr))) {
        LOG_WARN("failed to add left exprs", K(ret));
      } else if (is_right_valid && OB_FAIL(right_exprs.push_back(right_expr))) {
        LOG_WARN("failed to add right exprs", K(ret));
      } else if (OB_FAIL(all_left_exprs.push_back(left_expr))) {
        LOG_WARN("failed to push back left expr", K(ret));
      } else if (OB_FAIL(all_right_exprs.push_back(right_expr))) {
        LOG_WARN("failed to push back right expr", K(ret));
      }
    }
  }
  return ret;
}

int ObOptimizerUtil::split_or_qual_on_table(const ObDMLStmt* stmt, ObOptimizerContext& opt_ctx,
    const ObRelIds& table_ids, ObOpRawExpr& or_qual, ObOpRawExpr*& new_expr)
{
  int ret = OB_SUCCESS;
  bool is_valid = false;
  new_expr = NULL;
  ObSEArray<ObSEArray<ObRawExpr*, 16>, 8> sub_exprs;
  for (int64_t i = 0; OB_SUCC(ret) && i < or_qual.get_param_count(); ++i) {
    ObSEArray<ObRawExpr*, 16> exprs;
    if (OB_FAIL(sub_exprs.push_back(exprs))) {
      LOG_WARN("failed to push back se array", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(check_push_down_expr(table_ids, or_qual, sub_exprs, is_valid))) {
    LOG_WARN("failed to check push down expr", K(ret));
  } else if (!is_valid) {
    /* do nothing */
  } else if (OB_FAIL(generate_push_down_expr(stmt, opt_ctx, sub_exprs, new_expr))) {
    LOG_WARN("failed to generate push down expr", K(ret));
  }
  return ret;
}

int ObOptimizerUtil::check_push_down_expr(
    const ObRelIds& table_ids, ObOpRawExpr& or_qual, ObIArray<ObSEArray<ObRawExpr*, 16>>& sub_exprs, bool& all_contain)
{
  int ret = OB_SUCCESS;
  all_contain = true;
  for (int64_t i = 0; OB_SUCC(ret) && all_contain && i < or_qual.get_param_count(); ++i) {
    ObRawExpr* cur_expr = or_qual.get_param_expr(i);
    if (OB_ISNULL(cur_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr in or expr is null", K(ret));
    } else if (T_OP_AND == cur_expr->get_expr_type()) {
      ObOpRawExpr* and_expr = static_cast<ObOpRawExpr*>(cur_expr);
      for (int64_t j = 0; OB_SUCC(ret) && j < and_expr->get_param_count(); ++j) {
        ObRawExpr* cur_and_expr = and_expr->get_param_expr(j);
        if (OB_ISNULL(cur_and_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expr in and expr is null", K(ret));
        } else if (!cur_and_expr->has_flag(CNT_SUB_QUERY) && table_ids.is_superset(cur_and_expr->get_relation_ids()) &&
                   OB_FAIL(sub_exprs.at(i).push_back(cur_and_expr))) {
          LOG_WARN("failed to push back expr", K(ret));
        } else { /* do nothing */
        }
      }
      if (OB_FAIL(ret)) {
      } else if (sub_exprs.at(i).empty()) {
        all_contain = false;
      }
    } else if (cur_expr->has_flag(CNT_SUB_QUERY) || !table_ids.is_superset(cur_expr->get_relation_ids())) {
      all_contain = false;
    } else if (OB_FAIL(sub_exprs.at(i).push_back(cur_expr))) {
      LOG_WARN("failed to push back expr", K(ret));
    } else { /* do nothing */
    }
  }
  return ret;
}

int ObOptimizerUtil::generate_push_down_expr(const ObDMLStmt* stmt, ObOptimizerContext& opt_ctx,
    ObIArray<ObSEArray<ObRawExpr*, 16>>& sub_exprs, ObOpRawExpr*& new_expr)
{
  int ret = OB_SUCCESS;
  new_expr = NULL;
  ObRawExprFactory& expr_factory = opt_ctx.get_expr_factory();
  const ObSQLSessionInfo* session_info = opt_ctx.get_session_info();
  if (OB_ISNULL(session_info) || OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(session_info), K(stmt));
  } else if (OB_FAIL(expr_factory.create_raw_expr(T_OP_OR, new_expr))) {
    LOG_WARN("failed to create or expr", K(ret));
  } else if (OB_ISNULL(new_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < sub_exprs.count(); ++i) {
      ObIArray<ObRawExpr*>& cur_exprs = sub_exprs.at(i);
      const int64_t N = cur_exprs.count();
      if (1 == N) {
        if (OB_FAIL(new_expr->add_param_expr(cur_exprs.at(0)))) {
          LOG_WARN("failed to add param expr", K(ret));
        }
      } else {
        ObOpRawExpr* new_and_expr = NULL;
        if (OB_FAIL(expr_factory.create_raw_expr(T_OP_AND, new_and_expr))) {
          LOG_WARN("failed to create and expr", K(ret));
        } else if (OB_ISNULL(new_and_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("create expr get null", K(ret));
        }
        for (int64_t j = 0; OB_SUCC(ret) && j < N; ++j) {
          if (OB_FAIL(new_and_expr->add_param_expr(cur_exprs.at(j)))) {
            LOG_WARN("failed to add param expr", K(ret));
          }
        }
        if (OB_FAIL(ret)) {
          /* do nothing */
        } else if (OB_FAIL(new_and_expr->formalize(session_info))) {
          LOG_WARN("failed to formalize and expr", K(ret));
        } else if (OB_FAIL(new_and_expr->pull_relation_id_and_levels(stmt->get_current_level()))) {
          LOG_WARN("failed to pull relation id and levels", K(ret));
        } else if (OB_FAIL(new_expr->add_param_expr(new_and_expr))) {
          LOG_WARN("failed to add param expr", K(ret));
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
    /* do nothing */
  } else if (OB_FAIL(new_expr->formalize(session_info))) {
    LOG_WARN("failed to formalize or expr", K(ret));
  } else if (OB_FAIL(new_expr->pull_relation_id_and_levels(stmt->get_current_level()))) {
    LOG_WARN("failed to pull relation id and levels", K(ret));
  }
  return ret;
}

int ObOptimizerUtil::simplify_exprs(const ObFdItemSet fd_item_set, const EqualSets& equal_sets,
    const ObIArray<ObRawExpr*>& const_exprs, ObIArray<ObRawExpr*>& root_exprs, const ObIArray<ObRawExpr*>& candi_exprs)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObFdItem*, 8> unique_fd_items;
  ObSEArray<ObFdItem*, 8> non_unique_fd_items;
  ObFdItem* fd_item = NULL;
  ObRawExprSet* parent_exprs = NULL;
  int64_t min_parent_count = -1;
  ObRawExprSet* min_parent_exprs = NULL;
  bool is_contain = false;
  bool is_in_child = false;
  root_exprs.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < fd_item_set.count(); ++i) {
    if (OB_ISNULL(fd_item = fd_item_set.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null fd item", K(ret));
    } else if (fd_item->is_unique()) {
      if (OB_FAIL(unique_fd_items.push_back(fd_item))) {
        LOG_WARN("failed to push back fd item", K(ret));
      }
    } else if (OB_FAIL(non_unique_fd_items.push_back(fd_item))) {
      LOG_WARN("failed to push back fd item", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < unique_fd_items.count(); ++i) {
    if (OB_ISNULL(fd_item = unique_fd_items.at(i)) || OB_ISNULL(parent_exprs = fd_item->get_parent_exprs())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get get unexpect null", K(ret), K(fd_item));
    } else if (-1 == min_parent_count || parent_exprs->count() < min_parent_count) {
      if (OB_FAIL(is_exprs_contain_fd_parent(candi_exprs, *fd_item, equal_sets, const_exprs, is_contain))) {
        LOG_WARN("failed to check is order unique", K(ret));
      } else if (is_contain) {
        min_parent_exprs = parent_exprs;
        min_parent_count = parent_exprs->count();
      }
    }
  }
  if (OB_SUCC(ret) && NULL != min_parent_exprs) {
    int64_t expr_idx = -1;
    for (int64_t i = 0; OB_SUCC(ret) && i < min_parent_exprs->count(); ++i) {
      if (find_equal_expr(candi_exprs, min_parent_exprs->at(i), equal_sets, expr_idx)) {
        if (OB_FAIL(root_exprs.push_back(candi_exprs.at(expr_idx)))) {
          LOG_WARN("failed to push back root expr", K(ret));
        }
      } else { /* not find means min_parent_exprs->at(i) is const, do nothing */
      }
    }
  }
  if (OB_SUCC(ret) && NULL == min_parent_exprs) {
    ObSqlBitSet<> eliminate_set;
    for (int64_t i = 0;
         OB_SUCC(ret) && eliminate_set.num_members() < candi_exprs.count() && i < non_unique_fd_items.count();
         ++i) {
      if (OB_ISNULL(fd_item = non_unique_fd_items.at(i)) || OB_ISNULL(parent_exprs = fd_item->get_parent_exprs())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpect null", K(ret), K(fd_item), K(parent_exprs));
      } else if (OB_FAIL(is_exprs_contain_fd_parent(candi_exprs, *fd_item, equal_sets, const_exprs, is_contain))) {
        LOG_WARN("failed to check is order unique", K(ret));
      } else if (is_contain) {
        ObSEArray<ObRawExpr*, 8> new_root_exprs;
        bool eliminate_new = false;
        int64_t expr_idx = -1;
        for (int64_t j = 0; OB_SUCC(ret) && j < candi_exprs.count(); ++j) {
          if (eliminate_set.has_member(j)) {
            // do nothing
          } else if (OB_FAIL(fd_item->check_expr_in_child(candi_exprs.at(j), equal_sets, is_in_child))) {
            LOG_WARN("failed to check expr in child", K(ret));
          } else if (!is_in_child) {
            // do nothing
          } else if (OB_FAIL(eliminate_set.add_member(j))) {
            LOG_WARN("failed to add member", K(ret));
          } else {
            eliminate_new = true;
          }
        }
        for (int64_t j = 0; OB_SUCC(ret) && j < root_exprs.count(); ++j) {
          if (OB_FAIL(fd_item->check_expr_in_child(root_exprs.at(j), equal_sets, is_in_child))) {
            LOG_WARN("failed to check expr in child", K(ret));
          } else if (is_in_child) {
          } else if (OB_FAIL(new_root_exprs.push_back(root_exprs.at(j)))) {
            LOG_WARN("failed to push back root expr", K(ret));
          }
        }
        for (int64_t j = 0; OB_SUCC(ret) && j < parent_exprs->count(); ++j) {
          if (find_equal_expr(candi_exprs, parent_exprs->at(j), equal_sets, expr_idx)) {
            if (OB_FAIL(new_root_exprs.push_back(candi_exprs.at(expr_idx)))) {
              LOG_WARN("failed to push back root expr", K(ret));
            }
          }
        }

        if (OB_SUCC(ret) && (eliminate_new || new_root_exprs.count() < root_exprs.count())) {
          if (OB_FAIL(root_exprs.assign(new_root_exprs))) {
            LOG_WARN("failed to assign exprs", K(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret) && eliminate_set.num_members() < candi_exprs.count()) {
      for (int64_t i = 0; OB_SUCC(ret) && i < candi_exprs.count(); ++i) {
        if (eliminate_set.has_member(i)) {
          // do nothing
        } else if (OB_FAIL(root_exprs.push_back(candi_exprs.at(i)))) {
          LOG_WARN("failed to push back candi expr", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObOptimizerUtil::simplify_ordered_exprs(const ObFdItemSet& fd_item_set, const EqualSets& equal_sets,
    const ObIArray<ObRawExpr*>& const_exprs, ObIArray<OrderItem>& order_items, const ObIArray<OrderItem>& candi_items)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 8> candi_exprs;
  ObSEArray<ObOrderDirection, 8> directions;
  ObSEArray<ObRawExpr*, 8> order_exprs;
  if (OB_FAIL(split_expr_direction(candi_items, candi_exprs, directions))) {
    LOG_WARN("failed to split expr direction", K(ret));
  } else if (OB_FAIL(simplify_ordered_exprs(fd_item_set, equal_sets, const_exprs, order_exprs, candi_exprs))) {
    LOG_WARN("failed to simplify ordered exprs", K(ret));
  } else if (OB_FAIL(make_sort_keys(candi_items, order_exprs, order_items))) {
    LOG_WARN("failed to make sort keys", K(ret));
  }
  return ret;
}

int ObOptimizerUtil::simplify_ordered_exprs(const ObFdItemSet& fd_item_set, const EqualSets& equal_sets,
    const ObIArray<ObRawExpr*>& const_exprs, ObIArray<ObRawExpr*>& order_exprs, const ObIArray<ObRawExpr*>& candi_exprs)
{
  int ret = OB_SUCCESS;
  ObSqlBitSet<> eliminate_set;
  ObSqlBitSet<> checked_fd_item;
  ObFdItem* fd_item = NULL;
  bool is_contain = false;
  bool is_in_child = false;
  bool is_const = false;
  order_exprs.reset();
  ObSEArray<ObRawExpr*, 8> extended_order_exprs;
  ObSEArray<ObRawExpr*, 8> fd_set_parent_exprs;
  if (OB_FAIL(get_fd_set_parent_exprs(fd_item_set, fd_set_parent_exprs))) {
    LOG_WARN("failed to get fd set parent exprs ", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < candi_exprs.count(); ++i) {
    ObRawExpr* expr = NULL;
    if (OB_ISNULL(expr = candi_exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null fd item", K(ret));
    } else if (eliminate_set.has_member(i)) {
      /*do nothing*/
    } else if (OB_FAIL(is_const_or_equivalent_expr(candi_exprs, equal_sets, const_exprs, i, is_const))) {
      LOG_WARN("failed to check is const expr", K(ret));
    } else if (is_const) {
      /*do nothing*/
    } else if (OB_FAIL(order_exprs.push_back(expr))) {
      LOG_WARN("failed to push back exprs", K(ret));
    } else if (OB_FAIL(extended_order_exprs.push_back(expr))) {
      LOG_WARN("failed to push back exprs", K(ret));
    } else if (OB_FAIL(remove_item(fd_set_parent_exprs, expr))) {
      LOG_WARN("failed to remove expr", K(ret));
    } else {
      int64_t last_count = -1;
      while (extended_order_exprs.count() > last_count) {
        last_count = extended_order_exprs.count();
        for (int64_t fd_idx = 0; OB_SUCC(ret) && fd_idx < fd_item_set.count(); ++fd_idx) {
          if (checked_fd_item.has_member(fd_idx)) {
            /*do nothing*/
          } else if (OB_ISNULL(fd_item = fd_item_set.at(fd_idx))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get null fd item", K(ret));
          } else if (OB_FAIL(is_exprs_contain_fd_parent(
                         extended_order_exprs, *fd_item, equal_sets, const_exprs, is_contain))) {
            LOG_WARN("failed to check is exprs contain fd parent", K(ret));
          } else if (is_contain) {
            for (int64_t j = i + 1; OB_SUCC(ret) && j < candi_exprs.count(); ++j) {
              if (eliminate_set.has_member(j)) {
                /*do nothing*/
              } else if (OB_FAIL(fd_item->check_expr_in_child(candi_exprs.at(j), equal_sets, is_in_child))) {
                LOG_WARN("failed to check expr in fd child", K(ret));
              } else if (is_in_child && !candi_exprs.at(j)->has_flag(CNT_SUB_QUERY)) {
                ret = eliminate_set.add_member(j);
              }
            }
            if (OB_FAIL(ret)) {
              /*do nothing*/
            } else if (checked_fd_item.add_member(fd_idx)) {
              LOG_WARN("failed to add member", K(ret));
            } else if (OB_FAIL(split_child_exprs(fd_item, equal_sets, fd_set_parent_exprs, extended_order_exprs))) {
              LOG_WARN("failed to delete members", K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObOptimizerUtil::remove_equal_exprs(
    const EqualSets& equal_sets, const ObIArray<ObRawExpr*>& target_exprs, ObIArray<ObRawExpr*>& exprs)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 8> other_exprs;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
    if (find_equal_expr(target_exprs, exprs.at(i), equal_sets)) {
      /*do nothing*/
    } else if (other_exprs.push_back(exprs.at(i))) {
      LOG_WARN("failed to push back exprs", K(ret));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(exprs.assign(other_exprs))) {
    LOG_WARN("failed to assign exprs", K(ret));
  }
  return ret;
}

int ObOptimizerUtil::check_subquery_filter(const JoinedTable* table, bool& has)
{
  int ret = OB_SUCCESS;
  has = false;
  if (OB_ISNULL(table) || OB_ISNULL(table->left_table_) || OB_ISNULL(table->right_table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table is null", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && !has && i < table->get_join_conditions().count(); ++i) {
    const ObRawExpr* expr = NULL;
    if (OB_ISNULL(expr = table->get_join_conditions().at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is null", K(ret), K(expr));
    } else {
      has = expr->has_flag(CNT_SUB_QUERY);
    }
  }
  if (OB_SUCC(ret) && !has && table->left_table_->is_joined_table()) {
    if (OB_FAIL(check_subquery_filter(static_cast<const JoinedTable*>(table->left_table_), has))) {
      LOG_WARN("failed to check subquery filter", K(ret));
    }
  }
  if (OB_SUCC(ret) && !has && table->right_table_->is_joined_table()) {
    if (OB_FAIL(check_subquery_filter(static_cast<const JoinedTable*>(table->right_table_), has))) {
      LOG_WARN("failed to check subquery filter", K(ret));
    }
  }
  return ret;
}

bool ObOptimizerUtil::has_equal_join_conditions(const ObIArray<ObRawExpr*>& join_conditions)
{
  int bret = false;
  for (int64_t i = 0; !bret && i < join_conditions.count(); i++) {
    if (OB_NOT_NULL(join_conditions.at(i))) {
      bret = join_conditions.at(i)->has_flag(IS_JOIN_COND);
    }
  }
  return bret;
}

int ObOptimizerUtil::convert_subplan_scan_expr(ObRawExprFactory& expr_factory, const EqualSets& equal_sets,
    const uint64_t table_id, ObDMLStmt& parent_stmt, const ObSelectStmt& child_stmt, bool skip_invalid,
    const ObIArray<ObRawExpr*>& input_exprs, ObIArray<ObRawExpr*>& output_exprs)
{
  int ret = OB_SUCCESS;
  bool is_valid = true;
  ObSEArray<ObRawExpr*, 8> temp_exprs;
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < input_exprs.count(); i++) {
    ObRawExpr* expr = NULL;
    if (OB_ISNULL(input_exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null expr", K(input_exprs.at(i)), K(ret));
    } else if (OB_FAIL(convert_subplan_scan_expr(
                   expr_factory, equal_sets, table_id, parent_stmt, child_stmt, input_exprs.at(i), expr))) {
      LOG_WARN("failed to generate subplan scan expr", K(ret));
    } else if (OB_ISNULL(expr)) {
      if (!skip_invalid) {
        is_valid = false;
      }
    } else if (OB_FAIL(add_var_to_array_no_dup(temp_exprs, expr))) {
      LOG_WARN("failed to push back expr", K(ret));
    } else { /*do nothing*/
    }
  }
  if (OB_SUCC(ret) && is_valid && !temp_exprs.empty()) {
    if (OB_FAIL(output_exprs.assign(temp_exprs))) {
      LOG_WARN("failed to push back exprs", K(ret));
    } else {
      LOG_TRACE("succeed to convert subplan scan expr", K(input_exprs), K(output_exprs));
    }
  }
  return ret;
}

int ObOptimizerUtil::convert_subplan_scan_expr(ObRawExprFactory& expr_factory, const EqualSets& equal_sets,
    const uint64_t table_id, ObDMLStmt& parent_stmt, const ObSelectStmt& child_stmt, const ObRawExpr* input_expr,
    ObRawExpr*& output_expr)
{
  int ret = OB_SUCCESS;
  bool is_valid = true;
  output_expr = NULL;
  if (OB_FAIL(check_subplan_scan_expr_validity(equal_sets, table_id, parent_stmt, child_stmt, input_expr, is_valid))) {
    LOG_WARN("failed to check subplan scan expr validity", K(ret));
  } else if (!is_valid) {
    /*do nothing*/
  } else if (OB_FAIL(get_parent_stmt_expr(equal_sets, table_id, parent_stmt, child_stmt, input_expr, output_expr))) {
    LOG_WARN("failed to get parent stmt expr", K(ret));
  } else if (NULL != output_expr) {
    /*
     * this branch try to optimize the deep copy of input expr,
     * if we can find the root expr in child stmt, then we should directly get the parent expr
     */
  } else if (OB_FAIL(ObRawExprUtils::copy_expr(expr_factory, input_expr, output_expr, COPY_REF_DEFAULT))) {
    LOG_WARN("failed to copy expr", K(ret));
  } else if (OB_ISNULL(output_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to copy expr", K(ret));
  } else if (OB_FAIL(replace_subplan_scan_expr(equal_sets, table_id, parent_stmt, child_stmt, output_expr))) {
    LOG_WARN("failed to do inner convert subplan scan expr", K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObOptimizerUtil::replace_subplan_scan_expr(const EqualSets& equal_sets, const uint64_t table_id,
    ObDMLStmt& parent_stmt, const ObSelectStmt& child_stmt, ObRawExpr*& expr)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  ObRawExpr* parent_expr = NULL;
  if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("check stack overflow failed", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret));
  } else if (expr->is_const_expr()) {
    /*do nothing*/
  } else if (OB_FAIL(get_parent_stmt_expr(equal_sets, table_id, parent_stmt, child_stmt, expr, parent_expr))) {
    LOG_WARN("failed to get output expr idx", K(ret));
  } else if (NULL != parent_expr) {
    expr = parent_expr;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); i++) {
      if (OB_ISNULL(expr->get_param_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null expr", K(ret));
      } else if (OB_FAIL(SMART_CALL(replace_subplan_scan_expr(
                     equal_sets, table_id, parent_stmt, child_stmt, expr->get_param_expr(i))))) {
        LOG_WARN("failed to check subplan scan expr validity", K(ret));
      }
    }
  }
  return ret;
}

int ObOptimizerUtil::check_subplan_scan_expr_validity(const EqualSets& equal_sets, const uint64_t table_id,
    ObDMLStmt& parent_stmt, const ObSelectStmt& child_stmt, const ObRawExpr* input_expr, bool& is_valid)
{
  int ret = OB_SUCCESS;
  ObRawExpr* parent_expr = NULL;
  bool is_stack_overflow = false;
  is_valid = false;
  if (OB_ISNULL(input_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("check stack overflow failed", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret));
  } else if (input_expr->is_const_expr()) {
    is_valid = true;
  } else if (OB_FAIL(get_parent_stmt_expr(equal_sets, table_id, parent_stmt, child_stmt, input_expr, parent_expr))) {
    LOG_WARN("failed to get parent stmt expr", K(ret));
  } else if (NULL != parent_expr) {
    is_valid = true;
  } else if (ObRawExprUtils::is_sharable_expr(*input_expr)) {
    is_valid = false;
  } else if (input_expr->get_param_count() == 0) {
    is_valid = false;
  } else {
    is_valid = true;
    for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < input_expr->get_param_count(); i++) {
      bool temp_valid = false;
      if (OB_ISNULL(input_expr->get_param_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null expr", K(ret));
      } else if (OB_FAIL(SMART_CALL(check_subplan_scan_expr_validity(
                     equal_sets, table_id, parent_stmt, child_stmt, input_expr->get_param_expr(i), temp_valid)))) {
        LOG_WARN("failed to check subplan scan expr validity", K(ret));
      } else {
        is_valid &= temp_valid;
      }
    }
  }
  return ret;
}

int ObOptimizerUtil::get_parent_stmt_expr(const EqualSets& equal_sets, const uint64_t table_id, ObDMLStmt& parent_stmt,
    const ObSelectStmt& child_stmt, const ObRawExpr* child_expr, ObRawExpr*& parent_expr)
{
  int ret = OB_SUCCESS;
  parent_expr = NULL;
  if (OB_ISNULL(child_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && NULL == parent_expr && i < child_stmt.get_select_item_size(); ++i) {
      const ObRawExpr* expr = child_stmt.get_select_item(i).expr_;
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(expr), K(ret));
      } else if (ObOptimizerUtil::is_expr_equivalent(expr, child_expr, equal_sets)) {
        // parent_expr may be null, just ignore
        parent_expr = parent_stmt.get_column_expr_by_id(table_id, OB_APP_MIN_COLUMN_ID + i);
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObOptimizerUtil::compute_stmt_interesting_order(const ObIArray<OrderItem>& ordering, const ObDMLStmt* stmt,
    const bool in_subplan_scan, EqualSets& equal_sets, const ObIArray<ObRawExpr*>& const_exprs,
    const int64_t check_scope, int64_t& match_info, int64_t* max_prefix_count_ptr)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> ordering_exprs;
  ObSEArray<ObOrderDirection, 4> ordering_directions;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (check_scope == OrderingCheckScope::NOT_CHECK) {
    // do nothing
  } else if (OB_FAIL(split_expr_direction(ordering, ordering_exprs, ordering_directions))) {
    LOG_WARN("failed to split expr direction", K(ret));
  } else {
    int64_t prefix_count = 0;
    int64_t max_prefix_count = 0;
    const ObSelectStmt* select_stmt = NULL;
    bool check_group = (check_scope & OrderingCheckScope::CHECK_GROUP) > 0;
    bool check_winfunc = (check_scope & OrderingCheckScope::CHECK_WINFUNC) > 0;
    bool check_distinct = (check_scope & OrderingCheckScope::CHECK_DISTINCT) > 0;
    bool check_set = (check_scope & OrderingCheckScope::CHECK_SET) > 0;
    bool check_order = (check_scope & OrderingCheckScope::CHECK_ORDERBY) > 0;
    bool has_group = false;
    bool has_distinct = false;
    bool has_winfunc = false;
    bool has_orderby = stmt->has_order_by();
    bool winfunc_require_sort = false;
    bool group_match = false;
    bool winfunc_match = false;
    bool distinct_match = false;
    bool set_match = false;
    bool orderby_match = false;
    if (stmt->is_select_stmt()) {
      select_stmt = static_cast<const ObSelectStmt*>(stmt);
      has_group = select_stmt->get_group_expr_size() > 0 || select_stmt->get_rollup_expr_size() > 0;
      has_distinct = select_stmt->has_distinct();
      has_winfunc = select_stmt->has_window_function();
    }

    if (has_group && check_group) {
      prefix_count = 0;
      if (OB_FAIL(is_group_by_match(ordering, select_stmt, equal_sets, const_exprs, prefix_count, group_match))) {
        LOG_WARN("failed to check is group by match", K(ret));
      } else if (group_match) {
        max_prefix_count = std::max(max_prefix_count, prefix_count);
        match_info |= OrderingFlag::GROUP_MATCH;
        LOG_TRACE("ordering is math group by", K(max_prefix_count), K(prefix_count));
      }
    } else if (has_winfunc && check_winfunc) {
      prefix_count = 0;
      if (OB_FAIL(is_winfunc_match(
              ordering, select_stmt, equal_sets, const_exprs, prefix_count, winfunc_match, winfunc_require_sort))) {
        LOG_WARN("failed to check is winfunc match", K(ret));
      } else if (winfunc_match) {
        max_prefix_count = std::max(max_prefix_count, prefix_count);
        match_info |= OrderingFlag::WINFUNC_MATCH;
        LOG_TRACE("ordering is match window function", K(max_prefix_count), K(prefix_count));
      }
    }
    if (OB_SUCC(ret) && (!check_group || !has_group) && !winfunc_require_sort) {
      if (has_distinct && check_distinct) {
        prefix_count = 0;
        if (OB_FAIL(is_distinct_match(
                ordering_exprs, select_stmt, equal_sets, const_exprs, prefix_count, distinct_match))) {
          LOG_WARN("failed to check is distinct match", K(ret));
        } else if (distinct_match) {
          max_prefix_count = std::max(max_prefix_count, prefix_count);
          match_info |= OrderingFlag::DISTINCT_MATCH;
          LOG_TRACE("ordering is math distinct", K(max_prefix_count), K(prefix_count));
        }
      } else if (check_set) {
        prefix_count = 0;
        if (NULL != select_stmt && select_stmt->is_parent_set_distinct()) {
          if (OB_FAIL(is_set_match(ordering_exprs, select_stmt, equal_sets, const_exprs, prefix_count, set_match))) {
            LOG_WARN("failed to check is set match", K(ret));
          } else if (set_match) {
            max_prefix_count = std::max(max_prefix_count, prefix_count);
            match_info |= OrderingFlag::SET_MATCH;
            LOG_TRACE("ordering is match set", K(max_prefix_count), K(prefix_count));
          }
        }
      }
      /**
       * stmt without groupby (not just select)
       */
      if (OB_SUCC(ret) && has_orderby && check_order) {
        prefix_count = 0;
        if (OB_FAIL(is_order_by_match(ordering, stmt, equal_sets, const_exprs, prefix_count, orderby_match))) {
          LOG_WARN("failed to check is order by match", K(ret));
        } else if (orderby_match) {
          max_prefix_count = std::max(max_prefix_count, prefix_count);
          match_info |= OrderingFlag::ORDERBY_MATCH;
          LOG_TRACE("ordering is math order by", K(max_prefix_count), K(prefix_count));
        }
      }
    }
    if (OB_SUCC(ret) && NULL != max_prefix_count_ptr) {
      *max_prefix_count_ptr = max_prefix_count;
    }
    if (OB_SUCC(ret) && in_subplan_scan && !ordering_exprs.empty()) {
      if ((check_group && has_group && !group_match) || (check_winfunc && winfunc_require_sort) ||
          (check_distinct && has_distinct && !distinct_match) || (check_order && has_orderby && !orderby_match)) {
        // do nothing
      } else {
        match_info |= OrderingFlag::POTENTIAL_MATCH;
      }
    }
  }
  return ret;
}

int ObOptimizerUtil::is_group_by_match(const ObIArray<OrderItem>& ordering, const ObSelectStmt* select_stmt,
    const EqualSets& equal_sets, const ObIArray<ObRawExpr*>& const_exprs, int64_t& match_prefix_count, bool& sort_match)
{
  int ret = OB_SUCCESS;
  match_prefix_count = 0;
  sort_match = false;
  int64_t match_count = 0;
  bool full_covered = true;
  ObSEArray<ObRawExpr*, 4> ordering_exprs;
  ObSEArray<ObOrderDirection, 4> ordering_directions;
  if (OB_ISNULL(select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is invalid", K(ret), K(select_stmt));
  } else if (OB_FAIL(split_expr_direction(ordering, ordering_exprs, ordering_directions))) {
    LOG_WARN("failed to split expr direction", K(ret));
  } else if (select_stmt->get_group_expr_size() > 0 && OB_FAIL(prefix_subset_exprs(select_stmt->get_group_exprs(),
                                                           ordering_exprs,
                                                           equal_sets,
                                                           const_exprs,
                                                           full_covered,
                                                           &match_count))) {
    LOG_WARN("check is covered by ordering failed", K(ret));
  } else if (full_covered && select_stmt->get_rollup_expr_size() > 0) {
    ObSEArray<OrderItem, 4> mock_ordering;
    for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt->get_rollup_expr_size(); ++i) {
      OrderItem order_item(select_stmt->get_rollup_exprs().at(i));
      if (OB_FAIL(mock_ordering.push_back(order_item))) {
        LOG_WARN("failed to push back order item", K(ret));
      }
    }
    if (OB_SUCC(ret) &&
        OB_FAIL(match_order_by_against_index(
            mock_ordering, ordering, match_count, equal_sets, const_exprs, full_covered, match_count, false))) {
      LOG_WARN("failed to check match order by against index", K(ret));
    }
  }
  if (OB_SUCC(ret) && match_count > 0) {
    sort_match = true;
    match_prefix_count = match_count;
  }
  return ret;
}

int ObOptimizerUtil::is_winfunc_match(const ObIArray<OrderItem>& ordering, const ObSelectStmt* select_stmt,
    const EqualSets& equal_sets, const ObIArray<ObRawExpr*>& const_exprs, int64_t& match_prefix_count, bool& sort_match,
    bool& sort_is_required)
{
  int ret = OB_SUCCESS;
  match_prefix_count = 0;
  sort_is_required = false;
  if (OB_ISNULL(select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("select_stmt is null", K(ret), K(select_stmt));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt->get_window_func_count(); ++i) {
      int64_t match_count = 0;
      bool win_func_sort_match = false;
      bool win_func_require_sort = false;
      if (OB_FAIL(is_winfunc_match(ordering,
              select_stmt,
              select_stmt->get_window_func_expr(i),
              equal_sets,
              const_exprs,
              match_count,
              win_func_sort_match,
              win_func_require_sort))) {
        LOG_WARN("failed to check is winfunc match", K(ret));
      } else {
        sort_is_required |= win_func_require_sort;
        if (win_func_sort_match) {
          sort_match = true;
          if (match_count > match_prefix_count) {
            match_prefix_count = match_count;
          }
        }
      }
    }
  }
  return ret;
}

int ObOptimizerUtil::is_winfunc_match(const ObIArray<OrderItem>& ordering, const ObSelectStmt* select_stmt,
    const ObWinFunRawExpr* win_expr, const EqualSets& equal_sets, const ObIArray<ObRawExpr*>& const_exprs,
    int64_t& match_prefix_count, bool& sort_match, bool& sort_is_required)
{
  int ret = OB_SUCCESS;
  match_prefix_count = 0;
  sort_match = false;
  sort_is_required = false;
  int64_t partition_match_count = 0;
  int64_t order_match_count = 0;
  bool full_covered = false;
  ObSEArray<ObRawExpr*, 4> ordering_exprs;
  ObSEArray<ObOrderDirection, 4> ordering_directions;
  if (OB_ISNULL(select_stmt) || OB_ISNULL(win_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("input parameters have null", K(ret), K(select_stmt), K(win_expr));
  } else if (OB_FAIL(split_expr_direction(ordering, ordering_exprs, ordering_directions))) {
    LOG_WARN("failed to split expr direction", K(ret));
  } else if (OB_FAIL(prefix_subset_exprs(win_expr->get_partition_exprs(),
                 ordering_exprs,
                 equal_sets,
                 const_exprs,
                 full_covered,
                 &partition_match_count))) {
    LOG_WARN("check is covered by ordering failed", K(ret));
  } else if (!full_covered) {
    sort_is_required = true;
  } else if (OB_FAIL(match_order_by_against_index(win_expr->get_order_items(),
                 ordering,
                 partition_match_count,
                 equal_sets,
                 const_exprs,
                 full_covered,
                 order_match_count))) {
    LOG_WARN("failed to match order by against index", K(ret));
  } else if (!full_covered) {
    sort_is_required = true;
  }
  if (OB_SUCC(ret) && (partition_match_count > 0 || order_match_count > 0)) {
    match_prefix_count = order_match_count > 0 ? order_match_count : partition_match_count;
    sort_match = true;
  }
  return ret;
}

int ObOptimizerUtil::match_order_by_against_index(const ObIArray<OrderItem>& expect_ordering,
    const ObIArray<OrderItem>& input_ordering, const int64_t input_start_offset, const EqualSets& equal_sets,
    const ObIArray<ObRawExpr*>& const_exprs, bool& full_covered, int64_t& match_count,
    bool check_direction /* = true */)
{
  int ret = OB_SUCCESS;
  int64_t expect_offset = expect_ordering.count();
  int64_t input_offset = input_start_offset;
  bool is_const = true;
  full_covered = false;
  match_count = input_start_offset;
  for (int64_t i = 0; OB_SUCC(ret) && is_const && i < expect_ordering.count(); ++i) {
    if (OB_FAIL(is_const_expr(expect_ordering.at(i).expr_, equal_sets, const_exprs, is_const))) {
      LOG_WARN("failed to check is_const_expr", K(ret));
    } else if (!is_const) {
      expect_offset = i;
    }
  }
  ObRawExpr* input_expr = NULL;
  ObRawExpr* expect_expr = NULL;
  bool direction_match = false;
  while (OB_SUCC(ret) && input_offset < input_ordering.count() && expect_offset < expect_ordering.count()) {
    direction_match = check_direction ? is_ascending_direction(expect_ordering.at(expect_offset).order_type_) ==
                                            is_ascending_direction(input_ordering.at(input_offset).order_type_)
                                      : true;

    if (OB_ISNULL(input_expr = input_ordering.at(input_offset).expr_) ||
        OB_ISNULL(expect_expr = expect_ordering.at(expect_offset).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(input_expr), K(expect_expr));
    } else if (is_expr_equivalent(input_expr, expect_expr, equal_sets) && direction_match) {
      match_count = ++input_offset;
      ++expect_offset;
    } else if (OB_FAIL(is_const_expr(expect_expr, equal_sets, const_exprs, is_const))) {
      LOG_WARN("failed to check expr is const expr", K(ret));
    } else if (is_const) {
      ++expect_offset;
    } else if (OB_FAIL(is_const_expr(input_expr, equal_sets, const_exprs, is_const))) {
      LOG_WARN("failed to check expr is const expr", K(ret));
    } else if (is_const) {
      ++input_offset;
    } else {
      break;
    }
  }
  if (OB_SUCC(ret)) {
    full_covered = (expect_offset == expect_ordering.count());
  }
  return ret;
}

int ObOptimizerUtil::is_set_or_distinct_match(const ObIArray<ObRawExpr*>& keys, const ObSelectStmt* select_stmt,
    const EqualSets& equal_sets, const ObIArray<ObRawExpr*>& const_exprs, int64_t& match_prefix_count, bool& sort_match)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is invalid", K(ret));
  } else {
    ObSEArray<ObRawExpr*, 16> distinct_exprs;
    sort_match = false;
    match_prefix_count = 0;
    if (OB_FAIL(select_stmt->get_select_exprs(distinct_exprs))) {
      LOG_WARN("failed to get select exprs", K(ret));
    } else {
      bool full_covered = false;
      int64_t match_count = 0;
      if (OB_FAIL(prefix_subset_exprs(distinct_exprs, keys, equal_sets, const_exprs, full_covered, &match_count))) {
        LOG_WARN("check is covered by ordering failed", K(ret));
      } else if (match_count > 0) {
        sort_match = true;  // only consider prefix
        match_prefix_count = match_count;
      }
    }
  }
  return ret;
}

/**
 * create table t1(c1 int primary key);
 * select distinct(c1), c2 from t1
 */
int ObOptimizerUtil::is_distinct_match(const ObIArray<ObRawExpr*>& keys, const ObSelectStmt* stmt,
    const EqualSets& equal_sets, const ObIArray<ObRawExpr*>& const_exprs, int64_t& match_prefix_count, bool& sort_match)
{
  int ret = OB_SUCCESS;
  sort_match = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt should not be null", K(ret));
  } else if (stmt->has_distinct()) {
    if (OB_FAIL(is_set_or_distinct_match(keys, stmt, equal_sets, const_exprs, match_prefix_count, sort_match))) {
      LOG_WARN("check is_set_or_distinct_match failed", K(ret));
    }
  }
  return ret;
}

/**
 * (select c1 from t1) union (select c2 from t2);
 * the order of c1 can be use by union
 */
int ObOptimizerUtil::is_set_match(const ObIArray<ObRawExpr*>& keys, const ObSelectStmt* stmt,
    const EqualSets& equal_sets, const ObIArray<ObRawExpr*>& const_exprs, int64_t& match_prefix_count, bool& sort_match)
{
  return is_set_or_distinct_match(keys, stmt, equal_sets, const_exprs, match_prefix_count, sort_match);
}

// get max prefix
int ObOptimizerUtil::is_order_by_match(const ObIArray<OrderItem>& ordering, const ObDMLStmt* stmt,
    const EqualSets& equal_sets, const ObIArray<ObRawExpr*>& const_exprs, int64_t& match_prefix, bool& sort_match)
{
  int ret = OB_SUCCESS;
  int64_t match_count = 0;
  bool dummy_full_covered = false;
  match_prefix = 0;
  sort_match = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt should not be null", K(ret));
  } else if (OB_FAIL(match_order_by_against_index(stmt->get_order_items(),
                 ordering,
                 0,  // input ordering offser
                 equal_sets,
                 const_exprs,
                 dummy_full_covered,
                 match_count))) {
    LOG_WARN("failed to match order by against index", K(ret));
  } else if (match_count > 0) {
    sort_match = true;
    match_prefix = match_count;
  }
  LOG_TRACE("is_orderby match", K(ret), K(match_prefix), K(sort_match));
  return ret;
}

int ObOptimizerUtil::is_lossless_column_cast(const ObRawExpr* expr, bool& is_lossless)
{
  int ret = OB_SUCCESS;
  is_lossless = false;
  const ObRawExpr* child_expr = NULL;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (T_FUN_SYS_CAST != expr->get_expr_type()) {
    // do nothing
  } else if (OB_ISNULL(child_expr = expr->get_param_expr(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (child_expr->is_column_ref_expr()) {
    ObExprResType column_type = child_expr->get_result_type();
    ObObjTypeClass column_tc = column_type.get_type_class();
    ObExprResType dst_type = expr->get_result_type();
    ObObjTypeClass dst_tc = dst_type.get_type_class();
    ObAccuracy dst_acc = dst_type.get_accuracy();
    if (!is_oracle_mode()) {
      if (ObIntTC == column_tc || ObUIntTC == column_tc) {
        if (ObNumberTC == dst_tc) {
          // ObAccuracy lossless_acc =
          // ObAccuracy::DDL_DEFAULT_ACCURACY2[ObCompatibilityMode::MYSQL_MODE][column_type.get_type()];
          ObAccuracy lossless_acc = column_type.get_accuracy();
          if (dst_acc.get_scale() >= 0 &&
              dst_acc.get_precision() - dst_acc.get_scale() >= lossless_acc.get_precision()) {
            is_lossless = true;
          }
        }
      } else if (ObBitTC == column_tc) {
        if (ObNumberTC == dst_tc) {
          const double log10_2 = 0.30103;
          ObAccuracy lossless_acc = column_type.get_accuracy();
          if (dst_acc.get_scale() >= 0 &&
              dst_acc.get_precision() - dst_acc.get_scale() >= lossless_acc.get_precision() * log10_2) {
            // log10(2) = 0.30102999566398114; log10(2^n) = n*log10(2);
            // cast(b'111' as decimal(1,0)) is lossless
            is_lossless = true;
          }
        }
      } else if (ObFloatTC == column_tc || ObDoubleTC == column_tc) {
        if (ObDoubleTC == dst_tc) {
          if (-1 == dst_acc.get_precision() && -1 == dst_acc.get_scale()) {
            is_lossless = true;
          }
        }
      } else if (ObDateTimeType == column_type.get_type()) {
        // do nothing
      } else if (ObTimestampType == column_type.get_type()) {
        if (ObDateTimeType == dst_type.get_type()) {
          if (column_type.get_accuracy().get_precision() == dst_acc.get_precision() &&
              column_type.get_accuracy().get_scale() == dst_acc.get_scale()) {
            is_lossless = true;
          }
        }
      } else if (ObDateTC == column_tc || ObTimeTC == column_tc) {
        if (ObDateTimeType == dst_type.get_type() || ObTimestampType == dst_type.get_type()) {
          if (-1 == dst_acc.get_precision() && -1 == dst_acc.get_scale()) {
            is_lossless = true;
          }
        }
      } else if (ObYearTC == column_tc) {
        if (ObNumberTC == dst_tc) {
          ObAccuracy lossless_acc =
              ObAccuracy::DDL_DEFAULT_ACCURACY2[ObCompatibilityMode::MYSQL_MODE][column_type.get_type()];
          if (dst_acc.get_precision() - dst_acc.get_scale() >=
              lossless_acc.get_precision() - lossless_acc.get_scale()) {
            is_lossless = true;
          }
        } else if (ObDoubleTC == dst_tc) {
          if (-1 == dst_acc.get_precision() && -1 == dst_acc.get_scale()) {
            is_lossless = true;
          }
        }
      } else if (ObVarcharType == column_type.get_type()) {
        // varchar, varbinnary
      } else if (ObCharType == column_type.get_type()) {
        if (ObVarcharType == dst_type.get_type()) {
          if (dst_acc.get_length() == column_type.get_accuracy().get_length() &&
              dst_type.get_obj_meta().get_collation_type() == column_type.get_obj_meta().get_collation_type()) {
            is_lossless = true;
          }
        }
      }
    } else {
      // oracle mode, do nothing
    }
  }
  return ret;
}

int ObOptimizerUtil::gen_set_target_list(ObIAllocator* allocator, ObSQLSessionInfo* session_info,
    ObRawExprFactory* expr_factory, ObSelectStmt& left_stmt, ObSelectStmt& right_stmt,
    ObSelectStmt* select_stmt, const bool to_left_type /* false */)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObSelectStmt*, 1> left_stmts;
  ObSEArray<ObSelectStmt*, 1> right_stmts;
  if (OB_FAIL(left_stmts.push_back(&left_stmt)) || OB_FAIL(right_stmts.push_back(&right_stmt))) {
    LOG_WARN("failed to pushback stmt", K(ret));
  } else if (OB_FAIL(gen_set_target_list(
                 allocator, session_info, expr_factory, left_stmts, right_stmts, select_stmt, to_left_type))) {
    LOG_WARN("failed to get set target list", K(ret));
  }
  return ret;
}

int ObOptimizerUtil::gen_set_target_list(ObIAllocator* allocator, ObSQLSessionInfo* session_info,
    ObRawExprFactory* expr_factory, ObIArray<ObSelectStmt*>& left_stmts, ObIArray<ObSelectStmt*>& right_stmts,
    ObSelectStmt* select_stmt, const bool to_left_type /* false */)
{
  int ret = OB_SUCCESS;
  UNUSED(allocator);
  ObSelectStmt* child_stmt = NULL;
  ObSEArray<ObExprResType, 8> res_types;
  if (OB_ISNULL(session_info) || OB_ISNULL(expr_factory) || OB_ISNULL(select_stmt)) {
    ret = OB_NOT_INIT;
    LOG_WARN("params is invalid", K(ret));
  } else if (OB_FAIL(try_add_cast_to_set_child_list(allocator,
                 session_info,
                 expr_factory,
                 select_stmt->is_set_distinct(),
                 left_stmts,
                 right_stmts,
                 &res_types,
                 to_left_type))) {
    LOG_WARN("failed to try add cast to set child list", K(ret));
  } else if (OB_ISNULL(child_stmt = select_stmt->get_set_query(0)) ||
             OB_UNLIKELY(res_types.count() != child_stmt->get_select_item_size())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected set stmt", K(ret), K(child_stmt));
  } else {
    select_stmt->get_select_items().reset();
    ObItemType set_op_type = static_cast<ObItemType>(T_OP_SET + select_stmt->get_set_op());
    const int64_t num = child_stmt->get_select_item_size();
    SelectItem new_select_item;
    for (int64_t i = 0; OB_SUCC(ret) && i < num; i++) {
      SelectItem& select_item = child_stmt->get_select_item(i);
      new_select_item.alias_name_ = select_item.alias_name_;
      new_select_item.expr_name_ = select_item.expr_name_;
      new_select_item.default_value_ = select_item.default_value_;
      new_select_item.default_value_expr_ = select_item.default_value_expr_;
      new_select_item.is_real_alias_ = true;
      new_select_item.questions_pos_ = select_item.questions_pos_;
      new_select_item.params_idx_ = select_item.params_idx_;
      new_select_item.neg_param_idx_ = select_item.neg_param_idx_;
      new_select_item.esc_str_flag_ = select_item.esc_str_flag_;
      new_select_item.is_unpivot_mocked_column_ = select_item.is_unpivot_mocked_column_;
      new_select_item.paramed_alias_name_ = select_item.paramed_alias_name_;
      new_select_item.need_check_dup_name_ = select_item.need_check_dup_name_;
      if (OB_FAIL(ObRawExprUtils::make_set_op_expr(
              *expr_factory, i, set_op_type, res_types.at(i), session_info, new_select_item.expr_))) {
        LOG_WARN("create set op expr failed", K(ret));
      } else if (OB_FAIL(select_stmt->add_select_item(new_select_item))) {
        LOG_WARN("push back set select item failed", K(ret));
      } else if (OB_ISNULL(new_select_item.expr_) || OB_UNLIKELY(!new_select_item.expr_->is_set_op_expr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null or is not set op expr", "set op", PC(new_select_item.expr_));
      } else {
        new_select_item.expr_->set_expr_level(child_stmt->get_current_level());
      }
    }
  }
  return ret;
}

int ObOptimizerUtil::gen_set_target_list(
    ObIAllocator* allocator, ObSQLSessionInfo* session_info, ObRawExprFactory* expr_factory, ObSelectStmt* select_stmt)
{
  int ret = OB_SUCCESS;
  UNUSED(allocator);
  ObSelectStmt* child_stmt = NULL;
  ObSEArray<ObExprResType, 8> res_types;
  if (OB_ISNULL(session_info) || OB_ISNULL(expr_factory) || OB_ISNULL(select_stmt) ||
      OB_UNLIKELY(select_stmt->get_set_query().empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret));
  } else if (OB_FAIL(get_set_res_types(allocator, session_info, select_stmt->get_set_query(), res_types))) {
    LOG_WARN("failed to get set res types", K(ret));
  } else if (OB_ISNULL(child_stmt = select_stmt->get_set_query(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected set stmt", K(ret), K(child_stmt));
  } else {
    select_stmt->get_select_items().reset();
    ObItemType set_op_type = static_cast<ObItemType>(T_OP_SET + select_stmt->get_set_op());
    const int64_t num = child_stmt->get_select_item_size();
    SelectItem new_select_item;
    for (int64_t i = 0; OB_SUCC(ret) && i < num; ++i) {
      if (OB_FAIL(add_cast_to_set_list(session_info, expr_factory, select_stmt->get_set_query(), res_types.at(i), i))) {
        LOG_WARN("failed to add add cast to set list", K(ret));
      } else {
        SelectItem& select_item = child_stmt->get_select_item(i);
        new_select_item.alias_name_ = select_item.alias_name_;
        new_select_item.expr_name_ = select_item.expr_name_;
        new_select_item.default_value_ = select_item.default_value_;
        new_select_item.default_value_expr_ = select_item.default_value_expr_;
        new_select_item.is_real_alias_ = true;
        new_select_item.questions_pos_ = select_item.questions_pos_;
        new_select_item.params_idx_ = select_item.params_idx_;
        new_select_item.neg_param_idx_ = select_item.neg_param_idx_;
        new_select_item.esc_str_flag_ = select_item.esc_str_flag_;
        new_select_item.is_unpivot_mocked_column_ = select_item.is_unpivot_mocked_column_;
        new_select_item.paramed_alias_name_ = select_item.paramed_alias_name_;
        new_select_item.need_check_dup_name_ = select_item.need_check_dup_name_;
        if (OB_FAIL(ObRawExprUtils::make_set_op_expr(
                *expr_factory, i, set_op_type, res_types.at(i), session_info, new_select_item.expr_))) {
          LOG_WARN("create set op expr failed", K(ret));
        } else if (OB_FAIL(select_stmt->add_select_item(new_select_item))) {
          LOG_WARN("push back set select item failed", K(ret));
        } else if (OB_ISNULL(new_select_item.expr_) || OB_UNLIKELY(!new_select_item.expr_->is_set_op_expr())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expr is null or is not set op expr", "set op", PC(new_select_item.expr_));
        } else {
          new_select_item.expr_->set_expr_level(child_stmt->get_current_level());
        }
      }
    }
  }
  return ret;
}

int ObOptimizerUtil::get_set_res_types(ObIAllocator* allocator, ObSQLSessionInfo* session_info,
    ObIArray<ObSelectStmt*>& child_querys, ObIArray<ObExprResType>& res_types)
{
  int ret = OB_SUCCESS;
  res_types.reuse();
  ObSelectStmt* select_stmt = NULL;
  ObCollationType coll_type = CS_TYPE_INVALID;
  if (OB_ISNULL(allocator) || OB_ISNULL(session_info) || OB_UNLIKELY(child_querys.empty()) ||
      OB_ISNULL(select_stmt = child_querys.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(allocator), K(session_info), K(select_stmt));
  } else if (OB_FAIL(session_info->get_collation_connection(coll_type))) {
    LOG_WARN("failed to get collation connection", K(ret));
  } else {
    ObExprVersion dummy_op(*allocator);
    const ObLengthSemantics length_semantics = session_info->get_actual_nls_length_semantics();
    ObSEArray<ObExprResType, 2> types;
    ObExprResType res_type;
    const int64_t child_num = child_querys.count();
    const int64_t select_num = select_stmt->get_select_item_size();
    ObSelectStmt *cur_stmt = NULL;
    bool is_all_not_null = true;
    bool is_on_null_side = false;
    ObColumnRefRawExpr *col_expr = NULL;
    ObRawExpr *expr = NULL;
    for (int64_t idx = 0; OB_SUCC(ret) && idx < select_num; ++idx) {
      types.reuse();
      is_all_not_null = true;
      res_type.unset_result_flag(NOT_NULL_FLAG);
      for (int64_t i = 0; OB_SUCC(ret) && i < child_num; ++i) {
        if (OB_ISNULL(cur_stmt = child_querys.at(i)) ||
            OB_UNLIKELY(idx >= cur_stmt->get_select_item_size()) ||
            OB_ISNULL(expr = cur_stmt->get_select_item(idx).expr_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected child stmt", K(ret), K(cur_stmt), K(expr));
        } else if (OB_FAIL(add_var_to_array_no_dup(types, expr->get_result_type()))) {
          LOG_WARN("failed to add var", K(ret), K(expr->get_result_type()));
        } else if (!is_all_not_null) {
          /* do nothing */
        } else if (!expr->is_column_ref_expr()) {
          is_all_not_null = false;
        } else if (OB_FALSE_IT(col_expr = static_cast<ObColumnRefRawExpr*>(expr))) {
        } else if (!col_expr->get_result_type().has_result_flag(NOT_NULL_FLAG)) {
          is_all_not_null = false;
        } else if (NULL == cur_stmt->get_table_item_by_id(col_expr->get_table_id())) {
          // column expr from upper stmt, ignore NOT NULL FLAG
          is_all_not_null = false;
        } else if (OB_FAIL(ObOptimizerUtil::is_table_on_null_side(cur_stmt,
                                                                  col_expr->get_table_id(),
                                                                  is_on_null_side))) {
          LOG_WARN("check is table on null side failed", K(ret));
        } else if (is_on_null_side) {
          is_all_not_null = false;
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_UNLIKELY(types.empty())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected empty", K(ret), K(types.empty()));
      } else if (1 == types.count()) {
        ret = res_types.push_back(types.at(0));
      } else if (OB_FAIL(dummy_op.aggregate_result_type_for_merge(res_type,
                     &types.at(0),
                     types.count(),
                     coll_type,
                     is_oracle_mode(),
                     length_semantics,
                     session_info))) {
        LOG_WARN("failed to aggregate result type for merge", K(ret));
      } else if (OB_FAIL(res_types.push_back(res_type))) {
        LOG_WARN("failed to pushback res type", K(ret));
      }
      if (OB_FAIL(ret)) {
      } else if (OB_UNLIKELY(res_types.empty())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected empty", K(ret), K(res_types.empty()));
      } else if (is_all_not_null) {
        res_types.at(res_types.count()-1).set_result_flag(NOT_NULL_FLAG);
      } else {
        res_types.at(res_types.count()-1).unset_result_flag(NOT_NULL_FLAG);
      }
    }
  }
  return ret;
}

int ObOptimizerUtil::try_add_cast_to_set_child_list(ObIAllocator* allocator, ObSQLSessionInfo* session_info,
    ObRawExprFactory* expr_factory, const bool is_distinct, ObIArray<ObSelectStmt*>& left_stmts,
    ObIArray<ObSelectStmt*>& right_stmts, ObIArray<ObExprResType>* res_types, const bool to_left_type /* false */)
{
  int ret = OB_SUCCESS;
  ObExprResType res_type;
  ObSEArray<ObExprResType, 8> left_types;
  ObSEArray<ObExprResType, 8> right_types;
  if (OB_ISNULL(allocator) || OB_ISNULL(session_info) || OB_ISNULL(expr_factory)) {
    ret = OB_NOT_INIT;
    LOG_WARN("params is invalid", K(ret));
  } else if (left_stmts.empty() || right_stmts.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("empty left/right stmts", K(ret), K(left_stmts), K(right_stmts));
  } else if (OB_FAIL(get_set_res_types(allocator, session_info, left_stmts, left_types)) ||
             OB_FAIL(get_set_res_types(allocator, session_info, right_stmts, right_types))) {
    LOG_WARN("failed to get set res types", K(ret));
  } else if (OB_UNLIKELY(left_types.count() != right_types.count())) {
    ret = OB_ERR_COLUMN_SIZE;
    LOG_WARN(
        "The used SELECT statements have a different number of columns", K(left_types.count()), K(right_types.count()));
  } else {
    if (NULL != res_types) {
      res_types->reuse();
    }
    const int64_t num = left_types.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < num; i++) {
      res_type.reset();
      ObExprResType& left_type = left_types.at(i);
      ObExprResType& right_type = right_types.at(i);
      if (!left_type.has_result_flag(OB_MYSQL_NOT_NULL_FLAG) || !right_type.has_result_flag(OB_MYSQL_NOT_NULL_FLAG)) {
        left_type.unset_result_flag(OB_MYSQL_NOT_NULL_FLAG);
        right_type.unset_result_flag(OB_MYSQL_NOT_NULL_FLAG);
      }
      if (left_type != right_type || ob_is_enumset_tc(right_type.get_type())) {
        ObSEArray<ObExprResType, 2> types;
        ObExprVersion dummy_op(*allocator);
        ObCollationType coll_type = CS_TYPE_INVALID;
        if (share::is_oracle_mode()) {
          /*
           * Oracle has more strict constraints for data types used in set operator
           * https://docs.oracle.com/cd/B19306_01/server.102/b14200/queries004.htm
           */
          // need to refine this when more data type are added in oracle mode
          if (!((left_type.is_null() && !right_type.is_lob() && !right_type.is_lob_locator()) ||
                  (right_type.is_null() && !left_type.is_lob() && !left_type.is_lob_locator()) ||
                  (left_type.is_raw() && right_type.is_raw()) ||
                  (left_type.is_character_type() && right_type.is_character_type()) ||
                  (ob_is_oracle_numeric_type(left_type.get_type()) &&
                      ob_is_oracle_numeric_type(right_type.get_type())) ||
                  (ob_is_oracle_temporal_type(left_type.get_type()) &&
                      (ob_is_oracle_temporal_type(right_type.get_type()))))) {
            ret = OB_ERR_EXP_NEED_SAME_DATATYPE;
            LOG_WARN("expression must have same datatype as corresponding expression",
                K(ret),
                K(i),
                K(left_type),
                K(right_type));
          } else if (left_type.is_character_type() && right_type.is_character_type() &&
                     (left_type.is_varchar_or_char() != right_type.is_varchar_or_char())) {
            ret = OB_ERR_CHARACTER_SET_MISMATCH;
            LOG_WARN("character set mismatch", K(ret), K(left_type), K(right_type));
          } else if (left_type.is_string_or_lob_locator_type() && right_type.is_string_or_lob_locator_type()) {
            ObCharsetType left_cs = left_type.get_charset_type();
            ObCharsetType right_cs = right_type.get_charset_type();
            if (left_cs != right_cs) {
              if (CHARSET_UTF8MB4 == left_cs || CHARSET_UTF8MB4 == right_cs) {
                // sys table column exist utf8 varchar types, let it go
              } else {
                ret = OB_ERR_COLLATION_MISMATCH;  // ORA-12704
                LOG_WARN("character set mismatch", K(ret), K(left_cs), K(right_cs));
              }
            }
          }
          LOG_DEBUG("data type check for each select item in set operator", K(left_type), K(right_type));
        }
        const ObLengthSemantics length_semantics = session_info->get_actual_nls_length_semantics();
        if (OB_FAIL(ret)) {
        } else if (to_left_type) {
          res_type = left_type;
        } else if (OB_FAIL(types.push_back(left_type)) || OB_FAIL(types.push_back(right_type))) {
          LOG_WARN("failed to push back", K(ret));
        } else if (OB_FAIL(session_info->get_collation_connection(coll_type))) {
          LOG_WARN("failed to get collation connection", K(ret));
        } else if (OB_FAIL(dummy_op.aggregate_result_type_for_merge(
                       res_type, &types.at(0), 2, coll_type, is_oracle_mode(), length_semantics, session_info))) {
          LOG_WARN("failed to aggregate result type for merge", K(ret));
        }
        if (OB_FAIL(ret)) {
        } else if (ObMaxType == res_type.get_type()) {
          ret = OB_ERR_INVALID_TYPE_FOR_OP;
          LOG_WARN("column type incompatible", K(left_type), K(right_type));
        } else if (left_type != res_type &&
                   OB_FAIL(add_cast_to_set_list(session_info, expr_factory, left_stmts, res_type, i))) {
          LOG_WARN("failed to add add cast to set list", K(ret));
        } else if (right_type != res_type &&
                   OB_FAIL(add_cast_to_set_list(session_info, expr_factory, right_stmts, res_type, i))) {
          LOG_WARN("failed to add add cast to set list", K(ret));
        }
      } else if (share::is_oracle_mode() && is_distinct && (left_type.is_lob() || left_type.is_lob_locator())) {
        ret = OB_ERR_INVALID_TYPE_FOR_OP;
        LOG_WARN("column type incompatible", K(ret), K(left_type), K(right_type));
      } else {
        res_type = left_type;
      }
      if (OB_SUCC(ret) && NULL != res_types && OB_FAIL(res_types->push_back(res_type))) {
        LOG_WARN("failed to push back res type", K(ret));
      }
    }
  }
  return ret;
}

int ObOptimizerUtil::add_cast_to_set_list(ObSQLSessionInfo* session_info, ObRawExprFactory* expr_factory,
    ObIArray<ObSelectStmt*>& stmts, const ObExprResType& res_type, const int64_t idx)
{
  int ret = OB_SUCCESS;
  ObRawExpr* src_expr = NULL;
  ObRawExpr* new_expr = NULL;
  ObSelectStmt* stmt = NULL;
  const int64_t num = stmts.count();
  if (OB_ISNULL(session_info) || OB_ISNULL(expr_factory)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < num; ++i) {
    if (OB_ISNULL(stmt = stmts.at(i)) || idx >= stmt->get_select_item_size() ||
        OB_ISNULL(src_expr = stmt->get_select_item(idx).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected stmt", K(ret), K(stmts.at(i)));
    } else if (ob_is_enumset_tc(src_expr->get_result_type().get_type())) {
      ObSysFunRawExpr* cast_expr = NULL;
      if (src_expr->get_result_type() == res_type) {
        /*do nothing*/
      } else if (OB_FAIL(
                     ObRawExprUtils::create_cast_expr(*expr_factory, src_expr, res_type, cast_expr, session_info))) {
        LOG_WARN("create cast expr for stmt failed", K(ret));
      } else if (OB_FAIL(cast_expr->add_flag(IS_INNER_ADDED_EXPR))) {
        LOG_WARN("failed to add flag", K(ret));
      } else {
        stmt->get_select_item(idx).expr_ = cast_expr;
      }
    } else if (OB_FAIL(ObRawExprUtils::try_add_cast_expr_above(
                   expr_factory, session_info, *src_expr, res_type, new_expr))) {
      LOG_WARN("create cast expr for stmt failed", K(ret));
    } else if (src_expr == new_expr) {
      /*do nothing*/
    } else if (OB_FAIL(new_expr->add_flag(IS_INNER_ADDED_EXPR))) {
      LOG_WARN("failed to add flag", K(ret));
    } else {
      stmt->get_select_item(idx).expr_ = new_expr;
    }
  }
  return ret;
}

int ObOptimizerUtil::check_subquery_has_ref_assign_user_var(ObRawExpr* expr, bool& is_has)
{
  int ret = OB_SUCCESS;
  is_has = false;
  ObSEArray<ObQueryRefRawExpr*, 4> subqueries;
  ObQueryRefRawExpr* subquery = NULL;
  ObLogicalOperator* op = NULL;
  ObDMLStmt* stmt = NULL;
  if (OB_FAIL(ObTransformUtils::extract_query_ref_expr(expr, subqueries))) {
    LOG_WARN("failed to extract query ref expr", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !is_has && i < subqueries.count(); ++i) {
      if (OB_ISNULL(subquery = subqueries.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (subquery->is_ref_stmt()) {
        stmt = subquery->get_ref_stmt();
      } else if (OB_ISNULL(op = subquery->get_ref_operator())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else {
        stmt = op->get_stmt();
      }
      if (OB_SUCC(ret)) {
        if (OB_ISNULL(stmt)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (OB_FAIL(stmt->has_ref_assign_user_var(is_has))) {
          LOG_WARN("failed to check stmt has ref assign user var", K(ret));
        }
      }
    }
  }
  return ret;
}

bool ObOptimizerUtil::is_param_expr_correspond_subquey(
    const ObConstRawExpr& const_expr, ObIArray<std::pair<int64_t, ObRawExpr*>>& onetime_exprs)
{
  bool is_from = false;
  if (!const_expr.has_flag(IS_PARAM)) {
    /*do nothing*/
  } else {
    int64_t param_value = const_expr.get_value().get_unknown();
    ObRawExpr* init_expr = find_exec_param(onetime_exprs, param_value);
    if (init_expr == NULL) {
      // not found, skip it
    } else if (init_expr->has_flag(CNT_SUB_QUERY)) {
      is_from = true;
    }
  }
  return is_from;
}

int ObOptimizerUtil::pushdown_filter_into_subquery(ObDMLStmt& parent_stmt, ObSelectStmt& subquery,
    ObOptimizerContext& opt_ctx, ObIArray<ObRawExpr*>& pushdown_filters, ObIArray<ObRawExpr*>& candi_filters,
    ObIArray<ObRawExpr*>& remain_filters, bool& can_pushdown)
{
  int ret = OB_SUCCESS;
  bool has_rownum = false;
  bool has_rollup = false;
  can_pushdown = false;
  if (subquery.is_set_stmt()) {
    if (subquery.is_recursive_union() || subquery.has_limit()) {
      // do nothing
    } else if (OB_FAIL(check_pushdown_filter(
                   parent_stmt, subquery, opt_ctx, pushdown_filters, candi_filters, remain_filters))) {
      LOG_WARN("failed to check pushdown filter", K(ret));
    } else if (candi_filters.empty()) {
      // do thing
    } else {
      can_pushdown = true;
    }
  } else if (subquery.is_hierarchical_query()) {
    // can not pushdown do nothing
  } else if (0 == subquery.get_from_item_size()) {
    // expr value plan can not pushdown
  } else {
    has_rollup = subquery.has_rollup();
    if (OB_FAIL(subquery.has_rownum(has_rownum))) {
      LOG_WARN("failed to check stmt has rownum", K(ret));
    } else if (subquery.has_limit() || subquery.has_sequence() || subquery.is_contains_assignment() || has_rollup ||
               has_rownum) {
      // can not pushdown do nothing
    } else if (OB_FAIL(check_pushdown_filter(
                   parent_stmt, subquery, opt_ctx, pushdown_filters, candi_filters, remain_filters))) {
      LOG_WARN("failed to check pushdown filter", K(ret));
    } else if (candi_filters.empty()) {
      // do thing
    } else {
      can_pushdown = true;
    }
  }
  if (OB_SUCC(ret) && !can_pushdown) {
    ret = remain_filters.assign(pushdown_filters);
  }
  return ret;
}

int ObOptimizerUtil::check_pushdown_filter(ObDMLStmt& parent_stmt, ObSelectStmt& subquery, ObOptimizerContext& opt_ctx,
    ObIArray<ObRawExpr*>& pushdown_filters, ObIArray<ObRawExpr*>& candi_filters, ObIArray<ObRawExpr*>& remain_filters)
{
  int ret = OB_SUCCESS;
  bool is_valid = false;
  ObSEArray<ObRawExpr*, 4> common_exprs;
  if (!parent_stmt.is_set_stmt() && subquery.is_set_stmt()) {
    if (OB_FAIL(check_pushdown_filter_overlap_index(
            parent_stmt, opt_ctx, pushdown_filters, candi_filters, remain_filters))) {
      LOG_WARN("failed to check pushdown filter overlap index", K(ret));
    }
  } else if (OB_FAIL(get_groupby_win_func_common_exprs(subquery, common_exprs, is_valid))) {
    LOG_WARN("failed to get common exprs", K(ret));
  } else if (is_valid && common_exprs.empty()) {
    // can not pushdown any filter
  } else if (parent_stmt.is_set_stmt()) {
    if (OB_FAIL(check_pushdown_filter_for_set(static_cast<ObSelectStmt&>(parent_stmt),
            subquery,
            common_exprs,
            pushdown_filters,
            candi_filters,
            remain_filters))) {
      LOG_WARN("failed to check pushdown filter for set stmt", K(ret));
    }
  } else {
    if (OB_FAIL(check_pushdown_filter_for_subquery(static_cast<ObSelectStmt&>(parent_stmt),
            subquery,
            opt_ctx,
            common_exprs,
            pushdown_filters,
            candi_filters,
            remain_filters))) {
      LOG_WARN("failed to check pushdown filter for subquery", K(ret));
    }
  }
  return ret;
}

int ObOptimizerUtil::check_pushdown_filter_overlap_index(ObDMLStmt& stmt, ObOptimizerContext& opt_ctx,
    ObIArray<ObRawExpr*>& pushdown_filters, ObIArray<ObRawExpr*>& candi_filters, ObIArray<ObRawExpr*>& remain_filters)
{
  int ret = OB_SUCCESS;
  bool is_match_index = false;
  for (int64_t i = 0; OB_SUCC(ret) && !is_match_index && i < pushdown_filters.count(); ++i) {
    ObRawExpr* pred = NULL;
    ObSEArray<ObRawExpr*, 4> column_exprs;
    if (OB_ISNULL(pred = pushdown_filters.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("predicate is null", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(pred, column_exprs))) {
      LOG_WARN("failed to extract column exprs", K(ret));
    } else {
      for (int64_t j = 0; OB_SUCC(ret) && !is_match_index && j < column_exprs.count(); ++j) {
        ObRawExpr* expr = column_exprs.at(j);
        if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpect null expr", K(ret));
        } else if (OB_FAIL(ObTransformUtils::check_column_match_index(opt_ctx.get_root_stmt(),
                       &stmt,
                       opt_ctx.get_sql_schema_guard(),
                       static_cast<ObColumnRefRawExpr*>(column_exprs.at(j)),
                       is_match_index))) {
          LOG_WARN("failed to check select expr is overlap index", K(ret));
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (is_match_index) {
    ret = candi_filters.assign(pushdown_filters);
  } else {
    ret = remain_filters.assign(pushdown_filters);
  }
  return ret;
}

int ObOptimizerUtil::check_pushdown_filter_for_set(ObSelectStmt& parent_stmt, ObSelectStmt& subquery,
    ObIArray<ObRawExpr*>& common_exprs, ObIArray<ObRawExpr*>& pushdown_filters, ObIArray<ObRawExpr*>& candi_filters,
    ObIArray<ObRawExpr*>& remain_filters)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> child_select_list;
  ObSEArray<ObRawExpr*, 4> parent_select_list;
  if (OB_FAIL(subquery.get_select_exprs(child_select_list))) {
    LOG_WARN("get child stmt select exprs failed", K(ret));
  } else if (OB_FAIL(parent_stmt.get_select_exprs(parent_select_list))) {
    LOG_WARN("get parent stmt select exprs failed", K(ret));
  } else if (child_select_list.count() != parent_select_list.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child stmt select exprs size is incorrect",
        K(child_select_list.count()),
        K(parent_select_list.count()),
        K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < pushdown_filters.count(); ++i) {
    ObSEArray<ObRawExpr*, 4> view_column_exprs;
    ObRawExpr* expr = pushdown_filters.at(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null expr", K(ret));
    } else if (OB_FAIL(ObTransformUtils::replace_expr(parent_select_list, child_select_list, expr))) {
      SQL_LOG(WARN, "failed to replace expr", K(ret));
    } else if (OB_FAIL(expr->extract_info())) {
      LOG_WARN("failed to extract info", K(ret), K(*expr));
    } else if (expr->has_flag(CNT_WINDOW_FUNC) || expr->has_flag(CNT_AGG) || expr->has_flag(CNT_SUB_QUERY)) {
      ret = remain_filters.push_back(expr);
    } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(expr, view_column_exprs))) {
      LOG_WARN("failed to extract column exprs", K(ret));
    } else if (!common_exprs.empty() && !subset_exprs(view_column_exprs, common_exprs)) {
      ret = remain_filters.push_back(expr);
    } else if (OB_FAIL(candi_filters.push_back(expr))) {
      LOG_WARN("failed to push back predicate", K(ret));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObTransformUtils::replace_expr(child_select_list, parent_select_list, expr))) {
      SQL_LOG(WARN, "failed to replace expr", K(ret));
    } else if (OB_FAIL(expr->extract_info())) {
      LOG_WARN("failed to extract info", K(ret), K(*expr));
    } else {
      pushdown_filters.at(i) = expr;
    }
  }
  if (OB_SUCC(ret)) {
    LOG_TRACE("success to check_pushdown_filter_for_set", K(pushdown_filters), K(candi_filters), K(remain_filters));
  }
  return ret;
}

int ObOptimizerUtil::check_pushdown_filter_for_subquery(ObSelectStmt& parent_stmt, ObSelectStmt& subquery,
    ObOptimizerContext& opt_ctx, ObIArray<ObRawExpr*>& common_exprs, ObIArray<ObRawExpr*>& pushdown_filters,
    ObIArray<ObRawExpr*>& candi_filters, ObIArray<ObRawExpr*>& remain_filters)
{
  int ret = OB_SUCCESS;
  if (parent_stmt.is_set_stmt()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect set stmt here", K(ret));
  } else {
    bool is_match_index = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < pushdown_filters.count(); ++i) {
      ObRawExpr* pred = NULL;
      ObSEArray<ObRawExpr*, 4> column_exprs;
      ObSEArray<ObRawExpr*, 4> select_exprs;
      bool pushed = false;
      if (OB_ISNULL(pred = pushdown_filters.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("predicate is null", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(pred, column_exprs))) {
        LOG_WARN("failed to extract column exprs", K(ret));
      } else if (OB_FAIL(ObTransformUtils::convert_column_expr_to_select_expr(column_exprs, subquery, select_exprs))) {
        LOG_WARN("failed to convert column exprs to select exprs", K(ret));
      } else if (column_exprs.count() != select_exprs.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect column expr count", K(ret));
      } else {
        bool is_simple_expr = true;
        bool is_match = false;
        ObSEArray<ObRawExpr*, 4> view_column_exprs;
        for (int64_t j = 0; OB_SUCC(ret) && is_simple_expr && j < select_exprs.count(); ++j) {
          ObRawExpr* expr = select_exprs.at(j);
          if (OB_ISNULL(expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpect null expr", K(ret));
          } else if (expr->has_flag(CNT_WINDOW_FUNC) || expr->has_flag(CNT_AGG) || expr->has_flag(CNT_SUB_QUERY)) {
            is_simple_expr = false;
          } else if (OB_FAIL(ObTransformUtils::check_column_match_index(opt_ctx.get_root_stmt(),
                         &parent_stmt,
                         opt_ctx.get_sql_schema_guard(),
                         static_cast<ObColumnRefRawExpr*>(column_exprs.at(j)),
                         is_match))) {
            LOG_WARN("failed to check select expr is overlap index", K(ret));
          } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(expr, view_column_exprs))) {
            LOG_WARN("failed to extract column exprs", K(ret));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (!is_simple_expr) {
          // can not push down
        } else if (!common_exprs.empty() && !subset_exprs(view_column_exprs, common_exprs)) {
        } else if (OB_FAIL(candi_filters.push_back(pred))) {
          LOG_WARN("failed to push back predicate", K(ret));
        } else {
          pushed = true;
          if (is_match) {
            is_match_index = true;
          }
        }
      }
      if (OB_SUCC(ret) && !pushed) {
        ret = remain_filters.push_back(pred);
      }
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (!is_match_index) {
      candi_filters.reset();
      ret = remain_filters.assign(pushdown_filters);
    }
  }
  return ret;
}

int ObOptimizerUtil::get_groupby_win_func_common_exprs(
    ObSelectStmt& subquery, ObIArray<ObRawExpr*>& common_exprs, bool& is_valid)
{
  int ret = OB_SUCCESS;
  bool has_winfunc = false;
  bool has_group = false;
  is_valid = true;
  has_group = subquery.has_group_by();
  has_winfunc = subquery.has_window_function();
  if (!has_group && !has_winfunc) {
    is_valid = false;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < subquery.get_window_func_count(); ++i) {
      ObWinFunRawExpr* win_expr = NULL;
      if (OB_ISNULL(win_expr = subquery.get_window_func_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("window function expr is null", K(ret));
      } else if (i == 0) {
        if (OB_FAIL(common_exprs.assign(win_expr->get_partition_exprs()))) {
          LOG_WARN("failed to assign partition exprs", K(ret));
        }
      } else if (OB_FAIL(intersect_exprs(common_exprs, win_expr->get_partition_exprs(), common_exprs))) {
        LOG_WARN("failed to intersect expr array", K(ret));
      } else if (common_exprs.empty()) {
        break;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (has_group && !has_winfunc && OB_FAIL(append(common_exprs, subquery.get_group_exprs()))) {
      LOG_WARN("failed to append exprs", K(ret));
    } else if (has_group && has_winfunc &&
               OB_FAIL(intersect_exprs(common_exprs, subquery.get_group_exprs(), common_exprs))) {
      LOG_WARN("failed to intersect expr array", K(ret));
    }
  }
  return ret;
}

int ObOptimizerUtil::rename_pushdown_filter(const ObDMLStmt& parent_stmt, const ObSelectStmt& subquery,
    int64_t table_id, ObSQLSessionInfo* session_info, ObRawExprFactory& expr_factory,
    ObIArray<ObRawExpr*>& candi_filters, ObIArray<ObRawExpr*>& rename_filters)
{
  int ret = OB_SUCCESS;
  if (parent_stmt.is_set_stmt()) {
    ret = rename_set_op_pushdown_filter(static_cast<const ObSelectStmt&>(parent_stmt),
        subquery,
        session_info,
        expr_factory,
        candi_filters,
        rename_filters);
  } else {
    ret = rename_subquery_pushdown_filter(
        parent_stmt, subquery, table_id, session_info, expr_factory, candi_filters, rename_filters);
  }
  return ret;
}

int ObOptimizerUtil::rename_set_op_pushdown_filter(const ObSelectStmt& parent_stmt, const ObSelectStmt& subquery,
    ObSQLSessionInfo* session_info, ObRawExprFactory& expr_factory, ObIArray<ObRawExpr*>& candi_filters,
    ObIArray<ObRawExpr*>& rename_filters)
{
  int ret = OB_SUCCESS;
  int64_t stmt_level = -1;
  ObSEArray<ObRawExpr*, 4> child_select_list;
  ObSEArray<ObRawExpr*, 4> parent_select_list;
  if (OB_FAIL(subquery.get_select_exprs(child_select_list))) {
    LOG_WARN("get child stmt select exprs failed", K(ret));
  } else if (OB_FAIL(parent_stmt.get_select_exprs(parent_select_list))) {
    LOG_WARN("get parent stmt select exprs failed", K(ret));
  } else if (child_select_list.count() != parent_select_list.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child stmt select exprs size is incorrect",
        K(child_select_list.count()),
        K(parent_select_list.count()),
        K(ret));
  } else {
    stmt_level = subquery.get_current_level();
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < parent_select_list.count(); ++i) {
    ObRawExpr* expr = NULL;
    if (OB_ISNULL(expr = ObTransformUtils::get_expr_in_cast(parent_select_list.at(i)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("select expr is null", K(i), K(ret));
    } else {
      parent_select_list.at(i) = expr;
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < candi_filters.count(); ++i) {
    ObRawExpr* pred = candi_filters.at(i);
    ObRawExpr* new_pred = NULL;
    if (OB_ISNULL(pred)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null pred", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::copy_expr(expr_factory, pred, new_pred, COPY_REF_DEFAULT))) {
      LOG_WARN("failed to copy expr", K(ret));
    } else if (OB_FAIL(ObTransformUtils::replace_expr(parent_select_list, child_select_list, new_pred))) {
      LOG_WARN("failed to replace expr", K(ret));
    } else if (OB_FAIL(new_pred->formalize(session_info))) {
      LOG_WARN("failed to formalize expr", K(ret));
    } else if (OB_FAIL(new_pred->pull_relation_id_and_levels(stmt_level))) {
      LOG_WARN("failed to pull relation id and levels", K(ret));
    } else {
      ret = rename_filters.push_back(new_pred);
    }
  }
  return ret;
}

int ObOptimizerUtil::rename_subquery_pushdown_filter(const ObDMLStmt& parent_stmt, const ObSelectStmt& subquery,
    int64_t table_id, ObSQLSessionInfo* session_info, ObRawExprFactory& expr_factory,
    ObIArray<ObRawExpr*>& candi_filters, ObIArray<ObRawExpr*>& rename_filters)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> view_select_list;
  ObSEArray<ObRawExpr*, 4> view_column_list;
  ObSEArray<ObColumnRefRawExpr*, 4> table_columns;
  int64_t stmt_level = -1;
  if (parent_stmt.is_set_stmt()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect set stmt here", K(ret));
  } else if (OB_FAIL(parent_stmt.get_column_exprs(table_id, table_columns))) {
    LOG_WARN("failed to get column exprs", K(ret));
  } else if (OB_FAIL(append(view_column_list, table_columns))) {
    LOG_WARN("failed to append column exprs", K(ret));
  } else {
    stmt_level = subquery.get_current_level();
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < table_columns.count(); ++i) {
    ObRawExpr* sel_expr = NULL;
    ObColumnRefRawExpr* col_expr = table_columns.at(i);
    int64_t idx = -1;
    if (OB_ISNULL(col_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column expr is null", K(ret), K(col_expr));
    } else if (FALSE_IT(idx = col_expr->get_column_id() - OB_APP_MIN_COLUMN_ID)) {
      // do nothing
    } else if (OB_UNLIKELY(idx < 0 || idx >= subquery.get_select_item_size())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("select index is invalid", K(ret), K(idx));
    } else if (OB_ISNULL(sel_expr = subquery.get_select_item(idx).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("select expr expr is not found", K(ret), K(sel_expr));
    } else if (OB_FAIL(view_select_list.push_back(sel_expr))) {
      LOG_WARN("failed to push back select expr", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < candi_filters.count(); ++i) {
    ObRawExpr* pred = candi_filters.at(i);
    ObRawExpr* new_pred = NULL;
    if (OB_ISNULL(pred)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null pred", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::copy_expr(expr_factory, pred, new_pred, COPY_REF_DEFAULT))) {
      LOG_WARN("failed to copy expr", K(ret));
    } else if (OB_FAIL(ObTransformUtils::replace_expr(view_column_list, view_select_list, new_pred))) {
      LOG_WARN("failed to replace expr", K(ret));
    } else if (OB_FAIL(new_pred->formalize(session_info))) {
      LOG_WARN("failed to formalize expr", K(ret));
    } else if (OB_FAIL(new_pred->pull_relation_id_and_levels(stmt_level))) {
      LOG_WARN("failed to pull relation id and levels", K(ret));
    } else {
      ret = rename_filters.push_back(new_pred);
    }
  }
  return ret;
}

int ObOptimizerUtil::get_set_op_remain_filter(const ObSelectStmt& stmt,
    const ObIArray<ObRawExpr*>& child_pushdown_preds, ObIArray<ObRawExpr*>& output_pushdown_preds,
    const bool first_child)
{
  int ret = OB_SUCCESS;
  if (!stmt.is_set_stmt()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is not a set stmt", K(ret));
  } else if (stmt.is_recursive_union()) {
    /*do nothing*/
  } else if (first_child) {
    ret = output_pushdown_preds.assign(child_pushdown_preds);
  } else if (stmt.get_set_op() == ObSelectStmt::UNION) {
    for (int64_t i = 0; OB_SUCC(ret) && i < child_pushdown_preds.count(); ++i) {
      if (ObOptimizerUtil::find_equal_expr(output_pushdown_preds, child_pushdown_preds.at(i))) {
        /*do nothing*/
      } else if (OB_FAIL(output_pushdown_preds.push_back(child_pushdown_preds.at(i)))) {
        LOG_WARN("push back preds failed", K(ret));
      } else { /*do nothing*/
      }
    }
  } else if (stmt.get_set_op() == ObSelectStmt::INTERSECT) {
    ObSEArray<ObRawExpr*, 4> pushdown_preds;
    for (int64_t i = 0; OB_SUCC(ret) && i < child_pushdown_preds.count(); ++i) {
      if (!ObOptimizerUtil::find_equal_expr(output_pushdown_preds, child_pushdown_preds.at(i))) {
        /*do nothing*/
      } else if (OB_FAIL(pushdown_preds.push_back(child_pushdown_preds.at(i)))) {
        LOG_WARN("push back preds failed", K(ret));
      } else { /*do nothing*/
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(output_pushdown_preds.assign(pushdown_preds))) {
      LOG_WARN("failed to assign exprs", K(ret));
    }
  } else { /*do nothing*/
  }
  return ret;
}

int ObOptimizerUtil::check_is_null_qual(
    const ObEstSelInfo& est_sel_info, const ObRelIds& table_ids, const ObRawExpr* qual, bool& is_null_qual)
{
  int ret = OB_SUCCESS;
  is_null_qual = false;
  const ParamStore* params = est_sel_info.get_params();
  const ObDMLStmt* stmt = est_sel_info.get_stmt();
  const ObRawExpr* left_expr = NULL;
  const ObRawExpr* right_expr = NULL;
  ObObj result;
  if (OB_ISNULL(params) || OB_ISNULL(stmt) || OB_ISNULL(qual)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(params), K(stmt), K(qual));
  } else if (T_OP_IS != qual->get_expr_type()) {
    // do nothing
  } else if (OB_ISNULL(left_expr = qual->get_param_expr(0)) || OB_ISNULL(right_expr = qual->get_param_expr(1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Left and right expr should not be NULL", K(ret), K(left_expr), K(right_expr));
  } else if (!left_expr->is_column_ref_expr()) {
    // do nothing
  } else if (!ObOptEstUtils::is_calculable_expr(*right_expr, params->count())) {
    // do nothing
  } else if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(stmt->get_stmt_type(),
                 const_cast<ObSQLSessionInfo*>(est_sel_info.get_session_info()),
                 right_expr,
                 result,
                 params,
                 const_cast<ObIAllocator&>(est_sel_info.get_allocator())))) {
    LOG_WARN("Failed to calc const or calculable expr", K(ret));
  } else if (result.is_null()) {
    if (table_ids.is_superset(left_expr->get_relation_ids())) {
      is_null_qual = true;
    }
  }
  return ret;
}

int ObOptimizerUtil::check_expr_contain_subquery(const ObIArray<ObRawExpr*>& exprs,
    const ObIArray<std::pair<int64_t, ObRawExpr*>>* onetime_exprs, bool& has_subquery)
{
  int ret = OB_SUCCESS;
  has_subquery = false;
  for (int64_t i = 0; OB_SUCC(ret) && !has_subquery && i < exprs.count(); i++) {
    if (OB_ISNULL(exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::check_expr_contain_subquery(exprs.at(i), onetime_exprs, has_subquery))) {
      LOG_WARN("failed to check expr contain subquery", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObOptimizerUtil::check_expr_contain_subquery(
    const ObRawExpr* expr, const ObIArray<std::pair<int64_t, ObRawExpr*>>* onetime_exprs, bool& has_subquery)
{
  int ret = OB_SUCCESS;
  has_subquery = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (expr->has_flag(CNT_SUB_QUERY)) {
    has_subquery = true;
  } else if (NULL == onetime_exprs) {
    /*do nothing*/
  } else if (expr->has_flag(IS_EXEC_PARAM)) {
    const ObConstRawExpr* const_expr = static_cast<const ObConstRawExpr*>(expr);
    int64_t param = const_expr->get_value().get_unknown();
    ObRawExpr* exec_expr = ObOptimizerUtil::find_exec_param(*onetime_exprs, param);
    if (NULL != exec_expr && exec_expr->has_flag(CNT_SUB_QUERY)) {
      has_subquery = true;
    } else { /*do nothing*/
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !has_subquery && i < expr->get_param_count(); i++) {
      if (OB_ISNULL(expr->get_param_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(SMART_CALL(ObOptimizerUtil::check_expr_contain_subquery(
                     expr->get_param_expr(i), onetime_exprs, has_subquery)))) {
        LOG_WARN("failed to check whether expr contain subquery", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObOptimizerUtil::check_expr_contain_sharable_subquery(ObRawExpr* expr, int32_t stmt_level,
    const ObIArray<std::pair<int64_t, ObRawExpr*>>& onetime_exprs, const bool ignore_root, bool& contains)
{
  int ret = OB_SUCCESS;
  contains = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (!expr->has_flag(CNT_EXEC_PARAM) && !expr->has_flag(CNT_SUB_QUERY)) {
    /*do nothing*/
  } else if (expr->has_flag(IS_SUB_QUERY)) {
    if (!ignore_root && expr->get_ref_count() > 1 && stmt_level == expr->get_expr_level()) {
      contains = true;
    } else {
      ObSEArray<ObRawExpr*, 16> stmt_exprs;
      ObQueryRefRawExpr* query_expr = static_cast<ObQueryRefRawExpr*>(expr);
      ObSelectStmt* stmt = query_expr->get_ref_stmt();
      if (OB_ISNULL(stmt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(stmt->get_relation_exprs(stmt_exprs))) {
        LOG_WARN("failed to get relation exprs", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && !contains && i < stmt_exprs.count(); i++) {
          if (OB_ISNULL(stmt_exprs.at(i))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null", K(ret));
          } else if (OB_FAIL(SMART_CALL(ObOptimizerUtil::check_expr_contain_sharable_subquery(
                         stmt_exprs.at(i), stmt_level, onetime_exprs, false, contains)))) {
            LOG_WARN("failed to check whether expr contain subquery", K(ret));
          } else { /*do nothing*/
          }
        }
      }
    }
  } else if (expr->has_flag(IS_EXEC_PARAM)) {
    const ObConstRawExpr* const_expr = static_cast<const ObConstRawExpr*>(expr);
    int64_t param = const_expr->get_value().get_unknown();
    ObRawExpr* exec_expr = ObOptimizerUtil::find_exec_param(onetime_exprs, param);
    if (NULL != exec_expr && OB_FAIL(SMART_CALL(ObOptimizerUtil::check_expr_contain_sharable_subquery(
                                 exec_expr, stmt_level, onetime_exprs, ignore_root, contains)))) {
      LOG_WARN("failed to check whether expr contain subquery", K(ret));
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !contains && i < expr->get_param_count(); i++) {
      if (OB_ISNULL(expr->get_param_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(SMART_CALL(ObOptimizerUtil::check_expr_contain_sharable_subquery(
                     expr->get_param_expr(i), stmt_level, onetime_exprs, false, contains)))) {
        LOG_WARN("failed to check whether expr contain subquery", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

bool ObOptimizerUtil::has_psedu_column(const ObRawExpr& expr)
{
  return expr.has_flag(CNT_ROWNUM) || expr.has_flag(CNT_SEQ_EXPR) || expr.has_flag(CNT_LEVEL) ||
         expr.has_flag(CNT_CONNECT_BY_ISLEAF) || expr.has_flag(CNT_CONNECT_BY_ISCYCLE);
}

bool ObOptimizerUtil::has_hierarchical_expr(const ObRawExpr& expr)
{
  return expr.has_flag(IS_PRIOR) || expr.has_flag(CNT_PRIOR) || expr.has_flag(CNT_CONNECT_BY_ROOT) ||
         expr.has_flag(CNT_SYS_CONNECT_BY_PATH) || expr.has_flag(CNT_LEVEL) || expr.has_flag(CNT_CONNECT_BY_ISLEAF) ||
         expr.has_flag(CNT_CONNECT_BY_ISCYCLE);
}

/*
int ObOptimizerUtil::check_exprs_cnt_exec_param(const ObIArray<ObRawExpr*> &exprs,
                                                bool ignore_query_ref,
                                                int64_t level,
                                                bool &cnt_exec_param)
{
  int ret = OB_SUCCESS;
  cnt_exec_param = false;
  for (int64_t i = 0; OB_SUCC(ret) && !cnt_exec_param && i < exprs.count(); ++i) {
    if (OB_FAIL(check_expr_cnt_exec_param(exprs.at(i),
                                          ignore_query_ref,
                                          level,
                                          cnt_exec_param))) {
      LOG_WARN("failed to check expr cnt exec param", K(ret));
    }
  }
  return ret;
}

int ObOptimizerUtil::check_expr_cnt_exec_param(ObRawExpr *expr,
                                               bool ignore_query_ref,
                                               int64_t level,
                                               bool &cnt_exec_param)
{
  int ret = OB_SUCCESS;
  cnt_exec_param = false;
  bool is_correlated = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null expr", K(ret));
  } else if (expr->has_flag(CNT_EXEC_PARAM) &&
             OB_FAIL(ObTransformUtils::is_correlated_expr(expr,
                                                          level,
                                                          is_correlated)) ) {
    LOG_WARN("failed to check is correlated expr", K(ret));
  } else if (is_correlated) {
    cnt_exec_param = true;
  } else if (!ignore_query_ref && expr->is_query_ref_expr()) {
    ObQueryRefRawExpr *ref_query = static_cast<ObQueryRefRawExpr*>(expr);
    if (ref_query->is_ref_stmt()) {
      ObSelectStmt *stmt = ref_query->get_ref_stmt();
      ret = check_stmt_cnt_exec_param(stmt, level, cnt_exec_param);
    } else {
      ObLogicalOperator *log_plan = ref_query->get_ref_operator();
      ObDMLStmt *stmt = NULL;
      if (OB_ISNULL(log_plan)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null plan", K(ret));
      } else if (OB_ISNULL( stmt = log_plan->get_stmt())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null stmt", K(ret));
      } else if (!stmt->is_select_stmt()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expect select stmt", K(*stmt), K(ret));
      } else {
        ret = check_stmt_cnt_exec_param(static_cast<ObSelectStmt*>(stmt),
                                        level,
                                        cnt_exec_param);
      }
    }
  } else {
    int64_t count = expr->get_param_count();
    for (int64_t i = 0; OB_SUCC(ret) && !cnt_exec_param && i < count; ++i) {
      if (OB_ISNULL(expr->get_param_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr->get_param_expr(i) returns null", K(ret), K(i));
      } else if (OB_FAIL(SMART_CALL(check_expr_cnt_exec_param(expr->get_param_expr(i),
                                                              ignore_query_ref,
                                                              level,
                                                              cnt_exec_param)))) {
        LOG_WARN("failed to check expr cnt exec param", K(ret));
      }
    }
  }
  return ret;
}

int ObOptimizerUtil::check_stmt_cnt_exec_param(ObSelectStmt *select_stmt,
                                               bool ignore_query_ref,
                                               int64_t level,
                                               bool &cnt_exec_param)
{
  int ret = OB_SUCCESS;
  cnt_exec_param = false;
  bool is_stack_overflow = false;
  ObSEArray<ObRawExpr *, 8> relation_exprs;
  if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("check stack overflow failed", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret));
  } else if (OB_ISNULL(select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(select_stmt));
  } else if (OB_FAIL(select_stmt->get_relation_exprs(relation_exprs))) {
    LOG_WARN("failed to get relation exprs", K(ret));
  } else if (OB_FAIL(check_exprs_cnt_exec_param(relation_exprs,
                                                ignore_query_ref,
                                                level,
                                                cnt_exec_param))) {
    LOG_WARN("failed to check exprs cnt exec param", K(ret));
  }
  return ret;
}

int ObOptimizerUtil::check_stmt_cnt_exec_param(ObSelectStmt *stmt,
                                               int64_t level,
                                               bool &cnt_exec_param)
{
  int ret = OB_SUCCESS;
  cnt_exec_param = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  } else {
    ObArray<ObSelectStmt*> child_stmts;
    if (OB_FAIL(check_stmt_cnt_exec_param(stmt,
                                          true,
                                          level,
                                          cnt_exec_param))) {
      LOG_WARN("fail to check stmt cnt exec param", K(ret));
    } else if (OB_FAIL(stmt->get_child_stmts(child_stmts))) {
      LOG_WARN("get child stmt failed", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < child_stmts.count(); ++i) {
        if (OB_FAIL(SMART_CALL(check_stmt_cnt_exec_param(child_stmts.at(i),
                                                         level,
                                                         cnt_exec_param)))) {
          LOG_WARN("fail to check stmt cnt exec param", K(ret));
        }
      }
    }
  }
  return ret;
}
*/

int ObOptimizerUtil::check_pushdown_filter_to_base_table(ObLogPlan& plan, const uint64_t table_id,
    const ObIArray<ObRawExpr*>& pushdown_filters, const ObIArray<ObRawExpr*>& restrict_infos, bool& can_pushdown)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObColumnRefRawExpr*, 8> col_exprs;
  ObSEArray<ObColumnRefRawExpr*, 8> pushdown_col_exprs;
  ObDMLStmt* stmt = plan.get_stmt();
  can_pushdown = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < restrict_infos.count(); ++i) {
    if (OB_FAIL(ObTransformUtils::get_simple_filter_column(stmt, restrict_infos.at(i), table_id, col_exprs))) {
      LOG_WARN("failed to get simple filter column", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < pushdown_filters.count(); ++i) {
    if (OB_FAIL(
            ObTransformUtils::get_simple_filter_column(stmt, pushdown_filters.at(i), table_id, pushdown_col_exprs))) {
      LOG_WARN("failed to get simple filter column", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && !can_pushdown && i < pushdown_col_exprs.count(); ++i) {
    ObColumnRefRawExpr* col_expr = pushdown_col_exprs.at(i);
    if (OB_FAIL(ObTransformUtils::is_match_index(
            plan.get_optimizer_context().get_sql_schema_guard(), stmt, col_expr, can_pushdown, NULL, &col_exprs))) {
      LOG_WARN("failed to check is match index", K(ret));
    }
  }
  LOG_TRACE("check pushdown filter to tables", K(table_id), K(can_pushdown));
  return ret;
}
