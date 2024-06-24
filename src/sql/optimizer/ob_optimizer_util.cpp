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
#include "share/schema/ob_column_schema.h"
#include "share/schema/ob_table_schema.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/optimizer/ob_logical_operator.h"
#include "sql/optimizer/ob_log_plan.h"
#include "lib/ob_name_def.h"
#include "common/ob_smart_call.h"
#include "sql/engine/expr/ob_expr_version.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "lib/utility/utility.h"
#include "sql/optimizer/ob_opt_selectivity.h"
#include "sql/optimizer/ob_log_table_scan.h"
#include "share/location_cache/ob_location_service.h"
#include "share/ob_order_perserving_encoder.h"
#include "sql/rewrite/ob_predicate_deduce.h"
#include "sql/optimizer/ob_log_join.h"

using namespace oceanbase;
using namespace sql;
using namespace oceanbase::common;
using namespace oceanbase::share::schema;
using namespace oceanbase::share;


int MergeKeyInfo::assign(MergeKeyInfo &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(map_array_.assign(other.map_array_))) {
    LOG_WARN("failed to assign map array", K(ret));
  } else if (OB_FAIL(order_directions_.assign(other.order_directions_))) {
    LOG_WARN("failed to assign order directions", K(ret));
  } else if (OB_FAIL(order_exprs_.assign(other.order_exprs_))) {
    LOG_WARN("failed to assign order exprs", K(ret));
  } else if (OB_FAIL(order_items_.assign(other.order_items_))) {
    LOG_WARN("failed to assign order exprs", K(ret));
  } else {
    need_sort_ = other.need_sort_;
    order_needed_ = other.order_needed_;
    prefix_pos_ = other.prefix_pos_;
  }
  return ret;
}

int ObOptimizerUtil::is_prefix_ordering(const ObIArray<OrderItem> &first,
                                        const ObIArray<OrderItem> &second,
                                        const EqualSets &equal_sets,
                                        const ObIArray<ObRawExpr*> &const_exprs,
                                        bool &first_is_prefix,
                                        bool &second_is_prefix)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 5> first_exprs;
  ObSEArray<ObOrderDirection, 5> first_directions;
  ObSEArray<ObRawExpr*, 5> second_exprs;
  ObSEArray<ObOrderDirection, 5> second_directions;
  if (OB_FAIL(split_expr_direction(first, first_exprs, first_directions))) {
    LOG_WARN("Failed to construct direction", K(ret));
  } else if (OB_FAIL(split_expr_direction(second, second_exprs, second_directions))) {
    LOG_WARN("Failed to construct direction", K(ret));
  } else if (OB_FAIL(find_common_prefix_ordering(first_exprs,
                                                 second_exprs,
                                                 equal_sets,
                                                 const_exprs,
                                                 first_is_prefix,
                                                 second_is_prefix,
                                                 &first_directions,
                                                 &second_directions))) {
    LOG_WARN("Failed to check is prefix ordering", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObOptimizerUtil::find_common_prefix_ordering(const ObIArray<ObRawExpr *> &left_side,
                                                 const ObIArray<ObRawExpr *> &right_side,
                                                 const EqualSets &equal_sets,
                                                 const ObIArray<ObRawExpr*> &const_exprs,
                                                 bool &left_is_prefix,
                                                 bool &right_is_prefix,
                                                 const ObIArray<ObOrderDirection> *left_directions,
                                                 const ObIArray<ObOrderDirection> *right_directions)
{
  int ret = OB_SUCCESS;
  bool left_is_const = false;
  bool right_is_const = false;
  int64_t N = left_side.count();
  int64_t M = right_side.count();
  int64_t i = 0;
  int64_t j = 0;
  bool is_match = true;
  ObSEArray<ObRawExpr*, 1> dummy_exprs;
  left_is_prefix = false;
  right_is_prefix = false;
  while (OB_SUCCESS == ret && is_match && i < N && j < M) {
    if (((NULL != right_directions && NULL != left_directions)
         ? left_directions->at(i) != right_directions->at(j) : false)
        || (!is_expr_equivalent(left_side.at(i), right_side.at(j), equal_sets))) {
      if (OB_FAIL(is_const_or_equivalent_expr(left_side,
                                              equal_sets,
                                              const_exprs,
                                              dummy_exprs,
                                              i,
                                              left_is_const))) {
        LOG_WARN("failed to check is const or equivalent exprs", K(ret));
      } else if (OB_FAIL(is_const_or_equivalent_expr(right_side,
                                                     equal_sets,
                                                     const_exprs,
                                                     dummy_exprs,
                                                     j,
                                                     right_is_const))) {
        LOG_WARN("failed to check is const or equivalent exprs", K(ret));
      } else if (!left_is_const && !right_is_const) {
        is_match = false;
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
  if (OB_SUCC(ret) && is_match) {
    if (i != N && j != M) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect prefix ordering matching", K(ret));
    } else if (i < N) {
      right_is_prefix = true;
      for (; OB_SUCC(ret) && i < N; i++) {
        if (OB_FAIL(is_const_or_equivalent_expr(left_side,
                                                equal_sets,
                                                const_exprs,
                                                dummy_exprs,
                                                i,
                                                left_is_const))) {
          LOG_WARN("failed to check is const or equivlent expr", K(ret));
        } else if (!left_is_const) {
          break;
        }
      }
      if (OB_SUCC(ret) && i == N) {
        left_is_prefix = true;
      }
    } else if (j < M) {
      left_is_prefix = true;
      for (; OB_SUCC(ret) && j < M; j++) {
        if (OB_FAIL(is_const_or_equivalent_expr(right_side,
                                                equal_sets,
                                                const_exprs,
                                                dummy_exprs,
                                                j,
                                                right_is_const))) {
          LOG_WARN("failed to check is const or equivlent expr", K(ret));
        } else if (!right_is_const) {
          break;
        }
      }
      if (OB_SUCC(ret) && j == M) {
        right_is_prefix = true;
      }
    } else {
      left_is_prefix = true;
      right_is_prefix = true;
    }
  }
  return ret;
}

int ObOptimizerUtil::is_const_or_equivalent_expr(const common::ObIArray<ObRawExpr *> &exprs,
                                                 const EqualSets &equal_sets,
                                                 const ObIArray<ObRawExpr*> &const_exprs,
                                                 const ObIArray<ObRawExpr*> &exec_ref_exprs,
                                                 const int64_t &pos,
                                                 bool &is_const)
{
  int ret = OB_SUCCESS;
  is_const = false;
  if (OB_UNLIKELY(pos < 0 || pos >= exprs.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid array pos", K(pos), K(exprs.count()), K(ret));
  } else if (OB_ISNULL(exprs.at(pos))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexected null", K(ret));
  } else if (OB_FAIL(is_const_expr(exprs.at(pos),
                                   equal_sets,
                                   const_exprs,
                                   exec_ref_exprs,
                                   is_const))) {
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

int ObOptimizerUtil::is_const_or_equivalent_expr(const ObIArray<OrderItem> &order_items,
                                                 const EqualSets &equal_sets,
                                                 const ObIArray<ObRawExpr*> &const_exprs,
                                                 const ObIArray<ObRawExpr*> &exec_ref_exprs,
                                                 const int64_t &pos,
                                                 bool &is_const)
{
  int ret = OB_SUCCESS;
  is_const = false;
  if (OB_UNLIKELY(pos < 0 || pos >= order_items.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid array pos", K(pos), K(order_items.count()), K(ret));
  } else if (OB_ISNULL(order_items.at(pos).expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexected null", K(ret));
  } else if (OB_FAIL(is_const_expr(order_items.at(pos).expr_,
                                   equal_sets,
                                   const_exprs,
                                   exec_ref_exprs,
                                   is_const))) {
    LOG_WARN("failed to check const expr", K(order_items.at(pos).expr_), K(ret));
  } else if (is_const) {
    /*do nothing*/
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !is_const && i < pos; i++) {
      if (is_expr_equivalent(order_items.at(i).expr_,
                             order_items.at(pos).expr_,
                             equal_sets)) {
        is_const = true;
      }
    }
  }
  return ret;
}

int ObOptimizerUtil::prefix_subset_ids(const ObIArray<uint64_t> &pre,
                                       const ObIArray<uint64_t> &full,
                                       bool &is_prefix)
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

//要求exprs是不含常量表达式的表达式集合
int ObOptimizerUtil::prefix_subset_exprs(const ObIArray<ObRawExpr *> &exprs,
                                         const ObIArray<ObRawExpr *> &ordering,
                                         const EqualSets &equal_sets,
                                         const ObIArray<ObRawExpr*> &const_exprs,
                                         bool &is_covered,
                                         int64_t *match_count)
{
  int ret = OB_SUCCESS;
  is_covered = false;
  bool is_break = false;
  int64_t covered_count = 0;
  ObBitSet<64> expr_idxs;
  int64_t key_covered_count = 0; //标记索引能匹配多少列
  for (int64_t i = 0; OB_SUCC(ret) && !is_break && i < ordering.count(); ++i) {
    bool is_found = false;
    for (int64_t j = 0; OB_SUCC(ret) && !is_found && j < exprs.count(); ++j) {
      if (expr_idxs.has_member(j)) {
        //已经和其它序的表达式对应了，不再参与比较
      } else if (is_expr_equivalent(ordering.at(i), exprs.at(j), equal_sets)) {
        is_found = true;
        if (OB_FAIL(expr_idxs.add_member(j))) {
          LOG_WARN("add expr_idxs member", K(ret));
        }
      }
    }
    if (OB_SUCC(ret) && is_found) {
      ++covered_count;
      key_covered_count = i + 1;//得到最长匹配索引的前缀
    }
    if (OB_SUCC(ret) && !is_found) {
      //有一个序的表达式没有在exprs中被匹配，那么需要看这个表达式是否是常量，如果是常量可以跳过
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
          //已经和其它序的表达式对应了，不再参与比较
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
    if (covered_count == exprs.count()) { //所有表达式都被序覆盖
      is_covered = true;
    }
    if (NULL != match_count) {
      *match_count = key_covered_count; //返回最长匹配索引前缀
    }
  }
  return ret;
}

// fast check, just return a bool result
int ObOptimizerUtil::prefix_subset_exprs(const ObIArray<ObRawExpr *> &exprs,
                                         const ObIArray<ObRawExpr *> &ordering,
                                         const EqualSets &equal_sets,
                                         const ObIArray<ObRawExpr*> &const_exprs,
                                         bool &is_prefix)
{
  int ret = OB_SUCCESS;
  is_prefix = false;
  bool is_break = false;
  for (int64_t i = 0; OB_SUCC(ret) && !is_break && !is_prefix && i < ordering.count(); ++i) {
    for (int64_t j = 0; OB_SUCC(ret) && !is_prefix && j < exprs.count(); ++j) {
      if (is_expr_equivalent(ordering.at(i), exprs.at(j), equal_sets)) {
        is_prefix = true;
      }
    }
    if (OB_SUCC(ret) && !is_prefix) {
      //有一个序的表达式没有在exprs中被匹配，那么需要看这个表达式是否是常量，如果是常量可以跳过
      bool is_const = false;
      if (OB_FAIL(is_const_expr(ordering.at(i), equal_sets, const_exprs, is_const))) {
        LOG_WARN("check expr is const expr failed", K(ret));
      } else if (!is_const) {
        is_break = true;
      }
    }
  }
  return ret;
}

//要求exprs是不含常量表达式的表达式集合
int ObOptimizerUtil::adjust_exprs_by_ordering(ObIArray<ObRawExpr *> &exprs,
                                              const ObIArray<OrderItem> &ordering,
                                              const EqualSets &equal_sets,
                                              const ObIArray<ObRawExpr*> &const_exprs,
                                              const ObIArray<ObRawExpr*> &exec_ref_exprs,
                                              int64_t &prefix_count,
                                              bool &ordering_all_used,
                                              ObIArray<ObOrderDirection> &directions,
                                              ObIArray<int64_t> *match_map /*=NULL*/)
{
  int ret = OB_SUCCESS;
  prefix_count = -1;
  ordering_all_used = false;
  ObSEArray<ObRawExpr *, 8> adjusted_exprs;
  ObSEArray<ObOrderDirection, 8> order_types;
  ObSEArray<int64_t, 8> expr_map;
  ObBitSet<64> expr_idxs;
  bool is_found = true;
  for (int64_t i = 0; OB_SUCC(ret) && is_found && i < ordering.count(); ++i) {
    const OrderItem &sort_key = ordering.at(i);
    bool is_const = false;
    if (OB_FAIL(is_const_or_equivalent_expr(ordering,
                                            equal_sets,
                                            const_exprs,
                                            exec_ref_exprs,
                                            i,
                                            is_const))) {
      LOG_WARN("failed to check is const or equivalent expr", K(ret));
    } else if (is_const) {
      is_found = true;
    } else {
      is_found = false;
    }
    for (int64_t j = 0; OB_SUCC(ret) && !is_found && j < exprs.count(); ++j) {
      if (expr_idxs.has_member(j)) {
        //已经和其它序的表达式对应了，不再参与比较
      } else if (is_expr_equivalent(sort_key.expr_, exprs.at(j), equal_sets)) {
        is_found = true;
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
  }
  if (OB_SUCC(ret)) {
    prefix_count = adjusted_exprs.count();
    ordering_all_used = prefix_count > 0 && expr_idxs.num_members() == exprs.count();
    if (OB_FAIL(generate_stable_ordering(exprs, directions, expr_idxs, expr_map,
                                         adjusted_exprs, order_types))) {
      LOG_WARN("failed to generate stable ordering", K(ret));
    } else if (OB_UNLIKELY(adjusted_exprs.count() != exprs.count()
                           || order_types.count() != exprs.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("exprs don't covered completely by ordering",
               K(adjusted_exprs.count()), K(exprs.count()), K(order_types.count()));
    } else if (OB_FAIL(exprs.assign(adjusted_exprs))) {
      LOG_WARN("assign adjusted exprs failed", K(ret));
    } else if (OB_FAIL(directions.assign(order_types))) {
      LOG_WARN("failed to assign order types", K(ret));
    } else if (match_map != NULL && OB_FAIL(match_map->assign(expr_map))) {
      LOG_WARN("failed to assign expr indexs", K(ret));
    }
  }
  return ret;
}

// when there is no input ordering or interesting ordering, generate a stable ordering use this function
int ObOptimizerUtil::generate_stable_ordering(common::ObIArray<ObRawExpr *> &exprs,
                                              ObIArray<ObOrderDirection> &directions)
{
  int ret = OB_SUCCESS;
  ObBitSet<64> used_expr_idxs;
  ObSEArray<int64_t, 8> expr_map;
  ObSEArray<ObRawExpr *, 8> adjusted_exprs;
  ObSEArray<ObOrderDirection, 8> adjusted_directions;
  if (OB_FAIL(generate_stable_ordering(exprs,
                                       directions,
                                       used_expr_idxs,
                                       expr_map,
                                       adjusted_exprs,
                                       adjusted_directions))) {
    LOG_WARN("failed to generate stable ordering", K(ret));
  } else if (OB_FAIL(exprs.assign(adjusted_exprs))) {
    LOG_WARN("assign adjusted exprs failed", K(ret));
  } else if (OB_FAIL(directions.assign(adjusted_directions))) {
    LOG_WARN("failed to assign order types", K(ret));
  }
  return ret;
}

// when there is no input ordering or interesting ordering, generate a stable ordering use this function
int ObOptimizerUtil::generate_stable_ordering(common::ObIArray<ObRawExpr *> &exprs,
                                              ObIArray<ObOrderDirection> &directions,
                                              ObBitSet<64> &used_expr_idxs,
                                              ObIArray<int64_t> &expr_map,
                                              ObIArray<ObRawExpr*> &adjusted_exprs,
                                              ObIArray<ObOrderDirection> &adjusted_directions)
{
  int ret = OB_SUCCESS;
  ObSEArray<std::pair<ObRawExpr*,int64_t>, 8> sort_pairs;
  int64_t idx = OB_INVALID_INDEX;
  if (OB_UNLIKELY(exprs.count() != directions.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected array count", K(ret), K(exprs.count()), K(directions.count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    if (!used_expr_idxs.has_member(i) &&
        OB_FAIL(sort_pairs.push_back(std::pair<ObRawExpr*,int64_t>(exprs.at(i), i)))) {
      LOG_WARN("failed to push back", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
  // do not generate stable ordering now, remove comment for this sort when need stable ordering
  // lib::ob_sort(sort_pairs.begin(), sort_pairs.end(), stable_expr_cmp_func);
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < sort_pairs.count(); ++i) {
    idx = sort_pairs.at(i).second;
    if (OB_UNLIKELY(exprs.count() <= idx)) {
      LOG_WARN("unexpected idx", K(ret), K(exprs.count()), K(i), K(idx));
    } else if (OB_FAIL(adjusted_exprs.push_back(exprs.at(idx)))) {
      LOG_WARN("store ordered expr failed", K(ret), K(idx));
    } else if (OB_FAIL(expr_map.push_back(idx))) {
      LOG_WARN("failed to push back expr index", K(ret));
    } else if (OB_FAIL(adjusted_directions.push_back(directions.at(idx)))) {
      LOG_WARN("failed to push back order type", K(ret));
    } else if (OB_FAIL(used_expr_idxs.add_member(idx))) {
      LOG_WARN("add expr idxs member failed", K(ret), K(idx));
    }
  }
  return ret;
}

// used to generate specific ordering exprs
bool ObOptimizerUtil::stable_expr_cmp_func(std::pair<ObRawExpr*,int64_t> l_pair,
                                           std::pair<ObRawExpr*,int64_t> r_pair)
{
  int ret = OB_SUCCESS;
  bool bret = false;
  int64_t res = 0;
  if (OB_FAIL(cmp_expr(l_pair.first, r_pair.first, res))) {
    LOG_WARN("failed to compare two expr", K(ret), K(res), K(lbt()), KPC(l_pair.first), KPC(r_pair.first));
  } else {
    bret = (res <= 0);
  }
  return bret;
}

// used to generate specific ordering exprs
int ObOptimizerUtil::cmp_expr(ObRawExpr *l_expr, ObRawExpr *r_expr, int64_t &res)
{
  int ret = OB_SUCCESS;
  res = 0;
  if (OB_ISNULL(l_expr) || OB_ISNULL(r_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(l_expr), K(r_expr));
  } else if (l_expr->get_expr_type() != r_expr->get_expr_type()) {
    if (l_expr->is_column_ref_expr() || r_expr->is_column_ref_expr()) {
      res = l_expr->is_column_ref_expr() ? -1 : 1;
    } else {
      res = l_expr->get_expr_type() < r_expr->get_expr_type() ? -1 : 1;
    }
  } else if (l_expr->is_column_ref_expr()) {
    ObColumnRefRawExpr *l_col = static_cast<ObColumnRefRawExpr*>(l_expr);
    ObColumnRefRawExpr *r_col = static_cast<ObColumnRefRawExpr*>(r_expr);
    if (l_col->get_table_id() != r_col->get_table_id()) {
      res = l_col->get_table_id() < r_col->get_table_id() ? -1 : 1;
    } else if (l_col->get_column_id() != r_col->get_column_id()) {
      res = l_col->get_column_id() < r_col->get_column_id() ? -1 : 1;
    }
  } else if (l_expr->is_set_op_expr()) {
    res = static_cast<ObSetOpRawExpr*>(l_expr)->get_idx() < static_cast<ObSetOpRawExpr*>(r_expr)->get_idx()
          ? -1 : 1;
  } else if (l_expr->get_param_count() != r_expr->get_param_count()) {
    res = l_expr->get_param_count() < r_expr->get_param_count() ? -1 : 1;
  } else if (0 != l_expr->get_param_count()) {
    for (int64_t i = 0; 0 == res && OB_SUCC(ret) && i < l_expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(cmp_expr(l_expr->get_param_expr(i), r_expr->get_param_expr(i), res)))) {
        LOG_WARN("failed to smart call compare two expr", K(ret), K(res));
      }
    }
  } else {
    /* do nothing now */
  }
  return ret;
}

int ObOptimizerUtil::adjust_exprs_by_mapping(const common::ObIArray<ObRawExpr *> &exprs,
                                             const common::ObIArray<int64_t> &match_map,
                                             common::ObIArray<ObRawExpr *> &adjusted_exprs)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 4> tmp_array;
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

bool ObOptimizerUtil::is_same_ordering(const common::ObIArray<OrderItem> &ordering1,
                                       const common::ObIArray<OrderItem> &ordering2,
                                       const EqualSets &equal_sets)
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

bool ObOptimizerUtil::in_same_equalset(const ObRawExpr *from,
                                       const ObRawExpr *to,
                                       const EqualSets &equal_sets)
{
  bool found = false;
  int64_t N = equal_sets.count();
  for (int64_t i = 0; !found && i < N; ++i) {
    if (OB_ISNULL(equal_sets.at(i))) {
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "get null equal set");
    } else if (find_equal_expr(*equal_sets.at(i), from) &&
               find_equal_expr(*equal_sets.at(i), to)) {
      found = true;
    }
  }
  return found;
}

bool ObOptimizerUtil::is_expr_equivalent(const ObRawExpr *from,
                                         const ObRawExpr *to,
                                         const EqualSets &equal_sets)
{
  bool found = false;
  bool is_consistent = false;
  if (OB_ISNULL(from) || OB_ISNULL(to)) {
    // do nothing
  } else if (from == to) {
    found = true;
  } else if (!from->is_generalized_column() && from->same_as(*to)) {
    found = true;
  } else if (equal_sets.empty()) {
    // do nothing
  } else if (ObRawExprUtils::expr_is_order_consistent(from, to, is_consistent)
             != OB_SUCCESS) {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "check expr is order consist ent failed");
  } else if (is_consistent) {
    int64_t N = equal_sets.count();
    for (int64_t i = 0; !found && i < N; ++i) {
      if (OB_ISNULL(equal_sets.at(i))) {
        LOG_WARN_RET(OB_ERR_UNEXPECTED, "get null equal set");
      } else if (find_equal_expr(*equal_sets.at(i), from) &&
                 find_equal_expr(*equal_sets.at(i), to)) {
        found = true;
      }
    }
  }
  return found;
}

bool ObOptimizerUtil::is_exprs_equivalent(const common::ObIArray<ObRawExpr*> &from,
                                          const common::ObIArray<ObRawExpr*> &to,
                                          const EqualSets &equal_sets)
{
  bool b_ret = true;
  if (from.count() != to.count()) {
    b_ret = false;
  }
  for (int64_t i = 0; b_ret && i < from.count(); ++i) {
    b_ret = is_expr_equivalent(from.at(i), to.at(i), equal_sets);
  }
  return b_ret;
}

bool ObOptimizerUtil::is_expr_equivalent(const ObRawExpr *from,
                                         const ObRawExpr *to)
{
  bool found = false;
  if (OB_ISNULL(from) || OB_ISNULL(to)) {
    // do nothing
  } else if (from == to) {
    found = true;
  } else if (!from->is_generalized_column() && from->same_as(*to)) {
    found = true;
  }
  return found;
}

int ObOptimizerUtil::append_exprs_no_dup(ObIArray<ObRawExpr *> &dst, const ObIArray<ObRawExpr *> &src)
{
  int ret = OB_SUCCESS;
  for (int64_t idx = 0; OB_SUCC(ret) && idx < src.count(); ++idx) {
    ObRawExpr *expr = src.at(idx);
    if (find_equal_expr(dst, expr)) {
      //do nothing
    } else if (OB_FAIL(dst.push_back(expr))) {
      LOG_WARN("Add var to array error", K(ret));
    } else { } //do nothing
  }
  return ret;
}

bool ObOptimizerUtil::is_sub_expr(const ObRawExpr *sub_expr,
                                  const ObRawExpr *expr)
{
  bool found = false;
  if (NULL == sub_expr || NULL == expr) {
    /* do nothing */
  } else {
    if (sub_expr == expr) {
      found = true;
    } else if (ObRawExprUtils::is_same_raw_expr(sub_expr, expr)) {
      found = true;
    } else { /* do nothing. */ }
    for (int64_t i = 0; !found && i < expr->get_param_count(); ++i) {
      found = is_sub_expr(sub_expr, expr->get_param_expr(i));
    }
  }
  return found;
}

bool ObOptimizerUtil::is_sub_expr(const ObRawExpr *sub_expr,
                                  const ObIArray<ObRawExpr*> &exprs)
{
  bool found = false;
  for (int64_t i = 0; !found && i < exprs.count(); i++) {
    found = is_sub_expr(sub_expr, exprs.at(i));
  }
  return found;
}

bool ObOptimizerUtil::is_sub_expr(const ObRawExpr *sub_expr,
                                  ObRawExpr *&expr,
                                  ObRawExpr **&addr_matched_expr)
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

bool ObOptimizerUtil::is_point_based_sub_expr(const ObRawExpr *sub_expr, const ObRawExpr *expr)
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

int ObOptimizerUtil::is_const_expr(const ObRawExpr *expr,
                                   const EqualSets &equal_sets,
                                   const ObIArray<ObRawExpr *> &const_exprs,
                                   const ObIArray<ObRawExpr *> &exec_ref_exprs,
                                   bool &is_const)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(is_root_expr_const(expr, equal_sets, const_exprs, exec_ref_exprs, is_const))) {
    LOG_WARN("failed to check is const expr", K(ret));
  } else if (is_const) {
    // do nothing
  } else if (OB_FAIL(SMART_CALL(is_const_expr_recursively(expr, exec_ref_exprs, is_const)))) {
    LOG_WARN("failed to check const expr", K(ret));
  }
  return ret;
}

int ObOptimizerUtil::is_const_expr_recursively(const ObRawExpr *expr,
                                               const ObIArray<ObRawExpr *> &exec_ref_exprs,
                                               bool &is_const)
{
  int ret = OB_SUCCESS;
  bool is_const_inherit = true;
  is_const = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null expr", K(ret));
  } else if (OB_FAIL(is_const_expr(expr, exec_ref_exprs, is_const))) {
    LOG_WARN("failed to check const expr", K(ret));
  } else if (is_const) {
    // do nothing
  } else if (OB_FAIL(expr->is_const_inherit_expr(is_const_inherit, true))) {
    LOG_WARN("failed to check is const inherit expr", K(ret));
  } else if (is_const_inherit && expr->get_param_count() > 0) {
    bool is_param_const = true;
    for (int64_t i = 0; OB_SUCC(ret) && is_param_const && i < expr->get_param_count(); ++i) {
      const ObRawExpr *param_expr = expr->get_param_expr(i);
      if (OB_FAIL(SMART_CALL(is_const_expr_recursively(param_expr, exec_ref_exprs,
                                                       is_param_const)))) {
        LOG_WARN("failed to check param expr is const", K(ret));
      }
    }
    if (OB_SUCC(ret) && is_param_const) {
      is_const = true;
    }
  }
  return ret;
}

int ObOptimizerUtil::is_root_expr_const(const ObRawExpr *expr,
                                        const EqualSets &equal_sets,
                                        const ObIArray<ObRawExpr *> &const_exprs,
                                        const ObIArray<ObRawExpr *> &exec_ref_exprs,
                                        bool &is_const)
{
  int ret = OB_SUCCESS;
  is_const = false;
  if (OB_ISNULL(expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("expr passed in should not be NULL", K(expr), K(ret));
  } else if (OB_FAIL(is_const_expr(expr, const_exprs, is_const))) {
    LOG_WARN("failed to check if is const expr", K(expr), K(ret));
  } else if (!is_const && OB_FAIL(is_const_expr(expr, exec_ref_exprs, is_const))) {
    LOG_WARN("failed to check if is const expr", K(expr), K(ret));
  } else if (!is_const) {
    const EqualSet *equal_set = NULL;
    const ObRawExpr *cur_expr = NULL;
    bool is_consistent = false;
    for (int64_t i = 0; OB_SUCC(ret) && !is_const && i < equal_sets.count(); ++i) {
      if (OB_ISNULL(equal_set = equal_sets.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("equal set is null");
      } else if (find_equal_expr(*equal_set, expr)) {
        for (int64_t j = 0; OB_SUCC(ret) && !is_const && j < equal_set->count(); ++j) {
          if (OB_ISNULL(cur_expr = equal_set->at(j))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("expr passed in should not be NULL", K(j), K(ret));
          } else if (OB_FAIL(ObRawExprUtils::expr_is_order_consistent(cur_expr, expr, is_consistent))) {
            LOG_WARN("check expr is order consistent failed", K(ret));
          } else if (is_consistent && OB_FAIL(is_const_expr(cur_expr, const_exprs, is_const))) {
            LOG_WARN("failed to check const expr", K(cur_expr), K(ret));
          } else { /*do nothing*/ }
        }
      }
    }
  }
  return ret;
}


int ObOptimizerUtil::is_const_expr(const ObRawExpr *expr,
                                   const EqualSets &equal_sets,
                                   const ObIArray<ObRawExpr *> &const_exprs,
                                   bool &is_const)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 1> dummy_exprs;
  if (OB_FAIL(is_const_expr(expr, equal_sets, const_exprs, dummy_exprs, is_const))) {
    LOG_WARN("failed to check if is const expr", K(ret));
  }
  return ret;
}

int ObOptimizerUtil::is_const_expr(const ObRawExpr* expr,
                                   const ObIArray<ObRawExpr *> &const_exprs,
                                   bool &is_const)
{
  int ret = OB_SUCCESS;
  is_const = false;
  if (OB_ISNULL(expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("expr passed in should not be NULL", K(expr), K(ret));
  } else if (expr->is_const_expr()) { //为了能够将？+ const这种情况判断出是const，需要使用该函数判断
    is_const = true;
  } else if (find_item(const_exprs, expr)) {
    is_const = true;
  }
  return ret;
}

int ObOptimizerUtil::compute_const_exprs(const ObIArray<ObRawExpr*> &condition_exprs,
                                         ObIArray<ObRawExpr*> &const_exprs)
{
  int ret = OB_SUCCESS;
  ObRawExpr *const_expr = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < condition_exprs.count(); ++i) {
    if (OB_FAIL(compute_const_exprs(condition_exprs.at(i), const_expr))) {
      LOG_WARN("failed to compute const exprs", K(ret));
    } else if (NULL == const_expr) {
      /*do nothing*/
    } else if (OB_FAIL(add_var_to_array_no_dup(const_exprs, const_expr))) {
      LOG_WARN("failed to push back expr", K(ret));
    } else {/*do nothing*/}
  }
  return ret;
}

int ObOptimizerUtil::compute_const_exprs(ObRawExpr *cur_expr,
                                         ObRawExpr *&res_const_expr)
{
  int ret = OB_SUCCESS;
  res_const_expr = NULL;
  if (OB_ISNULL(cur_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr passed in should not be NULL", K(ret));
  } else if (T_OP_EQ == cur_expr->get_expr_type() || T_OP_IS == cur_expr->get_expr_type()) {
    ObRawExpr *param_1 = cur_expr->get_param_expr(0);
    ObRawExpr *param_2 = cur_expr->get_param_expr(1);
    if (OB_ISNULL(param_1) || OB_ISNULL(param_2)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null ptr", K(param_1), K(param_2), K(ret));
    } else if (OB_FAIL(get_expr_without_lossless_cast(param_1, param_1))) {
      LOG_WARN("failed to get expr without lossless cast", K(ret));
    } else if (OB_FAIL(get_expr_without_lossless_cast(param_2, param_2))) {
      LOG_WARN("failed to get expr without lossless cast", K(ret));
    } else {
      bool left_const = param_1->is_const_expr();
      bool right_const = param_2->is_const_expr();
      if (left_const || right_const) {
        ObRawExpr *const_expr = left_const ? param_1 : param_2;
        ObRawExpr *common_expr = left_const ? param_2 : param_1;
        if (T_OP_EQ == cur_expr->get_expr_type()) {
          bool is_const = true;
          if (!ob_is_valid_obj_tc(const_expr->get_type_class()) ||
              !ob_is_valid_obj_tc(common_expr->get_type_class())) {
            // (a, a) = (1, 1);
            is_const = false;
          } else if (OB_FAIL(ObObjCaster::is_const_consistent(const_expr->get_result_type().get_obj_meta(),
                                                              common_expr->get_result_type().get_obj_meta(),
                                                              cur_expr->get_result_type().get_calc_type(),
                                                              cur_expr->get_result_type().get_calc_meta().get_collation_type(),
                                                              is_const))) {
            LOG_WARN("check expr type is strict monotonic failed", K(ret));
          } else if (is_const) {
            res_const_expr = common_expr;
          }
        } else if (T_BOOL == const_expr->get_expr_type()) {
          // is true/false will not be regarded as const
        } else {
          res_const_expr = common_expr;
        }
      }
    }
  }
  return ret;
}

static inline Monotonicity get_opposite_of(Monotonicity mono) {
  Monotonicity ret = Monotonicity::CONST;
  if (mono == Monotonicity::ASC) {
    ret = Monotonicity::DESC;
  } else if (mono == Monotonicity::DESC) {
    ret = Monotonicity::ASC;
  } else if (mono == Monotonicity::CONST) {
    ret = Monotonicity::CONST;
  } else if (mono == Monotonicity::NONE_MONO) {
    ret = Monotonicity::NONE_MONO;
  }
  return ret;
}

int ObOptimizerUtil::get_expr_monotonicity(const ObRawExpr *expr,
                                           const ObRawExpr *var,
                                           ObExecContext &ctx,
                                           Monotonicity &monotonicity,
                                           bool &is_strict,
                                           const ParamStore &param_store,
                                           ObPCConstParamInfo& const_param_info)
{
  int ret = OB_SUCCESS;
  bool is_not_null = false;
  const ObColumnRefRawExpr *col = NULL;
  if (OB_ISNULL(expr) || OB_ISNULL(var)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected expression input is null error", K(ret));
  } else if (!var->is_column_ref_expr()) {
    monotonicity = Monotonicity::NONE_MONO;
  } else if (OB_FALSE_IT(col = static_cast<const ObColumnRefRawExpr*>(var))) {
    // never in
  } else if (OB_FAIL(get_expr_monotonicity_recursively(expr, col, ctx,
                                                       monotonicity, is_strict,
                                                       param_store, const_param_info))) {
    LOG_WARN("Failed to get expr monotonicity recursiviely ", K(ret));
  }
  return ret;
}

// require funtion type is null propagate expr
int ObOptimizerUtil::get_expr_monotonicity_recursively(const ObRawExpr* expr,
                                                       const ObColumnRefRawExpr *var,
                                                       ObExecContext& ctx,
                                                       Monotonicity &monotonicity,
                                                       bool &is_strict,
                                                       const ParamStore &param_store,
                                                       ObPCConstParamInfo& const_param_info)
{
  int ret = OB_SUCCESS;
  monotonicity = Monotonicity::NONE_MONO;
  is_strict = false;
  if (OB_ISNULL(expr) || OB_ISNULL(var)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected expression input is null error", K(ret));
  } else if (expr->is_const_raw_expr()) {
    // here if cannot merge into upperline
    // select x + 10(:?) 通过返回结果看 + int 或者+ null都会被解析为const raw expr 只有返回类型能够区分
    if (!expr->get_result_type().get_param().is_null_oracle()) {
      monotonicity = Monotonicity::CONST;
    }
  } else if (expr->is_column_ref_expr()) {
    // there is an another col in expr but is not var
    if (expr == var) {
      monotonicity = Monotonicity::ASC;
      is_strict = true;
    }
  } else {
    // The following is a classification discussion for composite cases.
    // Only one branch will be chosen for entry. Before entering, the monotonicity is none.
    Monotonicity mono = Monotonicity::NONE_MONO;
    bool is_strict_inner = false;
    if (expr->get_expr_type() == T_FUN_SYS_UPPER || expr->get_expr_type() == T_FUN_SYS_LOWER) {
      const ObRawExpr *param_expr = expr->get_param_expr(0);
      if (OB_ISNULL(param_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected expression input is null error", K(ret));
      } else if (ObCharset::is_ci_collate(param_expr->get_collation_type())) {
        if (OB_FAIL(SMART_CALL(get_expr_monotonicity_recursively(param_expr, var, ctx,
                                                                 mono, is_strict_inner,
                                                                 param_store, const_param_info)))) {
          LOG_WARN("get string param monotonicity failed", K(ret));
        } else {
          monotonicity = mono;
          is_strict = is_strict_inner;
        }
      }
    } else if (expr->get_expr_type() == T_FUN_SYS_CAST) {
      const ObRawExpr *param_expr = expr->get_param_expr(0);
      bool is_consistent = false;
      if (OB_ISNULL(param_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected exprssion input is null error", K(ret));
      } else if (OB_FAIL(ObObjCaster::is_order_consistent(param_expr->get_result_type(),
                                                          expr->get_result_type(),
                                                          is_consistent))) {
        if (OB_ERR_UNEXPECTED == ret) {
          LOG_WARN("failed to check is order consistent", K(ret));
        } else {
          ret = OB_SUCCESS;
          is_consistent = false;
        }
      } else if (is_consistent) {
        if (OB_FAIL(SMART_CALL(get_expr_monotonicity_recursively(param_expr, var, ctx,
                                                                 mono, is_strict_inner,
                                                                 param_store, const_param_info)))) {
          LOG_WARN("get data time param", K(ret));
        } else {
          monotonicity = mono;
          is_strict = false;
        }
      } else if (!is_oracle_mode()
                 && param_expr->get_result_type().is_datetime()
                 && expr->get_result_type().is_string_type()) {
        if (OB_FAIL(SMART_CALL(get_expr_monotonicity_recursively(param_expr, var, ctx,
                                                                 mono, is_strict_inner,
                                                                 param_store, const_param_info)))) {
          LOG_WARN("get data time param", K(ret));
        } else {
          monotonicity = mono;
          is_strict = is_strict_inner;
        }
      }
    } else if (expr->get_expr_type() == T_FUN_SYS_LEFT) {
      const ObRawExpr *param_expr_str = expr->get_param_expr(0);
      const ObRawExpr *param_expr_num = expr->get_param_expr(1);
      if (OB_ISNULL(param_expr_str) || OB_ISNULL(param_expr_num)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected exprssion input is null error", K(ret));
      } else if ((ObCharset::is_bin_sort(param_expr_str->get_result_type().get_collation_type()) ||
                  CS_TYPE_UTF8MB4_GENERAL_CI == param_expr_str->get_result_type().get_collation_type()) &&
                 param_expr_num->is_const_raw_expr() &&
                 !param_expr_num->get_result_type().is_null()) {
        if (OB_FAIL(SMART_CALL(get_expr_monotonicity_recursively(param_expr_str, var, ctx,
                                                                 mono, is_strict_inner,
                                                                 param_store, const_param_info)))) {
          LOG_WARN("get string param monotonicity failed", K(ret));
        } else {
          monotonicity = mono;
          is_strict = false;
        }
      }
    } else if (expr->get_expr_type() == T_FUN_SYS_FLOOR || expr->get_expr_type() == T_FUN_SYS_CEIL) {
      const ObRawExpr *param_expr = expr->get_param_expr(0);
      if (OB_ISNULL(param_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected exprssion input is null error", K(ret));
      } else if (OB_FAIL(SMART_CALL(get_expr_monotonicity_recursively(param_expr, var, ctx,
                                                                      mono, is_strict_inner,
                                                                      param_store, const_param_info)))) {
        LOG_WARN("get string param monotonicity failed", K(ret));
      } else {
        monotonicity = mono;
        is_strict = false;
      }
    } else if (expr->get_expr_type() == T_FUN_SYS_SUBSTR) {
      const ObRawExpr *param_expr_str = expr->get_param_expr(0);
      const ObRawExpr *param_expr_pos = expr->get_param_expr(1);
      const ObRawExpr *param_expr_len = expr->get_param_count() == 3 ? expr->get_param_expr(2) : NULL;
      ObSEArray<ObRawExpr*, 4> params;
      ObArenaAllocator local_allocator;
      int64_t value = 0;
      bool is_null_value = true;
      bool is_one = false;
      ObRawExpr* param_expr = NULL;
      const ObConstRawExpr *const_expr = NULL;
      ObObj target_value;
      target_value.set_int(ObIntType, 1);
      if (OB_ISNULL(param_expr_str) || OB_ISNULL(param_expr_pos)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected exprssion input is null error", K(ret));
      } else if ((ObCharset::is_bin_sort(param_expr_str->get_result_type().get_collation_type()) ||
                  CS_TYPE_UTF8MB4_GENERAL_CI == param_expr_str->get_result_type().get_collation_type()) &&
                 (param_expr_pos->is_const_raw_expr() && !param_expr_pos->get_result_type().is_null()) &&
                 (param_expr_len == NULL || (param_expr_len != NULL && param_expr_len->is_const_raw_expr() && !param_expr_len->get_result_type().is_null()))) {
        if (OB_FAIL(ObTransformUtils::get_expr_int_value(const_cast<ObRawExpr*>(param_expr_pos), &param_store,
                                                         &ctx, &local_allocator, value, is_null_value))) {
          LOG_WARN("failed to check limit value", K(ret));
          ret = OB_SUCCESS;   /* ignore the error check inside */
        } else if (is_null_value || value != 1) {
          /* monotonicity = Monotonicity::NONE_MONO; is_strict = false; */
        } else if (OB_FAIL(SMART_CALL(get_expr_monotonicity_recursively(param_expr_str, var, ctx,
                                                                        mono,is_strict_inner,
                                                                        param_store, const_param_info)))) {
          LOG_WARN("get string param monotonicity failed", K(ret));
        } else if (OB_FALSE_IT(const_expr = static_cast<const ObConstRawExpr*>(param_expr_pos))) {
        } else if (OB_FAIL(const_param_info.const_idx_.push_back(const_expr->get_value().get_unknown()))) {
          LOG_WARN("failed to push back param idx", K(ret));
        } else if (OB_FAIL(const_param_info.const_params_.push_back(target_value))) {
          LOG_WARN("failed to push back value", K(ret));
        } else {
          monotonicity = mono;
          is_strict = false;
        }
      }
    } else if (expr->get_expr_type() == T_OP_MINUS || expr->get_expr_type() == T_OP_ADD) {
      if (expr->get_param_count() != 2) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get param count", K(ret));
      } else {
        Monotonicity left_mono  = Monotonicity::NONE_MONO;
        Monotonicity right_mono = Monotonicity::NONE_MONO;
        const ObRawExpr *l_expr = expr->get_param_expr(0);
        const ObRawExpr *r_expr = expr->get_param_expr(1);
        bool is_strict_l = false;
        bool is_strict_r = false;
        if (OB_ISNULL(l_expr) || OB_ISNULL(r_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("op expr one side is null", K(l_expr), K(r_expr));
        } else if ((!l_expr->get_result_type().is_numeric_type() || !r_expr->get_result_type().is_numeric_type())
                   || (l_expr->get_result_type().is_float() || r_expr->get_result_type().is_float())
                   || (l_expr->get_result_type().is_double()|| r_expr->get_result_type().is_double())) {
          // 字符串类型是不正确的 abc < abcd 但是abcz > abcdz, 而且字符串没有减法
          // explain select distinct(t0.c2)  from t0 where upper(t0.c2) + "ZHU" = "QINGZHU";
          // 日期类型 日期加减有时候会是正确的。
          monotonicity = Monotonicity::NONE_MONO;
        } else if (OB_FAIL(SMART_CALL(get_expr_monotonicity_recursively(l_expr, var, ctx,
                                                                        left_mono, is_strict_l,
                                                                        param_store, const_param_info)))
                   || OB_FAIL(SMART_CALL(get_expr_monotonicity_recursively(r_expr, var, ctx,
                                                                           right_mono, is_strict_r,
                                                                           param_store, const_param_info)))) {
          LOG_WARN("failed to get expr monotonicity for expr", K(ret));
        } else {
          if (expr->get_expr_type() == T_OP_MINUS) {
            right_mono = get_opposite_of(right_mono);
          }
          if (left_mono == Monotonicity::CONST) {
            monotonicity = right_mono;
            is_strict = is_strict_r;
          } else if (right_mono == Monotonicity::CONST) {
            monotonicity = left_mono;
            is_strict = is_strict_l;
          } else if (left_mono == right_mono && (left_mono == Monotonicity::ASC || left_mono == Monotonicity::DESC)) {
            monotonicity = left_mono;
            is_strict = is_strict_l || is_strict_r;
          } else {
            monotonicity = Monotonicity::NONE_MONO;
          }
        }
      }
    } else {
      monotonicity = Monotonicity::NONE_MONO;
    }
  }

  return ret;
}

bool ObOptimizerUtil::overlap_exprs(const ObIArray<ObRawExpr*> &exprs1,
                                    const ObIArray<ObRawExpr*> &exprs2)
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

bool ObOptimizerUtil::subset_exprs(const ObIArray<ObRawExpr*> &sub_exprs,
                                   const ObIArray<ObRawExpr*> &exprs,
                                   const EqualSets &equal_sets)
{
  bool subset = true;
  for (int64_t i = 0; subset && i < sub_exprs.count(); ++i) {
    if (!find_equal_expr(exprs, sub_exprs.at(i), equal_sets)) {
      subset = false;
    }
  }
  return subset;
}

bool ObOptimizerUtil::subset_exprs(const ObIArray<ObRawExpr*> &sub_exprs,
                                   const ObIArray<ObRawExpr*> &exprs)
{
  bool subset = true;
  for (int64_t i = 0; subset && i < sub_exprs.count(); ++i) {
    if (!find_equal_expr(exprs, sub_exprs.at(i))) {
      subset = false;
    }
  }
  return subset;
}

int ObOptimizerUtil::prefix_subset_exprs(const ObIArray<ObRawExpr*> &sub_exprs,
                                         const uint64_t subexpr_prefix_count,
                                         const ObIArray<ObRawExpr*> &exprs,
                                         const uint64_t expr_prefix_count,
                                         bool &is_subset)
{
  int ret = OB_SUCCESS;
  is_subset = false;
  if (subexpr_prefix_count > sub_exprs.count() ||
      expr_prefix_count > exprs.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(subexpr_prefix_count), K(sub_exprs.count()),
        K(expr_prefix_count), K(exprs.count()));
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

bool ObOptimizerUtil::same_exprs(const common::ObIArray<ObRawExpr*> &src_exprs,
                                 const common::ObIArray<ObRawExpr*> &target_exprs,
                                 const EqualSets &equal_sets)
{
  int ret = OB_SUCCESS;
  if (src_exprs.count() != target_exprs.count()) {
    return false;
  } else {
    return subset_exprs(src_exprs, target_exprs, equal_sets) &&
           subset_exprs(target_exprs, src_exprs, equal_sets);
  }
  return ret;
}

bool ObOptimizerUtil::same_exprs(const common::ObIArray<ObRawExpr*> &src_exprs,
                                 const common::ObIArray<ObRawExpr*> &target_exprs)
{
  int ret = OB_SUCCESS;
  if (src_exprs.count() != target_exprs.count()) {
    return false;
  } else {
    return subset_exprs(src_exprs, target_exprs) &&
           subset_exprs(target_exprs, src_exprs);
  }
  return ret;
}

int ObOptimizerUtil::intersect_exprs(const ObIArray<ObRawExpr *> &first,
                                     const ObIArray<ObRawExpr *> &right,
                                     const EqualSets &equal_sets,
                                     ObIArray<ObRawExpr *> &result)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 4> tmp;
  for (int64_t i = 0; OB_SUCC(ret) && i < first.count(); ++i) {
    if (!find_equal_expr(right, first.at(i), equal_sets)) {
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

int ObOptimizerUtil::intersect_exprs(const ObIArray<ObRawExpr *> &first,
                                     const ObIArray<ObRawExpr *> &right,
                                     ObIArray<ObRawExpr *> &result)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 4> tmp;
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

int ObOptimizerUtil::except_exprs(const ObIArray<ObRawExpr *> &first,
                                  const ObIArray<ObRawExpr *> &right,
                                  ObIArray<ObRawExpr *> &result)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 4> tmp;
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

int ObOptimizerUtil::clone_expr_for_topk(ObRawExprFactory &expr_factory, ObRawExpr *src, ObRawExpr *&dest)
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
      case ObRawExpr::EXPR_CONST :
      case ObRawExpr::EXPR_EXEC_PARAM:
      case ObRawExpr::EXPR_COLUMN_REF :
      case ObRawExpr::EXPR_AGGR: {
        dest = src;
        break;
      }
      case ObRawExpr::EXPR_OPERATOR: {
        ObOpRawExpr *dest_op = NULL;
        if (OB_FAIL(expr_factory.create_raw_expr(src->get_expr_type(), dest_op))) {
          LOG_WARN("failed to allocate raw expr", K(ret));
        } else if (OB_ISNULL(dest = dest_op)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("dest operator expr is null", K(ret));
        } else {
          ObOpRawExpr *src_op = static_cast<ObOpRawExpr *>(src);
          if (OB_ISNULL(src_op)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("casted src_op is NULL", K(ret));
          } else if (OB_FAIL(dest_op->assign(*src_op))) {
            LOG_WARN("failed to assign expr", K(ret));
          } else {
            dest_op->clear_child();
            int64_t count = src_op->get_param_count();
            for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
              ObRawExpr *param_expr = src_op->get_param_expr(i);
              ObRawExpr *new_param_expr = NULL;
              if (OB_FAIL(SMART_CALL(clone_expr_for_topk(expr_factory, param_expr,
                                                         new_param_expr)))) {
                LOG_WARN("fail to copy_expr", K(ret));
              } else if (OB_FAIL(dest_op->add_param_expr(new_param_expr))) {
                LOG_WARN("fail to add param expr", K(ret));
              } else {/*do nothing*/}
            }
          }
        }
        break;
      }
      case ObRawExpr::EXPR_CASE_OPERATOR: {
        ObCaseOpRawExpr *dest_case = NULL;
        if (OB_FAIL(expr_factory.create_raw_expr(src->get_expr_type(), dest_case))) {
          LOG_WARN("failed to allocate raw expr", K(ret));
        } else if (OB_ISNULL(dest = dest_case)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("dest case expr is null", K(ret));
        } else {
          ObCaseOpRawExpr *src_case = static_cast<ObCaseOpRawExpr *>(src);
          if (OB_ISNULL(src_case)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("casted src_case is NULL", K(ret));
          } else if (OB_FAIL(dest_case->assign(*src_case))) {
            LOG_WARN("failed to assign expr", K(ret));
          } else {
            dest_case->clear_child();
            ObRawExpr *origin_arg = src_case->get_arg_param_expr();
            ObRawExpr *origin_default = src_case->get_default_param_expr();
            ObRawExpr *dest_arg = NULL;
            ObRawExpr *dest_default = NULL;
            if (OB_FAIL(SMART_CALL(clone_expr_for_topk(expr_factory, origin_arg, dest_arg)))) {
              LOG_WARN("fail to copy raw expr", K(ret));
            } else if (OB_FAIL(SMART_CALL(clone_expr_for_topk(expr_factory, origin_default,
                                                              dest_default)))) {
              LOG_WARN("fail to copy raw expr", K(ret));
            } else {
              dest_case->set_arg_param_expr(dest_arg);
              dest_case->set_default_param_expr(dest_default);
              int64_t count = src_case->get_when_expr_size();
              for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
                ObRawExpr *origin_when = src_case->get_when_param_expr(i);
                ObRawExpr *origin_then = src_case->get_then_param_expr(i);
                ObRawExpr *dest_when = NULL;
                ObRawExpr *dest_then = NULL;
                if (OB_FAIL(SMART_CALL(clone_expr_for_topk(expr_factory, origin_when, dest_when)))) {
                  LOG_WARN("fail to copy raw expr", K(ret));
                } else if (OB_FAIL(dest_case->add_when_param_expr(dest_when))) {
                  LOG_WARN("fail to add raw expr", K(ret));
                } else if (OB_FAIL(SMART_CALL(clone_expr_for_topk(expr_factory, origin_then,
                                                                  dest_then)))) {
                  LOG_WARN("fail to copy raw expr", K(ret));
                } else if (OB_FAIL(dest_case->add_then_param_expr(dest_then))) {
                  LOG_WARN("fail to add raw expr", K(ret), K(dest_then));
                } else {/*do nothing*/}
              }
            }
          }
        }
        break;
      }
      case ObRawExpr::EXPR_SYS_FUNC: {
        ObSysFunRawExpr *dest_sys = NULL;
        if (OB_FAIL(expr_factory.create_raw_expr(src->get_expr_type(), dest_sys))) {
          LOG_WARN("failed to allocate raw expr", K(ret));
        } else if (OB_ISNULL(dest = dest_sys)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("dest sys func is null", K(ret));
        } else {
          ObSysFunRawExpr *src_sys = static_cast<ObSysFunRawExpr *>(src);
          if (OB_ISNULL(src_sys)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("casted src sys func is null", K(ret));
          } else if (OB_FAIL(dest_sys->assign(*src_sys))) {
            LOG_WARN("failed to assign expr", K(ret));
          } else {
            dest_sys->clear_child();
            int64_t count = src_sys->get_param_count();
            for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
              ObRawExpr *param_expr = src_sys->get_param_expr(i);
              ObRawExpr *new_param_expr = NULL;
              if (OB_FAIL(SMART_CALL(clone_expr_for_topk(expr_factory, param_expr, new_param_expr)))) {
                LOG_WARN("fail to copy raw expr", K(ret));
              } else if (OB_FAIL(dest_sys->add_param_expr(new_param_expr))) {
                LOG_WARN("fail to push raw expr", K(ret));
              } else {/*do nothing*/}
            }
          }
        }
        break;
      }
      case ObRawExpr::EXPR_UDF: {
        ObUDFRawExpr *dest_udf = NULL;
        if (OB_FAIL(expr_factory.create_raw_expr(src->get_expr_type(), dest_udf))) {
          LOG_WARN("failed to allocate raw expr", K(ret));
        } else if (OB_ISNULL(dest = dest_udf)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("dest user define function is null", K(ret));
        } else {
          ObUDFRawExpr *src_udf = static_cast<ObUDFRawExpr *>(src);
          if (OB_ISNULL(src_udf)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("casted src user define function is null", K(ret));
          } else if (OB_FAIL(dest_udf->assign(*src_udf))) {
            LOG_WARN("failed to assign expr", K(ret));
          } else {
            dest_udf->clear_child();
            int64_t count = src_udf->get_param_count();
            for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
              ObRawExpr *param_expr = src_udf->get_param_expr(i);
              ObRawExpr *new_param_expr = NULL;
              if (OB_FAIL(SMART_CALL(clone_expr_for_topk(expr_factory, param_expr, new_param_expr)))) {
                LOG_WARN("fail to copy raw expr", K(ret));
              } else if (OB_FAIL(dest_udf->add_param_expr(new_param_expr))) {
                LOG_WARN("fail to push raw expr", K(ret));
              } else {/*do nothing*/}
            }
          }
        }
        break;
      }
      case ObRawExpr::EXPR_QUERY_REF :
        // TODO@nijia.nj : 暂时没有逻辑需要copy window_function, 未来还是需要加上
      case ObRawExpr::EXPR_WINDOW: {
        ret = OB_ERR_UNEXPECTED;
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

bool ObOptimizerUtil::find_equal_expr(const ObIArray<ObRawExpr*> &exprs,
                                      const ObRawExpr* expr,
                                      const EqualSets &equal_sets,
                                      int64_t &idx)
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

bool ObOptimizerUtil::find_equal_expr(const ObIArray<ObRawExpr *> &exprs,
                                      const ObRawExpr *expr,
                                      int64_t &idx)
{
  bool found = false;
  int64_t N = exprs.count();
  for (int64_t i = 0; !found && i < N; ++i) {
    if (is_expr_equivalent(exprs.at(i), expr)) {
      found = true;
      idx = i;
    }
  }
  return found;
}

int ObOptimizerUtil::find_stmt_expr_direction(const ObDMLStmt &stmt,
                                              const common::ObIArray<ObRawExpr*> &exprs,
                                              const EqualSets &equal_sets,
                                              common::ObIArray<ObOrderDirection> &directions)
{
  int ret = OB_SUCCESS;
  ObOrderDirection dir;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
    if (OB_FAIL(find_stmt_expr_direction(stmt, exprs.at(i), equal_sets, dir))) {
      LOG_WARN("failed to find stmt expr direction", K(ret));
    } else if (OB_FAIL(directions.push_back(dir))) {
      LOG_WARN("failed to push back direction", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObOptimizerUtil::find_stmt_expr_direction(const ObDMLStmt &stmt,
                                              const ObRawExpr *expr,
                                              const EqualSets &equal_sets,
                                              ObOrderDirection &direction)
{
  int ret = OB_SUCCESS;
  bool is_find = false;
  direction = default_asc_direction();
  if (stmt.is_select_stmt()) {
    const ObSelectStmt &select_stmt = static_cast<const ObSelectStmt&>(stmt);
    // find direction in window function exprs
    for (int64_t i = 0; OB_SUCC(ret) && !is_find && i < select_stmt.get_window_func_count(); ++i) {
      const ObWinFunRawExpr *win_expr = NULL;
      if (OB_ISNULL(win_expr = select_stmt.get_window_func_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("window function is null", K(ret));
      } else {
        is_find = find_expr_direction(win_expr->get_order_items(),
                                      expr,
                                      equal_sets,
                                      direction);
      }
    }
    LOG_TRACE("succeed to check expr direction in window function exprs",
        K(direction), K(is_find));
  }
  if (OB_SUCC(ret) && !is_find) {
    // find direction in order by exprs
    is_find = find_expr_direction(stmt.get_order_items(),
                                  expr,
                                  equal_sets,
                                  direction);
    LOG_TRACE("succeed to check expr direction in order by exprs", K(direction),
        K(is_find));
  }
  return ret;
}

bool ObOptimizerUtil::find_expr_direction(const ObIArray<OrderItem> &exprs,
                                          const ObRawExpr *expr,
                                          const EqualSets &equal_sets,
                                          ObOrderDirection &direction)
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

uint64_t ObOptimizerUtil::hash_array(uint64_t seed, const ObIArray<uint64_t> &data_array)
{
  uint64_t hash_value = seed;
  int64_t N = data_array.count();
  for (int64_t i = 0; i < N; ++i) {
    hash_value = common::do_hash(data_array.at(i), hash_value);
  }
  return hash_value;
}

bool ObOptimizerUtil::find_expr(ObIArray<ExprProducer> *ctx, const ObRawExpr &expr)
{
  bool found = false;
  ExprProducer *producer = NULL;
  found = find_expr(ctx, expr, producer);
  return found;
}

bool ObOptimizerUtil::find_expr(ObIArray<ExprProducer> *ctx, const ObRawExpr &expr, ExprProducer *&producer)
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

int ObOptimizerUtil::classify_equal_conds(const ObIArray<ObRawExpr *> &conds,
                                          ObIArray<ObRawExpr *> &normal_conds,
                                          ObIArray<ObRawExpr *> &nullsafe_conds)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < conds.count(); ++i) {
    if (OB_ISNULL(conds.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("condition is null", K(ret), K(conds.at(i)));
    } else if (conds.at(i)->get_expr_type() == T_OP_EQ ||
               conds.at(i)->get_expr_type() == T_OP_SQ_EQ) {
      if (OB_FAIL(normal_conds.push_back(conds.at(i)))) {
        LOG_WARN("failed to push back normal equal condition", K(ret));
      }
    } else if (conds.at(i)->get_expr_type() == T_OP_NSEQ ||
               conds.at(i)->get_expr_type() == T_OP_SQ_NSEQ) {
      if (OB_FAIL(nullsafe_conds.push_back(conds.at(i)))) {
        LOG_WARN("failed to push back nullsafe equal condition", K(ret));
      }
    }
  }
  return ret;
}

int ObOptimizerUtil::get_equal_keys(const ObIArray<ObRawExpr*> &exprs,
                                    const ObRelIds &left_table_sets,
                                    ObIArray<ObRawExpr*> &left_keys,
                                    ObIArray<ObRawExpr*> &right_keys,
                                    ObIArray<bool> &null_safe_info)
{
  int ret = OB_SUCCESS;
  ObRawExpr *temp_expr = NULL;
  ObRawExpr *left_expr = NULL;
  ObRawExpr *right_expr = NULL;
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
      bool is_null_safe = (temp_expr->get_expr_type() == T_OP_NSEQ ||
                           temp_expr->get_expr_type() == T_OP_SQ_NSEQ);
      if (!left_expr->get_relation_ids().is_subset(left_table_sets)) {
        std::swap(left_expr, right_expr);
      }
      if (OB_FAIL(left_keys.push_back(left_expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else if (OB_FAIL(right_keys.push_back(right_expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else if (OB_FAIL(null_safe_info.push_back(is_null_safe))) {
        LOG_WARN("failed to push back null safe info", K(ret));
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

bool ObOptimizerUtil::find_exec_param(const common::ObIArray<ObExecParamRawExpr *> &params,
                                      const ObExecParamRawExpr *ele)
{
  bool bret = false;
  if (NULL != ele) {
    for (int64_t i = 0; !bret && i < params.count(); ++i) {
      if (params.at(i) != NULL) {
        bret = params.at(i)->get_param_index() == ele->get_param_index();
      }
    }
  }
  return bret;
}

int ObOptimizerUtil::get_exec_ref_expr(const ObIArray<ObExecParamRawExpr *> &exec_params,
                                       ObIArray<ObRawExpr *> &ref_exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exec_params.count(); ++i) {
    if (OB_ISNULL(exec_params.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("exec param is null", K(ret));
    } else if (OB_FAIL(ref_exprs.push_back(exec_params.at(i)->get_ref_expr()))) {
      LOG_WARN("failed to push back ref expr", K(ret));
    }
  }
  return ret;
}

ObRawExpr* ObOptimizerUtil::find_exec_param(const ObIArray<std::pair<int64_t, ObRawExpr*> > &params,
                                            const int64_t param_num)
{
  ObRawExpr *expr = NULL;
  bool found = false;
  for (int64_t i = 0; !found && i < params.count(); ++i) {
    if (params.at(i).first == param_num) {
      expr = params.at(i).second;
      found = true;
    }
  }
  return expr;
}

int64_t ObOptimizerUtil::find_exec_param(const ObIArray<std::pair<int64_t, ObRawExpr*> > &params,
                                         const ObRawExpr *expr)
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

int ObOptimizerUtil::extract_equal_exec_params(const ObIArray<ObRawExpr *> &exprs,
                                               const ObIArray<ObExecParamRawExpr *> &my_params,
                                               ObIArray<ObRawExpr *> &left_key,
                                               ObIArray<ObRawExpr *> &right_key,
                                               ObIArray<bool> &null_safe_info)

{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    ObRawExpr *cur_expr =  exprs.at(i);
    if (OB_ISNULL(cur_expr)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("expr passed in is NULL", K(ret));
    } else if (T_OP_EQ == cur_expr->get_expr_type() || T_OP_NSEQ == cur_expr->get_expr_type()) {
      bool is_null_safe = (T_OP_NSEQ == cur_expr->get_expr_type());
      if (OB_ISNULL(cur_expr->get_param_expr(0)) || OB_ISNULL(cur_expr->get_param_expr(1))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Invalid op expr", K(*cur_expr), K(ret));
      } else if (cur_expr->get_param_expr(0)->is_exec_param_expr()) {
        ObExecParamRawExpr *exec_param = static_cast<ObExecParamRawExpr *>(cur_expr->get_param_expr(0));
        if (!find_item(my_params, exec_param)) {
          /*
           * 在参数列表里没找到，说明这不是一个与自己相关的参数，跳过即可
           * 例如NL的内表里有条件b1=? and b2=?,其中第一个？是一个const在parser阶段被参数化出来的，第二个？是外表相关
           * 属性a1，那么第一个?在这里是肯定找不到的，跳过即可。
           */
        } else if (OB_FAIL(left_key.push_back(exec_param->get_ref_expr()))) {
          LOG_WARN("push back error", K(ret));
        } else if (OB_FAIL(right_key.push_back(cur_expr->get_param_expr(1)))) {
          LOG_WARN("push back error", K(ret));
        } else if (OB_FAIL(null_safe_info.push_back(is_null_safe))) {
          LOG_WARN("push back error", K(ret));
        } else { /*do nothing*/ }
      } else if (cur_expr->get_param_expr(1)->is_exec_param_expr()) {
        ObExecParamRawExpr *exec_param = static_cast<ObExecParamRawExpr *>(cur_expr->get_param_expr(1));
        if (!find_item(my_params, exec_param)) {
          // not my exec param
        } else if (OB_FAIL(left_key.push_back(exec_param->get_ref_expr()))) {
          LOG_WARN("push back error", K(ret));
        } else if (OB_FAIL(right_key.push_back(cur_expr->get_param_expr(0)))) {
          LOG_WARN("push back error", K(ret));
        } else if (OB_FAIL(null_safe_info.push_back(is_null_safe))) {
          LOG_WARN("push back error", K(ret));
        } else { /*do nothing*/ }
      } else { /*do nothing*/ }
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObOptimizerUtil::generate_rowkey_exprs(const ObDMLStmt *stmt,
                                           ObOptimizerContext &opt_ctx,
                                           const uint64_t table_id,
                                           const uint64_t ref_table_id,
                                           ObIArray<ObRawExpr*> &keys,
                                           ObIArray<ObRawExpr*> &ordering)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = NULL;
  ObSqlSchemaGuard *schema_guard = NULL;
  if (OB_ISNULL(schema_guard = opt_ctx.get_sql_schema_guard())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(schema_guard), K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(table_id, ref_table_id, stmt, table_schema))) {
    LOG_WARN("fail to get table schema", K(ref_table_id), K(table_schema), K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index schema should not be null", K(table_schema), K(ret));
  } else if (OB_FAIL(generate_rowkey_exprs(stmt,
                                           opt_ctx.get_expr_factory(),
                                           table_id,
                                           *table_schema,
                                           keys,
                                           ordering))) {
    LOG_WARN("failed to get rowkeys raw expr", K(table_id), K(ref_table_id), K(ret));
  } else { /*do nothing*/ }

  return ret;
}

int ObOptimizerUtil::generate_rowkey_exprs(const ObDMLStmt* cstmt,
                                           ObRawExprFactory &expr_factory,
                                           uint64_t table_id,
                                           const ObTableSchema &index_table_schema,
                                           ObIArray<ObRawExpr*> &index_keys,
                                           ObIArray<ObRawExpr*> &index_ordering)
{
  int ret = OB_SUCCESS;
  ObDMLStmt *stmt = const_cast<ObDMLStmt *>(cstmt);
  if (OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(stmt), K(ret));
  } else {
    // get all the index keys
    const ObRowkeyInfo &rowkey_info = index_table_schema.get_rowkey_info();
    const ObColumnSchemaV2 *column_schema = NULL;
    ObColumnRefRawExpr *expr = NULL;
    for (int col_idx = 0; OB_SUCC(ret) && col_idx < rowkey_info.get_size(); ++col_idx) {
      uint64_t  column_id = OB_INVALID_ID;
      if (OB_FAIL(rowkey_info.get_column_id(col_idx, column_id))) {
        LOG_WARN("Failed to get column_id from rowkey_info", K(ret));
      } else if (OB_ISNULL(column_schema = (index_table_schema.get_column_schema(column_id)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get column schema", K(column_id), K(ret));
      } else {
        ObRawExpr *raw_expr = NULL;
        //!!!Dangerous. Different Unique indexes' hidden columns have the same column ids.
        //Now, we haven't thought the trouble this may cause. But, if we do multi-index scan
        //or other opt need to use different indexes in one logical plan, remember this problem.
        if (NULL != (raw_expr = stmt->get_column_expr_by_id(table_id, column_id))) {
          expr = static_cast<ObColumnRefRawExpr*>(raw_expr);
        } else if (OB_FAIL(generate_rowkey_expr(stmt, expr_factory, table_id, *column_schema, expr))) {
          LOG_WARN("failed to get row key expr", K(ret));
        } else { /*do nothing*/ }

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

int ObOptimizerUtil::build_range_columns(const ObDMLStmt *stmt,
                                         ObIArray<ObRawExpr*> &rowkeys,
                                         ObIArray<ColumnItem> &range_columns)
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
        ObColumnRefRawExpr *expr = static_cast<ObColumnRefRawExpr*>(rowkeys.at(i));
        const ColumnItem *column_item = stmt->get_column_item_by_id(expr->get_table_id(), expr->get_column_id());
        //@notice: be careful, range column only use the attribute that table_id, column_id and column type
        //other attributes do not guarantee the correctness
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

int ObOptimizerUtil::find_equal_set(const ObRawExpr* ordering,
                                    const EqualSets &equal_sets,
                                    ObIArray<const EqualSet*> &found_sets)
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

int ObOptimizerUtil::extract_row_col_idx_for_in(const common::ObIArray<uint64_t> &column_ids,
                                                const int64_t index_col_pos,
                                                const uint64_t table_id,
                                                const ObRawExpr &l_expr,
                                                const ObRawExpr &r_expr,
                                                common::ObBitSet<> &col_idxs,
                                                int64_t &min_col_idx,
                                                bool &is_table_filter)
{
  int ret = OB_SUCCESS;
  // banliu.zyd: init_xxx 记录第一次extract的结果（即对IN表达式的第一个值）
  // 我们期望对于IN中的每一项，extract结果都是一致的，如果有不一致，则不放到prefix_filter里
  // 如果每一项都是一致的，最终是否放到prefix_filter再由外层决定
  common::ObBitSet<> init_col_idxs = col_idxs;
  int64_t init_min_col_idx = min_col_idx;
  bool init_is_table_filter = false;
  // banliu.zyd: tmp_xxx 临时记录extract的结果

  // extract的结果是否和第一次结果一致
  bool equal = true;
  for (int64_t i = 0; OB_SUCC(ret) && equal && i < r_expr.get_param_count(); ++i) {
    const ObRawExpr *expr = r_expr.get_param_expr(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr should not be NULL", K(ret));
    } else {
      bool tmp_is_table_filter = false;
      common::ObBitSet<> tmp_col_idxs = col_idxs;
      int64_t tmp_min_col_idx = min_col_idx;
      if (i == 0) {
        if (OB_FAIL(extract_row_col_idx(column_ids, index_col_pos, table_id, l_expr,
                                        *expr, init_col_idxs, init_min_col_idx, init_is_table_filter))) {
          LOG_WARN("extract_row_col_idx for vector IN-expr failed", K(ret));
        }
      } else if (OB_FAIL(extract_row_col_idx(column_ids, index_col_pos, table_id,
                         l_expr, *expr, tmp_col_idxs, tmp_min_col_idx, tmp_is_table_filter))) {
        LOG_WARN("extract_row_col_idx for vector IN-expr failed", K(ret));
      } else if (!(tmp_col_idxs == init_col_idxs)
                 || tmp_min_col_idx != init_min_col_idx
                 || tmp_is_table_filter != init_is_table_filter) {
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

//row的处理应该作为一个整体来处理，对query range有影响的有效的列应该为按照column_ids有序的索引列
//比如(a,b,c,d) 为索引(a,b) + 主键(c,d)
//(a,b) > (1,1) 那么对range有影响的列为a,b
//(b,c) > (1,1) 或者 (b,a) > (1,1) b可能对range有影响，会把b放入col_idxs中，外面会再进行判断。
//(b,c) > (a,b) 这种range是判断不出来的，不放入col_idxs中
int ObOptimizerUtil::extract_row_col_idx(
  const ObIArray<uint64_t> &column_ids,
  const int64_t index_col_pos,
  const uint64_t table_id,
  const ObRawExpr &l_expr,
  const ObRawExpr &r_expr,
  ObBitSet<> &col_idxs,
  int64_t &min_col_idx,
  bool &is_table_filter)
{
  int ret = OB_SUCCESS;
  if (l_expr.get_param_count() != r_expr.get_param_count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param count is invalid", K(ret),
        K(l_expr.get_param_count()), K(r_expr.get_param_count()));
  } else if (T_OP_ROW == l_expr.get_expr_type() && T_OP_ROW == r_expr.get_expr_type()) {
    const ObRawExpr *column_expr = NULL;
    bool check_next = true;
    int64_t cur_col_idx = INITED_VALUE;
    int64_t last_idx = INITED_VALUE;
    int64_t i = 0;
    is_table_filter = true;
    for (i = 0 ; check_next && OB_SUCC(ret) && i < l_expr.get_param_count(); ++i) {
      column_expr = NULL;
      if (OB_ISNULL(l_expr.get_param_expr(i))
          || OB_ISNULL(r_expr.get_param_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("param expr i is null", K(ret));
      } else if (l_expr.get_param_expr(i)->is_column_ref_expr()
                 && r_expr.get_param_expr(i)->has_flag(IS_CONST)) {
        column_expr = l_expr.get_param_expr(i);
      } else if (r_expr.get_param_expr(i)->is_column_ref_expr()
                 && l_expr.get_param_expr(i)->has_flag(IS_CONST)) {
        column_expr = r_expr.get_param_expr(i);
      } else {
        check_next = false;
      }
      // add col_idx to bitset
      if (OB_SUCC(ret) && check_next) {
        if (OB_FAIL(extract_column_idx(column_ids, index_col_pos, table_id, column_expr, cur_col_idx, col_idxs))) {
          LOG_WARN("extract column idx failed", K(ret));
        } else if (cur_col_idx < 0
                   || (last_idx >= 0 && last_idx + 1 != cur_col_idx)) {
          // cur_col_idx为抽取的列id在column_idx中的pos
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

int ObOptimizerUtil::extract_column_idx(
    const ObIArray<uint64_t> &column_ids,
    const int64_t index_col_pos,
    const uint64_t table_id,
    const ObRawExpr *raw_expr,
    int64_t &col_idx,
    ObBitSet<> &col_idxs,
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
    const ObColumnRefRawExpr *col_ref = static_cast<const ObColumnRefRawExpr*>(raw_expr);
    const uint64_t col_id = col_ref->get_column_id();
    if (col_ref->get_table_id() != table_id) { //considered as const value
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
          } else { /*do nothing*/ }
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
      col_idx = MUL_INDEX_COL;//cannot be prefix filter
    }
    if ((IS_BASIC_CMP_OP(type) || T_OP_IN == type)
        && N == 2
        && (NULL == raw_expr->get_param_expr(0) || NULL == raw_expr->get_param_expr(1))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Invalid raw expr", K(*raw_expr), K(ret));
    } else if ((IS_BASIC_CMP_OP(type) || T_OP_IN == type)
        && N == 2
        && T_OP_ROW == raw_expr->get_param_expr(0)->get_expr_type()) {
      const ObRawExpr &l_expr = *(raw_expr->get_param_expr(0));
      const ObRawExpr &r_expr = *(raw_expr->get_param_expr(1));
      int64_t min_col_idx = column_ids.count();
      bool is_table_filter = false;
      if (T_OP_IN == type) {
        if (OB_FAIL(extract_row_col_idx_for_in(column_ids, index_col_pos, table_id,
                                               l_expr, r_expr, col_idxs, min_col_idx, is_table_filter))) {
          LOG_WARN("Extract colum idx error", K(ret));
        }
      } else {
        if (OB_FAIL(extract_row_col_idx(column_ids, index_col_pos, table_id,
                                        l_expr, r_expr, col_idxs, min_col_idx, is_table_filter))) {
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
        if (OB_FAIL(SMART_CALL(extract_column_idx(column_ids, index_col_pos, table_id,
                                       raw_expr->get_param_expr(i), col_idx, col_idxs)))) {
          LOG_WARN("Extrac column idx error", K(ret));
        } else if (TABLE_RELATED == col_idx) {
          is_related = true;
        } else { /*do nothing*/ }
      }
    }
  }
  return ret ;
}

bool ObOptimizerUtil::is_query_range_op(const ObItemType type)
{
  bool b_ret = false;
  if (IS_BASIC_CMP_OP(type)
      || T_OP_LIKE == type
      || T_OP_IS == type
      || T_OP_BTW == type
      || T_OP_NOT_BTW == type
      || T_OP_IN == type
      || T_OP_AND == type
      || T_OP_OR == type
      || T_OP_ROW == type) {
    b_ret = true;
  }
  return b_ret;
}

int ObOptimizerUtil::get_child_corresponding_exprs(const ObDMLStmt *upper_stmt,
                                                   const ObSelectStmt *stmt,
                                                   const ObIArray<ObRawExpr*> &exprs,
                                                   ObIArray<ObRawExpr*> &corr_exprs)
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
        } else if (!exprs.at(i)->is_column_ref_expr()) { //不是column可以直接报错了
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("not a column expr", K(exprs.at(i)), K(i), K(ret));
        } else if (static_cast<ObColumnRefRawExpr*>(exprs.at(i))->get_table_id() != subquery_id) {
          ret = corr_exprs.push_back(NULL); //NULL说明这个表达式找不到child stmt与之对应的表达式
        } else {
          int64_t idx = static_cast<int64_t>(static_cast<ObColumnRefRawExpr*>(exprs.at(i))->get_column_id() - OB_APP_MIN_COLUMN_ID);
          ret = corr_exprs.push_back(stmt->get_select_item(idx).expr_);
        }
      }
    }
  }
  return ret;
}


int ObOptimizerUtil::get_child_corresponding_exprs(const TableItem *table,
                                                   const ObIArray<ObRawExpr*> &exprs,
                                                   ObIArray<ObRawExpr*> &corr_exprs)
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
      } else if (!exprs.at(i)->is_column_ref_expr()) { //不是column可以直接报错了
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("not a column expr", K(exprs.at(i)), K(i), K(ret));
      } else if (static_cast<ObColumnRefRawExpr*>(exprs.at(i))->get_table_id() != table->table_id_) {
        ret = corr_exprs.push_back(NULL); //NULL说明这个表达式找不到child stmt与之对应的表达式
      } else {
        int64_t idx = static_cast<int64_t>(static_cast<ObColumnRefRawExpr*>(exprs.at(i))->get_column_id() - OB_APP_MIN_COLUMN_ID);
        ret = corr_exprs.push_back(table->ref_query_->get_select_item(idx).expr_);
      }
    }
  }
  return ret;
}

int ObOptimizerUtil::get_subquery_id(const ObDMLStmt *upper_stmt, const ObSelectStmt *stmt, uint64_t &id)
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
      } else if (upper_stmt->get_table_item(i)->is_generated_table()
                 && upper_stmt->get_table_item(i)->ref_query_ == stmt) {
        id = upper_stmt->get_table_item(i)->table_id_;
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

/**
 * @brief 
 *  given source and target table id, try to find its lowest common joined table
 *  then check if the source table is on the null side of the joined table
 * @param stmt 
 * @param source_table_id 
 * @param target_table_id 
 * @param is_on_null_side 
 * @return int 
 */
int ObOptimizerUtil::is_table_on_null_side_of_parent(const ObDMLStmt *stmt,
                                                     uint64_t source_table_id,
                                                     uint64_t target_table_id,
                                                     bool &is_on_null_side)
{
  int ret = OB_SUCCESS;
  is_on_null_side = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("upper_stmt is null", K(ret));
  } else {
    JoinedTable *common_joined_table = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && OB_ISNULL(common_joined_table) &&
         i < stmt->get_joined_tables().count(); ++i) {
      JoinedTable *joined_table = stmt->get_joined_tables().at(i);
      bool is_source_in_joined_table = false;
      if (OB_ISNULL(joined_table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(find_common_joined_table(joined_table,
                                                  source_table_id,
                                                  target_table_id,
                                                  common_joined_table))) {
        LOG_WARN("failed to find target joined table", K(ret));
      } else if (common_joined_table != NULL &&
                 OB_FAIL(is_table_on_null_side_recursively(common_joined_table,
                                                           source_table_id,
                                                           is_source_in_joined_table,
                                                           is_on_null_side))) {
        LOG_WARN("Check for generated table on null side recursively fails", K(is_on_null_side), K(i), K(ret));
      }
    }
  }
  return ret;
}

int ObOptimizerUtil::find_common_joined_table(JoinedTable *joined_table,
                                              uint64_t source_table_id,
                                              uint64_t target_table_id,
                                              JoinedTable *&target_joined_table)
{
  int ret = OB_SUCCESS;
  TableItem *left_table = NULL;
  TableItem *right_table = NULL;
  if (OB_ISNULL(joined_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_ISNULL(left_table = joined_table->left_table_) ||
             OB_ISNULL(right_table = joined_table->right_table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(left_table), K(right_table));
  } else if (is_contain(joined_table->single_table_ids_, source_table_id) &&
             is_contain(joined_table->single_table_ids_, target_table_id)) {
    target_joined_table = joined_table;
    JoinedTable *left_common_table = NULL;
    JoinedTable *right_common_table = NULL;
    if (left_table->is_joined_table() &&
        OB_FAIL(find_common_joined_table(static_cast<JoinedTable *>(left_table),
                                         source_table_id,
                                         target_table_id,
                                         left_common_table))) {
      LOG_WARN("failed to find left common joined table", K(ret));
    } else if (left_common_table != NULL) {
      target_joined_table = left_common_table;
    } else if (right_table->is_joined_table() &&
               OB_FAIL(find_common_joined_table(static_cast<JoinedTable *>(right_table),
                                                source_table_id,
                                                target_table_id,
                                                right_common_table))) {
      LOG_WARN("failed to find right common joined table", K(ret));
    } else if (right_common_table != NULL) {
      target_joined_table = right_common_table;
    }
  }
  return ret;
}

int ObOptimizerUtil::is_table_on_null_side(const ObDMLStmt *stmt, uint64_t table_id, bool &is_on_null_side)
{
  int ret = OB_SUCCESS;
  is_on_null_side = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("upper_stmt is null", K(ret));
  } else {
    bool is_in_joined_table = false;
    for (int64_t i = 0;
         OB_SUCC(ret) && !is_in_joined_table && i < stmt->get_joined_tables().count();
         i++) {
      JoinedTable *joined_table = stmt->get_joined_tables().at(i);
      if (OB_FAIL(is_table_on_null_side_recursively(joined_table, table_id, is_in_joined_table, is_on_null_side))) {
        LOG_WARN("Check for generated table on null side recursively fails", K(is_on_null_side), K(i), K(ret));
      }
    }
  }
  return ret;
}

int ObOptimizerUtil::is_table_on_null_side_recursively(const TableItem *table_item,
                                                       uint64_t table_id,
                                                       bool &found,
                                                       bool &is_on_null_side)
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
      TableItem *left_table = NULL;
      TableItem *right_table = NULL;
      const JoinedTable *joined_table = static_cast<const JoinedTable *>(table_item);
      ObJoinType join_type = joined_table->joined_type_;
      if (OB_ISNULL(left_table = joined_table->left_table_)
          || OB_ISNULL(right_table = joined_table->right_table_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Get unexpected null", K(ret), K(table_id), K(table_item),
            K(left_table), K(right_table), K(is_on_null_side));
      } else if (FULL_OUTER_JOIN == join_type) {
        is_on_null_side = true;
      } else if (LEFT_OUTER_JOIN == join_type) {
        if (OB_FAIL(find_table_item(right_table, table_id, found))) {
          LOG_WARN("Find in joined table fails", K(ret));
        } else if (found) {
          is_on_null_side = true;
        } else {
          if (OB_FAIL(SMART_CALL(is_table_on_null_side_recursively(left_table, table_id,
                                                                   found, is_on_null_side)))) {
            LOG_WARN("Checking for table on null side recursively fails", K(ret));
          }
        }
      } else if (RIGHT_OUTER_JOIN == join_type) {
        if (OB_FAIL(find_table_item(left_table, table_id, found))) {
          LOG_WARN("Find in joined table fails", K(ret));
        } else if (found) {
          is_on_null_side = true;
        } else {
          if (OB_FAIL(SMART_CALL(is_table_on_null_side_recursively(right_table, table_id,
                                                                   found, is_on_null_side)))) {
            LOG_WARN("Checking for table on null side recursively fails", K(ret));
          }
        }
      } else { // Other join type
        if (OB_FAIL(SMART_CALL(is_table_on_null_side_recursively(left_table, table_id,
                                                                 found, is_on_null_side)))) {
          LOG_WARN("Checking for table on null side recursively fails", K(ret));
        }
        if (OB_SUCC(ret) && !found) {
          if (OB_FAIL(SMART_CALL(is_table_on_null_side_recursively(right_table, table_id,
                                                                   found, is_on_null_side)))) {
            LOG_WARN("Checking for table on null side recursively fails", K(ret));
          }
        }
      }
    }
  }

  return ret;
}

int ObOptimizerUtil::find_table_item(const TableItem *table_item, uint64_t table_id, bool &found)
{
  int ret = OB_SUCCESS;
  found = false;
  if (OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("joined_table is null", K(ret), K(table_item));
  } else if (table_item->is_joined_table()) {
    const JoinedTable *joined_table = static_cast<const JoinedTable *>(table_item);
    found = find_item(joined_table->single_table_ids_, table_id);
  } else {
    found = table_id == table_item->table_id_;
  }
  return ret;
}

int ObOptimizerUtil::get_referenced_columns(const ObDMLStmt *stmt,
                                            const uint64_t table_id,
                                            const common::ObIArray<ObRawExpr*> &keys,
                                            common::ObIArray<ObRawExpr*> &columns)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get_stmt() returns null", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_column_size(); ++i) {
      const ColumnItem *col_item = stmt->get_column_item(i);
      if (OB_ISNULL(col_item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("col_item is null", K(i), K(ret));
      } else if (OB_ISNULL(col_item->expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null", K(ret));
      } else if (col_item->table_id_ == table_id
          && col_item->expr_->is_explicited_reference()
          && find_item(keys, col_item->expr_)) {
        ret = columns.push_back(col_item->expr_);
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObOptimizerUtil::get_non_referenced_columns(const ObDMLStmt *stmt,
                                                const uint64_t table_id,
                                                const common::ObIArray<ObRawExpr*> &keys,
                                                common::ObIArray<ObRawExpr*> &columns)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get_stmt() returns null", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_column_size(); ++i) {
      const ColumnItem *col_item = stmt->get_column_item(i);
      if (OB_ISNULL(col_item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("col_item is null", K(i), K(ret));
      } else if (OB_ISNULL(col_item->expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null", K(ret));
      } else if (col_item->table_id_ == table_id
          && col_item->expr_->is_explicited_reference()
          && !find_item(keys, col_item->expr_)) {
        ret = columns.push_back(col_item->expr_);
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObOptimizerUtil::extract_parameterized_correlated_filters(const ObIArray<ObRawExpr*> &filters,
                                                              ObIArray<ObRawExpr*> &correlated_filters,
                                                              ObIArray<ObRawExpr*> &uncorrelated_filters)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < filters.count(); ++i) {
    if (OB_ISNULL(filters.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("filter is invalid", K(ret));
    } else if (OB_FAIL(filters.at(i)->has_flag(CNT_DYNAMIC_PARAM))) {
      if (OB_FAIL(correlated_filters.push_back(filters.at(i)))) {
        LOG_WARN("failed to push back correlated filters", K(ret));
      }
    } else {
      if (OB_FAIL(uncorrelated_filters.push_back(filters.at(i)))) {
        LOG_WARN("failed to push back uncorrelated filters", K(ret));
      }
    }
  }//end for
  return ret;
}

int ObOptimizerUtil::add_parameterized_expr(ObRawExpr *&target_expr,
                                            ObRawExpr *orig_expr,
                                            ObRawExpr *child_expr,
                                            int64_t child_idx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(target_expr) || OB_ISNULL(child_expr) || OB_ISNULL(orig_expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null pointer passed in", K(ret));
  } else {
    switch(target_expr->get_expr_class()) {
    case ObRawExpr::EXPR_CASE_OPERATOR:{
      ObCaseOpRawExpr *new_expr = static_cast<ObCaseOpRawExpr*>(target_expr);
      ObCaseOpRawExpr *origin_expr = static_cast<ObCaseOpRawExpr*>(orig_expr);
      if (0 > child_idx || child_idx >= origin_expr->get_param_count()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid index for case op operator", K(ret));
      } else if (T_OP_ARG_CASE == orig_expr->get_expr_type()) {
        //param vector: arg expr, when expr, then expr ... default expr
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
        //param vector: when expr, then expr ... default expr
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
      if (child_idx != 0 && T_FUN_GROUP_CONCAT != target_expr->get_expr_type()
          && T_FUN_COUNT != target_expr->get_expr_type()) {
        LOG_WARN("invalid index for agg expr except group_concat and count", K(ret), K(*orig_expr));
      } else {
        ObAggFunRawExpr *cur_expr = static_cast<ObAggFunRawExpr*>(target_expr);
        //cur_expr->set_param_expr(to_add_expr);
        if (0 != cur_expr->get_real_param_count() &&
            T_FUN_GROUP_CONCAT != cur_expr->get_expr_type() && T_FUN_COUNT != target_expr->get_expr_type()) {
          LOG_WARN("except group_concat and count, now, agg expr real param count must be 0", K(ret), K(*cur_expr));
        } else if (OB_FAIL(cur_expr->add_real_param_expr(child_expr))) {
          LOG_WARN("failed to add expr to param expr", K(ret));
        }
      }
      break;
    }

    case ObRawExpr::EXPR_OPERATOR: //fall through
    case ObRawExpr::EXPR_SYS_FUNC:
    case ObRawExpr::EXPR_UDF: {
      ObOpRawExpr *cur_expr = static_cast<ObOpRawExpr*>(target_expr);
      if (OB_FAIL(cur_expr->add_param_expr(child_expr))) {
        LOG_WARN("failed to add expr to param expr", K(ret));
      }
      break;
    }
    default:
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid expr type passed in", K(ret));
    } //switch case end
  }
  return ret;
}

int ObOptimizerUtil::extract_column_ids(const ObRawExpr *expr,
                                        const uint64_t table_id,
                                        ObIArray<uint64_t> &column_ids)
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
        ObColumnRefRawExpr *column = static_cast<ObColumnRefRawExpr*>(columns.at(i));
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

int ObOptimizerUtil::extract_column_ids(const ObRawExpr *expr,
                                        const uint64_t table_id,
                                        ObBitSet<> &column_ids)
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

int ObOptimizerUtil::extract_column_ids(const ObIArray<ObRawExpr*> &exprs,
                                        const uint64_t table_id,
                                        ObIArray<uint64_t> &column_ids)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 16> ids;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    if (OB_FAIL(extract_column_ids(exprs.at(i), table_id, ids))) {
      LOG_WARN("failed to extract column ids", K(exprs.at(i)), K(table_id), K(ret));
    } else if (OB_FAIL(append(column_ids, ids))) {
      LOG_WARN("add members error", K(ids), K(column_ids), K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObOptimizerUtil::extract_column_ids(const ObIArray<ObRawExpr*> &exprs,
                                        const uint64_t table_id,
                                        ObBitSet<> &column_ids)
{
  int ret = OB_SUCCESS;
  ObBitSet<> ids;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    if (OB_FAIL(extract_column_ids(exprs.at(i), table_id, ids))) {
      LOG_WARN("failed to extract column ids", K(exprs.at(i)), K(table_id), K(ret));
    } else if (OB_FAIL(column_ids.add_members(ids))) {
      LOG_WARN("add members error", K(ids), K(column_ids), K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObOptimizerUtil::is_same_table(
  const ObIArray<OrderItem> &exprs,
  uint64_t &table_id,
  bool &is_same)
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
        ObColumnRefRawExpr *column = static_cast<ObColumnRefRawExpr*>(exprs.at(i).expr_);
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

int ObOptimizerUtil::get_default_directions(const int64_t direction_num,
                                            ObIArray<ObOrderDirection> &directions)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < direction_num; ++i) {
    if (OB_FAIL(directions.push_back(default_asc_direction()))) {
      LOG_WARN("failed to push back default asc direction", K(ret));
    }
  }
  return ret;
}

int ObOptimizerUtil::make_sort_keys(const ObIArray<ObRawExpr*> &sort_exprs,
                                    ObIArray<OrderItem> &sort_keys)
{
  int ret = OB_SUCCESS;
  sort_keys.reuse();
  OrderItem key;
  for (int64_t i = 0; OB_SUCC(ret) && i < sort_exprs.count(); ++i) {
    key.expr_ = sort_exprs.at(i);
    ret = sort_keys.push_back(key);
  }
  return ret;
}

int ObOptimizerUtil::make_sort_keys(const ObIArray<ObRawExpr*> &sort_exprs,
                                    const ObOrderDirection direction,
                                    ObIArray<OrderItem> &sort_keys)
{
  int ret = OB_SUCCESS;
  for (int64_t idx = 0; OB_SUCC(ret) && idx < sort_exprs.count(); ++idx) {
    if (OB_FAIL(sort_keys.push_back(OrderItem(sort_exprs.at(idx), direction)))) {
      LOG_WARN("Failed to add sort key", K(ret));
    }
  }
  return ret;
}

int ObOptimizerUtil::make_sort_keys(const ObIArray<OrderItem> &candi_sort_keys,
                                    const ObIArray<ObRawExpr *> &need_sort_exprs,
                                    ObIArray<OrderItem> &sort_keys)
{
  int ret = OB_SUCCESS;
  sort_keys.reuse();
  for (int64_t i = 0; OB_SUCC(ret) && i < candi_sort_keys.count()
                      && sort_keys.count() < need_sort_exprs.count(); ++i) {
    if (candi_sort_keys.at(i).expr_ == need_sort_exprs.at(sort_keys.count())) {
      ret = sort_keys.push_back(candi_sort_keys.at(i));
    }
  }
  if (OB_SUCC(ret) && sort_keys.count() != need_sort_exprs.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected candi sort key/sort exprs", K(ret), K(candi_sort_keys),
                                                     K(need_sort_exprs));
  }
  return ret;
}

int ObOptimizerUtil::make_sort_keys(const ObIArray<ObRawExpr *> &candi_sort_exprs,
                                    const ObIArray<ObOrderDirection> &candi_directions,
                                    const ObIArray<ObRawExpr *> &need_sort_exprs,
                                    ObIArray<OrderItem> &sort_keys)
{
  int ret = OB_SUCCESS;
  sort_keys.reset();
  if (candi_directions.count() != candi_sort_exprs.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected candi sort exprs/directions count", K(ret), K(candi_sort_exprs),
                                                             K(candi_directions));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < candi_sort_exprs.count()
                      && sort_keys.count() < need_sort_exprs.count(); ++i) {
    if (candi_sort_exprs.at(i) == need_sort_exprs.at(sort_keys.count())) {
      ret = sort_keys.push_back(OrderItem(candi_sort_exprs.at(i), candi_directions.at(i)));
    }
  }
  if (sort_keys.count() != need_sort_exprs.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected candi sort key/sort exprs", K(ret), K(candi_sort_exprs),
                                                     K(need_sort_exprs));
  }
  return ret;
}

int ObOptimizerUtil::make_sort_keys(
    const ObIArray<ObRawExpr *> &sort_exprs,
    const ObIArray<ObOrderDirection> &directions,
    ObIArray<OrderItem> &sort_keys)
{
  int ret = OB_SUCCESS;
  if (sort_exprs.count() != directions.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("exprs number dismatches directions number",
             K(ret), K(sort_exprs.count()), K(directions.count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < sort_exprs.count(); ++i) {
    OrderItem sort_key(sort_exprs.at(i), directions.at(i));
    if (OB_FAIL(sort_keys.push_back(sort_key))) {
      LOG_WARN("failed to add sort key", K(ret));
    }
  }
  return ret;
}

int ObOptimizerUtil::split_expr_direction(const ObIArray<OrderItem> &order_items,
                                          ObIArray<ObRawExpr*> &raw_exprs,
                                          ObIArray<ObOrderDirection> &directions)
{
  int ret = OB_SUCCESS;
  for (int64_t idx = 0; OB_SUCC(ret) && idx < order_items.count(); ++idx) {
    if (OB_FAIL(raw_exprs.push_back(order_items.at(idx).expr_))) {
      LOG_WARN("Failed to add expr", K(ret));
    } else if (OB_FAIL(directions.push_back(order_items.at(idx).order_type_))) {
      LOG_WARN("Failed to add direction", K(ret));
    } else { }//do nothing
  }
  return ret;
}

int ObOptimizerUtil::get_expr_and_types(const common::ObIArray<OrderItem> &order_items,
                                        ObIArray<ObRawExpr*> &order_exprs,
                                        ObIArray<ObExprResType> &order_types)
{
  int ret = OB_SUCCESS;
  ObRawExpr *expr = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < order_items.count(); i++) {
    if (OB_ISNULL(expr = order_items.at(i).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(expr), K(ret));
    } else if (OB_FAIL(order_exprs.push_back(expr))) {
      LOG_WARN("failed to get order exprs", K(ret));
    } else if (OB_FAIL(order_types.push_back(expr->get_result_type()))) {
      LOG_WARN("failed to push back expr", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObOptimizerUtil::check_equal_query_ranges(const ObIArray<ObNewRange*> &ranges,
                                              const int64_t prefix_len,
                                              bool &all_prefix_equal,
                                              bool &all_full_equal)
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
    if (OB_ISNULL(ranges.at(0)) ||
        OB_UNLIKELY((rowkey_count = ranges.at(0)->start_key_.length()) <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error", K(rowkey_count), K(ranges.at(0)), K(ret));
    } else if (OB_FAIL(check_prefix_ranges_count(ranges,
                                                 equal_prefix_count,
                                                 equal_prefix_null_count,
                                                 range_prefix_count,
                                                 contain_always_false))) {
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

int ObOptimizerUtil::check_prefix_ranges_count(const ObIArray<common::ObNewRange*> &ranges,
                                               int64_t &equal_prefix_count,
                                               int64_t &equal_prefix_null_count,
                                               int64_t &range_prefix_count,
                                               bool &contain_always_false)
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
      ObNewRange *range = ranges.at(i);
      int64_t temp_equal_prefix_count = 0;
      int64_t temp_range_prefix_count = 0;
      if (OB_ISNULL(range)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null query range", K(ret));
      } else if (range->start_key_.length() != range->end_key_.length()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid start and end range key", K(range->start_key_.length()),
                    K(range->end_key_.length()), K(ret));
      } else if (range->start_key_.ptr()[0].is_max_value() && range->end_key_.ptr()[0].is_min_value()) {
        contain_always_false = true;
        equal_prefix_count = 0;
        range_prefix_count = 0;
      } else if (OB_FAIL(check_prefix_range_count(range,
                                                  temp_equal_prefix_count,
                                                  temp_range_prefix_count))) {
        LOG_WARN("failed to check range prefix", K(ret));
      } else {
        equal_prefix_count = std::min(equal_prefix_count, temp_equal_prefix_count);
        range_prefix_count = std::min(range_prefix_count, temp_range_prefix_count);
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < ranges.count(); ++i) {
      const ObNewRange *range = ranges.at(i);
      int64_t temp_equal_prefix_null_count = 0;
      if (OB_FAIL(check_equal_prefix_null_count(range, equal_prefix_count,
                                                temp_equal_prefix_null_count))) {
        LOG_WARN("failed to check range prefix", K(ret));
      } else {
        equal_prefix_null_count = std::max(equal_prefix_null_count, temp_equal_prefix_null_count);
      }
    }
  }
  return ret;
}

int ObOptimizerUtil::check_prefix_ranges_count(const ObIArray<common::ObNewRange> &ranges,
                                               int64_t &equal_prefix_count,
                                               int64_t &equal_prefix_null_count,
                                               int64_t &range_prefix_count)
{
  int ret = OB_SUCCESS;
  equal_prefix_count = 0;
  equal_prefix_null_count = 0;
  range_prefix_count = 0;
  if (ranges.count() > 0) {
    equal_prefix_count = OB_USER_MAX_ROWKEY_COLUMN_NUMBER;
    range_prefix_count = OB_USER_MAX_ROWKEY_COLUMN_NUMBER;
    for (int64_t i = 0; OB_SUCC(ret) && i < ranges.count(); ++i) {
      const ObNewRange &range = ranges.at(i);
      int64_t temp_equal_prefix_count = 0;
      int64_t temp_range_prefix_count = 0;
      if (range.start_key_.length() != range.end_key_.length()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid start and end range key", K(range.start_key_.length()),
                    K(range.end_key_.length()), K(ret));
      } else if (OB_FAIL(check_prefix_range_count(&range,
                                                  temp_equal_prefix_count,
                                                  temp_range_prefix_count))) {
        LOG_WARN("failed to check range prefix", K(ret));
      } else {
        equal_prefix_count = std::min(equal_prefix_count, temp_equal_prefix_count);
        range_prefix_count = std::min(range_prefix_count, temp_range_prefix_count);
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < ranges.count(); ++i) {
      const ObNewRange &range = ranges.at(i);
      int64_t temp_equal_prefix_null_count = 0;
      if (OB_FAIL(check_equal_prefix_null_count(&range, equal_prefix_count,
                                                temp_equal_prefix_null_count))) {
        LOG_WARN("failed to check range prefix", K(ret));
      } else {
        equal_prefix_null_count = std::max(equal_prefix_null_count, temp_equal_prefix_null_count);
      }
    }
  }
  return ret;
}

int ObOptimizerUtil::check_equal_prefix_null_count(const common::ObNewRange *range,
                                                   const int64_t equal_prefix_count,
                                                   int64_t &equal_prefix_null_count)
{
  int ret = OB_SUCCESS;
  equal_prefix_null_count = 0;
  if (OB_ISNULL(range)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null query range", K(ret));
  } else if (range->start_key_.length() != range->end_key_.length()
             || equal_prefix_count > range->start_key_.length()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid start and end range key", K(ret), K(equal_prefix_count),
                                                K(range->start_key_.length()),
                                                K(range->end_key_.length()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < equal_prefix_count; ++i) {
    if (range->start_key_.ptr()[i] != range->end_key_.ptr()[i]) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected equal range", K(ret), K(range->start_key_.ptr()[i]),
                                         K(range->end_key_.ptr()[i]));
    } else if ((is_oracle_mode() && range->start_key_.ptr()[i].is_null_oracle())
                || range->start_key_.ptr()[i].is_null()) {
      ++equal_prefix_null_count;
    } else { /* do nothing */ }
  }
  return ret;
}

int ObOptimizerUtil::check_prefix_range_count(const common::ObNewRange *range,
                                              int64_t &equal_prefix_count,
                                              int64_t &range_prefix_count)
{
  int ret = OB_SUCCESS;
  equal_prefix_count = 0;
  range_prefix_count = 0;
  if (OB_ISNULL(range)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null query range", K(ret));
  } else if (range->start_key_.length() != range->end_key_.length()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid start and end range key", K(range->start_key_.length()),
                K(range->end_key_.length()), K(ret));
  } else {
    equal_prefix_count = range->start_key_.length();
    for (int64_t i = 0; OB_SUCC(ret) && i < range->start_key_.length()
        && equal_prefix_count == range->start_key_.length(); ++i) {
      if (range->start_key_.ptr()[i].is_min_value() ||
          range->start_key_.ptr()[i].is_max_value() ||
          range->end_key_.ptr()[i].is_min_value() ||
          range->end_key_.ptr()[i].is_max_value()) {
        equal_prefix_count = i;
      } else if (range->start_key_.ptr()[i] != range->end_key_.ptr()[i]) {
        equal_prefix_count = i;
      } else { /* do nothing */ }
    }
    range_prefix_count = range->start_key_.length();
    for (int64_t i = 0; OB_SUCC(ret) && i < range->start_key_.length() &&
        range_prefix_count == range->start_key_.length(); ++i) {
      if ((range->start_key_.ptr()[i].is_min_value() ||
           range->start_key_.ptr()[i].is_max_value()) &&
          (range->end_key_.ptr()[i].is_min_value() ||
           range->end_key_.ptr()[i].is_max_value())) {
        range_prefix_count = i;
      } else if (range->start_key_.ptr()[i].is_min_value() ||
                 range->start_key_.ptr()[i].is_max_value() ||
                 range->end_key_.ptr()[i].is_min_value() ||
                 range->end_key_.ptr()[i].is_max_value() ||
                 range->start_key_.ptr()[i] != range->end_key_.ptr()[i]) {
        range_prefix_count = i + 1;
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

bool ObOptimizerUtil::same_partition_exprs(const common::ObIArray<ObRawExpr *> &l_exprs,
                                           const common::ObIArray<ObRawExpr *> &r_exprs,
                                           const EqualSets &equal_sets)
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

int ObOptimizerUtil::classify_subquery_exprs(const ObIArray<ObRawExpr*> &exprs,
                                             ObIArray<ObRawExpr*> &subquery_exprs,
                                             ObIArray<ObRawExpr*> &non_subquery_exprs,
                                             const bool with_onetime)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
    ObRawExpr *temp_expr = exprs.at(i);
    if (OB_ISNULL(temp_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null expr", K(ret), K(temp_expr));
    } else if (temp_expr->has_flag(CNT_SUB_QUERY) ||
               (with_onetime && temp_expr->has_flag(CNT_ONETIME))) {
      // used to allocate subplan filter
      if (OB_FAIL(subquery_exprs.push_back(temp_expr))) {
        LOG_WARN("failed to push back subquery exprs", K(ret));
      } else { /*do nothing*/ }
    } else {
      if (OB_FAIL(non_subquery_exprs.push_back(temp_expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObOptimizerUtil::get_subquery_exprs(const ObIArray<ObRawExpr*> &exprs,
                                        ObIArray<ObRawExpr*> &subquery_exprs,
                                        const bool with_onetime /*=true*/)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
    if (OB_ISNULL(exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null expr", K(ret));
    } else if (exprs.at(i)->has_flag(CNT_SUB_QUERY)) {
      if (OB_FAIL(subquery_exprs.push_back(exprs.at(i)))) {
        LOG_WARN("failed to push back exprs", K(ret));
      }
    } else if (with_onetime && exprs.at(i)->has_flag(CNT_ONETIME)) {
      if (OB_FAIL(subquery_exprs.push_back(exprs.at(i)))) {
        LOG_WARN("failed to push back exprs", K(ret));
      }
    }
  }
  return ret;
}

int ObOptimizerUtil::get_onetime_exprs(ObRawExpr* expr,
                                       ObIArray<ObExecParamRawExpr*> &onetime_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(expr), K(ret));
  } else if (expr->has_flag(IS_ONETIME)) {
    if (OB_FAIL(onetime_exprs.push_back(static_cast<ObExecParamRawExpr*>(expr)))) {
      LOG_WARN("failed to push back expr", K(ret));
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); i++) {
      if (OB_FAIL(SMART_CALL(get_onetime_exprs(expr->get_param_expr(i),
                                               onetime_exprs)))) {
        LOG_WARN("failed to get one time expr", K(ret));
      }
    }
  }
  return ret;
}

int ObOptimizerUtil::get_query_ref_exprs(ObIArray<ObRawExpr *> &exprs,
                                         ObIArray<ObRawExpr *> &subqueries,
                                         ObIArray<ObRawExpr *> &nested_subqueries)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObQueryRefRawExpr *, 4> tmp;
  if (OB_FAIL(ObTransformUtils::extract_query_ref_expr(exprs,
                                                       tmp,
                                                       false))) {
    LOG_WARN("failed to extract query ref exprs", K(ret));
  } else if (OB_FAIL(append(subqueries, tmp))) {
    LOG_WARN("failed to append subqueries", K(ret));
  } else {
    tmp.reuse();
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < subqueries.count(); ++i) {
    ObRawExpr *expr = NULL;
    if (OB_ISNULL(expr = subqueries.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("subquery expr is null", K(ret), K(expr));
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < expr->get_param_count(); ++j) {
      if (OB_FAIL(ObTransformUtils::extract_query_ref_expr(expr->get_param_expr(j),
                                                           tmp,
                                                           false))) {
        LOG_WARN("failed to extract nested subqueries", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(append(nested_subqueries, tmp))) {
    LOG_WARN("failed to append nested subqueries", K(ret));
  }
  return ret;
}

int ObOptimizerUtil::get_nested_exprs(ObIArray<ObQueryRefRawExpr *> &exprs,
                                      ObIArray<ObRawExpr *> &nested_exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    if (OB_ISNULL(exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is null", K(ret));
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < exprs.at(i)->get_param_count(); ++j) {
      ObRawExpr *param = NULL;
      if (OB_ISNULL(param = exprs.at(i)->get_param_expr(j))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("param expr is null", K(ret));
      } else if (!param->has_flag(CNT_SUB_QUERY) && !param->has_flag(CNT_ONETIME)) {
        // do nothing
      } else if (OB_FAIL(nested_exprs.push_back(param))) {
        LOG_WARN("failed to push back nested exprs", K(ret));
      }
    }
  }
  return ret;
}

int ObOptimizerUtil::get_non_const_expr_size(const ObIArray<ObRawExpr *> &exprs,
                                             const EqualSets &equal_sets,
                                             const ObIArray<ObRawExpr *> &const_exprs,
                                             const ObIArray<ObRawExpr *> &exec_ref_exprs,
                                             int64_t &number)
{
  int ret = OB_SUCCESS;
  bool is_const = false;
  number = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    if (OB_FAIL(is_const_expr(exprs.at(i), equal_sets, const_exprs, exec_ref_exprs, is_const))) {
      LOG_WARN("failed to check is const expr", K(ret));
    } else if (!is_const) {
      ++number;
    }
  }
  return ret;
}

int ObOptimizerUtil::classify_get_scan_ranges(const common::ObIArray<ObNewRange> &input_ranges,
                                              common::ObIArray<ObNewRange> &get_ranges,
                                              common::ObIArray<ObNewRange> &scan_ranges)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < input_ranges.count(); i++) {
    if (input_ranges.at(i).is_single_rowkey()) {
      if (OB_FAIL(get_ranges.push_back(input_ranges.at(i)))) {
        LOG_WARN("failed to push back ranges", K(ret));
      } else { /*do nothing*/ }
    } else {
      if (OB_FAIL(scan_ranges.push_back(input_ranges.at(i)))) {
        LOG_WARN("failed to push back scan ranges", K(ret));
      } else { /*do nothing*/}
    }
  }
  return ret;
}

int ObOptimizerUtil::is_exprs_unique(const ObIArray<OrderItem> &ordering,
                                     const ObIArray<ObFdItem *> &fd_item_set,
                                     const EqualSets &equal_sets,
                                     const ObIArray<ObRawExpr *> &const_exprs,
                                     bool &order_unique)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 6> order_exprs;
  ObSEArray<ObOrderDirection, 6> order_directions;
  order_unique = false;
  if (OB_FAIL(split_expr_direction(ordering, order_exprs, order_directions))) {
    LOG_WARN("failed to split expr and direction", K(ret));
  } else if (OB_FAIL(is_exprs_unique(order_exprs, fd_item_set, equal_sets,
                                     const_exprs, order_unique))) {
    LOG_WARN("failed to check is order unique", K(ret));
  }
  return ret;
}

int ObOptimizerUtil::is_exprs_unique(const ObIArray<OrderItem> &ordering,
                                     const ObRelIds &all_tables,
                                     const ObIArray<ObFdItem *> &fd_item_set,
                                     const EqualSets &equal_sets,
                                     const ObIArray<ObRawExpr *> &const_exprs,
                                     bool &order_unique)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 6> order_exprs;
  ObSEArray<ObOrderDirection, 6> order_directions;
  order_unique = false;
  if (OB_FAIL(split_expr_direction(ordering, order_exprs, order_directions))) {
    LOG_WARN("failed to split expr and direction", K(ret));
  } else if (OB_FAIL(is_exprs_unique(order_exprs, all_tables, fd_item_set,
                                     equal_sets, const_exprs, order_unique))) {
    LOG_WARN("failed to check is order unique", K(ret));
  }
  return ret;
}

int ObOptimizerUtil::is_exprs_unique(const ObIArray<ObRawExpr *> &exprs,
                                     const ObRelIds &all_tables,
                                     const ObIArray<ObFdItem *> &fd_item_set,
                                     const EqualSets &equal_sets,
                                     const ObIArray<ObRawExpr *> &const_exprs,
                                     bool &is_unique)
{
  int ret = OB_SUCCESS;
  is_unique = false;
  ObSEArray<ObRawExpr *, 8> fd_set_parent_exprs;
  ObSEArray<ObRawExpr *, 8> extend_exprs;
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
    //使用 extend_exprs 判断是否 unique, 同时扩充 extend_exprs, 当 extend_exprs 数量增加且未 unique 时,
    //迭代进行检查, 暂时设定最大迭代次数为 10
    for (int64_t i = 0; OB_SUCC(ret) && !is_unique && i < 10
                        && extend_exprs.count() != exprs_count; ++i) {
      exprs_count = extend_exprs.count();
      if (OB_FAIL(is_exprs_unique(extend_exprs, remain_tables, fd_item_set,
                                  fd_set_parent_exprs, skip_fd, equal_sets,
                                  const_exprs, is_unique))) {
        LOG_WARN("failed to get fd set parent exprs ", K(ret));
      }
    }
  }
  return ret;
}


int ObOptimizerUtil::is_exprs_unique(ObIArray<ObRawExpr *> &extend_exprs,
                                     ObRelIds &remain_tables,
                                     const ObIArray<ObFdItem *> &fd_item_set,
                                     ObIArray<ObRawExpr *> &fd_set_parent_exprs,
                                     ObSqlBitSet<> &skip_fd,
                                     const EqualSets &equal_sets,
                                     const ObIArray<ObRawExpr *> &const_exprs,
                                     bool &is_unique)
{
  int ret = OB_SUCCESS;
  bool is_contain = false;
  ObFdItem *fd_item = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && !is_unique && i < fd_item_set.count(); ++i) {
    is_contain = false;
    if (OB_ISNULL(fd_item = fd_item_set.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null fd item", K(ret));
    } else if (skip_fd.has_member(i)) {
      /*do nothing*/
    } else if (fd_item->is_unique()) { //unique fd item
      if (OB_FAIL(is_exprs_contain_fd_parent(extend_exprs, *fd_item, equal_sets,
                                              const_exprs, is_contain))) {
        LOG_WARN("failed to check is order unique", K(ret));
      } else if (is_contain) {
        is_unique = true;
      }
    } else if (fd_item->is_table_fd_item()) { //not unique table fd item
      ObTableFdItem *table_fd_item = static_cast<ObTableFdItem *>(fd_item);
      if (!remain_tables.overlap(table_fd_item->get_child_tables())) {
        /*do nothing*/
      } else if (OB_FAIL(is_exprs_contain_fd_parent(extend_exprs, *fd_item, equal_sets,
                                                    const_exprs, is_contain))) {
        LOG_WARN("failed to check is order unique", K(ret));
      } else if (!is_contain) {
        /*do nothing*/
      } else if (OB_FAIL(remain_tables.del_members(table_fd_item->get_child_tables()))) {
        LOG_WARN("failed to delete members", K(ret));
      } else if (OB_FAIL(split_child_exprs(fd_item, equal_sets, fd_set_parent_exprs,
                                           extend_exprs))) {
        LOG_WARN("failed to delete members", K(ret));
      }
      is_unique = (0 == remain_tables.num_members());
    } else { //not unique expr fd item
      if (OB_FAIL(is_exprs_contain_fd_parent(extend_exprs, *fd_item, equal_sets,
                                             const_exprs, is_contain))) {
        LOG_WARN("failed to check is order unique", K(ret));
      } else if (is_contain && OB_FAIL(split_child_exprs(fd_item, equal_sets, fd_set_parent_exprs,
                                                         extend_exprs))) {
        LOG_WARN("failed to delete members", K(ret));
      }
    }
    if (OB_SUCC(ret) && is_contain && OB_FAIL(skip_fd.add_member(i))) {
      LOG_WARN("failed to add member", K(ret));
    }
  }
  return ret;
}

int ObOptimizerUtil::is_exprs_unique(const ObIArray<ObRawExpr *> &exprs,
                                     const ObIArray<ObFdItem *> &fd_item_set,
                                     const EqualSets &equal_sets,
                                     const ObIArray<ObRawExpr *> &const_exprs,
                                     bool &is_unique)
{
  int ret = OB_SUCCESS;
  is_unique = false;
  for (int64_t i = 0; OB_SUCC(ret) && !is_unique && i < fd_item_set.count(); ++i) {
    if (OB_ISNULL(fd_item_set.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (!fd_item_set.at(i)->is_unique()) {
      // do nothing
    } else if (OB_FAIL(is_exprs_contain_fd_parent(exprs, *fd_item_set.at(i),
                                                  equal_sets, const_exprs, is_unique))) {
      LOG_WARN("failed to check is order unique", K(ret));
    }
  }
  return ret;
}

int ObOptimizerUtil::get_fd_set_parent_exprs(const ObIArray<ObFdItem *> &fd_item_set,
                                             ObIArray<ObRawExpr *> &fd_set_parent_exprs)
{
  int ret = OB_SUCCESS;
  ObFdItem *fd_item = NULL;
  const ObRawExprSet *parent_exprs = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < fd_item_set.count(); ++i) {
    if (OB_ISNULL(fd_item = fd_item_set.at(i))
        || OB_ISNULL(parent_exprs = fd_item->get_parent_exprs())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else {
      for (int64_t j = 0; OB_SUCC(ret) && j < parent_exprs->count(); ++j) {
        if (OB_FAIL(add_var_to_array_no_dup(fd_set_parent_exprs, parent_exprs->at(j)))) {
          LOG_WARN("failed to append array no dup", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObOptimizerUtil::split_child_exprs(const ObFdItem *fd_item,
                                       const EqualSets &equal_sets,
                                       ObIArray<ObRawExpr *> &exprs,
                                       ObIArray<ObRawExpr *> &child_exprs)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 8> other_exprs;
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

int ObOptimizerUtil::is_expr_is_determined(const ObIArray<ObRawExpr *> &exprs,
                                           const ObFdItemSet &fd_item_set,
                                           const EqualSets &equal_sets,
                                           const ObIArray<ObRawExpr *> &const_exprs,
                                           const ObRawExpr *expr,
                                           bool &is_determined)
{
  int ret = OB_SUCCESS;
  is_determined = false;
  const ObFdItem *fd_item = NULL;
  for (int64_t fd_idx = 0; OB_SUCC(ret) && !is_determined && fd_idx < fd_item_set.count(); ++fd_idx) {
    if (OB_ISNULL(fd_item = fd_item_set.at(fd_idx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null fd item", K(ret));
    } else if (OB_FAIL(is_exprs_contain_fd_parent(exprs, *fd_item, equal_sets, const_exprs, is_determined))) {
      LOG_WARN("failed to check is exprs contain fd parent", K(ret));
    } else if (is_determined && OB_FAIL(fd_item->check_expr_in_child(expr, equal_sets, is_determined))) {
      LOG_WARN("failed to check expr in fd child", K(ret));
    }
  }
  return ret;
}

int ObOptimizerUtil::is_exprs_contain_fd_parent(const ObIArray<ObRawExpr *> &exprs,
                                                const ObFdItem &fd_item,
                                                const EqualSets &equal_sets,
                                                const ObIArray<ObRawExpr *> &const_exprs,
                                                bool &is_contain)
{
  int ret = OB_SUCCESS;
  is_contain = true;
  bool is_const = false;
  const ObRawExprSet *parent_exprs = NULL;
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
int ObOptimizerUtil::add_fd_item_set_for_n21_join(ObFdItemFactory &fd_factory,
                                                  ObIArray<ObFdItem *> &target,
                                                  const ObIArray<ObFdItem *> &source,
                                                  const ObIArray<ObRawExpr *> &join_exprs,
                                                  const EqualSets &equal_sets,
                                                  const ObRelIds &right_table_set)
{
  int ret = OB_SUCCESS;
  bool is_in_child = false;
  // if join unique, matched fd item must be table level fd item
  for (int64_t i = 0; OB_SUCC(ret) && i < source.count(); ++i) {
    ObFdItem *fd_item = source.at(i);
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
        ObTableFdItem *new_fd_item = NULL;
        if (fd_item->is_table_fd_item()) {
          // source fd item and matched fd item are both table level
          if (OB_FAIL(fd_factory.create_table_fd_item(new_fd_item,
                                                      *static_cast<ObTableFdItem *>(fd_item)))) {
            LOG_WARN("failed to copy fd item", K(ret));
          }
        // source fd item is expr level but matched fd item is table level
        } else if (OB_FAIL(fd_factory.create_table_fd_item(new_fd_item,
                                                           fd_item->is_unique(),
                                                           fd_item->get_parent_exprs()))) {
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
int ObOptimizerUtil::add_fd_item_set_for_n2n_join(ObFdItemFactory &fd_factory,
                                                  ObIArray<ObFdItem *> &target,
                                                  const ObIArray<ObFdItem *> &source)
{
  int ret = OB_SUCCESS;
  ObFdItem *new_fd_item = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < source.count(); ++i) {
    const ObFdItem *fd_item = source.at(i);
    if (OB_ISNULL(fd_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null fd item", K(ret));
    } else if (!fd_item->is_unique()) {
      new_fd_item = const_cast<ObFdItem *>(fd_item);
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

int ObOptimizerUtil::enhance_fd_item_set(const ObIArray<ObRawExpr *> &quals,
                                         ObIArray<ObFdItem *> &candi_fd_item_set,
                                         ObIArray<ObFdItem *> &fd_item_set,
                                         ObIArray<ObRawExpr *> &not_null_columns)
{
  int ret = OB_SUCCESS;
  bool is_happend = false;
  ObFdItem *fd_item = NULL;
  ObRawExprSet *parent_exprs = NULL;
  ObSEArray<ObFdItem *, 8> unused;
  for (int64_t i = 0; OB_SUCC(ret) && i < candi_fd_item_set.count(); ++i) {
    if (OB_ISNULL(fd_item = candi_fd_item_set.at(i)) ||
        OB_ISNULL(parent_exprs = fd_item->get_parent_exprs())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null fd item", K(ret));
    } else {
      bool all_not_null = true;
      bool contain_not_null = false;
      for (int64_t j = 0; OB_SUCC(ret) && j < parent_exprs->count(); ++j) {
        ObRawExpr *cur_expr = parent_exprs->at(j);
        bool has_null_reject = false;
        if (ObOptimizerUtil::find_item(not_null_columns, cur_expr)) {
          contain_not_null = true;
        } else if (OB_FAIL(ObTransformUtils::has_null_reject_condition(quals,
                                                                       cur_expr,
                                                                       has_null_reject))) {
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
        if (all_not_null || (lib::is_oracle_mode() && contain_not_null)) {
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

int ObOptimizerUtil::try_add_fd_item(const ObDMLStmt *stmt,
                                     ObFdItemFactory &fd_factory,
                                     const uint64_t table_id,
                                     ObRelIds &tables,
                                     const ObTableSchema *index_schema,
                                     const ObIArray<ObRawExpr *> &quals,
                                     ObIArray<ObRawExpr *> &not_null_columns,
                                     ObIArray<ObFdItem *> &fd_item_set,
                                     ObIArray<ObFdItem *> &candi_fd_item_set)
{
  int ret = OB_SUCCESS;
  bool all_columns_used = true;
  bool all_not_null = true;
  bool contain_not_null = false;
  const TableItem *table = NULL;
  ObSEArray<uint64_t, 8> column_ids;
  ObSEArray<ObRawExpr *, 8> unique_exprs;
  if (OB_ISNULL(stmt) || OB_ISNULL(index_schema)
      || OB_ISNULL(table = stmt->get_table_item_by_id(table_id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(stmt), K(index_schema));
  } else if (OB_ALL_VIRTUAL_TENANT_INFO_TID != table->ref_id_ &&
             is_virtual_table(table->ref_id_)) {
    /*虚拟表不产生 fd 及 not null 信息*/
  } else if (!index_schema->get_rowkey_info().is_valid()) {
    // do nothing
  } else if (OB_FAIL(index_schema->get_rowkey_info().get_column_ids(column_ids))) {
    LOG_WARN("failed to get index cols", K(ret));
  //new heap table not add partition key in rowkey and the tablet id is unique in partition,
  //we need check partition key
  } else if (index_schema->is_heap_table() && index_schema->get_partition_key_info().is_valid() &&
             OB_FAIL(index_schema->get_partition_key_info().get_column_ids(column_ids))) {
    LOG_WARN("failed to add part column ids", K(ret));
  } else if (index_schema->is_heap_table() && index_schema->get_subpartition_key_info().is_valid() &&
             OB_FAIL(index_schema->get_subpartition_key_info().get_column_ids(column_ids))) {
    LOG_WARN("failed to add subpart column ids", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && all_columns_used && i < column_ids.count(); ++i) {
      ObRawExpr *col_expr = NULL;
      bool is_not_null = false;
      if (is_shadow_column(column_ids.at(i))) {
        continue;
      } else if (OB_ISNULL(col_expr = stmt->get_column_expr_by_id(table_id, column_ids.at(i)))) {
        //column is not used by current stmt, skip
        all_columns_used = false;
      } else if (OB_FAIL(unique_exprs.push_back(col_expr))) {
        LOG_WARN("failed to push back unique col", K(ret));
      } else if (ObOptimizerUtil::find_item(not_null_columns, col_expr)) {
        // col_expr is not null
        contain_not_null = true;
        continue;
      } else if ((is_not_null = col_expr->is_not_null_for_read())) {
        // do nothing
      } else if (OB_FAIL(ObTransformUtils::has_null_reject_condition(
                          quals, col_expr, is_not_null))) {
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
    if (OB_SUCC(ret) && ObTransformUtils::need_compute_fd_item_set(unique_exprs)) {
      if (OB_SUCC(ret) && all_columns_used && unique_exprs.count() > 0) {
        ObTableFdItem *fd_item = NULL;
        if (OB_FAIL(fd_factory.create_table_fd_item(fd_item, true, unique_exprs, tables))) {
          LOG_WARN("failed to create fd item", K(ret));
        } else if (all_not_null ||
                  (lib::is_oracle_mode() && contain_not_null) ||
                  index_schema->get_table_id() == table->ref_id_) {
          // 1. 在oracle中, unique index (c1,c2) 允许存在多个 (null, null), 但不允许存在多个 (1, null),
          //    因此oracle模式下只要unique index中有一列是not null的, 该index中就不存在重复的值
          // 2. the primary index must be unique even if a partition table may have a nullable part-key
          //    wihch is a part of the primary key.
          if (OB_FAIL(fd_item_set.push_back(fd_item))) {
            LOG_WARN("failed to push back fd item", K(ret));
          }
        } else if (OB_FAIL(candi_fd_item_set.push_back(fd_item))) {
          LOG_WARN("failed to push back fd item", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObOptimizerUtil::convert_subplan_scan_equal_sets(ObIAllocator *allocator,
                                                     ObRawExprFactory &expr_factory,
                                                     const uint64_t table_id,
                                                     const ObDMLStmt &parent_stmt,
                                                     const ObSelectStmt &child_stmt,
                                                     const EqualSets &input_equal_sets,
                                                     EqualSets &output_equal_sets)
{
  int ret = OB_SUCCESS;
  EqualSets dummy_equal_sets;
  ObSEArray<ObRawExpr *, 8> raw_eset;
  TableItem *table_item = NULL;
  if (OB_ISNULL(table_item = parent_stmt.get_table_item_by_id(table_id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (table_item->is_lateral_table()) {
    // do nothing
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < input_equal_sets.count(); ++i) {
      const EqualSet *input_eset = input_equal_sets.at(i);
      raw_eset.reuse();
      if (OB_ISNULL(input_eset)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(ObOptimizerUtil::convert_subplan_scan_expr(expr_factory,
                                                                    dummy_equal_sets,
                                                                    table_id,
                                                                    parent_stmt,
                                                                    child_stmt,
                                                                    true,
                                                                    *input_eset,
                                                                    raw_eset))) {
      LOG_WARN("failed to convert subplan scan expr", K(ret));
      } else if (raw_eset.count() > 1 &&
                OB_FAIL(ObRawExprSetUtils::add_expr_set(allocator, raw_eset, output_equal_sets))) {
        LOG_WARN("failed to push back equal set", K(ret));
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObOptimizerUtil::convert_subplan_scan_fd_item_sets(ObFdItemFactory &fd_factory,
                                                       ObRawExprFactory &expr_factory,
                                                       const EqualSets &equal_sets,
                                                       const ObIArray<ObRawExpr*> &const_exprs,
                                                       const uint64_t table_id,
                                                       const ObDMLStmt &parent_stmt,
                                                       const ObSelectStmt &child_stmt,
                                                       const ObFdItemSet &input_fd_item_sets,
                                                       ObFdItemSet &output_fd_item_sets)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 8> new_parent_exprs;
  ObRelIds tables;
  TableItem* table_item = NULL;
  if (OB_FAIL(tables.add_member(parent_stmt.get_table_bit_index(table_id)))) {
    LOG_WARN("failed to add relid", K(ret), K(parent_stmt), K(table_id));
  } else if (OB_ISNULL(table_item = parent_stmt.get_table_item_by_id(table_id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(table_item));
  } else if (table_item->is_lateral_table()) {
    // do nothing
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < input_fd_item_sets.count(); ++i) {
      const ObFdItem *old_fd_item = NULL;
      bool contain_exec_param = false;
      new_parent_exprs.reuse();
      if (OB_ISNULL(old_fd_item = input_fd_item_sets.at(i)) ||
          OB_ISNULL(old_fd_item->get_parent_exprs())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(old_fd_item));
      } else if (OB_FAIL(convert_subplan_scan_fd_parent_exprs(expr_factory,
                                                              equal_sets,
                                                              const_exprs,
                                                              table_id,
                                                              parent_stmt,
                                                              child_stmt,
                                                              *old_fd_item->get_parent_exprs(),
                                                              new_parent_exprs))) {
        LOG_WARN("failed to convert subplan scan expr", K(ret));
      } else if (new_parent_exprs.empty()) {
        /*do nothing*/
      } else if (old_fd_item->is_unique()) {
        ObTableFdItem *table_fd_item = NULL;
        if (OB_FAIL(fd_factory.create_table_fd_item(table_fd_item, true, new_parent_exprs, tables))) {
          LOG_WARN("failed to create fd item", K(ret));
        } else if (OB_FAIL(output_fd_item_sets.push_back(table_fd_item))) {
          LOG_WARN("failed to push back fd item", K(ret));
        }
      } else {
        ObRawExpr *temp_expr = NULL;
        ObSEArray<ObRawExpr *, 8> child_select_exprs;
        ObSEArray<ObRawExpr *, 8> new_child_exprs;
        ObExprFdItem *expr_fd_item = NULL;
        if (OB_FAIL(child_stmt.get_select_exprs(child_select_exprs))) {
          LOG_WARN("failed to get select exprs", K(ret));
        } else {
          for (int64_t j = 0; OB_SUCC(ret) && j < child_select_exprs.count(); ++j) {
            bool is_in_child = false;
            if (OB_FAIL(old_fd_item->check_expr_in_child(child_select_exprs.at(j),
                                                        equal_sets,
                                                        is_in_child))) {
              LOG_WARN("failed to check expr in child");
            } else if (!is_in_child) {
              /*do nothing*/
            } else if (NULL == (temp_expr = parent_stmt.get_column_expr_by_id(
                                            table_id,
                                            OB_APP_MIN_COLUMN_ID + j))) {
              /*do nothing*/
            } else if (OB_FAIL(new_child_exprs.push_back(temp_expr))) {
              LOG_WARN("failed to push back exprs", K(ret));
            } else { /*do nothing*/ }
          }
          if (OB_SUCC(ret) && !new_child_exprs.empty()) {
            if (OB_FAIL(fd_factory.create_expr_fd_item(expr_fd_item, false,
                                                      new_parent_exprs,
                                                      new_child_exprs))) {
              LOG_WARN("failed to create fd item", K(ret));
            } else if (OB_FAIL(output_fd_item_sets.push_back(expr_fd_item))) {
              LOG_WARN("failed to push back fd item", K(ret));
            } else { /*do nothing*/}
          }
        }
      }
    }
  }

  return ret;
}

int ObOptimizerUtil::convert_subplan_scan_fd_parent_exprs(ObRawExprFactory &expr_factory,
                                                          const EqualSets &equal_sets,
                                                          const ObIArray<ObRawExpr*> &const_exprs,
                                                          const uint64_t table_id,
                                                          const ObDMLStmt &parent_stmt,
                                                          const ObSelectStmt &child_stmt,
                                                          const ObIArray<ObRawExpr*> &input_exprs,
                                                          ObIArray<ObRawExpr*> &output_exprs)
{
  int ret = OB_SUCCESS;
  bool is_valid = true;
  output_exprs.reuse();
  ObRawExprCopier copier(expr_factory);
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < input_exprs.count(); i++) {
    ObRawExpr *expr = NULL;
    if (OB_ISNULL(input_exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null expr", K(input_exprs.at(i)), K(ret));
    } else if (OB_FAIL(convert_subplan_scan_expr(copier,
                                                 equal_sets,
                                                 table_id,
                                                 parent_stmt,
                                                 child_stmt,
                                                 input_exprs.at(i),
                                                 expr))) {
      LOG_WARN("failed to generate subplan scan expr", K(ret));
    } else if (OB_ISNULL(expr)) {
      if (OB_FAIL(is_const_expr(input_exprs.at(i), equal_sets, const_exprs, is_valid))) {
        LOG_WARN("failed to check is const expr", K(ret));
      } else if (!is_valid) {
        output_exprs.reuse();
      } 
    } else if (OB_FAIL(add_var_to_array_no_dup(output_exprs, expr))) {
      LOG_WARN("failed to push back expr", K(ret));
    } else { /*do nothing*/ }
  }
  if (OB_SUCC(ret)) {
    LOG_TRACE("succeed to convert subplan scan expr skip const", K(input_exprs), K(output_exprs));
  }
  return ret;
}

/**
 * 1. 根据连接条件将 candi fd item 提升为 fd item
 * 2. 确定连接是否为 n to 1 连接
 * 3. 根据连接性质确定连接结果的 fd item
 * join_info == NULL means Cartesian join
 */
int ObOptimizerUtil::add_fd_item_set_for_left_join(ObFdItemFactory &fd_factory,
                                                   const ObRelIds &right_tables,
                                                   const ObIArray<ObRawExpr*> &right_join_exprs,
                                                   const ObIArray<ObRawExpr*> &right_const_exprs,
                                                   const EqualSets &right_equal_sets,
                                                   const ObIArray<ObFdItem *> &right_fd_item_sets,
                                                   const ObIArray<ObRawExpr*> &all_left_join_exprs,
                                                   const EqualSets &left_equal_sets,
                                                   const ObIArray<ObFdItem *> &left_fd_item_sets,
                                                   const ObIArray<ObFdItem *> &left_candi_fd_item_sets,
                                                   ObIArray<ObFdItem *> &fd_item_sets,
                                                   ObIArray<ObFdItem *> &candi_fd_item_sets)
{
  int ret = OB_SUCCESS;
  bool right_is_unique = false;
  if (OB_FAIL(is_exprs_unique(right_join_exprs, right_tables, right_fd_item_sets,
                              right_equal_sets, right_const_exprs, right_is_unique))) {
    LOG_WARN("failed to check is order unique", K(ret));
  } else if (right_is_unique) {
    if (OB_FAIL(add_fd_item_set_for_n21_join(fd_factory, fd_item_sets, left_fd_item_sets,
                                             all_left_join_exprs, left_equal_sets, right_tables))) {
      LOG_WARN("failed to add fd item set for n21 join", K(ret));
    } else if (OB_FAIL(add_fd_item_set_for_n21_join(fd_factory, candi_fd_item_sets,
                                                    left_candi_fd_item_sets, all_left_join_exprs,
                                                    left_equal_sets, right_tables))) {
      LOG_WARN("failed to add fd item set for n21 join", K(ret));
    }
  } else if (OB_FAIL(add_fd_item_set_for_n2n_join(fd_factory, fd_item_sets,
                                                  left_fd_item_sets))) {
    LOG_WARN("failed to add fd item set for n2n join", K(ret));
  } else if (OB_FAIL(add_fd_item_set_for_n2n_join(fd_factory, candi_fd_item_sets,
                                                  left_candi_fd_item_sets))) {
    LOG_WARN("failed to add fd item set for n2n join", K(ret));
  }
  return ret;
}

int ObOptimizerUtil::check_need_sort(const ObIArray<OrderItem> &expected_order_items,
                                     const ObIArray<OrderItem> &input_ordering,
                                     const ObFdItemSet &fd_item_set,
                                     const EqualSets &equal_sets,
                                     const ObIArray<ObRawExpr *> &const_exprs,
                                     const ObIArray<ObRawExpr *> &exec_ref_exprs,
                                     const bool is_at_most_one_row,
                                     bool &need_sort,
                                     int64_t &prefix_pos)
{
  int ret = OB_SUCCESS;
  const int64_t part_cnt = 0;
  return ObOptimizerUtil::check_need_sort(expected_order_items,
                                          input_ordering,
                                          fd_item_set,
                                          equal_sets,
                                          const_exprs,
                                          exec_ref_exprs,
                                          is_at_most_one_row,
                                          need_sort,
                                          prefix_pos,
                                          part_cnt);
}

int ObOptimizerUtil::check_need_sort(const ObIArray<OrderItem> &expected_order_items,
                                     const ObIArray<OrderItem> &input_ordering,
                                     const ObFdItemSet &fd_item_set,
                                     const EqualSets &equal_sets,
                                     const ObIArray<ObRawExpr *> &const_exprs,
                                     const ObIArray<ObRawExpr *> &exec_ref_exprs,
                                     const bool is_at_most_one_row,
                                     bool &need_sort,
                                     int64_t &prefix_pos,
                                     const int64_t part_cnt,
                                     const bool check_part_only/* default false */)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 6> expected_order_exprs;
  ObSEArray<ObOrderDirection, 6> expected_order_directions;
  if (OB_FAIL(split_expr_direction(expected_order_items,
                                   expected_order_exprs,
                                   expected_order_directions))) {
    LOG_WARN("failed to split expr and expected_order_directions", K(ret));
  } else if (OB_FAIL(check_need_sort(expected_order_exprs,
                                     &expected_order_directions,
                                     input_ordering,
                                     fd_item_set,
                                     equal_sets,
                                     const_exprs,
                                     exec_ref_exprs,
                                     is_at_most_one_row,
                                     need_sort,
                                     prefix_pos,
                                     part_cnt,
                                     check_part_only))) {
    LOG_WARN("failed to check need sort", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObOptimizerUtil::check_need_sort(const ObIArray<ObRawExpr*> &expected_order_exprs,
                                     const ObIArray<ObOrderDirection> *expected_order_directions,
                                     const ObIArray<OrderItem> &input_ordering,
                                     const ObFdItemSet &fd_item_set,
                                     const EqualSets &equal_sets,
                                     const ObIArray<ObRawExpr *> &const_exprs,
                                     const ObIArray<ObRawExpr *> &exec_ref_exprs,
                                     const bool is_at_most_one_row,
                                     bool &need_sort,
                                     int64_t &prefix_pos,
                                     const int64_t part_cnt,
                                     const bool check_part_only/* default false */)
{
  int ret = OB_SUCCESS;
  need_sort = true;
  if (!check_part_only &&
      OB_FAIL(ObOptimizerUtil::check_need_sort(expected_order_exprs,
                                               expected_order_directions,
                                               input_ordering,
                                               fd_item_set,
                                               equal_sets,
                                               const_exprs,
                                               exec_ref_exprs,
                                               is_at_most_one_row,
                                               need_sort,
                                               prefix_pos))) {
    LOG_WARN("failed to check need sort", K(ret));
  } else if (need_sort && part_cnt > 0) {
    // partition sort does not support prefix_pos, so if part_cnt is greater than 0, prefix_pos is always 0
    prefix_pos = 0;
    if (input_ordering.count() > 1 &&
        input_ordering.at(0).expr_->get_expr_type() == T_FUN_SYS_HASH &&
        part_cnt == input_ordering.at(0).expr_->get_children_count()) {
      int64_t tmp_prefix_pos = 0;
      common::ObSEArray<OrderItem, 1> ordering;
      for (int64_t i = 1; OB_SUCC(ret) && i < input_ordering.count(); ++i) {
        if (OB_FAIL(ordering.push_back(input_ordering.at(i)))) {
          LOG_WARN("failed to add order item", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ObOptimizerUtil::check_need_sort(expected_order_exprs,
                                                          expected_order_directions,
                                                          ordering,
                                                          fd_item_set,
                                                          equal_sets,
                                                          const_exprs,
                                                          exec_ref_exprs,
                                                          is_at_most_one_row,
                                                          need_sort,
                                                          tmp_prefix_pos))) {
        LOG_WARN("failed to check need sort", K(ret));
      }
    }
  }
  return ret;
}

/**
 * 假定下层序是 input_ordering，检查是否需要为 expected_order_exprs 分配 sort
 * （排序方向由 expected_order_directions 指定）
 * 当 input_ordering 能够决定 expected_order_exprs、expected_order_directions 序时不需要再分配 sort:
 * 1. expected_order_exprs 是 input_ordering 的前缀
 *  例如：expected_order_exprs: a,b
 *        input_ordering: a,b,c
 * 2. expected_order_exprs 与 input_ordering 公共前缀能决定其他expr
 *  例如：expected_order_exprs: a,b,e
 *        input_ordering: a,b,c
 *        其中 a,b function dependence c
 *  这里有一种特殊情况
 *       expected_order_exprs: e,f,g
 *       input_ordering: a,b,c
 *       两者没有公共前缀（i.e. right_match_count = 0）
 *       但假如有一组常量表达式是唯一的。那也不需要排序。
 *       常量+唯一意味着只有一行
 * 3. 使用函数依赖关系后能够找到公共前缀满足1、2
 * 例如：expected_order_exprs: a,f,b
 *        input_ordering: a,b,c
 *        其中 a,b function dependence c, a function dependence f
 **/
int ObOptimizerUtil::check_need_sort(const ObIArray<ObRawExpr*> &expected_order_exprs,
                                     const ObIArray<ObOrderDirection> *expected_order_directions,
                                     const ObIArray<OrderItem> &input_ordering,
                                     const ObFdItemSet &fd_item_set,
                                     const EqualSets &equal_sets,
                                     const ObIArray<ObRawExpr *> &const_exprs,
                                     const ObIArray<ObRawExpr *> &exec_ref_exprs,
                                     const bool is_at_most_one_row,
                                     bool &need_sort,
                                     int64_t &prefix_pos)
{
  int ret = OB_SUCCESS;

  int64_t left_count = input_ordering.count();
  int64_t right_count = expected_order_exprs.count();
  int64_t l_idx = 0;
  int64_t r_idx = 0;
  ObSEArray<ObRawExpr *, 8> extend_exprs;
  ObSEArray<ObRawExpr *, 8> fd_set_parent_exprs;
  bool find_unique = false;
  bool is_match = true;
  bool left_is_const = false;
  bool right_is_const = false;
  need_sort = true;
  prefix_pos = 0;
  if (is_at_most_one_row) {
    need_sort = false;
  } else {
    ObBitSet<> left_set;
    ObBitSet<> right_set;
    ObBitSet<> used_fd;
    if (OB_FAIL(get_fd_set_parent_exprs(fd_item_set, fd_set_parent_exprs))) {
      LOG_WARN("failed to get fd set parent exprs ", K(ret));
    }
    //r_idx < right_count、l_idx == left_count 时仍可对 expected_order_exprs 进行检查
    while (OB_SUCC(ret) && is_match && !find_unique && r_idx < right_count) {
      // if expected_order_expr = cast(col as dst_type), input_order_expr = col
      // and cast is lossless, no need to add sort for `cast(col as dst_type)`
      ObRawExpr *expected_order_expr = expected_order_exprs.at(r_idx);
      bool is_lossless_cast = false;
      if (OB_ISNULL(expected_order_expr)) {
        // do nothing
      } else if (OB_FAIL(is_lossless_column_cast(expected_order_expr, is_lossless_cast))) {
        LOG_WARN("check lossless column cast failed", K(ret));
      } else if (is_lossless_cast) {
        expected_order_expr = expected_order_expr->get_param_expr(0);
      }

      if (OB_SUCC(ret) && (l_idx < left_count)) {
        is_lossless_cast = false;
        if (OB_ISNULL(input_ordering.at(l_idx).expr_)) {
          // do nothing
        } else if (OB_FAIL(is_lossless_column_cast(input_ordering.at(l_idx).expr_, is_lossless_cast))) {
          LOG_WARN("check lossless column cast failed", K(ret), KPC(input_ordering.at(l_idx).expr_));
        }
      }
      if (OB_FAIL(ret)) {
        // do nothing
      } else if (l_idx < left_count
                 && (NULL == expected_order_directions
                     || input_ordering.at(l_idx).order_type_ == expected_order_directions->at(r_idx))
                 && is_expr_equivalent(
                     expected_order_expr,
                     is_lossless_cast ?
                         input_ordering.at(l_idx).expr_->get_param_expr(0) :
                         input_ordering.at(l_idx).expr_,
                     equal_sets)) {
        ret = extend_exprs.push_back(expected_order_exprs.at(r_idx));
        ++l_idx;
        ++r_idx;
      } else if (l_idx < left_count && OB_FAIL(is_const_or_equivalent_expr(input_ordering, equal_sets,
                                                                           const_exprs,
                                                                           exec_ref_exprs, l_idx,
                                                                           left_is_const))) {
        LOG_WARN("failed to check is const or equivalent exprs", K(ret));
      } else if (OB_FAIL(is_const_or_equivalent_expr(expected_order_exprs, equal_sets, const_exprs,
                                                     exec_ref_exprs, r_idx, right_is_const))) {
        LOG_WARN("failed to check is const or equivalent exprs", K(ret));
      } else if (right_is_const || (l_idx < left_count && left_is_const)) {
        if (l_idx < left_count && left_is_const) {
          ++l_idx;
        }
        if (right_is_const) {
          ++r_idx;
        }
      } else {
        ObFdItem *fd_item = NULL;
        int64_t extend_exprs_count = -1;
        while (OB_SUCC(ret) && !find_unique && extend_exprs.count() > extend_exprs_count) {
          extend_exprs_count = extend_exprs.count();
          for (int64_t fd_idx = 0; OB_SUCC(ret) && !find_unique
                                  && fd_idx < fd_item_set.count(); ++fd_idx) {
            bool is_contain = false;
            if (OB_ISNULL(fd_item = fd_item_set.at(fd_idx))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("get null fd item", K(ret));
            } else if (used_fd.has_member(fd_idx)) {
              // do nothing
            } else if (OB_FAIL(is_exprs_contain_fd_parent(extend_exprs, *fd_item,
                                                          equal_sets, const_exprs, is_contain))) {
              LOG_WARN("faield to check is order unique", K(ret));
            } else if (!is_contain) {
              // do nothing
            } else if (fd_item->is_unique()) {
              find_unique = true;
            } else if (OB_FAIL(used_fd.add_member(fd_idx))) {
              LOG_WARN("failed to add member to set", K(ret));
            } else if (OB_FAIL(split_child_exprs(fd_item, equal_sets, fd_set_parent_exprs,
                                                 extend_exprs))) {
              LOG_WARN("failed to delete members", K(ret));
            } else if (OB_FAIL(fd_item->check_exprs_in_child(expected_order_exprs, equal_sets,
                                                             r_idx, right_set))) {
              LOG_WARN("failed to check exprs in child", K(ret));
            } else if (OB_FAIL(fd_item->check_exprs_in_child(input_ordering, equal_sets,
                                                             l_idx, left_set))) {
              LOG_WARN("failed to check exprs in child", K(ret));
            }
          }
        }
        if (!find_unique && !left_set.has_member(l_idx) && !right_set.has_member(r_idx)) {
          is_match = false;
        }
      }
      while(left_set.has_member(l_idx)) {
        ++l_idx;
      };
      while(right_set.has_member(r_idx)) {
        ++r_idx;
      };
    }

    if (OB_SUCC(ret)) {
      if (!is_match) {
        need_sort = true;
        prefix_pos = r_idx;
      } else if (find_unique || expected_order_exprs.count() == r_idx) {
        need_sort = false;
      }
    }
  }
  LOG_TRACE("succeed to check need sort", K(need_sort), K(fd_item_set), K(equal_sets),
      K(const_exprs), K(is_at_most_one_row), K(expected_order_exprs),
      K(input_ordering), K(l_idx), K(r_idx), K(find_unique));
  return ret;
}

// try use input ordering
//  a. if use all input ordering, need not sort
//  b. if use no input ordering, create merge key use interesting ordering, need sort
//  c. if use some input ordering, need check need sort
int ObOptimizerUtil::decide_sort_keys_for_merge_style_op(const ObDMLStmt *stmt,
                                                         const EqualSets &stmt_equal_sets,
                                                         const ObIArray<OrderItem> &input_ordering,
                                                         const ObFdItemSet &fd_item_set,
                                                         const EqualSets &equal_sets,
                                                         const ObIArray<ObRawExpr*> &const_exprs,
                                                         const ObIArray<ObRawExpr*> &exec_ref_exprs,
                                                         const bool is_at_most_one_row,
                                                         const ObIArray<ObRawExpr*> &merge_exprs,
                                                         const ObIArray<ObOrderDirection> &default_directions,
                                                         MergeKeyInfo &merge_key,
                                                         MergeKeyInfo *&interesting_key)
{
  int ret = OB_SUCCESS;
  int64_t prefix_count = -1;
  bool input_ordering_all_used = false;
  ObSEArray<OrderItem, 8> order_items;
  ObSEArray<OrderItem, 8> final_items;
  if (OB_FAIL(merge_key.order_exprs_.assign(merge_exprs))) {
    LOG_WARN("failed to assign exprs", K(ret));
  } else if (OB_FAIL(merge_key.order_directions_.assign(default_directions))) {
    LOG_WARN("failed to assign exprs", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::adjust_exprs_by_ordering(merge_key.order_exprs_,
                                                               input_ordering,
                                                               equal_sets,
                                                               const_exprs,
                                                               exec_ref_exprs,
                                                               prefix_count,
                                                               input_ordering_all_used,
                                                               merge_key.order_directions_,
                                                               &merge_key.map_array_))) {
    LOG_WARN("failed to adjust expr by ordering", K(ret));
  } else if (prefix_count > 0 || input_ordering_all_used || NULL == interesting_key) {
    if (!input_ordering_all_used && prefix_count <= 0 &&
        OB_FAIL(create_interesting_merge_key(stmt, merge_exprs, stmt_equal_sets, merge_key))) {
      LOG_WARN("failed to create interesting key", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::make_sort_keys(merge_key.order_exprs_,
                                                       merge_key.order_directions_,
                                                       order_items))) {
      LOG_WARN("failed to make sort keys", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::simplify_ordered_exprs(fd_item_set,
                                                               equal_sets,
                                                               const_exprs,
                                                               exec_ref_exprs,
                                                               order_items,
                                                               final_items))) {
      LOG_WARN("failed to simply ordered exprs", K(ret));
    } else if (OB_FAIL(merge_key.order_items_.assign(final_items))) {
      LOG_WARN("failed to assign final items", K(ret));
    } else if (input_ordering_all_used) {
      merge_key.need_sort_ = false;
    } else if (OB_FAIL(ObOptimizerUtil::check_need_sort(merge_key.order_items_,
                                                        input_ordering,
                                                        fd_item_set,
                                                        equal_sets,
                                                        const_exprs,
                                                        exec_ref_exprs,
                                                        is_at_most_one_row,
                                                        merge_key.need_sort_,
                                                        merge_key.prefix_pos_))) {
      LOG_WARN("failed to check need sort", K(ret));
    } else if (prefix_count > 0) {
      /*do nothing*/
    } else {
      interesting_key = &merge_key;
    }
  } else if (OB_FAIL(merge_key.assign(*interesting_key))) {
    LOG_WARN("failed to assign merge key", K(ret));
    /* interesting_key->need_sort_ is true generally.
      When ObOptimizerUtil::check_need_sort use ordering contain lossless cast, need_sort_ can be false
      and ObOptimizerUtil::check_need_sort is needed for other path use the interesting_key */
  } else if (OB_FAIL(ObOptimizerUtil::check_need_sort(merge_key.order_items_,
                                                      input_ordering,
                                                      fd_item_set,
                                                      equal_sets,
                                                      const_exprs,
                                                      exec_ref_exprs,
                                                      is_at_most_one_row,
                                                      merge_key.need_sort_,
                                                      merge_key.prefix_pos_))) {
    LOG_WARN("failed to check need sort", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObOptimizerUtil::create_interesting_merge_key(const ObDMLStmt *stmt,
                                                  const ObIArray<ObRawExpr*> &merge_exprs,
                                                  const EqualSets &equal_sets,
                                                  MergeKeyInfo &merge_key)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 2> sort_exprs;
  ObSEArray<ObOrderDirection, 2> directions;
  ObSEArray<int64_t, 2> sort_map;
  bool is_find = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(stmt));
  } else if (stmt->is_select_stmt()) {
    // find direction in window function exprs
    const ObSelectStmt *select_stmt = static_cast<const ObSelectStmt*>(stmt);
    for (int64_t i = 0; OB_SUCC(ret) && sort_exprs.empty() &&
                                        i < select_stmt->get_window_func_count(); ++i) {
      const ObWinFunRawExpr *win_expr = NULL;
      if (OB_ISNULL(win_expr = select_stmt->get_window_func_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("window function is null", K(ret));
      } else if (win_expr->get_order_items().empty()) {
        /* do nothing */
      } else if (OB_FAIL(create_interesting_merge_key(merge_exprs, win_expr->get_order_items(),
                                                      equal_sets, sort_exprs,
                                                      directions, sort_map))) {
        LOG_WARN("failed to create interesting key", K(ret));
      }
    }
    LOG_TRACE("succeed to create merge key use order by items in window function exprs",
                                                                K(sort_exprs), K(directions));
  }

  if (OB_SUCC(ret) && sort_exprs.empty() && !stmt->get_order_items().empty()) {
    // find direction in order by exprs
    if (OB_FAIL(create_interesting_merge_key(merge_exprs, stmt->get_order_items(),
                                             equal_sets, sort_exprs, directions, sort_map))) {
      LOG_WARN("failed to create interesting key", K(ret));
    } else {
      LOG_TRACE("succeed to create merge key use order by items", K(sort_exprs), K(directions));
    }
  }

  if (OB_SUCC(ret) && sort_exprs.empty()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < merge_exprs.count(); ++i) {
      if (OB_FAIL(sort_exprs.push_back(merge_exprs.at(i)))) {
        LOG_WARN("failed to push back", K(ret));
      } else if (OB_FAIL(directions.push_back(default_asc_direction()))) {
        LOG_WARN("failed to push back", K(ret));
      } else if (OB_FAIL(sort_map.push_back(i))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(merge_key.order_exprs_.assign(sort_exprs))) {
    LOG_WARN("failed to assign exprs", K(ret));
  } else if (OB_FAIL(merge_key.order_directions_.assign(directions))) {
    LOG_WARN("failed to assign exprs", K(ret));
  } else if (OB_FAIL(merge_key.map_array_.assign(sort_map))) {
    LOG_WARN("failed to assign exprs", K(ret));
  }
  return ret;
}

int ObOptimizerUtil::create_interesting_merge_key(const ObIArray<ObRawExpr*> &merge_exprs,
                                                  const ObIArray<OrderItem> &expect_key,
                                                  const EqualSets &equal_sets,
                                                  ObIArray<ObRawExpr*> &sort_exprs,
                                                  ObIArray<ObOrderDirection> &directions,
                                                  ObIArray<int64_t> &sort_map)
{
  int ret = OB_SUCCESS;
  int64_t index = OB_INVALID_INDEX;
  if (OB_UNLIKELY(expect_key.empty() || merge_exprs.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected input", K(ret), K(expect_key), K(merge_exprs));
  } else if (!ObOptimizerUtil::find_equal_expr(merge_exprs, expect_key.at(0).expr_,
                                               equal_sets, index)) {
    /* do nothing */
  } else if (OB_UNLIKELY(index < 0 || index >= merge_exprs.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected index", K(ret), K(index), K(merge_exprs.count()));
  } else if (OB_FAIL(sort_exprs.push_back(merge_exprs.at(index)))) {
    LOG_WARN("failed to push back", K(ret));
  } else if (OB_FAIL(directions.push_back(expect_key.at(0).order_type_))) {
    LOG_WARN("failed to push back", K(ret));
  } else if (OB_FAIL(sort_map.push_back(index))) {
    LOG_WARN("failed to push back", K(ret));
  } else {
    ObSqlBitSet<> found_bs;
    bool found_key = true;
    bool found_join_expr = false;
    if (OB_FAIL(found_bs.add_member(index))) {
      LOG_WARN("failed to add member", K(ret));
    }
    for (int64_t i = 1; OB_SUCC(ret) && found_key && i < expect_key.count(); ++i) {
      found_join_expr = false;
      for (int64_t j = 0; OB_SUCC(ret) && !found_join_expr && j < merge_exprs.count(); ++j) {
        if (found_bs.has_member(j)) {
          /* do nothing */
        } else if (!ObOptimizerUtil::is_expr_equivalent(expect_key.at(i).expr_, merge_exprs.at(j),
                                                        equal_sets)) {
          /* do nothing */
        } else if (OB_FAIL(found_bs.add_member(j))) {
          LOG_WARN("failed to add member", K(ret));
        } else if (OB_FAIL(sort_exprs.push_back(merge_exprs.at(j)))) {
          LOG_WARN("failed to push back", K(ret));
        } else if (OB_FAIL(directions.push_back(expect_key.at(i).order_type_))) {
          LOG_WARN("failed to push back", K(ret));
        } else if (OB_FAIL(sort_map.push_back(j))) {
          LOG_WARN("failed to push back", K(ret));
        } else {
          found_join_expr = true;
        }
      }
      if (OB_SUCC(ret) && !found_join_expr) {
        found_key = false;
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < merge_exprs.count(); ++i) {
      if (found_bs.has_member(i)) {
        /* do nothing */
      } else if (OB_FAIL(sort_exprs.push_back(merge_exprs.at(i)))) {
        LOG_WARN("failed to push back", K(ret));
      } else if (OB_FAIL(directions.push_back(default_asc_direction()))) {
        LOG_WARN("failed to push back", K(ret));
      } else if (OB_FAIL(sort_map.push_back(i))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
  }
  return ret;
}

int ObOptimizerUtil::flip_op_type(const ObItemType expr_type, ObItemType &rotated_expr_type)
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

int ObOptimizerUtil::get_rownum_filter_info(ObRawExpr *rownum_expr,
                                            ObItemType &expr_type,
                                            ObRawExpr *&const_expr,
                                            bool &is_const_filter)
{
  int ret = OB_SUCCESS;
  is_const_filter = false;
  ObRawExpr *first_param = NULL;
  ObRawExpr *second_param = NULL;
  if (OB_ISNULL(rownum_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (rownum_expr->is_op_expr() && 2 == rownum_expr->get_param_count() &&
             (IS_RANGE_CMP_OP(rownum_expr->get_expr_type())
              || T_OP_EQ == rownum_expr->get_expr_type())) {
    first_param = rownum_expr->get_param_expr(0);
    second_param = rownum_expr->get_param_expr(1);
    if (OB_ISNULL(first_param) || OB_ISNULL(second_param)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null expr", K(ret), K(first_param), K(second_param));
    } else if (first_param->has_flag(IS_ROWNUM) &&
               second_param->is_static_const_expr() &&
               (second_param->get_result_type().is_int() ||
                second_param->get_result_type().is_number())) {
      expr_type = rownum_expr->get_expr_type();
      const_expr = second_param;
      is_const_filter = true;
    } else if (first_param->is_static_const_expr() &&
               second_param->has_flag(IS_ROWNUM) &&
               (first_param->get_result_type().is_int() ||
                first_param->get_result_type().is_number())) {
      if (OB_FAIL(flip_op_type(rownum_expr->get_expr_type(), expr_type))) {
        LOG_WARN("failed to rotate rownum_expr type", K(ret));
      } else {
        const_expr = first_param;
        is_const_filter = true;
      }
    }
  }
  return ret;
}

int ObOptimizerUtil::convert_rownum_filter_as_offset(ObRawExprFactory &expr_factory,
                                                     ObSQLSessionInfo *session_info,
                                                     const ObItemType filter_type,
                                                     ObRawExpr *const_expr,
                                                     ObRawExpr *&offset_int_expr,
                                                     ObRawExpr *zero_expr,
                                                     bool &offset_is_not_neg,
                                                     ObTransformerCtx *ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(const_expr) || OB_ISNULL(session_info) || OB_ISNULL(ctx) || OB_ISNULL(zero_expr)
      || OB_UNLIKELY(filter_type != T_OP_GE && filter_type != T_OP_GT)
      || OB_UNLIKELY(!const_expr->get_result_type().is_integer_type()
                     && !const_expr->get_result_type().is_number())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(const_expr), K(session_info), K(filter_type));
  } else if (T_OP_GT == filter_type) {
    if (OB_FAIL(ObTransformUtils::compare_const_expr_result(ctx, const_expr, T_OP_GE,
                                                                    0, offset_is_not_neg))) {
      LOG_WARN("offset value is negative calc failed", K(ret));
    } else if (!offset_is_not_neg) {
      offset_int_expr = zero_expr;
    } else if (OB_FAIL(floor_number_as_limit_offset_value(expr_factory, session_info,
                                                   const_expr, offset_int_expr))) {
      LOG_WARN("failed to floor number as offset value", K(ret));
    }
  } else if (T_OP_GE == filter_type) {
    if (OB_FAIL(ObTransformUtils::compare_const_expr_result(ctx, const_expr, T_OP_GE,
                                                                    0, offset_is_not_neg))) {
      LOG_WARN("offset value is negative calc failed", K(ret));
    } else if (!offset_is_not_neg) {
      offset_int_expr = zero_expr;
    } else if (OB_FAIL(ceil_number_as_limit_offset_value(expr_factory, session_info,
                                                  const_expr, offset_int_expr))) {
      LOG_WARN("failed to ceil number as offset value", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(offset_int_expr->formalize(session_info))) {
      LOG_WARN("failed to formalize offset int expr", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    // cast to int value in static typing engine.
    ObExprResType dst_type;
    dst_type.set_int();
    ObSysFunRawExpr *cast_expr = NULL;
    OZ(ObRawExprUtils::create_cast_expr(
            expr_factory, offset_int_expr, dst_type, cast_expr, session_info));
    CK(NULL != cast_expr);
    if (OB_SUCC(ret)) {
      offset_int_expr = cast_expr;
    }
  }
  return ret;
}

int ObOptimizerUtil::convert_rownum_filter_as_limit(ObRawExprFactory &expr_factory,
                                                    ObSQLSessionInfo *session_info,
                                                    const ObItemType filter_type,
                                                    ObRawExpr *const_expr,
                                                    ObRawExpr *&limit_int_expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(const_expr) || OB_ISNULL(session_info)
      || OB_UNLIKELY(filter_type != T_OP_LE && filter_type != T_OP_LT)
      || OB_UNLIKELY(!const_expr->get_result_type().is_integer_type()
                     && !const_expr->get_result_type().is_number()
                     && !const_expr->get_result_type().is_null())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rownum expr is null", K(ret), K(const_expr), K(session_info), K(filter_type));
  } else if (filter_type == T_OP_LE) {
    if (OB_FAIL(floor_number_as_limit_offset_value(expr_factory, session_info,
                                                   const_expr, limit_int_expr))) {
      LOG_WARN("failed to floor number as limit value", K(ret));
    }
  } else if (filter_type == T_OP_LT) {
    if (OB_FAIL(ceil_number_as_limit_offset_value(expr_factory, session_info,
                                                  const_expr, limit_int_expr))) {
      LOG_WARN("failed to ceil number as limit value", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(limit_int_expr->formalize(session_info))) {
      LOG_WARN("failed to formalize limit int expr", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    // cast to int value in static typing engine.
    ObExprResType dst_type;
    dst_type.set_int();
    ObSysFunRawExpr *cast_expr = NULL;
    OZ(ObRawExprUtils::create_cast_expr(
            expr_factory, limit_int_expr, dst_type, cast_expr, session_info));
    CK(NULL != cast_expr);
    if (OB_SUCC(ret)) {
      limit_int_expr = cast_expr;
    }
  }
  return ret;
}

int ObOptimizerUtil::floor_number_as_limit_offset_value(ObRawExprFactory &expr_factory,
                                                        ObSQLSessionInfo *session_info,
                                                        ObRawExpr *num_expr,
                                                        ObRawExpr *&limit_int) {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session_info) || OB_ISNULL(num_expr)
      || OB_UNLIKELY(!num_expr->get_result_type().is_integer_type()
                     && !num_expr->get_result_type().is_number()
                     && !num_expr->get_result_type().is_null())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params are invalid", K(ret), K(session_info), K(num_expr));
  } else if (num_expr->get_result_type().is_integer_type()) {
    limit_int = num_expr;
  } else {
    ObSysFunRawExpr *floor_expr = NULL;
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

int ObOptimizerUtil::ceil_number_as_limit_offset_value(ObRawExprFactory &expr_factory,
                                                       ObSQLSessionInfo *session_info,
                                                       ObRawExpr *num_expr,
                                                       ObRawExpr *&limit_int)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session_info) || OB_ISNULL(num_expr)
      || OB_UNLIKELY(!num_expr->get_result_type().is_integer_type()
                     && !num_expr->get_result_type().is_number()
                     && !num_expr->get_result_type().is_null())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params are invalid", K(ret), K(session_info), K(num_expr));
  } else if (num_expr->get_result_type().is_number()) {
    ObSysFunRawExpr *ceil_expr = NULL;
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
    ObRawExpr *minus_expr = NULL;
    ObConstRawExpr *const_one = NULL;
    if (OB_FAIL(ObRawExprUtils::build_const_int_expr(
                  expr_factory, ObIntType, 1L, const_one))) {
      LOG_WARN("failed to build int expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::create_double_op_expr(
                         expr_factory, session_info, T_OP_MINUS,
                         minus_expr, num_expr, const_one))) {
      LOG_WARN("failed to decrease const expr value", K(ret));
    } else {
      limit_int = minus_expr;
    }
  }
  return ret;
}

int ObOptimizerUtil::get_type_safe_join_exprs(const ObIArray<ObRawExpr *> &join_quals,
                                              const ObRelIds &left_tables,
                                              const ObRelIds &right_tables,
                                              ObIArray<ObRawExpr *> &left_exprs,
                                              ObIArray<ObRawExpr *> &right_exprs,
                                              ObIArray<ObRawExpr *> &all_left_exprs,
                                              ObIArray<ObRawExpr *> &all_right_exprs)
{
  int ret = OB_SUCCESS;
  ObRawExpr *cur_expr = NULL;
  ObRawExpr *first_expr = NULL;
  ObRawExpr *second_expr = NULL;
  ObRawExpr *left_expr = NULL;
  ObRawExpr *right_expr = NULL;
  for (int64_t i = 0 ; OB_SUCC(ret) && i < join_quals.count(); ++i) {
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
    } else if (T_OP_ROW == first_expr->get_expr_type() ||
               T_OP_ROW == second_expr->get_expr_type()) {
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

int ObOptimizerUtil::split_or_qual_on_table(const ObDMLStmt *stmt,
                                            ObRawExprFactory &expr_factory,
                                            const ObSQLSessionInfo *session_info,
                                            const ObRelIds &table_ids,
                                            ObOpRawExpr &or_qual,
                                            ObOpRawExpr *&new_expr)
{
  int ret = OB_SUCCESS;
  bool is_valid = false;
  new_expr = NULL;
  ObSEArray<ObSEArray<ObRawExpr *, 16>, 8> sub_exprs;
  for (int64_t i = 0; OB_SUCC(ret) && i < or_qual.get_param_count(); ++i) {
    ObSEArray<ObRawExpr *, 16> exprs;
    if (OB_FAIL(sub_exprs.push_back(exprs))) {
      LOG_WARN("failed to push back se array", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(check_push_down_expr(table_ids, or_qual, sub_exprs, is_valid))) {
    LOG_WARN("failed to check push down expr", K(ret));
  } else if (!is_valid) {
    /* do nothing */
  } else if (OB_FAIL(generate_push_down_expr(stmt, expr_factory, session_info, sub_exprs, new_expr))) {
    LOG_WARN("failed to generate push down expr", K(ret));
  }
  return ret;
}

int ObOptimizerUtil::check_push_down_expr(const ObRelIds &table_ids,
                                          ObOpRawExpr &or_qual,
                                          ObIArray<ObSEArray<ObRawExpr *, 16> > &sub_exprs,
                                          bool &all_contain)
{
  int ret = OB_SUCCESS;
  all_contain = true;
  for (int64_t i = 0; OB_SUCC(ret) && all_contain && i < or_qual.get_param_count(); ++i) {
    ObRawExpr *cur_expr = or_qual.get_param_expr(i);
    bool contain_op_row = false;
    if (OB_ISNULL(cur_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr in or expr is null", K(ret));
    } else if (T_OP_AND == cur_expr->get_expr_type()) {
      // and expr 中要求至少有一个子expr只涉及到该表的列
      ObOpRawExpr *and_expr = static_cast<ObOpRawExpr *>(cur_expr);
      for (int64_t j = 0; OB_SUCC(ret) && j < and_expr->get_param_count(); ++j) {
        ObRawExpr *cur_and_expr = and_expr->get_param_expr(j);
        contain_op_row = false;
        if (OB_ISNULL(cur_and_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expr in and expr is null", K(ret));
        } else if (cur_and_expr->has_flag(CNT_SUB_QUERY)) {
          //do nothing
        } else if (OB_FAIL(ObRawExprUtils::check_contain_op_row_expr(cur_and_expr, contain_op_row))) {
          LOG_WARN("fail to check contain op row", K(ret));
        } else if (contain_op_row) {
          // do nothing
        } else if (!table_ids.is_superset(cur_and_expr->get_relation_ids())) {
          //do nothing
        } else if (cur_and_expr->get_relation_ids().is_empty() &&
                  !cur_and_expr->is_const_expr()) {
          //do nothing
        } else if (OB_FAIL(sub_exprs.at(i).push_back(cur_and_expr))) {
          LOG_WARN("failed to push back expr", K(ret));
        } else { /* do nothing */ }
      }
      if (OB_FAIL(ret)) {
      } else if (sub_exprs.at(i).empty()) {
        all_contain = false;
      }
    } else if (cur_expr->has_flag(CNT_SUB_QUERY)) {
      all_contain = false;
    } else if (OB_FAIL(ObRawExprUtils::check_contain_op_row_expr(cur_expr, contain_op_row))) {
      LOG_WARN("fail to check contain op row", K(ret));
    } else if (contain_op_row) {
      all_contain = false;
    } else if (!table_ids.is_superset(cur_expr->get_relation_ids())) {
      all_contain = false;
    } else if (cur_expr->get_relation_ids().is_empty() &&
              !cur_expr->is_const_expr()) {
      all_contain = false;
    } else if (OB_FAIL(sub_exprs.at(i).push_back(cur_expr))) {
      LOG_WARN("failed to push back expr", K(ret));
    } else { /* do nothing */ }
  }
  return ret;
}

int ObOptimizerUtil::generate_push_down_expr(const ObDMLStmt *stmt,
                                             ObRawExprFactory &expr_factory,
                                             const ObSQLSessionInfo *session_info,
                                             ObIArray<ObSEArray<ObRawExpr *, 16> > &sub_exprs,
                                             ObOpRawExpr *&new_expr)
{
  int ret = OB_SUCCESS;
  new_expr = NULL;
  ObSEArray<ObRawExpr*, 4> new_expr_params;
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
      ObIArray<ObRawExpr *> &cur_exprs = sub_exprs.at(i);
      const int64_t N = cur_exprs.count();
      if (1 == N) {
        if (OB_FAIL(new_expr_params.push_back(cur_exprs.at(0)))) {
          LOG_WARN("failed to push back param expr", K(ret));
        }
      } else {
        ObOpRawExpr *new_param_expr = NULL;
        // 在原or expr的子expr上有多个expr符合条件，需要生成一个新的and expr
        if (OB_FAIL(expr_factory.create_raw_expr(T_OP_AND, new_param_expr))) {
          LOG_WARN("failed to create and expr", K(ret));
        } else if (OB_ISNULL(new_param_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("create expr get null", K(ret));
        } else if (OB_FAIL(new_param_expr->get_param_exprs().assign(cur_exprs))) {
          LOG_WARN("failed to assign param exprs", K(ret));
        } else if (OB_FAIL(new_param_expr->formalize(session_info))) {
          LOG_WARN("failed to formalize and expr", K(ret));
        } else if (OB_FAIL(new_param_expr->pull_relation_id())) {
          LOG_WARN("failed to pull relation id and levels", K(ret));
        } else if (OB_FAIL(new_expr_params.push_back(new_param_expr))) {
          LOG_WARN("failed to push back param expr", K(ret));
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
    /* do nothing */
  } else if (OB_FAIL(new_expr->get_param_exprs().assign(new_expr_params))) {
    LOG_WARN("failed to assign param exprs", K(ret));
  } else if (OB_FAIL(new_expr->formalize(session_info))) {
    LOG_WARN("failed to formalize or expr", K(ret));
  } else if (OB_FAIL(new_expr->pull_relation_id())) {
    LOG_WARN("failed to pull relation id and levels", K(ret));
  }
  return ret;
}

/**
 * @brief ObOptimizerUtil::simplify_exprs
 * 给定一组 candi_exprs，该函数要找一个最小子集 root_exprs 能够唯一确定所有的 candi_exprs
 * 1. 尝试从candi exprs中找到一个拥有最小parent set的unique fd item, 如果找到了则root exprs = parent set
 * 2. 如未找到匹配的unique fd item, 则遍历not unique fd item, 对于parent set在candi exprs中的,
 *   (a) 检查fd item能否决定candi exprs中未匹配的expr
 *   (b) 检查fd item能够决定root exprs中的expr
 *   (c）如果满足(a), 或者fd item parent set的数量小于该 fd item能够决定的root exprs的数量, 则消除root exprs
 *       中能被决定的root expr, 并添加新的root expr
 */
int ObOptimizerUtil::simplify_exprs(const ObFdItemSet &fd_item_set,
                                    const EqualSets &equal_sets,
                                    const ObIArray<ObRawExpr *> &const_exprs,
                                    const ObIArray<ObRawExpr *> &candi_exprs,
                                    ObIArray<ObRawExpr *> &root_exprs)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObFdItem *, 8> unique_fd_items;
  ObSEArray<ObFdItem *, 8> non_unique_fd_items;
  ObFdItem *fd_item = NULL;
  ObRawExprSet *parent_exprs = NULL;
  int64_t min_parent_count = -1;
  ObRawExprSet *min_parent_exprs = NULL;
  bool is_contain = false;
  bool is_in_child = false;
  ObSqlBitSet<> root_exprs_set;
  int64_t expr_idx = -1;
  root_exprs.reset();
  // 将 fd item 分为unique的和非unique的
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
  // 尝试找到一个parent set数量最少的unique fd item
  for (int64_t i = 0; OB_SUCC(ret) && i < unique_fd_items.count(); ++i) {
    if (OB_ISNULL(fd_item = unique_fd_items.at(i)) ||
        OB_ISNULL(parent_exprs = fd_item->get_parent_exprs())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get get unexpect null", K(ret), K(fd_item));
    } else if (-1 == min_parent_count || parent_exprs->count() < min_parent_count) {
      if (OB_FAIL(is_exprs_contain_fd_parent(candi_exprs, *fd_item,
                                             equal_sets, const_exprs, is_contain))) {
        LOG_WARN("failed to check is order unique", K(ret));
      } else if (is_contain) {
        min_parent_exprs = parent_exprs;
        min_parent_count = parent_exprs->count();
      }
    }
  }
  // 找到一个unique fd item, 直接从candi exprs里找出匹配parent set的expr
  if (OB_SUCC(ret) && NULL != min_parent_exprs) {
    for (int64_t i = 0; OB_SUCC(ret) && i < min_parent_exprs->count(); ++i) {
      if (find_equal_expr(candi_exprs, min_parent_exprs->at(i), equal_sets, expr_idx)) {
        if (OB_FAIL(root_exprs_set.add_member(expr_idx))) {
          LOG_WARN("failed to add member", K(ret));
        }
      } else { /* not find means min_parent_exprs->at(i) is const, do nothing */ }
    }
  }
  // 没有找到unique fd item, 尝试遍历non unique fd items找到一个最小的root expr集合
  if (OB_SUCC(ret) && NULL == min_parent_exprs) {
    ObSqlBitSet<> eliminate_set;
    for (int64_t i = 0; OB_SUCC(ret) && eliminate_set.num_members() < candi_exprs.count() &&
                        i < non_unique_fd_items.count(); ++i) {
      if (OB_ISNULL(fd_item = non_unique_fd_items.at(i)) ||
          OB_ISNULL(parent_exprs = fd_item->get_parent_exprs())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpect null", K(ret), K(fd_item), K(parent_exprs));
      } else if (OB_FAIL(is_exprs_contain_fd_parent(candi_exprs, *fd_item,
                                                    equal_sets, const_exprs, is_contain))) {
        LOG_WARN("failed to check is order unique", K(ret));
      } else if (is_contain) {
        ObSEArray<ObRawExpr *, 8> left_domain;
        ObSEArray<ObRawExpr *, 8> right_domain;
        // 生成新的root exprs集合
        for (int64_t j = 0; OB_SUCC(ret) && j < root_exprs.count(); ++j) {
          if (OB_FAIL(left_domain.push_back(root_exprs.at(j)))) {
            LOG_WARN("failed to push back root expr", K(ret));
          } else if (OB_FAIL(fd_item->check_expr_in_child(root_exprs.at(j), equal_sets, is_in_child))) {
            LOG_WARN("failed to check expr in child", K(ret));
          } else if (is_in_child) {
            // root expr可以被其它expr决定
          } else if (OB_FAIL(right_domain.push_back(root_exprs.at(j)))) {
            LOG_WARN("failed to push back root expr", K(ret));
          }
        }
        for (int64_t j = 0; OB_SUCC(ret) && j < parent_exprs->count(); ++j) {
          if (find_equal_expr(candi_exprs, parent_exprs->at(j), equal_sets, expr_idx)) {
            if (OB_FAIL(right_domain.push_back(candi_exprs.at(expr_idx)))) {
              LOG_WARN("failed to push back root expr", K(ret));
            } else if (eliminate_set.has_member(expr_idx)) {
              // do nothing
            } else if (OB_FAIL(left_domain.push_back(candi_exprs.at(expr_idx)))) {
              LOG_WARN("failed to push back root expr", K(ret));
            }
          }
        }
        // 检查当前fd item能否消除新的candi expr
        for (int64_t j = 0; OB_SUCC(ret) && j < candi_exprs.count(); ++j) {
          if (eliminate_set.has_member(j)) {
            // do nothing
          } else if (OB_FAIL(fd_item->check_expr_in_child(candi_exprs.at(j),
                                                          equal_sets,
                                                          is_in_child))) {
            LOG_WARN("failed to check expr in child", K(ret));
          } else if (!is_in_child) {
            // do nothing
          } else if (OB_FAIL(eliminate_set.add_member(j))) {
            LOG_WARN("failed to add member", K(ret));
          }
        }
        // 如果当前fd item消除了新的candi expr或者新的root expr集合比旧的root expr集合数量更少, 则用
        // 新的root expr集合替换旧的
        if (OB_SUCC(ret)) {
          ObIArray<ObRawExpr*> &new_root_exprs = left_domain.count() <= right_domain.count()
                                                 ? left_domain : right_domain;
          if (OB_FAIL(root_exprs.assign(new_root_exprs))) {
            LOG_WARN("failed to assign exprs", K(ret));
          }
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < root_exprs.count(); ++i) {
      if (OB_UNLIKELY(!find_item(candi_exprs, root_exprs.at(i), &expr_idx))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to find item", K(ret));
      } else if (OB_FAIL(root_exprs_set.add_member(expr_idx))) {
        LOG_WARN("failed to add member", K(ret));
      }
    }
    // 所有non unique fd item都遍历过了, 但是还有其它无法被决定的expr, 直接加入到root exprs中
    if (OB_SUCC(ret) && eliminate_set.num_members() < candi_exprs.count()) {
      for (int64_t i = 0; OB_SUCC(ret) && i < candi_exprs.count(); ++i) {
        if (eliminate_set.has_member(i)) {
          // do nothing
        } else if (OB_FAIL(root_exprs_set.add_member(i))) {
          LOG_WARN("failed to add member", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    root_exprs.reuse();
    // use bitset to keep exprs ordering
    for (int64_t i = 0; OB_SUCC(ret) && i < candi_exprs.count(); ++i) {
      if (!root_exprs_set.has_member(i)) {
        // do nothing
      } else if (OB_FAIL(root_exprs.push_back(candi_exprs.at(i)))) {
        LOG_WARN("failed to push back candi expr", K(ret));
      }
    }
    LOG_TRACE("succeed to simply exprs", K(fd_item_set), K(equal_sets), K(const_exprs),
        K(candi_exprs), K(root_exprs));
  }
  return ret;
}

int ObOptimizerUtil::simplify_ordered_exprs(const ObFdItemSet &fd_item_set,
                                            const EqualSets &equal_sets,
                                            const ObIArray<ObRawExpr *> &const_exprs,
                                            const ObIArray<ObRawExpr *> &exec_ref_exprs,
                                            const ObIArray<OrderItem> &candi_items,
                                            ObIArray<OrderItem> &order_items)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 8> candi_exprs;
  ObSEArray<ObOrderDirection, 8> directions;
  ObSEArray<ObRawExpr *, 8> order_exprs;
  if (OB_FAIL(split_expr_direction(candi_items, candi_exprs, directions))) {
    LOG_WARN("failed to split expr direction", K(ret));
  } else if (OB_FAIL(simplify_ordered_exprs(fd_item_set,
                                            equal_sets,
                                            const_exprs,
                                            exec_ref_exprs,
                                            candi_exprs,
                                            order_exprs))) {
    LOG_WARN("failed to simplify ordered exprs", K(ret));
  } else if (OB_FAIL(make_sort_keys(candi_items, order_exprs, order_items))) {
    LOG_WARN("failed to make sort keys", K(ret));
  }
  return ret;
}

/**
 * @brief ObOptimizerUtil::simplify_ordered_exprs
 * 对给定 candi_exprs, 该函数要找 order_exprs 使得按其排序时等价于 candi_exprs:
 *  1. 顺序读取 candi_exprs 一个元素 expr, 非 const 且未被标记移除时添加到 order_exprs;
 *  2. 获取 fd_item_set 中 parent exprs 为 order_exprs 子集的 fd_items;
 *  3. 标记当前 expr 之后存在于 fd_items child exprs 中的 expr 为待移除;
 *  4. 返回 1. 继续读取 candi_exprs 并添加得到完整 order_exprs.
 */
int ObOptimizerUtil::simplify_ordered_exprs(const ObFdItemSet &fd_item_set,
                                            const EqualSets &equal_sets,
                                            const ObIArray<ObRawExpr *> &const_exprs,
                                            const ObIArray<ObRawExpr *> &exec_ref_exprs,
                                            const ObIArray<ObRawExpr *> &candi_exprs,
                                            ObIArray<ObRawExpr *> &order_exprs)
{
  int ret = OB_SUCCESS;
  ObSqlBitSet<> eliminate_set;
  ObSqlBitSet<> checked_fd_item;
  ObFdItem *fd_item = NULL;
  bool is_contain = false;
  bool is_in_child = false;
  bool is_const = false;
  order_exprs.reset();
  ObSEArray<ObRawExpr *, 8> extended_order_exprs;
  ObSEArray<ObRawExpr *, 8> fd_set_parent_exprs;
  ObRawExpr *first_removed_expr = NULL;
  if (OB_FAIL(get_fd_set_parent_exprs(fd_item_set, fd_set_parent_exprs))) {
    LOG_WARN("failed to get fd set parent exprs ", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < candi_exprs.count(); ++i) {
    ObRawExpr *expr = NULL;
    if (OB_ISNULL(expr = candi_exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null fd item", K(ret));
    } else if (OB_FAIL(is_const_or_equivalent_expr(candi_exprs, equal_sets, const_exprs,
                                                   exec_ref_exprs, i, is_const))) {
      LOG_WARN("failed to check is const expr", K(ret));
    } else if (is_const) {//const expr 不需要排序
      /*do nothing*/
    } else if (eliminate_set.has_member(i)) {
      first_removed_expr = NULL == first_removed_expr ? expr : first_removed_expr;
    } else if (OB_FAIL(order_exprs.push_back(expr))) {
      LOG_WARN("failed to push back exprs", K(ret));
    } else if (OB_FAIL(extended_order_exprs.push_back(expr))) {
      LOG_WARN("failed to push back exprs", K(ret));
    } else if (OB_FAIL(remove_item(fd_set_parent_exprs, expr))) {
      LOG_WARN("failed to remove expr", K(ret));
    } else {
      //查找目前 extended_order_exprs 中包含的 fd, 并使用 fd 查找并标记 candi_exprs i 位置后需要移除的expr
      int64_t last_count = -1;
      while (extended_order_exprs.count() > last_count) {
        last_count = extended_order_exprs.count();
        for (int64_t fd_idx = 0; OB_SUCC(ret) && fd_idx < fd_item_set.count(); ++fd_idx) {
          if (checked_fd_item.has_member(fd_idx)) {//对于已经进行检查的 fd 不再重复检查
            /*do nothing*/
          } else if (OB_ISNULL(fd_item = fd_item_set.at(fd_idx))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get null fd item", K(ret));
          } else if (OB_FAIL(is_exprs_contain_fd_parent(extended_order_exprs, *fd_item,
                                                        equal_sets, const_exprs,
                                                        is_contain))) {
            LOG_WARN("failed to check is exprs contain fd parent", K(ret));
          } else if (is_contain) {
            for (int64_t j = i+1; OB_SUCC(ret) && j < candi_exprs.count(); ++j) {
              if (eliminate_set.has_member(j)) {
                /*do nothing*/
              } else if (OB_FAIL(fd_item->check_expr_in_child(candi_exprs.at(j), equal_sets,
                                                              is_in_child))) {
                LOG_WARN("failed to check expr in fd child", K(ret));
              } else if (!is_in_child) {
                /*do nothing*/
              } else if (candi_exprs.at(j)->has_flag(CNT_SUB_QUERY)) {
                /* to check output more than one row, do not remove subquery in order expr. */
              } else if (OB_FAIL(eliminate_set.add_member(j))) {
                LOG_WARN("failed to add member", K(ret));
              }
            }
            if (OB_FAIL(ret)) {
              /*do nothing*/
            } else if (OB_FAIL(checked_fd_item.add_member(fd_idx))) {
              LOG_WARN("failed to add member", K(ret));
            } else if (OB_FAIL(split_child_exprs(fd_item, equal_sets, fd_set_parent_exprs,
                                                 extended_order_exprs))) {
              LOG_WARN("failed to delete members", K(ret));
            }
          }
        }
      }
    }
  }
  if (OB_FAIL(ret) && order_exprs.empty() && NULL != first_removed_expr
      && OB_FAIL(order_exprs.push_back(first_removed_expr))) {
    LOG_WARN("failed to push back exprs", K(ret));
  }
  return ret;
}

int ObOptimizerUtil::check_subquery_filter(const JoinedTable *table, bool &has)
{
  int ret = OB_SUCCESS;
  has = false;
  if (OB_ISNULL(table)
      || OB_ISNULL(table->left_table_)
      || OB_ISNULL(table->right_table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table is null", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && !has && i < table->get_join_conditions().count(); ++i) {
    const ObRawExpr *expr = NULL;
    if (OB_ISNULL(expr = table->get_join_conditions().at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is null", K(ret), K(expr));
    } else {
      has = expr->has_flag(CNT_SUB_QUERY);
    }
  }
  if (OB_SUCC(ret) && !has && table->left_table_->is_joined_table()) {
    if (OB_FAIL(check_subquery_filter(
                  static_cast<const JoinedTable *>(table->left_table_), has))) {
      LOG_WARN("failed to check subquery filter", K(ret));
    }
  }
  if (OB_SUCC(ret) && !has && table->right_table_->is_joined_table()) {
    if (OB_FAIL(check_subquery_filter(
                  static_cast<const JoinedTable *>(table->right_table_), has))) {
      LOG_WARN("failed to check subquery filter", K(ret));
    }
  }
  return ret;
}

bool ObOptimizerUtil::has_equal_join_conditions(const ObIArray<ObRawExpr*> &join_conditions)
{
  int bret = false;
  for (int64_t i = 0; !bret && i < join_conditions.count(); i++) {
    if (OB_NOT_NULL(join_conditions.at(i))) {
      bret = join_conditions.at(i)->has_flag(IS_JOIN_COND);
    }
  }
  return bret;
}

int ObOptimizerUtil::get_subplan_const_column(const ObDMLStmt &parent_stmt,
                                              const uint64_t table_id,
                                              const ObSelectStmt &child_stmt,
                                              const ObIArray<ObRawExpr*> &exec_ref_exprs,
                                              ObIArray<ObRawExpr *> &output_exprs)
{
  int ret = OB_SUCCESS;
  TableItem *table_item = parent_stmt.get_table_item_by_id(table_id);
  if (OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (table_item->is_lateral_table()) {
    // do nothing
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < child_stmt.get_select_item_size(); ++i) {
      const ObRawExpr *expr = child_stmt.get_select_item(i).expr_;
      ObRawExpr *parent_expr = NULL;
      bool is_const = false;
      bool contains = false;
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null", K(ret), K(expr));
      } else if (OB_FAIL(is_const_expr_recursively(expr, exec_ref_exprs, is_const))) {
        LOG_WARN("failed to check expr is const expr", K(ret));
      } else if (!is_const) {
        // do nothing
      } else if (NULL == (parent_expr = parent_stmt.get_column_expr_by_id(table_id,
                                                                          OB_APP_MIN_COLUMN_ID + i))) {
        // parent expr is prunned
      } else if (OB_FAIL(add_var_to_array_no_dup(output_exprs, parent_expr))) {
        LOG_WARN("failed to add parent expr into output", K(ret));
      }
    }
  }

  return ret;
}

int ObOptimizerUtil::convert_subplan_scan_expr(ObRawExprFactory &expr_factory,
                                               const EqualSets &equal_sets,
                                               const uint64_t table_id,
                                               const ObDMLStmt &parent_stmt,
                                               const ObSelectStmt &child_stmt,
                                               bool skip_invalid,
                                               const ObIArray<ObRawExpr*> &input_exprs,
                                               ObIArray<ObRawExpr*> &output_exprs)
{
  int ret = OB_SUCCESS;
  bool is_valid = true;
  ObSEArray<ObRawExpr*, 8> temp_exprs;
  TableItem *table_item = parent_stmt.get_table_item_by_id(table_id);
  ObRawExprCopier copier(expr_factory);
  if (OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (table_item->is_lateral_table()) {
    // do nothing
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < input_exprs.count(); i++) {
      ObRawExpr *expr = NULL;
      if (OB_ISNULL(input_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null expr", K(input_exprs.at(i)), K(ret));
      } else if (OB_FAIL(convert_subplan_scan_expr(copier,
                                                  equal_sets,
                                                  table_id,
                                                  parent_stmt,
                                                  child_stmt,
                                                  input_exprs.at(i),
                                                  expr))) {
        LOG_WARN("failed to generate subplan scan expr", K(ret));
      } else if (OB_ISNULL(expr)) {
        if (!skip_invalid) {
          is_valid = false;
        }
      } else if (OB_FAIL(add_var_to_array_no_dup(temp_exprs, expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else { /*do nothing*/ }
    }
    if (OB_SUCC(ret) && is_valid && !temp_exprs.empty()) {
      if (OB_FAIL(output_exprs.assign(temp_exprs))) {
        LOG_WARN("failed to push back exprs", K(ret));
      } else {
        LOG_TRACE("succeed to convert subplan scan expr", K(input_exprs),
                  K(output_exprs));
      }
    }
  }
  return ret;
}

class ReplaceSubplanScanExpr : public ObIRawExprReplacer
{
public:
  ReplaceSubplanScanExpr(const EqualSets &eset,
                         const uint64_t table_id,
                         const ObDMLStmt &parent,
                         const ObSelectStmt &child) :
    equal_sets_(eset), table_id_(table_id), parent_stmt_(parent), child_stmt_(child)
  {}

  virtual int generate_new_expr(ObRawExprFactory &expr_factory,
                                ObRawExpr *old_expr,
                                ObRawExpr *&new_expr)
  {
    int ret = OB_SUCCESS;
    UNUSED(expr_factory);
    new_expr = NULL;
    if (old_expr->is_const_or_param_expr()) {
      new_expr = old_expr;
    } else if (OB_FAIL(ObOptimizerUtil::get_parent_stmt_expr(equal_sets_,
                                                     table_id_,
                                                     parent_stmt_,
                                                     child_stmt_,
                                                     old_expr,
                                                     new_expr))) {
      LOG_WARN("failed to get parent stmt expr", K(ret));
    } else {
      // does not replace the old_expr, iterate its child expr
    }
    return ret;
  }
private:
  const EqualSets &equal_sets_;
  const uint64_t table_id_;
  const ObDMLStmt &parent_stmt_;
  const ObSelectStmt &child_stmt_;
};

int ObOptimizerUtil::convert_subplan_scan_expr(ObRawExprCopier &copier,
                                               const EqualSets &equal_sets,
                                               const uint64_t table_id,
                                               const ObDMLStmt &parent_stmt,
                                               const ObSelectStmt &child_stmt,
                                               ObRawExpr *input_expr,
                                               ObRawExpr *&output_expr)
{
  int ret = OB_SUCCESS;
  bool is_valid = true;
  output_expr = NULL;
  ReplaceSubplanScanExpr replacer(equal_sets, table_id, parent_stmt, child_stmt);
  if (OB_FAIL(check_subplan_scan_expr_validity(equal_sets,
                                               table_id,
                                               parent_stmt,
                                               child_stmt,
                                               input_expr,
                                               is_valid))) {
    LOG_WARN("failed to check subplan scan expr validity", K(ret));
  } else if (!is_valid) {
    /*do nothing*/
  } else if (OB_FAIL(get_parent_stmt_expr(equal_sets,
                                          table_id,
                                          parent_stmt,
                                          child_stmt,
                                          input_expr,
                                          output_expr))) {
    LOG_WARN("failed to get parent stmt expr", K(ret));
  } else if (NULL != output_expr) {
    // do nothing
  } else if (OB_FAIL(copier.copy_on_replace(input_expr,
                                            output_expr,
                                            &replacer))) {
    LOG_WARN("failed to copy and replace expr", K(ret));
  } else if (OB_FAIL(output_expr->pull_relation_id())) {
    LOG_WARN("failed to pull releation id", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObOptimizerUtil::check_subplan_scan_expr_validity(const EqualSets &equal_sets,
                                                      const uint64_t table_id,
                                                      const ObDMLStmt &parent_stmt,
                                                      const ObSelectStmt &child_stmt,
                                                      const ObRawExpr *input_expr,
                                                      bool &is_valid)
{
  int ret = OB_SUCCESS;
  ObRawExpr *parent_expr = NULL;
  is_valid = false;
  if (OB_ISNULL(input_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (input_expr->is_const_expr()) {
    is_valid = true;
  } else if (OB_FAIL(get_parent_stmt_expr(equal_sets,
                                          table_id,
                                          parent_stmt,
                                          child_stmt,
                                          input_expr,
                                          parent_expr))) {
    LOG_WARN("failed to get parent stmt expr", K(ret));
  } else if (NULL != parent_expr) {
    is_valid = true;
  } else if (ObRawExprUtils::is_sharable_expr(*input_expr)) {
    is_valid = false;
  } else if (input_expr->get_param_count() == 0) {
    is_valid = false;
  } else {
    is_valid = true;
    for (int64_t i = 0;
        OB_SUCC(ret) && is_valid && i < input_expr->get_param_count(); i++) {
      bool temp_valid = false;
      if (OB_FAIL(SMART_CALL(check_subplan_scan_expr_validity(equal_sets,
                                                              table_id,
                                                              parent_stmt,
                                                              child_stmt,
                                                              input_expr->get_param_expr(i),
                                                              temp_valid)))) {
        LOG_WARN("failed to check subplan scan expr validity", K(ret));
      } else {
        is_valid &= temp_valid;
      }
    }
  }
  return ret;
}

int ObOptimizerUtil::get_parent_stmt_expr(const EqualSets &equal_sets,
                                          const uint64_t table_id,
                                          const ObDMLStmt &parent_stmt,
                                          const ObSelectStmt &child_stmt,
                                          const ObRawExpr *child_expr,
                                          ObRawExpr *&parent_expr)
{
  int ret = OB_SUCCESS;
  parent_expr = NULL;
  if (OB_ISNULL(child_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    for (int64_t i = 0;
        OB_SUCC(ret) && NULL == parent_expr && i < child_stmt.get_select_item_size(); ++i) {
      const ObRawExpr *expr = child_stmt.get_select_item(i).expr_;
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(expr), K(ret));
      } else if (ObOptimizerUtil::is_expr_equivalent(expr, child_expr, equal_sets)) {
        // parent_expr may be null, just ignore
        parent_expr = parent_stmt.get_column_expr_by_id(table_id,
                                                        OB_APP_MIN_COLUMN_ID + i);
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObOptimizerUtil::get_parent_stmt_exprs(const EqualSets &equal_sets,
                                          const uint64_t table_id,
                                          const ObDMLStmt &parent_stmt,
                                          const ObSelectStmt &child_stmt,
                                          const ObIArray<ObRawExpr *> &child_exprs,
                                          ObIArray<ObRawExpr *> &parent_exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < child_exprs.count(); i++) {
    ObRawExpr *parent_expr = NULL;
    if (OB_FAIL(get_parent_stmt_expr(equal_sets, table_id, parent_stmt,
                                     child_stmt, child_exprs.at(i), parent_expr))) {
      LOG_WARN("get parent stmt expr failed", K(ret));
    } else if (OB_FAIL(parent_exprs.push_back(parent_expr))) {
      LOG_WARN("push back expr failed", K(ret));
    }
  }
  return ret;
}

int ObOptimizerUtil::compute_stmt_interesting_order(const ObIArray<OrderItem> &ordering,
                                                    const ObDMLStmt *stmt,
                                                    const bool in_subplan_scan,
                                                    EqualSets &equal_sets,
                                                    const ObIArray<ObRawExpr *> &const_exprs,
                                                    const bool is_parent_set_distinct,
                                                    const int64_t check_scope,
                                                    int64_t &match_info,
                                                    int64_t &max_prefix_count_ptr)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 4> ordering_exprs;
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
    const ObSelectStmt *select_stmt = NULL;
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
      //group by 是否匹配索引前缀
      if (OB_FAIL(is_group_by_match(ordering, select_stmt, equal_sets, const_exprs,
                                    prefix_count, group_match))) {
        LOG_WARN("failed to check is group by match", K(ret));
      } else if (group_match) {
        max_prefix_count = std::max(max_prefix_count, prefix_count);
        match_info |= OrderingFlag::GROUP_MATCH;
        LOG_TRACE("ordering is math group by", K(max_prefix_count), K(prefix_count));
      }
    } else if (has_winfunc && check_winfunc) {
      prefix_count = 0;
      if (OB_FAIL(is_winfunc_match(ordering, select_stmt, equal_sets, const_exprs,
                                   prefix_count, winfunc_match, winfunc_require_sort))) {
        LOG_WARN("failed to check is winfunc match", K(ret));
      } else if (winfunc_match) {
        max_prefix_count = std::max(max_prefix_count, prefix_count);
        match_info |= OrderingFlag::WINFUNC_MATCH;
        LOG_TRACE("ordering is match window function", K(max_prefix_count), K(prefix_count));
      }
    }
    if (OB_SUCC(ret) && (!check_group || !has_group) && !winfunc_require_sort) {
      //没有group并且窗口函数不需要排序的情况下，看distinct和 order by 和 set
      if (has_distinct && check_distinct) {
        prefix_count = 0;
        if (OB_FAIL(is_distinct_match(ordering_exprs, select_stmt, equal_sets, const_exprs,
                                      prefix_count, distinct_match))) {
          LOG_WARN("failed to check is distinct match", K(ret));
        } else if (distinct_match) {
          max_prefix_count = std::max(max_prefix_count, prefix_count);
          match_info |= OrderingFlag::DISTINCT_MATCH;
          LOG_TRACE("ordering is math distinct", K(max_prefix_count), K(prefix_count));
        }
      } else if (check_set) {
        //没有distinct的情况下才看 set(union/interscept)
        prefix_count = 0;
        if (NULL != select_stmt && is_parent_set_distinct) {
          if (OB_FAIL(is_set_match(ordering_exprs, select_stmt, equal_sets, const_exprs,
                                   prefix_count, set_match))) {
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
        if (OB_FAIL(is_order_by_match(ordering, stmt, equal_sets, const_exprs,
                                      prefix_count, orderby_match))) {
          LOG_WARN("failed to check is order by match", K(ret));
        } else if (orderby_match) {
          max_prefix_count = std::max(max_prefix_count, prefix_count);
          match_info |= OrderingFlag::ORDERBY_MATCH;
          LOG_TRACE("ordering is math order by", K(max_prefix_count), K(prefix_count));
        }
      }
    }
    if (OB_SUCC(ret)) {
      max_prefix_count_ptr = max_prefix_count;
    }
    if (OB_SUCC(ret) && in_subplan_scan && !ordering_exprs.empty()) {
      if ((check_group && has_group && !group_match) ||
          (check_winfunc && winfunc_require_sort) ||
          (check_distinct && has_distinct && !distinct_match) ||
          (check_order && has_orderby && !orderby_match)) {
        // do nothing
      } else {
        match_info |= OrderingFlag::POTENTIAL_MATCH;
      }
    }
  }
  return ret;
}

int ObOptimizerUtil::compute_stmt_interesting_order(const ObIArray<OrderItem> &ordering,
                                                    const ObDMLStmt *stmt,
                                                    const bool in_subplan_scan,
                                                    EqualSets &equal_sets,
                                                    const ObIArray<ObRawExpr *> &const_exprs,
                                                    const bool is_parent_set_distinct,
                                                    const int64_t check_scope,
                                                    int64_t &match_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (check_scope == OrderingCheckScope::NOT_CHECK || ordering.empty()) {
    // do nothing
  } else {
    const ObSelectStmt *select_stmt = NULL;
    bool check_group = (check_scope & OrderingCheckScope::CHECK_GROUP) > 0;
    bool check_winfunc = (check_scope & OrderingCheckScope::CHECK_WINFUNC) > 0;
    bool check_distinct = (check_scope & OrderingCheckScope::CHECK_DISTINCT) > 0;
    bool check_set = (check_scope & OrderingCheckScope::CHECK_SET) > 0;
    bool check_order = (check_scope & OrderingCheckScope::CHECK_ORDERBY) > 0;
    bool has_group = false;
    bool has_distinct = false;
    bool has_winfunc = false;
    bool has_orderby = stmt->has_order_by();
    bool is_match = false;
    ObSEArray<ObRawExpr *, 4> ordering_exprs;
    ObSEArray<ObOrderDirection, 4> ordering_directions;
    if (stmt->is_select_stmt()) {
      select_stmt = static_cast<const ObSelectStmt*>(stmt);
      has_group = select_stmt->get_group_expr_size() > 0 || select_stmt->get_rollup_expr_size() > 0;
      has_distinct = select_stmt->has_distinct();
      has_winfunc = select_stmt->has_window_function();
    }

    if (OB_FAIL(split_expr_direction(ordering, ordering_exprs, ordering_directions))) {
        LOG_WARN("failed to split expr direction", K(ret));
    } else if (has_group && check_group) {
      if (select_stmt->get_group_expr_size() > 0) {
        if (OB_FAIL(prefix_subset_exprs(select_stmt->get_group_exprs(), ordering_exprs,
                                        equal_sets, const_exprs, is_match))) {
          LOG_WARN("check is covered by ordering failed", K(ret));
        }
      } else if (select_stmt->get_rollup_expr_size() > 0) {
        if (OB_FAIL(prefix_subset_exprs(select_stmt->get_rollup_exprs(), ordering_exprs,
                                        equal_sets, const_exprs, is_match))) {
          LOG_WARN("check is covered by ordering failed", K(ret));
        }
      }
      if (OB_SUCC(ret) && is_match) {
        match_info |= OrderingFlag::GROUP_MATCH;
        LOG_TRACE("ordering is math group by");
      }
    } else if (has_winfunc && check_winfunc) {
      bool winfunc_require_sort = false;
      if (OB_FAIL(is_winfunc_match(ordering, select_stmt, equal_sets, const_exprs, is_match))) {
        LOG_WARN("failed to check is winfunc match", K(ret));
      } else if (is_match) {
        match_info |= OrderingFlag::WINFUNC_MATCH;
        LOG_TRACE("ordering is match window function");
      }
    } else if (has_distinct && check_distinct) {
      ObSEArray<ObRawExpr *, 4> select_exprs;
      if (OB_FAIL(select_stmt->get_select_exprs(select_exprs))) {
        LOG_WARN("failed to get select exprs", K(ret));
      } else if (OB_FAIL(prefix_subset_exprs(select_exprs, ordering_exprs, equal_sets,
                                             const_exprs, is_match))) {
        LOG_WARN("check is covered by ordering failed", K(ret));
      } else if (is_match) {
        match_info |= OrderingFlag::DISTINCT_MATCH;
        LOG_TRACE("ordering is math distinct");
      }
    } else if (check_set && NULL != select_stmt && is_parent_set_distinct) {
      ObSEArray<ObRawExpr *, 4> select_exprs;
      if (OB_FAIL(select_stmt->get_select_exprs(select_exprs))) {
        LOG_WARN("failed to get select exprs", K(ret));
      } else if (OB_FAIL(prefix_subset_exprs(select_exprs, ordering_exprs, equal_sets,
                                             const_exprs, is_match))) {
        LOG_WARN("check is covered by ordering failed", K(ret));
      } else if (is_match) {
        match_info |= OrderingFlag::SET_MATCH;
        LOG_TRACE("ordering is match set");
      }
    } else if (has_orderby && check_order) {
      if (OB_FAIL(is_order_by_match(stmt->get_order_items(), ordering,
                                    equal_sets, const_exprs, is_match))) {
        LOG_WARN("failed to check is order by match", K(ret));
      } else if (is_match) {
        match_info |= OrderingFlag::ORDERBY_MATCH;
        LOG_TRACE("ordering is math order by");
      }
    } else if (in_subplan_scan) {
      match_info |= OrderingFlag::POTENTIAL_MATCH;
    }
  }
  return ret;
}

int ObOptimizerUtil::is_group_by_match(const ObIArray<OrderItem> &ordering,
                                       const ObSelectStmt *select_stmt,
                                       const EqualSets &equal_sets,
                                       const ObIArray<ObRawExpr *> &const_exprs,
                                       int64_t &match_prefix_count,
                                       bool &sort_match)
{
  int ret = OB_SUCCESS;
  match_prefix_count = 0;
  sort_match = false;
  int64_t match_count = 0;
  bool full_covered = true;
  ObSEArray<ObRawExpr *, 4> ordering_exprs;
  ObSEArray<ObOrderDirection, 4> ordering_directions;
  if (OB_ISNULL(select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is invalid", K(ret), K(select_stmt));
  } else if (OB_FAIL(split_expr_direction(ordering, ordering_exprs, ordering_directions))) {
    LOG_WARN("failed to split expr direction", K(ret));
  } else if (select_stmt->get_group_expr_size() > 0 &&
             OB_FAIL(prefix_subset_exprs(select_stmt->get_group_exprs(),
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
        OB_FAIL(match_order_by_against_index(mock_ordering,
                                             ordering,
                                             match_count,
                                             equal_sets,
                                             const_exprs,
                                             full_covered,
                                             match_count,
                                             false))) {
      LOG_WARN("failed to check match order by against index", K(ret));
    }
  }
  if (OB_SUCC(ret) && match_count > 0) {
    sort_match = true;
    match_prefix_count = match_count;
  }
  return ret;
}

int ObOptimizerUtil::is_winfunc_match(const ObIArray<OrderItem> &ordering,
                                      const ObSelectStmt *select_stmt,
                                      const EqualSets &equal_sets,
                                      const ObIArray<ObRawExpr *> &const_exprs,
                                      bool &is_match)
{
  int ret = OB_SUCCESS;
  is_match = false;
  ObSEArray<ObRawExpr *, 4> ordering_exprs;
  ObSEArray<ObOrderDirection, 4> ordering_directions;
  const ObWinFunRawExpr *win_expr = NULL;
  if (OB_ISNULL(select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("select_stmt is null", K(ret), K(select_stmt));
  } else if (OB_FAIL(split_expr_direction(ordering, ordering_exprs, ordering_directions))) {
    LOG_WARN("failed to split expr direction", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && !is_match && i < select_stmt->get_window_func_count(); ++i) {
    if (OB_ISNULL(win_expr = select_stmt->get_window_func_expr(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret), K(win_expr));
    } else if (!win_expr->get_partition_exprs().empty()) {
      if (OB_FAIL(prefix_subset_exprs(win_expr->get_partition_exprs(), ordering_exprs,
                                      equal_sets, const_exprs, is_match))) {
        LOG_WARN("check is covered by ordering failed", K(ret));
      }
    } else if (OB_FAIL(is_order_by_match(win_expr->get_order_items(), ordering,
                                         equal_sets, const_exprs, is_match))) {
      LOG_WARN("failed to check is winfunc match", K(ret));
    }
  }
  return ret;
}

int ObOptimizerUtil::is_order_by_match(const ObIArray<OrderItem> &expect_ordering,
                                       const ObIArray<OrderItem> &input_ordering,
                                       const EqualSets &equal_sets,
                                       const ObIArray<ObRawExpr *> &const_exprs,
                                       bool &is_match)
{
  int ret = OB_SUCCESS;
  is_match = false;
  ObRawExpr *input_expr = NULL;
  ObRawExpr *expect_expr = NULL;
  int64_t expect_offset = 0;
  int64_t input_offset = 0;
  bool direction_match = false;
  bool is_const = false;
  bool finish_check = false;
  while (OB_SUCC(ret) && !is_match && !finish_check && input_offset < input_ordering.count()
                      && expect_offset < expect_ordering.count()) {
    direction_match = is_ascending_direction(expect_ordering.at(expect_offset).order_type_)
                      == is_ascending_direction(input_ordering.at(input_offset).order_type_);
    if (OB_ISNULL(input_expr = input_ordering.at(input_offset).expr_)
        || OB_ISNULL(expect_expr = expect_ordering.at(expect_offset).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(input_expr), K(expect_expr));
    } else if (is_expr_equivalent(input_expr, expect_expr, equal_sets) && direction_match) {
      is_match = true;
    } else if (OB_FAIL(is_const_expr(expect_expr, equal_sets, const_exprs, is_const))) {
      LOG_WARN("failed to check expr is const expr", K(ret));
    } else if (is_const) {
      ++expect_offset;
    } else if (OB_FAIL(is_const_expr(input_expr, equal_sets, const_exprs, is_const))) {
      LOG_WARN("failed to check expr is const expr", K(ret));
    } else if (is_const) {
      ++input_offset;
    } else {
      finish_check = true;
    }
  }
  return ret;
}

int ObOptimizerUtil::is_winfunc_match(const ObIArray<OrderItem> &ordering,
                                      const ObSelectStmt *select_stmt,
                                      const EqualSets &equal_sets,
                                      const ObIArray<ObRawExpr *> &const_exprs,
                                      int64_t &match_prefix_count,
                                      bool &sort_match,
                                      bool &sort_is_required)
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
      if (OB_FAIL(is_winfunc_match(ordering, select_stmt,
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

int ObOptimizerUtil::is_winfunc_match(const ObIArray<OrderItem> &ordering,
                                      const ObSelectStmt *select_stmt,
                                      const ObWinFunRawExpr *win_expr,
                                      const EqualSets &equal_sets,
                                      const ObIArray<ObRawExpr *> &const_exprs,
                                      int64_t &match_prefix_count,
                                      bool &sort_match,
                                      bool &sort_is_required)
{
  int ret = OB_SUCCESS;
  match_prefix_count = 0;
  sort_match = false;
  sort_is_required = false;
  int64_t partition_match_count = 0;
  int64_t order_match_count = 0;
  bool full_covered = false;
  ObSEArray<ObRawExpr *, 4> ordering_exprs;
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

int ObOptimizerUtil::match_order_by_against_index(const ObIArray<OrderItem> &expect_ordering,
                                                  const ObIArray<OrderItem> &input_ordering,
                                                  const int64_t input_start_offset,
                                                  const EqualSets &equal_sets,
                                                  const ObIArray<ObRawExpr *> &const_exprs,
                                                  bool &full_covered,
                                                  int64_t &match_count,
                                                  bool check_direction /* = true */)
{
  int ret = OB_SUCCESS;
  int64_t expect_offset = expect_ordering.count();
  int64_t input_offset = input_start_offset;
  bool is_const = true;
  full_covered = false;
  match_count = input_start_offset;
  //找到第一个非const的order项
  for (int64_t i = 0; OB_SUCC(ret) && is_const && i < expect_ordering.count(); ++i) {
    if (OB_FAIL(is_const_expr(expect_ordering.at(i).expr_, equal_sets, const_exprs, is_const))) {
      LOG_WARN("failed to check is_const_expr", K(ret));
    } else if (!is_const) {
      expect_offset = i;
    }
  }
  //计算index和order项的最大匹配，跳过const表达式
  ObRawExpr *input_expr = NULL;
  ObRawExpr *expect_expr = NULL;
  bool direction_match = false;
  while (OB_SUCC(ret) && input_offset < input_ordering.count() &&
         expect_offset < expect_ordering.count()) {
    direction_match = check_direction ? is_ascending_direction(expect_ordering.at(expect_offset).order_type_)
        == is_ascending_direction(input_ordering.at(input_offset).order_type_) : true;

    if (OB_ISNULL(input_expr = input_ordering.at(input_offset).expr_)
        || OB_ISNULL(expect_expr = expect_ordering.at(expect_offset).expr_)) {
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

int ObOptimizerUtil::is_set_or_distinct_match(const ObIArray<ObRawExpr*> &keys,
                                              const ObSelectStmt *select_stmt,
                                              const EqualSets &equal_sets,
                                              const ObIArray<ObRawExpr *> &const_exprs,
                                              int64_t &match_prefix_count,
                                              bool &sort_match)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is invalid", K(ret));
  } else {
    ObSEArray<ObRawExpr *, 16> distinct_exprs;
    sort_match = false;
    match_prefix_count = 0;
    if (OB_FAIL(select_stmt->get_select_exprs(distinct_exprs))) {
      LOG_WARN("failed to get select exprs", K(ret));
    } else {
      bool full_covered = false;
      int64_t match_count = 0;
      if (OB_FAIL(prefix_subset_exprs(distinct_exprs,
                                      keys,
                                      equal_sets,
                                      const_exprs,
                                      full_covered,
                                      &match_count))) {
        LOG_WARN("check is covered by ordering failed", K(ret));
      } else if (match_count > 0) {
        sort_match = true; //only consider prefix
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
int ObOptimizerUtil::is_distinct_match(const ObIArray<ObRawExpr*> &keys,
                                       const ObSelectStmt *stmt,
                                       const EqualSets &equal_sets,
                                       const ObIArray<ObRawExpr *> &const_exprs,
                                       int64_t &match_prefix_count,
                                       bool &sort_match)
{
  int ret = OB_SUCCESS;
  sort_match = false;
  if (OB_ISNULL(stmt)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("stmt should not be null", K(ret));
  } else if (stmt->has_distinct()) {
    if (OB_FAIL(is_set_or_distinct_match(keys, stmt, equal_sets, const_exprs,
                                         match_prefix_count, sort_match))) {
      LOG_WARN("check is_set_or_distinct_match failed", K(ret));
    }
  }
  return ret;
}

/**
 * (select c1 from t1) union (select c2 from t2);
 * the order of c1 can be use by union
 */
int ObOptimizerUtil::is_set_match(const ObIArray<ObRawExpr*> &keys,
                                  const ObSelectStmt *stmt,
                                  const EqualSets &equal_sets,
                                  const ObIArray<ObRawExpr *> &const_exprs,
                                  int64_t &match_prefix_count,
                                  bool &sort_match)
{
  return is_set_or_distinct_match(keys, stmt, equal_sets, const_exprs,
                                  match_prefix_count, sort_match);
}

//get max prefix
int ObOptimizerUtil::is_order_by_match(const ObIArray<OrderItem> &ordering,
                                       const ObDMLStmt *stmt,
                                       const EqualSets &equal_sets,
                                       const ObIArray<ObRawExpr *> &const_exprs,
                                       int64_t &match_prefix,
                                       bool &sort_match)
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

int ObOptimizerUtil::is_lossless_column_conv(const ObRawExpr *expr, bool &is_lossless)
{
  int ret = OB_SUCCESS;
  is_lossless = false;
  const ObRawExpr *child_expr = NULL;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (T_FUN_COLUMN_CONV != expr->get_expr_type()) {
    // do nothing
  } else if (OB_ISNULL(child_expr = expr->get_param_expr(4))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
   } else {
    is_lossless = is_lossless_type_conv(child_expr->get_result_type(), expr->get_result_type());
  }
  return ret;
}

bool ObOptimizerUtil::is_lossless_type_conv(const ObExprResType &child_type, const ObExprResType &dst_type) {
  bool is_lossless = false;
  ObObjTypeClass child_tc = child_type.get_type_class();
  ObObjTypeClass dst_tc = dst_type.get_type_class();
  ObAccuracy dst_acc = dst_type.get_accuracy();
  if (child_type.get_type() == dst_type.get_type() &&
    (child_type.get_accuracy().get_precision() == dst_acc.get_precision() || -1 == dst_acc.get_precision()) &&
    (child_type.get_accuracy().get_scale() == dst_acc.get_scale() || -1 == dst_acc.get_scale())) {
    if (is_oracle_mode()
       && ObNumberTC == dst_tc
       && child_type.get_accuracy().get_scale() != child_type.get_accuracy().get_scale()) {
      //TODO: need to be supplemented for the conversion from number to number
    } else if (ob_is_string_type(child_type.get_type())) {
      if (dst_type.get_obj_meta().get_collation_type() == child_type.get_obj_meta().get_collation_type() &&
         (child_type.get_accuracy().get_length() <= dst_acc.get_length() || -1 == dst_acc.get_length())) {
        is_lossless = true;
      }
    } else if (ob_is_numeric_type(child_type.get_type()) || ob_is_temporal_type(child_type.get_type()) || ob_is_otimestampe_tc(child_type.get_type())) {
      is_lossless = true;
    }
  }
  if (is_lossless) {
    //do nothing
  } else if (!is_oracle_mode()) {
    // mysql模式允许的无损类型转换可以参考
    if (ObIntTC == child_tc || ObUIntTC == child_tc) {
      if (ObNumberTC == dst_tc || ObDecimalIntTC == dst_tc) {
        ObAccuracy lossless_acc = child_type.get_accuracy();
        if ((dst_acc.get_scale() >= 0
             && dst_acc.get_precision() - dst_acc.get_scale() >= lossless_acc.get_precision())
            || (-1 == dst_acc.get_precision() && -1 == dst_acc.get_scale())) {
          is_lossless = true;
        }
      } else if (child_type.get_type() == dst_type.get_type()) {
        is_lossless = true;
      }
    } else if (ObFloatTC == child_tc || ObDoubleTC == child_tc) {
      if (child_tc == dst_tc || ObDoubleTC == dst_tc) {
        ObAccuracy lossless_acc = child_type.get_accuracy();
        if (-1 == dst_acc.get_precision() && -1 == dst_acc.get_scale()) {
          is_lossless = true;
        } else if (dst_acc.get_precision() >= lossless_acc.get_precision()
                   && dst_acc.get_scale() >= lossless_acc.get_scale()) {
          is_lossless = true;
        }
      }
    } else if (ObTimestampType == child_type.get_type()
               || ObDateTimeType == child_type.get_type()) {
      if (ObDateTimeType == dst_type.get_type() || ObTimestampType == dst_type.get_type()) {
        if (child_type.get_accuracy().get_precision() == dst_acc.get_precision()
            && child_type.get_accuracy().get_scale() == dst_acc.get_scale()) {
          is_lossless = true;
        }
      }
    } else if (ObDateTC == child_tc || ObTimeTC == child_tc) {
      if (child_tc == dst_tc || ObDateTimeType == dst_type.get_type()
          || ObTimestampType == dst_type.get_type()) {
        if (-1 == dst_acc.get_precision() && -1 == dst_acc.get_scale()) { is_lossless = true; }
      }
    } else if (ObYearTC == child_tc) {
      if (ObNumberTC == dst_tc) {
        ObAccuracy lossless_acc = ObAccuracy::DDL_DEFAULT_ACCURACY2[ObCompatibilityMode::MYSQL_MODE][child_type.get_type()];
        if (dst_acc.get_precision() - dst_acc.get_scale() >= lossless_acc.get_precision() - lossless_acc.get_scale()) {
          is_lossless = true;
         }
      } else if (ObDoubleTC == dst_tc) {
        if (-1 == dst_acc.get_precision() && -1 == dst_acc.get_scale()) {
           is_lossless = true;
         }
      } else if (child_tc == dst_tc) {
        is_lossless = true;
      }
    } else if (ObDecimalIntType == child_type.get_type()) {
      if (ObNumberTC == dst_tc) {
        is_lossless = ((PRECISION_UNKNOWN_YET == dst_type.get_precision()
                        && NUMBER_SCALE_UNKNOWN_YET == dst_type.get_scale())
                       || (dst_type.get_scale() >= child_type.get_scale()
                           && dst_type.get_precision() >= child_type.get_precision()));
      } else if (ObDecimalIntTC == dst_tc) {
        is_lossless = (child_type.get_scale() <= dst_type.get_scale()
                       && child_type.get_precision() <= dst_type.get_precision());
      }
    } else if (ObNumberTC == child_tc) {
      if (ObDecimalIntTC == dst_tc) {
        if (child_type.get_scale() == NUMBER_SCALE_UNKNOWN_YET
            || child_type.get_precision() == PRECISION_UNKNOWN_YET) {
          is_lossless = false;
        } else if (dst_tc == ObNumberTC) {
          is_lossless = (dst_type.get_scale() >= child_type.get_scale()
                         && dst_type.get_precision() >= child_type.get_precision());
        }
      }
      // varchar, varbinnary
    } else if (ObCharType == child_type.get_type() || ObVarcharType == child_type.get_type()) {
      //in mysql c1 varchar(x) as (func(y)) can not insert data makes length(fun(y)) > x
      if (child_type.get_type() == dst_type.get_type() &&
            dst_type.get_obj_meta().get_collation_type() == child_type.get_obj_meta().get_collation_type()) {
        is_lossless = true;
      }
    }
  } else {
    if (ObIntTC == child_tc || ObUIntTC == child_tc) {
      //TODO: need to be supplemented for the conversion from number to number
      if (child_tc == dst_tc || ObNumberTC == dst_tc || ObDecimalIntTC == dst_tc) {
        ObAccuracy lossless_acc = child_type.get_accuracy();
        if ((dst_acc.get_scale() >= 0
             && dst_acc.get_precision() - dst_acc.get_scale() >= lossless_acc.get_precision())
            || (-1 == dst_acc.get_precision() && -1 == dst_acc.get_scale())) {
          is_lossless = true;
        }
      }
    } else if (ObCharType == child_type.get_type() || ObVarcharType == child_type.get_type()
               || ObNCharType == child_type.get_type()
               || ObNVarchar2Type == child_type.get_type()) {
      // in oracle creating table ... c1 varchar(x) as (func(y)) will ensure that x>=length(y);
      if (child_type.get_type() == dst_type.get_type()
          && dst_type.get_obj_meta().get_collation_type()
               == child_type.get_obj_meta().get_collation_type()) {
        is_lossless = true;
      }
    } else if (ObDecimalIntType == child_type.get_type()) {
      if (ObNumberTC == dst_tc) {
        is_lossless = ((NUMBER_SCALE_UNKNOWN_YET == dst_type.get_scale()
                        && PRECISION_UNKNOWN_YET == dst_type.get_precision())
                       || (dst_type.get_scale() >= child_type.get_scale()
                           && dst_type.get_precision() >= child_type.get_precision()));
      } else if (ObDecimalIntTC == dst_tc) {
        is_lossless = (child_type.get_scale() <= dst_type.get_scale()
                       && child_type.get_precision() <= dst_type.get_precision());
      }
    } else if (ObNumberTC == child_tc) {
      if (ObDecimalIntTC == dst_tc) {
        if (child_type.get_scale() == NUMBER_SCALE_UNKNOWN_YET
            || child_type.get_precision() == PRECISION_UNKNOWN_YET) {
          is_lossless = false;
        } else {
          is_lossless = (dst_type.get_scale() >= child_type.get_scale()
                         && dst_type.get_precision() >= child_type.get_precision());
        }
      } else if (ObNumberTC == dst_tc) {
        is_lossless = NUMBER_SCALE_UNKNOWN_YET == dst_type.get_scale()
                      && PRECISION_UNKNOWN_YET == dst_type.get_precision();
      }
    }
  }
  return is_lossless;
}

int ObOptimizerUtil::is_lossless_column_cast(const ObRawExpr *expr, bool &is_lossless)
{
  int ret = OB_SUCCESS;
  is_lossless = false;
  const ObRawExpr *child_expr = NULL;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (T_FUN_SYS_CAST != expr->get_expr_type()) {
    // do nothing
  } else if (expr->is_const_expr() && CM_IS_CONST_TO_DECIMAL_INT(expr->get_extra())) {
    // do nothing
  } else if (OB_ISNULL(child_expr = expr->get_param_expr(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    ObExprResType child_type = child_expr->get_result_type();
    ObObjTypeClass child_tc = child_type.get_type_class();
    ObExprResType dst_type = expr->get_result_type();
    ObObjTypeClass dst_tc = dst_type.get_type_class();
    ObAccuracy dst_acc = dst_type.get_accuracy();
    if (!is_oracle_mode()) {
      // mysql模式允许的无损类型转换可以参考
      if (ObIntTC == child_tc || ObUIntTC == child_tc) {
        if (ObNumberTC == dst_tc || ObDecimalIntTC == dst_tc) {
          // ObAccuracy lossless_acc = ObAccuracy::DDL_DEFAULT_ACCURACY2[ObCompatibilityMode::MYSQL_MODE][child_type.get_type()];
          ObAccuracy lossless_acc = child_type.get_accuracy();
          if ((dst_acc.get_scale() >= 0 &&
               dst_acc.get_precision() - dst_acc.get_scale() >= lossless_acc.get_precision()) ||
              (-1 == dst_acc.get_precision() && -1 == dst_acc.get_scale())) {
            is_lossless = true;
          }
        } else if (child_tc == dst_tc) {
          is_lossless = true;
        }
      } else if (ObBitTC == child_tc) {
        if (ObNumberTC == dst_tc || ObDecimalIntTC == dst_tc) {
          const double log10_2 = 0.30103;
          ObAccuracy lossless_acc = child_type.get_accuracy();
          if (dst_acc.get_scale() >= 0
              && dst_acc.get_precision() - dst_acc.get_scale()
                 >= lossless_acc.get_precision() * log10_2) {
            // log10(2) = 0.30102999566398114; log10(2^n) = n*log10(2);
            // cast(b'111' as decimal(1,0)) is lossless
            is_lossless = true;
          }
        }
      } else if (ObFloatTC == child_tc || ObDoubleTC == child_tc) {
        if (child_tc == dst_tc) {
          ObAccuracy lossless_acc = child_type.get_accuracy();
          if (lossless_acc.get_scale() == dst_acc.get_scale()) {
            is_lossless = true;
          }
        }
      } else if (ObDateTimeType == child_type.get_type()) {
        // do nothing
      } else if (ObTimestampType == child_type.get_type()) {
        if (child_tc == dst_tc || ObDateTimeType == dst_type.get_type()) {
          if (child_type.get_accuracy().get_precision() == dst_acc.get_precision() &&
              child_type.get_accuracy().get_scale() == dst_acc.get_scale()) {
            is_lossless = true;
          }
        }
      } else if (ObDateTC == child_tc || ObTimeTC == child_tc) {
        if (child_tc == dst_tc || ObDateTimeType == dst_type.get_type() || ObTimestampType == dst_type.get_type()) {
          if (-1 == dst_acc.get_precision() && -1 == dst_acc.get_scale()) {
            is_lossless = true;
          }
        }
      } else if (ObYearTC == child_tc) {
        if (ObNumberTC == dst_tc || ObDecimalIntTC == dst_tc) {
          ObAccuracy lossless_acc = ObAccuracy::DDL_DEFAULT_ACCURACY2[ObCompatibilityMode::MYSQL_MODE][child_type.get_type()];
          if (dst_acc.get_precision() - dst_acc.get_scale() >= lossless_acc.get_precision() - lossless_acc.get_scale()) {
            is_lossless = true;
          }
        } else if (ObDoubleTC == dst_tc) {
          if (-1 == dst_acc.get_precision() && -1 == dst_acc.get_scale()) {
            is_lossless = true;
          }
        }
      } else if (ObVarcharType == child_type.get_type()) {
        // varchar, varbinnary
      } else if (ObCharType == child_type.get_type()) {
        if (child_tc == dst_tc || ObVarcharType == dst_type.get_type()) {
          if (dst_acc.get_length() == child_type.get_accuracy().get_length() &&
              dst_type.get_obj_meta().get_collation_type() == child_type.get_obj_meta().get_collation_type()) {
            is_lossless = true;
          }
        }
      } else if (ObDecimalIntType == child_type.get_type()) {
        if (ObNumberTC == dst_tc) {
          is_lossless = ((PRECISION_UNKNOWN_YET == dst_type.get_precision()
                         && NUMBER_SCALE_UNKNOWN_YET == dst_type.get_scale())
                        || (dst_type.get_scale() >= child_type.get_scale()
                            && dst_type.get_precision() >= child_type.get_precision()));
        } else if (ObDecimalIntTC == dst_tc) {
          is_lossless = (child_type.get_scale() <= dst_type.get_scale()
                         && child_type.get_precision() <= dst_type.get_precision());
        }
      } else if (ObNumberTC == child_tc) {
        if (ObDecimalIntTC == dst_tc) {
          if (child_type.get_scale() == NUMBER_SCALE_UNKNOWN_YET
              || child_type.get_precision() == PRECISION_UNKNOWN_YET) {
            is_lossless = false;
          } else {
            is_lossless = (dst_type.get_scale() >= child_type.get_scale()
                           && dst_type.get_precision() >= child_type.get_precision());
          }
        }
      }
    } else {
      if (ObIntTC == child_tc || ObUIntTC == child_tc) {
        //TODO: need to be supplemented for the conversion from number to number
        if (child_tc == dst_tc || ObNumberTC == dst_tc || ObDecimalIntTC == dst_tc) {
          ObAccuracy lossless_acc = child_type.get_accuracy();
          if ((dst_acc.get_scale() >= 0 &&
               dst_acc.get_precision() - dst_acc.get_scale() >= lossless_acc.get_precision()) ||
              (-1 == dst_acc.get_precision() && -1 == dst_acc.get_scale())) {
            is_lossless = true;
          }
        }
      } else if (ObDecimalIntType == child_type.get_type()) {
        if (ObNumberTC == dst_tc) {
          is_lossless = ((NUMBER_SCALE_UNKNOWN_YET == dst_type.get_scale()
                         && PRECISION_UNKNOWN_YET == dst_type.get_precision())
                        || (dst_type.get_scale() >= child_type.get_scale()
                            && dst_type.get_precision() >= child_type.get_precision()));
        } else if (ObDecimalIntTC == dst_tc) {
          is_lossless = (child_type.get_scale() <= dst_type.get_scale()
                         && child_type.get_precision() <= dst_type.get_precision());
        }
      } else if (ObNumberTC == child_tc) {
        if (ObDecimalIntTC == dst_tc) {
          if (child_type.get_scale() == NUMBER_SCALE_UNKNOWN_YET
              || child_type.get_precision() == PRECISION_UNKNOWN_YET) {
            is_lossless = false;
          } else {
            is_lossless = (dst_type.get_scale() >= child_type.get_scale()
                           && dst_type.get_precision() >= child_type.get_precision());
          }
        } else if (ObNumberTC == dst_tc) {
          is_lossless = NUMBER_SCALE_UNKNOWN_YET == dst_type.get_scale()
                        && PRECISION_UNKNOWN_YET == dst_type.get_precision();
        }
      } else if (ObCharType == child_type.get_type()) {
        if (ObVarcharType == dst_type.get_type() && !expr->is_const_expr()) {
          if ((dst_acc.get_length() >= child_type.get_accuracy().get_length() ||
               dst_acc.get_length() == -1) &&
              dst_type.get_obj_meta().get_collation_type() == child_type.get_obj_meta().get_collation_type()) {
            is_lossless = true;
          }
        }
      } else if (ObNCharType == child_type.get_type()) {
        if (ObNVarchar2Type == dst_type.get_type() && !expr->is_const_expr()) {
          if ((dst_acc.get_length() >= child_type.get_accuracy().get_length() ||
               dst_acc.get_length() == -1) &&
               dst_type.get_obj_meta().get_collation_type() == child_type.get_obj_meta().get_collation_type()) {
            is_lossless = true;
          }
        }
      }
    }
    LOG_DEBUG("lossless column cast", K(child_type), K(child_tc), K(dst_type), K(dst_tc),
              K(is_lossless));
  }
  return ret;
}

int ObOptimizerUtil::get_expr_without_lossless_cast(const ObRawExpr* ori_expr,
                                                    const ObRawExpr*& expr)
{
  int ret = OB_SUCCESS;
  bool is_lossless = false;
  expr = ori_expr;
  if (OB_ISNULL(ori_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (ori_expr->get_expr_type() == T_FUN_SYS_INNER_ROW_CMP_VALUE) {
    if (OB_FAIL(get_expr_without_lossless_cast(ori_expr->get_param_expr(2), expr))) {
      LOG_WARN("failed to check is lossless column cast", K(ret));
    }
  } else if (OB_FAIL(is_lossless_column_cast(ori_expr, is_lossless))) {
    LOG_WARN("failed to check is lossless column cast", K(ret));
  } else if (is_lossless) {
    expr = ori_expr->get_param_expr(0);
  }
  return ret;
}

int ObOptimizerUtil::get_expr_without_lossless_cast(ObRawExpr* ori_expr,
                                                    ObRawExpr*& expr)
{
  int ret = OB_SUCCESS;
  bool is_lossless = false;
  expr = ori_expr;
  if (OB_ISNULL(ori_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(is_lossless_column_cast(ori_expr, is_lossless))) {
    LOG_WARN("failed to check is lossless column cast", K(ret));
  } else if (is_lossless) {
    expr = ori_expr->get_param_expr(0);
  }
  return ret;
}

int ObOptimizerUtil::gen_set_target_list(ObIAllocator *allocator,
                                         ObSQLSessionInfo *session_info,
                                         ObRawExprFactory *expr_factory,
                                         ObSelectStmt &left_stmt,
                                         ObSelectStmt &right_stmt,
                                         ObSelectStmt *select_stmt,
                                         const bool is_mysql_recursive_union /* false */,
                                         ObIArray<ObString> *rcte_col_name /* null */)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObSelectStmt*, 1> left_stmts;
  ObSEArray<ObSelectStmt*, 1> right_stmts;
  if (OB_FAIL(left_stmts.push_back(&left_stmt)) || OB_FAIL(right_stmts.push_back(&right_stmt))) {
    LOG_WARN("failed to pushback stmt", K(ret));
  } else if (OB_FAIL(gen_set_target_list(allocator, session_info, expr_factory,
                                         left_stmts, right_stmts, select_stmt, is_mysql_recursive_union,
                                         rcte_col_name))) {
    LOG_WARN("failed to get set target list", K(ret));
  }
  return ret;
}

int ObOptimizerUtil::gen_set_target_list(ObIAllocator *allocator,
                                         ObSQLSessionInfo *session_info,
                                         ObRawExprFactory *expr_factory,
                                         ObIArray<ObSelectStmt*> &left_stmts,
                                         ObIArray<ObSelectStmt*> &right_stmts,
                                         ObSelectStmt *select_stmt,
                                         const bool is_mysql_recursive_union /* false */,
                                         ObIArray<ObString> *rcte_col_name /* null */)
{
  int ret = OB_SUCCESS;
  UNUSED(allocator);
  ObSelectStmt *child_stmt = NULL;
  ObSEArray<ObExprResType, 8> res_types;
  if (OB_ISNULL(session_info) || OB_ISNULL(expr_factory) || OB_ISNULL(select_stmt)) {
    ret = OB_NOT_INIT;
    LOG_WARN("params is invalid", K(ret));
  } else if (OB_FAIL(try_add_cast_to_set_child_list(allocator, session_info, expr_factory,
                                                    select_stmt->is_set_distinct(),
                                                    left_stmts, right_stmts, &res_types,
                                                    is_mysql_recursive_union, rcte_col_name))) {
    LOG_WARN("failed to try add cast to set child list", K(ret));
  } else if (OB_ISNULL(child_stmt = select_stmt->get_set_query(0))
             || OB_UNLIKELY(res_types.count() != child_stmt->get_select_item_size())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected set stmt", K(ret), K(child_stmt));
  } else {
    select_stmt->get_select_items().reset();
    ObItemType set_op_type = static_cast<ObItemType>(T_OP_SET + select_stmt->get_set_op());
    const int64_t num = child_stmt->get_select_item_size();
    SelectItem new_select_item;
    for (int64_t i = 0; OB_SUCC(ret) && i < num; i++) {
      SelectItem &select_item = child_stmt->get_select_item(i);
      new_select_item.alias_name_ = select_item.alias_name_;
      new_select_item.expr_name_ = select_item.expr_name_;
      new_select_item.is_real_alias_ = select_item.is_real_alias_ ||
                  ObRawExprUtils::is_column_ref_skip_implicit_cast(select_item.expr_);
      new_select_item.questions_pos_ = select_item.questions_pos_;
      new_select_item.params_idx_ = select_item.params_idx_;
      new_select_item.neg_param_idx_ = select_item.neg_param_idx_;
      new_select_item.esc_str_flag_ = select_item.esc_str_flag_;
      new_select_item.is_unpivot_mocked_column_ = select_item.is_unpivot_mocked_column_;
      new_select_item.paramed_alias_name_ = select_item.paramed_alias_name_;
      new_select_item.need_check_dup_name_ = select_item.need_check_dup_name_;
      if (OB_FAIL(ObRawExprUtils::make_set_op_expr(*expr_factory, i, set_op_type,
                                                   res_types.at(i), session_info,
                                                   new_select_item.expr_))) {
        LOG_WARN("create set op expr failed", K(ret));
      } else if (OB_FAIL(select_stmt->add_select_item(new_select_item))) {
        LOG_WARN("push back set select item failed", K(ret));
      } else if (OB_ISNULL(new_select_item.expr_) ||
                 OB_UNLIKELY(!new_select_item.expr_->is_set_op_expr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null or is not set op expr", "set op", PC(new_select_item.expr_));
      }
    }
  }
  return ret;
}

// link.zt why this function has two very similar versions
int ObOptimizerUtil::gen_set_target_list(ObIAllocator *allocator,
                                         ObSQLSessionInfo *session_info,
                                         ObRawExprFactory *expr_factory,
                                         ObSelectStmt *select_stmt)
{
  int ret = OB_SUCCESS;
  UNUSED(allocator);
  ObSelectStmt *child_stmt = NULL;
  ObSEArray<ObExprResType, 8> res_types;
  if (OB_ISNULL(session_info) || OB_ISNULL(expr_factory) || OB_ISNULL(select_stmt)
      || OB_UNLIKELY(select_stmt->get_set_query().empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret));
  } else if (OB_FAIL(get_set_res_types(allocator, session_info, select_stmt->get_set_query(),
                                       res_types))) {
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
      if (OB_FAIL(add_cast_to_set_list(session_info, expr_factory, select_stmt->get_set_query(),
                                       res_types.at(i), i))) {
        LOG_WARN("failed to add add cast to set list", K(ret));
      } else {
        SelectItem &select_item = child_stmt->get_select_item(i);
        new_select_item.alias_name_ = select_item.alias_name_;
        new_select_item.expr_name_ = select_item.expr_name_;
        new_select_item.is_real_alias_ = select_item.is_real_alias_ ||
                    ObRawExprUtils::is_column_ref_skip_implicit_cast(select_item.expr_);
        new_select_item.questions_pos_ = select_item.questions_pos_;
        new_select_item.params_idx_ = select_item.params_idx_;
        new_select_item.neg_param_idx_ = select_item.neg_param_idx_;
        new_select_item.esc_str_flag_ = select_item.esc_str_flag_;
        new_select_item.is_unpivot_mocked_column_ = select_item.is_unpivot_mocked_column_;
        new_select_item.paramed_alias_name_ = select_item.paramed_alias_name_;
        new_select_item.need_check_dup_name_ = select_item.need_check_dup_name_;
        if (OB_FAIL(ObRawExprUtils::make_set_op_expr(*expr_factory, i, set_op_type,
                                                     res_types.at(i), session_info,
                                                     new_select_item.expr_))) {
          LOG_WARN("create set op expr failed", K(ret));
        } else if (OB_FAIL(select_stmt->add_select_item(new_select_item))) {
          LOG_WARN("push back set select item failed", K(ret));
        } else if (OB_ISNULL(new_select_item.expr_) ||
                   OB_UNLIKELY(!new_select_item.expr_->is_set_op_expr())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expr is null or is not set op expr", "set op", PC(new_select_item.expr_));
        }
      }
    }
  }
  return ret;
}

int ObOptimizerUtil::get_set_res_types(ObIAllocator *allocator,
                                       ObSQLSessionInfo *session_info,
                                       ObIArray<ObSelectStmt*> &child_querys,
                                       ObIArray<ObExprResType> &res_types)
{
  int ret = OB_SUCCESS;
  res_types.reuse();
  ObSelectStmt *select_stmt = NULL;
  ObCollationType coll_type = CS_TYPE_INVALID;
  if (OB_ISNULL(allocator) || OB_ISNULL(session_info) || OB_UNLIKELY(child_querys.empty())
      || OB_ISNULL(select_stmt = child_querys.at(0))) {
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
      bool all_types_is_decint = true;
      for (int64_t i = 0; OB_SUCC(ret) && all_types_is_decint && i < types.count(); ++i) {
        if (!types.at(i).is_decimal_int()) {
          all_types_is_decint = false;
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_UNLIKELY(types.empty())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected empty", K(ret), K(types.empty()));
      } else if (1 == types.count() || all_types_is_decint) {
        ret = res_types.push_back(types.at(0));
      } else if (OB_FAIL(dummy_op.aggregate_result_type_for_merge(res_type, &types.at(0),
                                                    types.count(), coll_type, is_oracle_mode(),
                                                    length_semantics))) {
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

int ObOptimizerUtil::try_add_cast_to_set_child_list(ObIAllocator *allocator,
                                                    ObSQLSessionInfo *session_info,
                                                    ObRawExprFactory *expr_factory,
                                                    const bool is_distinct,
                                                    ObIArray<ObSelectStmt*> &left_stmts,
                                                    ObIArray<ObSelectStmt*> &right_stmts,
                                                    ObIArray<ObExprResType> *res_types,
                                                    const bool is_mysql_recursive_union /* false */,
                                                    ObIArray<ObString> *rcte_col_name /* null */)
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
    LOG_WARN("The used SELECT statements have a different number of columns",
                                        K(left_types.count()),  K(right_types.count()));
  } else {
    if (NULL != res_types) {
      res_types->reuse();
    }
    const int64_t num = left_types.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < num; i++) {
      res_type.reset();
      ObExprResType &left_type = left_types.at(i);
      ObExprResType &right_type = right_types.at(i);
      if (!left_type.has_result_flag(NOT_NULL_FLAG) || !right_type.has_result_flag(NOT_NULL_FLAG)) {
        left_type.unset_result_flag(NOT_NULL_FLAG);
        right_type.unset_result_flag(NOT_NULL_FLAG);
      }
      if (left_type != right_type || ob_is_enumset_tc(right_type.get_type()) || is_mysql_recursive_union) {
        ObSEArray<ObExprResType, 2> types;
        ObExprVersion dummy_op(*allocator);
        ObCollationType coll_type = CS_TYPE_INVALID;
        bool skip_add_cast = false;
        if (lib::is_oracle_mode()) {
          /*
          * Oracle has more strict constraints for data types used in set operator
          * https://docs.oracle.com/cd/B19306_01/server.102/b14200/queries004.htm
          */
          // need to refine this when more data type are added in oracle mode
          if (!((left_type.is_null() && !right_type.is_lob() && !right_type.is_lob_locator())
                || (left_type.is_null() && (right_type.is_lob() || right_type.is_lob_locator()) && !is_distinct)
                || (right_type.is_null() && !left_type.is_lob() && !left_type.is_lob_locator())
                || (right_type.is_null() && (left_type.is_lob() || left_type.is_lob_locator()) && !is_distinct)
                || (left_type.is_raw() && right_type.is_raw())
                || (left_type.is_character_type() && right_type.is_character_type())
                || (ob_is_oracle_numeric_type(left_type.get_type())
                    && ob_is_oracle_numeric_type(right_type.get_type()))
                || (ob_is_oracle_temporal_type(left_type.get_type())
                    && (ob_is_oracle_temporal_type(right_type.get_type())))
                || (left_type.is_urowid() && right_type.is_urowid())
                || (is_oracle_mode() && left_type.is_lob() && right_type.is_lob() && left_type.get_collation_type() == right_type.get_collation_type())
                || (is_oracle_mode() && left_type.is_geometry() && right_type.is_geometry())
                || (is_oracle_mode() && left_type.is_lob_locator() && right_type.is_lob_locator() && left_type.get_collation_type() == right_type.get_collation_type())
                || (is_oracle_mode() && (ob_is_user_defined_sql_type(left_type.get_type()) || ob_is_user_defined_pl_type(left_type.get_type()))
                                     && (ob_is_user_defined_sql_type(right_type.get_type()) || ob_is_user_defined_pl_type(right_type.get_type()))))) {
                // || (left_type.is_lob() && right_type.is_lob() && !is_distinct))) {
                // Originally, cases like "select clob from t union all select blob from t" return error
            if (session_info->is_varparams_sql_prepare()) {
              skip_add_cast = true;
              LOG_WARN("ps prepare stage expression has different datatype", K(i), K(left_type), K(right_type));
            } else {
              ret = OB_ERR_EXP_NEED_SAME_DATATYPE;
              LOG_WARN("expression must have same datatype as corresponding expression", K(ret),
              K(session_info->is_varparams_sql_prepare()), K(right_type.is_varchar_or_char()),
              K(i), K(left_type), K(right_type));
            }
          } else if (left_type.is_character_type()
                    && right_type.is_character_type()
                    && (left_type.is_varchar_or_char() != right_type.is_varchar_or_char())) {
            ret = OB_ERR_CHARACTER_SET_MISMATCH;
            LOG_WARN("character set mismatch", K(ret), K(left_type), K(right_type));
          } else if (left_type.is_string_or_lob_locator_type() &&
                    right_type.is_string_or_lob_locator_type()) {
            ObCharsetType left_cs = left_type.get_charset_type();
            ObCharsetType right_cs = right_type.get_charset_type();
            if (left_cs != right_cs) {
              if (CHARSET_UTF8MB4 == left_cs || CHARSET_UTF8MB4 == right_cs) {
                //sys table column exist utf8 varchar types, let it go
              } else {
                ret = OB_ERR_COLLATION_MISMATCH; //ORA-12704
                LOG_WARN("character set mismatch", K(ret), K(left_cs), K(right_cs));
              }
            }
          } else if (lib::is_oracle_mode() && is_distinct
                     && (right_type.is_geometry() || left_type.is_geometry())) {
            ret = OB_ERR_COMPARE_VARRAY_LOB_ATTR;
            LOG_WARN("column type incompatible", K(ret), K(left_type), K(right_type));
          }
          LOG_DEBUG("data type check for each select item in set operator", K(left_type),
                                                                            K(right_type));
        }
        const ObLengthSemantics length_semantics = session_info->get_actual_nls_length_semantics();
        if (OB_FAIL(ret)) {
        } else if (is_mysql_recursive_union) {
          if (left_type.is_null()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected rcte type", K(ret), K(left_stmts));
          } else {
            res_type = left_type;
          }
        } else if (skip_add_cast) {
          res_type = left_type;
        } else if (OB_FAIL(types.push_back(left_type)) || OB_FAIL(types.push_back(right_type))) {
          LOG_WARN("failed to push back", K(ret));
        } else if (OB_FAIL(session_info->get_collation_connection(coll_type))) {
          LOG_WARN("failed to get collation connection", K(ret));
        } else if (OB_FAIL(dummy_op.aggregate_result_type_for_merge(res_type, &types.at(0), 2,
                            coll_type, is_oracle_mode(), length_semantics))) {
          if (session_info->is_varparams_sql_prepare()) {
            skip_add_cast = true;
            res_type = left_type;
            LOG_WARN("failed to deduce type in ps prepare stage", K(types));
          } else {
            LOG_WARN("failed to aggregate result type for merge", K(ret));
          }
        }
        if (OB_FAIL(ret) || skip_add_cast) {
        } else if (ObMaxType == res_type.get_type()) {
          ret = OB_ERR_INVALID_TYPE_FOR_OP;
          LOG_WARN("column type incompatible", K(left_type), K(right_type));
        } else if ((left_type.is_ext() && !right_type.is_ext())
                    || (!left_type.is_ext() && right_type.is_ext())
                    || (left_type.is_ext() && right_type.is_ext()
                          && left_type.get_udt_id() != right_type.get_udt_id())) {
          ret = OB_ERR_EXP_NEED_SAME_DATATYPE;
          LOG_WARN("expression must have same datatype as corresponding expression", K(ret), K(left_type), K(right_type));
        } else if (left_type != res_type && OB_FAIL(add_cast_to_set_list(session_info,
                                                        expr_factory, left_stmts, res_type, i))) {
          LOG_WARN("failed to add add cast to set list", K(ret));
        } else if (!is_mysql_recursive_union &&
                   right_type != res_type && OB_FAIL(add_cast_to_set_list(session_info,
                                                        expr_factory, right_stmts, res_type, i))) {
          LOG_WARN("failed to add add cast to set list", K(ret));
        } else if (is_mysql_recursive_union &&
                   OB_FAIL(add_column_conv_to_set_list(session_info, expr_factory, right_stmts,
                                                       res_type, i, rcte_col_name))) {
          LOG_WARN("failed to add column_conv to set list", K(ret));
        }
      } else if (lib::is_oracle_mode() && is_distinct &&
                (left_type.is_lob() || left_type.is_lob_locator())) {
        ret = OB_ERR_INVALID_TYPE_FOR_OP;
        LOG_WARN("column type incompatible", K(ret), K(left_type), K(right_type));
      } else if (lib::is_oracle_mode() && is_distinct && right_type.is_json()) {
        ret = OB_ERR_INVALID_CMP_OP;
        LOG_WARN("column type incompatible", K(ret), K(left_type), K(right_type));
      } else if (lib::is_oracle_mode() && is_distinct && right_type.is_geometry()) {
        ret = OB_ERR_COMPARE_VARRAY_LOB_ATTR;
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

int ObOptimizerUtil::add_cast_to_set_list(ObSQLSessionInfo *session_info,
                                          ObRawExprFactory *expr_factory,
                                          ObIArray<ObSelectStmt*> &stmts,
                                          const ObExprResType &res_type,
                                          const int64_t idx)
{
  int ret = OB_SUCCESS;
  ObRawExpr *src_expr = NULL;
  ObRawExpr *new_expr = NULL;
  ObSelectStmt *stmt = NULL;
  const int64_t num = stmts.count();
  if (OB_ISNULL(session_info) || OB_ISNULL(expr_factory)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < num; ++i) {
    if (OB_ISNULL(stmt = stmts.at(i)) || idx >= stmt->get_select_item_size()
        || OB_ISNULL(src_expr = stmt->get_select_item(idx).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected stmt", K(ret), K(stmts.at(i)));
    } else if (ob_is_enumset_tc(src_expr->get_result_type().get_type())) {
      ObSysFunRawExpr *to_str_expr = NULL;
      if (src_expr->get_result_type() == res_type) {
        /*do nothing*/
      } else if (OB_FAIL(ObRawExprUtils::create_type_to_str_expr(*expr_factory,
                                                                 src_expr,
                                                                 to_str_expr,
                                                                 session_info,
                                                                 true))) {
        LOG_WARN("create to str expr for stmt failed", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::try_add_cast_expr_above(expr_factory, session_info,
                                                                *to_str_expr, res_type,
                                                                new_expr))) {
        LOG_WARN("create cast expr for stmt failed", K(ret));
      } else if (OB_FAIL(new_expr->add_flag(IS_INNER_ADDED_EXPR))) {
        LOG_WARN("failed to add flag", K(ret));
      } else {
        stmt->get_select_item(idx).expr_ = new_expr;
      }
    } else if (OB_FAIL(ObRawExprUtils::try_add_cast_expr_above(expr_factory, session_info,
                                                               *src_expr, res_type,
                                                               new_expr))) {
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

int ObOptimizerUtil::add_column_conv_to_set_list(ObSQLSessionInfo *session_info,
                                                 ObRawExprFactory *expr_factory,
                                                 ObIArray<ObSelectStmt*> &stmts,
                                                 const ObExprResType &res_type,
                                                 const int64_t idx,
                                                 ObIArray<ObString> *rcte_col_name)
{
  int ret = OB_SUCCESS;
  ObRawExpr *src_expr = NULL;
  ObRawExpr *new_expr = NULL;
  ObSelectStmt *stmt = NULL;
  const int64_t num = stmts.count();
  if (OB_ISNULL(session_info) || OB_ISNULL(expr_factory)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < num; ++i) {
    if (OB_ISNULL(stmt = stmts.at(i)) || idx >= stmt->get_select_item_size()
        || OB_ISNULL(src_expr = stmt->get_select_item(idx).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected stmt", K(ret), K(stmts.at(i)));
    } else if (OB_FAIL(ObRawExprUtils::build_column_conv_expr(session_info,
                                                              *expr_factory,
                                                              res_type.get_type(),
                                                              res_type.get_collation_type(),
                                                              res_type.get_accuracy().get_accuracy(),
                                                              true,
                                                              NULL != rcte_col_name ? &rcte_col_name->at(idx) : NULL,
                                                              NULL,
                                                              stmt->get_select_item(idx).expr_))) {
      LOG_WARN("failed to build column conv expr", K(ret));
    }
  }
  return ret;
}

/**
 * check_subquery_has_ref_assign_user_var
 * 检查expr的子查询中是否包含涉及到赋值操作的用户变量
 */
int ObOptimizerUtil::check_subquery_has_ref_assign_user_var(ObRawExpr *expr, bool &is_has)
{
  int ret = OB_SUCCESS;
  is_has = false;
  ObSEArray<ObQueryRefRawExpr *, 4> subqueries;
  ObQueryRefRawExpr *subquery = NULL;
  const ObDMLStmt *stmt = NULL;
  if (OB_FAIL(ObTransformUtils::extract_query_ref_expr(expr, subqueries))) {
    LOG_WARN("failed to extract query ref expr", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !is_has && i < subqueries.count(); ++i) {
      if (OB_ISNULL(subquery = subqueries.at(i)) ||
          OB_ISNULL(stmt = subquery->get_ref_stmt())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(subquery), K(stmt));
      } else if (OB_FAIL(stmt->has_ref_assign_user_var(is_has))) {
        LOG_WARN("failed to check stmt has ref assign user var", K(ret));
      }
    }
  }
  return ret;
}

int ObOptimizerUtil::pushdown_filter_into_subquery(const ObDMLStmt &parent_stmt,
                                                   const ObSelectStmt &subquery,
                                                   ObOptimizerContext &opt_ctx,
                                                   ObIArray<ObRawExpr*> &pushdown_filters,
                                                   ObIArray<ObRawExpr*> &candi_filters,
                                                   ObIArray<ObRawExpr*> &remain_filters,
                                                   bool &can_pushdown,
                                                   bool check_match_index/* = true*/)
{
  int ret = OB_SUCCESS;
  bool has_rownum = false;
  bool has_rollup = false;
  can_pushdown = false;
  if (subquery.is_set_stmt()) {
    if (subquery.is_recursive_union() || subquery.has_limit()) {
      //do nothing
    } else if (OB_FAIL(check_pushdown_filter(parent_stmt,
                                             subquery,
                                             opt_ctx,
                                             pushdown_filters,
                                             candi_filters,
                                             remain_filters,
                                             check_match_index))) {
      LOG_WARN("failed to check pushdown filter", K(ret));
    } else if (candi_filters.empty()) {
      //do thing
    } else {
      can_pushdown = true;
    }
  } else if (subquery.is_hierarchical_query()) {
    //can not pushdown do nothing
  } else if (0 == subquery.get_from_item_size()) {
    //expr value plan can not pushdown
  } else {
    has_rollup = subquery.has_rollup();
    if (OB_FAIL(subquery.has_rownum(has_rownum))) {
      LOG_WARN("failed to check stmt has rownum", K(ret));
    } else if (subquery.has_limit() || subquery.has_sequence() ||
               subquery.is_contains_assignment() ||
               subquery.is_unpivot_select() ||
               subquery.is_dblink_stmt() ||
               has_rollup || has_rownum) {
      //can not pushdown do nothing
    } else if (OB_FAIL(check_pushdown_filter(parent_stmt,
                                             subquery,
                                             opt_ctx,
                                             pushdown_filters,
                                             candi_filters,
                                             remain_filters,
                                             check_match_index))) {
      LOG_WARN("failed to check pushdown filter", K(ret));
    } else if (candi_filters.empty()) {
      //do thing
    } else {
      can_pushdown = true;
    }
  }
  if (OB_SUCC(ret)) {
    if (can_pushdown) {
      if (OB_FAIL(remove_special_exprs(candi_filters, remain_filters))) {
        LOG_WARN("failed to remove special exprs", K(ret));
      }
    } else {
      if (OB_FAIL(remain_filters.assign(pushdown_filters))) {
        LOG_WARN("failed to assign exprs", K(ret));
      }
    }
  }
  return ret;
}

int ObOptimizerUtil::check_pushdown_filter(const ObDMLStmt &parent_stmt,
                                           const ObSelectStmt &subquery,
                                           ObOptimizerContext &opt_ctx,
                                           ObIArray<ObRawExpr*> &pushdown_filters,
                                           ObIArray<ObRawExpr*> &candi_filters,
                                           ObIArray<ObRawExpr*> &remain_filters,
                                           bool check_match_index/* = true*/)
{
  int ret = OB_SUCCESS;
  bool is_valid = false;
  ObSEArray<ObRawExpr *, 4> common_exprs;
  if (!parent_stmt.is_set_stmt() && subquery.is_set_stmt()) {
    //如果子查询是set stmt，只需要检查是否有下推的谓词overlap index
    if (check_match_index) {
      if (OB_FAIL(check_pushdown_filter_overlap_index(parent_stmt,
                                                      opt_ctx,
                                                      pushdown_filters,
                                                      candi_filters,
                                                      remain_filters))) {
        LOG_WARN("failed to check pushdown filter overlap index", K(ret));
      }
    } else if (OB_FAIL(candi_filters.assign(pushdown_filters))) {
      LOG_WARN("failed to assign exprs", K(ret));
    }
  } else if (OB_FAIL(get_groupby_win_func_common_exprs(subquery,
                                                      common_exprs,
                                                      is_valid))) {
    LOG_WARN("failed to get common exprs", K(ret));
  } else if (is_valid && common_exprs.empty()) {
    //can not pushdown any filter
  } else if (parent_stmt.is_set_stmt()) {
    if (OB_FAIL(check_pushdown_filter_for_set(static_cast<const ObSelectStmt&>(parent_stmt),
                                              subquery,
                                              common_exprs,
                                              pushdown_filters,
                                              candi_filters,
                                              remain_filters))) {
      LOG_WARN("failed to check pushdown filter for set stmt", K(ret));
    }
  } else {
    if (OB_FAIL(check_pushdown_filter_for_subquery(parent_stmt,
                                                  subquery,
                                                  opt_ctx,
                                                  common_exprs,
                                                  pushdown_filters,
                                                  candi_filters,
                                                  remain_filters,
                                                  check_match_index))) {
      LOG_WARN("failed to check pushdown filter for subquery", K(ret));
    }
  }
  return ret;
}

int ObOptimizerUtil::remove_special_exprs(ObIArray<ObRawExpr*> &pushdown_filters,
                                          ObIArray<ObRawExpr*> &remain_filters)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 4> normal_filters;
  for (int64_t i = 0; OB_SUCC(ret) && i < pushdown_filters.count(); ++i) {
    if (OB_ISNULL(pushdown_filters.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("predicate is null", K(ret));
    } else if (ObPredicateDeduce::contain_special_expr(*pushdown_filters.at(i))) {
      if (OB_FAIL(remain_filters.push_back(pushdown_filters.at(i)))) {
        LOG_WARN("failed to push back expr", K(ret));
      }
    } else if (OB_FAIL(normal_filters.push_back(pushdown_filters.at(i)))) {
      LOG_WARN("failed to push back expr", K(ret));
    }
  }
  if (OB_SUCC(ret) &&
      OB_FAIL(pushdown_filters.assign(normal_filters))) {
    LOG_WARN("failed to assign filters", K(ret));
  }
  return ret;
}

int ObOptimizerUtil::check_pushdown_filter_overlap_index(const ObDMLStmt &stmt,
                                                         ObOptimizerContext &opt_ctx,
                                                         ObIArray<ObRawExpr*> &pushdown_filters,
                                                         ObIArray<ObRawExpr*> &candi_filters,
                                                         ObIArray<ObRawExpr*> &remain_filters)
{
  int ret = OB_SUCCESS;
  bool is_match_index = false;
  for (int64_t i = 0; OB_SUCC(ret) && !is_match_index && i < pushdown_filters.count(); ++i) {
    ObRawExpr *pred = NULL;
    ObSEArray<ObRawExpr *, 4> column_exprs;
    if (OB_ISNULL(pred = pushdown_filters.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("predicate is null", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(pred, column_exprs))) {
      LOG_WARN("failed to extract column exprs", K(ret));
    } else {
      for (int64_t j = 0; OB_SUCC(ret) && !is_match_index && j < column_exprs.count(); ++j) {
        ObRawExpr *expr = column_exprs.at(j);
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
    if (OB_FAIL(candi_filters.assign(pushdown_filters))) {
      LOG_WARN("failed to assign exprs", K(ret));
    }
  } else {
    if (OB_FAIL(remain_filters.assign(pushdown_filters))) {
      LOG_WARN("failed to assign exprs", K(ret));
    }
  }
  return ret;
}

int ObOptimizerUtil::check_pushdown_filter_for_set(const ObSelectStmt &parent_stmt,
                                                   const ObSelectStmt &subquery,
                                                   ObIArray<ObRawExpr*> &common_exprs,
                                                   ObIArray<ObRawExpr*> &pushdown_filters,
                                                   ObIArray<ObRawExpr*> &candi_filters,
                                                   ObIArray<ObRawExpr*> &remain_filters)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 4> view_column_exprs;
  ObSEArray<ObRawExpr *, 4> set_op_exprs;
  ObSEArray<ObRawExpr *, 4> select_exprs;
  for (int64_t i = 0; OB_SUCC(ret) && i < pushdown_filters.count(); ++i) {
    ObRawExpr *pred = NULL;
    bool is_simple_expr = true;
    bool pushed = false;
    set_op_exprs.reuse();
    select_exprs.reuse();
    view_column_exprs.reuse();
    if (OB_ISNULL(pred = pushdown_filters.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("predicate is null", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::extract_set_op_exprs(pred, set_op_exprs))) {
      LOG_WARN("failed to extract set op exprs", K(ret));
    } else if (OB_FAIL(ObTransformUtils::check_pushdown_into_set_valid(&subquery,
                                                                       pred,
                                                                       set_op_exprs,
                                                                       is_simple_expr))) {
      LOG_WARN("failed to check pushdown into set", K(ret));
    } else if (!is_simple_expr) {
      // can not push down
    } else if (OB_FAIL(ObTransformUtils::convert_set_op_expr_to_select_expr(set_op_exprs,
                                                                            subquery,
                                                                            select_exprs))) {
      LOG_WARN("failed to convert set op exprs to select exprs", K(ret));
    } else if (set_op_exprs.count() != select_exprs.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect set op expr count", K(ret));
    }
    for (int64_t j = 0; OB_SUCC(ret) && is_simple_expr && j < select_exprs.count(); ++j) {
      ObRawExpr *expr = select_exprs.at(j);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null expr", K(ret));
      } else if (ObPredicateDeduce::contain_special_expr(*expr)) {
        is_simple_expr = false;
      } else if (expr->has_flag(CNT_WINDOW_FUNC) ||
                  expr->has_flag(CNT_AGG) ||
                  expr->has_flag(CNT_SUB_QUERY) ||
                  expr->has_flag(CNT_ONETIME)) {
        is_simple_expr = false;
      } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(expr, view_column_exprs))) {
        LOG_WARN("failed to extract column exprs", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (!is_simple_expr) {
      //can not push down
    } else if (!common_exprs.empty() &&
                !subset_exprs(view_column_exprs, common_exprs)) {
      //common_exprs为空，说明既没有windown func，也没有group by
    } else if (OB_FAIL(candi_filters.push_back(pred))) {
      LOG_WARN("failed to push back predicate", K(ret));
    } else {
      pushed = true;
    }
    if (OB_SUCC(ret) && !pushed &&
        OB_FAIL(remain_filters.push_back(pred))) {
      LOG_WARN("failed to push back expr", K(ret));
    }
  }
  return ret;
}

int ObOptimizerUtil::check_pushdown_filter_for_subquery(const ObDMLStmt &parent_stmt,
                                                        const ObSelectStmt &subquery,
                                                        ObOptimizerContext &opt_ctx,
                                                        ObIArray<ObRawExpr*> &common_exprs,
                                                        ObIArray<ObRawExpr*> &pushdown_filters,
                                                        ObIArray<ObRawExpr*> &candi_filters,
                                                        ObIArray<ObRawExpr*> &remain_filters,
                                                        bool check_match_index/* = true*/)
{
  int ret = OB_SUCCESS;
  if (parent_stmt.is_set_stmt()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect set stmt here", K(ret));
  } else {
    //只要某一个能够下推的谓词match索引，那么所有可以下推的谓词都下推
    //否则清空所有可以下推的谓词
    bool is_match_index = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < pushdown_filters.count(); ++i) {
      ObRawExpr *pred = NULL;
      ObSEArray<ObRawExpr *, 4> column_exprs;
      ObSEArray<ObRawExpr *, 4> select_exprs;
      bool pushed = false;
      if (OB_ISNULL(pred = pushdown_filters.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("predicate is null", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(pred, column_exprs))) {
        LOG_WARN("failed to extract column exprs", K(ret));
      } else if (OB_FAIL(ObTransformUtils::convert_column_expr_to_select_expr(column_exprs,
                                                                              subquery,
                                                                              select_exprs))) {
        LOG_WARN("failed to convert column exprs to select exprs", K(ret));
      } else if (column_exprs.count() != select_exprs.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect column expr count", K(ret));
      } else {
        bool is_simple_expr = true;
        bool is_match = false;
        ObSEArray<ObRawExpr *, 4> view_column_exprs;
        for (int64_t j = 0; OB_SUCC(ret) && is_simple_expr && j < select_exprs.count(); ++j) {
          ObRawExpr *expr = select_exprs.at(j);
          if (OB_ISNULL(expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpect null expr", K(ret));
          } else if (ObPredicateDeduce::contain_special_expr(*expr)) {
            is_simple_expr = false;
          } else if (expr->has_flag(CNT_WINDOW_FUNC) ||
                     expr->has_flag(CNT_AGG) ||
                     expr->has_flag(CNT_SUB_QUERY)) {
            is_simple_expr = false;
          } else if (check_match_index &&
                     OB_FAIL(ObTransformUtils::check_column_match_index(opt_ctx.get_root_stmt(),
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
          //can not push down
        } else if (!common_exprs.empty() &&
                   !subset_exprs(view_column_exprs, common_exprs)) {
          //common_exprs为空，说明既没有windown func，也没有group by
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
      //do nothing
    } else if (check_match_index && !is_match_index) {
      candi_filters.reset();
      ret = remain_filters.assign(pushdown_filters);
    }
  }
  return ret;
}

int ObOptimizerUtil::get_groupby_win_func_common_exprs(const ObSelectStmt &subquery,
                                                       ObIArray<ObRawExpr*> &common_exprs,
                                                       bool &is_valid)
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
    //取所有win func的partition by表达式交集
    for (int64_t i = 0; OB_SUCC(ret) && i < subquery.get_window_func_count(); ++i) {
      const ObWinFunRawExpr *win_expr = NULL;
      if (OB_ISNULL(win_expr = subquery.get_window_func_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("window function expr is null", K(ret));
      } else if (i == 0) {
        if (OB_FAIL(common_exprs.assign(win_expr->get_partition_exprs()))) {
          LOG_WARN("failed to assign partition exprs", K(ret));
        }
      } else if (OB_FAIL(intersect_exprs(common_exprs,
                                         win_expr->get_partition_exprs(),
                                         common_exprs))) {
        LOG_WARN("failed to intersect expr array", K(ret));
      } else if (common_exprs.empty()) {
        break;
      }
    }
    //再与group by表达式取交集
    if (OB_FAIL(ret)) {
    } else if (has_group && !has_winfunc &&
              OB_FAIL(append(common_exprs, subquery.get_group_exprs()))) {
      LOG_WARN("failed to append exprs", K(ret));
    } else if (has_group && has_winfunc &&
              OB_FAIL(intersect_exprs(common_exprs,
                                      subquery.get_group_exprs(),
                                      common_exprs))) {
      LOG_WARN("failed to intersect expr array", K(ret));
    }
  }
  return ret;
}

int ObOptimizerUtil::rename_pushdown_filter(const ObDMLStmt &parent_stmt,
                                            const ObSelectStmt &subquery,
                                            int64_t table_id,
                                            ObSQLSessionInfo *session_info,
                                            ObRawExprFactory &expr_factory,
                                            ObIArray<ObRawExpr*> &candi_filters,
                                            ObIArray<ObRawExpr*> &rename_filters)
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
    ret = rename_subquery_pushdown_filter(parent_stmt,
                                          subquery,
                                          table_id,
                                          session_info,
                                          expr_factory,
                                          candi_filters,
                                          rename_filters);
  }
  return ret;
}

int ObOptimizerUtil::rename_set_op_pushdown_filter(const ObSelectStmt &parent_stmt,
                                                   const ObSelectStmt &subquery,
                                                   ObSQLSessionInfo *session_info,
                                                   ObRawExprFactory &expr_factory,
                                                   ObIArray<ObRawExpr*> &candi_filters,
                                                   ObIArray<ObRawExpr*> &rename_filters)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 4> child_select_list;
  ObSEArray<ObRawExpr *, 4> parent_set_exprs;
  ObRawExprCopier copier(expr_factory);
  if (OB_FAIL(subquery.get_select_exprs(child_select_list))) {
    LOG_WARN("get child stmt select exprs failed", K(ret));
  } else if (OB_FAIL(parent_stmt.get_pure_set_exprs(parent_set_exprs))) {
    LOG_WARN("failed to get expr in cast", K(ret));
  } else if (OB_FAIL(copier.add_replaced_expr(parent_set_exprs, child_select_list))) {
    LOG_WARN("failed to add exprs", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < candi_filters.count(); ++i) {
    ObRawExpr *pred = candi_filters.at(i);
    ObRawExpr *new_pred = NULL;
    // Given a valid stmt, we only need to call the copy
    // Given a stmt in transformation, maybe there a column which should be renamed later
    if (OB_FAIL(copier.copy_on_replace(pred, new_pred))) {
      LOG_WARN("failed to replace expr", K(ret));
    } else if (OB_FAIL(new_pred->formalize(session_info))) {
      LOG_WARN("failed to formalize expr", K(ret));
    } else if (OB_FAIL(new_pred->pull_relation_id())) {
      LOG_WARN("failed to pull relation id and levels", K(ret));
    } else if (OB_FAIL(rename_filters.push_back(new_pred))) {
      LOG_WARN("failed to push back renamed filter", K(ret));
    }
  }
  return ret;
}

int ObOptimizerUtil::rename_subquery_pushdown_filter(const ObDMLStmt &parent_stmt,
                                                     const ObSelectStmt &subquery,
                                                     int64_t table_id,
                                                     ObSQLSessionInfo *session_info,
                                                     ObRawExprFactory &expr_factory,
                                                     ObIArray<ObRawExpr*> &candi_filters,
                                                     ObIArray<ObRawExpr*> &rename_filters)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 4> view_select_list;
  ObSEArray<ObRawExpr *, 4> view_column_list;
  ObSEArray<ObColumnRefRawExpr *, 4> table_columns;
  ObRawExprCopier copier(expr_factory);
  if (parent_stmt.is_set_stmt()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect set stmt here", K(ret));
  } else if (OB_FAIL(parent_stmt.get_column_exprs(table_id, table_columns))) {
    LOG_WARN("failed to get column exprs", K(ret));
  } else if (OB_FAIL(append(view_column_list, table_columns))) {
    LOG_WARN("failed to append column exprs", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < table_columns.count(); ++i) {
    ObRawExpr *sel_expr = NULL;
    ObColumnRefRawExpr *col_expr = table_columns.at(i);
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
  if (OB_SUCC(ret)) {
    // Given a valid stmt, we only need call copy here
    // Given a stmt in transformation, we need call copy_on_replace
    // Because some columns may belongs to the upper stmt, and can not
    // be renamed at the present, it should be renamed later
    if (OB_FAIL(copier.add_replaced_expr(view_column_list, view_select_list))) {
      LOG_WARN("failed to add exprs", K(ret));
    } else if (OB_FAIL(copier.copy_on_replace(candi_filters, rename_filters))) {
      LOG_WARN("failed to copy on replace filters", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < rename_filters.count(); ++i) {
    if (OB_ISNULL(rename_filters.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is null", K(ret));
    } else if (OB_FAIL(rename_filters.at(i)->formalize(session_info))) {
      LOG_WARN("failed to formalize expr", K(ret));
    } else if (OB_FAIL(rename_filters.at(i)->pull_relation_id())) {
      LOG_WARN("failed to pull relation id and levels", K(ret));
    }
  }
  return ret;
}

int ObOptimizerUtil::get_set_op_remain_filter(const ObSelectStmt &stmt,
                                              const ObIArray<ObRawExpr *> &child_pushdown_preds,
                                              ObIArray<ObRawExpr *> &output_pushdown_preds,
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
    //对于未成功下推的谓词, 取并集反回上层
    for (int64_t i = 0; OB_SUCC(ret) && i < child_pushdown_preds.count(); ++i) {
      if (ObOptimizerUtil::find_equal_expr(output_pushdown_preds, child_pushdown_preds.at(i))) {
        /*do nothing*/
      } else if (OB_FAIL(output_pushdown_preds.push_back(child_pushdown_preds.at(i)))) {
        LOG_WARN("push back preds failed", K(ret));
      } else {/*do nothing*/}
    }
  } else if (stmt.get_set_op() == ObSelectStmt::INTERSECT) {
    //对于未成功下推的谓词，取交集返回上层
    ObSEArray<ObRawExpr*, 4> pushdown_preds;
    for (int64_t i = 0; OB_SUCC(ret) && i < child_pushdown_preds.count(); ++i) {
      if (!ObOptimizerUtil::find_equal_expr(output_pushdown_preds, child_pushdown_preds.at(i))) {
        /*do nothing*/
      } else if (OB_FAIL(pushdown_preds.push_back(child_pushdown_preds.at(i)))) {
        LOG_WARN("push back preds failed", K(ret));
      } else {/*do nothing*/}
    }
    if (OB_SUCC(ret) && OB_FAIL(output_pushdown_preds.assign(pushdown_preds))) {
      LOG_WARN("failed to assign exprs", K(ret));
    }
  } else {/*do nothing*/}
  return ret;
}

int ObOptimizerUtil::check_is_null_qual(const ParamStore *params,
                                        const ObDMLStmt *stmt,
                                        ObExecContext *exec_ctx,
                                        ObIAllocator &allocator,
                                        const ObRelIds &table_ids,
                                        const ObRawExpr* qual,
                                        bool &is_null_qual)
{
  int ret = OB_SUCCESS;
  is_null_qual = false;
  const ObRawExpr *left_expr = NULL;
  const ObRawExpr *right_expr = NULL;
  ObObj result;
  bool got_result = false;
  if (OB_ISNULL(params) || OB_ISNULL(stmt) || OB_ISNULL(qual)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(params), K(stmt), K(qual));
  } else if (T_OP_IS != qual->get_expr_type()) {
    //do nothing
  } else if (OB_ISNULL(left_expr = qual->get_param_expr(0)) ||
             OB_ISNULL(right_expr = qual->get_param_expr(1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Left and right expr should not be NULL", K(ret), K(left_expr), K(right_expr));
  } else if (!left_expr->is_column_ref_expr()) {
    //do nothing
  } else if (!ObOptEstUtils::is_calculable_expr(*right_expr, params->count())) {
    //do nothing
  } else if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(
                    exec_ctx,
                    right_expr,
                    result,
                    got_result,
                    allocator))) {
    LOG_WARN("Failed to calc const or calculable expr", K(ret));
  } else if (!got_result) {
    // do nothing
  } else if (result.is_null()) {
      if (table_ids.is_superset(left_expr->get_relation_ids())) {
        is_null_qual = true;
      }
  }
  return ret;
}

int ObOptimizerUtil::check_expr_contain_subquery(const ObIArray<ObRawExpr*> &exprs,
                                                 bool &has_subquery)
{
  int ret = OB_SUCCESS;
  has_subquery = false;
  for (int64_t i = 0; OB_SUCC(ret) && !has_subquery && i < exprs.count(); i++) {
    if (OB_ISNULL(exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else {
      has_subquery = exprs.at(i)->has_flag(CNT_SUB_QUERY) ||
                     exprs.at(i)->has_flag(CNT_ONETIME);
    }
  }
  return ret;
}

bool ObOptimizerUtil::has_psedu_column(const ObRawExpr &expr)
{
  return expr.has_flag(CNT_ROWNUM) ||
         expr.has_flag(CNT_SEQ_EXPR) ||
         expr.has_flag(CNT_LEVEL) ||
         expr.has_flag(CNT_CONNECT_BY_ISLEAF) ||
         expr.has_flag(CNT_CONNECT_BY_ISCYCLE);
}

bool ObOptimizerUtil::has_hierarchical_expr(const ObRawExpr &expr)
{
  return expr.has_flag(IS_PRIOR) ||
         expr.has_flag(CNT_PRIOR) ||
         expr.has_flag(CNT_CONNECT_BY_ROOT) ||
         expr.has_flag(CNT_SYS_CONNECT_BY_PATH) ||
         expr.has_flag(CNT_LEVEL) ||
         expr.has_flag(CNT_CONNECT_BY_ISLEAF) ||
         expr.has_flag(CNT_CONNECT_BY_ISCYCLE);
}

int ObOptimizerUtil::compute_ordering_relationship(const bool left_is_interesting,
                                                   const bool right_is_interesting,
                                                   const ObIArray<OrderItem> &left_ordering,
                                                   const ObIArray<OrderItem> &right_ordering,
                                                   const EqualSets &equal_sets,
                                                   const ObIArray<ObRawExpr*> &condition_exprs,
                                                   DominateRelation &relation)
{
  int ret = OB_SUCCESS;
  bool is_left_prefix = false;
  bool is_right_prefix = false;
  relation = DominateRelation::OBJ_UNCOMPARABLE;
  if (!left_is_interesting && !right_is_interesting) {
    relation = DominateRelation::OBJ_EQUAL;
  } else if (left_is_interesting && !right_is_interesting) {
    relation = DominateRelation::OBJ_LEFT_DOMINATE;
  } else if (!left_is_interesting && right_is_interesting) {
    relation = DominateRelation::OBJ_RIGHT_DOMINATE;
  } else if (OB_FAIL(ObOptimizerUtil::is_prefix_ordering(left_ordering,
                                                         right_ordering,
                                                         equal_sets,
                                                         condition_exprs,
                                                         is_left_prefix,
                                                         is_right_prefix))) {
    LOG_WARN("failed to compute prefix ordering relationship", K(left_ordering),
        K(right_ordering), K(ret));
  } else if (is_left_prefix && is_right_prefix) {
    relation = DominateRelation::OBJ_EQUAL;
  } else if (is_left_prefix) {
    relation = DominateRelation::OBJ_RIGHT_DOMINATE;
  } else if (is_right_prefix) {
    relation = DominateRelation::OBJ_LEFT_DOMINATE;
  } else {
    relation = DominateRelation::OBJ_UNCOMPARABLE;
  }
  return ret;
}

int ObOptimizerUtil::compute_sharding_relationship(const ObShardingInfo *left_strong_sharding,
                                                   const ObIArray<ObShardingInfo*> &left_weak_sharding,
                                                   const ObShardingInfo *right_strong_sharding,
                                                   const ObIArray<ObShardingInfo*> &right_weak_sharding,
                                                   const EqualSets &equal_sets,
                                                   DominateRelation &relation)
{
  int ret = OB_SUCCESS;
  DominateRelation strong_relation;
  DominateRelation weak_relation;
  relation = DominateRelation::OBJ_UNCOMPARABLE;
  if (OB_FAIL(compute_sharding_relationship(left_strong_sharding,
                                            right_strong_sharding,
                                            equal_sets,
                                            strong_relation))) {
    LOG_WARN("failed to compute sharding relationship", K(ret));
  } else if (OB_FAIL(compute_sharding_relationship(left_weak_sharding,
                                                   right_weak_sharding,
                                                   equal_sets,
                                                   weak_relation))) {
    LOG_WARN("failed to compute sharding relationship", K(ret));
  } else if (DominateRelation::OBJ_EQUAL == strong_relation &&
             DominateRelation::OBJ_EQUAL == weak_relation) {
    relation = DominateRelation::OBJ_EQUAL;
  } else if ((DominateRelation::OBJ_EQUAL == strong_relation ||
              DominateRelation::OBJ_LEFT_DOMINATE == strong_relation) &&
             (DominateRelation::OBJ_EQUAL == weak_relation ||
              DominateRelation::OBJ_LEFT_DOMINATE == weak_relation)) {
    relation = DominateRelation::OBJ_LEFT_DOMINATE;
  } else if ((DominateRelation::OBJ_EQUAL == strong_relation ||
              DominateRelation::OBJ_RIGHT_DOMINATE == strong_relation) &&
             (DominateRelation::OBJ_EQUAL == weak_relation ||
              DominateRelation::OBJ_RIGHT_DOMINATE == weak_relation)) {
    relation = DominateRelation::OBJ_RIGHT_DOMINATE;
  } else {
    relation = DominateRelation::OBJ_UNCOMPARABLE;
  }
  return ret;
}

int ObOptimizerUtil::compute_sharding_relationship(const ObIArray<ObShardingInfo*> &left_sharding,
                                                   const ObIArray<ObShardingInfo*> &right_sharding,
                                                   const EqualSets &equal_sets,
                                                   DominateRelation &relation)
{
  int ret = OB_SUCCESS;
  bool is_left_dominate = false;
  bool is_right_dominate = false;
  relation = DominateRelation::OBJ_UNCOMPARABLE;
  if (OB_FAIL(check_sharding_set_left_dominate(left_sharding,
                                               right_sharding,
                                               equal_sets,
                                               is_left_dominate))) {
    LOG_WARN("failed to check left dominate relationship", K(ret));
  } else if (OB_FAIL(check_sharding_set_left_dominate(right_sharding,
                                                      left_sharding,
                                                      equal_sets,
                                                      is_right_dominate))) {
    LOG_WARN("failed to check left dominate relationship", K(ret));
  } else if (is_left_dominate && is_right_dominate) {
    relation = DominateRelation::OBJ_EQUAL;
  } else if (is_left_dominate) {
    relation = DominateRelation::OBJ_LEFT_DOMINATE;
  } else if (is_right_dominate) {
    relation = DominateRelation::OBJ_RIGHT_DOMINATE;
  } else {
    relation = DominateRelation::OBJ_UNCOMPARABLE;
  }
  return ret;
}

int ObOptimizerUtil::compute_sharding_relationship(const ObShardingInfo *left_sharding,
                                                   const ObShardingInfo *right_sharding,
                                                   const EqualSets &equal_sets,
                                                   DominateRelation &relation)
{
  int ret = OB_SUCCESS;
  bool is_equal = false;
  relation = DominateRelation::OBJ_UNCOMPARABLE;
  if (OB_FAIL(ObShardingInfo::is_sharding_equal(left_sharding,
                                                right_sharding,
                                                equal_sets,
                                                is_equal))) {
    LOG_WARN("failed to check whether sharding info is equal", K(ret));
  } else if (is_equal) {
    relation = DominateRelation::OBJ_EQUAL;
  } else if (NULL == left_sharding && NULL != right_sharding) {
    relation = DominateRelation::OBJ_RIGHT_DOMINATE;
  } else if (NULL != left_sharding && NULL == right_sharding) {
    relation = DominateRelation::OBJ_LEFT_DOMINATE;
  } else if (left_sharding->is_distributed_with_partitioning() &&
             right_sharding->is_distributed_without_partitioning()) {
    relation = DominateRelation::OBJ_LEFT_DOMINATE;
  } else if (left_sharding->is_distributed_without_partitioning() &&
             right_sharding->is_distributed_with_partitioning()) {
    relation = DominateRelation::OBJ_RIGHT_DOMINATE;
  } else {
    relation = DominateRelation::OBJ_UNCOMPARABLE;
  }
  return ret;
}

int ObOptimizerUtil::check_sharding_set_left_dominate(const ObIArray<ObShardingInfo*> &left_sharding,
                                                      const ObIArray<ObShardingInfo*> &right_sharding,
                                                      const EqualSets &equal_sets,
                                                      bool &is_left_dominate)
{
  int ret = OB_SUCCESS;
  is_left_dominate = true;
  for (int64_t i = 0; OB_SUCC(ret) && is_left_dominate && i < right_sharding.count(); i++) {
    bool is_find = false;
    DominateRelation relation = DominateRelation::OBJ_UNCOMPARABLE;
    for (int64_t j = 0; OB_SUCC(ret) && !is_find && j < left_sharding.count(); j++) {
      if (OB_FAIL(compute_sharding_relationship(left_sharding.at(j),
                                                right_sharding.at(i),
                                                equal_sets,
                                                relation))) {
        LOG_WARN("failed to compute sharding relationship", K(ret));
      } else if (DominateRelation::OBJ_LEFT_DOMINATE == relation ||
                 DominateRelation::OBJ_EQUAL == relation) {
        is_find = true;
      } else { /*do nothing*/ }
    }
    if (OB_SUCC(ret) && !is_find) {
      is_left_dominate = false;
    }
  }
  return ret;
}

int ObOptimizerUtil::get_range_params(ObLogicalOperator *root,
                                      ObIArray<ObRawExpr*> &range_exprs,
                                      ObIArray<ObRawExpr*> &all_table_filters)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(root)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null logical operator", K(ret));
  } else if (log_op_def::LOG_TABLE_SCAN == root->get_type()) {
    ObLogTableScan *scan = static_cast<ObLogTableScan *>(root);
    const ObCostTableScanInfo *info = scan->get_est_cost_info();
    if (NULL != info && info->pushdown_prefix_filters_.count() > 0) {
      if (OB_FAIL(append(range_exprs, info->pushdown_prefix_filters_))) {
        LOG_WARN("failed to append range exprs", K(ret));
      } else if (OB_FAIL(append(all_table_filters, info->pushdown_prefix_filters_))) {
        LOG_WARN("failed to append pushdown prefix filters", K(ret));
      } else if (OB_FAIL(append(all_table_filters, info->postfix_filters_))) {
        LOG_WARN("failed to append pushdown prefix filters", K(ret));
      } else if (OB_FAIL(append(all_table_filters, info->table_filters_))) {
        LOG_WARN("failed to append pushdown prefix filters", K(ret));
      }
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < root->get_num_of_child(); ++i) {
      ObLogicalOperator *child = root->get_child(i);
      if (OB_FAIL(SMART_CALL(get_range_params(child, range_exprs, all_table_filters)))) {
        LOG_WARN("failed to get range params", K(ret));
      }
    }
  }
  return ret;
}

int ObOptimizerUtil::check_basic_sharding_info(const ObAddr &local_addr,
                                               const ObIArray<ObLogicalOperator *> &child_ops,
                                               bool &is_basic)
{
  int ret = OB_SUCCESS;
  bool is_remote = false;
  is_basic = false;
  if (OB_FAIL(check_basic_sharding_info(local_addr, child_ops, is_basic, is_remote))) {
    LOG_WARN("failed to check basic sharding info", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObOptimizerUtil::check_basic_sharding_info(const ObAddr &local_addr,
                                               const ObIArray<ObLogicalOperator *> &child_ops,
                                               bool &is_basic,
                                               bool &is_remote)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObShardingInfo*, 8> sharding_infos;
  is_basic = false;
  is_remote = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < child_ops.count(); i++) {
    if (OB_ISNULL(child_ops.at(i)) || OB_ISNULL(child_ops.at(i)->get_sharding())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(sharding_infos.push_back(child_ops.at(i)->get_sharding()))) {
      LOG_WARN("failed to push back sharding infos", K(ret));
    } else { /*do nothing*/ }
  }
  if (OB_FAIL(ret)) {
    /*do nothing*/
  } else if (OB_FAIL(check_basic_sharding_info(local_addr, sharding_infos, is_basic, is_remote))) {
    LOG_WARN("failed to check basic sharding info", K(ret));
  } else { /*do nothing*/ }

  return ret;
}

int ObOptimizerUtil::check_basic_sharding_info(const ObAddr &local_addr,
                                               const ObIArray<ObShardingInfo*> &input_shardings,
                                               bool &is_basic)
{
  int ret = OB_SUCCESS;
  bool is_remote = false;
  is_basic = false;
  if (OB_FAIL(check_basic_sharding_info(local_addr, input_shardings, is_basic, is_remote))) {
    LOG_WARN("failed to check basic sharding info", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObOptimizerUtil::check_basic_sharding_info(const ObAddr &local_addr,
                                               const ObIArray<ObShardingInfo*> &input_shardings,
                                               bool &is_basic,
                                               bool &is_remote)
{
  int ret = OB_SUCCESS;
  ObAddr temp_addr;
  ObAddr remote_addr;
  int64_t local_num = 0;
  int64_t remote_num = 0;
  int64_t match_all_num = 0;
  int64_t dup_table_num = 0;
  const ObShardingInfo *sharding = NULL;
  ObSEArray<ObAddr, 8> valid_addrs;
  ObSEArray<ObAddr, 8> intersect_addrs;
  ObSEArray<ObAddr, 8> candidate_addrs;
  int64_t input_size = input_shardings.count();
  is_basic = false;
  is_remote = false;
  if (input_size <= 1) {
   if (OB_ISNULL(sharding = input_shardings.at(0))) {
     ret = OB_ERR_UNEXPECTED;
     LOG_WARN("get unexpected null", K(ret));
   } else {
     is_basic = true;
     is_remote = sharding->is_remote();
   }
  } else {
    is_basic = true;
    for (int64_t i = 0; OB_SUCC(ret) && is_basic && i < input_size; i++) {
      if (NULL == (sharding = input_shardings.at(i))) {
        is_basic = false;
      } else if (sharding->get_can_reselect_replica()) {
        dup_table_num++;
        valid_addrs.reuse();
        if (OB_ISNULL(sharding->get_phy_table_location_info())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (OB_FAIL(get_duplicate_table_replica(*sharding->get_phy_table_location_info(), valid_addrs))) {
          LOG_WARN("failed to get duplicated table replica", K(ret));
        } else if (intersect_addrs.empty()) {
          if (OB_FAIL(intersect_addrs.assign(valid_addrs))) {
            LOG_WARN("failed to assign addrs", K(ret));
          } else { /*do nothing*/ }
        } else {
          if (OB_FAIL(intersect(valid_addrs, intersect_addrs, candidate_addrs))) {
            LOG_WARN("failed to intersect addrs", K(ret));
          } else if (candidate_addrs.empty()) {
            is_basic = false;
          } else if (OB_FAIL(intersect_addrs.assign(candidate_addrs))) {
            LOG_WARN("failed to assign addrs", K(ret));
          } else { /*do nothing*/ }
        }
      } else if (sharding->is_local()) {
        local_num++;
      } else if (sharding->is_remote()) {
        remote_num++;
        if (OB_FAIL(sharding->get_remote_addr(temp_addr))) {
          LOG_WARN("failed to get remote addr", K(ret));
        } else if (!remote_addr.is_valid()) {
          remote_addr = temp_addr;
        } else if (remote_addr != temp_addr) {
          is_basic = false;
        } else { /*do nothing*/}
      } else if (sharding->is_match_all()) {
        match_all_num++;
      } else if (sharding->is_distributed()) {
        is_basic = false;
      } else { /*do nothing*/
      }
    }
    if (OB_SUCC(ret) && is_basic) {
      if (local_num > 0 && (local_num + match_all_num + dup_table_num == input_size)) {
        if (dup_table_num == 0) {
          is_basic = true;
        } else if (find_item(intersect_addrs, local_addr)) {
          is_basic = true;
        } else {
          is_basic = false;
        }
      } else if (remote_num > 0 && (remote_num + match_all_num + dup_table_num == input_size)) {
        if (dup_table_num == 0) {
          is_basic = true;
          is_remote = true;
        } else if (find_item(intersect_addrs, remote_addr)) {
          is_basic = true;
          is_remote = true;
        } else {
          is_basic = false;
        }
      } else if (match_all_num + dup_table_num == input_size) {
        if (dup_table_num == 0) {
          is_basic = true;
        } else if (intersect_addrs.empty()) {
          is_basic = false;
        } else {
          is_basic = true;
        }
      } else {
        is_basic = false;
      }
    }
  }
  if (OB_SUCC(ret)) {
    LOG_TRACE("succeed to check basic sharding info", K(is_basic), K(is_remote));
  }
  return ret;
}

int ObOptimizerUtil::compute_basic_sharding_info(const ObAddr &local_addr,
                                                 const ObIArray<ObLogicalOperator *> &child_ops,
                                                 ObIAllocator &allocator,
                                                 ObShardingInfo *&result_sharding,
                                                 int64_t &inherit_sharding_index)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObShardingInfo*, 8> sharding_infos;
  result_sharding = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < child_ops.count(); i++) {
    if (OB_ISNULL(child_ops.at(i)) ||
        OB_ISNULL(child_ops.at(i)->get_strong_sharding())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(child_ops.at(i)), K(ret));
    } else if (OB_FAIL(sharding_infos.push_back(child_ops.at(i)->get_strong_sharding()))) {
      LOG_WARN("failed to push back sharding infos", K(ret));
    } else { /*do nothing*/ }
  }
  if (OB_FAIL(ret)) {
    /*do nothing*/
  } else if (OB_FAIL(compute_basic_sharding_info(local_addr,
                                                 sharding_infos,
                                                 allocator,
                                                 result_sharding,
                                                 inherit_sharding_index))) {
    LOG_WARN("failed to compute basic sharding info", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObOptimizerUtil::compute_basic_sharding_info(const ObAddr &local_addr,
                                                 const ObIArray<ObShardingInfo*> &input_shardings,
                                                 ObIAllocator &allocator,
                                                 ObShardingInfo *&result_sharding,
                                                 int64_t &inherit_sharding_index)
{
  int ret = OB_SUCCESS;
  result_sharding = NULL;
  inherit_sharding_index = 0;
  if (input_shardings.count() <= 1) {
    result_sharding = input_shardings.at(0);
    inherit_sharding_index = 0;
  } else {
    ObAddr basic_addr;
    bool has_duplicated = false;
    bool is_replicas_same = true;
    ObShardingInfo *sharding = NULL;
    ObSEArray<ObAddr, 8> valid_addrs;
    ObSEArray<ObAddr, 8> intersect_addrs;
    ObSEArray<ObAddr, 8> candidate_addrs;
    for (int64_t i = 0; OB_SUCC(ret) && i < input_shardings.count(); i++) {
      if (OB_ISNULL(sharding = input_shardings.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(sharding), K(ret));
      } else if (sharding->get_can_reselect_replica()) {
        has_duplicated = true;
        valid_addrs.reuse();
        if (OB_ISNULL(sharding->get_phy_table_location_info())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (OB_FAIL(get_duplicate_table_replica(*sharding->get_phy_table_location_info(),
                                                        valid_addrs))) {
          LOG_WARN("failed to get duplicated table replica", K(ret));
        } else if (intersect_addrs.empty()) {
          if (OB_FAIL(intersect_addrs.assign(valid_addrs))) {
            LOG_WARN("failed to assign addrs", K(ret));
          } else { /*do nothing*/ }
        } else {
          if (OB_FAIL(ObOptimizerUtil::intersect(valid_addrs, intersect_addrs, candidate_addrs))) {
            LOG_WARN("failed to intersect addrs", K(ret));
          } else if (OB_FALSE_IT(is_replicas_same = is_replicas_same &&
                                                    valid_addrs.count() == candidate_addrs.count() &&
                                                    valid_addrs.count() == intersect_addrs.count())) {
            // do nothing
          } else if (OB_FAIL(intersect_addrs.assign(candidate_addrs))) {
            LOG_WARN("failed to assign addrs", K(ret));
          } else { /*do nothing*/ }
        }
      } else if (result_sharding != NULL) {
        //do nothing
      } else if (sharding->is_local()) {
        basic_addr = local_addr;
        result_sharding = sharding;
        inherit_sharding_index = i;
      } else if (sharding->is_remote()) {
        if (OB_FAIL(sharding->get_remote_addr(basic_addr))) {
          LOG_WARN("failed to get remote addr", K(ret));
        } else {
          result_sharding = sharding;
          inherit_sharding_index = i;
        }
      } else { /*do nothing*/ }
    }
    if (OB_SUCC(ret)) {
      if (result_sharding == NULL) {
        if (has_duplicated) {
          if (OB_UNLIKELY(intersect_addrs.empty())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected error", K(ret));
          } else if (ObOptimizerUtil::find_item(intersect_addrs, local_addr)) {
            basic_addr = local_addr;
          } else {
            basic_addr = intersect_addrs.at(0);
          }
        } else {
          result_sharding = input_shardings.at(0);
          inherit_sharding_index = 0;
        }
      }
      if (OB_FAIL(ret)) {
        /*do nothing*/
      } else if (!has_duplicated) {
        /*do nothing*/
      } else if (NULL != result_sharding) {
        /*do nothing*/
      } else if (is_replicas_same) {
        for (int64_t i = 0; OB_SUCC(ret) && NULL == result_sharding && i < input_shardings.count(); i++) {
          if (OB_ISNULL(sharding = input_shardings.at(i))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null", K(ret));
          } else if (sharding->get_can_reselect_replica()) {
            result_sharding = sharding;
            inherit_sharding_index = i;
          }
        }
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && NULL == result_sharding && i < input_shardings.count(); i++) {
          if (OB_ISNULL(sharding = input_shardings.at(i))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null", K(ret));
          } else if (sharding->get_can_reselect_replica() &&
                     OB_FAIL(ObOptimizerUtil::compute_duplicate_table_sharding(local_addr,
                                                                               basic_addr,
                                                                               allocator,
                                                                               *input_shardings.at(i),
                                                                               intersect_addrs,
                                                                               result_sharding))) {
            LOG_WARN("failed to compute duplicate table sharding", K(ret));
          } else if (NULL != result_sharding) {
             inherit_sharding_index = i;
          }
        }
      }
    }
  }
  if (OB_SUCC(ret) && NULL != result_sharding) {
    LOG_TRACE("succeed to compute basic sharding info", K(*result_sharding), K(input_shardings));
  }
  return ret;
}

int ObOptimizerUtil::get_duplicate_table_replica(const ObCandiTableLoc &phy_table_loc,
                                                 ObIArray<ObAddr> &valid_addrs)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(1 != phy_table_loc.get_partition_cnt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected partition count", K(ret), K(phy_table_loc.get_partition_cnt()));
  } else {
    const ObCandiTabletLoc &phy_part_loc = phy_table_loc.get_phy_part_loc_info_list().at(0);
    const ObIArray<ObRoutePolicy::CandidateReplica> &replicas =
        phy_part_loc.get_partition_location().get_replica_locations();
    for (int64_t i = 0; OB_SUCC(ret) && i < replicas.count(); ++i) {
      if (OB_FAIL(valid_addrs.push_back(replicas.at(i).get_server()))) {
        LOG_WARN("failed to push back replica address", K(ret));
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObOptimizerUtil::compute_duplicate_table_sharding(const ObAddr &local_addr,
                                                      const ObAddr &selected_addr,
                                                      ObIAllocator &allocator,
                                                      ObShardingInfo &src_sharding,
                                                      ObIArray<ObAddr> &valid_addrs,
                                                      ObShardingInfo *&target_sharding)
{
  int ret = OB_SUCCESS;
  ObCandiTableLoc *phy_table_loc = NULL;
  int64_t replica_index = OB_INVALID_INDEX;
  target_sharding = NULL;
  if (OB_ISNULL(target_sharding = reinterpret_cast<ObShardingInfo*>(
                                  allocator.alloc(sizeof(ObShardingInfo))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else if (OB_FALSE_IT(target_sharding = new(target_sharding) ObShardingInfo())) {
  } else if (OB_FAIL(target_sharding->copy_with_part_keys(src_sharding))) {
    LOG_WARN("failed to copy sharding info", K(ret));
  } else if (OB_ISNULL(src_sharding.get_phy_table_location_info()) ||
              OB_UNLIKELY(1 != src_sharding.get_phy_table_location_info()->get_partition_cnt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected partition count", K(ret));
  } else if (OB_FAIL(generate_duplicate_table_replicas(allocator,
                                                        src_sharding.get_phy_table_location_info(),
                                                        valid_addrs,
                                                        phy_table_loc))) {
    LOG_WARN("failed to compute duplicate table location", K(ret));
  } else if (OB_ISNULL(phy_table_loc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret));
  } else if (OB_FALSE_IT(target_sharding->set_phy_table_location_info(phy_table_loc))) {
  } else if (OB_UNLIKELY(1 != target_sharding->get_phy_table_location_info()->get_partition_cnt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected partition count", K(ret));
  } else {
    int64_t dup_table_pos = OB_INVALID_INDEX;
    ObCandiTabletLoc &phy_part_loc =
          phy_table_loc->get_phy_part_loc_info_list_for_update().at(0);
    if (!phy_part_loc.is_server_in_replica(selected_addr, dup_table_pos)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("no server in replica", K(selected_addr), K(ret));
    } else {
      phy_part_loc.set_selected_replica_idx(dup_table_pos);
      if (local_addr == selected_addr) {
        target_sharding->set_local();
      } else {
        target_sharding->set_remote();
      }
    }
  }
  return ret;
}

int ObOptimizerUtil::generate_duplicate_table_replicas(ObIAllocator &allocator,
                                                       const ObCandiTableLoc *source_table_loc,
                                                       ObIArray<ObAddr> &valid_addrs,
                                                       ObCandiTableLoc *&target_table_loc)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(source_table_loc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_UNLIKELY(1 != source_table_loc->get_partition_cnt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected partition count", K(ret),
             K(source_table_loc->get_partition_cnt()));
  } else if (OB_ISNULL(target_table_loc = static_cast<ObCandiTableLoc*>(
                       allocator.alloc(sizeof(ObCandiTableLoc))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else if (OB_FALSE_IT(target_table_loc = new(target_table_loc) ObCandiTableLoc())) {
    // do nothing
  } else if (OB_FAIL(target_table_loc->assign(*source_table_loc))) {
    LOG_WARN("failed to assign table location", K(ret));
  } else {
    ObCandiTabletLoc &phy_part_loc =
          target_table_loc->get_phy_part_loc_info_list_for_update().at(0);
    ObOptTabletLoc &opt_tablet_loc = phy_part_loc.get_partition_location();
    ObIArray<ObRoutePolicy::CandidateReplica> &replica_loc_list = opt_tablet_loc.get_replica_locations();
    for (int64_t i = replica_loc_list.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
      if (ObOptimizerUtil::find_item(valid_addrs,
                                     replica_loc_list.at(i).get_server())) {
        // do nothing
      } else if (OB_FAIL(replica_loc_list.remove(i))) {
        LOG_WARN("failed to remove relica loc list", K(ret));
      }
    }
  }
  return ret;
}

int ObOptimizerUtil::check_pushdown_filter_to_base_table(ObLogPlan &plan,
                                                         const uint64_t table_id,
                                                         const ObIArray<ObRawExpr*> &pushdown_filters,
                                                         const ObIArray<ObRawExpr*> &restrict_infos,
                                                         bool &can_pushdown)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObColumnRefRawExpr*, 8> col_exprs;
  ObSEArray<ObColumnRefRawExpr*, 8> pushdown_col_exprs;
  const ObDMLStmt *stmt = plan.get_stmt();
  can_pushdown = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < restrict_infos.count(); ++i) {
    if (OB_FAIL(ObTransformUtils::get_simple_filter_column(stmt,
                                                           restrict_infos.at(i),
                                                           table_id,
                                                           col_exprs))) {
      LOG_WARN("failed to get simple filter column", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < pushdown_filters.count(); ++i) {
    if (OB_FAIL(ObTransformUtils::get_simple_filter_column(stmt,
                                                           pushdown_filters.at(i),
                                                           table_id,
                                                           pushdown_col_exprs))) {
      LOG_WARN("failed to get simple filter column", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && !can_pushdown && i < pushdown_col_exprs.count(); ++i) {
    ObColumnRefRawExpr *col_expr = pushdown_col_exprs.at(i);
    if (OB_FAIL(ObTransformUtils::is_match_index(plan.get_optimizer_context().get_sql_schema_guard(),
                                                 stmt,
                                                 col_expr,
                                                 can_pushdown,
                                                 NULL, NULL,
                                                 &col_exprs))) {
      LOG_WARN("failed to check is match index", K(ret));
    }
  }
  LOG_TRACE("check pushdown filter to tables", K(table_id), K(can_pushdown));
  return ret;
}

int64_t ObOptimizerUtil::get_join_style_parallel(const int64_t left_parallel,
                                                 const int64_t right_parallel,
                                                 const DistAlgo join_dist_algo,
                                                 const bool use_left /* default false */)
{
  int64_t parallel = ObGlobalHint::UNSET_PARALLEL;
  if (DistAlgo::DIST_BASIC_METHOD == join_dist_algo ||
             DistAlgo::DIST_PULL_TO_LOCAL == join_dist_algo) {
    parallel = ObGlobalHint::DEFAULT_PARALLEL;
  } else if (DistAlgo::DIST_HASH_HASH == join_dist_algo ||
             DistAlgo::DIST_PARTITION_WISE == join_dist_algo ||
             DistAlgo::DIST_EXT_PARTITION_WISE == join_dist_algo) {
    parallel = (use_left || (left_parallel > right_parallel)) ? left_parallel : right_parallel;
  } else if (DistAlgo::DIST_BROADCAST_NONE == join_dist_algo ||
             DistAlgo::DIST_BC2HOST_NONE == join_dist_algo ||
             DistAlgo::DIST_HASH_NONE == join_dist_algo ||
             DistAlgo::DIST_PARTITION_NONE == join_dist_algo) {
    parallel = right_parallel;
  } else {
    parallel = left_parallel;
  }
  return parallel;
}

bool ObOptimizerUtil::is_left_need_exchange(const ObShardingInfo &sharding,
                                            const DistAlgo dist_algo)
{
  return dist_algo == DIST_HASH_HASH ||
         dist_algo == DIST_HASH_NONE ||
         dist_algo == DIST_BROADCAST_NONE ||
         dist_algo == DIST_BC2HOST_NONE ||
         dist_algo == DIST_PARTITION_NONE ||
         (dist_algo == DIST_PULL_TO_LOCAL && sharding.is_sharding());
}

bool ObOptimizerUtil::is_right_need_exchange(const ObShardingInfo &sharding,
                                             const DistAlgo dist_algo)
{
  return dist_algo == DIST_HASH_HASH ||
         dist_algo == DIST_NONE_BROADCAST ||
         dist_algo == DIST_NONE_PARTITION ||
         dist_algo == DIST_NONE_HASH ||
         (dist_algo == DIST_PULL_TO_LOCAL && sharding.is_sharding());
}

ObPQDistributeMethod::Type ObOptimizerUtil::get_left_dist_method(const ObShardingInfo &sharding,
                                                                 const DistAlgo dist_algo)
{
  ObPQDistributeMethod::Type dist_method = ObPQDistributeMethod::NONE;
  if (DistAlgo::DIST_HASH_HASH == dist_algo) {
    dist_method = ObPQDistributeMethod::HASH;
  } else if (DistAlgo::DIST_BROADCAST_NONE == dist_algo) {
    dist_method = ObPQDistributeMethod::BROADCAST;
  } else if (DistAlgo::DIST_BC2HOST_NONE == dist_algo) {
    dist_method = ObPQDistributeMethod::BC2HOST;
  } else if (DistAlgo::DIST_PARTITION_NONE == dist_algo) {
    dist_method = ObPQDistributeMethod::PARTITION;
  } else if (DistAlgo::DIST_HASH_NONE == dist_algo) {
    dist_method = ObPQDistributeMethod::HASH;
  } else if (DistAlgo::DIST_PULL_TO_LOCAL == dist_algo &&
             sharding.is_sharding()) {
    dist_method = ObPQDistributeMethod::LOCAL;
  } else {
    dist_method = ObPQDistributeMethod::NONE;
  }
  return dist_method;
}

ObPQDistributeMethod::Type ObOptimizerUtil::get_right_dist_method(const ObShardingInfo &sharding,
                                                                  const DistAlgo dist_algo)
{
  ObPQDistributeMethod::Type dist_method = ObPQDistributeMethod::NONE;
  if (DistAlgo::DIST_HASH_HASH == dist_algo) {
    dist_method = ObPQDistributeMethod::HASH;
  } else if (DistAlgo::DIST_NONE_BROADCAST == dist_algo) {
    dist_method = ObPQDistributeMethod::BROADCAST;
  } else if (DistAlgo::DIST_NONE_PARTITION == dist_algo) {
    dist_method = ObPQDistributeMethod::PARTITION;
  } else if (DistAlgo::DIST_NONE_HASH == dist_algo) {
    dist_method = ObPQDistributeMethod::HASH;
  } else if (DistAlgo::DIST_PULL_TO_LOCAL == dist_algo && sharding.is_sharding()) {
    dist_method = ObPQDistributeMethod::LOCAL;
  } else {
    dist_method = ObPQDistributeMethod::NONE;
  }
  return dist_method;
}

int ObOptimizerUtil::generate_pullup_aggr_expr(ObRawExprFactory &expr_factory,
                                               ObSQLSessionInfo *session_info,
                                               ObAggFunRawExpr *origin_expr,
                                               ObAggFunRawExpr *&pullup_aggr)
{
  int ret = OB_SUCCESS;
  pullup_aggr = NULL;
  if (OB_ISNULL(origin_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(origin_expr));
  } else if (T_FUN_MAX == origin_expr->get_expr_type() ||
             T_FUN_MIN == origin_expr->get_expr_type() ||
             T_FUN_SUM == origin_expr->get_expr_type() ||
             T_FUN_COUNT == origin_expr->get_expr_type() ||
             T_FUN_COUNT_SUM == origin_expr->get_expr_type() ||
             T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS == origin_expr->get_expr_type() ||
             T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS_MERGE == origin_expr->get_expr_type() ||
             T_FUN_SYS_BIT_AND == origin_expr->get_expr_type() ||
             T_FUN_SYS_BIT_OR == origin_expr->get_expr_type() ||
             T_FUN_SYS_BIT_XOR == origin_expr->get_expr_type() ||
             T_FUN_SUM_OPNSIZE == origin_expr->get_expr_type()) {
    /* MAX(a) -> MAX(MAX(a)), MIN(a) -> MIN(MIN(a)) SUM(a) -> SUM(SUM(a)) */
    ObItemType pullup_aggr_type = origin_expr->get_expr_type();
    if (T_FUN_COUNT == pullup_aggr_type || T_FUN_SUM_OPNSIZE == pullup_aggr_type) {
      pullup_aggr_type = T_FUN_COUNT_SUM;
    } else if (T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS == pullup_aggr_type) {
      pullup_aggr_type = T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS_MERGE;
    }

    if (OB_FAIL(ObRawExprUtils::build_common_aggr_expr(expr_factory,
                                                       session_info,
                                                       pullup_aggr_type,
                                                       origin_expr,
                                                       pullup_aggr))) {
      LOG_WARN("failed to build common aggr expr", K(ret));
    }
  } else if (T_FUN_GROUPING == origin_expr->get_expr_type() &&
             origin_expr->get_real_param_count() == 1) {
    ObRawExpr *param_expr = origin_expr->get_real_param_exprs_for_update().at(0);
    if (OB_FAIL(ObRawExprUtils::build_common_aggr_expr(expr_factory,
                                                       session_info,
                                                       T_FUN_GROUPING,
                                                       param_expr,
                                                       pullup_aggr))) {
      LOG_WARN("failed to pullup grouping aggr expr", K(ret));
    }
  } else if (T_FUN_TOP_FRE_HIST == origin_expr->get_expr_type()) {
    if (OB_UNLIKELY(4 != origin_expr->get_real_param_count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("real param count is invalid", K(ret));
    } else if (OB_FAIL(expr_factory.create_raw_expr(T_FUN_TOP_FRE_HIST, pullup_aggr))) {
      LOG_WARN("failed to create top frequency expr", K(ret));
    } else if (OB_FAIL(pullup_aggr->add_real_param_expr(
                         origin_expr->get_real_param_exprs_for_update().at(0)))) {
      LOG_WARN("failed to add real param expr", K(ret));
    } else if (OB_FAIL(pullup_aggr->add_real_param_expr(
                         origin_expr))) {
      LOG_WARN("failed to add real param expr", K(ret));
    } else if (OB_FAIL(pullup_aggr->add_real_param_expr(
                         origin_expr->get_real_param_exprs_for_update().at(2)))) {
      LOG_WARN("failed to add real param expr", K(ret));
    } else if (OB_FAIL(pullup_aggr->add_real_param_expr(
                         origin_expr->get_real_param_exprs_for_update().at(3)))) {
      LOG_WARN("failed to add real param expr", K(ret));
    } else if (FALSE_IT(pullup_aggr->set_is_need_deserialize_row(true))) {
      // do nothing
    } else if (OB_FAIL(pullup_aggr->formalize(session_info))) {
      LOG_WARN("failed to formalize top fequence expr", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected aggr type", K(ret), K(*origin_expr));
  }
  return ret;
}

int ObOptimizerUtil::check_filter_before_indexback(const ObIArray<ObRawExpr*> &filter_exprs,
                                                   const ObIArray<uint64_t> &index_columns,
                                                   ObIArray<bool> &filter_before_index_back)
{
  int ret = OB_SUCCESS;
  bool contains = false;
  ObRawExpr *expr = NULL;
  ObSEArray<uint64_t, 8> filter_ids;
  for (int64_t i = 0; OB_SUCC(ret) && i < filter_exprs.count(); i++) {
    filter_ids.reuse();
    if (OB_ISNULL(expr = filter_exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(check_contain_ora_rowscn_expr(expr, contains))) {
      LOG_WARN("failed to check contains ora rowscan expr", K(ret));
    } else if (contains) {
      if (OB_FAIL(filter_before_index_back.push_back(false))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else { /*do nothing*/ }
    } else if (expr->has_flag(CNT_MATCH_EXPR)) {
      if (OB_FAIL(filter_before_index_back.push_back(false))) {
        LOG_WARN("failed to push back expr", K(ret));
      }
    } else if (OB_FAIL(ObRawExprUtils::extract_column_ids(expr, filter_ids))) {
      LOG_WARN("failed to extract column ids", K(ret));
    } else {
      contains = ObOptimizerUtil::is_subset(filter_ids, index_columns);
      if (OB_FAIL(filter_before_index_back.push_back(contains))) {
        LOG_WARN("failed to push back element", K(ret));
      } else { /*do nothjing*/ }
    }
  }
  return ret;
}

int ObOptimizerUtil::generate_rowkey_expr(ObDMLStmt *stmt,
                                          ObRawExprFactory &expr_factory,
                                          const uint64_t &table_id,
                                          const ObColumnSchemaV2 &column_schema,
                                          ObColumnRefRawExpr *&rowkey,
                                          ObIArray<ColumnItem> *column_items)
{
  int ret = OB_SUCCESS;
  /* Now, let's create the raw expr */
  const TableItem *table_item = NULL;
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
    dummy_col_item.is_geo_ = column_schema.is_geometry();
    if (OB_FAIL(stmt->add_column_item(dummy_col_item))) {
      LOG_WARN("add column item to stmt failed", K(ret));
    } else if (FALSE_IT(rowkey->clear_explicited_referece())) {
      /*do nothing*/
    } else if (OB_FAIL(rowkey->formalize(NULL))) {
      LOG_WARN("formalize rowkey failed", K(ret));
    } else if (OB_FAIL(rowkey->pull_relation_id())) {
      LOG_WARN("failed to pullup relation ids", K(ret));
    } else if (NULL != column_items) {
      if (OB_FAIL(column_items->push_back(dummy_col_item))) {
        LOG_WARN("Failed to add dummy column item", K(ret));
      }
    } else { }//do nothing
  }
  return ret;
}


int ObOptimizerUtil::check_contain_ora_rowscn_expr(const ObRawExpr *expr,
                                                   bool &contains)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  contains = false;
  if (OB_ISNULL(expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid raw expr", K(expr), K(ret));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("check stack overflow failed", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret));
  } else if (T_ORA_ROWSCN == expr->get_expr_type()) {
    contains = true;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !contains && i < expr->get_param_count(); i++) {
      if (OB_FAIL(check_contain_ora_rowscn_expr(expr->get_param_expr(i), contains))) {
        LOG_WARN("failed to contain ora_rowscn expr", K(ret));
      } else { /*do nothing*/}
    }
  }
  return ret;
}

int ObOptimizerUtil::allocate_group_id_expr(ObLogPlan *log_plan, ObRawExpr *&group_id_expr)
{
  int ret = OB_SUCCESS;
  ObOptimizerContext *opt_ctx = NULL;
  ObOpPseudoColumnRawExpr *tmp_group_id_expr = NULL;
  group_id_expr = NULL;
  if (OB_ISNULL(log_plan) || OB_ISNULL(opt_ctx = &log_plan->get_optimizer_context())
      || OB_ISNULL(opt_ctx->get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Get unexpected null", K(ret), K(log_plan), K(opt_ctx));
  } else {
    ObExprResType res_type;
    res_type.set_type(ObIntType);
    res_type.set_accuracy(ObAccuracy::MAX_ACCURACY[ObIntType]);
    if (OB_FAIL(ObRawExprUtils::build_op_pseudo_column_expr(opt_ctx->get_expr_factory(),
                                                            T_PSEUDO_GROUP_ID,
                                                            "GROUP_ID",
                                                            res_type,
                                                            tmp_group_id_expr))) {
    } else if (OB_ISNULL(tmp_group_id_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("group expr is null", K(ret));
    } else if (OB_FAIL(tmp_group_id_expr->formalize(opt_ctx->get_session_info()))) {
      LOG_WARN("group expr formalize failed", K(ret));
    } else {
      group_id_expr = tmp_group_id_expr;
    }
  }
  return ret;
}

int ObOptimizerUtil::allocate_identify_seq_expr(ObOptimizerContext &opt_ctx, ObRawExpr *&identify_seq_expr)
{
  int ret = OB_SUCCESS;
  ObOpPseudoColumnRawExpr *tmp_identify_seq_expr = NULL;
  identify_seq_expr = NULL;
  if (OB_ISNULL(opt_ctx.get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Get unexpected null", K(ret), K(opt_ctx.get_session_info()));
  } else {
    ObExprResType res_type;
    res_type.set_type(ObUInt64Type);
    res_type.set_accuracy(ObAccuracy::MAX_ACCURACY[ObUInt64Type]);
    if (OB_FAIL(ObRawExprUtils::build_op_pseudo_column_expr(opt_ctx.get_expr_factory(),
                                                            T_PSEUDO_IDENTIFY_SEQ,
                                                            "IDENTIFY_SEQ",
                                                            res_type,
                                                            tmp_identify_seq_expr))) {
    } else if (OB_ISNULL(tmp_identify_seq_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("identify seq expr is null", K(ret));
    } else if (OB_FAIL(tmp_identify_seq_expr->formalize(opt_ctx.get_session_info()))) {
      LOG_WARN("identify seq expr formalize failed", K(ret));
    } else {
      identify_seq_expr = tmp_identify_seq_expr;
    }
  }
  return ret;
}

int ObOptimizerUtil::check_contribute_query_range(ObLogicalOperator *root,
                                                  const ObIArray<ObExecParamRawExpr *> &params,
                                                  bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  ObSEArray<ObRawExpr*, 32> range_exprs;
  ObSEArray<ObRawExpr*, 32> all_table_filters;
  ObSEArray<ObRawExpr*, 32> range_params;
  ObSEArray<ObRawExpr*, 8> pushdown_params;
  ObSEArray<ObRawExpr*, 8> exec_params;
  if (OB_ISNULL(root)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid root", K(ret));
  } else if (params.empty()) {
    // do nothing
  } else if (OB_FAIL(append(exec_params, params))) {
    LOG_WARN("failed to append nl params", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::get_range_params(root, range_exprs, all_table_filters))) {
    LOG_WARN("failed to get range_exprs", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::extract_params(range_exprs, range_params))) {
    LOG_WARN("failed to extract range params", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::intersect(exec_params, range_params, pushdown_params))) {
    LOG_WARN("failed to get intersect params", K(ret));
  } else if (pushdown_params.empty()) {
    // do nothing
  } else {
    is_valid = true;
    LOG_TRACE("pushdown filters can extend query range", K(pushdown_params));
  }
  return ret;
}

int ObOptimizerUtil::check_pushdown_range_cond(ObLogicalOperator *root,
                                               bool &cnt_pd_range_cond)
{
  int ret = OB_SUCCESS;
  cnt_pd_range_cond = false;
  if (OB_ISNULL(root)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid root", K(ret));
  } else if (root->is_table_scan()) {
    ObLogTableScan *tsc = static_cast<ObLogTableScan*>(root);
    if (OB_FAIL(check_exec_param_filter_exprs(tsc->get_range_conditions(),
                                              cnt_pd_range_cond))) {
      LOG_WARN("failed to check exec param filter", K(ret));
    } else {/*do nothing*/}
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !cnt_pd_range_cond && i < root->get_num_of_child(); ++i) {
      ObLogicalOperator *child = root->get_child(i);
      if (OB_ISNULL(child)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid child", K(ret));
      } else if (OB_FAIL(SMART_CALL(check_pushdown_range_cond(child, cnt_pd_range_cond)))) {
        LOG_WARN("failed to check pd range cond", K(ret));
      } else {/*do nothing*/}
    }
  }
  return ret;
}

int ObOptimizerUtil::check_exec_param_filter_exprs(const ObIArray<ObRawExpr *> &input_filters,
                                                   bool &has_exec_param_filters)
{
  int ret = OB_SUCCESS;
  has_exec_param_filters = false;
  for (int i = 0; OB_SUCC(ret) && !has_exec_param_filters && i < input_filters.count(); ++i) {
    ObRawExpr *expr = input_filters.at(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null expr", K(ret));
    } else if (expr->has_flag(CNT_DYNAMIC_PARAM)) {
      has_exec_param_filters = true;
    }
  }
  return ret;
}

int ObOptimizerUtil::check_contain_batch_stmt_parameter(ObRawExpr* expr, bool &contain)
{
  int ret = OB_SUCCESS;
  contain = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null expr", K(ret));
  } else if (!expr->has_flag(CNT_CONST)) {
    //do nothing
  } else if (expr->is_const_raw_expr()) {
    ObConstRawExpr *const_expr = static_cast<ObConstRawExpr*>(expr);
    contain = const_expr->is_batch_stmt_parameter();
  } else {
    for (int i = 0; !contain && OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(check_contain_batch_stmt_parameter(expr->get_param_expr(i), contain)))) {
        LOG_WARN("failed to check contain batch stmt parameter", K(ret));
      }
    }
  }
  return ret;
}

/* Check whether src_expr can be calculated by const exprs and dst_exprs */
int ObOptimizerUtil::expr_calculable_by_exprs(ObRawExpr *src_expr,
                                              const ObIArray<ObRawExpr*> &dst_exprs,
                                              const bool need_check_contain,
                                              const bool used_in_compare,
                                              bool &is_calculable)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 2> parent_exprs;
  if (OB_FAIL(expr_calculable_by_exprs(src_expr, dst_exprs, parent_exprs,
                                       need_check_contain, used_in_compare, is_calculable))) {
    LOG_WARN("fail to check expr is calculable by other exprs", K(ret));
  }
  return ret;
}


int ObOptimizerUtil::expr_calculable_by_exprs(ObRawExpr *src_expr,
                                              const ObIArray<ObRawExpr*> &dst_exprs,
                                              ObIArray<ObRawExpr*> &parent_exprs,
                                              const bool need_check_contain,
                                              const bool used_in_compare,
                                              bool &is_calculable)
{
  int ret = OB_SUCCESS;
  bool can_replace = false;
  bool is_const_inherit = false;
  is_calculable = true;
  if (OB_ISNULL(src_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL", K(ret));
  } else if (src_expr->is_const_expr()) {
    // is calculable
  } else if (dst_exprs.empty()) {
    is_calculable = false;
  } else if (OB_FAIL(ObTransformUtils::check_can_replace(src_expr, parent_exprs,
                                                         used_in_compare, can_replace))) {
    LOG_WARN("failed to check can replace expr", K(ret));
  } else if (!can_replace) {
    is_calculable = false;
  } else if (need_check_contain && ObOptimizerUtil::find_item(dst_exprs, src_expr)) {
    // is calculable
  } else if (OB_FAIL(src_expr->is_const_inherit_expr(is_const_inherit, true))) {
    LOG_WARN("failed to check is const inherit expr", K(ret));
  } else if (!is_const_inherit) {
    is_calculable = false;
  } else {
    if (OB_FAIL(parent_exprs.push_back(src_expr))) {
      LOG_WARN("failed to push back", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && is_calculable && i < src_expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(expr_calculable_by_exprs(src_expr->get_param_expr(i),
                                                      dst_exprs,
                                                      parent_exprs,
                                                      true,
                                                      used_in_compare,
                                                      is_calculable)))) {
        LOG_WARN("failed to smart call expr_calculable_by_exprs", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      parent_exprs.pop_back();
    }
  }
  return ret;
}

int ObOptimizerUtil::check_contain_my_exec_param(const ObRawExpr* expr, const common::ObIArray<ObExecParamRawExpr*> & my_exec_params, bool &contain)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  contain = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null expr", K(ret));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("check stack overflow failed", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret));
  } else if (!expr->has_flag(CNT_DYNAMIC_PARAM)) {
    //do nothing
  } else if (expr->is_exec_param_expr()) {
    const ObExecParamRawExpr *exec_expr = static_cast<const ObExecParamRawExpr*>(expr);
    contain = find_exec_param(my_exec_params, exec_expr);
  } else if (expr->is_set_op_expr() || expr->is_query_ref_expr() || expr->is_column_ref_expr()) {
    //do nothing
  } else {
    for (int64_t i = 0; !contain && OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(check_contain_my_exec_param(expr->get_param_expr(i), my_exec_params, contain)))) {
        LOG_WARN("failed to check contain batch stmt parameter", K(ret));
      }
    }
  }
  return ret;
}

/* get the smallest set from which all exprs can be evaluated */
int ObOptimizerUtil::get_minset_of_exprs(const ObIArray<ObRawExpr *> &src_exprs, ObIArray<ObRawExpr *> &min_set) {
  int ret = OB_SUCCESS;
  //find expr which can not be evaluated by other exprs
  for (int i = 0; OB_SUCC(ret) && i < src_exprs.count(); ++i) {
    ObRawExpr *expr = src_exprs.at(i);
    bool is_calculable = false;
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret), K(expr));
    } else if (OB_FAIL(expr_calculable_by_exprs(expr, src_exprs, false, true, is_calculable))) {
      LOG_WARN("fail to check expr is calculable by other exprs", K(ret));
    } else if (is_calculable) {
      // do nothing
    } else if (OB_FAIL(min_set.push_back(expr))) {
      LOG_WARN("fail to push back expr", K(ret));
    }
  }
  return ret;
}

int ObOptimizerUtil::check_can_encode_sortkey(const common::ObIArray<OrderItem> &order_keys,
                                                bool &can_sort_opt,
                                                ObLogPlan& plan,
                                                double card)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 4> sort_keys;
  double avg_len = 0;
  bool has_hint = false;
  can_sort_opt = true;
  bool old_can_opt = false;
  const ObOptParamHint opt_params = plan.get_optimizer_context().get_global_hint().opt_params_;
  if (OB_FAIL(opt_params.has_opt_param(ObOptParamHint::ENABLE_NEWSORT, has_hint))) {
    LOG_WARN("failed to check whether has hint param", K(ret));
  } else if (has_hint) {
    if (OB_FAIL(opt_params.get_bool_opt_param(ObOptParamHint::ENABLE_NEWSORT, can_sort_opt))) {
      LOG_WARN("failed to get bool hint param", K(ret));
    }
  } else {
    can_sort_opt &= GCONF._enable_newsort;

    for (int64_t i = 0; OB_SUCC(ret) && can_sort_opt && i < order_keys.count(); i++) {
      if (!ObOrderPerservingEncoder::can_encode_sortkey(
                            order_keys.at(i).expr_->get_data_type(),
                            order_keys.at(i).expr_->get_collation_type())) {
        can_sort_opt = false;
      } else if (OB_FAIL(sort_keys.push_back(order_keys.at(i).expr_))) {
        LOG_WARN("failed to add sort key expr", K(ret));
      } else { /* do nothing */ }
    }
    old_can_opt = can_sort_opt;

    // add width / row size policy, EN_ENABLE_NEWSORT_FORCE tracepoint will skip it
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (!can_sort_opt) {
      // do nothing
    } else if (OB_FAIL(ObOptEstCost::estimate_width_for_exprs(plan.get_basic_table_metas(),
                                                              plan.get_selectivity_ctx(),
                                                              sort_keys,
                                                              avg_len))) {
      LOG_WARN("failed to estimate width for output join column exprs", K(ret));
    } else if (avg_len > 256) {
      can_sort_opt = false;
    } else if (avg_len < 64 && card < 100000) {
      can_sort_opt = false;
    } else if (avg_len > 64 && avg_len < 128 && card < 1500000 ) {
      can_sort_opt = false;
    } else {
      // do nothing
    }

    int tmp_ret = OB_SUCCESS;
    tmp_ret = OB_E(EventTable::EN_ENABLE_NEWSORT_FORCE) OB_SUCCESS;
    if (OB_SUCCESS != tmp_ret) {
      can_sort_opt = old_can_opt;
    }
    LOG_TRACE("check encode sortkey", K(can_sort_opt), K(order_keys), K(card), K(avg_len),
              K(old_can_opt));
  }
  return ret;
}

int ObOptimizerUtil::build_rel_ids_by_equal_set(const EqualSet& equal_set,
                                                ObRelIds& rel_ids)
{
  int ret = OB_SUCCESS;
  for (int64_t j = 0; OB_SUCC(ret) && j < equal_set.count(); j++) {
    ObRawExpr *expr = equal_set.at(j);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if (!expr->is_column_ref_expr()) {
      // do nothing
    } else if (OB_FAIL(rel_ids.add_members(expr->get_relation_ids()))) {
      LOG_WARN("failed to add member", K(ret));
    }
  }
  return ret;
}

int ObOptimizerUtil::extract_equal_join_conditions(const ObIArray<ObRawExpr *> &equal_join_conditions,
                                                   const ObRelIds &left_tables,
                                                   ObIArray<ObRawExpr *> &left_exprs,
                                                   ObIArray<ObRawExpr *> &right_exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t j = 0; OB_SUCC(ret) && j < equal_join_conditions.count(); ++j) {
    ObRawExpr* expr = equal_join_conditions.at(j);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL expr", K(ret));
    } else if (!expr->has_flag(IS_JOIN_COND) ||
                2 != expr->get_param_count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected join condition", K(ret));
    } else {
      ObRawExpr *lexpr = expr->get_param_expr(0);
      ObRawExpr *rexpr = expr->get_param_expr(1);
      if (OB_ISNULL(lexpr) || OB_ISNULL(rexpr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null expr", K(ret));
      } else if (lexpr->get_relation_ids().is_subset(left_tables)) {
        if (OB_FAIL(left_exprs.push_back(lexpr))) {
          LOG_WARN("failed to push back expr", K(ret));
        } else if (OB_FAIL(right_exprs.push_back(rexpr))) {
          LOG_WARN("failed to push back expr", K(ret));
        }
      } else if (rexpr->get_relation_ids().is_subset(left_tables)) {
        if (OB_FAIL(left_exprs.push_back(rexpr))) {
          LOG_WARN("failed to push back expr", K(ret));
        } else if (OB_FAIL(right_exprs.push_back(lexpr))) {
          LOG_WARN("failed to push back expr", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObOptimizerUtil::build_rel_ids_by_equal_sets(const EqualSets& equal_sets,
                                                 ObIArray<ObRelIds>& rel_ids_array)
{
  int ret = OB_SUCCESS;
  rel_ids_array.reuse();
  for (int64_t i = 0; OB_SUCC(ret) && i < equal_sets.count(); i++) {
    EqualSet *equal_set = equal_sets.at(i);
    ObRelIds rel_ids;
    if (OB_ISNULL(equal_set)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if (OB_FAIL(build_rel_ids_by_equal_set(*equal_set, rel_ids))) {
      LOG_WARN("failed to build rel ids", K(ret));
    } else if (OB_FAIL(rel_ids_array.push_back(rel_ids))) {
      LOG_WARN("failed to push back", K(ret));
      }
  }
  return ret;
}

int ObOptimizerUtil::extract_pushdown_join_filter_quals(const ObIArray<ObRawExpr *> &left_quals,
                                                        const ObIArray<ObRawExpr *> &right_quals,
                                                        const ObSqlBitSet<> &right_tables,
                                                        ObIArray<ObRawExpr *> &pushdown_left_quals,
                                                        ObIArray<ObRawExpr *> &pushdown_right_quals)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(left_quals.count() != right_quals.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("quals count unexpected", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < right_quals.count(); ++i) {
    ObRawExpr *left_qual = left_quals.at(i);
    ObRawExpr *right_qual = right_quals.at(i);
    if (OB_ISNULL(left_qual) ||
        OB_ISNULL(right_qual)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexplected null", K(left_qual), K(right_qual), K(ret));
    // can not push down expr with subquery
    } else if (right_qual->has_flag(CNT_PSEUDO_COLUMN) ||
               right_qual->has_flag(CNT_PRIOR) ||
               right_qual->has_flag(CNT_ROWNUM)) {
      /* do noting */
    } else if (!right_qual->get_relation_ids().is_subset(right_tables)) {
      /* do noting */
    } else if (OB_FAIL(pushdown_right_quals.push_back(right_qual))) {
      LOG_WARN("failed to push back qual", K(ret));
    } else if (OB_FAIL(pushdown_left_quals.push_back(left_qual))) {
      LOG_WARN("failed to push back qual", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObOptimizerUtil::pushdown_join_filter_into_subquery(const ObDMLStmt &parent_stmt,
                                                        const ObSelectStmt &subquery,
                                                        const ObIArray<ObRawExpr*> &pushdown_left_quals,
                                                        const ObIArray<ObRawExpr*> &pushdown_right_quals,
                                                        ObIArray<ObRawExpr*> &candi_left_quals,
                                                        ObIArray<ObRawExpr*> &candi_right_quals,
                                                        bool &can_pushdown)
{
  int ret = OB_SUCCESS;
  bool has_rownum = false;
  bool has_rollup = false;
  can_pushdown = false;
  if (subquery.is_set_stmt()) {
    if (subquery.is_recursive_union() || subquery.has_limit()) {
      //do nothing
    } else if (OB_FAIL(check_pushdown_join_filter_quals(parent_stmt,
                                                        subquery,
                                                        pushdown_left_quals,
                                                        pushdown_right_quals,
                                                        candi_left_quals,
                                                        candi_right_quals))) {
      LOG_WARN("failed to check pushdown filter", K(ret));
    } else if (candi_right_quals.empty()) {
      //do thing
    } else {
      can_pushdown = true;
    }
  } else if (subquery.is_hierarchical_query()) {
    //can not pushdown do nothing
  } else if (0 == subquery.get_from_item_size()) {
    //expr value plan can not pushdown
  } else {
    has_rollup = subquery.has_rollup();
    if (OB_FAIL(subquery.has_rownum(has_rownum))) {
      LOG_WARN("failed to check stmt has rownum", K(ret));
    } else if (subquery.has_limit() || subquery.has_sequence() ||
               subquery.is_contains_assignment() ||
               has_rollup || has_rownum) {
      //can not pushdown do nothing
    } else if (OB_FAIL(check_pushdown_join_filter_quals(parent_stmt,
                                                        subquery,
                                                        pushdown_left_quals,
                                                        pushdown_right_quals,
                                                        candi_left_quals,
                                                        candi_right_quals))) {
      LOG_WARN("failed to check pushdown filter", K(ret));
    } else if (candi_right_quals.empty()) {
      //do thing
    } else {
      can_pushdown = true;
    }
  }
  return ret;
}

int ObOptimizerUtil::check_pushdown_join_filter_quals(const ObDMLStmt &parent_stmt,
                                                      const ObSelectStmt &subquery,
                                                      const ObIArray<ObRawExpr*> &pushdown_left_quals,
                                                      const ObIArray<ObRawExpr*> &pushdown_right_quals,
                                                      ObIArray<ObRawExpr*> &candi_left_quals,
                                                      ObIArray<ObRawExpr*> &candi_right_quals)
{
  int ret = OB_SUCCESS;
  bool is_valid = false;
  ObSEArray<ObRawExpr *, 4> common_exprs;
  if (!parent_stmt.is_set_stmt() && subquery.is_set_stmt()) {
    if (OB_FAIL(candi_left_quals.assign(pushdown_left_quals))) {
      LOG_WARN("failed to assign quals", K(ret));
    } else if (OB_FAIL(candi_right_quals.assign(pushdown_right_quals))) {
      LOG_WARN("failed to assign quals", K(ret));
    }
  } else if (OB_FAIL(get_groupby_win_func_common_exprs(subquery,
                                                       common_exprs,
                                                       is_valid))) {
    LOG_WARN("failed to get common exprs", K(ret));
  } else if (is_valid && common_exprs.empty()) {
    //can not pushdown any filter
  } else if (parent_stmt.is_set_stmt()) {
    if (OB_FAIL(check_pushdown_join_filter_for_set(static_cast<const ObSelectStmt&>(parent_stmt),
                                                   subquery,
                                                   common_exprs,
                                                   pushdown_left_quals,
                                                   pushdown_right_quals,
                                                   candi_left_quals,
                                                   candi_right_quals))) {
      LOG_WARN("failed to check pushdown filter for set stmt", K(ret));
    }
  } else {
    if (OB_FAIL(check_pushdown_join_filter_for_subquery(parent_stmt,
                                                        subquery,
                                                        common_exprs,
                                                        pushdown_left_quals,
                                                        pushdown_right_quals,
                                                        candi_left_quals,
                                                        candi_right_quals))) {
      LOG_WARN("failed to check pushdown filter for subquery", K(ret));
    }
  }
  return ret;
}

int ObOptimizerUtil::check_pushdown_join_filter_for_subquery(const ObDMLStmt &parent_stmt,
                                                             const ObSelectStmt &subquery,
                                                             ObIArray<ObRawExpr*> &common_exprs,
                                                             const ObIArray<ObRawExpr*> &pushdown_left_quals,
                                                             const ObIArray<ObRawExpr*> &pushdown_right_quals,
                                                             ObIArray<ObRawExpr*> &candi_left_quals,
                                                             ObIArray<ObRawExpr*> &candi_right_quals)
{
  int ret = OB_SUCCESS;
  if (parent_stmt.is_set_stmt() ||
      OB_UNLIKELY(pushdown_right_quals.count() != pushdown_left_quals.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect set stmt here", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < pushdown_right_quals.count(); ++i) {
      ObRawExpr *pred = NULL;
      ObSEArray<ObRawExpr *, 4> column_exprs;
      ObSEArray<ObRawExpr *, 4> select_exprs;
      if (OB_ISNULL(pred = pushdown_right_quals.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("predicate is null", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(pred, column_exprs))) {
        LOG_WARN("failed to extract column exprs", K(ret));
      } else if (OB_FAIL(ObTransformUtils::convert_column_expr_to_select_expr(column_exprs,
                                                                              subquery,
                                                                              select_exprs))) {
        LOG_WARN("failed to convert column exprs to select exprs", K(ret));
      } else if (column_exprs.count() != select_exprs.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect column expr count", K(ret));
      } else {
        bool is_simple_expr = true;
        ObSEArray<ObRawExpr *, 4> view_column_exprs;
        for (int64_t j = 0; OB_SUCC(ret) && is_simple_expr && j < select_exprs.count(); ++j) {
          ObRawExpr *expr = select_exprs.at(j);
          if (OB_ISNULL(expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpect null expr", K(ret));
          } else if (expr->has_flag(CNT_WINDOW_FUNC) ||
                     expr->has_flag(CNT_AGG) ||
                     ObPredicateDeduce::contain_special_expr(*expr)) {
            is_simple_expr = false;
          } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(expr, view_column_exprs))) {
            LOG_WARN("failed to extract column exprs", K(ret));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (!is_simple_expr) {
          //can not push down
        } else if (!common_exprs.empty() &&
                   !subset_exprs(view_column_exprs, common_exprs)) {
          //common_exprs为空，说明既没有windown func，也没有group by
        } else if (OB_FAIL(candi_right_quals.push_back(pred))) {
          LOG_WARN("failed to push back predicate", K(ret));
        } else if (OB_FAIL(candi_left_quals.push_back(pushdown_left_quals.at(i)))) {
          LOG_WARN("failed to push back predicate", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObOptimizerUtil::find_expr_in_equal_sets(const EqualSets &equal_sets,
                                             const ObRawExpr *target_expr,
                                             int64_t &idx)
{
  int ret = OB_SUCCESS;
  idx = OB_INVALID_ID;
  for (int64_t j = 0; idx == OB_INVALID_ID && j < equal_sets.count(); j++) {
    if (OB_ISNULL(equal_sets.at(j))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret), K(j));
    } else if (find_item(*equal_sets.at(j), target_expr)) {
      idx = j;
    }
  }
  return ret;
}

int ObOptimizerUtil::check_pushdown_join_filter_for_set(const ObSelectStmt &parent_stmt,
                                                        const ObSelectStmt &subquery,
                                                        ObIArray<ObRawExpr*> &common_exprs,
                                                        const ObIArray<ObRawExpr*> &pushdown_left_quals,
                                                        const ObIArray<ObRawExpr*> &pushdown_right_quals,
                                                        ObIArray<ObRawExpr*> &candi_left_quals,
                                                        ObIArray<ObRawExpr*> &candi_right_quals)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 4> child_select_list;
  ObSEArray<ObRawExpr *, 4> parent_select_list;
  if (OB_UNLIKELY(pushdown_right_quals.count() != pushdown_left_quals.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect quals count", K(ret));
  } else if (OB_FAIL(subquery.get_select_exprs(child_select_list))) {
    LOG_WARN("get child stmt select exprs failed", K(ret));
  } else if (OB_FAIL(parent_stmt.get_select_exprs(parent_select_list))) {
    LOG_WARN("get parent stmt select exprs failed", K(ret));
  } else if (child_select_list.count() != parent_select_list.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child stmt select exprs size is incorrect", K(child_select_list.count()),
                                                          K(parent_select_list.count()), K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < pushdown_right_quals.count(); ++i) {
    ObSEArray<ObRawExpr *, 4> view_column_exprs;
    ObRawExpr *expr = pushdown_right_quals.at(i);
    bool should_add = false;
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null expr", K(ret));
    } else if (OB_FAIL(ObTransformUtils::replace_expr(parent_select_list,
                                                      child_select_list,
                                                      expr))) {
      SQL_LOG(WARN, "failed to replace expr", K(ret));
    } else if (OB_FAIL(expr->extract_info())) {
      LOG_WARN("failed to extract info", K(ret), K(*expr));
    } else if (expr->has_flag(CNT_WINDOW_FUNC) ||
               expr->has_flag(CNT_AGG)) {
    } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(expr, view_column_exprs))) {
      LOG_WARN("failed to extract column exprs", K(ret));
    } else if (!common_exprs.empty() &&
               !subset_exprs(view_column_exprs, common_exprs)) {
      //common_exprs为空，说明既没有windown func，也没有group by
    } else {
      should_add = true;
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObTransformUtils::replace_expr(child_select_list,
                                                      parent_select_list,
                                                      expr))) {
      SQL_LOG(WARN, "failed to replace expr", K(ret));
    } else if (OB_FAIL(expr->extract_info())) {
      LOG_WARN("failed to extract info", K(ret), K(*expr));
    } else if (!should_add) {
    } else if (OB_FAIL(candi_right_quals.push_back(expr))) {
      LOG_WARN("failed to push back predicate", K(ret));
    } else if (OB_FAIL(candi_left_quals.push_back(pushdown_left_quals.at(i)))) {
      LOG_WARN("failed to push back predicate", K(ret));
    }
  }
  return ret;
}

int ObOptimizerUtil::replace_column_with_select_for_partid(const ObInsertStmt *stmt,
                                                           ObOptimizerContext &opt_ctx,
                                                           ObRawExpr *&calc_part_id_expr)
{
  int ret = OB_SUCCESS;
  // get column_item.
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null pointer", K(ret));
  } else {
    ObRawExprCopier copier(opt_ctx.get_expr_factory());
    const ObInsertTableInfo &insert_info = stmt->get_insert_table_info();
    for (int64_t i = 0; OB_SUCC(ret) && i < insert_info.column_exprs_.count(); ++i) {
      if (OB_FAIL(copier.add_replaced_expr(insert_info.column_exprs_.at(i),
                                           insert_info.column_conv_exprs_.at(i)))) {
        LOG_WARN("failed to add replace pair", K(ret));
      }
    }
    if (FAILEDx(copier.copy_on_replace(calc_part_id_expr,
                                       calc_part_id_expr))) {
      LOG_WARN("failed to copy on replace expr", K(ret));
    }
  }
  return ret;
}


int ObOptimizerUtil::replace_gen_column(ObLogPlan *log_plan, ObRawExpr *part_expr, ObRawExpr *&new_part_expr)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 8> column_exprs;
  new_part_expr = part_expr;
  if (OB_ISNULL(part_expr)) {
    // do nothing
  } else if (OB_ISNULL(log_plan)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(part_expr, column_exprs))) {
    LOG_WARN("fail to extract column exprs", K(part_expr), K(ret));
  } else {
    ObRawExprCopier copier(log_plan->get_optimizer_context().get_expr_factory());
    bool cnt_gen_columns = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < column_exprs.count(); ++i) {
      if (OB_ISNULL(column_exprs.at(i)) ||
          OB_UNLIKELY(!column_exprs.at(i)->is_column_ref_expr())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(ret));
      } else {
        ObColumnRefRawExpr *col = static_cast<ObColumnRefRawExpr *>(column_exprs.at(i));
        if (!col->is_generated_column()) {
          // do nothing
        } else if (OB_ISNULL(col->get_dependant_expr())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("dependant expr is null", K(ret), K(*col));
        } else if (OB_FAIL(copier.add_replaced_expr(col, col->get_dependant_expr()))) {
          LOG_WARN("failed to add replace pair", K(ret));
        } else {
          cnt_gen_columns = true;
        }
      }
    }
    if (OB_SUCC(ret) && cnt_gen_columns) {
      if (OB_FAIL(copier.copy_on_replace(part_expr, new_part_expr))) {
        LOG_WARN("failed to copy on replace expr", K(ret));
      }
    }
  }
  return ret;
}

int ObOptimizerUtil::generate_pseudo_trans_info_expr(ObOptimizerContext &opt_ctx,
                                                     const common::ObString &table_name,
                                                     ObOpPseudoColumnRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObExprResType res_type;
  char *pseudo_name = nullptr;
  res_type.set_type(ObVarcharType);
  res_type.set_collation_type(CS_TYPE_BINARY);
  res_type.set_accuracy(ObAccuracy::MAX_ACCURACY[ObVarcharType]);
  const char *name = ".TRANS_DEBUG_INFO";
  int64_t buf_len = table_name.length()+ STRLEN(name) + 1;
  int64_t pos = 0;
  if (OB_ISNULL(pseudo_name =
      static_cast<char*>(opt_ctx.get_allocator().alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate name buffer failed", K(ret), K(buf_len));
  } else if (OB_FAIL(databuff_printf(pseudo_name, buf_len, pos, "%.*s", table_name.length(), table_name.ptr()))) {
    LOG_WARN("databuff print column name failed", K(ret));
  } else if (OB_FAIL(databuff_printf(pseudo_name, buf_len, pos, "%.*s", static_cast<int32_t>(STRLEN(name)), name))) {
    LOG_WARN("databuff print column name failed", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_op_pseudo_column_expr(opt_ctx.get_expr_factory(),
                                                                 T_PSEUDO_ROW_TRANS_INFO_COLUMN,
                                                                 pseudo_name,
                                                                 res_type,
                                                                 expr))) {
    LOG_WARN("build operator pseudo column failed", K(ret));
  } else if (OB_FAIL(expr->formalize(opt_ctx.get_session_info()))) {
    LOG_WARN("expr formalize failed", K(ret));
  }
  return ret;
}

int ObOptimizerUtil::is_in_range_optimization_enabled(const ObGlobalHint &global_hint, ObSQLSessionInfo *session_info, bool &is_enabled)
{
  int ret = OB_SUCCESS;
  bool has_hint = false;
  bool is_hint_enabled = false;
  if (OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(global_hint.opt_params_.get_bool_opt_param(ObOptParamHint::ENABLE_IN_RANGE_OPTIMIZATION, is_hint_enabled, has_hint))) {
    LOG_WARN("failed to check has opt param", K(ret));
  } else if (has_hint) {
    is_enabled = is_hint_enabled;
  } else {
    is_enabled = session_info->is_in_range_optimization_enabled();
  }
  return ret;
}

int ObOptimizerUtil::pushdown_and_rename_filter_into_subquery(const ObDMLStmt &parent_stmt,
                                                              const ObSelectStmt &subquery,
                                                              int64_t table_id,
                                                              ObOptimizerContext &opt_ctx,
                                                              ObIArray<ObRawExpr *> &input_filters,
                                                              ObIArray<ObRawExpr *> &push_filters,
                                                              ObIArray<ObRawExpr *> &remain_filters,
                                                              bool check_match_index)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 4> candi_filters;
  bool can_pushdown = false;
  ObSQLSessionInfo *session_info = opt_ctx.get_session_info();
  ObRawExprFactory &expr_factory = opt_ctx.get_expr_factory();
  if (!input_filters.empty() &&
      OB_FAIL(pushdown_filter_into_subquery(parent_stmt,
                                            subquery,
                                            opt_ctx,
                                            input_filters,
                                            candi_filters,
                                            remain_filters,
                                            can_pushdown,
                                            check_match_index))) {
    LOG_WARN("pushdown filters into left query failed", K(ret));
  } else if (!candi_filters.empty() &&
            OB_FAIL(rename_pushdown_filter(parent_stmt, subquery,
                                           table_id, session_info,
                                           expr_factory,
                                           candi_filters,
                                           push_filters))) {
    LOG_WARN("failed to rename pushdown filter", K(ret));
  }
  for (int64_t i = 0 ; OB_SUCC(ret) && i < remain_filters.count(); i ++) {
    ObRawExpr *part_push_filter = NULL;
    bool can_pushdown_all = false;
    if (OB_FAIL(split_or_filter_into_subquery(parent_stmt,
                                              subquery,
                                              table_id,
                                              opt_ctx,
                                              remain_filters.at(i),
                                              part_push_filter,
                                              can_pushdown_all,
                                              check_match_index))) {
      LOG_WARN("failed to push part of the filter", K(ret));
    } else if (OB_UNLIKELY(can_pushdown_all)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("can not push the whole remain filter", K(ret), KPC(remain_filters.at(i)));
    } else if (NULL != part_push_filter &&
               OB_FAIL(push_filters.push_back(part_push_filter))) {
      LOG_WARN("failed to push back", K(ret));
    }
  }
  return ret;
}

/**
 * If the filter is an or predicate,
 * we try to push part of it into the subquery
 * e.g.
 *   select * from (select a, count(*) as cnt from t1 group by a) v where a = 1 and cnt = 1 or a = 2;
 *   We can not push the whole filter `a = 1 and cnt = 1 or a = 2`,
 *   But we can push `a = 1 or a = 2` into the subquery.
*/
int ObOptimizerUtil::split_or_filter_into_subquery(const ObDMLStmt &parent_stmt,
                                                   const ObSelectStmt &subquery,
                                                   int64_t table_id,
                                                   ObOptimizerContext &opt_ctx,
                                                   ObRawExpr *filter,
                                                   ObRawExpr *&push_filter,
                                                   bool &can_pushdown_all,
                                                   bool check_match_index)
{
  int ret = OB_SUCCESS;
  push_filter = NULL;
  ObRawExprFactory &expr_factory = opt_ctx.get_expr_factory();
  ObSQLSessionInfo *session_info = opt_ctx.get_session_info();
  if (OB_ISNULL(filter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null expr", K(ret));
  } else if (T_OP_OR != filter->get_expr_type()) {
    // do nothing
  } else {
    ObOpRawExpr *or_pred = static_cast<ObOpRawExpr *>(filter);
    ObSEArray<ObIArray<ObRawExpr*> *, 4> or_filter_params;
    ObSEArray<const ObDMLStmt *, 4> parent_stmts;
    ObSEArray<const ObSelectStmt *, 4> subqueries;
    ObSEArray<int64_t, 4> table_ids;
    ObArenaAllocator tmp_allocator;
    for (int64_t i = 0; OB_SUCC(ret) && i < or_pred->get_param_count(); ++i) {
      ObRawExpr *cur_expr = or_pred->get_param_expr(i);
      ObIArray<ObRawExpr*> *param_exprs = NULL;
      void *ptr = NULL;
      if (OB_ISNULL(cur_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null expr", K(ret), KPC(or_pred));
      } else if (OB_ISNULL(ptr = tmp_allocator.alloc(sizeof(ObSEArray<ObRawExpr*, 4>)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (FALSE_IT((param_exprs = new (ptr)ObSEArray<ObRawExpr*, 4>()))) {
      } else if (T_OP_AND == cur_expr->get_expr_type()) {
        ObOpRawExpr *and_pred = static_cast<ObOpRawExpr *>(cur_expr);
        if (OB_FAIL(param_exprs->assign(and_pred->get_param_exprs()))) {
          LOG_WARN("failed to assgin predicates", K(ret));
        }
      } else {
        if (OB_FAIL(param_exprs->push_back(cur_expr))) {
          LOG_WARN("failed to push back predicate", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(or_filter_params.push_back(param_exprs))) {
        LOG_WARN("failed to push back", K(ret));
      } else if (OB_FAIL(parent_stmts.push_back(&parent_stmt))) {
        LOG_WARN("failed to push back", K(ret));
      } else if (OB_FAIL(subqueries.push_back(&subquery))) {
        LOG_WARN("failed to push back", K(ret));
      } else if (OB_FAIL(table_ids.push_back(table_id))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
    if (OB_SUCC(ret) &&
        OB_FAIL(split_or_filter_into_subquery(parent_stmts,
                                              subqueries,
                                              table_ids,
                                              or_filter_params,
                                              opt_ctx,
                                              push_filter,
                                              can_pushdown_all,
                                              check_match_index))) {
      LOG_WARN("failed to split or fitler", K(ret));
    }
    for (int64_t i = 0; i < or_filter_params.count(); i ++) {
      if (OB_NOT_NULL(or_filter_params.at(i))) {
        or_filter_params.at(i)->~ObIArray();
      }
    }
  }
  return ret;
}

int ObOptimizerUtil::split_or_filter_into_subquery(ObIArray<const ObDMLStmt *> &parent_stmts,
                                                   ObIArray<const ObSelectStmt *> &subqueries,
                                                   ObIArray<int64_t> &table_ids,
                                                   ObIArray<ObIArray<ObRawExpr *>*> &or_filter_params,
                                                   ObOptimizerContext &opt_ctx,
                                                   ObRawExpr *&push_filter,
                                                   bool &can_pushdown_all,
                                                   bool check_match_index)
{
  int ret = OB_SUCCESS;
  push_filter = NULL;
  ObRawExprFactory &expr_factory = opt_ctx.get_expr_factory();
  ObSQLSessionInfo *session_info = opt_ctx.get_session_info();
  can_pushdown_all = true;
  bool have_push_filter = true;
  ObSEArray<ObSEArray<ObRawExpr*, 4>, 2> final_push_filters;
  if (OB_UNLIKELY(parent_stmts.count() != subqueries.count()) ||
      OB_UNLIKELY(subqueries.count() != table_ids.count()) ||
      OB_UNLIKELY(table_ids.count() != or_filter_params.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected param count", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && have_push_filter && i < or_filter_params.count(); ++i) {
    ObSEArray<ObRawExpr*, 4> push_filters;
    ObSEArray<ObRawExpr*, 4> remain_filters;
    bool can_push_to_where = false;
    if (OB_ISNULL(parent_stmts.at(i)) ||
        OB_ISNULL(subqueries.at(i)) ||
        OB_ISNULL(or_filter_params.at(i)) ||
        OB_UNLIKELY(or_filter_params.at(i)->empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null stmt", K(ret));
    } else if (OB_FAIL(pushdown_filter_into_subquery(*parent_stmts.at(i),
                                                     *subqueries.at(i),
                                                     opt_ctx,
                                                     *or_filter_params.at(i),
                                                     push_filters,
                                                     remain_filters,
                                                     can_push_to_where,
                                                     check_match_index))) {
      LOG_WARN("failed to pushdown filter", K(ret));
    } else if (push_filters.empty()) {
      // AND pred can not be pushed
      have_push_filter = false;
      can_pushdown_all = false;
    } else {
      // Part/All of AND pred can be pushed
      can_pushdown_all &= remain_filters.empty();
      if (OB_FAIL(final_push_filters.push_back(push_filters))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
  }

  if (OB_SUCC(ret) && have_push_filter) {
    ObSEArray<ObRawExpr *, 4> rename_and_exprs;
    ObRawExpr *new_or_expr = NULL;
    for (int64_t i = 0 ; OB_SUCC(ret) && i < final_push_filters.count() ; i ++) {
      ObSEArray<ObRawExpr*, 4> rename_exprs;
      ObRawExpr *rename_and_expr = NULL;
      if (OB_FAIL(rename_pushdown_filter(*parent_stmts.at(i),
                                         *subqueries.at(i),
                                         table_ids.at(i),
                                         session_info,
                                         expr_factory,
                                         final_push_filters.at(i),
                                         rename_exprs))) {
        LOG_WARN("failed to rename push down preds", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::build_and_expr(expr_factory,
                                                        rename_exprs,
                                                        rename_and_expr))) {
        LOG_WARN("failed to build and expr", K(ret));
      } else if (OB_FAIL(rename_and_exprs.push_back(rename_and_expr))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObRawExprUtils::build_or_exprs(expr_factory,
                                                      rename_and_exprs,
                                                      new_or_expr))) {
      LOG_WARN("failed to build and expr", K(ret));
    } else if (OB_FAIL(new_or_expr->formalize(session_info))) {
      LOG_WARN("failed to formalize expr", K(ret));
    } else if (OB_FAIL(new_or_expr->pull_relation_id())) {
      LOG_WARN("failed to pull relation id and levels", K(ret));
    } else {
      push_filter = new_or_expr;
    }
  }

  return ret;
}

/**
 * If every appearance of cte is accompanied by some filter,
 * We can combine these filters to reduce the data materialized by cte.
 * We try to push these filters to where condition.
 * If all filters can be push to where condition, nonwhere_filter will be NULL.
 * Otherwise, we might have both where_filter and nonwhere_filter.
 * e.g.
 *   with cte as (select a,count(*) as cnt from t1 group by a)
 *    select * from cte where a = 1 and cnt = 1 union all select * from cte where a = 2 and cnt = 2;
 *   nonwhere_filter :  (a = 1 and cnt = 1) or (a = 2 and cnt = 2)
 *   where_filter : (a = 1) or (a = 2)
*/
int ObOptimizerUtil::try_push_down_temp_table_filter(ObOptimizerContext &opt_ctx,
                                                     ObSqlTempTableInfo &info,
                                                     ObRawExpr *&nonwhere_filter,
                                                     ObRawExpr *&where_filter)
{
  int ret = OB_SUCCESS;
  bool have_filter = true;
  nonwhere_filter = NULL;
  where_filter = NULL;
  ObSEArray<ObIArray<ObRawExpr *> *, 4> temp_table_filters;
  ObSEArray<const ObDMLStmt *, 4> parent_stmts;
  ObSEArray<const ObSelectStmt *, 4> subqueries;
  ObSEArray<int64_t, 4> table_ids;
  ObSelectStmt *temp_table_query = info.table_query_;
  for (int64_t i = 0; OB_SUCC(ret) && have_filter && i < info.table_infos_.count(); ++i) {
    TableItem *table =  info.table_infos_.at(i).table_item_;
    ObDMLStmt *stmt = info.table_infos_.at(i).upper_stmt_;
    if (OB_ISNULL(table) || OB_ISNULL(stmt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if (info.table_infos_.at(i).table_filters_.empty()) {
      have_filter = false;
    } else if (OB_FAIL(temp_table_filters.push_back(&info.table_infos_.at(i).table_filters_))) {
      LOG_WARN("failed to push back", K(ret));
    } else if (OB_FAIL(parent_stmts.push_back(stmt))) {
      LOG_WARN("failed to push back", K(ret));
    } else if (OB_FAIL(subqueries.push_back(temp_table_query))) {
      LOG_WARN("failed to push back", K(ret));
    } else if (OB_FAIL(table_ids.push_back(table->table_id_))) {
      LOG_WARN("failed to push back", K(ret));
    }
  }

  if (OB_SUCC(ret) && have_filter) {
    OPT_TRACE("pushdown filter into temp table:", info.table_query_);
    bool can_push_all = false;
    if (OB_FAIL(ObOptimizerUtil::split_or_filter_into_subquery(parent_stmts,
                                                               subqueries,
                                                               table_ids,
                                                               temp_table_filters,
                                                               opt_ctx,
                                                               where_filter,
                                                               can_push_all,
                                                               /*check_match_index = */false))) {
      LOG_WARN("failed to split filter", K(ret));
    } else if (can_push_all) {
      // do nothing
    } else if (OB_FAIL(push_down_temp_table_filter(opt_ctx.get_expr_factory(), opt_ctx.get_session_info(),
                                                   info, nonwhere_filter))) {
      LOG_WARN("failed to push down remain temp table filter", K(ret));
    }
    if (NULL != where_filter) {
      OPT_TRACE("succeed to pushdown filter to where:", where_filter);
    }
    if (NULL != nonwhere_filter) {
      OPT_TRACE("succeed to pushdown filter into the top of temp table:", nonwhere_filter);
    }
  }
  return ret;
}

int ObOptimizerUtil::push_down_temp_table_filter(ObRawExprFactory &expr_factory,
                                                 ObSQLSessionInfo *session_info,
                                                 ObSqlTempTableInfo &info,
                                                 ObRawExpr *&temp_table_filter,
                                                 ObSelectStmt *temp_table_query)
{
  int ret = OB_SUCCESS;
  temp_table_filter = NULL;
  ObSEArray<ObRawExpr *, 8> and_exprs;
  ObRawExpr *or_expr = NULL;
  bool have_temp_table_filter = true;
  temp_table_query = (NULL == temp_table_query) ? info.table_query_ : temp_table_query;
  if (OB_ISNULL(temp_table_query)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null param", K(info), K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && have_temp_table_filter && i < info.table_infos_.count(); ++i) {
    have_temp_table_filter &= !info.table_infos_.at(i).table_filters_.empty();
  }
  for (int64_t i = 0; OB_SUCC(ret) && have_temp_table_filter && i < info.table_infos_.count(); ++i) {
    ObDMLStmt *upper_stmt = info.table_infos_.at(i).upper_stmt_;
    TableItem *table = info.table_infos_.at(i).table_item_;
    ObIArray<ObRawExpr *> &table_filters = info.table_infos_.at(i).table_filters_;
    ObSEArray<ObRawExpr *, 8> rename_exprs;
    ObRawExpr *and_expr = NULL;
    if (table_filters.empty() || OB_ISNULL(upper_stmt) ||
        OB_ISNULL(table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null table info", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::rename_pushdown_filter(*upper_stmt,
                                                               *temp_table_query,
                                                               table->table_id_,
                                                               session_info,
                                                               expr_factory,
                                                               table_filters,
                                                               rename_exprs))) {
      LOG_WARN("failed to rename push down preds", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_and_expr(expr_factory,
                                                      rename_exprs,
                                                      and_expr))) {
      LOG_WARN("failed to build and expr", K(ret));
    }
    if (OB_SUCC(ret) && OB_FAIL(and_exprs.push_back(and_expr))) {
      LOG_WARN("failed to push back expr", K(ret));
    }
  }
  if (OB_FAIL(ret) || !have_temp_table_filter) {
  } else if (OB_FAIL(ObRawExprUtils::build_or_exprs(expr_factory,
                                                    and_exprs,
                                                    or_expr))) {
    LOG_WARN("failed to build or expr", K(ret));
  } else if (OB_FAIL(or_expr->formalize(session_info))) {
    LOG_WARN("failed to formalize expr", K(ret));
  } else if (OB_FAIL(or_expr->pull_relation_id())) {
    LOG_WARN("failed to pull relation id and levels", K(ret));
  } else {
    temp_table_filter = or_expr;
  }
  return ret;
}

bool ObOptimizerUtil::find_superset(const ObRelIds &rel_ids,
                                    const ObIArray<ObRelIds> &single_table_ids)
{
  bool bret = false;
  for (int64_t i = 0; !bret && i < single_table_ids.count(); ++i) {
    bret = (single_table_ids.at(i).is_superset(rel_ids));
  }
  return bret;
}

int ObOptimizerUtil::check_contain_my_exec_param(const ObIArray<ObRawExpr *> &exprs,
                                                 const ObIArray<ObExecParamRawExpr*> &my_exec_params,
                                                 bool &contain)
{
  int ret = OB_SUCCESS;
  contain = false;
  for (int64_t i = 0; OB_SUCC(ret) && !contain && i < exprs.count(); ++i) {
    const ObRawExpr* expr = exprs.at(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(check_contain_my_exec_param(expr,
                                                   my_exec_params,
                                                   contain))) {
      LOG_WARN("failed to check contain my exec param", K(ret));
    }
  }
  return ret;
}int ObOptimizerUtil::check_is_static_false_expr(ObOptimizerContext &opt_ctx, ObRawExpr &expr, bool &is_static_false)
{
  int ret = OB_SUCCESS;
  ObObj const_value;
  bool got_result = false;
  bool is_result_true = false;
  if (!expr.is_static_const_expr()) {
    // do nothing
  } else if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(opt_ctx.get_exec_ctx(),
                                                               &expr,
                                                               const_value,
                                                               got_result,
                                                               opt_ctx.get_allocator()))) {
    LOG_WARN("failed to calc const or calculable expr", K(ret));
  } else if (!got_result) {
    // do nothing
  } else if (OB_FAIL(ObObjEvaluator::is_true(const_value, is_result_true))) {
    LOG_WARN("failed to check is const value true", K(ret));
  } else {
    is_static_false = !is_result_true;
  }
  return ret;
}

int ObOptimizerUtil::check_ancestor_node_support_skip_scan(ObLogicalOperator* op, bool &can_use_batch_nlj)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* parent = nullptr;
  if (OB_ISNULL(op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Current operator node is null", K(ret));
  } else if (OB_ISNULL(parent = op->get_parent())) {
    // do nothing
  } else if (can_use_batch_nlj) {
    if (parent->get_type() == log_op_def::LOG_SUBPLAN_FILTER) {
      can_use_batch_nlj = false;
    } else if (parent->get_type() == log_op_def::LOG_JOIN) {
      ObLogJoin *join = static_cast<ObLogJoin*>(parent);
      if (IS_SEMI_ANTI_JOIN(join->get_join_type())) {
        can_use_batch_nlj = false;
      }
    }
    if (can_use_batch_nlj && OB_FAIL(SMART_CALL(check_ancestor_node_support_skip_scan(parent, can_use_batch_nlj)))) {
      LOG_WARN("failed to check parent node can use batch NLJ", K(ret));
    }
  }
  return ret;
}

int ObOptimizerUtil::try_split_or_qual(const ObDMLStmt *stmt,
                                       ObRawExprFactory &expr_factory,
                                       const ObSQLSessionInfo *session_info,
                                       const ObRelIds &table_ids,
                                       ObOpRawExpr &or_qual,
                                       ObIArray<ObRawExpr*> &table_quals,
                                       ObIArray<ObRawExpr*> &new_or_quals)
{
  int ret = OB_SUCCESS;
  ObOpRawExpr *new_expr = NULL;
  if (OB_FAIL(ObOptimizerUtil::split_or_qual_on_table(stmt,
                                                      expr_factory,
                                                      session_info,
                                                      table_ids,
                                                      or_qual,
                                                      new_expr))) {
    LOG_WARN("failed to split or qual on table", K(ret));
  } else if (NULL == new_expr) {
    /* do nothing */
  } else if (ObOptimizerUtil::find_equal_expr(table_quals, new_expr)) {
    /* do nothing */
  } else if (OB_FAIL(table_quals.push_back(new_expr))) {
    LOG_WARN("failed to push back new expr", K(ret));
  } else if (OB_FAIL(new_or_quals.push_back(new_expr))) {
    LOG_WARN("failed to push back expr", K(ret));
  }
  return ret;
}

int ObOptimizerUtil::split_or_quals(const ObDMLStmt *stmt,
                                    ObRawExprFactory &expr_factory,
                                    const ObSQLSessionInfo *session_info,
                                    const ObIArray<TableItem*> &table_items,
                                    ObIArray<ObRawExpr*> &quals,
                                    ObIArray<ObRawExpr*> &new_or_quals)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 8> new_quals;
  for (int64_t i = 0; OB_SUCC(ret) && i < quals.count(); ++i) {
    if (OB_FAIL(split_or_quals(stmt,
                               expr_factory,
                               session_info,
                               table_items,
                               quals.at(i),
                               new_quals,
                               new_or_quals))) {
      LOG_WARN("failed to split or quals", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(append(quals, new_quals))) {
      LOG_WARN("failed to append quals", K(ret));
    }
  }
  return ret;
}

int ObOptimizerUtil::split_or_quals(const ObDMLStmt *stmt,
                                    ObRawExprFactory &expr_factory,
                                    const ObSQLSessionInfo *session_info,
                                    const ObIArray<TableItem*> &table_items,
                                    ObRawExpr *qual,
                                    ObIArray<ObRawExpr*> &new_quals,
                                    ObIArray<ObRawExpr*> &new_or_quals)
{
  int ret = OB_SUCCESS;
  ObRelIds table_ids;
  bool is_filter = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(qual)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpect null", K(ret), K(stmt), K(qual));
  } else if (T_OP_OR != qual->get_expr_type()) {
    //do nothing
  } else if (!qual->has_flag(CNT_SUB_QUERY) &&
              OB_FAIL(is_joined_table_filter(stmt, table_items, qual, is_filter))) {
    LOG_WARN("failed to check is joined table filter", K(ret));
  } else if (is_filter) {
    //joined table内部谓词等到下推on condition再拆分，否则会重复拆分or谓词
  } else {
    ObOpRawExpr *or_qual = static_cast<ObOpRawExpr*>(qual);
    for (int64_t j = 0; OB_SUCC(ret) && j < table_items.count(); ++j) {
      TableItem *table_item = table_items.at(j);
      table_ids.reuse();
      if (OB_ISNULL(table_item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table item is null", K(ret));
      } else if (OB_FAIL(stmt->get_table_rel_ids(*table_item, table_ids))) {
        LOG_WARN("failed to get table ids", K(ret));
      } else if (!table_ids.overlap(or_qual->get_relation_ids())) {
        //do nothing
      } else if (qual->has_flag(CNT_SUB_QUERY) ||
                (!table_ids.is_superset(or_qual->get_relation_ids()))) {
        ret= try_split_or_qual(stmt, expr_factory, session_info, table_ids, *or_qual, new_quals, new_or_quals);
      }
    }
  }
  return ret;
}

int ObOptimizerUtil::is_joined_table_filter(const ObDMLStmt *stmt,
                                            const ObIArray<TableItem*> &table_items,
                                            const ObRawExpr *expr,
                                            bool &is_filter)
{
  int ret = OB_SUCCESS;
  is_filter = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null expr or stmt", K(ret), K(stmt), K(expr));
  } else if (expr->get_relation_ids().is_empty()) {
    //do nothing
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !is_filter && i < table_items.count(); ++i) {
      TableItem *item = table_items.at(i);
      ObRelIds joined_table_ids;
      if (OB_ISNULL(item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null semi item", K(ret));
      } else if (!item->is_joined_table()) {
        //do nothing
      } else if (OB_FAIL(stmt->get_table_rel_ids(*item, joined_table_ids))) {
        LOG_WARN("failed to get table ids", K(ret));
      } else if (joined_table_ids.is_superset(expr->get_relation_ids())) {
        is_filter = true;
      }
    }
  }
  return ret;
}