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
#include "sql/rewrite/ob_equal_analysis.h"
#include "common/ob_smart_call.h"
namespace oceanbase
{
using namespace common;
namespace sql
{
ObEqualAnalysis::ObEqualAnalysis()
    : expr_idx_map_()
{

}

int ObEqualAnalysis::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr_idx_map_.create(128, ObModIds::OB_SQL_OPTIMIZER_EQUAL_SETS,
                                   ObModIds::OB_SQL_OPTIMIZER_EQUAL_SETS))) {
    LOG_WARN("failed to create hash map", K(ret));
  }
  return ret;
}

ObEqualAnalysis::~ObEqualAnalysis()
{
  destroy();
}

void ObEqualAnalysis::destroy()
{
  if (expr_idx_map_.created()) {
    expr_idx_map_.destroy();
  }
}

int ObEqualAnalysis::get_expr_idx(ObRawExpr *expr, int64_t &expr_idx)
{
  int ret = OB_SUCCESS;
  expr_idx = -1;
  EqualSetKey key;
  key.expr_ = expr;
  if (OB_SUCC(expr_idx_map_.get_refactored(key, expr_idx))) {
    /*do nothing*/
  } else if (OB_HASH_NOT_EXIST != ret) {
    LOG_WARN("failed to get from hash map", K(ret), K(expr_idx));
  } else {
    ret = OB_SUCCESS;
    expr_idx = expr_idx_map_.size();
    if (OB_FAIL(expr_idx_map_.set_refactored(key, expr_idx))) {
      expr_idx = -1;
      LOG_WARN("set expr index to column set failed", K(ret), K(expr_idx));
    } else if (OB_FAIL(parent_idx_.push_back(expr_idx))) {
      LOG_WARN("failed to push back", K(expr_idx));
    } else if (OB_FAIL(exprs_.push_back(expr))) {
      LOG_WARN("failed to push back", K(ret));
    }
  }
  if (OB_SUCC(ret) && expr_idx < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected idx", K(ret));
  }
  return ret;
}

int ObEqualAnalysis::find_root_idx(const int64_t expr_idx, int64_t &root_idx)
{
  int ret = OB_SUCCESS;
  int64_t parent_idx = -1;
  root_idx = expr_idx;
  if (OB_UNLIKELY(root_idx < 0 || root_idx >= parent_idx_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid expr idx", K(ret), K(root_idx));
  }
  while (OB_SUCC(ret) && parent_idx_.at(root_idx) != root_idx) {
    parent_idx = parent_idx_.at(root_idx);
    if (OB_UNLIKELY(parent_idx < 0 || parent_idx >= parent_idx_.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid expr idx", K(ret), K(parent_idx));
    } else {
      parent_idx_.at(root_idx) = parent_idx_.at(parent_idx);
      root_idx = parent_idx;
    }
  }
  if (OB_SUCC(ret)) {
    parent_idx_.at(expr_idx) = root_idx;
  }
  return ret;
}

int ObEqualAnalysis::union_expr(const int64_t l_idx, const int64_t r_idx)
{
  int ret = OB_SUCCESS;
  int64_t l_root_idx = -1;
  int64_t r_root_idx = -1;
  if (l_idx == r_idx) {
    // do nothing
  } else if (OB_FAIL(find_root_idx(l_idx, l_root_idx))) {
    LOG_WARN("find root idx failed", K(ret));
  } else if (OB_FAIL(find_root_idx(r_idx, r_root_idx))) {
    LOG_WARN("find root idx failed", K(ret));
  } else if (l_root_idx == r_root_idx) {
    // do nothing
  } else {
    parent_idx_[r_root_idx] = l_root_idx;
  }
  return ret;
}

int ObEqualAnalysis::feed_where_expr(ObRawExpr *expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (T_OP_EQ == expr->get_expr_type()) {
    /// only add pred like `c1 = c2`
    ObOpRawExpr *eq_expr = static_cast<ObOpRawExpr *>(expr);
    if (OB_FAIL(add_equal_cond(*eq_expr))) {
      LOG_WARN("add equal condition failed", K(ret));
    } else {
      LOG_PRINT_EXPR(DEBUG, "accept expr for equal set", expr);
    }
  } else {
    LOG_PRINT_EXPR(DEBUG, "unaccept expr for equal set", expr);
  }
  return ret;
}

int ObEqualAnalysis::feed_equal_sets(const EqualSets &input_equal_sets)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < input_equal_sets.count(); ++i) {
    const EqualSet *eset = input_equal_sets.at(i);
    ObSEArray<int64_t, 4> expr_idx_array;
    int64_t min_expr_idx = INT64_MAX;
    for (int64_t j = 0; OB_SUCC(ret) && j < eset->count(); ++j) {
      int64_t expr_idx = 0;
      ObRawExpr *expr;
      if (OB_ISNULL(expr = eset->at(j))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null expr", K(ret));
      } else if (OB_FAIL(get_expr_idx(expr, expr_idx))) {
        LOG_WARN("failed to get expr idx", K(ret), KPC(expr));
      } else if (OB_FAIL(expr_idx_array.push_back(expr_idx))) {
        LOG_WARN("failed to push back expr idx", K(ret));
      } else {
        min_expr_idx = MIN(min_expr_idx, expr_idx);
      }
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < expr_idx_array.count(); ++j) {
      int64_t expr_idx = expr_idx_array.at(j);
      if (OB_FAIL(union_expr(min_expr_idx, expr_idx))) {
        LOG_WARN("failed to union expr", K(ret), K(min_expr_idx), K(expr_idx));
      }
    }
  }
  return ret;
}

int ObEqualAnalysis::add_equal_cond(ObOpRawExpr &expr)
{
  int ret = OB_SUCCESS;
  int64_t l_expr_idx = 0;
  int64_t r_expr_idx = 0;
  bool type_safe = false;
  if (OB_UNLIKELY(expr.get_expr_type() != T_OP_EQ)
      || OB_ISNULL(expr.get_param_expr(0))
      || OB_ISNULL(expr.get_param_expr(1))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("expr is invalid", K(expr));
  } else if (T_OP_ROW == expr.get_param_expr(0)->get_expr_type() ||
             T_OP_ROW == expr.get_param_expr(1)->get_expr_type() ||
             expr.get_param_expr(0)->same_as(*expr.get_param_expr(1))) {
    // do nothing
  } else if (OB_FAIL(ObRelationalExprOperator::is_equal_transitive(
                                                  expr.get_param_expr(0)->get_result_type(),
                                                  expr.get_param_expr(1)->get_result_type(),
                                                  type_safe))) {
    LOG_WARN("failed to check is equal transitive", K(ret), K(expr));
  } else if (!type_safe) {
    // do nothting
  } else if (OB_FAIL(get_expr_idx(expr.get_param_expr(0), l_expr_idx))) {
    LOG_WARN("failed to get left expr idx", K(ret));
  } else if (OB_FAIL(get_expr_idx(expr.get_param_expr(1), r_expr_idx))) {
    LOG_WARN("failed to get right expr idx", K(ret));
  } else if (OB_UNLIKELY(l_expr_idx == r_expr_idx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr idx should not be same", K(ret), K(l_expr_idx), K(r_expr_idx), K(expr));
  } else if (OB_FAIL(union_expr(l_expr_idx, r_expr_idx))) {
    LOG_WARN("failed to union expr", K(ret), K(l_expr_idx), K(r_expr_idx));
  }
  return ret;
}

int ObEqualAnalysis::get_equal_sets(ObIAllocator *alloc, EqualSets &equal_sets) const
{
  int ret = OB_SUCCESS;
  ObSEArray<int64_t, 4> union_set_map;
  ObSEArray<int64_t, 4> set_count_list;
  if (OB_ISNULL(alloc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (OB_UNLIKELY(exprs_.count() != parent_idx_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected count", K(ret), K(exprs_.count()), K(parent_idx_.count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < parent_idx_.count(); ++i) {
    int64_t root_idx = parent_idx_.at(i);
    if (OB_UNLIKELY(root_idx < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected root_idx", K(ret), K(root_idx));
    } else if (root_idx >= union_set_map.count()) {
      int64_t old_count = union_set_map.count();
      if (OB_FAIL(union_set_map.prepare_allocate(root_idx + 1))) {
        LOG_WARN("failed to prepare allocate", K(ret));
      } else if (OB_FAIL(set_count_list.push_back(1))) {
        LOG_WARN("failed to push back", K(ret));
      } else {
        for (int64_t i = old_count; OB_SUCC(ret) && i < root_idx; ++i) {
          union_set_map.at(i) = -1;
        }
        union_set_map.at(root_idx) = set_count_list.count() - 1;
      }
    } else if (-1 == union_set_map.at(root_idx)) {
      if (OB_FAIL(set_count_list.push_back(1))) {
        LOG_WARN("failed to push back", K(ret));
      } else {
        union_set_map.at(root_idx) = set_count_list.count() - 1;
      }
    } else {
      int64_t set_idx = union_set_map.at(root_idx);
      ++set_count_list.at(set_idx);
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < set_count_list.count(); ++i) {
    ObRawExprSet *expr_set = NULL;
    void *ptr = NULL;
    if (OB_UNLIKELY(set_count_list.at(i) < 2)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected set count", K(set_count_list), K(parent_idx_));
    } else if (OB_ISNULL(ptr = alloc->alloc(sizeof(ObRawExprSet)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("no memory to create ObRawExprSet", K(ret));
    } else {
      expr_set = new(ptr) ObRawExprSet();
      expr_set->set_allocator(alloc);
      if (OB_FAIL(expr_set->init(set_count_list.at(i)))) {
        LOG_WARN("failed to init expr set", K(ret));
      } else if (OB_FAIL(equal_sets.push_back(expr_set))) {
        LOG_WARN("failed to push back expr set", K(ret));
      }
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < parent_idx_.count(); ++i) {
    int64_t root_idx = parent_idx_.at(i);
    int64_t set_idx = union_set_map.at(root_idx);
    if (OB_UNLIKELY(set_idx < 0 || set_idx >= equal_sets.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected set_idx", K(ret), K(root_idx));
    } else if (OB_ISNULL(equal_sets.at(set_idx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null equal set", K(ret));
    } else if (OB_FAIL(equal_sets.at(set_idx)->push_back(exprs_.at(i)))) {
      LOG_WARN("failed to push back expr", K(ret));
    }
  }
  return ret;
}

int ObEqualAnalysis::finish_feed()
{
  int ret = OB_SUCCESS;
  int64_t dummy_idx = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < parent_idx_.count(); ++i) {
    if (OB_FAIL(find_root_idx(i, dummy_idx))) {
      LOG_WARN("failed to find root idx", K(ret));
    }
  }
  return ret;
}

int ObEqualAnalysis::compute_equal_set(ObIAllocator *allocator,
                                       const ObIArray<ObRawExpr *> &eset_conditions,
                                       EqualSets &equal_sets)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is null", K(ret));
  } else if (eset_conditions.count() > 0) {
    ObEqualAnalysis ana;
    if (OB_FAIL(ana.init())) {
      LOG_WARN("failed to init equal analysis", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < eset_conditions.count(); ++i) {
      if (OB_FAIL(ana.feed_where_expr(eset_conditions.at(i)))) {
        LOG_WARN("failed to feed where expr", K(ret));
      } else { /*do nothing*/ }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ana.finish_feed())) {
        LOG_WARN("finish feed failed", K(ret));
      } else if (OB_FAIL(ana.get_equal_sets(allocator, equal_sets))) {
        LOG_WARN("get equal sets failed", K(ret));
      }
    }
  }
  return ret;
}

int ObEqualAnalysis::compute_equal_set(ObIAllocator *allocator,
                                       const ObIArray<ObRawExpr *> &eset_conditions,
                                       const EqualSets &input_equal_sets,
                                       EqualSets &output_equal_sets)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is null", K(ret));
  } else if (eset_conditions.count() > 0) {
    ObEqualAnalysis ana;
    if (OB_FAIL(ana.init())) {
      LOG_WARN("failed to init equal analysis", K(ret));
    } else if (OB_FAIL(ana.feed_equal_sets(input_equal_sets))) {
      LOG_WARN("failed to feed input equal sets", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < eset_conditions.count(); ++i) {
      if (OB_FAIL(ana.feed_where_expr(eset_conditions.at(i)))) {
        LOG_WARN("failed to feed where expr", K(ret));
      } else { /*do nothing*/ }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ana.finish_feed())) {
        LOG_WARN("finish feed failed", K(ret));
      } else if (OB_FAIL(ana.get_equal_sets(allocator, output_equal_sets))) {
        LOG_WARN("get equal sets failed", K(ret));
      }
    }
  } else if (OB_FAIL(output_equal_sets.assign(input_equal_sets))) {
    LOG_WARN("failed to assign equal sets", K(ret));
  }
  return ret;
}

int ObEqualAnalysis::compute_equal_set(ObIAllocator *allocator,
                                       ObRawExpr *eset_condition,
                                       const EqualSets &input_equal_sets,
                                       EqualSets &output_equal_sets)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is null", K(ret));
  } else {
    ObEqualAnalysis ana;
    if (OB_FAIL(ana.init())) {
      LOG_WARN("failed to init equal analysis", K(ret));
    } else if (OB_FAIL(ana.feed_equal_sets(input_equal_sets))) {
      LOG_WARN("failed to feed input equal sets", K(ret));
    } else if (OB_FAIL(ana.feed_where_expr(eset_condition))) {
      LOG_WARN("failed to feed where expr", K(ret));
    } else if (OB_FAIL(ana.finish_feed())) {
      LOG_WARN("finish feed failed", K(ret));
    } else if (OB_FAIL(ana.get_equal_sets(allocator, output_equal_sets))) {
      LOG_WARN("get equal sets failed", K(ret));
    }
  }
  return ret;
}

int ObEqualAnalysis::merge_equal_set(ObIAllocator *allocator,
                                    const EqualSets &left_equal_sets,
                                    const EqualSets &right_equal_sets,
                                    EqualSets &output_equal_sets)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is null", K(ret));
  } else {
    ObEqualAnalysis ana;
    if (OB_FAIL(ana.init())) {
      LOG_WARN("failed to init equal analysis", K(ret));
    } else if (OB_FAIL(ana.feed_equal_sets(left_equal_sets))) {
      LOG_WARN("failed to feed input equal sets", K(ret));
    } else if (OB_FAIL(ana.feed_equal_sets(right_equal_sets))) {
      LOG_WARN("failed to feed where expr", K(ret));
    } else if (OB_FAIL(ana.finish_feed())) {
      LOG_WARN("finish feed failed", K(ret));
    } else if (OB_FAIL(ana.get_equal_sets(allocator, output_equal_sets))) {
      LOG_WARN("get equal sets failed", K(ret));
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
