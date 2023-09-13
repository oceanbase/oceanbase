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
#include "sql/engine/expr/ob_expr_operator.h"
#include "common/ob_smart_call.h"
namespace oceanbase
{
using namespace common;
namespace sql
{
ObEqualAnalysis::ObEqualAnalysis()
    : equal_set_alloc_(ObMemAttr(MTL_ID(), ObModIds::OB_SQL_OPTIMIZER_EQUAL_SETS)),
      column_set_(),
      equal_sets_()
{

}

int ObEqualAnalysis::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(column_set_.create(128, ObModIds::OB_SQL_OPTIMIZER_EQUAL_SETS,
                                 ObModIds::OB_SQL_OPTIMIZER_EQUAL_SETS))) {
    LOG_WARN("failed to create hash map", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

ObEqualAnalysis::~ObEqualAnalysis()
{
  reset();
}

void ObEqualAnalysis::reset()
{
  //The data structure has only the clear interface
  column_set_.reuse();
  DLIST_REMOVE_ALL_NORET(p, equal_sets_) {
    if (p != NULL) {
      p->~ObEqualSet();
      equal_set_alloc_.free(p);
      p = NULL;
    }
  }
  equal_sets_.reset();
  //the allocator is not equal analysis enjoy alone, so can't reset allocator at here
}

int ObEqualAnalysis::get_or_add_expr_idx(const ObRawExpr *expr, int64_t &expr_idx)
{
  int ret = OB_SUCCESS;
  expr_idx = -1;
  EqualSetKey key;
  key.expr_ = expr;
  if (OB_SUCC(column_set_.get_refactored(key, expr_idx))) {
    /*do nothing*/
  } else if (OB_HASH_NOT_EXIST != ret) {
    LOG_WARN("failed to get from hash map", K(ret), K(expr_idx));
  } else {
    ret = OB_SUCCESS;
    expr_idx = column_set_.size();
    if (OB_FAIL(column_set_.set_refactored(key, expr_idx))) {
      expr_idx = -1;
      LOG_WARN("set expr index to column set failed", K(ret), K(expr_idx));
    }
  }
  if (OB_SUCC(ret) && expr_idx < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected idx", K(ret));
  }
  return ret;
}

int ObEqualAnalysis::feed_where_expr(const ObRawExpr *expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr) ||
      (T_OP_EQ == expr->get_expr_type() &&
       (OB_ISNULL(expr->get_param_expr(0))
        || OB_ISNULL(expr->get_param_expr(1))))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (T_OP_EQ == expr->get_expr_type()) {
    /// only add pred like `c1 = c2`
    const ObOpRawExpr *eq_expr = static_cast<const ObOpRawExpr *>(expr);
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
    ObExprEqualSet *equal_set = NULL;
    //create new equal set
    if (OB_ISNULL(eset)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null equal set", K(ret));
    } else if (OB_ISNULL(equal_set = new_equal_set())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create new equal set failed", K(ret));
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < eset->count(); ++j) {
      int64_t expr_idx = 0;
      const ObRawExpr *expr;
      if (OB_ISNULL(expr = eset->at(j))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null", K(ret));
      } else if (OB_FAIL(get_or_add_expr_idx(expr, expr_idx))) {
        LOG_WARN("get or add expr idx failed", K(ret));
      }  else if (OB_FAIL(equal_set->add_expr(expr_idx, true, expr))) {
        LOG_WARN("add expr to equal set failed", K(ret));
      }
    }
  }
  return ret;
}

int ObEqualAnalysis::add_equal_cond(const ObOpRawExpr &expr)
{
  int ret = OB_SUCCESS;
  ObExprEqualSet *equal_set = NULL;
  int64_t l_expr_idx = 0;
  int64_t r_expr_idx = 0;

  if (OB_UNLIKELY(expr.get_expr_type() != T_OP_EQ)
      || OB_ISNULL(expr.get_param_expr(0))
      || OB_ISNULL(expr.get_param_expr(1))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("expr is invalid", K(expr));
  } else if (OB_FAIL(get_or_add_expr_idx(expr.get_param_expr(0), l_expr_idx))) {
    LOG_WARN("get or add left expr idx failed", K(ret));
  } else if (OB_FAIL(get_or_add_expr_idx(expr.get_param_expr(1), r_expr_idx))) {
    LOG_WARN("get or add right expr idx failed", K(ret));
  } else if (l_expr_idx != r_expr_idx) {
    //左右两边是完全相等的表达式，例如c1=c1, sum(c1)=sum(c1),属于恒true范围，不具有传递关系，所以不加入集合中
    if (OB_FAIL(find_equal_set(l_expr_idx, expr.get_param_expr(1), equal_set))) {
      LOG_WARN("find equal set failed", K(ret), K(l_expr_idx), K(*expr.get_param_expr(1)));
    } else if (equal_set != NULL) {
      if (!equal_set->has_expr(r_expr_idx)) {
        if (OB_FAIL(equal_set->add_expr(r_expr_idx, true, expr.get_param_expr(1)))) {
          LOG_WARN("add expr to equal set failed", K(ret));
        }
      }
    } else if (OB_FAIL(find_equal_set(r_expr_idx, expr.get_param_expr(0), equal_set))) {
      LOG_WARN("find equal set failed", K(ret), K(r_expr_idx), K(*expr.get_param_expr(0)));
    } else if (equal_set != NULL) {
      if (!equal_set->has_expr(l_expr_idx)) {
        if (OB_FAIL(equal_set->add_expr(l_expr_idx, true, expr.get_param_expr(0)))) {
          LOG_WARN("add expr to equal set failed", K(ret));
        }
      }
    }
    if (OB_SUCC(ret) && NULL == equal_set) {
      //create new equal set
      if (OB_ISNULL(equal_set = new_equal_set())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("create new equal set failed", K(ret));
      } else if (OB_FAIL(equal_set->add_expr(l_expr_idx, true, expr.get_param_expr(0)))) {
        LOG_WARN("add expr to equal set failed", K(ret));
      } else if (OB_FAIL(equal_set->add_expr(r_expr_idx, true, expr.get_param_expr(1)))) {
        LOG_WARN("add expr to equal set failed", K(ret));
      }
    }
  }
  return ret;
}

int ObEqualAnalysis::get_equal_sets(ObIAllocator *alloc, EqualSets &equal_sets) const
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 8> raw_equal_set;
  DLIST_FOREACH(equal_set, equal_sets_) {
    raw_equal_set.reuse();
    for (ObExprEqualSet::ColumnIterator iter = equal_set->column_begin();
        OB_SUCC(ret) && iter != equal_set->column_end(); ++iter) {
      if (OB_FAIL(raw_equal_set.push_back(
                    const_cast<ObRawExpr*>(iter.get_expr_info())))) {
        LOG_WARN("push back equal expr info failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObRawExprSetUtils::add_expr_set(
                    alloc, raw_equal_set, equal_sets))) {
        LOG_WARN("failed to add expr set", K(ret));
      }
    }
  }
  return ret;
}

ObExprEqualSet *ObEqualAnalysis::new_equal_set()
{
  void *ptr = NULL;
  ObExprEqualSet *equal_set = NULL;
  if (NULL != (ptr = equal_set_alloc_.alloc(sizeof(ObExprEqualSet)))) {
    equal_set = new(ptr) ObExprEqualSet();
    if (!equal_sets_.add_last(equal_set)) {
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "add equal set failed");
      equal_set->~ObEqualSet();
      equal_set_alloc_.free(equal_set);
      equal_set = NULL;
    }
  }
  return equal_set;
}

int ObEqualAnalysis::find_equal_set(int64_t expr_idx, const ObRawExpr *new_expr, ObExprEqualSet *&equal_set_ret)
{
  int ret = OB_SUCCESS;
  const ObRawExpr* const *same_expr = NULL;
  DLIST_FOREACH(equal_set, equal_sets_) {
    if (OB_ISNULL(equal_set)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("equal set is null");
    } else if ((same_expr = equal_set->get_expr(expr_idx)) != NULL) {
      bool can_be = false;
      if (OB_FAIL(expr_can_be_add_to_equal_set(*equal_set, *same_expr, new_expr, can_be))) {
        LOG_WARN("check expr whether can be add to equal set failed", K(ret));
      } else if (can_be) {
        equal_set_ret = equal_set;
        break;
      }
    }
  }
  return ret;
}

//检查equal set中的元素和要加入equal set中的元素的compare type是否都一致
int ObEqualAnalysis::expr_can_be_add_to_equal_set(const ObExprEqualSet &equal_set,
                                                  const ObRawExpr *same_expr,
                                                  const ObRawExpr *new_expr,
                                                  bool &can_be) const
{
  can_be = true;
  int ret = OB_SUCCESS;
  //equal set中的元素至少是两个表达式
  if (equal_set.get_column_num() <= 1 || OB_ISNULL(same_expr) || OB_ISNULL(new_expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("arguments are invalid", K(equal_set.get_column_num()), K(same_expr), K(new_expr));
  } else {
    for (ObExprEqualSet::ColumnIterator iter = equal_set.column_begin();
        OB_SUCC(ret) && can_be && iter != equal_set.column_end(); ++iter) {
      const ObRawExpr *cur_expr = iter.get_expr_info();
      if (OB_ISNULL(cur_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("expr is null in equal set");
      } else if (cur_expr == same_expr) {
        //do nothing
      } else if (OB_FAIL(check_type_equivalent(*cur_expr, *same_expr, *new_expr, can_be))) {
        LOG_WARN("failed to check type equivalent", K(ret));
      }
    }
  }
  return ret;
}

//检查两个equal set是否能够被合并成一个equal set
//能够被合并的规则是，两个equal set有相同的表达式，并且不相同的表达式的元素都在另一个equal set中的compare type都相同
int ObEqualAnalysis::check_whether_can_be_merged(const ObExprEqualSet &equal_set,
                                                 const ObExprEqualSet &another_set,
                                                 bool &can_be) const
{
  can_be = true;
  int ret = OB_SUCCESS;
  const ObRawExpr* const *same_expr = NULL;
  if (!equal_set.intersect_equal_set(another_set)) {
    can_be = false;
  } else {
    for (ObExprEqualSet::ColumnIterator iter = another_set.column_begin();
        OB_ISNULL(same_expr) && iter != another_set.column_end(); ++iter) {
      same_expr = equal_set.get_expr(iter.get_expr_idx());
    }
  }
  for (ObExprEqualSet::ColumnIterator iter = another_set.column_begin();
      OB_SUCC(ret) && can_be && iter != another_set.column_end(); ++iter) {
    if (iter.get_expr_info() != *same_expr) {
      if (OB_FAIL(expr_can_be_add_to_equal_set(equal_set, *same_expr, iter.get_expr_info(), can_be))) {
        LOG_WARN("check expr whether can be add to equal set failed", K(ret));
      }
    }
  }
  return ret;
}

int ObEqualAnalysis::finish_feed()
{
  int ret = OB_SUCCESS;
  // merge intersected sets
  ObExprEqualSet *another_set = NULL;
  DLIST_FOREACH(equal_set, equal_sets_) {
    if (OB_ISNULL(equal_set)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("equal set is null", K(equal_set));
    } else {
      another_set = equal_set->get_next();
    }
    while (OB_SUCC(ret) && equal_sets_.get_header() != another_set) {
      bool can_be = false;
      if (OB_ISNULL(another_set)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("equal set is null", K(another_set));
      } else if (OB_FAIL(check_whether_can_be_merged(*equal_set, 
                                                     *another_set, 
                                                     can_be))) {
        LOG_WARN("check whether can be merged failed", K(ret));
      } else if (!can_be) {
        another_set = another_set->get_next();
      } else {
        // merge another_set into equal_set
        for (ObExprEqualSet::ColumnIterator it = another_set->column_begin();
            OB_SUCC(ret) && it != another_set->column_end(); ++it) {
          if (!equal_set->has_expr(it.get_expr_idx())) {
            if (OB_FAIL(equal_set->add_expr(it.get_expr_idx(), it.get_flag(), it.get_expr_info()))) {
              LOG_WARN("add expr to equal set failed", K(ret));
            }
          }
        }
        if (OB_SUCC(ret)) {
          ObExprEqualSet *tmp = another_set->get_next();
          equal_sets_.remove(another_set);
          equal_set_alloc_.free(another_set);
          another_set = tmp;
        }
      }
    }
  }
  return ret;
}

int ObEqualAnalysis::check_type_equivalent(
    const ObRawExpr &cur_expr, const ObRawExpr &same_expr,
    const ObRawExpr &new_expr, bool &can_be)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret), K(is_stack_overflow));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_stack_overflow));
  } else if (T_OP_ROW == cur_expr.get_expr_type()
             && T_OP_ROW == new_expr.get_expr_type()
             && T_OP_ROW == same_expr.get_expr_type()) {
    if (cur_expr.get_param_count() != same_expr.get_param_count()
        || same_expr.get_param_count() != new_expr.get_param_count()) {
      can_be = false;
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && can_be && i < cur_expr.get_param_count(); i++) {
        const ObRawExpr *c_cur = NULL;
        const ObRawExpr *c_same = NULL;
        const ObRawExpr *c_new = NULL;
        if (OB_ISNULL(c_cur = cur_expr.get_param_expr(i))
            || OB_ISNULL(c_same = same_expr.get_param_expr(i))
            || OB_ISNULL(c_new = new_expr.get_param_expr(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL expr", K(ret), K(c_cur), K(c_same), K(c_new));
        } else if (OB_FAIL(SMART_CALL(check_type_equivalent(*c_cur, *c_same, *c_new, can_be)))) {
          LOG_WARN("failed to check type in row");
        }
      }
    }
  } else if (T_OP_ROW != cur_expr.get_expr_type()
             && T_OP_ROW != new_expr.get_expr_type()
             && T_OP_ROW != same_expr.get_expr_type()) {
    if (OB_FAIL(ObRelationalExprOperator::is_equivalent(
                cur_expr.get_result_type(),
                same_expr.get_result_type(),
                new_expr.get_result_type(), can_be))) {
      LOG_WARN("check is equivalent failed", K(ret));
    }
  } else {
    can_be = false;
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
