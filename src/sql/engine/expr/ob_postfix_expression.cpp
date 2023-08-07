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

#define USING_LOG_PREFIX SQL_ENG

#include "share/ob_unique_index_row_transformer.h"
#include "sql/engine/expr/ob_postfix_expression.h"
#include "sql/ob_result_set.h"
#include "sql/engine/expr/ob_expr_regexp.h"
#include "sql/engine/expr/ob_expr_like.h"
#include "lib/utility/ob_hang_fatal_error.h"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace sql
{

template <typename AllocatorT>
int ob_write_expr_item(AllocatorT &alloc, const ObPostExprItem &src,
                       ObPostExprItem &dst, ObWriteExprItemFlag flag)
{
  int ret = OB_SUCCESS;
  ObItemType item_type = src.get_item_type();
  dst.set_item_type(item_type);
  ObExprOperatorFactory factory(alloc);
  if (T_REF_COLUMN == item_type) {
    dst.set_accuracy(src.get_accuracy());
    ret = dst.set_column(src.get_column());
  } else if (IS_DATATYPE_OR_QUESTIONMARK_OP(item_type)) {
    ObObj tmp_obj;
    if (OB_SUCC(ob_write_obj(alloc, src.get_obj(), tmp_obj))) {
      dst.set_accuracy(src.get_accuracy());
      ret = dst.assign(tmp_obj);
    }
  } else if (IS_EXPR_OP(item_type)) {
    if (NEW_OP_WHEN_COPY == flag) {
      ObExprOperator *op = NULL;
      if (OB_FAIL(factory.alloc(item_type, op))) {
        LOG_WARN("fail to alloc expr_op", K(ret));
      } else if (OB_ISNULL(op)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("no memory to alloc op", K(ret), K(item_type));
      } else if (OB_ISNULL(src.get_expr_operator())) {
        ret = OB_BAD_NULL_ERROR;
        LOG_WARN("src expr op is null", K(ret));
      } else if (OB_FAIL(op->assign(*src.get_expr_operator()))) {
        LOG_WARN("deep copy op failed", K(ret));
      } else if (OB_FAIL(dst.assign(op))) {
        LOG_WARN("failed to assign dst expr item", K(ret));
      }
    } else {
      if (OB_ISNULL(src.get_expr_operator())) {
        ret = OB_BAD_NULL_ERROR;
        LOG_WARN("src expr op is null", K(ret));
      } else if (OB_FAIL(dst.assign(const_cast<ObPostExprItem &>(src).get_expr_operator()))) {
        LOG_WARN("failed to assign dst expr item", K(ret));
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unknown expr item to serialize", K(item_type));
  }
  return ret;
}


int ObPostExprItem::assign(const common::ObObj &obj)
{
  int ret = OB_SUCCESS;
  ObItemType item_type = static_cast<ObItemType>((obj.get_type()));
  if (OB_UNLIKELY(!IS_DATATYPE_OR_QUESTIONMARK_OP(item_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid obj type", K(ret), K(obj));
  } else {
    new(&v2_.v1_) ObObj(obj);
    item_type_ = item_type;
  }
  return ret;
}

int ObPostExprItem::set_column(int64_t index)
{
  int ret = OB_SUCCESS;
  if (index < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid index", K(ret), K(index));
  } else {
    item_type_ = T_REF_COLUMN;
    v2_.cell_index_ = index;
  }
  return ret;
}

int ObPostExprItem::assign(ObExprOperator *op)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(op)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("op is NULL", K(ret));
  } else {
    item_type_ = op->get_type();
    v2_.op_ = op;
  }
  return ret;
}

int ObPostExprItem::assign(ObItemType item_type)
{
  int ret = OB_SUCCESS;
  if (T_INVALID == item_type) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("item type is invalid", K(ret));
  } else {
    item_type_ = item_type;
  }
  return ret;
}

/* for unittest only */
int ObPostExprItem::set_op(ObIAllocator &alloc, const char *op_name, ObExprOperator *&op)
{
  int ret = OB_SUCCESS;
  ObExprOperatorType type = ObExprOperatorFactory::get_type_by_name(ObString::make_string(op_name));
  ObExprOperatorFactory factory(alloc);
  if (T_INVALID == type) {
    ret = OB_ERR_FUNCTION_UNKNOWN;
    LOG_WARN("unknown operator/function name", K(ret), K(op_name));
  } else if (OB_FAIL(factory.alloc(type, op))) {
    LOG_WARN("fail to alloc expr_op", K(ret));
  } else if (OB_ISNULL(op)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to alloc expr operator", K(ret));
  } else {
    ret = assign(op);
  }
  return ret;
}

/* for unittest only */
/* so need NOT normalize */
void ObPostExprItem::set_op(ObIAllocator &alloc, const char *op_name, int32_t real_param_num)
{
  ObExprOperatorType type = ObExprOperatorFactory::get_type_by_name(ObString::make_string(op_name));
  if (T_INVALID == type) {
    LOG_ERROR_RET(OB_INVALID_ARGUMENT, "invaid op type", K(type));
    right_to_die_or_duty_to_live();
  } else {
    ObExprOperator *op = NULL;
    ObExprOperatorFactory factory(alloc);
    factory.alloc(type, op);
    if (OB_ISNULL(op)) {
    } else {
      op->set_real_param_num(real_param_num);
      assign(op);
    }
  }
}

int64_t ObPostExprItem::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (IS_DATATYPE_OP(item_type_)) {
    J_OW(J_KV(N_CONST, get_obj(),
              N_ACCURACY, accuracy_));
  } else {
    switch (item_type_) {
      case T_REF_COLUMN: {
        J_OW(J_KV(N_COLUMN_INDEX, get_column(),
                  N_ACCURACY, accuracy_));
        break;
      }
      case T_QUESTIONMARK: {
        J_OW(J_KV(N_PARAM, get_obj().get_int(),
                  N_ACCURACY, accuracy_));
        break;
      }
      default: {
        if (IS_EXPR_OP(item_type_)) {
          J_OW(J_KV(N_OP, *get_expr_operator()));
        } else {
          LOG_WARN_RET(OB_ERR_UNEXPECTED, "unknown item", K_(item_type));
        }
        break;
      }
    } // end switch
  }

  return pos;
}

DEFINE_SERIALIZE(ObPostExprItem)
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(item_type_);
  if (OB_SUCC(ret)) {
    if (T_REF_COLUMN == item_type_) {
      OB_UNIS_ENCODE(v2_.cell_index_);
      OB_UNIS_ENCODE(accuracy_);
    } else if (IS_DATATYPE_OR_QUESTIONMARK_OP(item_type_)) {
      ObObj tmp = get_obj();
      OB_UNIS_ENCODE(tmp);
      OB_UNIS_ENCODE(accuracy_);
    } else if (IS_EXPR_OP(item_type_)) {
      OB_UNIS_ENCODE(*v2_.op_);
    } else {
      ret = OB_UNKNOWN_OBJ;
      LOG_ERROR("Unknown expr item to serialize", K(ret), K_(item_type));
    }
  }
  return ret;
}

int ObPostExprItem::deserialize(ObIAllocator &alloc,
                                const char *buf,
                                const int64_t data_len,
                                int64_t &pos)
{
  int ret = OB_SUCCESS;
  OB_UNIS_DECODE(item_type_);
  if (OB_SUCC(ret)) {
    if (T_REF_COLUMN == item_type_) {
      OB_UNIS_DECODE(v2_.cell_index_);
      OB_UNIS_DECODE(accuracy_);
    } else if (IS_DATATYPE_OR_QUESTIONMARK_OP(item_type_)) {
      ObObj tmp;
      OB_UNIS_DECODE(tmp);
      ObObj local_mem_obj;
      if (OB_FAIL(deep_copy_obj(alloc, tmp, local_mem_obj))) {
        LOG_WARN("failed to deep copy obj", K(ret));
      } else {
        new(&v2_.v1_) ObObj(local_mem_obj);
        OB_UNIS_DECODE(accuracy_);
      }
    } else if (IS_EXPR_OP(item_type_)) {
      ObExprOperatorFactory factory(alloc);
      if (OB_FAIL(factory.alloc(item_type_, v2_.op_))) {
        LOG_WARN("fail to alloc expr_op", K(ret));
      } else if (OB_ISNULL(v2_.op_)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("failed to allc expr operator", K(ret), K_(item_type));
      } else {
        OB_UNIS_DECODE(*v2_.op_);
      }
    } else {
      ret = OB_UNKNOWN_OBJ;
      LOG_ERROR("Unknown expr item to deserialize", K(ret), K_(item_type));
    }
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObPostExprItem)
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN(item_type_);
  if (T_REF_COLUMN == item_type_) {
    OB_UNIS_ADD_LEN(v2_.cell_index_);
    OB_UNIS_ADD_LEN(accuracy_);
  } else if (IS_DATATYPE_OR_QUESTIONMARK_OP(item_type_)) {
    ObObj tmp = get_obj();
    OB_UNIS_ADD_LEN(tmp);
    OB_UNIS_ADD_LEN(accuracy_);
  } else if (IS_EXPR_OP(item_type_)) {
    OB_UNIS_ADD_LEN(*v2_.op_);
  } else {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "Unknown expr item to serialize", K_(item_type));
  }
  return len;
}

////////////////////////////////////////////////////////////////

ObPostfixExpression::ObPostfixExpression(ObIAllocator &alloc, int64_t item_count)
    : post_exprs_(),
      str_buf_(alloc),
      output_column_count_(1) //默认为1
{
  UNUSED(item_count);
}

ObPostfixExpression::~ObPostfixExpression()
{
  reset();
}

void ObPostfixExpression::reset()
{
  data_clear();
}

void ObPostfixExpression::data_clear()
{
  post_exprs_.reset(str_buf_);
  output_column_count_ = 1;
}

int ObPostfixExpression::assign(const ObPostfixExpression &other)
{
  int ret = OB_SUCCESS;
  if (&other != this) {
    data_clear();
    if (OB_FAIL(post_exprs_.reserve(other.post_exprs_.count(), str_buf_))) {
      LOG_WARN("failed to reserve", K(ret), "item_count", other.post_exprs_.count());
    } else {
      ObPostExprItem item_clone;
      for (int64_t i = 0; OB_SUCC(ret) && i < other.post_exprs_.count(); ++i) {
        const ObPostExprItem &item = other.post_exprs_[i];
        if (OB_FAIL(ob_write_expr_item(str_buf_, item, item_clone, NEW_OP_WHEN_COPY))) {
          LOG_WARN("failed to deep copy expr item", K(ret));
        } else if (OB_FAIL(post_exprs_.push_back(item_clone))) {
          LOG_WARN("failed to push into array", K(ret));
        }
      } // end for
      output_column_count_ = other.output_column_count_;
    }
  }
  return ret;
}

int ObPostfixExpression::add_expr_item(const ObPostExprItem &item)
{
  int ret = OB_SUCCESS;
  ObPostExprItem item_clone;
  if (OB_FAIL(ob_write_expr_item(str_buf_, item, item_clone, NO_NEW_OP_WHEN_COPY))) {
    LOG_WARN("failed to deep copy expr item", K(ret));
  } else if (OB_FAIL(post_exprs_.push_back(item_clone))) {
    LOG_WARN("failed to push into array", K(ret));
  }
  return ret;
}

int ObPostfixExpression::generate_idx_for_regexp_ops(int16_t &cur_regexp_op_count)
{
  /*please note that, cur_regexp_op_count is a reference of a variable in ObCodeGeneratorImpl
   * it is used as a total counter here
   * This function maybe be called many times within different exprs
   * so, you should NOT set cur_regexp_op_count to be 0 here.
   */
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < post_exprs_.count(); ++i) {
    if (T_OP_REGEXP == post_exprs_.at(i).get_item_type()) {
      ObExprRegexp *regexp_op = static_cast<ObExprRegexp *>(post_exprs_.at(i).get_expr_operator());
      if (OB_ISNULL(regexp_op)) {
        ret = OB_BAD_NULL_ERROR;
        LOG_WARN("regexp op is null", K(ret));
      } else {
        regexp_op->set_regexp_idx(cur_regexp_op_count++);
      }
    }
  }
  return ret;
}

int ObPostfixExpression::calc(common::ObExprCtx &expr_ctx, const common::ObNewRow &row,
                              ObObj &result_val) const
{
  int ret = OB_SUCCESS;
  int64_t last_idx = post_exprs_.count() - 1;
  if (OB_LIKELY(1 == post_exprs_.count()) && OB_LIKELY(post_exprs_.at(0).can_get_value_directly())) {
    const ObObj *value = NULL;
    if (OB_FAIL(post_exprs_.at(0).get_item_value_directly(*expr_ctx.phy_plan_ctx_, row, value)) || OB_ISNULL(value)) {
      ret = COVER_SUCC(OB_ERR_UNEXPECTED);
      LOG_WARN("get item value directly failed", K(ret), K(row), K_(post_exprs));
    } else if (!expr_ctx.is_pre_calculation_) {
      //in pre calculation, if only one question mark expr, return the question mark expression
      //result directly, even if it is a param array, this is to avoid constructing a param array
      //in the pre calculation
      if (OB_FAIL(ObSqlExpressionUtil::expand_array_params(expr_ctx, *value, value))) {
        LOG_WARN("expand array params failed", K(ret), KPC(value));
      }
    }
    if (OB_SUCC(ret)) {
      result_val = *value;
    }
  } else if (OB_UNLIKELY(post_exprs_.at(last_idx).get_item_type() == T_OP_SHADOW_UK_PROJECT)) {
    //对于shadow unique key project表达式走优化路径
    if (OB_FAIL(uk_fast_project(expr_ctx, row, result_val))) {
      LOG_WARN("fail to do uk fast project", K(ret));
    }
  } else {
    ObNewRow result_row;
    result_row.cells_ = &result_val;
    result_row.count_ = 1;
    if (OB_FAIL(calc_result_row(expr_ctx, row, result_row))) {
      LOG_WARN("fail to calc result list", K(ret));
    } else {
      result_val = result_row.cells_[0];
    }
  }
  return ret;
}

int ObPostfixExpression::calc(common::ObExprCtx &expr_ctx, const common::ObNewRow &row1,
                              const common::ObNewRow &row2,
                              ObObj &result_val) const
{
  int ret = OB_SUCCESS;
  ObNewRow result_row;
  result_row.cells_ = &result_val;
  result_row.count_ = 1;
  if (OB_FAIL(calc_result_row(expr_ctx, row1, row2, result_row))) {
    LOG_WARN("fail to calc result list", K(ret), K(row1), K(row2));
  } else {
    result_val = result_row.cells_[0];
  }
  return ret;
}

inline int ObPostfixExpression::uk_fast_project(ObExprCtx &expr_ctx, const ObNewRow &row, ObObj &result_val) const
{
  int ret = OB_SUCCESS;
  int64_t expr_cnt = post_exprs_.count();
  const int64_t unique_key_cnt = expr_cnt - 2;
  if (OB_UNLIKELY(expr_cnt < 3)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("expr cnt is invalid", K(expr_cnt));
  } else if (OB_UNLIKELY(!expr_ctx.row_ctx_.is_uk_checked_)) {
    ObArray<int64_t> projector;
    for (int64_t i = 0; OB_SUCC(ret) && i < unique_key_cnt; ++i) {
      if (OB_UNLIKELY(post_exprs_.at(i).get_item_type() != T_REF_COLUMN)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("post expr item is invalid", K(post_exprs_.at(i)));
      } else {
        const int64_t col_idx = post_exprs_.at(i).get_column();
        if (OB_FAIL(projector.push_back(col_idx))) {
          LOG_WARN("fail to push back projector", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObUniqueIndexRowTransformer::check_need_shadow_columns(row, static_cast<ObCompatibilityMode>(THIS_WORKER.get_compatibility_mode()), unique_key_cnt,
          &projector, expr_ctx.row_ctx_.is_uk_cnt_null_))) {
        LOG_WARN("fail to check need shadow columns", K(ret));
      }
    }
    expr_ctx.row_ctx_.is_uk_checked_ = true;
  }
  if (OB_SUCC(ret)) {
    const ObPostExprItem &item = post_exprs_.at(unique_key_cnt);
    if (!expr_ctx.row_ctx_.is_uk_cnt_null_) {
      result_val.set_null();
    } else if (OB_UNLIKELY(item.get_item_type() != T_REF_COLUMN)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("post expr item is invalid", K(item));
    } else {
      int64_t col_idx = item.get_column();
      if (OB_UNLIKELY(col_idx < 0) || OB_UNLIKELY(col_idx >= row.count_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column index is invalid", K(col_idx), K(row.count_));
      } else {
        result_val = row.cells_[col_idx];
      }
    }
  }
  return ret;
}

int ObPostfixExpression::calc_result_row(ObExprCtx &expr_ctx, const common::ObNewRow &row,
                                         ObNewRow &result_row) const
{
  int ret = OB_SUCCESS;
  // fast path for single ref column or question mark.
  if (1 == post_exprs_.count()
      && (post_exprs_[0].get_item_type() == T_REF_COLUMN || post_exprs_[0].get_item_type() == T_QUESTIONMARK)) {
    const ObPostExprItem &item = post_exprs_[0];
    if (item.get_item_type() == T_REF_COLUMN) {
      int64_t idx = item.get_column();
      if (OB_UNLIKELY(idx < 0 || idx >= row.count_)) {
        ret = OB_ARRAY_OUT_OF_RANGE;
        LOG_WARN("cell index out of range", K(ret), K(idx), K_(row.count));
      } else {
        result_row.cells_[0] = row.cells_[idx];
      }
    } else { // T_QUESTIONMARK
      const ObObj *obj = NULL;
      if (OB_FAIL(item.get_indirect_const(*expr_ctx.phy_plan_ctx_, obj))) {
        LOG_WARN("fail to get obj", K(ret));
      } else if (!expr_ctx.is_pre_calculation_) {
        //in pre calculation, if only one question mark expr, return the question mark expression
        //result directly, even if it is a param array, this is to avoid constructing a param array
        //in the pre calculation
        if (OB_FAIL(ObSqlExpressionUtil::expand_array_params(expr_ctx, *obj, obj))) {
          LOG_WARN("expand array params failed", K(ret), KPC(obj));
        }
      }
      if (OB_SUCC(ret)) {
        result_row.cells_[0] = *obj;
      }
    }
  } else {
    // infix_expr_ is need for parameter lazy evaluation in infix expression,
    // set to NULL to avoid evaluate in postfix expression.
    expr_ctx.infix_expr_ = NULL;

    int64_t idx = 0;
    RLOCAL(int64_t, stack_top);
    //在每次计算的开始，应该保留栈的开始位置，如果计算是正常结束，计算栈里面的值会自然的被弹空
    //但是如果计算是异常退出，压栈的元素不能被弹出，需要在退出的时候将栈置为开始的位置,不然会导致栈异常增长
    int64_t stack_start = stack_top;
    ObObj *stack = NULL;
    auto *the_stack = GET_TSI_MULT(ObPostfixExpressionCalcStack, 1);
    if (OB_ISNULL(the_stack)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("no memory", K(ret));
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("no memory or deep copy has failed", K(ret));
    } else if (OB_ISNULL(result_row.cells_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result row cells is null", K(ret));
    } else {
      result_row.cells_[0].set_type(ObMaxType);
      stack = the_stack->stack_;
      const int64_t expr_len = post_exprs_.count();
      for (idx = 0; OB_SUCC(ret) && idx < expr_len; ++idx) {
        if (OB_UNLIKELY(stack_top >= ObPostfixExpressionCalcStack::STACK_SIZE)) {
          ret = OB_SIZE_OVERFLOW;
          LOG_WARN("calculation stack overflow", K(+stack_top), K(idx), K(expr_len));
        } else {
          const ObPostExprItem &item = post_exprs_[idx];
          switch (item.get_item_type()) {
            case T_OP_AGG_PARAM_LIST: {
              if (OB_FAIL(calc_agg_param_list(result_row, item, stack, stack_top))) {
                LOG_WARN("failed to calc agg param list", K(ret), K_(post_exprs));
              }
              break;
            }
            case T_REF_COLUMN: {
              if (OB_FAIL(calc_ref_column(row, result_row, item, stack, stack_top))) {
                LOG_WARN("failed to calc ref column", K(ret), K_(post_exprs));
              }
              break;
            }
            case T_QUESTIONMARK: {
              if (OB_FAIL(calc_question_mark(expr_ctx, result_row, item, stack, stack_top))) {
                LOG_WARN("failed to calc question mark", K(ret), K_(post_exprs));
              }
              break;
            }
            default: {
              if (OB_FAIL(calc_other_op(expr_ctx, item, stack, stack_top))) {
                LOG_WARN("failed to calc other op", K(ret), K_(post_exprs));
              }
              break;
            }
          } // end switch
        } // end else
      } // end for

      if (OB_SUCC(ret) && ObMaxType == result_row.cells_[0].get_type()) {
        if (OB_UNLIKELY(idx != expr_len || stack_top <= 0
            || 1 != result_row.count_ || 1 != output_column_count_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("idx not equal to expr_len, or stack top less than 0, or cell or column count is not 1",
              K(ret), K(idx), K(expr_len), K(+stack_top), K(result_row.count_), K_(output_column_count));
        } else {
          result_row.cells_[0] = stack[--stack_top];
        }
      }
    }
    stack_top = stack_start;
  }
  return ret;
}

int ObPostfixExpression::calc_result_row(ObExprCtx &expr_ctx, const common::ObNewRow &row1,
                                         const common::ObNewRow &row2, ObNewRow &result_row) const
{
  int ret = OB_SUCCESS;
  int64_t idx = 0;
  RLOCAL(int64_t, stack_top);
  //在每次计算的开始，应该保留栈的开始位置，如果计算是正常结束，计算栈里面的值会自然的被弹空
  //但是如果计算是异常退出，压栈的元素不能被弹出，需要在退出的时候将栈置为开始的位置,不然会导致栈异常增长
  int64_t stack_start = stack_top;
  ObObj *stack = NULL;
  auto *the_stack = GET_TSI_MULT(ObPostfixExpressionCalcStack, 2);
  if (OB_ISNULL(the_stack)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("no memory");
  }

  // infix_expr_ is need for parameter lazy evaluation in infix expression,
  // set to NULL to avoid evaluate in postfix expression.
  expr_ctx.infix_expr_ = NULL;

  if (OB_FAIL(ret)) {
    LOG_WARN("no memory or deep copy has failed", K(ret));
  } else if (OB_ISNULL(result_row.cells_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("result row cells is null", K(ret));
  } else {
    result_row.cells_[0].set_type(ObMaxType);
    stack = the_stack->stack_;
    const int64_t expr_len = post_exprs_.count();
    for (idx = 0; OB_SUCC(ret) && idx < expr_len; ++idx) {
      if (OB_UNLIKELY(stack_top >= ObPostfixExpressionCalcStack::STACK_SIZE)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("calculation stack overflow", K(+stack_top), K(idx), K(expr_len));
      } else {
        const ObPostExprItem &item = post_exprs_[idx];
        switch (item.get_item_type()) {
          case T_OP_AGG_PARAM_LIST: {
            if (OB_FAIL(calc_agg_param_list(result_row, item, stack, stack_top))) {
              LOG_WARN("failed to calc agg param list", K(ret));
            }
            break;
          }
          case T_REF_COLUMN: {
            if (OB_FAIL(calc_ref_column(row1, row2, result_row, item, stack, stack_top))) {
              LOG_WARN("failed to calc ref column", K(ret));
            }
            break;
          }
          case T_QUESTIONMARK: {
            if (OB_FAIL(calc_question_mark(expr_ctx, result_row, item, stack, stack_top))) {
              LOG_WARN("failed to calc question mark", K(ret));
            }
            break;
          }
          default: {
            if (OB_FAIL(calc_other_op(expr_ctx, item, stack, stack_top))) {
              LOG_WARN("failed to calc other op", K(ret));
            }
            break;
          }
        } // end switch
      }//end of else
    }//end of for

    if (OB_SUCC(ret) && ObMaxType == result_row.cells_[0].get_type()) {
      if (OB_UNLIKELY(idx != expr_len || stack_top <= 0
                      || 1 != result_row.count_ || 1 != output_column_count_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("idx not equal to expr_len, or stack top less than 0, or cell or column count is not 1",
                 K(ret), K(idx), K(expr_len), K(+stack_top), K(result_row.count_), K_(output_column_count));
      } else {
        result_row.cells_[0] = stack[--stack_top];
      }
    }
  }
  stack_top = stack_start;
  return ret;
}

OB_INLINE int ObPostfixExpression::calc_agg_param_list(ObNewRow &result_row,
                                                       const ObPostExprItem &item,
                                                       ObObj *stack, int64_t stack_top) const
{
  int ret = OB_SUCCESS;
  //根据dimension取出栈中的元素填充到result_row中
  const ObExprOperator *agg_param_list = item.get_expr_operator();
  if (OB_ISNULL(stack) || OB_ISNULL(agg_param_list) || OB_ISNULL(result_row.cells_)
      || OB_UNLIKELY(stack_top < 0 || stack_top >= ObPostfixExpressionCalcStack::STACK_SIZE)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("something is null, or stack_top out of range",
             K(ret), K(stack), K(agg_param_list), K_(result_row.cells), K(stack_top));
  } else {
    int32_t param_num = agg_param_list->get_real_param_num() * agg_param_list->get_row_dimension();
    if (OB_UNLIKELY(!(&item == &post_exprs_.at(post_exprs_.count() - 1)
                      && T_OP_AGG_PARAM_LIST == agg_param_list->get_type()
                      && ObExprOperator::NOT_ROW_DIMENSION != agg_param_list->get_row_dimension()
                      && param_num <= stack_top
                      && param_num == result_row.count_
                      && param_num == output_column_count_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("something is invalid", K(ret),
               K(agg_param_list->get_type()), K(agg_param_list->get_row_dimension()),
               K(param_num), K(stack_top), K_(result_row.count), K_(output_column_count));
    } else {
      for (int64_t i = 0, j = stack_top - param_num; OB_SUCC(ret) && i < param_num; ++i, ++j) {
        result_row.cells_[i] = stack[j];
      }
    }
  }
  return ret;
}


OB_INLINE int ObPostfixExpression::calc_ref_column(const ObNewRow &row,
                                                   ObNewRow &result_row, const ObPostExprItem &item,
                                                   ObObj *stack, int64_t &stack_top) const
{
  int ret = OB_SUCCESS;
  int64_t cell_idx = item.get_column();
  if (OB_ISNULL(stack) || OB_ISNULL(row.cells_) || OB_ISNULL(result_row.cells_)
      || OB_UNLIKELY(stack_top < 0 || stack_top >= ObPostfixExpressionCalcStack::STACK_SIZE)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("something is null, or stack_top out of range",
             K(ret), K(stack), K_(row.cells), K_(result_row.cells), K(stack_top));
  } else if (OB_UNLIKELY(cell_idx < 0 || cell_idx >= row.count_)) {
    ret = OB_ARRAY_OUT_OF_RANGE;
    LOG_WARN("cell_index out of range", K(ret), K(cell_idx), K_(row.count));
  } else {
    stack[stack_top] = row.cells_[cell_idx];
    if (OB_UNLIKELY(ObBitType == row.cells_[cell_idx].get_type())) {
      //use object scale to store bit length
      stack[stack_top].set_scale(item.get_accuracy().get_precision());
    } else if (-1 != item.get_accuracy().get_scale()) {
      stack[stack_top].set_scale(item.get_accuracy().get_scale());
    }
    ++stack_top;
  }
  return ret;
}

OB_INLINE int ObPostfixExpression::calc_ref_column(const ObNewRow &row1, const ObNewRow &row2,
                                                   ObNewRow &result_row, const ObPostExprItem &item,
                                                   ObObj *stack, int64_t &stack_top) const
{
  int ret = OB_SUCCESS;
  int64_t cell_idx = item.get_column();
  if (OB_ISNULL(stack) || OB_ISNULL(row1.cells_) || OB_ISNULL(row2.cells_) ||
      OB_ISNULL(result_row.cells_)
      || OB_UNLIKELY(stack_top < 0 || stack_top >= ObPostfixExpressionCalcStack::STACK_SIZE)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("something is null, or stack_top out of range",
             K(ret), K(stack), K_(row1.cells), K_(row2.cells), K_(result_row.cells), K(stack_top));
  } else if (OB_UNLIKELY(cell_idx < 0 || cell_idx >= row1.count_ + row2.count_)) {
    ret = OB_ARRAY_OUT_OF_RANGE;
    LOG_WARN("cell_index out of range", K(ret), K(cell_idx), K_(row1.count), K_(row2.count));
  } else {
    ObObj &obj = (cell_idx < row1.count_) ? row1.cells_[cell_idx] : row2.cells_[cell_idx - row1.count_];
    if (1 == post_exprs_.count()) {
      if (OB_UNLIKELY(!(1 == result_row.count_ && 1 == output_column_count_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("cell or column count is not 1", K(ret), K(result_row.count_), K_(output_column_count));
      } else {
        result_row.cells_[0] = obj;
      }
    } else {
      stack[stack_top] = obj;
      if (OB_UNLIKELY(ObBitType == obj.get_type())) {
        //use object scale to store bit length
        stack[stack_top++].set_scale(item.get_accuracy().get_precision());
      } else {
        stack[stack_top++].set_scale(item.get_accuracy().get_scale());
      }
    }
  }
  return ret;
}

OB_INLINE int ObPostfixExpression::calc_question_mark(ObExprCtx &expr_ctx,
                                                      ObNewRow &result_row,
                                                      const ObPostExprItem &item,
                                                      ObObj *stack,
                                                      int64_t &stack_top) const
{
  int ret = OB_SUCCESS;
  const ObObj *obj = NULL;
  if (OB_ISNULL(stack) || OB_ISNULL(result_row.cells_) || OB_ISNULL(expr_ctx.phy_plan_ctx_)
      || OB_UNLIKELY(stack_top < 0 || stack_top >= ObPostfixExpressionCalcStack::STACK_SIZE)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("something is null, or stack_top out of range",
             K(ret), K(stack), K_(result_row.cells), K(stack_top), K_(expr_ctx.phy_plan_ctx));
  } else if (OB_FAIL(item.get_indirect_const(*expr_ctx.phy_plan_ctx_, obj))) {
    LOG_WARN("failed to get value obj", K(ret));
  } else if (OB_FAIL(ObSqlExpressionUtil::expand_array_params(expr_ctx, *obj, obj))) {
    LOG_WARN("expand array params failed", K(ret), KPC(obj));
  } else {
    stack[stack_top] = *obj;
    stack_top++;
  }
  return ret;
}

OB_INLINE int ObPostfixExpression::calc_other_op(ObExprCtx &expr_ctx, const ObPostExprItem &item,
                                                 ObObj *stack, int64_t &stack_top) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stack)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("stack is null", K(ret));
  } else if (IS_DATATYPE_OP(item.get_item_type())) {
    stack[stack_top] = item.get_obj();
    stack_top++;
  } else if (IS_EXPR_OP(item.get_item_type())) {
    const ObExprOperator *expr_op = item.get_expr_operator();
    if (OB_ISNULL(expr_op)) {
      ret = OB_BAD_NULL_ERROR;
      LOG_WARN("expr_op is null", K(ret), K(item.get_item_type()));
    } else if (OB_FAIL(expr_op->call(stack, stack_top, expr_ctx))) {
      LOG_WARN("failed to call expr operator", K(ret),
               "expr_name", item.get_expr_operator()->get_name(), K(item));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid expression operator", K(ret), "item_type", get_type_name(item.get_item_type()));
  }
  return ret;
}

bool ObPostfixExpression::is_equijoin_cond(int64_t &c1, int64_t &c2,
                                           ObObjType &cmp_type, ObCollationType &cmp_cs_type,
                                           bool &is_null_safe) const
{
  bool ret = false;
  if (post_exprs_.count() == 3) {
    if ((post_exprs_[2].get_item_type() == T_OP_EQ || post_exprs_[2].get_item_type() == T_OP_NSEQ)
        && post_exprs_[1].get_item_type() == T_REF_COLUMN
        && post_exprs_[0].get_item_type() == T_REF_COLUMN) {
      c1 = post_exprs_[0].get_column();
      c2 = post_exprs_[1].get_column();
      cmp_type = post_exprs_[2].get_expr_operator()->get_result_type().get_calc_type();
      cmp_cs_type = post_exprs_[2].get_expr_operator()->get_result_type().get_calc_collation_type();
      if (post_exprs_[2].get_item_type() == T_OP_NSEQ) {
        is_null_safe = true;
      }
      ret = true;
    }
  }
  return ret;
}

DEFINE_SERIALIZE(ObPostfixExpression)
{
  int ret = OB_SUCCESS;
  int64_t N = post_exprs_.count();
  OB_UNIS_ENCODE_ARRAY(post_exprs_.get_ptr(), N);
  OB_UNIS_ENCODE(output_column_count_);
  return ret;
}

DEFINE_DESERIALIZE(ObPostfixExpression)
{
  int ret = OB_SUCCESS;
  data_clear();
  int64_t item_count = 0;
  ObPostExprItem item;
  OB_UNIS_DECODE(item_count);
  if (OB_SUCC(ret)) {
    if (item_count > 0 && OB_FAIL(post_exprs_.reserve(item_count, str_buf_))) {
      LOG_WARN("failed to reserve", K(ret), K(item_count));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < item_count; ++i) {
        if (OB_FAIL(item.deserialize(str_buf_, buf, data_len, pos))) {
          LOG_WARN("fail to deserialize post_item", K(ret));
        } else if (OB_FAIL(post_exprs_.push_back(item))) {
          LOG_WARN("failed to push into array", K(ret));
        }
      } // end for
      OB_UNIS_DECODE(output_column_count_);
    }
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObPostfixExpression)
{
  int64_t len = 0;
  int64_t N = post_exprs_.count();
  OB_UNIS_ADD_LEN_ARRAY(post_exprs_.get_ptr(), N);
  OB_UNIS_ADD_LEN(output_column_count_);
  return len;
}

int64_t ObPostfixExpression::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_ARRAY_START();
  for (int i = 0; i < post_exprs_.count(); ++i) {
    if (0 != i) {
      J_COMMA();
    }
    BUF_PRINTO(post_exprs_[i]);
  } // end for
  J_ARRAY_END();
  return pos;
}
}
}
