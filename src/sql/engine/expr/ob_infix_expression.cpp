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

#include <limits>
#include "ob_infix_expression.h"
#include "lib/utility/ob_unify_serialize.h"

#include "sql/engine/expr/ob_expr_operator_factory.h"
#include "sql/engine/expr/ob_expr_regexp.h"
#include "share/ob_unique_index_row_transformer.h"
#include "share/ob_json_access_utils.h"
#include "sql/engine/expr/ob_sql_expression.h"
#include "lib/json_type/ob_json_tree.h"
#include "lib/json_type/ob_json_base.h"
#include "lib/json_type/ob_json_bin.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace sql
{

OB_DEF_SERIALIZE_SIZE(ObInfixExprItem)
{
  int64_t len = 0;
  BASE_ADD_LEN((ObInfixExprItem, ObPostExprItem));
  bool param_lazy = false;
  bool is_called_in_sql = IS_EXPR_OP(get_item_type()) ? get_expr_operator()->is_called_in_sql() : true;
  LST_DO_CODE(OB_UNIS_ADD_LEN, param_idx_, param_num_, param_lazy,
                               is_called_in_sql, is_boolean_);
  return len;
}

OB_DEF_SERIALIZE(ObInfixExprItem)
{
  int ret = OB_SUCCESS;
  BASE_SER((ObInfixExprItem, ObPostExprItem));
  bool param_lazy = false;
  bool is_called_in_sql = IS_EXPR_OP(get_item_type()) ? get_expr_operator()->is_called_in_sql() : true;
  LST_DO_CODE(OB_UNIS_ENCODE, param_idx_, param_num_, param_lazy,
                              is_called_in_sql, is_boolean_);
  return ret;
}

OB_DEF_DESERIALIZE(ObInfixExprItem)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObPostExprItem::deserialize(CURRENT_CONTEXT->get_arena_allocator(), buf, data_len, pos))) {
    LOG_WARN("expr item deserialize failed", K(ret));
  } else {
    bool param_lazy = false;
    bool is_called_in_sql = true;
    LST_DO_CODE(OB_UNIS_DECODE, param_idx_, param_num_, param_lazy,
                                is_called_in_sql, is_boolean_);
    if (IS_EXPR_OP(get_item_type())) {
      param_lazy_eval_ = get_expr_operator()->is_param_lazy_eval();
      get_expr_operator()->set_is_called_in_sql(is_called_in_sql);
    }
  }
  return ret;
}

// same with ob_write_expr_item in ob_postfix_expression.cpp
int ObInfixExprItem::deep_copy(common::ObIAllocator &alloc, const bool only_obj /* = false */)
{
  int ret = OB_SUCCESS;
  if (T_REF_COLUMN == item_type_) {
    // do nothing
  } else if (IS_DATATYPE_OR_QUESTIONMARK_OP(item_type_)) {
    ObObj obj;
    if (OB_FAIL(ob_write_obj(alloc, get_obj(), obj))) {
      LOG_WARN("copy object failed", K(ret));
    } else {
      *reinterpret_cast<ObObj *>(&v2_.v1_) = obj;
    }
  } else if (IS_EXPR_OP(item_type_)) {
    if (!only_obj) {
      ObExprOperatorFactory factory(alloc);
      ObExprOperator *op = NULL;
      if (OB_FAIL(factory.alloc(item_type_, op))) {
        LOG_WARN("alloc expr operator failed", K(ret));
      } else if (OB_ISNULL(op)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL operator returned", K(ret));
      } else if (OB_FAIL(op->assign(*get_expr_operator()))) {
        LOG_WARN("expr operator assign failed", K(ret));
      } else {
        v2_.op_ = op;
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unknown expr type", K(ret), K(item_type_));
  }
  return ret;
}

ObInfixExpression::ObInfixExpression(ObIAllocator &alloc, const int64_t expr_cnt)
  : alloc_(alloc), exprs_()
{
  UNUSED(expr_cnt);
}

ObInfixExpression::~ObInfixExpression()
{
}

void ObInfixExpression::reset()
{
  exprs_.reset(alloc_);
}

OB_DEF_SERIALIZE_SIZE(ObInfixExpression)
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN_ARRAY(exprs_.get_ptr(), exprs_.count());
  return len;
}

OB_DEF_SERIALIZE(ObInfixExpression)
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE_ARRAY(exprs_.get_ptr(), exprs_.count());
  return ret;
}

OB_DEF_DESERIALIZE(ObInfixExpression)
{
  int ret = OB_SUCCESS;
  int64_t count = 0;
  OB_UNIS_DECODE(count);
  if (OB_SUCC(ret) && count > 0) {
    if (OB_FAIL(exprs_.init(count, alloc_))) {
      LOG_WARN("failed to init array", K(ret));
    } else {
      ObInfixExprItem tmp_expr_item;
      for (int64_t i = 0; OB_SUCC(ret) && i < count; i++) {
        OB_UNIS_DECODE(tmp_expr_item);
        if (OB_FAIL(ret)) {
          // do mothing
        } else if (OB_FAIL(exprs_.push_back(tmp_expr_item))) {
          LOG_WARN("failed to push back item", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObInfixExpression::set_item_count(const int64_t count)
{
  int ret = OB_SUCCESS;
  if (count < 0 || count >= std::numeric_limits<uint16_t>::max()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid item count", K(ret), K(count));
  }
  return exprs_.init(count, alloc_);
}

int ObInfixExpression::assign(const ObInfixExpression &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    if (other.exprs_.count() > 0) {
      if (OB_FAIL(exprs_.reserve(other.exprs_.count(), alloc_))) {
        LOG_WARN("array reserve failed", K(ret));
      } else {
        FOREACH_CNT_X(item, other.exprs_, OB_SUCC(ret)) {
          if (OB_FAIL(exprs_.push_back(*item))) {
            LOG_WARN("array push back failed", K(ret));
          } else {
            if (OB_FAIL(exprs_.at(exprs_.count() - 1).deep_copy(alloc_))) {
              LOG_WARN("deep copy expr op failed", K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObInfixExpression::add_expr_item(const ObInfixExprItem &item)
{
  int ret = OB_SUCCESS;
  const bool copy_obj_only = true;
  // check T_OP_AGG_PARAM_LIST must be the root node of expression
  if (T_OP_AGG_PARAM_LIST == item.get_item_type() && 0 != exprs_.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("T_OP_AGG_PARAM_LIST not root node of expression", K(ret));
  } else if (OB_FAIL(exprs_.push_back(item))) {
    LOG_WARN("array push back failed", K(ret));
  } else {
    ObInfixExprItem &new_item = exprs_.at(exprs_.count() - 1);
    if (OB_FAIL(new_item.deep_copy(alloc_, copy_obj_only))) {
      LOG_WARN("expr deep copy failed", K(ret));
    } else if (IS_EXPR_OP(new_item.get_item_type())) {
      if (OB_ISNULL(new_item.get_expr_operator())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr operator is NULL", K(ret));
      } else {
        if (new_item.get_expr_operator()->is_param_lazy_eval()) {
          new_item.set_param_lazy_eval();
        }
      }
    }
  }
  return ret;
}

// FREE_EVAL_STACK must called in same code block
#define ALLOC_EVAL_STACK \
  const int64_t _stack_size = exprs_.count(); \
  const int64_t _stack_alloc_size = _stack_size < STACK_ALLOC_STACK_SIZE ? _stack_size : 0; \
  char _buf[_stack_alloc_size * sizeof(ObObj)]; \
  bool _use_global_stack = false; \
  ObObj *stack = reinterpret_cast<ObObj *>(_buf); \
  if (OB_UNLIKELY(0 == _stack_alloc_size)) { \
    int64_t &_cur_top = global_stack_top(); \
    auto *_calc_stack = global_stack(); \
    if (OB_ISNULL(_calc_stack)) { \
      ret = OB_ALLOCATE_MEMORY_FAILED; \
      LOG_WARN("get thread local stack failed", K(ret)); \
    } else if (OB_UNLIKELY(_stack_size + _cur_top > ObPostfixExpressionCalcStack::STACK_SIZE)) { \
      if (NULL == (stack = static_cast<ObObj *>(expr_ctx.calc_buf_->alloc( \
                      sizeof(ObObj) * _stack_size)))) { \
        ret = OB_ALLOCATE_MEMORY_FAILED; \
        LOG_WARN("get thread local stack failed", K(ret)); \
      } \
    } else { \
      _use_global_stack = true; \
      stack = _calc_stack->stack_ + _cur_top; \
      _cur_top += _stack_size; \
    } \
  } \
  /* set stack to expr_ctx */ \


#define FREE_EVAL_STACK \
  if (OB_UNLIKELY(0 == _stack_alloc_size)) { \
    if (_use_global_stack) { \
      int64_t &_cur_top = global_stack_top();\
      _cur_top -= _stack_size; \
    } \
  }

#define SAVE_EVAL_CTX(ctx, stack, r1, r2) \
    do { \
      ctx.infix_expr_ = this; \
      ctx.stack_ = stack; \
      ctx.row1_ = r1; \
      ctx.row2_ = r2; \
    } while (false)

int ObInfixExpression::calc(common::ObExprCtx &expr_ctx,
    const common::ObNewRow &row, common::ObObj &val) const
{
  int ret = OB_SUCCESS;
#ifdef NDEBUG
  // prefetch for read
  __builtin_prefetch(&exprs_.at(0), 0);
#endif
  if (OB_LIKELY(1 == exprs_.count())
             && OB_LIKELY(exprs_.at(0).can_get_value_directly())) {
    const ObObj *value = NULL;
    if (OB_FAIL(exprs_.at(0).get_item_value_directly(*expr_ctx.phy_plan_ctx_, row, value))
        || OB_ISNULL(value)) {
      ret = COVER_SUCC(OB_ERR_UNEXPECTED);
      LOG_WARN("get item value directly failed", K(ret), K(row), K_(exprs));
    } else if (value->is_ext() && ObSqlExpressionUtil::expand_array_params(expr_ctx, *value, value)) {
      LOG_WARN("expand array params failed", K(ret));
    } else {
      val= *value;
    }
  } else if (OB_UNLIKELY(exprs_.at(0).get_item_type() == T_OP_SHADOW_UK_PROJECT)) {
    if (OB_FAIL(uk_fast_project(expr_ctx, row, val))) {
      LOG_WARN("fail to uk fast project", K(ret));
    }
  } else {
    ALLOC_EVAL_STACK;

    SAVE_EVAL_CTX(expr_ctx, stack, &row, NULL);

    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(exprs_.count() <= 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(exprs_.count()));
    } else if (OB_FAIL(eval(expr_ctx, row, stack, 0))) {
      LOG_WARN("expr evaluate failed", K(ret));
    } else {
      val = *stack;
    }

    FREE_EVAL_STACK;
  }

  return ret;
}

int ObInfixExpression::calc(common::ObExprCtx &expr_ctx, const common::ObNewRow &row1,
    const common::ObNewRow &row2, common::ObObj &val) const
{
  int ret = OB_SUCCESS;

  ALLOC_EVAL_STACK;

  SAVE_EVAL_CTX(expr_ctx, stack, &row1, &row2);

  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(exprs_.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(exprs_.count()));
  } else if (OB_FAIL(eval(expr_ctx, row1, stack, 0))) {
    LOG_WARN("expr evaluate failed", K(ret));
  } else {
    val = *stack;
  }

  FREE_EVAL_STACK;

  return ret;
}

int ObInfixExpression::calc_row(common::ObExprCtx &expr_ctx, const common::ObNewRow &row,
                                ObItemType aggr_fun, const ObExprResType &res_type,
                                common::ObNewRow &res_row) const
{
  int ret = OB_SUCCESS;

  ALLOC_EVAL_STACK;

  SAVE_EVAL_CTX(expr_ctx, stack, &row, NULL);

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(res_row.cells_) || OB_UNLIKELY(exprs_.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(exprs_.count()));
  } else {
    const ObInfixExprItem &item = exprs_.at(0);
    if (OB_UNLIKELY(T_OP_AGG_PARAM_LIST == item.get_item_type())) {
      for (int64_t i = 0; i < item.get_param_num() && OB_SUCC(ret); i++) {
        if (OB_FAIL(eval(expr_ctx, row, stack, item.get_param_idx() + i))) {
          LOG_WARN("eval expr failed", K(ret), K(i));
        } else if(aggr_fun == T_FUN_JSON_OBJECTAGG && i == 1) {
          bool is_bool = false;
          ObObj *tmp = stack + item.get_param_idx() + i;
          if (OB_FAIL(get_param_is_boolean(expr_ctx, *tmp, is_bool))) {
            LOG_WARN("get_param_is_boolean failed", K(ret));
          } else if (is_bool) {
            ObJsonBoolean j_bool(tmp->get_bool());
            ObIJsonBase *j_base = &j_bool;
            ObString raw_bin; //
            if (OB_FAIL(ObJsonWrapper::get_raw_binary(j_base, raw_bin, expr_ctx.calc_buf_))) {
              LOG_WARN("get result binary failed", K(ret), K(*j_base));
            } else {
              // bool type convert to json bool, need to know outside has lob header or not
              bool has_lob_header = true;
              ObTextStringObObjResult text_result(ObJsonType, nullptr, tmp, has_lob_header);
              if (OB_FAIL(text_result.init(raw_bin.length(), expr_ctx.calc_buf_))) {
                LOG_WARN("init lob result failed");
              } else if (OB_FAIL(text_result.append(raw_bin.ptr(), raw_bin.length()))) {
                LOG_WARN("failed to append realdata", K(ret), K(raw_bin), K(text_result));
              } else {
                text_result.set_result();
              }
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_UNLIKELY(item.get_param_num() > res_row.count_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("to small result row count",  K(ret), K(item), K(res_row.count_));
        } else {
          MEMCPY(res_row.cells_, stack + item.get_param_idx(),
              sizeof(ObObj) * item.get_param_num());
        }
      }
    } else {
      if (OB_FAIL(eval(expr_ctx, row, stack, 0))) {
        LOG_WARN("expr evaluate failed", K(ret));
      } else if (aggr_fun == T_FUN_JSON_ARRAYAGG) {
        bool is_bool = false;
        if (OB_FAIL(get_param_is_boolean(expr_ctx, *stack, is_bool))) {
          LOG_WARN("get_param_is_boolean failed", K(ret));
        } else if (is_bool) {
          ObJsonBoolean j_bool(stack->get_bool());
          ObIJsonBase *j_base = &j_bool;
          ObString raw_bin;
          if (OB_FAIL(ObJsonWrapper::get_raw_binary(j_base, raw_bin, expr_ctx.calc_buf_))) {
            LOG_WARN("get result binary failed", K(ret), K(*j_base));
          } else {
            // bool type convert to json bool, need to know outside has lob header or not
            bool has_lob_header = true;
            ObTextStringObObjResult text_result(ObJsonType, nullptr, stack, has_lob_header);
            if (OB_FAIL(text_result.init(raw_bin.length(), expr_ctx.calc_buf_))) {
              LOG_WARN("init lob result failed");
            } else if (OB_FAIL(text_result.append(raw_bin.ptr(), raw_bin.length()))) {
              LOG_WARN("failed to append realdata", K(ret), K(raw_bin), K(text_result));
            } else {
              text_result.set_result();
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        res_row.cells_[0] = *stack;
      }
    }
  }

  FREE_EVAL_STACK;

  return ret;
}

#undef ALLOC_EVAL_STACK
#undef SAVE_EVAL_CTX
#undef FREE_EVAL_STACK

int ObInfixExpression::eval(common::ObExprCtx &ctx, const common::ObNewRow &row,
    common::ObObj *stack, const int64_t pos) const
{
  int ret = OB_SUCCESS;
  const ObInfixExprItem &item = exprs_.at(pos);
  if (item.get_param_num() > 0 ) {
    // prefetch for read
    __builtin_prefetch(&exprs_.at(item.get_param_idx()), 0);
    if (!item.is_param_lazy_eval()) {
      for (int64_t i = 0; i < item.get_param_num() && OB_SUCC(ret); i++) {
        if (OB_FAIL(eval(ctx, row, stack, item.get_param_idx() + i))) {
          LOG_WARN("eval expr failed", K(ret), K(i));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    // T_OP_AGG_PARAM_LIST is special processed in calc_row()
    if (OB_UNLIKELY(T_OP_AGG_PARAM_LIST == item.get_item_type())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("agg param list not expected here", K(ret));
    } else if (T_REF_COLUMN == item.get_item_type()) {
      const int64_t idx = item.get_column();
      if (OB_LIKELY(idx >= 0 && OB_LIKELY(idx < row.count_) && OB_LIKELY(NULL != row.cells_))) {
        stack[pos] = row.cells_[idx];
        if (OB_UNLIKELY(ObBitType == stack[pos].get_type() &&
                        -1 != item.get_accuracy().get_precision())) {
          //use object scale to store bit length
          stack[pos].set_scale(item.get_accuracy().get_precision());
        } else if (OB_UNLIKELY(ob_is_text_tc(stack[pos].get_type()))) {
          //do nothing ...
        } else if (-1 != item.get_accuracy().get_scale()) {
          stack[pos].set_scale(item.get_accuracy().get_scale());
        }
      } else if (NULL != ctx.row2_ && NULL != ctx.row2_->cells_
          && idx >= row.count_ && idx - row.count_ < ctx.row2_->count_) {
        stack[pos] = ctx.row2_->cells_[idx - row.count_];
        if (OB_UNLIKELY(ObBitType == stack[pos].get_type() &&
                        -1 != item.get_accuracy().get_precision())) {
          //use object scale to store bit length
          stack[pos].set_scale(item.get_accuracy().get_precision());
        } else if (-1 != item.get_accuracy().get_scale()) {
          stack[pos].set_scale(item.get_accuracy().get_scale());
        }
      } else {
        ret = OB_INDEX_OUT_OF_RANGE;
        LOG_WARN("cell index out of range", K(idx), K(row), K(ctx.row2_));
      }
    } else if (T_QUESTIONMARK == item.get_item_type()) {
      const ObObj *obj = NULL;
      if (OB_FAIL(item.get_indirect_const(*ctx.phy_plan_ctx_, obj)) || OB_ISNULL(obj)) {
        ret = COVER_SUCC(OB_ERR_UNEXPECTED);
        LOG_WARN("get obj failed", K(ret));
      } else if (obj->is_ext() && OB_FAIL(ObSqlExpressionUtil::expand_array_params(ctx, *obj, obj))) {
        LOG_WARN("expand array params failed", K(ret));
      } else {
        stack[pos] = *obj;
      }
    } else if (IS_DATATYPE_OP(item.get_item_type())) {
      stack[pos] = item.get_obj();
    } else if (IS_EXPR_OP(item.get_item_type())) {
      const ObExprOperator *op = item.get_expr_operator();
      if (OB_ISNULL(op)) {
        ret = OB_BAD_NULL_ERROR;
        LOG_WARN("expr operator is NULL", K(ret));
      } else if (OB_FAIL(op->eval(ctx, stack[pos],
          stack + item.get_param_idx(), item.get_param_num()))) {
        LOG_WARN("expr evaluate failed", K(ret),
            "type", item.get_item_type(),
            "type_name", get_type_name(item.get_item_type()));
      } else  {
        // For expression op, we need set scale info after it has been evaluated.
        if (OB_UNLIKELY(ObBitType == stack[pos].get_type() &&
                        -1 != item.get_accuracy().get_precision())) {
          // use object scale to store bit length
          stack[pos].set_scale(item.get_accuracy().get_precision());
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid expression operator", K(ret),
          "type", item.get_item_type(),
          "type_name", get_type_name(item.get_item_type()));
    }
  }
  // 此处在参数eval失败的情况下, 将该参数值设置为NULL, 因为该参数值
  // ObObj之前没有初始化过, 如果不强制设置为NULL, 后面日志栈中如果出现
  // 打印该参数的情况可能导致core
  if (OB_FAIL(ret)) {
    stack[pos].set_null();
  }
  return ret;
}

// copy && paste from ob_postfix_expression.cpp
int ObInfixExpression::generate_idx_for_regexp_ops(int16_t &cur_regexp_op_count)
{
  /*please note that, cur_regexp_op_count is a reference of a variable in ObCodeGeneratorImpl
   * it is used as a total counter here
   * This function maybe be called many times within different exprs
   * so, you should NOT set cur_regexp_op_count to be 0 here.
   */
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs_.count(); ++i) {
    if (T_OP_REGEXP == exprs_.at(i).get_item_type()) {
      ObExprRegexp *regexp_op = static_cast<ObExprRegexp *>(exprs_.at(i).get_expr_operator());
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

// copy && paste from ob_postfix_expression.cpp
bool ObInfixExpression::is_equijoin_cond(int64_t &c1, int64_t &c2,
    common::ObObjType &cmp_type, common::ObCollationType &cmp_cs_type,
    bool &is_null_safe) const
{
  bool ret = false;
  if (exprs_.count() == 3) {
    if ((exprs_[0].get_item_type() == T_OP_EQ || exprs_[0].get_item_type() == T_OP_NSEQ)
        && exprs_[1].get_item_type() == T_REF_COLUMN
        && exprs_[2].get_item_type() == T_REF_COLUMN) {
      c1 = exprs_[1].get_column();
      c2 = exprs_[2].get_column();
      cmp_type = exprs_[0].get_expr_operator()->get_result_type().get_calc_type();
      cmp_cs_type = exprs_[0].get_expr_operator()->get_result_type().get_calc_collation_type();
      if (exprs_[0].get_item_type() == T_OP_NSEQ) {
        is_null_safe = true;
      }
      ret = true;
    }
  }
  return ret;
}

// copy && paste from ob_postfix_expression.cpp
int ObInfixExpression::uk_fast_project(common::ObExprCtx &expr_ctx, const common::ObNewRow &row,
    common::ObObj &result_val) const
{
  int ret = OB_SUCCESS;
  int64_t expr_cnt = exprs_.count();
  const int64_t unique_key_cnt = expr_cnt - 2;
  if (OB_UNLIKELY(expr_cnt < 3)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("expr cnt is invalid", K(expr_cnt));
  } else if (OB_UNLIKELY(!expr_ctx.row_ctx_.is_uk_checked_)) {
    ObArray<int64_t> projector;
    for (int64_t i = 1; OB_SUCC(ret) && i < unique_key_cnt + 1; ++i) {
      if (OB_UNLIKELY(exprs_.at(i).get_item_type() != T_REF_COLUMN)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("post expr item is invalid", K(exprs_.at(i)));
      } else {
        const int64_t col_idx = exprs_.at(i).get_column();
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
    const ObPostExprItem &item = exprs_.at(unique_key_cnt + 1);
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

} // end namespace sql
} // end namespace oceanbase
