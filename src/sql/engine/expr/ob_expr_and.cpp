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
#include "sql/engine/expr/ob_expr_and.h"
#include "lib/oblog/ob_log.h"
#include "share/object/ob_obj_cast.h"
#include "common/object/ob_obj_compare.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/resolver/expr/ob_raw_expr_deduce_type.h"
namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprAnd::ObExprAnd(ObIAllocator &alloc):
    ObLogicalExprOperator(alloc, T_OP_AND, N_AND, PARAM_NUM_UNKNOWN, NOT_ROW_DIMENSION)
{
  param_lazy_eval_ = true;
};

// scale以及precision也统一下
int ObExprAnd::calc_result_typeN(ObExprResType &type,
                                 ObExprResType *types_stack,
                                 int64_t param_num,
                                 ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(types_stack)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null types", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < param_num; i++) {
      if (types_stack[i].is_ext()) {
        ret = OB_ERR_EXPRESSION_WRONG_TYPE;
        LOG_WARN("PLS-00382: expression is of wrong type", K(ret), K(types_stack[i].get_type()));
      }
    }
  }

  if (OB_SUCC(ret)) {
    type.set_type(ObInt32Type);
    type.set_precision(DEFAULT_PRECISION_FOR_BOOL);
    type.set_scale(DEFAULT_SCALE_FOR_INTEGER);
  }
  return ret;
}

static int calc_and_expr2(const ObDatum &left, const ObDatum &right,
                          ObDatum &res)
{
  int ret = OB_SUCCESS;
  if (left.is_false() || right.is_false()) {
    res.set_false();
  } else if (left.is_null() || right.is_null()) {
    res.set_null();
  } else {
    res.set_true();
  }
  return ret;
}

int calc_and_exprN(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
  LOG_DEBUG("calc and common mode");
  int ret = OB_SUCCESS;
  ObDatum *tmp_res = NULL;
  ObDatum *child_res = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, tmp_res))) {
    LOG_WARN("eval arg 0 failed", K(ret));
  } else if (tmp_res->is_null()) {
    res_datum.set_null();
  } else {
    res_datum.set_bool(tmp_res->get_bool());
  }
  if (OB_SUCC(ret) && !res_datum.is_false()) {
    for (int64_t i = 1; OB_SUCC(ret) && i < expr.arg_cnt_; ++i) {
      if (OB_FAIL(expr.args_[i]->eval(ctx, child_res))) {
        LOG_WARN("eval arg failed", K(ret), K(i));
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(calc_and_expr2(res_datum, *child_res, res_datum))) {
          LOG_WARN("calc_and_expr2 failed", K(ret), K(i));
        } else if (res_datum.is_false()) {
          break;
        }
      }
    }
  }
  return ret;
}

int ObExprAnd::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  if (OB_ISNULL(rt_expr.args_) || OB_UNLIKELY(2 > rt_expr.arg_cnt_) ||
      OB_UNLIKELY(rt_expr.arg_cnt_ != raw_expr.get_param_count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("args_ is NULL or arg_cnt_ is invalid or raw_expr is invalid",
              K(ret), K(rt_expr), K(raw_expr));
  } else {
    rt_expr.eval_func_ = calc_and_exprN;
    rt_expr.eval_batch_func_ = eval_and_batch_exprN;
    rt_expr.eval_vector_func_ = eval_and_vector;
  }
  return ret;
}

int ObExprAnd::eval_and_batch_exprN(const ObExpr &expr, ObEvalCtx &ctx,
                                    const ObBitVector &skip, const int64_t batch_size)
{
  LOG_DEBUG("eval and batch mode", K(batch_size));
  int ret = OB_SUCCESS;
  ObDatum* results = expr.locate_batch_datums(ctx);
  if (OB_ISNULL(results)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr results frame is not init", K(ret));
  } else {
    ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
    ObBitVector &my_skip = expr.get_pvt_skip(ctx);
    my_skip.deep_copy(skip, batch_size);
    const int64_t real_param = batch_size - my_skip.accumulate_bit_cnt(batch_size);
    int64_t skip_cnt = 0; //记录一个batch中skip的数量， 所有参数被skip即可结束

    /*为省略对results的初始化操作以及减少对my_skip的写入，分成3段求值
    *args_[first]需要设置eval flags，设置初始值，为false设置skip
    *args_[middle]需要设置非true的result，和false的skip
    *args_[last]只需要设置非true的result
    */

    //eval first
    if (real_param == skip_cnt) {
    } else if (OB_FAIL(expr.args_[0]->eval_batch(ctx, my_skip, batch_size))) {
      LOG_WARN("failed to eval batch result args0", K(ret));
    } else if (expr.args_[0]->is_batch_result()) {
      ObDatum *curr_datum  = nullptr;
      ObDatum *datum_array = expr.args_[0]->locate_batch_datums(ctx);
      for (int64_t j = 0; OB_SUCC(ret) && j < batch_size; ++j) {
        if (my_skip.at(j)) {
          continue;
        }
        curr_datum = &datum_array[j];
        if (curr_datum->is_null()) {
          results[j].set_null();
        } else if (false == curr_datum->get_bool()) {
          results[j].set_bool(false);
          my_skip.set(j);
          ++skip_cnt;
        } else {
          results[j].set_bool(true);
        }
        eval_flags.set(j);
      }
    } else {
      ObDatum *curr_datum = &expr.args_[0]->locate_expr_datum(ctx);
      for (int64_t j = 0; OB_SUCC(ret) && j < batch_size; ++j) {
        if (my_skip.at(j)) {
          continue;
        }
        if (curr_datum->is_null()) {
          results[j].set_null();
        } else if (false == curr_datum->get_bool()) {
          results[j].set_bool(false);
          my_skip.set(j);
          ++skip_cnt;
        } else {
          results[j].set_bool(true);
        }
        eval_flags.set(j);
      }
    }

    //eval middle
    int64_t arg_idx = 1;
    for (; OB_SUCC(ret) && arg_idx < expr.arg_cnt_ - 1 && skip_cnt < real_param; ++arg_idx) {
      if (OB_FAIL(expr.args_[arg_idx]->eval_batch(ctx, my_skip, batch_size))) {
        LOG_WARN("failed to eval batch result", K(ret), K(arg_idx));
      } else if (expr.args_[arg_idx]->is_batch_result()) {
        ObDatum *curr_datum  = nullptr;
        ObDatum *datum_array = expr.args_[arg_idx]->locate_batch_datums(ctx);
        for (int64_t j = 0; OB_SUCC(ret) && j < batch_size; ++j) {
          if (my_skip.at(j)) {
            continue;
          }
          curr_datum = &datum_array[j];
          if (curr_datum->is_null()) {
            results[j].set_null();
          } else if (false == curr_datum->get_bool()) {
            results[j].set_bool(false);
            my_skip.set(j);
          } else {
            //do nothing
          }
        }
      } else {
        ObDatum *curr_datum = &expr.args_[arg_idx]->locate_expr_datum(ctx);
        for (int64_t j = 0; OB_SUCC(ret) && j < batch_size; ++j) {
          if (my_skip.at(j)) {
            continue;
          }
          if (curr_datum->is_null()) {
            results[j].set_null();
          } else if (false == curr_datum->get_bool()) {
            results[j].set_bool(false);
            my_skip.set(j);
          } else {
            //do nothing
          }
        }
      }
    }

    //eval last
    if (OB_FAIL(ret)) {
    } else if (real_param == skip_cnt) {
    } else if (OB_FAIL(expr.args_[arg_idx]->eval_batch(ctx, my_skip, batch_size))) {
      LOG_WARN("failed to eval batch result args0", K(ret));
    } else if (expr.args_[arg_idx]->is_batch_result()) {
      ObDatum *curr_datum  = nullptr;
      ObDatum *datum_array = expr.args_[arg_idx]->locate_batch_datums(ctx);
      for (int64_t j = 0; OB_SUCC(ret) && j < batch_size; ++j) {
        if (my_skip.at(j)) {
          continue;
        }
        curr_datum = &datum_array[j];
        if (curr_datum->is_null()) {
          results[j].set_null();
        } else if (false == curr_datum->get_bool()) {
          results[j].set_bool(false);
        } else {
          //do nothing
        }
      }
    } else {
      ObDatum *curr_datum = &expr.args_[arg_idx]->locate_expr_datum(ctx);
      for (int64_t j = 0; OB_SUCC(ret) && j < batch_size; ++j) {
        if (my_skip.at(j)) {
          continue;
        }
        if (curr_datum->is_null()) {
          results[j].set_null();
        } else if (false == curr_datum->get_bool()) {
          results[j].set_bool(false);
        } else {
          //do nothing
        }
      }
    }
  }
  return ret;
}

template <typename ArgVec, typename ResVec,
          ObExprAnd::EvalAndStage Stage>
static int inner_eval_and_vector(const ObExpr &expr,
                                 ObEvalCtx &ctx,
                                 ObBitVector &my_skip,
                                 const EvalBound &bound,
                                 const int64_t& arg_idx,
                                 int64_t& skip_cnt)
{
  int ret = OB_SUCCESS;
  ArgVec *curr_vec = static_cast<ArgVec *>(expr.args_[arg_idx]->get_vector(ctx));
  ResVec *results = static_cast<ResVec *>(expr.get_vector(ctx));
  for (int64_t j = bound.start(); OB_SUCC(ret) && j < bound.end(); ++j) {
    if (my_skip.at(j)) {
      continue;
    }
    if (curr_vec->is_null(j)) {
      results->set_null(j);
    } else if (false == curr_vec->get_bool(j)) {
      if (Stage == ObExprAnd::FIRST) {
        my_skip.set(j);
        ++skip_cnt;
      } else if (Stage == ObExprAnd::MIDDLE) {
        results->unset_null(j);
        my_skip.set(j);
        ++skip_cnt;
      } else {
        results->unset_null(j);
      }
      results->set_bool(j, false);
    } else {
      if (Stage == ObExprAnd::FIRST) {
        results->set_bool(j, true);
      }
    }
  }
  return ret;
}

static int dispatch_eval_and_vector(const ObExpr &expr,
                                    ObEvalCtx &ctx,
                                    ObBitVector &my_skip,
                                    const EvalBound &bound,
                                    const int64_t& arg_idx,
                                    int64_t& skip_cnt)
{
  int ret = OB_SUCCESS;
  VectorFormat res_format = expr.get_format(ctx);
  VectorFormat arg_format = expr.args_[arg_idx]->get_format(ctx);
  // When res_format == VEC_FIXED,
  // the template parameter cannot be passed as ObVectorBase.
  // If ObVectorBase is passed,
  // the condition typeid(ResVec) == typeid(IntegerFixedVec) will become invalid,
  // resulting in unset_null not being called and causing correctness issues.
  if (arg_idx == 0 &&
      arg_format == VEC_FIXED &&
      res_format == VEC_FIXED) {
    ret = inner_eval_and_vector<IntegerFixedVec, IntegerFixedVec, ObExprAnd::FIRST>(
                                        expr, ctx, my_skip, bound, arg_idx, skip_cnt);
  } else if (arg_idx == 0) {
    ret = inner_eval_and_vector<ObVectorBase, ObVectorBase, ObExprAnd::FIRST>(
                                  expr, ctx, my_skip, bound, arg_idx, skip_cnt);
  } else if (arg_idx == expr.arg_cnt_ - 1 &&
             arg_format == VEC_FIXED &&
             res_format == VEC_FIXED) {
    ret = inner_eval_and_vector<IntegerFixedVec, IntegerFixedVec, ObExprAnd::LAST>(
                                      expr, ctx, my_skip, bound, arg_idx, skip_cnt);
  } else if (arg_idx == expr.arg_cnt_ - 1) {
    ret = inner_eval_and_vector<ObVectorBase, ObVectorBase, ObExprAnd::LAST>(
                                expr, ctx, my_skip, bound, arg_idx, skip_cnt);
  } else if (arg_format == VEC_FIXED &&
             res_format == VEC_FIXED) {
    ret = inner_eval_and_vector<IntegerFixedVec, IntegerFixedVec, ObExprAnd::MIDDLE>(
                                        expr, ctx, my_skip, bound, arg_idx, skip_cnt);
  } else {
    ret = inner_eval_and_vector<ObVectorBase, ObVectorBase, ObExprAnd::MIDDLE>(
                                  expr, ctx, my_skip, bound, arg_idx, skip_cnt);
  }
  return ret;
}

int ObExprAnd::eval_and_vector(const ObExpr &expr,
                               ObEvalCtx &ctx,
                               const ObBitVector &skip,
                               const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  ObBitVector &my_skip = expr.get_pvt_skip(ctx);
  my_skip.deep_copy(skip, bound.start(), bound.end());
  const int64_t total_cnt = bound.end() - bound.start();
  // Record the number of skips in a batch,
  // end when all parameters are skipped.
  int64_t skip_cnt = my_skip.accumulate_bit_cnt(bound);
  EvalBound my_bound = bound;

  for (int64_t arg_idx = 0; OB_SUCC(ret) &&
       arg_idx < expr.arg_cnt_ && skip_cnt < total_cnt; ++arg_idx) {
    if (skip_cnt > 0) {
      my_bound.set_all_row_active(false);
    }
    if (OB_FAIL(expr.args_[arg_idx]->eval_vector(ctx, my_skip, my_bound))) {
      LOG_WARN("failed to eval vector result", K(ret), K(arg_idx));
    } else if (OB_FAIL(dispatch_eval_and_vector(
                expr, ctx, my_skip, my_bound, arg_idx, skip_cnt))){
      LOG_WARN("failed to dispatch eval vector and", K(ret),
      K(expr), K(ctx), K(my_bound), K(arg_idx), K(skip_cnt));
    }
  }

  // It would be more reasonable for eval_flags to be set after the calculation is completed
  // rather than setting it to 1 before the calculation.
  if (OB_SUCC(ret)) {
    ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
    eval_flags.bit_not(skip, my_bound);
  }

  return ret;
}

}
}
