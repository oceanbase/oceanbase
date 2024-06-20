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
#include "sql/engine/expr/ob_expr_or.h"
#include "lib/oblog/ob_log.h"
#include "share/object/ob_obj_cast.h"
#include "sql/engine/expr/ob_expr_json_func_helper.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprOr::ObExprOr(ObIAllocator &alloc)
    : ObLogicalExprOperator(alloc, T_OP_OR, N_OR, PARAM_NUM_UNKNOWN, NOT_ROW_DIMENSION)
{
  param_lazy_eval_ = true;
}

int ObExprOr::calc_result_typeN(ObExprResType &type,
                                ObExprResType *types_stack,
                                int64_t param_num,
                                ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  UNUSED(types_stack);
  UNUSED(param_num);
  int ret = OB_SUCCESS;
  //just keep enumset as origin
  type.set_int32();
  type.set_precision(DEFAULT_PRECISION_FOR_BOOL);
  type.set_scale(DEFAULT_SCALE_FOR_INTEGER);
  return ret;
}

int ObExprOr::calc_res_with_one_param_null(common::ObObj &res,
                                           const common::ObObj &left,
                                           const common::ObObj &right,
                                           common::ObExprCtx &expr_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY((!left.is_null()) || right.is_null())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected params", K(ret), K(left), K(right));
  } else {
    //left is null while right is not null. evaluate the right obj
    bool value = false;

    EXPR_SET_CAST_CTX_MODE(expr_ctx);
    if (OB_FAIL(ObObjEvaluator::is_true(right, expr_ctx.cast_mode_ | CM_NO_RANGE_CHECK, value))) {
      LOG_WARN("fail to evaluate obj1", K(right), K(ret));
      //ugly. but compatible with mysql.
      if (OB_ERR_TRUNCATED_WRONG_VALUE_FOR_FIELD == ret) {
        ret = OB_SUCCESS;
        res.set_null();
      }
      // By design, compatible with mysql and oracle:
      // null or false == NULL. null or true == true.
      // see "NULL and the three-valued logic" https://en.wikipedia.org/wiki/Null_(SQL)#Comparisons_with_NULL_and_the_three-valued_logic_(3VL)
    } else if (value) {
      res.set_int32(static_cast<int32_t>(true));
    } else {
      res.set_null();
    }
  }
  return ret;
}

// create table t1 as (select null or ''); -> NULL;
// create table t1 as (select 1 or ''); -> 1;
// create table t1 as (select 0 or ''); -> 0;
/*static int calc_with_one_empty_str(const ObDatum &in_datum, ObDatum &out_datum)
{
  int ret = OB_SUCCESS;
  if (in_datum.is_null()) {
    out_datum.set_null();
  } else {
    out_datum.set_bool(in_datum.get_bool());
  }
  return ret;
}*/

static int calc_or_expr2(const ObDatum &left, const ObDatum &right, ObDatum &res)
{
  int ret = OB_SUCCESS;
  if (left.is_true() || right.is_true()) {
    res.set_true();
  } else if (left.is_null() || right.is_null()) {
    res.set_null();
  } else {
    res.set_false();
  }
  return ret;
}

// 特殊处理：
// 1. select null or ''; -> or结果为0
//    原因：针对select语句，cast_mode会自动加上WARN_ON_FAIL，所以空串转为number时，
//    结果为0，错误码被覆盖，上述语句就会返回0
// 2. create table t1 as (select null or ''); -> or的结果为NULL
//    原因：针对非select/explain语句，cast_mode没有WARN_ON_FAIL，所以上面空串转number时，
//    报OB_ERR_TRUNCATED_WRONG_VALUE_FOR_FIELD，但是为了兼容MySQL，这里会进行特殊处理，
//    将错误码覆盖，并且or的结果为NULL而非0
//
// 上面说的针对空串的特殊处理都是针对MySQL的，因为Oracle模式下or两边的子节点都必须是有
// 布尔语义的表达式，不会直接是空串
int calc_or_exprN(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
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
  if (OB_SUCC(ret) && !res_datum.is_true()) {
    for (int64_t i = 1; OB_SUCC(ret) && i < expr.arg_cnt_; ++i) {
      if (OB_FAIL(expr.args_[i]->eval(ctx, child_res))) {
        LOG_WARN("eval arg failed", K(ret), K(i));
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(calc_or_expr2(res_datum, *child_res, res_datum))) {
          LOG_WARN("calc_or_expr2 failed", K(ret), K(i));
        } else if (res_datum.is_true()) {
          break;
        }
      }
    }
  }
  return ret;
}

int ObExprOr::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
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
    rt_expr.eval_func_ = calc_or_exprN;
    rt_expr.eval_batch_func_ = eval_or_batch_exprN;
    rt_expr.eval_vector_func_ = eval_or_vector;
  }
  return ret;
}

int ObExprOr::eval_or_batch_exprN(const ObExpr &expr, ObEvalCtx &ctx,
                                    const ObBitVector &skip, const int64_t batch_size)
{
  LOG_DEBUG("eval or batch mode", K(batch_size));
  int ret = OB_SUCCESS;
  ObDatum *results = expr.locate_batch_datums(ctx);
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
    *args_[first]需要设置eval flags， 设置初始值， 为true设置skip
    *args_[middle]需要设置非false的result， 为true设置skip
    *args_[last]只需要设置非false的result*/

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
        } else if (true == curr_datum->get_bool()) {
          results[j].set_bool(true);
          my_skip.set(j);
        } else {
          results[j].set_bool(false);
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
        } else if (true == curr_datum->get_bool()) {
          results[j].set_bool(true);
          my_skip.set(j);
        } else {
          results[j].set_bool(false);
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
        ObDatum *curr_datum = nullptr;
        ObDatum *datum_array = expr.args_[arg_idx]->locate_batch_datums(ctx);
        for (int64_t j = 0; OB_SUCC(ret) && j < batch_size; ++j) {
          if (my_skip.at(j)) {
            continue;
          }
          curr_datum = &datum_array[j];
          if (curr_datum->is_null()) {
            results[j].set_null();
          } else if (true == curr_datum->get_bool()) {
            results[j].set_bool(true);
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
          } else if (true == curr_datum->get_bool()) {
            results[j].set_bool(true);
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
        } else if (true == curr_datum->get_bool()) {
          results[j].set_bool(true);
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
        } else if (true == curr_datum->get_bool()) {
          results[j].set_bool(true);
        } else {
          //do nothing
        }
      }
    }
  }
  return ret;
}

template <typename ArgVec, typename ResVec,
          ObExprOr::EvalOrStage Stage>
static int inner_eval_or_vector(const ObExpr &expr,
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
    } else if (true == curr_vec->get_bool(j)) {
      if (Stage == ObExprOr::FIRST) {
        my_skip.set(j);
        ++skip_cnt;
      } else if (Stage == ObExprOr::MIDDLE) {
        results->unset_null(j);
        my_skip.set(j);
        ++skip_cnt;
      } else {
        results->unset_null(j);
      }
      results->set_bool(j, true);
    } else {
      if (Stage == ObExprOr::FIRST) {
        results->set_bool(j, false);
      }
    }
  }
  return ret;
}

static int dispatch_eval_or_vector(const ObExpr &expr,
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
    ret = inner_eval_or_vector<IntegerFixedVec, IntegerFixedVec, ObExprOr::FIRST>(
                                        expr, ctx, my_skip, bound, arg_idx, skip_cnt);
  } else if (arg_idx == 0) {
    ret = inner_eval_or_vector<ObVectorBase, ObVectorBase, ObExprOr::FIRST>(
                                  expr, ctx, my_skip, bound, arg_idx, skip_cnt);
  } else if (arg_idx == expr.arg_cnt_ - 1 &&
             arg_format == VEC_FIXED &&
             res_format == VEC_FIXED) {
    ret = inner_eval_or_vector<IntegerFixedVec, IntegerFixedVec, ObExprOr::LAST>(
                                      expr, ctx, my_skip, bound, arg_idx, skip_cnt);
  } else if (arg_idx == expr.arg_cnt_ - 1) {
    ret = inner_eval_or_vector<ObVectorBase, ObVectorBase, ObExprOr::LAST>(
                                expr, ctx, my_skip, bound, arg_idx, skip_cnt);
  } else if (arg_format == VEC_FIXED &&
             res_format == VEC_FIXED) {
    ret = inner_eval_or_vector<IntegerFixedVec, IntegerFixedVec, ObExprOr::MIDDLE>(
                                        expr, ctx, my_skip, bound, arg_idx, skip_cnt);
  } else {
    ret = inner_eval_or_vector<ObVectorBase, ObVectorBase, ObExprOr::MIDDLE>(
                                  expr, ctx, my_skip, bound, arg_idx, skip_cnt);
  }
  return ret;
}

int ObExprOr::eval_or_vector(const ObExpr &expr,
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
    } else if (OB_FAIL(dispatch_eval_or_vector(
                expr, ctx, my_skip, my_bound, arg_idx, skip_cnt))){
      LOG_WARN("failed to dispatch eval vector or", K(ret),
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

