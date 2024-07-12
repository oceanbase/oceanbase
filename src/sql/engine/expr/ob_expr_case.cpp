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
#include "sql/engine/expr/ob_expr_case.h"
#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

typedef int (*CheckIsMatchFunc)(const ObDatum *when_datum, bool &match_when);

ObExprCase::ObExprCase(ObIAllocator &alloc)
    : ObExprOperator(alloc, T_OP_CASE, N_CASE, MORE_THAN_ONE, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
  disable_operand_auto_cast();
  param_lazy_eval_ = true;
}

ObExprCase::~ObExprCase()
{
}

/*
 * NOTE：calc_result_typeN中param_num只涵盖了then/else表达式，未涵盖when表达式
 */
int ObExprCase::calc_result_typeN(ObExprResType &type,
                                  ObExprResType *types_stack,
                                  int64_t param_num,
                                  ObExprTypeCtx &type_ctx) const
{
  // case
  //  when 10 then expr1
  //  when 11 then expr2
  //  [else expr3]
  int ret = OB_SUCCESS;
  if (OB_ISNULL(types_stack)) {
    LOG_WARN("null types");
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(param_num < 3 || param_num % 2 == 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param num is not correct", K(param_num));
  } else { //param_num >=3 and param_num is odd

    /* in order to be compatible with mysql
     * both in ob_expr_case.cpp and ob_expr_arg_case.cpp
     * types_stack includes the condition exprs.
     * In expr_case, there is no arg param expr compared with expr_arg_case
     */
    const int64_t cond_type_count = param_num / 2;
    const int64_t val_type_count = param_num - cond_type_count;
    const ObLengthSemantics default_length_semantics = (OB_NOT_NULL(type_ctx.get_session()) ? type_ctx.get_session()->get_actual_nls_length_semantics() : LS_BYTE);

    if (OB_FAIL(aggregate_result_type_for_case(
                  type,
                  types_stack + cond_type_count,
                  val_type_count,
                  type_ctx.get_coll_type(),
                  lib::is_oracle_mode(),
                  default_length_semantics,
                  true, false,
                  is_called_in_sql_))) {
      LOG_WARN("failed to aggregate result type");
    } else {
      ObExprOperator::calc_result_flagN(type, types_stack + cond_type_count, val_type_count);
    }

    if (OB_SUCC(ret)) {
      for (int64_t i = 0; i < cond_type_count; ++i) {
        const ObObjType cond_type = types_stack[i].get_type();
        const ObObjTypeClass cond_tc = ob_obj_type_class(cond_type);
        if (ObIntTC == cond_tc || ObUIntTC == cond_tc
            || ObNumberTC == cond_tc || ObDecimalIntTC == cond_tc
            || ObNullTC == cond_tc) {
          types_stack[i].set_calc_type(cond_type);
          types_stack[i].set_calc_collation(types_stack[i]);
        } else {
          types_stack[i].set_calc_type(ObDoubleType);
          types_stack[i].set_calc_collation_type(CS_TYPE_BINARY);
          types_stack[i].set_calc_collation_level(CS_LEVEL_NUMERIC);
        }
      }

      bool is_expr_integer_type = (ob_is_int_tc(type.get_type()) ||
                                   ob_is_uint_tc(type.get_type()));
      for (int64_t i = cond_type_count; OB_SUCC(ret) && i < param_num; ++i) {
        bool is_arg_integer_type = (ob_is_int_tc(types_stack[i].get_type()) ||
                                    ob_is_uint_tc(types_stack[i].get_type()));
        if ((is_arg_integer_type && is_expr_integer_type) ||
            ObNullType == types_stack[i].get_type()) {
          // see ObExprCoalesce::calc_result_typeN
          types_stack[i].set_calc_meta(types_stack[i].get_obj_meta());
        } else {
          types_stack[i].set_calc_meta(type.get_obj_meta());
          if (ObDecimalIntType == type.get_obj_meta().get_type() ||
              ob_is_double_type(type.get_obj_meta().get_type())) {
            types_stack[i].set_calc_accuracy(type.get_accuracy());
          }
        }
      }
    }
  }
  return ret;
}

int ObExprCase::cg_expr(ObExprCGCtx &op_cg_ctx,
                        const ObRawExpr &raw_expr,
                        ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(op_cg_ctx);

  const ObCaseOpRawExpr &case_expr = dynamic_cast<const ObCaseOpRawExpr&>(raw_expr);
  // 新引擎下case表达式when expr一定要返回int/null，即when expr一定是布尔语义的表达式
  for (int64_t i = 0; OB_SUCC(ret) && i < case_expr.get_when_expr_size(); ++i) {
    const ObRawExpr *when_expr = case_expr.get_when_param_expr(i);
    const ObObjType &when_expr_res_type = when_expr->get_result_type().get_type();
    if (OB_UNLIKELY(ObNullType != when_expr_res_type &&
                    !ob_is_integer_type(when_expr_res_type))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("when expr must return integer", K(ret), K(when_expr_res_type));
    }
  }

  if (OB_SUCC(ret)) {
    rt_expr.eval_func_ = calc_case_expr;
    rt_expr.eval_batch_func_ = eval_case_batch;
    rt_expr.eval_vector_func_ = eval_case_vector;
  }
  return ret;
}

static int check_is_match(const ObDatum &when_datum, bool &match_when)
{
  int ret = OB_SUCCESS;
  if (when_datum.is_null()) {
    match_when = false;
  } else {
    int64_t v = when_datum.get_int();
    match_when = (v != 0) ? true : false;
  }
  return ret;
}

int ObExprCase::calc_case_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  const bool has_else = (expr.arg_cnt_ % 2 != 0);
  int64_t loop = (has_else) ? expr.arg_cnt_ - 1 : expr.arg_cnt_;
  bool match_when = false;
  ObDatum *when_datum = NULL;
  ObDatum *then_datum = NULL;
  bool has_result = false;
  int64_t expr_idx = 0;
  for ( ; OB_SUCC(ret) && !match_when && expr_idx < loop; expr_idx += 2) {
    if (OB_FAIL(expr.args_[expr_idx]->eval(ctx, when_datum))) {
      LOG_WARN("eval when expr failed", K(ret), K(expr_idx));
    } else if (OB_FAIL(check_is_match(*when_datum, match_when))) {
      LOG_WARN("check is when expr match failed", K(ret), K(expr_idx));
    } else if (match_when) {
      if (OB_FAIL(expr.args_[expr_idx+1]->eval(ctx, then_datum))) {
        LOG_WARN("eval then expr failed", K(ret), K(expr_idx+1));
      } else {
        has_result = true;
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (!match_when) {
      if (has_else) {
        if (OB_FAIL(expr.args_[expr.arg_cnt_-1]->eval(ctx, then_datum))) {
          LOG_WARN("eval else expr failed for case when", K(ret));
        } else {
          has_result = true;
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (!has_result) {
      res_datum.set_null();
    } else {
      if (OB_ISNULL(then_datum)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("then_datum is NULL", K(ret));
      } else {
        res_datum.set_datum(*then_datum);
      }
    }
  }
  return ret;
}

// Oracle模式下，在deduce type阶段需要将when/then expr类型要一致
int ObExprCase::is_same_kind_type_for_case(const ObIArray<ObExprResType> &type_arr)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ret)) {
    bool match = false;
    int64_t first_not_null_idx = OB_INVALID_ID;
    for (int64_t i = 0; OB_SUCC(ret) &&
          OB_INVALID_ID == first_not_null_idx && i < type_arr.count(); ++i) {
      if (!ob_is_null(type_arr.at(i).get_type())) {
        first_not_null_idx = i;
      }
    }
    first_not_null_idx = OB_INVALID_ID == first_not_null_idx ? 0 : first_not_null_idx;
    const ObExprResType &res_type = type_arr.at(first_not_null_idx);
    for (int64_t i = first_not_null_idx+1; OB_SUCC(ret) && i < type_arr.count(); ++i) {
      if (OB_FAIL(ObExprOperator::is_same_kind_type_for_case(res_type,
                                                             type_arr.at(i), match))) {
        LOG_WARN("fail to judge same type", K(i), K(res_type), K(type_arr.at(i)), K(ret));
      } else if (!match) {
        ret = OB_ERR_INVALID_TYPE_FOR_OP;
        LOG_WARN("fail to judge same type", K(i), K(res_type), K(type_arr.at(i)), K(ret));
      }
    }
  }
  return ret;
}

int ObExprCase::eval_case_batch(const ObExpr &expr,
                                ObEvalCtx &ctx,
                                const ObBitVector &skip,
                                const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  const bool has_else = (expr.arg_cnt_ % 2 != 0);
  int64_t loop = (has_else) ? expr.arg_cnt_ - 1 : expr.arg_cnt_;
  bool match_when = false;
  ObDatum *results = expr.locate_batch_datums(ctx);
  LOG_DEBUG("eval_case_batch", K(expr.arg_cnt_));
  if (OB_ISNULL(results)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("results frame is not init", K(ret));
  } else if (batch_size <= 0) {
    // do nothing
  } else {
    ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
    ObBitVector *case_when_match = nullptr;
    ObBitVector *case_not_match = nullptr;
    void * data = nullptr;
    void * data1 = nullptr;
    ObEvalCtx::TempAllocGuard alloc_guard(ctx);
    if (OB_ISNULL(data = alloc_guard.get_allocator().alloc(ObBitVector::memory_size(batch_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory for case_when_match", K(ret), K(batch_size));
    } else if (OB_ISNULL(data1 = alloc_guard.get_allocator().alloc(ObBitVector::memory_size(batch_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory for case_when_match", K(ret), K(batch_size));
    } else {
      case_when_match = to_bit_vector(data);
      case_not_match = to_bit_vector(data1);
      case_when_match->reset(batch_size);
      case_not_match->reset(batch_size);
      //case_when_match = eval_flags | skip
      case_when_match->bit_calculate(skip, eval_flags, batch_size,
                               [](const uint64_t l, const uint64_t r) { return (l | r); });
      case_not_match->bit_calculate(skip, eval_flags, batch_size,
                               [](const uint64_t l, const uint64_t r) { return (l | r); });
    }
    // E.G
    // SELECT CASE WHEN expr1 THEN expr2 WHEN expr3 THEN expr4 ... ELSE exprN END
    // the logic is
    // 1. calc when branch, save result in when_datums and use match_when flag
    //    to mark which rows are matched in when branch and these rows should be
    //    calculated in then branch
    // 2. calc then branch, put matching result(then_datums) into output datums
    //    (results)
    // REPEAT 1. and 2.
    // ...
    // LAST.
    //    calc else branch and put matching result(then_datums) into output datums
    for (int64_t expr_idx = 0; OB_SUCC(ret) && expr_idx < loop; expr_idx += 2) {
      if (OB_FAIL(expr.args_[expr_idx]->eval_batch(ctx, *case_when_match, batch_size))) {
        LOG_WARN("failed to eval batch", K(ret), K(expr_idx));
      } else {
        ObDatumVector when_datums = expr.args_[expr_idx]->locate_expr_datumvector(ctx);
        //first eval when datums
        for (int64_t j = 0; OB_SUCC(ret) && j < batch_size; ++j) {
          if (case_when_match->at(j)) {
            continue;
          }
          if (OB_FAIL(check_is_match(*when_datums.at(j), match_when))) {
            LOG_WARN("check is when expr match failed", K(ret), K(j));
          } else if (match_when) {
            case_when_match->set(j);
          } else {
            // not match, mark case_not_match to stop calculating then branch
            case_not_match->set(j);
          }
        }
        //now eval then datums
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(expr.args_[expr_idx + 1]->eval_batch(ctx, *case_not_match, batch_size))) {
          LOG_WARN("failed to eval batch", K(ret), K(expr_idx + 1));
        } else {
          ObDatumVector then_datums = expr.args_[expr_idx + 1]->locate_expr_datumvector(ctx);
          for (int64_t j = 0; OB_SUCC(ret) && j < batch_size; ++j) {
            if (case_not_match->at(j)) {
              continue;
            }
            results[j].set_datum(*then_datums.at(j));
            eval_flags.set(j);
          }

          // rows matched in this round should not match in next round, therefor,
          // copy last round matched rows flag(case_when_match) into case_not_match
          case_not_match->deep_copy(*case_when_match, batch_size);
        }
      }
    }
    //now set the result of the rest, skip rows already matched (case_when_match)
    if (OB_SUCC(ret)) {
      if (has_else) {
        if (OB_FAIL(expr.args_[expr.arg_cnt_ - 1]->eval_batch(ctx, *case_when_match, batch_size))) {
          LOG_WARN("failed to eval batch", K(ret));
        } else {
          ObDatumVector else_datums = expr.args_[expr.arg_cnt_ - 1]->locate_expr_datumvector(ctx);
          for (int64_t j = 0; OB_SUCC(ret) && j < batch_size; ++j) {
            if (case_when_match->at(j)) {
              continue;
            }
            results[j].set_datum(*else_datums.at(j));
            eval_flags.set(j);
          }
        }
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && j < batch_size; ++j) {
          if (case_when_match->at(j)) {
            continue;
          }
          results[j].set_null();
          eval_flags.set(j);
        }
      }
    }
  }
  return ret;
}

template<typename WhenVec>
static int check_when_is_match(const ObExpr &expr, ObEvalCtx &ctx,
                          ObBitVector &case_when_match, const EvalBound &bound,
                          const int64_t &when_expr_idx)
{
  int ret = OB_SUCCESS;
  WhenVec *when_vec = static_cast<WhenVec *>(expr.args_[when_expr_idx]->get_vector(ctx));
  for (int64_t j = bound.start(); OB_SUCC(ret) && j < bound.end(); ++j) {
    if (case_when_match.at(j)) {
      continue;
    }
    if (!when_vec->is_null(j) && when_vec->get_int(j) != 0) {
      case_when_match.set(j);
    }
  }
  return ret;
}

static int dispatch_check_when_is_match(const ObExpr &expr, ObEvalCtx &ctx,
                                  ObBitVector &case_when_match, const EvalBound &bound,
                                  const int64_t &when_expr_idx)
{
  int ret = OB_SUCCESS;
  VectorFormat when_format = expr.args_[when_expr_idx]->get_format(ctx);
  switch (when_format) {
    case VEC_FIXED: {
      ret = check_when_is_match<IntegerFixedVec>(expr, ctx, case_when_match, bound, when_expr_idx);
      break;
    }
    case VEC_UNIFORM: {
      ret = check_when_is_match<IntegerUniVec>(expr, ctx, case_when_match, bound, when_expr_idx);
      break;
    }
    case VEC_UNIFORM_CONST: {
      ret = check_when_is_match<IntegerUniCVec>(expr, ctx, case_when_match, bound, when_expr_idx);
      break;
    }
    default: {
      ret = check_when_is_match<ObVectorBase>(expr, ctx, case_when_match, bound, when_expr_idx);
    }
  }
  return ret;
}

template<typename ThenVec, typename ResVec>
static int eval_match_then(const ObExpr &expr, ObEvalCtx &ctx,
                          ObBitVector &case_when_match, const EvalBound &bound,
                          const int64_t &then_expr_idx)
{
  int ret = OB_SUCCESS;
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  ThenVec *then_vec = static_cast<ThenVec *>(expr.args_[then_expr_idx]->get_vector(ctx));
  ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
  for (int64_t j = bound.start(); OB_SUCC(ret) && j < bound.end(); ++j) {
    if (case_when_match.at(j)) {
      continue;
    }
    if (then_vec->is_null(j)) {
      res_vec->set_null(j);
    } else {
      res_vec->set_payload_shallow(j, then_vec->get_payload(j), then_vec->get_length(j));
    }
    eval_flags.set(j);
  }
  return ret;
}

template<typename ResVec>
static int dispatch_eval_match_then(const ObExpr &expr, ObEvalCtx &ctx,
                                  ObBitVector &case_when_match, const EvalBound &bound,
                                  const int64_t &then_expr_idx)
{
  int ret = OB_SUCCESS;
  VectorFormat then_format = expr.args_[then_expr_idx]->get_format(ctx);
  switch (then_format) {
    case VEC_FIXED: {
      ret = eval_match_then<ObFixedLengthBase, ResVec>(expr, ctx, case_when_match, bound, then_expr_idx);
      break;
    }
    case VEC_DISCRETE: {
      ret = eval_match_then<ObDiscreteFormat, ResVec>(expr, ctx, case_when_match, bound, then_expr_idx);
      break;
    }
    case VEC_CONTINUOUS: {
      ret = eval_match_then<ObContinuousFormat, ResVec>(expr, ctx, case_when_match, bound, then_expr_idx);
      break;
    }
    case VEC_UNIFORM: {
      ret = eval_match_then<ObUniformFormat<false>, ResVec>(expr, ctx, case_when_match, bound, then_expr_idx);
      break;
    }
    case VEC_UNIFORM_CONST: {
      ret = eval_match_then<ObUniformFormat<true>, ResVec>(expr, ctx, case_when_match, bound, then_expr_idx);
      break;
    }
    default: {
      ret = eval_match_then<ObVectorBase, ResVec>(expr, ctx, case_when_match, bound, then_expr_idx);
    }
  }
  return ret;
}

template <typename ResVec>
static int inner_eval_case_vector(const ObExpr &expr,
                                  ObEvalCtx &ctx,
                                  const ObBitVector &skip,
                                  const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  if (bound.end() > bound.start()) {
    const bool has_else = (expr.arg_cnt_ % 2 != 0);
    int64_t loop = (has_else) ? expr.arg_cnt_ - 1 : expr.arg_cnt_;
    LOG_DEBUG("eval_case_vector", K(expr.arg_cnt_));
    // Record the number of skips in a batch,
    // end when all parameters are skipped.
    const int64_t total_cnt = bound.end() - bound.start();
    int64_t skip_cnt = 0;
    ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
    // After verification, it is necessary to adopt the double ObBitVector scheme.
    // The single ObBitVector scheme can ensure correctness,
    // but the then_expression will result in redundant calculations.
    ObBitVector *case_when_match = &expr.get_pvt_skip(ctx);
    // The relevant implementation of "before_case_when_match" may seem a bit hacky,
    // but ObBitVector ensures that it only operates on data within bounds, so it is safe.
    ObBitVector *before_case_when_match = nullptr;
    // Remember to set all_row_active of bound to false when handling skip.
    // Otherwise, it may cause issues if all_row_active is used for specialized calculations elsewhere.
    EvalBound my_bound(bound.batch_size(), bound.start(), bound.end(), bound.get_all_rows_active());
    void *data_tmp = nullptr;
    ObEvalCtx::TempAllocGuard alloc_guard(ctx);
    // For single-line scenarios,
    // the additional allocated memory needs to be as small as possible in order to optimize performance.
    // The following approach consumes at most ObBitVector::BYTES_PER_WORD more byte of memory
    // compared to the ideal scenario,
    // but subsequent bitwise operations do not require shifting,
    // resulting in performance optimization and easier implementation.
    // The unit is byte.
    int64_t mem_size = (((my_bound.end() - 1) / ObBitVector::WORD_BITS) -
                        (my_bound.start() / ObBitVector::WORD_BITS) + 1) * ObBitVector::BYTES_PER_WORD;
    if (OB_ISNULL(data_tmp = alloc_guard.get_allocator().alloc(mem_size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory for before_case_when_match", K(ret), K(mem_size));
    } else {
      before_case_when_match = (ObBitVector *)((uint64_t *)data_tmp -
                                        (my_bound.start() / ObBitVector::WORD_BITS));
      // There is no need to call reset for initialization here,
      // because the subsequent bit_calculate will override the original values of ObBitVector.
      // case_when_match = eval_flags | skip
      case_when_match->bit_calculate(skip, eval_flags, my_bound,
                              [](const uint64_t l, const uint64_t r) { return (l | r); });
      skip_cnt = case_when_match->accumulate_bit_cnt(my_bound);
      before_case_when_match->deep_copy(*case_when_match, my_bound.start(), my_bound.end());
    }

    // E.G
    // SELECT CASE WHEN expr1 THEN expr2 WHEN expr3 THEN expr4 ... ELSE exprN END
    // the logic is
    // 1. calc when branch, save result in when_vec and use match_when flag
    //    to mark which rows are matched in when branch and these rows should be
    //    calculated in then branch
    // 2. calc then branch, put matching result(then_vec) into output vec
    //    (result_vec)
    // REPEAT 1. and 2.
    // ...
    // LAST.
    //    calc else branch and put matching result(else_vec) into output vec(result_vec)
    for (int64_t expr_idx = 0; OB_SUCC(ret) && skip_cnt < total_cnt && expr_idx < loop; expr_idx += 2) {
      if (my_bound.get_all_rows_active() && skip_cnt != 0) {
        my_bound.set_all_row_active(false);
      }
      if (OB_FAIL(expr.args_[expr_idx]->eval_vector(ctx, *case_when_match, my_bound))) {
        LOG_WARN("failed to eval vector", K(ret), K(expr_idx));
      // first eval when datums
      } else if (OB_FAIL(dispatch_check_when_is_match(expr, ctx, *case_when_match, my_bound, expr_idx))) {
        LOG_WARN("failed to dispatch_check_is_match", K(ret), K(expr_idx));
      } else {  // now eval then datums
        // Reverse case_when_match for use with case_not_match.
        case_when_match->bit_calculate(*case_when_match, *before_case_when_match, my_bound,
                      [](const uint64_t self, const uint64_t other) { return ((~self) | other); });
        skip_cnt = case_when_match->accumulate_bit_cnt(my_bound);
        if (skip_cnt < total_cnt) {
          if (my_bound.get_all_rows_active() && skip_cnt != 0) {
            my_bound.set_all_row_active(false);
          }
          if (OB_FAIL(expr.args_[expr_idx + 1]->eval_vector(ctx, *case_when_match, my_bound))) {
            LOG_WARN("failed to eval vector", K(ret), K(expr_idx + 1));
          } else if (OB_FAIL(dispatch_eval_match_then<ResVec>(expr, ctx, *case_when_match, my_bound, expr_idx + 1))) {
            LOG_WARN("failed to dispatch_eval_match_then", K(ret), K(expr_idx + 1));
          }
        }
        // Reverse case_when_match to be used again as case_when_match.
        case_when_match->bit_calculate(*case_when_match, *before_case_when_match, my_bound,
                        [](const uint64_t self, const uint64_t other) { return ((~self) | other); });
        skip_cnt = case_when_match->accumulate_bit_cnt(my_bound);
        if (expr_idx + 2 < loop) {
          // rows matched in this round should not match in next round, therefor,
          // copy last round matched rows flag(case_when_match) into before_case_when_match
          before_case_when_match->deep_copy(*case_when_match, my_bound.start(), my_bound.end());
        }
      }
    }
    // now set the result of the rest, skip rows already matched (case_when_match)
    if (OB_SUCC(ret) && skip_cnt < total_cnt) {
      if (has_else) {
        if (my_bound.get_all_rows_active() && skip_cnt != 0) {
          my_bound.set_all_row_active(false);
        }
        if (OB_FAIL(expr.args_[expr.arg_cnt_ - 1]->eval_vector(ctx, *case_when_match, my_bound))) {
          LOG_WARN("failed to eval batch", K(ret));
          // The calculation method of "else" is consistent with "then".
        } else if (OB_FAIL(dispatch_eval_match_then<ResVec>(expr, ctx, *case_when_match, my_bound, expr.arg_cnt_ - 1))) {
          LOG_WARN("failed to dispatch_eval_match_else", K(ret), K(expr.arg_cnt_ - 1));
        }
      } else {
        for (int64_t j = my_bound.start(); OB_SUCC(ret) && j < my_bound.end(); ++j) {
          if (case_when_match->at(j)) {
            continue;
          }
          ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
          res_vec->set_null(j);
          eval_flags.set(j);
        }
      }
    }
  }
  return ret;
}

int ObExprCase::eval_case_vector(const ObExpr &expr,
                             ObEvalCtx &ctx,
                             const ObBitVector &skip,
                             const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  VectorFormat res_format = expr.get_format(ctx);
  switch (res_format) {
    case VEC_DISCRETE: {
      ret = inner_eval_case_vector<ObDiscreteFormat>(expr, ctx, skip, bound);
      break;
    }
    case VEC_CONTINUOUS: {
      ret = inner_eval_case_vector<ObContinuousFormat>(expr, ctx, skip, bound);
      break;
    }
    case VEC_FIXED: {
      ret = inner_eval_case_vector<ObFixedLengthBase>(expr, ctx, skip, bound);
      break;
    }
    case VEC_UNIFORM: {
      ret = inner_eval_case_vector<ObUniformFormat<false>>(expr, ctx, skip, bound);
      break;
    }
    case VEC_UNIFORM_CONST: {
      ret = inner_eval_case_vector<ObUniformFormat<true>>(expr, ctx, skip, bound);
      break;
    }
    default: {
      ret = inner_eval_case_vector<ObVectorBase>(expr, ctx, skip, bound);
    }
  }
  return ret;
}

DEF_SET_LOCAL_SESSION_VARS(ObExprCase, raw_expr) {
  int ret = OB_SUCCESS;
  SET_LOCAL_SYSVAR_CAPACITY(1);
  EXPR_ADD_LOCAL_SYSVAR(share::SYS_VAR_COLLATION_CONNECTION);
  return ret;
}

}
}
