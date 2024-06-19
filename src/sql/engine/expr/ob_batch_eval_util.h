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

#ifndef OCEANBASE_EXPR_OB_BATCH_EVAL_UTIL_H_
#define OCEANBASE_EXPR_OB_BATCH_EVAL_UTIL_H_

#include <type_traits>

#include "sql/engine/expr/ob_expr.h"
#include "share/vector/ob_uniform_base.h"
#include "share/vector/ob_discrete_base.h"
#include "ob_expr_add.h"
namespace oceanbase
{
namespace sql
{

using common::OB_SUCCESS;
using common::number::ObNumber;

// Batch evaluate operand of binary operator
int binary_operand_batch_eval(const ObExpr &expr,
                              ObEvalCtx &ctx,
                              const ObBitVector &skip,
                              const int64_t size,
                              const bool null_short_circuit);

int binary_operand_vector_eval(const ObExpr &expr,
                              ObEvalCtx &ctx,
                              const ObBitVector &skip,
                              EvalBound &bound,
                              const bool null_short_circuit,
                              bool &right_evaluated);

// Iterate datums of batch result argument
struct ObArgBatchDatumIter
{
  explicit ObArgBatchDatumIter(const ObDatum *datums) : datums_(datums) {}
  const ObDatum &datum(const int64_t idx) const { return datums_[idx]; }

  const ObDatum *datums_;
};

// Iterate datum of scalar result argument
struct ObArgScalarDatumIter
{
  explicit ObArgScalarDatumIter(const ObDatum *datum) : datum_(datum) {}
  const ObDatum &datum(const int64_t) const { return *datum_; }

  const ObDatum *datum_;
};

// Iterate datums of result
struct ObResBatchDatumIter
{
  explicit ObResBatchDatumIter(ObDatum *datums) : datums_(datums) {}
  ObDatum &datum(const int64_t idx) const { return datums_[idx]; }

  ObDatum *datums_;
};

// Iterate raw data of batch result argument
template <typename RawType>
struct ObArgBatchRawIter : public ObArgBatchDatumIter
{
  explicit ObArgBatchRawIter(const ObDatum *datums, const char *rev_buf)
      : ObArgBatchDatumIter(datums), rev_(reinterpret_cast<const RawType *>(rev_buf)) {}
  const RawType &raw(const int64_t i) const { return rev_[i]; }

  const RawType *rev_;
};

// Iterate raw data of scalar result argument
template <typename RawType>
struct ObArgScalarRawIter : public ObArgScalarDatumIter
{
  using ObArgScalarDatumIter::ObArgScalarDatumIter;
  const RawType &raw(const int64_t) const
  {
    return *reinterpret_cast<const RawType *>(datum_->ptr_);
  }
};

// Iterate raw data of result
template <typename RawType>
struct ObResBatchRawIter : public ObResBatchDatumIter
{
  explicit ObResBatchRawIter(ObDatum *datums, char *rev_buf)
      : ObResBatchDatumIter(datums), rev_(reinterpret_cast<RawType *>(rev_buf)) {}
  RawType &raw(int64_t i) const { return rev_[i]; }
  RawType *rev_;
};

template <typename ArithOp>
struct ObDoArithBatchEval
{
  template <typename ResIter, typename LeftIter, typename RightIter, typename... Args>
  inline int operator()(const ObExpr &expr,
                        ObEvalCtx &ctx,
                        const ObBitVector &skip,
                        const int64_t size,
                        const ResIter &iter,
                        const LeftIter &l_it,
                        const RightIter &r_it,
                        const bool in_frame_notnull,
                        Args &...args) const
  {
    int ret = OB_SUCCESS;
    ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
    const int64_t step_size = sizeof(uint16_t) * CHAR_BIT;
    common::ObDatumDesc desc;
    for (int64_t i = 0; i < size && OB_SUCC(ret);) {
      const int64_t bit_vec_off = i / step_size;
      const uint16_t skip_v = skip.reinterpret_data<uint16_t>()[bit_vec_off];
      uint16_t &eval_v = eval_flags.reinterpret_data<uint16_t>()[bit_vec_off];
      if (i + step_size < size && (0 == (skip_v | eval_v))) {
        if (ArithOp::is_raw_op_supported() && in_frame_notnull) {
          for (int64_t j = 0; j < step_size; i++, j++) {
            ArithOp::raw_op(iter.raw(i), l_it.raw(i), r_it.raw(i), args...);
          }
          i -= step_size;
          for (int64_t j = 0; OB_SUCC(ret) && j < step_size; i++, j++) {
            iter.datum(i).pack_ = sizeof(typename ArithOp::RES_RAW_TYPE);
            ret = ArithOp::raw_check(iter.raw(i), l_it.raw(i), r_it.raw(i));
          }
        } else {
          for (int64_t j = 0; OB_SUCC(ret) && j < step_size; i++, j++) {
            ret = ArithOp::datum_op(iter.datum(i), l_it.datum(i), r_it.datum(i), args...);
            desc.pack_ |= iter.datum(i).pack_;
          }
        }
        if (OB_SUCC(ret)) {
          eval_v = 0xFFFF;
        }
      } else if (i + step_size < size && (0xFFFF == (skip_v | eval_v))) {
        i += step_size;
      } else {
        const int64_t new_size = std::min(size, i + step_size);
        for (; i < new_size && OB_SUCC(ret); i++) {
          if (!(skip.at(i) || eval_flags.at(i))) {
            ret = ArithOp::datum_op(iter.datum(i), l_it.datum(i), r_it.datum(i), args...);
            eval_flags.bit_or_assign(i, OB_SUCCESS == ret);
            desc.pack_ |= iter.datum(i).pack_;
          }
        }
      }
    }
    if (OB_SUCC(ret) && desc.is_null()) {
      expr.get_eval_info(ctx).notnull_ = false;
    }
    return ret;
  }
};


template <typename ResVector, typename LeftVector, typename RightVector, typename ArithOp>
struct ObDoArithVectorBaseEval
{
  template <typename... Args>
  inline int operator()(VECTOR_EVAL_FUNC_ARG_DECL, const bool right_evaluated, Args &... args) const
  {
    int ret = OB_SUCCESS;

    const LeftVector *left_vec = static_cast<const LeftVector *>(expr.args_[0]->get_vector(ctx));
    const RightVector *right_vec = static_cast<const RightVector *>(expr.args_[1]->get_vector(ctx));
    const bool all_not_null = !left_vec->has_null() &&
                                    !(right_evaluated ? right_vec->has_null() : true);
    ResVector *res_vec = static_cast<ResVector *>(expr.get_vector(ctx));
    ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
    const int64_t cond =
      //(bound.get_all_rows_active() << 2) | (expr.get_eval_info(ctx).evaluated_ << 1) | all_not_null;
      (bound.get_all_rows_active() << 2) | all_not_null;
    if (cond == 4) { // all_rows_active == true, evaluated = false, all_not_null = false
      for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
        ret = ArithOp::null_check_vector_op(*res_vec, *left_vec, *right_vec, idx, args...);
      }
      eval_flags.set_all(bound.start(), bound.end());
    } else if (cond == 5) { // all_rows_active == true, evaluated = false, all_not_null = true
      for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
        ret = ArithOp::vector_op(*res_vec, *left_vec, *right_vec, idx, args...);
      }
      eval_flags.set_all(bound.start(), bound.end());
    } else {
      for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
        if (!skip.at(idx) && !eval_flags.at(idx)) {
          ret = ArithOp::null_check_vector_op(*res_vec, *left_vec, *right_vec, idx, args...);
          eval_flags.set(idx);
        }
      }
    }
    return ret;
  }
};


template <typename ResVector, typename LeftVector, typename RightVector, typename ArithOp>
struct ObDoArithFixedVectorEval
{
  template <typename... Args>
  inline int operator()(VECTOR_EVAL_FUNC_ARG_DECL, const bool right_evaluated, Args &... args) const
  {
    int ret = OB_SUCCESS;
    const LeftVector *left_vec = static_cast<const LeftVector *>(expr.args_[0]->get_vector(ctx));
    const RightVector *right_vec = static_cast<const RightVector *>(expr.args_[1]->get_vector(ctx));
    ResVector *res_vec = static_cast<ResVector *>(expr.get_vector(ctx));
    const typename ArithOp::L_RAW_TYPE *left_arr =
      reinterpret_cast<const typename ArithOp::L_RAW_TYPE *>(left_vec->get_data());
    const typename ArithOp::R_RAW_TYPE *right_arr =
      reinterpret_cast<const typename ArithOp::R_RAW_TYPE *>(right_vec->get_data());
    typename ArithOp::RES_RAW_TYPE *res_arr =
      reinterpret_cast<typename ArithOp::RES_RAW_TYPE *>(res_vec->get_data());
    const bool all_not_null = !left_vec->has_null() &&
                                    !(right_evaluated ? right_vec->has_null() : true);
    ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);

    const int64_t cond = (ArithOp::is_raw_op_supported() << 3) | (bound.get_all_rows_active() << 2)
                     //| (expr.get_eval_info(ctx).evaluated_ << 1) | all_not_null;
                       | all_not_null;
    // 1101: row_op_support = true, all_rows_active == true, evaluated = false, all_not_null = true
    if (cond == 13) {
      for (int64_t idx = bound.start(); idx < bound.end(); ++idx) {
        ArithOp::raw_op(res_arr[idx], left_arr[idx], right_arr[idx], args...);
      }
      res_vec->get_nulls()->unset_all(bound.start(), bound.end());
      if (expr.may_not_need_raw_check_ && ob_is_int_less_than_64(expr.args_[0]->datum_meta_.type_)
          && ob_is_int_less_than_64(expr.args_[1]->datum_meta_.type_)) {
        // do nothing
      } else {
        for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
          ret = ArithOp::raw_check(res_arr[idx], left_arr[idx], right_arr[idx]);
        }
      }
      eval_flags.set_all(bound.start(), bound.end());
    } else {
      ret = ObDoArithVectorBaseEval<ResVector, LeftVector, RightVector, ArithOp>()(
        expr, ctx, skip, bound, right_evaluated, args...);
    }
    return ret;
  }
};


template <typename ResVector, typename LeftVector, typename RightVector, typename ArithOp>
struct ObDoArithFixedConstVectorEval
{
  template <typename... Args>
  inline int operator()(VECTOR_EVAL_FUNC_ARG_DECL, const bool right_evaluated, Args &... args) const
  {
    int ret = OB_SUCCESS;
    const LeftVector *left_vec = static_cast<const LeftVector *>(expr.args_[0]->get_vector(ctx));
    const RightVector *right_vec = static_cast<const RightVector *>(expr.args_[1]->get_vector(ctx));
    ResVector *res_vec = static_cast<ResVector *>(expr.get_vector(ctx));
    const typename ArithOp::L_RAW_TYPE *left_arr =
      reinterpret_cast<const typename ArithOp::L_RAW_TYPE *>(left_vec->get_data());
    const typename ArithOp::R_RAW_TYPE *right_val =
      reinterpret_cast<const typename ArithOp::R_RAW_TYPE *>(right_vec->get_payload(0));
    typename ArithOp::RES_RAW_TYPE *res_arr =
      reinterpret_cast<typename ArithOp::RES_RAW_TYPE *>(res_vec->get_data());
    const bool all_not_null = !left_vec->has_null() &&
                                    !(right_evaluated ? right_vec->has_null() : true);
    ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);

    const int cond = (ArithOp::is_raw_op_supported() << 3) | (bound.get_all_rows_active() << 2)
                     //| (expr.get_eval_info(ctx).evaluated_ << 1) | all_not_null;
                       | all_not_null;
    // 1101: row_op_support = true, all_rows_active == true, evaluated = false, all_not_null = true
    if (cond == 13) {
      for (int64_t idx = bound.start(); idx < bound.end(); ++idx) {
        ArithOp::raw_op(res_arr[idx], left_arr[idx], *right_val, args...);
      }
      res_vec->get_nulls()->unset_all(bound.start(), bound.end());
      if (expr.may_not_need_raw_check_ && ob_is_int_less_than_64(expr.args_[0]->datum_meta_.type_)
          && INT_MIN < *right_val < INT_MAX) {
        // do nothing
      } else {
        for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
          ret = ArithOp::raw_check(res_arr[idx], left_arr[idx], *right_val);
        }
      }
      eval_flags.set_all(bound.start(), bound.end());
    } else {
      ret = ObDoArithVectorBaseEval<ResVector, LeftVector, RightVector, ArithOp>()(
        expr, ctx, skip, bound, right_evaluated, args...);
    }
    return ret;
  }
};


template <typename ResVector, typename LeftVector, typename RightVector, typename ArithOp>
struct ObDoArithConstFixedVectorEval
{
  template <typename... Args>
  inline int operator()(VECTOR_EVAL_FUNC_ARG_DECL, const bool right_evaluated, Args &... args) const
  {
    int ret = OB_SUCCESS;
    const LeftVector *left_vec = static_cast<const LeftVector *>(expr.args_[0]->get_vector(ctx));
    const RightVector *right_vec = static_cast<const RightVector *>(expr.args_[1]->get_vector(ctx));
    ResVector *res_vec = static_cast<ResVector *>(expr.get_vector(ctx));
    const typename ArithOp::L_RAW_TYPE *left_val =
      reinterpret_cast<const typename ArithOp::L_RAW_TYPE *>(left_vec->get_payload(0));
    const typename ArithOp::R_RAW_TYPE *right_arr =
      reinterpret_cast<const typename ArithOp::R_RAW_TYPE *>(right_vec->get_data());
    typename ArithOp::RES_RAW_TYPE *res_arr =
      reinterpret_cast<typename ArithOp::RES_RAW_TYPE *>(res_vec->get_data());
    const bool all_not_null = !left_vec->has_null() &&
                                    !(right_evaluated ? right_vec->has_null() : true);
    ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);

    const int cond = (ArithOp::is_raw_op_supported() << 3) | (bound.get_all_rows_active() << 2)
                     //| (expr.get_eval_info(ctx).evaluated_ << 1) | all_not_null;
                       | all_not_null;
    // 1101: row_op_support = true, all_rows_active == true, evaluated = false, all_not_null = true
    if (cond == 13) {
      for (int64_t idx = bound.start(); idx < bound.end(); ++idx) {
        ArithOp::raw_op(res_arr[idx], *left_val, right_arr[idx], args...);
      }
      res_vec->get_nulls()->unset_all(bound.start(), bound.end());
      if (expr.may_not_need_raw_check_ && INT_MIN < *left_val < INT_MAX
          && ob_is_int_less_than_64(expr.args_[1]->datum_meta_.type_)) {
      } else {
        // do nothing
        for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
          ret = ArithOp::raw_check(res_arr[idx], *left_val, right_arr[idx]);
        }
      }
      eval_flags.set_all(bound.start(), bound.end());
    } else {
      ret = ObDoArithVectorBaseEval<ResVector, LeftVector, RightVector, ArithOp>()(
        expr, ctx, skip, bound, right_evaluated, args...);
    }
    return ret;
  }
};

template<typename NmbFastOp, typename ResVector, typename LeftVector, typename RightVector>
struct _nmb_eval_op_impl
{
  OB_INLINE int operator()(const int64_t idx, const LeftVector *left_vec,
                           const RightVector *right_vec, ResVector *res_vec, NmbFastOp &nmb_fast_op,
                           ObDataBuffer &local_alloc)
  {
    int ret = OB_SUCCESS;
    const number::ObCompactNumber *l_cnum = reinterpret_cast<const number::ObCompactNumber *>(left_vec->get_payload(idx));
    const number::ObCompactNumber *r_cnum = reinterpret_cast<const number::ObCompactNumber *>(right_vec->get_payload(idx));

    ObNumber l_num(*l_cnum);
    ObNumber r_num(*r_cnum);
    const number::ObCompactNumber *res_num = reinterpret_cast<const number::ObCompactNumber *>(res_vec->get_payload(idx));
    uint32_t *res_digits = const_cast<uint32_t *>(res_num->digits_);
    ObNumber::Desc &desc_buf = const_cast<ObNumber::Desc &>(res_num->desc_);
    ObNumber::Desc *res_desc = new (&desc_buf) ObNumber::Desc();
    // speedup detection
    if (nmb_fast_op(l_num, r_num, res_digits, *res_desc)) {
      // res_vec->set_payload_shallow(idx, res_num, sizeof(ObNumberDesc) + res_num->desc_.len_ * sizeof(uint32_t));
      res_vec->set_length(idx, sizeof(ObNumberDesc) + res_num->desc_.len_ * sizeof(uint32_t));
    } else {
      ObNumber tmp_num;
      if (OB_FAIL(nmb_fast_op(l_num, r_num, tmp_num, local_alloc))) {
        SQL_LOG(WARN, "num arith op failed", K(ret), K(l_num), K(r_num));
      } else {
        const_cast<number::ObCompactNumber *>(res_num)->desc_ = tmp_num.d_;
        uint32_t *digits = &(const_cast<number::ObCompactNumber *>(res_num)->digits_[0]);
        MEMCPY(digits, tmp_num.get_digits(), tmp_num.d_.len_ * sizeof(uint32_t));
        res_vec->set_length(idx,  tmp_num.d_.len_ * sizeof(uint32_t) + sizeof(ObNumberDesc));
      }
      local_alloc.free();
    }
    return ret;
  }
};

template <typename NmbFastOp, typename ResVector, typename LeftVector, typename RightVector>
inline int ObDoNumberVectorEval(VECTOR_EVAL_FUNC_ARG_DECL, const bool right_evaluated,
                                       NmbFastOp &nmb_fast_op)
{
  int ret = OB_SUCCESS;
  const LeftVector *left_vec = static_cast<const LeftVector *>(expr.args_[0]->get_vector(ctx));
  const RightVector *right_vec = static_cast<const RightVector *>(expr.args_[1]->get_vector(ctx));
  ResVector *res_vec = static_cast<ResVector *>(expr.get_vector(ctx));
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  int64_t calc_cnt = eval_flags.accumulate_bit_cnt(bound);
  bool need_calc_all = (calc_cnt == 0);
  bool need_calc = (calc_cnt != bound.range_size());

  char local_buff[ObNumber::MAX_BYTE_LEN];
  ObDataBuffer local_alloc(local_buff, ObNumber::MAX_BYTE_LEN);

  _nmb_eval_op_impl<NmbFastOp, ResVector, LeftVector, RightVector> nmb_eval_op;
  if (OB_LIKELY(bound.get_all_rows_active() && need_calc_all)) {
    if (!left_vec->has_null() && !(right_evaluated ? right_vec->has_null() : true)) {
      for (int idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); idx++) {
        ret = nmb_eval_op(idx, left_vec, right_vec, res_vec, nmb_fast_op, local_alloc);
      }
      if (std::is_same<ResVector, ObDiscreteFormat>::value) {
        reinterpret_cast<ObDiscreteFormat *>(res_vec)->get_nulls()->unset_all(bound.start(),
                                                                              bound.end());
      }
    } else {
      for (int idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); idx++) {
        if (left_vec->is_null(idx) || right_vec->is_null(idx)) {
          res_vec->set_null(idx);
        } else {
          if (std::is_same<ResVector, ObDiscreteFormat>::value) {
            res_vec->unset_null(idx);
          }
          ret = nmb_eval_op(idx, left_vec, right_vec, res_vec, nmb_fast_op, local_alloc);
        }
      }
    }
    eval_flags.set_all(bound.start(), bound.end());
  } else if (need_calc) {
    for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
      if (eval_flags.at(idx) || skip.at(idx)) { continue; }
      if (left_vec->is_null(idx) || right_vec->is_null(idx)) {
        res_vec->set_null(idx);
        eval_flags.set(idx);
        continue;
      }
      if (std::is_same<ResVector, ObDiscreteFormat>::value) {
        res_vec->unset_null(idx);
      }
      ret = nmb_eval_op(idx, left_vec, right_vec, res_vec, nmb_fast_op, local_alloc);
      if (OB_SUCC(ret)) { eval_flags.set(idx); }
    }
  }
  return ret;
}

template <typename ArithOp, template <class> class Functor, typename... Args>
inline int call_functor_with_arg_iter(const ObExpr &expr,
                                     ObEvalCtx &ctx,
                                     const ObBitVector &skip,
                                     const int64_t size,
                                     Args &...args)
{
  int ret = OB_SUCCESS;
  const ObExpr &left = *expr.args_[0];
  const ObExpr &right = *expr.args_[1];
  ObResBatchRawIter<typename ArithOp::RES_RAW_TYPE> res(
      expr.locate_batch_datums(ctx), expr.get_rev_buf(ctx));
  if (left.is_batch_result() && !right.is_batch_result()) {
    ObArgBatchRawIter<typename ArithOp::L_RAW_TYPE> l(
        left.locate_batch_datums(ctx), left.get_rev_buf(ctx));
    ObArgScalarRawIter<typename ArithOp::R_RAW_TYPE> r(&right.locate_expr_datum(ctx));
    ret = Functor<ArithOp>()(expr, ctx, skip, size, res, l, r,
                             (left.get_eval_info(ctx).in_frame_notnull()
                              && !right.locate_expr_datum(ctx).is_null()),
                             args...);
  } else if (!left.is_batch_result() && right.is_batch_result()) {
    ObArgScalarRawIter<typename ArithOp::L_RAW_TYPE> l(&left.locate_expr_datum(ctx));
    ObArgBatchRawIter<typename ArithOp::R_RAW_TYPE> r(
        right.locate_batch_datums(ctx), right.get_rev_buf(ctx));
    ret = Functor<ArithOp>()(expr, ctx, skip, size, res, l, r,
                             (right.get_eval_info(ctx).in_frame_notnull()
                              && !left.locate_expr_datum(ctx).is_null()),
                             args...);
  } else if (left.is_batch_result() && right.is_batch_result()) {
    ObArgBatchRawIter<typename ArithOp::L_RAW_TYPE> l(
        left.locate_batch_datums(ctx), left.get_rev_buf(ctx));
    ObArgBatchRawIter<typename ArithOp::R_RAW_TYPE> r(
        right.locate_batch_datums(ctx), right.get_rev_buf(ctx));
    ret = Functor<ArithOp>()(expr, ctx, skip, size, res, l, r,
                             (left.get_eval_info(ctx).in_frame_notnull()
                              && right.get_eval_info(ctx).in_frame_notnull()),
                             args...);
  } else {
    ret = common::OB_ERR_UNEXPECTED;
    SQL_LOG(WARN, "one argument must be batch result in arith batch evaluate",
            K(ret), K(expr), K(left), K(right));
  }
  return ret;
};


// define arith evaluate batch function.
// see example in ObExprAdd
template <typename ArithOp, typename... Args>
int def_batch_arith_op(const ObExpr &expr,
                       ObEvalCtx &ctx,
                       const ObBitVector &skip,
                       const int64_t size,
                       Args &...args)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(binary_operand_batch_eval(expr, ctx, skip, size, lib::is_oracle_mode()))) {
    SQL_LOG(WARN, "bianry operand batch evaluate failed", K(ret), K(expr));
  } else {
    ret = call_functor_with_arg_iter<ArithOp, ObDoArithBatchEval>(expr, ctx, skip, size, args...);
  }
  return ret;
}

constexpr int CONVER_FORMAT_VALUE(VectorFormat base_format, VectorFormat format) {
  return (format == base_format) ? 0 :
          (format == VEC_UNIFORM) ? 1 :
          (format == VEC_UNIFORM_CONST) ? 2 : 3;
}

constexpr int64_t GET_FORMAT_CONDITION(VectorFormat base_format, VectorFormat res_format,
                                              VectorFormat left_format, VectorFormat right_format)
{
  return ((CONVER_FORMAT_VALUE(base_format, res_format) << 4)
          | (CONVER_FORMAT_VALUE(base_format, left_format) << 2)
          | CONVER_FORMAT_VALUE(base_format, right_format));
}

using Discrete = ObDiscreteFormat;
template<typename ValueType>
using Fixed = ObFixedLengthFormat<ValueType>;
using Const = ObUniformFormat<true>;
using Uniform = ObUniformFormat<false>;
#define NUMBER_FORMAT_DISPATCH_BRANCH(RES, LEFT, RIGH)                                             \
  case GET_FORMAT_CONDITION(VEC_DISCRETE, RES::FORMAT, LEFT::FORMAT, RIGH::FORMAT): {              \
    ret = ObDoNumberVectorEval<NmbFastOp, RES, LEFT, RIGH>(expr, ctx, skip, pvt_bound,             \
                                                           right_evaluated, nmb_fast_op);          \
    break;                                                                                         \
  }
template <typename NmbFastOp, typename... Args>
inline int def_number_vector_arith_op(VECTOR_EVAL_FUNC_ARG_DECL, NmbFastOp &nmb_fast_op)
{
  int ret = OB_SUCCESS;
  EvalBound pvt_bound = bound;
  bool right_evaluated = true;
  if (OB_FAIL(binary_operand_vector_eval(expr, ctx, skip, pvt_bound, lib::is_oracle_mode(), right_evaluated))) {
    SQL_LOG(WARN, "binary number batch evaluation failure", K(ret));
  } else {
    const VectorFormat left_format = expr.args_[0]->get_format(ctx);
    const VectorFormat right_format = expr.args_[1]->get_format(ctx);
    const VectorFormat res_format = expr.get_format(ctx);
    const int64_t cond = GET_FORMAT_CONDITION(VEC_DISCRETE, res_format, left_format, right_format);
    switch (cond) {
      NUMBER_FORMAT_DISPATCH_BRANCH(Discrete, Discrete, Discrete);
      NUMBER_FORMAT_DISPATCH_BRANCH(Discrete, Discrete, Uniform);
      NUMBER_FORMAT_DISPATCH_BRANCH(Discrete, Discrete, Const);
      NUMBER_FORMAT_DISPATCH_BRANCH(Discrete, Uniform, Discrete);
      NUMBER_FORMAT_DISPATCH_BRANCH(Discrete, Uniform, Uniform);
      NUMBER_FORMAT_DISPATCH_BRANCH(Discrete, Uniform, Const);
      NUMBER_FORMAT_DISPATCH_BRANCH(Discrete, Const, Discrete);
      NUMBER_FORMAT_DISPATCH_BRANCH(Discrete, Const, Uniform);
      NUMBER_FORMAT_DISPATCH_BRANCH(Uniform, Discrete, Discrete);
      NUMBER_FORMAT_DISPATCH_BRANCH(Uniform, Discrete, Uniform);
      NUMBER_FORMAT_DISPATCH_BRANCH(Uniform, Discrete, Const);
      NUMBER_FORMAT_DISPATCH_BRANCH(Uniform, Uniform, Discrete);
      NUMBER_FORMAT_DISPATCH_BRANCH(Uniform, Uniform, Uniform);
      NUMBER_FORMAT_DISPATCH_BRANCH(Uniform, Uniform, Const);
      NUMBER_FORMAT_DISPATCH_BRANCH(Uniform, Const, Discrete);
      NUMBER_FORMAT_DISPATCH_BRANCH(Uniform, Const, Uniform);
      default: {
        ret = ObDoNumberVectorEval<NmbFastOp, ObVectorBase, ObVectorBase, ObVectorBase>(
                  expr, ctx, skip, pvt_bound, right_evaluated, nmb_fast_op);
      }
    }
  }
  return ret;
}
#undef NUMBER_FORMAT_DISPATCH_BRANCH

#define FORMAT_DISPATCH_BRANCH(base_format, RES, LEFT, RIGH, OP)                                   \
  case GET_FORMAT_CONDITION(base_format, RES::FORMAT, LEFT::FORMAT, RIGH::FORMAT): {               \
    ret = ObDoArithVectorBaseEval<RES, LEFT, RIGH, OP>()(expr, ctx, skip, pvt_bound,               \
                                                         right_evaluated, args...);                \
    break;                                                                                         \
  }
template <typename ArithOp, typename... Args>
int def_fixed_len_vector_arith_op(VECTOR_EVAL_FUNC_ARG_DECL, Args &... args)
{
  int ret = OB_SUCCESS;
  EvalBound pvt_bound = bound;
  bool right_evaluated = true;
  if (OB_FAIL(binary_operand_vector_eval(expr, ctx, skip, pvt_bound, lib::is_oracle_mode(), right_evaluated))) {
    SQL_LOG(WARN, "binary operand vector evaluate failed", K(ret), K(expr));
  } else {
    const VectorFormat left_format = expr.args_[0]->get_format(ctx);
    const VectorFormat right_format = expr.args_[1]->get_format(ctx);
    const VectorFormat res_format = expr.get_format(ctx);
    #define VEC_ARG_LIST expr, ctx, skip, pvt_bound, right_evaluated
    using L_Fixed = Fixed<typename ArithOp::L_RAW_TYPE>;
    using R_Fixed = Fixed<typename ArithOp::R_RAW_TYPE>;
    using RES_Fixed = Fixed<typename ArithOp::RES_RAW_TYPE>;
    const int64_t cond = GET_FORMAT_CONDITION(VEC_FIXED, res_format, left_format, right_format);
    switch (cond)
    {
      case GET_FORMAT_CONDITION(VEC_FIXED, RES_Fixed::FORMAT, L_Fixed::FORMAT, R_Fixed::FORMAT):
        ret = ObDoArithFixedVectorEval<RES_Fixed, L_Fixed, R_Fixed, ArithOp>()(VEC_ARG_LIST, args...);
        break;
      case GET_FORMAT_CONDITION(VEC_FIXED, RES_Fixed::FORMAT, L_Fixed::FORMAT, Const::FORMAT):
        ret = ObDoArithFixedConstVectorEval<RES_Fixed, L_Fixed, Const, ArithOp>()(VEC_ARG_LIST,
                                                                                  args...);
        break;
      case GET_FORMAT_CONDITION(VEC_FIXED, RES_Fixed::FORMAT, Const::FORMAT, R_Fixed::FORMAT):
        ret = ObDoArithConstFixedVectorEval<RES_Fixed, Const, R_Fixed, ArithOp>()(VEC_ARG_LIST,
                                                                                  args...);
        break;
      FORMAT_DISPATCH_BRANCH(VEC_FIXED, RES_Fixed, L_Fixed, Uniform, ArithOp);
      FORMAT_DISPATCH_BRANCH(VEC_FIXED, RES_Fixed, Uniform, R_Fixed, ArithOp);
      FORMAT_DISPATCH_BRANCH(VEC_FIXED, RES_Fixed, Uniform, Uniform, ArithOp);
      FORMAT_DISPATCH_BRANCH(VEC_FIXED, RES_Fixed, Uniform, Const, ArithOp);
      FORMAT_DISPATCH_BRANCH(VEC_FIXED, RES_Fixed, Const, Uniform, ArithOp);
      FORMAT_DISPATCH_BRANCH(VEC_FIXED, Uniform, L_Fixed, R_Fixed, ArithOp);
      FORMAT_DISPATCH_BRANCH(VEC_FIXED, Uniform, L_Fixed, Uniform, ArithOp);
      FORMAT_DISPATCH_BRANCH(VEC_FIXED, Uniform, L_Fixed, Const, ArithOp);
      FORMAT_DISPATCH_BRANCH(VEC_FIXED, Uniform, Uniform, R_Fixed, ArithOp);
      FORMAT_DISPATCH_BRANCH(VEC_FIXED, Uniform, Uniform, Uniform, ArithOp);
      FORMAT_DISPATCH_BRANCH(VEC_FIXED, Uniform, Uniform, Const, ArithOp);
      FORMAT_DISPATCH_BRANCH(VEC_FIXED, Uniform, Const, R_Fixed, ArithOp);
      FORMAT_DISPATCH_BRANCH(VEC_FIXED, Uniform, Const, Uniform, ArithOp);
      default:
        ret = ObDoArithVectorBaseEval<ObVectorBase, ObVectorBase, ObVectorBase, ArithOp>()(
          VEC_ARG_LIST, args...);
    }
  }
  if (OB_SUCC(ret)) {
    SQL_LOG(DEBUG, "expr", K(ToStrVectorHeader(expr, ctx, &skip, pvt_bound)));
  }
  return ret;
}

template <typename ArithOp, typename... Args>
int def_variable_len_vector_arith_op(VECTOR_EVAL_FUNC_ARG_DECL, Args &... args)
{
  int ret = OB_SUCCESS;
  EvalBound pvt_bound = bound;
  bool right_evaluated = true;
  if (OB_FAIL(binary_operand_vector_eval(expr, ctx, skip, pvt_bound, lib::is_oracle_mode(),
                                         right_evaluated))) {
    SQL_LOG(WARN, "binary operand vector evaluate failed", K(ret), K(expr));
  } else {
    const VectorFormat left_format = expr.args_[0]->get_format(ctx);
    const VectorFormat right_format = expr.args_[1]->get_format(ctx);
    const VectorFormat res_format = expr.get_format(ctx);
    const int64_t cond = GET_FORMAT_CONDITION(VEC_DISCRETE, res_format, left_format, right_format);
    switch (cond)
    {
      FORMAT_DISPATCH_BRANCH(VEC_DISCRETE, Discrete, Discrete, Discrete, ArithOp);
      FORMAT_DISPATCH_BRANCH(VEC_DISCRETE, Discrete, Discrete, Uniform, ArithOp);
      FORMAT_DISPATCH_BRANCH(VEC_DISCRETE, Discrete, Discrete, Const, ArithOp);
      FORMAT_DISPATCH_BRANCH(VEC_DISCRETE, Discrete, Uniform, Discrete, ArithOp);
      FORMAT_DISPATCH_BRANCH(VEC_DISCRETE, Discrete, Uniform, Uniform, ArithOp);
      FORMAT_DISPATCH_BRANCH(VEC_DISCRETE, Discrete, Uniform, Const, ArithOp);
      FORMAT_DISPATCH_BRANCH(VEC_DISCRETE, Discrete, Const, Discrete, ArithOp);
      FORMAT_DISPATCH_BRANCH(VEC_DISCRETE, Discrete, Const, Uniform, ArithOp);
      FORMAT_DISPATCH_BRANCH(VEC_DISCRETE, Uniform, Discrete, Discrete, ArithOp);
      FORMAT_DISPATCH_BRANCH(VEC_DISCRETE, Uniform, Discrete, Uniform, ArithOp);
      FORMAT_DISPATCH_BRANCH(VEC_DISCRETE, Uniform, Discrete, Const, ArithOp);
      FORMAT_DISPATCH_BRANCH(VEC_DISCRETE, Uniform, Uniform, Discrete, ArithOp);
      FORMAT_DISPATCH_BRANCH(VEC_DISCRETE, Uniform, Uniform, Uniform, ArithOp);
      FORMAT_DISPATCH_BRANCH(VEC_DISCRETE, Uniform, Uniform, Const, ArithOp);
      FORMAT_DISPATCH_BRANCH(VEC_DISCRETE, Uniform, Const, Discrete, ArithOp);
      FORMAT_DISPATCH_BRANCH(VEC_DISCRETE, Uniform, Const, Uniform, ArithOp);
      default:
        ret = ObDoArithVectorBaseEval<ObVectorBase, ObVectorBase, ObVectorBase, ArithOp>()(
          VEC_ARG_LIST, args...);
    }
  }
  if (OB_SUCC(ret)) {
    SQL_LOG(DEBUG, "expr", K(ToStrVectorHeader(expr, ctx, &skip, pvt_bound)));
  }
  return ret;
}
#undef FORMAT_DISPATCH_BRANCH

template <typename Res, typename Left, typename Righ>
struct ObArithOpRawType
{
  typedef Res RES_RAW_TYPE;
  typedef Left L_RAW_TYPE;
  typedef Righ R_RAW_TYPE;
};

struct ObArithOpBase : public ObArithOpRawType<char, char, char>
{
  constexpr static bool is_raw_op_supported() { return false; }

  template <typename... Args>
  static void raw_op(RES_RAW_TYPE &, const L_RAW_TYPE &, const R_RAW_TYPE, Args &...args) {}
  static int raw_check(const RES_RAW_TYPE &, const L_RAW_TYPE &, const R_RAW_TYPE &)
  {
    return common::OB_ERR_UNEXPECTED;
  }
};

template <typename Res, typename Left, typename Right>
struct ObArithTypedBase : public ObArithOpRawType<Res, Left, Right>
{
  constexpr static bool is_raw_op_supported()
  {
    return false;
  }

  template <typename... Args>
  static void raw_op(Res &, const Left &, const Right, Args &...args)
  {}
  static int raw_check(const Res &, const Left &, const Right &)
  {
    return common::OB_ERR_UNEXPECTED;
  }
};

// Wrap arith operate with null check.
template <typename DatumFunctor>
struct ObWrapArithOpNullCheck: public ObArithOpBase
{
  template <typename... Args>
  static int datum_op(ObDatum &res, const ObDatum &l, const ObDatum &r, Args &...args)
  {
    int ret = OB_SUCCESS;
    if (l.is_null() || r.is_null()) {
      res.set_null();
    } else {
      ret = DatumFunctor()(res, l, r, args...);
    }
    return ret;
  }
};

// Wrap arith operate with null check for vector.
template <typename VectorFunctor, typename Base>
struct ObWrapVectorArithOpNullCheck: public Base
{
  template <typename ResVector, typename LeftVector, typename RightVector, typename... Args>
  static int vector_op(ResVector &res_vec, const LeftVector &left_vec,
                       const RightVector &right_vec, const int64_t idx, Args &... args)
  {
    return VectorFunctor()(res_vec, left_vec, right_vec, idx, args...);
  }

  template <typename ResVector, typename LeftVector, typename RightVector, typename... Args>
  static int null_check_vector_op(ResVector &res_vec, const LeftVector &left_vec,
                       const RightVector &right_vec, const int64_t idx, Args &... args)
  {
    int ret = OB_SUCCESS;
    if (left_vec.is_null(idx) || right_vec.is_null(idx)) {
      res_vec.set_null(idx);
    } else {
      ret = VectorFunctor()(res_vec, left_vec, right_vec, idx, args...);
    }
    return ret;
  }
};

template <typename DatumFunctor, typename... Args>
int def_batch_arith_op_by_datum_func(BATCH_EVAL_FUNC_ARG_DECL, Args &...args)
{
  return def_batch_arith_op<ObWrapArithOpNullCheck<DatumFunctor>, Args...>(
      BATCH_EVAL_FUNC_ARG_LIST, args...);
}

template <typename VectorFunctor, typename ArithBase, typename... Args>
int def_fixed_len_vector_arith_op_func(VECTOR_EVAL_FUNC_ARG_DECL, Args &...args)
{
  return def_fixed_len_vector_arith_op<ObWrapVectorArithOpNullCheck<VectorFunctor, ArithBase>, Args...>(
      VECTOR_EVAL_FUNC_ARG_LIST, args...);
}

template <typename VectorFunctor, typename ArithBase, typename... Args>
int def_variable_len_vector_arith_op_func(VECTOR_EVAL_FUNC_ARG_DECL, Args &...args)
{
  return def_variable_len_vector_arith_op<ObWrapVectorArithOpNullCheck<VectorFunctor, ArithBase>, Args...>(
      VECTOR_EVAL_FUNC_ARG_LIST, args...);
}


// Wrap arith datum operate from raw operate.
template <typename Base>
struct ObArithOpWrap : public Base
{
  constexpr static bool is_raw_op_supported() { return true; }
  template <typename... Args>
  int operator()(ObDatum &res, const ObDatum &l, const ObDatum &r, Args &...args) const
  {
    Base::raw_op(
        *const_cast<typename Base::RES_RAW_TYPE *>(
            reinterpret_cast<const typename Base::RES_RAW_TYPE *>(res.ptr_)),
        *reinterpret_cast<const typename Base::L_RAW_TYPE *>(l.ptr_),
        *reinterpret_cast<const typename Base::R_RAW_TYPE *>(r.ptr_),
        args...);
    res.pack_ = sizeof(typename Base::RES_RAW_TYPE);
    return Base::raw_check(*reinterpret_cast<const typename Base::RES_RAW_TYPE *>(res.ptr_),
                           *reinterpret_cast<const typename Base::L_RAW_TYPE *>(l.ptr_),
                           *reinterpret_cast<const typename Base::R_RAW_TYPE *>(r.ptr_));
  }

  template <typename... Args>
  static int datum_op(ObDatum &res, const ObDatum &l, const ObDatum &r, Args &...args)
  {
    int ret = OB_SUCCESS;
    if (l.is_null() || r.is_null()) {
      res.set_null();
    } else {
      ret = ObArithOpWrap()(res, l, r, args...);
    }
    return ret;
  }
};

// Wrap arith vector operate from raw operate.
template <typename Base>
struct ObVectorArithOpWrap : public Base
{
  constexpr static bool is_raw_op_supported() { return true; }
  template <typename ResVector, typename LeftVector, typename RightVector, typename... Args>
  int operator()(ResVector &res_vec, const LeftVector &left_vec, const RightVector &right_vec,
                 const int64_t idx, Args &... args) const
  {
    Base::raw_op(*reinterpret_cast<typename Base::RES_RAW_TYPE *>(
                   const_cast<char *>(res_vec.get_payload(idx))),
                 *reinterpret_cast<const typename Base::L_RAW_TYPE *>(left_vec.get_payload(idx)),
                 *reinterpret_cast<const typename Base::R_RAW_TYPE *>(right_vec.get_payload(idx)),
                 args...);
    res_vec.set_length(idx, sizeof(typename Base::RES_RAW_TYPE));
    return Base::raw_check(
      *reinterpret_cast<typename Base::RES_RAW_TYPE *>(
        const_cast<char *>(res_vec.get_payload(idx))),
      *reinterpret_cast<const typename Base::L_RAW_TYPE *>(left_vec.get_payload(idx)),
      *reinterpret_cast<const typename Base::R_RAW_TYPE *>(right_vec.get_payload(idx)));
  }

  template <typename ResVector, typename LeftVector, typename RightVector, typename... Args>
  static int vector_op(ResVector &res_vec, const LeftVector &left_vec,
                       const RightVector &right_vec, const int64_t idx, Args &... args)
  {
    return ObVectorArithOpWrap()(res_vec, left_vec, right_vec, idx, args...);
  }

  template <typename ResVector, typename LeftVector, typename RightVector, typename... Args>
  static int null_check_vector_op(ResVector &res_vec, const LeftVector &left_vec,
                       const RightVector &right_vec, const int64_t idx, Args &... args)
  {
    int ret = OB_SUCCESS;
    if (left_vec.is_null(idx) || right_vec.is_null(idx)) {
      res_vec.set_null(idx);
    } else {
      res_vec.unset_null(idx);
      ret = ObVectorArithOpWrap()(res_vec, left_vec, right_vec, idx, args...);
    }
    return ret;
  }
};

} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_EXPR_OB_BATCH_EVAL_UTIL_H_
