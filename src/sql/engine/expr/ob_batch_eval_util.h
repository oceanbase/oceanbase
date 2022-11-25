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
namespace oceanbase
{
namespace sql
{

using common::OB_SUCCESS;

// Batch evaluate operand of binary operator
int binary_operand_batch_eval(const ObExpr &expr,
                              ObEvalCtx &ctx,
                              const ObBitVector &skip,
                              const int64_t size,
                              const bool null_short_circuit);

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
            ArithOp::raw_op(iter.raw(i), l_it.raw(i), r_it.raw(i));
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

  static void raw_op(RES_RAW_TYPE &, const L_RAW_TYPE &, const R_RAW_TYPE) {}
  static int raw_check(const RES_RAW_TYPE &, const L_RAW_TYPE &, const R_RAW_TYPE &)
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

template <typename DatumFunctor, typename... Args>
int def_batch_arith_op_by_datum_func(BATCH_EVAL_FUNC_ARG_DECL, Args &...args)
{
  return def_batch_arith_op<ObWrapArithOpNullCheck<DatumFunctor>, Args...>(
      BATCH_EVAL_FUNC_ARG_LIST, args...);
}

// Wrap arith datum operate from raw operate.
template <typename Base>
struct ObArithOpWrap : public Base
{
  constexpr static bool is_raw_op_supported() { return true; }
  int operator()(ObDatum &res, const ObDatum &l, const ObDatum &r) const
  {
    Base::raw_op(
        *const_cast<typename Base::RES_RAW_TYPE *>(
            reinterpret_cast<const typename Base::RES_RAW_TYPE *>(res.ptr_)),
        *reinterpret_cast<const typename Base::L_RAW_TYPE *>(l.ptr_),
        *reinterpret_cast<const typename Base::R_RAW_TYPE *>(r.ptr_));
    res.pack_ = sizeof(typename Base::RES_RAW_TYPE);
    return Base::raw_check(*reinterpret_cast<const typename Base::RES_RAW_TYPE *>(res.ptr_),
                           *reinterpret_cast<const typename Base::L_RAW_TYPE *>(l.ptr_),
                           *reinterpret_cast<const typename Base::R_RAW_TYPE *>(r.ptr_));
  }

  static int datum_op(ObDatum &res, const ObDatum &l, const ObDatum &r)
  {
    int ret = OB_SUCCESS;
    if (l.is_null() || r.is_null()) {
      res.set_null();
    } else {
      ret = ObArithOpWrap()(res, l, r);
    }
    return ret;
  }
};

} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_EXPR_OB_BATCH_EVAL_UTIL_H_
