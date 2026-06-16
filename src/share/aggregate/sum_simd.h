/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_AGGREGATE_SUM_SIMD_H_
#define OCEANBASE_SHARE_AGGREGATE_SUM_SIMD_H_

#include "share/aggregate/sum.h"

#if defined(__ARM_NEON)
#include <arm_neon.h>
#endif

namespace oceanbase
{
namespace share
{
namespace aggregate
{

// ---------------------------------------------------------------------------
// SIMD / multi-issue fast-path kernel for SUM(decint32) -> int128.
//
// Scalar path (util.h add_values) costs one 128-bit multi-limb add per row
// even when the input is int32. This kernel accumulates in 64-bit and merges
// into int128 only once per batch. The add path performs no per-row overflow
// check (overflow is validated only at collect time for DEC_INT512 mysql
// mode), so this kernel matches its semantics exactly.
// ---------------------------------------------------------------------------

#if defined(__ARM_NEON)
// int32 -> int128: NEON widening add (SADDW). Each int32 is sign-extended to
// int64 before accumulation -- no int32+int32 add ever happens. 16 elems per
// iteration into 4 independent int64x2 accumulators (8 lanes) to fill ILP.
// int64 lanes cannot overflow within one batch (|v| < 2^31, batch << 2^35).
OB_INLINE void neon_sum_i32_to_i128(const int32_t *data, const int64_t n, int128_t &acc128)
{
  int64x2_t a0 = vdupq_n_s64(0), a1 = vdupq_n_s64(0);
  int64x2_t a2 = vdupq_n_s64(0), a3 = vdupq_n_s64(0);
  int64_t i = 0;
  for (; i + 16 <= n; i += 16) {
    int32x4_t v0 = vld1q_s32(data + i);
    int32x4_t v1 = vld1q_s32(data + i + 4);
    int32x4_t v2 = vld1q_s32(data + i + 8);
    int32x4_t v3 = vld1q_s32(data + i + 12);
    a0 = vaddw_s32(a0, vget_low_s32(v0)); a0 = vaddw_high_s32(a0, v0);
    a1 = vaddw_s32(a1, vget_low_s32(v1)); a1 = vaddw_high_s32(a1, v1);
    a2 = vaddw_s32(a2, vget_low_s32(v2)); a2 = vaddw_high_s32(a2, v2);
    a3 = vaddw_s32(a3, vget_low_s32(v3)); a3 = vaddw_high_s32(a3, v3);
  }
  int64x2_t s = vaddq_s64(vaddq_s64(a0, a1), vaddq_s64(a2, a3));
  int64_t partial = vgetq_lane_s64(s, 0) + vgetq_lane_s64(s, 1);
  for (; i < n; ++i) { partial += data[i]; }  // tail: int64 += sext(int32)
  acc128 += partial;                          // one int128 add per batch
}
#endif // __ARM_NEON

#if defined(__ARM_NEON)
// ===========================================================================
// Delegate base + final fast-path class
//
// SumAggregate<in_tc, out_tc> is `final` (intentionally retained). It is also
// stateless (state lives in agg_ctx / agg_cell), so we compose it -- not
// inherit -- to reuse all per-row / collect / rollup behavior. The NeonSum*
// class is its own CRTP leaf on BatchAggregateWrapper, overriding only
// add_batch_rows to take the SIMD path; everything else forwards to the
// embedded SumAggregate.
// ===========================================================================
template <typename Derived, VecValueTypeClass in_tc>
class NeonSumDelegateBase : public BatchAggregateWrapper<Derived>
{
  using Inner = SumAggregate<in_tc, VEC_TC_DEC_INT128>;
  static_assert(in_tc == VEC_TC_DEC_INT32, "NeonSumDelegateBase: only decint32 -> decint128");
  static_assert(sizeof(int128_t) == 16, "int128_t must be 16 bytes");

public:
  static const constexpr VecValueTypeClass IN_TC  = in_tc;
  static const constexpr VecValueTypeClass OUT_TC = VEC_TC_DEC_INT128;

  NeonSumDelegateBase() : inner_() {}

protected:
  // Shared SIMD entry: validate fast-path preconditions, take kernel, otherwise
  // delegate to inner_'s scalar add_batch_rows path.
  template <typename RawT, typename Kernel>
  OB_INLINE int add_batch_simd(RuntimeContext &agg_ctx, const int32_t agg_col_id,
                               const sql::ObBitVector &skip, const sql::EvalBound &bound,
                               char *agg_cell, const RowSelector &row_sel, Kernel &&kern)
  {
    int ret = OB_SUCCESS;
    ObAggrInfo &info = agg_ctx.aggr_infos_.at(agg_col_id);
    sql::ObEvalCtx &ctx = agg_ctx.eval_ctx_;
    const bool can_simd = (info.param_exprs_.count() == 1)
                          && !agg_ctx.removal_info_.enable_removal_opt_
                          && row_sel.is_empty() && bound.get_all_rows_active()
                          && (agg_cell != nullptr);
    bool handled = false;
    if (can_simd) {
      ObIVector *ivec = info.param_exprs_.at(0)->get_vector(ctx);
      if (ivec->get_format() == common::VEC_FIXED && !ivec->has_null()) {
        using FixFmt = ObFixedLengthFormat<RawT>;
        FixFmt *cols = static_cast<FixFmt *>(ivec);
        const RawT *data = reinterpret_cast<const RawT *>(cols->get_payload(bound.start()));
        const int64_t n = bound.end() - bound.start();
        int128_t &acc = *reinterpret_cast<int128_t *>(agg_cell);
        kern(data, n, acc);
        if (n > 0) {
          NotNullBitVector &not_nulls = agg_ctx.locate_notnulls_bitmap(agg_col_id, agg_cell);
          not_nulls.set(agg_col_id);
        }
        handled = true;
      }
    }
    if (!handled) {
      ret = inner_.add_batch_rows(agg_ctx, agg_col_id, skip, bound, agg_cell, row_sel);
    }
    return ret;
  }

  Inner inner_;

public:
  // ----- virtual overrides forwarded to inner_ -----------------------------
  // `final` on these is intentional: the leaf class is also `final`, so this
  // helps the compiler devirtualize / inline the inner forwarding call (the
  // multi-group scatter path does one add_one_row per row).
  OB_INLINE int add_one_row(RuntimeContext &agg_ctx, int64_t batch_idx, int64_t batch_size,
                            const bool is_null, const char *data, const int32_t data_len,
                            int32_t agg_col_idx, char *agg_cell) override final
  {
    return inner_.add_one_row(agg_ctx, batch_idx, batch_size, is_null, data, data_len,
                              agg_col_idx, agg_cell);
  }

  int rollup_aggregation(RuntimeContext &agg_ctx, const int32_t agg_col_idx,
                         AggrRowPtr group_row, AggrRowPtr rollup_row,
                         int64_t cur_rollup_group_idx,
                         int64_t max_group_cnt = INT64_MIN) override final
  {
    return inner_.rollup_aggregation(agg_ctx, agg_col_idx, group_row, rollup_row,
                                     cur_rollup_group_idx, max_group_cnt);
  }

  inline int64_t get_batch_calc_info(RuntimeContext &agg_ctx, int32_t agg_col_idx,
                                     char *agg_cell) override final
  {
    return inner_.get_batch_calc_info(agg_ctx, agg_col_idx, agg_cell);
  }

  // ----- CRTP template hooks forwarded to inner_ ---------------------------
  // These are detected via SFINAE in util.h (AddRow / AddNullableRow / etc).
  template <typename ColumnFmt>
  OB_INLINE int add_row(RuntimeContext &agg_ctx, ColumnFmt &columns, const int32_t row_num,
                        const int32_t agg_col_id, char *agg_cell, void *tmp_res,
                        int64_t &calc_info)
  {
    return inner_.template add_row<ColumnFmt>(agg_ctx, columns, row_num, agg_col_id, agg_cell,
                                              tmp_res, calc_info);
  }

  template <typename ColumnFmt>
  OB_INLINE int sub_row(RuntimeContext &agg_ctx, ColumnFmt &columns, const int32_t row_num,
                        const int32_t agg_col_id, char *agg_cell, void *tmp_res,
                        int64_t &calc_info)
  {
    return inner_.template sub_row<ColumnFmt>(agg_ctx, columns, row_num, agg_col_id, agg_cell,
                                              tmp_res, calc_info);
  }

  template <typename ColumnFmt>
  OB_INLINE int add_nullable_row(RuntimeContext &agg_ctx, ColumnFmt &columns,
                                 const int32_t row_num, const int32_t agg_col_id, char *agg_cell,
                                 void *tmp_res, int64_t &calc_info)
  {
    return inner_.template add_nullable_row<ColumnFmt>(agg_ctx, columns, row_num, agg_col_id,
                                                       agg_cell, tmp_res, calc_info);
  }

  template <typename ColumnFmt>
  OB_INLINE int add_or_sub_row(RuntimeContext &agg_ctx, ColumnFmt &columns,
                               const int32_t row_num, const int32_t agg_col_id, char *agg_cell,
                               void *tmp_res, int64_t &calc_info)
  {
    return inner_.template add_or_sub_row<ColumnFmt>(agg_ctx, columns, row_num, agg_col_id,
                                                     agg_cell, tmp_res, calc_info);
  }

  template <typename ColumnFmt>
  int collect_group_result(RuntimeContext &agg_ctx, const sql::ObExpr &agg_expr,
                           const int32_t agg_col_id, const char *agg_cell,
                           const int32_t agg_cell_len)
  {
    return inner_.template collect_group_result<ColumnFmt>(agg_ctx, agg_expr, agg_col_id,
                                                           agg_cell, agg_cell_len);
  }
};

// Functor wrapping neon_sum_i32_to_i128 as the SIMD kernel passed to add_batch_simd.
struct NeonSumI32ToI128Kernel {
  OB_INLINE void operator()(const int32_t *d, int64_t n, int128_t &a) const
  {
    neon_sum_i32_to_i128(d, n, a);
  }
};

// int32 -> int128: NEON widening (SADDW)
class NeonSumI32ToI128 final
  : public NeonSumDelegateBase<NeonSumI32ToI128, VEC_TC_DEC_INT32>
{
public:
  int add_batch_rows(RuntimeContext &agg_ctx, const int32_t agg_col_id,
                     const sql::ObBitVector &skip, const sql::EvalBound &bound,
                     char *agg_cell, const RowSelector row_sel = RowSelector{}) override
  {
    return this->template add_batch_simd<int32_t>(
        agg_ctx, agg_col_id, skip, bound, agg_cell, row_sel,
        NeonSumI32ToI128Kernel{});
  }

  TO_STRING_KV("aggregate", "neon_sum_i32_i128");
};
#endif // __ARM_NEON

} // end namespace aggregate
} // end namespace share
} // end namespace oceanbase

#endif // OCEANBASE_SHARE_AGGREGATE_SUM_SIMD_H_
