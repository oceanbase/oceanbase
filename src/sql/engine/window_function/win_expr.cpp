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

#include "win_expr.h"
#include "share/aggregate/iaggregate.h"
#include "sql/engine/window_function/ob_window_function_vec_op.h"
#include "sql/engine/expr/ob_expr_truncate.h"
#include "lib/timezone/ob_time_convert.h"

namespace oceanbase
{
namespace sql
{
namespace winfunc
{
static int eval_bound_exprs(WinExprEvalCtx &ctx, const int64_t row_start, const int64_t batch_size,
                            const ObBitVector &skip, const bool is_upper);

static int calc_borders_for_current_row(WinExprEvalCtx &ctx, const int64_t row_start,
                                        const int64_t batch_size, const ObBitVector &skip,
                                        const bool is_upper);

static int eval_and_check_between_literal(WinExprEvalCtx &ctx, ObBitVector &eval_skip,
                                          const ObExpr *between_expr, const int64_t batch_size,
                                          int64_t *pos_arr);
static int calc_borders_for_rows_between(WinExprEvalCtx &ctx, const int64_t row_start,
                                         const int64_t batch_size, const ObBitVector &eval_skip,
                                         const ObExpr *between_expr, const bool is_preceding,
                                         const bool is_upper, int64_t *pos_arr);

static int calc_borders_for_no_sort_expr(WinExprEvalCtx &ctx,
                                         const int64_t batch_size, const ObBitVector &eval_skip,
                                         const ObExpr *bound_expr, const bool is_upper,
                                         int64_t *pos_arr);

static int calc_borders_for_sort_expr(WinExprEvalCtx &ctx, const ObExpr *bound_expr,
                                      const int64_t batch_size, const int64_t row_start,
                                      ObBitVector &eval_skip, const bool is_upper,
                                      int64_t *pos_arr);

static int cmp_prev_row(WinExprEvalCtx &ctx, const int64_t cur_idx, int &cmp_ret);

template<VecValueTypeClass vec_tc>
struct int_trunc
{
  static int get(const char *payload, const int32_t len,const ObDatumMeta &meta, int64_t &value);
};

// WinExprHelper
template <typename Derived>
int WinExprWrapper<Derived>::process_partition(WinExprEvalCtx &ctx, const int64_t part_start,
                                               const int64_t part_end, const int64_t row_start,
                                               const int64_t row_end, const ObBitVector &skip)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(part_start > row_start || part_end < row_end)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid partition", K(part_start), K(part_end), K(row_start), K(row_end));
    } else if (OB_UNLIKELY(part_start >= part_end || row_start >= row_end)) {
      LOG_DEBUG("empty partition", K(part_start), K(part_end), K(row_start), K(row_end));
    } else {
      const ObCompactRow *prev_row = nullptr, *cur_row = nullptr;
      Frame prev_frame, cur_frame;
      bool whole_frame = true, valid_frame = true;
      ObEvalCtx::BatchInfoScopeGuard guard(ctx.win_col_.op_.get_eval_ctx());
      guard.set_batch_size(row_end - row_start);
      if (OB_FAIL(eval_bound_exprs(ctx, row_start, row_end - row_start, skip, true))) {
        LOG_WARN("eval upper bound failed", K(ret));
      } else if (OB_FAIL(eval_bound_exprs(ctx, row_start, row_end-row_start, skip, false))) {
        LOG_WARN("eval lower bound failed", K(ret));
      } else if (is_aggregate_expr()) {
        AggrExpr *agg_expr = reinterpret_cast<AggrExpr *>(this);
        prev_frame = agg_expr->last_valid_frame_;
        if (row_start  > part_start) {
          ctx.win_col_.agg_ctx_->removal_info_ = agg_expr->last_removal_info_;
        }
        // TODO: maybe prefetch agg rows is a good idea
        int prev_calc_idx = -1;
        for (int row_idx = row_start; OB_SUCC(ret) && row_idx < row_end; row_idx++) {
          int32_t batch_idx = row_idx - row_start;
          if (skip.at(batch_idx)) {
            continue;
          }
          guard.set_batch_idx(batch_idx);
          aggregate::AggrRowPtr agg_row = ctx.win_col_.aggr_rows_[batch_idx];
          bool is_null = false; // useless for aggregation function
          if (OB_FAIL(update_frame(ctx, prev_frame, cur_frame, batch_idx, row_start, whole_frame,
                                   valid_frame))) {
            LOG_WARN("update frame failed", K(ret));
          } else if (OB_UNLIKELY(!valid_frame)) {
            if (OB_FAIL(AggrExpr::set_result_for_invalid_frame(ctx, agg_row))) {
              LOG_WARN("set result for invalid frame failed", K(ret));
            }
          } else if (prev_frame == cur_frame) {
            // for aggregate function, same frame means same results
            // just copy aggr row
            char *copied_row = (prev_calc_idx == -1 ? agg_expr->last_aggr_row_ :
                                                      ctx.win_col_.aggr_rows_[prev_calc_idx]);
            if (OB_FAIL(copy_aggr_row(ctx, copied_row, agg_row))) {
              LOG_WARN("copy aggr row failed", K(ret));
            }
          } else if (whole_frame) {
            ctx.win_col_.agg_ctx_->removal_info_.reset_for_new_frame();
            if (OB_FAIL(static_cast<Derived *>(this)->process_window(ctx, cur_frame, row_idx, agg_row, is_null))) {
              LOG_WARN("eval aggregate function failed", K(ret));
            }
          } else if (OB_FAIL(static_cast<Derived *>(this)->accum_process_window(
                       ctx, cur_frame, prev_frame, row_idx, agg_row, is_null))) {
            LOG_WARN("increase evaluation function failed", K(ret));
          }
          if (OB_FAIL(ret)) {
          } else {
            prev_frame = cur_frame;
            prev_calc_idx = batch_idx;
          }
        } // end for
        if (OB_SUCC(ret) && prev_calc_idx != -1) {
          agg_expr->last_valid_frame_ = prev_frame;
          if (row_end < part_end) {
            agg_expr->last_removal_info_ = ctx.win_col_.agg_ctx_->removal_info_;
          }
          int32_t row_size = ctx.win_col_.agg_ctx_->row_meta().row_size_;
          void *tmp_buf = nullptr;
          if (agg_expr->last_aggr_row_ != nullptr) {
            if (OB_FAIL(copy_aggr_row(ctx, ctx.win_col_.aggr_rows_[prev_calc_idx], agg_expr->last_aggr_row_))) {
              LOG_WARN("copy aggr row failed", K(ret));
            }
          } else if (OB_ISNULL(tmp_buf = ctx.reserved_buf(row_size))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("allocate memory failed", K(ret));
          } else if (OB_FAIL(copy_aggr_row(ctx, ctx.win_col_.aggr_rows_[prev_calc_idx], (char *)tmp_buf))) {
            LOG_WARN("copy aggr row failed", K(ret));
          } else {
            agg_expr->last_aggr_row_ = (char *)tmp_buf;
          }
        }
      } else {
        void *extra = nullptr;
        int32_t non_aggr_row_size = ctx.win_col_.non_aggr_reserved_row_size();
        bool is_null = false;
        if (OB_FAIL(static_cast<Derived *>(this)->generate_extra(ctx.allocator_, extra))) {
          LOG_WARN("generate extra data failed", K(ret));
        } else {
          MEMSET(ctx.win_col_.non_aggr_results_, 0, non_aggr_row_size * (row_end - row_start));
          ctx.extra_ = extra;
        }
        for (int row_idx = row_start; OB_SUCC(ret) && row_idx < row_end; row_idx++) {
          int32_t batch_idx = row_idx - row_start;
          if (skip.at(batch_idx)) {
            continue;
          }
          guard.set_batch_idx(batch_idx);
          is_null = false;
          char *non_aggr_res = ctx.win_col_.non_aggr_results_ + non_aggr_row_size * batch_idx;
          if (OB_FAIL(update_frame(ctx, prev_frame, cur_frame, batch_idx, row_start, whole_frame,
                                   valid_frame))) {
            LOG_WARN("update frame failed", K(ret));
          } else if (OB_UNLIKELY(!valid_frame)) {
            is_null = true;
          } else if (OB_FAIL(static_cast<Derived *>(this)->process_window(ctx, cur_frame, row_idx,
                                                                          non_aggr_res, is_null))) {
            LOG_WARN("process window failed", K(ret));
          }
          if (OB_FAIL(ret)) {
          } else if (is_null) {
            ctx.win_col_.null_nonaggr_results_->set(batch_idx);
          }
        } // end for
      }
      // collect partition results
      if (OB_SUCC(ret)) {
        ObExpr *wf_expr = ctx.win_col_.wf_info_.expr_;
        ObEvalCtx &eval_ctx = ctx.win_col_.op_.get_eval_ctx();
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(static_cast<Derived *>(this)->collect_part_results(ctx, row_start,
                                                                              row_end, skip))) {
          LOG_WARN("collect partition results failed", K(ret));
        }
      }
    }
  return ret;
}

template <typename Derived>
int WinExprWrapper<Derived>::update_frame(WinExprEvalCtx &ctx, const Frame &prev_frame,
                                          Frame &new_frame, const int64_t idx,
                                          const int64_t row_start, bool &whole_frame,
                                          bool &valid_frame)
{
  int ret = OB_SUCCESS;
  int64_t part_first_idx = ctx.win_col_.part_first_row_idx_;
  int64_t part_end_idx = ctx.win_col_.op_.get_part_end_idx();
  int64_t *upper_pos_arr = ctx.win_col_.op_.batch_ctx_.upper_pos_arr_;
  int64_t *lower_pos_arr = ctx.win_col_.op_.batch_ctx_.lower_pos_arr_;
  Frame part_frame(part_first_idx, part_end_idx);
  new_frame.head_ = upper_pos_arr[idx];
  new_frame.tail_ = lower_pos_arr[idx];
  valid_frame = true;
  whole_frame = true;
  const ObWindowFunctionVecSpec &spec = static_cast<const ObWindowFunctionVecSpec &>(ctx.win_col_.op_.get_spec());
  if (OB_UNLIKELY(new_frame.head_ == INT64_MAX || new_frame.tail_ == INT64_MAX)) {
    LOG_DEBUG("invalid frame", K(new_frame));
    valid_frame = false;
  } else if (FALSE_IT(valid_frame = Frame::valid_frame(part_frame, new_frame))) {
  } else if (!valid_frame) {
  } else if (spec.single_part_parallel_) {
    // whole frame, no need to update
  } else {
    Frame::prune_frame(part_frame, new_frame);
    if (static_cast<Derived *>(this)->is_aggregate_expr()) {
      bool can_inv = (ctx.win_col_.wf_info_.remove_type_ != common::REMOVE_INVALID);
      aggregate::Processor *processor  = reinterpret_cast<AggrExpr *>(this)->aggr_processor_;
      if (prev_frame.is_valid()
          && !Frame::need_restart_aggr(can_inv, prev_frame, new_frame,
                                       ctx.win_col_.agg_ctx_->removal_info_,
                                       ctx.win_col_.wf_info_.remove_type_)) {
        whole_frame = false;
      }
    }
  }
  LOG_DEBUG("update frame", K(ret), K(valid_frame), K(prev_frame), K(new_frame), K(idx),
            K(whole_frame), K(ctx.win_col_.wf_info_.remove_type_));
  return ret;
}

template<typename Derived>
int WinExprWrapper<Derived>::copy_aggr_row(WinExprEvalCtx &ctx, const char *src_row, char *dst_row)
{
  int ret = OB_SUCCESS;
  aggregate::RuntimeContext *agg_ctx = ctx.win_col_.agg_ctx_;
  MEMCPY(dst_row, src_row, ctx.win_col_.agg_ctx_->row_meta().row_size_);
  if (!agg_ctx->row_meta().is_var_len(0)) {// do nothing
  } else {
    int32_t cell_len = agg_ctx->row_meta().get_cell_len(0, dst_row);
    int64_t &payload_addr =
      *reinterpret_cast<int64_t *>(agg_ctx->row_meta().locate_cell_payload(0, dst_row));
    const char *payload = reinterpret_cast<const char *>(payload_addr);
    bool is_not_null = agg_ctx->row_meta().locate_notnulls_bitmap(dst_row).at(0);

    if (OB_LIKELY(is_not_null && cell_len > 0)) {
      void *tmp_buf = ctx.reserved_buf(cell_len);
      if (OB_ISNULL(tmp_buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret));
      } else {
        MEMCPY(tmp_buf, payload, cell_len);
        payload_addr = reinterpret_cast<int64_t>(tmp_buf);
      }
    } else {
      payload_addr = 0;
    }
  }
  return ret;
}

int NonAggrWinExpr::eval_param_int_value(ObExpr *param, ObEvalCtx &ctx, const bool need_check_valid,
                                         const bool need_nmb, ParamStatus &status)
{
  int ret = OB_SUCCESS;
  int64_t mock_skip_data = 0;
  ObBitVector *skip = to_bit_vector(&mock_skip_data);
  ObEvalCtx::BatchInfoScopeGuard guard(ctx);
  guard.set_batch_size(1);
  guard.set_batch_idx(0);
  EvalBound bound(1, true);
  bool is_valid_param = true;
  bool is_null = false;
  int64_t val = 0;
  if (OB_FAIL(param->eval_vector(ctx, *skip, bound))) {
    LOG_WARN("eval failed", K(ret));
  } else if (param->get_vector(ctx)->is_null(0)) {
    is_null = true;
    is_valid_param = !need_check_valid;
    status.is_null_ = true;
    status.calculated_ = true;
  } else if (need_nmb || param->obj_meta_.is_number() || param->obj_meta_.is_number_float()) {
    number::ObNumber result_nmb(param->get_vector(ctx)->get_number(0));
    is_valid_param = !need_check_valid || !result_nmb.is_negative();
    if (OB_FAIL(result_nmb.extract_valid_int64_with_trunc(val))) {
      LOG_WARN("extract int64_t value failed", K(ret));
    }
  } else if (param->obj_meta_.is_decimal_int()) {
    const ObDecimalInt *decint = param->get_vector(ctx)->get_decimal_int(0);
    int32_t in_bytes = param->get_vector(ctx)->get_length(0);
    ObDecimalIntBuilder trunc_res_val;
    const int16_t in_prec = param->datum_meta_.precision_;
    const int16_t in_scale = param->datum_meta_.scale_;
    const int16_t out_scale = 0;
    is_valid_param = !need_check_valid || !wide::is_negative(decint, in_bytes);
    ObDatum in_datum;
    in_datum.ptr_ = reinterpret_cast<const char *>(decint);
    in_datum.len_ = in_bytes;
    if (in_scale == out_scale) {
      trunc_res_val.from(decint, in_bytes);
    } else if (OB_FAIL(ObExprTruncate::do_trunc_decimalint(in_prec, in_scale, in_prec, out_scale,
                                                           out_scale, in_datum, trunc_res_val))) {
      LOG_WARN("truncate decimal int failed", K(ret));
    }
    bool is_in_val_valid = false;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(wide::check_range_valid_int64(trunc_res_val.get_decimal_int(),
                                                     trunc_res_val.get_int_bytes(), is_in_val_valid,
                                                     val))) {
      LOG_WARN("check valid int64 failed", K(ret));
    } else if (!is_in_val_valid) {
      ret = OB_DATA_OUT_OF_RANGE;
      LOG_WARN("res val is not a valid int64", K(ret));
    }
  } else if (ob_is_int_tc(param->datum_meta_.type_)) {
    val = param->get_vector(ctx)->get_int(0);
    is_valid_param = !need_check_valid || val >= 0;
  } else if (ob_is_uint_tc(param->datum_meta_.type_)) {
    uint64_t tmp_val = param->get_vector(ctx)->get_uint(0);
    is_valid_param = !need_check_valid || static_cast<int64_t>(tmp_val) >= 0;
    if (tmp_val > INT64_MAX && is_valid_param) {
      ret = OB_DATA_OUT_OF_RANGE;
      LOG_WARN("int64 out of range", K(ret), K(tmp_val), K(INT64_MAX));
    } else {
      val = static_cast<int64_t>(tmp_val);
    }
  } else if (ob_is_float_tc(param->datum_meta_.type_)) {
    float tmp_val = param->get_vector(ctx)->get_float(0);
    is_valid_param = !need_check_valid || tmp_val >= 0;
    if (tmp_val > INT64_MAX && is_valid_param) {
      ret = OB_DATA_OUT_OF_RANGE;
      LOG_WARN("int64 out of range", K(ret), K(tmp_val));
    } else {
      val = static_cast<int64_t>(tmp_val);
    }
  } else if (ob_is_double_tc(param->datum_meta_.type_)) {
    double tmp_val = param->get_vector(ctx)->get_double(0);
    is_valid_param = !need_check_valid || tmp_val >= 0;
    if (tmp_val > INT64_MAX && is_valid_param) {
      ret = OB_DATA_OUT_OF_RANGE;
      LOG_WARN("int64 out of range", K(ret), K(tmp_val));
    } else {
      val = static_cast<int64_t>(tmp_val);
    }
  } else if (ob_is_bit_tc(param->datum_meta_.type_)) {
    uint64_t tmp_val = param->get_vector(ctx)->get_bit(0);
    is_valid_param = !need_check_valid || static_cast<int64_t>(tmp_val) >= 0;
    if (tmp_val > INT64_MAX && is_valid_param) {
      ret = OB_DATA_OUT_OF_RANGE;
      LOG_WARN("int64 out of range", K(ret), K(tmp_val), K(INT64_MAX));
    } else {
      val = static_cast<int64_t>(tmp_val);
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not support type", K(ret), K(*param));
  }
  if (OB_SUCC(ret) && !status.is_null_) {
    status.calculated_ = true;
    status.int_val_ = val;
  }
  if (OB_SUCC(ret) && !is_valid_param) {
    ret = OB_ERR_WINDOW_FRAME_ILLEGAL;
    LOG_WARN("frame start or end is negative, NULL or of non-integral type", K(ret), K(val));
  }
  return ret;
}

#define SET_NON_AGG_DATAS(fmt)                                                                     \
  do {                                                                                             \
    fmt *data = static_cast<fmt *>(non_agg_expr->get_vector(eval_ctx));                            \
    int32_t offset = 0, step = ctx.win_col_.non_aggr_reserved_row_size();                          \
    for (int i = 0; OB_SUCC(ret) && i < batch_size; i++, offset += step) {                         \
      if (skip.at(i)) { continue; }                                                                \
      guard.set_batch_idx(i);                                                                      \
      char *res_row = ctx.win_col_.non_aggr_results_ + offset;                                     \
      if (!ctx.win_col_.null_nonaggr_results_->at(i)) {                                            \
        if (is_fixed_len_data) {                                                                   \
          payload = res_row;                                                                       \
          len = ctx.win_col_.non_aggr_reserved_row_size();                                         \
          data->set_payload(i, payload, len);                                                      \
        } else if (vec_tc == VEC_TC_NUMBER) {                                                      \
          data->set_number(i, *reinterpret_cast<const number::ObCompactNumber *>(res_row));        \
        } else {                                                                                   \
          payload = reinterpret_cast<const char *>(*reinterpret_cast<int64_t *>(res_row));         \
          len = *reinterpret_cast<int32_t *>(res_row + sizeof(char *));                            \
          char *res_buf = non_agg_expr->get_str_res_mem(eval_ctx, len);                            \
          if (OB_ISNULL(res_buf)) {                                                                \
            ret = OB_ALLOCATE_MEMORY_FAILED;                                                       \
            LOG_WARN("allocate memory failed", K(ret));                                            \
          } else {                                                                                 \
            MEMCPY(res_buf, payload, len);                                                         \
            data->set_payload_shallow(i, res_buf, len);                                            \
          }                                                                                        \
        }                                                                                          \
      } else {                                                                                     \
        data->set_null(i);                                                                         \
      }                                                                                            \
    }                                                                                              \
  } while (false)

#define SET_NON_AGG_FIXED_DATAS(vec_tc)                                                            \
  case (vec_tc): {                                                                                 \
    SET_NON_AGG_DATAS(ObFixedLengthFormat<RTCType<vec_tc>>);                                       \
  } break

int NonAggrWinExpr::collect_part_results(WinExprEvalCtx &ctx, const int64_t row_start,
                                         const int64_t row_end, const ObBitVector &skip)
{
  int ret = OB_SUCCESS;
  ObExpr *non_agg_expr = ctx.win_col_.wf_info_.expr_;
  int64_t batch_size = row_end - row_start;
  ObEvalCtx &eval_ctx = ctx.win_col_.op_.get_eval_ctx();
  VectorFormat fmt = non_agg_expr->get_format(eval_ctx);
  VecValueTypeClass vec_tc = non_agg_expr->get_vec_value_tc();
  bool is_fixed_len_data = is_fixed_length_vec(vec_tc);
  const char *payload = nullptr;
  ObEvalCtx::BatchInfoScopeGuard guard(eval_ctx);
  guard.set_batch_size(batch_size);
  int32_t len = 0;
  switch (fmt) {
  case common::VEC_UNIFORM: {
    SET_NON_AGG_DATAS(ObUniformFormat<false>);
    break;
  }
  case common::VEC_FIXED: {
    switch (vec_tc) {
      LST_DO_CODE(SET_NON_AGG_FIXED_DATAS, FIXED_VEC_LIST);
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected tc", K(vec_tc));
    }
    }
    break;
  }
  case common::VEC_DISCRETE: {
    SET_NON_AGG_DATAS(ObDiscreteFormat);
    break;
  }
  default: {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected format", K(ret));
  }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("collect part results failed", K(ret));
  } else {
    non_agg_expr->set_evaluated_projected(eval_ctx);
  }
  return ret;
}

template <ObItemType rank_op>
int RankLikeExpr<rank_op>::process_window(WinExprEvalCtx &ctx, const Frame &frame,
                                          const int64_t row_idx, char *res, bool &is_null)
{
  int ret = OB_SUCCESS;
  bool equal_with_prev_row = false;
  is_null = false;
  if (row_idx != frame.head_) {
    int cmp_ret = 0;
    if (OB_FAIL(cmp_prev_row(ctx, row_idx, cmp_ret))) {
      LOG_WARN("compare previous row failed", K(ret));
    } else {
      equal_with_prev_row = (cmp_ret == 0);
    }
  } else {
    // reset rank
    rank_of_prev_row_ = 0;
  }
  if (OB_SUCC(ret)) {
    int64_t rank = -1;
    if (equal_with_prev_row) {
      rank = rank_of_prev_row_;
    } else if (rank_op == T_WIN_FUN_RANK || rank_op == T_WIN_FUN_PERCENT_RANK) {
      rank = row_idx - frame.head_ + 1;
    } else if (rank_op == T_WIN_FUN_DENSE_RANK) {
      rank = rank_of_prev_row_ + 1;
    }
    LOG_DEBUG("calculate rank result", K(rank_op), K(rank), K(frame));
    if (rank_op == T_WIN_FUN_PERCENT_RANK) {
      if (ob_is_number_tc(ctx.win_col_.wf_info_.expr_->datum_meta_.type_)) {
        // in mysql mode, percent rank may return double
        if (0 == frame.tail_ - frame.head_ - 1) {
          number::ObNumber zero_nmb;
          zero_nmb.set_zero();
          MEMCPY(res, &(zero_nmb.d_), sizeof(ObNumberDesc));
        } else {
          number::ObNumber numerator;
          number::ObNumber denominator;
          number::ObNumber res_nmb;
          ObNumStackAllocator<3> tmp_alloc;
          if (OB_FAIL(numerator.from(rank - 1, tmp_alloc))) {
            LOG_WARN("failed to build number from int64_t", K(ret));
          } else if (OB_FAIL(denominator.from(frame.tail_ - frame.head_ - 1, tmp_alloc))) {
            LOG_WARN("failed to build number from int64_t", K(ret));
          } else if (OB_FAIL(numerator.div(denominator, res_nmb, tmp_alloc))) {
            LOG_WARN("failed to div number", K(ret));
          } else {
            number::ObCompactNumber *res_cnum = reinterpret_cast<number::ObCompactNumber *>(res);
            res_cnum->desc_ = res_nmb.d_;
            MEMCPY(&(res_cnum->digits_[0]), res_nmb.get_digits(), sizeof(uint32_t) * res_nmb.d_.len_);
          }
        }
      } else if (ObDoubleType == ctx.win_col_.wf_info_.expr_->datum_meta_.type_) {
        if (0 == frame.tail_ - frame.head_ - 1) {
          *reinterpret_cast<double *>(res) = 0;
        } else {
          double numerator = static_cast<double>(rank - 1);
          double denominator= static_cast<double>(frame.tail_ - frame.head_ - 1);
          *reinterpret_cast<double *>(res) = (numerator / denominator);
        }
      }
    } else if (lib::is_oracle_mode()) {
      number::ObNumber res_nmb;
      ObNumStackAllocator<1> tmp_alloc;
      if (OB_FAIL(res_nmb.from(rank, tmp_alloc))) {
        LOG_WARN("failed to build number from int64_t", K(ret));
      } else {
        MEMCPY(res, &(res_nmb.d_), sizeof(ObNumberDesc));
        MEMCPY(res + sizeof(ObNumberDesc), res_nmb.get_digits(),
               sizeof(uint32_t) * res_nmb.d_.len_);
      }
    } else {
      *reinterpret_cast<int64_t *>(res) = rank;
    }
    if (OB_SUCC(ret)) {
      rank_of_prev_row_ = rank;
    }
  }
  return ret;
}

template<ObItemType rank_op>
int RankLikeExpr<rank_op>::generate_extra(ObIAllocator &allocator, void *&extra)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(extra = allocator.alloc(sizeof(int64_t)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  } else {
    *reinterpret_cast<int64_t *>(extra) = 0;
  }
  return ret;
}

int Ntile::process_window(WinExprEvalCtx &ctx, const Frame &frame, const int64_t row_idx, char *res, bool &is_null)
{
  int ret = OB_SUCCESS;
  ParamStatus *param_status = reinterpret_cast<ParamStatus *>(ctx.extra_);
  is_null = false;
  if (OB_UNLIKELY(!param_status->calculated_)) {
    // calculated bucket number
    const ObExprPtrIArray &params = ctx.win_col_.wf_info_.param_exprs_;
    ObExpr *param = nullptr;
    int64_t bucket_num = 0;
    bool is_null = false;
    if (OB_UNLIKELY(params.count() != 1)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("The number of arguments of NTILE should be 1", K(params.count()), K(ret));
    } else if (OB_ISNULL(param = params.at(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null param expr", K(ret));
    } else if (!is_oracle_mode() && !param->obj_meta_.is_numeric_type()) {
      ret = OB_DATA_OUT_OF_RANGE;
      LOG_WARN("invalid argument", K(ret), K(param->obj_meta_));
    } else if (OB_FAIL(NonAggrWinExpr::eval_param_int_value(param, ctx.win_col_.op_.get_eval_ctx(),
                                                           lib::is_mysql_mode(), false, *param_status))) {
      if (ret == OB_ERR_WINDOW_FRAME_ILLEGAL) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("Incorrect arguments to ntile", K(ret));
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ntile");
      } else {
        LOG_WARN("get_param_int_value failed", K(ret));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (param_status->is_null_) {
    // do nothing
    is_null = true;
  } else if (param_status->int_val_ <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("bucket number is invalid", K(ret), K(param_status->int_val_));
  } else {
    int64_t bucket_num = param_status->int_val_;
    int64_t total = frame.tail_ - frame.head_;
    int64_t x = total / bucket_num;
    int64_t y = total % bucket_num;
    const int64_t f_row_idx = row_idx - frame.head_;
    int64_t result = 0;
    LOG_DEBUG("print ntile param", K(total), K(x), K(y), K(f_row_idx));
    if (0 == x) {
      result  = f_row_idx + 1;
    } else {
      if (f_row_idx < (y * (x + 1))) {
        result = f_row_idx / (x + 1) + 1;
      } else {
        result = (f_row_idx - y * (x + 1)) / x + y + 1;
      }
    }
    if (ctx.win_col_.wf_info_.expr_->datum_meta_.type_ == ObNumberType) {
       ObNumStackOnceAlloc tmp_alloc;
      number::ObNumber result_num;
      if (OB_FAIL(result_num.from(result, tmp_alloc))) {
        LOG_WARN("number from int failed", K(ret));
      } else {
        MEMCPY(res, &(result_num.d_), sizeof(ObNumberDesc));
        MEMCPY(res + sizeof(ObNumberDesc), result_num.get_digits(), sizeof(uint32_t) * result_num.d_.len_);
      }
    } else {
      *reinterpret_cast<int64_t *>(res) = result;
    }
  }
  return ret;
}

int Ntile::generate_extra(ObIAllocator &allocator, void *&extra)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(extra = allocator.alloc(sizeof(ParamStatus)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  } else {
    new(extra)ParamStatus();
  }
  return ret;
}

static int memcpy_results(WinExprEvalCtx &ctx, VecValueTypeClass res_tc, char *res_buf,
                            const char *src, int32_t len)
{
  int ret = OB_SUCCESS;
  char *data_buf = nullptr;
  int32_t data_len = len;
  ObExpr *win_expr = ctx.win_col_.wf_info_.expr_;
  if (is_fixed_length_vec(res_tc) || res_tc == VEC_TC_NUMBER) {
    MEMCPY(res_buf, src, len);
  } else if (OB_UNLIKELY(len == 0)) {
    *reinterpret_cast<int64_t *>(res_buf) = 0;
    *reinterpret_cast<int32_t *>(res_buf + sizeof(char *)) = len;
  } else if (OB_ISNULL(data_buf = ctx.reserved_buf(len))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  } else {
    MEMCPY(data_buf, src, len);
    *reinterpret_cast<int64_t *>(res_buf) = reinterpret_cast<int64_t>(data_buf);
    *reinterpret_cast<int32_t *>(res_buf + sizeof(char *)) = len;
  }
  return ret;
}

int NthValue::process_window(WinExprEvalCtx &ctx, const Frame &frame, const int64_t row_idx, char *res, bool &is_null)
{
  int ret = OB_SUCCESS;
  is_null = false;
  const ObExprPtrIArray &params = ctx.win_col_.wf_info_.param_exprs_;
  bool is_param_null = false;
  int64_t nth_val = 0;
  const RowMeta &input_row_meta = ctx.win_col_.op_.get_input_row_meta();
  ObEvalCtx &eval_ctx = ctx.win_col_.op_.get_eval_ctx();
  ObIArray<ObExpr *> &all_exprs = ctx.win_col_.op_.get_all_expr();
  // TODO: second param of nth_value in mysql mode is a const expr, optimize calculating.
  ctx.win_col_.op_.clear_evaluated_flag();
  ParamStatus param_status;
  if (OB_FAIL(ctx.input_rows_.attach_rows(all_exprs, input_row_meta, eval_ctx, row_idx, row_idx + 1,
                                          false))) {
    LOG_WARN("attach rows failed", K(ret));
  } else if (OB_UNLIKELY(params.count() != 2)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid number of params", K(ret), K(params.count()), K(ret));
  } else if (OB_FAIL(
               NonAggrWinExpr::eval_param_int_value(params.at(1), ctx.win_col_.op_.get_eval_ctx(),
                                                    lib::is_mysql_mode(), false, param_status))) {
    if (ret == OB_ERR_WINDOW_FRAME_ILLEGAL) {
      if (param_status.is_null_) {
        ret = OB_SUCCESS;
        is_null = true;
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("Incorrect arguments to nth_value", K(ret));
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "nth_value");
      }
    } else {
      LOG_WARN("get_param_int_value failed", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else {
    is_param_null = param_status.is_null_;
    nth_val = param_status.int_val_;
  }
  if (OB_SUCC(ret) && !is_null) {
    ObWindowFunctionVecOp &op = ctx.win_col_.op_;
    if (OB_UNLIKELY(lib::is_oracle_mode() && (is_param_null || nth_val <= 0))) {
      ret = OB_DATA_OUT_OF_RANGE;
      LOG_WARN("invalid argument", K(ret), K(is_param_null), K(nth_val));
    } else if (OB_UNLIKELY(lib::is_mysql_mode()
                           && (!params.at(1)->obj_meta_.is_integer_type() || nth_val == 0))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arguments to nth_value", K(ret), K(nth_val), K(params.at(1)->obj_meta_));
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "nth_value");
    } else {
      bool is_ignore_null = ctx.win_col_.wf_info_.is_ignore_null_;
      bool is_from_first = ctx.win_col_.wf_info_.is_from_first_;

      int64_t k = 0, cur_idx = (is_from_first ? frame.head_ : frame.tail_ - 1);
      bool is_calc_nth = false;
      const RowMeta &input_row_meta = ctx.win_col_.op_.get_input_row_meta();
      LOG_DEBUG("nth value params", K(is_param_null), K(nth_val), K(is_from_first), K(is_ignore_null), K(frame));
      while (OB_SUCC(ret) && k < nth_val) {
        op.clear_evaluated_flag();
        int64_t batch_size = std::min(nth_val - k, ctx.win_col_.op_.get_spec().max_batch_size_);
        batch_size = std::min((is_from_first ? (frame.tail_ - cur_idx) : (cur_idx - frame.head_ + 1)),
                              batch_size);

        int64_t start_idx = (is_from_first ? cur_idx : cur_idx - batch_size + 1);
        int64_t end_idx = (is_from_first ? cur_idx + batch_size : cur_idx + 1);
        if (start_idx >= end_idx) { break; }
        int64_t word_cnt = ObBitVector::word_count(op.get_spec().max_batch_size_);
        MEMSET(op.get_batch_ctx().bound_eval_skip_, 0, ObBitVector::BYTES_PER_WORD * word_cnt);
        int64_t step = (is_from_first ? 1 : -1);
        EvalBound bound(batch_size, true);
        VecValueTypeClass res_tc = params.at(0)->get_vec_value_tc();
        VectorFormat param_fmt = VEC_INVALID;
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(ctx.input_rows_.attach_rows(op.get_all_expr(), input_row_meta,
                                                       op.get_eval_ctx(), start_idx, end_idx,
                                                       false))) {
          LOG_WARN("attach rows failed", K(ret));
        } else if (OB_FAIL(params.at(0)->eval_vector(
                     op.get_eval_ctx(), *op.get_batch_ctx().bound_eval_skip_, bound))) {
          LOG_WARN("eval vector failed", K(ret));
        } else if (FALSE_IT(param_fmt = params.at(0)->get_format(op.get_eval_ctx()))) {
        } else if (param_fmt != VEC_UNIFORM && param_fmt != VEC_UNIFORM_CONST) {
          ObIVector *data = params.at(0)->get_vector(op.get_eval_ctx());
          ObBitmapNullVectorBase *param_nulls = static_cast<ObBitmapNullVectorBase *>(data);
          for (int i = 0, idx = (is_from_first ? 0 : batch_size - 1);
               !is_calc_nth && i < batch_size; i++, idx += step) {
            if ((!param_nulls->is_null(idx) || !is_ignore_null) && ++k == nth_val) {
              is_calc_nth = true;
              if (param_nulls->is_null(idx)) {
                is_null = true;
              } else {
                ret = memcpy_results(ctx, res_tc, res, data->get_payload(idx), data->get_length(idx));
              }
            }
          }
        } else if (param_fmt == VEC_UNIFORM) {
          ObUniformFormat<false> *data = static_cast<ObUniformFormat<false> *>(params.at(0)->get_vector(op.get_eval_ctx()));
          for (int j = 0, idx = (is_from_first ? 0 : batch_size - 1);
               !is_calc_nth && j < batch_size; j++, idx += step) {
            if ((!data->is_null(idx) || !is_ignore_null) && ++k == nth_val) {
              is_calc_nth = true;
              if (data->is_null(idx)) {
                is_null = true;
              } else {
                ret = memcpy_results(ctx, res_tc, res, data->get_payload(idx), data->get_length(idx));
              }
            }
          }
        } else if (param_fmt == VEC_UNIFORM_CONST) {
          ObUniformFormat<true> *data = static_cast<ObUniformFormat<true> *>(params.at(0)->get_vector(op.get_eval_ctx()));
          for (int j = 0, idx = (is_from_first ? 0 : batch_size - 1);
               !is_calc_nth && j < batch_size; j++, idx += step) {
            if ((!data->is_null(idx) || !is_ignore_null) && ++k == nth_val) {
              is_calc_nth = true;
              if (data->is_null(idx)) {
                is_null = true;
              } else {
                ret = memcpy_results(ctx, res_tc, res, data->get_payload(idx), data->get_length(idx));
              }
            }
          }
        }
        if (OB_SUCC(ret)) {
          (is_from_first ? (cur_idx += batch_size) : (cur_idx -= batch_size));
          if ((is_from_first && cur_idx >= frame.tail_) || (!is_from_first && cur_idx < frame.head_)) {
            break;
          }
        }
      } // end while
      if (!is_calc_nth) {
        is_null = true;
      }
    }
  }
  return ret;
}

int NthValue::generate_extra(ObIAllocator &allocator, void *&extra)
{
  int ret = OB_SUCCESS;
  extra = nullptr;
  if (lib::is_mysql_mode()) {
    void *buf = allocator.alloc(sizeof(ParamStatus));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else {
      new (buf) ParamStatus();
      extra = buf;
    }
  }
  return ret;
}

int LeadOrLag::process_window(WinExprEvalCtx &ctx, const Frame &frame, const int64_t row_idx,
                              char *res, bool &is_null)
{
  int ret = OB_SUCCESS;
  enum LeadLagParamType
  {
    VALUE_EXPR = 0,
    OFFSET = 1,
    DEFAULT_VALUE = 2,
    NUM_LEAD_LAG_PARAMS
  };
  const bool is_lead = (T_WIN_FUN_LEAD == ctx.win_col_.wf_info_.func_type_);
  int lead_lag_offset_direction = (is_lead ? 1 : -1);
  // if not specified, the default offset is 1.
  bool is_lead_lag_offset_used = false;
  const ObIArray<ObExpr *> &params = ctx.win_col_.wf_info_.param_exprs_;
  const ObCompactRow *a_row = nullptr;
  ObIArray<ObExpr *> &all_exprs = ctx.win_col_.op_.get_all_expr();
  ObEvalCtx &eval_ctx = ctx.win_col_.op_.get_eval_ctx();
  const RowMeta &input_row_meta = ctx.win_col_.op_.get_input_row_meta();
  ObEvalCtx::BatchInfoScopeGuard guard(eval_ctx);
  guard.set_batch_size(1);
  guard.set_batch_idx(0);
  sql::EvalBound eval_bound(1, true);
  int64_t mock_skip_data = 0;
  ObBitVector *mock_skip = to_bit_vector(&mock_skip_data);
  char *default_val = nullptr;
  int32_t default_val_len = 0;
  if (OB_UNLIKELY(params.count() > NUM_LEAD_LAG_PARAMS || params.count() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid number of params", K(ret), K(params.count()));
  } else if (FALSE_IT(ctx.win_col_.op_.clear_evaluated_flag())) {
  } else if (OB_FAIL(ctx.input_rows_.attach_rows(all_exprs, input_row_meta, eval_ctx, row_idx,
                                                 row_idx + 1, false))) {
    LOG_WARN("attach rows failed", K(ret));
  } else {
    for (int j = 0; OB_SUCC(ret) && j < params.count(); j++) {
      if (OB_ISNULL(params.at(j))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid null param", K(ret), K(j));
      } else if (OB_FAIL(params.at(j)->eval_vector(eval_ctx, *mock_skip, eval_bound))) {
        LOG_WARN("eval vector failed", K(ret));
      } else if (j == DEFAULT_VALUE && !params.at(j)->get_vector(eval_ctx)->is_null(0)) {
        const char *payload = nullptr;
        int32_t len = 0;
        params.at(j)->get_vector(eval_ctx)->get_payload(0, payload, len);
        if (OB_UNLIKELY(len == 0)) {
          default_val_len = len;
          default_val = nullptr;
        } else if (OB_ISNULL(default_val = (char *)ctx.allocator_.alloc(len))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate memory failed", K(ret));
        } else {
          MEMCPY(default_val, payload, len);
          default_val_len = len;
        }
      } else {
        is_lead_lag_offset_used |= (j == OFFSET);
      }
    }
  }
  int64_t offset = 0;
  if (OB_FAIL(ret)) {
  } else if (is_lead_lag_offset_used) {
    NonAggrWinExpr::ParamStatus param_status;
    if (OB_FAIL(NonAggrWinExpr::eval_param_int_value(params.at(OFFSET), eval_ctx, false, false,
                                                     param_status))) {
      LOG_WARN("eval param int value failed", K(ret));
    } else if (OB_UNLIKELY(param_status.is_null_ || param_status.int_val_ < 0
                           || (lib::is_oracle_mode() && ctx.win_col_.wf_info_.is_ignore_null_
                               && param_status.int_val_ == 0))) {
      ret = OB_ERR_ARGUMENT_OUT_OF_RANGE;
      if (!param_status.is_null_) {
        LOG_USER_ERROR(OB_ERR_ARGUMENT_OUT_OF_RANGE, param_status.int_val_);
      }
      LOG_WARN("lead/lag argument is out of range", K(ret), K(param_status.is_null_),
               K(param_status.int_val_));
    } else {
      offset = param_status.int_val_;
    }
  } else {
    offset = 1; // default to 1
  }
  LOG_DEBUG("lead/lag expr", K(is_lead_lag_offset_used), K(offset), K(lead_lag_offset_direction));
  if (OB_SUCC(ret)) {
    // FIXME: opt this code
    int64_t step = 0;
    bool found = false;
    const char *src = nullptr;
    int32_t src_len = 0;
    bool src_isnull = false;
    for (int64_t i = row_idx; OB_SUCC(ret) && !found && i >= frame.head_ && i < frame.tail_;
         i += lead_lag_offset_direction) {
      ctx.win_col_.op_.clear_evaluated_flag();
      if (OB_FAIL(
            ctx.input_rows_.attach_rows(all_exprs, input_row_meta, eval_ctx, i, i + 1, false))) {
        LOG_WARN("attach rows failed", K(ret));
      } else if (OB_FAIL(params.at(0)->eval_vector(eval_ctx, *mock_skip, eval_bound))) {
        LOG_WARN("eval vector failed", K(ret));
      } else if (ctx.win_col_.wf_info_.is_ignore_null_
                 && params.at(0)->get_vector(eval_ctx)->is_null(0)) {
        step = (i == row_idx) ? step + 1 : step;
      } else if (step++ == offset) {
        src_isnull = params.at(0)->get_vector(eval_ctx)->is_null(0);
        params.at(0)->get_vector(eval_ctx)->get_payload(0, src, src_len);
        found = true;
      }
    }
    VecValueTypeClass res_tc = params.at(0)->get_vec_value_tc();
    if (OB_SUCC(ret)) {
      if (!found) {
        if (default_val != nullptr) {
          if (OB_FAIL(memcpy_results(ctx, res_tc, res, default_val, default_val_len))) {
            LOG_WARN("copy results failed", K(ret));
          }
        } else {
          is_null = true;
        }
      } else if (src_isnull) {
        is_null = true;
      } else if (OB_FAIL(memcpy_results(ctx, res_tc, res, src, src_len))) {
        LOG_WARN("copy results failed", K(ret));
      }
    }
  }
  return ret;
}

int LeadOrLag::generate_extra(ObIAllocator &allocator, void *&extra)
{
  int ret = OB_SUCCESS;
  extra = nullptr;
  return ret;
}

int CumeDist::process_window(WinExprEvalCtx &ctx, const Frame &frame, const int64_t row_idx,
                             char *res, bool &is_null)
{
  int ret = OB_SUCCESS;
  int64_t same_idx = row_idx;
  ObIArray<ObExpr *> &all_exprs = ctx.win_col_.op_.get_all_expr();
  ObEvalCtx &eval_ctx = ctx.win_col_.op_.get_eval_ctx();
  LastCompactRow ref_row(ctx.allocator_);
  const ObCompactRow *iter_row = nullptr;
  if (OB_FAIL(ref_row.init_row_meta(all_exprs, 0, true))) {
    LOG_WARN("init row meta failed", K(ret));
  } else if (OB_FAIL(ctx.input_rows_.get_row(row_idx, iter_row))) {
    LOG_WARN("get row failed", K(ret));
  } else if (OB_FAIL(ref_row.save_store_row(*iter_row))) {
    LOG_WARN("save store row failed", K(ret));
  }
  bool should_continue = true;
  ExprFixedArray &sort_cols = ctx.win_col_.wf_info_.sort_exprs_;
  ObSortCollations &sort_collations = ctx.win_col_.wf_info_.sort_collations_;
  ObSortFuncs &sort_cmp_funcs = ctx.win_col_.wf_info_.sort_cmp_funcs_;
  const RowMeta &input_row_meta = ctx.win_col_.op_.get_input_row_meta();
  while (should_continue && OB_SUCC(ret) && same_idx + 1 < frame.tail_) {
    if (OB_FAIL(ctx.input_rows_.get_row(same_idx + 1, iter_row))) {
      LOG_WARN("get row failed", K(ret));
    } else {
      int cmp_ret = 0;
      const char *l_data = nullptr, *r_data = nullptr;
      int32_t l_len = 0, r_len = 0;
      bool l_isnull = false, r_isnull = false;
      for (int i = 0; OB_SUCC(ret) && should_continue && i < sort_cols.count(); i++) {
        ObObjMeta &obj_meta = sort_cols.at(i)->obj_meta_;
        int64_t field_idx = sort_collations.at(i).field_idx_;
        sql::NullSafeRowCmpFunc cmp_fn = sort_cmp_funcs.at(i).row_cmp_func_;
        iter_row->get_cell_payload(input_row_meta, field_idx, l_data, l_len);
        l_isnull = iter_row->is_null(field_idx);
        ref_row.compact_row_->get_cell_payload(input_row_meta, field_idx, r_data, r_len);
        r_isnull = ref_row.compact_row_->is_null(field_idx);
        if (OB_FAIL(cmp_fn(obj_meta, obj_meta, l_data, l_len, l_isnull, r_data, r_len, r_isnull,
                           cmp_ret))) {
          LOG_WARN("compare failed", K(ret));
        } else if (cmp_ret != 0) {
          should_continue = false;
        }
      }
      if (OB_SUCC(ret) && should_continue) {
        same_idx++;
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (ob_is_number_tc(ctx.win_col_.wf_info_.expr_->datum_meta_.type_)) {
      number::ObNumber numerator;
      number::ObNumber denominator;
      number::ObNumber res_nmb;

      ObNumStackAllocator<3> tmp_alloc;
      if (OB_FAIL(numerator.from(same_idx - frame.head_ + 1, tmp_alloc))) {
        LOG_WARN("number::from failed", K(ret));
      } else if (OB_FAIL(denominator.from(frame.tail_ - frame.head_, tmp_alloc))) {
        LOG_WARN("number::from failed", K(ret));
      } else if (OB_FAIL(numerator.div(denominator, res_nmb, tmp_alloc))) {
        LOG_WARN("failed to div number", K(ret));
      } else {
        number::ObCompactNumber *res_cnum = reinterpret_cast<number::ObCompactNumber *>(res);
        res_cnum->desc_ = res_nmb.d_;
        MEMCPY(&(res_cnum->digits_[0]), res_nmb.get_digits(), sizeof(uint32_t) * res_nmb.d_.len_);
        is_null = false;
      }
    } else if (ObDoubleType == ctx.win_col_.wf_info_.expr_->datum_meta_.type_) {
      double numerator, denominator;
      numerator = static_cast<double>(same_idx - frame.head_ + 1);
      denominator = static_cast<double>(frame.tail_ - frame.head_);
      *reinterpret_cast<double *>(res) = (numerator / denominator);
      is_null = false;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("he result type of window function is unexpected", K(ret), K(ctx.win_col_.wf_info_));
    }
  }
  return ret;
}

int CumeDist::generate_extra(ObIAllocator &allocator, void *&extra)
{
  int ret = OB_SUCCESS;
  extra = nullptr;
  return ret;
}

int RowNumber::process_window(WinExprEvalCtx &ctx, const Frame &frame, const int64_t row_idx,
                              char *res, bool &is_null)
{
  int ret = OB_SUCCESS;
  int64_t row_nmb = row_idx - frame.head_ + 1;
  if (lib::is_oracle_mode()) {
    number::ObNumber res_nmb;
    ObNumStackOnceAlloc tmp_alloc;
    if (OB_FAIL(res_nmb.from(row_nmb, tmp_alloc))) {
      LOG_WARN("number::from failed", K(ret));
    } else {
      number::ObCompactNumber *cnum = reinterpret_cast<number::ObCompactNumber *>(res);
      cnum->desc_ = res_nmb.d_;
      MEMCPY(&(cnum->digits_[0]), res_nmb.get_digits(), res_nmb.d_.len_ * sizeof(uint32_t));
    }
  } else {
    *reinterpret_cast<int64_t *>(res) = row_nmb;
  }
  return ret;
}

int RowNumber::generate_extra(ObIAllocator &allocator, void *&extra)
{
  int ret = OB_SUCCESS;
  extra = nullptr;
  return ret;
}

int AggrExpr::process_window(WinExprEvalCtx &ctx, const Frame &frame, const int64_t row_idx,
                             char *agg_row, bool &is_null)
{
  int ret = OB_SUCCESS;
  ObEvalCtx &eval_ctx = ctx.win_col_.op_.get_eval_ctx();
  ObExpr *agg_expr = ctx.win_col_.wf_info_.expr_;
  ObWindowFunctionVecOp &op = ctx.win_col_.op_;
  int64_t total_size = frame.tail_ - frame.head_;
  const RowMeta &input_row_meta = op.get_input_row_meta();
  ObBitVector &eval_skip = *op.get_batch_ctx().bound_eval_skip_;
  ObEvalCtx::BatchInfoScopeGuard guard(op.get_eval_ctx());
  int64_t row_start = frame.head_;
  ObBatchRows tmp_brs;
  aggregate::RemovalInfo &removal_info = ctx.win_col_.agg_ctx_->removal_info_;
  LOG_DEBUG("aggregate expr process window", K(frame), K(removal_info), K(row_start));
  char *res_buf = nullptr;
  int total_calc_size = 0, calc_cnt = 0;
  while (OB_SUCC(ret) && total_size > 0) {
    op.clear_evaluated_flag();
    int64_t batch_size = std::min(total_size, op.get_spec().max_batch_size_);
    guard.set_batch_size(batch_size);
    tmp_brs.size_ = batch_size;
    tmp_brs.end_ = false;
    tmp_brs.skip_ = &eval_skip;
    total_calc_size += batch_size;
    calc_cnt += 1;
    if (OB_FAIL(ctx.input_rows_.attach_rows(op.get_all_expr(), input_row_meta, eval_ctx, row_start,
                                            row_start + batch_size, false))) {
      LOG_WARN("attach rows failed", K(ret));
    } else if (OB_FAIL(calc_pushdown_skips(ctx, batch_size, eval_skip, tmp_brs.all_rows_active_))) {
      LOG_WARN("calc pushdown skips failed", K(ret));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(aggr_processor_->eval_aggr_param_batch(tmp_brs))) {
      LOG_WARN("eval aggr params failed", K(ret));
    } else if (OB_FAIL(aggr_processor_->add_batch_rows(0, 1, agg_row, tmp_brs, (uint16_t)0,
                                                       batch_size))) {
      LOG_WARN("add batch rows failed", K(ret));
    } else if (ctx.win_col_.wf_info_.remove_type_ == REMOVE_EXTRENUM
               && removal_info.is_max_min_idx_changed_) {
      removal_info.max_min_index_ += row_start;
      removal_info.is_max_min_idx_changed_ = false;
    }
    if (OB_SUCC(ret)) {
      total_size -= batch_size;
      row_start += batch_size;
    }
    // if result is variable-length type, address stored in agg_row maybe invalid after `attach_rows`
    // thus we copy results into res_buf and store corresponding address instread.
    bool is_res_not_null = ctx.win_col_.agg_ctx_->row_meta().locate_notnulls_bitmap(agg_row).at(0);
    if (ctx.win_col_.agg_ctx_->row_meta().is_var_len(0) && is_res_not_null) {
      int64_t addr_val = *reinterpret_cast<int64_t *>(ctx.win_col_.agg_ctx_->row_meta().locate_cell_payload(0, agg_row));
      int32_t val_len = ctx.win_col_.agg_ctx_->row_meta().get_cell_len(0, agg_row);
      const char *val = reinterpret_cast<const char *>(addr_val);
      if (val != res_buf && val_len > 0) { // new value
        res_buf = ctx.reserved_buf(val_len);
        if (OB_ISNULL(res_buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate memory failed", K(ret));
        } else {
          MEMCPY(res_buf, val, val_len);
          addr_val = reinterpret_cast<int64_t>(res_buf);
          *reinterpret_cast<int64_t *>(ctx.win_col_.agg_ctx_->row_meta().locate_cell_payload(0, agg_row)) = addr_val;
        }
      }
    }
  } // end while
  if (OB_FAIL(ret)) {
  } else if (aggregate::agg_res_not_null(ctx.win_col_.wf_info_.func_type_)) {
    ctx.win_col_.agg_ctx_->row_meta().locate_notnulls_bitmap(agg_row).set(0);
  } else if (removal_info.enable_removal_opt_ && !frame.is_accum_frame_) {
    if (removal_info.null_cnt_ == frame.tail_ - frame.head_) {
      ctx.win_col_.agg_ctx_->row_meta().locate_notnulls_bitmap(agg_row).unset(0);
    } else {
      ctx.win_col_.agg_ctx_->row_meta().locate_notnulls_bitmap(agg_row).set(0);
    }
  }
  return ret;
}

int AggrExpr::set_result_for_invalid_frame(WinExprEvalCtx &ctx, char *agg_row)
{
  int ret = OB_SUCCESS;
  ObExprOperatorType fun_type = ctx.win_col_.wf_info_.func_type_;
  aggregate::RuntimeContext &agg_ctx = *ctx.win_col_.agg_ctx_;
  aggregate::NotNullBitVector &not_nulls = agg_ctx.row_meta().locate_notnulls_bitmap(agg_row);
  switch(fun_type)
  {
    case T_FUN_COUNT: {
      *reinterpret_cast<int64_t *>(agg_ctx.row_meta().locate_cell_payload(0, agg_row)) = 0;
      not_nulls.set(0);
      break;
    }
    case T_FUN_SYS_BIT_AND:
    case T_FUN_SYS_BIT_OR:
    case T_FUN_SYS_BIT_XOR: {
      uint64_t res_val = (fun_type == T_FUN_SYS_BIT_AND ? UINT_MAX_VAL[ObUInt64Type] : 0);
      *reinterpret_cast<uint64_t *>(agg_ctx.row_meta().locate_cell_payload(0, agg_row)) = res_val;
      not_nulls.set(0);
      break;
    }
    default: {
      not_nulls.unset(0);
      break;
    }
  }
  return ret;
}

int AggrExpr::calc_pushdown_skips(WinExprEvalCtx &ctx, const int64_t batch_size,
                                  sql::ObBitVector &skip, bool &all_active)
{
  int ret = OB_SUCCESS;
  skip.unset_all(0, batch_size);
  const ObWindowFunctionVecSpec &spec = static_cast<const ObWindowFunctionVecSpec &>(ctx.win_col_.op_.get_spec());
  ObWindowFunctionVecOp &op = ctx.win_col_.op_;
  all_active = true;
  if (spec.is_push_down()) {
    VectorFormat fmt = spec.wf_aggr_status_expr_->get_format(op.get_eval_ctx());
    VecValueTypeClass tc = spec.wf_aggr_status_expr_->get_vec_value_tc();
    if (OB_UNLIKELY(fmt != VEC_FIXED || tc != VEC_TC_INTEGER)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected format & type class", K(ret), K(fmt), K(tc));
    } else {
      if (spec.is_participator()) {
        // participator's status code is filled by window function, data format must be VEC_FIXED
        if (OB_UNLIKELY(spec.wf_aggr_status_expr_->get_format(op.get_eval_ctx()) != VEC_FIXED)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected data format", K(ret),
                   K(spec.wf_aggr_status_expr_->get_format(op.get_eval_ctx())));
        } else {
          ObFixedLengthFormat<int64_t> *data = static_cast<ObFixedLengthFormat<int64_t> *>(
            spec.wf_aggr_status_expr_->get_vector(op.get_eval_ctx()));
          for (int i = 0; i < batch_size; i++) {
            int64_t status = *reinterpret_cast<const int64_t *>(data->get_payload(i));
            if (status < 0) {
              skip.set(i);
              all_active = false;
            }
          }
        }
      } else if (spec.is_consolidator()) {
        int64_t wf_idx = ctx.win_col_.wf_idx_;
        VectorFormat status_fmt = spec.wf_aggr_status_expr_->get_format(op.get_eval_ctx());
        if (status_fmt == VEC_FIXED) {
          ObFixedLengthFormat<int64_t> *data = static_cast<ObFixedLengthFormat<int64_t> *>(
            spec.wf_aggr_status_expr_->get_vector(op.get_eval_ctx()));
          for (int i = 0; i < batch_size; i++) {
            int64_t status = *reinterpret_cast<const int64_t *>(data->get_payload(i));
            if ((status < 0 && -status != wf_idx) || (status >= 0 && status < wf_idx)) {
              skip.set(i);
              all_active = false;
            }
          }
        } else if (status_fmt == VEC_UNIFORM) {
          ObUniformFormat<false> *data = static_cast<ObUniformFormat<false> *>(
            spec.wf_aggr_status_expr_->get_vector(op.get_eval_ctx()));
          for (int i = 0; i < batch_size; i++) {
            int64_t status = *reinterpret_cast<const int64_t *>(data->get_payload(i));
            if ((status < 0 && -status != wf_idx) || (status >= 0 && status < wf_idx)) {
              skip.set(i);
              all_active = false;
            }
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected data format", K(ret), K(status_fmt));
        }
      }
    }
  }
  return ret;
}

int AggrExpr::accum_process_window(WinExprEvalCtx &ctx, const Frame &cur_frame,
                                   const Frame &prev_frame, const int64_t row_idx, char *agg_row,
                                   bool &is_null)
{
  int ret = OB_SUCCESS;
  ObEvalCtx &eval_ctx = ctx.win_col_.op_.get_eval_ctx();
  ObExpr *agg_expr = ctx.win_col_.wf_info_.expr_;
  ObWindowFunctionVecOp &op = ctx.win_col_.op_;
  int64_t head_l = std::min(cur_frame.head_, prev_frame.head_);
  int64_t head_r = std::max(cur_frame.head_, prev_frame.head_);
  int64_t tail_l = std::min(cur_frame.tail_, prev_frame.tail_);
  int64_t tail_r = std::max(cur_frame.tail_, prev_frame.tail_);
  int64_t total_size = (head_r - head_l) + (tail_r - tail_l);
  Frame new_frame(head_l, head_r, true), new_frame2(tail_l, tail_r, true);
  aggregate::RemovalInfo &removal_info = ctx.win_col_.agg_ctx_->removal_info_;
  ctx.win_col_.agg_ctx_->set_inverse_agg(prev_frame.head_ < cur_frame.head_);
  int64_t cur_idx = eval_ctx.get_batch_idx();
  if (OB_UNLIKELY(cur_idx < 0 || cur_idx >= eval_ctx.get_batch_size())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected idx", K(ret), K(cur_idx));
  } else {
    char *prev_row = (cur_idx == 0 ? last_aggr_row_ : ctx.win_col_.aggr_rows_[cur_idx - 1]);
    if (OB_FAIL(copy_aggr_row(ctx, prev_row, agg_row))) {
      LOG_WARN("copy aggr row failed", K(ret));
    }
  }
  if (OB_FAIL(ret)) { // TODO: if frame size is small, should `add_one_row` for `process_window` @optimize
  } else if (!new_frame.is_empty()
             && OB_FAIL(process_window(ctx, new_frame, row_idx, agg_row, is_null))) {
    LOG_WARN("process window failed", K(ret));
  } else if (FALSE_IT(ctx.win_col_.agg_ctx_->set_inverse_agg(cur_frame.tail_ < prev_frame.tail_))) {
  } else if (!new_frame2.is_empty()
             && OB_FAIL(process_window(ctx, new_frame2, row_idx, agg_row, is_null))) {
    LOG_WARN("process window failed", K(ret));
  }
  if (OB_SUCC(ret) && !aggregate::agg_res_not_null(ctx.win_col_.wf_info_.func_type_)
      && removal_info.enable_removal_opt_) {
    if (removal_info.null_cnt_ == cur_frame.tail_ - cur_frame.head_) {
      ctx.win_col_.agg_ctx_->row_meta().locate_notnulls_bitmap(agg_row).unset(0);
    } else {
      ctx.win_col_.agg_ctx_->row_meta().locate_notnulls_bitmap(agg_row).set(0);
    }
  }
  return ret;
}

void AggrExpr::destroy()
{
  if (aggr_processor_ != nullptr) {
    aggr_processor_->destroy();
    aggr_processor_ = nullptr;
  }
}

int AggrExpr::collect_part_results(WinExprEvalCtx &ctx, const int64_t row_start,
                                   const int64_t row_end, const ObBitVector &skip)
{
  int ret = OB_SUCCESS;
  aggregate::IAggregate *iagg = aggr_processor_->get_aggregates().at(0);
  aggregate::RuntimeContext &agg_ctx = *ctx.win_col_.agg_ctx_;
  int64_t batch_size = row_end - row_start;
  int32_t output_size = 0;
  if (OB_FAIL(iagg->collect_batch_group_results(agg_ctx, 0, 0, 0, batch_size, output_size, &skip))) {
    LOG_WARN("collect batch group results failed", K(ret));
  }
  return ret;
}

template <typename ColumnFmt>
int AggrExpr::set_payload(WinExprEvalCtx &ctx, ColumnFmt *columns, const int64_t idx,
                          const char *payload, int32_t len)
{
  int ret = OB_SUCCESS;
  ObExpr *res_expr = ctx.win_col_.wf_info_.expr_;
  VecValueTypeClass vec_tc = res_expr->get_vec_value_tc();
  ObEvalCtx &eval_ctx = ctx.win_col_.op_.get_eval_ctx();
  const ObWindowFunctionVecSpec &spec = static_cast<const ObWindowFunctionVecSpec &>(ctx.win_col_.op_.get_spec());
  // guard used for `get_str_res_mem`
  ObEvalCtx::BatchInfoScopeGuard guard(eval_ctx);
  guard.set_batch_idx(idx);
  // count function in consolidator is T_FUN_COUNT, not T_FUN_COUNT_SUM!!!
  bool is_count_sum = (T_FUN_COUNT == ctx.win_col_.wf_info_.func_type_ && spec.is_consolidator());
  if (T_FUN_COUNT != ctx.win_col_.wf_info_.func_type_ || is_count_sum || lib::is_mysql_mode()) {
    if (is_fixed_length_vec(vec_tc)) {
      columns->set_payload(idx, payload, len);
    } else if (vec_tc == VEC_TC_NUMBER) {
      columns->set_number(idx, *reinterpret_cast<const number::ObCompactNumber *>(payload));
    } else {
      char *res_buf = res_expr->get_str_res_mem(eval_ctx, len);
      if (OB_ISNULL(res_buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret));
      } else {
        MEMCPY(res_buf, payload, len);
        columns->set_payload_shallow(idx, res_buf, len);
      }
    }
  } else {
    number::ObNumber res_nmb;
    if (OB_FAIL(res_nmb.from(*reinterpret_cast<const int64_t *>(payload), ctx.allocator_))) {
      LOG_WARN("cast to number failed", K(ret));
    } else {
      columns->set_number(idx, res_nmb);
    }
  }
  return ret;
}
// >>>>>>>> helper functions
int eval_bound_exprs(WinExprEvalCtx &ctx, const int64_t row_start, const int64_t batch_size,
                     const ObBitVector &skip, const bool is_upper)
{
  int ret = OB_SUCCESS;
  OB_ASSERT(batch_size <= ctx.win_col_.op_.get_spec().max_batch_size_);
  WinFuncInfo &wf_info = ctx.win_col_.wf_info_;
  ObEvalCtx &eval_ctx = ctx.win_col_.op_.get_eval_ctx();
  ObBitVector *eval_skip = ctx.win_col_.op_.get_batch_ctx().bound_eval_skip_;
  int64_t *pos_arr = (is_upper ? ctx.win_col_.op_.get_batch_ctx().upper_pos_arr_ :
                                 ctx.win_col_.op_.get_batch_ctx().lower_pos_arr_);
  bool is_rows = (wf_info.win_type_ == WINDOW_ROWS);
  bool is_preceding = (is_upper ? wf_info.upper_.is_preceding_ : wf_info.lower_.is_preceding_);
  bool is_unbounded = (is_upper ? wf_info.upper_.is_unbounded_ : wf_info.lower_.is_unbounded_);
  bool is_nmb_literal = (is_upper ? wf_info.upper_.is_nmb_literal_ : wf_info.lower_.is_nmb_literal_);
  ObExpr *between_value_expr =
    (is_upper ? wf_info.upper_.between_value_expr_ : wf_info.lower_.between_value_expr_);
  ObExpr *bound_expr =
    (is_upper ? wf_info.upper_.range_bound_expr_ : wf_info.lower_.range_bound_expr_);

  eval_skip->deep_copy(skip, batch_size);
  MEMSET(pos_arr, -1, batch_size * sizeof(int64_t));
  LOG_DEBUG("eval bound exprs", K(is_rows), K(is_upper), K(is_preceding), K(is_unbounded),
            K(is_nmb_literal), K(batch_size), KPC(between_value_expr), KPC(bound_expr),
            K(wf_info.sort_exprs_), K(row_start), K(ctx.win_col_.part_first_row_idx_),
            K(ctx.win_col_.op_.get_part_end_idx()));
  bool is_finished = false;

  if (NULL == between_value_expr && is_unbounded) {
    // no care rows if range, no need to evaluated;
    if (is_preceding) {
      for (int i = 0; i < batch_size; i++) {
        if (eval_skip->at(i)) { continue; }
        pos_arr[i] = ctx.win_col_.part_first_row_idx_;
      }
    } else {
      for (int i = 0; i < batch_size; i++) {
        if (eval_skip->at(i)) { continue; }
        pos_arr[i] = ctx.win_col_.op_.get_part_end_idx();
      }
    }
    is_finished = true;
  } else if (NULL == between_value_expr && !is_unbounded) {
    // current row by rows/range, no need to evaluate bound exprs
    if (OB_FAIL(calc_borders_for_current_row(ctx, row_start, batch_size, *eval_skip, is_upper))) {
      LOG_WARN("calc borders for current_row failed", K(ret));
    } else {
      is_finished = true;
    }
  } else if (is_nmb_literal) {
    if (OB_ISNULL(between_value_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null between value expr", K(ret));
    } else if (OB_UNLIKELY(lib::is_mysql_mode() && is_rows
                           && !between_value_expr->obj_meta_.is_integer_type())) {
      ret = OB_ERR_WINDOW_FRAME_ILLEGAL;
      LOG_WARN("frame start or end is negative, NULL or non-integral type", K(ret),
               K(between_value_expr->obj_meta_));
    } else if (OB_FAIL(eval_and_check_between_literal(ctx, *eval_skip, between_value_expr,
                                                      batch_size, pos_arr))) {
      LOG_WARN("eval and check between literal is failed", K(ret));
    }
  }
  // between ... and ...
  if (OB_FAIL(ret)) {
  } else if (is_finished) {
  } else if (is_rows) {
    if (OB_FAIL(calc_borders_for_rows_between(ctx, row_start, batch_size, *eval_skip,
                                              between_value_expr, is_preceding, is_upper, pos_arr))) {
      LOG_WARN("calculate borders for `rows between ... and ...` failed", K(ret));
    }
  } else if (OB_ISNULL(bound_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null bound expr", K(ret));
  } else if (wf_info.sort_exprs_.count() == 0) {
    if (OB_FAIL(calc_borders_for_no_sort_expr(ctx, batch_size, *eval_skip, bound_expr, is_upper,
                                              pos_arr))) {
      LOG_WARN("calc borders failed", K(ret));
    }
  } else if (wf_info.sort_exprs_.count() != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("only need one sort expr", K(ret));
  } else if (OB_FAIL(calc_borders_for_sort_expr(ctx, bound_expr, batch_size, row_start, *eval_skip,
                                                is_upper, pos_arr))) {
    LOG_WARN("calc borders failed", K(ret));
  }
  return ret;
}

struct __data_tuple
{
  const char *data_;
  int32_t len_;
  bool is_null_;

  __data_tuple(const char *data, int32_t len, bool null): data_(data), len_(len), is_null_(null) {}
  __data_tuple(): data_(), len_(), is_null_(true) {}

  __data_tuple(const __data_tuple &other) :
    data_(other.data_), len_(other.len_), is_null_(other.is_null_)
  {}

  TO_STRING_KV(KP_(data), K_(len), K_(is_null));
};

int calc_borders_for_current_row(winfunc::WinExprEvalCtx &ctx, const int64_t row_start,
                                 const int64_t batch_size, const ObBitVector &skip,
                                 const bool is_upper)
{
  int ret = OB_SUCCESS;
  WinFuncInfo &wf_info = ctx.win_col_.wf_info_;
  bool is_rows = (wf_info.win_type_ == WINDOW_ROWS);
  int64_t *pos_arr = (is_upper ? ctx.win_col_.op_.get_batch_ctx().upper_pos_arr_:
                                 ctx.win_col_.op_.get_batch_ctx().lower_pos_arr_);
  ObEvalCtx &eval_ctx = ctx.win_col_.op_.get_eval_ctx();
  if (is_rows) {
    for (int i = 0; i < batch_size; i++) {
      if (skip.at(i)) { continue; }
      pos_arr[i] = row_start + i + (is_upper ? 0 : 1);
    }
  } else {
    // range
    // for current row, it's no sense for is_preceding
    // we should jump to detect step by step(for case that the sort columns has very small ndv)

    // Exponential detection
    int32_t step = 1;
    int pos = row_start, prev_row_pos = -1;
    ObSortCollations &sort_collations = wf_info.sort_collations_;
    ObSortFuncs &sort_cmp_funcs = wf_info.sort_cmp_funcs_;
    ObExprPtrIArray &all_exprs = ctx.win_col_.op_.get_all_expr();
    const char *l_payload = nullptr, *r_payload = nullptr;
    int32_t l_len = 0, r_len = 0;
    int cmp_ret = 0;
    for (int i = 0; OB_SUCC(ret) && i < batch_size; i++) {
      if (skip.at(i)) { continue; }
      int32_t cur_idx = i;
      if (prev_row_pos != -1) {
        if (OB_FAIL(cmp_prev_row(ctx, i + row_start, cmp_ret))) {
          LOG_WARN("compare previous row failed", K(ret));
        } else if (cmp_ret == 0) { // same as before
          pos_arr[i] = prev_row_pos;
          continue;
        } else if (is_upper) {
          // cur_row != prev_row, cur_row's upper border equals to cur_idx
          pos_arr[i] = cur_idx + row_start;
          prev_row_pos = pos_arr[i];
          continue;
        }
      }
      if (OB_FAIL(ret)) {
      } else {
        const ObCompactRow *a_row = nullptr;
        int32_t step = 1;
        int32_t pos = cur_idx + row_start;
        ObSEArray<__data_tuple, 16> cur_row_tuple;
        for (int i = 0; OB_SUCC(ret) && i < sort_collations.count(); i++) {
          const char *payload = nullptr;
          int32_t len = 0;
          bool is_cur_null = false;
          int64_t field_idx = sort_collations.at(i).field_idx_;
          ObIVector *data = all_exprs.at(field_idx)->get_vector(eval_ctx);
          data->get_payload(cur_idx, payload, len);
          is_cur_null = data->is_null(cur_idx);
          if (OB_FAIL(cur_row_tuple.push_back(__data_tuple(payload, len, is_cur_null)))) {
            LOG_WARN("push back element failed", K(ret));
          }
        }

        while (OB_SUCC(ret)) {
          bool found_border = false;
          is_upper ? (pos -= step) : (pos += step);
          bool overflow = (is_upper ? (pos < ctx.win_col_.part_first_row_idx_) :
                                      (pos >= ctx.win_col_.op_.get_part_end_idx()));
          if (overflow) {
            found_border = true;
          } else if (OB_FAIL(ctx.input_rows_.get_row(pos, a_row))) {
            LOG_WARN("get stored row failed", K(ret));
          } else {
            cmp_ret = 0;
            for (int j = 0; OB_SUCC(ret) && !found_border && j < sort_collations.count(); j++) {
              const int64_t field_idx = sort_collations.at(j).field_idx_;
              sql::NullSafeRowCmpFunc cmp_fn = sort_cmp_funcs.at(j).row_cmp_func_;
              l_payload = cur_row_tuple.at(j).data_;
              l_len = cur_row_tuple.at(j).len_;
              bool l_isnull = cur_row_tuple.at(j).is_null_;
              a_row->get_cell_payload(ctx.win_col_.op_.get_input_row_meta(), field_idx, r_payload, r_len);
              bool r_isnull = a_row->is_null(field_idx);
              if (OB_ISNULL(cmp_fn)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("unexpected null compare function", K(ret));
              } else if (OB_FAIL(cmp_fn(all_exprs.at(field_idx)->obj_meta_,
                                        all_exprs.at(field_idx)->obj_meta_,
                                        l_payload, l_len, l_isnull,
                                        r_payload, r_len, r_isnull, cmp_ret))) {
                LOG_WARN("compare failed", K(ret));
              } else {
                found_border = (cmp_ret != 0);
              }
            }
          }
          if (OB_FAIL(ret)) {
          } else if (found_border) {
            is_upper ? (pos += step) : (pos -= step);
            if (step == 1) {
              break;
            } else {
              step = 1;
            }
          } else {
            step *= 2;
          }
        } // end inner while
        if (OB_SUCC(ret)) {
          if (pos < ctx.win_col_.part_first_row_idx_ || pos >= ctx.win_col_.op_.get_part_end_idx()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid position", K(ret), K(pos));
          } else {
            pos_arr[i] = pos + (is_upper ? 0 : 1);
            prev_row_pos = pos_arr[i];
          }
        }
      }
    } // end for
  }
  return ret;
}

template <typename Fmt, VecValueTypeClass vec_tc>
static int _check_betweenn_value(const ObExpr *expr, ObEvalCtx &ctx, const ObBitVector &skip,
                                 const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  Fmt *columns = static_cast<Fmt *>(expr->get_vector(ctx));
  const char *payload = nullptr;
  int32_t len = 0;
  int64_t value = 0;
  for (int i = bound.start(); OB_SUCC(ret) && i < bound.end(); i++) {
    if (skip.at(i) || columns->is_null(i)) { continue; }
    columns->get_payload(i, payload, len);
    ret = int_trunc<vec_tc>::get(payload, len, expr->datum_meta_, value);
    if (OB_FAIL(ret)) {
      LOG_WARN("truncate integer failed", K(ret));
    } else if (OB_UNLIKELY(value < 0)) {
      if (lib::is_mysql_mode()) {
        ret = OB_ERR_WINDOW_FRAME_ILLEGAL;
        LOG_WARN("rame start or end is negative, NULL or of non-integral type", K(ret), K(value));
      } else {
        ret = OB_DATA_OUT_OF_RANGE;
        LOG_WARN("invaid argument", K(ret), K(value));
      }
    }
  }
  return ret;
}
// if border is_nmb_literal == true, check value of between_expr is_valid
// in mysql mode, null value is invalid with error reporting
// in oracle mode, null value is invalid without error, just set invalid frame
#define CHECK_BTW_FIXED_VAL(vec_tc)                                                                \
  case (vec_tc): {                                                                                 \
    ret = _check_betweenn_value<ObFixedLengthFormat<RTCType<vec_tc>>, vec_tc>(                     \
      between_expr, eval_ctx, eval_skip, eval_bound);                                              \
  } break

#define CHECK_BTW_UNI_VAL(vec_tc)                                                                  \
  case (vec_tc): {                                                                                 \
    ret = _check_betweenn_value<ObUniformFormat<false>, vec_tc>(between_expr, eval_ctx, eval_skip, \
                                                                eval_bound);                       \
  } break

#define CHECK_BTW_CONST_VAL(vec_tc)                                                                \
  case (vec_tc): {                                                                                 \
    ret = _check_betweenn_value<ObUniformFormat<true>, vec_tc>(between_expr, eval_ctx, eval_skip,  \
                                                               eval_bound);                        \
  } break

int eval_and_check_between_literal(winfunc::WinExprEvalCtx &ctx, ObBitVector &eval_skip,
                                   const ObExpr *between_expr, const int64_t batch_size, int64_t *pos_arr)
{
  int ret = OB_SUCCESS;
  ObEvalCtx &eval_ctx = ctx.win_col_.op_.get_eval_ctx();
  EvalBound eval_bound(batch_size, false);
  if (OB_ISNULL(between_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null between expr", K(ret));
  } else if (OB_FAIL(between_expr->eval_vector(eval_ctx, eval_skip, eval_bound))) {
    LOG_WARN("expr evaluation failed", K(ret));
  } else {
    VectorFormat fmt = between_expr->get_format(eval_ctx);
    VecValueTypeClass vec_tc = between_expr->get_vec_value_tc();
    switch (fmt) {
    case common::VEC_DISCRETE:
    case common::VEC_CONTINUOUS:
    case common::VEC_FIXED: {
      ObBitmapNullVectorBase *data = static_cast<ObBitmapNullVectorBase *>(between_expr->get_vector(eval_ctx));
      if (lib::is_mysql_mode()) {
        for (int i = 0; OB_SUCC(ret) && i < batch_size; i++) {
          if (eval_skip.at(i)) { continue; }
          if (data->is_null(i)) {
            ret = OB_ERR_WINDOW_FRAME_ILLEGAL;
            LOG_WARN("frame start or end is negative, NULL or non-integral type", K(ret));
          }
        }
      } else {
        for (int i = 0; OB_SUCC(ret) && i < batch_size; i++) {
          if (eval_skip.at(i)) { continue; }
          if (data->is_null(i)) {
            // frame of current must be invalid,
            // we set pos_arr[i] to INT64_MAX to represent invalid frame border
            pos_arr[i] = INT64_MAX;
          }
        }
      }
    } break;
    case common::VEC_UNIFORM: {
      ObUniformFormat<false> *data = static_cast<ObUniformFormat<false> *>(between_expr->get_vector(eval_ctx));
      if (lib::is_mysql_mode()) {
        for (int i = 0; OB_SUCC(ret) && i < batch_size; i++) {
          if (eval_skip.at(i)) { continue; }
          if (data->is_null(i)) {
            ret = OB_ERR_WINDOW_FRAME_ILLEGAL;
            LOG_WARN("frame start or end is_negative, NULL or non-integral type", K(ret));
          }
        }
      } else {
        for (int i = 0; i < batch_size; i++) {
          if (eval_skip.at(i)) { continue; }
          if (data->is_null(i)) {
            pos_arr[i] = INT64_MAX;
          }
        }
      }
    } break;
    case common::VEC_UNIFORM_CONST: {
      ObUniformFormat<true> *data = static_cast<ObUniformFormat<true> *>(between_expr->get_vector(eval_ctx));
      bool has_null = false;
      for (int i = 0; i < batch_size; i++) {
        if (eval_skip.at(i)) {continue; }
        has_null = data->is_null(i);
        break;
      }
      if (has_null) {
        if (lib::is_mysql_mode()) {
          ret = OB_ERR_WINDOW_FRAME_ILLEGAL;
          LOG_WARN("frame start or end is negative, NULL or non-integral type", K(ret));
        } else {
          for (int i = 0; i < batch_size; i++) {
            if (eval_skip.at(i)) { continue; }
            pos_arr[i] = INT64_MAX;
          }
        }
      }
    } break;
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected format", K(ret), K(fmt));
    }
    }
    if (OB_SUCC(ret)) {
      // check interval value valid
      switch(fmt) {
      case common::VEC_DISCRETE: {
        ret = _check_betweenn_value<ObDiscreteFormat, VEC_TC_NUMBER>(between_expr, eval_ctx, eval_skip, eval_bound);
        break;
      }
      case common::VEC_FIXED: {
        switch (vec_tc) {
          LST_DO_CODE(CHECK_BTW_FIXED_VAL, VEC_TC_INTEGER, VEC_TC_UINTEGER, VEC_TC_FLOAT,
                      VEC_TC_DOUBLE, VEC_TC_FIXED_DOUBLE, VEC_TC_BIT, VEC_TC_DEC_INT32,
                      VEC_TC_DEC_INT64, VEC_TC_DEC_INT128, VEC_TC_DEC_INT256, VEC_TC_DEC_INT512);
          default: {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("not supported vec type class", K(vec_tc));
          }
        }
        break;
      }
      case common::VEC_UNIFORM: {
        switch (vec_tc) {
          LST_DO_CODE(CHECK_BTW_UNI_VAL, VEC_TC_NUMBER, VEC_TC_INTEGER, VEC_TC_UINTEGER,
                      VEC_TC_FLOAT, VEC_TC_DOUBLE, VEC_TC_FIXED_DOUBLE, VEC_TC_BIT,
                      VEC_TC_DEC_INT32, VEC_TC_DEC_INT64, VEC_TC_DEC_INT128, VEC_TC_DEC_INT256,
                      VEC_TC_DEC_INT512);
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("not supported vec type class", K(vec_tc));
        }
        }
        break;
      }
      case common::VEC_UNIFORM_CONST: {
        switch (vec_tc) {
          LST_DO_CODE(CHECK_BTW_CONST_VAL, VEC_TC_NUMBER, VEC_TC_INTEGER, VEC_TC_UINTEGER,
                      VEC_TC_FLOAT, VEC_TC_DOUBLE, VEC_TC_FIXED_DOUBLE, VEC_TC_BIT,
                      VEC_TC_DEC_INT32, VEC_TC_DEC_INT64, VEC_TC_DEC_INT128, VEC_TC_DEC_INT256,
                      VEC_TC_DEC_INT512);
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("not supported vec type class", K(vec_tc));
        }
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("not supported format", K(ret), K(fmt));
      }
      }
      if (OB_FAIL(ret)) {
        LOG_WARN("check between value faile", K(ret));
      }
    }
  }
  return ret;
}

static OB_INLINE int check_interval_valid(const int64_t row_idx, const int64_t interval,
                                          const bool is_preceding)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(interval < 0)) {
    ret = OB_DATA_OUT_OF_RANGE;
    LOG_WARN("invalid interval", K(ret), K(interval));
  } else if (OB_UNLIKELY(!is_preceding && static_cast<uint64_t>(row_idx + interval) > INT64_MAX)) {
    if (lib::is_mysql_mode()) {
      ret = OB_ERR_WINDOW_FRAME_ILLEGAL;
      LOG_WARN("frame start or end is negative, NULL or of non-integral type", K(ret),
               K(row_idx + interval));
    } else {
      ret = OB_DATA_OUT_OF_RANGE;
      LOG_WARN("int64 out of range", K(ret), K(row_idx + interval));
    }
  }
  return ret;
}
template <typename ColumnFmt>
int _calc_borders_for_rows_between(winfunc::WinExprEvalCtx &ctx, const int64_t row_start,
                                   const int64_t batch_size, const ObBitVector &eval_skip,
                                   ColumnFmt *between_data, const ObDatumMeta &meta,
                                   const bool is_preceding, const bool is_upper, int64_t *pos_arr)
{
  int ret = OB_SUCCESS;
  const char *payload = nullptr;
  int32_t len = 0;
  int64_t interval = 0;
  if (ob_is_number_tc(meta.type_)) {
    for (int i = 0; OB_SUCC(ret) && i < batch_size; i++) {
      int64_t row_idx = row_start + i;
      if (eval_skip.at(i) || between_data->is_null(i)) { continue; }
      between_data->get_payload(i, payload, len);
      const number::ObCompactNumber *cnum = reinterpret_cast<const number::ObCompactNumber *>(payload);
      number::ObNumber result_nmb(*cnum);
      if (lib::is_mysql_mode()) {
        if (OB_UNLIKELY(result_nmb.is_negative())) {
          ret = OB_ERR_WINDOW_FRAME_ILLEGAL;
          LOG_WARN("frame start or end is negative, NULL or of non-integral type", K(ret), K(result_nmb));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(result_nmb.extract_valid_int64_with_trunc(interval))) {
        LOG_WARN("extract invalid int64 failed", K(ret));
      } else if (OB_FAIL(check_interval_valid(row_idx, interval, is_preceding))) {
        LOG_WARN("check interval valid failed", K(ret));
      } else {
        pos_arr[i] = (is_preceding ? row_idx - interval : row_idx + interval);
        pos_arr[i] += (is_upper ? 0 : 1);
      }
    }
  } else if (ob_is_decimal_int(meta.type_)) {
    int16_t in_prec = meta.precision_;
    int16_t in_scale = meta.scale_;
    int16_t out_scale = 0;
    ObDecimalIntBuilder trunc_res_val;
    ObDatum in_datum;
    for (int i = 0; OB_SUCC(ret) && i < batch_size; i++) {
      int64_t row_idx = row_start + i;
      if (eval_skip.at(i) || between_data->is_null(i)) { continue; }
      between_data->get_payload(i, payload, len);
      if (lib::is_mysql_mode()) {
        if (OB_UNLIKELY(wide::is_negative(reinterpret_cast<const ObDecimalInt *>(payload), len))) {
          ret = OB_ERR_WINDOW_FRAME_ILLEGAL;
          LOG_WARN("frame start or end is negative, NULL or of non-integral type", K(ret));
        }
      } else if (in_scale == out_scale) {
        trunc_res_val.from(reinterpret_cast<const ObDecimalInt *>(payload), len);
      } else {
        in_datum.ptr_ = payload;
        in_datum.len_ = len;
        if (OB_FAIL(sql::ObExprTruncate::do_trunc_decimalint(in_prec, in_scale, in_prec, out_scale,
                                                             out_scale, in_datum, trunc_res_val))) {
          LOG_WARN("trunc decimal int failed", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        bool is_in_val_valid = false;
        if (OB_FAIL(wide::check_range_valid_int64(trunc_res_val.get_decimal_int(),
                                                  trunc_res_val.get_int_bytes(), is_in_val_valid,
                                                  interval))) {
          LOG_WARN("check range valid int64 failed", K(ret));
        } else if (!is_in_val_valid) {
          ret = OB_DATA_OUT_OF_RANGE;
          LOG_WARN("res_val is not valid int64", K(ret));
        } else if (OB_FAIL(check_interval_valid(row_idx, interval, is_preceding))) {
          LOG_WARN("invalid interval", K(ret));
        } else {
          pos_arr[i] = (is_preceding ? row_idx - interval : row_idx + interval);
          pos_arr[i] += (is_upper ? 0 : 1);
        }
      }
    }
  } else if (ob_is_int_tc(meta.type_)) {
    for (int i = 0; OB_SUCC(ret) && i < batch_size; i++) {
      int64_t row_idx = row_start + i;
      if (eval_skip.at(i) || between_data->is_null(i)) { continue; }
      between_data->get_payload(i, payload, len);
      interval = *reinterpret_cast<const int64_t *>(payload);
      if (lib::is_mysql_mode() && OB_UNLIKELY(interval < 0)) {
        ret = OB_ERR_WINDOW_FRAME_ILLEGAL;
        LOG_WARN("frame start or end is negative, NULL or of non-integral type", K(ret), K(interval));
      } else if (OB_FAIL(check_interval_valid(row_idx, interval, is_preceding))) {
        LOG_WARN("invalid interval", K(ret));
      } else {
        pos_arr[i] = (is_preceding ? row_idx - interval : row_idx + interval);
        pos_arr[i] += (is_upper ? 0 : 1);
      }
    }
  } else if (ob_is_uint_tc(meta.type_)) {
    for (int i = 0; OB_SUCC(ret) && i < batch_size; i++) {
      int64_t row_idx = row_start + i;
      if (eval_skip.at(i) || between_data->is_null(i)) { continue; }
      between_data->get_payload(i, payload, len);
      uint64_t tmp_val = *reinterpret_cast<const uint64_t *>(payload);
      if (lib::is_mysql_mode() && static_cast<int64_t>(tmp_val) < 0) {
        ret = OB_ERR_WINDOW_FRAME_ILLEGAL;
        LOG_WARN("frame start or end is negative, NULL or of non-integral type", K(ret), K(tmp_val));
      } else if (tmp_val > INT64_MAX) {
        ret = OB_DATA_OUT_OF_RANGE;
        LOG_WARN("int64 out of range", K(ret), K(tmp_val));
      } else {
        interval = static_cast<int64_t>(tmp_val);
        if (OB_FAIL(check_interval_valid(row_idx, interval, is_preceding))) {
          LOG_WARN("check interval failed", K(ret));
        } else {
          pos_arr[i] = (is_preceding ? row_idx - interval : row_idx + interval);
          pos_arr[i] += (is_upper ? 0 : 1);
        }
      }
    }
  } else if (ob_is_float_tc(meta.type_)) {
    for (int i = 0; OB_SUCC(ret) && i < batch_size; i++) {
      int64_t row_idx = row_start + i;
      if (eval_skip.at(i) || between_data->is_null(i)) { continue; }
      between_data->get_payload(i, payload, len);
      float tmp_val = *reinterpret_cast<const float *>(payload);
      if (lib::is_mysql_mode() && tmp_val < 0) {
        ret = OB_ERR_WINDOW_FRAME_ILLEGAL;
        LOG_WARN("frame start or end is negative, NULL or of non-integral type", K(ret), K(tmp_val));
      } else if (tmp_val > INT64_MAX) {
        ret = OB_DATA_OUT_OF_RANGE;
        LOG_WARN("out of range", K(ret), K(tmp_val));
      } else {
        interval = static_cast<int64_t>(tmp_val);
        if (OB_FAIL(check_interval_valid(row_idx, interval, is_preceding))) {
          LOG_WARN("check interval failed", K(ret));
        } else {
          pos_arr[i] = (is_preceding ? row_idx - interval : row_idx + interval);
          pos_arr[i] += (is_upper ? 0 : 1);
        }
      }
    }
  } else if (ob_is_double_tc(meta.type_)) {
    for (int i = 0; OB_SUCC(ret) && i < batch_size; i++) {
      int64_t row_idx = row_start + i;
      if (eval_skip.at(i) || between_data->is_null(i)) { continue; }
      between_data->get_payload(i, payload, len);
      double tmp_val = *reinterpret_cast<const double *>(payload);
      if (lib::is_mysql_mode() && tmp_val < 0) {
        ret = OB_ERR_WINDOW_FRAME_ILLEGAL;
        LOG_WARN("frame start or end is negative, NULL or of non-integral type", K(ret), K(tmp_val));
      } else if (tmp_val > INT64_MAX) {
        ret = OB_DATA_OUT_OF_RANGE;
        LOG_WARN("out of range", K(ret), K(tmp_val));
      } else {
        interval = static_cast<int64_t>(tmp_val);
        if (OB_FAIL(check_interval_valid(row_idx, interval, is_preceding))) {
          LOG_WARN("check interval failed", K(ret));
        } else {
          pos_arr[i] = (is_preceding ? row_idx - interval : row_idx + interval);
          pos_arr[i] += (is_upper ? 0 : 1);
        }
      }
    }
  } else if (ob_is_bit_tc(meta.type_)) {
    for (int i = 0; OB_SUCC(ret) && i < batch_size; i++) {
      int64_t row_idx = row_start + i;
      if (eval_skip.at(i) || between_data->is_null(i)) { continue; }
      between_data->get_payload(i, payload, len);
      uint64_t tmp_val = *reinterpret_cast<const uint64_t *>(payload);
      if (lib::is_mysql_mode() && static_cast<int64_t>(tmp_val) < 0) {
        ret = OB_ERR_WINDOW_FRAME_ILLEGAL;
        LOG_WARN("frame start or end is negative, NULL or of non-integral type", K(ret), K(tmp_val));
      } else if (tmp_val > INT64_MAX) {
        ret = OB_DATA_OUT_OF_RANGE;
        LOG_WARN("out of range", K(ret), K(tmp_val));
      } else {
        interval = static_cast<int64_t>(tmp_val);
        if (OB_FAIL(check_interval_valid(row_idx, interval, is_preceding))) {
          LOG_WARN("check interval failed", K(ret));
        } else {
          pos_arr[i] = (is_preceding ? row_idx - interval : row_idx + interval);
          pos_arr[i] += (is_upper ? 0 : 1);
        }
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not support type", K(ret), K(meta));
  }
  return ret;
}

int calc_borders_for_rows_between(winfunc::WinExprEvalCtx &ctx, const int64_t row_start,
                                  const int64_t batch_size, const ObBitVector &eval_skip,
                                  const ObExpr *between_expr, const bool is_preceding, const bool is_upper,
                                  int64_t *pos_arr)
{
#define CALC_BORDER(fmt)                                                                           \
  ret = _calc_borders_for_rows_between(ctx, row_start, batch_size, eval_skip,                      \
                                       static_cast<fmt *>(data), meta, is_preceding, is_upper,     \
                                       pos_arr)
#define CALC_FIXED_TYPE_BORDER(vec_tc)                                                             \
  case (vec_tc): { CALC_BORDER(ObFixedLengthFormat<RTCType<vec_tc>>); } break

  int ret = OB_SUCCESS;
  ObEvalCtx &eval_ctx = ctx.win_col_.op_.get_eval_ctx();
  VectorFormat fmt = between_expr->get_format(eval_ctx);
  const ObDatumMeta &meta = between_expr->datum_meta_;
  VecValueTypeClass vec_tc = get_vec_value_tc(meta.type_, meta.scale_, meta.precision_);
  ObIVector *data = between_expr->get_vector(eval_ctx);
  switch(fmt) {
  case common::VEC_UNIFORM: {
    CALC_BORDER(ObUniformFormat<false>);
    break;
  }
  case common::VEC_UNIFORM_CONST: {
    CALC_BORDER(ObUniformFormat<true>);
    break;
  }
  case common::VEC_DISCRETE: {
    CALC_BORDER(ObDiscreteFormat);
    break;
  }
  case common::VEC_CONTINUOUS: {
    CALC_BORDER(ObContinuousFormat);
    break;
  }
  case common::VEC_FIXED: {
    switch(vec_tc) {
      LST_DO_CODE(CALC_FIXED_TYPE_BORDER,
                  VEC_TC_INTEGER, VEC_TC_UINTEGER, VEC_TC_FLOAT,
                  VEC_TC_DOUBLE, VEC_TC_FIXED_DOUBLE, VEC_TC_BIT, VEC_TC_DEC_INT32,
                  VEC_TC_DEC_INT64, VEC_TC_DEC_INT128, VEC_TC_DEC_INT256, VEC_TC_DEC_INT512);
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected vector tc", K(ret), K(vec_tc));
      }
    }
    break;
  }
  default: {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected data format", K(ret), K(fmt));
  }
  }
  return ret;
#undef CALC_BORDER
#undef CALC_FIXED_TYPE_BORDER
}

int calc_borders_for_no_sort_expr(WinExprEvalCtx &ctx, const int64_t batch_size,
                                  const ObBitVector &eval_skip, const ObExpr *bound_expr,
                                  const bool is_upper, int64_t *pos_arr)
{
  int ret = OB_SUCCESS;
  ObEvalCtx &eval_ctx = ctx.win_col_.op_.get_eval_ctx();
  if (OB_UNLIKELY(!ob_is_integer_type(bound_expr->datum_meta_.type_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid bound value type", K(ret));
  } else if (OB_FAIL(bound_expr->eval_vector(eval_ctx, eval_skip, EvalBound(batch_size, false)))) {
    LOG_WARN("expr eval_vector failed", K(ret));
  } else {
    VectorFormat fmt = bound_expr->get_format(eval_ctx);
    VecValueTypeClass vec_tc = bound_expr->get_vec_value_tc();
    switch(fmt) {
    case common::VEC_UNIFORM: {
      ObUniformFormat<false> *data = static_cast<ObUniformFormat<false> *>(bound_expr->get_vector(eval_ctx));
      int64_t part_first_idx = ctx.win_col_.part_first_row_idx_;
      int64_t part_end_idx = ctx.win_col_.op_.get_part_end_idx();
      for (int i = 0; i < batch_size; i++) {
        if (eval_skip.at(i) || pos_arr[i] == INT64_MAX) { continue; }
        if (data->is_null(i) || data->get_bool(i)) {
          pos_arr[i] = (is_upper ? part_first_idx : part_end_idx);
        } else {
          pos_arr[i] = (is_upper ? part_end_idx : part_first_idx - 1);
        }
      }
      break;
    }
    case common::VEC_UNIFORM_CONST: {
      ObUniformFormat<true> *data = static_cast<ObUniformFormat<true> *>(bound_expr->get_vector(eval_ctx));
      int64_t part_first_idx = ctx.win_col_.part_first_row_idx_;
      int64_t part_end_idx = ctx.win_col_.op_.get_part_end_idx();
      for (int i = 0; i < batch_size; i++) {
        if (eval_skip.at(i) || pos_arr[i] == INT64_MAX) { continue; }
        if (data->is_null(i) || data->get_bool(i)) {
          pos_arr[i] = (is_upper ? part_first_idx : part_end_idx);
        } else {
          pos_arr[i] = (is_upper ? part_end_idx : part_first_idx - 1);
        }
      }
      break;
    }
    case common::VEC_FIXED: {
      if (vec_tc != VEC_TC_INTEGER && vec_tc != VEC_TC_UINTEGER) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected vec tc", K(ret), K(vec_tc));
      } else {
        // just use int64_t as RTCType, results are same for int64_t/uint64_t
        ObFixedLengthFormat<int64_t> *data =
          static_cast<ObFixedLengthFormat<int64_t> *>(bound_expr->get_vector(eval_ctx));
        int64_t part_first_idx = ctx.win_col_.part_first_row_idx_;
        int64_t part_end_idx = ctx.win_col_.op_.get_part_end_idx();
        for (int i = 0; i < batch_size; i++) {
          if (eval_skip.at(i) || pos_arr[i] == INT64_MAX) { continue; }
          if (data->is_null(i) || data->get_bool(i)) {
            pos_arr[i] = (is_upper ? part_first_idx : part_end_idx);
          } else {
            pos_arr[i] = (is_upper ? part_end_idx : part_first_idx - 1);
          }
        }
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected data format", K(ret));
    }
    }
  }
  return ret;
}

template <typename IntervalFmt, typename UnitFmt>
static int _batch_check_datetime_interval(IntervalFmt *interval_data, UnitFmt *unit_data,
                                          const int64_t batch_size, const ObBitVector &eval_skip)
{
  int ret = OB_SUCCESS;
  ObString interval_val;
  int64_t unit_value;
  ObInterval interval_time;
  for (int i = 0; OB_SUCC(ret) && i < batch_size; i++) {
    if (eval_skip.at(i)) {
    } else if (interval_data->is_null(i) || unit_data->is_null(i)) {
      ret = OB_ERR_WINDOW_FRAME_ILLEGAL;
      LOG_WARN("frame start or end is negative, NULL or of non-integral type", K(ret),
               K(interval_data->is_null(i)), K(unit_data->is_null(i)));
    } else {
      interval_val = interval_data->get_string(i);
      unit_value = unit_data->get_int(i);
      ObDateUnitType unit_val = static_cast<ObDateUnitType>(unit_value);
      if (OB_FAIL(ObTimeConverter::str_to_ob_interval(interval_val, unit_val, interval_time))) {
        if (OB_UNLIKELY(OB_INVALID_DATE_VALUE == ret)) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to convert string to ob interval", K(ret));
        }
      } else {
        bool is_valid = !(DT_MODE_NEG & interval_time.mode_);
        if (!is_valid) {
          ret = OB_ERR_WINDOW_FRAME_ILLEGAL;
          LOG_WARN("frame start or end is negative, NULL or of non-integral type", K(ret),
                   K(interval_val), K(unit_value));
        }
      }
    }
  }
  return ret;
}
static int _check_datetime_interval_valid(ObEvalCtx &eval_ctx, const ObExpr *bound_expr,
                                         const int64_t batch_size, const ObBitVector &eval_skip)
{
  int ret = OB_SUCCESS;
  if (bound_expr->type_ == T_FUN_SYS_DATE_ADD || bound_expr->type_ == T_FUN_SYS_DATE_SUB) {
    ObExpr *interval_expr = bound_expr->args_[1];
    ObExpr *unit_expr = bound_expr->args_[2];
    if (OB_FAIL(interval_expr->eval_vector(eval_ctx, eval_skip, EvalBound(batch_size, false)))
        || OB_FAIL(unit_expr->eval_vector(eval_ctx, eval_skip, EvalBound(batch_size, false)))) {
      LOG_WARN("expr evaluation failed", K(ret));
    } else if (OB_LIKELY(interval_expr->get_format(eval_ctx) == VEC_DISCRETE
                         && unit_expr->get_format(eval_ctx) == VEC_FIXED)) {
      ObDiscreteFormat *interval_data = static_cast<ObDiscreteFormat *>(interval_expr->get_vector(eval_ctx));
      ObFixedLengthFormat<int64_t> *unit_data = static_cast<ObFixedLengthFormat<int64_t> *>(unit_expr->get_vector(eval_ctx));
      if (OB_FAIL(_batch_check_datetime_interval(interval_data, unit_data, batch_size, eval_skip))) {
        LOG_WARN("check failed", K(ret));
      }
    } else {
      ObIVector *interval_data = interval_expr->get_vector(eval_ctx);
      ObIVector *unit_data = unit_expr->get_vector(eval_ctx);
      if (OB_FAIL(_batch_check_datetime_interval(interval_data, unit_data, batch_size, eval_skip))) {
        LOG_WARN("check failed", K(ret));
      }
    }
  }
  return ret;
}

template <typename ColumnFmt>
int _calc_borders_for_sort_expr(WinExprEvalCtx &ctx, const int64_t batch_size,
                                const int64_t row_start, const ObBitVector &eval_skip,
                                const bool is_upper, ColumnFmt *bound_data,
                                const ObExpr *bound_expr, int64_t *pos_arr)
{
  int ret = OB_SUCCESS;
  WinFuncInfo &wf_info = ctx.win_col_.wf_info_;
  int64_t cell_idx = wf_info.sort_collations_.at(0).field_idx_;
  bool is_ascending = wf_info.sort_collations_.at(0).is_ascending_;
  bool is_preceding = (is_upper ? wf_info.upper_.is_preceding_ : wf_info.lower_.is_preceding_);
  ObExpr *sort_expr = wf_info.sort_exprs_.at(0);
  sql::NullSafeRowCmpFunc sort_cmp_fn = wf_info.sort_cmp_funcs_.at(0).row_cmp_func_;
  ObObjMeta sort_obj_meta = sort_expr->obj_meta_;
  ObObjMeta bound_obj_meta = bound_expr->obj_meta_;
  const RowMeta &input_row_meta = ctx.win_col_.op_.get_input_row_meta();
  // range between ... and ...
  // copy from `ObWindowFunctionOp::get_pos`
  const static int L = 1;
  const static int LE = 1 << 1;
  const static int G = 1 << 2;
  const static int GE = 1 << 3;
  const static int ROLL = L | G;

  int64_t prev_row_border = -1;
  const char *l_ptr = nullptr, *r_ptr = nullptr;
  int32_t l_len = 0, r_len = 0;
  int cmp_ret = 0;
  bool l_isnull = false, r_isnull = false;
  int64_t part_first_idx = ctx.win_col_.part_first_row_idx_;
  int64_t part_end_idx = ctx.win_col_.op_.get_part_end_idx();
  for (int i = 0; OB_SUCC(ret) && i < batch_size; i++) {
    if (eval_skip.at(i) || pos_arr[i] == INT64_MAX) { continue; }
    int64_t row_idx = row_start + i;
    bool found = false;
    if (prev_row_border != -1) {
      if (OB_FAIL(cmp_prev_row(ctx, row_idx, cmp_ret))) {
        LOG_WARN("compare previous row failed", K(ret));
      } else if (cmp_ret == 0) {
        found = true;
        pos_arr[i] = prev_row_border;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (!found) {
      int64_t pos = row_idx;
      bool re_direction = false;
      int cmp_mode = !(is_preceding ^ is_ascending) ? L : G;
      if (is_preceding ^ is_upper) {
        cmp_mode = cmp_mode << 1; // why???
      }
      int step = (cmp_mode & ROLL) ? 1 : 0;
      int cmp_times = 0;
      r_isnull = bound_data->is_null(i);
      bound_data->get_payload(i, r_ptr, r_len);
      while (OB_SUCC(ret)) {
        cmp_times++;
        bool match = false;
        const ObCompactRow *a_row = nullptr;
        (is_preceding ^ re_direction) ? (pos -= step) : (pos += step);
        const bool overflow =
          (is_preceding ^ re_direction) ? (pos < part_first_idx) : (pos >= part_end_idx);
        if (overflow) {
          match = true;
        } else if (OB_FAIL(ctx.input_rows_.get_row(pos , a_row))) {
          LOG_WARN("get row failed", K(ret));
        } else {
          l_isnull = a_row->is_null(cell_idx);
          a_row->get_cell_payload(input_row_meta, cell_idx, l_ptr, l_len);
          if (OB_FAIL(sort_cmp_fn(sort_obj_meta, sort_obj_meta,
                                  l_ptr, l_len, l_isnull,
                                  r_ptr, r_len, r_isnull, cmp_ret))) {
            LOG_WARN("compare failed", K(ret));
          } else {
            match = ((cmp_mode & L) && cmp_ret < 0)
                    || ((cmp_mode & LE) && cmp_ret <= 0)
                    || ((cmp_mode & G) && cmp_ret > 0)
                    || ((cmp_mode & GE) && cmp_ret >= 0);
            LOG_DEBUG("cmp_result", K(ret), K(cmp_mode), K(match), K(pos), K(row_start),
                      K(cmp_times), K(l_isnull), K(r_isnull), K(is_preceding), K(is_ascending));
          }
        }
        if (OB_SUCC(ret)) {
          if (match) {
            if (pos == row_idx && !(cmp_mode & ROLL)) {
              // for LE/GE, if equal to current row,
              // change cmp_mode to search opposite direction.
              if (LE == cmp_mode) {
                cmp_mode = G;
              } else if (GE == cmp_mode) {
                cmp_mode = L;
              }
              re_direction = true;
              step = 1;
            } else if (step <= 1) {
              if (cmp_mode & ROLL) {
                (is_preceding ^ re_direction) ? (pos += step) : (pos -= step);
              }
              break;
            } else {
              (is_preceding ^ re_direction) ? (pos += step) : (pos -= step);
              step = 1;
            }
          } else {
            step = (0 == step ? 1 : (2 * step));
          }
        }
      } // end inner while
      if (OB_FAIL(ret)) {
      } else {
        pos_arr[i] = pos + (is_upper ? 0 : 1);
        prev_row_border = pos_arr[i];
      }
    }
  } // end for
  return ret;
}
int calc_borders_for_sort_expr(WinExprEvalCtx &ctx, const ObExpr *bound_expr,
                               const int64_t batch_size, const int64_t row_start,
                               ObBitVector &eval_skip, const bool is_upper, int64_t *pos_arr)
{
#define CALC_BORDER(fmt)                                                                           \
  ret = _calc_borders_for_sort_expr(ctx, batch_size, row_start, eval_skip, is_upper,               \
                                    static_cast<fmt *>(data), bound_expr, pos_arr)

#define CALC_FIXED_TYPE_BORDER(vec_tc)                                                             \
  case (vec_tc): { CALC_BORDER(ObFixedLengthFormat<RTCType<vec_tc>>); } break

  int ret = OB_SUCCESS;
  ObEvalCtx &eval_ctx = ctx.win_col_.op_.get_eval_ctx();
  bool is_nmb_literal = (is_upper ? ctx.win_col_.wf_info_.upper_.is_nmb_literal_ :
                                    ctx.win_col_.wf_info_.lower_.is_nmb_literal_);
  if (OB_FAIL(bound_expr->eval_vector(
        eval_ctx, eval_skip,
        EvalBound(batch_size, eval_skip.accumulate_bit_cnt(batch_size) == 0)))) {
    LOG_WARN("eval vector failed", K(ret));
  } else if (lib::is_mysql_mode() && !is_nmb_literal
             && ob_is_temporal_type(bound_expr->datum_meta_.type_)) {
    if (OB_FAIL(_check_datetime_interval_valid(eval_ctx, bound_expr, batch_size, eval_skip))) {
      LOG_WARN("invalid datetime interval", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else {
    ObIVector *data = bound_expr->get_vector(eval_ctx);
    VectorFormat fmt = bound_expr->get_format(eval_ctx);
    VecValueTypeClass vec_tc = bound_expr->get_vec_value_tc();
    switch(fmt) {
    case common::VEC_UNIFORM: {
      CALC_BORDER(ObUniformFormat<false>);
      break;
    }
    case common::VEC_UNIFORM_CONST: {
      CALC_BORDER(ObUniformFormat<true>);
      break;
    }
    case common::VEC_DISCRETE: {
      CALC_BORDER(ObDiscreteFormat);
      break;
    }
    case common::VEC_CONTINUOUS: {
      CALC_BORDER(ObContinuousFormat);
      break;
    }
    case common::VEC_FIXED: {
      switch(vec_tc) {
        // TODO: check supported types of bound expr
        LST_DO_CODE(CALC_FIXED_TYPE_BORDER, FIXED_VEC_LIST);
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unsupported type", K(ret), K(vec_tc));
        }
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unsupport data format", K(ret), K(fmt), K(*bound_expr));
    }
    }
  }
  return ret;
}

int cmp_prev_row(WinExprEvalCtx &ctx, const int64_t cur_idx, int &cmp_ret)
{
  int ret = OB_SUCCESS;
  const ObCompactRow *stored_rows[2] = {nullptr};
  if (OB_FAIL(ctx.input_rows_.get_batch_rows(cur_idx - 1, cur_idx + 1, stored_rows))) {
    LOG_WARN("get stored rows failed", K(ret));
  }
  cmp_ret = 0;
  ObSortCollations &sort_collations = ctx.win_col_.wf_info_.sort_collations_;
  ObSortFuncs &sort_cmp_funcs = ctx.win_col_.wf_info_.sort_cmp_funcs_;
  ObIArray<ObExpr *> &sort_exprs = ctx.win_col_.wf_info_.sort_exprs_;
  const char *l_data = nullptr, *r_data = nullptr;
  int32_t l_len = 0, r_len = 0;
  bool l_isnull = false, r_isnull = false;
  const RowMeta &input_row_meta = ctx.win_col_.op_.get_input_row_meta();
  for (int i = 0; OB_SUCC(ret) && cmp_ret == 0 && i < sort_collations.count(); i++) {
    int64_t field_idx = sort_collations.at(i).field_idx_;
    ObObjMeta &obj_meta = sort_exprs.at(i)->obj_meta_;
    sql::NullSafeRowCmpFunc cmp_fn = sort_cmp_funcs.at(i).row_cmp_func_;
    stored_rows[0]->get_cell_payload(input_row_meta, field_idx, l_data, l_len);
    l_isnull = stored_rows[0]->is_null(field_idx);
    stored_rows[1]->get_cell_payload(input_row_meta, field_idx, r_data, r_len);
    r_isnull = stored_rows[1]->is_null(field_idx);
    if (OB_FAIL(
          cmp_fn(obj_meta, obj_meta, l_data, l_len, l_isnull, r_data, r_len, r_isnull, cmp_ret))) {
      LOG_WARN("compare failed", K(ret));
    }
  }
  return ret;
}

template<>
struct int_trunc<VEC_TC_NUMBER>
{
  static int get(const char *payload, const int32_t len,const ObDatumMeta &meta, int64_t &value)
  {
    int ret = OB_SUCCESS;
    number::ObNumber res_nmb(*reinterpret_cast<const number::ObCompactNumber *>(payload));
    if (OB_FAIL(res_nmb.extract_valid_int64_with_trunc(value))) {
      LOG_WARN("truncate integer failed", K(ret));
    }
    return ret;
  }
};

template<>
struct int_trunc<VEC_TC_INTEGER>
{
  static int get(const char *payload, const int32_t len,const ObDatumMeta &meta, int64_t &value)
  {
    int ret = OB_SUCCESS;
    value = *reinterpret_cast<const int64_t *>(payload);
    return ret;
  }
};

template <>
struct int_trunc<VEC_TC_UINTEGER>
{
  static int get(const char *payload, const int32_t len, const ObDatumMeta &meta, int64_t &value)
  {
    int ret = OB_SUCCESS;
    uint64_t tmp_value = *reinterpret_cast<const uint64_t *>(payload);
    bool is_valid_param = !lib::is_mysql_mode() || static_cast<int64_t>(tmp_value) >= 0;
    if (tmp_value > INT64_MAX && is_valid_param) {
      ret = OB_DATA_OUT_OF_RANGE;
      LOG_WARN("int64 out of range", K(ret));
    } else {
      value = static_cast<int64_t>(tmp_value);
    }
    return ret;
  }
};

template<>
struct int_trunc<VEC_TC_BIT>: public int_trunc<VEC_TC_UINTEGER> {};

template<>
struct int_trunc<VEC_TC_FLOAT>
{
  static int get(const char *payload, const int32_t len,const ObDatumMeta &meta, int64_t &value)
  {
    int ret = OB_SUCCESS;
    const float tmp_value = *reinterpret_cast<const float *>(payload);
    if (tmp_value > INT64_MAX) {
      ret = OB_DATA_OUT_OF_RANGE;
      LOG_WARN("int64 out of range", K(ret));
    } else {
      value = static_cast<int64_t>(tmp_value);
    }
    return ret;
  }
};

template<>
struct int_trunc<VEC_TC_DOUBLE>
{
  static int get(const char *payload, const int32_t len,const ObDatumMeta &meta, int64_t &value)
  {
    int ret = OB_SUCCESS;
    const double tmp_value = *reinterpret_cast<const double *>(payload);
    if (tmp_value > INT64_MAX) {
      ret = OB_DATA_OUT_OF_RANGE;
      LOG_WARN("int64 out of range", K(ret));
    } else {
      value = static_cast<int64_t>(tmp_value);
    }
    return ret;
  }
};

template<>
struct int_trunc<VEC_TC_FIXED_DOUBLE>: public int_trunc<VEC_TC_DOUBLE> {};

template<typename int_type>
struct dec_int_trunc
{
  static int get(const char *payload, const int32_t len,const ObDatumMeta &meta, int64_t &value)
  {
    int ret = OB_SUCCESS;
    ObScale out_scale = 0;
    ObDecimalIntBuilder trunc_res_val;
    ObDatum in_datum;
    in_datum.ptr_ = payload;
    in_datum.len_ = len;
    bool is_valid = false;
    if (meta.scale_ == out_scale) {
      trunc_res_val.from(reinterpret_cast<const ObDecimalInt *>(payload), len);
    } else if (OB_FAIL(ObExprTruncate::do_trunc_decimalint(meta.precision_, meta.scale_,
                                                           meta.precision_, out_scale, out_scale,
                                                          in_datum, trunc_res_val))) {
      LOG_WARN("truncate decimal int failed", K(ret));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(wide::check_range_valid_int64(trunc_res_val.get_decimal_int(),
                                                     trunc_res_val.get_int_bytes(), is_valid,
                                                     value))) {
      LOG_WARN("check valid int64 failed", K(ret));
    } else if (OB_UNLIKELY(!is_valid)) {
      ret = OB_DATA_OUT_OF_RANGE;
      LOG_WARN("int64 out of range", K(ret));
    }
    return ret;
  }
};

template<>
struct int_trunc<VEC_TC_DEC_INT32>: public dec_int_trunc<int32_t>{};

template<>
struct int_trunc<VEC_TC_DEC_INT64>: public dec_int_trunc<int64_t>{};

template<>
struct int_trunc<VEC_TC_DEC_INT128>: public dec_int_trunc<int128_t>{};

template<>
struct int_trunc<VEC_TC_DEC_INT256>: public dec_int_trunc<int256_t>{};

template<>
struct int_trunc<VEC_TC_DEC_INT512>: public dec_int_trunc<int512_t>{};
} // end winfunc
} // end sql
} // end oceanbase