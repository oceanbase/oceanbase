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
#include "sql/engine/window_function/ob_window_function_op.h"
#include "lib/utility/utility.h"
#include "share/object/ob_obj_cast.h"
#include "common/row/ob_row_util.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/expr/ob_sql_expression.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_func_ceil.h"
#include "sql/engine/expr/ob_expr_add.h"
#include "sql/engine/expr/ob_expr_minus.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "sql/engine/expr/ob_expr_truncate.h"
#include "sql/engine/px/ob_px_sqc_proxy.h"
#include "sql/engine/px/ob_px_sqc_handler.h"
#include "sql/engine/px/datahub/components/ob_dh_range_dist_wf.h"

namespace oceanbase
{
using namespace common;
namespace sql
{
const int64_t CHECK_STATUS_INTERVAL = 10000;
OB_SERIALIZE_MEMBER(WinFuncInfo::ExtBound,
                    is_preceding_,
                    is_unbounded_,
                    is_nmb_literal_,
                    between_value_expr_,
                    range_bound_expr_);

OB_SERIALIZE_MEMBER(WinFuncInfo,
                    win_type_,
                    func_type_,
                    is_ignore_null_,
                    is_from_first_,
                    expr_,
                    aggr_info_,
                    upper_,
                    lower_,
                    param_exprs_,
                    partition_exprs_,
                    sort_exprs_,
                    sort_collations_,
                    sort_cmp_funcs_,
                    remove_type_,
                    can_push_down_);

OB_SERIALIZE_MEMBER((ObWindowFunctionSpec, ObOpSpec),
                    wf_infos_,
                    all_expr_,
                    single_part_parallel_,
                    range_dist_parallel_,
                    rd_wfs_,
                    rd_coord_exprs_,
                    rd_sort_collations_,
                    rd_sort_cmp_funcs_,
                    rd_pby_sort_cnt_,
                    role_type_,
                    wf_aggr_status_expr_,
                    input_rows_mem_bound_ratio_,
                    estimated_part_cnt_,
                    enable_hash_base_distinct_);

OB_SERIALIZE_MEMBER(ObWindowFunctionOpInput, local_task_count_, total_task_count_, wf_participator_shared_info_);

// to ensure whole_msg_provider->reset() after all the tasks have dealt with the whole msg
int ObWindowFunctionOpInput::sync_wait(
    ObExecContext &ctx, ObReportingWFWholeMsg::WholeMsgProvider *whole_msg_provider)
{
  int ret = OB_SUCCESS;
  ObWFParticipatorSharedInfo *shared_info =
      reinterpret_cast<ObWFParticipatorSharedInfo *>(wf_participator_shared_info_);
  if (OB_ISNULL(shared_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: shared info is null", K(ret));
  } else if (OB_ISNULL(whole_msg_provider)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: whole_msg_provider is null", K(ret));
  } else if (!whole_msg_provider->msg_set()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: whole_msg_provider has not been set msg", K(ret));
  } else {
    const int64_t exit_cnt = shared_info->sqc_thread_count_;
    int64_t &sync_cnt = shared_info->process_cnt_;
    bool has_process = false;
    int64_t loop = 0;
    while (OB_SUCC(ret)) {
      ++loop;
      if (!has_process) {
        ObSpinLockGuard guard(shared_info->lock_);
        // guarantee next wait loop to get lock for one thread
        has_process = true;
        if (0 == ATOMIC_AAF(&sync_cnt, 1) % exit_cnt) {
          // last thread, it will signal and exit by self
          shared_info->cond_.signal();
          LOG_DEBUG("debug sync_cnt", K(ret), K(sync_cnt), K(lbt()));
          break;
        }
      }
      if (OB_SUCCESS != shared_info->ret_) {
        ret = shared_info->ret_; // the thread already return error
      } else if (0 == loop % 8 && OB_UNLIKELY(IS_INTERRUPTED())) {
        ObInterruptCode code = GET_INTERRUPT_CODE();
        ret = code.code_; // overwrite ret
        LOG_WARN("received a interrupt", K(code), K(ret));
      } else if (0 == loop % 16 && OB_FAIL(ctx.fast_check_status())) {
        LOG_WARN("failed to check status", K(ret));
      } else if (0 == ATOMIC_LOAD(&sync_cnt) % exit_cnt) { // timeout, and signal has done
        LOG_DEBUG("debug sync_cnt", K(ret), K(sync_cnt), K(loop), K(exit_cnt), K(lbt()));
        break;
      } else {
        auto key = shared_info->cond_.get_key();
        shared_info->cond_.wait(key, 1000); // wait 1000 us each time
      }
    } // end while
    if (OB_SUCC(ret)) {
      ObSpinLockGuard guard(shared_info->lock_);
      if (whole_msg_provider->msg_set()) {
        whole_msg_provider->reset();
        //ATOMIC_SET(&sync_cnt, 0);
      }
    }
  }
  return ret;
}

DEF_TO_STRING(ObWindowFunctionSpec)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_NAME("op_spec");
  J_COLON();
  pos += ObOpSpec::to_string(buf + pos, buf_len - pos);
  J_COMMA();
  J_KV(K_(wf_infos));
  J_OBJ_END();
  return pos;
}

int ObWindowFunctionSpec::rd_generate_patch(ObRDWFPieceMsgCtx &ctx) const
{
  int ret = OB_SUCCESS;
  // sort by (PBY, OBY, SQC_ID, THREAD_ID)
  lib::ob_sort(ctx.infos_.begin(), ctx.infos_.end(),
            [&](ObRDWFPartialInfo *l, ObRDWFPartialInfo *r) {
              int cmp = 0;
              (void)rd_pby_oby_cmp(l->first_row_, r->first_row_, cmp);
              if (0 == cmp) {
                (void)rd_pby_oby_cmp(l->last_row_, r->last_row_, cmp);
              }
              return (0 == cmp)
              ? (std::tie(l->sqc_id_, l->thread_id_) < std::tie(r->sqc_id_, r->thread_id_))
              : (cmp < 0);
            });
  LOG_TRACE("before generate patch", K(ctx.infos_));
  auto frame_offset = [](ObStoredDatumRow *f)->ObRDWFPartialInfo::RowExtType & {
    return f->extra_payload<ObRDWFPartialInfo::RowExtType>();
  };
  ObRDWFPartialInfo *prev = NULL;

  // generate frame offset first
  int cmp_ret = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < ctx.infos_.count(); i++) {
    ObRDWFPartialInfo *cur = ctx.infos_.at(i);
    if (NULL == cur->first_row_) {
      break;
    }
    bool prev_same_part = NULL != prev;
    if (prev_same_part) {
      if (OB_FAIL(rd_pby_cmp(prev->last_row_, cur->first_row_, cmp_ret))) {
        LOG_WARN("compare failed", K(ret));
      } else {
        prev_same_part = prev_same_part && (cmp_ret == 0);
      }
    }
    if (OB_FAIL(ret)) {
    } else if (prev_same_part) {
      frame_offset(cur->first_row_) = frame_offset(prev->last_row_) + 1;
      if (OB_FAIL(rd_pby_cmp(cur->first_row_, cur->last_row_, cmp_ret))) {
        LOG_WARN("compare failed", K(ret));
      } else if (0 == cmp_ret) {
        frame_offset(cur->last_row_) += frame_offset(prev->last_row_) + 1;
      }
    }
    prev = cur;
  }
  // generate patch for each window function
  typedef ObWindowFunctionOp OP;
  for (int64_t idx = 0; OB_SUCC(ret) && idx < rd_wfs_.count(); idx++) {
    const WinFuncInfo &info = wf_infos_.at(rd_wfs_.at(idx));
    const bool is_rank = T_WIN_FUN_RANK == info.func_type_;
    const bool is_dense_rank = T_WIN_FUN_DENSE_RANK == info.func_type_;
    int64_t res_idx = idx + rd_sort_collations_.count();
    auto res_datum = [&res_idx](ObStoredDatumRow *r)->ObDatum & { return r->cells()[res_idx]; };
    prev = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < ctx.infos_.count(); i++) {
      ObRDWFPartialInfo *cur = ctx.infos_.at(i);
      if (NULL == cur->first_row_) {
        if (NULL != prev) {
          res_datum(prev->last_row_).set_null();
        }
        break;
      }
      bool prev_same_part = NULL != prev;
      if (prev_same_part) {
        if (OB_FAIL(rd_pby_cmp(prev->last_row_, cur->first_row_, cmp_ret))) {
          LOG_WARN("compare failed", K(ret));
        } else {
          prev_same_part = prev_same_part && (cmp_ret == 0);
        }
      }
      if (OB_FAIL(ret)) {
      } else if (!prev_same_part) {
        res_datum(cur->first_row_).set_null();
        if (NULL != prev) {
          res_datum(prev->last_row_).set_null();
        }
      } else {
        ObDatum prev_last;
        prev_last.set_null();
        bool prev_same_order = false;
        if (OB_FAIL(rd_oby_cmp(prev->last_row_, cur->first_row_, cmp_ret))) {
          LOG_WARN("compare failed", K(ret));
        } else {
          prev_same_order = (0 == cmp_ret);
        }
        if (OB_FAIL(ret)) {
        } else if (prev_same_order) {
          // aggregate the remaining partial result of the same order to %prev_last
          if (!(is_rank || is_dense_rank) && WINDOW_RANGE == info.win_type_) {
            prev_last = res_datum(cur->first_row_);
            for (int64_t j = i + 1; OB_SUCC(ret) && j < ctx.infos_.count(); j++) {
              ObRDWFPartialInfo *partial_info = ctx.infos_.at(j);
              cmp_ret = 1;
              if (NULL != partial_info->first_row_ && OB_FAIL(rd_pby_oby_cmp(cur->first_row_, partial_info->first_row_, cmp_ret))) {
                LOG_WARN("compare failed", K(ret));
              } else if (cmp_ret == 0) {
                OZ(OP::merge_aggregated_result(prev_last, info, ctx.arena_alloc_,
                                               prev_last, res_datum(partial_info->first_row_)));
              } else {
                break;
              }
            }
          }
        }
        if (OB_SUCC(ret)) {
          if (is_rank || is_dense_rank) {
            if (prev_same_order) {
              OZ(OP::rank_add(res_datum(cur->first_row_), info, ctx.arena_alloc_,
                              res_datum(prev->last_row_), -1));
            } else {
              if (is_rank) {
                ObDatum null_datum;
                null_datum.set_null();
                OZ(OP::rank_add(res_datum(cur->first_row_), info, ctx.arena_alloc_,
                                null_datum, frame_offset(cur->first_row_)));
              } else {
                res_datum(cur->first_row_) = res_datum(prev->last_row_);
              }
            }
          } else {
            res_datum(cur->first_row_) = res_datum(prev->last_row_);
          }
        }
        if (OB_SUCC(ret)) {
          res_datum(prev->last_row_) = prev_last;
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(rd_pby_cmp(cur->first_row_, cur->last_row_, cmp_ret))) {
          LOG_WARN("compare failed", K(ret));
        } else if (cmp_ret == 0) {
          bool do_rank_add = false;
          if (is_rank) {
            if (OB_FAIL(rd_oby_cmp(cur->first_row_, cur->last_row_, cmp_ret))) {
              LOG_WARN("compare failed", K(ret));
            } else if (cmp_ret != 0) {
              do_rank_add = true;
            }
          }
          if (OB_FAIL(ret)) {
          } else if (do_rank_add) {
            OZ(OP::rank_add(res_datum(cur->last_row_), info, ctx.arena_alloc_,
                            res_datum(cur->last_row_), frame_offset(cur->first_row_)));
          } else {
            OZ(OP::merge_aggregated_result(res_datum(cur->last_row_), info, ctx.arena_alloc_,
                                          res_datum(cur->last_row_), res_datum(cur->first_row_)));
          }
        }
      }
      prev = cur;
    }
    // set the last info's last row's patch to NULL
    if (OB_SUCC(ret)) {
      auto last_info = ctx.infos_.at(ctx.infos_.count() - 1);
      if (NULL != last_info->last_row_) {
        res_datum(last_info->last_row_).set_null();
      }
    }
  }
  LOG_TRACE("after generate patch", K(ctx.infos_));
  return ret;
}

int ObWindowFunctionOp::WinFuncCell::reset_res(const int64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(res_.reset(tenant_id))) {
    LOG_WARN("reset part rows store failed", K(ret));
  }
  return ret;
}

int ObWindowFunctionOp::AggrCell::trans_self(const ObRADatumStore::StoredRow &row)
{
  int ret = OB_SUCCESS;
  ObAggregateProcessor::GroupRow *group_row = NULL;
  if (!finish_prepared_ && OB_FAIL(aggr_processor_.init_one_group())) {
    LOG_WARN("fail to prepare the aggr func", K(ret), K(row));
  } else if (OB_FAIL(aggr_processor_.get_group_row(0, group_row))) {
    LOG_WARN("failed to get_group_row", K(ret));
  } else if (OB_ISNULL(group_row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("group_row is null", K(ret));
  } else if (!finish_prepared_) {
    if ((OB_FAIL(aggr_processor_.prepare(*group_row)))) {
      LOG_WARN("fail to prepare the aggr func", K(ret), K(row));
    } else {
      finish_prepared_ = true;
    }
  } else {
    // TODO: shanting, batch process
    if (OB_FAIL(aggr_processor_.process(*group_row))) {
      LOG_WARN("fail to process the aggr func", K(ret), K(row));
    }
  }

  if (OB_SUCC(ret)) {
    // uppon invoke trans(), forbidden it to reuse the last_result
    got_result_ = false;
  }
  return ret;
}

int ObWindowFunctionOp::AggrCell::inv_trans_self(const ObRADatumStore::StoredRow &row)
{
  int ret = OB_SUCCESS;
  if (common::REMOVE_STATISTICS == remove_type_) {
    // only support sum count and avg now.
    if (!finish_prepared_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("not finish prepare before inv_trans", K(ret));
    } else {
      aggr_processor_.get_removal_info().is_inv_aggr_ = true;
      ObAggregateProcessor::GroupRow *group_row = NULL;
      if (OB_FAIL(aggr_processor_.get_group_row(0, group_row))) {
        LOG_WARN("failed to get_group_row", K(ret));
      } else if (OB_ISNULL(group_row)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("group_row is null", K(ret));
      } else {
        // TODO: shanting, batch process
        if (OB_FAIL(aggr_processor_.process(*group_row))) {
          LOG_WARN("fail to process the aggr func", K(ret), K(row));
        }
      }
      if (OB_SUCC(ret)) {
        // uppon invoke inv_trans(), forbidden it to reuse the last_result
        got_result_ = false;
      }
      aggr_processor_.get_removal_info().is_inv_aggr_ = false;
    }
  } else if (common::REMOVE_EXTRENUM == remove_type_) {
    // inv_trans for max and min, do nothing
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("aggr not support inv_trans", K(ret));
  }
  return ret;
}

int ObWindowFunctionOp::AggrCell::final(ObDatum &val)
{
  int ret = OB_SUCCESS;
  if (!got_result_) {
    if (OB_FAIL(aggr_processor_.collect(0))) {
      LOG_WARN("fail to collect", K(ret));
    } else {
      val = wf_info_.aggr_info_.expr_->locate_expr_datum(op_.eval_ctx_);
      if (OB_FAIL(aggr_processor_.clone_cell_for_wf(result_, val,
                                             wf_info_.aggr_info_.expr_->obj_meta_.is_number()))) {
        LOG_WARN("fail to clone_cell", K(ret));
      } else {
        got_result_ = true;
      }
    }
  } else {
    val = static_cast<ObDatum>(result_);
  }
  return ret;
}

DEF_TO_STRING(ObWindowFunctionOp::AggrCell)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_NAME("wf_cell");
  J_COLON();
  pos += ObWindowFunctionOp::WinFuncCell::to_string(buf + pos, buf_len - pos);
  J_COMMA();
  J_KV(K_(finish_prepared), K_(result));
  J_OBJ_END();
  return pos;
}

template <typename OP>
int ObWindowFunctionOp::foreach_stores(OP op)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(op(input_rows_))) {
    LOG_WARN("operate input_rows_ failed", K(ret));
  } else {
    DLIST_FOREACH(wf, wf_list_) {
      if (OB_FAIL(op(wf->res_))) {
        LOG_WARN("operate window function result failed", K(ret));
      }
    }
  }
  return ret;
}

int ObWindowFunctionOp::get_param_int_value(ObExpr &expr,
                                            ObEvalCtx &eval_ctx,
                                            bool &is_null,
                                            int64_t &value,
                                            const bool need_number/* = false*/,
                                            const bool need_check_valid/* = false*/)
{
  int ret = OB_SUCCESS;
  ObDatum *result = NULL;
  bool is_valid_param = true;
  is_null = false;
  value = 0;
  if (OB_FAIL(expr.eval(eval_ctx, result))) {
    LOG_WARN("eval failed", K(ret));
  } else if (result->is_null()) {
    is_null = true;
    is_valid_param = !need_check_valid;
  } else if (need_number || expr.obj_meta_.is_number()) {
    //we restrict the bucket_num in range [0, (1<<63)-1]
    number::ObNumber result_nmb;
    number::ObCompactNumber &cnum = const_cast<number::ObCompactNumber &>(
        result->get_number());
    result_nmb.assign(cnum.desc_.desc_, cnum.digits_ + 0);
    is_valid_param = !need_check_valid || !result_nmb.is_negative();
    if (OB_FAIL(result_nmb.extract_valid_int64_with_trunc(value))) {
      LOG_WARN("extract_valid_int64_with_trunc failed", K(ret));
    }
  } else if (expr.obj_meta_.is_decimal_int()) {
    ObDecimalIntBuilder trunc_res_val;
    const int16_t in_prec = expr.datum_meta_.precision_;
    const int16_t in_scale = expr.datum_meta_.scale_;
    const int16_t out_scale = 0;
    if (in_scale == out_scale) {
      trunc_res_val.from(result->get_decimal_int(), result->get_int_bytes());
    } else if (OB_FAIL(ObExprTruncate::do_trunc_decimalint(in_prec, in_scale,
        in_prec, out_scale, out_scale, *result, trunc_res_val))) {
      LOG_WARN("calc_trunc_decimalint failed", K(ret), K(in_prec), K(in_scale),
             K(in_prec), K(out_scale), K(result->get_int_bytes()));
    }
    if (OB_SUCC(ret)) {
      bool is_in_val_valid = false;
      if (OB_FAIL(wide::check_range_valid_int64(trunc_res_val.get_decimal_int(),
                  trunc_res_val.get_int_bytes(), is_in_val_valid, value))) {
        LOG_WARN("check_range_valid_int64 failed", K(ret), K(trunc_res_val.get_int_bytes()));
      } else if (!is_in_val_valid) {
        ret = OB_DATA_OUT_OF_RANGE;
        LOG_WARN("res_val is not a valid int64", K(ret), K(result->get_int_bytes()), K(in_scale));
      }
    }
  } else {
    switch (expr.obj_meta_.get_type_class()) {
      case ObIntTC: {
        value = result->get_int();
        is_valid_param = !need_check_valid || value >= 0;
        break;
      }
      case ObUIntTC: {
        const uint64_t tmp_value = result->get_uint();
        is_valid_param = !need_check_valid || static_cast<int64_t>(tmp_value) >= 0;
        if (tmp_value > INT64_MAX && is_valid_param) {
          ret = OB_DATA_OUT_OF_RANGE;
          LOG_WARN("int64 out of range", K(ret), K(tmp_value), K(INT64_MAX));
        } else {
          value = static_cast<int64_t>(tmp_value);
        }
        break;
      }
      case ObFloatTC: {
        const float tmp_value = result->get_float();
        is_valid_param = !need_check_valid || tmp_value >= 0;
        if (tmp_value > INT64_MAX) {
          ret = OB_DATA_OUT_OF_RANGE;
          LOG_WARN("int64 out of range", K(ret), K(tmp_value), K(INT64_MAX));
        } else {
          value = static_cast<int64_t>(tmp_value);
        }
        break;
      }
      case ObDoubleTC: {
        const double tmp_value = result->get_double();
        is_valid_param = !need_check_valid || tmp_value >= 0;
        if (tmp_value > INT64_MAX) {
          ret = OB_DATA_OUT_OF_RANGE;
          LOG_WARN("int64 out of range", K(ret), K(tmp_value), K(INT64_MAX));
        } else {
          value = static_cast<int64_t>(tmp_value);
        }
        break;
      }
      case ObBitTC: {
        const uint64_t tmp_value = result->get_bit();
        is_valid_param = !need_check_valid || static_cast<int64_t>(tmp_value) >= 0;
        if (tmp_value > INT64_MAX && is_valid_param) {
          ret = OB_DATA_OUT_OF_RANGE;
          LOG_WARN("int64 out of range", K(ret), K(tmp_value), K(INT64_MAX));
        } else {
          value = static_cast<int64_t>(tmp_value);
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("not support type", K(expr), K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && !is_valid_param) {
    ret = OB_ERR_WINDOW_FRAME_ILLEGAL;
    LOG_WARN("frame start or end is negative, NULL or of non-integral type", K(ret), K(value), KPC(result));
  }
  return ret;
}


int ObWindowFunctionOp::NonAggrCellRowNumber::eval(RowsReader &row_reader,
                                                const int64_t row_idx,
                                                const ObRADatumStore::StoredRow &row,
                                                const Frame &frame,
                                                ObDatum &val)
{
  int ret = OB_SUCCESS;
  UNUSED(row_reader);
  UNUSED(row_idx);
  UNUSED(row);
  UNUSED(frame);
  ObDatum &expr_datum = wf_info_.expr_->locate_datum_for_write(op_.eval_ctx_);
  int64_t row_number = row_idx - frame.head_ + 1;
  wf_info_.expr_->set_evaluated_flag(op_.eval_ctx_);
  if (lib::is_oracle_mode()) {
    number::ObNumber res_nmb;
    ObNumStackAllocator<3> tmp_alloc;
    if (OB_FAIL(res_nmb.from(row_number, tmp_alloc))) {
      LOG_WARN("failed to build number from int64_t", K(ret));
    } else {
      expr_datum.set_number(res_nmb);
    }
  } else {
    expr_datum.set_int(row_number);
  }
  val = expr_datum;
  return ret;
}

int ObWindowFunctionOp::NonAggrCellNthValue::eval(RowsReader &row_reader,
                                                  const int64_t row_idx,
                                                  const ObRADatumStore::StoredRow &row,
                                                  const Frame &frame,
                                                  ObDatum &val)
{
  UNUSED(row_idx);
  UNUSED(row);
  int ret = OB_SUCCESS;
  const ObExprPtrIArray &params = wf_info_.param_exprs_;
  int64_t nth_val = 0;
  bool is_null = false;
  if (OB_UNLIKELY(params.count() != 2)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid number of params", K(params.count()), K(ret));
  } else if (OB_FAIL(ObWindowFunctionOp::get_param_int_value(*params.at(1),
      op_.eval_ctx_, is_null, nth_val, false, lib::is_mysql_mode()))) {
    if (ret == OB_ERR_WINDOW_FRAME_ILLEGAL) {
      if (is_null) {
        ret = OB_SUCCESS;
        val.set_null();
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("Incorrect arguments to nth_value", K(ret));
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "nth_value");
      }
    } else {
      LOG_WARN("get_param_int_value failed", K(ret));
    }
  } else if (OB_UNLIKELY(lib::is_oracle_mode() && (is_null || nth_val <= 0))) {
    ret = OB_DATA_OUT_OF_RANGE;
    LOG_WARN("invalid argument", K(ret), K(is_null), K(nth_val));
  } else if (OB_UNLIKELY(lib::is_mysql_mode() &&
                         (!params.at(1)->obj_meta_.is_integer_type() || nth_val == 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments to nth_value", K(ret), K(nth_val), K(params.at(1)->obj_meta_));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "nth_value");
  } else {
    const bool is_ignore_null = wf_info_.is_ignore_null_;
    const bool is_from_first = wf_info_.is_from_first_;

    int64_t k = 0;
    bool is_calc_nth = false;
    ObDatum *tmp_result = NULL;
    for (int64_t i = is_from_first ? frame.head_ : frame.tail_;
         OB_SUCC(ret) && (is_from_first ? (i <= frame.tail_) : (i >= frame.head_));
         is_from_first ? ++i : --i) {
      const ObRADatumStore::StoredRow* a_row = NULL;
      tmp_result = NULL;
      if (OB_FAIL(row_reader.get_row(i, a_row))) {
        LOG_WARN("failed to get row", K(ret), K(i));
      } else if (FALSE_IT(op_.clear_evaluated_flag())) {
      } else if (OB_FAIL(a_row->to_expr(op_.get_all_expr(), op_.eval_ctx_))) {
        LOG_WARN("Failed to to_expr", K(ret));
      } else if (OB_FAIL(params.at(0)->eval(op_.eval_ctx_, tmp_result))) {
        LOG_WARN("fail to calc result row", K(ret));
      } else if ((!tmp_result->is_null() || !is_ignore_null) && ++k == nth_val) {
        is_calc_nth = true;
        break;
      }
    }
    if (OB_SUCC(ret)) {
      if (tmp_result != NULL) {
        val = *tmp_result;
      }
      if (!is_calc_nth) {
        val.set_null();
      }
    }
  }
  return ret;
}

int ObWindowFunctionOp::NonAggrCellLeadOrLag::eval(RowsReader &row_reader,
                                                const int64_t row_idx,
                                                const ObRADatumStore::StoredRow &row,
                                                const Frame &frame,
                                                ObDatum &val)
{
  int ret = OB_SUCCESS;
  UNUSED(row_reader);
  UNUSED(row_idx);
  UNUSED(row);
  UNUSED(frame);
  UNUSED(val);
  const ObExprPtrIArray &params = wf_info_.param_exprs_;
  // LEAD provides access to a row at a given physical offset beyond that position
  // while LAG provides access to a row at a given physical offset prior to that position.
  const bool is_lead = T_WIN_FUN_LEAD == wf_info_.func_type_;
  int lead_lag_offset_direction = is_lead ? +1 : -1;
  // 0 -> value_expr 1 -> offset 2 -> default value
  ObDatum lead_lag_params[3];
  enum LeadLagParamType {
    VALUE_EXPR = 0,
    OFFSET = 1,
    DEFAULT_VALUE = 2,
    NUM_LEAD_LAG_PARAMS
  };
  // if not specified, the default offset is 1.
  bool is_lead_lag_offset_used = false;
//  lead_lag_params[OFFSET].set_int(1);

  // if not specified, the default value is NULL.
   lead_lag_params[DEFAULT_VALUE].set_null();

  if (OB_UNLIKELY(params.count() > NUM_LEAD_LAG_PARAMS || params.count() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid number of params", K(ret));
  } else {
    for (int64_t j = 0; OB_SUCC(ret) && j < params.count(); ++j) {
      ObDatum *result = NULL;
      if (OB_ISNULL(params.at(j))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid param", K(ret));
      } else if (OB_FAIL(params.at(j)->eval(op_.eval_ctx_, result))) {
        LOG_WARN("fail to calc result row", K(ret));
      } else if (j == DEFAULT_VALUE && !result->is_null()) {
        char *buf = NULL;
        int64_t buf_size = result->get_deep_copy_size();
        int64_t pos = 0;
        if (OB_ISNULL(buf = wf_info_.expr_->get_str_res_mem(op_.eval_ctx_, buf_size))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to alloc", K(buf), K(ret));
        } else if (OB_FAIL(lead_lag_params[j].deep_copy(*result, buf, buf_size, pos))) {
          LOG_WARN("failed to deep copy datum", K(ret));
        }
      } else {
        lead_lag_params[j] = *result;
        is_lead_lag_offset_used |= (j == OFFSET);
      }
    }
    int64_t offset = 0;
    if (OB_SUCC(ret)) {
      if (is_lead_lag_offset_used) {
        bool is_null = false;
        if (OB_FAIL(ObWindowFunctionOp::get_param_int_value(*params.at(OFFSET),
            op_.eval_ctx_, is_null, offset))) {
          LOG_WARN("get_param_int_value failed", K(ret));
        } else if (OB_UNLIKELY(is_null || offset < 0 ||
                            (lib::is_oracle_mode() && wf_info_.is_ignore_null_ && offset == 0))) {
          ret = OB_ERR_ARGUMENT_OUT_OF_RANGE;
          if (!is_null) {
            LOG_USER_ERROR(OB_ERR_ARGUMENT_OUT_OF_RANGE, offset);
          }
          LOG_WARN("lead/lag argument is out of range", K(ret), K(is_null), K(offset));
        }
      } else {
        offset = 1;
      }
    }

    if (OB_SUCC(ret)) {
      int64_t step = 0;
      bool found = false;
      for (int64_t j = row_idx;
           OB_SUCC(ret) && !found && j >= frame.head_ && j <= frame.tail_;
           j += lead_lag_offset_direction) {
        const ObRADatumStore::StoredRow *a_row = NULL;
        ObDatum *tmp_result = NULL;
        if (OB_FAIL(row_reader.get_row(j, a_row))) {
          LOG_WARN("failed to get row", K(ret), K(j));
        } else if (FALSE_IT(op_.clear_evaluated_flag())) {
        } else if (OB_FAIL(a_row->to_expr(op_.get_all_expr(), op_.eval_ctx_))) {
          LOG_WARN("Failed to to_expr", K(ret));
        } else if (OB_FAIL(params.at(0)->eval(op_.eval_ctx_, tmp_result))) {
          LOG_WARN("fail to calc result row", K(ret));
        } else {
          lead_lag_params[VALUE_EXPR] = *tmp_result;
          if (wf_info_.is_ignore_null_
              && tmp_result->is_null()) {
            //bug:
            //row_idx为null的时候，非ignore nulls下漏掉step++;
            step = (j == row_idx) ? step+1 : step;
          } else if (step++ == offset) {
            found = true;
            val = *tmp_result;
          }
        }
      }
      if (OB_SUCC(ret) && !found) {
        val = lead_lag_params[DEFAULT_VALUE];
      }
    }
  }
  return ret;
}

int ObWindowFunctionOp::NonAggrCellNtile::eval(RowsReader &row_reader,
                                            const int64_t row_idx,
                                            const ObRADatumStore::StoredRow &row,
                                            const Frame &frame,
                                            ObDatum &val)
{
  int ret = OB_SUCCESS;
  UNUSED(row_reader);
  UNUSED(row);
  UNUSED(row_reader);
  UNUSED(row_idx);
  UNUSED(row);
  UNUSED(frame);
  UNUSED(val);
  /**
   * let total represent total rows in partition
   * let part_row_idx represent row_idx in partition (0, 1, 2 ...)
   * let x = total / bucket_num, y = total % bucket_num
   * so total = xb + y = xb + xy - xy + y = (x+1)y + x(b-y)
   * it means there are y buckets which contain (x+1) elements
   * there are (b-y) buckets which contain x elements
   *    total 5 elements divide into 3 bucket
   *    5/3=1..2, 1st bucket contains two elements and 2nd and 3rd bucket contain one element
   *    ------------------------
   *    | 1,1   | 2,2   | 3    |
   *    ------------------------
   * if (x == 0) { //not each bucket has one element
   *   result = part_row_idx + 1
   * } else {
   *   if (part_row_idx < y * (x + 1))
   *     result = part_row_idx / (x + 1) + 1
   *   else
   *     result = (part_row_idx - (y * (x + 1))) / x + y + 1
   * }
   */
  ObExpr *param = NULL;
  int64_t bucket_num = 0;
  const ObExprPtrIArray &params = wf_info_.param_exprs_;
  bool is_null = false;
  if (OB_UNLIKELY(params.count() != 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("The number of arguments of NTILE should be 1", K(params.count()), K(ret));
  } else if (OB_ISNULL(param = params.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("argument is NULL", K(ret));
  } else if (OB_FAIL(ObWindowFunctionOp::get_param_int_value(*param,
      op_.eval_ctx_, is_null, bucket_num, false, lib::is_mysql_mode()))) {
    if (ret == OB_ERR_WINDOW_FRAME_ILLEGAL) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Incorrect arguments to ntile", K(ret));
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ntile");
    } else {
      LOG_WARN("get_param_int_value failed", K(ret));
    }
  } else if (is_null) {
    // return NULL when bucket_num is NULL
    val.set_null();
  } else if (!is_oracle_mode()
             && !param->obj_meta_.is_numeric_type()) {
    ret = OB_DATA_OUT_OF_RANGE;
    LOG_WARN("invalid argument", K(ret), K(param->obj_meta_));
  } else if (OB_UNLIKELY(bucket_num <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("bucket_num is invalid", K(ret), K(bucket_num));
  } else {
    const int64_t total = frame.tail_ - frame.head_ + 1;
    const int64_t x = total / bucket_num;
    const int64_t y = total % bucket_num;
    const int64_t f_row_idx = row_idx - frame.head_;
    int64_t result = 0;
    LOG_DEBUG("print ntile param", K(total), K(x), K(y), K(f_row_idx));
    if (0 == x) {
      result = f_row_idx + 1;
    } else {
      if (f_row_idx < ( y * (x + 1))) {
        result = f_row_idx / (x + 1) + 1;
      } else {
        result = (f_row_idx - ( y * (x + 1))) / x + y + 1;
      }
    }
    ObDatum &expr_datum = wf_info_.expr_->locate_datum_for_write(op_.eval_ctx_);
    if (ObNumberType == wf_info_.expr_->datum_meta_.type_) {
      ObNumStackOnceAlloc tmp_alloc;
      number::ObNumber result_num;
      if (OB_FAIL(result_num.from(result, tmp_alloc))) {
        LOG_WARN("number from int failed", K(ret));
      } else {
        expr_datum.set_number(result_num);
      }
    } else {
      expr_datum.set_int(result);
    }
    wf_info_.expr_->set_evaluated_flag(op_.eval_ctx_);
    val = expr_datum;
    LOG_DEBUG("ntile print result", K(result), K(bucket_num));
  }
  return ret;
}

int ObWindowFunctionOp::NonAggrCellRankLike::eval(RowsReader &row_reader,
                                               const int64_t row_idx,
                                               const ObRADatumStore::StoredRow &row,
                                               const Frame &frame,
                                               ObDatum &val)
{
  int ret = OB_SUCCESS;
  UNUSED(row_reader);
  UNUSED(row_idx);
  UNUSED(row);
  UNUSED(frame);
  UNUSED(val);
  bool equal_with_prev_row = false;
  if (row_idx != frame.head_) {
    equal_with_prev_row = true;
    ExprFixedArray &sort_cols = wf_info_.sort_exprs_;
    ObSortCollations &sort_collations = wf_info_.sort_collations_;
    ObSortFuncs &sort_cmp_funcs = wf_info_.sort_cmp_funcs_;
    const ObRADatumStore::StoredRow *tmp_row = NULL;
    if (OB_FAIL(row_reader.get_row(row_idx - 1, tmp_row))) {
      LOG_WARN("failed to get row", K(ret), K(row_idx));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < sort_cols.count(); ++i) {
        ObDatumCmpFuncType cmp_func = sort_cmp_funcs.at(i).cmp_func_;
        const int64_t idx = sort_collations.at(i).field_idx_;
        const ObDatum &l_datum = tmp_row->cells()[idx];
        const ObDatum &r_datum = row.cells()[idx];
        int match = 0;
        if (OB_FAIL(cmp_func(l_datum, r_datum, match))) {
          LOG_WARN("cmp failed", K(ret), K(idx), K(l_datum), K(r_datum), K(match));
        } else {
          LOG_DEBUG("cmp ", K(idx), K(l_datum), K(r_datum), K(match));
          if (0 != match) {
            equal_with_prev_row = false;
            break;
          }
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    int64_t rank = -1;
    if (equal_with_prev_row) {
      rank = rank_of_prev_row_;
    } else if (T_WIN_FUN_RANK == wf_info_.func_type_ ||
               T_WIN_FUN_PERCENT_RANK == wf_info_.func_type_) {
      rank = row_idx - frame.head_ + 1;
    } else if (T_WIN_FUN_DENSE_RANK == wf_info_.func_type_) {
      // dense_rank
      rank = rank_of_prev_row_ + 1;
    }
    ObDatum &expr_datum = wf_info_.expr_->locate_datum_for_write(op_.eval_ctx_);
    wf_info_.expr_->set_evaluated_flag(op_.eval_ctx_);
    if (T_WIN_FUN_PERCENT_RANK == wf_info_.func_type_) {
      if (ob_is_number_tc(wf_info_.expr_->datum_meta_.type_)) {
        //in mysql mode, percent rank may return double
        if (0 == frame.tail_ - frame.head_) {
          number::ObNumber res_nmb;
          res_nmb.set_zero();
          expr_datum.set_number(res_nmb);
        } else {
          number::ObNumber numerator;
          number::ObNumber denominator;
          number::ObNumber res_nmb;
          ObNumStackAllocator<3> tmp_alloc;
          if (OB_FAIL(numerator.from(rank - 1, tmp_alloc))) {
            LOG_WARN("failed to build number from int64_t", K(ret));
          } else if (OB_FAIL(denominator.from(frame.tail_ - frame.head_, tmp_alloc))) {
            LOG_WARN("failed to build number from int64_t", K(ret));
          } else if (OB_FAIL(numerator.div(denominator, res_nmb, tmp_alloc))) {
            LOG_WARN("failed to div number", K(ret));
          } else {
            expr_datum.set_number(res_nmb);
          }
        }
      } else if (ObDoubleType == wf_info_.expr_->datum_meta_.type_) {
        if (0 == frame.tail_ - frame.head_) {
          expr_datum.set_double(0);
        } else {
          double numerator = static_cast<double>(rank - 1);
          double denominator= static_cast<double>(frame.tail_ - frame.head_);
          expr_datum.set_double(numerator / denominator);
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the result type of window function is unexpected", K(ret), K(wf_info_.expr_->datum_meta_));
      }
    } else if (lib::is_oracle_mode()) {
      number::ObNumber res_nmb;
      ObNumStackAllocator<3> tmp_alloc;
      if (OB_FAIL(res_nmb.from(rank, tmp_alloc))) {
        LOG_WARN("failed to build number from int64_t", K(ret));
      } else {
        expr_datum.set_number(res_nmb);
      }
    } else {
      expr_datum.set_int(rank);
    }

    if (OB_SUCC(ret)) {
      rank_of_prev_row_ = rank;
      val = static_cast<ObDatum &>(expr_datum);
    }
  }
  return ret;
}

DEF_TO_STRING(ObWindowFunctionOp::NonAggrCellRankLike)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_NAME("wf_cell");
  J_COLON();
  pos += ObWindowFunctionOp::WinFuncCell::to_string(buf + pos, buf_len - pos);
  J_COMMA();
  J_KV(K_(rank_of_prev_row));
  J_OBJ_END();
  return pos;
}

int ObWindowFunctionOp::NonAggrCellCumeDist::eval(RowsReader &row_reader,
                                               const int64_t row_idx,
                                               const ObRADatumStore::StoredRow &row,
                                               const Frame &frame,
                                               ObDatum &val)
{
  int ret = OB_SUCCESS;
  UNUSED(row_reader);
  UNUSED(row_idx);
  UNUSED(row);
  UNUSED(frame);
  UNUSED(val);
  int64_t same_idx = row_idx;
  bool should_continue = true;
  const ObRADatumStore::StoredRow &ref_row = row;
  ExprFixedArray &sort_cols = wf_info_.sort_exprs_;
  ObSortCollations &sort_collations = wf_info_.sort_collations_;
  ObSortFuncs &sort_cmp_funcs = wf_info_.sort_cmp_funcs_;
  while (should_continue && OB_SUCC(ret) && same_idx < frame.tail_) {
    const ObRADatumStore::StoredRow *a_row = NULL;
    if (OB_FAIL(row_reader.get_row(same_idx + 1, a_row))) {
      LOG_WARN("fail to get row", K(ret), K(same_idx));
    } else {
      const ObRADatumStore::StoredRow &iter_row = *a_row;
      for (int64_t i = 0; should_continue && OB_SUCC(ret) && i < sort_cols.count(); i++) {
        ObDatumCmpFuncType cmp_func = sort_cmp_funcs.at(i).cmp_func_;
        const int64_t idx = sort_collations.at(i).field_idx_;
        const ObDatum &l_datum = ref_row.cells()[idx];
        const ObDatum &r_datum = iter_row.cells()[idx];
        int match = 0;
        if (OB_FAIL(cmp_func(l_datum, r_datum, match))) {
          LOG_WARN("cmp failed", K(ret), K(idx), K(l_datum), K(r_datum), K(match));
        } else {
          LOG_DEBUG("cmp ", K(idx), K(l_datum), K(r_datum), K(match));
          if (0 != match) {
            should_continue = false;
          }
        }
      }
      if (OB_SUCC(ret) && should_continue) {
        ++same_idx;
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (ob_is_number_tc(wf_info_.expr_->datum_meta_.type_)) {
      // number of row[cur] >= row[:] (whether `>=` or other is depend on ORDER BY)
      number::ObNumber numerator;
      // total tuple of current window
      number::ObNumber denominator;
      // result number
      number::ObNumber res_nmb;
      ObNumStackAllocator<3> tmp_alloc;
      if (OB_FAIL(numerator.from(same_idx - frame.head_ + 1, tmp_alloc))) {
        LOG_WARN("failed to build number from int64_t", K(ret));
      } else if (OB_FAIL(denominator.from(frame.tail_ - frame.head_ + 1, tmp_alloc))) {
        LOG_WARN("failed to build number from int64_t", K(ret));
      } else if (OB_FAIL(numerator.div(denominator, res_nmb, tmp_alloc))) {
        LOG_WARN("failed to div number", K(ret));
      } else {
        ObDatum &expr_datum = wf_info_.expr_->locate_datum_for_write(op_.eval_ctx_);
        wf_info_.expr_->set_evaluated_flag(op_.eval_ctx_);
        expr_datum.set_number(res_nmb);
        val = static_cast<ObDatum &>(expr_datum);
      }
    } else if (ObDoubleType == wf_info_.expr_->datum_meta_.type_) {
      // number of row[cur] >= row[:] (whether `>=` or other is depend on ORDER BY)
      double numerator;
      // total tuple of current window
      double denominator;
      numerator = static_cast<double>(same_idx - frame.head_ + 1);
      denominator = static_cast<double>(frame.tail_ - frame.head_ + 1);
      ObDatum &expr_datum = wf_info_.expr_->locate_datum_for_write(op_.eval_ctx_);
      wf_info_.expr_->set_evaluated_flag(op_.eval_ctx_);
      expr_datum.set_double(numerator / denominator);
      val = static_cast<ObDatum &>(expr_datum);
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the result type of window function is unexpected", K(ret), K(wf_info_.expr_->datum_meta_));
    }
  }

  return ret;
}

int ObWindowFunctionOp::check_same_partition(WinFuncCell &cell, bool &same)
{
  int ret = OB_SUCCESS;
  int cmp_ret = 0;
  same = true;
  const auto &exprs = cell.wf_info_.partition_exprs_;
  if (!exprs.empty()) {
    if (NULL == cell.part_values_.store_row_
        || cell.part_values_.store_row_->cnt_ != exprs.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("current partition value not saved or cell count mismatch",
               K(ret), K(cell.part_values_));
    } else {
      ObDatum *val = NULL;
      for (int64_t i = 0; OB_SUCC(ret) && same && i < exprs.count(); i++) {
        if (OB_FAIL(exprs.at(i)->eval(eval_ctx_, val))) {
          LOG_WARN("expression evaluate failed", K(ret));
        } else if (OB_FAIL(exprs.at(i)->basic_funcs_->null_first_cmp_(*val,
                           cell.part_values_.store_row_->cells()[i], cmp_ret))) {
          LOG_WARN("compare failed", K(ret));
        } else if (0 != cmp_ret) {
          same = false;
        }
      }
    }
  }
  return ret;
}

// todo: concern boundary
bool ObWindowFunctionOp::Frame::need_restart_aggr(const bool can_inv,
                                                  const Frame &last_valid_frame,
                                                  const Frame &new_frame,
                                                  RemovalInfo &removal_info,
                                                  const uint64_t &remove_type)
{
  bool need = false;
  if (-1 == last_valid_frame.head_ || -1 == last_valid_frame.tail_) {
    need = true;
  } else {
    const int64_t inc_cost =
      std::abs(last_valid_frame.head_ - new_frame.head_) +
      std::abs(last_valid_frame.tail_ - new_frame.tail_);
    const int64_t restart_cost = new_frame.tail_ - new_frame.head_;
    if (inc_cost > restart_cost) {
      need = true;
    } else if (!can_inv) {
      // has sliding-out row
      if (new_frame.head_ > last_valid_frame.head_
          || new_frame.tail_ < last_valid_frame.tail_) {
        need = true;
      }
    } else if (common::REMOVE_EXTRENUM == remove_type) {
      // max_min index miss from calculation range
      if (removal_info.max_min_index_ < new_frame.head_
          || removal_info.max_min_index_ > new_frame.tail_) {
        need = true;
      }
    }
  }
  return need;
}

bool ObWindowFunctionOp::Frame::valid_frame(const Frame &part_frame,
                                            const Frame &frame)
{
  return frame.head_ <= frame.tail_ && frame.head_ <= part_frame.tail_ &&
    frame.tail_ >= part_frame.head_;
}

bool ObWindowFunctionOp::Frame::same_frame(const Frame &left,
                                         const Frame &right)
{
  return left.head_ == right.head_ && left.tail_ == right.tail_;
}

void ObWindowFunctionOp::Frame::prune_frame(const Frame &part_frame,
                                            Frame &frame)
{
  // it's caller's responsibility for invoking valid_frame() first
  if (frame.head_ < part_frame.head_) {
    frame.head_ = part_frame.head_;
  }
  if (frame.tail_ > part_frame.tail_) {
    frame.tail_ = part_frame.tail_;
  }
}

template<class FuncType>
int ObWindowFunctionOp::FuncAllocer::alloc(WinFuncCell *&return_func,
    WinFuncInfo &wf_info, ObWindowFunctionOp &op, const int64_t tenant_id)
{
  int ret = OB_SUCCESS;
  void *tmp_ptr = local_allocator_->alloc(sizeof(FuncType));
  if (OB_ISNULL(tmp_ptr)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    return_func = new (tmp_ptr) FuncType(wf_info, op);
    ret = return_func->reset_res(tenant_id);
  }
  return ret;
}

int ObWindowFunctionOp::init()
{
  int ret = OB_SUCCESS;
  ObWindowFunctionOpInput *op_input = static_cast<ObWindowFunctionOpInput*>(input_);
  if (OB_UNLIKELY(!wf_list_.is_empty())) {
    ret = OB_INIT_TWICE;
    LOG_WARN("wf_list_ is inited", K(ret));
  } else if (OB_ISNULL(ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(ret));
  } else {
    const uint64_t tenant_id = ctx_.get_my_session()->get_effective_tenant_id();
    local_allocator_.set_tenant_id(tenant_id);
    local_allocator_.set_label(ObModIds::OB_SQL_WINDOW_LOCAL);
    local_allocator_.set_ctx_id(ObCtxIds::WORK_AREA);
    rescan_alloc_.set_tenant_id(tenant_id);
    rescan_alloc_.set_label("WfRescanAlloc");
    patch_alloc_.set_tenant_id(tenant_id);
    patch_alloc_.set_label("WfPatchAlloc");
    ObMemAttr attr(tenant_id, "WfArray");
    participator_whole_msg_array_.set_attr(attr);
    pby_hash_values_.set_attr(attr);
    pby_hash_values_sets_.set_attr(attr);
    pby_expr_cnt_idx_array_.set_attr(attr);
    FuncAllocer func_alloc;
    func_alloc.local_allocator_ = &local_allocator_;
    int64_t prev_pushdown_pby_col_count = -1;
    WFInfoFixedArray &wf_infos = *const_cast<WFInfoFixedArray *>(&MY_SPEC.wf_infos_);
    if (OB_FAIL(ObChunkStoreUtil::alloc_dir_id(dir_id_))) {
      LOG_WARN("failed to alloc dir id", K(ret));
    } else if (OB_FAIL(curr_row_collect_values_.prepare_allocate(wf_infos.count()))) {
      LOG_WARN("cur row collect values prepare allocate failed", K(ret));
    } else if (MY_SPEC.is_vectorized()) {
      ObExprPtrIArray &all_exprs = get_all_expr();
      if (OB_FAIL(all_expr_datums_copy_.init(all_exprs.count()))) {
        LOG_WARN("all expr datums copy prepare allocate failed", K(ret));
      } else if (OB_FAIL(all_expr_datums_.init(all_exprs.count()))) {

      }
      int64_t datums_size = MY_SPEC.max_batch_size_ * sizeof(ObDatum);
      for (int64_t i = 0; i < all_exprs.count() && OB_SUCC(ret); i++) {
        ObDatum *datums_buf = NULL;
        ObDatum *expr_datum = all_exprs.at(i)->locate_batch_datums(eval_ctx_);
        if (OB_ISNULL(datums_buf = static_cast<ObDatum *>(local_allocator_.alloc(datums_size)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate memory failed", K(ret), K(datums_size), K(i));
        } else if (OB_FAIL(all_expr_datums_copy_.push_back(datums_buf))) {
          LOG_WARN("push back failed", K(ret));
        } else if (OB_FAIL(all_expr_datums_.push_back(expr_datum))) {
          LOG_WARN("push back failed", K(ret));
        } else {
          LOG_DEBUG("finish init all expr datum", K(datums_size), K(all_expr_datums_copy_.count()));
        }
      }
    }

    for (int64_t i = 0; i < wf_infos.count() && OB_SUCC(ret); ++i) {
      WinFuncInfo &wf_info = wf_infos.at(i);
      WinFuncCell *wf_cell = NULL;

      if (OB_SUCC(ret)) {
        switch(wf_info.func_type_) {
          case T_FUN_SUM:
          case T_FUN_MAX:
          case T_FUN_MIN:
          case T_FUN_COUNT:
          case T_FUN_AVG:
          case T_FUN_MEDIAN:
          case T_FUN_GROUP_PERCENTILE_CONT:
          case T_FUN_GROUP_PERCENTILE_DISC:
          case T_FUN_STDDEV:
          case T_FUN_STDDEV_SAMP:
          case T_FUN_VARIANCE:
          case T_FUN_STDDEV_POP:
          case T_FUN_APPROX_COUNT_DISTINCT:
          case T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS:
          case T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS_MERGE:
          case T_FUN_GROUP_CONCAT:
          case T_FUN_CORR:
          case T_FUN_COVAR_POP:
          case T_FUN_COVAR_SAMP:
          case T_FUN_VAR_POP:
          case T_FUN_VAR_SAMP:
          case T_FUN_REGR_SLOPE:
          case T_FUN_REGR_INTERCEPT:
          case T_FUN_REGR_COUNT:
          case T_FUN_REGR_R2:
          case T_FUN_REGR_AVGX:
          case T_FUN_REGR_AVGY:
          case T_FUN_REGR_SXX:
          case T_FUN_REGR_SYY:
          case T_FUN_REGR_SXY:
          case T_FUN_SYS_BIT_AND:
          case T_FUN_SYS_BIT_OR:
          case T_FUN_SYS_BIT_XOR:
          case T_FUN_KEEP_MAX:
          case T_FUN_KEEP_MIN:
          case T_FUN_KEEP_SUM:
          case T_FUN_KEEP_COUNT:
          case T_FUN_KEEP_WM_CONCAT:
          case T_FUN_WM_CONCAT:
          case T_FUN_TOP_FRE_HIST:
          case T_FUN_PL_AGG_UDF:
          case T_FUN_JSON_ARRAYAGG:
          case T_FUN_JSON_OBJECTAGG:
          case T_FUN_ORA_JSON_ARRAYAGG:
          case T_FUN_ORA_JSON_OBJECTAGG:
          case T_FUN_ORA_XMLAGG:
          case T_FUN_SYS_ST_ASMVT:
          case T_FUN_SYS_RB_BUILD_AGG:
          case T_FUN_SYS_RB_OR_AGG:
          case T_FUN_SYS_RB_AND_AGG: {
            void *tmp_ptr = local_allocator_.alloc(sizeof(AggrCell));
            void *tmp_array = local_allocator_.alloc(sizeof(AggrInfoFixedArray));
            ObIArray<ObAggrInfo> *aggr_infos = NULL;
            if (OB_ISNULL(tmp_ptr) || OB_ISNULL(tmp_array)) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("failed to alloc", KP(tmp_ptr), KP(tmp_array), K(ret));
            } else if (FALSE_IT(aggr_infos = new (tmp_array) AggrInfoFixedArray(local_allocator_,
                                                                                1))) {
            } else if (OB_FAIL(aggr_infos->push_back(wf_info.aggr_info_))) {
              LOG_WARN("failed to push_back", K(wf_info.aggr_info_), K(ret));
            } else {
              AggrCell *aggr_func = new (tmp_ptr) AggrCell(wf_info, *this, *aggr_infos, tenant_id);
              aggr_func->aggr_processor_.set_in_window_func();
              if (OB_FAIL(aggr_func->aggr_processor_.init())) {
                LOG_WARN("failed to initialize init_group_rows", K(ret));
                aggr_func->~AggrCell();
                aggr_func = NULL;
              } else {
                aggr_func->aggr_processor_.set_dir_id(dir_id_);
                aggr_func->aggr_processor_.set_io_event_observer(&io_event_observer_);
                wf_cell = aggr_func;
              }
            }
            break;
          }
          case T_WIN_FUN_RANK:
          case T_WIN_FUN_DENSE_RANK:
          case T_WIN_FUN_PERCENT_RANK: {
            ret = func_alloc.alloc<NonAggrCellRankLike>(wf_cell, wf_info, *this, tenant_id);
            break;
          }
          case T_WIN_FUN_CUME_DIST: {
            ret = func_alloc.alloc<NonAggrCellCumeDist>(wf_cell, wf_info, *this, tenant_id);
            break;
          }
          case T_WIN_FUN_ROW_NUMBER: {
            ret = func_alloc.alloc<NonAggrCellRowNumber>(wf_cell, wf_info, *this, tenant_id);
            break;
          }
          // first_value && last_value has been converted to nth_value when resolving
          //case T_WIN_FUN_FIRST_VALUE:
          //case T_WIN_FUN_LAST_VALUE:
          case T_WIN_FUN_NTH_VALUE: {
            ret = func_alloc.alloc<NonAggrCellNthValue>(wf_cell, wf_info, *this, tenant_id);
            break;
          }
          case T_WIN_FUN_LEAD:
          case T_WIN_FUN_LAG: {
            ret = func_alloc.alloc<NonAggrCellLeadOrLag>(wf_cell, wf_info, *this, tenant_id);
            break;
          }
          case T_WIN_FUN_NTILE: {
            ret = func_alloc.alloc<NonAggrCellNtile>(wf_cell, wf_info, *this, tenant_id);
            break;
          }
          default: {
            ret = OB_NOT_SUPPORTED;
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "this window function");
            LOG_WARN("unsupported function", K(wf_info.func_type_), K(ret));
            break;
          }
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_ISNULL(wf_cell)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("wf_cell is null", K(wf_info), K(ret));
        } else {
          wf_cell->wf_idx_ = i + 1;
          if (OB_UNLIKELY(!wf_list_.add_last(wf_cell))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("add func failed", K(ret));
          } else {
            LOG_DEBUG("add func succ", KPC(wf_cell));
            wf_cell = NULL;
          }
        }
      }
      if (OB_UNLIKELY(NULL != wf_cell)) {
        wf_cell->~WinFuncCell();
        wf_cell = NULL;
      }
      if (OB_SUCC(ret) && MY_SPEC.is_participator()) {
        if (wf_info.can_push_down_) {
          if (common::OB_INVALID_COUNT == next_wf_pby_expr_cnt_to_transmit_) {
            // next_wf_pby_expr_cnt_to_transmit_ is for pushdown transmit to datahub
            next_wf_pby_expr_cnt_to_transmit_ = wf_info.partition_exprs_.count();
          }
          if (wf_info.partition_exprs_.count() != prev_pushdown_pby_col_count) {
            ++pby_set_count_;
            prev_pushdown_pby_col_count = wf_info.partition_exprs_.count();
          }
        }
      }
    } // end for
    if (OB_SUCC(ret) && MY_SPEC.is_participator()) {
      if (OB_FAIL(build_pby_hash_values_for_transmit())) {
        LOG_WARN("fail to build_pby_hash_values_for_transmit", K(ret));
      } else if (OB_FAIL(build_participator_whole_msg_array())) {
        LOG_WARN("fail to build_participator_whole_msg_array", K(ret));
      } else {
        prev_pushdown_pby_col_count = -1;
        int64_t idx = OB_INVALID_ID;
        for (int64_t i = 0; OB_SUCC(ret) && i < wf_infos.count(); ++i) {
          WinFuncInfo &wf_info = wf_infos.at(i);
          if (!wf_info.can_push_down_) {
            if (OB_FAIL(pby_expr_cnt_idx_array_.push_back(OB_INVALID_ID))) {
              LOG_WARN("push_back to pby_expr_cnt_idx_array_ failed", K(ret));
            }
          } else {
            if (wf_info.partition_exprs_.count() == prev_pushdown_pby_col_count) {
              if (OB_FAIL(pby_expr_cnt_idx_array_.push_back(idx))) {
                LOG_WARN("push_back to pby_expr_cnt_idx_array_ failed", K(ret));
              }
            } else {
              prev_pushdown_pby_col_count = wf_info.partition_exprs_.count();
              if (OB_FAIL(pby_expr_cnt_idx_array_.push_back(++idx))) {
                LOG_WARN("push_back to pby_expr_cnt_idx_array_ failed", K(ret));
              } else {
                ReportingWFHashSet *hash_set = OB_NEWx(ReportingWFHashSet, (&local_allocator_));
                if (OB_ISNULL(hash_set)) {
                  ret = OB_ALLOCATE_MEMORY_FAILED;
                  LOG_WARN("hash_set is null, allocate memory failed", K(ret));
                } else if (OB_FAIL(static_cast<ReportingWFHashSet *>(hash_set)->create( // dop * dop
                         op_input->get_total_task_count() * op_input->get_total_task_count()))) {
                  LOG_WARN("row set init failed", K(ret), K(op_input->get_total_task_count()));
                } else if (OB_FAIL(pby_hash_values_sets_.push_back(hash_set))) {
                  LOG_WARN("push_back to pby_hash_values_sets_ failed", K(ret));
                }
              }
            }
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      ret = foreach_stores([&](Stores &s) { return create_stores(s); });
      if (OB_FAIL(ret)) {
        LOG_WARN("create stores failed", K(ret));
      }
    }
  }
  return ret;
}

int ObWindowFunctionOp::build_pby_hash_values_for_transmit()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < pby_set_count_; ++i) {
    PbyHashValueArray *hash_value_array = OB_NEWx(PbyHashValueArray, (&local_allocator_));
    if (OB_ISNULL(hash_value_array)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc", K(ret));
    } else if (OB_FAIL(pby_hash_values_.push_back(hash_value_array))) {
      LOG_WARN("pushback hash_value_array to pushdown_pby_hash_values_for_transmit failed", K(ret));
    }
  }
  return ret;
}

int ObWindowFunctionOp::build_participator_whole_msg_array()
{
  int ret = OB_SUCCESS;
  int64_t tenant_id = ctx_.get_my_session()->get_effective_tenant_id();
  bool enable_dump = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < pby_set_count_; ++i) {
    ObReportingWFWholeMsg *whole_msg = OB_NEWx(ObReportingWFWholeMsg, (&local_allocator_));
    if (OB_ISNULL(whole_msg)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate whole_msg mem failed", K(ret));
    } else if (OB_FAIL(participator_whole_msg_array_.push_back(whole_msg))) {
      LOG_WARN("pushback whole_msg to participator_whole_msg_array_ failed", K(ret));
    }
  }
  return ret;
}

int ObWindowFunctionOp::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOperator::inner_open())) {
    LOG_WARN("inner_open child operator failed", K(ret));
  } else if (OB_ISNULL(ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(ret));
  } else if (OB_FAIL(next_row_.init(local_allocator_, child_->get_spec().output_.count()))) {
    LOG_WARN("init shadow copy row failed", K(ret));
  } else if (OB_FAIL(init())) {
    LOG_WARN("init failed", K(ret));
  } else if (OB_FAIL(reset_for_scan(ctx_.get_my_session()->get_effective_tenant_id()))) {
    LOG_WARN("reset_for_scan failed", K(ret));
  }
  LOG_DEBUG("window function inner open", K(MY_SPEC.single_part_parallel_));
  return ret;
}

int ObWindowFunctionOp::inner_rescan()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOperator::inner_rescan())) {
    LOG_WARN("rescan child operator failed", K(ret));
  } else if (OB_ISNULL(ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(ret));
  } else if (OB_FAIL(reset_for_scan(ctx_.get_my_session()->get_effective_tenant_id()))) {
    LOG_WARN("reset_for_scan failed", K(ret));
  }
  stat_ = ProcessStatus::PARTIAL;
  restore_row_cnt_ = 0;
  iter_end_ = false;
  first_part_saved_ = false;
  last_part_saved_ = false;
  rescan_alloc_.reset();
  rd_patch_ = NULL;

  patch_alloc_.reset();
  first_part_outputed_ = false;
  patch_first_ = false;
  patch_last_ = false;
  first_row_same_order_cache_ = SAME_ORDER_CACHE_DEFAULT;
  last_row_same_order_cache_ = SAME_ORDER_CACHE_DEFAULT;
  last_computed_part_rows_ = 0;
  last_aggr_status_ = 0;

  next_wf_pby_expr_cnt_to_transmit_ =
      const_cast<WFInfoFixedArray *>(&MY_SPEC.wf_infos_)->at(0).partition_exprs_.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < pby_hash_values_.count(); ++i) {
    if (OB_ISNULL(pby_hash_values_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret), K(pby_hash_values_.count()), K(i));
    } else {
      pby_hash_values_.at(i)->reuse();
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < participator_whole_msg_array_.count(); ++i) {
    if (OB_ISNULL(participator_whole_msg_array_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret), K(participator_whole_msg_array_.count()), K(i));
    } else {
      participator_whole_msg_array_.at(i)->reset();
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < pby_hash_values_sets_.count(); ++i) {
    if (OB_ISNULL(pby_hash_values_sets_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret), K(pby_hash_values_sets_.count()), K(i));
    } else {
      pby_hash_values_sets_.at(i)->reuse();
    }
  }
  return ret;
}

int ObWindowFunctionOp::inner_close()
{
  curr_row_collect_values_.reset();
  input_rows_.foreach_store([](RowsStore *&s) { s->ra_rs_.reset(); return OB_SUCCESS; });
  all_expr_datums_copy_.reset();
  all_expr_datums_.reset();
  DLIST_FOREACH_NORET(func, wf_list_) {
    if (func != NULL) {
      func->~WinFuncCell();
    }
  }
  wf_list_.reset();
  pby_expr_cnt_idx_array_.reset();
  for (int64_t i = 0; i < pby_hash_values_.count(); ++i) {
    pby_hash_values_.at(i)->reset();
    local_allocator_.free(pby_hash_values_.at(i));
    pby_hash_values_.at(i) = NULL;
  }
  pby_hash_values_.reset();
  for (int64_t i = 0; i < participator_whole_msg_array_.count(); ++i) {
    participator_whole_msg_array_.at(i)->reset();
    local_allocator_.free(participator_whole_msg_array_.at(i));
    participator_whole_msg_array_.at(i) = NULL;
  }
  participator_whole_msg_array_.reset();
  for (int64_t i = 0; i < pby_hash_values_sets_.count(); ++i) {
    pby_hash_values_sets_.at(i)->destroy();
    local_allocator_.free(pby_hash_values_sets_.at(i));
    pby_hash_values_sets_.at(i) = NULL;
  }
  pby_hash_values_sets_.reset();
  return ObOperator::inner_close();
}

void ObWindowFunctionOp::destroy()
{
  input_rows_.~Stores();
  wf_list_.~WinFuncCellList();
  local_allocator_.reset();
  local_allocator_.~ObArenaAllocator();
  rescan_alloc_.~ObArenaAllocator();
  patch_alloc_.~ObArenaAllocator();
  ObOperator::destroy();
}

int ObWindowFunctionOp::fetch_child_row()
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  if (next_row_.is_saved()) {
    // restore datum ptr of child output
    ret = next_row_.restore(child_->get_spec().output_, eval_ctx_);
    next_row_.reuse();
  } else {
    ret = child_->get_next_row();
    if (OB_ITER_END == ret) {
      child_iter_end_ = true;
    }
  }
  return ret;
}

int ObWindowFunctionOp::input_one_row(WinFuncCell &wf_cell, bool &part_end)
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  bool is_same_part = false;
  if (OB_FAIL(fetch_child_row())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("child_op failed to get next row", K(ret));
    } else {
      part_end = true;
      ret = OB_SUCCESS;
    }
  } else if (OB_FAIL(check_same_partition(wf_cell, is_same_part))) {
    LOG_WARN("check same partition failed", K(ret));
  } else if (!is_same_part) {
    part_end = true;
    // backup datum ptr of child output
    if (OB_FAIL(next_row_.shadow_copy(child_->get_spec().output_, eval_ctx_))) {
      LOG_WARN("shadow copy row failed", K(ret));
    }
  } else if (OB_FAIL(input_rows_.cur_->add_row(get_all_expr(), &eval_ctx_))) {
    LOG_WARN("add row failed", K(get_all_expr()), K(ret));
  }
  return ret;
}

int ObWindowFunctionOp::create_row_store(RowsStore *&s)
{
  int ret = OB_SUCCESS;
  if (NULL == s) {
    s = OB_NEWx(RowsStore, (&local_allocator_));
    if (NULL == s) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    }
  }
  return ret;
}

int ObWindowFunctionOp::create_stores(Stores &s)
{
  int ret = OB_SUCCESS;
  OZ(create_row_store(s.cur_));
  if (MY_SPEC.is_vectorized()) {
    OZ(create_row_store(s.processed_));
  }
  if (MY_SPEC.range_dist_parallel_) {
    OZ(create_row_store(s.first_));
    OZ(create_row_store(s.last_));
  }
  return ret;
}

int ObWindowFunctionOp::set_it_age(Stores &s)
{
  int ret = OB_SUCCESS;
  if (MY_SPEC.is_vectorized()) {
    if (OB_ISNULL(s.cur_) || OB_ISNULL(s.processed_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("row store is null", K(ret));
    } else {
      s.cur_->ra_rs_.set_iteration_age(&output_rows_it_age_);
      s.processed_->ra_rs_.set_iteration_age(&output_rows_it_age_);
    }
  }
  return ret;
}

int ObWindowFunctionOp::unset_it_age(Stores &s)
{
  int ret = OB_SUCCESS;
  if (MY_SPEC.is_vectorized()) {
    if (OB_ISNULL(s.cur_) || OB_ISNULL(s.processed_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("row store is null", K(ret));
    } else {
      s.cur_->ra_rs_.set_iteration_age(NULL);
      s.processed_->ra_rs_.set_iteration_age(NULL);
    }
  }
  return ret;
}

int ObWindowFunctionOp::reset_for_scan(const int64_t tenant_id)
{
  int ret = OB_SUCCESS;
  last_output_row_idx_ = common::OB_INVALID_INDEX;
  next_row_.reuse();
  child_iter_end_ = false;
  ret = foreach_stores([&](Stores &s) { return s.reset(tenant_id);});
  return ret;
}

int ObWindowFunctionOp::reset_for_part_scan(const int64_t tenant_id)
{
  int ret = OB_SUCCESS;
  last_output_row_idx_ = OB_INVALID_INDEX;
  foreach_stores([&](Stores &s) { return s.cur_->reset(tenant_id); });
  LOG_DEBUG("finish reset_for_part_scan", K(ret));
  return ret;
}

int ObWindowFunctionOp::compute_push_down_by_pass(WinFuncCell &wf_cell, common::ObDatum &val)
{
  int ret = OB_SUCCESS;
  if (!wf_cell.is_aggr()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("wf_cell is not aggr", K(ret));
  } else {
    AggrCell *aggr_func = static_cast<AggrCell *>(&wf_cell);
    if (OB_UNLIKELY(1 != aggr_func->aggr_processor_.get_aggr_infos().count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("aggr infos count mismatch", K(ret),
                K(aggr_func->aggr_processor_.get_aggr_infos().count()));
    } else if (OB_FAIL(aggr_func->aggr_processor_.fast_single_row_agg(
                eval_ctx_, aggr_func->aggr_processor_.get_aggr_infos().at(0)))) {
      LOG_WARN("fast_single_row_one_aggr_info failed", K(ret));
    } else {
      ObDatum &result_datum = wf_cell.wf_info_.aggr_info_.expr_->locate_expr_datum(eval_ctx_);
      val = static_cast<ObDatum &>(result_datum);
    }
  }
  return ret;
}

int ObWindowFunctionOp::compute(RowsReader &row_reader, WinFuncCell &wf_cell,
    const int64_t row_idx, ObDatum &val)
{
  int ret = OB_SUCCESS;
  const ObRADatumStore::StoredRow *row = NULL;
  Frame new_frame;
  bool upper_has_null = false;
  bool lower_has_null = false;
  if (OB_FAIL(input_rows_.cur_->get_row(row_idx, row))) {
    LOG_WARN("failed to get row", K(ret), K(row_idx));
  } else if (FALSE_IT(clear_evaluated_flag())) {
  } else if (OB_FAIL(row->to_expr(get_all_expr(), eval_ctx_))) {
    LOG_WARN("Failed to to_expr", K(ret));
  } else if (OB_FAIL(get_pos(row_reader,
                             wf_cell,
                             row_idx,
                             *row,
                             true,
                             new_frame.head_,
                             upper_has_null))) {
    LOG_WARN("get pos failed", K(ret));
  } else if (OB_FAIL(get_pos(row_reader,
                             wf_cell,
                             row_idx,
                             *row,
                             false,
                             new_frame.tail_,
                             lower_has_null))) {
    LOG_WARN("get pos failed", K(ret));
  } else {
    Frame &last_valid_frame = wf_cell.last_valid_frame_;
    // 这里的part_frame仅仅用于裁剪, 只有上边界是准确的
    Frame part_frame(wf_cell.part_first_row_idx_, get_part_end_idx());

    LOG_DEBUG("dump frame", K(part_frame), K(last_valid_frame), K(new_frame),
              K(input_rows_.cur_->count()),
              K(row_idx), K(upper_has_null), K(lower_has_null), K(wf_cell));
    if (!upper_has_null && !lower_has_null && Frame::valid_frame(part_frame, new_frame)) {
      Frame::prune_frame(part_frame, new_frame);
      if (wf_cell.is_aggr()) {
        AggrCell *aggr_func = static_cast<AggrCell *>(&wf_cell);
        const ObRADatumStore::StoredRow *cur_row = NULL;
        if (!Frame::same_frame(last_valid_frame, new_frame)) {
          if (!Frame::need_restart_aggr(aggr_func->can_inv(), last_valid_frame, new_frame,
                                        aggr_func->aggr_processor_.get_removal_info(),
                                        wf_cell.wf_info_.remove_type_)) {
            if (aggr_func->aggr_processor_.get_removal_info().is_out_of_range_
                && (new_frame.head_ > last_valid_frame.head_
                    || new_frame.tail_ < last_valid_frame.tail_)) {
              // must inverse translate and type obnumber is out_of_range, calculate by traversal later
            } else {
              bool use_trans = new_frame.head_ < last_valid_frame.head_;
              int64_t b = min(new_frame.head_, last_valid_frame.head_);
              int64_t e = max(new_frame.head_, last_valid_frame.head_);
              for (int64_t i = b, skip_cnt = 0; OB_SUCC(ret) && i < e; ++i) {
                if (OB_FAIL(input_rows_.cur_->get_row(i, cur_row))) {
                  LOG_WARN("get cur row failed", K(ret), K(i));
                } else if (FALSE_IT(clear_evaluated_flag())) {
                } else if (OB_FAIL(cur_row->to_expr(get_all_expr(), eval_ctx_))) {
                  LOG_WARN("Failed to to_expr", K(ret));
                } else if (skip_calc(wf_cell.wf_idx_)) {
                  ++skip_cnt;
                  continue;
                } else if (OB_FAIL(aggr_func->invoke_aggr(use_trans, *cur_row))) {
                  LOG_WARN("invoke failed", K(use_trans), K(ret));
                } else if (common::REMOVE_EXTRENUM == wf_cell.wf_info_.remove_type_) {
                  aggr_func->aggr_processor_.get_removal_info().max_min_update(i - skip_cnt);
                }
              }
              use_trans = new_frame.tail_ > last_valid_frame.tail_;
              b = min(new_frame.tail_, last_valid_frame.tail_);
              e = max(new_frame.tail_, last_valid_frame.tail_);
              for (int64_t i = b + 1, skip_cnt = 0; OB_SUCC(ret) && i <= e; ++i) {
                if (OB_FAIL(input_rows_.cur_->get_row(i, cur_row))) {
                  LOG_WARN("get cur row failed", K(ret), K(i));
                } else if (FALSE_IT(clear_evaluated_flag())) {
                } else if (OB_FAIL(cur_row->to_expr(get_all_expr(), eval_ctx_))) {
                  LOG_WARN("Failed to to_expr", K(ret));
                } else if (skip_calc(wf_cell.wf_idx_)) {
                  ++skip_cnt;
                  continue;
                } else if (OB_FAIL(aggr_func->invoke_aggr(use_trans, *cur_row))) {
                  LOG_WARN("invoke failed", K(use_trans), K(ret));
                } else if (common::REMOVE_EXTRENUM == wf_cell.wf_info_.remove_type_) {
                  aggr_func->aggr_processor_.get_removal_info().max_min_update(i - skip_cnt);
                }
              }
            }
            // If the calculated obnumber type is out of bounds, fallback and use traversal.
            if (OB_UNLIKELY(aggr_func->aggr_processor_.get_removal_info().is_out_of_range_
                            && (new_frame.head_ > last_valid_frame.head_
                                || new_frame.tail_ < last_valid_frame.tail_))) {
              aggr_func->reset_for_restart();
              for (int64_t i = new_frame.head_; OB_SUCC(ret) && i <= new_frame.tail_; ++i) {
                if (OB_FAIL(input_rows_.cur_->get_row(i, cur_row))) {
                  LOG_WARN("get cur row failed", K(ret), K(i));
                } else if (FALSE_IT(clear_evaluated_flag())) {
                } else if (OB_FAIL(cur_row->to_expr(get_all_expr(), eval_ctx_))) {
                  LOG_WARN("Failed to to_expr", K(ret));
                } else if (skip_calc(wf_cell.wf_idx_)) {
                  continue;
                } else if (OB_FAIL(aggr_func->trans(*cur_row))) {
                  LOG_WARN("trans failed", K(ret));
                }
              }
            }
          } else {
            aggr_func->reset_for_restart();
            if (common::REMOVE_EXTRENUM == wf_cell.wf_info_.remove_type_) {
              // reset max_min index as head of new frame
              aggr_func->aggr_processor_.get_removal_info().max_min_index_ = new_frame.head_;
            }
            LOG_DEBUG("restart agg", K(last_valid_frame), K(new_frame), KPC(aggr_func));
            for (int64_t i = new_frame.head_, skip_cnt = 0;
                OB_SUCC(ret) && i <= new_frame.tail_;
                ++i) {
              if (OB_FAIL(input_rows_.cur_->get_row(i, cur_row))) {
                LOG_WARN("get cur row failed", K(ret), K(i));
              } else if (FALSE_IT(clear_evaluated_flag())) {
              } else if (OB_FAIL(cur_row->to_expr(get_all_expr(), eval_ctx_))) {
                LOG_WARN("Failed to to_expr", K(ret));
              } else if (skip_calc(wf_cell.wf_idx_)) {
                ++skip_cnt;
                continue;
              } else if (OB_FAIL(aggr_func->trans(*cur_row))) {
                LOG_WARN("trans failed", K(ret));
              } else if (common::REMOVE_EXTRENUM == wf_cell.wf_info_.remove_type_) {
                aggr_func->aggr_processor_.get_removal_info().max_min_update(i - skip_cnt);
              }
            }
          }
        } else {
          LOG_DEBUG("use last value");
          // reuse last result, invoke final directly...
        }
        if (OB_SUCC(ret)) {
          if (MY_SPEC.is_consolidator() && !aggr_func->finish_prepared_) { // all rows skipped
            val.set_null();
            last_valid_frame = new_frame;
          } else if (OB_FAIL(aggr_func->final(val))) {
            LOG_WARN("final failed", K(ret));
          } else {
            if (aggr_func->aggr_processor_.get_removal_info().null_cnt_
                == new_frame.tail_ - new_frame.head_ + 1) {
              val.set_null();
            }
            last_valid_frame = new_frame;
            LOG_DEBUG("finish compute", K(row_idx), K(last_valid_frame), K(val));
          }
        }
      } else {
        NonAggrCell *non_aggr_func = static_cast<NonAggrCell *>(&wf_cell);
        if (!Frame::same_frame(last_valid_frame, new_frame)) {
          non_aggr_func->reset_for_restart();
        }
        if (OB_FAIL(non_aggr_func->eval(row_reader, row_idx, *row, new_frame, val))) {
          LOG_WARN("eval failed", K(ret));
        } else {
          last_valid_frame = new_frame;
        }
      }
    } else {
      if (OB_FAIL(set_compute_result_for_invalid_frame(wf_cell, val))) {
        LOG_WARN("set compute result for invalid frame fail", KR(ret));
      }
    }
  }

  return ret;
}

bool ObWindowFunctionOp::skip_calc(const int64_t wf_idx)
{
  bool bret = false;
  if (MY_SPEC.is_push_down()) { // check if need to skip calc
    int64_t aggr_status = MY_SPEC.wf_aggr_status_expr_->locate_expr_datum(eval_ctx_).get_int();
    if (MY_SPEC.is_participator() && aggr_status < 0) {
      bret = true;
    } else if (MY_SPEC.is_consolidator()) {
      if ((aggr_status < 0 && -aggr_status != wf_idx)
          || (aggr_status >= 0 && aggr_status < wf_idx) ) {
        bret = true;
      }
    }
  }
  return bret;
}

int ObWindowFunctionOp::set_compute_result_for_invalid_frame(WinFuncCell &wf_cell, ObDatum &val) {
  int ret = OB_SUCCESS;

  switch (wf_cell.wf_info_.func_type_) {
    case T_FUN_COUNT: {
      ObDatum &expr_datum = wf_cell.wf_info_.aggr_info_.expr_->locate_datum_for_write(eval_ctx_);
      expr_datum.set_int(0);
      wf_cell.wf_info_.aggr_info_.expr_->set_evaluated_flag(eval_ctx_);
      val = static_cast<ObDatum &>(expr_datum);
      break;
    }
    case T_FUN_SYS_BIT_AND:
    case T_FUN_SYS_BIT_OR:
    case T_FUN_SYS_BIT_XOR: {
      ObDatum &expr_datum = wf_cell.wf_info_.aggr_info_.expr_->locate_datum_for_write(eval_ctx_);
      uint64_t temp_val = wf_cell.wf_info_.func_type_ == T_FUN_SYS_BIT_AND ? UINT_MAX_VAL[ObUInt64Type] : 0;
      expr_datum.set_uint(temp_val);
      wf_cell.wf_info_.aggr_info_.expr_->set_evaluated_flag(eval_ctx_);
      val = static_cast<ObDatum &>(expr_datum);
      break;
    }
    default: {
      // set null for invalid frame
      val.set_null();
    }
  }

  return ret;
}

int ObWindowFunctionOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  bool got_row = false;
  do {
    switch (stat_) {
      case ProcessStatus::PARTIAL: {
        if (OB_FAIL(partial_next_row())) {
          if (OB_ITER_END == ret) {
            if (MY_SPEC.single_part_parallel_ || MY_SPEC.range_dist_parallel_) {
              ret = OB_SUCCESS;
              stat_ = ProcessStatus::COORINDATE;
            }
          } else {
            LOG_WARN("partial next row failed", K(ret));
          }
        } else if (MY_SPEC.is_consolidator() && !input_rows_.cur_->need_output_) {
          got_row = false;
        } else {
          got_row = true;
        }
        break;
      }
      case ProcessStatus::COORINDATE: {
        if (OB_FAIL(coordinate())) {
          LOG_WARN("coordinate failed", K(ret));
        } else {
          stat_ = ProcessStatus::FINAL;
        }
        break;
      }
      case ProcessStatus::FINAL: {
        if (OB_FAIL(final_next_row())) {
          if (OB_ITER_END != ret) {
            LOG_WARN("get next row failed", K(ret));
          }
        } else {
          got_row = true;
        }
        break;
      }
    }
  } while (OB_SUCC(ret) && !got_row);

  if (OB_ITER_END == ret) {
    iter_end_ = true;
    reset_for_scan(ctx_.get_my_session()->get_effective_tenant_id());
  }
  return ret;
}

int ObWindowFunctionOp::detect_aggr_status() // for participator
{

  int ret = OB_SUCCESS;
  WinFuncCell *first = wf_list_.get_first();
  WinFuncCell *end = wf_list_.get_header();
  last_aggr_status_ = 0;
  int64_t prev_wf_pby_expr_count = -1; // prev_wf_pby_expr_count transmit to datahub
  for (WinFuncCell *wf = first; OB_SUCC(ret) && wf != end; wf = wf->get_next()) {
    bool is_pushdown_bypass = !wf->wf_info_.can_push_down_;
    if (OB_SUCC(ret) && wf->wf_info_.can_push_down_) {
      int64_t pushdown_wf_idx = pby_expr_cnt_idx_array_.at(wf->wf_idx_ - 1);
      if (wf->wf_info_.partition_exprs_.count() > next_wf_pby_expr_cnt_to_transmit_) {
        // has sent data to datahub already
        // check whether pby is in hashset to decide if need bypass or not
        ReportingWFHashSet *pushdown_pby_hash_values_set =
            pby_hash_values_sets_.at(pushdown_wf_idx);
        uint64_t hash_value;
        if (OB_ISNULL(pushdown_pby_hash_values_set)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("pushdown_pby_row_stores_set is null", K(ret));
        } else if (OB_FAIL(calc_part_exprs_hash(
            &wf->wf_info_.partition_exprs_, wf->part_values_.store_row_, hash_value))) {
          LOG_WARN("calc_part_exprs_hash", K(ret));
        } else if (OB_FAIL(pushdown_pby_hash_values_set->exist_refactored(hash_value))) {
          if (OB_HASH_NOT_EXIST == ret) {
            is_pushdown_bypass = true;
            ret = OB_SUCCESS;
          } else if (OB_HASH_EXIST == ret) {
            is_pushdown_bypass = false;
            ret = OB_SUCCESS;
          } else{
            LOG_WARN("Failed to find in hashmap", K(ret));
          }
        }
      }
      if (OB_SUCC(ret)
          && wf->wf_info_.partition_exprs_.count() != prev_wf_pby_expr_count
          && wf->wf_info_.partition_exprs_.count() <= next_wf_pby_expr_cnt_to_transmit_) {
        if (pby_hash_values_.at(pushdown_wf_idx)->count()
            < static_cast<ObWindowFunctionOpInput*>(input_)->get_total_task_count()) {
          // has not sent data to datahub
          // check whether the count of different pby comes up to the count of dop
          // and decide if need to send data to datahub
          uint64_t hash_value;
          if (OB_FAIL(calc_part_exprs_hash(
                      &wf->wf_info_.partition_exprs_, wf->part_values_.store_row_, hash_value))) {
            LOG_WARN("calc_part_exprs_hash", K(ret));
          } else if (OB_FAIL(
              pby_hash_values_.at(pushdown_wf_idx)->push_back(hash_value))) {
            LOG_WARN("push_back to pby_hash_values_ failed", K(ret), KPC(wf),
                     K(pushdown_wf_idx),
                     K(ObToStringExprRow(eval_ctx_, wf->wf_info_.partition_exprs_)), K(hash_value));
          }
        }
        if (OB_SUCC(ret)
           && (pby_hash_values_.at(pushdown_wf_idx)->count()
                  == static_cast<ObWindowFunctionOpInput*>(input_)->get_total_task_count()
               || child_iter_end_)) {
          // participator_coordinate
          if (OB_FAIL(participator_coordinate(
              pushdown_wf_idx, wf->wf_info_.partition_exprs_))) {
            LOG_WARN("participator_coordinate failed", K(ret), K(pushdown_wf_idx),
                     K(wf->wf_info_), K(wf->wf_info_.partition_exprs_));
          } else {
            next_wf_pby_expr_cnt_to_transmit_ = wf->wf_info_.partition_exprs_.count() - 1;
          }
        }
      }
      if (OB_SUCC(ret)) {
        prev_wf_pby_expr_count = wf->wf_info_.partition_exprs_.count();
      }
    }

    if (OB_SUCC(ret)) {
      if (is_pushdown_bypass) {
        last_aggr_status_ = wf->wf_idx_;
      }
    }
  }
  if (OB_SUCC(ret)) {
    //last_aggr_status_ = aggr_status_value;
    MY_SPEC.wf_aggr_status_expr_->locate_datum_for_write(eval_ctx_).set_int(last_aggr_status_);
  }
  return ret;
}

// for participator, add aggr result row
int ObWindowFunctionOp::found_part_end(
    const WinFuncCell *end,
    //const int64_t aggr_status_value,
    RowsStore *rows_store,
    bool add_row_cnt /* = true */)
{
  int ret = OB_SUCCESS;
  if (MY_SPEC.is_participator()) {
    if (last_aggr_status_ < wf_list_.get_last()->wf_idx_) {
      for (WinFuncCell *wf = wf_list_.get_first(); OB_SUCC(ret) && wf != end; wf = wf->get_next()) {
        if (last_aggr_status_ < wf->wf_idx_) {
          MY_SPEC.wf_aggr_status_expr_->locate_datum_for_write(eval_ctx_).set_int(-wf->wf_idx_);
          if (rows_store->stored_row_cnt_ <= 0) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("rows_store->stored_row_cnt_ <= 0",
                     K(ret), K(rows_store->count()), K(rows_store->stored_row_cnt_));
          } else {
            const int64_t row_idx = rows_store->stored_row_cnt_ - 1; // the last row
            const ObRADatumStore::StoredRow *sr = NULL; // the last row of last part
            if (OB_FAIL(rows_store->get_row(row_idx, sr))) {
              LOG_WARN("get_row failed", K(ret), K(row_idx));
            } else if (OB_ISNULL(sr)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("sr is null", K(ret), K(row_idx));
            } else {
              int64_t i = 0;
              ObArray<common::ObDatum> datums;
              for (; OB_SUCC(ret) && i < MY_SPEC.get_child()->output_.count(); ++i) {
                if (OB_FAIL(datums.push_back(sr->cells()[i]))) {
                  LOG_WARN("fail to push_back to datums", K(ret), K(i), K(sr->cells()[i]));
                }
              }
              if (OB_FAIL(ret)) {
              } else if (MY_SPEC.all_expr_.count() != i + 1) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("the count of all_expr_ is unexpected",
                         K(ret), K(MY_SPEC.all_expr_.count()), K(i + 1));
              } else if (OB_FAIL(datums.push_back(
                         MY_SPEC.wf_aggr_status_expr_->locate_expr_datum(eval_ctx_)))) {
                LOG_WARN("fail to push_back to datums", K(ret), K(i),
                         K(MY_SPEC.wf_aggr_status_expr_->locate_expr_datum(eval_ctx_).get_int()));
              } else if (OB_FAIL(rows_store->add_row(datums, NULL, add_row_cnt))) {
                LOG_WARN("fail to add_row to rows_store", K(ret), K(datums.count()), K(add_row_cnt));
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObWindowFunctionOp::partial_next_row()
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("before partial_next_row",
            "total_size", this->local_allocator_.get_arena().total(),
            "used_size", this->local_allocator_.get_arena().used());
  if (OB_UNLIKELY(iter_end_)) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(ctx_.check_status())) {
    LOG_WARN("check physical plan status failed", K(ret));
  }
  const uint64_t tenant_id = ctx_.get_my_session()->get_effective_tenant_id();
  WinFuncCell *first = wf_list_.get_first();
  WinFuncCell *end = wf_list_.get_header();
  //int64_t aggr_status_value = 0;
  while (OB_SUCC(ret)) {
    if (child_iter_end_ && all_outputed()) {
      LOG_DEBUG("iter end", K(last_output_row_idx_));
      ret = OB_ITER_END;
    } else if (all_outputed()) {
      LOG_DEBUG("ObWindowFunctionOp::partial_next_row() begin compute",
                K(input_rows_.cur_->count()), K(last_output_row_idx_), K(MY_SPEC.get_role_type()));
      // load && compute
      if (OB_FAIL(reset_for_part_scan(tenant_id))) {
        LOG_WARN("fail to reset_for_part_scan", K(ret));
      }
      int64_t check_times = 0;
      do {
        // <1> get first row
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(fetch_child_row())) {
          if (OB_ITER_END != ret) {
            LOG_WARN("get part first row from child failed", K(ret));
          }
        }
        // <2> save partition by value
        if (OB_SUCC(ret) && OB_FAIL(found_new_part(true))) { // save partition by value of first row
          LOG_WARN("store partition exprs datum failed", K(ret));
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(input_rows_.cur_->add_row(get_all_expr(), &eval_ctx_))) {
            LOG_WARN("add row to row store failed", K(ret));
          }
        }
        // <3> iterate child rows and check same partition with the first window function
        bool part_end = false;
        while (OB_SUCC(ret) && !part_end) {
          if (OB_FAIL(input_one_row(*first, part_end))) {
            LOG_WARN("input one row failed", K(ret));
          }
        }
        // <4> check the following window functions whether in same partition
        end = wf_list_.get_header();
        if (OB_FAIL(ret)) {
        } else if (child_iter_end_ || 1 == wf_list_.get_size()) {
          // all window functions end current partition.
        } else if (OB_FAIL(check_wf_same_partition(end))) {
          LOG_WARN("check other window function failed", K(ret));
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(found_part_end(end, input_rows_.cur_))) {
          // For participator, add aggr result row to input rows
          LOG_WARN("found_part_end failed", K(ret), K(last_aggr_status_));
        } else if (OB_FAIL(compute_wf_values(end, check_times))) {
          LOG_WARN("compute wf values failed", K(ret));
        }
      } while (OB_SUCC(ret) && end != wf_list_.get_header());
    } else {
      if (MY_SPEC.single_part_parallel_) {
        ret = OB_ITER_END;
      } else if (MY_SPEC.range_dist_parallel_ && !first_part_saved_) {
        first_part_saved_ = true;
        foreach_stores([](Stores &s) { std::swap(s.cur_, s.first_); return OB_SUCCESS; });
        continue;
      } else if (MY_SPEC.range_dist_parallel_ && child_iter_end_ && !last_part_saved_) {
        last_part_saved_ = true;
        foreach_stores([](Stores &s) { std::swap(s.cur_, s.last_); return OB_SUCCESS; });
        ret = OB_ITER_END;
        break;
      } else {
        if (OB_FAIL(output_row())) {
          LOG_WARN("output row failed", K(ret));
        } else {
          break;
        }
      }
    }
  }
  // to make sure to send piece data to datahub even though no row fetched
  if (MY_SPEC.is_participator() && OB_ITER_END == ret) {
    ret = OB_SUCCESS;
    if (OB_FAIL(send_empty_piece_data())) {
      LOG_WARN("send_empty_piece_data failed", K(ret));
    }
    ret = OB_SUCC(ret) ? OB_ITER_END : ret;
  }

  LOG_DEBUG("after partial_next_row", K(ret), K(child_iter_end_),
            "total_size", this->local_allocator_.get_arena().total(),
            "used_size", this->local_allocator_.get_arena().used());
  return ret;
}

int ObWindowFunctionOp::output_row()
{
  last_output_row_idx_ += 1;
  return output_row(last_output_row_idx_);
}

int ObWindowFunctionOp::output_row(int64_t idx,
                                   StoreMemberPtr store_member,
                                   const ObRADatumStore::StoredRow **all_expr_row /* = NULL */)
{
  int ret = OB_SUCCESS;
  WinFuncCell &wf_cell = *wf_list_.get_last();
  LOG_DEBUG("do output", "idx", idx, K(wf_cell),
            K((input_rows_.*store_member)->count()));
  const ObRADatumStore::StoredRow *child_row = NULL;
  const ObRADatumStore::StoredRow *result_row = NULL;
  (input_rows_.*store_member)->need_output_ = false;

  if (MY_SPEC.is_consolidator()) { // skip aggr result row from participator
    bool has_more_to_output = last_output_row_idx_ < (input_rows_.*store_member)->count();
    while (OB_SUCC(ret)
           && !(input_rows_.*store_member)->need_output_
           && has_more_to_output) {
      if (OB_FAIL((input_rows_.*store_member)->get_row(last_output_row_idx_, child_row))) {
        LOG_WARN("get row failed", K(ret), K(last_output_row_idx_));
      } else if (OB_FAIL(child_row->to_expr(get_all_expr(), eval_ctx_))) {
        LOG_WARN("Failed to get next row", K(ret));
      } else if (MY_SPEC.wf_aggr_status_expr_->locate_expr_datum(eval_ctx_).get_int() >= 0) {
        (input_rows_.*store_member)->need_output_ = true;
        idx = last_output_row_idx_;
      } else if (last_output_row_idx_ + 1 < input_rows_.cur_->count()) {
        ++last_output_row_idx_;
      } else {
        has_more_to_output = false;
      }
    }
  } else {
    (input_rows_.*store_member)->need_output_ = true;
    if (OB_FAIL((input_rows_.*store_member)->get_row(idx, child_row))) {
      LOG_WARN("get row failed", K(ret), K(idx));
    } else if (OB_FAIL(child_row->to_expr(get_all_expr(), eval_ctx_))) {
      LOG_WARN("Failed to get next row", K(ret));
    }
  }
  if (OB_SUCC(ret) && (input_rows_.*store_member)->need_output_) {
    if (MY_SPEC.single_part_parallel_ &&
               OB_FAIL((wf_cell.res_.*store_member)->get_row(0, result_row))) {
      LOG_WARN("get row failed", K(ret), K(idx));
    } else if (!MY_SPEC.single_part_parallel_ &&
               OB_FAIL((wf_cell.res_.*store_member)->get_row(idx, result_row))) {
      LOG_WARN("get row failed", K(ret), K(idx));
    } else {
      if (NULL != all_expr_row) {
        *all_expr_row = child_row;
      }
      clear_evaluated_flag();
      WinFuncCell *tmp_wf_cell = wf_list_.get_first();
      for (int64_t i = 0;
           i < result_row->cnt_ && tmp_wf_cell != NULL;
           ++i, tmp_wf_cell = tmp_wf_cell->get_next()) {
        tmp_wf_cell->wf_info_.expr_->locate_expr_datum(eval_ctx_) = result_row->cells()[i];
        tmp_wf_cell->wf_info_.expr_->set_evaluated_projected(eval_ctx_);
      }
      LOG_DEBUG("finish output one row", "idx", idx, KPC(child_row), KPC(result_row));
    }
  }

  return ret;
}

bool ObWindowFunctionOp::first_row_same_order(const ObRADatumStore::StoredRow *row)
{
  if (SAME_ORDER_CACHE_DEFAULT == first_row_same_order_cache_) {
    int cmp_ret = 0;
    MY_SPEC.rd_oby_cmp(rd_patch_->first_row_, row, cmp_ret);
    first_row_same_order_cache_ = cmp_ret;
  }
  return 0 == first_row_same_order_cache_;
}

bool ObWindowFunctionOp::last_row_same_order(const ObRADatumStore::StoredRow *row)
{
  if (SAME_ORDER_CACHE_DEFAULT == last_row_same_order_cache_) {
    int cmp_ret = 0;
    MY_SPEC.rd_oby_cmp(rd_patch_->last_row_, row, cmp_ret);
    last_row_same_order_cache_ = cmp_ret;
  }
  return 0 == last_row_same_order_cache_;
}

int ObWindowFunctionOp::rd_output_final()
{
  int ret = OB_SUCCESS;
  if (all_outputed()) {
    ret = OB_ITER_END;
  } else {
    last_output_row_idx_ += 1;
    if (patch_alloc_.used() >= OB_MALLOC_MIDDLE_BLOCK_SIZE) {
      patch_alloc_.reset_remain_one_page();
    }
    if (OB_FAIL(rd_output_final_row(last_output_row_idx_, patch_first_, patch_last_))) {
      LOG_WARN("get result and apply patch failed", K(ret));
    }
  }
  return ret;
}

int ObWindowFunctionOp::rd_output_final_batch(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  if (input_rows_.cur_->to_output_rows() <= 0) {
    brs_.end_ = true;
    brs_.size_ = 0;
    iter_end_ = true;
  } else {
    if (patch_alloc_.used() >= OB_MALLOC_MIDDLE_BLOCK_SIZE) {
      patch_alloc_.reset_remain_one_page();
    }
    int64_t cnt = std::min(max_row_cnt, input_rows_.cur_->to_output_rows());
    ObEvalCtx::BatchInfoScopeGuard guard(eval_ctx_);
    guard.set_batch_size(cnt);
    output_rows_it_age_.inc();
    OZ(foreach_stores([&](Stores &s) { return set_it_age(s); }));
    for (int64_t i = 0; OB_SUCC(ret) && i < cnt; i++) {
      guard.set_batch_idx(i);
      int64_t idx = input_rows_.cur_->output_row_idx_ + i;
      // only rows in last computed partition range need to apply last patch.
      const bool in_last_part = idx + last_computed_part_rows_ >= input_rows_.cur_->count();
      if (OB_FAIL(rd_output_final_row(idx, patch_first_, patch_last_ && in_last_part))) {
        LOG_WARN("get result and patch failed", K(ret));
      }
    }
    OZ(foreach_stores([&](Stores &s) { return unset_it_age(s); }));
    if (OB_SUCC(ret)) {
      brs_.size_ = cnt;
      input_rows_.cur_->output_row_idx_ += cnt;
    }
  }
  return ret;
}

int ObWindowFunctionOp::rd_output_final_row(const int64_t idx,
                                            const bool patch_first,
                                            const bool patch_last)
{
  int ret = OB_SUCCESS;
  const ObRADatumStore::StoredRow *cur_row = NULL;
  if (OB_FAIL(output_row(idx, &Stores::cur_, &cur_row))) {
    LOG_WARN("output row failed", K(ret));
  } else {
    // apply patches
    if (patch_first) {
      reset_first_row_same_order_cache();
      for (int64_t wf_idx = 0; OB_SUCC(ret) && wf_idx < MY_SPEC.rd_wfs_.count(); wf_idx++) {
        const WinFuncInfo &info = MY_SPEC.wf_infos_.at(MY_SPEC.rd_wfs_.at(wf_idx));
        const ObDatum &patch_datum = rd_patch_->first_row_->cells()[
            MY_SPEC.rd_sort_collations_.count() + wf_idx];
        if (!patch_datum.is_null()) {
          ObDatum &res = info.expr_->locate_expr_datum(eval_ctx_);
          if (T_WIN_FUN_RANK == info.func_type_ && !first_row_same_order(cur_row)) {
            int64_t patch_val = rd_patch_->first_row_
                ->extra_payload<ObRDWFPartialInfo::RowExtType>();
            OZ(rank_add(res, info, patch_alloc_, res, patch_val));
            LOG_DEBUG("after patch rank value",
                      K(patch_val), "res", number::ObNumber(res.get_number()));
          } else {
            OZ(merge_aggregated_result(res, info, patch_alloc_, res, patch_datum));
          }
        }
      }
    }
    if (OB_SUCC(ret) && patch_last) {
      reset_last_row_same_order_cache();
      for (int64_t wf_idx = 0; OB_SUCC(ret) && wf_idx < MY_SPEC.rd_wfs_.count(); wf_idx++) {
        const WinFuncInfo &info = MY_SPEC.wf_infos_.at(MY_SPEC.rd_wfs_.at(wf_idx));
        const ObDatum &patch_datum = rd_patch_->last_row_->cells()[
            MY_SPEC.rd_sort_collations_.count() + wf_idx];
        if (!patch_datum.is_null() && last_row_same_order(cur_row)) {
          ObDatum &res = info.expr_->locate_expr_datum(eval_ctx_);
          OZ(merge_aggregated_result(res, info, patch_alloc_, res, patch_datum));
        }
      }
    }
  }
  return ret;
}

int ObWindowFunctionOp::coordinate()
{
  int ret = OB_SUCCESS;
  if (MY_SPEC.single_part_parallel_) {
    if (OB_FAIL(parallel_winbuf_process())) {
      LOG_WARN("parallel single partition window function process failed", K(ret));
    }
  } else if (MY_SPEC.range_dist_parallel_) {
    LOG_DEBUG("before fetch patch", K(last_computed_part_rows_),
              K(*input_rows_.first_),
              K(*input_rows_.last_));
    if (OB_FAIL(rd_fetch_patch())) {
      LOG_WARN("fetch patch info from PX COORD failed", K(ret));
    } else {
      LOG_DEBUG("fetch patch", K(*rd_patch_));
      // prepare for final output
      last_output_row_idx_ = OB_INVALID_INDEX;
      foreach_stores([](Stores &s){ std::swap(s.first_, s.cur_); return OB_SUCCESS; });
      patch_first_ = true;
      if (input_rows_.last_->count() <= 0) {
        // only one partition
        patch_last_ = true;
      }
    }
  }
  return ret;
}

int ObWindowFunctionOp::rd_fetch_patch()
{
  int ret = OB_SUCCESS;
  ObPxSqcHandler *handler = ctx_.get_sqc_handler();
  if (NULL == handler) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null sqc handler", K(ret));
  } else {
    const int64_t row_ext_size = 8;
    ObRDWFPieceMsg piece_msg;
    piece_msg.op_id_ = MY_SPEC.id_;
    piece_msg.thread_id_ = GETTID();
    piece_msg.source_dfo_id_ = handler->get_sqc_proxy().get_dfo_id();
    piece_msg.target_dfo_id_ = handler->get_sqc_proxy().get_dfo_id();

    piece_msg.info_.sqc_id_ = handler->get_sqc_proxy().get_sqc_id();
    piece_msg.info_.thread_id_ = GETTID();
    auto build_store_row = [&](ObStoredDatumRow *&row, int64_t idx, StoreMemberPtr store_member) {
      clear_evaluated_flag();
      ObEvalCtx::BatchInfoScopeGuard batch_info_guard(eval_ctx_);
      batch_info_guard.set_batch_idx(0);
      batch_info_guard.set_batch_size(1);
      int64_t size = 0;
      if (OB_FAIL(output_row(idx, store_member))) {
        LOG_WARN("output row failed", K(ret));
      } else if (OB_FAIL(ObStoredDatumRow::build(row,
                                                 MY_SPEC.rd_coord_exprs_,
                                                 eval_ctx_,
                                                 piece_msg.arena_alloc_,
                                                 sizeof(ObRDWFPartialInfo::RowExtType)))) {
        LOG_WARN("build stored datum row failed", K(ret));
      } else {
        row->extra_payload<ObRDWFPartialInfo::RowExtType>() = idx;
      }
    };
    if (input_rows_.first_->count() > 0) {
      build_store_row(piece_msg.info_.first_row_, 0, &Stores::first_);
      if (OB_SUCC(ret) && input_rows_.last_->count() <= 0) {
        // get the first partition's last row if only one partition
        build_store_row(piece_msg.info_.last_row_,
                        input_rows_.first_->count() - 1,
                        &Stores::first_);
      }
    }
    if (OB_SUCC(ret) && input_rows_.last_->count() > 0) {
      build_store_row(piece_msg.info_.last_row_,
                      input_rows_.last_->count() - 1,
                      &Stores::last_);
      if (OB_SUCC(ret) && MY_SPEC.is_vectorized()) {
        piece_msg.info_.last_row_->extra_payload<ObRDWFPartialInfo::RowExtType>()
            = last_computed_part_rows_ - 1;
      }
    }

    if (OB_SUCC(ret)) {
      LOG_TRACE("build piece message info", K(piece_msg.info_));
    }

    const ObRDWFWholeMsg *whole_msg = NULL;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(handler->get_sqc_proxy().get_dh_msg_sync(
                MY_SPEC.id_, dtl::DH_RANGE_DIST_WF_PIECE_MSG, piece_msg, whole_msg,
                ctx_.get_physical_plan_ctx()->get_timeout_timestamp()))) {
      LOG_WARN("get range distribute window function msg failed", K(ret));
    } else if (OB_ISNULL(whole_msg)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL msg", K(ret));
    } else {
      // find patch of my worker,
      // `ObRDWFWholeMsg::infos_` already sorted by thread_id in QC
      int64_t tid = GETTID();
      ObRDWFWholeMsg *m = const_cast<ObRDWFWholeMsg *>(whole_msg);
      auto info = std::lower_bound(m->infos_.begin(), m->infos_.end(), tid,
                                   [](ObRDWFPartialInfo *it, int64_t id)
                                   { return it->thread_id_ < id; });
      if (info == m->infos_.end() || (*info)->thread_id_ != tid) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("worker's response message not found in whole message", K(ret), K(tid));
      } else if (OB_ISNULL(rd_patch_ = (*info)->dup(rescan_alloc_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("duplicate patch failed", K(ret));
      }
    }
  }
  return ret;
}

int ObWindowFunctionOp::final_next_row()
{
  int ret = OB_SUCCESS;
  if (MY_SPEC.range_dist_parallel_) {
    ret = rd_output_final();
    if (OB_ITER_END == ret && !first_part_outputed_) {
      first_part_outputed_ = true;
      if (input_rows_.last_->count() > 0) {
        iter_end_ = false;
        ret = OB_SUCCESS;
        last_output_row_idx_ = OB_INVALID_INDEX;
        foreach_stores([](Stores &s){ std::swap(s.first_, s.cur_); return OB_SUCCESS; });
        foreach_stores([](Stores &s){ std::swap(s.last_, s.cur_); return OB_SUCCESS; });
        patch_first_ = false;
        patch_last_ = true;
        ret = rd_output_final();
      }
    }
  } else {
    if (all_outputed()) {
      ret = OB_ITER_END;
    } else {
      if (OB_FAIL(output_row())) {
        LOG_WARN("output row failed", K(ret));
      }
    }
  }
  return ret;
}


int ObWindowFunctionOp::copy_datum_row(const ObRADatumStore::StoredRow &row, ObWinbufPieceMsg &piece,
  int64_t buf_len, char *buf)
{
  int ret = OB_SUCCESS;
  CK(OB_NOT_NULL(buf));
  CK(buf_len > 0);
  CK(row.row_size_ > sizeof(ObRADatumStore::StoredRow));
  if (OB_SUCC(ret)) {
    piece.datum_row_ = new(buf) ObChunkDatumStore::StoredRow();
    piece.datum_row_->row_size_ = row.row_size_;
    piece.datum_row_->cnt_ = row.cnt_;
    MEMCPY(piece.datum_row_->payload_, row.payload_,
        row.row_size_ - sizeof(ObRADatumStore::StoredRow));
    char *base = const_cast<char *>(row.payload_);
    piece.datum_row_->unswizzling(base);
    piece.datum_row_->swizzling(piece.datum_row_->payload_);
  }
  return ret;
}

// send piece msg and wait whole msg.
int ObWindowFunctionOp::get_whole_msg(bool is_end,
    ObWinbufWholeMsg &whole,
    const ObRADatumStore::StoredRow *res_row)
{
  int ret = OB_SUCCESS;
  const ObWinbufWholeMsg *temp_whole_msg = NULL;
  ObPxSqcHandler *handler = ctx_.get_sqc_handler();
  if (OB_ISNULL(handler)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("parallel winbuf only supported in parallel execution mode", K(MY_SPEC.single_part_parallel_));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "parallel winbuf in non-px mode");
  } else {
    ObPxSQCProxy &proxy = handler->get_sqc_proxy();
    ObWinbufPieceMsg piece;
    piece.op_id_ = MY_SPEC.id_;
    piece.thread_id_ = GETTID();
    piece.source_dfo_id_ = proxy.get_dfo_id();
    piece.target_dfo_id_ = proxy.get_dfo_id();
    piece.is_datum_ = true;
    piece.col_count_ = res_row ? res_row->cnt_ : 0;
    if (is_end) {
      piece.is_end_ = true;
    } else if (OB_ISNULL(res_row)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("row ptr is null", K(ret));
    } else {
      piece.row_size_ = res_row->row_size_;
      piece.is_end_ = false;
      piece.payload_len_ = res_row->row_size_ - sizeof(ObChunkDatumStore::StoredRow);
      int64_t len = res_row->row_size_;
      char *buf = NULL;
      if (OB_ISNULL(buf = (char *)ctx_.get_allocator().alloc(len))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc row buf", K(ret));
      } else if (OB_FAIL(copy_datum_row(*res_row, piece, len, buf))) {
        LOG_WARN("fail to deep copy row", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      // Synchronization is requested here.
      if (OB_FAIL(proxy.get_dh_msg_sync(MY_SPEC.id_,
          dtl::DH_WINBUF_WHOLE_MSG,
          piece,
          temp_whole_msg,
          ctx_.get_physical_plan_ctx()->get_timeout_timestamp()))) {
        LOG_WARN("fail get win buf msg", K(ret));
      } else if (OB_ISNULL(temp_whole_msg)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("whole msg is unexpected", K(ret));
      } else if (OB_FAIL(whole.assign(*temp_whole_msg))) {
        LOG_WARN("fail to assign msg", K(ret));
      }
    }
  }

  return ret;
}

int ObWindowFunctionOp::parallel_winbuf_process()
{
  int ret = OB_SUCCESS;
  WinFuncCell &wf_cell = *wf_list_.get_last();
  const ObRADatumStore::StoredRow *res_row = NULL;
  ObWinbufWholeMsg whole;
  if (wf_cell.res_.cur_->count() <= 0) {
    if (OB_FAIL(get_whole_msg(true, whole))) {
      LOG_WARN("fail to get whole msg", K(ret));
    }
  } else if (OB_FAIL(wf_cell.res_.cur_->get_row(0, res_row))) {
    LOG_WARN("get row faild", K(ret), K(res_row));
  } else if (OB_FAIL(get_whole_msg(false, whole, res_row))) {
    LOG_WARN("fail to get whole msg", K(ret));
  } else if (whole.is_empty_) {
    /*do nothing*/
  } else {
    LOG_DEBUG("parallel winbuf process start", K(res_row), KPC(res_row));
    ObChunkDatumStore::Iterator row_store_it;
    ObRADatumStore::StoredRow *new_row = const_cast<
        ObRADatumStore::StoredRow *>(res_row);
    const ObChunkDatumStore::StoredRow *row = NULL;
    bool is_first = true;
    if (OB_FAIL(whole.datum_store_.begin(row_store_it))) {
      LOG_WARN("failed to begin iterator for chunk row store", K(ret));
    } else {
      while (OB_SUCC(ret)) {
        if (OB_FAIL(row_store_it.get_next_row(row))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("fail to get next row", K(ret));
          }
        } else if (is_first) {
          is_first = false;
          for (int i = 0; i < new_row->cnt_; ++i) {
            new_row->cells()[i] = row->cells()[i];
          }
        } else {
          int idx = 0;
          int64_t cmp_index = 0;
          DLIST_FOREACH_X(wf_node, wf_list_, OB_SUCC(ret)) {
            ObDatum &l_datum = new_row->cells()[idx];
            const ObDatum &r_datum = row->cells()[idx];
            if (OB_FAIL(merge_aggregated_result(
                        l_datum, wf_node->wf_info_, ctx_.get_allocator(), l_datum, r_datum))) {
              LOG_WARN("binary aggregated result failed", K(ret));
            } else {
              idx += 1;
            }
          }
        }
      }
      if (ret == OB_ITER_END) {
        ret = OB_SUCCESS;
        for (int i = 0; i < new_row->cnt_ && OB_SUCC(ret); ++i) {
          OZ(new_row->cells()[i].deep_copy(new_row->cells()[i], ctx_.get_allocator()));
        }
        LOG_DEBUG("parallel winbuf process end", K(new_row), KPC(new_row));
      }
    }
  }
  return ret;
}

int ObWindowFunctionOp::merge_aggregated_result(ObDatum &res,
                                                const WinFuncInfo &wf_info,
                                                common::ObIAllocator &alloc,
                                                const ObDatum &src0,
                                                const ObDatum &src1)
{
  int ret = OB_SUCCESS;
  int cmp_ret = 0;
  switch(wf_info.func_type_) {
    case T_FUN_SUM:
    case T_FUN_COUNT:
    case T_WIN_FUN_RANK:
    case T_WIN_FUN_DENSE_RANK: {
      ObObjTypeClass tc = ob_obj_type_class(wf_info.expr_->datum_meta_.type_);
      if (src0.is_null() && !src1.is_null()) {
        res = src1;
      } else if (OB_FAIL(ObAggregateCalcFunc::add_calc(
            src0, src1, res, tc, wf_info.expr_->datum_meta_.precision_, alloc))) {
        LOG_WARN("fail to add calc", K(ret));
      }
      break;
    }
    case T_FUN_MAX: {
      if (OB_FAIL(wf_info.expr_->basic_funcs_->null_first_cmp_(src1, src0, cmp_ret))) {
        LOG_WARN("fail to compare", K(ret));
      } else {
        res = cmp_ret > 0 ? src1 : src0;
      }
      break;
    }
    case T_FUN_MIN: {
      if (OB_FAIL(wf_info.expr_->basic_funcs_->null_last_cmp_(src1, src0, cmp_ret))) {
        LOG_WARN("fail to compare", K(ret));
      } else {
        res = cmp_ret < 0 ? src1 : src0;
      }
      break;
    }
    default : {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "this window function");
      LOG_WARN("func is not supported", K(ret));
      break;
    }
  }
  return ret;
}

int ObWindowFunctionOp::rank_add(ObDatum &res,
                                 const WinFuncInfo &info,
                                 common::ObIAllocator &alloc,
                                 const ObDatum &rank,
                                 const int64_t val)
{
  int ret = OB_SUCCESS;
  if (ObUInt64Type == info.expr_->datum_meta_.type_) {
    int64_t v = rank.is_null() ? val : val + static_cast<int64_t>(rank.get_uint64());
    res.ptr_ = static_cast<char *>(alloc.alloc(sizeof(uint64_t)));
    if (NULL == res.ptr_) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else {
      res.set_uint(static_cast<uint64_t>(v));
    }
  } else if (ObNumberType == info.expr_->datum_meta_.type_) {
    ObNumStackAllocator<2> tmp_alloc;
    number::ObNumber res_nmb;
    if (OB_FAIL(res_nmb.from(val, tmp_alloc))) {
      LOG_WARN("convert int to number failed", K(ret), K(val));
    } else if (!rank.is_null()) {
      number::ObNumber rank_nmb(rank.get_number());
      if (OB_FAIL(rank_nmb.add(res_nmb, res_nmb, tmp_alloc))) {
        LOG_WARN("number add failed", K(ret), K(rank_nmb));
      }
    }
    if (OB_SUCC(ret)) {
      const int64_t len = sizeof(res_nmb.get_desc())
          + res_nmb.get_desc().len_ * sizeof(*res_nmb.get_digits());
      res.ptr_ = static_cast<char *>(alloc.alloc(len));
      if (NULL == res.ptr_) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret));
      } else {
        res.set_number(res_nmb);
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected rank() result type", K(ret), K(info));
  }
  return ret;
}

// get upper/lower bound for calculating wf_cell of row row_idx.
int ObWindowFunctionOp::get_pos(RowsReader &row_reader,
                                WinFuncCell &wf_cell,
                                const int64_t row_idx,
                                const ObRADatumStore::StoredRow &row,
                                const bool is_upper,
                                int64_t &pos,
                                bool &got_null_val) //上下限计算遇到了null值
{
  const bool is_rows = WINDOW_ROWS == wf_cell.wf_info_.win_type_;
  const bool is_preceding = (is_upper ? wf_cell.wf_info_.upper_.is_preceding_
                                      : wf_cell.wf_info_.lower_.is_preceding_);
  const bool is_unbounded = (is_upper ? wf_cell.wf_info_.upper_.is_unbounded_
                                      : wf_cell.wf_info_.lower_.is_unbounded_);
  const bool is_nmb_literal = (is_upper ? wf_cell.wf_info_.upper_.is_nmb_literal_
                                        : wf_cell.wf_info_.lower_.is_nmb_literal_);
  ObExpr *between_value_expr = (is_upper ? wf_cell.wf_info_.upper_.between_value_expr_
                                         : wf_cell.wf_info_.lower_.between_value_expr_);

  LOG_DEBUG("get_pos", K(is_rows), K(is_upper), K(is_preceding),
            K(is_unbounded), K(is_nmb_literal),
            K(row_idx),
            KPC(between_value_expr),
            "part first row", wf_cell.part_first_row_idx_,
            "part end row",  input_rows_.cur_->count() -1,
            "order by cnt", wf_cell.wf_info_.sort_exprs_.count());
  int ret = OB_SUCCESS;
  pos = -1;
  got_null_val = false;
  if (NULL == between_value_expr && is_unbounded) {
    // no care rows or range
    pos = is_preceding ? wf_cell.part_first_row_idx_ : get_part_end_idx();
  } else if (NULL == between_value_expr && !is_unbounded) {
    // current row
    if (is_rows) {
      pos = row_idx;
    } else {
      // range
      // for current row, it's no sense for is_preceding
      // we should jump to detect step by step(for case that the sort columns has very small ndv)
      // @TODO: mark detected pos to use for next row

      // Exponential detection
      pos = row_idx;
      int step = 1;
      ObSortCollations &sort_collations = wf_cell.wf_info_.sort_collations_;
      ObSortFuncs &sort_cmp_funcs = wf_cell.wf_info_.sort_cmp_funcs_;
      const ObRADatumStore::StoredRow *a_row = NULL;
      while (OB_SUCC(ret)) {
        bool match = false;
        is_upper ? (pos -= step) : (pos += step);
        const bool overflow = is_upper
                              ? (pos < wf_cell.part_first_row_idx_)
                              : (pos > get_part_end_idx());
        if (overflow) {
          match = true;
        } else if (OB_FAIL(row_reader.get_row(pos, a_row))) {
          LOG_WARN("failed to get row", K(pos), K(ret));
        } else {
          int cmp_ret = 0;
          for (int64_t i = 0; OB_SUCC(ret) && i < sort_collations.count() && !match; i++) {
            ObDatumCmpFuncType cmp_func = sort_cmp_funcs.at(i).cmp_func_;
            const int64_t idx = sort_collations.at(i).field_idx_;
            const ObDatum &l_datum = a_row->cells()[idx];
            const ObDatum &r_datum = row.cells()[idx];
            if (OB_FAIL(cmp_func(l_datum, r_datum, cmp_ret))) {
              LOG_WARN("cmp failed", K(ret), K(idx), K(l_datum), K(r_datum), K(match), K(step),
                        K(is_upper), K(pos));
            } else {
              match = (0 != cmp_ret);
              LOG_DEBUG("cmp ", K(idx), K(l_datum), K(r_datum), K(match), K(step),
                        K(is_upper), K(pos));
            }
          }
        }

        if (match) {
          is_upper ? (pos += step) : (pos -= step);
          if (1 == step) {
            break;
          } else {
            step = 1;
          }
        } else {
          step *= 2;
        }
      }//end of while
    }
  } else {
    // between ... and ...
    int64_t interval = 0;
    bool is_null = false;
    if (is_nmb_literal) {
      if (OB_ISNULL(between_value_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("between_value_expr is unexpected", KPC(between_value_expr), K(ret));
      } else if (OB_UNLIKELY(lib::is_mysql_mode() && is_rows && !between_value_expr->obj_meta_.is_integer_type())) {
        ret = OB_ERR_WINDOW_FRAME_ILLEGAL;
        LOG_WARN("frame start or end is negative, NULL or of non-integral type", K(ret), K(between_value_expr->obj_meta_));
      } else if (OB_FAIL(get_param_int_value(*between_value_expr, eval_ctx_, is_null, interval, false, lib::is_mysql_mode()))) {
        LOG_WARN("get_param_int_value failed", K(ret), KPC(between_value_expr));
      } else if (is_null) {
        got_null_val = true;
      } else if (interval < 0) {
        ret = OB_DATA_OUT_OF_RANGE;
        LOG_WARN("invalid argument", K(ret), K(interval));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (is_rows) {
    // range or rows with expr
      if (OB_UNLIKELY(!is_preceding && static_cast<uint64>(row_idx + interval) > INT64_MAX)) {
        if (lib::is_mysql_mode()) {
          ret = OB_ERR_WINDOW_FRAME_ILLEGAL;
          LOG_WARN("frame start or end is negative, NULL or of non-integral type", K(ret), K(row_idx + interval));
        } else {
          ret = OB_DATA_OUT_OF_RANGE;
          LOG_WARN("int64 out of range", K(ret), K(row_idx + interval));
        }
      } else {
        pos = is_preceding ? row_idx - interval : row_idx + interval;
      }
    } else if (wf_cell.wf_info_.sort_exprs_.count() == 0) {
      //Only when order by is const, sort_exprs_.count() == 0
      ObDatum *cmp_result = NULL;
      ObExpr *bound_expr = (is_upper ? wf_cell.wf_info_.upper_.range_bound_expr_
                                     : wf_cell.wf_info_.lower_.range_bound_expr_);
      if (OB_ISNULL(bound_expr) || !ob_is_integer_type(bound_expr->datum_meta_.get_type())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(ret), K(bound_expr));
      } else if (OB_FAIL(bound_expr->eval(eval_ctx_, cmp_result))) {
        LOG_WARN("calc compare value failed", K(ret));
      } else {
        bool match = cmp_result->is_null() || cmp_result->get_bool();
        if (match) {
          pos = is_upper ? wf_cell.part_first_row_idx_ : get_part_end_idx();
        } else {
          pos = is_upper ? get_part_end_idx() + 1 : wf_cell.part_first_row_idx_ - 1;
        }
      }
    } else if (wf_cell.wf_info_.sort_exprs_.count() != 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("only need one sort_exprs", K(ret));
    } else {
      // range
      /* 这个地方有点绕，举个例子，对于order by v range between x preceding and y following
        语义上是(v +/- x) <= v <= (v +/- y)之间的所有行
        +/-由preceding和序的方向共同决定, 同或-，异或+
        编码上，我们就需要找出大于等于(v +/- x)的最小值的行和小于等于(v +/- y)的最大值的行
      */
      const bool is_ascending_ = wf_cell.wf_info_.sort_collations_.at(0).is_ascending_;
      const int64_t cell_idx = wf_cell.wf_info_.sort_collations_.at(0).field_idx_;

      const static int L = 1;
      const static int LE = 1 << 1;
      const static int G = 1 << 2;
      const static int GE = 1 << 3;
      const static int ROLL = L | G;

      int cmp_mode = !(is_preceding ^ is_ascending_) ? L : G;
      if (is_preceding ^ is_upper) {
        cmp_mode = cmp_mode << 1;
      }
      pos = row_idx;
      bool re_direction = false;
      int step = cmp_mode & ROLL ? 1 : 0;
      int64_t cmp_times = 0;
      ObDatum *cmp_val = NULL;

      ObExpr *bound_expr = (is_upper ? wf_cell.wf_info_.upper_.range_bound_expr_
                                     : wf_cell.wf_info_.lower_.range_bound_expr_);
      if (OB_ISNULL(bound_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(ret), K(bound_expr));
      } else if (OB_FAIL(bound_expr->eval(eval_ctx_, cmp_val))) {
        LOG_WARN("calc compare value failed", K(ret));
      } else if (lib::is_mysql_mode() &&
                 !is_nmb_literal &&
                 ob_is_temporal_type(bound_expr->datum_meta_.get_type())) {
        if (OB_FAIL(check_interval_valid(*bound_expr))) {
          LOG_WARN("failed to check interval valid", K(ret));
        }
      }
      ObSortFuncs &sort_cmp_funcs = wf_cell.wf_info_.sort_cmp_funcs_;
      ObDatumCmpFuncType cmp_func = sort_cmp_funcs.at(0).cmp_func_;
      while (OB_SUCC(ret)) {
        cmp_times++;
        ObDatum cur_val;
        bool match = false;
        const ObRADatumStore::StoredRow *a_row = NULL;
        (is_preceding ^ re_direction) ? (pos -= step) : (pos += step);
        const bool overflow = (is_preceding ^ re_direction)
                              ? pos < wf_cell.part_first_row_idx_
                              : pos > get_part_end_idx();
        if (overflow) {
          match = true;
        } else if (OB_FAIL(row_reader.get_row(pos, a_row))) {
          LOG_WARN("failed to get row", K(ret), K(pos));
        } else if (FALSE_IT(cur_val = a_row->cells()[cell_idx])) {
          // will not reach here
        } else {
          int cmp_result = 0;
          int tmp_ret = cmp_func(cur_val, *cmp_val, cmp_result);
          match = ((cmp_mode & L) && cmp_result < 0)
                  || ((cmp_mode & LE) && cmp_result <= 0)
                  || ((cmp_mode & G) && cmp_result > 0)
                  || ((cmp_mode & GE) && cmp_result >= 0);

          ObToStringDatum cur_val1(*bound_expr, cur_val);
          ObToStringDatum cmp_val1(*bound_expr, *cmp_val);
          LOG_DEBUG("cmp result", K(tmp_ret), K(pos), K(cell_idx), K(cmp_times), K(cur_val), KPC(cmp_val),
                    K(cmp_mode),K(cmp_result), K(match),
                    "cur_val1", ObToStringDatum(*bound_expr, cur_val),
                    "cmp_val1", ObToStringDatum(*bound_expr, *cmp_val));
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
                (is_preceding ^ re_direction) ? pos += step : pos -= step;
              }
              break;
            } else {
              (is_preceding ^ re_direction) ? pos += step : pos -= step;
              step = 1;
            }
          } else {
            step = 0 == step ? 1 : (2 * step);
          }
        }
      }//end of while
    }
  }
  return ret;
}

int ObWindowFunctionOp::collect_result(const int64_t idx, ObDatum &in_datum, WinFuncCell &wf_cell)
{
  int ret = OB_SUCCESS;
  const ObRADatumStore::StoredRow *prev_stored_row = NULL;
  int64_t result_datum_cnt = 0;
  if (OB_UNLIKELY(curr_row_collect_values_.count() != wf_list_.get_size())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("size is not equal", K(curr_row_collect_values_.count()), K(wf_list_.get_size()),
                                  K(ret));
  } else {
    if (wf_cell.get_prev() != wf_list_.get_header()) {
      WinFuncCell *prev_wf_cell = wf_cell.get_prev();
      result_datum_cnt += prev_wf_cell->wf_idx_;
      if (OB_FAIL(prev_wf_cell->res_.cur_->get_row(idx,
                                                   prev_stored_row))) {
        LOG_WARN("failed to get row", K(ret), K(idx), KPC(prev_wf_cell));
      } else if (OB_ISNULL(prev_stored_row)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("prev_stored_row is null", K(idx), K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < prev_stored_row->cnt_; ++i) {
          ObDatum &last_value = curr_row_collect_values_.at(i);
          last_value = prev_stored_row->cells()[i];
        }
      }
    }
    curr_row_collect_values_.at(result_datum_cnt++) = in_datum;
  }
  if (OB_SUCC(ret)) {
    const ObArrayHelper<ObDatum> tmp_array(result_datum_cnt,
                                           curr_row_collect_values_.get_data(),
                                           result_datum_cnt);
    if (OB_FAIL(wf_cell.res_.cur_->add_row(tmp_array))) {
      LOG_WARN("add row failed", K(ret));
    } else {
      LOG_DEBUG("succ to collect_result",
          K(idx), K(in_datum), KPC(prev_stored_row), K(tmp_array), K(wf_cell));
    }
  }
  return ret;
}


int ObWindowFunctionSpec::register_to_datahub(ObExecContext &ctx) const
{
  int ret = OB_SUCCESS;
  if (single_part_parallel_) {
    if (OB_ISNULL(ctx.get_sqc_handler())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null unexpected", K(ret));
    } else {
      void *buf = ctx.get_allocator().alloc(sizeof(ObWinbufWholeMsg::WholeMsgProvider));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        ObWinbufWholeMsg::WholeMsgProvider *provider =
          new (buf)ObWinbufWholeMsg::WholeMsgProvider();
        ObSqcCtx &sqc_ctx = ctx.get_sqc_handler()->get_sqc_ctx();
        if (OB_FAIL(sqc_ctx.add_whole_msg_provider(get_id(), dtl::DH_WINBUF_WHOLE_MSG, *provider))) {
          LOG_WARN("fail add whole msg provider", K(ret));
        }
      }
    }
  } else if (range_dist_parallel_) {
    auto provider = OB_NEWx(ObRDWFWholeMsg::WholeMsgProvider, (&ctx.get_allocator()));
    OV(NULL != provider, OB_ALLOCATE_MEMORY_FAILED);
    auto handler = ctx.get_sqc_handler();
    CK(NULL != handler);
    OZ(handler->get_sqc_ctx().add_whole_msg_provider(get_id(), dtl::DH_RANGE_DIST_WF_PIECE_MSG, *provider));
  } else if (is_participator()) {
    auto provider = OB_NEWx(ObReportingWFWholeMsg::WholeMsgProvider, (&ctx.get_allocator()));
    OV(NULL != provider, OB_ALLOCATE_MEMORY_FAILED);
    auto handler = ctx.get_sqc_handler();
    CK(NULL != handler);
    OZ(handler->get_sqc_ctx().add_whole_msg_provider(
       get_id(), dtl::DH_SECOND_STAGE_REPORTING_WF_WHOLE_MSG, *provider));
  }
  return ret;
}

int ObWindowFunctionOp::found_new_part(const bool update_part_first_row_idx)
{
  int ret = OB_SUCCESS;

  // save_partition_by_exprs_and_part_idx
  WinFuncCell *end = wf_list_.get_header();
  for (WinFuncCell *wf = wf_list_.get_first(); OB_SUCC(ret) && wf != end; wf = wf->get_next()) {
    if (update_part_first_row_idx) {
      wf->part_first_row_idx_ = wf->res_.cur_->count();
    }
    if (!wf->wf_info_.partition_exprs_.empty()) {
      if (OB_FAIL(wf->part_values_.save_store_row(
                  wf->wf_info_.partition_exprs_, eval_ctx_))) {
        LOG_WARN("save current partition values failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && MY_SPEC.is_participator()) {
    if (OB_FAIL(detect_aggr_status())) {
      LOG_WARN("fail to detect_aggr_status", K(ret));
    }
  }

  return ret;
}

int ObWindowFunctionOp::save_part_first_row_idx()
{
  int ret = OB_SUCCESS;
  WinFuncCell *end = wf_list_.get_header();
  for (WinFuncCell *wf = wf_list_.get_first(); OB_SUCC(ret) && wf != end; wf = wf->get_next()) {
    wf->part_first_row_idx_ = wf->res_.cur_->count();
  }
  return ret;
}

// when get a row of next partition of the first window function, check whether the other window
// functions go to next partition.
int ObWindowFunctionOp::check_wf_same_partition(WinFuncCell *&end)
{
  int ret = OB_SUCCESS;
  end = wf_list_.get_header();
  WinFuncCell &first = *wf_list_.get_first();
  for (WinFuncCell *wf = first.get_next();
               OB_SUCC(ret) && wf != wf_list_.get_header();
               wf = wf->get_next()) {
    bool same = false;
    if (OB_FAIL(check_same_partition(*wf, same))) {
      LOG_WARN("check same partition failed", K(ret));
    } else if (same) {
      end = wf;
      break;
    }
  }
  return ret;
}

int ObWindowFunctionOp::detect_computing_method(
    const int64_t row_idx, const int64_t wf_idx,
    bool &is_result_datum_null, bool &is_pushdown_bypass)
{
  int ret = OB_SUCCESS;

  if (MY_SPEC.is_push_down()) {
    const ObRADatumStore::StoredRow *row = NULL;
    is_pushdown_bypass = false;
    is_result_datum_null = false;
    int64_t aggr_status = 0;
    if (OB_FAIL(input_rows_.cur_->get_row(row_idx, row))) {
      LOG_WARN("failed to get row", K(ret), K(row_idx));
    } else if (FALSE_IT(clear_evaluated_flag())) {
    } else if (OB_FAIL(row->to_expr(get_all_expr(), eval_ctx_))) {
      LOG_WARN("Failed to to_expr", K(ret));
    } else if (FALSE_IT(aggr_status
                        = MY_SPEC.wf_aggr_status_expr_->locate_expr_datum(eval_ctx_).get_int())) {
    } else if (MY_SPEC.is_participator()) {
      if (aggr_status >= 0) { // original row
        if (aggr_status < wf_idx) {
          is_result_datum_null = true;
        } else {
          is_result_datum_null = false;
          is_pushdown_bypass = true;
        }
      } else if (aggr_status < 0) { // aggr result row, don't calc, only set result
        is_result_datum_null = aggr_status == -wf_idx ? false : true;
        is_pushdown_bypass = false;
      }
    } else if (MY_SPEC.is_consolidator()) {
      if (aggr_status < 0) { // don't need to compute and output, set result null directly
        is_result_datum_null = true;
      }
    }
  }

  return ret;
}

// check_wf_same_partition checked wfs from first to end have get a complete partition,
// compute wf values for them.
int ObWindowFunctionOp::compute_wf_values(const WinFuncCell *end, int64_t &check_times)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(eval_ctx_);
  batch_info_guard.set_batch_idx(0);
  batch_info_guard.set_batch_size(1);
  const uint64_t tenant_id = ctx_.get_my_session()->get_effective_tenant_id();
  WinFuncCell *first = wf_list_.get_first();
  int64_t prev_wf_pby_expr_count = -1; // prev_wf_pby_expr_count transmit to datahub
  for (WinFuncCell *wf = first; OB_SUCC(ret) && wf != end; wf = wf->get_next()) {
    wf->reset_for_restart();
    ObDatum result_datum;
    RowsReader row_reader(*input_rows_.cur_);
    if (wf == wf_list_.get_last()) {
      // record the last computed partition row count
      const int v = input_rows_.cur_->count() - wf->part_first_row_idx_;
      if (v > 0) {
        last_computed_part_rows_ = v;
      }
    }
    bool is_pushdown_bypass = false;
    bool is_result_datum_null = false;
    for (int64_t i = wf->part_first_row_idx_;
         OB_SUCC(ret) && i < input_rows_.cur_->count();
         ++i) {
      // we should check status interval since this loop will occupy cpu!
      // TODO: not check whether need check status for each row.
      if (0 == ++check_times % CHECK_STATUS_INTERVAL) {
        if (OB_FAIL(ctx_.check_status())) {
          break;
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(detect_computing_method(
          i, wf->wf_idx_, is_result_datum_null, is_pushdown_bypass))) {
        LOG_WARN("Failed to detect_computing_method", K(ret), K(i), K(wf->wf_idx_));
      }
      if (OB_FAIL(ret)) {
      } else if (is_result_datum_null) {
        result_datum.set_null();
      } else if (is_pushdown_bypass) {
        if (OB_FAIL(compute_push_down_by_pass(*wf, result_datum))) {
          // don't need to compute, if aggr is count, res is 1; else res is param
          LOG_WARN("compute_push_down_by_pass failed", K(ret));
        }
      } else if (OB_FAIL(compute(row_reader, *wf, i, result_datum))) { // real compute
        LOG_WARN("compute failed", K(ret));
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(collect_result(i, result_datum, *wf))) {
        LOG_WARN("collect_result failed", K(ret), K(i));
      }
    }
    // free prev buf, because result of all prev wf has been copied to this wf's part_rows_store
    if (OB_SUCC(ret) && first != wf) {
      wf->get_prev()->res_.cur_->reset_buf(tenant_id);
    }
  }
  // Row project flag is set when read row from RADatumStore, but the remain rows in batch
  // are not evaluated, need reset the flag here.
  clear_evaluated_flag();

  return ret;
}

int ObWindowFunctionOp::calc_part_exprs_hash(
    const common::ObIArray<ObExpr *> *exprs_,
    const ObChunkDatumStore::StoredRow *row_,
    uint64_t &hash_value)
{
  int ret = OB_SUCCESS;
  hash_value = 99194853094755497L;
  if (OB_ISNULL(exprs_) || OB_ISNULL(row_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("exprs_ or row_ is null", K(ret), K(exprs_), K(row_));
  } else {
    ObExpr *expr = NULL;
    ObDatum *datum = NULL;
    for (int64_t i = 0; i < exprs_->count(); i++) {
      if (OB_ISNULL(expr = exprs_->at(i)) || OB_ISNULL(expr->basic_funcs_)) {
      } else {
        if (OB_ISNULL(row_)) {
          if (OB_FAIL(expr->eval(eval_ctx_, datum))) {
            LOG_WARN("eval expr failed", K(ret), KPC(expr));
          }
        } else {
          datum = const_cast<ObDatum *>(&row_->cells()[i]);
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(expr->basic_funcs_->murmur_hash_v2_(*datum, hash_value, hash_value))) {
            LOG_WARN("do hash failed", K(ret), KPC(expr));
          }
        }
      }
    }
  }
  return ret;
}

int ObWindowFunctionOp::get_participator_whole_msg(
    const PbyHashValueArray &pby_hash_value_array, ObReportingWFWholeMsg &whole)
{
  int ret = OB_SUCCESS;
  const ObReportingWFWholeMsg *temp_whole_msg = NULL;
  ObPxSqcHandler *handler = ctx_.get_sqc_handler();
  if (OB_ISNULL(handler)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("parallel winbuf only supported in parallel execution mode",
        K(MY_SPEC.single_part_parallel_));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "parallel reporting_wf in non-px mode");
  } else {
    ObPxSQCProxy &proxy = handler->get_sqc_proxy();
    ObReportingWFPieceMsg piece;
    piece.op_id_ = MY_SPEC.id_;
    piece.thread_id_ = GETTID();
    piece.source_dfo_id_ = proxy.get_dfo_id();
    piece.target_dfo_id_ = proxy.get_dfo_id();
    piece.pby_hash_value_array_ = pby_hash_value_array;
    if (OB_SUCC(ret)) {
      if (OB_FAIL(proxy.get_dh_msg_sync(MY_SPEC.id_, dtl::DH_SECOND_STAGE_REPORTING_WF_WHOLE_MSG,
                  piece, temp_whole_msg, ctx_.get_physical_plan_ctx()->get_timeout_timestamp()))) {
        LOG_WARN("fail to get reporting wf whole msg", K(ret), K(piece), KPC(temp_whole_msg));
      } else if (OB_ISNULL(temp_whole_msg)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("whole msg is unexpected", K(ret));
      } else if (OB_FAIL(whole.assign(*temp_whole_msg))) {
        LOG_WARN("fail to assign msg", K(ret));
      }
      if (OB_SUCC(ret)) {
        ObWindowFunctionOpInput *op_input = static_cast<ObWindowFunctionOpInput*>(input_);
        ObPxDatahubDataProvider *provider = nullptr;
        ObReportingWFWholeMsg::WholeMsgProvider *whole_msg_provider = nullptr;
        if (OB_FAIL(proxy.sqc_ctx_.get_whole_msg_provider(
                    MY_SPEC.id_, dtl::DH_SECOND_STAGE_REPORTING_WF_WHOLE_MSG, provider))) {
          LOG_WARN("fail get provider", K(ret));
        } else if (FALSE_IT(whole_msg_provider =
            static_cast<ObReportingWFWholeMsg::WholeMsgProvider *>(provider))) {
        } else if (OB_FAIL(op_input->sync_wait(ctx_, whole_msg_provider))) {
          LOG_WARN("fail to sync_wait", K(ret));
        }
      }

    }
  }
  return ret;
}

int ObWindowFunctionOp::participator_coordinate(
    const int64_t pushdown_wf_idx, const ExprFixedArray &pby_exprs)
{
  int ret = OB_SUCCESS;
  ObReportingWFWholeMsg *whole_msg = participator_whole_msg_array_.at(pushdown_wf_idx);
  const PbyHashValueArray *pby_hash_value_array = pby_hash_values_.at(pushdown_wf_idx);
  ReportingWFHashSet *pushdown_pby_hash_set = pby_hash_values_sets_.at(pushdown_wf_idx);
  if (OB_ISNULL(whole_msg) || OB_ISNULL(pby_hash_value_array) || OB_ISNULL(pushdown_pby_hash_set)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is null", K(ret),
        K(pushdown_wf_idx), K(whole_msg), K(pby_hash_value_array), K(pushdown_pby_hash_set));
  } else if (OB_FAIL(get_participator_whole_msg(*pby_hash_value_array, *whole_msg))) {
    LOG_WARN("fail to get whole msg", K(ret));
  } else if (0 == pby_hash_value_array->count()) {
    // no more new row, do nothing
    LOG_WARN("not fetch any row, do nothing", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < whole_msg->pby_hash_value_array_.count(); ++i) {
      if (OB_FAIL(pushdown_pby_hash_set->set_refactored_1(whole_msg->pby_hash_value_array_.at(i), 1))) {
        LOG_WARN("Failed to insert to hashmap", K(ret), K(i), K(whole_msg->pby_hash_value_array_.at(i)));
      }
    }
  }
  return ret;
}

// Store from store_begin_idx to store_begin_idx + store_num
// Skip rows of beginning that have been stored already but haven't been restore
int ObWindowFunctionOp::store_all_expr_datums(int64_t store_begin_idx, int64_t store_num)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(store_begin_idx > restore_row_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup interval not continuous", K(ret), K(store_begin_idx), K(restore_row_cnt_));
  } else if (restore_row_cnt_ < store_begin_idx + store_num) {
    const ObIArray<ObExpr *> &all_expr = get_all_expr();
    int64_t memcpy_length = (store_begin_idx + store_num - restore_row_cnt_) * sizeof(ObDatum);
    if (OB_LIKELY(memcpy_length > 0)) {
      ObIArray<ObDatum*> &src_datums = all_expr_datums_;
      ObIArray<ObDatum*> &dest_datums = all_expr_datums_copy_;
      for (int64_t i = 0; i < all_expr_datums_.count(); i++) {
        const bool expr_batch_result = all_expr.at(i)->is_batch_result();
        ObDatum *src_datum = src_datums.at(i) + restore_row_cnt_;
        ObDatum *dest_datum = dest_datums.at(i) + restore_row_cnt_;
        if (expr_batch_result) {
          MEMCPY(dest_datum, src_datum, memcpy_length);
        } else if (0 == store_begin_idx && store_num >= 1) {
          MEMCPY(dest_datum, src_datum, sizeof(ObDatum));
        }
      }
      restore_row_cnt_ = store_num + store_begin_idx;
    }
  }
  return ret;
}

void ObWindowFunctionOp::restore_all_expr_datums()
{
  int64_t memcpy_length = restore_row_cnt_ * sizeof(ObDatum);
  if (OB_LIKELY(memcpy_length > 0)) {
    const ObIArray<ObExpr *> &all_expr = get_all_expr();
    ObIArray<ObDatum*> &src_datums = all_expr_datums_copy_;
    ObIArray<ObDatum*> &dest_datums = all_expr_datums_;
    for (int64_t i = 0; i < all_expr_datums_.count(); i++) {
      ObDatum *src_datum = src_datums.at(i);
      ObDatum *dest_datum = dest_datums.at(i);
      const bool expr_batch_result = all_expr.at(i)->is_batch_result();
      MEMCPY(dest_datum, src_datum, expr_batch_result ? memcpy_length : sizeof(ObDatum));
      restore_row_cnt_ = 0;
    }
  }
}

int64_t ObWindowFunctionOp::next_nonskip_row_index(int64_t cur_idx, const ObBatchRows &child_brs)
{
  int64_t res = cur_idx + 1;
  const ObBitVector &skip = *child_brs.skip_;
  for(; res < child_brs.size_; res++) {
    if (!skip.at(res)) {
      break;
    }
  }
  return res;
}

// Store all_expr datums after get next batch from child, and restore them before get next batch from child.
// Since we keep batch_size_ = 0 and only modify first row between two get_next_batch_from_child,
// we only need to store the first row after get next batch from child and restore it before next call.
int ObWindowFunctionOp::get_next_batch_from_child(int64_t batch_size, const ObBatchRows *&child_brs)
{
  int ret = OB_SUCCESS;

  restore_all_expr_datums();
  // get next batch which is not all skipped.
  bool found = false;
  while (!found && OB_SUCC(ret)) {
    clear_evaluated_flag();
    if (OB_FAIL(child_->get_next_batch(batch_size, child_brs))) {
      LOG_WARN("get child next batch failed", K(ret));
    } else if (child_brs->end_) {
      found = true;
    } else {
      int64_t size = child_brs->size_;
      found = child_brs->skip_->accumulate_bit_cnt(size) != size;
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(store_all_expr_datums(0, 1))) {
  // only backup the first row, because NonAggrCellNthValue::eval will change datum ptr of this row
    LOG_WARN("store all expr datums failed", K(ret));
  }
  return ret;
}

// get next big partition, big means it's a complete partition for last wf.
// When find a big partition, store remaining rows in the batch from child in:
// - %processed_ row store, if %processed_ is empty, and we will swap %processed_ and %cur_ later.
// - %cur_ row store, if %processed_ is not empty
// so, the remaining rows are always in %cur_ rows after the function.
int ObWindowFunctionOp::get_next_partition(int64_t &check_times)
{
  int ret = OB_SUCCESS;
  int64_t batch_size = MY_SPEC.max_batch_size_;
  bool found_next_part = false;
  const ObBatchRows *child_brs = nullptr;
  WinFuncCell *first = wf_list_.get_first();
  WinFuncCell *end = wf_list_.get_header();
  RowsStore &current = *input_rows_.cur_;
  bool first_batch = 0 == current.count();
  int64_t row_idx = -1;

  if (child_iter_end_) {
    if (!first_batch) {
      if (OB_FAIL(found_part_end(end, input_rows_.cur_))) {
        // add aggr result row for the last part
        LOG_WARN("found_part_end failed", K(ret), K(last_aggr_status_));
      } else if (OB_FAIL(compute_wf_values(end, check_times))) {
        LOG_WARN("compute wf values failed", K(ret));
      }
    }
  } else if (OB_FAIL(get_next_batch_from_child(batch_size, child_brs))) {
    LOG_WARN("get child next batch failed", K(ret));
  } else if ((child_brs->end_ && 0 == child_brs->size_)
             || child_brs->size_ == (row_idx = next_nonskip_row_index(row_idx, *child_brs))) {
    child_iter_end_ = true;
    if (!first_batch) {
      if (OB_FAIL(found_part_end(end, input_rows_.cur_))) {
        LOG_WARN("found_part_end failed", K(ret), K(last_aggr_status_));
      } else if (OB_FAIL(compute_wf_values(end, check_times))) {
        LOG_WARN("compute wf values failed", K(ret));
      }
    }
  } else {
    if (child_brs->end_) {
      child_iter_end_ = true;
    }
    ObEvalCtx::BatchInfoScopeGuard guard(eval_ctx_);
    guard.set_batch_size(child_brs->size_);
    guard.set_batch_idx(row_idx);
    if (first_batch && child_brs->size_ > 0) {
      if (OB_FAIL(found_new_part(true))) {
        LOG_WARN("store partition exprs datum failed", K(ret));
      } else if (OB_FAIL(current.add_row_with_index(row_idx++,
                                                    get_all_expr(), &eval_ctx_, NULL))) {
        LOG_WARN("push first row in rows_store failed", K(ret));
      } else if (FALSE_IT(restore_row_cnt_ = 0)) {
      } else if (FALSE_IT(store_all_expr_datums(0, 1))) {
        // defense code: store valid datum for first time after eval in "add_row_with_index".
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(process_child_batch(row_idx, child_brs, check_times))) {
      LOG_WARN("add row to rows_store failed", K(ret));
    }
  }
  return ret;
}

int ObWindowFunctionOp::process_child_batch(
    const int64_t batch_idx,
    const ObBatchRows *child_brs,
    int64_t &check_times)
{
  int ret = OB_SUCCESS;
  int64_t batch_size = MY_SPEC.max_batch_size_;
  int64_t row_idx = batch_idx;
  WinFuncCell *first = wf_list_.get_first();
  WinFuncCell *end = wf_list_.get_header();
  ObEvalCtx::BatchInfoScopeGuard guard(eval_ctx_);
  guard.set_batch_size(child_brs->size_);
  guard.set_batch_idx(batch_idx);
  bool found_next_part = false;
  OZ(check_stack_overflow());

  while (OB_SUCC(ret) && !found_next_part) { // handle current batch
    const ObBitVector &skip = *child_brs->skip_;
    bool need_loop_until_child_brs_end = true;
    while (OB_SUCC(ret) && need_loop_until_child_brs_end) {
      need_loop_until_child_brs_end = false;
      found_next_part = false;
      RowsStore &current = *input_rows_.cur_;
      RowsStore &processed = *input_rows_.processed_;
      // When find a big partition, store remaining rows of the batch to %remain row store
      RowsStore &remain = processed.is_empty() ? processed : current;
      const bool need_split_store = processed.is_empty();
      while (OB_SUCC(ret) && row_idx < child_brs->size_ && !found_next_part) {
        bool same_part = false;
        guard.set_batch_idx(row_idx);
        int64_t row_cnt_inc = 0;
        if (skip.contain(row_idx)) {
          ++row_idx;
          continue;
        } else if (OB_FAIL(check_same_partition(*first, same_part))) {
          LOG_WARN("check same partition failed", K(ret));
        } else if (!same_part) {
          if (OB_FAIL(check_wf_same_partition(end))) {
            LOG_WARN("check wf same partition failed", K(ret));
          } else if (OB_FAIL(found_part_end(end, input_rows_.cur_))) {
            LOG_WARN("found_part_end failed", K(ret));
          } else if (end != wf_list_.get_header()) {
            if (OB_FAIL(found_new_part(false))) {
              LOG_WARN("save partition by exprs failed", K(ret));
            } else if (OB_FAIL(current.add_row_with_index(row_idx, get_all_expr(),
                                                          &eval_ctx_, NULL, false))) {
              LOG_WARN("add row to rows_store failed", K(ret));
            } else {
              // the biggest part end : remain row_cnt_ + 0, store_row_cnt_ + 1
              // other parts end : after compute_wf_values, current row_cnt_ + 1, store_row_cnt_ + 1
              row_cnt_inc = 1;
            }
          } else {
            found_next_part = true;
            if (OB_FAIL(found_new_part(false))) {
              LOG_WARN("save partition by exprs failed", K(ret));
            } else if (OB_FAIL(remain.add_row_with_index(row_idx, get_all_expr(), &eval_ctx_,
                                                  NULL, false))) {
              LOG_WARN("add row to rows_store failed", K(ret));
            }
          }
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(compute_wf_values(end, check_times))) {
            LOG_WARN("compute wf values failed", K(ret));
          } else if (OB_FAIL(save_part_first_row_idx())) {
            LOG_WARN("save partition by exprs failed", K(ret));
          } else {
            // compute_wf_values will use current.row_cnt to decide which rows need to compute
            // so add row_cnt_inc to current.row_cnt after compute_wf_values
            current.row_cnt_ += row_cnt_inc;
          }
        } else { // same part
          if (MY_SPEC.is_participator()) {
            MY_SPEC.wf_aggr_status_expr_->locate_datum_for_write(eval_ctx_).set_int(last_aggr_status_);
          }
          if (OB_FAIL(current.add_row_with_index(row_idx, get_all_expr(), &eval_ctx_))) {
            LOG_WARN("add row to rows_store failed", K(ret));
          }
        }
        if (OB_SUCC(ret) && !found_next_part) {
          ++row_idx;
        }
      }
      if (OB_SUCC(ret)) {
        if (found_next_part) {
          if (need_split_store) {
            // switch %cur_ and %processed_ row store if necessary
            // %cur_ always be rows store we are computing.
            // And rows in %processed_ row store are all computed and ready to output.
            // swap $cur_ and $processed_ row store
            foreach_stores([](Stores &s){ std::swap(s.cur_, s.processed_); return OB_SUCCESS; });
            if (MY_SPEC.range_dist_parallel_ && !first_part_saved_) {
              first_part_saved_ = true;
              foreach_stores([](Stores &s){ std::swap(s.first_, s.processed_); return OB_SUCCESS; });
            }
          }
          ++row_idx;
          ++remain.row_cnt_;
          if (need_split_store && OB_FAIL(save_part_first_row_idx())) {
            LOG_WARN("save partition by exprs failed", K(ret));
          } else { // need to deal with remain rows in this child_brs after found new part
            need_loop_until_child_brs_end = true;
          }
        } else if (!child_iter_end_) {
          if (need_split_store) { // need_split_store is true means we have not found next part ever
            if (OB_FAIL(get_next_batch_from_child(batch_size, child_brs))) {
              LOG_WARN("get child next batch failed", K(ret));
            } else {
              child_iter_end_ = child_brs->end_;
              row_idx = 0;
              guard.set_batch_size(child_brs->size_);
            }
          } else {
            found_next_part = true;
            current.row_cnt_ = wf_list_.get_last()->part_first_row_idx_;
          }
        } else { // child_iter_end_
          found_next_part = true;
          // add aggr result row for the last part
          if (OB_FAIL(found_part_end(wf_list_.get_header(), input_rows_.cur_))) {
            LOG_WARN("found_part_end failed", K(ret), K(last_aggr_status_));
          } else if (OB_FAIL(compute_wf_values(wf_list_.get_header(), check_times))) {
            LOG_WARN("compute wf values failed", K(ret));
          }
        }
      }
    }
  }

  return ret;
}

int ObWindowFunctionOp::output_rows_store_rows(const int64_t output_row_cnt,
                                               RowsStore &rows_store,
                                               RowsStore &part_rows_store,
                                               ObEvalCtx::BatchInfoScopeGuard &guard)
{
  int ret = OB_SUCCESS;
  const ObRADatumStore::StoredRow *child_row = NULL;
  const ObRADatumStore::StoredRow *result_row = NULL;
  int64_t batch_idx_offset = eval_ctx_.get_batch_idx();
  for (int64_t i = 0; i < output_row_cnt && OB_SUCC(ret);
       i++, guard.set_batch_idx(batch_idx_offset + i)) {
    const int64_t idx = i + rows_store.output_row_idx_;
    if (OB_FAIL(rows_store.get_row(idx, child_row))) {
      LOG_WARN("get row failed", K(ret));
    } else if (OB_FAIL(child_row->to_expr(get_all_expr(), eval_ctx_))) {
      LOG_WARN("row to expr failed", K(ret));
    } else if (MY_SPEC.is_consolidator()
               && MY_SPEC.wf_aggr_status_expr_->locate_expr_datum(eval_ctx_).get_int() < 0) {
      brs_.skip_->set(eval_ctx_.get_batch_idx());
      continue;
    } else if (OB_FAIL(part_rows_store.get_row(
               MY_SPEC.single_part_parallel_ ? 0 : idx, result_row))) {
      LOG_WARN("get row failed", K(ret), K(MY_SPEC.single_part_parallel_), K(idx));
    } else {
      LOG_DEBUG("get result row", K(result_row), KPC(result_row));
      WinFuncCell *tmp_wf_cell = wf_list_.get_first();
      for (int64_t j = 0;
            j < result_row->cnt_ && tmp_wf_cell != NULL;
            ++j, tmp_wf_cell = tmp_wf_cell->get_next()) {
        tmp_wf_cell->wf_info_.expr_->locate_expr_datum(eval_ctx_) = result_row->cells()[j];
      }
    }
  }
  rows_store.output_row_idx_ += output_row_cnt;
  part_rows_store.output_row_idx_ += output_row_cnt;
  return ret;
}

int ObWindowFunctionOp::output_batch_rows(const int64_t output_row_cnt)
{
  int ret = OB_SUCCESS;
  // restore rows from rows stores to datums. output the processed rows_store first.
  RowsStore &processed = *input_rows_.processed_;
  RowsStore &current = *input_rows_.cur_;
  WinFuncCell &wf_cell = *wf_list_.get_last();
  RowsStore &current_res = *wf_cell.res_.cur_;
  RowsStore &processed_res = *wf_cell.res_.processed_;

  if (OB_UNLIKELY(processed.stored_row_cnt_ != processed.row_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rows in processed rows store should be all computed", K(ret), K(processed));
  } else if (processed.row_cnt_ != processed_res.row_cnt_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("count of rows in rows store and part row store should be same",
             K(ret), K(processed), K(processed_res));
  } else if (current.row_cnt_ != current_res.row_cnt_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("count of rows in rows store and part row store should be same",
             K(ret), K(current), K(current_res));
  } else {
    int64_t rows_cnt_processed = MIN(processed.to_output_rows(), output_row_cnt);
    int64_t rows_cnt_current = MIN(current.to_output_rows(), output_row_cnt - rows_cnt_processed);
    ObEvalCtx::BatchInfoScopeGuard guard(eval_ctx_);
    guard.set_batch_idx(0);
    LOG_DEBUG("start to output2", K(rows_cnt_processed), K(rows_cnt_current), K(output_row_cnt));
    // the first row has been stored before calc, store second row to last row
    OZ(store_all_expr_datums(1, rows_cnt_processed + rows_cnt_current - 1));
    output_rows_it_age_.inc();
    OZ(foreach_stores([&](Stores &s) { return set_it_age(s); }));
    OZ(output_rows_store_rows(rows_cnt_processed, processed, processed_res, guard));
    OZ(output_rows_store_rows(rows_cnt_current, current, current_res, guard));
    OZ(foreach_stores([&](Stores &s) { return unset_it_age(s); }));
    if (OB_SUCC(ret) && eval_ctx_.get_batch_idx() > 0) {
      DLIST_FOREACH(func, wf_list_) {
        ObExpr *expr = func->wf_info_.expr_;
        expr->get_eval_info(eval_ctx_).cnt_ = eval_ctx_.get_batch_idx();
        expr->set_evaluated_projected(eval_ctx_);
      }
      if (MY_SPEC.is_participator()) {
        MY_SPEC.wf_aggr_status_expr_->get_eval_info(eval_ctx_).cnt_ = eval_ctx_.get_batch_idx();
        MY_SPEC.wf_aggr_status_expr_->set_evaluated_projected(eval_ctx_);
      }
    }
    if (OB_SUCC(ret)) {
      ObExprPtrIArray &all_exprs = get_all_expr();
      for (int64_t i = 0; i < all_exprs.count(); i++) {
        ObExpr *expr = all_exprs.at(i);
        expr->get_eval_info(eval_ctx_).cnt_ = eval_ctx_.get_batch_idx();
        expr->set_evaluated_projected(eval_ctx_);
      }
    }
    if (OB_SUCC(ret)) {
      if (eval_ctx_.get_batch_idx() > 0) {
        ObExprPtrIArray &all_exprs = get_all_expr();
        for (int64_t i = 0; i < all_exprs.count(); i++) {
          ObExpr *expr = all_exprs.at(i);
          expr->get_eval_info(eval_ctx_).cnt_ = eval_ctx_.get_batch_idx();
          expr->set_evaluated_projected(eval_ctx_);
        }
      }
      brs_.size_ = eval_ctx_.get_batch_idx();
      if (MY_SPEC.is_consolidator()) {
        brs_.end_ = 0 == current_res.to_output_rows() && 0 == current.to_compute_rows();
      } else {
        brs_.end_ = 0 == current.to_output_rows() && 0 == current.to_compute_rows();
      }
      iter_end_ = brs_.end_;
    }
  }
  return ret;
}

int ObWindowFunctionOp::inner_get_next_batch(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  do {
    switch (stat_) {
      case ProcessStatus::PARTIAL: {
        if (OB_FAIL(partial_next_batch(max_row_cnt))) {
          LOG_WARN("partial next batch failed", K(ret));
        } else {
          if (brs_.end_) {
            if (MY_SPEC.single_part_parallel_ || MY_SPEC.range_dist_parallel_) {
              stat_ = ProcessStatus::COORINDATE;
              brs_.end_ = false;
              iter_end_ = false;
            }
          }
        }
        break;
      }
      case ProcessStatus::COORINDATE: {
        brs_.size_ = 0;
        brs_.end_ = false;
        if (OB_FAIL(coordinate())) {
          LOG_WARN("coordinate failed", K(ret));
        }
        stat_ = ProcessStatus::FINAL;
        break;
      }
      case ProcessStatus::FINAL: {
        if (OB_FAIL(final_next_batch(max_row_cnt))) {
          LOG_WARN("get next row failed", K(ret));
        }
        break;
      }
    }
  } while (OB_SUCC(ret) && !(brs_.end_ || brs_.size_ > 0));

  // Window function expr use batch_size as 1 for evaluation. When it is shared expr and eval again
  // as output expr, it will cause core dump because batch_size in output expr is set to a value
  // greater than 1. So we clear evaluated flag here to make shared expr in output can evaluated
  // normally.
  clear_evaluated_flag();
  return ret;
}

int ObWindowFunctionOp::do_partial_next_batch(const int64_t max_row_cnt, bool &do_output)
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  int64_t check_times = 0;
  int64_t output_row_cnt = MIN(max_row_cnt, MY_SPEC.max_batch_size_);
  const uint64_t tenant_id = ctx_.get_my_session()->get_effective_tenant_id();
  ObEvalCtx::BatchInfoScopeGuard guard(eval_ctx_);
  guard.set_batch_idx(0);
  if (OB_UNLIKELY(iter_end_)) {
    brs_.size_ = 0;
    brs_.end_ = true;
  } else if (OB_FAIL(ctx_.check_status())) {
    LOG_WARN("check physical plan status failed", K(ret));
  } else if (input_rows_.processed_->to_output_rows() == 0
             && input_rows_.processed_->count() > 0) {
    foreach_stores([&](Stores &s){ return s.processed_->reset(tenant_id); });
  }

  // <1> vec compute
  if (OB_SUCC(ret)) {
    WinFuncCell *first = wf_list_.get_first();
    WinFuncCell *end = wf_list_.get_header();
    int64_t rows_output_cnt = input_rows_.cur_->to_output_rows()
        + input_rows_.processed_->to_output_rows();
    while (OB_SUCC(ret) && rows_output_cnt < output_row_cnt) {
      RowsStore &current = *input_rows_.cur_;
      current.row_cnt_ = current.stored_row_cnt_;
      if (child_iter_end_) {
        break;
      } else {
        if (OB_FAIL(get_next_partition(check_times))) {
          LOG_WARN("get next partition from child", K(ret));
        }
        // Now we have found the next partition and store it in current rows_store
        // and they are all ready to output.
        rows_output_cnt = input_rows_.cur_->to_output_rows()
            + input_rows_.processed_->to_output_rows();
      }
    }
  }
  // <2> vec output
  if (OB_SUCC(ret)) {
    if (MY_SPEC.single_part_parallel_) {
      brs_.end_ = true;
      brs_.size_ = 0;
      do_output = true;
    } else if (MY_SPEC.range_dist_parallel_
               && child_iter_end_
               && input_rows_.cur_->to_compute_rows() == 0
               && !last_part_saved_) {
      last_part_saved_ = true;
      if (!first_part_saved_) {
        // only one partition
        first_part_saved_ = true;
        foreach_stores([](Stores &s){ std::swap(s.first_, s.cur_); return OB_SUCCESS; });
      } else {
        foreach_stores([](Stores &s){ std::swap(s.last_, s.cur_); return OB_SUCCESS; });
      }
      output_row_cnt = input_rows_.processed_->to_output_rows();
      if (output_row_cnt > 0) {
        if (OB_FAIL(output_batch_rows(output_row_cnt))) {
          LOG_WARN("output batch rows failed", K(ret));
        }
      } else {
        brs_.end_ = true;
        brs_.size_ = 0;
      }
      do_output = true;
    } else {
      if (OB_FAIL(output_batch_rows(output_row_cnt))) {
        LOG_WARN("output batch rows failed", K(ret));
      } else {
        do_output = true;
      }
      // to make sure to send piece data to datahub even though no row fetched
      if (OB_SUCC(ret) && MY_SPEC.is_participator() && brs_.end_) {
        if (OB_FAIL(send_empty_piece_data())) {
          LOG_WARN("send_empty_piece_data failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObWindowFunctionOp::send_empty_piece_data()
{
  int ret = OB_SUCCESS;

  for (WinFuncCell *wf = wf_list_.get_first();
       OB_SUCC(ret) && wf != wf_list_.get_header();
       wf = wf->get_next()) {
    if (OB_SUCC(ret) && wf->wf_info_.can_push_down_
        && wf->wf_info_.partition_exprs_.count() <= next_wf_pby_expr_cnt_to_transmit_) {
      int64_t idx = pby_expr_cnt_idx_array_.at(wf->wf_idx_ - 1);
      if (OB_FAIL(participator_coordinate(idx, wf->wf_info_.partition_exprs_))) {
        LOG_WARN("participator_coordinate failed",
            K(ret), K(idx), K(wf->wf_info_), K(wf->wf_info_.partition_exprs_));
      } else {
        next_wf_pby_expr_cnt_to_transmit_ = wf->wf_info_.partition_exprs_.count() - 1;
      }
    }
  }

  return ret;
}

int ObWindowFunctionOp::partial_next_batch(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  bool do_output = false;
  while (OB_SUCC(ret) && !do_output) {
    if (OB_FAIL(do_partial_next_batch(max_row_cnt, do_output))) {
      LOG_WARN("do_partial_next_batch failed", K(ret));
    }
  }
  return ret;
}

int ObWindowFunctionOp::final_next_batch(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  if (MY_SPEC.range_dist_parallel_) {
    if (OB_FAIL(rd_output_final_batch(std::min(max_row_cnt, MY_SPEC.max_batch_size_)))) {
      LOG_WARN("output first last part batch failed", K(ret));
    } else {
      if (brs_.end_ && brs_.size_ == 0 && !first_part_outputed_) {
        first_part_outputed_ = true;
        if (input_rows_.last_->count() > 0) {
          brs_.end_ = false;
          iter_end_ = false;
          foreach_stores([](Stores &s){ std::swap(s.first_, s.cur_); return OB_SUCCESS; });
          foreach_stores([](Stores &s){ std::swap(s.last_, s.cur_); return OB_SUCCESS; });
          patch_first_ = false;
          patch_last_ = true;
          ret = rd_output_final_batch(std::min(max_row_cnt, MY_SPEC.max_batch_size_));
        }
      }
    }
  } else {
    ObEvalCtx::BatchInfoScopeGuard guard(eval_ctx_);
    guard.set_batch_idx(0);
    if (OB_UNLIKELY(iter_end_)) {
      brs_.size_ = 0;
      brs_.end_ = true;
    } else if (OB_FAIL(output_batch_rows(std::min(max_row_cnt, MY_SPEC.max_batch_size_)))) {
      LOG_WARN("output batch rows failed", K(ret));
    }
  }
  return ret;
}

int ObWindowFunctionOp::check_interval_valid(ObExpr &expr)
{
  int ret = OB_SUCCESS;
  if (expr.type_ == T_FUN_SYS_DATE_ADD ||
      expr.type_ == T_FUN_SYS_DATE_SUB) {
    bool is_valid = true;
    ObDatum *date = NULL;
    ObDatum *interval = NULL;
    ObDatum *unit = NULL;
    if (OB_FAIL(expr.eval_param_value(eval_ctx_, date, interval, unit))) {
      LOG_WARN("eval param value failed");
    } else if (OB_UNLIKELY(date->is_null() || interval->is_null())) {
      is_valid = false;
    } else {
      ObString interval_val = interval->get_string();
      int64_t unit_value = unit->get_int();
      ObDateUnitType unit_val = static_cast<ObDateUnitType>(unit_value);
      ObInterval interval_time;
      if (OB_FAIL(ObTimeConverter::str_to_ob_interval(interval_val, unit_val, interval_time))) {
        if (OB_UNLIKELY(OB_INVALID_DATE_VALUE == ret)) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to convert string to ob interval", K(ret));
        }
      } else {
        is_valid = !(DT_MODE_NEG & interval_time.mode_);
      }
    }
    if (OB_SUCC(ret) && !is_valid) {
      ret = OB_ERR_WINDOW_FRAME_ILLEGAL;
      LOG_WARN("frame start or end is negative, NULL or of non-integral type", K(ret), KPC(interval), KPC(unit));
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
