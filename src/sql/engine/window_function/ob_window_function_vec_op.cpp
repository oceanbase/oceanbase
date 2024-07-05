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

#include "lib/utility/ob_print_utils.h"
#include "lib/utility/ob_sort.h"
#include "ob_window_function_vec_op.h"
#include "share/aggregate/iaggregate.h"
#include "share/aggregate/processor.h"
#include "sql/engine/px/ob_px_sqc_handler.h"
#include "sql/engine/px/datahub/components/ob_dh_winbuf.h"

#define SWAP_STORES(left, right)                                                                   \
  do {                                                                                             \
    input_stores_.swap_##left##_##right();                                                         \
    for (WinFuncColExpr *it = wf_list_.get_first(); it != wf_list_.get_header();                   \
         it = it->get_next()) {                                                                    \
      it->res_->swap_##left##_##right();                                                           \
    }                                                                                              \
  } while (0)

#define FOREACH_WINCOL(end)                                                                        \
  for (WinFuncColExpr *it = wf_list_.get_first(); OB_SUCC(ret) && it != (end); it = it->get_next())

#define END_WF (wf_list_.get_header())

namespace oceanbase
{
namespace sql
{
OB_SERIALIZE_MEMBER((ObWindowFunctionVecSpec, ObWindowFunctionSpec));

OB_SERIALIZE_MEMBER(ObWindowFunctionVecOpInput, local_task_count_, total_task_count_,
                    wf_participator_shared_info_);


// if agg_func == T_INVALID, use wf_info.func_type_ to dispatch merge function dynamicly
template<ObExprOperatorType agg_func>
struct __PartialResult
{
  __PartialResult(ObEvalCtx &ctx, ObIAllocator &allocator) : eval_ctx_(ctx), merge_alloc_(allocator)
  {}
  template<typename ResFmt>
  int merge(const WinFuncInfo &wf_info, const bool src_isnull, const char *src, int32_t src_len)
  {
    int ret = OB_SUCCESS;
    ResFmt *res_data = static_cast<ResFmt *>(wf_info.expr_->get_vector(eval_ctx_));
    VecValueTypeClass out_tc = wf_info.expr_->get_vec_value_tc();
    int64_t output_idx = eval_ctx_.get_batch_idx();
    if (agg_func == T_FUN_MIN || agg_func == T_FUN_MAX
        || wf_info.func_type_ == T_FUN_MIN || wf_info.func_type_ == T_FUN_MAX) {
      bool is_min_fn = (agg_func == T_FUN_MIN || wf_info.func_type_ == T_FUN_MIN);
      NullSafeRowCmpFunc cmp_fn = (is_min_fn ? wf_info.expr_->basic_funcs_->row_null_last_cmp_ :
                                               wf_info.expr_->basic_funcs_->row_null_first_cmp_);
      ObObjMeta &obj_meta = wf_info.expr_->obj_meta_;
      const char *payload = nullptr;
      int32_t len = 0;
      int cmp_ret =0;
      bool cur_isnull = false;
      res_data->get_payload(output_idx, cur_isnull, payload, len);
      if (cur_isnull && !src_isnull) {
        if (OB_FAIL(set_payload(res_data, out_tc, output_idx, src, src_len))) {
          LOG_WARN("set payload failed", K(ret));
        }
      } else if (src_isnull) {
        // do nothing
      } else if (OB_FAIL(cmp_fn(obj_meta, obj_meta, payload, len, cur_isnull, src, src_len,
                                src_isnull, cmp_ret))) {
        LOG_WARN("compare failed", K(ret));
      } else if ((cmp_ret > 0 && (agg_func == T_FUN_MIN || wf_info.func_type_ == T_FUN_MIN))
                 || (cmp_ret < 0 && (agg_func == T_FUN_MAX || wf_info.func_type_ == T_FUN_MAX))) {
        if (OB_FAIL(set_payload(res_data, out_tc, output_idx, src, src_len))) {
          LOG_WARN("set payload failed", K(ret));
        }
      }
    } else if (agg_func == T_FUN_COUNT
               || agg_func == T_WIN_FUN_RANK
               || agg_func == T_WIN_FUN_DENSE_RANK
               || wf_info.func_type_ == T_FUN_COUNT
               || wf_info.func_type_ == T_WIN_FUN_RANK
               || wf_info.func_type_ == T_WIN_FUN_DENSE_RANK) {
      // same as COUNT_SUM
      const char *res_buf = nullptr;
      bool res_isnull = false;
      int32_t res_len = 0;
      res_data->get_payload(output_idx, res_isnull, res_buf, res_len);
      bool is_rank_like = !(agg_func == T_FUN_COUNT || wf_info.func_type_ == T_FUN_COUNT);
      if (src_isnull) {
      } else if (lib::is_oracle_mode()) {
        ret = sum_merge<number::ObCompactNumber>(res_buf, src, res_isnull, src_isnull, res_data, output_idx);
      } else if (is_rank_like) {
        ret = sum_merge<uint64_t>(res_buf, src, res_isnull, src_isnull, res_data, output_idx);
      } else if (!is_rank_like ) {
        ret = sum_merge<int64_t>(res_buf, src, res_isnull, src_isnull, res_data, output_idx);
      }
      if (OB_FAIL(ret)) {
        LOG_WARN("merge result failed", K(ret), K(agg_func), K(wf_info.func_type_));
      }
    } else if (agg_func == T_FUN_SUM || wf_info.func_type_ == T_FUN_SUM) {
      const char *res_buf;
      bool res_isnull = false;
      int32_t res_len = 0;
      res_data->get_payload(output_idx, res_isnull, res_buf, res_len);
      VecValueTypeClass res_tc = wf_info.expr_->get_vec_value_tc();
      if (src_isnull) { // do nothing
      } else if (std::is_same<ResFmt, ObDiscreteFormat>::value || res_tc == VEC_TC_NUMBER) {
        ret = sum_merge<number::ObCompactNumber>(res_buf, src, res_isnull, src_isnull, res_data, output_idx);
      } else if (std::is_same<ResFmt, ObFixedLengthFormat<int32_t>>::value || res_tc == VEC_TC_DEC_INT32) {
        ret = sum_merge<int32_t>(res_buf, src, res_isnull, src_isnull, res_data, output_idx);
      } else if (std::is_same<ResFmt, ObFixedLengthFormat<int64_t>>::value || res_tc == VEC_TC_DEC_INT64) {
        ret = sum_merge<int64_t>(res_buf, src, res_isnull, src_isnull, res_data, output_idx);
      } else if (std::is_same<ResFmt, ObFixedLengthFormat<int128_t>>::value || res_tc == VEC_TC_DEC_INT128) {
        ret = sum_merge<int128_t>(res_buf, src, res_isnull, src_isnull, res_data, output_idx);
      } else if (std::is_same<ResFmt, ObFixedLengthFormat<int256_t>>::value || res_tc == VEC_TC_DEC_INT256) {
        ret = sum_merge<int256_t>(res_buf, src, res_isnull, src_isnull, res_data, output_idx);
      } else if (std::is_same<ResFmt, ObFixedLengthFormat<int512_t>>::value || res_tc == VEC_TC_DEC_INT512) {
        ret = sum_merge<int512_t>(res_buf, src, res_isnull, src_isnull, res_data, output_idx);
      } else if (std::is_same<ResFmt, ObFixedLengthFormat<float>>::value || res_tc == VEC_TC_FLOAT) {
        ret = sum_merge<float>(res_buf, src, res_isnull, src_isnull, res_data, output_idx);
      } else if (std::is_same<ResFmt, ObFixedLengthFormat<double>>::value || res_tc == VEC_TC_DOUBLE
                 || res_tc == VEC_TC_FIXED_DOUBLE) {
        ret = sum_merge<double>(res_buf, src, res_isnull, src_isnull, res_data, output_idx);
      }
      if (OB_FAIL(ret)) {
        LOG_WARN("merge sum partial results failed", K(ret), K(res_tc), K(wf_info), K(agg_func));
      }
    }
    return ret;
  }

  template <typename ResFmt>
  int add_rank(const WinFuncInfo &wf_info, const bool src_isnull, const char *src, int32_t src_len,
               int64_t rank_val)
  {
    int ret = OB_SUCCESS;
    ResFmt *res_data = static_cast<ResFmt *>(wf_info.expr_->get_vector(eval_ctx_));
    int64_t output_idx = eval_ctx_.get_batch_idx();
    if (std::is_same<ResFmt, ObDiscreteFormat>::value
        || wf_info.expr_->datum_meta_.type_ == ObNumberType) {
      ObNumStackAllocator<4> tmp_alloc;
      number::ObNumber extra_nmb;
      number::ObNumber src2_nmb(res_data->get_number(output_idx));
      number::ObNumber res_nmb;
      if (OB_FAIL(extra_nmb.from(rank_val, tmp_alloc))) {
        LOG_WARN("from number failed", K(ret));
      } else if (OB_FAIL(res_nmb.add(extra_nmb, res_nmb, tmp_alloc))) {
        LOG_WARN("add number failed", K(ret));
      } else if (!src_isnull) {
        number::ObNumber src_nmb(*reinterpret_cast<const number::ObCompactNumber *>(src));
        if (OB_FAIL(res_nmb.add(src_nmb, res_nmb, tmp_alloc))) {
          LOG_WARN("add number failed", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (!res_data->is_null(output_idx)) {
        number::ObNumber src_nmb(res_data->get_number(output_idx));
        if (OB_FAIL(res_nmb.add(src_nmb, res_nmb, tmp_alloc))) {
          LOG_WARN("add number failed", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else {
        int32_t output_len = res_nmb.get_desc().len_ * sizeof(uint32_t) + sizeof(ObNumberDesc);
        void *out_buf = merge_alloc_.alloc(output_len);
        if (OB_ISNULL(out_buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate memory failed", K(ret));
        } else {
          number::ObCompactNumber *res_cnum = reinterpret_cast<number::ObCompactNumber *>(out_buf);
          res_cnum->desc_ = res_nmb.get_desc();
          MEMCPY(&(res_cnum->digits_[0]), res_nmb.get_digits(),  res_nmb.get_desc().len_ * sizeof(uint32_t));
          res_data->set_number_shallow(output_idx, *res_cnum);
        }
      }
    } else if (std::is_same<ResFmt, ObFixedLengthFormat<uint64_t>>::value
               || wf_info.expr_->datum_meta_.type_ == ObUInt64Type) {
      int64_t patch_val = rank_val;
      patch_val = patch_val + (src_isnull ? 0 : static_cast<int64_t>(*reinterpret_cast<const uint64_t *>(src)));
      patch_val = patch_val + (res_data->is_null(output_idx) ? 0 : static_cast<int64_t>(res_data->get_uint64(output_idx)));
      res_data->set_uint(output_idx, static_cast<uint64_t>(patch_val));
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected ranking function", K(ret), K(wf_info));
    }
    return ret;
  }

private:
  template <typename T>
  int add_result(char *res_buf, const char *left_ptr, const char *right_ptr, bool l_isnull, bool r_isnull)
  {

    int ret = OB_SUCCESS;
    if (l_isnull || r_isnull) {
      if (l_isnull && !r_isnull) {
        MEMCPY(res_buf, right_ptr, sizeof(T));
      } else if (!l_isnull && r_isnull) {
        MEMCPY(res_buf, left_ptr, sizeof(T));
      }
    } else {
      const T *left = reinterpret_cast<const T *>(left_ptr);
      const T *right = reinterpret_cast<const T *>(right_ptr);
      T &res = *reinterpret_cast<T *>(res_buf);
      if (std::is_same<T, float>::value) {
        ret = aggregate::add_overflow(*left, *right, res_buf, sizeof(res_buf));
      } else {
        res = *left + *right;
      }
    }
    return ret;
  }

  template <>
  int add_result<number::ObCompactNumber>(char *res_buf,
                                          const char *left_ptr,
                                          const char *right_ptr,
                                          bool l_isnull, bool r_isnull)
  {
    int ret = OB_SUCCESS;
    ObNumStackOnceAlloc tmp_alloc;
    number::ObNumber res_nmb;
    const number::ObCompactNumber *left = reinterpret_cast<const number::ObCompactNumber *>(left_ptr);
    const number::ObCompactNumber *right = reinterpret_cast<const number::ObCompactNumber *>(right_ptr);
    if (l_isnull || r_isnull) {
      if (l_isnull && !r_isnull) {
        MEMCPY(res_buf, right, sizeof(uint32_t) + right->desc_.len_ * sizeof(uint32_t));
      } else if (!l_isnull && r_isnull) {
        MEMCPY(res_buf, left, sizeof(uint32_t) +  left->desc_.len_ * sizeof(uint32_t));
      }
    } else {
      number::ObNumber l(*left), r(*right);
      if (OB_FAIL(l.add(r, res_nmb, tmp_alloc))) {
        LOG_WARN("add_v3 failed", K(ret));
      } else {
        number::ObCompactNumber *res_cnum = reinterpret_cast<number::ObCompactNumber *>(res_buf);
        res_cnum->desc_ = res_nmb.d_;
        MEMCPY(&(res_cnum->digits_[0]), res_nmb.get_digits(), res_nmb.d_.len_ * sizeof(uint32_t));
      }
    }
    return ret;
  }

  template <typename T, typename ResFmt>
  int sum_merge(const char *left, const char *right, bool l_isnull, bool r_isnull, ResFmt *res_data,
                const int64_t output_idx)
  {
    int ret = OB_SUCCESS;
    const int32_t constexpr tmp_res_size =
      (std::is_same<T, number::ObCompactNumber>::value ? number::ObNumber::MAX_CALC_BYTE_LEN :
                                                         sizeof(T));
    char tmp_res[tmp_res_size] = {0};
    ret = add_result<T>(tmp_res, left, right, l_isnull, r_isnull);
    if (OB_FAIL(ret)) {
      LOG_WARN("add result failed", K(ret));
    } else if (std::is_same<T, number::ObCompactNumber>::value) {
      number::ObCompactNumber *res_cnum = reinterpret_cast<number::ObCompactNumber *>(tmp_res);
      int32_t output_len = res_cnum->desc_.len_ * sizeof(uint32_t) + sizeof(ObNumberDesc);
      char *out_buf = (char *)merge_alloc_.alloc(output_len);
      if (OB_ISNULL(out_buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret));
      } else {
        MEMCPY(out_buf, tmp_res, output_len);
        res_data->set_payload_shallow(output_idx, out_buf, output_len);
      }
    } else {
      res_data->set_payload(output_idx, tmp_res, sizeof(T));
    }
    return ret;
  }


  template <typename ResFmt>
  int set_payload(ResFmt *res_data, VecValueTypeClass out_tc, int64_t output_idx, const char *src,
                  int32_t src_len)
  {
    int ret = OB_SUCCESS;
    if (std::is_same<ResFmt, ObDiscreteFormat>::value || is_discrete_vec(out_tc)) {
      res_data->set_payload_shallow(output_idx, src, src_len);
    } else {
      res_data->set_payload(output_idx, src, src_len);
    }
    return ret;
  }
  ObEvalCtx &eval_ctx_;
  ObIAllocator &merge_alloc_;
};

int ObWindowFunctionVecOp::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOperator::inner_open())) {
    LOG_WARN("inner ope child operator failed", K(ret));
  } else if (OB_ISNULL(ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid null session ptr", K(ret));
  } else if (OB_FAIL(init())) {
    LOG_WARN("init window function failed", K(ret));
  } else if (OB_FAIL(reset_for_scan(ctx_.get_my_session()->get_effective_tenant_id()))) {
    LOG_WARN("reset for scan failed", K(ret));
  }
  LOG_TRACE("window function inner open", K(MY_SPEC), K(MY_SPEC.single_part_parallel_), K(MY_SPEC.range_dist_parallel_));
  return ret;
}

int ObWindowFunctionVecOp::inner_close()
{
  int ret = OB_SUCCESS;
  sql_mem_processor_.unregister_profile();
  destroy_stores();
  all_expr_vector_copy_.reset();
  FOREACH_WINCOL(END_WF) {
    it->destroy();
  }
  wf_list_.reset();
  pby_expr_cnt_idx_array_.reset();
  for (int i = 0; i < pby_hash_values_.count(); i++) {
    pby_hash_values_.at(i)->reset();
  }
  pby_hash_values_.reset();
  for (int i = 0; i < participator_whole_msg_array_.count(); i++) {
    participator_whole_msg_array_.at(i)->reset();
  }
  participator_whole_msg_array_.reset();
  for (int i = 0; i < pby_hash_values_sets_.count(); i++) {
    pby_hash_values_sets_.at(i)->destroy();
  }
  pby_hash_values_sets_.reset();
  input_row_meta_.reset();
  batch_ctx_.reset();
  if (MY_SPEC.single_part_parallel_ && all_wf_res_row_meta_ != nullptr) {
    all_wf_res_row_meta_->reset();
  }
  if (MY_SPEC.range_dist_parallel_ && rd_coord_row_meta_ != nullptr) {
    rd_coord_row_meta_->reset();
  }
  all_part_exprs_.reset();
  return ObOperator::inner_close();
}

int ObWindowFunctionVecOp::inner_rescan()
{
  int ret = OB_SUCCESS;
  // FIXME: add rescan logic
  if (OB_FAIL(ObOperator::inner_rescan())) {
    LOG_WARN("inner_open child operator failed", K(ret));
  } else if (OB_ISNULL(ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null session", K(ret));
  } else if (OB_FAIL(reset_for_scan(ctx_.get_my_session()->get_effective_tenant_id()))) {
    LOG_WARN("reset for scan failed", K(ret));
  } else {
    FOREACH_WINCOL(END_WF) {
      it->reset_for_scan();
    }
  }
  if (OB_FAIL(ret)) {
  } else {
    stat_ = ProcessStatus::PARTIAL;
    iter_end_ = false;
    first_part_saved_ = false;
    last_part_saved_ = false;
    rescan_alloc_.reset();

    patch_alloc_.reset();
    first_part_outputed_ = false;
    patch_first_ = false;
    patch_last_ = false;
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
  }
  return ret;
}

int ObWindowFunctionVecOp::inner_get_next_batch(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  do {
    switch(stat_) {
    case ProcessStatus::PARTIAL: {
      if (OB_FAIL(partial_next_batch(max_row_cnt))) {
        LOG_WARN("partial next batch failed", K(ret));
      } else if (brs_.end_) {
        if (MY_SPEC.single_part_parallel_ || MY_SPEC.range_dist_parallel_) {
          stat_ = ProcessStatus::COORDINATE;
          brs_.end_ = false;
          iter_end_ = false;
        }
      }
      break;
    }
    case ProcessStatus::COORDINATE: {
      brs_.size_ = 0;
      brs_.end_ = false;
      if (OB_FAIL(coordinate())) {
        LOG_WARN("coordinate failed", K(ret));
      } else {
        stat_ = ProcessStatus::FINAL;
      }
      break;
    }
    case ProcessStatus::FINAL: {
      if (OB_FAIL(final_next_batch(max_row_cnt))) {
        LOG_WARN("get next batch failed", K(ret));
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

void ObWindowFunctionVecOp::destroy()
{
  sql_mem_processor_.unregister_profile_if_necessary();
  input_stores_.destroy();
  for (WinFuncColExpr *it = wf_list_.get_first(); it != wf_list_.get_header();
       it = it->get_next()) {
    it->destroy();
  }
  wf_list_.~WinFuncColExprList();
  rescan_alloc_.~ObArenaAllocator();
  patch_alloc_.~ObArenaAllocator();
  destroy_mem_context();
  local_allocator_ = nullptr;
  ObOperator::destroy();
}

int ObWindowFunctionVecOp::reset_for_scan(const int64_t tenant_id)
{
  int ret = OB_SUCCESS;
  last_output_row_idx_ = OB_INVALID_INDEX;
  child_iter_end_ = false;
  reset_stores();
  if (sp_merged_row_ != nullptr) {
    sp_merged_row_->reset();
    sp_merged_row_ = nullptr;
  }
  batch_ctx_.reset();
  last_computed_part_rows_ = 0;
  if (rd_patch_ != nullptr) {
    rd_patch_->reset();
    rd_patch_ = nullptr;
  }
  backuped_size_ = 0;
  return ret;
}

int ObWindowFunctionVecOp::create_stores(const int64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObIAllocator *store_alloc = &mem_context_->get_malloc_allocator();
  input_stores_.processed_ = OB_NEWx(winfunc::RowStore, local_allocator_, tenant_id, store_alloc,
                                     local_allocator_, input_stores_);
  input_stores_.cur_ = OB_NEWx(winfunc::RowStore, local_allocator_, tenant_id, store_alloc,
                               local_allocator_, input_stores_);
  input_stores_.set_operator(this);
  if (OB_ISNULL(input_stores_.processed_) || OB_ISNULL(input_stores_.cur_)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  } else if (MY_SPEC.range_dist_parallel_) {
    input_stores_.first_ = OB_NEWx(winfunc::RowStore, local_allocator_, tenant_id, store_alloc,
                                   local_allocator_, input_stores_);
    input_stores_.last_ = OB_NEWx(winfunc::RowStore, local_allocator_, tenant_id, store_alloc,
                                  local_allocator_, input_stores_);
    if (OB_ISNULL(input_stores_.first_) || OB_ISNULL(input_stores_.last_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    }
  }
  ObIArray<ObExpr *> &all_exprs = get_all_expr();
  lib::ObMemAttr stored_mem_attr(tenant_id, ObModIds::OB_SQL_WINDOW_ROW_STORE, ObCtxIds::WORK_AREA);
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(input_stores_.init(MY_SPEC.max_batch_size_, input_row_meta_, stored_mem_attr,
                                        INT64_MAX, true))) {
    LOG_WARN("init input stores failed", K(ret));
  }
  FOREACH_WINCOL(END_WF) {
    it->res_ = OB_NEWx(winfunc::RowStores, local_allocator_);
    it->res_->set_operator(this);
    if (OB_ISNULL(it->res_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else {
      it->res_->processed_ = OB_NEWx(winfunc::RowStore, local_allocator_, tenant_id, store_alloc,
                                     local_allocator_, *it->res_);
      it->res_->cur_ = OB_NEWx(winfunc::RowStore, local_allocator_, tenant_id, store_alloc,
                               local_allocator_, *it->res_);
      if (OB_ISNULL(it->res_->processed_) || OB_ISNULL(it->res_->cur_)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret));
      } else if (MY_SPEC.range_dist_parallel_) {
        it->res_->first_ = OB_NEWx(winfunc::RowStore, local_allocator_, tenant_id, store_alloc,
                                   local_allocator_, *it->res_);
        it->res_->last_ = OB_NEWx(winfunc::RowStore, local_allocator_, tenant_id, store_alloc,
                                  local_allocator_, *it->res_);
        if (OB_ISNULL(it->res_->first_) || OB_ISNULL(it->res_->last_)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate memory failed", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(it->res_->init(MY_SPEC.max_batch_size_, it->wf_res_row_meta_,
                                        stored_mem_attr, INT64_MAX, true))) {
        LOG_WARN("init row stores failed", K(ret));
      }
    }
  }
  return ret;
}

void ObWindowFunctionVecOp::reset_stores()
{
  input_stores_.reset();
  for (WinFuncColExpr *it = wf_list_.get_first(); it != END_WF; it = it->get_next()) {
    if (it->res_ != nullptr) { it->res_->reset(); }
  }
}

void ObWindowFunctionVecOp::destroy_stores()
{
  input_stores_.destroy();
  for (WinFuncColExpr *it = wf_list_.get_first(); it != END_WF; it = it->get_next()) {
    if (it->res_ != nullptr) {
      it->res_->destroy();
    }
  }
}

int ObWindowFunctionVecOp::build_pby_hash_values_for_transmit()
{
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < pby_set_count_; i++) {
    PbyHashValueArray *arr = OB_NEWx(PbyHashValueArray, local_allocator_);
    if (OB_ISNULL(arr)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else if (OB_FAIL(pby_hash_values_.push_back(arr))) {
      LOG_WARN("push back elements failed", K(ret));
    }
  }
  return ret;
}

int ObWindowFunctionVecOp::build_participator_whole_msg_array()
{
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < pby_set_count_; i++) {
    ObReportingWFWholeMsg *msg = OB_NEWx(ObReportingWFWholeMsg, local_allocator_);
    if (OB_ISNULL(msg)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else if (OB_FAIL(participator_whole_msg_array_.push_back(msg))) {
      LOG_WARN("push back element failed", K(ret));
    }
  }
  return ret;
}

int ObWindowFunctionVecOp::setup_participator_pby_hash_sets(WFInfoFixedArray &wf_infos,
                                                            ObWindowFunctionVecOpInput *op_input)
{
  int ret = OB_SUCCESS;
  int64_t prev_pushdown_pby_col_count = -1;
  int64_t idx = -1;
  if (OB_ISNULL(op_input)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid operator input", K(ret));
  }
  for (int i = 0; OB_SUCC(ret) && i < wf_infos.count(); i++) {
    WinFuncInfo &wf_info = wf_infos.at(i);
    if (!wf_info.can_push_down_) {
      if (OB_FAIL(pby_expr_cnt_idx_array_.push_back(OB_INVALID_ID))) {
        LOG_WARN("push back element failed", K(ret));
      }
    } else {
      if (wf_info.partition_exprs_.count() == prev_pushdown_pby_col_count) {
        if (OB_FAIL(pby_expr_cnt_idx_array_.push_back(idx))) {
          LOG_WARN("push back element failed", K(ret));
        }
      } else {
        prev_pushdown_pby_col_count = wf_info.partition_exprs_.count();
        if (OB_FAIL(pby_expr_cnt_idx_array_.push_back(++idx))) {
          LOG_WARN("push back element failed", K(ret));
        } else {
          ReportingWFHashSet *hash_set = OB_NEWx(ReportingWFHashSet, local_allocator_);
          if (OB_ISNULL(hash_set)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("allocate memory failed", K(ret));
          } else if (OB_FAIL(hash_set->create(op_input->get_total_task_count()
                                              * op_input->get_total_task_count()))) { // dop * dop
            LOG_WARN("init hash sets failed", K(ret));
          } else if (OB_FAIL(pby_hash_values_sets_.push_back(hash_set))) {
            LOG_WARN("push back element failed", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

template<typename WinExpr>
static int alloc_expr(ObIAllocator &allocator, WinExpr *&expr)
{
  int ret = OB_SUCCESS;
  void *expr_buf = nullptr;
  if (OB_ISNULL(expr_buf = allocator.alloc(sizeof(WinExpr)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  } else {
    expr = new (expr_buf) WinExpr();
  }
  return ret;
}
int ObWindowFunctionVecOp::init()
{
  int ret = OB_SUCCESS;
  ObWindowFunctionVecOpInput *op_input = static_cast<ObWindowFunctionVecOpInput *>(input_);
  if (OB_UNLIKELY(!wf_list_.is_empty())) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_ISNULL(ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null session ptr", K(ret));
  } else if (OB_FAIL(init_mem_context())) {
    LOG_WARN("init memory context failed", K(ret));
  } else {
    int64_t est_rows = MY_SPEC.rows_;
    if (OB_FAIL(ObPxEstimateSizeUtil::get_px_size(&ctx_, MY_SPEC.px_est_size_factor_, est_rows, est_rows))) {
      LOG_WARN("failed to get px size", K(ret));
    } else if (OB_FAIL(sql_mem_processor_.init(
                         &mem_context_->get_malloc_allocator(),
                         ctx_.get_my_session()->get_effective_tenant_id(),
                         (est_rows * MY_SPEC.width_ / MY_SPEC.estimated_part_cnt_),
                         MY_SPEC.type_, MY_SPEC.id_, &ctx_))) {
      LOG_WARN("init sql mem processor failed", K(ret));
    } else {
      LOG_TRACE("show some est values", K(ret), K(MY_SPEC.rows_), K(est_rows), K(MY_SPEC.width_),
                K(MY_SPEC.estimated_part_cnt_), K(MY_SPEC.input_rows_mem_bound_ratio_));
    }
  }

  if (OB_FAIL(ret)) {
  } else {
    const int64_t tenant_id = ctx_.get_my_session()->get_effective_tenant_id();
    local_allocator_ = &(mem_context_->get_arena_allocator());
    rescan_alloc_.set_tenant_id(tenant_id);
    rescan_alloc_.set_label("WfRescanAlloc");
    patch_alloc_.set_tenant_id(tenant_id);
    patch_alloc_.set_label("WfPatchAlloc");
    ObMemAttr attr(tenant_id, "WfArray");
    participator_whole_msg_array_.set_attr(attr);
    pby_hash_values_.set_attr(attr);
    pby_hash_values_sets_.set_attr(attr);
    pby_expr_cnt_idx_array_.set_attr(attr);
    all_part_exprs_.set_attr(attr);
    int prev_pushdown_pby_col_count = -1;
    WFInfoFixedArray &wf_infos = const_cast<WFInfoFixedArray &>(MY_SPEC.wf_infos_);
    if (OB_FAIL(ObChunkStoreUtil::alloc_dir_id(dir_id_))) {
      LOG_WARN("failed to alloc dir id", K(ret));
    } else if (MY_SPEC.max_batch_size_ > 0) {
      if (OB_FAIL(all_expr_vector_copy_.init(child_->get_spec().output_, eval_ctx_))) {
        LOG_WARN("init vector holder failed", K(ret));
      } else {
        LOG_DEBUG("init expr vector holder", K(get_all_expr()));
      }
    }
    // init wf row meta & create stores
    if (OB_SUCC(ret)) {
      // setup input_row_meta_
      input_row_meta_.set_allocator(local_allocator_);
      if (OB_FAIL(input_row_meta_.init(get_all_expr(), 0))) {
        LOG_WARN("init row meta failed", K(ret));
      }
    }
    // create aggr rows
    for (int wf_idx = 1; OB_SUCC(ret) && wf_idx <= wf_infos.count(); wf_idx++) {
      WinFuncInfo &wf_info = wf_infos.at(wf_idx - 1);
      for (int j = 0; OB_SUCC(ret) && j < wf_info.partition_exprs_.count(); j++) {
        if (OB_FAIL(add_var_to_array_no_dup(all_part_exprs_, wf_info.partition_exprs_.at(j)))) {
          LOG_WARN("add element failed", K(ret));
        }
      }
      void *win_col_buf = nullptr, *pby_row_mapped_value_buf = nullptr;
      WinFuncColExpr *win_col = nullptr;
      int64_t agg_col_id = wf_idx - 1;
      if (OB_ISNULL(win_col_buf = local_allocator_->alloc(sizeof(WinFuncColExpr)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret));
      } else {
        win_col = new (win_col_buf) WinFuncColExpr(wf_info, *this, wf_idx);
        win_col->pby_row_mapped_idxes_ = reinterpret_cast<int32_t *>(pby_row_mapped_value_buf);
        switch (wf_info.func_type_) {
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
        case T_FUN_ORA_XMLAGG: {
          aggregate::IAggregate *agg_func = nullptr;
          winfunc::AggrExpr *aggr_expr = nullptr;
          if (OB_FAIL(alloc_expr<winfunc::AggrExpr>(*local_allocator_, aggr_expr))) {
            LOG_WARN("allocate aggr expr failed", K(ret));
          } else {
            win_col->wf_expr_ = aggr_expr;
          }
          break;
        }
        case T_WIN_FUN_RANK: {
          using ranklike_expr = winfunc::RankLikeExpr<T_WIN_FUN_RANK>;
          ranklike_expr *rank_expr = nullptr;
          if (OB_FAIL(alloc_expr<ranklike_expr>(*local_allocator_, rank_expr))) {
            LOG_WARN("allocate rank expr failed", K(ret));
          } else {
            win_col->wf_expr_ = rank_expr;
          }
          break;
        }
        case T_WIN_FUN_DENSE_RANK: {
          using ranklike_expr = winfunc::RankLikeExpr<T_WIN_FUN_DENSE_RANK>;
          ranklike_expr *rank_expr = nullptr;
          if (OB_FAIL(alloc_expr<ranklike_expr>(*local_allocator_, rank_expr))) {
            LOG_WARN("allocate rank expr failed", K(ret));
          } else {
            win_col->wf_expr_ = rank_expr;
          }
          break;
        }
        case T_WIN_FUN_PERCENT_RANK: {
          using ranklike_expr = winfunc::RankLikeExpr<T_WIN_FUN_PERCENT_RANK>;
          ranklike_expr *rank_expr = nullptr;
          if (OB_FAIL(alloc_expr<ranklike_expr>(*local_allocator_, rank_expr))) {
            LOG_WARN("allocate rank expr failed", K(ret));
          } else {
            win_col->wf_expr_ = rank_expr;
          }
          break;
        }
        case T_WIN_FUN_CUME_DIST: {
          winfunc::CumeDist *cume_expr = nullptr;
          if (OB_FAIL(alloc_expr<winfunc::CumeDist>(*local_allocator_, cume_expr))) {
            LOG_WARN("allocate cume dist expr failed", K(ret));
          } else {
            win_col->wf_expr_ = cume_expr;
          }
          break;
        }
        case T_WIN_FUN_ROW_NUMBER: {
          winfunc::RowNumber *row_nmb = nullptr;
          if (OB_FAIL(alloc_expr<winfunc::RowNumber>(*local_allocator_, row_nmb))) {
            LOG_WARN("allocate row number expr failed", K(ret));
          } else {
            win_col->wf_expr_ = row_nmb;
          }
          break;
        }
          // first_value && last_value has been converted to nth_value when resolving
          // case T_WIN_FUN_FIRST_VALUE:
          // case T_WIN_FUN_LAST_VALUE:
        case T_WIN_FUN_NTH_VALUE: {
          winfunc::NthValue *nth_expr = nullptr;
          if (OB_FAIL(alloc_expr<winfunc::NthValue>(*local_allocator_, nth_expr))) {
            LOG_WARN("allocate nth value expr failed", K(ret));
          } else {
            win_col->wf_expr_ = nth_expr;
          }
          break;
        }
        case T_WIN_FUN_LAG:
        case T_WIN_FUN_LEAD: {
          winfunc::LeadOrLag *lead_lag_expr = nullptr;
          if (OB_FAIL(alloc_expr<winfunc::LeadOrLag>(*local_allocator_, lead_lag_expr))) {
            LOG_WARN("allocate lead_or_lag expr failed", K(ret));
          } else {
            win_col->wf_expr_ = lead_lag_expr;
          }
          break;
        }
        case T_WIN_FUN_NTILE: {
          winfunc::Ntile *ntile_expr = nullptr;
          if (OB_FAIL(alloc_expr<winfunc::Ntile>(*local_allocator_, ntile_expr))) {
            LOG_WARN("allocate ntile expr failed", K(ret));
          } else {
            win_col->wf_expr_ = ntile_expr;
          }
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected window function type", K(ret), K(wf_info.func_type_));
        }
        }
        if (OB_FAIL(ret)) {
          // do nothing
        } else if (OB_FAIL(win_col->init_res_rows(tenant_id))) {
          LOG_WARN("init result compact rows failed", K(ret));
        } else if (win_col->wf_expr_->is_aggregate_expr()
                   && OB_FAIL(win_col->init_aggregate_ctx(tenant_id))) {
          LOG_WARN("init aggr ctx and rows failed", K(ret));
        } else if (!win_col->wf_expr_->is_aggregate_expr()
                   && OB_FAIL(win_col->init_non_aggregate_ctx())) {
          LOG_WARN("init non-aggr ctx failed", K(ret));
        } else {
          if (OB_UNLIKELY(!wf_list_.add_last(win_col))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("add window function col failed", K(ret));
          } else {
            LOG_DEBUG("added wf", K(*win_col));
          }
        }
        if (OB_SUCC(ret) && MY_SPEC.is_participator()) {
          if (wf_info.can_push_down_) {
            if (common::OB_INVALID_COUNT == next_wf_pby_expr_cnt_to_transmit_) {
              // next_wf_pby_expr_cnt_to_transmit_ is for pushdown transmit to datahub
              next_wf_pby_expr_cnt_to_transmit_ = wf_info.partition_exprs_.count();
            }
            if (wf_info.partition_exprs_.count() != prev_pushdown_pby_col_count) {
              pby_set_count_++;
              prev_pushdown_pby_col_count = wf_info.partition_exprs_.count();
            }
          }
        }
      }
    } // end for
    if (OB_SUCC(ret)) {
      max_pby_col_cnt_ = all_part_exprs_.count();
    }

    if (OB_SUCC(ret) && MY_SPEC.is_participator()) {
      if (OB_FAIL(build_pby_hash_values_for_transmit())) {
        LOG_WARN("build transimitting hash values failed", K(ret));
      } else if (OB_FAIL(build_participator_whole_msg_array())) {
        LOG_WARN("build participator whole msg array failed", K(ret));
      } else if (OB_FAIL(setup_participator_pby_hash_sets(wf_infos, op_input))) {
        LOG_WARN("setup oby hash sets failed", K(ret));
      }
    }
    if (OB_SUCC(ret) && max_pby_col_cnt_ > 0) {
      // init pby row mapped idx array
      int32_t arr_buf_size = max_pby_col_cnt_ * sizeof(int32_t) * (MY_SPEC.max_batch_size_ + 1);
      void *arr_buf = local_allocator_->alloc(arr_buf_size);
      int32_t last_row_idx_arr_offset = max_pby_col_cnt_ * sizeof(int32_t) * MY_SPEC.max_batch_size_;
      if (OB_ISNULL(arr_buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret));
      } else {
        MEMSET(arr_buf, -1, arr_buf_size);
        pby_row_mapped_idx_arr_ = reinterpret_cast<int32_t *>(arr_buf);
        last_row_idx_arr_ = reinterpret_cast<int32_t *>((char *)arr_buf + last_row_idx_arr_offset);
      }
      FOREACH_WINCOL(END_WF) {
        it->pby_row_mapped_idxes_ = (int32_t *)local_allocator_->alloc(max_pby_col_cnt_ * sizeof(int32_t));
        if (OB_ISNULL(it->pby_row_mapped_idxes_)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate memory failed", K(ret));
        } else {
          MEMSET(it->pby_row_mapped_idxes_, -1, sizeof(int32_t) * max_pby_col_cnt_);
        }
        // we may have wf info part exprs like:
        // win_expr(T_WIN_FUN_RANK()), partition_by([testwn1.c], [testwn1.a], [testwn1.b]),
        // win_expr(T_WIN_FUN_RANK()), partition_by([testwn1.b], [testwn1.a])
        // if so, we need a idx array to correctly compare partition exprs

        // partition exprs may have `partition_by(t.a, t.a)`
        // in this case, reorderd_pby_row_idx_ will be [0, 0]
        bool same_part_order = (it->wf_info_.partition_exprs_.count() <= all_part_exprs_.count());
        for (int i = 0; OB_SUCC(ret) && i < it->wf_info_.partition_exprs_.count() && same_part_order; i++) {
          same_part_order = (it->wf_info_.partition_exprs_.at(i) == all_part_exprs_.at(i));
        }
        if (OB_UNLIKELY(!same_part_order)) {
          LOG_TRACE("orders of partition exprs are different", K(it->wf_info_),
                    K(it->wf_info_.partition_exprs_), K(all_part_exprs_));
          const ObExprPtrIArray &part_exprs = it->wf_info_.partition_exprs_;

          it->reordered_pby_row_idx_ = (int32_t *)local_allocator_->alloc(sizeof(int32_t) * part_exprs.count());
          if (OB_ISNULL(it->reordered_pby_row_idx_)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("allocate memory failed", K(ret));
          } else {
            for (int i = 0; OB_SUCC(ret) && i < part_exprs.count(); i++) {
              int32_t idx = -1;
              for (int j = 0; idx == -1 && j < all_part_exprs_.count(); j++) {
                if (all_part_exprs_.at(j) == part_exprs.at(i)) {
                  idx = j;
                }
              }
              if (OB_UNLIKELY(idx == -1)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("invalid part expr idx", K(ret));
              } else {
                it->reordered_pby_row_idx_[i] = idx;
              }
            }
          }
        } else {
          it->reordered_pby_row_idx_ = nullptr;
        }
      }
    }

    if (OB_SUCC(ret)) {// init batch_ctx_
      if (OB_FAIL(init_batch_ctx())) {
        LOG_WARN("init batch context failed", K(ret));
      }
    }
    // init sing partition parallel execution members
    if (OB_SUCC(ret)) {
      ObSEArray<ObExpr *, 8> all_wf_exprs;
      void *row_meta_buf = nullptr;
      if (MY_SPEC.single_part_parallel_) {
        FOREACH_WINCOL(END_WF) {
          if (OB_FAIL(all_wf_exprs.push_back(it->wf_info_.expr_))) {
            LOG_WARN("push back element failed", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_ISNULL(row_meta_buf = local_allocator_->alloc(sizeof(RowMeta)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
          } else {
            all_wf_res_row_meta_ = new(row_meta_buf)RowMeta(local_allocator_);
            all_wf_res_row_meta_->set_allocator(local_allocator_);
            if (OB_FAIL(all_wf_res_row_meta_->init(all_wf_exprs, 0, false))) {
              LOG_WARN("init wf results row meta failed", K(ret));
            }
          }
        }
      } else {
        all_wf_res_row_meta_ = nullptr;
        sp_merged_row_ = nullptr;
      }
    }
    if (OB_SUCC(ret) && MY_SPEC.range_dist_parallel_) {
      // init rd_coord_row_meta_;
      rd_coord_row_meta_ = OB_NEWx(RowMeta, local_allocator_, local_allocator_);
      if (OB_ISNULL(rd_coord_row_meta_)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret));
        // rd_coord_exprs + frame_offset
      } else if (OB_FAIL(rd_coord_row_meta_->init(MY_SPEC.rd_coord_exprs_, sizeof(int64_t), false))) {
        LOG_WARN("init rd_coord_row_meta failed", K(ret));
      }
    }
    // create stores
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(create_stores(tenant_id))) {
      LOG_WARN("create stores failed", K(ret));
    }
  }
  return ret;
}

int ObWindowFunctionVecOp::init_batch_ctx()
{
  int ret = OB_SUCCESS;
  int32_t bitmap_sz = ObBitVector::word_count(MY_SPEC.max_batch_size_) * ObBitVector::BYTES_PER_WORD;
  int32_t tmp_buf_sz = MY_SPEC.max_batch_size_ * sizeof(ObCompactRow *) + bitmap_sz * 4
                       + sizeof(int64_t) * MY_SPEC.max_batch_size_ * 2;
  char *tmp_buf = nullptr;
  int32_t offset = 0;
  if (OB_ISNULL(tmp_buf = (char *)local_allocator_->alloc(tmp_buf_sz))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  } else {
    MEMSET(tmp_buf, 0, tmp_buf_sz);
    batch_ctx_.stored_rows_ = reinterpret_cast<const ObCompactRow **>(tmp_buf + offset);
    offset += sizeof(const ObCompactRow *) * MY_SPEC.max_batch_size_;
    batch_ctx_.nullres_skip_ = to_bit_vector(tmp_buf + offset);
    batch_ctx_.pushdown_skip_ = to_bit_vector((char *)tmp_buf + offset + bitmap_sz);
    batch_ctx_.calc_wf_skip_ = to_bit_vector((char *)tmp_buf + offset + bitmap_sz * 2);
    batch_ctx_.bound_eval_skip_ = to_bit_vector((char *)tmp_buf + offset + bitmap_sz * 3);
    offset += bitmap_sz * 4;
    batch_ctx_.upper_pos_arr_ = reinterpret_cast<int64_t *>(tmp_buf + offset);
    offset += MY_SPEC.max_batch_size_ * sizeof(int64_t);
    batch_ctx_.lower_pos_arr_ = reinterpret_cast<int64_t *>(tmp_buf + offset);
    offset +=  MY_SPEC.max_batch_size_ * sizeof(int64_t);
  }
  if (OB_SUCC(ret) && MY_SPEC.single_part_parallel_) {
    batch_ctx_.tmp_wf_res_row_ = OB_NEWx(LastCompactRow, local_allocator_, *local_allocator_);
    if (OB_ISNULL(batch_ctx_.tmp_wf_res_row_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    }
  }
  if (OB_SUCC(ret) && MY_SPEC.is_participator()) {
    batch_ctx_.tmp_input_row_ = OB_NEWx(LastCompactRow, local_allocator_, *local_allocator_);
    if (OB_ISNULL(batch_ctx_.tmp_input_row_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    }
  }
  if (OB_SUCC(ret)
      && OB_FAIL(ObVectorsResultHolder::calc_backup_size(get_all_expr(), eval_ctx_,
                                                         batch_ctx_.all_exprs_backup_buf_len_))) {
    LOG_WARN("calculate all exprs backup size failed", K(ret));
  } else if (batch_ctx_.all_exprs_backup_buf_len_ > 0
             && OB_ISNULL(batch_ctx_.all_exprs_backup_buf_ =
                            local_allocator_->alloc(batch_ctx_.all_exprs_backup_buf_len_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  }
  return ret;
}

int ObWindowFunctionVecOp::get_next_batch_from_child(int64_t batch_size,
                                                     const ObBatchRows *&child_brs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(restore_child_vectors())) {
    LOG_WARN("restore all expr datas failed", K(ret));
  } else {
    bool found = false;
    while (!found && OB_SUCC(ret)) {
      clear_evaluated_flag();
      if (OB_FAIL(child_->get_next_batch(batch_size, child_brs))) {
        LOG_WARN("get child next batch failed", K(ret));
      } else if (child_brs->end_) {
        found = true;
      } else {
        int64_t size = child_brs->size_;
        found = (child_brs->skip_->accumulate_bit_cnt(size) != size);
      }
    }
    LOG_TRACE("get next batch from child", K(found), K(child_brs->size_));
    // max rows of overwrote is MY_SPEC.max_batch_size_
    if (OB_SUCC(ret) && found && OB_FAIL(backup_child_vectors(MY_SPEC.max_batch_size_))) {
      LOG_WARN("save expr data failed", K(ret));
    }
    if (OB_SUCC(ret) && found && MY_SPEC.is_participator()) {
      // for each iteration of child input, init agg_status for later calculating
      ObExpr *agg_status = MY_SPEC.wf_aggr_status_expr_;
      if (OB_FAIL(agg_status->init_vector_for_write(eval_ctx_, agg_status->get_default_res_format(),
                                                    MY_SPEC.max_batch_size_))) {
        LOG_WARN("init vector failed", K(ret));
      }
    }
    // mapping pby row to idx array
    const ObCompactRow *last_row = nullptr;
    if (OB_FAIL(ret)) {
    } else if (!found) { // do nothing
    } else if (OB_FAIL(get_last_input_row_of_prev_batch(last_row))) {
      LOG_WARN("get last row failed", K(ret));
    } else if (OB_SUCC(ret) && OB_FAIL(mapping_pby_row_to_idx_arr(*child_brs, last_row))) {
      LOG_WARN("mapping pby row to idx array failed", K(ret));
    } else {
    }
  }
  return ret;
}

template <typename ColumnFmt>
int ObWindowFunctionVecOp::mapping_pby_col_to_idx_arr(int32_t col_id, const ObExpr &part_expr,
                                                      const ObBatchRows &brs,
                                                      const cell_info *last_part_res)
{
  int ret = OB_SUCCESS;
  int32_t val_idx = col_id, step = max_pby_col_cnt_;
  const char *prev_data = nullptr, *cur_data = nullptr;
  int32_t prev_len = 0, cur_len = 0;
  int32_t prev = -1;
  int cmp_ret = 0;
  bool prev_is_null = false, cur_is_null = false;
  ColumnFmt *column = static_cast<ColumnFmt *>(part_expr.get_vector(eval_ctx_));
  ObExprPtrIArray &all_exprs = get_all_expr();
  for (int i = 0; OB_SUCC(ret) && i < brs.size_; i++, val_idx +=step) {
    if (brs.skip_->at(i)) {
      continue;
    } else if (OB_LIKELY(prev != -1)) {
      column->get_payload(i, cur_is_null, cur_data, cur_len);
      if (OB_FAIL(part_expr.basic_funcs_->row_null_first_cmp_(
            part_expr.obj_meta_, part_expr.obj_meta_,
            prev_data, prev_len, prev_is_null,
            cur_data, cur_len, cur_is_null,
            cmp_ret))) {
        LOG_WARN("null first cmp failed", K(ret));
      } else if (cmp_ret == 0) {
        pby_row_mapped_idx_arr_[val_idx] = prev;
      } else {
        int32_t new_idx = prev + 1;
        pby_row_mapped_idx_arr_[val_idx] = new_idx;
        prev = new_idx;
        prev_data = cur_data;
        prev_len = cur_len;
        prev_is_null = cur_is_null;
      }
    } else if (last_part_res != nullptr) {
      column->get_payload(i, cur_is_null, cur_data, cur_len);
      prev_is_null = last_part_res->is_null_;
      prev_len = last_part_res->len_;
      prev_data = last_part_res->payload_;
      if (OB_FAIL(part_expr.basic_funcs_->row_null_first_cmp_(
            part_expr.obj_meta_, part_expr.obj_meta_,
            prev_data, prev_len, prev_is_null,
            cur_data, cur_len, cur_is_null, cmp_ret))) {
        LOG_WARN("compare failed", K(ret));
      } else if (cmp_ret == 0) {
        prev = last_row_idx_arr_[col_id];
        pby_row_mapped_idx_arr_[val_idx] = prev;
      } else {
        prev = last_row_idx_arr_[col_id] + 1;
        pby_row_mapped_idx_arr_[val_idx] = prev;
        prev_data = cur_data;
        prev_len = cur_len;
        prev_is_null = cur_is_null;
      }
    } else {
      pby_row_mapped_idx_arr_[val_idx] = i;
      column->get_payload(i, prev_is_null, prev_data, prev_len);
      prev = i;
    }
  }
  return ret;
}

int ObWindowFunctionVecOp::eval_prev_part_exprs(const ObCompactRow *last_row, ObIAllocator &alloc,
                                                const ObExprPtrIArray &part_exprs,
                                                common::ObIArray<cell_info> &last_part_infos)
{
  int ret = OB_SUCCESS;
  ObExprPtrIArray &all_exprs = get_all_expr();
  bool backuped_child_vector = false;
  ObDataBuffer backup_alloc((char *)batch_ctx_.all_exprs_backup_buf_, batch_ctx_.all_exprs_backup_buf_len_);
  ObVectorsResultHolder tmp_holder(&backup_alloc);
  for (int i = 0; OB_SUCC(ret) && last_row != nullptr && i < part_exprs.count(); i++) {
    ObExpr *part_expr = part_exprs.at(i);
    int part_expr_field_idx = -1;
    for (int j = 0; j < all_exprs.count() && part_expr_field_idx == -1; j++) {
      if (part_expr == all_exprs.at(j)) {
        part_expr_field_idx = j;
      }
    }
    if (part_expr_field_idx != -1) { // partition expr is child input
      const char *payload = nullptr;
      bool is_null = false;
      int32_t len = 0;
      last_row->get_cell_payload(input_row_meta_, part_expr_field_idx, payload, len);
      is_null = last_row->is_null(part_expr_field_idx);
      if (OB_FAIL(last_part_infos.push_back(cell_info(is_null, len, payload)))) {
        LOG_WARN("push back element failed", K(ret));
      }
    } else {
      if (backuped_child_vector) {
      } else if (OB_FAIL(tmp_holder.init(all_exprs, eval_ctx_))) {
        LOG_WARN("init result holder failed", K(ret));
      } else if (OB_FAIL(tmp_holder.save(1))) {
        LOG_WARN("save vector results failed", K(ret));
      } else {
        backuped_child_vector = true;
        if (OB_FAIL(attach_row_to_output(last_row))) {
          LOG_WARN("attach row failed", K(ret));
        }
      }
      int64_t mock_skip_data = 0;
      ObBitVector *mock_skip = to_bit_vector(&mock_skip_data);
      EvalBound tmp_bound(1, true);
      char *part_res_buf = nullptr;
      const char *payload = nullptr;
      int32_t len = 0;
      bool is_null = false;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(part_expr->eval_vector(eval_ctx_, *mock_skip, tmp_bound))) {
        LOG_WARN("eval vector failed", K(ret));
      } else {
        ObIVector *part_res_vec = part_expr->get_vector(eval_ctx_);
        part_res_vec->get_payload(0, is_null, payload, len);
        if (is_null || len <= 0) {
          part_res_buf = nullptr;
          len = 0;
        } else if (OB_ISNULL(part_res_buf = (char *)alloc.alloc(len))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate memory failed", K(ret));
        } else {
          MEMCPY(part_res_buf, payload, len);
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(last_part_infos.push_back(cell_info(is_null, len, part_res_buf)))) {
          LOG_WARN("push back element failed", K(ret));
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (backuped_child_vector && OB_FAIL(tmp_holder.restore())) {
    LOG_WARN("restore vector results failed", K(ret));
  }
  return ret;
}

int ObWindowFunctionVecOp::get_last_input_row_of_prev_batch(const ObCompactRow *&last_row)
{
  int ret = OB_SUCCESS;
  last_row = nullptr;
  // search order: current->processed->first
  winfunc::RowStore *fetch_store = nullptr;
  if (!input_stores_.cur_->is_empty()) {
    fetch_store = input_stores_.cur_;
  } else if (!input_stores_.processed_->is_empty()) {
    fetch_store = input_stores_.processed_;
  } else if (MY_SPEC.range_dist_parallel_ && !input_stores_.first_->is_empty()) {
    fetch_store = input_stores_.first_;
  }
  if (OB_ISNULL(fetch_store)) {
    // do nothing
  } else if (OB_FAIL(fetch_store->get_row(fetch_store->stored_row_cnt_ - 1, last_row))) {
    LOG_WARN("get row failed", K(ret));
  } else {
  }
  return ret;
}

// In vectorization 2.0, data accessing of expr is specified by `VectorFormat`,
// there's no easy way to access a complete partition row with multiple columns.
// In order to easily calculate partition, we map pby expr to idx array here.
// For each partition expr, pby_mapped_idx_arr[row_idx] = (get_payload(row_idx) == get_payload(row_idx - 1) ?
//                                                           pby_mapped_idx_arr[row_idx-1]
//                                                           : row_idx);
// For example, suppose partition exprs are `<int, int, int>` and inputs are:
//  1, 2, 3
//  1, 2, 3
//  1, 2, 4
//  1, 2, 4
//  2, 3, 1
//  2, 3, 1
// mapped idx arrays are:
//  0, 0, 0
//  0, 0, 0
//  0, 0, 2
//  0, 0, 2
//  4, 4, 4
//  4, 4, 4
int ObWindowFunctionVecOp::mapping_pby_row_to_idx_arr(const ObBatchRows &child_brs,
                                                      const ObCompactRow *last_row)
{
#define MAP_FIXED_COL_CASE(vec_tc)                                                                 \
  case (vec_tc): {                                                                                 \
    ret = mapping_pby_col_to_idx_arr<ObFixedLengthFormat<RTCType<vec_tc>>>(                        \
      i, *part_exprs.at(i), child_brs, last_part_cell);                                            \
  } break

  int ret = OB_SUCCESS;
  ObIArray<ObExpr *> &part_exprs = all_part_exprs_;
  ObArenaAllocator tmp_mem_alloc(ObModIds::OB_SQL_WINDOW_LOCAL, OB_MALLOC_NORMAL_BLOCK_SIZE,
                                 ctx_.get_my_session()->get_effective_tenant_id(),
                                 ObCtxIds::WORK_AREA);
  ObSEArray<cell_info, 8> part_cell_infos;
  // calculate last part expr of previous batch first
  // memset all idx to -1
  if (max_pby_col_cnt_ > 0) {
    MEMSET(pby_row_mapped_idx_arr_, -1, child_brs.size_ * sizeof(int32_t) * max_pby_col_cnt_);
    if (OB_FAIL(eval_prev_part_exprs(last_row, tmp_mem_alloc, part_exprs, part_cell_infos))) {
      LOG_WARN("eval last partition exprs of last row failed", K(ret));
    }
    bool prev_row_not_null = (last_row != nullptr);
    for (int i = 0; OB_SUCC(ret) && i < part_exprs.count(); i++) {
      const cell_info *last_part_cell = (prev_row_not_null ? &(part_cell_infos.at(i)) : nullptr);
      if (OB_ISNULL(part_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null expr", K(ret));
      } else if (OB_FAIL(part_exprs.at(i)->eval_vector(eval_ctx_, child_brs))) {
        LOG_WARN("eval vector failed", K(ret));
      } else {
        VectorFormat fmt = part_exprs.at(i)->get_format(eval_ctx_);
        VecValueTypeClass tc = part_exprs.at(i)->get_vec_value_tc();
        switch (fmt) {
        case VEC_FIXED: {
          switch (tc) {
            LST_DO_CODE(MAP_FIXED_COL_CASE, FIXED_VEC_LIST);
          default: {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected vector type class", K(tc));
          }
          }
          break;
        }
        case VEC_UNIFORM: {
          ret = mapping_pby_col_to_idx_arr<ObUniformFormat<false>>(i, *part_exprs.at(i), child_brs,
                                                                   last_part_cell);
          break;
        }
        case VEC_UNIFORM_CONST: {
          ret = mapping_pby_col_to_idx_arr<ObUniformFormat<true>>(i, *part_exprs.at(i), child_brs,
                                                                  last_part_cell);
          break;
        }
        case VEC_DISCRETE: {
          ret = mapping_pby_col_to_idx_arr<ObDiscreteFormat>(i, *part_exprs.at(i), child_brs,
                                                             last_part_cell);
          break;
        }
        case VEC_CONTINUOUS: {
          ret = mapping_pby_col_to_idx_arr<ObContinuousFormat>(i, *part_exprs.at(i), child_brs,
                                                               last_part_cell);
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected data format", K(ret), K(fmt), K(tc));
        }
        }
        if (OB_FAIL(ret)) {
          LOG_WARN("mapping pby col to idx array failed", K(ret), K(i), K(*part_exprs.at(i)));
        }
      }
    }

    // record last_row_idx_arr_
    for (int i = child_brs.size_ - 1; i >= 0; i--) {
      if (child_brs.skip_->at(i)) {
      } else {
        MEMCPY(last_row_idx_arr_, &(pby_row_mapped_idx_arr_[i * max_pby_col_cnt_]),
               max_pby_col_cnt_ * sizeof(int32_t));
        break;
      }
    }
  }
  return ret;
#undef MAP_FIXED_COL_CASE
}

int ObWindowFunctionVecOp::partial_next_batch(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  bool do_output = false;
  while(OB_SUCC(ret) && !do_output) {
    if (OB_FAIL(do_partial_next_batch(max_row_cnt, do_output))) {
      LOG_WARN("do partial next batch failed", K(ret));
    }
  }
  return ret;
}

int ObWindowFunctionVecOp::do_partial_next_batch(const int64_t max_row_cnt, bool &do_output)
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  int64_t check_times = 0;
  int64_t output_row_cnt = MIN(max_row_cnt, MY_SPEC.max_batch_size_);
  ObEvalCtx::BatchInfoScopeGuard guard(eval_ctx_);
  guard.set_batch_idx(0);
  if (OB_UNLIKELY(iter_end_)) {
    brs_.size_ = 0;
    brs_.end_ = true;
  } else if (OB_FAIL(ctx_.check_status())) {
    LOG_WARN("check  physical plan status failed", K(ret));
  } else if (OB_ISNULL(ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid null session", K(ret));
  } else {
    const int64_t tenant_id = ctx_.get_my_session()->get_effective_tenant_id();
    if (input_stores_.processed_->to_output_rows() == 0 && input_stores_.processed_->count() > 0) {
      // all processed rows are outputed, reset processed
      input_stores_.processed_->reset();
      FOREACH_WINCOL(END_WF) {
        it->res_->processed_->reset();
      }
    }
    // step.1 vec compute
    WinFuncColExpr *first = wf_list_.get_first();
    WinFuncColExpr *end = wf_list_.get_header();
    int64_t rows_output_cnt = input_stores_.cur_->to_output_rows() + input_stores_.processed_->to_output_rows();
    while (OB_SUCC(ret) && rows_output_cnt < output_row_cnt) {
      // reset to `row_cnt_` to  `stored_row_cnt_`
      // we may get extra rows of different big partition in one batch, those rows are added to `current` store but are not computed.
      // Hence, in the beginning of next partition iteration, resetting is necessary for computing next partition values.
      input_stores_.cur_->row_cnt_ = input_stores_.cur_->stored_row_cnt_;
      if (OB_FAIL(get_next_partition(check_times))) {
        LOG_WARN("get next partition failed", K(ret));
      } else {
        rows_output_cnt =
          input_stores_.processed_->to_output_rows() + input_stores_.cur_->to_output_rows();
      }
      if (OB_SUCC(ret) && child_iter_end_) {
        break;
      }
    }
    // step.2 vec output
    if (OB_SUCC(ret)) {
      if (MY_SPEC.single_part_parallel_) {
        brs_.end_ = true;
        brs_.size_ = 0;
        do_output = true;
      } else if (MY_SPEC.range_dist_parallel_
                 && child_iter_end_
                 && input_stores_.cur_->to_compute_rows() == 0
                 && !last_part_saved_) {
        last_part_saved_ = true;
        if (!first_part_saved_) {
          // only one partition
          first_part_saved_ = true;
          SWAP_STORES(first, cur);
        } else {
          SWAP_STORES(last, cur);
        }
        // Rows stored in `processed_` neither in the first partition nor in the last partition
        if (input_stores_.processed_->to_output_rows() > 0) {
          if (OB_FAIL(output_batch_rows(input_stores_.processed_->to_output_rows()))) {
            LOG_WARN("output batch rows failed", K(ret));
          }
        } else {
          brs_.size_ = 0;
          brs_.end_ = true;
        }
        do_output = true;
      } else if (OB_FAIL(output_batch_rows(output_row_cnt))) {
        LOG_WARN("output batch rows failed", K(ret));
      } else {
        do_output = true;
        if (OB_SUCC(ret) && MY_SPEC.is_participator() && brs_.end_) {
          // to make sure to send piece data to datahub even though no row fetched
          // for some pushdown wf expr, it's partition count may not exceed DOP, hence no piece data
          // is sent to datahub. When iteration is done, send empty piece data anyway.
          if (OB_FAIL(rwf_send_empty_piece_data())) {
            LOG_WARN("send empty piece data failed", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObWindowFunctionVecOp::get_next_partition(int64_t &check_times)
{
  int ret = OB_SUCCESS;
  int64_t batch_size = MY_SPEC.max_batch_size_;
  bool found_next_part = false;
  const ObBatchRows *child_brs = nullptr;
  winfunc::RowStore &current = *input_stores_.cur_;
  bool first_batch = current.count() == 0;
  int64_t row_idx = -1;
  WinFuncColExpr *first = wf_list_.get_first();
  WinFuncColExpr *end = wf_list_.get_header();
  ObExpr *agg_code_expr = MY_SPEC.wf_aggr_status_expr_;
  if (child_iter_end_) {
    if (!first_batch) {
      // wf value haven't computed, why add row into input_stores_.cur_ first?
      // aggr_res_row is same as last row of current partition except aggr_status_expr
      // iteration in `compute_wf_values` will encounter aggr_res_row, and do wf computing.
      // while aggr_res_row has same frame as before, computing will be replaced as copying results.
      if (OB_FAIL(add_aggr_res_row_for_participator(end, current))) {
        LOG_WARN("add aggr result row for last partition failed", K(ret));
      } else if (OB_FAIL(compute_wf_values(end, check_times))) {
        LOG_WARN("compute wf values failed", K(ret));
      }
    }
  } else if (OB_FAIL(get_next_batch_from_child(batch_size, child_brs))) {
    LOG_WARN("get next batch from child failed", K(ret));
  } else if ((child_brs->end_ && 0 == child_brs->size_)
             || (child_brs->size_ == (row_idx = next_nonskip_row_index(row_idx, *child_brs)))) {
    child_iter_end_ = true;
    if (!first_batch) {
      if (OB_FAIL(add_aggr_res_row_for_participator(end, current))) {
        LOG_WARN("add aggr result row for last_partition failed", K(ret));
      } else if (OB_FAIL(compute_wf_values(end, check_times))) {
        LOG_WARN("compute wf values failed", K(ret));
      }
    }
  } else {
    if (child_brs->end_) {
      child_iter_end_ = true;
    }
    ObEvalCtx::BatchInfoScopeGuard guard(eval_ctx_);
    int64_t part_start_idx = row_idx;
    if (first_batch && child_brs->size_ > 0) {
      // new partition
      // 1. save pby row
      // 2. update first partition row idx
      // 3. detect and send aggr status if participator
      if (OB_FAIL(save_pby_row_for_wf(end, row_idx))) {
        LOG_WARN("save pby row failed", K(ret));
      } else if (OB_FAIL(update_part_first_row_idx(end))) {
        LOG_WARN("update first row idx of partition failed", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(process_child_batch(row_idx, child_brs, check_times))) {
      LOG_WARN("process child batch rows failed", K(ret));
    }
  }
  return ret;
}

int ObWindowFunctionVecOp::process_child_batch(const int64_t batch_idx,
                                               const ObBatchRows *child_brs, int64_t &check_times)
{
  int ret = OB_SUCCESS;
  int64_t batch_size = MY_SPEC.max_batch_size_;
  int64_t row_idx = batch_idx;
  WinFuncColExpr *first = wf_list_.get_first();
  WinFuncColExpr *end = wf_list_.get_header();
  ObEvalCtx::BatchInfoScopeGuard guard(eval_ctx_);
  guard.set_batch_size(child_brs->size_);
  guard.set_batch_idx(row_idx);
  // found next part means finding a big partition
  // wf part exprs array are organized as <a, b, c>, <a, b>, <a>
  // if `<a>` changed, found_next_part = true
  bool found_next_part = false;
  if (OB_FAIL(check_stack_overflow())) {
    LOG_WARN("stack overflow", K(ret));
  }
  while (OB_SUCC(ret) && !found_next_part) { // find a big partition
    bool need_loop_util_child_brs_end = true;
    while (OB_SUCC(ret) && need_loop_util_child_brs_end) {
      need_loop_util_child_brs_end = false;
      found_next_part = false;
      winfunc::RowStore &current = *input_stores_.cur_;
      winfunc::RowStore &processed = *input_stores_.processed_;
      // winfunc::RowStore &remain = processed.is_empty() ? processed : current;
      bool need_swap_store = processed.is_empty();
      int64_t part_start_idx = row_idx;
      while (OB_SUCC(ret) && row_idx < child_brs->size_ && !found_next_part) {
        bool same_part = true;
        guard.set_batch_idx(row_idx);
        if (child_brs->skip_->at(row_idx)) {
          row_idx++;
          continue;
        }
        if (OB_FAIL(check_same_partition(*first, same_part))) {
          LOG_WARN("check same partition failed", K(ret));
        } else if (OB_UNLIKELY(!same_part)) {
          // find same partition of any other window function
          if (OB_FAIL(find_same_partition_of_wf(end))) {
            LOG_WARN("find same partition of wf failed", K(ret));
          } else {
            // new big partition or not
            found_next_part = (end == wf_list_.get_header());
            // 1. save pby row
            // 2. set values for aggr status expr if possible
            // 3. detect and report aggr status
            // 4. add batch rows into current store
            // 5. add aggr res row into current store
            // update part first row idx after computing wf values
            if (OB_FAIL(save_pby_row_for_wf(end, eval_ctx_.get_batch_idx()))) {
              LOG_WARN("save partition groupby row failed", K(ret));
            } else if (OB_FAIL(detect_and_report_aggr_status(*child_brs, part_start_idx, row_idx))) {
              LOG_WARN("detect and report aggr status failed", K(ret));
            } else if (OB_FAIL(current.add_batch_rows(
                         get_all_expr(), input_row_meta_, eval_ctx_,
                         EvalBound(child_brs->size_, part_start_idx, row_idx, false),
                         *child_brs->skip_, true, nullptr, true))) {
              LOG_WARN("add batch rows failed", K(ret));
            } else if (OB_FAIL(add_aggr_res_row_for_participator(end, *input_stores_.cur_))) {
              // new partition found, add aggr result row
              LOG_WARN("add aggregate result row for participator failed", K(ret));
            } else {
              LOG_TRACE("found new partition", K(found_next_part), K(part_start_idx), K(row_idx),
                        K(child_brs), K(need_swap_store));
            }
          }
          if (OB_FAIL(ret)) {
            // do nothing
          } else if (OB_FAIL(compute_wf_values(end, check_times))) {
            LOG_WARN("compute wf values failed", K(ret));
          } else {
            part_start_idx = row_idx;
          }
        } else {
          row_idx++;
        }
      }
      if (OB_SUCC(ret)) {
        if (found_next_part) {
          if (need_swap_store) {
            // switch %cur_ and %processed_ row store if necessary
            // %cur_ always be rows store we are computing.
            // And rows in %processed_ row store are all computed and ready to output.
            // swap $cur_ and $processed_ row store

            SWAP_STORES(cur, processed);
            if (MY_SPEC.range_dist_parallel_ && !first_part_saved_) {
              // save first partition by swapping
              first_part_saved_ = true;
              SWAP_STORES(first, processed);
            }
          }
          // store maybe swapped, update partitions' first row idxes.
          if (need_swap_store && OB_FAIL(update_part_first_row_idx(END_WF))) {
            LOG_WARN("save partition first row idx failed", K(ret));
          } else {
            need_loop_util_child_brs_end = true;
          }
        } else if (!child_iter_end_) {
          // if part_start_id < row_idx, add rest of rows into current store
          // note that, if need_swap_store == true, it means we haven't got a complete big partition yet
          // and iteration needs going on.
          // If need_swap_store == false, rest of rows are not included in big partition
          // and should not count as computed rows (haven't compute wf values yet). We just add those rows
          // and set `current.row_cnt_` to `wf_list_.get_last()->part_first_row_idx_`.
          // `current.row_cnt_` will be adjusted back to `current.store_row_cnt_` in the beginning `do_partial_next_batch`
          if (part_start_idx < row_idx) {
            if (OB_FAIL(detect_and_report_aggr_status(*child_brs, part_start_idx, row_idx))) {
              LOG_WARN("detect and report aggr status failed", K(ret));
            } else if (OB_FAIL(current.add_batch_rows(
                         get_all_expr(), input_row_meta_, eval_ctx_,
                         EvalBound(child_brs->size_, part_start_idx, row_idx,
                                   child_brs->all_rows_active_),
                         *child_brs->skip_, need_swap_store, nullptr, true))) {
              LOG_WARN("add batch rows failed", K(ret));
            }
          }
          if (OB_FAIL(ret)) {
          } else if (need_swap_store) { // this means we haven't got a complete big partition
            if (OB_FAIL(get_next_batch_from_child(batch_size, child_brs))) {
              LOG_WARN("get next batch from child failed", K(ret));
            } else {
              child_iter_end_ = child_brs->end_
                                || (child_brs->size_ == child_brs->skip_->accumulate_bit_cnt(child_brs->size_));
              if (child_iter_end_) {
                // all child rows are iterated
                // add aggr result row and compute wf values
                if (OB_FAIL(
                             add_aggr_res_row_for_participator(END_WF, *input_stores_.cur_))) {
                  LOG_WARN("add aggregate result row failed", K(ret));
                } else if (OB_FAIL(compute_wf_values(END_WF, check_times))) {
                  LOG_WARN("compute wf values failed", K(ret));
                } else {
                  found_next_part = true;
                }
              } else { // continue to read rows from child batch
                row_idx = 0;
                guard.set_batch_size(child_brs->size_);
              }
            }
          } else {
            found_next_part = true;
            current.row_cnt_ = wf_list_.get_last()->part_first_row_idx_;
          }
        } else {
          // all child rows are iterated
          found_next_part = true;
          if (part_start_idx < row_idx) {
            if (OB_FAIL(detect_and_report_aggr_status(*child_brs, part_start_idx, row_idx))) {
              LOG_WARN("detect and report aggr status failed", K(ret));
            } else if (OB_FAIL(current.add_batch_rows(
                         get_all_expr(), input_row_meta_, eval_ctx_,
                         EvalBound(child_brs->size_, part_start_idx, row_idx, false),
                         *child_brs->skip_, true, nullptr, true))) {
              LOG_WARN("add batch rows failed", K(ret));
            } else if (OB_FAIL(add_aggr_res_row_for_participator(wf_list_.get_header(),
                                                                 *input_stores_.cur_))) {
              LOG_WARN("add aggregate result row failed", K(ret));
            } else if (OB_FAIL(compute_wf_values(END_WF, check_times))) {
              LOG_WARN("compute wf values failed", K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

int64_t ObWindowFunctionVecOp::next_nonskip_row_index(int64_t cur_idx, const ObBatchRows &brs)
{
  int64_t res = cur_idx + 1;
  for (; res < brs.size_ && brs.skip_->at(res); res++) {
  }
  return res;
}

int ObWindowFunctionVecOp::save_pby_row_for_wf(WinFuncColExpr *end_wf, const int64_t batch_idx)
{
  int ret = OB_SUCCESS;
  if (max_pby_col_cnt_ > 0) {
    int32_t offset = batch_idx * max_pby_col_cnt_;
    int32_t *pby_row_idxes = &(pby_row_mapped_idx_arr_[offset]);
    FOREACH_WINCOL(end_wf)
    {
      MEMCPY(it->pby_row_mapped_idxes_, pby_row_idxes, sizeof(int32_t) * max_pby_col_cnt_);
    }
  }
  return ret;
}

int ObWindowFunctionVecOp::update_part_first_row_idx(WinFuncColExpr *end)
{
  int ret = OB_SUCCESS;
  FOREACH_WINCOL(end)
  {
    it->part_first_row_idx_ = it->res_->cur_->count();
  }
  return ret;
}

int ObWindowFunctionVecOp::check_same_partition(WinFuncColExpr &wf_col, bool &same)
{
  int ret = OB_SUCCESS;
  same = (wf_col.wf_info_.partition_exprs_.count() <= 0);
  if (!same) {
    int64_t part_cnt = wf_col.wf_info_.partition_exprs_.count();
    int64_t row_idx = eval_ctx_.get_batch_idx();
    int32_t offset = max_pby_col_cnt_ * row_idx;
    int32_t *pby_row_idxes = &(pby_row_mapped_idx_arr_[offset]);
    if (OB_UNLIKELY(wf_col.reordered_pby_row_idx_ == nullptr)) {
      same = (MEMCMP(pby_row_idxes, wf_col.pby_row_mapped_idxes_, sizeof(int32_t) * part_cnt) == 0);
    } else {
      same = true;
      for (int i = 0; i < part_cnt && same; i++) {
        int32_t idx = wf_col.reordered_pby_row_idx_[i];
        same = (wf_col.pby_row_mapped_idxes_[idx] == pby_row_idxes[idx]);
      }
    }
  }
  return ret;
}

int ObWindowFunctionVecOp::find_same_partition_of_wf(WinFuncColExpr *&end_wf)
{
  int ret = OB_SUCCESS;
  end_wf = wf_list_.get_header();
  bool same = false;
  FOREACH_WINCOL(END_WF) {
    if (OB_FAIL(check_same_partition(*it, same))) {
      LOG_WARN("check same partition failed", K(ret));
    } else if (same) {
      end_wf = it;
      break;
    }
  }
  return ret;
}

int ObWindowFunctionVecOp::coordinate()
{
  int ret = OB_SUCCESS;
  if (MY_SPEC.single_part_parallel_) {
    if (OB_FAIL(collect_sp_partial_results())) {
      LOG_WARN("collect single partition partial results failed", K(ret));
    }
  } else if (MY_SPEC.range_dist_parallel_) {
    if (OB_FAIL(rd_fetch_patch())) {
      LOG_WARN("fetch patch info from PX COORD failed", K(ret));
    } else {
      LOG_DEBUG("fetch patch", K(*rd_patch_));
      last_output_row_idx_ = OB_INVALID_INDEX;
      SWAP_STORES(first, cur);
      patch_first_ = true;
      if (input_stores_.last_->count() <= 0) {
        // only one partition
        patch_last_ = true;
      }
    }
  }
  return ret;
}

int ObWindowFunctionVecOp::final_next_batch(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  if (MY_SPEC.range_dist_parallel_) {
    if (OB_FAIL(rd_output_final_batch(std::min(max_row_cnt, MY_SPEC.max_batch_size_)))) {
      LOG_WARN("output first part failed", K(ret));
    } else {
      if (brs_.end_ && brs_.size_ == 0 && !first_part_outputed_) {
        first_part_outputed_ = true;
        // now output last part if possible
        if (input_stores_.last_->to_output_rows() > 0) {
          brs_.end_ = false;
          iter_end_ = false;
          SWAP_STORES(first, cur);
          SWAP_STORES(last, cur);
          patch_first_ = false;
          patch_last_ = true;
          ret = rd_output_final_batch(std::min(max_row_cnt, MY_SPEC.max_batch_size_));
        }
      }
    }
  } else {
    // ObEvalCtx::BatchInfoScopeGuard guard(eval_ctx_);
    // guard.set_batch_idx(0);
    if (OB_UNLIKELY(iter_end_)) {
      brs_.size_ = 0;
      brs_.end_ = true;
    } else if (OB_FAIL(output_batch_rows(std::min(max_row_cnt, MY_SPEC.max_batch_size_)))) {
      LOG_WARN("output batch rows failed", K(ret));
    }
  }
  return ret;
}

int ObWindowFunctionVecOp::output_batch_rows(const int64_t output_row_cnt)
{
  int ret = OB_SUCCESS;
  winfunc::RowStore &processed = *input_stores_.processed_;
  winfunc::RowStore &current = *input_stores_.cur_;
  if (OB_UNLIKELY(processed.row_cnt_ != processed.stored_row_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rows in processed row store should be all computed", K(ret), K(processed));
  } else {
    FOREACH_WINCOL(END_WF)
    {
      if (OB_UNLIKELY(it->res_->processed_->row_cnt_ != processed.row_cnt_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("rows number in input stores and result stores should be same", K(ret),
                 K(*it->res_->processed_), K(processed));
      } else if (OB_UNLIKELY(it->res_->processed_->row_cnt_
                             != it->res_->processed_->stored_row_cnt_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("rows in processed row store should be all computed", K(ret),
                 K(*it->res_->processed_));
      }
      // only last wf contains exact row_cnt_ for output.
      if (it == wf_list_.get_last() && OB_UNLIKELY(it->res_->cur_->row_cnt_ != current.row_cnt_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("rows number in input stores and result stores should be same", K(ret),
                 K(*it->res_->cur_), K(current), K(it->wf_idx_));
      }
    }
    if (OB_SUCC(ret)) {
      int64_t rows_cnt_processed = std::min(processed.to_output_rows(), output_row_cnt);
      int64_t rows_cnt_current = std::min(current.to_output_rows(), output_row_cnt - rows_cnt_processed);
      int64_t word_cnt = ObBitVector::word_count(rows_cnt_processed + rows_cnt_current);

      if (backuped_size_ < rows_cnt_processed + rows_cnt_current) {
        if (OB_FAIL(restore_child_vectors())) {
          LOG_WARN("restore child vector results failed", K(ret));
        } else if (OB_FAIL(backup_child_vectors(rows_cnt_processed + rows_cnt_current))) {
          LOG_WARN("backup child vector results failed", K(ret));
        }
      }
      MEMSET(brs_.skip_, 0, word_cnt * ObBitVector::BYTES_PER_WORD);
      brs_.all_rows_active_ = true;

      int64_t outputed_cnt = 0, outputed_wf_res_cnt = 0;
      // `output_stored_rows` gets batch rows from row store and write row into exprs
      // if rows of current batch are distributed in different blocks, all blocks are loaded must be valid
      winfunc::StoreGuard store_guard(*this);
      if (OB_FAIL(output_stored_rows(rows_cnt_processed, rows_cnt_current, input_stores_, outputed_cnt))) {
        LOG_WARN("output processed store rows failed", K(ret));
      } else {
        FOREACH_WINCOL(END_WF) {
          if (OB_FAIL(output_stored_rows(rows_cnt_processed, rows_cnt_current, *it, outputed_wf_res_cnt))) {
            LOG_WARN("output stored rows failed", K(ret));
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_UNLIKELY(outputed_cnt != outputed_wf_res_cnt)){
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected output rows", K(outputed_cnt), K(outputed_wf_res_cnt));
      } else if (MY_SPEC.is_consolidator()) {
        // partial aggregate row with aggr_status < 0 can't be outputed
        VectorFormat agg_status_fmt = MY_SPEC.wf_aggr_status_expr_->get_format(eval_ctx_);
        if (agg_status_fmt == VEC_FIXED) {
          ObFixedLengthFormat<int64_t> *aggr_status_data =
            static_cast<ObFixedLengthFormat<int64_t> *>(MY_SPEC.wf_aggr_status_expr_->get_vector(eval_ctx_));
          for (int i = 0; i < outputed_cnt; i++) {
            if (*reinterpret_cast<const int64_t *>(aggr_status_data->get_payload(i)) < 0) {
              brs_.skip_->set(i);
              brs_.all_rows_active_ = false;
            }
          }
        } else if (agg_status_fmt == VEC_UNIFORM) {
          ObUniformFormat<false> *aggr_status_data =
            static_cast<ObUniformFormat<false> *>(MY_SPEC.wf_aggr_status_expr_->get_vector(eval_ctx_));
          for (int i = 0; i < outputed_cnt; i++) {
            if (*reinterpret_cast<const int64_t *>(aggr_status_data->get_payload(i)) < 0) {
              brs_.skip_->set(i);
              brs_.all_rows_active_ = false;
            }
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected agg status format", K(ret), K(agg_status_fmt));
        }
      }
      if (OB_FAIL(ret)) {
      } else {
        if (outputed_cnt > 0) {
          FOREACH_WINCOL(END_WF)
          {
            it->wf_info_.expr_->get_eval_info(eval_ctx_).cnt_ = eval_ctx_.get_batch_idx();
            it->wf_info_.expr_->set_evaluated_projected(eval_ctx_);
          }
          ObIArray<ObExpr *> &all_exprs = get_all_expr();
          for (int i = 0; i < all_exprs.count(); i++) { // aggr status is the last expr
            all_exprs.at(i)->get_eval_info(eval_ctx_).cnt_ = eval_ctx_.get_batch_idx();
            all_exprs.at(i)->set_evaluated_projected(eval_ctx_);
          }
        }
        brs_.size_ = outputed_cnt;
        if (MY_SPEC.is_consolidator()) {
          WinFuncColExpr *last_wf = wf_list_.get_last();
          winfunc::RowStore &current_res = *(last_wf->res_->cur_);
          brs_.end_ = (current_res.to_output_rows() == 0 && current.to_compute_rows() == 0);
        } else {
          brs_.end_ = (current.to_output_rows() == 0 && current.to_compute_rows() == 0);
        }
        iter_end_ = brs_.end_;
      }
    }
  }
  return ret;
}

int ObWindowFunctionVecOp::output_stored_rows(const int64_t out_processed_cnt,
                                              const int64_t out_cur_cnt, winfunc::RowStores &store,
                                              int64_t &output_cnt)
{
  int ret = OB_SUCCESS;
  OB_ASSERT(out_processed_cnt + out_cur_cnt <= MY_SPEC.max_batch_size_);
  const ObCompactRow **input_stored_rows = batch_ctx_.stored_rows_;
  MEMSET(input_stored_rows, 0, sizeof(ObCompactRow *) * (out_processed_cnt + out_cur_cnt));
  int64_t out_start = store.processed_->output_row_idx_;
  // increase row store memory iteration age to keep row store memory
  // get processed_ first
  if (OB_FAIL(store.processed_->get_batch_rows(
        store.processed_->output_row_idx_, store.processed_->output_row_idx_ + out_processed_cnt,
        input_stored_rows))) {
    LOG_WARN("get batch rows failed", K(ret));
  } else if (OB_FAIL(store.cur_->get_batch_rows(store.cur_->output_row_idx_,
                                                store.cur_->output_row_idx_ + out_cur_cnt,
                                                &input_stored_rows[out_processed_cnt]))) {
    LOG_WARN("get batch rows failed", K(ret));
  } else {
    int64_t out_batch = out_processed_cnt + out_cur_cnt;
    if (OB_FAIL(attach_rows_to_output(input_stored_rows, out_batch))) {
      LOG_WARN("attach rows failed", K(ret));
    } else {
      store.processed_->output_row_idx_ += out_processed_cnt;
      store.cur_->output_row_idx_ += out_cur_cnt;
      output_cnt += out_processed_cnt + out_cur_cnt;
    }
  }
  return ret;
}

int ObWindowFunctionVecOp::attach_row_to_output(const ObCompactRow *row)
{
  int ret = OB_SUCCESS;
  const ObCompactRow **input_stored_rows = batch_ctx_.stored_rows_;
  input_stored_rows[0] = row;
  return attach_rows_to_output(input_stored_rows, 1);
}

int ObWindowFunctionVecOp::attach_rows_to_output(const ObCompactRow **rows, int64_t row_cnt)
{
  int ret = OB_SUCCESS;
  ObExprPtrIArray &all_exprs = get_all_expr();
  for (int i = 0; OB_SUCC(ret) && i < all_exprs.count(); i++) { // do not project const expr!!!
    if (all_exprs.at(i)->is_const_expr()) {// do nothing
    } else if (OB_FAIL(all_exprs.at(i)->init_vector_for_write(
                 eval_ctx_, all_exprs.at(i)->get_default_res_format(), row_cnt))) {
      LOG_WARN("init vector failed", K(ret));
    } else if (OB_FAIL(all_exprs.at(i)->get_vector(eval_ctx_)->from_rows(input_row_meta_, rows, row_cnt, i))) {
      LOG_WARN("from rows failed", K(ret));
    } else {
      all_exprs.at(i)->set_evaluated_projected(eval_ctx_);
    }
  }
  return ret;
}

#define SET_SP_RES(fmt)                                                                            \
  for (int i = 0; i < out_batch; i++) {                                                            \
    if (is_null) {                                                                                 \
      i_data->set_null(i);                                                                         \
    } else {                                                                                       \
      static_cast<fmt *>(i_data)->set_payload_shallow(i, res, res_len);                            \
    }                                                                                              \
  }

#define SET_SP_FIXED_RES_CASE(vec_tc)                                                              \
  case (vec_tc): {                                                                                 \
    SET_SP_RES(ObFixedLengthFormat<RTCType<vec_tc>>);                                              \
  } break

int ObWindowFunctionVecOp::output_stored_rows(const int64_t out_processed_cnt,
                                              const int64_t out_cur_cnt, WinFuncColExpr &wf_col,
                                              int64_t &outputed_cnt)
{
  int ret = OB_SUCCESS;
  OB_ASSERT(out_processed_cnt + out_cur_cnt <= MY_SPEC.max_batch_size_);
  int64_t out_batch = out_processed_cnt + out_cur_cnt;
  if (OB_FAIL(wf_col.wf_info_.expr_->init_vector_for_write(
        eval_ctx_, wf_col.wf_info_.expr_->get_default_res_format(), out_batch))) {
    LOG_WARN("init vector for write failed", K(ret));
  } else if (MY_SPEC.single_part_parallel_) {
    int64_t col_idx = wf_col.wf_idx_ - 1;
    if (OB_ISNULL(sp_merged_row_)) {
      // empty merged row, no results for current worker
      outputed_cnt = 0;
    } else if (OB_ISNULL(all_wf_res_row_meta_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null merged row", K(ret), K(all_wf_res_row_meta_));
    } else {
      const char *res = nullptr;
      int32_t res_len = 0;
      ObCompactRow *merged_row = sp_merged_row_->compact_row_;
      bool is_null = merged_row->is_null(col_idx);
      merged_row->get_cell_payload(*all_wf_res_row_meta_, col_idx, res, res_len);
      VecValueTypeClass vec_tc = wf_col.wf_info_.expr_->get_vec_value_tc();
      VectorFormat fmt = wf_col.wf_info_.expr_->get_format(eval_ctx_);
      ObIVector *i_data = wf_col.wf_info_.expr_->get_vector(eval_ctx_);
      switch (fmt) {
      case common::VEC_UNIFORM: {
        SET_SP_RES(ObUniformFormat<false>);
        break;
      }
      case common::VEC_DISCRETE: {
        SET_SP_RES(ObDiscreteFormat);
        break;
      }
      case common::VEC_CONTINUOUS: {
        SET_SP_RES(ObContinuousFormat);
        break;
      }
      case common::VEC_FIXED: {
        switch (vec_tc) {
          LST_DO_CODE(SET_SP_FIXED_RES_CASE,
                      VEC_TC_INTEGER,
                      VEC_TC_UINTEGER,
                      VEC_TC_FLOAT,
                      VEC_TC_DOUBLE,
                      VEC_TC_FIXED_DOUBLE,
                      VEC_TC_DATETIME,
                      VEC_TC_DATE,
                      VEC_TC_TIME,
                      VEC_TC_YEAR,
                      VEC_TC_BIT,
                      VEC_TC_ENUM_SET,
                      VEC_TC_TIMESTAMP_TZ,
                      VEC_TC_TIMESTAMP_TINY,
                      VEC_TC_INTERVAL_YM,
                      VEC_TC_INTERVAL_DS,
                      VEC_TC_DEC_INT32,
                      VEC_TC_DEC_INT64,
                      VEC_TC_DEC_INT128,
                      VEC_TC_DEC_INT256,
                      VEC_TC_DEC_INT512);
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
      if (OB_SUCC(ret)) { outputed_cnt = out_batch; }
    }
  } else {
    MEMSET(wf_col.res_rows_, 0, sizeof(ObCompactRow *) * out_batch);
    if (OB_FAIL(wf_col.res_->processed_->get_batch_rows(
          wf_col.res_->processed_->output_row_idx_,
          wf_col.res_->processed_->output_row_idx_ + out_processed_cnt, wf_col.res_rows_))) {
      LOG_WARN("get batch rows failed", K(ret));
    } else if (OB_FAIL(
                 wf_col.res_->cur_->get_batch_rows(wf_col.res_->cur_->output_row_idx_,
                                                   wf_col.res_->cur_->output_row_idx_ + out_cur_cnt,
                                                   &wf_col.res_rows_[out_processed_cnt]))) {
      LOG_WARN("get batch rows failed", K(ret));
    } else if (OB_FAIL(wf_col.wf_info_.expr_->get_vector(eval_ctx_)->from_rows(
                 wf_col.wf_res_row_meta_, wf_col.res_rows_, out_batch, 0))) {
      LOG_WARN("from rows failed", K(ret));
    } else {
      wf_col.res_->processed_->output_row_idx_ += out_processed_cnt;
      wf_col.res_->cur_->output_row_idx_ += out_cur_cnt;
      outputed_cnt = out_processed_cnt + out_cur_cnt;
    }
  }
  return ret;
}
#undef SET_SP_RES
#undef SET_SP_FIXED_RES_CASE

int ObWindowFunctionVecOp::compute_wf_values(WinFuncColExpr *end, int64_t &check_times)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::BatchInfoScopeGuard guard(eval_ctx_);
  char *tmp_buf = nullptr;
  // Partition size maybe exceed MY_SPEC.batch_size_, if so, calculation is divided into several
  // batches of size MY_SPEC.max_batch_size.
  ObBitVector *nullres_skip  = batch_ctx_.nullres_skip_,
              *pushdown_skip = batch_ctx_.pushdown_skip_,
              *wf_skip       = batch_ctx_.calc_wf_skip_;
  ObDataBuffer backup_alloc((char *)batch_ctx_.all_exprs_backup_buf_, batch_ctx_.all_exprs_backup_buf_len_);
  ObVectorsResultHolder tmp_holder(&backup_alloc);
  int64_t saved_batch_size = 0;
  FOREACH_WINCOL(end) {
    saved_batch_size = std::max(input_stores_.cur_->count() - it->part_first_row_idx_, saved_batch_size);
  }
  saved_batch_size = std::min(saved_batch_size, MY_SPEC.max_batch_size_);
  if (OB_FAIL(tmp_holder.init(get_all_expr(), eval_ctx_))) {
    LOG_WARN("init tmp result holder failed", K(ret));
  } else if (OB_FAIL(tmp_holder.save(saved_batch_size))) {
    LOG_WARN("save vector resule failed", K(ret));
  }
  FOREACH_WINCOL(end) {
    if (it == wf_list_.get_last()) {
      const int v = input_stores_.cur_->count() - it->part_first_row_idx_;
      if (v > 0) {
        last_computed_part_rows_ = v;
      }
    }
    int64_t total_size = input_stores_.cur_->count() - it->part_first_row_idx_;
    winfunc::WinExprEvalCtx win_expr_ctx(*input_stores_.cur_, *it,
                                         ctx_.get_my_session()->get_effective_tenant_id());
    int64_t start_idx = it->part_first_row_idx_;
    if (it->wf_expr_->is_aggregate_expr()) {
      // if aggregate expr, reset last_valid_frame & last_valid_row before process partition
      winfunc::AggrExpr *agg_expr = static_cast<winfunc::AggrExpr *>(it->wf_expr_);
      agg_expr->last_valid_frame_.reset();
      agg_expr->last_aggr_row_ = nullptr;
    }
    while (OB_SUCC(ret) && total_size > 0) {
      clear_evaluated_flag();
      if (0 == ++check_times % CHECK_STATUS_INTERVAL) { // check per-batch
        if (OB_FAIL(ctx_.check_status())) { break; }
      }
      int64_t batch_size = std::min(total_size, MY_SPEC.max_batch_size_);
      if OB_SUCC(ret) {
        guard.set_batch_size(batch_size);
        guard.set_batch_idx(0);
        wf_skip->unset_all(0, batch_size);
        // add store guard to make sure read blocks are valid
        winfunc::StoreGuard store_guard(*this);
        if (OB_FAIL(it->wf_info_.expr_->init_vector_for_write(
              eval_ctx_, it->wf_info_.expr_->get_default_res_format(), batch_size))) {
          LOG_WARN("init vector for write failed", K(ret));
        } else if (OB_FAIL(input_stores_.cur_->attach_rows(get_all_expr(), input_row_meta_,
                                                           eval_ctx_, start_idx,
                                                           start_idx + batch_size, false))) {
          // step.1: attach rows
          LOG_WARN("attach rows failed", K(ret), K(start_idx), K(batch_size),
                   K(*input_stores_.cur_));
        } else if (OB_FAIL(it->reset_for_partition(batch_size, *wf_skip))) {
          LOG_WARN("reset for partition failed", K(ret));
        } else if (it->wf_info_.can_push_down_ && MY_SPEC.is_push_down()) {
          if (OB_FAIL(
                detect_nullres_or_pushdown_rows(*it, *nullres_skip, *pushdown_skip, *wf_skip))) {
            // step.2 find nullres rows and bypass-pushdown rows
            LOG_WARN("find null result rows or bypass-pushdown rows failed", K(ret));
          } else if (OB_FAIL(calc_bypass_pushdown_rows_of_wf(*it, batch_size, *pushdown_skip))) {
            // by pass collect will reset null bitmap by calling `init_vector_for_write`
            // must be called before `set_null_results_of_wf`
            // step.4 calculate bypass-pushdown rows
            LOG_WARN("calculate pushdown rows failed", K(ret));
          } else if (OB_FAIL(set_null_results_of_wf(*it, batch_size, *nullres_skip))) {
            // step.3 set null results
            LOG_WARN("set null results failed", K(ret));
          } else {
          }
        }
      }
      if (OB_SUCC(ret) && it->wf_expr_->is_aggregate_expr()) {
        // enable removal optimization
        it->agg_ctx_->removal_info_.enable_removal_opt_ =
          !(MY_SPEC.single_part_parallel_) && it->wf_info_.remove_type_ != common::REMOVE_INVALID;
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(it->wf_expr_->process_partition(win_expr_ctx, it->part_first_row_idx_,
                                                         input_stores_.cur_->count(), start_idx,
                                                         start_idx + batch_size, *wf_skip))) {
        //   step.5 calculate window function results for rest rows
        LOG_WARN("process partition failed", K(ret));
      } else {
        ObSEArray<ObExpr *, 1> tmp_exprs;
        // reset skip to add all rows
        wf_skip->unset_all(0, batch_size);
        if (OB_FAIL(tmp_exprs.push_back(it->wf_info_.expr_))) {
          LOG_WARN("push back element failed", K(ret));
        } else if (OB_FAIL(it->res_->cur_->add_batch_rows(
                     tmp_exprs, it->wf_res_row_meta_, eval_ctx_,
                     EvalBound(batch_size, 0, batch_size, true), *wf_skip, true))) {
          LOG_WARN("add batch rows failed", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else {
        start_idx += batch_size;
        total_size -= batch_size;
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(tmp_holder.restore())) {
    LOG_WARN("restore results failed", K(ret));
  } else if (OB_FAIL(update_part_first_row_idx(end))) {
    LOG_WARN("update part first row idx failed", K(ret));
  }
  // Row project flag is set when read row from RAStore, but the remain rows in batch
  // are not evaluated, need reset the flag here.
  clear_evaluated_flag();
  return ret;
}

int ObWindowFunctionVecOp::set_null_results_of_wf(WinFuncColExpr &wf, const int64_t batch_size,
                                                  const ObBitVector &nullres_skip)
{
  int ret = OB_SUCCESS;
  ObExpr *wf_expr = wf.wf_info_.expr_;
  VectorFormat fmt = wf_expr->get_format(eval_ctx_);
  switch (fmt) {
  case common::VEC_FIXED:
  case common::VEC_DISCRETE:
  case common::VEC_CONTINUOUS: {
    ObBitmapNullVectorBase *data = static_cast<ObBitmapNullVectorBase *>(wf_expr->get_vector(eval_ctx_));
    data->get_nulls()->bit_not(nullres_skip, batch_size);
    break;
  }
  case common::VEC_UNIFORM: {
    ObUniformFormat<false> *data = static_cast<ObUniformFormat<false> *>(wf_expr->get_vector(eval_ctx_));
    for (int i = 0; i < batch_size; i++) {
      if (nullres_skip.at(i)) {
      } else {
        data->set_null(i);
      }
    }
    break;
  }
  case common::VEC_UNIFORM_CONST: {
    ObUniformFormat<true> *data = static_cast<ObUniformFormat<true> *>(wf_expr->get_vector(eval_ctx_));
    for (int i = 0; i < batch_size; i++) {
      if (nullres_skip.at(i)) {
      } else {
        data->set_null(i);
        break;
      }
    }
    break;
  }
  default: {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected format", K(ret), K(fmt));
  }
  }
  return ret;
}

int ObWindowFunctionVecOp::calc_bypass_pushdown_rows_of_wf(WinFuncColExpr &wf,
                                                           const int64_t batch_size,
                                                           const ObBitVector &pushdown_skip)
{
  int ret = OB_SUCCESS;
  winfunc::AggrExpr *agg_expr = static_cast<winfunc::AggrExpr *>(wf.wf_expr_);
  if (pushdown_skip.accumulate_bit_cnt(batch_size) == batch_size) {
    // do nothing
  } else if (OB_FAIL(agg_expr->aggr_processor_->init_fast_single_row_aggs())) {
    LOG_WARN("init fast single row aggregate failed", K(ret));
  } else if (OB_FAIL(agg_expr->aggr_processor_->single_row_agg_batch(wf.aggr_rows_, batch_size,
                                                                     eval_ctx_, pushdown_skip))) {
    LOG_WARN("bypass calculation failed", K(ret));
  }
  return ret;
}

int ObWindowFunctionVecOp::collect_sp_partial_results()
{
  int ret = OB_SUCCESS;
  WinFuncColExpr &wf_col = *wf_list_.get_last();
  common::ObMemAttr attr(ctx_.get_my_session()->get_effective_tenant_id(),
                         ObModIds::OB_SQL_WINDOW_LOCAL, ObCtxIds::WORK_AREA);
  SPWinFuncPXWholeMsg whole_msg(attr);
  ObVectorsResultHolder tmp_holder;
  if (wf_col.res_->cur_->count() <= 0) {
    // current rows is empty, no need to compute wf values, hency just send empty msg and return.
    if (OB_FAIL(sp_get_whole_msg(true, whole_msg, nullptr))) {
      LOG_WARN("sp_get_whole_msg failed", K(ret));
    }
  } else {
    // In order to construct a compact row with wf_expr list, following steps are needed:
    // 1. backup first row of exprs
    // 2. attach first row wf_res_result to wf_expr seperately
    // 3. calculate row size of needed compact row and allocate one row
    // 4. call `ObIVector::to_rows` to get compact row
    ObEvalCtx::BatchInfoScopeGuard guard(eval_ctx_);
    guard.set_batch_idx(0);
    guard.set_batch_size(1);
    if (OB_FAIL(tmp_holder.init(get_all_expr(), eval_ctx_))) {
      LOG_WARN("init results holder failed", K(ret));
    } else if (OB_FAIL(tmp_holder.save(1))) {
      LOG_WARN("save vector data failed", K(ret));
    } else {
      ObCompactRow *wf_res_row = nullptr;
      ObSEArray<ObExpr *, 8> all_wf_exprs;
      FOREACH_WINCOL(END_WF) { // only one row, no need to set row store mem guard
        if (OB_FAIL(it->res_->cur_->attach_rows(it->wf_info_.expr_, it->wf_res_row_meta_, eval_ctx_,
                                                0, 1, false))) {
          LOG_WARN("attach rows failed", K(ret));
        } else if (OB_FAIL(all_wf_exprs.push_back(it->wf_info_.expr_))) {
          LOG_WARN("push back element failed", K(ret));
        }
      }
      int64_t mocked_skip = 0;
      ObBatchRows brs;
      brs.size_ = 1;
      brs.end_ = false;
      brs.all_rows_active_ = true;
      brs.skip_ = to_bit_vector(&mocked_skip);
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(batch_ctx_.tmp_wf_res_row_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null tmp row", K(ret));
      } else if (OB_FAIL(batch_ctx_.tmp_wf_res_row_->save_store_row(all_wf_exprs, brs, eval_ctx_, 0, false))) {
        LOG_WARN("save wf res row failed", K(ret));
      } else if (OB_FAIL(sp_get_whole_msg(false, whole_msg, batch_ctx_.tmp_wf_res_row_->compact_row_))) { // all done ready to send piece msg and wait for whole
        LOG_WARN("sp_get_whole msg failed", K(ret));
      } else if (OB_FAIL(sp_merge_partial_results(whole_msg))) {
        LOG_WARN("merge partial results failed", K(ret));
      } else if (OB_FAIL(tmp_holder.restore())) {
        LOG_WARN("restore failed", K(ret));
      } else { // do nothing
      }
    }
  }
  return ret;
}

int ObWindowFunctionVecOp::sp_get_whole_msg(bool is_empty, SPWinFuncPXWholeMsg &msg,
                                            ObCompactRow *sending_row)
{
  int ret = OB_SUCCESS;
  ObPxSqcHandler *handler = ctx_.get_sqc_handler();
  const SPWinFuncPXWholeMsg *temp_whole_msg = nullptr;
  if (OB_ISNULL(handler)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("parallel winbuf only supported in parallel execution mode", K(MY_SPEC.single_part_parallel_));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "parallel winbuf in non-px mode");
  } else {
    ObPxSQCProxy &proxy = handler->get_sqc_proxy();
    common::ObMemAttr attr(ctx_.get_my_session()->get_effective_tenant_id(),
                           ObModIds::OB_SQL_WINDOW_LOCAL, ObCtxIds::WORK_AREA);
    SPWinFuncPXPieceMsg piece(attr);
    piece.op_id_ = MY_SPEC.get_id();
    piece.thread_id_ = GETTID();
    piece.source_dfo_id_ = proxy.get_dfo_id();
    piece.target_dfo_id_ = proxy.get_dfo_id();
    piece.is_empty_ = is_empty;
    if (!is_empty) {
      if (OB_ISNULL(sending_row)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null sending row", K(ret));
      } else if (OB_FAIL(
                   piece.row_meta_.deep_copy(*all_wf_res_row_meta_, &piece.deserial_allocator_))) {
        LOG_WARN("deep copy row meta failed", K(ret));
      } else {
        piece.row_ = sending_row;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(proxy.get_dh_msg_sync(
                 MY_SPEC.get_id(), dtl::DH_SP_WINFUNC_PX_WHOLE_MSG, piece, temp_whole_msg,
                 ctx_.get_physical_plan_ctx()->get_timeout_timestamp()))) {
      LOG_WARN("get whole msg failed", K(ret));
    } else if (OB_FAIL(msg.assign(*temp_whole_msg))) {
      LOG_WARN("assign whole msg failed", K(ret));
    }
  }
  return ret;
}

int ObWindowFunctionVecOp::sp_merge_partial_results(SPWinFuncPXWholeMsg &msg)
{
  int ret = OB_SUCCESS;
  const ObCompactRow *a_row = nullptr;
  const char *res = nullptr, *cur = nullptr;
  int32_t res_len = 0, cur_len = 0;
  bool res_isnull = true, cur_isnull = false;
  ObTempRowStore::Iterator store_iter;
  ObSEArray<ObExpr *, 8> all_wf_exprs;
  int cmp_ret = 0;
  if (OB_UNLIKELY(msg.is_empty_)) {
  } else if (OB_FAIL(store_iter.init(&msg.row_store_))) {
    LOG_WARN("init row store iteration failed", K(ret));
  } else {
    int64_t batch_size = std::min(msg.row_store_.get_row_cnt(), MY_SPEC.max_batch_size_);
    int64_t read_rows = 0;
    MEMSET(batch_ctx_.stored_rows_, 0, sizeof(const ObCompactRow *) * batch_size);
    ObEvalCtx::BatchInfoScopeGuard guard(eval_ctx_);
    guard.set_batch_idx(0);
    guard.set_batch_size(1);
    FOREACH_WINCOL(END_WF) {
      if (OB_FAIL(all_wf_exprs.push_back(it->wf_info_.expr_))) {
        LOG_WARN("push back element failed", K(ret));
      } else if (OB_FAIL(it->wf_info_.expr_->init_vector_default(eval_ctx_, 1))) {
        LOG_WARN("init vector for write failed", K(ret));
      } else {
        it->wf_info_.expr_->get_vector(eval_ctx_)->set_null(0); // initialize to null
      }
    }
    __PartialResult<T_INVALID> part_res(eval_ctx_, msg.assign_allocator_);
    while (OB_SUCC(ret)) {
      if (OB_FAIL(store_iter.get_next_batch(batch_size, read_rows, batch_ctx_.stored_rows_))) {
        if (ret == OB_ITER_END) {
        } else {
          LOG_WARN("get batch from store failed", K(ret));
        }
      } else if (OB_UNLIKELY(read_rows == 0)) {
        ret = OB_ITER_END;
      }
      FOREACH_WINCOL(END_WF)
      {
        int col_idx = it->wf_idx_ - 1;
        for (int i = 0; OB_SUCC(ret) && i < read_rows; i++) {
          a_row = batch_ctx_.stored_rows_[i];
          cur_isnull = a_row->is_null(col_idx);
          a_row->get_cell_payload(msg.row_meta_, col_idx, cur, cur_len);
          if (OB_FAIL(part_res.merge<ObIVector>(it->wf_info_, cur_isnull, cur, cur_len))) {
            LOG_WARN("merge result failed", K(ret));
          }
        }
      } // end wf for loop
    }
    if (OB_FAIL(ret)) {
      if (ret == OB_ITER_END) { ret = OB_SUCCESS; }
    }
    if (OB_SUCC(ret)) {
      if (sp_merged_row_ == nullptr) {
        sp_merged_row_ = OB_NEWx(LastCompactRow, local_allocator_, *local_allocator_);
        if (OB_ISNULL(sp_merged_row_)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate memory failed", K(ret));
        }
      }
      int64_t mocked_skip = 0;
      ObBatchRows brs;
      brs.size_ = 1;
      brs.end_ = false;
      brs.all_rows_active_ = true;
      brs.skip_ = to_bit_vector(&mocked_skip);
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(sp_merged_row_->save_store_row(all_wf_exprs, brs, eval_ctx_, 0, false))) {
        LOG_WARN("save wf res row failed", K(ret));
      } else {
      }
    }
  }
  return ret;
}
#undef MERGE_AGG_RES

int ObWindowFunctionVecOp::add_aggr_res_row_for_participator(WinFuncColExpr *end,
                                                             winfunc::RowStore &row_store)
{
  int ret = OB_SUCCESS;
  if (MY_SPEC.is_participator()) {
    // found a new part, add extra output row with aggr_status < 0 for partial agg results if possible
    // timeline of wf computing (suppose dop = 2)
    // pby_row        <a, b, c> (wf_idx = 1)                            <a, b> (wf_idx = 2)
    //       firs_part(last_aggr_stats = 0, no bypass)
    //       second_part(last_aggr_status = 0, no bypass)
    //       third_part(last_aggr_status = 1, bypass)
    //                                                          first_part(last_aggr_status=1, no bypass)
    //                                                          second_part(last_aggr_status=1, no bypass)
    //                                                          third_part(last_aggr_status = 2, bypass)
    if (last_aggr_status_ < wf_list_.get_last()->wf_idx_) {
      int64_t no_skip_data = 0;
      ObBitVector *mock_skip = to_bit_vector(&no_skip_data);
      EvalBound tmp_bound(1, true);
      ObDataBuffer vec_res_alloc((char *)batch_ctx_.all_exprs_backup_buf_,
                                 batch_ctx_.all_exprs_backup_buf_len_);
      ObVectorsResultHolder tmp_holder(&vec_res_alloc);
      if (OB_FAIL(tmp_holder.init(get_all_expr(), eval_ctx_))) {
        LOG_WARN("init result holder failed", K(ret));
      } else if (OB_FAIL(tmp_holder.save(1))) {
        LOG_WARN("save result failed", K(ret));
      }
      FOREACH_WINCOL(end)
      {
        if (it->part_first_row_idx_ >= row_store.count()) {
          // all partial results are calculated, no need calculating
          break;
        }
        // `add_batch_rows` may invalidate memory of current block
        // `attach_rows` before adding row is necessary to avoid invalid memory.
        if (OB_FAIL(row_store.attach_rows(get_all_expr(), input_row_meta_, eval_ctx_,
                                          row_store.stored_row_cnt_ - 1, row_store.stored_row_cnt_,
                                          false))) {
          LOG_WARN("attach rows failed", K(ret));
        } else {
          ObExpr *agg_status_code = get_all_expr().at(get_all_expr().count() - 1);
          if (last_aggr_status_ < it->wf_idx_) {
            int64_t agg_status = -(it->wf_idx_);
            agg_status_code->get_vector(eval_ctx_)->set_int(0, agg_status);
            if (OB_FAIL(row_store.add_batch_rows(get_all_expr(), input_row_meta_, eval_ctx_,
                                                 tmp_bound, *mock_skip, true, nullptr, true))) {
              LOG_WARN("add batch rows failed", K(ret));
            }
          }
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(tmp_holder.restore())) {
        LOG_WARN("restore result failed", K(ret));
      }
    }
  }
  return ret;
}

int ObWindowFunctionVecOp::detect_and_report_aggr_status(const ObBatchRows &child_brs,
                                                         const int64_t start_idx,
                                                         const int64_t end_idx)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::BatchInfoScopeGuard guard(eval_ctx_);
  if (MY_SPEC.is_participator()) {
    // find first nonskip idx;
    int64_t batch_idx = next_nonskip_row_index(start_idx - 1, child_brs);
    if (batch_idx >= end_idx) {
      // maybe got a new batch within different partition.
      // aggr status already detected in previous batch, do nothing
      LOG_DEBUG("all rows are skipped", K(ret), K(start_idx), K(end_idx));
    } else {
      guard.set_batch_idx(batch_idx);
      last_aggr_status_ = 0;
      int64_t total_task_cnt = 0;
      int64_t prev_pushdown_wf_pby_expr_count = -1; // prev_wf_pby_expr_count transmit to datahub
      FOREACH_WINCOL(END_WF)
      {
        bool is_pushdown_bypass = !it->wf_info_.can_push_down_;
        if (OB_SUCC(ret) && it->wf_info_.can_push_down_) {
          int64_t pushdown_wf_idx = pby_expr_cnt_idx_array_.at(it->wf_idx_ - 1);
          if (it->wf_info_.partition_exprs_.count() > next_wf_pby_expr_cnt_to_transmit_) {
            // already sent participator's hash set to datahub and got whole msg
            ReportingWFHashSet *pushdown_pby_hash_values_set =
              pby_hash_values_sets_.at(pushdown_wf_idx);
            uint64_t hash_value = 0;
            if (OB_ISNULL(pushdown_pby_hash_values_set)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected null hash value set", K(ret));
            } else if (OB_FAIL(rwf_calc_pby_row_hash(child_brs, it->wf_info_.partition_exprs_,
                                                     hash_value))) {
              LOG_WARN("calculate hash value failed", K(ret));
            } else if (FALSE_IT(ret = pushdown_pby_hash_values_set->exist_refactored(hash_value))) {
            } else if (ret == OB_HASH_NOT_EXIST) {
              is_pushdown_bypass = true;
              ret = OB_SUCCESS;
            } else if (ret == OB_HASH_EXIST) {
              is_pushdown_bypass = false;
              ret = OB_SUCCESS;
            } else {
              LOG_WARN("exist refactored failed", K(ret));
            }
          }
          total_task_cnt =
            static_cast<ObWindowFunctionVecOpInput *>(input_)->get_total_task_count();
          if (OB_SUCC(ret)
              && it->wf_info_.partition_exprs_.count() != prev_pushdown_wf_pby_expr_count
              && it->wf_info_.partition_exprs_.count() <= next_wf_pby_expr_cnt_to_transmit_) {
            if (pby_hash_values_.at(pushdown_wf_idx)->count() < total_task_cnt) {
              // has not send data to datahub yet.
              uint64_t hash_value = 0;
              if (OB_FAIL(
                    rwf_calc_pby_row_hash(child_brs, it->wf_info_.partition_exprs_, hash_value))) {
                LOG_WARN("calc hash value failed", K(ret));
              }
              bool exists = false;
              for (int i = 0;
                   OB_SUCC(ret) && !exists && i < pby_hash_values_.at(pushdown_wf_idx)->count();
                   i++) {
                exists = (hash_value == pby_hash_values_.at(pushdown_wf_idx)->at(i));
              }
              if (OB_FAIL(ret)) {
              } else if (!exists
                         && OB_FAIL(pby_hash_values_.at(pushdown_wf_idx)->push_back(hash_value))) {
                LOG_WARN("pushback element failed", K(ret));
              }
            }
            if (OB_SUCC(ret)
                && (pby_hash_values_.at(pushdown_wf_idx)->count() == total_task_cnt
                    || child_iter_end_)) {
              if (OB_FAIL(rwf_participator_coordinate(pushdown_wf_idx))) {
                LOG_WARN("participator coordinate failed", K(ret));
              } else {
                next_wf_pby_expr_cnt_to_transmit_ = it->wf_info_.partition_exprs_.count() - 1;
              }
            }
          }
          if (OB_SUCC(ret)) {
            prev_pushdown_wf_pby_expr_count = it->wf_info_.partition_exprs_.count();
          }
        }
        if (OB_SUCC(ret) && is_pushdown_bypass) { last_aggr_status_ = it->wf_idx_; }
        LOG_TRACE("detect and report aggr status", K(next_wf_pby_expr_cnt_to_transmit_),
                  K(last_aggr_status_), K(total_task_cnt), K(child_iter_end_), K(it->wf_idx_));
      }
      // update result value in expr
      if (OB_SUCC(ret)) {
        if (OB_FAIL(rwf_update_aggr_status_code(start_idx, end_idx))) {
          LOG_WARN("update aggr_status code failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObWindowFunctionVecOp::rwf_update_aggr_status_code(const int64_t start_idx, const int64_t end_idx)
{
  int ret = OB_SUCCESS;
  if (MY_SPEC.is_participator()) {
    if (OB_UNLIKELY(start_idx >= end_idx || end_idx > eval_ctx_.get_batch_size())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected row idx", K(start_idx), K(end_idx), K(eval_ctx_.get_batch_size()));
    } else {
      ObFixedLengthFormat<int64_t> *data = static_cast<ObFixedLengthFormat<int64_t> *>(
        MY_SPEC.wf_aggr_status_expr_->get_vector(eval_ctx_));
      VectorFormat fmt = MY_SPEC.wf_aggr_status_expr_->get_format(eval_ctx_);
      VecValueTypeClass vec_tc = MY_SPEC.wf_aggr_status_expr_->get_vec_value_tc();
      if (OB_UNLIKELY(!(fmt == VEC_FIXED && vec_tc == VEC_TC_INTEGER))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected data format for agg status expr", K(ret), K(fmt), K(vec_tc));
      }
      for (int i = start_idx; OB_SUCC(ret) && i < end_idx; i++) {
        data->set_payload(i, &last_aggr_status_, sizeof(int64_t));
      }
      if (OB_SUCC(ret)) {
        MY_SPEC.wf_aggr_status_expr_->set_evaluated_projected(eval_ctx_);
      }
    }
  }
  return ret;
}

int ObWindowFunctionVecOp::rwf_calc_pby_row_hash(const ObBatchRows &child_brs,
                                                 const ObIArray<ObExpr *> &pby_exprs,
                                                 uint64_t &hash_value)
{
  int ret = OB_SUCCESS;
  int64_t batch_idx = eval_ctx_.get_batch_idx();
  hash_value = 99194853094755497L; // `copy from ObWindowFunctionOp::calc_part_exprs_hash`
  for (int i = 0; OB_SUCC(ret) && i < pby_exprs.count(); i++) {
    if (OB_FAIL(pby_exprs.at(i)->eval_vector(
          eval_ctx_, *child_brs.skip_,
          EvalBound(child_brs.size_, batch_idx, batch_idx + 1, true)))) {
      LOG_WARN("eval vector failed", K(ret));
    } else {
      ObIVector *data = pby_exprs.at(i)->get_vector(eval_ctx_);
      if (OB_FAIL(data->murmur_hash_v3_for_one_row(*pby_exprs.at(i), hash_value, batch_idx,
                                                   eval_ctx_.get_batch_size(), hash_value))) {
        LOG_WARN("murmur hash failed", K(ret));
      }
    }
  }
  return ret;
}

int ObWindowFunctionVecOp::rwf_participator_coordinate(const int64_t pushdown_wf_idx)
{
  int ret = OB_SUCCESS;
  ObReportingWFWholeMsg *whole_msg = participator_whole_msg_array_.at(pushdown_wf_idx);
  const PbyHashValueArray *pby_hash_value_array = pby_hash_values_.at(pushdown_wf_idx);
  ReportingWFHashSet *pushdown_pby_hash_set = pby_hash_values_sets_.at(pushdown_wf_idx);
  if (OB_ISNULL(whole_msg) || OB_ISNULL(pby_hash_value_array) || OB_ISNULL(pushdown_pby_hash_set)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null arguments", K(ret), K(whole_msg), K(pby_hash_value_array),
             K(pushdown_pby_hash_set));
  } else if (OB_FAIL(rwf_get_whole_msg(pby_hash_value_array, *whole_msg))) {
    LOG_WARN("get whole msg failed", K(ret));
  } else if (0 == pby_hash_value_array->count()) {
    // empty input, do nothing
  } else {
    for (int i = 0; OB_SUCC(ret) && i < whole_msg->pby_hash_value_array_.count(); i++) {
      if (OB_FAIL(pushdown_pby_hash_set->set_refactored_1(whole_msg->pby_hash_value_array_.at(i), true))) {
        LOG_WARN("insert hash set failed", K(ret));
      }
    }
  }
  return ret;
}

int ObWindowFunctionVecOp::rwf_get_whole_msg(const PbyHashValueArray *hash_value_arr,
                                             ObReportingWFWholeMsg &whole_msg)
{
  int ret = OB_SUCCESS;
  const ObReportingWFWholeMsg *temp_whole_msg = nullptr;
  ObPxSqcHandler *handler = ctx_.get_sqc_handler();
  if (OB_ISNULL(handler)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("reporting window function only supported in parallel execution mode",
             K(MY_SPEC.is_participator()));
  } else {
    ObPxSQCProxy &proxy = handler->get_sqc_proxy();
    ObReportingWFPieceMsg piece;
    piece.op_id_ = MY_SPEC.get_id();
    piece.thread_id_ = GETTID();
    piece.source_dfo_id_ = proxy.get_dfo_id();
    piece.target_dfo_id_ = proxy.get_dfo_id();
    piece.pby_hash_value_array_ = *hash_value_arr;
    if (OB_FAIL(proxy.get_dh_msg_sync(MY_SPEC.get_id(), dtl::DH_SECOND_STAGE_REPORTING_WF_WHOLE_MSG,
                                      piece, temp_whole_msg,
                                      ctx_.get_physical_plan_ctx()->get_timeout_timestamp()))) {
      LOG_WARN("get reporting window function whole msg failed", K(ret));
    } else if (OB_ISNULL(temp_whole_msg)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null whole msg", K(ret));
    } else if (OB_FAIL(whole_msg.assign(*temp_whole_msg))) {
      LOG_WARN("assign whole msg failed", K(ret));
    } else {
      ObWindowFunctionVecOpInput *op_input = static_cast<ObWindowFunctionVecOpInput *>(input_);
      ObPxDatahubDataProvider *provider = nullptr;
      ObReportingWFWholeMsg::WholeMsgProvider *msg_provider = nullptr;
      if (OB_FAIL(proxy.sqc_ctx_.get_whole_msg_provider(
            MY_SPEC.get_id(), dtl::DH_SECOND_STAGE_REPORTING_WF_WHOLE_MSG, provider))) {
        LOG_WARN("get whole msg provider failed", K(ret));
      } else {
        // `sync_wait` will wait for all workers to receive whole msg and then reset datahub's resource.
        // for reporting window function parallel excution, multiple pushdown wf exprs maybe exists in operator
        // pushdown exprs will send pieces and get whole msg in order
        // if we do not do synchronous waiting, following pushdown wf expr may get previous wf_expr's whole msg
        // and cause unexpected error.
        msg_provider = static_cast<ObReportingWFWholeMsg::WholeMsgProvider *>(provider);
        if (OB_FAIL(op_input->sync_wait(ctx_, msg_provider))) {
          LOG_WARN("sync wait failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObWindowFunctionVecOp::detect_nullres_or_pushdown_rows(WinFuncColExpr &wf,
                                                           ObBitVector &nullres_skip,
                                                           ObBitVector &pushdown_skip,
                                                           ObBitVector &wf_skip)
{
  int ret = OB_SUCCESS;
  int64_t word_cnt = ObBitVector::word_count(eval_ctx_.get_batch_size());
  nullres_skip.set_all(eval_ctx_.get_batch_size());
  pushdown_skip.set_all(eval_ctx_.get_batch_size());
  MEMSET(wf_skip.data_, 0, word_cnt);
  if (MY_SPEC.is_participator()) {
    if (OB_UNLIKELY(MY_SPEC.wf_aggr_status_expr_->get_format(eval_ctx_) != VEC_FIXED)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected data format", K(ret),
               K(MY_SPEC.wf_aggr_status_expr_->get_format(eval_ctx_)));
    } else {
      ObFixedLengthFormat<int64_t> *agg_status_data = static_cast<ObFixedLengthFormat<int64_t> *>(
        MY_SPEC.wf_aggr_status_expr_->get_vector(eval_ctx_));
      for (int i = 0; i < eval_ctx_.get_batch_size(); i++) {
        int64_t aggr_status = *reinterpret_cast<const int64_t *>(agg_status_data->get_payload(i));
        if (aggr_status >= 0) {
          if (aggr_status < wf.wf_idx_) { // original rows participating in partial aggregation
            // set aggr results to null
            nullres_skip.unset(i);
          } else { // original rows using bypass-pushdown output
            pushdown_skip.unset(i);
          }
          wf_skip.set(i);
        } else { // extra added row with partial aggregation results
          if (aggr_status != -wf.wf_idx_) {
            // if aggr_status == -wf.wf_idx_, this row needs calculation to get partial aggregation result.
            nullres_skip.unset(i);
            wf_skip.set(i);
          }
        }
      }
    }
  } else if (MY_SPEC.is_consolidator()) {
    VectorFormat status_fmt = MY_SPEC.wf_aggr_status_expr_->get_format(eval_ctx_);
    if (status_fmt == VEC_FIXED) {
      ObFixedLengthFormat<int64_t> *agg_status_data = static_cast<ObFixedLengthFormat<int64_t> *>(
        MY_SPEC.wf_aggr_status_expr_->get_vector(eval_ctx_));
      for (int i = 0; i < eval_ctx_.get_batch_size(); i++) {
        int64_t aggr_status = *reinterpret_cast<const int64_t *>(agg_status_data->get_payload(i));
        if (aggr_status < 0) {
          // row with partial result, do not calculation wf value
          wf_skip.set(i);
          nullres_skip.unset(i);
        }
      }
    } else if (status_fmt == VEC_UNIFORM) {
      ObUniformFormat<false> *agg_status_data = static_cast<ObUniformFormat<false> *>(
        MY_SPEC.wf_aggr_status_expr_->get_vector(eval_ctx_));
      for (int i = 0; i < eval_ctx_.get_batch_size(); i++) {
        int64_t aggr_status = *reinterpret_cast<const int64_t *>(agg_status_data->get_payload(i));
        if (aggr_status < 0) {
          // row with partial result, do not calculation wf value
          wf_skip.set(i);
          nullres_skip.unset(i);
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected data format", K(ret), K(status_fmt));
    }
  }
  return ret;
}

int ObWindowFunctionVecOp::rwf_send_empty_piece_data()
{
  int ret = OB_SUCCESS;
  FOREACH_WINCOL(END_WF) {
    if (it->wf_info_.can_push_down_ && it->wf_info_.partition_exprs_.count() <= next_wf_pby_expr_cnt_to_transmit_) {
      int64_t pushdown_wf_idx = pby_expr_cnt_idx_array_.at(it->wf_idx_ - 1);
      if (OB_FAIL(rwf_participator_coordinate(pushdown_wf_idx))) {
        LOG_WARN("participator coordinating failed", K(ret));
      } else {
        next_wf_pby_expr_cnt_to_transmit_ = it->wf_info_.partition_exprs_.count() - 1;
      }
    }
  }
  return ret;
}

struct __tid_cmp
{
  OB_INLINE bool operator()(RDWinFuncPXPartialInfo *it, int64_t tid)
  {
    return it->thread_id_ < tid;
  }
};

int ObWindowFunctionVecOp::rd_fetch_patch()
{
  int ret = OB_SUCCESS;
  ObPxSqcHandler *handler = ctx_.get_sqc_handler();
  if (OB_ISNULL(handler) || OB_ISNULL(rd_coord_row_meta_) || OB_ISNULL(ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null arguments", K(ret), K(handler), K(rd_coord_row_meta_),
             K(ctx_.get_my_session()));
  } else {
    common::ObMemAttr attr(ctx_.get_my_session()->get_effective_tenant_id(),
                           ObModIds::OB_SQL_WINDOW_FUNC, ObCtxIds::WORK_AREA);
    RDWinFuncPXPieceMsg piece_msg(attr);
    piece_msg.op_id_ = MY_SPEC.get_id();
    piece_msg.thread_id_ = GETTID();
    piece_msg.source_dfo_id_ = handler->get_sqc_proxy().get_dfo_id();
    piece_msg.target_dfo_id_ = handler->get_sqc_proxy().get_dfo_id();

    piece_msg.info_.sqc_id_ = handler->get_sqc_proxy().get_sqc_id();
    piece_msg.info_.thread_id_ = GETTID();
    if (OB_FAIL(piece_msg.info_.row_meta_.deep_copy(*rd_coord_row_meta_, &piece_msg.arena_alloc_))) {
      LOG_WARN("deep copy row meta failed", K(ret));
    }
    if (OB_SUCC(ret) && input_stores_.first_->count() > 0) {
      if (OB_FAIL(rd_build_partial_info_row(0, true, piece_msg.arena_alloc_,
                                            piece_msg.info_.first_row_))) {
        LOG_WARN("build first row failed", K(ret));
      } else if (input_stores_.last_->count() <= 0) {
        // get first partition's last row if only exists one partition
        if (OB_FAIL(rd_build_partial_info_row(input_stores_.first_->count() - 1,
                                              true, piece_msg.arena_alloc_,
                                              piece_msg.info_.last_row_))) {
          LOG_WARN("build last row failed", K(ret));
        }
      }
    }
    if (OB_SUCC(ret) && input_stores_.last_->count() > 0) {
      if (OB_FAIL(rd_build_partial_info_row(input_stores_.last_->count() - 1,
                                            false, piece_msg.arena_alloc_,
                                            piece_msg.info_.last_row_))) {
        LOG_WARN("build last row failed", K(ret));
      } else {
        *reinterpret_cast<int64_t *>(piece_msg.info_.last_row_->get_extra_payload(
          *rd_coord_row_meta_)) = last_computed_part_rows_ - 1;
      }
    }
    const RDWinFuncPXWholeMsg *whole_msg = nullptr;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(handler->get_sqc_proxy().get_dh_msg_sync(
                 MY_SPEC.get_id(), dtl::DH_RD_WINFUNC_PX_WHOLE_MSG, piece_msg, whole_msg,
                 ctx_.get_physical_plan_ctx()->get_timeout_timestamp()))) {
      LOG_WARN("get range distributed winfunc msg failed", K(ret));
    } else if (OB_ISNULL(whole_msg)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null msg", K(ret));
    } else {
      // find patch of this worker
      // `RDWinFuncWholeMsg::infos_` already sorted by thread_id in QC
      int64_t tid = GETTID();
      RDWinFuncPXWholeMsg *m = const_cast<RDWinFuncPXWholeMsg *>(whole_msg);
      decltype(m->infos_)::iterator info = std::lower_bound(m->infos_.begin(), m->infos_.end(), tid, __tid_cmp());
      if (info == m->infos_.end() || (*info)->thread_id_ != tid) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("worker's response msg not found in whole msg", K(ret), K(tid));
      } else if (OB_ISNULL(rd_patch_ = (*info)->dup(rescan_alloc_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("duplicate patch info failed", K(ret));
      }
    }
  }
  return ret;
}

int ObWindowFunctionVecOp::rd_build_partial_info_row(int64_t idx, bool is_first_part,
                                                     ObIAllocator &alloc, ObCompactRow *&build_row)
{
  int ret = OB_SUCCESS;
  // no need to save first row of all_exprs
  clear_evaluated_flag();
  ObEvalCtx::BatchInfoScopeGuard guard(eval_ctx_);
  guard.set_batch_size(1);
  guard.set_batch_idx(0);
  ObVectorsResultHolder tmp_holder;
  const ObCompactRow *input_row = nullptr, *wf_res_row = nullptr;
  winfunc::RowStore *input = (is_first_part ? input_stores_.first_ : input_stores_.last_);
  if (OB_FAIL(tmp_holder.init(get_all_expr(), eval_ctx_))) {
    LOG_WARN("init tmp result holder failed", K(ret));
  } else if (OB_FAIL(tmp_holder.save(1))) {
    LOG_WARN("save batch results failed", K(ret));
  } else if (OB_FAIL(input->attach_rows(get_all_expr(), input_row_meta_, eval_ctx_, idx, idx + 1,
                                        false))) { // first attach input row
    LOG_WARN("attach row failed", K(ret));
  }

  // second: attach wf res row
  FOREACH_WINCOL(END_WF) {
    ObExpr *expr = it->wf_info_.expr_;
    winfunc::RowStore *wf_res = (is_first_part ? it->res_->first_ : it->res_->last_);
    if (OB_FAIL(wf_res->attach_rows(it->wf_info_.expr_, it->wf_res_row_meta_, eval_ctx_, idx,
                                    idx + 1, false))) {
      LOG_WARN("attach row failed", K(ret));
    }
  }
  // third: build rd_coord_res_row_
  if (OB_SUCC(ret)) {
    LastCompactRow *rd_coord_row = OB_NEWx(LastCompactRow, &alloc, alloc);
    int64_t tmp_skip = 0;
    ObBatchRows tmp_brs;
    tmp_brs.size_ = eval_ctx_.get_batch_size();
    tmp_brs.skip_ = to_bit_vector(&tmp_skip);
    tmp_brs.end_ = false;
    tmp_brs.all_rows_active_ = true;
    if (OB_ISNULL(rd_coord_row)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else if (OB_FAIL(rd_coord_row->save_store_row(MY_SPEC.rd_coord_exprs_, tmp_brs, eval_ctx_,
                                                    sizeof(int64_t), false))) {
      LOG_WARN("save store row failed", K(ret));
    } else {
      build_row = rd_coord_row->compact_row_;
      *reinterpret_cast<int64_t *>(build_row->get_extra_payload(*rd_coord_row_meta_)) = idx;
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(tmp_holder.restore())) {
    LOG_WARN("restore batch results failed", K(ret));
  }
  return ret;
}

int ObWindowFunctionVecOp::rd_output_final_batch(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  if (input_stores_.cur_->to_output_rows() <= 0) {
    brs_.end_ = true;
    brs_.size_ = 0;
    iter_end_ = true;
  } else {
    if (patch_alloc_.used() >= OB_MALLOC_MIDDLE_BLOCK_SIZE) {
      patch_alloc_.reset_remain_one_page();
    }
    int64_t cnt = std::min(max_row_cnt, input_stores_.cur_->to_output_rows());
    ObEvalCtx::BatchInfoScopeGuard guard(eval_ctx_);
    guard.set_batch_size(cnt);
    int64_t store_start_idx = input_stores_.cur_->output_row_idx_;
    int64_t store_end_idx = input_stores_.cur_->output_row_idx_ + cnt;
    LOG_DEBUG("rd output final batch", K(store_start_idx), K(store_end_idx), K(cnt),
              K(patch_first_), K(patch_last_));
    winfunc::StoreGuard store_guard(*this);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(input_stores_.cur_->attach_rows(get_all_expr(), input_row_meta_, eval_ctx_,
                                                       store_start_idx, store_end_idx, true))) {
      LOG_WARN("attach rows failed", K(ret));
    }
    FOREACH_WINCOL(END_WF) {
      if (OB_FAIL(it->res_->cur_->attach_rows(it->wf_info_.expr_, it->wf_res_row_meta_, eval_ctx_,
                                              store_start_idx, store_end_idx, true))) {
        LOG_WARN("attach rows failed", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(rd_apply_patches(cnt))) {
      LOG_WARN("apply patches failed", K(ret));
    } else {
      brs_.size_ = cnt;
      brs_.all_rows_active_ = true;
      input_stores_.cur_->output_row_idx_ += cnt;
      FOREACH_WINCOL(END_WF) {
        it->res_->cur_->output_row_idx_ += cnt;
      }
    }
  }
  return ret;
}

template <typename PartialMerge, typename ResFmt>
int ObWindowFunctionVecOp::rd_merge_result(PartialMerge &part_res, WinFuncInfo &info,
                                           int64_t rd_col_id,
                                           int64_t first_row_same_order_upper_bound,
                                           int64_t last_row_same_order_lower_bound)
{
  int ret = OB_SUCCESS;
  int64_t batch_size = eval_ctx_.get_batch_size();
  const char *payload = nullptr;
  int32_t len = 0;
  bool null_payload = false;
  ObExpr *wf_expr = info.expr_;
  const bool constexpr is_rank = std::is_same<PartialMerge, __PartialResult<T_WIN_FUN_RANK>>::value;
  ResFmt *res_data = static_cast<ResFmt *>(wf_expr->get_vector(eval_ctx_));
  ObEvalCtx::BatchInfoScopeGuard guard(eval_ctx_);
  if (patch_first_) {
    null_payload = rd_patch_->first_row_->is_null(rd_col_id);
    rd_patch_->first_row_->get_cell_payload(*rd_coord_row_meta_, rd_col_id, payload, len);
    for (int i = 0; !null_payload && OB_SUCC(ret) && i < batch_size; i++) {
      guard.set_batch_idx(i);
      if (is_rank && i >= first_row_same_order_upper_bound) {
        int64_t rank_patch = rd_patch_->first_row_frame_offset();
        if (OB_FAIL(part_res.template add_rank<ResFmt>(info, true, nullptr, 0, rank_patch))) {
          LOG_WARN("add rank failed", K(ret));
        }
      } else if (OB_FAIL(part_res.template merge<ResFmt>(info, null_payload, payload, len))) {
        LOG_WARN("merge result failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && patch_last_) {
    null_payload = rd_patch_->last_row_->is_null(rd_col_id);
    rd_patch_->last_row_->get_cell_payload(*rd_coord_row_meta_, rd_col_id, payload, len);
    for (int i = 0; !null_payload && OB_SUCC(ret) && i < batch_size; i++) {
      guard.set_batch_idx(i);
      if (i >= last_row_same_order_lower_bound) {
        if (OB_FAIL(part_res.template merge<ResFmt>(info, null_payload, payload, len))) {
          LOG_WARN("merge last row's patch failed", K(ret));
        }
      }
    }
  }
  return ret;
}

#define MERGE_FIXED_LEN_RES(vec_tc)                                                                \
  case (vec_tc): {                                                                                 \
    ret = rd_merge_result<decltype(part_res), ObFixedLengthFormat<RTCType<vec_tc>>>(               \
      part_res, info, col_idx, first_row_same_order_upper, last_row_same_order_lower);             \
  } break

#define MERGE_RES_COMMON()                                                                         \
  do {                                                                                             \
    if (fmt == VEC_DISCRETE) {                                                                     \
      ret = rd_merge_result<decltype(part_res), ObDiscreteFormat>(                                 \
        part_res, info, col_idx, first_row_same_order_upper, last_row_same_order_lower);           \
    } else if (fmt == VEC_CONTINUOUS) {                                                            \
      ret = rd_merge_result<decltype(part_res), ObContinuousFormat>(                               \
        part_res, info, col_idx, first_row_same_order_upper, last_row_same_order_lower);           \
    } else if (fmt == VEC_FIXED) {                                                                 \
      switch (vec_tc) {                                                                            \
        LST_DO_CODE(MERGE_FIXED_LEN_RES, FIXED_VEC_LIST);                                          \
      default: {                                                                                   \
        ret = OB_ERR_UNEXPECTED;                                                                   \
        LOG_WARN("unexpected vector type class", K(ret), K(vec_tc));                               \
      }                                                                                            \
      }                                                                                            \
    } else {                                                                                       \
      ret = OB_ERR_UNEXPECTED;                                                                     \
      LOG_WARN("unexpected format", K(ret), K(fmt));                                               \
    }                                                                                              \
  } while (false)

int ObWindowFunctionVecOp::rd_apply_patches(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  int64_t first_row_same_order_upper = 0, last_row_same_order_lower = max_row_cnt - 1;
  if (patch_first_ && OB_FAIL(rd_find_first_row_upper_bound(max_row_cnt, first_row_same_order_upper))) {
    LOG_WARN("find first row same order lower bound idx failed", K(ret));
  } else if (patch_last_ && OB_FAIL(rd_find_last_row_lower_bound(max_row_cnt, last_row_same_order_lower))) {
    LOG_WARN("find last row same order upper bound idx failed", K(ret));
  } else {
    LOG_TRACE("rd apply patch", K(first_row_same_order_upper), K(last_row_same_order_lower));
  }
  for (int i = 0; OB_SUCC(ret) && i < MY_SPEC.rd_wfs_.count(); i++) {
    WinFuncInfo &info = const_cast<WinFuncInfo &>(MY_SPEC.wf_infos_.at(MY_SPEC.rd_wfs_.at(i)));
    VectorFormat fmt = info.expr_->get_format(eval_ctx_);
    VecValueTypeClass vec_tc = info.expr_->get_vec_value_tc();
    int64_t col_idx = i + MY_SPEC.rd_sort_collations_.count();
    switch(info.func_type_) {
    case T_FUN_MIN: {
      __PartialResult<T_FUN_MIN> part_res(eval_ctx_, patch_alloc_);
      MERGE_RES_COMMON();
      break;
    }
    case T_FUN_MAX: {
      __PartialResult<T_FUN_MAX> part_res(eval_ctx_, patch_alloc_);
      MERGE_RES_COMMON();
      break;
    }
    case T_FUN_COUNT: {
      __PartialResult<T_FUN_COUNT> part_res(eval_ctx_, patch_alloc_);
      MERGE_RES_COMMON();
      break;
    }
    case T_FUN_SUM: {
      __PartialResult<T_FUN_SUM> part_res(eval_ctx_, patch_alloc_);
      if (fmt == VEC_DISCRETE) {
        ret = rd_merge_result<decltype(part_res), ObDiscreteFormat>(
          part_res, info, col_idx, first_row_same_order_upper, last_row_same_order_lower);
      } else if (fmt == VEC_FIXED) {
        switch(vec_tc) {
          LST_DO_CODE(MERGE_FIXED_LEN_RES, VEC_TC_FLOAT, VEC_TC_DOUBLE, VEC_TC_DEC_INT32,
                      VEC_TC_DEC_INT64, VEC_TC_DEC_INT128, VEC_TC_DEC_INT256, VEC_TC_DEC_INT512);
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected type class", K(ret));
        }
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected format", K(ret), K(fmt));
      }
      break;
    }
    case T_WIN_FUN_RANK: {
      __PartialResult<T_WIN_FUN_RANK> part_res(eval_ctx_, patch_alloc_);
      if (fmt == VEC_DISCRETE) {
        ret = rd_merge_result<decltype(part_res), ObDiscreteFormat>(
          part_res, info, col_idx, first_row_same_order_upper, last_row_same_order_lower);
      } else if (fmt == VEC_FIXED) {
        if (vec_tc != VEC_TC_UINTEGER) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected vec tc", K(vec_tc));
        } else {
          ret = rd_merge_result<decltype(part_res), ObFixedLengthFormat<uint64_t>>(
          part_res, info, col_idx, first_row_same_order_upper, last_row_same_order_lower);
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected format", K(ret), K(fmt));
      }
      break;
    }
    case T_WIN_FUN_DENSE_RANK: {
      __PartialResult<T_WIN_FUN_DENSE_RANK> part_res(eval_ctx_, patch_alloc_);
      if (fmt == VEC_DISCRETE) {
        ret = rd_merge_result<decltype(part_res), ObDiscreteFormat>(
          part_res, info, col_idx, first_row_same_order_upper, last_row_same_order_lower);
      } else if (fmt == VEC_FIXED) {
        if (vec_tc != VEC_TC_UINTEGER) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected vec tc", K(vec_tc));
        } else {
          ret = rd_merge_result<decltype(part_res), ObFixedLengthFormat<uint64_t>>(
            part_res, info, col_idx, first_row_same_order_upper, last_row_same_order_lower);
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected format", K(ret), K(fmt));
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("not supported func type", K(ret), K(info.func_type_));
    }
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("merge result failed", K(info), K(fmt), K(vec_tc), K(col_idx));
    }
  }
  return ret;
}

int ObWindowFunctionVecOp::rd_find_first_row_upper_bound(int64_t batch_size, int64_t &upper_bound)
{
  int ret = OB_SUCCESS;
  upper_bound = batch_size;
  const char *val = nullptr;
  int32_t val_len = 0;
  bool val_isnull = false;
  int32_t word_cnt = ObBitVector::word_count(batch_size);
  MEMSET(batch_ctx_.bound_eval_skip_, 0, word_cnt * ObBitVector::BYTES_PER_WORD);
  EvalBound bound(batch_size, 0, batch_size, true);
  if (OB_ISNULL(rd_patch_->first_row_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null first patch row", K(ret));
  }
  for (int i = 0; OB_SUCC(ret) && i < MY_SPEC.rd_sort_collations_.count(); i++) {
    int64_t tmp_bound = -1;
    ObExpr *sort_expr = MY_SPEC.rd_coord_exprs_.at(i);
    NullSafeRowCmpFunc cmp_fn = (MY_SPEC.rd_sort_collations_.at(i).null_pos_ == NULL_FIRST ?
                                  sort_expr->basic_funcs_->row_null_first_cmp_ :
                                  sort_expr->basic_funcs_->row_null_last_cmp_);
    rd_patch_->first_row_->get_cell_payload(rd_patch_->row_meta_, i, val, val_len);
    val_isnull = rd_patch_->first_row_->is_null(i);
    VectorRangeUtil::NullSafeCmp cmp_op(sort_expr->obj_meta_, cmp_fn, val, val_len, val_isnull,
                                        MY_SPEC.rd_sort_collations_.at(i).is_ascending_);
    if (OB_FAIL(VectorRangeUtil::upper_bound(sort_expr, eval_ctx_, bound,
                                             *batch_ctx_.bound_eval_skip_, cmp_op, tmp_bound))) {
      LOG_WARN("find upper bound failed", K(ret));
    } else if (OB_UNLIKELY(tmp_bound == -1)) {
      // no value is larger than first_row
      // upper_bound = batch_size;
      // do nothing
    } else {
      upper_bound = std::min(tmp_bound, upper_bound);
    }
  }
  return ret;
}

int ObWindowFunctionVecOp::rd_find_last_row_lower_bound(int64_t batch_size, int64_t &lower_bound)
{
  int ret = OB_SUCCESS;
  lower_bound = 0;
  const char *val = nullptr;
  int32_t val_len = 0;
  bool val_isnull = false;
  int32_t word_cnt = ObBitVector::word_count(batch_size);
  MEMSET(batch_ctx_.bound_eval_skip_, 0, word_cnt * ObBitVector::BYTES_PER_WORD);
  EvalBound bound(batch_size, 0, batch_size, true);
  if (OB_ISNULL(rd_patch_->last_row_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null last patch row", K(ret));
  }
  for (int i = 0; OB_SUCC(ret) && i < MY_SPEC.rd_sort_collations_.count(); i++) {
    int64_t tmp_bound = -1;
    int64_t field_idx = MY_SPEC.rd_sort_collations_.at(i).field_idx_;
    ObExpr *sort_expr = MY_SPEC.all_expr_.at(field_idx);
    NullSafeRowCmpFunc cmp_fn = (MY_SPEC.rd_sort_collations_.at(i).null_pos_ == NULL_FIRST ?
                                  sort_expr->basic_funcs_->row_null_first_cmp_ :
                                  sort_expr->basic_funcs_->row_null_last_cmp_);
    rd_patch_->last_row_->get_cell_payload(rd_patch_->row_meta_, i, val, val_len);
    val_isnull = rd_patch_->last_row_->is_null(i);
    VectorRangeUtil::NullSafeCmp cmp_op(sort_expr->obj_meta_, cmp_fn, val, val_len, val_isnull,
                                        MY_SPEC.rd_sort_collations_.at(i).is_ascending_);
    if (OB_FAIL(VectorRangeUtil::lower_bound(sort_expr, eval_ctx_, bound,
                                             *batch_ctx_.bound_eval_skip_, cmp_op, tmp_bound))) {
      LOG_WARN("find lower bound failed", K(ret));
    } else if (OB_UNLIKELY(tmp_bound == -1)) {
      lower_bound = batch_size;
      break;
    } else {
      lower_bound = std::max(lower_bound, tmp_bound);
    }
  }
  return ret;
}

bool ObWindowFunctionVecOp::all_supported_winfuncs(const ObIArray<ObWinFunRawExpr *> &win_exprs)
{
  bool ret = true;
  for (int i = 0; ret && i < win_exprs.count(); i++) {
    ObWinFunRawExpr *win_expr = win_exprs.at(i);
    if (win_expr->get_agg_expr() != nullptr) {
      ret = aggregate::supported_aggregate_function(win_expr->get_func_type())
            && !win_expr->get_agg_expr()->is_param_distinct();
    }
  }
  return ret;
}

int ObWindowFunctionVecOp::backup_child_vectors(int64_t batch_size)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(all_expr_vector_copy_.save(batch_size))) {
    LOG_WARN("save vector results failed", K(ret));
  } else {
    backuped_size_ = batch_size;
  }
  return ret;
}

int ObWindowFunctionVecOp::restore_child_vectors()
{
  int ret = OB_SUCCESS;
  if (backuped_size_ <= 0) {
  } else if (OB_FAIL(all_expr_vector_copy_.restore())) {
    LOG_WARN("restore vector results failed", K(ret));
  } else {
    backuped_size_ = 0;
  }
  return ret;
}

int ObWindowFunctionVecOp::init_mem_context()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(mem_context_)) {
    ObSQLSessionInfo *sess = ctx_.get_my_session();
    uint64_t tenant_id = sess->get_effective_tenant_id();
    lib::ContextParam param;
    param.set_mem_attr(tenant_id, ObModIds::OB_SQL_WINDOW_ROW_STORE, ObCtxIds::WORK_AREA)
      .set_properties(lib::USE_TL_PAGE_OPTIONAL);
    if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(mem_context_, param))) {
      LOG_WARN("create entity failed", K(ret));
    } else if (OB_ISNULL(mem_context_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null memory entity returned", K(ret));
    }
  }
  return ret;
}

void ObWindowFunctionVecOp::destroy_mem_context()
{
  if (OB_NOT_NULL(mem_context_)) {
    DESTROY_CONTEXT(mem_context_);
    mem_context_ = nullptr;
  }
}

struct __mem_pred_op
{
  const static int64_t UPDATE_MEM_SIZE_PERIODIC_CNT = 1024;
  __mem_pred_op(ObSqlMemMgrProcessor &p, int64_t &amm_p_cnt): p_(p), amm_p_cnt_(amm_p_cnt) {}
  OB_INLINE bool operator()(int64_t cur_cnt)
  {
    UNUSED(cur_cnt);
    p_.set_periodic_cnt(1024);
    return 0 == ((++amm_p_cnt_) % UPDATE_MEM_SIZE_PERIODIC_CNT);
  }
private:
  ObSqlMemMgrProcessor &p_;
  int64_t &amm_p_cnt_;
};

struct __mem_extend_op
{
  __mem_extend_op(ObSqlMemMgrProcessor &p): p_(p) {}
  OB_INLINE bool operator()(int64_t max_memory_size)
  {
    return p_.get_data_size() > max_memory_size;
  }
private:
  ObSqlMemMgrProcessor &p_;
};
int ObWindowFunctionVecOp::update_mem_limit_version_periodically()
{
  int ret = OB_SUCCESS;
  // update global mem bound every 1024 rows
  // use total_stored_row_cnt of wf op instead of row_cnt of each ra_rs_ here
  // because total_stored_row_cnt is monotone increasing
  bool updated = false;
  bool need_inc_version = false;
  if (!GCONF.is_sql_operator_dump_enabled()) {
  } else if (OB_FAIL(sql_mem_processor_.update_max_available_mem_size_periodically(
               &mem_context_->get_malloc_allocator(),
               __mem_pred_op(sql_mem_processor_, amm_periodic_cnt_), updated))) {
    LOG_WARN("failed to update max available memory size periodically", K(ret));
  } else if (updated || need_dump()) {
    if (OB_FAIL(sql_mem_processor_.extend_max_memory_size(
          &mem_context_->get_malloc_allocator(), __mem_extend_op(sql_mem_processor_),
          need_inc_version, sql_mem_processor_.get_data_size()))) {
      LOG_WARN("failed to extend max memory size", K(ret), K(updated), K(need_dump()));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (need_inc_version) {
    ++global_mem_limit_version_;
  }
  return ret;
}

// ================ WinFuncColExpr
int WinFuncColExpr::init_aggregate_ctx(const int64_t tenant_id)
{
  // only aggr expr need agg_ctx_
  using info_array = ObFixedArray<ObAggrInfo, ObIAllocator>;
  int ret = OB_SUCCESS;
  ObIAllocator &local_allocator = *op_.local_allocator_;
  info_array *aggr_infos = OB_NEWx(info_array, &local_allocator, local_allocator, 1);
  void *aggr_row_buf = nullptr, *res_row_buf = nullptr;
  int32_t aggr_row_buf_sz = 0, res_row_buf_sz = 0;
  if (OB_ISNULL(aggr_infos)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  } else if (OB_FAIL(aggr_infos->push_back(wf_info_.aggr_info_))) {
    LOG_WARN("push back element failed");
  } else {
    winfunc::AggrExpr *agg_expr = static_cast<winfunc::AggrExpr *>(wf_expr_);
    agg_expr->aggr_processor_ = OB_NEWx(aggregate::Processor, &local_allocator,
                                        op_.eval_ctx_, *aggr_infos,
                                        ObModIds::OB_SQL_WINDOW_LOCAL,
                                        op_.op_monitor_info_, tenant_id);
    if (OB_ISNULL(agg_expr->aggr_processor_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else if (OB_FAIL(agg_expr->aggr_processor_->init())) {
      LOG_WARN("processor init failed", K(ret), K(wf_info_.aggr_info_));
    } else if (FALSE_IT(agg_expr->aggr_processor_->set_support_fast_single_row_agg(true))) {
    } else if (FALSE_IT(agg_ctx_ = agg_expr->aggr_processor_->get_rt_ctx())) {
    } else if (FALSE_IT(aggr_row_buf_sz = op_.spec_.max_batch_size_ * agg_ctx_->row_meta().row_size_)) {
      // do nothing
    } else if (OB_ISNULL(aggr_row_buf = local_allocator.alloc(aggr_row_buf_sz))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else {
      agg_ctx_->win_func_agg_ = true;
      MEMSET(aggr_row_buf, 0, aggr_row_buf_sz);
      aggr_rows_ = (aggregate::AggrRowPtr *)local_allocator.alloc(sizeof(aggregate::AggrRowPtr)
                                                                  * op_.spec_.max_batch_size_);
      if (OB_ISNULL(aggr_rows_)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret));
      } else {
        int32_t offset = 0, step = agg_ctx_->row_meta().row_size_;
        for (int i = 0; i < op_.spec_.max_batch_size_; i++, offset += step) {
          aggr_rows_[i] = (char *)aggr_row_buf + offset;
        }
      }
    }
  }
  return ret;
}

int32_t WinFuncColExpr::non_aggr_reserved_row_size() const
{
  int32_t ret_size = 0;
  VecValueTypeClass vec_tc = wf_info_.expr_->get_vec_value_tc();
  if (!wf_expr_->is_aggregate_expr()) {
    if (is_fixed_length_vec(vec_tc) || vec_tc == VEC_TC_NUMBER) {
      ret_size = ObDatum::get_reserved_size(
        ObDatum::get_obj_datum_map_type(wf_info_.expr_->datum_meta_.type_), wf_info_.expr_->datum_meta_.precision_);
    } else {
      ret_size = sizeof(char *) + sizeof(uint32_t); // <char *, len>
    }
  }
  return ret_size;
}
int WinFuncColExpr::init_non_aggregate_ctx()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(wf_expr_->is_aggregate_expr())) {
    // do nothing
  } else {
    int64_t batch_size = op_.get_spec().max_batch_size_;
    ObIAllocator &local_alloc = *op_.local_allocator_;
    int64_t word_cnt = ObBitVector::word_count(batch_size);
    int32_t reserved_row_size = non_aggr_reserved_row_size();
    void *data_buf = local_alloc.alloc(reserved_row_size * batch_size);
    null_nonaggr_results_ = (ObBitVector *)local_alloc.alloc(word_cnt * ObBitVector::BYTES_PER_WORD);
    if (OB_ISNULL(data_buf) || OB_ISNULL(null_nonaggr_results_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else {
      MEMSET(data_buf, 0, reserved_row_size * batch_size);
      MEMSET(null_nonaggr_results_, 0, word_cnt * ObBitVector::BYTES_PER_WORD);
      int64_t offset = 0;
      non_aggr_results_ = (char *)data_buf;
    }
  }
  return ret;
}

int WinFuncColExpr::init_res_rows(const int64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObIAllocator &local_allocator = *op_.local_allocator_;
  wf_res_row_meta_.set_allocator(&local_allocator);
  void *res_row_ptr_buf = nullptr;
  int32_t res_row_ptr_buf_sz = 0;
  ObSEArray<ObExpr *, 1> all_exprs;
  if (OB_FAIL(all_exprs.push_back(wf_info_.expr_))) {
    LOG_WARN("push back elements failed", K(ret));
  } else if (OB_FAIL(wf_res_row_meta_.init(all_exprs, 0))) {
    LOG_WARN("init compact row meta failed", K(ret));
  } else if (FALSE_IT(res_row_ptr_buf_sz = op_.spec_.max_batch_size_ * sizeof(const ObCompactRow *))) {
  } else if (OB_ISNULL(res_row_ptr_buf = local_allocator.alloc(res_row_ptr_buf_sz))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  } else {
    MEMSET(res_row_ptr_buf, 0, res_row_ptr_buf_sz);
    res_rows_ = (const ObCompactRow **)res_row_ptr_buf;
  }
  return ret;
}

int WinFuncColExpr::reset_for_partition(const int64_t batch_size, const ObBitVector &skip)
{
  int ret = OB_SUCCESS;
  UNUSED(skip);
  OB_ASSERT(batch_size <= op_.spec_.max_batch_size_);
  if (wf_expr_->is_aggregate_expr()) {
    agg_ctx_->reuse();
    agg_ctx_->removal_info_.reset();
    aggregate::Processor *processor = static_cast<winfunc::AggrExpr *>(wf_expr_)->aggr_processor_;
    for (int i = 0; OB_SUCC(ret) && i < batch_size; i++) {
      if (OB_FAIL(processor->add_one_aggregate_row(aggr_rows_[i], agg_ctx_->row_meta().row_size_))) {
        LOG_WARN("setup rt info failed", K(ret));
      }
    }
  } else {
    int64_t word_cnt = ObBitVector::word_count(batch_size);
    MEMSET(null_nonaggr_results_, 0, ObBitVector::BYTES_PER_WORD * word_cnt);
  }
  return ret;
}

void WinFuncColExpr::reset()
{
  if (wf_expr_ != nullptr) {
    wf_expr_->destroy();
    wf_expr_ = nullptr;
  }
  if (res_ != nullptr) {
    res_->reset();
    res_ = nullptr;
  }
  agg_ctx_ = nullptr;
  wf_res_row_meta_.reset();
}

void WinFuncColExpr::reset_for_scan()
{
  part_first_row_idx_ = 0;
  MEMSET(pby_row_mapped_idxes_, 0, op_.max_pby_col_cnt_ * sizeof(int32_t));
}
// ================ ObWindownFunctionVecSpec
int ObWindowFunctionVecSpec::register_to_datahub(ObExecContext &ctx) const
{
  int ret = OB_SUCCESS;
  if (single_part_parallel_) {
    if (OB_ISNULL(ctx.get_sqc_handler())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null sqc handler", K(ret));
    } else {
      SPWinFuncPXWholeMsg::WholeMsgProvider *provider =
        OB_NEWx(SPWinFuncPXWholeMsg::WholeMsgProvider, &ctx.get_allocator());
      if (OB_ISNULL(provider)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret));
      } else {
        ObSqcCtx &sqc_ctx = ctx.get_sqc_handler()->get_sqc_ctx();
        if (OB_FAIL(sqc_ctx.add_whole_msg_provider(get_id(), dtl::DH_SP_WINFUNC_PX_WHOLE_MSG,
                                                   *provider))) {
          LOG_WARN("add whole msg provider failed", K(ret));
        }
      }
    }
  } else if (range_dist_parallel_) {
    RDWinFuncPXWholeMsg::WholeMsgProvider *provider = OB_NEWx(RDWinFuncPXWholeMsg::WholeMsgProvider, &ctx.get_allocator());
    if (OB_ISNULL(provider)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else if (OB_ISNULL(ctx.get_sqc_handler())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null sqc handler", K(ret));
    } else if (OB_FAIL(ctx.get_sqc_handler()->get_sqc_ctx().add_whole_msg_provider(
                 get_id(), dtl::DH_RD_WINFUNC_PX_WHOLE_MSG, *provider))) {
      LOG_WARN("add whole msg failed", K(ret));
    }
  } else if (is_participator()) {
    if (OB_ISNULL(ctx.get_sqc_handler())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null sqc handler", K(ret));
    } else {
      ObReportingWFWholeMsg::WholeMsgProvider *provider =
        OB_NEWx(ObReportingWFWholeMsg::WholeMsgProvider, &ctx.get_allocator());
      if (OB_ISNULL(provider)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret));
      } else {
        ObSqcCtx &sqc_ctx = ctx.get_sqc_handler()->get_sqc_ctx();
        if (OB_FAIL(sqc_ctx.add_whole_msg_provider(
              get_id(), dtl::DH_SECOND_STAGE_REPORTING_WF_WHOLE_MSG, *provider))) {
          LOG_WARN("add whole msg failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObWindowFunctionVecSpec::rd_sort_cmp(RowMeta &row_meta, const ObCompactRow *l_row,
                                         const ObCompactRow *r_row, const int64_t begin,
                                         const int64_t end, int &cmp_ret) const
{
  int ret = OB_SUCCESS;
  cmp_ret = 0;
  const char *l_payload = nullptr, *r_payload = nullptr;
  int32_t l_len = 0, r_len = 0;
  bool l_isnull = false, r_isnull = false;
  if (l_row == nullptr && r_row == nullptr) {
    cmp_ret = 0;
  } else if (l_row == nullptr) {
    cmp_ret = 1;
  } else if (r_row == nullptr) {
    cmp_ret = -1;
  } else {
    for (int64_t i = begin; cmp_ret == 0 && OB_SUCC(ret) && i < end; i++) {
      ObObjMeta &obj_meta = rd_coord_exprs_.at(i)->obj_meta_;
      l_row->get_cell_payload(row_meta, i, l_payload, l_len);
      l_isnull = l_row->is_null(i);
      r_row->get_cell_payload(row_meta, i, r_payload, r_len);
      r_isnull = r_row->is_null(i);
      if (OB_FAIL(rd_sort_cmp_funcs_.at(i).row_cmp_func_(obj_meta, obj_meta, l_payload, l_len,
                                                         l_isnull, r_payload, r_len, r_isnull,
                                                         cmp_ret))) {
        LOG_WARN("compare failed", K(ret));
      } else if(!rd_sort_collations_.at(i).is_ascending_) {
        cmp_ret = cmp_ret * (-1);
      }
    }
  }
  return ret;
}

struct __pby_oby_sort_op
{
  __pby_oby_sort_op(const ObWindowFunctionVecSpec &spec): spec_(spec) {}
  // sort by (PBY, OBY, SQC_ID, THREAD_ID)
  OB_INLINE int operator()(RDWinFuncPXPartialInfo *l, RDWinFuncPXPartialInfo *r)
  {
    int cmp_ret = 0;
    (void)spec_.rd_pby_oby_cmp(l->row_meta_, l->first_row_, r->first_row_, cmp_ret);
    if (cmp_ret == 0) {
      (void)spec_.rd_pby_oby_cmp(l->row_meta_, l->last_row_, r->last_row_, cmp_ret);
    }
    return (cmp_ret == 0) ?
             (std::tie(l->sqc_id_, l->thread_id_) < std::tie(r->sqc_id_, r->thread_id_)) :
             (cmp_ret < 0);
  };
private:
  const ObWindowFunctionVecSpec &spec_;
};

int ObWindowFunctionVecSpec::rd_generate_patch(RDWinFuncPXPieceMsgCtx &msg_ctx, ObEvalCtx &eval_ctx) const
{
  int ret = OB_SUCCESS;
  lib::ob_sort(msg_ctx.infos_.begin(), msg_ctx.infos_.end(), __pby_oby_sort_op(*this));
#ifndef NDEBUG
  for (int i = 0; i < msg_ctx.infos_.count(); i++) {
    RDWinFuncPXPartialInfo *info = msg_ctx.infos_.at(i);
    if (info->first_row_ == nullptr) { break; }
    CompactRow2STR first_row(info->row_meta_, *info->first_row_, &rd_coord_exprs_);
    CompactRow2STR last_row(info->row_meta_, *info->last_row_, &rd_coord_exprs_);
    int64_t first_row_extra = *reinterpret_cast<int64_t *>(info->first_row_->get_extra_payload(info->row_meta_));
    int64_t last_row_extra = *reinterpret_cast<int64_t *>(info->last_row_->get_extra_payload(info->row_meta_));
    LOG_INFO("generating patch", K(i), K(first_row), K(last_row), K(first_row_extra), K(last_row_extra));
  }
#endif
  // first generate frame offset
  RDWinFuncPXPartialInfo *prev = nullptr;
  int cmp_ret = 0;
  for (int i = 0; OB_SUCC(ret) && i < msg_ctx.infos_.count(); i++) {
    RDWinFuncPXPartialInfo *cur = msg_ctx.infos_.at(i);
    if (cur->first_row_ == NULL) {
      break;
    }
    bool prev_same_part = (nullptr != prev);
    if (prev_same_part) {
      if (OB_FAIL(rd_pby_cmp(cur->row_meta_, prev->last_row_, cur->first_row_, cmp_ret))) {
        LOG_WARN("compare failed", K(ret));
      } else {
        prev_same_part = (cmp_ret == 0);
      }
    }
    if (OB_FAIL(ret)) {
    } else if (prev_same_part) {
      cur->first_row_frame_offset() = prev->last_row_frame_offset() + 1;
      if (OB_FAIL(rd_pby_cmp(cur->row_meta_, cur->first_row_, cur->last_row_, cmp_ret))) {
        LOG_WARN("compare failed", K(ret));
      } else if (cmp_ret == 0) {
        cur->last_row_frame_offset() += prev->last_row_frame_offset() + 1;
      }
    }
    prev = cur;
  } // end for

  // second: generate patch info for each window function
  ObEvalCtx::BatchInfoScopeGuard guard(eval_ctx);
  // indexes:
  // 0: first_row's patch
  // 1: last_row's patch
  // 2: first_row's ranking after patching
  // 3: last_row's ranking after patching
  guard.set_batch_size(4);
  const char *payload = nullptr;
  int32_t len = 0;
  bool null_payload;
  using patch_pair = std::pair<ObCompactRow *, ObCompactRow *>;

  ObSEArray<patch_pair, 128> patch_pairs;
  LastCompactRow first_row_patch(msg_ctx.arena_alloc_);
  LastCompactRow last_row_patch(msg_ctx.arena_alloc_);
  __PartialResult<T_INVALID> part_res(eval_ctx, msg_ctx.arena_alloc_);
  // use uniform/uniform_const format as data format
  // PX Coordinator may not supported vectorization 2.0,
  // in this case, if vector headers are initilized as default formats (discrete/fixed_length formats)
  // but expr data are filled with datums, unexpected errors will happen.
  for (int i = 0; OB_SUCC(ret) && i < rd_coord_exprs_.count(); i++) {
    VectorFormat default_fmt = rd_coord_exprs_.at(i)->get_default_res_format();
    // TODO: @zongmei.zzm support batch_size < 4 for range distribution
    if (OB_FAIL(rd_coord_exprs_.at(i)->init_vector_for_write(
          eval_ctx, rd_coord_exprs_.at(i)->is_const_expr() ? VEC_UNIFORM_CONST : VEC_UNIFORM, 4))) {
      LOG_WARN("init vector failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(first_row_patch.init_row_meta(rd_coord_exprs_, sizeof(int64_t), false))) {
      LOG_WARN("init row meta failed", K(ret));
    } else if (OB_FAIL(last_row_patch.init_row_meta(rd_coord_exprs_, sizeof(int64_t), false))) {
      LOG_WARN("init row meta failed", K(ret));
    }
  }
  for (int idx = 0; OB_SUCC(ret) && idx < msg_ctx.infos_.count(); idx++) {
    RDWinFuncPXPartialInfo *cur = msg_ctx.infos_.at(idx);
    if (cur->first_row_ == nullptr) {
      break;
    }
    guard.set_batch_idx(0);
    if (OB_FAIL(first_row_patch.save_store_row(*cur->first_row_))) {
      LOG_WARN("save first row failed", K(ret));
    } else if (OB_FAIL(first_row_patch.to_expr(rd_coord_exprs_, eval_ctx))) {
      LOG_WARN("to expr failed", K(ret));
    } else if (OB_FAIL(last_row_patch.save_store_row(*cur->last_row_))) {
      LOG_WARN("save last row failed", K(ret));
    } else if (FALSE_IT(guard.set_batch_idx(1))) {
    } else if (OB_FAIL(last_row_patch.to_expr(rd_coord_exprs_, eval_ctx))) {
      LOG_WARN("to expr failed", K(ret));
    } else {// do nothing
    }
    for (int i = 0; OB_SUCC(ret) && i < rd_wfs_.count(); i++) {
      const WinFuncInfo &wf_info = wf_infos_.at(rd_wfs_.at(i));
      const bool is_rank = (wf_info.func_type_ == T_WIN_FUN_RANK);
      const bool is_dense_rank = (wf_info.func_type_ == T_WIN_FUN_DENSE_RANK);
      const bool is_range_frame = (wf_info.win_type_ == WINDOW_RANGE);
      int64_t res_idx = i + rd_sort_collations_.count();
      ObExpr *patch_expr = rd_coord_exprs_.at(res_idx);
      if (FALSE_IT(patch_expr->get_vector(eval_ctx)->set_null(0))) {
      } else if (FALSE_IT(patch_expr->get_vector(eval_ctx)->set_null(1))) {
        // do nothing
      } else if (is_rank || is_dense_rank) {
        prev = nullptr;
        int64_t prev_idx = idx - 1;
        bool prev_same_part = (prev_idx >= 0);
        if (prev_same_part) {
          prev = msg_ctx.infos_.at(prev_idx);
          if (OB_FAIL(rd_pby_cmp(cur->row_meta_, prev->last_row_, cur->first_row_, cmp_ret))) {
            LOG_WARN("compare failed", K(ret));
          } else {
            prev_same_part = (cmp_ret == 0);
          }
        }
        // patch first_row
        guard.set_batch_idx(0);
        if (OB_FAIL(ret)) {
        } else if (!prev_same_part) {
          // do nothing
        } else if (OB_FAIL(rd_oby_cmp(cur->row_meta_, prev->last_row_, cur->first_row_, cmp_ret))) {
          LOG_WARN("compare failed", K(ret));
        } else if (cmp_ret == 0) { // prev same order
          // prev last row
          patch_expr->get_vector(eval_ctx)->get_payload(3, payload, len);
          ObCompactRow *prev_last_row_patch = patch_pairs.at(prev_idx).second;
          if (OB_FAIL(part_res.add_rank<ObIVector>(wf_info, false, payload, len, -1))) {
            LOG_WARN("add rank failed", K(ret));
          }
        } else if (is_rank
                   && OB_FAIL(part_res.add_rank<ObIVector>(wf_info, true, nullptr, 0,
                                                           cur->first_row_frame_offset()))) {
          LOG_WARN("add rank failed", K(ret));
        } else if (is_dense_rank) {
          patch_expr->get_vector(eval_ctx)->get_payload(3, payload, len);
          if (OB_FAIL(part_res.add_rank<ObIVector>(wf_info, false, payload, len, 0))) {
            LOG_WARN("add rank failed", K(ret));
          }
        }
        // if first_row & last_row in different partition, patch is not needed for last_row
        // if first_row & last_row in same partition, patch_first will patch value into last row, no need patching for last row as well

        // store rank results
        patch_expr->get_vector(eval_ctx)->set_null(2);
        patch_expr->get_vector(eval_ctx)->set_null(3);
        ObIVector *patch = patch_expr->get_vector(eval_ctx);
        guard.set_batch_idx(2);
        if (FALSE_IT(cur->first_row_->get_cell_payload(cur->row_meta_, res_idx, payload, len))) {
        } else if (OB_FAIL(part_res.add_rank<ObIVector>(wf_info, cur->first_row_->is_null(res_idx),
                                                        payload, len, 0))) {
          LOG_WARN("add rank failed", K(ret));
        } else if (FALSE_IT(patch->get_payload(0, payload, len))) {
        } else if (OB_FAIL(
                     part_res.add_rank<ObIVector>(wf_info, patch->is_null(0), payload, len, 0))) {
          LOG_WARN("add rank failed", K(ret));
        }
        guard.set_batch_idx(3);
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(rd_pby_cmp(cur->row_meta_, cur->first_row_, cur->last_row_, cmp_ret))) {
          LOG_WARN("compare failed", K(ret));
        } else if (cmp_ret == 0) {
          // first_row & last_row have same order, add first_row's patch into last_row
          // else add first row's frame_offset into last_row
          if (OB_FAIL(rd_oby_cmp(cur->row_meta_, cur->first_row_, cur->last_row_, cmp_ret))) {
            LOG_WARN("compare failed", K(ret));
          } else if (cmp_ret == 0 || is_dense_rank) {
            if (OB_FAIL(
                  part_res.add_rank<ObIVector>(wf_info, patch->is_null(0), payload, len, 0))) {
              LOG_WARN("add rank failed", K(ret));
            }
          } else if (cmp_ret != 0
                     && OB_FAIL(part_res.add_rank<ObIVector>(wf_info, true, nullptr, 0,
                                                             cur->first_row_frame_offset()))) {
            LOG_WARN("add rank failed", K(ret));
          }
        }
        if (OB_FAIL(ret)) {
          LOG_WARN("add rank failed", K(ret));
        } else if (FALSE_IT(cur->get_cell(res_idx, false, payload, len))) {
        } else if (OB_FAIL(part_res.add_rank<ObIVector>(wf_info, cur->is_null(res_idx, false),
                                                        payload, len, 0))) {
          LOG_WARN("add rank failed", K(ret));
        }
      } else { // aggregation function
        cmp_ret = 0;
        // coordinator will patch partial results in following steps:
        // 1. for [0...part_cnt], patch first_row's patch into each rows in partition
        // 2. for [0...part_cnt], patch last_row's patch into rows which have same order as last_row in partition

        // hence, first_row's patch is sum of previous partial results with same partition
        // last_row's patch is sum of following partial results with same partition and same order

        // first row's patch
        guard.set_batch_idx(0);
        for (int prev_idx = idx - 1; cmp_ret == 0 && OB_SUCC(ret) && prev_idx >= 0; prev_idx--) {
          RDWinFuncPXPartialInfo *prev = msg_ctx.infos_.at(prev_idx);
          if (OB_FAIL(rd_pby_cmp(cur->row_meta_, prev->last_row_, cur->first_row_, cmp_ret))) {
            LOG_WARN("compare failed", K(ret));
          } else if (cmp_ret == 0) {
            prev->get_cell(res_idx, false, payload, len);
            null_payload = prev->is_null(res_idx, false);
            if (OB_FAIL(part_res.merge<ObIVector>(wf_info, null_payload, payload, len))) {
              LOG_WARN("merge result failed", K(ret));
            }
          }
        } // end for

        // last row's patch
        guard.set_batch_idx(1);
        cmp_ret = 0;
        for (int post_idx = idx + 1;
             is_range_frame && cmp_ret == 0 && OB_SUCC(ret) && post_idx < msg_ctx.infos_.count();
             post_idx++) {
          RDWinFuncPXPartialInfo *post = msg_ctx.infos_.at(post_idx);
          if (OB_FAIL(rd_pby_oby_cmp(cur->row_meta_, post->first_row_, cur->last_row_, cmp_ret))) {
            LOG_WARN("compare failed", K(ret));
          } else if (cmp_ret == 0) {
            post->get_cell(res_idx, true, payload, len);
            null_payload = post->is_null(res_idx, true);
            if (OB_FAIL(part_res.merge<ObIVector>(wf_info, null_payload, payload, len))) {
              LOG_WARN("merge result failed", K(ret));
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        patch_expr->set_evaluated_projected(eval_ctx);
      }
    } // end iter of wf_infos
    if (OB_SUCC(ret)) {
      int64_t mock_skip = 0;
      ObBatchRows tmp_brs;
      tmp_brs.size_ = 2;
      tmp_brs.skip_ = to_bit_vector(&mock_skip);
      tmp_brs.end_ = false;
      guard.set_batch_idx(0);
      patch_pair tmp_pair;
      if (OB_FAIL(first_row_patch.save_store_row(rd_coord_exprs_, tmp_brs, eval_ctx, sizeof(int64_t), false))) {
        LOG_WARN("save store row failed", K(ret));
      } else if (FALSE_IT(guard.set_batch_idx(1))) {
      } else if (OB_FAIL(
                   last_row_patch.save_store_row(rd_coord_exprs_, tmp_brs, eval_ctx, sizeof(int64_t), false))) {
        LOG_WARN("save store row failed", K(ret));
      } else {
        *reinterpret_cast<int64_t *>(first_row_patch.compact_row_->get_extra_payload(
          cur->row_meta_)) = cur->first_row_frame_offset();
        *reinterpret_cast<int64_t *>(last_row_patch.compact_row_->get_extra_payload(
          cur->row_meta_)) = cur->last_row_frame_offset();
        int32_t buf_size = first_row_patch.compact_row_->get_row_size()
                           + last_row_patch.compact_row_->get_row_size();
        char *buf = (char *)msg_ctx.arena_alloc_.alloc(buf_size);
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate memory failed", K(ret));
        } else {
          MEMCPY(buf, first_row_patch.compact_row_, first_row_patch.compact_row_->get_row_size());
          tmp_pair.first = reinterpret_cast<ObCompactRow *>(buf);
          buf += first_row_patch.compact_row_->get_row_size();
          MEMCPY(buf, last_row_patch.compact_row_, last_row_patch.compact_row_->get_row_size());
          tmp_pair.second = reinterpret_cast<ObCompactRow *>(buf);
          if (OB_FAIL(patch_pairs.push_back(tmp_pair))) { LOG_WARN("push back failed", K(ret)); }
        }
      }
    }
  }
  for (int i = 0; OB_SUCC(ret) && i < msg_ctx.infos_.count(); i++) {
    if (i >= patch_pairs.count()) {
      msg_ctx.infos_.at(i)->first_row_ = nullptr;
      msg_ctx.infos_.at(i)->last_row_ = nullptr;
    } else {
      msg_ctx.infos_.at(i)->first_row_ = patch_pairs.at(i).first;
      msg_ctx.infos_.at(i)->last_row_ = patch_pairs.at(i).second;
    }
  }
#ifndef NDEBUG
  for (int i = 0; i < msg_ctx.infos_.count(); i++) {
    RDWinFuncPXPartialInfo *info = msg_ctx.infos_.at(i);
    if (info->first_row_ == nullptr) { break; }
    CompactRow2STR first_row(info->row_meta_, *info->first_row_, &rd_coord_exprs_);
    CompactRow2STR last_row(info->row_meta_, *info->last_row_, &rd_coord_exprs_);
    int64_t first_row_extra = *reinterpret_cast<int64_t *>(info->first_row_->get_extra_payload(info->row_meta_));
    int64_t last_row_extra = *reinterpret_cast<int64_t *>(info->last_row_->get_extra_payload(info->row_meta_));
    LOG_INFO("after generating patch", K(i), K(first_row), K(last_row), K(first_row_extra), K(last_row_extra));
  }
#endif
  return ret;
}

// ================ ObWindowFunctionVecOpInput
int ObWindowFunctionVecOpInput::init_wf_participator_shared_info(ObIAllocator &alloc, int64_t task_cnt)
{
  int ret = OB_SUCCESS;
  ObWFParticipatorSharedInfo *shared_info = nullptr;
  if (OB_ISNULL(shared_info = reinterpret_cast<ObWFParticipatorSharedInfo *>(
                  alloc.alloc(sizeof(ObWFParticipatorSharedInfo))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  } else {
    wf_participator_shared_info_ = reinterpret_cast<int64_t>(shared_info);
    shared_info->sqc_thread_count_ = task_cnt;
    shared_info->process_cnt_ = 0;
    shared_info->ret_ = OB_SUCCESS;
    new (&shared_info->cond_)common::SimpleCond(common::ObWaitEventIds::SQL_WF_PARTICIPATOR_COND_WAIT);
    new (&shared_info->lock_)ObSpinLock(common::ObLatchIds::SQL_WF_PARTICIPATOR_COND_LOCK);
  }
  return ret;
}

int ObWindowFunctionVecOpInput::sync_wait(ObExecContext &ctx, ObReportingWFWholeMsg::WholeMsgProvider *msg_provider)
{
  int ret = OB_SUCCESS;
  ObWFParticipatorSharedInfo *shared_info =
    reinterpret_cast<ObWFParticipatorSharedInfo *>(wf_participator_shared_info_);
  if (OB_ISNULL(shared_info) || OB_ISNULL(msg_provider)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null arguments", K(ret), K(shared_info), K(msg_provider));
  } else {
    int64_t exit_cnt = shared_info->sqc_thread_count_;
    int64_t &sync_cnt = shared_info->process_cnt_;
    bool synced_once = false;
    int64_t loop = 0;
    while (OB_SUCC(ret)) {
      ++loop;
      if (!synced_once) {
        ObSpinLockGuard guard(shared_info->lock_);
        synced_once = true;
        if (0 == ATOMIC_AAF(&sync_cnt, 1) % exit_cnt) {
          shared_info->cond_.signal();
          LOG_DEBUG("debug sync_cnt", K(ret), K(sync_cnt), K(lbt()));
          break;
        }
      }
      int tmp_ret = ATOMIC_LOAD(&shared_info->ret_);
      if (OB_FAIL(tmp_ret)) {
        ret = tmp_ret;
      } else if (0 == loop % 8 && OB_UNLIKELY(IS_INTERRUPTED())) {
        ObInterruptCode code = GET_INTERRUPT_CODE();
        ret = code.code_; // overwrite ret
        LOG_WARN("received a interrupt", K(code), K(ret));
      } else if (0 == loop % 16 && OB_FAIL(ctx.fast_check_status())) {
        LOG_WARN("failed to check status", K(ret));
      } else if (0 == ATOMIC_LOAD(&sync_cnt) % exit_cnt) {
        LOG_DEBUG("debug sunc_cnt", K(ret), K(sync_cnt), K(loop), K(exit_cnt), K(lbt()));
        break;
      } else {
        uint32_t key = shared_info->cond_.get_key();
        shared_info->cond_.wait(key, 1000); // wait for 1000 us per loop
      }
    } // end while
  }
  return ret;
}

} // end sql
} // end oceanbase
