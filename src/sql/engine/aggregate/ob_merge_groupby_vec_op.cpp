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

#include "sql/engine/aggregate/ob_merge_groupby_vec_op.h"
#include "sql/engine/px/ob_px_sqc_handler.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

OB_SERIALIZE_MEMBER((ObMergeGroupByVecSpec, ObGroupBySpec),
                    group_exprs_,
                    rollup_exprs_,
                    is_duplicate_rollup_expr_,
                    has_rollup_,
                    distinct_exprs_,
                    is_parallel_,
                    rollup_status_,
                    rollup_id_expr_,
                    sort_exprs_,
                    sort_collations_,
                    sort_cmp_funcs_,
                    enable_encode_sort_,
                    est_rows_per_group_,
                    enable_hash_base_distinct_
);

DEF_TO_STRING(ObMergeGroupByVecSpec)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_NAME("groupbyvec_spec");
  J_COLON();
  pos += ObGroupBySpec::to_string(buf + pos, buf_len - pos);
  J_COMMA();
  J_KV(K_(group_exprs), K_(rollup_exprs), K_(is_duplicate_rollup_expr), K_(has_rollup));
  J_OBJ_END();
  return pos;
}

int ObMergeGroupByVecSpec::add_group_expr(ObExpr *expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret));
  } else if (OB_FAIL(group_exprs_.push_back(expr))) {
    LOG_WARN("failed to push_back expr");
  }
  return ret;
}

int ObMergeGroupByVecSpec::add_rollup_expr(ObExpr *expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret));
  } else if (OB_FAIL(rollup_exprs_.push_back(expr))) {
    LOG_WARN("failed to push_back expr");
  }
  return ret;
}

int ObMergeGroupByVecSpec::register_to_datahub(ObExecContext &ctx) const
{
  int ret = OB_SUCCESS;
  if (is_parallel_) {
    if (OB_ISNULL(ctx.get_sqc_handler())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null unexpected", K(ret));
    } else {
      void *buf = ctx.get_allocator().alloc(sizeof(ObRollupKeyWholeMsg::WholeMsgProvider));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        ObRollupKeyWholeMsg::WholeMsgProvider *provider =
          new (buf) ObRollupKeyWholeMsg::WholeMsgProvider();
        ObSqcCtx &sqc_ctx = ctx.get_sqc_handler()->get_sqc_ctx();
        if (OB_FAIL(
              sqc_ctx.add_whole_msg_provider(get_id(), dtl::DH_ROLLUP_KEY_WHOLE_MSG, *provider))) {
          LOG_WARN("fail add whole msg provider", K(ret));
        }
      }
    }
  }
  return ret;
}

#define IS_VALID_DIFF_ROWID(id) (diff_row_idx_list_[id] != UINT32_MAX)

int ObRowsGroupProcessor::init(ObEvalCtx &eval_ctx, const ObIArray<ObExpr *> &group_exprs)
{
  int ret = OB_SUCCESS;
  groupby_exprs_ = &group_exprs;
  group_expr_cnt_ = group_exprs.count();
  group_vectors_.reuse();
  diff_row_idx_list_.reuse();
  if (OB_FAIL(group_vectors_.reserve(group_expr_cnt_))) {
    LOG_WARN("failed to init", K(ret), K(group_expr_cnt_));
  } else if (OB_FAIL(diff_row_idx_list_.reserve(group_expr_cnt_))) {
    LOG_WARN("failed to init", K(ret), K(group_expr_cnt_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < group_expr_cnt_; ++i) {
      ObExpr *expr = groupby_exprs_->at(i);
      if (OB_FAIL(group_vectors_.push_back(expr->get_vector(eval_ctx)))) {
        LOG_WARN("group_vectors_ push back failed", K(ret));
      }
    }
  }
  return ret;
}

int ObRowsGroupProcessor::prepare_process_next_batch()
{
  int ret = OB_SUCCESS;
  last_group_row_idx_ = -1;
  diff_row_idx_list_.reuse();
  for (int64_t i = 0; OB_SUCC(ret) && i < group_expr_cnt_; ++i) {
    if (OB_FAIL(diff_row_idx_list_.push_back(UINT32_MAX))) {
      LOG_WARN("failed to push idx", K(ret));
    }
  }
  return ret;
}

void ObRowsGroupProcessor::reuse()
{
  last_group_row_idx_ = -1;
  group_vectors_.reuse();
  diff_row_idx_list_.reuse();
}

void ObRowsGroupProcessor::reset()
{
  groupby_exprs_ = nullptr;
  group_expr_cnt_ = 0;
  last_group_row_idx_ = -1;
  group_vectors_.reset();
  diff_row_idx_list_.reset();
}

void ObRowsGroupProcessor::derive_last_group_info(int64_t &diff_group_idx, uint32_t &group_end_idx)
{
  diff_group_idx = 0;
  group_end_idx = diff_row_idx_list_[0];
  for (uint32_t i = 1; i < group_expr_cnt_; ++i) {
    if (IS_VALID_DIFF_ROWID(i)) {
      group_end_idx = std::min(group_end_idx, diff_row_idx_list_[i]);
      if (IS_VALID_DIFF_ROWID(i - 1) && diff_row_idx_list_[i] < diff_row_idx_list_[i - 1]) {
        diff_group_idx = i;
      }
    }
  }
}

int ObRowsGroupProcessor::find_next_new_group(const ObBatchRows &brs,
                                              const LastCompactRow &group_store_row,
                                              bool &found_new_group, uint32_t &group_end_idx,
                                              int64_t &diff_group_idx)
{
  int ret = OB_SUCCESS;
  found_new_group = false;
  diff_group_idx = -1;
  ObLength r_len = 0;
  bool r_null = false;
  const char *r_v = nullptr;
  int64_t start_row_idx = 0;
  int64_t end_row_idx = brs.size_;
  int64_t diff_row_idx = 0;
  int cmp_ret = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < group_expr_cnt_; ++i) {
    if (IS_VALID_DIFF_ROWID(i) && diff_row_idx_list_[i] > last_group_row_idx_) {
      LOG_DEBUG("skip the current column match", K(i), K(diff_row_idx_list_[i]),
                K(last_group_row_idx_));
      continue;
    } else {
      if (IS_VALID_DIFF_ROWID(i)) {
        start_row_idx = diff_row_idx_list_[i] > last_group_row_idx_ ? diff_row_idx_list_[i] + 1 :
                                                                      last_group_row_idx_ + 1;
      } else {
        start_row_idx = (-1 == last_group_row_idx_) ? 0 : last_group_row_idx_ + 1;
      }
      end_row_idx = (i > 0 && IS_VALID_DIFF_ROWID(i - 1) ? diff_row_idx_list_[i - 1] : brs.size_);
      if (start_row_idx >= brs.size_ || end_row_idx - start_row_idx < 0) {
        continue;
      } else if (i > 0 && (0 == end_row_idx - start_row_idx)) {
        found_new_group = true;
        diff_row_idx_list_[i] = end_row_idx;
        continue;
      }
      r_null = group_store_row.is_null(i);
      group_store_row.get_cell_payload(i, r_v, r_len);
      ObExpr *expr = groupby_exprs_->at(i);
      if (OB_FAIL(group_vectors_[i]->null_first_mul_cmp(
            *expr, *brs.skip_,
            EvalBound(brs.size_, start_row_idx, end_row_idx, brs.all_rows_active_), r_null, r_v,
            r_len, diff_row_idx, cmp_ret))) {
        LOG_WARN("compare failed", K(ret));
      } else {
        if ((0 != cmp_ret && diff_row_idx != end_row_idx) || diff_row_idx != brs.size_) {
          /* Why is cmp_ret equal to 0 and diff_row_idx not equal to brs.size_? Need to set
          *  found_new_group to true
          *                              [1, 2, 3]
          *                              [1, 2, 3] -> first group
          *                              [1, 3, 4]
          *                              [1, 3, 4] -> second group
          * first column diff row idx -> [2, 3, 4]
          */
          found_new_group = true;
        } else {
          diff_row_idx = end_row_idx;
        }
        diff_row_idx_list_[i] = diff_row_idx;
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (found_new_group) {
    derive_last_group_info(diff_group_idx, group_end_idx);
    last_group_row_idx_ = group_end_idx;
  } else {
    diff_group_idx = group_expr_cnt_ - 1;
    group_end_idx = brs.size_;
    last_group_row_idx_ = brs.size_;
  }
  return ret;
}

#undef IS_VALID_DIFF_ROWID

int ObMergeGroupByVecOp::init_mem_context()
{
  int ret = OB_SUCCESS;
  if (mem_context_ == nullptr) {
    void *buf = nullptr;
    lib::ContextParam param;
    param.set_mem_attr(ctx_.get_my_session()->get_effective_tenant_id(), ObModIds::OB_SQL_AGGR_FUNC,
                       ObCtxIds::WORK_AREA);
    if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(mem_context_, param))) {
      LOG_WARN("memory entity create failed", K(ret));
    } else if (OB_ISNULL(mem_context_)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "mem entity is null", K(ret));
    } else if (OB_ISNULL(buf = mem_context_->allocp(sizeof(ObArenaAllocator)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_ENG_LOG(WARN, "failed to allocate memory", K(ret));
    } else {
      arena_alloc_ = new (buf) ObArenaAllocator(mem_context_->get_malloc_allocator());
      arena_alloc_->set_label("VecGroupRows");
    }
  }
  return ret;
}

void ObMergeGroupByVecOp::reset()
{
  is_end_ = false;
  cur_output_group_id_ = OB_INVALID_INDEX;
  first_output_group_id_ = 0;
  last_child_output_.reset();
  cur_group_rowid_ = common::OB_INVALID_INDEX;
  output_queue_cnt_ = 0;
  for (int64_t i = 0; i < output_groupby_rows_.count(); i++) {
    if (OB_NOT_NULL(output_groupby_rows_.at(i))) {
      output_groupby_rows_.at(i)->reset();
    }
  }
  brs_holder_.reset();
  output_groupby_rows_.reset();
  cur_aggr_row_ = nullptr;
  cur_group_store_row_ = nullptr;
  is_group_first_calc_ = true;
  cur_group_last_row_idx_ = -1;
  first_batch_from_sort_ = false;
  partial_rollup_idx_ = INT64_MAX;
  cur_grouping_id_ = INT64_MAX;
  use_sort_data_ = false;
  inner_sort_.reset();
  inner_sort_exprs_.reset();
  global_rollup_key_.reset();
  group_processor_.reuse();
  group_rows_.reuse();
  rollup_context_.reset();
  if (OB_NOT_NULL(arena_alloc_)) {
    arena_alloc_->reset();
  }
}

int ObMergeGroupByVecOp::init_rollup_distributor()
{
  int ret = OB_SUCCESS;
  if (ObRollupStatus::ROLLUP_DISTRIBUTOR == MY_SPEC.rollup_status_) {
    // init hyperloglog calculator to calculate ndv
    char *buf = (char *)ctx_.get_allocator().alloc(sizeof(ObHyperLogLogCalculator)
                                                   * (MY_SPEC.rollup_exprs_.count() + 1));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret));
    } else if (OB_ISNULL(output_rollup_ids_ =
                           static_cast<void **>(mem_context_->get_malloc_allocator().alloc(
                             sizeof(void *) * MY_SPEC.max_batch_size_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_ENG_LOG(WARN, "allocate memory failed", K(ret));
    } else {
      // init ndv calculator
      ndv_calculator_ = reinterpret_cast<ObHyperLogLogCalculator *>(buf);
      for (int64_t i = 0; i < MY_SPEC.rollup_exprs_.count() + 1 && OB_SUCC(ret); ++i) {
        new (&ndv_calculator_[i]) ObHyperLogLogCalculator();
        if (OB_FAIL(ndv_calculator_[i].init(&ctx_.get_allocator(), N_HYPERLOGLOG_BIT))) {
          LOG_WARN("failed to initialize ndv calculator", K(ret));
        }
      }

      // init sort
      if (OB_FAIL(ret)) {
      } else if (0 == all_groupby_exprs_.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected status: all_groupby_exprs is empty", K(ret));
      } else if (OB_FAIL((append(inner_sort_exprs_, MY_SPEC.sort_exprs_)))) {
        LOG_WARN("failed to append exprs", K(ret));
      } else {
        for (int64_t i = 0; i < child_->get_spec().output_.count() && OB_SUCC(ret); ++i) {
          ObExpr *expr = child_->get_spec().output_.at(i);
          if (!is_contain(inner_sort_exprs_, expr)) {
            if (OB_FAIL(inner_sort_exprs_.push_back(expr))) {
              LOG_WARN("failed to push back expr", K(ret));
            }
          }
        }
      }
      int64_t row_count = child_->get_spec().rows_;
      ObSortVecOpContext context;
      context.tenant_id_ = ctx_.get_my_session()->get_effective_tenant_id();
      context.sk_exprs_ = &inner_sort_exprs_;
      context.sk_collations_ = &MY_SPEC.sort_collations_;
      context.enable_encode_sortkey_ = MY_SPEC.enable_encode_sort_;
      context.eval_ctx_ = &eval_ctx_;
      context.exec_ctx_ = &ctx_;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(inner_sort_.init(context))) {
        LOG_WARN("failed to init sort", K(ret));
      } else if (OB_FAIL(ObPxEstimateSizeUtil::get_px_size(&ctx_, MY_SPEC.px_est_size_factor_,
                                                           row_count, row_count))) {
        LOG_WARN("failed to get px size", K(ret));
      } else {
        inner_sort_.set_input_rows(row_count);
        inner_sort_.set_input_width(MY_SPEC.width_);
        inner_sort_.set_operator_type(MY_SPEC.type_);
        inner_sort_.set_operator_id(MY_SPEC.id_);
        inner_sort_.set_io_event_observer(&io_event_observer_);
      }

      // init hash values
      if (OB_SUCC(ret)) {
        int64_t max_size = MY_SPEC.max_batch_size_;
        int64_t rollup_hash_vals_pos = 0;
        int64_t sort_batch_skip_pos = rollup_hash_vals_pos + sizeof(uint64_t) * max_size;
        int64_t max_mem_size = sort_batch_skip_pos + ObBitVector::memory_size(max_size);
        char *buf = (char *)ctx_.get_allocator().alloc(max_mem_size);
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory", K(ret), K(max_size));
        } else {
          MEMSET(buf, 0, max_mem_size);
          rollup_hash_vals_ = reinterpret_cast<uint64_t *>(buf);
          sort_batch_rows_.skip_ = to_bit_vector(buf + sort_batch_skip_pos);
        }
      }
    }
  }
  return ret;
}

int ObMergeGroupByVecOp::init_one_group(const int64_t group_id, bool fill_pos /* false */)
{
  int ret = OB_SUCCESS;
  ObCompactRow *aggr_row = nullptr;
  if (OB_FAIL(aggr_processor_.init_one_aggr_row(agg_row_meta_, aggr_row,
                                                mem_context_->get_malloc_allocator(), group_id))) {
    LOG_WARN("init scalar aggregate row failed", K(ret));
  } else if (fill_pos) {
    if (group_rows_.count() <= group_id) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to fill group id", K(ret), K(group_id), K(group_rows_.count()));
    } else {
      group_rows_.at(group_id) = aggr_row;
    }
  } else if (OB_FAIL(group_rows_.push_back(aggr_row))) {
    LOG_WARN("push_back failed", K(group_id), K(aggr_row), K(ret));
  } else {
    LOG_DEBUG("succ init aggr_row", K(group_id), KPC(aggr_row), K(ret));
  }
  return ret;
}

// it's need call for rescan
int ObMergeGroupByVecOp::init_group_rows(const int64_t row_count)
{
  int ret = OB_SUCCESS;
  cur_group_rowid_ = 0;
  cur_output_group_id_ = 0;
  if (MY_SPEC.has_rollup_) {
    cur_group_rowid_ = all_groupby_exprs_.count();
    cur_output_group_id_ = all_groupby_exprs_.count();
  }
  if (OB_FAIL(group_rows_.reserve(row_count))) {
    LOG_WARN("failed to reserve", "cnt", row_count, K(ret));
  } else if (OB_FAIL(aggr_processor_.init_aggr_row_meta(agg_row_meta_))) {
    LOG_WARN("failed to init aggregate row meta", K(ret));
  } else if (aggr_processor_.has_distinct()) {
    const int64_t hp_infras_cnt = row_count <= 0 ? 1 : row_count;
    const int64_t distinct_cnt = aggr_processor_.get_distinct_count();
    if (OB_FAIL(hp_infras_mgr_.reserve_hp_infras(hp_infras_cnt * distinct_cnt))) {
      LOG_WARN("failed to init hp infras group", K(ret));
    }
  }
  for (int64_t i = group_rows_.count(); OB_SUCC(ret) && i < row_count; ++i) {
    if (OB_FAIL(init_one_group(i))) {
      LOG_WARN("failed to init one group", K(i), K(row_count), K(ret));
    }
  }
  return ret;
}

int ObMergeGroupByVecOp::init_group_row_meta()
{
  int ret = OB_SUCCESS;
  group_row_meta_.reset();
  if (OB_FAIL(group_row_meta_.init(
        all_groupby_exprs_,
        ROLLUP_DISTRIBUTOR == MY_SPEC.rollup_status_ ? ROLLUP_BASE_ROW_EXTRA_SIZE : 0))) {
    LOG_WARN("failed to init row meta", K(ret));
  }
  return ret;
}

int ObMergeGroupByVecOp::init_3stage_info()
{
  int ret = OB_SUCCESS;
  if (ObThreeStageAggrStage::NONE_STAGE != MY_SPEC.aggr_stage_) {
    if (OB_ISNULL(MY_SPEC.aggr_code_expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: aggr_code_expr is null in three stage aggregation", K(ret));
    } else if (ObThreeStageAggrStage::FIRST_STAGE != MY_SPEC.aggr_stage_
               && 0 == MY_SPEC.dist_aggr_group_idxes_.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: distinct aggregation group is 0", K(ret), K(MY_SPEC.aggr_stage_),
               K(MY_SPEC.dist_aggr_group_idxes_.count()));
    } else {
      int64_t idx = -1;
      for (int64_t i = 0; i < MY_SPEC.distinct_exprs_.count() && OB_SUCC(ret); ++i) {
        if (has_exist_in_array(child_->get_spec().output_, MY_SPEC.distinct_exprs_.at(i), &idx)) {
          OZ(distinct_col_idx_in_output_.push_back(idx));
        } else {
          // is_const
          distinct_col_idx_in_output_.push_back(-1);
          LOG_DEBUG("distinct expr is const and is not in the output of child", K(i), K(ret));
        }
      }
      LOG_DEBUG("debug distinct exprs", K(ret), K(MY_SPEC.distinct_exprs_.count()));
    }
  }
  return ret;
}

int ObMergeGroupByVecOp::rollup_init()
{
  int ret = OB_SUCCESS;
  if (MY_SPEC.has_rollup_) {
    init_group_cnt_ = MY_SPEC.group_exprs_.count() + MY_SPEC.rollup_exprs_.count();
    // for vectorization, from 0 and col_count - 1 are the rollup group row
    //                      and the col_col is the last group row
    // it's not init firstly
    if (OB_FAIL(append(all_groupby_exprs_, MY_SPEC.group_exprs_))) {
      LOG_WARN("failed to append group exprs", K(ret));
    } else if (OB_FAIL(append(all_groupby_exprs_, MY_SPEC.rollup_exprs_))) {
      LOG_WARN("failed to append group exprs", K(ret));
    } else if (OB_FAIL(init_rollup_distributor())) {
      LOG_WARN("failed to init rollup distributor", K(ret));
    } else {
      // prepare initial group
      for (int64_t i = 0; !has_dup_group_expr_ && i < MY_SPEC.is_duplicate_rollup_expr_.count(); ++i) {
        has_dup_group_expr_ = MY_SPEC.is_duplicate_rollup_expr_.at(i);
      }
      aggr_processor_.set_op_eval_infos(&eval_infos_);
      rollup_context_.set_partial_rollup_idx(MY_SPEC.group_exprs_.count(),
                                             all_groupby_exprs_.count());
      aggr_processor_.set_has_rollup();
      aggr_processor_.set_rollup_ctx(&rollup_context_);
    }
  }
  return ret;
}

// only init in inner_open
int ObMergeGroupByVecOp::gby_init()
{
  int ret = OB_SUCCESS;
  if (!MY_SPEC.has_rollup_) {
    init_group_cnt_ = 1;
    all_groupby_exprs_.reset();
    if (OB_FAIL(append(all_groupby_exprs_, MY_SPEC.group_exprs_))) {
      LOG_WARN("failed to append group exprs", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(output_stored_rows_ = static_cast<const ObCompactRow **>(
              mem_context_->get_malloc_allocator().alloc(sizeof(ObCompactRow *) *
                                                        MY_SPEC.max_batch_size_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SQL_ENG_LOG(WARN, "allocate memory failed", K(ret));
  }
  return ret;
}

int ObMergeGroupByVecOp::init()
{
  int ret = OB_SUCCESS;
  group_rows_.set_tenant_id(MTL_ID());
  group_rows_.set_ctx_id(ObCtxIds::DEFAULT_CTX_ID);
  if (OB_FAIL(ObChunkStoreUtil::alloc_dir_id(ctx_.get_my_session()->get_effective_tenant_id(), dir_id_))) {
    LOG_WARN("failed to alloc dir id", K(ret));
  } else if (FALSE_IT(aggr_processor_.set_dir_id(dir_id_))) {
  } else if (FALSE_IT(aggr_processor_.set_io_event_observer(&io_event_observer_))) {
  } else if (OB_FAIL(init_mem_context())) {
    LOG_WARN("init memory context failed", K(ret));
  } else if (OB_FAIL(brs_holder_.init(child_->get_spec().output_, eval_ctx_))) {
    LOG_WARN("failed to initialize brs_holder_", K(ret));
  } else if (OB_FAIL(gby_init())) {
    LOG_WARN("failed to init", K(ret));
  } else if (OB_FAIL(rollup_init())) {
    LOG_WARN("failed to init rollup", K(ret));
  } else if (OB_FAIL(init_3stage_info())) {
    LOG_WARN("failed to init 3stage info", K(ret));
  } else if (OB_FAIL(init_hp_infras_group_mgr())) {
    LOG_WARN("failed to init hp infras group manager", K(ret));
  } else if (OB_FAIL(init_group_rows(init_group_cnt_))) {
    LOG_WARN("failed to init group rows", K(ret), K(init_group_cnt_));
  } else if (OB_FAIL(init_group_row_meta())) {
    LOG_WARN("failed to init fixed group row meta", K(ret));
  } else if (OB_FAIL(group_processor_.init(eval_ctx_, all_groupby_exprs_))) {
    LOG_WARN("failed to init group processor", K(ret));
  } else {
    if (aggr_processor_.has_extra()) {
      // set group_batch_factor_ to 1 avoid out of memory error
      group_batch_factor_ = 1;
    }
    last_child_output_.reuse_ = true;
  }
  return ret;
}

int ObMergeGroupByVecOp::inner_open()
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_FAIL(ObGroupByVecOp::inner_open())) {
    LOG_WARN("failed to inner_open", K(ret));
  } else if (OB_FAIL(init())) {
    LOG_WARN("failed to init", K(ret));
  }
  return ret;
}

int ObMergeGroupByVecOp::inner_close()
{
  reset();
  sql_mem_processor_.unregister_profile();
  return ObGroupByVecOp::inner_close();
}

void ObMergeGroupByVecOp::destroy()
{
  sql_mem_processor_.unregister_profile_if_necessary();
  all_groupby_exprs_.reset();
  distinct_col_idx_in_output_.reset();
  inner_sort_exprs_.reset();
  reset();
  arena_alloc_ = nullptr;
  group_rows_.reset();
  agg_row_meta_.reset();
  group_row_meta_.reset();
  group_processor_.reset();
  hp_infras_mgr_.destroy();
  ObGroupByVecOp::destroy();
  if (nullptr != mem_context_) {
    if (nullptr != output_stored_rows_) {
      mem_context_->get_malloc_allocator().free(output_stored_rows_);
      output_stored_rows_ = nullptr;
    }
    if (nullptr != output_rollup_ids_) {
      mem_context_->get_malloc_allocator().free(output_rollup_ids_);
      output_rollup_ids_ = nullptr;
    }
    DESTROY_CONTEXT(mem_context_);
    mem_context_ = nullptr;
  }
}

int ObMergeGroupByVecOp::inner_switch_iterator()
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_FAIL(ObGroupByVecOp::inner_switch_iterator())) {
    LOG_WARN("failed to switch_iterator", K(ret));
  } else if (OB_FAIL(init_group_rows(init_group_cnt_))) {
    LOG_WARN("failed to init group rows", K(ret));
  } else if (OB_FAIL(group_processor_.init(eval_ctx_, all_groupby_exprs_))) {
    LOG_WARN("failed to init group processor", K(ret));
  }
  return ret;
}

int ObMergeGroupByVecOp::inner_rescan()
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_FAIL(ObGroupByVecOp::inner_rescan())) {
    LOG_WARN("failed to rescan", K(ret));
  } else if (OB_FAIL(init_group_rows(init_group_cnt_))) {
    LOG_WARN("failed to init group rows", K(ret));
  } else if (OB_FAIL(group_processor_.init(eval_ctx_, all_groupby_exprs_))) {
    LOG_WARN("failed to init group processor", K(ret));
  }
  return ret;
}

int ObMergeGroupByVecOp::find_candidate_key(ObRollupNDVInfo &ndv_info)
{
  int ret = OB_SUCCESS;
  int64_t n_group = MY_SPEC.group_exprs_.count();
  uint64_t candidate_ndv = 0;
  ObPxSqcHandler *sqc_handle = ctx_.get_sqc_handler();
  ndv_info.dop_ = 1;
  // ndv_info.max_keys_ = 0;
  // TODO: Three stage can't process rollup level
  ndv_info.max_keys_ =
    ObThreeStageAggrStage::SECOND_STAGE == MY_SPEC.aggr_stage_ ? all_groupby_exprs_.count() : 0;
  if (OB_NOT_NULL(sqc_handle)) {
    ObPxRpcInitSqcArgs &sqc_args = sqc_handle->get_sqc_init_arg();
    ndv_info.dop_ = sqc_args.sqc_.get_total_task_count();
  }
  for (int64_t i = 0; i < MY_SPEC.rollup_exprs_.count() + 1 && OB_SUCC(ret); ++i) {
    if (0 == n_group && i == MY_SPEC.rollup_exprs_.count()) {
      break;
    }
    candidate_ndv = ndv_calculator_[i].estimate();
    if (candidate_ndv >= ObRollupKeyPieceMsgCtx::FAR_GREATER_THAN_RATIO * ndv_info.dop_) {
      ndv_info.ndv_ = candidate_ndv;
      ndv_info.n_keys_ = 0 == n_group ? i + 1 : i + n_group;
      break;
    }
  }
  if (0 == ndv_info.n_keys_) {
    // can't found, use all groupby keys
    ndv_info.ndv_ = candidate_ndv;
    ndv_info.n_keys_ = all_groupby_exprs_.count();
  }
  return ret;
}

int ObMergeGroupByVecOp::process_parallel_rollup_key(ObRollupNDVInfo &ndv_info)
{
  int ret = OB_SUCCESS;
  ObRollupKeyWholeMsg whole_msg;
  const ObRollupKeyWholeMsg *temp_whole_msg = NULL;
  ObPxSqcHandler *handler = ctx_.get_sqc_handler();
  if (OB_ISNULL(handler)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("parallel merge groupby only supported in parallel execution mode",
             K(MY_SPEC.is_parallel_));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "parallel winbuf in non-px mode");
  } else {
    ObPxSQCProxy &proxy = handler->get_sqc_proxy();
    ObRollupKeyPieceMsg piece;
    piece.op_id_ = MY_SPEC.id_;
    piece.thread_id_ = GETTID();
    piece.source_dfo_id_ = proxy.get_dfo_id();
    piece.target_dfo_id_ = proxy.get_dfo_id();
    piece.rollup_ndv_ = ndv_info;
    if (OB_FAIL(proxy.get_dh_msg_sync(MY_SPEC.id_, dtl::DH_ROLLUP_KEY_WHOLE_MSG, piece,
                                      temp_whole_msg,
                                      ctx_.get_physical_plan_ctx()->get_timeout_timestamp()))) {
      LOG_WARN("fail get rollup key msg", K(ret));
    } else if (OB_ISNULL(temp_whole_msg)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("whole msg is unexpected", K(ret));
    } else if (OB_FAIL(whole_msg.assign(*temp_whole_msg))) {
      LOG_WARN("fail to assign msg", K(ret));
    } else {
      global_rollup_key_ = whole_msg.rollup_ndv_;
      if (global_rollup_key_.n_keys_ > MY_SPEC.group_exprs_.count()) {
        partial_rollup_idx_ = global_rollup_key_.n_keys_;
        if (global_rollup_key_.n_keys_ > all_groupby_exprs_.count()) {
          LOG_ERROR("unexpected number of partial rollup keys", K(global_rollup_key_.n_keys_));
          global_rollup_key_.n_keys_ = all_groupby_exprs_.count();
          partial_rollup_idx_ = all_groupby_exprs_.count();
        }
      } else {
        partial_rollup_idx_ = MY_SPEC.group_exprs_.count();
      }
      rollup_context_.set_partial_rollup_idx(MY_SPEC.group_exprs_.count(), partial_rollup_idx_);
    }
    LOG_DEBUG("debug partial rollup keys", K(partial_rollup_idx_));
  }
  return ret;
}

int ObMergeGroupByVecOp::get_child_next_batch_row(const int64_t max_row_cnt,
                                                  const ObBatchRows *&batch_rows)
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  if (use_sort_data_) {
    int64_t read_rows = 0;
    batch_rows = &sort_batch_rows_;
    if (OB_FAIL(inner_sort_.get_next_batch(max_row_cnt, read_rows))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        const_cast<ObBatchRows *>(batch_rows)->size_ = 0;
        const_cast<ObBatchRows *>(batch_rows)->end_ = true;
        LOG_DEBUG("debug to get sorted row", K(ret), K(max_row_cnt),
                  K(const_cast<ObBatchRows *>(batch_rows)->size_), K(ret));
      } else {
        LOG_WARN("failed to get sorted row", K(ret));
      }
    } else if (aggr_processor_.get_need_advance_collect()
               && OB_FAIL(brs_holder_.save(MY_SPEC.max_batch_size_))) {
      LOG_WARN("failed to backup child exprs", K(ret));
    } else {
      const_cast<ObBatchRows *>(batch_rows)->size_ = read_rows;
      const_cast<ObBatchRows *>(batch_rows)->end_ = false;
      if (first_batch_from_sort_) {
        int64_t max_size = MY_SPEC.max_batch_size_;
        const_cast<ObBatchRows *>(batch_rows)->skip_->reset(max_size);
        first_batch_from_sort_ = false;
      } else {
        // if has rollup, then don't duplicate data in get_next_batch/row
        // use unique_sort_op_ to duplicate data
        // so skip_ don'e reset [ batch_rows->skip_->reset(max_row_cnt); ]
      }
      LOG_DEBUG("debug to get sorted row", K(ret), K(max_row_cnt),
                K(const_cast<ObBatchRows *>(batch_rows)->size_));
    }
  } else {
    if (OB_FAIL(child_->get_next_batch(max_row_cnt, batch_rows))) {
      LOG_WARN("failed to get child row", K(ret));
    } else if (aggr_processor_.get_need_advance_collect()
               && OB_FAIL(brs_holder_.save(MY_SPEC.max_batch_size_))) {
      LOG_WARN("failed to backup child exprs", K(ret));
    }
  }
  return ret;
}

void ObMergeGroupByVecOp::sets(ObHyperLogLogCalculator &ndv_calculator, uint64_t *hash_vals,
                               ObBitVector *skip, int64_t count)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < count; i++) {
    if (OB_NOT_NULL(skip) && skip->at(i)) {
      continue;
    }
    ndv_calculator.set(hash_vals[i]);
  }
}

int ObMergeGroupByVecOp::batch_collect_local_ndvs(const ObBatchRows *child_brs)
{
  int ret = OB_SUCCESS;
  int64_t n_group = MY_SPEC.group_exprs_.count();
  // same as hash groupby
  uint64_t hash_value_seed = 99194853094755497L;
  ObDatum *datum = nullptr;
  for (int64_t i = 0; i < all_groupby_exprs_.count() && OB_SUCC(ret); ++i) {
    ObExpr *expr = all_groupby_exprs_.at(i);
    if (OB_FAIL(expr->eval_vector(eval_ctx_, *child_brs))) {
      LOG_WARN("failed to eval expr", K(ret));
    } else {
      bool is_batch_seed = (0 != i);
      ObIVector *vec = expr->get_vector(eval_ctx_);
      if (OB_FAIL(vec->murmur_hash_v3(*expr, rollup_hash_vals_, *child_brs->skip_,
                                      EvalBound(child_brs->size_, child_brs->all_rows_active_),
                                      is_batch_seed ? rollup_hash_vals_ : &hash_value_seed,
                                      is_batch_seed))) {
        SQL_ENG_LOG(WARN, "failed to calc hash value", K(ret));
      }
      if (OB_FAIL(ret)) {
      } else if ((0 < n_group && i == n_group - 1) || i >= n_group) {
        if (0 < n_group) {
          sets(ndv_calculator_[i - n_group + 1], rollup_hash_vals_, child_brs->skip_,
               child_brs->size_);
        } else {
          sets(ndv_calculator_[i - n_group], rollup_hash_vals_, child_brs->skip_, child_brs->size_);
        }
      }
    }
  }
  LOG_DEBUG("debug batch collect local ndvs", K(ret));
  return ret;
}

int ObMergeGroupByVecOp::batch_process_rollup_distributor(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  if (!use_sort_data_ && MY_SPEC.is_parallel_) {
    bool need_dump = false;
    int64_t child_batch_cnt = common::max(max_row_cnt, MY_SPEC.max_batch_size_);
    const ObBatchRows *child_brs = nullptr;
    // 1. get all data and calculate ndv and sort
    while (OB_SUCC(ret)) {
      clear_evaluated_flag();
      if (OB_FAIL(child_->get_next_batch(child_batch_cnt, child_brs))) {
        if (OB_ITER_END == ret) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected status: return error code iter_end", K(ret));
        }
        LOG_WARN("failed to get child batch", K(ret));
      } else if (child_brs->end_ && child_brs->size_ == 0) {
        LOG_DEBUG("reach iterating end with empty result, do nothing");
        break;
      } else if (OB_FAIL(try_check_status())) {
        LOG_WARN("check status failed", K(ret));
      } else if (OB_FAIL(batch_collect_local_ndvs(child_brs))) {
        LOG_WARN("failed to calculate ndvs", K(ret));
      } else if (OB_FAIL(inner_sort_.add_batch(*child_brs, need_dump))) {
        LOG_WARN("failed to add row", K(ret));
      }
    }
    // set true and get data from inner_sort_
    // 2. wait QC to get the distribution keys
    use_sort_data_ = true;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(inner_sort_.sort())) {
      LOG_WARN("failed to sort rows", K(ret));
    } else if (OB_FAIL(find_candidate_key(global_rollup_key_))) {
      LOG_WARN("failed to find candidate key", K(ret));
    } else if (OB_FAIL(process_parallel_rollup_key(global_rollup_key_))) {
      LOG_WARN("failed to process parallel", K(ret));
    } else {
      clear_evaluated_flag();
      LOG_DEBUG("debug batch process distributor", K(ret));
    }
  }
  return ret;
}

int ObMergeGroupByVecOp::advance_collect_result(int64_t group_id)
{
  int ret = OB_SUCCESS;
  aggregate::AggrRowPtr aggr_row = nullptr;
  if (OB_FAIL(get_aggr_row(group_id, aggr_row))) {
    LOG_WARN("failed to get aggr row", K(ret), K(group_id));
  } else if (OB_FAIL(
               aggr_processor_.advance_collect_result(group_id, agg_row_meta_, aggr_row))) {
    LOG_WARN("failed to calc and material distinct result", K(ret), K(group_id));
  } /*else if (OB_FAIL(brs_holder_.restore())) {
    LOG_WARN("failed to restore child exprs", K(ret));
  }*/
  // 按现在的优化分析下来，这里不需要restore了
  clear_evaluated_flag();
  return ret;
}

int ObMergeGroupByVecOp::before_process_next_batch(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  set_output_queue_cnt(0);
  if (cur_group_rowid_ > common::OB_INVALID_INDEX && OB_FAIL(brs_holder_.restore())) {
    LOG_WARN("failed to restore previous exprs", K(ret));
  } else if (ObRollupStatus::ROLLUP_DISTRIBUTOR == MY_SPEC.rollup_status_
             && OB_FAIL(batch_process_rollup_distributor(max_row_cnt))) {
    LOG_WARN("failed to process rollup distributor", K(ret));
  }
  return ret;
}

int ObMergeGroupByVecOp::inner_get_next_batch(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  int64_t output_batch_cnt = common::min(max_row_cnt, MY_SPEC.max_batch_size_);
  int64_t child_batch_cnt = common::max(max_row_cnt, MY_SPEC.max_batch_size_);
  const ObBatchRows *child_brs = nullptr;
  LOG_DEBUG("before inner_get_next_batch", "aggr_hold_size", aggr_processor_.get_aggr_hold_size(),
            "aggr_used_size", aggr_processor_.get_aggr_used_size(), K(output_batch_cnt),
            K(max_row_cnt));

  if (is_output_queue_not_empty()) {
    // consume aggr results generated in previous round
    if (OB_FAIL(calc_batch_results(is_end_, output_batch_cnt))) {
      LOG_WARN("failed to calc output results", K(ret));
    }
  } else {
    LOG_DEBUG("begin to get_next_batch rows from child", K(child_batch_cnt));
    if (OB_FAIL(before_process_next_batch(child_batch_cnt))) {
      LOG_WARN("failed to before process next batch", K(child_batch_cnt));
    } else {
      brs_holder_.reset();
      while (OB_SUCC(ret) && OB_SUCC(get_child_next_batch_row(child_batch_cnt, child_brs))) {
        if (child_brs->end_ && child_brs->size_ == 0) {
          LOG_DEBUG("reach iterating end with empty result, do nothing");
          break;
        }
        if (OB_FAIL(try_check_status())) {
          LOG_WARN("check status failed", K(ret));
        } else if (OB_FAIL(groupby_datums_eval_batch(*child_brs))) {
          LOG_WARN("failed to calc_groupby_datums", K(ret));
        } else if (OB_FAIL(process_batch(*child_brs))) {
          LOG_WARN("failed to process_batch_result", K(ret));
        } else if (stop_batch_iterating(*child_brs, output_batch_cnt)) {
          if (!aggr_processor_.get_need_advance_collect()) {
            OZ(brs_holder_.save(std::min(MY_SPEC.max_batch_size_, get_output_queue_cnt())));
          }
          LOG_DEBUG("break out of iteratation", K(child_brs->end_), K(output_batch_cnt),
                    K(output_queue_cnt_));
          break;
        } else { // do nothing
        }
      }

      if (OB_SUCC(ret) && child_brs->end_ && !OB_ISNULL(cur_aggr_row_)) {
        // add last unfinised grouprow into output group
        inc_output_queue_cnt();
        const int64_t advance_collect_group_id = cur_group_rowid_;
        if (MY_SPEC.has_rollup_ && OB_FAIL(process_rollup(MY_SPEC.group_exprs_.count() - 1, true))) {
          LOG_WARN("failed to process rollup", K(ret));
        } else if (aggr_processor_.get_need_advance_collect()
                   && OB_FAIL(advance_collect_result(advance_collect_group_id))) {
          LOG_WARN("failed to collect distinct result", K(ret), K(advance_collect_group_id));
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(calc_batch_results(child_brs->end_, output_batch_cnt))) {
        LOG_WARN("failed to calc output results", K(ret));
      }
    }
  }

  LOG_DEBUG("after inner_get_next_batch", "aggr_hold_size", aggr_processor_.get_aggr_hold_size(),
            "aggr_used_size", aggr_processor_.get_aggr_used_size(), K(output_batch_cnt), K(ret));
  return ret;
}

int ObMergeGroupByVecOp::set_null(int64_t idx, LastCompactRow *rollup_store_row)
{
  int ret = OB_SUCCESS;
  if (0 > idx) {
  } else {
    rollup_store_row->set_null(idx);
    LOG_DEBUG("set null", K(idx), K(MY_SPEC.rollup_exprs_.count()));
    if (has_dup_group_expr_) {
      int64_t start_idx = idx - MY_SPEC.group_exprs_.count();
      ObExpr *base_expr = MY_SPEC.rollup_exprs_.at(start_idx);
      for (int i = start_idx + 1; i < MY_SPEC.rollup_exprs_.count() && OB_SUCC(ret); ++i) {
        if (base_expr == MY_SPEC.rollup_exprs_.at(i)) {
          // set null to the same expr
          rollup_store_row->set_null(i + MY_SPEC.group_exprs_.count());
          LOG_DEBUG("set null", K(i + MY_SPEC.group_exprs_.count()), K(start_idx),
                    K(MY_SPEC.rollup_exprs_.count()));
        }
      }
    }
  }
  return ret;
}

int ObMergeGroupByVecOp::get_rollup_row(int64_t prev_group_row_id, int64_t group_row_id,
                                        aggregate::AggrRowPtrRef curr_aggr_row,
                                        LastCompactRow *&cur_gby_store_row,
                                        aggregate::AggrRowPtrRef prev_aggr_row,
                                        LastCompactRow *&prev_gby_store_row, bool &need_set_null,
                                        int64_t idx)
{
  int ret = OB_SUCCESS;
  curr_aggr_row = nullptr;
  cur_gby_store_row = nullptr;
  prev_aggr_row = nullptr;
  prev_gby_store_row = nullptr;
  need_set_null = false;
  (void) get_aggr_and_group_row(prev_group_row_id, prev_aggr_row, prev_gby_store_row);
  if (OB_ISNULL(prev_aggr_row) || OB_ISNULL(prev_gby_store_row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get prev group row", K(ret), K(prev_group_row_id));
  } else if (group_row_id < get_group_rows_count()) {
    // critical path: reuse grouprow directly no defensive check
    (void) get_aggr_and_group_row(group_row_id, curr_aggr_row, cur_gby_store_row);
  }
  if (OB_FAIL(ret)) {
  } else if (nullptr == curr_aggr_row) {
    if (OB_FAIL(init_one_group(group_row_id, true))) {
      LOG_WARN("failed to init_one_group", K(ret));
    } else if (OB_FAIL(get_aggr_row(group_row_id, curr_aggr_row))) {
      LOG_WARN("failed to get_aggr_row", K(ret));
      // performance critical: use curr_group_row directly, no defensive check
    } else {
      // deep copy from prev group row
      if (OB_FAIL(get_groupby_store_row(group_row_id, cur_gby_store_row))) {
        LOG_WARN("failed to get_groupby_store_row", K(ret));
      } else if (OB_FAIL(cur_gby_store_row->deep_copy_last_compact_row(*prev_gby_store_row))) {
        LOG_WARN("failed to store group row", K(ret));
      } else {
        if (ROLLUP_DISTRIBUTOR == MY_SPEC.rollup_status_) {
          *reinterpret_cast<int64_t *>(cur_gby_store_row->get_extra_payload()) = group_row_id;
        }
        need_set_null = true;
        if (0 <= idx) {
          // if the expr in rollup in group by or the expr exists more than one tiem,
          // then only the first expr need to set null
          // eg: group by c1, rollup(c1,c1)
          // then don't reset null
          OZ(set_null(idx, cur_gby_store_row));
        }
      }
    }
  } else if (cur_gby_store_row->is_empty()) {
    // deep copy from prev group row
    if (OB_FAIL(cur_gby_store_row->deep_copy_last_compact_row(*prev_gby_store_row))) {
      LOG_WARN("failed to store group row", K(ret));
    } else {
      if (ROLLUP_DISTRIBUTOR == MY_SPEC.rollup_status_) {
        *reinterpret_cast<int64_t *>(cur_gby_store_row->get_extra_payload()) = group_row_id;
      }
      need_set_null = true;
      if (0 <= idx) {
        OZ(set_null(idx, cur_gby_store_row));
      }
    }
  }
  return ret;
}

int ObMergeGroupByVecOp::get_empty_rollup_row(int64_t group_row_id,
                                              aggregate::AggrRowPtrRef curr_group_row,
                                              LastCompactRow *&curr_gby_store_row)
{
  int ret = OB_SUCCESS;
  curr_group_row = nullptr;
  curr_gby_store_row = nullptr;
  if (group_row_id < get_group_rows_count()) {
    // critical path: reuse grouprow directly no defensive check
    (void) get_aggr_and_group_row(group_row_id, curr_group_row, curr_gby_store_row);
    if (OB_ISNULL(curr_group_row)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get group row", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (nullptr == curr_group_row) {
    if (OB_FAIL(init_one_group(group_row_id))) {
      LOG_WARN("failed to init_one_group", K(ret));
    } else if (OB_FAIL(get_aggr_row(group_row_id, curr_group_row))) {
      LOG_WARN("failed to get_aggr_row", K(ret));
      // performance critical: use curr_group_row directly, no defensive check
    } else if (OB_FAIL(get_groupby_store_row(group_row_id, curr_gby_store_row))) {
      LOG_WARN("failed to get groupby store row", K(ret), K(group_row_id));
    } else if (OB_ISNULL(curr_gby_store_row)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: store row is null", K(ret));
    } else if (output_groupby_rows_.count() != get_group_rows_count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: store row is null", K(ret));
    }
  }
  return ret;
}

int ObMergeGroupByVecOp::get_grouping_id(const ObBatchRows &brs)
{
  int ret = OB_SUCCESS;
  if (ROLLUP_COLLECTOR == MY_SPEC.rollup_status_) {
    if (OB_FAIL(MY_SPEC.rollup_id_expr_->eval_vector(eval_ctx_, brs))) {
      LOG_WARN("Failed to calculate expression", K(ret));
    } else if (MY_SPEC.rollup_id_expr_->get_vector(eval_ctx_)->is_null(eval_ctx_.get_batch_idx())) {
      cur_grouping_id_ = 0;
    } else {
      cur_grouping_id_ =
        MY_SPEC.rollup_id_expr_->get_vector(eval_ctx_)->get_int(eval_ctx_.get_batch_idx());
    }
  }
  return ret;
}

int ObMergeGroupByVecOp::get_aggr_row(const int64_t group_id, aggregate::AggrRowPtrRef aggr_row)
{
  int ret = OB_SUCCESS;
  aggr_row = nullptr;
  ObCompactRow *row = nullptr;
  if (OB_FAIL(group_rows_.at(group_id, row))) {
    LOG_WARN("failed to get group row", K(group_id));
  } else {
    aggr_row = static_cast<char *>(row->get_extra_payload(agg_row_meta_));
  }
  return ret;
}

int ObMergeGroupByVecOp::get_aggr_and_group_row(const int64_t group_id,
                                                aggregate::AggrRowPtrRef aggr_row,
                                                LastCompactRow *&store_row)
{
  int ret = OB_SUCCESS;
  aggr_row = nullptr;
  store_row = nullptr;
  if (OB_FAIL(get_aggr_row(group_id, aggr_row))) {
    LOG_WARN("failed to get aggr row", K(ret), K(group_id));
  } else if (OB_FAIL(get_groupby_store_row(group_id, store_row))) {
    LOG_WARN("failed to get group by store row", K(ret), K(group_id));
  }
  return ret;
}

inline int ObMergeGroupByVecOp::swap_group_row(const int a, const int b)
{
  int ret = OB_SUCCESS;
  if (a == b) { // do nothing
  } else if (OB_FAIL(aggr_processor_.swap_group_row(a, b))) {
    LOG_WARN("failed to swap group row", K(ret), K(a), K(b));
  } else if (group_rows_.count() > common::max(a, b)) {
    ObCompactRow *groupb = group_rows_.at(b);
    group_rows_.at(b) = group_rows_.at(a);
    group_rows_.at(a) = groupb;
  } else {
    ret = OB_ARRAY_OUT_OF_RANGE;
  }
  return ret;
}

inline int ObMergeGroupByVecOp::create_groupby_store_row(LastCompactRow *&store_row)
{
  int ret = OB_SUCCESS;
  void *buf = arena_alloc_->alloc(sizeof(LastCompactRow));
  if (OB_ISNULL(buf)) {
    LOG_WARN("failed alloc memory", K(ret));
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    store_row = new (buf) LastCompactRow(*arena_alloc_);
    if (OB_FAIL(output_groupby_rows_.push_back(store_row))) {
      LOG_WARN("failed push back", K(ret));
    }
    store_row->reuse_ = true;
  }
  return ret;
}

inline int ObMergeGroupByVecOp::get_groupby_store_row(int i, LastCompactRow *&store_row)
{
  int ret = OB_SUCCESS;
  store_row = nullptr;
  if (i < output_groupby_rows_.count()) {
    store_row = output_groupby_rows_.at(i);
  } else if (i == output_groupby_rows_.count()) {
    if (OB_FAIL(create_groupby_store_row(store_row))) {
      LOG_WARN("failed create groupby store row", K(ret));
    }
  } else {
    for (int64_t cur = output_groupby_rows_.count(); OB_SUCC(ret) && cur <= i; ++cur) {
      if (OB_FAIL(create_groupby_store_row(store_row))) {
        LOG_WARN("failed create groupby store row", K(ret));
      }
    }
  }
  return ret;
}

// In batch mode, extra_size save the grouping_id for Rollup Distributor
// others extra_size is 0
int ObMergeGroupByVecOp::prepare_and_save_curr_groupby_datums(const ObBatchRows &brs,
                                                              int64_t curr_group_rowid,
                                                              ObIArray<ObExpr *> &group_exprs,
                                                              LastCompactRow *&gby_store_row,
                                                              int64_t extra_size)
{
  int ret = OB_SUCCESS;
  gby_store_row = nullptr;
  if (OB_FAIL(get_groupby_store_row(curr_group_rowid, gby_store_row))) {
    LOG_WARN("failed to get_groupby_store_row", K(ret));
  } else if (OB_FAIL(gby_store_row->save_store_row(group_exprs, brs, eval_ctx_,
                                                   group_row_meta_))) {
    LOG_WARN("failed to store group row", K(ret));
  } else if (0 < extra_size) {
    *reinterpret_cast<int64_t *>(gby_store_row->get_extra_payload()) = -partial_rollup_idx_;
  }
  LOG_DEBUG("finish prepare and save groupby store row", K(group_exprs), K(ret),
            K(ROWEXPR2STR(eval_ctx_, group_exprs)));
  return ret;
}

int ObMergeGroupByVecOp::get_cur_group_row(const ObBatchRows &brs, int64_t group_row_id,
                                           aggregate::AggrRowPtrRef curr_aggr_row,
                                           LastCompactRow *&cur_gby_store_row,
                                           ObIArray<ObExpr *> &group_exprs)
{
  int ret = OB_SUCCESS;
  curr_aggr_row = nullptr;
  cur_gby_store_row = nullptr;
  if (group_row_id < get_group_rows_count()) {
    // critical path: reuse grouprow directly no defensive check
    (void)get_aggr_row(group_row_id, curr_aggr_row);
    if (OB_ISNULL(curr_aggr_row)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get group row", K(ret), K(group_row_id));
    } else if (OB_FAIL(prepare_and_save_curr_groupby_datums(
                 brs, cur_group_rowid_, group_exprs, cur_gby_store_row,
                 ROLLUP_DISTRIBUTOR == MY_SPEC.rollup_status_ ? ROLLUP_BASE_ROW_EXTRA_SIZE : 0))) {
      LOG_WARN("failed to prepare group datums", K(ret));
    } else if (OB_FAIL(get_grouping_id(brs))) {
      LOG_WARN("failed to get grouping id", K(ret));
    }
  } else {
    if (OB_FAIL(init_one_group(group_row_id))) {
      LOG_WARN("failed to init_one_group", K(ret));
    } else if (OB_FAIL(get_aggr_row(group_row_id, curr_aggr_row))) {
      LOG_WARN("failed to get_aggr_row", K(ret));
      // performance critical: use curr_aggr_row directly, no defensive check
    } else if (OB_FAIL(prepare_and_save_curr_groupby_datums(
                 brs, cur_group_rowid_, group_exprs, cur_gby_store_row,
                 ROLLUP_DISTRIBUTOR == MY_SPEC.rollup_status_ ? ROLLUP_BASE_ROW_EXTRA_SIZE : 0))) {
      LOG_WARN("failed to eval_aggr_param_batch");
    } else if (OB_FAIL(get_grouping_id(brs))) {
      LOG_WARN("failed to get grouping id", K(ret));
    }
  }
  return ret;
}

int ObMergeGroupByVecOp::set_all_null(int64_t start,
  int64_t end,
  int64_t max_group_idx,
  LastCompactRow *rollup_store_row)
{
  int ret = OB_SUCCESS;
  for (int64_t i = end - 1; i >= start && OB_SUCC(ret); --i) {
    if (!MY_SPEC.is_duplicate_rollup_expr_.at(i - MY_SPEC.group_exprs_.count())) {
      if (OB_FAIL(set_null(i, rollup_store_row))) {
        LOG_WARN("failed to set null", K(ret), K(i));
      }
    }
  }
  return ret;
}

/*
 * generate rollup group row by start_diff_group_idx and cur_rollup_idx
 * eg: count(*)   group by c1, rollup(c2,c3,c4)
 *       c1   c2    c3    c4  count(*) rollup_group_row       output
 *        1    1     1    1
 *        1    1     1    2    ->      (1,1,1,null, 1)          N
 *        1    1     3    2    ->      (1,1,1,null, 2)          Y
 *                                     (1,1,null,null, 2)       N
 *        2    2     3    2    ->      (1,1,3,null, 1)          Y
 *                                     (1,1,null,null, 3)       Y
 *                                     (1,null,null,null, 3)    Y
 *       iter end
 *                                     (2,2,3,null, 1)          Y
 *                                     (2,2,null,null, 1)       Y
 *                                     (2,null,null,null, 1)    Y
 */
int ObMergeGroupByVecOp::gen_rollup_group_rows(int64_t start_diff_group_idx, int64_t end_group_idx,
                                               int64_t max_group_idx, int64_t cur_group_row_id)
{
  int ret = OB_SUCCESS;
  int64_t cur_rollup_group_id = cur_group_row_id;
  int64_t prev_group_row_id = cur_rollup_group_id;
  int64_t cur_rollup_idx = end_group_idx;
  int64_t start_rollup_idx = start_diff_group_idx;
  int64_t null_idx = all_groupby_exprs_.count();
  const int64_t group_exprs_cnt = MY_SPEC.group_exprs_.count();
  aggregate::AggrRowPtr curr_group_row = nullptr;
  aggregate::AggrRowPtr prev_group_row = nullptr;
  LastCompactRow *curr_group_store_row = nullptr;
  LastCompactRow *prev_group_store_row = nullptr;
  bool need_set_null = false;
  for (int64_t idx = cur_rollup_idx; OB_SUCC(ret) && idx >= start_rollup_idx; --idx) {
    cur_rollup_group_id = idx;
    if (idx <= max_group_idx) {
    } else if (OB_FAIL(get_rollup_row(
                 prev_group_row_id, cur_rollup_group_id, curr_group_row, curr_group_store_row,
                 prev_group_row, prev_group_store_row, need_set_null,
                 MY_SPEC.is_duplicate_rollup_expr_.at(idx - group_exprs_cnt) ? -1 : idx))) {
      LOG_WARN("failed to get one new group row", K(ret));
    } else if (idx == end_group_idx && need_set_null
               && end_group_idx < all_groupby_exprs_.count() - 1
               && OB_FAIL(set_all_null(end_group_idx + 1, all_groupby_exprs_.count(), max_group_idx,
                                       curr_group_store_row))) {
      LOG_WARN("failed to set all null", K(ret));
    } else if (OB_FAIL(aggr_processor_.rollup_batch_process(
                 prev_group_row, curr_group_row,
                 MY_SPEC.is_duplicate_rollup_expr_.at(idx - group_exprs_cnt) ? null_idx : idx,
                 all_groupby_exprs_.count()))) {
      LOG_WARN("failed to rollup process", K(ret));
    }
    if (OB_FAIL(ret)) {
    } else if (idx != cur_rollup_idx) {
      // output prev rollup group row
      ++cur_group_rowid_;
      curr_group_row = nullptr;
      curr_group_store_row = nullptr;
      inc_output_queue_cnt();
      if (aggr_processor_.get_need_advance_collect()
          && OB_FAIL(advance_collect_result(prev_group_row_id))) {
        LOG_WARN("failed to calc and material distinct result", K(ret), K(prev_group_row_id));
      } else if (OB_FAIL(
                   get_empty_rollup_row(cur_group_rowid_, curr_group_row, curr_group_store_row))) {
        LOG_WARN("failed to get one new group row", K(ret));
      } else if (OB_FAIL(swap_group_row(prev_group_row_id, cur_group_rowid_))) {
        LOG_WARN("failed to swap group row", K(ret));
      } else {
        // It must be same as swap group row
        std::swap(output_groupby_rows_[prev_group_row_id], output_groupby_rows_[cur_group_rowid_]);
        LOG_DEBUG("debug gen rollup group row", K(prev_group_row_id), K(cur_group_rowid_),
                  K(cur_rollup_group_id), K(output_queue_cnt_), K(start_diff_group_idx), K(idx),
                  K(max_group_idx));
      }
    }
    prev_group_row_id = idx;
  }
  return ret;
}

int ObMergeGroupByVecOp::rollup_batch_process(aggregate::AggrRowPtr aggr_row,
                                             aggregate::AggrRowPtr rollup_row,
                                             int64_t diff_group_idx /* -1 */,
                                             const int64_t max_group_cnt /* INT64_MIN */)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(aggr_row) || OB_ISNULL(rollup_row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("aggr_row is null", K(aggr_row), K(rollup_row), K(ret));
  } else if (OB_FAIL(rollup_batch_process(aggr_row, rollup_row, diff_group_idx, max_group_cnt))) {
    LOG_WARN("failed to batch process rollup", K(ret));
  }
  return ret;
}

int ObMergeGroupByVecOp::process_rollup(const int64_t diff_group_idx, bool is_end)
{
  int ret = OB_SUCCESS;
  int64_t start_rollup_id = diff_group_idx;
  int64_t end_rollup_id = all_groupby_exprs_.count() - 1;
  int64_t max_group_idx = MY_SPEC.group_exprs_.count() - 1;
  if (ROLLUP_DISTRIBUTOR == MY_SPEC.rollup_status_) {
    start_rollup_id = min(partial_rollup_idx_ - 1, start_rollup_id);
    end_rollup_id = partial_rollup_idx_ - 1;
  } else if (ROLLUP_COLLECTOR == MY_SPEC.rollup_status_) {
    if (0 <= cur_grouping_id_) {
      // if grouping_id is not greater than 0, then it's not base row, don't rollup row
      start_rollup_id = all_groupby_exprs_.count();
    } else {
      // if grouping_id is less than 0, then it's base row for rollup collector
      // +1 is added for grouping_id that is added to group_exprs_
      if (ObThreeStageAggrStage::THIRD_STAGE == MY_SPEC.aggr_stage_) {
        partial_rollup_idx_ = -cur_grouping_id_ - 1;
      } else {
        partial_rollup_idx_ = -cur_grouping_id_;
      }
      start_rollup_id = max(start_rollup_id, partial_rollup_idx_);
      max_group_idx = max(start_rollup_id - 1, partial_rollup_idx_);
    }
  }
  LOG_DEBUG("debug grouping_id", K(end_rollup_id), K(start_rollup_id), K(max_group_idx));
  if (end_rollup_id >= start_rollup_id
      && OB_FAIL(gen_rollup_group_rows(start_rollup_id, end_rollup_id, max_group_idx,
                                        cur_group_rowid_))) {
    LOG_WARN("failed to genereate rollup group row", K(ret));
  }
  return ret;
}

int ObMergeGroupByVecOp::add_batch_rows_for_3stage(const ObBatchRows &brs,
                                                   int32_t start_idx,
                                                   int32_t end_idx)
{
  int ret = OB_SUCCESS;
  if (ObThreeStageAggrStage::THIRD_STAGE == MY_SPEC.aggr_stage_) {
    if (OB_FAIL(add_batch_rows_for_3th_stage(brs, start_idx, end_idx))) {
      LOG_WARN("failed to add batch rows for 3th stage", K(ret), K(start_idx), K(end_idx));
    }
  } else {
    int32_t start_agg_id = 0, end_agg_id = aggr_processor_.aggregates_cnt();
    if (OB_FAIL(ObGroupByVecOp::calculate_3stage_agg_info(
          *cur_group_store_row_->compact_row_, *cur_group_store_row_->ref_row_meta_,
          static_cast<int64_t>(start_idx) /*batch_idx*/, start_agg_id, end_agg_id))) {
      LOG_WARN("calculate stage info failed", K(ret));
    } else if (OB_UNLIKELY(start_agg_id == -1 || end_agg_id == -1)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid start/end aggregate id", K(ret), K(start_agg_id), K(end_agg_id));
    } else if (OB_FAIL(aggr_processor_.add_batch_rows(start_agg_id, end_agg_id, cur_aggr_row_, brs,
                                                      start_idx, end_idx))) {
      LOG_WARN("add batch rows failed", K(ret));
    }
  }
  return ret;
}

int ObMergeGroupByVecOp::add_batch_rows_for_3th_stage(const ObBatchRows &brs,
                                                      const uint32_t start_idx,
                                                      const uint32_t end_idx)
{
  int ret = OB_SUCCESS;
  int32_t start_agg_id = 0, end_agg_id = aggr_processor_.aggregates_cnt();
  for (int i = start_idx; OB_SUCC(ret) && i < end_idx; i++) {
    if (OB_FAIL(ObGroupByVecOp::calculate_3stage_agg_info(*cur_group_store_row_->compact_row_,
                                                          *cur_group_store_row_->ref_row_meta_, i,
                                                          start_agg_id, end_agg_id))) {
      LOG_WARN("calculate stage info failed", K(ret));
    } else if (OB_UNLIKELY(start_agg_id == -1 || end_agg_id == -1)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid start/end aggregate id", K(ret), K(start_agg_id), K(end_agg_id));
    } else if (OB_FAIL(aggr_processor_.add_batch_rows(start_agg_id, end_agg_id, cur_aggr_row_, brs,
                                                      i, i + 1))) {
      LOG_WARN("add batch rows failed", K(ret));
    }
  }
  return ret;
}

int ObMergeGroupByVecOp::aggregate_group_rows(const ObBatchRows &brs, int32_t start_idx,
                                              int32_t end_idx)
{
  int ret = OB_SUCCESS;
  if (!(start_idx == 0 && end_idx == 0)) {
    int32_t start_agg_id = 0, end_agg_id = aggr_processor_.aggregates_cnt();
    if (OB_LIKELY(ObThreeStageAggrStage::NONE_STAGE == MY_SPEC.aggr_stage_)) {
      if (OB_FAIL(aggr_processor_.add_batch_rows(start_agg_id, end_agg_id, cur_aggr_row_, brs,
                                                 start_idx, end_idx))) {
        LOG_WARN("add batch rows failed", K(ret));
      }
    } else {
      if (OB_FAIL(add_batch_rows_for_3stage(brs, start_idx, end_idx))) {
        LOG_WARN("add three stage rows failed", K(ret));
      }
    }
  }
  return ret;
}

int ObMergeGroupByVecOp::process_batch(const ObBatchRows &brs)
{
  int ret = OB_SUCCESS;
  uint32_t group_start_idx = 0;
  uint32_t group_end_idx = 0;
  bool is_iter_end = false;
  bool found_new_group = false;
  int64_t diff_group_idx = -1;
  int64_t group_count = MY_SPEC.group_exprs_.count();
  int64_t all_group_cnt = all_groupby_exprs_.count();
  bool need_dup_data = 0 < MY_SPEC.distinct_exprs_.count() && 0 == MY_SPEC.rollup_exprs_.count();
  int cmp_ret = 0;
  LOG_DEBUG("begin process_batch_results", K(brs.size_), K(group_start_idx), K(group_end_idx),
            K(cur_group_rowid_));

  if (OB_FAIL(aggr_processor_.eval_aggr_param_batch(brs))) {
    LOG_WARN("failed to eval_aggr_param_batch");
  }
  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(eval_ctx_);
  batch_info_guard.set_batch_size(brs.size_);
  if (OB_FAIL(ret)) {
  } else if (all_group_cnt > 0) {
    cur_group_last_row_idx_ = -1;
    int idx = 0;
    group_processor_.prepare_process_next_batch();
    group_end_idx = brs.size_;
    while (OB_SUCC(ret) && !is_iter_end && idx < brs.size_) {
      if (brs.skip_->at(idx)) {
        ++idx;
        continue;
      }
      found_new_group = false;
      if (nullptr == cur_aggr_row_) {
        batch_info_guard.set_batch_idx(idx);
        // only first group
        if (OB_FAIL(get_cur_group_row(brs, cur_group_rowid_, cur_aggr_row_, cur_group_store_row_,
                                      all_groupby_exprs_))) {
          LOG_WARN("failed to get one new group row", K(ret));
        } else {
          is_group_first_calc_ = true;
          cur_group_last_row_idx_ = idx;
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(cur_aggr_row_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected status: cur_aggr_row_ is null", K(ret));
      } else if (OB_FAIL(group_processor_.find_next_new_group(
                   brs, *cur_group_store_row_, found_new_group, group_end_idx, diff_group_idx))) {
        LOG_WARN("failed to find next new group", K(ret));
      } else if (need_dup_data
                 && OB_FAIL(process_unique_distinct_columns_for_batch(
                      *cur_group_store_row_, last_child_output_, group_start_idx, group_end_idx,
                      const_cast<ObBatchRows &>(brs)))) {
        LOG_WARN("failed to check unique distinct columns", K(ret));
      } else if (found_new_group) {
        diff_group_idx = diff_group_idx < group_count ?
                           std::max(diff_group_idx, MY_SPEC.group_exprs_.count() - 1) :
                           diff_group_idx;
        is_group_first_calc_ = true;
        is_iter_end = group_end_idx >= brs.size_ ? true : false;
        group_end_idx = group_end_idx < brs.size_ ? group_end_idx : brs.size_ - 1;
        // calc last group result
        cur_group_last_row_idx_ =
          (is_iter_end ? brs.size_ - 1 : (group_end_idx > 0 ? group_end_idx - 1 : 0));
        LOG_DEBUG("new group found, calc ast group result", K(brs.size_), K(group_start_idx),
                  K(group_end_idx), K(cur_group_rowid_), K(output_queue_cnt_));
        inc_output_queue_cnt();
        if (OB_FAIL(aggregate_group_rows(brs, group_start_idx, group_end_idx))) {
          LOG_WARN("failed to aggregate_group_rows", K(cur_group_rowid_), K(ret),
                   K(group_start_idx), K(group_end_idx));
        } else {
          const int64_t advance_collect_group_id = cur_group_rowid_;
          if (MY_SPEC.has_rollup_ && OB_FAIL(process_rollup(diff_group_idx))) {
            LOG_WARN("failed to process rollup", K(ret));
          } else if (aggr_processor_.get_need_advance_collect()
                     && OB_FAIL(advance_collect_result(advance_collect_group_id))) {
            LOG_WARN("failed to collect distinct result", K(ret), K(advance_collect_group_id));
          } else {
            ++cur_group_rowid_;
            // create new group
            batch_info_guard.set_batch_idx(group_end_idx);
            if (OB_FAIL(get_cur_group_row(brs, cur_group_rowid_, cur_aggr_row_,
                                          cur_group_store_row_, all_groupby_exprs_))) {
              LOG_WARN("failed to get one new group row", K(ret));
            } else {
              group_start_idx = group_end_idx; // record new start idx in next round
            }
          }
        }
      }
      idx = group_end_idx;
    }
  } else { // no groupby column, equals to scalar group by
    if (nullptr == cur_aggr_row_) {
      if (OB_FAIL(get_cur_group_row(brs, cur_group_rowid_, cur_aggr_row_, cur_group_store_row_,
                                    all_groupby_exprs_))) {
        LOG_WARN("failed to get new group row", K(ret));
      }
    }
  }

  group_end_idx = brs.size_;
  LOG_DEBUG("calc last unfinished group row", K(brs.size_), K(found_new_group),
            K(cur_group_rowid_), K(group_start_idx), K(group_end_idx));
  // cur_group_rowid_ is common::OB_INVALID_INDEX means all rows are skipped
  // therefore, do nothing when all rows are skipped
  if (OB_SUCC(ret) && cur_group_rowid_ != common::OB_INVALID_INDEX
      && OB_FAIL(aggregate_group_rows(brs, group_start_idx, group_end_idx))) {
    LOG_WARN("failed to aggregate_group_rows", K(ret), K(cur_group_rowid_), K(group_start_idx),
             K(group_end_idx));
  }
  if (OB_SUCC(ret) && 0 < MY_SPEC.distinct_exprs_.count() && -1 != cur_group_last_row_idx_) {
    // the current row is not same as before row, then save the current row
    ObEvalCtx::BatchInfoScopeGuard batch_info_guard(eval_ctx_);
    batch_info_guard.set_batch_size(brs.size_);
    batch_info_guard.set_batch_idx(cur_group_last_row_idx_);
    if (OB_FAIL(last_child_output_.save_store_row(child_->get_spec().output_, brs, eval_ctx_))) {
      LOG_WARN("failed to store child output", K(ret));
    }
  }
  return ret;
}

int ObMergeGroupByVecOp::groupby_datums_eval_batch(const ObBatchRows &brs)
{
  int ret = OB_SUCCESS;
  int64_t all_count = all_groupby_exprs_.count();

  for (int64_t i = 0; OB_SUCC(ret) && i < all_count; i++) {
    ObExpr *expr = all_groupby_exprs_.at(i);
    if (OB_FAIL(expr->eval_vector(eval_ctx_, brs))) {
      LOG_WARN("eval failed", K(ret));
    }
  }
  return ret;
}

int ObMergeGroupByVecOp::process_unique_distinct_columns_for_batch(
  const LastCompactRow &cur_gby_store_row, const LastCompactRow &last_child_output,
  const uint32_t group_start_idx, const uint32_t group_end_idx, ObBatchRows &brs)
{
  int ret = OB_SUCCESS;
  int cmp_ret = 0;
  ObLength r_len = 0;
  bool r_null = false;
  const char *r_v = nullptr;
  bool is_same_before_row = true;
  const ObDatum &aggr_code_datum = cur_gby_store_row.get_datum(MY_SPEC.aggr_code_idx_);
  for (uint32_t idx = group_start_idx; OB_SUCC(ret) && idx < group_end_idx; idx++) {
    is_same_before_row = true;
    if (brs.skip_->at(idx)) {
      continue;
    } else if (is_group_first_calc_) {
      is_same_before_row = false;
      is_group_first_calc_ = false;
    } else if (aggr_code_datum.get_int() >= MY_SPEC.dist_aggr_group_idxes_.count()) {
      // non-distinct aggregate function
      is_same_before_row = false;
      LOG_DEBUG("debug non-distinct aggregate function", K(ret), K(aggr_code_datum.get_int()),
                K(MY_SPEC.dist_aggr_group_idxes_.count()));
    } else if (-1 != cur_group_last_row_idx_) {
      // non first batch
      for (int64_t i = 0;
           is_same_before_row && i < distinct_col_idx_in_output_.count() && OB_SUCC(ret); ++i) {
        if (-1 == distinct_col_idx_in_output_.at(i)) {
          continue;
        }
        ObExpr *expr = MY_SPEC.distinct_exprs_.at(i);
        ObIVector *vec = expr->get_vector(eval_ctx_);
        vec->get_payload(cur_group_last_row_idx_, r_null, r_v, r_len);
        if (OB_FAIL(vec->null_first_cmp(*expr, idx, r_null, r_v, r_len, cmp_ret))) {
          LOG_WARN("compare failed", K(ret));
        } else if (0 != cmp_ret) {
          is_same_before_row = false;
        }
      } // end for
      LOG_DEBUG("debug non-distinct aggregate function", K(ret), K(aggr_code_datum.get_int()),
                K(MY_SPEC.dist_aggr_group_idxes_.count()), K(is_same_before_row));
    } else {
      for (int64_t i = 0;
           is_same_before_row && i < distinct_col_idx_in_output_.count() && OB_SUCC(ret); ++i) {
        if (-1 == distinct_col_idx_in_output_.at(i)) {
          continue;
        }
        r_null = last_child_output.is_null(distinct_col_idx_in_output_.at(i));
        last_child_output.get_cell_payload(distinct_col_idx_in_output_.at(i), r_v, r_len);
        ObExpr *expr = MY_SPEC.distinct_exprs_.at(i);
        ObIVector *vec = expr->get_vector(eval_ctx_);
        if (OB_FAIL(vec->null_first_cmp(*expr, idx, r_null, r_v, r_len, cmp_ret))) {
          LOG_WARN("compare failed", K(ret));
        } else if (0 != cmp_ret) {
          is_same_before_row = false;
        }
      } // end for
      LOG_DEBUG("finish check unique distinct columns", K(ret), K(aggr_code_datum.get_int()), "row",
                ROWEXPR2STR(eval_ctx_, MY_SPEC.distinct_exprs_), K(is_same_before_row));
    }
    LOG_DEBUG("finish check unique distinct columns", K(ret), "row",
              ROWEXPR2STR(eval_ctx_, MY_SPEC.distinct_exprs_), K(is_same_before_row));
    if (OB_SUCC(ret)) {
      if (is_same_before_row) {
        brs.skip_->set(idx);
        brs.set_all_rows_active(false);
      }
      cur_group_last_row_idx_ = idx;
    }
  }
  return ret;
}

int ObMergeGroupByVecOp::collect_group_results(const RowMeta &row_meta,
                                               const ObIArray<ObExpr *> &groupby_exprs,
                                               const int32_t output_batch_size,
                                               ObBatchRows &output_brs, int64_t &cur_group_id)
{
  int ret = OB_SUCCESS;
  const int64_t start_group_id = cur_group_id;
  int32_t output_size = output_batch_size;
  LastCompactRow *start_group_row = get_groupby_store_row(start_group_id);
  if (OB_ISNULL(start_group_row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("is null", K(start_group_id), K(output_groupby_rows_.count()));
  } else if (OB_FAIL(aggr_processor_.collect_group_results(row_meta, output_size, output_brs,
                                                           cur_output_group_id_))) {
    LOG_WARN("failed to collect batch result", K(ret));
  } else {
    clear_evaluated_flag();
    for (int i = 0; OB_SUCC(ret) && i < output_size; i++) {
      int64_t group_id = start_group_id + i;
      LastCompactRow *aggr_row = get_groupby_store_row(group_id);
      if (OB_ISNULL(aggr_row)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("is null", K(group_id), K(output_groupby_rows_.count()));
      } else if (FALSE_IT(output_stored_rows_[i] = aggr_row->compact_row_)) {
      } else if (ObRollupStatus::ROLLUP_DISTRIBUTOR == MY_SPEC.rollup_status_) {
        output_rollup_ids_[i] = aggr_row->get_extra_payload();
      }
    }
    for (int col_id = 0; OB_SUCC(ret) && col_id < groupby_exprs.count(); col_id++) {
      ObExpr *out_expr = groupby_exprs.at(col_id);
      if (OB_ISNULL(out_expr)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_LOG(WARN, "invalid null expr", K(ret));
      } else if (OB_UNLIKELY(out_expr->is_const_expr())) {
        // do nothing
      } else if (OB_FAIL(out_expr->init_vector_default(eval_ctx_, output_batch_size))) {
        SQL_LOG(WARN, "init vector failed", K(ret));
      } else if (OB_FAIL(out_expr->get_vector(eval_ctx_)->from_rows(
                   *start_group_row->ref_row_meta_, output_stored_rows_, output_size, col_id))) {
        SQL_LOG(WARN, "from rows failed", K(ret));
      }
    }
    for (int i = 0; OB_SUCC(ret) && i < groupby_exprs.count(); i++) {
      if (!groupby_exprs.at(i)->is_const_expr()) {
        groupby_exprs.at(i)->set_evaluated_projected(eval_ctx_);
      }
    }
    if (OB_SUCC(ret) && ObRollupStatus::ROLLUP_DISTRIBUTOR == MY_SPEC.rollup_status_) {
      if (OB_FAIL(MY_SPEC.rollup_id_expr_->init_vector_default(eval_ctx_, output_batch_size))) {
        LOG_WARN("failed to init vector", K(ret));
      } else {
        ObIVector *vec = MY_SPEC.rollup_id_expr_->get_vector(eval_ctx_);
        VectorFormat fmt = MY_SPEC.rollup_id_expr_->get_format(eval_ctx_);
        VecValueTypeClass vec_tc = MY_SPEC.rollup_id_expr_->get_vec_value_tc();
        if (VEC_FIXED == fmt && VEC_TC_INTEGER == vec_tc) {
          for (int i = 0; i < output_size; i++) {
            static_cast<ObFixedLengthFormat<RTCType<VEC_TC_INTEGER>> *>(vec)->set_payload(
              i, output_rollup_ids_[i], sizeof(int64_t));
          }
        } else {
          for (int i = 0; i < output_size; i++) {
            vec->set_payload(i, output_rollup_ids_[i], sizeof(int64_t));
          }
        }
        MY_SPEC.rollup_id_expr_->set_evaluated_projected(eval_ctx_);
      }
    }
  }
  return ret;
}

int ObMergeGroupByVecOp::calc_batch_results(const bool is_iter_end, const int64_t max_output_size)
{
  int ret = OB_SUCCESS;
  if (nullptr == cur_aggr_row_) {
    // get empty result from child, just return empty
    brs_.size_ = 0;
    brs_.end_ = is_iter_end;
    LOG_DEBUG("debug cur_aggr_row_ is null", K(ret));
  } else {
    if (is_iter_end) {
      is_end_ = true;
    }

    // make sure output size is less than aggr group_rows
    int64_t output_size = common::min(get_output_queue_cnt(), max_output_size);
    clear_evaluated_flag();
    // note: brs_.size_ is set in collect_result_batch
    if (OB_FAIL(collect_group_results(agg_row_meta_, all_groupby_exprs_, output_size, brs_,
                                      cur_output_group_id_))) {
      LOG_WARN("failed to collect batch result", K(ret));
    } else {
      LOG_DEBUG("collect result done", K(cur_output_group_id_),
                K(get_output_queue_cnt()), K(is_iter_end), K(output_size),
                K(get_group_rows_count()));
      set_output_queue_cnt(get_output_queue_cnt() - brs_.size_);
      if (!is_output_queue_not_empty()) {
        LOG_DEBUG("all output row are consumed, do the cleanup",
                  K(cur_output_group_id_), K(get_output_queue_cnt()),
                  K(is_iter_end), K(output_size),
                  K(get_group_rows_count()),
                  K(cur_group_rowid_));

        // aggregation rows cleanup and reuse
        int64_t start_pos = MY_SPEC.has_rollup_ ? all_groupby_exprs_.count(): 0;
        for (int64_t i = start_pos; OB_SUCC(ret) && i < cur_group_rowid_; i++) {
          if (OB_FAIL(reuse_group(i))) {
            LOG_WARN("failed to reuse group ", K(ret), K(i));
          }
        }
        if (is_end_) {// all output rows are consumed, child operator eached end. mark batch end
          brs_.end_ = true;
        } else {
          // move last unfinished grouprow
          // curr_group_rowid(last group row) calculation is NOT done, move it
          // to group 0
          if (OB_FAIL(swap_group_row(start_pos, cur_group_rowid_))) {
            LOG_WARN("failed to swap aggregation group rows", K(ret),
                     K(cur_group_rowid_));
          } else {
            std::swap(output_groupby_rows_[start_pos],
                      output_groupby_rows_[cur_group_rowid_]);
          }
          cur_group_rowid_ = start_pos;
          cur_output_group_id_ = start_pos;
        }
      }
    }
  }
  return ret;
}

int ObMergeGroupByVecOp::reuse_group(const int64_t group_id)
{
  int ret = OB_SUCCESS;
  LastCompactRow *store_row = nullptr;
  if (OB_FAIL(aggr_processor_.reuse_group(group_id))) {
    LOG_WARN("failed to reuse group", K(ret), K(group_id));
  } else if (OB_FAIL(get_groupby_store_row(group_id, store_row))) {
    LOG_WARN("failed to get group by store row", K(ret), K(group_id));
  } else {
    store_row->reuse();
  }
  return ret;
}

int ObMergeGroupByVecOp::get_n_shuffle_keys_for_exchange(int64_t &shuffle_n_keys)
{
  int ret = OB_SUCCESS;
  shuffle_n_keys = 0;
  if (INT64_MAX == partial_rollup_idx_ || 0 >= partial_rollup_idx_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: invalid partial rollup idx", K(ret), K(partial_rollup_idx_));
  } else {
    // the keys of exchange contains group_exprs, [aggr_code], grouping_id and rollup_exprs
    // grouping_id is append shuffle expr
    if (MY_SPEC.group_exprs_.count() >= partial_rollup_idx_) {
      shuffle_n_keys = partial_rollup_idx_;
    } else {
      if (ObThreeStageAggrStage::SECOND_STAGE == MY_SPEC.aggr_stage_) {
        // aggr_code is groupby exprs in second stage,
        // but it's not groupby exprs in third stage
        shuffle_n_keys = partial_rollup_idx_;
      } else {
        shuffle_n_keys = partial_rollup_idx_ + 1;
      }
      LOG_TRACE("debug merge groupby shuffle keys", K(shuffle_n_keys));
    }
  }
  return ret;
}

int ObMergeGroupByVecOp::init_hp_infras_group_mgr()
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = ctx_.get_my_session()->get_effective_tenant_id();
  if (aggr_processor_.has_distinct()) {
    int64_t est_rows = MY_SPEC.est_rows_per_group_;
    aggr_processor_.set_io_event_observer(&io_event_observer_);
    if (OB_FAIL(ObPxEstimateSizeUtil::get_px_size(&ctx_, MY_SPEC.px_est_size_factor_, est_rows,
                                                  est_rows))) {
      LOG_WARN("failed to get px size", K(ret));
    } else if (OB_FAIL(sql_mem_processor_.init(&ctx_.get_allocator(), tenant_id,
                                               est_rows * MY_SPEC.width_, MY_SPEC.type_,
                                               MY_SPEC.id_, &ctx_))) {
      LOG_WARN("failed to init sql mem processor", K(ret));
    } else if (OB_FAIL(hp_infras_mgr_.init(tenant_id, GCONF.is_sql_operator_dump_enabled(),
                                           est_rows, MY_SPEC.width_, true /*unique*/, 1 /*ways*/,
                                           &eval_ctx_, &sql_mem_processor_, &io_event_observer_,
                                           MY_SPEC.compress_type_))) {
      LOG_WARN("failed to init hash infras group", K(ret));
    } else {
      aggr_processor_.set_hp_infras_mgr(&hp_infras_mgr_);
    }
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase
