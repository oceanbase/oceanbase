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

#include "sql/engine/aggregate/ob_hash_distinct_vec_op.h"
#include "sql/engine/px/ob_px_util.h"
#include "sql/engine/basic/ob_hp_infras_vec_mgr.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObHashDistinctVecSpec::ObHashDistinctVecSpec(ObIAllocator &alloc, const ObPhyOperatorType type)
  : ObOpSpec(alloc, type),
  distinct_exprs_(alloc),
  sort_collations_(alloc),
  is_block_mode_(false),
  by_pass_enabled_(false),
  is_push_down_(false),
  group_distinct_exprs_(alloc),
  grouping_id_(nullptr),
  group_sort_collations_(alloc),
  has_non_distinct_aggr_params_(false)
{}

OB_SERIALIZE_MEMBER((ObHashDistinctVecSpec, ObOpSpec),
                    distinct_exprs_,
                    sort_collations_,
                    is_block_mode_,
                    by_pass_enabled_,
                    is_push_down_,
                    group_distinct_exprs_,
                    grouping_id_,
                    group_sort_collations_,
                    has_non_distinct_aggr_params_);

ObHashDistinctVecOp::ObHashDistinctVecOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
    : ObOperator(exec_ctx, spec, input),
    first_got_row_(true),
    has_got_part_(false),
    iter_end_(false),
    child_op_is_end_(false),
    need_init_(true),
    profile_(ObSqlWorkAreaType::HASH_WORK_AREA),
    sql_mem_processor_(profile_, op_monitor_info_),
    group_cnt_(0),
    hash_values_for_batch_(nullptr),
    extend_bkt_num_push_down_(0),
    build_distinct_data_batch_func_(&ObHashDistinctVecOp::build_distinct_data_for_batch),
    bypass_ctrl_(),
    mem_context_(NULL),
    hp_infras_mgr_(nullptr),
    hp_infras_arr_(ctx_.get_allocator()),
    group_selector_arr_(ctx_.get_allocator()),
    group_iter_idx_(-1),
    min_stored_group_idx_(INT64_MAX),
    group_distinct_state_(GroupDistinctState::ITER_END),
    non_distinct_aggr_params_store_(nullptr),
    non_distinct_aggr_params_iter_(nullptr),
    group_distinct_bucket_cnt_(0)
{
  enable_sql_dumped_ = GCONF.is_sql_operator_dump_enabled();
}

int ObHashDistinctVecOp::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == left_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: left is null", K(ret));
  } else if (OB_FAIL(ObOperator::inner_open())) {
    LOG_WARN("failed to inner open", K(ret));
  } else if (MY_SPEC.is_push_down_
            && OB_FAIL(ctx_.get_my_session()
                            ->get_sys_variable(share::SYS_VAR__GROUPBY_NOPUSHDOWN_CUT_RATIO,
                                                                            bypass_ctrl_.cut_ratio_))) {
    LOG_WARN("failed to get no pushdown cut ratio", K(ret));
  } else if (OB_FAIL(init_mem_context())) {
    LOG_WARN("failed to init mem context", K(ret));
  } else if (MY_SPEC.group_distinct_exprs_.count() > 0) {
    if (OB_FAIL(init_group_hp_infras())) {
      LOG_WARN("failed to init group distinct infras", K(ret));
    } else {
      get_next_batch_func_ = &ObHashDistinctVecOp::do_group_distinct_for_batch;
    }
  } else {
    first_got_row_ = true;
    tenant_id_ = ctx_.get_my_session()->get_effective_tenant_id();
    if (MY_SPEC.is_block_mode_) {
      get_next_batch_func_ = &ObHashDistinctVecOp::do_block_distinct_for_batch;
    } else {
      get_next_batch_func_ = &ObHashDistinctVecOp::do_unblock_distinct_for_batch;
    }
    if (MY_SPEC.by_pass_enabled_) {
      CK (!MY_SPEC.is_block_mode_);
      // to avoid performance decrease, at least deduplicate 2/3
      bypass_ctrl_.cut_ratio_ = (bypass_ctrl_.cut_ratio_ <= ObAdaptiveByPassCtrl::INIT_CUT_RATIO
                                  ? ObAdaptiveByPassCtrl::INIT_CUT_RATIO : bypass_ctrl_.cut_ratio_);
      build_distinct_data_batch_func_ = &ObHashDistinctVecOp::build_distinct_data_for_batch_by_pass;
    }
    LOG_TRACE("trace block mode", K(MY_SPEC.is_block_mode_),
                                  K(spec_.id_), K(MY_SPEC.is_push_down_), K(MY_SPEC.group_distinct_exprs_),
                                  K(MY_SPEC.distinct_exprs_), K(MY_SPEC.has_non_distinct_aggr_params_));
  }
  return ret;
}

int ObHashDistinctVecOp::inner_close()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOperator::inner_close())) {
    LOG_WARN("failed to inner close", K(ret));
  } else {
    reset();
  }
  sql_mem_processor_.unregister_profile();
  return ret;
}

void ObHashDistinctVecOp::reset()
{
  first_got_row_ = true;
  has_got_part_ = false;
  group_cnt_ = 0;
  group_distinct_bucket_cnt_ = 0;
  hp_infras_.reset();
  bypass_ctrl_.reset();
  if (MY_SPEC.group_distinct_exprs_.count() > 0) {
    get_next_batch_func_ = &ObHashDistinctVecOp::do_group_distinct_for_batch;
  } else if (MY_SPEC.is_block_mode_) {
    get_next_batch_func_ = &ObHashDistinctVecOp::do_block_distinct_for_batch;
  } else {
    get_next_batch_func_ = &ObHashDistinctVecOp::do_unblock_distinct_for_batch;
  }
  for (int i = 0; i < hp_infras_arr_.count(); i++) {
    if (hp_infras_arr_.at(i) != nullptr) {
      hp_infras_arr_.at(i)->reset();
    }
  }
  if (non_distinct_aggr_params_iter_ != nullptr) {
    non_distinct_aggr_params_iter_->reset();
  }
  if (non_distinct_aggr_params_store_ != nullptr) {
    non_distinct_aggr_params_store_->reset();
  }
  group_iter_idx_ = -1;
  min_stored_group_idx_ = INT64_MAX;
  group_distinct_state_ = GroupDistinctState::ITER_END;
  LOG_TRACE("trace block mode", K(MY_SPEC.is_block_mode_), K(spec_.id_), K(MY_SPEC.group_distinct_exprs_.count()));
}

int ObHashDistinctVecOp::inner_rescan()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOperator::inner_rescan())) {
    LOG_WARN("failed to rescan child operator", K(ret));
  } else {
    reset();
    iter_end_ = false;
  }
  return ret;
}

void ObHashDistinctVecOp::destroy()
{
  sql_mem_processor_.unregister_profile_if_necessary();
  hp_infras_.destroy();
  for (int i = 0; i < hp_infras_arr_.count(); i++) {
    if (hp_infras_arr_.at(i) != nullptr) {
      hp_infras_arr_.at(i)->destroy();
      hp_infras_arr_.at(i) = nullptr;
    }
  }
  hp_infras_arr_.reset();
  if (hp_infras_mgr_ != nullptr) {
    hp_infras_mgr_->destroy();
    hp_infras_mgr_ = nullptr;
  }
  if (non_distinct_aggr_params_iter_ != nullptr) {
    non_distinct_aggr_params_iter_->~Iterator();
    non_distinct_aggr_params_iter_ = nullptr;
  }
  if (non_distinct_aggr_params_store_ != nullptr) {
    non_distinct_aggr_params_store_->~ObTempColumnStore();
    non_distinct_aggr_params_store_ = nullptr;
  }
  group_selector_arr_.reset();
  group_iter_idx_ = -1;
  min_stored_group_idx_ = INT64_MAX;
  group_distinct_state_ = GroupDistinctState::ITER_END;
  if (OB_LIKELY(NULL != mem_context_)) {
    DESTROY_CONTEXT(mem_context_);
    mem_context_ = NULL;
  }
  ObOperator::destroy();
}

int ObHashDistinctVecOp::init_hash_partition_infras()
{
  int ret = OB_SUCCESS;
  int64_t est_rows = MY_SPEC.rows_;
  if (OB_FAIL(ObPxEstimateSizeUtil::get_px_size(
      &ctx_, MY_SPEC.px_est_size_factor_, est_rows, est_rows))) {
    LOG_WARN("failed to get px size", K(ret));
  } else if (OB_FAIL(sql_mem_processor_.init(
                  &mem_context_->get_malloc_allocator(),
                  tenant_id_,
                  est_rows * MY_SPEC.width_,
                  MY_SPEC.type_,
                  MY_SPEC.id_,
                  &ctx_))) {
    LOG_WARN("failed to init sql mem processor", K(ret));
  } else if (OB_FAIL(hp_infras_.init(tenant_id_,
      enable_sql_dumped_,
      true, true, 2, MY_SPEC.max_batch_size_, MY_SPEC.distinct_exprs_, &sql_mem_processor_,
      MY_SPEC.compress_type_))) {
    LOG_WARN("failed to init hash partition infrastructure", K(ret));
  } else {
    hp_infras_.set_io_event_observer(&io_event_observer_);
    if (MY_SPEC.by_pass_enabled_) {
      hp_infras_.set_push_down();
    }
    int64_t est_bucket_num = hp_infras_.est_bucket_count(est_rows, MY_SPEC.width_,
                                MIN_BUCKET_COUNT, MAX_BUCKET_COUNT);
    if (OB_FAIL(hp_infras_.set_funcs(&MY_SPEC.sort_collations_, &eval_ctx_))) {
      LOG_WARN("failed to set funcs", K(ret));
    } else if (OB_FAIL(hp_infras_.start_round())) {
      LOG_WARN("failed to start round", K(ret));
    } else if (OB_FAIL(hp_infras_.init_hash_table(est_bucket_num,
        MIN_BUCKET_COUNT, MAX_BUCKET_COUNT))) {
      LOG_WARN("failed to init hash table", K(ret));
    } else {
      extend_bkt_num_push_down_ = INIT_L3_CACHE_SIZE / hp_infras_.get_bucket_size();
      op_monitor_info_.otherstat_1_value_ = est_bucket_num;
      op_monitor_info_.otherstat_1_id_ = ObSqlMonitorStatIds::HASH_INIT_BUCKET_COUNT;
      op_monitor_info_.otherstat_4_value_ = MY_SPEC.is_block_mode_;
      op_monitor_info_.otherstat_4_id_ = ObSqlMonitorStatIds::DISTINCT_BLOCK_MODE;
    }
  }
  return ret;
}

int ObHashDistinctVecOp::init_hash_partition_infras_for_batch()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_hash_partition_infras())) {
    LOG_WARN("failed to get next row", K(ret));
  } else if (need_init_ && OB_FAIL(hp_infras_.init_my_skip(MY_SPEC.max_batch_size_))) {
    LOG_WARN("failed to init hp skip", K(ret));
  }
  return ret;
}

int ObHashDistinctVecOp::build_distinct_data_for_batch(const int64_t batch_size, bool is_block)
{
  int ret = OB_SUCCESS;
  bool got_batch = false;
  int64_t read_rows = -1;
  bool finish_turn = false;
  ObBitVector *output_vec = nullptr;
  while(OB_SUCC(ret) && !got_batch) {
    const ObBatchRows *child_brs = nullptr;
    if (!has_got_part_) {
      clear_evaluated_flag();
      if (child_op_is_end_) {
        finish_turn = true;
      } else if (OB_FAIL(child_->get_next_batch(batch_size, child_brs))) {
        LOG_WARN("failed to get next batch from child op", K(ret), K(is_block));
      } else if (OB_FAIL(hp_infras_.calc_hash_value_for_batch(
                   MY_SPEC.distinct_exprs_, *child_brs->skip_, child_brs->size_,
                   child_brs->all_rows_active_, hash_values_for_batch_))) {
        LOG_WARN("failed to calc hash values batch for child", K(ret));
      } else {
        //child_op_is_end_ means last batch data is return, finish_turn means no data to process
        child_op_is_end_ = child_brs->end_ && (child_brs->size_ != 0);
        finish_turn = child_brs->end_ && (0 == child_brs->size_);
        read_rows = child_brs->size_;
      }
    } else if (OB_FAIL(hp_infras_.get_left_next_batch(MY_SPEC.distinct_exprs_,
                                                      batch_size,
                                                      read_rows,
                                                      hash_values_for_batch_))) {
      if (OB_ITER_END == ret) {
        //no data to process
        finish_turn = true;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to get next batch from hp infra", K(ret));
      }
    }
    if (OB_SUCC(ret) && finish_turn) {
      ret = OB_SUCCESS;
      finish_turn = false;
      //get dump partition
      if (OB_FAIL(hp_infras_.finish_insert_row())) {
        LOG_WARN("failed to finish insert row", K(ret));
      } else if (!has_got_part_) {
        has_got_part_ = true;
        LOG_DEBUG("start get dumped partition", K(group_cnt_));
      } else {
        if (OB_FAIL(hp_infras_.close_cur_part(InputSide::LEFT))) {
          LOG_WARN("failed to close curr part", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (is_block) {
        //if true, means we have process a full partition, then break the loop and return rows
        break;
        LOG_TRACE("trace block", K(is_block));
      } else if (OB_FAIL(hp_infras_.end_round())) {
        LOG_WARN("failed to end round", K(ret));
      } else if (OB_FAIL(hp_infras_.start_round())) {
        LOG_WARN("failed to start round", K(ret));
      } else if (OB_FAIL(hp_infras_.get_next_partition(InputSide::LEFT))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to get next dump partition", K(ret));
        }
      } else if (OB_FAIL(hp_infras_.open_cur_part(InputSide::LEFT))) {
        LOG_WARN("failed to open curr part", K(ret));
      } else if (OB_FAIL(hp_infras_.resize(hp_infras_.get_cur_part_row_cnt(InputSide::LEFT)))) {
        LOG_WARN("failed to init hash table", K(ret));
      }
    } else if (OB_FAIL(ret)) {
    } else if (OB_FAIL(try_check_status())) {
      LOG_WARN("failed to check status", K(ret));
    } else if (!has_got_part_
               && OB_FAIL(hp_infras_.insert_row_for_batch(MY_SPEC.distinct_exprs_,
                                                          hash_values_for_batch_,
                                                          read_rows,
                                                          child_brs->skip_,
                                                          output_vec))) {
      LOG_WARN("failed to insert batch rows, no dump", K(ret));
    } else if (has_got_part_
               && OB_FAIL(hp_infras_.insert_row_for_batch(MY_SPEC.distinct_exprs_,
                                                          hash_values_for_batch_,
                                                          read_rows,
                                                          nullptr,
                                                          output_vec))) {
      LOG_WARN("failed to insert batch rows, dump", K(ret));
    } else if (OB_ISNULL(output_vec)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get output vector", K(ret));
    } else if (!is_block) {
      brs_.size_ = read_rows;
      brs_.skip_->deep_copy(*output_vec, read_rows);
      int64_t got_rows = read_rows - output_vec->accumulate_bit_cnt(read_rows);
      group_cnt_ += got_rows;
      got_batch = (got_rows != 0);
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
    brs_.size_ = 0;
    brs_.end_ = true;
    iter_end_ = true;
  }
  return ret;
}

int ObHashDistinctVecOp::build_distinct_data_for_batch_by_pass(const int64_t batch_size, bool is_block)
{
  int ret = OB_SUCCESS;
  bool got_batch = false;
  int64_t read_rows = -1;
  bool finish_turn = false;
  ObBitVector *output_vec = nullptr;
  bool can_insert = true;
  int64_t exists_count = 0;
  while(OB_SUCC(ret) && !got_batch) {
    if (!bypass_ctrl_.by_pass_ && bypass_ctrl_.rebuild_times_ >= MAX_REBUILD_TIMES) {
      bypass_ctrl_.by_pass_ = true;
    }
    const ObBatchRows *child_brs = nullptr;
    clear_evaluated_flag();
    if (bypass_ctrl_.by_pass_) {
      if (OB_FAIL(by_pass_get_next_batch(batch_size))) {
        LOG_WARN("failed to get next batch", K(ret));
      }
      break;
    }
    if (OB_FAIL(ret)) {
    } else if (child_op_is_end_) {
      finish_turn = true;
    } else if (OB_FAIL(child_->get_next_batch(batch_size, child_brs))) {
      LOG_WARN("failed to get next batch from child op", K(ret));
    } else if (OB_FAIL(hp_infras_.calc_hash_value_for_batch(
                 MY_SPEC.distinct_exprs_, *child_brs->skip_, child_brs->size_,
                 child_brs->all_rows_active_, hash_values_for_batch_))) {
      LOG_WARN("failed to calc hash values batch for child", K(ret));
    } else {
      int64_t add_cnt = (child_brs->size_
                            - (child_brs->skip_->accumulate_bit_cnt(child_brs->size_)));
      bypass_ctrl_.processed_cnt_ += add_cnt;
      //child_op_is_end_ means last batch data is return, finish_turn means no data to process
      child_op_is_end_ = child_brs->end_ && (child_brs->size_ != 0);
      finish_turn = child_brs->end_ && (0 == child_brs->size_);
      read_rows = child_brs->size_;
      if (OB_FAIL(process_state(add_cnt, extend_bkt_num_push_down_, bypass_ctrl_, hp_infras_, can_insert))) {
        LOG_WARN("failed to process state ", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (finish_turn) {
      ret = OB_ITER_END;
    } else if (OB_FAIL(try_check_status())) {
      LOG_WARN("failed to check status", K(ret));
    } else if (OB_FAIL(hp_infras_.
                         do_insert_batch_with_unique_hash_table_by_pass(MY_SPEC.distinct_exprs_,
                                                                        hash_values_for_batch_,
                                                                        read_rows,
                                                                        child_brs->skip_,
                                                                        is_block,
                                                                        can_insert,
                                                                        exists_count,
                                                                        bypass_ctrl_.by_pass_,
                                                                        output_vec))) {
      LOG_WARN("failed to insert batch rows, no dump", K(ret));
    } else if (OB_ISNULL(output_vec)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get output vector", K(ret));
    } else {
      bypass_ctrl_.exists_cnt_ += exists_count;
      brs_.size_ = read_rows;
      brs_.skip_->deep_copy(*output_vec, read_rows);
      int64_t got_rows = read_rows - output_vec->accumulate_bit_cnt(read_rows);
      group_cnt_ += got_rows;
      got_batch = (got_rows != 0);
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
    brs_.size_ = 0;
    brs_.end_ = true;
    iter_end_ = true;
  }
  return ret;
}

int ObHashDistinctVecOp::do_unblock_distinct_for_batch(const int64_t batch_size)
{
  LOG_DEBUG("calc unblock hash distinct batch mode", K(batch_size));
  int ret = OB_SUCCESS;
  if (first_got_row_) {
    if (OB_FAIL(init_hash_partition_infras_for_batch())) {
      LOG_WARN("failed to init hash infra for batch", K(ret));
    }
    first_got_row_ = false;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(((this->*build_distinct_data_batch_func_)(batch_size, false)))) {
    LOG_WARN("failed to build distinct data for batch", K(ret));
  }
  return ret;
}

int ObHashDistinctVecOp::do_block_distinct_for_batch(const int64_t batch_size) {
  LOG_DEBUG("calc block hash distinct batch mode", K(batch_size));
  int ret = OB_SUCCESS;
  if (first_got_row_) {
    if (OB_FAIL(init_hash_partition_infras_for_batch())) {
      LOG_WARN("failed to init hash infra for batch", K(ret));
    } else if (OB_FAIL(((this->*build_distinct_data_batch_func_)(batch_size, true)))) {
      LOG_WARN("failed to build distinct data", K(ret));
    } else if (OB_FAIL(hp_infras_.open_hash_table_part())) {
      LOG_WARN("failed to open hash table part", K(ret));
    }
    clear_evaluated_flag();
    first_got_row_ = false;
  }
  if (OB_SUCC(ret)) {
    int64_t read_rows = 0;
    if (OB_FAIL(hp_infras_.get_next_hash_table_batch(MY_SPEC.distinct_exprs_,
                                                    batch_size,
                                                    read_rows,
                                                    nullptr))) {
      //Ob_ITER_END means data from child_ or current
      //partition in infra is run out， read_size <= batch_size
      if (OB_ITER_END == ret) {
        if (OB_FAIL(hp_infras_.end_round())) {
          LOG_WARN("failed to end round", K(ret));
        } else if (OB_FAIL(hp_infras_.start_round())) {
          LOG_WARN("failed to start round", K(ret));
        } else if (OB_FAIL(hp_infras_.get_next_partition(InputSide::LEFT))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("failed to get dumped partitions", K(ret));
          }
        } else if (OB_FAIL(hp_infras_.open_cur_part(InputSide::LEFT))) {
          LOG_WARN("failed to open cur part", K(ret));
        } else if (OB_FAIL(hp_infras_.resize(hp_infras_.get_cur_part_row_cnt(InputSide::LEFT)))) {
          LOG_WARN("failed to init hashtable", K(ret));
        } else if (OB_FAIL(((this->*build_distinct_data_batch_func_)(batch_size, true)))) {
          LOG_WARN("failed to build distinct data for batch", K(ret));
        } else if (OB_FAIL(hp_infras_.open_hash_table_part())) {
          LOG_WARN("failed to open hash table part", K(ret));
        } else if (OB_FAIL(hp_infras_.get_next_hash_table_batch(MY_SPEC.distinct_exprs_,
                                                                batch_size,
                                                                read_rows,
                                                                nullptr))) {
          LOG_WARN("failed to get next row in hash table", K(ret));
        } else {
          group_cnt_ += read_rows;
          brs_.size_ = read_rows;
        }
      } else {
        LOG_WARN("failed to get next batch in hash table", K(ret));
      }
    } else {
      group_cnt_ += read_rows;
      brs_.size_ = read_rows;
      brs_.all_rows_active_ = true;
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      brs_.size_ = 0;
      brs_.end_ = true;
      iter_end_ = true;
    }
  }

  return ret;
}

int ObHashDistinctVecOp::inner_get_next_batch(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  int64_t batch_size = min(max_row_cnt, MY_SPEC.max_batch_size_);
  if (iter_end_) {
    brs_.size_ = 0;
    brs_.end_ = true;
  } else if (OB_UNLIKELY(OB_ISNULL(get_next_batch_func_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get next batch func is nullptr", K(ret));
  } else if (need_init_) {
    if (OB_ISNULL(hash_values_for_batch_
                  = static_cast<uint64_t *> (ctx_.get_allocator().alloc(MY_SPEC.max_batch_size_ * sizeof(uint64_t))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to init hash values for batch", K(ret), K(MY_SPEC.max_batch_size_));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL((this->*get_next_batch_func_)(batch_size))) {
    LOG_WARN("failed to get next batch", K(ret));
  } else if (brs_.end_) {
    op_monitor_info_.otherstat_2_value_ = group_cnt_;
    op_monitor_info_.otherstat_2_id_ = ObSqlMonitorStatIds::HASH_ROW_COUNT;
    if (MY_SPEC.group_distinct_exprs_.count() > 0) {
      op_monitor_info_.otherstat_3_value_ = group_distinct_bucket_cnt_;
      op_monitor_info_.otherstat_3_id_ = ObSqlMonitorStatIds::HASH_BUCKET_COUNT;
    } else {
      op_monitor_info_.otherstat_3_value_ = get_hash_bucket_num();
      op_monitor_info_.otherstat_3_id_ = ObSqlMonitorStatIds::HASH_BUCKET_COUNT;
    }
    //for hash distinct , brs_.end means no data need to process, so do reset here
    reset();
    iter_end_ = true;
  }
  need_init_ = false;
  return ret;
}

int ObHashDistinctVecOp::by_pass_get_next_batch(const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  const ObBatchRows *child_brs = nullptr;
  if (OB_FAIL(child_->get_next_batch(batch_size, child_brs))) {
    LOG_WARN("failed to get next batcn", K(ret));
  } else {
    brs_.size_ = child_brs->size_;
    group_cnt_ += (child_brs->size_ - child_brs->skip_->accumulate_bit_cnt(batch_size));
    brs_.skip_->deep_copy(*(child_brs->skip_), brs_.size_);
    if (child_brs->end_ && (0 == child_brs->size_)) {
      brs_.end_ = true;
      brs_.size_ = 0;
      iter_end_ = true;
    }
  }
  return ret;
}

int ObHashDistinctVecOp::process_state(const int64_t probe_cnt,
                                       const int64_t extend_bkt_num_push_down,
                                       ObAdaptiveByPassCtrl &bypass_ctrl,
                                       ObHashPartInfrastructureVecImpl &hp_infras, bool &can_insert)
{
  int64_t min_period_cnt = ObAdaptiveByPassCtrl::MIN_PERIOD_CNT;
  can_insert = true;
  int ret = OB_SUCCESS;
  if (ObAdaptiveByPassCtrl::STATE_L2_INSERT == bypass_ctrl.state_) {
    can_insert = true;
    if (hp_infras.get_actual_mem_used() > INIT_L2_CACHE_SIZE) {
      bypass_ctrl.period_cnt_ = std::max(hp_infras.get_hash_table_size(), min_period_cnt);
      bypass_ctrl.probe_cnt_ += probe_cnt;
      bypass_ctrl.state_ = ObAdaptiveByPassCtrl::STATE_ANALYZE;
    }
  } else if (ObAdaptiveByPassCtrl::STATE_L3_INSERT == bypass_ctrl.state_) {
    can_insert = true;
    if (hp_infras.get_actual_mem_used() > INIT_L3_CACHE_SIZE) {
      bypass_ctrl.period_cnt_ = std::max(hp_infras.get_hash_table_size(), min_period_cnt);
      bypass_ctrl.probe_cnt_ += probe_cnt;
      bypass_ctrl.state_ = ObAdaptiveByPassCtrl::STATE_ANALYZE;
    }
  } else if (ObAdaptiveByPassCtrl::STATE_PROBE == bypass_ctrl.state_) {
    can_insert = false;
    bypass_ctrl.probe_cnt_ += probe_cnt;
    if (bypass_ctrl.probe_cnt_ >= bypass_ctrl.period_cnt_) {
      bypass_ctrl.state_ = ObAdaptiveByPassCtrl::STATE_ANALYZE;
    }
  } else if (ObAdaptiveByPassCtrl::STATE_ANALYZE == bypass_ctrl.state_) {
    can_insert = false;
    double ratio = MIN_RATIO_FOR_L3;
    if (!(hp_infras.get_hash_bucket_num() >= extend_bkt_num_push_down)
        && static_cast<double> (bypass_ctrl.exists_cnt_) / bypass_ctrl.probe_cnt_ >=
                       std::max(ratio, 1 - (1 / static_cast<double> (bypass_ctrl.cut_ratio_)))) {
      bypass_ctrl.rebuild_times_ = 0;
      if (hp_infras.hash_table_full()) {
        bypass_ctrl.state_ = ObAdaptiveByPassCtrl::STATE_L3_INSERT;
        if (OB_FAIL(hp_infras.extend_hash_table_l3())) {
          LOG_WARN("failed to extend hash table", K(ret));
        }
      } else {
        bypass_ctrl.state_ = ObAdaptiveByPassCtrl::STATE_PROBE;
      }
    } else if (static_cast<double> (bypass_ctrl.exists_cnt_) / bypass_ctrl.probe_cnt_ >=
                                           1 - (1 / static_cast<double> (bypass_ctrl.cut_ratio_))) {
      bypass_ctrl.state_ = ObAdaptiveByPassCtrl::STATE_PROBE;
      bypass_ctrl.rebuild_times_ = 0;
    } else {
      LOG_TRACE("get new state", K(bypass_ctrl.state_), K(bypass_ctrl.processed_cnt_), K(hp_infras.get_hash_bucket_num()),
                              K(hp_infras.get_hash_table_size()), K(bypass_ctrl.exists_cnt_), K(bypass_ctrl.probe_cnt_));
      hp_infras.reset_hash_table_for_by_pass();
      bypass_ctrl.state_ = ObAdaptiveByPassCtrl::STATE_L2_INSERT;
      ++bypass_ctrl.rebuild_times_;
    }
    bypass_ctrl.probe_cnt_ = 0;
    bypass_ctrl.exists_cnt_ = 0;
  }
  return ret;
}

int ObHashDistinctVecOp::init_mem_context()
{
  int ret = OB_SUCCESS;
  if (NULL == mem_context_) {
    lib::ContextParam param;
    param.set_mem_attr(ctx_.get_my_session()->get_effective_tenant_id(),
        "ObHashDistRows",
        ObCtxIds::WORK_AREA);
    if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(mem_context_, param))) {
      LOG_WARN("memory entity create failed", K(ret));
    }
  }
  return ret;
}

int ObHashDistinctVecOp::do_group_distinct_for_batch(const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(build_group_distinct_data_for_batch(batch_size))) {
    LOG_WARN("failed to build group distinct data for batch", K(ret));
  } else if (OB_FAIL(read_group_distinct_data(batch_size))) {
    LOG_WARN("failed to read group distinct data", K(ret));
  }
  return ret;
}

int ObHashDistinctVecOp::init_group_hp_infras()
{
  int ret = OB_SUCCESS;
  void *mgr_buf = nullptr;
  int64_t est_rows = MY_SPEC.rows_;
  int64_t tenant_id = -1;
  if (OB_UNLIKELY(MY_SPEC.group_distinct_exprs_.count() <= 0)
      || OB_UNLIKELY(MY_SPEC.group_distinct_exprs_.count() != MY_SPEC.group_sort_collations_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid group distinct exprs count", K(ret));
  } else if (OB_ISNULL(ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid null session", K(ret));
  } else if (FALSE_IT(tenant_id = ctx_.get_my_session()->get_effective_tenant_id())) {
  } else if (OB_FAIL(ObPxEstimateSizeUtil::get_px_size(&ctx_, MY_SPEC.px_est_size_factor_, est_rows,
                                                       est_rows))) {
    LOG_WARN("failed to get px size", K(ret));
  } else if (OB_FAIL(sql_mem_processor_.init(&mem_context_->get_malloc_allocator(), tenant_id,
                                             est_rows * MY_SPEC.width_, MY_SPEC.type_, MY_SPEC.id_,
                                             &ctx_))) {
    LOG_WARN("failed to init sql mem processor", K(ret));
  } else if (OB_ISNULL(mgr_buf = ctx_.get_allocator().alloc(sizeof(ObHashPartInfrasVecMgr)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc mgr buf", K(ret));
  } else {
    hp_infras_mgr_ = new (mgr_buf) ObHashPartInfrasVecMgr(ctx_.get_my_session()->get_effective_tenant_id());
    if (OB_FAIL(hp_infras_mgr_->init(ctx_.get_my_session()->get_effective_tenant_id(),
                                     GCONF.is_sql_operator_dump_enabled(), est_rows, MY_SPEC.width_,
                                     true, 1, &eval_ctx_, &sql_mem_processor_, &io_event_observer_,
                                     MY_SPEC.compress_type_))) {
      LOG_WARN("failed to init hp infras mgr", K(ret));
    } else if (OB_FAIL(hp_infras_mgr_->reserve_hp_infras(MY_SPEC.group_distinct_exprs_.count()))) {
      LOG_WARN("failed to reserve hp infras", K(ret));
    } else if (OB_FAIL(hp_infras_arr_.init(MY_SPEC.group_distinct_exprs_.count()))) {
      LOG_WARN("failed to init hp infras arr", K(ret));
    }
    HashPartInfrasVec *hp_infras = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.group_distinct_exprs_.count(); i++) {
      if (OB_FAIL(hp_infras_mgr_->init_one_hp_infras(false, &(MY_SPEC.group_sort_collations_.at(i)),
                                                     MY_SPEC.group_distinct_exprs_.at(i),
                                                     hp_infras,
                                                     MY_SPEC.is_ordered_group_output()))) {
        LOG_WARN("failed to init one hp infras", K(ret));
      } else if (OB_FAIL(hp_infras_arr_.push_back(hp_infras))) {
        LOG_WARN("failed to push back hp infras", K(ret));
      } else {
        op_monitor_info_.otherstat_1_id_ = ObSqlMonitorStatIds::HASH_INIT_BUCKET_COUNT;
        op_monitor_info_.otherstat_1_value_ = max(op_monitor_info_.otherstat_1_value_, hp_infras->get_bucket_num());
      }
    }
    void *selector_buf = nullptr;
    int64_t selector_buf_size = ObBitVector::memory_size(MY_SPEC.max_batch_size_ * sizeof(int32_t));
    int64_t total_sel_size = selector_buf_size * MY_SPEC.group_distinct_exprs_.count();
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(selector_buf = ctx_.get_allocator().alloc(total_sel_size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc selector buf", K(ret));
    } else if (OB_FAIL(group_selector_arr_.init(MY_SPEC.group_distinct_exprs_.count()))) {
      LOG_WARN("failed to init group selector arr", K(ret));
    } else {
      MEMSET(selector_buf, 0, total_sel_size);
    }
    for(int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.group_distinct_exprs_.count(); i++) {
      char *selector_arr = (char *)selector_buf + i * selector_buf_size;
      if (OB_FAIL(group_selector_arr_.push_back(selector_arr))) {
        LOG_WARN("failed to push back selector arr", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      group_iter_idx_ = MY_SPEC.group_distinct_exprs_.count() - 1;
      min_stored_group_idx_ = MY_SPEC.group_distinct_exprs_.count();
      group_distinct_state_ = GroupDistinctState::OPEN_HASH_PART;
    }

    if (OB_SUCC(ret) && MY_SPEC.has_non_distinct_aggr_params_) {
      void *buf = nullptr;
      void *iter_buf = nullptr;
      lib::ObMemAttr mem_attr(ctx_.get_my_session()->get_effective_tenant_id(), "NonAggrStore", ObCtxIds::WORK_AREA);
      if (OB_ISNULL(buf = ctx_.get_allocator().alloc(sizeof(ObTempColumnStore)))
          || OB_ISNULL(iter_buf = ctx_.get_allocator().alloc(sizeof(ObTempColumnStore::Iterator)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc temp column store", K(ret));
      } else {
        non_distinct_aggr_params_store_ = new (buf) ObTempColumnStore(&mem_context_->get_malloc_allocator());
        non_distinct_aggr_params_iter_ = new (iter_buf) ObTempColumnStore::Iterator();
        if (OB_FAIL(non_distinct_aggr_params_store_->init(child_->get_spec().output_,
                                                          MY_SPEC.max_batch_size_, mem_attr,
                                                          0, true, true, ObCompressorType::NONE_COMPRESSOR))) {
          LOG_WARN("failed to init non distinct aggr params store", K(ret));
        } else if (OB_FAIL(non_distinct_aggr_params_iter_->init(non_distinct_aggr_params_store_))) {
          LOG_WARN("failed to init non distinct aggr params iter", K(ret));
        } else {
          non_distinct_aggr_params_store_->set_allocator(mem_context_->get_malloc_allocator());
          non_distinct_aggr_params_store_->set_callback(&sql_mem_processor_);
          non_distinct_aggr_params_store_->set_io_event_observer(&io_event_observer_);
          non_distinct_aggr_params_store_->set_dir_id(sql_mem_processor_.get_dir_id());
        }
      }
    }
  }
  return ret;
}

int ObHashDistinctVecOp::build_group_distinct_data_for_batch(const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  const ObBatchRows *child_brs = nullptr;
  int64_t max_row_cnt = MIN(batch_size, MY_SPEC.max_batch_size_);
  int64_t min_group = 0, max_group = 0;
  VectorFormat group_id_fmt = VEC_INVALID;
  if (OB_ISNULL(MY_SPEC.grouping_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid null grouping id", K(ret));
  }
  while (OB_SUCC(ret) && !child_op_is_end_ && group_iter_idx_ <= min_stored_group_idx_) {
    clear_evaluated_flag();
    if (OB_FAIL(try_check_status())) {
      LOG_WARN("failed to try check status", K(ret));
    } else if (OB_FAIL(child_->get_next_batch(max_row_cnt, child_brs))) {
      LOG_WARN("failed to get next batch", K(ret));
    } else if (FALSE_IT(child_op_is_end_ = child_brs->end_)) {
    } else if (child_brs->size_ > 0 && child_brs->skip_->accumulate_bit_cnt(child_brs->size_) < child_brs->size_) {
      if (OB_FAIL(MY_SPEC.grouping_id_->eval_vector(eval_ctx_, *child_brs))) {
        LOG_WARN("failed to eval grouping id", K(ret));
      } else if (FALSE_IT(group_id_fmt = MY_SPEC.grouping_id_->get_format(eval_ctx_))) {
      } else if (group_id_fmt == VEC_UNIFORM) {
        ret = group_child_input<ObUniformFormat<false>>(
          *child_brs, MY_SPEC.grouping_id_->get_vector(eval_ctx_), min_group, max_group);
      } else if (group_id_fmt == VEC_FIXED) {
        ret = group_child_input<ObFixedLengthFormat<RTCType<VEC_TC_INTEGER>>>(
          *child_brs, MY_SPEC.grouping_id_->get_vector(eval_ctx_), min_group, max_group);
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid group id format", K(ret), K(group_id_fmt));
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(insert_group_distinct_data(*child_brs, min_group, max_group))) {
        LOG_WARN("failed to insert group distinct data", K(ret));
      } else if (MY_SPEC.is_ordered_group_output()) {
        min_stored_group_idx_ = min_group;
      }
    }
  }
  if (OB_SUCC(ret) && (!MY_SPEC.is_ordered_group_output() || child_op_is_end_)) {
    min_stored_group_idx_ = -1;
  }
  LOG_TRACE("after building group distinct data", K(group_iter_idx_), K(min_stored_group_idx_),
            K(child_op_is_end_), K(group_distinct_state_));
  return ret;
}

int ObHashDistinctVecOp::read_group_distinct_data(int64_t batch_size)
{
  int ret = OB_SUCCESS;
  int64_t max_row_cnt = MIN(batch_size, MY_SPEC.max_batch_size_);
  if (OB_UNLIKELY(group_iter_idx_ < 0)) {
    // iter end
    brs_.end_ = true;
    brs_.size_ = 0;
    iter_end_ = true;
  } else {
    bool output_data = false;
    while (OB_SUCC(ret) && !output_data && group_iter_idx_ > min_stored_group_idx_) {
      HashPartInfrasVec *hp_infras = hp_infras_arr_.at(group_iter_idx_);
      switch (group_distinct_state_) {
      case GroupDistinctState::OPEN_HASH_PART: {
        if (group_iter_idx_ == 0 && MY_SPEC.has_non_distinct_aggr_params_) {
          // read from non distinct aggr params store
          group_distinct_state_ = GroupDistinctState::READ_ROWS;
        } else if (OB_FAIL(hp_infras->finish_insert_row())) {
          LOG_WARN("failed to finish insert row", K(ret));
        } else if (OB_FAIL(hp_infras->open_hash_table_part())) {
          LOG_WARN("failed to open hash table part", K(ret));
        } else {
          group_distinct_state_ = GroupDistinctState::READ_ROWS;
        }
        break;
      }
      case GroupDistinctState::READ_ROWS: {
        if (group_iter_idx_ == 0 && MY_SPEC.has_non_distinct_aggr_params_) {
          int64_t read_rows = 0;
          ret = non_distinct_aggr_params_iter_->get_next_batch(child_->get_spec().output_,
                                                               eval_ctx_, max_row_cnt, read_rows);
          if (OB_SUCC(ret)) {
            group_cnt_ += read_rows;
            brs_.size_ = read_rows;
            brs_.all_rows_active_ = true;
            brs_.skip_->reset(read_rows);
          }
        } else {
          ret = read_group_distinct_data(max_row_cnt, hp_infras);
        }
        if (ret == OB_ITER_END) {
          ret = OB_SUCCESS;
          group_distinct_state_ = GroupDistinctState::ITER_END;
          if (OB_FAIL(hp_infras_mgr_->free_one_hp_infras(hp_infras, true))) {
            LOG_WARN("failed to free hp infras", K(ret));
          }
          if (OB_SUCC(ret) && group_iter_idx_ == 0 && non_distinct_aggr_params_iter_ != nullptr) {
            non_distinct_aggr_params_iter_->~Iterator();
            non_distinct_aggr_params_store_->~ObTempColumnStore();
          }
        } else if (OB_FAIL(ret)) {
          LOG_WARN("failed to read group distinct data", K(ret));
        } else {
          output_data = true;
        }
        break;
      }
      case GroupDistinctState::ITER_END: {
        if (group_iter_idx_ > 0 || (group_iter_idx_ == 0 && !MY_SPEC.has_non_distinct_aggr_params_)) {
          HashPartInfrasVec *hp_infras = hp_infras_arr_.at(group_iter_idx_);
          if (OB_ISNULL(hp_infras)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid null hp infras", K(ret));
          } else {
            group_distinct_bucket_cnt_ += hp_infras->get_bucket_num();
          }
        }
        group_iter_idx_--;
        if (OB_FAIL(ret)) {
        } else if (group_iter_idx_ > min_stored_group_idx_) {
          group_distinct_state_ = GroupDistinctState::OPEN_HASH_PART;
        } else if (min_stored_group_idx_ < 0) {
          brs_.end_ = true;
          brs_.size_ = 0;
          output_data = true;
        }
        break;
      }
      }
    }
    if (OB_SUCC(ret) && !brs_.end_ && !output_data
        && GroupDistinctState::ITER_END == group_distinct_state_
        && min_stored_group_idx_ >= 0) {
      // continue to read data from child
      group_distinct_state_ = GroupDistinctState::OPEN_HASH_PART;
      if (OB_FAIL(SMART_CALL(do_group_distinct_for_batch(batch_size)))) {
        LOG_WARN("failed to do group distinct for batch", K(ret));
      }
    }
  }
  return ret;
}

int ObHashDistinctVecOp::read_group_distinct_data(const int64_t max_row_cnt, HashPartInfrasVec *hp_infras)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObExpr *> &exprs = MY_SPEC.group_distinct_exprs_.at(group_iter_idx_);
  int64_t read_rows = 0;
  if (OB_FAIL(hp_infras->get_next_hash_table_batch(exprs, max_row_cnt, read_rows, nullptr))) {
    if (ret == OB_ITER_END) {
      if (OB_FAIL(hp_infras->end_round())) {
        LOG_WARN("failed to end round", K(ret));
      } else if (OB_FAIL(hp_infras->start_round())) {
        LOG_WARN("failed to open round", K(ret));
      } else if (OB_FAIL(hp_infras->get_next_partition(InputSide::LEFT))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to get dumped partitions", K(ret));
        }
      } else if (OB_FAIL(hp_infras->open_cur_part(InputSide::LEFT))) {
        LOG_WARN("failed to open cur part", K(ret));
      } else if (OB_FAIL(hp_infras->resize(hp_infras->get_cur_part_row_cnt(InputSide::LEFT)))) {
        LOG_WARN("failed to init hash table", K(ret));
      } else if (OB_FAIL(insert_group_distinct_data_from_dump_part(hp_infras))) {
        if (ret == OB_ITER_END) {
          ret = OB_ERR_UNEXPECTED;
        }
        LOG_WARN("failed to insert group distinct data from dump part", K(ret));
      } else if (OB_FAIL(hp_infras->open_hash_table_part())) {
        LOG_WARN("failed to open hash table part", K(ret));
      } else if (OB_FAIL(SMART_CALL(read_group_distinct_data(max_row_cnt, hp_infras)))) {
        LOG_WARN("failed to read group distinct data", K(ret));
      }
    } else {
      LOG_WARN("failed to get next hash table batch", K(ret));
    }
  } else if (OB_FAIL(setup_null_expr_and_grouping_id(read_rows))) {
    LOG_WARN("failed to setup null expr and grouping id", K(ret));
  } else {
    group_cnt_ += read_rows;
    brs_.size_ = read_rows;
    brs_.all_rows_active_ = true;
    brs_.skip_->reset(read_rows);
  }
  return ret;
}

int ObHashDistinctVecOp::setup_null_expr_and_grouping_id(const int64_t row_cnt)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObExpr *> &distinct_set = MY_SPEC.group_distinct_exprs_.at(group_iter_idx_);
  ObExpr *grouping_expr = MY_SPEC.grouping_id_;
  for (int i = 0; OB_SUCC(ret) && i < MY_SPEC.distinct_exprs_.count(); i++) {
    ObExpr *expr = MY_SPEC.distinct_exprs_.at(i);
    if (has_exist_in_array(distinct_set, expr) || expr == grouping_expr) {
      // do nothing
    } else if (OB_FAIL(expr->init_vector_default(eval_ctx_, row_cnt))) {
      LOG_WARN("init vector for write failed", K(ret));
    } else {
      ObBitmapNullVectorBase *null_vec = static_cast<ObBitmapNullVectorBase *>(expr->get_vector(eval_ctx_));
      null_vec->set_has_null(true);
      null_vec->get_nulls()->set_all(row_cnt);
      expr->set_evaluated_projected(eval_ctx_);
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(grouping_expr->init_vector_for_write(
               eval_ctx_, grouping_expr->get_default_res_format(), row_cnt))) {
    LOG_WARN("failed to init vector for write", K(ret));
  } else if (OB_UNLIKELY(grouping_expr->get_format(eval_ctx_) != VEC_FIXED
                         || !ob_is_integer_type(grouping_expr->datum_meta_.type_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid grouping id format", K(ret), K(grouping_expr->datum_meta_));
  } else {
    ObFixedLengthFormat<RTCType<VEC_TC_INTEGER>> *grouping_vec =
      static_cast<ObFixedLengthFormat<RTCType<VEC_TC_INTEGER>> *>(grouping_expr->get_vector(eval_ctx_));
    for (int64_t i = 0; OB_SUCC(ret) && i < row_cnt; i++) {
      grouping_vec->set_int(i, group_iter_idx_);
    }
    if (OB_SUCC(ret)) {
      grouping_expr->set_evaluated_projected(eval_ctx_);
    }
  }
  return ret;
}

int ObHashDistinctVecOp::insert_group_distinct_data_from_dump_part(ObHashPartInfrastructureVecImpl *hp_infras)
{
  int ret = OB_SUCCESS;
  int64_t read_rows = 0;
  ObBitVector *output_bit = nullptr;
  const ObIArray<ObExpr *> &exprs = MY_SPEC.group_distinct_exprs_.at(group_iter_idx_);
  const int64_t batch_size = MY_SPEC.max_batch_size_;
  while (OB_SUCC(ret)) {
    ret = hp_infras->get_left_next_batch(exprs, batch_size, read_rows, hash_values_for_batch_);
    if (ret == OB_ITER_END) {
      ret = OB_SUCCESS;
      if (OB_FAIL(hp_infras->finish_insert_row())) {
        LOG_WARN("failed to finish insert row", K(ret));
      } else if (OB_FAIL(hp_infras->close_cur_part(InputSide::LEFT))) {
        LOG_WARN("failed to close cur part", K(ret));
      } else {
        LOG_TRACE("finish one dumped partition");
        break;
      }
    } else if (OB_FAIL(ret)) {
      LOG_WARN("failed to get left next batch", K(ret));
    } else if (OB_FAIL(try_check_status())) {
      LOG_WARN("failed to try check status", K(ret));
    } else if (OB_FAIL(hp_infras->insert_row_for_batch(exprs, hash_values_for_batch_, read_rows,
                                                       nullptr, output_bit))) {
      LOG_WARN("failed to insert row for batch", K(ret));
    }
  }
  return ret;
}

template <typename ColumnFmt>
int ObHashDistinctVecOp::group_child_input(const ObBatchRows &child_brs, ObIVector *grouping_id_vec,
                                           int64_t &min_group, int64_t &max_group)
{
  int ret = OB_SUCCESS;
  ColumnFmt *grouping_id_col = static_cast<ColumnFmt *>(grouping_id_vec);
  bool finished = false;
  if (MY_SPEC.is_ordered_group_output()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < child_brs.size_; i++) {
      if (child_brs.skip_->at(i) || grouping_id_col->is_null(i)) { continue; }
      min_group = grouping_id_col->get_int(i);
      break;
    }
    for (int64_t i = child_brs.size_ - 1; OB_SUCC(ret) && i >= 0; i--) {
      if (child_brs.skip_->at(i) || grouping_id_col->is_null(i)) { continue; }
      max_group = grouping_id_col->get_int(i);
      break;
    }
    if (OB_UNLIKELY(min_group < 0
      || min_group >= group_selector_arr_.count()
      || max_group < 0
      || max_group >= group_selector_arr_.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid group id", K(ret), K(min_group), K(max_group),K(group_selector_arr_.count()));
    } else if (min_group == max_group) {
      to_bit_vector(group_selector_arr_.at(min_group))->deep_copy(*child_brs.skip_, child_brs.size_);
      finished = true;
    }
  }
  if (OB_SUCC(ret) && !finished) {
    int64_t select_buf_size = MY_SPEC.max_batch_size_ * sizeof(int32_t);
    for (int i = 0; OB_SUCC(ret) && i < group_selector_arr_.count(); i++) {
      ObBitVector *selector = to_bit_vector(group_selector_arr_.at(i));
      selector->set_all(child_brs.size_); // all set to 1
    }
    for (int i = 0; OB_SUCC(ret) && i < child_brs.size_; i++) {
      if (child_brs.skip_->at(i) || grouping_id_col->is_null(i)) { continue; }
      int64_t group_id = grouping_id_col->get_int(i);
      if (OB_UNLIKELY(group_id < 0 || group_id >= group_selector_arr_.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid group id", K(ret), K(group_id), K(group_selector_arr_.count()));
      } else {
        to_bit_vector(group_selector_arr_.at(group_id))->unset(i);
        min_group = MIN(min_group, group_id);
        max_group = MAX(max_group, group_id);
      }
    }
  }
  LOG_DEBUG("group child input", K(ret), K(min_group), K(max_group));
  return ret;
}

int ObHashDistinctVecOp::insert_group_distinct_data(const ObBatchRows &child_brs,
                                                    const int64_t group_start,
                                                    const int64_t group_end)
{
  int ret = OB_SUCCESS;
  ObBitVector *output_vec = nullptr;
  if (OB_UNLIKELY(group_start < 0 || group_end >= group_selector_arr_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid group start or end", K(ret), K(group_start), K(group_end), K(group_selector_arr_.count()));
  }
  for (int64_t i = group_start; OB_SUCC(ret) && i <= group_end; i++) {
    HashPartInfrasVec *hp_infras = hp_infras_arr_.at(i);
    const ObIArray<ObExpr *> &exprs = MY_SPEC.group_distinct_exprs_.at(i);
    ObBitVector *my_skip = to_bit_vector(group_selector_arr_.at(i));
    bool all_rows_active = my_skip->accumulate_bit_cnt(child_brs.size_) == 0;
    if (i == 0 && MY_SPEC.has_non_distinct_aggr_params_) {
      // non aggr group,
      ObBatchRows tmp_brs(*my_skip, child_brs.size_, all_rows_active);
      int64_t inserted_rows = 0;
      if (OB_FAIL(process_non_distinct_store_dump())) {
        LOG_WARN("failed to process non distinct store dump", K(ret));
      } else if (OB_FAIL(non_distinct_aggr_params_store_->add_batch(child_->get_spec().output_,
                                                             eval_ctx_, tmp_brs, inserted_rows))) {
        LOG_WARN("failed to add batch", K(ret));
      } else {
      }
    } else if (OB_ISNULL(hp_infras)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid null hp infras", K(ret));
    } else if (OB_FAIL(hp_infras->calc_hash_value_for_batch(
                 exprs, *my_skip, child_brs.size_, all_rows_active, hash_values_for_batch_))) {
      LOG_WARN("failed to calc hash value for batch", K(ret));
    } else if (OB_FAIL(hp_infras->insert_row_for_batch(exprs, hash_values_for_batch_,
                                                       child_brs.size_, my_skip, output_vec))) {
      LOG_WARN("failed to insert row for batch", K(ret));
    } else {
      // do nothing
    }
  }
  return ret;
}

int64_t ObHashDistinctVecOp::get_hash_bucket_num() const
{
  int64_t buck_num = 0;
  if (MY_SPEC.group_distinct_exprs_.count() > 0) {
    if (group_iter_idx_ < 0) {
      buck_num = 0;
    } else if (!MY_SPEC.has_non_distinct_aggr_params_ || group_iter_idx_ < MY_SPEC.group_distinct_exprs_.count()) {
      buck_num = hp_infras_arr_.at(group_iter_idx_)->get_bucket_num();
    }
  } else {
    buck_num = hp_infras_.get_bucket_num();
  }
  return buck_num;
}

bool ObHashDistinctVecOp::need_dump_non_distinct_store()
{
  bool ret = false;
  int64_t mem_bound = MY_SPEC.is_ordered_group_output() ?
                        sql_mem_processor_.get_mem_bound() :
                        sql_mem_processor_.get_mem_bound() / MY_SPEC.group_distinct_exprs_.count();
  if (non_distinct_aggr_params_store_->get_mem_hold() > mem_bound) {
    ret = true;
  }
  return ret;
}

int ObHashDistinctVecOp::process_non_distinct_store_dump()
{
  int ret = OB_SUCCESS;
  bool updated = false;
  bool need_dump = false;
  if (OB_FAIL(sql_mem_processor_.update_max_available_mem_size_periodically(
        &mem_context_->get_malloc_allocator(),
        ObSqlMemMgrProcessor::DefaultPeriodicCheckOp<ObTempColumnStore>(*non_distinct_aggr_params_store_),
        updated))) {
    LOG_WARN("failed to update max available memory size periodically", K(ret));
  } else if (need_dump_non_distinct_store() && GCONF.is_sql_operator_dump_enabled()
             && OB_FAIL(sql_mem_processor_.extend_max_memory_size(
               &mem_context_->get_malloc_allocator(),
               NonDistinctStoreDumpCheckOp(*this),
               need_dump,
               sql_mem_processor_.get_data_size() / (MY_SPEC.is_ordered_group_output() ? 1 : MY_SPEC.group_distinct_exprs_.count())))) {
    LOG_WARN("failed to extend max memory size", K(ret));
  } else if (need_dump) {
    if (OB_FAIL(non_distinct_aggr_params_store_->dump(false))) {
      LOG_WARN("failed to dump non distinct aggr params store", K(ret));
    } else {
      sql_mem_processor_.set_number_pass(1);
      LOG_TRACE("trace non distinct aggr params store dump", K(sql_mem_processor_.get_data_size()),
                K(non_distinct_aggr_params_store_->get_row_cnt_in_memory()),
                K(sql_mem_processor_.get_mem_bound()), K(MY_SPEC.is_ordered_group_output()),
                K(MY_SPEC.group_distinct_exprs_.count()));
    }
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase
