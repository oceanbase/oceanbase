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
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/ob_exec_context.h"

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
  is_push_down_(false)
{}

OB_SERIALIZE_MEMBER((ObHashDistinctVecSpec, ObOpSpec),
                    distinct_exprs_,
                    sort_collations_,
                    is_block_mode_,
                    by_pass_enabled_,
                    is_push_down_);

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
    mem_context_(NULL)
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
                                  K(spec_.id_), K(MY_SPEC.is_push_down_));
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
  hp_infras_.reset();
  bypass_ctrl_.reset();
  if (MY_SPEC.is_block_mode_) {
    get_next_batch_func_ = &ObHashDistinctVecOp::do_block_distinct_for_batch;
  } else {
    get_next_batch_func_ = &ObHashDistinctVecOp::do_unblock_distinct_for_batch;
  }
  LOG_TRACE("trace block mode", K(MY_SPEC.is_block_mode_), K(spec_.id_));
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
      if (OB_FAIL(process_state(add_cnt, can_insert))) {
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
    op_monitor_info_.otherstat_3_value_ = hp_infras_.get_bucket_num();
    op_monitor_info_.otherstat_3_id_ = ObSqlMonitorStatIds::HASH_BUCKET_COUNT;
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
    brs_.skip_->deep_copy(*(child_brs->skip_), batch_size);
    if (child_brs->end_ && (0 == child_brs->size_)) {
      brs_.end_ = true;
      brs_.size_ = 0;
      iter_end_ = true;
    }
  }
  return ret;
}

int ObHashDistinctVecOp::process_state(int64_t probe_cnt, bool &can_insert)
{
  int64_t min_period_cnt = ObAdaptiveByPassCtrl::MIN_PERIOD_CNT;
  can_insert = true;
  int ret = OB_SUCCESS;
  if (ObAdaptiveByPassCtrl::STATE_L2_INSERT == bypass_ctrl_.state_) {
    can_insert = true;
    if (hp_infras_.get_actual_mem_used() > INIT_L2_CACHE_SIZE) {
      bypass_ctrl_.period_cnt_ = std::max(hp_infras_.get_hash_table_size(), min_period_cnt);
      bypass_ctrl_.probe_cnt_ += probe_cnt;
      bypass_ctrl_.state_ = ObAdaptiveByPassCtrl::STATE_ANALYZE;
    }
  } else if (ObAdaptiveByPassCtrl::STATE_L3_INSERT == bypass_ctrl_.state_) {
    can_insert = true;
    if (hp_infras_.get_actual_mem_used() > INIT_L3_CACHE_SIZE) {
      bypass_ctrl_.period_cnt_ = std::max(hp_infras_.get_hash_table_size(), min_period_cnt);
      bypass_ctrl_.probe_cnt_ += probe_cnt;
      bypass_ctrl_.state_ = ObAdaptiveByPassCtrl::STATE_ANALYZE;
    }
  } else if (ObAdaptiveByPassCtrl::STATE_PROBE == bypass_ctrl_.state_) {
    can_insert = false;
    bypass_ctrl_.probe_cnt_ += probe_cnt;
    if (bypass_ctrl_.probe_cnt_ >= bypass_ctrl_.period_cnt_) {
      bypass_ctrl_.state_ = ObAdaptiveByPassCtrl::STATE_ANALYZE;
    }
  } else if (ObAdaptiveByPassCtrl::STATE_ANALYZE == bypass_ctrl_.state_) {
    can_insert = false;
    double ratio = MIN_RATIO_FOR_L3;
    if (!(hp_infras_.get_hash_bucket_num() >= extend_bkt_num_push_down_)
        && static_cast<double> (bypass_ctrl_.exists_cnt_) / bypass_ctrl_.probe_cnt_ >=
                       std::max(ratio, 1 - (1 / static_cast<double> (bypass_ctrl_.cut_ratio_)))) {
      bypass_ctrl_.rebuild_times_ = 0;
      if (hp_infras_.hash_table_full()) {
        bypass_ctrl_.state_ = ObAdaptiveByPassCtrl::STATE_L3_INSERT;
        if (OB_FAIL(hp_infras_.extend_hash_table_l3())) {
          LOG_WARN("failed to extend hash table", K(ret));
        }
      } else {
        bypass_ctrl_.state_ = ObAdaptiveByPassCtrl::STATE_PROBE;
      }
    } else if (static_cast<double> (bypass_ctrl_.exists_cnt_) / bypass_ctrl_.probe_cnt_ >=
                                           1 - (1 / static_cast<double> (bypass_ctrl_.cut_ratio_))) {
      bypass_ctrl_.state_ = ObAdaptiveByPassCtrl::STATE_PROBE;
      bypass_ctrl_.rebuild_times_ = 0;
    } else {
      LOG_TRACE("get new state", K(bypass_ctrl_.state_), K(bypass_ctrl_.processed_cnt_), K(hp_infras_.get_hash_bucket_num()),
                              K(hp_infras_.get_hash_table_size()), K(bypass_ctrl_.exists_cnt_), K(bypass_ctrl_.probe_cnt_));
      hp_infras_.reset_hash_table_for_by_pass();
      bypass_ctrl_.state_ = ObAdaptiveByPassCtrl::STATE_L2_INSERT;
      ++bypass_ctrl_.rebuild_times_;
    }
    bypass_ctrl_.probe_cnt_ = 0;
    bypass_ctrl_.exists_cnt_ = 0;
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

} // end namespace sql
} // end namespace oceanbase
