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

#include "sql/engine/aggregate/ob_hash_distinct_op.h"
#include "sql/engine/px/ob_px_util.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase {
using namespace common;
namespace sql {

ObHashDistinctSpec::ObHashDistinctSpec(ObIAllocator& alloc, const ObPhyOperatorType type)
    : ObDistinctSpec(alloc, type), sort_collations_(alloc), hash_funcs_(alloc)
{}

OB_SERIALIZE_MEMBER((ObHashDistinctSpec, ObDistinctSpec), sort_collations_, hash_funcs_);

ObHashDistinctOp::ObHashDistinctOp(ObExecContext& exec_ctx, const ObOpSpec& spec, ObOpInput* input)
    : ObOperator(exec_ctx, spec, input),
      first_got_row_(true),
      has_got_part_(false),
      iter_end_(false),
      get_next_row_func_(nullptr),
      profile_(ObSqlWorkAreaType::HASH_WORK_AREA),
      sql_mem_processor_(profile_),
      hp_infras_(),
      group_cnt_(0)
{
  enable_sql_dumped_ = GCONF.is_sql_operator_dump_enabled() && !(GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_2250);
}

int ObHashDistinctOp::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == left_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: left is null", K(ret));
  } else if (OB_FAIL(ObOperator::inner_open())) {
    LOG_WARN("failed to inner open", K(ret));
  } else {
    first_got_row_ = true;
    if (MY_SPEC.is_block_mode_) {
      get_next_row_func_ = &ObHashDistinctOp::do_block_distinct;
    } else {
      get_next_row_func_ = &ObHashDistinctOp::do_unblock_distinct;
    }
    LOG_TRACE("trace block mode", K(MY_SPEC.is_block_mode_), K(spec_.id_));
  }
  return ret;
}

int ObHashDistinctOp::inner_close()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOperator::inner_close())) {
    LOG_WARN("failed to inner close", K(ret));
  } else {
    reset();
    sql_mem_processor_.unregister_profile();
  }
  return ret;
}

void ObHashDistinctOp::reset()
{
  first_got_row_ = true;
  has_got_part_ = false;
  group_cnt_ = 0;
  hp_infras_.reset();
  if (MY_SPEC.is_block_mode_) {
    get_next_row_func_ = &ObHashDistinctOp::do_block_distinct;
  } else {
    get_next_row_func_ = &ObHashDistinctOp::do_unblock_distinct;
  }
  LOG_TRACE("trace block mode", K(MY_SPEC.is_block_mode_), K(spec_.id_));
}

int ObHashDistinctOp::rescan()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOperator::rescan())) {
    LOG_WARN("failed to rescan child operator", K(ret));
  } else {
    reset();
    iter_end_ = false;
  }
  return ret;
}

void ObHashDistinctOp::destroy()
{
  hp_infras_.~ObHashPartInfrastructure();
  ObOperator::destroy();
}

int ObHashDistinctOp::init_hash_partition_infras()
{
  int ret = OB_SUCCESS;
  int64_t est_rows = MY_SPEC.rows_;
  if (OB_FAIL(ObPxEstimateSizeUtil::get_px_size(&ctx_, MY_SPEC.px_est_size_factor_, est_rows, est_rows))) {
    LOG_WARN("failed to get px size", K(ret));
  } else if (OB_FAIL(sql_mem_processor_.init(&ctx_.get_allocator(),
                 ctx_.get_my_session()->get_effective_tenant_id(),
                 est_rows * MY_SPEC.width_,
                 MY_SPEC.type_,
                 MY_SPEC.id_,
                 &ctx_))) {
    LOG_WARN("failed to init sql mem processor", K(ret));
  } else if (OB_FAIL(hp_infras_.init(ctx_.get_my_session()->get_effective_tenant_id(),
                 enable_sql_dumped_,
                 true,
                 true,
                 2,
                 &sql_mem_processor_))) {
    LOG_WARN("failed to init hash partition infrastructure", K(ret));
  } else {
    int64_t est_bucket_num = hp_infras_.est_bucket_count(est_rows, MY_SPEC.width_, MIN_BUCKET_COUNT, MAX_BUCKET_COUNT);
    if (OB_FAIL(
            hp_infras_.set_funcs(&MY_SPEC.hash_funcs_, &MY_SPEC.sort_collations_, &MY_SPEC.cmp_funcs_, &eval_ctx_))) {
      LOG_WARN("failed to set funcs", K(ret));
    } else if (OB_FAIL(hp_infras_.start_round())) {
      LOG_WARN("failed to start round", K(ret));
    } else if (OB_FAIL(hp_infras_.init_hash_table(est_bucket_num, MIN_BUCKET_COUNT, MAX_BUCKET_COUNT))) {
      LOG_WARN("failed to init hash table", K(ret));
    } else {
      op_monitor_info_.otherstat_1_value_ = est_bucket_num;
      op_monitor_info_.otherstat_1_id_ = ObSqlMonitorStatIds::HASH_INIT_BUCKET_COUNT;
      op_monitor_info_.otherstat_4_value_ = MY_SPEC.is_block_mode_;
      op_monitor_info_.otherstat_4_id_ = ObSqlMonitorStatIds::DISTINCT_BLOCK_MODE;
    }
  }
  return ret;
}

int ObHashDistinctOp::build_distinct_data(bool is_block)
{
  int ret = OB_SUCCESS;
  bool got_row = false;
  bool has_exists = false;
  bool inserted = false;
  const ObChunkDatumStore::StoredRow* store_row = nullptr;
  while (OB_SUCC(ret) && !got_row) {
    if (!has_got_part_) {
      clear_evaluated_flag();
      ret = child_->get_next_row();
    } else {
      ret = hp_infras_.get_left_next_row(store_row, MY_SPEC.distinct_exprs_);
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      // get dumped partition
      if (OB_FAIL(hp_infras_.finish_insert_row())) {
        LOG_WARN("failed to finish to insert row", K(ret));
      } else if (!has_got_part_) {
        has_got_part_ = true;
      } else {
        if (OB_FAIL(hp_infras_.close_cur_part(InputSide::LEFT))) {
          LOG_WARN("failed to close cur part", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (is_block) {
        // return data after handling this batch of data in block mode.
        LOG_TRACE("trace block", K(is_block));
        break;
      } else if (OB_FAIL(hp_infras_.end_round())) {
        LOG_WARN("failed to end round", K(ret));
      } else if (OB_FAIL(hp_infras_.start_round())) {
        LOG_WARN("failed to open round", K(ret));
      } else if (OB_FAIL(hp_infras_.get_next_partition(InputSide::LEFT))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to create dumped partitions", K(ret));
        }
      } else if (OB_FAIL(hp_infras_.open_cur_part(InputSide::LEFT))) {
        LOG_WARN("failed to open cur part");
      } else if (OB_FAIL(hp_infras_.resize(hp_infras_.get_cur_part_row_cnt(InputSide::LEFT)))) {
        LOG_WARN("failed to init hash table", K(ret));
      }
    } else if (OB_FAIL(ret)) {
    } else if (OB_FAIL(try_check_status())) {
      LOG_WARN("failed to check status", K(ret));
    } else if (OB_FAIL(hp_infras_.insert_row(MY_SPEC.distinct_exprs_, has_exists, inserted))) {
      LOG_WARN("failed to insert row", K(ret));
    } else if (has_exists) {
      // Already in hash map, do nothing
    } else if (inserted && !is_block) {
      got_row = true;
      ++group_cnt_;
    }
  }  // end of while
  return ret;
}

int ObHashDistinctOp::do_unblock_distinct()
{
  int ret = OB_SUCCESS;
  if (first_got_row_) {
    if (OB_FAIL(init_hash_partition_infras())) {
      LOG_WARN("failed to get next row", K(ret));
    }
    first_got_row_ = false;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(build_distinct_data(false))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to build distinct data", K(ret));
    } else {
      LOG_TRACE("trace iter end", K(ret));
    }
  }
  return ret;
}

int ObHashDistinctOp::do_block_distinct()
{
  int ret = OB_SUCCESS;
  const ObChunkDatumStore::StoredRow* store_row = nullptr;
  if (first_got_row_) {
    if (OB_FAIL(init_hash_partition_infras())) {
      LOG_WARN("failed to get next row", K(ret));
    } else if (OB_FAIL(build_distinct_data(true))) {
      if (OB_ITER_END == ret) {
        ret = OB_ERR_UNEXPECTED;
      }
      LOG_WARN("failed to build distinct data", K(ret));
    } else if (OB_FAIL(hp_infras_.open_hash_table_part())) {
      LOG_WARN("failed to open hash table part", K(ret));
    }
    clear_evaluated_flag();
    first_got_row_ = false;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(hp_infras_.get_next_hash_table_row(store_row, &MY_SPEC.distinct_exprs_))) {
    if (OB_ITER_END == ret) {
      // begin to handle next batch data after return the previous batch.
      if (OB_FAIL(hp_infras_.end_round())) {
        LOG_WARN("failed to end round", K(ret));
      } else if (OB_FAIL(hp_infras_.start_round())) {
        LOG_WARN("failed to open round", K(ret));
      } else if (OB_FAIL(hp_infras_.get_next_partition(InputSide::LEFT))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to create dumped partitions", K(ret));
        }
      } else if (OB_FAIL(hp_infras_.open_cur_part(InputSide::LEFT))) {
        LOG_WARN("failed to open cur part");
      } else if (OB_FAIL(hp_infras_.resize(hp_infras_.get_cur_part_row_cnt(InputSide::LEFT)))) {
        LOG_WARN("failed to init hash table", K(ret));
      } else if (OB_FAIL(build_distinct_data(true))) {
        if (OB_ITER_END == ret) {
          ret = OB_ERR_UNEXPECTED;
        }
        LOG_WARN("failed to build distinct data", K(ret));
      } else if (OB_FAIL(hp_infras_.open_hash_table_part())) {
        LOG_WARN("failed to open hash table part", K(ret));
      } else if (OB_FAIL(hp_infras_.get_next_hash_table_row(store_row, &MY_SPEC.distinct_exprs_))) {
        LOG_WARN("failed to get next row in hash table", K(ret));
      }
    } else {
      LOG_WARN("failed to get next row in hash table", K(ret));
    }
  } else {
    ++group_cnt_;
  }
  return ret;
}

int ObHashDistinctOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  if (OB_UNLIKELY(iter_end_)) {
    ret = OB_ITER_END;
  } else if (OB_UNLIKELY(nullptr == get_next_row_func_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get next row func is null", K(ret));
  } else if (OB_FAIL((this->*get_next_row_func_)())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get next row", K(ret));
    } else {
      op_monitor_info_.otherstat_2_value_ = group_cnt_;
      op_monitor_info_.otherstat_2_id_ = ObSqlMonitorStatIds::HASH_ROW_COUNT;
      op_monitor_info_.otherstat_3_value_ = hp_infras_.get_bucket_num();
      op_monitor_info_.otherstat_3_id_ = ObSqlMonitorStatIds::HASH_BUCKET_COUNT;
      reset();
      iter_end_ = true;
    }
  } else {
    LOG_DEBUG("trace output row", K(ROWEXPR2STR(eval_ctx_, MY_SPEC.output_)));
  }
  return ret;
}

}  // end namespace sql
}  // end namespace oceanbase
