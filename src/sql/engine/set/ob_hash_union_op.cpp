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

#include "sql/engine/set/ob_hash_union_op.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObHashUnionSpec::ObHashUnionSpec(ObIAllocator &alloc, const ObPhyOperatorType type)
    : ObHashSetSpec(alloc, type)
{
}

OB_SERIALIZE_MEMBER((ObHashUnionSpec, ObHashSetSpec));

ObHashUnionOp::ObHashUnionOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
  : ObHashSetOp(exec_ctx, spec, input),
  cur_child_op_(nullptr),
  is_left_child_(true)
{}

int ObHashUnionOp::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObHashSetOp::inner_open())) {
    LOG_WARN("failed to inner open", K(ret));
  } else {
    cur_child_op_ = left_;
    is_left_child_ = true;
  }
  return ret;
}

int ObHashUnionOp::inner_close()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObHashSetOp::inner_close())) {
    LOG_WARN("failed to inner close", K(ret));
  }
  return ret;
}

int ObHashUnionOp::inner_rescan()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObHashSetOp::inner_rescan())) {
    LOG_WARN("failed to rescan child operator", K(ret));
  } else {
    cur_child_op_ = left_;
    is_left_child_ = true;
  }
  return ret;
}

void ObHashUnionOp::destroy()
{
  ObHashSetOp::destroy();
}

int ObHashUnionOp::get_child_next_row()
{
  int ret = cur_child_op_->get_next_row();
  if (OB_ITER_END == ret) {
    if (is_left_child_) {
      ret = OB_SUCCESS;
      is_left_child_ = false;
      cur_child_op_ = right_;
      ret = cur_child_op_->get_next_row();
    }
  }
  return ret;
}

int ObHashUnionOp::get_child_next_batch(const int64_t batch_size, const ObBatchRows *&child_brs)
{
  int ret = cur_child_op_->get_next_batch(batch_size, child_brs);
  if (OB_SUCC(ret) && 0 == child_brs->size_ && child_brs->end_) {
    if (is_left_child_) {
      is_left_child_ = false;
      cur_child_op_ = right_;
      ret = cur_child_op_->get_next_batch(batch_size, child_brs);
    }
  }
  return ret;
}

int ObHashUnionOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  if (OB_ISNULL(cur_child_op_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("cur_child_op is null", K(ret), K(cur_child_op_));
  } else if (first_get_left_) {
    if (OB_FAIL(ObHashSetOp::init_hash_partition_infras())) {
      LOG_WARN("failed to init hash partition infra", K(ret));
    }
    first_get_left_ = false;
  }
  bool got_row = false;
  bool has_exists = false;
  bool inserted = false;
  const ObChunkDatumStore::StoredRow *store_row = nullptr;
  while (OB_SUCC(ret) && !got_row) {
    if (!has_got_part_) {
      ret = get_child_next_row();
    } else {
      // TODO: longzhong.wlz 这里其实可以将storerow拿出来直接insert而不是先转成output再写入
      //        后续可以再优化
      ret = hp_infras_.get_left_next_row(store_row, MY_SPEC.set_exprs_);
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
      } else if (OB_FAIL(hp_infras_.end_round())) {
        LOG_WARN("failed to end round", K(ret));
      } else if (OB_FAIL(try_check_status())) {
        LOG_WARN("failed to check status", K(ret));
      } else if (OB_FAIL(hp_infras_.start_round())) {
        LOG_WARN("failed to open round", K(ret));
      } else if (OB_FAIL(hp_infras_.get_next_partition(InputSide::LEFT))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to create dumped partitions", K(ret));
        }
      } else if (OB_FAIL(hp_infras_.open_cur_part(InputSide::LEFT))) {
        LOG_WARN("failed to open cur part");
      } else if (OB_FAIL(hp_infras_.resize(
          hp_infras_.get_cur_part_row_cnt(InputSide::LEFT)))) {
        LOG_WARN("failed to init hash table", K(ret));
      }
    } else if (OB_FAIL(ret)) {
    } else if (OB_FAIL(hp_infras_.insert_row(
        has_got_part_ ? MY_SPEC.set_exprs_ : cur_child_op_->get_spec().output_,
        has_exists, inserted))) {
      LOG_WARN("failed to insert row", K(ret));
    } else if (has_exists) {
      //Already in hash map, do nothing
    } else if (inserted) {
      got_row = true;
    }
  } //end of while
  if (OB_SUCC(ret) && !has_got_part_) {
    if (OB_FAIL(convert_row(cur_child_op_->get_spec().output_, MY_SPEC.set_exprs_))) {
      LOG_WARN("copy current row failed", K(ret));
    }
  }
  return ret;
}

int ObHashUnionOp::inner_get_next_batch(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  const int64_t batch_size = std::min(max_row_cnt, MY_SPEC.max_batch_size_);
  bool child_op_end = false;
  bool end_to_process = false;
  int64_t read_rows = -1;
  clear_evaluated_flag();
  if (OB_ISNULL(cur_child_op_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("cur_child_op is null", K(ret));
  } else if (first_get_left_) {
    if (OB_FAIL(init_hash_partition_infras_for_batch())) {
      LOG_WARN("failed to init hash partition infra batch", K(ret));
    }
    first_get_left_ = false;
  }
  bool got_batch = false;
  ObBitVector *output_vec = nullptr;
  while(OB_SUCC(ret) && !got_batch) {
    const ObBatchRows *child_brs = nullptr;
    if (!has_got_part_) {
      if (child_op_end) {
        end_to_process = true;
      } else if (OB_FAIL(get_child_next_batch(batch_size, child_brs))) {
        LOG_WARN("failed to get child next batch", K(ret));
      } else if (OB_FAIL(hp_infras_.calc_hash_value_for_batch(cur_child_op_->get_spec().output_, 
                                                              child_brs->size_, 
                                                              child_brs->skip_, 
                                                              hash_values_for_batch_))) {
        LOG_WARN("failed to calc hash value for batch", K(ret));
      } else {
        child_op_end = cur_child_op_ == right_ && child_brs->end_ && 0 != child_brs->size_;
        end_to_process = cur_child_op_ == right_ && child_brs->end_ && 0 == child_brs->size_;
        read_rows = child_brs->size_;
      }
    } else if (OB_FAIL(hp_infras_.get_left_next_batch(MY_SPEC.set_exprs_, 
                                                      batch_size, 
                                                      read_rows, 
                                                      hash_values_for_batch_))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        end_to_process = true;
      } else {
        LOG_WARN("failed to get batch from infra", K(ret));
      }
    }
    if (OB_SUCC(ret) && end_to_process) {
      end_to_process = false;
      if (OB_FAIL(hp_infras_.finish_insert_row())) {
        LOG_WARN("failed to finish insert row", K(ret));
      } else if (!has_got_part_) {
        has_got_part_ = true;
      } else if (OB_FAIL(hp_infras_.close_cur_part(InputSide::LEFT))) {
        LOG_WARN("failed to close cur part", K(ret));
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(hp_infras_.end_round())) {
        LOG_WARN("failed to end round", K(ret));
      } else if (OB_FAIL(try_check_status())) {
        LOG_WARN("failed to check status", K(ret));
      } else if (OB_FAIL(hp_infras_.start_round())) {
        LOG_WARN("failed to start round", K(ret));
      } else if (OB_FAIL(hp_infras_.get_next_partition(InputSide::LEFT))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to get next dumped partition", K(ret));
        }
      } else if (OB_FAIL(hp_infras_.open_cur_part(InputSide::LEFT))) {
        LOG_WARN("failed to open cur part", K(ret));
      } else if (OB_FAIL(hp_infras_.resize(hp_infras_.get_cur_part_row_cnt(InputSide::LEFT)))) {
        LOG_WARN("failed to resize cur part", K(ret));
      }
    } else if (OB_FAIL(ret)) {
    } else if (has_got_part_ && OB_FAIL(hp_infras_.insert_row_for_batch(MY_SPEC.set_exprs_,
                                                                        hash_values_for_batch_,
                                                                        read_rows,
                                                                        nullptr,
                                                                        output_vec))) {
      LOG_WARN("failed to insert batch for dump", K(ret));
    } else if (!has_got_part_ && OB_FAIL(hp_infras_.insert_row_for_batch(cur_child_op_->get_spec().output_,
                                                                         hash_values_for_batch_,
                                                                         read_rows,
                                                                         child_brs->skip_,
                                                                         output_vec))) {
      LOG_WARN("failed to insert batch for no dump", K(ret));
    } else if (OB_ISNULL(output_vec)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get output vec", K(ret));
    } else {
      brs_.size_ = read_rows;
      brs_.skip_->deep_copy(*output_vec, read_rows);
      int64_t got_rows = read_rows - output_vec->accumulate_bit_cnt(read_rows);
      got_batch = (got_rows != 0);
    }
  }
  if (OB_SUCC(ret) && !has_got_part_) {
    if (OB_FAIL(convert_batch(cur_child_op_->get_spec().output_, 
                              MY_SPEC.set_exprs_, 
                              brs_.size_, 
                              *brs_.skip_))) {
      LOG_WARN("copy current row failed", K(ret));
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
    brs_.size_ = 0;
    brs_.end_ = true;
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase

