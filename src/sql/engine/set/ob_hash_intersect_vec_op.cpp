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

#include "sql/engine/set/ob_hash_intersect_vec_op.h"
#include "sql/engine/px/ob_px_util.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObHashIntersectVecSpec::ObHashIntersectVecSpec(ObIAllocator &alloc, const ObPhyOperatorType type)
    : ObHashSetVecSpec(alloc, type)
{
}

OB_SERIALIZE_MEMBER((ObHashIntersectVecSpec, ObHashSetVecSpec));

ObHashIntersectVecOp::ObHashIntersectVecOp(
    ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
  : ObHashSetVecOp(exec_ctx, spec, input)
{
}

int ObHashIntersectVecOp::inner_open()
{
  return ObHashSetVecOp::inner_open();
}

int ObHashIntersectVecOp::inner_close()
{
  return ObHashSetVecOp::inner_close();
}

int ObHashIntersectVecOp::inner_rescan()
{
  return ObHashSetVecOp::inner_rescan();
}

void ObHashIntersectVecOp::destroy()
{
  return ObHashSetVecOp::destroy();
}

int ObHashIntersectVecOp::build_hash_table_by_part(int64_t batch_size)
{
  int ret = OB_SUCCESS;
  bool found = false;
  batch_size = batch_size == 0 ? 1 :batch_size;
  while (OB_SUCC(ret) && !found) {
    if (OB_FAIL(hp_infras_.get_next_pair_partition(InputSide::LEFT))) {
      LOG_WARN("failed to get next pair partitions", K(ret));
    } else if (!hp_infras_.has_cur_part(InputSide::LEFT)) {
      ret = OB_ITER_END;
    } else if (!hp_infras_.has_cur_part(InputSide::RIGHT)) {
      // left part has no matched right part
      if (OB_FAIL(hp_infras_.close_cur_part(InputSide::LEFT))) {
        LOG_WARN("failed to close cur part", K(ret));
      }
    } else if (OB_FAIL(build_hash_table_from_left_batch(false, batch_size))) {
      LOG_WARN("failed to build hash table batch", K(ret));
    } else if (OB_FAIL(hp_infras_.open_cur_part(InputSide::RIGHT))) {
      LOG_WARN("failed to open cur part");
    } else {
      found = true;
      hp_infras_.switch_right();
    }
  }
  return ret;
}

int ObHashIntersectVecOp::inner_get_next_row()
{
  return OB_NOT_IMPLEMENT;
}

int ObHashIntersectVecOp::inner_get_next_batch(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  const int64_t batch_size = std::min(max_row_cnt, MY_SPEC.max_batch_size_);
  brs_.skip_->reset(batch_size);
  if (first_get_left_) {
    const ObBatchRows *child_brs = nullptr;
    if (OB_FAIL(left_->get_next_batch(batch_size, child_brs))) {
      LOG_WARN("failed to get next batch", K(ret));
    } else if (FALSE_IT(left_brs_ = child_brs)) {
    } else if (child_brs->end_ && 0 == child_brs->size_) {
      ret = OB_ITER_END;
    } else if (OB_FAIL(init_hash_partition_infras_for_batch())) {
      LOG_WARN("failed to init hash partition for batch", K(ret));
    } else if (OB_FAIL(build_hash_table_from_left_batch(true, batch_size))) {
      LOG_WARN("failed to build hash table for batch", K(ret));
    } else {
      hp_infras_.switch_right();
    }
  }

  bool got_batch = false;
  const ObBatchRows *right_brs = nullptr;
  const common::ObIArray<ObExpr*> *cur_exprs = nullptr;
  int64_t read_rows = 0;
  while(OB_SUCC(ret) && !got_batch) {
    if (!has_got_part_) {
      if (OB_FAIL(right_->get_next_batch(batch_size, right_brs))) {
        LOG_WARN("failed to get next batch", K(ret));
      } else if (OB_FAIL(convert_vector(right_->get_spec().output_,
                      MY_SPEC.set_exprs_,
                      right_brs))) {
      } else if (right_brs->end_ && 0 == right_brs->size_) {
        ret = OB_ITER_END;
      } else {
        cur_exprs = &MY_SPEC.set_exprs_;
        read_rows = right_brs->size_;
        brs_.size_ = read_rows;
      }
    } else if (OB_FAIL(hp_infras_.get_right_next_batch(MY_SPEC.set_exprs_,
                                                        batch_size,
                                                        read_rows))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get next batch", K(ret));
      }
    } else {
      cur_exprs = &MY_SPEC.set_exprs_;
      brs_.size_ = read_rows;
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
        // get next dumped partition
      if (OB_FAIL(hp_infras_.finish_insert_row())) {
        LOG_WARN("failed to finish to insert row", K(ret));
      } else if (!has_got_part_) {
        has_got_part_ = true;
      } else {
        if (OB_FAIL(hp_infras_.close_cur_part(InputSide::RIGHT))) {
          LOG_WARN("failed to close cur part", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(hp_infras_.end_round())) {
        LOG_WARN("failed to end round", K(ret));
      } else if (OB_FAIL(hp_infras_.start_round())) {
        LOG_WARN("failed to open round", K(ret));
      } else if (OB_FAIL(build_hash_table_by_part(read_rows))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to build hash table", K(ret));
        }
      }
    } else if (OB_FAIL(ret)) {
    } else if (OB_FAIL(try_check_status())) {
      LOG_WARN("failed to check status", K(ret));
    } else if (OB_FAIL(hp_infras_.exists_batch(*cur_exprs,
                                                has_got_part_ ? brs_ : *right_brs,
                                                brs_.skip_,
                                                hash_values_for_batch_))) {
      LOG_WARN("failed to exist batch", K(ret));
    } else {
      got_batch = true;
    }
  }

  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
    brs_.end_ = true;
    brs_.size_ = 0;
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase
