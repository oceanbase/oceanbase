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

#include "sql/engine/set/ob_hash_except_vec_op.h"
#include "sql/engine/px/ob_px_util.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObHashExceptVecSpec::ObHashExceptVecSpec(ObIAllocator &alloc, const ObPhyOperatorType type)
    : ObHashSetVecSpec(alloc, type)
{
}

OB_SERIALIZE_MEMBER((ObHashExceptVecSpec, ObHashSetVecSpec));

ObHashExceptVecOp::ObHashExceptVecOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
  : ObHashSetVecOp(exec_ctx, spec, input), get_row_from_hash_table_(false)
{
}

int ObHashExceptVecOp::inner_open()
{
  return ObHashSetVecOp::inner_open();
}

int ObHashExceptVecOp::inner_close()
{
  return ObHashSetVecOp::inner_close();
}

int ObHashExceptVecOp::inner_rescan()
{
  return ObHashSetVecOp::inner_rescan();
}

void ObHashExceptVecOp::destroy()
{
  return ObHashSetVecOp::destroy();
}

int ObHashExceptVecOp::build_hash_table_by_part(int64_t batch_size)
{
  int ret= OB_SUCCESS;
  bool found = false;
  batch_size = batch_size == 0 ? 1 : batch_size;
  while (OB_SUCC(ret) && !found) {
    if (OB_FAIL(hp_infras_.get_next_pair_partition(InputSide::LEFT))) {
      LOG_WARN("failed to get next partition", K(ret));
    } else if (!hp_infras_.has_cur_part(InputSide::LEFT)) {
      //If there is no part on the left, there is no data to return.
      ret = OB_ITER_END;
    } else if (OB_FAIL(build_hash_table_from_left_batch(false, batch_size))) {
      LOG_WARN("failed to build hash table batch", K(ret));
    } else if (!hp_infras_.has_cur_part(InputSide::RIGHT)) {
      //The right side is null, and the data is taken directly from the hash table on the left.
      get_row_from_hash_table_ = true;
      found = true;
    } else if (OB_FAIL(hp_infras_.open_cur_part(InputSide::RIGHT))) {
      LOG_WARN("failed to open cur part");
    } else {
      found = true;
      hp_infras_.switch_right();//dump logic falls on the right
    }
  }
  return ret;
}

int ObHashExceptVecOp::inner_get_next_row()
{
  return OB_NOT_IMPLEMENT;
}

int ObHashExceptVecOp::batch_process_right_vectorize(const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  const common::ObIArray<ObExpr*> *cur_exprs = nullptr;
  const ObBatchRows *right_brs = nullptr;
  int64_t read_rows = 0;
  brs_.skip_->reset(batch_size);
  while (OB_SUCC(ret)) {
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
      }
    } else if (OB_FAIL(hp_infras_.get_right_next_batch(MY_SPEC.set_exprs_,
                                                       batch_size,
                                                       read_rows))) {
      LOG_WARN("failed to get next batch from dumped partition", K(ret), K(read_rows));
    } else {
      cur_exprs = &MY_SPEC.set_exprs_;
      brs_.size_ = read_rows;
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(try_check_status())) {
      LOG_WARN("failed to check status", K(ret));
    } else if (OB_FAIL(hp_infras_.exists_batch(*cur_exprs,
                                               has_got_part_ ? brs_ : *right_brs,
                                               brs_.skip_,
                                               hash_values_for_batch_))) {
      LOG_WARN("failed to exists batch", K(ret));
    } else {
      //for except, do not need set skip vector in exists_batch, all rows are from hash table.
      brs_.skip_->reset(batch_size);
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObHashExceptVecOp::get_next_batch_from_hashtable(const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  bool got_batch = false;
  int64_t read_rows = 0;
  const ObCompactRow *store_rows[batch_size];
  while (OB_SUCC(ret) && !got_batch) {
    if (!get_row_from_hash_table_) {
      if (OB_FAIL(hp_infras_.finish_insert_row())) {
        LOG_WARN("failed to finish insert row", K(ret));
      } else if (OB_FAIL(hp_infras_.end_round())) {
        LOG_WARN("faild to end round", K(ret));
      } else if (OB_FAIL(hp_infras_.start_round())) {
        LOG_WARN("failed to start round", K(ret));
      } else if (OB_FAIL(build_hash_table_by_part(batch_size))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to build hash table by part", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (get_row_from_hash_table_) {
      } else if (OB_FAIL(batch_process_right_vectorize(batch_size))) {
        LOG_WARN("failed to process right vec", K(ret));
      } else if (OB_FAIL(hp_infras_.close_cur_part(InputSide::RIGHT))) {
        LOG_WARN("failed to close right part", K(ret));
      } else {
        get_row_from_hash_table_ = true;
      }
      if (OB_SUCC(ret) && OB_FAIL(hp_infras_.open_hash_table_part())) {
        LOG_WARN("failed to open hashtable part", K(ret));
      }
    } else if (OB_FAIL(hp_infras_.get_next_hash_table_batch(MY_SPEC.set_exprs_,
                                                            batch_size,
                                                            read_rows,
                                                            &store_rows[0]))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get next hash table batch", K(ret));
      } else {
        get_row_from_hash_table_ = false;
        ret = OB_SUCCESS;
      }
    } else {
      brs_.size_ = read_rows;
      for (int64_t i = 0; i < read_rows; ++i) {
        const ObHashPartItem *sr = static_cast<const ObHashPartItem *>(store_rows[i]);
        if (sr->is_match(hp_infras_.get_hash_store_row_meta())) {
          brs_.skip_->set(i);
        }
      }
      got_batch = true;
    }
  }
  return ret;
}

int ObHashExceptVecOp::inner_get_next_batch(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  if (first_get_left_) {
    // Use a larger size before outputting.
    const ObBatchRows *child_brs = nullptr;
    if (OB_FAIL(left_->get_next_batch(MY_SPEC.max_batch_size_, child_brs))) {
      LOG_WARN("failed get left batch", K(ret));
    } else if (FALSE_IT(left_brs_ = child_brs)) {
    } else if (child_brs->end_ && 0 == child_brs->size_) {
      ret = OB_ITER_END;
    } else if (OB_FAIL(init_hash_partition_infras_for_batch())) {
      LOG_WARN("failed to init hash partition infras", K(ret));
    } else if (OB_FAIL(build_hash_table_from_left_batch(true, MY_SPEC.max_batch_size_))) {
      LOG_WARN("failed to build hash table", K(ret));
    } else {
      hp_infras_.switch_right();
      if (OB_FAIL(batch_process_right_vectorize(MY_SPEC.max_batch_size_))) {
        LOG_WARN("failed to batch process right", K(ret));
      } else if (OB_FAIL(hp_infras_.open_hash_table_part())) {
        LOG_WARN("failed to open hash table part", K(ret));
      } else {
        get_row_from_hash_table_ = true;
        has_got_part_ = true;
      }
    }
  }

  const int64_t batch_size = std::min(max_row_cnt, MY_SPEC.max_batch_size_);
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_next_batch_from_hashtable(batch_size))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get next row from hash table", K(ret));
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
    brs_.end_ = true;
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase
