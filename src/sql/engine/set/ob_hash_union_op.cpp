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

namespace oceanbase {
using namespace common;
namespace sql {

ObHashUnionSpec::ObHashUnionSpec(ObIAllocator& alloc, const ObPhyOperatorType type) : ObHashSetSpec(alloc, type)
{}

OB_SERIALIZE_MEMBER((ObHashUnionSpec, ObHashSetSpec));

ObHashUnionOp::ObHashUnionOp(ObExecContext& exec_ctx, const ObOpSpec& spec, ObOpInput* input)
    : ObHashSetOp(exec_ctx, spec, input), cur_child_op_(nullptr), is_left_child_(true)
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

int ObHashUnionOp::rescan()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObHashSetOp::rescan())) {
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

int ObHashUnionOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  if (OB_ISNULL(cur_child_op_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("cur_child_op is null", K(ret), K(cur_child_op_));
  } else if (first_get_left_) {
    if (OB_FAIL(ObHashSetOp::init_hash_partition_infras())) {
      LOG_WARN("failed to get next row", K(ret));
    }
    first_get_left_ = false;
  }
  bool got_row = false;
  bool has_exists = false;
  bool inserted = false;
  const ObChunkDatumStore::StoredRow* store_row = nullptr;
  while (OB_SUCC(ret) && !got_row) {
    if (!has_got_part_) {
      ret = get_child_next_row();
    } else {
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
      } else if (OB_FAIL(hp_infras_.resize(hp_infras_.get_cur_part_row_cnt(InputSide::LEFT)))) {
        LOG_WARN("failed to init hash table", K(ret));
      }
    } else if (OB_FAIL(ret)) {
    } else if (OB_FAIL(hp_infras_.insert_row(
                   has_got_part_ ? MY_SPEC.set_exprs_ : cur_child_op_->get_spec().output_, has_exists, inserted))) {
      LOG_WARN("failed to insert row", K(ret));
    } else if (has_exists) {
      // Already in hash map, do nothing
    } else if (inserted) {
      got_row = true;
    }
  }  // end of while
  if (OB_SUCC(ret) && !has_got_part_) {
    if (OB_FAIL(convert_row(cur_child_op_->get_spec().output_, MY_SPEC.set_exprs_))) {
      LOG_WARN("copy current row failed", K(ret));
    }
  }
  return ret;
}

}  // end namespace sql
}  // end namespace oceanbase
