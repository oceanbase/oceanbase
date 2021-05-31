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

#include "sql/engine/set/ob_hash_intersect_op.h"
#include "sql/engine/px/ob_px_util.h"
#include "sql/engine/basic/ob_hash_partitioning_infrastructure_op.h"

namespace oceanbase {
using namespace common;
namespace sql {

ObHashIntersectSpec::ObHashIntersectSpec(ObIAllocator& alloc, const ObPhyOperatorType type) : ObHashSetSpec(alloc, type)
{}

OB_SERIALIZE_MEMBER((ObHashIntersectSpec, ObHashSetSpec));

ObHashIntersectOp::ObHashIntersectOp(ObExecContext& exec_ctx, const ObOpSpec& spec, ObOpInput* input)
    : ObHashSetOp(exec_ctx, spec, input)
{}

int ObHashIntersectOp::inner_open()
{
  return ObHashSetOp::inner_open();
}

int ObHashIntersectOp::inner_close()
{
  return ObHashSetOp::inner_close();
}

int ObHashIntersectOp::rescan()
{
  return ObHashSetOp::rescan();
}

void ObHashIntersectOp::destroy()
{
  return ObHashSetOp::destroy();
}

int ObHashIntersectOp::build_hash_table_by_part()
{
  int ret = OB_SUCCESS;
  bool found = false;
  while (OB_SUCC(ret) && !found) {
    if (OB_FAIL(hp_infras_.get_next_pair_partition(InputSide::RIGHT))) {
      LOG_WARN("failed to get next pair partitions", K(ret));
    } else if (!hp_infras_.has_cur_part(InputSide::RIGHT)) {
      ret = OB_ITER_END;
    } else if (!hp_infras_.has_cur_part(InputSide::LEFT)) {
      // right part has no matched left part
      if (OB_FAIL(hp_infras_.close_cur_part(InputSide::RIGHT))) {
        LOG_WARN("failed to close cur part", K(ret));
      }
    } else if (OB_FAIL(build_hash_table(false))) {
      LOG_WARN("failed to build hash table", K(ret));
    } else if (OB_FAIL(hp_infras_.open_cur_part(InputSide::LEFT))) {
      LOG_WARN("failed to open cur part");
    } else {
      found = true;
      hp_infras_.switch_left();
    }
  }
  return ret;
}

int ObHashIntersectOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  bool left_has_row = false;
  bool got_row = false;
  const ObChunkDatumStore::StoredRow* store_row = nullptr;
  const ObHashPartCols* part_cols = nullptr;
  const common::ObIArray<ObExpr*>* cur_exprs = nullptr;
  clear_evaluated_flag();
  if (first_get_left_) {
    if (OB_FAIL(is_left_has_row(left_has_row))) {
      LOG_WARN("failed to judge left has row", K(ret));
    } else if (!left_has_row) {
      ret = OB_ITER_END;
    } else if (OB_FAIL(ObHashSetOp::init_hash_partition_infras())) {
      LOG_WARN("failed to init hash partition infras", K(ret));
    } else if (OB_FAIL(build_hash_table(true))) {
      LOG_WARN("failed to build hash table", K(ret));
    } else {
      hp_infras_.switch_left();
    }
  }
  while (OB_SUCC(ret) && !got_row) {
    if (!has_got_part_) {
      if (OB_FAIL(get_left_row())) {
        if (ret != OB_ITER_END) {
          LOG_WARN("failed to get left row", K(ret));
        }
      } else {
        cur_exprs = &left_->get_spec().output_;
      }
    } else {
      if (OB_FAIL(hp_infras_.get_left_next_row(store_row, MY_SPEC.output_))) {
      } else {
        cur_exprs = &MY_SPEC.output_;
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      // get next dumped partition
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
      } else if (OB_FAIL(hp_infras_.start_round())) {
        LOG_WARN("failed to open round", K(ret));
      } else if (OB_FAIL(build_hash_table_by_part())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to build hash table", K(ret));
        }
      }
    } else if (OB_FAIL(ret)) {
    } else if (OB_FAIL(try_check_status())) {
      LOG_WARN("failed to check status", K(ret));
    } else if (OB_FAIL(hp_infras_.exists_row(*cur_exprs, part_cols))) {
      LOG_WARN("failed to probe exists row", K(ret));
    } else if (OB_NOT_NULL(part_cols)) {
      // LOG_TRACE("trace part cols store row",
      //   K(*part_cols->store_row_), K(part_cols->store_row_->is_match()));
      if (!part_cols->store_row_->is_match()) {
        // exists & not return
        got_row = true;
        part_cols->store_row_->set_is_match(true);
      }
    } else {
      // row is not exists in hash table
      if (hp_infras_.has_right_dumped()) {
        // dump left row if right is dumpe
        if (!hp_infras_.has_left_dumped() && OB_FAIL(hp_infras_.create_dumped_partitions(InputSide::LEFT))) {
          LOG_WARN("failed to create dump partitions", K(ret));
        } else if (OB_FAIL(hp_infras_.insert_row_on_partitions(*cur_exprs))) {
          LOG_WARN("failed to insert row into partitions", K(ret));
        }
      }
    }
  }  // end of while
  if (OB_SUCC(ret) && !has_got_part_) {
    if (OB_FAIL(convert_row(*cur_exprs, MY_SPEC.output_))) {
      LOG_WARN("copy current row failed", K(ret));
    }
  }
  return ret;
}

}  // end namespace sql
}  // end namespace oceanbase
