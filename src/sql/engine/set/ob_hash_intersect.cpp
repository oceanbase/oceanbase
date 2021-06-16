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
#include "sql/engine/set/ob_hash_intersect.h"
#include "lib/utility/utility.h"

namespace oceanbase {
using namespace common;

namespace sql {

class ObHashIntersect::ObHashIntersectCtx : public ObHashSetOperatorCtx {
  explicit ObHashIntersectCtx(ObExecContext& ctx) : ObHashSetOperatorCtx(ctx)
  {}

  ~ObHashIntersectCtx()
  {}

private:
  DISALLOW_COPY_AND_ASSIGN(ObHashIntersectCtx);
  friend class ObHashIntersect;
};

ObHashIntersect::ObHashIntersect(common::ObIAllocator& alloc) : ObHashSetOperator(alloc)
{}

ObHashIntersect::~ObHashIntersect()
{}

int ObHashIntersect::inner_create_operator_ctx(ObExecContext& ctx, ObPhyOperatorCtx*& op_ctx) const
{
  return CREATE_PHY_OPERATOR_CTX(ObHashIntersectCtx, ctx, get_id(), get_type(), op_ctx);
}

int ObHashIntersect::inner_open(ObExecContext& ctx) const
{
  return ObHashSetOperator::inner_open(ctx);
}

int ObHashIntersect::build_hash_table_by_part(ObExecContext& ctx, ObHashIntersectCtx* intersect_ctx) const
{
  int ret = OB_SUCCESS;
  bool found = false;
  while (OB_SUCC(ret) && !found) {
    if (OB_FAIL(intersect_ctx->hp_infras_.get_next_pair_partition(InputSide::RIGHT))) {
      LOG_WARN("failed to get next pair partitions", K(ret));
    } else if (!intersect_ctx->hp_infras_.has_cur_part(InputSide::RIGHT)) {
      ret = OB_ITER_END;
    } else if (!intersect_ctx->hp_infras_.has_cur_part(InputSide::LEFT)) {
      // right part has no matched left part
      if (OB_FAIL(intersect_ctx->hp_infras_.close_cur_part(InputSide::RIGHT))) {
        LOG_WARN("failed to close cur part", K(ret));
      }
    } else if (OB_FAIL(build_hash_table(ctx, false))) {
      LOG_WARN("failed to build hash table", K(ret));
    } else if (OB_FAIL(intersect_ctx->hp_infras_.open_cur_part(InputSide::LEFT))) {
      LOG_WARN("failed to open cur part");
    } else {
      found = true;
      intersect_ctx->hp_infras_.switch_left();
    }
  }
  return ret;
}

int ObHashIntersect::inner_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ObHashIntersectCtx* intersect_ctx = NULL;
  const ObNewRow* cur_row = NULL;
  bool left_has_row = false;
  bool got_row = false;
  const ObChunkRowStore::StoredRow* store_row = nullptr;
  const HashPartCols* part_cols = nullptr;
  if (OB_ISNULL(intersect_ctx = GET_PHY_OPERATOR_CTX(ObHashIntersectCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get physical operator context failed", K(ret));
  } else if (intersect_ctx->first_get_left_) {
    if (OB_FAIL(intersect_ctx->is_left_has_row(ctx, left_has_row))) {
      LOG_WARN("failed to judge left has row", K(ret));
    } else if (!left_has_row) {
      ret = OB_ITER_END;
      intersect_ctx->iter_end_ = true;
    } else if (OB_FAIL(ObHashSetOperator::init_hash_partition_infras(ctx))) {
      LOG_WARN("failed to init hash partition infras", K(ret));
    } else if (OB_FAIL(build_hash_table(ctx, true))) {
      LOG_WARN("failed to build hash table", K(ret));
    } else {
      intersect_ctx->hp_infras_.switch_left();
    }
  } else if (intersect_ctx->iter_end_) {
    ret = OB_ITER_END;
  }
  while (OB_SUCC(ret) && !got_row) {
    if (!intersect_ctx->has_got_part_) {
      if (OB_FAIL(intersect_ctx->get_left_row(ctx, cur_row))) {
        if (ret != OB_ITER_END) {
          LOG_WARN("failed to get left row", K(ret));
        }
      }
    } else {
      ret = intersect_ctx->hp_infras_.get_left_next_row(store_row, cur_row);
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      // get next dumped partition
      if (OB_FAIL(intersect_ctx->hp_infras_.finish_insert_row())) {
        LOG_WARN("failed to finish to insert row", K(ret));
      } else if (!intersect_ctx->has_got_part_) {
        intersect_ctx->has_got_part_ = true;
      } else {
        if (OB_FAIL(intersect_ctx->hp_infras_.close_cur_part(InputSide::LEFT))) {
          LOG_WARN("failed to close cur part", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(intersect_ctx->hp_infras_.end_round())) {
        LOG_WARN("failed to end round", K(ret));
      } else if (OB_FAIL(intersect_ctx->hp_infras_.start_round())) {
        LOG_WARN("failed to open round", K(ret));
      } else if (OB_FAIL(build_hash_table_by_part(ctx, intersect_ctx))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to build hash table", K(ret));
        } else {
          intersect_ctx->iter_end_ = true;
        }
      }
    } else if (OB_FAIL(ret)) {
    } else if (OB_FAIL(try_check_status(ctx))) {
      LOG_WARN("failed to check status", K(ret));
    } else if (OB_FAIL(intersect_ctx->hp_infras_.exists_row(cur_row, part_cols))) {
      LOG_WARN("failed to probe exists row", K(ret));
    } else if (OB_NOT_NULL(part_cols)) {
      // LOG_TRACE("trace part cols store row",
      //   K(*part_cols->store_row_), K(part_cols->store_row_->is_match()));
      if (!part_cols->store_row_->is_match()) {
        // exists & not return
        got_row = true;
        row = cur_row;
        part_cols->store_row_->set_is_match(true);
      }
    } else {
      // row is not exists in hash table
      if (intersect_ctx->hp_infras_.has_right_dumped()) {
        // dump left row if right is dumpe
        if (!intersect_ctx->hp_infras_.has_left_dumped() &&
            OB_FAIL(intersect_ctx->hp_infras_.create_dumped_partitions(InputSide::LEFT))) {
          LOG_WARN("failed to create dump partitions", K(ret));
        } else if (OB_FAIL(intersect_ctx->hp_infras_.insert_row_on_partitions(cur_row))) {
          LOG_WARN("failed to insert row into partitions", K(ret));
        }
      }
    }
  }  // end of while
  if (OB_SUCC(ret)) {
    if (OB_FAIL(copy_cur_row_by_projector(*intersect_ctx, row))) {
      LOG_WARN("copy current row failed", K(ret));
    }
  }
  return ret;
}

int ObHashIntersect::inner_close(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObHashSetOperator::inner_close(ctx))) {
    LOG_WARN("failed to close", K(ret));
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
