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
#include "sql/engine/set/ob_hash_union.h"
#include "lib/utility/utility.h"
namespace oceanbase {
using namespace common;

namespace sql {
class ObHashUnion::ObHashUnionCtx : public ObHashSetOperatorCtx {
public:
  explicit ObHashUnionCtx(ObExecContext& ctx) : ObHashSetOperatorCtx(ctx), cur_child_op_(NULL), is_left_child_(true)
  {}

  virtual ~ObHashUnionCtx()
  {}

private:
  DISALLOW_COPY_AND_ASSIGN(ObHashUnionCtx);

protected:
  ObPhyOperator* cur_child_op_;
  bool is_left_child_;

  friend class ObHashUnion;
};

ObHashUnion::ObHashUnion(common::ObIAllocator& alloc) : ObHashSetOperator(alloc)
{}

ObHashUnion::~ObHashUnion()
{}

int ObHashUnion::rescan(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObHashUnionCtx* union_ctx = NULL;
  if (OB_FAIL(ObHashSetOperator::rescan(ctx))) {
    LOG_WARN("rescan child operator failed", K(ret));
  } else if (OB_ISNULL(union_ctx = GET_PHY_OPERATOR_CTX(ObHashUnionCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get union ctx", K(ret), K(union_ctx));
  } else {
    union_ctx->cur_child_op_ = get_child(FIRST_CHILD);
    union_ctx->is_left_child_ = true;
  }
  return ret;
}

int ObHashUnion::inner_create_operator_ctx(ObExecContext& ctx, ObPhyOperatorCtx*& op_ctx) const
{
  return CREATE_PHY_OPERATOR_CTX(ObHashUnionCtx, ctx, get_id(), get_type(), op_ctx);
}

int ObHashUnion::inner_open(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObHashUnionCtx* union_ctx = NULL;
  if (init_op_ctx(ctx)) {
    LOG_WARN("failed to init operator context", K(ret));
  } else if (OB_ISNULL(union_ctx = GET_PHY_OPERATOR_CTX(ObHashUnionCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get physical operator context failed", K(ret), K(ctx), K_(id));
  } else {
    union_ctx->cur_child_op_ = get_child(FIRST_CHILD);
    union_ctx->is_left_child_ = true;
  }
  return ret;
}

int ObHashUnion::get_child_next_row(ObExecContext& ctx, ObHashUnionCtx* union_ctx, const common::ObNewRow*& row) const
{
  int ret = union_ctx->cur_child_op_->get_next_row(ctx, row);
  if (OB_ITER_END == ret) {
    if (union_ctx->is_left_child_) {
      ret = OB_SUCCESS;
      union_ctx->is_left_child_ = false;
      union_ctx->cur_child_op_ = get_child(SECOND_CHILD);
      ret = union_ctx->cur_child_op_->get_next_row(ctx, row);
    }
  }
  return ret;
}

// If bkt_created_ false, create bucket fist.
// If not matched int hash buckets, store in bucket and return this row.
// If matched, get next row until get row not matched.
// Once the left child is exhausted, continue to visist the right child
int ObHashUnion::inner_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ObHashUnionCtx* union_ctx = NULL;
  const ObNewRow* cur_row = NULL;
  if (OB_ISNULL(union_ctx = GET_PHY_OPERATOR_CTX(ObHashUnionCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get physical operator context failed", K(ret));
  } else if (OB_ISNULL(union_ctx->cur_child_op_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("cur_child_op is null", K(ret), K(union_ctx->cur_child_op_));
  } else if (union_ctx->first_get_left_) {
    if (OB_FAIL(ObHashSetOperator::init_hash_partition_infras(ctx))) {
      LOG_WARN("failed to get next row", K(ret));
    }
    union_ctx->first_get_left_ = false;
  } else if (union_ctx->iter_end_) {
    ret = OB_ITER_END;
  }
  HashCols hash_cols;
  bool got_row = false;
  bool has_exists = false;
  bool inserted = false;
  const ObChunkRowStore::StoredRow* store_row = nullptr;
  while (OB_SUCC(ret) && !got_row) {
    if (!union_ctx->has_got_part_) {
      ret = get_child_next_row(ctx, union_ctx, cur_row);
    } else {
      ret = union_ctx->hp_infras_.get_left_next_row(store_row, cur_row);
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      // get dumped partition
      if (OB_FAIL(union_ctx->hp_infras_.finish_insert_row())) {
        LOG_WARN("failed to finish to insert row", K(ret));
      } else if (!union_ctx->has_got_part_) {
        union_ctx->has_got_part_ = true;
      } else {
        if (OB_FAIL(union_ctx->hp_infras_.close_cur_part(InputSide::LEFT))) {
          LOG_WARN("failed to close cur part", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(union_ctx->hp_infras_.end_round())) {
        LOG_WARN("failed to end round", K(ret));
      } else if (OB_FAIL(try_check_status(ctx))) {
        LOG_WARN("failed to check status", K(ret));
      } else if (OB_FAIL(union_ctx->hp_infras_.start_round())) {
        LOG_WARN("failed to open round", K(ret));
      } else if (OB_FAIL(union_ctx->hp_infras_.get_next_partition(InputSide::LEFT))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to create dumped partitions", K(ret));
        } else {
          union_ctx->iter_end_ = true;
        }
      } else if (OB_FAIL(union_ctx->hp_infras_.open_cur_part(InputSide::LEFT))) {
        LOG_WARN("failed to open cur part");
      } else if (OB_FAIL(union_ctx->hp_infras_.resize(union_ctx->hp_infras_.get_cur_part_row_cnt(InputSide::LEFT)))) {
        LOG_WARN("failed to init hash table", K(ret));
      }
    } else if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(cur_row)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("row is null", K(ret), K(cur_row));
    } else if (OB_FAIL(union_ctx->hp_infras_.insert_row(cur_row, has_exists, inserted))) {
      LOG_WARN("failed to insert row", K(ret));
    } else if (has_exists) {
      // Already in hash map, do nothing
    } else if (inserted) {
      got_row = true;
      row = cur_row;
    }
  }  // end of while
  if (OB_SUCC(ret)) {
    if (OB_FAIL(copy_cur_row_by_projector(*union_ctx, row))) {
      LOG_WARN("copy current row failed", K(ret));
    }
  }
  return ret;
}

int ObHashUnion::inner_close(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObHashSetOperator::inner_close(ctx))) {
    LOG_WARN("failed to close", K(ret));
  }
  return ret;
}
}  // namespace sql
}  // namespace oceanbase
