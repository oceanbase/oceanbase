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
#include "ob_hash_except.h"
#include "lib/utility/utility.h"

namespace oceanbase {
using namespace common;

namespace sql {

class ObHashExcept::ObHashExceptCtx : public ObHashSetOperatorCtx {
  explicit ObHashExceptCtx(ObExecContext& ctx) : ObHashSetOperatorCtx(ctx)
  {}

  ~ObHashExceptCtx()
  {}

private:
  DISALLOW_COPY_AND_ASSIGN(ObHashExceptCtx);
  friend class ObHashExcept;
};

ObHashExcept::ObHashExcept(common::ObIAllocator& alloc) : ObHashSetOperator(alloc)
{}

ObHashExcept::~ObHashExcept()
{}

int ObHashExcept::inner_create_operator_ctx(ObExecContext& ctx, ObPhyOperatorCtx*& op_ctx) const
{
  return CREATE_PHY_OPERATOR_CTX(ObHashExceptCtx, ctx, get_id(), get_type(), op_ctx);
}

int ObHashExcept::inner_open(ObExecContext& ctx) const
{
  return ObHashSetOperator::inner_open(ctx);
}

int ObHashExcept::build_hash_table_by_part(ObExecContext& ctx, ObHashExceptCtx* except_ctx) const
{
  int ret = OB_SUCCESS;
  bool found = false;
  while (OB_SUCC(ret) && !found) {
    if (OB_FAIL(except_ctx->hp_infras_.get_next_pair_partition(InputSide::RIGHT))) {
      LOG_WARN("failed to get next pair partitions", K(ret));
    } else if (!except_ctx->hp_infras_.has_cur_part(InputSide::RIGHT)) {
      if (OB_FAIL(except_ctx->hp_infras_.get_next_partition(InputSide::LEFT))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to get next partition", K(ret));
        }
      } else if (OB_FAIL(except_ctx->hp_infras_.open_cur_part(InputSide::LEFT))) {
        LOG_WARN("failed to open cur part");
      } else if (OB_FAIL(except_ctx->hp_infras_.resize(except_ctx->hp_infras_.get_cur_part_row_cnt(InputSide::LEFT)))) {
        LOG_WARN("failed to init hash table", K(ret));
      } else {
        found = true;
        except_ctx->hp_infras_.switch_left();
      }
    } else if (!except_ctx->hp_infras_.has_cur_part(InputSide::LEFT)) {
      // left: empty right: yes
      if (OB_FAIL(except_ctx->hp_infras_.close_cur_part(InputSide::RIGHT))) {
        LOG_WARN("failed to close cur part", K(ret));
      }
    } else if (OB_FAIL(build_hash_table(ctx, false))) {
      LOG_WARN("failed to build hash table", K(ret));
    } else if (OB_FAIL(except_ctx->hp_infras_.open_cur_part(InputSide::LEFT))) {
      LOG_WARN("failed to open cur part");
    } else {
      found = true;
      except_ctx->hp_infras_.switch_left();
    }
  }
  return ret;
}

int ObHashExcept::inner_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ObHashExceptCtx* except_ctx = NULL;
  const ObNewRow* cur_row = NULL;
  bool left_has_row = false;
  bool got_row = false;
  bool inserted = false;
  bool exists = false;
  const HashPartCols* part_cols = nullptr;
  const ObChunkRowStore::StoredRow* store_row = nullptr;
  if (OB_ISNULL(except_ctx = GET_PHY_OPERATOR_CTX(ObHashExceptCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get physical operator context failed", K(ret));
  } else if (except_ctx->first_get_left_) {
    if (OB_FAIL(except_ctx->is_left_has_row(ctx, left_has_row))) {
      LOG_WARN("failed to judge left has row", K(ret));
    } else if (!left_has_row) {
      ret = OB_ITER_END;
      except_ctx->iter_end_ = true;
    } else if (OB_FAIL(ObHashSetOperator::init_hash_partition_infras(ctx))) {
      LOG_WARN("failed to init hash partition infras", K(ret));
    } else if (OB_FAIL(build_hash_table(ctx, true))) {
      LOG_WARN("failed to build hash table", K(ret));
    } else {
      except_ctx->hp_infras_.switch_left();
    }
  } else if (except_ctx->iter_end_) {
    ret = OB_ITER_END;
  }
  while (OB_SUCC(ret) && !got_row) {
    if (!except_ctx->has_got_part_) {
      if (OB_FAIL(except_ctx->get_left_row(ctx, cur_row))) {
        if (ret != OB_ITER_END) {
          LOG_WARN("failed to get left row", K(ret));
        }
      }
    } else {
      ret = except_ctx->hp_infras_.get_left_next_row(store_row, cur_row);
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      // get next dumped partition
      if (OB_FAIL(except_ctx->hp_infras_.finish_insert_row())) {
        LOG_WARN("failed to finish to insert row", K(ret));
      } else if (!except_ctx->has_got_part_) {
        except_ctx->has_got_part_ = true;
      } else {
        if (OB_FAIL(except_ctx->hp_infras_.close_cur_part(InputSide::LEFT))) {
          LOG_WARN("failed to close cur part", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(except_ctx->hp_infras_.end_round())) {
        LOG_WARN("failed to end round", K(ret));
      } else if (OB_FAIL(except_ctx->hp_infras_.start_round())) {
        LOG_WARN("failed to open round", K(ret));
      } else if (OB_FAIL(build_hash_table_by_part(ctx, except_ctx))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to build hash table", K(ret));
        } else {
          except_ctx->iter_end_ = true;
        }
      }
    } else if (OB_FAIL(ret)) {
    } else if (OB_FAIL(try_check_status(ctx))) {
      LOG_WARN("failed to check status", K(ret));
    } else if (OB_FAIL(except_ctx->hp_infras_.exists_row(cur_row, part_cols))) {
      LOG_WARN("failed to probe exists row", K(ret));
    } else if (OB_NOT_NULL(part_cols)) {
      // exists
    } else {
      // row is not exists in hash table
      if (except_ctx->hp_infras_.has_right_dumped() || except_ctx->hp_infras_.has_left_dumped()) {
        // dump row
        if (!except_ctx->hp_infras_.has_left_dumped() &&
            OB_FAIL(except_ctx->hp_infras_.create_dumped_partitions(InputSide::LEFT))) {
          LOG_WARN("failed to create dump partitions", K(ret));
        } else if (OB_FAIL(except_ctx->hp_infras_.insert_row_on_partitions(cur_row))) {
          LOG_WARN("failed to insert row into partitions", K(ret));
        }
      } else {
        // insert and return row
        if (OB_FAIL(except_ctx->hp_infras_.insert_row(cur_row, exists, inserted))) {
          LOG_WARN("failed to insert row", K(ret));
        } else if (inserted) {
          got_row = true;
          row = cur_row;
        }
      }
    }
  }  // end of while
  if (OB_SUCC(ret)) {
    if (OB_FAIL(copy_cur_row_by_projector(*except_ctx, row))) {
      LOG_WARN("copy current row failed", K(ret));
    }
  }
  return ret;
}

int ObHashExcept::inner_close(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObHashSetOperator::inner_close(ctx))) {
    LOG_WARN("failed to close", K(ret));
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
