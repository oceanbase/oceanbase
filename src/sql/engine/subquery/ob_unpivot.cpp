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
#include "sql/engine/subquery/ob_unpivot.h"
#include "sql/engine/ob_phy_operator_type.h"
#include "sql/engine/ob_exec_context.h"
namespace oceanbase {
using namespace common;
namespace sql {
class ObUnpivot::ObUnpivotCtx : public ObPhyOperatorCtx {
public:
  explicit ObUnpivotCtx(ObExecContext& ctx)
      : ObPhyOperatorCtx(ctx), last_input_row_(NULL), is_end_(false), curr_part_idx_(common::OB_INVALID_INDEX)
  {}
  void reset()
  {
    last_input_row_ = NULL;
    is_end_ = false;
    curr_part_idx_ = common::OB_INVALID_INDEX;
  }
  virtual void destroy()
  {
    ObPhyOperatorCtx::destroy_base();
  }
  friend class ObUnpivot;

public:
  const ObNewRow* last_input_row_;
  bool is_end_;
  int64_t curr_part_idx_;
};

ObUnpivot::ObUnpivot(ObIAllocator& alloc) : ObSingleChildPhyOperator(alloc), output_indexs_(alloc), unpivot_info_()
{}

ObUnpivot::~ObUnpivot()
{}

OB_SERIALIZE_MEMBER((ObUnpivot, ObSingleChildPhyOperator), output_indexs_, unpivot_info_);

void ObUnpivot::reset()
{
  unpivot_info_.reset();
  output_indexs_.reset();
  ObSingleChildPhyOperator::reset();
}

int ObUnpivot::add_output_index(int64_t index)
{
  return output_indexs_.push_back(index);
}

int ObUnpivot::init_op_ctx(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPhyOperatorCtx* op_ctx = NULL;
  if (OB_FAIL(CREATE_PHY_OPERATOR_CTX(ObUnpivotCtx, ctx, get_id(), get_type(), op_ctx))) {
    LOG_WARN("failed to create SubQueryCtx", K(ret));
  } else if (OB_ISNULL(op_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("op_ctx is null");
  } else if (OB_FAIL(op_ctx->create_cur_row(get_column_count(), projector_, projector_size_))) {
    LOG_WARN("create current row failed", K(ret));
  } else {
    static_cast<ObUnpivotCtx*>(op_ctx)->reset();
  }
  return ret;
}

int ObUnpivot::inner_open(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_op_ctx(ctx))) {
    LOG_WARN("init subquery scan context failed", K(ret));
  }
  return ret;
}

int ObUnpivot::rescan(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObUnpivotCtx* unpivot_ctx = NULL;
  if (OB_ISNULL(unpivot_ctx = GET_PHY_OPERATOR_CTX(ObUnpivotCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get subquery scan context failed", K(ret));
  } else {
    unpivot_ctx->reset();
    ret = ObSingleChildPhyOperator::rescan(ctx);
  }
  // do noting, just rescan child operator
  return ret;
}

int ObUnpivot::inner_close(ObExecContext& ctx) const
{
  UNUSED(ctx);
  return OB_SUCCESS;
}

int ObUnpivot::inner_get_next_row(ObExecContext& ctx, const ObNewRow*& row) const
{
  // The role of ObUnpivot is similar to that of SubQueryScan, which generates new rows from
  // the results of the iteration in the subquery according to the data required by the upper query
  // but need transpose unpivot_mocked_column to new row
  int ret = OB_SUCCESS;
  ObUnpivotCtx* unpivot_ctx = NULL;
  if (OB_ISNULL(unpivot_ctx = GET_PHY_OPERATOR_CTX(ObUnpivotCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get subquery scan context failed", K(ret));
  } else {
    const ObNewRow* input_row = NULL;
    ObNewRow& output_row = unpivot_ctx->get_cur_row();
    bool got_next_row = false;
    const int64_t max_part_count =
        (output_indexs_.count() - unpivot_info_.old_column_count_) / unpivot_info_.get_new_column_count();
    if (unpivot_ctx->curr_part_idx_ <= 0 || unpivot_ctx->curr_part_idx_ >= max_part_count) {
      if (OB_FAIL(child_op_->get_next_row(ctx, input_row))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get next row from child operator failed", K(ret));
        }
      } else if (OB_ISNULL(input_row)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("input_row is null");
      } else {
        unpivot_ctx->curr_part_idx_ = 0;
        unpivot_ctx->last_input_row_ = input_row;
        got_next_row = true;
      }
    } else {
      input_row = unpivot_ctx->last_input_row_;
    }

    LOG_DEBUG("arrive unpivot",
        K(ret),
        KPC(input_row),
        K(output_row),
        K(output_indexs_),
        K(unpivot_ctx->curr_part_idx_),
        K(max_part_count),
        K(unpivot_info_));

    if (!unpivot_info_.is_include_null_) {
      bool need_try_next_part = true;
      while (need_try_next_part && OB_SUCC(ret)) {
        if (unpivot_ctx->curr_part_idx_ >= max_part_count) {
          if (got_next_row) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("has already get next row once, maybe filter does not work", K(ret), KPC(input_row));
          } else if (OB_FAIL(child_op_->get_next_row(ctx, input_row))) {
            if (OB_ITER_END != ret) {
              LOG_WARN("get next row from child operator failed", K(ret));
            }
          } else if (OB_ISNULL(input_row)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("input_row is null");
          } else {
            unpivot_ctx->curr_part_idx_ = 0;
            unpivot_ctx->last_input_row_ = input_row;
            got_next_row = true;
          }
        }

        int64_t null_count = 0;
        const int64_t base_idx = unpivot_ctx->curr_part_idx_ * unpivot_info_.get_new_column_count() +
                                 unpivot_info_.old_column_count_ + unpivot_info_.for_column_count_;
        for (int64_t i = 0; OB_SUCC(ret) && i < unpivot_info_.unpivot_column_count_; ++i) {
          const int64_t col_idx = output_indexs_.at(i + base_idx);
          if (OB_UNLIKELY(col_idx >= input_row->get_count())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("row not match", K(ret), K(i), K(base_idx), K(col_idx), K(input_row->get_count()), KPC(input_row));
          } else {
            null_count += input_row->get_cell(col_idx).is_null_oracle();
          }
        }

        if (OB_SUCC(ret)) {
          if (null_count == unpivot_info_.unpivot_column_count_) {
            LOG_DEBUG(
                "is null, try next row", K(ret), K(unpivot_ctx->curr_part_idx_), K(unpivot_info_), KPC(input_row));
            unpivot_ctx->curr_part_idx_++;
          } else {
            need_try_next_part = false;
          }
        }
      }  // end of while
    }    // end of exclude null

    if (OB_SUCC(ret)) {
      int64_t output_idx = 0;
      for (; OB_SUCC(ret) && output_idx < unpivot_info_.old_column_count_; ++output_idx) {
        const int64_t col_idx = output_indexs_.at(output_idx);
        if (OB_UNLIKELY(col_idx >= input_row->get_count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("row not match", K(output_idx), K(col_idx), K(output_row), KPC(input_row));
        } else {
          output_row.cells_[output_idx] = input_row->get_cell(col_idx);
        }
      }
      const int64_t base_idx = unpivot_ctx->curr_part_idx_ * unpivot_info_.get_new_column_count();
      for (; OB_SUCC(ret) && output_idx < output_row.get_count(); ++output_idx) {
        int64_t col_idx = output_indexs_.at(output_idx + base_idx);
        if (OB_UNLIKELY(col_idx >= input_row->get_count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("row not match", K(ret), K(col_idx), K(output_row), KPC(input_row));
        } else {
          output_row.cells_[output_idx] = input_row->get_cell(col_idx);
        }
      }
    }

    if (OB_SUCC(ret)) {
      row = &output_row;
      LOG_DEBUG("output next row", K(unpivot_ctx->curr_part_idx_), K(unpivot_info_), K(output_row), KPC(input_row));
      ++unpivot_ctx->curr_part_idx_;
    }
  }
  return ret;
}

int64_t ObUnpivot::to_string_kv(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_KV(K_(output_indexs), K_(unpivot_info));
  return pos;
}
}  // namespace sql
}  // namespace oceanbase
