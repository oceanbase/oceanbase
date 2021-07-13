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
#include "lib/allocator/ob_malloc.h"
#include "lib/utility/utility.h"
#include "sql/engine/aggregate/ob_merge_distinct.h"
#include "common/row/ob_row.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase {
using namespace common;

namespace sql {
class ObMergeDistinct::ObMergeDistinctCtx : public ObPhyOperatorCtx {
public:
  explicit ObMergeDistinctCtx(ObExecContext& ctx)
      : ObPhyOperatorCtx(ctx),
        got_first_row_(false),
        last_row_(),
        last_row_buf_(NULL),
        last_row_buf_size_(OB_MAX_ROW_LENGTH)
  {}

  int alloc_last_row_buf(const int64_t column_count)
  {
    int ret = OB_SUCCESS;
    int64_t row_size = column_count * sizeof(ObObj);
    void* ptr = NULL;
    if (OB_UNLIKELY(NULL == (last_row_buf_ = exec_ctx_.get_allocator().alloc(last_row_buf_size_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc last row buffer failed", K_(last_row_buf_size));
    } else if (OB_UNLIKELY(NULL == (ptr = exec_ctx_.get_allocator().alloc(row_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc last row cell memory failed", K(row_size));
    } else {
      last_row_.cells_ = new (ptr) common::ObObj[column_count];
      last_row_.count_ = column_count;
    }
    return ret;
  }

  int store_last_row(const ObNewRow& row)
  {
    int ret = OB_SUCCESS;
    ObDataBuffer data_buf(static_cast<char*>(last_row_buf_), OB_ROW_BUF_SIZE);

    if (OB_UNLIKELY(row.is_invalid()) || OB_UNLIKELY(last_row_.is_invalid()) ||
        OB_UNLIKELY(row.get_count() != last_row_.get_count())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid_argument", K(row), K(last_row_));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < row.get_count(); ++i) {
      if (OB_FAIL(ob_write_obj(data_buf, row.get_cell(i), last_row_.cells_[i]))) {
        LOG_WARN("write obj failed", K(ret), K(i), K(row));
      }
    }
    // give second chance
    if (OB_NOT_NULL(last_row_buf_) && OB_ALLOCATE_MEMORY_FAILED == ret && OB_MAX_ROW_LENGTH == last_row_buf_size_) {
      exec_ctx_.get_allocator().free(last_row_buf_);
      last_row_buf_size_ = OB_MAX_ROW_LENGTH_IN_MEMTABLE;
      if (OB_ISNULL(last_row_buf_ = exec_ctx_.get_allocator().alloc(last_row_buf_size_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SQL_ENG_LOG(ERROR, "alloc last row buffer failed", K_(last_row_buf_size));
      } else {
        ret = store_last_row(row);
      }
    }
    return ret;
  }
  void free_last_row_buf()
  {
    if (last_row_buf_) {
      exec_ctx_.get_allocator().free(last_row_buf_);
      last_row_buf_ = NULL;
    }
    if (last_row_.cells_) {
      exec_ctx_.get_allocator().free(last_row_.cells_);
      last_row_.cells_ = NULL;
    }
  }
  virtual void destroy()
  {
    ObPhyOperatorCtx::destroy_base();
  }

private:
  static const int64_t OB_ROW_BUF_SIZE = OB_MAX_ROW_LENGTH;

private:
  bool got_first_row_;
  ObNewRow last_row_;
  void* last_row_buf_;
  int64_t last_row_buf_size_;
  friend class ObMergeDistinct;
};

ObMergeDistinct::ObMergeDistinct(common::ObIAllocator& alloc) : ObDistinct(alloc)
{}

ObMergeDistinct::~ObMergeDistinct()
{}

void ObMergeDistinct::reset()
{
  ObDistinct::reset();
}

void ObMergeDistinct::reuse()
{
  ObDistinct::reuse();
}

int ObMergeDistinct::init_op_ctx(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPhyOperatorCtx* op_ctx = NULL;
  if (OB_FAIL(CREATE_PHY_OPERATOR_CTX(ObMergeDistinctCtx, ctx, get_id(), get_type(), op_ctx))) {
    LOG_WARN("create physical operator context failed", K(ret));
  } else if (OB_ISNULL(op_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("op_ctx is null", K(ret));
  } else {
    if (OB_FAIL(init_cur_row(*op_ctx, need_copy_row_for_compute()))) {
      LOG_WARN("init current row failed", K(ret));
    }
  }
  return ret;
}

int ObMergeDistinct::inner_open(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObMergeDistinctCtx* distinct_ctx = NULL;

  if (OB_FAIL(init_op_ctx(ctx))) {
    LOG_WARN("initialize operator context failed", K(ret));
  } else if (OB_ISNULL(child_op_) ||
             OB_ISNULL(distinct_ctx = GET_PHY_OPERATOR_CTX(ObMergeDistinctCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get physical operator context failed", K_(child_op));
  } else {
    int64_t real_col_cnt = child_op_->get_output_count();
    if (OB_FAIL(distinct_ctx->alloc_last_row_buf(real_col_cnt))) {
      LOG_WARN("alloc last row buffer failed", K(ret), "column_count", real_col_cnt);
    }
  }
  return ret;
}

int ObMergeDistinct::rescan(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObMergeDistinctCtx* distinct_ctx = NULL;
  if (OB_FAIL(ObSingleChildPhyOperator::rescan(ctx))) {
    LOG_WARN("rescan child operator failed", K(ret));
  } else if (OB_ISNULL(distinct_ctx = GET_PHY_OPERATOR_CTX(ObMergeDistinctCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("distinct ctx is null");
  } else {
    distinct_ctx->got_first_row_ = false;
  }
  return ret;
}

int ObMergeDistinct::inner_close(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObMergeDistinctCtx* phy_op_ctx = NULL;
  if (OB_ISNULL(phy_op_ctx = GET_PHY_OPERATOR_CTX(ObMergeDistinctCtx, ctx, get_id()))) {
    LOG_DEBUG("The operator has not been opened.", K(ret), K_(id), "op_type", ob_phy_operator_type_str(get_type()));
  } else {
    phy_op_ctx->free_last_row_buf();
  }
  return ret;
}

int ObMergeDistinct::inner_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ObMergeDistinctCtx* phy_op_ctx = NULL;
  const ObNewRow* this_row = NULL;
  bool got_distinct_row = false;
  bool equal = false;

  if (OB_ISNULL(child_op_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("child_op is null");
  } else if (OB_ISNULL(phy_op_ctx = GET_PHY_OPERATOR_CTX(ObMergeDistinctCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get physical operator context failed", K(ret));
  } else if (phy_op_ctx->got_first_row_) {
    while (OB_SUCC(ret) && !got_distinct_row) {
      if (OB_FAIL(child_op_->get_next_row(ctx, this_row))) {
        if (ret != OB_ITER_END) {
          LOG_WARN("child operator get next row failed", K(ret));
        }
      } else if (OB_ISNULL(this_row)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("this row is null");
      } else {
        // compare current_row and last_row
        if (OB_FAIL(compare_equal(*this_row, phy_op_ctx->last_row_, equal))) {
          LOG_WARN("fail to compare this_row and last_row_", K(ret), K(*this_row), K_(phy_op_ctx->last_row));
        } else if (!equal) {
          got_distinct_row = true;
          /* return value */
          row = this_row;
          /* save this row to local buffer. last_row_buf_ reused */
          if (OB_FAIL(phy_op_ctx->store_last_row(*this_row))) {
            LOG_WARN("fail to storage row to operator context", K(ret), K(*this_row));
          }
        }
      }
    }      /* end while */
  } else { /* first row, always output */
    if (OB_FAIL(child_op_->get_next_row(ctx, row))) {
      if (ret != OB_ITER_END) {
        LOG_WARN("fail to get next row", K(ret));
      }
    } else if (OB_FAIL(phy_op_ctx->store_last_row(*row))) {
      LOG_WARN("fail to storage row to operator context", K(ret), K(*row));
    } else {
      phy_op_ctx->got_first_row_ = true;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(copy_cur_row(*phy_op_ctx, row))) {
      LOG_WARN("copy current row failed", K(ret));
    }
  }
  return ret;
}

int ObMergeDistinct::compare_equal(const ObNewRow& this_row, const ObNewRow& last_row, bool& result) const
{
  int ret = OB_SUCCESS;
  int64_t column_count = 0;
  int64_t col_index = 0;
  ObCollationType cs_type = CS_TYPE_INVALID;
  bool cmp_val = true;

  result = false;
  if (OB_UNLIKELY(this_row.is_invalid()) || OB_UNLIKELY(last_row.is_invalid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("compared row invalid", K(this_row), K(last_row));
  } else {
    column_count = distinct_columns_.count();
    for (int64_t i = 0; OB_SUCC(ret) && cmp_val && i < column_count; ++i) {
      col_index = distinct_columns_.at(i).index_;
      cs_type = distinct_columns_.at(i).cs_type_;
      if (col_index < 0 || col_index >= this_row.get_count() || col_index >= last_row.get_count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("col_index is invalid", K(col_index), K(this_row.get_count()), K(last_row.get_count()));
      } else if (this_row.get_cell(col_index).compare(last_row.get_cell(col_index), cs_type) != 0) {
        cmp_val = false;
      }
    } /* end for */
  }
  if (OB_SUCC(ret)) {
    result = cmp_val;
  }
  return ret;
}

int64_t ObMergeDistinct::to_string_kv(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_KV(N_DISTINCT, distinct_columns_);
  return pos;
}

OB_SERIALIZE_MEMBER((ObMergeDistinct, ObSingleChildPhyOperator), distinct_columns_);
}  // namespace sql
}  // namespace oceanbase
