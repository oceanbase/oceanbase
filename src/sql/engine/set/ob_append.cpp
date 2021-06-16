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
#include "sql/engine/set/ob_append.h"
#include "common/object/ob_object.h"
#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase {
using namespace common;
namespace sql {

class ObAppend::ObAppendCtx : public ObPhyOperatorCtx {
public:
  explicit ObAppendCtx(ObExecContext& ctx) : ObPhyOperatorCtx(ctx), current_child_op_idx_(0)
  {}
  ~ObAppendCtx()
  {}
  virtual void destroy()
  {
    ObPhyOperatorCtx::destroy_base();
  }

public:
  int64_t current_child_op_idx_;
};

ObAppend::ObAppend(common::ObIAllocator& alloc) : ObMultiChildrenPhyOperator(alloc)
{}

ObAppend::~ObAppend()
{}

void ObAppend::reset()
{
  ObMultiChildrenPhyOperator::reset();
}

void ObAppend::reuse()
{
  ObMultiChildrenPhyOperator::reuse();
}

int ObAppend::inner_get_next_row(ObExecContext& ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ObAppendCtx* append_ctx = NULL;
  if (OB_ISNULL(append_ctx = GET_PHY_OPERATOR_CTX(ObAppendCtx, ctx, get_id()))) {
    ret = OB_ERR_NULL_VALUE;
  } else if (append_ctx->current_child_op_idx_ >= child_num_ || append_ctx->current_child_op_idx_ < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error. current child op idx is out of range", K(ret), K(append_ctx->current_child_op_idx_));
  } else if (OB_ISNULL(child_array_[append_ctx->current_child_op_idx_])) {
    ret = OB_NOT_INIT;
    LOG_WARN("child_op_ is null");
  } else if (child_array_[append_ctx->current_child_op_idx_]->is_dml_operator()) {
    ret = OB_ITER_END;
  } else {
    ret = child_array_[append_ctx->current_child_op_idx_]->get_next_row(ctx, row);
    if (OB_SUCCESS != ret && OB_ITER_END != ret) {
      LOG_WARN("fail to get next row", K(ret));
    }
  }
  return ret;
}

int ObAppend::get_next_row(ObExecContext& ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ObAppendCtx* append_ctx = NULL;
  if (OB_ISNULL(append_ctx = GET_PHY_OPERATOR_CTX(ObAppendCtx, ctx, get_id()))) {
    ret = OB_ERR_NULL_VALUE;
  } else {
    while (true) {
      ret = inner_get_next_row(ctx, row);
      if (OB_SUCC(ret)) {
        break;
      } else if (OB_ITER_END == ret) {
        if (append_ctx->current_child_op_idx_ < child_num_ - 1) {
          ++append_ctx->current_child_op_idx_;
          // go on iterating
        } else {
          // iterate ended. really.
          break;
        }
      } else {
        LOG_WARN("inner get next row failed", K(ret));
        break;
      }
    }
  }
  return ret;
}

int ObAppend::init_op_ctx(ObExecContext& ctx) const
{
  ObPhyOperatorCtx* op_ctx = NULL;
  UNUSED(op_ctx);
  return CREATE_PHY_OPERATOR_CTX(ObAppendCtx, ctx, get_id(), get_type(), op_ctx);
}

int ObAppend::inner_open(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_op_ctx(ctx))) {
    LOG_WARN("init operator context failed", K(ret), K_(id));
  } else if (OB_FAIL(handle_op_ctx(ctx))) {
    LOG_WARN("handle op ctx failed", K(ret));
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
