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

#define USING_LOG_PREFIX SQL_EXE

#include "sql/executor/ob_direct_transmit.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase {
namespace sql {
using namespace oceanbase::common;

ObDirectTransmitInput::ObDirectTransmitInput() : ObTransmitInput()
{}

ObDirectTransmitInput::~ObDirectTransmitInput()
{}

int ObDirectTransmitInput::init(ObExecContext& ctx, ObTaskInfo& task_info, const ObPhyOperator& op)
{
  UNUSED(ctx);
  UNUSED(task_info);
  UNUSED(op);
  return OB_SUCCESS;
}

OB_SERIALIZE_MEMBER((ObDirectTransmitInput, ObTransmitInput));

ObDirectTransmit::ObDirectTransmit(common::ObIAllocator& alloc) : ObTransmit(alloc)
{}

ObDirectTransmit::~ObDirectTransmit()
{}

int ObDirectTransmit::get_next_row(ObExecContext& ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ret = ObPhyOperator::get_next_row(ctx, row);
  return ret;
}

int ObDirectTransmit::inner_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ObDirectTransmitCtx* direct_ctx = NULL;
  if (OB_ISNULL(child_op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child_op_ is NULL", K(ret));
  } else if (OB_ISNULL(direct_ctx = GET_PHY_OPERATOR_CTX(ObDirectTransmitCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("direct ctx is NULL", K(ret));
  } else if (OB_FAIL(child_op_->get_next_row(ctx, row))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("fail to get next row from child op", K(ret), K(*child_op_));
    }
  } else if (OB_FAIL(copy_cur_row(*direct_ctx, row))) {
    LOG_WARN("fail to copy current row", "op_type", ob_phy_operator_type_str(child_op_->get_type()), K(ret));
  }
  return ret;
}

int ObDirectTransmit::create_operator_input(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObIPhyOperatorInput* input = NULL;
  if (OB_FAIL(CREATE_PHY_OP_INPUT(ObDirectTransmitInput, ctx, get_id(), get_type(), input))) {
    LOG_WARN("fail to create phy op input", K(ret), K(get_id()), K(get_type()));
  }
  UNUSED(input);
  return ret;
}

int ObDirectTransmit::init_op_ctx(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPhyOperatorCtx* op_ctx = NULL;
  if (OB_UNLIKELY(calc_exprs_.get_size() > 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("calc exprs should be empty", K(ret), K(calc_exprs_.get_size()));
  } else if (OB_UNLIKELY(filter_exprs_.get_size() > 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("filter exprs should be empty", K(ret), K(filter_exprs_.get_size()));
  } else if (OB_FAIL(CREATE_PHY_OPERATOR_CTX(ObDirectTransmitCtx, ctx, get_id(), get_type(), op_ctx))) {
    LOG_WARN("fail to create phy op ctx", K(ret), K(get_id()), K(get_type()));
  } else if (OB_ISNULL((op_ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("op ctx is NULL", K(ret));
  } else if (OB_FAIL(init_cur_row(*op_ctx, false))) {
    LOG_WARN("fail to init cur row", K(ret));
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
