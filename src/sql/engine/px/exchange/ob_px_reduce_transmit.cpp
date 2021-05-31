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

#include "ob_px_reduce_transmit.h"
#include "sql/dtl/ob_dtl_channel_group.h"
#include "sql/dtl/ob_dtl_rpc_channel.h"
#include "sql/executor/ob_slice_calc.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::share::schema;

OB_SERIALIZE_MEMBER((ObPxReduceTransmitInput, ObPxTransmitInput));

//////////////////////////////////////////

ObPxReduceTransmit::ObPxReduceTransmitCtx::ObPxReduceTransmitCtx(ObExecContext& ctx)
    : ObPxTransmit::ObPxTransmitCtx(ctx)
{}

ObPxReduceTransmit::ObPxReduceTransmitCtx::~ObPxReduceTransmitCtx()
{}

//////////////////////////////////////////

ObPxReduceTransmit::ObPxReduceTransmit(common::ObIAllocator& alloc) : ObPxTransmit(alloc)
{}

ObPxReduceTransmit::~ObPxReduceTransmit()
{}

int ObPxReduceTransmit::inner_open(ObExecContext& exec_ctx) const
{
  int ret = OB_SUCCESS;
  if (!is_no_repart_exchange()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expect no repartition", K(ret));
  } else if (OB_FAIL(ObPxTransmit::inner_open(exec_ctx))) {
    LOG_WARN("initialize operator context failed", K(ret));
  }
  return ret;
}

int ObPxReduceTransmit::do_transmit(ObExecContext& exec_ctx) const
{
  int ret = OB_SUCCESS;
  ObAllToOneSliceIdxCalc fixed_slice_calc(exec_ctx.get_allocator());
  ObPxReduceTransmitCtx* transmit_ctx = NULL;
  if (OB_ISNULL(transmit_ctx = GET_PHY_OPERATOR_CTX(ObPxReduceTransmitCtx, exec_ctx, get_id()))) {
    LOG_WARN("fail to op ctx", "op_id", get_id(), "op_type", get_type(), KP(transmit_ctx), K(ret));
  } else if (OB_FAIL(send_rows(exec_ctx, *transmit_ctx, fixed_slice_calc))) {
    LOG_WARN("fail send rows", K(ret));
  }
  return ret;
}

int ObPxReduceTransmit::inner_close(ObExecContext& exec_ctx) const
{
  return ObPxTransmit::inner_close(exec_ctx);
}

int ObPxReduceTransmit::init_op_ctx(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPhyOperatorCtx* op_ctx = NULL;
  if (OB_FAIL(CREATE_PHY_OPERATOR_CTX(ObPxReduceTransmitCtx, ctx, get_id(), get_type(), op_ctx))) {
    LOG_WARN("fail to create phy op ctx", K(ret), K(get_id()), K(get_type()));
  } else if (OB_ISNULL(op_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("op ctx is NULL", K(ret));
  } else if (OB_FAIL(init_cur_row(*op_ctx, has_partition_id_column_idx()))) {
    LOG_WARN("fail to int cur row", K(ret));
  }
  return ret;
}

int ObPxReduceTransmit::create_operator_input(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObIPhyOperatorInput* input = NULL;
  if (OB_FAIL(CREATE_PHY_OP_INPUT(ObPxReduceTransmitInput, ctx, get_id(), get_type(), input))) {
    LOG_WARN("fail to create phy op input", K(ret), K(get_id()), K(get_type()));
  }
  UNUSED(input);
  return ret;
}
