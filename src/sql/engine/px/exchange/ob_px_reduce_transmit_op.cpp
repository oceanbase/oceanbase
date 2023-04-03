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

#include "ob_px_reduce_transmit_op.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

OB_SERIALIZE_MEMBER((ObPxReduceTransmitOpInput, ObPxTransmitOpInput));

OB_SERIALIZE_MEMBER((ObPxReduceTransmitSpec, ObPxTransmitSpec));

int ObPxReduceTransmitOp::inner_open()
{
  int ret = OB_SUCCESS;

  if (!MY_SPEC.is_no_repart_exchange()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expect no repartition", K(ret));
  } else if (OB_FAIL(ObPxTransmitOp::inner_open())) {
    LOG_WARN("initialize operator context failed", K(ret));
  }
  return ret;
}

int ObPxReduceTransmitOp::do_transmit()
{
  int ret = OB_SUCCESS;
  ObAllToOneSliceIdxCalc fixed_slice_calc(ctx_.get_allocator());
  ret = send_rows(fixed_slice_calc);
  return ret;
}

int ObPxReduceTransmitOp::inner_close()
{
  return ObPxTransmitOp::inner_close();
}

} // end namespace sql
} // end namespace oceanbase
