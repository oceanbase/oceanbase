/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG

#include "ob_px_reduce_transmit_op.h"

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
  ret = send_rows<ObSliceIdxCalc::ALL_TO_ONE>(fixed_slice_calc);
  return ret;
}

int ObPxReduceTransmitOp::inner_close()
{
  return ObPxTransmitOp::inner_close();
}

} // end namespace sql
} // end namespace oceanbase
