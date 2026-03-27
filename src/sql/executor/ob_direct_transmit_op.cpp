/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_EXE

#include "sql/executor/ob_direct_transmit_op.h"

namespace oceanbase
{
namespace sql
{
using namespace oceanbase::common;

int ObDirectTransmitOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(child_->get_next_row()) && OB_ITER_END != ret) {
    LOG_WARN("get next row from child failed", K(ret));
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObDirectTransmitOpInput, ObTransmitOpInput));

OB_SERIALIZE_MEMBER((ObDirectTransmitSpec, ObTransmitSpec));

} /* ns sql */
} /* ns oceanbase */
