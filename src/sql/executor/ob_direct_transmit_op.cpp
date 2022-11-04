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
