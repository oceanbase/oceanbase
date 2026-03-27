/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX RPC_FRAME
#include "rpc/frame/ob_req_deliver.h"

using namespace oceanbase::rpc::frame;
using namespace oceanbase::common;

ObReqQDeliver::ObReqQDeliver(ObiReqQHandler &qhandler)
    : qhandler_(qhandler)
{
  // empty
}
