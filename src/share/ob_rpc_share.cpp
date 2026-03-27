/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "ob_rpc_share.h"
#include "deps/oblib/src/rpc/obrpc/ob_rpc_proxy.h"

namespace oceanbase {
namespace share {

rpc::frame::ObReqTransport *g_obrpc_transport = nullptr;

}  // share
}  // oceanbase
