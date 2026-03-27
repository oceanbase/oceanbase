/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "ob_common_rpc_proxy.h"

namespace oceanbase {
namespace obrpc {

#define OB_RPC_CLASS ObCommonRpcProxy
#include "ob_common_rpc_proxy.ipp"
#undef OB_RPC_CLASS

}
}