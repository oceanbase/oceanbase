/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SERVER

#include "ob_table_load_resource_rpc_proxy.h"
#include "ob_table_load_resource_rpc_executor.h"

namespace oceanbase
{
namespace observer
{
using namespace common;

int ObTableLoadResourceRpcProxy::dispatch(const ObDirectLoadResourceOpRequest &request,
                                          ObDirectLoadResourceOpResult &result,
                                          common::ObIAllocator &allocator)
{
#define OB_TABLE_LOAD_RESOURCE_RPC_DISPATCH(pcode)                                          \
  case pcode:                                                                               \
    OB_TABLE_LOAD_RPC_PROCESS(ObTableLoadResourceRpc, pcode, request, result, allocator);   \
    break;

  int ret = OB_SUCCESS;
  switch (request.command_type_) {
    OB_TABLE_LOAD_RESOURCE_RPC_DISPATCH(ObDirectLoadResourceCommandType::APPLY);
    OB_TABLE_LOAD_RESOURCE_RPC_DISPATCH(ObDirectLoadResourceCommandType::RELEASE);
    OB_TABLE_LOAD_RESOURCE_RPC_DISPATCH(ObDirectLoadResourceCommandType::UPDATE);
    OB_TABLE_LOAD_RESOURCE_RPC_DISPATCH(ObDirectLoadResourceCommandType::CHECK);
    default:
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "unexpected command type", K(ret), K(request));
      break;
  }

  return ret;
}

} // namespace observer
} // namespace oceanbase
