/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SERVER

#include "ob_table_direct_load_rpc_proxy.h"
#include "ob_table_direct_load_rpc_executor.h"

namespace oceanbase
{
namespace observer
{
using namespace table;

int ObTableDirectLoadRpcProxy::dispatch(ObTableDirectLoadExecContext &ctx,
                                        const ObTableDirectLoadRequest &request,
                                        ObTableDirectLoadResult &result)
{
#define OB_TABLE_DIRECT_LOAD_RPC_DISPATCH(pcode)                                  \
  case pcode:                                                                     \
    OB_TABLE_LOAD_RPC_PROCESS(ObTableDirectLoadRpc, pcode, request, result, ctx); \
    break;

  int ret = OB_SUCCESS;
  switch (request.header_.operation_type_) {
    OB_TABLE_DIRECT_LOAD_RPC_DISPATCH(ObTableDirectLoadOperationType::BEGIN);
    OB_TABLE_DIRECT_LOAD_RPC_DISPATCH(ObTableDirectLoadOperationType::COMMIT);
    OB_TABLE_DIRECT_LOAD_RPC_DISPATCH(ObTableDirectLoadOperationType::ABORT);
    OB_TABLE_DIRECT_LOAD_RPC_DISPATCH(ObTableDirectLoadOperationType::GET_STATUS);
    OB_TABLE_DIRECT_LOAD_RPC_DISPATCH(ObTableDirectLoadOperationType::INSERT);
    OB_TABLE_DIRECT_LOAD_RPC_DISPATCH(ObTableDirectLoadOperationType::HEART_BEAT);
    default:
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "unexpected command type", K(ret), K(request));
      break;
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
