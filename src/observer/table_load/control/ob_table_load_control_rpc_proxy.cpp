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

#include "ob_table_load_control_rpc_proxy.h"
#include "ob_table_load_control_rpc_executor.h"

namespace oceanbase
{
namespace observer
{
using namespace common;

int ObTableLoadControlRpcProxy::dispatch(const ObDirectLoadControlRequest &request,
                                         ObDirectLoadControlResult &result, ObIAllocator &allocator)
{
#define OB_TABLE_LOAD_CONTROL_RPC_DISPATCH(pcode)                                        \
  case pcode:                                                                            \
    OB_TABLE_LOAD_RPC_PROCESS(ObTableLoadControlRpc, pcode, request, result, allocator); \
    break;

  int ret = OB_SUCCESS;
  switch (request.command_type_) {
    OB_TABLE_LOAD_CONTROL_RPC_DISPATCH(ObDirectLoadControlCommandType::PRE_BEGIN);
    OB_TABLE_LOAD_CONTROL_RPC_DISPATCH(ObDirectLoadControlCommandType::CONFIRM_BEGIN);
    OB_TABLE_LOAD_CONTROL_RPC_DISPATCH(ObDirectLoadControlCommandType::PRE_MERGE);
    OB_TABLE_LOAD_CONTROL_RPC_DISPATCH(ObDirectLoadControlCommandType::START_MERGE);
    OB_TABLE_LOAD_CONTROL_RPC_DISPATCH(ObDirectLoadControlCommandType::COMMIT);
    OB_TABLE_LOAD_CONTROL_RPC_DISPATCH(ObDirectLoadControlCommandType::ABORT);
    OB_TABLE_LOAD_CONTROL_RPC_DISPATCH(ObDirectLoadControlCommandType::GET_STATUS);
    OB_TABLE_LOAD_CONTROL_RPC_DISPATCH(ObDirectLoadControlCommandType::HEART_BEAT);
    /// trans
    OB_TABLE_LOAD_CONTROL_RPC_DISPATCH(ObDirectLoadControlCommandType::PRE_START_TRANS);
    OB_TABLE_LOAD_CONTROL_RPC_DISPATCH(ObDirectLoadControlCommandType::CONFIRM_START_TRANS);
    OB_TABLE_LOAD_CONTROL_RPC_DISPATCH(ObDirectLoadControlCommandType::PRE_FINISH_TRANS);
    OB_TABLE_LOAD_CONTROL_RPC_DISPATCH(ObDirectLoadControlCommandType::CONFIRM_FINISH_TRANS);
    OB_TABLE_LOAD_CONTROL_RPC_DISPATCH(ObDirectLoadControlCommandType::ABANDON_TRANS);
    OB_TABLE_LOAD_CONTROL_RPC_DISPATCH(ObDirectLoadControlCommandType::GET_TRANS_STATUS);
    OB_TABLE_LOAD_CONTROL_RPC_DISPATCH(ObDirectLoadControlCommandType::INSERT_TRANS);
    default:
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "unexpected command type", K(ret), K(request));
      break;
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
