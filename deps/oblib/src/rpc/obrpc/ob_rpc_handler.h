/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_RPC_OBRPC_OB_RPC_HANDLER_
#define OCEANBASE_RPC_OBRPC_OB_RPC_HANDLER_

#include "rpc/obrpc/ob_rpc_net_handler.h"
#include "rpc/frame/ob_req_deliver.h"

namespace oceanbase
{
namespace obrpc
{
// Collection of easy callback functions, implement of OceanBase RPC
// processing. Pass to deliverer once RPC packet comes.
class ObRpcHandler
    : public ObRpcNetHandler
{
public:
  explicit ObRpcHandler(rpc::frame::ObReqDeliver &deliver);
  virtual ~ObRpcHandler();

  int init();
  int process(easy_request_t *r);

private:
  rpc::frame::ObReqDeliver &deliver_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObRpcHandler);
}; // end of class ObRpcHandler

} // end of namespace obrpc
} // end of namespace oceanbase

#endif //OCEANBASE_RPC_OBRPC_OB_RPC_HANDLER_
