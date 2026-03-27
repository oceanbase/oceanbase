/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEABASE_OBSERVER_OB_SRV_RPC_HANDLER_H_
#define OCEABASE_OBSERVER_OB_SRV_RPC_HANDLER_H_

#include "rpc/obrpc/ob_rpc_handler.h"

namespace oceanbase
{
namespace observer
{
class ObSrvRpcHandler : public obrpc::ObRpcHandler
{
public:
  explicit ObSrvRpcHandler(rpc::frame::ObReqDeliver &deliver);
  virtual ~ObSrvRpcHandler() {}
  virtual int on_close(easy_connection_t *c);
};

} // end of namespace observer
} // end of namespace oceanbase

#endif /* OCEABASE_OBSERVER_OB_SRV_RPC_HANDLER_H_ */