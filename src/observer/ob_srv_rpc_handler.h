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