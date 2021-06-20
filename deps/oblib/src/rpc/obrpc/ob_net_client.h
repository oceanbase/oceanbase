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

#ifndef OCEANBASE_RPC_OBRPC_OB_NET_CLIENT_
#define OCEANBASE_RPC_OBRPC_OB_NET_CLIENT_

#include "io/easy_io_struct.h"
#include "rpc/frame/ob_net_easy.h"
#include "rpc/obrpc/ob_rpc_handler.h"
#include "rpc/frame/ob_req_transport.h"

namespace oceanbase {
namespace common {
class ObAddr;
}  // end of namespace common

namespace obrpc {
class ObRpcProxy;

class ObNetClient {
public:
  ObNetClient();
  virtual ~ObNetClient();

  int init();
  int init(const rpc::frame::ObNetOptions opts);
  void destroy();
  int get_proxy(ObRpcProxy& proxy);

private:
  int init_(const rpc::frame::ObNetOptions opts);

private:
  bool inited_;
  rpc::frame::ObNetEasy net_;
  ObRpcNetHandler pkt_handler_;
  rpc::frame::ObReqTransport* transport_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObNetClient);
};  // end of class ObNetClient

}  // namespace obrpc
}  // end of namespace oceanbase

#endif  // OCEANBASE_RPC_OBRPC_OB_NET_CLIENT_
