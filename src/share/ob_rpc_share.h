/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_RPC_SHARE_H
#define OB_RPC_SHARE_H

#include "rpc/obrpc/ob_rpc_proxy.h"

namespace oceanbase {
namespace rpc {
namespace frame {
class ObReqTransport;
}  // frame
}  // rpc

namespace share {
extern rpc::frame::ObReqTransport *g_obrpc_transport;

OB_INLINE void set_obrpc_transport(rpc::frame::ObReqTransport *obrpc_transport)
{
  g_obrpc_transport = obrpc_transport;
}

OB_INLINE int init_obrpc_proxy(obrpc::ObRpcProxy &proxy)
{
  return proxy.init(g_obrpc_transport);
}

}  // share
}  // oceanbase

#endif /* OB_RPC_SHARE_H */
