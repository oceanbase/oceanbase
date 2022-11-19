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
