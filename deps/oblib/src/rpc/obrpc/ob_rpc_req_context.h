/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_RPC_OBRPC_REQ_CONTEXT_
#define OCEANBASE_RPC_OBRPC_REQ_CONTEXT_

namespace oceanbase
{
namespace obrpc
{
class ObRpcStreamCond;
struct ObRpcReqContext
{
  ObRpcStreamCond *scond_;
};

} // end of namespace obrpc
} // end of namespace oceanbase

#endif //OCEANBASE_RPC_OBRPC_REQ_CONTEXT_
