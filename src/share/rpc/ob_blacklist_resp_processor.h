/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_RPC_OB_BLACKLIST_RESP_PROCESSOR_H_
#define OCEANBASE_RPC_OB_BLACKLIST_RESP_PROCESSOR_H_

#include "rpc/obrpc/ob_rpc_processor.h"
#include "share/rpc/ob_blacklist_proxy.h"

namespace oceanbase
{
namespace obrpc
{
class ObBlacklistRespP : public ObRpcProcessor< obrpc::ObBlacklistRpcProxy::ObRpc<OB_SERVER_BLACKLIST_RESP> >
{
public:
  ObBlacklistRespP() {}
  ~ObBlacklistRespP() {}
protected:
  int process();
private:
  DISALLOW_COPY_AND_ASSIGN(ObBlacklistRespP);
};
}; // end namespace rpc
}; // end namespace oceanbase

#endif /* OCEANBASE_RPC_OB_BLACKLIST_RESP_PROCESSOR_H_ */
