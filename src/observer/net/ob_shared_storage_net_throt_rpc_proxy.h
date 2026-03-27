/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_NET_THROT_RPC_PROXY_H_
#define OCEANBASE_STORAGE_NET_THROT_RPC_PROXY_H_

#include "observer/net/ob_shared_storage_net_throt_rpc_struct.h"
#include "observer/ob_server_struct.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "share/ob_define.h"
#include "share/rpc/ob_async_rpc_proxy.h"

namespace oceanbase
{

namespace obrpc
{
RPC_F(OB_SHARED_STORAGE_NET_THROT_PREDICT, obrpc::ObSSNTEndpointArg, obrpc::ObSharedDeviceResourceArray,
    ObSharedStorageNetThrotPredictProxy);
RPC_F(OB_SHARED_STORAGE_NET_THROT_SET, obrpc::ObSharedDeviceResourceArray, obrpc::ObSSNTSetRes,
    ObSharedStorageNetThrotSetProxy);
class ObSSNTRpcProxy : public obrpc::ObRpcProxy
{
public:
  DEFINE_TO(ObSSNTRpcProxy);
  RPC_S(PR5 shared_storage_net_throt_register, OB_SHARED_STORAGE_NET_THROT_REGISTER, (obrpc::ObSSNTEndpointArg));
};
}  // namespace obrpc
}  // namespace oceanbase
#endif /* OCEANBASE_STORAGE_NET_THROT_RPC_PROXY_H_ */