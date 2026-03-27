/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_ENDPOINT_INGRESS_RPC_PROXY_H_
#define OCEANBASE_ENDPOINT_INGRESS_RPC_PROXY_H_

#include "share/ob_define.h"
#include "observer/ob_server_struct.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "share/rpc/ob_async_rpc_proxy.h"
#include "observer/net/ob_net_endpoint_ingress_rpc_struct.h"

namespace oceanbase
{

namespace obrpc
{

RPC_F(OB_PREDICT_INGRESS_BW, obrpc::ObNetEndpointPredictIngressArg, obrpc::ObNetEndpointPredictIngressRes,
    ObNetEndpointPredictIngressProxy);
RPC_F(OB_SET_INGRESS_BW, obrpc::ObNetEndpointSetIngressArg, obrpc::ObNetEndpointSetIngressRes,
    ObNetEndpointSetIngressProxy);

class ObNetEndpointIngressRpcProxy : public obrpc::ObRpcProxy
{
public:
  DEFINE_TO(ObNetEndpointIngressRpcProxy);
  RPC_S(PR5 net_endpoint_register, OB_NET_ENDPOINT_REGISTER, (obrpc::ObNetEndpointRegisterArg));
};
}  // namespace obrpc
}  // namespace oceanbase
#endif /* OCEANBASE_ENDPOINT_INGRESS_RPC_PROXY_H_ */