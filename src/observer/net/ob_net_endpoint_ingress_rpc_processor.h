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

#ifndef OCEANBASE_ENDPOINT_INGRESS_RPC_PROCESSOR_H_
#define OCEANBASE_ENDPOINT_INGRESS_RPC_PROCESSOR_H_

#include "observer/net/ob_net_endpoint_ingress_rpc_proxy.h"
#include "observer/ob_rpc_processor_simple.h"
namespace oceanbase
{
namespace observer
{

OB_DEFINE_PROCESSOR_S(NetEndpointIngress, OB_NET_ENDPOINT_REGISTER, ObNetEndpointRegisterP);
OB_DEFINE_PROCESSOR_S(Srv, OB_PREDICT_INGRESS_BW, ObNetEndpointPredictIngressP);
OB_DEFINE_PROCESSOR_S(Srv, OB_SET_INGRESS_BW, ObNetEndpointSetIngressP);

}  // namespace observer
}  // namespace oceanbase

#endif /* OCEANBASE_ENDPOINT_INGRESS_RPC_PROCESSOR_H_ */