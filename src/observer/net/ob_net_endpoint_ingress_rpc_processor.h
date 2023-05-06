
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