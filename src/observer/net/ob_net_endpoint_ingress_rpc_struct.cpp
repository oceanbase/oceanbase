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

#include "observer/net/ob_net_endpoint_ingress_rpc_struct.h"
#include "observer/ob_server_struct.h"
#include "observer/ob_srv_network_frame.h"

#define USING_LOG_PREFIX RPC

namespace oceanbase
{
using namespace common;

namespace obrpc
{

OB_SERIALIZE_MEMBER(ObNetEndpointKey, addr_, group_id_);
OB_SERIALIZE_MEMBER(ObNetEndpointValue, predicted_bw_, assigned_bw_, expire_time_);

OB_SERIALIZE_MEMBER(ObNetEndpointRegisterArg, endpoint_key_, expire_time_);
int ObNetEndpointRegisterArg::assign(const ObNetEndpointRegisterArg &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(endpoint_key_.assign(other.endpoint_key_))) {
    LOG_WARN("fail to assign endpoint_key", KR(ret));
  } else {
    expire_time_ = other.expire_time_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObNetEndpointPredictIngressArg, endpoint_key_);
int ObNetEndpointPredictIngressArg::assign(const ObNetEndpointPredictIngressArg &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(endpoint_key_.assign(other.endpoint_key_))) {
    LOG_WARN("fail to assign endpoint_key", KR(ret));
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObNetEndpointPredictIngressRes, predicted_bw_);
int ObNetEndpointPredictIngressRes::assign(const ObNetEndpointPredictIngressRes &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    predicted_bw_ = other.predicted_bw_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObNetEndpointSetIngressArg, endpoint_key_, assigned_bw_);
int ObNetEndpointSetIngressArg::assign(const ObNetEndpointSetIngressArg &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(endpoint_key_.assign(other.endpoint_key_))) {
    LOG_WARN("fail to assign endpoint_key", KR(ret));
  } else {
    assigned_bw_ = other.assigned_bw_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObNetEndpointSetIngressRes, res_);
int ObNetEndpointSetIngressRes::assign(const ObNetEndpointSetIngressRes &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    res_ = other.res_;
  }
  return OB_SUCCESS;
}
}  // namespace obrpc
}  // namespace oceanbase