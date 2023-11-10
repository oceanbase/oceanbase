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

#include "observer/net/ob_net_endpoint_ingress_rpc_processor.h"
#include "observer/ob_srv_network_frame.h"

#define USING_LOG_PREFIX RPC

namespace oceanbase
{
using namespace common;

namespace observer
{

int ObNetEndpointRegisterP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.net_frame_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("net_frame in GCTX is null", KR(ret));
  } else if (OB_FAIL(gctx_.net_frame_->net_endpoint_register(arg_.endpoint_key_, arg_.expire_time_))) {
    LOG_WARN("failed to net_endpoint_register", KR(ret), K(arg_));
  }
  return ret;
}
int ObNetEndpointPredictIngressP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.net_frame_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("net_frame in GCTX is null", KR(ret));
  } else if (!arg_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg_), KR(ret));
  } else if (OB_FAIL(gctx_.net_frame_->net_endpoint_predict_ingress(arg_.endpoint_key_, result_.predicted_bw_))) {
    LOG_WARN("failed to net_endpoint_predict_ingress", KR(ret), K(arg_));
  }
  return ret;
}

int ObNetEndpointSetIngressP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.net_frame_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("net_frame in GCTX is null", KR(ret));
  } else if (!arg_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg_), KR(ret));
  } else if (OB_FAIL(gctx_.net_frame_->net_endpoint_set_ingress(arg_.endpoint_key_, arg_.assigned_bw_))) {
    LOG_WARN("failed to net_endpoint_set_ingress", KR(ret), K(arg_));
  }
  return ret;
}

}  // namespace observer
}  // namespace oceanbase