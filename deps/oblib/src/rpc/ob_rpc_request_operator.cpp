/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "ob_rpc_request_operator.h"
#include "rpc/obrpc/ob_easy_rpc_request_operator.h"
#include "rpc/obrpc/ob_poc_rpc_request_operator.h"
#include "lib/stat/ob_diagnostic_info_guard.h"
#include "lib/stat/ob_diagnostic_info_container.h"

namespace oceanbase
{
using namespace obrpc;
namespace rpc
{
ObEasyRpcRequestOperator global_easy_req_operator;
ObPocRpcRequestOperator global_poc_req_operator;
ObIRpcRequestOperator& ObRpcRequestOperator::get_operator(const ObRequest* req)
{
  ObIRpcRequestOperator* op = NULL;
  switch(req->get_nio_protocol()) {
    case ObRequest::TRANSPORT_PROTO_POC:
      op = &global_poc_req_operator;
      break;
    default:
      op = &global_easy_req_operator;
  }
  return *op;
}

void ObRpcRequestOperator::response_result(ObRequest* req, obrpc::ObRpcPacket* pkt) {
  common::ObDiagnosticInfo *di = req->get_diagnostic_info();
  if (OB_NOT_NULL(di)) {
    req->reset_diagnostic_info();
  }
  return get_operator(req).response_result(req, pkt);
}

ObRpcRequestOperator global_rpc_req_operator;
}; // end namespace rpc
}; // end namespace oceanbase

