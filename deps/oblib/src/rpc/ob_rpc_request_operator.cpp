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

#include "rpc/ob_rpc_request_operator.h"
#include "rpc/obrpc/ob_easy_rpc_request_operator.h"
#include "rpc/obrpc/ob_poc_rpc_request_operator.h"
#include "rpc/obrpc/ob_rpc_opts.h"

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

ObRpcRequestOperator global_rpc_req_operator;
}; // end namespace rpc
}; // end namespace oceanbase

