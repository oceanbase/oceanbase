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

#ifndef OCEANBASE_RPC_OB_RPC_REQUEST_OPERATOR_H_
#define OCEANBASE_RPC_OB_RPC_REQUEST_OPERATOR_H_

#include "rpc/ob_request.h"
namespace oceanbase
{
namespace rpc
{
class ObIRpcRequestOperator
{
public:
  ObIRpcRequestOperator() {}
  virtual ~ObIRpcRequestOperator() {}
  virtual void* alloc_response_buffer(ObRequest* req, int64_t size) = 0;
  virtual void response_result(ObRequest* req, obrpc::ObRpcPacket* pkt) = 0;
  virtual common::ObAddr get_peer(const ObRequest* req) = 0;
};

class ObRpcRequestOperator: public ObIRpcRequestOperator
{
public:
  ObRpcRequestOperator() {}
  virtual ~ObRpcRequestOperator() {}
  virtual void* alloc_response_buffer(ObRequest* req, int64_t size) override {
    return get_operator(req).alloc_response_buffer(req, size);
  }
  virtual void response_result(ObRequest* req, obrpc::ObRpcPacket* pkt) override {
    return get_operator(req).response_result(req, pkt);
  }
  virtual common::ObAddr get_peer(const ObRequest* req) override {
    return get_operator(req).get_peer(req);
  }
private:
  ObIRpcRequestOperator& get_operator(const ObRequest* req);
};

extern ObRpcRequestOperator global_rpc_req_operator;
#define RPC_REQ_OP (oceanbase::rpc::global_rpc_req_operator)
} // end of namespace rp
} // end of namespace oceanbase

#endif /* OCEANBASE_RPC_OB_RPC_REQUEST_OPERATOR_H_ */
