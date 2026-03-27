/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
  virtual void set_trace_point(const ObRequest* req, int32_t trace_point) {};
};

class ObRpcRequestOperator: public ObIRpcRequestOperator
{
public:
  ObRpcRequestOperator() {}
  virtual ~ObRpcRequestOperator() {}
  virtual void* alloc_response_buffer(ObRequest* req, int64_t size) override {
    return get_operator(req).alloc_response_buffer(req, size);
  }
  virtual void response_result(ObRequest* req, obrpc::ObRpcPacket* pkt) override;
  virtual common::ObAddr get_peer(const ObRequest* req) override {
    return get_operator(req).get_peer(req);
  }
  virtual void set_trace_point(const ObRequest* req, int32_t trace_point) {
    get_operator(req).set_trace_point(req, trace_point);
  }
private:
  ObIRpcRequestOperator& get_operator(const ObRequest* req);
};

extern ObRpcRequestOperator global_rpc_req_operator;
#define RPC_REQ_OP (oceanbase::rpc::global_rpc_req_operator)
} // end of namespace rp
} // end of namespace oceanbase

#endif /* OCEANBASE_RPC_OB_RPC_REQUEST_OPERATOR_H_ */
