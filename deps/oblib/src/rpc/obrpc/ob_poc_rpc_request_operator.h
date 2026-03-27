/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_OBRPC_OB_POC_RPC_REQUEST_OPERATOR_H_
#define OCEANBASE_OBRPC_OB_POC_RPC_REQUEST_OPERATOR_H_
#include "rpc/ob_rpc_request_operator.h"

namespace oceanbase
{
namespace obrpc
{
class ObPocRpcRequestOperator: public rpc::ObIRpcRequestOperator
{
public:
  ObPocRpcRequestOperator() {}
  virtual ~ObPocRpcRequestOperator() {}
  virtual void* alloc_response_buffer(rpc::ObRequest* req, int64_t size) override;
  virtual void response_result(rpc::ObRequest* req, obrpc::ObRpcPacket* pkt) override;
  virtual common::ObAddr get_peer(const rpc::ObRequest* req) override;
  virtual void set_trace_point(const rpc::ObRequest* req, int32_t trace_point) override;
};

}; // end namespace obrpc
}; // end namespace oceanbase

#endif /* OCEANBASE_OBRPC_OB_POC_RPC_REQUEST_OPERATOR_H_ */

