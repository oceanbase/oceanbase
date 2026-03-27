/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_RPC_OB_EASY_RPC_REQUEST_OPERATOR_H_
#define OCEANBASE_RPC_OB_EASY_RPC_REQUEST_OPERATOR_H_
#include "rpc/ob_rpc_request_operator.h"

namespace oceanbase
{
namespace obrpc
{
class ObEasyRpcRequestOperator: public rpc::ObIRpcRequestOperator
{
public:
  ObEasyRpcRequestOperator() {}
  virtual ~ObEasyRpcRequestOperator() {}
  virtual void* alloc_response_buffer(rpc::ObRequest* req, int64_t size) override;
  virtual void response_result(rpc::ObRequest* req, obrpc::ObRpcPacket* pkt) override;
  virtual common::ObAddr get_peer(const rpc::ObRequest* req) override;
};

}; // end namespace rpc
}; // end namespace oceanbase

#endif /* OCEANBASE_RPC_OB_EASY_RPC_REQUEST_OPERATOR_H_ */

