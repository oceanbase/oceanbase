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
};

}; // end namespace obrpc
}; // end namespace oceanbase

#endif /* OCEANBASE_OBRPC_OB_POC_RPC_REQUEST_OPERATOR_H_ */

