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

#ifndef OCEANBASE_RPC_OBRPC_OB_RPC_REQUEST_
#define OCEANBASE_RPC_OBRPC_OB_RPC_REQUEST_

#include "common/data_buffer.h"
#include "rpc/ob_request.h"
#include "rpc/obrpc/ob_rpc_result_code.h"

namespace oceanbase
{
namespace obrpc
{

class ObRpcRequest : public rpc::ObRequest
{
public:
  ObRpcRequest(Type type): ObRequest(type){}

  template <class T>
  int response_success(common::ObDataBuffer *buffer,
                       const T &result,
                       const ObRpcResultCode &res_code,
                       const int64_t &sessid);

  inline int response_fail(common::ObDataBuffer *buffer,
                           const ObRpcResultCode &res_code,
                           const int64_t &sessid);
protected:

  template <class T>
  int do_serilize_result(common::ObDataBuffer *buffer,
                         const T &result,
                         const ObRpcResultCode &res_code);

  inline int do_flush_buffer(common::ObDataBuffer *buffer,
                             const ObRpcPacket *pkt,
                             int64_t sessid);

  inline int do_serilize_fail(common::ObDataBuffer *buffer, const ObRpcResultCode &res_code);

private:
  DISALLOW_COPY_AND_ASSIGN(ObRpcRequest);
};

template <class T>
int ObRpcRequest::response_success(
    common::ObDataBuffer *buffer,
    const T &result,
    const ObRpcResultCode &res_code,
    const int64_t &sessid)
{
  using namespace common;
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(buffer)) {
    ret = common::OB_ERR_NULL_VALUE;
    RPC_OBRPC_LOG(WARN, "input buffer is null", K(ret));
  } else if (OB_ISNULL(buffer->get_data())) {
    ret = common::OB_ERR_NULL_VALUE;
    RPC_OBRPC_LOG(WARN, "input buffer is null", K(ret));
  } else if (OB_ISNULL(ez_req_)) {
    ret = common::OB_ERR_UNEXPECTED;
    RPC_OBRPC_LOG(WARN, "ez_req_ is null", K(ret));
  } else if (OB_ISNULL(pkt_)) {
    ret = common::OB_ERR_NULL_VALUE;
    RPC_OBRPC_LOG(WARN, "pkt_ pointer is null", K(ret));
  } else if (OB_ISNULL(ez_req_->ms)) {
    ret = common::OB_ERR_UNEXPECTED;
    RPC_OBRPC_LOG(WARN, "ez_req_'s message is null", K(ret));
  } else if (OB_FAIL(do_serilize_result<T>(buffer, result, res_code))) {
    RPC_OBRPC_LOG(WARN, "failed to serilize result code and result", K(ret));
  } else if (OB_FAIL(do_flush_buffer(buffer, reinterpret_cast<const obrpc::ObRpcPacket*>(pkt_),
                                     sessid))){
    RPC_OBRPC_LOG(WARN, "failed to flush rpc buffer", K(ret));
  } else {
    //do nothing
  }
  return ret;
}

template <class T>
int ObRpcRequest::do_serilize_result(
    common::ObDataBuffer *buffer,
    const T &result,
    const ObRpcResultCode &res_code)
{
  using namespace obrpc;
  using namespace common;
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(buffer)) {
    ret = common::OB_ERR_NULL_VALUE;
    RPC_OBRPC_LOG(WARN, "buffer is nul", K(ret));
  } else if (OB_ISNULL(buffer->get_data())) {
    ret = common::OB_ERR_NULL_VALUE;
    RPC_OBRPC_LOG(WARN, "buffer is nul", K(ret));
  } else if (OB_FAIL(res_code.serialize(buffer->get_data(),
                                        buffer->get_capacity(),
                                        buffer->get_position()))) {
    RPC_OBRPC_LOG(WARN, "serialize result code fail", K(ret));
  } else if (OB_FAIL(common::serialization::encode(buffer->get_data(),
                                                   buffer->get_capacity(),
                                                   buffer->get_position(),
                                                   result))) {
    RPC_OBRPC_LOG(WARN, "serialize result fail", K(ret));
  } else {
    //do nothing
  }
  return ret;
}

} // end of namespace obrpc
} // end of namespace oceanbase
#endif
