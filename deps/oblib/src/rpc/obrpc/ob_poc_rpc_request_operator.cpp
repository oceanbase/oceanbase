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

#include "rpc/obrpc/ob_poc_rpc_request_operator.h"
#include "rpc/obrpc/ob_poc_rpc_server.h"

using namespace oceanbase::rpc;
namespace oceanbase
{
namespace obrpc
{
ObPocServerHandleContext* get_poc_handle_context(const rpc::ObRequest* req)
{
  return (ObPocServerHandleContext*)req->get_server_handle_context();
}

void* ObPocRpcRequestOperator::alloc_response_buffer(ObRequest* req, int64_t size)
{
  return get_poc_handle_context(req)->alloc(size);
}

void ObPocRpcRequestOperator::response_result(ObRequest* req, obrpc::ObRpcPacket* pkt)
{
  obmysql::request_finish_callback();
  get_poc_handle_context(req)->resp(pkt);
  get_poc_handle_context(req)->destroy();

}

ObAddr ObPocRpcRequestOperator::get_peer(const ObRequest* req)
{
  return get_poc_handle_context(req)->get_peer();
}

}; // end namespace obrpc
}; // end namespace oceanbase

