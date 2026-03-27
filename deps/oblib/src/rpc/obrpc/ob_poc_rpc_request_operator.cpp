/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "rpc/obrpc/ob_poc_rpc_request_operator.h"
#include "rpc/obrpc/ob_poc_rpc_server.h"
#include "rpc/obmysql/ob_mysql_request_utils.h"

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

void ObPocRpcRequestOperator::set_trace_point(const ObRequest* req, int32_t trace_point)
{
  get_poc_handle_context(req)->set_trace_point(trace_point);
}

}; // end namespace obrpc
}; // end namespace oceanbase

