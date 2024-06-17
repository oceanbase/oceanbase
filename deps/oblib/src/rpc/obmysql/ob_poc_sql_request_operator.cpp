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

#include "rpc/obmysql/ob_poc_sql_request_operator.h"
#include "rpc/obmysql/ob_sql_sock_session.h"
#include "rpc/obrpc/ob_rpc_opts.h"
#include "rpc/obmysql/ob_sql_sock_processor.h"

namespace oceanbase
{
using namespace common;
using namespace rpc;
namespace obmysql
{
void __attribute__((weak)) request_finish_callback();
void* ObPocSqlRequestOperator::get_sql_session(ObRequest* req)
{
  ObSqlSockSession* sess = (ObSqlSockSession*)req->get_server_handle_context();
  return &sess->conn_;
}

SSL* ObPocSqlRequestOperator::get_sql_ssl_st(ObRequest* req)
{
  ObSqlSockSession* sess = (ObSqlSockSession*)req->get_server_handle_context();
  SSL *ssl_st = sess->get_ssl_st();
  return ssl_st;
}

char* ObPocSqlRequestOperator::alloc_sql_response_buffer(ObRequest* req, int64_t size)
{
  ObSqlSockSession* sess = (ObSqlSockSession*)req->get_server_handle_context();
  return (char*)sess->alloc(size);
}

char *ObPocSqlRequestOperator::sql_reusable_alloc(ObRequest* req, int64_t size)
{
  void *buf = NULL;
  ObSqlSockSession* sess = (ObSqlSockSession*)req->get_server_handle_context();
  if(NULL == (buf = req->reusable_mem_.alloc(size))) {
    if (NULL != (buf = sess->alloc(size))) {
      req->reusable_mem_.add(buf, size);
    }
  }
  return static_cast<char*>(buf);
}

ObAddr ObPocSqlRequestOperator::get_peer(const ObRequest* req)
{
  ObSqlSockSession* sess = (ObSqlSockSession*)req->get_server_handle_context();
  return sess->client_addr_;
}

void ObPocSqlRequestOperator::disconnect_sql_conn(ObRequest* req)
{
  ObSqlSockSession* sess = (ObSqlSockSession*)req->get_server_handle_context();
  sess->set_shutdown();
}

void ObPocSqlRequestOperator::finish_sql_request(ObRequest* req)
{
  ObSqlSockSession* sess = (ObSqlSockSession*)req->get_server_handle_context();
  req->set_trace_point(ObRequest::OB_FINISH_SQL_REQUEST);
  obmysql::request_finish_callback();
  sess->revert_sock();
}

int ObPocSqlRequestOperator::create_read_handle(ObRequest* req, void*& read_handle)
{
  ObSqlSockSession* sess = (ObSqlSockSession*)req->get_server_handle_context();
  return sess->create_read_handle(read_handle);
}

int ObPocSqlRequestOperator::release_read_handle(ObRequest* req, void* read_handle)
{
  ObSqlSockSession* sess = (ObSqlSockSession*)req->get_server_handle_context();
  return sess->release_read_handle(read_handle);
}

int ObPocSqlRequestOperator::read_packet(ObRequest* req,
                                         ObICSMemPool& mem_pool,
                                         void* read_handle,
                                         ObSqlSockProcessor &sock_processor,
                                         ObPacket*& pkt)
{
  ObSqlSockSession* sess = (ObSqlSockSession*)req->get_server_handle_context();
  return sock_processor.decode_sql_packet(mem_pool, *sess, read_handle, pkt);
}

int ObPocSqlRequestOperator::release_packet(ObRequest* req, void* read_handle, ObPacket* pkt)
{
  ObSqlSockSession* sess = (ObSqlSockSession*)req->get_server_handle_context();
  obmysql::ObMySQLRawPacket* mysql_packet = static_cast<obmysql::ObMySQLRawPacket*>(pkt);
  return sess->consume_data(read_handle, mysql_packet->get_consume_size());
}

int ObPocSqlRequestOperator::write_response(ObRequest* req, const char* buf, int64_t sz)
{
  ObSqlSockSession* sess = (ObSqlSockSession*)req->get_server_handle_context();
  return sess->write_data(buf, sz);
}

int ObPocSqlRequestOperator::async_write_response(ObRequest* req, const char* buf, int64_t sz)
{
  ObSqlSockSession* sess = (ObSqlSockSession*)req->get_server_handle_context();
  return sess->async_write_data(buf, sz);
}

void ObPocSqlRequestOperator::get_sock_desc(ObRequest* req, ObSqlSockDesc& desc)
{
  desc.set(ObRequest::TRANSPORT_PROTO_POC, (void*)req->get_server_handle_context());
}

void ObPocSqlRequestOperator::disconnect_by_sql_sock_desc(ObSqlSockDesc& desc)
{
  ObSqlSockSession* sess = (ObSqlSockSession*)desc.sock_desc_;
  sess->shutdown();
}

void ObPocSqlRequestOperator::destroy(rpc::ObRequest* req)
{
  UNUSED(req);
}

void ObPocSqlRequestOperator::set_sql_session_to_sock_desc(rpc::ObRequest* req, void* sess)
{
  ObSqlSockSession* sock_sess = (ObSqlSockSession*)req->get_server_handle_context();
  sock_sess->set_sql_session_info(sess);
}

}; // end namespace rpc
}; // end namespace oceanbase
