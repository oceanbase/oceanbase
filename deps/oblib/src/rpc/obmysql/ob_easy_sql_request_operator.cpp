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

#include "rpc/obmysql/ob_easy_sql_request_operator.h"
#include "rpc/ob_request.h"
#include "rpc/obrpc/ob_rpc_opts.h"
#include "lib/utility/utility.h"

namespace oceanbase
{
using namespace common;
using namespace rpc;
namespace obmysql
{

void __attribute__((weak)) request_finish_callback();
void* ObEasySqlRequestOperator::get_sql_session(ObRequest* req)
{
  void* session = NULL;
  easy_request_t* ez_req = req->get_ez_req();
  if (OB_ISNULL(ez_req)
      || OB_ISNULL(ez_req->ms)
      || OB_ISNULL(ez_req->ms->c)) {
    RPC_LOG_RET(ERROR, OB_INVALID_ARGUMENT, "invalid argument", K(ez_req));
  } else {
    session = ez_req->ms->c->user_data;
  }
  return session;
}

SSL* ObEasySqlRequestOperator::get_sql_ssl_st(ObRequest* req)
{
  SSL *ssl_st = NULL;
  easy_request_t* ez_req = req->get_ez_req();
  if (OB_ISNULL(ez_req)
      || OB_ISNULL(ez_req->ms)
      || OB_ISNULL(ez_req->ms->c)) {
    RPC_LOG_RET(ERROR, OB_INVALID_ARGUMENT, "invalid argument", K(ez_req));
  } else if (NULL != ez_req->ms->c->sc) {
    ssl_st = ez_req->ms->c->sc->connection ;
  }
  return ssl_st;
}

char* ObEasySqlRequestOperator::alloc_sql_response_buffer(ObRequest* req, int64_t size)
{
  void *buf = NULL;
  easy_request_t* ez_req = req->get_ez_req();
  if (OB_ISNULL(ez_req) || OB_ISNULL(ez_req->ms) || OB_ISNULL(ez_req->ms->pool)) {
    RPC_LOG_RET(ERROR, OB_INVALID_ARGUMENT, "ez_req is not corret");
  } else {
    buf = easy_pool_alloc(
        ez_req->ms->pool, static_cast<uint32_t>(size));
  }
  return static_cast<char*>(buf);
}

char *ObEasySqlRequestOperator::sql_reusable_alloc(ObRequest* req, int64_t size)
{
  void *buf = NULL;
  easy_request_t* ez_req = req->get_ez_req();
  if (OB_ISNULL(ez_req) || OB_ISNULL(ez_req->ms) || OB_ISNULL(ez_req->ms->pool)) {
    RPC_LOG_RET(ERROR, OB_INVALID_ARGUMENT, "ez_req is not corret");
  } else {
    if(NULL == (buf = req->reusable_mem_.alloc(size))) {
      buf = easy_pool_alloc(
          ez_req->ms->pool, static_cast<uint32_t>(size));
      if (NULL != buf) {
        req->reusable_mem_.add(buf, size);
      }
    }
  }
  return static_cast<char*>(buf);
}

ObAddr ObEasySqlRequestOperator::get_peer(const ObRequest* req)
{
  ObAddr addr;
  easy_request_t* ez_req = req->ez_req_;
  if (OB_ISNULL(ez_req) || OB_ISNULL(ez_req->ms)
      || OB_ISNULL(ez_req->ms->c)) {
    RPC_LOG_RET(ERROR, OB_INVALID_ARGUMENT, "invalid argument", K(ez_req));
  } else {
    easy_addr_t &ez = ez_req->ms->c->addr;
    if (!ez2ob_addr(addr, ez)) {
      RPC_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "fail to convert easy_addr to ob_addr", K(ez_req));
    } // otherwise leave addr be zeros
  }
  return addr;
}

void ObEasySqlRequestOperator::disconnect_sql_conn(ObRequest* req)
{
  easy_request_t* ez_req = req->get_ez_req();
  if (OB_ISNULL(ez_req) || OB_ISNULL(ez_req->ms)) {
    RPC_LOG_RET(ERROR, OB_INVALID_ARGUMENT, "invalid argument", K(ez_req));
  } else {
    easy_connection_destroy_dispatch(ez_req->ms->c);
  }
}

void ObEasySqlRequestOperator::finish_sql_request(ObRequest* req)
{
  easy_request_t* ez_req = req->get_ez_req();
  if (OB_ISNULL(ez_req) || OB_ISNULL(ez_req->ms)) {
    RPC_LOG_RET(ERROR, OB_INVALID_ARGUMENT, "invalid argument", K(ez_req));
  } else {
    if (ez_req->retcode != EASY_AGAIN) {
      obmysql::request_finish_callback();
    }
    easy_request_wakeup(ez_req);
  }
}

int ObEasySqlRequestOperator::write_response(ObRequest* req, const char* buf, int64_t sz)
{
  UNUSED(req);
  UNUSED(buf);
  UNUSED(sz);
  return 0;
}

int ObEasySqlRequestOperator::async_write_response(ObRequest* req, const char* buf, int64_t sz)
{
  UNUSED(req);
  UNUSED(buf);
  UNUSED(sz);
  return 0;
}

void ObEasySqlRequestOperator::get_sock_desc(ObRequest* req, ObSqlSockDesc& desc)
{
  desc.set(ObRequest::TRANSPORT_PROTO_EASY, (void*)req->get_ez_req()->ms->c);
}

void ObEasySqlRequestOperator::disconnect_by_sql_sock_desc(ObSqlSockDesc& desc)
{
  easy_connection_destroy_dispatch((easy_connection_t*)desc.sock_desc_);
}

void ObEasySqlRequestOperator::destroy(ObRequest* req)
{
  UNUSED(req);
}

void ObEasySqlRequestOperator::set_sql_session_to_sock_desc(rpc::ObRequest* req, void* sess)
{
  UNUSED(req);
  UNUSED(sess);
}

}; // end namespace rpc
}; // end namespace oceanbase

