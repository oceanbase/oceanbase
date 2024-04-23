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

#define USING_LOG_PREFIX RPC_OBMYSQL
#include "rpc/obmysql/ob_sql_sock_handler.h"
#include "rpc/frame/ob_req_deliver.h"
#include "rpc/obmysql/ob_sql_sock_processor.h"

namespace oceanbase
{
using namespace common;
using namespace observer;
namespace obmysql
{

int ObSqlSockHandler::init(rpc::frame::ObReqDeliver* deliver)
{
  int ret = OB_SUCCESS;
  deliver_ = deliver;
  return ret;
}

static int get_client_addr_for_sql_sock_session(int fd, ObAddr& client_addr)
{
  int ret = OB_SUCCESS;
  struct sockaddr_storage addr;
  socklen_t addr_len = sizeof(addr);

  if (getpeername(fd, (struct sockaddr *)&addr, &addr_len) < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql nio getpeername failed", K(errno), K(ret));
  } else {
    client_addr.from_sockaddr(&addr);
  }

  return ret;
}

int ObSqlSockHandler::on_connect(void* udata, int fd)
{
  int ret  = OB_SUCCESS;
  ObSqlSockSession* sess = (ObSqlSockSession*)udata;
  new(sess)ObSqlSockSession(conn_cb_, nio_);
  if (OB_FAIL(get_client_addr_for_sql_sock_session(fd, sess->client_addr_))) {
    LOG_WARN("sql nio get_client_addr_for_sql_sock_session failed", K(ret));
  } else  if (OB_FAIL(sess->init())) {
    LOG_WARN("sess init failed", K(ret));
  }
  return ret;
}

void ObSqlSockHandler::on_close(void* udata, int err)
{
  UNUSED(err);
  ObSqlSockSession* sess = (ObSqlSockSession*)udata;
  sess->destroy();
}

void ObSqlSockHandler::on_flushed(void* udata)
{
  ObSqlSockSession* sess = (ObSqlSockSession*)udata;
  sess->on_flushed();
}

/*
share::ObTenantSpace* global_tenant_space = NULL;
int process_my_request(rpc::frame::ObReqProcessor& processor, rpc::ObRequest& req)
{
  ObAddr myaddr;
  ObCurTraceId::set(req.generate_trace_id(myaddr));
  ob_setup_default_tsi_warning_buffer();
  ob_reset_tsi_warning_buffer();

  processor.set_ob_request(req);
  processor.run();
  ObCurTraceId::reset();
  return 0;
}

int handle_sql_req_inplace(void* udata)
{
  int ret = OB_SUCCESS;
  auto *pm = common::ObPageManager::thread_local_instance();
  pm->set_tenant_ctx(OB_SERVER_TENANT_ID, ObCtxIds::DEFAULT_CTX_ID);
  lib::ContextTLOptGuard guard(true);
  lib::ContextParam param;
  param.set_mem_attr(OB_SERVER_TENANT_ID, ObModIds::OB_SQL_EXECUTOR, ObCtxIds::DEFAULT_CTX_ID)
      .set_properties(lib::USE_TL_PAGE_OPTIONAL)
      .set_ablock_size(lib::INTACT_MIDDLE_AOBJECT_SIZE);
  CREATE_WITH_TEMP_CONTEXT(param) {
    THIS_WORKER.set_allocator(&CURRENT_CONTEXT->get_arena_allocator());
    CREATE_WITH_TEMP_ENTITY(RESOURCE_OWNER, OB_SERVER_TENANT_ID) {
      WITH_ENTITY(global_tenant_space) {
        ObSqlSockSession* sess = (ObSqlSockSession*)udata;
        ret = process_my_request(sess->req_processor_, sess->sock_req_);
      }
    }
  }
  return ret;
}
*/
int ObSqlSockHandler::on_readable(void* udata)
{
  int ret = OB_SUCCESS;
  rpc::ObPacket *pkt = NULL;
  rpc::ObRequest *sql_req = NULL;
  ObSqlSockSession* sess = (ObSqlSockSession*)udata;

  if (NULL == sess) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("sess is null!", K(ret));
  } else if (OB_FAIL(sock_processor_.decode_sql_packet(sess->pool_, *sess, NULL, pkt))) {
    LOG_WARN("decode sql req fail", K(ret), K(sess->sql_session_id_));
  } else if (NULL == pkt) {
    sess->revert_sock();
  } else if (OB_FAIL(sock_processor_.build_sql_req(*sess, pkt, sql_req))) {
    LOG_WARN("build sql req fail", K(ret), K(sess->sql_session_id_));
  }

  if (OB_SUCCESS != ret || NULL == sql_req) {
  } else if (FALSE_IT(sess->set_last_decode_succ_and_deliver_time(ObClockGenerator::getClock()))) {
  } else if (OB_FAIL(deliver_->deliver(*sql_req))) {
    LOG_WARN("deliver sql request fail", K(ret), K(sess->sql_session_id_));
  }

  return ret;
}

}; // end namespace obmysql
}; // end namespace oceanbase
