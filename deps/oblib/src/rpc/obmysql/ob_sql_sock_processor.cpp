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
#include "rpc/obmysql/ob_sql_sock_processor.h"
#include "rpc/frame/ob_req_translator.h"
#include "rpc/ob_request.h"
#include "rpc/obmysql/ob_sql_nio.h"
#include "rpc/obmysql/ob_sql_sock_session.h"

namespace oceanbase
{
using namespace common;
using namespace rpc;
using namespace observer;
namespace obmysql
{
static int processor_do_decode(ObVirtualCSProtocolProcessor& processor, ObSMConnection& conn, ObICSMemPool& pool, const char* buf, int64_t limit, rpc::ObPacket*& pkt, int64_t& req_sz, int64_t& consume_sz)
{
  int ret = OB_SUCCESS;
  int64_t next_read_bytes = 0;
  const char* start = buf;
  if (OB_FAIL(processor.do_decode(conn, pool, buf, buf + limit, pkt, next_read_bytes))) {
  } else {
    req_sz = limit + next_read_bytes;
    consume_sz = buf - start;
  }
  return ret;
}

int ObSqlSockProcessor::decode_sql_packet(ObICSMemPool& mem_pool,
                                          ObSqlSockSession& sess,
                                          void* read_handle,
                                          rpc::ObPacket*& ret_pkt)
{
  int ret = OB_SUCCESS;
  ObSMConnection& conn = sess.conn_;
  rpc::ObPacket* pkt = NULL;
  ObVirtualCSProtocolProcessor* processor = get_protocol_processor(sess.conn_.get_cs_protocol_type());
  const char* start = NULL;
  int64_t limit = 1;
  int64_t read_sz = 0;
  char* buf = NULL;
  ret_pkt = NULL;

  while(OB_SUCCESS == ret && NULL == ret_pkt) {
    bool need_read_more = false;
    int64_t consume_sz = 0;
    if (OB_FAIL(sess.peek_data(read_handle, limit, start, read_sz))) {
      LOG_WARN("peed data fail", K(ret));
    } else if (read_sz < limit) {
      break;
    } else if (OB_FAIL(processor_do_decode(*processor, conn, mem_pool, start, read_sz, pkt, limit, consume_sz))) {
      LOG_WARN("do_decode fail", K(ret));
    } else if (NULL == pkt) {
      // try read more
    } else if (conn.is_in_ssl_connect_phase()) {
      ret_pkt = NULL;
      sess.set_last_pkt_sz(consume_sz);
      if (OB_FAIL(sess.set_ssl_enabled())) {
        LOG_WARN("sql nio enable ssl for server failed", K(ret));
      }
      break;
    } else if (!conn.is_in_authed_phase() && !conn.is_in_auth_switch_phase()) {
      ret_pkt = pkt;
      sess.set_last_pkt_sz(consume_sz);
    } else if (OB_FAIL(processor->do_splice(conn, mem_pool, (void*&)pkt, need_read_more))) {
      LOG_WARN("do_splice fail");
    } else if (!need_read_more) {
      ret_pkt = pkt;
      if (NULL == read_handle) {
        sess.set_last_pkt_sz(consume_sz);
      } else if (OB_LIKELY(ret_pkt != NULL)) {
        ObMySQLRawPacket *raw_packet = static_cast<ObMySQLRawPacket *>(ret_pkt);
        raw_packet->set_consume_size(consume_sz);
      }
    } else {
      sess.consume_data(read_handle, consume_sz); // assert read_handle == NULL
      limit = 1; // new pkt need more data
    }
  }
  return ret;
}

ObVirtualCSProtocolProcessor *ObSqlSockProcessor::get_protocol_processor(ObCSProtocolType type)
{
  ObVirtualCSProtocolProcessor *processor = NULL;
  switch (type) {
    case OB_MYSQL_CS_TYPE: {
      processor = &mysql_processor_;
      break;
    }
    case OB_MYSQL_COMPRESS_CS_TYPE: {
      processor = &compress_processor_;
      break;
    }
    case OB_2_0_CS_TYPE: {
      processor = &ob_2_0_processor_;
      break;
    }
    default: {
      LOG_ERROR_RET(OB_ERR_UNEXPECTED, "invalid cs protocol type", K(type));
      break;
    }
  }
  return processor;
}

int ObSqlSockProcessor::build_sql_req(ObSqlSockSession& sess, rpc::ObPacket* pkt, rpc::ObRequest*& sql_req)
{
  int ret = OB_SUCCESS;
  ObRequest* ret_req = &sess.sql_req_;
  new(ret_req)ObRequest(ObRequest::OB_MYSQL, 1);
  ret_req->set_server_handle_context(&sess);
  ret_req->set_packet(pkt);
  ret_req->set_receive_timestamp(common::ObTimeUtility::current_time());
  ret_req->set_connection_phase(sess.conn_.connection_phase_);
  LOG_DEBUG("build_sql_req", KP(ret_req), K(sess.conn_.connection_phase_));
  sql_req = ret_req;
  return ret;
}

}; // end namespace obmysql
}; // end namespace oceanbase
