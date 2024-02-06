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
#include "rpc/obmysql/ob_sql_sock_session.h"
#include "rpc/obmysql/ob_sql_nio.h"

namespace oceanbase
{
using namespace common;
using namespace rpc;
using namespace observer;
namespace obmysql
{

ObSqlSockSession::ObSqlSockSession(ObISMConnectionCallback& conn_cb, ObSqlNio* nio):
    nio_(nio),
    sm_conn_cb_(conn_cb),
    sql_req_(ObRequest::OB_MYSQL, 1),
    last_pkt_sz_(0),
    pending_write_buf_(NULL),
    pending_write_sz_(0),
    sql_session_id_(0)
{
  sql_req_.set_server_handle_context(this);
  is_inited_ = true;
}

ObSqlSockSession::~ObSqlSockSession() {}

int ObSqlSockSession::init()
{
  return sm_conn_cb_.init(*this, conn_);
}

void ObSqlSockSession::destroy()
{
  sm_conn_cb_.destroy(conn_);
  pool_.reset();
}

void ObSqlSockSession::destroy_sock()
{
  return nio_->destroy_sock((void*)this);
}

int ObSqlSockSession::on_disconnect()
{
  return sm_conn_cb_.on_disconnect(conn_);
}

void ObSqlSockSession::set_shutdown()
{
  return nio_->set_shutdown((void *)this);
}

void ObSqlSockSession::shutdown()
{
  return nio_->shutdown((void *)this);
}

void ObSqlSockSession::revert_sock()
{
  if (last_pkt_sz_ > 0) {
    nio_->consume_data((void*)this, last_pkt_sz_);
    last_pkt_sz_ = 0;
  }
  sql_req_.reset_trace_id();
  if (pending_write_buf_) {
    const char * data = pending_write_buf_;
    int64_t sz = pending_write_sz_;
    pending_write_buf_ = NULL;
    pending_write_sz_ = 0;
    nio_->async_write_data((void*)this, data, sz);
  } else {
    pool_.reuse();
    nio_->revert_sock((void*)this);
  }
}

void ObSqlSockSession::on_flushed()
{
  /* TODO should not go here*/
  //abort();
  pool_.reuse();
  nio_->revert_sock((void*)this);
}

bool ObSqlSockSession::has_error()
{
  return nio_->has_error((void*)this);
}

int ObSqlSockSession::peek_data(int64_t limit, const char*& buf, int64_t& sz)
{
  int ret = OB_SUCCESS;
  if (has_error()) {
    ret = OB_IO_ERROR;
    LOG_WARN("sock has error", K(ret));
  } else if (OB_FAIL(nio_->peek_data((void*)this,  limit, buf, sz))) {
    destroy_sock();
  }
  return ret;
}

void ObSqlSockSession::clear_sql_session_info() 
{
  nio_->reset_sql_session_info(this);
}

int ObSqlSockSession::consume_data(int64_t sz)
{
  int ret = OB_SUCCESS;
  if (has_error()) {
    ret = OB_IO_ERROR;
    LOG_WARN("sock has error", K(ret));
  } else if (OB_FAIL(nio_->consume_data((void*)this, sz))) {
    destroy_sock();
  }
  return ret;
}

void ObSqlSockSession::set_last_decode_succ_and_deliver_time(int64_t time)
{
  nio_->set_last_decode_succ_time((void*)this, time);
}
int ObSqlSockSession::write_data(const char* buf, int64_t sz)
{
  int ret = OB_SUCCESS;
  if (has_error()) {
    ret = OB_IO_ERROR;
    LOG_WARN("sock has error", K(ret));
  } else if (OB_FAIL(nio_->write_data((void*)this, buf, sz))) {
    destroy_sock();
  }
  return ret;
}

int ObSqlSockSession::async_write_data(const char* buf, int64_t sz)
{
  int ret = OB_SUCCESS;
  if (has_error()) {
    ret = OB_IO_ERROR;
    LOG_WARN("sock has error", K(ret));
  } else {
    pending_write_buf_ = buf;
    pending_write_sz_ = sz;
  }
  return ret;
}

void ObSqlSockSession::set_sql_session_info(void* sess)
{
  nio_->set_sql_session_info((void *)this, sess);
}

int ObSqlSockSession::set_ssl_enabled()
{
  return nio_->set_ssl_enabled((void *)this);
}

SSL* ObSqlSockSession::get_ssl_st()
{
  return nio_->get_ssl_st((void *)this);
}

int ObSqlSockSession::write_hanshake_packet(const char *buf, int64_t sz)
{
  return nio_->write_handshake_packet((void *)this, buf, sz);
}

void ObSqlSockSession::set_tls_version_option(uint64_t tls_option)
{
  nio_->set_tls_version_option((void *)this, tls_option);
}

}; // end namespace obmysql
}; // end namespace oceanbase
