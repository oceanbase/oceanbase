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

#define USING_LOG_PREFIX RPC_FRAME

#include "rpc/frame/ob_req_transport.h"

#include <byteswap.h>
#include <arpa/inet.h>
#include "util/easy_mod_stat.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/worker.h"
#include "rpc/obrpc/ob_rpc_packet.h"

namespace easy {
int64_t __attribute__((weak)) get_easy_per_dest_memory_limit()
{
  return 0;
}
};  // namespace easy

using namespace oceanbase::common;
using namespace oceanbase::rpc;
using namespace oceanbase::obrpc;
using namespace oceanbase::rpc::frame;

// file private function, called when using asynchronous rpc call.
int async_cb(easy_request_t* r)
{
  int ret = OB_SUCCESS;
  int easy_err = EASY_OK;
  const int64_t start_time = ObTimeUtility::current_time();
  int64_t after_decode_time = 0;
  int64_t after_process_time = 0;
  ObRpcPacketCode pcode = OB_INVALID_RPC_CODE;
  if (OB_ISNULL(r)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(r), K(ret));
  } else if (r->user_data) {
    typedef ObReqTransport::AsyncCB ACB;
    ACB* cb = reinterpret_cast<ACB*>(r->user_data);
    cb->set_request(r);
    cb->do_first();

    if (!r->ipacket) {
      // 1. destination doesn't response
      // 2. destination responses but not well-formed
      // 3. destination responses but can't fulfilled as a single packet until timeout
      // We set easy error so that return EASY_ERROR to easy.
      easy_err = cb->get_error();
      ret = cb->on_error(easy_err);
      if (OB_ERROR == ret) {
        /*
         * The derived classe has not overwrite thie own on_error callback. We still use
         * on_timeout for For backward compatibility.
         */
        cb->on_timeout();
      }

      ret = OB_LIBEASY_ERROR;
    } else if (OB_FAIL(cb->decode(r->ipacket))) {
      cb->on_invalid();
      LOG_DEBUG("decode failed", K(ret));
    } else {
      after_decode_time = ObTimeUtility::current_time();
      ObRpcPacket* pkt = reinterpret_cast<ObRpcPacket*>(r->ipacket);
      pcode = pkt->get_pcode();

      EVENT_INC(RPC_PACKET_IN);
      EVENT_ADD(RPC_PACKET_IN_BYTES, pkt->get_clen() + pkt->get_header_size() + OB_NET_HEADER_LENGTH);

      if (OB_FAIL(cb->process())) {
        LOG_DEBUG("process failed", K(ret));
      }
    }
  } else {
    // async rpc without callback
  }

  after_process_time = ObTimeUtility::current_time();
  // whether process correctly or not, session must been destroyed.
  if (NULL != r && NULL != r->ms) {
    easy_session_destroy((easy_session_t*)r->ms);
  } else {
    LOG_ERROR("receive NULL request or message", K(r));
  }

  if (!OB_SUCC(ret)) {
    LOG_DEBUG("process async request fail", K(r), K(ret));
  }

  const int64_t cur_time = ObTimeUtility::current_time();
  const int64_t total_time = cur_time - start_time;
  const int64_t decode_time = after_decode_time - start_time;
  const int64_t process_time = after_process_time - after_decode_time;
  const int64_t session_destroy_time = cur_time - after_process_time;
  if (total_time > OB_EASY_HANDLER_COST_TIME) {
    LOG_WARN("async_cb handler cost too much time",
        K(total_time),
        K(decode_time),
        K(process_time),
        K(session_destroy_time),
        K(pcode));
  }

  return EASY_OK;
}

int ObReqTransport::AsyncCB::get_error() const
{
  return NULL != req_ ? ((easy_session_t*)req_->ms)->error : EASY_ERROR;
}

int ObReqTransport::AsyncCB::on_error(int)
{
  /*
   * To avoid confusion, we implement this new interface to notify exact error
   * type to upper-layer modules.
   *
   * For backward compatibility, this function returns OB_ERROR. The derived
   * classes should overwrite this function and return OB_SUCCESS.
   */
  return OB_ERROR;
}

ObReqTransport::ObReqTransport(easy_io_t* eio, easy_io_handler_pt* handler)
    : eio_(eio), handler_(handler), sgid_(0), bucket_count_(0)
{
  // empty
}

static int easy_session_create_respect_memory_limit(easy_session_t*& session, int size, easy_addr_t& dest)
{
  int ret = OB_SUCCESS;
  const int64_t easy_server_memory_limit = ::easy::get_easy_per_dest_memory_limit();
  uint64_t id = *(uint64_t*)&dest;
  easy_mod_stat_t* stat = NULL;
  if (0 == easy_server_memory_limit) {
  } else if (NULL == (stat = easy_fetch_mod_stat(id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("easy fetch mod stat fail", K(id));
  } else if (stat->size > easy_server_memory_limit) {
    ret = OB_EXCEED_MEM_LIMIT;
    session = NULL;
    char buff[OB_SERVER_ADDR_STR_LEN];
    LOG_WARN(
        "easy exceed memory limit, dest server has too many pending request, maybe dest server slow or unreachable",
        "dest",
        easy_inet_addr_to_str(&dest, buff, OB_SERVER_ADDR_STR_LEN),
        K(stat->size),
        K(stat->count),
        K(easy_server_memory_limit));
  }
  if (OB_SUCC(ret)) {
    easy_cur_mod_stat = stat;
    if (NULL == (session = easy_session_create(size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    }
    easy_cur_mod_stat = NULL;
  }
  return ret;
}

int ObReqTransport::create_session(easy_session_t*& session, const ObAddr& addr, int64_t size, int64_t timeout,
    const ObAddr& local_addr, const common::ObString& ssl_invited_nodes, const AsyncCB* cb, AsyncCB** newcb) const
{
  int ret = OB_SUCCESS;
  easy_addr_t ez_addr;

  if (!addr.is_valid()) {
    ret = OB_LIBEASY_ERROR;
    LOG_WARN("destinate address isn't invalid", K(ret));
  } else {
    ez_addr = to_ez_addr(addr);
    if (OB_FAIL(easy_session_create_respect_memory_limit(session, static_cast<int>(size), ez_addr))) {
      LOG_WARN("create session fail", K(ret), K(addr));
    }
  }

  if (OB_SUCC(ret)) {
    // auto connect if there is no connection
    session->status = EASY_CONNECT_SEND;
    session->addr = ez_addr;
    session->thread_ptr = handler_;
    if (cb) {
      *newcb = cb->clone(SPAlloc(session->pool));
      session->r.user_data = *newcb;
      if (!*newcb) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
    }
    if (NULL == cb) {
      session->unneed_response = 1;
      if (REACH_TIME_INTERVAL(3000000)) {
        session->enable_trace = 1;
        LOG_INFO("unneed response", KP(session));
      }
    }
    session->callback = async_cb;
    // easy accept timeout for ms unit, so we convert timeout received
    // from ns unit to ms unit.
    session->timeout = static_cast<ev_tstamp>(timeout / 1000);

    bool use_ssl = false;
    if (NULL != handler_ && 1 == handler_->is_ssl && 0 == handler_->is_ssl_opt && NULL != eio_ && NULL != eio_->ssl) {
      if (ssl_invited_nodes.empty() || 0 == ssl_invited_nodes.case_compare("NONE")) {
        // nothing
      } else if (0 == ssl_invited_nodes.case_compare("ALL")) {
        use_ssl = true;
      } else {
        char addr_buffer[MAX_IP_ADDR_LENGTH] = {};
        // both client and server in ssl_invited_node, we will use ssl
        if (addr.ip_to_string(addr_buffer, MAX_IP_ADDR_LENGTH) &&
            NULL != strstr(ssl_invited_nodes.ptr(), addr_buffer) &&
            local_addr.ip_to_string(addr_buffer, MAX_IP_ADDR_LENGTH) &&
            NULL != strstr(ssl_invited_nodes.ptr(), addr_buffer)) {
          use_ssl = true;
        }
      }
    }
    LOG_DEBUG("rpc connection session create", K(local_addr), "dest", addr, K(use_ssl), K(ssl_invited_nodes));

    if (use_ssl) {
      session->packet_id |= (EASY_CONNECT_SSL | EASY_CONNECT_SSL_OB);
    } else {
      session->packet_id &= ~(EASY_CONNECT_SSL | EASY_CONNECT_SSL_OB);
    }
  }

  if (OB_FAIL(ret) && session != NULL) {
    easy_session_destroy(session);
    session = NULL;
  }

  return ret;
}

int ObReqTransport::balance_assign() const
{
  static uint8_t io_index = 0;
  int idx = 0;
  if (bucket_count_ > 0) {
    // batch rpc eio
    idx = sgid_ + (__sync_fetch_and_add(&io_index, 1) % bucket_count_);
  } else {
    idx = sgid_ + (__sync_fetch_and_add(&io_index, 1) %
                      (eio_->io_thread_pool->thread_count * OB_RPC_CONNECTION_COUNT_PER_THREAD));
  }
  return idx;
}

ObPacket* ObReqTransport::send_session(easy_session_t* s) const
{
  int ret = OB_SUCCESS;
  ObPacket* pkt = NULL;
  char buff[OB_SERVER_ADDR_STR_LEN] = {'\0'};

  if (OB_ISNULL(s)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(s), K(ret));
  } else if (OB_ISNULL(eio_) || OB_ISNULL(eio_->io_thread_pool)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(s), K(eio_));
  }

  if (OB_SUCC(ret)) {
    static uint8_t io_index = 0;
    // Synchronous rpc always needs to return packets
    s->unneed_response = false;
    s->r.client_start_time = common::ObTimeUtility::current_time();
    if (0 == s->addr.cidx) {
      s->addr.cidx = balance_assign();
    }

    easy_inet_addr_to_str(&s->addr, buff, OB_SERVER_ADDR_STR_LEN);
    pkt = reinterpret_cast<ObPacket*>(easy_client_send(eio_, s->addr, s));
    if (NULL == pkt) {
      SERVER_LOG(WARN, "send packet fail", "dst", buff, KP(s));
    } else {
      SERVER_LOG(DEBUG, "send session successfully", "dst", buff);
    }
  }
  return pkt;
}

int ObReqTransport::post_session(easy_session_t* s) const
{
  int ret = OB_SUCCESS;
  int eret = EASY_OK;
  char buff[OB_SERVER_ADDR_STR_LEN] = {'\0'};

  if (OB_ISNULL(s)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(s), K(ret));
  } else if (OB_ISNULL(eio_) || OB_ISNULL(eio_->io_thread_pool)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(s), K(eio_));
  }

  if (OB_SUCC(ret)) {
    s->r.client_start_time = common::ObTimeUtility::current_time();
    if (0 == s->addr.cidx) {
      s->addr.cidx = balance_assign();
    }

    easy_inet_addr_to_str(&s->addr, buff, OB_SERVER_ADDR_STR_LEN);
    if (EASY_OK != (eret = easy_client_dispatch(eio_, s->addr, s))) {
      LOG_WARN("send packet fail", K(eret), "dst", buff, KP(s));
      ret = OB_RPC_POST_ERROR;
    } else {
      LOG_DEBUG("send session successfully", "dst", buff);
    }
  }
  return ret;
}

easy_addr_t ObReqTransport::to_ez_addr(const ObAddr& addr) const
{
  easy_addr_t ez;
  memset(&ez, 0, sizeof(ez));
  if (addr.is_valid()) {
    ez.port = (htons)(static_cast<uint16_t>(addr.get_port()));
    ez.cidx = 0;
    if (addr.using_ipv4()) {
      ez.family = AF_INET;
      ez.u.addr = htonl(addr.get_ipv4());
    } else {
      ez.family = AF_INET6;
      (void)addr.get_ipv6(&ez.u.addr6, sizeof(ez.u.addr6));
    }
  }
  return ez;
}
