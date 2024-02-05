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
#include "lib/net/ob_addr.h"
#include "rpc/obrpc/ob_rpc_packet.h"
#include "rpc/obrpc/ob_rpc_stat.h"
#include "rpc/obrpc/ob_net_keepalive.h"
#include "rpc/frame/ob_net_easy.h"

namespace easy
{
int64_t __attribute__((weak)) get_easy_per_dest_memory_limit()
{
  return 0;
}
};

using namespace oceanbase::common;
using namespace oceanbase::rpc;
using namespace oceanbase::obrpc;
using namespace oceanbase::rpc::frame;

easy_addr_t oceanbase::rpc::frame::to_ez_addr(const ObAddr &addr)
{
  easy_addr_t ez;
  if (addr.is_valid()) {
    memset(&ez, 0, sizeof (ez));
    ez.port   = (htons)(static_cast<uint16_t>(addr.get_port()));
    ez.cidx   = 0;
    if (addr.using_ipv4()) {
      ez.family = AF_INET;
      ez.u.addr = htonl(addr.get_ipv4());
    } else if (addr.using_unix()) {
      ez.family = AF_UNIX;
      snprintf(ez.u.unix_path, UNIX_PATH_MAX, "%s", addr.get_unix_path());
    } else {
      ez.family = AF_INET6;
      (void) addr.get_ipv6(&ez.u.addr6, sizeof(ez.u.addr6));
    }
  }
  return ez;
}

// file private function, called when using asynchronous rpc call.
int async_cb(easy_request_t *r)
{
  int ret = OB_SUCCESS;
  int easy_err = EASY_OK;
  const int64_t start_time = ObTimeUtility::current_time();
  int64_t after_decode_time = 0;
  int64_t after_process_time = 0;
  ObRpcPacketCode pcode = OB_INVALID_RPC_CODE;
  if (OB_ISNULL(r) || OB_ISNULL(r->ms)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(r),K(ret));
  } else if (r->user_data) {
    typedef ObReqTransport::AsyncCB ACB;
    ACB *cb = reinterpret_cast<ACB*>(r->user_data);
    cb->record_stat(r->ipacket == NULL);
    pcode = cb->get_pcode();

    if (!r->ipacket) {
      // 1. destination doesn't response
      // 2. destination responses but not well-formed
      // 3. destination responses but can't fulfilled as a single packet until timeout
      // We set easy error so that return EASY_ERROR to easy.
      easy_err = r->ms->error;
      cb->set_error(easy_err);
      ret = cb->on_error(easy_err);
      if (OB_SUCCESS != ret) {
        /*
         * The derived classe has not overwrite thie own on_error callback. We still use
         * on_timeout for For backward compatibility.
         */
        cb->on_timeout();
      }

      ret = OB_LIBEASY_ERROR;
    } else if (OB_FAIL(cb->decode(r->ipacket))) {
      cb->on_invalid();
      LOG_WARN("decode failed", K(ret), K(pcode));
    } else if (OB_PACKET_CLUSTER_ID_NOT_MATCH == cb->get_rcode()) {
      LOG_WARN("wrong cluster id", K(ret), K(easy_connection_str(r->ms->c)), K(pcode));
      cb->set_error(EASY_CLUSTER_ID_MISMATCH);
      ret = cb->on_error(EASY_CLUSTER_ID_MISMATCH);
      if (OB_ERROR == ret) {
        /*
         * The derived classe has not overwrite thie own on_error callback. We still use
         * on_timeout for For backward compatibility.
         */
        cb->on_timeout();
      }
    } else {
      after_decode_time = ObTimeUtility::current_time();
      ObRpcPacket* pkt = reinterpret_cast<ObRpcPacket*>(r->ipacket);
      pcode = pkt->get_pcode();
      bool cb_cloned = cb->get_cloned();

      EVENT_INC(RPC_PACKET_IN);
      EVENT_ADD(RPC_PACKET_IN_BYTES,
                pkt->get_clen() + pkt->get_header_size() + OB_NET_HEADER_LENGTH);

      if (OB_FAIL(cb->process())) {
        LOG_WARN("process failed", K(ret), K(pcode));
      }

      if (cb_cloned) {
        LOG_DEBUG("reset rcode", K(cb_cloned));
        cb->~AsyncCB();
      }
    }
  } else {
    // async rpc without callback
  }

  after_process_time = ObTimeUtility::current_time();
  // whether process correctly or not, session must been destroyed.
  if (NULL != r && NULL != r->ms) {
    easy_session_destroy((easy_session_t *)r->ms);
  } else {
    LOG_ERROR("receive NULL request or message", K(r));
  }

  if (!OB_SUCC(ret)) {
    LOG_WARN("process async request fail", K(r), K(ret), K(pcode));
  }
  THIS_WORKER.get_sql_arena_allocator().reset();

  const int64_t cur_time = ObTimeUtility::current_time();
  const int64_t total_time = cur_time  - start_time;
  const int64_t decode_time = after_decode_time - start_time;
  const int64_t process_time = after_process_time - after_decode_time;
  const int64_t session_destroy_time = cur_time - after_process_time;
  if (total_time > OB_EASY_HANDLER_COST_TIME) {
    LOG_WARN_RET(OB_ERR_TOO_MUCH_TIME, "async_cb handler cost too much time", K(total_time), K(decode_time),
        K(process_time), K(session_destroy_time), K(ret), K(pcode));
  }

  return EASY_OK;
}

void ObReqTransport::AsyncCB::record_stat(bool is_timeout)
{
  using namespace oceanbase::common;
  rpc::RpcStatPiece piece;
  piece.async_ = true;
  piece.size_ = payload_;
  piece.time_ = ObTimeUtility::current_time() - send_ts_;
  if (is_timeout) {
    piece.is_timeout_ = true;
    piece.failed_ = true;
  }
  RPC_STAT((ObRpcPacketCode)pcode_, tenant_id_, piece);
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

ObReqTransport::ObReqTransport(
    easy_io_t *eio, easy_io_handler_pt *handler)
    : eio_(eio), handler_(handler), sgid_(0), bucket_count_(0), ratelimit_enabled_(0), enable_use_ssl_(false)
{
  // empty
}

static int easy_session_create_respect_memory_limit(
    easy_session_t*& session,
    int size, easy_addr_t& dest,
    bool do_ratelimit,
    bool is_check_memory_limit = true)
{
  int ret = OB_SUCCESS;
  const int64_t easy_server_memory_limit = ::easy::get_easy_per_dest_memory_limit();
  uint64_t id = 0;
  easy_mod_stat_t* stat = NULL;
  easy_addr_t dest_mod_stat;

  dest_mod_stat = dest;
  if (do_ratelimit) {
    /*
     * Here, we use the highest bit in binary of the field 'family' of struct easy_addr_t to
     * distinguish the destination address, on which connection is established with ratelimit
     * enabled, from the default.
     *
     * Maybe it looks more like a hack. Anyway, it can solve the problem with least cost.
     */
    dest_mod_stat.family |= EASY_ADDR_FAMILY_RL_MOD_MASK;
  }
  id = *(uint64_t*)&dest_mod_stat;

  if (0 == easy_server_memory_limit) {
  } else if (NULL == (stat = easy_fetch_mod_stat(id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("easy fetch mod stat fail", K(id));
  } else if (is_check_memory_limit && stat->size > easy_server_memory_limit) {
    ret = OB_EXCEED_MEM_LIMIT;
    session = NULL;
    char buff[OB_SERVER_ADDR_STR_LEN];
    LOG_WARN("easy exceed memory limit, dest server has too many pending request, maybe dest server slow or unreachable",
            "dest", easy_inet_addr_to_str(&dest, buff, OB_SERVER_ADDR_STR_LEN), K(stat->size), K(stat->count),
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

int ObReqTransport::create_session(
    easy_session_t *&session,
    const ObAddr &addr, int64_t size,
    int64_t timeout, const ObAddr &local_addr,
    bool do_ratelimit,
    int8_t is_bg_flow,
    const common::ObString &ssl_invited_nodes,
    const AsyncCB *cb, AsyncCB **newcb) const
{
  int ret = OB_SUCCESS;
  easy_addr_t ez_addr;

  if (!addr.is_valid()) {
    ret = OB_LIBEASY_ERROR;
    LOG_WARN("destinate address isn't invalid", K(ret));
  } else {
    ez_addr = to_ez_addr(addr);
    if (!ratelimit_enabled_) {
      do_ratelimit = false;
    }
    bool in_black = ObNetKeepAlive::get_instance().in_black(ez_addr);
    if (in_black) {
      ret = OB_RPC_POST_ERROR;
      if (REACH_TIME_INTERVAL(1000000)) {
        LOG_WARN("address in blacklist", K(ret), K(addr));
      }
    } else {
      bool is_check_memory_limit = true;
      if (NULL != eio_ && ObNetEasy::HIGH_PRI_RPC_EIO_MAGIC == eio_->magic) {
        // high priority rpc not check memory limit
        is_check_memory_limit = false;
      }
      if (OB_FAIL(easy_session_create_respect_memory_limit(session, static_cast<int>(size), ez_addr, do_ratelimit, is_check_memory_limit))) {
        LOG_WARN("create session fail", K(ret), K(addr));
      }
    }
  }

  if (OB_SUCC(ret)) {
    // auto connect if there is no connection
    session->status = EASY_CONNECT_SEND;
    session->addr = ez_addr;
    session->thread_ptr = handler_;
    if (do_ratelimit) {
      session->type = EASY_TYPE_RL_SESSION;
      session->is_bg_flow = is_bg_flow;
      session->ratelimit_enabled = do_ratelimit;
    }

    if (cb) {
      *newcb = cb->clone(EasySPAlloc(session->pool));
      session->r.user_data = *newcb;
      if (!*newcb) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        if (cb != *newcb) {
          (*newcb)->set_cloned(true);
        }
      }
    } else {
      session->unneed_response = 1;
      if (REACH_TIME_INTERVAL(3000000)) {
        session->enable_trace = 1;
      }
    }
    session->callback = async_cb;
    // easy accept timeout for ms unit, so we convert timeout received
    // from ns unit to ms unit.
    session->timeout = static_cast<ev_tstamp>(timeout / 1000);

    bool use_ssl = false;
    if (NULL != handler_ && 1 == handler_->is_ssl && 0 == handler_->is_ssl_opt
        && NULL != eio_ && NULL != eio_->ssl) {
      if (enable_use_ssl_) {
        use_ssl = true;
      } else if (ssl_invited_nodes.empty() || 0 == ssl_invited_nodes.case_compare("NONE")) {
        //nothing
      } else if (0 == ssl_invited_nodes.case_compare("ALL")) {
        use_ssl = true;
      } else {
        char addr_buffer[MAX_IP_ADDR_LENGTH] = {};
        //both client and server in ssl_invited_node, we will use ssl
        if (addr.ip_to_string(addr_buffer, MAX_IP_ADDR_LENGTH)
            && NULL != strstr(ssl_invited_nodes.ptr(), addr_buffer)
            && local_addr.ip_to_string(addr_buffer, MAX_IP_ADDR_LENGTH)
            && NULL != strstr(ssl_invited_nodes.ptr(), addr_buffer) ) {
          use_ssl = true;
        }
      }
    }
    LOG_DEBUG("rpc connection session create", K(local_addr), "dest", addr, K(use_ssl), K(enable_use_ssl_),
              K(ssl_invited_nodes), K(handler_->is_ssl), K(handler_->is_ssl_opt), K(eio_->ssl));

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

int ObReqTransport::create_request(
    Request &req, const ObAddr &dst,
    int64_t size, int64_t timeout,
    const ObAddr &local_addr,
    bool do_ratelimit,
    int8_t is_bg_flow,
    const common::ObString &ssl_invited_nodes,
    const AsyncCB *cb) const
{
  int ret = common::OB_SUCCESS;
  easy_session_t *s = NULL;
  uint32_t rpc_header_size = static_cast<uint32_t>(obrpc::ObRpcPacket::get_header_size());
  uint32_t ez_rpc_header_size = OB_NET_HEADER_LENGTH + rpc_header_size;
  int64_t rpc_pkt_size = ez_rpc_header_size + size;
  int64_t size2 = sizeof(obrpc::ObRpcPacket) + rpc_pkt_size;

  if (timeout <= 0) {
    ret = common::OB_INVALID_ARGUMENT;
    RPC_FRAME_LOG(WARN, "invalid argument", K(timeout));
  } else if (!dst.is_valid()) {
    ret = common::OB_INVALID_ARGUMENT;
    RPC_FRAME_LOG(WARN, "invalid address", K(dst));
  } else if (OB_FAIL(create_session(s, dst, size2, timeout, local_addr,
      do_ratelimit, is_bg_flow, ssl_invited_nodes, cb, &req.cb_))) {
    RPC_FRAME_LOG(WARN, "create session fail", K(ret));
  } else if (NULL == s) {
    ret = common::OB_ERR_UNEXPECTED;
    RPC_FRAME_LOG(ERROR, "unexpected branch", K(ret));
  } else {
    /*
     *                   RPC request packet buffer format
     *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     *  |  ObRpcPacket  |  easy header |  RPC header  | RPC request |
     *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     */
    req.pkt_ = new (s->tx_buf) obrpc::ObRpcPacket();
    s->r.opacket = req.pkt_;
    s->opacket_size = rpc_pkt_size;
    req.s_ = s;

    // req.buf_ = reinterpret_cast<char*>(req.pkt_ + 1);
    req.buf_ = reinterpret_cast<char*>(req.pkt_ + 1) + ez_rpc_header_size;
    req.buf_len_ = size;
    req.pkt_->set_content(req.buf_, req.buf_len_);
  }
  return ret;
}

int ObReqTransport::balance_assign(easy_session_t *s) const
{
  static uint8_t io_index = 0;
  int idx = 0;
  if (bucket_count_ > 0) {
    /*
     * batch rpc eio
     *
     * Tatally (bucket_count_ + 1) IO thread created for batch RPC. The beginning (0 ~ bucket_count_ -1) IO
     * threads are used for legacy RPC. The last IO thread is dedicated to the connections with Ratelimit enabled.
     */
    if (EASY_TYPE_RL_SESSION == s->type) {
      idx = sgid_ + bucket_count_;
    } else {
      idx = sgid_ + (__sync_fetch_and_add(&io_index, 1) % bucket_count_);
    }
  } else {
    idx = sgid_ + (__sync_fetch_and_add(&io_index, 1) %
                            (eio_->io_thread_pool->thread_count));
  }
  return idx;
}

ObPacket *ObReqTransport::send_session(easy_session_t *s) const
{
  int ret = OB_SUCCESS;
  ObPacket *pkt = NULL;

  if (OB_ISNULL(s)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(s), K(ret));
  } else if (OB_ISNULL(eio_) || OB_ISNULL(eio_->io_thread_pool)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(s), K(eio_));
  }

  if (OB_SUCC(ret)) {
    // static uint8_t io_index = 0;
    // Synchronous rpc always needs to return packets
    s->unneed_response = false;
    s->r.client_start_time = common::ObTimeUtility::current_time();
    lib::Thread::loop_ts_ = s->r.client_start_time; // avoid clear_clock
    if (0 == s->addr.cidx) {
      s->addr.cidx = balance_assign(s);
    }

    pkt = reinterpret_cast<ObPacket*>(easy_client_send(eio_, s->addr, s));
    if (NULL == pkt) {
      char buff[OB_SERVER_ADDR_STR_LEN] = {'\0'};
      easy_inet_addr_to_str(&s->addr, buff, OB_SERVER_ADDR_STR_LEN);
      SERVER_LOG(WARN, "send packet fail", "dst", buff, KP(s));
    }
  }
  return pkt;
}

int ObReqTransport::post_session(easy_session_t *s) const
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
      s->addr.cidx = balance_assign(s);
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

int ObReqTransport::send(const Request &req, Result &r) const
{
  int easy_error = EASY_OK;
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(req.s_)) {
    ret = common::OB_INVALID_ARGUMENT;
    RPC_FRAME_LOG(ERROR, "invalid argument", K(req));
  } else {
    EVENT_INC(RPC_PACKET_OUT);
    EVENT_ADD(RPC_PACKET_OUT_BYTES,
              req.const_pkt().get_clen() + req.const_pkt().get_header_size() + common::OB_NET_HEADER_LENGTH);

    {
      lib::Thread::RpcGuard guard(req.s_->addr, req.const_pkt().get_pcode());
      r.pkt_ = reinterpret_cast<obrpc::ObRpcPacket*>(send_session(req.s_));
    }
    if (NULL == r.pkt_) {
      easy_error = req.s_->error;
      if (EASY_TIMEOUT == easy_error) {
        ret = common::OB_TIMEOUT;
        RPC_FRAME_LOG(WARN, "send packet fail due to session timeout. It may be caused by tenant queue "
                      "being full or deadlock in RPC server side, or someting else. Please look into it "
                      "in the log of server side", K(easy_error), K(ret));
      } else {
        ret = common::OB_RPC_SEND_ERROR;
        RPC_FRAME_LOG(WARN, "send packet fail", K(easy_error), K(ret));
      }
    } else {
      EVENT_INC(RPC_PACKET_IN);
      EVENT_ADD(RPC_PACKET_IN_BYTES,
                r.pkt_->get_clen() + r.pkt_->get_header_size() + common::OB_NET_HEADER_LENGTH);
    }
  }
  return ret;
}

int ObReqTransport::post(const Request &req) const
{
  EVENT_INC(RPC_PACKET_OUT);
  EVENT_ADD(RPC_PACKET_OUT_BYTES,
            req.const_pkt().get_clen() + req.const_pkt().get_header_size() + common::OB_NET_HEADER_LENGTH);
  return post_session(req.s_);
}
