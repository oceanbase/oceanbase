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

#include "rpc/obrpc/ob_poc_rpc_proxy.h"
#include "rpc/obrpc/ob_poc_rpc_server.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "rpc/obrpc/ob_net_keepalive.h"
#include "share/ob_errno.h"
extern "C" {
#include "rpc/pnio/r0/futex.h"
}

using namespace oceanbase::common;
namespace oceanbase
{
namespace obrpc
{
extern const int easy_head_size = 16;

common::ObCompressorType get_proxy_compressor_type(ObRpcProxy& proxy) {
  return proxy.get_compressor_type();
}

int ObSyncRespCallback::handle_resp(int io_err, const char* buf, int64_t sz)
{
  if (PNIO_OK != io_err) {
    if (PNIO_TIMEOUT == io_err) {
      send_ret_ = OB_TIMEOUT;
    } else {
      send_ret_ = OB_RPC_SEND_ERROR;
      RPC_LOG_RET(WARN, send_ret_, "pnio error", KP(buf), K(sz), K(io_err));
    }
  } else if (NULL == buf || sz <= easy_head_size) {
    send_ret_ = OB_TIMEOUT;
    RPC_LOG_RET(WARN, send_ret_, "response is null", KP(buf), K(sz), K(io_err));
  } else {
    EVENT_INC(RPC_PACKET_IN);
    EVENT_ADD(RPC_PACKET_IN_BYTES, sz);
    buf = buf + easy_head_size;
    sz = sz - easy_head_size; // skip easy header
    sz_ = sz;
    resp_ = reinterpret_cast<char *>(alloc(sz_));
    if (resp_ == NULL) {
      send_ret_ = OB_ALLOCATE_MEMORY_FAILED;
      RPC_LOG_RET(WARN, send_ret_, "alloc response buffer fail");
    } else {
      memcpy(resp_, buf, sz_);
    }
  }
  int ret = send_ret_;
  ATOMIC_STORE(&cond_, 1);
  rk_futex_wake(&cond_, 1);
  return ret;
}
int ObSyncRespCallback::wait(const int64_t wait_timeout_us, const int64_t pcode, const int64_t req_sz)
{
  ObWaitEventGuard wait_guard(ObWaitEventIds::SYNC_RPC, wait_timeout_us / 1000, pcode, req_sz);
  const struct timespec ts = {1, 0};
  bool has_terminated = false;
  while(ATOMIC_LOAD(&cond_) == 0) {
    if (OB_UNLIKELY((obrpc::OB_REMOTE_SYNC_EXECUTE == pcode || obrpc::OB_REMOTE_EXECUTE == pcode)
                    && !has_terminated
                    && OB_ERR_SESSION_INTERRUPTED == THIS_WORKER.check_status())) {
      RPC_LOG(INFO, "check session killed, will execute pn_terminate_pkt", K(gtid_), K(pkt_id_));
      int err = 0;
      if ((err = pn_terminate_pkt(gtid_, pkt_id_)) != 0) {
        int tmp_ret = tranlate_to_ob_error(err);
        RPC_LOG_RET(WARN, tmp_ret, "pn_terminate_pkt failed", K(err));
      } else {
        has_terminated = true;
      }
    }
    rk_futex_wait(&cond_, 0, &ts);
  }
  return send_ret_;
}

class ObPocSPAlloc: public rpc::frame::SPAlloc
{
public:
  ObPocSPAlloc(ObRpcMemPool& pool): pool_(pool) {}
  virtual ~ObPocSPAlloc() {}
  void* alloc(int64_t sz) const {
    return pool_.alloc(sz);
  }
private:
  ObRpcMemPool& pool_;
};

int ObAsyncRespCallback::create(ObRpcMemPool& pool, UAsyncCB* ucb, ObAsyncRespCallback*& pcb)
{
  int ret = OB_SUCCESS;
  ObPocSPAlloc sp_alloc(pool);
  UAsyncCB* cb = NULL;
  pcb = NULL;
  if (NULL == ucb) {
    // do nothing and not to allocate ObAsyncRespCallback object
  } else if (NULL == (pcb = (ObAsyncRespCallback*)pool.alloc(sizeof(ObAsyncRespCallback)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    RPC_LOG(WARN, "alloc resp callback fail", K(ret));
  } else if (NULL == (cb = ucb->clone(sp_alloc))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    pcb = NULL;
    RPC_LOG(WARN, "ucb.clone fail", K(ret));
  } else {
    cb->low_level_cb_ = pcb;
    if (cb != ucb) {
      cb->set_cloned(true);
    }
    new(pcb)ObAsyncRespCallback(pool, cb);
  }
  return ret;
}

int ObAsyncRespCallback::handle_resp(int io_err, const char* buf, int64_t sz)
{
  int ret = OB_SUCCESS;
  const int64_t start_time = ObTimeUtility::current_time();
  int64_t after_decode_time = 0;
  int64_t after_process_time = 0;
  ObRpcPacketCode pcode = OB_INVALID_RPC_CODE;
  ObRpcPacket* ret_pkt = NULL;
  if (buf != NULL && sz > easy_head_size) {
    EVENT_INC(RPC_PACKET_IN);
    EVENT_ADD(RPC_PACKET_IN_BYTES, sz);
    sz = sz - easy_head_size;
    buf = buf + easy_head_size;
  } else {
    sz = 0;
    buf = NULL;
  }
  if (ucb_ == NULL) {
    // do nothing
  } else {
    ucb_->record_stat(buf == NULL);
    bool cb_cloned = ucb_->get_cloned();
    pcode = ucb_->get_pcode();
    if (0 != io_err) {
      ucb_->set_error(io_err);
      if (OB_SUCCESS != ucb_->on_error(io_err)) {
        ucb_->on_timeout();
      }
    } else if (NULL == buf) {
      ucb_->on_timeout();
    } else if (OB_FAIL(rpc_decode_ob_packet(pool_, buf, sz, ret_pkt))) {
      ucb_->on_invalid();
      RPC_LOG(WARN, "rpc_decode_ob_packet fail", K(ret));
    } else if (OB_FALSE_IT(ObCurTraceId::set(ret_pkt->get_trace_id()))) {
    }
#ifdef ERRSIM
    else if (OB_FALSE_IT(THIS_WORKER.set_module_type(ret_pkt->get_module_type()))) {
    }
#endif
    else if (OB_FAIL(ucb_->decode(ret_pkt))) {
      ucb_->on_invalid();
      RPC_LOG(WARN, "ucb.decode fail", K(ret));
    } else {
      after_decode_time = ObTimeUtility::current_time();
      int tmp_ret = OB_SUCCESS;
      pcode = ret_pkt->get_pcode();
      if (OB_SUCCESS != (tmp_ret = ucb_->process())) {
        RPC_LOG(WARN, "ucb.process fail", K(tmp_ret));
      }
      after_process_time = ObTimeUtility::current_time();
    }
    if (cb_cloned) {
      ucb_->~AsyncCB();
    }
  }
  pool_.destroy();
  ObCurTraceId::reset();
  THIS_WORKER.get_sql_arena_allocator().reset();
  const int64_t cur_time = ObTimeUtility::current_time();
  const int64_t total_time = cur_time  - start_time;
  const int64_t decode_time = after_decode_time - start_time;
  const int64_t process_time = after_process_time - after_decode_time;
  const int64_t session_destroy_time = cur_time - after_process_time;
  if (total_time > OB_EASY_HANDLER_COST_TIME) {
    RPC_LOG(WARN, "async_cb handler cost too much time", K(total_time), K(decode_time),
        K(process_time), K(session_destroy_time), K(ret), K(pcode));
  }
  return ret;
}

void init_ucb(ObRpcProxy& proxy, UAsyncCB* ucb, const common::ObAddr& dest, int64_t send_ts, int64_t payload_sz)
{
  ucb->set_dst(dest);
  ucb->set_tenant_id(proxy.get_tenant());
  ucb->set_timeout(proxy.timeout());
  ucb->set_send_ts(send_ts);
  ucb->set_payload(payload_sz);
}
static easy_addr_t to_ez_addr(const ObAddr &addr)
{
  easy_addr_t ez;
  memset(&ez, 0, sizeof (ez));
  if (addr.is_valid()) {
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

int64_t ObPocClientStub::get_proxy_timeout(ObRpcProxy& proxy) {
  return proxy.timeout();
}
int32_t ObPocClientStub::get_proxy_group_id(ObRpcProxy& proxy) {
  return proxy.get_group_id();
}

void ObPocClientStub::set_rcode(ObRpcProxy& proxy, const ObRpcResultCode& rcode) {
  proxy.set_result_code(rcode);
}
void ObPocClientStub::set_handle(ObRpcProxy& proxy, Handle* handle, const ObRpcPacketCode& pcode, const ObRpcOpts& opts, bool is_stream_next, int64_t session_id, int64_t pkt_id, int64_t send_ts) {
  proxy.set_handle_attr(handle, pcode, opts, is_stream_next, session_id, pkt_id, send_ts);
}
int ObPocClientStub::translate_io_error(int io_err) {
  return tranlate_to_ob_error(io_err);
}

int ObPocClientStub::log_user_error_and_warn(const ObRpcResultCode &rcode) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_SUCCESS != rcode.rcode_)) {
    FORWARD_USER_ERROR(rcode.rcode_, rcode.msg_);
  }
  for (int i = 0; OB_SUCC(ret) && i < rcode.warnings_.count(); ++i) {
    const common::ObWarningBuffer::WarningItem warning_item = rcode.warnings_.at(i);
    if (ObLogger::USER_WARN == warning_item.log_level_) {
      FORWARD_USER_WARN(warning_item.code_, warning_item.msg_);
    } else if (ObLogger::USER_NOTE == warning_item.log_level_) {
      FORWARD_USER_NOTE(warning_item.code_, warning_item.msg_);
    } else {
      ret = common::OB_ERR_UNEXPECTED;
      RPC_LOG(WARN, "unknown log type", K(ret));
    }
  }
  return ret;
}

int ObPocClientStub::check_blacklist(const common::ObAddr& addr) {
  int ret = OB_SUCCESS;
  if(!addr.is_valid()) {
    ret = common::OB_INVALID_ARGUMENT;
    RPC_LOG(WARN, "invalid addr", K(ret), K(addr));
  } else {
    easy_addr_t ez_addr = to_ez_addr(addr);
    if (ObNetKeepAlive::get_instance().in_black(ez_addr)) {
      ret = OB_RPC_POST_ERROR;
      if (REACH_TIME_INTERVAL(1000000)) {
        RPC_LOG(WARN, "address in blacklist", K(ret), K(addr));
      }
    }
  }
  return ret;
}
ObPocClientStub global_poc_client;
}; // end namespace obrpc
}; // end namespace oceanbase
