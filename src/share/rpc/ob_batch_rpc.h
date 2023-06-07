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

#ifndef OCEANBASE_RPC_OB_BATCH_RPC_H_
#define OCEANBASE_RPC_OB_BATCH_RPC_H_

#include "ob_batch_proxy.h"
#include "lib/thread/ob_thread_name.h"
#include "lib/ob_define.h"
#include "lib/allocator/ob_qsync.h"
#include "lib/queue/ob_lighty_queue.h"
#include "lib/utility/serialization.h"
#include "lib/thread/thread_mgr_interface.h"
#include "share/config/ob_server_config.h"
#include "share/ob_cascad_member.h"
#include "share/ob_cluster_version.h"
#include "share/ob_thread_pool.h"
#include "share/ob_thread_mgr.h"
#include "share/ob_ls_id.h"
#include "common/ob_clock_generator.h"

namespace oceanbase
{
namespace obrpc
{
#define MYASSERT(x) if (!(x)) { ob_abort(); }
class ObSingleRpcBuffer
{
public:
  enum { HEADER_SIZE = sizeof(ObBatchPacket) };
  ObSingleRpcBuffer(int64_t seq, int64_t limit):
      seq_(seq), pos_(HEADER_SIZE), cnt_(0), capacity_(limit - sizeof(*this)) {}
  ~ObSingleRpcBuffer() {}
  bool is_empty() { return cnt_ <= 0; }
  bool wait(int64_t seq) { return seq == ATOMIC_LOAD(&seq_); }
  void reuse(int64_t seq) {
    pos_ = HEADER_SIZE;
    cnt_ = 0;
    ATOMIC_STORE(&seq_, seq);
  }
  char* alloc(int64_t size) {
    char* ret = NULL;
    MYASSERT(size <= capacity_ - HEADER_SIZE);
    int64_t limit = capacity_ - size;
    int64_t pos = faa_bounded(&pos_, size, limit);
    if (pos <= limit) {
      ATOMIC_FAA(&cnt_, 1);
      ret = buffer_ + pos;
    }
    return ret;
  }
  int64_t read(char*& buf, int64_t& cnt) {
    buf = buffer_;
    cnt = ATOMIC_LOAD(&cnt_);
    return ATOMIC_LOAD(&pos_);
  }
private:
  static int64_t faa_bounded(int64_t* addr, int64_t x, int64_t limit) {
    int64_t nv = ATOMIC_LOAD(addr);
    int64_t ov = 0;
    while((ov = nv) <= limit && ov != (nv = ATOMIC_VCAS(addr, ov, ov + x))) {
      PAUSE();
    }
    return ov;
  }
private:
  int64_t seq_ CACHE_ALIGNED;
  int64_t pos_ CACHE_ALIGNED;
  int64_t cnt_ CACHE_ALIGNED;
  int64_t capacity_ CACHE_ALIGNED;
  char buffer_[0];
};

class ObRingRpcBuffer
{
public:
  typedef ObSingleRpcBuffer Buffer;
  typedef ObIFill Req;
  typedef ObSimpleReqHeader ReqHeader;
  enum { RESERVED_SIZE = 1024 };
public:
  ObRingRpcBuffer(const int64_t buf_size, const int64_t buf_cnt)
      : read_seq_(0), fill_seq_(0), buf_size_(buf_size), buf_cnt_(buf_cnt)
  {
    for(int i = 0; i < buf_cnt_; i++) {
      new(get(i))Buffer(i, buf_size);
    }
  }
  ~ObRingRpcBuffer() {}
  bool write(const uint32_t batch_type, const uint32_t sub_type,
             const Req& req)
  {
    ReqHeader* header = NULL;
    const int64_t req_size = req.get_req_size();
    int64_t total_size = req_size;
    common::ObCurTraceId::TraceId *trace_id = common::ObCurTraceId::get_trace_id();
    uint32_t flag = 0;
    // with trace id
    flag = 1;
    total_size = total_size + trace_id->get_serialize_size();
    if (total_size <= buf_size_ - RESERVED_SIZE) {
      while(NULL == header) {
        CriticalGuard(get_qs());
        int64_t seq = get_fill_seq();
        Buffer* buffer = get(seq);
        if (buffer->wait(seq)) {
          if (NULL != (header = (ReqHeader*)buffer->alloc(sizeof(ReqHeader) + total_size))) {
            header->set(flag, batch_type, sub_type, (int32_t)total_size);
            char *buf = (char*)(header + 1);
            int64_t pos = 0;
            int64_t filled_size = 0;
            int ret = common::OB_SUCCESS;
            if ((1 == flag) && OB_FAIL(trace_id->serialize(buf, total_size, pos))) {
              header = NULL;
            } else {
              if (OB_FAIL(req.fill_buffer(buf + pos, req_size, filled_size))) {
                RPC_LOG(WARN, "fill buffer failed", K(ret), K(req_size), K(total_size));
                header = NULL;
              }
            }
          } else {
            freeze(seq);
          }
        } else {
          break;
        }
      }
    }
    return NULL != header;
  }
  bool write(const uint32_t batch_type, const int16_t sub_type, const share::ObLSID& ls,
             const Req& req)
  {
    ReqHeader* header = NULL;
    const int64_t req_size = req.get_req_size();
    int64_t total_size = ls.get_serialize_size() + req_size;
    common::ObCurTraceId::TraceId *trace_id = common::ObCurTraceId::get_trace_id();
    uint32_t flag = 0;
    flag = 1;
    total_size = total_size + trace_id->get_serialize_size();
    if (total_size <= buf_size_ - RESERVED_SIZE) {
      while(NULL == header) {
        CriticalGuard(get_qs());
        int64_t seq = get_fill_seq();
        Buffer* buffer = get(seq);
        if (buffer->wait(seq)) {
          if (NULL != (header = (ReqHeader*)buffer->alloc(sizeof(ReqHeader) + total_size))) {
            header->set(flag, batch_type, sub_type, (int32_t)total_size);
            char *buf = (char*)(header + 1);
            int64_t pos = 0;
            int64_t filled_size = 0;
            int ret = common::OB_SUCCESS;
            if ((1 == flag) && OB_FAIL(trace_id->serialize(buf, total_size, pos))) {
              header = NULL;
            } else if (OB_FAIL(ls.serialize(buf, total_size, pos))) {
              header = NULL;
            } else {
              if (OB_FAIL(req.fill_buffer(buf + pos, req_size, filled_size))) {
                RPC_LOG(WARN, "fill buffer failed", K(ret), K(req_size), K(total_size));
                header = NULL;
              }
            }
          } else {
            freeze(seq);
          }
        } else {
          break;
        }
      }
    }
    return NULL != header;
  }
  int64_t read(int64_t& seq, char*& buf, int64_t& cnt, bool read_current) {
    int64_t size = 0;
    const int64_t cur_read_seq = get_read_seq();
    if (cur_read_seq < get_fill_seq()
        || (read_current && !get(cur_read_seq)->is_empty())) {
      seq = cur_read_seq;
      freeze(seq);
      update_read_seq(cur_read_seq + 1);
      Buffer* buffer = get(seq);
      WaitQuiescent(get_qs());
      size = buffer->read(buf, cnt);
    }
    return size;
  }
  void reuse(int64_t seq) {
    get(seq)->reuse(seq + buf_cnt_);
  }
  void freeze() {
    const int64_t cur_read_seq = get_read_seq();
    if (!get(cur_read_seq)->is_empty()) {
      freeze(cur_read_seq);
    }
  }
  bool is_empty() const {
    bool bool_ret = false;
    const int64_t cur_read_seq = get_read_seq();
    if (cur_read_seq >= get_fill_seq() && get(cur_read_seq)->is_empty()) {
      bool_ret = true;
    }
    return bool_ret;
  }
protected:
  int64_t get_fill_seq() const { return ATOMIC_LOAD(&fill_seq_); }
  int64_t get_read_seq() const { return ATOMIC_LOAD(&read_seq_); }
  common::ObQSync& get_qs() {
    static common::ObQSync qsync;
    return qsync;
  }
  void freeze(int64_t seq) { common::inc_update(&fill_seq_, seq + 1); }
  void update_read_seq(const int64_t new_seq) { common::inc_update(&read_seq_, new_seq); }
private:
  Buffer* get(int64_t seq) const { return (Buffer*)(buf_ + buf_size_ * (seq % buf_cnt_)); }
private:
  int64_t read_seq_ CACHE_ALIGNED;
  int64_t fill_seq_ CACHE_ALIGNED;
  int64_t buf_size_ CACHE_ALIGNED;
  int64_t buf_cnt_ CACHE_ALIGNED;
  char buf_[0];
};

class ObRpcBuffer: public common::SpHashNode
{
public:
  typedef ObBatchPacket Packet;
  typedef ObRingRpcBuffer RingBuffer;
  typedef RingBuffer::Req Req;
  typedef ObBatchRpcProxy Rpc;
  ObRpcBuffer(const uint64_t tenant_id, const common::ObAddr& addr, const int64_t dst_cluster_id, const int batch_type, const int64_t buf_size, const int64_t buf_cnt):
      common::SpHashNode(calc_hash(tenant_id, addr, dst_cluster_id) | 1),
      tenant_id_(tenant_id),
      server_(addr),
      last_use_timestamp_(0),
      dst_cluster_id_(dst_cluster_id),
      batch_type_(batch_type),
      buffer_(buf_size, buf_cnt)
  {}
  ~ObRpcBuffer() {}
  bool fill(const uint32_t sub_type, const Req& req) {
    record_use_time();
    return buffer_.write(batch_type_, sub_type, req);
  }
  bool fill(const int16_t sub_type, const share::ObLSID& ls, const Req& req) {
    record_use_time();
    return buffer_.write(batch_type_, sub_type, ls, req);
  }
  void freeze() { buffer_.freeze(); }
  int64_t send(Rpc& rpc, uint64_t tenant_id, const common::ObAddr &sender, bool send_current);
  void record_use_time()
  {
    const int64_t now = common::ObClockGenerator::getClock();
    common::inc_update(&last_use_timestamp_, now);
  }
  int64_t get_last_use_ts() const { return ATOMIC_LOAD(&last_use_timestamp_); }
  uint64_t get_tenant_id() const { return tenant_id_; }
  common::ObAddr get_server() const { return server_; }
  int64_t get_dst_cluster_id() const { return ATOMIC_LOAD(&dst_cluster_id_); }
  bool is_empty() const {
    return buffer_.is_empty();
  }
  int64_t calc_hash(const uint64_t tenant_id, const common::ObAddr &addr, const int64_t dst_cluster_id) const
  {
    // server和cluster_id计算hash值与ObCascadMember::hash()一样
    int64_t ret_hash = (addr.hash() | (dst_cluster_id << 32));
    ret_hash = common::murmurhash(&tenant_id, sizeof(tenant_id), ret_hash);
    return ret_hash;
  }
  int compare(ObRpcBuffer* that) {
    int ret = 0;
    if (this->hash_ > that->hash_) {
      ret = 1;
    } else if (this->hash_ < that->hash_) {
      ret = -1;
    } else if (this->is_dummy()) {
      ret = 0;
    } else if (this->tenant_id_ > that->tenant_id_) {
      return 1;
    } else if (this->tenant_id_ < that->tenant_id_) {
      return -1;
    } else if(this->server_ > that->server_) {
      return 1;
    } else if(this->server_ < that->server_) {
      ret = -1;
    } else if(this->dst_cluster_id_ > that->dst_cluster_id_) {
      ret = 1;
    } else if(this->dst_cluster_id_ < that->dst_cluster_id_) {
      ret = -1;
    } else {
      ret = 0;
    }
    return ret;
  }
  void set_dst_cluster_id(const int64_t cluster_id) {
    if (cluster_id == common::OB_INVALID_CLUSTER_ID) {
      RPC_LOG_RET(ERROR, common::OB_INVALID_ARGUMENT, "cluster_id is invalid, unexpected", "server", server_, K(cluster_id));
    } else if (cluster_id != dst_cluster_id_) {
      const int64_t old_cluster_id = dst_cluster_id_;
      dst_cluster_id_ = cluster_id;
      RPC_LOG(INFO, "update server dst_cluster_id finished", "server", server_,
              "old cluster_id", old_cluster_id, "new cluster_id", cluster_id);
    }
  }
private:
  uint64_t tenant_id_;
  common::ObAddr server_;
  int64_t last_use_timestamp_;
  int64_t dst_cluster_id_;
  int batch_type_;
  RingBuffer buffer_;
};

class SingleWaitCond
{
public:
  SingleWaitCond(): n_waiters_(0), futex_() {}
  ~SingleWaitCond() {}
  void signal() {
    auto &ready = futex_.val();
    if (!ATOMIC_LOAD(&ready) && ATOMIC_BCAS(&ready, 0, 1)) {
      if (ATOMIC_LOAD(&n_waiters_) > 0) {
        futex_.wake(1);
      }
    }
  }
  bool wait(int64_t timeout)  {
    bool ready = ATOMIC_LOAD(&futex_.val());
    if (!ready) {
      ATOMIC_FAA(&n_waiters_, 1);
      futex_.wait(0, timeout);
      ATOMIC_FAA(&n_waiters_, -1);
    } else {
      ATOMIC_STORE(&futex_.val(), 0);
    }
    return ready;
  }
private:
  int32_t n_waiters_;
  lib::ObFutex futex_;
};

class ObBatchRpcBase
{
public:
  enum { BUCKET_NUM = 64, BATCH_BUFFER_COUNT = 4 };
  typedef ObRpcBuffer RpcBuffer;
  typedef RpcBuffer::Rpc Rpc;
  typedef RpcBuffer::Packet Packet;
  typedef RpcBuffer::Req Req;
  typedef common::FixedHash2<RpcBuffer> BufferMap;
  typedef SingleWaitCond SendCond;
  static const int64_t SVR_IDLE_TIME_THRESHOLD = 10 * 60 * 1000 * 1000L;  // 10 minutes
  ObBatchRpcBase(): is_inited_(false), batch_type_(-1), self_(), delay_us_(0), rpc_(nullptr), buffer_map_(nullptr)
  {}
  ~ObBatchRpcBase()
  {
    if (is_inited_) {
      is_inited_ = false;
      while (nullptr != buffer_map_ && !buffer_map_->is_empty()) {
        RpcBuffer* iter = NULL;
        if (NULL != (iter = buffer_map_->quick_next(iter))) {
          int cnt = 0;
          while (iter->send(*rpc_, iter->get_tenant_id(), self_, 0 == cnt) > 0) {
            cnt++;
          }
          if (iter->is_empty()) {
            RpcBuffer cur_key(iter->get_tenant_id(), iter->get_server(), iter->get_dst_cluster_id(), 0, 0, 0);
            RpcBuffer* cur_buf = NULL;
            if (0 == buffer_map_->del(&cur_key, cur_buf)) {
              destroy_buffer(cur_buf);
            }
          }
        }
      }
      if (nullptr != buffer_map_) {
        ob_free(buffer_map_);
      }
      RPC_LOG(INFO, "ObBatchRpcBase destroy finished");
    }
  }
  int init(int64_t delay_us, const uint32_t batch_type, Rpc* rpc, const common::ObAddr &self_addr)
  {
    int ret = common::OB_SUCCESS;
    void *obj_buf = common::ob_malloc(sizeof(BufferMap), common::ObModIds::OB_RPC_BUFFER);
    void *buf = common::ob_malloc(BUCKET_NUM * sizeof(common::SpHashNode), common::ObModIds::OB_RPC_BUFFER);
    if (NULL == obj_buf || NULL == buf) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      RPC_LOG(ERROR, "alloc memory failed", K(ret));
    } else {
      int64_t size = BUCKET_NUM * sizeof(common::SpHashNode);
      buffer_map_ = new (obj_buf) BufferMap(buf, size);
      if (CLOG_BATCH_REQ_NODELAY2 != batch_type) {
        batch_type_ = batch_type;
      } else {
        batch_type_ = CLOG_BATCH_REQ_NODELAY;
      }
      delay_us_ = delay_us;
      self_ = self_addr;
      rpc_ = rpc;
      is_inited_ = true;
    }
    if (!is_inited_) {
      if (NULL != obj_buf) {
        common::ob_free(obj_buf);
      }
      if (NULL != buf) {
        common::ob_free(buf);
      }
    }
    return ret;
  }
  int post(const uint64_t tenant_id, const common::ObAddr &dest, const int64_t dst_cluster_id,
      const uint32_t batch_type, const uint32_t sub_type,
      const Req& req);
  int post(const uint64_t tenant_id, const common::ObAddr &dest, const int64_t dst_cluster_id,
      const uint32_t batch_type, const int16_t sub_type,
      const share::ObLSID &ls, const Req& req);
  void do_work();
  int get_dst_svr_list(common::ObIArray<share::ObCascadMember> &dst_list);
protected:
  RpcBuffer* fetch(const uint64_t tenant_id, const common::ObAddr &server, const int64_t dst_cluster_id);
  common::ObQSync& get_qs() {
    static common::ObQSync qsync;
    return qsync;
  }
private:
  RpcBuffer* create_buffer(const uint64_t tenant_id, const common::ObAddr& addr, const int64_t dst_cluster_id);
  void destroy_buffer(RpcBuffer* p);
private:
  bool is_inited_;
  int batch_type_;
  common::ObAddr self_;
  int64_t delay_us_;
  Rpc* rpc_;
  SendCond cond_;
  BufferMap *buffer_map_;
};

class ObBatchRpc: public lib::TGRunnable
{
public:
  enum { MAX_THREAD_COUNT = BATCH_REQ_TYPE_COUNT,
         MINI_MODE_THREAD_COUNT = BATCH_REQ_TYPE_COUNT, };
  typedef ObRpcBuffer::Rpc Rpc;
  typedef ObRpcBuffer::Req Req;
  ObBatchRpc() : thread_cnt_(!lib::is_mini_mode() ? MAX_THREAD_COUNT : MINI_MODE_THREAD_COUNT)
  {}
  ~ObBatchRpc() { destroy(); }
  int init(rpc::frame::ObReqTransport *transport, rpc::frame::ObReqTransport *hp_transport,
           const common::ObAddr &self_addr)
  {
    int ret = common::OB_SUCCESS;
    bool is_hp_eio_enabled = false;
    if (static_cast<int32_t>(GCONF.high_priority_net_thread_count) > 0) {
      is_hp_eio_enabled = true;
    }

    if (OB_FAIL(rpc_.init(transport, self_addr))) {
      RPC_LOG(WARN, "rpc init failed", K(ret));
    } else if (is_hp_eio_enabled && OB_FAIL(hp_rpc_.init(hp_transport, self_addr))) {
      RPC_LOG(WARN, "hp_rpc init failed", K(ret));
    } else {
      for(int i = 0; i < thread_cnt_; i++) {
        if (is_hp_eio_enabled && is_hp_rpc(i)) {
          base_[i].init(get_batch_delay_us(i), i, &hp_rpc_, self_addr);
        } else {
          base_[i].init(get_batch_delay_us(i), i, &rpc_, self_addr);
        }
        RPC_LOG(INFO, "base thread init finished", K(is_hp_eio_enabled));
      }
    }
    return ret;
  }
  void stop() {
    TG_STOP(lib::TGDefIDs::BRPC);
  }
  void wait() {
    TG_WAIT(lib::TGDefIDs::BRPC);
  }
  void destroy() {
    TG_DESTROY(lib::TGDefIDs::BRPC);
  }
  void run1() {
    int64_t idx = (int64_t)get_thread_idx();
    lib::set_thread_name("BRPC", idx);
    while(!has_set_stop()) {
      base_[idx].do_work();
    }
  }
  int post(const uint64_t tenant_id, const common::ObAddr &dest, const int64_t dst_cluster_id,
           const uint32_t batch_type, const uint32_t sub_type,
           const Req& req) {
    int ret = common::OB_SUCCESS;
    const int idx = get_batch_thread_idx(batch_type);
    if (OB_UNLIKELY(has_set_stop())) {
      ret = common::OB_NOT_RUNNING;
    } else if (common::OB_INVALID_CLUSTER_ID == dst_cluster_id
          || !is_valid_tenant_id(tenant_id)
          || !dest.is_valid()) {
      ret = common::OB_INVALID_ARGUMENT;
    } else if (idx < thread_cnt_) {
      ret = base_[idx].post(tenant_id, dest, dst_cluster_id, batch_type, sub_type, req);
    } else {
      ret = common::OB_ERR_UNEXPECTED;
    }
    return ret;
  }
  int post(const uint64_t tenant_id, const common::ObAddr &dest, const int64_t dst_cluster_id,
           const uint32_t batch_type, const int16_t sub_type, const share::ObLSID& ls,
           const Req& req) {
    int ret = common::OB_SUCCESS;
    const int idx = get_batch_thread_idx(batch_type);
    if (OB_UNLIKELY(has_set_stop())) {
      ret = common::OB_NOT_RUNNING;
    } else if (common::OB_INVALID_CLUSTER_ID == dst_cluster_id
          || !is_valid_tenant_id(tenant_id)
          || !dest.is_valid()
          || !ls.is_valid()) {
      ret = common::OB_INVALID_ARGUMENT;
    } else if (idx < thread_cnt_) {
      ret = base_[idx].post(tenant_id, dest, dst_cluster_id, batch_type, sub_type, ls, req);
    } else {
      ret = common::OB_ERR_UNEXPECTED;
    }
    return ret;
  }
  int get_dst_svr_list(common::ObIArray<share::ObCascadMember> &dst_list);

private:
  Rpc rpc_;
  Rpc hp_rpc_;
  ObBatchRpcBase base_[MAX_THREAD_COUNT];
  const int thread_cnt_;
};

}; // end namespace rpc
}; // end namespace oceanbase

#undef MYASSERT
#endif /* OCEANBASE_RPC_OB_BATCH_RPC_H_ */
