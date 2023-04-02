#pragma once
extern "C" {
#include "../pkt-nio.h"
};
#include "rpc_mem_pool.h"
#include "simple_mem_pool.h"

class NioImpl;
class ReqHandleCtx
{
public:
  ReqHandleCtx(IMemPool& pool): pool_(pool) {}
  ~ReqHandleCtx() {}
  IMemPool& get_pool() { return pool_; }
  int resp(const char* req, int64_t req_size);
  void* alloc(int64_t sz) { return pool_.alloc(sz); }
  void destroy() { pool_.destroy(); }
  void set(NioImpl* nio, void* req_ref, uint64_t sock_id, uint64_t pkt_id) {
    nio_ = nio;
    req_ref_ = req_ref;
    sock_id_ = sock_id;
    pkt_id_ = pkt_id;
  }
  uint64_t get_sock_id() { return sock_id_; }
  uint64_t get_pkt_id() { return pkt_id_; }
  void release_req() {
    if (NULL != req_ref_) {
      ref_free(req_ref_);
      req_ref_ = NULL;
    }
  }
private:
  IMemPool& pool_;
  NioImpl* nio_;
  void* req_ref_;
  uint64_t sock_id_;
  uint64_t pkt_id_;
};

class IReqHandler
{
public:
  IReqHandler() {}
  virtual ~IReqHandler() {}
  virtual IMemPool* create_pool() { return RpcMemPool::create(0); }
  virtual int handle_req(ReqHandleCtx* ctx, const char* buf, int64_t sz) = 0;
};

class IRespHandler
{
public:
  IRespHandler(IMemPool* pool): pool_(pool), ref_(0), packet_id_(0) {}
  virtual ~IRespHandler() {}
  virtual int handle_resp(int io_err, const char* buf, int64_t sz) = 0;
  IMemPool* get_pool() { return pool_; }
  void* alloc(int64_t sz) { return pool_->alloc(sz); }
  void free_resp() { do_xref(-1); }
  void free_req() { do_xref(1); }
  uint64_t get_packet_id() const { return packet_id_; }
  void set_packet_id(uint64_t packet_id) { packet_id_ = packet_id; }
private:
  void destroy() {
    // can not detect null pool_ ptr
    pool_->destroy();
  }
  int xref(int x) { return AAF(&ref_, x); }
  void do_xref(int x) {
    if (0 == xref(x)) {
      destroy();
    }
  }
private:
  IMemPool* pool_;
  int ref_;
  uint64_t packet_id_;
};

class NioImpl
{
public:
  NioImpl(): idx_(0), next_pkt_id_(0), req_handler_(NULL) {}
  ~NioImpl() {}
  struct ClientReq {
    IRespHandler* handler_;
    pktc_cb_t cb_;
    pktc_req_t req_;
    easy_head_t head_;
  };
  struct ServerResp {
    ReqHandleCtx* ctx_;
    pkts_req_t req_;
    easy_head_t head_;
  };
  int init(int idx, IReqHandler* req_handler, int port) {
    int err = 0;
    pkts_cfg_t svr_cfg;
    svr_cfg.handle_func = pkts_handle_func;
    addr_init(&svr_cfg.addr, "0.0.0.0", port);
    if (0 != (err = eloop_init(&ep_))) {
    } else if (port > 0 && 0 != (err = pkts_init(&pkts_, &ep_, &svr_cfg))) {
    } else if (0 != (err = pktc_init(&pktc_, &ep_, 0))) {
    } else {
      idx_ = idx;
      req_handler_ = req_handler;
    }
    return err;
  }
  int post(const addr_t& addr, const char* req, int64_t req_size,  IRespHandler* resp_handler, int64_t timeout_us) {
    ClientReq* post_struct = (typeof(post_struct))resp_handler->alloc(sizeof(*post_struct) + req_size);
    post_struct->handler_ = resp_handler;
    pktc_req_t* r = &post_struct->req_;
    pktc_cb_t* cb = &post_struct->cb_;
    cb->id = gen_packet_id();
    cb->expire_us = rk_get_corse_us() + timeout_us;
    cb->resp_cb = pktc_resp_cb_func;
    r->flush_cb = pktc_flush_cb_func;
    r->resp_cb = cb;
    r->dest = addr;
    eh_copy_msg(&r->msg, cb->id, req, req_size);
    resp_handler->set_packet_id(cb->id);
    return pktc_post(&pktc_, r);
  }
  int resp(ReqHandleCtx* ctx, const char* req, int64_t req_size) {
    ServerResp* resp_struct = (typeof(resp_struct))ctx->alloc(sizeof(*resp_struct) + req_size);
    resp_struct->ctx_ = ctx;
    pkts_req_t* r = &resp_struct->req_;
    r->flush_cb = pkts_flush_cb_func;
    r->sock_id = ctx->get_sock_id();
    eh_copy_msg(&r->msg, ctx->get_pkt_id(), req, req_size);
    return pkts_resp(&pkts_, r);
  }
  void do_work() {
    eloop_run(&ep_);
  }
private:
  uint32_t gen_packet_id() {
    static uint32_t g_next_pkt_id = 0;
    return AAF(&g_next_pkt_id, 1);
  }
  static int pkts_handle_func(pkts_t* pkts, void* req_handle, const char* b, int64_t s, uint64_t sock_id) {
    NioImpl* impl = structof(pkts, NioImpl, pkts_);
    IReqHandler* req_handler = impl->req_handler_;
    uint64_t pkt_id = eh_packet_id(b);
    IMemPool* pool = req_handler->create_pool();
    ReqHandleCtx* ctx = (typeof(ctx))pool->alloc(sizeof(*ctx));
    new(ctx)ReqHandleCtx(*pool);
    ctx->set(impl, req_handle, sock_id, pkt_id);
    return req_handler->handle_req(ctx, b + sizeof(easy_head_t), s - sizeof(easy_head_t));
  }
  static void pkts_flush_cb_func(pkts_req_t* req) {
    ServerResp* resp_struct = structof(req, ServerResp, req_);
    ReqHandleCtx* ctx = resp_struct->ctx_;
    ctx->destroy();
  }
  static void pktc_flush_cb_func(pktc_req_t* req) {
    ClientReq* ps = structof(req, ClientReq, req_);
    IRespHandler* resp_cb = ps->handler_;
    resp_cb->free_req();
  }
  static void pktc_resp_cb_func(pktc_cb_t* cb, const char* resp, int64_t sz) {
    ClientReq* ps = structof(cb, ClientReq, cb_);
    IRespHandler* resp_cb = ps->handler_;
    if (NULL != resp) {
      resp += sizeof(easy_head_t);
      sz -= sizeof(easy_head_t);
    }
    resp_cb->handle_resp(0, resp, sz);
  }
private:
  eloop_t ep_;
  uint64_t idx_;
  uint32_t next_pkt_id_;
  IReqHandler* req_handler_;
  pkts_t pkts_;
  pktc_t pktc_;
};

inline int ReqHandleCtx::resp(const char* req, int64_t req_size)
{
  release_req(); // after nio_->resp(), ctx self is invalid.
  int err = nio_->resp(this, req, req_size);
  return err;
}

#include <pthread.h>
class Nio
{
public:
  enum { N_THREAD = 32};
  Nio(): n_thread_(0), thread_idx_(0), io_idx_round_robin_(0) {}
  ~Nio() {}
  int start(IReqHandler* req_handler, int port, int n_thread) {
    int err = 0;
    for(int i = 0; 0 == err && i < n_thread; i++) {
      if (0 != (err = impl_[i].init(i, req_handler, port))) {
        rk_error("nio init fail: %d", err);
      } else {
        if (0 != (err = pthread_create(pd_ + i, NULL, thread_work, this))) {
          rk_error("pthread create fail: %d", err);
        }
      }
    }
    if (0 == err) {
      n_thread_ = n_thread;
    }
    return err;
  }
  int post(const addr_t& addr, const char* req, int64_t req_size,  IRespHandler* resp_handler, int64_t timeout_us) {
    int64_t idx = (FAA(&io_idx_round_robin_, 1) % n_thread_);
    return impl_[idx].post(addr, req, req_size, resp_handler, timeout_us);
  }
private:
  void do_thread_work() {
    int64_t idx = FAA(&thread_idx_, 1);
    impl_[idx].do_work();
  }
  static void* thread_work(void* arg) {
    Nio* nio = (typeof(nio))arg;
    nio->do_thread_work();
    return NULL;
  }
private:
  int64_t n_thread_;
  int64_t thread_idx_;
  int64_t io_idx_round_robin_;
  pthread_t pd_[N_THREAD];
  NioImpl impl_[N_THREAD];
};
