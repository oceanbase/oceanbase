/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define MAX_PN_LISTEN 256
#define MAX_PN_GRP (1<<17)
#define MAX_PN_PER_GRP 64
#define CHUNK_SIZE (1<<14) - 128

typedef struct pn_listen_t
{
  listen_t l;
  serve_cb_t serve_cb;
  pthread_t pd;
} pn_listen_t;

struct pn_t;
typedef struct pn_grp_t
{
  PN_GRP_COMM;
  struct pn_t* pn_array[MAX_PN_PER_GRP];
} pn_grp_t;

typedef struct pn_t
{
  PN_COMM;
  eloop_t ep;
  serve_cb_t serve_cb;
  fifo_alloc_t server_ctx_alloc;
  cfifo_alloc_t server_resp_alloc;
  cfifo_alloc_t client_req_alloc;
  cfifo_alloc_t client_cb_alloc;
  chunk_cache_t server_ctx_chunk_alloc;
  chunk_cache_t server_resp_chunk_alloc;
  chunk_cache_t client_req_chunk_alloc;
  chunk_cache_t client_cb_chunk_alloc;
  pkts_t pkts;
  pktc_t pktc;
} pn_t;

static int next_pn_listen_idx;
static pn_listen_t pn_listen_array[MAX_PN_LISTEN];
static pn_grp_t* pn_grp_array[MAX_PN_GRP];
static int pn_has_listened = 0;
int64_t pnio_keepalive_timeout;
PN_API int64_t pn_set_keepalive_timeout(int64_t user_timeout) {
  if (user_timeout >= 0) {
    pnio_keepalive_timeout = user_timeout;
  }
  return pnio_keepalive_timeout;
}
static pn_listen_t* locate_listen(int idx)
{
  return pn_listen_array + idx;
}

static void* listen_thread_func(void* arg)
{
  thread_counter_reg();
  ob_set_thread_name("pnlisten");
  pn_listen_t* l = (typeof(l))arg;
  eloop_run(&l->l.ep);
  return NULL;
}

static __thread pn_comm_t* current_pnio;
pn_comm_t* get_current_pnio()
{
  return current_pnio;
}
static void* pn_thread_func(void* arg)
{
  thread_counter_reg();
  pn_t* pn = (typeof(pn))arg;
  char pnio_name[16];
  snprintf(pnio_name, sizeof(pnio_name), "pnio%d", pn->gid);
  ob_set_thread_name(pnio_name);
  current_pnio = (pn_comm_t*)pn;
  eloop_run(&pn->ep);
  return NULL;
}

static int pnl_dispatch_accept(int fd, const void* b, int sz);
extern bool is_support_ipv6_c();
PN_API int pn_listen(int port, serve_cb_t cb)
{
  int idx = FAA(&next_pn_listen_idx, 1);
  pn_listen_t* pnl = locate_listen(idx);
  addr_t addr;
  addr_t addr6;
  addr_init(&addr, "0.0.0.0", port);
  if (is_support_ipv6_c()) {
    addr_init(&addr6, "::", port);
  }

  if (ATOMIC_BCAS(&pn_has_listened, 0, 1)) {
    if (listen_create(addr) <= 0 ||
        (is_support_ipv6_c() && listen_create(addr6) <= 0)) {
      idx = -1;
      ATOMIC_STORE(&pn_has_listened, 0);
    } else {
      pnl->serve_cb = cb;
    }
  }
  return idx;
}

static pn_grp_t* create_grp()
{
  pn_grp_t* grp = (typeof(grp))salloc(sizeof(*grp));
  if (grp) {
    memset(grp, 0, sizeof(*grp));
    grp->rx_bw = RATE_UNLIMITED;
  }
  return grp;
}

static pn_grp_t* locate_grp(int gid)
{
  if (unlikely(gid < 0 || gid >= arrlen(pn_grp_array))) {
    return NULL;
  }
  return pn_grp_array[gid];
}

static pn_grp_t* ensure_grp(int gid)
{
  pn_grp_t** pgrp = pn_grp_array + gid;
  if (unlikely(NULL == *pgrp)) {
    *pgrp = create_grp();
  }
  return *pgrp;
}

static int dispatch_fd_to(int fd, uint32_t gid, uint32_t tid)
{
  int err = 0;
  pn_grp_t* grp = locate_grp(gid);
  if (NULL == grp || grp->count == 0) {
    err = -ENOENT;
  } else {
    pn_t* pn = grp->pn_array[tid % grp->count];
    int wbytes = 0;
    while((wbytes = write(pn->accept_qfd, (const char*)&fd, sizeof(fd))) < 0
          && EINTR == errno);
    if (wbytes != sizeof(fd)) {
      err = -EIO;
    }
  }
  return err;
}

static int pnl_dispatch_accept(int fd, const void* b, int sz)
{
  int err = 0;
  uint64_t dispatch_id = 0;
  if ((uint64_t)sz < sizeof(dispatch_id)) {
    err = dispatch_fd_to(fd, 1, 0);
  } else {
    dispatch_id = *(uint64_t*)b;
    uint32_t gid = dispatch_id >> 32;
    uint32_t tid = dispatch_id & ((1ULL<<32) - 1);
    err = dispatch_fd_to(fd, gid, tid);
  }
  return err;
}

static int pn_pkts_handle_func(pkts_t* pkts, void* req_handle, const char* b, int64_t s, uint64_t chid);

static pn_t* pn_alloc()
{
  pn_t* pn = (typeof(pn))salloc(sizeof(*pn));
  if (pn) {
    memset(pn, 0, sizeof(*pn));
  }
  return pn;
}

static void pn_destroy(pn_t* pn)
{
  if (pn->accept_qfd >= 0) {
    close(pn->accept_qfd);
    pn->accept_qfd = -1;
  }
  sfree(pn);
}

static uint64_t calc_dispatch_id(uint32_t gid, uint32_t tid)
{
  return ((uint64_t)gid)<<32 | tid;
}

static int pn_init_pkts(int listen_id, pn_t* pn)
{
  int err = 0;
  int pfd[2] = {-1, -1};
  pkts_cfg_t cfg = {.handle_func = pn_pkts_handle_func };
  if (listen_id < 0) {
  } else if (0 != pipe2(pfd, O_NONBLOCK|O_CLOEXEC)) {
    err = errno;
  } else {
    pn->accept_qfd = pfd[1];
    cfg.accept_qfd = pfd[0];
    if (0 != (err = pkts_init(&pn->pkts, &pn->ep, &cfg))) {
    } else {
      pn_listen_t* pnl = locate_listen(listen_id);
      pn->serve_cb = pnl->serve_cb;
    }
  }
  return err;
}

static pn_t* pn_create(int listen_id, int gid, int tid)
{
  int err = 0;
  pn_t* pn = NULL;
  if (NULL == (pn = pn_alloc())) {
  } else if (0 != (err = eloop_init(&pn->ep))) {
  } else if (0 != (err = eloop_rl_init(&pn->ep, &pn->ep.rl_impl))) {
  } else if (0 != (err = pktc_init(&pn->pktc, &pn->ep, calc_dispatch_id(gid, tid))))  {
  } else if (0 != (err = pn_init_pkts(listen_id, pn))) {
  } else {
    pn->gid = gid;
    pn->tid = tid;
    pn->pn_grp = (pn_grp_comm_t*)locate_grp(gid);
    chunk_cache_init(&pn->server_ctx_chunk_alloc, CHUNK_SIZE, MOD_SERVER_CTX_CHUNK);
    chunk_cache_init(&pn->server_resp_chunk_alloc, CHUNK_SIZE, MOD_SERVER_RESP_CHUNK);
    chunk_cache_init(&pn->client_req_chunk_alloc, CHUNK_SIZE, MOD_CLIENT_REQ_CHUNK);
    chunk_cache_init(&pn->client_cb_chunk_alloc, CHUNK_SIZE, MOD_CLIENT_CB_CHUNK);

    fifo_alloc_init(&pn->server_ctx_alloc, &pn->server_ctx_chunk_alloc);
    cfifo_alloc_init(&pn->server_resp_alloc, &pn->server_resp_chunk_alloc);
    cfifo_alloc_init(&pn->client_req_alloc, &pn->client_req_chunk_alloc);
    cfifo_alloc_init(&pn->client_cb_alloc, &pn->client_cb_chunk_alloc);
    pn->is_stop_ = false;
  }
  if (0 != err && NULL != pn) {
    pn_destroy(pn);
    pn = NULL;
  }
  return pn;
}


PN_API int pn_provision(int listen_id, int gid, int thread_count)
{
  int err = 0;
  int count = 0;
  pn_grp_t* pn_grp = ensure_grp(gid);
  if (pn_grp == NULL) {
    err = -ENOMEM;
    rk_error("ensure group failed, gid=%d", gid);
  } else if (thread_count > MAX_PN_PER_GRP) {
    err = -EINVAL;
    rk_error("thread count is too large, thread_count=%d, MAX_PN_PER_GRP=%d", thread_count, MAX_PN_PER_GRP);
  }
  count = pn_grp->count;
  // restart stoped threads
  for (int i = 0; 0 == err && i < count; i++) {
    pn_t* pn = pn_grp->pn_array[i];
    if (!pn->is_stop_) {
      // pn thread is running, do nothing
    } else if (pn->pd) {
      err = PNIO_ERROR;
      rk_error("pn is stopped but the thread is still running, gid=%d, i=%d", gid, i)
    } else {
      pn->is_stop_ = false;
      if (0 != (err = ob_pthread_create(&pn->pd, pn_thread_func, pn))) {
        pn->is_stop_ = true;
        rk_error("pthread_create failed, gid=%d, i=%d", gid, i);
      } else {
        rk_info("pn pthread created, gid=%d, i=%d", gid, i);
      }
    }
  }
  while(0 == err && count < thread_count) {
    pn_t* pn = pn_create(listen_id, gid, count);
    if (NULL == pn) {
      err = ENOMEM;
    } else if (0 != (err = ob_pthread_create(&pn->pd, pn_thread_func, pn))) {
      pn_destroy(pn);
    } else {
      pn_grp->pn_array[count++] = pn;
    }
  }
  int ret = -1;
  if (0 == err) {
    pn_grp->count = count;
    ret = pn_grp->count;
  }
  return ret;
}

typedef struct pn_pktc_cb_t
{
  pktc_cb_t cb;
  client_cb_t client_cb;
  void* arg;
} pn_pktc_cb_t;

typedef struct pn_client_req_t
{
  pktc_req_t req;
  easy_head_t head;
} pn_client_req_t;


typedef struct pn_client_slice_t
{
  int64_t ref_;
  pn_pktc_cb_t cb_;
  pn_client_req_t req_;
} pn_client_slice_t;

static void pn_pktc_flush_cb(pktc_req_t* r)
{
  pn_client_req_t* pn_req = structof(r, pn_client_req_t, req);
  pktc_cb_t* cb = r->resp_cb;
  if (cb) {
    cb->req = NULL;
  }
  cfifo_free(pn_req);
}

static void pn_pktc_resp_cb(pktc_cb_t* cb, const char* resp, int64_t sz)
{
  pn_pktc_cb_t* pn_cb = structof(cb, pn_pktc_cb_t, cb);
  pktc_req_t* req = cb->req;
  if (req) {
    req->resp_cb = NULL;
  }
  if (cb->sk) {
    cb->sk->sk_diag_info.doing_cnt --;
    cb->sk->sk_diag_info.done_cnt ++;
  }
  PNIO_DELAY_WARN(STAT_TIME_GUARD(eloop_client_cb_count, eloop_client_cb_time));
  pn_cb->client_cb(pn_cb->arg, cb->errcode, resp, sz);
  cfifo_free(pn_cb);
}

static pktc_req_t* pn_create_pktc_req(pn_t* pn, uint64_t pkt_id, addr_t dest, const pn_pkt_t* pkt)
{
  const char* req = pkt->buf;
  const int64_t req_sz = pkt->sz;
  pn_client_req_t* pn_req = (typeof(pn_req))cfifo_alloc(&pn->client_req_alloc, sizeof(*pn_req) + req_sz);
  if (unlikely(NULL == pn_req)) {
    return NULL;
  }
  struct pn_pktc_cb_t* pn_cb = (typeof(pn_cb))cfifo_alloc(&pn->client_cb_alloc, sizeof(*pn_cb));
  if (unlikely(NULL == pn_cb)) {
    cfifo_free(pn_req);
    return NULL;
  }
  pktc_cb_t* cb = &pn_cb->cb;
  pktc_req_t* r = &pn_req->req;
  pn_cb->client_cb = pkt->cb;
  pn_cb->arg = pkt->arg;
  cb->id = pkt_id;
  cb->expire_us = pkt->expire_us;
  cb->resp_cb = pn_pktc_resp_cb;
  cb->errcode = PNIO_OK;
  cb->req = r;
  cb->sk = NULL;
  r->pkt_type = PN_NORMAL_PKT;
  r->flush_cb = pn_pktc_flush_cb;
  r->resp_cb = cb;
  r->dest = dest;
  r->categ_id = pkt->categ_id;
  r->sk = NULL;
  dlink_init(&r->link);
  eh_copy_msg(&r->msg, cb->id, req, req_sz);
  return r;
}

static pktc_req_t* pn_create_cmd_req(pn_t* pn, int64_t cmd, uint64_t pkt_id)
{
  pktc_req_t* r = NULL;
  pn_client_cmd_req_t* pn_req = (typeof(pn_req))cfifo_alloc(&pn->client_req_alloc, sizeof(*pn_req));
  if (likely(pn_req)) {
    memset(pn_req, 0, sizeof(*pn_req));
    r = &pn_req->req;
    r->pkt_type = PN_CMD_PKT;
    r->flush_cb = NULL;
    r->resp_cb = NULL;
    pn_req->cmd = cmd;
    pn_req->arg = pkt_id;
    eh_copy_msg(&r->msg, pkt_id, NULL, 0);
  }
  return r;
}

static uint32_t global_next_pkt_id RK_CACHE_ALIGNED;
static uint32_t gen_pkt_id()
{
  uint32_t next_pkt_id = FAA(&global_next_pkt_id, 1);
  return next_pkt_id;
}

static pn_t* get_pn_for_send(pn_grp_t* pgrp, int tid)
{
  return pgrp->pn_array[tid % pgrp->count];
}

extern bool is_valid_sockaddr_c(struct sockaddr_storage *sock_addr);
PN_API int pn_send(uint64_t gtid, struct sockaddr_storage* sock_addr, const pn_pkt_t* pkt, uint32_t* pkt_id_ret)
{
  int err = 0;
  const char* buf = pkt->buf;
  const int64_t sz = pkt->sz;
  const int16_t categ_id = pkt->categ_id;
  const int64_t expire_us = pkt->expire_us;
  const void* arg = pkt->arg;

  pn_grp_t* pgrp = locate_grp(gtid>>32);
  pn_t* pn = get_pn_for_send(pgrp, gtid & 0xffffffff);
  addr_t dest;
  sockaddr_to_addr(sock_addr, &dest);
  uint32_t pkt_id = gen_pkt_id();
  if (!is_valid_sockaddr_c(sock_addr)) {
    err = -EINVAL;
    rk_warn("invalid sin_addr");
  } else if (expire_us < 0) {
    err = -EINVAL;
    rk_error("invalid rpc timeout: %ld, it might be that the up-layer rpc timeout is too large, categ_id=%d", expire_us, categ_id);
  } else if (LOAD(&pn->is_stop_)) {
    err = PNIO_STOPPED;
  } else {
    pktc_req_t* r = pn_create_pktc_req(pn, pkt_id, dest, pkt);
    if (NULL == r) {
      err = ENOMEM;
    } else {
      if (NULL != arg) {
        *((void**)arg) = r;
        *pkt_id_ret = pkt_id;
      }
      err = pktc_post(&pn->pktc, r);
    }
  }
  rk_trace("send rpc packet, gtid=%lx, pkt_id=%u, catg_id=%d, expire_us=%ld, sz=%ld, err=%d", gtid, pkt_id, categ_id, expire_us, sz, err);
  return err;
}

PN_API void pn_stop(uint64_t gid)
{
  pn_grp_t *pgrp = locate_grp(gid);
  if (pgrp != NULL) {
    for (int tid = 0; tid < pgrp->count; tid++) {
      pn_t *pn = get_pn_for_send(pgrp, tid);
      ATOMIC_STORE(&pn->is_stop_, true);
    }
  }
}

PN_API void pn_wait(uint64_t gid)
{
  pn_grp_t *pgrp = locate_grp(gid);
  if (pgrp != NULL) {
    for (int tid = 0; tid < pgrp->count; tid++) {
      pn_t *pn = get_pn_for_send(pgrp, tid);
      if (NULL != pn->pd) {
        ob_pthread_join(pn->pd);
        pn->pd = NULL;
      }
    }
  }
}

void pn_release(pn_comm_t* pn_comm)
{
  if (NULL == pn_comm) {
    // do nothing
    rk_warn("unexpected argument");
  } else {
    pn_t* pn = (typeof(pn))pn_comm;
    // empty pktc->req_queue
    link_t* l = NULL;
    pktc_t* pktc = &pn->pktc;
    while((l = sc_queue_pop(&pktc->req_queue))) {
      pktc_req_t* req = structof(l, pktc_req_t, link);
      pktc_post_io(pktc, req);
    }
    // destroy pktc socket
    dlink_for(&pktc->sk_list, p) {
      pktc_sk_t* s = structof(p, pktc_sk_t, list_link);
      rk_info("sock destroy: sock=%p, connection=%s", s, T2S(sock_fd, s->fd));
      sock_destroy((sock_t*)s);
    }
  }
}

typedef struct pn_resp_ctx_t
{
  pn_t* pn;
  void* req_handle;
  uint64_t sock_id;
  uint64_t pkt_id;
  char reserve[sizeof(pkts_req_t)];
} pn_resp_ctx_t;

static pn_resp_ctx_t* create_resp_ctx(pn_t* pn, void* req_handle, uint64_t sock_id, uint64_t pkt_id)
{
  int64_t gid = pn->tid;
  unused(gid);
  pn_resp_ctx_t* ctx = (typeof(ctx))fifo_alloc(&pn->server_ctx_alloc, sizeof(*ctx));
  if (ctx) {
    ctx->pn = pn;
    ctx->req_handle = req_handle;
    ctx->sock_id = sock_id;
    ctx->pkt_id = pkt_id;
  }
  return ctx;
}

static int pn_pkts_handle_func(pkts_t* pkts, void* req_handle, const char* b, int64_t s, uint64_t chid)
{
  int err = 0;
  uint64_t pkt_id = eh_packet_id(b);
  pn_t* pn = structof(pkts, pn_t, pkts);
  pn_resp_ctx_t* ctx = create_resp_ctx(pn, req_handle, chid, pkt_id);
  if (NULL == ctx) {
    rk_info("create_resp_ctx failed, errno = %d", errno);
  } else {
    PNIO_DELAY_WARN(STAT_TIME_GUARD(eloop_server_process_count, eloop_server_process_time));
    err = pn->serve_cb(pn->gid, b, s, (uint64_t)ctx);
  }
  return err;
}

typedef struct pn_resp_t
{
  pn_resp_ctx_t* ctx;
  pkts_req_t req;
  easy_head_t head;
} pn_resp_t;

static void pn_pkts_flush_cb_func(pkts_req_t* req)
{
  pn_resp_t* resp = structof(req, pn_resp_t, req);
  if ((uint64_t)resp->ctx->reserve == (uint64_t)resp) {
    fifo_free(resp->ctx);
  } else {
    fifo_free(resp->ctx);
    cfifo_free(resp);
  }
}

static void pn_pkts_flush_cb_error_func(pkts_req_t* req)
{
  pn_resp_ctx_t* ctx = (typeof(ctx))structof(req, pn_resp_ctx_t, reserve);
  fifo_free(ctx);
}

PN_API int pn_resp(uint64_t req_id, const char* buf, int64_t sz, int64_t resp_expired_abs_us)
{
  pn_resp_ctx_t* ctx = (typeof(ctx))req_id;
  pn_resp_t* resp = NULL;
  if (sizeof(pn_resp_t) + sz <= sizeof(ctx->reserve)) {
    resp = (typeof(resp))(ctx->reserve);
  } else {
    resp = (typeof(resp))cfifo_alloc(&ctx->pn->server_resp_alloc, sizeof(*resp) + sz);
  }
  pkts_req_t* r = NULL;
  if (NULL != resp) {
    r = &resp->req;
    resp->ctx = ctx;
    r->errcode = 0;
    r->flush_cb = pn_pkts_flush_cb_func;
    r->sock_id = ctx->sock_id;
    r->categ_id = 0;
    r->expire_us = resp_expired_abs_us;
    eh_copy_msg(&r->msg, ctx->pkt_id, buf, sz);
  } else {
    r = (typeof(r))(ctx->reserve);
    r->errcode = ENOMEM;
    r->flush_cb = pn_pkts_flush_cb_error_func;
    r->sock_id = ctx->sock_id;
    r->categ_id = 0;
    r->expire_us = resp_expired_abs_us;
  }
  pkts_t* pkts = &ctx->pn->pkts;
  return pkts_resp(pkts, r);
}

PN_API int pn_get_peer(uint64_t req_id, struct sockaddr_storage* addr) {
  int err = 0;
  pn_resp_ctx_t* ctx = (typeof(ctx))req_id;
  if (unlikely(NULL == ctx || NULL == addr)) {
    err = -EINVAL;
    rk_warn("invalid arguments, req_id=%p", ctx);
  } else {
    pkts_t* pkts = &ctx->pn->pkts;
    pkts_sk_t* sock = (typeof(sock))idm_get(&pkts->sk_map, ctx->sock_id);
    if (unlikely(NULL == sock)) {
      err = -EINVAL;
      rk_warn("idm_get sock failed, sock_id=%lx", ctx->sock_id);
    } else {
      make_sockaddr(addr, sock->peer);
    }
  }
  return err;
}

PN_API int pn_ratelimit(int grp_id, int64_t value) {
  int err = 0;
  pn_grp_t* pn_grp = locate_grp(grp_id);
  if (NULL == pn_grp || value < 0) {
    err = -EINVAL;
  } else {
    rk_info("set ratelimit as %ld bytes/s, grp_id=%d", value, grp_id);
    STORE(&pn_grp->rx_bw, value);
  }
  return err;
}

PN_API int64_t pn_get_ratelimit(int grp_id) {
  int64_t bytes = -1;
  pn_grp_t* pn_grp = locate_grp(grp_id);
  if (pn_grp) {
    bytes = LOAD(&pn_grp->rx_bw);
  }
  return bytes;
}

PN_API uint64_t pn_get_rxbytes(int grp_id) {
  uint64_t bytes = 0;
  pn_grp_t* grp = locate_grp(grp_id);
  if (NULL == grp) {
    rk_warn("group not exists: %d", grp_id);
  } else {
    bytes = LOAD(&grp->rx_bytes);
  }
  return bytes;
}

PN_API int pn_terminate_pkt(uint64_t gtid, uint32_t pkt_id) {
  int err = 0;
  pn_grp_t* pgrp = locate_grp(gtid>>32);
  if (NULL == pgrp) {
    err = EINVAL;
  } else {
    pn_t* pn = get_pn_for_send(pgrp, gtid & 0xffffffff);
    pktc_req_t* r = pn_create_cmd_req(pn, PN_CMD_TERMINATE_PKT, pkt_id);
    if (NULL == r) {
      err = ENOMEM;
      rk_warn("create cmd req failed, gtid=0x%lx, pkt_id=%u", gtid, pkt_id);
    } else {
      err = pktc_post(&pn->pktc, r);
    }
  }
  return err;
}

int dispatch_accept_fd_to_certain_group(int fd, uint64_t gid)
{
  int ret = 0;
  if (UINT64_MAX == gid) {
    rk_info("dispatch fd to oblistener, fd:%d", fd);
    ret = DISPATCH_EXTERNAL(fd);
  } else {
    uint32_t group_id = gid >> 32;
    uint32_t thread_idx = gid & ((1ULL<<32) - 1);
    rk_info("dispatch fd to certain group, fd:%d, gid:0x%lx", fd, gid);
    ret = dispatch_fd_to(fd, group_id, thread_idx);
  }
  return ret;
}

PN_API int pn_get_fd(uint64_t req_id)
{
  int fd = -1;
  pn_resp_ctx_t* ctx = (typeof(ctx))req_id;
  if (unlikely(NULL == ctx)) {
    rk_warn("invalid arguments, req_id=%p", ctx);
  } else {
    pkts_t* pkts = &ctx->pn->pkts;
    pkts_sk_t* sock = (typeof(sock))idm_get(&pkts->sk_map, ctx->sock_id);
    fd = sock->fd;
  }
  return fd;
}

PN_API int64_t pn_get_pkt_id(uint64_t req_id)
{
  int64_t pkt_id = -1;
  pn_resp_ctx_t* ctx = (typeof(ctx))req_id;
  if (unlikely(NULL == ctx)) {
    rk_warn("invalid arguments, req_id=%p", ctx);
  } else {
    pkt_id = ctx->pkt_id;
  }
  return pkt_id;
}

void pn_print_diag_info(pn_comm_t* pn_comm) {
  pn_t* pn = (pn_t*)pn_comm;
  int64_t client_cnt = 0;
  int64_t server_cnt = 0;
  // print socket diag info
  dlink_for(&pn->pktc.sk_list, p) {
    pktc_sk_t* s = structof(p, pktc_sk_t, list_link);
    rk_info("client:%p_%s_%s_%d_%ld_%d, write_queue=%lu/%lu, write=%lu/%lu, read=%lu/%lu, doing=%lu, done=%lu, write_time=%lu, read_time=%lu, process_time=%lu",
              s, T2S(addr, s->sk_diag_info.local_addr), T2S(addr, s->dest), s->fd, s->sk_diag_info.establish_time, s->conn_ok,
              s->wq.cnt, s->wq.sz,
              s->sk_diag_info.write_cnt, s->sk_diag_info.write_size,
              s->sk_diag_info.read_cnt, s->sk_diag_info.read_size,
              s->sk_diag_info.doing_cnt, s->sk_diag_info.done_cnt,
              s->sk_diag_info.write_wait_time, s->sk_diag_info.read_time, s->sk_diag_info.read_process_time);
    client_cnt++;
  }
  if (pn->pkts.sk_list.next != NULL) {
    dlink_for(&pn->pkts.sk_list, p) {
      pkts_sk_t* s = structof(p, pkts_sk_t, list_link);
      rk_info("server:%p_%s_%d_%ld, write_queue=%lu/%lu, write=%lu/%lu, read=%lu/%lu, doing=%lu, done=%lu, write_time=%lu, read_time=%lu, process_time=%lu",
                s, T2S(addr, s->peer), s->fd, s->sk_diag_info.establish_time,
                s->wq.cnt, s->wq.sz,
                s->sk_diag_info.write_cnt, s->sk_diag_info.write_size,
                s->sk_diag_info.read_cnt, s->sk_diag_info.read_size,
                s->sk_diag_info.doing_cnt, s->sk_diag_info.done_cnt,
                s->sk_diag_info.write_wait_time, s->sk_diag_info.read_time, s->sk_diag_info.read_process_time);
      server_cnt++;
    }
  }
  // print pnio diag info
  rk_info("client_send:%lu/%lu, client_queue_time=%lu, cnt=%ld, server_send:%lu/%lu, server_queue_time=%lu, cnt=%ld",
            pn->pktc.diag_info.send_cnt, pn->pktc.diag_info.send_size, pn->pktc.diag_info.sc_queue_time, client_cnt,
            pn->pkts.diag_info.send_cnt, pn->pkts.diag_info.send_size, pn->pkts.diag_info.sc_queue_time, server_cnt);
}
