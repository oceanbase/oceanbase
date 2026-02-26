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
#define MAX_PN_PER_GRP 128
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
  int listen_id;
  int gid;
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
int64_t pnio_read_bytes;
int64_t pnio_write_bytes;

void reset_pnio_statistics(int64_t *read_bytes, int64_t *write_bytes)
{
  *read_bytes = ATOMIC_SET(&pnio_read_bytes, 0);
  *write_bytes = ATOMIC_SET(&pnio_write_bytes, 0);
}

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
  int thread_count = 0;
  if (NULL == grp || (thread_count = LOAD(&grp->count)) == 0) {
    err = -ENOENT;
  } else {
    pn_t* pn = grp->pn_array[tid % thread_count];
    int wbytes = 0;
    listenfd_dispatch_t listen_fd = {
      .fd = fd,
      .tid = (int)tid,
      .sock_ptr = NULL
    };
    while((wbytes = write(pn->accept_qfd, (const char*)&listen_fd, sizeof(listen_fd))) < 0
          && EINTR == errno);
    if (wbytes != sizeof(listen_fd)) {
      err = -EIO;
    }
    rk_info("fd:%d, gid:0x%ux, tid:%d, actual_tid:%d, err=%d", fd, gid, tid, tid % thread_count, err);
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
  } else {
    pn_grp->gid = gid;
    pn_grp->listen_id = listen_id;
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
    pn_grp->config_count = count;
    pn_grp->created_count = count;
    ret = pn_grp->count;
  }
  return ret;
}

PN_API int pn_update_thread_count(int gid, int thread_count)
{
  int err = 0;
  int count = 0;
  pn_grp_t* pn_grp = locate_grp(gid);
  if (pn_grp == NULL) {
    err = -EINVAL;
    rk_error("group id not found, gid=%d", gid);
  } else if (thread_count > MAX_PN_PER_GRP || thread_count <= 0) {
    err = -EINVAL;
    rk_error("thread_count invalid, gid=%d, thread_count=%d, MAX_PN_PER_GRP=%d", gid, thread_count, MAX_PN_PER_GRP);
  } else {
    int cur_thread_count = LOAD(&pn_grp->count);
    if (thread_count < cur_thread_count) {
      rk_warn("The threads will not be deleted after being decreased, but there won't be any new RPC requests sent by these threads."
               "gid=%d, thread_count=%d, cur=%d", gid, thread_count, cur_thread_count);
    }
    STORE(&pn_grp->config_count, thread_count);
  }
  return err;
}

// not thread safety
int pn_set_thread_count(int listen_id, int gid, int thread_count)
{
  int err = 0;
  int count = 0;
  pn_grp_t* pn_grp = locate_grp(gid);
  if (pn_grp == NULL) {
    err = -EINVAL;
    rk_error("group id not found, gid=%d", gid);
  } else {
    int old_count = LOAD(&pn_grp->count);
    if (thread_count < old_count) {
      // decrease thread, only set pn_grp->count and not to delete thread
      STORE(&pn_grp->count, thread_count);
      rk_info("gid=%d, old_count:%d, set_count:%d", gid, old_count, thread_count);
    } else if (thread_count > old_count) {
      // increase thread
      int new_created_count = 0;
      int count = old_count;
      for (; count < thread_count && err == 0; count++) {
        pn_t* pn = pn_grp->pn_array[count];
        if (NULL != pn) {
          rk_info("pn_thread has been created, gid=%d, idx=%d, pn=%p", gid, count, pn);
        } else {
          pn = pn_create(listen_id, gid, count);
          if (NULL == pn) {
            err = ENOMEM;
            rk_warn("pn_create failed, might be memory exhaustion, gid=%d, count=%d", gid, count);
          } else if (0 != (err = ob_pthread_create(&pn->pd, pn_thread_func, pn))) {
            pn_destroy(pn);
          } else {
            pn_grp->pn_array[count] = pn;
            new_created_count += 1;
          }
        }
      }
      STORE(&pn_grp->count, count);
      if (new_created_count > 0) {
        AAF(&pn_grp->created_count, new_created_count);
      }
      rk_info("gid=%d, old_count:%d, set_count:%d, cur_count:%d, new_created_count:%d, err:%d",
              gid, old_count, thread_count, count, new_created_count, err);
    }
  }
  return err;
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
  pn_client_req_t* pn_req = (typeof(pn_req))req - 1;
  struct pn_pktc_cb_t* pn_cb = (typeof(pn_cb))cfifo_alloc(&pn->client_cb_alloc, sizeof(*pn_cb));
  if (unlikely(NULL == pn_cb)) {
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

PN_API void* pn_send_alloc(uint64_t gtid, int64_t sz)
{
  pn_grp_t* pgrp = locate_grp(gtid>>32);
  pn_t* pn = get_pn_for_send(pgrp, gtid & 0xffffffff);
  pn_client_req_t* pn_req = (typeof(pn_req))cfifo_alloc(&pn->client_req_alloc, sizeof(*pn_req) + sz);
  void* p = NULL;
  if (unlikely(NULL != pn_req)) {
    p = (void*)(pn_req + 1);
  }
  return p;
}

PN_API void pn_send_free(void* p)
{
  if (NULL != p) {
    pn_client_req_t* pn_req =  (pn_client_req_t*)p - 1;
    cfifo_free(pn_req);
  }
}

PN_API void pn_stop(uint64_t gid)
{
  pn_grp_t *pgrp = locate_grp(gid);
  if (pgrp != NULL) {
    int created_count = ATOMIC_LOAD(&pgrp->created_count);
    for (int tid = 0; tid < created_count; tid++) {
      pn_t *pn = pgrp->pn_array[tid];
      if (NULL != pn) {
        rk_info("mark stop, gid=%lu, tid=%d", gid, tid);
        ATOMIC_STORE(&pn->is_stop_, true);
      }
    }
  }
}

PN_API void pn_wait(uint64_t gid)
{
  pn_grp_t *pgrp = locate_grp(gid);
  if (pgrp != NULL) {
    int created_count = ATOMIC_LOAD(&pgrp->created_count);
    for (int tid = 0; tid < created_count; tid++) {
      pn_t *pn = pgrp->pn_array[tid];
      if (NULL != pn && NULL != pn->pd) {
        rk_info("wait pn thread, gid=%lu, tid=%d, pt=%p", gid, tid, pn->pd);
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
      char sock_fd_buf[PNIO_NIO_FD_ADDR_LEN] = {'\0'};
      rk_info("sock destroy: sock=%p, connection=%s", s, sock_fd_str(s->fd, sock_fd_buf, sizeof(sock_fd_buf)));
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
  void* resp_ptr;
  time_dlink_t time_dlink;
  uint64_t trace_id[4];
  int64_t tenant_id;
  int32_t trace_point;
  int16_t pcode;
  int16_t timeout_warn_count;
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
    ctx->resp_ptr = NULL;
    time_dlink_init(&ctx->time_dlink);
    ctx->tenant_id = 0;
    ctx->trace_point = 0;
    ctx->pcode = 0;
    ctx->timeout_warn_count = 0;
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
    tw_regist_timeout(&pkts->resp_ctx_hold, &ctx->time_dlink, HOLD_BY_UP_LAYER_TIMEOUT);
    err = pn->serve_cb(pn->gid, b, s, (uint64_t)ctx);
  }
  return err;
}

typedef struct pn_resp_t
{
  pn_resp_ctx_t* ctx;
  void* resp_ptr; // used for free response buffer
  pkts_req_t req;
  easy_head_t head;
} pn_resp_t;

static void pn_pkts_flush_cb_func(pkts_req_t* req)
{
  pn_resp_t* resp = structof(req, pn_resp_t, req);
  if (likely(resp->resp_ptr)) {
    cfifo_free(resp->resp_ptr);
  }
}

static void pn_pkts_flush_cb_error_func(pkts_req_t* req)
{
  pn_resp_ctx_t* ctx = (typeof(ctx))structof(req, pn_resp_ctx_t, reserve);
  dlink_delete(&ctx->time_dlink.dlink);
  fifo_free(ctx);
}

pkts_t* locate_pn_pkts(int gid, int tid)
{
  pkts_t* pkts = NULL;
  pn_grp_t* pgrp = locate_grp(gid);
  int created_count = 0;
  if (NULL != pgrp && tid >= 0 && tid < (created_count = ATOMIC_LOAD(&pgrp->created_count))) {
    pn_t* pn = pgrp->pn_array[tid];
    pkts = &pn->pkts;
  } else {
    int err = -EINVAL;
    rk_error("locate_pkts failed, gid=%d, tid=%d, created_count=%d", gid, tid, created_count);
  }
  return pkts;
}

void pn_release_ctx(pkts_req_t* req) {
  pn_resp_t* resp = structof(req, pn_resp_t, req);
  pn_resp_ctx_t* ctx = resp->ctx;
  // When socket relocating and the resp is post to queue seocondly,
  // ctx is NULL and no need to release ctx again.
  if (NULL != ctx) {
    dlink_delete(&ctx->time_dlink.dlink);
    resp->resp_ptr = ctx->resp_ptr;
    resp->ctx = NULL;
    fifo_free(ctx);
  }
}

void hold_by_uplayer_timeout(time_wheel_t* tw, dlink_t* l) {
  time_dlink_t* td = structof(l, time_dlink_t, dlink);
  pn_resp_ctx_t* ctx = structof(td, pn_resp_ctx_t, time_dlink);
  char buf[72] = {'\0'};
  ctx->timeout_warn_count ++;
  const int16_t trace_point_in_tenant_queue = 3;
  const int realarm_timeout_us = 2000000;
  int wait_seconds = 9 + (ctx->timeout_warn_count - 1) * 2;
  if (ctx->trace_point == trace_point_in_tenant_queue) {
    // Reduce the frequency of printing logs if the backlog of queues leads to holding for too much time
    if ((ctx->pkt_id & 0xff) == 0) {
      rk_warn("reqeust hold by upper-layer for too much time. "
              "req=%p, pcode=%d, tenant_id=%ld, trace_point=%d, hold_time=%dus, trace_id(%s)",
              ctx, ctx->pcode, ctx->tenant_id, ctx->trace_point, HOLD_BY_UP_LAYER_TIMEOUT,
              trace_id_to_str_c(&ctx->trace_id[0], buf, sizeof(buf)));
    }
  } else {
    rk_warn("reqeust hold by upper-layer for too much time. "
            "req=%p, pcode=%d, tenant_id=%ld, trace_point=%d, hold_time=%dus, trace_id(%s)",
            ctx, ctx->pcode, ctx->tenant_id, ctx->trace_point,
            HOLD_BY_UP_LAYER_TIMEOUT + (ctx->timeout_warn_count - 1) * realarm_timeout_us,
            trace_id_to_str_c(&ctx->trace_id[0], buf, sizeof(buf)));
    if (ctx->timeout_warn_count < 3) {
      // print hold by upper-layer log up to 3 times
      tw_regist_timeout(tw, td, realarm_timeout_us);
    }
  }
}

PN_API void* pn_resp_pre_alloc(uint64_t req_id, int64_t sz)
{
  void* p = NULL;
  pn_resp_ctx_t* ctx = (typeof(ctx))req_id;
  if (likely(ctx)) {
    if (unlikely(ctx->resp_ptr != NULL)) {
      int err = PNIO_ERROR;
      rk_error("pn_resp_pre_alloc might has been executed, it is unexpected, ctx=%p, resp_ptr=%p", ctx, ctx->resp_ptr);
      cfifo_free(ctx->resp_ptr);
    }
    p = cfifo_alloc(&ctx->pn->server_resp_alloc, sz);
    ctx->resp_ptr = p;
  }
  return p;
}

PN_API int pn_resp(uint64_t req_id, const char* buf, int64_t hdr_sz, int64_t payload_sz, int64_t resp_expired_abs_us)
{
  pn_resp_ctx_t* ctx = (typeof(ctx))req_id;
  pn_resp_t* resp = NULL;
  if (unlikely(0 == hdr_sz)) { // response null or response error
    if (ctx->resp_ptr) {
      cfifo_free(ctx->resp_ptr);
    }
    ctx->resp_ptr = cfifo_alloc(&ctx->pn->server_resp_alloc, sizeof(*resp) + payload_sz);
    resp = (typeof(resp))ctx->resp_ptr;
  } else {
    assert(hdr_sz >= sizeof(*resp));
    resp = (typeof(resp))(buf + hdr_sz - sizeof(*resp));
  }
  pkts_req_t* r = NULL;
  if (NULL != resp) {
    r = &resp->req;
    resp->ctx = ctx;
    resp->resp_ptr = NULL;
    r->errcode = 0;
    r->flush_cb = pn_pkts_flush_cb_func;
    r->sock_id = ctx->sock_id;
    r->categ_id = 0;
    r->expire_us = resp_expired_abs_us;
    eh_copy_msg(&r->msg, ctx->pkt_id, buf + hdr_sz, payload_sz);
  } else {
    rk_warn("allocate memory for pn_resp_t failed");
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

PN_API inline void pn_set_trace_point(uint64_t req_id, int32_t trace_point) {
  pn_resp_ctx_t* ctx = (typeof(ctx))req_id;
  ctx->trace_point = trace_point;
}

PN_API inline void pn_set_trace_info(uint64_t req_id, int64_t tenant_id, int16_t pocde, const uint64_t* trace_id) {
  pn_resp_ctx_t* ctx = (typeof(ctx))req_id;
  ctx->tenant_id = tenant_id;
  ctx->pcode = pocde;
  ctx->trace_id[0] = trace_id[0];
  ctx->trace_id[1] = trace_id[1];
  ctx->trace_id[2] = trace_id[2];
  ctx->trace_id[3] = trace_id[3];
}

void pn_print_diag_info(pn_comm_t* pn_comm) {
  pn_t* pn = (pn_t*)pn_comm;
  int64_t client_cnt = 0;
  int64_t server_cnt = 0;
  // print socket diag info
  char sock_buf[PNIO_NIO_SOCK_ADDR_LEN] = {'\0'};
  dlink_for(&pn->pktc.sk_list, p) {
    pktc_sk_t* s = structof(p, pktc_sk_t, list_link);
    rk_info("client:%p_%s, write_queue=%lu/%lu, write=%lu/%lu, read=%lu/%lu, doing=%lu, done=%lu, write_time=%lu, read_time=%lu, process_time=%lu",
              s, my_sock_t_str(s, sock_buf, sizeof(sock_buf)),
              s->wq.cnt, s->wq.sz,
              s->sk_diag_info.write_cnt, s->sk_diag_info.write_size,
              s->sk_diag_info.read_cnt, s->sk_diag_info.read_size,
              s->sk_diag_info.doing_cnt, s->sk_diag_info.done_cnt,
              s->sk_diag_info.write_wait_time, s->sk_diag_info.read_time, s->sk_diag_info.read_process_time);
    client_cnt++;
  }
  int ttid = get_current_pnio()->tid;
  if (pn->pkts.sk_list.next != NULL) {
    dlink_for(&pn->pkts.sk_list, p) {
      pkts_sk_t* s = structof(p, pkts_sk_t, list_link);
      char peer_addr_buf[PNIO_NIO_ADDR_LEN] = {'\0'};
      rk_info("[%d]server:%p_%s, write_queue=%lu/%lu, write=%lu/%lu, read=%lu/%lu, doing=%lu, done=%lu, write_time=%lu, read_time=%lu, process_time=%lu, "
                "relocate_status:%d",
                ttid, s, my_sock_t_str(s, sock_buf, sizeof(sock_buf)),
                s->wq.cnt, s->wq.sz,
                s->sk_diag_info.write_cnt, s->sk_diag_info.write_size,
                s->sk_diag_info.read_cnt, s->sk_diag_info.read_size,
                s->sk_diag_info.doing_cnt, s->sk_diag_info.done_cnt,
                s->sk_diag_info.write_wait_time, s->sk_diag_info.read_time, s->sk_diag_info.read_process_time,
                s->relocate_status);
      server_cnt++;
    }
  }
  // print pnio diag info
  rk_info("client_send:%lu/%lu, client_queue_time=%lu, cnt=%ld, server_send:%lu/%lu, server_queue_time=%lu, cnt=%ld",
            pn->pktc.diag_info.send_cnt, pn->pktc.diag_info.send_size, pn->pktc.diag_info.sc_queue_time, client_cnt,
            pn->pkts.diag_info.send_cnt, pn->pkts.diag_info.send_size, pn->pkts.diag_info.sc_queue_time, server_cnt);
  int64_t current_time_us = rk_get_us();
  if (current_time_us - pn->pktc.cb_tw.finished_us > 10 * 1000000) {
    int err = -EIO;
    rk_error("timer fd has been blocked for too long, check the timerfd has been closed unexpectedly or os error,"
             "finished_us=%ld, timer_fd=%d, err=%d",
             pn->pktc.cb_tw.finished_us, pn->pktc.cb_timerfd.fd, err);
  }
}

void pn_check_thread_count() {
  // TODO:@fangwu.lcc assuming that there are only two pn_groups in the observer process,
  // fix here when supporting more pn groups
  const int default_pn_group = 1;
  const int ratelimit_pn_group = 2;
  for (int i = default_pn_group; i <= ratelimit_pn_group; i++) {
    pn_grp_t* pn_grp = locate_grp(i);
    if (pn_grp != NULL) {
      int cur_count = LOAD(&pn_grp->count);
      int config_count = LOAD(&pn_grp->config_count);
      if ( cur_count != config_count) {
        int err = pn_set_thread_count(pn_grp->listen_id, pn_grp->gid, config_count);
        if (err != 0) {
          rk_error("pn_set_thread_count failed, err=%d", err);
        }
      }
    }
  }
}

static int relocate_sk_and_wait(pkts_sk_t* s, uint32_t gid)
{
  int err = 0;
  pn_grp_t* grp = locate_grp(gid);
  int fd = s->fd;
  int tid = s->tid;
  if (NULL == grp) {
    err = -ENOENT;
    rk_error("locate pn failed, gid=%d,tid=%d", gid, tid);
  } else {
    pn_t* pn = grp->pn_array[tid];
    int wbytes = 0;
    listenfd_dispatch_t listen_fd = {
      .fd = fd,
      .tid = (int)tid,
      .sock_ptr = s
    };
    while((wbytes = write(pn->accept_qfd, (const char*)&listen_fd, sizeof(listen_fd))) < 0
          && EINTR == errno);
    if (wbytes != sizeof(listen_fd)) {
      err = -EIO;
      rk_error("write pipe fd faild, errno=%d", errno);
    }
    // sync wait dst thread has received and processed the dispatch task;
    while(ATOMIC_LOAD(&s->relocate_status) == SOCK_NORMAL) {
      rk_futex_wait(&s->relocate_status, SOCK_NORMAL, NULL);
    }
  }
  char sock_fd_buf[PNIO_NIO_FD_ADDR_LEN] = {'\0'};
  rk_info("[socket_relocation] dispatch connection, s=%p,%s, gid=%d, tid=%d, status=%d, relocate_sock_id=%lu",
          s, sock_fd_str(fd, sock_fd_buf, sizeof(sock_fd_buf)), gid, tid, s->relocate_status, s->relocate_sock_id);
  return err;
}

static int pkts_sk_relocate(pkts_sf_t* sf, pkts_sk_t* s, uint32_t gid) {
  int err = 0;
  // similar to sock_destroy but not free memory
  err = epoll_ctl(s->ep_fd, EPOLL_CTL_DEL, s->fd, NULL);
  if (0 != err) {
    // the fd might be closed, unneed to relocate
    rk_warn("[socket_relocation] epoll_ctl delete fd faild, s=%p, s->fd=%d, errno=%d", s, s->fd, errno);
  } else if ((err = relocate_sk_and_wait(s, gid)) != 0) {
    rk_error("relocate_sk_and_wait failed, try to add the socket again, err=%d", err);
    int tmp_err = 0;
    struct epoll_event event;
    uint32_t flag = EPOLLIN | EPOLLOUT | EPOLLERR | EPOLLET;
    if (0 != (tmp_err = epoll_ctl(s->ep_fd, EPOLL_CTL_ADD, s->fd, __make_epoll_event(&event, flag, s)))) {
      rk_error("eloop_regist failed, tmp_err=%d, s=%p, fd=%d", tmp_err, s, s->fd);
    }
  } else {
    rk_info("[socket_relocation] s=%p, processing_cnt=%ld", s, s->processing_cnt);
    dlink_delete(&s->ready_link);
    pkts_t* pkts = structof(sf, pkts_t, sf);
    // not to delete from sk_map until there are no rpc requests being processing
    if (0 == s->processing_cnt) {
      idm_del(&pkts->sk_map, s->id);
    }
    dlink_delete(&s->list_link);
    dlink_delete(&s->rl_ready_link);
  }
  return err;
}

void pkts_sk_rebalance() {
  pn_t* pn = (pn_t*)get_current_pnio();
  pn_grp_comm_t* pn_grp = pn->pn_grp;
  int running_thread_conut = ATOMIC_LOAD(&pn_grp->count);
  int tid = pn->tid;
  if (pn->pkts.sk_list.next != NULL) { // the pkts.sk_list is NULL if the pn is only used as client end
    dlink_for(&pn->pkts.sk_list, p) {
      pkts_sk_t *sk = structof(p, pkts_sk_t, list_link);
      if (sk->tid != tid && sk->tid < running_thread_conut && SOCK_NORMAL == sk->relocate_status) {
        // the socket is imbalanced and its target thread has been created
        IGNORE_RETURN pkts_sk_relocate(&pn->pkts.sf, sk, pn->gid);
      }
    }
  }
}
