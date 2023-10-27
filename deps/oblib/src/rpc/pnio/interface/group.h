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

#pragma once
#include <stdint.h>
#include <netinet/in.h>
#ifndef PN_API
#define PN_API
#endif

#ifndef PN_PKTC_ALLOC
#define pn_alloc_pktc_cb(arg, sz) malloc(sz)
#define pn_free_pktc_cb(cb) free(cb)
#define pn_alloc_pktc_req(arg, sz) malloc(sz)
#define pn_free_pktc_req(r) free(r)
#endif

#ifndef PN_PKTS_ALLOC
#define pn_alloc_pkts_ctx(gid, sz) mod_alloc(sz, MOD_PKTS_RESP_CTX)
#define pn_free_pkts_ctx(ctx) mod_free(ctx)
#define pn_alloc_pkts_resp(ctx, sz) mod_alloc(sz, MOD_PKTS_RESP)
#define pn_free_pkts_resp(r) mod_free(r)
#endif

#define RATE_UNLIMITED INT64_MAX

typedef int (*serve_cb_t)(int grp, const char* b, int64_t sz, uint64_t req_id);
typedef int (*client_cb_t)(void* arg, int io_err, const char* b, int64_t sz);

#ifndef RK_CACHE_ALIGNED
#define RK_CACHE_ALIGNED __attribute__((aligned(64)))
#endif
#define PN_GRP_COMM                           \
  int count;                                  \
  int64_t rx_bw RK_CACHE_ALIGNED;             \
  uint64_t rx_bytes RK_CACHE_ALIGNED;         \
  int64_t next_readable_time RK_CACHE_ALIGNED
typedef struct pn_grp_comm_t
{
  PN_GRP_COMM;
} pn_grp_comm_t;

#define PN_COMM                \
  bool is_stop_;               \
  bool has_stopped_;           \
  void *pd;                    \
  int accept_qfd;              \
  int gid;                     \
  int tid;                     \
  int64_t next_readable_time;  \
  pn_grp_comm_t* pn_grp
typedef struct pn_comm_t
{
  PN_COMM;
} pn_comm_t;

PN_API int64_t pn_set_keepalive_timeout(int64_t user_timeout);
PN_API int pn_listen(int port, serve_cb_t cb);
// if listen_id == -1,  act as client only
// make sure grp != 0
PN_API int pn_provision(int listen_id, int grp, int thread_count);
// gid_tid = (gid<<8) | tid
PN_API int pn_send(uint64_t gid_tid, struct sockaddr_in* addr, const char* buf, int64_t sz, int16_t categ_id, int64_t expire_us, client_cb_t cb, void* arg);
PN_API int pn_resp(uint64_t req_id, const char* buf, int64_t sz, int64_t resp_expired_abs_us);
PN_API int pn_get_peer(uint64_t req_id, struct sockaddr_storage* addr);
PN_API int pn_ratelimit(int grp_id, int64_t value);
PN_API int64_t pn_get_ratelimit(int grp_id);
PN_API uint64_t pn_get_rxbytes(int grp_id);
PN_API int dispatch_accept_fd_to_certain_group(int fd, uint64_t gid);
PN_API void pn_stop(uint64_t gid);
PN_API void pn_wait(uint64_t gid);
extern int64_t pnio_keepalive_timeout;
pn_comm_t* get_current_pnio();
void pn_release(pn_comm_t* pn_comm);

#define PNIO_OK                     0
#define PNIO_ERROR                  (-1)
#define PNIO_STOPPED                (-45)
#define PNIO_DISCONNECT             (-46)
#define PNIO_TIMEOUT                (-47)
#define PNIO_CONNECT_FAIL           (-49)
#define PNIO_DISPATCH_ERROR         (-51)
#define PNIO_TIMEOUT_NOT_SENT_OUT       (-54)
#define PNIO_DISCONNECT_NOT_SENT_OUT    (-55)
#define PNIO_LISTEN_ERROR               (-56)

/*
// 启动listen线程和epool线程池, epoll线程池有10个线程
int listen_id = pn_listen(8042, cb);
pn_provision(listen_id, GRP_ID, 10);

// client 发包
pn_send((GRP_ID<<8) + tid, addr, req, sz, get_us() + 1000000, resp_cb, arg);

// server处理
   int serve_cb(int grp, const char* b, int64_t sz, uint64_t req_id) {
       //handle xxx
       pn_resp(req_id, resp, sz);
   }
 */
