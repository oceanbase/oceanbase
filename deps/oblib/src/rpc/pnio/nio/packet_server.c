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

typedef struct pkts_msg_t {
  int64_t sz;
  char* payload;
} pkts_msg_t;

static int64_t pkts_decode(char* b, int64_t s) { return eh_decode(b, s);}

void pkts_flush_cb(pkts_t* io, pkts_req_t* req) {
  PNIO_DELAY_WARN(delay_warn("pkts_flush_cb", req->ctime_us, FLUSH_DELAY_WARN_US));
  req->flush_cb(req);
}

static int pkts_sk_read(void** b, pkts_sk_t* s, int64_t sz, int64_t* avail_bytes) {
  return sk_read_with_ib(b, (sock_t*)s, &s->ib, sz, avail_bytes);
}

static int pkts_sk_handle_msg(pkts_sk_t* s, pkts_msg_t* msg) {
  pkts_t* pkts = structof(s->fty, pkts_t, sf);
  int ret = pkts->on_req(pkts, s->ib.b, msg->payload, msg->sz, s->id);
  ib_consumed(&s->ib, msg->sz);
  return ret;
}

static int pkts_wq_flush(sock_t* s, write_queue_t* wq, dlink_t** old_head) {
  // delete response req that has reached expired time
  if (PNIO_REACH_TIME_INTERVAL(10*1000)) {
    int64_t cur_time = rk_get_us();
    dlink_for(&wq->queue.head, p) {
      pkts_req_t* req = structof(p, pkts_req_t, link);
      if (req->expire_us > 0 && cur_time >= req->expire_us) {
        if (PNIO_OK == wq_delete(wq, p)) {
          rk_warn("rpc resp is expired, expire_us=%ld, sock_id=%ld", req->expire_us, req->sock_id);
          pkts_flush_cb(NULL, req);
        }
      }
    }
  }
  return wq_flush(s, wq, old_head);
}

#define tns(x) pkts ## x
#include "nio-tpl-ns.h"
#include "write_queue.t.h"
#include "decode.t.h"
#include "handle_io.t.h"
#include "nio-tpl-ns.h"

#include "pkts_sk_factory.h"
#include "pkts_post.h"

int pkts_init(pkts_t* io, eloop_t* ep, pkts_cfg_t* cfg) {
  int err = 0;
  int lfd = -1;
  io->ep = ep;
  ef(err = pkts_sf_init(&io->sf, cfg));
  sc_queue_init(&io->req_queue);
  ef(err = evfd_init(io->ep, &io->evfd, (handle_event_t)pkts_evfd_cb));
  lfd = cfg->accept_qfd >= 0 ?cfg->accept_qfd: listen_create(cfg->addr);
  ef(err = listenfd_init(io->ep, &io->listenfd, (sf_t*)&io->sf, lfd));
  rk_info("pkts listen at %s", T2S(addr, cfg->addr));
  idm_init(&io->sk_map, arrlen(io->sk_table));
  io->on_req = cfg->handle_func;
  el();
  return err;
}
