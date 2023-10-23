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

typedef struct pktc_msg_t {
  int64_t sz;
  char* payload;
} pktc_msg_t;
static int64_t pktc_decode(char* b, int64_t s) { return eh_decode(b, s); }
static uint64_t pktc_get_id(pktc_msg_t* m) { return eh_packet_id(m->payload); }

static int pktc_sk_read(void** b, pktc_sk_t* s, int64_t sz, int64_t* read_bytes) {
  return sk_read_with_ib(b, (sock_t*)s, &s->ib, sz, read_bytes);
}

static void pktc_flush_cb(pktc_t* io, pktc_req_t* req) {
  unused(io);
  PNIO_DELAY_WARN(delay_warn("pktc_flush_cb", req->ctime_us, FLUSH_DELAY_WARN_US));
  req->flush_cb(req);
}

static int pktc_wq_flush(sock_t* s, write_queue_t* wq, dlink_t** old_head) {
  return wq_flush(s, wq, old_head);
}

#include "pktc_resp.h"

#define tns(x) pktc ## x
#include "nio-tpl-ns.h"
#include "write_queue.t.h"
#include "decode.t.h"
#include "handle_io.t.h"
#include "nio-tpl-ns.h"

#include "pktc_sk_factory.h"
#include "pktc_post.h"

int64_t pktc_init(pktc_t* io, eloop_t* ep, uint64_t dispatch_id) {
  int err = 0;
  io->ep = ep;
  io->dispatch_id = dispatch_id;
  ef(err = pktc_sf_init(&io->sf));
  ef(err = evfd_init(io->ep, &io->evfd, (handle_event_t)pktc_evfd_cb));
  sc_queue_init(&io->req_queue);
  ef(err = timerfd_init_tw(io->ep, &io->cb_timerfd));
  tw_init(&io->cb_tw, pktc_resp_cb_on_timeout);
  hash_init(&io->sk_map, arrlen(io->sk_table));
  hash_init(&io->cb_map, arrlen(io->cb_table));
  dlink_init(&io->sk_list);
  rk_info("pktc init succ");
  el();
  return err;
}
