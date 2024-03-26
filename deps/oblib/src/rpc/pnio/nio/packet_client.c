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
  int64_t ctime_us;
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

void* sk_key_func(link_t* link)
{
  pktc_sk_t *sk = structof(link, pktc_sk_t, hash);
  return &sk->dest;
}

uint64_t sk_hash_func(void* key)
{
  addr_t* addr = (addr_t*)key;
  uint64_t hash = addr->port;
  if (!addr->is_ipv6) {
    hash += addr->ip;
  } else {
    for (int i = 0; i < sizeof(addr->ipv6)/sizeof(addr->ipv6[0]); i++) {
      hash += addr->ipv6[i];
    }
  }
  return hash;
}

bool sk_equal_func(void* l_key, void* l_right)
{
  addr_t *l = (addr_t*)l_key;
  addr_t *r = (addr_t*)l_right;
  bool eq = true;
  if (l->is_ipv6 != r->is_ipv6) {
    eq = false;
  } else if (!l->is_ipv6) {
    eq = l->ip == r->ip;
  } else {
    for (int i = 0; eq && i < sizeof(l->ipv6)/sizeof(l->ipv6[0]); i++) {
      eq = l->ipv6[i] == r->ipv6[i];
    }
  }
  return eq;
}

void* cb_key_func(link_t* link)
{
  pktc_cb_t *cb = structof(link, pktc_cb_t, hash_link);
  return &cb->id;
}

uint64_t cb_hash_func(void* key)
{
  uint64_t id = *(uint64_t*)key;
  return fasthash64(&id, sizeof(id), 0);
}

bool cb_equal_func(void* l_key, void* l_right)
{
  return *(uint64_t*)l_key == *(uint64_t*)l_right;
}

int64_t pktc_init(pktc_t* io, eloop_t* ep, uint64_t dispatch_id) {
  int err = 0;
  io->ep = ep;
  io->dispatch_id = dispatch_id;
  ef(err = pktc_sf_init(&io->sf));
  ef(err = evfd_init(io->ep, &io->evfd, (handle_event_t)pktc_evfd_cb));
  sc_queue_init(&io->req_queue);
  ef(err = timerfd_init_tw(io->ep, &io->cb_timerfd));
  tw_init(&io->cb_tw, pktc_resp_cb_on_timeout);
  hash_init(&io->sk_map, arrlen(io->sk_table), sk_key_func, sk_hash_func, sk_equal_func);
  hash_init(&io->cb_map, arrlen(io->cb_table), cb_key_func, cb_hash_func, cb_equal_func);
  dlink_init(&io->sk_list);
  rk_info("pktc init succ");
  el();
  return err;
}
