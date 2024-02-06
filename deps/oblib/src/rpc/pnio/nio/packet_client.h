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

typedef struct pktc_req_t pktc_req_t;
typedef struct pktc_cb_t pktc_cb_t;
typedef struct pktc_t pktc_t;
typedef void (*pktc_flush_cb_func_t)(pktc_req_t* req);
typedef void (*pktc_resp_cb_func_t)(pktc_cb_t* cb, const char* resp, int64_t sz);

struct pktc_cb_t {
  dlink_t sk_dlink;
  link_t hash_link;
  uint64_t id;
  dlink_t timer_dlink;
  int64_t expire_us;
  pktc_resp_cb_func_t resp_cb;
  pktc_req_t* req;
  int errcode;
};

struct pktc_req_t {
  struct pktc_sk_t* sk;
  PNIO_DELAY_WARN(int64_t ctime_us);
  pktc_flush_cb_func_t flush_cb;
  pktc_cb_t* resp_cb;
  addr_t dest;
  int64_t categ_id; // ATTENTION! Cannot add new structure field from categ_id!
  dlink_t link;
  str_t msg;
};

extern int64_t pktc_init(pktc_t* io, eloop_t* ep, uint64_t dispatch_id);
extern int pktc_post(pktc_t* io, pktc_req_t* req);

typedef struct pktc_sk_t {
  SOCK_COMMON;
  dlink_t list_link;
  struct pktc_t* pc; // for debug
  link_t hash;
  addr_t dest;
  write_queue_t wq;
  ibuffer_t ib;
  dlink_t cb_head;
  int64_t user_keepalive_timeout;
} pktc_sk_t;

typedef struct pktc_sf_t {
  SOCK_FACTORY_COMMON;
} pktc_sf_t;

typedef struct pktc_t {
  eloop_t* ep;
  uint64_t dispatch_id;
  pktc_sf_t sf;
  evfd_t evfd;
  sc_queue_t req_queue;
  timerfd_t cb_timerfd;
  time_wheel_t cb_tw;
  dlink_t sk_list;
  hash_t sk_map;
  link_t sk_table[1024];
  hash_t cb_map;
  link_t cb_table[1<<16];
} pktc_t;
