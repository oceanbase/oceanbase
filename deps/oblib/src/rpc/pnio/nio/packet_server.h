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

typedef struct pkts_sk_t pkts_sk_t;
typedef struct pkts_req_t pkts_req_t;
typedef struct pkts_t pkts_t;
typedef int (*pkts_handle_func_t)(pkts_t* pkts, void* req_handle, const char* b, int64_t s, uint64_t chid);
typedef void (*pkts_flush_cb_func_t)(pkts_req_t* req);

typedef struct pkts_cfg_t {
  int accept_qfd;
  addr_t addr;
  pkts_handle_func_t handle_func;
} pkts_cfg_t;

typedef struct pkts_req_t {
  PNIO_DELAY_WARN(int64_t ctime_us);
  int errcode;
  pkts_flush_cb_func_t flush_cb;
  uint64_t sock_id;
  int64_t expire_us;
  int64_t categ_id; // ATTENTION! Cannot add new structure field from categ_id!
  dlink_t link;
  str_t msg;
} pkts_req_t;

extern int pkts_init(pkts_t* io, eloop_t* ep, pkts_cfg_t* cfg);
extern int pkts_resp(pkts_t* pkts, pkts_req_t* req);

typedef struct pkts_sk_t {
  SOCK_COMMON;
  uint64_t id;
  write_queue_t wq;
  ibuffer_t ib;
} pkts_sk_t;

typedef struct pkts_sf_t {
  SOCK_FACTORY_COMMON;
} pkts_sf_t;

typedef struct pkts_t {
  eloop_t* ep;
  listenfd_t listenfd;
  pkts_sf_t sf;
  pkts_handle_func_t on_req;
  evfd_t evfd;
  sc_queue_t req_queue;
  idm_t sk_map;
  idm_item_t sk_table[1<<16];
} pkts_t;
