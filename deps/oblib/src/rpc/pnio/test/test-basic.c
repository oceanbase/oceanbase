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

#include "interface/pkt-nio.h"
#include <pthread.h>

addr_t dest_addr;
#define N 1
eloop_t ep_[N];
pkts_t pkts_[N];
pktc_t pktc_[N];
pkts_cfg_t svr_cfg;

char big_req[1<<21];
void mock_msg_init(str_t* msg, int64_t id, int64_t sz)
{
  eh_copy_msg(msg, id, big_req, sz);
}

void pkts_flush_cb_func(pkts_req_t* req)
{
  mod_free(req);
}

int64_t handle_cnt;
pkts_req_t* create_resp(int64_t sz, uint64_t pkt_id, uint64_t sock_id)
{
  pkts_req_t* r = mod_alloc(sizeof(*r) + sizeof(easy_head_t) + sz, MOD_PKTS_RESP);
  r->errcode = 0;
  r->flush_cb = pkts_flush_cb_func;
  r->sock_id = sock_id;
  mock_msg_init(&r->msg, pkt_id, sz);
  return r;
}

int pkts_handle_func(pkts_t* pkts, void* req_handle, const char* b, int64_t s, uint64_t chid)
{
  uint64_t pkt_id = eh_packet_id(b);
  if (pkt_id == 0) abort();
  //rk_info("pkts handle: chid=%lx pkt_id=%lx", chid, pkt_id);
  FAA(&handle_cnt, 1);
  ref_free(req_handle);
  pkts_resp(pkts, create_resp(48, pkt_id, chid));
  return 0;
}

void init_svr_cfg(pkts_cfg_t* cfg)
{
  cfg->accept_qfd = -1;
  cfg->addr = dest_addr;
  cfg->handle_func = pkts_handle_func;
}

int64_t cb_cnt = 0;
void pktc_flush_cb_func(pktc_req_t* r)
{
  if (NULL == r->resp_cb) {
    mod_free(r);
  }
}

void pktc_resp_cb(pktc_cb_t* cb, const char* resp, int64_t sz)
{
  FAA(&cb_cnt, 1);
  mod_free(cb);
}

pktc_req_t* create_req_with_cb(int64_t id, int64_t sz)
{
  pktc_cb_t* cb = mod_alloc(sizeof(*cb) + sizeof(pktc_req_t) + sizeof(easy_head_t) + sz, MOD_PKTC_CB);
  cb->id = id;
  cb->expire_us = rk_get_us() + 100000000;
  cb->resp_cb = pktc_resp_cb;
  pktc_req_t* r = (typeof(r))(cb + 1);
  r->flush_cb = pktc_flush_cb_func;
  r->resp_cb = cb;
  r->dest = dest_addr;
  mock_msg_init(&r->msg, id, sz);
  return r;
}

pktc_req_t* create_req(int64_t sz)
{
  pktc_req_t* r = mod_alloc(sizeof(pktc_req_t) + sizeof(easy_head_t) + sz, MOD_PKTC_REQ);
  r->resp_cb = NULL;
  r->dest = dest_addr;
  mock_msg_init(&r->msg, 0, sz);
  return r;
}

int io_thread(int i)
{
  eloop_run(ep_ + i);
  return 0;
}

void stress_limit(uint64_t pkt_id)
{
  while(LOAD(&cb_cnt) + 1000 < pkt_id)
    ;
}

int64_t g_pkt_id = 0;
int stress_thread(int i)
{
  pktc_t* pktc = pktc_ + i;
  int err = 0;
  while(1) {
    uint64_t pkt_id = AAF(&g_pkt_id, 1);
    stress_limit(pkt_id);
    int64_t req_sz = random() % sizeof(big_req);
    err = pktc_post(pktc, create_req_with_cb(pkt_id, req_sz));
    //err = pktc_post(pktc, create_req(48));
    assert(0 == err);
  }
  return 0;
}

void* thread_func(void* arg)
{
  int64_t idx = (int64_t)arg;
  if (idx < N) {
    io_thread(idx);
  } else {
    stress_thread(idx - N);
  }
  return NULL;
}

void report()
{
  int64_t last_handle_cnt = 0;
  int64_t last_cb_cnt = 0;
  format_t f;
  format_init(&f, sizeof(f.buf));
  while(1) {
    format_reset(&f);
    mod_report(&f);
    printf("handle: %ld cb: %ld alloc: %s\n", handle_cnt - last_handle_cnt, cb_cnt - last_cb_cnt, format_gets(&f));
    last_handle_cnt = handle_cnt;
    last_cb_cnt = cb_cnt;
    usleep(1000 * 1000);
  }
}

int main()
{
  int err = 0;
  addr_init(&dest_addr, "127.0.0.1", 8042);
  init_svr_cfg(&svr_cfg);
  for(int i = 0; i < N; i++) {
    eloop_init(ep_ + i);
    if (0 != (err = pkts_init(pkts_ + i, ep_ + i, &svr_cfg))) {
      abort();
    }
    if (0 != (err = pktc_init(pktc_ + i, ep_ + i, 0))) {
      abort();
    }
  }
  pthread_t thd[2 * N];
  for(int64_t i = 0; i < 2 * N; i++) {
    pthread_create(thd + i, NULL, thread_func, (void*)i);
  }
  report();
  for(int64_t i = 0; i < 2 * N; i++) {
    pthread_join(thd[i], NULL);
  }
  return 0;
}

#include "interface/pkt-nio.c"
