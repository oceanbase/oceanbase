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

static void pkts_post_io(pkts_t* io, pkts_req_t* r) {
  pkts_sk_t* sk = (typeof(sk))idm_get(&io->sk_map, r->sock_id);
  if (sk && 0 == r->errcode) {
    if (sk->wq.cnt >= MAX_WRITE_QUEUE_COUNT && PNIO_REACH_TIME_INTERVAL(500*1000)) {
      rk_warn("too many requests in pkts write queue, wq_cnt=%ld, wq_sz=%ld, sock_id=%ld, sk=%p", sk->wq.cnt, sk->wq.sz, r->sock_id, sk);
    }
    wq_push(&sk->wq, &r->link);
    eloop_fire(io->ep, (sock_t*)sk);
  } else {
    pkts_flush_cb_on_post_fail(io, r);
  }
}

int pkts_resp(pkts_t* io, pkts_req_t* req) {
  PNIO_DELAY_WARN(req->ctime_us = rk_get_corse_us());
  int64_t queue_cnt = 0;
  int64_t queue_sz = 0;
  link_t* req_link = (link_t*)(&req->link);
  sc_queue_inc(&io->req_queue, req_link, &queue_cnt, &queue_sz);
  if (queue_cnt >= MAX_REQ_QUEUE_COUNT && PNIO_REACH_TIME_INTERVAL(500*1000)) {
    rk_warn("too many requests in pkts req_queue, queue_cnt=%ld, queue_sz=%ld", queue_cnt, queue_sz);
  }
  if (sc_queue_push(&io->req_queue, req_link)) {
    evfd_signal(io->evfd.fd);
  }
  return 0;
}

static int pkts_handle_req_queue(pkts_t* io) {
  link_t* l = NULL;
  int cnt = 0;
  int64_t sz = 0;
  int64_t sc_queue_time = 0;
  while(cnt < 128 && (l = sc_queue_pop(&io->req_queue))) {
    pkts_req_t* req = structof(l, pkts_req_t, link);
    int64_t cur_time = rk_get_corse_us();
    int64_t delay_time = cur_time - req->ctime_us;
    if (delay_time > HANDLE_DELAY_WARN_US && PNIO_REACH_TIME_INTERVAL(500*1000)) {
      rk_warn("[delay_warn] delay high: %ld", delay_time);
    }
    cnt++;
    sz += req->msg.s;
    sc_queue_time += delay_time;
    req->ctime_us = cur_time;
    pkts_post_io(io, req);
  }
  io->diag_info.send_cnt += cnt;
  io->diag_info.send_size += sz;
  io->diag_info.sc_queue_time += sc_queue_time;
  return cnt == 0? EAGAIN: 0;
}

static int pkts_evfd_cb(sock_t* s) {
  evfd_drain(s->fd);
  return pkts_handle_req_queue(structof(s, pkts_t, evfd));
}
