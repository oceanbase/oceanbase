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

extern void pn_release_ctx(pkts_req_t* req);
extern pkts_t* locate_pn_pkts(int gid, int tid);
extern void on_dispatch(listenfd_dispatch_t* lfd, sf_t* sf, eloop_t* ep);

static void pkts_post_io(pkts_t* io, pkts_req_t* r) {
  pkts_sk_t* sk = (typeof(sk))idm_get(&io->sk_map, r->sock_id);
  if (ENOMEM == r->errcode) {
    rk_warn("req has err, sock_id=%ld, err=%d, alloted memory failed in pn_resp", r->sock_id, r->errcode);
    pkts_flush_cb_on_post_fail(io, r); // release rpc ctx memory in the pn_pkts_flush_cb_error_func
  } else if (0 == r->errcode) {
    pn_release_ctx(r); // release rpc ctx memory and set resp_ptr
    if (unlikely(NULL == sk)) {
      rk_warn("sk is null, sock_id=%ld, the socket might be disconnected", r->sock_id);
      pkts_flush_cb_on_post_fail(io, r);
    } else if (unlikely(SOCK_RELOCATING == sk->relocate_status)) {
      // push secondry resp
      rk_info("[socket_relocation] send secondry resp, old_sk=%p, target_tid=%lu, target_scok_id=%lu",
              sk, sk->id, sk->relocate_sock_id);
      pn_comm_t* pn = get_current_pnio();
      int gid = pn->gid;
      pkts_t* relocate_pkts = locate_pn_pkts(gid, sk->tid);
      if (relocate_pkts != NULL) {
        r->sock_id = sk->relocate_sock_id;
        pkts_resp(relocate_pkts, r);
      } else {
        rk_warn("[socket_relocation] failed to get relocate_pkts, gid=%d, tid=%d", gid, sk->tid);
        pkts_flush_cb_on_post_fail(io, r);
      }
    } else {
      if (sk->wq.cnt >= MAX_WRITE_QUEUE_COUNT && PNIO_REACH_TIME_INTERVAL(500*1000)) {
        rk_warn("too many requests in pkts write queue, wq_cnt=%ld, wq_sz=%ld, sock_id=%ld, sk=%p", sk->wq.cnt, sk->wq.sz, r->sock_id, sk);
      }
      wq_push(&sk->wq, &r->link);
      eloop_fire(io->ep, (sock_t*)sk);
    }
  } else {
    rk_warn("unexpected errcode, err=%d", r->errcode);
  }
  if (NULL != sk) {
    sk->processing_cnt --;
    if (SOCK_RELOCATING == sk->relocate_status && sk->processing_cnt == 0) {
      rk_info("[socket_relocation] drop sk in src thread, sk=%p", sk);
      idm_del(&io->sk_map, sk->id);
      sfree(sk);
    }
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
