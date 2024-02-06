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

static pktc_sk_t* pktc_do_connect(pktc_t* cl, addr_t dest) {
  pktc_sk_t* sk = NULL;
  ef(!(sk = pktc_sk_new(&cl->sf)));
  sk->pc = cl;
  sk->dest = dest;
  ef((sk->fd = async_connect(dest, cl->dispatch_id)) < 0);
  rk_info("sk_new: sk=%p, fd=%d", sk, sk->fd);
  ef(eloop_regist(cl->ep, (sock_t*)sk, EPOLLIN|EPOLLOUT));
  return sk;
  el();
  if (sk) {
    rk_warn("connect failed, sk=%p, fd=%d, errno=%d", sk, sk->fd, errno);
    pktc_sk_delete(&cl->sf, sk);
  }
  return NULL;
}

static pktc_sk_t* pktc_try_connect(pktc_t* cl, addr_t dest) {
  pktc_sk_t* sk = NULL;
  link_t* sk_link = ihash_get(&cl->sk_map, *(uint64_t*)&dest);
  if (sk_link) {
    sk = structof(sk_link, pktc_sk_t, hash);
  } else {
    ef(!(sk = pktc_do_connect(cl, dest)));
    ihash_insert(&cl->sk_map, &sk->hash);
    dlink_insert(&cl->sk_list, &sk->list_link);
  }
  return sk;
  el();
  rk_warn("sk create fail: %s sk=%p", T2S(addr, dest), sk);
  if (sk) {
    pktc_sk_destroy(&cl->sf, sk);
  }
  return NULL;
}

static int pktc_wq_push_pre(write_queue_t* wq, pktc_req_t* r) {
  int err = 0;
  int16_t* bucket = wq->categ_count_bucket;
  int16_t id = r->categ_id % arrlen(wq->categ_count_bucket);
  if (wq->cnt >= MAX_WRITE_QUEUE_COUNT || bucket[id] >= MAX_CATEG_COUNT) {
    if (PNIO_REACH_TIME_INTERVAL(500*1000)) {
      rk_warn("too many requests in pktc write queue, wq_cnt=%ld, wq_sz=%ld, categ_id=%ld, categ_cnt=%d, socket=(ptr=%p,fd=%d)",
        wq->cnt, wq->sz, r->categ_id, bucket[id], r->sk, r->sk->fd);
    }
    err = PNIO_DISPATCH_ERROR;
  }
  return err;
}
static int pktc_do_post(pktc_t* io, pktc_sk_t* sk, pktc_req_t* r) {
  pktc_cb_t* cb = r->resp_cb;
  int err = pktc_wq_push_pre(&sk->wq, r);
  if (err != PNIO_OK) {
    // drop req
  } else {
    if (cb) {
      dlink_insert(&sk->cb_head, &cb->sk_dlink);
      ihash_insert(&io->cb_map, &cb->hash_link);
      tw_regist(&io->cb_tw, &cb->timer_dlink);
    }
    wq_push(&sk->wq, &r->link);
  }
  return err;
}

static void pktc_post_io(pktc_t* io, pktc_req_t* r) {
  int err = PNIO_OK;
  pktc_sk_t* sk = pktc_try_connect(io, r->dest);
  r->sk = sk;
  if (NULL == sk) {
    err = PNIO_CONNECT_FAIL;
  } else if  (PNIO_OK != (err = pktc_do_post(io, sk, r))) {
    rk_debug("req was dropped, req_id=%ld, sock=(%p,%d)", r->resp_cb->id, sk, sk->fd);
  } else {
    eloop_fire(io->ep, (sock_t*)sk);
  }
  if (err != PNIO_OK) {
    pktc_cb_t* cb = r->resp_cb;
    cb->errcode = err;
    pktc_resp_cb_on_post_fail(io, cb);
    pktc_flush_cb_on_post_fail(io, r);
  }
}

int pktc_post(pktc_t* io, pktc_req_t* req) {
  PNIO_DELAY_WARN(req->ctime_us = rk_get_corse_us());
  if (req->msg.s < (int64_t)sizeof(req->msg)) {
    return -EINVAL;
  }
  int64_t queue_cnt = 0;
  int64_t queue_sz = 0;
  link_t* req_link = (link_t*)(&req->link);
  sc_queue_inc(&io->req_queue, req_link, &queue_cnt, &queue_sz);
  if (queue_cnt >= MAX_REQ_QUEUE_COUNT && PNIO_REACH_TIME_INTERVAL(500*1000)) {
    rk_warn("too many requests in pktc req_queue, queue_cnt=%ld, queue_sz=%ld, pnio dispatch_id=%ld", queue_cnt, queue_sz, io->dispatch_id);
  }
  if (sc_queue_push(&io->req_queue, req_link)) {
    evfd_signal(io->evfd.fd);
  }
  return 0;
}

static int pktc_handle_req_queue(pktc_t* io) {
  link_t* l = NULL;
  int cnt = 0;
  while(cnt < 128 && (l = sc_queue_pop(&io->req_queue))) {
    pktc_req_t* req = structof(l, pktc_req_t, link);
    PNIO_DELAY_WARN(delay_warn("pktc_handle_req_queue", req->ctime_us, HANDLE_DELAY_WARN_US));
    pktc_post_io(io, req);
    cnt++;
  }
  return cnt == 0? EAGAIN: 0;
}

static int pktc_evfd_cb(sock_t* s) {
  evfd_drain(s->fd);
  return pktc_handle_req_queue(structof(s, pktc_t, evfd));
}
