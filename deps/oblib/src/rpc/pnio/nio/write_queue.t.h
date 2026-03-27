/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

static void my_flush_cb_exception(my_t* io, my_req_t* r) {
  return my_flush_cb(io, r);
}

static void my_write_queue_on_sk_destroy(my_t* io, my_sk_t* s) {
  dlink_for(&(s->wq.queue.head), p) {
    my_req_t* req = structof(p, my_req_t, link);
    my_flush_cb_exception(io, req);
  }
}

static void my_flush_cb_on_post_fail(my_t* io, my_req_t* r) {
  return my_flush_cb_exception(io, r);
}

static void my_flush_cb_after_flush(my_t* io, my_req_t* r) {
  return my_flush_cb(io, r);
}

static int my_sk_do_flush(my_sk_t* s, int64_t* remain) {
  dlink_t* h = NULL;
  int err = my_wq_flush((sock_t*)s, &s->wq, &h);
  my_t* io = structof(s->fty, my_t, sf);
  int64_t flushed_time_us = rk_get_corse_us();
  if (0 == err && NULL != h) {
    dlink_t* stop = dqueue_top(&s->wq.queue);
    while(h != stop) {
      my_req_t* req = structof(h, my_req_t, link);
      h = h->next;
      s->sk_diag_info.write_cnt ++;
      s->sk_diag_info.write_size += req->msg.s;
      s->sk_diag_info.write_wait_time += (flushed_time_us - req->ctime_us);
      my_flush_cb_after_flush(io, req);
    }
  }
  *remain = !dqueue_empty(&s->wq.queue);
  return err;
}
