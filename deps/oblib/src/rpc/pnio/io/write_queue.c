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

str_t* sfl(dlink_t* l) { return (str_t*)(l+1); }
int64_t cidfl(dlink_t* l) {return  *((int64_t*)l-1); }
static int iov_from_blist(struct iovec* iov, int64_t limit, dlink_t* head) {
  int cnt = 0;
  dlink_for(head, p) {
    if (cnt >= limit) {
      break;
    }
    iov_set_from_str(iov + cnt, sfl(p));
    cnt++;
  }
  return cnt;
}

static int sk_flush_blist(sock_t* s, dlink_t* head, int64_t last_pos, int64_t* wbytes) {
  int err = 0;
  struct iovec iov[64];
  int cnt = iov_from_blist(iov, arrlen(iov), head);
  if (cnt > 0) {
    iov_consume_one(iov, last_pos);
    err = sk_writev(s, iov, cnt, wbytes);
  }
  return err;
}

void wq_inc(write_queue_t* wq, dlink_t* l) {
  int64_t bytes = sfl(l)->s;
  wq->cnt ++;
  wq->sz += bytes;
  int64_t cid = cidfl(l);
  wq->categ_count_bucket[cid % arrlen(wq->categ_count_bucket)] ++;
}

void wq_dec(write_queue_t* wq, dlink_t* l) {
  int64_t bytes = sfl(l)->s;
  wq->cnt --;
  wq->sz -= bytes;
  int64_t cid = cidfl(l);
  wq->categ_count_bucket[cid % arrlen(wq->categ_count_bucket)] --;
}

static dlink_t* wq_consume(write_queue_t* wq, int64_t bytes) {
  int64_t s = 0;
  dlink_t* top = dqueue_top(&wq->queue);
  dlink_t* h = top;
  if((s = sfl(h)->s - wq->pos) <= bytes) {
    bytes -= s;
    wq_dec(wq, h);
    h = h->next;
    while(bytes > 0 && (s = sfl(h)->s) <= bytes) {
      bytes -= s;
      wq_dec(wq, h);
      h = h->next;
    }
    wq->pos = bytes;
  } else {
    wq->pos += bytes;
  }
  dqueue_set(&wq->queue, h);
  return top;
}

void wq_init(write_queue_t* wq) {
  dqueue_init(&wq->queue);
  wq->pos = 0;
  wq->cnt = 0;
  wq->sz = 0;
  memset(wq->categ_count_bucket, 0, sizeof(wq->categ_count_bucket));
}

inline void wq_push(write_queue_t* wq, dlink_t* l) {
  wq_inc(wq, l);
  dqueue_push(&wq->queue, l);
}

inline int wq_delete(write_queue_t* wq, dlink_t* l) {
  int err = PNIO_OK;
  if (dqueue_top(&wq->queue) == l) {
    // not to delete the first req of write_queue
    err = PNIO_ERROR;
  } else if (l == l->prev) {
    // req hasn't been inserted into flush_list
    err = PNIO_ERROR;
  } else {
    wq_dec(wq, l);
    dqueue_delete(&wq->queue, l);
  }
  return err;
}

int wq_flush(sock_t* s, write_queue_t* wq, dlink_t** old_head) {
  int err = 0;
  int64_t wbytes = 0;
  err = sk_flush_blist((sock_t*)s, &wq->queue.head, wq->pos, &wbytes);
  if (0 == err && wbytes > 0) {
    *old_head = wq_consume(wq, wbytes);
  }
  return err;
}
