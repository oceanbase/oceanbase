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

void sc_queue_init(sc_queue_t* q) {
  q->head.next = NULL;
  q->tail = &q->head;
  q->cnt = 0;
  q->sz = 0;
}

extern str_t* sfl(dlink_t* l);
int64_t sc_queue_inc(sc_queue_t* q, link_t* n, int64_t* ret_cnt, int64_t* ret_sz) {
  *ret_cnt = AAF(&q->cnt, 1);
  *ret_sz = AAF(&q->sz, sfl((dlink_t*)n)->s);
  return *ret_cnt;
}
void sc_queue_dec(sc_queue_t* q, link_t* n) {
  FAA(&q->cnt, -1);
  FAA(&q->sz, -sfl((dlink_t*)n)->s);
}

extern link_t* sc_queue_top(sc_queue_t* q);
extern bool sc_queue_push(sc_queue_t* q, link_t* n);
extern link_t* sc_queue_pop(sc_queue_t* q);
