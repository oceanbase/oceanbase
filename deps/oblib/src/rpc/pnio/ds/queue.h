/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

typedef struct queue_t {
  link_t head;
  link_t* tail;
} queue_t;

extern void queue_init(queue_t* q);
inline void queue_push(queue_t* q, link_t* n) {
  q->tail = link_insert(q->tail, n);
}

inline link_t* queue_top(queue_t* q) {
  return q->head.next;
}

inline bool queue_empty(queue_t* q) { return NULL == queue_top(q); }
inline void queue_set(queue_t* q, link_t* n) {
  if (!(q->head.next = n)) {
    q->tail = &q->head;
  }
}

inline link_t* queue_pop(queue_t* q) {
  link_t* n = queue_top(q);
  if (n) {
    q->head.next = n->next;
    if (q->tail == n) {
      q->tail = &q->head;
    }
  }
  return n;
}
