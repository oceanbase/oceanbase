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

void queue_init(queue_t* q) {
  q->head.next = NULL;
  q->tail = &q->head;
}

extern void queue_push(queue_t* q, link_t* n);
extern link_t* queue_pop(queue_t* q);
extern link_t* queue_top(queue_t* q);
extern bool queue_empty(queue_t* q);
extern void queue_set(queue_t* q, link_t* n);

void dqueue_init(dqueue_t* q) {
  dlink_init(&q->head);
}
extern void dqueue_push(dqueue_t* q, dlink_t* n);
extern dlink_t* dqueue_top(dqueue_t* q);
extern bool dqueue_empty(dqueue_t* q);
extern void dqueue_set(dqueue_t* q, dlink_t* n);
extern void dqueue_delete(dqueue_t* q, dlink_t* n);
