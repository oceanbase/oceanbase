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

typedef struct sc_queue_t {
  link_t head RK_CACHE_ALIGNED;
  link_t* tail RK_CACHE_ALIGNED;
  int64_t cnt RK_CACHE_ALIGNED;
  int64_t sz RK_CACHE_ALIGNED;
} sc_queue_t;

extern void sc_queue_init(sc_queue_t* q);
inline link_t* sc_queue_top(sc_queue_t* q) {
  return LOAD(&q->head.next);
}

int64_t sc_queue_inc(sc_queue_t* q, link_t* n, int64_t* ret_cnt, int64_t* ret_sz);
void sc_queue_dec(sc_queue_t* q, link_t* n);
inline bool sc_queue_push(sc_queue_t* q, link_t* n) {
  n->next = NULL;
  link_t* ot = TAS(&q->tail, n);
  STORE(&ot->next, n);
  return ot == &q->head;
}

inline link_t* sc_queue_pop(sc_queue_t* q) {
  link_t* ret = sc_queue_top(q);
  if (NULL != ret) {
    sc_queue_dec(q, ret);
    link_t* next = LOAD(&ret->next);
    if (NULL != next) {
      STORE(&q->head.next, next);
    } else {
      if (BCAS(&q->tail, ret, &q->head)) {
        // 此处无论成功失败与否都符合预期。如果成功，代表没有push竞争，更新顺利，失败代表有push竞争，无需更新
        BCAS(&q->head.next, ret, NULL);
      } else {
        while(NULL == (next = LOAD(&ret->next))) {
          SPIN_PAUSE();
        }
        STORE(&q->head.next, next);
      }
    }
  }
  return ret;
}
