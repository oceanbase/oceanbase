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

typedef struct fixed_queue_t {
  void** data;
  int64_t capacity;
  int64_t push RK_CACHE_ALIGNED;
  int64_t pop RK_CACHE_ALIGNED;
} fixed_queue_t;

void fixed_queue_init(fixed_queue_t* q, void* buf, int64_t bytes);
inline int fixed_queue_push(fixed_queue_t* q, void* p) {
  int err = -EAGAIN;
  uint64_t push_limit = LOAD(&q->pop) + q->capacity;
  uint64_t old_push = 0;
  uint64_t push = LOAD(&q->push);
  while((old_push = push) < push_limit
        && old_push != (push = VCAS(&q->push, old_push, old_push + 1))) {
    SPIN_PAUSE();
  }
  if (push < push_limit) {
    void** pdata = q->data + (push % q->capacity);
    while(!BCAS(pdata, NULL, p)) {
      SPIN_PAUSE();
    }
    err = 0;
  }
  return err;
}

inline void* fixed_queue_pop(fixed_queue_t* q) {
  void* p = NULL;
  uint64_t pop_limit = LOAD(&q->push);
  uint64_t old_pop = 0;
  uint64_t pop = LOAD(&q->pop);
  while((old_pop = pop) < pop_limit
        && old_pop != (pop = VCAS(&q->pop, old_pop, old_pop + 1))) {
    SPIN_PAUSE();
  }
  if (pop < pop_limit) {
    void** pdata = q->data + (pop % q->capacity);
    while(NULL == LOAD(pdata) || NULL == (p = TAS(pdata, NULL))) {
      SPIN_PAUSE();
    }
  }
  return p;
}
