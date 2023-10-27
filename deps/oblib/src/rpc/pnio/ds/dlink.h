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

typedef struct dlink_t {
  struct dlink_t* next;
  struct dlink_t* prev;
} dlink_t;

inline bool dlink_is_empty(dlink_t* n) { return n->next == n; }

inline void dlink_init(dlink_t* n) {
  n->prev = n;
  n->next = n;
}

inline void __dlink_insert(dlink_t* prev, dlink_t* next, dlink_t* n) {
  n->prev = prev;
  n->next = next;
  prev->next = n;
  next->prev = n;
}

inline void __dlink_delete(dlink_t* prev, dlink_t* next) {
  prev->next = next;
  next->prev = prev;
}

inline void dlink_insert(dlink_t* head, dlink_t* n) {
  __dlink_insert(head, head->next, n);
}

inline void dlink_insert_before(dlink_t* head, dlink_t* n) {
  __dlink_insert(head->prev, head, n);
}

inline void dlink_delete(dlink_t* n) {
  if (n->next) {
    __dlink_delete(n->prev, n->next);
    n->next = NULL;
  }
}

#define dlink_for(head, p) for(dlink_t* p = (head)->next, *_np = p->next; p != (head); p = _np, _np = p->next)
