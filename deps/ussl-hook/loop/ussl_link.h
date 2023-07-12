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

typedef struct ussl_link_t {
  struct ussl_link_t* next;
} ussl_link_t;

inline void ussl_link_init(ussl_link_t* n) {
  n->next = n;
}

inline ussl_link_t* ussl_link_insert(ussl_link_t* prev, ussl_link_t* t) {
  t->next = prev->next;
  return prev->next = t;
}

inline ussl_link_t* ussl_link_delete(ussl_link_t* prev) {
  ussl_link_t* next = prev->next;
  prev->next = next->next;
  return next;
}

inline ussl_link_t* ussl_link_pop(ussl_link_t* h) {
  ussl_link_t* ret = h->next;
  if (ret) {
    h->next = ret->next;
  }
  return ret;
}
#define ussl_link_for_each(h, n) for(ussl_link_t *lpi = h, *n = NULL; lpi && (n = lpi->next, 1);  lpi = n)
