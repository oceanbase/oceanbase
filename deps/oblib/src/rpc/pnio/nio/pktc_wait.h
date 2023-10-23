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

typedef struct pktc_wait_t {
  int32_t done;
  uint32_t sz;
  char resp[];
} pktc_wait_t;

inline void pktc_wait_cb(const char* b, int64_t s, void* arg) {
  pktc_wait_t* w = (pktc_wait_t*)arg;
  memcpy(w->resp, b, s);
  w->sz = s;
  STORE(&w->done, 1);
  rk_futex_wake(&w->done, 1);
}

inline char* pktc_wait(pktc_wait_t* w, int64_t* sz) {
  while(!w->done) {
    rk_futex_wait(&w->done, 0, NULL);
  }
  if (NULL != sz) {
    *sz = w->sz;
  }
  return w->resp;
}
