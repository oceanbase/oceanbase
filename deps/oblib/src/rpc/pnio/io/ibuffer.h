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

typedef struct ibuffer_t {
  int mod;
  int64_t cur_ref_;
  char* limit;
  char* b;
  char* s;
  char* e;
} ibuffer_t;

extern void ib_init(ibuffer_t* ib, int mod);
extern void ib_consumed(ibuffer_t* ib, int64_t sz);
extern int sk_read_with_ib(void** ret, sock_t* s, ibuffer_t* ib, int64_t sz, int64_t* read_bytes_ret);
extern void* ib_ref(ibuffer_t* ib);
extern void ib_ref_free(void* p);
extern void ib_destroy(ibuffer_t* ib);
