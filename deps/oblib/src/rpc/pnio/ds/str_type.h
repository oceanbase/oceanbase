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

typedef struct str_t {
  int64_t s;
  char b[0];
} str_t;
inline int64_t str_hash(str_t* s) { return fasthash64(s->b, s->s, 0); }
inline int str_cmp(str_t* s1, str_t* s2) {
  int cmp = memcmp(s1->b, s2->b, rk_min(s1->s, s2->s));
  if (!cmp) return cmp;
  return s1->s - s2->s;
}
