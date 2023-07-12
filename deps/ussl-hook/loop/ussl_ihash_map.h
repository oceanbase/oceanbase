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

typedef struct ussl_hash_t {
  int64_t capacity;
  ussl_link_t table[0];
} ussl_hash_t;

extern ussl_link_t* ussl_ihash_insert(ussl_hash_t* map, ussl_link_t* k);
extern ussl_link_t* ussl_ihash_del(ussl_hash_t* map, uint64_t k);
extern ussl_link_t* ussl_ihash_get(ussl_hash_t* map, uint64_t k);
extern void ussl_hash_init(ussl_hash_t* h, int64_t capacity);
