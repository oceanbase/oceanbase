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

typedef struct hash_t {
  int64_t capacity;
  link_t table[0];
} hash_t;

extern hash_t* hash_create(int64_t capacity);
extern void hash_init(hash_t* h, int64_t capacity);
extern link_t* hash_insert(hash_t* map, link_t* k);
extern link_t* hash_del(hash_t* map, str_t* k);
extern link_t* hash_get(hash_t* map, str_t* k);
