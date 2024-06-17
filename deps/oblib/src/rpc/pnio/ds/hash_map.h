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

typedef void* (*_key_func)(link_t* link);
typedef uint64_t (*_hash_func)(void* key);
typedef bool (*_equal_func)(void* l_key, void *r_key);

typedef struct hash_t {
  _key_func key_func;
  _hash_func hash_func;
  _equal_func equal_func;
  int64_t capacity;
  link_t table[0];
} hash_t;

extern void hash_init(hash_t* h, int64_t capacity,
                      _key_func key_func,
                      _hash_func hash_func,
                      _equal_func equal_func);
extern link_t* hash_insert(hash_t* map, link_t* k);
extern link_t* hash_del(hash_t* map, str_t* k);
extern link_t* hash_get(hash_t* map, str_t* k);
