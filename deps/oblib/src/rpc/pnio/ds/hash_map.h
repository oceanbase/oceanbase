/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
