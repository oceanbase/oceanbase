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

void hash_init(hash_t* h, int64_t capacity,
               _key_func key_func,
               _hash_func hash_func,
               _equal_func equal_func) {
  h->capacity = capacity;
  memset(&h->table, 0, sizeof(link_t) * capacity);
  h->key_func = key_func;
  h->hash_func = hash_func;
  h->equal_func = equal_func;
}

static uint64_t __hash_calc(link_t* k) { return str_hash((str_t*)(k + 1)); }
static link_t* __hash_locate(hash_t* map, link_t* k) { return &map->table[__hash_calc(k) % map->capacity]; }
static int __hash_cmp(link_t* s1, link_t* s2) { return str_cmp((str_t*)(s1 + 1), (str_t*)(s2 + 1)); }
static link_t* __hash_list_search(link_t* start, link_t* k, link_t** prev) {
  link_t* p = start;
  int cmp = -1;
  while(p->next != NULL && (cmp = __hash_cmp(k, p->next)) > 0) {
    p = p->next;
  }
  if (NULL != prev) {
    *prev = p;
  }
  return 0 == cmp? p->next: NULL;
}

link_t* pnio_hash_insert(hash_t* map, link_t* k) {
  link_t* prev = NULL;
  if(!__hash_list_search(__hash_locate(map, k), k, &prev)) {
    link_insert(prev, k);
  } else {
    k = NULL;
  }
  return k;
}

link_t* hash_del(hash_t* map, str_t* k) {
  link_t* ret = NULL;
  link_t* klink = (link_t*)k - 1;
  link_t* prev = NULL;
  if((ret = __hash_list_search(__hash_locate(map, klink), klink, &prev))) {
    link_delete(prev);
  }
  return ret;
}

link_t* hash_get(hash_t* map, str_t* k) {
  link_t* klink = (link_t*)k - 1;
  return __hash_list_search(__hash_locate(map, klink), klink, NULL);
}
