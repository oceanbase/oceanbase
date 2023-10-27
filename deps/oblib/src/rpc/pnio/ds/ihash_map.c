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

static uint64_t __ihash_calc(uint64_t k) { return fasthash64(&k, sizeof(k), 0); }
static link_t* __ihash_locate(hash_t* map, uint64_t k) { return &map->table[__ihash_calc(k) % map->capacity]; }
static uint64_t __ihash_key(link_t* l) { return *(uint64_t*)(l + 1); }
static link_t* __ihash_list_search(link_t* start, uint64_t k, link_t** prev) {
  link_t* p = start;
  while(p->next != NULL && __ihash_key(p->next) != k) {
    p = p->next;
  }
  if (NULL != prev) {
    *prev = p;
  }
  return p->next;
}

link_t* ihash_insert(hash_t* map, link_t* klink) {
  link_t* prev = NULL;
  uint64_t k = __ihash_key(klink);
  if(!__ihash_list_search(__ihash_locate(map, k), k, &prev)) {
    link_insert(prev, klink);
  } else {
    klink = NULL;
  }
  return klink;
}

link_t* ihash_del(hash_t* map, uint64_t k) {
  link_t* ret = NULL;
  link_t* prev = NULL;
  if((ret = __ihash_list_search(__ihash_locate(map, k), k, &prev))) {
    link_delete(prev);
  }
  return ret;
}

link_t* ihash_get(hash_t* map, uint64_t k) {
  return __ihash_list_search(__ihash_locate(map, k), k, NULL);
}
