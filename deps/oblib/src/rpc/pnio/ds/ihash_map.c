/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

static link_t* __ihash_locate(hash_t* map, void* key)
{
  return &map->table[map->hash_func(key) % map->capacity];
}

static link_t* __ihash_list_search(hash_t* map, void* key, link_t** prev) {
  link_t* start = __ihash_locate(map, key);
  link_t* p = start;
  while(p->next != NULL && !map->equal_func(map->key_func(p->next), key)) {
    p = p->next;
  }
  if (NULL != prev) {
    *prev = p;
  }
  return p->next;
}

link_t* ihash_insert(hash_t* map, link_t* klink) {
  link_t* prev = NULL;
  void* key = map->key_func(klink);
  if(!__ihash_list_search(map, key, &prev)) {
    link_insert(prev, klink);
  } else {
    klink = NULL;
  }
  return klink;
}

link_t* ihash_del(hash_t* map, void* key) {
  link_t* ret = NULL;
  link_t* prev = NULL;
  if((ret = __ihash_list_search(map, key, &prev))) {
    link_delete(prev);
  }
  return ret;
}

link_t* ihash_get(hash_t* map, void* key) {
  return __ihash_list_search(map, key, NULL);
}
