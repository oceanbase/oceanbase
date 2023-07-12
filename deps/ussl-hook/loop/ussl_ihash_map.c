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

#define __ussl_fhmix(h) ({                      \
      (h) ^= (h) >> 23;                         \
      (h) *= 0x2127599bf4325c37ULL;             \
      (h) ^= (h) >> 47; })

static uint64_t ussl_fasthash64(const void *buf, size_t len, uint64_t seed)
{
	const uint64_t    m = 0x880355f21e6d1965ULL;
	const uint64_t *pos = (const uint64_t *)buf;
	const uint64_t *end = pos + (len / 8);
	const unsigned char *pos2;
	uint64_t h = seed ^ (len * m);
	uint64_t v;

	while (pos != end) {
		v  = *pos++;
		h ^= __ussl_fhmix(v);
		h *= m;
	}

	pos2 = (const unsigned char*)pos;
	v = 0;

	switch (len & 7) {
    case 7: v ^= (uint64_t)pos2[6] << 48;
      // fall through
    case 6: v ^= (uint64_t)pos2[5] << 40;
      // fall through
    case 5: v ^= (uint64_t)pos2[4] << 32;
      // fall through
    case 4: v ^= (uint64_t)pos2[3] << 24;
      // fall through
    case 3: v ^= (uint64_t)pos2[2] << 16;
      // fall through
    case 2: v ^= (uint64_t)pos2[1] << 8;
      // fall through
    case 1: v ^= (uint64_t)pos2[0];
      h ^= __ussl_fhmix(v);
      h *= m;
	}
	return __ussl_fhmix(h);
}

static uint64_t __ussl_ihash_calc(uint64_t k) { return ussl_fasthash64(&k, sizeof(k), 0); }
static ussl_link_t* __ussl_ihash_locate(ussl_hash_t* map, uint64_t k) { return &map->table[__ussl_ihash_calc(k) % map->capacity]; }
static uint64_t __ussl_ihash_key(ussl_link_t* l) { return *(uint64_t*)(l + 1); }
static ussl_link_t* __ussl_ihash_list_search(ussl_link_t* start, uint64_t k, ussl_link_t** prev) {
  ussl_link_t* p = start;
  while(p->next != NULL && __ussl_ihash_key(p->next) != k) {
    p = p->next;
  }
  if (NULL != prev) {
    *prev = p;
  }
  return p->next;
}

ussl_link_t* ussl_ihash_insert(ussl_hash_t* map, ussl_link_t* klink) {
  ussl_link_t* prev = NULL;
  uint64_t k = __ussl_ihash_key(klink);
  if(!__ussl_ihash_list_search(__ussl_ihash_locate(map, k), k, &prev)) {
    ussl_link_insert(prev, klink);
  } else {
    klink = NULL;
  }
  return klink;
}

ussl_link_t* ussl_ihash_del(ussl_hash_t* map, uint64_t k) {
  ussl_link_t* ret = NULL;
  ussl_link_t* prev = NULL;
  if((ret = __ussl_ihash_list_search(__ussl_ihash_locate(map, k), k, &prev))) {
    ussl_link_delete(prev);
  }
  return ret;
}

ussl_link_t* ussl_ihash_get(ussl_hash_t* map, uint64_t k) {
  return __ussl_ihash_list_search(__ussl_ihash_locate(map, k), k, NULL);
}

void ussl_hash_init(ussl_hash_t* h, int64_t capacity) {
  h->capacity = capacity;
  memset(&h->table, 0, sizeof(ussl_link_t) * capacity);
}