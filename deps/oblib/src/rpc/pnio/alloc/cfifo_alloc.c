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

typedef struct cfifo_page_t
{
  // 每次alloc ref不加1，每次free，ref减1. retire page的时候ref加K
  int ref_ RK_CACHE_ALIGNED;
  uint64_t stock_ RK_CACHE_ALIGNED;
  char data_[0];
} cfifo_page_t;

static void cfifo_page_born(cfifo_page_t* pg, int sz)
{
  pg->ref_ = 0;
  pg->stock_ = (1ULL<<63) + sz - sizeof(*pg);
}

static int32_t cfifo_calc_pgsz(int sz)
{
  return sz + sizeof(cfifo_page_t);
}

static cfifo_page_t* cfifo_page_create(cfifo_alloc_t* alloc, int pgsz, int* ret_pgsz)
{
  cfifo_page_t* pg = NULL;
  if ((pg = (cfifo_page_t*)chunk_cache_alloc(alloc->chunk_alloc, pgsz, ret_pgsz))) {
    cfifo_page_born(pg, *ret_pgsz);
  }
  return pg;
}

static void cfifo_page_release(cfifo_page_t* pg)
{
  if (0 == AAF(&pg->ref_, -1)) {
   chunk_cache_free(pg);
  }
}

static void* cfifo_alloc_from_page(cfifo_page_t* pg, int32_t offset)
{
  void* ret = NULL;
  ret = pg->data_ +  (int32_t)offset;
  *(typeof(&pg))ret = pg;
  ret = (void*)((typeof(&pg))ret + 1);
  return ret;
}

static bool cfifo_page_after_sliced(cfifo_page_t* pg, int32_t sz)
{
  bool ret = 0;
  int64_t compound_sz = sz + (1ULL<<32);
  uint64_t pos = AAF(&pg->stock_, -compound_sz);
  if ((ret = ((int32_t)pos <= 0))) {
    if (0 == AAF(&pg->ref_, (1<<31) ^ -(int32_t)(pos>>32))) {
      chunk_cache_free(pg);
    }
  }
  return ret;
}

void cfifo_alloc_init(cfifo_alloc_t* alloc, chunk_cache_t* chunk_alloc)
{
  alloc->chunk_alloc = chunk_alloc;
  alloc->cur = NULL;
  alloc->remain = 0;
}

void* cfifo_alloc(cfifo_alloc_t* alloc, int sz)
{
  void* ret = NULL;
  int64_t req_sz = upalign8(sz) + sizeof(void*);
  while(NULL == ret) {
    int32_t remain = LOAD(&alloc->remain);
    if (unlikely(remain <= 0)) {
      if (LOAD(&alloc->cur)) {
        continue;
      }
      int32_t pgsz = cfifo_calc_pgsz(req_sz);
      int32_t ret_pgsz = 0;
      cfifo_page_t* pg = cfifo_page_create(alloc, pgsz, &ret_pgsz);
      if (NULL != pg) {
        if (BCAS(&alloc->cur, NULL, pg)) {
          STORE(&alloc->remain, ret_pgsz - sizeof(*pg));
        } else {
          chunk_cache_free(pg);
        }
      } else {
        break;
      }
    } else if (unlikely(remain < req_sz)) {
      if (BCAS(&alloc->remain, remain, 0)) {
        cfifo_page_t* pg = LOAD(&alloc->cur);
        if (cfifo_page_after_sliced(pg, remain)) {
          STORE(&alloc->cur, NULL);
        }
        cfifo_page_release(pg);
      }
    } else if (BCAS(&alloc->remain, remain, remain - req_sz)) {
      cfifo_page_t* pg = LOAD(&alloc->cur);
      if (cfifo_page_after_sliced(pg, req_sz)) {
        STORE(&alloc->cur, NULL);
      }
      ret = cfifo_alloc_from_page(pg, remain - req_sz);
    }
  }
  return ret;
}

void cfifo_free(void* p)
{
  cfifo_page_t* pg = *((typeof(&pg))p - 1);
  cfifo_page_release(pg);
}
