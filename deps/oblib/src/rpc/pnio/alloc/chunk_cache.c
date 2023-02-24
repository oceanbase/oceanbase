
void chunk_cache_init(chunk_cache_t* cache, int chunk_bytes, int mod)
{
  fixed_stack_init(&cache->free_list);
  cache->mod = mod;
  cache->chunk_bytes = chunk_bytes;
}

void* chunk_cache_alloc(chunk_cache_t* cache, int64_t sz, int* ret_sz)
{
  void* ret = NULL;
  void* p = NULL;
  if (unlikely(sz > cache->chunk_bytes)) {
    p = mod_alloc(sz + sizeof(chunk_cache_t*), cache->mod);
    if (p) {
      *(chunk_cache_t**)p = NULL;
      *ret_sz = sz;
    }
  } else if (NULL != (p = fixed_stack_pop(&cache->free_list))) {
    *ret_sz = cache->chunk_bytes;
  } else if (NULL != (p = mod_alloc(cache->chunk_bytes + sizeof(chunk_cache_t*), cache->mod))) {
    if (p) {
      *(chunk_cache_t**)p = cache;
      *ret_sz = cache->chunk_bytes;
    }
  } else {
    //fail;
  }
  if (p) {
    ret = (void*)((chunk_cache_t**)p + 1);
  }
  return ret;
}

void chunk_cache_free(void* p)
{
  void** pcache = ((void**)p) - 1;
  chunk_cache_t* cache = (typeof(cache))*pcache;
  if (NULL == cache || 0 != fixed_stack_push(&cache->free_list, pcache)) {
    mod_free(pcache);
  }
}
