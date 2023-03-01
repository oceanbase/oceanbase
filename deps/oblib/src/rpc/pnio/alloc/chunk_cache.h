typedef struct chunk_cache_t
{
  int mod;
  int chunk_bytes;
  fixed_stack_t free_list;
} chunk_cache_t;

extern void chunk_cache_init(chunk_cache_t* cache, int chunk_bytes, int mod);
extern void* chunk_cache_alloc(chunk_cache_t* cache, int64_t sz, int* chunk_size);
extern void chunk_cache_free(void* p);
