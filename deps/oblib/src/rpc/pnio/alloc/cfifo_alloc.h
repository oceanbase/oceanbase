struct cfifo_page_t;
typedef struct cfifo_alloc_t
{
  chunk_cache_t* chunk_alloc;
  struct cfifo_page_t* cur RK_CACHE_ALIGNED;
  int32_t remain RK_CACHE_ALIGNED;
} cfifo_alloc_t;

extern void cfifo_alloc_init(cfifo_alloc_t* alloc, chunk_cache_t* chunk_alloc);
extern void* cfifo_alloc(cfifo_alloc_t* alloc, int sz);
extern void cfifo_free(void* p);
