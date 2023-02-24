typedef struct fifo_alloc_t
{
  chunk_cache_t* chunk_alloc;
  void* cur;
} fifo_alloc_t;
extern void fifo_alloc_init(fifo_alloc_t* alloc, chunk_cache_t* chunk_alloc);
extern void* fifo_alloc(fifo_alloc_t* alloc, int sz);
extern void fifo_free(void* p);
