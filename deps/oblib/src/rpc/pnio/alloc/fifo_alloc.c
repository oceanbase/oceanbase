typedef struct fifo_page_t
{
  int ref_;
  int pos_;
  char data_[0];
} fifo_page_t;

static void fifo_page_born(fifo_page_t* pg, int sz)
{
  pg->ref_ = 1;
  pg->pos_ = sz - sizeof(*pg);
}

static fifo_page_t* fifo_page_create(fifo_alloc_t* alloc, int sz)
{
  fifo_page_t* pg = NULL;
  int pg_sz = sz + sizeof(*pg) + sizeof(&pg);
  int chunk_size = 0;
  if ((pg = (fifo_page_t*)chunk_cache_alloc(alloc->chunk_alloc, pg_sz, &chunk_size))) {
    fifo_page_born(pg, chunk_size);
  }
  return pg;
}

static void fifo_page_release(fifo_page_t* pg)
{
  if (0 == --pg->ref_) {
    chunk_cache_free(pg);
  }
}

static void* fifo_alloc_from_page(fifo_page_t* pg, int sz)
{
  void* ret = NULL;
  sz += sizeof(&pg);
  if ((pg->pos_ -= sz) >= 0) {
    ret = pg->data_ + pg->pos_;
    *(typeof(&pg))ret = pg;
    ret = (void*)((typeof(&pg))ret + 1);
    pg->ref_++;
  }
  return ret;
}

void fifo_alloc_init(fifo_alloc_t* alloc, chunk_cache_t* chunk_alloc)
{
  alloc->chunk_alloc = chunk_alloc;
  alloc->cur = NULL;
}

void* fifo_alloc(fifo_alloc_t* alloc, int sz1)
{
  void* ret = NULL;
  fifo_page_t* pg = (fifo_page_t*)alloc->cur;
  int sz = (int)upalign8(sz1);
  if (pg) {
    ret = fifo_alloc_from_page(pg, sz);
  }
  if (!ret) {
    fifo_page_t* npg = fifo_page_create(alloc, sz);
    if (npg) {
      if (pg) {
        fifo_page_release(pg);
      }
      alloc->cur = npg;
      ret = fifo_alloc_from_page(npg, sz);
    }
  }
  return ret;
}

void fifo_free(void* p)
{
  fifo_page_t* pg = *((typeof(&pg))p - 1);
  fifo_page_release(pg);
}
