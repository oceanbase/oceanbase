void fixed_queue_init(fixed_queue_t* q, void* buf, int64_t bytes)
{
  q->push = 0;
  q->pop = 0;
  q->data = (void**)buf;
  q->capacity = bytes/sizeof(void*);
  memset(buf, 0, bytes);
}

extern int fixed_queue_push(fixed_queue_t* q, void* p);
extern void* fixed_queue_pop(fixed_queue_t* q);
