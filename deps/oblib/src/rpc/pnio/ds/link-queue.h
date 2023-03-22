typedef struct link_queue_t
{
  link_t *head_ RK_CACHE_ALIGNED;
  link_t *tail_ RK_CACHE_ALIGNED;
  link_t dummy_ RK_CACHE_ALIGNED;
} link_queue_t;

static void link_queue_init(link_queue_t* q)
{
  q->head_ = &q->dummy_;
  q->tail_ = &q->dummy_;
}

static link_t* link_queue_do_pop(link_queue_t* q)
{
  link_t* ret = NULL;
  link_t* head = NULL;
  while(NULL == (head = TAS(&q->head_, NULL))) {
    SPIN_PAUSE();
  }
  if (head == LOAD(&q->tail_)) {
    STORE(&q->head_, head);
  } else {
    link_t* next = NULL;
    while(NULL == (next = LOAD(&head->next))) {
      SPIN_PAUSE();
    }
    STORE(&q->head_, next);
    ret = head;
  }
  return ret;
}

static void link_queue_push(link_queue_t *q, link_t* p)
{
  link_t *tail = NULL;
  p->next = NULL;
  tail = TAS(&q->tail_, p);
  STORE(&tail->next, p);
}

static link_t* link_queue_pop(link_queue_t *q)
{
  link_t* ret = NULL;
  while(NULL != (ret = link_queue_do_pop(q)) && ret == &q->dummy_) {
    link_queue_push(q, &q->dummy_);
  }
  return ret;
}
