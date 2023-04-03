void sc_queue_init(sc_queue_t* q) {
  q->head.next = NULL;
  q->tail = &q->head;
  q->cnt = 0;
  q->sz = 0;
}

extern str_t* sfl(link_t* l);
int64_t sc_queue_inc(sc_queue_t* q, link_t* n, int64_t* ret_cnt, int64_t* ret_sz) {
  *ret_cnt = AAF(&q->cnt, 1);
  *ret_sz = AAF(&q->sz, sfl(n)->s);
  return *ret_cnt;
}
void sc_queue_dec(sc_queue_t* q, link_t* n) {
  FAA(&q->cnt, -1);
  FAA(&q->sz, -sfl(n)->s);
}

extern link_t* sc_queue_top(sc_queue_t* q);
extern bool sc_queue_push(sc_queue_t* q, link_t* n);
extern link_t* sc_queue_pop(sc_queue_t* q);
