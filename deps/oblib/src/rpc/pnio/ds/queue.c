void queue_init(queue_t* q) {
  q->head.next = NULL;
  q->tail = &q->head;
}

extern void queue_push(queue_t* q, link_t* n);
extern link_t* queue_pop(queue_t* q);
extern link_t* queue_top(queue_t* q);
extern bool queue_empty(queue_t* q);
extern void queue_set(queue_t* q, link_t* n);
