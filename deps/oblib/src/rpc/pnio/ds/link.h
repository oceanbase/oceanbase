typedef struct link_t {
  struct link_t* next;
} link_t;

inline void link_init(link_t* n) {
  n->next = n;
}

inline bool link_is_empty(link_t* n) {
  return n->next == n;
}

inline link_t* link_insert(link_t* prev, link_t* t) {
  t->next = prev->next;
  return prev->next = t;
}

inline link_t* link_delete(link_t* prev) {
  link_t* next = prev->next;
  prev->next = next->next;
  return next;
}

inline link_t* link_pop(link_t* h) {
  link_t* ret = h->next;
  if (ret) {
    h->next = ret->next;
  }
  return ret;
}
#define link_for_each(h, n) for(link_t *lpi = h, *n = NULL; lpi && (n = lpi->next, 1);  lpi = n)
