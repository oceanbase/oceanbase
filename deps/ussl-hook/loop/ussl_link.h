typedef struct ussl_link_t {
  struct ussl_link_t* next;
} ussl_link_t;

inline void ussl_link_init(ussl_link_t* n) {
  n->next = n;
}

inline ussl_link_t* ussl_link_insert(ussl_link_t* prev, ussl_link_t* t) {
  t->next = prev->next;
  return prev->next = t;
}

inline ussl_link_t* ussl_link_delete(ussl_link_t* prev) {
  ussl_link_t* next = prev->next;
  prev->next = next->next;
  return next;
}

inline ussl_link_t* ussl_link_pop(ussl_link_t* h) {
  ussl_link_t* ret = h->next;
  if (ret) {
    h->next = ret->next;
  }
  return ret;
}
#define ussl_link_for_each(h, n) for(ussl_link_t *lpi = h, *n = NULL; lpi && (n = lpi->next, 1);  lpi = n)
