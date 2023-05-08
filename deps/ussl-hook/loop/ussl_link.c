extern inline void ussl_link_init(ussl_link_t* n);
extern inline int ussl_link_is_empty(ussl_link_t* n);
extern inline ussl_link_t* ussl_link_insert(ussl_link_t* prev, ussl_link_t* t);
extern inline ussl_link_t* ussl_link_delete(ussl_link_t* prev);
extern ussl_link_t* ussl_link_pop(ussl_link_t* h);