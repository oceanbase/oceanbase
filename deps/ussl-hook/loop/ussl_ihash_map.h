typedef struct ussl_hash_t {
  int64_t capacity;
  ussl_link_t table[0];
} ussl_hash_t;

extern ussl_link_t* ussl_ihash_insert(ussl_hash_t* map, ussl_link_t* k);
extern ussl_link_t* ussl_ihash_del(ussl_hash_t* map, uint64_t k);
extern ussl_link_t* ussl_ihash_get(ussl_hash_t* map, uint64_t k);
extern void ussl_hash_init(ussl_hash_t* h, int64_t capacity);
