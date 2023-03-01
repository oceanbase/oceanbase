typedef struct hash_t {
  int64_t capacity;
  link_t table[0];
} hash_t;

extern hash_t* hash_create(int64_t capacity);
extern void hash_init(hash_t* h, int64_t capacity);
extern link_t* hash_insert(hash_t* map, link_t* k);
extern link_t* hash_del(hash_t* map, str_t* k);
extern link_t* hash_get(hash_t* map, str_t* k);
