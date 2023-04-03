typedef struct str_t {
  int64_t s;
  char b[0];
} str_t;
inline int64_t str_hash(str_t* s) { return fasthash64(s->b, s->s, 0); }
inline int str_cmp(str_t* s1, str_t* s2) {
  int cmp = memcmp(s1->b, s2->b, rk_min(s1->s, s2->s));
  if (!cmp) return cmp;
  return s1->s - s2->s;
}
