typedef struct ibuffer_t {
  int mod;
  int64_t cur_ref_;
  char* limit;
  char* b;
  char* s;
  char* e;
} ibuffer_t;

extern void ib_init(ibuffer_t* ib, int mod);
extern void ib_consumed(ibuffer_t* ib, int64_t sz);
extern int sk_read_with_ib(void** ret, sock_t* s, ibuffer_t* ib, int64_t sz);
extern void* ib_ref(ibuffer_t* ib);
extern void ib_ref_free(void* p);
extern void ib_destroy(ibuffer_t* ib);
