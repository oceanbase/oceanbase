typedef struct pktc_wait_t {
  int32_t done;
  uint32_t sz;
  char resp[];
} pktc_wait_t;

inline void pktc_wait_cb(const char* b, int64_t s, void* arg) {
  pktc_wait_t* w = (pktc_wait_t*)arg;
  memcpy(w->resp, b, s);
  w->sz = s;
  STORE(&w->done, 1);
  rk_futex_wake(&w->done, 1);
}

inline char* pktc_wait(pktc_wait_t* w, int64_t* sz) {
  while(!w->done) {
    rk_futex_wait(&w->done, 0, NULL);
  }
  if (NULL != sz) {
    *sz = w->sz;
  }
  return w->resp;
}
