void* ref_alloc(int64_t sz, int mod) {
  int64_t* ref = (int64_t*)mod_alloc(sz + sizeof(int64_t), mod);
  if (ref) {
    *ref = 0;
    return ref + 1;
  }
  return NULL;
}

void ref_free(void* p) {
  if (NULL == p) return;
  int64_t* ref = (int64_t*)p - 1;
  if (0 == AAF(ref, -1)) {
    mod_free(ref);
  }
}
