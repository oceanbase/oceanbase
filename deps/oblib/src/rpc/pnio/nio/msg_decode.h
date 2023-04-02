inline int64_t msg_decode(const char* b, int64_t s) {
  if (s < 8) {
    return 8;
  } else {
    return *(int64_t*)b;
  }
}

inline uint64_t msg_get_id(const char* b) {
  return *((uint64_t*)b + 1);
}
