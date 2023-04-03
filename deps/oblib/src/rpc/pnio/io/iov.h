#include <sys/uio.h>
inline void iov_set(struct iovec* iov, char* b, int64_t s) {
  iov->iov_base = b;
  iov->iov_len = s;
}

inline void iov_set_from_str(struct iovec* iov, str_t* s) {
  iov_set(iov, s->b, s->s);
}

inline void iov_consume_one(struct iovec* iov, int64_t bytes) {
  iov->iov_base = (void*)((uint64_t)iov->iov_base + bytes);
  iov->iov_len -= bytes;
}
