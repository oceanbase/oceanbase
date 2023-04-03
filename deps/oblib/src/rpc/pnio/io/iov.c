extern void iov_set(struct iovec* iov, char* b, int64_t s);
extern void iov_set_from_str(struct iovec* iov, str_t* s);
extern void iov_consume_one(struct iovec* iov, int64_t bytes);
