extern int sk_read(sock_t* s, char* buf, size_t size, ssize_t* rbytes);
extern int sk_readv(sock_t* s, struct iovec* iov, int cnt, ssize_t* rbytes);
extern int sk_write(sock_t* s, const char* buf, size_t size, ssize_t* wbytes);
extern int sk_writev(sock_t* s, struct iovec* iov, int cnt, ssize_t* wbytes);
