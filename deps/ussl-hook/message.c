# define USSL_DEFAULT_VERSION 1

int send_negotiation_message(int fd, const char *b, int sz)
{
  int err = 0;
  char buf[USSL_BUF_LEN];
  negotiation_head_t *h = (typeof(h))buf;
  if (sz + sizeof(*h) > sizeof(buf)) {
    err = -EINVAL;
  } else {
    h->magic = NEGOTIATION_MAGIC;
    h->version = USSL_DEFAULT_VERSION;
    h->len = sz;
    memcpy(h + 1, b, sz);
    ssize_t wbytes = 0;
    while ((wbytes = libc_write(fd, buf, sizeof(*h) + sz)) < 0 && EINTR == errno)
      ;
    if (wbytes != sz + sizeof(*h)) {
      err = -EIO;
    }
  }
  return err;
}