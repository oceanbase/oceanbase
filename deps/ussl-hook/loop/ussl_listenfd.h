#ifndef USSL_HOOK_LOOP_LISTENFD_
#define USSL_HOOK_LOOP_LISTENFD_

typedef struct ussl_listenfd_t
{
  USSL_SOCK_COMMON;
  ussl_eloop_t *ep;
} ussl_listenfd_t;

extern int ussl_listenfd_init(ussl_eloop_t *ep, ussl_listenfd_t *s, ussl_sf_t *sf, int fd);
#endif // USSL_HOOK_LOOP_LISTENFD_
