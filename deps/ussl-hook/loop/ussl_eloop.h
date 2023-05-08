#ifndef USSL_HOOK_LOOP_ELOOP_
#define USSL_HOOK_LOOP_ELOOP_

typedef struct ussl_eloop_t
{
  int fd;
  ussl_dlink_t ready_link;
} ussl_eloop_t;

extern int ussl_eloop_init(ussl_eloop_t *ep);
extern int ussl_eloop_run(ussl_eloop_t *ep);
extern int ussl_eloop_regist(ussl_eloop_t *ep, ussl_sock_t *s, uint32_t eflag);

#endif // USSL_HOOK_LOOP_ELOOP_
