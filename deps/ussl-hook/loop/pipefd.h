#ifndef USSL_HOOK_LOOP_PIPEFD_
#define USSL_HOOK_LOOP_PIPEFD_

typedef struct pipefd_t
{
  USSL_SOCK_COMMON;
  // when pipefd is readable, it needs to add new events(i.e. call epoll_ctl) via ep->fd
  ussl_eloop_t *ep;
} pipefd_t;

extern int pipefd_init(ussl_eloop_t *ep, pipefd_t *s, ussl_sf_t *sf, int fd);

#endif // USSL_HOOK_LOOP_PIPEFD_
