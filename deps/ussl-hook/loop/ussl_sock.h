#ifndef USSL_HOOK_LOOP_SOCK_
#define USSL_HOOK_LOOP_SOCK_

struct ussl_sock_t;
typedef int (*ussl_handle_event_t)(struct ussl_sock_t *);

#define USSL_SOCK_COMMON            \
  struct ussl_sf_t *fty;            \
  ussl_handle_event_t handle_event; \
  ussl_dlink_t ready_link;          \
  int fd;                           \
  uint32_t mask;                    \
  uint8_t conn_ok : 1

typedef struct ussl_sock_t
{
  USSL_SOCK_COMMON;
} ussl_sock_t;

#define USSL_SOCK_FACTORY_COMMON                            \
  struct ussl_sock_t *(*create)(struct ussl_sf_t *);        \
  void (*destroy)(struct ussl_sf_t *, struct ussl_sock_t *);

typedef struct ussl_sf_t
{
  USSL_SOCK_FACTORY_COMMON;
} ussl_sf_t;

#ifndef EPOLLPENDING
#define EPOLLPENDING EPOLLONESHOT
#endif

inline void ussl_skset(ussl_sock_t *s, uint32_t m) { s->mask |= m; }
inline void ussl_skclear(ussl_sock_t *s, uint32_t m) { s->mask &= ~m; }
inline int ussl_sktest(ussl_sock_t *s, uint32_t m) { return s->mask & m; }
#define ussl_sks(s, flag) ussl_skset((ussl_sock_t *)s, EPOLL##flag)
#define ussl_skt(s, flag) ussl_sktest((ussl_sock_t *)s, EPOLL##flag)
#define ussl_skc(s, flag) ussl_skclear((ussl_sock_t *)s, EPOLL##flag)

extern void ussl_sf_init(ussl_sf_t *sf, void *create, void *destroy);
extern void ussl_sk_init(ussl_sock_t *s, ussl_sf_t *sf, void *handle_event, int fd);

#endif // USSL_HOOK_LOOP_SOCK_
