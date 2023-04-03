#include <sys/eventfd.h>
typedef struct evfd_t {
  SOCK_COMMON;
} evfd_t;

extern void evfd_signal(int fd);
extern int evfd_drain(int fd);
extern int evfd_init(eloop_t* ep, evfd_t* s, handle_event_t handle);
