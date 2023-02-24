#include <sys/timerfd.h>
typedef struct timerfd_t {
  SOCK_COMMON;
} timerfd_t;

extern int timerfd_set_interval(timerfd_t* s, int64_t interval);
extern int timerfd_init(eloop_t* ep, timerfd_t* s, handle_event_t handle);
