#define RL_SLEEP_TIME_US 100000 // determine the min sleep time when the socket is rate-limited
typedef struct rl_timerfd_t {
  SOCK_COMMON;
} rl_timerfd_t;

typedef struct rl_impl_t {
  dlink_t ready_link;
  int64_t bw;
  rl_timerfd_t rlfd;
} rl_impl_t;

void rl_sock_push(rl_impl_t* rl, sock_t* sk);