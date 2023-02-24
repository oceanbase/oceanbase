#include <linux/futex.h>
#ifdef futex
#define rk_futex(...) futex(__VA_ARGS__)
#else
#define rk_futex(...) ((int)syscall(SYS_futex, __VA_ARGS__))
#endif

int rk_futex_wake(int *p, int val) {
  int err = 0;
  if (0 != rk_futex((uint *)p, FUTEX_WAKE_PRIVATE, val, NULL)) {
    err = errno;
  }
  return err;
}

int rk_futex_wait(int *p, int val, const struct timespec *timeout) {
  int err = 0;
  if (0 != rk_futex((uint *)p, FUTEX_WAIT_PRIVATE, val, timeout)) {
    err = errno;
  }
  return err;
}
