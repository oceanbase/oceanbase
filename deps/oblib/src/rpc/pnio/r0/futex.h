extern int rk_futex_wake(int *p, int val);
extern int rk_futex_wait(int *p, int val, const struct timespec *timeout);
