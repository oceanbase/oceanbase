int timerfd_set_interval(timerfd_t* s, int64_t interval) {
  rk_info("set interval: %ld", interval);
  struct itimerspec it = {{interval/1000000, 1000 * (interval % 1000000)}, {0, 1}};
  return timerfd_settime(s->fd, 0, &it, NULL)? errno: 0;
}

int timerfd_init(eloop_t* ep, timerfd_t* s, handle_event_t handle) {
  int err = 0;
  sk_init((sock_t*)s, NULL, (void*)handle, timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK|TFD_CLOEXEC));
  if (s->fd < 0) {
    err = EIO;
  } else {
    err = eloop_regist(ep, (sock_t*)s, EPOLLIN);
  }
  if (0 != err && s->fd >= 0) {
    close(s->fd);
  }
  return err;
}
