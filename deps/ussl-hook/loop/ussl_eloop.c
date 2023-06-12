int64_t ob_update_loop_ts();

int ussl_is_stop()
{
  return ATOMIC_LOAD(&ussl_is_stopped);
}
struct epoll_event *ussl_make_epoll_event(struct epoll_event *event, uint32_t event_flag, void *val)
{
  event->events = event_flag;
  event->data.ptr = val;
  return event;
}

int ussl_eloop_init(ussl_eloop_t *ep)
{
  ep->fd = epoll_create1(EPOLL_CLOEXEC);
  ussl_dlink_init(&ep->ready_link);
  return (ep->fd < 0) ? errno : 0;
}

int ussl_eloop_regist(ussl_eloop_t *ep, ussl_sock_t *s, uint32_t eflag)
{
  int err = 0;
  struct epoll_event event;
  uint32_t flag = eflag | EPOLLERR;
  s->mask = 0;
  s->ready_link.next = NULL;
  if (0 != libc_epoll_ctl(ep->fd, EPOLL_CTL_ADD, s->fd, ussl_make_epoll_event(&event, flag, s))) {
    err = -EIO;
    ussl_log_error("epoll_ctl add failed, epfd:%d, fd:%d, errno:%d", ep->fd, s->fd, errno);
  } else {
    ussl_log_info("sock regist: %p fd=%d", s, s->fd);
  }
  return err;
}

static void ussl_eloop_fire(ussl_eloop_t *ep, ussl_sock_t *s)
{
  if (!s->ready_link.next) {
    ussl_dlink_insert(&ep->ready_link, &s->ready_link);
  } else {
    ussl_sks(s, PENDING);
  }
}

static void ussl_eloop_refire(ussl_eloop_t *ep)
{
  const int maxevents = 512;
  struct epoll_event events[maxevents];
  int cnt = ob_epoll_wait(ep->fd, events, maxevents, 1000);
  for (int i = 0; i < cnt; i++) {
    ussl_sock_t *s = (ussl_sock_t *)events[i].data.ptr;
    s->mask |= events[i].events;
    ussl_eloop_fire(ep, s);
  }
}

static void ussl_sock_destroy(ussl_sock_t *s)
{
  ussl_dlink_delete(&s->ready_link);
  if (s->fd >= 0) {
    close(s->fd);
  }
  if (s->fty) {
    s->fty->destroy(s->fty, s);
  }
}

static void ussl_eloop_handle_sock_event(ussl_sock_t *s)
{
  int err = 0;
  if (0 == (err = s->handle_event(s))) {
    // yield
  } else if (EAGAIN == err) {
    if (ussl_skt(s, PENDING)) {
      ussl_skc(s, PENDING);
    } else {
      ussl_dlink_delete(&s->ready_link);
    }
  } else {
    ussl_sock_destroy(s);
  }
}

int ussl_eloop_run(ussl_eloop_t *ep)
{
  while (!ussl_is_stop()) {
    ob_update_loop_ts();
    ussl_eloop_refire(ep);
    ussl_dlink_for(&ep->ready_link, p) { ussl_eloop_handle_sock_event(ussl_structof(p, ussl_sock_t, ready_link)); }
    check_and_handle_timeout_event();
  }
  close(ep->fd);
  return 0;
}
