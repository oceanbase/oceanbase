/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

struct epoll_event *__make_epoll_event(struct epoll_event *event, uint32_t event_flag, void* val) {
  event->events = event_flag;
  event->data.ptr = val;
  return event;
}

int eloop_init(eloop_t* ep) {
  ep->fd = epoll_create1(EPOLL_CLOEXEC);
  dlink_init(&ep->ready_link);
  // dlink_init(&ep->rl_ready_link);
  return (ep->fd < 0)? errno: 0;
}

int eloop_unregist(eloop_t* ep, sock_t* s)
{
  int err = 0;
  if (0 != epoll_ctl(ep->fd, EPOLL_CTL_DEL, s->fd, NULL)) {
    err = -EIO;
  } else {
    dlink_delete(&s->ready_link);
  }
  return err;
}

int eloop_regist(eloop_t* ep, sock_t* s, uint32_t eflag) {
  int err = 0;
  struct epoll_event event;
  uint32_t flag = eflag | EPOLLERR | EPOLLET;
  s->mask = 0;
  s->ready_link.next = NULL;
  if (0 != ussl_epoll_ctl(ep->fd, EPOLL_CTL_ADD, s->fd, __make_epoll_event(&event, flag, s))) {
    err = -EIO;
  } else {
    s->ep_fd = ep->fd;
    rk_info("sock regist: %p fd=%d", s, s->fd);
  }
  return err;
}

void eloop_fire(eloop_t* ep, sock_t* s) {
  if (!s->ready_link.next) {
    dlink_insert(&ep->ready_link, &s->ready_link);
  } else {
    sks(s, PENDING);
  }
}

static void eloop_refire(eloop_t* ep, int64_t timeout) {
  const int maxevents = 512;
  struct epoll_event events[maxevents];
  int cnt = ob_epoll_wait(ep->fd, events, maxevents, timeout);
  for(int i = 0; i < cnt; i++) {
    sock_t* s = (sock_t*)events[i].data.ptr;
    s->mask |= events[i].events;
    rk_debug("eloop fire: %p mask=%x", s, s->mask);
    eloop_fire(ep, s);
  }
}

static void sock_destroy(sock_t* s) {
  dlink_delete(&s->ready_link);
  int err = 0;
  if (s->ep_fd >= 0) {
    err = epoll_ctl(s->ep_fd, EPOLL_CTL_DEL, s->fd, NULL);
    if (0 != err) {
      rk_warn("epoll_ctl delete fd faild, s=%p, s->fd=%d, errno=%d", s, s->fd, errno);
    }
  }
  if (s->fd >= 0) {
    err = ussl_close(s->fd);
    if (0 != err) {
      rk_warn("close sock fd faild, s=%p, s->fd=%d, errno=%d", s, s->fd, errno);
    }
  }
  if (s->fty) {
    s->fty->destroy(s->fty, s);
  }
}

static void eloop_handle_sock_event(sock_t* s) {
  int err = 0;
  if (skt(s, ERR) || skt(s, HUP)) {
    rk_info("sock destroy: sock=%p, connection=%s, s->mask=0x%x", s, T2S(sock_fd, s->fd), s->mask);
    sock_destroy(s);
  } else if (0 == (err = s->handle_event(s))) {
    // yield
  } else if (EAGAIN == err) {
    if (skt(s, PENDING)) {
      skc(s, PENDING);
    } else {
      rk_debug("sock sleep: %p", s);
      dlink_delete(&s->ready_link);
    }
  } else {
    rk_info("sock destroy: sock=%p, connection=%s, err=%d", s, T2S(sock_fd, s->fd), err);
    sock_destroy(s);
  }
}

int eloop_thread_run(eloop_t** udata) {
  return eloop_run(*udata);
}

int eloop_run(eloop_t* ep) {
  pn_comm_t* pn = get_current_pnio();
  while(!ATOMIC_LOAD(&pn->is_stop_)) {
    int64_t epoll_timeout = 1000;
    ob_update_loop_ts();
    if (ep->ready_link.next != &ep->ready_link) {
      epoll_timeout = 0; // make sure all events handled when progarm is blocked in epoll_ctl
    }
    eloop_refire(ep, epoll_timeout);
    PNIO_DELAY_WARN(reset_eloop_time_stat());
    PNIO_DELAY_WARN(int64_t start_us = rk_get_corse_us());
    dlink_for(&ep->ready_link, p) {
      eloop_handle_sock_event(structof(p, sock_t, ready_link));
    }

    PNIO_DELAY_WARN(eloop_delay_warn(start_us, ELOOP_WARN_US));
    if (unlikely(NULL != pn && 0 == pn->tid && PNIO_REACH_TIME_INTERVAL(1000000))) {
      static __thread uint64_t last_rx_bytes = 0;
      static __thread uint64_t last_time = 0;
      uint64_t rx_bytes = pn_get_rxbytes(pn->gid);
      int64_t cur_time_us = rk_get_us();
      uint64_t bytes = rx_bytes >= last_rx_bytes? rx_bytes - last_rx_bytes : 0xffffffff - last_rx_bytes + rx_bytes;
      double bw = ((double)(bytes)) / (cur_time_us - last_time) * 0.95367431640625;
      rk_info("[ratelimit] time: %8ld, bytes: %ld, bw: %8lf MB/s, add_ts: %ld, add_bytes: %ld\n", cur_time_us, rx_bytes, bw, cur_time_us - last_time, rx_bytes - last_rx_bytes);
      last_rx_bytes = rx_bytes;
      last_time = cur_time_us;
    }
  }
  pn_release(pn);
  return 0;
}
