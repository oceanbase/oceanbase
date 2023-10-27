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

extern int pktc_sk_handle_event(pktc_sk_t* s);
extern int pkts_sk_handle_event(pkts_sk_t* s);
extern int pktc_sk_consume(pktc_sk_t* s, int64_t time_limit, int64_t* avail_bytes);
extern int pkts_sk_consume(pkts_sk_t* s, int64_t time_limit, int64_t* avail_bytes);

static int eloop_rl_fire(rl_impl_t* rl_impl, int64_t wakeup_time_us) {
  int timer_fd = rl_impl->rlfd.fd;
  if (wakeup_time_us <= 0) {
    wakeup_time_us = 1; // wakeup_time_us is expired, we should wakeup timerfd as soon
  }
  struct itimerspec it = {{0, 0}, {wakeup_time_us/1000000, 1000 * (wakeup_time_us % 1000000)}};
  return timerfd_settime(timer_fd, 0, &it, NULL)? errno: 0;
}
void rl_sock_push(rl_impl_t* rl_impl, sock_t* sk) {
  if (NULL != sk->rl_ready_link.next) {
    // socket has already in rl queue
  } else {
    bool empty = false;
    if (dlink_is_empty(&rl_impl->ready_link)) {
      empty = true;
    }
    dlink_insert_before(&rl_impl->ready_link, &sk->rl_ready_link);
    if (empty) {
      eloop_rl_fire(rl_impl, 0);
    }
  }
}

static int handle_rl_sock_read_event(eloop_t* ep, sock_t* s, rl_impl_t* rl, int64_t* avail_bytes) {
  int err = 0;
  if ((handle_event_t)pkts_sk_handle_event == s->handle_event) {
    err = pkts_sk_consume((pkts_sk_t*)s, get_epoll_handle_time_limit(), avail_bytes);
  } else if ((handle_event_t)pktc_sk_handle_event == s->handle_event) {
    err = pktc_sk_consume((pktc_sk_t*)s, get_epoll_handle_time_limit(), avail_bytes);
  } else {
    err = -EIO;
    rk_error("[ratelimit] unexpect socket struct:%p, s->handle_event=%p", s, s->handle_event);
  }
  if (err == EAGAIN && *avail_bytes <= 0) {
    err = 0; // not real EAGIAN
  }
  return err;
}

static int rl_timerfd_handle_event(rl_timerfd_t* s) {
  int err = EAGAIN;
  evfd_drain(s->fd);
  rl_impl_t* rl = structof(s, rl_impl_t, rlfd);
  eloop_t* ep = structof(rl, eloop_t, rl_impl);
  dlink_t* rl_queue = &rl->ready_link;
  int64_t read_start_time = rk_get_us();
  pn_comm_t* pn = get_current_pnio();
  pn_grp_comm_t* pn_grp = pn->pn_grp;
  if (read_start_time < pn->next_readable_time) {
    // rate-limitted
  } else if ((rl->bw = LOAD(&pn_grp->rx_bw)) <= 0) {
    // not to read any data and check pn_grp->rx_bw again after 1 second
    pn->next_readable_time = read_start_time + 1000000;
  } else if (RATE_UNLIMITED == rl->bw) {
    // socket should be handled in normal queue when ratelimit disabled
    dlink_for(rl_queue, p) {
      sock_t* read_sock = structof(p, sock_t, rl_ready_link);
      dlink_delete(&read_sock->rl_ready_link);
      eloop_fire(ep, (sock_t*)read_sock);
    }
  } else {
    // read data
    int64_t avail_bytes = rl->bw * 0.1 + 1;
    if (avail_bytes > 65536) {
      avail_bytes = 65536; // read only up to 64k once
    }
    int64_t temp_bytes = avail_bytes;
    int rl_err = 0;
    dlink_t* last_dlink = rl_queue->prev;
    dlink_for(rl_queue, p) {
      if (avail_bytes <= 0) {
        break;
      }
      sock_t* read_sock = structof(p, sock_t, rl_ready_link);
      rl_err = handle_rl_sock_read_event(ep, read_sock, rl, &avail_bytes);
      // deal with handle error
      if (0 == rl_err) {
        // the socket is still readable, move it to the end of rl queue
        dlink_delete(&read_sock->rl_ready_link);
        dlink_insert_before(rl_queue, &read_sock->rl_ready_link);
      } else if (EAGAIN == rl_err) {
        // the socket is no data to read
        dlink_delete(&read_sock->rl_ready_link);
      } else {
        // mark the socket as EPOLLERR and it will be deleted in normal queue
        sks(read_sock, ERR);
        dlink_delete(&read_sock->rl_ready_link);
        eloop_fire(ep, (sock_t*)read_sock);
      }
      if (p == last_dlink) {
        break;
      }
    }
    // set sleep time
    temp_bytes = temp_bytes - avail_bytes;
    if (temp_bytes <= 0) {
      // all sockets are unreadable, do nothing
      rk_info("[ratelimit] all sockets are unreadable, now the rl_link queue is empty: %d", dlink_is_empty(rl_queue));
    } else {
      int64_t wait_time_us = (temp_bytes * 1000000)/rl->bw + 1;
      rk_debug("[ratelimit] wait_time_us=%ld, next_readable_time=%ld, temp_bytes=%ld, avail_bytes=%ld", wait_time_us, pn->next_readable_time, temp_bytes, avail_bytes);
      pn->next_readable_time = AAF(&pn_grp->next_readable_time, wait_time_us);
      int64_t read_finish_time = rk_get_us();
      if (pn->next_readable_time < read_finish_time - 1000000) { // 1s time compensation
        rk_info("[ratelimit] update global next_readable_time as %ld, pn->next_readable_time=%ld", read_finish_time, pn->next_readable_time);
        STORE(&pn_grp->next_readable_time, read_finish_time);
      }
    }
  }
  if (!dlink_is_empty(rl_queue)) {
    int64_t sleep_time = pn->next_readable_time - rk_get_us();
    eloop_rl_fire(rl, sleep_time);
  }
  return err;
}

int eloop_rl_init(eloop_t* ep, rl_impl_t* rl_impl) {
  int err = 0;
  dlink_init(&rl_impl->ready_link);
  rl_impl->bw = RATE_UNLIMITED;
  rl_timerfd_t* s = &rl_impl->rlfd;
  sk_init((sock_t*)s, NULL, (void*)rl_timerfd_handle_event, timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK|TFD_CLOEXEC));
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