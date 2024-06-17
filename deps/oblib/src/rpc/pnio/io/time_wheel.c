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

static int64_t tw_align_time(int64_t us) { return us & ~(TIME_WHEEL_SLOT_INTERVAL - 1); }
static dlink_t* tw_get_slot(time_wheel_t* tw, int64_t us) { return tw->slot + (us/TIME_WHEEL_SLOT_INTERVAL) % TIME_WHEEL_SLOT_NUM; }
void tw_init(time_wheel_t* tw, timer_cb_t* cb) {
  for(int i = 0; i < TIME_WHEEL_SLOT_NUM; i++) {
    dlink_init(tw->slot + i);
  }
  tw->cb = cb;
  tw->finished_us = tw_align_time(rk_get_corse_us());
}

static int64_t tw_get_expire_us(dlink_t* l) { return *(int64_t*)(l + 1); }
static void tw_fire(time_wheel_t* tw, dlink_t* l) { tw->cb(tw, l); }

static int tw_check_node(time_wheel_t* tw, dlink_t* l) {
  if (tw_get_expire_us(l) < tw->finished_us) {
    dlink_delete(l);
    tw_fire(tw, l);
    return ETIMEDOUT;
  }
  return 0;
}

int tw_regist(time_wheel_t* tw, dlink_t* l) {
  dlink_insert(tw_get_slot(tw, tw_get_expire_us(l)), l);
  return tw_check_node(tw, l);
}

static void tw_sweep_slot(time_wheel_t* tw) {
  dlink_t* slot = tw_get_slot(tw, tw->finished_us - TIME_WHEEL_SLOT_INTERVAL);
  dlink_for(slot, p) {
    tw_check_node(tw, p);
  }
}

void tw_check(time_wheel_t* tw) {
  int64_t cur_us = rk_get_corse_us();
  while(tw->finished_us < cur_us) {
    tw->finished_us += TIME_WHEEL_SLOT_INTERVAL;
    tw_sweep_slot(tw);}
}

extern int64_t pnio_keepalive_timeout;
bool pn_server_in_black(struct sockaddr* sa) {return 0;}
#ifndef SERVER_IN_BLACK
#define SERVER_IN_BLACK(sa) pn_server_in_black(sa)
#endif
void keepalive_check(pktc_t* io) {
  static __thread int time_count = 0;
  time_count ++;
  if (time_count < (int)(1000000/TIME_WHEEL_SLOT_INTERVAL)) {
  } else {
    time_count = 0;
    // walks through pktc_t skmap, refresh tcp keepalive params and check server keepalive
    dlink_for(&io->sk_list, p) {
      pktc_sk_t *sk = structof(p, pktc_sk_t, list_link);
      struct sockaddr_storage sock_addr;
      if (sk->conn_ok && SERVER_IN_BLACK((struct sockaddr*)make_sockaddr(&sock_addr, sk->dest))) {
        // mark the socket as waiting for destroy
        rk_info("socket dest server in blacklist, it will be destroyed, sock=(ptr=%p,dest=%d:%d)", sk, sk->dest.ip, sk->dest.port);
        sk->mask |= EPOLLERR;
        eloop_fire(io->ep, (sock_t*)sk);
      } else if(sk->user_keepalive_timeout != pnio_keepalive_timeout) {
        rk_info("user_keepalive_timeout has been reset, sock=(ptr=%p,dest=%d:%d), old=%ld, new=%ld",
          sk, sk->dest.ip, sk->dest.port, sk->user_keepalive_timeout, pnio_keepalive_timeout);
        update_socket_keepalive_params(sk->fd, pnio_keepalive_timeout);
        sk->user_keepalive_timeout = pnio_keepalive_timeout;
      }
    }
  }
}

static int timerfd_handle_tw(timerfd_t* s) {
  time_wheel_t* tw = (time_wheel_t*)(s + 1);
  tw_check(tw);
  evfd_drain(s->fd);
  keepalive_check(structof(s, pktc_t, cb_timerfd));
  return EAGAIN;
}

int timerfd_init_tw(eloop_t* ep, timerfd_t* s) {
  int err = 0;
  ef(err = timerfd_init(ep, s, (handle_event_t)timerfd_handle_tw));
  ef(err = timerfd_set_interval(s, TIME_WHEEL_SLOT_INTERVAL));
  el();
  return err;
}
