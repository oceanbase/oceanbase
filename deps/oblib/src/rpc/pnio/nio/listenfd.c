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

static listenfd_dispatch_t do_accept(int fd, int is_pipe)
{
  int ret = 0;
  listenfd_dispatch_t t = {
    .fd = -1,
    .tid = -1,
    .sock_ptr = NULL
  };
  if (is_pipe) {
    if (read(fd, (char*)&t, sizeof(listenfd_dispatch_t)) <= 0) {
      t.fd = -1;
    }
  } else {
    t.fd = tcp_accept(fd);
  }
  return t;
}

void on_accept(listenfd_dispatch_t* lfd, sf_t* sf, eloop_t* ep)
{
  int err = 0;
  bool add_succ = false;
  sock_t* ns = sf->create(sf);
  int fd = lfd->fd;
  if (NULL != ns) {
    set_tcp_nodelay(fd);
    update_socket_keepalive_params(fd, pnio_keepalive_timeout);
    ns->fd = fd;
    ns->fty = sf;
    ns->peer = get_remote_addr(fd);
    ns->tid = lfd->tid;
    if (eloop_regist(ep, ns, EPOLLIN | EPOLLOUT) == 0) {
      add_succ = true;
      char sock_fd_buf[PNIO_NIO_FD_ADDR_LEN] = {'\0'};
      rk_info("accept new connection, ns=%p, fd=%s, tid=%d",
          ns, sock_fd_str(ns->fd, sock_fd_buf, sizeof(sock_fd_buf)), ns->tid);
    } else {
      err = -EIO;
    }
  } else {
    err = -ENOMEM;
  }
  if (!add_succ) {
    if (fd >= 0) {
      ussl_close(fd);
    }
    if (NULL != ns) {
      sf->destroy(sf, ns);
    }
    rk_error("accept newfd fail");
  }
}

void on_dispatch(listenfd_dispatch_t* lfd, sf_t* sf, eloop_t* ep)
{
  int err = 0;
  int fd = lfd->fd;
  pkts_sk_t* old_sk = (pkts_sk_t*)lfd->sock_ptr;

  bool add_succ = false;
  sock_t* ns = sf->create(sf);
  if (NULL != ns) {
    ns->fd = fd;
    ns->fty = sf;
    ns->peer = get_remote_addr(fd);
    ns->tid = lfd->tid;
    if (eloop_regist(ep, ns, EPOLLIN | EPOLLOUT) == 0) {
      add_succ = true;
      char sock_fd_buf[PNIO_NIO_FD_ADDR_LEN] = {'\0'};
      // copy sock stuct
      pkts_sk_t* new_sk = (pkts_sk_t*)ns;
      new_sk->relocate_status = SOCK_DISABLE_RELOCATE;
      new_sk->processing_cnt = old_sk->processing_cnt;
      rk_info("[socket_relocation] accept new connection, s=%p,%s, tid=%d, old_sk=%p",
              new_sk, sock_fd_str(ns->fd, sock_fd_buf, sizeof(sock_fd_buf)), ns->tid, old_sk);
      wq_move(&new_sk->wq, &old_sk->wq);
      memcpy(&new_sk->ib, &old_sk->ib, sizeof(old_sk->ib));
      new_sk->sk_diag_info.doing_cnt = old_sk->sk_diag_info.doing_cnt;
      old_sk->relocate_sock_id = new_sk->id;
      // To avoid dropping events, add socket to handle list
      eloop_fire(ep, ns);
    } else {
      err = -EIO;
    }
  } else {
    err = -ENOMEM;
  }
  if (!add_succ) {
    if (fd >= 0) {
      ussl_close(fd);
    }
    if (NULL != ns) {
      sf->destroy(sf, ns);
    }
    rk_error("accept dispatch fd fail, err=%d", err);
  }
  old_sk->relocate_status = SOCK_RELOCATING;
  rk_futex_wake(&old_sk->relocate_status, 1);
}

int listenfd_handle_event(listenfd_t* s) {
  int err = 0;
  listenfd_dispatch_t lfd = do_accept(s->fd, s->is_pipe);
  if (lfd.fd >= 0 ) {
    if (NULL != lfd.sock_ptr) {
      // sock is dispathed from other pn thread
      on_dispatch(&lfd, s->sf, s->ep);
    } else {
      on_accept(&lfd, s->sf, s->ep);
    }
  } else {
    if (EINTR == errno) {
      // do nothing
    } else if (EAGAIN == errno || EWOULDBLOCK == errno) {
      s->mask &= ~EPOLLIN;
      err = EAGAIN;
    } else {
      err = EIO;
    }
    rk_warn("do_accept failed, err=%d, errno=%d, fd=%d", err, errno, lfd.fd);
  }
  return err;
}

int listenfd_init(eloop_t* ep, listenfd_t* s, sf_t* sf, int fd) {
  sk_init((sock_t*)s, NULL, (void*)listenfd_handle_event, fd);
  s->is_pipe = is_pipe(fd);
  s->ep = ep;
  s->sf = sf;
  ef(s->fd < 0);
  ef(eloop_regist(ep, (sock_t*)s, EPOLLIN) != 0);
  rk_info("listen succ: %d", fd);
  return 0;
  el();
  int err = PNIO_LISTEN_ERROR;
  rk_error("listen fd init fail: fd=%d", s->fd);
  if (s->fd >= 0) {
    close(s->fd);
  }
  return EIO;
}
