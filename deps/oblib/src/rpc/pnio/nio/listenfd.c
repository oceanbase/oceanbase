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

static int do_accept(int fd, int is_pipe)
{
  int ret = -1;
  if (is_pipe) {
    if (read(fd, &ret, sizeof(ret)) <= 0) {
      ret = -1;
    }
  } else {
    ret = tcp_accept(fd);
  }
  return ret;
}

void on_accept(int fd, sf_t* sf, eloop_t* ep)
{
  int err = 0;
  bool add_succ = false;
  sock_t* ns = sf->create(sf);
  if (NULL != ns) {
    set_tcp_nodelay(fd);
    update_socket_keepalive_params(fd, pnio_keepalive_timeout);
    ns->fd = fd;
    ns->fty = sf;
    ns->peer = get_remote_addr(fd);
    if (eloop_regist(ep, ns, EPOLLIN | EPOLLOUT) == 0) {
      add_succ = true;
      rk_info("accept new connection, ns=%p, fd=%s", ns, T2S(sock_fd, ns->fd));
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

int listenfd_handle_event(listenfd_t* s) {
  int err = 0;
  int fd = do_accept(s->fd, s->is_pipe);
  if (fd >= 0) {
    on_accept(fd, s->sf, s->ep);
  } else {
    if (EINTR == errno) {
      // do nothing
    } else if (EAGAIN == errno || EWOULDBLOCK == errno) {
      s->mask &= ~EPOLLIN;
      err = EAGAIN;
    } else {
      err = EIO;
    }
    rk_warn("do_accept failed, err=%d, errno=%d, fd=%d", err, errno, fd);
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
