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

#define PNIO_TCP_SYNCNT 3
int check_connect_result(int fd) {
  int err = 0;
  int sys_err = 0;
  socklen_t errlen = sizeof(sys_err);
  if (0 != getsockopt(fd, SOL_SOCKET, SO_ERROR, &sys_err, &errlen)) {
    err = -EIO;
  } else if (EINPROGRESS == sys_err) {
    err = -EAGAIN;
  } else if (0 != sys_err) {
    err = -EIO;
    rk_warn("connect error: err=%d %s", sys_err, T2S(sock_fd, fd));
  }
  return err;
}

int async_connect(addr_t dest, uint64_t dispatch_id) {
  int fd = -1;
  struct sockaddr_storage sock_addr;
  const int ssl_ctx_id = 0;
  socklen_t ssl_ctx_id_len = sizeof(ssl_ctx_id);
  socklen_t dispatch_id_len = sizeof(dispatch_id);
  int send_negotiation_flag = 1;
  socklen_t send_negotiation_len = sizeof(send_negotiation_flag);
  ef((fd = socket(!dest.is_ipv6 ? AF_INET : AF_INET6, SOCK_STREAM, 0)) < 0);
  ef(make_fd_nonblocking(fd));
  set_tcpopt(fd, TCP_SYNCNT, PNIO_TCP_SYNCNT);
  ef(ussl_setsockopt(fd, SOL_OB_SOCKET, SO_OB_SET_CLIENT_GID, &dispatch_id, dispatch_id_len));
  ef(ussl_setsockopt(fd, SOL_OB_SOCKET, SO_OB_SET_CLIENT_SSL_CTX_ID, &ssl_ctx_id, ssl_ctx_id_len));
  ef(ussl_setsockopt(fd, SOL_OB_SOCKET, SO_OB_SET_SEND_NEGOTIATION_FLAG, &send_negotiation_flag, send_negotiation_len));
  ef(ussl_connect(fd, make_sockaddr(&sock_addr, dest), sizeof(sock_addr)) < 0 && EINPROGRESS != errno);
  set_tcp_nodelay(fd);
  return fd;
  el();
  if (fd >= 0) {
    ussl_close(fd);
  }
  return -1;
}

int listen_create(addr_t src) {
  int fd = -1;
  int err = 0;
  struct sockaddr_storage sock_addr;
  int ipv6_only_on = 1; /* Disable IPv4-mapped IPv6 addresses */
  if ((fd = socket(!src.is_ipv6 ? AF_INET : AF_INET6, SOCK_STREAM|SOCK_NONBLOCK|SOCK_CLOEXEC, 0)) < 0) {
    rk_warn("create socket failed, src=%s, errno=%d", T2S(addr, src), errno);
    err = PNIO_LISTEN_ERROR;
  } else if (set_tcp_reuse_addr(fd) != 0) {
    err = PNIO_LISTEN_ERROR;
    rk_warn("reuse_addr failed, src=%s, fd=%d, errno=%d", T2S(addr, src), fd, errno);
  } else if (set_tcp_reuse_port(fd) != 0) {
    err = PNIO_LISTEN_ERROR;
    rk_warn("reuse_port failed, src=%s, fd=%d, errno=%d", T2S(addr, src), fd, errno);
  } else if (src.is_ipv6 &&
             ussl_setsockopt(fd, IPPROTO_IPV6, IPV6_V6ONLY, &ipv6_only_on, sizeof(ipv6_only_on)) != 0) {
    err = PNIO_LISTEN_ERROR;
    rk_warn("set sock opt IPV6_V6ONLY failed, src=%s, fd=%d, errno=%d", T2S(addr, src), fd, errno);
  } else if (bind(fd,  (const struct sockaddr*)make_sockaddr(&sock_addr, src), sizeof(sock_addr)) != 0) {
    err = PNIO_LISTEN_ERROR;
    rk_warn("bind failed, src=%s, fd=%d, errno=%d", T2S(addr, src), fd, errno);
  } else if (ussl_listen(fd, 1024) != 0) {
    err = PNIO_LISTEN_ERROR;
    rk_warn("listen failed, src=%s, fd=%d, errno=%d", T2S(addr, src), fd, errno);
  }
  if (err != 0 && fd >= 0) {
    ussl_close(fd);
    fd = -1;
  }
  return fd;
}

int tcp_accept(int fd) {
  return accept4(fd, NULL, NULL, SOCK_NONBLOCK|SOCK_CLOEXEC);
}

int set_tcp_reuse_addr(int fd) {
  int flag = 1;
  return ussl_setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &flag, sizeof(flag));
}

int set_tcp_reuse_port(int fd) {
  int flag = 1;
  return ussl_setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &flag, sizeof(flag));
}

int set_tcp_linger_on(int fd) {
  struct linger so_linger;
  so_linger.l_onoff = 1;
  so_linger.l_linger = 0;
  return setsockopt(fd, SOL_SOCKET, SO_LINGER, &so_linger, sizeof so_linger);
}

int set_tcp_nodelay(int fd) {
  int flag = 1;
  return setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, (char *)&flag, sizeof(flag));
}

int set_tcpopt(int fd, int option, int value) {
  int ret = setsockopt(fd, IPPROTO_TCP, option, (const void *) &value, sizeof(value));
  if (ret < 0) {
    rk_warn("IPPROTO_TCP fd: %d, errno: %d, option: %d, value: %d", fd, errno, option, value);
  }
  return ret;
}

int set_sock_opt(int fd, int option, int value)
{
  int ret = setsockopt(fd, SOL_SOCKET, option, (const void *)&value, sizeof(value));
  if (ret < 0) {
    rk_warn("set socket level option failed, fd: %d, errno: %d, option: %d, value: %d", fd, errno, option, value);
  }
  return ret;
}

void update_socket_keepalive_params(int fd, int64_t user_timeout) {
  int tcp_keepalive = (user_timeout > 0) ? 1: 0;
  int tcp_keepidle = user_timeout/5000000;
  if (tcp_keepidle < 1) {
    tcp_keepidle = 1;
  }
  int tcp_keepintvl = tcp_keepidle;
  int tcp_keepcnt = 5;
  int tcp_user_timeout = (tcp_keepcnt + 1) * tcp_keepidle * 1000 - 100;
  if (1 == tcp_keepalive) {
    if (set_sock_opt(fd, SO_KEEPALIVE, 1)) {
      rk_warn("set SO_KEEPALIVE error: %d, fd=%d\n", errno, fd);
    } else {
      ignore_ret_value(set_tcpopt(fd, TCP_KEEPIDLE, tcp_keepidle));
      ignore_ret_value(set_tcpopt(fd, TCP_KEEPINTVL, tcp_keepintvl));
      ignore_ret_value(set_tcpopt(fd, TCP_KEEPCNT, tcp_keepcnt)); // TCP_USER_TIMEOUT will override keepalive to determine when to close a connection due to keepalive failure
      ignore_ret_value(set_tcpopt(fd, TCP_USER_TIMEOUT, tcp_user_timeout));
    }
  } else {
    if (set_tcpopt(fd, SO_KEEPALIVE, 0)) {
      rk_warn("disable SO_KEEPALIVE error: %d, fd=%d\n", errno, fd);
    } else {
      ignore_ret_value(set_tcpopt(fd, TCP_USER_TIMEOUT, 0));
    }
  }
}

int set_tcp_recv_buf(int fd, int size) {
  return setsockopt(fd, SOL_SOCKET, SO_RCVBUF, (const char*)&size, sizeof(size));
}

int set_tcp_send_buf(int fd, int size) {
  return setsockopt(fd, SOL_SOCKET, SO_SNDBUF, (const char*)&size, sizeof(size));
}

const char* sock_fd_str(format_t* f, int fd) {
  format_t tf;
  addr_t local = get_local_addr(fd);
  addr_t remote = get_remote_addr(fd);
  format_init(&tf, sizeof(tf.buf));
  return format_sf(f, "fd:%d:local:%s:remote:%s", fd, addr_str(&tf, local), addr_str(&tf, remote));
}
