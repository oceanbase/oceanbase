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
    rk_error("connect error: err=%d %s", sys_err, T2S(sock_fd, fd));
  }
  return err;
}

int async_connect(addr_t dest) {
  int fd = -1;
  struct sockaddr_in sin;
  ef((fd = socket(AF_INET, SOCK_STREAM, 0)) < 0);
  ef(make_fd_nonblocking(fd));
  set_tcpopt(fd, TCP_SYNCNT, PNIO_TCP_SYNCNT);
  ef(connect(fd, (struct sockaddr*)make_sockaddr(&sin, dest), sizeof(sin)) < 0 && EINPROGRESS != errno);
  set_tcp_nodelay(fd);
  return fd;
  el();
  if (fd >= 0) {
    close(fd);
  }
  return -1;
}

int listen_create(addr_t src) {
  int fd = 0;
  struct sockaddr_in sin;
  ef((fd = socket(AF_INET, SOCK_STREAM|SOCK_NONBLOCK|SOCK_CLOEXEC, 0)) < 0);
  ef(set_tcp_reuse_addr(fd));
  ef(set_tcp_reuse_port(fd));
  ef(bind(fd, (const struct sockaddr*)make_sockaddr(&sin, src), sizeof(sin)));
  ef(listen(fd, 1024));
  return fd;
  el();
  if (fd >= 0) {
    close(fd);
  }
  return -1;
}

int tcp_accept(int fd) {
  return accept4(fd, NULL, NULL, SOCK_NONBLOCK|SOCK_CLOEXEC);
}

int set_tcp_reuse_addr(int fd) {
  int flag = 1;
  return setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &flag, sizeof(flag));
}

int set_tcp_reuse_port(int fd) {
  int flag = 1;
  return setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &flag, sizeof(flag));
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

int set_tcpopt(int fd, int option, int value)
{
  int ret = setsockopt(fd, IPPROTO_TCP, option, (const void *) &value, sizeof(value));
  if (ret < 0) {
    rk_warn("IPPROTO_TCP fd: %d, errno: %d, option: %d, value: %d", fd, errno, option, value);
  }
  return ret;
}
void update_socket_keepalive_params(int fd, int64_t user_timeout)
{
  int tcp_keepalive = (user_timeout > 0) ? 1: 0;
  int tcp_keepidle = user_timeout/5000000;
  if (tcp_keepidle < 1) {
    tcp_keepidle = 1;
  }
  int tcp_keepintvl = tcp_keepidle;
  int tcp_keepcnt = 5;
  int tcp_user_timeout = (tcp_keepcnt + 1) * tcp_keepidle * 1000 - 100;
  if (1 == tcp_keepalive) {
    if (set_tcpopt(fd, SO_KEEPALIVE, 1)) {
      rk_warn("set SO_KEEPALIVE error: %d, fd=%d\n", errno, fd);
    } else {
      ignore_ret_value(set_tcpopt(fd, TCP_KEEPIDLE, tcp_keepidle));
      ignore_ret_value(set_tcpopt(fd, TCP_KEEPINTVL, tcp_keepintvl));
      ignore_ret_value(set_tcpopt(fd, TCP_KEEPCNT, tcp_keepcnt)); // TCP_USER_TIMEOUT will override keepalive to determine when to close a connection due to keepalive failure
      ignore_ret_value(set_tcpopt(fd, TCP_USER_TIMEOUT, tcp_user_timeout));
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
