/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

static int pipefd_handle_event(pipefd_t *s)
{
  int ret = 0;
  // because edge trigger, a loop is needed.
  // Note, if an error occurs while processing a client_fd_info,
  // the returned result will still be 0.
  while (1) {
    client_fd_info_t client_fd_info;
    int rbytes = read(s->fd, &client_fd_info, sizeof(client_fd_info));
    if (0 == rbytes) {
      ret = EAGAIN;
      ussl_log_error("read EOF, pipe fd may be closed, pipefd:%d, errno:%d",
                     s->fd, errno);
      break;
    } else if (-1 == rbytes) {
      if (EAGAIN != errno && EWOULDBLOCK != errno) {
        ussl_log_error("call read failed, pipefd:%d, errno:%d", s->fd, errno);
      }
      ret = EAGAIN;
      break;
    } else {
      clientfd_sk_t *clientfd_sk = (clientfd_sk_t *)s->fty->create((ussl_sf_t *)s->fty);
      if (!clientfd_sk) {
        ussl_log_error("create clientfd sock failed, the clientfd will be closed. clientfd:%d, "
                       "gid:%lu, errno:%d",
                       client_fd_info.client_fd, client_fd_info.client_gid, errno);
        if (client_fd_info.client_fd >= 0) {
          shutdown(client_fd_info.client_fd, SHUT_WR);
          if (0 != libc_epoll_ctl(client_fd_info.org_epfd, EPOLL_CTL_ADD,
                                  client_fd_info.client_fd, &client_fd_info.event)) {
            ussl_log_error("give back fd to origin epoll failed, fd:%d, errno:%d",
                           client_fd_info.client_fd, errno);
          }
        }
      } else {
        memcpy(&clientfd_sk->fd_info, &client_fd_info, sizeof(client_fd_info));
        clientfd_sk->fd = client_fd_info.client_fd;
        clientfd_sk->ep = s->ep;
        if (0 != ussl_eloop_regist(clientfd_sk->ep, (ussl_sock_t *)clientfd_sk, EPOLLOUT)) {
          ussl_log_warn("[pipefd_handle_event] call eloop_regist failed, the clientfd will be "
                         "closed. clientfd:%d, gid:0x%lx, errno:%d",
                         client_fd_info.client_fd, client_fd_info.client_gid, errno);
          if (client_fd_info.client_fd >= 0) {
            shutdown(client_fd_info.client_fd, SHUT_WR);
            if (0 != libc_epoll_ctl(client_fd_info.org_epfd, EPOLL_CTL_ADD,
                                    client_fd_info.client_fd, &client_fd_info.event)) {
              ussl_log_warn("give back fd to origin epoll failed, fd:%d, errno:%d",
                             client_fd_info.client_fd, errno);
            }
          }
          s->fty->destroy(s->fty, (ussl_sock_t *)clientfd_sk);
        }
      }
    }
  }
  return ret;
}

int pipefd_init(ussl_eloop_t *ep, pipefd_t *s, ussl_sf_t *sf, int fd)
{
  int ret = 0;
  ussl_sk_init((ussl_sock_t *)s, sf, (void *)pipefd_handle_event, fd);
  s->ep = ep;
  if (s->fd < 0) {
    ret = -EIO;
    errno = EINVAL;
    ussl_log_error("pipefd is initialized with an invalid fd, errno:%d", errno);
  } else if (0 != (ret = ussl_eloop_regist(ep, (ussl_sock_t *)s, EPOLLIN))) {
    ussl_log_error("regist pipefd failed, ret:%d", ret);
    if (s->fd >= 0) {
      close(s->fd);
    }
  }
  return ret;
}