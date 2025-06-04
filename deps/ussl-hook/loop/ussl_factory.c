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
#include <string.h>
static clientfd_sk_t *clientfd_sk_new(ussl_sf_t *sf)
{
  clientfd_sk_t *s = NULL;
  if (NULL == (s = (typeof(s))malloc(sizeof(clientfd_sk_t)))) {
    ussl_log_error("malloc for clientfd_sk_t failed, errno:%d", errno);
  } else {
    s->fty = sf;
    s->handle_event = (ussl_handle_event_t)clientfd_sk_handle_event;
    s->type = CLIENT_SOCK;
    s->has_error = 0;
    ussl_dlink_init(&s->timeout_link);
  }
  return s;
}

static void clientfd_sk_delete(ussl_sf_t *sf, clientfd_sk_t *s)
{
  if (NULL != s) {
    int ret = 0;
    remove_from_timeout_list(&s->timeout_link);
    char client_addr[IP_STRING_MAX_LEN] = {0};
    ussl_get_peer_addr(s->fd_info.client_fd, client_addr, IP_STRING_MAX_LEN);
    if (s->fd >= 0) {
      if (s->has_error) {
        shutdown(s->fd, SHUT_WR);
      }
      if (NULL != s->ep) {
        if (0 != (ret = libc_epoll_ctl(s->ep->fd, EPOLL_CTL_DEL, s->fd, NULL))) {
          ussl_log_warn("delete client fd from epoll failed, epfd:%d, fd:%d, errno:%d", s->ep->fd,
                        s->fd, errno);
        }
      }
      if (0 != (ret = libc_epoll_ctl(s->fd_info.org_epfd, EPOLL_CTL_ADD,
                                            s->fd_info.client_fd, &s->fd_info.event))) {
        ussl_log_warn("give back fd to origin epoll failed, fd:%d, errno:%d", s->fd_info.client_fd,
                      errno);
      }
      if (0 == ret) {
        ussl_log_info("give back fd to origin epoll succ, client_fd:%d, client_epfd:%d, event:0x%x, client_addr:%s, has_error:%d",
                      s->fd_info.client_fd, s->fd_info.org_epfd, s->fd_info.event.events, client_addr, s->has_error);
      }
      s->fd = -1;
    }
    free(s);
  }
}

int clientfd_sf_init(ussl_sf_t *sf)
{
  ussl_sf_init(sf, (void *)clientfd_sk_new, (void *)clientfd_sk_delete);
  return 0;
}

static acceptfd_sk_t *acceptfd_sk_new(ussl_sf_t *sf)
{
  acceptfd_sk_t *s = NULL;
  if (NULL == (s = (typeof(s))malloc(sizeof(acceptfd_sk_t)))) {
    ussl_log_error("malloc for acceptfd_sk_t failed, errno:%d", errno);
  } else {
    s->fty = sf;
    s->handle_event = (ussl_handle_event_t)acceptfd_sk_handle_event;
    s->type = SERVER_SOCK;
    s->has_error = 0;
    s->start_time = time(NULL);
    s->fd = -1;
    s->ep = NULL;
    ussl_dlink_init(&s->timeout_link);
  }
  return s;
}
#define CONN_PEEK_BUF_LEN 16
static int try_dispatch_http_conn(int fd, int *dispatch_succ)
{
  int err = 0;
  int pipe_fd_write = ATOMIC_LOAD(&http_conn_pipe[1]);
  if (pipe_fd_write >= 0) {
    char buf[CONN_PEEK_BUF_LEN];
    ssize_t bytes = 0;
    while ((bytes = recv(fd, buf, sizeof(buf), MSG_PEEK)) < 0 && EINTR == errno);
    if (bytes <= 0) {
      ussl_log_warn("peek data failed, the data might be consumed"
                    "because the connection is_local_ip_address, regard it as a obrpc connection, bytes=%zd, err=%d",
                    bytes, errno);
    } else {
      // gRPC uses HTTP/2, and the client end of connection starts with the string PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n)
      buf[CONN_PEEK_BUF_LEN - 1] = '\0';
      if (NULL != strstr(buf, "HTTP")) {
        ussl_log_info("receive a http connection, fd=%d, buf:%s", fd, buf);
        while((bytes = write(pipe_fd_write, (const char*)&fd, sizeof(fd))) < 0 && EINTR == errno);
        if (bytes <= 0) {
          err = -1;
          ussl_log_warn("write grpc sock fd failed, pfd=%d, bytes=%zd, err=%d", pipe_fd_write, bytes, errno);
        } else {
          *dispatch_succ = 1;
          ussl_log_info("write grpc sock fd success, bytes=%zd, pfd=%d", bytes, pipe_fd_write);
        }
      } else {
        ussl_log_info("not a supported http conn, hdr:%s, fd=%d", buf, fd);
      }
    }
  }
  return err;
}

static void acceptfd_sk_delete(ussl_sf_t *sf, acceptfd_sk_t *s)
{
  if (NULL != s) {
    remove_from_timeout_list(&s->timeout_link);
    if (s->fd >= 0) {
      int need_close = 1;
      int err = 0;
      if (s->has_error) {
      } else {
        int dispatch_http_succ = 0;
        if (NULL == s->ep) {
          need_close = 1;
        } else if (0 != (err = libc_epoll_ctl(s->ep->fd, EPOLL_CTL_DEL, s->fd, NULL))) {
          ussl_log_warn("remove acceptfd from epoll failed, epfd:%d, fd:%d, errno:%d", s->ep->fd, s->fd,
                        errno);
        } else if (s->fd_info.client_gid == UINT64_MAX && 0 != (err = try_dispatch_http_conn(s->fd, &dispatch_http_succ))) {
          ussl_log_error("try_dispatch_http_conn failed, epfd:%d, fd:%d, errno:%d", s->ep->fd, s->fd, errno);
        } else if (0 == dispatch_http_succ && 0 != (err = dispatch_accept_fd_to_certain_group(s->fd, s->fd_info.client_gid))) {
          ussl_log_warn("dispatch fd to default group failed, fd:%d, ret:%d", s->fd, ret);
        } else {
          need_close = 0;
        }
      }
      if (need_close) {
        if (NULL != s->ep) {
          if (0 != (err = libc_epoll_ctl(s->ep->fd, EPOLL_CTL_DEL, s->fd, NULL))) {
            ussl_log_warn("delete accept fd from epoll failed, epfd:%d, fd:%d, errno:%d", s->ep->fd,
                          s->fd, errno);
          }
        }
        if (0 != (err = ussl_close(s->fd))) {
          ussl_log_warn("ussl_close failed, fd:%d, errno:%d", s->fd, errno);
        }
      }
    }
    s->fd = -1;
    free(s);
  }
}

int acceptfd_sf_init(ussl_sf_t *sf)
{
  ussl_sf_init(sf, (void *)acceptfd_sk_new, (void *)acceptfd_sk_delete);
  return 0;
}