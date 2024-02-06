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

#include "ussl_listenfd.h"
#include "../ussl-deps.h"
#include <errno.h>
#include <stdlib.h>

void ussl_on_accept(int fd, ussl_sf_t *sf, ussl_eloop_t *ep)
{
  int add_succ = 0;
  ussl_sock_t *ns = sf->create(sf);
  if (NULL == ns) {
    ussl_log_error("accept sock factory create sk failed, fd:%d", fd);
  } else if (0 != make_socket_non_blocking(fd)) {
    ussl_log_error("set non-blocking failed, fd:%d", fd);
  } else {
    ns->fd = fd;
    ns->fty = sf;
    acceptfd_sk_t* accept_sk = (acceptfd_sk_t *)ns;
    accept_sk->fd_info.stage = SERVER_ACCEPT_CONNECTION;
    accept_sk->ep = ep;
    if (0 != ussl_eloop_regist(ep, ns, EPOLLIN)) {
      ussl_log_error("add accept fd to epoll failed, fd:%d", fd);
    } else {
      add_succ = 1;
      add_to_timeout_list(&accept_sk->timeout_link);
      char src_addr[IP_STRING_MAX_LEN] = {0};
      ussl_get_peer_addr(fd, src_addr, IP_STRING_MAX_LEN);
      ussl_log_info("accept new connection, fd:%d, src_addr:%s", fd, src_addr);
    }
  }
  if (!add_succ) {
    if (fd >= 0) {
      close(fd);
    }
    if (NULL != ns) {
      sf->destroy(sf, ns);
    }
  }
}

static int ussl_listenfd_handle_event(ussl_listenfd_t *s)
{
  int ret = 0;
  if (ussl_skt(s, IN)) {
    struct sockaddr_storage addr;
    socklen_t addr_len = sizeof(addr);
    int accept_fd = -1;
    // because edge trigger, a loop is needed.
    // Note, if an error occurs while processing an acceptfd,
    // the returned result will still be 0.
    while (1) {
      accept_fd = libc_accept(s->fd, (struct sockaddr *)&addr, &addr_len);
      if (accept_fd >= 0) {
        ussl_on_accept(accept_fd, s->fty, s->ep);
      } else {
        if (EINTR == errno) {
          continue;
        } else if (EAGAIN == errno || EWOULDBLOCK == errno) {
          s->mask &= ~EPOLLIN;
          ret = EAGAIN;
          break;
        } else {
          /*
            if error happens in accept, we also need to return EAGAIN
            because ussl_listenfd_t *s cannot be destroyed
          */
          s->mask &= ~EPOLLIN;
          ret = EAGAIN;
          break;
        }
      }
    }
  }
  return ret;
}

int ussl_listenfd_init(ussl_eloop_t *ep, ussl_listenfd_t *s, ussl_sf_t *sf, int fd)
{
  int ret = 0;
  ussl_sk_init((ussl_sock_t *)s, sf, (void *)ussl_listenfd_handle_event, fd);
  s->ep = ep;
  if (s->fd < 0) {
    ret = -EIO;
    errno = EINVAL;
    ussl_log_error("listenfd is initialized with an invalid fd, errno:%d", errno);
  } else if (0 != (ret = ussl_eloop_regist(ep, (ussl_sock_t *)s, EPOLLIN))) {
    ussl_log_error("regist listenfd failed, errno:%d", errno);
    if (s->fd >= 0) {
      close(s->fd);
    }
  }
  return ret;
}