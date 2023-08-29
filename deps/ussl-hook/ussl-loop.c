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

#include "ussl-loop.h"
#include <errno.h>
#include <netinet/in.h>
#include <pthread.h>
#include <sys/prctl.h>

#define TIMEOUT_THRESHOLD_SEC 3

typedef struct uloop_t
{
  ussl_eloop_t ep;
  ussl_listenfd_t listenfd;
  pipefd_t pipefd;
  ussl_dlink_t timeout_list;
} uloop_t;

uloop_t global_ussl_loop_struct;
static int ussl_has_listened = 0;
static ussl_sf_t acceptfd_fty;
static ussl_sf_t clientfd_fty;
static void *ussl_bg_thread_id;

int ob_pthread_create(void **ptr, void *(*start_routine) (void *), void *arg);
void ob_pthread_join(void *ptr);
void ob_set_thread_name(const char* type);

static int uloop_init(uloop_t *l)
{
  int err = 0;
  ussl_dlink_init(&l->timeout_list);
  if (0 != (err = ussl_eloop_init(&l->ep))) {
    ussl_log_error("eloop init fail: %d", err);
  } else if (0 != clientfd_sf_init(&clientfd_fty)) {
    ussl_log_error("initialize clientfd factory fail.")
  } else if (0 !=
             (err = pipefd_init(&l->ep, &l->pipefd, (ussl_sf_t *)&clientfd_fty, client_comm_pipe[0]))) {
    ussl_log_error("pipefd init fail: %d", err);
  }
  return err;
}

static int uloop_run(uloop_t *loop)
{
  return ussl_eloop_run(&loop->ep);
}

static int uloop_add_listen(uloop_t *l, int listen_fd, int backlog)
{
  int ret = 0;
  if (libc_listen(listen_fd, backlog) < 0) {
    ret = -EIO;
    ussl_log_error("listen failed, fd:%d, errno:%d", listen_fd, errno);
  } else if (0 != acceptfd_sf_init(&acceptfd_fty)) {
    ret = -1;
    ussl_log_error("acceptfd_sf_init failed, fd:%d", listen_fd);
  } else if (0 != ussl_listenfd_init(&l->ep, &l->listenfd, (ussl_sf_t *)&acceptfd_fty, listen_fd)) {
    ret = -1;
    ussl_log_error("listenfd_init failed, fd:%d", listen_fd);
  }
  if (0 != ret) {
    if (listen_fd >= 0) {
      close(listen_fd);
    }
  }
  return ret;
}

static void *bg_thread_func(void *arg)
{
  ob_set_thread_name("ussl_loop");
  uloop_run(&global_ussl_loop_struct);
  return NULL;
}

#ifndef F_SETPIPE_SZ
#define F_SETPIPE_SZ 1031
#endif

int ussl_init_bg_thread()
{
  int ret = 0;
  static const int pipe_resize = 128 * 1024;
  if (0 != pipe2(client_comm_pipe, O_NONBLOCK | O_CLOEXEC)) {
    ret = EIO;
    ussl_log_error("create client_communicate_pipe failed, errno:%d", errno);
  } else if (fcntl(client_comm_pipe[1], F_SETPIPE_SZ, pipe_resize) < 0) {
    int pipe_size = fcntl(client_comm_pipe[1], F_GETPIPE_SZ);
    ussl_log_warn("resize pipe failed, pipefd:%d, errno:%d, pipe_size:%d", client_comm_pipe[1], errno, pipe_size);
  }
  if (0 == ret) {
    if (0 != uloop_init(&global_ussl_loop_struct)) {
      ussl_log_error("initialize uloop failed.")
    } else if (0 != ob_pthread_create(&ussl_bg_thread_id, bg_thread_func, NULL)) {
      ret = EIO;
      ussl_log_error("create background thread failed, errno:%d", errno);
    } else {
      ussl_log_info("create background thread success!");
    }
  }
  return ret;
}

extern int is_ussl_bg_thread_started;
void ussl_wait_bg_thread()
{
  if (ATOMIC_LOAD(&is_ussl_bg_thread_started)) {
    ob_pthread_join(ussl_bg_thread_id);
    ussl_bg_thread_id = NULL;
    ATOMIC_STORE(&is_ussl_bg_thread_started, 0);
  }
}

void add_to_timeout_list(ussl_dlink_t *l)
{
  ussl_dlink_insert(&global_ussl_loop_struct.timeout_list, l);
}

void remove_from_timeout_list(ussl_dlink_t *l)
{
  ussl_dlink_delete(l);
}

static void handle_client_timeout_event(clientfd_sk_t *client_sk)
{
  ussl_sf_t *clientfd_fty = client_sk->fty;
  shutdown(client_sk->fd, SHUT_WR);
  if (libc_epoll_ctl(client_sk->ep->fd, EPOLL_CTL_DEL, client_sk->fd, NULL) < 0) {
    ussl_log_warn("remove fd from current epoll failed, fd:%d, cur_epollfd:%d, errno:%d",
                   client_sk->fd, client_sk->ep->fd, errno);
  } else if (libc_epoll_ctl(client_sk->fd_info.org_epfd, EPOLL_CTL_ADD, client_sk->fd,
                            &client_sk->fd_info.event) < 0) {
    ussl_log_warn("give back fd to original epoll failed, fd:%d, origin_epfd:%d, errno:%d",
                   client_sk->fd, client_sk->fd_info.org_epfd, errno);
  }
  clientfd_fty->destroy(clientfd_fty, (ussl_sock_t *)client_sk);
}

static void handle_server_timeout_event(acceptfd_sk_t *accept_sk)
{
  ussl_sf_t *acceptfd_fty = accept_sk->fty;
  if (libc_epoll_ctl(accept_sk->ep->fd, EPOLL_CTL_DEL, accept_sk->fd, NULL) < 0) {
    ussl_log_warn("remove fd from current epoll failed, fd:%d, cur_epollfd:%d, errno:%d",
                   accept_sk->fd, accept_sk->ep->fd, errno);
  }
  close(accept_sk->fd);
  acceptfd_fty->destroy(acceptfd_fty, (ussl_sock_t *)accept_sk);
}

void check_and_handle_timeout_event()
{
  time_t cur_time = time(NULL);
  ussl_dlink_for(&global_ussl_loop_struct.timeout_list, p)
  {
    int type = *(int *)(p + 1);
    if (CLIENT_SOCK == type) {
      clientfd_sk_t *client_sk = ussl_structof(p, clientfd_sk_t, timeout_link);
      if (cur_time - client_sk->start_time > TIMEOUT_THRESHOLD_SEC) {
        char dst_addr[IP_STRING_MAX_LEN] = {0};
        ussl_get_peer_addr(client_sk->fd, dst_addr, IP_STRING_MAX_LEN);
        ussl_log_warn("clientfd timeout, fd:%d, dst_addr:%s", client_sk->fd, dst_addr);
        ussl_dlink_delete(&client_sk->ready_link);
        ussl_dlink_delete(p);
        handle_client_timeout_event(client_sk);
      }
    } else {
      acceptfd_sk_t *accept_sk = ussl_structof(p, acceptfd_sk_t, timeout_link);
      if (cur_time - accept_sk->start_time > TIMEOUT_THRESHOLD_SEC) {
        char src_addr[IP_STRING_MAX_LEN] = {0};
        ussl_get_peer_addr(accept_sk->fd, src_addr, IP_STRING_MAX_LEN);
        ussl_log_warn("acceptfd timeout, fd:%d, src_addr:%s", accept_sk->fd, src_addr);
        ussl_dlink_delete(&accept_sk->ready_link);
        ussl_dlink_delete(p);
        handle_server_timeout_event(accept_sk);
      }
    }
  }
}

int ussl_loop_add_listen_once(int listen_fd, int backlog)
{
  int ret = 0;
  if (ATOMIC_BCAS(&ussl_has_listened, 0, 1)) {
    // get addr from fd
    struct sockaddr_storage ussl_listened_addr;
    socklen_t ussl_listened_addrlen = sizeof(ussl_listened_addr);
    if (0 !=
        getsockname(listen_fd, (struct sockaddr *)&ussl_listened_addr, &ussl_listened_addrlen)) {
      ret = -1;
      ussl_log_error("getsockname failed, fd:%d, errno:%d", listen_fd, errno);
      ATOMIC_STORE(&ussl_has_listened, 0);
    } else if (AF_INET != ussl_listened_addr.ss_family &&
               AF_INET6 != ussl_listened_addr.ss_family) {
      ret = -1;
      ussl_log_info("the protocol family is not IPv4 or IPv6, fd:%d", listen_fd);
      ATOMIC_STORE(&ussl_has_listened, 0);
    } else if (0 != uloop_add_listen(&global_ussl_loop_struct, listen_fd, backlog)) {
      ret = -1;
      ussl_log_error("uloop_add_listen failed, fd:%d errno:%d", listen_fd, errno);
      ATOMIC_STORE(&ussl_has_listened, 0);
    } else {
      int port = 0;
      if (AF_INET == ussl_listened_addr.ss_family) {
        struct sockaddr_in *s = (struct sockaddr_in *)(&ussl_listened_addr);
        port = s->sin_port;
      } else if (AF_INET6 == ussl_listened_addr.ss_family) {
        struct sockaddr_in6 * s = (struct sockaddr_in6 *)(&ussl_listened_addr);
        port = s->sin6_port;
      }
      ussl_log_info("uloop add listen success! port:%d", ntohs(port));
    }
  }
  return ret;
}

int ussl_loop_add_clientfd(int client_fd, uint64_t gid, int ctx_id, int send_negotiation, int auth_methods, int epfd,
                           struct epoll_event *event)
{
  int ret = 0;
  client_fd_info_t client_fd_info;

  client_fd_info.client_gid = gid;
  client_fd_info.ssl_ctx_id = ctx_id;
  client_fd_info.event = *event;
  client_fd_info.org_epfd = epfd;
  client_fd_info.client_fd = client_fd;
  client_fd_info.auth_methods = auth_methods;
  client_fd_info.send_negotiation = send_negotiation;
  ssize_t wbytes = 0;

  if (sizeof(client_fd_info) !=
      (wbytes = libc_write(client_comm_pipe[1], &client_fd_info, sizeof(client_fd_info)))) {
    ret = -1;
    ussl_log_error(
        "write client_fd_info failed, clientfd:%d, gid:0x%lx, ctx_id:%d, errno:%d, wbytes:%ld",
        client_fd, gid, ctx_id, errno, wbytes);
  } else {
    ussl_log_info("write client fd succ, fd:%d, gid:0x%lx, need_send_negotiation:%d", client_fd, gid, send_negotiation);
  }

  return ret;
}
