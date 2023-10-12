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

#include <sys/socket.h>
#include <sys/epoll.h>
#include <errno.h>
#include <stdlib.h>
#include <sys/stat.h>
#include "ussl-hook.h"
#include "ussl-deps.h"
#include "loop/ussl_sock.h"
#include "loop/ussl_link.h"
#include "loop/ussl_ihash_map.h"
#include "loop/auth-methods.h"
#include "loop/ussl_eloop.h"
#include "loop/ussl_factory.h"
#include "loop/handle-event.h"
#include "loop/ussl_listenfd.h"
#include "loop/pipefd.h"
#include "ssl/ssl_config.h"
#include "ussl-loop.h"
#include "message.h"


#define USSL_MAX_FD_NUM 1024 * 1024
static __thread uint64_t ussl_dispatch_id;
static __thread int ussl_server_ctx_id = -1;
static uint64_t global_gid_arr[USSL_MAX_FD_NUM];
static int global_client_ctx_id_arr[USSL_MAX_FD_NUM];
static int global_send_negotiation_arr[USSL_MAX_FD_NUM];
int is_ussl_bg_thread_started = 0;
static int ussl_is_stopped = 0;

static __attribute__((constructor(102))) void init_global_array()
{
  for (int i = 0; i < USSL_MAX_FD_NUM; ++i) {
    global_gid_arr[i] = UINT64_MAX;
    global_client_ctx_id_arr[i] = -1;
    global_send_negotiation_arr[i] = -1;
  }
}

static void ussl_clear_client_opt(int fd)
{
  if (fd >= 0 && fd < USSL_MAX_FD_NUM) {
    global_gid_arr[fd] = UINT64_MAX;
    global_client_ctx_id_arr[fd] = -1;
    global_send_negotiation_arr[fd] = -1;
  }
}

static int ussl_is_pipe(int fd)
{
  struct stat st;
  return 0 == fstat(fd, &st) && S_ISFIFO(st.st_mode);
}

int ussl_connect(int sockfd, const struct sockaddr *address, socklen_t address_len)
{
  int ret = 0;
  if ((ret = libc_connect(sockfd, address, address_len)) < 0) {
    if (EINPROGRESS != errno) {
      ussl_clear_client_opt(sockfd);
    }
  } else {
    if (AF_INET == address->sa_family) {
      struct sockaddr_in self_addr;
      socklen_t len = sizeof(self_addr);
      if (0 == getsockname(sockfd, (struct sockaddr *)&self_addr, &len)) {
        struct sockaddr_in *dst_addr = (struct sockaddr_in *)address;
        if (self_addr.sin_port == dst_addr->sin_port && self_addr.sin_addr.s_addr == dst_addr->sin_addr.s_addr) {
          ret = -1;
          errno = EIO;
          char str[INET_ADDRSTRLEN];
          ussl_log_warn("connection to %s failed, self connect self", inet_ntop(AF_INET, (const void*)(address), str, sizeof(str)));
        }
      } else {
        ret = -1;
        ussl_log_warn("getsockname failed, fd:%d, error:%s", sockfd, strerror(errno));
      }
    }
    /* if gid has been set and connect return success, we also let the ret to be -1 and errno to be
     * EINPROGRESS */
    if (ret == 0 && sockfd >= 0 && sockfd < USSL_MAX_FD_NUM && global_gid_arr[sockfd] != UINT64_MAX) {
      ret = -1;
      errno = EINPROGRESS;
    }
  }
  return ret;
}

static int ussl_check_methods(int methods)
{
  int ret = 0;
  int flag =
      methods & ~USSL_AUTH_NONE & ~USSL_AUTH_SSL_HANDSHAKE & ~USSL_AUTH_SSL_IO;
  if (methods <= 0 || 0 != flag) {
    ret = -1;
    errno = EINVAL;
    ussl_log_warn("invalid methods, all valid methods: USSL_AUTH_NONE(1) "
                   "USSL_AUTH_SSL_HANDSHAKE(2), USSL_AUTH_SSL_IO(4).")
  }
  return  ret;
}

static int ussl_check_fd(int fd)
{
  int ret = 0;
  if (fd >= USSL_MAX_FD_NUM || fd < 0) {
    ret = -1;
    errno = EINVAL;
    ussl_log_error(
        "fd is expected to be a non-negative integer and less than %d, but currently fd is %d",
        USSL_MAX_FD_NUM, fd);
  }
  return ret;
}

static int ussl_check_ctx_id(int ctx_id)
{
  int ret = 0;
  if (ctx_id < 0) {
    ret = -1;
    errno = EINVAL;
    ussl_log_error("ctx_id cannot be negative, ctx_id:%d", ctx_id);
  }
  return ret;
}

int ussl_setsockopt(int socket, int level, int optname, const void *optval, socklen_t optlen)
{
  int ret = 0;
  if (ATOMIC_BCAS(&is_ussl_bg_thread_started, 0, 1)) {
    ret = ussl_init_bg_thread();
    if (0 != ret) {
      ussl_log_error("start ussl-bk-thread failed!, ret:%d", ret);
      ATOMIC_STORE(&is_ussl_bg_thread_started, 0);
    }
  }
  if (0 == ret) {
    if (SOL_OB_SOCKET == level) {
      if (SO_OB_SET_CLIENT_GID == optname || SO_OB_SET_SERVER_GID == optname) {
        uint64_t gid = *(uint64_t *)optval;
        if (0 == (ret = ussl_check_fd(socket))) {
          global_gid_arr[socket] = gid;
        }
      } else if (SO_OB_SET_CLIENT_SSL_CTX_ID == optname) {
        int client_ctx_id = *(int *)optval;
        if (0 == (ret = ussl_check_fd(socket)) && 0 == (ret = ussl_check_ctx_id(client_ctx_id))) {
          global_client_ctx_id_arr[socket] = client_ctx_id;
        }
      } else if (SO_OB_SET_SERVER_SSL_CTX_ID == optname) {
        int server_ctx_id = *(int *)optval;
        if (0 == (ret = ussl_check_ctx_id(server_ctx_id))) {
          ussl_server_ctx_id = server_ctx_id;
        }
      } else if (SO_OB_SET_SEND_NEGOTIATION_FLAG == optname) {
        int need_send_flag = *(int *)optval;
        if (0 == (ret = ussl_check_fd(socket))) {
          global_send_negotiation_arr[socket] = need_send_flag;
        }
      }
    } else if (SOL_OB_CTX == level) {
      if (SO_OB_CTX_SERVER_AUTH_METHODS == optname) {
        int methods = *(int *)optval;
        if (0 == (ret = ussl_check_methods(methods))){
          set_server_auth_methods(methods);
        }
      } else if (SO_OB_CTX_CLIENT_AUTH_METHODS == optname) {
        int methods = *(int *)optval;
        if (0 == (ret = ussl_check_methods(methods))){
          set_client_auth_methods(methods);
        }
      } else if (SO_OB_CTX_SET_SSL_CONFIG == optname) {
        ret = ssl_load_config(socket, (ssl_config_item_t *)optval);
      }
    } else {
      ret = libc_setsockopt(socket, level, optname, optval, optlen);
    }
  }
  return ret;
}

void ussl_stop()
{
  ATOMIC_STORE(&ussl_is_stopped, 1);
}

int ussl_is_stop()
{
  return ATOMIC_LOAD(&ussl_is_stopped);
}

void ussl_wait()
{
  ussl_wait_bg_thread();
}

int ussl_listen(int socket, int backlog)
{
  int ret = 0;
  if (socket < 0 || socket >= USSL_MAX_FD_NUM) {
    ret = -1;
    errno = EINVAL;
    ussl_fatal(
    "fd is expected to be a non-negative integer and less than %d, but currently fd is %d",
    USSL_MAX_FD_NUM, socket);
  } else {
    if (0 != ussl_loop_add_listen_once(socket, backlog)) {
      ret = -1;
      ussl_log_error("add listen filed, fd:%d", socket);
    }
  }
  return ret;
}

int ussl_accept(int socket, struct sockaddr *address, socklen_t *address_len)
{
  return ussl_accept4(socket, address, address_len, 0);
}

int ussl_accept4(int socket, struct sockaddr *address, socklen_t *address_len, int flags)
{
  int accept_fd = -1;
  if (ussl_is_pipe(socket)) {
    if (read(socket, &accept_fd, sizeof(accept_fd)) != sizeof(accept_fd)) {
      accept_fd = -1;
      ussl_log_error("read from pipe fd filed, fd:%d, errno:%d", socket, errno);
    } else if (NULL != address && getpeername(accept_fd, address, address_len) < 0) {
      accept_fd = -1;
      ussl_log_error("getpeername failed, fd:%d, errno:%d", accept_fd, errno);
    }
  } else {
    accept_fd = libc_accept4(socket, address, address_len, flags);
  }
  return accept_fd;
}

int ussl_epoll_ctl(int epfd, int op, int fd, struct epoll_event *event)
{
  int ret = 0;
  // Normally, ussl_setsockopt ia called before ussl_epoll_ctl and there is a checker for
  // array out-of-bounds in ussl_setsockopt. If the global_gid_arr is out-of-bounds here,
  // it must be a serious error.
  if (fd < 0 || fd >= USSL_MAX_FD_NUM) {
    ret = -1;
    ussl_fatal(
        "fd is expected to be a non-negative integer and less than %d, but currently fd is %d",
        USSL_MAX_FD_NUM, fd);
  } else if (global_gid_arr[fd] != UINT64_MAX && EPOLL_CTL_ADD == op) {
    ret = ussl_loop_add_clientfd(fd, global_gid_arr[fd], global_client_ctx_id_arr[fd], global_send_negotiation_arr[fd],
                                 get_client_auth_methods(), epfd, event);
    ussl_clear_client_opt(fd);
  } else {
    ret = libc_epoll_ctl(epfd, op, fd, event);
  }
  return ret;
}

ssize_t ussl_read(int fd, char *buf, size_t nbytes)
{
  return read_regard_ssl(fd, buf, nbytes);
}

ssize_t ussl_write(int fd, const void *buf, size_t nbytes)
{
  return write_regard_ssl(fd, buf, nbytes);
}

int ussl_close(int fd)
{
  fd_disable_ssl(fd);
  return libc_close(fd);
}

ssize_t ussl_writev(int fildes, const struct iovec *iov, int iovcnt)
{
  return writev_regard_ssl(fildes, iov, iovcnt);
}

#include "ussl-deps.c"
#include "loop/auth-methods.c"
#include "loop/ussl_eloop.c"
#include "loop/ussl_factory.c"
#include "loop/handle-event.c"
#include "loop/ussl_listenfd.c"
#include "loop/pipefd.c"
#include "loop/ussl_sock.c"
#include "loop/ussl_link.c"
#include "loop/ussl_ihash_map.c"
#include "ssl/ssl_config.c"
#include "ussl-loop.c"
#include "message.c"