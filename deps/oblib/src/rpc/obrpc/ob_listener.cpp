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

#include "rpc/obrpc/ob_listener.h"
#include "lib/ob_running_mode.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/serialization.h"
#include "lib/net/ob_net_util.h"
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <sys/epoll.h>
#include <sys/types.h>
#include <netinet/tcp.h>
#include <sys/ioctl.h>
#include <errno.h>

#define ussl_log(level, errcode, format, ...) _OB_LOG_RET(level, errcode, "[ussl] " format, ##__VA_ARGS__)
extern "C" {
#include "ussl-hook.h"
};

using namespace oceanbase::common;
using namespace oceanbase::obrpc;
using namespace oceanbase::common::serialization;

#ifndef SO_REUSEPORT
#define SO_REUSEPORT 15
#endif

ObListener::ObListener()
{
  listen_fd_ = -1;
  memset(&io_wrpipefd_map_, 0, sizeof(io_wrpipefd_map_));
}

ObListener::~ObListener()
{
  if (listen_fd_ >= 0) {
    close(listen_fd_);
    listen_fd_ = -1;
  }
}

int ObListener::ob_listener_set_tcp_opt(int fd, int option, int value)
{
    return setsockopt(fd, IPPROTO_TCP, option, (const void *) &value, sizeof(value));
}

int ObListener::ob_listener_set_opt(int fd, int option, int value)
{
    return setsockopt(fd, SOL_SOCKET, option, (void *)&value, sizeof(value));
}

int ObListener::listen_special(int family, int port)
{
  int ret = OB_SUCCESS;
  int fd = -1;
  struct sockaddr_storage addr;
  int no_block_flag = 1;
  memset(&addr, 0, sizeof(addr));
  if (port <= 0) {
    ret = OB_INVALID_ARGUMENT;
    RPC_LOG(ERROR, "invalid port", K(ret), K(port));
  } else if (AF_INET != family && AF_INET6 != family) {
    ret = OB_INVALID_ARGUMENT;
    RPC_LOG(ERROR, "invalid port", K(ret), K(port));
  } else if ((fd = socket(family, SOCK_STREAM|SOCK_CLOEXEC, 0)) < 0) {
    ret = OB_SERVER_LISTEN_ERROR;
    RPC_LOG(ERROR, "create socket failed!", K(ret), K(fd), K(family), K(port), K(errno));
  } else if (ioctl(fd, FIONBIO, &no_block_flag) < 0) {
    ret = OB_SERVER_LISTEN_ERROR;
    RPC_LOG(ERROR, "set non block failed!", K(ret), K(fd), K(family), K(port), K(errno));
  } else if (ob_listener_set_tcp_opt(fd, TCP_DEFER_ACCEPT, 1) < 0) {
    ret = OB_SERVER_LISTEN_ERROR;
    RPC_LOG(ERROR, "set tcp defer accept failed!", K(ret), K(fd), K(family), K(port), K(errno));
  } else if (ob_listener_set_opt(fd, SO_REUSEADDR, 1) < 0) {
    ret = OB_SERVER_LISTEN_ERROR;
    RPC_LOG(ERROR, "set reuse addr fail!", K(ret), K(fd), K(family), K(port), K(errno));
  } else if (ob_listener_set_opt(fd, SO_REUSEPORT, 1) < 0) {
    ret = OB_SERVER_LISTEN_ERROR;
    RPC_LOG(ERROR, "set reuse port fail!", K(fd), K(family), K(port), K(errno));
  } else if (bind(fd, (sockaddr*)obsys::ObNetUtil::make_unix_sockaddr_any(AF_INET6 == family, port, &addr),
              sizeof(addr)) < 0) {
    ret = OB_SERVER_LISTEN_ERROR;
    RPC_LOG(ERROR, "bind failed!", K(fd), K(family), K(port), K(errno));
  } else if (listen(fd, 1024) < 0) {
    ret = OB_SERVER_LISTEN_ERROR;
    RPC_LOG(ERROR, "listen failed", K(fd), K(family), K(port), K(errno));
  }
  if (OB_FAIL(ret) && fd >= 0) {
    close(fd);
    fd = -1;
  }
  return fd;
}

int ObListener::listen_create(int port) {
  int ret = OB_SUCCESS;
  if ((listen_fd_ = listen_special(AF_INET, port)) < 0) {
    RPC_LOG(ERROR, "create listen for IPv4 fail!", K(errno));
  }
  if (listen_fd_ < 0) {
    ret = OB_SERVER_LISTEN_ERROR;
  }
  return ret;
}

void ObListener::run(int64_t idx)
{
  UNUSED(idx);
  listen_start();
}

int ObListener::listen_start()
{
  int ret = OB_SUCCESS;
  this->do_work();
  return ret;
}

void ObListener::destroy()
{
  Threads::destroy();
  if (listen_fd_ >= 0) {
    struct linger so_linger;
    so_linger.l_onoff = 1;
    so_linger.l_linger = 0;
    if (listen_fd_ >= 0) {
      setsockopt(listen_fd_, SOL_SOCKET, SO_LINGER, &so_linger, sizeof(so_linger));
      close(listen_fd_);
      listen_fd_ = -1;
    }
    memset(&io_wrpipefd_map_, 0, sizeof(io_wrpipefd_map_));
  }
}

int ObListener::regist(uint64_t magic, int count, int *pipefd_array)
{
  int ret = OB_SUCCESS;
  io_wrpipefd_map_t *pipefd_map = NULL;

  for (int i = 0; i < MAX_PROTOCOL_TYPE_SIZE; i++)
  {
    if (0 == io_wrpipefd_map_[i].used) {
      pipefd_map = &io_wrpipefd_map_[i];
      break;
    }
  }

  if (NULL == pipefd_map || NULL == pipefd_array) {
    ret = OB_INVALID_ARGUMENT;
    RPC_LOG(ERROR, "register protocol type size is beyond limit!", K(pipefd_array));
  } else {
    int fd[2];
    int wrfd = 0;
    int rdfd = 0;
    pipefd_map->magic = magic;

    for (int i = 0; i < count && OB_SUCC(ret); i++) {
      if (0 == pipe2(fd, O_NONBLOCK)) {
        wrfd = fd[1];
        rdfd = fd[0];
        pipefd_map->ioth_wrpipefd_pool.pipefd[i] = wrfd;
        pipefd_map->ioth_wrpipefd_pool.count++;
        pipefd_array[i] = rdfd;
      } else {
        ret = OB_ERR_UNEXPECTED;
        RPC_LOG(ERROR, "call pipe2 failed!", K(errno));
      }
    }
    pipefd_map->used = 1;
  }

  return ret;
}

/*
* easy negotiation packet format
PACKET HEADER:
+------------------------------------------------------------------------+
|         negotiation packet header magic(8B)  | msg body len (2B)
+------------------------------------------------------------------------+

PACKET MSG BODY:
+------------------------------------------------------------------------+
|   io thread corresponding eio magic(8B) |  io thread index (1B)
+------------------------------------------------------------------------+
*/

static int read_client_magic(int fd, uint64_t &client_magic, uint8_t &index)
{
  int ret = OB_SUCCESS;
  const int64_t recv_buf_len = 1 * 1024;
  int64_t rcv_byte = 0;
  uint64_t header_magic = 0;
  int64_t pos = 0;
  uint16_t msg_body_len = 0;
  char recv_buf[recv_buf_len];
  memset(recv_buf, 0, sizeof(recv_buf_len));

  while ((rcv_byte = recv(fd, (char *) recv_buf, sizeof(recv_buf), MSG_PEEK)) < 0 && EINTR == errno);

  if (rcv_byte < 0) {
    RPC_LOG(WARN, "recv bytes is less than 0!", K(errno));
    if (EAGAIN == errno || EWOULDBLOCK == errno) {
      ret = OB_EAGAIN;
    } else {
      ret = OB_IO_ERROR;
    }
  } else if (0 == rcv_byte) {
    RPC_LOG(INFO, "peer closed connection!");
    ret = OB_IO_ERROR;
  } else {
    RPC_LOG(INFO, "read negotiation msg", K(rcv_byte));
  }

  if (OB_FAIL(ret)) {
    RPC_LOG(INFO, "read from client failed");
  } else {
    if (OB_FAIL(decode_i64(recv_buf, recv_buf_len, pos, reinterpret_cast<int64_t *>(&header_magic)))) {
      RPC_LOG(INFO, "decode header magic failed!", K(ret));
    } else if (header_magic != NEGOTIATION_PACKET_HEADER_MAGIC_EASY) {
      ret = OB_NOT_THE_OBJECT;
      RPC_LOG(INFO, "not negotiation msg! header magic does not match!");
    } else if (OB_FAIL(decode_i16(recv_buf, recv_buf_len, pos, reinterpret_cast<int16_t *>(&msg_body_len)))) {
      RPC_LOG(INFO, "decode msg body len failed!", K(ret));
    } else if (OB_FAIL(decode_i64(recv_buf, recv_buf_len, pos, reinterpret_cast<int64_t *>(&client_magic)))) {
      RPC_LOG(INFO, "decode eio magic failed!", K(ret));
    } else {
      index = recv_buf[pos++];
    }

    if (OB_SUCC(ret)) {
      RPC_LOG(INFO, "read_client_magic, ", K(client_magic), K(index));
    }
  }

  return ret;
}

static int connection_redispatch(int conn_fd, io_threads_pipefd_pool_t *ths_fd_pool, int index)
{
  int ret = OB_SUCCESS;
  int wrfd = -1;
  int64_t write_bytes = 0;

  if (OB_ISNULL(ths_fd_pool)) {
    RPC_LOG(ERROR, "ths_fd_pool is NULL");
    ret = OB_INVALID_ARGUMENT;
  } else if (conn_fd < 0) {
    RPC_LOG(ERROR, "conn_fd invalid", K(conn_fd));
    ret = OB_INVALID_ARGUMENT;
  } else {
    int count = ths_fd_pool->count;
    wrfd = ths_fd_pool->pipefd[index % count];
    RPC_LOG(INFO, "dipatch", K(conn_fd), K(count), K(index), K(wrfd));
    while ((write_bytes = write(wrfd, &conn_fd, sizeof(conn_fd))) < 0 && errno == EINTR);
    if (write_bytes != sizeof(conn_fd)) {
      RPC_LOG(ERROR, "dispatch failed!", K(errno), K(conn_fd), K(wrfd));
      ret = OB_IO_ERROR;
    } else {
      RPC_LOG(INFO, "dispatch success!", K(conn_fd), K(wrfd));
    }
  }

  return ret;
}

static void trace_connection_info(int fd)
{
    struct sockaddr_storage addr;
    socklen_t addr_len;
    addr_len = sizeof(addr);

    if (getpeername(fd, (struct sockaddr *)&addr, &addr_len) == 0) {
      ObAddr peer;
      peer.from_sockaddr(&addr);
      RPC_LOG(INFO, "oblistener receive connection from", K(peer));
    }
}

void ObListener::do_work()
{
  int ret = OB_SUCCESS;
  int epoll_fd = -1;

  if (listen_fd_ < 0) {
    ret = OB_IO_ERROR;
    RPC_LOG(ERROR, "listen_fd_ is less than 0");
  } else if ((epoll_fd = epoll_create(256)) < 0) {
    ret = OB_IO_ERROR;
    RPC_LOG(ERROR, "epoll create failed", K(errno));
  }

  if (OB_SUCC(ret)) {
    if (listen_fd_ >= 0) {
      struct epoll_event listen_ev4;
      listen_ev4.events = EPOLLIN;
      listen_ev4.data.fd = listen_fd_;
      if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, listen_fd_, &listen_ev4) < 0) {
        ret = OB_IO_ERROR;
        RPC_LOG(ERROR, "epoll add listen fd for IPv4 failed!", K(errno));
      }
    }
  }

  if (OB_FAIL(ret)) {
    if (epoll_fd >= 0) {
      close(epoll_fd);
    }
    RPC_LOG(ERROR, "error happens, do not start epoll_wait.");
  } else {
    const int MAXEPOLLSIZE = 64;
    struct epoll_event events[MAXEPOLLSIZE];
    struct epoll_event conn_ev;

    while(!has_set_stop()) {
      int cnt = ob_epoll_wait(epoll_fd, events, MAXEPOLLSIZE, 1000);
      for (int i = 0; i < cnt; i++) {
        int accept_fd = events[i].data.fd;
        uint64_t client_magic = 0;
        uint8_t index = 0;
        io_threads_pipefd_pool_t *pipefd_pool = NULL;

        if (accept_fd == listen_fd_) {
          int conn_fd = ussl_accept(listen_fd_, NULL, NULL);
          if (conn_fd < 0) {
            RPC_LOG(ERROR, "accept failed!", K(errno));
          } else {
            conn_ev.events = EPOLLIN;
            conn_ev.data.fd = conn_fd;

            if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, conn_fd, &conn_ev) < 0) {
              RPC_LOG(ERROR, "add conn_fd to epoll failed!", K(errno));
              close(conn_fd);
            }
          }
        } else {
            if (OB_FAIL(read_client_magic(accept_fd, client_magic, index))) {
              /* for compatibility purposes,  round robin dispatch */
              if (epoll_ctl(epoll_fd, EPOLL_CTL_DEL, accept_fd, NULL) < 0) {
                RPC_LOG(ERROR, "delete accept_fd from epoll failed!", K(errno));
              }

              index = compatible_balance_assign(pipefd_pool);
              trace_connection_info(accept_fd);
              if (OB_FAIL(connection_redispatch(accept_fd, pipefd_pool, index))) {
                close(accept_fd);
              }
              pipefd_pool = NULL;
            } else {
                if (epoll_ctl(epoll_fd, EPOLL_CTL_DEL, accept_fd, NULL) < 0) {
                  RPC_LOG(ERROR, "delete accept_fd from epoll failed!", K(errno));
                }

                for (int i = 0; i < MAX_PROTOCOL_TYPE_SIZE; i++) {
                  if (io_wrpipefd_map_[i].used && io_wrpipefd_map_[i].magic == client_magic) {
                    pipefd_pool = &(io_wrpipefd_map_[i].ioth_wrpipefd_pool);
                  }
                }

                trace_connection_info(accept_fd);

                if (OB_ISNULL(pipefd_pool)) { /* high_prio_rpc_eio exist or not is decided by configuration */
                  index = compatible_balance_assign(pipefd_pool);
                } else {
                  RPC_LOG(INFO, "dispatch to", K(client_magic), K(index));
                }

                if(OB_FAIL(connection_redispatch(accept_fd, pipefd_pool, index))) {
                  close(accept_fd);
                }
                pipefd_pool = NULL;
            }
        }
      }
    }
  }

  return;
}

int ObListener::do_one_event(int accept_fd) {
  int err = 0;
  int tmp_ret = OB_SUCCESS;
  uint64_t client_magic = 0;
  uint8_t index = 0;
  io_threads_pipefd_pool_t *pipefd_pool = NULL;

  if (OB_TMP_FAIL(read_client_magic(accept_fd, client_magic, index))) {
    index = compatible_balance_assign(pipefd_pool);
    trace_connection_info(accept_fd);
    if (OB_TMP_FAIL(connection_redispatch(accept_fd, pipefd_pool, index))) {
      close(accept_fd);
    }
    pipefd_pool = NULL;
  } else {
    for (int i = 0; i < MAX_PROTOCOL_TYPE_SIZE; i++) {
      if (io_wrpipefd_map_[i].used && io_wrpipefd_map_[i].magic == client_magic) {
        pipefd_pool = &(io_wrpipefd_map_[i].ioth_wrpipefd_pool);
      }
    }

    trace_connection_info(accept_fd);

    if (OB_ISNULL(pipefd_pool)) { /* high_prio_rpc_eio exist or not is decided by configuration */
      index = compatible_balance_assign(pipefd_pool);
    } else {
      RPC_LOG(INFO, "dispatch to", K(client_magic), K(index));
    }
    if(OB_TMP_FAIL(connection_redispatch(accept_fd, pipefd_pool, index))) {
      close(accept_fd);
    }
    pipefd_pool = NULL;
  }
  return err;
}

 uint8_t ObListener::compatible_balance_assign(io_threads_pipefd_pool_t  * &pipefd_pool)
 {
   static uint8_t ioth_index_inc = 0;
   uint8_t ioth_index = 0;
   uint64_t magic = 0;

   for (int i = 0; i < MAX_PROTOCOL_TYPE_SIZE; i++) {
     if (1 == io_wrpipefd_map_[i].used) {
      pipefd_pool = &(io_wrpipefd_map_[i].ioth_wrpipefd_pool);
      magic = io_wrpipefd_map_[i].magic;
      break;
     }
   }

   ioth_index = ATOMIC_FAA(&ioth_index_inc, 1);
   RPC_LOG(INFO, "round-robin, dispatch to", K(magic), K(ioth_index));

   return ioth_index;
 }

 extern "C" {
   #include "ussl-hook.c"
 };
