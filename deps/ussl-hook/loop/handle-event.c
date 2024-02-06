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

#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <arpa/inet.h>

#define USSL_SCRAMBLE_LEN 16
#define USSL_MAX_KEY_LEN 16
#define AUTH_TYPE_STRING_MAX_LEN 32

enum ClientNegoStage {
  SEND_FIRST_NEGO_MESSAGE = 1,
  DOING_SSL_HANSHAKE = 2,
};

enum ServerNegoStage {
  SERVER_ACCEPT_CONNECTION = 1,
  SERVER_ACK_NEGO_AND_AUTH = 2,
  SERVER_ACK_NEGO_AND_SSL = 3,
};

static void auth_type_to_str(int auth_type, char *buf, size_t len)
{
  if (USSL_AUTH_NONE == auth_type) {
    strncpy(buf, "NONE", len);
  } else if (USSL_AUTH_SSL_HANDSHAKE == auth_type) {
    strncpy(buf, "SSL_NO_ENCRYPT", len);
  } else if (USSL_AUTH_SSL_IO == auth_type) {
    strncpy(buf, "SSL_IO", len);
  }
}

static void get_client_addr(int fd, char *buf, int len)
{
  struct sockaddr_storage addr;
  socklen_t sock_len = sizeof(addr);
  if (0 != getsockname(fd, (struct sockaddr *)&addr, &sock_len)) {
    ussl_log_warn("getsockname failed, fd:%d, errno:%d", fd, errno);
  } else {
    char src_addr[INET6_ADDRSTRLEN];
    if (AF_INET == addr.ss_family) {
      struct sockaddr_in *s = (struct sockaddr_in *)&addr;
      if (NULL != inet_ntop(AF_INET, &s->sin_addr, src_addr, INET_ADDRSTRLEN)) {
        if (snprintf(buf, len, "%s:%d", src_addr, ntohs(s->sin_port)) < 0) {
          ussl_log_warn("snprintf failed, errno:%d", errno);
        }
      } else {
        ussl_log_warn("call inet_ntop for AF_INET failed, errno:%d", errno);
      }
    } else {
      struct sockaddr_in6 *s = (struct sockaddr_in6 *)&addr;
      if (NULL != inet_ntop(AF_INET6, &s->sin6_addr, src_addr, INET6_ADDRSTRLEN)) {
        if (snprintf(buf, len, "[%s]:%d", src_addr, ntohs(s->sin6_port)) < 0) {
          ussl_log_warn("snprintf failed, errno:%d", errno);
        }
      } else {
        ussl_log_warn("call inet_ntop for AF_INET6 failed, errno:%d", errno);
      }
    }
  }
}

static int is_local_ip_address(const char *addr)
{
  int ret = 0;
  if (NULL != strstr(addr, "127.0.0.1")) {
    ret = 1;
  }
  return ret;
}

static int handle_client_writable_event(ussl_sock_t *s)
{
  int ret = EAGAIN;
  int err = 0;
  int so_error = 0;
  int need_giveback = 0;
  socklen_t len = sizeof(err);
  clientfd_sk_t *cs = (clientfd_sk_t *)s;
  if (-1 == (err = getsockopt(cs->fd, SOL_SOCKET, SO_ERROR, (void *)&so_error, &len))) {
    ussl_log_error("call getsockopt failed, fd:%d, errno:%d", cs->fd, errno);
  } else if (0 != so_error) {
    ussl_log_warn("there is an error on the socket, fd:%d, so_error:%d", cs->fd, so_error);
  } else {
    // 1.remode EPOLLOUT & add EPOLLIN
    cs->mask &= ~EPOLLOUT;
    struct epoll_event event;
    uint32_t new_flags = EPOLLIN | EPOLLERR;
    int client_am = get_client_auth_methods();
    if (0 != (err = libc_epoll_ctl(cs->ep->fd, EPOLL_CTL_MOD, cs->fd,
                                   ussl_make_epoll_event(&event, new_flags, (ussl_sock_t *)cs)))) {
      ussl_log_error("modify epoll flag failed, fd:%d, errno:%d", cs->fd, errno);
    } else { // 3.send negotiation message
      int need_send_negotiation = 1;
      if (USSL_AUTH_NONE == client_am) {
        if (1 == cs->fd_info.send_negotiation) {
          need_send_negotiation = 1;
        } else {
          need_send_negotiation = 0;
        }
        need_giveback = 1;
      } else {
        need_send_negotiation = 1;
      }
      if (1 == need_send_negotiation) {
        negotiation_message_t nego_msg;
        nego_msg.type = client_am;
        nego_msg.client_gid = cs->fd_info.client_gid;
        if (0 != (err = send_negotiation_message(cs->fd, (char *)&nego_msg, sizeof(nego_msg)))) {
          ussl_log_warn("send negotiation message failed, fd:%d, err:%d, errno:%d", cs->fd, err,
                        errno);
        } else { // 4.add to timeout list (if needed)
          // succ log
          char client_addr[IP_STRING_MAX_LEN] = {0};
          get_client_addr(cs->fd, client_addr, IP_STRING_MAX_LEN);
          char auth_type[AUTH_TYPE_STRING_MAX_LEN] = {0};
          auth_type_to_str(nego_msg.type, auth_type, AUTH_TYPE_STRING_MAX_LEN);
          ussl_log_info("client send negotiation message succ, fd:%d, addr:%s, auth_method:%s, gid:0x%lx",
                        cs->fd, client_addr, auth_type, cs->fd_info.client_gid);
          if (USSL_AUTH_NONE == client_am) {
            need_giveback = 1;
          } else {
            if (is_local_ip_address(client_addr)) {
                need_giveback = 1;
            } else {
                cs->start_time = time(NULL);
                add_to_timeout_list(&cs->timeout_link);
                cs->fd_info.stage = SEND_FIRST_NEGO_MESSAGE;
                ret = EAGAIN;
            }
          }
        }
      }
    }
  }
  if (0 != err  || 0 != so_error || need_giveback) {
    s->has_error = ((err != 0) || (so_error != 0)) ? 1 : 0;
    ret = EUCLEAN;
  }
  return ret;
}

static int client_do_ssl_handshake(clientfd_sk_t *cs)
{
  int ret = EAGAIN;
  int err = 0;
  err = ssl_do_handshake(cs->fd);
  if (0 == err) {
    // stop timer and give back
    cs->has_error = 0;
    ret = EUCLEAN;
  } else if (EAGAIN == err) {
    ret = EAGAIN;
  } else {
    ussl_log_warn("client do ssl handshake failed, fd:%d, err:%d, errno:%d", cs->fd, err, errno);
    cs->has_error = 1;
    ret = EUCLEAN;
  }
  return ret;
}

static int handle_client_readable_event(ussl_sock_t *s)
{
  int ret = EAGAIN;
  clientfd_sk_t *cs = (clientfd_sk_t *)s;
  char client_addr[IP_STRING_MAX_LEN] = {0};
  get_client_addr(cs->fd, client_addr, IP_STRING_MAX_LEN);
  char auth_type[AUTH_TYPE_STRING_MAX_LEN] = {0};
  auth_type_to_str(cs->fd_info.auth_methods, auth_type, AUTH_TYPE_STRING_MAX_LEN);
  if (SEND_FIRST_NEGO_MESSAGE == cs->fd_info.stage) {
    int64_t rbytes = 0;
    char buf[USSL_BUF_LEN];
    // peek
    while ((rbytes = recv(cs->fd, buf, sizeof(buf), MSG_PEEK)) < 0 && EINTR == errno)
      ;
    if (0 == rbytes) {
      ret = EUCLEAN;
      cs->has_error = 1;
      ussl_log_info("read EOF, fd:%d, src_addr:%s", cs->fd, client_addr);
    } else if (rbytes < 0) {
      if (EINTR == errno) {
        ret = 0;
      } else if (EAGAIN == errno || EWOULDBLOCK == errno) {
        s->mask &= ~EPOLLIN;
        ret = EAGAIN;
      } else {
        s->has_error = 1;
        ret = EUCLEAN;
        ussl_log_warn("read failed, fd:%d, errno:%d", s->fd, errno);
      }
    } else if (rbytes < sizeof(negotiation_head_t)) {
      ussl_log_warn("recv message is not complete, close connection, rbytes:%ld, fd:%d", rbytes, cs->fd);
      cs->has_error = 1;
      ret = EUCLEAN;
    } else { // get mag len & read msg
      negotiation_head_t msg_head;
      memcpy(&msg_head, buf, sizeof(msg_head));
      if (NEGOTIATION_MAGIC != msg_head.magic) {
        cs->has_error = 1;
        ret = EUCLEAN;
      } else if (rbytes < sizeof(negotiation_head_t) + msg_head.len) {
        ussl_log_warn("recv message is not complete, close connection, rbytes:%ld, fd:%d", rbytes, cs->fd);
        cs->has_error = 1;
        ret = EUCLEAN;
      } else {
        while ((rbytes = recv(cs->fd, buf, sizeof(msg_head) + msg_head.len, 0)) < 0 &&
              EINTR == errno)
          ;
        if (rbytes != sizeof(msg_head) + msg_head.len) {
          ussl_log_warn("recv data failed, fd:%d, errno:%d, rbytes:%ld", cs->fd, errno, rbytes);
          cs->has_error = 1;
          ret = EUCLEAN;
        } else {
          negotiation_message_t nego_msg;
          memcpy(&nego_msg, buf + sizeof(msg_head), sizeof(nego_msg));
          if (USSL_AUTH_SSL_HANDSHAKE == nego_msg.type || USSL_AUTH_SSL_IO == nego_msg.type) {
            // do ssl handshake
            if (0 !=
                (ret = fd_enable_ssl_for_client(cs->fd, cs->fd_info.ssl_ctx_id, nego_msg.type))) {
              cs->has_error = 1;
              ussl_log_error("create SSL failed, fd:%d, errno:%d", s->fd, errno);
            } else {
              ussl_log_info("client do ssl handshake first, fd:%d, addr:%s, auth_method:%s", cs->fd,
                             client_addr, auth_type);
              ret = client_do_ssl_handshake(cs);
              if (EAGAIN == ret) {
                cs->fd_info.stage = DOING_SSL_HANSHAKE;
              }
            }
          }
        }
      }
    }
  } else {
    ussl_log_info("client do ssl handshake again, fd:%d, addr:%s, auth_method:%s", cs->fd,
              client_addr, auth_type);
    ret = client_do_ssl_handshake(cs);
  }
  return ret;
}

int clientfd_sk_handle_event(clientfd_sk_t *s)
{
  int ret = EAGAIN;
  if (ussl_skt(s, OUT)) {
    ret = handle_client_writable_event((ussl_sock_t *)s);
  } else if (ussl_skt(s, IN)) {
    ret = handle_client_readable_event((ussl_sock_t *)s);
  }
  return ret;
}

void ussl_get_peer_addr(int fd, char *buf, int len)
{
  struct sockaddr_storage addr;
  socklen_t sock_len = sizeof(addr);
  if (0 != getpeername(fd, (struct sockaddr *)&addr, &sock_len)) {
    ussl_log_warn("getpeername failed, fd:%d, errno:%d", fd, errno);
  } else {
    char src_addr[INET6_ADDRSTRLEN];
    if (AF_INET == addr.ss_family) {
      struct sockaddr_in *s = (struct sockaddr_in *)&addr;
      if (NULL != inet_ntop(AF_INET, &s->sin_addr, src_addr, INET_ADDRSTRLEN)) {
        if (snprintf(buf, len, "%s:%d", src_addr, ntohs(s->sin_port)) < 0) {
          ussl_log_warn("snprintf failed, errno:%d", errno);
        }
      } else {
        ussl_log_warn("call inet_ntop for AF_INET failed, errno:%d", errno);
      }
    } else {
      struct sockaddr_in6 *s = (struct sockaddr_in6 *)&addr;
      if (NULL != inet_ntop(AF_INET6, &s->sin6_addr, src_addr, INET6_ADDRSTRLEN)) {
        if (snprintf(buf, len, "[%s]:%d", src_addr, ntohs(s->sin6_port)) < 0) {
          ussl_log_warn("snprintf failed, errno:%d", errno);
        }
      } else {
        ussl_log_warn("call inet_ntop for AF_INET6 failed, errno:%d", errno);
      }
    }
  }
}

static int acceptfd_handle_first_readable_event(acceptfd_sk_t *s)
{
  int err = 0;
  char buf[USSL_BUF_LEN];
  ssize_t rbytes = 0;
  while ((rbytes = recv(s->fd, buf, sizeof(buf), MSG_PEEK)) < 0 && EINTR == errno);
  negotiation_head_t *h = (typeof(h))buf;
  char src_addr[IP_STRING_MAX_LEN] = {0};
  ussl_get_peer_addr(s->fd, src_addr, IP_STRING_MAX_LEN);
  if (0 == rbytes) {
    err = EUCLEAN;
    s->has_error = 1;
    ussl_log_info("read EOF, fd:%d, src_addr:%s", s->fd, src_addr);
  } else if (rbytes < 0) {
    if (EINTR == errno) {
    } else if (EAGAIN == errno || EWOULDBLOCK == errno) {
      s->mask &= ~EPOLLIN;
      err = EAGAIN;
    } else {
      err = EUCLEAN;
      s->has_error = 1;
      ussl_log_info("recv failed, fd:%d, errno:%d, src_addr:%s", s->fd, errno, src_addr);
    }
  } else if (rbytes < sizeof(negotiation_head_t)) {
    err = EUCLEAN;
    s->has_error = 1;
    ussl_log_info("read EOF, fd:%d, src_addr:%s", s->fd, src_addr);
  } else if (h->magic != NEGOTIATION_MAGIC) {
    int need_dispatch = 0;
    if (test_server_auth_methods(USSL_AUTH_NONE)) {
      need_dispatch = 1;
    } else if (is_local_ip_address(src_addr)) {
      ussl_log_info("local ip address:%s, need dispatch", src_addr);
      need_dispatch = 1;
    } else {
      need_dispatch = is_net_keepalive_connection(rbytes, buf);
      ussl_log_info("easy negotation message, need dispatch:%d, src:%s, fd:%d", need_dispatch, src_addr, s->fd);
    }
    if (need_dispatch) {
      err = EUCLEAN;
      s->fd_info.client_gid = UINT64_MAX;
      ussl_log_info("recv non-negotiation message, the fd will be dispatched, fd:%d, src_addr:%s, magic:0x%x",
              s->fd, src_addr, h->magic);
    } else {
      char auth_type[AUTH_TYPE_STRING_MAX_LEN] = {0};
      auth_type_to_str(get_server_auth_methods(), auth_type, AUTH_TYPE_STRING_MAX_LEN);
      err = EUCLEAN;
      s->has_error = 1;
      ussl_log_warn("connection is not allowed, fd:%d, src_addr:%s, server_auth_method:%s, "
                     "rbytes:%ld, magic:%x",
                     s->fd, src_addr, auth_type, rbytes, h->magic);
    }
  } else if (h->len + sizeof(*h) > rbytes) {
    err = EUCLEAN;
    s->has_error = 1;
    ussl_log_warn("recv message is not complete, close connection, rbytes:%ld, fd:%d", rbytes, s->fd);
  } else {
    while ((rbytes = recv(s->fd, buf, h->len + sizeof(negotiation_head_t), 0)) < 0 &&
           EINTR == errno)
      ;
    if (rbytes != h->len + sizeof(negotiation_head_t)) {
      err = EUCLEAN;
      s->has_error = 1;
      ussl_log_warn("consume nego message failed, rbytes:%ld, fd:%d, errno:%d", rbytes, s->fd,
                     errno);
    } else {
      if (is_local_ip_address(src_addr)) {
        //TODO fix me
        //if observer use local loop ip to start service, there will be error here
        err = EUCLEAN;
        s->fd_info.client_gid = UINT64_MAX;
        ussl_log_info("local ip address:%s, dispatch after consume", src_addr);
      } else {
        negotiation_message_t *nego_message = (typeof(nego_message))(h + 1);
        s->fd_info.client_gid = nego_message->client_gid;
        char auth_type[AUTH_TYPE_STRING_MAX_LEN] = {0};
        auth_type_to_str(nego_message->type, auth_type, AUTH_TYPE_STRING_MAX_LEN);
        if (USSL_AUTH_NONE == nego_message->type) {
          if (test_server_auth_methods(USSL_AUTH_NONE)) {
            err = EUCLEAN;
            s->fd_info.client_gid = nego_message->client_gid;
            ussl_log_info("auth mothod is NONE, the fd will be dispatched, fd:%d, src_addr:%s", s->fd,
                          src_addr);
          } else {
            err = EUCLEAN;
            s->has_error = 1;
            ussl_log_warn("ussl server not support mode:%s, fd:%d", auth_type, s->fd);
          }
        } else if (USSL_AUTH_SSL_IO == nego_message->type ||
                  USSL_AUTH_SSL_HANDSHAKE == nego_message->type) {
          if (test_server_auth_methods(USSL_AUTH_SSL_IO) ||
              test_server_auth_methods(USSL_AUTH_SSL_HANDSHAKE)) {
            if (-1 == ssl_config_ctx_id) {
              err = EUCLEAN;
              s->has_error = 1;
              ussl_log_error("ssl config not configured!");
            } else {
              negotiation_message_t nego_message_ack;
              nego_message_ack.type = nego_message->type;
              if (0 != fd_enable_ssl_for_server(s->fd, ssl_config_ctx_id, nego_message->type)) {
                err = EUCLEAN;
                s->has_error = 1;
                ussl_log_error("fd_enable_ssl_for_server failed, fd:%d", s->fd);
              } else if (0 != send_negotiation_message(s->fd, (char *)&nego_message_ack,
                                                      sizeof(nego_message_ack))) {
                err = EUCLEAN;
                s->has_error = 1;
                ussl_log_warn("send_negotiation_message failed, auth-mode:%d, fd:%d",
                              nego_message->type, s->fd);
              } else {
                ussl_log_info("auth method is SSL_NO_ENCRYPT or SSL_IO, and the negotiation message "
                              "has be sent, fd:%d, src_addr:%s",
                              s->fd, src_addr);
                s->fd_info.stage = SERVER_ACK_NEGO_AND_SSL;
                err = EAGAIN;
              }
            }
          } else {
            err = EUCLEAN;
            s->has_error = 1;
            ussl_log_warn("ussl server not support mode:%s, fd:%d", auth_type, s->fd);
          }
        }
      }
    }
  }
  return err;
}

static int acceptfd_handle_ssl_event(acceptfd_sk_t *s)
{
  int ret = 0;
  char src_addr[IP_STRING_MAX_LEN] = {0};
  ussl_get_peer_addr(s->fd, src_addr, IP_STRING_MAX_LEN);
  ret = ssl_do_handshake(s->fd);
  if (0 == ret) {
    ret = EUCLEAN;
    ussl_log_info("ssl_do_handshake succ, fd:%d, client_gid:%lu, src_addr:%s", s->fd, s->fd_info.client_gid, src_addr);
  } else if (EAGAIN == ret) {
  } else {
    s->has_error = 1;
    ussl_log_warn("ssl_do_handshake failed, fd:%d, ret:%d, src_addr:%s", s->fd, ret, src_addr);
  }
  return ret;
}

int acceptfd_sk_handle_event(acceptfd_sk_t *s)
{
  int ret = 0;
  if (ussl_skt(s, IN)) {
    if (SERVER_ACCEPT_CONNECTION == s->fd_info.stage) {
      ret = acceptfd_handle_first_readable_event(s);
    } else if (SERVER_ACK_NEGO_AND_SSL == s->fd_info.stage) {
      ret = acceptfd_handle_ssl_event(s);
    }
  }
  return ret;
}