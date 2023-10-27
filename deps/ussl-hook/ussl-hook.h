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

#ifndef USSL_HOOK_USSL_HOOK_
#define USSL_HOOK_USSL_HOOK_

#ifdef OVERRIDE_SYSCALL
#define ussl_setsockopt setsockopt
#define ussl_listen listen
#define ussl_connect connect
#define ussl_accept accept
#define ussl_accept4 accept4
#define ussl_epoll_ctl epoll_ctl
#define ussl_write write
#define ussl_read read
#define ussl_close close
#endif

#define SOL_OB_SOCKET -1
#define SOL_OB_CTX -2

enum SocketLevelOptname {
  SO_OB_SET_CLIENT_GID = 1,
  SO_OB_SET_SERVER_GID = 2,
  SO_OB_SET_CLIENT_SSL_CTX_ID = 3,
  SO_OB_SET_SERVER_SSL_CTX_ID = 4,
  SO_OB_SET_SEND_NEGOTIATION_FLAG = 5
};

enum UsslAuthMethods {
  USSL_AUTH_NONE = 1,
  USSL_AUTH_SSL_HANDSHAKE = 2,
  USSL_AUTH_SSL_IO = 4
};

enum CtxLevelOptName {
  SO_OB_CTX_SERVER_AUTH_METHODS = 1, // bitmask: ref UsslAuthMethods
  SO_OB_CTX_CLIENT_AUTH_METHODS,
  SO_OB_CTX_SET_PLAIN_KEY_DIR,
  SO_OB_CTX_SET_PLAIN_AUTH_LIST_DIR,
  SO_OB_CTX_SET_SSL_KEY_DIR,
  SO_OB_CTX_SET_SSL_CONFIG,
};

void ussl_stop();
void ussl_wait();

int ussl_setsockopt(int sockfd, int level, int optname, const void *optval, socklen_t optlen);
int ussl_listen(int fd, int n);
int ussl_connect(int fd, const struct sockaddr *addr, socklen_t len);
int ussl_accept(int fd, struct sockaddr *addr, socklen_t *addr_len);
int ussl_accept4(int fd, struct sockaddr *addr, socklen_t *addr_len, int flags);
int ussl_epoll_ctl(int epfd, int op, int fd, struct epoll_event *event);
ssize_t ussl_read(int fd, char *buf, size_t nbytes);
ssize_t ussl_write(int fd, const void *buf, size_t nbytes);
ssize_t ussl_writev(int fildes, const struct iovec *iov, int iovcnt);
int ussl_close(int fd);

typedef struct ssl_config_item_t
{
  int is_from_file;
  int is_sm;
  const char *ca_cert;
  const char *sign_cert;
  const char *sign_private_key;
  const char *enc_cert;
  const char *enc_private_key;
  const char *ssl_invited_nodes; // the list of observers to enable SSL
} ssl_config_item_t;

extern int ob_epoll_wait(int __epfd, struct epoll_event *__events,
		                     int __maxevents, int __timeout);

#endif // USSL_HOOK_USSL_HOOK_
