#ifndef EASY_SOCKET_H_
#define EASY_SOCKET_H_

#include "easy_define.h"
#include "io/easy_io_struct.h"
#include "io/easy_log.h"
#include <netinet/in.h>

/**
 * socket处理
 */

EASY_CPP_START

#define EASY_FLAGS_DEFERACCEPT 0x001
#define EASY_FLAGS_REUSEPORT   0x002
#define EASY_FLAGS_SREUSEPORT  0x004
#define EASY_FLAGS_NOLISTEN    0x008
#define SO_REUSEPORT 15

int easy_unix_domain_listen(const char* path, int backlog);
int easy_socket_listen(int udp, easy_addr_t *address, int *flags, int backlog);
ssize_t easy_socket_write(easy_connection_t *c, easy_list_t *l);
ssize_t easy_socket_read(easy_connection_t *c, char *buf, size_t size, int *pending);
int easy_socket_non_blocking(int fd);
int easy_socket_set_tcpopt(int fd, int option, int value);
int easy_socket_set_opt(int fd, int option, int value);
int easy_socket_support_ipv6();
ssize_t easy_socket_usend(easy_connection_t *c, easy_list_t *l);
int easy_socket_error(int fd);
int easy_socket_set_linger(int fd, int t);

ssize_t easy_socket_udpwrite(easy_connection_t *conn, struct sockaddr *addr, easy_list_t *l);
ssize_t easy_socket_tcpwrite(easy_connection_t *conn, easy_list_t *l);

EASY_CPP_END

#endif
