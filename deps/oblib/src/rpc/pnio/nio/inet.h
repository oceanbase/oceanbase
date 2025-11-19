/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include <netinet/tcp.h>
#define PNIO_NIO_ADDR_LEN (INET6_ADDRSTRLEN + 6)
#define PNIO_NIO_FD_ADDR_LEN 140
#define PNIO_NIO_SOCK_ADDR_LEN 160
extern int async_connect(addr_t dest, uint64_t dispatch_id);
extern int listen_create(addr_t src);
extern int tcp_accept(int fd);
extern int check_connect_result(int fd);
extern int set_tcp_reuse_addr(int fd);
extern int set_tcp_reuse_port(int fd);
extern int set_tcp_linger_on(int fd);
extern int set_tcp_nodelay(int fd);
extern int set_tcpopt(int fd, int option, int value);
extern void update_socket_keepalive_params(int fd, int64_t user_timeout);
extern int check_socket_write_ack(int fd, socket_diag_info_t* sk_diag_info, const int64_t user_timeout);
extern int set_tcp_recv_buf(int fd, int size);
extern int set_tcp_send_buf(int fd, int size);
extern const char* sock_fd_str(int fd, char *buf, int buf_len);
extern const char* sock_t_str(sock_t* sock, socket_diag_info_t* info, char *buf, int buf_len);
#define my_sock_t_str(SOCK, BUF, BUF_LEN) sock_t_str((sock_t*)SOCK, &SOCK->sk_diag_info, BUF, BUF_LEN)
