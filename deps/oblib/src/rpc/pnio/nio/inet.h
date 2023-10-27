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
extern int set_tcp_recv_buf(int fd, int size);
extern int set_tcp_send_buf(int fd, int size);
extern const char* sock_fd_str(format_t* f, int fd);
