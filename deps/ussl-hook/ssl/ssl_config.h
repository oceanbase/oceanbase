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

#pragma once

int ssl_config_ctx_id = -1;

enum SSL_ROLE {
  SSL_ROLE_CLIENT,
  SSL_ROLE_SERVER,
  SSL_ROLE_MAX,
};

int ssl_load_config(int ctx_id, const ssl_config_item_t *ssl_config);
int fd_enable_ssl_for_server(int fd, int ctx_id, int type, int has_method_none);
int fd_enable_ssl_for_client(int fd, int ctx_id, int type);
void fd_disable_ssl(int fd);
int ssl_do_handshake(int fd);
SSL_CTX* ussl_get_server_ctx(int ctx_id);
ssize_t read_regard_ssl(int fd, char *buf, size_t nbytes);
ssize_t write_regard_ssl(int fd, const void *buf, size_t nbytes);
ssize_t writev_regard_ssl(int fildes, const struct iovec *iov, int iovcnt);
