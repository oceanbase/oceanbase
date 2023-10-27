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

#ifndef OB_SSL_CONFIG_H_
#define OB_SSL_CONFIG_H_

#include <stdint.h>
#include <openssl/ssl.h>

namespace oceanbase {
namespace common {

typedef struct ObSSLConfig {
  ObSSLConfig(int is_from_file, int is_sm, const char* ca_cert, const char* sign_cert,
              const char* sign_private_key, const char* enc_cert, const char* enc_private_key):
              is_from_file_(is_from_file), is_sm_(is_sm), ca_cert_(ca_cert), sign_cert_(sign_cert),
              sign_private_key_(sign_private_key), enc_cert_(enc_cert), enc_private_key_(enc_cert) {}
  int is_from_file_;
  int is_sm_;
  const char* ca_cert_;
  const char* sign_cert_;
  const char* sign_private_key_;
  const char* enc_cert_;
  const char* enc_private_key_;
} ObSSLConfig;

enum OB_SSL_CTX_ID {
  OB_SSL_CTX_ID_SQL_NIO,
  OB_SSL_CTX_ID_MAX
};

enum OB_SSL_ROLE {
  OB_SSL_ROLE_CLIENT,
  OB_SSL_ROLE_SERVER,
  OB_SSL_ROLE_MAX
};

int  ob_ssl_load_config(int ctx_id, const ObSSLConfig& ssl_config);
int  ob_fd_enable_ssl_for_server(int fd, int ctx_id, uint64_t tls_option);
int  ob_fd_enable_ssl_for_client(int fd, int ctx_id);
void  ob_fd_disable_ssl(int fd);
SSL* ob_fd_get_ssl_st(int fd);
ssize_t ob_read_regard_ssl(int fd, void *buf, size_t nbytes);
ssize_t ob_write_regard_ssl(int fd, const void *buf, size_t nbytes);
}
}

#endif