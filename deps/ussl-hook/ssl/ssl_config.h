#pragma once

int ssl_config_ctx_id = -1;

enum SSL_ROLE {
  SSL_ROLE_CLIENT,
  SSL_ROLE_SERVER,
  SSL_ROLE_MAX,
};

int ssl_load_config(int ctx_id, const ssl_config_item_t *ssl_config);
int fd_enable_ssl_for_server(int fd, int ctx_id, int type);
int fd_enable_ssl_for_client(int fd, int ctx_id, int type);
void fd_disable_ssl(int fd);
int ssl_do_handshake(int fd);
ssize_t read_regard_ssl(int fd, char *buf, size_t nbytes);
ssize_t write_regard_ssl(int fd, const void *buf, size_t nbytes);
ssize_t writev_regard_ssl(int fildes, const struct iovec *iov, int iovcnt);
