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
extern bool straddr_to_addr_c(const char *ip_str, bool *is_ipv6, void *ip);
extern void sockaddr_to_addr_c(struct sockaddr_storage *sock_addr, bool *is_ipv6, void *ip, int *port);
extern char *sockaddr_to_str_c(struct sockaddr_storage *sock_addr, char *buf, int len);
extern struct sockaddr_storage* make_unix_sockaddr_c(bool is_ipv6, void *ip, int port, struct sockaddr_storage *sock_addr);

const char* addr_str(format_t* f, addr_t addr) {
  char buf[INET6_ADDRSTRLEN + 6];
  struct sockaddr_storage sock_addr;
  return format_sf(f, "%s", sockaddr_to_str_c(make_sockaddr(&sock_addr, addr), buf, sizeof(buf)));
}

addr_t* addr_init(addr_t* addr, const char* ip, int port) {
  memset(addr, 0, sizeof(*addr));
  straddr_to_addr_c(ip, &addr->is_ipv6, &addr->ip);
  addr->port = port;
  return addr;
}

void addr_reset(addr_t* addr)
{
  memset(addr, 0, sizeof(*addr));
}

addr_t get_remote_addr(int fd) {
  addr_t addr;
  struct sockaddr_storage sock_addr;
  socklen_t addr_len = sizeof(sock_addr);
  if (0 == getpeername(fd, (struct sockaddr*)&sock_addr, &addr_len)) {
    sockaddr_to_addr_c(&sock_addr, &addr.is_ipv6, &addr.ip, (int*)&addr.port);
  } else {
    addr_reset(&addr);
  }
  return addr;
}

addr_t get_local_addr(int fd) {
  addr_t addr;
  struct sockaddr_storage sock_addr;
  socklen_t addr_len = sizeof(sock_addr);
  if (0 == getpeername(fd, (struct sockaddr*)&sock_addr, &addr_len)) {
    sockaddr_to_addr_c(&sock_addr, &addr.is_ipv6, &addr.ip, (int*)&addr.port);
  } else {
    addr_reset(&addr);
  }
  return addr;
}

static struct sockaddr_storage* rk_make_unix_sockaddr(struct sockaddr_storage *sock_addr, addr_t addr) {
  make_unix_sockaddr_c(addr.is_ipv6, &addr.ip, addr.port, sock_addr);
  return sock_addr;
}

struct sockaddr_storage* make_sockaddr(struct sockaddr_storage* sock_addr, addr_t addr) {
  return rk_make_unix_sockaddr(sock_addr, addr);
}

addr_t *sockaddr_to_addr(struct sockaddr_storage *sock_addr, addr_t *addr)
{
  memset(addr, 0, sizeof(*addr));
  sockaddr_to_addr_c(sock_addr, &addr->is_ipv6, &addr->ip, (int*)&addr->port);
  return addr;
}
