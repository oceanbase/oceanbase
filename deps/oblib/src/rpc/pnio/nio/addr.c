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

const char* addr_str(format_t* f, addr_t addr) {
  char buf[18];
  return format_sf(f, "%s:%hu",
                          inet_ntop(AF_INET, (struct in_addr*)(&addr.ip), buf, sizeof(buf)), addr.port);
}

addr_t addr_build(const char* ip, int port) {
  addr_t addr;
  return *addr_init(&addr, ip, port);
}

addr_t* addr_init(addr_t* addr, const char* ip, int port) {
  *addr = (addr_t){inet_addr(ip), (uint16_t)port, 0};
  return addr;
}

addr_t* addr_set(addr_t* addr, uint32_t ip, uint16_t port, uint16_t id) {
  addr->ip = ip;
  addr->port = port;
  addr->tid = id;
  return addr;
}

void addr_reset(addr_t* addr)
{
  addr_set(addr, 0, 0, 0);
}

addr_t get_remote_addr(int fd) {
  addr_t addr;
  struct sockaddr_in sa;
  socklen_t sa_len = sizeof(sa);
  if (0 == getpeername(fd, (struct sockaddr*)&sa, &sa_len)) {
    int ip = sa.sin_addr.s_addr;
    int port = (int)ntohs(sa.sin_port);
    addr_set(&addr, ip, (uint16_t)port, 0);
  } else {
    addr_reset(&addr);
  }
  return addr;
}

addr_t get_local_addr(int fd) {
  addr_t addr;
  struct sockaddr_in sa;
  socklen_t sa_len = sizeof(sa);
  if (0 == getsockname(fd, (struct sockaddr*)&sa, &sa_len)) {
    int ip = sa.sin_addr.s_addr;
    int port = (int)ntohs(sa.sin_port);
    addr_set(&addr, ip, (uint16_t)port, 0);
  } else {
    addr_reset(&addr);
  }
  return addr;
}

static struct sockaddr_in* rk_make_unix_sockaddr(struct sockaddr_in *sin, in_addr_t ip, int port) {
  if (NULL != sin) {
    sin->sin_port = (uint16_t)htons((uint16_t)port);
    sin->sin_addr.s_addr = ip;
    sin->sin_family = AF_INET;
  }
  return sin;
}

struct sockaddr_in* make_sockaddr(struct sockaddr_in* sin, addr_t addr) {
  return rk_make_unix_sockaddr(sin, addr.ip, addr.port);
}
