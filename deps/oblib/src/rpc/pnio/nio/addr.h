/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once
#include <sys/socket.h>
#include <arpa/inet.h>

typedef struct addr_t {
  bool is_ipv6;
  union {
    uint32_t ip;
    uint8_t ipv6[16];
  };
  uint16_t port;
  uint16_t tid;
} addr_t;
extern const char* addr_str(addr_t addr, char *buf, int buf_len);
extern addr_t* addr_init(addr_t* addr, const char* ip, int port);

extern struct sockaddr_storage* make_sockaddr(struct sockaddr_storage *sock_addr, addr_t addr);
extern addr_t get_remote_addr(int fd);
extern addr_t get_local_addr(int fd);
extern addr_t* sockaddr_to_addr(struct sockaddr_storage *sock_addr, addr_t *addr);
