/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <sys/socket.h>
typedef struct listenfd_t {
  SOCK_COMMON;
  bool is_pipe;
  eloop_t* ep;
  sf_t* sf;
} listenfd_t;

typedef struct listenfd_dispatch_t {
  int fd;
  int tid;
  void* sock_ptr;
} listenfd_dispatch_t;

extern int listenfd_init(eloop_t* ep, listenfd_t* s, sf_t* sf, int fd);
