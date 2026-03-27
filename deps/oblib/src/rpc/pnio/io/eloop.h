/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

typedef struct eloop_t {
  int fd;
  dlink_t ready_link;
  rl_impl_t rl_impl;
  int8_t thread_usage[12];
} eloop_t;

extern int eloop_init(eloop_t* ep);
extern int eloop_thread_run(eloop_t** ep);
extern int eloop_run(eloop_t* ep);
extern int eloop_unregist(eloop_t* ep, sock_t* s);
extern int eloop_regist(eloop_t* ep, sock_t* s, uint32_t eflag);
extern void eloop_fire(eloop_t* ep, sock_t* s);
