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

typedef struct eloop_t {
  int fd;
  dlink_t ready_link;
  rl_impl_t rl_impl;
} eloop_t;

extern int eloop_init(eloop_t* ep);
extern int eloop_thread_run(eloop_t** ep);
extern int eloop_run(eloop_t* ep);
extern int eloop_unregist(eloop_t* ep, sock_t* s);
extern int eloop_regist(eloop_t* ep, sock_t* s, uint32_t eflag);
extern void eloop_fire(eloop_t* ep, sock_t* s);
