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

typedef int (*dispatch_fd_func_t)(int fd, const void* buf, int sz);
typedef struct listen_t
{
  eloop_t ep;
  listenfd_t listenfd;
  sf_t sf;
  dispatch_fd_func_t dispatch_func;
} listen_t;

extern int send_dispatch_handshake(int fd, const char* b, int sz);
extern int listen_init(listen_t* l, addr_t addr, dispatch_fd_func_t dispatch_func);
