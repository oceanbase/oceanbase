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

#ifndef USSL_HOOK_LOOP_PIPEFD_
#define USSL_HOOK_LOOP_PIPEFD_

typedef struct pipefd_t
{
  USSL_SOCK_COMMON;
  // when pipefd is readable, it needs to add new events(i.e. call epoll_ctl) via ep->fd
  ussl_eloop_t *ep;
} pipefd_t;

extern int pipefd_init(ussl_eloop_t *ep, pipefd_t *s, ussl_sf_t *sf, int fd);

#endif // USSL_HOOK_LOOP_PIPEFD_
