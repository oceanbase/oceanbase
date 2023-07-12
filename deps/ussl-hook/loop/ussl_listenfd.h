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

#ifndef USSL_HOOK_LOOP_LISTENFD_
#define USSL_HOOK_LOOP_LISTENFD_

typedef struct ussl_listenfd_t
{
  USSL_SOCK_COMMON;
  ussl_eloop_t *ep;
} ussl_listenfd_t;

extern int ussl_listenfd_init(ussl_eloop_t *ep, ussl_listenfd_t *s, ussl_sf_t *sf, int fd);
#endif // USSL_HOOK_LOOP_LISTENFD_
