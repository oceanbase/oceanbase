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

#ifndef USSL_HOOK_LOOP_ELOOP_
#define USSL_HOOK_LOOP_ELOOP_

typedef struct ussl_eloop_t
{
  int fd;
  ussl_dlink_t ready_link;
} ussl_eloop_t;

extern int ussl_eloop_init(ussl_eloop_t *ep);
extern int ussl_eloop_run(ussl_eloop_t *ep);
extern int ussl_eloop_regist(ussl_eloop_t *ep, ussl_sock_t *s, uint32_t eflag);

#endif // USSL_HOOK_LOOP_ELOOP_
