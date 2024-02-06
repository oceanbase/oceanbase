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

#ifndef USSL_HOOK_LOOP_HANDLE_EVENT_
#define USSL_HOOK_LOOP_HANDLE_EVENT_

#define IP_STRING_MAX_LEN 64

extern int clientfd_sk_handle_event(clientfd_sk_t *s);
extern int acceptfd_sk_handle_event(acceptfd_sk_t *s);
extern void ussl_get_peer_addr(int fd, char *buf, int len);
extern int is_net_keepalive_connection(ssize_t rbytes, char *buf);
#endif // USSL_HOOK_LOOP_HANDLE_EVENT_