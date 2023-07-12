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

#ifndef USSL_HOOK_LOOP_FACTORY_
#define USSL_HOOK_LOOP_FACTORY_

enum SockType {
  CLIENT_SOCK = 0,
  SERVER_SOCK = 1
};

typedef struct client_fd_info_t
{
  int client_fd;
  uint64_t client_gid;
  int ssl_ctx_id;
  int auth_methods;
  int org_epfd;
  struct epoll_event event;
  int stage;
  int send_negotiation;
} client_fd_info_t;

typedef struct clientfd_sk_t
{
  USSL_SOCK_COMMON;
  ussl_dlink_t timeout_link;
  int type; // 0:client 1:server
  ussl_eloop_t *ep;
  client_fd_info_t fd_info;
  time_t start_time;
} clientfd_sk_t;

typedef struct acceptfd_info_t
{
  uint64_t client_gid; // client git from negotiation msg
  char scramble[16];
  int stage;
} acceptfd_info_t;

typedef struct acceptfd_sk_t
{
  USSL_SOCK_COMMON;
  ussl_dlink_t timeout_link;
  int type; // 0:client 1:server
  ussl_eloop_t *ep;
  acceptfd_info_t fd_info;
  time_t start_time;
} acceptfd_sk_t;

extern int clientfd_sf_init(ussl_sf_t *sf);
extern int acceptfd_sf_init(ussl_sf_t *sf);

#endif // USSL_HOOK_LOOP_FACTORY_