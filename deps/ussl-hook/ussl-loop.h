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

#ifndef USSL_HOOK_LOOP_USSL_LOOP_
#define USSL_HOOK_LOOP_USSL_LOOP_

// the client communicates with the background thread through this pipe
int client_comm_pipe[2];

extern int ussl_loop_add_listen(int listen_fd, int backlog);
extern int ussl_loop_add_clientfd(int client_fd, uint64_t gid, int ctx_id, int send_negotiation, int auth_methods, int epfd,
                                  struct epoll_event *event);
int  __attribute__((weak)) dispatch_accept_fd_to_certain_group(int fd, uint64_t gid);
extern void add_to_timeout_list(ussl_dlink_t *l);
extern void remove_from_timeout_list(ussl_dlink_t *l);
extern void check_and_handle_timeout_event();
extern int ussl_init_bg_thread();
extern void ussl_wait_bg_thread();
#endif // USSL_HOOK_LOOP_USSL_LOOP_
