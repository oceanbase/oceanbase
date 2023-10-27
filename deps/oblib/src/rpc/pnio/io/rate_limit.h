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

#define RL_SLEEP_TIME_US 100000 // determine the min sleep time when the socket is rate-limited
typedef struct rl_timerfd_t {
  SOCK_COMMON;
} rl_timerfd_t;

typedef struct rl_impl_t {
  dlink_t ready_link;
  int64_t bw;
  rl_timerfd_t rlfd;
} rl_impl_t;

void rl_sock_push(rl_impl_t* rl, sock_t* sk);