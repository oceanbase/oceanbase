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

static bool is_epoll_handle_timeout(int64_t time_limit)
{
  return time_limit > 0 && rk_get_corse_us() > time_limit;
}

static int64_t get_epoll_handle_time_limit()
{
  return EPOLL_HANDLE_TIME_LIMIT > 0?  rk_get_corse_us() + EPOLL_HANDLE_TIME_LIMIT: -1;
}
