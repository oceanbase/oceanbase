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

extern void delay_warn(const char* msg, int64_t start_us, int64_t warn_us);
extern void eloop_delay_warn(int64_t start_us, int64_t warn_us);
extern void reset_eloop_time_stat();

__thread int64_t eloop_malloc_count;
__thread int64_t eloop_malloc_time;
__thread int64_t eloop_write_count;
__thread int64_t eloop_write_time;
__thread int64_t eloop_read_count;
__thread int64_t eloop_read_time;
__thread int64_t eloop_client_cb_count;
__thread int64_t eloop_client_cb_time;
__thread int64_t eloop_server_process_count;
__thread int64_t eloop_server_process_time;

void stat_cleanup(void *s) {
  int64_t cost = rk_get_corse_us() - ((struct stat_time_guard_t *)s)->start;
  int64_t *cnt = ((struct stat_time_guard_t *)s)->cnt;
  *cnt += 1;
  int64_t *time = ((struct stat_time_guard_t *)s)->time;
  *time += cost;
  if (cost > ELOOP_WARN_US) {
    rk_warn("[delay_warn] cost too much time: %ldus, procedure: %s", cost, ((struct stat_time_guard_t *)s)->procedure);
  }
}
