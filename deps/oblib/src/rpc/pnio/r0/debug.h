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

struct stat_time_guard_t {
  int64_t start;
  int64_t *cnt;
  int64_t *time;
  const char *procedure;
};

extern __thread int64_t eloop_malloc_count;
extern __thread int64_t eloop_malloc_time;
extern __thread int64_t eloop_write_count;
extern __thread int64_t eloop_write_time;
extern __thread int64_t eloop_read_count;
extern __thread int64_t eloop_read_time;
extern __thread int64_t eloop_client_cb_count;
extern __thread int64_t eloop_client_cb_time;
extern __thread int64_t eloop_server_process_count;
extern __thread int64_t eloop_server_process_time;

void stat_cleanup(void *s);
inline void reset_eloop_time_stat() {
  eloop_malloc_count = 0;
  eloop_malloc_time = 0;
  eloop_write_count = 0;
  eloop_write_time = 0;
  eloop_read_count = 0;
  eloop_read_time = 0;
  eloop_client_cb_count = 0;
  eloop_client_cb_time = 0;
  eloop_server_process_count = 0;
  eloop_server_process_time = 0;
}
#define STAT_TIME_GUARD(_cnt, _time)                                                                  \
  struct stat_time_guard_t _tg_stat_time_guard __attribute__((cleanup(stat_cleanup))) = {             \
      .start = rk_get_corse_us(),                                                                     \
      .cnt = &(_cnt),                                                                                 \
      .time = &(_time),                                                                               \
      .procedure = __FUNCTION__,                                                                      \
  };

#define PNIO_REACH_TIME_INTERVAL(i)               \
  ({                                              \
    bool bret = false;                            \
    static __thread int64_t last_time = 0;        \
    int64_t cur_time = rk_get_us();               \
    if ((i + last_time < cur_time))               \
    {                                             \
      last_time = cur_time;                       \
      bret = true;                                \
    }                                             \
    bret;                                         \
  })

inline void eloop_delay_warn(int64_t start_us, int64_t warn_us) {
  if (warn_us > 0) {
    int64_t delay = rk_get_corse_us() - start_us;
    if (delay > warn_us) {
      rk_warn("[delay_warn] eloop handle events delay high: %ld, malloc=%ld/%ld write=%ld/%ld read=%ld/%ld server_process=%ld/%ld client_cb=%ld/%ld",
        delay, eloop_malloc_time, eloop_malloc_count, eloop_write_time, eloop_write_count, eloop_read_time, eloop_read_count,
        eloop_server_process_time, eloop_server_process_count, eloop_client_cb_time, eloop_client_cb_count);
    }
  }
}

void delay_warn(const char* msg, int64_t start_us, int64_t warn_us)
{
  if (warn_us > 0) {
    int64_t delay = rk_get_corse_us() - start_us;
    if (delay > warn_us && PNIO_REACH_TIME_INTERVAL(500*1000)) {
      rk_warn("[delay_warn] %s delay high: %ld, start_us=%ld", msg, delay, start_us);
    }
  }
}
