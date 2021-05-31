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

#ifndef EASY_LOG_H_
#define EASY_LOG_H_

#include "easy_define.h"
#include "util/easy_string.h"
#include "io/easy_baseth_pool.h"
#include "util/easy_time.h"

EASY_CPP_START

typedef void (*easy_log_print_pt)(const char* message);
typedef void (*easy_log_format_pt)(
    int level, const char* file, int line, const char* function, uint64_t location_hash_val, const char* fmt, ...);
typedef enum {
  EASY_LOG_OFF = -2,
  EASY_LOG_FATAL,
  EASY_LOG_ERROR,
  EASY_LOG_USER_ERROR,  // align with observer
  EASY_LOG_WARN,
  EASY_LOG_INFO,
  EASY_LOG_DEBUG,
  EASY_LOG_TRACE,
  EASY_LOG_ALL
} easy_log_level_t;

#define EASY_STRINGIZE_(x) #x
#define EASY_STRINGIZE(x) EASY_STRINGIZE_(x)

#define easy_common_log(level, format, args...)                                           \
  do {                                                                                    \
    if (easy_log_level >= level) {                                                        \
      static uint64_t hash_val = 0;                                                       \
      if (0 == hash_val) {                                                                \
        hash_val = easy_fnv_hash(__FILE__ ":" EASY_STRINGIZE(__LINE__));                  \
      }                                                                                   \
      easy_log_format(level, __FILE__, __LINE__, __FUNCTION__, hash_val, format, ##args); \
    }                                                                                     \
  } while (0)
#define easy_fatal_log(format, args...) easy_common_log(EASY_LOG_FATAL, format, ##args)
#define easy_error_log(format, args...) easy_common_log(EASY_LOG_ERROR, format, ##args)
#define easy_warn_log(format, args...) easy_common_log(EASY_LOG_WARN, format, ##args)
#define easy_info_log(format, args...) easy_common_log(EASY_LOG_INFO, format, ##args)
#define easy_debug_log(format, args...) easy_common_log(EASY_LOG_DEBUG, format, ##args)
#define easy_trace_log(format, args...) easy_common_log(EASY_LOG_TRACE, format, ##args)
#define SYS_ERROR(format...) easy_error_log(format)

#define EASY_PRINT_BT(format, args...)                                                                    \
  {                                                                                                       \
    char _buffer_stack_[256];                                                                             \
    {                                                                                                     \
      void* array[10];                                                                                    \
      int i, idx = 0, n = backtrace(array, 10);                                                           \
      for (i = 0; i < n; i++)                                                                             \
        idx += lnprintf(idx + _buffer_stack_, 25, "%p ", array[i]);                                       \
    }                                                                                                     \
    easy_log_format(EASY_LOG_OFF, __FILE__, __LINE__, __FUNCTION__, "%s" format, _buffer_stack_, ##args); \
  }

extern easy_log_level_t easy_log_level;
extern easy_log_format_pt easy_log_format;
extern void easy_log_set_print(easy_log_print_pt p);
extern void easy_log_set_format(easy_log_format_pt p);
extern void easy_log_format_default(
    int level, const char* file, int line, const char* function, uint64_t location_hash_val, const char* fmt, ...);
extern void easy_log_print_default(const char* message);

int64_t get_us();

uint64_t easy_fnv_hash(const char* str);

extern int64_t ev_loop_warn_threshold;
extern __thread int64_t ev_malloc_count;
extern __thread int64_t ev_malloc_time;
extern __thread int64_t ev_write_count;
extern __thread int64_t ev_write_time;
extern __thread int64_t ev_read_count;
extern __thread int64_t ev_read_time;
extern __thread int64_t ev_client_cb_count;
extern __thread int64_t ev_client_cb_time;
extern __thread int64_t ev_server_process_count;
extern __thread int64_t ev_server_process_time;
extern __thread void* ev_watch_pending_addr;
extern __thread int ev_watch_pending;

#ifndef __clang__

#define EASY_STAT_TIME_GUARD(stat, format, ...)                                     \
  void __tg_cleanup(void* s)                                                        \
  {                                                                                 \
    int64_t cost = get_us() - *(int64_t*)s;                                         \
    stat;                                                                           \
    if (cost > ev_loop_warn_threshold) {                                            \
      easy_warn_log("easy cost too much time: %ldus " format, cost, ##__VA_ARGS__); \
    }                                                                               \
  }                                                                                 \
  int64_t _tg_s __attribute__((cleanup(__tg_cleanup))) = get_us();

#define EASY_SOCKET_IO_TIME_GUARD(stat, format, ...)                                   \
  void __tg_cleanup(void* s)                                                           \
  {                                                                                    \
    int ret;                                                                           \
    int64_t cost = get_us() - *(int64_t*)s;                                            \
    double loadavg[3];                                                                 \
    stat;                                                                              \
    if (cost > ev_loop_warn_threshold) {                                               \
      ret = getloadavg(loadavg, 3);                                                    \
      if (ret == 3) {                                                                  \
        easy_warn_log("easy cost too much time(%ldus). loadavg(%lf, %lf, %lf)" format, \
            cost,                                                                      \
            loadavg[0],                                                                \
            loadavg[1],                                                                \
            loadavg[2],                                                                \
            ##__VA_ARGS__);                                                            \
      } else {                                                                         \
        easy_warn_log("easy cost too much time(%ldus) " format, cost, ##__VA_ARGS__);  \
      }                                                                                \
    }                                                                                  \
  }                                                                                    \
  int64_t _tg_s __attribute__((cleanup(__tg_cleanup))) = get_us();

#else

#define EASY_STAT_TIME_GUARD(...)
#define EASY_SOCKET_IO_TIME_GUARD(...)

#endif

#define EASY_TIME_GUARD(format, ...) EASY_STAT_TIME_GUARD((void)0, format, ##__VA_ARGS__)
EASY_CPP_END

#endif
