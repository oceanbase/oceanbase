#ifndef EASY_LOG_H_
#define EASY_LOG_H_

/**
 * 简单的log输出
 */
#include "easy_define.h"
#include "util/easy_string.h"
#include "io/easy_baseth_pool.h"
#include "util/easy_time.h"

EASY_CPP_START

typedef void (*easy_log_print_pt)(const char *message);
typedef void (*easy_log_format_pt)(int level, const char *file, int line, const char *function,
                                   uint64_t location_hash_val, const char *fmt, ...);
typedef enum {
    EASY_LOG_OFF = -2,
    EASY_LOG_FATAL,
    EASY_LOG_ERROR,
    EASY_LOG_USER_ERROR, // align with observer
    EASY_LOG_WARN,
    EASY_LOG_INFO,
    EASY_LOG_DEBUG,
    EASY_LOG_TRACE,
    EASY_LOG_ALL
} easy_log_level_t;

#define EASY_STRINGIZE_(x) #x
#define EASY_STRINGIZE(x) EASY_STRINGIZE_(x)

#define easy_common_log(level, format, args...)                                              \
    do {                                                                                     \
      if (easy_log_level >= level) {                                                           \
        static uint64_t hash_val = 0;                                                        \
        if (0 == hash_val) {                                                                 \
          hash_val = easy_fnv_hash(__FILE__ ":" EASY_STRINGIZE(__LINE__));                   \
        }                                                                                    \
        easy_log_format(level, __FILE__, __LINE__, __FUNCTION__, hash_val, format, ## args); \
      }                                                                                      \
    } while (0)
#define easy_fatal_log(format, args...) easy_common_log(EASY_LOG_FATAL, format, ## args)
#define easy_error_log(format, args...) easy_common_log(EASY_LOG_ERROR, format, ## args)
#define easy_warn_log(format, args...) easy_common_log(EASY_LOG_WARN, format, ## args)
#define easy_info_log(format, args...) easy_common_log(EASY_LOG_INFO, format, ## args)
#define easy_debug_log(format, args...) easy_common_log(EASY_LOG_DEBUG, format, ## args)
#define easy_trace_log(format, args...) easy_common_log(EASY_LOG_TRACE, format, ## args)
#define SYS_ERROR(format...) easy_error_log(format)
// 打印backtrace
#define EASY_PRINT_BT(format, args...) \
  {easy_log_format(EASY_LOG_OFF, __FILE__, __LINE__, __FUNCTION__, "%s" format, easy_lbt(), ## args);}

extern easy_log_level_t easy_log_level;
extern easy_log_format_pt easy_log_format;
extern void easy_log_set_print(easy_log_print_pt p);
extern void easy_log_set_format(easy_log_format_pt p);
extern void easy_log_format_default(int level, const char *file, int line, const char *function,
                                    uint64_t location_hash_val, const char *fmt, ...);
extern void easy_log_print_default(const char *message);

int64_t get_us();

uint64_t easy_fnv_hash(const char *str);
void __tg_cleanup(void *s);
void __tg_stat_cleanup(void *s);
void __tg_io_cleanup(void *s);
void __tg_stat_cleanup_s(void *s);

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
extern __thread void    *ev_watch_pending_addr;
extern __thread int     ev_watch_pending;

struct easy_stat_time_guard_t {
  int64_t start;
  int64_t *cnt;
  int64_t *time;
  const char *procedure;
  uint32_t *size;
};

struct easy_time_guard_t {
  int64_t start;
  const char *procedure;
};

#define EASY_STAT_TIME_GUARD(_cnt, _time)                                                             \
  struct easy_stat_time_guard_t _tg_stat_time_guard __attribute__((cleanup(__tg_stat_cleanup))) = {   \
      .start = get_us(),                                                                              \
      .cnt = &(_cnt),                                                                                 \
      .time = &(_time),                                                                               \
      .procedure = __FUNCTION__,                                                                      \
  };

#define EASY_STAT_TIME_GUARD_WITH_SIZE(_cnt, _time, _size)                                              \
  struct easy_stat_time_guard_t _tg_stat_time_guard __attribute__((cleanup(__tg_stat_cleanup_s))) = {   \
      .start = get_us(),                                                                                \
      .cnt = &(_cnt),                                                                                   \
      .time = &(_time),                                                                                 \
      .procedure = __FUNCTION__,                                                                        \
      .size = (uint32_t *)&(_size),                                                                     \
  };

#define EASY_TIME_GUARD()                                                              \
  struct easy_time_guard_t _tg_time_guard __attribute__((cleanup(__tg_cleanup))) = {   \
      .start = get_us(),                                                               \
      .procedure = __FUNCTION__,                                                       \
  };

#define EASY_SOCKET_IO_TIME_GUARD(_cnt, _time, _size)                                              \
  struct easy_stat_time_guard_t _tg__io_time_guard __attribute__((cleanup(__tg_io_cleanup))) = {   \
      .start = get_us(),                                                                           \
      .cnt = &(_cnt),                                                                              \
      .time = &(_time),                                                                            \
      .procedure = __FUNCTION__,                                                                   \
      .size = (uint32_t *)&(_size),                                                                \
  };

EASY_CPP_END

#endif
