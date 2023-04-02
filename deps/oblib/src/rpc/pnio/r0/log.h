#include <stdio.h>
#include <unistd.h>
#include <sys/syscall.h>

typedef void (*log_func_t)(int level, const char *file, int line, const char *function, const char *fmt, va_list ap);
extern void do_log(int level, const char* file, int line, const char* func, const char* format, ...) __attribute__((format(printf, 5, 6)));

extern log_func_t g_log_func;
extern int g_log_level;
enum { LOG_LEVEL_ERROR = 0, LOG_LEVEL_USER_LEVEL = 1, LOG_LEVEL_WARN = 2, LOG_LEVEL_INFO = 3, LOG_LEVEL_TRACE = 4, LOG_LEVEL_DEBUG = 5 };


extern __thread format_t g_log_fbuf;
#ifndef rk_log_macro
#define rk_log_macro(level, ret, ...) {  if (LOG_LEVEL_ ## level <= g_log_level)  do_log(LOG_LEVEL_ ## level, __FILE__, __LINE__, __func__, ##__VA_ARGS__); }
#endif
#define do_rk_log_macro(...) { format_reset(&g_log_fbuf); rk_log_macro(__VA_ARGS__); }
#define rk_error(...) do_rk_log_macro(ERROR, oceanbase::common::OB_ERR_SYS, ##__VA_ARGS__)
#define rk_info(...) do_rk_log_macro(INFO, oceanbase::common::OB_SUCCESS, ##__VA_ARGS__)
#define rk_warn(...) do_rk_log_macro(WARN, oceanbase::common::OB_SUCCESS, ##__VA_ARGS__)
#define rk_fatal(...) { rk_error(__VA_ARGS__); exit(1); }
#define T2S(type, obj) type ## _str(&g_log_fbuf, obj)
