#include "io/easy_log.h"
#include "util/easy_time.h"
#include <pthread.h>
#include <unistd.h>
#include <sys/syscall.h>
#include "io/easy_io.h"

#define gettid() syscall(__NR_gettid)

easy_log_print_pt       easy_log_print = easy_log_print_default;
easy_log_format_pt      easy_log_format = easy_log_format_default;
easy_log_level_t        easy_log_level = EASY_LOG_INFO;

__attribute__((constructor)) void init_easy_warn_threshold()
{
    ev_loop_warn_threshold = atoll(getenv("easy_warn_threshold") ? : "10000");
}

/**
 * 设置log的打印函数
 */
void  easy_log_set_print(easy_log_print_pt p)
{
    easy_log_print = p;
    ev_set_syserr_cb(easy_log_print);
}

int64_t get_us()
{
    return fast_current_time();
}

/**
 * 计算log位置的hash函数
 */
uint64_t easy_fnv_hash(const char *str)
{
  uint32_t offset_basis = 0x811C9DC5;
  uint32_t prime = 0x01000193;
  uint32_t fnv1 = offset_basis;
  uint32_t fnv1a = offset_basis;
  int i = strlen(str);
  while (i-- && str[i] != '/') {
    fnv1 *= prime;
    fnv1 ^= str[i];
    fnv1a ^= str[i];
    fnv1a *= prime;
  }
  return (uint64_t)fnv1 << 32 | fnv1a;
}

/**
 * 设置log的格式函数
 */
void  easy_log_set_format(easy_log_format_pt p)
{
    easy_log_format = p;
}

/**
 * 加上日志
 */
void easy_log_format_default(int level, const char *file, int line, const char *function, uint64_t location_hash_val,
                             const char *fmt, ...)
{
    ((void)(location_hash_val));
    static __thread ev_tstamp   oldtime = 0.0;
    static __thread char        time_str[32];
    ev_tstamp                   now;
    int                         len;
    char                        buffer[4096];

    // 从loop中取
    if (easy_baseth_self && easy_baseth_self->loop) {
        now = ev_now(easy_baseth_self->loop);
    } else {
        now = time(NULL);
    }

    if (oldtime != now) {
        time_t                  t;
        struct tm               tm;
        oldtime = now;
        t = (time_t) now;
        easy_localtime((const time_t *)&t, &tm);
        lnprintf(time_str, 32, "[%04d-%02d-%02d %02d:%02d:%02d.%03d]",
                 tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
                 tm.tm_hour, tm.tm_min, tm.tm_sec, (int)((now - t) * 1000));
    }

    // print
    len = lnprintf(buffer, 128, "%s %s:%d(tid:%lx)[%ld] ", time_str, file, line, pthread_self(), gettid());
    va_list                 args;
    va_start(args, fmt);
    len += easy_vsnprintf(buffer + len, 4090 - len,  fmt, args);
    va_end(args);

    // 去掉最后'\n'
    while (buffer[len - 1] == '\n') {
        len --;
    }

    buffer[len++] = '\n';
    buffer[len] = '\0';

    easy_log_print(buffer);
}

/**
 * 打印出来
 */
void easy_log_print_default(const char *message)
{
    easy_ignore(write(2, message, strlen(message)));
}

void __attribute__((constructor)) easy_log_start_()
{
    char                    *p = getenv("easy_log_level");

    if (p) easy_log_level = (easy_log_level_t)atoi(p);
}

void __tg_cleanup(void *s)
{
  int64_t cost = get_us() - ((struct easy_time_guard_t *)s)->start;
  if (cost > ev_loop_warn_threshold) {
    easy_warn_log("easy cost too much time: %ldus, procedure: %s", cost, ((struct easy_time_guard_t *)s)->procedure);
  }
}

void __tg_stat_cleanup(void *s)
{
  int64_t cost = get_us() - ((struct easy_stat_time_guard_t *)s)->start;
  int64_t *cnt = ((struct easy_stat_time_guard_t *)s)->cnt;
  *cnt += 1;
  int64_t *time = ((struct easy_stat_time_guard_t *)s)->time;
  *time += cost;
  if (cost > ev_loop_warn_threshold) {
    easy_warn_log("easy cost too much time: %ldus, procedure: %s", cost, ((struct easy_stat_time_guard_t *)s)->procedure);
  }
}

void __tg_stat_cleanup_s(void *s)
{
  int64_t cost = get_us() - ((struct easy_stat_time_guard_t *)s)->start;
  int64_t *cnt = ((struct easy_stat_time_guard_t *)s)->cnt;
  *cnt += 1;
  int64_t *time = ((struct easy_stat_time_guard_t *)s)->time;
  *time += cost;
  if (cost > ev_loop_warn_threshold) {
    easy_warn_log("easy cost too much time: %ldus, procedure: %s, size:%u", cost, ((struct easy_stat_time_guard_t *)s)->procedure,
                  *(((struct easy_stat_time_guard_t *)s)->size));
  }
}

void __tg_io_cleanup(void *s)
{
  int ret = 0;
  int64_t cost = get_us() - ((struct easy_stat_time_guard_t *)s)->start;
  double loadavg[3];
  int64_t *cnt = ((struct easy_stat_time_guard_t *)s)->cnt;
  *cnt += 1;
  int64_t *time = ((struct easy_stat_time_guard_t *)s)->time;
  *time += cost;
  if (cost > ev_loop_warn_threshold) {
    ret = getloadavg(loadavg, 3);
    if (ret == 3) {
      easy_warn_log("easy cost too much time(%ldus). loadavg(%lf, %lf, %lf), procedure: %s, size:%u",
                    cost,
                    loadavg[0],
                    loadavg[1],
                    loadavg[2],
                    ((struct easy_stat_time_guard_t *)s)->procedure,
                    *(((struct easy_stat_time_guard_t *)s)->size));
    } else {
      easy_warn_log("easy cost too much time(%ldus), procedure: %s, size:%u", cost, ((struct easy_stat_time_guard_t *)s)->procedure,
                    *(((struct easy_stat_time_guard_t *)s)->size));
    }
  }
}