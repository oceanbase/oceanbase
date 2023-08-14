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

#ifndef ENABLE_SANITY
#else
#include "lib/alloc/memory_sanity.h"
#include "lib/utility/utility.h"

__thread bool enable_sanity_check = true;
struct t_vip {
public:
  typedef char t_func[128];
  t_func func_;
  int64_t min_addr_;
  int64_t max_addr_;
};
static char whitelist[1024]; // store origin str
static t_vip vips[8];

void sanity_set_whitelist(const char *str)
{
  if (0 == strncmp(str, whitelist, sizeof(whitelist))) {
  } else {
    strncpy(whitelist, str, sizeof(whitelist));
    memset(vips, 0, sizeof(vips));
    decltype(whitelist) whitelist_cpy;
    memcpy(whitelist_cpy, whitelist, sizeof(whitelist));
    char *p = whitelist_cpy;
    char *saveptr = NULL;
    int i = 0;
    while ((p = strtok_r(p, ";", &saveptr)) != NULL && i < 8) {
      t_vip *vip = &vips[i++];
      strncpy(vip->func_, p, sizeof(vip->func_));
      vip->min_addr_ = vip->max_addr_ = 0;
      p = saveptr;
    }
  }
}

BacktraceSymbolizeFunc backtrace_symbolize_func = NULL;

void memory_sanity_abort()
{
  if ('\0' == whitelist[0]) {
    abort();
  }
  void *addrs[128];
  int n_addr = ob_backtrace(addrs, sizeof(addrs)/sizeof(addrs[0]));
  addrs_to_offsets(addrs, n_addr);
  void *vip_addr = NULL;
  for (int i = 0; NULL == vip_addr && i < n_addr; i++) {
    for (int j = 0; NULL == vip_addr && j < sizeof(vips)/sizeof(vips[0]); j++) {
      t_vip *vip = &vips[j];
      if ('\0' == vip->func_[0]) {
        break;
      } else if (0 == vip->min_addr_ || 0 == vip->max_addr_) {
        continue;
      } else if ((int64_t)addrs[i] >= vip->min_addr_ && (int64_t)addrs[i] <= vip->max_addr_) {
        vip_addr = addrs[i];
        break;
      }
    }
  }
  if (vip_addr != NULL) {
    if (REACH_TIME_INTERVAL(1000 * 1000)) {
      fprintf(stderr, "[ERROR] sanity check failed, vip_addr: %p, lbt: %s\n",
              vip_addr, oceanbase::common::parray((int64_t*)addrs, n_addr));
    }
  } else {
    char buf[8192];
    int64_t buf_len = sizeof(buf);
    int32_t pos = 0;
    t_vip::t_func vip_func = {'\0'};
    auto check_vip = [&](void *addr, const char *func_name, const char *file_name, uint32_t line)
    {
      int real_len = snprintf(buf + pos, buf_len - pos, "    %-14p %s %s:%u\n", addr, func_name, file_name, line);
      if (real_len < buf_len - pos) {
        pos += real_len;
      }
      for (int i = 0; i < sizeof(vips)/sizeof(vips[0]); i++) {
        t_vip *vip = &vips[i];
        if ('\0' == vip->func_[0]) {
          break;
        } else if (strstr(func_name, vip->func_) != NULL) {
          strncpy(vip_func, func_name, sizeof(vip_func));
          if ((int64_t)addr < vip->min_addr_ || 0 == vip->min_addr_) {
            vip->min_addr_ = (int64_t)addr;
          }
          if ((int64_t)addr > vip->max_addr_) {
            vip->max_addr_ = (int64_t)addr;
          }
          break;
        }
      }
    };
    if (backtrace_symbolize_func != NULL) {
      backtrace_symbolize_func(addrs, n_addr, check_vip);
    }
    while (pos > 0 && '\n' == buf[pos - 1]) pos--;
    fprintf(stderr, "[ERROR] sanity check failed, vip_func: %s, lbt: %s\nsymbolize:\n%.*s\n", vip_func,
            oceanbase::common::parray((int64_t*)addrs, n_addr), pos, buf);
    if ('\0' == vip_func[0]) {
      abort();
    }
  }
}

template <class Tp>
inline void DoNotOptimize(Tp const& value)
{
  asm volatile("" : : "g"(value) : "memory");
}

EXTERN_C_BEGIN

void *memset(void *s, int c, size_t n)
{
  static void *(*real_func)(void *, int, size_t)
    = (__typeof__(real_func)) dlsym(RTLD_NEXT, "memset");
  sanity_check_range(s, n);
  return real_func(s, c, n);
}

void bzero(void *s, size_t n)
{
  static void (*real_func)(void *, size_t)
    = (__typeof__(real_func)) dlsym(RTLD_NEXT, "bzero");
  sanity_check_range(s, n);
  return real_func(s, n);
}

void *memcpy(void *dest, const void *src, size_t n)
{
  static void *(*real_func)(void *, const void *, size_t)
    = (__typeof__(real_func)) dlsym(RTLD_NEXT, "memcpy");
  sanity_check_range(dest, n);
  sanity_check_range(src, n);
  return real_func(dest, src, n);
}

void *memmove(void *dest, const void *src, size_t n)
{
  static void *(*real_func)(void *, const void *, size_t)
    = (__typeof__(real_func)) dlsym(RTLD_NEXT, "memmove");
  sanity_check_range(dest, n);
  sanity_check_range(src, n);
  return real_func(dest, src, n);
}

int memcmp(const void *s1, const void *s2, size_t n)
{
  static int (*real_func)(const void *, const void *, size_t)
    = (__typeof__(real_func)) dlsym(RTLD_NEXT, "memcmp");
  sanity_check_range(s1, n);
  sanity_check_range(s2, n);
  return real_func(s1, s2, n);
}

size_t strlen(const char *s)
{
  static size_t (*real_func)(const char *)
    = (__typeof__(real_func)) dlsym(RTLD_NEXT, "strlen");
  size_t len = real_func(s);
  sanity_check_range(s, len + 1); // include the terminating null byte
  return len;
}

size_t strnlen(const char *s, size_t maxlen)
{
  static size_t (*real_func)(const char *, size_t)
    = (__typeof__(real_func)) dlsym(RTLD_NEXT, "strnlen");
  size_t len = real_func(s, maxlen);
  sanity_check_range(s, len);
  return len;
}

char *strcpy(char *dest, const char *src)
{
  static char *(*real_func)(char *, const char *)
    = (__typeof__(real_func)) dlsym(RTLD_NEXT, "strcpy");
  sanity_check_range(dest, strlen(src) + 1); // invoke strlen directly, utilize checker within strlen
  return real_func(dest, src);
}

char *strncpy(char *dest, const char *src, size_t n)
{
  static char *(*real_func)(char *, const char *, size_t)
    = (__typeof__(real_func)) dlsym(RTLD_NEXT, "strncpy");
  sanity_check_range(dest, strnlen(src, n));
  return real_func(dest, src, n);
}

int strcmp(const char *s1, const char *s2)
{
  static int (*real_func)(const char *, const char *)
    = (__typeof__(real_func)) dlsym(RTLD_NEXT, "strcmp");
  DoNotOptimize(strlen(s1));
  DoNotOptimize(strlen(s2));
  return real_func(s1, s2);
}

int strncmp(const char *s1, const char *s2, size_t n)
{
  static int (*real_func)(const char *, const char *, size_t)
    = (__typeof__(real_func)) dlsym(RTLD_NEXT, "strncmp");
  DoNotOptimize(strnlen(s1, n));
  DoNotOptimize(strnlen(s2, n));
  return real_func(s1, s2, n);
}

int strcasecmp(const char *s1, const char *s2)
{
  static int (*real_func)(const char *, const char *)
    = (__typeof__(real_func)) dlsym(RTLD_NEXT, "strcasecmp");
  DoNotOptimize(strlen(s1));
  DoNotOptimize(strlen(s2));
  return real_func(s1, s2);
}

int strncasecmp(const char *s1, const char *s2, size_t n)
{
  static int (*real_func)(const char *, const char *, size_t)
    = (__typeof__(real_func)) dlsym(RTLD_NEXT, "strncasecmp");
  DoNotOptimize(strnlen(s1, n));
  DoNotOptimize(strnlen(s2, n));
  return real_func(s1, s2, n);
}

int vsprintf(char *str, const char *format, va_list ap)
{
  static int (*real_func)(char *, const char *, va_list)
    = (__typeof__(real_func)) dlsym(RTLD_NEXT, "vsprintf");
  int n = real_func(str, format, ap);
  sanity_check_range(str, n + 1);
  return n;
}

int vsnprintf(char *str, size_t size, const char *format, va_list ap)
{
  static int (*real_func)(char *, size_t, const char *, va_list)
    = (__typeof__(real_func)) dlsym(RTLD_NEXT, "vsnprintf");
  int n = real_func(str, size, format, ap);
  sanity_check_range(str, OB_LIKELY((n + 1) < size) ? (n + 1) : size);
  return n;
}

int sprintf(char *str, const char *format, ...)
{
  va_list ap;
  va_start(ap, format);
  int n = vsprintf(str, format, ap);
  va_end(ap);
  return n;
}

int snprintf(char *str, size_t size, const char *format, ...)
{
  va_list ap;
  va_start(ap, format);
  int n = vsnprintf(str, size, format, ap);
  va_end(ap);
  return n;
}

EXTERN_C_END
#endif
