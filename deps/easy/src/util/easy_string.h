#ifndef EASY_STRING_H_
#define EASY_STRING_H_

/**
 * inet的通用函数
 */
#include <stdarg.h>
#include "easy_define.h"
#include "util/easy_pool.h"

EASY_CPP_START

extern char *easy_strncpy(char *dst, const char *src, size_t n);
extern char *easy_string_tohex(const char *str, int n, char *result, int size);
extern char *easy_string_toupper(char *str);
extern char *easy_string_tolower(char *str);
extern char *easy_string_format_size(double byte, char *buffer, int size);
extern char *easy_strcpy(char *dest, const char *src);
extern char *easy_num_to_str(char *dest, int len, uint64_t number);
extern char *easy_string_capitalize(char *str, int len);
extern int easy_vsnprintf(char *buf, size_t size, const char *fmt, va_list args);
extern int lnprintf(char *str, size_t size, const char *fmt, ...) __attribute__ ((__format__ (__printf__, 3, 4)));

EASY_CPP_END

#endif
