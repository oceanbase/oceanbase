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

#ifndef LIBCOS_LOG_H
#define LIBCOS_LOG_H

#include "cos_sys_define.h"


COS_CPP_START

typedef void (*cos_log_print_pt)(const char *message, int len);

typedef void (*cos_log_format_pt)(int level,
                                  const char *file,
                                  int line,
                                  const char *function,
                                  const char *fmt, ...)
        __attribute__ ((__format__ (__printf__, 5, 6)));

void cos_log_set_print(cos_log_print_pt p);
void cos_log_set_format(cos_log_format_pt p);

typedef enum {
    COS_LOG_OFF = 1,
    COS_LOG_FATAL,
    COS_LOG_ERROR,
    COS_LOG_WARN,
    COS_LOG_INFO,
    COS_LOG_DEBUG,
    COS_LOG_TRACE,
    COS_LOG_ALL
} cos_log_level_e;

#ifdef WIN32
#define cos_fatal_log(format, ...) if(cos_log_level>=COS_LOG_FATAL) \
        cos_log_format(COS_LOG_FATAL, __FILE__, __LINE__, __FUNCTION__, format, ##__VA_ARGS__)
#define cos_error_log(format, ...) if(cos_log_level>=COS_LOG_ERROR) \
        cos_log_format(COS_LOG_ERROR, __FILE__, __LINE__, __FUNCTION__, format, ##__VA_ARGS__)
#define cos_warn_log(format, ...) if(cos_log_level>=COS_LOG_WARN)   \
        cos_log_format(COS_LOG_WARN, __FILE__, __LINE__, __FUNCTION__, format, ##__VA_ARGS__)
#define cos_info_log(format, ...) if(cos_log_level>=COS_LOG_INFO)   \
        cos_log_format(COS_LOG_INFO, __FILE__, __LINE__, __FUNCTION__, format, ##__VA_ARGS__)
#define cos_debug_log(format, ...) if(cos_log_level>=COS_LOG_DEBUG) \
        cos_log_format(COS_LOG_DEBUG, __FILE__, __LINE__, __FUNCTION__, format, ##__VA_ARGS__)
#define cos_trace_log(format, ...) if(cos_log_level>=COS_LOG_TRACE) \
        cos_log_format(COS_LOG_TRACE, __FILE__, __LINE__, __FUNCTION__, format, ##__VA_ARGS__)
#else
#define cos_fatal_log(format, args...) if(cos_log_level>=COS_LOG_FATAL) \
        cos_log_format(COS_LOG_FATAL, __FILE__, __LINE__, __FUNCTION__, format, ## args)
#define cos_error_log(format, args...) if(cos_log_level>=COS_LOG_ERROR) \
        cos_log_format(COS_LOG_ERROR, __FILE__, __LINE__, __FUNCTION__, format, ## args)
#define cos_warn_log(format, args...) if(cos_log_level>=COS_LOG_WARN)   \
        cos_log_format(COS_LOG_WARN, __FILE__, __LINE__, __FUNCTION__, format, ## args)
#define cos_info_log(format, args...) if(cos_log_level>=COS_LOG_INFO)   \
        cos_log_format(COS_LOG_INFO, __FILE__, __LINE__, __FUNCTION__, format, ## args)
#define cos_debug_log(format, args...) if(cos_log_level>=COS_LOG_DEBUG) \
        cos_log_format(COS_LOG_DEBUG, __FILE__, __LINE__, __FUNCTION__, format, ## args)
#define cos_trace_log(format, args...) if(cos_log_level>=COS_LOG_TRACE) \
        cos_log_format(COS_LOG_TRACE, __FILE__, __LINE__, __FUNCTION__, format, ## args)
#endif

void cos_log_set_level(cos_log_level_e level);

void cos_log_set_output(apr_file_t *output);

void cos_log_print_default(const char *message, int len);

void cos_log_format_default(int level,
                            const char *file,
                            int line,
                            const char *function,
                            const char *fmt, ...)
        __attribute__ ((__format__ (__printf__, 5, 6)));

extern cos_log_level_e cos_log_level;
extern cos_log_format_pt cos_log_format;
extern cos_log_format_pt cos_log_format;

COS_CPP_END

#endif
