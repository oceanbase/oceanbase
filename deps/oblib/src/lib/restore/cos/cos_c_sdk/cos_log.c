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

#include "cos_log.h"
#include "apr_portable.h"

cos_log_print_pt  cos_log_print = cos_log_print_default;
cos_log_format_pt cos_log_format = cos_log_format_default;
cos_log_level_e   cos_log_level = COS_LOG_WARN;

extern apr_file_t *cos_stderr_file;


void cos_log_set_print(cos_log_print_pt p)
{
    cos_log_print = p;
}

void cos_log_set_format(cos_log_format_pt p)
{
    cos_log_format = p;
}

void cos_log_set_level(cos_log_level_e level)
{
    cos_log_level = level;
}

void cos_log_set_output(apr_file_t *output)
{
    cos_stderr_file = output;
}

void cos_log_print_default(const char *message, int len)
{
    if (cos_stderr_file == NULL) {
        fprintf(stderr, "%s", message);
    } else {
        apr_size_t bnytes = len;
        apr_file_write(cos_stderr_file, message, &bnytes);
    }
}

void cos_log_format_default(int level,
                            const char *file,
                            int line,
                            const char *function,
                            const char *fmt, ...)
{
    int len;
    apr_time_t t;
    int s;
    apr_time_exp_t tm;
    va_list args;
    char buffer[4096];

    t = apr_time_now();
    if ((s = apr_time_exp_lt(&tm, t)) != APR_SUCCESS) {
        return;
    }

    len = apr_snprintf(buffer, 4090, "[%04d-%02d-%02d %02d:%02d:%02d.%03d] %" APR_INT64_T_FMT " %s:%d ",
                   tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
                   tm.tm_hour, tm.tm_min, tm.tm_sec, tm.tm_usec/1000,
                   (int64_t)apr_os_thread_current(), file, line);

    va_start(args, fmt);
    len += vsnprintf(buffer + len, 4090 - len, fmt, args);
    va_end(args);

    while (buffer[len -1] == '\n') len--;
    buffer[len++] = '\n';
    buffer[len] = '\0';

    cos_log_print(buffer, len);
}
