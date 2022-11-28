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

#ifndef LIBCOS_SYS_DEFINE_H
#define LIBCOS_SYS_DEFINE_H

#include <stdint.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <ctype.h>
#include <time.h>

#include <apr_time.h>
#include <apr_strings.h>
#include <apr_pools.h>
#include <apr_tables.h>
#include <apr_file_io.h>

#include <curl/curl.h>

#ifdef __cplusplus
# define COS_CPP_START extern "C" {
# define COS_CPP_END }
#else
# define COS_CPP_START
# define COS_CPP_END
#endif

typedef enum {
    HTTP_GET,
    HTTP_HEAD,
    HTTP_PUT,
    HTTP_POST,
    HTTP_DELETE
} http_method_e;

typedef enum {
    COSE_OK = 0,
    COSE_OUT_MEMORY = -1000,
    COSE_OVER_MEMORY = -999,
    COSE_FAILED_CONNECT = -998,
    COSE_ABORT_CALLBACK = -997,
    COSE_INTERNAL_ERROR = -996,
    COSE_REQUEST_TIMEOUT = -995,
    COSE_INVALID_ARGUMENT = -994,
    COSE_INVALID_OPERATION = -993,
    COSE_CONNECTION_FAILED = -992,
    COSE_FAILED_INITIALIZE = -991,
    COSE_NAME_LOOKUP_ERROR = -990,
    COSE_FAILED_VERIFICATION = -989,
    COSE_WRITE_BODY_ERROR = -988,
    COSE_READ_BODY_ERROR = -987,
    COSE_SERVICE_ERROR = -986,
    COSE_OPEN_FILE_ERROR = -985,
    COSE_FILE_SEEK_ERROR = -984,
    COSE_FILE_INFO_ERROR = -983,
    COSE_FILE_READ_ERROR = -982,
    COSE_FILE_WRITE_ERROR = -981,
    COSE_XML_PARSE_ERROR = -980,
    COSE_UTF8_ENCODE_ERROR = -979,
    COSE_CRC_INCONSISTENT_ERROR = -978,
    COSE_FILE_FLUSH_ERROR = -977,
    COSE_FILE_TRUNC_ERROR = -976,
    COSE_UNKNOWN_ERROR = -100
} cos_error_code_e;

typedef apr_pool_t cos_pool_t;
typedef apr_table_t cos_table_t;
typedef apr_table_entry_t cos_table_entry_t;
typedef apr_array_header_t cos_array_header_t;

#define cos_table_elts(t) apr_table_elts(t)
#define cos_is_empty_table(t) apr_is_empty_table(t)
#define cos_table_make(p, n) apr_table_make(p, n)
#define cos_table_add_int(t, key, value) do {       \
        char value_str[64];                             \
        apr_snprintf(value_str, sizeof(value_str), "%d", value);\
        apr_table_add(t, key, value_str);               \
    } while(0)

#define cos_table_add_int64(t, key, value) do {       \
        char value_str[64];                             \
        apr_snprintf(value_str, sizeof(value_str), "%" APR_INT64_T_FMT, value);\
        apr_table_add(t, key, value_str);               \
    } while(0)

#define cos_table_set_int64(t, key, value) do {       \
        char value_str[64];                             \
        apr_snprintf(value_str, sizeof(value_str), "%" APR_INT64_T_FMT, value);\
        apr_table_set(t, key, value_str);               \
    } while(0)

#define cos_pool_create(n, p) apr_pool_create(n, p)
#define cos_pool_destroy(p) apr_pool_destroy(p)
#define cos_palloc(p, s) apr_palloc(p, s)
#define cos_pcalloc(p, s) apr_pcalloc(p, s)

#define COS_RETRY_TIME 2

#define COS_INIT_WINSOCK 1
#define COS_MD5_STRING_LEN 32
#define COS_MAX_URI_LEN 2048
#define COS_MAX_HEADER_LEN 8192
#define COS_MAX_QUERY_ARG_LEN 1024
#define COS_MAX_GMT_TIME_LEN 128

#define COS_MAX_XML_NODE_VALUE_LEN 1024
#define COS_MAX_INT64_STRING_LEN 64

#define COS_CONNECT_TIMEOUT 10
#define COS_DNS_CACHE_TIMOUT 60
#define COS_MIN_SPEED_LIMIT 8
#define COS_MIN_SPEED_TIME 120
#define COS_MAX_MEMORY_SIZE 1024*1024*1024L
#define COS_MAX_PART_SIZE 512*1024*1024L
#define COS_DEFAULT_PART_SIZE 1024*1024L

#define COS_REQUEST_STACK_SIZE 32

#define cos_abs(value)       (((value) >= 0) ? (value) : - (value))
#define cos_max(val1, val2)  (((val1) < (val2)) ? (val2) : (val1))
#define cos_min(val1, val2)  (((val1) > (val2)) ? (val2) : (val1))

#define LF     (char) 10
#define CR     (char) 13
#define CRLF   "\x0d\x0a"

#define COS_VERSION    "5.0.8"
#define COS_VER        "cos-sdk-c/" COS_VERSION

#define COS_HTTP_PREFIX   "http://"
#define COS_HTTPS_PREFIX  "https://"
#define COS_RTMP_PREFIX   "rtmp://"

#define COS_TEMP_FILE_SUFFIX  ".tmp"

#define COS_FALSE     0
#define COS_TRUE      1

#endif
