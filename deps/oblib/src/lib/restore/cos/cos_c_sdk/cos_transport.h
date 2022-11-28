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

#ifndef LIBCOS_TRANSPORT_H
#define LIBCOS_TRANSPORT_H

#include "cos_sys_define.h"
#include "cos_buf.h"


COS_CPP_START

typedef struct cos_http_request_s cos_http_request_t;
typedef struct cos_http_response_s cos_http_response_t;
typedef struct cos_http_transport_s cos_http_transport_t;
typedef struct cos_http_controller_s cos_http_controller_t;

typedef struct cos_http_request_options_s cos_http_request_options_t;
typedef struct cos_http_transport_options_s cos_http_transport_options_t;
typedef struct cos_curl_http_transport_s cos_curl_http_transport_t;

typedef int (*cos_read_http_body_pt)(cos_http_request_t *req, char *buffer, int len);
typedef int (*cos_write_http_body_pt)(cos_http_response_t *resp, const char *buffer, int len);

typedef void (*cos_progress_callback)(int64_t consumed_bytes, int64_t total_bytes);

void cos_curl_response_headers_parse(cos_pool_t *p, cos_table_t *headers, char *buffer, int len);
cos_http_transport_t *cos_curl_http_transport_create(cos_pool_t *p);
int cos_curl_http_transport_perform(cos_http_transport_t *t);

struct cos_http_request_options_s {
    int speed_limit;
    int speed_time;
    int dns_cache_timeout;
    int connect_timeout;
    int64_t max_memory_size;
    int enable_crc;
    int enable_md5;
    char *proxy_host;
    char *proxy_auth;
    char *host_ip;
    int host_port;
};

struct cos_http_transport_options_s {
    char *user_agent;
    char *cacerts_path;
    uint32_t ssl_verification_disabled:1;
};

#define COS_HTTP_BASE_CONTROLLER_DEFINE         \
    cos_http_request_options_t *options;        \
    cos_pool_t *pool;                           \
    int64_t start_time;                         \
    int64_t first_byte_time;                    \
    int64_t finish_time;                        \
    uint32_t owner:1;                           \
    void *user_data;

struct cos_http_controller_s {
    COS_HTTP_BASE_CONTROLLER_DEFINE
};

typedef struct cos_http_controller_ex_s {
    COS_HTTP_BASE_CONTROLLER_DEFINE
    // private
    int error_code;
    char *reason; // can't modify
} cos_http_controller_ex_t;

typedef enum {
    BODY_IN_MEMORY = 0,
    BODY_IN_FILE,
    BODY_IN_CALLBACK
} cos_http_body_type_e;

struct cos_http_request_s {
    char *host;
    char *proto;
    char *signed_url;

    http_method_e method;
    char *uri;
    char *resource;
    cos_table_t *headers;
    cos_table_t *query_params;

    cos_list_t body;
    int64_t body_len;
    char *file_path;
    cos_file_buf_t *file_buf;

    cos_pool_t *pool;
    void *user_data;
    cos_read_http_body_pt read_body;

    cos_http_body_type_e type;

    cos_progress_callback progress_callback;
    uint64_t crc64;
    int64_t  consumed_bytes;
};

struct cos_http_response_s {
    int status;
    cos_table_t *headers;

    cos_list_t body;
    int64_t body_len;
    char *file_path;
    cos_file_buf_t* file_buf;
    int64_t content_length;

    cos_pool_t *pool;
    void *user_data;
    cos_write_http_body_pt write_body;

    cos_http_body_type_e type;

    cos_progress_callback progress_callback;
    uint64_t crc64;
};

typedef enum {
    TRANS_STATE_INIT,
    TRANS_STATE_HEADER,
    TRANS_STATE_BODY_IN,
    TRANS_STATE_BODY_OUT,
    TRANS_STATE_ABORT,
    TRANS_STATE_DONE
} cos_transport_state_e;

#define COS_HTTP_BASE_TRANSPORT_DEFINE           \
    cos_http_request_t *req;                     \
    cos_http_response_t *resp;                   \
    cos_pool_t *pool;                            \
    cos_transport_state_e state;                 \
    cos_array_header_t *cleanup;                 \
    cos_http_transport_options_t *options;       \
    cos_http_controller_ex_t *controller;

struct cos_http_transport_s {
    COS_HTTP_BASE_TRANSPORT_DEFINE
};

struct cos_curl_http_transport_s {
    COS_HTTP_BASE_TRANSPORT_DEFINE
    CURL *curl;
    char *url;
    struct curl_slist *headers;
    curl_read_callback header_callback;
    curl_read_callback read_callback;
    curl_write_callback write_callback;
};

COS_CPP_END

#endif
