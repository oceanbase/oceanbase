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

#ifndef LIBCOS_STATUS_H
#define LIBCOS_STATUS_H

#include "cos_sys_define.h"
#include "cos_list.h"

COS_CPP_START

typedef struct cos_status_s cos_status_t;

struct cos_status_s {
    int code; // > 0 http code
    char *error_code; // can't modify
    char *error_msg; // can't modify
    char *req_id;   // can't modify
};

static APR_INLINE int cos_status_is_ok(cos_status_t *s)
{
    return s->code > 0 && s->code / 100 == 2;
}

static APR_INLINE int cos_http_is_ok(int st)
{
    return st / 100 == 2;
}

#define cos_status_set(s, c, ec, es)                                    \
    (s)->code = c; (s)->error_code = (char *)ec; (s)->error_msg = (char *)es

/**
 * @brief determine whether the request should be retried
 * @param[in]   s             the return status of api, such as cos_put_object_from_buffer
 * @return      int           COS_FALSE indicates no retries, COS_TRUE retry
 */
int cos_should_retry(cos_status_t *s);

cos_status_t *cos_status_create(cos_pool_t *p);

cos_status_t *cos_status_dup(cos_pool_t *p, cos_status_t *src);

cos_status_t *cos_status_parse_from_body(cos_pool_t *p, cos_list_t *bc, int code, cos_status_t *s);

extern const char COS_XML_PARSE_ERROR_CODE[];
extern const char COS_OPEN_FILE_ERROR_CODE[];
extern const char COS_WRITE_FILE_ERROR_CODE[];
extern const char COS_HTTP_IO_ERROR_CODE[];
extern const char COS_UNKNOWN_ERROR_CODE[];
extern const char COS_CLIENT_ERROR_CODE[];
extern const char COS_UTF8_ENCODE_ERROR_CODE[];
extern const char COS_URL_ENCODE_ERROR_CODE[];
extern const char COS_INCONSISTENT_ERROR_CODE[];
extern const char COS_CREATE_QUEUE_ERROR_CODE[];
extern const char COS_CREATE_THREAD_POOL_ERROR_CODE[];
extern const char COS_LACK_OF_CONTENT_LEN_ERROR_CODE[];

COS_CPP_END

#endif
