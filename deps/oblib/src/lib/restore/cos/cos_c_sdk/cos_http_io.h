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

#ifndef LIBCOS_HTTP_IO_H
#define LIBCOS_HTTP_IO_H

#include "cos_transport.h"
#include "cos_define.h"


COS_CPP_START

cos_http_controller_t *cos_http_controller_create(cos_pool_t *p, int owner);

/* http io error message*/
static APR_INLINE const char *cos_http_controller_get_reason(cos_http_controller_t *ctl)
{
    cos_http_controller_ex_t *ctle = (cos_http_controller_ex_t *)ctl;
    return ctle->reason;
}

CURL *cos_request_get();
void request_release(CURL *request);

int cos_http_io_initialize(const char *user_agent_info, int flag);
void cos_http_io_deinitialize();

int cos_http_send_request(cos_http_controller_t *ctl, cos_http_request_t *req, cos_http_response_t *resp);

void cos_set_default_request_options(cos_http_request_options_t *op);
void cos_set_default_transport_options(cos_http_transport_options_t *op);

cos_http_request_options_t *cos_http_request_options_create(cos_pool_t *p);

cos_http_request_t *cos_http_request_create(cos_pool_t *p);
cos_http_response_t *cos_http_response_create(cos_pool_t *p);

int cos_read_http_body_memory(cos_http_request_t *req, char *buffer, int len);
int cos_write_http_body_memory(cos_http_response_t *resp, const char *buffer, int len);

int cos_read_http_body_file(cos_http_request_t *req, char *buffer, int len);
int cos_write_http_body_file(cos_http_response_t *resp, const char *buffer, int len);
int cos_write_http_body_file_part(cos_http_response_t *resp, const char *buffer, int len);


typedef cos_http_transport_t *(*cos_http_transport_create_pt)(cos_pool_t *p);
typedef int (*cos_http_transport_perform_pt)(cos_http_transport_t *t);

extern cos_pool_t *cos_global_pool;
extern apr_file_t *cos_stderr_file;

extern cos_http_request_options_t *cos_default_http_request_options;
extern cos_http_transport_options_t *cos_default_http_transport_options;

extern cos_http_transport_create_pt cos_http_transport_create;
extern cos_http_transport_perform_pt cos_http_transport_perform;

COS_CPP_END

#endif
