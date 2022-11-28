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
#include "cos_http_io.h"
#include "cos_sys_define.h"
#include <apr_thread_mutex.h>
#include <apr_file_io.h>

cos_pool_t *cos_global_pool = NULL;
apr_file_t *cos_stderr_file = NULL;

cos_http_request_options_t *cos_default_http_request_options = NULL;
cos_http_transport_options_t *cos_default_http_transport_options = NULL;

cos_http_transport_create_pt cos_http_transport_create = cos_curl_http_transport_create;
cos_http_transport_perform_pt cos_http_transport_perform = cos_curl_http_transport_perform;

static apr_thread_mutex_t* requestStackMutexG = NULL;
apr_thread_mutex_t* downloadMutex = NULL;
static CURL *requestStackG[COS_REQUEST_STACK_SIZE];
static int requestStackCountG;
static char cos_user_agent[256];


static cos_http_transport_options_t *cos_http_transport_options_create(cos_pool_t *p);

CURL *cos_request_get()
{
    CURL *request = NULL;

    apr_thread_mutex_lock(requestStackMutexG);
    if (requestStackCountG > 0) {
        request = requestStackG[--requestStackCountG];
    }
    apr_thread_mutex_unlock(requestStackMutexG);

    // If we got one, deinitialize it for re-use
    if (request) {
        curl_easy_reset(request);
    }
    else {
        request = curl_easy_init();
    }

    return request;
}

void request_release(CURL *request)
{
    apr_thread_mutex_lock(requestStackMutexG);

    // If the request stack is full, destroy this one
    // else put this one at the front of the request stack; we do this because
    // we want the most-recently-used curl handle to be re-used on the next
    // request, to maximize our chances of re-using a TCP connection before it
    // times out
    if (requestStackCountG == COS_REQUEST_STACK_SIZE) {
        apr_thread_mutex_unlock(requestStackMutexG);
        curl_easy_cleanup(request);
    }
    else {
        requestStackG[requestStackCountG++] = request;
        apr_thread_mutex_unlock(requestStackMutexG);
    }
}

void cos_set_default_request_options(cos_http_request_options_t *op)
{
    cos_default_http_request_options = op;
}

void cos_set_default_transport_options(cos_http_transport_options_t *op)
{
    cos_default_http_transport_options = op;
}

cos_http_request_options_t *cos_http_request_options_create(cos_pool_t *p)
{
    cos_http_request_options_t *options;

    options = (cos_http_request_options_t *)cos_pcalloc(p, sizeof(cos_http_request_options_t));
    options->speed_limit = COS_MIN_SPEED_LIMIT;
    options->speed_time = COS_MIN_SPEED_TIME;
    options->connect_timeout = COS_CONNECT_TIMEOUT;
    options->dns_cache_timeout = COS_DNS_CACHE_TIMOUT;
    options->max_memory_size = COS_MAX_MEMORY_SIZE;
    options->enable_crc = COS_TRUE;
    options->enable_md5 = COS_TRUE;
    options->proxy_auth = NULL;
    options->proxy_host = NULL;

    return options;
}

cos_http_transport_options_t *cos_http_transport_options_create(cos_pool_t *p)
{
    return (cos_http_transport_options_t *)cos_pcalloc(p, sizeof(cos_http_transport_options_t));
}

cos_http_controller_t *cos_http_controller_create(cos_pool_t *p, int owner)
{
    int s;
    cos_http_controller_t *ctl;

    if(p == NULL) {
        if ((s = cos_pool_create(&p, NULL)) != APR_SUCCESS) {
            cos_fatal_log("cos_pool_create failure.");
            return NULL;
        }
    }

    ctl = (cos_http_controller_t *)cos_pcalloc(p, sizeof(cos_http_controller_ex_t));
    ctl->pool = p;
    ctl->owner = owner;
    ctl->options = cos_default_http_request_options;

    return ctl;
}

cos_http_request_t *cos_http_request_create(cos_pool_t *p)
{
    cos_http_request_t *req;

    req = (cos_http_request_t *)cos_pcalloc(p, sizeof(cos_http_request_t));
    req->method = HTTP_GET;
    req->headers = cos_table_make(p, 5);
    req->query_params = cos_table_make(p, 3);
    cos_list_init(&req->body);
    req->type = BODY_IN_MEMORY;
    req->body_len = 0;
    req->pool = p;
    req->read_body = cos_read_http_body_memory;

    return req;
}

cos_http_response_t *cos_http_response_create(cos_pool_t *p)
{
    cos_http_response_t *resp;

    resp = (cos_http_response_t *)cos_pcalloc(p, sizeof(cos_http_response_t));
    resp->status = -1;
    resp->headers = cos_table_make(p, 10);
    cos_list_init(&resp->body);
    resp->type = BODY_IN_MEMORY;
    resp->body_len = 0;
    resp->pool = p;
    resp->write_body = cos_write_http_body_memory;

    return resp;
}

int cos_read_http_body_memory(cos_http_request_t *req, char *buffer, int len)
{
    int wsize;
    int bytes = 0;
    cos_buf_t *b;
    cos_buf_t *n;

    cos_list_for_each_entry_safe(cos_buf_t, b, n, &req->body, node) {
        wsize = cos_buf_size(b);
        if (wsize == 0) {
            cos_list_del(&b->node);
            continue;
        }
        wsize = cos_min(len - bytes, wsize);
        if (wsize == 0) {
            break;
        }
        memcpy(buffer + bytes, b->pos, wsize);
        b->pos += wsize;
        bytes += wsize;
        if (b->pos == b->last) {
            cos_list_del(&b->node);
        }
    }

    return bytes;
}

int cos_read_http_body_file(cos_http_request_t *req, char *buffer, int len)
{
    int s;
    char buf[256];
    apr_size_t nbytes = len;
    apr_size_t bytes_left;

    if (req->file_buf == NULL || req->file_buf->file == NULL) {
        cos_error_log("request body arg invalid file_buf NULL.");
        return COSE_INVALID_ARGUMENT;
    }

    if (req->file_buf->file_pos >= req->file_buf->file_last) {
        cos_debug_log("file read finish.");
        return 0;
    }

    bytes_left = (apr_size_t)(req->file_buf->file_last - req->file_buf->file_pos);
    if (nbytes > bytes_left) {
        nbytes = bytes_left;
    }

    if ((s = apr_file_read(req->file_buf->file, buffer, &nbytes)) != APR_SUCCESS) {
        cos_error_log("apr_file_read filure, code:%d %s.", s, apr_strerror(s, buf, sizeof(buf)));
        return COSE_FILE_READ_ERROR;
    }
    req->file_buf->file_pos += nbytes;
    return nbytes;
}

int cos_write_http_body_memory(cos_http_response_t *resp, const char *buffer, int len)
{
    cos_buf_t *b;

    b = cos_create_buf(resp->pool, len);
    memcpy(b->pos, buffer, len);
    b->last += len;
    cos_list_add_tail(&b->node, &resp->body);
    resp->body_len += len;

    return len;
}

int cos_write_http_body_file(cos_http_response_t *resp, const char *buffer, int len)
{
    int elen;
    int s;
    char buf[256];
    apr_size_t nbytes = len;

    if (resp->file_buf == NULL) {
        resp->file_buf = cos_create_file_buf(resp->pool);
    }

    if (resp->file_buf->file == NULL) {
        if (resp->file_path == NULL) {
            cos_error_log("resp body file arg NULL.");
            return COSE_INVALID_ARGUMENT;
        }
        cos_trace_log("open file %s.", resp->file_path);
        if ((elen = cos_open_file_for_write(resp->pool, resp->file_path, resp->file_buf)) != COSE_OK) {
            return elen;
        }
    }

    assert(resp->file_buf->file != NULL);
    if ((s = apr_file_write(resp->file_buf->file, buffer, &nbytes)) != APR_SUCCESS) {
        cos_error_log("apr_file_write fialure, code:%d %s.", s, apr_strerror(s, buf, sizeof(buf)));
        return COSE_FILE_WRITE_ERROR;
    }

    resp->file_buf->file_last += nbytes;
    resp->body_len += nbytes;

    return nbytes;
}

int cos_write_http_body_file_part(cos_http_response_t *resp, const char *buffer, int len)
{
    int s;
    char buf[256];
    apr_size_t nbytes = len;

    if (resp->file_buf == NULL) {
        cos_error_log("file_buf is NULL.");
        return COSE_INVALID_ARGUMENT;
    }

    assert(resp->file_buf->file != NULL);
    if ((s = apr_file_write(resp->file_buf->file, buffer, &nbytes)) != APR_SUCCESS) {
        cos_error_log("apr_file_write fialure, code:%d %s.", s, apr_strerror(s, buf, sizeof(buf)));
        return COSE_FILE_WRITE_ERROR;
    }

    resp->file_buf->file_last += nbytes;
    resp->body_len += nbytes;

    return nbytes;
}


int cos_http_io_initialize(const char *user_agent_info, int flags)
{
    CURLcode ecode;
    int s;
    char buf[256];
    cos_http_request_options_t *req_options;
    cos_http_transport_options_t *trans_options;

    if ((ecode = curl_global_init(CURL_GLOBAL_ALL &
           ~((flags & COS_INIT_WINSOCK) ? 0: CURL_GLOBAL_WIN32))) != CURLE_OK)
    {
        cos_error_log("curl_global_init failure, code:%d %s.\n", ecode, curl_easy_strerror(ecode));
        return COSE_INTERNAL_ERROR;
    }

    if ((s = apr_initialize()) != APR_SUCCESS) {
        cos_error_log("apr_initialize failue.\n");
        return COSE_INTERNAL_ERROR;
    }

    if (!user_agent_info || !*user_agent_info) {
        user_agent_info = "Unknown";
    }

    if ((s = cos_pool_create(&cos_global_pool, NULL)) != APR_SUCCESS) {
        cos_error_log("cos_pool_create failure, code:%d %s.\n", s, apr_strerror(s, buf, sizeof(buf)));
        return COSE_INTERNAL_ERROR;
    }

    if ((s = apr_thread_mutex_create(&requestStackMutexG, APR_THREAD_MUTEX_DEFAULT, cos_global_pool)) != APR_SUCCESS) {
        cos_error_log("apr_thread_mutex_create failure, code:%d %s.\n", s, apr_strerror(s, buf, sizeof(buf)));
        return COSE_INTERNAL_ERROR;
    }
    requestStackCountG = 0;

    if ((s = apr_thread_mutex_create(&downloadMutex, APR_THREAD_MUTEX_DEFAULT, cos_global_pool)) != APR_SUCCESS) {
        cos_error_log("apr_thread_mutex_create failure, code:%d %s.\n", s, apr_strerror(s, buf, sizeof(buf)));
        return COSE_INTERNAL_ERROR;
    }

    apr_snprintf(cos_user_agent, sizeof(cos_user_agent)-1, "%s(Compatible %s)",
                 COS_VER, user_agent_info);

    req_options = cos_http_request_options_create(cos_global_pool);
    trans_options = cos_http_transport_options_create(cos_global_pool);
    trans_options->user_agent = cos_user_agent;

    cos_set_default_request_options(req_options);
    cos_set_default_transport_options(trans_options);

    return COSE_OK;
}

void cos_http_io_deinitialize()
{
    apr_thread_mutex_destroy(requestStackMutexG);
    apr_thread_mutex_destroy(downloadMutex);

    while (requestStackCountG--) {
        curl_easy_cleanup(requestStackG[requestStackCountG]);
    }

    if (cos_stderr_file != NULL) {
        apr_file_close(cos_stderr_file);
        cos_stderr_file = NULL;
    }
    if (cos_global_pool != NULL) {
        cos_pool_destroy(cos_global_pool);
        cos_global_pool = NULL;
    }
    apr_terminate();
}

int cos_http_send_request(cos_http_controller_t *ctl, cos_http_request_t *req, cos_http_response_t *resp)
{
    cos_http_transport_t *t;

    t = cos_http_transport_create(ctl->pool);
    t->req = req;
    t->resp = resp;
    t->controller = (cos_http_controller_ex_t *)ctl;

    return cos_http_transport_perform(t);
}
