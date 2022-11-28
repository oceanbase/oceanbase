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
#include "cos_sys_util.h"
#include "cos_string.h"
#include "cos_http_io.h"
#include "cos_transport.h"
#include "cos_crc64.h"

int cos_curl_code_to_status(CURLcode code);
static void cos_init_curl_headers(cos_curl_http_transport_t *t);
static void cos_transport_cleanup(cos_http_transport_t *t);
static int cos_init_curl_url(cos_curl_http_transport_t *t);
static void cos_curl_transport_headers_done(cos_curl_http_transport_t *t);
static int cos_curl_transport_setup(cos_curl_http_transport_t *t);
static void cos_curl_transport_finish(cos_curl_http_transport_t *t);
static void cos_move_transport_state(cos_curl_http_transport_t *t, cos_transport_state_e s);

static size_t cos_curl_default_header_callback(char *buffer, size_t size, size_t nitems, void *userdata);
static size_t cos_curl_default_write_callback(char *ptr, size_t size, size_t nmemb, void *userdata);
static size_t cos_curl_default_read_callback(char *buffer, size_t size, size_t nitems, void *instream);

static void cos_init_curl_headers(cos_curl_http_transport_t *t)
{
    int pos;
    char *header;
    const cos_array_header_t *tarr;
    const cos_table_entry_t *telts;
    union cos_func_u func;

    if (t->req->method == HTTP_PUT || t->req->method == HTTP_POST) {
        header = apr_psprintf(t->pool, "Content-Length: %" APR_INT64_T_FMT, t->req->body_len);
        t->headers = curl_slist_append(t->headers, header);
    }

    tarr = cos_table_elts(t->req->headers);
    telts = (cos_table_entry_t*)tarr->elts;
    for (pos = 0; pos < tarr->nelts; ++pos) {
        header = apr_psprintf(t->pool, "%s: %s", telts[pos].key, telts[pos].val);
        t->headers = curl_slist_append(t->headers, header);
    }

    /* Disable these headers if they're not set explicitly */
    if (NULL == apr_table_get(t->req->headers, COS_EXPECT)) {
        header = apr_psprintf(t->pool, "%s: %s", COS_EXPECT, "");
        t->headers = curl_slist_append(t->headers, header);
    }
    if (NULL == apr_table_get(t->req->headers, COS_TRANSFER_ENCODING)) {
        header = apr_psprintf(t->pool, "%s: %s", COS_TRANSFER_ENCODING, "");
        t->headers = curl_slist_append(t->headers, header);
    }

    func.func1 = (cos_func1_pt)curl_slist_free_all;
    cos_fstack_push(t->cleanup, t->headers, func, 1);
}

static int cos_init_curl_url(cos_curl_http_transport_t *t)
{
    int rs;
    const char *proto;
    cos_string_t querystr;
    char uristr[3*COS_MAX_URI_LEN+1];

    uristr[0] = '\0';
    cos_str_null(&querystr);

    if ((rs = cos_url_encode(uristr, t->req->uri, COS_MAX_URI_LEN)) != COSE_OK) {
        t->controller->error_code = rs;
        t->controller->reason = "uri invalid argument.";
        return rs;
    }

    if ((rs = cos_query_params_to_string(t->pool, t->req->query_params, &querystr)) != COSE_OK) {
        t->controller->error_code = rs;
        t->controller->reason = "query params invalid argument.";
        return rs;
    }

    proto = strlen(t->req->proto) != 0 ? t->req->proto : COS_HTTP_PREFIX;
    /* use original host to build url */
    if (NULL == t->controller->options->host_ip || 0 >= t->controller->options->host_port) {
        if (querystr.len == 0) {
            t->url = apr_psprintf(t->pool, "%s%s/%s",
                                  proto,
                                  t->req->host,
                                  uristr);
        } else {
            t->url = apr_psprintf(t->pool, "%s%s/%s%.*s",
                                  proto,
                                  t->req->host,
                                  uristr,
                                  querystr.len,
                                  querystr.data);
        }
    }
    /* use specified ip-port to build url */
    else {
        if (querystr.len == 0) {
            t->url = apr_psprintf(t->pool, "%s%s:%d/%s",
                                  proto,
                                  t->controller->options->host_ip,
                                  t->controller->options->host_port,
                                  uristr);
        } else {
            t->url = apr_psprintf(t->pool, "%s%s:%d/%s%.*s",
                                  proto,
                                  t->controller->options->host_ip,
                                  t->controller->options->host_port,
                                  uristr,
                                  querystr.len,
                                  querystr.data);
        }
    }

    cos_info_log("url:%s", t->url);

    return COSE_OK;
}

static void cos_transport_cleanup(cos_http_transport_t *t)
{
    int s;
    char buf[256];

    if (t->req->file_buf != NULL && t->req->file_buf->owner) {
        cos_trace_log("close request body file.");
        if ((s = apr_file_close(t->req->file_buf->file)) != APR_SUCCESS) {
            cos_warn_log("apr_file_close failure, %s.", apr_strerror(s, buf, sizeof(buf)));
        }
        t->req->file_buf = NULL;
    }

    if (t->resp->file_buf != NULL && t->resp->file_buf->owner) {
        cos_trace_log("close response body file.");
        if ((s = apr_file_close(t->resp->file_buf->file)) != APR_SUCCESS) {
            cos_warn_log("apr_file_close failure, %s.", apr_strerror(s, buf, sizeof(buf)));
        }
        t->resp->file_buf = NULL;
    }
}

cos_http_transport_t *cos_curl_http_transport_create(cos_pool_t *p)
{
    cos_func_u func;
    cos_curl_http_transport_t *t;

    t = (cos_curl_http_transport_t *)cos_pcalloc(p, sizeof(cos_curl_http_transport_t));

    t->pool = p;
    t->options = cos_default_http_transport_options;
    t->cleanup = cos_fstack_create(p, 5);

    func.func1 = (cos_func1_pt)cos_transport_cleanup;
    cos_fstack_push(t->cleanup, t, func, 1);

    t->curl = cos_request_get();
    func.func1 = (cos_func1_pt)request_release;
    cos_fstack_push(t->cleanup, t->curl, func, 1);

    t->header_callback = cos_curl_default_header_callback;
    t->read_callback = cos_curl_default_read_callback;
    t->write_callback = cos_curl_default_write_callback;

    return (cos_http_transport_t *)t;
}

static void cos_move_transport_state(cos_curl_http_transport_t *t, cos_transport_state_e s)
{
    if (t->state < s) {
        t->state = s;
    }
}

void cos_curl_response_headers_parse(cos_pool_t *p, cos_table_t *headers, char *buffer, int len)
{
    char *pos;
    cos_string_t str;
    cos_string_t key;
    cos_string_t value;

    str.data = buffer;
    str.len = len;

    cos_trip_space_and_cntrl(&str);

    pos = cos_strlchr(str.data, str.data + str.len, ':');
    if (pos == NULL) {
        return;
    }
    key.data = str.data;
    key.len = pos - str.data;

    pos += 1;
    value.len = str.data + str.len - pos;
    value.data = pos;
    cos_strip_space(&value);

    apr_table_addn(headers, cos_pstrdup(p, &key), cos_pstrdup(p, &value));
}

size_t cos_curl_default_header_callback(char *buffer, size_t size, size_t nitems, void *userdata)
{
    int len;
    cos_curl_http_transport_t *t;

    t = (cos_curl_http_transport_t *)(userdata);
    len = size * nitems;

    if (t->controller->first_byte_time == 0) {
        t->controller->first_byte_time = apr_time_now();
    }

    cos_curl_response_headers_parse(t->pool, t->resp->headers, buffer, len);

    cos_move_transport_state(t, TRANS_STATE_HEADER);

    return len;
}

static void cos_curl_transport_headers_done(cos_curl_http_transport_t *t)
{
    long http_code;
    CURLcode code;
    const char *value;

    if (t->controller->error_code != COSE_OK) {
        cos_debug_log("has error %d.", t->controller->error_code);
        return;
    }

    if (t->resp->status > 0) {
        cos_trace_log("http response status %d.", t->resp->status);
        return;
    }

    t->resp->status = 0;
    if ((code = curl_easy_getinfo(t->curl, CURLINFO_RESPONSE_CODE, &http_code)) != CURLE_OK) {
        t->controller->reason = apr_pstrdup(t->pool, curl_easy_strerror(code));
        t->controller->error_code = COSE_INTERNAL_ERROR;
        return;
    } else {
        t->resp->status = http_code;
    }

    value = apr_table_get(t->resp->headers, "Content-Length");
    if (value != NULL) {
        t->resp->content_length = cos_atoi64(value);
    }
}

size_t cos_curl_default_write_callback(char *ptr, size_t size, size_t nmemb, void *userdata)
{
    int len;
    int bytes;
    cos_curl_http_transport_t *t;

    t = (cos_curl_http_transport_t *)(userdata);
    len = size * nmemb;

    if (t->controller->first_byte_time == 0) {
        t->controller->first_byte_time = apr_time_now();
    }

    cos_curl_transport_headers_done(t);

    if (t->controller->error_code != COSE_OK) {
        cos_debug_log("write callback abort");
        return 0;
    }

    // On HTTP error, we expect to parse an HTTP error response
    if (t->resp->status < 200 || t->resp->status > 299) {
        bytes = cos_write_http_body_memory(t->resp, ptr, len);
        assert(bytes == len);
        cos_move_transport_state(t, TRANS_STATE_BODY_IN);
        return bytes;
    }

    if (t->resp->type == BODY_IN_MEMORY && t->resp->body_len >= (int64_t)t->controller->options->max_memory_size) {
        t->controller->reason = apr_psprintf(t->pool,
             "receive body too big, current body size: %" APR_INT64_T_FMT ", max memory size: %" APR_INT64_T_FMT,
              t->resp->body_len, t->controller->options->max_memory_size);
        t->controller->error_code = COSE_OVER_MEMORY;
        cos_error_log("error reason:%s, ", t->controller->reason);
        return 0;
    }

    if ((bytes = t->resp->write_body(t->resp, ptr, len)) < 0) {
        cos_debug_log("write body failure, %d.", bytes);
        t->controller->error_code = COSE_WRITE_BODY_ERROR;
        t->controller->reason = "write body failure.";
        return 0;
    }

    if (bytes >= 0) {
        // progress callback
        if (NULL != t->resp->progress_callback) {
            t->resp->progress_callback(t->resp->body_len, t->resp->content_length);
        }

        // crc
        if (t->controller->options->enable_crc) {
            t->resp->crc64 = cos_crc64(t->resp->crc64, ptr, bytes);
        }
    }

    cos_move_transport_state(t, TRANS_STATE_BODY_IN);

    return bytes;
}

size_t cos_curl_default_read_callback(char *buffer, size_t size, size_t nitems, void *instream)
{
    int len;
    int bytes;
    cos_curl_http_transport_t *t;

    t = (cos_curl_http_transport_t *)(instream);
    len = size * nitems;

    if (t->controller->error_code != COSE_OK) {
        cos_debug_log("abort read callback.");
        return CURL_READFUNC_ABORT;
    }

    if ((bytes = t->req->read_body(t->req, buffer, len)) < 0) {
        cos_debug_log("read body failure, %d.", bytes);
        t->controller->error_code = COSE_READ_BODY_ERROR;
        t->controller->reason = "read body failure.";
        return CURL_READFUNC_ABORT;
    }

    if (bytes >= 0) {
        // progress callback
        t->req->consumed_bytes += bytes;
        if (NULL != t->req->progress_callback) {
            t->req->progress_callback(t->req->consumed_bytes, t->req->body_len);
        }

        // crc
        if (t->controller->options->enable_crc) {
            t->req->crc64 = cos_crc64(t->req->crc64, buffer, bytes);
        }
    }

    cos_move_transport_state(t, TRANS_STATE_BODY_OUT);

    return bytes;
}

int cos_curl_code_to_status(CURLcode code)
{
    switch (code) {
        case CURLE_OUT_OF_MEMORY:
            return COSE_OUT_MEMORY;
        case CURLE_COULDNT_RESOLVE_PROXY:
        case CURLE_COULDNT_RESOLVE_HOST:
            return COSE_NAME_LOOKUP_ERROR;
        case CURLE_COULDNT_CONNECT:
            return COSE_FAILED_CONNECT;
        case CURLE_WRITE_ERROR:
        case CURLE_OPERATION_TIMEDOUT:
            return COSE_CONNECTION_FAILED;
        case CURLE_PARTIAL_FILE:
            return COSE_OK;
        case CURLE_SSL_CACERT:
            return COSE_FAILED_VERIFICATION;
        default:
            return COSE_INTERNAL_ERROR;
    }
}

static void cos_curl_transport_finish(cos_curl_http_transport_t *t)
{
    cos_curl_transport_headers_done(t);

    if (t->cleanup != NULL) {
        cos_fstack_destory(t->cleanup);
        t->cleanup = NULL;
    }
}

int cos_curl_transport_setup(cos_curl_http_transport_t *t)
{
    CURLcode code;

#define curl_easy_setopt_safe(opt, val)                                 \
    if ((code = curl_easy_setopt(t->curl, opt, val)) != CURLE_OK) {    \
            t->controller->reason = apr_pstrdup(t->pool, curl_easy_strerror(code)); \
            t->controller->error_code = COSE_FAILED_INITIALIZE;         \
            cos_error_log("curl_easy_setopt failed, code:%d %s.", code, t->controller->reason); \
            return COSE_FAILED_INITIALIZE;                              \
    }

    curl_easy_setopt_safe(CURLOPT_PRIVATE, t);

    curl_easy_setopt_safe(CURLOPT_HEADERDATA, t);
    curl_easy_setopt_safe(CURLOPT_HEADERFUNCTION, t->header_callback);

    curl_easy_setopt_safe(CURLOPT_READDATA, t);
    curl_easy_setopt_safe(CURLOPT_READFUNCTION, t->read_callback);

    curl_easy_setopt_safe(CURLOPT_WRITEDATA, t);
    curl_easy_setopt_safe(CURLOPT_WRITEFUNCTION, t->write_callback);

    curl_easy_setopt_safe(CURLOPT_FILETIME, 1);
    curl_easy_setopt_safe(CURLOPT_NOSIGNAL, 1);
    curl_easy_setopt_safe(CURLOPT_NOPROGRESS, 1);
    curl_easy_setopt_safe(CURLOPT_TCP_NODELAY, 1);
    curl_easy_setopt_safe(CURLOPT_NETRC, CURL_NETRC_IGNORED);

    // transport options
    curl_easy_setopt_safe(CURLOPT_SSL_VERIFYHOST, 0);
    curl_easy_setopt_safe(CURLOPT_SSL_VERIFYPEER, 0);
    curl_easy_setopt_safe(CURLOPT_USERAGENT, t->options->user_agent);

    // request options
    curl_easy_setopt_safe(CURLOPT_DNS_CACHE_TIMEOUT, t->controller->options->dns_cache_timeout);
    curl_easy_setopt_safe(CURLOPT_CONNECTTIMEOUT, t->controller->options->connect_timeout);
    curl_easy_setopt_safe(CURLOPT_LOW_SPEED_LIMIT, t->controller->options->speed_limit);
    curl_easy_setopt_safe(CURLOPT_LOW_SPEED_TIME, t->controller->options->speed_time);

    cos_init_curl_headers(t);
    curl_easy_setopt_safe(CURLOPT_HTTPHEADER, t->headers);

    if (t->controller->options->proxy_host != NULL) {
        // proxy
        curl_easy_setopt_safe(CURLOPT_PROXYTYPE, CURLPROXY_HTTP);
        curl_easy_setopt_safe(CURLOPT_PROXY, t->controller->options->proxy_host);
        // authorize
        if (t->controller->options->proxy_auth != NULL) {
            curl_easy_setopt_safe(CURLOPT_PROXYAUTH, CURLAUTH_BASIC);
            curl_easy_setopt_safe(CURLOPT_PROXYUSERPWD, t->controller->options->proxy_auth);
        }
    }

    if (NULL == t->req->signed_url) {
        if (cos_init_curl_url(t) != COSE_OK) {
            return t->controller->error_code;
        }
    }
    else {
        t->url = t->req->signed_url;
    }
    curl_easy_setopt_safe(CURLOPT_URL, t->url);

    switch (t->req->method) {
        case HTTP_HEAD:
            curl_easy_setopt_safe(CURLOPT_NOBODY, 1);
            break;
        case HTTP_PUT:
            curl_easy_setopt_safe(CURLOPT_UPLOAD, 1);
            break;
        case HTTP_POST:
            curl_easy_setopt_safe(CURLOPT_POST, 1);
            break;
        case HTTP_DELETE:
            curl_easy_setopt_safe(CURLOPT_CUSTOMREQUEST, "DELETE");
            break;
        default: // HTTP_GET
            break;
    }

#undef curl_easy_setopt_safe

    t->state = TRANS_STATE_INIT;

    return COSE_OK;
}

int cos_curl_http_transport_perform(cos_http_transport_t *t_)
{
    int ecode;
    CURLcode code;
    cos_curl_http_transport_t *t = (cos_curl_http_transport_t *)(t_);
    ecode = cos_curl_transport_setup(t);
    if (ecode != COSE_OK) {
        return ecode;
    }

    t->controller->start_time = apr_time_now();
    code = curl_easy_perform(t->curl);
    t->controller->finish_time = apr_time_now();
    cos_move_transport_state(t, TRANS_STATE_DONE);

    if ((code != CURLE_OK) && (t->controller->error_code == COSE_OK)) {
        ecode = cos_curl_code_to_status(code);
        if (ecode != COSE_OK) {
            t->controller->error_code = ecode;
            t->controller->reason = apr_pstrdup(t->pool, curl_easy_strerror(code));
            cos_error_log("transport failure curl code:%d error:%s", code, t->controller->reason);
        }
    }

    cos_curl_transport_finish(t);

    return t->controller->error_code;
}
