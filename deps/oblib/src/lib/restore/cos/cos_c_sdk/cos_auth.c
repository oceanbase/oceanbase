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

#include "cos_auth.h"
#include "cos_log.h"
#include "cos_utility.h"

#if 0
static const char *g_s_cos_sub_resource_list[] = {
    "acl",
    "uploadId",
    "uploads",
    "partNumber",
    "response-content-type",
    "response-content-language",
    "response-expires",
    "response-cache-control",
    "response-content-disposition",
    "response-content-encoding",
    "append",
    "position",
    "lifecycle",
    "delete",
    "live",
    "status",
    "comp",
    "vod",
    "startTime",
    "endTime",
    "x-cos-process",
    "security-token",
    NULL,
};
#endif

int cos_get_string_to_sign(cos_pool_t *p,
                           http_method_e method,
                           const cos_string_t *secret_id,
                           const cos_string_t *secret_key,
                           const cos_string_t *canon_res,
                           const cos_table_t *headers,
                           const cos_table_t *params,
                           const int64_t expire,
                           cos_string_t *signstr)
{
    cos_buf_t *fmt_str;
    cos_buf_t *sign_str;
    const char *value;
    apr_time_t now;
    unsigned char time_str[64];
    int time_str_len = 0;
    unsigned char hexdigest[40];
    unsigned char sign_key[40];

    cos_str_null(signstr);

    fmt_str = cos_create_buf(p, 1024);
    if (NULL == fmt_str) {
        cos_error_log("failed to call cos_create_buf.");
        return COSE_OVER_MEMORY;
    }
    sign_str = cos_create_buf(p, 256);
    if (NULL == sign_str) {
        cos_error_log("failed to call cos_create_buf.");
        return COSE_OVER_MEMORY;
    }

    // method
    value = cos_http_method_to_string_lower(method);
    cos_buf_append_string(p, fmt_str, value, strlen(value));
    cos_buf_append_string(p, fmt_str, "\n", sizeof("\n")-1);

    // canonicalized resource(URI)
    cos_buf_append_string(p, fmt_str, canon_res->data, canon_res->len);
    cos_buf_append_string(p, fmt_str, "\n", sizeof("\n")-1);

    // query-parameters
    cos_buf_append_string(p, fmt_str, "\n", sizeof("\n")-1);


    // Host
    cos_buf_append_string(p, fmt_str, "host=", sizeof("host=")-1);
    if (headers != NULL && (value = apr_table_get(headers, COS_HOST)) != NULL) {
        cos_buf_append_string(p, fmt_str, value, strlen(value));
    }
    cos_buf_append_string(p, fmt_str, "\n", sizeof("\n")-1);

    // Format-String sha1hash
    cos_get_sha1_hexdigest(hexdigest, fmt_str->pos, cos_buf_size(fmt_str));

    // construct the string to sign
    cos_buf_append_string(p, sign_str, "sha1\n", sizeof("sha1\n")-1);
    now = apr_time_sec(apr_time_now());
    time_str_len = apr_snprintf((char*)time_str, 64, "%"APR_INT64_T_FMT";%"APR_INT64_T_FMT, now, now + expire);
    cos_buf_append_string(p, sign_str, (char*)time_str, time_str_len);
    cos_buf_append_string(p, sign_str, "\n", sizeof("\n")-1);
    cos_buf_append_string(p, sign_str, (const char*)hexdigest, sizeof(hexdigest));
    cos_buf_append_string(p, sign_str, "\n", sizeof("\n")-1);
    cos_get_hmac_sha1_hexdigest(sign_key, (unsigned char*)secret_key->data, secret_key->len, time_str, time_str_len);
    cos_get_hmac_sha1_hexdigest(hexdigest, sign_key, sizeof(sign_key), sign_str->pos, cos_buf_size(sign_str));

    value = apr_psprintf(p, "q-sign-algorithm=sha1&q-ak=%.*s&q-sign-time=%.*s&q-key-time=%.*s&q-header-list=host&q-url-param-list=&q-signature=%.*s",
                         secret_id->len, secret_id->data,
                         time_str_len, (char*)time_str,
                         time_str_len, (char*)time_str,
                         (int)sizeof(hexdigest), hexdigest);

    // result
    signstr->data = (char *)value;
    signstr->len = strlen(value);

    return COSE_OK;
}

void cos_sign_headers(cos_pool_t *p,
                      const cos_string_t *signstr,
                      const cos_string_t *access_key_id,
                      const cos_string_t *access_key_secret,
                      cos_table_t *headers)
{

    apr_table_setn(headers, COS_AUTHORIZATION, signstr->data);

    return;
}

int cos_get_signed_headers(cos_pool_t *p,
                           const cos_string_t *access_key_id,
                           const cos_string_t *access_key_secret,
                           const cos_string_t* canon_res,
                           cos_http_request_t *req)
{
    int res;
    cos_string_t signstr;

    res = cos_get_string_to_sign(p, req->method, access_key_id, access_key_secret, canon_res,
                                 req->headers, req->query_params, COS_AUTH_EXPIRE_DEFAULT, &signstr);

    if (res != COSE_OK) {
        return res;
    }

    cos_debug_log("signstr:%.*s.", signstr.len, signstr.data);

    cos_sign_headers(p, &signstr, access_key_id, access_key_secret, req->headers);

    return COSE_OK;
}

int cos_sign_request(cos_http_request_t *req,
                     const cos_config_t *config)
{
    cos_string_t canon_res;
    char canon_buf[COS_MAX_URI_LEN];
    char datestr[COS_MAX_GMT_TIME_LEN];
    const char *value;
    int res = COSE_OK;
    int len = 0;

    len = strlen(req->resource);
    if (len >= COS_MAX_URI_LEN - 1) {
        cos_error_log("http resource too long, %s.", req->resource);
        return COSE_INVALID_ARGUMENT;
    }

    canon_res.data = canon_buf;
    canon_res.len = apr_snprintf(canon_buf, sizeof(canon_buf), "/%s", req->resource);

    if ((value = apr_table_get(req->headers, COS_CANNONICALIZED_HEADER_DATE)) == NULL) {
        cos_get_gmt_str_time(datestr);
        apr_table_set(req->headers, COS_DATE, datestr);
    }

    if (req->host && !apr_table_get(req->headers, COS_HOST)) {
        apr_table_set(req->headers, COS_HOST, req->host);
    }

    res = cos_get_signed_headers(req->pool, &config->access_key_id,
                                 &config->access_key_secret, &canon_res, req);
    return res;
}

#if 0
static int is_cos_sub_resource(const char *str);
static int is_cos_canonicalized_header(const char *str);
static int cos_get_canonicalized_headers(cos_pool_t *p,
        const cos_table_t *headers, cos_buf_t *signbuf);
static int cos_get_canonicalized_resource(cos_pool_t *p,
        const cos_table_t *params, cos_buf_t *signbuf);
static int cos_get_canonicalized_params(cos_pool_t *p,
    const cos_table_t *params, cos_buf_t *signbuf);

static int is_cos_sub_resource(const char *str)
{
    int i = 0;
    for ( ; g_s_cos_sub_resource_list[i]; i++) {
        if (apr_strnatcmp(g_s_cos_sub_resource_list[i], str) == 0) {
            return 1;
        }
    }
    return 0;
}


static int is_cos_canonicalized_header(const char *str)
{
    size_t len = strlen(COS_CANNONICALIZED_HEADER_PREFIX);
    return strncasecmp(str, COS_CANNONICALIZED_HEADER_PREFIX, len) == 0;
}

static int cos_get_canonicalized_headers(cos_pool_t *p,
                                         const cos_table_t *headers,
                                         cos_buf_t *signbuf)
{
    int pos;
    int meta_count = 0;
    int i;
    int len;
    const cos_array_header_t *tarr;
    const cos_table_entry_t *telts;
    char **meta_headers;
    const char *value;
    cos_string_t tmp_str;
    char *tmpbuf = (char*)malloc(COS_MAX_HEADER_LEN + 1);
    if (NULL == tmpbuf) {
        cos_error_log("malloc %d memory failed.", COS_MAX_HEADER_LEN + 1);
        return COSE_OVER_MEMORY;
    }

    if (apr_is_empty_table(headers)) {
        free(tmpbuf);
        return COSE_OK;
    }

    // sort user meta header
    tarr = cos_table_elts(headers);
    telts = (cos_table_entry_t*)tarr->elts;
    meta_headers = cos_pcalloc(p, tarr->nelts * sizeof(char*));
    for (pos = 0; pos < tarr->nelts; ++pos) {
        if (is_cos_canonicalized_header(telts[pos].key)) {
            cos_string_t key = cos_string(telts[pos].key);
            cos_string_tolower(&key);
            meta_headers[meta_count++] = key.data;
        }
    }
    if (meta_count == 0) {
        free(tmpbuf);
        return COSE_OK;
    }
    cos_gnome_sort((const char **)meta_headers, meta_count);

    // sign string
    for (i = 0; i < meta_count; ++i) {
        value = apr_table_get(headers, meta_headers[i]);
        cos_str_set(&tmp_str, value);
        cos_strip_space(&tmp_str);
        len = apr_snprintf(tmpbuf, COS_MAX_HEADER_LEN + 1, "%s:%.*s",
                           meta_headers[i], tmp_str.len, tmp_str.data);
        if (len > COS_MAX_HEADER_LEN) {
            free(tmpbuf);
            cos_error_log("user meta header too many, %d > %d.",
                          len, COS_MAX_HEADER_LEN);
            return COSE_INVALID_ARGUMENT;
        }
        tmp_str.data = tmpbuf;
        tmp_str.len = len;
        cos_buf_append_string(p, signbuf, tmpbuf, len);
        cos_buf_append_string(p, signbuf, "\n", sizeof("\n")-1);
    }

    free(tmpbuf);
    return COSE_OK;
}

static int cos_get_canonicalized_resource(cos_pool_t *p,
                                          const cos_table_t *params,
                                          cos_buf_t *signbuf)
{
    int pos;
    int subres_count = 0;
    int i;
    int len;
    char sep;
    const char *value;
    char tmpbuf[COS_MAX_QUERY_ARG_LEN+1];
    char **subres_headers;
    const cos_array_header_t *tarr;
    const cos_table_entry_t *telts;

    if (apr_is_empty_table(params)) {
        return COSE_OK;
    }

    // sort sub resource param
    tarr = cos_table_elts(params);
    telts = (cos_table_entry_t*)tarr->elts;
    subres_headers = cos_pcalloc(p, tarr->nelts * sizeof(char*));
    for (pos = 0; pos < tarr->nelts; ++pos) {
        if (is_cos_sub_resource(telts[pos].key)) {
            subres_headers[subres_count++] = telts[pos].key;
        }
    }
    if (subres_count == 0) {
        return COSE_OK;
    }
    cos_gnome_sort((const char **)subres_headers, subres_count);

    // sign string
    sep = '?';
    for (i = 0; i < subres_count; ++i) {
        value = apr_table_get(params, subres_headers[i]);
        if (value != NULL && *value != '\0') {
            len = apr_snprintf(tmpbuf, sizeof(tmpbuf), "%c%s=%s",
                    sep, subres_headers[i], value);
        } else {
            len = apr_snprintf(tmpbuf, sizeof(tmpbuf), "%c%s",
                    sep, subres_headers[i]);
        }
        if (len >= COS_MAX_QUERY_ARG_LEN) {
            cos_error_log("http query params too long, %s.", tmpbuf);
            return COSE_INVALID_ARGUMENT;
        }
        cos_buf_append_string(p, signbuf, tmpbuf, len);
        sep = '&';
    }

    return COSE_OK;
}

int get_cos_request_signature(const cos_request_options_t *options,
                              cos_http_request_t *req,
                              const cos_string_t *expires,
                              cos_string_t *signature)
{
    cos_string_t canon_res;
    char canon_buf[COS_MAX_URI_LEN];
    const char *value;
    cos_string_t signstr;
    int res = COSE_OK;
    int b64Len;
    unsigned char hmac[20];
    char b64[((20 + 1) * 4) / 3];

    canon_res.data = canon_buf;
    canon_res.len = apr_snprintf(canon_buf, sizeof(canon_buf), "/%s", req->resource);

    apr_table_set(req->headers, COS_DATE, expires->data);

    if ((res = cos_get_string_to_sign(options->pool, req->method, &options->config->access_key_id, &options->config->access_key_secret, &canon_res,
        req->headers, req->query_params, &signstr))!= COSE_OK) {
        return res;
    }

    HMAC_SHA1(hmac, (unsigned char *)options->config->access_key_secret.data,
              options->config->access_key_secret.len,
              (unsigned char *)signstr.data, signstr.len);

    b64Len = cos_base64_encode(hmac, 20, b64);
    value = apr_psprintf(options->pool, "%.*s", b64Len, b64);
    cos_str_set(signature, value);

    return res;
}

int cos_get_signed_url(const cos_request_options_t *options,
                       cos_http_request_t *req,
                       const cos_string_t *expires,
                       cos_string_t *signed_url)
{
    char *signed_url_str;
    cos_string_t querystr;
    char uristr[3*COS_MAX_URI_LEN+1];
    int res = COSE_OK;
    cos_string_t signature;
    const char *proto;

    if (options->config->sts_token.data != NULL) {
        apr_table_set(req->query_params, COS_SECURITY_TOKEN, options->config->sts_token.data);
    }

    res = get_cos_request_signature(options, req, expires, &signature);
    if (res != COSE_OK) {
        return res;
    }

    apr_table_set(req->query_params, COS_ACCESSKEYID, options->config->access_key_id.data);
    apr_table_set(req->query_params, COS_EXPIRES, expires->data);
    apr_table_set(req->query_params, COS_SIGNATURE, signature.data);

    uristr[0] = '\0';
    cos_str_null(&querystr);
    res = cos_url_encode(uristr, req->uri, COS_MAX_URI_LEN);
    if (res != COSE_OK) {
        return res;
    }

    res = cos_query_params_to_string(options->pool, req->query_params, &querystr);
    if (res != COSE_OK) {
        return res;
    }

    proto = strlen(req->proto) != 0 ? req->proto : COS_HTTP_PREFIX;
    signed_url_str = apr_psprintf(options->pool, "%s%s/%s%.*s",
                                  proto, req->host, uristr,
                                  querystr.len, querystr.data);
    cos_str_set(signed_url, signed_url_str);

    return res;
}


int cos_get_rtmp_signed_url(const cos_request_options_t *options,
                            cos_http_request_t *req,
                            const cos_string_t *expires,
                            const cos_string_t *play_list_name,
                            cos_table_t *params,
                            cos_string_t *signed_url)
{
    char *signed_url_str;
    cos_string_t querystr;
    char uristr[3*COS_MAX_URI_LEN+1];
    int res = COSE_OK;
    cos_string_t signature;
    int pos = 0;
    const cos_array_header_t *tarr;
    const cos_table_entry_t *telts;

    if (NULL != params) {
        tarr = cos_table_elts(params);
        telts = (cos_table_entry_t*)tarr->elts;
        for (pos = 0; pos < tarr->nelts; ++pos) {
            apr_table_set(req->query_params, telts[pos].key, telts[pos].val);
        }
    }
    apr_table_set(req->query_params, COS_PLAY_LIST_NAME, play_list_name->data);

    res = get_cos_rtmp_request_signature(options, req, expires,&signature);
    if (res != COSE_OK) {
        return res;
    }

    apr_table_set(req->query_params, COS_ACCESSKEYID,
                  options->config->access_key_id.data);
    apr_table_set(req->query_params, COS_EXPIRES, expires->data);
    apr_table_set(req->query_params, COS_SIGNATURE, signature.data);

    uristr[0] = '\0';
    cos_str_null(&querystr);
    res = cos_url_encode(uristr, req->uri, COS_MAX_URI_LEN);
    if (res != COSE_OK) {
        return res;
    }

    res = cos_query_params_to_string(options->pool, req->query_params, &querystr);
    if (res != COSE_OK) {
        return res;
    }

    signed_url_str = apr_psprintf(options->pool, "%s%s/%s%.*s",
                                  req->proto, req->host, uristr,
                                  querystr.len, querystr.data);
    cos_str_set(signed_url, signed_url_str);

    return res;
}

int get_cos_rtmp_request_signature(const cos_request_options_t *options,
                                   cos_http_request_t *req,
                                   const cos_string_t *expires,
                                   cos_string_t *signature)
{
    cos_string_t canon_res;
    char canon_buf[COS_MAX_URI_LEN];
    const char *value;
    cos_string_t signstr;
    int res = COSE_OK;
    int b64Len;
    unsigned char hmac[20];
    char b64[((20 + 1) * 4) / 3];

    canon_res.data = canon_buf;
    canon_res.len = apr_snprintf(canon_buf, sizeof(canon_buf), "/%s", req->resource);

    if ((res = cos_get_rtmp_string_to_sign(options->pool, expires, &canon_res,
        req->query_params, &signstr))!= COSE_OK) {
        return res;
    }

    HMAC_SHA1(hmac, (unsigned char *)options->config->access_key_secret.data,
              options->config->access_key_secret.len,
              (unsigned char *)signstr.data, signstr.len);

    b64Len = cos_base64_encode(hmac, 20, b64);
    value = apr_psprintf(options->pool, "%.*s", b64Len, b64);
    cos_str_set(signature, value);

    return res;
}

int cos_get_rtmp_string_to_sign(cos_pool_t *p,
                                const cos_string_t *expires,
                                const cos_string_t *canon_res,
                                const cos_table_t *params,
                                cos_string_t *signstr)
{
    int res;
    cos_buf_t *signbuf;
    cos_str_null(signstr);

    signbuf = cos_create_buf(p, 1024);

    // expires
    cos_buf_append_string(p, signbuf, expires->data, expires->len);
    cos_buf_append_string(p, signbuf, "\n", sizeof("\n")-1);

    // canonicalized params
    if ((res = cos_get_canonicalized_params(p, params, signbuf)) != COSE_OK) {
        return res;
    }

    // canonicalized resource
    cos_buf_append_string(p, signbuf, canon_res->data, canon_res->len);

    // result
    signstr->data = (char *)signbuf->pos;
    signstr->len = cos_buf_size(signbuf);

    return COSE_OK;
}

static int cos_get_canonicalized_params(cos_pool_t *p,
                                        const cos_table_t *params,
                                        cos_buf_t *signbuf)
{
    int pos;
    int meta_count = 0;
    int i;
    int len;
    const cos_array_header_t *tarr;
    const cos_table_entry_t *telts;
    char **meta_headers;
    const char *value;
    cos_string_t tmp_str;
    char *tmpbuf = (char*)malloc(COS_MAX_HEADER_LEN + 1);
    if (NULL == tmpbuf) {
        cos_error_log("malloc %d memory failed.", COS_MAX_HEADER_LEN + 1);
        return COSE_OVER_MEMORY;
    }

    if (apr_is_empty_table(params)) {
        free(tmpbuf);
        return COSE_OK;
    }

    // sort user meta header
    tarr = cos_table_elts(params);
    telts = (cos_table_entry_t*)tarr->elts;
    meta_headers = cos_pcalloc(p, tarr->nelts * sizeof(char*));
    for (pos = 0; pos < tarr->nelts; ++pos) {
        cos_string_t key = cos_string(telts[pos].key);
        meta_headers[meta_count++] = key.data;
    }
    if (meta_count == 0) {
        free(tmpbuf);
        return COSE_OK;
    }
    cos_gnome_sort((const char **)meta_headers, meta_count);

    // sign string
    for (i = 0; i < meta_count; ++i) {
        value = apr_table_get(params, meta_headers[i]);
        cos_str_set(&tmp_str, value);
        cos_strip_space(&tmp_str);
        len = apr_snprintf(tmpbuf, COS_MAX_HEADER_LEN + 1, "%s:%.*s",
                           meta_headers[i], tmp_str.len, tmp_str.data);
        if (len > COS_MAX_HEADER_LEN) {
            free(tmpbuf);
            cos_error_log("rtmp parameters too many, %d > %d.",
                          len, COS_MAX_HEADER_LEN);
            return COSE_INVALID_ARGUMENT;
        }
        tmp_str.data = tmpbuf;
        tmp_str.len = len;
        cos_buf_append_string(p, signbuf, tmpbuf, len);
        cos_buf_append_string(p, signbuf, "\n", sizeof("\n")-1);
    }

    free(tmpbuf);
    return COSE_OK;
}
#endif
