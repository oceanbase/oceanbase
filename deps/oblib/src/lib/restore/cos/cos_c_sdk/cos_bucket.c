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
#include "cos_sys_define.h"
#include "cos_sys_util.h"
#include "cos_string.h"
#include "cos_status.h"
#include "cos_auth.h"
#include "cos_utility.h"
#include "cos_xml.h"
#include "cos_api.h"

cos_status_t *cos_get_service(const cos_request_options_t *options,
                                cos_get_service_params_t *params,
                                cos_table_t **resp_headers)
{
    return cos_do_get_service(options, params, NULL, resp_headers);
}


cos_status_t *cos_do_get_service(const cos_request_options_t *options,
                                cos_get_service_params_t *params,
                                cos_table_t *header,
                                cos_table_t **resp_headers)
{
    int res;
    cos_status_t *s = NULL;
    cos_http_request_t *req = NULL;
    cos_http_response_t *resp = NULL;

    cos_table_t *headers = NULL;
    cos_table_t *query_params = NULL;

    query_params = cos_table_create_if_null(options, query_params, 0);
    headers = cos_table_create_if_null(options, header, 1);

    cos_init_service_request(options, HTTP_GET, &req, query_params, headers, params->all_region, &resp);

    s = cos_process_request(options, req, resp);
    cos_fill_read_response_header(resp, resp_headers);
    if (!cos_status_is_ok(s)) {
        return s;
    }

    res = cos_get_service_parse_from_body(options->pool, &resp->body, params);
    if (res != COSE_OK) {
        cos_xml_error_status_set(s, res);
    }

    return s;
}


cos_status_t *cos_head_bucket(const cos_request_options_t *options,
                                const cos_string_t *bucket,
                                cos_table_t **resp_headers)
{
    return cos_do_head_bucket(options, bucket, NULL, resp_headers);
}

cos_status_t *cos_do_head_bucket(const cos_request_options_t *options,
                                const cos_string_t *bucket,
                                cos_table_t *header,
                                cos_table_t **resp_headers)
{
    cos_status_t *s = NULL;
    cos_http_request_t *req = NULL;
    cos_http_response_t *resp = NULL;
    cos_table_t *headers = NULL;
    cos_table_t *query_params = NULL;

    query_params = cos_table_create_if_null(options, query_params, 0);
    headers = cos_table_create_if_null(options, header, 1);

    cos_init_bucket_request(options, bucket, HTTP_HEAD, &req, query_params, headers, &resp);

    s = cos_process_request(options, req, resp);
    cos_fill_read_response_header(resp, resp_headers);
    return s;
}

cos_status_t *cos_create_bucket(const cos_request_options_t *options,
                                const cos_string_t *bucket,
                                cos_acl_e cos_acl,
                                cos_table_t **resp_headers)
{
    return cos_do_create_bucket(options, bucket, cos_acl, NULL, resp_headers);
}

cos_status_t *cos_do_create_bucket(const cos_request_options_t *options,
                                const cos_string_t *bucket,
                                cos_acl_e cos_acl,
                                cos_table_t *headers,
                                cos_table_t **resp_headers)
{
    const char *cos_acl_str = NULL;
    cos_status_t *s = NULL;
    cos_http_request_t *req = NULL;
    cos_http_response_t *resp = NULL;
    cos_table_t *pHeaders = NULL;
    cos_table_t *query_params = NULL;

    query_params = cos_table_create_if_null(options, query_params, 0);

    //init headers
    pHeaders = cos_table_create_if_null(options, headers, 1);
    cos_acl_str = get_cos_acl_str(cos_acl);
    if (cos_acl_str) {
        apr_table_set(pHeaders, COS_CANNONICALIZED_HEADER_ACL, cos_acl_str);
    }

    cos_init_bucket_request(options, bucket, HTTP_PUT, &req,
                            query_params, pHeaders, &resp);

    s = cos_process_request(options, req, resp);
    cos_fill_read_response_header(resp, resp_headers);

    return s;
}


cos_status_t *cos_delete_bucket(const cos_request_options_t *options,
                                const cos_string_t *bucket,
                                cos_table_t **resp_headers)
{
    return cos_do_delete_bucket(options, bucket, NULL, resp_headers);
}

cos_status_t *cos_do_delete_bucket(const cos_request_options_t *options,
                                const cos_string_t *bucket,
                                cos_table_t *headers,
                                cos_table_t **resp_headers)
{
    cos_status_t *s = NULL;
    cos_http_request_t *req = NULL;
    cos_http_response_t *resp = NULL;
    cos_table_t *query_params = NULL;
    cos_table_t *pHeaders = NULL;

    pHeaders = cos_table_create_if_null(options, headers, 0);
    query_params = cos_table_create_if_null(options, query_params, 0);

    cos_init_bucket_request(options, bucket, HTTP_DELETE, &req,
                            query_params, pHeaders, &resp);

    s = cos_process_request(options, req, resp);
    cos_fill_read_response_header(resp, resp_headers);

    return s;
}


cos_status_t *cos_list_object(const cos_request_options_t *options,
                              const cos_string_t *bucket,
                              cos_list_object_params_t *params,
                              cos_table_t **resp_headers)
{
    return cos_do_list_object(options, bucket, NULL, params, resp_headers);
}

cos_status_t *cos_do_list_object(const cos_request_options_t *options,
                              const cos_string_t *bucket,
                              cos_table_t *headers,
                              cos_list_object_params_t *params,
                              cos_table_t **resp_headers)
{
    int res;
    cos_status_t *s = NULL;
    cos_http_request_t *req = NULL;
    cos_http_response_t *resp = NULL;
    cos_table_t *query_params = NULL;
    cos_table_t *pHeaders = NULL;

    //init query_params
    query_params = cos_table_create_if_null(options, query_params, 5);
    if (!cos_is_null_string(&params->encoding_type)) apr_table_add(query_params, COS_ENCODING_TYPE, params->encoding_type.data);
    if (!cos_is_null_string(&params->prefix)) apr_table_add(query_params, COS_PREFIX, params->prefix.data);
    if (!cos_is_null_string(&params->delimiter)) apr_table_add(query_params, COS_DELIMITER, params->delimiter.data);
    if (!cos_is_null_string(&params->marker)) apr_table_add(query_params, COS_MARKER, params->marker.data);
    cos_table_add_int(query_params, COS_MAX_KEYS, params->max_ret);

    //init headers
    pHeaders = cos_table_create_if_null(options, headers, 0);

    cos_init_bucket_request(options, bucket, HTTP_GET, &req,
                            query_params, pHeaders, &resp);

    s = cos_process_request(options, req, resp);
    cos_fill_read_response_header(resp, resp_headers);
    if (!cos_status_is_ok(s)) {
        return s;
    }

    res = cos_list_objects_parse_from_body(options->pool, &resp->body,
            &params->object_list, &params->common_prefix_list,
            &params->next_marker, &params->truncated);
    if (res != COSE_OK) {
        cos_xml_error_status_set(s, res);
    }

    return s;
}


cos_status_t *cos_delete_objects(const cos_request_options_t *options,
                                 const cos_string_t *bucket,
                                 cos_list_t *object_list,
                                 int is_quiet,
                                 cos_table_t **resp_headers,
                                 cos_list_t *deleted_object_list)
{
    return cos_do_delete_objects(options, bucket, object_list, is_quiet, NULL, resp_headers, deleted_object_list);
}

cos_status_t *cos_do_delete_objects(const cos_request_options_t *options,
                                 const cos_string_t *bucket,
                                 cos_list_t *object_list,
                                 int is_quiet,
                                 cos_table_t *headers,
                                 cos_table_t **resp_headers,
                                 cos_list_t *deleted_object_list)
{
    int res;
    cos_status_t *s = NULL;
    cos_http_request_t *req = NULL;
    cos_http_response_t *resp = NULL;
    cos_table_t *pHeaders = NULL;
    cos_table_t *query_params = NULL;
    cos_list_t body;
    unsigned char *md5 = NULL;
    char *buf = NULL;
    int64_t body_len;
    char *b64_value = NULL;
    int b64_buf_len = (20 + 1) * 4 / 3;
    int b64_len;

    //init query_params
    query_params = cos_table_create_if_null(options, query_params, 1);
    apr_table_add(query_params, COS_DELETE, "");

    //init headers
    pHeaders = cos_table_create_if_null(options, headers, 1);
    apr_table_set(pHeaders, COS_CONTENT_TYPE, COS_MULTIPART_CONTENT_TYPE);

    cos_init_bucket_request(options, bucket, HTTP_POST, &req,
                            query_params, pHeaders, &resp);

    build_delete_objects_body(options->pool, object_list, is_quiet, &body);

    //add Content-MD5
    body_len = cos_buf_list_len(&body);
    buf = cos_buf_list_content(options->pool, &body);
    md5 = cos_md5(options->pool, buf, (apr_size_t)body_len);
    b64_value = cos_pcalloc(options->pool, b64_buf_len);
    b64_len = cos_base64_encode(md5, 16, b64_value);
    b64_value[b64_len] = '\0';
    apr_table_addn(pHeaders, COS_CONTENT_MD5, b64_value);

    cos_write_request_body_from_buffer(&body, req);

    s = cos_process_request(options, req, resp);
    cos_fill_read_response_header(resp, resp_headers);

    if (is_quiet) {
        return s;
    }

    if (!cos_status_is_ok(s)) {
        return s;
    }

    res = cos_delete_objects_parse_from_body(options->pool, &resp->body,
                                             deleted_object_list);
    if (res != COSE_OK) {
        cos_xml_error_status_set(s, res);
    }

    return s;
}


cos_status_t *cos_delete_objects_by_prefix(cos_request_options_t *options,
                                           const cos_string_t *bucket,
                                           const cos_string_t *prefix)
{
    cos_pool_t *subpool = NULL;
    cos_pool_t *parent_pool = NULL;
    cos_pool_t *nextmark_pool = NULL;
    int is_quiet = 1;
    cos_status_t *s = NULL;
    cos_status_t *ret = NULL;
    cos_list_object_params_t *params = NULL;
    int list_object_count = 0;

    parent_pool = options->pool;
    params = cos_create_list_object_params(parent_pool);
    if (prefix->data == NULL) {
        cos_str_set(&params->prefix, "");
    } else {
        cos_str_set(&params->prefix, prefix->data);
    }

    cos_pool_create(&nextmark_pool, parent_pool);
    while (params->truncated) {
        cos_table_t *list_object_resp_headers = NULL;
        cos_list_t object_list;
        cos_list_t deleted_object_list;
        cos_list_object_content_t *list_content = NULL;
        cos_table_t *delete_objects_resp_headers = NULL;
        char *key = NULL;
        char *next_mark = NULL;

        cos_pool_create(&subpool, parent_pool);
        options->pool = subpool;
        list_object_count = 0;
        cos_list_init(&object_list);
        s = cos_list_object(options, bucket, params, &list_object_resp_headers);
        if (!cos_status_is_ok(s)) {
            ret = cos_status_dup(parent_pool, s);
            cos_pool_destroy(subpool);
            cos_pool_destroy(nextmark_pool);
            options->pool = parent_pool;
            return ret;
        }

        cos_list_for_each_entry(cos_list_object_content_t, list_content, &params->object_list, node) {
            cos_object_key_t *object_key = cos_create_cos_object_key(subpool);
            key = apr_psprintf(subpool, "%.*s", list_content->key.len,
                               list_content->key.data);
            cos_str_set(&object_key->key, key);
            cos_list_add_tail(&object_key->node, &object_list);
            list_object_count += 1;
        }

        if (list_object_count == 0) {
            ret = cos_status_dup(parent_pool, s);
            cos_pool_destroy(subpool);
            cos_pool_destroy(nextmark_pool);
            options->pool = parent_pool;
            return ret;
        }

        cos_list_init(&deleted_object_list);
        s = cos_delete_objects(options, bucket, &object_list, is_quiet,
                               &delete_objects_resp_headers, &deleted_object_list);
        if (!cos_status_is_ok(s)) {
            ret = cos_status_dup(parent_pool, s);
            cos_pool_destroy(subpool);
            cos_pool_destroy(nextmark_pool);
            options->pool = parent_pool;
            return ret;
        }
        if (!params->truncated) {
            ret = cos_status_dup(parent_pool, s);
        }

        cos_pool_destroy(nextmark_pool);
        cos_pool_create(&nextmark_pool, parent_pool);
        if (params->next_marker.data) {
            next_mark = apr_psprintf(nextmark_pool, "%.*s", params->next_marker.len, params->next_marker.data);
            cos_str_set(&params->marker, next_mark);
        }
        cos_list_init(&params->object_list);

        cos_pool_destroy(subpool);
    }
    cos_pool_destroy(nextmark_pool);
    options->pool = parent_pool;

    return ret;
}

cos_status_t *cos_put_bucket_acl(const cos_request_options_t *options,
                                 const cos_string_t *bucket,
                                 cos_acl_e cos_acl,
                                 const cos_string_t *grant_read,
                                 const cos_string_t *grant_write,
                                 const cos_string_t *grant_full_ctrl,
                                 cos_table_t **resp_headers)
{
    cos_status_t *s = NULL;
    cos_http_request_t *req = NULL;
    cos_http_response_t *resp = NULL;
    cos_table_t *query_params = NULL;
    cos_table_t *headers = NULL;
    const char *cos_acl_str = NULL;

    query_params = cos_table_create_if_null(options, query_params, 1);
    apr_table_add(query_params, COS_ACL, "");

    headers = cos_table_create_if_null(options, headers, 4);
    cos_acl_str = get_cos_acl_str(cos_acl);
    if (cos_acl_str) {
        apr_table_add(headers, COS_CANNONICALIZED_HEADER_ACL, cos_acl_str);
    }
    if (grant_read && !cos_is_null_string((cos_string_t *)grant_read)) {
        apr_table_add(headers, COS_GRANT_READ, grant_read->data);
    }
    if (grant_write && !cos_is_null_string((cos_string_t *)grant_write)) {
        apr_table_add(headers, COS_GRANT_WRITE, grant_write->data);
    }
    if (grant_full_ctrl && !cos_is_null_string((cos_string_t *)grant_full_ctrl)) {
        apr_table_add(headers, COS_GRANT_FULL_CONTROL, grant_full_ctrl->data);
    }

    cos_init_bucket_request(options, bucket, HTTP_PUT, &req,
                            query_params, headers, &resp);

    s = cos_process_request(options, req, resp);
    cos_fill_read_response_header(resp, resp_headers);

    return s;
}

cos_status_t *cos_get_bucket_acl(const cos_request_options_t *options,
                                 const cos_string_t *bucket,
                                 cos_acl_params_t *acl_param,
                                 cos_table_t **resp_headers)
{
    cos_status_t *s = NULL;
    int res;
    cos_http_request_t *req = NULL;
    cos_http_response_t *resp = NULL;
    cos_table_t *query_params = NULL;
    cos_table_t *headers = NULL;

    query_params = cos_table_create_if_null(options, query_params, 1);
    apr_table_add(query_params, COS_ACL, "");

    headers = cos_table_create_if_null(options, headers, 0);

    cos_init_bucket_request(options, bucket, HTTP_GET, &req,
                            query_params, headers, &resp);

    s = cos_process_request(options, req, resp);
    cos_fill_read_response_header(resp, resp_headers);
    if (!cos_status_is_ok(s)) {
        return s;
    }

    res = cos_acl_parse_from_body(options->pool, &resp->body, acl_param);
    if (res != COSE_OK) {
        cos_xml_error_status_set(s, res);
    }

    return s;
}

cos_status_t *cos_put_bucket_lifecycle(const cos_request_options_t *options,
                                       const cos_string_t *bucket,
                                       cos_list_t *lifecycle_rule_list,
                                       cos_table_t **resp_headers)
{
    cos_status_t *s = NULL;
    cos_http_request_t *req = NULL;
    cos_http_response_t *resp = NULL;
    apr_table_t *query_params = NULL;
    cos_table_t *headers = NULL;
    cos_list_t body;
    unsigned char *md5 = NULL;
    char *buf = NULL;
    int64_t body_len;
    char *b64_value = NULL;
    int b64_buf_len = (20 + 1) * 4 / 3;
    int b64_len;

    //init query_params
    query_params = cos_table_create_if_null(options, query_params, 1);
    apr_table_add(query_params, COS_LIFECYCLE, "");

    //init headers
    headers = cos_table_create_if_null(options, headers, 1);

    cos_init_bucket_request(options, bucket, HTTP_PUT, &req,
                            query_params, headers, &resp);

    build_lifecycle_body(options->pool, lifecycle_rule_list, &body);

    //add Content-MD5
    body_len = cos_buf_list_len(&body);
    buf = cos_buf_list_content(options->pool, &body);
    md5 = cos_md5(options->pool, buf, (apr_size_t)body_len);
    b64_value = cos_pcalloc(options->pool, b64_buf_len);
    b64_len = cos_base64_encode(md5, 16, b64_value);
    b64_value[b64_len] = '\0';
    apr_table_addn(headers, COS_CONTENT_MD5, b64_value);

    cos_write_request_body_from_buffer(&body, req);
    s = cos_process_request(options, req, resp);
    cos_fill_read_response_header(resp, resp_headers);

    return s;
}

cos_status_t *cos_get_bucket_lifecycle(const cos_request_options_t *options,
                                       const cos_string_t *bucket,
                                       cos_list_t *lifecycle_rule_list,
                                       cos_table_t **resp_headers)
{
    int res;
    cos_status_t *s = NULL;
    cos_http_request_t *req = NULL;
    cos_http_response_t *resp = NULL;
    cos_table_t *query_params = NULL;
    cos_table_t *headers = NULL;

    //init query_params
    query_params = cos_table_create_if_null(options, query_params, 1);
    apr_table_add(query_params, COS_LIFECYCLE, "");

    //init headers
    headers = cos_table_create_if_null(options, headers, 0);

    cos_init_bucket_request(options, bucket, HTTP_GET, &req,
                            query_params, headers, &resp);

    s = cos_process_request(options, req, resp);
    cos_fill_read_response_header(resp, resp_headers);
    if (!cos_status_is_ok(s)) {
        return s;
    }

    res = cos_lifecycle_rules_parse_from_body(options->pool,
            &resp->body, lifecycle_rule_list);
    if (res != COSE_OK) {
        cos_xml_error_status_set(s, res);
    }

    return s;
}

cos_status_t *cos_delete_bucket_lifecycle(const cos_request_options_t *options,
                                          const cos_string_t *bucket,
                                          cos_table_t **resp_headers)
{
    cos_status_t *s = NULL;
    cos_http_request_t *req = NULL;
    cos_http_response_t *resp = NULL;
    cos_table_t *query_params = NULL;
    cos_table_t *headers = NULL;

    //init query_params
    query_params = cos_table_create_if_null(options, query_params, 1);
    apr_table_add(query_params, COS_LIFECYCLE, "");

    //init headers
    headers = cos_table_create_if_null(options, headers, 0);

    cos_init_bucket_request(options, bucket, HTTP_DELETE, &req,
                            query_params, headers, &resp);

    s = cos_process_request(options, req, resp);
    cos_fill_read_response_header(resp, resp_headers);

    return s;
}

cos_status_t *cos_put_bucket_cors(const cos_request_options_t *options,
                                       const cos_string_t *bucket,
                                       cos_list_t *cors_rule_list,
                                       cos_table_t **resp_headers)
{
    cos_status_t *s = NULL;
    cos_http_request_t *req = NULL;
    cos_http_response_t *resp = NULL;
    apr_table_t *query_params = NULL;
    cos_table_t *headers = NULL;
    cos_list_t body;
    unsigned char *md5 = NULL;
    char *buf = NULL;
    int64_t body_len;
    char *b64_value = NULL;
    int b64_buf_len = (20 + 1) * 4 / 3;
    int b64_len;

    //init query_params
    query_params = cos_table_create_if_null(options, query_params, 1);
    apr_table_add(query_params, COS_CORS, "");

    //init headers
    headers = cos_table_create_if_null(options, headers, 2);

    cos_init_bucket_request(options, bucket, HTTP_PUT, &req,
                            query_params, headers, &resp);

    build_cors_body(options->pool, cors_rule_list, &body);

    //add Content-MD5
    body_len = cos_buf_list_len(&body);
    buf = cos_buf_list_content(options->pool, &body);
    md5 = cos_md5(options->pool, buf, (apr_size_t)body_len);
    b64_value = cos_pcalloc(options->pool, b64_buf_len);
    b64_len = cos_base64_encode(md5, 16, b64_value);
    b64_value[b64_len] = '\0';
    apr_table_addn(headers, COS_CONTENT_MD5, b64_value);

    apr_table_addn(headers, COS_CONTENT_TYPE, "application/xml");

    cos_write_request_body_from_buffer(&body, req);
    s = cos_process_request(options, req, resp);
    cos_fill_read_response_header(resp, resp_headers);

    return s;
}

cos_status_t *cos_get_bucket_cors(const cos_request_options_t *options,
                                       const cos_string_t *bucket,
                                       cos_list_t *cors_rule_list,
                                       cos_table_t **resp_headers)
{
    int res;
    cos_status_t *s = NULL;
    cos_http_request_t *req = NULL;
    cos_http_response_t *resp = NULL;
    cos_table_t *query_params = NULL;
    cos_table_t *headers = NULL;

    //init query_params
    query_params = cos_table_create_if_null(options, query_params, 1);
    apr_table_add(query_params, COS_CORS, "");

    //init headers
    headers = cos_table_create_if_null(options, headers, 0);

    cos_init_bucket_request(options, bucket, HTTP_GET, &req,
                            query_params, headers, &resp);

    s = cos_process_request(options, req, resp);
    cos_fill_read_response_header(resp, resp_headers);
    if (!cos_status_is_ok(s)) {
        return s;
    }

    res = cos_cors_rules_parse_from_body(options->pool,
            &resp->body, cors_rule_list);
    if (res != COSE_OK) {
        cos_xml_error_status_set(s, res);
    }

    return s;
}

cos_status_t *cos_delete_bucket_cors(const cos_request_options_t *options,
                                          const cos_string_t *bucket,
                                          cos_table_t **resp_headers)
{
    cos_status_t *s = NULL;
    cos_http_request_t *req = NULL;
    cos_http_response_t *resp = NULL;
    cos_table_t *query_params = NULL;
    cos_table_t *headers = NULL;

    //init query_params
    query_params = cos_table_create_if_null(options, query_params, 1);
    apr_table_add(query_params, COS_CORS, "");

    //init headers
    headers = cos_table_create_if_null(options, headers, 0);

    cos_init_bucket_request(options, bucket, HTTP_DELETE, &req,
                            query_params, headers, &resp);

    s = cos_process_request(options, req, resp);
    cos_fill_read_response_header(resp, resp_headers);

    return s;
}

cos_status_t *cos_put_bucket_versioning
(
    const cos_request_options_t *options,
    const cos_string_t *bucket,
    cos_versioning_content_t *versioning,
    cos_table_t **resp_headers
)
{
    cos_status_t *s = NULL;
    cos_http_request_t *req = NULL;
    cos_http_response_t *resp = NULL;
    apr_table_t *query_params = NULL;
    cos_table_t *headers = NULL;
    cos_list_t body;
    unsigned char *md5 = NULL;
    char *buf = NULL;
    int64_t body_len;
    char *b64_value = NULL;
    int b64_buf_len = (20 + 1) * 4 / 3;
    int b64_len;

    //init query_params
    query_params = cos_table_create_if_null(options, query_params, 1);
    apr_table_add(query_params, COS_VERSIONING, "");

    //init headers
    headers = cos_table_create_if_null(options, headers, 2);

    cos_init_bucket_request(options, bucket, HTTP_PUT, &req,
                            query_params, headers, &resp);

    build_versioning_body(options->pool, versioning, &body);

    //add Content-MD5
    body_len = cos_buf_list_len(&body);
    buf = cos_buf_list_content(options->pool, &body);
    md5 = cos_md5(options->pool, buf, (apr_size_t)body_len);
    b64_value = cos_pcalloc(options->pool, b64_buf_len);
    b64_len = cos_base64_encode(md5, 16, b64_value);
    b64_value[b64_len] = '\0';
    apr_table_addn(headers, COS_CONTENT_MD5, b64_value);

    apr_table_addn(headers, COS_CONTENT_TYPE, "application/xml");

    cos_write_request_body_from_buffer(&body, req);
    s = cos_process_request(options, req, resp);
    cos_fill_read_response_header(resp, resp_headers);

    return s;
}

cos_status_t *cos_get_bucket_versioning
(
    const cos_request_options_t *options,
    const cos_string_t *bucket,
    cos_versioning_content_t *versioning,
    cos_table_t **resp_headers
)
{
    int res;
    cos_status_t *s = NULL;
    cos_http_request_t *req = NULL;
    cos_http_response_t *resp = NULL;
    cos_table_t *query_params = NULL;
    cos_table_t *headers = NULL;

    //init query_params
    query_params = cos_table_create_if_null(options, query_params, 1);
    apr_table_add(query_params, COS_VERSIONING, "");

    //init headers
    headers = cos_table_create_if_null(options, headers, 0);

    cos_init_bucket_request(options, bucket, HTTP_GET, &req,
                            query_params, headers, &resp);

    s = cos_process_request(options, req, resp);
    cos_fill_read_response_header(resp, resp_headers);
    if (!cos_status_is_ok(s)) {
        return s;
    }

    res = cos_versioning_parse_from_body(options->pool, &resp->body, versioning);
    if (res != COSE_OK) {
        cos_xml_error_status_set(s, res);
    }

    return s;
}

cos_status_t *cos_put_bucket_replication
(
    const cos_request_options_t *options,
    const cos_string_t *bucket,
    cos_replication_params_t *replication_param,
    cos_table_t **resp_headers
)
{
    cos_status_t *s = NULL;
    cos_http_request_t *req = NULL;
    cos_http_response_t *resp = NULL;
    apr_table_t *query_params = NULL;
    cos_table_t *headers = NULL;
    cos_list_t body;
    unsigned char *md5 = NULL;
    char *buf = NULL;
    int64_t body_len;
    char *b64_value = NULL;
    int b64_buf_len = (20 + 1) * 4 / 3;
    int b64_len;

    //init query_params
    query_params = cos_table_create_if_null(options, query_params, 1);
    apr_table_add(query_params, COS_REPLICATION, "");

    //init headers
    headers = cos_table_create_if_null(options, headers, 2);

    cos_init_bucket_request(options, bucket, HTTP_PUT, &req,
                            query_params, headers, &resp);

    build_replication_body(options->pool, replication_param, &body);

    //add Content-MD5
    body_len = cos_buf_list_len(&body);
    buf = cos_buf_list_content(options->pool, &body);
    md5 = cos_md5(options->pool, buf, (apr_size_t)body_len);
    b64_value = cos_pcalloc(options->pool, b64_buf_len);
    b64_len = cos_base64_encode(md5, 16, b64_value);
    b64_value[b64_len] = '\0';
    apr_table_addn(headers, COS_CONTENT_MD5, b64_value);

    apr_table_addn(headers, COS_CONTENT_TYPE, "application/xml");

    cos_write_request_body_from_buffer(&body, req);
    s = cos_process_request(options, req, resp);
    cos_fill_read_response_header(resp, resp_headers);

    return s;
}

cos_status_t *cos_get_bucket_replication
(
    const cos_request_options_t *options,
    const cos_string_t *bucket,
    cos_replication_params_t *replication_param,
    cos_table_t **resp_headers
)
{
    cos_status_t *s = NULL;
    int res;
    cos_http_request_t *req = NULL;
    cos_http_response_t *resp = NULL;
    cos_table_t *query_params = NULL;
    cos_table_t *headers = NULL;

    query_params = cos_table_create_if_null(options, query_params, 1);
    apr_table_add(query_params, COS_REPLICATION, "");

    headers = cos_table_create_if_null(options, headers, 0);

    cos_init_bucket_request(options, bucket, HTTP_GET, &req,
                            query_params, headers, &resp);

    s = cos_process_request(options, req, resp);
    cos_fill_read_response_header(resp, resp_headers);
    if (!cos_status_is_ok(s)) {
        return s;
    }

    res = cos_replication_parse_from_body(options->pool, &resp->body, replication_param);
    if (res != COSE_OK) {
        cos_xml_error_status_set(s, res);
    }

    return s;
}

cos_status_t *cos_delete_bucket_replication
(
    const cos_request_options_t *options,
    const cos_string_t *bucket,
    cos_table_t **resp_headers
)
{
    cos_status_t *s = NULL;
    cos_http_request_t *req = NULL;
    cos_http_response_t *resp = NULL;
    cos_table_t *query_params = NULL;
    cos_table_t *headers = NULL;

    //init query_params
    query_params = cos_table_create_if_null(options, query_params, 1);
    apr_table_add(query_params, COS_REPLICATION, "");

    //init headers
    headers = cos_table_create_if_null(options, headers, 0);

    cos_init_bucket_request(options, bucket, HTTP_DELETE, &req,
                            query_params, headers, &resp);

    s = cos_process_request(options, req, resp);
    cos_fill_read_response_header(resp, resp_headers);

    return s;
}

cos_status_t *cos_put_bucket_website(const cos_request_options_t *options,
                                        const cos_string_t *bucket,
                                        cos_website_params_t *website_params,
                                        cos_table_t **resp_headers)
{
    cos_status_t *s = NULL;
    cos_http_request_t *req = NULL;
    cos_http_response_t *resp = NULL;
    cos_table_t *query_params = NULL;
    cos_table_t *headers = NULL;

    cos_list_t body;
    unsigned char *md5 = NULL;
    char *buf = NULL;
    int64_t body_len;
    char *b64_value = NULL;
    int b64_buf_len = (20 + 1) * 4 / 3;
    int b64_len;

    query_params = cos_table_create_if_null(options, query_params, 1);
    apr_table_add(query_params, COS_WEBSITE, "");

    headers = cos_table_create_if_null(options, headers, 0);

    cos_init_bucket_request(options, bucket, HTTP_PUT, &req,
            query_params, headers, &resp);

    build_website_body(options->pool, website_params, &body);

    //add Content-MD5
    body_len = cos_buf_list_len(&body);
    buf = cos_buf_list_content(options->pool, &body);
    md5 = cos_md5(options->pool, buf, (apr_size_t)body_len);
    b64_value = cos_pcalloc(options->pool, b64_buf_len);
    b64_len = cos_base64_encode(md5, 16, b64_value);
    b64_value[b64_len] = '\0';
    apr_table_addn(headers, COS_CONTENT_MD5, b64_value);

    apr_table_addn(headers, COS_CONTENT_TYPE, "application/xml");

    cos_write_request_body_from_buffer(&body, req);
    s = cos_process_request(options, req, resp);
    cos_fill_read_response_header(resp, resp_headers);

    return s;
}

cos_status_t *cos_get_bucket_website(const cos_request_options_t *options,
                                        const cos_string_t *bucket,
                                        cos_website_params_t *website_params,
                                        cos_table_t **resp_headers)
{
    int res;
    cos_status_t *s = NULL;
    cos_http_request_t *req = NULL;
    cos_http_response_t *resp = NULL;
    cos_table_t *query_params = NULL;
    cos_table_t *headers = NULL;

    query_params = cos_table_create_if_null(options, query_params, 1);
    apr_table_add(query_params, COS_WEBSITE, "");

    headers = cos_table_create_if_null(options, headers, 0);

    cos_init_bucket_request(options, bucket, HTTP_GET, &req,
            query_params, headers, &resp);

    s = cos_process_request(options, req, resp);
    cos_fill_read_response_header(resp, resp_headers);
    if (!cos_status_is_ok(s)) {
        return s;
    }

    res = cos_get_website_parse_from_body(options->pool, &resp->body, website_params);
    if (res != COSE_OK) {
        cos_xml_error_status_set(s, res);
    }

    return s;
}

cos_status_t *cos_delete_bucket_website(const cos_request_options_t *options,
                                        const cos_string_t *bucket,
                                        cos_table_t **resp_headers)
{
    cos_status_t *s = NULL;
    cos_http_request_t *req = NULL;
    cos_http_response_t *resp = NULL;
    cos_table_t *query_params = NULL;
    cos_table_t *headers = NULL;

    query_params = cos_table_create_if_null(options, query_params, 1);
    apr_table_add(query_params, COS_WEBSITE, "");
    headers = cos_table_create_if_null(options, headers, 0);

    cos_init_bucket_request(options, bucket, HTTP_DELETE, &req,
            query_params, headers, &resp);

    s = cos_process_request(options, req, resp);
    cos_fill_read_response_header(resp, resp_headers);

    return s;
}

cos_status_t *cos_put_bucket_domain(const cos_request_options_t *options,
                                    const cos_string_t *bucket,
                                    cos_domain_params_t *domain_params,
                                    cos_table_t **resp_headers)
{
    cos_status_t *s = NULL;
    cos_http_request_t *req = NULL;
    cos_http_response_t *resp = NULL;
    cos_table_t *query_params = NULL;
    cos_table_t *headers = NULL;

    cos_list_t body;
    unsigned char *md5 = NULL;
    char *buf = NULL;
    int64_t body_len;
    char *b64_value = NULL;
    int b64_buf_len = (20 + 1) * 4 / 3;
    int b64_len;

    query_params = cos_table_create_if_null(options, query_params, 1);
    apr_table_add(query_params, COS_DOMAIN, "");

    headers = cos_table_create_if_null(options, headers, 0);

    cos_init_bucket_request(options, bucket, HTTP_PUT, &req,
            query_params, headers, &resp);

    build_domain_body(options->pool, domain_params, &body);

    //add Content-MD5
    body_len = cos_buf_list_len(&body);
    buf = cos_buf_list_content(options->pool, &body);
    md5 = cos_md5(options->pool, buf, (apr_size_t)body_len);
    b64_value = cos_pcalloc(options->pool, b64_buf_len);
    b64_len = cos_base64_encode(md5, 16, b64_value);
    b64_value[b64_len] = '\0';
    apr_table_addn(headers, COS_CONTENT_MD5, b64_value);

    apr_table_addn(headers, COS_CONTENT_TYPE, "application/xml");

    cos_write_request_body_from_buffer(&body, req);
    s = cos_process_request(options, req, resp);
    cos_fill_read_response_header(resp, resp_headers);

    return s;
}

cos_status_t *cos_get_bucket_domain(const cos_request_options_t *options,
                                    const cos_string_t *bucket,
                                    cos_domain_params_t *domain_params,
                                    cos_table_t **resp_headers)
{
    int res;
    cos_status_t *s = NULL;
    cos_http_request_t *req = NULL;
    cos_http_response_t *resp = NULL;
    cos_table_t *query_params = NULL;
    cos_table_t *headers = NULL;

    query_params = cos_table_create_if_null(options, query_params, 1);
    apr_table_add(query_params, COS_DOMAIN, "");

    headers = cos_table_create_if_null(options, headers, 0);

    cos_init_bucket_request(options, bucket, HTTP_GET, &req,
            query_params, headers, &resp);

    s = cos_process_request(options, req, resp);
    cos_fill_read_response_header(resp, resp_headers);
    if (!cos_status_is_ok(s)) {
        return s;
    }

    res = cos_get_domain_parse_from_body(options->pool, &resp->body, domain_params);
    if (res != COSE_OK) {
        cos_xml_error_status_set(s, res);
    }

    return s;
}

cos_status_t *cos_put_bucket_logging(const cos_request_options_t *options,
                                    const cos_string_t *bucket,
                                    cos_logging_params_t *logging_params,
                                    cos_table_t **resp_headers)
{
    cos_status_t *s = NULL;
    cos_http_request_t *req = NULL;
    cos_http_response_t *resp = NULL;
    cos_table_t *query_params = NULL;
    cos_table_t *headers = NULL;

    cos_list_t body;
    unsigned char *md5 = NULL;
    char *buf = NULL;
    int64_t body_len;
    char *b64_value = NULL;
    int b64_buf_len = (20 + 1) * 4 / 3;
    int b64_len;

    query_params = cos_table_create_if_null(options, query_params, 1);
    apr_table_add(query_params, COS_LOGGING, "");

    headers = cos_table_create_if_null(options, headers, 0);

    cos_init_bucket_request(options, bucket, HTTP_PUT, &req,
            query_params, headers, &resp);

    build_logging_body(options->pool, logging_params, &body);

    //add Content-MD5
    body_len = cos_buf_list_len(&body);
    buf = cos_buf_list_content(options->pool, &body);
    md5 = cos_md5(options->pool, buf, (apr_size_t)body_len);
    b64_value = cos_pcalloc(options->pool, b64_buf_len);
    b64_len = cos_base64_encode(md5, 16, b64_value);
    b64_value[b64_len] = '\0';
    apr_table_addn(headers, COS_CONTENT_MD5, b64_value);

    apr_table_addn(headers, COS_CONTENT_TYPE, "application/xml");

    cos_write_request_body_from_buffer(&body, req);
    s = cos_process_request(options, req, resp);
    cos_fill_read_response_header(resp, resp_headers);

    return s;
}

cos_status_t *cos_get_bucket_logging(const cos_request_options_t *options,
                                    const cos_string_t *bucket,
                                    cos_logging_params_t *logging_params,
                                    cos_table_t **resp_headers)
{
    int res;
    cos_status_t *s = NULL;
    cos_http_request_t *req = NULL;
    cos_http_response_t *resp = NULL;
    cos_table_t *query_params = NULL;
    cos_table_t *headers = NULL;

    query_params = cos_table_create_if_null(options, query_params, 1);
    apr_table_add(query_params, COS_LOGGING, "");

    headers = cos_table_create_if_null(options, headers, 0);

    cos_init_bucket_request(options, bucket, HTTP_GET, &req,
            query_params, headers, &resp);

    s = cos_process_request(options, req, resp);
    cos_fill_read_response_header(resp, resp_headers);
    if (!cos_status_is_ok(s)) {
        return s;
    }

    res = cos_get_logging_parse_from_body(options->pool, &resp->body, logging_params);
    if (res != COSE_OK) {
        cos_xml_error_status_set(s, res);
    }
    return s;
}

cos_status_t *cos_put_bucket_inventory(const cos_request_options_t *options,
                                    const cos_string_t *bucket,
                                    cos_inventory_params_t *inventory_params,
                                    cos_table_t **resp_headers)
{
    cos_status_t *s = NULL;
    cos_http_request_t *req = NULL;
    cos_http_response_t *resp = NULL;
    cos_table_t *query_params = NULL;
    cos_table_t *headers = NULL;

    cos_list_t body;
    unsigned char *md5 = NULL;
    char *buf = NULL;
    int64_t body_len;
    char *b64_value = NULL;
    int b64_buf_len = (20 + 1) * 4 / 3;
    int b64_len;

    query_params = cos_table_create_if_null(options, query_params, 2);
    apr_table_add(query_params, COS_INVENTORY, "");
    apr_table_add(query_params, "id", inventory_params->id.data);

    headers = cos_table_create_if_null(options, headers, 0);

    cos_init_bucket_request(options, bucket, HTTP_PUT, &req,
            query_params, headers, &resp);

    build_inventory_body(options->pool, inventory_params, &body);

    //add Content-MD5
    body_len = cos_buf_list_len(&body);
    buf = cos_buf_list_content(options->pool, &body);
    md5 = cos_md5(options->pool, buf, (apr_size_t)body_len);
    b64_value = cos_pcalloc(options->pool, b64_buf_len);
    b64_len = cos_base64_encode(md5, 16, b64_value);
    b64_value[b64_len] = '\0';
    apr_table_addn(headers, COS_CONTENT_MD5, b64_value);

    apr_table_addn(headers, COS_CONTENT_TYPE, "application/xml");

    cos_write_request_body_from_buffer(&body, req);
    s = cos_process_request(options, req, resp);
    cos_fill_read_response_header(resp, resp_headers);

    return s;

}

cos_status_t *cos_get_bucket_inventory(const cos_request_options_t *options,
                                    const cos_string_t *bucket,
                                    cos_inventory_params_t *inventory_params,
                                    cos_table_t **resp_headers)
{
    int res;
    cos_status_t *s = NULL;
    cos_http_request_t *req = NULL;
    cos_http_response_t *resp = NULL;
    cos_table_t *query_params = NULL;
    cos_table_t *headers = NULL;

    if (cos_is_null_string(&inventory_params->id)) {
        s = cos_status_create(options->pool);
        cos_status_set(s, COSE_INVALID_ARGUMENT, COS_CLIENT_ERROR_CODE, "Inventory id is empty");
        return s;
    }
    query_params = cos_table_create_if_null(options, query_params, 2);
    apr_table_add(query_params, COS_INVENTORY, "");
    apr_table_add(query_params, "id", inventory_params->id.data);

    headers = cos_table_create_if_null(options, headers, 0);

    cos_init_bucket_request(options, bucket, HTTP_GET, &req,
            query_params, headers, &resp);

    s = cos_process_request(options, req, resp);
    cos_fill_read_response_header(resp, resp_headers);
    if (!cos_status_is_ok(s)) {
        return s;
    }

    res = cos_get_inventory_parse_from_body(options->pool, &resp->body, inventory_params);
    if (res != COSE_OK) {
        cos_xml_error_status_set(s, res);
    }
    return s;
}

cos_status_t *cos_list_bucket_inventory(const cos_request_options_t *options,
                                        const cos_string_t *bucket,
                                        cos_list_inventory_params_t *inventory_params,
                                        cos_table_t **resp_headers)
{
    int res;
    cos_status_t *s = NULL;
    cos_http_request_t *req = NULL;
    cos_http_response_t *resp = NULL;
    cos_table_t *query_params = NULL;
    cos_table_t *headers = NULL;

    query_params = cos_table_create_if_null(options, query_params, 1);
    apr_table_add(query_params, COS_INVENTORY, "");
    if (cos_is_null_string(&inventory_params->continuation_token)) {
        apr_table_add(query_params, "continuation-token", inventory_params->continuation_token.data);
    }

    headers = cos_table_create_if_null(options, headers, 0);

    cos_init_bucket_request(options, bucket, HTTP_GET, &req,
            query_params, headers, &resp);

    s = cos_process_request(options, req, resp);
    cos_fill_read_response_header(resp, resp_headers);
    if (!cos_status_is_ok(s)) {
        return s;
    }

    res = cos_list_inventory_parse_from_body(options->pool, &resp->body, inventory_params);
    if (res != COSE_OK) {
        cos_xml_error_status_set(s, res);
    }

    return s;
}

cos_status_t *cos_delete_bucket_inventory(const cos_request_options_t *options,
                                        const cos_string_t *bucket,
                                        const cos_string_t *id,
                                        cos_table_t **resp_headers)
{
    cos_status_t *s = NULL;
    cos_http_request_t *req = NULL;
    cos_http_response_t *resp = NULL;
    cos_table_t *query_params = NULL;
    cos_table_t *headers = NULL;

    query_params = cos_table_create_if_null(options, query_params, 1);
    apr_table_add(query_params, COS_INVENTORY, "");
    apr_table_add(query_params, "id", id->data);
    headers = cos_table_create_if_null(options, headers, 0);

    cos_init_bucket_request(options, bucket, HTTP_DELETE, &req,
            query_params, headers, &resp);

    s = cos_process_request(options, req, resp);
    cos_fill_read_response_header(resp, resp_headers);

    return s;
}

cos_status_t *cos_put_bucket_tagging(const cos_request_options_t *options,
                                    const cos_string_t *bucket,
                                    cos_tagging_params_t *tagging_params,
                                    cos_table_t **resp_headers)
{
    cos_status_t *s = NULL;
    cos_http_request_t *req = NULL;
    cos_http_response_t *resp = NULL;
    cos_table_t *query_params = NULL;
    cos_table_t *headers = NULL;

    cos_list_t body;
    unsigned char *md5 = NULL;
    char *buf = NULL;
    int64_t body_len;
    char *b64_value = NULL;
    int b64_buf_len = (20 + 1) * 4 / 3;
    int b64_len;

    query_params = cos_table_create_if_null(options, query_params, 1);
    apr_table_add(query_params, COS_TAGGING, "");

    headers = cos_table_create_if_null(options, headers, 0);

    cos_init_bucket_request(options, bucket, HTTP_PUT, &req,
            query_params, headers, &resp);

    build_tagging_body(options->pool, tagging_params, &body);

    //add Content-MD5
    body_len = cos_buf_list_len(&body);
    buf = cos_buf_list_content(options->pool, &body);
    md5 = cos_md5(options->pool, buf, (apr_size_t)body_len);
    b64_value = cos_pcalloc(options->pool, b64_buf_len);
    b64_len = cos_base64_encode(md5, 16, b64_value);
    b64_value[b64_len] = '\0';
    apr_table_addn(headers, COS_CONTENT_MD5, b64_value);

    apr_table_addn(headers, COS_CONTENT_TYPE, "application/xml");

    cos_write_request_body_from_buffer(&body, req);
    s = cos_process_request(options, req, resp);
    cos_fill_read_response_header(resp, resp_headers);

    return s;
}

cos_status_t *cos_get_bucket_tagging(const cos_request_options_t *options,
                                    const cos_string_t *bucket,
                                    cos_tagging_params_t *tagging_params,
                                    cos_table_t **resp_headers)
{
    int res;
    cos_status_t *s = NULL;
    cos_http_request_t *req = NULL;
    cos_http_response_t *resp = NULL;
    cos_table_t *query_params = NULL;
    cos_table_t *headers = NULL;

    query_params = cos_table_create_if_null(options, query_params, 1);
    apr_table_add(query_params, COS_TAGGING, "");

    headers = cos_table_create_if_null(options, headers, 0);

    cos_init_bucket_request(options, bucket, HTTP_GET, &req,
            query_params, headers, &resp);

    s = cos_process_request(options, req, resp);
    cos_fill_read_response_header(resp, resp_headers);
    if (!cos_status_is_ok(s)) {
        return s;
    }

    res = cos_get_tagging_parse_from_body(options->pool, &resp->body, tagging_params);
    if (res != COSE_OK) {
        cos_xml_error_status_set(s, res);
    }
    return s;
}

cos_status_t *cos_delete_bucket_tagging(const cos_request_options_t *options,
                                        const cos_string_t *bucket,
                                        cos_table_t **resp_headers)
{
    cos_status_t *s = NULL;
    cos_http_request_t *req = NULL;
    cos_http_response_t *resp = NULL;
    cos_table_t *query_params = NULL;
    cos_table_t *headers = NULL;

    query_params = cos_table_create_if_null(options, query_params, 1);
    apr_table_add(query_params, COS_TAGGING, "");
    headers = cos_table_create_if_null(options, headers, 0);

    cos_init_bucket_request(options, bucket, HTTP_DELETE, &req,
            query_params, headers, &resp);

    s = cos_process_request(options, req, resp);
    cos_fill_read_response_header(resp, resp_headers);

    return s;
}

cos_status_t *cos_put_bucket_intelligenttiering(const cos_request_options_t *options,
                                                const cos_string_t *bucket,
                                                cos_intelligenttiering_params_t *params,
                                                cos_table_t **resp_headers)
{
    cos_status_t *s = NULL;
    cos_http_request_t *req = NULL;
    cos_http_response_t *resp = NULL;
    cos_table_t *query_params = NULL;
    cos_table_t *headers = NULL;

    cos_list_t body;
    unsigned char *md5 = NULL;
    char *buf = NULL;
    int64_t body_len;
    char *b64_value = NULL;
    int b64_buf_len = (20 + 1) * 4 / 3;
    int b64_len;

    query_params = cos_table_create_if_null(options, query_params, 1);
    apr_table_add(query_params, COS_INTELLIGENTTIERING, "");

    headers = cos_table_create_if_null(options, headers, 0);

    cos_init_bucket_request(options, bucket, HTTP_PUT, &req,
            query_params, headers, &resp);

    build_intelligenttiering_body(options->pool, params, &body);

    //add Content-MD5
    body_len = cos_buf_list_len(&body);
    buf = cos_buf_list_content(options->pool, &body);
    md5 = cos_md5(options->pool, buf, (apr_size_t)body_len);
    b64_value = cos_pcalloc(options->pool, b64_buf_len);
    b64_len = cos_base64_encode(md5, 16, b64_value);
    b64_value[b64_len] = '\0';
    apr_table_addn(headers, COS_CONTENT_MD5, b64_value);

    apr_table_addn(headers, COS_CONTENT_TYPE, "application/xml");

    cos_write_request_body_from_buffer(&body, req);
    s = cos_process_request(options, req, resp);
    cos_fill_read_response_header(resp, resp_headers);

    return s;
}

cos_status_t *cos_get_bucket_intelligenttiering(const cos_request_options_t *options,
                                                const cos_string_t *bucket,
                                                cos_intelligenttiering_params_t *params,
                                                cos_table_t **resp_headers)
{
    int res;
    cos_status_t *s = NULL;
    cos_http_request_t *req = NULL;
    cos_http_response_t *resp = NULL;
    cos_table_t *query_params = NULL;
    cos_table_t *headers = NULL;

    query_params = cos_table_create_if_null(options, query_params, 1);
    apr_table_add(query_params, COS_INTELLIGENTTIERING, "");

    headers = cos_table_create_if_null(options, headers, 0);

    cos_init_bucket_request(options, bucket, HTTP_GET, &req,
            query_params, headers, &resp);

    s = cos_process_request(options, req, resp);
    cos_fill_read_response_header(resp, resp_headers);
    if (!cos_status_is_ok(s)) {
        return s;
    }

    res = cos_get_intelligenttiering_parse_from_body(options->pool, &resp->body, params);
    if (res != COSE_OK) {
        cos_xml_error_status_set(s, res);
    }
    return s;
}
