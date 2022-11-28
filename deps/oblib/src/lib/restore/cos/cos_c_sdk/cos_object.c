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
#include "cos_status.h"
#include "cos_auth.h"
#include "cos_utility.h"
#include "cos_xml.h"
#include "cos_api.h"

cos_status_t *cos_put_object_from_buffer(const cos_request_options_t *options,
                                         const cos_string_t *bucket,
                                         const cos_string_t *object,
                                         cos_list_t *buffer,
                                         cos_table_t *headers,
                                         cos_table_t **resp_headers)
{
    return cos_do_put_object_from_buffer(options, bucket, object, buffer,
                                         headers, NULL, NULL, resp_headers, NULL);
}

cos_status_t *cos_do_put_object_from_buffer(const cos_request_options_t *options,
                                            const cos_string_t *bucket,
                                            const cos_string_t *object,
                                            cos_list_t *buffer,
                                            cos_table_t *headers,
                                            cos_table_t *params,
                                            cos_progress_callback progress_callback,
                                            cos_table_t **resp_headers,
                                            cos_list_t *resp_body)
{
    cos_status_t *s = NULL;
    cos_http_request_t *req = NULL;
    cos_http_response_t *resp = NULL;
    cos_table_t *query_params = NULL;

    headers = cos_table_create_if_null(options, headers, 2);
    set_content_type(NULL, object->data, headers);
    apr_table_add(headers, COS_EXPECT, "");

    query_params = cos_table_create_if_null(options, params, 0);

    cos_add_content_md5_from_buffer(options, buffer, headers);

    cos_init_object_request(options, bucket, object, HTTP_PUT,
                            &req, query_params, headers, progress_callback, 0, &resp);
    cos_write_request_body_from_buffer(buffer, req);

    s = cos_process_request(options, req, resp);
    cos_fill_read_response_body(resp, resp_body);
    cos_fill_read_response_header(resp, resp_headers);

    if (is_enable_crc(options) && has_crc_in_response(resp)) {
        cos_check_crc_consistent(req->crc64, resp->headers, s);
    }

    return s;
}

cos_status_t *cos_put_object_from_file(const cos_request_options_t *options,
                                       const cos_string_t *bucket,
                                       const cos_string_t *object,
                                       const cos_string_t *filename,
                                       cos_table_t *headers,
                                       cos_table_t **resp_headers)
{
    return cos_do_put_object_from_file(options, bucket, object, filename,
                                       headers, NULL, NULL, resp_headers, NULL);
}

cos_status_t *cos_do_put_object_from_file(const cos_request_options_t *options,
                                          const cos_string_t *bucket,
                                          const cos_string_t *object,
                                          const cos_string_t *filename,
                                          cos_table_t *headers,
                                          cos_table_t *params,
                                          cos_progress_callback progress_callback,
                                          cos_table_t **resp_headers,
                                          cos_list_t *resp_body)
{
    cos_status_t *s = NULL;
    cos_http_request_t *req = NULL;
    cos_http_response_t *resp = NULL;
    cos_table_t *query_params = NULL;
    int res = COSE_OK;

    s = cos_status_create(options->pool);

    headers = cos_table_create_if_null(options, headers, 2);
    set_content_type(filename->data, object->data, headers);
    apr_table_add(headers, COS_EXPECT, "");

    query_params = cos_table_create_if_null(options, params, 0);

    cos_add_content_md5_from_file(options, filename, headers);

    cos_init_object_request(options, bucket, object, HTTP_PUT, &req,
                            query_params, headers, progress_callback, 0, &resp);

    res = cos_write_request_body_from_file(options->pool, filename, req);
    if (res != COSE_OK) {
        cos_file_error_status_set(s, res);
        return s;
    }

    s = cos_process_request(options, req, resp);
    cos_fill_read_response_body(resp, resp_body);
    cos_fill_read_response_header(resp, resp_headers);

    if (is_enable_crc(options) && has_crc_in_response(resp)) {
        cos_check_crc_consistent(req->crc64, resp->headers, s);
    }

    return s;
}

cos_status_t *cos_get_object_to_buffer(const cos_request_options_t *options,
                                       const cos_string_t *bucket,
                                       const cos_string_t *object,
                                       cos_table_t *headers,
                                       cos_table_t *params,
                                       cos_list_t *buffer,
                                       cos_table_t **resp_headers)
{
    return cos_do_get_object_to_buffer(options, bucket, object, headers,
                                       params, buffer, NULL, resp_headers);
}

cos_status_t *cos_do_get_object_to_buffer(const cos_request_options_t *options,
                                          const cos_string_t *bucket,
                                          const cos_string_t *object,
                                          cos_table_t *headers,
                                          cos_table_t *params,
                                          cos_list_t *buffer,
                                          cos_progress_callback progress_callback,
                                          cos_table_t **resp_headers)
{
    cos_status_t *s = NULL;
    cos_http_request_t *req = NULL;
    cos_http_response_t *resp = NULL;

    headers = cos_table_create_if_null(options, headers, 0);
    params = cos_table_create_if_null(options, params, 0);

    cos_init_object_request(options, bucket, object, HTTP_GET,
                            &req, params, headers, progress_callback, 0, &resp);

    s = cos_process_request(options, req, resp);
    cos_fill_read_response_body(resp, buffer);
    cos_fill_read_response_header(resp, resp_headers);

    if (is_enable_crc(options) && has_crc_in_response(resp) &&
        !has_range_or_process_in_request(req)) {
        cos_check_crc_consistent(resp->crc64, resp->headers, s);
    } else if (is_enable_crc(options)) {
        cos_check_len_consistent(buffer, resp->headers, s);
    }

    return s;
}

cos_status_t *cos_get_object_to_file(const cos_request_options_t *options,
                                     const cos_string_t *bucket,
                                     const cos_string_t *object,
                                     cos_table_t *headers,
                                     cos_table_t *params,
                                     cos_string_t *filename,
                                     cos_table_t **resp_headers)
{
    return cos_do_get_object_to_file(options, bucket, object, headers,
                                     params, filename, NULL, resp_headers);
}

cos_status_t *cos_do_get_object_to_file(const cos_request_options_t *options,
                                        const cos_string_t *bucket,
                                        const cos_string_t *object,
                                        cos_table_t *headers,
                                        cos_table_t *params,
                                        cos_string_t *filename,
                                        cos_progress_callback progress_callback,
                                        cos_table_t **resp_headers)
{
    cos_status_t *s = NULL;
    cos_http_request_t *req = NULL;
    cos_http_response_t *resp = NULL;
    int res = COSE_OK;
    cos_string_t tmp_filename;

    headers = cos_table_create_if_null(options, headers, 0);
    params = cos_table_create_if_null(options, params, 0);

    cos_get_temporary_file_name(options->pool, filename, &tmp_filename);

    cos_init_object_request(options, bucket, object, HTTP_GET,
                            &req, params, headers, progress_callback, 0, &resp);

    s = cos_status_create(options->pool);
    res = cos_init_read_response_body_to_file(options->pool, &tmp_filename, resp);
    if (res != COSE_OK) {
        cos_file_error_status_set(s, res);
        return s;
    }

    s = cos_process_request(options, req, resp);
    cos_fill_read_response_header(resp, resp_headers);

    if (is_enable_crc(options) && has_crc_in_response(resp) &&
        !has_range_or_process_in_request(req)) {
            cos_check_crc_consistent(resp->crc64, resp->headers, s);
    }

    cos_temp_file_rename(s, tmp_filename.data, filename->data, options->pool);

    return s;
}

cos_status_t *cos_head_object(const cos_request_options_t *options,
                              const cos_string_t *bucket,
                              const cos_string_t *object,
                              cos_table_t *headers,
                              cos_table_t **resp_headers)
{
    cos_status_t *s = NULL;
    cos_http_request_t *req = NULL;
    cos_http_response_t *resp = NULL;
    cos_table_t *query_params = NULL;

    headers = cos_table_create_if_null(options, headers, 0);

    query_params = cos_table_create_if_null(options, query_params, 0);

    cos_init_object_request(options, bucket, object, HTTP_HEAD,
                            &req, query_params, headers, NULL, 0, &resp);

    s = cos_process_request(options, req, resp);
    cos_fill_read_response_header(resp, resp_headers);

    return s;
}

cos_status_t *cos_delete_object(const cos_request_options_t *options,
                                const cos_string_t *bucket,
                                const cos_string_t *object,
                                cos_table_t **resp_headers)
{
    return cos_do_delete_object(options, bucket, object, NULL, resp_headers);
}

cos_status_t *cos_do_delete_object(const cos_request_options_t *options,
                                const cos_string_t *bucket,
                                const cos_string_t *object,
                                cos_table_t *headers,
                                cos_table_t **resp_headers)
{
    cos_status_t *s = NULL;
    cos_http_request_t *req = NULL;
    cos_http_response_t *resp = NULL;
    cos_table_t *pHeaders = NULL;
    cos_table_t *query_params = NULL;

    if (cos_is_null_string((cos_string_t *)object)) {
        s = cos_status_create(options->pool);
        cos_status_set(s, COSE_INVALID_ARGUMENT, COS_CLIENT_ERROR_CODE, "Object is invalid");
        return s;
    }

    pHeaders = cos_table_create_if_null(options, headers, 0);
    query_params = cos_table_create_if_null(options, query_params, 0);

    cos_init_object_request(options, bucket, object, HTTP_DELETE,
                            &req, query_params, pHeaders, NULL, 0, &resp);
    cos_get_object_uri(options, bucket, object, req);

    s = cos_process_request(options, req, resp);
    cos_fill_read_response_header(resp, resp_headers);

    return s;
}


cos_status_t *cos_append_object_from_buffer(const cos_request_options_t *options,
                                            const cos_string_t *bucket,
                                            const cos_string_t *object,
                                            int64_t position,
                                            cos_list_t *buffer,
                                            cos_table_t *headers,
                                            cos_table_t **resp_headers)
{
    cos_status_t *s = NULL;
    cos_http_request_t *req = NULL;
    cos_http_response_t *resp = NULL;
    cos_table_t *query_params = NULL;

    /* init query_params */
    query_params = cos_table_create_if_null(options, query_params, 2);
    apr_table_add(query_params, COS_APPEND, "");
    cos_table_add_int64(query_params, COS_POSITION, position);

    /* init headers */
    headers = cos_table_create_if_null(options, headers, 2);
    set_content_type(NULL, object->data, headers);
    apr_table_add(headers, COS_EXPECT, "");

    cos_init_object_request(options, bucket, object, HTTP_POST,
                            &req, query_params, headers, NULL, 0, &resp);
    cos_write_request_body_from_buffer(buffer, req);

    s = cos_process_request(options, req, resp);
    cos_fill_read_response_header(resp, resp_headers);

    return s;
}

cos_status_t *cos_do_append_object_from_buffer(const cos_request_options_t *options,
                                               const cos_string_t *bucket,
                                               const cos_string_t *object,
                                               int64_t position,
                                               uint64_t init_crc,
                                               cos_list_t *buffer,
                                               cos_table_t *headers,
                                               cos_table_t *params,
                                               cos_progress_callback progress_callback,
                                               cos_table_t **resp_headers,
                                               cos_list_t *resp_body)
{
    cos_status_t *s = NULL;
    cos_http_request_t *req = NULL;
    cos_http_response_t *resp = NULL;
    cos_table_t *query_params = NULL;

    /* init query_params */
    query_params = cos_table_create_if_null(options, params, 2);
    apr_table_add(query_params, COS_APPEND, "");
    cos_table_add_int64(query_params, COS_POSITION, position);

    /* init headers */
    headers = cos_table_create_if_null(options, headers, 2);
    set_content_type(NULL, object->data, headers);
    apr_table_add(headers, COS_EXPECT, "");

    cos_init_object_request(options, bucket, object, HTTP_POST, &req, query_params,
                            headers, progress_callback, init_crc, &resp);
    cos_write_request_body_from_buffer(buffer, req);

    s = cos_process_request(options, req, resp);
    cos_fill_read_response_header(resp, resp_headers);
    cos_fill_read_response_body(resp, resp_body);

    if (is_enable_crc(options) && has_crc_in_response(resp)) {
        cos_check_crc_consistent(req->crc64, resp->headers, s);
    }

    return s;
}

cos_status_t *cos_append_object_from_file(const cos_request_options_t *options,
                                          const cos_string_t *bucket,
                                          const cos_string_t *object,
                                          int64_t position,
                                          const cos_string_t *append_file,
                                          cos_table_t *headers,
                                          cos_table_t **resp_headers)
{
    cos_status_t *s = NULL;
    cos_http_request_t *req = NULL;
    cos_http_response_t *resp = NULL;
    cos_table_t *query_params = NULL;
    int res = COSE_OK;

    /* init query_params */
    query_params = cos_table_create_if_null(options, query_params, 2);
    apr_table_add(query_params, COS_APPEND, "");
    cos_table_add_int64(query_params, COS_POSITION, position);

    /* init headers */
    headers = cos_table_create_if_null(options, headers, 2);
    set_content_type(append_file->data, object->data, headers);
    apr_table_add(headers, COS_EXPECT, "");

    cos_init_object_request(options, bucket, object, HTTP_POST,
                            &req, query_params, headers, NULL, 0, &resp);
    res = cos_write_request_body_from_file(options->pool, append_file, req);

    s = cos_status_create(options->pool);
    if (res != COSE_OK) {
        cos_file_error_status_set(s, res);
        return s;
    }

    s = cos_process_request(options, req, resp);
    cos_fill_read_response_header(resp, resp_headers);

    return s;
}

cos_status_t *cos_do_append_object_from_file(const cos_request_options_t *options,
                                             const cos_string_t *bucket,
                                             const cos_string_t *object,
                                             int64_t position,
                                             uint64_t init_crc,
                                             const cos_string_t *append_file,
                                             cos_table_t *headers,
                                             cos_table_t *params,
                                             cos_progress_callback progress_callback,
                                             cos_table_t **resp_headers,
                                             cos_list_t *resp_body)
{
    cos_status_t *s = NULL;
    cos_http_request_t *req = NULL;
    cos_http_response_t *resp = NULL;
    cos_table_t *query_params = NULL;
    int res = COSE_OK;

    /* init query_params */
    query_params = cos_table_create_if_null(options, params, 2);
    apr_table_add(query_params, COS_APPEND, "");
    cos_table_add_int64(query_params, COS_POSITION, position);

    /* init headers */
    headers = cos_table_create_if_null(options, headers, 2);
    set_content_type(append_file->data, object->data, headers);
    apr_table_add(headers, COS_EXPECT, "");

    cos_init_object_request(options, bucket, object, HTTP_POST,  &req, query_params,
                            headers, progress_callback, init_crc, &resp);
    res = cos_write_request_body_from_file(options->pool, append_file, req);

    s = cos_status_create(options->pool);
    if (res != COSE_OK) {
        cos_file_error_status_set(s, res);
        return s;
    }

    s = cos_process_request(options, req, resp);
    cos_fill_read_response_header(resp, resp_headers);
    cos_fill_read_response_body(resp, resp_body);

    if (is_enable_crc(options) && has_crc_in_response(resp)) {
        cos_check_crc_consistent(req->crc64, resp->headers, s);
    }

    return s;
}

cos_status_t *cos_put_object_acl(const cos_request_options_t *options,
                                 const cos_string_t *bucket,
                                 const cos_string_t *object,
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

    cos_init_object_request(options, bucket, object, HTTP_PUT, &req,
                            query_params, headers, NULL, 0, &resp);

    s = cos_process_request(options, req, resp);
    cos_fill_read_response_header(resp, resp_headers);

    return s;
}

cos_status_t *cos_get_object_acl(const cos_request_options_t *options,
                                 const cos_string_t *bucket,
                                 const cos_string_t *object,
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

    cos_init_object_request(options, bucket, object, HTTP_GET, &req,
                            query_params, headers, NULL, 0, &resp);

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

cos_status_t *cos_copy_object(const cos_request_options_t *options,
                              const cos_string_t *src_bucket,
                              const cos_string_t *src_object,
                              const cos_string_t *src_endpoint,
                              const cos_string_t *dest_bucket,
                              const cos_string_t *dest_object,
                              cos_table_t *headers,
                              cos_copy_object_params_t *copy_object_param,
                              cos_table_t **resp_headers)
{
    cos_status_t *s = NULL;
    int res;
    cos_http_request_t *req = NULL;
    cos_http_response_t *resp = NULL;
    cos_table_t *query_params = NULL;
    char *copy_source = NULL;

    s = cos_status_create(options->pool);

    headers = cos_table_create_if_null(options, headers, 2);
    query_params = cos_table_create_if_null(options, query_params, 0);

    /* init headers */
    copy_source = apr_psprintf(options->pool, "%.*s.%.*s/%.*s",
                               src_bucket->len, src_bucket->data,
                               src_endpoint->len, src_endpoint->data,
                               src_object->len, src_object->data);
    apr_table_add(headers, COS_CANNONICALIZED_HEADER_COPY_SOURCE, copy_source);
    set_content_type(NULL, dest_object->data, headers);

    cos_init_object_request(options, dest_bucket, dest_object, HTTP_PUT,
                            &req, query_params, headers, NULL, 0, &resp);

    s = cos_process_request(options, req, resp);
    cos_fill_read_response_header(resp, resp_headers);
    if (!cos_status_is_ok(s)) {
        return s;
    }

    res = cos_copy_object_parse_from_body(options->pool, &resp->body, copy_object_param);
    if (res != COSE_OK) {
        cos_xml_error_status_set(s, res);
    }

    return s;
}

#if 0
cos_status_t *cos_copy_obj
(
    cos_request_options_t *options,
    const cos_string_t *copy_source,
    const cos_string_t *dest_bucket,
    const cos_string_t *dest_object
)
{
    cos_pool_t *subpool = NULL;
    cos_pool_t *parent_pool = NULL;
    cos_status_t *s = NULL;
    cos_status_t *ret = NULL;
    int64_t total_size = 0;

    parent_pool = options->pool;
    cos_pool_create(&subpool, options->pool);
    options->pool = subpool;

    //get object size
    cos_table_t *head_resp_headers = NULL;
    s = cos_head_object(options, dest_bucket, dest_object, NULL, &head_resp_headers);
    if (!cos_status_is_ok(s)) {
        ret = cos_status_dup(parent_pool, s);
        cos_pool_destroy(subpool);
        options->pool = parent_pool;
        return ret;
    }
    total_size = atol((char*)apr_table_get(head_resp_headers, COS_CONTENT_LENGTH));

    //use part copy if the object is larger than 5G
    if (total_size > (int64_t)5*1024*1024*1024) {
        s = cos_upload_object_by_part_copy(options, copy_source, dest_bucket, dest_object, (int64_t)5*1024*1024*1024);
    }
    //use object copy if the object is no larger than 5G
    else {
        cos_copy_object_params_t *params = NULL;
        params = cos_create_copy_object_params(options->pool);
        s = cos_copy_object(options, copy_source, dest_bucket, dest_object, NULL, params, NULL);
    }

    ret = cos_status_dup(parent_pool, s);
    cos_pool_destroy(subpool);
    options->pool = parent_pool;
    return ret;
}
#endif

cos_status_t *copy
(
    cos_request_options_t *options,
    const cos_string_t *src_bucket,
    const cos_string_t *src_object,
    const cos_string_t *src_endpoint,
    const cos_string_t *dest_bucket,
    const cos_string_t *dest_object,
    int32_t thread_num
)
{
    cos_pool_t *subpool = NULL;
    cos_pool_t *parent_pool = NULL;
    cos_status_t *s = NULL;
    cos_status_t *ret = NULL;
    int64_t total_size = 0;
    int64_t part_size = 0;

    parent_pool = options->pool;
    cos_pool_create(&subpool, options->pool);
    options->pool = subpool;

    //get object size
    cos_table_t *head_resp_headers = NULL;
    cos_request_options_t *head_options = cos_request_options_create(subpool);
    head_options->config = cos_config_create(subpool);
    cos_str_set(&head_options->config->endpoint, src_endpoint->data);
    cos_str_set(&head_options->config->access_key_id, options->config->access_key_id.data);
    cos_str_set(&head_options->config->access_key_secret, options->config->access_key_secret.data);
    cos_str_set(&head_options->config->appid, "");
    head_options->ctl = cos_http_controller_create(subpool, 0);
    s = cos_head_object(head_options, src_bucket, src_object, NULL, &head_resp_headers);
    if (!cos_status_is_ok(s)) {
        ret = cos_status_dup(parent_pool, s);
        cos_pool_destroy(subpool);
        options->pool = parent_pool;
        return ret;
    }
    total_size = atol((char*)apr_table_get(head_resp_headers, COS_CONTENT_LENGTH));
    options->pool = parent_pool;
    cos_pool_destroy(subpool);

    if (thread_num < 1) {
        thread_num = 1;
    }

    part_size = 5*1024*1024;
    while (part_size * 10000 < total_size) {
        part_size *= 2;
    }
    if (part_size > (int64_t)5*1024*1024*1024) {
        part_size = (int64_t)5*1024*1024*1024;
    }

    //use part copy if the object is larger than 5G
    if (total_size > (int64_t)5*1024*1024*1024 && 0 != strcmp(src_endpoint->data, options->config->endpoint.data)) {
        s = cos_upload_object_by_part_copy_mt(options, (cos_string_t *)src_bucket, (cos_string_t *)src_object, (cos_string_t *)src_endpoint, (cos_string_t *)dest_bucket, (cos_string_t *)dest_object, part_size, thread_num, NULL);
    }
    //use object copy if the object is no larger than 5G
    else {
        cos_copy_object_params_t *params = NULL;
        params = cos_create_copy_object_params(options->pool);
        s = cos_copy_object(options, (cos_string_t *)src_bucket, (cos_string_t *)src_object, (cos_string_t *)src_endpoint, dest_bucket, dest_object, NULL, params, NULL);
    }

    ret = cos_status_dup(parent_pool, s);
    cos_pool_destroy(subpool);
    options->pool = parent_pool;
    return ret;
}

cos_status_t *cos_post_object_restore(const cos_request_options_t *options,
                                            const cos_string_t *bucket,
                                            const cos_string_t *object,
                                            cos_object_restore_params_t *restore_params,
                                            cos_table_t *headers,
                                            cos_table_t *params,
                                            cos_table_t **resp_headers)
{
    cos_status_t *s = NULL;
    cos_http_request_t *req = NULL;
    cos_http_response_t *resp = NULL;
    cos_table_t *query_params = NULL;
    cos_list_t body;
    unsigned char *md5 = NULL;
    char *buf = NULL;
    int64_t body_len;
    char *b64_value = NULL;
    int b64_buf_len = (20 + 1) * 4 / 3;
    int b64_len;

    query_params = cos_table_create_if_null(options, params, 1);
    apr_table_add(query_params, COS_RESTORE, "");

    headers = cos_table_create_if_null(options, headers, 1);

    cos_init_object_request(options, bucket, object, HTTP_POST,
                            &req, query_params, headers, NULL, 0, &resp);

    build_object_restore_body(options->pool, restore_params, &body);

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

int cos_gen_sign_string(const cos_request_options_t *options,
                        const cos_string_t *bucket,
                        const cos_string_t *object,
                        const int64_t expire,
                        cos_http_request_t *req,
                        cos_string_t *signstr)
{
    char canon_buf[COS_MAX_URI_LEN];
    cos_string_t canon_res;
    int res;
    int len;

    len = strlen(req->resource);
    if (len >= COS_MAX_URI_LEN - 1) {
        cos_error_log("http resource too long, %s.", req->resource);
        return COSE_INVALID_ARGUMENT;
    }

    canon_res.data = canon_buf;
    canon_res.len = apr_snprintf(canon_buf, sizeof(canon_buf), "/%s", req->resource);

    res = cos_get_string_to_sign(options->pool, req->method, &options->config->access_key_id, &options->config->access_key_secret, &canon_res,
                                 req->headers, req->query_params, expire, signstr);

    if (res != COSE_OK) {
        return res;
    }

    return COSE_OK;
}

int cos_gen_presigned_url(const cos_request_options_t *options,
                          const cos_string_t *bucket,
                          const cos_string_t *object,
                          const int64_t expire,
                          http_method_e method,
                          cos_string_t *presigned_url)
{
    cos_string_t signstr;
    int res;
    cos_http_request_t *req = NULL;
    cos_http_response_t *resp = NULL;
    char uristr[3*COS_MAX_URI_LEN+1];
    char param[3*COS_MAX_QUERY_ARG_LEN+1];
    char *url = NULL;
    const char *proto;

    uristr[0] = '\0';
    param[0] = '\0';
    cos_str_null(&signstr);

    cos_init_object_request(options, bucket, object, method,
                            &req, cos_table_make(options->pool, 1), cos_table_make(options->pool, 1), NULL, 0, &resp);
    if (req->host) {
        apr_table_set(req->headers, COS_HOST, req->host);
    }
    res = cos_gen_sign_string(options, bucket, object, expire, req, &signstr);
    if (res != COSE_OK) {
        cos_error_log("failed to call cos_gen_sign_string, res=%d", res);
        return res;
    }

    res = cos_url_encode(uristr, req->uri, COS_MAX_URI_LEN);
    if (res != COSE_OK) {
        cos_error_log("failed to call cos_url_encode, res=%d", res);
        return res;
    }

    res = cos_url_encode(param, signstr.data, COS_MAX_QUERY_ARG_LEN);
    if (res != COSE_OK) {
        cos_error_log("failed to call cos_url_encode, res=%d", res);
        return res;
    }

    proto = req->proto != NULL && strlen(req->proto) != 0 ? req->proto : COS_HTTP_PREFIX;

    url = apr_psprintf(options->pool, "%s%s/%s?sign=%s",
                       proto,
                       req->host,
                       uristr,
                       param);
    cos_str_set(presigned_url, url);

    return COSE_OK;
}


#if 0
char *cos_gen_signed_url(const cos_request_options_t *options,
                         const cos_string_t *bucket,
                         const cos_string_t *object,
                         int64_t expires,
                         cos_http_request_t *req)
{
    cos_string_t signed_url;
    char *expires_str = NULL;
    cos_string_t expires_time;
    int res = COSE_OK;

    expires_str = apr_psprintf(options->pool, "%" APR_INT64_T_FMT, expires);
    cos_str_set(&expires_time, expires_str);
    cos_get_object_uri(options, bucket, object, req);
    res = cos_get_signed_url(options, req, &expires_time, &signed_url);
    if (res != COSE_OK) {
        return NULL;
    }
    return signed_url.data;
}

cos_status_t *cos_put_object_from_buffer_by_url(const cos_request_options_t *options,
                                                const cos_string_t *signed_url,
                                                cos_list_t *buffer,
                                                cos_table_t *headers,
                                                cos_table_t **resp_headers)
{
    cos_status_t *s = NULL;
    cos_http_request_t *req = NULL;
    cos_http_response_t *resp = NULL;
    cos_table_t *query_params = NULL;

    /* init query_params */
    headers = cos_table_create_if_null(options, headers, 0);
    query_params = cos_table_create_if_null(options, query_params, 0);

    cos_init_signed_url_request(options, signed_url, HTTP_PUT,
                                &req, query_params, headers, &resp);

    cos_write_request_body_from_buffer(buffer, req);

    s = cos_process_signed_request(options, req, resp);
    cos_fill_read_response_header(resp, resp_headers);

    if (is_enable_crc(options) && has_crc_in_response(resp)) {
        cos_check_crc_consistent(req->crc64, resp->headers, s);
    }

    return s;
}

cos_status_t *cos_put_object_from_file_by_url(const cos_request_options_t *options,
                                              const cos_string_t *signed_url,
                                              cos_string_t *filename,
                                              cos_table_t *headers,
                                              cos_table_t **resp_headers)
{
    cos_status_t *s = NULL;
    cos_http_request_t *req = NULL;
    cos_http_response_t *resp = NULL;
    cos_table_t *query_params = NULL;
    int res = COSE_OK;

    s = cos_status_create(options->pool);

    headers = cos_table_create_if_null(options, headers, 0);
    query_params = cos_table_create_if_null(options, query_params, 0);

    cos_init_signed_url_request(options, signed_url, HTTP_PUT,
                                &req, query_params, headers, &resp);
    res = cos_write_request_body_from_file(options->pool, filename, req);
    if (res != COSE_OK) {
        cos_file_error_status_set(s, res);
        return s;
    }

    s = cos_process_signed_request(options, req, resp);
    cos_fill_read_response_header(resp, resp_headers);

    if (is_enable_crc(options) && has_crc_in_response(resp)) {
        cos_check_crc_consistent(req->crc64, resp->headers, s);
    }

    return s;
}

cos_status_t *cos_get_object_to_buffer_by_url(const cos_request_options_t *options,
                                              const cos_string_t *signed_url,
                                              cos_table_t *headers,
                                              cos_table_t *params,
                                              cos_list_t *buffer,
                                              cos_table_t **resp_headers)
{
    cos_status_t *s = NULL;
    cos_http_request_t *req = NULL;
    cos_http_response_t *resp = NULL;

    headers = cos_table_create_if_null(options, headers, 0);
    params = cos_table_create_if_null(options, params, 0);

    cos_init_signed_url_request(options, signed_url, HTTP_GET,
                                &req, params, headers, &resp);

    s = cos_process_signed_request(options, req, resp);
    cos_fill_read_response_body(resp, buffer);
    cos_fill_read_response_header(resp, resp_headers);

    if (is_enable_crc(options) && has_crc_in_response(resp) &&
        !has_range_or_process_in_request(req)) {
            cos_check_crc_consistent(resp->crc64, resp->headers, s);
    }

    return s;
}

cos_status_t *cos_get_object_to_file_by_url(const cos_request_options_t *options,
                                            const cos_string_t *signed_url,
                                            cos_table_t *headers,
                                            cos_table_t *params,
                                            cos_string_t *filename,
                                            cos_table_t **resp_headers)
{
    cos_status_t *s = NULL;
    cos_http_request_t *req = NULL;
    cos_http_response_t *resp = NULL;
    int res = COSE_OK;
    cos_string_t tmp_filename;

    s = cos_status_create(options->pool);

    headers = cos_table_create_if_null(options, headers, 0);
    params = cos_table_create_if_null(options, params, 0);

    cos_get_temporary_file_name(options->pool, filename, &tmp_filename);

    cos_init_signed_url_request(options, signed_url, HTTP_GET,
                                &req, params, headers, &resp);

    res = cos_init_read_response_body_to_file(options->pool, filename, resp);
    if (res != COSE_OK) {
        cos_file_error_status_set(s, res);
        return s;
    }

    s = cos_process_signed_request(options, req, resp);
    cos_fill_read_response_header(resp, resp_headers);

    if (is_enable_crc(options) && has_crc_in_response(resp) &&
        !has_range_or_process_in_request(req)) {
            cos_check_crc_consistent(resp->crc64, resp->headers, s);
    }

    cos_temp_file_rename(s, tmp_filename.data, filename->data, options->pool);

    return s;
}


cos_status_t *cos_head_object_by_url(const cos_request_options_t *options,
                                     const cos_string_t *signed_url,
                                     cos_table_t *headers,
                                     cos_table_t **resp_headers)
{
    cos_status_t *s = NULL;
    cos_http_request_t *req = NULL;
    cos_http_response_t *resp = NULL;
    cos_table_t *query_params = NULL;

    headers = cos_table_create_if_null(options, headers, 0);
    query_params = cos_table_create_if_null(options, query_params, 0);

    cos_init_signed_url_request(options, signed_url, HTTP_HEAD,
                                &req, query_params, headers, &resp);

    s = cos_process_signed_request(options, req, resp);
    cos_fill_read_response_header(resp, resp_headers);

    return s;
}
#endif
