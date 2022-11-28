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

cos_status_t *cos_init_multipart_upload(const cos_request_options_t *options,
                                        const cos_string_t *bucket,
                                        const cos_string_t *object,
                                        cos_string_t *upload_id,
                                        cos_table_t *headers,
                                        cos_table_t **resp_headers)
{
    int res = COSE_OK;
    cos_status_t *s = NULL;
    cos_http_request_t *req = NULL;
    cos_http_response_t *resp = NULL;
    cos_table_t *query_params = NULL;

    //init query_params
    query_params = cos_table_create_if_null(options, query_params, 1);
    apr_table_add(query_params, COS_UPLOADS, "");

    //init headers
    headers = cos_table_create_if_null(options, headers, 1);
    set_content_type(NULL, object->data, headers);
    cos_set_multipart_content_type(headers);

    cos_init_object_request(options, bucket, object, HTTP_POST,
                            &req, query_params, headers, NULL, 0, &resp);

    s = cos_process_request(options, req, resp);
    cos_fill_read_response_header(resp, resp_headers);
    if (!cos_status_is_ok(s)) {
        return s;
    }

    res = cos_upload_id_parse_from_body(options->pool, &resp->body, upload_id);
    if (res != COSE_OK) {
        cos_xml_error_status_set(s, res);
    }

    return s;
}

cos_status_t *cos_abort_multipart_upload(const cos_request_options_t *options,
                                         const cos_string_t *bucket,
                                         const cos_string_t *object,
                                         cos_string_t *upload_id,
                                         cos_table_t **resp_headers)
{
    cos_status_t *s = NULL;
    cos_http_request_t *req = NULL;
    cos_http_response_t *resp = NULL;
    cos_table_t *query_params = NULL;
    cos_table_t *headers = NULL;

    //init query_params
    query_params = cos_table_create_if_null(options, query_params, 1);
    apr_table_add(query_params, COS_UPLOAD_ID, upload_id->data);

    //init headers
    headers = cos_table_create_if_null(options, headers, 0);

    cos_init_object_request(options, bucket, object, HTTP_DELETE,
                            &req, query_params, headers, NULL, 0, &resp);

    s = cos_process_request(options, req, resp);
    cos_fill_read_response_header(resp, resp_headers);

    return s;
}

cos_status_t *cos_list_upload_part(const cos_request_options_t *options,
                                   const cos_string_t *bucket,
                                   const cos_string_t *object,
                                   const cos_string_t *upload_id,
                                   cos_list_upload_part_params_t *params,
                                   cos_table_t **resp_headers)
{
    int res = COSE_OK;
    cos_status_t *s = NULL;
    cos_http_request_t *req = NULL;
    cos_http_response_t *resp = NULL;
    cos_table_t *query_params = NULL;
    cos_table_t *headers = NULL;

    //init query_params
    query_params = cos_table_create_if_null(options, query_params, 4);
    apr_table_add(query_params, COS_UPLOAD_ID, upload_id->data);
    if (!cos_is_null_string(&params->encoding_type)) apr_table_add(query_params, COS_ENCODING_TYPE, params->encoding_type.data);
    cos_table_add_int(query_params, COS_MAX_PARTS, params->max_ret);
    if (!cos_is_null_string(&params->part_number_marker)) apr_table_add(query_params, COS_PART_NUMBER_MARKER, params->part_number_marker.data);

    //init headers
    headers = cos_table_create_if_null(options, headers, 0);

    cos_init_object_request(options, bucket, object, HTTP_GET,
                            &req, query_params, headers, NULL, 0, &resp);

    s = cos_process_request(options, req, resp);
    cos_fill_read_response_header(resp, resp_headers);
    if (!cos_status_is_ok(s)) {
        return s;
    }

    res = cos_list_parts_parse_from_body(options->pool, &resp->body,
            &params->part_list, &params->next_part_number_marker,
            &params->truncated);
    if (res != COSE_OK) {
        cos_xml_error_status_set(s, res);
    }

    return s;
}

cos_status_t *cos_list_multipart_upload(const cos_request_options_t *options,
                                        const cos_string_t *bucket,
                                        cos_list_multipart_upload_params_t *params,
                                        cos_table_t **resp_headers)
{
    int res = COSE_OK;
    cos_status_t *s = NULL;
    cos_http_request_t *req = NULL;
    cos_http_response_t *resp = NULL;
    cos_table_t *query_params = NULL;
    cos_table_t *headers = NULL;

    //init query_params
    query_params = cos_table_create_if_null(options, query_params, 7);
    apr_table_add(query_params, COS_UPLOADS, "");
    if (!cos_is_null_string(&params->encoding_type)) apr_table_add(query_params, COS_ENCODING_TYPE, params->encoding_type.data);
    if (!cos_is_null_string(&params->prefix)) apr_table_add(query_params, COS_PREFIX, params->prefix.data);
    if (!cos_is_null_string(&params->delimiter)) apr_table_add(query_params, COS_DELIMITER, params->delimiter.data);
    if (!cos_is_null_string(&params->key_marker)) apr_table_add(query_params, COS_KEY_MARKER, params->key_marker.data);
    if (!cos_is_null_string(&params->upload_id_marker)) apr_table_add(query_params, COS_UPLOAD_ID_MARKER, params->upload_id_marker.data);
    cos_table_add_int(query_params, COS_MAX_UPLOADS, params->max_ret);

    //init headers
    headers = cos_table_create_if_null(options, headers, 0);

    cos_init_bucket_request(options, bucket, HTTP_GET, &req,
                            query_params, headers, &resp);

    s = cos_process_request(options, req, resp);
    cos_fill_read_response_header(resp, resp_headers);
    if (!cos_status_is_ok(s)) {
        return s;
    }

    res = cos_list_multipart_uploads_parse_from_body(options->pool, &resp->body,
            &params->upload_list, &params->next_key_marker,
            &params->next_upload_id_marker, &params->truncated);
    if (res != COSE_OK) {
        cos_xml_error_status_set(s, res);
    }

    return s;
}

cos_status_t *cos_complete_multipart_upload(const cos_request_options_t *options,
                                            const cos_string_t *bucket,
                                            const cos_string_t *object,
                                            const cos_string_t *upload_id,
                                            cos_list_t *part_list,
                                            cos_table_t *headers,
                                            cos_table_t **resp_headers)
{
    return cos_do_complete_multipart_upload(options, bucket, object, upload_id, part_list,
                                            headers, NULL, resp_headers, NULL);
}

cos_status_t *cos_do_complete_multipart_upload(const cos_request_options_t *options,
                                               const cos_string_t *bucket,
                                               const cos_string_t *object,
                                               const cos_string_t *upload_id,
                                               cos_list_t *part_list,
                                               cos_table_t *headers,
                                               cos_table_t *params,
                                               cos_table_t **resp_headers,
                                               cos_list_t *resp_body)
{
    cos_status_t *s = NULL;
    cos_http_request_t *req = NULL;
    cos_http_response_t *resp = NULL;
    apr_table_t *query_params = NULL;
    cos_list_t body;

    //init query_params
    query_params = cos_table_create_if_null(options, params, 1);
    apr_table_add(query_params, COS_UPLOAD_ID, upload_id->data);

    //init headers
    headers = cos_table_create_if_null(options, headers, 1);
    set_content_type(NULL, object->data, headers);
    cos_set_multipart_content_type(headers);
    //apr_table_add(headers, COS_REPLACE_OBJECT_META, COS_YES);

    cos_init_object_request(options, bucket, object, HTTP_POST,
                            &req, query_params, headers, NULL, 0, &resp);

    build_complete_multipart_upload_body(options->pool, part_list, &body);
    cos_write_request_body_from_buffer(&body, req);

    s = cos_process_request(options, req, resp);
    cos_fill_read_response_header(resp, resp_headers);
    cos_fill_read_response_body(resp, resp_body);

    return s;
}

cos_status_t *cos_upload_part_from_buffer(const cos_request_options_t *options,
                                          const cos_string_t *bucket,
                                          const cos_string_t *object,
                                          const cos_string_t *upload_id,
                                          int part_num,
                                          cos_list_t *buffer,
                                          cos_table_t **resp_headers)
{
    return cos_do_upload_part_from_buffer(options, bucket, object, upload_id, part_num,
                                          buffer, NULL, NULL, NULL, resp_headers, NULL);
}

cos_status_t *cos_do_upload_part_from_buffer(const cos_request_options_t *options,
                                             const cos_string_t *bucket,
                                             const cos_string_t *object,
                                             const cos_string_t *upload_id,
                                             int part_num,
                                             cos_list_t *buffer,
                                             cos_progress_callback progress_callback,
                                             cos_table_t *headers,
                                             cos_table_t *params,
                                             cos_table_t **resp_headers,
                                             cos_list_t *resp_body)
{
    cos_status_t *s = NULL;
    cos_http_request_t *req = NULL;
    cos_http_response_t *resp = NULL;
    cos_table_t *query_params = NULL;

    //init query_params
    query_params = cos_table_create_if_null(options, params, 2);
    apr_table_add(query_params, COS_UPLOAD_ID, upload_id->data);
    cos_table_add_int(query_params, COS_PARTNUMBER, part_num);

    //init headers
    headers = cos_table_create_if_null(options, headers, 0);

    cos_add_content_md5_from_buffer(options, buffer, headers);

    cos_init_object_request(options, bucket, object, HTTP_PUT, &req, query_params,
                            headers, progress_callback, 0, &resp);

    cos_write_request_body_from_buffer(buffer, req);

    s = cos_process_request(options, req, resp);
    cos_fill_read_response_header(resp, resp_headers);
    cos_fill_read_response_body(resp, resp_body);

    if (is_enable_crc(options) && has_crc_in_response(resp)) {
        cos_check_crc_consistent(req->crc64, resp->headers, s);
    }

    return s;
}

cos_status_t *cos_upload_part_from_file(const cos_request_options_t *options,
                                        const cos_string_t *bucket,
                                        const cos_string_t *object,
                                        const cos_string_t *upload_id,
                                        int part_num,
                                        cos_upload_file_t *upload_file,
                                        cos_table_t **resp_headers)
{
    return cos_do_upload_part_from_file(options, bucket, object, upload_id, part_num,
                                        upload_file, NULL, NULL, NULL, resp_headers, NULL);
}

cos_status_t *cos_do_upload_part_from_file(const cos_request_options_t *options,
                                           const cos_string_t *bucket,
                                           const cos_string_t *object,
                                           const cos_string_t *upload_id,
                                           int part_num,
                                           cos_upload_file_t *upload_file,
                                           cos_progress_callback progress_callback,
                                           cos_table_t *headers,
                                           cos_table_t *params,
                                           cos_table_t **resp_headers,
                                           cos_list_t *resp_body)
{
    cos_status_t *s = NULL;
    cos_http_request_t *req = NULL;
    cos_http_response_t *resp = NULL;
    cos_table_t *query_params = NULL;
    int res = COSE_OK;

    s = cos_status_create(options->pool);

    //init query_params
    query_params = cos_table_create_if_null(options, params, 2);
    apr_table_add(query_params, COS_UPLOAD_ID, upload_id->data);
    cos_table_add_int(query_params, COS_PARTNUMBER, part_num);

    //init headers
    headers = cos_table_create_if_null(options, headers, 0);

    cos_add_content_md5_from_file_range(options, upload_file, headers);

    cos_init_object_request(options, bucket, object, HTTP_PUT, &req,
                            query_params, headers, progress_callback, 0, &resp);

    res = cos_write_request_body_from_upload_file(options->pool, upload_file, req);
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

cos_status_t *cos_upload_part_copy(const cos_request_options_t *options,
                                   cos_upload_part_copy_params_t *params,
                                   cos_table_t *headers,
                                   cos_table_t **resp_headers)
{
    cos_status_t *s = NULL;
    cos_http_request_t *req = NULL;
    cos_http_response_t *resp = NULL;
    cos_table_t *query_params = NULL;
    char *copy_source_range = NULL;
    int res;

    s = cos_status_create(options->pool);

    //init query_params
    query_params = cos_table_create_if_null(options, query_params, 2);
    apr_table_add(query_params, COS_UPLOAD_ID, params->upload_id.data);
    cos_table_add_int(query_params, COS_PARTNUMBER, params->part_num);

    //init headers
    headers = cos_table_create_if_null(options, headers, 2);
    apr_table_add(headers, COS_COPY_SOURCE, params->copy_source.data);
    if (-1 != params->range_start && -1 != params->range_end) {
        copy_source_range = apr_psprintf(options->pool,
                "bytes=%" APR_INT64_T_FMT "-%" APR_INT64_T_FMT,
                params->range_start, params->range_end);
        apr_table_add(headers, COS_COPY_SOURCE_RANGE, copy_source_range);
    }

    cos_init_object_request(options, &params->dest_bucket, &params->dest_object,
                            HTTP_PUT, &req, query_params, headers, NULL, 0, &resp);

    s = cos_process_request(options, req, resp);
    cos_fill_read_response_header(resp, resp_headers);
    if (!cos_status_is_ok(s)) {
        return s;
    }

    if (NULL != params->rsp_content) {
        res = cos_copy_object_parse_from_body(options->pool, &resp->body, params->rsp_content);
        if (res != COSE_OK) {
            cos_xml_error_status_set(s, res);
        }
    }

    return s;
}

cos_status_t *cos_get_sorted_uploaded_part(cos_request_options_t *options,
                                           const cos_string_t *bucket,
                                           const cos_string_t *object,
                                           const cos_string_t *upload_id,
                                           cos_list_t *complete_part_list,
                                           int *part_count)
{
    cos_pool_t *subpool = NULL;
    cos_pool_t *parent_pool = NULL;
    cos_status_t *s = NULL;
    cos_status_t *ret = NULL;
    cos_upload_part_t *part_arr = NULL;
    int part_index = 0;
    int index = 0;
    int uploaded_part_count = 0;
    cos_list_upload_part_params_t *params = NULL;
    cos_list_part_content_t *part_content = NULL;
    cos_complete_part_content_t *complete_content = NULL;
    cos_table_t *list_part_resp_headers = NULL;
    char *part_num_str = NULL;

    parent_pool = options->pool;
    part_arr = cos_palloc(parent_pool, COS_MAX_PART_NUM * sizeof(cos_upload_part_t *));
    params = cos_create_list_upload_part_params(parent_pool);
    while (params->truncated) {
        cos_pool_create(&subpool, parent_pool);
        options->pool = subpool;
        s = cos_list_upload_part(options, bucket, object,
                upload_id, params, &list_part_resp_headers);
        if (!cos_status_is_ok(s)) {
            ret = cos_status_dup(parent_pool, s);
            cos_pool_destroy(subpool);
            options->pool = parent_pool;
            return ret;
        }
        if (!params->truncated) {
            ret = cos_status_dup(parent_pool, s);
        }
        cos_list_for_each_entry(cos_list_part_content_t, part_content, &params->part_list, node) {
            cos_upload_part_t upload_part;
            upload_part.etag = part_content->etag.data;
            upload_part.part_num = atoi(part_content->part_number.data);
            part_arr[part_index++] = upload_part;
            uploaded_part_count++;
        }

        cos_list_init(&params->part_list);
        if (params->next_part_number_marker.data != NULL) {
            cos_str_set(&params->part_number_marker,
                        params->next_part_number_marker.data);
        }

        //sort multipart upload part content
        qsort(part_arr, uploaded_part_count, sizeof(part_arr[0]), part_sort_cmp);

        for (index = 0; index < part_index; ++index) {
            complete_content = cos_create_complete_part_content(parent_pool);
            part_num_str = apr_psprintf(parent_pool, "%d", part_arr[index].part_num);
            cos_str_set(&complete_content->part_number, part_num_str);
            cos_str_set(&complete_content->etag, part_arr[index].etag);
            cos_list_add_tail(&complete_content->node, complete_part_list);
        }
        part_index = 0;
        cos_pool_destroy(subpool);
    }

    *part_count = uploaded_part_count;
    options->pool = parent_pool;

    return ret;
}

cos_status_t *cos_upload_file(cos_request_options_t *options,
                              const cos_string_t *bucket,
                              const cos_string_t *object,
                              cos_string_t *upload_id,
                              cos_string_t *filepath,
                              int64_t part_size,
                              cos_table_t *headers)
{
    cos_pool_t *subpool = NULL;
    cos_pool_t *parent_pool = NULL;
    int64_t start_pos;
    int64_t end_pos;
    int part_num;
    int part_count = 0;
    int res = COSE_OK;
    cos_status_t *s = NULL;
    cos_status_t *ret = NULL;
    cos_file_buf_t *fb = NULL;
    cos_upload_file_t *upload_file = NULL;
    cos_table_t *upload_part_resp_headers = NULL;
    char *part_num_str = NULL;
    char *etag = NULL;
    cos_list_t complete_part_list;
    cos_complete_part_content_t *complete_content = NULL;
    cos_table_t *complete_resp_headers = NULL;

    cos_list_init(&complete_part_list);
    parent_pool = options->pool;

    //get upload_id and uploaded part
    if (NULL == upload_id->data) {
        cos_table_t *init_multipart_headers = NULL;
        cos_table_t *init_multipart_resp_headers = NULL;

        init_multipart_headers = cos_table_make(parent_pool, 0);
        s = cos_init_multipart_upload(options, bucket, object,
                upload_id, init_multipart_headers, &init_multipart_resp_headers);
        if (!cos_status_is_ok(s)) {
            ret = cos_status_dup(parent_pool, s);
           return ret;
        }
    } else {
        s = cos_get_sorted_uploaded_part(options, bucket, object, upload_id,
                &complete_part_list, &part_count);
        if (!cos_status_is_ok(s)) {
            ret = cos_status_dup(parent_pool, s);
            return ret;
        }
    }

    //get part size
    fb = cos_create_file_buf(parent_pool);
    res = cos_open_file_for_read(parent_pool, filepath->data, fb);
    if (res != COSE_OK) {
        s = cos_status_create(parent_pool);
        cos_file_error_status_set(s, res);
        options->pool = parent_pool;
        return s;
    }
    cos_get_part_size(fb->file_last, &part_size);

    //upload part from file
    upload_file = cos_create_upload_file(parent_pool);
    cos_str_set(&upload_file->filename, filepath->data);
    start_pos = part_size * part_count;
    end_pos = start_pos + part_size;
    part_num = part_count + 1;

    while (1) {
        cos_pool_create(&subpool, parent_pool);
        options->pool = subpool;
        upload_file->file_pos = start_pos;
        upload_file->file_last = end_pos;

        s = cos_upload_part_from_file(options, bucket, object, upload_id,
            part_num, upload_file, &upload_part_resp_headers);
        if (!cos_status_is_ok(s)) {
            ret = cos_status_dup(parent_pool, s);
            cos_pool_destroy(subpool);
            options->pool = parent_pool;
            return ret;
        }

        complete_content = cos_create_complete_part_content(parent_pool);
        part_num_str = apr_psprintf(parent_pool, "%d", part_num);
        cos_str_set(&complete_content->part_number, part_num_str);
        etag = apr_pstrdup(parent_pool,
                           (char*)apr_table_get(upload_part_resp_headers, "ETag"));
        cos_str_set(&complete_content->etag, etag);
        cos_list_add_tail(&complete_content->node, &complete_part_list);
        cos_pool_destroy(subpool);
        if (end_pos >= fb->file_last) {
            break;
        }
        start_pos += part_size;
        end_pos += part_size;
        if (end_pos > fb->file_last)
            end_pos = fb->file_last;
        part_num += 1;
    }

    //complete multipart
    cos_pool_create(&subpool, parent_pool);
    options->pool = subpool;

    headers = cos_table_create_if_null(options, headers, 0);

    s = cos_complete_multipart_upload(options, bucket, object, upload_id,
            &complete_part_list, headers, &complete_resp_headers);
    ret = cos_status_dup(parent_pool, s);
    cos_pool_destroy(subpool);
    options->pool = parent_pool;
    return ret;
}

cos_status_t *cos_upload_object_by_part_copy
(
        cos_request_options_t *options,
        const cos_string_t *copy_source,
        const cos_string_t *dest_bucket,
        const cos_string_t *dest_object,
        int64_t part_size
)
{
    cos_pool_t *subpool = NULL;
    cos_pool_t *parent_pool = NULL;
    cos_status_t *s = NULL;
    cos_status_t *ret = NULL;
    char *part_num_str = NULL;
    char *etag = NULL;
    cos_list_t complete_part_list;
    cos_complete_part_content_t *complete_content = NULL;
    int64_t total_size = 0;

    cos_list_init(&complete_part_list);
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

    //set part copy param
    cos_upload_part_copy_params_t *upload_part_copy_params = cos_create_upload_part_copy_params(parent_pool);
    cos_str_set(&upload_part_copy_params->copy_source, copy_source->data);
    cos_str_set(&upload_part_copy_params->dest_bucket, dest_bucket->data);
    cos_str_set(&upload_part_copy_params->dest_object, dest_object->data);
    upload_part_copy_params->range_start = 0;
    upload_part_copy_params->range_end = 0;
    upload_part_copy_params->part_num = 1;

    //get upload_id
    cos_table_t *init_multipart_headers = NULL;
    cos_table_t *init_multipart_resp_headers = NULL;
    cos_string_t upload_id;
    init_multipart_headers = cos_table_make(subpool, 0);
    s = cos_init_multipart_upload(options, dest_bucket, dest_object,
            &upload_id, init_multipart_headers, &init_multipart_resp_headers);
    if (!cos_status_is_ok(s)) {
        ret = cos_status_dup(parent_pool, s);
        cos_pool_destroy(subpool);
        options->pool = parent_pool;
        return ret;
    }
    cos_str_set(&upload_part_copy_params->upload_id, apr_pstrdup(parent_pool, upload_id.data));
    cos_pool_destroy(subpool);

    //upload part by copy
    while (1) {
        cos_pool_create(&subpool, parent_pool);
        options->pool = subpool;
        upload_part_copy_params->range_end = cos_min(upload_part_copy_params->range_start + part_size - 1, total_size - 1);
        s = cos_upload_part_copy(options, upload_part_copy_params, NULL, NULL);
        if (!cos_status_is_ok(s)) {
            ret = cos_status_dup(parent_pool, s);
            cos_pool_destroy(subpool);
            options->pool = parent_pool;
            return ret;
        }
        complete_content = cos_create_complete_part_content(parent_pool);
        part_num_str = apr_psprintf(parent_pool, "%d", upload_part_copy_params->part_num);
        cos_str_set(&complete_content->part_number, part_num_str);
        etag = apr_pstrdup(parent_pool, upload_part_copy_params->rsp_content->etag.data);
        cos_str_set(&complete_content->etag, etag);
        cos_list_add_tail(&complete_content->node, &complete_part_list);
        cos_pool_destroy(subpool);
        if (upload_part_copy_params->range_end + 1 >= total_size) {
            break;
        }
        upload_part_copy_params->range_start += part_size;
        upload_part_copy_params->part_num++;
    }

    //complete multipart
    cos_pool_create(&subpool, parent_pool);
    options->pool = subpool;
    s = cos_complete_multipart_upload(options, dest_bucket, dest_object, &upload_part_copy_params->upload_id,
            &complete_part_list, NULL, NULL);
    ret = cos_status_dup(parent_pool, s);
    cos_pool_destroy(subpool);
    options->pool = parent_pool;
    return ret;
}

cos_status_t *cos_download_part_to_file(const cos_request_options_t *options,
                                        const cos_string_t *bucket,
                                        const cos_string_t *object,
                                        cos_upload_file_t *download_file,
                                        cos_table_t **resp_headers)
{
    return cos_do_download_part_to_file(options, bucket, object,
                                        download_file, NULL, NULL, NULL, resp_headers);
}

cos_status_t *cos_do_download_part_to_file(const cos_request_options_t *options,
                                           const cos_string_t *bucket,
                                           const cos_string_t *object,
                                           cos_upload_file_t *download_file,
                                           cos_progress_callback progress_callback,
                                           cos_table_t *headers,
                                           cos_table_t *params,
                                           cos_table_t **resp_headers)
{
    cos_status_t *s = NULL;
    cos_http_request_t *req = NULL;
    cos_http_response_t *resp = NULL;
    int res = COSE_OK;
    char range_buf[64];

    headers = cos_table_create_if_null(options, headers, 1);
    params = cos_table_create_if_null(options, params, 0);
    apr_snprintf(range_buf, sizeof(range_buf), "bytes=%"APR_INT64_T_FMT"-%"APR_INT64_T_FMT, download_file->file_pos, download_file->file_last-1);
    apr_table_add(headers, COS_RANGE, range_buf);

    cos_init_object_request(options, bucket, object, HTTP_GET,
                            &req, params, headers, progress_callback, 0, &resp);

    s = cos_status_create(options->pool);
    res = cos_init_read_response_body_to_file_part(options->pool, download_file, resp);
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

    return s;
}
