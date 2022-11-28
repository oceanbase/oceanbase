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
#include "cos_resumable.h"

int32_t cos_get_thread_num(cos_resumable_clt_params_t *clt_params)
{
    if ((NULL == clt_params) || (clt_params->thread_num <= 0 || clt_params->thread_num > 1024)) {
        return 1;
    }
    return clt_params->thread_num;
}

void cos_get_checkpoint_path(cos_resumable_clt_params_t *clt_params, const cos_string_t *filepath,
                             cos_pool_t *pool, cos_string_t *checkpoint_path)
{
    if ((NULL == checkpoint_path) || (NULL == clt_params) || (!clt_params->enable_checkpoint)) {
        return;
    }

    if (cos_is_null_string(&clt_params->checkpoint_path)) {
        int len = filepath->len + strlen(".cp") + 1;
        char *buffer = (char *)cos_pcalloc(pool, len);
        apr_snprintf(buffer, len, "%.*s.cp", filepath->len, filepath->data);
        cos_str_set(checkpoint_path , buffer);
        return;
    }

    checkpoint_path->data = clt_params->checkpoint_path.data;
    checkpoint_path->len = clt_params->checkpoint_path.len;
}

int cos_get_file_info(const cos_string_t *filepath, cos_pool_t *pool, apr_finfo_t *finfo)
{
    apr_status_t s;
    char buf[256];
    apr_file_t *thefile;

    s = apr_file_open(&thefile, filepath->data, APR_READ, APR_UREAD | APR_GREAD, pool);
    if (s != APR_SUCCESS) {
        cos_error_log("apr_file_open failure, code:%d %s.", s, apr_strerror(s, buf, sizeof(buf)));
        return s;
    }

    s = apr_file_info_get(finfo, APR_FINFO_NORM, thefile);
    if (s != APR_SUCCESS) {
        apr_file_close(thefile);
        cos_error_log("apr_file_info_get failure, code:%d %s.", s, apr_strerror(s, buf, sizeof(buf)));
        return s;
    }
    apr_file_close(thefile);

    return COSE_OK;
}

int cos_does_file_exist(const cos_string_t *filepath, cos_pool_t *pool)
{
    apr_status_t s;
    apr_file_t *thefile;

    s = apr_file_open(&thefile, filepath->data, APR_READ, APR_UREAD | APR_GREAD, pool);
    if (s != APR_SUCCESS) {
        return COS_FALSE;
    }

    apr_file_close(thefile);
    return COS_TRUE;
}

int cos_open_checkpoint_file(cos_pool_t *pool,  cos_string_t *checkpoint_path, cos_checkpoint_t *checkpoint)
{
    apr_status_t s;
    apr_file_t *thefile;
    char buf[256];
    s = apr_file_open(&thefile, checkpoint_path->data, APR_CREATE | APR_WRITE, APR_UREAD | APR_UWRITE | APR_GREAD, pool);
    if (s == APR_SUCCESS) {
        cos_error_log("apr_file_info_get failure, code:%d %s.", s, apr_strerror(s, buf, sizeof(buf)));
        checkpoint->thefile = thefile;
    }
    return s;
}

int cos_get_part_num(int64_t file_size, int64_t part_size)
{
    int64_t num = 0;
    int64_t left = 0;
    left = (file_size % part_size == 0) ? 0 : 1;
    num = file_size / part_size + left;
    return (int)num;
}

void cos_build_parts(int64_t file_size, int64_t part_size, cos_checkpoint_part_t *parts)
{
    int i = 0;
    for (; i * part_size < file_size; i++) {
        parts[i].index = i;
        parts[i].offset = i * part_size;
        parts[i].size = cos_min(part_size, (file_size - i * part_size));
        parts[i].completed = COS_FALSE;
    }
}

void cos_build_thread_params(cos_transport_thread_params_t *thr_params, int part_num,
                             cos_pool_t *parent_pool, cos_request_options_t *options,
                             cos_string_t *bucket, cos_string_t *object, cos_string_t *filepath,
                             cos_string_t *upload_id, cos_checkpoint_part_t *parts,
                             cos_part_task_result_t *result)
{
    int i = 0;
    cos_pool_t *subpool = NULL;
    cos_config_t *config = NULL;
    cos_http_controller_t *ctl;
    for (; i < part_num; i++) {
        cos_pool_create(&subpool, parent_pool);
        config = cos_config_create(subpool);
        cos_str_set(&config->endpoint, options->config->endpoint.data);
        cos_str_set(&config->access_key_id, options->config->access_key_id.data);
        cos_str_set(&config->access_key_secret, options->config->access_key_secret.data);
        cos_str_set(&config->appid, options->config->appid.data);
        if (options->config->sts_token.data != NULL) {
            cos_str_set(&config->sts_token, options->config->sts_token.data);
        }
        config->is_cname = options->config->is_cname;
        ctl = cos_http_controller_create(subpool, 0);
        thr_params[i].options.config = config;
        thr_params[i].options.ctl = ctl;
        thr_params[i].options.pool = subpool;
        thr_params[i].bucket = bucket;
        thr_params[i].object = object;
        thr_params[i].filepath = filepath;
        thr_params[i].upload_id = upload_id;
        thr_params[i].part = parts + i;
        thr_params[i].result = result + i;
        thr_params[i].result->part = thr_params[i].part;
    }
}

void cos_build_copy_thread_params(cos_upload_copy_thread_params_t *thr_params, int part_num,
                             cos_pool_t *parent_pool, cos_request_options_t *options,
                             cos_string_t *bucket, cos_string_t *object, cos_string_t *copy_source,
                             cos_string_t *upload_id, cos_checkpoint_part_t *parts,
                             cos_part_task_result_t *result)
{
    int i = 0;
    cos_pool_t *subpool = NULL;
    cos_config_t *config = NULL;
    cos_http_controller_t *ctl;
    for (; i < part_num; i++) {
        cos_pool_create(&subpool, parent_pool);
        config = cos_config_create(subpool);
        cos_str_set(&config->endpoint, options->config->endpoint.data);
        cos_str_set(&config->access_key_id, options->config->access_key_id.data);
        cos_str_set(&config->access_key_secret, options->config->access_key_secret.data);
        cos_str_set(&config->appid, options->config->appid.data);
        config->is_cname = options->config->is_cname;
        ctl = cos_http_controller_create(subpool, 0);
        thr_params[i].options.config = config;
        thr_params[i].options.ctl = ctl;
        thr_params[i].options.pool = subpool;
        thr_params[i].bucket = bucket;
        thr_params[i].object = object;
        thr_params[i].copy_source = copy_source;
        thr_params[i].upload_id = upload_id;
        thr_params[i].part = parts + i;
        thr_params[i].result = result + i;
        thr_params[i].result->part = thr_params[i].part;
    }
}


void cos_destroy_thread_pool(cos_transport_thread_params_t *thr_params, int part_num)
{
    int i = 0;
    for (; i < part_num; i++) {
        cos_pool_destroy(thr_params[i].options.pool);
    }
}

void cos_destroy_copy_thread_pool(cos_upload_copy_thread_params_t *thr_params, int part_num)
{
    int i = 0;
    for (; i < part_num; i++) {
        cos_pool_destroy(thr_params[i].options.pool);
    }
}

void cos_set_task_tracker(cos_transport_thread_params_t *thr_params, int part_num,
                          apr_uint32_t *launched, apr_uint32_t *failed, apr_uint32_t *completed,
                          apr_queue_t *failed_parts, apr_queue_t *completed_parts)
{
    int i = 0;
    for (; i < part_num; i++) {
        thr_params[i].launched = launched;
        thr_params[i].failed = failed;
        thr_params[i].completed = completed;
        thr_params[i].failed_parts = failed_parts;
        thr_params[i].completed_parts = completed_parts;
    }
}

void cos_set_copy_task_tracker(cos_upload_copy_thread_params_t *thr_params, int part_num,
                          apr_uint32_t *launched, apr_uint32_t *failed, apr_uint32_t *completed,
                          apr_queue_t *failed_parts, apr_queue_t *completed_parts)
{
    int i = 0;
    for (; i < part_num; i++) {
        thr_params[i].launched = launched;
        thr_params[i].failed = failed;
        thr_params[i].completed = completed;
        thr_params[i].failed_parts = failed_parts;
        thr_params[i].completed_parts = completed_parts;
    }
}

int cos_verify_checkpoint_md5(cos_pool_t *pool, const cos_checkpoint_t *checkpoint)
{
    return COS_TRUE;
}

void cos_build_upload_checkpoint(cos_pool_t *pool, cos_checkpoint_t *checkpoint, cos_string_t *file_path,
                                 apr_finfo_t *finfo, cos_string_t *upload_id, int64_t part_size)
{
    int i = 0;

    checkpoint->cp_type = COS_CP_UPLOAD;
    cos_str_set(&checkpoint->file_path, cos_pstrdup(pool, file_path));
    checkpoint->file_size = finfo->size;
    checkpoint->file_last_modified = finfo->mtime;
    cos_str_set(&checkpoint->upload_id, cos_pstrdup(pool, upload_id));

    checkpoint->part_size = part_size;
    for (; i * part_size < finfo->size; i++) {
        checkpoint->parts[i].index = i;
        checkpoint->parts[i].offset = i * part_size;
        checkpoint->parts[i].size = cos_min(part_size, (finfo->size - i * part_size));
        checkpoint->parts[i].completed = COS_FALSE;
        cos_str_set(&checkpoint->parts[i].etag , "");
    }
    checkpoint->part_num = i;
}

int cos_dump_checkpoint(cos_pool_t *parent_pool, const cos_checkpoint_t *checkpoint)
{
    char *xml_body = NULL;
    apr_status_t s;
    char buf[256];
    apr_size_t len;
    cos_pool_t *pool;

    cos_pool_create(&pool, parent_pool);
    // to xml
    xml_body = cos_build_checkpoint_xml(pool, checkpoint);
    if (NULL == xml_body) {
        cos_pool_destroy(pool);
        return COSE_OUT_MEMORY;
    }

    // truncate to empty
    s = apr_file_trunc(checkpoint->thefile, 0);
    if (s != APR_SUCCESS) {
        cos_error_log("apr_file_write fialure, code:%d %s.", s, apr_strerror(s, buf, sizeof(buf)));
        cos_pool_destroy(pool);
        return COSE_FILE_TRUNC_ERROR;
    }

    // write to file
    len = strlen(xml_body);
    s = apr_file_write(checkpoint->thefile, xml_body, &len);
    if (s != APR_SUCCESS) {
        cos_error_log("apr_file_write fialure, code:%d %s.", s, apr_strerror(s, buf, sizeof(buf)));
        cos_pool_destroy(pool);
        return COSE_FILE_WRITE_ERROR;
    }

    // flush file
    s = apr_file_flush(checkpoint->thefile);
    if (s != APR_SUCCESS) {
        cos_error_log("apr_file_flush fialure, code:%d %s.", s, apr_strerror(s, buf, sizeof(buf)));
        cos_pool_destroy(pool);
        return COSE_FILE_FLUSH_ERROR;
    }

    cos_pool_destroy(pool);
    return COSE_OK;
}

int cos_load_checkpoint(cos_pool_t *pool, const cos_string_t *filepath, cos_checkpoint_t *checkpoint)
{
    apr_status_t s;
    char buf[256];
    apr_size_t len;
    apr_finfo_t finfo;
    char *xml_body = NULL;
    apr_file_t *thefile;

    // open file
    s = apr_file_open(&thefile, filepath->data, APR_READ, APR_UREAD | APR_GREAD, pool);
    if (s != APR_SUCCESS) {
        cos_error_log("apr_file_open failure, code:%d %s.", s, apr_strerror(s, buf, sizeof(buf)));
        return COSE_OPEN_FILE_ERROR;
    }

    // get file stat
    s = apr_file_info_get(&finfo, APR_FINFO_NORM, thefile);
    if (s != APR_SUCCESS) {
        cos_error_log("apr_file_info_get failure, code:%d %s.", s, apr_strerror(s, buf, sizeof(buf)));
        apr_file_close(thefile);
        return COSE_FILE_INFO_ERROR;
    }

    xml_body = (char *)cos_palloc(pool, (apr_size_t)(finfo.size + 1));

    // read
    s = apr_file_read_full(thefile, xml_body, (apr_size_t)finfo.size, &len);
    if (s != APR_SUCCESS) {
        cos_error_log("apr_file_read_full fialure, code:%d %s.", s, apr_strerror(s, buf, sizeof(buf)));
        apr_file_close(thefile);
        return COSE_FILE_READ_ERROR;
    }
    apr_file_close(thefile);
    xml_body[len] = '\0';

    // parse
    return cos_checkpoint_parse_from_body(pool, xml_body, checkpoint);
}

int cos_is_upload_checkpoint_valid(cos_pool_t *pool, cos_checkpoint_t *checkpoint, apr_finfo_t *finfo)
{
    if (cos_verify_checkpoint_md5(pool, checkpoint) &&
        (checkpoint->file_size == finfo->size) &&
        (checkpoint->file_last_modified == finfo->mtime)) {
        return COS_TRUE;
    }
    return COS_FALSE;
}

void cos_update_checkpoint(cos_pool_t *pool, cos_checkpoint_t *checkpoint, int32_t part_index, cos_string_t *etag)
{
    char *p = NULL;
    checkpoint->parts[part_index].completed = COS_TRUE;
    p = apr_pstrdup(pool, etag->data);
    cos_str_set(&checkpoint->parts[part_index].etag, p);
}

void cos_get_checkpoint_undo_parts(cos_checkpoint_t *checkpoint, int *part_num, cos_checkpoint_part_t *parts)
{
    int i = 0;
    int idx = 0;
    for (; i < checkpoint->part_num; i++) {
        if (!checkpoint->parts[i].completed) {
            parts[idx].index = checkpoint->parts[i].index;
            parts[idx].offset = checkpoint->parts[i].offset;
            parts[idx].size = checkpoint->parts[i].size;
            parts[idx].completed = checkpoint->parts[i].completed;
            idx++;
        }
    }
    *part_num = idx;
}

void * APR_THREAD_FUNC upload_part(apr_thread_t *thd, void *data)
{
    cos_status_t *s = NULL;
    cos_upload_thread_params_t *params = NULL;
    cos_upload_file_t *upload_file = NULL;
    cos_table_t *resp_headers = NULL;
    int part_num;
    char *etag;

    params = (cos_upload_thread_params_t *)data;
    if (apr_atomic_read32(params->failed) > 0) {
        apr_atomic_inc32(params->launched);
        return NULL;
    }

    part_num = params->part->index + 1;
    upload_file = cos_create_upload_file(params->options.pool);
    cos_str_set(&upload_file->filename, params->filepath->data);
    upload_file->file_pos = params->part->offset;
    upload_file->file_last = params->part->offset + params->part->size;

    s = cos_upload_part_from_file(&params->options, params->bucket, params->object, params->upload_id,
        part_num, upload_file, &resp_headers);
    if (!cos_status_is_ok(s)) {
        apr_atomic_inc32(params->failed);
        params->result->s = s;
        apr_queue_push(params->failed_parts, params->result);
        return s;
    }

    etag = apr_pstrdup(params->options.pool, (char*)apr_table_get(resp_headers, "ETag"));
    cos_str_set(&params->result->etag, etag);
    apr_atomic_inc32(params->completed);
    apr_queue_push(params->completed_parts, params->result);
    return NULL;
}

cos_status_t *cos_resumable_upload_file_without_cp(cos_request_options_t *options,
                                                   cos_string_t *bucket,
                                                   cos_string_t *object,
                                                   cos_string_t *filepath,
                                                   cos_table_t *headers,
                                                   cos_table_t *params,
                                                   int32_t thread_num,
                                                   int64_t part_size,
                                                   apr_finfo_t *finfo,
                                                   cos_progress_callback progress_callback,
                                                   cos_table_t **resp_headers,
                                                   cos_list_t *resp_body)
{
    cos_pool_t *subpool = NULL;
    cos_pool_t *parent_pool = NULL;
    cos_status_t *s = NULL;
    cos_status_t *ret = NULL;
    cos_list_t completed_part_list;
    cos_complete_part_content_t *complete_content = NULL;
    cos_string_t upload_id;
    cos_checkpoint_part_t *parts;
    cos_part_task_result_t *results;
    cos_part_task_result_t *task_res;
    cos_upload_thread_params_t *thr_params;
    cos_table_t *cb_headers = NULL;
    apr_thread_pool_t *thrp;
    apr_uint32_t launched = 0;
    apr_uint32_t failed = 0;
    apr_uint32_t completed = 0;
    apr_uint32_t total_num = 0;
    apr_queue_t *failed_parts;
    apr_queue_t *completed_parts;
    int64_t consume_bytes = 0;
    void *task_result;
    char *part_num_str;
    char *etag;
    int part_num = 0;
    int i = 0;
    int rv;

    // prepare
    parent_pool = options->pool;
    ret = cos_status_create(parent_pool);
    part_num = cos_get_part_num(finfo->size, part_size);
    parts = (cos_checkpoint_part_t *)cos_palloc(parent_pool, sizeof(cos_checkpoint_part_t) * part_num);
    cos_build_parts(finfo->size, part_size, parts);
    results = (cos_part_task_result_t *)cos_palloc(parent_pool, sizeof(cos_part_task_result_t) * part_num);
    thr_params = (cos_upload_thread_params_t *)cos_palloc(parent_pool, sizeof(cos_upload_thread_params_t) * part_num);
    cos_build_thread_params(thr_params, part_num, parent_pool, options, bucket, object, filepath, &upload_id, parts, results);

    // init upload
    cos_pool_create(&subpool, parent_pool);
    options->pool = subpool;
    s = cos_init_multipart_upload(options, bucket, object, &upload_id, headers, resp_headers);
    if (!cos_status_is_ok(s)) {
        s = cos_status_dup(parent_pool, s);
        cos_pool_destroy(subpool);
        options->pool = parent_pool;
        return s;
    }
    cos_str_set(&upload_id, apr_pstrdup(parent_pool, upload_id.data));
    options->pool = parent_pool;
    cos_pool_destroy(subpool);

    // upload parts
    rv = apr_thread_pool_create(&thrp, 0, thread_num, parent_pool);
    if (APR_SUCCESS != rv) {
        cos_status_set(ret, rv, COS_CREATE_THREAD_POOL_ERROR_CODE, NULL);
        return ret;
    }

    rv = apr_queue_create(&failed_parts, part_num, parent_pool);
    if (APR_SUCCESS != rv) {
        cos_status_set(ret, rv, COS_CREATE_QUEUE_ERROR_CODE, NULL);
        return ret;
    }

    rv = apr_queue_create(&completed_parts, part_num, parent_pool);
    if (APR_SUCCESS != rv) {
        cos_status_set(ret, rv, COS_CREATE_QUEUE_ERROR_CODE, NULL);
        return ret;
    }

    // launch
    cos_set_task_tracker(thr_params, part_num, &launched, &failed, &completed, failed_parts, completed_parts);
    for (i = 0; i < part_num; i++) {
        apr_thread_pool_push(thrp, upload_part, thr_params + i, 0, NULL);
    }

    // wait until all tasks exit
    total_num = apr_atomic_read32(&launched) + apr_atomic_read32(&failed) + apr_atomic_read32(&completed);
    for ( ; total_num < (apr_uint32_t)part_num; ) {
        rv = apr_queue_trypop(completed_parts, &task_result);
        if (rv == APR_EINTR || rv == APR_EAGAIN) {
            apr_sleep(1000);
        } else if(rv == APR_EOF) {
            break;
        } else if(rv == APR_SUCCESS) {
            task_res = (cos_part_task_result_t*)task_result;
            if (NULL != progress_callback) {
                consume_bytes += task_res->part->size;
                progress_callback(consume_bytes, finfo->size);
            }
        }
        total_num = apr_atomic_read32(&launched) + apr_atomic_read32(&failed) + apr_atomic_read32(&completed);
    }

    // deal with left successful parts
    while(APR_SUCCESS == apr_queue_trypop(completed_parts, &task_result)) {
        task_res = (cos_part_task_result_t*)task_result;
        if (NULL != progress_callback) {
            consume_bytes += task_res->part->size;
            progress_callback(consume_bytes, finfo->size);
        }
    }

    // failed
    if (apr_atomic_read32(&failed) > 0) {
        apr_queue_pop(failed_parts, &task_result);
        task_res = (cos_part_task_result_t*)task_result;
        s = cos_status_dup(parent_pool, task_res->s);
        cos_destroy_thread_pool(thr_params, part_num);
        return s;
    }

    // successful
    cos_pool_create(&subpool, parent_pool);
    cos_list_init(&completed_part_list);
    for (i = 0; i < part_num; i++) {
        complete_content = cos_create_complete_part_content(subpool);
        part_num_str = apr_psprintf(subpool, "%d", thr_params[i].part->index + 1);
        cos_str_set(&complete_content->part_number, part_num_str);
        etag = apr_pstrdup(subpool, thr_params[i].result->etag.data);
        cos_str_set(&complete_content->etag, etag);
        cos_list_add_tail(&complete_content->node, &completed_part_list);
    }
    cos_destroy_thread_pool(thr_params, part_num);

    // complete upload
    options->pool = subpool;
    if (NULL != headers && NULL != apr_table_get(headers, COS_CALLBACK)) {
        cb_headers = cos_table_make(subpool, 2);
        apr_table_set(cb_headers, COS_CALLBACK, apr_table_get(headers, COS_CALLBACK));
        if (NULL != apr_table_get(headers, COS_CALLBACK_VAR)) {
            apr_table_set(cb_headers, COS_CALLBACK_VAR, apr_table_get(headers, COS_CALLBACK_VAR));
        }
    }
    s = cos_do_complete_multipart_upload(options, bucket, object, &upload_id,
        &completed_part_list, cb_headers, NULL, resp_headers, resp_body);
    s = cos_status_dup(parent_pool, s);
    cos_pool_destroy(subpool);
    options->pool = parent_pool;

    return s;
}

cos_status_t *cos_resumable_upload_file_with_cp(cos_request_options_t *options,
                                                cos_string_t *bucket,
                                                cos_string_t *object,
                                                cos_string_t *filepath,
                                                cos_table_t *headers,
                                                cos_table_t *params,
                                                int32_t thread_num,
                                                int64_t part_size,
                                                cos_string_t *checkpoint_path,
                                                apr_finfo_t *finfo,
                                                cos_progress_callback progress_callback,
                                                cos_table_t **resp_headers,
                                                cos_list_t *resp_body)
{
    cos_pool_t *subpool = NULL;
    cos_pool_t *parent_pool = NULL;
    cos_status_t *s = NULL;
    cos_status_t *ret = NULL;
    cos_list_t completed_part_list;
    cos_complete_part_content_t *complete_content = NULL;
    cos_string_t upload_id;
    cos_checkpoint_part_t *parts;
    cos_part_task_result_t *results;
    cos_part_task_result_t *task_res;
    cos_upload_thread_params_t *thr_params;
    cos_table_t *cb_headers = NULL;
    apr_thread_pool_t *thrp;
    apr_uint32_t launched = 0;
    apr_uint32_t failed = 0;
    apr_uint32_t completed = 0;
    apr_uint32_t total_num = 0;
    apr_queue_t *failed_parts;
    apr_queue_t *completed_parts;
    cos_checkpoint_t *checkpoint = NULL;
    int need_init_upload = COS_TRUE;
    int has_left_result = COS_FALSE;
    int64_t consume_bytes = 0;
    void *task_result;
    char *part_num_str;
    int part_num = 0;
    int i = 0;
    int rv;

    // checkpoint
    parent_pool = options->pool;
    ret = cos_status_create(parent_pool);
    checkpoint = cos_create_checkpoint_content(parent_pool);
    if(cos_does_file_exist(checkpoint_path, parent_pool)) {
        if (COSE_OK == cos_load_checkpoint(parent_pool, checkpoint_path, checkpoint) &&
            cos_is_upload_checkpoint_valid(parent_pool, checkpoint, finfo)) {
                cos_str_set(&upload_id, checkpoint->upload_id.data);
                need_init_upload = COS_FALSE;
        } else {
            apr_file_remove(checkpoint_path->data, parent_pool);
        }
    }

    if (need_init_upload) {
        // init upload
        cos_pool_create(&subpool, parent_pool);
        options->pool = subpool;
        s = cos_init_multipart_upload(options, bucket, object, &upload_id, headers, resp_headers);
        if (!cos_status_is_ok(s)) {
            s = cos_status_dup(parent_pool, s);
            cos_pool_destroy(subpool);
            options->pool = parent_pool;
            return s;
        }
        cos_str_set(&upload_id, apr_pstrdup(parent_pool, upload_id.data));
        options->pool = parent_pool;
        cos_pool_destroy(subpool);

        // build checkpoint
        cos_build_upload_checkpoint(parent_pool, checkpoint, filepath, finfo, &upload_id, part_size);
    }

    rv = cos_open_checkpoint_file(parent_pool, checkpoint_path, checkpoint);
    if (rv != APR_SUCCESS) {
        cos_status_set(ret, rv, COS_OPEN_FILE_ERROR_CODE, NULL);
        return ret;
    }

    // prepare
    ret = cos_status_create(parent_pool);
    parts = (cos_checkpoint_part_t *)cos_palloc(parent_pool, sizeof(cos_checkpoint_part_t) * (checkpoint->part_num));
    cos_get_checkpoint_undo_parts(checkpoint, &part_num, parts);
    results = (cos_part_task_result_t *)cos_palloc(parent_pool, sizeof(cos_part_task_result_t) * part_num);
    thr_params = (cos_upload_thread_params_t *)cos_palloc(parent_pool, sizeof(cos_upload_thread_params_t) * part_num);
    cos_build_thread_params(thr_params, part_num, parent_pool, options, bucket, object, filepath, &upload_id, parts, results);

    // upload parts
    rv = apr_thread_pool_create(&thrp, 0, thread_num, parent_pool);
    if (APR_SUCCESS != rv) {
        cos_status_set(ret, rv, COS_CREATE_THREAD_POOL_ERROR_CODE, NULL);
        return ret;
    }

    rv = apr_queue_create(&failed_parts, part_num, parent_pool);
    if (APR_SUCCESS != rv) {
        cos_status_set(ret, rv, COS_CREATE_QUEUE_ERROR_CODE, NULL);
        return ret;
    }

    rv = apr_queue_create(&completed_parts, part_num, parent_pool);
    if (APR_SUCCESS != rv) {
        cos_status_set(ret, rv, COS_CREATE_QUEUE_ERROR_CODE, NULL);
        return ret;
    }

    // launch
    cos_set_task_tracker(thr_params, part_num, &launched, &failed, &completed, failed_parts, completed_parts);
    for (i = 0; i < part_num; i++) {
        apr_thread_pool_push(thrp, upload_part, thr_params + i, 0, NULL);
    }

    // wait until all tasks exit
    total_num = apr_atomic_read32(&launched) + apr_atomic_read32(&failed) + apr_atomic_read32(&completed);
    for ( ; total_num < (apr_uint32_t)part_num; ) {
        rv = apr_queue_trypop(completed_parts, &task_result);
        if (rv == APR_EINTR || rv == APR_EAGAIN) {
            apr_sleep(1000);
        } else if(rv == APR_EOF) {
            break;
        } else if(rv == APR_SUCCESS) {
            task_res = (cos_part_task_result_t*)task_result;
            cos_update_checkpoint(parent_pool, checkpoint, task_res->part->index, &task_res->etag);
            rv = cos_dump_checkpoint(parent_pool, checkpoint);
            if (rv != COSE_OK) {
                int idx = task_res->part->index;
                cos_status_set(ret, rv, COS_WRITE_FILE_ERROR_CODE, NULL);
                apr_atomic_inc32(&failed);
                thr_params[idx].result->s = ret;
                apr_queue_push(failed_parts, thr_params[idx].result);
            }
            if (NULL != progress_callback) {
                consume_bytes += task_res->part->size;
                progress_callback(consume_bytes, finfo->size);
            }
        }
        total_num = apr_atomic_read32(&launched) + apr_atomic_read32(&failed) + apr_atomic_read32(&completed);
    }

    // deal with left successful parts
    while(APR_SUCCESS == apr_queue_trypop(completed_parts, &task_result)) {
        task_res = (cos_part_task_result_t*)task_result;
        cos_update_checkpoint(parent_pool, checkpoint, task_res->part->index, &task_res->etag);
        consume_bytes += task_res->part->size;
        has_left_result = COS_TRUE;
    }
    if (has_left_result) {
        rv = cos_dump_checkpoint(parent_pool, checkpoint);
        if (rv != COSE_OK) {
            cos_status_set(ret, rv, COS_WRITE_FILE_ERROR_CODE, NULL);
            return ret;
        }
        if (NULL != progress_callback) {
            progress_callback(consume_bytes, finfo->size);
        }
    }
    apr_file_close(checkpoint->thefile);

    // failed
    if (apr_atomic_read32(&failed) > 0) {
        apr_queue_pop(failed_parts, &task_result);
        task_res = (cos_part_task_result_t*)task_result;
        s = cos_status_dup(parent_pool, task_res->s);
        cos_destroy_thread_pool(thr_params, part_num);
        return s;
    }

    // successful
    cos_pool_create(&subpool, parent_pool);
    cos_list_init(&completed_part_list);
    for (i = 0; i < checkpoint->part_num; i++) {
        complete_content = cos_create_complete_part_content(subpool);
        part_num_str = apr_psprintf(subpool, "%d", checkpoint->parts[i].index + 1);
        cos_str_set(&complete_content->part_number, part_num_str);
        cos_str_set(&complete_content->etag, checkpoint->parts[i].etag.data);
        cos_list_add_tail(&complete_content->node, &completed_part_list);
    }
    cos_destroy_thread_pool(thr_params, part_num);

    // complete upload
    options->pool = subpool;
    if (NULL != headers && NULL != apr_table_get(headers, COS_CALLBACK)) {
        cb_headers = cos_table_make(subpool, 2);
        apr_table_set(cb_headers, COS_CALLBACK, apr_table_get(headers, COS_CALLBACK));
        if (NULL != apr_table_get(headers, COS_CALLBACK_VAR)) {
            apr_table_set(cb_headers, COS_CALLBACK_VAR, apr_table_get(headers, COS_CALLBACK_VAR));
        }
    }
    s = cos_do_complete_multipart_upload(options, bucket, object, &upload_id,
        &completed_part_list, cb_headers, NULL, resp_headers, resp_body);
    s = cos_status_dup(parent_pool, s);
    cos_pool_destroy(subpool);
    options->pool = parent_pool;

    // remove chepoint file
    apr_file_remove(checkpoint_path->data, parent_pool);

    return s;
}

cos_status_t *cos_resumable_upload_file(cos_request_options_t *options,
                                        cos_string_t *bucket,
                                        cos_string_t *object,
                                        cos_string_t *filepath,
                                        cos_table_t *headers,
                                        cos_table_t *params,
                                        cos_resumable_clt_params_t *clt_params,
                                        cos_progress_callback progress_callback,
                                        cos_table_t **resp_headers,
                                        cos_list_t *resp_body)
{
    int32_t thread_num = 0;
    int64_t part_size = 0;
    cos_string_t checkpoint_path;
    cos_pool_t *sub_pool;
    apr_finfo_t finfo;
    cos_status_t *s;
    int res;

    thread_num = cos_get_thread_num(clt_params);

    cos_pool_create(&sub_pool, options->pool);
    res = cos_get_file_info(filepath, sub_pool, &finfo);
    if (res != COSE_OK) {
        cos_error_log("Open read file fail, filename:%s\n", filepath->data);
        s = cos_status_create(options->pool);
        cos_file_error_status_set(s, res);
        cos_pool_destroy(sub_pool);
        return s;
    }
    part_size = clt_params->part_size;
    cos_get_part_size(finfo.size, &part_size);

    if (NULL != clt_params && clt_params->enable_checkpoint) {
        cos_get_checkpoint_path(clt_params, filepath, sub_pool, &checkpoint_path);
        s = cos_resumable_upload_file_with_cp(options, bucket, object, filepath, headers, params, thread_num,
            part_size, &checkpoint_path, &finfo, progress_callback, resp_headers, resp_body);
    } else {
        s = cos_resumable_upload_file_without_cp(options, bucket, object, filepath, headers, params, thread_num,
            part_size, &finfo, progress_callback, resp_headers, resp_body);
    }

    cos_pool_destroy(sub_pool);
    return s;
}

void * APR_THREAD_FUNC upload_part_copy(apr_thread_t *thd, void *data)
{
    cos_status_t *s = NULL;
    cos_upload_copy_thread_params_t *params = NULL;
    cos_table_t *resp_headers = NULL;
    char *etag;

    params = (cos_upload_copy_thread_params_t *)data;
    if (apr_atomic_read32(params->failed) > 0) {
        apr_atomic_inc32(params->launched);
        return NULL;
    }

    cos_upload_part_copy_params_t *upload_part_copy_params = cos_create_upload_part_copy_params(params->options.pool);
    cos_str_set(&upload_part_copy_params->copy_source, params->copy_source->data);
    cos_str_set(&upload_part_copy_params->dest_bucket, params->bucket->data);
    cos_str_set(&upload_part_copy_params->dest_object, params->object->data);
    cos_str_set(&upload_part_copy_params->upload_id, params->upload_id->data);
    upload_part_copy_params->range_start = params->part->offset;
    upload_part_copy_params->range_end = params->part->offset + params->part->size - 1;
    upload_part_copy_params->part_num = params->part->index + 1;

    s = cos_upload_part_copy(&params->options, upload_part_copy_params, NULL, &resp_headers);
    if (!cos_status_is_ok(s)) {
        apr_atomic_inc32(params->failed);
        params->result->s = s;
        apr_queue_push(params->failed_parts, params->result);
        return s;
    }

    etag = apr_pstrdup(params->options.pool, (char*)upload_part_copy_params->rsp_content->etag.data);
    cos_str_set(&params->result->etag, etag);
    apr_atomic_inc32(params->completed);
    apr_queue_push(params->completed_parts, params->result);
    return NULL;
}

cos_status_t *cos_upload_object_by_part_copy_mt
(
        cos_request_options_t *options,
        cos_string_t *src_bucket,
        cos_string_t *src_object,
        cos_string_t *src_endpoint,
        cos_string_t *dest_bucket,
        cos_string_t *dest_object,
        int64_t part_size,
        int32_t thread_num,
        cos_progress_callback progress_callback
)
{
    cos_pool_t *subpool = NULL;
    cos_pool_t *parent_pool = NULL;
    cos_status_t *s = NULL;
    cos_status_t *ret = NULL;
    char *part_num_str = NULL;
    char *etag = NULL;
    cos_list_t completed_part_list;
    cos_complete_part_content_t *complete_content = NULL;
    int64_t total_size = 0;
    int part_num = 0;
    cos_string_t upload_id;
    cos_checkpoint_part_t *parts;
    cos_part_task_result_t *results;
    cos_part_task_result_t *task_res;
    cos_upload_copy_thread_params_t *thr_params;
    cos_table_t *cb_headers = NULL;
    apr_thread_pool_t *thrp;
    apr_uint32_t launched = 0;
    apr_uint32_t failed = 0;
    apr_uint32_t completed = 0;
    apr_uint32_t total_num = 0;
    apr_queue_t *failed_parts;
    apr_queue_t *completed_parts;
    int64_t consume_bytes = 0;
    void *task_result;
    int i = 0;
    int rv;
    cos_string_t copy_source;
    char *copy_source_str = NULL;

    copy_source_str = apr_psprintf(options->pool, "%.*s.%.*s/%.*s",
                                   src_bucket->len, src_bucket->data,
                                   src_endpoint->len, src_endpoint->data,
                                   src_object->len, src_object->data);
    cos_str_set(&copy_source, copy_source_str);

    cos_list_init(&completed_part_list);
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

    // prepare
    ret = cos_status_create(parent_pool);
    part_num = cos_get_part_num(total_size, part_size);
    parts = (cos_checkpoint_part_t *)cos_palloc(parent_pool, sizeof(cos_checkpoint_part_t) * part_num);
    cos_build_parts(total_size, part_size, parts);
    results = (cos_part_task_result_t *)cos_palloc(parent_pool, sizeof(cos_part_task_result_t) * part_num);
    thr_params = (cos_upload_copy_thread_params_t *)cos_palloc(parent_pool, sizeof(cos_upload_copy_thread_params_t) * part_num);
    cos_build_copy_thread_params(thr_params, part_num, parent_pool, options, dest_bucket, dest_object, &copy_source, &upload_id, parts, results);

    // init upload
    cos_pool_create(&subpool, parent_pool);
    options->pool = subpool;
    cos_table_t *init_multipart_headers = NULL;
    cos_table_t *init_multipart_resp_headers = NULL;
    s = cos_init_multipart_upload(options, dest_bucket, dest_object, &upload_id, init_multipart_headers, &init_multipart_resp_headers);
    if (!cos_status_is_ok(s)) {
        s = cos_status_dup(parent_pool, s);
        cos_pool_destroy(subpool);
        options->pool = parent_pool;
        return s;
    }
    cos_str_set(&upload_id, apr_pstrdup(parent_pool, upload_id.data));
    options->pool = parent_pool;
    cos_pool_destroy(subpool);

    // upload parts
    rv = apr_thread_pool_create(&thrp, 0, thread_num, parent_pool);
    if (APR_SUCCESS != rv) {
        cos_status_set(ret, rv, COS_CREATE_THREAD_POOL_ERROR_CODE, NULL);
        return ret;
    }

    rv = apr_queue_create(&failed_parts, part_num, parent_pool);
    if (APR_SUCCESS != rv) {
        cos_status_set(ret, rv, COS_CREATE_QUEUE_ERROR_CODE, NULL);
        return ret;
    }

    rv = apr_queue_create(&completed_parts, part_num, parent_pool);
    if (APR_SUCCESS != rv) {
        cos_status_set(ret, rv, COS_CREATE_QUEUE_ERROR_CODE, NULL);
        return ret;
    }

    // launch
    cos_set_copy_task_tracker(thr_params, part_num, &launched, &failed, &completed, failed_parts, completed_parts);
    for (i = 0; i < part_num; i++) {
        apr_thread_pool_push(thrp, upload_part_copy, thr_params + i, 0, NULL);
    }

    // wait until all tasks exit
    total_num = apr_atomic_read32(&launched) + apr_atomic_read32(&failed) + apr_atomic_read32(&completed);
    for ( ; total_num < (apr_uint32_t)part_num; ) {
        rv = apr_queue_trypop(completed_parts, &task_result);
        if (rv == APR_EINTR || rv == APR_EAGAIN) {
            apr_sleep(1000);
        } else if(rv == APR_EOF) {
            break;
        } else if(rv == APR_SUCCESS) {
            task_res = (cos_part_task_result_t*)task_result;
            if (NULL != progress_callback) {
                consume_bytes += task_res->part->size;
                progress_callback(consume_bytes, total_size);
            }
        }
        total_num = apr_atomic_read32(&launched) + apr_atomic_read32(&failed) + apr_atomic_read32(&completed);
    }

    // deal with left successful parts
    while(APR_SUCCESS == apr_queue_trypop(completed_parts, &task_result)) {
        task_res = (cos_part_task_result_t*)task_result;
        if (NULL != progress_callback) {
            consume_bytes += task_res->part->size;
            progress_callback(consume_bytes, total_size);
        }
    }

    // failed
    if (apr_atomic_read32(&failed) > 0) {
        apr_queue_pop(failed_parts, &task_result);
        task_res = (cos_part_task_result_t*)task_result;
        s = cos_status_dup(parent_pool, task_res->s);
        cos_destroy_copy_thread_pool(thr_params, part_num);
        return s;
    }

    // successful
    cos_pool_create(&subpool, parent_pool);
    cos_list_init(&completed_part_list);
    for (i = 0; i < part_num; i++) {
        complete_content = cos_create_complete_part_content(subpool);
        part_num_str = apr_psprintf(subpool, "%d", thr_params[i].part->index + 1);
        cos_str_set(&complete_content->part_number, part_num_str);
        etag = apr_pstrdup(subpool, thr_params[i].result->etag.data);
        cos_str_set(&complete_content->etag, etag);
        cos_list_add_tail(&complete_content->node, &completed_part_list);
    }
    cos_destroy_copy_thread_pool(thr_params, part_num);

    // complete upload
    options->pool = subpool;
    s = cos_do_complete_multipart_upload(options, dest_bucket, dest_object, &upload_id,
        &completed_part_list, cb_headers, NULL, NULL, NULL);
    s = cos_status_dup(parent_pool, s);
    cos_pool_destroy(subpool);
    options->pool = parent_pool;

    return s;
}

void * APR_THREAD_FUNC download_part(apr_thread_t *thd, void *data)
{
    cos_status_t *s = NULL;
    cos_upload_thread_params_t *params = NULL;
    cos_upload_file_t *download_file = NULL;
    cos_table_t *resp_headers = NULL;
    int part_num;
    char *etag;

    params = (cos_upload_thread_params_t *)data;
    if (apr_atomic_read32(params->failed) > 0) {
        apr_atomic_inc32(params->launched);
        return NULL;
    }

    part_num = params->part->index + 1;
    download_file = cos_create_upload_file(params->options.pool);
    cos_str_set(&download_file->filename, params->filepath->data);
    download_file->file_pos = params->part->offset;
    download_file->file_last = params->part->offset + params->part->size;

    s = cos_download_part_to_file(&params->options, params->bucket, params->object, download_file, &resp_headers);
    if (!cos_status_is_ok(s)) {
        apr_atomic_inc32(params->failed);
        params->result->s = s;
        apr_queue_push(params->failed_parts, params->result);
        return s;
    }

    cos_warn_log("download part = %d, start byte = %"APR_INT64_T_FMT", end byte = %"APR_INT64_T_FMT, part_num, download_file->file_pos, download_file->file_last-1);

    etag = apr_pstrdup(params->options.pool, (char*)apr_table_get(resp_headers, "ETag"));
    cos_str_set(&params->result->etag, etag);
    apr_atomic_inc32(params->completed);
    apr_queue_push(params->completed_parts, params->result);
    return NULL;
}

int64_t cos_get_safe_size_for_download(int64_t part_size)
{
    if (part_size < 4*1024*1024) return 4*1024*1024;
    else if (part_size > 20*1024*1024) return 20*1024*1024;
    else return part_size;
}

cos_status_t *cos_resumable_download_file_without_cp(cos_request_options_t *options,
                                                   cos_string_t *bucket,
                                                   cos_string_t *object,
                                                   cos_string_t *filepath,
                                                   cos_table_t *headers,
                                                   cos_table_t *params,
                                                   int32_t thread_num,
                                                   int64_t part_size,
                                                   cos_progress_callback progress_callback)
{
    cos_pool_t *subpool = NULL;
    cos_pool_t *parent_pool = NULL;
    cos_status_t *s = NULL;
    cos_status_t *ret = NULL;
    cos_string_t upload_id;
    cos_checkpoint_part_t *parts;
    cos_part_task_result_t *results;
    cos_part_task_result_t *task_res;
    cos_transport_thread_params_t *thr_params;
    apr_thread_pool_t *thrp;
    apr_uint32_t launched = 0;
    apr_uint32_t failed = 0;
    apr_uint32_t completed = 0;
    apr_uint32_t total_num = 0;
    apr_queue_t *failed_parts;
    apr_queue_t *completed_parts;
    int64_t consume_bytes = 0;
    void *task_result;
    int part_num = 0;
    int i = 0;
    int rv;
    const char *value = NULL;
    int64_t file_size = 0;
    cos_table_t *resp_headers = NULL;

    // prepare
    parent_pool = options->pool;
    ret = cos_status_create(parent_pool);
    // get object file size
    cos_pool_create(&subpool, parent_pool);
    options->pool = subpool;
    s = cos_head_object(options, bucket, object, NULL, &resp_headers);
    if (!cos_status_is_ok(s)) {
        s = cos_status_dup(parent_pool, s);
        cos_pool_destroy(subpool);
        options->pool = parent_pool;
        return s;
    }
    value = apr_table_get(resp_headers, COS_CONTENT_LENGTH);
    if (NULL == value) {
        cos_status_set(ret, COSE_INVALID_ARGUMENT, COS_LACK_OF_CONTENT_LEN_ERROR_CODE, NULL);
        cos_pool_destroy(subpool);
        options->pool = parent_pool;
        return ret;
    }
    file_size = cos_atoi64(value);
    cos_pool_destroy(subpool);
    options->pool = parent_pool;
    // init download params
    part_size = cos_get_safe_size_for_download(part_size);
    part_num = cos_get_part_num(file_size, part_size);
    parts = (cos_checkpoint_part_t *)cos_palloc(parent_pool, sizeof(cos_checkpoint_part_t) * part_num);
    cos_build_parts(file_size, part_size, parts);
    results = (cos_part_task_result_t *)cos_palloc(parent_pool, sizeof(cos_part_task_result_t) * part_num);
    thr_params = (cos_transport_thread_params_t *)cos_palloc(parent_pool, sizeof(cos_transport_thread_params_t) * part_num);
    cos_build_thread_params(thr_params, part_num, parent_pool, options, bucket, object, filepath, &upload_id, parts, results);

    // download parts
    rv = apr_thread_pool_create(&thrp, 0, thread_num, parent_pool);
    if (APR_SUCCESS != rv) {
        cos_status_set(ret, rv, COS_CREATE_THREAD_POOL_ERROR_CODE, NULL);
        return ret;
    }

    rv = apr_queue_create(&failed_parts, part_num, parent_pool);
    if (APR_SUCCESS != rv) {
        cos_status_set(ret, rv, COS_CREATE_QUEUE_ERROR_CODE, NULL);
        return ret;
    }

    rv = apr_queue_create(&completed_parts, part_num, parent_pool);
    if (APR_SUCCESS != rv) {
        cos_status_set(ret, rv, COS_CREATE_QUEUE_ERROR_CODE, NULL);
        return ret;
    }

    // launch
    cos_set_task_tracker(thr_params, part_num, &launched, &failed, &completed, failed_parts, completed_parts);
    for (i = 0; i < part_num; i++) {
        apr_thread_pool_push(thrp, download_part, thr_params + i, 0, NULL);
    }

    // wait until all tasks exit
    total_num = apr_atomic_read32(&launched) + apr_atomic_read32(&failed) + apr_atomic_read32(&completed);
    for ( ; total_num < (apr_uint32_t)part_num; ) {
        rv = apr_queue_trypop(completed_parts, &task_result);
        if (rv == APR_EINTR || rv == APR_EAGAIN) {
            apr_sleep(1000);
        } else if(rv == APR_EOF) {
            break;
        } else if(rv == APR_SUCCESS) {
            task_res = (cos_part_task_result_t*)task_result;
            if (NULL != progress_callback) {
                consume_bytes += task_res->part->size;
                progress_callback(consume_bytes, file_size);
            }
        }
        total_num = apr_atomic_read32(&launched) + apr_atomic_read32(&failed) + apr_atomic_read32(&completed);
    }

    // deal with left successful parts
    while(APR_SUCCESS == apr_queue_trypop(completed_parts, &task_result)) {
        task_res = (cos_part_task_result_t*)task_result;
        if (NULL != progress_callback) {
            consume_bytes += task_res->part->size;
            progress_callback(consume_bytes, file_size);
        }
    }

    // failed
    if (apr_atomic_read32(&failed) > 0) {
        apr_queue_pop(failed_parts, &task_result);
        task_res = (cos_part_task_result_t*)task_result;
        s = cos_status_dup(parent_pool, task_res->s);
        cos_destroy_thread_pool(thr_params, part_num);
        return s;
    }

    // successful
    cos_destroy_thread_pool(thr_params, part_num);

    s = cos_status_create(options->pool);
    return s;
}
