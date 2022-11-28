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

#ifndef LIBCOS_RESUMABLE_H
#define LIBCOS_RESUMABLE_H

#include "cos_sys_define.h"
#include "apr_atomic.h"
#include "apr_queue.h"
#include "apr_thread_pool.h"

COS_CPP_START

#define COS_CP_UPLOAD   1
#define COS_CP_DOWNLOAD 2

typedef struct {
    int32_t index;  // the index of part, start from 0
    int64_t offset; // the offset point of part
    int64_t size;   // the size of part
    int completed;  // COS_TRUE completed, COS_FALSE uncompleted
    cos_string_t etag; // the etag of part, for upload
} cos_checkpoint_part_t;

typedef struct {
    cos_string_t md5;      // the md5 of checkout content
    int cp_type;           // 1 upload, 2 download
    apr_file_t *thefile;   // the handle of checkpoint file

    cos_string_t file_path;        // local file path
    int64_t    file_size;          // local file size, for upload
    apr_time_t file_last_modified; // local file last modified time, for upload
    cos_string_t file_md5;         // the md5 of the local file content, for upload, reserved

    cos_string_t object_name;          // object name
    int64_t object_size;               // object size, for download
    cos_string_t object_last_modified; // object last modified time, for download
    cos_string_t object_etag;          // object etag, for download

    cos_string_t upload_id;  // upload id

    int  part_num;                 // the total number of parts
    int64_t part_size;             // the part size, byte
    cos_checkpoint_part_t *parts;  // the parts of local or object, from 0
} cos_checkpoint_t;

typedef struct {
    cos_checkpoint_part_t *part;
    cos_status_t *s;
    cos_string_t etag;
} cos_part_task_result_t;

typedef struct {
    cos_request_options_t options;
    cos_string_t *bucket;
    cos_string_t *object;
    cos_string_t *upload_id;
    cos_string_t *filepath;
    cos_checkpoint_part_t *part;
    cos_part_task_result_t *result;

    apr_uint32_t *launched;        // the number of launched part tasks, use atomic
    apr_uint32_t *failed;          // the number of failed part tasks, use atomic
    apr_uint32_t *completed;       // the number of completed part tasks, use atomic
    apr_queue_t  *failed_parts;    // the queue of failed parts tasks, thread safe
    apr_queue_t  *completed_parts; // the queue of completed parts tasks, thread safe
} cos_upload_thread_params_t;

typedef struct {
    cos_request_options_t options;
    cos_string_t *bucket;
    cos_string_t *object;
    cos_string_t *upload_id;
    cos_string_t *copy_source;
    cos_checkpoint_part_t *part;
    cos_part_task_result_t *result;

    apr_uint32_t *launched;        // the number of launched part tasks, use atomic
    apr_uint32_t *failed;          // the number of failed part tasks, use atomic
    apr_uint32_t *completed;       // the number of completed part tasks, use atomic
    apr_queue_t  *failed_parts;    // the queue of failed parts tasks, thread safe
    apr_queue_t  *completed_parts; // the queue of completed parts tasks, thread safe
} cos_upload_copy_thread_params_t;


typedef cos_upload_thread_params_t cos_transport_thread_params_t;

int32_t cos_get_thread_num(cos_resumable_clt_params_t *clt_params);

void cos_get_checkpoint_path(cos_resumable_clt_params_t *clt_params, const cos_string_t *filepath,
                             cos_pool_t *pool, cos_string_t *checkpoint_path);

int cos_get_file_info(const cos_string_t *filepath, cos_pool_t *pool, apr_finfo_t *finfo);

int cos_does_file_exist(const cos_string_t *filepath, cos_pool_t *pool);

int cos_open_checkpoint_file(cos_pool_t *pool,  cos_string_t *checkpoint_path, cos_checkpoint_t *checkpoint);

int cos_open_checkpoint_file(cos_pool_t *pool,  cos_string_t *checkpoint_path, cos_checkpoint_t *checkpoint);

int cos_get_part_num(int64_t file_size, int64_t part_size);

void cos_build_parts(int64_t file_size, int64_t part_size, cos_checkpoint_part_t *parts);

void cos_build_thread_params(cos_transport_thread_params_t *thr_params, int part_num,
                             cos_pool_t *parent_pool, cos_request_options_t *options,
                             cos_string_t *bucket, cos_string_t *object, cos_string_t *filepath,
                             cos_string_t *upload_id, cos_checkpoint_part_t *parts,
                             cos_part_task_result_t *result);

void cos_build_copy_thread_params(cos_upload_copy_thread_params_t *thr_params, int part_num,
                             cos_pool_t *parent_pool, cos_request_options_t *options,
                             cos_string_t *bucket, cos_string_t *object, cos_string_t *copy_source,
                             cos_string_t *upload_id, cos_checkpoint_part_t *parts,
                             cos_part_task_result_t *result);


void cos_destroy_thread_pool(cos_transport_thread_params_t *thr_params, int part_num);

void cos_set_task_tracker(cos_transport_thread_params_t *thr_params, int part_num,
                          apr_uint32_t *launched, apr_uint32_t *failed, apr_uint32_t *completed,
                          apr_queue_t *failed_parts, apr_queue_t *completed_parts);

int cos_verify_checkpoint_md5(cos_pool_t *pool, const cos_checkpoint_t *checkpoint);

void cos_build_upload_checkpoint(cos_pool_t *pool, cos_checkpoint_t *checkpoint, cos_string_t *file_path,
                                 apr_finfo_t *finfo, cos_string_t *upload_id, int64_t part_size);

int cos_dump_checkpoint(cos_pool_t *pool, const cos_checkpoint_t *checkpoint);

int cos_load_checkpoint(cos_pool_t *pool, const cos_string_t *filepath, cos_checkpoint_t *checkpoint);

int cos_is_upload_checkpoint_valid(cos_pool_t *pool, cos_checkpoint_t *checkpoint, apr_finfo_t *finfo);

void cos_update_checkpoint(cos_pool_t *pool, cos_checkpoint_t *checkpoint, int32_t part_index, cos_string_t *etag);

void cos_get_checkpoint_undo_parts(cos_checkpoint_t *checkpoint, int *part_num, cos_checkpoint_part_t *parts);

void * APR_THREAD_FUNC upload_part(apr_thread_t *thd, void *data);

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
                                                   cos_list_t *resp_body);

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
                                                cos_list_t *resp_body);

void * APR_THREAD_FUNC upload_part_copy(apr_thread_t *thd, void *data);

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
);

void * APR_THREAD_FUNC download_part(apr_thread_t *thd, void *data);

int64_t cos_get_safe_size_for_download(int64_t part_size);

cos_status_t *cos_resumable_download_file_without_cp(cos_request_options_t *options,
                                                   cos_string_t *bucket,
                                                   cos_string_t *object,
                                                   cos_string_t *filepath,
                                                   cos_table_t *headers,
                                                   cos_table_t *params,
                                                   int32_t thread_num,
                                                   int64_t part_size,
                                                   cos_progress_callback progress_callback);



COS_CPP_END

#endif
