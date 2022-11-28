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

#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <new>

#include "cos_api.h"
#include "cos_log.h"
#include "cos_utility.h"
#include "cos_string.h"
#include "cos_status.h"
#include "cos_auth.h"
#include "cos_sys_util.h"
#include "apr_errno.h"

#include "ob_cos_wrapper.h"


namespace oceanbase
{
namespace common
{
namespace qcloud_cos
{


constexpr int OB_SUCCESS                             = 0;
constexpr int OB_INVALID_ARGUMENT                    = -4002;
constexpr int OB_INIT_TWICE                          = -4005;
constexpr int OB_IO_ERROR                            = -4009;
constexpr int OB_ALLOCATE_MEMORY_FAILED              = -4013;
constexpr int OB_SIZE_OVERFLOW                       = -4019;
constexpr int OB_BACKUP_FILE_NOT_EXIST               = -9011;
constexpr int OB_COS_ERROR                           = -9060;
constexpr int OB_IO_LIMIT                            = -9061;
constexpr int OB_BACKUP_PERMISSION_DENIED            = -9071;

const int COS_OBJECT_NOT_EXIST  = 404;
const int COS_PERMISSION_DENIED = 403;

//datetime formate : Tue, 09 Apr 2019 06:24:00 GMT
//time unit is second
static int64_t strtotime(const char *date_time)
{
  int64_t time = 0;
  struct tm tm_time;
  if (NULL == strptime(date_time, "%a, %d %b %Y %H:%M:%S %Z", &tm_time)) {
    //skip set ret, for compat data formate
    cos_warn_log("[COS]failed to transform time, time=%s\n", date_time);
  } else {
    time = mktime(&tm_time);
  }

  return time;
}

static void convert_io_error(cos_status_t *cos_ret, int &ob_errcode)
{
  if (NULL == cos_ret) {
    ob_errcode = OB_COS_ERROR;
  } else if (!cos_status_is_ok(cos_ret)) {
    switch (cos_ret->code) {

    case COS_PERMISSION_DENIED: {
        ob_errcode = OB_BACKUP_PERMISSION_DENIED;
    break;
     }
    case COS_OBJECT_NOT_EXIST: {
        ob_errcode = OB_BACKUP_FILE_NOT_EXIST;
    break;
     }
    default: {
        ob_errcode = OB_COS_ERROR;
     }
    }
  }
}


int ObCosAccount::set_field(const char *value, char *field, uint32_t length)
{
  int ret = OB_SUCCESS;

  if (NULL == value || NULL == field) {
    ret = OB_INVALID_ARGUMENT;
    cos_warn_log("[COS]invalid args, value=%p, field=%p, ret=%d\n", value, field, ret);
  } else {
    const uint32_t value_len = strlen(value);
    if (value_len >= length) {
      ret = OB_SIZE_OVERFLOW;
      cos_warn_log("[COS]value is too long, value_len=%u, length=%u, ret=%d\n", value_len, length, ret);
    } else {
      memcpy(field, value, value_len);
      field[value_len] = '\0';
    }
  }

  return ret;
}

int ObCosAccount::parse_from(const char *storage_info, uint32_t size)
{
  int ret = OB_SUCCESS;
  if (NULL == storage_info || MAX_COS_DOMAIN_LENGTH <= size) {
    ret = OB_INVALID_ARGUMENT;
    cos_warn_log("[COS]cos parse account failed, storage_info=%s, size=%d, ret=%d\n", storage_info, size, ret);
  } else {
    // host=xxxx&access_id=xxx&access_key=xxx&appid=xxx
    char tmp[MAX_COS_DOMAIN_LENGTH];
    char *token = NULL;
    char *saved_ptr = NULL;
    const char *HOST = "host=";
    const char *ACCESS_ID = "access_id=";
    const char *ACCESS_KEY = "access_key=";
    const char *APPID = "appid=";

    uint8_t bitmap = 0;

    memcpy(tmp, storage_info, size);
    tmp[size] = '\0';
    token = tmp;
    for (char *str = token; ret == OB_SUCCESS; str = NULL) {
      token = ::strtok_r(str, "&", &saved_ptr);
      if (NULL == token) {
        break;
      } else if (0 == strncmp(HOST, token, strlen(HOST))) {
        if (OB_SUCCESS != (ret = set_field(token + strlen(HOST), endpoint, sizeof(endpoint)))) {
          cos_warn_log("[COS]failed to set endpoint=%s, ret=%d\n", token, ret);
        } else {
          bitmap |= 1;
        }
      } else if (0 == strncmp(ACCESS_ID, token, strlen(ACCESS_ID))) {
        if (OB_SUCCESS != (ret = set_field(token + strlen(ACCESS_ID), access_id, sizeof(access_id)))) {
          cos_warn_log("[COS]failed to set access_id=%s, ret=%d\n", token, ret);
        } else {
          bitmap |= (1 << 1);
        }
      } else if (0 == strncmp(ACCESS_KEY, token, strlen(ACCESS_KEY))) {
        if (OB_SUCCESS != (ret = set_field(token + strlen(ACCESS_KEY), access_key, sizeof(access_key)))) {
          cos_warn_log("[COS]failed to set access_key=%s, ret=%d\n", token, ret);
        } else {
          bitmap |= (1 << 2);
        }
      } else if (0 == strncmp(APPID, token, strlen(APPID))) {
        if (OB_SUCCESS != (ret = set_field(token + strlen(APPID), appid, sizeof(appid)))) {
          cos_warn_log("[COS]failed to set appid=%s, ret=%d\n", token, ret);
        } else {
           bitmap |= (1 << 3);
        }
      } else {
        cos_warn_log("[COS]unkown token:%s\n", token);
      }
    }

    if (OB_SUCCESS == ret && bitmap != 0x0F) {
      ret = OB_COS_ERROR;
      cos_warn_log("[COS]failed to parse cos account storage_info=%s, bitmap=%x, ret=%d\n", storage_info, bitmap, ret);
    }
  }

  return ret;
}

int ObCosEnv::init()
{
  int ret = OB_SUCCESS;
  int cos_ret = COSE_OK;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    cos_warn_log("[COS]cannot init cos env more than once, ret=%d\n", ret);
  } else if(COSE_OK != (cos_ret = cos_http_io_initialize(NULL, 0))) {
    ret = OB_COS_ERROR;
    cos_warn_log("[COS]fail to init cos env, cos_ret=%d, ret=%d\n", cos_ret, ret);
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObCosEnv::fin()
{
  if (is_inited_) {
    cos_http_io_deinitialize();
    is_inited_ = false;
  }
}

int ObCosEnv::set_slice_size(const int64_t size)
{
  int ret = OB_SUCCESS;
  if (ObCosEnv::Conf::MAX_COS_SLICE_SIZE < size || ObCosEnv::Conf::MIN_COS_SLICE_SIZE > size) {
    ret = OB_INVALID_ARGUMENT;
    cos_warn_log("[COS]fail to set slice size, ret=%d, size=%ld\n", ret, size);
  } else if (size != conf_.slice_size) {
    conf_.slice_size = size;
    cos_warn_log("[COS]set slice size to %ld\n", size);
  }

  return ret;
}

int64_t ObCosEnv::get_slice_size() const
{
  return conf_.slice_size;
}


// define cos context
struct CosContext
{
  cos_pool_t *mem_pool;
  cos_request_options_t *options;
  OB_COS_customMem custom_mem;

  CosContext(OB_COS_customMem &mem) : mem_pool(NULL), options(NULL)
  {
    custom_mem.customAlloc = mem.customAlloc;
    custom_mem.customFree = mem.customFree;
    custom_mem.opaque = mem.opaque;
  }
};

static void log_status(cos_status_t *s)
{
  if (NULL != s) {
    cos_warn_log("status->code: %d", s->code);
    if (s->error_code) cos_warn_log("status->error_code: %s", s->error_code);
    if (s->error_msg) cos_warn_log("status->error_msg: %s", s->error_msg);
    if (s->req_id) cos_warn_log("status->req_id: %s", s->req_id);
  }
}


int ObCosWrapper::create_cos_handle(
  OB_COS_customMem &custom_mem,
  const struct ObCosAccount &account,
  ObCosWrapper::Handle **h)
{
  int ret = OB_SUCCESS;
  int apr_ret = APR_SUCCESS;

  *h = NULL;
  CosContext *ctx = static_cast<CosContext*>(custom_mem.customAlloc(custom_mem.opaque, sizeof(CosContext)));

  if (NULL == ctx) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    cos_warn_log("[COS]fail to allocate cos context memory, ret=%d\n", ret);
  } else {
    ctx = new(ctx) CosContext(custom_mem);
    if (APR_SUCCESS != (apr_ret = cos_pool_create(&ctx->mem_pool, NULL)) || NULL == ctx->mem_pool) {
      custom_mem.customFree(custom_mem.opaque, ctx);
      ret = OB_ALLOCATE_MEMORY_FAILED;
      cos_warn_log("[COS]fail to create cos memory pool, apr_ret=%d, ret=%d\n", apr_ret, ret);
    } else if (NULL == (ctx->options = cos_request_options_create(ctx->mem_pool))) {
      cos_pool_destroy(ctx->mem_pool);
      custom_mem.customFree(custom_mem.opaque, ctx);
      ret = OB_ALLOCATE_MEMORY_FAILED;
      cos_warn_log("[COS]fail to create cos request option, ret=%d\n", ret);
    } else if (NULL == (ctx->options->config = cos_config_create(ctx->mem_pool))) {
      cos_pool_destroy(ctx->mem_pool);
      custom_mem.customFree(custom_mem.opaque, ctx);
      ret = OB_ALLOCATE_MEMORY_FAILED;
      cos_warn_log("[COS]fail to create cos option config, ret=%d\n", ret);
    } else if (NULL == (ctx->options->ctl = cos_http_controller_create(ctx->options->pool, 0))) {
      cos_pool_destroy(ctx->mem_pool);
      custom_mem.customFree(custom_mem.opaque, ctx);
      ret = OB_ALLOCATE_MEMORY_FAILED;
      cos_warn_log("[COS]fail to create cos http controller, ret=%d\n", ret);
    } else {
      *h = reinterpret_cast<Handle *>(ctx);

      cos_str_set(&ctx->options->config->endpoint, account.endpoint);
      cos_str_set(&ctx->options->config->access_key_id, account.access_id);
      cos_str_set(&ctx->options->config->access_key_secret, account.access_key);
      cos_str_set(&ctx->options->config->appid, account.appid);
      ctx->options->config->is_cname = 0;
      // connection timeout, default 60s
      ctx->options->ctl->options->connect_timeout = 60;
      // DNS timeout, default 60s
      ctx->options->ctl->options->dns_cache_timeout = 120;
      ctx->options->ctl->options->speed_time = 60;
      ctx->options->ctl->options->speed_limit = 16000;
    }
  }

  return ret;
}


void ObCosWrapper::destroy_cos_handle(
  Handle *h)
{
  if (NULL != h) {
    CosContext *ctx = reinterpret_cast<CosContext *>(h);
    cos_pool_destroy(ctx->mem_pool);
    ctx->custom_mem.customFree(ctx->custom_mem.opaque, ctx);
  }
}


int ObCosWrapper::put(
  Handle *h,
  const CosStringBuffer &bucket_name,
  const CosStringBuffer &object_name,
  const char *buf,
  const int64_t buf_size)
{
  int ret = OB_SUCCESS;

  CosContext *ctx = reinterpret_cast<CosContext *>(h);

  if (NULL == h) {
    ret = OB_INVALID_ARGUMENT;
    cos_warn_log("[COS]cos handle is null, ret=%d\n", ret);
  } else if (bucket_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    cos_warn_log("[COS]bucket name is null, ret=%d\n", ret);
  } else if (object_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    cos_warn_log("[COS]object name is null, ret=%d\n", ret);
  } else if (0 > buf_size) {
    // To support empty object, size = 0 is valid.
    ret = OB_INVALID_ARGUMENT;
    cos_warn_log("[COS]buf size not valid, buf_size=%ld, ret=%d\n", buf_size, ret);
  } else if (NULL == buf && 0 < buf_size) {
    ret = OB_INVALID_ARGUMENT;
    cos_warn_log("[COS]buf is null, buf=%p, buf_size=%ld, ret=%d\n", buf, buf_size, ret);
  } else {
    cos_string_t bucket;
    cos_string_t object;
    cos_str_set(&bucket, bucket_name.data);
    cos_str_set(&object, object_name.data);
    cos_table_t *resp_headers = NULL;
    cos_status_t *cos_ret = NULL;
    cos_list_t buffer;
    cos_buf_t *content = NULL;
    cos_list_init(&buffer);

    if (NULL == (content = cos_buf_pack(ctx->mem_pool, buf, static_cast<int32_t>(buf_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      cos_warn_log("[COS]fail to pack buf, ret=%d\n", ret);
    } else {
      cos_list_add_tail(&content->node, &buffer);
      if(NULL == (cos_ret = cos_put_object_from_buffer(ctx->options, &bucket, &object, &buffer, NULL, &resp_headers))
      || !cos_status_is_ok(cos_ret)) {
        convert_io_error(cos_ret, ret);
        cos_warn_log("[COS]failed to put one object to cos, ret=%d\n", ret);
        log_status(cos_ret);
      }
    }
  }

  return ret;
}

int ObCosWrapper::head_object_meta(
  Handle *h,
  const CosStringBuffer &bucket_name,
  const CosStringBuffer &object_name,
  bool &is_exist,
  CosObjectMeta &meta)
{
  int ret = OB_SUCCESS;

  CosContext *ctx = reinterpret_cast<CosContext *>(h);

  if (NULL == h) {
    ret = OB_INVALID_ARGUMENT;
    cos_warn_log("[COS]cos handle is null, ret=%d\n", ret);
  } else if (bucket_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    cos_warn_log("[COS]bucket name is null, ret=%d\n", ret);
  } else if (object_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    cos_warn_log("[COS]object name is null, ret=%d\n", ret);
  } else {
    cos_string_t bucket;
    cos_string_t object;
    cos_str_set(&bucket, bucket_name.data);
    cos_str_set(&object, object_name.data);
    cos_table_t *resp_headers = NULL;
    cos_status_t *cos_ret = NULL;
    char *file_length_ptr = NULL;
    char *last_modified_ptr = NULL;

    const char COS_CONTENT_LENGTH[] = "Content-Length";
    const char COS_LAST_MODIFIED[] = "Last-Modified";

    if (NULL == (cos_ret = cos_head_object(ctx->options, &bucket, &object, NULL, &resp_headers))
      || !cos_status_is_ok(cos_ret)) {
      if (NULL != cos_ret && COS_OBJECT_NOT_EXIST == cos_ret->code) {
        is_exist = false;
      } else {
        convert_io_error(cos_ret, ret);
        cos_warn_log("[COS]failed to get file meta, ret=%d.\n", ret);
        log_status(cos_ret);
      }
    } else {
      is_exist = true;
      // get object length
      if (NULL != (file_length_ptr = (char*)apr_table_get(resp_headers, COS_CONTENT_LENGTH))) {
        // enhance verification
        // not a valid file length, start with a non-digit character
        if (0 == isdigit(*file_length_ptr)) {
          ret = OB_IO_ERROR;
          cos_warn_log("[COS]not a valid file length, something wrong unexpected, ret=%d, file_length_ptr=%s.\n", ret, file_length_ptr);
        } else {
          char *end;
          meta.file_length = strtoll(file_length_ptr, &end, 10);
          // not a valid file length, end with a non-digit character
          if (0 != *end) {
            ret = OB_IO_ERROR;
            cos_warn_log("[COS]not a valid file length, something wrong unexpected, ret=%d, file_length_ptr=%s.\n", ret, file_length_ptr);
          }
        }
      }

      // get object last modified time
      if (NULL != (last_modified_ptr = (char*)apr_table_get(resp_headers, COS_LAST_MODIFIED))) {
        meta.last_modified_ts = strtotime(last_modified_ptr);
      } else {
        ret = OB_COS_ERROR;
        cos_warn_log("[COS]failed to get last modified from apr table, something wrong unexpected, ret=%d.\n", ret);
      }
    }
  }

  return ret;
}


int ObCosWrapper::del(
  Handle *h,
  const CosStringBuffer &bucket_name,
  const CosStringBuffer &object_name)
{
  int ret = OB_SUCCESS;

  CosContext *ctx = reinterpret_cast<CosContext *>(h);

  if (NULL == h) {
    ret = OB_INVALID_ARGUMENT;
    cos_warn_log("[COS]cos handle is null, ret=%d\n", ret);
  } else if (bucket_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    cos_warn_log("[COS]bucket name is null, ret=%d\n", ret);
  } else if (object_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    cos_warn_log("[COS]object name is null, ret=%d\n", ret);
  } else {
    cos_string_t bucket;
    cos_string_t object;
    cos_str_set(&bucket, bucket_name.data);
    cos_str_set(&object, object_name.data);
    cos_table_t *resp_headers = NULL;
    cos_status_t *cos_ret = NULL;

    if (NULL == (cos_ret = cos_delete_object(ctx->options, &bucket, &object, &resp_headers))
    || !cos_status_is_ok(cos_ret)) {
      convert_io_error(cos_ret, ret);
      cos_warn_log("[COS]failed to delete object, ret=%d\n", ret);
      log_status(cos_ret);
    }
  }

  return ret;
}

int ObCosWrapper::del_objects_in_dir(
  Handle *h,
  const CosStringBuffer &bucket_name,
  const CosStringBuffer &dir_name,
  int64_t &deleted_cnt)
{
  // There is no ability to delete those objects that have a common prefix
  // directly which sdk supports. Otherwise, we need list these objects then
  // delete them.
  int ret = OB_SUCCESS;

  const char *seperator = "/";
  // dir_name must be end with '/\0'
  const int32_t min_dir_name_str_len = 2;

  CosContext *ctx = reinterpret_cast<CosContext *>(h);
  deleted_cnt = 0;
  if (NULL == h) {
    ret = OB_INVALID_ARGUMENT;
    cos_warn_log("[COS]cos handle is null, ret=%d\n", ret);
  } else if (bucket_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    cos_warn_log("[COS]bucket name is null, ret=%d\n", ret);
  } else if (NULL == dir_name.data || min_dir_name_str_len >= dir_name.size) {
    ret = OB_INVALID_ARGUMENT;
    cos_warn_log("[COS]dir_name size too short, size=%d, ret=%d\n", dir_name.size, ret);
  } else if (dir_name.data[dir_name.size - 2] != *seperator) {
    ret = OB_INVALID_ARGUMENT;
    cos_warn_log("[COS]dir_name format not right, dir=%s, ret=%d\n", dir_name.data, ret);
  } else {
    cos_status_t *cos_ret = NULL;
    cos_list_object_params_t *params = NULL;

    if (NULL == (params = cos_create_list_object_params(ctx->mem_pool))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      cos_warn_log("[COS]fail to create list object params, ret=%d\n", ret);
    } else {
      cos_str_set(&params->prefix, dir_name.data);
      cos_str_set(&params->marker, "/");
      params->max_ret = 1000;
      cos_string_t bucket;
      cos_str_set(&bucket, bucket_name.data);
      cos_list_object_content_t *content = NULL;

      cos_list_t to_delete_object_list;
      cos_list_t deleted_object_list;
      cos_object_key_t *to_delete_object = NULL;
      cos_table_t *resp_headers = NULL;

      cos_list_init(&to_delete_object_list);
      cos_list_init(&deleted_object_list);

      do {
        // List objects from cos, limit 1000.
        cos_ret = cos_list_object(ctx->options, &bucket, params, NULL);
        if (NULL == cos_ret || !cos_status_is_ok(cos_ret)) {
          if (NULL != cos_ret && 503 == cos_ret->code) {
            // Request is limited by cos, returne "SlowDown, Reduce your request rate".
            ret = OB_IO_LIMIT;
          } else {
            ret = OB_COS_ERROR;
          }
          cos_warn_log("[COS]fail to list object, ret=%d\n", ret);
          log_status(cos_ret);
        } else {
          // Traverse the returned objects
          cos_list_for_each_entry(cos_list_object_content_t, content, &params->object_list, node) {
            // Check if the prefix of returned object key match the dir_name
            size_t dir_name_str_len = strlen(dir_name.data);
            if (0 != memcmp(content->key.data, dir_name.data, dir_name_str_len)) {
              ret = OB_COS_ERROR;
              cos_warn_log("[COS]returned object prefix not match, dir=%s, object=%s, ret=%d\n", dir_name.data, content->key.data, ret);
            } else if (NULL == (to_delete_object = cos_create_cos_object_key(ctx->mem_pool))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              cos_warn_log("[COS]create to delete object key memory failed, dir=%s, object=%s, ret=%d\n", dir_name.data, content->key.data, ret);
            } else {
              // Mark object to do delete
              cos_str_set(&to_delete_object->key, content->key.data);
              cos_list_add_tail(&to_delete_object->node, &to_delete_object_list);
            }

            if (OB_SUCCESS != ret) {
              break;
            }
          }

          // Delete current batch of objects, limit 1000.
          if (OB_SUCCESS == ret && !cos_list_empty(&to_delete_object_list)) {
            if (NULL == (cos_ret = cos_delete_objects(ctx->options, &bucket, &to_delete_object_list, COS_FALSE, &resp_headers, &deleted_object_list))
              || !cos_status_is_ok(cos_ret)) {
              convert_io_error(cos_ret, ret);
              cos_warn_log("[COS]delete objects failed, ret=%d.\n", ret);
              log_status(cos_ret);
            }

            // Traverse the deleted objects
            cos_list_object_content_t *deleted_object = NULL;
            cos_list_for_each_entry(cos_list_object_content_t, deleted_object, &deleted_object_list, node) {
              ++deleted_cnt;
            }
          }

          // Delete next batch of objects.
          char *next_marker_str = NULL;
          if (OB_SUCCESS == ret && COS_TRUE == params->truncated) {
            if (NULL == (next_marker_str = apr_psprintf(ctx->mem_pool, "%.*s", params->next_marker.len, params->next_marker.data))) {
            ret = OB_COS_ERROR;
            cos_warn_log("[COS]get next marker is null, ret=%d\n", ret);
            } else {
              cos_str_set(&params->marker, next_marker_str);
              cos_list_init(&params->object_list);
              cos_list_init(&params->common_prefix_list);

              cos_list_init(&to_delete_object_list);
              cos_list_init(&deleted_object_list);
            }
          }
        }
      } while(COS_TRUE == params->truncated && OB_SUCCESS == ret);
    }
  }

  return ret;
}

int ObCosWrapper::update_object_modified_ts(
  Handle *h,
  const CosStringBuffer &bucket_name,
  const CosStringBuffer &object_name)
{
  int ret = OB_SUCCESS;

  CosContext *ctx = reinterpret_cast<CosContext *>(h);

  if (NULL == h) {
    ret = OB_INVALID_ARGUMENT;
    cos_warn_log("[COS]cos handle is null, ret=%d\n", ret);
  } else if (bucket_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    cos_warn_log("[COS]bucket name is null, ret=%d\n", ret);
  } else if (object_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    cos_warn_log("[COS]object name is null, ret=%d\n", ret);
  } else {
    cos_string_t bucket;
    cos_string_t object;
    cos_str_set(&bucket, bucket_name.data);
    cos_str_set(&object, object_name.data);
    cos_table_t *resp_headers = NULL;
    cos_status_t *cos_ret = NULL;

    cos_table_t *headers = NULL;
    cos_copy_object_params_t *params = NULL;
    if (NULL == (headers = cos_table_make(ctx->mem_pool, 2))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      cos_warn_log("[COS]failed to allocate header memory, ret=%d\n", ret);
    } else if (NULL == (params = cos_create_copy_object_params(ctx->mem_pool))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      cos_warn_log("[COS]failed to allocate copy object param memory, ret=%d\n", ret);
    } else {
      // In Cos, things are different. We cannot refresh the object's latest modified
      // time by updating ACL just like what we do for OSS. Instead, we use "PUT Object - Copy".
      // See https://cloud.tencent.com/document/product/436/10881.

      // Let copied object replace the source meta, default is copy.
      apr_table_add(headers, "x-cos-metadata-directive", "Replaced");

      // Following is not needed. To delete later.
      // apr_table_add(headers, "Cache-Control", "no-cache");
      // Let the source and destination objects same.
      cos_ret = cos_copy_object(ctx->options, &bucket, &object, &ctx->options->config->endpoint, &bucket, &object, headers, params, &resp_headers);
      if (NULL == cos_ret || !cos_status_is_ok(cos_ret)) {
        ret = OB_COS_ERROR;
        cos_warn_log("[COS]fail to call copy object, ret=%d\n", ret);
        log_status(cos_ret);
      } else {
        cos_warn_log("[COS]cos put copy succeeded, ret=%d.\n", ret);
      }
    }
  }

  return ret;
}

int ObCosWrapper::pread(
  Handle *h,
  const CosStringBuffer &bucket_name,
  const CosStringBuffer &object_name,
  int64_t offset,
  char *buf,
  int64_t buf_size,
  int64_t &read_size)
{
  int ret = OB_SUCCESS;

  CosContext *ctx = reinterpret_cast<CosContext *>(h);
  read_size = 0;

  if (NULL == h) {
    ret = OB_INVALID_ARGUMENT;
    cos_warn_log("[COS]cos handle is null, ret=%d\n", ret);
  } else if (bucket_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    cos_warn_log("[COS]bucket name is null, ret=%d\n", ret);
  } else if (object_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    cos_warn_log("[COS]object name is null, ret=%d\n", ret);
  } else if (NULL == buf || 0 >= buf_size) {
    ret = OB_INVALID_ARGUMENT;
    cos_warn_log("[COS]buffer is null, ret=%d\n", ret);
  } else {
    cos_string_t bucket;
    cos_string_t object;
    cos_str_set(&bucket, bucket_name.data);
    cos_str_set(&object, object_name.data);
    cos_table_t *headers = NULL;
    cos_table_t *resp_headers = NULL;
    cos_status_t *cos_ret = NULL;

    cos_list_t buffer;
    cos_list_init(&buffer);

    const int COS_RANGE_SIZE = 256;
    const char* const COS_RANGE_KEY = "Range";

    char range_size[COS_RANGE_SIZE];
    // Cos read range of [10, 100] include the start offset 10 and the end offset 100.
    // But what we except is [10, 100) which does not include the end.
    // So we subtract 1 from the end.
    int n = snprintf(range_size, COS_RANGE_SIZE, "bytes=%ld-%ld", offset, offset + buf_size - 1);
    if (0 >= n || COS_RANGE_SIZE <= n) {
      ret = OB_SIZE_OVERFLOW;
      cos_warn_log("[COS]fail to format range size,n=%d, ret=%d\n", n, ret);
    } else if (NULL == (headers = cos_table_make(ctx->mem_pool, 1))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      cos_warn_log("[COS]fail to make cos headers, ret=%d\n", ret);
    } else {
      apr_table_set(headers, COS_RANGE_KEY, range_size);

      if (NULL == (cos_ret = cos_get_object_to_buffer(ctx->options, &bucket, &object, headers, NULL, &buffer, &resp_headers)) ||
        !cos_status_is_ok(cos_ret)) {
        ret = OB_COS_ERROR;
        cos_warn_log("[COS]fail to get object to buffer, ret=%d\n", ret);
        log_status(cos_ret);
      } else {
        int64_t size = 0;
        int64_t buf_pos = 0;
        cos_buf_t *content = NULL;
        cos_list_for_each_entry(cos_buf_t, content, &buffer, node) {
          size = cos_buf_size(content);
          if (buf_pos + size > buf_size) {
            ret = OB_IO_ERROR;
            cos_warn_log("[COS]unexpected error, too much data returned, ret=%d, range_size=%s, buf_pos=%ld, size=%ld, req_id=%s.\n", ret, range_size, buf_pos, size, cos_ret->req_id);
            log_status(cos_ret);
            break;
          } else {
            // copy to buf
            memcpy(buf + buf_pos, content->pos, (size_t)size);
            buf_pos += size;
            read_size += size;
          }
        }
      }
    }
  }

  return ret;
}

int ObCosWrapper::get_object(
    Handle *h,
    const CosStringBuffer &bucket_name,
    const CosStringBuffer &object_name,
    char *buf,
    int64_t buf_size,
    int64_t &read_size)
{
  int ret = OB_SUCCESS;

  CosContext *ctx = reinterpret_cast<CosContext *>(h);
  read_size = 0;

  if (NULL == h) {
    ret = OB_INVALID_ARGUMENT;
    cos_warn_log("[COS]cos handle is null, ret=%d\n", ret);
  } else if (bucket_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    cos_warn_log("[COS]bucket name is null, ret=%d\n", ret);
  } else if (object_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    cos_warn_log("[COS]object name is null, ret=%d\n", ret);
  } else if (NULL == buf || 0 >= buf_size) {
    ret = OB_INVALID_ARGUMENT;
    cos_warn_log("[COS]buffer is null, ret=%d\n", ret);
  } else {
    cos_string_t bucket;
    cos_string_t object;
    cos_str_set(&bucket, bucket_name.data);
    cos_str_set(&object, object_name.data);
    cos_table_t *headers = NULL;
    cos_table_t *resp_headers = NULL;
    cos_status_t *cos_ret = NULL;

    cos_list_t buffer;
    cos_list_init(&buffer);

    if (NULL == (cos_ret = cos_get_object_to_buffer(ctx->options, &bucket, &object, headers, NULL, &buffer, &resp_headers)) ||
      !cos_status_is_ok(cos_ret)) {
      ret = OB_COS_ERROR;
      cos_warn_log("[COS]fail to get object to buffer, ret=%d\n", ret);
      log_status(cos_ret);
    } else {
      int64_t size = 0;
      int64_t buf_pos = 0;
      cos_buf_t *content = NULL;
      cos_list_for_each_entry(cos_buf_t, content, &buffer, node) {
        size = cos_buf_size(content);
        if (buf_pos + size > buf_size) {
          ret = OB_IO_ERROR;
          cos_warn_log("[COS]unexpected error, too much data returned, ret=%d, buf_pos=%ld, size=%ld, buf_size=%ld, req_id=%s.\n", ret, buf_pos, size, buf_size, cos_ret->req_id);
          log_status(cos_ret);
          break;
        } else {
          // copy to buf
          memcpy(buf + buf_pos, content->pos, (size_t)size);
          buf_pos += size;
          read_size += size;
        }
      }
    }
  }

  return ret;
}

int ObCosWrapper::list_objects(
  Handle *h,
  const CosStringBuffer &bucket_name,
  const CosStringBuffer &dir_name,
  const CosStringBuffer &next_marker,
  handleObjectNameFunc handle_object_name_f,
  void *arg)
{
  int ret = OB_SUCCESS;

  const char *seperator = "/";
  // dir_name must be end with '/\0'
  const int32_t min_dir_name_str_len = 2;

  CosContext *ctx = reinterpret_cast<CosContext *>(h);

  if (NULL == h) {
    ret = OB_INVALID_ARGUMENT;
    cos_warn_log("[COS]cos handle is null, ret=%d\n", ret);
  } else if (bucket_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    cos_warn_log("[COS]bucket name is null, ret=%d\n", ret);
  } else if (NULL == dir_name.data || min_dir_name_str_len >= dir_name.size) {
    ret = OB_INVALID_ARGUMENT;
    cos_warn_log("[COS]dir_name size too short, size=%d, ret=%d\n", dir_name.size, ret);
  } else if (dir_name.data[dir_name.size - 2] != *seperator) {
    ret = OB_INVALID_ARGUMENT;
    cos_warn_log("[COS]dir_name format not right, dir=%s, ret=%d\n", dir_name.data, ret);
  } else if (next_marker.empty()) {
    // next_marker end with '\0', its size must be > 0
    ret = OB_INVALID_ARGUMENT;
    cos_warn_log("[COS]next_marker is null, ret=%d\n", ret);
  } else if (NULL == handle_object_name_f) {
    ret = OB_INVALID_ARGUMENT;
    cos_warn_log("[COS]handle_object_name_f is null, ret=%d\n", ret);
  } else {
    cos_status_t *cos_ret = NULL;
    cos_list_object_params_t *params = NULL;

    if (NULL == (params = cos_create_list_object_params(ctx->mem_pool))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      cos_warn_log("[COS]fail to create list object params, ret=%d\n", ret);
    } else {
      cos_str_set(&params->prefix, dir_name.data);
      cos_str_set(&params->marker, next_marker.data);
      params->max_ret = 1000;
      cos_string_t bucket;
      cos_str_set(&bucket, bucket_name.data);
      cos_list_object_content_t *content = NULL;

      CosListObjPara para;
      para.arg = arg;

      do {
        cos_table_t *resp_headers = NULL;
        // List objects from cos
        if (NULL == (cos_ret = cos_list_object(ctx->options, &bucket, params, &resp_headers))
            || !cos_status_is_ok(cos_ret)) {
          if (NULL != cos_ret && 503 == cos_ret->code) {
            // Request is limited by cos, returne "SlowDown, Reduce your request rate".
            ret = OB_IO_LIMIT;
          } else {
            ret = OB_COS_ERROR;
          }
          cos_warn_log("[COS]fail to list object, ret=%d\n", ret);
          log_status(cos_ret);
        } else {
          char *request_id = (char*)apr_table_get(resp_headers, "x-cos-request-id");
          // Traverse the returned objects
          cos_list_for_each_entry(cos_list_object_content_t, content, &params->object_list, node) {
            // check if the prefix of returned object key match the dir_name
            size_t dir_name_str_len = strlen(dir_name.data);
            if (0 != memcmp(content->key.data, dir_name.data, dir_name_str_len)) {
              ret = OB_IO_ERROR;
              cos_warn_log("[COS]returned object prefix not match, dir=%s, object=%s, requestid=%s, ret=%d\n", dir_name.data, content->key.data, request_id, ret);
            } else if (NULL == content->size.data || 0 == content->size.len) {
              ret = OB_IO_ERROR;
              cos_warn_log("[COS]returned object size is empty, dir=%s, object=%s, requestid=%s, ret=%d\n", dir_name.data, content->key.data, request_id, ret);
            } else {
              // Callback to handle the object name, it is a absolute path.
              para.cur_full_path_slice_name = content->key.data;
              para.full_path_size = content->key.len;
              para.cur_object_size = cos_atoi64(content->size.data);
              ret = handle_object_name_f(para);
              if (OB_SUCCESS != ret){
                cos_warn_log("[COS]handle object name failed, ret=%d, object=%s, requestid=%s\n", ret, content->key.data, request_id);
              }
            }

            if (OB_SUCCESS != ret || !para.next_flag) {
              break;
            }
          }

          char *next_marker_str = NULL;
          if (OB_SUCCESS == ret && para.next_flag && COS_TRUE == params->truncated) {
            if (NULL == (next_marker_str = apr_psprintf(ctx->mem_pool, "%.*s", params->next_marker.len, params->next_marker.data))) {
            ret = OB_IO_ERROR;
            cos_warn_log("[COS]get next marker is null, ret=%d\n", ret);
            } else {
              cos_str_set(&params->marker, next_marker_str);
              cos_list_init(&params->object_list);
              cos_list_init(&params->common_prefix_list);
            }
          }
        }
      } while(COS_TRUE == params->truncated && OB_SUCCESS == ret && para.next_flag);
    }
  }

  return ret;
}

int ObCosWrapper::list_directories(
  Handle *h,
  const CosStringBuffer &bucket_name,
  const CosStringBuffer &dir_name,
  const CosStringBuffer &next_marker,
  const CosStringBuffer &delimiter,
  handleDirectoryFunc handle_directory_name_f,
  void *arg)
{
  int ret = OB_SUCCESS;

  const char *seperator = "/";
  // dir_name must be end with '/\0'
  const int32_t min_dir_name_str_len = 2;

  CosContext *ctx = reinterpret_cast<CosContext *>(h);

  if (NULL == h) {
    ret = OB_INVALID_ARGUMENT;
    cos_warn_log("[COS]cos handle is null, ret=%d\n", ret);
  } else if (bucket_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    cos_warn_log("[COS]bucket name is null, ret=%d\n", ret);
  } else if (NULL == dir_name.data || min_dir_name_str_len >= dir_name.size) {
    ret = OB_INVALID_ARGUMENT;
    cos_warn_log("[COS]dir_name size too short, size=%d, ret=%d\n", dir_name.size, ret);
  } else if (dir_name.data[dir_name.size - 2] != *seperator) {
    ret = OB_INVALID_ARGUMENT;
    cos_warn_log("[COS]dir_name format not right, dir=%s, ret=%d\n", dir_name.data, ret);
  } else if (next_marker.empty()) {
    // next_marker end with '\0', its size must be > 0
    ret = OB_INVALID_ARGUMENT;
    cos_warn_log("[COS]next_marker is null, ret=%d\n", ret);
  } else if (NULL == handle_directory_name_f) {
    ret = OB_INVALID_ARGUMENT;
    cos_warn_log("[COS]handle_directory_name_f is null, ret=%d\n", ret);
  } else {
    cos_status_t *cos_ret = NULL;
    cos_list_object_params_t *params = NULL;

    if (NULL == (params = cos_create_list_object_params(ctx->mem_pool))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      cos_warn_log("[COS]fail to create list object params, ret=%d\n", ret);
    } else {
      cos_str_set(&params->prefix, dir_name.data);
      cos_str_set(&params->marker, next_marker.data);
      cos_str_set(&params->delimiter, delimiter.data);
      params->max_ret = 1000;
      cos_string_t bucket;
      cos_str_set(&bucket, bucket_name.data);
      cos_list_object_common_prefix_t *common_prefix = NULL;

      // A mark which indicates whether to process the next object.

      do {
        cos_table_t *resp_headers = NULL;
        // List objects from cos
        if (NULL == (cos_ret = cos_list_object(ctx->options, &bucket, params, &resp_headers))
            || !cos_status_is_ok(cos_ret)) {
          if (NULL != cos_ret && 503 == cos_ret->code) {
            // Request is limited by cos, returne "SlowDown, Reduce your request rate".
            ret = OB_IO_LIMIT;
          } else {
            ret = OB_IO_ERROR;
          }
          cos_warn_log("[COS]fail to list object, ret=%d\n", ret);
          log_status(cos_ret);
        } else {
          char *request_id = (char*)apr_table_get(resp_headers, "x-cos-request-id");
          // Traverse the returned objects
          cos_list_for_each_entry(cos_list_object_common_prefix_t, common_prefix, &params->common_prefix_list, node) {
            // check if the prefix of returned object key match the dir_name
            const size_t dir_name_str_len = strlen(dir_name.data);
            const size_t prefix_str_len = strlen(common_prefix->prefix.data);
            if (prefix_str_len < dir_name_str_len) {
              ret = OB_IO_ERROR;
              cos_warn_log("[COS]prefix str len should not smaller than dir name str len. prefix str len : %lu, "
                  "dir name str len : %lu, ret = %d \n", prefix_str_len, dir_name_str_len, ret);
            } else if (0 != memcmp(common_prefix->prefix.data, dir_name.data, dir_name_str_len)) {
              ret = OB_IO_ERROR;
              cos_warn_log("[COS]returned object prefix not match, dir=%s, object=%s, requestid=%s, ret=%d\n", dir_name.data, common_prefix->prefix.data, request_id, ret);
            } else {
              // Callback to handle the object name, it is a absolute path.
              const int64_t object_size = common_prefix->prefix.len - dir_name_str_len; //include '/'
              ret = handle_directory_name_f(arg, common_prefix->prefix.data + dir_name_str_len, object_size);
              if (OB_SUCCESS != ret) {
                // Something wrong happened when handle object name
                cos_warn_log("[COS]handle object name failed, ret=%d, object=%s, requestid=%s\n", ret, common_prefix->prefix.data, request_id);
              }
            }

            if (OB_SUCCESS != ret) {
              break;
            }
          }

          char *next_marker_str = NULL;
          if (OB_SUCCESS == ret && COS_TRUE == params->truncated) {
            if (NULL == (next_marker_str = apr_psprintf(ctx->mem_pool, "%.*s", params->next_marker.len, params->next_marker.data))) {
            ret = OB_IO_ERROR;
            cos_warn_log("[COS]get next marker is null, ret=%d\n", ret);
            } else {
              cos_str_set(&params->marker, next_marker_str);
              cos_list_init(&params->object_list);
              cos_list_init(&params->common_prefix_list);
            }
          }
        }
      } while(COS_TRUE == params->truncated && OB_SUCCESS == ret);
    }
  }

  return ret;
}



}
}
}
