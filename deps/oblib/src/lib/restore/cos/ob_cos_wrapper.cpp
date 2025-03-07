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


#include "cos_api.h"
#include "cos_log.h"
#include "cos_auth.h"

#include "ob_cos_wrapper.h"
#include "lib/restore/ob_object_storage_base.h"

namespace oceanbase
{
namespace common
{

namespace qcloud_cos
{
using namespace oceanbase::common;

const int COS_BAD_REQUEST = 400;
const int COS_OBJECT_NOT_EXIST  = 404;
const int COS_PERMISSION_DENIED = 403;
const int COS_APPEND_POSITION_ERROR = 409;
const int COS_SERVICE_UNAVAILABLE = 503;

const int64_t OB_STORAGE_LIST_MAX_NUM = 1000;
const int64_t MAX_COS_PART_NUM = 10000;

static apr_allocator_t *COS_GLOBAL_APR_ALLOCATOR = nullptr;
static apr_pool_t *COS_GLOBAL_APR_POOL = nullptr;

int ob_copy_apr_tables(apr_table_t *dst, const apr_table_t *src)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(dst) || OB_ISNULL(src)) {
    ret = OB_INVALID_ARGUMENT;
    cos_warn_log("[COS]invalid args, dst=%p, src=%p\n", dst, src);
  } else {
    apr_table_clear(dst);
    apr_table_overlap(dst, src, APR_OVERLAP_TABLES_SET);
  }
  return ret;
}

//datetime formate : Tue, 09 Apr 2019 06:24:00 GMT
//time unit is second
static int64_t ob_strtotime(const char *date_time)
{
  int64_t time = 0;
  struct tm tm_time;
  memset(&tm_time, 0, sizeof(struct tm));
  if (NULL == strptime(date_time, "%a, %d %b %Y %H:%M:%S %Z", &tm_time)) {
    // ignore ret
    //skip set ret, for compat data formate
    cos_warn_log("[COS]fail to transform time, time=%s\n", date_time);
  } else {
    time = mktime(&tm_time);
  }

  return time;
}

static void convert_io_error(cos_status_t *cos_ret, int &ob_errcode)
{
  if (NULL == cos_ret) {
    ob_errcode = OB_OBJECT_STORAGE_IO_ERROR;
  } else if (!cos_status_is_ok(cos_ret)) {
    switch (cos_ret->code) {
      case COS_PERMISSION_DENIED: {
        ob_errcode = OB_OBJECT_STORAGE_PERMISSION_DENIED;
        break;
      }
      case COS_OBJECT_NOT_EXIST: {
        if (nullptr != cos_ret->error_code
            && (0 == strcmp("NoSuchBucket", cos_ret->error_code))) {
          ob_errcode = OB_INVALID_OBJECT_STORAGE_ENDPOINT;
        } else {
          ob_errcode = OB_OBJECT_NOT_EXIST;
        }
        break;
      }
      case COS_APPEND_POSITION_ERROR: {
        ob_errcode = OB_OBJECT_STORAGE_PWRITE_OFFSET_NOT_MATCH;
        break;
      }
      case COS_SERVICE_UNAVAILABLE: {
        ob_errcode = OB_IO_LIMIT;
        break;
      }
      case COS_BAD_REQUEST: {
        if (nullptr == cos_ret->error_code) {
          ob_errcode = OB_OBJECT_STORAGE_IO_ERROR;
        } else if (0 == strcmp("InvalidDigest", cos_ret->error_code)) {
          ob_errcode = OB_OBJECT_STORAGE_CHECKSUM_ERROR;
        } else if (0 == strcmp("InvalidRegionName", cos_ret->error_code)) {
          ob_errcode = OB_INVALID_OBJECT_STORAGE_ENDPOINT;
        }
        // COS reports different errors for object names that exceed the limited length
        // put: InvalidURI, list: InvalidArgument
        else if (0 == strcmp("InvalidURI", cos_ret->error_code)
            || 0 == strcmp("InvalidArgument", cos_ret->error_code)) {
          ob_errcode = OB_INVALID_ARGUMENT;
        } else {
          ob_errcode = OB_OBJECT_STORAGE_IO_ERROR;
        }
        break;
      }
      default: {
        ob_errcode = OB_OBJECT_STORAGE_IO_ERROR;
        break;
      }
    }
  }
}

int ob_cos_str_assign(cos_string_t &dst, const int64_t len, const char *src)
{
  int ret = OB_SUCCESS;
  if (src == nullptr || len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    cos_warn_log("[COS]invalid args when assign cos_string_t, src=%p, len=%ld, ret=%d\n", src, len, ret);
  } else {
    dst.len = len;
    dst.data = (char *)src;
  }
  return ret;
}

static int64_t get_current_time_us()
{
  int err_ret = 0;
  struct timeval t;
  char errno_buf[512] = "";
  if (OB_UNLIKELY((err_ret = gettimeofday(&t, nullptr)) < 0)) {
    cos_error_log("[COS]gettimeofday error, err_ret=%d, err=%s",
        err_ret, strerror_r(errno, errno_buf, sizeof(errno_buf)));
  }
  return (static_cast<int64_t>(t.tv_sec) * 1000000L +
          static_cast<int64_t>(t.tv_usec));
}

int ob_set_retry_headers(
    apr_pool_t *p,
    apr_table_t *&headers,
    apr_table_t *&origin_headers,
    apr_table_t **&ref_headers)
{
  int ret = OB_SUCCESS;
  origin_headers = nullptr;
  ref_headers = nullptr;
  if (OB_NOT_NULL(headers)) {
    if (OB_ISNULL(p)) {
      ret = OB_INVALID_ARGUMENT;
      cos_warn_log("[COS]apr pool is null, ret=%d, headers=%p\n", ret, headers);
    } else if (OB_ISNULL(origin_headers = apr_table_clone(p, headers))) {
      ret = OB_OBJECT_STORAGE_IO_ERROR;
      cos_warn_log("[COS]fail to deep copy headers, ret=%d, p=%p, headers=%p\n",
          ret, p, headers);
    } else {
      ref_headers = &headers;
    }
  }
  return ret;
}

class ObStorageCOSRetryStrategy : public ObStorageIORetryStrategyBase<cos_status_t *>
{
public:
  ObStorageCOSRetryStrategy(const int64_t timeout_us = ObObjectStorageTenantGuard::get_timeout_us())
      : ObStorageIORetryStrategyBase<cos_status_t *>(timeout_us),
        origin_headers_(nullptr),
        ref_headers_(nullptr),
        ref_buffer_(nullptr),
        deleted_object_list_(nullptr),
        params_(nullptr)
  {
    cos_list_init(&origin_write_content_list_);
    start_time_us_ = get_current_time_us();
  }
  //  ~ObStorageCOSRetryStrategy() {}

  virtual int64_t current_time_us() const override
  {
    return get_current_time_us();
  }

  virtual void log_error(
      const RetType &outcome, const int64_t attempted_retries) const override
  {
    if (OB_NOT_NULL(outcome)) {
      cos_warn_log("[COS]cos log error, start_time_us=%ld, timeout_us=%ld, retries=%d,"
                   " code=%d, err_code=%s, err_msg=%s, req_id=%s\n",
          start_time_us_, timeout_us_, attempted_retries,
          outcome->code, outcome->error_code, outcome->error_msg, outcome->req_id);
    } else {
      cos_warn_log("[COS]cos log error with null outcome, start_time_us=%ld, timeout_us=%ld, retries=%d",
          start_time_us_, timeout_us_, attempted_retries);
    }
  }

  int set_retry_headers(apr_pool_t *p, apr_table_t *&headers)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(ob_set_retry_headers(p, headers, origin_headers_, ref_headers_))) {
      cos_warn_log("[COS]fail to set retry headers, ret=%d, p=%p, headers=%p\n",
            ret, p, headers);
    }
    return ret;
  }

  int set_retry_buffer(cos_list_t *write_content_buffer)
  {
    int ret = OB_SUCCESS;
    if (OB_NOT_NULL(write_content_buffer)) {
      // Using origin_write_content_list_ to
      // record the head and tail nodes of the data nodes in write_content_buffer.
      // During retries, these data nodes are re-linked to write_content_buffer,
      // ensuring data consistency during the retry process.
      // And currently only allow a single node list.
      if (OB_UNLIKELY(write_content_buffer->prev != write_content_buffer->next)) {
        ret = OB_INVALID_ARGUMENT;
        cos_warn_log("[COS]write_content_buffer should have only one node, ret=%d, prev=%p, next=%p\n",
            ret, write_content_buffer->prev, write_content_buffer->next);
      } else {
        // aos_list_t/cos_list_t is a doubly circular linked list
        cos_list_init(&origin_write_content_list_);
        origin_write_content_list_.prev = write_content_buffer->prev;
        origin_write_content_list_.next = write_content_buffer->next;
        ref_buffer_ = write_content_buffer;
      }
    }
    return ret;
  }

  // When batch deleting, the names of successfully deleted objects will be added to the deleted_object_list,
  // so they need to be reset during retries.
  // Only used for errsim cases.
  int set_retry_deleted_object_list(cos_list_t *deleted_object_list)
  {
    int ret = OB_SUCCESS;
    if (OB_NOT_NULL(deleted_object_list)) {
      deleted_object_list_ = deleted_object_list;
    }
    return ret;
  }

  // When listing, the names of successfully listed objects will be added to the params,
  // so they need to be reset during retries.
  // Only used for errsim cases.
  int set_retry_list_object_params(cos_list_object_params_t *params)
  {
    int ret = OB_SUCCESS;
    if (OB_NOT_NULL(params)) {
      params_ = params;
    }
    return ret;
  }

protected:
  virtual bool is_timeout_(const int64_t attempted_retries) const override
  {
    bool bret = false;
    const int64_t cur_time_us = current_time_us();
    if (cur_time_us >= start_time_us_ + timeout_us_) {
      cos_warn_log("[COS]request reach time limit, "
          "start_time_us=%ld, timeout_us=%ld, cur_time_us=%ld, retries=%d",
          start_time_us_, timeout_us_, cur_time_us, attempted_retries);
      bret = true;
    }
    return bret;
  }

  int reinitialize_headers_() const
  {
    int ret = OB_SUCCESS;
    if (OB_NOT_NULL(ref_headers_)
        && OB_FAIL(ob_copy_apr_tables(*ref_headers_, origin_headers_))) {
      // Note: We cannot directly set *ref_headers_ = origin_headers_.
      // For example, in the function:
      // cos_put_object_from_buffer(const cos_request_options_t *options,
      //                            const cos_string_t *bucket,
      //                            const cos_string_t *object,
      //                            coss_list_t *buffer,
      //                            cos_table_t *headers,
      //                            cos_table_t **resp_headers)
      //
      // The 'headers' parameter in the SDK points to a memory address.
      // When we do *ref_headers_ points to the same memory location.
      // However, if we set *ref_headers_ = origin_headers_, *ref_headers_ will point to
      // a new memory allocation, but the 'headers' parameter
      // in the oss_put_object_from_buffer function will still refer to the old memory address.
      cos_warn_log("[COS]fail to reset headers, ret=%d, ref_headers=%p, origin_headers=%p\n",
            ret, ref_headers_, origin_headers_);
    }
    return ret;
  }

  int reinitialize_buffer_() const
  {
    int ret = OB_SUCCESS;
    if (OB_NOT_NULL(ref_buffer_)) {
      ref_buffer_->prev = origin_write_content_list_.prev;
      ref_buffer_->prev->next = ref_buffer_;
      ref_buffer_->next = origin_write_content_list_.next;
      ref_buffer_->next->prev = ref_buffer_;

      cos_buf_t *content = nullptr;
      cos_list_for_each_entry(cos_buf_t, content, ref_buffer_, node) {
        content->pos = content->start;
      }
    }
    return ret;
  }

  virtual bool should_retry_impl_(
      const RetType &outcome, const int64_t attempted_retries) const override
  {
    bool bret = false;
    if (OB_ISNULL(outcome)) {
      bret = false;
    }  else if (cos_status_is_ok(outcome)) {
      bret = false;
    } else {
      const int cos_code = outcome->code;
      const char *error_code = outcome->error_code;
      const char *req_id = outcome->req_id;
      if (cos_code / 100 == 5
          || cos_code == COSE_CONNECTION_FAILED || cos_code == COSE_SERVICE_ERROR
          || cos_code == COSE_FAILED_CONNECT || cos_code == COSE_REQUEST_TIMEOUT) {
        bret = true;
      } else if (OB_NOT_NULL(error_code) && 0 == strcmp(error_code, COS_HTTP_IO_ERROR_CODE)) {
        bret = true;
      } else if (cos_code / 100 != 2 && (OB_ISNULL(req_id) || req_id[0] == '\0')) {
        bret = true;
      }
    }

    int ret = OB_SUCCESS;
    if (bret) {
      int ret = OB_SUCCESS;
      if (OB_NOT_NULL(deleted_object_list_)) {
        cos_list_init(deleted_object_list_);
      }
      if (OB_NOT_NULL(params_)) {
        // reuse params
        cos_list_init(&params_->object_list);
        cos_list_init(&params_->common_prefix_list);
      }

      if (FAILEDx(reinitialize_headers_())) {
        cos_warn_log("[COS]fail to reinitialize headers, "
            "ret=%d, origin_headers=%p, ref_headers=%p\n",
            ret, origin_headers_, ref_headers_);
      } else if (OB_FAIL(reinitialize_buffer_())) {
        cos_warn_log("[COS]fail to reinitialize buffer, "
            "ret=%d, ref_buffer=%p, n_origin_list_entry=%ld\n",
            ret, ref_buffer_, 0);
      }

      if (OB_FAIL(ret)) {
        bret = false;
      }
    }

    return bret;
  }

private:
  // When the COS SDK sends a request, it modifies the header information,
  // which results in the header containing additional fields during retries
  cos_table_t *origin_headers_;
  cos_table_t **ref_headers_;
  cos_list_t *ref_buffer_;
  cos_list_t origin_write_content_list_;
  cos_list_t *deleted_object_list_;
  cos_list_object_params_t *params_;
};

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
    cos_warn_log("[COS]cos parse account failed, storage_info=%p, size=%d, ret=%d\n", storage_info, size, ret);
  } else {
    // host=xxxx&access_id=xxx&access_key=xxx&appid=xxx
    char tmp[MAX_COS_DOMAIN_LENGTH];
    char *token = NULL;
    char *saved_ptr = NULL;
    const char *HOST = "host=";
    const char *ACCESS_ID = "access_id=";
    const char *ACCESS_KEY = "access_key=";
    const char *APPID = "appid=";
    const char *DELETE_MODE = "delete_mode=";

    uint8_t bitmap = 0;

    memcpy(tmp, storage_info, size);
    tmp[size] = '\0';
    token = tmp;
    for (char *str = token; ret == OB_SUCCESS; str = NULL) {
      token = ::strtok_r(str, "&", &saved_ptr);
      if (NULL == token) {
        break;
      } else if (0 == strncmp(HOST, token, strlen(HOST))) {
        if (OB_SUCCESS != (ret = set_field(token + strlen(HOST), endpoint_, sizeof(endpoint_)))) {
          cos_warn_log("[COS]fail to set endpoint=%s, ret=%d\n", token, ret);
        } else {
          bitmap |= 1;
        }
      } else if (0 == strncmp(ACCESS_ID, token, strlen(ACCESS_ID))) {
        if (OB_SUCCESS != (ret = set_field(token + strlen(ACCESS_ID), access_id_, sizeof(access_id_)))) {
          cos_warn_log("[COS]fail to set access_id=%s, ret=%d\n", token, ret);
        } else {
          bitmap |= (1 << 1);
        }
      } else if (0 == strncmp(ACCESS_KEY, token, strlen(ACCESS_KEY))) {
        if (OB_SUCCESS != (ret = set_field(token + strlen(ACCESS_KEY), access_key_, sizeof(access_key_)))) {
          cos_warn_log("[COS]fail to set access_key, ret=%d\n", ret);
        } else {
          bitmap |= (1 << 2);
        }
      } else if (0 == strncmp(APPID, token, strlen(APPID))) {
        if (OB_SUCCESS != (ret = set_field(token + strlen(APPID), appid_, sizeof(appid_)))) {
          cos_warn_log("[COS]fail to set appid=%s, ret=%d\n", token, ret);
        } else {
          bitmap |= (1 << 3);
        }
      } else if (0 == strncmp(DELETE_MODE, token, strlen(DELETE_MODE))) {
        if (OB_SUCCESS != (ret = set_field(token + strlen(DELETE_MODE), delete_mode_, sizeof(delete_mode_)))) {
          cos_warn_log("[COS]fail to set delete_mode=%s, ret=%d", token, ret);
        }
      }
    }

    if (OB_SUCCESS == ret && bitmap != 0x0F) {
      ret = OB_OBJECT_STORAGE_IO_ERROR;
      cos_warn_log("[COS]fail to parse cos account storage_info=%p, bitmap=%x, ret=%d\n", storage_info, bitmap, ret);
    }
  }

  return ret;
}

int ObCosEnv::init(apr_abortfunc_t abort_fn)
{
  int ret = OB_SUCCESS;
  int cos_ret = COSE_OK;
  const int64_t MAX_COS_APR_HOLD_SIZE = 10LL * 1024LL * 1024LL; // 10MB
  apr_thread_mutex_t *mutex = nullptr;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    cos_warn_log("[COS]cannot init cos env more than once, ret=%d\n", ret);
  } else if (COSE_OK != (cos_ret = cos_http_io_initialize(NULL, 0))) {
    ret = OB_OBJECT_STORAGE_IO_ERROR;
    cos_warn_log("[COS]fail to init cos env, cos_ret=%d, ret=%d\n", cos_ret, ret);
  } else {
    int apr_ret = APR_SUCCESS;
    if (APR_SUCCESS != (apr_ret = apr_allocator_create(&COS_GLOBAL_APR_ALLOCATOR))
        || nullptr == COS_GLOBAL_APR_ALLOCATOR) {
      ret = OB_OBJECT_STORAGE_IO_ERROR;
      cos_warn_log("[COS]fail to create global apr allocator, ret=%d, apr_ret=%d, allocator=%p\n",
          ret, apr_ret, COS_GLOBAL_APR_ALLOCATOR);
    } else if (APR_SUCCESS != (apr_ret = apr_pool_create_ex(&COS_GLOBAL_APR_POOL,
                                                            nullptr/*parent*/,
                                                            // It is OK to pass a null pointer. If a null pointer is passed,
                                                            // COS_GLOBAL_APR_POOL will automatically use the global_pool's abort_fn in the APR library.
                                                            abort_fn,
                                                            COS_GLOBAL_APR_ALLOCATOR))
        || nullptr == COS_GLOBAL_APR_POOL) {
      ret = OB_OBJECT_STORAGE_IO_ERROR;
      cos_warn_log("[COS]fail to create apr pool, ret=%d, apr_ret=%d, apr_pool=%p\n",
          ret, apr_ret, COS_GLOBAL_APR_POOL);
    } else if (APR_SUCCESS != (apr_ret = apr_thread_mutex_create(&mutex, APR_THREAD_MUTEX_DEFAULT,
                                                                 COS_GLOBAL_APR_POOL))
        || nullptr == mutex) {
      ret = OB_OBJECT_STORAGE_IO_ERROR;
      cos_warn_log("[COS]fail to create apr thread mutex, ret=%d, apr_ret=%d, mutex=%p\n",
          ret, apr_ret, mutex);
    } else {
      apr_allocator_max_free_set(COS_GLOBAL_APR_ALLOCATOR, MAX_COS_APR_HOLD_SIZE);
      apr_allocator_mutex_set(COS_GLOBAL_APR_ALLOCATOR, mutex);
      apr_allocator_owner_set(COS_GLOBAL_APR_ALLOCATOR, COS_GLOBAL_APR_POOL);
      is_inited_ = true;
    }

    if (OB_SUCCESS != ret) {
      // The owner of COS_GLOBAL_APR_ALLOCATOR will only be set to COS_GLOBAL_APR_POOL
      // if the initialization succeeds.
      // Therefore, if an error occurs during initialization,
      // destroying COS_GLOBAL_APR_POOL won't automatically free COS_GLOBAL_APR_ALLOCATOR.
      // apr_thread_mutex_t allocates memory based on apr_allocator_t.
      // When the corresponding allocator is destroyed,
      // the memory allocated for the mutex is automatically released.
      if (nullptr != COS_GLOBAL_APR_POOL) {
        apr_pool_destroy(COS_GLOBAL_APR_POOL);
        COS_GLOBAL_APR_POOL = nullptr;
      }
      if (nullptr != COS_GLOBAL_APR_ALLOCATOR) {
        apr_allocator_destroy(COS_GLOBAL_APR_ALLOCATOR);
        COS_GLOBAL_APR_ALLOCATOR = nullptr;
      }
    }
  }
  return ret;
}

void ObCosEnv::destroy()
{
  if (is_inited_) {
    if (nullptr != COS_GLOBAL_APR_POOL) {
      // After successful initialization,
      // the owner of COS_GLOBAL_APR_ALLOCATOR is set to COS_GLOBAL_APR_POOL.
      // Thus, destroying COS_GLOBAL_APR_POOL will also free COS_GLOBAL_APR_ALLOCATOR.
      apr_pool_destroy(COS_GLOBAL_APR_POOL);
      COS_GLOBAL_APR_POOL = nullptr;
      COS_GLOBAL_APR_ALLOCATOR = nullptr;
    }
    cos_http_io_deinitialize();
    is_inited_ = false;
  }
}


// define cos context
struct CosContext
{
  cos_pool_t *mem_pool;
  cos_request_options_t *options;
  OB_COS_customMem custom_mem;

  CosContext(OB_COS_customMem &mem)
    : mem_pool(NULL), options(NULL)
  {
    custom_mem.customAlloc = mem.customAlloc;
    custom_mem.customFree = mem.customFree;
    custom_mem.opaque = mem.opaque;
  }
};

static void log_status(cos_status_t *s, const int ob_errcode)
{
  if (nullptr != s) {
    if (OB_OBJECT_STORAGE_CHECKSUM_ERROR == ob_errcode) {
      cos_error_log("[COS]cos_log_status ret=%d, code=%d, error_code=%s, error_msg=%s, req_id=%s",
          ob_errcode, s->code, s->error_code, s->error_msg, s->req_id);
    } else {
      cos_warn_log("[COS]cos_log_status ret=%d, code=%d, error_code=%s, error_msg=%s, req_id=%s",
          ob_errcode, s->code, s->error_code, s->error_msg, s->req_id);
    }
  }
}

int ObCosWrapper::CosListObjPara::set_cur_obj_meta(
    char *obj_full_path,
    const int64_t full_path_size,
    char *object_size_str,
    const int64_t object_size_str_len)
{
  int ret = OB_SUCCESS;
  if (NULL == obj_full_path || full_path_size <= 0 || NULL == object_size_str) {
    ret = OB_INVALID_ARGUMENT;
    cos_warn_log("[COS]invalid object meta, obj_full_path=%s, full_path_size=%ld, object_size_str=%s, ret=%d\n",
        obj_full_path, full_path_size, object_size_str, ret);
  } else {
    cur_obj_full_path_ = obj_full_path;
    full_path_size_ = full_path_size;
    cur_object_size_str_ = object_size_str;
    cur_object_size_str_len_ = object_size_str_len;
  }
  return ret;
}

int ObCosWrapper::create_cos_handle(
    OB_COS_customMem &custom_mem,
    const struct ObCosAccount &account,
    const bool check_md5,
    const char *cos_sts_token,
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
    if (APR_SUCCESS != (apr_ret = cos_pool_create(&ctx->mem_pool, COS_GLOBAL_APR_POOL)) || NULL == ctx->mem_pool) {
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
      // A separate instance of ctl->options is now allocated for each request,
      // ensuring that disabling CRC checks is a request-specific action
      // and does not impact the global setting for COS request options.
    } else if (NULL ==
        (ctx->options->ctl->options = cos_http_request_options_create(ctx->options->pool))) {
      cos_pool_destroy(ctx->mem_pool);
      custom_mem.customFree(custom_mem.opaque, ctx);
      ret = OB_ALLOCATE_MEMORY_FAILED;
      cos_warn_log("[COS]fail to create cos http request options, ret=%d\n", ret);
    } else {
      *h = reinterpret_cast<Handle *>(ctx);

      cos_str_set(&ctx->options->config->endpoint, account.endpoint_);
      cos_str_set(&ctx->options->config->access_key_id, account.access_id_);
      cos_str_set(&ctx->options->config->access_key_secret, account.access_key_);
      cos_str_set(&ctx->options->config->appid, account.appid_);
      if (nullptr != cos_sts_token) {
        cos_str_set(&ctx->options->config->sts_token, cos_sts_token);
      }
      ctx->options->config->is_cname = 0;
      // connection timeout, default 60s
      ctx->options->ctl->options->connect_timeout = 60;
      // DNS timeout, default 60s
      ctx->options->ctl->options->dns_cache_timeout = 120;
      ctx->options->ctl->options->speed_time = 60;
      ctx->options->ctl->options->speed_limit = 16000;

      if (check_md5) {
        cos_set_content_md5_enable(ctx->options->ctl, COS_TRUE);
      } else {
        cos_set_content_md5_enable(ctx->options->ctl, COS_FALSE);
      }
      ctx->options->ctl->options->enable_crc = false;
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
    cos_str_set(&bucket, bucket_name.data_);
    cos_str_set(&object, object_name.data_);
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
      ObStorageCOSRetryStrategy strategy;
      if (OB_FAIL(strategy.set_retry_buffer(&buffer))) {
        cos_warn_log("[COS]fail to set buffer, ret=%d, bucket=%s, object=%s\n",
            ret, bucket_name.data_, object_name.data_);
      } else if (OB_ISNULL(cos_ret = execute_until_timeout(
            strategy, cos_put_object_from_buffer,
            ctx->options, &bucket, &object, &buffer, nullptr, &resp_headers))
         || !cos_status_is_ok(cos_ret)) {
        convert_io_error(cos_ret, ret);
        cos_warn_log("[COS]fail to put one object to cos, ret=%d\n", ret);
        log_status(cos_ret, ret);
      }
    }
  }

  return ret;
}

int ObCosWrapper::append(
    Handle *h,
    const CosStringBuffer &bucket_name,
    const CosStringBuffer &object_name,
    const char *buf,
    const int64_t buf_size,
    const int64_t offset)
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
    ret = OB_INVALID_ARGUMENT;
    cos_warn_log("[COS]buf size not valid, buf_size=%ld, ret=%d\n", buf_size, ret);
  } else if (NULL == buf && 0 < buf_size) {
    ret = OB_INVALID_ARGUMENT;
    cos_warn_log("[COS]buf is null, buf=%p, buf_size=%ld, ret=%d\n", buf, buf_size, ret);
  } else if (0 > offset) {
    ret = OB_INVALID_ARGUMENT;
    cos_warn_log("[COS]position not valid, offset=%ld, ret=%d\n", offset, ret);
  } else {
    cos_string_t bucket;
    cos_string_t object;
    cos_str_set(&bucket, bucket_name.data_);
    cos_str_set(&object, object_name.data_);
    cos_table_t *headers = nullptr;
    cos_table_t *resp_headers = nullptr;
    cos_status_t *cos_ret = nullptr;
    cos_list_t buffer;
    cos_buf_t *content = nullptr;
    cos_list_init(&buffer);

    if (nullptr == (headers = cos_table_make(ctx->mem_pool, 1))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      cos_warn_log("[COS]fail to allocate header memory, ret=%d\n", ret);
    } else if (nullptr == (content = cos_buf_pack(ctx->mem_pool, buf, static_cast<int32_t>(buf_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      cos_warn_log("[COS]fail to pack buf, ret=%d\n", ret);
    } else {
      cos_list_add_tail(&content->node, &buffer);
      // If content MD5 is enabled, the function 'cos_add_content_md5_from_buffer'
      // will calculate the MD5 checksum and add it to the headers.
      // If the option is disabled, the MD5 checksum will not be calculated.
      int tmp_ret = cos_add_content_md5_from_buffer(ctx->options, &buffer, headers);
      if (COSE_OK != tmp_ret) {
        ret = OB_OBJECT_STORAGE_IO_ERROR;
        cos_warn_log("[COS]fail to add content md5, ret=%d, tmp_ret=%d\n", ret, tmp_ret);
      // append interface, do not retry
      } else if (nullptr == (cos_ret = cos_append_object_from_buffer(ctx->options, &bucket, &object,
         offset, &buffer, headers, &resp_headers)) || !cos_status_is_ok(cos_ret)) {
        convert_io_error(cos_ret, ret);
        cos_warn_log("[COS]fail to append object from buffer to cos, ret=%d\n", ret);
        log_status(cos_ret, ret);
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
  is_exist = true;

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
    cos_str_set(&bucket, bucket_name.data_);
    cos_str_set(&object, object_name.data_);
    cos_table_t *resp_headers = NULL;
    cos_status_t *cos_ret = NULL;
    char *file_length_ptr = NULL;
    char *last_modified_ptr = NULL;

    const char COS_CONTENT_LENGTH[] = "Content-Length";
    const char COS_LAST_MODIFIED[] = "Last-Modified";
    const char COS_OBJECT_TYPE[] = "x-cos-object-type";
    ObStorageCOSRetryStrategy strategy;

    if (OB_ISNULL(cos_ret = execute_until_timeout(
            strategy, cos_head_object,
            ctx->options, &bucket, &object, nullptr/*headers*/, &resp_headers))
        || !cos_status_is_ok(cos_ret)) {
      if (OB_NOT_NULL(cos_ret) && COS_OBJECT_NOT_EXIST == cos_ret->code) {
        is_exist = false;
      } else {
        convert_io_error(cos_ret, ret);
        cos_warn_log("[COS]fail to get file meta, ret=%d.\n", ret);
        log_status(cos_ret, ret);
      }
    }

    if (OB_SUCCESS == ret && is_exist) {
      char *object_type = (char*)(apr_table_get(resp_headers, COS_OBJECT_TYPE));
      if (NULL != object_type) {
        if (0 == strncmp(COS_OBJECT_TYPE_APPENDABLE, object_type, strlen(COS_OBJECT_TYPE_APPENDABLE))) {
          meta.type_ = CosObjectMeta::CosObjectType::COS_OBJ_APPENDABLE;
        } else if (0 == strncmp(COS_OBJECT_TYPE_NORMAL, object_type, strlen(COS_OBJECT_TYPE_NORMAL))) {
          meta.type_ = CosObjectMeta::CosObjectType::COS_OBJ_NORMAL;
        } else {
          ret = OB_OBJECT_STORAGE_IO_ERROR;
          cos_warn_log("[COS]unknown cos object type, ret=%d.\n", ret);
        }
      }

      is_exist = true;
      // get object length
      if (OB_SUCCESS != ret) {
      } else if (NULL != (file_length_ptr = (char*)apr_table_get(resp_headers, COS_CONTENT_LENGTH))) {
        // enhance verification
        // not a valid file length, start with a non-digit character
        if (0 == isdigit(*file_length_ptr)) {
          ret = OB_OBJECT_STORAGE_IO_ERROR;
          cos_warn_log("[COS]not a valid file length, something wrong unexpected, ret=%d, file_length_ptr=%s.\n", ret, file_length_ptr);
        } else {
          char *end;
          meta.file_length_ = strtoll(file_length_ptr, &end, 10);
          // not a valid file length, end with a non-digit character
          if (0 != *end) {
            ret = OB_OBJECT_STORAGE_IO_ERROR;
            cos_warn_log("[COS]not a valid file length, something wrong unexpected, ret=%d, file_length_ptr=%s.\n", ret, file_length_ptr);
          }
        }
      }

      // get object last modified time
      if (OB_SUCCESS != ret) {
      } else if (NULL != (last_modified_ptr = (char*)apr_table_get(resp_headers, COS_LAST_MODIFIED))) {
        meta.last_modified_ts_ = ob_strtotime(last_modified_ptr);
      } else {
        ret = OB_OBJECT_STORAGE_IO_ERROR;
        cos_warn_log("[COS]fail to get last modified from apr table, something wrong unexpected, ret=%d.\n", ret);
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
    cos_str_set(&bucket, bucket_name.data_);
    cos_str_set(&object, object_name.data_);
    cos_table_t *resp_headers = nullptr;
    cos_status_t *cos_ret = nullptr;
    ObStorageCOSRetryStrategy strategy;

    if (nullptr == (cos_ret = execute_until_timeout(
            strategy, cos_delete_object,
            ctx->options, &bucket, &object, &resp_headers))
        || !cos_status_is_ok(cos_ret)) {
      convert_io_error(cos_ret, ret);
      cos_warn_log("[COS]fail to delete object, ret=%d\n", ret);
      log_status(cos_ret, ret);
    }
  }

  return ret;
}

int ObCosWrapper::batch_del(
    Handle *h,
    const CosStringBuffer &bucket_name,
    const CosStringBuffer *objects_to_delete_list,
    const char **succeed_deleted_objects_list,
    int64_t *succeed_deleted_objects_len_list,
    const int64_t n_objects_to_delete,
    int64_t &n_succeed_deleted_objects)
{
  int ret = OB_SUCCESS;
  n_succeed_deleted_objects = 0;
  CosContext *ctx = reinterpret_cast<CosContext *>(h);
  if (nullptr == h) {
    ret = OB_INVALID_ARGUMENT;
    cos_warn_log("[COS]cos handle is null, ret=%d\n", ret);
  } else if (bucket_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    cos_warn_log("[COS]bucket name is null, ret=%d\n", ret);
  } else if (nullptr == objects_to_delete_list || nullptr == succeed_deleted_objects_list || nullptr == succeed_deleted_objects_len_list) {
    ret = OB_INVALID_ARGUMENT;
    cos_warn_log("[COS]objects list are null, ret=%d, to_delete=%p, succees_deleted=%p, succeed_deleted_len=%p\n",
        ret, objects_to_delete_list, succeed_deleted_objects_list, succeed_deleted_objects_len_list);
  } else if (0 >= n_objects_to_delete) {
    ret = OB_INVALID_ARGUMENT;
    cos_warn_log("[COS]n_objects_to_delete invalid, ret=%d, n_objects_to_delete=%ld\n",
        ret, n_objects_to_delete);
  } else {
    cos_status_t *cos_ret = nullptr;
    cos_table_t *resp_headers = nullptr;
    cos_string_t bucket;
    int is_quiet = COS_FALSE;
    cos_list_t object_list;
    cos_list_t deleted_object_list;
    cos_list_init(&object_list);
    cos_list_init(&deleted_object_list);
    cos_str_set(&bucket, bucket_name.data_);

    for (int64_t i = 0; OB_SUCCESS == ret && i < n_objects_to_delete; i++) {
      if (nullptr == objects_to_delete_list[i].data_ || objects_to_delete_list[i].size_ <= 0) {
        ret = OB_INVALID_ARGUMENT;
        cos_warn_log("[COS]object name is null, ret=%d, object=%p, object_len=%ld, i=%ld\n",
            ret, objects_to_delete_list[i].data_, objects_to_delete_list[i].size_, i);
      } else {
        cos_object_key_t *content = cos_create_cos_object_key(ctx->options->pool);
        if (OB_SUCCESS != (ret = ob_cos_str_assign(content->key, objects_to_delete_list[i].size_, objects_to_delete_list[i].data_))) {
        cos_warn_log("[COS]fail to assign object, ret=%d, object=%p, object_len=%ld, i=%ld\n",
            ret, objects_to_delete_list[i].data_, objects_to_delete_list[i].size_, i);
        }
        cos_list_add_tail(&content->node, &object_list);
      }
    }

    ObStorageCOSRetryStrategy strategy;
    if (FAILEDx(strategy.set_retry_deleted_object_list(&deleted_object_list))) {
        cos_warn_log("[COS]fail to set deleted_object_list, ret=%d, bucket=%s\n",
            ret, bucket_name.data_);
    } else if (OB_ISNULL(cos_ret = execute_until_timeout(
            strategy, cos_delete_objects,
            ctx->options, &bucket, &object_list, is_quiet,
            &resp_headers, &deleted_object_list))
        || !cos_status_is_ok(cos_ret)) {
      convert_io_error(cos_ret, ret);
      cos_warn_log("[COS]fail to delete objects, ret=%d\n", ret);
      log_status(cos_ret, ret);
    } else {
      cos_object_key_t *object_key = nullptr;
      cos_list_for_each_entry(cos_object_key_t, object_key, &deleted_object_list, node) {
        if (nullptr == object_key->key.data || 0 >= object_key->key.len) {
          ret = OB_OBJECT_STORAGE_IO_ERROR;
          cos_warn_log("[COS]returned object key is null, ret=%d, key=%p, key_len=%ld\n",
              ret, object_key->key.data, object_key->key.len);
          log_status(cos_ret, ret);
        } else if (n_succeed_deleted_objects >= n_objects_to_delete) {
          ret = OB_ERR_UNEXPECTED;
          cos_warn_log("[COS]succeed deleted objects num unexpected, ret=%d, key=%s, n_succeed_deleted_objects=%ld, n_objects_to_delete=%ld\n",
              ret, object_key->key.data, n_succeed_deleted_objects, n_objects_to_delete);
        } else {
          succeed_deleted_objects_list[n_succeed_deleted_objects] = object_key->key.data;
          succeed_deleted_objects_len_list[n_succeed_deleted_objects] = object_key->key.len;
          n_succeed_deleted_objects++;
          cos_debug_log("[COS]succeed deleting object, key=%s\n", object_key->key.data);
        }

        if (OB_SUCCESS != ret) {
          break;
        }
      } // end cos_list_for_each_entry
    }
  }
  return ret;
}

int ObCosWrapper::tag(
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
    cos_str_set(&bucket, bucket_name.data_);
    cos_str_set(&object, object_name.data_);
    cos_table_t *resp_headers = NULL;
    cos_status_t *cos_ret = NULL;
    cos_tagging_params_t *params = NULL;
    cos_tagging_tag_t *tag = NULL;

    if (NULL == (params = cos_create_tagging_params(ctx->mem_pool))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      cos_warn_log("[COS]fail to create tagging params, ret=%d\n", ret);
    } else if (NULL == (tag = cos_create_tagging_tag(ctx->mem_pool))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      cos_warn_log("[COS]fail to create tagging tag, ret=%d\n", ret);
    } else {
      cos_str_set(&tag->key, "delete_mode");
      cos_str_set(&tag->value, "tagging");
      cos_list_add_tail(&tag->node, &params->node);
      ObStorageCOSRetryStrategy strategy;

      if (nullptr == (cos_ret = execute_until_timeout(
              strategy, cos_put_object_tagging,
              ctx->options, &bucket, &object, nullptr, nullptr, params, &resp_headers))
          || !cos_status_is_ok(cos_ret)) {
        convert_io_error(cos_ret, ret);
        cos_warn_log("[COS]fail to tag object, ret=%d\n", ret);
        log_status(cos_ret, ret);
      }
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
  } else if (NULL == dir_name.data_ || min_dir_name_str_len >= dir_name.size_) {
    ret = OB_INVALID_ARGUMENT;
    cos_warn_log("[COS]dir_name size too short, size=%d, ret=%d\n", dir_name.size_, ret);
  } else if (dir_name.data_[dir_name.size_ - 2] != *seperator) {
    ret = OB_INVALID_ARGUMENT;
    cos_warn_log("[COS]dir_name format not right, dir=%s, ret=%d\n", dir_name.data_, ret);
  } else {
    cos_status_t *cos_ret = NULL;
    cos_list_object_params_t *params = NULL;

    if (NULL == (params = cos_create_list_object_params(ctx->mem_pool))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      cos_warn_log("[COS]fail to create list object params, ret=%d\n", ret);
    } else {
      cos_str_set(&params->prefix, dir_name.data_);
      cos_str_set(&params->marker, "/");
      params->max_ret = OB_STORAGE_LIST_MAX_NUM;
      cos_string_t bucket;
      cos_str_set(&bucket, bucket_name.data_);
      cos_list_object_content_t *content = NULL;

      cos_list_t to_delete_object_list;
      cos_list_t deleted_object_list;
      cos_object_key_t *to_delete_object = NULL;
      cos_table_t *resp_headers = NULL;

      cos_list_init(&to_delete_object_list);
      cos_list_init(&deleted_object_list);

      do {
        // List objects from cos, limit OB_STORAGE_LIST_MAX_NUM.
        cos_ret = cos_list_object(ctx->options, &bucket, params, NULL);
        if (NULL == cos_ret || !cos_status_is_ok(cos_ret)) {
          convert_io_error(cos_ret, ret);
          cos_warn_log("[COS]fail to list object, ret=%d\n", ret);
          log_status(cos_ret, ret);
        } else {
          // Traverse the returned objects
          cos_list_for_each_entry(cos_list_object_content_t, content, &params->object_list, node) {
            // Check if the prefix of returned object key match the dir_name
            size_t dir_name_str_len = dir_name.get_safe_str_len();
            if (NULL == content->key.data) {
              ret = OB_OBJECT_STORAGE_IO_ERROR;
              cos_warn_log("[COS]returned object key data is null, dir=%s, ret=%d\n", dir_name.data_, ret);
            } else if (0 != memcmp(content->key.data, dir_name.data_, dir_name_str_len)) {
              ret = OB_OBJECT_STORAGE_IO_ERROR;
              cos_warn_log("[COS]returned object prefix not match, dir=%s, object=%s, ret=%d\n", dir_name.data_, content->key.data, ret);
            } else if (NULL == (to_delete_object = cos_create_cos_object_key(ctx->mem_pool))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              cos_warn_log("[COS]create to delete object key memory failed, dir=%s, object=%s, ret=%d\n", dir_name.data_, content->key.data, ret);
            } else {
              // Mark object to do delete
              if (OB_SUCCESS != (ret = ob_cos_str_assign(to_delete_object->key, content->key.len, content->key.data))) {
                cos_warn_log("[COS]fail to assign object, ret=%d, object=%p, object_len=%ld\n", ret, content->key.data, content->key.len);
              } else {
                cos_list_add_tail(&to_delete_object->node, &to_delete_object_list);
              }
            }

            if (OB_SUCCESS != ret) {
              break;
            }
          }

          // Delete current batch of objects, limit OB_STORAGE_LIST_MAX_NUM.
          if (OB_SUCCESS == ret && !cos_list_empty(&to_delete_object_list)) {
            if (NULL == (cos_ret = cos_delete_objects(ctx->options, &bucket, &to_delete_object_list, COS_FALSE, &resp_headers, &deleted_object_list))
                || !cos_status_is_ok(cos_ret)) {
              convert_io_error(cos_ret, ret);
              cos_warn_log("[COS]delete objects failed, ret=%d.\n", ret);
              log_status(cos_ret, ret);
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
            if (nullptr == params->next_marker.data || params->next_marker.len == 0) {
              ret = OB_OBJECT_STORAGE_IO_ERROR;
              cos_warn_log("[COS]returned next marker is invalid, data=%s, len=%d, ret=%d\n",
                  params->next_marker.data, params->next_marker.len, ret);
            } else if (NULL == (next_marker_str = apr_psprintf(ctx->mem_pool, "%.*s",
                                                               params->next_marker.len,
                                                               params->next_marker.data))) {
              ret = OB_OBJECT_STORAGE_IO_ERROR;
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
      } while (COS_TRUE == params->truncated && OB_SUCCESS == ret);
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
    cos_str_set(&bucket, bucket_name.data_);
    cos_str_set(&object, object_name.data_);
    cos_table_t *resp_headers = NULL;
    cos_status_t *cos_ret = NULL;

    cos_table_t *headers = NULL;
    cos_copy_object_params_t *params = NULL;
    if (NULL == (headers = cos_table_make(ctx->mem_pool, 2))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      cos_warn_log("[COS]fail to allocate header memory, ret=%d\n", ret);
    } else if (NULL == (params = cos_create_copy_object_params(ctx->mem_pool))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      cos_warn_log("[COS]fail to allocate copy object param memory, ret=%d\n", ret);
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
        convert_io_error(cos_ret, ret);
        cos_warn_log("[COS]fail to call copy object, ret=%d\n", ret);
        log_status(cos_ret, ret);
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
    const int64_t offset,
    char *buf,
    const int64_t buf_size,
    const bool is_range_read,
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
    cos_str_set(&bucket, bucket_name.data_);
    cos_str_set(&object, object_name.data_);
    cos_table_t *headers = NULL;
    cos_table_t *resp_headers = NULL;
    cos_status_t *cos_ret = NULL;

    cos_list_t buffer;
    cos_list_init(&buffer);

    const int COS_RANGE_SIZE = 256;
    const char* const COS_RANGE_KEY = "Range";

    char range_size[COS_RANGE_SIZE];
    if (NULL == (headers = cos_table_make(ctx->mem_pool, 1))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      cos_warn_log("[COS]fail to make cos headers, ret=%d\n", ret);
    } else {
      if (is_range_read) {
        // Cos read range of [10, 100] include the start offset 10 and the end offset 100.
        // But what we except is [10, 100) which does not include the end.
        // So we subtract 1 from the end.
        int n = snprintf(range_size, COS_RANGE_SIZE, "bytes=%ld-%ld", offset, offset + buf_size - 1);
        if (0 >= n || COS_RANGE_SIZE <= n) {
          ret = OB_SIZE_OVERFLOW;
          cos_warn_log("[COS]fail to format range size,n=%d, ret=%d\n", n, ret);
        } else {
          apr_table_set(headers, COS_RANGE_KEY, range_size);
        }
      }

      ObStorageCOSRetryStrategy strategy;
      if (FAILEDx(strategy.set_retry_headers(ctx->mem_pool, headers))) {
        cos_warn_log("[COS]fail to set headers, ret=%d, bucket=%s, object=%s\n",
            ret, bucket_name.data_, object_name.data_);
      } else if (OB_ISNULL(cos_ret = execute_until_timeout(
              strategy, cos_get_object_to_buffer,
              ctx->options, &bucket, &object, headers, nullptr, &buffer, &resp_headers))
          || !cos_status_is_ok(cos_ret)) {
        convert_io_error(cos_ret, ret);
        cos_warn_log("[COS]fail to get object to buffer, ret=%d\n", ret);
        log_status(cos_ret, ret);
      } else {
        read_size = 0;
        int64_t size = 0;
        int64_t buf_pos = 0;
        cos_buf_t *content = NULL;
        int64_t needed_size = -1;
        cos_list_for_each_entry(cos_buf_t, content, &buffer, node) {
          size = cos_buf_size(content);
          needed_size = size;
          if (buf_size - buf_pos < size) {
            needed_size = buf_size - buf_pos;
          }
          if (is_range_read && (buf_pos + size > buf_size)) {
            ret = OB_OBJECT_STORAGE_IO_ERROR;
            cos_warn_log("[COS]unexpected error, too much data returned, ret=%d, range_size=%s, buf_pos=%ld, size=%ld, req_id=%s.\n", ret, range_size, buf_pos, size, cos_ret->req_id);
            log_status(cos_ret, ret);
            break;
          } else if (NULL == content->pos) {
            ret = OB_OBJECT_STORAGE_IO_ERROR;
            cos_warn_log("[COS]unexpected error, data pos is null, ret=%d, range_size=%s, buf_pos=%ld, size=%ld, req_id=%s.\n", ret, range_size, buf_pos, size, cos_ret->req_id);
            log_status(cos_ret, ret);
            break;
          } else {
            // copy to buf
            memcpy(buf + buf_pos, content->pos, (size_t)needed_size);
            buf_pos += needed_size;
            read_size += needed_size;

            if (buf_pos >= buf_size) {
              break;
            }
          }
        } // end cos_list_for_each_entry
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
  return ObCosWrapper::pread(h, bucket_name, object_name,
                             0, buf, buf_size, false/*is_range_read*/, read_size);
}

int ObCosWrapper::is_object_tagging(
    Handle *h,
    const CosStringBuffer &bucket_name,
    const CosStringBuffer &object_name,
    bool &is_tagging)
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
    cos_str_set(&bucket, bucket_name.data_);
    cos_str_set(&object, object_name.data_);
    cos_table_t *resp_headers = nullptr;
    cos_status_t *cos_ret = nullptr;
    cos_string_t version_id = cos_string("");
    cos_tagging_params_t *result = nullptr;
    ObStorageCOSRetryStrategy strategy;

    if (OB_ISNULL(result = cos_create_tagging_params(ctx->mem_pool))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      cos_warn_log("[COS]fail to create tagging params, ret=%d\n", ret);
    } else if (OB_ISNULL(cos_ret = execute_until_timeout(
            strategy, cos_get_object_tagging,
            ctx->options, &bucket, &object, &version_id, nullptr/*headers*/, result, &resp_headers))
        || !cos_status_is_ok(cos_ret)) {
      convert_io_error(cos_ret, ret);
      cos_warn_log("[COS]fail to get object tagging, ret=%d\n", ret);
      log_status(cos_ret, ret);
    } else {
      cos_tagging_tag_t *tag = NULL;
      cos_list_for_each_entry(cos_tagging_tag_t, tag, &result->node, node) {
        char key_str[MAX_TAGGING_STR_LEN];
        char value_str[MAX_TAGGING_STR_LEN];

        if ((NULL != tag->key.data) && (NULL != tag->value.data)) {
          int key_n = snprintf(key_str, MAX_TAGGING_STR_LEN, "%.*s", tag->key.len, tag->key.data);
          int val_n = snprintf(value_str, MAX_TAGGING_STR_LEN, "%.*s", tag->key.len, tag->value.data);
          if (0 >= key_n || MAX_TAGGING_STR_LEN <= key_n) {
            ret = OB_SIZE_OVERFLOW;
            cos_warn_log("[COS]fail to format tag, key_n=%d, ret=%d\n", key_n, ret);
          } else if (0 >= val_n || MAX_TAGGING_STR_LEN <= val_n) {
            ret = OB_SIZE_OVERFLOW;
            cos_warn_log("[COS]fail to format tag, val_n=%d, ret=%d\n", val_n, ret);
          } else if (0 == strcmp("delete_mode", key_str) && 0 == strcmp("tagging", value_str)) {
            is_tagging = true;
          }
        }

        if (OB_SUCCESS != ret) {
          break;
        }
      }
    }
  }

  return ret;
}

struct CosListArguments
{
  CosListArguments(
      CosContext *ctx,
      const CosStringBuffer &bucket_name,
      const CosStringBuffer &full_dir_path,
      const char *next_marker,
      const char *delimiter,
      const int64_t max_ret)
      : ctx_(ctx), bucket_name_(bucket_name), full_dir_path_(full_dir_path),
        next_marker_(next_marker), delimiter_(delimiter), max_ret_(max_ret)
  {
  }

  int check_validity() const
  {
    int ret = OB_SUCCESS;
    if (nullptr == ctx_) {
      ret = OB_INVALID_ARGUMENT;
      cos_warn_log("[COS]cos context is null, ret=%d\n", ret);
    } else if (bucket_name_.empty()) {
      ret = OB_INVALID_ARGUMENT;
      cos_warn_log("[COS]bucket name is empty, ret=%d\n", ret);
    } else if (!full_dir_path_.is_null_or_end_with_slash()) {
      ret = OB_INVALID_ARGUMENT;
      cos_warn_log("[COS]full_dir_path format not right, dir=%s, dir_len=%ld, ret=%d\n",
          full_dir_path_.data_, full_dir_path_.size_, ret);
    } else if (max_ret_ <= 0 || max_ret_ > OB_STORAGE_LIST_MAX_NUM) {
      ret = OB_INVALID_ARGUMENT;
      cos_warn_log("[COS]max_ret invalid, max_ret=%ld, ret=%d\n", max_ret_, ret);
    }
    // 'next_marker' and 'delimiter' are not required parameters
    // for the 'cos_list_object' interface, so they can be NULL.
    return ret;
  }

  CosContext *ctx_;
  const CosStringBuffer &bucket_name_;
  const CosStringBuffer &full_dir_path_;
  const char *next_marker_;
  const char *delimiter_;
  const int64_t max_ret_;
};

static int do_list_(
    const CosListArguments &cos_list_args,
    cos_list_object_params_t *&params,
    cos_table_t **resp_headers)
{
  int ret = OB_SUCCESS;
  cos_string_t bucket;
  cos_status_t *cos_ret = nullptr;

  if (OB_SUCCESS != (ret = cos_list_args.check_validity())) {
    cos_warn_log("[COS]cos_list_args is invalid, ret=%d\n", ret);
  } else {
    if (nullptr == params) {
      if (nullptr == (params = cos_create_list_object_params(cos_list_args.ctx_->mem_pool))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        cos_warn_log("[COS]fail to create list object params, ret=%d\n", ret);
      }
    } else {
      // reuse params
      cos_list_init(&params->object_list);
      cos_list_init(&params->common_prefix_list);
    }


    if (OB_SUCCESS == ret) {
      params->max_ret = cos_list_args.max_ret_;
      cos_str_set(&bucket, cos_list_args.bucket_name_.data_);
      if (nullptr != cos_list_args.full_dir_path_.data_) {
        cos_str_set(&params->prefix, cos_list_args.full_dir_path_.data_);
      }
      if (nullptr != cos_list_args.next_marker_ && strlen(cos_list_args.next_marker_) > 0) {
        cos_str_set(&params->marker, cos_list_args.next_marker_);
      }
      if (nullptr != cos_list_args.delimiter_ && strlen(cos_list_args.delimiter_) > 0) {
        cos_str_set(&params->delimiter, cos_list_args.delimiter_);
      }
    }

    ObStorageCOSRetryStrategy strategy;
    if (FAILEDx(strategy.set_retry_list_object_params(params))) {
      cos_warn_log("[COS]fail to set list object params, ret=%d, bucekt=%s, full_dir_path=%s\n",
          ret, cos_list_args.bucket_name_.data_, cos_list_args.full_dir_path_.data_);
    } else if (OB_ISNULL(cos_ret = execute_until_timeout(
            strategy, cos_list_object,
            cos_list_args.ctx_->options, &bucket, params, resp_headers))
        || !cos_status_is_ok(cos_ret)) {
      convert_io_error(cos_ret, ret);
      cos_warn_log("[COS]fail to list object, ret=%d\n", ret);
      log_status(cos_ret, ret);
    }
  }

  return ret;
}

int ObCosWrapper::list_objects(
    Handle *h,
    const CosStringBuffer &bucket_name,
    const CosStringBuffer &full_dir_path,
    handleObjectNameFunc handle_object_name_f,
    void *arg)
{
  int ret = OB_SUCCESS;
  CosContext *ctx = reinterpret_cast<CosContext *>(h);
  CosListArguments cos_list_args(ctx, bucket_name, full_dir_path,
                                 NULL/*next_marker*/, NULL/*delimiter*/, OB_STORAGE_LIST_MAX_NUM);

  if (OB_SUCCESS != (ret = cos_list_args.check_validity())) {
    cos_warn_log("[COS]cos_list_args is invalid, ret=%d\n", ret);
  } else if (NULL == handle_object_name_f) {
    ret = OB_INVALID_ARGUMENT;
    cos_warn_log("[COS]handle_object_name_f is null, ret=%d\n", ret);
  } else {
    cos_list_object_params_t *params = NULL;
    cos_list_object_content_t *content = NULL;
    cos_table_t *resp_headers = NULL;

    CosListObjPara para;
    para.arg_ = arg;
    para.type_ = CosListObjPara::CosListType::COS_LIST_CB_ARG;

    do {
      if (OB_SUCCESS != (ret = do_list_(cos_list_args, params, &resp_headers))) {
        cos_warn_log("[COS]fail to do list, bucket=%s, path=%s, next_marker=%s, ret=%d\n",
            bucket_name.data_, full_dir_path.data_, cos_list_args.next_marker_, ret);
      } else {
        char *request_id = (char*)apr_table_get(resp_headers, "x-cos-request-id");
        // Traverse the returned objects
        cos_list_for_each_entry(cos_list_object_content_t, content, &params->object_list, node) {
          // check if the prefix of returned object key match the full_dir_path
          size_t full_dir_path_len = full_dir_path.get_safe_str_len();
          if (nullptr == content->key.data || 0 == content->key.len) {
            ret = OB_OBJECT_STORAGE_IO_ERROR;
            cos_warn_log("[COS]returned object key is invalid, dir=%s, requestid=%s, ret=%d\n", full_dir_path.data_, request_id, ret);
          } else if (false == full_dir_path.is_prefix_of(content->key.data, content->key.len)) {
            ret = OB_OBJECT_STORAGE_IO_ERROR;
            cos_warn_log("[COS]returned object prefix not match, dir=%s, object=%s, requestid=%s, ret=%d\n",
                full_dir_path.data_, content->key.data, request_id, ret);
          } else if (content->key.len == full_dir_path_len) {
            // skip
            cos_info_log("[COS]exist object path length is same with dir path length, dir=%s, object=%s, requestid=%s, ret=%d\n",
                full_dir_path.data_, content->key.data, request_id, ret);
          } else if (OB_SUCCESS != (ret = para.set_cur_obj_meta(content->key.data,
                                                                content->key.len,
                                                                content->size.data,
                                                                content->size.len))) {
            cos_warn_log("[COS]fail to set cur obj meta, ret=%d, obj_full_path=%s, full_path_size=%ld, obj_size_str=%s, obj_size_str_len=%ld, requestid=%s\n",
                ret, content->key.data, content->key.len, content->size.data, content->size.len, request_id);
          } else if (OB_SUCCESS != (ret = handle_object_name_f(para))) {
            cos_warn_log("[COS]handle object name failed, ret=%d, object=%s, requestid=%s\n",
                ret, content->key.data, request_id);
          }

          if (OB_SUCCESS != ret) {
            break;
          }
        }  // end cos_list_for_each_entry

        if (OB_SUCCESS == ret && COS_TRUE == params->truncated) {
          if (nullptr == params->next_marker.data || params->next_marker.len == 0) {
            ret = OB_OBJECT_STORAGE_IO_ERROR;
            cos_warn_log("[COS]returned next marker is invalid, data=%s, len=%d, ret=%d\n",
                params->next_marker.data, params->next_marker.len, ret);
          } else if (nullptr == (cos_list_args.next_marker_ = apr_psprintf(ctx->mem_pool, "%.*s",
                                                                           params->next_marker.len,
                                                                           params->next_marker.data))) {
            ret = OB_OBJECT_STORAGE_IO_ERROR;
            cos_warn_log("[COS]get next marker is null, ret=%d\n", ret);
          }
        }
      }
    } while (COS_TRUE == params->truncated && OB_SUCCESS == ret);
  }

  return ret;
}

int ObCosWrapper::list_part_objects(
    Handle *h,
    const CosStringBuffer &bucket_name,
    const CosStringBuffer &full_dir_path,
    const CosStringBuffer &next_marker,
    handleObjectNameFunc handle_object_name_f,
    void *arg)
{
  int ret = OB_SUCCESS;
  CosContext *ctx = reinterpret_cast<CosContext *>(h);
  CosListArguments cos_list_args(ctx, bucket_name, full_dir_path,
                                 next_marker.data_, NULL/*delimiter*/, OB_STORAGE_LIST_MAX_NUM);

  if (OB_SUCCESS != (ret = cos_list_args.check_validity())) {
    cos_warn_log("[COS]cos_list_args is invalid, ret=%d\n", ret);
  } else if (NULL == handle_object_name_f) {
    ret = OB_INVALID_ARGUMENT;
    cos_warn_log("[COS]handle_object_name_f is null, ret=%d\n", ret);
  } else {
    cos_list_object_params_t *params = NULL;
    cos_list_object_content_t *content = NULL;
    cos_table_t *resp_headers = NULL;

    CosListObjPara para;
    para.arg_ = arg;
    para.type_ = CosListObjPara::CosListType::COS_PART_LIST_CTX;

    if (OB_SUCCESS != (ret = do_list_(cos_list_args, params, &resp_headers))) {
      cos_warn_log("[COS]fail to do list, bucket=%s, path=%s, next_marker=%s, ret=%d\n",
          bucket_name.data_, full_dir_path.data_, cos_list_args.next_marker_, ret);
    } else {
      char *request_id = (char*)apr_table_get(resp_headers, "x-cos-request-id");
      // Traverse the returned objects
      cos_list_for_each_entry(cos_list_object_content_t, content, &params->object_list, node) {
        // check if the prefix of returned object key match the full_dir_path
        size_t full_dir_path_len = full_dir_path.get_safe_str_len();
        if (nullptr == content->key.data || content->key.len <= 0) {
            ret = OB_OBJECT_STORAGE_IO_ERROR;
            cos_warn_log("[COS]returned object key is invalid, dir=%s, requestid=%s, ret=%d\n",
                full_dir_path.data_, request_id, ret);
        } else if (false == full_dir_path.is_prefix_of(content->key.data, content->key.len)) {
          ret = OB_OBJECT_STORAGE_IO_ERROR;
          cos_warn_log("[COS]returned object prefix not match, dir=%s, object=%s, requestid=%s, ret=%d\n",
              full_dir_path.data_, content->key.data, request_id, ret);
        } else if (content->key.len == full_dir_path_len) {
          // skip
          cos_info_log("[COS]exist object path length is same with dir path length, dir=%s, object=%s, requestid=%s, ret=%d\n",
              full_dir_path.data_, content->key.data, request_id, ret);
        } else if (OB_SUCCESS != (ret = para.set_cur_obj_meta(content->key.data,
                                                              content->key.len,
                                                              content->size.data,
                                                              content->size.len))) {
          cos_warn_log("[COS]fail to set cur obj meta, ret=%d, obj_full_path=%s, full_path_size=%ld, obj_size_str=%s, obj_size_str_len=%ld, requestid=%s\n",
              ret, content->key.data, content->key.len, content->size.data, content->size.len, request_id);
        } else if (OB_SUCCESS != (ret = handle_object_name_f(para))) {
          cos_warn_log("[COS]handle object name failed, ret=%d, object=%s, requestid=%s\n",
              ret, content->key.data, request_id);
        }

        if (OB_SUCCESS != ret) {
          break;
        }
      } // end cos_list_for_each_entry

      if (OB_SUCCESS == ret) {
        para.finish_part_list_ = true;
        para.next_flag_ = (COS_TRUE == params->truncated);
        if (para.next_flag_) {
          para.next_token_ = params->next_marker.data;
          para.next_token_size_ = params->next_marker.len;
        }

        // Here, we invoke handle_object_name_f, just for flaging 'has_next' and saving 'next_token'
        // It won't handle the last object twice.
        ret = handle_object_name_f(para);
        if (OB_SUCCESS != ret){
          cos_warn_log("[COS]handle object name failed, ret=%d\n", ret);
        }
      }
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
  const char seperator = '/';
  CosContext *ctx = reinterpret_cast<CosContext *>(h);
  CosListArguments cos_list_args(ctx, bucket_name, dir_name,
                                 next_marker.data_, delimiter.data_, OB_STORAGE_LIST_MAX_NUM);

  if (OB_SUCCESS != (ret = cos_list_args.check_validity())) {
    cos_warn_log("[COS]cos_list_args is invalid, ret=%d\n", ret);
  } else if (delimiter.empty() || delimiter.data_[0] != seperator) {
    ret = OB_INVALID_ARGUMENT;
    cos_warn_log("[COS]delimiter is invalid, delimiter=%s, ret=%d\n", delimiter.data_);
  } else if (NULL == handle_directory_name_f) {
    ret = OB_INVALID_ARGUMENT;
    cos_warn_log("[COS]handle_directory_name_f is null, ret=%d\n", ret);
  } else {
    cos_list_object_params_t *params = NULL;
    cos_list_object_common_prefix_t *common_prefix = NULL;
    cos_table_t *resp_headers = NULL;

    do {
      if (OB_SUCCESS != (ret = do_list_(cos_list_args, params, &resp_headers))) {
        cos_warn_log("[COS]fail to do list, bucket=%s, path=%s, next_marker=%s, ret=%d\n",
            bucket_name.data_, dir_name.data_, cos_list_args.next_marker_, ret);
      } else {
        char *request_id = (char*)apr_table_get(resp_headers, "x-cos-request-id");
        // Traverse the returned objects
        cos_list_for_each_entry(cos_list_object_common_prefix_t, common_prefix, &params->common_prefix_list, node) {
          // For example,
          //       dir1
          //         --file1
          //         --dir11
          //            --file11
          // if we list directories in 'dir1', then full_dir_path == 'dir1/'
          // and listed_dir_full_path == 'dir1/dir11/', which represents the full directory path of 'dir11'
          const char *listed_dir_full_path = common_prefix->prefix.data;
          const int64_t listed_dir_full_path_len = common_prefix->prefix.len;
          // check if the prefix of returned object key match the dir_name
          const size_t dir_name_str_len = dir_name.get_safe_str_len();
          if (nullptr == listed_dir_full_path || listed_dir_full_path_len <= 0) {
            ret = OB_OBJECT_STORAGE_IO_ERROR;
            cos_warn_log("[COS]returned dirs is invalid, dir=%s, requestid=%s, ret=%d\n", dir_name.data_, request_id, ret);
          } else if (false == dir_name.is_prefix_of(listed_dir_full_path, listed_dir_full_path_len)) {
            ret = OB_OBJECT_STORAGE_IO_ERROR;
            cos_warn_log("[COS]returned object prefix not match, dir=%s, object=%s, requestid=%s, ret=%d, obj_path_len=%d, dir_name_len=%d\n",
                dir_name.data_, listed_dir_full_path, request_id, ret, listed_dir_full_path_len, dir_name.size_);
          } else if (seperator != listed_dir_full_path[listed_dir_full_path_len - 1]) {
            ret = OB_OBJECT_STORAGE_IO_ERROR;
            cos_warn_log("[COS]the data has no directory, dir=%s, object=%s, requestid=%s, ret=%d obj_len=%d\n",
                dir_name.data_, listed_dir_full_path, request_id, ret, listed_dir_full_path_len);
          } else {
            // Callback to handle the object name, it is a absolute path.
            // remove trailing '/'
            const int64_t listed_dir_name_len = listed_dir_full_path_len - 1 - dir_name_str_len;
            CosListObjPara::CosListType type = CosListObjPara::CosListType::COS_LIST_CB_ARG;
            if (OB_SUCCESS != (ret = handle_directory_name_f(arg, type,
                                                             listed_dir_full_path + dir_name_str_len,
                                                             listed_dir_name_len))) {
              // Something wrong happened when handle object name
              cos_warn_log("[COS]handle object name failed, ret=%d, object=%s, requestid=%s\n", ret, listed_dir_full_path, request_id);
            }
          }

          if (OB_SUCCESS != ret) {
            break;
          }
        } // end cos_list_for_each_entry

        if (OB_SUCCESS == ret && COS_TRUE == params->truncated) {
          if (nullptr == params->next_marker.data || params->next_marker.len == 0) {
            ret = OB_OBJECT_STORAGE_IO_ERROR;
            cos_warn_log("[COS]returned next marker is invalid, data=%s, len=%d, ret=%d\n",
                params->next_marker.data, params->next_marker.len, ret);
          } else if (nullptr == (cos_list_args.next_marker_ = apr_psprintf(ctx->mem_pool, "%.*s",
                                                                           params->next_marker.len,
                                                                           params->next_marker.data))) {
            ret = OB_OBJECT_STORAGE_IO_ERROR;
            cos_warn_log("[COS]get next marker is null, ret=%d\n", ret);
          }
        }
      }
    } while (COS_TRUE == params->truncated && OB_SUCCESS == ret);
  }

  return ret;
}

int ObCosWrapper::is_empty_directory(
    Handle *h,
    const CosStringBuffer &bucket_name,
    const CosStringBuffer &dir_name,
    bool &is_empty_dir)
{
  int ret = OB_SUCCESS;
  is_empty_dir = false;
  CosContext *ctx = reinterpret_cast<CosContext *>(h);
  // it just decides if it is not empty
  const int64_t max_ret = 1;
  CosListArguments cos_list_args(ctx, bucket_name, dir_name,
                                 NULL/*next_marker*/, NULL/*delimiter*/, max_ret);

  if (OB_SUCCESS != (ret = cos_list_args.check_validity())) {
    cos_warn_log("[COS]cos_list_args is invalid, ret=%d\n", ret);
  } else {
    cos_list_object_params_t *params = NULL;
    cos_table_t *resp_headers = NULL;

    if (OB_SUCCESS != (ret = do_list_(cos_list_args, params, &resp_headers))) {
      cos_warn_log("[COS]fail to do list, bucket=%s, path=%s, next_marker=%s, ret=%d\n",
          bucket_name.data_, dir_name.data_, cos_list_args.next_marker_, ret);
    } else {
      is_empty_dir = static_cast<bool>(cos_list_empty(&params->object_list));
    }
  }

  return ret;
}

int ObCosWrapper::add_part_info(
    Handle *h,
    void *complete_part_list_ptr,
    const int partnum,
    const char *etag_header_str)
{
  int ret = OB_SUCCESS;
  const char *partnum_str = nullptr;
  const char *etag_str = nullptr;
  cos_complete_part_content_t *complete_part_content = nullptr;
  CosContext *ctx = reinterpret_cast<CosContext *>(h);
  cos_list_t *complete_part_list = reinterpret_cast<cos_list_t *>(complete_part_list_ptr);

  if (nullptr == h) {
    ret = OB_INVALID_ARGUMENT;
    cos_warn_log("[COS]cos handle is null, ret=%d\n", ret);
  } else if (nullptr == complete_part_list_ptr) {
    ret = OB_INVALID_ARGUMENT;
    cos_warn_log("[COS]complete_part_list is null, ret=%d\n", ret);
  } else if (nullptr == etag_header_str) {
    ret = OB_INVALID_ARGUMENT;
    cos_warn_log("[COS]etag is null, ret=%d\n", ret);
  } else if (partnum < 1 || partnum > MAX_COS_PART_NUM) {
    ret = OB_INVALID_ARGUMENT;
    cos_warn_log("[COS]partnum is invalid, ret=%d, partnum=%d, max_limit=%ld\n",
        ret, partnum, MAX_COS_PART_NUM);
  } else if (nullptr == (complete_part_content = cos_create_complete_part_content(ctx->mem_pool))) {
    ret = OB_OBJECT_STORAGE_IO_ERROR;
    cos_warn_log("[COS]fail to create complete part content, ret=%d, pool=%p\n",
        ret, ctx->mem_pool);
  } else if (nullptr == (partnum_str = apr_psprintf(ctx->mem_pool, "%d", partnum))) {
    ret = OB_OBJECT_STORAGE_IO_ERROR;
    cos_warn_log("[COS]fail to construct partnum_str, ret=%d, pool=%p, partnum=%d\n",
        ret, ctx->mem_pool, partnum);
  } else if (nullptr == (etag_str = apr_pstrdup(ctx->mem_pool, etag_header_str))) {
    ret = OB_OBJECT_STORAGE_IO_ERROR;
    cos_warn_log("[COS]fail to construct etag_str, ret=%d, pool=%p, etag=%s\n",
        ret, ctx->mem_pool, etag_header_str);
  } else {
    cos_str_set(&complete_part_content->part_number, partnum_str);
    cos_str_set(&complete_part_content->etag, etag_str);
    cos_list_add_tail(&complete_part_content->node, complete_part_list);
  }
  return ret;
}

int ObCosWrapper::init_multipart_upload(
    Handle *h,
    const CosStringBuffer &bucket_name,
    const CosStringBuffer &object_name,
    char *&upload_id_str,
    void *&complete_part_list)
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
  } else if (nullptr == (complete_part_list = apr_palloc(ctx->mem_pool, sizeof(cos_list_t)))) {
    ret = OB_OBJECT_STORAGE_IO_ERROR;
    cos_warn_log("[COS]fail to alloc buf for complete_part_list, ret=%d, pool=%p\n",
        ret, ctx->mem_pool);
  } else {
    cos_list_init(static_cast<cos_list_t *>(complete_part_list));
    cos_string_t bucket;
    cos_string_t object;
    cos_str_set(&bucket, bucket_name.data_);
    cos_str_set(&object, object_name.data_);
    cos_string_t upload_id;

    cos_status_t *cos_ret = nullptr;
    cos_table_t *resp_headers = nullptr;
    ObStorageCOSRetryStrategy strategy;

    if (nullptr == (cos_ret = execute_until_timeout(
            strategy, cos_init_multipart_upload,
            ctx->options, &bucket, &object, &upload_id, nullptr, &resp_headers))
        || !cos_status_is_ok(cos_ret) || upload_id.len < 1) {
      convert_io_error(cos_ret, ret);
      cos_warn_log("[COS]fail to init multipart upload, ret=%d, upload_id_length=%d\n", ret, upload_id.len);
      log_status(cos_ret, ret);
    } else {
      const int64_t upload_id_len = upload_id.len + 1;
      upload_id_str = static_cast<char*>(ctx->custom_mem.customAlloc(ctx->custom_mem.opaque, upload_id_len));
      if (NULL == upload_id_str) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        cos_warn_log("[COS]fail to alloc upload_id mem, ret=%d, len=%d\n", ret, upload_id_len);
      } else {
        memcpy(upload_id_str, upload_id.data, upload_id.len);
        upload_id_str[upload_id.len] = '\0';
      }
    }
  }
  return ret;
}

int ObCosWrapper::upload_part_from_buffer(
    Handle *h,
    const CosStringBuffer &bucket_name,
    const CosStringBuffer &object_name,
    const CosStringBuffer &upload_id_str,
    const int part_num,
    const char *buf,
    const int64_t buf_size,
    const char *&etag_header_str)
{
  int ret = OB_SUCCESS;
  CosContext *ctx = reinterpret_cast<CosContext *>(h);
  etag_header_str = nullptr;

  if (NULL == h) {
    ret = OB_INVALID_ARGUMENT;
    cos_warn_log("[COS]cos handle is null, ret=%d\n", ret);
  } else if (bucket_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    cos_warn_log("[COS]bucket name is null, ret=%d\n", ret);
  } else if (object_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    cos_warn_log("[COS]object name is null, ret=%d\n", ret);
  } else if (upload_id_str.empty()) {
    ret = OB_INVALID_ARGUMENT;
    cos_warn_log("[COS]upload_id is null, ret=%d\n", ret);
  } else if (NULL == buf || buf_size < 1 || part_num < 1 || part_num > MAX_COS_PART_NUM) {
    ret = OB_INVALID_ARGUMENT;
    cos_warn_log("[COS]invalid buf/buf_size/part_num, ret=%d, buf_size=%d, part_num=%d\n",
                 ret, buf_size, part_num);
  } else {
    cos_string_t bucket;
    cos_string_t object;
    cos_string_t upload_id;
    cos_str_set(&bucket, bucket_name.data_);
    cos_str_set(&object, object_name.data_);
    cos_str_set(&upload_id, upload_id_str.data_);
    cos_status_t *cos_ret = NULL;
    cos_table_t *resp_headers = NULL;
    cos_list_t buffer;
    cos_buf_t *content = NULL;
    cos_list_init(&buffer);
    ObStorageCOSRetryStrategy strategy;

    if (nullptr == (content = cos_buf_pack(ctx->mem_pool, buf, static_cast<int32_t>(buf_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      cos_warn_log("[COS]fail to pack buf, ret=%d, buf_size=%d\n", ret, buf_size);
    } else {
      cos_list_add_tail(&content->node, &buffer);
      if (OB_FAIL(strategy.set_retry_buffer(&buffer))) {
        cos_warn_log("[COS]fail to set buffer, ret=%d, bucket=%s, object=%s, upload_id=%s\n",
            ret, bucket_name.data_, object_name.data_, upload_id_str.data_);
      } else if (OB_ISNULL(cos_ret = execute_until_timeout(
              strategy, cos_upload_part_from_buffer,
              ctx->options, &bucket, &object, &upload_id, part_num, &buffer, &resp_headers))
          || !cos_status_is_ok(cos_ret)) {
        convert_io_error(cos_ret, ret);
        cos_warn_log("[COS]fail to upload part to cos, ret=%d, part_num=%d\n", ret, part_num);
        log_status(cos_ret, ret);
      } else {
        etag_header_str = apr_table_get(resp_headers, "ETag");
        if (nullptr == etag_header_str) {
          ret = OB_OBJECT_STORAGE_IO_ERROR;
          cos_warn_log("[COS]returned etag is null, ret=%d\n", ret);
        }
      }
    }
  }
  return ret;
}

int ObCosWrapper::complete_multipart_upload(
    Handle *h,
    const CosStringBuffer &bucket_name,
    const CosStringBuffer &object_name,
    const CosStringBuffer &upload_id_str,
    void *complete_part_list_ptr)
{
  int ret = OB_SUCCESS;
  CosContext *ctx = reinterpret_cast<CosContext *>(h);
  cos_list_t *complete_part_list = reinterpret_cast<cos_list_t *>(complete_part_list_ptr);

  if (NULL == h) {
    ret = OB_INVALID_ARGUMENT;
    cos_warn_log("[COS]cos handle is null, ret=%d\n", ret);
  } else if (nullptr == complete_part_list_ptr) {
    ret = OB_INVALID_ARGUMENT;
    cos_warn_log("[COS]complete_part_list_ptr is null, ret=%d\n", ret);
  } else if (bucket_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    cos_warn_log("[COS]bucket name is null, ret=%d\n", ret);
  } else if (object_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    cos_warn_log("[COS]object name is null, ret=%d\n", ret);
  } else if (upload_id_str.empty()) {
    ret = OB_INVALID_ARGUMENT;
    cos_warn_log("[COS]upload_id is null, ret=%d\n", ret);
  } else {
    cos_string_t bucket;
    cos_string_t object;
    cos_string_t upload_id;
    cos_str_set(&bucket, bucket_name.data_);
    cos_str_set(&object, object_name.data_);
    cos_str_set(&upload_id, upload_id_str.data_);
    cos_status_t *cos_ret = nullptr;
    cos_table_t *resp_headers = nullptr;

    if (cos_list_empty(complete_part_list)) {
      // If 'complete' without uploading any data, COS will return the error
      // 'MalformedXML, The XML you provided was not well-formed or did not validate against our published schema'
      // write an empty object instead
      ret = put(h, bucket_name, object_name, "", 0);
      if (OB_SUCCESS != ret) {
        cos_warn_log("[COS]complete an empty multipart upload, but fail to write an empty object, ret=%d, upload_id=%s\n",
            ret, upload_id.data);
      }
    // complete interface, do not retry
    } else if (nullptr == (cos_ret = cos_complete_multipart_upload(
            ctx->options, &bucket, &object, &upload_id, complete_part_list,
            nullptr, &resp_headers))
        || !cos_status_is_ok(cos_ret)) {
      convert_io_error(cos_ret, ret);
      cos_warn_log("[COS]fail to complete multipart upload, ret=%d\n", ret);
      log_status(cos_ret, ret);
    }
  }
  return ret;
}

int ObCosWrapper::abort_multipart_upload(
    Handle *h,
    const CosStringBuffer &bucket_name,
    const CosStringBuffer &object_name,
    const CosStringBuffer &upload_id_str)
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
  } else if (upload_id_str.empty()) {
    ret = OB_INVALID_ARGUMENT;
    cos_warn_log("[COS]upload_id is null, ret=%d\n", ret);
  } else {
    cos_string_t bucket;
    cos_string_t object;
    cos_string_t upload_id;
    cos_str_set(&bucket, bucket_name.data_);
    cos_str_set(&object, object_name.data_);
    cos_str_set(&upload_id, upload_id_str.data_);
    cos_status_t *cos_ret = nullptr;
    ObStorageCOSRetryStrategy strategy;

    cos_table_t *resp_headers = nullptr;
    if (nullptr == (cos_ret = execute_until_timeout(
            strategy, cos_abort_multipart_upload,
            ctx->options, &bucket, &object, &upload_id, &resp_headers))
        || !cos_status_is_ok(cos_ret)) {
      convert_io_error(cos_ret, ret);
      if (OB_OBJECT_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        cos_warn_log("[COS]fail to abort multipart upload, ret=%d, bucket=%s, object=%s, upload_id=%s\n",
            ret, bucket_name.data_, object_name.data_, upload_id_str.data_);
        log_status(cos_ret, ret);
      }
    }
  }
  return ret;
}

int ObCosWrapper::del_unmerged_parts(
    Handle *h,
    const CosStringBuffer &bucket_name,
    const CosStringBuffer &object_name)
{
  int ret = OB_SUCCESS;
  CosContext *ctx = reinterpret_cast<CosContext *>(h);
  cos_list_multipart_upload_params_t *params = NULL;
  const char *next_key_marker = "";
  const char *next_upload_id_marker = "";

  if (NULL == h) {
    ret = OB_INVALID_ARGUMENT;
    cos_warn_log("[COS]cos handle is null, ret=%d\n", ret);
  } else if (bucket_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    cos_warn_log("[COS]bucket name is null, ret=%d\n", ret);
  } else if (object_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    cos_warn_log("[COS]object name is null, ret=%d\n", ret);
  } else if (NULL == (params = cos_create_list_multipart_upload_params(ctx->mem_pool))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    cos_warn_log("[COS]fail to create multipart upload params, ret=%d, bucket=%s\n",
        ret, bucket_name.data_);
  } else {
    cos_str_set(&params->prefix, object_name.data_);
    cos_str_set(&params->next_key_marker, next_key_marker);
    cos_str_set(&params->next_upload_id_marker, next_upload_id_marker);
    params->max_ret = OB_STORAGE_LIST_MAX_NUM;
    cos_string_t bucket;
    cos_string_t object;
    cos_str_set(&bucket, bucket_name.data_);
    cos_list_multipart_upload_content_t *content = NULL;
    cos_table_t *resp_headers = NULL;
    cos_status_t *cos_ret = NULL;

    do {
      // There is a bug in the 'cos_list_multipart_upload'
      // when parsing the response data to retrieve the upload id of multipart uploads
      // in the 'cos_xml.c:cos_list_multipart_uploads_content_parse' function,
      // the identifier being parsed should be 'UploadId' instead of 'UploadID'.
      // This issue results in the inability to correctly parse the upload id for multipart upload events.
      // This bug will be fixed in the next version of the cos SDK,
      // therefore, the del_unmerged_parts feature is currently not supported.
      if (NULL == (cos_ret = cos_list_multipart_upload(ctx->options, &bucket, params, &resp_headers))
          || !cos_status_is_ok(cos_ret)) {
        convert_io_error(cos_ret, ret);
        cos_warn_log("[COS]fail to list multipart uploads, ret=%d\n", ret);
        log_status(cos_ret, ret);
      } else {
        char *request_id = (char*)apr_table_get(resp_headers, "x-cos-request-id");
        cos_list_for_each_entry(cos_list_multipart_upload_content_t, content, &params->upload_list, node) {
          if (nullptr == content->key.data || nullptr == content->upload_id.data) {
            ret = OB_OBJECT_STORAGE_IO_ERROR;
            cos_warn_log("[COS]returned key or upload id is invalid, dir=%s, requestid=%s, ret=%d, key=%s, upload id=%s\n",
                object_name.data_, request_id, ret, content->key.data, content->upload_id.data);
          } else if (nullptr == (cos_ret = cos_abort_multipart_upload(ctx->options, &bucket,
                                                                      &(content->key),
                                                                      &(content->upload_id),
                                                                      &resp_headers))
              || !cos_status_is_ok(cos_ret)) {
            convert_io_error(cos_ret, ret);
            cos_warn_log("[COS]fail to abort multipart upload, ret=%d, bucket=%s, object=%s, upload_id=%s\n",
                ret, bucket_name.data_, content->key.data, content->upload_id.data);
            log_status(cos_ret, ret);
            break;
          } else {
            cos_info_log("[COS]succeed to abort multipart upload, bucket=%s, object=%s, upload_id=%s\n",
                bucket_name.data_, content->key.data, content->upload_id.data);
          }

          if (OB_SUCCESS != ret) {
            break;
          }
        }
      }

      if (OB_SUCCESS == ret && COS_TRUE == params->truncated) {
        if (nullptr == params->next_key_marker.data || nullptr == params->next_upload_id_marker.data
            || params->next_key_marker.len == 0 || params->next_upload_id_marker.len == 0) {
          ret = OB_OBJECT_STORAGE_IO_ERROR;
          cos_warn_log("[COS]returned key marker or upload id is invalid, key data=%s, key len=%d, upload id data=%s, upload id len=%d, ret=%d\n",
              params->next_key_marker.data, params->next_key_marker.len,
              params->next_upload_id_marker.data, params->next_upload_id_marker.len, ret);
        } else if (nullptr == (next_key_marker = apr_psprintf(ctx->mem_pool, "%.*s",
                                                              params->next_key_marker.len,
                                                              params->next_key_marker.data))) {
          ret = OB_OBJECT_STORAGE_IO_ERROR;
          cos_warn_log("[COS]get next key marker is null, ret=%d\n", ret);
        } else if (NULL == (next_upload_id_marker = apr_psprintf(ctx->mem_pool, "%.*s",
                                                                 params->next_upload_id_marker.len,
                                                                 params->next_upload_id_marker.data))) {
          ret = OB_OBJECT_STORAGE_IO_ERROR;
          cos_warn_log("[COS]get next upload id marker is null, ret=%d\n", ret);
        } else {
          cos_str_set(&params->key_marker, next_key_marker);
          cos_str_set(&params->upload_id_marker, next_upload_id_marker);
          cos_list_init(&params->upload_list);
        }
      }
    } while (COS_TRUE == params->truncated && OB_SUCCESS == ret);
  }
  return ret;
}

} // qcloud_cos
} // common
} // oceanbase
