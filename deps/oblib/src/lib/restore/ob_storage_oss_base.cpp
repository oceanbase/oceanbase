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

#include <stdlib.h>
#include <libgen.h>
#include "lib/restore/ob_storage.h"
#include "ob_storage_oss_base.h"
#include "common/ob_string_buf.h"
#include "apr_errno.h"
#include "ob_storage.h"
#include "aos_crc64.h"
#include "lib/hash/ob_hashset.h"
#include "lib/utility/ob_tracepoint.h"

namespace oceanbase
{
using namespace common;

namespace common
{

int init_oss_env()
{
  return ObOssEnvIniter::get_instance().global_init();
}

void fin_oss_env()
{
  // wait doing io finish before destroy oss env.
  const int64_t start_time = ObTimeUtility::current_time();
  const int64_t timeout = ObExternalIOCounter::FLYING_IO_WAIT_TIMEOUT;
  int64_t flying_io_cnt = ObExternalIOCounter::get_flying_io_cnt();
  while(0 < flying_io_cnt) {
    const int64_t end_time = ObTimeUtility::current_time();
    if (end_time - start_time > timeout) {
      OB_LOG(INFO, "force fin_oss_env", K(flying_io_cnt));
      break;
    }
    usleep(100 * 1000L); // 100ms
    flying_io_cnt = ObExternalIOCounter::get_flying_io_cnt();
  }

  ObOssEnvIniter::get_instance().global_destroy();
}

ObStorageOssStaticVar::ObStorageOssStaticVar()
  : compressor_(NULL),
    compress_type_(INVALID_COMPRESSOR)
{
}

ObStorageOssStaticVar::~ObStorageOssStaticVar()
{
  compressor_ = NULL;
  compress_type_ = INVALID_COMPRESSOR;
}

ObStorageOssStaticVar &ObStorageOssStaticVar::get_instance()
{
  static ObStorageOssStaticVar static_instance;
  return static_instance;
}

int ObStorageOssStaticVar::set_oss_compress_name(const char *name)
{
  int ret = OB_SUCCESS;
  char oss_compress_name[OB_MAX_COMPRESSOR_NAME_LENGTH] = "none";

  if (OB_ISNULL(name) || strlen(name) <= 0) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "argument is invalid", K(ret), KP(name), K(strlen(name)));
  } else if (strlen(name) >= OB_MAX_COMPRESSOR_NAME_LENGTH) {
    ret = OB_SIZE_OVERFLOW;
    OB_LOG(WARN, "compress name is too long", K(ret), K(strlen(name)), K(OB_MAX_COMPRESSOR_NAME_LENGTH));
  } else {
    int n = snprintf(oss_compress_name, OB_MAX_COMPRESSOR_NAME_LENGTH, "%s", name);
    if (n <= 0 || n >= OB_MAX_COMPRESSOR_NAME_LENGTH) {
      ret = OB_SIZE_OVERFLOW;
      OB_LOG(WARN, "compress name is overflow", K(ret), K(n), K(OB_MAX_COMPRESSOR_NAME_LENGTH), KCSTRING(name));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObCompressorPool::get_instance().get_compressor_type(oss_compress_name, compress_type_))) {
      OB_LOG(WARN, "fail to get compress func type", K(ret), KCSTRING(oss_compress_name));
    } else if (OB_FAIL(ObCompressorPool::get_instance().get_compressor(oss_compress_name, compressor_))) {
      OB_LOG(WARN, "fail to get compress func name", K(ret), KCSTRING(oss_compress_name));
    }
  }

  return ret;
}

ObCompressor *ObStorageOssStaticVar::get_oss_compressor()
{
  return compressor_;
}

ObCompressorType ObStorageOssStaticVar::get_compressor_type()
{
  return compress_type_;
}

template <typename AllocatorT>
int get_bucket_object_name(const ObString &uri, ObString &bucket, ObString &object,
    AllocatorT &allocator)
{
  int ret = OB_SUCCESS;
  ObString tmp_bucket;
  ObString tmp_object;
  ObString::obstr_size_t bucket_start = 0;
  ObString::obstr_size_t bucket_end = 0;
  ObString::obstr_size_t object_start = 0;
  char *buf = NULL;

  if (!uri.prefix_match(OB_OSS_PREFIX)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "uri is invalid ", KCSTRING(OB_OSS_PREFIX), K(uri), K(ret));
  } else {
    bucket_start = static_cast<ObString::obstr_size_t>(strlen(OB_OSS_PREFIX));
    for (int64_t i = bucket_start; OB_SUCC(ret) && i < uri.length() - 2; i++) {
      if ('/' == *(uri.ptr() + i) && '/' == *(uri.ptr() + i + 1)) {
        ret = OB_INVALID_ARGUMENT;
        OB_LOG(WARN, "uri has two // ", K(uri), K(ret), K(i));
        break;
      }
    }
  }
  //get the bucket name
  if (OB_SUCC(ret)) {
    if (bucket_start >= uri.length()) {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "bucket name is NULL", K(uri), K(ret));
    } else {
      for (bucket_end = bucket_start; OB_SUCC(ret) && bucket_end < uri.length(); ++bucket_end) {
        if ('/' == *(uri.ptr() + bucket_end)) {
          ObString::obstr_size_t bucket_length = bucket_end - bucket_start;
          if (OB_ISNULL(buf = reinterpret_cast<char *>(allocator.alloc(OB_MAX_URI_LENGTH)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            OB_LOG(WARN,"allocate memory error", K(OB_MAX_URI_LENGTH), K(ret));
          } else {
            //must end with '\0'
            int n = snprintf(buf, OB_MAX_URI_LENGTH, "%.*s", bucket_length,
                uri.ptr() + bucket_start);
            if (n <= 0 || n >= OB_MAX_URI_LENGTH) {
              ret = OB_SIZE_OVERFLOW;
              OB_LOG(WARN, "fail to deep copy bucket", K(uri), K(bucket_start), K(bucket_length),
                  K(n), K(OB_MAX_URI_LENGTH), K(ret));
            } else {
              bucket.assign_ptr(buf, n + 1);//must include '\0'
            }
            break;
          }
        }
        if (OB_SUCC(ret) && bucket_end == uri.length()) {
          ret = OB_INVALID_ARGUMENT;
          OB_LOG(WARN, "bucket name is invalid", K(uri), K(ret));
        }
      }
    }
  }
  //get object name
  if (OB_SUCC(ret)) {
    if (bucket_end + 1 >= uri.length()) {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "object name is NULL", K(uri), K(ret));
    } else {
      object_start = bucket_end + 1;
      ObString::obstr_size_t object_length = uri.length() - object_start;
      if (OB_ISNULL(buf = reinterpret_cast<char *>(allocator.alloc(OB_MAX_URI_LENGTH)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        OB_LOG(WARN, "fail to allocator memory", K(OB_MAX_URI_LENGTH), K(ret));
      } else {
        //must end with '\0'
        int n = snprintf(buf, OB_MAX_URI_LENGTH, "%.*s", object_length, uri.ptr() + object_start);
        if (n <= 0 || n >= OB_MAX_URI_LENGTH) {
          ret = OB_SIZE_OVERFLOW;
          OB_LOG(WARN, "fail to deep copy object", K(uri), K(object_start), K(object_length),
              K(n), K(OB_MAX_URI_LENGTH), K(ret));
        } else {
          object.assign_ptr(buf, n + 1);//must include '\0'
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    OB_LOG(DEBUG, "get bucket object name ", K(uri), K(bucket), K(object));
  }

  return ret;
}

static void convert_io_error(aos_status_t *aos_ret, int &ob_errcode)
{
  if (OB_ISNULL(aos_ret)) {
    ob_errcode = OB_OSS_ERROR;
  } else if (!aos_status_is_ok(aos_ret)) {
    switch (aos_ret->code) {
      case OSS_PERMISSION_DENIED: {
        ob_errcode = OB_BACKUP_PERMISSION_DENIED;
        break;
      }

      case OSS_OBJECT_NOT_EXIST: {
        ob_errcode = OB_BACKUP_FILE_NOT_EXIST;
        break;
      }

      case OSS_OBJECT_PWRITE_OFFSET_NOT_MATH: {
        ob_errcode = OB_BACKUP_PWRITE_OFFSET_NOT_MATCH;
        break;
      }

      case OSS_LIMIT_EXCEEDED: {
        ob_errcode = OB_IO_LIMIT;
        break;
      }

      case OSS_BAD_REQUEST: {
        if (OB_ISNULL(aos_ret->error_code)) {
          ob_errcode = OB_OSS_ERROR;
        } else if (0 == STRCMP("InvalidDigest", aos_ret->error_code)) {
          ob_errcode = OB_CHECKSUM_ERROR;
        } else if (0 == STRCMP("InvalidBucketName", aos_ret->error_code)) {
          ob_errcode = OB_INVALID_OBJECT_STORAGE_ENDPOINT;
        } else {
          ob_errcode = OB_OSS_ERROR;
        }
        break;
      }

      default: {
        ob_errcode = OB_OSS_ERROR;
        break;
      }
    } // end swtich
  }
}

template <typename AllocatorT>
int get_bucket_object_name_for_list(const ObString &uri, ObString &bucket, ObString &object,
    AllocatorT &allocator)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  if (OB_FAIL(get_bucket_object_name(uri, bucket, object, allocator))) {
    OB_LOG(WARN, "get bucket object name error", K(uri), K(ret));
  } else if (object.length() <= OSS_INVALID_OBJECT_LENGTH) {
    ret = OB_URI_ERROR;
    OB_LOG(WARN, "uri has invaild object", K(uri), K(object), K(ret));
  } else {
    //object not end with '/', object last is '\0'
    const char last = *(object.ptr() + object.length() - 1 - 1);
    if ('/' == last) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "the uri is invalid", K(uri), K(last), K(ret));
    } else {
      if (OB_ISNULL(buf = reinterpret_cast<char *>(allocator.alloc(OB_MAX_URI_LENGTH)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        OB_LOG(WARN,"allocator memory error", K(OB_MAX_URI_LENGTH), K(ret));
      } else {
        int n = snprintf(buf, OB_MAX_URI_LENGTH,"%.*s/", object.length(), object.ptr());
        if (n <= 0 || n >= OB_MAX_URI_LENGTH){
          ret = OB_SIZE_OVERFLOW;
          OB_LOG(WARN, "generate object error", K(object), K(n), K(ret));
        } else {
          object.assign_ptr(buf, n);
        }
      }
    }
  }
  return ret;
}

ObOssEnvIniter::ObOssEnvIniter()
  : lock_(common::ObLatchIds::OBJECT_DEVICE_LOCK),
    is_global_inited_(false)
{
}

ObOssEnvIniter &ObOssEnvIniter::get_instance()
{
  static ObOssEnvIniter initer;
  return initer;
}

int ObOssEnvIniter::global_init()
{
  int ret = OB_SUCCESS;
  int aos_ret = AOSE_OK;

  common::SpinWLockGuard guard(lock_);

  if (is_global_inited_) {
    ret = OB_INIT_TWICE;
    OB_LOG(WARN, "cannot init twice", K(ret));
  } else if (AOSE_OK != (aos_ret = aos_http_io_initialize(NULL, 0))) {
    ret = OB_OSS_ERROR;
    OB_LOG(WARN, "fail to init aos", K(aos_ret));
  } else {
    is_global_inited_ = true;
  }
  return ret;
}

void ObOssEnvIniter::global_destroy()
{
  common::SpinWLockGuard guard(lock_);
  if (is_global_inited_) {
    aos_http_io_deinitialize();
    is_global_inited_ = false;
  }
}


void ObOssAccount::reset_account()
{
  memset(oss_domain_, 0, MAX_OSS_ENDPOINT_LENGTH);
  memset(oss_id_, 0, MAX_OSS_ID_LENGTH);
  memset(oss_key_, 0, MAX_OSS_KEY_LENGTH);
  delete_mode_ = ObIStorageUtil::DELETE;
  is_inited_ = false;
}

ObOssAccount::ObOssAccount()
{
  reset_account();
}

ObOssAccount::~ObOssAccount()
{
  reset_account();
}

ObStorageOssBase::ObStorageOssBase()
  :aos_pool_(NULL),
   oss_option_(NULL),
   is_inited_(false),
   oss_account_(),
   checksum_type_(ObStorageChecksumType::OB_MD5_ALGO)
{
  memset(oss_endpoint_, 0, MAX_OSS_ENDPOINT_LENGTH);
}

ObStorageOssBase::~ObStorageOssBase()
{
  reset();
}

void ObStorageOssBase::reset()
{
  memset(oss_endpoint_, 0, MAX_OSS_ENDPOINT_LENGTH);
  if (is_inited_) {
    if (NULL != aos_pool_) {
      aos_pool_destroy(aos_pool_);
      aos_pool_ = NULL;
    }
    oss_option_ = NULL;
    is_inited_ = false;
    checksum_type_ = ObStorageChecksumType::OB_MD5_ALGO;
  }
}

int ObStorageOssBase::init_with_storage_info(common::ObObjectStorageInfo *storage_info)
{
  int ret = OB_SUCCESS;
  char info_str[common::OB_MAX_BACKUP_STORAGE_INFO_LENGTH] = { 0 };

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    OB_LOG(WARN, "oss client init twice", K(ret));
  } else if (OB_ISNULL(storage_info) || OB_UNLIKELY(!storage_info->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "oss account is invalid, fail to init oss base!", K(ret), KPC(storage_info));
  } else if (OB_FAIL(storage_info->get_storage_info_str(info_str, sizeof(info_str)))) {
    OB_LOG(WARN, "fail to get storage info str", K(ret), KPC(storage_info));
  } else if (OB_FAIL(oss_account_.parse_oss_arg(info_str))) {
    OB_LOG(WARN, "fail to build oss account", K(ret));
  } else if (OB_FAIL(init_oss_options(aos_pool_, oss_option_))) {
    OB_LOG(WARN, "fail to init oss options", K(aos_pool_), K(oss_option_), K(ret));
  } else if (OB_ISNULL(aos_pool_) || OB_ISNULL(oss_option_)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "aos pool or oss option is NULL", K(aos_pool_), K(oss_option_));
  } else {
    checksum_type_ = storage_info->get_checksum_type();
    if (OB_UNLIKELY(!is_oss_supported_checksum(checksum_type_))) {
      ret = OB_CHECKSUM_TYPE_NOT_SUPPORTED;
      OB_LOG(WARN, "that checksum algorithm is not supported for oss", K(ret), K_(checksum_type));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObOssAccount::set_delete_mode(const char *parameter)
{
  int ret = OB_SUCCESS;
  if (NULL == parameter) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid args", K(ret), KP(parameter));
  } else if (0 == strcmp(parameter, "delete")) {
    delete_mode_ = ObIStorageUtil::DELETE;
  } else if (0 == strcmp(parameter, "tagging")) {
    delete_mode_ = ObIStorageUtil::TAGGING;
  } else {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "delete mode is invalid", K(ret), K(parameter));
  }
  return ret;
}

int ObOssAccount::parse_oss_arg(const common::ObString &storage_info)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    OB_LOG(WARN, "oss client init twice", K(ret));
  } else if (OB_ISNULL(storage_info.ptr()) || storage_info.length() >= OB_MAX_URI_LENGTH) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "uri is too long", K(ret), K(storage_info.length()));
  } else {
    // host=xxxx&access_id=xxx&access_key=xxx
    char tmp[OB_MAX_URI_LENGTH];
    char *token = NULL;
    char *saved_ptr = NULL;
    const char *HOST = "host=";
    const char *ACCESS_ID = "access_id=";
    const char *ACCESS_KEY = "access_key=";
    const char *DELETE_MODE = "delete_mode=";

    MEMCPY(tmp, storage_info.ptr(), storage_info.length());
    tmp[storage_info.length()] = '\0';
    token = tmp;
    for (char *str = token; OB_SUCC(ret); str = NULL) {
      token = ::strtok_r(str, "&", &saved_ptr);
      if (NULL == token) {
        break;
      } else if (0 == strncmp(HOST, token, strlen(HOST))) {
        if (OB_FAIL(set_oss_field(token + strlen(HOST), oss_domain_, sizeof(oss_domain_)))) {
          OB_LOG(WARN, "failed to set oss_domain", K(ret), KCSTRING(token));
        }
      } else if (0 == strncmp(ACCESS_ID, token, strlen(ACCESS_ID))) {
        if (OB_FAIL(set_oss_field(token + strlen(ACCESS_ID), oss_id_, sizeof(oss_id_)))) {
          OB_LOG(WARN, "failed to set oss_id_", K(ret), KCSTRING(token));
        }
      } else if (0 == strncmp(ACCESS_KEY, token, strlen(ACCESS_KEY))) {
        if (OB_FAIL(set_oss_field(token + strlen(ACCESS_KEY), oss_key_, sizeof(oss_key_)))) {
          OB_LOG(WARN, "failed to set oss_key_", K(ret));
        }
      } else if (0 == strncmp(DELETE_MODE, token, strlen(DELETE_MODE))) {
        if (OB_FAIL(set_delete_mode(token + strlen(DELETE_MODE)))) {
          OB_LOG(WARN, "failed to set delete mode", K(ret), K(token)); 
        }
      } else {
        OB_LOG(DEBUG, "unkown oss info", K(*token), KP(storage_info.ptr()));
      }
    }

    if (strlen(oss_domain_) == 0 || strlen(oss_id_) == 0 || strlen(oss_key_) == 0) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "failed to parse oss info",
          K(ret), KCSTRING(oss_domain_), KCSTRING(oss_id_));
    } else {
      STORAGE_LOG(DEBUG, "success to parse oss info",
          K(ret), KCSTRING(oss_domain_), KCSTRING(oss_id_));
    }
  }
  return ret;
}

int ObOssAccount::set_oss_field(const char *info, char *field, const int64_t length)
{
  int ret = OB_SUCCESS;

  if (NULL == info || NULL == field) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid args", K(ret), KP(info), KP(field));
  } else {
    const int64_t info_len = strlen(info);
    if (info_len >= length) {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "info is too long ", K(ret), K(info_len), K(length));
    } else {
      MEMCPY(field, info, info_len);
      field[info_len] = '\0';
    }
  }
  return ret;
}
/* only used by pread and init. Initialize one aos_pol and oss_option. Other methods shouldn't call this method.
 * pread need alloc memory on aos_pol and release after read finish */
int ObStorageOssBase::init_oss_options(aos_pool_t *&aos_pool, oss_request_options_t *&oss_option)
{
  int ret = OB_SUCCESS;
  int apr_ret = APR_SUCCESS;
  if (APR_SUCCESS != (apr_ret = aos_pool_create(&aos_pool, NULL)) || NULL == aos_pool) {
    ret = OB_OSS_ERROR;
    OB_LOG(WARN, "fail to create apr pool", K(apr_ret), K(aos_pool), K(ret));
  } else if (OB_ISNULL(oss_option = oss_request_options_create(aos_pool))) {
    ret = OB_OSS_ERROR;
    OB_LOG(WARN, "fail to init option ", K(oss_option), K(ret));
  } else if (OB_ISNULL(oss_option->config = oss_config_create(oss_option->pool))) {
    ret = OB_OSS_ERROR;
    OB_LOG(WARN, "fail to create oss config", K(ret));
  } else if (OB_FAIL(init_oss_endpoint())) {
    OB_LOG(WARN, "fail to init oss endpoind", K(ret));
  } else if (OB_ISNULL(oss_option->ctl = aos_http_controller_create(oss_option->pool, 0))) {
    ret = OB_OSS_ERROR;
    OB_LOG(WARN, "fail to create aos http controller", K(ret));
    // A separate instance of ctl->options is now allocated for each request,
    // ensuring that disabling CRC checks is a request-specific action
    // and does not impact the global setting for OSS request options.
  } else if (OB_ISNULL(oss_option->ctl->options = aos_http_request_options_create(oss_option->pool))) {
    ret = OB_OSS_ERROR;
    OB_LOG(WARN, "fail to create aos http request options", K(ret));
  } else {
    aos_str_set(&oss_option->config->endpoint, oss_endpoint_);
    aos_str_set(&oss_option->config->access_key_id, oss_account_.oss_id_);
    aos_str_set(&oss_option->config->access_key_secret, oss_account_.oss_key_);
    oss_option->config->is_cname = 0;

    // Set connection timeout, the default value is 10s
    oss_option->ctl->options->connect_timeout = 60;
    // Set DNS timeout, the default value is 60s
    oss_option->ctl->options->dns_cache_timeout = 120;
    // The default value, which means that if the transmission rate for 15 consecutive seconds is less than 1K, it will time out
    // To prevent overtime, reduce the minimum tolerable rate and increase the maximum tolerable time
    // Control the minimum rate that can be tolerated, the default is 1024, which is 1K
    // oss_option->ctl->options->speed_limit = 256;
    // The maximum time that the control can tolerate, the default is 15 seconds
    oss_option->ctl->options->speed_limit = 16000;
    oss_option->ctl->options->speed_time = 60;

    oss_option->ctl->options->enable_crc = false;
  }
  return ret;
}

int ObStorageOssBase::reinit_oss_option()
{
  int ret = OB_SUCCESS;

  if (!is_inited()) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "please init first", K(ret));
  } else if (OB_FAIL(init_oss_endpoint())) {
    OB_LOG(WARN, "fail to init oss endpoint", K(ret));
  } else {
    //reset oss_option endpoint
    aos_str_set(&oss_option_->config->endpoint, oss_endpoint_);
  }
  return ret;
}

int ObStorageOssBase::init_oss_endpoint()
{
  int ret = OB_SUCCESS;
  if (0 == strlen(oss_account_.oss_domain_)) {
    ret = OB_OSS_ERROR;
    OB_LOG(WARN, "oss domain is empty", K(ret));
  } else {
    int64_t str_len = strlen(oss_account_.oss_domain_);
    if (str_len >= sizeof(oss_endpoint_)) {
      ret = OB_BUF_NOT_ENOUGH;
      OB_LOG(WARN, "oss domain is too long", K(ret), KCSTRING(oss_account_.oss_domain_));
    } else {
      MEMCPY(oss_endpoint_, oss_account_.oss_domain_, str_len);
      oss_endpoint_[str_len] = '\0';
    }
  }

  return ret;
}

bool ObStorageOssBase::is_inited()
{
  return is_inited_;
}

int ObStorageOssBase::get_oss_file_meta(const ObString &bucket_ob_string,
                                        const ObString &object_ob_string,
                                        bool &is_file_exist,
                                        char *&remote_md5,
                                        int64_t &file_length)
{
  int ret = OB_SUCCESS;
  is_file_exist = false;
  remote_md5 = NULL;
  file_length = -1;

  if (!is_inited()) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "oss client not inited", K(ret));
  } else if (OB_ISNULL(bucket_ob_string.ptr()) || OB_ISNULL(object_ob_string.ptr())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), K(bucket_ob_string), K(object_ob_string));
  } else {
    aos_string_t bucket;
    aos_string_t object;
    aos_str_set(&bucket, bucket_ob_string.ptr());
    aos_str_set(&object, object_ob_string.ptr());
    aos_table_t *resp_headers = NULL;
    aos_status_t *aos_ret = NULL;
    aos_table_t *headers = NULL;
    char *file_length_ptr = NULL;

    if (OB_ISNULL(aos_ret = oss_head_object(oss_option_, &bucket, &object, headers, &resp_headers)) || !aos_status_is_ok(aos_ret)) {
      if (OB_NOT_NULL(aos_ret) && OSS_OBJECT_NOT_EXIST == aos_ret->code) {
        is_file_exist = false;
      } else {
        convert_io_error(aos_ret, ret);
      }
      OB_LOG(WARN, "fail to head object", K(bucket_ob_string), K(object_ob_string), K(ret));
      print_oss_info(resp_headers, aos_ret, ret);
    } else {
      is_file_exist = true;
      //get md5
      remote_md5 = (char*)apr_table_get(resp_headers, OSS_META_MD5);
      //get file length
      if (OB_ISNULL(file_length_ptr = (char*)apr_table_get(resp_headers, OSS_CONTENT_LENGTH))) {
        ret = OB_OSS_ERROR;
        OB_LOG(WARN, "fail to get file length string from response", K(ret));
      } else if (OB_FAIL(ob_atoll(file_length_ptr, file_length))) {
        OB_LOG(WARN, "fail to convert string to int64", K(ret), K(file_length_ptr), K(file_length));
      }

      if (OB_SUCC(ret) && file_length < 0) {
        ret = OB_OSS_ERROR;
        OB_LOG(WARN, "fail to get object length", K(bucket_ob_string), K(object_ob_string), K(file_length), K(ret));
      }
    }
  }

  return ret;
}

void ObStorageOssBase::print_oss_info(
    aos_table_t *resp_headers,
    aos_status_s *aos_ret,
    const int ob_errcode)
{
  int ret = OB_SUCCESS;
  char *delay_time_number = NULL;
  char delay_time[OB_MAX_TIME_STR_LENGTH] = { 0 };
  if (NULL != aos_ret) {
    if (OB_NOT_NULL(resp_headers)) {
      delay_time_number = (char*)apr_table_get(resp_headers, "x-oss-qos-delay-time");
      if (OB_NOT_NULL(delay_time_number)) {
        if (OB_FAIL(databuff_printf(delay_time, sizeof(delay_time), "%s ms", delay_time_number))) {
          OB_LOG(WARN, "fail to print delay time", K(ret), K(delay_time_number));
        }
      }
    }
    if (OB_CHECKSUM_ERROR == ob_errcode) {
      OB_LOG_RET(ERROR, OB_CHECKSUM_ERROR, "oss info ", K(aos_ret->code), KCSTRING(aos_ret->error_code),
        KCSTRING(aos_ret->error_msg), KCSTRING(aos_ret->req_id),  KCSTRING(delay_time),
        KCSTRING(oss_account_.oss_domain_), KCSTRING(oss_endpoint_), KCSTRING(oss_account_.oss_id_));
    } else {
      OB_LOG_RET(WARN, OB_SUCCESS, "oss info ", K(aos_ret->code), KCSTRING(aos_ret->error_code),
        KCSTRING(aos_ret->error_msg), KCSTRING(aos_ret->req_id),  KCSTRING(delay_time),
        KCSTRING(oss_account_.oss_domain_), KCSTRING(oss_endpoint_), KCSTRING(oss_account_.oss_id_));
    }
  } else {
    OB_LOG_RET(WARN, OB_SUCCESS, "oss info ", KCSTRING(oss_account_.oss_domain_), KCSTRING(oss_endpoint_), KCSTRING(oss_account_.oss_id_));
  }
}

ObStorageOssMultiPartWriter::ObStorageOssMultiPartWriter()
  : mod_(ObModIds::BACKUP),
    allocator_(ModuleArena::DEFAULT_PAGE_SIZE, mod_),
    base_buf_(NULL),
    base_buf_pos_(0),
    bucket_(),
    object_(),
    partnum_(0),
    is_opened_(false),
    file_length_(-1)
{
  upload_id_.len = -1;
  upload_id_.data = NULL;
}

ObStorageOssMultiPartWriter::~ObStorageOssMultiPartWriter()
{
}

int ObStorageOssMultiPartWriter::open(const ObString &uri, common::ObObjectStorageInfo *storage_info)
{
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;
  if (uri.empty()) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "uri is empty", K(ret), K(uri));
  } else if (is_opened_) {
    ret = OB_OSS_ERROR;
    OB_LOG(WARN, "already open, cannot open again", K(ret));
  } else if (OB_FAIL(init_with_storage_info(storage_info))) {
    OB_LOG(WARN, "failed to init oss", K(ret));
  } else if (OB_FAIL(get_bucket_object_name(uri, bucket_, object_, allocator_))) {
    OB_LOG(WARN, "get bucket object error", K(uri), K(ret));
  } else {
    aos_string_t bucket;
    aos_string_t object;
    aos_str_set(&bucket, bucket_.ptr());
    aos_str_set(&object, object_.ptr());
    aos_table_t *headers = NULL;
    aos_table_t *resp_headers = NULL;
    aos_status_t *aos_ret = NULL;

    if (OB_ISNULL(base_buf_ = static_cast<char *>(allocator_.alloc(OSS_BASE_BUFFER_SIZE)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        OB_LOG(WARN, "fail to alloc memory", K(OSS_BASE_BUFFER_SIZE), K(ret));
    } else if (OB_ISNULL(headers = aos_table_make(aos_pool_, AOS_TABLE_INIT_SIZE))) {
      ret = OB_OSS_ERROR;
      OB_LOG(WARN, "fail to make apr table", K(ret));
    } else if (NULL == (aos_ret = oss_init_multipart_upload(oss_option_, &bucket, &object, &upload_id_,
                                                           headers, &resp_headers))
              || !aos_status_is_ok(aos_ret)) {
      convert_io_error(aos_ret, ret);
      OB_LOG(WARN, "oss init multipart upload error", K(uri), K(ret));
      print_oss_info(resp_headers, aos_ret, ret);
    } else if (OB_ISNULL(upload_id_.data) || OB_UNLIKELY(upload_id_.len <= 0)) {
      ret = OB_OSS_ERROR;
      OB_LOG(WARN, "upload id is invalid", K(ret), KP(upload_id_.data), K(upload_id_.len));
    } else {
      is_opened_ = true;
      base_buf_pos_ = 0;
      file_length_ = 0;
    }
  }
  return ret;
}

int ObStorageOssMultiPartWriter::write(const char * buf,const int64_t size)
{
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;
  int64_t fill_size = 0;
  int64_t buf_pos = 0;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "oss client not inited", K(ret));
  } else if (NULL == buf || size < 0) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "buf is NULL or size is invalid", KP(buf), K(size), K(ret));
  } else if (!is_opened_) {
    ret = OB_OSS_ERROR;
    OB_LOG(WARN, "oss writer cannot write before it is opened", K(ret));
  }

  while (OB_SUCC(ret) && buf_pos != size) {
    fill_size = std::min(OSS_BASE_BUFFER_SIZE - base_buf_pos_, size - buf_pos);
    memcpy(base_buf_ + base_buf_pos_, buf + buf_pos, fill_size);
    base_buf_pos_ += fill_size;
    buf_pos += fill_size;
    if (base_buf_pos_ == OSS_BASE_BUFFER_SIZE) {
      if (OB_FAIL(write_single_part())) {
        OB_LOG(WARN, "write file error", K(bucket_), K(object_), K(ret));
      } else {
        base_buf_pos_ = 0;
      }
    }
  }

  if (OB_SUCCESS == ret) {
    file_length_ += size;
  }
  return ret;
}

int ObStorageOssMultiPartWriter::pwrite(const char *buf, const int64_t size, const int64_t offset)
{
  UNUSED(offset);
  return write(buf, size);
}

static int add_content_md5(oss_request_options_t *options, const char *buf, const int64_t size, aos_table_t *headers)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(options) || OB_ISNULL(buf) || OB_ISNULL(headers) || OB_UNLIKELY(size < 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid arguments", K(ret), KP(options), KP(buf), K(size), KP(headers));
  } else {
    unsigned char *md5 = nullptr;
    char *b64_value = nullptr;    // store the base64-encoded MD5 value
    const int in_len = APR_MD5_DIGESTSIZE + 1;  // including trailing '\0'
    // Calculate the buffer size needed for the base64-encoded string including the null terminator.
    // Base64 encoding represents every 3 bytes of input with 4 bytes of output,
    // so allocate enough space based on this ratio and add extra byte for the null terminator.
    const int b64_buf_len = (in_len + 1) * 4 / 3 + 1;

    if (OB_ISNULL(md5 = aos_md5(options->pool, buf, (apr_size_t)size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      OB_LOG(WARN, "fail to calculate content md5", K(ret), K(size));
    } else if (OB_ISNULL(b64_value = (char *)aos_pcalloc(options->pool, b64_buf_len))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      OB_LOG(WARN, "fail to alloc memory for content md5 base64 value buf", K(ret), K(b64_buf_len));
    } else {
      int b64_len = aos_base64_encode(md5, in_len, b64_value);
      b64_value[b64_len] = '\0';
      apr_table_set(headers, OSS_CONTENT_MD5, b64_value);
    }
  }
  return ret;
}

int ObStorageOssMultiPartWriter::write_single_part()
{
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;

  ++partnum_;
  //cal part num begin from 0
  if(partnum_ > OSS_MAX_PART_NUM) {
    ret = OB_OUT_OF_ELEMENT;
    OB_LOG(WARN, "Out of oss element ", K(partnum_), K(OSS_MAX_PART_NUM), K(ret));
  } else if (!is_inited()) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "please init first", K(ret));
  } else if (!is_opened_) {
    ret = OB_OSS_ERROR;
    OB_LOG(WARN, "write oss should open first", K(ret));
  }

  if (OB_SUCCESS == ret) {
    //upload data
    aos_string_t bucket;
    aos_string_t object;
    aos_str_set(&bucket, bucket_.ptr());
    aos_str_set(&object, object_.ptr());
    aos_table_t *headers = nullptr;
    aos_table_t *resp_headers = nullptr;
    aos_status_t *aos_ret = nullptr;
    aos_list_t buffer;
    aos_buf_t *content = nullptr;
    aos_list_init(&buffer);

    const int64_t start_time = ObTimeUtility::current_time();
    if (OB_ISNULL(headers = aos_table_make(aos_pool_, AOS_TABLE_INIT_SIZE))) {
      ret = OB_OSS_ERROR;
      OB_LOG(WARN, "fail to make apr table", K(ret));
    } else if (OB_ISNULL(content = aos_buf_pack(aos_pool_, base_buf_, static_cast<int32_t>(base_buf_pos_)))) {
      ret = OB_OSS_ERROR;
      OB_LOG(WARN, "fail to pack buf", K(content), K(ret));
    } else if ((checksum_type_ == ObStorageChecksumType::OB_MD5_ALGO)
        && OB_FAIL(add_content_md5(oss_option_, base_buf_, base_buf_pos_, headers))) {
      OB_LOG(WARN, "fail to add content md5 when uploading part", K(ret));
    } else {
      aos_list_add_tail(&content->node, &buffer);

      if (NULL == (aos_ret = oss_do_upload_part_from_buffer(oss_option_, &bucket, &object,
                                                            &upload_id_, partnum_, &buffer, nullptr,
                                                            headers, nullptr, &resp_headers, nullptr))
          || !aos_status_is_ok(aos_ret)) {
        convert_io_error(aos_ret, ret);
        OB_LOG(WARN, "fail to upload one part from buffer",
            K_(base_buf_pos), K_(bucket), K_(object), K(ret));
        print_oss_info(resp_headers, aos_ret, ret);
      }
      bool is_slow = false;
      print_access_storage_log("oss upload one part ", object_, start_time, base_buf_pos_, &is_slow);
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCC(ret) && is_slow) {
        print_oss_info(resp_headers, aos_ret, ret);
        if (OB_SUCCESS != (tmp_ret = reinit_oss_option())) {
          OB_LOG(WARN, "fail to reinit oss option", K(tmp_ret));
          ret = tmp_ret;
        }
      }
    }
  }
  return ret;
}

int ObStorageOssMultiPartWriter::complete()
{
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;
  const int64_t start_time = ObTimeUtility::current_time();
  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "oss client not inited", K(ret));
  } else if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_OSS_ERROR;
    OB_LOG(WARN, "oss multipart writer cannot close before it is opened", K(ret));
  } else if (0 != base_buf_pos_) {//base_buf has data
    if (OB_FAIL(write_single_part())) {
      OB_LOG(WARN, "write the last size to oss error",
          K_(base_buf_pos), K_(bucket), K_(object), K(ret));
    } else {
      base_buf_pos_ = 0;
    }
  }

  if (OB_SUCC(ret)) {
    int64_t total_parts = 0;
    aos_string_t bucket;
    aos_string_t object;
    aos_str_set(&bucket, bucket_.ptr());
    aos_str_set(&object, object_.ptr());
    aos_table_t *resp_headers = NULL;
    aos_status_t *aos_ret = NULL;
    aos_table_t *complete_headers = NULL;
    aos_list_t complete_part_list;
    aos_list_init(&complete_part_list);
    oss_list_upload_part_params_t *params = NULL;
    oss_list_part_content_t *part_content = NULL;
    oss_complete_part_content_t *complete_part_content = NULL;

    //add all parts to complete_part_list
    if (OB_ISNULL(params = oss_create_list_upload_part_params(aos_pool_))) {
      ret = OB_OSS_ERROR;
      OB_LOG(WARN, "fail to create oss upload params", K_(bucket), K_(object), K(ret));
    } else {
      params->max_ret = OB_STORAGE_LIST_MAX_NUM;
      do {
        if (OB_ISNULL(aos_ret = oss_list_upload_part(oss_option_, &bucket, &object, &upload_id_,
            params, &resp_headers)) || !aos_status_is_ok(aos_ret)) {
          convert_io_error(aos_ret, ret);
          OB_LOG(WARN, "fail to list oss upload parts", K_(bucket), K_(object), K(ret));
          print_oss_info(resp_headers, aos_ret, ret);
        } else {
          aos_list_for_each_entry(oss_list_part_content_t, part_content, &params->part_list, node) {
            if (OB_ISNULL(complete_part_content = oss_create_complete_part_content(aos_pool_))) {
              ret = OB_OSS_ERROR;
              OB_LOG(WARN, "fail to create complete part content", K_(bucket), K_(object), K(ret));
            } else if (OB_ISNULL(part_content->part_number.data)
                      || OB_ISNULL(part_content->etag.data)) {
              ret = OB_OSS_ERROR;
              OB_LOG(WARN, "invalid part_number or etag",
                  K(ret), KP(part_content->part_number.data), KP(part_content->etag.data));
            } else {
              aos_str_set(&complete_part_content->part_number, part_content->part_number.data);
              aos_str_set(&complete_part_content->etag, part_content->etag.data);
              aos_list_add_tail(&complete_part_content->node, &complete_part_list);
            }

            if (OB_FAIL(ret)) {
              break;
            } else {
              total_parts++;
            }
          }

          if (OB_SUCC(ret) && AOS_TRUE == params->truncated) {
            const char *next_part_number_marker = NULL;
            if (OB_ISNULL(params->next_part_number_marker.data)
                || OB_UNLIKELY(params->next_part_number_marker.len <= 0)) {
              ret = OB_OSS_ERROR;
              OB_LOG(WARN, "next_part_number_marker is invalid", K(ret),
                  K(params->next_part_number_marker.data), K(params->next_part_number_marker.len));
            } else if (OB_ISNULL(next_part_number_marker =
                apr_psprintf(aos_pool_, "%.*s",
                             params->next_part_number_marker.len,
                             params->next_part_number_marker.data))) {
              ret = OB_OSS_ERROR;
              OB_LOG(WARN, "next part number marker is NULL", K(ret),
                  K(params->next_part_number_marker.data), K(params->next_part_number_marker.len));
            } else {
              aos_str_set(&params->part_number_marker, next_part_number_marker);
              aos_list_init(&params->part_list);
            }
          }
        }
      } while (OB_SUCC(ret) && AOS_TRUE == params->truncated);
    }

    //complete multipart upload
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(total_parts == 0)) {
      // If 'complete' without uploading any data, OSS will create an object with a size of 0
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "no parts have been uploaded!", K(ret), K(total_parts), K(upload_id_.data));
    } else {
      if (OB_ISNULL(aos_ret = oss_complete_multipart_upload(oss_option_, &bucket, &object, &upload_id_,
          &complete_part_list, complete_headers, &resp_headers)) || !aos_status_is_ok(aos_ret)) {
        convert_io_error(aos_ret, ret);
        OB_LOG(WARN, "fail to complete multipart upload", K_(bucket), K_(object), K(ret));
        print_oss_info(resp_headers, aos_ret, ret);
      }
    }

  }

  const int64_t total_cost_time = ObTimeUtility::current_time() - start_time;
  if (total_cost_time > 3 * 1000 * 1000) {
    OB_LOG_RET(WARN, OB_ERR_TOO_MUCH_TIME, "oss writer complete cost too much time",
        K(total_cost_time), K(ret));
  }

  return ret;
}

int ObStorageOssMultiPartWriter::close()
{
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;
  mod_.reset();
  base_buf_pos_ = 0;
  partnum_ = 0;
  base_buf_ = nullptr;
  is_opened_ = false;
  file_length_ = -1;
  upload_id_.len = -1;
  upload_id_.data = NULL;
  reset();
  return ret;
}

int ObStorageOssMultiPartWriter::abort()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "oss client not inited", K(ret));
  } else if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_OSS_ERROR;
    OB_LOG(WARN, "oss multipart writer cannot abort before it is opened", K(ret));
  } else {
    aos_string_t bucket;
    aos_string_t object;
    aos_str_set(&bucket, bucket_.ptr());
    aos_str_set(&object, object_.ptr());
    aos_table_t *resp_headers = NULL;
    aos_status_t *aos_ret = NULL;

    if (OB_ISNULL(aos_ret = oss_abort_multipart_upload(oss_option_, &bucket, &object,
        &upload_id_, &resp_headers)) || !aos_status_is_ok(aos_ret)) {
      convert_io_error(aos_ret, ret);
      OB_LOG(WARN, "Abort the multipart error", K_(bucket), K_(object), K(ret));
    }
  }
  return ret;
}

ObStorageOssReader::ObStorageOssReader():
    bucket_(),
    object_(),
    file_length_(-1),
    is_opened_(false),
    has_meta_(false),
    allocator_(OB_STORAGE_OSS_ALLOCATOR)
{
}

ObStorageOssReader::~ObStorageOssReader()
{
}

int ObStorageOssReader::open(const ObString &uri,
    common::ObObjectStorageInfo *storage_info, const bool head_meta)
{
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;
  bool is_file_exist = false;
  char *remote_md5 = NULL;
  int64_t file_length = -1;

  if (uri.empty()) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "uri is empty", K(ret));
  } else if (is_opened_) {
    ret = OB_OSS_ERROR;
    OB_LOG(WARN, "already open, cannot open again", K(ret));
  } else if (OB_FAIL(init_with_storage_info(storage_info))) {
    OB_LOG(WARN, "failed to init oss", K(ret));
  } else if (OB_SUCCESS != (ret = get_bucket_object_name(uri, bucket_, object_, allocator_))) {
    OB_LOG(WARN, "bucket name of object name is empty", K(ret));
  } else {
    if (head_meta) {
      if (OB_SUCCESS != (ret = get_oss_file_meta(bucket_, object_, is_file_exist,
          remote_md5, file_length))) {
        OB_LOG(WARN, "fail to get file meta", K(bucket_), K(object_), K(ret));
      } else if (!is_file_exist) {
        ret = OB_BACKUP_FILE_NOT_EXIST;
        OB_LOG(WARN, "backup file is not exist", K(bucket_), K(object_), K(ret));
      } else {
        file_length_ = file_length;
        has_meta_ = true;
      }
    }

    if (OB_SUCC(ret)) {
      is_opened_ = true;
    }
  }
  return ret;
}

int ObStorageOssReader::pread(
    char *buf,const int64_t buf_size, const int64_t offset, int64_t &read_size)
{
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;
  aos_pool_t *aos_pool = NULL;
  oss_request_options_t *oss_option = NULL;

  if (!is_inited()) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "oss client not inited", K(ret));
  } else if (OB_ISNULL(buf) || OB_UNLIKELY(buf_size <= 0 || offset < 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "buf is NULL or buf_size is invalid", K(buf_size), K(ret), KP(buf), K(offset));
  } else if (!is_opened_) {
    ret = OB_OSS_ERROR;
    OB_LOG(WARN, "oss reader cannot read before it is opened", K(ret));
  } else if (OB_FAIL(init_oss_options(aos_pool, oss_option))) {
    OB_LOG(WARN, "fail to init oss options", K(aos_pool), K(oss_option), K(ret));
  } else if (OB_ISNULL(aos_pool) || OB_ISNULL(oss_option)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "aos pool or oss option is NULL", K(aos_pool), K(oss_option), K(ret));
  } else {
    // When is_range_read is true, it indicates that only a part of the data is read.
    // When false, it indicates that the entire object is read
    bool is_range_read = true;
    int64_t get_data_size = buf_size;
    if (has_meta_) {
      if (file_length_ < offset) {
        ret = OB_FILE_LENGTH_INVALID;
        OB_LOG(WARN, "File lenth is invilid", K_(file_length),
            K(offset), K_(bucket), K_(object), K(ret));
      } else {
        get_data_size = MIN(buf_size, file_length_ - offset);
        if (get_data_size == file_length_) {
          // read entire object
          is_range_read = false;
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (get_data_size == 0) {
      read_size = 0;
    } else {
      const int64_t start_time = ObTimeUtility::current_time();
      aos_string_t bucket;
      aos_string_t object;
      aos_str_set(&bucket, bucket_.ptr());
      aos_str_set(&object, object_.ptr());
      aos_table_t *resp_headers = NULL;
      aos_status_t *aos_ret = NULL;
      aos_table_t *headers = NULL;
      aos_table_t *params = NULL;
      aos_list_t buffer;
      aos_buf_t *content = NULL;
      int64_t len = 0;
      int64_t size = 0;
      int64_t buf_pos = 0;
      const int64_t OSS_RANGE_SIZE = 256;
      const char *const OSS_RANGE_KEY = "Range";

      char range_size[OSS_RANGE_SIZE];
      if (NULL == (headers = aos_table_make(aos_pool, AOS_TABLE_INIT_SIZE))) {
        ret = OB_OSS_ERROR;
        OB_LOG(WARN, "fail to make oss headers", K(ret));
      } else {
        if (is_range_read) {
          // oss read size is [10, 100] include the 10 and 100 bytes
          // we except is [10, 100) not include the end, so we subtraction 1 to the end
          if (OB_FAIL(databuff_printf(range_size, OSS_RANGE_SIZE, "bytes=%ld-%ld",
                                      offset, offset + get_data_size - 1))) {
            OB_LOG(WARN, "fail to get range size", K(ret),
                K(offset), K(get_data_size), K(OSS_RANGE_SIZE));
          } else {
            apr_table_set(headers, OSS_RANGE_KEY, range_size);
          }
        }

        if (OB_FAIL(ret)) {
        } else if (NULL == (aos_ret = oss_get_object_to_buffer(oss_option, &bucket, &object, headers, params,
            &buffer, &resp_headers)) ||  !aos_status_is_ok(aos_ret)) {
          convert_io_error(aos_ret, ret);
          OB_LOG(WARN, "fail to get object to buffer", K_(bucket), K_(object), K(ret));
          print_oss_info(resp_headers, aos_ret, ret);
        } else {
          //check date len
          aos_list_for_each_entry(aos_buf_t, content, &buffer, node) {
            len += aos_buf_size(content);
          }
          // For appendable file, there may be data written in between the time when the reader is opened and pread is called.
          // At this point, the actual size of the file could be larger than the determined file length at the time of opening.
          // Therefore, if it's not a range read, the data read could exceed the expected amount to be read
          if (is_range_read && len > get_data_size) {
            ret = OB_OSS_ERROR;
            OB_LOG(WARN, "get data size error", K(get_data_size), K(len), K_(bucket),
                K_(object), K(ret), K_(has_meta), K(offset));
          } else {
            //copy to buf
            read_size = 0;
            int64_t needed_size = -1;
            aos_list_for_each_entry(aos_buf_t, content, &buffer, node) {
              size = aos_buf_size(content);
              needed_size = MIN(size, get_data_size - buf_pos);
              if (is_range_read && (buf_pos + size > get_data_size)) {
                ret = OB_SIZE_OVERFLOW;
                OB_LOG(WARN, "the size is too long", K(buf_pos), K(size), K(get_data_size), K(ret));
                break;
              } else if (OB_ISNULL(content->pos)) {
                ret = OB_OSS_ERROR;
                OB_LOG(WARN, "unexpected error, the data pos is null", K(size), K(ret));
              } else {
                memcpy(buf + buf_pos, content->pos, (size_t)needed_size);
                buf_pos += needed_size;
                read_size += needed_size;

                if (buf_pos >= get_data_size) {
                  break;
                }
              }

              if (OB_FAIL(ret)) {
                break;
              }
            } // end aos_list_for_each_entry
          }
        }
      }
      bool is_slow = false;
      print_access_storage_log("oss read one part ", object_, start_time, read_size, &is_slow);
      if (is_slow) {
        print_oss_info(resp_headers, aos_ret, ret);
      }
    }//if(file_length_ - file_offset_ > 0)
  }
  if (NULL != aos_pool) {
    aos_pool_destroy(aos_pool);
  }

  return ret;
}

int ObStorageOssReader::close()
{
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;
  is_opened_ = false;
  has_meta_ = false;
  file_length_ = -1;
  // Release memory
  allocator_.clear();
  reset();

  return ret;
}

ObStorageOssUtil::ObStorageOssUtil() :is_opened_(false), storage_info_(NULL)
{
}

ObStorageOssUtil::~ObStorageOssUtil()
{
}

int ObStorageOssUtil::open(common::ObObjectStorageInfo *storage_info)
{
  int ret = OB_SUCCESS;
  if (is_opened_) {
    ret = OB_OSS_ERROR;
    OB_LOG(WARN, "already open, cannot open again", K(ret));
  } else if (OB_ISNULL(storage_info)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "oss account is null", K(ret));
  } else {
    is_opened_ = true;
    storage_info_ = storage_info;
  }
  return ret;
}

void ObStorageOssUtil::close()
{
  is_opened_ = false;
  storage_info_ = NULL;
}

int ObStorageOssUtil::is_exist(const ObString &uri, bool &exist)
{
  int ret = OB_SUCCESS;
  exist = false;
  ObStorageObjectMetaBase obj_meta;
  if (OB_FAIL(head_object_meta(uri, obj_meta))) {
    OB_LOG(WARN, "fail to head object meta", K(uri), K(ret));
  } else {
    exist = obj_meta.is_exist_;
  }
  return ret;
}

int ObStorageOssUtil::get_file_length(const common::ObString &uri, int64_t &file_length)
{
  int ret = OB_SUCCESS;
  file_length = 0;
  ObStorageObjectMetaBase obj_meta;
  if (OB_FAIL(head_object_meta(uri, obj_meta))) {
    OB_LOG(WARN, "fail to head object meta", K(uri), K(ret));
  } else if (!obj_meta.is_exist_) {
    ret = OB_BACKUP_FILE_NOT_EXIST;
    OB_LOG(WARN, "backup file is not exist", K(uri), K(ret));
  } else {
    file_length = obj_meta.length_;
  }
  return ret;
}

int ObStorageOssUtil::head_object_meta(const common::ObString &uri, ObStorageObjectMetaBase &obj_meta)
{
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;
  ObString bucket_ob_string;
  ObString object_ob_string;
  common::ObArenaAllocator allocator(OB_STORAGE_OSS_ALLOCATOR);
  char *remote_md5 = NULL;
  ObStorageOssBase oss_base;

  if (uri.empty()) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "name is empty", K(ret));
  } else if (!is_opened_) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "oss util is not inited", K(ret), K(uri));
  } else if (OB_FAIL(oss_base.init_with_storage_info(storage_info_))) {
    OB_LOG(WARN, "fail to init oss base with account", K(ret), K(uri));
  } else if (OB_SUCCESS != (ret = get_bucket_object_name(uri, bucket_ob_string, object_ob_string, allocator))) {
    OB_LOG(WARN, "bucket or object name is empty", K(bucket_ob_string), K(object_ob_string), K(ret));
  } else if (OB_FAIL(oss_base.get_oss_file_meta(bucket_ob_string, object_ob_string, obj_meta.is_exist_,
             remote_md5, obj_meta.length_))) {
    OB_LOG(WARN, "fail to get file meta info", K(bucket_ob_string), K(object_ob_string), K(ret));
  }
  return ret;
}

int ObStorageOssUtil::write_single_file(
    const common::ObString &uri,
    const char *buf,
    const int64_t size)
{
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;
  ObStorageOssWriter writer;
  if (OB_FAIL(writer.open(uri, storage_info_))) {
    OB_LOG(WARN, "fail to open the writer(overwrite)", K(ret));
  } else if (OB_FAIL(writer.write(buf, size))) {
    OB_LOG(WARN, "fail to write the writer(overwrite)", K(ret), K(size), KP(buf));
  } else if (OB_FAIL(writer.close())) {
    OB_LOG(WARN, "fail to close the writer(overwrite)", K(ret));
  }
  return ret;
}

int ObStorageOssUtil::mkdir(const common::ObString &uri)
{
  int ret = OB_SUCCESS;
  UNUSED(uri);
  return ret;
}

int ObStorageOssUtil::is_tagging(
    const common::ObString &uri,
    bool &is_tagging)
{
  int ret = OB_SUCCESS;
  const int64_t OB_MAX_TAGGING_STR_LENGTH = 16;
  ObExternalIOCounterGuard io_guard;
  common::ObArenaAllocator allocator(OB_STORAGE_OSS_ALLOCATOR);
  ObString bucket_str;
  ObString object_str;
  ObStorageOssBase oss_base;
  if (uri.empty()) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "name is empty", K(ret));
  } else if (OB_FAIL(oss_base.init_with_storage_info(storage_info_))) {
    OB_LOG(WARN, "fail to init oss base with account", K(ret), K(uri));
  } else if (OB_FAIL(get_bucket_object_name(uri, bucket_str, object_str, allocator))) {
    OB_LOG(WARN, "bucket or object name is empty", K(ret), K(uri), K(bucket_str), K(object_str));
  } else {
    aos_string_t bucket;
    aos_string_t object;
    aos_table_t *head_resp_headers = NULL;
    aos_list_t tag_list;
    oss_tag_content_t *b;
    aos_status_t *aos_ret = NULL;
    aos_str_set(&bucket, bucket_str.ptr());
    aos_str_set(&object, object_str.ptr());
    aos_list_init(&tag_list);
    if (OB_ISNULL(aos_ret = oss_get_object_tagging(oss_base.oss_option_, &bucket, &object, &tag_list, &head_resp_headers)) || !aos_status_is_ok(aos_ret)) {
      convert_io_error(aos_ret, ret);
      OB_LOG(WARN, "get object tag fail", K(ret), K(uri));
      oss_base.print_oss_info(head_resp_headers, aos_ret, ret);
    } else {
      aos_list_for_each_entry(oss_tag_content_t, b, &tag_list, node) {
        char key_str[OB_MAX_TAGGING_STR_LENGTH];
        char value_str[OB_MAX_TAGGING_STR_LENGTH];
        if (OB_NOT_NULL(b->key.data) && OB_NOT_NULL(b->value.data)) {
          if (OB_FAIL(databuff_printf(key_str, OB_MAX_TAGGING_STR_LENGTH , "%.*s",b->key.len, b->key.data))) {
            OB_LOG(WARN, "failed to databuff printf key str", K(ret));
          } else if (OB_FAIL(databuff_printf(value_str, OB_MAX_TAGGING_STR_LENGTH, "%.*s", b->key.len, b->value.data))) {
            OB_LOG(WARN, "failed to databuff printf value str", K(ret));
          } else if (0 == strcmp("delete_mode", key_str) && 0 == strcmp("tagging", value_str)) {
            is_tagging = true;
          }
        }

        if (OB_FAIL(ret)) {
          break;
        }
      }
    }
  }

  return ret;
}

int ObStorageOssUtil::delete_object_(
    const common::ObString &uri,
    ObStorageOssBase &oss_base,
    const common::ObString &bucket_str,
    const common::ObString &object_str)
{
  int ret = OB_SUCCESS;
  aos_string_t bucket;
  aos_string_t object;
  aos_str_set(&bucket, bucket_str.ptr());
  aos_str_set(&object, object_str.ptr());
  aos_table_t *resp_headers = NULL;
  aos_status_t *aos_ret = NULL;

  if (OB_ISNULL(aos_ret = oss_delete_object(oss_base.oss_option_, &bucket, &object, &resp_headers)) || !aos_status_is_ok(aos_ret)) {
    convert_io_error(aos_ret, ret);
    OB_LOG(WARN, "delete object fail", K(ret), K(uri));
    oss_base.print_oss_info(resp_headers, aos_ret, ret);
  } else {
    OB_LOG(INFO, "delete object succ", K(uri));
  }
  return ret;
}

int ObStorageOssUtil::tagging_object_(
    const common::ObString &uri,
    ObStorageOssBase &oss_base,
    const common::ObString &bucket_str,
    const common::ObString &object_str)
{
  int ret = OB_SUCCESS;
  /*set object tagging*/
  aos_string_t bucket;
  aos_string_t object;
  aos_table_t *head_resp_headers = NULL;
  oss_tag_content_t *tag_content = NULL;
  aos_list_t tag_list;
  aos_status_t *aos_ret = NULL;
  aos_str_set(&bucket, bucket_str.ptr());
  aos_str_set(&object, object_str.ptr());
  aos_list_init(&tag_list);
  if (OB_ISNULL(tag_content = oss_create_tag_content(oss_base.aos_pool_))) {
    ret = OB_OSS_ERROR; 
    OB_LOG(WARN, "tag content is null", K(ret), K(uri));
  } else {
    aos_str_set(&tag_content->key, "delete_mode");
    aos_str_set(&tag_content->value, "tagging");
    aos_list_add_tail(&tag_content->node, &tag_list);
    if (OB_ISNULL(aos_ret = oss_put_object_tagging(oss_base.oss_option_, &bucket, &object, &tag_list, &head_resp_headers)) || !aos_status_is_ok(aos_ret)) {
      convert_io_error(aos_ret, ret);
      OB_LOG(WARN, "set object tag fail", K(ret), K(uri));
      oss_base.print_oss_info(head_resp_headers, aos_ret, ret);
    } else {
      OB_LOG(INFO, "set object tag succ", K(uri));
    }
  }

  return ret;
}

int ObStorageOssUtil::del_file(const common::ObString &uri)
{
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;
  common::ObArenaAllocator allocator(OB_STORAGE_OSS_ALLOCATOR);
  ObString bucket_str;
  ObString object_str;
  ObStorageOssBase oss_base;
  if (uri.empty()) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "name is empty", K(ret));
  } else if (!is_opened_) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "oss util is not inited", K(ret), K(uri));
  } else if (OB_FAIL(oss_base.init_with_storage_info(storage_info_))) {
    OB_LOG(WARN, "fail to init oss base with account", K(ret), K(uri));
  } else if (OB_FAIL(get_bucket_object_name(uri, bucket_str, object_str, allocator))) {
    OB_LOG(WARN, "bucket or object name is empty", K(ret), K(uri), K(bucket_str), K(object_str));
  } else if (ObIStorageUtil::DELETE == oss_base.oss_account_.delete_mode_) {
    if (OB_FAIL(delete_object_(uri, oss_base, bucket_str, object_str))) {
      OB_LOG(WARN, "failed to delete object", K(ret), K(uri));
    }
  } else if (ObIStorageUtil::TAGGING == oss_base.oss_account_.delete_mode_) {
    if (OB_FAIL(tagging_object_(uri, oss_base, bucket_str, object_str))) {
      OB_LOG(WARN, "failed to tagging file", K(ret), K(uri));
    }
  } else {
    ret = OB_INVALID_ARGUMENT; 
    OB_LOG(WARN, "delete mode invalid", K(ret), K(uri)); 
  }
  return ret;
}

int handle_oss_file_name(
    const char* file_name_data,
    int file_name_len,
    common::ObBaseDirEntryOperator &op)
{
   int ret = OB_SUCCESS;
   dirent entry;
   entry.d_type = DT_REG;

   if (OB_ISNULL(file_name_data) || OB_UNLIKELY(sizeof(entry.d_name) <= file_name_len)) {
     ret = OB_INVALID_ARGUMENT;
     OB_LOG(WARN, "file name is too long",
         K(ret), KP(file_name_data), K(file_name_len), K(sizeof(entry.d_name)));
   } else {
     MEMCPY(entry.d_name, file_name_data, file_name_len);
     entry.d_name[file_name_len] = '\0'; // set str end
     if (OB_FAIL(op.func(&entry))) { 
      OB_LOG(WARN, "fail to exe application callback", K(ret));
     }
   }
   return ret;
}

int ObStorageOssUtil::do_list_(ObStorageOssBase &oss_base,
    const ObString &bucket_str, const char *full_dir_path,
    const int64_t max_ret, const char *delimiter,
    const char *next_marker, oss_list_object_params_t *&params)
{
  int ret = OB_SUCCESS;
  aos_string_t bucket;
  aos_status_t *aos_ret = NULL;
  aos_table_t *resp_headers = NULL;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "oss util is not inited", K(ret));
  } else if (OB_UNLIKELY(!oss_base.is_inited()
                        || bucket_str.empty() || !is_end_with_slash(full_dir_path)
                        || max_ret <= 0 || max_ret > OB_STORAGE_LIST_MAX_NUM)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid arguments", K(ret),
        K(oss_base.is_inited()), K(bucket_str), K(max_ret), K(full_dir_path));
  } else {
    if (NULL == params) {
      if (OB_ISNULL(params = oss_create_list_object_params(oss_base.aos_pool_))) {
        ret = OB_OSS_ERROR;
        OB_LOG(WARN, "fail to create list object params", K(ret), K(bucket_str), K(full_dir_path));
      }
    } else {
      // reuse params
      aos_list_init(&params->object_list);
      aos_list_init(&params->common_prefix_list);
    }

    if (OB_SUCC(ret)) {
      params->max_ret = max_ret;
      aos_str_set(&bucket, bucket_str.ptr());
      aos_str_set(&params->prefix, full_dir_path);
      if (NULL != next_marker && strlen(next_marker) > 0) {
        aos_str_set(&params->marker, next_marker);
      }
      if (NULL != delimiter && strlen(delimiter) > 0) {
        aos_str_set(&params->delimiter, delimiter);
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(aos_ret = oss_list_object(oss_base.oss_option_, &bucket,
                                                   params, &resp_headers))
        || !aos_status_is_ok(aos_ret)) {
      convert_io_error(aos_ret, ret);
      OB_LOG(WARN, "fail to list oss objects", K(ret), K(bucket_str), K(full_dir_path));
      oss_base.print_oss_info(resp_headers, aos_ret, ret);
    }
  }
  return ret;
}

int ObStorageOssUtil::list_files(
    const common::ObString &uri,
    common::ObBaseDirEntryOperator &op)
{
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;
  common::ObArenaAllocator tmp_allocator(OB_STORAGE_OSS_ALLOCATOR);
  ObString bucket_str;
  ObString object_str;
  ObStorageOssBase oss_base;
  const char *full_dir_path = NULL;

  if (OB_UNLIKELY(uri.empty())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "uri is invalid", K(ret), K(uri));
  } else if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "oss util is not inited", K(ret), K(uri));
  } else if (OB_FAIL(oss_base.init_with_storage_info(storage_info_))) {
    OB_LOG(WARN, "fail to init oss base with account", K(ret), K(uri), KPC_(storage_info));
  } else if (OB_FAIL(get_bucket_object_name(uri, bucket_str, object_str, tmp_allocator))) {
    OB_LOG(WARN, "bucket or object name is empty", K(ret), K(uri), K(bucket_str), K(object_str));
  } else if (FALSE_IT(full_dir_path = object_str.ptr())) {
  } else if (OB_UNLIKELY(!is_end_with_slash(full_dir_path))) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "uri is not terminated with '/'", K(ret), K(uri), K(full_dir_path));
  } else {
    ObString object_path;
    const char *next_marker = "";
    const int64_t full_dir_path_len = strlen(full_dir_path);
    oss_list_object_content_t *content = NULL;
    oss_list_object_params_t *params = NULL;
    do {
      if (OB_FAIL(do_list_(oss_base, bucket_str, full_dir_path, OB_STORAGE_LIST_MAX_NUM,
                           NULL/*delimiter*/, next_marker, params))) {
        OB_LOG(WARN, "fail to list oss objects",
            K(ret), K(bucket_str), K(full_dir_path), K(next_marker));
      } else {
        object_path.reset();
        aos_list_for_each_entry(oss_list_object_content_t, content, &params->object_list, node) {
          //key.data has prefix object_str, we only get the file name
          object_path.assign(content->key.data, content->key.len);
          int64_t object_size = -1;

          if (OB_UNLIKELY(false == object_path.prefix_match(full_dir_path))) {
            ret = OB_OSS_ERROR;
            OB_LOG(WARN, "returned object prefix not match",
                K(ret), K(object_path), K(full_dir_path), K(uri));
          } else if (OB_UNLIKELY(object_path.length() == full_dir_path_len)) {
            // skip
            OB_LOG(INFO, "exist object path length is same with dir path length",
                K(object_path), K(full_dir_path), K(full_dir_path_len));
          } else if (OB_FAIL(c_str_to_int(content->size.data, object_size))) {
            OB_LOG(WARN, "fail to get object size", K(ret), K(content->size.data), K(object_path));
          } else if (OB_FAIL(handle_listed_object(op, content->key.data + full_dir_path_len,
                                                  content->key.len - full_dir_path_len,
                                                  object_size))) {
            OB_LOG(WARN, "fail to handle oss file name",
                K(ret), K(object_path), K(full_dir_path), K(content->size.data), K(object_size));
          }

          if (OB_FAIL(ret)) {
            break;
          }
        } // end aos_list_for_each_entry
      }
      if (OB_SUCC(ret) && AOS_TRUE == params->truncated) {
        if (OB_ISNULL(params->next_marker.data) || OB_UNLIKELY(params->next_marker.len <= 0)) {
          ret = OB_OSS_ERROR;
          OB_LOG(WARN, "next_marker is invalid", K(ret),
              K(params->next_marker.data), K(params->next_marker.len));
        } else if (nullptr == (next_marker = apr_psprintf(oss_base.aos_pool_, "%.*s",
                                                          params->next_marker.len,
                                                          params->next_marker.data))) {
          ret = OB_OSS_ERROR;
          OB_LOG(WARN, "next marker is NULL", K(ret), KP(next_marker), K(params->next_marker.data));
        }
      }
    } while (AOS_TRUE == params->truncated && OB_SUCC(ret));
  }

  return ret;
}

int ObStorageOssUtil::list_files(
    const common::ObString &uri,
    ObStorageListCtxBase &ctx_base)
{
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;
  common::ObArenaAllocator tmp_allocator(OB_STORAGE_OSS_ALLOCATOR);
  ObString bucket_str;
  ObString object_str;
  ObStorageOssBase oss_base;
  const char *full_dir_path = NULL;
  ObStorageListObjectsCtx &list_ctx = static_cast<ObStorageListObjectsCtx &>(ctx_base);

  if (OB_UNLIKELY(uri.empty() || !list_ctx.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid arguments", K(ret), K(uri), K(list_ctx));
  } else if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "oss util is not inited", K(ret), K(uri));
  } else if (OB_FAIL(oss_base.init_with_storage_info(storage_info_))) {
    OB_LOG(WARN, "fail to init oss base with account", K(ret), K(uri), KPC_(storage_info));
  } else if (OB_FAIL(get_bucket_object_name(uri, bucket_str, object_str, tmp_allocator))) {
    OB_LOG(WARN, "bucket or object name is empty", K(ret), K(uri), K(bucket_str), K(object_str));
  } else if (FALSE_IT(full_dir_path = object_str.ptr())) {
  } else if (OB_UNLIKELY(!is_end_with_slash(full_dir_path))) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "uri is not terminated with '/'", K(ret), K(uri), K(full_dir_path));
  } else {
    ObString object_path;
    const int64_t full_dir_path_len = strlen(full_dir_path);
    oss_list_object_content_t *content = NULL;
    oss_list_object_params_t *params = NULL;
    const char *next_marker = "";
    if (list_ctx.next_token_ != NULL && list_ctx.next_token_[0] != '\0') {
      next_marker = list_ctx.next_token_;
    }
    const int64_t max_ret = MIN(OB_STORAGE_LIST_MAX_NUM, list_ctx.max_list_num_);

    if (OB_FAIL(do_list_(oss_base, bucket_str, full_dir_path, max_ret,
                         NULL/*delimiter*/, next_marker, params))) {
      OB_LOG(WARN, "fail to list oss objects",
          K(ret), K(bucket_str), K(full_dir_path), K(max_ret), K(next_marker));
    } else if (OB_FAIL(list_ctx.set_next_token(AOS_TRUE == params->truncated,
                                               params->next_marker.data,
                                               params->next_marker.len))) {
      OB_LOG(WARN, "fail to set next token when listing oss objects",
          K(ret), K(params->truncated), K(params->next_marker.data));
    } else {
      object_path.reset();
      aos_list_for_each_entry(oss_list_object_content_t, content, &params->object_list, node) {
        //key.data has prefix object_str, we only get the file name
        object_path.assign(content->key.data, content->key.len);
        int64_t object_size = -1;

        if (OB_UNLIKELY(false == object_path.prefix_match(full_dir_path))) {
          ret = OB_OSS_ERROR;
          OB_LOG(WARN, "returned object prefix not match",
              K(ret), K(object_path), K(full_dir_path), K(uri));
        } else if (OB_UNLIKELY(object_path.length() == full_dir_path_len)) {
          // skip
          OB_LOG(INFO, "exist object path length is same with dir path length",
              K(object_path), K(full_dir_path), K(full_dir_path_len));
        } else if (OB_FAIL(c_str_to_int(content->size.data, object_size))) {
          OB_LOG(WARN, "fail to get object size", K(ret), K(content->size.data), K(object_path));
        } else if (OB_FAIL(list_ctx.handle_object(content->key.data,
                                                  content->key.len,
                                                  object_size))) {
          OB_LOG(WARN, "fail to add listed oss obejct meta into ctx",
              K(ret), K(object_path), K(full_dir_path), K(content->size.data), K(object_size));
        }

        if (OB_FAIL(ret)) {
          break;
        }
      } // end aos_list_for_each_entry
    }
  }

  return ret;
}

int ObStorageOssUtil::del_dir(const common::ObString &uri)
{
  int ret = OB_SUCCESS;
  UNUSED(uri);
  return ret;
}

//datetime formate : Tue, 09 Apr 2019 06:24:00 GMT
//time unit is second
int ObStorageOssUtil::ob_strtotime(const char *date_time, int64_t &time)
{
  int ret = OB_SUCCESS;
  time = 0;
  struct tm tm_time;
  memset(&tm_time, 0, sizeof(struct tm));
  if (OB_ISNULL(date_time)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "ob_strtotime get invalid argument", K(ret), KP(date_time));
  } else if (OB_ISNULL(strptime(date_time, "%a, %d %b %Y %H:%M:%S %Z", &tm_time))) {
    // ignore ret
    //skip set ret, for compat data formate
    OB_LOG(WARN, "failed to strptime", K(ret), K(*date_time));
  } else {
    time = mktime(&tm_time);
  }
  return ret;
}

int ObStorageOssUtil::list_directories(
    const common::ObString &uri,
    common::ObBaseDirEntryOperator &op)
{
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;
  common::ObArenaAllocator tmp_allocator(OB_STORAGE_OSS_ALLOCATOR);
  ObString bucket_str;
  ObString object_str;
  ObStorageOssBase oss_base;
  const char *full_dir_path = NULL;

  if (OB_UNLIKELY(uri.empty())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "name is empty", K(ret), K(uri));
  } else if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "oss util is not inited", K(ret), K(uri));
  } else if (OB_FAIL(oss_base.init_with_storage_info(storage_info_))) {
    OB_LOG(WARN, "fail to init oss base with account", K(ret), K(uri));
  } else if (OB_FAIL(get_bucket_object_name(uri, bucket_str, object_str, tmp_allocator))) {
    OB_LOG(WARN, "bucket or object name is empty", K(ret), K(uri), K(bucket_str), K(object_str));
  } else if (FALSE_IT(full_dir_path = object_str.ptr())) {
  } else if (OB_UNLIKELY(!is_end_with_slash(full_dir_path))) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "uri is not terminated with '/'", K(ret), K(uri), K(full_dir_path));
  } else {
    ObString listed_dir_full_path;
    const char *next_marker = "";
    const char *delimiter = "/";
    const int64_t full_dir_path_len = strlen(full_dir_path);
    oss_list_object_common_prefix_t *common_prefix = NULL;
    oss_list_object_params_t *params = NULL;

    do {
      if (OB_FAIL(do_list_(oss_base, bucket_str, full_dir_path, OB_STORAGE_LIST_MAX_NUM,
                           delimiter, next_marker, params))) {
        OB_LOG(WARN, "fail to list s3 objects", K(ret), K(uri), K(delimiter), K(next_marker));
      } else {
        aos_list_for_each_entry(oss_list_object_common_prefix_t, common_prefix, &params->common_prefix_list, node) {
          // For example,
          //       dir1
          //         --file1
          //         --dir11
          //            --file11
          // if we list directories in 'dir1', then full_dir_path == 'dir1/'
          // and listed_dir_full_path == 'dir1/dir11/', which represents the full directory path of 'dir11'
          listed_dir_full_path.assign(common_prefix->prefix.data, common_prefix->prefix.len);
          if (OB_UNLIKELY(false == listed_dir_full_path.prefix_match(full_dir_path))) {
            ret = OB_OSS_ERROR;
            OB_LOG(WARN, "returned object prefix not match",
                K(ret), K(listed_dir_full_path), K(full_dir_path), K(uri));
          } else if (OB_UNLIKELY(!is_end_with_slash(listed_dir_full_path.ptr()))) {
            ret = OB_S3_ERROR;
            OB_LOG(WARN, "the data has no directory",
                K(ret), K(full_dir_path), K(listed_dir_full_path), K(uri));
          } else if (OB_FAIL(handle_listed_directory(op,
              common_prefix->prefix.data + full_dir_path_len,
              common_prefix->prefix.len - 1 - full_dir_path_len))) {
            // common_prefix->prefix.len is the length of listed_dir_full_path, including the trailing '/' character.
            // Therefore, the length for dir_name needs to exclude both the prefix full_dir_path and the trailing '/'.
            OB_LOG(WARN, "fail to handle oss directory name",
                K(ret), K(listed_dir_full_path), K(full_dir_path), K(full_dir_path_len));
          }

          if (OB_FAIL(ret)) {
            break;
          }
        } // end aos_list_for_each_entry
      }
      if (OB_SUCC(ret) && AOS_TRUE == params->truncated) {
        if (OB_ISNULL(params->next_marker.data) || OB_UNLIKELY(params->next_marker.len <= 0)) {
          ret = OB_OSS_ERROR;
          OB_LOG(WARN, "next_marker is invalid", K(ret),
              K(params->next_marker.data), K(params->next_marker.len));
        } else if (nullptr == (next_marker = apr_psprintf(oss_base.aos_pool_, "%.*s",
                                                          params->next_marker.len,
                                                          params->next_marker.data))) {
          ret = OB_OSS_ERROR;
          OB_LOG(WARN, "next marker is NULL", K(ret), KP(next_marker), K(params->next_marker.data));
        }
      }
    } while (AOS_TRUE == params->truncated && OB_SUCC(ret));
  }

  OB_LOG(INFO, "list directory count", K(uri));
  return ret;
}

int ObStorageOssUtil::del_unmerged_parts(const ObString &uri)
{
  int ret = OB_SUCCESS;
  oss_list_multipart_upload_params_t *params = NULL;
  const char *next_key_marker = "";
  const char *next_upload_id_marker = "";
  ObString bucket_str;
  ObString object_str;
  ObStorageOssBase oss_base;
  ObArenaAllocator allocator(OB_STORAGE_OSS_ALLOCATOR);
  ObExternalIOCounterGuard io_guard;

  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "oss util is not inited", K(ret), K(uri));
  } else if (OB_UNLIKELY(uri.empty())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "uri is empty", K(ret), K(uri));
  } else if (OB_FAIL(oss_base.init_with_storage_info(storage_info_))) {
    OB_LOG(WARN, "fail to init oss base with account", K(ret), K(uri), KPC_(storage_info));
  } else if (OB_FAIL(get_bucket_object_name(uri, bucket_str, object_str, allocator))) {
    OB_LOG(WARN, "bucket or object name is empty", K(ret), K(uri), K(bucket_str), K(object_str));
  } else if (OB_ISNULL(params = oss_create_list_multipart_upload_params(oss_base.aos_pool_))) {
    ret = OB_OSS_ERROR;
    OB_LOG(WARN, "fail to create list oss multipart upload params", K(ret), K(bucket_str));
  } else {
    aos_str_set(&params->prefix, object_str.ptr());
    aos_str_set(&params->next_key_marker, next_key_marker);
    aos_str_set(&params->next_upload_id_marker, next_upload_id_marker);
    params->max_ret = OB_STORAGE_LIST_MAX_NUM;
    aos_string_t bucket;
    aos_string_t object;
    aos_str_set(&bucket, bucket_str.ptr());
    oss_list_multipart_upload_content_t *content = NULL;
    aos_table_t *resp_headers = NULL;
    aos_status_t *aos_ret = NULL;

    do {
      if (OB_ISNULL(aos_ret = oss_list_multipart_upload(oss_base.oss_option_, &bucket,
                                                        params, &resp_headers))
          || !aos_status_is_ok(aos_ret)) {
        convert_io_error(aos_ret, ret);
        OB_LOG(WARN, "fail to list oss multipart uploads", K(ret), K(bucket_str));
        oss_base.print_oss_info(resp_headers, aos_ret, ret);
      } else {
        aos_list_for_each_entry(oss_list_multipart_upload_content_t, content, &params->upload_list, node) {
          if (OB_ISNULL(content->key.data) || OB_ISNULL(content->upload_id.data)) {
            ret = OB_OSS_ERROR;
            OB_LOG(WARN, "returned key or upload id invalid",
                K(ret), K(content->key.data), K(content->upload_id.data));
          } else if (OB_ISNULL(aos_ret = oss_abort_multipart_upload(oss_base.oss_option_, &bucket,
                                                                    &(content->key),
                                                                    &(content->upload_id),
                                                                    &resp_headers))
              || !aos_status_is_ok(aos_ret)) {
            convert_io_error(aos_ret, ret);
            OB_LOG(WARN, "fail to abort oss multipart upload",
                K(ret), K(bucket_str), K(content->key.data), K(content->upload_id.data));
            oss_base.print_oss_info(resp_headers, aos_ret, ret);
            break;
          } else {
            OB_LOG(INFO, "succeed abort oss multipart upload",
                K(bucket_str), K(content->key.data), K(content->upload_id.data));
          }

          if (OB_FAIL(ret)) {
            break;
          }
        }
      }

      if (OB_SUCC(ret) && AOS_TRUE == params->truncated) {
        if (OB_ISNULL(params->next_key_marker.data) || OB_ISNULL(params->next_upload_id_marker.data)
            || OB_UNLIKELY(params->next_key_marker.len <= 0 || params->next_upload_id_marker.len <= 0)) {
          ret = OB_OSS_ERROR;
          OB_LOG(WARN, "next_key_marker or next_upload_id_marker is invalid", K(ret),
              K(params->next_key_marker.data), K(params->next_key_marker.len),
              K(params->next_upload_id_marker.data), K(params->next_upload_id_marker.len));
        } else if (OB_ISNULL(next_key_marker =
            apr_psprintf(oss_base.aos_pool_, "%.*s",
                         params->next_key_marker.len,
                         params->next_key_marker.data))) {
          ret = OB_OSS_ERROR;
          OB_LOG(WARN, "next key marker is NULL", K(ret), KP(next_key_marker));
        } else if (OB_ISNULL(next_upload_id_marker =
            apr_psprintf(oss_base.aos_pool_, "%.*s",
                         params->next_upload_id_marker.len,
                         params->next_upload_id_marker.data))) {
          ret = OB_OSS_ERROR;
          OB_LOG(WARN, "next upload id marker is NULL", K(ret), KP(next_upload_id_marker));
        } else {
          aos_str_set(&params->key_marker, next_key_marker);
          aos_str_set(&params->upload_id_marker, next_upload_id_marker);
          aos_list_init(&params->upload_list);
        }
      }
    } while (OB_SUCC(ret) && AOS_TRUE == params->truncated);
  }
  return ret;
}


ObStorageOssAppendWriter::ObStorageOssAppendWriter()
  : is_opened_(false),
    file_length_(-1),
    allocator_(OB_STORAGE_OSS_ALLOCATOR),
    bucket_(),
    object_()
{
}

ObStorageOssAppendWriter::~ObStorageOssAppendWriter()
{
}

int ObStorageOssAppendWriter::open(
    const common::ObString &uri,
    common::ObObjectStorageInfo *storage_info)
{
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;

  if (uri.empty()) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "uri is empty", K(ret), K(uri));
  } else if (is_opened_) {
    ret = OB_OSS_ERROR;
    OB_LOG(WARN, "already open, cannot open again", K(ret), K(uri));
  } else if (OB_FAIL(init_with_storage_info(storage_info))) {
    OB_LOG(WARN, "failed to init oss", K(ret), K(uri));
  } else if (OB_FAIL(get_bucket_object_name(uri, bucket_, object_, allocator_))) {
    OB_LOG(WARN, "get bucket object error", K(uri), K(ret));
  } else {
    is_opened_ = true;
    file_length_ = 0;
  }

  return ret;
}

int ObStorageOssAppendWriter::write(const char *buf,
    const int64_t size)
{
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;
  const int64_t fake_offset = 0;
  const bool is_pwrite = false;

  if (!is_inited()) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "oss client not inited", K(ret));
  } else if (NULL == buf || size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "buf is NULL or size is invalid", KP(buf), K(size), K(ret));
  } else if (OB_FAIL(do_write(buf, size, fake_offset, is_pwrite))) {
    OB_LOG(WARN, "failed to do write", K(ret), KP(buf), K(size));
  }
  return ret;
}

int ObStorageOssAppendWriter::pwrite(const char *buf, const int64_t size, const int64_t offset)
{
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;
  const bool is_pwrite = true;

  if (!is_inited()) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "oss client not inited", K(ret));
  } else if (NULL == buf || size <= 0 || offset < 0) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid arguments", KP(buf), K(size), K(ret), K(offset));
  } else if (OB_FAIL(do_write(buf, size, offset, is_pwrite))) {
    OB_LOG(WARN, "failed to do write", K(ret), KP(buf), K(size), K(offset));
  }
  return ret;
}

int ObStorageOssAppendWriter::close()
{
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;
  is_opened_ = false;
  allocator_.clear();
  reset();

  return ret;
}

int ObStorageOssAppendWriter::do_write(const char *buf, const int64_t size, const int64_t offset, const bool is_pwrite)
{
  int ret = OB_SUCCESS;

  const int64_t start_time = ObTimeUtility::current_time();
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "oss client not inited", K(ret));
  } else if (NULL == buf || size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "buf is NULL or size is invalid", KP(buf), K(size), K(ret));
  } else {
    // bucket object
    aos_string_t bucket;
    aos_string_t object;
    aos_str_set(&bucket, bucket_.ptr());
    aos_str_set(&object, object_.ptr());
    // oss_ret
    aos_status_t *aos_ret = NULL;
    // aos_table_t
    aos_table_t *headers1 = NULL;
    aos_table_t *headers2 = NULL;
    aos_table_t *resp_headers = NULL;
    // buf
    aos_list_t buffer;
    aos_buf_t *content = NULL;
    aos_list_init(&buffer);
    int64_t position = 0;
    if (OB_ISNULL(headers1 = aos_table_make(aos_pool_, 0))) {
      ret = OB_OSS_ERROR;
      OB_LOG(WARN, "fail to make apr table", K(ret));
    } else if (OB_ISNULL(aos_ret = oss_head_object(oss_option_, &bucket, &object, headers1, &resp_headers))) {
      print_oss_info(resp_headers, aos_ret, ret);
      ret = OB_OSS_ERROR;
      OB_LOG(WARN, "oss head object fail", K(ret), K_(bucket), K_(object), K(aos_ret));
    } else {
      if (0 != aos_status_is_ok(aos_ret)) { // != 0 means ok
        char *object_type = (char*)(apr_table_get(resp_headers, OSS_OBJECT_TYPE));
        if (OB_ISNULL(object_type)) {
          ret = OB_OSS_ERROR;
          OB_LOG(ERROR, "oss type is null", K(ret));
        } else if (0 != strncmp(OSS_OBJECT_TYPE_APPENDABLE, object_type, strlen(OSS_OBJECT_TYPE_APPENDABLE))) {
          ret = OB_CLOUD_OBJECT_NOT_APPENDABLE;
          OB_LOG(WARN, "oss object must be appendable", K(ret), KCSTRING(object_type));
        } else {
          char *next_append_position = (char*)(apr_table_get(resp_headers, OSS_NEXT_APPEND_POSITION));
          if (OB_ISNULL(next_append_position)) {
            ret = OB_OSS_ERROR;
            OB_LOG(WARN, "next_append_position is not found", K(ret), K_(bucket), K_(object));
          } else if (OB_FAIL(c_str_to_int(next_append_position, position))) {
            OB_LOG(WARN, "fail to get position", K(ret), K(next_append_position), K_(object));
          } else {
            if (0 > position) {
              ObString tmp_position_string(next_append_position);
              ret = OB_OSS_ERROR;
              OB_LOG(WARN, "invalid append position", K(ret), K(position), K(tmp_position_string));
            }
          }
        }
      }

      if (OB_SUCC(ret) && is_pwrite && position != offset) {
        ret = OB_BACKUP_PWRITE_OFFSET_NOT_MATCH;
        OB_LOG(WARN, "position and offset do not match", K(ret), K(position), K(offset));
      }

      if (OB_SUCC(ret)) {
        if (OB_ISNULL(headers2 = aos_table_make(aos_pool_, AOS_TABLE_INIT_SIZE))) {
          ret = OB_OSS_ERROR;
          OB_LOG(WARN, "fail to make apr table", K(ret));
        } else if (OB_ISNULL(content = aos_buf_pack(aos_pool_, buf, static_cast<int32_t>(size)))) {
          ret = OB_OSS_ERROR;
          OB_LOG(WARN, "fail to pack buf", K(content), K(ret));
        } else if ((checksum_type_ == ObStorageChecksumType::OB_MD5_ALGO)
            && OB_FAIL(add_content_md5(oss_option_, buf, size, headers2))) {
          OB_LOG(WARN, "fail to add content md5 when apending object", K(ret));
        } else {
          aos_list_add_tail(&content->node, &buffer);
          aos_ret = oss_append_object_from_buffer(oss_option_, &bucket, &object, position, &buffer,
              headers2, &resp_headers);
          if (OB_NOT_NULL(aos_ret) && 0 != aos_status_is_ok(aos_ret)) { // != 0 means ok
            file_length_ += size;
          } else {
            print_oss_info(resp_headers, aos_ret, ret);
            convert_io_error(aos_ret, ret);
            OB_LOG(WARN, "fail to append", K(content), K(ret));

            // If append failed, print the current object meta, to help debugging.
            aos_table_t *headers3 = NULL;
            int tmp_ret = OB_SUCCESS;
            if(OB_NOT_NULL(headers3 = aos_table_make(aos_pool_, 0))) {
              if(OB_NOT_NULL(aos_ret = oss_head_object(oss_option_, &bucket, &object, headers3, &resp_headers))) {
                if ((0 != aos_status_is_ok(aos_ret))) {
                  int64_t cur_pos = -1;
                  char *append_pos_str = (char*)(apr_table_get(resp_headers, OSS_NEXT_APPEND_POSITION));
                  if (OB_ISNULL(append_pos_str)) {
                    // ignore ret
                    OB_LOG(WARN, "after append fail, current append pos is not found", K(ret));
                  } else if (OB_TMP_FAIL(c_str_to_int(append_pos_str, cur_pos))) {
                    OB_LOG(WARN, "after append fail, fail to get append pos",
                        K(ret), K(tmp_ret), K(append_pos_str), K_(object));
                  } else {
                    OB_LOG(WARN, "after append fail, we got the object meta", K(ret), K(cur_pos));
                  }
                }
              }
            }
          }
        }
      }
    }
    //print slow info
    if (OB_SUCC(ret)) {
      const int64_t cost_time = ObTimeUtility::current_time() - start_time;
      const int64_t warn_cost_time = 1000 * 1000; //1s
      long double speed = (cost_time <= 0) ? 0 :
                                             (long double) size * 1000.0 * 1000.0 / 1024.0 / 1024.0 / cost_time;
      if (cost_time > warn_cost_time) {
        _OB_LOG_RET(WARN, OB_ERR_TOO_MUCH_TIME, "oss append one object cost too much time, time:%ld, size:%ld speed:%.2Lf MB/s file_length=%ld, ret=%d",
            cost_time, size, speed, file_length_, ret);
        print_oss_info(resp_headers, aos_ret, ret);
      } else {
        _OB_LOG(DEBUG, "oss append one object time:%ld, size:%ld speed:%.2Lf MB/s file_length=%ld, ret=%d",
            cost_time, size, speed, file_length_, ret);
      }
    }
  }
  return ret;
}

ObStorageOssWriter::ObStorageOssWriter()
  : is_opened_(false),
    file_length_(-1),
    allocator_(OB_STORAGE_OSS_ALLOCATOR),
    bucket_(),
    object_()
{
}

ObStorageOssWriter::~ObStorageOssWriter()
{
  if (is_opened_) {
    close();
  }
}

int ObStorageOssWriter::open(
    const common::ObString &uri,
    common::ObObjectStorageInfo *storage_info)
{
  int ret = OB_SUCCESS;
  if (uri.empty()) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "uri is empty", K(ret), K(uri));
  } else if (is_opened_) {
    ret = OB_OSS_ERROR;
    OB_LOG(WARN, "already open, cannot open again", K(ret));
  } else if (OB_FAIL(init_with_storage_info(storage_info))) {
    OB_LOG(WARN, "failed to init oss", K(ret), K(uri));
  } else if (OB_FAIL(get_bucket_object_name(uri, bucket_, object_, allocator_))) {
    OB_LOG(WARN, "get bucket object error", K(uri), K(ret));
  } else {
    is_opened_ = true;
    file_length_ = 0;
  }
  return ret;
}

int ObStorageOssWriter::close()
{
  int ret = OB_SUCCESS;
  is_opened_ = false;
  // Release memory
  allocator_.clear();
  file_length_ = -1;
	reset();

  return ret;
}

int ObStorageOssWriter::pwrite(const char *buf, const int64_t size, const int64_t offset)
{
  int ret = OB_NOT_SUPPORTED;
  UNUSED(buf);
  UNUSED(size);
  UNUSED(offset);
  return ret;
}

int ObStorageOssWriter::write(const char *buf, const int64_t size)
{
  int ret = OB_SUCCESS;
  const int64_t start_time = ObTimeUtility::current_time();
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "oss writer not opened", K(ret));
  } else if (OB_ISNULL(buf) || OB_UNLIKELY(size < 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "buf is NULL or size is invalid", KP(buf), K(size), K(ret));
  } else {
    aos_string_t bucket;
    aos_string_t object;
    aos_str_set(&bucket, bucket_.ptr());
    aos_str_set(&object, object_.ptr());
    aos_table_t *headers = NULL;
    aos_table_t *resp_headers = NULL;
    aos_status_t *aos_ret = NULL;
    aos_list_t buffer;
    aos_buf_t *content = NULL;
    aos_list_init(&buffer);

    if (OB_ISNULL(headers = aos_table_make(aos_pool_, AOS_TABLE_INIT_SIZE))) {
      ret = OB_OSS_ERROR;
      OB_LOG(WARN, "fail to make apr table", K(ret));
    } else if (OB_ISNULL(content = aos_buf_pack(aos_pool_, buf, static_cast<int32_t>(size)))) {
      ret = OB_OSS_ERROR;
      OB_LOG(WARN, "fail to pack buf", K(content), K(ret));
    } else if ((checksum_type_ == ObStorageChecksumType::OB_MD5_ALGO)
        && OB_FAIL(add_content_md5(oss_option_, buf, size, headers))) {
      OB_LOG(WARN, "fail to add content md5 when putting object", K(ret));
    } else {
      aos_list_add_tail(&content->node, &buffer);
      if (OB_ISNULL(aos_ret = oss_put_object_from_buffer(oss_option_, &bucket, &object,
                    &buffer, headers, &resp_headers)) || !aos_status_is_ok(aos_ret)) {
        convert_io_error(aos_ret, ret);
        OB_LOG(WARN, "fail to upload one object", K(bucket_), K(object_), K(ret));
        print_oss_info(resp_headers, aos_ret, ret);
      } else {
        file_length_ = size;
      }
    }
    //print slow info
    bool is_slow = false;
    print_access_storage_log("oss upload one object ", object_, start_time, size, &is_slow);
    if (is_slow) {
      print_oss_info(resp_headers, aos_ret, ret);
    }
  }
  return ret; 
}



}//tools
}//oceanbase
