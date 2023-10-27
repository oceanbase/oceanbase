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

    default: {
        ob_errcode = OB_OSS_ERROR;
     }
    }
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

int build_md5_str(MD5_CTX *c, char *buf, int64_t size)
{
  int ret = OB_SUCCESS;
  unsigned char md5[MD5_DIGEST_LENGTH];
  if (0 == MD5_Final(md5, c)) {
    ret = OB_FINAL_MD5_ERROR;
    OB_LOG(WARN, "final MD5_CTX error", K(ret));
  } else {
    int64_t pos = 0;
    for (int i = 0; OB_SUCC(ret) && i < MD5_DIGEST_LENGTH; ++i) {
      int n = snprintf(buf + pos, size - pos, "%02X", md5[i]);
      if (n < 0 || n >= size - pos) {
        ret = OB_SIZE_OVERFLOW;
        OB_LOG (WARN, "md5_buf_size not enough", K(size), K(i), K(pos), K(n), K(ret));
      } else {
        pos += n;
      }
    }
  }
  OB_LOG(INFO, "The Whole File MD5 ", KCSTRING(buf));
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
  } else if(AOSE_OK != (aos_ret = aos_http_io_initialize(NULL, 0))) {
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
   oss_account_()
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
  if(is_inited_) {
    if(NULL != aos_pool_) {
      aos_pool_destroy(aos_pool_);
      aos_pool_ = NULL;
    }
    oss_option_ = NULL;
    is_inited_ = false;
  }
}

int ObStorageOssBase::init_with_storage_info(common::ObObjectStorageInfo *storage_info)
{
  int ret = OB_SUCCESS;
  char info_str[common::OB_MAX_BACKUP_STORAGE_INFO_LENGTH] = { 0 };

  if(is_inited_) {
    ret = OB_INIT_TWICE;
    OB_LOG(WARN, "oss client init twice", K(ret));
  } else if (OB_ISNULL(storage_info)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "oss account is null, fail to init oss base!", K(ret));
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
    is_inited_ = true;
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

  if(is_inited_) {
    ret = OB_INIT_TWICE;
    OB_LOG(WARN, "oss client init twice", K(ret));
  } else if (OB_ISNULL(storage_info.ptr()) || storage_info.length() >= OB_MAX_URI_LENGTH) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "uri is too long", K(ret), K(storage_info), K(storage_info.length()));
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
          OB_LOG(WARN, "failed to set oss_key_", K(ret), KCSTRING(token));
        }
      } else if (0 == strncmp(DELETE_MODE, token, strlen(DELETE_MODE))) {
        if (OB_FAIL(set_delete_mode(token + strlen(DELETE_MODE)))) {
          OB_LOG(WARN, "failed to set delete mode", K(ret), K(token)); 
        }
      } else {
        OB_LOG(DEBUG, "unkown oss info", K(*token), K(storage_info));
      }
    }

    if (strlen(oss_domain_) == 0 || strlen(oss_id_) == 0 || strlen(oss_key_) == 0) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "failed to parse oss info",
          K(ret), KCSTRING(oss_domain_), KCSTRING(oss_id_), KCSTRING(oss_key_), K(storage_info));
    } else {
      STORAGE_LOG(DEBUG, "success to parse oss info",
          K(ret), KCSTRING(oss_domain_), KCSTRING(oss_id_), KCSTRING(oss_key_), K(storage_info));
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
      OB_LOG(WARN, "info is too long ", K(ret), KCSTRING(info), K(length));
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
  } else {
    aos_str_set(&oss_option->config->endpoint, oss_endpoint_);
    aos_str_set(&oss_option->config->access_key_id, oss_account_.oss_id_);
    aos_str_set(&oss_option->config->access_key_secret, oss_account_.oss_key_);
    oss_option->config->is_cname = 0;
    oss_option->ctl = aos_http_controller_create(oss_option->pool, 0);

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
  }
  return ret;
}

int ObStorageOssBase::reinit_oss_option()
{
  int ret = OB_SUCCESS;

  if (!is_inited()) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "please init first", K(ret));
  } else if(OB_FAIL(init_oss_endpoint())) {
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

  if(!is_inited()) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "oss client not inited", K(ret));
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
      print_oss_info(resp_headers, aos_ret);
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

void ObStorageOssBase::print_oss_info(aos_table_t *resp_headers, aos_status_s *aos_ret)
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
    OB_LOG_RET(WARN, OB_SUCCESS, "oss info ", K(aos_ret->code), KCSTRING(aos_ret->error_code),
        KCSTRING(aos_ret->error_msg), KCSTRING(aos_ret->req_id),  KCSTRING(delay_time),
        KCSTRING(oss_account_.oss_domain_), KCSTRING(oss_endpoint_), KCSTRING(oss_account_.oss_id_));
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
  MEMSET(&whole_file_md5_, 0, sizeof(whole_file_md5_));
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

    if(NULL == (headers = aos_table_make(aos_pool_, AOS_TABLE_INIT_SIZE))) {
      ret = OB_OSS_ERROR;
      OB_LOG(WARN, "fail to make apr table", K(ret));
    } else if(NULL == (aos_ret = oss_init_multipart_upload(oss_option_, &bucket, &object, &upload_id_,
                                                           headers, &resp_headers))
              || !aos_status_is_ok(aos_ret)) {
      ret = OB_OSS_ERROR;
      OB_LOG(WARN, "oss init multipart upload error", K(uri), K(ret));
      print_oss_info(resp_headers, aos_ret);
      cleanup();
    }

    if(OB_SUCCESS == ret) {
      //alloc memory from aos_pool_
      if(OB_ISNULL(base_buf_ = static_cast<char *>(allocator_.alloc(OSS_BASE_BUFFER_SIZE)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        OB_LOG(WARN, "fail to alloc memory", K(OSS_BASE_BUFFER_SIZE), K(ret));
      } else if(0 == MD5_Init(&whole_file_md5_)) {//init MD5_CTX,return 0 for failed
        ret = OB_INIT_MD5_ERROR;
        OB_LOG(WARN, "init MD5_CTX error,uri=%s, ret=%d", to_cstring(uri), ret);
      } else {
        is_opened_ = true;
        base_buf_pos_ = 0;
      }
    }
  }
  return ret;
}

int ObStorageOssMultiPartWriter::upload_data(const char *buf, const int64_t size, char *&upload_buf, int64_t &upload_size)
{
  ObMemBuf *writer_buf = GET_TSI_MULT(ObMemBuf, 1);

  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;
  upload_buf = NULL;
  upload_size = 0;

  if (!is_inited()) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "oss client not inited", K(ret));
  } else if (NULL == buf || size < 0 || OB_ISNULL(writer_buf)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "argument is invalid", KP(buf), K(size), K(writer_buf), K(ret));
  } else {
    int64_t max_size = 0;
    int64_t max_over_flow_size = 0;
    int64_t compress_size = 0;
    int64_t pos = 0;
    if (OB_FAIL(ObStorageOssStaticVar::get_instance().get_oss_compressor()->get_max_overflow_size(size, max_over_flow_size))) {
      OB_LOG(WARN, "fail to get max over flow size", K(ret));
    } else {
      max_size = sizeof(int64_t) + size + max_over_flow_size;
      if (OB_FAIL(writer_buf->ensure_space(max_size, ObModIds::BACKUP))) {
        OB_LOG(WARN, "fail to allocation memory", K(max_size), K(ret));
//      } else if (OB_FAIL(serialization::encode_i8(writer_buf.data(), max_size, pos, compress_type_))) {
//        OB_LOG(WARN, "fail to serialization compress type", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (ObCompressorType::NONE_COMPRESSOR != ObStorageOssStaticVar::get_instance().get_compressor_type() && 0 != size) {
        if (OB_FAIL(ObStorageOssStaticVar::get_instance().get_oss_compressor()->compress(buf,
            size, writer_buf->get_buffer() + pos, max_size, compress_size))) {
          OB_LOG(WARN, "fail to compress data", K(ret));
        } else {
          upload_size = compress_size + pos;
        }
      } else if (ObCompressorType::NONE_COMPRESSOR == ObStorageOssStaticVar::get_instance().get_compressor_type() || 0 == size) {
        MEMCPY(writer_buf->get_buffer() + pos, buf, size);
        upload_size = size + pos;
      }
      upload_buf = writer_buf->get_buffer();
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
  if(!is_inited()) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "oss client not inited", K(ret));
  } else if(NULL == buf || size < 0) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "buf is NULL or size is invalid", KP(buf), K(size), K(ret));
  } else if(!is_opened_) {
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

  if(OB_SUCCESS == ret) {
    file_length_ += size;
  }
  return ret;
}

int ObStorageOssMultiPartWriter::pwrite(const char *buf, const int64_t size, const int64_t offset)
{
  int ret = OB_NOT_SUPPORTED;
  UNUSEDx(buf, size, offset);
  return ret;
}

int ObStorageOssMultiPartWriter::write_single_part()
{
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;
  char *upload_buf = NULL;
  int64_t upload_size = 0;

  ++partnum_;
  //cal part num begin from 0
  if(partnum_ >= OSS_MAX_PART_NUM) {
    ret = OB_OUT_OF_ELEMENT;
    OB_LOG(WARN, "Out of oss element ", K(partnum_), K(OSS_MAX_PART_NUM), K(ret));
  } else if(!is_inited()) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "please init first", K(ret));
  } else if(!is_opened_) {
    ret = OB_OSS_ERROR;
    OB_LOG(WARN, "write oss should open first", K(ret));
  } else if(0 == MD5_Update(&whole_file_md5_, base_buf_, base_buf_pos_)) {
    ret = OB_UPDATE_MD5_ERROR;
    OB_LOG(WARN, "MD5 update error", K(ret));
  } else if (OB_FAIL(upload_data(base_buf_, base_buf_pos_, upload_buf, upload_size))) {
    OB_LOG(WARN, "fail to compress upload data", K(ret));
  }

  if(OB_SUCCESS == ret) {
    //upload data
    aos_string_t bucket;
    aos_string_t object;
    aos_str_set(&bucket, bucket_.ptr());
    aos_str_set(&object, object_.ptr());
    aos_table_t *resp_headers = NULL;
    aos_status_t *aos_ret = NULL;
    aos_list_t buffer;
    aos_buf_t *content = NULL;
    aos_list_init(&buffer);

    const int64_t start_time = ObTimeUtility::current_time();
    if (OB_ISNULL(content = aos_buf_pack(aos_pool_, upload_buf, static_cast<int32_t>(upload_size)))) {
      ret = OB_OSS_ERROR;
      OB_LOG(WARN, "fail to pack buf", K(content), K(ret));
    } else {
      aos_list_add_tail(&content->node, &buffer);

      if (NULL == (aos_ret = oss_upload_part_from_buffer(oss_option_, &bucket, &object, &upload_id_,
          partnum_, &buffer, &resp_headers)) || !aos_status_is_ok(aos_ret)) {
        ret = OB_OSS_ERROR;
        OB_LOG(WARN, "fail to upload one part from buffer",
            K(upload_size), K(bucket_), K(object_), K(ret));
        print_oss_info(resp_headers, aos_ret);
        cleanup();
      }
      bool is_slow = false;
      print_access_storage_log("oss upload one part ", object_, start_time, upload_size, &is_slow);
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCC(ret) && is_slow) {
        print_oss_info(resp_headers, aos_ret);
        if (OB_SUCCESS != (tmp_ret = reinit_oss_option())) {
          OB_LOG(WARN, "fail to reinit oss option", K(tmp_ret));
          ret = tmp_ret;
        }
      }
    }
  }
  return ret;
}

int ObStorageOssMultiPartWriter::close()
{
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;
  const int64_t start_time = ObTimeUtility::current_time();
  if(!is_inited()) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "oss client not inited", K(ret));
  } else if(!is_opened_) {
    ret = OB_OSS_ERROR;
    OB_LOG(WARN, "oss writer cannot close before it is opened");
  } else if(0 != base_buf_pos_) {//base_buf has data
    if(OB_SUCCESS != (ret = write_single_part())) {
      OB_LOG(WARN, "write the last size to oss error",
          K(base_buf_pos_), K(bucket_), K(object_),K(ret));
      cleanup();
      ret = OB_OSS_ERROR;
    } else {
      base_buf_pos_ = 0;
    }
  }

  if(OB_SUCCESS == ret) {
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
    if(NULL == (params = oss_create_list_upload_part_params(aos_pool_))) {
      ret = OB_OSS_ERROR;
      OB_LOG(WARN, "fail to create oss upload params", K(bucket_), K(object_), K(ret));
    } else if(NULL == (aos_ret = oss_list_upload_part(oss_option_, &bucket, &object, &upload_id_,
        params, &resp_headers)) || !aos_status_is_ok(aos_ret)) {
      ret = OB_OSS_ERROR;
      OB_LOG(WARN, "fail to list oss upload parts", K(bucket_), K(object_), K(ret));
      print_oss_info(resp_headers, aos_ret);
      cleanup();
    } else {
      aos_list_for_each_entry(oss_list_part_content_t, part_content, &params->part_list, node) {
        if (NULL == (complete_part_content = oss_create_complete_part_content(aos_pool_))) {
          ret = OB_OSS_ERROR;
          OB_LOG(WARN, "fail to create complete part content", K(bucket_), K(object_), K(ret));
          break;
        } else {
          aos_str_set(&complete_part_content->part_number, part_content->part_number.data);
          aos_str_set(&complete_part_content->etag, part_content->etag.data);
          aos_list_add_tail(&complete_part_content->node, &complete_part_list);
        }
      }
    }

    //complete multipart upload
    if(OB_SUCCESS == ret) {
      if(NULL == (aos_ret = oss_complete_multipart_upload(oss_option_, &bucket, &object, &upload_id_,
          &complete_part_list, complete_headers, &resp_headers)) || !aos_status_is_ok(aos_ret)) {
        ret = OB_OSS_ERROR;
        OB_LOG(WARN, "fail to complete multipart upload", K(bucket_), K(object_), K(ret));
        print_oss_info(resp_headers, aos_ret);
        cleanup();
      }
    }
  }

  if(OB_SUCCESS == ret) {
    //The oss c client verifies the integrity of each part every time it uploads, but the md5 value of the entire file is not saved,
    //Here you need to upload md5 by oss_copy_object.
    char md5_str[MD5_STR_LENGTH + 1] = {0};//must end with '\0'
    if(OB_SUCCESS != (ret = build_md5_str(&whole_file_md5_, md5_str, sizeof(md5_str)))) {
      OB_LOG(WARN, "fail to build md5 ", K(bucket_), K(object_), K(ret));
    } else {
      aos_string_t bucket;
      aos_string_t object;
      aos_str_set(&bucket, bucket_.ptr());
      aos_str_set(&object, object_.ptr());
      aos_table_t *resp_headers = NULL;
      aos_status_t *aos_ret = NULL;
      aos_table_t *headers = NULL;

      if(NULL == (headers = aos_table_make(aos_pool_, AOS_TABLE_INIT_SIZE))) {
        ret = OB_OSS_ERROR;
        OB_LOG(WARN, "fail to make aos table", K(ret));
      } else {
        apr_table_set(headers, OSS_META_MD5, md5_str);
        if(NULL == (aos_ret = oss_copy_object(oss_option_, &bucket, &object, &bucket, &object,
            headers, &resp_headers)) || !aos_status_is_ok(aos_ret)) {
          ret = OB_OSS_ERROR;
          OB_LOG(WARN, "fail to copy object with md5", K(bucket_), K(object_), K(ret));
          print_oss_info(resp_headers, aos_ret);
          cleanup();
        }
      }
    }
  }

  reset();

  const int64_t total_cost_time = ObTimeUtility::current_time() - start_time;
  if (total_cost_time > 3 * 1000 * 1000) {
    OB_LOG_RET(WARN, OB_ERR_TOO_MUCH_TIME, "oss writer close cost too much time", K(total_cost_time), K(ret));
  }

  return ret;
}

int ObStorageOssMultiPartWriter::cleanup()
{
  int ret = OB_SUCCESS;
  if(!is_inited()) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "oss client not inited", K(ret));
  } else if(!is_opened_) {
    ret = OB_OSS_ERROR;
    OB_LOG(WARN, "oss writer cannot cleanup before it is opened", K(ret));
  } else {
    aos_string_t bucket;
    aos_string_t object;
    aos_str_set(&bucket, bucket_.ptr());
    aos_str_set(&object, object_.ptr());
    aos_table_t *resp_headers = NULL;
    aos_status_t *aos_ret = NULL;

    if(NULL == (aos_ret = oss_abort_multipart_upload(oss_option_, &bucket, &object,
        &upload_id_, &resp_headers)) || !aos_status_is_ok(aos_ret)) {
      ret = OB_OSS_ERROR;
      OB_LOG(WARN, "Abort the multipart error", K(bucket_), K(object_), K(ret));
    }
  }
  return ret;
}

ObStorageOssReader::ObStorageOssReader():
    bucket_(),
    object_(),
    file_length_(-1),
    is_opened_(false),
    allocator_(ObModIds::BACKUP)
{
}

ObStorageOssReader::~ObStorageOssReader()
{
}

int ObStorageOssReader::open(const ObString &uri, common::ObObjectStorageInfo *storage_info)
{
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;
  bool is_file_exist = false;
  char *remote_md5 = NULL;
  int64_t file_length = -1;

  if(uri.empty()) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "uri is empty", K(ret));
  } else if(is_opened_) {
    ret = OB_OSS_ERROR;
    OB_LOG(WARN, "already open, cannot open again", K(ret));
  } else if (OB_FAIL(init_with_storage_info(storage_info))) {
    OB_LOG(WARN, "failed to init oss", K(ret));
  } else if (OB_SUCCESS != (ret = get_bucket_object_name(uri, bucket_, object_, allocator_))) {
    OB_LOG(WARN, "bucket name of object name is empty", K(ret));
  } else if (OB_SUCCESS != (ret = get_oss_file_meta(bucket_, object_, is_file_exist,
      remote_md5, file_length))) {
    OB_LOG(WARN, "fail to get file meta", K(bucket_), K(object_), K(ret));
  } else if (!is_file_exist) {
    ret = OB_BACKUP_FILE_NOT_EXIST;
    OB_LOG(WARN, "backup file is not exist", K(bucket_), K(object_), K(ret));
  } else {
    file_length_ = file_length;
    is_opened_ = true;
  }
  return ret;
}

int ObStorageOssReader::pread(
    char *buf,const int64_t buf_size, int64_t offset, int64_t &read_size)
{
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;
  aos_pool_t *aos_pool = NULL;
  oss_request_options_t *oss_option = NULL;

  if (!is_inited()) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "oss client not inited", K(ret));
  } else if (NULL == buf || buf_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "buf is NULL or buf_size is invalid", K(buf_size), K(ret));
  } else if (!is_opened_) {
    ret = OB_OSS_ERROR;
    OB_LOG(WARN, "oss reader cannot read before it is opened", K(ret));
  } else if (OB_FAIL(init_oss_options(aos_pool, oss_option))) {
    OB_LOG(WARN, "fail to init oss options", K(aos_pool), K(oss_option), K(ret));
  } else if (OB_ISNULL(aos_pool) || OB_ISNULL(oss_option)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "aos pool or oss option is NULL", K(aos_pool), K(oss_option), K(ret));
  } else {
    if (file_length_ == offset) {
      read_size = 0;
    } else if ( file_length_ < offset) {
      ret = OB_FILE_LENGTH_INVALID;
      OB_LOG(WARN, "File lenth is invilid", K(file_length_), K(offset),
          K(bucket_), K(object_), K(ret));
    } else if (file_length_ - offset > 0) {
      const int64_t start_time = ObTimeUtility::current_time();
      int64_t get_data_size = std::min(buf_size, file_length_ - offset);
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
      //oss read size is [10, 100] include the 10 and 100 bytes
      //we except is [10, 100) not include the end, so we subtraction 1 to the end
      int n = snprintf(range_size, OSS_RANGE_SIZE, "bytes=%ld-%ld", offset, offset + get_data_size - 1);
      if(n <=0 || n >= OSS_RANGE_SIZE) {
        ret = OB_SIZE_OVERFLOW;
        OB_LOG(WARN, "fail to get range size", K(n), K(OSS_RANGE_SIZE), K(ret));
      } else if (NULL == (headers = aos_table_make(aos_pool, AOS_TABLE_INIT_SIZE))) {
        ret = OB_OSS_ERROR;
        OB_LOG(WARN, "fail to make oss headers", K(ret));
      } else {
        apr_table_set(headers, OSS_RANGE_KEY, range_size);

        if (NULL == (aos_ret = oss_get_object_to_buffer(oss_option, &bucket, &object, headers, params,
            &buffer, &resp_headers)) ||  !aos_status_is_ok(aos_ret)) {
            convert_io_error(aos_ret, ret);
          OB_LOG(WARN, "fail to get object to buffer", K(bucket_), K(object_), K(ret));
          print_oss_info(resp_headers, aos_ret);
        } else {
          //check date len
          aos_list_for_each_entry(aos_buf_t, content, &buffer, node) {
            len += aos_buf_size(content);
          }
          if(len != get_data_size) {
            ret = OB_OSS_ERROR;
            OB_LOG(WARN, "get data size error", K(get_data_size), K(len), K(bucket_),
                K(object_), K(ret));
          } else {
            //copy to buf
            aos_list_for_each_entry(aos_buf_t, content, &buffer, node) {
              size = aos_buf_size(content);
              if (buf_pos + size > get_data_size) {
                ret = OB_SIZE_OVERFLOW;
                OB_LOG(WARN, "the size is too long", K(buf_pos), K(size), K(get_data_size), K(ret));
                break;
              } else {
                memcpy(buf + buf_pos, content->pos, (size_t)size);
                buf_pos += size;
              }
            }

            read_size = get_data_size;
          }
        }
      }
      bool is_slow = false;
      print_access_storage_log("oss read one part ", object_, start_time, get_data_size, &is_slow);
      if (is_slow) {
        print_oss_info(resp_headers, aos_ret);
      }
    }//if(file_length_ - file_offset_ > 0)
  }
  if(NULL != aos_pool) {
    aos_pool_destroy(aos_pool);
  }

  return ret;
}

int ObStorageOssReader::close()
{
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;
  //1 for store the last '\0'
  if(!is_inited()) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "oss client not inited", K(ret));
  } else if(!is_opened_) {
    ret = OB_OSS_ERROR;
    OB_LOG(WARN, "oss reader cannot close before it is opened", K(ret));
  } else {
    is_opened_ = false;
    file_length_ = -1;
    // Release memory
    allocator_.clear();

    reset();
  }

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
  if (!is_opened_) {
    OB_LOG_RET(WARN, OB_ERR_UNEXPECTED, "oss util cannot close before it is opened");
  } else {
    is_opened_ = false;
    storage_info_ = NULL;
  }
}

int ObStorageOssUtil::is_exist(const ObString &uri, bool &exist)
{
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;
  exist = false;
  ObString bucket_ob_string;
  ObString object_ob_string;
  common::ObArenaAllocator allocator(ObModIds::RESTORE);
  char *remote_md5 = NULL;
  int64_t file_length = 0;
  ObStorageOssBase oss_base;

  if (uri.empty()) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "name is empty", K(ret));
  } else if (!is_opened_) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "oss util is not inited", K(ret), K(uri));
  } else if (OB_FAIL(oss_base.init_with_storage_info(storage_info_))) {
    OB_LOG(WARN, "fail to init oss base with account", K(ret), K(uri));
  } else if (OB_FAIL(get_bucket_object_name(uri, bucket_ob_string,
      object_ob_string, allocator))) {
    OB_LOG(WARN, "bucket or object name is empty",
        K(bucket_ob_string), K(object_ob_string), K(ret));
  } else if (OB_FAIL(oss_base.get_oss_file_meta(bucket_ob_string, object_ob_string, exist,
      remote_md5, file_length))) {
    OB_LOG(WARN, "fail to get file meta info",
                K(bucket_ob_string), K(object_ob_string), K(ret));
  }

  return ret;
}

int ObStorageOssUtil::get_file_length(const common::ObString &uri, int64_t &file_length)
{
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;
  bool exist = false;
  ObString bucket_ob_string;
  ObString object_ob_string;
  common::ObArenaAllocator allocator;
  char *remote_md5 = NULL;
  ObStorageOssBase oss_base;
  file_length = 0;

  if (uri.empty()) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "name is empty", K(ret));
  } else if (!is_opened_) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "oss util is not inited", K(ret), K(uri));
  } else if (OB_FAIL(oss_base.init_with_storage_info(storage_info_))) {
    OB_LOG(WARN, "fail to init oss base with account", K(ret), K(uri));
  } else if (OB_SUCCESS != (ret = get_bucket_object_name(uri, bucket_ob_string,
      object_ob_string, allocator))) {
    OB_LOG(WARN, "bucket or object name is empty",
        K(bucket_ob_string), K(object_ob_string), K(ret));
  } else if (OB_FAIL(oss_base.get_oss_file_meta(bucket_ob_string, object_ob_string, exist,
      remote_md5, file_length))) {
    OB_LOG(WARN, "fail to get file meta info",
        K(bucket_ob_string), K(object_ob_string), K(ret));
  } else if (!exist) {
    ret = OB_BACKUP_FILE_NOT_EXIST;
    OB_LOG(WARN, "backup file is not exist", K(bucket_ob_string), K(object_ob_string), K(ret));
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
  common::ObArenaAllocator allocator;
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
      oss_base.print_oss_info(head_resp_headers, aos_ret);
    } else {
      aos_list_for_each_entry(oss_tag_content_t, b, &tag_list, node) {
        char key_str[OB_MAX_TAGGING_STR_LENGTH];
        char value_str[OB_MAX_TAGGING_STR_LENGTH];
        if (OB_FAIL(databuff_printf(key_str, OB_MAX_TAGGING_STR_LENGTH , "%.*s",b->key.len, b->key.data))) {
          OB_LOG(WARN, "failed to databuff printf key str", K(ret));
        } else if (OB_FAIL(databuff_printf(value_str, OB_MAX_TAGGING_STR_LENGTH, "%.*s", b->key.len, b->value.data))) {
          OB_LOG(WARN, "failed to databuff printf value str", K(ret));
        } else if (0 == strcmp("delete_mode", key_str) && 0 == strcmp("tagging", value_str)) {
          is_tagging = true;
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
    oss_base.print_oss_info(resp_headers, aos_ret);
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
      oss_base.print_oss_info(head_resp_headers, aos_ret);
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
  common::ObArenaAllocator allocator;
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

int handle_oss_file_name(const char* file_name_data, int file_name_len,
      common::ObBaseDirEntryOperator &op)
{
   int ret = OB_SUCCESS;
   dirent entry;
   entry.d_type = DT_REG;

   if (NAME_MAX < file_name_len) {
     ret = OB_INVALID_ARGUMENT;
     OB_LOG(WARN, "file name is too long", K(file_name_len));
   } else {
     MEMCPY(entry.d_name, file_name_data, file_name_len);
     entry.d_name[file_name_len] = '\0'; // set str end
     if (OB_FAIL(op.func(&entry))) { 
      OB_LOG(WARN, "fail to exe application callback", K(ret));
     }
   }
   return ret;
} 

int ObStorageOssUtil::list_files(const common::ObString &dir_path,
    common::ObBaseDirEntryOperator &op)
{
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;
  common::ObArenaAllocator tmp_allocator;
  ObString bucket_str;
  ObString object_str;
  char object_dir_str[OB_MAX_URI_LENGTH] = {0};
  const char *object_dir_ptr = NULL;
  aos_status_t *aos_ret = NULL;
  oss_list_object_params_t *params = NULL;
  const char *next_marker = "/";
  const char end_marker = '\0';
  //object_str end with '\0', so object_str.length() need > 2
  const int64_t min_object_str_len = 2;
  ObStorageOssBase oss_base;

  if (dir_path.empty()) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "name is empty", K(ret), K(dir_path));
  } else if (!is_opened_) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "oss util is not inited", K(ret), K(dir_path));
  } else if (OB_FAIL(oss_base.init_with_storage_info(storage_info_))) {
    OB_LOG(WARN, "fail to init oss base with account", K(ret), K(dir_path));
  } else if (OB_FAIL(get_bucket_object_name(dir_path, bucket_str, object_str, tmp_allocator))) {
    OB_LOG(WARN, "bucket or object name is empty", K(ret), K(dir_path), K(bucket_str), K(object_str));
  } else if (object_str.length() < min_object_str_len) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "object str length is unepxected", K(ret), K(object_str), "length", object_str.length());
  } else {
    //make path end with '/'
    if (object_str.ptr()[object_str.length() - 1] != end_marker) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "object str not end with '\0'", K(ret), K(object_str));
    } else if (object_str.ptr()[object_str.length() - 2] == *next_marker) {
      object_dir_ptr = object_str.ptr();
    } else {
      int n = snprintf(object_dir_str, OB_MAX_URI_LENGTH, "%s/", object_str.ptr());
      if (n <= 0 || n >= OB_MAX_URI_LENGTH) {
        ret = OB_SIZE_OVERFLOW;
        OB_LOG(WARN, "fail to deep copy object", K(object_str), K(object_str.length()),
            K(n), K(OB_MAX_URI_LENGTH), K(ret));
      } else {
        object_dir_ptr = object_dir_str;
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(params = oss_create_list_object_params(oss_base.aos_pool_))) {
    ret = OB_OSS_ERROR;
    OB_LOG(WARN, "fail to create list object params", K(ret), K(dir_path));
  } else if (OB_ISNULL(object_dir_ptr)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "object_dir_ptr is null", K(ret), K(dir_path));
  } else {
    aos_str_set(&params->prefix, object_dir_ptr);
    aos_str_set(&params->marker, next_marker);
    aos_string_t bucket;
    aos_str_set(&bucket, bucket_str.ptr());
    oss_list_object_content_t *content = NULL;
    ObString tmp_string;
    aos_table_t *resp_headers = NULL;


    do {
      tmp_string.reset();
      if (OB_ISNULL(aos_ret = oss_list_object(oss_base.oss_option_, &bucket, params, &resp_headers))
      || !aos_status_is_ok(aos_ret)) {
        convert_io_error(aos_ret, ret);
        OB_LOG(WARN, "fail to list all object", K(ret));
        oss_base.print_oss_info(resp_headers, aos_ret);
      } else {
        aos_list_for_each_entry(oss_list_object_content_t, content, &params->object_list, node) {
          //key.data has prefix object_str, we only get the file name
          tmp_string.assign(content->key.data, content->key.len);
          const int64_t object_len = strlen(object_dir_ptr);
          if (0 != MEMCMP(tmp_string.ptr(), object_dir_ptr, object_len)) {
            ret = OB_OSS_ERROR;
            OB_LOG(WARN, "the date has no object prefix", K(ret), K(tmp_string),
                K(object_str), K(tmp_string.length()), K(object_len));
          } else {
            const int64_t file_name_pos = object_len;
            const int32_t name_length = static_cast<int32_t>(content->key.len - file_name_pos);
            ObString tmp_file_name(name_length, name_length, content->key.data + file_name_pos);
            if (op.need_get_file_size()) {
              const int64_t object_size = aos_atoi64(content->size.data);
              if (0 > object_size) {
                ObString tmp_size_string(content->size.data);
                ret = OB_OSS_ERROR;
                OB_LOG(WARN, "invalid object size", K(ret), K(object_size), K(tmp_size_string), K(tmp_file_name));
              } else {
                op.set_size(object_size);
              }
            }
            if (OB_FAIL(handle_oss_file_name(tmp_file_name.ptr(), tmp_file_name.length(), op))) {
              OB_LOG(WARN, "fail to handle oss file name", K(ret), K(tmp_file_name));
            } else {
              OB_LOG(DEBUG, "get file name", K(tmp_string));
            }
          }
          if (OB_FAIL(ret)) {
            break;
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (NULL == (next_marker = apr_psprintf(oss_base.aos_pool_, "%.*s", params->next_marker.len, params->next_marker.data))) {
          ret = OB_OSS_ERROR;
          OB_LOG(WARN, "next marker is NULL", K(ret), KP(next_marker));
        } else {
          aos_str_set(&params->marker, next_marker);
          aos_list_init(&params->object_list);
          aos_list_init(&params->common_prefix_list);
        }
      }
    } while (AOS_TRUE == params->truncated && OB_SUCC(ret));
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
int ObStorageOssUtil::strtotime(const char *date_time, int64_t &time)
{
  int ret = OB_SUCCESS;
  time = 0;
  struct tm tm_time;
  memset(&tm_time, 0, sizeof(struct tm));
  if (OB_ISNULL(date_time)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "strtotime get invalid argument", K(ret), KP(date_time));
  } else if (OB_ISNULL(strptime(date_time, "%a, %d %b %Y %H:%M:%S %Z", &tm_time))) {
    //skip set ret, for compat data formate
    OB_LOG(WARN, "failed to strptime", K(ret), K(*date_time));
  } else {
    time = mktime(&tm_time);
  }
  return ret;
}

int ObStorageOssUtil::list_directories(
      const common::ObString &dir_path,
      common::ObBaseDirEntryOperator &op)
{
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;
  common::ObArenaAllocator tmp_allocator;
  ObString bucket_str;
  ObString object_str;
  char object_dir_str[OB_MAX_URI_LENGTH] = {0};
  const char *object_dir_ptr = NULL;
  aos_status_t *aos_ret = NULL;
  oss_list_object_params_t *params = NULL;
  const char *next_marker = "";
  const char end_marker = '\0';
  const char *delimiter = "/";
  //object_str end with '\0', so object_str.length() need > 2
  const int64_t min_object_str_len = 2;
  ObStorageOssBase oss_base;

  if (dir_path.empty()) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "name is empty", K(ret), K(dir_path));
  } else if (!is_opened_) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "oss util is not inited", K(ret), K(dir_path));
  } else if (OB_FAIL(oss_base.init_with_storage_info(storage_info_))) {
    OB_LOG(WARN, "fail to init oss base with account", K(ret), K(dir_path));
  } else if (OB_FAIL(get_bucket_object_name(dir_path, bucket_str, object_str, tmp_allocator))) {
    OB_LOG(WARN, "bucket or object name is empty", K(ret), K(dir_path), K(bucket_str), K(object_str));
  } else if (object_str.length() < min_object_str_len) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "object str length is unepxected", K(ret), K(object_str), "length", object_str.length());
  } else {
    //make path end with '/'
    if (object_str.ptr()[object_str.length() - 1] != end_marker) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "object str not end with '\0'", K(ret), K(object_str));
    } else if (object_str.ptr()[object_str.length() - 2] == *delimiter) {
      object_dir_ptr = object_str.ptr();
    } else {
      int n = snprintf(object_dir_str, OB_MAX_URI_LENGTH, "%s/", object_str.ptr());
      if (n <= 0 || n >= OB_MAX_URI_LENGTH) {
        ret = OB_SIZE_OVERFLOW;
        OB_LOG(WARN, "fail to deep copy object", K(object_str), K(object_str.length()),
            K(n), K(OB_MAX_URI_LENGTH), K(ret));
      } else {
        object_dir_ptr = object_dir_str;
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(params = oss_create_list_object_params(oss_base.aos_pool_))) {
    ret = OB_OSS_ERROR;
    OB_LOG(WARN, "fail to create list object params", K(ret), K(dir_path));
  } else if (OB_ISNULL(object_dir_ptr)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "object_dir_ptr is null", K(ret), K(dir_path));
  } else {
    aos_str_set(&params->prefix, object_dir_ptr);
    aos_str_set(&params->marker, next_marker);
    aos_str_set(&params->delimiter, delimiter);
    aos_string_t bucket;
    aos_str_set(&bucket, bucket_str.ptr());
    aos_table_t *resp_headers;
    oss_list_object_common_prefix_t *common_prefix = NULL;
    ObString tmp_string;
    ObString directory_name;

    do {
      tmp_string.reset();
      directory_name.reset();
      if (OB_ISNULL(aos_ret = oss_list_object(oss_base.oss_option_, &bucket, params, &resp_headers)) || !aos_status_is_ok(aos_ret)) {
        convert_io_error(aos_ret, ret);
        OB_LOG(WARN, "fail to list all object", K(ret));
        oss_base.print_oss_info(resp_headers, aos_ret);
      } else {
        aos_list_for_each_entry(oss_list_object_common_prefix_t, common_prefix, &params->common_prefix_list, node) {
          //key.data has prefix object_str, we only get the file name
          tmp_string.assign(common_prefix->prefix.data, common_prefix->prefix.len);
          const int64_t object_len = strlen(object_dir_ptr);
          const int64_t prefix_len = strlen(common_prefix->prefix.data);
          if (prefix_len < object_len) {
            ret = OB_OSS_ERROR;
            OB_LOG(WARN, "the object prefix len should not be smaller than directory len", K(ret), K(tmp_string),
                K(object_str), K(tmp_string.length()), K(object_len), K(object_len), K(prefix_len));
          } else if (0 != MEMCMP(tmp_string.ptr(), object_dir_ptr, object_len)) {
            ret = OB_OSS_ERROR;
            OB_LOG(WARN, "the date has no object prefix", K(ret), K(tmp_string),
                K(object_str), K(tmp_string.length()), K(object_len));
          } else  if (*delimiter != tmp_string.ptr()[common_prefix->prefix.len - 1]) {
            ret = OB_OSS_ERROR;
            OB_LOG(WARN, "the date has no directory", K(ret), K(tmp_string),
                K(object_str), K(tmp_string.length()), K(object_len));
          } else {
            const int64_t directory_name_pos = object_len;
            const int32_t name_length = static_cast<int32_t>(common_prefix->prefix.len - directory_name_pos) - 1; //remove '/'
            if (name_length <= 0) {
              ret = OB_OSS_ERROR;
              OB_LOG(WARN, "the object do not exist", K(ret), K(tmp_string),
                  K(object_str), K(tmp_string.length()), K(object_len), K(name_length), K(directory_name_pos));
            } else {
              ObString tmp_directory_name(name_length, name_length, common_prefix->prefix.data + directory_name_pos);
              dirent entry;
              entry.d_type = DT_DIR;
              if (OB_FAIL(databuff_printf(entry.d_name, sizeof(entry.d_name), 
                                "%.*s", tmp_directory_name.length(), tmp_directory_name.ptr()))) {
                OB_LOG(WARN, "fail to copy name to entry", K(ret), K(tmp_directory_name));
              } else if (OB_FAIL(op.func(&entry))) {
                OB_LOG(WARN, "fail to call operation callback", K(ret), K(tmp_directory_name));
              }
            }
            OB_LOG(DEBUG, "get directory name", K(tmp_string), K(directory_name));
          }
          if (OB_FAIL(ret)) {
            break;
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (NULL == (next_marker = apr_psprintf(oss_base.aos_pool_, "%.*s", params->next_marker.len, params->next_marker.data))) {
          ret = OB_OSS_ERROR;
          OB_LOG(WARN, "next marker is NULL", K(ret), KP(next_marker));
        } else {
          aos_str_set(&params->marker, next_marker);
          aos_list_init(&params->object_list);
          aos_list_init(&params->common_prefix_list);
        }
      }
    } while (AOS_TRUE == params->truncated && OB_SUCC(ret));
  }

  OB_LOG(INFO, "list directory count", K(dir_path));
  return ret;
}



ObStorageOssAppendWriter::ObStorageOssAppendWriter()
  : is_opened_(false),
    file_length_(-1),
    allocator_(ObModIds::BACKUP),
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
  } else if(NULL == buf || size < 0) {
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
  } else if(NULL == buf || size < 0 || offset < 0) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "buf is NULL or size is invalid", KP(buf), K(size), K(ret));
  } else if (OB_FAIL(do_write(buf, size, offset, is_pwrite))) {
    OB_LOG(WARN, "failed to do write", K(ret), KP(buf), K(size), K(offset));
  }
  return ret;
}

int ObStorageOssAppendWriter::close()
{
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;

  if(!is_inited()) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "oss client not inited", K(ret));
  } else if(!is_opened_) {
    ret = OB_OSS_ERROR;
    OB_LOG(WARN, "oss writer cannot close before it is opened");
  } else {
    is_opened_ = false;
    // �ͷ��ڴ�
    allocator_.clear();

    reset();
  }

  return ret;
}

int ObStorageOssAppendWriter::do_write(const char *buf, const int64_t size, const int64_t offset, const bool is_pwrite)
{
  int ret = OB_SUCCESS;

  const int64_t start_time = ObTimeUtility::current_time();
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "oss client not inited", K(ret));
  } else if(NULL == buf || size < 0) {
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
    if(OB_ISNULL(headers1 = aos_table_make(aos_pool_, 0))) {
      ret = OB_OSS_ERROR;
      OB_LOG(WARN, "fail to make apr table", K(ret));
    } else if(OB_ISNULL(aos_ret = oss_head_object(oss_option_, &bucket, &object, headers1, &resp_headers))) {
      print_oss_info(resp_headers, aos_ret);
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
          position = aos_atoi64(next_append_position);
          if (0 > position) {
            ObString tmp_position_string(next_append_position);
            ret = OB_OSS_ERROR;
            OB_LOG(WARN, "invalid append position", K(ret), K(position), K(tmp_position_string));
          }
        }
      }

      if (OB_SUCC(ret) && is_pwrite && position != offset) {
        ret = OB_BACKUP_PWRITE_OFFSET_NOT_MATCH;
        OB_LOG(WARN, "position and offset do not match", K(ret), K(position), K(offset));
      }

      if (OB_SUCC(ret)) {
        if(OB_ISNULL(headers2 = aos_table_make(aos_pool_, AOS_TABLE_INIT_SIZE))) {
          ret = OB_OSS_ERROR;
          OB_LOG(WARN, "fail to make apr table", K(ret));
        } else if (OB_ISNULL(content = aos_buf_pack(aos_pool_, buf, static_cast<int32_t>(size)))) {
          ret = OB_OSS_ERROR;
          OB_LOG(WARN, "fail to pack buf", K(content), K(ret));
        } else {
          aos_list_add_tail(&content->node, &buffer);
          aos_ret = oss_append_object_from_buffer(oss_option_, &bucket, &object, position, &buffer,
              headers2, &resp_headers);
          if (0 != aos_status_is_ok(aos_ret)) { // != 0 means ok
            file_length_ += size;
          } else {
            print_oss_info(resp_headers, aos_ret);
            convert_io_error(aos_ret, ret);
            OB_LOG(WARN, "fail to append", K(content), K(ret));

            // If append failed, print the current object meta, to help debugging.
            aos_table_t *headers3 = NULL;
            if(OB_NOT_NULL(headers3 = aos_table_make(aos_pool_, 0))) {
              if(OB_NOT_NULL(aos_ret = oss_head_object(oss_option_, &bucket, &object, headers3, &resp_headers))) {
                if ((0 != aos_status_is_ok(aos_ret))) {
                  char *append_pos_str = (char*)(apr_table_get(resp_headers, OSS_NEXT_APPEND_POSITION));
                  int64_t cur_pos = aos_atoi64(append_pos_str);
                  OB_LOG(WARN, "after append fail, we got the object meta", K(cur_pos));
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
        print_oss_info(resp_headers, aos_ret);
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
    allocator_(ObModIds::BACKUP),
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

  if(!is_opened_) {
    ret = OB_OSS_ERROR;
    OB_LOG(WARN, "oss writer cannot close before it is opened");
  } else {
    is_opened_ = false;
    // Release memory
    allocator_.clear();
    file_length_ = -1;
	  reset();
  }

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
  if (!is_opened_) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "oss writer not opened", K(ret));
  } else if(NULL == buf || size < 0) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "buf is NULL or size is invalid", KP(buf), K(size), K(ret));
  }  else {
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
    } else {
      aos_list_add_tail(&content->node, &buffer);
      if(OB_ISNULL(aos_ret = oss_put_object_from_buffer(oss_option_, &bucket, &object,
                   &buffer, headers, &resp_headers)) || !aos_status_is_ok(aos_ret)) {
        convert_io_error(aos_ret, ret);
        OB_LOG(WARN, "fail to upload one object", K(bucket_), K(object_), K(ret));
        print_oss_info(resp_headers, aos_ret);

      } else {
        file_length_ = size;
      }
    }
    //print slow info
    bool is_slow = false;
    print_access_storage_log("oss upload one object ", object_, start_time, size, &is_slow);
    if (is_slow) {
      print_oss_info(resp_headers, aos_ret);
    }
  }
  return ret; 
}



}//tools
}//oceanbase
