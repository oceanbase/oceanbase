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

#include "ob_storage_obdal_base.h"
#include "ob_storage.h"

namespace oceanbase
{
namespace common
{

int init_obdal_env()
{
  return ObDalEnvIniter::get_instance().global_init();
}

void fin_obdal_env()
{
  // wait doing io finish before destroy obdal env.
  const int64_t start_time = ObTimeUtility::current_time();
  const int64_t timeout = ObExternalIOCounter::FLYING_IO_WAIT_TIMEOUT;
  int64_t flying_io_cnt = ObExternalIOCounter::get_flying_io_cnt();
  while(0 < flying_io_cnt) {
    const int64_t end_time = ObTimeUtility::current_time();
    if (end_time - start_time > timeout) {
      OB_LOG(INFO, "force fin_obdal_env", K(flying_io_cnt));
      break;
    }
    ob_usleep(100 * 1000L); // 100ms
    flying_io_cnt = ObExternalIOCounter::get_flying_io_cnt();
  }
  ObDalEnvIniter::get_instance().global_destroy();
}

//========================= ObDalEnvIniter =========================
ObDalEnvIniter::ObDalEnvIniter()
  : lock_(common::ObLatchIds::OBJECT_DEVICE_LOCK),
    is_global_inited_(false)
{
}

ObDalEnvIniter &ObDalEnvIniter::get_instance()
{
  static ObDalEnvIniter initer;
  return initer;
}

void *obdal_malloc(std::size_t size, std::size_t align)
{
  void *ptr = nullptr;
  ObMemAttr attr;
  attr.label_ = OB_DAL_SDK;
  const int64_t tenant_id = ObDalAccessor::obdal_get_tenant_id();
  if (size < OBDAL_MALLOC_BIG_SIZE) {
    attr.tenant_id_ = OB_SERVER_TENANT_ID;
  } else {
    attr.tenant_id_ = tenant_id;
  }
  SET_IGNORE_MEM_VERSION(attr);
  do {
    // ptr = ObDalMemoryManager::get_instance().allocate(size, align);
    ptr = ob_malloc_align(align, size, attr);
    if (OB_ISNULL(ptr)) {
      ob_usleep(10000);   // 10ms
      if (TC_REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
        OB_LOG_RET(ERROR, OB_ALLOCATE_MEMORY_FAILED, "obdal failed to allocate memory", K(size), K(align));
      }
    }
  } while (OB_ISNULL(ptr));
  return ptr;
}

void obdal_free(void *ptr)
{
  // return ObDalMemoryManager::get_instance().free(ptr);
  return ob_free_align(ptr);
}

void obdal_log_handler(const char *level, const char *message) {
  int ret = OB_SUCCESS;
  const char *format = "[ObDal], %s";
  if (OB_ISNULL(level) || OB_ISNULL(message)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "obdal log handler is called without level or message", K(ret), KP(level), KP(message));
  } else if (STRCMP(level, "INFO") == 0) {
    _OB_LOG(INFO, format, message);
  } else if (STRCMP(level, "TRACE") == 0) {
    _OB_LOG(TRACE, format, message);
  } else if (STRCMP(level, "DEBUG") == 0) {
    _OB_LOG(DEBUG, format, message);
  } else if (STRCMP(level, "WARN") == 0) {
    _OB_LOG(WARN, format, message);
  } else if (STRCMP(level, "ERROR") == 0) {
    _OB_LOG(ERROR, format, message);
  }
}

static int64_t get_obdal_thread_cnt()
{
  int64_t thread_cnt = 4;
  const int64_t cpu_num = get_cpu_num();
  if (cpu_num <= 16) {
    thread_cnt = max(thread_cnt, cpu_num);
  } else {
    thread_cnt = 16 + (cpu_num - 16) / 3;
    thread_cnt = min(thread_cnt, 256);
  }
  return thread_cnt;
}

int ObDalEnvIniter::global_init()
{
  int ret = OB_SUCCESS;
  common::SpinWLockGuard guard(lock_);

  if (is_global_inited_) {
    ret = OB_INIT_TWICE;
    OB_LOG(WARN, "cannot init twice", K(ret));
  } else if (OB_FAIL(ObDalAccessor::init_env(reinterpret_cast<void *>(obdal_malloc),
                                             reinterpret_cast<void *>(obdal_free),
                                             reinterpret_cast<void *>(obdal_log_handler),
                                             OB_LOGGER.get_level(),
                                             get_obdal_thread_cnt(),
                                             POOL_MAX_IDLE_PER_HOST,
                                             POOL_MAX_IDLE_TIME_S,
                                             CONNECT_TIMEOUT_S))) {
    OB_LOG(WARN, "failed init obdal env", K(ret));
  } else {
    signal(SIGPIPE, SIG_IGN);
    OB_LOG(INFO, "obdal env init success", K(ret));
    is_global_inited_ = true;
  }
  return ret;
}

void ObDalEnvIniter::global_destroy()
{
  int ret = OB_SUCCESS;
  common::SpinWLockGuard guard(lock_);
  if (is_global_inited_) {
    ObDalAccessor::fin_env();
    OB_LOG(INFO, "obdal env fin success", K(ret));
    is_global_inited_ = false;
  }
}

//========================= ObDalAccount =========================
ObDalAccount::ObDalAccount()
{
  reset();
}

ObDalAccount::~ObDalAccount()
{
  if (is_valid_) {
    reset();
  }
}

void ObDalAccount::reset()
{
  ObStorageAccount::reset();
  MEMSET(region_, 0, sizeof(region_));
  addressing_model_ = ObStorageAddressingModel::OB_VIRTUAL_HOSTED_STYLE;
}

int ObDalAccount::assign(const ObObjectStorageInfo *storage_info)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_ISNULL(storage_info) || OB_UNLIKELY(!storage_info->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "storage info is invalid", K(ret), KPC(storage_info));
  } else if (OB_FAIL(storage_info->to_account(*this))) {
    OB_LOG(WARN, "failed from storage_info to account", K(ret), KPC(storage_info));
  } else if (STRLEN(storage_info->region_) != 0 && OB_FAIL(ob_set_field(storage_info->region_ + strlen(REGION), region_, sizeof(region_)))) {
    OB_LOG(WARN, "failed set region", K(ret), K(storage_info->region_), KPC(storage_info));
  } else {
    addressing_model_ = storage_info->addressing_model_;
  }

  if (OB_SUCC(ret)) {
    is_valid_ = true;
    OB_LOG(DEBUG, "succeed to init obdal account",
        KCSTRING(region_), KCSTRING(endpoint_), KCSTRING(access_id_));
  } else {
    reset();
  }
  return ret;
}

//========================= ObStorageObDalBase =========================
ObStorageObDalBase::ObStorageObDalBase()
  : is_inited_(false),
    is_write_with_if_match_(false),
    allocator_(OB_STORAGE_OBDAL_ALLOCATOR, OB_MALLOC_NORMAL_BLOCK_SIZE, ObObjectStorageTenantGuard::get_tenant_id()),
    storage_type_(ObStorageType::OB_STORAGE_MAX_TYPE),
    bucket_(),
    object_(),
    obdal_account_(),
    checksum_type_(ObStorageChecksumType::OB_NO_CHECKSUM_ALGO),
    options_(nullptr),
    op_(nullptr)
{
}

ObStorageObDalBase::~ObStorageObDalBase()
{
  ObStorageObDalBase::reset();
}

void ObStorageObDalBase::reset()
{
  int ret = OB_SUCCESS;
  is_inited_ = false;
  allocator_.clear();
  storage_type_ = ObStorageType::OB_STORAGE_MAX_TYPE;
  bucket_.reset();
  object_.reset();
  obdal_account_.reset();
  checksum_type_ = ObStorageChecksumType::OB_NO_CHECKSUM_ALGO;
  if (OB_NOT_NULL(options_)) {
    if (OB_FAIL(ObDalAccessor::obdal_operator_options_free(options_))) {
      OB_LOG(WARN, "failed to free obdal options", K(ret));
    }
    options_ = nullptr;
  }
  if (OB_NOT_NULL(op_)) {
    if (OB_FAIL(ObDalAccessor::obdal_operator_free(op_))) {
      OB_LOG(WARN, "failed to free obdal operator", K(ret));
    }
    op_ = nullptr;
  }
}

int ObStorageObDalBase::open(const ObString &uri, ObObjectStorageInfo *storage_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    OB_LOG(WARN, "obdal base alreagy inited", K(ret));
  } else if (OB_FAIL(inner_open(uri, storage_info))) {
    OB_LOG(WARN, "failed to inner open", K(ret), K(uri), KPC(storage_info));
  }

  // object name should not be empty
  if (OB_SUCC(ret) && OB_UNLIKELY(object_.empty())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "object name is empty", K(uri), K(ret), K(uri));
    reset();
  }
  return ret;
}

int set_obdal_options_with_account(
    opendal_operator_options *options,
    const ObStorageType storage_type,
    const ObDalAccount &obdal_account,
    const ObString &bucket)
{
  int ret = OB_SUCCESS;
  const int64_t tenant_id = ObObjectStorageTenantGuard::get_tenant_id();
  static constexpr int INTEGER_BUF_LEN = 32;
  char tenant_id_str[INTEGER_BUF_LEN] = {0};
  if (OB_ISNULL(options)
      || OB_UNLIKELY(!obdal_account.is_valid()
      || bucket.empty())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), KP(options), K(obdal_account), K(bucket));
  } else if (OB_FAIL(databuff_printf(tenant_id_str, INTEGER_BUF_LEN, "%ld", tenant_id))) {
    OB_LOG(WARN, "failed to set tenant id", K(ret), K(tenant_id));
  } else {
    if (storage_type == ObStorageType::OB_STORAGE_S3) {
      if (OB_FAIL(ObDalAccessor::obdal_operator_options_set(options, "bucket", bucket.ptr()))) {
        OB_LOG(WARN, "failed to set bucket", K(ret), K(bucket));
      } else if (OB_FAIL(ObDalAccessor::obdal_operator_options_set(options, "endpoint", obdal_account.endpoint_))) {
        OB_LOG(WARN, "failed to set endpoint", K(ret), K(obdal_account.endpoint_));
      } else if (OB_FAIL(ObDalAccessor::obdal_operator_options_set(options, "region", obdal_account.region_))) {
        OB_LOG(WARN, "failed to set region", K(ret), K(obdal_account.region_));
      } else if (OB_FAIL(ObDalAccessor::obdal_operator_options_set(options, "access_key_id", obdal_account.access_id_))) {
        OB_LOG(WARN, "failed to set access id", K(ret), K(obdal_account.access_id_));
      } else if (OB_FAIL(ObDalAccessor::obdal_operator_options_set(options, "secret_access_key", obdal_account.access_key_))) {
        OB_LOG(WARN, "failed to set access key", K(ret));
      } else if (OB_FAIL(ObDalAccessor::obdal_operator_options_set(options, "disable_config_load", "true"))) {
        OB_LOG(WARN, "failed to set disable config load", K(ret));
      } else if (OB_FAIL(ObDalAccessor::obdal_operator_options_set(options, "disable_ec2_metadata", "true"))) {
        OB_LOG(WARN, "failed to set disable ec2 metadata", K(ret));
      } else if (obdal_account.addressing_model_ == ObStorageAddressingModel::OB_VIRTUAL_HOSTED_STYLE
                 && OB_FAIL(ObDalAccessor::obdal_operator_options_set(options, "enable_virtual_host_style", "true"))) {
        // The default enable_virtual_host_style for obdal s3 servicec is fasle
        OB_LOG(WARN, "faield to set enable virtual host style", K(ret));
      } else if (OB_FAIL(ObDalAccessor::obdal_operator_options_set(options, "timeout", "60"))) {
        OB_LOG(WARN, "failed to set timeout", K(ret));
      } else if (OB_FAIL(ObDalAccessor::obdal_operator_options_set(options, "tenant_id", tenant_id_str))) {
        OB_LOG(WARN, "failed to set tenant id", K(ret));
      }
    } else if (storage_type == ObStorageType::OB_STORAGE_OSS) {
      if (OB_FAIL(ObDalAccessor::obdal_operator_options_set(options, "bucket", bucket.ptr()))) {
        OB_LOG(WARN, "failed to set bucket", K(ret), K(bucket));
      } else if (OB_FAIL(ObDalAccessor::obdal_operator_options_set(options, "endpoint", obdal_account.endpoint_))) {
        OB_LOG(WARN, "failed to set endpoint", K(ret), K(obdal_account.endpoint_));
      } else if (OB_FAIL(ObDalAccessor::obdal_operator_options_set(options, "access_key_id", obdal_account.access_id_))) {
        OB_LOG(WARN, "failed to set access id", K(ret), K(obdal_account.access_id_));
      } else if (OB_FAIL(ObDalAccessor::obdal_operator_options_set(options, "access_key_secret", obdal_account.access_key_))) {
        OB_LOG(WARN, "failed to set access key", K(ret));
      } else if (OB_FAIL(ObDalAccessor::obdal_operator_options_set(options, "timeout", "60"))) {
        OB_LOG(WARN, "failed to set timeout", K(ret));
      } else if (OB_FAIL(ObDalAccessor::obdal_operator_options_set(options, "tenant_id", tenant_id_str))) {
        OB_LOG(WARN, "failed to set tenant id", K(ret));
      }
    } else if (storage_type == ObStorageType::OB_STORAGE_AZBLOB) {
      if (OB_FAIL(ObDalAccessor::obdal_operator_options_set(options, "container", bucket.ptr()))) {
        OB_LOG(WARN, "failed to set bucket", K(ret), K(bucket));
      } else if (OB_FAIL(ObDalAccessor::obdal_operator_options_set(options, "endpoint", obdal_account.endpoint_))) {
        OB_LOG(WARN, "failed to set endpoint", K(ret), K(obdal_account.endpoint_));
      } else if (OB_FAIL(ObDalAccessor::obdal_operator_options_set(options, "account_name", obdal_account.access_id_))) {
        OB_LOG(WARN, "failed to set access id", K(ret), K(obdal_account.access_id_));
      } else if (OB_FAIL(ObDalAccessor::obdal_operator_options_set(options, "account_key", obdal_account.access_key_))) {
        OB_LOG(WARN, "failed to set access key", K(ret));
      } else if (OB_FAIL(ObDalAccessor::obdal_operator_options_set(options, "timeout", "120"))) {
        OB_LOG(WARN, "failed to set timeout", K(ret));
      } else if (OB_FAIL(ObDalAccessor::obdal_operator_options_set(options, "tenant_id", tenant_id_str))) {
        OB_LOG(WARN, "failed to set tenant id", K(ret));
      }
    }
  }
  return ret;
}

int set_options_checksum_algorithm(
    const ObStorageType storage_type,
    const ObStorageChecksumType checksum_type,
    opendal_operator_options *options)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(options)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), KP(options), K(checksum_type));
  } else if (OB_UNLIKELY(!is_obdal_supported_checksum(storage_type, checksum_type))) {
    ret = OB_CHECKSUM_TYPE_NOT_SUPPORTED;
    OB_LOG(WARN, "that checksum algorithm is not supported for obdal", K(ret), K(checksum_type));
  } else {
    if (checksum_type == ObStorageChecksumType::OB_CRC32_ALGO) {
      if (OB_FAIL(ObDalAccessor::obdal_operator_options_set(options, "checksum_algorithm", "crc32"))) {
        OB_LOG(WARN, "failed to set checksum algorithm", K(ret), K(checksum_type));
      }
    } else if (checksum_type == ObStorageChecksumType::OB_MD5_ALGO) {
      if (OB_FAIL(ObDalAccessor::obdal_operator_options_set(options, "checksum_algorithm", "md5"))) {
        OB_LOG(WARN, "failed to set checksum algorithm", K(ret), K(checksum_type));
      }
    } else {
      // default no checksum algorithm
    }
  }
  return ret;
}

int ObStorageObDalBase::inner_open(const ObString &uri, ObObjectStorageInfo *storage_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    OB_LOG(WARN, "obdal base alreagy inited", K(ret));
  } else if (OB_ISNULL(storage_info) || OB_UNLIKELY(uri.empty() || !storage_info->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "failed to init obdal base, invalid arguments", K(ret), K(uri), KPC(storage_info));
  } else if (OB_FAIL(get_storage_type_from_path(uri, storage_type_))) {
    OB_LOG(WARN, "failed to get storage type from path", K(ret), K(uri), K(storage_type_));
  } else if (OB_FAIL(build_bucket_and_object_name(allocator_, uri, bucket_, object_))) {
    OB_LOG(WARN, "failed to parse uri", K(ret), K(uri));
  } else if (OB_FAIL(obdal_account_.assign(storage_info))) {
    OB_LOG(WARN, "failed to build obdal account", K(ret));
  } else {
    checksum_type_ = storage_info->get_checksum_type();
    is_write_with_if_match_ = storage_info->is_write_with_if_match();
#ifdef ERRSIM
    if (OB_NOT_NULL(storage_info) && (OB_SUCCESS != EventTable::EN_ENABLE_LOG_OBJECT_STORAGE_CHECKSUM_TYPE)) {
      OB_LOG(ERROR, "errsim backup io with checksum type", "checksum_type", storage_info->get_checksum_type_str());
    }
#endif
    if (OB_UNLIKELY(!is_obdal_supported_checksum(storage_type_, checksum_type_))) {
      ret = OB_CHECKSUM_TYPE_NOT_SUPPORTED;
      OB_LOG(WARN, "that checksum algorithm is not supported for obdal", K(ret), K_(checksum_type));
    } else if (OB_FAIL(ObDalAccessor::obdal_operator_options_new(options_))) {
      OB_LOG(WARN, "failed to new options", K(ret), K(bucket_), K(object_), K(obdal_account_));
    } else if (OB_FAIL(set_obdal_options_with_account(options_, storage_type_, obdal_account_, bucket_))) {
      OB_LOG(WARN, "fail set opendal operator options", K(ret),
          K(obdal_account_), K(bucket_), K(uri), KPC(storage_info));
    } else if (OB_FAIL(set_options_checksum_algorithm(storage_type_, checksum_type_, options_))) {
      OB_LOG(WARN, "fail set options with checksum algorithm", K(ret), K(checksum_type_),
          K(obdal_account_), K(bucket_), K(uri), KPC(storage_info));
    } else {
      if (storage_type_ == OB_STORAGE_OSS) {
        if (OB_FAIL(ObDalAccessor::obdal_operator_new("oss", options_, op_))) {
          OB_LOG(WARN, "fail get opendal operator with options", K(ret),
              K(obdal_account_), K(bucket_), K(uri), KPC(storage_info));
        }
      } else if (storage_type_ == OB_STORAGE_S3) {
        if (OB_FAIL(ObDalAccessor::obdal_operator_new("s3", options_, op_))) {
          OB_LOG(WARN, "fail get opendal operator with options", K(ret),
              K(obdal_account_), K(bucket_), K(uri), KPC(storage_info));
        }
      } else if (storage_type_ == OB_STORAGE_AZBLOB) {
        if (OB_FAIL(ObDalAccessor::obdal_operator_new("azblob", options_, op_))) {
          OB_LOG(WARN, "fail get opendal operator with options", K(ret),
              K(obdal_account_), K(bucket_), K(uri), KPC(storage_info));
        }
      }
      if (OB_SUCC(ret)) {
        is_inited_ = true;
        OB_LOG(DEBUG, "succeed to inner open obdal base", K(obdal_account_), K(bucket_), K(uri), KPC(storage_info));
      }
    }
  }
  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

// can only be used to get the metadata of normal objects
int ObStorageObDalBase::get_file_meta(ObDalObjectMeta &meta)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  meta.reset();
  opendal_metadata *query_meta = nullptr;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "obdal base not inited", K(ret));
  } else if (OB_UNLIKELY(object_.empty())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "object name is empty", K(ret), K(bucket_), K(object_));
  } else if (OB_FAIL(ObDalAccessor::obdal_operator_stat(op_, object_.ptr(), query_meta))) {
    if (ret == OB_OBJECT_NOT_EXIST) {
      ret = OB_SUCCESS;
    } else {
      OB_LOG(WARN, "fail exec operator stat", K(ret), K(bucket_), K(object_), K(obdal_account_));
    }
  } else if (OB_FAIL(ObDalAccessor::obdal_metadata_content_length(query_meta, meta.length_))) {
    OB_LOG(WARN, "fail get content length", K(ret), K(bucket_), K(object_));
  } else {
    meta.is_exist_ = true;

    if (OB_TMP_FAIL(ObDalAccessor::obdal_metadata_last_modified(query_meta, meta.mtime_s_))) {
      OB_LOG(WARN, "fail get last modified", K(ret), K(tmp_ret), K(bucket_), K(object_));
      // This field is currently only used by external tables.
      // To avoid impacting existing functionality,
      // even if the value is invalid, no error is reported. Instead, `meta.mtime_s_` is set to -1.
      meta.mtime_s_ = -1;
    }

    char *obdal_etag = nullptr;
    if (OB_TMP_FAIL(ObDalAccessor::obdal_metadata_etag(query_meta, obdal_etag))) {
      OB_LOG(WARN, "fail get etag", K(ret), K(tmp_ret), K(bucket_), K(object_));
    } else if (OB_TMP_FAIL(meta.digest_.set(obdal_etag))) {
      OB_LOG(WARN, "fail to set digest", K(ret), K(tmp_ret), K(bucket_), K(object_));
    }

    if (OB_NOT_NULL(obdal_etag)) {
      if (OB_TMP_FAIL(ObDalAccessor::obdal_c_char_free(obdal_etag))) {
        OB_LOG(WARN, "failed to free obdal etag", K(ret), K(tmp_ret), K(bucket_), K(object_));
      }
      obdal_etag = nullptr;
    }
  }

  if (OB_NOT_NULL(query_meta)) {
    if (OB_TMP_FAIL(ObDalAccessor::obdal_metadata_free(query_meta))) {
      OB_LOG(WARN, "fail free opendal metadata",
          K(ret), K(tmp_ret), KP(query_meta), K(bucket_), K(object_));
    }
    ret = COVER_SUCC(tmp_ret);
    query_meta = nullptr;
  }
  return ret;
}

//========================= ObStorageObDalWriter =========================
ObStorageObDalWriter::ObStorageObDalWriter()
  : ObStorageObDalBase(),
    is_opened_(false),
    file_length_(-1)
{
}

ObStorageObDalWriter::~ObStorageObDalWriter()
{
  if (is_opened_) {
    close();
  }
}

void ObStorageObDalWriter::reset()
{
  is_opened_ = false;
  int ret = OB_SUCCESS;
  file_length_ = -1;
  ObStorageObDalBase::reset();
}

int ObStorageObDalWriter::open(const common::ObString &uri, common::ObObjectStorageInfo *storage_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_opened_)) {
    ret = OB_OPEN_TWICE;
    OB_LOG(WARN, "obdal writer already open, cannot open again", K(ret), K(uri));
  } else if (OB_FAIL(ObStorageObDalBase::open(uri, storage_info))) {
    OB_LOG(WARN, "failed to open in obdal base", K(ret), K(uri), KPC(storage_info));
  } else {
    is_opened_ = true;
    file_length_ = 0;
  }
  return ret;
}

int ObStorageObDalWriter::write(const char *buf, const int64_t size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_OPEN;
    OB_LOG(WARN, "obdal writer not init", K(ret), K(is_opened_));
  } else if (OB_ISNULL(buf) || OB_UNLIKELY(size < 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), KP(buf), K(size));
  } else if (is_write_with_if_match_ && OB_FAIL(ObDalAccessor::obdal_operator_write_with_if_not_exists(op_, object_.ptr(), buf, size))) {
    if (OB_UNLIKELY(ret != OB_OBJECT_STORAGE_CONDITION_NOT_MATCH)) {
      OB_LOG(WARN, "fail write with if not exists in obdal", K(ret), K(object_), K(bucket_), K(obdal_account_));
    } else {
      ret = OB_SUCCESS;
      opendal_reader *reader = nullptr;
      char *read_buf = nullptr;
      // Notice: in order to avoid reading the prefixes of existing files, size
      // should be incremented by 1. if not, suppose that there already exists a
      // file with content 'abcde' of length 5, and then overwriting content 'abc'
      // of length 3, the overwriting will be mistaken for consistent content.
      const int64_t read_buf_size = size + 1;
      int64_t read_size = 0;
      ObArenaAllocator allocator(OB_DAL_SDK);
      if (OB_FAIL(ObDalAccessor::obdal_operator_reader(op_, object_.ptr(), reader))) {
        OB_LOG(WARN, "failed to get opendal reader", K(ret), KP(op_), K(object_), K(obdal_account_));
      } else if (OB_ISNULL(read_buf = static_cast<char *>(allocator.alloc(read_buf_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        OB_LOG(WARN, "failed to alloc read buf", K(ret), K(read_buf_size));
      } else if (OB_FAIL(ObDalAccessor::obdal_reader_read(reader, read_buf, read_buf_size, 0/*offset*/, read_size))) {
        OB_LOG(WARN, "failed to read", K(ret), KP(reader), K(read_buf_size));
      } else if (OB_UNLIKELY(read_size != size || 0 != MEMCMP(read_buf, buf, size))) {
        ret = OB_OBJECT_STORAGE_CONDITION_NOT_MATCH;
        OB_LOG(ERROR, "failed write_with_if_match", KR(ret), K(read_size), K(size));
      } else {
        // if 'if-match' is enabled, the lastmodify time of the object will no longer be accurate.
        OB_LOG(INFO, "an overlay write occurs and the data is consistent", K(ret), K(object_), K(bucket_), K(obdal_account_));
      }

      if (OB_NOT_NULL(reader)) {
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(ObDalAccessor::obdal_reader_free(reader))) {
          ret = COVER_SUCC(tmp_ret);
          OB_LOG(WARN, "failed to free opendal reader", K(tmp_ret), KP(reader));
        }
        reader = nullptr;
      }
    }
  } else if (!is_write_with_if_match_ && OB_FAIL(ObDalAccessor::obdal_operator_write(op_, object_.ptr(), buf, size))) {
    OB_LOG(WARN, "fail write in obdal", K(ret), K(object_), K(bucket_), K(obdal_account_));
  }

  if (OB_SUCC(ret)) {
    file_length_ = size;
  }
  return ret;
}

int ObStorageObDalWriter::pwrite(const char *buf, const int64_t size, const int64_t offset)
{
  int ret = OB_NOT_SUPPORTED;
  UNUSED(buf);
  UNUSED(size);
  UNUSED(offset);
  return ret;
}

int ObStorageObDalWriter::close()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_opened_)) {
    OB_LOG(WARN, "obdal writer not open", K(ret), K(is_opened_));
  }
  reset();
  return ret;
}

//========================= ObStorageObDalReader =========================
ObStorageObDalReader::ObStorageObDalReader()
  : ObStorageObDalBase(),
    is_opened_(false),
    has_meta_(false),
    file_length_(-1),
    opendal_reader_(nullptr)
{}

ObStorageObDalReader::~ObStorageObDalReader()
{
  if (is_opened_) {
    close();
  }
}

void ObStorageObDalReader::reset()
{
  int ret = OB_SUCCESS;
  ObStorageObDalBase::reset();
  is_opened_ = false;
  has_meta_ = false;
  file_length_ = -1;
  if (opendal_reader_ != nullptr) {
    if (OB_FAIL(ObDalAccessor::obdal_reader_free(opendal_reader_))) {
      OB_LOG(WARN, "failed to free opendal reader", K(ret));
    }
    opendal_reader_ = nullptr;
  }
}


int ObStorageObDalReader::open(
    const ObString &uri,
    ObObjectStorageInfo *storage_info,
    const bool head_meta)
{
  int ret = OB_SUCCESS;
  ObDalObjectMeta meta;
  if (OB_UNLIKELY(is_opened_)) {
    ret = OB_NOT_OPEN;
    OB_LOG(WARN, "obdal reader already open, cannot open again", K(ret), K(uri), KPC(storage_info));
  } else if (OB_FAIL(ObStorageObDalBase::open(uri, storage_info))) {
    OB_LOG(WARN, "failed to open in obdal base", K(ret), K(uri), KPC(storage_info));
  } else if (OB_FAIL(ObDalAccessor::obdal_operator_reader(op_, object_.ptr(), opendal_reader_))) {
    OB_LOG(WARN, "failed to get opendal reader", K(ret), KP(op_), K(object_), K(uri), KPC(storage_info));
  } else {
    if (head_meta) {
      if (OB_FAIL(get_file_meta(meta))) {
        OB_LOG(WARN, "failed to get obdal object meta", K(ret), K(uri));
      } else if (!meta.is_exist_) {
        ret = OB_OBJECT_NOT_EXIST;
        OB_LOG(WARN, "object is not exist", K(ret), K(uri), K_(bucket), K_(object));
      } else {
        file_length_ = meta.length_;
        has_meta_ = true;
      }
    }
  }

  if (OB_FAIL(ret)) {
    reset();
  } else {
    is_opened_ = true;
  }
  return ret;
}

int ObStorageObDalReader::pread(
    char *buf,
    const int64_t buf_size,
    const int64_t offset,
    int64_t &read_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_OBJECT_STORAGE_IO_ERROR;
    OB_LOG(WARN, "obdal reader not opened", K(ret));
  } else if (OB_ISNULL(buf) || OB_UNLIKELY(buf_size <= 0 || offset < 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid arguments", K(ret), KP(buf), K(buf_size), K(offset));
  } else {
    int64_t get_data_size = buf_size;
    if (has_meta_) {
      if (file_length_ < offset) {
        ret = OB_FILE_LENGTH_INVALID;
        OB_LOG(WARN, "offset is larger than file length",
            K(ret), K(offset), K_(file_length), K_(bucket), K_(object));
      } else {
        get_data_size = MIN(buf_size, file_length_ - offset);
      }
    }

    if (OB_FAIL(ret)) {
    } else if (get_data_size == 0) {
      read_size = 0;
    } else if (OB_FAIL(ObDalAccessor::obdal_reader_read(opendal_reader_, buf, get_data_size, offset, read_size))) {
      OB_LOG(WARN, "failed to read object from obdal", K(ret), K(bucket_), K(object_),
          K(offset), K(buf_size), K(has_meta_), K(file_length_));
    }
  }
  return ret;
}

int ObStorageObDalReader::close()
{
  int ret = OB_SUCCESS;
  reset();
  return ret;
}

//========================= ObStorageObDalUtil =========================
ObStorageObDalUtil::ObStorageObDalUtil()
  : is_opened_(false),
    storage_info_(nullptr)
{}

ObStorageObDalUtil::~ObStorageObDalUtil()
{
  close();
}

int ObStorageObDalUtil::open(common::ObObjectStorageInfo *storage_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_opened_)) {
    ret = OB_OBJECT_STORAGE_IO_ERROR;
    OB_LOG(WARN, "obdal util already open, cannot open again", K(ret));
  } else if (OB_ISNULL(storage_info) || OB_UNLIKELY(!storage_info->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "obdal account is null", K(ret), KPC(storage_info));
  } else {
    is_opened_ = true;
    storage_info_ = storage_info;
  }
  return ret;
}

void ObStorageObDalUtil::close()
{
  is_opened_ = false;
  storage_info_ = nullptr;
}

int ObStorageObDalUtil::is_exist(const common::ObString &uri, bool &exist)
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

int ObStorageObDalUtil::get_file_length(const common::ObString &uri, int64_t &file_length)
{
  int ret = OB_SUCCESS;
  file_length = 0;
  ObStorageObjectMetaBase obj_meta;
  if (OB_FAIL(head_object_meta(uri, obj_meta))) {
    OB_LOG(WARN, "fail to head object meta", K(ret), K(uri));
  } else if (!obj_meta.is_exist_) {
    ret = OB_OBJECT_NOT_EXIST;
    OB_LOG(WARN, "object is not exist", K(ret), K(uri));
  } else {
    file_length = obj_meta.length_;
  }
  return ret;
}

int ObStorageObDalUtil::head_object_meta(const common::ObString &uri, ObStorageObjectMetaBase &obj_meta)
{
  int ret = OB_SUCCESS;
  ObDalObjectMeta meta;
  ObStorageObDalBase obdal_base;
  obj_meta.reset();
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_OPEN_TWICE;
    OB_LOG(WARN, "obdal util not opened", K(ret));
  } else if (OB_UNLIKELY(uri.empty())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "uri is empty", K(ret), K(uri));
  } else if (OB_FAIL(obdal_base.open(uri, storage_info_))) {
    OB_LOG(WARN, "failed to open s3 base", K(ret), K(uri));
  } else if (OB_FAIL(obdal_base.get_file_meta(meta))) {
    OB_LOG(WARN, "failed to get s3 file meta", K(ret), K(uri));
  } else {
    obj_meta.is_exist_ = meta.is_exist_;
    if (obj_meta.is_exist_) {
      obj_meta.length_ = meta.length_;
      obj_meta.mtime_s_ = meta.mtime_s_;
      if (!meta.digest_.empty() && OB_FAIL(obj_meta.digest_.assign(meta.digest_))) {
        OB_LOG(WARN, "fail to set digest", K(ret), K(uri), K(meta));
      }
    }
  }
  return ret;
}

int ObStorageObDalUtil::write_single_file(
    const common::ObString &uri,
    const char *buf,
    const int64_t size)
{
  int ret = OB_SUCCESS;
  ObStorageObDalWriter obdal_writer;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "obdal util not opened", K(ret));
  } else if (OB_UNLIKELY(uri.empty() || size < 0) || OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid arguments", K(ret), K(uri), KP(buf), K(size));
  } else if (OB_FAIL(obdal_writer.open(uri, storage_info_))) {
    OB_LOG(WARN, "failed to open obdal writer", K(ret), K(uri), KPC(storage_info_));
  } else if (OB_FAIL(obdal_writer.write(buf, size))) {
    // if write failed, obdal_writer's destructor is responsible for cleaning up the resource
    OB_LOG(WARN, "failed to write into obdal", K(ret), K(uri), KP(buf), K(size));
  } else if (OB_FAIL(obdal_writer.close())) {
    OB_LOG(WARN, "failed to close obdal writer", K(ret), K(uri), KP(buf), K(size));
  }
  return ret;
}

int ObStorageObDalUtil::mkdir(const common::ObString &uri)
{
  int ret = OB_SUCCESS;
  OB_LOG(DEBUG, "no need to create dir in obdal", K(uri));
  UNUSED(uri);
  return ret;
}

int put_tagging(const opendal_operator *op, const char *path)
{
  int ret = OB_SUCCESS;
  opendal_object_tagging *tagging = nullptr;
  if (OB_ISNULL(op) || OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid arguments", K(ret), KP(op), KP(path));
  } else if (OB_FAIL(ObDalAccessor::obdal_object_tagging_new(tagging))) {
    OB_LOG(WARN, "failed to new obdal object tagging", K(ret));
  } else if (OB_FAIL(ObDalAccessor::obdal_object_tagging_set(tagging, "delete_mode", "tagging"))) {
    OB_LOG(WARN, "failed to set obdal object tagging", K(ret), K(path));
  } else if (OB_FAIL(ObDalAccessor::obdal_operator_put_object_tagging(op, path, tagging))) {
    OB_LOG(WARN, "failed to put object tagging", K(ret), K(path));
  }

  if (OB_NOT_NULL(tagging)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(ObDalAccessor::obdal_object_tagging_free(tagging))) {
      ret = COVER_SUCC(tmp_ret);
      OB_LOG(WARN, "failed to free obdal object tagging", K(tmp_ret), K(path));
    }
    tagging = nullptr;
  }
  return ret;
}

int ObStorageObDalUtil::del_file(const common::ObString &uri)
{
  int ret = OB_SUCCESS;
  ObStorageObDalBase obdal_base;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "obdal util not opened", K(ret));
  } else if (OB_UNLIKELY(uri.empty())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid arguments", K(ret), K(uri));
  } else if (OB_FAIL(obdal_base.open(uri, storage_info_))) {
    OB_LOG(WARN, "failed to open obdal base", K(ret), K(uri));
  } else {
    const int64_t delete_mode = obdal_base.obdal_account_.delete_mode_;
    if (ObStorageDeleteMode::STORAGE_DELETE_MODE == delete_mode) {
      if (OB_FAIL(ObDalAccessor::obdal_operator_delete(obdal_base.op_, obdal_base.object_.ptr()))) {
        if (OB_OBJECT_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          OB_LOG(WARN, "failed to delete obdal object", K(ret), K(uri));
        }
      }
    } else if (ObStorageDeleteMode::STORAGE_TAGGING_MODE == delete_mode) {
      if (OB_FAIL(put_tagging(obdal_base.op_, obdal_base.object_.ptr()))) {
        OB_LOG(WARN, "failed to tag obdal object", K(ret), K(uri));
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "obdal delete mode invalid", K(ret), K(uri), K(delete_mode));
    }
  }
  return ret;
}

int ObStorageObDalUtil::batch_del_files(
    const ObString &uri,
    hash::ObHashMap<ObString, int64_t> &files_to_delete,
    ObIArray<int64_t> &failed_files_idx)
{
  int ret = OB_SUCCESS;
  ObStorageObDalBase obdal_base;
  opendal_deleter *deleter = nullptr;
  int64_t deleted_cnt = 0;
  const int64_t n_files_to_delete = files_to_delete.size();

  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "obdal util not opened", K(ret), K(uri));
  } else if (OB_FAIL(check_files_map_validity(files_to_delete))) {
    OB_LOG(WARN, "files_to_delete is invalid", K(ret), K(uri));
  } else if (OB_FAIL(obdal_base.open(uri, storage_info_))) {
    OB_LOG(WARN, "failed to open obdal base", K(ret), K(uri), KPC_(storage_info));
  } else if (OB_UNLIKELY(ObStorageDeleteMode::STORAGE_TAGGING_MODE == obdal_base.obdal_account_.delete_mode_)) {
    ret = OB_NOT_SUPPORTED;
    OB_LOG(WARN, "batch tagging is not supported", K(ret), K(uri), K(obdal_base.obdal_account_));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObDalAccessor::obdal_operator_deleter(obdal_base.op_, deleter))) {
    OB_LOG(WARN, "failed to exec obdal operator deleter", K(ret));
  } else {
    hash::ObHashMap<ObString, int64_t>::const_iterator iter = files_to_delete.begin();
    while (OB_SUCC(ret) && iter != files_to_delete.end()) {
      if (OB_FAIL(ObDalAccessor::obdal_deleter_delete(deleter, iter->first.ptr()))) {
        OB_LOG(WARN, "failed to exec obdal deleter delete", K(ret), K(iter->first));
      } else {
        iter++;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObDalAccessor::obdal_deleter_flush(deleter, deleted_cnt))) {
      OB_LOG(WARN, "failed to exec obdal deleter flush", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(deleted_cnt != n_files_to_delete)) {
    ObArray<ObString> deleted_array;
    hash::ObHashMap<ObString, int64_t>::const_iterator iter = files_to_delete.begin();
    bool deleted = false;
    while (OB_SUCC(ret) && iter != files_to_delete.end()) {
      deleted = false;
      if (OB_FAIL(ObDalAccessor::obdal_deleter_deleted(deleter, iter->first.ptr(), deleted))) {
        OB_LOG(WARN, "failed to exec obdal deleter deleted", K(ret));
      } else if (deleted) {
        if (OB_FAIL(deleted_array.push_back(iter->first.ptr()))) {
          OB_LOG(WARN, "failed to push back deleted object", K(ret), K(iter->first));
        }
      } else {
        if (OB_FAIL(failed_files_idx.push_back(iter->second))) {
          OB_LOG(WARN, "failed to push back failed object", K(ret), K(iter->first));
        }
      }
      iter++;
    }

    for (size_t i = 0; OB_SUCC(ret) && i < deleted_array.size(); i++) {
      if (OB_FAIL(files_to_delete.erase_refactored(deleted_array[i]))) {
        OB_LOG(WARN, "faild to erase succeed deleted object", K(ret), K(deleted_array[i]));
      }
    }
  } else {
    files_to_delete.clear();
  }

  if (OB_NOT_NULL(deleter)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(ObDalAccessor::obdal_deleter_free(deleter))) {
      ret = COVER_SUCC(tmp_ret);
      OB_LOG(WARN, "fail free opendal deleter", K(ret), K(uri));
    }
  }
  return ret;
}

int get_entry_length(const opendal_entry *entry, int64_t &length)
{
  int ret = OB_SUCCESS;
  opendal_metadata *meta = nullptr;
  length = -1;
  if (OB_ISNULL(entry)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid arguments", K(ret), KP(entry));
  } else if (OB_FAIL(ObDalAccessor::obdal_entry_metadata(entry, meta))) {
    OB_LOG(WARN, "failed to get entry metadata", K(ret));
  } else if (OB_FAIL(ObDalAccessor::obdal_metadata_content_length(meta, length))) {
    OB_LOG(WARN, "failed to get entry content length", K(ret));
  }
  if (OB_NOT_NULL(meta)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(ObDalAccessor::obdal_metadata_free(meta))) {
      OB_LOG(WARN, "fail free opendal metadata", K(tmp_ret));
    }
    ret = COVER_SUCC(tmp_ret);
  }
  return ret;
}

int get_file_extra_info(ObIAllocator &allocator, const opendal_entry *entry, ObFileExtraInfo &file_extra_info)
{
  int ret = OB_SUCCESS;
  file_extra_info.reset();
  opendal_metadata *meta = nullptr;
  char *obdal_etag = nullptr;
  char *deep_copied_etag = nullptr;
  int64_t last_modified_time_s = 0;
  if (OB_ISNULL(entry)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid arguments", K(ret), KP(entry));
  } else if (OB_FAIL(ObDalAccessor::obdal_entry_metadata(entry, meta))) {
    OB_LOG(WARN, "failed to get entry metadata", K(ret), KP(entry));
  } else if (OB_FAIL(ObDalAccessor::obdal_metadata_etag(meta, obdal_etag))) {
    OB_LOG(WARN, "failed to get entry etag", K(ret), KP(entry), KP(meta));
  } else if (OB_FAIL(ObDalAccessor::obdal_metadata_last_modified(meta, last_modified_time_s))) {
    OB_LOG(WARN, "failed to get entry last modified time", K(ret), KP(entry), KP(meta));
  } else if (OB_FAIL(ob_dup_cstring(allocator, obdal_etag, deep_copied_etag))) {
    OB_LOG(WARN, "failed to deep copy obdal etag data", K(ret), KP(entry), KP(meta));
  } else {
    file_extra_info.last_modified_time_ms_ = last_modified_time_s * 1000LL;
    file_extra_info.etag_ = deep_copied_etag;
    file_extra_info.etag_len_ = get_safe_str_len(deep_copied_etag);
  }

  int tmp_ret = OB_SUCCESS;
  if (OB_NOT_NULL(obdal_etag)) {
    if (OB_TMP_FAIL(ObDalAccessor::obdal_c_char_free(obdal_etag))) {
      OB_LOG(WARN, "failed to free obdal etag", K(ret), K(tmp_ret));
    }
    obdal_etag = nullptr;
    ret = COVER_SUCC(tmp_ret);
  }
  if (OB_NOT_NULL(meta)) {
    tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(ObDalAccessor::obdal_metadata_free(meta))) {
      OB_LOG(WARN, "fail free opendal metadata", K(ret), K(tmp_ret));
    }
    meta = nullptr;
    ret = COVER_SUCC(tmp_ret);
  }

  return ret;
}

int ObStorageObDalUtil::list_files(const common::ObString &uri, common::ObBaseDirEntryOperator &op)
{
  int ret = OB_SUCCESS;
  ObStorageObDalBase obdal_base;
  ObExternalIOCounterGuard io_guard;
  const char *full_dir_path = nullptr;
  opendal_lister *lister = nullptr;

  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "obdal util not opened", K(ret));
  } else if (OB_UNLIKELY(uri.empty())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid arguments", K(ret), K(uri));
  } else if (OB_FAIL(obdal_base.inner_open(uri, storage_info_))) {
    OB_LOG(WARN, "fail to open obdal base", K(ret), K(uri));
  } else if (FALSE_IT(full_dir_path = obdal_base.object_.ptr())) {
  } else if (OB_UNLIKELY(!is_null_or_end_with_slash(full_dir_path))) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "uri is not terminated with '/'", K(ret), K(uri), K(full_dir_path));
  } else if (OB_FAIL(ObDalAccessor::obdal_operator_list(obdal_base.op_, obdal_base.object_.ptr(),
          OB_STORAGE_LIST_MAX_NUM, true/*recursive*/, "", lister))) {
    OB_LOG(WARN, "failed to list obdal objects", K(ret), K(uri), K(OB_STORAGE_LIST_MAX_NUM));
  } else {
    ObArenaAllocator etag_allocator(OB_STORAGE_OBDAL_ALLOCATOR);
    opendal_entry *entry = nullptr;
    const int64_t full_dir_path_len = get_safe_str_len(full_dir_path);
    while (OB_SUCC(ret)) {
      etag_allocator.clear();
      if (OB_FAIL(ObDalAccessor::obdal_lister_next(lister, entry))) {
        OB_LOG(WARN, "failed to exec obdal lister next", K(ret));
      } else if (entry == nullptr) {
        // When `entry` is `nullptr`, break, indicating that all entries have been listed.
        // If `entry` is not `nullptr`, the `while` will continue.
        // Since `entry` needs to be freed promptly and there is no complex logic after the while loop,
        // a break is used to exit the loop here.
        break;
      } else {
        int64_t obj_size = -1;
        char *cur_obj_path = nullptr;
        int64_t cur_obj_path_len = 0;
        ObFileExtraInfo file_extra_info;
        if (op.need_get_file_meta() && OB_FAIL(get_entry_length(entry, obj_size))) {
          OB_LOG(WARN, "faield to exec obdal entry content length", K(ret));
        } else if (OB_FAIL(ObDalAccessor::obdal_entry_path(entry, cur_obj_path))) {
          OB_LOG(WARN, "failed exec obdal entry path", K(ret));
        } else {
          cur_obj_path_len = get_safe_str_len(cur_obj_path);
        }

        if (OB_FAIL(ret)) {
        } else if (OB_ISNULL(cur_obj_path) || OB_UNLIKELY(false == ObString(cur_obj_path).prefix_match(full_dir_path))) {
          ret = OB_OBJECT_STORAGE_IO_ERROR;
          OB_LOG(WARN, "returned object prefix not match",
              K(ret), K(cur_obj_path), K(full_dir_path), K(uri));
        } else if (OB_UNLIKELY(cur_obj_path_len == full_dir_path_len)) {
          OB_LOG(INFO, "exist object path length is same with dir path length",
              K(cur_obj_path), K(full_dir_path), K(full_dir_path_len));
        } else if (op.need_get_file_meta()
            && OB_FAIL(get_file_extra_info(etag_allocator, entry, file_extra_info))) {
          OB_LOG(WARN, "fail to get file extra info", K(ret),
              K(cur_obj_path), K(full_dir_path), K(full_dir_path_len));
        } else if (OB_FAIL(handle_listed_object(op, cur_obj_path + full_dir_path_len,
                                                cur_obj_path_len - full_dir_path_len,
                                                obj_size,
                                                &file_extra_info))) {
          OB_LOG(WARN, "fail to add listed obdal obejct meta into ctx",
              K(ret), K(cur_obj_path), K(cur_obj_path_len), K(file_extra_info),
              K(full_dir_path), K(full_dir_path_len), K(obj_size));
        }

        if (OB_NOT_NULL(entry)) {
          int tmp_ret = OB_SUCCESS;
          if (OB_TMP_FAIL(ObDalAccessor::obdal_entry_free(entry))) {
            OB_LOG(WARN, "failed to exec obdal entry free", K(ret), KP(entry));
          }
          ret = COVER_SUCC(tmp_ret);
        }
        if (OB_NOT_NULL(cur_obj_path)) {
          int tmp_ret = OB_SUCCESS;
          if (OB_TMP_FAIL(ObDalAccessor::obdal_c_char_free(cur_obj_path))) {
            OB_LOG(WARN, "failed to exec obdal c char free", K(ret), KP(cur_obj_path));
          }
          ret = COVER_SUCC(tmp_ret);
        }
      }
    }
  }

  if (OB_NOT_NULL(lister)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(ObDalAccessor::obdal_lister_free(lister))) {
      OB_LOG(WARN, "failed to exec obdal lister free", K(ret), KP(lister));
    }
    ret = COVER_SUCC(tmp_ret);
  }
  return ret;
}

int ObStorageObDalUtil::list_files(const common::ObString &uri, ObStorageListCtxBase &ctx_base)
{
  int ret = OB_SUCCESS;
  ObStorageListObjectsCtx &list_ctx = static_cast<ObStorageListObjectsCtx &>(ctx_base);
  ObStorageObDalBase obdal_base;
  const char *full_dir_path = nullptr;
  const int64_t max_list_num = MIN(OB_STORAGE_LIST_MAX_NUM, list_ctx.max_list_num_);

  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_OBJECT_STORAGE_IO_ERROR;
    OB_LOG(WARN, "obdal util not opened", K(ret), K(uri));
  } else if (OB_UNLIKELY(uri.empty() || !list_ctx.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid arguments", K(ret), K(uri), K(list_ctx));
  } else if (OB_FAIL(obdal_base.inner_open(uri, storage_info_))) {
    OB_LOG(WARN, "failed open obdal base", K(ret), K(uri));
  } else if (FALSE_IT(full_dir_path = obdal_base.object_.ptr())) {
  } else if (OB_UNLIKELY(!is_null_or_end_with_slash(full_dir_path))) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "uri is not terminated with '/'", K(ret), K(uri), K(full_dir_path));
  } else if (list_ctx.opendal_lister_ == nullptr) {
    if (OB_FAIL(ObDalAccessor::obdal_operator_list(obdal_base.op_, obdal_base.object_.ptr(), max_list_num,
                true/*recursive*/, list_ctx.next_token_, list_ctx.opendal_lister_))) {
      OB_LOG(WARN, "failed to list obdal objects", K(ret), K(uri), K(list_ctx.max_list_num_));
    }
  }

  // When called externally, the function will determine whether it needs to continue calling
  // based on the has_next flag in the context. However, in opendal, it only knows that the
  // listing has ended when lister_next returns a nullptr. Therefore, we initially set has_next to true.
  // If during the loop we do not iterate through the expected number of entries and receive a nullptr, we set it to false.
  if (OB_FAIL(ret)) {
  } else if (OB_LIKELY(max_list_num > 0)) {
    list_ctx.has_next_ = true;
  }

  if (OB_FAIL(ret)) {
  } else {
    opendal_entry *entry = nullptr;
    const int64_t full_dir_path_len = get_safe_str_len(full_dir_path);

    for (int64_t i = 0; OB_SUCC(ret) && (i < max_list_num); i++) {
      int64_t obj_size = -1;
      char *cur_obj_path = nullptr;
      int64_t cur_obj_path_len = 0;
      if (OB_FAIL(ObDalAccessor::obdal_lister_next(list_ctx.opendal_lister_, entry))) {
        OB_LOG(WARN, "failed to exec obdal lister next", K(ret));
      } else if (entry == nullptr) {
        list_ctx.has_next_ = false;
        break;
      } else if (list_ctx.need_meta_ && OB_FAIL(get_entry_length(entry, obj_size))) {
        OB_LOG(WARN, "faield to exec obdal entry content length", K(ret));
      } else if (OB_FAIL(ObDalAccessor::obdal_entry_path(entry, cur_obj_path))) {
        OB_LOG(WARN, "failed exec obdal entry path", K(ret));
      } else {
        cur_obj_path_len = get_safe_str_len(cur_obj_path);
      }

      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(cur_obj_path) || OB_UNLIKELY(false == ObString(cur_obj_path).prefix_match(full_dir_path))) {
        ret = OB_OBJECT_STORAGE_IO_ERROR;
        OB_LOG(WARN, "returned object prefix not match",
            K(ret), K(cur_obj_path), K(full_dir_path), K(uri));
      } else if (OB_UNLIKELY(cur_obj_path_len == full_dir_path_len)) {
        OB_LOG(INFO, "exist object path length is same with dir path length",
            K(cur_obj_path), K(full_dir_path), K(full_dir_path_len));
      } else if (OB_FAIL(list_ctx.handle_object(cur_obj_path, cur_obj_path_len, obj_size))) {
        OB_LOG(WARN, "fail to add listed obdal obejct meta into ctx",
            K(ret), K(cur_obj_path), K(cur_obj_path_len), K(obj_size));
      }

      if (OB_NOT_NULL(entry)) {
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(ObDalAccessor::obdal_entry_free(entry))) {
          OB_LOG(WARN, "failed to exec obdal entry free", K(ret), KP(entry));
        }
        ret = COVER_SUCC(tmp_ret);
      }
      if (OB_NOT_NULL(cur_obj_path)) {
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(ObDalAccessor::obdal_c_char_free(cur_obj_path))) {
          OB_LOG(WARN, "failed to exec obdal c char free", K(ret), KP(cur_obj_path));
        }
        ret = COVER_SUCC(tmp_ret);
      }
    } // end for
  }

  return ret;
}

int ObStorageObDalUtil::del_dir(const common::ObString &uri)
{
  int ret = OB_SUCCESS;
  OB_LOG(DEBUG, "no need to del dir in obdal", K(uri));
  UNUSED(uri);
  return ret;
}

int ObStorageObDalUtil::list_directories(const common::ObString &uri, common::ObBaseDirEntryOperator &op)
{
  int ret = OB_SUCCESS;
  ObStorageObDalBase obdal_base;
  ObExternalIOCounterGuard io_guard;
  const char *full_dir_path = NULL;
  opendal_lister *lister = nullptr;

  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "obdal util not opened", K(ret));
  } else if (OB_UNLIKELY(uri.empty())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid arguments", K(ret), K(uri));
  } else if (OB_FAIL(obdal_base.inner_open(uri, storage_info_))) {
    OB_LOG(WARN, "fail to open obdal base", K(ret), K(uri));
  } else if (FALSE_IT(full_dir_path = obdal_base.object_.ptr())) {
  } else if (OB_UNLIKELY(!is_null_or_end_with_slash(full_dir_path))) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "uri is not terminated with '/'", K(ret), K(uri), K(full_dir_path));
  } else if (OB_FAIL(ObDalAccessor::obdal_operator_list(obdal_base.op_, obdal_base.object_.ptr(),
          OB_STORAGE_LIST_MAX_NUM, false/*recursive*/, "", lister))) {
    OB_LOG(WARN, "failed to list obdal objects", K(ret), K(uri), K(OB_STORAGE_LIST_MAX_NUM));
  } else {
    const int64_t full_dir_path_len = get_safe_str_len(full_dir_path);
    opendal_entry *entry = nullptr;

    while (OB_SUCC(ret)) {
      if (OB_FAIL(ObDalAccessor::obdal_lister_next(lister, entry))) {
        OB_LOG(WARN, "failed to exec obdal lister next", K(ret));
      } else if (entry == nullptr) {
        break;
      } else {
        char *listed_dir_path = nullptr;
        int64_t listed_dir_path_len = 0;

        if (OB_FAIL(ObDalAccessor::obdal_entry_path(entry, listed_dir_path))) {
          OB_LOG(WARN, "faield to exec obdal entry name", K(ret));
        } else if (FALSE_IT(listed_dir_path_len = get_safe_str_len(listed_dir_path))) {
        } else if (OB_UNLIKELY(false == ObString(listed_dir_path).prefix_match(full_dir_path))) {
          ret = OB_OBJECT_STORAGE_IO_ERROR;
          OB_LOG(WARN, "returned object prefix not match", K(ret), K(uri), K(listed_dir_path), K(full_dir_path));
        } else if (listed_dir_path_len > full_dir_path_len
                   && listed_dir_path[listed_dir_path_len - 1] == '/') {
          // The condition listed_dir_path_len > full_dir_path_len is used to prevent the scenario where listed_dir_path is equal to full_dir_path.
          // oss mkdir will create a empty file named ended with '/', althouth ob not do mkdir, the outer layer will.
          if (OB_FAIL(handle_listed_directory(op, listed_dir_path + full_dir_path_len, listed_dir_path_len - full_dir_path_len - 1))) {
            OB_LOG(WARN, "fail to handle obdal directory name", K(ret),
                K(full_dir_path), K(listed_dir_path), K(listed_dir_path_len));
          }
        }

        if (OB_NOT_NULL(listed_dir_path)) {
          int tmp_ret = OB_SUCCESS;
          if (OB_TMP_FAIL(ObDalAccessor::obdal_c_char_free(listed_dir_path))) {
            OB_LOG(WARN, "failed to exec obdal c char free", K(ret), KP(listed_dir_path));
          }
          ret = COVER_SUCC(tmp_ret);
          listed_dir_path = nullptr;
        }
        if (OB_NOT_NULL(entry)) {
          int tmp_ret = OB_SUCCESS;
          if (OB_TMP_FAIL(ObDalAccessor::obdal_entry_free(entry))) {
            OB_LOG(WARN, "failed to exec obdal entry free", K(ret), KP(entry));
          }
          ret = COVER_SUCC(tmp_ret);
        }
      }
    }
  }

  if (lister != nullptr) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(ObDalAccessor::obdal_lister_free(lister))) {
      OB_LOG(WARN, "failed to exec obdal lister free", K(ret), KP(lister));
    }
    ret = COVER_SUCC(tmp_ret);
  }
  return ret;
}

int is_tagging_helper(
  const opendal_operator *op,
  const char *path,
  bool &is_tagging)
{
  int ret = OB_SUCCESS;
  opendal_object_tagging *tagging = nullptr;
  is_tagging = false;
  if (OB_ISNULL(op) || OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid arguments", K(ret), KP(op), KP(path));
  } else if (OB_FAIL(ObDalAccessor::obdal_operator_get_object_tagging(op, path, tagging))) {
    OB_LOG(WARN, "failed to get object tagging", K(ret), K(path));
  } else {
    opendal_bytes value;
    value.data = nullptr;
    value.len = 0;
    value.capacity = 0;
    if (OB_FAIL(ObDalAccessor::obdal_object_tagging_get(tagging, "delete_mode", value))) {
      OB_LOG(WARN, "failed to get object tagging value", K(ret), K(path));
    } else if (OB_ISNULL(value.data)) {
      ret = OB_OBJECT_STORAGE_IO_ERROR;
      OB_LOG(WARN, "failed to get object tagging value without errmsg", K(ret), K(path));
    } else {
      if (STRNCMP(reinterpret_cast<char *>(value.data), "tagging", value.len) == 0) {
        is_tagging = true;
      }
      if (OB_FAIL(ObDalAccessor::obdal_bytes_free(&value))) {
        OB_LOG(WARN, "failed to free obdal bytes", K(ret), K(path));
      }
    }
  }

  if (OB_NOT_NULL(tagging)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(ObDalAccessor::obdal_object_tagging_free(tagging))) {
      OB_LOG(WARN, "failed to free obdal object tagging", K(tmp_ret), K(path));
    }
    ret = COVER_SUCC(tmp_ret);
  }
  return ret;
}


int ObStorageObDalUtil::is_tagging(const common::ObString &uri, bool &is_tagging)
{
  int ret = OB_SUCCESS;
  ObStorageObDalBase obdal_base;

  is_tagging = false;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_OBJECT_STORAGE_IO_ERROR;
    OB_LOG(WARN, "obdal util not opened", K(ret));
  } else if (OB_UNLIKELY(uri.empty())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid arguments", K(ret), K(uri));
  } else if (OB_FAIL(obdal_base.open(uri, storage_info_))) {
    OB_LOG(WARN, "failed to open obdal base", K(ret), K(uri));
  } else if (OB_FAIL(is_tagging_helper(obdal_base.op_, obdal_base.object_.ptr(), is_tagging))) {
    OB_LOG(WARN, "failed to get obdal object tagging", K(ret), K(uri), K(obdal_base.obdal_account_));
  }
  return ret;
}
int ObStorageObDalUtil::del_unmerged_parts(const ObString &uri)
{
  int ret = OB_NOT_SUPPORTED;
  OB_LOG(WARN, "del unmerged parts not supported in obdal", K(ret), K(uri));
  return ret;
}

//========================= ObStorageObDalAppendWriter =========================
ObStorageObDalAppendWriter::ObStorageObDalAppendWriter()
  : ObStorageObDalBase(),
    is_opened_(false),
    storage_info_(nullptr),
    file_length_(-1),
    writer_(nullptr)
{
}

ObStorageObDalAppendWriter::~ObStorageObDalAppendWriter()
{
  reset();
}

void ObStorageObDalAppendWriter::reset()
{
  int ret = OB_SUCCESS;
  is_opened_ = false;
  storage_info_ = nullptr;
  file_length_ = -1;
  if (OB_NOT_NULL(writer_)) {
    if (OB_FAIL(ObDalAccessor::obdal_writer_free(writer_))) {
      OB_LOG(WARN, "failed to free obdal writer", K(ret));
    }
  }
  ObStorageObDalBase::reset();
}

int ObStorageObDalAppendWriter::open(const ObString &uri, ObObjectStorageInfo *storage_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_opened_)) {
    ret = OB_OPEN_TWICE;
    OB_LOG(WARN, "obdal append writer already open, cannot open again", K(ret), K(uri));
  } else if (OB_FAIL(ObStorageObDalBase::open(uri, storage_info))) {
    OB_LOG(WARN, "failed to open in obdal base", K(ret), K(uri), KPC(storage_info));
  } else {
    if (storage_type_ == OB_STORAGE_OSS || storage_type_ == OB_STORAGE_AZBLOB) {
      if (OB_FAIL(ObDalAccessor::obdal_operator_append_writer(op_, object_.ptr(), writer_))) {
        OB_LOG(WARN, "failed to get obdal operator append writer", K(ret), K(uri));
      }
    }
  }
  if (OB_SUCC(ret)) {
    is_opened_ = true;
    file_length_ = 0;
    storage_info_ = storage_info;
  }
  return ret;
}

int ObStorageObDalAppendWriter::write(const char *buf, const int64_t size)
{
  int ret = OB_NOT_SUPPORTED;
  UNUSED(buf);
  UNUSED(size);
  return ret;
}

int ObStorageObDalAppendWriter::pwrite(const char *buf, const int64_t size, const int64_t offset)
{
  int ret = OB_SUCCESS;
  char fragment_name[OB_MAX_BACKUP_STORAGE_INFO_LENGTH] = { 0 };

  if(OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_OPEN;
    OB_LOG(WARN, "obdal append writer cannot write before it is not opened", K(ret));
  } else if(OB_ISNULL(buf) || OB_UNLIKELY(size <= 0 || offset < 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid arguments", K(ret), KP(buf), K(size), K(offset));
  } else {
    if (storage_type_ == OB_STORAGE_OSS || storage_type_ == OB_STORAGE_AZBLOB) {
      if (OB_FAIL(ObDalAccessor::obdal_writer_write_with_offset(writer_, offset, buf, size))) {
        OB_LOG(WARN, "failed to write with offset in obdal append writer", K(ret), K(offset), K(size));
      }
    } else {
      // write the format file when writing the first fragment because the appender may open multiple times
      if (offset == 0) {
        // obdal employs the same format-meta as S3
        if (OB_FAIL(construct_fragment_full_name(object_, OB_ADAPTIVELY_APPENDABLE_FORMAT_META,
                                                fragment_name, sizeof(fragment_name)))) {
          OB_LOG(WARN, "failed to construct obdal mock append object foramt name",
              K(ret), K_(bucket), K_(object));
        } else if (OB_FAIL(ObDalAccessor::obdal_operator_write(op_, fragment_name,
                                              OB_ADAPTIVELY_APPENDABLE_FORMAT_CONTENT_V1,
                                              strlen(OB_ADAPTIVELY_APPENDABLE_FORMAT_CONTENT_V1)))) {
          OB_LOG(WARN, "fail to write obdal mock append object format file", K(ret), K(fragment_name));
        }
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(construct_fragment_full_name(object_, offset, offset + size,
                                                      fragment_name,  sizeof(fragment_name)))) {
        // fragment_name: /xxx/xxx/appendable_obj_name/start-end,
        // the data range covered by this file is from start to end == [start, end), include start not include end
        // start == offset, end == offset + size
        // fragment length == size
        OB_LOG(WARN, "failed to set fragment name for obdal append writer",
            K(ret), K_(bucket), K_(object), K(buf), K(size), K(offset));
      } else if (OB_FAIL(ObDalAccessor::obdal_operator_write(op_, fragment_name, buf, size))) {
        OB_LOG(WARN, "fail to append a fragment into obdal",
            K(ret), K_(bucket), K_(object), K(fragment_name), K(size));
      }
    }
  }

  if (OB_SUCC(ret)) {
    file_length_ += size;
    OB_LOG(DEBUG, "succeed to append a fragment into obdal",
        K_(bucket), K_(object), K(fragment_name), K(size), K(offset));
  }
  return ret;
}

int ObStorageObDalAppendWriter::close()
{
  int ret = OB_SUCCESS;
  reset();
  return ret;
}

int64_t ObStorageObDalAppendWriter::get_length() const
{
  int length = file_length_;
  int ret = OB_NOT_SUPPORTED;
  if (storage_type_ == OB_STORAGE_S3) {
    length = -1;
    OB_LOG(WARN, "obdal appender do not support get length now", K(ret));
  }
  return length;
}

//========================== ObStorageObDalMultiPartWriter =========================
ObStorageObDalMultiPartWriter::ObStorageObDalMultiPartWriter()
  : ObStorageObDalBase(),
    is_opened_(false),
    opendal_writer_(nullptr),
    file_length_(-1)
{}

ObStorageObDalMultiPartWriter::~ObStorageObDalMultiPartWriter()
{
  reset();
}

void ObStorageObDalMultiPartWriter::reset()
{
  int ret = OB_SUCCESS;
  is_opened_ = false;
  file_length_ = -1;
  if (opendal_writer_ != nullptr) {
    if (OB_FAIL(ObDalAccessor::obdal_writer_free(opendal_writer_))) {
      OB_LOG(WARN, "failed to free opendal writer", K(ret), KP(opendal_writer_));
    }
    opendal_writer_ = nullptr;
  }
  ObStorageObDalBase::reset();
}

int ObStorageObDalMultiPartWriter::open(const ObString &uri, ObObjectStorageInfo *storage_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_opened_)) {
    ret = OB_OPEN_TWICE;
    OB_LOG(WARN, "obdal multipart writer already open, cannot open again", K(ret), K(uri));
  } else if (OB_FAIL(ObStorageObDalBase::open(uri, storage_info))) {
    OB_LOG(WARN, "failed to open in obdal base", K(ret), K(uri));
  } else if (OB_FAIL(ObDalAccessor::obdal_operator_writer(op_, object_.ptr(), opendal_writer_))) {
    OB_LOG(WARN, "failed get opendal writer", K(ret), K(object_),
        K(obdal_account_), K(bucket_), K(uri), KPC(storage_info));
  } else {
    is_opened_ = true;
    file_length_ = 0;
  }
  return ret;
}

int ObStorageObDalMultiPartWriter::write(const char *buf, const int64_t size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_OPEN;
    OB_LOG(WARN, "obdal multipart writer not init", K(ret), K(is_opened_));
  } else if (OB_ISNULL(buf) || OB_UNLIKELY(size < 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), KP(buf), K(size));
  } else if (OB_FAIL(ObDalAccessor::obdal_writer_write(opendal_writer_, buf, size))) {
    OB_LOG(WARN, "failed write in obdal writer", K(ret), K(object_), K(bucket_), K(obdal_account_));
  } else {
    file_length_ += size;
  }
  return ret;
}

int ObStorageObDalMultiPartWriter::pwrite(const char *buf, const int64_t size, const int64_t offset)
{
  UNUSED(offset);
  return write(buf, size);
}

int ObStorageObDalMultiPartWriter::abort()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_OPEN;
    OB_LOG(WARN, "obdal multipart writer not init", K(ret), K(is_opened_));
  } else if (OB_FAIL(ObDalAccessor::obdal_writer_abort(opendal_writer_))) {
    OB_LOG(WARN, "failed to abort obdal multipart writer", K(ret), KP(opendal_writer_));
  } else {
    is_opened_ = false;
  }
  reset();
  return ret;
}

int ObStorageObDalMultiPartWriter::complete()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_OPEN;
    OB_LOG(WARN, "obdal multipart writer not init", K(ret), K(is_opened_));
  } else if (OB_FAIL(ObDalAccessor::obdal_writer_close(opendal_writer_))) {
    OB_LOG(WARN, "failed to close obdal multipart writer", K(ret));
  }
  return ret;
}

int ObStorageObDalMultiPartWriter::close()
{
  int ret = OB_SUCCESS;
  reset();
  return ret;
}

//========================= ObStorageParallelObDalMultiPartWriter =========================
ObStorageParallelObDalMultiPartWriter::ObStorageParallelObDalMultiPartWriter()
  : ObStorageObDalBase(),
    is_opened_(false),
    has_writed_(false),
    opendal_multipart_writer_(nullptr)
{

}

ObStorageParallelObDalMultiPartWriter::~ObStorageParallelObDalMultiPartWriter()
{
  reset();
}

void ObStorageParallelObDalMultiPartWriter::reset()
{
  int ret = OB_SUCCESS;
  is_opened_ = false;
  has_writed_ = false;
  if (opendal_multipart_writer_ != nullptr) {
    if (OB_FAIL(ObDalAccessor::obdal_multipart_writer_free(opendal_multipart_writer_))) {
      OB_LOG(WARN, "failed to free opendal multipart writer", K(ret), KP(opendal_multipart_writer_));
    }
    opendal_multipart_writer_ = nullptr;
  }
  ObStorageObDalBase::reset();
}

int ObStorageParallelObDalMultiPartWriter::open(const ObString &uri, ObObjectStorageInfo *storage_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_opened_)) {
    ret = OB_OPEN_TWICE;
    OB_LOG(WARN, "parallel obdal multipart writer already opened, cannot open again", K(ret), K(uri));
  } else if (OB_FAIL(ObStorageObDalBase::open(uri, storage_info))) {
    OB_LOG(WARN, "failed to open in obdal base", K(ret), K(uri), KPC(storage_info));
  } else if (OB_FAIL(ObDalAccessor::obdal_operator_multipart_writer(op_, object_.ptr(), opendal_multipart_writer_))) {
    OB_LOG(WARN, "failed get opendal writer", K(ret), K(object_),
        K(obdal_account_), K(bucket_), K(uri), KPC(storage_info));
  } else if (OB_FAIL(ObDalAccessor::obdal_multipart_writer_initiate(opendal_multipart_writer_))) {
    OB_LOG(WARN, "failed to exec obdal multipart writer initiat", K(ret));
  } else {
    is_opened_ = true;
  }

  return ret;
}

int ObStorageParallelObDalMultiPartWriter::upload_part(const char *buf, const int64_t size, const int64_t part_id)
{
  int ret = OB_SUCCESS;
  int64_t obdal_part_id = part_id;
  // In obdal, the part id of the operator writer starts from 0,
  // and then each service increases by 1 according to the actual situation.
  // Since S3, OSS all require part ids to start at 1 and be continuous,
  // and the old logic of OB, including tests, is already specified from 1,
  // 1 needs to be subtracted in advance here
  if (OB_LIKELY(storage_type_ == OB_STORAGE_S3
                || storage_type_ == OB_STORAGE_OSS)) {
    obdal_part_id -= 1;
  }

  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_OPEN;
    OB_LOG(WARN, "parallel obdal multipart writer not opened", K(ret));
  } else if (OB_ISNULL(buf) || OB_UNLIKELY(size <= 0) || OB_UNLIKELY(part_id < 1 || part_id > MAX_S3_PART_NUM)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), KP(buf), K(size), K(part_id));
  } else if (OB_FAIL(ObDalAccessor::obdal_multipart_writer_write(opendal_multipart_writer_, buf, size, obdal_part_id))) {
    OB_LOG(WARN, "failed write in obdal writer", K(ret), K(object_), K(bucket_), K(obdal_account_), K(size), K(obdal_part_id));
  } else {
    ATOMIC_SET(&has_writed_, true);
  }
  return ret;
}

int ObStorageParallelObDalMultiPartWriter::abort()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_OPEN;
    OB_LOG(WARN, "obdal multipart writer not init", K(ret), K(is_opened_));
  } else if (OB_FAIL(ObDalAccessor::obdal_multipart_writer_abort(opendal_multipart_writer_))) {
    OB_LOG(WARN, "failed to abort obdal multipart writer", K(ret), KP(opendal_multipart_writer_));
  }
  return ret;
}

int ObStorageParallelObDalMultiPartWriter::complete()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_OPEN;
    OB_LOG(WARN, "obdal multipart writer not init", K(ret), K(is_opened_));
  } else if (ATOMIC_LOAD(&has_writed_) == false) {
    if (OB_FAIL(ObDalAccessor::obdal_operator_write(op_, object_.ptr(), "", 0))) {
      OB_LOG(WARN, "complete an empty multipart upload, but fail to write an empty object",
          K(ret), K(has_writed_), K(bucket_), K(object_));
    }
  } else if (OB_FAIL(ObDalAccessor::obdal_multipart_writer_close(opendal_multipart_writer_))) {
    OB_LOG(WARN, "failed to close obdal multipart writer", K(ret));
  }
  return ret;
}

int ObStorageParallelObDalMultiPartWriter::close()
{
  int ret = OB_SUCCESS;
  reset();
  return ret;
}

} // common
} // oceanbase
