/*
 *  Copyright (c) 2022 OceanBase
 *  OceanBase is licensed under Mulan PubL v2.
 *  You can use this software according to the terms and conditions of the Mulan PubL v2.
 *  You may obtain a copy of Mulan PubL v2 at:
 *           http://license.coscl.org.cn/MulanPubL-2.0
 *  THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 *  EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 *  MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 *  See the Mulan PubL v2 for more details.
 *  Authors:
 *
 */
#define USING_LOG_PREFIX STORAGE

#include "ob_storage_info.h"
#include "share/object_storage/ob_object_storage_struct.h"
#include "lib/json/ob_json.h"
#include "lib/restore/hmac_signature.h"
#include "lib/string/ob_string.h"
namespace oceanbase
{

namespace common
{
const char *OB_STORAGE_CHECKSUM_TYPE_STR[] = {CHECKSUM_TYPE_NO_CHECKSUM, CHECKSUM_TYPE_MD5, CHECKSUM_TYPE_CRC32};

const char *get_storage_checksum_type_str(const ObStorageChecksumType &type)
{
  const char *str = "UNKNOWN";
  STATIC_ASSERT(static_cast<int64_t>(OB_STORAGE_CHECKSUM_MAX_TYPE) == ARRAYSIZEOF(OB_STORAGE_CHECKSUM_TYPE_STR), "ObStorageChecksumType count mismatch");
  if (type >= OB_NO_CHECKSUM_ALGO && type < OB_STORAGE_CHECKSUM_MAX_TYPE) {
    str = OB_STORAGE_CHECKSUM_TYPE_STR[type];
  }
  return str;
}

//***********************ObSTSToken***************************
ObSTSToken::ObSTSToken() :
            data_(nullptr),
            allocator_(OB_DEVICE_CREDENTIAL_ALLOCATOR),
            data_len_(0),
            is_below_predefined_length_(false),
            is_inited_(false)
{
  data_arr_[0] = '\0';
}

ObSTSToken::~ObSTSToken()
{
  reset();
}

void ObSTSToken::reset()
{
  data_ = nullptr;
  data_arr_[0] = '\0';
  data_len_ = 0;
  is_below_predefined_length_ = false;
  allocator_.clear();
  is_inited_ = false;
}

int ObSTSToken::set(const ObString &token)
{
  int ret = OB_SUCCESS;
  if (data_len_ > 0) {
    reset();
  }
  if (!token.empty()) {
    if (OB_UNLIKELY(token.length() > OB_PREDEFINED_STS_TOKEN_LENGTH - 1)
        && OB_FAIL(ob_dup_cstring(allocator_, token, data_))) {
      OB_LOG(WARN, "failed to deep copy ObSTSToken", K(ret), K(token));
    } else if (token.length() <= OB_PREDEFINED_STS_TOKEN_LENGTH - 1) {
      MEMCPY(data_arr_, token.ptr(), token.length());
      data_arr_[token.length()] = '\0';
      is_below_predefined_length_ = true;
    }
    if (OB_SUCC(ret)) {
      data_len_ = token.length();
      is_inited_ = true;
    }
  }
  return ret;
}

bool ObSTSToken::is_valid() const
{
  return is_inited_;
}

int ObSTSToken::assign(const ObSTSToken &token)
{
  int ret = OB_SUCCESS;
  if (token.is_below_predefined_length() && OB_FAIL(set(ObString(token.data_arr_)))) {
    OB_LOG(WARN, "assign ObSTSToken failed", K(ret), K(token));
  } else if (!token.is_below_predefined_length() && OB_FAIL(set(ObString(token.data_)))) {
    OB_LOG(INFO, "assign ObSTSToken failed", K(ret), K(token));
  }
  return ret;
}

const char *ObSTSToken::get_data() const
{
  const char *data = nullptr;
  if (data_len_ > 0) {
    if (is_below_predefined_length_) {
      data = data_arr_;
    } else if (!is_below_predefined_length_) {
      data = data_;
    }
  }
  return data;
}

//***********************ObObjectStorageCredential***************************
ObObjectStorageCredential::ObObjectStorageCredential()
    : sts_token_(), expiration_s_(0), access_time_us_(0), born_time_us_(0)
{
  access_id_[0] = '\0';
  access_key_[0] = '\0';
}

void ObObjectStorageCredential::reset()
{
  access_id_[0] = '\0';
  access_key_[0] = '\0';
  sts_token_.reset();
  expiration_s_ = 0;
  access_time_us_ = 0;
  born_time_us_ = 0;
}

int ObObjectStorageCredential::assign(const ObObjectStorageCredential &credential)
{
  int ret = OB_SUCCESS;
  MEMCPY(access_id_, credential.access_id_, sizeof(access_id_));
  MEMCPY(access_key_, credential.access_key_, sizeof(access_key_));
  expiration_s_ = credential.expiration_s_;
  access_time_us_ = credential.access_time_us_;
  born_time_us_ = credential.born_time_us_;
  if (OB_FAIL(sts_token_.assign(credential.sts_token_))) {
    LOG_WARN("failed to assign sts token", K(ret), K(credential), KPC(this));
  }
  char *a = nullptr;
  return ret;
}

//***********************ObObjectStorageInfo***************************
ObObjectStorageInfo::ObObjectStorageInfo()
  : delete_mode_(ObStorageDeleteMode::STORAGE_DELETE_MODE),
    device_type_(ObStorageType::OB_STORAGE_MAX_TYPE),
    checksum_type_(ObStorageChecksumType::OB_NO_CHECKSUM_ALGO),
    is_assume_role_mode_(false)
{
  endpoint_[0] = '\0';
  access_id_[0] = '\0';
  access_key_[0] = '\0';
  extension_[0] = '\0';
  hdfs_extension_[0] = '\0';
  max_iops_ = 0;
  max_bandwidth_ = 0;
  role_arn_[0] = '\0';
  external_id_[0] = '\0';
}

ObObjectStorageInfo::~ObObjectStorageInfo()
{
  reset();
}

void ObObjectStorageInfo::reset()
{
  delete_mode_ = ObStorageDeleteMode::STORAGE_DELETE_MODE;
  device_type_ = ObStorageType::OB_STORAGE_MAX_TYPE;
  checksum_type_ = ObStorageChecksumType::OB_NO_CHECKSUM_ALGO;
  endpoint_[0] = '\0';
  access_id_[0] = '\0';
  access_key_[0] = '\0';
  extension_[0] = '\0';
  hdfs_extension_[0] = '\0';
  role_arn_[0] = '\0';
  external_id_[0] = '\0';
  is_assume_role_mode_ = false;
}

bool ObObjectStorageInfo::is_valid() const
{
  return device_type_ >= 0 && device_type_ < ObStorageType::OB_STORAGE_MAX_TYPE;
}

int64_t ObObjectStorageInfo::hash() const
{
  int64_t hash_value = 0;
  hash_value = murmurhash(&device_type_, static_cast<int32_t>(sizeof(device_type_)), hash_value);
  hash_value = murmurhash(&checksum_type_, static_cast<int32_t>(sizeof(checksum_type_)), hash_value);
  hash_value = murmurhash(endpoint_, static_cast<int32_t>(strlen(endpoint_)), hash_value);
  hash_value = murmurhash(access_id_, static_cast<int32_t>(strlen(access_id_)), hash_value);
  hash_value = murmurhash(access_key_, static_cast<int32_t>(strlen(access_key_)), hash_value);
  hash_value = murmurhash(extension_, static_cast<int32_t>(strlen(extension_)), hash_value);
  hash_value = murmurhash(hdfs_extension_, static_cast<int32_t>(strlen(hdfs_extension_)), hash_value);
  hash_value = murmurhash(&max_iops_, static_cast<int32_t>(sizeof(max_iops_)), hash_value);
  hash_value = murmurhash(&max_bandwidth_, static_cast<int32_t>(sizeof(max_bandwidth_)), hash_value);
  hash_value = murmurhash(role_arn_, static_cast<int32_t>(strlen(role_arn_)), hash_value);
  hash_value = murmurhash(external_id_, static_cast<int32_t>(strlen(external_id_)), hash_value);

  hash_value = murmurhash(&is_assume_role_mode_, static_cast<int32_t>(sizeof(is_assume_role_mode_)), hash_value);
  return hash_value;
}

bool ObObjectStorageInfo::operator ==(const ObObjectStorageInfo &storage_info) const
{
  return device_type_ == storage_info.device_type_
      && checksum_type_ == storage_info.checksum_type_
      && (0 == STRCMP(endpoint_, storage_info.endpoint_))
      && (0 == STRCMP(access_id_, storage_info.access_id_))
      && (0 == STRCMP(access_key_, storage_info.access_key_))
      && (0 == STRCMP(extension_, storage_info.extension_))
      && (0 == STRCMP(hdfs_extension_, storage_info.hdfs_extension_))
      && (0 == STRCMP(role_arn_, storage_info.role_arn_))
      && (0 == STRCMP(external_id_, storage_info.external_id_))
      && is_assume_role_mode_ == storage_info.is_assume_role_mode_
      && max_iops_ == storage_info.max_iops_
      && max_bandwidth_ == storage_info.max_bandwidth_;
}

bool ObObjectStorageInfo::operator !=(const ObObjectStorageInfo &storage_info) const
{
  return !(*this == storage_info);
}

bool ObObjectStorageInfo::is_access_info_equal(const ObObjectStorageInfo &storage_info) const
{
  return (0 == STRCMP(access_id_, storage_info.access_id_)) &&
         (0 == STRCMP(access_key_, storage_info.access_key_));
}

int ObObjectStorageInfo::reset_access_id_and_access_key(
    const char *access_id, const char *access_key)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(access_id)) {
    int64_t pos = 0;
    if (OB_FAIL(databuff_printf(access_id_, OB_MAX_BACKUP_ACCESSID_LENGTH, pos, "%s%s", ACCESS_ID, access_id))) {
      LOG_WARN("failed to databuff printf", K(ret), KCSTRING(access_id));
    }
  }
  if (OB_NOT_NULL(access_key)) {
    int64_t pos = 0;
    if (FAILEDx(databuff_printf(access_key_, OB_MAX_BACKUP_ACCESSKEY_LENGTH, pos, "%s%s", ACCESS_KEY, access_key))) {
      LOG_WARN("failed to databuff printf", K(ret), KCSTRING(access_key));
    }
  }
  return ret;
}

bool ObObjectStorageInfo::is_assume_role_mode() const
{
  return is_assume_role_mode_;
}

ObClusterVersionBaseMgr *ObObjectStorageInfo::cluster_version_mgr_ = nullptr;
int ObObjectStorageInfo::register_cluster_version_mgr(ObClusterVersionBaseMgr *cluster_version_mgr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cluster_version_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("cluster_version_mgr is null", K(ret));
  } else {
    cluster_version_mgr_ = cluster_version_mgr;
    LOG_INFO("register cluster_version_mgr successfully", K(ret), KP_(cluster_version_mgr));
  }
  return ret;
}

const char *ObObjectStorageInfo::get_type_str() const
{
  return get_storage_type_str(device_type_);
}

ObStorageType ObObjectStorageInfo::get_type() const
{
  return device_type_;
}

ObStorageChecksumType ObObjectStorageInfo::get_checksum_type() const
{
  return checksum_type_;
}

const char *ObObjectStorageInfo::get_checksum_type_str() const
{
  return get_storage_checksum_type_str(checksum_type_);
}

// oss:host=xxxx&access_id=xxx&access_key=xxx
// cos:host=xxxx&access_id=xxx&access_key=xxxappid=xxx
// s3:host=xxxx&access_id=xxx&access_key=xxx&s3_region=xxx
// hdfs:krb5conf=xxx&principal=xxx&keytab=xxx&ticket_cache_path=xxx
int ObObjectStorageInfo::set(const common::ObStorageType device_type, const char *storage_info)
{
  bool has_needed_extension = false;
  int ret = OB_SUCCESS;
  if (is_valid()) {
    ret = OB_INIT_TWICE;
    LOG_WARN("storage info init twice", K(ret));
  } else if (OB_ISNULL(storage_info) || strlen(storage_info) >= OB_MAX_BACKUP_STORAGE_INFO_LENGTH) {
    ret = OB_INVALID_BACKUP_DEST;
    LOG_WARN("storage info is invalid", K(ret), KP(storage_info));
  } else if (FALSE_IT(device_type_ = device_type)) {
  } else if (0 == strlen(storage_info)) {
    // Only file/hdfs storage could be with empty storage_info.
    if (OB_STORAGE_FILE != device_type_ && OB_STORAGE_HDFS != device_type_) {
      ret = OB_INVALID_BACKUP_DEST;
      LOG_WARN("storage info is empty", K(ret), K_(device_type));
    }
  } else if (OB_FAIL(parse_storage_info_(storage_info, has_needed_extension))) {
    LOG_WARN("parse storage info failed", K(ret), KP(storage_info), K_(device_type));
  } else if (OB_STORAGE_COS == device_type && !has_needed_extension) {
    ret = OB_INVALID_BACKUP_DEST;
    LOG_WARN("invalid cos info, appid do not allow to be empty", K(ret), K_(extension));
  } else if (OB_FAIL(validate_arguments())) {
    ret = OB_INVALID_BACKUP_DEST;
    LOG_WARN("invalid arguments after parse storage info", K(ret), KPC(this));
  }

  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

int ObObjectStorageInfo::validate_arguments() const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ObObjectStorageInfo::is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), K(device_type_));
  } else if (OB_STORAGE_FILE != device_type_ && OB_STORAGE_HDFS != device_type_) {
    if (OB_UNLIKELY(0 == strlen(endpoint_))) {
      ret = OB_INVALID_BACKUP_DEST;
      LOG_WARN("backup device is not nfs, endpoint do not allow to be empty", K(ret),
          K_(device_type), K_(endpoint));
    } else {
      if (is_assume_role_mode_) {
        if (OB_UNLIKELY(0 != strlen(access_id_) || 0 != strlen(access_key_))) {
          ret = OB_INVALID_BACKUP_DEST;
          LOG_WARN("ak/sk should not given in assume_role mode",
              K(ret), K_(access_id), KP_(access_key), K(strlen(access_id_)), K(strlen(access_key_)));
        }
      } else {
        if (OB_UNLIKELY(0 == strlen(access_id_) || 0 == strlen(access_key_))) {
          ret = OB_INVALID_BACKUP_DEST;
          LOG_WARN("ak/sk must be given in in ak/sk mode", K(ret), K_(access_id), KP_(access_key),
              K(strlen(access_id_)), K(strlen(access_key_)));
        }
      }
    }
  } else if (OB_STORAGE_HDFS == device_type_) {
    if (OB_UNLIKELY(0 != strlen(endpoint_) || 0 != strlen(access_id_) || 0 != strlen(access_key_)
        || 0 != strlen(role_arn_) || 0 != strlen(external_id_) || 0 != strlen(extension_))) {
      ret = OB_INVALID_BACKUP_DEST;
      LOG_WARN("backup device is hdfs, endpoint/access_id/access_key/ must be empty", K(ret));
    }
  } else {
    if (OB_UNLIKELY(0 != strlen(endpoint_) || 0 != strlen(access_id_) || 0 != strlen(access_key_)
        || 0 != strlen(role_arn_) || 0 != strlen(external_id_))) {
      ret = OB_INVALID_BACKUP_DEST;
      LOG_WARN("backup device is nfs, endpoint/access_id/access_key must be empty", K(ret),
          K_(device_type), K_(endpoint), K_(access_id), KP_(access_key), KP_(role_arn), KP_(external_id));
    }
  }
  return ret;
}

int ObObjectStorageInfo::set(const char *uri, const char *storage_info)
{
  int ret = OB_SUCCESS;
  common::ObStorageType device_type;
  if (OB_FAIL(get_storage_type_from_path(uri, device_type))) {
    LOG_WARN("failed to get storage type from path", K(ret), KPC(this));
  } else if (OB_FAIL(set(device_type, storage_info))) {
    LOG_WARN("failed to set storage info", K(ret), KPC(this));
  }
  return ret;
}

int ObObjectStorageInfo::parse_storage_info_(const char *storage_info, bool &has_needed_extension)
{
  int ret = OB_SUCCESS;
  has_needed_extension = false;
  if (OB_ISNULL(storage_info) || strlen(storage_info) >= OB_MAX_BACKUP_STORAGE_INFO_LENGTH) {
    ret = OB_INVALID_BACKUP_DEST;
    LOG_WARN("storage info is invalid", K(ret), KP(storage_info), K(strlen(storage_info)));
  } else {
    char tmp[OB_MAX_BACKUP_STORAGE_INFO_LENGTH] = { 0 };
    char *token = NULL;
    char *saved_ptr = NULL;
    int64_t info_len = strlen(storage_info);

    MEMCPY(tmp, storage_info, info_len);
    tmp[info_len] = '\0';
    token = tmp;
    for (char *str = token; OB_SUCC(ret); str = NULL) {
      token = ::strtok_r(str, "&", &saved_ptr);
      if (NULL == token) {
        break;
      } else if (0 == strncmp(REGION, token, strlen(REGION))) {
        if (OB_UNLIKELY(OB_STORAGE_S3 != device_type_)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("only s3 protocol can set s3_region", K(ret), K(token), K(device_type_));
        } else if (OB_FAIL(set_storage_info_field_(token, extension_, sizeof(extension_)))) {
          LOG_WARN("failed to set region", K(ret), K(token));
        }
      } else if (0 == strncmp(HOST, token, strlen(HOST))) {
        if (OB_FAIL(set_storage_info_field_(token, endpoint_, sizeof(endpoint_)))) {
          LOG_WARN("failed to set endpoint", K(ret), K(token));
        }
      } else if (0 == strncmp(ACCESS_ID, token, strlen(ACCESS_ID))) {
        if (OB_FAIL(set_storage_info_field_(token, access_id_, sizeof(access_id_)))) {
          LOG_WARN("failed to set access id", K(ret), K(token));
        }
      } else if (0 == strncmp(ACCESS_KEY, token, strlen(ACCESS_KEY))) {
        if (OB_FAIL(set_storage_info_field_(token, access_key_, sizeof(access_key_)))) {
          LOG_WARN("failed to set access key", K(ret));
        }
      } else if (0 == strncmp(MAX_IOPS, token, strlen(MAX_IOPS))) {
        int64_t value = 0;
        char buf[OB_MAX_BACKUP_STORAGE_INFO_LENGTH] = { 0 };
        int64_t pos = 0;
        if (OB_FAIL(databuff_printf(buf, OB_MAX_BACKUP_STORAGE_INFO_LENGTH, pos, "%s", token))) {
          LOG_WARN("failed to databuff printf", K(ret));
        } else if (1 == sscanf(buf, "max_iops=%ld", &value)) {
          max_iops_ = value;
          LOG_INFO("set max iops", K(ret), K(value));
        } else {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("failed to set max iops", K(ret), K(value));
        }
      } else if (0 == strncmp(MAX_BANDWIDTH, token, strlen(MAX_BANDWIDTH))) {
        int64_t value = 0;
        char buf[OB_MAX_BACKUP_STORAGE_INFO_LENGTH] = { 0 };
        int64_t pos = 0;
        if (OB_FAIL(databuff_printf(buf, OB_MAX_BACKUP_STORAGE_INFO_LENGTH, pos, "%s", token))) {
          LOG_WARN("failed to databuff printf", K(ret));
        } else {
          bool is_valid = false;
          value = parse_config_capacity(buf + strlen(MAX_BANDWIDTH), is_valid, true /*check_unit*/);
          if (!is_valid) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("convert failed", K(ret), K(buf));
          } else {
            max_bandwidth_ = value;
            LOG_INFO("parse bandwidth value", K(buf), K(value));
          }
        }
      } else if (0 == strncmp(APPID, token, strlen(APPID))) {
        has_needed_extension = (OB_STORAGE_COS == device_type_);
        if (OB_UNLIKELY(OB_STORAGE_COS != device_type_)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("only cos protocol can appid", K(ret), K(token), K(device_type_));
        } else if (OB_FAIL(set_storage_info_field_(token, extension_, sizeof(extension_)))) {
          LOG_WARN("failed to set appid", K(ret), K(token));
        }
      } else if (0 == strncmp(DELETE_MODE, token, strlen(DELETE_MODE))) {
        if (OB_STORAGE_FILE == device_type_) {
          ret = OB_INVALID_BACKUP_DEST;
          OB_LOG(WARN, "OB_STORAGE_FILE don't support delete mode yet",
            K(ret), K_(device_type), K(token));
        } else if (OB_FAIL(check_delete_mode_(token + strlen(DELETE_MODE)))) {
          OB_LOG(WARN, "failed to check delete mode", K(ret), K(token));
        } else if (OB_FAIL(set_storage_info_field_(token, extension_, sizeof(extension_)))) {
          LOG_WARN("failed to set delete mode", K(ret), K(token));
        }
      } else if (0 == strncmp(ADDRESSING_MODEL, token, strlen(ADDRESSING_MODEL))) {
        if (OB_FAIL(check_addressing_model_(token + strlen(ADDRESSING_MODEL)))) {
          OB_LOG(WARN, "failed to check addressing model", K(ret), K(token));
        } else if (OB_FAIL(set_storage_info_field_(token, extension_, sizeof(extension_)))) {
          LOG_WARN("failed to set addressing model", K(ret), K(token));
        }
      } else if (0 == strncmp(CHECKSUM_TYPE, token, strlen(CHECKSUM_TYPE))) {
        const char *checksum_type_str = token + strlen(CHECKSUM_TYPE);
        if (OB_FAIL(set_checksum_type_(checksum_type_str))) {
          OB_LOG(WARN, "fail to set checksum type", K(ret), K(checksum_type_str));
        } else if (OB_FAIL(set_storage_info_field_(token, extension_, sizeof(extension_)))) {
          LOG_WARN("fail to set checksum type into extension", K(ret), K(token));
        }
      } else if (0 == strncmp(ROLE_ARN, token, strlen(ROLE_ARN))) {
        if (ObStorageType::OB_STORAGE_FILE == device_type_) {
          ret = OB_INVALID_BACKUP_DEST;
          LOG_WARN("OB_STORAGE_FILE don't support assume role yet",
              K(ret), K_(device_type), KP(token));
        } else if (OB_FAIL(set_storage_info_field_(token, role_arn_, sizeof(role_arn_)))) {
          LOG_WARN("failed to set role arn", K(ret), KP(token), KP_(role_arn), K(sizeof(role_arn_)));
        }
      } else if (0 == strncmp(EXTERNAL_ID, token, strlen(EXTERNAL_ID))) {
        if (ObStorageType::OB_STORAGE_FILE == device_type_) {
          ret = OB_INVALID_BACKUP_DEST;
          LOG_WARN("OB_STORAGE_FILE don't support external id yet", K(ret),
              K_(device_type), KP(token));
        } else if (OB_FAIL(set_storage_info_field_(token, external_id_, sizeof(external_id_)))) {
          LOG_WARN("failed to set external id", K(ret), KP(token), KP_(external_id),
              K(sizeof(external_id_)));
        }
      } else if (0 == strncmp(KRB5CONF, token, strlen(KRB5CONF))) {
        if (ObStorageType::OB_STORAGE_HDFS != device_type_) {
          ret = OB_INVALID_BACKUP_DEST;
          LOG_WARN("device don't support KRB5CONF yet", K(ret),
              K_(device_type), KP(token));
        } else if (OB_FAIL(set_storage_info_field_(token, hdfs_extension_, sizeof(hdfs_extension_)))) {
          LOG_WARN("failed to set krb5conf", K(ret), KP(token));
        }
      } else if (0 == strncmp(PRINCIPAL, token, strlen(PRINCIPAL))) {
        if (ObStorageType::OB_STORAGE_HDFS != device_type_) {
          ret = OB_INVALID_BACKUP_DEST;
          LOG_WARN("device don't support PRINCIPAL yet", K(ret),
              K_(device_type), KP(token));
        } else if (OB_FAIL(set_storage_info_field_(token, hdfs_extension_, sizeof(hdfs_extension_)))) {
          LOG_WARN("failed to set principal", K(ret), KP(token));
        }
      } else if (0 == strncmp(KEYTAB, token, strlen(KEYTAB))) {
        if (ObStorageType::OB_STORAGE_HDFS != device_type_) {
          ret = OB_INVALID_BACKUP_DEST;
          LOG_WARN("device don't support KEYTAB yet", K(ret),
              K_(device_type), KP(token));
        } else if (OB_FAIL(set_storage_info_field_(token, hdfs_extension_, sizeof(hdfs_extension_)))) {
          LOG_WARN("failed to set keytab", K(ret), KP(token));
        }
      } else if (0 == strncmp(TICKET_CACHE_PATH, token, strlen(TICKET_CACHE_PATH))) {
        if (ObStorageType::OB_STORAGE_HDFS != device_type_) {
          ret = OB_INVALID_BACKUP_DEST;
          LOG_WARN("device don't support TICKET_CACHE_PATH yet", K(ret),
              K_(device_type), KP(token));
        } else if (OB_FAIL(set_storage_info_field_(token, hdfs_extension_, sizeof(hdfs_extension_)))) {
          LOG_WARN("failed to set ticiket_cache_path", K(ret), KP(token));
        }
      } else if (0 == strncmp(HDFS_CONFIGS, token, strlen(HDFS_CONFIGS))) {
        if (ObStorageType::OB_STORAGE_HDFS != device_type_) {
          ret = OB_INVALID_BACKUP_DEST;
          LOG_WARN("device don't support HDFS_CONFIGS yet", K(ret),
              K_(device_type), KP(token));
        } else if (OB_FAIL(set_storage_info_field_(token, hdfs_extension_, sizeof(hdfs_extension_)))) {
          LOG_WARN("failed to set configs", K(ret), KP(token));
        }
      }
    }

    // If access by assume role, try to get temporary ak/sk into cache to speed up access
    if (OB_SUCC(ret)) {
      if (strlen(role_arn_) > strlen(ROLE_ARN)) {
        if (OB_ISNULL(cluster_version_mgr_)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("cluster version mgr is null", K(ret), KP(cluster_version_mgr_));
        } else if (OB_FAIL(cluster_version_mgr_->is_supported_assume_version())) {
          LOG_WARN("The current version does not support the assume role", K(ret), KPC(this));
        } else {
          is_assume_role_mode_ = true;
          ObObjectStorageCredential credential;
          if (OB_FAIL(ObDeviceCredentialMgr::get_instance().get_credential(*this, credential))) {
            LOG_WARN("failed to get credential by role arn first", K(ret), KPC(this),
                KP(storage_info), K(credential));
          }
        }
      }
    }
  }
  return ret;
}
//TODO(shifagndan): define delete mode as enum
int ObObjectStorageInfo::check_delete_mode_(const char *delete_mode)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(delete_mode)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid args", K(ret), KP(delete_mode));
  } else if (0 != strcmp(delete_mode, "delete") && 0 != strcmp(delete_mode, "tagging")) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "delete mode is invalid", K(ret), K(delete_mode));
  } else if (0 == strcmp(delete_mode, "delete")) {
    delete_mode_ = ObStorageDeleteMode::STORAGE_DELETE_MODE;
  } else {
    delete_mode_ = ObStorageDeleteMode::STORAGE_TAGGING_MODE;
  }
  return ret;
}

int ObObjectStorageInfo::check_addressing_model_(const char *addressing_model) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(addressing_model)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid args", K(ret), KP(addressing_model));
  } else if (0 != strcmp(addressing_model, ADDRESSING_MODEL_VIRTUAL_HOSTED_STYLE)
      && 0 != strcmp(addressing_model, ADDRESSING_MODEL_PATH_STYLE)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "addressing model is invalid", K(ret), K(addressing_model));
  }
  return ret;
}

bool is_oss_supported_checksum(const ObStorageChecksumType checksum_type)
{
  return checksum_type == ObStorageChecksumType::OB_NO_CHECKSUM_ALGO
      || checksum_type == ObStorageChecksumType::OB_MD5_ALGO;
}

bool is_cos_supported_checksum(const ObStorageChecksumType checksum_type)
{
  return checksum_type == ObStorageChecksumType::OB_NO_CHECKSUM_ALGO
      || checksum_type == ObStorageChecksumType::OB_MD5_ALGO;
}

bool is_s3_supported_checksum(const ObStorageChecksumType checksum_type)
{
  return checksum_type == ObStorageChecksumType::OB_CRC32_ALGO
      || checksum_type == ObStorageChecksumType::OB_MD5_ALGO
      || checksum_type == ObStorageChecksumType::OB_NO_CHECKSUM_ALGO;
}

int ObObjectStorageInfo::set_checksum_type_(const char *checksum_type_str)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(checksum_type_str)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid args", K(ret), KP(checksum_type_str));
  } else if (0 == strcmp(checksum_type_str, CHECKSUM_TYPE_NO_CHECKSUM)) {
    checksum_type_ = OB_NO_CHECKSUM_ALGO;
  } else if (0 == strcmp(checksum_type_str, CHECKSUM_TYPE_MD5)) {
    checksum_type_ = OB_MD5_ALGO;
  } else if (0 == strcmp(checksum_type_str, CHECKSUM_TYPE_CRC32)) {
    checksum_type_ = OB_CRC32_ALGO;
  } else {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid checksum type", K(ret), K(checksum_type_str));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(OB_STORAGE_OSS == device_type_ && !is_oss_supported_checksum(checksum_type_))) {
    ret = OB_CHECKSUM_TYPE_NOT_SUPPORTED;
    OB_LOG(WARN, "not supported checksum type for oss",
        K(ret), K_(device_type), K(checksum_type_str), K_(checksum_type));
  } else if (OB_UNLIKELY(OB_STORAGE_COS == device_type_ && !is_cos_supported_checksum(checksum_type_))) {
    ret = OB_CHECKSUM_TYPE_NOT_SUPPORTED;
    OB_LOG(WARN, "not supported checksum type for cos",
        K(ret), K_(device_type), K(checksum_type_str), K_(checksum_type));
  } else if (OB_UNLIKELY(OB_STORAGE_S3 == device_type_ && !is_s3_supported_checksum(checksum_type_))) {
    ret = OB_CHECKSUM_TYPE_NOT_SUPPORTED;
    OB_LOG(WARN, "not supported checksum type for s3",
        K(ret), K_(device_type), K(checksum_type_str), K_(checksum_type));
  }

  return ret;
}

int ObObjectStorageInfo::set_storage_info_field_(const char *info, char *field, const int64_t length)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(info) || OB_ISNULL(field)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(info), KP(field));
  } else {
    const int64_t info_len = strlen(info);
    int64_t pos = strlen(field);
    if (info_len >= length) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("info is too long ", K(ret), K(info_len), K(length));
    } else if (pos > 0 && OB_FAIL(databuff_printf(field, length, pos, "&"))) {
      // cos:host=xxxx&access_id=xxx&access_key=xxx&appid=xxx&delete_mode=xxx
      // extension_ may contain both appid and delete_mode
      // so delimiter '&' should be included
      LOG_WARN("failed to add delimiter to storage info field", K(ret), K(pos), KP(field), K(length));
    } else if (OB_FAIL(databuff_printf(field, length, pos, "%s", info))) {
      LOG_WARN("failed to set storage info field", K(ret), K(pos), KP(field), K(length));
    }
  }
  return ret;
}

int ObObjectStorageInfo::assign(const ObObjectStorageInfo &storage_info)
{
  int ret = OB_SUCCESS;
  delete_mode_ = storage_info.delete_mode_;
  device_type_ = storage_info.device_type_;
  checksum_type_ = storage_info.checksum_type_;
  MEMCPY(endpoint_, storage_info.endpoint_, sizeof(endpoint_));
  MEMCPY(access_id_, storage_info.access_id_, sizeof(access_id_));
  MEMCPY(access_key_, storage_info.access_key_, sizeof(access_key_));
  MEMCPY(extension_, storage_info.extension_, sizeof(extension_));
  MEMCPY(hdfs_extension_, storage_info.hdfs_extension_, sizeof(hdfs_extension_));
  max_iops_ = storage_info.max_iops_;
  max_bandwidth_ = storage_info.max_bandwidth_;
  MEMCPY(role_arn_, storage_info.role_arn_, sizeof(role_arn_));
  MEMCPY(external_id_, storage_info.external_id_, sizeof(external_id_));
  is_assume_role_mode_ = storage_info.is_assume_role_mode_;
  return ret;
}

int ObObjectStorageInfo::get_info_str_(char *storage_info, const int64_t info_len) const
{
  int ret = OB_SUCCESS;
  const int64_t key_len = MAX(OB_MAX_BACKUP_SERIALIZEKEY_LENGTH, OB_MAX_BACKUP_ACCESSKEY_LENGTH);
  char key[key_len] = {0};
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("storage info is invalid", K(ret), K_(device_type));
  } else if (OB_ISNULL(storage_info) || OB_UNLIKELY(info_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(storage_info), K(info_len));
  } else if (OB_STORAGE_FILE != device_type_ &&
             OB_STORAGE_HDFS != device_type_ && !is_assume_role_mode_) {
    // Access object storage by ak/sk
    if (OB_FAIL(get_access_key_(key, sizeof(key)))) {
      LOG_WARN("failed to get access key", K(ret));
    } else if (OB_FAIL(databuff_printf(storage_info, info_len, "%s&%s&%s", endpoint_, access_id_, key))) {
      LOG_WARN("failed to set storage info", K(ret), K(info_len));
    }
  } else if (OB_STORAGE_HDFS == device_type_) {
    // Setup storage info for simple auth on hdfs, more related auth configs will get in
    // append_extension_str_.
    if (OB_ISNULL(hdfs_extension_) || 0 == strlen(hdfs_extension_)) {
      int64_t str_len = strlen(storage_info);
      if (OB_FAIL(databuff_printf(storage_info, info_len, str_len, "&"))) {
        LOG_WARN("failed to set storage info for hdfs device", K(ret), K(info_len));
      }
    }
  }
  return ret;
}

int ObObjectStorageInfo::append_extension_str_(char *storage_info, const int64_t info_len) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("storage info is invalid", K(ret), K_(device_type));
  } else if (OB_ISNULL(storage_info) || OB_UNLIKELY(info_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(storage_info), K(info_len));
  } else if (0 != strlen(extension_) && info_len > strlen(storage_info)) {
    // if OB_STORAGE_FILE's extension is not empty, delimiter should be included
    int64_t str_len = strlen(storage_info);
    if (str_len > 0 && OB_FAIL(databuff_printf(storage_info, info_len, str_len, "&"))) {
      LOG_WARN("failed to add delimiter to storage info", K(ret), K(info_len), K(str_len));
    } else if (OB_FAIL(databuff_printf(storage_info, info_len, str_len, "%s", extension_))) {
      LOG_WARN("failed to add extension", K(ret), K(info_len), K(str_len), K_(extension));
    }
  } else if (OB_STORAGE_HDFS == device_type_ && 0 != strlen(hdfs_extension_) &&
             info_len > strlen(storage_info)) {
    int64_t str_len = strlen(storage_info);
    if (str_len > 0 &&
        OB_FAIL(databuff_printf(storage_info, info_len, str_len, "&"))) {
      LOG_WARN("failed to add delimiter to storage info", K(ret), K(info_len),
               K(str_len));
    } else if (OB_FAIL(databuff_printf(storage_info, info_len, str_len, "%s",
                                       hdfs_extension_))) {
      LOG_WARN("failed to add hdfs extension", K(ret), K(info_len), K(str_len),
               K_(hdfs_extension));
    }
  }
  return ret;
}

int ObObjectStorageInfo::get_storage_info_str(char *storage_info, const int64_t info_len) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("storage info is invalid", K(ret), K_(device_type));
  } else if (OB_ISNULL(storage_info) || OB_UNLIKELY(info_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(storage_info), K(info_len));
  } else if (!is_assume_role_mode_ && OB_FAIL(get_info_str_(storage_info, info_len))) {
    OB_LOG(WARN, "failed to get storage info str", K(ret), K(info_len), KPC(this));
  } else if (is_assume_role_mode_) {
    // Access object storage by assume_role
    int64_t pos = 0;
    if (OB_FAIL(databuff_printf(storage_info, info_len, pos, "%s&%s", endpoint_, role_arn_))) {
      LOG_WARN("failed to set storage info of other types", K(ret), K(info_len), KPC(this));
    }
    // `external_id` is optional
    else if (strlen(external_id_) > strlen(EXTERNAL_ID)) {
      if (OB_FAIL(databuff_printf(storage_info, info_len, pos, "&%s", external_id_))) {
        LOG_WARN("failed to set storage info of other types", K(ret), K(info_len), KPC(this));
      }
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(append_extension_str_(storage_info, info_len))) {
    LOG_WARN("failed to append extension str", K(ret), K(info_len), KPC(this));
  }
  return ret;
}

// This function is used to obtain storage_info_str containing ak/sk,
// regardless of whether the access_mode is assume_role mode or access_id mode
int ObObjectStorageInfo::get_authorization_str(
    char *authorization_str, const int64_t authorization_str_len, ObSTSToken &sts_token) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("storage info is invalid", K(ret), K_(device_type));
  } else if (OB_ISNULL(authorization_str) || OB_UNLIKELY(authorization_str_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(authorization_str), K(authorization_str_len));
  } else if (!is_assume_role_mode_
             && OB_FAIL(get_info_str_(authorization_str, authorization_str_len))) {
    OB_LOG(WARN, "failed to get authorization str", K(ret), K(authorization_str), KPC(this),
        K(authorization_str_len));
  } else if (is_assume_role_mode_) {
    // access by assume role
    ObObjectStorageCredential credential;
    if (OB_FAIL(ObDeviceCredentialMgr::get_instance().get_credential(*this, credential))) {
      OB_LOG(WARN, "failed to get credential", K(ret), KPC(this), K(credential));
    } else if (OB_FAIL(databuff_printf(authorization_str, authorization_str_len,
                   "%s&%s%s&%s%s", endpoint_, ACCESS_ID, credential.access_id_, ACCESS_KEY,
                   credential.access_key_))) {
      OB_LOG(WARN, "failed to set storage info of other types", K(ret), KP(authorization_str),
          K(authorization_str_len), KPC(this));
    } else if (OB_FAIL(sts_token.assign(credential.sts_token_))) {
      OB_LOG(WARN, "failed to set sts_token_", K(ret), K(sts_token), K(credential.sts_token_));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(append_extension_str_(authorization_str, authorization_str_len))) {
    LOG_WARN("failed to append extension str", K(ret), K(authorization_str_len), KPC(this));
  }
  return ret;
}

// Note: delete_mode_ redundantly stores a copy in extension. thus, device map key
// includes extension_ is enough, and has no need to include delete_mode_.
int ObObjectStorageInfo::get_device_map_key_str(char *key_str, const int64_t len) const
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage info not init", K(ret));
  } else if (OB_ISNULL(key_str) || (len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(key_str), K(len));
  } else if (OB_STORAGE_HDFS == device_type_) {
    // handle with hdfs
    int64_t hdfs_extension_hash = murmurhash(
        hdfs_extension_, static_cast<int32_t>(strlen(hdfs_extension_)), 0);
    if (OB_FAIL(databuff_printf(key_str, len, "%u&%ld",
                                static_cast<uint32_t>(device_type_),
                                hdfs_extension_hash))) {
      LOG_WARN("failed to set key str for hdfs", K(ret), K(len), KPC(this));
    }
  } else if (is_assume_role_mode_) {
    // access by assume_role
    int64_t pos = 0;
    if (OB_FAIL(databuff_printf(key_str, len, pos, "%u&%u&%s&%s&%s",
            static_cast<uint32_t>(device_type_), static_cast<uint32_t>(checksum_type_), endpoint_,
            role_arn_, extension_))) {
      LOG_WARN("failed to set key str with assume role", K(ret), K(len), K(pos), KPC(this));
    } else if (strlen(external_id_) > strlen(EXTERNAL_ID)) {
      if (OB_FAIL(databuff_printf(key_str, len, pos, "&%s", external_id_))) {
        LOG_WARN("failed to set key str with assume role", K(ret), K(len), K(pos), KPC(this));
      }
    }
  }
  // access by access_id
  else if (OB_FAIL(databuff_printf(key_str, len, "%u&%u&%s&%s&%s&%s",
               static_cast<uint32_t>(device_type_), static_cast<uint32_t>(checksum_type_),
               endpoint_, access_id_, access_key_, extension_))) {
    LOG_WARN("failed to set key str with access_id", K(ret), K(len), KPC(this));
  }
  return ret;
}

// Note: device map key does not include delete_mode_. thus, device map key len does not include
// delete_mode_ len too.
int64_t ObObjectStorageInfo::get_device_map_key_len() const
{
  // ObStorageType and ObStorageChecksumType are uint8_t, but static_cast to uint32_t in get_device_map_key_str.
  // therefore, ObStorageType and ObStorageChecksumType each occupies up to 10 characters.
  // 10(one ObStorageType) + 10(one ObStorageChecksumType) + 5(five '&') + 1(one '\0') = 26.
  // reserve some free space, increase 26 to 30.
  int64_t len = 0;
  if (OB_STORAGE_HDFS == device_type_) {
    // Hdfs storage info will store in hdfs_extension_, and it maybe a long string, so hash it as key.
    // 8 is sizeof(int64_t) which is the size of murmurhash value.
    len = 30 + 8;
  } else if (is_assume_role_mode_) {
    // When accessing by assume_role, the length of key_str is also need reversed free space.
    len = STRLEN(endpoint_) + STRLEN(role_arn_) + STRLEN(extension_) + 30;
    // external_id is optional
    if (STRLEN(external_id_) > STRLEN(EXTERNAL_ID)) {
      len += STRLEN(external_id_);
    }
  } else {
    len = STRLEN(endpoint_) + STRLEN(access_id_) + STRLEN(access_key_) + STRLEN(extension_) + 30;
  }
  return len;
}

int ObObjectStorageInfo::get_access_key_(char *key_buf, const int64_t key_buf_len) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(key_buf) || key_buf_len <= strlen(access_key_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(key_buf), K(key_buf_len));
  } else {
    MEMCPY(key_buf, access_key_, strlen(access_key_));
    key_buf[strlen(access_key_)] = '\0';
  }
  return ret;
}

//***********************ObStsCredential***************************
ObStsCredential::ObStsCredential()
    : tenant_id_(OB_SERVER_TENANT_ID), is_inited_(false)
{
  sts_ak_[0] = '\0';
  sts_sk_[0] = '\0';
  sts_url_[0] = '\0';
}

ObStsCredential::~ObStsCredential()
{
  reset();
}

int ObStsCredential::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("sts credential init twice", K(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(ret), K(tenant_id));
  } else if (OB_FAIL(get_sts_credential())) {
    LOG_WARN("failed to get sts credential", K(ret), K(tenant_id_));
  } else {
    tenant_id_ = tenant_id;
    is_inited_ = true;
  }
  return ret;
}

int ObStsCredential::get_sts_credential()
{
  int ret = OB_SUCCESS;
  char sts_credential[OB_MAX_STS_CREDENTIAL_LENGTH] = {0};
  int64_t sts_credential_len = 0;
  if (OB_ISNULL(sts_credential_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sts_credential_mgr is null", K(ret), KPC(this));
  } else if (OB_FAIL(sts_credential_mgr_->get_sts_credential(sts_credential, sizeof(sts_credential)))) {
    LOG_WARN("fail to get sts credential", K(ret), KPC(this));
  } else if (FALSE_IT(sts_credential_len = strlen(sts_credential))) {
  } else if (OB_UNLIKELY(sts_credential_len <= 0
      || sts_credential_len >= OB_MAX_STS_CREDENTIAL_LENGTH)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sts_credential is invalid", K(ret), K(sts_credential), K(sts_credential_len));
  } else if (OB_FAIL(check_sts_credential_format(sts_credential, *this))) {
    LOG_WARN("failed to check sts credential format", K(ret), KP(sts_credential), KPC(this));
  }
  return ret;
}

void ObStsCredential::reset()
{
  is_inited_ = false;
  tenant_id_ = OB_SERVER_TENANT_ID;
  sts_ak_[0] = '\0';
  sts_sk_[0] = '\0';
  sts_url_[0] = '\0';
}

//***********************ObDeviceCredentialKey***************************

ObDeviceCredentialKey::ObDeviceCredentialKey()
    : tenant_id_(OB_SERVER_TENANT_ID), is_inited_(false)
{
  role_arn_[0] = '\0';
  external_id_[0] = '\0';
}

ObDeviceCredentialKey::~ObDeviceCredentialKey()
{
  reset();
}

int ObDeviceCredentialKey::init(const ObObjectStorageInfo &storage_info)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(!storage_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(storage_info));
  } else if (OB_FAIL(init_(storage_info.role_arn_, storage_info.external_id_))) {
    LOG_WARN("failed to init device credential key", K(ret), K(storage_info));
  }
  return ret;
}

int ObDeviceCredentialKey::init_(const char *role_arn, const char *external_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_ISNULL(role_arn) || OB_ISNULL(external_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(role_arn), KP(external_id));
  } else {
    // format:
    // role_arn: role_arn=xxx
    // external_id: external_id=xxx
    const int64_t role_arn_len = strlen(role_arn);
    const int64_t external_id_len = strlen(external_id);
    if (OB_UNLIKELY(0 != STRNCMP(role_arn, ROLE_ARN, STRLEN(ROLE_ARN))
        || role_arn_len >= OB_MAX_ROLE_ARN_LENGTH)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid role arn", K(ret), K(role_arn_len), KP(role_arn), K(ROLE_ARN));
    }
    // External id is optional, so the length of external id can be 0
    else if (OB_UNLIKELY(external_id_len < 0 || external_id_len >= OB_MAX_EXTERNAL_ID_LENGTH
        || (external_id_len > 0 && 0 != STRNCMP(external_id, EXTERNAL_ID, STRLEN(EXTERNAL_ID))))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid external id", K(ret), KP(external_id), K(external_id_len), K(EXTERNAL_ID));
    } else {
        tenant_id_ = ObObjectStorageTenantGuard::get_tenant_id();
        if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id_))) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid tenant_id", K(ret), K_(tenant_id));
        } else {
          MEMCPY(role_arn_, role_arn, role_arn_len);
          role_arn_[role_arn_len] = '\0';
          MEMCPY(external_id_, external_id, external_id_len);
          external_id_[external_id_len] = '\0';
          is_inited_ = true;
        }
        if (OB_FAIL(ret)) {
          reset();
        }
    }
  }
  return ret;
}

void ObDeviceCredentialKey::reset()
{
  role_arn_[0] = '\0';
  external_id_[0] = '\0';
  tenant_id_ = OB_SERVER_TENANT_ID;
  is_inited_ = false;
}

uint64_t ObDeviceCredentialKey::hash() const
{
  uint64_t hash_value = 0;
  hash_value = murmurhash(role_arn_, static_cast<int32_t>(strlen(role_arn_)), hash_value);
  hash_value = murmurhash(external_id_, static_cast<int32_t>(strlen(external_id_)), hash_value);
  hash_value = murmurhash(&tenant_id_, sizeof(tenant_id_), hash_value);
  hash_value = murmurhash(&is_inited_, static_cast<int32_t>(sizeof(is_inited_)), hash_value);
  return hash_value;
}

int ObDeviceCredentialKey::assign(const ObDeviceCredentialKey &other)
{
  int ret = OB_SUCCESS;
  if (this == &other) {
  } else if (OB_UNLIKELY(!other.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(other));
  } else {
    MEMCPY(role_arn_, other.role_arn_, sizeof(role_arn_));
    MEMCPY(external_id_, other.external_id_, sizeof(external_id_));
    tenant_id_ = other.tenant_id_;
    is_inited_ = other.is_inited_;
  }
  return ret;
}

bool ObDeviceCredentialKey::operator==(const ObDeviceCredentialKey &other) const
{
  return (is_inited_ == other.is_inited_)
         && (0 == STRCMP(role_arn_, other.role_arn_))
         && (0 == STRCMP(external_id_, other.external_id_))
         && (tenant_id_ == other.tenant_id_);
}

bool ObDeviceCredentialKey::operator!=(const ObDeviceCredentialKey &other) const
{
  return !(*this == other);
}

bool ObDeviceCredentialKey::is_valid() const
{
  return is_inited_;
}

int ObDeviceCredentialKey::construct_signed_url(char *url_buf, const int64_t url_buf_len) const
{
  int ret = OB_SUCCESS;
  const char *role_arn = role_arn_ + STRLEN(ROLE_ARN);
  const char *external_id = external_id_ + STRLEN(EXTERNAL_ID);
  char json_data[OB_MAX_ASSUME_ROLE_JSON_DATA_LENGTH] = {0};
  char signature_nonce[OB_MAX_STS_SIGNATURE_NONCE_LENTH] = {0};
  // request_id is used by OCP to identify the request when connected to the service of OCP
  char request_id[OB_MAX_STS_REQUEST_ID_LENTH] = {0};
  char signature[OB_MAX_STS_SIGNATURE_LENGTH] = {0};
  ObStsCredential sts_credential;
  using ParamType = std::pair<const char *, const char *>;
  ObArray<ParamType> params;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "ObDeviceCredentialKey is not valid", K(ret));
  } else if (OB_ISNULL(url_buf) || OB_UNLIKELY(url_buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid args", K(ret), KP(url_buf), K(url_buf_len));
  } else if (OB_FAIL(generate_signature_nonce(signature_nonce, sizeof(signature_nonce)))) {
    LOG_WARN("failed to generate signature nonce",
        K(ret), K(signature_nonce), K(sizeof(signature_nonce)));
  } else if (OB_FAIL(generate_request_id(request_id, sizeof(request_id)))) {
    LOG_WARN("failed to generate request id", K(ret), K(request_id), K(sizeof(request_id)));
  } else if (OB_FAIL(sts_credential.init(tenant_id_))) {
    LOG_WARN("failed to init sts credential", K(ret), K(tenant_id_));
  } else {
    if (OB_FAIL(params.push_back(ParamType("Action", STS_ACTION)))) {
      OB_LOG(WARN, "fail to push back", K(ret), K(params.count()), K(params));
    } else if (OB_FAIL(params.push_back(ParamType("RequestSource", STS_RESOURCE_SOURCE)))) {
      OB_LOG(WARN, "fail to push back", K(ret), K(params.count()), K(params));
    } else if (OB_FAIL(params.push_back(ParamType("RequestId", request_id)))) {
      OB_LOG(WARN, "fail to push back", K(ret), K(params.count()), K(params));
    } else if (OB_FAIL(params.push_back(ParamType("RoleArn", role_arn)))) {
      OB_LOG(WARN, "fail to push back", K(ret), K(params.count()), K(params));
    } else if (OB_FAIL(params.push_back(ParamType("SignatureNonce", signature_nonce)))) {
      OB_LOG(WARN, "fail to push back", K(ret), K(params.count()), K(params));
    } else if (OB_FAIL(params.push_back(ParamType("ObSignatureKey", sts_credential.sts_ak_)))) {
      OB_LOG(WARN, "fail to push back", K(ret), K(params.count()), K(params));
    } else if (OB_FAIL(params.push_back(ParamType("ObSignatureSecret", sts_credential.sts_sk_)))) {
      OB_LOG(WARN, "fail to push back", K(ret), K(params.count()), K(params));
    // be careful to handle external_id_ and external_id
    } else if (external_id_[0] != '\0'
        && OB_FAIL(params.push_back(ParamType("ExternalId", external_id)))) {
      OB_LOG(WARN, "fail to push back", K(ret), K(params.count()));
    }

    int64_t pos = 0;
    if (FAILEDx(databuff_printf(url_buf, url_buf_len, pos,
        "%s?Action=%s&RequestSource=%s"
        "&RequestId=%s&RoleArn=%s&SignatureNonce=%s&ObSignatureKey=%s",
        sts_credential.sts_url_, STS_ACTION, STS_RESOURCE_SOURCE,
        request_id, role_arn, signature_nonce, sts_credential.sts_ak_))) {
      LOG_WARN("failed to generate json payload", K(ret), K(url_buf_len), KPC(this));
    } else if (external_id_[0] != '\0'
        && OB_FAIL(databuff_printf(url_buf, url_buf_len, pos, "&ExternalId=%s", external_id))) {
      LOG_WARN("failed to concat external id", K(ret), K(url_buf_len), K(pos), KPC(this));
    } else if (OB_FAIL(sign_request(params, "POST", signature, sizeof(signature)))) {
      LOG_WARN("failed to sign request", K(ret), K(signature), K(params), KPC(this));
    } else if (OB_FAIL(databuff_printf(url_buf, url_buf_len, pos, "&ObSignature=%s", signature))) {
      LOG_WARN("failed to concat sts signature", K(ret), K(pos), K(url_buf_len), KPC(this));
    }
  }
  return ret;
}

ObTenantStsCredentialBaseMgr *ObStsCredential::sts_credential_mgr_ = nullptr;
int ObStsCredential::register_sts_credential_mgr(
    ObTenantStsCredentialBaseMgr *sts_credential_mgr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sts_credential_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "sts_credential_mgr is null", K(ret), KP(sts_credential_mgr));
  } else {
    sts_credential_mgr_ = sts_credential_mgr;
  }
  return ret;
}

int check_sts_credential_format(const char *sts_credential_str, ObStsCredential &sts_credential)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sts_credential_str)
      || OB_UNLIKELY(strlen(sts_credential_str) <= 0
      || strlen(sts_credential_str) >= OB_MAX_STS_CREDENTIAL_LENGTH)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(sts_credential_str));
  } else {
    // sts_credential_str: sts_ak=xxx&sts_sk=xxx&sts_url=xxx
    // sts_ak, sts_sk, sts_url are separated by '&'
    // sts_ak, sts_sk, sts_url are connected by '='
    char tmp_sts_credential[OB_MAX_STS_CREDENTIAL_LENGTH] = {0};
    char *token = nullptr;
    char *saved_ptr = nullptr;
    int64_t sts_credential_len = strlen(sts_credential_str);
    char *sts_ak = sts_credential.sts_ak_;
    char *sts_sk = sts_credential.sts_sk_;
    char *sts_url = sts_credential.sts_url_;

    MEMCPY(tmp_sts_credential, sts_credential_str, sts_credential_len);
    tmp_sts_credential[sts_credential_len] = '\0';
    token = tmp_sts_credential;
    for (char *str = token; OB_SUCC(ret); str = nullptr) {
      token = ::strtok_r(str, "&", &saved_ptr);
      if (nullptr == token) {
        break;
      } else if (0 == strncmp(STS_AK, token, strlen(STS_AK))) {
        if (OB_FAIL(ob_set_field(token + strlen(STS_AK), sts_ak, sizeof(sts_credential.sts_ak_)))) {
          LOG_WARN("failed to set sts ak", K(ret), KP(token), K(sts_ak),
              K(sizeof(sts_credential.sts_ak_)));
        }
      } else if (0 == strncmp(STS_SK, token, strlen(STS_SK))) {
        if (OB_FAIL(ob_set_field(token + strlen(STS_SK), sts_sk, sizeof(sts_credential.sts_sk_)))) {
          LOG_WARN("failed to set sts sk", K(ret), KP(token), KP(sts_sk),
              K(sizeof(sts_credential.sts_sk_)));
        }
      } else if (0 == strncmp(STS_URL, token, strlen(STS_URL))) {
        if (OB_FAIL(ob_set_field(token + strlen(STS_URL), sts_url, sizeof(sts_credential.sts_url_)))) {
          LOG_WARN("failed to set sts url", K(ret), K(token), K(sts_url),
              K(sizeof(sts_credential.sts_url_)));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(sts_ak[0] == '\0' || sts_sk[0] == '\0' || sts_url[0] == '\0')) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid sts credential", K(ret), K(sts_ak), KP(sts_sk), K(sts_url));
      }
    }
  }
  return ret;
}


//***********************ObDeviceCredentialMgr***************************
const char *ObDeviceCredentialMgr::response_items_[RESPONSE_ITEM_CNT] = {
    "AccessKeyId", "AccessKeySecret", "SecurityToken", "DurationSeconds"};

ObDeviceCredentialMgr &ObDeviceCredentialMgr::get_instance()
{
  static ObDeviceCredentialMgr device_credential_mgr;
  return device_credential_mgr;
}

ObDeviceCredentialMgr::ObDeviceCredentialMgr()
    : is_inited_(false),
      credential_map_(),
      credential_lock_(common::ObLatchIds::OB_DEVICE_CREDENTIAL_MGR_LOCK),
      credential_duration_us_(0)
{}

ObDeviceCredentialMgr::~ObDeviceCredentialMgr()
{
  destroy();
}

int ObDeviceCredentialMgr::init()
{
  int ret = OB_SUCCESS;
  const int64_t DEFAULT_MAP_BUCKET_CNT = 100;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(credential_map_.create(DEFAULT_MAP_BUCKET_CNT, "CredentialMap", "CredentialMap"))) {
    LOG_WARN("fail to init credential map", K(ret), K(DEFAULT_MAP_BUCKET_CNT));
  } else {
    credential_duration_us_ = MAX_CREDENTIAL_IDLE_DURATION_US;
    is_inited_ = true;
  }
  return ret;
}

void ObDeviceCredentialMgr::destroy()
{
  is_inited_ = false;
  credential_duration_us_ = 0;
  credential_map_.destroy();
}

int ObDeviceCredentialMgr::connect_to_sts(
    const ObDeviceCredentialKey &credential_key,
    ResponseAndAllocator &res_and_allocator)
{
  int ret = OB_SUCCESS;
  OBJECT_STORAGE_GUARD(nullptr/*storage_info*/, "ObDeviceCredentialMgr::connect_to_sts", IO_HANDLED_SIZE_ZERO);
  CURL *curl = nullptr;
  char json_data[OB_MAX_ASSUME_ROLE_JSON_DATA_LENGTH] = {0};

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDeviceCredentialMgr not init", K(ret));
  } else if (OB_UNLIKELY(!credential_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(credential_key));
  } else if (OB_ISNULL(curl = curl_easy_init())) {
    ret = OB_CURL_ERROR;
    LOG_WARN("fail to init curl", K(ret));
  } else if (OB_FAIL(credential_key.construct_signed_url(json_data, sizeof(json_data)))) {
    LOG_WARN("fail to construct signed url", K(ret), K(credential_key));
  } else {
    CURLcode cc;
    int64_t http_code = 0;
    const int64_t no_signal = 1;
    const int64_t no_delay = 1;
    struct curl_slist *headers = nullptr;
    struct curl_slist *new_header = nullptr;

    const char *header_list[] = {
        "Accept: application/json",
        "Content-Type: application/json",
        "charset: utf-8"
    };

    const uint32_t num_headers = sizeof(header_list) / sizeof(header_list[0]);
    for (uint32_t i = 0; OB_SUCC(ret) && i < num_headers; ++i) {
      if (OB_ISNULL(new_header = curl_slist_append(headers, header_list[i]))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to append header", K(ret), K(i), K(header_list[i]));
      } else {
        headers = new_header;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (CURLE_OK != (cc = curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers))) {
      LOG_WARN("set http header failed", K(cc));
    }
    // libcurl must carry data field, otherwise it will get stuck. An empty data field is added here
    // for normal access.
    else if (CURLE_OK != (cc = curl_easy_setopt(curl, CURLOPT_POSTFIELDS, ""))) {
      LOG_WARN("set post fields failed", K(cc));
    } else if (CURLE_OK != (cc = curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, 0))) {
      LOG_WARN("set post fields size failed", K(cc));
    } else if (CURLE_OK != (cc = curl_easy_setopt(curl, CURLOPT_POST, 1))) {
      LOG_WARN("set post failed", K(cc));
    } else if (CURLE_OK != (cc = curl_easy_setopt(curl, CURLOPT_READFUNCTION, nullptr))) {
      LOG_WARN("set read function failed", K(cc));
    } else if (CURLE_OK != (cc = curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, &on_write_data_))) {
      LOG_WARN("set write function failed", K(cc));
    } else if (CURLE_OK != (cc = curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void *)&res_and_allocator))) {
      LOG_WARN("set wrrite data failed", K(ret));
    } else if (CURLE_OK != (cc = curl_easy_setopt(curl, CURLOPT_NOSIGNAL, no_signal))) {
      LOG_WARN("set no signal failed", K(cc), K(no_signal));
    } else if (CURLE_OK != (cc = curl_easy_setopt(curl, CURLOPT_TCP_NODELAY, no_delay))) {
      LOG_WARN("set no delay failed", K(cc), K(no_delay));
    } else if (CURLE_OK != (cc = curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT_MS, OB_MAX_STS_CURL_CONNECTTIMEOUT_MS))) {  // 10s
      LOG_WARN("set connection timeout ms failed", K(cc));
    } else if (CURLE_OK != (cc = curl_easy_setopt(curl, CURLOPT_TIMEOUT, OB_MAX_STS_CURL_TIMEOUT_SECONDS))) {  // 10s
      LOG_WARN("set timeout failed", K(cc));
    } else if (CURLE_OK != (cc = curl_easy_setopt(curl, CURLOPT_URL, json_data))) {
      LOG_WARN("set url failed", K(cc));
    }
#ifndef NDEBUG
    else if (CURLE_OK != (cc = curl_easy_setopt(curl, CURLOPT_DEBUGFUNCTION, &debug_callback))) {
      LOG_WARN("set debug function failed", K(cc));
    } else if (CURLE_OK != (cc = curl_easy_setopt(curl, CURLOPT_VERBOSE, 1L))) {
      LOG_WARN("set curl verbose failed", K(cc));
    }
#endif
    else if (CURLE_OK != (cc = curl_easy_perform(curl))) {
      LOG_WARN("perform curl failed", K(cc));
    } else if (CURLE_OK != (cc = curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code))) {
      LOG_WARN("curl get response code failed", K(cc));
    } else if ((http_code / 100) != 2) {  // http status code 2xx means success
      ret = OB_CURL_ERROR;
      LOG_WARN("unexpected http status code", K(ret), K(http_code));
    }
    if (OB_SUCC(ret) && (CURLE_OK != cc)) {
      ret = OB_CURL_ERROR;
      LOG_WARN("fail to curl credential", K(ret), K(cc), KP(json_data));
    }
    if (OB_NOT_NULL(headers)) {
      curl_slist_free_all(headers);
    }
  }
  if (OB_NOT_NULL(curl)) {
    curl_easy_cleanup(curl);
  }
  return ret;
}

int ObDeviceCredentialMgr::curl_credential(
    const ObObjectStorageInfo &storage_info,
    const bool update_access_time)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDeviceCredentialMgr not init", K(ret));
  } else if (OB_UNLIKELY(!storage_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("storage_info is invalid", K(ret), K(storage_info));
  } else {
    ObDeviceCredentialKey credential_key;
    if (OB_FAIL(credential_key.init(storage_info))) {
      LOG_WARN("failed to init credential key", K(ret), K(storage_info));
    } else if (OB_FAIL(curl_credential(credential_key, update_access_time))) {
      LOG_WARN("failed to curl credential", K(ret), K(credential_key));
    }
  }
  return ret;
}

int ObDeviceCredentialMgr::curl_credential(
    const ObDeviceCredentialKey &credential_key,
    const bool update_access_time)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDeviceCredentialMgr not init", K(ret));
  } else if (OB_UNLIKELY(!credential_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("credential_key is invalid", K(ret), K(credential_key));
  } else {
    char *response = nullptr;
    ObArenaAllocator allocator(OB_DEVICE_CREDENTIAL_ALLOCATOR);
    ResponseAndAllocator res_and_allocator(response, allocator);
    if (OB_FAIL(connect_to_sts(credential_key, res_and_allocator))) {
      LOG_WARN("fail to connect to sts", K(ret), K(credential_key));
    } else {
      ObObjectStorageCredential device_credential;
      CredentialAccessTimeCallBack read_callback(false /*update_access_time*/);
      if (OB_FAIL(parse_device_credential_(res_and_allocator.response_, device_credential))) {
        LOG_WARN("fail to parse device credential", K(ret), K(res_and_allocator.response_), K(credential_key));
      } else {
        SpinWLockGuard w_guard(credential_lock_);
        if (update_access_time) {
          device_credential.access_time_us_ = ObTimeUtility::current_time();
        } else {
          if (OB_FAIL(credential_map_.read_atomic(credential_key, read_callback))) {
            if (ret == OB_HASH_NOT_EXIST) {
              ret = OB_SUCCESS;
              device_credential.access_time_us_ = ObTimeUtility::current_time();
            } else {
              LOG_WARN("fail to get credential access time", K(ret), K(device_credential), K(credential_key));
            }
          } else {
            device_credential.access_time_us_ = read_callback.original_access_time_us_;
          }
        }

        if (FAILEDx(credential_map_.set_refactored(
                credential_key, device_credential, true /*overwrite*/))) {
          LOG_WARN("fail to set refactored", K(ret), K(credential_key), K(device_credential));
        }
      }
    }
  }
  return ret;
}

int ObDeviceCredentialMgr::get_credential(
    const ObObjectStorageInfo &storage_info,
    ObObjectStorageCredential &device_credential)
{
  int ret = OB_SUCCESS;
  ObDeviceCredentialKey credential_key;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDeviceCredentialMgr not init", K(ret));
  } else if (OB_UNLIKELY(!storage_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("storage_info is invalid", K(ret), K(storage_info));
  } else if (OB_FAIL(credential_key.init(storage_info))) {
    LOG_WARN("failed to init credential key", K(ret), K(storage_info));
  } else if (OB_FAIL(get_credential(credential_key, device_credential))) {
    LOG_WARN("failed to get credential", K(ret), K(storage_info), K(credential_key));
  }
  return ret;
}

int ObDeviceCredentialMgr::get_credential(
    const ObDeviceCredentialKey &credential_key, ObObjectStorageCredential &device_credential)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDeviceCredentialMgr not init", K(ret));
  } else if (OB_UNLIKELY(!credential_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("credential_key is invalid", K(ret), K(credential_key));
  } else if (OB_FAIL(get_credential_from_map_(credential_key, device_credential))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;  // ignore ret
      if (OB_FAIL(curl_credential(credential_key, true /*update_access_time*/))) {
        LOG_WARN("fail to curl credential", K(ret), K(credential_key));
      } else if (OB_FAIL(get_credential_from_map_(credential_key, device_credential))) {
        LOG_WARN("fail to get_refactored", K(ret), K(credential_key));
      }
    } else {
      LOG_WARN("fail to get_refactored", K(ret), K(credential_key));
    }
  }

  if (OB_SUCC(ret)) {
    int tmp_ret = OB_SUCCESS;
    const int64_t remained_time_us = device_credential.born_time_us_
                                   + device_credential.expiration_s_ * 1000LL * 1000LL
                                   - ObTimeUtility::current_time();
    if (remained_time_us < CREDENTIAL_TASK_SCHEDULE_INTERVAL_US / 2) {
      ObObjectStorageCredential tmp_credential;
      if (OB_TMP_FAIL(curl_credential(credential_key, true/*update_access_time*/))) {
        LOG_WARN("fail to refresh the nearly expiring credential", K(ret),
            K(tmp_ret), K(credential_key), K(device_credential), K(remained_time_us));
      } else {
        LOG_INFO("succeed refreshing the cached credential",
            K(credential_key), K(remained_time_us));

        if (OB_TMP_FAIL(get_credential_from_map_(credential_key, tmp_credential))) {
          LOG_WARN("fail to get refreshed credential",
              K(ret), K(tmp_ret), K(credential_key), K(remained_time_us));
        } else if (OB_FAIL(device_credential.assign(tmp_credential))) {
          LOG_WARN("succeed refreshing the cached credential but fail to assign",
              K(ret), K(credential_key), K(remained_time_us));
        }
      }
    }

    // If the old credential has expired, it is necessary to use the refreshed key
    if (OB_SUCC(ret) && remained_time_us <= 0 && OB_TMP_FAIL(tmp_ret)) {
      ret = tmp_ret;
      OB_LOG(WARN, "the old credential has expired, and fail to refresh",
          K(ret), K(remained_time_us));
    }
  }
  return ret;
}

int ObDeviceCredentialMgr::get_credential_from_map_(
    const ObDeviceCredentialKey &credential_key,
    ObObjectStorageCredential &device_credential)
{
  int ret = OB_SUCCESS;
  SpinRLockGuard r_guard(credential_lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDeviceCredentialMgr is not inited", K(ret));
  } else if (OB_UNLIKELY(!credential_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("credential_key is invalid", K(ret), K(credential_key));
  } else {
    CredentialAccessTimeCallBack callback(true /*update_access_time*/);
    if (OB_FAIL(credential_map_.get_refactored(credential_key, device_credential))) {
      LOG_WARN("fail to get_refactored", K(ret), K(credential_key));
    } else if (OB_FAIL(credential_map_.atomic_refactored(credential_key, callback))) {
      LOG_WARN("fail to update access time", K(ret), K(device_credential), K(credential_key));
    }
  }
  return ret;
}

int64_t ObDeviceCredentialMgr::on_write_data_(
    const void *ptr,
    const int64_t size,
    const int64_t nmemb,
    void *user_data)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ptr) || OB_UNLIKELY((size < 0 || nmemb < 0)) || OB_ISNULL(user_data)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(ptr), K(size), K(nmemb), KP(user_data));
  } else {
    // "阿里云STS服务返回的安全令牌（STS
    // Token）的长度不固定，强烈建议您不要假设安全令牌的最大长度。" there exists sts_token in the
    // response of curl. therefore, use allocator to alloc mem dynamically
    ResponseAndAllocator *res_and_allocator = static_cast<ResponseAndAllocator *>(user_data);
    char *&response = res_and_allocator->response_;
    ObArenaAllocator &allocator = res_and_allocator->allocator_;
    ObArenaAllocator tmp_allocator(OB_DEVICE_CREDENTIAL_ALLOCATOR);
    if (nullptr != response) {  // already has received response data
      // 1. save previous response data to tmp_response
      char *tmp_response = nullptr;
      const int64_t prev_response_size = STRLEN(response);
      if (OB_FAIL(ob_dup_cstring(tmp_allocator, ObString(response), tmp_response))) {
        LOG_WARN("fail to dup cstring", K(ret), K(response), K(prev_response_size));
      }
      // 2. copy previous response data and current response data to response
      if (OB_SUCC(ret)) {
        const int64_t curr_response_size = prev_response_size + size * nmemb;
        if (OB_ISNULL(response = static_cast<char *>(allocator.alloc(curr_response_size + 1)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc memory", K(ret), "sz", curr_response_size + 1);
        } else {
          MEMCPY(response, tmp_response, prev_response_size);
          const char *ptr_data = static_cast<const char *>(ptr);
          MEMCPY(response + prev_response_size, ptr_data, size * nmemb);
          response[curr_response_size] = '\0';
        }
      }
    } else {  // first time to receive response data
      // copy response data to response
      if (OB_FAIL(ob_dup_cstring(allocator, ObString(static_cast<const char *>(ptr)), response))) {
        LOG_WARN("fail to dup cstring", K(ret), K(ptr), K(size), K(nmemb));
      }
    }
  }
  return (OB_SUCCESS == ret) ? (size * nmemb) : 0;
}

int64_t ObDeviceCredentialMgr::debug_callback(
    CURL *handle,
    curl_infotype type,
    char *data,
    size_t size,
    void *userp)
{
  UNUSED(handle);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(data)
      || OB_UNLIKELY(size == 0 || type < 0 || type >= curl_infotype::CURLINFO_END)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(data), K(size), K(type));
  } else {
    switch (type) {
      case CURLINFO_HEADER_OUT:
      case CURLINFO_DATA_OUT:
        LOG_WARN("Send data", K(size), K(data));
        break;
      case CURLINFO_HEADER_IN:
      case CURLINFO_DATA_IN:
        LOG_WARN("Recv data", K(size), K(data));
        break;
      case CURLINFO_TEXT: {
        int http_code = 0;
        curl_easy_getinfo(handle, CURLINFO_RESPONSE_CODE, &http_code);
        if (200 != http_code) {
          LOG_WARN("unexpected http status code", K(http_code));
        }
        break;
      }
      default:
        break;
    }
  }
  return ret;
}

int ObDeviceCredentialMgr::parse_device_credential_(
    const char *res_ptr,
    ObObjectStorageCredential &credential)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(OB_DEVICE_CREDENTIAL_ALLOCATOR);
  json::Value *root = nullptr;
  json::Parser parser;
  if (OB_ISNULL(res_ptr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("res_ptr is null", K(ret));
  } else if (OB_FAIL(parser.init(&allocator))) {
    LOG_WARN("parser init failed", K(ret));
  } else if (OB_FAIL(parser.parse(res_ptr, STRLEN(res_ptr), root))) {
    LOG_WARN("parse json failed", K(ret));
  } else if (OB_ISNULL(root)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no root value", K(ret));
  } else if (json::JT_OBJECT != root->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error json format", K(ret), K(root->get_type()));
  } else {
    ObString access_key_id;
    ObString access_key_secret;
    ObString security_token;
    int64_t expiration = 0;
    int64_t code = 0;
    ObString err_code;
    ObString success_val;
    const json::Value *response_val = nullptr;
    ObString request_id;
    DLIST_FOREACH_X(it, root->get_object(), OB_SUCC(ret)) {
      if (it->name_.case_compare("Code") == 0) {
        code = (it->value_->get_number());
      } else if (it->name_.case_compare("ErrorCode") == 0) {
        err_code = (it->value_->get_string());
      } else if (it->name_.case_compare("Success") == 0) {
        success_val = it->value_->get_string();
      } else if (it->name_.case_compare("Data") == 0) {
        response_val = it->value_;
      } else if (it->name_.case_compare("RequestId") == 0) {
        request_id = (it->value_->get_string());
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(code != 200 || !success_val.case_compare("true"))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected response", K(ret), K(code), K(err_code), K(success_val), K(request_id));
    } else if (OB_UNLIKELY(json::JT_OBJECT != response_val->get_type())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid response value", K(ret), K(response_val->get_type()));
    } else {
      DLIST_FOREACH_X(it, response_val->get_object(), OB_SUCC(ret))
      {
        for (int64_t i = 0; (i < ARRAYSIZEOF(response_items_)) && OB_SUCC(ret); ++i) {
          if (0 == it->name_.case_compare(response_items_[i])) {
            if (i != 3 && OB_UNLIKELY(json::JT_STRING != it->value_->get_type())) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("invalid response item", K(ret), K(it->name_), K(it->value_->get_type()));
            } else if (i == 3 && OB_UNLIKELY(json::JT_NUMBER != it->value_->get_type())) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("invalid response item", K(ret), K(it->name_), K(it->value_->get_type()));
            } else {
              switch (i) {
#define EXTRACT_JSON_STRING_PARAM(param_id, param) \
  case param_id:                                   \
    param = it->value_->get_string();              \
    break;
#define EXTRACT_JSON_NUMBER_PARAM(param_id, param) \
  case param_id:                                   \
    param = it->value_->get_number();              \
    break;
                EXTRACT_JSON_STRING_PARAM(ResponseItem::AccessKeyId, access_key_id);
                EXTRACT_JSON_STRING_PARAM(ResponseItem::AccessKeySecret, access_key_secret);
                EXTRACT_JSON_STRING_PARAM(ResponseItem::SecurityToken, security_token);
                EXTRACT_JSON_NUMBER_PARAM(ResponseItem::DurationSeconds, expiration);
#undef EXTRACT_JSON_STRING_PARAM
                default:
                  break;
              }
            }
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(access_key_id.empty() || access_key_secret.empty() || security_token.empty()
                      || expiration <= 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("exists empty response item", K(ret), "access_key_id", access_key_id.empty(),
            "access_key_secret", access_key_secret.empty(), "security_token",
            security_token.empty(), "expiration", expiration);
      } else if (OB_FAIL(databuff_printf(credential.access_id_, sizeof(credential.access_id_),
                     "%.*s", access_key_id.length(), access_key_id.ptr()))) {
        LOG_WARN("failed to set credential access id", K(ret), K(access_key_id));
      } else if (OB_FAIL(databuff_printf(credential.access_key_, sizeof(credential.access_key_),
                     "%.*s", access_key_secret.length(), access_key_secret.ptr()))) {
        LOG_WARN("failed to set credential access key", K(ret), K(access_key_secret.length()));
      } else if (OB_FAIL(credential.sts_token_.set(security_token))) {
        LOG_WARN("failed to set credential sts token", K(ret));
      } else {
        if (OB_UNLIKELY(expiration < CREDENTIAL_TASK_SCHEDULE_INTERVAL_US / 1000LL / 1000LL / 2)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid expiration", K(ret), K(expiration));
        } else {
          credential.expiration_s_ = expiration;
          credential.born_time_us_ = ObTimeUtility::current_time();
        }
      }
    }
  }
  return ret;
}

int ObDeviceCredentialMgr::refresh()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDeviceCredentialMgr not init", K(ret));
  } else {
    ObArray<ObDeviceCredentialKey> credential_key_to_refresh;
    ObArray<ObDeviceCredentialKey> credential_key_to_delete;
    CredentialMap::iterator iter = credential_map_.begin();
    {
      SpinWLockGuard r_guard(credential_lock_);
      while (OB_SUCC(ret) && iter != credential_map_.end()) {
        ObDeviceCredentialKey &tmp_key = iter->first;
        if (ObTimeUtility::current_time() - iter->second.access_time_us_ >= credential_duration_us_) {
          if (OB_FAIL(credential_key_to_delete.push_back(tmp_key))) {
            LOG_WARN("failed to push back into credential key array", K(ret),
                K(credential_key_to_delete), K(tmp_key), K_(credential_duration_us));
          }
        } else {
          if (OB_FAIL(credential_key_to_refresh.push_back(tmp_key))) {
            LOG_WARN("failed to push back into credential key array", K(ret),
                K(credential_key_to_refresh), K(tmp_key), K_(credential_duration_us));
          }
        }
        ++iter;
      }
    }
    //A write lock is requested in curl_credential, so the above read lock needs to be released as soon as possible
    for (int64_t i = 0; OB_SUCC(ret) && i < credential_key_to_refresh.count(); i++) {
      if (OB_FAIL(curl_credential(credential_key_to_refresh[i], false /*update_access_time*/))) {
        LOG_WARN("failed to refresh credential", K(ret), K(i), K(credential_key_to_refresh));
      }
    }
    // Minimize the scope of write locks
    SpinWLockGuard w_guard(credential_lock_);
    for (int64_t i = 0; OB_SUCC(ret) && i < credential_key_to_delete.count(); i++) {
      if (OB_FAIL(credential_map_.erase_refactored(credential_key_to_delete[i]))) {
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to delete credential", K(ret), K(i), K(credential_key_to_delete[i]));
        }
      } else {
        LOG_INFO("succeed delete expired credential", K(i), K(credential_key_to_delete[i]));
      }
    }
  }
  return ret;
}
}  // namespace common
}  // namespace oceanbase
