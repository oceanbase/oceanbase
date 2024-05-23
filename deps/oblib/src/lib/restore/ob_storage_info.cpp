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
#include "ob_storage.h"

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

//***********************ObObjectStorageInfo***************************
ObObjectStorageInfo::ObObjectStorageInfo()
  : device_type_(ObStorageType::OB_STORAGE_MAX_TYPE),
    checksum_type_(ObStorageChecksumType::OB_MD5_ALGO)
{
  endpoint_[0] = '\0';
  access_id_[0] = '\0';
  access_key_[0] = '\0';
  extension_[0] = '\0';
}

ObObjectStorageInfo::~ObObjectStorageInfo()
{
  reset();
}

void ObObjectStorageInfo::reset()
{
  device_type_ = ObStorageType::OB_STORAGE_MAX_TYPE;
  checksum_type_ = ObStorageChecksumType::OB_MD5_ALGO;
  endpoint_[0] = '\0';
  access_id_[0] = '\0';
  access_key_[0] = '\0';
  extension_[0] = '\0';
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
  return hash_value;
}

bool ObObjectStorageInfo::operator ==(const ObObjectStorageInfo &storage_info) const
{
  return device_type_ == storage_info.device_type_
      && checksum_type_ == storage_info.checksum_type_
      && (0 == STRCMP(endpoint_, storage_info.endpoint_))
      && (0 == STRCMP(access_id_, storage_info.access_id_))
      && (0 == STRCMP(access_key_, storage_info.access_key_))
      && (0 == STRCMP(extension_, storage_info.extension_));
}

bool ObObjectStorageInfo::operator !=(const ObObjectStorageInfo &storage_info) const
{
  return !(*this == storage_info);
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
    if (OB_STORAGE_FILE != device_type_) {
      ret = OB_INVALID_BACKUP_DEST;
      LOG_WARN("storage info is empty", K(ret), K_(device_type));
    }
  } else if (OB_FAIL(parse_storage_info_(storage_info, has_needed_extension))) {
    LOG_WARN("parse storage info failed", K(ret));
  } else if (OB_STORAGE_FILE != device_type
      && (0 == strlen(endpoint_) || 0 == strlen(access_id_) || 0 == strlen(access_key_))) {
    ret = OB_INVALID_BACKUP_DEST;
    LOG_WARN("backup device is not nfs, endpoint/access_id/access_key do not allow to be empty",
        K(ret), K_(device_type), K_(endpoint), K_(access_id));
  } else if (OB_STORAGE_COS == device_type && !has_needed_extension) {
    ret = OB_INVALID_BACKUP_DEST;
    LOG_WARN("invalid cos info, appid do not allow to be empty", K(ret), K_(extension));
  } else if (OB_STORAGE_FILE == device_type
      && (0 != strlen(endpoint_) || 0 != strlen(access_id_) || 0 != strlen(access_key_))) {
    ret = OB_INVALID_BACKUP_DEST;
    LOG_WARN("backup device is nfs, endpoint/access_id/access_key must be empty",
        K(ret), K_(device_type), K_(endpoint), K_(access_id));
  } else {
  }

  if (OB_FAIL(ret)) {
    reset();
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
        if (OB_FAIL(set_storage_info_field_(token, extension_, sizeof(extension_)))) {
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
      } else if (OB_STORAGE_FILE != device_type_ && 0 == strncmp(APPID, token, strlen(APPID))) {
        has_needed_extension = (OB_STORAGE_COS == device_type_);
        if (OB_FAIL(set_storage_info_field_(token, extension_, sizeof(extension_)))) {
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
      } else if (0 == strncmp(CHECKSUM_TYPE, token, strlen(CHECKSUM_TYPE))) {
        const char *checksum_type_str = token + strlen(CHECKSUM_TYPE);
        if (OB_FAIL(set_checksum_type_(checksum_type_str))) {
          OB_LOG(WARN, "fail to set checksum type", K(ret), K(checksum_type_str));
        }
      } else {
      }
    }
  }
  return ret;
}

int ObObjectStorageInfo::check_delete_mode_(const char *delete_mode) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(delete_mode)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid args", K(ret), KP(delete_mode));
  } else if (0 != strcmp(delete_mode, "delete") && 0 != strcmp(delete_mode, "tagging")) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "delete mode is invalid", K(ret), K(delete_mode));
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
      || checksum_type == ObStorageChecksumType::OB_MD5_ALGO;
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
      // cos:host=xxxx&access_id=xxx&access_key=xxxappid=xxx&delete_mode=xxx
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
  device_type_ = storage_info.device_type_;
  checksum_type_ = storage_info.checksum_type_;
  MEMCPY(endpoint_, storage_info.endpoint_, sizeof(endpoint_));
  MEMCPY(access_id_, storage_info.access_id_, sizeof(access_id_));
  MEMCPY(access_key_, storage_info.access_key_, sizeof(access_key_));
  MEMCPY(extension_, storage_info.extension_, sizeof(extension_));

  return ret;
}

int ObObjectStorageInfo::get_storage_info_str(char *storage_info, const int64_t info_len) const
{
  int ret = OB_SUCCESS;
  const int64_t key_len = MAX(OB_MAX_BACKUP_SERIALIZEKEY_LENGTH, OB_MAX_BACKUP_ACCESSKEY_LENGTH);
  char key[key_len] = { 0 };
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("storage info not init", K(ret));
  } else if (OB_ISNULL(storage_info) || (info_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(storage_info), K(info_len));
  } else if (OB_STORAGE_FILE != device_type_) {
    if (OB_FAIL(get_access_key_(key, sizeof(key)))) {
      LOG_WARN("failed to get access key", K(ret));
    } else if (OB_FAIL(databuff_printf(storage_info, info_len, "%s&%s&%s&%s%s",
                                       endpoint_, access_id_, key,
                                       CHECKSUM_TYPE, get_checksum_type_str()))) {
      LOG_WARN("failed to set storage info", K(ret), K(info_len));
    }
  }

  if (OB_SUCC(ret) && 0 != strlen(extension_) && info_len > strlen(storage_info)) {
    // if OB_STORAGE_FILE's extension is not empty, delimiter should be included
    int64_t str_len = strlen(storage_info);
    if (str_len > 0 && OB_FAIL(databuff_printf(storage_info, info_len, str_len, "&"))) {
      LOG_WARN("failed to add delimiter to storage info", K(ret), K(info_len), K(str_len));
    } else if (OB_FAIL(databuff_printf(storage_info, info_len, str_len, "%s", extension_))) {
      LOG_WARN("failed to add extension", K(ret), K(info_len), K(str_len), K_(extension));
    }
  }

  return ret;
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

}
}
