/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#define USING_LOG_PREFIX SQL_ENG

#include "ob_hdfs_storage_info.h"

namespace oceanbase
{
namespace share
{

//***********************ObHDFSStorageInfo***************************
ObHDFSStorageInfo::~ObHDFSStorageInfo()
{
  reset();
}

bool ObHDFSStorageInfo::operator ==(const ObHDFSStorageInfo &external_storage_info) const
{
  return (0 == STRCMP(hdfs_extension_, external_storage_info.hdfs_extension_));
}

bool ObHDFSStorageInfo::operator !=(const ObHDFSStorageInfo &external_storage_info) const
{
  return !(*this == external_storage_info);
}

int ObHDFSStorageInfo::assign(const ObObjectStorageInfo &storage_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!storage_info.is_hdfs_storage())) {
    ret = OB_INVALID_BACKUP_DEST;
    LOG_WARN("only external storage info can assign", K(ret));
  } else {
    const ObHDFSStorageInfo *external_storage_info =
        static_cast<const ObHDFSStorageInfo *>(&storage_info);
    // Note: currently only support hdfs storage type to assign in ObHDFSStorageInfo
    device_type_ = storage_info.device_type_;
    MEMCPY(hdfs_extension_, external_storage_info->hdfs_extension_,
           sizeof(hdfs_extension_));
  }
  LOG_TRACE("external storage assign info", K(ret), K(device_type_),
           K(storage_info.device_type_), K(hdfs_extension_));
  return ret;
}

void ObHDFSStorageInfo::reset()
{
  common::ObObjectStorageInfo::reset();
  hdfs_extension_[0] = '\0';
}

int64_t ObHDFSStorageInfo::hash() const
{
  int64_t hash_value = 0;
  hash_value = murmurhash(hdfs_extension_, static_cast<int32_t>(strlen(hdfs_extension_)), hash_value);
  return hash_value;
}

int ObHDFSStorageInfo::validate_arguments() const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), K(device_type_));
  } else if (OB_UNLIKELY(!is_hdfs_storage())) {
    ret = OB_INVALID_BACKUP_DEST;
    OB_LOG(WARN, "invalid device to parse object storage info", K(ret), K(device_type_));
  } else {
    if (OB_UNLIKELY(0 != strlen(endpoint_) || 0 != strlen(access_id_) ||
                    0 != strlen(access_key_) || 0 != strlen(role_arn_) ||
                    0 != strlen(external_id_) || 0 != strlen(extension_))) {
      ret = OB_INVALID_BACKUP_DEST;
      LOG_WARN("backup device is hdfs, endpoint/access_id/access_key/extension_ must be empty", K(ret));
    }
  }
  return ret;
}

int ObHDFSStorageInfo::get_device_map_key_str(char *key_str, const int64_t len) const
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage info not init", K(ret));
  } else if (OB_ISNULL(key_str) || (len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(key_str), K(len));
  } else if (OB_UNLIKELY(!is_hdfs_storage())) {
    ret = OB_INVALID_BACKUP_DEST;
    LOG_WARN("only hdfs storage support to get device map key by external storage info",
             K(ret), K_(device_type));
  } else {
    // handle with hdfs
    int64_t hdfs_extension_hash = murmurhash(
        hdfs_extension_, static_cast<int32_t>(strlen(hdfs_extension_)), 0);
    if (OB_FAIL(databuff_printf(key_str, len, "%u&%ld",
                                static_cast<uint32_t>(device_type_),
                                hdfs_extension_hash))) {
      LOG_WARN("failed to set key str for hdfs", K(ret), K(len), KPC(this));
    }
  }
  return ret;
}

int64_t ObHDFSStorageInfo::get_device_map_key_len() const
{
  int64_t len = 0;
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_hdfs_storage())) {
    ret = OB_INVALID_BACKUP_DEST;
    LOG_WARN("only hdfs storage support to get device map key len by external storage info",
             K(ret), K_(device_type));
  } else {
    // Hdfs storage info will store in hdfs_extension_, and it maybe a long string, so hash it as key.
    // 8 is sizeof(int64_t) which is the size of murmurhash value.
    len = 30 + 8;
  }
  return len;
}

int ObHDFSStorageInfo::parse_storage_info_(const char *storage_info, bool &has_needed_extension)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(storage_info) || strlen(storage_info) >= OB_MAX_BACKUP_STORAGE_INFO_LENGTH) {
    ret = OB_INVALID_BACKUP_DEST;
    LOG_WARN("storage info is invalid", K(ret), KP(storage_info), K(strlen(storage_info)));
  } else if (OB_UNLIKELY(!is_hdfs_storage())) {
    ret = OB_INVALID_BACKUP_DEST;
    LOG_WARN("only hdfs storage support to get device map key len by external storage info",
             K(ret), K_(device_type));
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
      } else if (0 == strncmp(HADOOP_USERNAME, token, strlen(HADOOP_USERNAME))) {
        if (ObStorageType::OB_STORAGE_HDFS != device_type_) {
          ret = OB_INVALID_BACKUP_DEST;
          LOG_WARN("device don't support HADOOP_USERNAME yet", K(ret), K_(device_type), KP(token));
        } else if (OB_FAIL(
                       set_storage_info_field_(token, hdfs_extension_, sizeof(hdfs_extension_)))) {
          LOG_WARN("failed to set hadoop username", K(ret), KP(token));
        }
      }
    }
  }
  return ret;
}

int ObHDFSStorageInfo::get_info_str_(char *storage_info, const int64_t info_len) const
{
  int ret = OB_SUCCESS;
  const int64_t key_len = OB_MAX_HDFS_BACKUP_EXTENSION_LENGTH;
  char key[key_len] = {0};
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("storage info is invalid", K(ret), K_(device_type));
  } else if (OB_ISNULL(storage_info) || OB_UNLIKELY(info_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(storage_info), K(info_len));
  } else if (OB_UNLIKELY(!is_hdfs_storage())) {
    ret = OB_INVALID_BACKUP_DEST;
    LOG_WARN("invalid device type to get info str", K(ret), K_(device_type),
             KP(storage_info), K(info_len));
  } else {
    // Setup storage info for simple auth on hdfs, more related auth configs will get in 
    // append_extension_str_.
    if (OB_ISNULL(hdfs_extension_) || 0 == strlen(hdfs_extension_)) {
      LOG_TRACE("access hdfs with simple auth and hdfs extension is empty", K(ret));
    }
  }
  return ret;
}

int ObHDFSStorageInfo::append_extension_str_(char *storage_info, const int64_t info_len) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("storage info is invalid", K(ret), K_(device_type));
  } else if (OB_ISNULL(storage_info) || OB_UNLIKELY(info_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(storage_info), K(info_len));
  } else if (OB_UNLIKELY(!is_hdfs_storage())) {
    ret = OB_INVALID_BACKUP_DEST;
    LOG_WARN("invalid device type to append extension str", K(ret), K_(device_type),
             KP(storage_info), K(info_len));
  } else if (0 != strlen(hdfs_extension_) && info_len > strlen(storage_info)) {
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

} // namespace sql
} // namespace oceanbase