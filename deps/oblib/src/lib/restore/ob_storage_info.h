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

#ifndef OCEANBASE_LIB_RESTORE_OB_STORAGE_INFO_H_
#define OCEANBASE_LIB_RESTORE_OB_STORAGE_INFO_H_

#include "common/storage/ob_device_common.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/ob_unify_serialize.h"

namespace oceanbase
{

namespace common
{

const int64_t OB_MAX_BACKUP_EXTENSION_LENGTH = 512;
const int64_t OB_MAX_BACKUP_ENDPOINT_LENGTH = 256;
const int64_t OB_MAX_BACKUP_ACCESSID_LENGTH = 256;
const int64_t OB_MAX_BACKUP_ACCESSKEY_LENGTH = 256;
const int64_t OB_MAX_BACKUP_STORAGE_INFO_LENGTH = 1536;
const int64_t OB_MAX_BACKUP_ENCRYPTKEY_LENGTH = OB_MAX_BACKUP_ACCESSKEY_LENGTH + 32;
const int64_t OB_MAX_BACKUP_SERIALIZEKEY_LENGTH = OB_MAX_BACKUP_ENCRYPTKEY_LENGTH * 2;

const char *const ACCESS_ID = "access_id=";
const char *const ACCESS_KEY = "access_key=";
const char *const HOST = "host=";
const char *const APPID = "appid=";
const char *const DELETE_MODE = "delete_mode=";
const char *const REGION = "s3_region=";

const char *const CHECKSUM_TYPE = "checksum_type=";
const char *const CHECKSUM_TYPE_NO_CHECKSUM = "no_checksum";
const char *const CHECKSUM_TYPE_MD5 = "md5";
const char *const CHECKSUM_TYPE_CRC32 = "crc32";

enum ObStorageChecksumType
{
  OB_NO_CHECKSUM_ALGO = 0,
  OB_MD5_ALGO = 1,
  OB_CRC32_ALGO = 2,
  OB_STORAGE_CHECKSUM_MAX_TYPE
};

bool is_oss_supported_checksum(const ObStorageChecksumType checksum_type);
bool is_cos_supported_checksum(const ObStorageChecksumType checksum_type);
bool is_s3_supported_checksum(const ObStorageChecksumType checksum_type);
const char *get_storage_checksum_type_str(const ObStorageChecksumType &type);

// [Extensions]
//   load_data_* : sql/engine/cmd/ob_load_data_storage_info.h

class ObObjectStorageInfo
{
  OB_UNIS_VERSION(1);
public:
  ObObjectStorageInfo();
  virtual ~ObObjectStorageInfo();

  virtual int set(const common::ObStorageType device_type, const char *storage_info);
  virtual int set(const char *uri, const char *storage_info);
  virtual int assign(const ObObjectStorageInfo &storage_info);
  ObStorageType get_type() const;
  const char *get_type_str() const;
  ObStorageChecksumType get_checksum_type() const;
  const char *get_checksum_type_str() const;
  virtual int get_storage_info_str(char *storage_info, const int64_t info_len) const;

  virtual bool is_valid() const;
  virtual void reset();
  int64_t hash() const;
  bool operator ==(const ObObjectStorageInfo &storage_info) const;
  bool operator !=(const ObObjectStorageInfo &storage_info) const;
  TO_STRING_KV(K_(endpoint), K_(access_id), K_(extension),
               "type", get_type_str(), K_(checksum_type));

protected:
  virtual int get_access_key_(char *key_buf, const int64_t key_buf_len) const;
  virtual int parse_storage_info_(const char *storage_info, bool &has_appid);
  int check_delete_mode_(const char *delete_mode) const;
  int set_checksum_type_(const char *checksum_type_str);
  int set_storage_info_field_(const char *info, char *field, const int64_t length);

public:
  // TODO: Rename device_type_ to storage_protocol_type_ for better clarity
  // Prefix in the storage_info string, such as 's3://', indicates the protocol used to access the target
  // Currently, both OBS and GCS are accessed via the s3 protocol,
  // hence s3_region is updated to be an optional parameter
  common::ObStorageType device_type_;
  // Optional parameter. If not provided, the default value OB_MD5_ALGO will be used.
  // For OSS/COS, OB_NO_CHECKSUM_ALGO indicates that no checksum algorithm will be used.
  // For Object Storage Services accessed via the S3 protocol,
  // OB_NO_CHECKSUM_ALGO is not supported.
  ObStorageChecksumType checksum_type_;
  char endpoint_[OB_MAX_BACKUP_ENDPOINT_LENGTH];
  char access_id_[OB_MAX_BACKUP_ACCESSID_LENGTH];
  char access_key_[OB_MAX_BACKUP_ACCESSKEY_LENGTH];
  char extension_[OB_MAX_BACKUP_EXTENSION_LENGTH];
};

}
}

#endif
