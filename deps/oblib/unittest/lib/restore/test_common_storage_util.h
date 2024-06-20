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

#ifndef TEST_STORAGE_COMMON_STORAGE_UTIL_H_
#define TEST_STORAGE_COMMON_STORAGE_UTIL_H_

#include "lib/restore/ob_storage.h"

namespace oceanbase {

using namespace oceanbase::common;

class TestCommonStorageUtil
{
public:
  static int build_object_storage_info(const char *bucket, const char *endpoint,
    const char *ak, const char *sk, const char *region, const char *appid,
    const char *checksum_type, ObObjectStorageInfo &storage_info);

  static int build_fs_storage_info(ObObjectStorageInfo &storage_info);


  // The format of obj_uri will have two types:
  //   1. media_type://bucket/raw_dir_path/obj_name
  //   2. media_type://bucket/raw_dir_path/
  static int gen_object_uri(char *obj_uri, const int64_t uri_buf_len, const char *bucket, const char *raw_dir_path, const char *obj_name);

  // The format of fs_uri will have two types:
  //   1. media_type://pwd_path/raw_dir_path/file_name
  //   2. media_type://pwd_path/raw_dir_path/
  static int gen_fs_uri(char *fs_uri, const int64_t uri_buf_len, const char *pwd_path, const char *raw_dir_path, const char *file_name);
};

int TestCommonStorageUtil::build_object_storage_info(
    const char *bucket,
    const char *endpoint,
    const char *ak,
    const char *sk,
    const char *region,
    const char *appid,
    const char *checksum_type,
    ObObjectStorageInfo &storage_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(bucket) || OB_ISNULL(endpoint) ||
      OB_ISNULL(ak) || OB_ISNULL(sk)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), KP(bucket), KP(region), KP(endpoint), KP(ak), KP(sk));
  } else {
    char account[OB_MAX_URI_LENGTH] = { 0 };
    ObStorageType storage_type = ObStorageType::OB_STORAGE_MAX_TYPE;
    if (OB_FAIL(get_storage_type_from_path(bucket, storage_type))) {
      OB_LOG(WARN, "fail to get storage type from path", K(ret), K(bucket));
    } else if (ObStorageType::OB_STORAGE_FILE == storage_type) {
      if (OB_FAIL(storage_info.set(storage_type, account))) {
        OB_LOG(WARN, "fail to set storage info", K(ret));
      }
    } else {
      int64_t pos = 0;
      if (OB_FAIL(databuff_printf(account, sizeof(account), pos,
          "host=%s&access_id=%s&access_key=%s", endpoint, ak, sk))) {
        OB_LOG(WARN, "fail to databuff printf", K(ret), K(endpoint), K(ak), K(sk));
      } else if (OB_NOT_NULL(checksum_type) && strlen(checksum_type) > 0
          && OB_FAIL(databuff_printf(account, sizeof(account), pos,
                                     "&checksum_type=%s", checksum_type))) {
        OB_LOG(WARN, "fail to databuff printf", K(ret), K(checksum_type));
      } else if (ObStorageType::OB_STORAGE_COS == storage_type &&
                 databuff_printf(account, sizeof(account), pos, "&appid=%s", appid)) {
        OB_LOG(WARN, "fail to databuff printf", K(ret), K(appid));
      } else if (ObStorageType::OB_STORAGE_S3 == storage_type &&
                 databuff_printf(account, sizeof(account), pos, "&s3_region=%s", region)) {
        OB_LOG(WARN, "fail to databuff printf", K(ret), K(region));
      }

      if (FAILEDx(storage_info.set(storage_type, account))) {
        OB_LOG(WARN, "fail to set storage info", K(ret), K(storage_type), K(account));
      }
    }
  }
  return ret;
}

int TestCommonStorageUtil::build_fs_storage_info(
    ObObjectStorageInfo &storage_info)
{
  int ret = OB_SUCCESS;
  char account[OB_MAX_URI_LENGTH] = { 0 };
  if (OB_FAIL(storage_info.set(ObStorageType::OB_STORAGE_FILE, account))) {
    OB_LOG(WARN, "fail to set storage info", K(ret));
  }
  return ret;
}

int TestCommonStorageUtil::gen_object_uri(
    char *obj_uri,
    const int64_t uri_buf_len,
    const char *bucket,
    const char *raw_dir_path,
    const char *obj_name)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  const bool exist_obj = (obj_name != nullptr);
  if (OB_ISNULL(obj_uri) || OB_ISNULL(bucket) || OB_ISNULL(raw_dir_path)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), KP(obj_uri), KP(bucket), KP(raw_dir_path));
  } else if (exist_obj && OB_FAIL(databuff_printf(obj_uri, uri_buf_len, pos, "%s/%s/%s",
             bucket, raw_dir_path, obj_name))) {
    OB_LOG(WARN, "fail to databuff printf", K(ret), K(bucket), K(raw_dir_path), K(obj_name), K(uri_buf_len));
  } else if (!exist_obj && OB_FAIL(databuff_printf(obj_uri, uri_buf_len, pos, "%s/%s/", bucket, raw_dir_path))) {
    OB_LOG(WARN, "fail to databuff printf", K(ret), K(bucket), K(raw_dir_path), K(uri_buf_len));
  }
  return ret;
}

int TestCommonStorageUtil::gen_fs_uri(
    char *fs_uri,
    const int64_t uri_buf_len,
    const char *pwd_path,
    const char *raw_dir_path,
    const char *file_name)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  const bool exist_file = (file_name != nullptr);
  if (OB_ISNULL(fs_uri) || OB_ISNULL(pwd_path) || OB_ISNULL(raw_dir_path)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), KP(fs_uri), KP(pwd_path), KP(raw_dir_path));
  } else if (exist_file && OB_FAIL(databuff_printf(fs_uri, uri_buf_len, pos, "%s%s/%s/%s", "file://",
             pwd_path, raw_dir_path, file_name))) {
    OB_LOG(WARN, "fail to databuff printf", K(ret), K(pwd_path), K(raw_dir_path), K(file_name), K(uri_buf_len));
  } else if (!exist_file && OB_FAIL(databuff_printf(fs_uri, uri_buf_len, pos, "%s%s/%s/", "file://",
             pwd_path, raw_dir_path))) {
    OB_LOG(WARN, "fail to databuff printf", K(ret), K(pwd_path), K(raw_dir_path), K(uri_buf_len));
  }
  return ret;
}

}

#endif