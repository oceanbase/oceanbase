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
#ifndef TEST_STORAGE_COMMON_STORAGE_H_
#define TEST_STORAGE_COMMON_STORAGE_H_

#include "lib/utility/ob_print_utils.h"
#include "lib/hash/ob_hashmap.h"
#include "gtest/gtest.h"

/**
 * USER GUIDE
 *
 * This test case is for testing different storage media type, like oss, s3, fs, etc.
 * It will check reader/writer/util/appender/multipartupload primary interfaces correctness.
 * Besides, it will generate some abnormal situations to check the relative function correctness.
 *
 * If you want to run this test case, you just need to execute some simple steps.
 *
 * 1. For object storage, for example, s3, you just need to revise S3_BUCKET、S3_REGION、S3_ENDPOINT、S3_AK、
 *    S3_SK as correct info, so as to oss、cos
 *
 * 2. For NFS, you just need to revise FS_PATH as empty string(means "") or some other value, but not INVALID_STR. It will
 *    build a directory in the same directory as this test bin file.
 *
 * NOTICE: consider the running time, you'd better check these media one by one, or it may be timeout.
 */

namespace oceanbase{
const char *INVALID_STR = "xxx";

// S3 CONFIG
const char *S3_BUCKET = INVALID_STR;
const char *S3_REGION = INVALID_STR;
const char *S3_ENDPOINT = INVALID_STR;
const char *S3_AK = INVALID_STR;
const char *S3_SK = INVALID_STR;
const char *S3_CHECKSUM_TYPE = INVALID_STR;

// OSS CONFIG
const char *OSS_BUCKET = INVALID_STR;
const char *OSS_ENDPOINT = INVALID_STR;
const char *OSS_AK = INVALID_STR;
const char *OSS_SK = INVALID_STR;
const char *OSS_CHECKSUM_TYPE = INVALID_STR;

// COS CONFIG
const char *COS_BUCKET = INVALID_STR;
const char *COS_ENDPOINT = INVALID_STR;
const char *COS_AK = INVALID_STR;
const char *COS_SK = INVALID_STR;
const char *COS_APPID = INVALID_STR;
const char *COS_CHECKSUM_TYPE = INVALID_STR;

// NFS CONFIG
const char *FS_PATH = INVALID_STR; // if FS_PATH value not equals to invalid string, we will use current path as FS_PATH

enum class ObTestStorageType : uint8_t
{
  TEST_STORAGE_INVALID = 0,
  TEST_STORAGE_OSS = 1,
  TEST_STORAGE_S3 = 2,
  TEST_STORAGE_COS = 3,
  TEST_STORAGE_FS = 4,
  TEST_STORAGE_MAX = 5
};

const char test_storage_type_str_arr[5][8] = {"INVALID", "OSS", "S3", "COS", "NFS"};

struct ObTestStorageInfoConfig
{
public:
  const static int64_t CFG_BUF_LEN = 1024;

  char bucket_[CFG_BUF_LEN];
  char region_[CFG_BUF_LEN];
  char endpoint_[CFG_BUF_LEN];
  char ak_[CFG_BUF_LEN];
  char sk_[CFG_BUF_LEN];
  char checksum_type_[CFG_BUF_LEN];
  union {
    char appid_[CFG_BUF_LEN];
    char fs_path_[CFG_BUF_LEN];
  };

  bool is_valid_;

  ObTestStorageInfoConfig() : is_valid_(false) {}
  ~ObTestStorageInfoConfig() {}

  #define SET_FIELD(field_name) \
    void set_##field_name(const char *value) \
    {  \
      if (nullptr != value) { \
        const int64_t val_len = strlen(value); \
        MEMCPY(field_name##_, value, val_len); \
        field_name##_[val_len] = '\0'; \
      } \
    } \

  SET_FIELD(bucket);
  SET_FIELD(region);
  SET_FIELD(endpoint);
  SET_FIELD(ak);
  SET_FIELD(sk);
  SET_FIELD(appid);
  SET_FIELD(fs_path);
  SET_FIELD(checksum_type);

  bool is_valid() const { return is_valid_; }

  TO_STRING_KV(K_(bucket),
      K_(region), K_(endpoint), K_(ak), K_(sk), K_(appid), K_(fs_path), K_(checksum_type));
};

struct ObTestStorageMeta
{
public:
  ObTestStorageType type_;
  ObTestStorageInfoConfig config_;

  ObTestStorageMeta() {}

  ~ObTestStorageMeta() {}

  int build_config(const ObTestStorageType type);
  bool is_valid() const;

  bool is_file_type() const { return type_ == ObTestStorageType::TEST_STORAGE_FS; }
  bool is_obj_type() const { return type_ == ObTestStorageType::TEST_STORAGE_S3 ||
                                    type_ == ObTestStorageType::TEST_STORAGE_COS ||
                                    type_ == ObTestStorageType::TEST_STORAGE_OSS;}

  TO_STRING_KV(K_(type), K_(config));

private:
  bool is_valid_type(const ObTestStorageType type) const;
  void build_s3_cfg();
  void build_oss_cfg();
  void build_cos_cfg();
  void build_fs_cfg();

};

} // end of oceanbase

#endif