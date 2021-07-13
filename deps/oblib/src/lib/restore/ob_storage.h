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

#ifndef SRC_LIBRARY_SRC_LIB_RESTORE_OB_STORAGE_H_
#define SRC_LIBRARY_SRC_LIB_RESTORE_OB_STORAGE_H_
#include "ob_i_storage.h"
#include "ob_storage_file.h"
#ifdef _WITH_OSS
#include "ob_storage_oss_base.h"
#endif

namespace oceanbase {
namespace common {

enum ObStorageType { OB_STORAGE_OSS = 0, OB_STORAGE_FILE = 1, OB_STORAGE_COS = 2, OB_STORAGE_MAX_TYPE };

void print_access_storage_log(
    const char* msg, const common::ObString& uri, const int64_t start_ts, const int64_t size = 0, bool* is_slow = NULL);
int get_storage_type_from_path(const common::ObString& uri, ObStorageType& type);
int get_storage_type_from_name(const char* type_str, ObStorageType& type);
const char* get_storage_type_str(const ObStorageType& type);

enum ObAppendStrategy {
  // Each write is a PUT operation that will overlay the old object
  OB_APPEND_USE_SIMPLE_PUT = 0,
  // Each write will be done by the following operations:
  // 1. read the whole object
  // 2. write with previously read data as a newer object
  OB_APPEND_USE_OVERRITE = 1,
  // Append data to the tail of the object with specific offset. The write
  // will be done only if actual tail is equal to the input offset. Otherwise,
  // return failed.
  OB_APPEND_USE_APPEND = 2,
  // In this case, the object is a logical one which is actually composed of several
  // pythysical subobject. A number will be given for each write to format the name of
  // the subobject combined with the logical object name.
  OB_APPEND_USE_SLICE_PUT = 3,
  // In this case, we will use multi-part upload provided by object storage, eg S3, to write
  // for the object. Note that the object is invisible before all parts are written.
  OB_APPEND_USE_MULTI_PART_UPLOAD = 4,
  OB_APPEND_STRATEGY_TYPE
};

struct ObStorageObjectVersionParam {
  // Must be monotone increasing
  int64_t version_;
  // If true, version will be used to mark the newer object.
  bool open_object_version_;
};

class ObStorageUtil {
public:
  static const int64_t OB_AGENT_MAX_RETRY_TIME = 5 * 60 * 1000 * 1000;  // 300s
  static const int64_t OB_AGENT_SINGLE_SLEEP_US = 5 * 1000 * 1000;      // 5s
  // should not use retry during physical backup
  // When physical backup lease is timeout, retry won't stop until 300s.
  explicit ObStorageUtil(const bool need_retry, const int64_t max_retry_duraion_us = OB_AGENT_MAX_RETRY_TIME,
      const uint32_t retry_sleep_us = OB_AGENT_SINGLE_SLEEP_US);
  virtual ~ObStorageUtil()
  {}
  int is_exist(const common::ObString& uri, const common::ObString& storage_info, bool& exist);
  int get_file_length(const common::ObString& uri, const common::ObString& storage_info, int64_t& file_length);
  int del_file(const common::ObString& uri, const common::ObString& storage_info);
  int mkdir(const common::ObString& uri, const common::ObString& storage_info);
  int mk_parent_dir(const common::ObString& uri, const common::ObString& storage_info);
  // It is recommended to use this interface to write a file in the whole block of memory.
  // In the case of oss, this interface has much better performance than the writer.
  int write_single_file(
      const common::ObString& uri, const common::ObString& storage_info, const char* buf, const int64_t size);
  int read_single_file(const common::ObString& uri, const common::ObString& storage_info, char* buf,
      const int64_t buf_size, int64_t& read_size);
  // has '\0' in the end
  int read_single_text_file(
      const common::ObString& uri, const common::ObString& storage_info, char* buf, const int64_t buf_size);
  int update_file_modify_time(const common::ObString& uri, const common::ObString& storage_info);
  int list_files(const common::ObString& dir_path, const common::ObString& storage_info,
      common::ObIAllocator& allocator, common::ObIArray<common::ObString>& file_names);
  // this interface allow read part of file
  int read_part_file(const common::ObString& uri, const common::ObString& storage_info, char* buf,
      const int64_t buf_size, const int64_t offset, int64_t& read_size);
  int del_dir(const common::ObString& uri, const common::ObString& storage_info);
  int get_pkeys_from_dir(const common::ObString& uri, const common::ObString& storage_info,
      common::ObIArray<common::ObPartitionKey>& pkeys);
  // uri is directory
  int delete_tmp_files(const common::ObString& uri, const common::ObString& storage_info);

private:
  int get_util(const common::ObString& uri, ObIStorageUtil*& util);

  int do_read_single_file(const common::ObString& uri, const common::ObString& storage_info, char* buf,
      const int64_t buf_size, int64_t& read_size);
  int do_read_part_file(const common::ObString& uri, const common::ObString& storage_info, char* buf,
      const int64_t buf_size, const int64_t offset, int64_t& read_size);
  ObStorageFileUtil file_util_;
#ifdef _WITH_OSS
  ObStorageOssUtil oss_util_;
#endif
  int64_t max_retry_duraion_us_;
  uint32_t retry_sleep_us_;
  DISALLOW_COPY_AND_ASSIGN(ObStorageUtil);
};

class ObStorageReader {
public:
  ObStorageReader();
  virtual ~ObStorageReader();
  int open(const common::ObString& uri, const common::ObString& storage_info);
  int pread(char* buf, const int64_t buf_size, int64_t offset, int64_t& read_size);
  int close();
  int64_t get_length() const
  {
    return file_length_;
  }

private:
  int64_t file_length_;
  ObIStorageReader* reader_;
  ObStorageFileReader file_reader_;
#ifdef _WITH_OSS
  ObStorageOssReader oss_reader_;
#endif
  int64_t start_ts_;
  char uri_[OB_MAX_URI_LENGTH];
  DISALLOW_COPY_AND_ASSIGN(ObStorageReader);
};

class ObStorageWriter {
public:
  ObStorageWriter();
  virtual ~ObStorageWriter();
  int open(const common::ObString& uri, const common::ObString& storage_info);
  int write(const char* buf, const int64_t size);
  int close();

private:
  ObIStorageWriter* writer_;
  ObStorageFileWriter file_writer_;
#ifdef _WITH_OSS
  ObStorageOssMultiPartWriter oss_writer_;
#endif
  int64_t start_ts_;
  char uri_[OB_MAX_URI_LENGTH];
  DISALLOW_COPY_AND_ASSIGN(ObStorageWriter);
};

class ObStorageAppender {
public:
  ObStorageAppender(StorageOpenMode mode);
  ObStorageAppender();
  virtual ~ObStorageAppender();

  struct AppenderParam {
    ObAppendStrategy strategy_;
    ObStorageObjectVersionParam version_param_;
  };

  int open(const common::ObString& uri, const common::ObString& storage_info, const AppenderParam& param);

  // TODO: out of date interface, to be deprecated.
  int open_deprecated(const common::ObString& uri, const common::ObString& storage_info);
  int write(const char* buf, const int64_t size);
  int close();
  bool is_opened() const
  {
    return is_opened_;
  }
  int64_t get_length();
  TO_STRING_KV(KP(appender_), K_(start_ts), K_(is_opened), K_(uri));

private:
  ObIStorageWriter* appender_;
  ObStorageFileAppender file_appender_;
#ifdef _WITH_OSS
  ObStorageOssAppendWriter oss_appender_;
#endif
  int64_t start_ts_;
  bool is_opened_;
  char uri_[OB_MAX_URI_LENGTH];
  DISALLOW_COPY_AND_ASSIGN(ObStorageAppender);
};

class ObStorageMetaWrapper {
public:
  ObStorageMetaWrapper();
  virtual ~ObStorageMetaWrapper();
  int get(const common::ObString& uri, const common::ObString& storage_info, char* buf, const int64_t buf_size,
      int64_t& read_size);
  int set(const common::ObString& uri, const common::ObString& storage_info, const char* buf, const int64_t size);

private:
  int get_meta(const common::ObString& uri, ObIStorageMetaWrapper*& meta);
  ObStorageFileMetaWrapper file_meta_;
#ifdef _WITH_OSS
  ObStorageOssMetaMgr oss_meta_;
#endif
  DISALLOW_COPY_AND_ASSIGN(ObStorageMetaWrapper);
};

}  // namespace common
}  // namespace oceanbase

#endif /* SRC_LIBRARY_SRC_LIB_RESTORE_OB_STORAGE_H_ */
