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
#include "ob_storage_oss_base.h"
#include "ob_storage_cos_base.h"

namespace oceanbase
{
namespace common
{

void print_access_storage_log(const char *msg, const common::ObString &uri,
    const int64_t start_ts, const int64_t size = 0, bool *is_slow = NULL);
int get_storage_type_from_path(const common::ObString &uri, ObStorageType &type);
int validate_uri_type(const common::ObString &uri);
int get_storage_type_from_name(const char *type_str, ObStorageType &type);
const char *get_storage_type_str(const ObStorageType &type);
bool is_io_error(const int result);


class ObExternalIOCounter final
{
public:
  void inc_flying_io();

  void dec_flying_io();

  static int64_t get_flying_io_cnt();

  // 'FLYING_IO_WAIT_TIMEOUT' indicates the threshold we have to wait before all flying io back.
  static const int64_t FLYING_IO_WAIT_TIMEOUT = 120000000; // default 120s.

private:
  static int64_t flying_io_cnt_; // oss doing io counter.
};

class ObExternalIOCounterGuard final
{
public:
  ObExternalIOCounterGuard();

  ~ObExternalIOCounterGuard();

private:
  ObExternalIOCounter io_counter_;
};


class ObStorageGlobalIns
{
public:
  ObStorageGlobalIns();

  // not thread safe
  static ObStorageGlobalIns& get_instance();

  int init();
  
  void fin();
  // When the observer is in not in white list, no matter read or write io is not allowed.
  void set_io_prohibited(bool prohibited);

  bool is_io_prohibited() const;

private:
  bool io_prohibited_;
};

enum ObAppendStrategy 
{
  // Each write will be done by the following operations:
  // 1. read the whole object
  // 2. write with previously read data as a newer object
  OB_APPEND_USE_OVERRITE = 0,
  // Append data to the tail of the object with specific offset. The write 
  // will be done only if actual tail is equal to the input offset. Otherwise,
  // return failed.
  OB_APPEND_USE_APPEND = 1,
  // In this case, the object is a logical one which is actually composed of several 
  // pythysical subobject. A number will be given for each write to format the name of
  // the subobject combined with the logical object name.
  OB_APPEND_USE_SLICE_PUT = 2,
  // In this case, we will use multi-part upload provided by object storage, eg S3, to write
  // for the object. Note that the object is invisible before all parts are written.
  OB_APPEND_USE_MULTI_PART_UPLOAD = 3,
  OB_APPEND_STRATEGY_TYPE
};

struct ObStorageObjectVersionParam {
  // Must be monotone increasing
  int64_t version_;
  // If true, version will be used to mark the newer object.
  bool open_object_version_;
};

class ObStorageUtil
{
public:
  // should not use retry during physical backup
  // When physical backup lease is timeout, retry won't stop until 300s.
  explicit ObStorageUtil();
  virtual ~ObStorageUtil() {}
  int open(common::ObObjectStorageInfo *storage_info);
  void close();
  int is_exist(const common::ObString &uri, bool &exist);
  int get_file_length(const common::ObString &uri, int64_t &file_length);
  int del_file(const common::ObString &uri);
  int mkdir(const common::ObString &uri);
  int write_single_file(const common::ObString &uri, const char *buf, const int64_t size);
  int list_files(const common::ObString &dir_path, common::ObBaseDirEntryOperator &op);
  int del_dir(const common::ObString &uri);
  int list_directories(const common::ObString &dir_path, common::ObBaseDirEntryOperator &op);
  int is_tagging(const common::ObString &uri, bool &is_tagging);

private:
  // we does not use storage_info_ to judge init, since for nfs&local, storage_info_ is null
  bool is_init() {return init_state;}

  ObStorageFileUtil file_util_;
  ObStorageOssUtil oss_util_;
  ObStorageCosUtil cos_util_;
  ObIStorageUtil* util_;
  common::ObObjectStorageInfo* storage_info_;
  bool init_state;
  DISALLOW_COPY_AND_ASSIGN(ObStorageUtil);
};

class ObStorageReader
{
public:
  ObStorageReader();
  virtual ~ObStorageReader();
  int open(const common::ObString &uri, common::ObObjectStorageInfo *storage_info);
  int pread(char *buf,const int64_t buf_size, int64_t offset, int64_t &read_size);
  int close();
  int64_t get_length() const { return file_length_; }
private:
  int64_t file_length_;
  ObIStorageReader *reader_;
  ObStorageFileReader file_reader_;
  ObStorageOssReader oss_reader_;
  ObStorageCosReader cos_reader_;
  int64_t start_ts_;
  char uri_[OB_MAX_URI_LENGTH];
  DISALLOW_COPY_AND_ASSIGN(ObStorageReader);
};

class ObStorageWriter
{
public:
  ObStorageWriter();
  virtual ~ObStorageWriter();
  int open(const common::ObString &uri, common::ObObjectStorageInfo *storage_info);
  int write(const char *buf,const int64_t size);
  int close();
private:
  ObIStorageWriter *writer_;
  ObStorageFileWriter file_writer_;
  ObStorageOssWriter oss_writer_;
  ObStorageCosWriter cos_writer_;
  int64_t start_ts_;
  char uri_[OB_MAX_URI_LENGTH];
  DISALLOW_COPY_AND_ASSIGN(ObStorageWriter);
};

class ObStorageAppender
{
public:
  ObStorageAppender(StorageOpenMode mode);
  ObStorageAppender();
  virtual ~ObStorageAppender();

  struct AppenderParam 
  {
    ObAppendStrategy strategy_;
    ObStorageObjectVersionParam version_param_;
  };

  int open(const common::ObString &uri, common::ObObjectStorageInfo *storage_info);
  int write(const char *buf,const int64_t size);
  int pwrite(const char *buf, const int64_t size, const int64_t offset);
  int close();
  bool is_opened() const { return is_opened_; }
  int64_t get_length();
  void set_open_mode(StorageOpenMode mode) {file_appender_.set_open_mode(mode);}
  TO_STRING_KV(KP(appender_), K_(start_ts), K_(is_opened),KCSTRING_(uri));
private:
  ObIStorageWriter *appender_;
  ObStorageFileAppender file_appender_;
  ObStorageOssAppendWriter oss_appender_;
  ObStorageCosAppendWriter cos_appender_;
  int64_t start_ts_;
  bool is_opened_;
  char uri_[OB_MAX_URI_LENGTH];
  common::ObObjectStorageInfo storage_info_;
  ObArenaAllocator allocator_;

  int repeatable_pwrite_(const char *buf, const int64_t size, const int64_t offset);
  DISALLOW_COPY_AND_ASSIGN(ObStorageAppender);
};

}//common
}//oceanbase

#endif /* SRC_LIBRARY_SRC_LIB_RESTORE_OB_STORAGE_H_ */
