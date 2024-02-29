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
#include "ob_storage_s3_base.h"

namespace oceanbase
{
namespace common
{
/* In order to uniform naming format, here we will define the name format about uri/path.
 *   a. 'uri' represents a full path which has type prefix, like OSS/FILE.
 *   b. 'raw_dir_path' represents a dir path which does not have suffix '/'
 *   c. 'dir_path' represents a dir path, but we can't ensure that this path has suffix '/' or not
 *   d. 'full_dir_path' represents a dir path which has suffix '/'
 *   e. 'dir_name' represents a directory name, not a path
 *   f. 'obj_path' represents a object/file path
 *   g. 'obj_name' represents a object/file name, not a path
 */

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

// If the object is 'SIMULATE_APPEND' type, we will use this operation to list all of its children objects.
class ListAppendableObjectFragmentOp : public common::ObBaseDirEntryOperator
{
public:
  ListAppendableObjectFragmentOp(const bool need_size = true)
    : exist_format_meta_(false), exist_seal_meta_(false), meta_arr_(), need_size_(need_size) {}

  virtual ~ListAppendableObjectFragmentOp() { meta_arr_.reset(); }
  virtual int func(const dirent *entry) override;
  virtual bool need_get_file_size() const { return need_size_; }
  int gen_object_meta(ObStorageObjectMeta &obj_meta);

  bool exist_format_meta() const { return exist_format_meta_; }
  bool exist_seal_meta() const { return exist_seal_meta_; }

private:
  bool exist_format_meta_;
  bool exist_seal_meta_;
  ObArray<ObAppendableFragmentMeta> meta_arr_; // save all 'data fragment meta'
  bool need_size_;
};

// If the object is 'SIMULATE_APPEND' type, we will use this operation to delete all of its children objects.
class ObStorageUtil;
class DelAppendableObjectFragmentOp : public ObBaseDirEntryOperator
{
public:
  DelAppendableObjectFragmentOp(const common::ObString &uri, ObStorageUtil &util);
  virtual ~DelAppendableObjectFragmentOp() {}
  virtual int func(const dirent *entry) override;

private:
  const common::ObString &uri_;
  ObStorageUtil &util_;
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

  ////////////////////// READY //// TO //// DROP ///// BELOW ////////////////////////////////
  int is_exist(const common::ObString &uri, bool &exist);
  int get_file_length(const common::ObString &uri, int64_t &file_length);
  int del_file(const common::ObString &uri);
  int list_files(const common::ObString &dir_path, common::ObBaseDirEntryOperator &op);
  int list_directories(const common::ObString &dir_path, common::ObBaseDirEntryOperator &op);
  ////////////////////// READY //// TO //// DROP ///// ABOVE ////////////////////////////////

  int mkdir(const common::ObString &uri);
  int write_single_file(const common::ObString &uri, const char *buf, const int64_t size);
  int del_dir(const common::ObString &uri);
  int is_tagging(const common::ObString &uri, bool &is_tagging);
  // This func is to check the object/file/dir exists or not.
  // If the uri is a common directory(not a 'SIMULATE_APPEND' object), please set @is_adaptive as FALSE
  // If the uri is a normal object, please set @is_adaptive as FALSE
  // If the uri is a 'SIMULATE_APPEND' object or we can't ensure that it is a normal object or a
  // 'SIMULATE_APPEND' object, please set @is_adaptive as TRUE.
  int is_exist(const common::ObString &uri, const bool is_adaptive, bool &exist);
  int get_file_length(const common::ObString &uri, const bool is_adaptive, int64_t &file_length);
  int list_appendable_file_fragments(const common::ObString &uri, ObStorageObjectMeta &obj_meta);

  int del_file(const common::ObString &uri, const bool is_adaptive);
  int del_unmerged_parts(const common::ObString &uri);

  // For one object, if given us the uri(no matter in oss, cos or s3), we can't tell the type of this object.
  // It may be a 'single、normal' object. Or it may be a 's3-appendable-object'(like a dir), containing several
  // 'single、normal' objects.
  // So, this function is for checking the object meta, to get its meta info
  //
  // @uri, the object full path in object storage.
  // @is_adaptive, if FALSE, means it is a normal object absolutely.
  //               if TRUE, means we don't know it type. We need to check its real type.
  // @need_fragment_meta, if TRUE and the type is a 's3-appendable-object', we need to get its child objects meta.
  //                      for example, when using adaptive reader, this param will set as TRUE; when using is_exist(),
  //                      this param will set as FALSE
  // @obj_meta the result, which saves the meta info of this object. If the target object not exists, we can check
  //           obj_meta.is_exist_, not return OB_BACKUP_FILE_NOT_EXIST.
  int detect_storage_obj_meta(const common::ObString &uri, const bool is_adaptive,
                              const bool need_fragment_meta, ObStorageObjectMeta &obj_meta);

  // Due to the 'SIMULATE_APPEND' object and 'NORMAL' object may exist together, thus we can't simply list all objects
  // based on the prefix.
  //
  // For example,
  //       dir1
  //         --file1
  //         --file2
  //         --dir11
  //            --file11
  //            --file12
  //            --appendable11
  //               --@FORMAT_META
  //         --appendable1
  //            --@FORMAT_META
  //            --@0-100
  //
  // ['appendable1' and 'appendable11' are 'SIMULATE_APPEND' type]. If we want to list 'dir1/', we supposed to get the result as flows:
  //  dir1/file1, dir1/file2
  //  dir1/dir11/file11, dir1/dir11/file12, dir1/dir11/appendable11
  //  dir1/appendable1
  //  Above 6 object paths are the final result.
  //
  // @is_adaptive  If we can ensure that there not exist 'SIMULATE_APPEND' type object in @uri, we can set this param
  //               as FALSE, otherwise set it as TRUE.
  int list_files(const common::ObString &uri, const bool is_adaptive, common::ObBaseDirEntryOperator &op);
  int list_directories(const common::ObString &uri, const bool is_adaptive, common::ObBaseDirEntryOperator &op);

private:
  // we does not use storage_info_ to judge init, since for nfs&local, storage_info_ is null
  bool is_init() { return init_state; }

  // If there only exists common type object in this uri, this function will list all the files.
  // If there also exists 'SIMULATE_APPEND' type object in this uri, this function will just list
  // this 'appendable-dir' name, not include its children objects' name.
  //
  // NOTICE: children objects of 'appendable-dir' all have the same prefix(OB_S3_APPENDABLE_FRAGMENT_PREFIX).
  //         If there exists some children objects not have this prefix, these objects will also be listed.
  //         Cuz we think these objects are just some common objects.
  int list_adaptive_files(const common::ObString &uri, common::ObBaseDirEntryOperator &op);
  // ObjectStorage and Filesystem need to handle seperately.
  int handle_listed_objs(ObStorageListCtxBase *ctx_base, const common::ObString &uri,
                         const common::ObString &dir_path, common::ObBaseDirEntryOperator &op);
  int handle_listed_appendable_obj(ObStorageListObjectsCtx *list_ctx, const common::ObString &uri,
                                   const common::ObString &dir_path, common::ObBaseDirEntryOperator &op);
  int handle_listed_fs(ObStorageListCtxBase *ctx_base, const common::ObString &uri,
                       const common::ObString &dir_path, common::ObBaseDirEntryOperator &op);

  // For 'SIMULATE_APPEND' type file, if we want to get its file length, we can't get its length from object meta directly.
  // <1> First, we need to check if there exists SEAL_META, if exists, read its content and get the file length
  // <2> If not exists, we need to list all its children objects and get the file length
  int get_adaptive_file_length(const common::ObString &uri, int64_t &file_length);
  int read_seal_meta_if_needed(const common::ObString &uri, ObStorageObjectMeta &obj_meta);

  int del_appendable_file(const common::ObString &uri);

  ObStorageFileUtil file_util_;
  ObStorageOssUtil oss_util_;
  ObStorageCosUtil cos_util_;
  ObStorageS3Util s3_util_;
  ObIStorageUtil* util_;
  common::ObObjectStorageInfo* storage_info_;
  bool init_state;
  ObStorageType device_type_;
  DISALLOW_COPY_AND_ASSIGN(ObStorageUtil);
};

class ObStorageReader
{
public:
  ObStorageReader();
  virtual ~ObStorageReader();
  virtual int open(const common::ObString &uri, common::ObObjectStorageInfo *storage_info);
  int pread(char *buf, const int64_t buf_size, int64_t offset, int64_t &read_size);
  int close();
  int64_t get_length() const { return file_length_; }

protected:
  int64_t file_length_;
  ObIStorageReader *reader_;
  ObStorageFileReader file_reader_;
  ObStorageOssReader oss_reader_;
  ObStorageCosReader cos_reader_;
  ObStorageS3Reader s3_reader_;
  int64_t start_ts_;
  char uri_[OB_MAX_URI_LENGTH];
private:
  DISALLOW_COPY_AND_ASSIGN(ObStorageReader);
};

// The most important meaning of this class is to read SIMULATE_APPEND file.
// But, if we use this class to read a normal object/file, it should also work well
class ObStorageAdaptiveReader
{
public:
  ObStorageAdaptiveReader();
  ~ObStorageAdaptiveReader();
  int open(const common::ObString &uri, common::ObObjectStorageInfo *storage_info);
  int pread(char *buf, const int64_t buf_size, const int64_t offset, int64_t &read_size);
  int close();
  int64_t get_length() const { return meta_.length_; }

private:
  ObArenaAllocator allocator_;
  ObStorageObjectMeta meta_;
  ObString bucket_;
  ObString object_;
  ObIStorageReader *reader_;
  ObStorageFileReader file_reader_;
  ObStorageOssReader oss_reader_;
  ObStorageCosReader cos_reader_;
  ObStorageS3Reader s3_reader_;
  int64_t start_ts_;
  char uri_[OB_MAX_URI_LENGTH];
  ObObjectStorageInfo *storage_info_;
  DISALLOW_COPY_AND_ASSIGN(ObStorageAdaptiveReader);
};

class ObStorageWriter
{
public:
  ObStorageWriter();
  virtual ~ObStorageWriter();
  virtual int open(const common::ObString &uri, common::ObObjectStorageInfo *storage_info);
  int write(const char *buf,const int64_t size);
  int close();
protected:
  ObIStorageWriter *writer_;
  ObStorageFileWriter file_writer_;
  ObStorageOssWriter oss_writer_;
  ObStorageCosWriter cos_writer_;
  ObStorageS3Writer s3_writer_;
  int64_t start_ts_;
  char uri_[OB_MAX_URI_LENGTH];
private:
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
  int write(const char *buf, const int64_t size);
  int pwrite(const char *buf, const int64_t size, const int64_t offset);
  int close();
  bool is_opened() const { return is_opened_; }
  int64_t get_length();
  void set_open_mode(StorageOpenMode mode) {file_appender_.set_open_mode(mode);}
  int seal_for_adaptive();
  TO_STRING_KV(KP_(appender), K_(start_ts), K_(is_opened), KCSTRING_(uri));

private:
  ObIStorageWriter *appender_;
  ObStorageFileAppender file_appender_;
  ObStorageOssAppendWriter oss_appender_;
  ObStorageCosAppendWriter cos_appender_;
  ObStorageS3AppendWriter s3_appender_;
  int64_t start_ts_;
  bool is_opened_;
  char uri_[OB_MAX_URI_LENGTH];
  common::ObObjectStorageInfo storage_info_;
  ObArenaAllocator allocator_;
  ObStorageType type_;

  int repeatable_pwrite_(const char *buf, const int64_t size, const int64_t offset);
  DISALLOW_COPY_AND_ASSIGN(ObStorageAppender);
};

class ObStorageMultiPartWriter
{
public:
  ObStorageMultiPartWriter();
  virtual ~ObStorageMultiPartWriter();
  virtual int open(const common::ObString &uri, common::ObObjectStorageInfo *storage_info);
  int write(const char *buf, const int64_t size);
  int pwrite(const char *buf, const int64_t size, const int64_t offset);
  int complete();
  int abort();
  int close();
  bool is_opened()  const {return is_opened_;}
  int64_t get_length();
  TO_STRING_KV(KP_(multipart_writer), K_(start_ts), K_(is_opened), KCSTRING_(uri));

protected:
  ObIStorageMultiPartWriter *multipart_writer_;
  ObStorageFileMultiPartWriter file_multipart_writer_;
  ObStorageCosMultiPartWriter cos_multipart_writer_;
  ObStorageOssMultiPartWriter oss_multipart_writer_;
  ObStorageS3MultiPartWriter s3_multipart_writer_;
  int64_t start_ts_;
  bool is_opened_;
  char uri_[OB_MAX_URI_LENGTH];
  common::ObObjectStorageInfo storage_info_;
	DISALLOW_COPY_AND_ASSIGN(ObStorageMultiPartWriter);
};

}//common
}//oceanbase

#endif /* SRC_LIBRARY_SRC_LIB_RESTORE_OB_STORAGE_H_ */
