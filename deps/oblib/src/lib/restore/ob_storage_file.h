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

#ifndef SRC_LIBRARY_SRC_LIB_RESTORE_OB_STORAGE_FILE_H_
#define SRC_LIBRARY_SRC_LIB_RESTORE_OB_STORAGE_FILE_H_

#include "ob_i_storage.h"

namespace oceanbase
{
namespace common
{
class ObStorageFileUtil: public ObIStorageUtil
{
public:
  ObStorageFileUtil();
  virtual ~ObStorageFileUtil();
  virtual int open(common::ObObjectStorageInfo *storage_info)
  {
    UNUSED(storage_info);
    return OB_SUCCESS;
  }

  virtual void close() {}

  virtual int is_exist(const common::ObString &uri, bool &exist);
  virtual int get_file_length(const common::ObString &uri, int64_t &file_length);
  virtual int head_object_meta(const common::ObString &uri, ObStorageObjectMetaBase &obj_meta);
  virtual int del_file(const common::ObString &uri);
  virtual int write_single_file(const common::ObString &uri, const char *buf, const int64_t size);
  virtual int mkdir(const common::ObString &uri);
  virtual int list_files(const common::ObString &uri, common::ObBaseDirEntryOperator &op);
  virtual int list_files(const common::ObString &uri, ObStorageListCtxBase &list_ctx);
  virtual int del_dir(const common::ObString &uri);
  virtual int list_directories(const common::ObString &uri, common::ObBaseDirEntryOperator &op);
  virtual int is_tagging(const common::ObString &uri, bool &is_tagging);
  virtual int del_unmerged_parts(const ObString &uri) override;
private:
  int get_tmp_file_format_timestamp(const char *file_name, bool &is_tmp_file, int64_t &timestamp);
  int check_is_appendable(const common::ObString &uri, struct dirent &entry, bool &is_appendable_file);

private:
  DISALLOW_COPY_AND_ASSIGN(ObStorageFileUtil);
};

class ObStorageFileReader: public ObIStorageReader
{
public:
  ObStorageFileReader();
  virtual ~ObStorageFileReader();
  virtual int open(const common::ObString &uri,
      common::ObObjectStorageInfo *storage_info = NULL, const bool head_meta = true) override;
  virtual int pread(char *buf,
      const int64_t buf_size, const int64_t offset, int64_t &read_size) override;
  virtual int close() override;
  virtual int64_t get_length() const override { return file_length_; }
  virtual bool is_opened() const override { return is_opened_; }
private:
  int fd_;
  bool is_opened_;
  char path_[OB_MAX_URI_LENGTH];
  int64_t file_length_;
  DISALLOW_COPY_AND_ASSIGN(ObStorageFileReader);
};

// Only used for NFS file systems mounted in direct form, so there is no fsync
class ObStorageFileBaseWriter: public ObIStorageWriter
{
public:
  ObStorageFileBaseWriter();
  virtual ~ObStorageFileBaseWriter();
  virtual int open(const common::ObString &uri, common::ObObjectStorageInfo *storage_info = NULL) = 0;
  virtual int open(const int flags);
  virtual int write(const char *buf,const int64_t size);
  virtual int pwrite(const char *buf, const int64_t size, const int64_t offset);
  virtual int close();
  virtual int64_t get_length() const { return file_length_; }
  virtual bool is_opened() const { return is_opened_; }
protected:
  int fd_;
  bool is_opened_;
  char path_[OB_MAX_URI_LENGTH];
  int64_t file_length_;
  bool has_error_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObStorageFileBaseWriter);
};

// Only used for NFS file systems mounted in direct form, so there is no fsync
class ObStorageFileWriter: public ObStorageFileBaseWriter
{
public:
  ObStorageFileWriter();
  virtual ~ObStorageFileWriter();
  virtual int open(const common::ObString &uri, common::ObObjectStorageInfo *storage_info = NULL);
  virtual int close() override;
protected:
  char real_path_[OB_MAX_URI_LENGTH];
private:
  DISALLOW_COPY_AND_ASSIGN(ObStorageFileWriter);
};

// Only used for NFS file systems mounted in direct form, so there is no fsync
// use file lock to gurantee one process appendding this file
class ObStorageFileAppender: public ObStorageFileBaseWriter
{
public:
  ObStorageFileAppender();
  ObStorageFileAppender(StorageOpenMode mode);
  virtual ~ObStorageFileAppender();
  virtual int open(const common::ObString &uri, common::ObObjectStorageInfo *storage_info = NULL);
  virtual int close() override;
  void set_open_mode(StorageOpenMode mode) {open_mode_ = mode;}
private:
  int get_open_flag_and_mode_(int &flag, bool &need_lock);
private:
  bool need_unlock_;
  StorageOpenMode open_mode_;
  DISALLOW_COPY_AND_ASSIGN(ObStorageFileAppender);
};

class ObStorageFileMultiPartWriter : public ObStorageFileWriter, public ObIStorageMultiPartWriter
{
public:
  ObStorageFileMultiPartWriter() {}
  virtual ~ObStorageFileMultiPartWriter() {}

  virtual int open(const common::ObString &uri, common::ObObjectStorageInfo *storage_info) override
  {
    return ObStorageFileWriter::open(uri, storage_info);
  }
  virtual int write(const char *buf, const int64_t size) override
  {
    return ObStorageFileWriter::write(buf, size);
  }
  virtual int64_t get_length() const override
  {
    return ObStorageFileWriter::get_length();
  }
  virtual bool is_opened() const override
  {
    return ObStorageFileWriter::is_opened();
  }

  virtual int pwrite(const char *buf, const int64_t size, const int64_t offset) override;
  virtual int complete() override;
  virtual int abort() override;
  virtual int close() override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObStorageFileMultiPartWriter);
};


}//common
}//oceanbase

#endif /* SRC_LIBRARY_SRC_LIB_RESTORE_OB_STORAGE_FILE_H_ */
