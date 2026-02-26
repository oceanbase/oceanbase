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
#include "lib/container/ob_heap.h"            // ObBinaryHeap
#include "lib/container/ob_array_iterator.h"

namespace oceanbase
{
namespace common
{

static constexpr char OB_STORAGE_NFS_ALLOCATOR[] = "StorageNFS";

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
  virtual int batch_del_files(
      const ObString &uri,
      hash::ObHashMap<ObString, int64_t> &files_to_delete,
      ObIArray<int64_t> &failed_files_idx) override;
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
  int inner_pwrite(const char *buf, const int64_t size, const int64_t offset);
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
// allow to call write() at most once. if call write() multiple times, then return error.
class ObStorageFileSingleWriter: public ObStorageFileBaseWriter
{
public:
  ObStorageFileSingleWriter();
  virtual ~ObStorageFileSingleWriter();
  virtual int open(const common::ObString &uri, common::ObObjectStorageInfo *storage_info = NULL);
  virtual int write(const char *buf,const int64_t size) override;
  virtual int close() override;
protected:
  int close_and_rename();
protected:
  char real_path_[OB_MAX_URI_LENGTH];
private:
  DISALLOW_COPY_AND_ASSIGN(ObStorageFileSingleWriter);
  bool is_file_path_obtained_;
  bool is_written_;
};

// allow to call pwrite() multiple times. the real file is not readable until close, which rename
// tmp file to real file. only used for implementing MultiPartWriter.
class ObStorageFileMultipleWriter: public ObStorageFileSingleWriter
{
public:
  ObStorageFileMultipleWriter() {}
  virtual ~ObStorageFileMultipleWriter() {}
  virtual int open(const common::ObString &uri, common::ObObjectStorageInfo *storage_info = NULL) override;
  virtual int write(const char *buf,const int64_t size) override;
  virtual int close() override;
  void set_error();
private:
  DISALLOW_COPY_AND_ASSIGN(ObStorageFileMultipleWriter);
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
  virtual int pwrite(const char *buf, const int64_t size, const int64_t offset) override;
  virtual int close() override;
  void set_open_mode(StorageOpenMode mode) {open_mode_ = mode;}
private:
  int get_open_flag_and_mode_(int &flag, bool &need_lock);
private:
  bool need_unlock_;
  StorageOpenMode open_mode_;
  DISALLOW_COPY_AND_ASSIGN(ObStorageFileAppender);
};

class ObStorageFileMultiPartWriter : public ObStorageFileMultipleWriter, public ObIStorageMultiPartWriter
{
public:
  ObStorageFileMultiPartWriter() {}
  virtual ~ObStorageFileMultiPartWriter() {}

  virtual int open(const common::ObString &uri, common::ObObjectStorageInfo *storage_info) override
  {
    return ObStorageFileMultipleWriter::open(uri, storage_info);
  }
  virtual int write(const char *buf, const int64_t size) override
  {
    return ObStorageFileMultipleWriter::write(buf, size);
  }
  virtual int pwrite(const char *buf, const int64_t size, const int64_t offset) override
  {
    UNUSED(offset);
    return write(buf, size);
  }
  virtual int64_t get_length() const override
  {
    return ObStorageFileMultipleWriter::get_length();
  }
  virtual bool is_opened() const override
  {
    return ObStorageFileMultipleWriter::is_opened();
  }

  virtual int complete() override;
  virtual int abort() override;
  virtual int close() override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObStorageFileMultiPartWriter);
};

class ObStorageParallelFileMultiPartWriter: public ObIStorageParallelMultipartWriter
{
public:
  ObStorageParallelFileMultiPartWriter();
  virtual ~ObStorageParallelFileMultiPartWriter();
  void reset();

  virtual int open(const ObString &uri, ObObjectStorageInfo *storage_info) override;
  // During the upload phase, each part of the file is written to a separate temporary file
  virtual int upload_part(const char *buf, const int64_t size, const int64_t part_id) override;
  // Upon completion, these temporary files are read in sequence according to their assigned
  // part IDs and merged into the final target file.
  virtual int complete() override;
  // If the upload is aborted, all temporary
  // files are removed to clean up the storage space
  virtual int abort() override;
  virtual int close() override;
  virtual bool is_opened() const override { return is_opened_; }

private:
  static constexpr const char *TMP_NAME_FORMAT = "%s%s.tmp.%ld";

  SpinRWLock lock_;
  bool is_opened_;
  char real_path_[OB_MAX_URI_LENGTH];
  char path_[OB_MAX_URI_LENGTH];
  int64_t max_part_size_;
  ObArray<int64_t> uploaded_part_id_list_;

  DISALLOW_COPY_AND_ASSIGN(ObStorageParallelFileMultiPartWriter);
};


}//common
}//oceanbase

#endif /* SRC_LIBRARY_SRC_LIB_RESTORE_OB_STORAGE_FILE_H_ */
