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

#ifndef OCEANBASE_AGENTSERVER_OSS_STORAGE_COS_BASE_H_
#define OCEANBASE_AGENTSERVER_OSS_STORAGE_COS_BASE_H_

#include "lib/allocator/page_arena.h"
#include "lib/container/ob_array.h"
#include "lib/allocator/page_arena.h"
#include "lib/lock/ob_spin_rwlock.h"
#include "ob_i_storage.h"
#include "cos/ob_cos_wrapper_handle.h"

namespace oceanbase
{
namespace common
{
class ObStorageCosBase;
struct CosListFilesCbArg;

// Before using cos, you need to initialize cos enviroment.
// Thread safe guaranteed by user.
int init_cos_env();

// You need to clean cos resource when not use cos any more.
// Thread safe guaranteed by user.
void fin_cos_env();

class ObStorageCosUtil: public ObIStorageUtil
{
public:
  ObStorageCosUtil();
  virtual ~ObStorageCosUtil();
  virtual int open(common::ObObjectStorageInfo *storage_info);
  virtual void close();
  virtual int is_exist(const common::ObString &uri, bool &is_exist);
  virtual int get_file_length(const common::ObString &uri, int64_t &file_length);
  virtual int write_single_file(const common::ObString &uri, const char *buf,
                                const int64_t size);

  //cos no dir
  virtual int mkdir(const common::ObString &uri);
  virtual int del_file(const common::ObString &uri);
  virtual int list_files(const common::ObString &uri, common::ObBaseDirEntryOperator &op);
  virtual int del_dir(const common::ObString &uri);
  virtual int list_directories(const common::ObString &uri, common::ObBaseDirEntryOperator &op);
  virtual int is_tagging(const common::ObString &uri, bool &is_tagging);
private:
  int get_object_meta_(const common::ObString &uri, bool &is_file_exist, int64_t &file_length);

private:
  bool is_opened_;
  common::ObObjectStorageInfo *storage_info_;
};

class ObStorageCosBase
{
public:
  ObStorageCosBase();
  virtual ~ObStorageCosBase();

  virtual int open(const common::ObString &uri, common::ObObjectStorageInfo *storage_info);
  void reset();
  const ObCosWrapperHandle &get_handle() { return handle_; }

  // some cos function
  int get_cos_file_meta(bool &is_file_exist, common::qcloud_cos::CosObjectMeta &obj_meta);
  int delete_object(const common::ObString &uri);
  int list_objects(const common::ObString &uri, const common::ObString &dir_name_str,
      const char *separator, common::CosListFilesCbArg &arg);
  int list_directories(const common::ObString &uri, const common::ObString &dir_name_str,
      const char *next_marker_str, const char *delimiter_str, common::CosListFilesCbArg &arg);
  int is_object_tagging(const common::ObString &uri, bool &is_tagging);

private:
  int init_handle(const common::ObObjectStorageInfo &storage_info);
  bool is_valid() const { return handle_.is_valid(); }

protected:
  bool is_opened_;
  ObCosWrapperHandle handle_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObStorageCosBase);
};

class ObStorageCosWriter : public ObStorageCosBase, public ObIStorageWriter
{
public:
  ObStorageCosWriter();
  ~ObStorageCosWriter();
  virtual int open(const common::ObString &uri, common::ObObjectStorageInfo *storage_info) override;
  int write(const char *buf, const int64_t size);
  int pwrite(const char *buf, const int64_t size, const int64_t offset);
  int close();
  int64_t get_length() const { return file_length_;}
  bool is_opened() const { return is_opened_; }
private:
  int64_t file_length_;
  DISALLOW_COPY_AND_ASSIGN(ObStorageCosWriter);
};

class ObStorageCosReader: public ObStorageCosBase, public ObIStorageReader
{
public:
  ObStorageCosReader();
  virtual ~ObStorageCosReader();
  virtual int open(const common::ObString &uri, common::ObObjectStorageInfo *storage_info) override;
  int pread(char *buf, const int64_t buf_size, int64_t offset, int64_t &read_size);
  int close();
  int64_t get_length() const { return file_length_; }
  bool is_opened() const { return is_opened_; }

private:
  int64_t file_length_;

  DISALLOW_COPY_AND_ASSIGN(ObStorageCosReader);
};

class ObStorageCosAppendWriter : public ObStorageCosBase, public ObIStorageWriter
{
public:
  ObStorageCosAppendWriter();
  virtual ~ObStorageCosAppendWriter();

public:
  virtual int open(const common::ObString &uri, common::ObObjectStorageInfo *storage_info) override;
  int write(const char *buf, const int64_t size);
  int pwrite(const char *buf, const int64_t size, const int64_t offset);
  int close();
  int64_t get_length() const { return file_length_; }
  bool is_opened() const { return is_opened_; }

private:
  int do_write(const char *buf, const int64_t size, const int64_t offset, const bool is_pwrite);

private:
  int64_t file_length_;

  DISALLOW_COPY_AND_ASSIGN(ObStorageCosAppendWriter);
};

} //common
} //oceanbase
#endif