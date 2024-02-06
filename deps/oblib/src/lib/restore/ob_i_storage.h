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

#ifndef SRC_LIBRARY_SRC_LIB_RESTORE_OB_I_STORAGE_H_
#define SRC_LIBRARY_SRC_LIB_RESTORE_OB_I_STORAGE_H_

#include "lib/ob_define.h"
#include "lib/string/ob_string.h"
#include "lib/container/ob_array.h"
#include "common/storage/ob_device_common.h"
#include "ob_storage_info.h"

namespace oceanbase
{
namespace common
{
enum StorageOpenMode
{
  CREATE_OPEN_LOCK = 0, // default, create and open
  EXCLUSIVE_CREATE = 1, // exclusive create and open
  ONLY_OPEN_UNLOCK = 2, // only open
  CREATE_OPEN_NOLOCK = 3, // create and open nolock
};

class ObIStorageUtil
{
public:
  enum {
    NONE = 0,
    DELETE = 1,
    TAGGING = 2,
    MAX
  };
  virtual int open(common::ObObjectStorageInfo *storage_info) = 0;
  virtual void close() = 0;
  virtual int is_exist(const common::ObString &uri, bool &exist) = 0;
  virtual int get_file_length(const common::ObString &uri, int64_t &file_length) = 0;
  virtual int del_file(const common::ObString &uri) = 0;
  virtual int write_single_file(const common::ObString &uri, const char *buf, const int64_t size) = 0;
  virtual int mkdir(const common::ObString &uri) = 0;
  virtual int list_files(const common::ObString &dir_path, common::ObBaseDirEntryOperator &op) = 0;
  virtual int del_dir(const common::ObString &uri) = 0;
  virtual int list_directories(const common::ObString &dir_path, common::ObBaseDirEntryOperator &op) = 0;
  virtual int is_tagging(const common::ObString &uri, bool &is_tagging) = 0;
};

class ObIStorageReader
{
public:
  virtual int open(const common::ObString &uri, common::ObObjectStorageInfo *storage_info) = 0;
  virtual int pread(char *buf,const int64_t buf_size, int64_t offset, int64_t &read_size) = 0;
  virtual int close() = 0;
  virtual int64_t get_length() const = 0;
  virtual bool is_opened() const = 0;
};

class ObIStorageWriter
{
public:
  virtual int open(const common::ObString &uri, common::ObObjectStorageInfo *storage_info) = 0;
  virtual int write(const char *buf,const int64_t size) = 0;
  virtual int pwrite(const char *buf, const int64_t size, const int64_t offset) = 0;
  virtual int close() = 0;
  virtual int64_t get_length() const = 0;
  virtual bool is_opened() const = 0;
};


}//common
}//oceanbase
#endif /* SRC_LIBRARY_SRC_LIB_RESTORE_OB_I_STORAGE_H_ */
