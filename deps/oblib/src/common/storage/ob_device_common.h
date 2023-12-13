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

#ifndef SRC_LIBRARY_SRC_COMMON_STORAGE_OB_DEVICE_COMMON_
#define SRC_LIBRARY_SRC_COMMON_STORAGE_OB_DEVICE_COMMON_

#include <dirent.h>
#include "lib/ob_define.h"

namespace oceanbase
{
namespace common
{

const static int DEFAULT_OPT_ARG_NUM = 4;

class ObBaseDirEntryOperator
{
public:
  enum ObDirOpFlag {
    DOF_REG = 0,
    DOF_DIR = 1,
    DOF_MAX_FLAG
  };
  ObBaseDirEntryOperator() : op_flag_(DOF_REG), size_(0) {}
  virtual ~ObBaseDirEntryOperator() = default;
  virtual int func(const dirent *entry) = 0;
  virtual bool need_get_file_size() const { return false; }
  void set_dir_flag() {op_flag_ = DOF_DIR;}
  bool is_dir_scan() {return (op_flag_ == DOF_DIR) ? true : false;}
  void set_size(const int64_t size) { size_ = size; }
  int64_t get_size() const { return size_; }
private:
  int op_flag_;
  int64_t size_; // Always set 0 for directory.
};

/*ObStorageType and OB_STORAGE_TYPES_STR should be mapped one by one*/
enum ObStorageType
{
  OB_STORAGE_OSS = 0,
  OB_STORAGE_FILE = 1,
  OB_STORAGE_COS = 2,
  OB_STORAGE_LOCAL = 3,
  OB_STORAGE_S3 = 4,
  OB_STORAGE_MAX_TYPE
};

enum ObStorageAccessType
{
  OB_STORAGE_ACCESS_READER = 0,
  OB_STORAGE_ACCESS_ADAPTIVE_READER = 1,
  OB_STORAGE_ACCESS_OVERWRITER = 2,
  OB_STORAGE_ACCESS_APPENDER = 3,
  OB_STORAGE_ACCESS_RANDOMWRITER = 4,
  OB_STORAGE_ACCESS_MULTIPART_WRITER = 5,
  OB_STORAGE_ACCESS_MAX_TYPE
};

}
}
#endif
