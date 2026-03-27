/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef  OCEANBASE_UPDATESERVER_FILEINFO_MANAGER_H_
#define  OCEANBASE_UPDATESERVER_FILEINFO_MANAGER_H_
#include <sys/types.h>
#include <dirent.h>
#include <sys/vfs.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <pthread.h>
#include <new>
#include <algorithm>
#include "lib/ob_define.h"

namespace oceanbase
{
namespace common
{
class IFileInfo
{
public:
  virtual ~IFileInfo() {};
public:
  virtual int get_fd() const = 0;
};

class IFileInfoMgr
{
public:
  virtual ~IFileInfoMgr() {};
public:
  virtual const IFileInfo *get_fileinfo(const uint64_t key_id) = 0;
  virtual int revert_fileinfo(const IFileInfo *file_info) = 0;
  virtual int erase_fileinfo(const uint64_t key_id)
  {
    UNUSED(key_id);
    return OB_NOT_SUPPORTED;
  };
};
}
}

#endif //OCEANBASE_UPDATESERVER_FILEINFO_MANAGER_H_

