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

#ifndef OCEANBASE_LOGSERVICE_LOG_BLOCK_POOL_INTERFACE_
#define OCEANBASE_LOGSERVICE_LOG_BLOCK_POOL_INTERFACE_
#include <stdint.h>
#include "lib/file/file_directory_utils.h"        // FileDirectoryUtils
#include "log_define.h"                           // block_id_t
#include "log_block_header.h"                     // LogBlockHeader
namespace oceanbase
{
namespace palf
{

class ILogBlockPool
{
public:
  virtual int create_block_at(const palf::FileDesc &dir_fd,
                              const char *block_path,
                              const int64_t block_size) = 0;
  virtual int remove_block_at(const palf::FileDesc &dir_fd,
                              const char *block_path) = 0;
};

int is_block_used_for_palf(const int fd, const char *path, bool &result);
int remove_file_at(const char *dir, const char *path, ILogBlockPool *log_block_pool);
int remove_directory_rec(const char *path, ILogBlockPool *log_block_pool);
int remove_tmp_file_or_directory_at(const char *path, ILogBlockPool *log_block_pool);
} // namespace palf
} // namespace oceanbase
#endif
