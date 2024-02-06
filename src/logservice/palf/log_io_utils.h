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

#ifndef OCEANBASE_LOGSERVICE_LOG_IO_UTILS_
#define OCEANBASE_LOGSERVICE_LOG_IO_UTILS_
#include <dirent.h>                                      // dirent
#include "log_define.h"
namespace oceanbase
{
namespace palf
{

int openat_with_retry(const int dir_fd,
                      const char *block_path,
                      const int flag,
                      const int mode,
                      int &fd);
int close_with_ret(const int fd);

int rename_with_retry(const char *src_name, const char *dest_name);

int renameat_with_retry(const int srd_dir_fd, const char *src_name,
                        const int dest_dir_fd, const char *dest_name);

int fsync_with_retry(const int dir_fd);

class ObBaseDirFunctor
{
public:
  virtual int func(const dirent *entry) = 0;
};

int scan_dir(const char *dir_name, ObBaseDirFunctor &functor);

class GetBlockCountFunctor : public ObBaseDirFunctor
{
public:
  GetBlockCountFunctor(const char *dir)
    : dir_(dir), count_(0)
  {
  }
  virtual ~GetBlockCountFunctor() = default;

  int func(const dirent *entry) override final;
	int64_t get_block_count() {return count_;}
private:
  const char *dir_;
	int64_t count_;

  DISALLOW_COPY_AND_ASSIGN(GetBlockCountFunctor);
};

class TrimLogDirectoryFunctor : public ObBaseDirFunctor
{
public:
  TrimLogDirectoryFunctor(const char *dir, ILogBlockPool *log_block_pool)
    : dir_(dir),
      min_block_id_(LOG_INVALID_BLOCK_ID),
      max_block_id_(LOG_INVALID_BLOCK_ID),
      log_block_pool_(log_block_pool)
  {
  }
  virtual ~TrimLogDirectoryFunctor() = default;

  int func(const dirent *entry) override final;
  block_id_t get_min_block_id() const { return min_block_id_; }
  block_id_t get_max_block_id() const { return max_block_id_; }
private:
	int rename_flashback_to_normal_(const char *file_name);
  int try_to_remove_block_(const int dir_fd, const char *file_name);
  const char *dir_;
  block_id_t min_block_id_;
  block_id_t max_block_id_;
  ILogBlockPool *log_block_pool_;

  DISALLOW_COPY_AND_ASSIGN(TrimLogDirectoryFunctor);
};

int reuse_block_at(const int fd, const char *block_path);

} // end namespace palf
} // end namespace oceanbase
#endif
