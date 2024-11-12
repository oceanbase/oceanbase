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
#include "common/storage/ob_device_common.h"
#include "lib/container/ob_se_array.h"
#include "lib/allocator/page_arena.h"
namespace oceanbase
{
namespace common
{
struct ObIOFd;
class ObIODevice;
}
namespace palf
{
const int OB_LOG_STORE_EPOCH_CHANGED = OB_STATE_NOT_MATCH;

int open_with_retry(const char *pathname,
                    const int flags,
                    const mode_t mode,
                    ObIOFd &fd,
                    ObIODevice *io_device);

int rename_with_retry(const char *src_name, const char *dest_name);

int fsync_with_retry(const char *src_name);

int fsync_with_retry(const char *src_name, ObIODevice *io_device);

int reuse_block(const char *block_path);

int fallocate_with_retry(const char *block_path, const int64_t block_size);

int ftruncate_with_retry(const char *block_path, const int64_t block_size);

int is_directory(const char *block_path,
                 bool &result);
int is_exists(const char *block_path,
              bool &result);

int get_file_size(const char *block_path,
                  int64_t &file_size);

int write_until_success(const char *pathname, const char *src_buf,
                        const int64_t src_buf_len, const int64_t offset);
int read_until_success(const char *pathname, char *dest_buf,
                       const int64_t dest_buf_len, const int64_t offset);

int read_until_success(const ObIOFd &fd, char *dest_buf,
                       const int64_t dest_buf_len,
                       const int64_t offset,
                       int64_t &read_size);

int mkdir(const char *path);
int rmdir(const char *path);

enum class ScanDirType {
  // get block whose name is end with flashback
  TRIM_LOG_DIRECTORY = 0,
  // get block count under spec directory
  GET_BLOCK_COUNT = 1,
  // get dir names under spec directory
  LIST_DIR = 2,
  // get file names with spec suffix
  LIST_FILES_WITH_SUFFIX = 3,
};

class ObBaseDirFunctor : public common::ObBaseDirEntryOperator
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
  int try_to_remove_block_(const char *file_name);
  const char *dir_;
  block_id_t min_block_id_;
  block_id_t max_block_id_;
  ILogBlockPool *log_block_pool_;

  DISALLOW_COPY_AND_ASSIGN(TrimLogDirectoryFunctor);
};

class ListDir: public ObBaseDirFunctor
{
public:
  static constexpr int64_t COUNT = 128;
  typedef ObSEArray<const char*, COUNT> Contents;
public:
  ListDir(const char *base_dir);
  virtual ~ListDir();

  int func(const dirent *entry) override final;
  const Contents get_contents() const { return contents_; };
private:
  common::ObArenaAllocator alloc_;
  Contents contents_;
  char base_dir_[OB_MAX_FILE_NAME_LENGTH];
  DISALLOW_COPY_AND_ASSIGN(ListDir);
};

inline bool is_flashback_block(const char *block_name)
{
  bool bool_ret = false;
  if (NULL != block_name && NULL != strstr(block_name, FLASHBACK_SUFFIX)) {
    bool_ret = true;
  }
  return bool_ret;
}

inline int convert_to_flashback_block(const char *log_dir,
                                      const block_id_t  block_id,
                                      char *buf,
                                      const int64_t buf_len)
{
  int64_t pos = 0;
  return databuff_printf(buf, buf_len, pos, "%s/%lu%s", log_dir,
          block_id, FLASHBACK_SUFFIX);
}

inline int convert_to_tmp_block(const char *log_dir,
                               const block_id_t  block_id,
                               char *buf,
                               const int64_t buf_len)
{
  int64_t pos = 0;
  return databuff_printf(buf, buf_len, pos, "%s/%lu%s", log_dir,
          block_id, TMP_SUFFIX);
}

inline int convert_to_normal_block(const char *log_dir,
                                   const block_id_t  block_id,
                                   char *buf,
                                   const int64_t buf_len)
{
  int64_t pos = 0;
  return databuff_printf(buf, buf_len, pos, "%s/%lu", log_dir, block_id);
}

int block_id_to_string(const block_id_t block_id,
                       char *str,
                       const int64_t str_len);
int block_id_to_tmp_string(const block_id_t block_id,
                           char *str,
                           const int64_t str_len);
int block_id_to_flashback_string(const block_id_t block_id,
																 char *str,
																 const int64_t str_len);
int construct_absolute_block_path(const char *dir_path,
                                  const block_id_t block_id,
                                  const int64_t buf_len,
                                  char *absolute_block_path);
int construct_absolute_tmp_block_path(const char *dir_path,
                                      const block_id_t block_id,
                                      const int64_t buf_len,
                                      char *absolute_tmp_block_path);
int construct_absolute_flashback_block_path(const char *dir_path,
                                            const block_id_t block_id,
                                            const int64_t buf_len,
                                            char *absolute_flashback_block_path);
int convert_sys_errno();
bool is_number(const char *);

// =========== Disk io start ==================
constexpr int LOG_READ_FLAG = O_RDONLY | O_DIRECT | O_SYNC;
constexpr int LOG_WRITE_FLAG = O_RDWR | O_DIRECT | O_SYNC;
constexpr mode_t FILE_OPEN_MODE = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
constexpr int CREATE_FILE_FLAG = O_RDWR | O_CREAT | O_EXCL | O_SYNC | O_DIRECT;
constexpr int OPEN_FILE_FLAG = O_RDWR | O_SYNC | O_DIRECT;
constexpr int OPEN_DIR_FLAG = O_DIRECTORY | O_RDONLY;
constexpr int CREATE_DIR_MODE = S_IRUSR | S_IWUSR | S_IXUSR | S_IRGRP | S_IROTH;
constexpr int CREATE_FILE_MODE = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
// =========== Disk io end ====================

// The layout of second_id_ in ObIOFd
// | sign bit | sync io bit | holder | 29 unused bit | 32 fd bit |
const int64_t SYNC_MODE_MASK = (1ll << 62);
const int64_t HOLDER_MODE_MASK = (1ll << 61);

inline void set_fd_in_sync_mode(int64_t &fd)
{
  fd |= SYNC_MODE_MASK;
}

inline void set_fd_holder(int64_t &fd, bool io_adapter)
{
  if (io_adapter) {
    fd |= HOLDER_MODE_MASK;
  } else {}
}

inline bool check_fd_is_in_sync_mode(const int64_t fd)
{
  return (fd & SYNC_MODE_MASK) > 0;
}

inline bool check_fd_holder_by_io_adapter(const int64_t fd)
{
  return (fd & HOLDER_MODE_MASK) > 0;
}

enum class LogSyncMode {
  INVALID = 0,
  SYNC = 1,
  ASYNC = 2
};

inline bool is_valid_log_sync_mode(const LogSyncMode mode)
{
  return LogSyncMode::INVALID != mode;
}

inline const char *log_sync_mode_user_str(const LogSyncMode mode)
{
  #define LOG_SYNC_MODE_STR(x) case(LogSyncMode::x): return #x
  switch (mode)
  {
    LOG_SYNC_MODE_STR(SYNC);
    LOG_SYNC_MODE_STR(ASYNC);
    default:
      return "InvalidMode";
  }
  #undef LOG_SYNC_MODE_STR
}

} // end namespace palf
} // end namespace oceanbase
#endif
