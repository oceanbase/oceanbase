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

#include "ob_log_file_pool.h"
#include <sys/types.h>
#include <dirent.h>
#include <sys/vfs.h>
#include <libgen.h>
#include "lib/file/ob_file.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/statistic_event/ob_stat_event.h"
#include "lib/utility/ob_tracepoint.h"
#include "common/log/ob_log_data_writer.h"
#include "share/redolog/ob_log_file_reader.h"
#include "ob_log_common.h"
#include "ob_log_dir.h"
#include "ob_clog_mgr.h"

namespace oceanbase {
using namespace common;
using namespace share;
namespace clog {

// for stat event
void set_free_quota_event(const char* path, const int64_t free_quota)
{
  struct statfs fsst;
  int tmp_ret = OB_SUCCESS;
  if (OB_ISNULL(path)) {
    tmp_ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "path is null", K(tmp_ret), K(path), K(free_quota));
  } else if (OB_UNLIKELY(0 != (tmp_ret = statfs(path, &fsst)))) {
    CLOG_LOG(WARN, "statfs error", K(path), K(errno), KERRMSG, "ret", tmp_ret);
  } else {
    int64_t total_size = (int64_t)fsst.f_bsize * (int64_t)fsst.f_blocks;
    EVENT_SET(CLOG_DISK_FREE_SIZE, free_quota);
    EVENT_SET(CLOG_DISK_FREE_RATIO, (int)((double)free_quota / (double)total_size * 100));
  }
}

static void check_last_modify_duration(const char* path)
{
  int ret = OB_SUCCESS;
  if (NULL == path) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), KP(path));
  } else {
    int err = 0;
    struct stat src_stat;
    if (0 != (err = stat(path, &src_stat))) {
      ret = OB_IO_ERROR;
      CLOG_LOG(WARN, "get file stat fail", K(ret), K(err), K(path), K(errno), KERRMSG);
    } else {
      const int64_t now = ObTimeUtility::current_time();
      const int64_t diff_time = static_cast<int64_t>(difftime(static_cast<time_t>(now / 1000000), src_stat.st_mtime));
      if (diff_time <= CLOG_RESERVED_TIME_FOR_LIBOBLOG_SEC) {
        if (REACH_TIME_INTERVAL(600 * 1000 * 1000)) {
          CLOG_LOG(WARN, "reuse or remove log file which should be reserved", K(path));
        }
      }
    }
  }
}

ObLogWriteFilePool::ObLogWriteFilePool()
    : is_inited_(false),
      file_size_(0),
      free_quota_(0),
      limit_free_quota_(0),
      log_dir_(NULL),
      type_(INVALID_WRITE_POOL),
      min_file_id_(OB_INVALID_FILE_ID),
      min_using_file_id_(OB_INVALID_FILE_ID),
      max_file_id_(OB_INVALID_FILE_ID),
      ctx_(NULL),
      iocb_(),
      ioevent_()
{}

ObLogWriteFilePool::~ObLogWriteFilePool()
{
  destroy();
}

int ObLogWriteFilePool::init(ObILogDir* log_dir, const int64_t file_size, const ObLogWritePoolType type)
{
  int ret = OB_SUCCESS;
  const int64_t hard_limit_free_quota = ObServerConfig::get_instance().clog_disk_usage_limit_percentage;
  int64_t disk_use_percent = ObILogFileStore::DEFAULT_DISK_USE_PERCENT;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(WARN, "ObLogWriteFilePool init twice", K(ret));
  } else if (OB_UNLIKELY(NULL == log_dir) || OB_UNLIKELY(file_size <= 0) || OB_UNLIKELY(INVALID_WRITE_POOL == type)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "ObLogWriteFilePool init error", K(ret), KP(file_size), K(type));
  } else {
    if (CLOG_WRITE_POOL == type || ILOG_WRITE_POOL == type) {
      disk_use_percent = ObServerConfig::get_instance().clog_disk_utilization_threshold;
    } else if (SLOG_WRITE_POOL == type) {
      disk_use_percent = ObILogFileStore::SLOG_DISK_USE_PERCENT;
    } else {
      CLOG_LOG(ERROR, "unexpected type", K(type));
    }

    if (OB_FAIL(update_free_quota(log_dir->get_dir_name(), disk_use_percent, hard_limit_free_quota))) {
      CLOG_LOG(WARN, "update free quota error", K(ret));
    } else {
      memset(&ctx_, 0, sizeof(ctx_));
      if (0 != (ret = io_setup(TASK_NUM, &ctx_))) {
        CLOG_LOG(ERROR, "io setup fail", KERRNOMSG(-ret));
        ret = OB_IO_ERROR;
      }
    }
  }
  if (OB_SUCC(ret)) {
    is_inited_ = true;
    file_size_ = file_size;
    log_dir_ = log_dir;
    type_ = type;
    CLOG_LOG(INFO, "init log write file pool success", K(file_size), K(disk_use_percent), K(type));
  }
  return ret;
}

void ObLogWriteFilePool::destroy()
{
  is_inited_ = false;
  file_size_ = 0;
  free_quota_ = 0;
  limit_free_quota_ = 0;
  type_ = INVALID_WRITE_POOL;
  if (NULL != ctx_) {
    io_destroy(ctx_);
  }
}

// 1. Use mutex, keep thread safe
// 2. Generate file name by dest_file_id
// 3. Open file by file name
//
int ObLogWriteFilePool::get_fd(const file_id_t dest_file_id, int& fd)
{
  int ret = OB_SUCCESS;
  char dest_file[MAX_PATH_SIZE];
  int dir_fd = 0;
  ObSpinLockGuard guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_valid_file_id(dest_file_id))) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(log_dir_->get_path(dest_file_id, dest_file, sizeof(dest_file), dir_fd))) {
    CLOG_LOG(WARN, "get path fail", K(ret), K(dest_file_id), K(dir_fd));
  } else if ((fd = openat(dir_fd, dest_file, OPEN_FLAG_WITH_SYNC, OPEN_MODE)) >= 0) {
    // if the file exists, return the fd
    CLOG_LOG(INFO, "CLOG_FILE_POOL: open exist file", K(dir_fd), K(dest_file_id), K_(log_dir));
  } else if (errno != ENOENT) {
    ret = (errno == EMFILE) ? OB_FILE_NOT_OPENED : OB_IO_ERROR;
    CLOG_LOG(ERROR, "open file fail", K(dir_fd), K(dest_file), K_(log_dir), K(errno), KERRMSG);
  } else if (OB_FAIL(create_new_file(dir_fd, dest_file_id, dest_file, fd))) {
    CLOG_LOG(WARN, "create new file error", K(ret), K_(log_dir), K(dest_file_id), K(fd), K(dir_fd), K(dest_file));
  } else {
    CLOG_LOG(INFO, "create new file success", K(dest_file_id), K_(log_dir), K(dest_file), K(dir_fd));
    remove_overquota_file();
  }
  return ret;
}

int ObLogWriteFilePool::close_fd(const file_id_t file_id, const int fd)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_FILE_ID == file_id || fd < 0) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "invalid argument", K(ret), K(file_id), K(fd), K_(log_dir));
  } else {
    (void)clog::close_fd(fd);
  }
  return ret;
}

int ObLogWriteFilePool::get_file_id_range(file_id_t& min_file_id, file_id_t& max_file_id)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else {
    min_file_id = ATOMIC_LOAD(&min_file_id_);
    max_file_id = ATOMIC_LOAD(&max_file_id_);
    if (OB_INVALID_FILE_ID == min_file_id || OB_INVALID_FILE_ID == max_file_id) {
      ret = log_dir_->get_file_id_range(min_file_id, max_file_id);
      if (OB_SUCC(ret)) {
        CLOG_LOG(INFO, "get min/max file id from IO", K(min_file_id), K(max_file_id), K(lbt()));
      }
    }
  }
  return ret;
}

int ObLogWriteFilePool::update_free_quota()
{
  int ret = OB_SUCCESS;
  const int64_t hard_limit_free_quota = ObServerConfig::get_instance().clog_disk_usage_limit_percentage;
  int64_t disk_use_percent = ObILogFileStore::DEFAULT_DISK_USE_PERCENT;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else {
    if (CLOG_WRITE_POOL == type_ || ILOG_WRITE_POOL == type_) {
      disk_use_percent = ObServerConfig::get_instance().clog_disk_utilization_threshold;
    } else if (SLOG_WRITE_POOL == type_) {
      disk_use_percent = ObILogFileStore::SLOG_DISK_USE_PERCENT;
    } else {
      CLOG_LOG(ERROR, "unexpected type_", K(type_));
    }

    ret = update_free_quota(log_dir_->get_dir_name(), disk_use_percent, hard_limit_free_quota);
  }
  return ret;
}

void ObLogWriteFilePool::remove_overquota_file()
{
  int ret = OB_SUCCESS;
  file_id_t file_id = OB_INVALID_FILE_ID;
  char fname[MAX_PATH_SIZE];
  int dir_fd = 0;
  if (ATOMIC_LOAD(&free_quota_) > 0) {
    // do nothing
  } else if (OB_FAIL(get_recyclable_file(file_id))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      CLOG_LOG(WARN, "get recyclable file failed", K(ret));
    }
  } else {
    if (OB_FAIL(log_dir_->get_path(file_id, fname, sizeof(fname), dir_fd))) {
      CLOG_LOG(WARN, "get path failed", K(file_id), K_(log_dir), K(dir_fd));
    } else {
      check_last_modify_duration(fname);
      unlink_file(fname, dir_fd);
    }
  }
}

void ObLogWriteFilePool::update_min_file_id(const file_id_t file_id)
{
  while (true) {
    const file_id_t orig_min_file_id = ATOMIC_LOAD(&min_file_id_);
    // check origin min_file_id_ is valid or not
    if (is_valid_file_id(orig_min_file_id) && file_id <= orig_min_file_id) {
      break;
    } else if (ATOMIC_BCAS(&min_file_id_, orig_min_file_id, file_id)) {
      break;
    } else {
      // do nothing
    }
  }
}

void ObLogWriteFilePool::update_max_file_id(const file_id_t file_id)
{
  while (true) {
    const file_id_t orig_max_file_id = ATOMIC_LOAD(&max_file_id_);
    if (is_valid_file_id(orig_max_file_id) && file_id <= orig_max_file_id) {
      break;
    } else if (ATOMIC_BCAS(&max_file_id_, orig_max_file_id, file_id)) {
      break;
    } else {
      // do nothing
    }
  }
}

void ObLogWriteFilePool::update_min_using_file_id(const file_id_t file_id)
{
  const file_id_t orig_min_using_file_id = ATOMIC_LOAD(&min_using_file_id_);
  if (is_valid_file_id(orig_min_using_file_id) && orig_min_using_file_id > file_id) {
    CLOG_LOG(ERROR, "min using file id go back", K(orig_min_using_file_id), K(file_id));
  } else if (orig_min_using_file_id == file_id) {
    // do nothing
  } else {
    ATOMIC_STORE(&min_using_file_id_, file_id);
  }
}

void ObLogWriteFilePool::try_recycle_file()
{
  ObSpinLockGuard guard(lock_);
  remove_overquota_file();
}

int ObLogWriteFilePool::get_total_used_size(int64_t& total_size) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else {
    ret = log_dir_->get_total_size(total_size);
  }
  return ret;
}

int ObLogWriteFilePool::create_new_file(const int dest_dir_fd, const int dest_file_id, const char* dest_file, int& fd)
{
  int ret = OB_SUCCESS;
  int tmp_fd = -1;
  file_id_t file_id = OB_INVALID_FILE_ID;
  char tmp_file[MAX_PATH_SIZE];
  int tmp_dir_fd = -1;
  bool need_create = false;
  if (ATOMIC_LOAD(&free_quota_) > RESERVED_QUOTA) {
    need_create = true;
  } else if (OB_FAIL(get_recyclable_file(file_id))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      CLOG_LOG(WARN, "get recyclable file failed", K(ret));
    } else {
      need_create = true;
      // rewrite ret
      ret = OB_SUCCESS;
    }
  } else {
    if (OB_FAIL(log_dir_->get_path(file_id, tmp_file, sizeof(tmp_file), tmp_dir_fd))) {
      CLOG_LOG(WARN, "get path failed", K(ret), K(file_id), K_(log_dir), K(tmp_dir_fd));
    } else {
      if ((tmp_fd = openat(tmp_dir_fd, tmp_file, OPEN_FLAG_WITHOUT_SYNC, OPEN_MODE)) < 0) {
        CLOG_LOG(WARN, "open file error", K(ret), K(tmp_file), K(tmp_dir_fd), KERRMSG);
        if (ENOENT != errno) {
          ret = (errno == EMFILE) ? OB_FILE_NOT_OPENED : OB_IO_ERROR;
        } else {
          need_create = true;
        }
      }
    }
  }
  if (OB_SUCCESS == ret) {
    // Create temporary file, and get its directory file description
    if (need_create && OB_FAIL(create_tmp_file(dest_file_id, tmp_file, sizeof(tmp_file), tmp_fd, tmp_dir_fd))) {
      CLOG_LOG(WARN, "create tmp file error", K(ret), K(dest_file_id));
    }
  }
  if (OB_SUCCESS == ret) {
    if (type_ == ObLogWritePoolType::CLOG_WRITE_POOL &&
        OB_FAIL(init_raw_file(tmp_fd, 0, sizeof(ObNewLogFileBuf::buf_)))) {
      CLOG_LOG(WARN, "init raw file error", K(ret), K(tmp_fd), K(tmp_file));
    } else if (OB_FAIL(rename_file(tmp_dir_fd, tmp_file, dest_dir_fd, dest_file))) {
      CLOG_LOG(WARN, "rename file error", K(ret), K(tmp_dir_fd), K(tmp_file), K(dest_file), K(dest_dir_fd));
    } else {
      ATOMIC_STORE(&max_file_id_, dest_file_id);
      CLOG_LOG(INFO, "rename file success", K(tmp_dir_fd), K(tmp_file), K(dest_dir_fd), K(dest_file), K(dest_file_id));
      if (!need_create) {
        if (OB_FAIL(OB_LOG_FILE_READER.evict_fd(log_dir_->get_dir_name(), file_id))) {
          CLOG_LOG(WARN, "evict fd fail", K(ret), K(log_dir_), K(file_id));
        } else {
          // Avoid reading file that are being recycled
          WaitQuiescent(get_log_file_qs());
        }
      }
    }
  }
  if (OB_SUCCESS == ret) {
    fd = tmp_fd;
  } else {
    if (tmp_fd >= 0) {
      (void)clog::close_fd(tmp_fd);
      tmp_fd = -1;
    }
  }
  return ret;
}

int ObLogWriteFilePool::create_tmp_file(const file_id_t file_id, char* fname, const int64_t size, int& fd, int& dir_fd)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(log_dir_->get_tmp_path(file_id, fname, size, dir_fd))) {
    CLOG_LOG(WARN, "get tmp path failed", K(ret), K(file_id), K_(log_dir));
  } else if (OB_FAIL(create_file(dir_fd, fname, file_size_, fd))) {
    CLOG_LOG(WARN, "create file failed", K(ret), K(fname), K(size), K_(log_dir), K(dir_fd));
  } else {
    // do nothing
  }
  return ret;
}

int ObLogWriteFilePool::update_free_quota(const char* path, const int64_t percent, const int64_t limit_percent)
{
  int ret = OB_SUCCESS;
  struct statfs fsst;
  if (NULL == path || 0 > percent || 100 < percent || 0 > limit_percent || 100 < limit_percent) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(0 != statfs(path, &fsst))) {
    ret = OB_IO_ERROR;
    CLOG_LOG(ERROR, "statfs error", K(ret), K(path), K(errno), KERRMSG);
  } else {
    const int64_t total_size = (int64_t)fsst.f_bsize * (int64_t)fsst.f_blocks;
    const int64_t free_quota =
        (int64_t)fsst.f_bsize * ((int64_t)fsst.f_blocks * percent / 100LL - (int64_t)(fsst.f_blocks - fsst.f_bavail));
    const int64_t used_size = (int64_t)fsst.f_bsize * (int64_t)(fsst.f_blocks - fsst.f_bavail);
    int64_t limit_quota = (int64_t)fsst.f_bsize *
                          ((int64_t)fsst.f_blocks * limit_percent / 100LL - (int64_t)(fsst.f_blocks - fsst.f_bavail));
    if (100 == limit_percent) {
      limit_quota = limit_quota - RESERVED_QUOTA_2;
    }
    ATOMIC_STORE(&free_quota_, free_quota);
    ATOMIC_STORE(&limit_free_quota_, limit_quota);
    EVENT_SET(CLOG_DISK_FREE_SIZE, free_quota);
    EVENT_SET(CLOG_DISK_FREE_RATIO, (int)((double)free_quota / (double)total_size * 100));
    if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
      CLOG_LOG(INFO,
          "update free quota",
          K(type_),
          K(total_size),
          K(used_size),
          K(free_quota),
          K(limit_quota),
          "warn_percent(%)",
          percent,
          K(limit_percent),
          "used_percent(%)",
          used_size * 100 / (total_size + 1));
    }
    if (free_quota + 1024 * 1024 * 1024 < 0) {
      if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
        CLOG_LOG(ERROR,
            "clog disk is almost full",
            K(type_),
            K(total_size),
            K(free_quota),
            "warn_percent(%)",
            percent,
            K(limit_percent),
            "used_percent(%)",
            used_size * 100 / (total_size + 1));
      }
    }
  }
  return ret;
}

int ObLogWriteFilePool::init_raw_file(const int fd, const int64_t start_pos, const int64_t size)
{
  int ret = OB_SUCCESS;
  if (0 > fd || 0 > start_pos || 0 >= size) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(fd), K(start_pos), K(size));
  } else {
    int64_t offset = start_pos;
    int64_t left = size;
    while (OB_SUCCESS == ret && left > 0) {
      const int64_t to_write = (sizeof(ObNewLogFileBuf::buf_) < left ? sizeof(ObNewLogFileBuf::buf_) : left);
      const int64_t nwrite =
          aio_write(fd, ObNewLogFileBuf::buf_, to_write, offset, CLOG_AIO_WRITE_TIMEOUT, iocb_, ctx_, ioevent_);
      if (nwrite != to_write) {
        ret = OB_IO_ERROR;
        CLOG_LOG(ERROR, "write eof flag error", K(ret), K(fd), K(errno), KERRMSG);
      } else {
        offset = offset + nwrite;
        left = left - nwrite;
      }
    }
  }
  return ret;
}

int ObLogWriteFilePool::get_recyclable_file(file_id_t& file_id)
{
  int ret = OB_SUCCESS;
  while (OB_SUCCESS == ret) {
    const file_id_t min_file_id = get_min_file_id();
    const file_id_t min_using_file_id = get_min_using_file_id();
    if (min_file_id >= min_using_file_id) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      if (ATOMIC_BCAS(&min_file_id_, min_file_id, min_file_id + 1)) {
        file_id = min_file_id;
        break;
      }
    }
  }
  return ret;
}

int ObLogWriteFilePool::create_file(const int dir_fd, const char* fname, const int64_t file_size, int& fd)
{
  int ret = OB_SUCCESS;
  int tmp_fd = -1;
  if (OB_ISNULL(fname) || OB_UNLIKELY(0 >= file_size) || OB_UNLIKELY(dir_fd < 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else if ((tmp_fd = OB_I(open) openat(dir_fd, fname, CREATE_FLAG_WITH_SYNC, OPEN_MODE)) < 0) {
    ret = (errno == EMFILE) ? OB_FILE_NOT_OPENED : OB_IO_ERROR;
    CLOG_LOG(ERROR, "open error", K(ret), K(dir_fd), K(fname), K(errno), KERRMSG);
  } else {
    // Only CLOG need allocate file size first.
    if (type_ == ObLogWritePoolType::CLOG_WRITE_POOL) {
      if (0 != myfallocate(tmp_fd, 0, 0, file_size)) {
        ret = OB_IO_ERROR;
        CLOG_LOG(ERROR, "fallocate error", K(ret), K(tmp_fd), K(fname), K(errno), KERRMSG);
      } else if (0 != fsync(tmp_fd)) {
        ret = OB_IO_ERROR;
        CLOG_LOG(ERROR, "fsync error", K(ret), K(tmp_fd), K(fname), K(errno), KERRMSG);
      } else {
        // do nothing
      }
    }
    if (OB_SUCCESS != ret) {
      (void)clog::close_fd(tmp_fd);
      tmp_fd = -1;
    } else {
      fd = tmp_fd;
      (void)ATOMIC_AAF(&free_quota_, 0 - file_size);
      (void)ATOMIC_AAF(&limit_free_quota_, 0 - file_size);
    }
  }
  return ret;
}

void ObLogWriteFilePool::unlink_file(const char* fname, const int dir_fd)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(fname) || OB_UNLIKELY(dir_fd < 0)) {
    CLOG_LOG(WARN, "invalid file name or directory fd", KP(fname), K(dir_fd));
  } else if (0 != unlinkat(dir_fd, fname, 0)) {
    CLOG_LOG(WARN, "unlink file error", K(fname), K(dir_fd), K(errno), KERRMSG);
  } else if (OB_FAIL(fsync_dir(dir_fd))) {
    CLOG_LOG(WARN, "fsync dir failed", K(ret), K(dir_fd), K(fname), KERRMSG);
  } else {
    CLOG_LOG(INFO, "unlink file success", K(fname), K(dir_fd));
    (void)ATOMIC_AAF(&free_quota_, file_size_);
    (void)ATOMIC_AAF(&limit_free_quota_, file_size_);
  }
}

int ObLogWriteFilePool::fsync_dir(const int dir_fd)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(dir_fd < 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    if (0 != fsync(dir_fd)) {
      ret = OB_IO_ERROR;
      CLOG_LOG(WARN, "fsync dir error", K(ret), K(dir_fd), K(errno), KERRMSG);
    }
  }
  return ret;
}

int ObLogWriteFilePool::get_dir_name(const char* fname, char* dir_name, const int64_t len)
{
  int ret = OB_SUCCESS;
  if (NULL == fname || NULL == dir_name || 0 >= len) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    dir_name[len - 1] = '\0';
    (void)snprintf(dir_name, len - 1, "%s", fname);
    dirname(dir_name);
  }
  return ret;
}

int ObLogWriteFilePool::rename_file(
    const int src_dir_fd, const char* srcfile, const int dest_dir_fd, const char* destfile)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(srcfile) || OB_ISNULL(destfile) || OB_UNLIKELY(src_dir_fd < 0) || OB_UNLIKELY(dest_dir_fd < 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (0 != renameat(src_dir_fd, srcfile, dest_dir_fd, destfile)) {
    CLOG_LOG(WARN, "renameat error", K(ret), K(src_dir_fd), K(srcfile), K(dest_dir_fd), K(destfile), K(errno), KERRMSG);
    ret = OB_IO_ERROR;
  } else if (OB_FAIL(fsync_dir(src_dir_fd))) {
  } else if (dest_dir_fd != src_dir_fd) {
    ret = fsync_dir(dest_dir_fd);
  } else {
    // do nothing
  }
  return ret;
}
}  // end namespace clog
}  // end namespace oceanbase
