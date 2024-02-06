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

#include <sys/vfs.h>
#include <sys/statvfs.h>
#include <unistd.h>
#include <linux/falloc.h>
#include "share/ob_local_device.h"
#include "share/ob_errno.h"
#include "share/config/ob_server_config.h"
#include "share/ob_resource_limit.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/slog/ob_storage_logger_manager.h"

using namespace oceanbase::common;

namespace oceanbase {
namespace share {
const char *BLOCK_SSTBALE_DIR_NAME = "sstable";
const char *BLOCK_SSTBALE_FILE_NAME = "block_file";

/**
 * ---------------------------------------------ObLocalIOEvents---------------------------------------------------
 */
ObLocalIOEvents::ObLocalIOEvents()
  : complete_io_cnt_(0),
  io_events_(nullptr)
{
}

ObLocalIOEvents::~ObLocalIOEvents()
{
}

int64_t ObLocalIOEvents::get_complete_cnt() const
{
  return complete_io_cnt_;
}

int ObLocalIOEvents::get_ith_ret_code(const int64_t i) const
{
  int ret_code = -1;
  if (nullptr != io_events_ && i < complete_io_cnt_) {
    const int64_t res = static_cast<int64_t>(io_events_[i].res);
    if (res >= 0) {
      ret_code = 0;
    } else {
      ret_code = static_cast<int32_t>(-res);
    }
  } else {
    SHARE_LOG_RET(WARN, ret_code, "invalid member", KP(io_events_), K(i), K(complete_io_cnt_));
  }
  return ret_code;
}

int ObLocalIOEvents::get_ith_ret_bytes(const int64_t i) const
{
  const int64_t res = static_cast<int64_t>(io_events_[i].res);
  return (nullptr != io_events_ && i < complete_io_cnt_ && res >= 0) ? static_cast<int32_t>(res) : 0;
}

void *ObLocalIOEvents::get_ith_data(const int64_t i) const
{
  return (nullptr != io_events_ && i < complete_io_cnt_) ? io_events_[i].data : 0;
}


/**
 * ---------------------------------------------ObLocalDevice---------------------------------------------------
 */
ObLocalDevice::ObLocalDevice()
  : is_inited_(false),
    is_marked_(false),
    block_fd_(0),
    block_lock_(common::ObLatchIds::LOCAL_DEVICE_LOCK),
    block_size_(0),
    block_file_size_(0),
    disk_percentage_(0),
    free_block_push_pos_(0),
    free_block_pop_pos_(0),
    free_block_cnt_(0),
    total_block_cnt_(0),
    free_block_array_(nullptr),
    block_bitmap_(nullptr),
    allocator_(),
    iocb_pool_(),
    is_fs_support_punch_hole_(true)
{

  MEMSET(store_dir_, 0, sizeof(store_dir_));
  MEMSET(sstable_dir_, 0, sizeof(sstable_dir_));
  MEMSET(store_path_, 0, sizeof(store_path_));
}

ObLocalDevice::~ObLocalDevice()
{
  destroy();
}

int ObLocalDevice::init(const common::ObIODOpts &opts)
{
  int ret = OB_SUCCESS;
  const ObMemAttr mem_attr(OB_SYS_TENANT_ID, "LOCALDEVICE");
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    SHARE_LOG(WARN, "The local device has been inited, ", K(ret));
  } else if (OB_FAIL(allocator_.init(ObMallocAllocator::get_instance(),
                                     OB_MALLOC_MIDDLE_BLOCK_SIZE, mem_attr))) {
    SHARE_LOG(WARN, "Fail to init allocator", K(ret));
  } else if (OB_FAIL(iocb_pool_.init(allocator_, DEFUALT_PRE_ALLOCATED_IOCB_COUNT))) {
    SHARE_LOG(WARN, "Fail to init iocb pool", K(ret));
  } else if (0 == opts.opt_cnt_) {
    SHARE_LOG(INFO, "For utl_file usage, skip initializing block_file");
  } else {
    const char *store_dir = nullptr;
    const char *sstable_dir = nullptr;
    int64_t datafile_size = 0;
    int64_t block_size = 0;
    int64_t datafile_disk_percentage = 0;
    bool is_exist = false;
    int64_t media_id = 0;

    for (int64_t i = 0; OB_SUCC(ret) && i < opts.opt_cnt_; ++i) {
      if (0 == STRCMP(opts.opts_[i].key_, "data_dir")) {
        store_dir = opts.opts_[i].value_.value_str;
      } else if (0 == STRCMP(opts.opts_[i].key_, "sstable_dir")) {
        sstable_dir = opts.opts_[i].value_.value_str;
      } else if (0 == STRCMP(opts.opts_[i].key_, "block_size")) {
        block_size = opts.opts_[i].value_.value_int64;
      } else if (0 == STRCMP(opts.opts_[i].key_, "datafile_disk_percentage")) {
        datafile_disk_percentage = opts.opts_[i].value_.value_int64;
      } else if (0 == STRCMP(opts.opts_[i].key_, "datafile_size")) {
        datafile_size = opts.opts_[i].value_.value_int64;
      } else if (0 == STRCMP(opts.opts_[i].key_, "media_id")) {
        media_id = opts.opts_[i].value_.value_int64;
      } else {
        ret = OB_NOT_SUPPORTED;
        SHARE_LOG(WARN, "Not supported option, ", K(ret), K(i), K(opts.opts_[i].key_));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_ISNULL(store_dir) || 0 == STRLEN(store_dir)) {
        ret = OB_INVALID_ARGUMENT;
        SHARE_LOG(WARN, "invalid args", K(ret), KP(store_dir));
      } else if (RL_IS_ENABLED && datafile_size > RL_CONF.get_max_datafile_size()) {
        ret = OB_RESOURCE_OUT;
        SHARE_LOG(WARN, "block file size too large", K(ret), K(datafile_size),
            K(RL_CONF.get_max_datafile_size()));
      } else {
        block_size_ = block_size;
        block_file_size_ = datafile_size;
        disk_percentage_ = datafile_disk_percentage;
        STRNCPY(store_dir_, store_dir, STRLEN(store_dir));
        STRNCPY(sstable_dir_, sstable_dir, STRLEN(sstable_dir));
        media_id_ = media_id;
      }
    }
  }

  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }

  if (OB_UNLIKELY(!is_inited_)) {
    destroy();
  }

  return ret;
}

int ObLocalDevice::reconfig(const common::ObIODOpts &opts)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "The ObLocalDevice has not been inited, ", K(ret));
  } else {
    int64_t datafile_size = 0;
    int64_t datafile_disk_percentage = 0;
    int64_t reserved_size = 0;

    for (int64_t i = 0; OB_SUCC(ret) && i < opts.opt_cnt_; ++i) {
      if (0 == STRCMP(opts.opts_[i].key_, "datafile_disk_percentage")) {
        datafile_disk_percentage = opts.opts_[i].value_.value_int64;
      } else if (0 == STRCMP(opts.opts_[i].key_, "datafile_size")) {
        datafile_size = opts.opts_[i].value_.value_int64;
      } else if (0 == STRCMP(opts.opts_[i].key_, "reserved_size")) {
        reserved_size = opts.opts_[i].value_.value_int64;
      } else {
        ret = OB_NOT_SUPPORTED;
        SHARE_LOG(WARN, "Not supported option, ", K(ret), K(i), K(opts.opts_[i].key_));
      }
    }

    if (OB_SUCC(ret)) {
      lib::ObMutexGuard guard(block_lock_);
      int64_t new_datafile_size = block_file_size_;
      if (OB_FAIL(get_block_file_size(sstable_dir_, reserved_size, block_size_,
          datafile_size, datafile_disk_percentage, new_datafile_size))) {
        SHARE_LOG(WARN, "Fail to get block file size", K(ret), K(reserved_size), K(block_size_),
            K(datafile_size), K(datafile_disk_percentage));
      } else if (RL_IS_ENABLED && new_datafile_size > RL_CONF.get_max_datafile_size()) {
        ret = OB_RESOURCE_OUT;
        SHARE_LOG(WARN, "block file size too large", K(ret), K(datafile_size), K(new_datafile_size),
            K(RL_CONF.get_max_datafile_size()));
      } else if (OB_FAIL(resize_block_file(new_datafile_size))) {
        SHARE_LOG(WARN, "Fail to open block file, ", K(ret), K(new_datafile_size), K(datafile_size));
      }
    }
  }
  return ret;
}

int ObLocalDevice::get_config(ObIODOpts &opts)
{
  UNUSED(opts);
  return OB_NOT_SUPPORTED;
}

//bool &need_format_super_block
int ObLocalDevice::start(const common::ObIODOpts &opts)
{
  int ret = OB_SUCCESS;
  bool is_exist = true;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "The ObLocalDevice has not been inited, ", K(ret));
  } else if (1 != opts.opt_cnt_ || NULL == opts.opts_) {
    SHARE_LOG(WARN, "opts args is wrong ", K(opts.opt_cnt_));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(0 != STRCMP(opts.opts_[0].key_, "reserved size")
                      || opts.opts_[0].value_.value_int64 < 0)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid opt arguments", K(ret), K(opts.opts_[0].key_),
        K(opts.opts_[0].value_.value_int64));
  } else if (OB_FAIL(open_block_file(store_dir_, sstable_dir_, block_size_, block_file_size_,
      disk_percentage_, opts.opts_[0].value_.value_int64, is_exist))) {
    SHARE_LOG(WARN, "Fail to open block file, ", K(ret));
  } else {
    opts.opts_[0].set("need_format_super_block", !is_exist);
  }
  return ret;
}

void ObLocalDevice::destroy()
{
  if (nullptr != block_bitmap_) {
    ob_free(block_bitmap_);
    block_bitmap_ = nullptr;
  }
  if (nullptr != free_block_array_) {
    ob_free(free_block_array_);
    free_block_array_ = nullptr;
  }
  iocb_pool_.reset();
  allocator_.~ObFIFOAllocator();
  total_block_cnt_ = 0;
  free_block_cnt_ = 0;
  free_block_pop_pos_ = 0;
  free_block_push_pos_ = 0;
  block_size_ = 0;
  block_file_size_ = 0;
  if (block_fd_ > 0) {
    ::close(block_fd_);
  }
  block_fd_ = 0;
  is_inited_ = false;
  is_marked_ = false;
  is_fs_support_punch_hole_ = true;

  MEMSET(store_dir_, 0, sizeof(store_dir_));
  MEMSET(sstable_dir_, 0, sizeof(sstable_dir_));
}

//file/dir interfaces
int ObLocalDevice::open(const char *pathname, const int flags, const mode_t mode, ObIOFd &fd, common::ObIODOpts *opts)
{
  UNUSED(opts);
  int ret = OB_SUCCESS;
  int local_fd = 0;

  if (OB_ISNULL(pathname)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "Invalid pathname, ", K(ret), KP(pathname));
  } else if ((local_fd = ::open(pathname, flags, mode)) < 0) {
    ret = convert_sys_errno();
    // use DEBUG log level to avoid too many unnecessary logs
    SHARE_LOG(DEBUG, "Fail to open file, ", K(ret), K(pathname), K(flags), K(mode), K(local_fd), KERRMSG);
  } else {
    fd.first_id_ = ObIOFd::NORMAL_FILE_ID;
    fd.second_id_ = local_fd;
  }
  return ret;
}

int ObLocalDevice::close(const ObIOFd &fd)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(fd.is_block_file())) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "The block file does not need close, ", K(ret), K(fd));
  } else if (0 != ::close(static_cast<int32_t>(fd.second_id_))) {
    ret = convert_sys_errno();
    SHARE_LOG(WARN, "Fail to close file, ", K(ret), K(fd));
  }
  return ret;
}

int ObLocalDevice::mkdir(const char *pathname, mode_t mode)
{
  int ret = OB_SUCCESS;
  if (NULL == pathname || STRLEN(pathname) == 0) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid arguments.", K(pathname), K(ret));
  } else if (::mkdir(pathname, mode) != 0) {
    if (EEXIST == errno) {
      ret = OB_SUCCESS;
    } else {
      ret = convert_sys_errno();
      SHARE_LOG(WARN, "create directory failed.", K(pathname), K(errno), KERRMSG, K(ret));
    }
  }
  return ret;
}

int ObLocalDevice::rmdir(const char *pathname)
{
  int ret = OB_SUCCESS;
  ObIODFileStat f_stat;
  if (OB_ISNULL(pathname) || OB_UNLIKELY(0 == STRLEN(pathname))) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid arguments.", K(ret), KP(pathname));
  } else if (OB_FAIL(stat(pathname, f_stat))) {
    SHARE_LOG(WARN, "stat path fail", K(pathname), K(ret));
  } else if (!S_ISDIR(f_stat.mode_)) {
    ret = OB_NO_SUCH_FILE_OR_DIRECTORY;
    SHARE_LOG(WARN, "file path is not a directory.", K(pathname), K(ret));
  } else if (0 != ::rmdir(pathname)) {
    ret = convert_sys_errno();
    SHARE_LOG(WARN, "rmdir failed.", K(pathname), K(errno), KERRMSG, K(ret));
  }
  return ret;
}

int ObLocalDevice::unlink(const char *pathname)
{
  int ret = OB_SUCCESS;
  ObIODFileStat f_stat;
  if (OB_ISNULL(pathname) || OB_UNLIKELY(0 == STRLEN(pathname))) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid arguments.", K(pathname), K(ret));
  } else if (OB_FAIL(stat(pathname, f_stat))) {
    SHARE_LOG(WARN, "stat path fail", K(pathname), K(ret));
  } else if (!S_ISREG(f_stat.mode_)) {
    ret = OB_NO_SUCH_FILE_OR_DIRECTORY;
    SHARE_LOG(WARN, "file path is a directory.", K(pathname), K(ret));
  } else if (0 != ::unlink(pathname)){
    ret = convert_sys_errno();
    SHARE_LOG(WARN, "unlink file failed.", K(pathname), K(errno), KERRMSG, K(ret));
  }
  return ret;
}

int ObLocalDevice::rename(const char *oldpath, const char *newpath)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(oldpath) || OB_ISNULL(newpath)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "Invalid argument, ", K(ret), KP(oldpath), KP(newpath));
  } else {
    int sys_ret = 0;
    if (0 != (sys_ret = ::rename(oldpath, newpath))) {
      ret = convert_sys_errno();
      SHARE_LOG(WARN, "Fail to rename file, ", K(ret), K(sys_ret), KERRMSG);
    }
  }
  return ret;
}

int ObLocalDevice::seal_file(const ObIOFd &fd)
{
  UNUSED(fd);
  return OB_NOT_SUPPORTED;
}

int ObLocalDevice::scan_dir(const char *dir_name, int (*func)(const dirent *entry))
{
  int ret = OB_SUCCESS;
  DIR *open_dir = nullptr;
  struct dirent entry;
  struct dirent *result;

  if (OB_ISNULL(dir_name)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid argument", K(ret), K(dir_name));
  } else if (OB_ISNULL(open_dir = ::opendir(dir_name))) {
    if (ENOENT != errno) {
      ret = OB_FILE_NOT_OPENED;
      SHARE_LOG(WARN, "Fail to open dir, ", K(ret), K(dir_name));
    } else {
      ret = OB_NO_SUCH_FILE_OR_DIRECTORY;
      SHARE_LOG(WARN, "dir does not exist", K(ret), K(dir_name));
    }
  } else {
    while (OB_SUCC(ret) && NULL != open_dir) {
      if (0 != ::readdir_r(open_dir, &entry, &result)) {
        ret = convert_sys_errno();
        SHARE_LOG(WARN, "read dir error", K(ret), KERRMSG);
      } else if (NULL != result
          && 0 != STRCMP(entry.d_name, ".")
          && 0 != STRCMP(entry.d_name, "..")) {
        if (OB_FAIL((*func)(&entry))) {
          SHARE_LOG(WARN, "fail to operate dir entry", K(ret), K(dir_name));
        }
      } else if (NULL == result) {
        break;//end file
      }
    }
    //close dir
    if (NULL != open_dir) {
      ::closedir(open_dir);
    }
  }
  return ret;
}

int ObLocalDevice::is_tagging(const char *pathname, bool &is_tagging)
{
  UNUSED(pathname);
  UNUSED(is_tagging);
  return OB_NOT_SUPPORTED;
}

int ObLocalDevice::scan_dir(const char *dir_name, common::ObBaseDirEntryOperator &op)
{
  int ret = OB_SUCCESS;
  DIR *open_dir = nullptr;
  struct dirent entry;
  struct dirent *result;

  if (OB_ISNULL(dir_name)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid argument", K(ret), K(dir_name));
  } else if (OB_ISNULL(open_dir = ::opendir(dir_name))) {
    if (ENOENT != errno) {
      ret = OB_FILE_NOT_OPENED;
      SHARE_LOG(WARN, "Fail to open dir, ", K(ret), K(dir_name));
    } else {
      ret = OB_NO_SUCH_FILE_OR_DIRECTORY;
      SHARE_LOG(WARN, "dir does not exist", K(ret), K(dir_name));
    }
  } else {
    while (OB_SUCC(ret) && NULL != open_dir) {
      if (0 != ::readdir_r(open_dir, &entry, &result)) {
        ret = convert_sys_errno();
        SHARE_LOG(WARN, "read dir error", K(ret), KERRMSG);
      } else if (NULL != result
          && 0 != STRCMP(entry.d_name, ".")
          && 0 != STRCMP(entry.d_name, "..")) {
        if (OB_FAIL(op.func(&entry))) {
          SHARE_LOG(WARN, "fail to operate dir entry", K(ret), K(dir_name));
        }
      } else if (NULL == result) {
        break; //end file
      }
    }
    //close dir
    if (NULL != open_dir) {
      ::closedir(open_dir);
    }
  }
  return ret;
}

int ObLocalDevice::fsync(const ObIOFd &fd)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!fd.is_normal_file())) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid fd, not normal file, ", K(ret), K(fd));
  } else {
    int sys_ret = 0;
    if (0 != (sys_ret = ::fsync(static_cast<int32_t>(fd.second_id_)))) {
      ret = convert_sys_errno();
      SHARE_LOG(WARN, "Fail to fsync, ", K(ret), K(sys_ret), K(fd), KERRMSG);
    }
  }
  return ret;
}

int ObLocalDevice::fdatasync(const ObIOFd &fd)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!fd.is_normal_file())) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid fd, not normal file, ", K(ret), K(fd));
  } else {
    int sys_ret = 0;
    if (0 != (sys_ret = ::fdatasync(static_cast<int32_t>(fd.second_id_)))) {
      ret = convert_sys_errno();
      SHARE_LOG(WARN, "Fail to fdatasync, ", K(ret), K(sys_ret), K(fd), KERRMSG);
    }
  }
  return ret;
}

int ObLocalDevice::fallocate(const ObIOFd &fd, mode_t mode, const int64_t offset, const int64_t len)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!fd.is_normal_file())) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid args, not normal file", K(ret), K(fd));
  } else {
    int sys_ret = 0;
    if (0 != (sys_ret = ::fallocate(static_cast<int32_t>(fd.second_id_), mode, offset, len))) {
      ret = convert_sys_errno();
      SHARE_LOG(WARN, "fail to fallocate", K(ret), K(sys_ret), K(fd), K(offset), K(len), KERRMSG);
    }
  }
  return ret;
}

int ObLocalDevice::lseek(const ObIOFd &fd, const int64_t offset, const int whence, int64_t &result_offset)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!fd.is_normal_file())) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid args, not normal file", K(ret), K(fd));
  } else if (-1 == (result_offset = ::lseek(static_cast<int32_t>(fd.second_id_), offset, whence))) {
    ret = convert_sys_errno();
    SHARE_LOG(WARN, "fail to lseek", K(ret), K(fd), K(offset), K(errno), KERRMSG);
  }
  return ret;
}

int ObLocalDevice::truncate(const char *pathname, const int64_t len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(pathname) || STRLEN(pathname) == 0) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid argument", K(ret), KP(pathname));
  } else {
    int sys_ret = 0;
    if (0 != (sys_ret = ::truncate(pathname, len))) {
      ret = convert_sys_errno();
      SHARE_LOG(WARN, "fail to ftruncate", K(ret), K(sys_ret), K(pathname), K(len), KERRMSG);
    }
  }
  return ret;
}

int ObLocalDevice::exist(const char *pathname, bool &is_exist)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(pathname) || STRLEN(pathname) == 0) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid argument", K(ret), KP(pathname));
  } else {
    if (0 != ::access(pathname, F_OK)) {
      if (errno == ENOENT) {
        ret = OB_SUCCESS;
        is_exist = false;
      } else {
        ret = convert_sys_errno();
        SHARE_LOG(WARN, "Fail to access file, ", K(ret), K(pathname), K(errno), KERRMSG);
      }
    } else {
      is_exist = true;
    }
  }
  return ret;
}

int ObLocalDevice::stat(const char *pathname, ObIODFileStat &statbuf)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(pathname) || STRLEN(pathname) == 0) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid argument", K(ret), KP(pathname));
  } else {
    struct stat buf;
    if (0 != ::stat(pathname, &buf)) {
      ret = convert_sys_errno();
      SHARE_LOG(WARN, "Fail to stat file, ", K(ret), K(pathname), K(errno), KERRMSG);
    } else {
      statbuf.rdev_ = static_cast<uint64_t>(buf.st_rdev);
      statbuf.dev_ = static_cast<uint64_t>(buf.st_dev);
      statbuf.inode_ = static_cast<uint64_t>(buf.st_ino);
      statbuf.mask_ = 0; // local file system stat does not offer mask
      statbuf.mode_ = static_cast<uint32_t>(buf.st_mode);
      statbuf.nlink_ = static_cast<uint32_t>(buf.st_nlink);
      statbuf.uid_ = static_cast<uint64_t>(buf.st_uid);
      statbuf.gid_ = static_cast<uint64_t>(buf.st_gid);
      statbuf.size_ = static_cast<uint64_t>(buf.st_size);
      statbuf.block_cnt_ = static_cast<uint64_t>(buf.st_blocks);
      statbuf.block_size_ = static_cast<uint64_t>(buf.st_blksize);
      statbuf.atime_s_ = static_cast<int64_t>(buf.st_atim.tv_sec);
      statbuf.mtime_s_ = static_cast<int64_t>(buf.st_mtim.tv_sec);
      statbuf.ctime_s_ = static_cast<int64_t>(buf.st_ctim.tv_sec);
      statbuf.btime_s_ = INT64_MAX; // local file system stat does not offer birth time
    }
  }
  return ret;
}

int ObLocalDevice::fstat(const ObIOFd &fd, ObIODFileStat &statbuf)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!fd.is_normal_file())) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid args, not normal file", K(ret), K(fd));
  } else {
    struct stat buf;
    if (0 != ::fstat(static_cast<int32_t>(fd.second_id_), &buf)) {
      ret = convert_sys_errno();
      SHARE_LOG(WARN, "Fail to stat file, ", K(ret), K(fd), K(errno), KERRMSG);
    } else {
      statbuf.rdev_ = static_cast<uint64_t>(buf.st_rdev);
      statbuf.dev_ = static_cast<uint64_t>(buf.st_dev);
      statbuf.inode_ = static_cast<uint64_t>(buf.st_ino);
      statbuf.mask_ = 0; // local file system stat does not offer mask
      statbuf.mode_ = static_cast<uint32_t>(buf.st_mode);
      statbuf.nlink_ = static_cast<uint32_t>(buf.st_nlink);
      statbuf.uid_ = static_cast<uint64_t>(buf.st_uid);
      statbuf.gid_ = static_cast<uint64_t>(buf.st_gid);
      statbuf.size_ = static_cast<uint64_t>(buf.st_size);
      statbuf.block_cnt_ = static_cast<uint64_t>(buf.st_blocks);
      statbuf.block_size_ = static_cast<uint64_t>(buf.st_blksize);
      statbuf.atime_s_ = static_cast<int64_t>(buf.st_atim.tv_sec);
      statbuf.mtime_s_ = static_cast<int64_t>(buf.st_mtim.tv_sec);
      statbuf.ctime_s_ = static_cast<int64_t>(buf.st_ctim.tv_sec);
      statbuf.btime_s_ = INT64_MAX; // local file system stat does not offer birth time
    }
  }
  return ret;
}

//block interfaces
int ObLocalDevice::mark_blocks(ObIBlockIterator &block_iter)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "The local device has not been inited, ", K(ret));
  } else if (OB_UNLIKELY(is_marked_)) {
    ret = OB_INIT_TWICE;
    SHARE_LOG(WARN, "The local device has been masked, ", K(ret));
  } else {
    lib::ObMutexGuard guard(block_lock_);

    MEMSET(block_bitmap_, 0, sizeof(bool) * total_block_cnt_);

    for (int64_t i = 0; i < RESERVED_BLOCK_INDEX; ++i) {
      block_bitmap_[i] = 1;
    }

    ObIOFd block_id;
    int64_t block_idx = 0;
    while (OB_SUCC(ret)) {
      if (OB_SUCC(block_iter.get_next_block(block_id))) {
        block_idx = block_id.second_id_;
        if (block_idx >= total_block_cnt_ || block_idx == 0) {
          ret = OB_INVALID_DATA;
          SHARE_LOG(WARN, "Invalid block id,", K(block_idx), K_(total_block_cnt));
        } else {
          block_bitmap_[block_idx] = 1;
        }
      }
    }

    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }

    if (OB_FAIL(ret)) {
      MEMSET(block_bitmap_, 0, sizeof(bool) * total_block_cnt_);
    } else {
      free_block_cnt_ = 0;
      free_block_push_pos_ = 0;
      for (int64_t i = RESERVED_BLOCK_INDEX; i < total_block_cnt_; ++i) {
        if (!block_bitmap_[i]) {
          free_block_array_[free_block_push_pos_++] = i;
          free_block_cnt_++;
          try_punch_hole(i); // ignore ret on purpose
        }
      }
      is_marked_ = true;
      SHARE_LOG(INFO, "The local block device has been masked, ", K(ret), K_(free_block_cnt),
          K(total_block_cnt_));
    }
  }
  return ret;
}


int ObLocalDevice::alloc_block(const common::ObIODOpts *opts, ObIOFd &block_id)
{
  UNUSED(opts);
  int ret = OB_SUCCESS;
  int64_t block_idx = 0;

  lib::ObMutexGuard guard(block_lock_);
  if (OB_UNLIKELY(!is_inited_) || OB_UNLIKELY(!is_marked_)) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "The ObLocalDevice is not ready, ", K(ret), K(is_inited_), K(is_marked_));
  } else if (OB_UNLIKELY(free_block_cnt_ <= 0)) {
    ret = OB_SERVER_OUTOF_DISK_SPACE;
    SHARE_LOG(WARN, "Fail to alloc block", K(ret), K(free_block_cnt_), K(total_block_cnt_));
  } else {
    block_idx = free_block_array_[free_block_pop_pos_];
    if (0 == block_bitmap_[block_idx]) {
      block_bitmap_[block_idx] = 1;
    } else {
      ret = OB_ERR_UNEXPECTED;
      SHARE_LOG(ERROR, "unexpected error, bitmap isn't 0", K(ret), K(block_idx));
    }
    free_block_pop_pos_ = (free_block_pop_pos_ + 1) % total_block_cnt_;
    --free_block_cnt_;
    if (OB_SUCC(ret)) {
      block_id.first_id_ = 0;
      block_id.second_id_ = block_idx;
    }
  }

  return ret;
}

int ObLocalDevice::alloc_blocks(
    const common::ObIODOpts *opts,
    const int64_t count,
    common::ObIArray<ObIOFd> &blocks)
{
  UNUSED(opts);
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT || OB_UNLIKELY(!is_marked_)) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "The ObLocalDevice is not ready, ", K(ret), K(is_inited_), K(is_marked_));
  } else if (OB_UNLIKELY(0 == count)) {
    // do nothing
  } else {
    blocks.reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      ObIOFd tmp_fd;
      if (OB_FAIL(alloc_block(opts, tmp_fd)) && OB_SERVER_OUTOF_DISK_SPACE != ret) {
        SHARE_LOG(WARN, "Alloc block fail", K(ret));
      } else if (OB_SERVER_OUTOF_DISK_SPACE == ret) {
        ret = OB_SUCCESS;
        break;
      } else if (OB_FAIL(blocks.push_back(tmp_fd))) {
        SHARE_LOG(WARN, "Push free block fail", K(ret), K(tmp_fd));
      }
    }
  }
  return ret;
}

void ObLocalDevice::free_block(const ObIOFd &block_id)
{
  int ret = OB_SUCCESS;
  const int64_t block_idx = block_id.second_id_;
  lib::ObMutexGuard guard(block_lock_);
  if (OB_UNLIKELY(!is_inited_) || OB_UNLIKELY(!is_marked_)) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "The ObLocalDevice is not ready, ", K(ret), K(is_inited_), K(is_marked_));
  } else if (0 == block_idx || block_idx >= total_block_cnt_) {
    ret = OB_INVALID_DATA;
    SHARE_LOG(ERROR, "Invalid block id to free, ", K(ret), K(block_id));
  } else if (1 == block_bitmap_[block_idx]) {
    free_block_array_[free_block_push_pos_] = block_idx;
    free_block_push_pos_ = (free_block_push_pos_ + 1) % total_block_cnt_;
    block_bitmap_[block_idx] = 0;
    ++free_block_cnt_;
  } else {
    ret = OB_ERR_UNEXPECTED;
    SHARE_LOG(WARN, "unexpected error, bitmap isn't 1", K(ret), K(block_idx), K(block_id));
  }

  if (OB_SUCC(ret) && OB_FAIL(try_punch_hole(block_idx))) {
    SHARE_LOG(WARN, "try punch hole fail", K(ret), K(block_idx));
  }
}

int ObLocalDevice::try_punch_hole(const int64_t block_index)
{
  int ret = OB_SUCCESS;
  // FALLOC_FL_PUNCH_HOLE is only supported after glibc-2.17
  // https://bugzilla.redhat.com/show_bug.cgi?id=1476120
# if __linux && (__GLIBC__ > 2 || (__GLIBC__ == 2 && __GLIBC_MINOR__ >= 17))
  if (is_fs_support_punch_hole_ && GCONF._enable_block_file_punch_hole) {
    const int64_t len = block_size_;
    const int64_t offset = block_size_ * block_index;
    const int sys_ret = ::fallocate(block_fd_, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, offset, len);
    if (sys_ret < 0) {
      const int sys_err = errno;
      if (EOPNOTSUPP == sys_err) {
        // The file system containing the file referred to by fd does
        // not support this operation; or the mode is not supported
        // by the file system containing the file referred to by fd.
        is_fs_support_punch_hole_ = false;
        SHARE_LOG(WARN, "Punch hole not support", K(block_index), K(offset), K(len), K(sys_ret), K(sys_err), KERRMSG);
      } else {
        SHARE_LOG(ERROR, "Punch hole fail", K(block_index), K(offset), K(len), K(sys_ret), K(sys_err), KERRMSG);
      }
    } else {
      SHARE_LOG(INFO, "Punch hole success", K(block_index), K(offset), K(len));
    }
  }
#endif
  return ret;
}

int ObLocalDevice::fsync_block()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "The ObLocalDevice has not been inited, ", K(ret));
  } else {
    int sys_ret = 0;
    if (0 != (sys_ret = ::fsync(block_fd_))) {
      ret = OB_IO_ERROR;
      SHARE_LOG(WARN, "Fail to sync block file, ", K(ret), K(sys_ret), KERRMSG);
    }
  }
  return ret;
}

int ObLocalDevice::get_restart_sequence(uint32_t &restart_id) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "The ObLocalDevice has not been inited, ", K(ret));
  } else {
    restart_id = 0;
  }
  return ret;
}

int ObLocalDevice::mark_blocks(const common::ObIArray<ObIOFd> &blocks)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "The ObLocalDevice has not been inited, ", K(ret));
  } else if (OB_UNLIKELY(0 == blocks.count())) {
    // do nothing
  } else {
    ret = OB_NOT_SUPPORTED;
  }
  return ret;
}

//sync io interfaces
int ObLocalDevice::pread(
    const ObIOFd &fd,
    const int64_t offset,
    const int64_t size,
    void *buf,
    int64_t &read_size,
    common::ObIODPreadChecker *checker)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "The ObLocalDevice has not been inited, ", K(ret));
  } else if (fd.is_super_block()) {
    if (OB_UNLIKELY(0 != offset) || OB_ISNULL(checker)) {
      ret = OB_INVALID_ARGUMENT;
      SHARE_LOG(WARN, "super block read offset or checker is invalid", K(ret), K(offset), KP(checker));
    } else if (OB_FAIL(pread_impl(block_fd_, buf, size, 0, read_size))) {
      SHARE_LOG(WARN, "read main super block fail", K(ret), K(offset), K(size), KP(buf), K(block_fd_), KERRMSG);
    } else if (OB_FAIL(checker->do_check(buf, read_size))) {
      SHARE_LOG(WARN, "check main super block fail", K(ret), K(read_size), KP(buf));
    } else if (OB_FAIL(pread_impl(block_fd_, buf, size, block_size_, read_size))) {
      SHARE_LOG(WARN, "read main super block fail", K(ret), K(block_size_), K(size), KP(buf), K(block_fd_), KERRMSG);
    } else if (OB_FAIL(checker->do_check(buf, read_size))) {
      SHARE_LOG(WARN, "check backup super block fail", K(ret), K(read_size), KP(buf));
    }
  } else {
    if (fd.is_block_file()) {
      const int64_t block_file_offset = get_block_file_offset(fd, offset);
      if (OB_FAIL(pread_impl(block_fd_, buf, size, block_file_offset, read_size))) {
        SHARE_LOG(WARN, "failed to pread", K(ret), K(block_fd_), K(size), K(block_file_offset));
      }
    } else if (fd.is_normal_file()) {
      if (OB_FAIL(pread_impl(fd.second_id_, buf, size, offset, read_size))) {
        SHARE_LOG(WARN, "failed to pread", K(ret), K(fd), K(size), K(offset));
      }
    }

    if (read_size < 0) {
      ret = OB_IO_ERROR;
      SHARE_LOG(WARN, "Fail to pread fd, ", K(ret), K(read_size), K(offset), K(size), KP(buf), KERRMSG);
    }
    if (OB_SUCC(ret) && nullptr != checker) {
      if (OB_FAIL(checker->do_check(buf, read_size))) {
        SHARE_LOG(WARN, "check pread result fail", K(ret), KP(buf), K(read_size));
      }
    }
  }
  return ret;
}

int ObLocalDevice::pwrite(
    const ObIOFd &fd,
    const int64_t offset,
    const int64_t size,
    const void *buf,
    int64_t &write_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "The ObLocalDevice has not been inited, ", K(ret));
  } else if (fd.is_super_block()) {
    // ensure server entry data atomic write
    if (OB_UNLIKELY(0 != offset)) {
      ret = OB_INVALID_ARGUMENT;
      SHARE_LOG(WARN, "super block write offset must be 0", K(ret), K(offset));
    } else if (OB_FAIL(pwrite_impl(block_fd_, buf, size, 0, write_size))) {
        SHARE_LOG(WARN, "Fail to write main superblock, try backup", K(ret), K(write_size), K(offset), K(size), KP(buf));
        if (OB_FAIL(pwrite_impl(block_fd_, buf, size, block_size_, write_size))) {
          SHARE_LOG(WARN, "Neither main nor backup superblock write success!!!", K(ret), K(write_size), K(offset), K(size), KP(buf));
        }
    } else if (OB_UNLIKELY(OB_SUCCESS != pwrite_impl(block_fd_, buf, size, 1 * block_size_ + offset, write_size))) {
      // main superblock success, allow backup failure
      SHARE_LOG(WARN, "Fail to write backup superblock", K(write_size), K(offset), K(size), KP(buf));
    }
  } else {
    if (fd.is_block_file()) {
      const int64_t block_file_offset = get_block_file_offset(fd, offset);
      if (OB_FAIL(pwrite_impl(block_fd_, buf, size, block_file_offset, write_size))) {
        SHARE_LOG(WARN, "failed to pwrite", K(ret), K(block_fd_), K(size), K(block_file_offset));
      }
    } else if (fd.is_normal_file()) {
      if (OB_FAIL(pwrite_impl(fd.second_id_, buf, size, offset, write_size))) {
        SHARE_LOG(WARN, "failed to pwrite", K(ret), K(fd), K(size), K(offset));
      }
    }

    if (write_size < 0) {
      ret = OB_IO_ERROR;
      SHARE_LOG(WARN, "Fail to pwrite fd, ", K(ret), K(fd), K(write_size),
          K(offset), K(size), KP(buf), K(errno), KERRNOMSG(errno));
    }
  }

  return ret;
}

int ObLocalDevice::read(
  const ObIOFd &fd,
  void *buf,
  const int64_t size,
  int64_t &read_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "The ObLocalDevice has not been inited, ", K(ret));
  } else if (OB_UNLIKELY(!fd.is_normal_file())) {
    ret = OB_ERR_UNEXPECTED;
    SHARE_LOG(WARN, "fd is not normal file", K(ret), K(fd));
  } else if (-1 == (read_size = ::read(static_cast<int32_t>(fd.second_id_), buf, size))) {
    ret = convert_sys_errno();
    SHARE_LOG(WARN, "fail to read", K(ret), K(fd), K(size), K(errno), KERRMSG);
  }
  return ret;
}

int ObLocalDevice::write(
  const ObIOFd &fd,
  const void *buf,
  const int64_t size,
  int64_t &write_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "The ObLocalDevice has not been inited, ", K(ret));
  } else if (OB_UNLIKELY(!fd.is_normal_file())) {
    ret = OB_ERR_UNEXPECTED;
    SHARE_LOG(WARN, "fd is not normal file", K(ret), K(fd));
  } else if (-1 == (write_size = ::write(static_cast<int32_t>(fd.second_id_), buf, size))) {
    ret = convert_sys_errno();
    SHARE_LOG(WARN, "fail to read", K(ret), K(fd), K(size), K(errno), KERRMSG);
  }
  return ret;
}

//async io interfaces
int ObLocalDevice::io_setup(
    uint32_t max_events,
    common::ObIOContext *&io_context)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "The ObLocalDevice has not been inited, ", K(ret));
  } else if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObLocalIOContext)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SHARE_LOG(WARN, "Fail to allocate memory, ", K(ret));
  } else {
    int sys_ret = 0;
    ObLocalIOContext *local_context = nullptr;
    local_context = new (buf) ObLocalIOContext();
    if (0 != (sys_ret = ::io_setup(max_events, &(local_context->io_context_)))) {
      ret = OB_IO_ERROR;
      SHARE_LOG(WARN, "Fail to setup io context, ", K(ret), K(sys_ret), KERRMSG);
    } else {
      io_context = local_context;
    }
  }

  if (OB_FAIL(ret) && nullptr != buf) {
    allocator_.free(buf);
  }
  return ret;
}

int ObLocalDevice::io_destroy(common::ObIOContext *io_context)
{
  int ret = OB_SUCCESS;
  ObLocalIOContext *local_io_context = nullptr;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "The ObLocalDevice has not been inited, ", K(ret));
  } else if (OB_ISNULL(io_context)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "Invalid argument, ", KP(io_context));
  } else if (OB_ISNULL(local_io_context = dynamic_cast<ObLocalIOContext*> (io_context))) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "Invalid io context pointer, ", K(ret), KP(io_context));
  } else {
    int sys_ret = 0;
    if ((sys_ret = ::io_destroy(local_io_context->io_context_)) != 0) {
      ret = OB_IO_ERROR;
      SHARE_LOG(WARN, "Fail to destroy io context, ", K(ret), K(sys_ret), KERRMSG);
    } else {
      allocator_.free(io_context);
    }
  }
  return ret;
}

int ObLocalDevice::io_prepare_pwrite(
    const ObIOFd &fd,
    void *buf,
    size_t count,
    int64_t offset,
    ObIOCB *iocb,
    void *callback)
{
  int ret = OB_SUCCESS;
  ObLocalIOCB *local_iocb = nullptr;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "The ObLocalDevice has not been inited, ", K(ret));
  } else if (OB_ISNULL(buf) || OB_ISNULL(iocb)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "Invalid argument, ", K(ret), KP(buf), KP(iocb));
  } else if (OB_ISNULL(local_iocb = dynamic_cast<ObLocalIOCB*> (iocb))) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "Invalid iocb pointer, ", K(ret), KP(iocb));
  } else if (OB_UNLIKELY(fd.is_super_block())) {
    ret = OB_NOT_SUPPORTED;
    SHARE_LOG(WARN, "server entry doesn't support AIO", K(ret), K(fd));
  } else {
    if (fd.is_block_file()) {
      ::io_prep_pwrite(&(local_iocb->iocb_), block_fd_, buf, count, get_block_file_offset(fd, offset));
    } else {
      ::io_prep_pwrite(&(local_iocb->iocb_), static_cast<int32_t>(fd.second_id_), buf, count, offset);
    }
    local_iocb->iocb_.data = callback;
  }
  return ret;
}

int ObLocalDevice::io_prepare_pread(
  const ObIOFd &fd,
  void *buf,
  size_t count,
  int64_t offset,
  ObIOCB *iocb,
  void *callback)
{
  int ret = OB_SUCCESS;
  ObLocalIOCB *local_iocb = nullptr;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "The ObLocalDevice has not been inited, ", K(ret));
  } else if (OB_ISNULL(buf) || OB_ISNULL(iocb)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "Invalid argument, ", K(ret), KP(buf), KP(iocb));
  } else if (OB_ISNULL(local_iocb = dynamic_cast<ObLocalIOCB*> (iocb))) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "Invalid iocb pointer, ", K(ret), KP(iocb));
  } else if (OB_UNLIKELY(fd.is_super_block())) {
    ret = OB_NOT_SUPPORTED;
    SHARE_LOG(WARN, "server entry doesn't support AIO", K(ret), K(fd));
  } else {
    if (fd.is_block_file()) {
      ::io_prep_pread(&(local_iocb->iocb_), block_fd_, buf, count, get_block_file_offset(fd, offset));
    } else {
      ::io_prep_pread(&(local_iocb->iocb_), static_cast<int32_t>(fd.second_id_), buf, count, offset);
    }
    local_iocb->iocb_.data = callback;
  }

  return ret;
}

int ObLocalDevice::io_submit(
    common::ObIOContext *io_context,
    common::ObIOCB *iocb)
{
  int ret = OB_SUCCESS;
  ObTimeGuard time_guard("LocalDevice", 5000); //5ms
  ObLocalIOContext *local_io_context = nullptr;
  ObLocalIOCB *local_iocb = nullptr;
  struct iocb *iocbp = nullptr;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "The ObLocalDevice has not been inited, ", K(ret));
  } else if (OB_ISNULL(io_context) || OB_ISNULL(iocb)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "Invalid argument, ", KP(io_context), KP(iocb));
  } else if (OB_ISNULL(local_iocb = dynamic_cast<ObLocalIOCB*> (iocb))) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "Invalid iocb pointer, ", K(ret), KP(iocb));
  } else if (OB_ISNULL(local_io_context = dynamic_cast<ObLocalIOContext*> (io_context))) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "Invalid io context pointer, ", K(ret), KP(io_context));
  } else {
    iocbp = &(local_iocb->iocb_);
    int submit_ret = ::io_submit(local_io_context->io_context_, 1, &iocbp);
    time_guard.click("LocalDevice_submit");
    if (1 != submit_ret) {
      ret = OB_IO_ERROR;
      SHARE_LOG(WARN, "Fail to submit aio, ", K(ret), K(submit_ret), K(errno), KERRMSG);
    }
  }
  return ret;
}

int ObLocalDevice::io_cancel(
    common::ObIOContext *io_context,
    common::ObIOCB *iocb)
{
  int ret = OB_SUCCESS;
  ObLocalIOContext *local_io_context = nullptr;
  ObLocalIOCB *local_iocb = nullptr;
  struct io_event local_event;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "The ObLocalDevice has not been inited, ", K(ret));
  } else if (OB_ISNULL(io_context) || OB_ISNULL(iocb)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "Invalid argument, ", KP(io_context),KP(iocb));
  } else if (OB_ISNULL(local_iocb = dynamic_cast<ObLocalIOCB*> (iocb))) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "Invalid iocb pointer, ", K(ret), KP(iocb));
  } else if (OB_ISNULL(local_io_context = dynamic_cast<ObLocalIOContext*> (io_context))) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "Invalid io context pointer, ", K(ret), KP(io_context));
  } else {
    int sys_ret = 0;
    if ((sys_ret = ::io_cancel(local_io_context->io_context_, &(local_iocb->iocb_), &local_event)) < 0) {
      ret = OB_IO_ERROR;
      SHARE_LOG(DEBUG, "Fail to cancel aio, ", K(ret), K(sys_ret), KERRMSG);
    }
  }
  return ret;
}

int ObLocalDevice::io_getevents(
    common::ObIOContext *io_context,
    int64_t min_nr,
    common::ObIOEvents *events,
    struct timespec *timeout)
{
  int ret = OB_SUCCESS;
  ObLocalIOContext *local_io_context = nullptr;
  ObLocalIOEvents *local_io_events = nullptr;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "The ObLocalDevice has not been inited, ", K(ret));
  } else if (OB_ISNULL(io_context) || OB_ISNULL(events)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "Invalid argument, ", KP(io_context),KP(events));
  } else if (OB_ISNULL(local_io_events = dynamic_cast<ObLocalIOEvents*> (events))) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "Invalid io events pointer, ", K(ret), KP(events));
  } else if (OB_ISNULL(local_io_context = dynamic_cast<ObLocalIOContext*> (io_context))) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "Invalid io context pointer, ", K(ret), KP(io_context));
  } else {
    int sys_ret = 0;
    {
      oceanbase::lib::Thread::WaitGuard guard(oceanbase::lib::Thread::WAIT_FOR_IO_EVENT);
      while ((sys_ret = ::io_getevents(
          local_io_context->io_context_,
          min_nr,
          local_io_events->max_event_cnt_,
          local_io_events->io_events_,
          timeout)) < 0 && -EINTR == sys_ret); // ignore EINTR
    }
    if (sys_ret < 0) {
      ret = OB_IO_ERROR;
      SHARE_LOG(WARN, "Fail to get io events, ", K(ret), K(sys_ret), KERRMSG);
    } else {
      local_io_events->complete_io_cnt_ = sys_ret;
    }
  }
  return ret;
}

common::ObIOCB* ObLocalDevice::alloc_iocb()
{
  ObLocalIOCB *iocb = nullptr;
  ObLocalIOCB *buf = nullptr;
  if (OB_LIKELY(is_inited_)) {
    if (NULL != (buf = iocb_pool_.alloc())) {
      iocb = new (buf) ObLocalIOCB();
    }
  }
  return iocb;
}

common::ObIOEvents *ObLocalDevice::alloc_io_events(const uint32_t max_events)
{
  ObLocalIOEvents *io_events = nullptr;
  char *buf = nullptr;
  int64_t size = 0;

  if (OB_LIKELY(is_inited_)) {
    size = sizeof(ObLocalIOEvents) + max_events * sizeof(struct io_event);
    if (NULL != (buf = (char*) allocator_.alloc(size))) {
      MEMSET(buf, 0, size);
      io_events = new (buf) ObLocalIOEvents();
      io_events->max_event_cnt_ = max_events;
      io_events->complete_io_cnt_ = 0;
      io_events->io_events_ = reinterpret_cast<struct io_event*> (buf + sizeof(ObLocalIOEvents));
    }
  }
  return io_events;
}

void ObLocalDevice::free_iocb(common::ObIOCB *iocb)
{
  if (OB_LIKELY(is_inited_)) {
    ObLocalIOCB *local_iocb = static_cast<ObLocalIOCB *>(iocb);
    local_iocb->~ObLocalIOCB();
    iocb_pool_.free(local_iocb);
  }
}

void ObLocalDevice::free_io_events(common::ObIOEvents *io_event)
{
  if (OB_LIKELY(is_inited_)) {
    allocator_.free(io_event);
  }
}

int64_t ObLocalDevice::get_total_block_size() const
{
  return block_file_size_;
}

int64_t ObLocalDevice::get_free_block_count() const
{
  return free_block_cnt_;
}

int64_t ObLocalDevice::get_reserved_block_count() const
{
  return 2;// reserved only for supper block.
}

int64_t ObLocalDevice::get_max_block_count(int64_t reserved_size) const
{
  return block_size_ > 0 ? get_max_block_size(reserved_size) / block_size_ : 0;
}

int64_t ObLocalDevice::get_max_block_size(int64_t reserved_size) const
{
  int ret = OB_SUCCESS;
  int64_t ret_size = 0;
  struct statvfs svfs;

  const int64_t config_max_file_size = GCONF.datafile_maxsize;
  int64_t block_file_max_size = block_file_size_;

  if (config_max_file_size < block_file_max_size) {
    // auto extend is off
  } else if (OB_ISNULL(sstable_dir_)) {
    ret = OB_ERR_UNEXPECTED;
    SHARE_LOG(WARN, "Failed to get max block size", K(ret), K(sstable_dir_));
  } else if (OB_UNLIKELY(0 != statvfs(sstable_dir_, &svfs))) {
    ret = convert_sys_errno();
    SHARE_LOG(WARN, "Failed to get disk space", K(ret), K(sstable_dir_));
  } else {
    const int64_t free_space = std::max(0L, (int64_t)(svfs.f_bavail * svfs.f_bsize));
    const int64_t max_file_size = block_file_size_ + free_space - reserved_size;
    /* when datafile_maxsize is large than current datafile_size, we should return
       the Maximun left space that can be extend. */
    if (max_file_size > config_max_file_size) {
      block_file_max_size = lower_align(config_max_file_size, block_size_);
    } else {
      block_file_max_size = lower_align(max_file_size, block_size_);
    }
  }
  // still return current block file size when ret=fail
  return block_file_max_size;
}

int ObLocalDevice::check_space_full(const int64_t required_size) const
{
  int ret = OB_SUCCESS;
  int64_t reserved_size = 4 * 1024 * 1024 * 1024L; // default RESERVED_DISK_SIZE -> 4G

  if (OB_UNLIKELY(!is_marked_)) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "The ObLocalDevice has not been marked", K(ret));
  } else if (OB_UNLIKELY(required_size < 0)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid argument", K(ret), K(required_size));
  } else if (OB_FAIL(SLOGGERMGR.get_reserved_size(reserved_size))) {
    SHARE_LOG(WARN, "Fail to get reserved size", K(ret));
  } else {
    int64_t max_block_cnt = get_max_block_count(reserved_size);
    int64_t actual_free_block_cnt = free_block_cnt_;
    if (max_block_cnt > total_block_cnt_) {  // auto extend is on
      actual_free_block_cnt = max_block_cnt - total_block_cnt_ + free_block_cnt_;
    }
    const int64_t NO_LIMIT_PERCENT = 100;
    const int64_t required_count = required_size / block_size_;
    const int64_t free_count = actual_free_block_cnt - required_count;
    const int64_t used_percent = 100 - 100 * free_count / total_block_cnt_;
    if (GCONF.data_disk_usage_limit_percentage != NO_LIMIT_PERCENT
        && used_percent >= GCONF.data_disk_usage_limit_percentage) {
      ret = OB_SERVER_OUTOF_DISK_SPACE;
      if (REACH_TIME_INTERVAL(24 * 3600LL * 1000 * 1000 /* 24h */)) {
        LOG_DBA_ERROR(OB_SERVER_OUTOF_DISK_SPACE, "msg", "disk is almost full", K(ret), K(required_size),
            K(required_count), K(free_count), K(used_percent));
      }
    }
  }
  return ret;
}

int ObLocalDevice::get_block_file_size(
    const char *sstable_dir,
    const int64_t reserved_size,
    const int64_t block_size,
    const int64_t suggest_file_size,
    const int64_t disk_percentage,
    int64_t &block_file_size)
{
  int ret = OB_SUCCESS;
  // check space availability if file not exist
  int64_t total_space = 0;
  int64_t free_space = 0;
  struct statvfs svfs;

  if (OB_ISNULL(sstable_dir)
      || OB_UNLIKELY(reserved_size < 0)
      || OB_UNLIKELY(block_size <= 0)
      || OB_UNLIKELY(suggest_file_size < 0)
      || OB_UNLIKELY(disk_percentage < 0)
      || OB_UNLIKELY(disk_percentage >= 100)
      || OB_UNLIKELY(block_file_size < 0)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "Invalid argument, ", K(ret), KP(sstable_dir), K(reserved_size), K(block_size),
        K(suggest_file_size), K(disk_percentage), K(block_file_size));
  } else if (OB_UNLIKELY(0 != statvfs(sstable_dir, &svfs))) {
    ret = convert_sys_errno();
    SHARE_LOG(WARN, "Failed to get disk space ", K(ret), K(sstable_dir));
  } else {
    const int64_t old_block_file_size = block_file_size;
    // remove reserved space for root user
    total_space = std::max(0L, (int64_t)((svfs.f_blocks + svfs.f_bavail - svfs.f_bfree) * svfs.f_bsize - reserved_size));
    free_space = std::max(0L, (int64_t)(svfs.f_bavail * svfs.f_bsize - reserved_size));
    block_file_size = suggest_file_size > 0 ? suggest_file_size : total_space * disk_percentage / 100;
    if (block_file_size > old_block_file_size + free_space) {
      ret = OB_SERVER_OUTOF_DISK_SPACE;
      LOG_DBA_ERROR(OB_SERVER_OUTOF_DISK_SPACE, "msg", "data file size is too large", K(ret), K(block_file_size_), K(total_space),
          K(free_space), K(reserved_size), K(old_block_file_size), K(block_file_size));
    } else if (block_file_size <= block_size) {
      ret = OB_INVALID_ARGUMENT;
      SHARE_LOG(ERROR, "data file size is too small, ", K(ret), K(total_space), K(free_space),
          K(reserved_size), K(old_block_file_size), K(block_file_size));
    } else {
      block_file_size = lower_align(block_file_size, block_size);
    }
  }
  return ret;
}

int ObLocalDevice::open_block_file(
    const char *store_dir,
    const char *sstable_dir,
    const int64_t block_size,
    const int64_t file_size,
    const int64_t disk_percentage,
    const int64_t reserved_size,
    bool &is_exist)
{
  int ret = OB_SUCCESS;
  int sys_ret = 0;
  is_exist = false;
  int64_t adjust_file_size = 0; // The original data file size is 0 because of the first initialization.

  if (OB_FAIL(databuff_printf(store_path_, OB_MAX_FILE_NAME_LENGTH, "%s/%s/%s",
    store_dir, BLOCK_SSTBALE_DIR_NAME, BLOCK_SSTBALE_FILE_NAME))) {
    SHARE_LOG(WARN, "The block file path is too long, ", K(ret), K(store_dir));
  } else if (OB_FAIL(exist(store_path_, is_exist))) {
    SHARE_LOG(WARN, "Fail to check if file exist, ", K(ret), K(store_path_));
  } else if (!is_exist
      && OB_FAIL(get_block_file_size(sstable_dir, reserved_size, block_size,
          file_size, disk_percentage, adjust_file_size))) {
    SHARE_LOG(WARN, "Fail to get block file size", K(ret), K(block_size), K(file_size),
        K(adjust_file_size), K(disk_percentage));
  } else {
    int open_flag = is_exist ? O_DIRECT | O_RDWR | O_LARGEFILE
                          : O_CREAT | O_EXCL | O_DIRECT | O_RDWR | O_LARGEFILE;
    if ((block_fd_ = ::open(store_path_, open_flag, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH)) < 0) {
      ret = OB_IO_ERROR;
      SHARE_LOG(ERROR, "open file error", K(ret), K(store_path_), K(errno), KERRMSG);
    } else {
      if (!is_exist) {
        if (0 != (sys_ret = ::fallocate(block_fd_, 0/*MODE*/, 0/*offset*/, adjust_file_size))) {
          ret = convert_sys_errno();
          SHARE_LOG(ERROR, "Fail to fallocate block file, ", K(ret), K(sys_ret), K(store_path_), K(adjust_file_size), KERRMSG);
        } else {
          block_file_size_ = adjust_file_size;
        }
      } else {
        ObIODFileStat f_stat;
        if (OB_FAIL(stat(store_path_, f_stat))) {
          SHARE_LOG(ERROR, "stat store_path_ fail ", K(ret), K(store_path_));
        } else {
          block_file_size_ = lower_align(f_stat.size_, block_size);
        }
      }
    }

    if (OB_SUCC(ret)) {
      const ObMemAttr mem_attr(OB_SYS_TENANT_ID, "LOCALDEVICE");
      total_block_cnt_ = block_file_size_ / block_size_;
      if (OB_ISNULL(free_block_array_ = (int64_t *) ob_malloc(sizeof(int64_t) * total_block_cnt_, mem_attr))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SHARE_LOG(WARN, "Fail to allocate memory, ", K(ret), K(block_file_size_), K(total_block_cnt_));
      } else if (OB_ISNULL(block_bitmap_ = (bool *) ob_malloc(sizeof(bool) * total_block_cnt_, mem_attr))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SHARE_LOG(WARN, "Fail to allocate memory, ", K(ret), K(block_file_size_), K(total_block_cnt_));
      } else {
        free_block_cnt_ = 0;
        free_block_push_pos_ = 0;
        free_block_pop_pos_ = 0;
      }
    }
  }

  return ret;
}

int ObLocalDevice::resize_block_file(const int64_t new_size)
{
  // copy free block info to new_free_block_array
  int ret = OB_SUCCESS;
  int sys_ret = 0;
  const ObMemAttr mem_attr(OB_SYS_TENANT_ID, "LOCALDEVICE");
  int64_t new_total_block_cnt = new_size / block_size_;
  int64_t *new_free_block_array = nullptr;
  bool *new_block_bitmap = nullptr;
  int64_t delta_size = new_size - block_file_size_;

  if (delta_size < 0) {
    ret = OB_NOT_SUPPORTED;
    SHARE_LOG(WARN, "Can not make the block file smaller, ", K(ret), K(new_size),
        K(block_file_size_));
  } else if (0 == delta_size) {
    SHARE_LOG(INFO, "The file size is not changed, ", K(new_size), K(block_file_size_));
  } else if (0 != (sys_ret = ::fallocate(block_fd_, 0, block_file_size_, delta_size))) {
    ret = convert_sys_errno();;
    SHARE_LOG(WARN, "fail to expand file size", K(ret), K(sys_ret), K(block_file_size_),
        K(delta_size), K(errno), KERRMSG);
  } else if (OB_ISNULL(new_free_block_array
      = (int64_t *) ob_malloc(sizeof(int64_t) * new_total_block_cnt, mem_attr))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SHARE_LOG(WARN, "Fail to allocate memory, ", K(ret), K(new_size), K(new_total_block_cnt));
  } else if (OB_ISNULL(new_block_bitmap
      = (bool *) ob_malloc(sizeof(bool) * new_total_block_cnt, mem_attr))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SHARE_LOG(WARN, "Fail to allocate memory, ", K(ret), K(new_size), K(new_total_block_cnt));
  } else {
    if (free_block_pop_pos_ > free_block_push_pos_) {
      MEMCPY(new_free_block_array, free_block_array_ + free_block_pop_pos_,
          (total_block_cnt_ - free_block_pop_pos_) * sizeof(int64_t));
      MEMCPY(new_free_block_array +  total_block_cnt_ - free_block_pop_pos_, free_block_array_,
          free_block_push_pos_ * sizeof(int64_t));
    } else if (free_block_pop_pos_ < free_block_push_pos_) {
      MEMCPY(new_free_block_array, free_block_array_ + free_block_pop_pos_,
          (free_block_push_pos_ - free_block_pop_pos_) * sizeof(int64_t));
    } else {
      MEMCPY(new_free_block_array, free_block_array_, free_block_cnt_ * sizeof(int64_t));
    }

    free_block_pop_pos_ = 0;
    free_block_push_pos_ = free_block_cnt_;
    MEMSET(new_block_bitmap, 0, sizeof(bool) * new_total_block_cnt);
    MEMCPY(new_block_bitmap, block_bitmap_, sizeof(bool) * total_block_cnt_);

    ob_free(free_block_array_);
    ob_free(block_bitmap_);
    free_block_array_ = new_free_block_array;
    block_bitmap_ = new_block_bitmap;
    block_file_size_ = new_size;
    for (int64_t i = total_block_cnt_; i < new_total_block_cnt; ++i) {
      if (!block_bitmap_[i]) {
        free_block_array_[free_block_push_pos_++] = i;
        free_block_cnt_++;
      }
    }
    total_block_cnt_ = new_total_block_cnt;
    SHARE_LOG(INFO, "succeed to resize file", K(new_size), K(total_block_cnt_), K(free_block_cnt_));
  }

  if (OB_FAIL(ret)) {
    if (nullptr != new_free_block_array) {
      ob_free(new_free_block_array);
      new_free_block_array = nullptr;
    }
    if (nullptr != new_block_bitmap) {
      ob_free(new_block_bitmap);
      new_block_bitmap = nullptr;
    }
  }

  return ret;
}

int ObLocalDevice::pread_impl(const int64_t fd, void *buf, const int64_t size, const int64_t offset, int64_t &read_size)
{
  int ret = OB_SUCCESS;
  read_size = 0;
  char *buffer = static_cast<char *>(buf);
  int64_t read_sz = size;
  int64_t read_offset = offset;
  ssize_t sz = 0;
  while (OB_SUCC(ret) && read_sz > 0) {
    sz = ::pread(static_cast<int32_t>(fd), buffer, read_sz, read_offset);
    if (sz < 0) {
      if (EINTR == errno) {
        SHARE_LOG(INFO, "pread is interrupted before any data is read, just retry", K(errno), KERRMSG);
      } else {
        ret = OB_IO_ERROR;
        SHARE_LOG(WARN, "failed to pread", K(ret), K(fd), K(read_sz), K(read_offset), K(errno), KERRMSG);
      }
    } else if (0 == sz) {
      // actual read size is 0, may reach the end of file
      SHARE_LOG(INFO, "read nothing because the end of file is reached", K(ret), K(read_sz), K(read_offset));
      break;
    } else {
      buffer += sz;
      read_sz -= sz;
      read_offset += sz;
      read_size += sz;
    }
  }
  return ret;
}

int ObLocalDevice::pwrite_impl(const int64_t fd, const void *buf, const int64_t size, const int64_t offset, int64_t &write_size)
{
  int ret = OB_SUCCESS;
  write_size = 0;
  const char *buffer = static_cast<const char *>(buf);
  int64_t write_sz = size;
  int64_t write_offset = offset;
  ssize_t sz = 0;
  while (OB_SUCC(ret) && write_sz > 0) {
    sz = ::pwrite(static_cast<int32_t>(fd), buffer, write_sz, write_offset);
    if (sz <= 0) {
      // if physical end of medium is reached and there's no space for any byte, EFBIG is set
      // and we think pwrite will never return 0
      if (EINTR == errno) {
        SHARE_LOG(INFO, "pwrite is interrupted before any data is written, just retry", K(errno), KERRMSG);
      } else {
        ret = OB_IO_ERROR;
        SHARE_LOG(WARN, "failed to pwrite", K(ret), K(fd), K(write_sz), K(write_offset), K(errno), KERRMSG);
      }
    } else {
      buffer += sz;
      write_sz -= sz;
      write_offset += sz;
      write_size += sz;
    }
  }
  return ret;
}

int ObLocalDevice::convert_sys_errno()
{
  int ret = OB_IO_ERROR;
  bool use_warn_log = false;
  switch (errno) {
    case EACCES:
      ret = OB_FILE_OR_DIRECTORY_PERMISSION_DENIED;
      break;
    case ENOENT:
      ret = OB_NO_SUCH_FILE_OR_DIRECTORY;
      break;
    case EEXIST:
    case ENOTEMPTY:
      ret = OB_FILE_OR_DIRECTORY_EXIST;
      break;
    case ETIMEDOUT:
      ret = OB_TIMEOUT;
      break;
    case EAGAIN:
      ret = OB_EAGAIN;
      break;
    default:
      use_warn_log = true;
      break;
  }
  if (use_warn_log) {
    SHARE_LOG(WARN, "convert sys errno", K(ret), K(errno), KERRMSG);
  } else {
    SHARE_LOG(INFO, "convert sys errno", K(ret), K(errno), KERRMSG);
  }
  return ret;
}
} /* namespace share */
} /* namespace oceanbase */
