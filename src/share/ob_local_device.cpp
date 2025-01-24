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

#include "ob_local_device.h"
#include <sys/vfs.h>
#include <sys/statvfs.h>
#include <unistd.h>
#include <linux/falloc.h>
#include "share/ob_resource_limit.h"
#include "storage/slog/ob_storage_logger_manager.h"
#include "lib/ash/ob_active_session_guard.h"
#include "storage/meta_store/ob_server_storage_meta_service.h"

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
  int ret_val = 0;
  if (nullptr != io_events_ && i < complete_io_cnt_) {
    const int64_t res = static_cast<int64_t>(io_events_[i].res);
    ret_val = res >= 0 ? static_cast<int32_t>(res) : 0;
  }
  return ret_val;
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
  const ObMemAttr mem_attr = SET_IGNORE_MEM_VERSION(ObMemAttr(OB_SYS_TENANT_ID, "LDIOSetup"));
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
  SHARE_LOG(INFO, "init local device", K(ret), KP(this), K(lbt()));

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
      if (OB_FAIL(ObIODeviceLocalFileOp::get_block_file_size(sstable_dir_, reserved_size, block_size_,
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
  } else {
    BlockFileAttr block_file_attr(store_path_, BLOCK_SSTBALE_DIR_NAME, BLOCK_SSTBALE_FILE_NAME,
      block_fd_, block_file_size_, block_size_, total_block_cnt_, free_block_array_, block_bitmap_,
      free_block_cnt_, free_block_push_pos_, free_block_pop_pos_, "LDBlockBitMap");
    if (OB_FAIL(ObIODeviceLocalFileOp::open_block_file(store_dir_, sstable_dir_, block_size_,
        block_file_size_, disk_percentage_, opts.opts_[0].value_.value_int64, is_exist, block_file_attr))) {
      SHARE_LOG(WARN, "Fail to open block file, ", K(ret));
    } else {
      opts.opts_[0].set("need_format_super_block", !is_exist);
    }
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
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIODeviceLocalFileOp::open(pathname, flags, mode, fd, opts))) {
    SHARE_LOG(WARN, "Fail to open", K(ret), K(pathname), K(flags), K(mode), K(fd));
  }
  return ret;
}

int ObLocalDevice::complete(const ObIOFd &fd)
{
  UNUSED(fd);
  return OB_NOT_SUPPORTED;
}

int ObLocalDevice::abort(const ObIOFd &fd)
{
  UNUSED(fd);
  return OB_NOT_SUPPORTED;
}

int ObLocalDevice::close(const ObIOFd &fd)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIODeviceLocalFileOp::close(fd))) {
    SHARE_LOG(WARN, "Fail to close", K(ret), K(fd));
  }
  return ret;
}

int ObLocalDevice::mkdir(const char *pathname, mode_t mode)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIODeviceLocalFileOp::mkdir(pathname, mode))) {
    SHARE_LOG(WARN, "Fail to mkdir", K(ret), K(pathname), K(mode));
  }
  return ret;
}

int ObLocalDevice::rmdir(const char *pathname)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIODeviceLocalFileOp::rmdir(pathname))) {
    SHARE_LOG(WARN, "Fail to rmdir", K(ret), K(pathname));
  }
  return ret;
}

int ObLocalDevice::unlink(const char *pathname)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIODeviceLocalFileOp::unlink(pathname))) {
    SHARE_LOG(WARN, "Fail to unlink", K(ret), K(pathname));
  }
  return ret;
}

int ObLocalDevice::batch_del_files(
    const ObIArray<ObString> &files_to_delete, ObIArray<int64_t> &failed_files_idx)
{
  UNUSED(files_to_delete);
  UNUSED(failed_files_idx);
  return OB_NOT_SUPPORTED;
}

int ObLocalDevice::rename(const char *oldpath, const char *newpath)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIODeviceLocalFileOp::rename(oldpath, newpath))) {
    SHARE_LOG(WARN, "Fail to rename", K(ret), K(oldpath), K(newpath));
  }
  return ret;
}

int ObLocalDevice::seal_file(const ObIOFd &fd)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIODeviceLocalFileOp::seal_file(fd))) {
    SHARE_LOG(WARN, "Fail to seal_file", K(ret), K(fd));
  }
  return ret;
}

int ObLocalDevice::scan_dir(const char *dir_name, int (*func)(const dirent *entry))
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIODeviceLocalFileOp::scan_dir(dir_name, func))) {
    SHARE_LOG(WARN, "Fail to scan_dir", K(ret), K(dir_name));
  }
  return ret;
}

int ObLocalDevice::is_tagging(const char *pathname, bool &is_tagging)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIODeviceLocalFileOp::is_tagging(pathname, is_tagging))) {
    SHARE_LOG(WARN, "Fail to check is_tagging", K(ret), K(pathname));
  }
  return ret;
}

int ObLocalDevice::scan_dir(const char *dir_name, common::ObBaseDirEntryOperator &op)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIODeviceLocalFileOp::scan_dir(dir_name, op))) {
    SHARE_LOG(WARN, "Fail to scan_dir", K(ret), K(dir_name));
  }
  return ret;
}

int ObLocalDevice::fsync(const ObIOFd &fd)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIODeviceLocalFileOp::fsync(fd))) {
    SHARE_LOG(WARN, "Fail to fsync", K(ret), K(fd));
  }
  return ret;
}

int ObLocalDevice::fdatasync(const ObIOFd &fd)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIODeviceLocalFileOp::fdatasync(fd))) {
    SHARE_LOG(WARN, "Fail to fdatasync", K(ret), K(fd));
  }
  return ret;
}

int ObLocalDevice::fallocate(const ObIOFd &fd, mode_t mode, const int64_t offset, const int64_t len)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIODeviceLocalFileOp::fallocate(fd, mode, offset, len))) {
    SHARE_LOG(WARN, "Fail to fallocate", K(ret), K(fd), K(mode), K(offset), K(len));
  }
  return ret;
}

int ObLocalDevice::lseek(const ObIOFd &fd, const int64_t offset, const int whence, int64_t &result_offset)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIODeviceLocalFileOp::lseek(fd, offset, whence, result_offset))) {
    SHARE_LOG(WARN, "Fail to lseek", K(ret), K(fd), K(offset), K(whence));
  }
  return ret;
}

int ObLocalDevice::truncate(const char *pathname, const int64_t len)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIODeviceLocalFileOp::truncate(pathname, len))) {
    SHARE_LOG(WARN, "Fail to truncate", K(ret), K(pathname), K(len));
  }
  return ret;
}

int ObLocalDevice::exist(const char *pathname, bool &is_exist)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIODeviceLocalFileOp::exist(pathname, is_exist))) {
    SHARE_LOG(WARN, "Fail to check is exist", K(ret), K(pathname));
  }
  return ret;
}

int ObLocalDevice::stat(const char *pathname, ObIODFileStat &statbuf)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIODeviceLocalFileOp::stat(pathname, statbuf))) {
    SHARE_LOG(WARN, "Fail to stat", K(ret), K(pathname));
  }
  return ret;
}

int ObLocalDevice::fstat(const ObIOFd &fd, ObIODFileStat &statbuf)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIODeviceLocalFileOp::fstat(fd, statbuf))) {
    SHARE_LOG(WARN, "Fail to fstat", K(ret), K(fd));
  }
  return ret;
}

int ObLocalDevice::del_unmerged_parts(const char *pathname)
{
  UNUSED(pathname);
  return OB_NOT_SUPPORTED;
}

int ObLocalDevice::adaptive_exist(const char *pathname, bool &is_exist)
{
  UNUSED(pathname);
  UNUSED(is_exist);
  return OB_NOT_SUPPORTED;
}

int ObLocalDevice::adaptive_stat(const char *pathname, ObIODFileStat &statbuf)
{
  UNUSED(pathname);
  UNUSED(statbuf);
  return OB_NOT_SUPPORTED;
}

int ObLocalDevice::adaptive_unlink(const char *pathname)
{
  UNUSED(pathname);
  return OB_NOT_SUPPORTED;
}

int ObLocalDevice::adaptive_scan_dir(const char *dir_name, ObBaseDirEntryOperator &op)
{
  UNUSED(dir_name);
  UNUSED(op);
  return OB_NOT_SUPPORTED;
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
      ret = ObIODeviceLocalFileOp::convert_sys_errno();
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
    } else if (OB_FAIL(ObIODeviceLocalFileOp::pread_impl(block_fd_, buf, size, 0, read_size))) {
      SHARE_LOG(WARN, "read main super block fail", K(ret), K(offset), K(size), KP(buf), K(block_fd_), KERRMSG);
    } else if (OB_FAIL(checker->do_check(buf, read_size))) {
      SHARE_LOG(WARN, "check main super block fail", K(ret), K(read_size), KP(buf));
    } else if (OB_FAIL(ObIODeviceLocalFileOp::pread_impl(block_fd_, buf, size, block_size_, read_size))) {
      SHARE_LOG(WARN, "read main super block fail", K(ret), K(block_size_), K(size), KP(buf), K(block_fd_), KERRMSG);
    } else if (OB_FAIL(checker->do_check(buf, read_size))) {
      SHARE_LOG(WARN, "check backup super block fail", K(ret), K(read_size), KP(buf));
    }
  } else {
    if (fd.is_block_file()) {
      const int64_t block_file_offset = get_block_file_offset(fd, offset);
      if (OB_FAIL(ObIODeviceLocalFileOp::pread_impl(block_fd_, buf, size, block_file_offset, read_size))) {
        SHARE_LOG(WARN, "failed to pread", K(ret), K(block_fd_), K(size), K(block_file_offset));
      }
    } else if (fd.is_normal_file()) {
      if (OB_FAIL(ObIODeviceLocalFileOp::pread_impl(fd.second_id_, buf, size, offset, read_size))) {
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
    } else if (OB_FAIL(ObIODeviceLocalFileOp::pwrite_impl(block_fd_, buf, size, 0, write_size))) {
        SHARE_LOG(WARN, "Fail to write main superblock, try backup", K(ret), K(write_size), K(offset), K(size), KP(buf));
        if (OB_FAIL(ObIODeviceLocalFileOp::pwrite_impl(block_fd_, buf, size, block_size_, write_size))) {
          SHARE_LOG(WARN, "Neither main nor backup superblock write success!!!", K(ret), K(write_size), K(offset), K(size), KP(buf));
        }
    } else if (OB_UNLIKELY(OB_SUCCESS != ObIODeviceLocalFileOp::pwrite_impl(block_fd_, buf, size, 1 * block_size_ + offset, write_size))) {
      // main superblock success, allow backup failure
      SHARE_LOG(WARN, "Fail to write backup superblock", K(write_size), K(offset), K(size), KP(buf));
    }
  } else {
    if (fd.is_block_file()) {
      const int64_t block_file_offset = get_block_file_offset(fd, offset);
      if (OB_FAIL(ObIODeviceLocalFileOp::pwrite_impl(block_fd_, buf, size, block_file_offset, write_size))) {
        SHARE_LOG(WARN, "failed to pwrite", K(ret), K(block_fd_), KP(buf), K(size), K(block_file_offset));
      }
    } else if (fd.is_normal_file()) {
      if (OB_FAIL(ObIODeviceLocalFileOp::pwrite_impl(fd.second_id_, buf, size, offset, write_size))) {
        SHARE_LOG(WARN, "failed to pwrite", K(ret), K(fd), KP(buf), K(size), K(offset));
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
  } else if (OB_FAIL(ObIODeviceLocalFileOp::read(fd, buf, size, read_size))) {
    SHARE_LOG(WARN, "Fail to read", K(ret), K(fd), K(buf), K(size));
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
  } else if (OB_FAIL(ObIODeviceLocalFileOp::write(fd, buf, size, write_size))) {
    SHARE_LOG(WARN, "Fail to write", K(ret), K(fd), K(buf), K(size));
  }
  return ret;
}

int ObLocalDevice::upload_part(
    const ObIOFd &fd,
    const char *buf,
    const int64_t size,
    const int64_t part_id,
    int64_t &write_size)
{
  UNUSED(fd);
  UNUSED(buf);
  UNUSED(size);
  UNUSED(part_id);
  UNUSED(write_size);
  return OB_NOT_SUPPORTED;
}

int ObLocalDevice::buf_append_part(
    const ObIOFd &fd,
    const char *buf,
    const int64_t size,
    const uint64_t tenant_id,
    bool &is_full)
{
  UNUSED(fd);
  UNUSED(buf);
  UNUSED(size);
  UNUSED(tenant_id);
  UNUSED(is_full);
  return OB_NOT_SUPPORTED;
}

int ObLocalDevice::get_part_id(const ObIOFd &fd, bool &is_exist, int64_t &part_id)
{
  UNUSED(fd);
  UNUSED(is_exist);
  UNUSED(part_id);
  return OB_NOT_SUPPORTED;
}

int ObLocalDevice::get_part_size(const ObIOFd &fd, const int64_t part_id, int64_t &part_size)
{
  UNUSED(fd);
  UNUSED(part_id);
  UNUSED(part_size);
  return OB_NOT_SUPPORTED;
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
      // libaio on error it returns a negated error number (the negative of one of the values listed in ERRORS)
      ret = ObIODeviceLocalFileOp::convert_sys_errno(-sys_ret);
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
  } else if (OB_UNLIKELY(ObIOContextType::IO_CONTEXT_TYPE_LOCAL != io_context->get_type())) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "Invalid io context pointer", K(ret), KP(io_context),
              "io_context_type", io_context->get_type());
  } else if (OB_ISNULL(local_io_context = static_cast<ObLocalIOContext *> (io_context))) {
    ret = OB_ERR_UNEXPECTED;
    SHARE_LOG(WARN, "local io context is null", K(ret), KP(io_context));
  } else {
    int sys_ret = 0;
    if ((sys_ret = ::io_destroy(local_io_context->io_context_)) != 0) {
      // libaio on error it returns a negated error number (the negative of one of the values listed in ERRORS)
      ret = ObIODeviceLocalFileOp::convert_sys_errno(-sys_ret);
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
  } else if (OB_UNLIKELY(ObIOCBType::IOCB_TYPE_LOCAL != iocb->get_type())) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "Invalid iocb pointer", K(ret), KP(iocb), "iocb_type", iocb->get_type());
  } else if (OB_ISNULL(local_iocb = static_cast<ObLocalIOCB *> (iocb))) {
    ret = OB_ERR_UNEXPECTED;
    SHARE_LOG(WARN, "local iocb is null", K(ret), KP(iocb));
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
  } else if (OB_UNLIKELY(ObIOCBType::IOCB_TYPE_LOCAL != iocb->get_type())) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "Invalid iocb pointer", K(ret), KP(iocb), "iocb_type", iocb->get_type());
  } else if (OB_ISNULL(local_iocb = static_cast<ObLocalIOCB *> (iocb))) {
    ret = OB_ERR_UNEXPECTED;
    SHARE_LOG(WARN, "local iocb is null", K(ret), KP(iocb));
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
  } else if (OB_UNLIKELY((ObIOContextType::IO_CONTEXT_TYPE_LOCAL != io_context->get_type())
                         || (ObIOCBType::IOCB_TYPE_LOCAL != iocb->get_type()))) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "Invalid io_context or iocb pointer", K(ret), KP(io_context), "io_context_type",
             io_context->get_type(), KP(iocb), "iocb_type", iocb->get_type());
  } else if (OB_ISNULL(local_iocb = static_cast<ObLocalIOCB *> (iocb))) {
    ret = OB_ERR_UNEXPECTED;
    SHARE_LOG(WARN, "local iocb pointer is null", K(ret), KP(iocb));
  } else if (OB_ISNULL(local_io_context = static_cast<ObLocalIOContext *> (io_context))) {
    ret = OB_ERR_UNEXPECTED;
    SHARE_LOG(WARN, "local io context pointer is null", K(ret), KP(io_context));
  } else {
    iocbp = &(local_iocb->iocb_);
    int submit_ret = ::io_submit(local_io_context->io_context_, 1, &iocbp);
    time_guard.click("LocalDevice_submit");
    if (1 != submit_ret) {
      // libaio on error it returns a negated error number (the negative of one of the values listed in ERRORS)
      ret = ObIODeviceLocalFileOp::convert_sys_errno(-submit_ret);
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
  } else if (OB_UNLIKELY((ObIOContextType::IO_CONTEXT_TYPE_LOCAL != io_context->get_type())
                         || (ObIOCBType::IOCB_TYPE_LOCAL != iocb->get_type()))) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "Invalid io_context or iocb pointer", K(ret), KP(io_context), "io_context_type",
             io_context->get_type(), KP(iocb), "iocb_type", iocb->get_type());
  } else if (OB_ISNULL(local_iocb = static_cast<ObLocalIOCB *> (iocb))) {
    ret = OB_ERR_UNEXPECTED;
    SHARE_LOG(WARN, "local iocb pointer is null", K(ret), KP(iocb));
  } else if (OB_ISNULL(local_io_context = static_cast<ObLocalIOContext *> (io_context))) {
    ret = OB_ERR_UNEXPECTED;
    SHARE_LOG(WARN, "local io context pointer is null", K(ret), KP(io_context));
  } else {
    int sys_ret = 0;
    if ((sys_ret = ::io_cancel(local_io_context->io_context_, &(local_iocb->iocb_), &local_event)) < 0) {
      // libaio on error it returns a negated error number (the negative of one of the values listed in ERRORS)
      ret = ObIODeviceLocalFileOp::convert_sys_errno(-sys_ret);
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
  } else if (OB_UNLIKELY((ObIOContextType::IO_CONTEXT_TYPE_LOCAL != io_context->get_type())
                         || (ObIOEventsType::IO_EVENTS_TYPE_LOCAL != events->get_type()))) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "Invalid io_context or io_events pointer", K(ret), KP(io_context),
      "io_context_type", io_context->get_type(), KP(events), "io_events_type", events->get_type());
  } else if (OB_ISNULL(local_io_events = static_cast<ObLocalIOEvents *> (events))) {
    ret = OB_ERR_UNEXPECTED;
    SHARE_LOG(WARN, "local io events pointer is null", K(ret), KP(events));
  } else if (OB_ISNULL(local_io_context = static_cast<ObLocalIOContext *> (io_context))) {
    ret = OB_ERR_UNEXPECTED;
    SHARE_LOG(WARN, "local io context pointer is null", K(ret), KP(io_context));
  } else {
    int sys_ret = 0;
    {
      oceanbase::lib::Thread::WaitGuard guard(oceanbase::lib::Thread::WAIT_FOR_IO_EVENT);
      common::ObBKGDSessInActiveGuard inactive_guard;
      while ((sys_ret = ::io_getevents(
          local_io_context->io_context_,
          min_nr,
          local_io_events->max_event_cnt_,
          local_io_events->io_events_,
          timeout)) < 0 && -EINTR == sys_ret); // ignore EINTR
    }
    if (sys_ret < 0) {
      // libaio on error it returns a negated error number (the negative of one of the values listed in ERRORS)
      ret = ObIODeviceLocalFileOp::convert_sys_errno(-sys_ret);
      SHARE_LOG(WARN, "Fail to get io events, ", K(ret), K(sys_ret), KERRMSG);
    } else {
      local_io_events->complete_io_cnt_ = sys_ret;
    }
  }
  return ret;
}

common::ObIOCB* ObLocalDevice::alloc_iocb(const uint64_t tenant_id)
{
  UNUSED(tenant_id);
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
  int ret = OB_SUCCESS;
  if (OB_LIKELY(is_inited_)) {
    ObLocalIOCB *local_iocb = nullptr;
    if (OB_UNLIKELY(ObIOCBType::IOCB_TYPE_LOCAL != iocb->get_type())) {
      ret = OB_INVALID_ARGUMENT;
      SHARE_LOG(WARN, "Invalid iocb pointer", K(ret), KP(iocb), "iocb_type", iocb->get_type());
    } else if (OB_ISNULL(local_iocb = static_cast<ObLocalIOCB *>(iocb))) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_LOG(WARN, "local iocb is null", K(ret), KP(iocb));
    } else {
      local_iocb->~ObLocalIOCB();
      iocb_pool_.free(local_iocb);
    }
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
    ret = ObIODeviceLocalFileOp::convert_sys_errno();
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

int ObLocalDevice::check_space_full(
    const int64_t required_size,
    const bool alarm_if_space_full) const
{
  int ret = OB_SUCCESS;
  int64_t used_percent = 0;
  const int64_t NO_LIMIT_PERCENT = 100;

  if (OB_UNLIKELY(!is_marked_)) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "The ObLocalDevice has not been marked", K(ret));
  } else if (OB_FAIL(get_data_disk_used_percentage_(required_size,
                                                    used_percent))) {
    SHARE_LOG(WARN, "Fail to get disk used percentage", K(ret));
  } else {
    if (GCONF.data_disk_usage_limit_percentage != NO_LIMIT_PERCENT
        && used_percent >= GCONF.data_disk_usage_limit_percentage) {
      ret = OB_SERVER_OUTOF_DISK_SPACE;
      if (alarm_if_space_full && REACH_TIME_INTERVAL(24 * 3600LL * 1000 * 1000 /* 24h */)) {
        LOG_DBA_ERROR_V2(OB_SHARE_OUTOF_DISK_SPACE, OB_SERVER_OUTOF_DISK_SPACE,
                         "disk is almost full. resuired size is ", required_size,
                         " and used percent is ", used_percent, "%. ",
                         "[suggestion] Increase the datafile_size or datafile_disk_percentage parameter. ");
      }
    }
  }
  return ret;
}

int ObLocalDevice::check_write_limited() const
{
  int ret = OB_SUCCESS;
  int64_t used_percent = 0;
  const int64_t required_size = 0;
  const int64_t limit_percent = GCONF.data_disk_write_limit_percentage;

  if (OB_UNLIKELY(!is_marked_)) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "The ObLocalDevice has not been marked", K(ret));
  } else if (OB_FAIL(get_data_disk_used_percentage_(required_size,
                                                    used_percent))) {
    SHARE_LOG(WARN, "Fail to get disk used percentage", K(ret));
  } else if (limit_percent != 0 && used_percent >= limit_percent) {
    ret = OB_SERVER_OUTOF_DISK_SPACE;
    if (REACH_TIME_INTERVAL(60 * 1000 * 1000 /* 1min */)) {
      SHARE_LOG(WARN, "disk is full, user write should be stopped", K(ret), K(used_percent),
                K(limit_percent));
    }
  }
  return ret;
}

int ObLocalDevice::get_data_disk_used_percentage_(
    const int64_t required_size,
    int64_t &percent) const
{
  int ret = OB_SUCCESS;
  int64_t reserved_size = 4 * 1024 * 1024 * 1024L; // default RESERVED_DISK_SIZE -> 4G

  if (OB_UNLIKELY(!is_marked_)) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "The ObLocalDevice has not been marked", K(ret));
  } else if (OB_UNLIKELY(required_size < 0)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid argument", K(ret), K(required_size));
  } else if (OB_FAIL(SERVER_STORAGE_META_SERVICE.get_reserved_size(reserved_size))) {
    SHARE_LOG(WARN, "Fail to get reserved size", K(ret));
  } else {
    int64_t max_block_cnt = get_max_block_count(reserved_size);
    int64_t actual_free_block_cnt = free_block_cnt_;
    if (max_block_cnt > total_block_cnt_) {  // auto extend is on
      actual_free_block_cnt = max_block_cnt - total_block_cnt_ + free_block_cnt_;
    }
    const int64_t required_count = required_size / block_size_;
    const int64_t free_count = actual_free_block_cnt - required_count;
    percent = 100 - 100 * free_count / total_block_cnt_;
  }
  return ret;
}

int ObLocalDevice::resize_block_file(const int64_t new_size)
{
  // copy free block info to new_free_block_array
  int ret = OB_SUCCESS;
  int sys_ret = 0;
  const ObMemAttr mem_attr = SET_IGNORE_MEM_VERSION(ObMemAttr(OB_SYS_TENANT_ID, "LDBlockBitMap"));
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
    ret = ObIODeviceLocalFileOp::convert_sys_errno();
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

} /* namespace share */
} /* namespace oceanbase */
