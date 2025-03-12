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

#define USING_LOG_PREFIX STORAGE

#include "storage/tmp_file/ob_tmp_file_io_ctx.h"
#include "observer/ob_server_struct.h"

namespace oceanbase
{
using namespace storage;
using namespace share;

namespace tmp_file
{
ObTmpFileIOCtx::ObTmpFileIOCtx():
                is_inited_(false),
                is_read_(false),
                fd_(ObTmpFileGlobal::INVALID_TMP_FILE_FD),
                dir_id_(ObTmpFileGlobal::INVALID_TMP_FILE_DIR_ID),
                buf_(nullptr),
                buf_size_(-1),
                done_size_(-1),
                todo_size_(-1),
                read_offset_in_file_(-1),
                disable_page_cache_(false),
                disable_block_cache_(false),
                prefetch_(false),
                io_flag_(),
                io_timeout_ms_(DEFAULT_IO_WAIT_TIME_MS),
                io_handles_(),
                page_cache_handles_(),
                block_cache_handles_(),
                is_unaligned_write_(false),
                write_persisted_tail_page_cnt_(0),
                lack_page_cnt_(0),
                is_unaligned_read_(false),
                total_truncated_page_read_cnt_(0),
                total_kv_cache_page_read_cnt_(0),
                total_uncached_page_read_cnt_(0),
                total_wbp_page_read_cnt_(0),
                truncated_page_read_hits_(0),
                kv_cache_page_read_hits_(0),
                uncached_page_read_hits_(0),
                aggregate_read_io_cnt_(0),
                wbp_page_read_hits_(0)
{
  io_handles_.set_attr(ObMemAttr(MTL_ID(), "TMP_IO_HDL"));
  page_cache_handles_.set_attr(ObMemAttr(MTL_ID(), "TMP_PCACHE_HDL"));
  block_cache_handles_.set_attr(ObMemAttr(MTL_ID(), "TMP_BCACHE_HDL"));
}

ObTmpFileIOCtx::~ObTmpFileIOCtx()
{
  reset();
}

int ObTmpFileIOCtx::init(const int64_t fd, const int64_t dir_id,
                         const bool is_read,
                         const common::ObIOFlag io_flag,
                         const int64_t io_timeout_ms,
                         const bool disable_page_cache,
                         const bool disable_block_cache,
                         const bool prefetch)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("ObTmpFileIOCtx init twice", KR(ret));
  } else if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_TMP_FILE_FD == fd ||
                         ObTmpFileGlobal::INVALID_TMP_FILE_DIR_ID == dir_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(fd), K(dir_id));
  } else if (OB_UNLIKELY(!io_flag.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(fd), K(io_flag));
  } else if (OB_UNLIKELY(io_timeout_ms < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(fd), K(io_timeout_ms));
  } else {
    fd_ = fd;
    dir_id_ = dir_id;
    is_read_ = is_read;
    io_flag_ = io_flag;
    io_timeout_ms_ = io_timeout_ms;
    disable_page_cache_ = disable_page_cache;
    disable_block_cache_ = disable_block_cache;
    prefetch_ = prefetch;
    is_inited_ = true;
  }
  return ret;
}

void ObTmpFileIOCtx::reuse()
{
  buf_ = nullptr;
  buf_size_ = -1;
  done_size_ = -1;
  todo_size_ = -1;
  read_offset_in_file_ = -1;
  for (int32_t i = 0; i < io_handles_.count(); i++) {
    io_handles_.at(i).handle_.reset();
  }
  for (int32_t i = 0; i < page_cache_handles_.count(); i++) {
    page_cache_handles_.at(i).page_handle_.reset();
  }
  for (int32_t i = 0; i < block_cache_handles_.count(); i++) {
    block_cache_handles_.at(i).block_handle_.reset();
  }
  io_handles_.reset();
  page_cache_handles_.reset();
  block_cache_handles_.reset();
}

void ObTmpFileIOCtx::reset()
{
  reuse();
  is_inited_ = false;
  is_read_ = false;
  fd_ = ObTmpFileGlobal::INVALID_TMP_FILE_FD;
  dir_id_ = ObTmpFileGlobal::INVALID_TMP_FILE_DIR_ID;
  disable_page_cache_ = false;
  disable_block_cache_ = false;
  io_flag_.reset();
  io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  /********for virtual table statistics begin********/
  is_unaligned_write_ = false;
  write_persisted_tail_page_cnt_ = 0;
  lack_page_cnt_ = 0;
  is_unaligned_read_ = false;
  total_truncated_page_read_cnt_ = 0;
  total_kv_cache_page_read_cnt_ = 0;
  total_uncached_page_read_cnt_ = 0;
  total_wbp_page_read_cnt_ = 0;
  truncated_page_read_hits_ = 0;
  kv_cache_page_read_hits_ = 0;
  uncached_page_read_hits_ = 0;
  aggregate_read_io_cnt_ = 0;
  wbp_page_read_hits_ = 0;
  /********for virtual table statistics end ********/
}

bool ObTmpFileIOCtx::is_valid() const
{
  return is_inited_ &&
         fd_ != ObTmpFileGlobal::INVALID_TMP_FILE_FD &&
         dir_id_ != ObTmpFileGlobal::INVALID_TMP_FILE_DIR_ID &&
         nullptr != buf_ && buf_size_ > 0 && done_size_ >= 0 && todo_size_ >= 0 &&
         (is_read_ ? read_offset_in_file_ >= 0 : true) &&
         io_timeout_ms_ >= 0 && io_flag_.is_valid();
}

int ObTmpFileIOCtx::prepare_read(char *read_buf, const int64_t read_size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), KPC(this));
  } else if (OB_ISNULL(read_buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(fd_), KP(read_buf));
  } else if (OB_UNLIKELY(read_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(fd_), K(read_size));
  } else {
    is_read_ = true;
    buf_ = read_buf;
    buf_size_ = read_size;
    done_size_ = 0;
    todo_size_ = read_size;
    read_offset_in_file_ = -1;
  }
  return ret;
}

int ObTmpFileIOCtx::prepare_read(char *read_buf, const int64_t read_size, const int64_t read_offset)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(read_offset < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(fd_), K(read_offset));
  } else if (OB_FAIL(prepare_read(read_buf, read_size))) {
    LOG_WARN("failed to prepare read", KR(ret), K(fd_), KP(read_buf), K(read_size));
  } else {
    read_offset_in_file_ = read_offset;
  }
  return ret;
}

int ObTmpFileIOCtx::prepare_write(char *write_buf, const int64_t write_size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), KPC(this));
  } else if (OB_ISNULL(write_buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(fd_), KP(write_buf));
  } else if (OB_UNLIKELY(write_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(fd_), K(write_size));
  } else {
    is_read_ = false;
    buf_ = write_buf;
    buf_size_ = write_size;
    done_size_ = 0;
    todo_size_ = write_size;
    read_offset_in_file_ = -1;
  }
  return ret;
}

int ObTmpFileIOCtx::update_data_size(const int64_t size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid ctx", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(size > todo_size_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(fd_), K(size), K(todo_size_));
  } else if (OB_UNLIKELY(is_read_ && read_offset_in_file_ < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("read offset is invalid", KR(ret), K(fd_), K(read_offset_in_file_));
  } else {
    if (is_read_) {
      read_offset_in_file_ += size;
    }
    done_size_ += size;
    todo_size_ -= size;
  }
  return ret;
}

int ObTmpFileIOCtx::wait()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid ctx", KR(ret), K(fd_), KPC(this));
  } else if (!is_read_) {
    // due to tmp file always writes data in buffer,
    // there are no asynchronous io tasks need to wait
    // do nothing
  } else if (OB_FAIL(wait_read_finish_())) {
    STORAGE_LOG(WARN, "wait read finish failed", KR(ret), K(fd_), K(is_read_));
  }

  return ret;
}

int ObTmpFileIOCtx::wait_read_finish_()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_read_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the handle is prepared for writing, not allowed to wait read finish", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(buf_size_ != done_size_ + todo_size_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("done_size_ + todo_size_ is not equal to buf size", KR(ret), KPC(this));
  } else if (OB_FAIL(do_read_wait_())) {
    LOG_WARN("fail to wait tmp file io", KR(ret), K(fd_));
  }

  return ret;
}

int ObTmpFileIOCtx::do_read_wait_()
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; OB_SUCC(ret) && i < page_cache_handles_.count(); ++i) {
    ObPageCacheHandle &page_cache_handle = page_cache_handles_.at(i);
    if (OB_UNLIKELY(!page_cache_handle.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("page cache handle is not valid", KR(ret), K(fd_), K(page_cache_handle), KPC(this));
    } else {
      const char * page_buf = page_cache_handle.page_handle_.value_->get_buffer();
      const int64_t offset_in_page = page_cache_handle.offset_in_src_data_buf_;
      const int64_t read_size = page_cache_handle.read_size_;
      char * read_buf = page_cache_handle.dest_user_read_buf_;
      if (OB_ISNULL(page_buf)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("data buf is null", KR(ret), K(fd_), K(page_cache_handle));
      } else if (OB_UNLIKELY(!check_buf_range_valid(read_buf, read_size))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid range", KR(ret), K(fd_), KP(read_buf), KP(buf_), K(read_size), K(buf_size_), KPC(this));
      } else if (OB_UNLIKELY(offset_in_page + read_size > ObTmpFileGlobal::PAGE_SIZE)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("read size is over than page range", KR(ret), KPC(this), K(offset_in_page), K(read_size));
      } else {
        MEMCPY(read_buf, page_buf + offset_in_page, read_size);
        page_cache_handle.page_handle_.reset();
      }
    }
  }
  if (OB_SUCC(ret)) {
    page_cache_handles_.reset();
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < block_cache_handles_.count(); ++i) {
    ObBlockCacheHandle &block_cache_handle = block_cache_handles_.at(i);
    if (OB_UNLIKELY(!block_cache_handle.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("block cache handle is not valid", KR(ret), K(fd_), K(block_cache_handle), KPC(this));
    } else {
      const char * block_buf = block_cache_handle.block_handle_.value_->get_buffer();
      const int64_t offset_in_block = block_cache_handle.offset_in_src_data_buf_;
      const int64_t read_size = block_cache_handle.read_size_;
      char * read_buf = block_cache_handle.dest_user_read_buf_;
      if (OB_ISNULL(block_buf)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("data buf is null", KR(ret), K(fd_), K(block_cache_handle));
      } else if (GCTX.is_shared_storage_mode()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not support read from block cache in shared storage mode", KR(ret), KPC(this));
      } else if (OB_UNLIKELY(!check_buf_range_valid(read_buf, read_size))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid range", KR(ret), K(fd_), KP(read_buf), KP(buf_), K(read_size), K(buf_size_), KPC(this));
      } else if (OB_UNLIKELY(offset_in_block + read_size > ObTmpFileGlobal::SN_BLOCK_SIZE)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("read size is over than macro block range", KR(ret), KPC(this), K(offset_in_block), K(read_size));
      } else {
        MEMCPY(read_buf, block_buf + offset_in_block, read_size);
        block_cache_handle.block_handle_.reset();
      }
    }
  }
  if (OB_SUCC(ret)) {
    block_cache_handles_.reset();
  }

  // Wait read io finish.
  for (int64_t i = 0; OB_SUCC(ret) && i < io_handles_.count(); ++i) {
    ObIOReadHandle &io_handle = io_handles_.at(i);
    if (OB_UNLIKELY(!io_handle.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("io handle is not valid", KR(ret), K(fd_), K(io_handle), KPC(this));
    } else if (OB_FAIL(io_handle.handle_.wait())) {
      LOG_WARN("fail to do object handle read wait", KR(ret), K(fd_), K(io_handle));
    } else {
      const char * data_buf = io_handle.handle_.get_buffer();
      const int64_t offset = io_handle.offset_in_src_data_buf_;
      const int64_t size = io_handle.read_size_;
      char * read_buf = io_handle.dest_user_read_buf_;
      if (OB_ISNULL(data_buf)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("data buf is null", KR(ret), K(fd_), K(io_handle));
      } else if (OB_UNLIKELY(!check_buf_range_valid(read_buf, size))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid range", KR(ret), K(fd_), KP(read_buf), KP(buf_), K(size), K(buf_size_), KPC(this));
      } else {
        MEMCPY(read_buf, data_buf + offset, size);
        io_handle.handle_.reset();
      }
    }
  }

  if (OB_SUCC(ret)) {
    io_handles_.reset();
  }

  return ret;
}

ObTmpFileIOCtx::ObIReadHandle::ObIReadHandle()
  : dest_user_read_buf_(nullptr), offset_in_src_data_buf_(0), read_size_(0)
{
}

ObTmpFileIOCtx::ObIReadHandle::ObIReadHandle(
    char *dest_user_read_buf, const int64_t offset_in_src_data_buf, const int64_t read_size)
  : dest_user_read_buf_(dest_user_read_buf), offset_in_src_data_buf_(offset_in_src_data_buf), read_size_(read_size)
{
}

ObTmpFileIOCtx::ObIReadHandle::~ObIReadHandle()
{
}

ObTmpFileIOCtx::ObIReadHandle::ObIReadHandle(const ObTmpFileIOCtx::ObIReadHandle &other)
{
  *this = other;
}

ObTmpFileIOCtx::ObIReadHandle &ObTmpFileIOCtx::ObIReadHandle::operator=(
    const ObTmpFileIOCtx::ObIReadHandle &other)
{
  if (&other != this) {
    offset_in_src_data_buf_ = other.offset_in_src_data_buf_;
    dest_user_read_buf_ = other.dest_user_read_buf_;
    read_size_ = other.read_size_;
  }
  return *this;
}

ObTmpFileIOCtx::ObIOReadHandle::ObIOReadHandle()
  : ObIReadHandle(), handle_()
{
}

ObTmpFileIOCtx::ObIOReadHandle::ObIOReadHandle(
    char *dest_user_read_buf, const int64_t offset_in_src_data_buf, const int64_t read_size)
  : ObIReadHandle(dest_user_read_buf, offset_in_src_data_buf, read_size), handle_(), block_handle_()
{
}

ObTmpFileIOCtx::ObIOReadHandle::ObIOReadHandle(
    char *dest_user_read_buf, const int64_t offset_in_src_data_buf, const int64_t read_size,
    ObTmpFileBlockHandle block_handle)
  : ObIReadHandle(dest_user_read_buf, offset_in_src_data_buf, read_size), handle_(), block_handle_(block_handle)
{
}

ObTmpFileIOCtx::ObIOReadHandle::~ObIOReadHandle()
{
}

ObTmpFileIOCtx::ObIOReadHandle::ObIOReadHandle(const ObTmpFileIOCtx::ObIOReadHandle &other)
{
  *this = other;
}

ObTmpFileIOCtx::ObIOReadHandle &ObTmpFileIOCtx::ObIOReadHandle::operator=(
    const ObTmpFileIOCtx::ObIOReadHandle &other)
{
  if (&other != this) {
    handle_ = other.handle_;
    block_handle_ = other.block_handle_;
    ObIReadHandle::operator=(other);
  }
  return *this;
}

bool ObTmpFileIOCtx::ObIOReadHandle::is_valid()
{
  bool bret = OB_NOT_NULL(dest_user_read_buf_) && offset_in_src_data_buf_ >= 0 &&
              read_size_ >= 0 &&
              handle_.is_valid();

  if (bret) {
    if (!GCTX.is_shared_storage_mode()) {
      bret = read_size_ <= ObTmpFileGlobal::SN_BLOCK_SIZE && block_handle_.is_inited();
    #ifdef OB_BUILD_SHARED_STORAGE
    } else {
      bret = read_size_ <= ObTmpFileGlobal::SS_BLOCK_SIZE;
    #endif
    }
  }

  return bret;
}

ObTmpFileIOCtx::ObBlockCacheHandle::ObBlockCacheHandle()
  : ObIReadHandle(), block_handle_()
{
}

ObTmpFileIOCtx::ObBlockCacheHandle::ObBlockCacheHandle(const ObTmpBlockValueHandle &block_handle,
    char *dest_user_read_buf, const int64_t offset_in_src_data_buf, const int64_t read_size)
  : ObIReadHandle(dest_user_read_buf, offset_in_src_data_buf, read_size), block_handle_(block_handle)
{
}

ObTmpFileIOCtx::ObBlockCacheHandle::~ObBlockCacheHandle()
{
}

ObTmpFileIOCtx::ObBlockCacheHandle::ObBlockCacheHandle(
    const ObTmpFileIOCtx::ObBlockCacheHandle &other)
{
  *this = other;
}

ObTmpFileIOCtx::ObBlockCacheHandle &ObTmpFileIOCtx::ObBlockCacheHandle::operator=(
    const ObTmpFileIOCtx::ObBlockCacheHandle &other)
{
  if (&other != this) {
    block_handle_ = other.block_handle_;
    ObIReadHandle::operator=(other);
  }
  return *this;
}

bool ObTmpFileIOCtx::ObBlockCacheHandle::is_valid()
{
  bool bret = false;
  if (!GCTX.is_shared_storage_mode() &&
      OB_NOT_NULL(dest_user_read_buf_) && offset_in_src_data_buf_ >= 0 &&
      read_size_ >= 0 &&
      read_size_ <= ObTmpFileGlobal::SN_BLOCK_SIZE &&
      OB_NOT_NULL(block_handle_.value_) &&
      block_handle_.handle_.is_valid()) {
    bret = true;
  }
  return bret;
}

ObTmpFileIOCtx::ObPageCacheHandle::ObPageCacheHandle()
  : ObIReadHandle(), page_handle_()
{
}

ObTmpFileIOCtx::ObPageCacheHandle::ObPageCacheHandle(const ObTmpPageValueHandle &page_handle,
    char *dest_user_read_buf, const int64_t offset_in_src_data_buf, const int64_t read_size)
  : ObIReadHandle(dest_user_read_buf, offset_in_src_data_buf, read_size), page_handle_(page_handle)
{
}

ObTmpFileIOCtx::ObPageCacheHandle::~ObPageCacheHandle()
{
}

ObTmpFileIOCtx::ObPageCacheHandle::ObPageCacheHandle(
    const ObTmpFileIOCtx::ObPageCacheHandle &other)
{
  *this = other;
}

ObTmpFileIOCtx::ObPageCacheHandle &ObTmpFileIOCtx::ObPageCacheHandle::operator=(
    const ObTmpFileIOCtx::ObPageCacheHandle &other)
{
  if (&other != this) {
    page_handle_ = other.page_handle_;
    ObIReadHandle::operator=(other);
  }
  return *this;
}

bool ObTmpFileIOCtx::ObPageCacheHandle::is_valid()
{
  bool bret = false;
  if (OB_NOT_NULL(dest_user_read_buf_) && offset_in_src_data_buf_ >= 0 &&
      read_size_ >= 0 &&
      read_size_ <= ObTmpFileGlobal::PAGE_SIZE &&
      OB_NOT_NULL(page_handle_.value_) &&
      page_handle_.handle_.is_valid()) {
    bret = true;
  }
  return bret;
}

} // end namespace tmp_file
} // end namespace oceanbase
