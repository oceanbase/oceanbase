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
/**************************** ObTmpFileIOBaseCtx begin ****************************/
ObTmpFileIOBaseCtx::ObTmpFileIOBaseCtx():
                    is_inited_(false),
                    fd_(ObTmpFileGlobal::INVALID_TMP_FILE_FD),
                    buf_(nullptr),
                    buf_size_(-1),
                    done_size_(-1),
                    todo_size_(-1),
                    io_flag_(),
                    io_timeout_ms_(DEFAULT_IO_WAIT_TIME_MS)
{
}

ObTmpFileIOBaseCtx::~ObTmpFileIOBaseCtx()
{
}

int ObTmpFileIOBaseCtx::init(const int64_t fd,
                             const common::ObIOFlag io_flag,
                             const int64_t io_timeout_ms)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("ObTmpFileIOBaseCtx init twice", KR(ret));
  } else if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_TMP_FILE_FD == fd)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(fd));
  } else if (OB_UNLIKELY(!io_flag.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(fd), K(io_flag));
  } else if (OB_UNLIKELY(io_timeout_ms < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(fd), K(io_timeout_ms));
  } else {
    fd_ = fd;
    io_flag_ = io_flag;
    io_timeout_ms_ = io_timeout_ms;
    is_inited_ = true;
  }
  return ret;
}

void ObTmpFileIOBaseCtx::reuse()
{
  buf_ = nullptr;
  buf_size_ = -1;
  done_size_ = -1;
  todo_size_ = -1;
}

void ObTmpFileIOBaseCtx::reset()
{
  reuse();
  is_inited_ = false;
  fd_ = ObTmpFileGlobal::INVALID_TMP_FILE_FD;
  io_flag_.reset();
  io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
}

bool ObTmpFileIOBaseCtx::is_valid() const
{
  return is_inited_ &&
         fd_ != ObTmpFileGlobal::INVALID_TMP_FILE_FD &&
         nullptr != buf_ && buf_size_ > 0 && done_size_ >= 0 && todo_size_ >= 0 &&
         io_timeout_ms_ >= 0 && io_flag_.is_valid();
}

int ObTmpFileIOBaseCtx::update_data_size(const int64_t size)
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
  } else {
    done_size_ += size;
    todo_size_ -= size;
  }
  return ret;
}

int ObTmpFileIOBaseCtx::prepare(char *buf, const int64_t size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), KPC(this));
  } else if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(fd_), KP(buf));
  } else if (OB_UNLIKELY(size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(fd_), K(size));
  } else {
    buf_ = buf;
    buf_size_ = size;
    done_size_ = 0;
    todo_size_ = size;
  }
  return ret;
}
/**************************** ObTmpFileIOBaseCtx over ****************************/

/**************************** ObTmpFileIOWriteCtx begin ****************************/
ObTmpFileIOWriteCtx::ObTmpFileIOWriteCtx():
                     ObTmpFileIOBaseCtx(),
                     is_unaligned_write_(false),
                     write_persisted_tail_page_cnt_(0),
                     lack_page_cnt_(0)
{
}

ObTmpFileIOWriteCtx::~ObTmpFileIOWriteCtx()
{
  reset();
}

void ObTmpFileIOWriteCtx::reset()
{
  /********for virtual table statistics begin********/
  is_unaligned_write_ = false;
  write_persisted_tail_page_cnt_ = 0;
  lack_page_cnt_ = 0;
  /********for virtual table statistics end ********/
  ObTmpFileIOBaseCtx::reset();
}
/**************************** ObTmpFileIOWriteCtx end ****************************/

/**************************** ObTmpFileIOReadCtx begin ****************************/

ObTmpFileIOReadCtx::ObTmpFileIOReadCtx():
                    ObTmpFileIOBaseCtx(),
                    read_offset_in_file_(-1),
                    disable_page_cache_(false),
                    prefetch_(false),
                    io_handles_(),
                    page_cache_handles_(),
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
}

ObTmpFileIOReadCtx::~ObTmpFileIOReadCtx()
{
  reset();
}

int ObTmpFileIOReadCtx::init(const int64_t fd,
                             const common::ObIOFlag io_flag,
                             const int64_t io_timeout_ms,
                             const bool disable_page_cache,
                             const bool prefetch)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("ObTmpFileIOReadCtx init twice", KR(ret));
  } else if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_TMP_FILE_FD == fd)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(fd));
  } else if (OB_UNLIKELY(!io_flag.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(fd), K(io_flag));
  } else if (OB_UNLIKELY(io_timeout_ms < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(fd), K(io_timeout_ms));
  } else {
    fd_ = fd;
    io_flag_ = io_flag;
    io_timeout_ms_ = io_timeout_ms;
    disable_page_cache_ = disable_page_cache;
    prefetch_ = prefetch;
    is_inited_ = true;
  }
  return ret;
}

void ObTmpFileIOReadCtx::reuse()
{
  read_offset_in_file_ = -1;
  for (int32_t i = 0; i < io_handles_.count(); i++) {
    io_handles_.at(i).handle_.reset();
  }
  for (int32_t i = 0; i < page_cache_handles_.count(); i++) {
    page_cache_handles_.at(i).page_handle_.reset();
  }
  io_handles_.reset();
  page_cache_handles_.reset();
  ObTmpFileIOBaseCtx::reuse();
}

void ObTmpFileIOReadCtx::reset()
{
  reuse();
  is_inited_ = false;
  fd_ = ObTmpFileGlobal::INVALID_TMP_FILE_FD;
  disable_page_cache_ = false;
  io_flag_.reset();
  io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  /********for virtual table statistics begin********/
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
  ObTmpFileIOBaseCtx::reset();
}

bool ObTmpFileIOReadCtx::is_valid() const
{
  return ObTmpFileIOBaseCtx::is_valid() && read_offset_in_file_ >= 0;
}

int ObTmpFileIOReadCtx::prepare(char *buf, const int64_t size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), KPC(this));
  } else if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(fd_), KP(buf));
  } else if (OB_UNLIKELY(size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(fd_), K(size));
  } else {
    buf_ = buf;
    buf_size_ = size;
    done_size_ = 0;
    todo_size_ = size;
    read_offset_in_file_ = -1;
  }
  return ret;
}

int ObTmpFileIOReadCtx::prepare(char *buf, const int64_t size, const int64_t read_offset)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(read_offset < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(fd_), K(read_offset));
  } else if (OB_FAIL(prepare(buf, size))) {
    LOG_WARN("failed to prepare read", KR(ret), K(fd_), KP(buf), K(size));
  } else {
    read_offset_in_file_ = read_offset;
  }
  return ret;
}

int ObTmpFileIOReadCtx::update_data_size(const int64_t size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(read_offset_in_file_ < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("read offset is invalid", KR(ret), K(fd_), K(read_offset_in_file_));
  } else if (OB_FAIL(ObTmpFileIOBaseCtx::update_data_size(size))) {
    LOG_WARN("failed to update data size", KR(ret), K(size), KPC(this));
  } else {
    read_offset_in_file_ += size;
  }
  return ret;
}

int ObTmpFileIOReadCtx::wait()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid ctx", KR(ret), K(fd_), KPC(this));
  } else if (OB_UNLIKELY(buf_size_ != done_size_ + todo_size_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("done_size_ + todo_size_ is not equal to buf size", KR(ret), KPC(this));
  } else {
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
  }

  return ret;
}


ObTmpFileIOReadCtx::ObIReadHandle::ObIReadHandle()
  : dest_user_read_buf_(nullptr), offset_in_src_data_buf_(0), read_size_(0)
{
}

ObTmpFileIOReadCtx::ObIReadHandle::ObIReadHandle(
    char *dest_user_read_buf, const int64_t offset_in_src_data_buf, const int64_t read_size)
  : dest_user_read_buf_(dest_user_read_buf), offset_in_src_data_buf_(offset_in_src_data_buf), read_size_(read_size)
{
}

ObTmpFileIOReadCtx::ObIReadHandle::~ObIReadHandle()
{
}

ObTmpFileIOReadCtx::ObIReadHandle::ObIReadHandle(const ObTmpFileIOReadCtx::ObIReadHandle &other)
{
  *this = other;
}

ObTmpFileIOReadCtx::ObIReadHandle &ObTmpFileIOReadCtx::ObIReadHandle::operator=(
    const ObTmpFileIOReadCtx::ObIReadHandle &other)
{
  if (&other != this) {
    offset_in_src_data_buf_ = other.offset_in_src_data_buf_;
    dest_user_read_buf_ = other.dest_user_read_buf_;
    read_size_ = other.read_size_;
  }
  return *this;
}

ObTmpFileIOReadCtx::ObIOReadHandle::ObIOReadHandle()
  : ObIReadHandle(), handle_()
{
}

ObTmpFileIOReadCtx::ObIOReadHandle::ObIOReadHandle(
    char *dest_user_read_buf, const int64_t offset_in_src_data_buf, const int64_t read_size)
  : ObIReadHandle(dest_user_read_buf, offset_in_src_data_buf, read_size), handle_(), block_handle_()
{
}

ObTmpFileIOReadCtx::ObIOReadHandle::ObIOReadHandle(
    char *dest_user_read_buf, const int64_t offset_in_src_data_buf, const int64_t read_size,
    ObTmpFileBlockHandle block_handle)
  : ObIReadHandle(dest_user_read_buf, offset_in_src_data_buf, read_size), handle_(), block_handle_(block_handle)
{
}

ObTmpFileIOReadCtx::ObIOReadHandle::~ObIOReadHandle()
{
}

ObTmpFileIOReadCtx::ObIOReadHandle::ObIOReadHandle(const ObTmpFileIOReadCtx::ObIOReadHandle &other)
{
  *this = other;
}

ObTmpFileIOReadCtx::ObIOReadHandle &ObTmpFileIOReadCtx::ObIOReadHandle::operator=(
    const ObTmpFileIOReadCtx::ObIOReadHandle &other)
{
  if (&other != this) {
    handle_ = other.handle_;
    block_handle_ = other.block_handle_;
    ObIReadHandle::operator=(other);
  }
  return *this;
}

bool ObTmpFileIOReadCtx::ObIOReadHandle::is_valid()
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

ObTmpFileIOReadCtx::ObPageCacheHandle::ObPageCacheHandle()
  : ObIReadHandle(), page_handle_()
{
}

ObTmpFileIOReadCtx::ObPageCacheHandle::ObPageCacheHandle(
    char *dest_user_read_buf, const int64_t offset_in_src_data_buf, const int64_t read_size)
  : ObIReadHandle(dest_user_read_buf, offset_in_src_data_buf, read_size), page_handle_()
{
}

ObTmpFileIOReadCtx::ObPageCacheHandle::~ObPageCacheHandle()
{
}

bool ObTmpFileIOReadCtx::ObPageCacheHandle::is_valid()
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
/**************************** ObTmpFileIOReadCtx end ****************************/

} // end namespace tmp_file
} // end namespace oceanbase
