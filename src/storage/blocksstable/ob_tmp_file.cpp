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

#include "ob_tmp_file.h"
#include "ob_tmp_file_cache.h"
#include "observer/ob_server_struct.h"
#include "storage/ob_file_system_util.h"
#include "share/ob_task_define.h"

namespace oceanbase {
using namespace storage;
using namespace share;

namespace blocksstable {

ObTmpFileIOInfo::ObTmpFileIOInfo() : fd_(0), dir_id_(0), size_(0), tenant_id_(OB_INVALID_ID), buf_(NULL), io_desc_()
{}

ObTmpFileIOInfo::~ObTmpFileIOInfo()
{}

void ObTmpFileIOInfo::reset()
{
  fd_ = 0;
  dir_id_ = 0;
  size_ = 0;
  tenant_id_ = OB_INVALID_ID;
  buf_ = NULL;
}

bool ObTmpFileIOInfo::is_valid() const
{
  return fd_ >= 0 && dir_id_ >= 0 && size_ > 0 && OB_INVALID_ID != tenant_id_ && NULL != buf_ && io_desc_.is_valid();
}

ObTmpFileIOHandle::ObTmpFileIOHandle()
    : tmp_file_(NULL),
      io_handles_(),
      page_cache_handles_(),
      block_cache_handles_(),
      buf_(NULL),
      size_(0),
      is_read_(false),
      has_wait_(false),
      expect_read_size_(0),
      last_read_offset_(-1),
      io_flag_(),
      update_offset_in_file_(false)
{}

ObTmpFileIOHandle::~ObTmpFileIOHandle()
{
  reset();
}

int ObTmpFileIOHandle::prepare_read(const int64_t read_size, const int64_t read_offset, const common::ObIODesc &io_flag,
    char *read_buf, ObTmpFile *file)
{
  int ret = OB_SUCCESS;
  if (NULL == read_buf || NULL == file) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP_(buf));
  } else {
    buf_ = read_buf;
    size_ = 0;
    tmp_file_ = file;
    is_read_ = true;
    has_wait_ = false;
    expect_read_size_ = read_size;
    last_read_offset_ = read_offset;
    io_flag_ = io_flag;
  }
  return ret;
}

int ObTmpFileIOHandle::prepare_write(char* write_buf, const int64_t write_size, ObTmpFile* file)
{
  int ret = OB_SUCCESS;
  if (NULL == write_buf || NULL == file) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP_(buf));
  } else {
    buf_ = write_buf;
    size_ = write_size;
    tmp_file_ = file;
    is_read_ = false;
    has_wait_ = false;
    expect_read_size_ = 0;
    last_read_offset_ = -1;
  }
  return ret;
}

int ObTmpFileIOHandle::wait(const int64_t timeout_ms)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(has_wait_ && is_read_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "read wait() isn't reentrant interface, shouldn't call again", K(ret));
  } else if (OB_FAIL(do_wait(timeout_ms))) {
    STORAGE_LOG(WARN, "fail to wait tmp file io", K(ret), K(timeout_ms));
  } else if (is_read_ && !has_wait_) {
    if (size_ == expect_read_size_) {
      // do nothing
    } else if (OB_UNLIKELY(size_ > expect_read_size_)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "read size more than expected size", K(ret), K(timeout_ms));
    } else {
      ObTmpFileIOInfo io_info;
      io_info.fd_ = tmp_file_->get_fd();
      io_info.dir_id_ = tmp_file_->get_dir_id();
      io_info.tenant_id_ = tmp_file_->get_tenant_id();
      io_info.size_ = expect_read_size_;
      io_info.buf_ = buf_;
      io_info.io_desc_ = io_flag_;
      while (OB_SUCC(ret) && size_ < expect_read_size_) {
        if (OB_FAIL(tmp_file_->once_aio_read_batch(io_info, update_offset_in_file_, last_read_offset_, *this))) {
          STORAGE_LOG(WARN, "fail to read once batch", K(ret), K(timeout_ms), K(io_info), K(*this));
        } else if (OB_FAIL(do_wait(timeout_ms))) {
          STORAGE_LOG(WARN, "fail to wait tmp file io", K(ret), K(timeout_ms));
        }
      }
    }
  }

  if (OB_SUCC(ret) || OB_ITER_END == ret) {
    has_wait_ = true;
    expect_read_size_ = 0;
    last_read_offset_ = -1;
    update_offset_in_file_ = false;
  }
  return ret;
}

int ObTmpFileIOHandle::do_wait(const int64_t timeout_ms)
{
  int ret = OB_SUCCESS;
  for (int32_t i = 0; OB_SUCC(ret) && i < block_cache_handles_.count(); i++) {
    ObBlockCacheHandle &tmp = block_cache_handles_.at(i);
    MEMCPY(tmp.buf_, tmp.block_handle_.value_->get_buffer() + tmp.offset_, tmp.size_);
    tmp.block_handle_.reset();
  }
  block_cache_handles_.reset();

  for (int32_t i = 0; OB_SUCC(ret) && i < page_cache_handles_.count(); i++) {
    ObPageCacheHandle &tmp = page_cache_handles_.at(i);
    MEMCPY(tmp.buf_, tmp.page_handle_.value_->get_buffer() + tmp.offset_, tmp.size_);
    tmp.page_handle_.reset();
  }
  page_cache_handles_.reset();

  for (int32_t i = 0; OB_SUCC(ret) && i < io_handles_.count(); i++) {
    ObIOReadHandle &tmp = io_handles_.at(i);
    if (OB_FAIL(tmp.macro_handle_.wait(timeout_ms))) {
      STORAGE_LOG(WARN, "fail to wait tmp read io", K(ret));
    } else {
      MEMCPY(tmp.buf_, tmp.macro_handle_.get_buffer() + tmp.offset_, tmp.size_);
      tmp.macro_handle_.reset();
    }
  }
  io_handles_.reset();
  return ret;
}

void ObTmpFileIOHandle::reset()
{
  for (int32_t i = 0; i < io_handles_.count(); i++) {
    io_handles_.at(i).macro_handle_.reset();
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
  buf_ = NULL;
  size_ = 0;
  tmp_file_ = NULL;
  is_read_ = false;
  has_wait_ = false;
  expect_read_size_ = 0;
  last_read_offset_ = -1;
  update_offset_in_file_ = false;
}

bool ObTmpFileIOHandle::is_valid()
{
  return NULL != tmp_file_ && NULL != buf_ && size_ >= 0;
}

ObTmpFileIOHandle::ObIOReadHandle::ObIOReadHandle() : macro_handle_(), buf_(NULL), offset_(0), size_(0)
{}

ObTmpFileIOHandle::ObIOReadHandle::ObIOReadHandle(
    const ObMacroBlockHandle& macro_handle, char* buf, const int64_t offset, const int64_t size)
    : macro_handle_(macro_handle), buf_(buf), offset_(offset), size_(size)
{}

ObTmpFileIOHandle::ObIOReadHandle::~ObIOReadHandle()
{}

ObTmpFileIOHandle::ObIOReadHandle::ObIOReadHandle(const ObTmpFileIOHandle::ObIOReadHandle& other)
{
  *this = other;
}

ObTmpFileIOHandle::ObIOReadHandle& ObTmpFileIOHandle::ObIOReadHandle::operator=(
    const ObTmpFileIOHandle::ObIOReadHandle& other)
{
  int ret = OB_SUCCESS;
  if (&other != this) {
    macro_handle_ = other.macro_handle_;
    offset_ = other.offset_;
    buf_ = other.buf_;
    size_ = other.size_;
  }
  return *this;
}
ObTmpFileIOHandle::ObPageCacheHandle::ObPageCacheHandle() : page_handle_(), buf_(NULL), offset_(0), size_(0)
{}

ObTmpFileIOHandle::ObPageCacheHandle::ObPageCacheHandle(
    const ObTmpPageValueHandle& page_handle, char* buf, const int64_t offset, const int64_t size)
    : page_handle_(page_handle), buf_(buf), offset_(offset), size_(size)
{}

ObTmpFileIOHandle::ObPageCacheHandle::~ObPageCacheHandle()
{}

ObTmpFileIOHandle::ObPageCacheHandle::ObPageCacheHandle(const ObTmpFileIOHandle::ObPageCacheHandle& other)
{
  *this = other;
}

ObTmpFileIOHandle::ObPageCacheHandle& ObTmpFileIOHandle::ObPageCacheHandle::operator=(
    const ObTmpFileIOHandle::ObPageCacheHandle& other)
{
  int ret = OB_SUCCESS;
  if (&other != this) {
    page_handle_ = other.page_handle_;
    offset_ = other.offset_;
    buf_ = other.buf_;
    size_ = other.size_;
  }
  return *this;
}

ObTmpFileIOHandle::ObBlockCacheHandle::ObBlockCacheHandle() : block_handle_(), buf_(NULL), offset_(0), size_(0)
{}

ObTmpFileIOHandle::ObBlockCacheHandle::ObBlockCacheHandle(
    const ObTmpBlockValueHandle& block_handle, char* buf, const int64_t offset, const int64_t size)
    : block_handle_(block_handle), buf_(buf), offset_(offset), size_(size)
{}

ObTmpFileIOHandle::ObBlockCacheHandle::~ObBlockCacheHandle()
{}

ObTmpFileIOHandle::ObBlockCacheHandle::ObBlockCacheHandle(const ObTmpFileIOHandle::ObBlockCacheHandle& other)
{
  *this = other;
}

ObTmpFileIOHandle::ObBlockCacheHandle& ObTmpFileIOHandle::ObBlockCacheHandle::operator=(
    const ObTmpFileIOHandle::ObBlockCacheHandle& other)
{
  int ret = OB_SUCCESS;
  if (&other != this) {
    block_handle_ = other.block_handle_;
    offset_ = other.offset_;
    buf_ = other.buf_;
    size_ = other.size_;
  }
  return *this;
}

void ObTmpFileExtent::set_global_offset(const int64_t g_offset_start, const int64_t g_offset_end)
{
  g_offset_start_ = g_offset_start;
  g_offset_end_ = g_offset_end;
}

void ObTmpFileExtent::get_global_offset(int64_t& g_offset_start, int64_t& g_offset_end) const
{
  g_offset_start = g_offset_start_;
  g_offset_end = g_offset_end_;
}

ObTmpFileExtent::ObTmpFileExtent(ObTmpFile* file)
    : is_alloced_(false),
      fd_(file->get_fd()),
      g_offset_start_(0),
      g_offset_end_(0),
      owner_(file),
      start_page_id_(-1),
      page_nums_(0),
      block_id_(-1),
      offset_(0),
      is_closed_(false)
{}

ObTmpFileExtent::~ObTmpFileExtent()
{}

int ObTmpFileExtent::read(
    const ObTmpFileIOInfo& io_info, const int64_t offset, const int64_t size, char* buf, ObTmpFileIOHandle& handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_alloced_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "ObTmpFileExtent has not been allocated", K(ret));
  } else if (offset < 0 || offset >= offset_ || size <= 0 || offset + size > offset_ || NULL == buf) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(offset), K(offset_), K(size), K(buf));
  } else {
    ObTmpBlockIOInfo info;
    info.buf_ = buf;
    info.io_desc_ = io_info.io_desc_;
    info.block_id_ = block_id_;
    info.offset_ = start_page_id_ * ObTmpMacroBlock::get_default_page_size() + offset;
    info.size_ = size;
    info.tenant_id_ = io_info.tenant_id_;
    if (OB_FAIL(OB_TMP_FILE_STORE.read(owner_->get_tenant_id(), info, handle))) {
      STORAGE_LOG(WARN, "fail to read the extent", K(ret), K(info), K(*this));
    } else {
      STORAGE_LOG(DEBUG, "debug tmp file: read extent", K(ret), K(info), K(*this));
    }
  }
  return ret;
}

int ObTmpFileExtent::write(const ObTmpFileIOInfo& io_info, int64_t& size, char*& buf)
{
  int ret = OB_SUCCESS;
  int write_size = 0;
  int64_t remain = 0;
  bool is_write = false;
  if (size <= 0 || NULL == buf) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret));
  } else if (OB_UNLIKELY(!is_alloced_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "ObTmpFileExtent has not been allocated", K(ret));
  } else if (offset_ == page_nums_ * ObTmpMacroBlock::get_default_page_size()) {
    close();
  } else {
    SpinWLockGuard guard(lock_);
    if (!is_closed()) {
      remain = page_nums_ * ObTmpMacroBlock::get_default_page_size() - offset_;
      write_size = std::min(remain, size);
      ObTmpBlockIOInfo info;
      info.block_id_ = block_id_;
      info.buf_ = buf;
      info.io_desc_ = io_info.io_desc_;
      info.offset_ = start_page_id_ * ObTmpMacroBlock::get_default_page_size() + offset_;
      info.size_ = write_size;
      info.tenant_id_ = io_info.tenant_id_;
      if (OB_FAIL(OB_TMP_FILE_STORE.write(owner_->get_tenant_id(), info))) {
        STORAGE_LOG(WARN, "fail to write the extent", K(ret));
      } else {
        offset_ += write_size;
        g_offset_end_ = offset_ + g_offset_start_;
        buf += write_size;
        size -= write_size;
        is_write = true;
        STORAGE_LOG(DEBUG, "debug tmp file: write extent", K(ret), K(info), K(*this));
      }
    }
  }
  if (is_write) {
    if (remain == write_size) {
      close();
    }
  }
  return ret;
}

void ObTmpFileExtent::reset()
{
  is_alloced_ = false;
  fd_ = -1;
  g_offset_start_ = 0;
  g_offset_end_ = 0;
  offset_ = 0;
  owner_ = NULL;
  start_page_id_ = -1;
  page_nums_ = 0;
  block_id_ = -1;
  is_closed_ = false;
}

bool ObTmpFileExtent::is_valid()
{
  return start_page_id_ >= 0 && page_nums_ >= 0 && block_id_ > 0;
}

bool ObTmpFileExtent::close(bool force)
{
  int ret = OB_SUCCESS;
  int32_t page_start_id = -1;
  int32_t page_nums = 0;
  if (!is_closed_) {
    if (close(page_start_id, page_nums, force)) {
      if (-1 == page_start_id && 0 == page_nums) {
        // nothing to do
      } else if (page_start_id < 0 || page_nums < 0) {
        ret = OB_INVALID_ARGUMENT;
        STORAGE_LOG(WARN, "fail to close the extent", K(ret), K_(block_id), K(page_start_id), K(page_nums), K_(offset));
      } else if (OB_FAIL(OB_TMP_FILE_STORE.free(owner_->get_tenant_id(), block_id_, page_start_id, page_nums))) {
        STORAGE_LOG(WARN, "fail to free", K(ret));
      }
      if (OB_FAIL(ret)) {
        unclose(page_nums);
      }
    }
  }
  return is_closed_;
}

bool ObTmpFileExtent::close(int32_t& free_page_start_id, int32_t& free_page_nums, bool force)
{
  int ret = OB_SUCCESS;
  free_page_start_id = -1;
  free_page_nums = 0;
  SpinWLockGuard guard(lock_);
  if (!is_closed_) {
    if (!force && 0 != page_nums_ && 0 == offset_) {
      // Nothing to do. This extent is alloced just now, so it cannot be closed.
    } else {
      if (offset_ != page_nums_ * ObTmpMacroBlock::get_default_page_size()) {
        int32_t offset_page_id = common::upper_align(offset_, ObTmpMacroBlock::get_default_page_size()) /
                                 ObTmpMacroBlock::get_default_page_size();
        free_page_nums = page_nums_ - offset_page_id;
        free_page_start_id = start_page_id_ + offset_page_id;
        page_nums_ -= free_page_nums;
      }
      is_closed_ = true;
    }
  }
  return is_closed_;
}

void ObTmpFileExtent::unclose(const int32_t page_nums)
{
  SpinWLockGuard guard(lock_);
  if (page_nums >= 0) {
    page_nums_ += page_nums;
  }
  is_closed_ = false;
}

ObTmpFileMeta::~ObTmpFileMeta()
{
  clear();
}

int ObTmpFileMeta::init(const int64_t fd, const int64_t dir_id, common::ObIAllocator* allocator)
{
  int ret = OB_SUCCESS;
  if (fd < 0 || dir_id < 0 || NULL == allocator) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(fd), K(dir_id));
  } else {
    fd_ = fd;
    dir_id_ = dir_id;
    allocator_ = allocator;
  }
  return ret;
}

ObTmpFileExtent* ObTmpFileMeta::get_last_extent()
{
  return extents_.count() > 0 ? extents_.at(extents_.count() - 1) : NULL;
}

int ObTmpFileMeta::deep_copy(const ObTmpFileMeta& other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(extents_.assign(other.extents_))) {
    STORAGE_LOG(WARN, "fail to assign extents array", K(ret));
  } else {
    fd_ = other.fd_;
    dir_id_ = other.dir_id_;
    allocator_ = other.allocator_;
  }
  return ret;
}

int ObTmpFileMeta::clear()
{
  // free extents
  int ret = OB_SUCCESS;
  ObTmpFileExtent* tmp = NULL;
  for (int64_t i = extents_.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
    tmp = extents_.at(i);
    if (NULL != tmp) {
      if (!tmp->is_alloced()) {
        // nothing to do.
      } else if (OB_FAIL(OB_TMP_FILE_STORE.free(tmp->get_owner().get_tenant_id(), tmp))) {
        STORAGE_LOG(WARN, "fail to free extents", K(ret));
      }
      if (OB_SUCC(ret)) {
        tmp->~ObTmpFileExtent();
        allocator_->free(tmp);
        extents_.at(i) = NULL;
      }
    }
  }
  if (OB_SUCC(ret)) {
    extents_.reset();
    allocator_ = NULL;
    fd_ = -1;
    dir_id_ = -1;
  }
  return ret;
}

ObTmpFile::ObTmpFile()
  : file_meta_(),
    is_big_(false),
    tenant_id_(-1),
    offset_(0),
    allocator_(NULL),
    last_extent_id_(0),
    last_extent_min_offset_(0),
    last_extent_max_offset_(INT64_MAX),
    is_inited_(false)
{
}

ObTmpFile::~ObTmpFile()
{
  clear();
}

int ObTmpFile::clear()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(file_meta_.clear())) {
    STORAGE_LOG(WARN, "fail to clear file meta", K(ret));
  } else {
    if (is_inited_) {
      is_big_ = false;
      tenant_id_ = -1;
      offset_ = 0;
      last_extent_id_ = 0;
      last_extent_min_offset_ = 0;
      last_extent_max_offset_ = INT64_MAX;
      allocator_ = NULL;
      is_inited_ = false;
    }
  }
  return ret;
}

int ObTmpFile::init(const int64_t fd, const int64_t dir_id, common::ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObTmpFile has not been inited", K(ret));
  } else if (fd < 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(fd));
  } else if (OB_FAIL(file_meta_.init(fd, dir_id, &allocator))) {
    STORAGE_LOG(WARN, "fail to init file meta", K(ret));
  } else {
    allocator_ = &allocator;
    is_inited_ = true;
  }

  if (!is_inited_) {
    clear();
  }
  return ret;
}

int64_t ObTmpFile::find_first_extent(const int64_t offset)
{
  common::ObIArray<ObTmpFileExtent *> &extents = file_meta_.get_extents();
  int64_t first_extent = 0;
  int64_t left = 0;
  int64_t right = extents.count() - 1;
  ObTmpFileExtent *tmp = nullptr;
  while(left < right) {
    int64_t mid = (left + right) / 2;
    tmp = extents.at(mid);
    if (tmp->get_global_start() <= offset && offset < tmp->get_global_end()) {
      first_extent = mid;
      break;
    } else if(tmp->get_global_start() > offset) {
      right = mid - 1;
    } else if(tmp->get_global_end() <= offset) {
      left = mid + 1;
    }
  }
  return first_extent;
}

int ObTmpFile::aio_read_without_lock(const ObTmpFileIOInfo &io_info, int64_t &offset, ObTmpFileIOHandle &handle)
{
  int ret = OB_SUCCESS;
  ObTmpFileExtent *tmp = nullptr;

  if (OB_ISNULL(tmp = file_meta_.get_last_extent())) {
    ret = OB_BAD_NULL_ERROR;
    STORAGE_LOG(WARN, "fail to read, because the tmp file is empty", K(ret), KP(tmp), K(io_info));
  } else if (OB_UNLIKELY(io_info.size_ > 0 && offset >= tmp->get_global_end())) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(handle.prepare_read(io_info.size_, offset, io_info.io_desc_, io_info.buf_, this))) {
    STORAGE_LOG(WARN, "fail to prepare read io handle", K(ret), K(io_info), K(offset));
  } else if (OB_FAIL(once_aio_read_batch_without_lock(io_info, offset, handle))) {
    STORAGE_LOG(WARN, "fail to read one batch", K(ret), K(offset), K(handle));
  } else {
    handle.set_last_read_offset(offset);
  }
  return ret;
}

int ObTmpFile::once_aio_read_batch(
    const ObTmpFileIOInfo &io_info, const bool need_update_offset, int64_t &offset, ObTmpFileIOHandle &handle)
{
  int ret = OB_SUCCESS;
  ObTmpFileExtent *tmp = nullptr;
  const int64_t remain_size = io_info.size_ - handle.get_data_size();

  SpinWLockGuard guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpFile has not been initialized", K(ret));
  } else if (OB_UNLIKELY(offset < 0 || remain_size < 0) || OB_ISNULL(io_info.buf_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(offset), K(remain_size), KP(io_info.buf_));
  } else if (OB_ISNULL(tmp = file_meta_.get_last_extent())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected error, null tmp file extent", K(ret), KP(tmp), K(io_info));
  } else if (OB_UNLIKELY(remain_size > 0 && offset >= tmp->get_global_end())) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(once_aio_read_batch_without_lock(io_info, offset, handle))) {
    STORAGE_LOG(WARN, "fail to read one batch", K(ret), K(offset), K(handle));
  } else {
    handle.set_last_read_offset(offset);
  }

  if (need_update_offset) {
    offset_ = offset;
  }
  return ret;
}

int ObTmpFile::once_aio_read_batch_without_lock(
    const ObTmpFileIOInfo &io_info, int64_t &offset, ObTmpFileIOHandle &handle)
{
  int ret = OB_SUCCESS;
  int64_t one_batch_read_size = 0;
  char *buf = io_info.buf_ + handle.get_data_size();
  int64_t remain_size = io_info.size_ - handle.get_data_size();
  int64_t read_size = 0;
  int64_t ith_extent = 0;
  ObTmpFileExtent *tmp = nullptr;
  common::ObIArray<ObTmpFileExtent *> &extents = file_meta_.get_extents();

  if (offset >= last_extent_min_offset_ && offset_ <= last_extent_max_offset_) {
    ith_extent = last_extent_id_;
  } else {
    ith_extent = find_first_extent(offset);
  }

  while (OB_SUCC(ret) && ith_extent < extents.count() && remain_size > 0 && one_batch_read_size < READ_SIZE_PER_BATCH) {
    tmp = extents.at(ith_extent);
    if (tmp->get_global_start() <= offset && offset < tmp->get_global_end()) {
      if (offset + remain_size > tmp->get_global_end()) {
        read_size = tmp->get_global_end() - offset;
      } else {
        read_size = remain_size;
      }
      // read from the extent.
      if (OB_FAIL(tmp->read(io_info, offset - tmp->get_global_start(), read_size, buf, handle))) {
        STORAGE_LOG(WARN, "fail to read the extent", K(ret), K(io_info), K(buf), KP_(io_info.buf));
      } else {
        offset += read_size;
        remain_size -= read_size;
        buf += read_size;
        one_batch_read_size += read_size;
        handle.add_data_size(read_size);
        last_extent_id_ = ith_extent;
        last_extent_min_offset_ = tmp->get_global_start();
        last_extent_max_offset_ = tmp->get_global_end();
      }
    }
    ++ith_extent;
  }
  return ret;
}

int ObTmpFile::aio_read(const ObTmpFileIOInfo& io_info, ObTmpFileIOHandle& handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpFile has not been inited", K(ret));
  } else {
    tenant_id_ = io_info.tenant_id_;
    SpinWLockGuard guard(lock_);
    if (OB_FAIL(aio_read_without_lock(io_info, offset_, handle))) {
      if (OB_ITER_END != ret) {
        STORAGE_LOG(WARN, "fail to do aio read without lock", K(ret));
      }
    } else {
      handle.set_update_offset_in_file();
    }
  }
  return ret;
}
int ObTmpFile::aio_pread(const ObTmpFileIOInfo& io_info, const int64_t offset, ObTmpFileIOHandle& handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpFile has not been inited", K(ret));
  } else {
    int64_t tmp_offset = offset;
    SpinRLockGuard guard(lock_);
    if (OB_FAIL(aio_read_without_lock(io_info, tmp_offset, handle))) {
      if (OB_ITER_END != ret) {
        STORAGE_LOG(WARN, "fail to do aio read without lock", K(ret));
      }
    }
  }
  return ret;
}

int ObTmpFile::seek(const int64_t offset, const int whence)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpFile has not been inited", K(ret));
  } else {
    switch (whence) {
      case SET_SEEK:
        offset_ = offset;
        break;
      case CUR_SEEK:
        offset_ += offset;
        break;
      default:
        ret = OB_NOT_SUPPORTED;
        STORAGE_LOG(WARN, "not supported whence", K(ret), K(whence));
    }
  }
  return ret;
}

int ObTmpFile::read(const ObTmpFileIOInfo& io_info, const int64_t timeout_ms, ObTmpFileIOHandle& handle)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(aio_read(io_info, handle))) {
    if (OB_ITER_END != ret) {
      STORAGE_LOG(WARN, "fail to read data using asynchronous io", K(ret), K(io_info));
    } else {
      if (OB_FAIL(handle.wait(timeout_ms))) {
        STORAGE_LOG(WARN, "fail to wait io finish", K(ret), K(timeout_ms));
      } else {
        ret = OB_ITER_END;
      }
    }
  } else if (OB_FAIL(handle.wait(timeout_ms))) {
    if (OB_ITER_END != ret) {
      STORAGE_LOG(WARN, "fail to wait io finish", K(ret), K(timeout_ms));
    }
  }
  return ret;
}

int ObTmpFile::pread(
    const ObTmpFileIOInfo& io_info, const int64_t offset, const int64_t timeout_ms, ObTmpFileIOHandle& handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpFile has not been inited", K(ret));
  } else if (OB_FAIL(aio_pread(io_info, offset, handle))) {
    if (OB_ITER_END != ret) {
      STORAGE_LOG(WARN, "fail to read data using asynchronous io", K(ret), K(io_info));
    } else {
      if (OB_FAIL(handle.wait(timeout_ms))) {
        STORAGE_LOG(WARN, "fail to wait io finish", K(ret), K(timeout_ms));
      } else {
        ret = OB_ITER_END;
      }
    }
  } else if (OB_FAIL(handle.wait(timeout_ms))) {
    if (OB_ITER_END != ret) {
      STORAGE_LOG(WARN, "fail to wait io finish", K(ret), K(timeout_ms));
    }
  }
  return ret;
}

int ObTmpFile::aio_write(const ObTmpFileIOInfo& io_info, ObTmpFileIOHandle& handle)
{
  // only support append at present.
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpFile has not been inited", K(ret));
  } else if (!io_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(io_info), K(handle));
  } else if (OB_FAIL(handle.prepare_write(io_info.buf_, io_info.size_, this))) {
    STORAGE_LOG(WARN, "fail to prepare write io handle", K(ret));
  } else {
    tenant_id_ = io_info.tenant_id_;
    int64_t size = io_info.size_;
    char* buf = io_info.buf_;
    SpinWLockGuard guard(lock_);
    ObTmpFileExtent* tmp = file_meta_.get_last_extent();
    void* buff = NULL;
    ObTmpFileExtent* extent = NULL;
    while (OB_SUCC(ret) && size > 0) {
      if (NULL != tmp && tmp->is_alloced() && !tmp->is_closed()) {
        if (OB_FAIL(write_file_extent(io_info, tmp, size, buf))) {
          STORAGE_LOG(WARN, "fail to write the extent", K(ret));
        }
      } else if (NULL == tmp || (tmp->is_alloced() && tmp->is_closed())) {
        int64_t alloc_size = 0;
        // Pre-Allocation Strategy:
        // 1. small file
        //    a.  0KB < size <= 32KB, alloc_size = 32KB;
        //    b. 32KB < size <= 64KB, alloc_size = 64KB;
        //    c. 64KB < size        , alloc_size = size;
        // 2. big file
        //    a. 32KB < size <= 64KB, alloc_size = 64KB;
        //    b. 64KB < size        , alloc_size = size;
        //
        // NOTE: if the size is more than block size, it will be split into
        // multiple allocation.
        if (size <= big_file_prealloc_size()) {
          if (!is_big_ && size <= small_file_prealloc_size()) {
            alloc_size = common::upper_align(size, small_file_prealloc_size());
          } else {
            alloc_size = common::upper_align(size, big_file_prealloc_size());
          }
        } else if (size > OB_TMP_FILE_STORE.get_block_size()) {
          alloc_size = OB_TMP_FILE_STORE.get_block_size();
        } else {
          alloc_size = size;
        }
        if (OB_ISNULL(buff = allocator_->alloc(sizeof(ObTmpFileExtent)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          STORAGE_LOG(WARN, "fail to alloc a buf", K(ret));
        } else if (OB_ISNULL(extent = new (buff) ObTmpFileExtent(this))) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "fail to new a ObTmpFileExtent", K(ret));
        } else if (OB_FAIL(OB_TMP_FILE_STORE.alloc(get_dir_id(), tenant_id_, alloc_size, *extent))) {
          STORAGE_LOG(WARN, "fail to allocate extents", K(ret), K_(tenant_id));
        } else {
          if (NULL != tmp) {
            extent->set_global_offset(tmp->get_global_end(), tmp->get_global_end());
          }
          tmp = extent;
          if (OB_FAIL(write_file_extent(io_info, tmp, size, buf))) {
            STORAGE_LOG(WARN, "fail to write the extent", K(ret));
          } else if (OB_FAIL(file_meta_.push_back_extent(tmp))) {
            STORAGE_LOG(WARN, "fail to push the extent", K(ret));
          } else {
            extent = NULL;
            buff = NULL;
          }
        }
        // If it fails, the unused extent should be returned to the macro block and cleaned up.
        if (OB_FAIL(ret)) {
          if (OB_NOT_NULL(extent)) {
            if (extent->is_valid()) {
              OB_TMP_FILE_STORE.free(tenant_id_, extent);
              extent->~ObTmpFileExtent();
            }
            allocator_->free(extent);
            extent = NULL;
          }
        }
      } else {
        // this extent has allocated, no one page, invalid extent(tmp->is_alloced = false).
        ret = OB_INVALID_ERROR;
        STORAGE_LOG(WARN, "invalid extent", K(ret));
      }
    }
    handle.sub_data_size(io_info.size_ - size);
    if (OB_SUCC(ret) && !is_big_) {
      is_big_ = tmp->get_global_end() >= SMALL_FILE_MAX_THRESHOLD * ObTmpMacroBlock::get_default_page_size();
    }
  }
  return ret;
}

int64_t ObTmpFile::get_dir_id() const
{
  return file_meta_.get_dir_id();
}

uint64_t ObTmpFile::get_tenant_id() const
{
  return tenant_id_;
}

int64_t ObTmpFile::get_fd() const
{
  return file_meta_.get_fd();
}

int ObTmpFile::sync(const int64_t timeout_ms)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  ObTmpFileExtent* tmp = file_meta_.get_last_extent();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpFile has not been inited", K(ret));
  } else if (NULL == tmp) {
    ret = OB_BAD_NULL_ERROR;
    STORAGE_LOG(WARN, "the file does not have a extent", K(timeout_ms));
  } else if (tmp->is_closed()) {
    STORAGE_LOG(INFO, "the file has been closed", K(timeout_ms));
  } else {
    tmp->close(true /*force*/);
  }
  return ret;
}

int ObTmpFile::deep_copy(char* buf, const int64_t buf_len, ObTmpFile*& value) const
{
  int ret = OB_SUCCESS;
  const int64_t deep_copy_size = get_deep_copy_size();
  if (OB_ISNULL(buf) || buf_len < deep_copy_size) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(buf), K(buf_len));
  } else {
    ObTmpFile* pvalue = new (buf) ObTmpFile();
    if (OB_FAIL(pvalue->file_meta_.deep_copy(file_meta_))) {
      STORAGE_LOG(WARN, "fail to deep copy meta", K(ret));
    } else {
      pvalue->is_big_ = is_big_;
      pvalue->offset_ = offset_;
      pvalue->tenant_id_ = tenant_id_;
      pvalue->allocator_ = allocator_;
      pvalue->is_inited_ = is_inited_;
      value = pvalue;
    }
  }
  return ret;
}

int64_t ObTmpFile::get_deep_copy_size() const
{
  return sizeof(*this);
}

int ObTmpFile::write_file_extent(
    const ObTmpFileIOInfo& io_info, ObTmpFileExtent* file_extent, int64_t& size, char*& buf)
{
  int ret = OB_SUCCESS;
  if (NULL == file_extent || size <= 0 || NULL == buf) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret));
  } else if (OB_FAIL(file_extent->write(io_info, size, buf))) {
    STORAGE_LOG(WARN, "fail to write the extent", K(ret), K(size), KP(buf));
  }
  return ret;
}

int ObTmpFile::write(const ObTmpFileIOInfo& io_info, const int64_t timeout_ms)
{
  int ret = OB_SUCCESS;
  ObTmpFileIOHandle handle;
  if (OB_FAIL(aio_write(io_info, handle))) {
    STORAGE_LOG(WARN, "fail to write using asynchronous io", K(ret), K(io_info));
  } else if (OB_FAIL(handle.wait(timeout_ms))) {
    STORAGE_LOG(WARN, "fail to wait io finish", K(ret));
  }
  return ret;
}

int64_t ObTmpFile::small_file_prealloc_size()
{
  return SMALL_FILE_MAX_THRESHOLD * ObTmpMacroBlock::get_default_page_size();
}

int64_t ObTmpFile::big_file_prealloc_size()
{
  return BIG_FILE_PREALLOC_EXTENT_SIZE * ObTmpMacroBlock::get_default_page_size();
}

ObTmpFileHandle::ObTmpFileHandle() : ObResourceHandle<ObTmpFile>()
{}

ObTmpFileHandle::~ObTmpFileHandle()
{
  reset();
}

void ObTmpFileHandle::reset()
{
  if (NULL != ptr_) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = ObTmpFileManager::get_instance().dec_handle_ref(*this))) {
      STORAGE_LOG(WARN, "fail to decrease handle reference count", K(tmp_ret));
    }
  }
}

ObTmpFileManager& ObTmpFileManager::get_instance()
{
  static ObTmpFileManager instance;
  return instance;
}

int ObTmpFileManager::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObTmpFileManager has not been inited", K(ret));
  } else if (OB_FAIL(
                 files_.init(DEFAULT_BUCKET_NUM, ObModIds::OB_TMP_FILE_MANAGER, TOTAL_LIMIT, HOLD_LIMIT, BLOCK_SIZE))) {
    STORAGE_LOG(WARN, "fail to init map for temporary files", K(ret));
  } else if (OB_FAIL(OB_FILE_SYSTEM.alloc_file(storage_file_.file_))) {
    STORAGE_LOG(WARN, "fail to alloc storage file, ", K(ret));
  } else if (OB_FAIL(storage_file_.file_->init(GCTX.self_addr_, ObStorageFile::FileType::TMP_FILE))) {
    STORAGE_LOG(WARN, "fail to init storage file, ", K(ret), K(GCTX.self_addr_));
  } else {
    storage_file_.inc_ref();
    file_handle_.set_storage_file_with_ref(storage_file_);
    if (OB_FAIL(OB_TMP_FILE_STORE.init(file_handle_))) {
      STORAGE_LOG(WARN, "fail to init the block manager for temporary files", K(ret));
    } else {
      is_inited_ = true;
    }
  }

  if (!is_inited_) {
    destroy();
  }
  return ret;
}

int ObTmpFileManager::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpFileManager has not been inited", K(ret));
  } else if (OB_FAIL(storage_file_.file_->open(ObFileSystemUtil::CREATE_FLAGS))) {
    if (OB_FILE_OR_DIRECTORY_EXIST == ret) {
      ret = OB_SUCCESS;
      STORAGE_LOG(WARN, "storage file already created", K(ret));
      if (OB_FAIL(storage_file_.file_->unlink())) {
        STORAGE_LOG(WARN, "delete old storage file fail", K(ret), K(*storage_file_.file_));
      } else if (OB_FAIL(storage_file_.file_->open(ObFileSystemUtil::CREATE_FLAGS))) {
        STORAGE_LOG(WARN, "create new storage file fail", K(ret), K(*storage_file_.file_));
      }
    } else {
      STORAGE_LOG(WARN, "create storage file fail", K(ret), K(*storage_file_.file_));
    }
  }

  if (OB_SUCC(ret)) {
    storage_file_.file_->enable_mark_and_sweep();
  }
  return ret;
}

int ObTmpFileManager::alloc_dir(int64_t& dir)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpFileManager has not been inited", K(ret));
  } else if (OB_FAIL(get_next_dir(dir))) {
    STORAGE_LOG(WARN, "The directory of ObTmpFileManager has been used up", K(ret));
  }
  return ret;
}

int ObTmpFileManager::get_next_dir(int64_t& next_dir)
{
  int ret = OB_SUCCESS;
  next_value(next_dir_, next_dir);
  if (INT64_MAX - 1 == next_dir_) {
    ret = OB_SIZE_OVERFLOW;
  }
  return ret;
}

void ObTmpFileManager::next_value(int64_t& current_val, int64_t& next_val)
{
  int64_t old_val = ATOMIC_LOAD(&current_val);
  int64_t new_val = 0;
  bool finish = false;
  while (!finish) {
    new_val = (old_val + 1) % INT64_MAX;
    next_val = new_val;
    finish = (old_val == (new_val = ATOMIC_VCAS(&current_val, old_val, new_val)));
    old_val = new_val;
  }
}

int ObTmpFileManager::open(int64_t& fd, int64_t& dir)
{
  int ret = OB_SUCCESS;
  ObTmpFile file;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpFileManager has not been inited", K(ret));
  } else if (OB_FAIL(get_next_fd(fd))) {
    STORAGE_LOG(WARN, "fail to get next fd", K(ret));
  } else if (OB_FAIL(file.init(fd, dir, files_.get_allocator()))) {
    STORAGE_LOG(WARN, "fail to open file", K(ret));
  } else if (OB_FAIL(files_.set(fd, file))) {
    STORAGE_LOG(WARN, "fail to set tmp file", K(ret));
  } else {
    ObTaskController::get().allow_next_syslog();
    STORAGE_LOG(INFO, "succeed to open a tmp file", K(fd), K(dir), K(common::lbt()));
  }
  return ret;
}

int ObTmpFileManager::get_next_fd(int64_t& next_fd)
{
  int ret = OB_SUCCESS;
  ObTmpFileHandle handle;
  next_value(next_fd_, next_fd);
  if (OB_FAIL(files_.get(next_fd, handle))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      STORAGE_LOG(WARN, "fail to get from resource map", K(ret), K(next_fd));
    } else {
      ret = OB_SUCCESS;
    }
  } else {
    ret = OB_FILE_ALREADY_EXIST;
    STORAGE_LOG(WARN, "too much file", K(ret));
  }
  return ret;
}

int ObTmpFileManager::aio_read(const ObTmpFileIOInfo& io_info, ObTmpFileIOHandle& handle)
{
  int ret = OB_SUCCESS;
  ObTmpFileHandle file_handle;
  handle.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpFileManager has not been inited", K(ret));
  } else if (!io_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(io_info));
  } else if (OB_FAIL(files_.get(io_info.fd_, file_handle))) {
    STORAGE_LOG(WARN, "fail to get tmp file handle", K(ret));
  } else if (OB_FAIL(file_handle.get_resource_ptr()->aio_read(io_info, handle))) {
    if (common::OB_ITER_END != ret) {
      STORAGE_LOG(WARN, "fail to do asynchronous read", K(ret), K(io_info));
    }
  }
  return ret;
}

int ObTmpFileManager::aio_pread(const ObTmpFileIOInfo& io_info, const int64_t offset, ObTmpFileIOHandle& handle)
{
  int ret = OB_SUCCESS;
  ObTmpFileHandle file_handle;
  handle.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpFileManager has not been inited", K(ret));
  } else if (!io_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(io_info));
  } else if (OB_FAIL(files_.get(io_info.fd_, file_handle))) {
    STORAGE_LOG(WARN, "fail to get tmp file handle", K(ret));
  } else if (OB_FAIL(file_handle.get_resource_ptr()->aio_pread(io_info, offset, handle))) {
    if (common::OB_ITER_END != ret) {
      STORAGE_LOG(WARN, "fail to do asynchronous pread", K(ret), K(io_info), K(offset));
    }
  }
  return ret;
}

int ObTmpFileManager::read(const ObTmpFileIOInfo& io_info, const int64_t timeout_ms, ObTmpFileIOHandle& handle)
{
  int ret = OB_SUCCESS;
  ObTmpFileHandle file_handle;
  handle.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpFileManager has not been inited", K(ret));
  } else if (!io_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(io_info));
  } else if (OB_FAIL(files_.get(io_info.fd_, file_handle))) {
    STORAGE_LOG(WARN, "fail to get temporary file handle", K(ret), K(io_info));
  } else if (OB_FAIL(file_handle.get_resource_ptr()->read(io_info, timeout_ms, handle))) {
    if (OB_ITER_END != ret) {
      STORAGE_LOG(WARN, "fail to read", K(ret), K(io_info));
    }
  }
  return ret;
}

int ObTmpFileManager::pread(
    const ObTmpFileIOInfo& io_info, const int64_t offset, const int64_t timeout_ms, ObTmpFileIOHandle& handle)
{
  int ret = OB_SUCCESS;
  ObTmpFileHandle file_handle;
  handle.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpFileManager has not been inited", K(ret));
  } else if (!io_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(io_info));
  } else if (OB_FAIL(files_.get(io_info.fd_, file_handle))) {
    STORAGE_LOG(WARN, "fail to get tmp file handle", K(ret), K(io_info));
  } else if (OB_FAIL(file_handle.get_resource_ptr()->pread(io_info, offset, timeout_ms, handle))) {
    if (OB_ITER_END != ret) {
      STORAGE_LOG(WARN, "fail to pread", K(ret), K(io_info));
    }
  }
  return ret;
}

int ObTmpFileManager::aio_write(const ObTmpFileIOInfo& io_info, ObTmpFileIOHandle& handle)
{
  int ret = OB_SUCCESS;
  ObTmpFileHandle file_handle;
  handle.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpFileManager has not been inited", K(ret));
  } else if (!io_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret));
  } else if (OB_FAIL(files_.get(io_info.fd_, file_handle))) {
    STORAGE_LOG(WARN, "fail to get tmp file handle", K(ret), K(io_info));
  } else if (OB_FAIL(file_handle.get_resource_ptr()->aio_write(io_info, handle))) {
    STORAGE_LOG(WARN, "fail to aio_write", K(ret), K(io_info));
  }
  return ret;
}

int ObTmpFileManager::write(const ObTmpFileIOInfo& io_info, const int64_t timeout_ms)
{
  int ret = OB_SUCCESS;
  ObTmpFileHandle file_handle;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpFileManager has not been inited", K(ret));
  } else if (!io_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(io_info));
  } else if (OB_FAIL(files_.get(io_info.fd_, file_handle))) {
    STORAGE_LOG(WARN, "fail to get temporary file handle", K(ret), K(io_info));
  } else if (OB_FAIL(file_handle.get_resource_ptr()->write(io_info, timeout_ms))) {
    STORAGE_LOG(WARN, "fail to write", K(ret), K(io_info));
  }
  return ret;
}

int ObTmpFileManager::seek(const int64_t fd, const int64_t offset, const int whence)
{
  int ret = OB_SUCCESS;
  ObTmpFileHandle file_handle;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpFileManager has not been inited", K(ret));
  } else if (OB_FAIL(files_.get(fd, file_handle))) {
    STORAGE_LOG(WARN, "fail to get tmp file handle", K(ret), K(fd));
  } else if (OB_FAIL(file_handle.get_resource_ptr()->seek(offset, whence))) {
    STORAGE_LOG(WARN, "fail to seek file", K(ret));
  }
  return ret;
}

int ObTmpFileManager::remove(const int64_t fd)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpFileManager has not been inited", K(ret));
  } else {
    common::SpinWLockGuard guard(rm_file_lock_);
    ObTmpFileHandle file_handle;
    if (OB_FAIL(files_.get(fd, file_handle))) {
      if (common::OB_ENTRY_NOT_EXIST != ret) {
        STORAGE_LOG(WARN, "fail to get tmp file handle", K(ret), K(fd));
      } else {
        ret = OB_SUCCESS;
        STORAGE_LOG(INFO, "this tmp file has been removed", K(fd), K(common::lbt()));
      }
    } else if (OB_FAIL(files_.erase(fd))) {
      STORAGE_LOG(WARN, "fail to erase from map", K(ret));
    } else {
      ObTaskController::get().allow_next_syslog();
      STORAGE_LOG(INFO, "succeed to remove a tmp file", K(fd), K(common::lbt()));
    }
  }
  return ret;
}

int ObTmpFileManager::remove_tenant_file(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<int64_t, 32> fd_list;
  RmTenantTmpFileOp rm_tenant_file_op(tenant_id, &fd_list);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpFileManager has not been inited", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(files_.foreach (rm_tenant_file_op))) {
    STORAGE_LOG(WARN, "fail to foreach files_", K(ret), K(tenant_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < fd_list.count(); i++) {
      if (OB_FAIL(remove(fd_list.at(i)))) {
        STORAGE_LOG(WARN, "fail to remove tmp file", K(ret), K(fd_list.at(i)), K(i));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(OB_TMP_FILE_STORE.free_tenant_file_store(tenant_id))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          STORAGE_LOG(WARN, "fail to free tmp tenant file store", K(ret), K(tenant_id));
        }
      }
    }
  }
  return ret;
}

int ObTmpFileManager::get_all_tenant_id(common::ObIArray<uint64_t> &tenant_ids)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpFileManager has not been inited", K(ret));
  } else if (OB_FAIL(OB_TMP_FILE_STORE.get_all_tenant_id(tenant_ids))) {
    STORAGE_LOG(WARN, "fail to get all tenant ids", K(ret));
  }
  return ret;
}

int ObTmpFileManager::sync(const int64_t fd, const int64_t timeout_ms)
{
  int ret = OB_SUCCESS;
  ObTmpFileHandle file_handle;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpFileManager has not been inited", K(ret));
  } else if (OB_FAIL(files_.get(fd, file_handle))) {
    STORAGE_LOG(WARN, "fail to get tmp file handle", K(ret), K(fd));
  } else if (OB_FAIL(file_handle.get_resource_ptr()->sync(timeout_ms))) {
    STORAGE_LOG(WARN, "fail to close file", K(ret));
  }
  return ret;
}

ObTmpFileManager::ObTmpFileManager()
    : files_(), storage_file_(), file_handle_(), next_fd_(-1), next_dir_(-1), rm_file_lock_(), is_inited_(false)
{}

ObTmpFileManager::~ObTmpFileManager()
{
  destroy();
}

void ObTmpFileManager::destroy()
{
  files_.destroy();
  OB_TMP_FILE_STORE.destroy();
  if (OB_NOT_NULL(storage_file_.file_)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = storage_file_.file_->close())) {
      STORAGE_LOG(WARN, "fail to close storage file, ", K(tmp_ret));
    }
    if (OB_SUCCESS != (tmp_ret = storage_file_.file_->unlink())) {
      STORAGE_LOG(WARN, "fail to delete storage file, ", K(tmp_ret), K(*storage_file_.file_));
    }
    if (OB_SUCCESS != (tmp_ret = OB_FILE_SYSTEM.free_file(storage_file_.file_))) {
      STORAGE_LOG(WARN, "fail to free storage file, ", K(tmp_ret));
    }
    storage_file_.dec_ref();
    storage_file_.file_ = NULL;
  }
  next_fd_ = -1;
  next_dir_ = -1;
  is_inited_ = false;
}

int ObTmpFileManager::dec_handle_ref(ObTmpFileHandle& handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpFileManager has not been inited", K(ret));
  } else if (OB_FAIL(files_.dec_handle_ref(handle.ptr_))) {
    STORAGE_LOG(WARN, "fail to dec handle ref without lock", K(ret));
  }
  return ret;
}

}  // end namespace blocksstable
}  // end namespace oceanbase
