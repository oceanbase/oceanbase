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
#include "share/ob_task_define.h"

namespace oceanbase
{
using namespace storage;
using namespace share;

namespace blocksstable
{

ObTmpFileIOInfo::ObTmpFileIOInfo()
    : fd_(0), dir_id_(0), size_(0), io_timeout_ms_(DEFAULT_IO_WAIT_TIME_MS),
      tenant_id_(OB_INVALID_TENANT_ID), buf_(NULL), io_desc_(),
      disable_page_cache_(false)
{
}

ObTmpFileIOInfo::~ObTmpFileIOInfo()
{
}

void ObTmpFileIOInfo::reset()
{
  fd_ = 0;
  dir_id_ = 0;
  size_ = 0;
  io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  tenant_id_ = OB_INVALID_TENANT_ID;
  buf_ = NULL;
}

bool ObTmpFileIOInfo::is_valid() const
{
  return fd_ >= 0 && dir_id_ >= 0 && size_ > 0 && OB_INVALID_TENANT_ID != tenant_id_
        && NULL != buf_ && io_desc_.is_valid() && io_timeout_ms_ > 0;
}

ObTmpFileIOHandle::ObTmpFileIOHandle()
  : io_handles_(),
    page_cache_handles_(),
    block_cache_handles_(),
    write_block_ids_(),
    fd_(OB_INVALID_FD),
    dir_id_(OB_INVALID_ID),
    tenant_id_(OB_INVALID_TENANT_ID),
    buf_(NULL),
    size_(0),
    is_read_(false),
    has_wait_(false),
    is_finished_(false),
    disable_page_cache_(false),
    ret_code_(OB_SUCCESS),
    expect_read_size_(0),
    last_read_offset_(-1),
    io_flag_(),
    update_offset_in_file_(false),
    last_fd_(OB_INVALID_FD),
    last_extent_id_(0)
{
  io_handles_.set_attr(ObMemAttr(MTL_ID(), "TMP_IO_HDL"));
  page_cache_handles_.set_attr(ObMemAttr(MTL_ID(), "TMP_PCACHE_HDL"));
  block_cache_handles_.set_attr(ObMemAttr(MTL_ID(), "TMP_BCACHE_HDL"));
}

ObTmpFileIOHandle::~ObTmpFileIOHandle()
{
  reset();
}

int ObTmpFileIOHandle::prepare_read(
    const int64_t read_size,
    const int64_t read_offset,
    const common::ObIOFlag io_flag,
    char *read_buf,
    int64_t fd,
    int64_t dir_id,
    uint64_t tenant_id,
    bool disable_page_cache)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(read_buf)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP_(buf));
  } else {
    buf_ = read_buf;
    size_ = 0;
    fd_ = fd;
    dir_id_ = dir_id;
    tenant_id_ = tenant_id;
    is_read_ = true;
    has_wait_ = false;
    expect_read_size_ = read_size;
    last_read_offset_ = read_offset;
    io_flag_ = io_flag;
    disable_page_cache_ = disable_page_cache;
    if (last_fd_ != fd_) {
      last_fd_ = fd_;
      last_extent_id_ = 0;
    }
  }
  return ret;
}

int ObTmpFileIOHandle::prepare_write(
    char *write_buf,
    const int64_t write_size,
    int64_t fd,
    int64_t dir_id,
    uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  const int64_t bkt_cnt = 17;
  lib::ObMemAttr bkt_mem_attr(tenant_id, "TmpBlkIDBkt");
  if (OB_ISNULL(write_buf)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP_(buf));
  } else if (OB_FAIL(write_block_ids_.create(bkt_cnt, bkt_mem_attr))) {
    STORAGE_LOG(WARN, "create write block id set failed", K(ret), K(bkt_cnt));
  } else {
    buf_ = write_buf;
    size_ = write_size;
    fd_ = fd;
    dir_id_ = dir_id;
    tenant_id_ = tenant_id;
    is_read_ = false;
    has_wait_ = false;
    expect_read_size_ = 0;
    last_read_offset_ = -1;
    io_flag_.reset();
  }
  return ret;
}

int ObTmpFileIOHandle::wait()
{
  int ret = OB_SUCCESS;
  const int64_t timeout_ms = std::max(GCONF._data_storage_io_timeout / 1000, DEFAULT_IO_WAIT_TIME_MS);
  if (timeout_ms < 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument. timeout must be positive", K(ret), K(timeout_ms));
  } else if (!is_finished_) {
    if (is_read_ && OB_FAIL(wait_read_finish(timeout_ms))) {
      STORAGE_LOG(WARN, "wait read finish failed", K(ret), K(timeout_ms), K(is_read_));
    } else if (!is_read_ && OB_FAIL(wait_write_finish(timeout_ms))) {
      STORAGE_LOG(WARN, "wait write finish failed", K(ret), K(timeout_ms), K(is_read_));
    }
    ret_code_ = ret;
    is_finished_ = true;
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ret_code_)) {
      STORAGE_LOG(WARN, "tmp file io error", K(ret), KPC(this));
    }
  }

  return ret;
}

int ObTmpFileIOHandle::wait_write_finish(int64_t timeout_ms)
{
  int ret = OB_SUCCESS;
  if (write_block_ids_.size() == 0) {
    STORAGE_LOG(DEBUG, "write block ids size is 0", K(ret), K(timeout_ms));
  } else {
    // iter all blocks, execute wait
    common::hash::ObHashSet<int64_t>::const_iterator iter;
    int64_t begin_us = ObTimeUtility::fast_current_time();
    int64_t wait_ms = timeout_ms;
    for (iter = write_block_ids_.begin(); OB_SUCC(ret) && iter != write_block_ids_.end(); ++iter) {
      const int64_t &blk_id = iter->first;
      if (OB_FAIL(OB_TMP_FILE_STORE.wait_write_finish(tenant_id_, blk_id, wait_ms))) {
        STORAGE_LOG(WARN, "fail to wait write finish", K(ret), K(blk_id), K(timeout_ms));
      }
      wait_ms = timeout_ms - (ObTimeUtility::fast_current_time() - begin_us) / 1000;
      if (OB_SUCC(ret) && OB_UNLIKELY(wait_ms <= 0)) {
        ret = OB_TIMEOUT;
        STORAGE_LOG(WARN, "fail to wait tmp file write finish", K(ret), K(wait_ms), K(blk_id), K(timeout_ms));
      }
    }
    int bret = OB_SUCCESS;
    if (OB_UNLIKELY(OB_SUCCESS != (bret = write_block_ids_.destroy()))) {
      STORAGE_LOG(WARN, "fail to destroy write block id set", K(bret), K(wait_ms), K(timeout_ms));
    }
  }
  return ret;
}

int ObTmpFileIOHandle::wait_read_finish(const int64_t timeout_ms)
{
  int ret = OB_SUCCESS;
  ObTmpFileHandle file_handle;
  if (OB_UNLIKELY(has_wait_ && is_read_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "read wait() isn't reentrant interface, shouldn't call again", K(ret));
  } else if (OB_FAIL(do_read_wait(timeout_ms))) {
    STORAGE_LOG(WARN, "fail to wait tmp file io", K(ret), K(timeout_ms));
  } else if (is_read_ && !has_wait_) {
    if (size_ == expect_read_size_) {
      //do nothing
    } else if (OB_UNLIKELY(size_ > expect_read_size_)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "read size more than expected size", K(ret), K(timeout_ms));
    } else if (OB_FAIL(FILE_MANAGER_INSTANCE_V2.get_tmp_file_handle(fd_, file_handle))) {
      STORAGE_LOG(WARN, "fail to get tmp file handle", K(ret));
    } else {
      ObTmpFileIOInfo io_info;
      io_info.fd_ = fd_;
      io_info.dir_id_ = dir_id_;
      io_info.tenant_id_ = tenant_id_;
      io_info.size_ = expect_read_size_;
      io_info.buf_ = buf_;
      io_info.io_desc_ = io_flag_;
      io_info.io_timeout_ms_ = timeout_ms;
      while (OB_SUCC(ret) && size_ < expect_read_size_) {
        if (OB_FAIL(file_handle.get_resource_ptr()->once_aio_read_batch(io_info,
                                                    update_offset_in_file_,
                                                    last_read_offset_,
                                                    *this))) {
          STORAGE_LOG(WARN, "fail to read once batch", K(ret), K(timeout_ms), K(io_info), K(*this));
        } else if (OB_FAIL(do_read_wait(timeout_ms))) {
          STORAGE_LOG(WARN, "fail to wait tmp file io", K(ret), K(timeout_ms));
        }
      }
    }
  }

  if (OB_SUCC(ret) || OB_ITER_END == ret) {
    has_wait_ = true;
    expect_read_size_ = 0;
    last_read_offset_ = -1;
    io_flag_.reset();
    update_offset_in_file_ = false;
  }
  return ret;
}

int ObTmpFileIOHandle::do_read_wait(const int64_t timeout_ms)
{
  int ret = OB_SUCCESS;
  for (int32_t i = 0; OB_SUCC(ret) && i < block_cache_handles_.count(); i++) {
    ObBlockCacheHandle &tmp = block_cache_handles_.at(i);
    MEMCPY(tmp.buf_, tmp.block_handle_.value_->get_buffer() + tmp.offset_, tmp.size_);
    tmp.block_handle_.reset();
  }
  if (0 != block_cache_handles_.count()) {
    OB_TMP_FILE_STORE.dec_block_cache_num(tenant_id_, block_cache_handles_.count());
  }
  block_cache_handles_.reset();

  for (int32_t i = 0; OB_SUCC(ret) && i < page_cache_handles_.count(); i++) {
    ObPageCacheHandle &tmp = page_cache_handles_.at(i);
    MEMCPY(tmp.buf_, tmp.page_handle_.value_->get_buffer() + tmp.offset_, tmp.size_);
    tmp.page_handle_.reset();
  }
  if (0 != page_cache_handles_.count()) {
    OB_TMP_FILE_STORE.dec_page_cache_num(tenant_id_, page_cache_handles_.count());
  }
  page_cache_handles_.reset();

  for (int32_t i = 0; OB_SUCC(ret) && i < io_handles_.count(); i++) {
    ObIOReadHandle &tmp = io_handles_.at(i);
    if (OB_FAIL(tmp.macro_handle_.wait())) {
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
  for (int32_t i = 0; i < block_cache_handles_.count(); i++) {
    block_cache_handles_.at(i).block_handle_.reset();
  }
  if (0 != block_cache_handles_.count()) {
    OB_TMP_FILE_STORE.dec_block_cache_num(tenant_id_, block_cache_handles_.count());
  }
  for (int32_t i = 0; i < page_cache_handles_.count(); i++) {
    page_cache_handles_.at(i).page_handle_.reset();
  }
  if (0 != page_cache_handles_.count()) {
    OB_TMP_FILE_STORE.dec_page_cache_num(tenant_id_, page_cache_handles_.count());
  }
  io_handles_.reset();
  page_cache_handles_.reset();
  block_cache_handles_.reset();
  write_block_ids_.destroy();
  fd_ = OB_INVALID_FD;
  dir_id_ = OB_INVALID_ID;
  tenant_id_ = OB_INVALID_TENANT_ID;
  buf_ = NULL;
  size_ = 0;
  is_read_ = false;
  has_wait_ = false;
  expect_read_size_ = 0;
  last_read_offset_ = -1;
  io_flag_.reset();
  update_offset_in_file_ = false;
  is_finished_ = false;
  ret_code_ = OB_SUCCESS;
}

bool ObTmpFileIOHandle::is_valid() const
{
  return OB_INVALID_FD != fd_ && OB_INVALID_ID != dir_id_ && OB_INVALID_TENANT_ID != tenant_id_
      && NULL != buf_ && size_ >= 0;
}

int ObTmpFileIOHandle::record_block_id(const int64_t block_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(write_block_ids_.set_refactored(block_id, 1))) {
    STORAGE_LOG(WARN, "record block id failed", K(ret), K(block_id));
  }
  return ret;
}

void ObTmpFileIOHandle::set_last_extent_id(const int64_t last_extent_id)
{
  last_extent_id_ = last_extent_id;
}

int64_t ObTmpFileIOHandle::get_last_extent_id() const
{
  return last_extent_id_;
}

ObTmpFileIOHandle::ObIOReadHandle::ObIOReadHandle()
  : macro_handle_(), buf_(NULL), offset_(0), size_(0)
{
}

ObTmpFileIOHandle::ObIOReadHandle::ObIOReadHandle(const ObMacroBlockHandle &macro_handle,
    char *buf, const int64_t offset, const int64_t size)
  : macro_handle_(macro_handle), buf_(buf), offset_(offset), size_(size)
{
}

ObTmpFileIOHandle::ObIOReadHandle::~ObIOReadHandle()
{
}

ObTmpFileIOHandle::ObIOReadHandle::ObIOReadHandle(const ObTmpFileIOHandle::ObIOReadHandle &other)
  : macro_handle_(), buf_(NULL), offset_(0), size_(0)
{
  *this = other;
}

ObTmpFileIOHandle::ObIOReadHandle &ObTmpFileIOHandle::ObIOReadHandle::operator=(
    const ObTmpFileIOHandle::ObIOReadHandle &other)
{
  if (&other != this) {
    macro_handle_ = other.macro_handle_;
    offset_ = other.offset_;
    buf_ = other.buf_;
    size_ = other.size_;
  }
  return *this;
}
ObTmpFileIOHandle::ObPageCacheHandle::ObPageCacheHandle()
  : page_handle_(), buf_(NULL), offset_(0), size_(0)
{
}

ObTmpFileIOHandle::ObPageCacheHandle::ObPageCacheHandle(const ObTmpPageValueHandle &page_handle,
    char *buf, const int64_t offset, const int64_t size)
  : page_handle_(page_handle), buf_(buf), offset_(offset), size_(size)
{
}

ObTmpFileIOHandle::ObPageCacheHandle::~ObPageCacheHandle()
{
}

ObTmpFileIOHandle::ObPageCacheHandle::ObPageCacheHandle(
    const ObTmpFileIOHandle::ObPageCacheHandle &other)
  : page_handle_(), buf_(NULL), offset_(0), size_(0)
{
  *this = other;
}

ObTmpFileIOHandle::ObPageCacheHandle &ObTmpFileIOHandle::ObPageCacheHandle::operator=(
    const ObTmpFileIOHandle::ObPageCacheHandle &other)
{
  if (&other != this) {
    page_handle_ = other.page_handle_;
    offset_ = other.offset_;
    buf_ = other.buf_;
    size_ = other.size_;
  }
  return *this;
}

ObTmpFileIOHandle::ObBlockCacheHandle::ObBlockCacheHandle()
  : block_handle_(), buf_(NULL), offset_(0), size_(0)
{
}

ObTmpFileIOHandle::ObBlockCacheHandle::ObBlockCacheHandle(const ObTmpBlockValueHandle &block_handle,
    char *buf, const int64_t offset, const int64_t size)
  : block_handle_(block_handle), buf_(buf), offset_(offset), size_(size)
{
}

ObTmpFileIOHandle::ObBlockCacheHandle::~ObBlockCacheHandle()
{
}

ObTmpFileIOHandle::ObBlockCacheHandle::ObBlockCacheHandle(
    const ObTmpFileIOHandle::ObBlockCacheHandle &other)
  : block_handle_(), buf_(NULL), offset_(0), size_(0)
{
  *this = other;
}

ObTmpFileIOHandle::ObBlockCacheHandle &ObTmpFileIOHandle::ObBlockCacheHandle::operator=(
    const ObTmpFileIOHandle::ObBlockCacheHandle &other)
{
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

void ObTmpFileExtent::get_global_offset(int64_t &g_offset_start, int64_t &g_offset_end) const
{
  g_offset_start = g_offset_start_;
  g_offset_end = g_offset_end_;
}

ObTmpFileExtent::ObTmpFileExtent(ObTmpFile *file)
  : is_alloced_(false),
    is_closed_(false),
    start_page_id_(-1),
    page_nums_(0),
    offset_(0),
    fd_(file->get_fd()),
    g_offset_start_(0),
    g_offset_end_(0),
    owner_(file),
    block_id_(-1),
    lock_(common::ObLatchIds::TMP_FILE_EXTENT_LOCK),
    is_truncated_(false)
{
}

ObTmpFileExtent::~ObTmpFileExtent()
{
}

int ObTmpFileExtent::read(const ObTmpFileIOInfo &io_info, const int64_t offset, const int64_t size,
    char *buf, ObTmpFileIOHandle &handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_alloced_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "ObTmpFileExtent has not been allocated", K(ret));
  } else if (OB_UNLIKELY(offset < 0 || offset >= get_offset() || size <= 0
      || offset + size > get_offset()) || OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(offset), K(get_offset()), K(size), K(buf));
  } else {
    ObTmpBlockIOInfo info;
    info.buf_ =  buf;
    info.io_desc_ = io_info.io_desc_;
    info.block_id_ = block_id_;
    info.offset_ = start_page_id_ * ObTmpMacroBlock::get_default_page_size() + offset;
    info.size_ = size;
    info.tenant_id_ = io_info.tenant_id_;
    info.io_timeout_ms_ = io_info.io_timeout_ms_;
    if (OB_FAIL(OB_TMP_FILE_STORE.read(owner_->get_tenant_id(), info, handle))) {
      STORAGE_LOG(WARN, "fail to read the extent", K(ret), K(info), K(*this));
    } else {
      STORAGE_LOG(DEBUG, "debug tmp file: read extent", K(ret), K(info), K(*this));
    }
  }
  return ret;
}

int ObTmpFileExtent::write(const ObTmpFileIOInfo &io_info,int64_t &size, char *&buf)
{
  int ret = OB_SUCCESS;
  int write_size = 0;
  int64_t remain = 0;
  bool is_write = false;
  bool need_close = false;
  if (OB_UNLIKELY(size <= 0) || OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret));
  } else if (OB_UNLIKELY(!is_alloced_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "ObTmpFileExtent has not been allocated", K(ret));
  } else if (get_offset() == page_nums_ * ObTmpMacroBlock::get_default_page_size()) {
    need_close = true;
  } else {
    SpinWLockGuard guard(lock_);
    if (!is_closed()) {
      remain = page_nums_ * ObTmpMacroBlock::get_default_page_size() - get_offset();
      write_size = std::min(remain, size);
      ObTmpBlockIOInfo info;
      info.block_id_ = block_id_;
      info.buf_ =  buf;
      info.io_desc_ = io_info.io_desc_;
      info.offset_ = start_page_id_ * ObTmpMacroBlock::get_default_page_size() + get_offset();
      info.size_ = write_size;
      info.tenant_id_ = io_info.tenant_id_;
      info.io_timeout_ms_ = io_info.io_timeout_ms_;
      if (OB_FAIL(OB_TMP_FILE_STORE.write(owner_->get_tenant_id(), info))) {
        STORAGE_LOG(WARN, "fail to write the extent", K(ret));
      } else {
        ATOMIC_FAA(&offset_, write_size);
        g_offset_end_ = get_offset() + g_offset_start_;
        buf += write_size;
        size -= write_size;
        if (remain == write_size) {
          need_close = true;
        }
        STORAGE_LOG(DEBUG, "debug tmp file: write extent", K(ret), K(info), K(*this));
      }
    }
  }
  if (need_close) {
    close();
    try_sync_block();
  }
  return ret;
}

void ObTmpFileExtent::reset()
{
  is_alloced_ = false;
  fd_ = -1;
  g_offset_start_ = 0;
  g_offset_end_ = 0;
  ATOMIC_SET(&offset_, 0);
  owner_ = NULL;
  start_page_id_ = -1;
  page_nums_ = 0;
  block_id_ = -1;
  ATOMIC_STORE(&is_closed_, false);
  ATOMIC_STORE(&is_truncated_, false);
}

bool ObTmpFileExtent::is_valid()
{
  return start_page_id_ >= 0 && page_nums_ >= 0 && block_id_ > 0;
}

bool ObTmpFileExtent::close(bool force)
{
  int ret = OB_SUCCESS;
  uint8_t page_start_id = ObTmpFilePageBuddy::MAX_PAGE_NUMS;
  uint8_t page_nums = 0;
  if (!is_closed_) {
    if (close(page_start_id, page_nums, force)) {
      if (ObTmpFilePageBuddy::MAX_PAGE_NUMS == page_start_id && 0 == page_nums) {
        //nothing to do
      } else if (OB_UNLIKELY(page_start_id > ObTmpFilePageBuddy::MAX_PAGE_NUMS - 1
                 || page_nums > ObTmpFilePageBuddy::MAX_PAGE_NUMS)) {
        ret = OB_INVALID_ARGUMENT;
        STORAGE_LOG(WARN, "fail to close the extent", K(ret), K_(block_id), K(page_start_id),
          K(page_nums), K_(offset));
      } else if (OB_FAIL(OB_TMP_FILE_STORE.free(owner_->get_tenant_id(), block_id_, page_start_id,
          page_nums))) {
        STORAGE_LOG(WARN, "fail to free", K(ret));
      }
      if (OB_FAIL(ret)) {
        unclose(page_nums);
      }
    }
  }
  return is_closed();
}

bool ObTmpFileExtent::close(uint8_t &free_page_start_id, uint8_t &free_page_nums, bool force)
{
  free_page_start_id = ObTmpFilePageBuddy::MAX_PAGE_NUMS;
  free_page_nums = 0;
  SpinWLockGuard guard(lock_);
  if (!is_closed()) {
    if (!force && 0 != page_nums_ && 0 == get_offset()) {
      // Nothing to do. This extent is alloced just now, so it cannot be closed.
    } else {
      if (get_offset() != page_nums_ * ObTmpMacroBlock::get_default_page_size()) {
        uint8_t offset_page_id = common::upper_align(get_offset(), ObTmpMacroBlock::get_default_page_size())
                             / ObTmpMacroBlock::get_default_page_size();
        free_page_nums = page_nums_ - offset_page_id;
        free_page_start_id = start_page_id_ + offset_page_id;
        page_nums_ -= free_page_nums;
      }
      ATOMIC_STORE(&is_closed_, true);
    }
  }
  return is_closed();
}

int ObTmpFileExtent::try_sync_block()
{
  int ret = OB_SUCCESS;
  ObTmpTenantMemBlockManager::ObIOWaitInfoHandle handle;
  ObTmpMacroBlock *blk = NULL;
  if (OB_FAIL(OB_TMP_FILE_STORE.get_macro_block(owner_->get_tenant_id(), block_id_, blk))) {
    STORAGE_LOG(WARN, "fail to get macro block", K(ret), K(owner_->get_tenant_id()),K(block_id_));
  } else if (OB_ISNULL(blk)) {
    ret = OB_ERR_NULL_VALUE;
  } else if (0 != blk->get_free_page_nums()) {
    STORAGE_LOG(DEBUG, "ob tmp macro block has not been used up", K(ret), K(blk->get_free_page_nums()), K(owner_->get_tenant_id()),K(block_id_));
  } else if (OB_FAIL(OB_TMP_FILE_STORE.wash_block(owner_->get_tenant_id(), block_id_, handle))) {
    // try to flush the block to the disk. If fails, do nothing.
    STORAGE_LOG(DEBUG, "fail to sync block", K(ret), K(owner_->get_tenant_id()), K(block_id_));
  } else {
    STORAGE_LOG(DEBUG, "succeed to sync wash block", K(block_id_));
  }

  return ret;
}

void ObTmpFileExtent::unclose(const int32_t page_nums)
{
  SpinWLockGuard guard(lock_);
  if (page_nums >= 0) {
    page_nums_ += page_nums;
  }
  ATOMIC_STORE(&is_closed_, false);
}

ObTmpFileMeta::~ObTmpFileMeta()
{
  clear();
}

int ObTmpFileMeta::init(const int64_t fd, const int64_t dir_id, common::ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(fd < 0 || dir_id < 0) || OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(fd), K(dir_id));
  } else {
    fd_ = fd;
    dir_id_ = dir_id;
    allocator_ = allocator;
  }
  return ret;
}

ObTmpFileExtent *ObTmpFileMeta::get_last_extent()
{
  return extents_.count() > 0 ? extents_.at(extents_.count() - 1) : NULL;
}


int ObTmpFileMeta::deep_copy(const ObTmpFileMeta &other)
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
  //free extents
  int ret = OB_SUCCESS;
  ObTmpFileExtent *tmp = NULL;
  for (int64_t i = extents_.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
    tmp = extents_.at(i);
    if (NULL != tmp) {
      if (!tmp->is_alloced() || tmp->is_truncated()) {
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
  : is_inited_(false),
    is_big_(false),
    offset_(0),
    tenant_id_(-1),
    lock_(common::ObLatchIds::TMP_FILE_LOCK),
    allocator_(NULL),
    file_meta_(),
    read_guard_(0),
    next_truncated_extent_id_(0)
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
      allocator_ = NULL;
      is_inited_ = false;
      read_guard_ = 0;
      next_truncated_extent_id_ = 0;
    }
  }
  return ret;
}

int ObTmpFile::init(const int64_t fd, const int64_t dir_id, common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
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

int ObTmpFile::aio_read_without_lock(const ObTmpFileIOInfo &io_info,
    int64_t &offset, ObTmpFileIOHandle &handle)
{
  int ret = OB_SUCCESS;
  ObTmpFileExtent *tmp = nullptr;
  if (OB_ISNULL(tmp = file_meta_.get_last_extent())) {
    ret = OB_BAD_NULL_ERROR;
    STORAGE_LOG(WARN, "fail to read, because the tmp file is empty", K(ret), KP(tmp), K(io_info));
  } else if (OB_FAIL(handle.prepare_read(io_info.size_,
                                         offset,
                                         io_info.io_desc_,
                                         io_info.buf_,
                                         file_meta_.get_fd(),
                                         file_meta_.get_dir_id(),
                                         io_info.tenant_id_,
                                         io_info.disable_page_cache_))) {
    STORAGE_LOG(WARN, "fail to prepare read io handle", K(ret), K(io_info), K(offset));
  } else if (OB_UNLIKELY(io_info.size_ > 0 && offset >= tmp->get_global_end())) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(once_aio_read_batch_without_lock(io_info, offset, handle))) {
    STORAGE_LOG(WARN, "fail to read one batch", K(ret), K(offset), K(handle));
  } else {
    handle.set_last_read_offset(offset);
  }
  return ret;
}

int ObTmpFile::once_aio_read_batch(
    const ObTmpFileIOInfo &io_info,
    const bool need_update_offset,
    int64_t &offset,
    ObTmpFileIOHandle &handle)
{
  int ret = OB_SUCCESS;
  ObTmpFileExtent *tmp = nullptr;
  const int64_t remain_size = io_info.size_ - handle.get_data_size();

  {
    SpinRLockGuard guard(lock_);
    if (IS_NOT_INIT) {
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
  }

  if (need_update_offset) {
    SpinWLockGuard guard(lock_);
    offset_ = offset;
  }
  return ret;
}

int ObTmpFile::fill_zero(char *buf, const int64_t size)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || size < 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "buf is null or size is negative", K(ret), K(size), KP(buf));
  } else {
    MEMSET(buf, 0, size);
  }
  return ret;
}

int ObTmpFile::once_aio_read_batch_without_lock(
    const ObTmpFileIOInfo &io_info,
    int64_t &offset,
    ObTmpFileIOHandle &handle)
{
  int ret = OB_SUCCESS;
  int64_t one_batch_read_size = 0;
  char *buf = io_info.buf_ + handle.get_data_size();
  int64_t remain_size = io_info.size_ - handle.get_data_size();
  int64_t read_size = 0;
  ObTmpFileExtent *tmp = nullptr;
  common::ObIArray<ObTmpFileExtent *> &extents = file_meta_.get_extents();
  int64_t ith_extent = get_extent_cache(offset, handle);

  while (OB_SUCC(ret)
      && ith_extent < extents.count()
      && remain_size > 0
      && one_batch_read_size < READ_SIZE_PER_BATCH) {
    tmp = extents.at(ith_extent);
    if (tmp->get_global_start() <= offset && offset < tmp->get_global_end()) {
      if (offset + remain_size > tmp->get_global_end()) {
        read_size = tmp->get_global_end() - offset;
      } else {
        read_size = remain_size;
      }
      // read from the extent.
      if (tmp->is_truncated()) {
        if (read_guard_ < tmp->get_global_end()) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "extent is truncated but read_guard not set correctlly", K(ret), K(tmp), K(read_guard_));
        } else if (OB_FAIL(fill_zero(buf, read_size))) {
          STORAGE_LOG(WARN, "fail to fill zero data to buf", K(ret));
        }
      } else if (offset >= read_guard_) {
        if (OB_FAIL(tmp->read(io_info, offset - tmp->get_global_start(), read_size, buf, handle))) {
          STORAGE_LOG(WARN, "fail to read the extent", K(ret), K(io_info), K(buf), KP_(io_info.buf));
        }
      } else {
        if (read_guard_ < offset + read_size) {
          const int64_t zero_size = read_guard_ - offset;
          const int64_t file_read_size = read_size - zero_size;
          if (OB_FAIL(fill_zero(buf, zero_size))) {
            STORAGE_LOG(WARN, "fail to read zero from truncated pos", K(ret));
          } else if (OB_FAIL(tmp->read(io_info, read_guard_ - tmp->get_global_start(), file_read_size, buf + zero_size, handle))) {
            STORAGE_LOG(WARN, "fail to read the extent", K(ret), K(io_info), K(buf + zero_size), KP_(io_info.buf));
          }
        } else {
          // read 0;
          if (OB_FAIL(fill_zero(buf, read_size))) {
            STORAGE_LOG(WARN, "fail to read zero from truncated pos", K(ret), KP(buf), K(read_size));
          }
        }

      }
      if (OB_SUCC(ret)) {
        offset += read_size;
        remain_size -= read_size;
        buf += read_size;
        one_batch_read_size += read_size;
        handle.add_data_size(read_size);
      }
    }
    ++ith_extent;
  }

  if (OB_SUCC(ret) && OB_LIKELY(ith_extent > 0)) {
    handle.set_last_extent_id(ith_extent - 1);
  }
  return ret;
}

int64_t ObTmpFile::get_extent_cache(const int64_t offset, const ObTmpFileIOHandle &handle)
{
  common::ObIArray<ObTmpFileExtent *> &extents = file_meta_.get_extents();
  int64_t ith_extent = -1;
  int64_t last_extent_id = handle.get_last_extent_id();
  if (OB_UNLIKELY(last_extent_id < 0 || last_extent_id >= extents.count() - 1)) {
    ith_extent = find_first_extent(offset);
  } else if (OB_LIKELY(extents.at(last_extent_id)->get_global_start() <= offset
                       && offset < extents.at(last_extent_id)->get_global_end())) {
    ith_extent = last_extent_id;
  } else if (offset == extents.at(last_extent_id)->get_global_end()
             && last_extent_id != extents.count() - 1) {
    ith_extent = last_extent_id + 1;
  } else {
    ith_extent = find_first_extent(offset);
  }

  return ith_extent;
}

int ObTmpFile::aio_read(const ObTmpFileIOInfo &io_info, ObTmpFileIOHandle &handle)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
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
int ObTmpFile::aio_pread(const ObTmpFileIOInfo &io_info, const int64_t offset,
    ObTmpFileIOHandle &handle)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
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
  if (IS_NOT_INIT) {
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

int ObTmpFile::read(const ObTmpFileIOInfo &io_info, ObTmpFileIOHandle &handle)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(aio_read(io_info, handle))) {
    if (OB_ITER_END != ret) {
      STORAGE_LOG(WARN, "fail to read data using asynchronous io", K(ret), K(io_info));
    } else {
      if (OB_FAIL(handle.wait())) {
        STORAGE_LOG(WARN, "fail to wait io finish", K(ret), K(io_info));
      } else {
        ret = OB_ITER_END;
      }
    }
  } else if (OB_FAIL(handle.wait())) {
    if (OB_ITER_END != ret) {
      STORAGE_LOG(WARN, "fail to wait io finish", K(ret), K(io_info));
    }
  }
  return ret;
}

int ObTmpFile::pread(const ObTmpFileIOInfo &io_info, const int64_t offset, ObTmpFileIOHandle &handle)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpFile has not been inited", K(ret));
  } else if (OB_FAIL(aio_pread(io_info, offset, handle))) {
    if (OB_ITER_END != ret) {
      STORAGE_LOG(WARN, "fail to read data using asynchronous io", K(ret), K(io_info));
    } else {
      if (OB_FAIL(handle.wait())) {
        STORAGE_LOG(WARN, "fail to wait io finish", K(ret), K(io_info));
      } else {
        ret = OB_ITER_END;
      }
    }
  } else if (OB_FAIL(handle.wait())) {
    if (OB_ITER_END != ret) {
      STORAGE_LOG(WARN, "fail to wait io finish", K(ret), K(io_info));
    }
  }
  return ret;
}

int ObTmpFile::aio_write(const ObTmpFileIOInfo &io_info, ObTmpFileIOHandle &handle)
{
  // only support append at present.
  int ret = OB_SUCCESS;
  DISABLE_SQL_MEMLEAK_GUARD;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpFile has not been inited", K(ret));
  } else if (!io_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(io_info), K(handle));
  } else if (OB_FAIL(handle.prepare_write(io_info.buf_,
                                          io_info.size_,
                                          file_meta_.get_fd(),
                                          file_meta_.get_dir_id(),
                                          io_info.tenant_id_))) {
    STORAGE_LOG(WARN, "fail to prepare write io handle", K(ret));
  } else {
    tenant_id_ = io_info.tenant_id_;
    int64_t size = io_info.size_;
    char *buf = io_info.buf_;
    SpinWLockGuard guard(lock_);
    ObTmpFileExtent *tmp = file_meta_.get_last_extent();
    void *buff = NULL;
    ObTmpFileExtent *extent = NULL;
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
        } else if (size > ObTmpFileStore::get_block_size()) {
          alloc_size = ObTmpFileStore::get_block_size();
        } else {
          alloc_size = size;
        }
        lib::ObMemAttr attr(tenant_id_, "TmpFileExtent");
        if (OB_ISNULL(buff = allocator_->alloc(sizeof(ObTmpFileExtent), attr))) {
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
          if (OB_NOT_NULL(tmp)) {
            tmp = NULL;
          }
        }
      } else {
        // this extent has allocated, no one page, invalid extent(tmp->is_alloced = false).
        ret = OB_INVALID_ERROR;
        STORAGE_LOG(WARN, "invalid extent", K(ret), KPC(tmp), KPC(this));
      }
      if (OB_SUCC(ret)) {
        if (OB_ISNULL(tmp)) {
          ret = OB_ERR_NULL_VALUE;
          STORAGE_LOG(ERROR, "invalid tmp", K(ret));
        } else if (OB_FAIL(handle.record_block_id(tmp->get_block_id()))) {
          if (OB_HASH_EXIST == ret) {
            ret = OB_SUCCESS;
          } else {
            STORAGE_LOG(WARN, "set block id failed", K(ret));
          }
        }
      }

      if (OB_EAGAIN == ret) {
        ret = OB_SUCCESS;
      }
    }
    handle.sub_data_size(io_info.size_ - size);
    if (OB_SUCC(ret) && !is_big_){
      is_big_ = tmp->get_global_end() >=
          SMALL_FILE_MAX_THRESHOLD * ObTmpMacroBlock::get_default_page_size();
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
  ObTmpFileExtent *tmp = file_meta_.get_last_extent();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpFile has not been inited", K(ret));
  } else if (OB_ISNULL(tmp)) {
    ret = OB_BAD_NULL_ERROR;
    STORAGE_LOG(WARN, "the file does not have a extent",  K(ret), K(timeout_ms));
  } else {
    tmp->close(true/*force*/);
    // all extents has been closed.
    const ObIArray<ObTmpFileExtent *> &extents = file_meta_.get_extents();
    common::hash::ObHashSet<int64_t> blk_id_set;
    lib::ObMemAttr attr(tenant_id_, "TmpBlkIDSet");
    if (OB_FAIL(blk_id_set.create(min(extents.count(), 1024 * 1024), attr))){
      STORAGE_LOG(WARN, "create block id set failed", K(ret), K(timeout_ms));
    } else {
      // get extents block id set.
      for (int64_t i=0; OB_SUCC(ret) && i < extents.count(); ++i) {
        const ObTmpFileExtent* e = extents.at(i);
        const int64_t &blk_id = e->get_block_id();
        if (OB_FAIL(blk_id_set.set_refactored(blk_id))) {
          STORAGE_LOG(WARN, "add block id to set failed", K(ret), K(blk_id));
        }
      }
      // iter all blocks, execute async wash.
      common::hash::ObHashSet<int64_t>::const_iterator iter;
      common::ObSEArray<ObTmpTenantMemBlockManager::ObIOWaitInfoHandle, 1> handles;
      handles.set_attr(ObMemAttr(MTL_ID(), "TMP_SYNC_HDL"));
      for (iter = blk_id_set.begin(); OB_SUCC(ret) && iter != blk_id_set.end(); ++iter) {
        const int64_t &blk_id = iter->first;
        ObTmpTenantMemBlockManager::ObIOWaitInfoHandle handle;
        if (OB_FAIL(OB_TMP_FILE_STORE.sync_block(tenant_id_, blk_id, handle))) {
          // OB_HASH_NOT_EXIST:
          // if multiple file sync same block, the block may be not exist in hash map.
          // OB_STATE_NOT_MATCH:
          // the extents in block may be not all close and shouldn't sync it now.
          if (OB_HASH_NOT_EXIST == ret || OB_STATE_NOT_MATCH == ret) {
            ret = OB_SUCCESS;
          } else {
            STORAGE_LOG(WARN, "sync block failed", K(ret), K(blk_id));
          }
        } else if (OB_NOT_NULL(handle.get_wait_info()) && OB_FAIL(handles.push_back(handle))) {
          STORAGE_LOG(WARN, "push back wait handle to array failed", K(ret), K(blk_id));
        }
      }

      int64_t begin_us = ObTimeUtility::fast_current_time();
      int64_t wait_ms = timeout_ms;
      for (int64_t i=0; OB_SUCC(ret) && i < handles.count(); ++i) {
        const ObTmpTenantMemBlockManager::ObIOWaitInfoHandle handle = handles.at(i);
        if (OB_FAIL(handle.get_wait_info()->wait(timeout_ms))) {
          STORAGE_LOG(WARN, "add block id to set failed", K(ret), K(timeout_ms));
        } else {
          wait_ms = timeout_ms - (ObTimeUtility::fast_current_time() - begin_us) / 1000;
        }

        if (OB_SUCC(ret) && OB_UNLIKELY(wait_ms <= 0)) { // rarely happen
          ret = OB_TIMEOUT;
          STORAGE_LOG(WARN, "fail to wait tmp file sync finish", K(ret), K(wait_ms));
        }
      }
    }
  }
  return ret;
}

int ObTmpFile::deep_copy(char *buf, const int64_t buf_len, ObTmpFile *&value) const
{
  int ret = OB_SUCCESS;
  const int64_t deep_copy_size = get_deep_copy_size();
  if (OB_ISNULL(buf) || buf_len < deep_copy_size) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(buf), K(buf_len));
  } else {
    ObTmpFile *pvalue = new (buf) ObTmpFile();
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

void ObTmpFile::get_file_size(int64_t &file_size)
{
  ObTmpFileExtent *tmp = file_meta_.get_last_extent();
  file_size = (nullptr == tmp) ? 0 : tmp->get_global_end();
}

/*
 * to avoid truncating blocks that is using now (e.g., the io request is in io manager but not finish).
 * we need to ensure there is no other file operation while calling truncate.
 */
int ObTmpFile::truncate(const int64_t offset)
{
  int ret = OB_SUCCESS;

  SpinWLockGuard guard(lock_);
  // release extents
  ObTmpFileExtent *tmp = nullptr;
  //the extents before read_guard_ is truncated;
  int64_t ith_extent = next_truncated_extent_id_;
  common::ObIArray<ObTmpFileExtent *> &extents = file_meta_.get_extents();
  STORAGE_LOG(INFO, "truncate ", K(offset), K(read_guard_), K(ith_extent));

  if (OB_ISNULL(tmp = file_meta_.get_last_extent())) {
    ret = OB_BAD_NULL_ERROR;
    STORAGE_LOG(WARN, "fail to truncate, because the tmp file is empty", K(ret), KP(tmp));
  } else if (offset < 0 || offset > tmp->get_global_end()) {
    ret = OB_INDEX_OUT_OF_RANGE;
    STORAGE_LOG(WARN, "offset out of range", K(ret), K(tmp), K(offset));
  }

  while (OB_SUCC(ret) && ith_extent >= 0
      && ith_extent < extents.count()) {
    tmp = extents.at(ith_extent);
    if (tmp->get_global_start() >= offset) {
      break;
    } else if (!tmp->is_closed()) {
      // for extent that is not closed, shouldn't truncate.
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "the truncate extent is not closed", K(ret));
    } else if (tmp->get_global_end() > offset) {
      break;
    } else {
      // release space
      if (!tmp->is_truncated()) {
        tmp->set_truncated();
        if (OB_FAIL(OB_TMP_FILE_STORE.free(get_tenant_id(), tmp->get_block_id(),
                                          tmp->get_start_page_id(),
                                          tmp->get_page_nums()))) {
          STORAGE_LOG(WARN, "fail to release space", K(ret), K(read_guard_), K(tmp));
        }
        STORAGE_LOG(TRACE, "release extents", K(ith_extent), K(tmp->get_start_page_id()), K(tmp->get_page_nums()));
      }
      if (OB_SUCC(ret)) {
        // if only part of extent is truncated, we only need to set the read_guard
        ith_extent++;
      }
    }
  }

  if (OB_SUCC(ret) && offset > read_guard_) {
    read_guard_ = offset;
    next_truncated_extent_id_ = ith_extent;
  }
  return ret;
}

int ObTmpFile::write_file_extent(const ObTmpFileIOInfo &io_info, ObTmpFileExtent *file_extent,
    int64_t &size, char *&buf)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(size <= 0)|| OB_ISNULL(file_extent) || OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret));
  } else if (OB_FAIL(file_extent->write(io_info, size, buf))) {
    STORAGE_LOG(WARN, "fail to write the extent", K(ret), K(size), KP(buf));
  }
  return ret;
}

int ObTmpFile::write(const ObTmpFileIOInfo &io_info)
{
  int ret = OB_SUCCESS;
  ObTmpFileIOHandle handle;
  if (OB_FAIL(aio_write(io_info, handle))) {
    STORAGE_LOG(WARN, "fail to write using asynchronous io", K(ret), K(io_info));
  } else if (OB_FAIL(handle.wait())) {
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


ObTmpFileHandle::ObTmpFileHandle()
  : ObResourceHandle<ObTmpFile>()
{
}

ObTmpFileHandle::~ObTmpFileHandle()
{
  reset();
}

void ObTmpFileHandle::reset()
{
  if (NULL != ptr_) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = ObTmpFileManager::get_instance().dec_handle_ref(*this))) {
      STORAGE_LOG_RET(WARN, tmp_ret, "fail to decrease handle reference count", K(tmp_ret));
    } else {
      ptr_ = nullptr;
    }
  }
}

ObTmpFileManager &ObTmpFileManager::get_instance()
{
  static ObTmpFileManager instance;
  return instance;
}

int ObTmpFileManager::init()
{
  int ret = OB_SUCCESS;
  ObMemAttr attr = SET_USE_500(ObMemAttr(OB_SERVER_TENANT_ID, ObModIds::OB_TMP_FILE_MANAGER)); //TODO: split tmp file map into each tenant.
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObTmpFileManager has not been inited", K(ret));
  } else if (OB_FAIL(files_.init(DEFAULT_BUCKET_NUM, attr, *lib::ObMallocAllocator::get_instance()))) {
    STORAGE_LOG(WARN, "fail to init map for temporary files", K(ret));
  } else if (OB_FAIL(OB_TMP_FILE_STORE.init())) {
    STORAGE_LOG(WARN, "fail to init the block manager for temporary files", K(ret));
  } else {
    is_inited_ = true;
  }

  if (!is_inited_) {
    destroy();
  }
  return ret;
}

int ObTmpFileManager::alloc_dir(int64_t &dir)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpFileManager has not been inited", K(ret));
  } else if (OB_FAIL(get_next_dir(dir))) {
    STORAGE_LOG(WARN, "The directory of ObTmpFileManager has been used up", K(ret));
  }
  return ret;
}

int ObTmpFileManager::get_next_dir(int64_t &next_dir)
{
  int ret = OB_SUCCESS;
  next_value(next_dir_, next_dir);
  if (INT64_MAX - 1 == next_dir_) {
    ret = OB_SIZE_OVERFLOW;
  }
  return ret;
}

void ObTmpFileManager::next_value(int64_t &current_val, int64_t &next_val)
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

int ObTmpFileManager::open(int64_t &fd, int64_t &dir)
{
  int ret = OB_SUCCESS;
  ObTmpFile file;
  common::ObIAllocator *allocator = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpFileManager has not been inited", K(ret));
  } else if (OB_FAIL(OB_TMP_FILE_STORE.get_tenant_extent_allocator(MTL_ID(), allocator))) {
    STORAGE_LOG(WARN, "fail to get extent allocator", K(ret));
  } else if (OB_FAIL(get_next_fd(fd))) {
    STORAGE_LOG(WARN, "fail to get next fd", K(ret));
  } else if (OB_FAIL(file.init(fd, dir, *allocator))) {
    STORAGE_LOG(WARN, "fail to open file", K(ret));
  } else if (OB_FAIL(files_.set(fd, file))) {
    STORAGE_LOG(WARN, "fail to set tmp file", K(ret));
  } else {
    ObTaskController::get().allow_next_syslog();
    STORAGE_LOG(INFO, "succeed to open a tmp file", K(fd), K(dir), K(common::lbt()));
  }
  return ret;
}

int ObTmpFileManager::get_next_fd(int64_t &next_fd)
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
    STORAGE_LOG_RET(WARN, OB_ERR_TOO_MUCH_TIME, "too much file", K(ret));
  }
  return ret;
}

int ObTmpFileManager::aio_read(const ObTmpFileIOInfo &io_info, ObTmpFileIOHandle &handle)
{
  int ret = OB_SUCCESS;
  ObTmpFileHandle file_handle;
  handle.reset();
  if (IS_NOT_INIT) {
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

int ObTmpFileManager::aio_pread(const ObTmpFileIOInfo &io_info, const int64_t offset,
    ObTmpFileIOHandle &handle)
{
  int ret = OB_SUCCESS;
  ObTmpFileHandle file_handle;
  handle.reset();
  if (IS_NOT_INIT) {
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

int ObTmpFileManager::read(const ObTmpFileIOInfo &io_info, ObTmpFileIOHandle &handle)
{
  int ret = OB_SUCCESS;
  ObTmpFileHandle file_handle;
  handle.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpFileManager has not been inited", K(ret));
  } else if (!io_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(io_info));
  } else if (OB_FAIL(files_.get(io_info.fd_, file_handle))) {
    STORAGE_LOG(WARN, "fail to get temporary file handle", K(ret), K(io_info));
  } else if (OB_FAIL(file_handle.get_resource_ptr()->read(io_info, handle))) {
    if (OB_ITER_END != ret) {
      STORAGE_LOG(WARN, "fail to read", K(ret), K(io_info));
    }
  }
  return ret;
}

int ObTmpFileManager::pread(const ObTmpFileIOInfo &io_info, const int64_t offset, ObTmpFileIOHandle &handle)
{
  int ret = OB_SUCCESS;
  ObTmpFileHandle file_handle;
  handle.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpFileManager has not been inited", K(ret));
  } else if (!io_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(io_info));
  } else if (OB_FAIL(files_.get(io_info.fd_, file_handle))) {
    STORAGE_LOG(WARN, "fail to get tmp file handle", K(ret), K(io_info));
  } else if (OB_FAIL(file_handle.get_resource_ptr()->pread(io_info, offset, handle))) {
    if (OB_ITER_END != ret) {
      STORAGE_LOG(WARN, "fail to pread", K(ret), K(io_info));
    }
  }
  return ret;
}

int ObTmpFileManager::aio_write(const ObTmpFileIOInfo &io_info, ObTmpFileIOHandle &handle)
{
  int ret = OB_SUCCESS;
  ObTmpFileHandle file_handle;
  handle.reset();
  if (IS_NOT_INIT) {
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

int ObTmpFileManager::write(const ObTmpFileIOInfo &io_info)
{
  int ret = OB_SUCCESS;
  ObTmpFileHandle file_handle;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpFileManager has not been inited", K(ret));
  } else if (!io_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(io_info));
  } else if (OB_FAIL(files_.get(io_info.fd_, file_handle))) {
    STORAGE_LOG(WARN, "fail to get temporary file handle", K(ret), K(io_info));
  } else if (OB_FAIL(file_handle.get_resource_ptr()->write(io_info))) {
    STORAGE_LOG(WARN, "fail to write", K(ret), K(io_info));
  }
  return ret;
}

int ObTmpFileManager::seek(const int64_t fd, const int64_t offset, const int whence)
{
  int ret = OB_SUCCESS;
  ObTmpFileHandle file_handle;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpFileManager has not been inited", K(ret));
  } else if (OB_FAIL(files_.get(fd, file_handle))) {
    STORAGE_LOG(WARN, "fail to get tmp file handle", K(ret), K(fd));
  } else if (OB_FAIL(file_handle.get_resource_ptr()->seek(offset, whence))) {
    STORAGE_LOG(WARN, "fail to seek file", K(ret));
  }
  return ret;
}

int ObTmpFileManager::truncate(const int64_t fd, const int64_t offset)
{
  int ret = OB_SUCCESS;
  ObTmpFileHandle file_handle;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpFileManager has not been inited", K(ret));
  } else if (OB_FAIL(files_.get(fd, file_handle))) {
    STORAGE_LOG(WARN, "fail to get tmp file handle", K(ret), K(fd));
  } else if (OB_FAIL(file_handle.get_resource_ptr()->truncate(offset))) {
    STORAGE_LOG(WARN, "fail to seek file", K(ret));
  }
  return ret;
}

int ObTmpFileManager::get_tmp_file_handle(const int64_t fd, ObTmpFileHandle &handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpFileManager has not been inited", K(ret));
  } else if (OB_FAIL(files_.get(fd, handle))) {
    STORAGE_LOG(WARN, "fail to get tmp file handle", K(ret), K(fd));
  }
  return ret;
}

int ObTmpFileManager::remove(const int64_t fd)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpFileManager has not been inited", K(ret));
  } else{
    if (OB_FAIL(files_.erase(fd))) {
      if (common::OB_ENTRY_NOT_EXIST != ret) {
        STORAGE_LOG(WARN, "fail to get tmp file handle", K(ret), K(fd));
      } else {
        ret = OB_SUCCESS;
        STORAGE_LOG(INFO, "this tmp file has been removed", K(fd), K(common::lbt()));
      }
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
  fd_list.set_attr(ObMemAttr(MTL_ID(), "TMP_FD_LIST"));
  RmTenantTmpFileOp rm_tenant_file_op(tenant_id, &fd_list);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpFileManager has not been inited", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(files_.foreach(rm_tenant_file_op))) {
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
  if (IS_NOT_INIT) {
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
  if (IS_NOT_INIT) {
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
  : is_inited_(false),
    next_fd_(-1),
    next_dir_(-1),
    rm_file_lock_(common::ObLatchIds::TMP_FILE_MGR_LOCK),
    files_()
{
}

ObTmpFileManager::~ObTmpFileManager()
{
  destroy();
}

void ObTmpFileManager::destroy()
{
  files_.destroy();
  OB_TMP_FILE_STORE.destroy();
  next_fd_ = -1;
  next_dir_ = -1;
  is_inited_ = false;
}

int ObTmpFileManager::dec_handle_ref(ObTmpFileHandle &handle)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpFileManager has not been inited", K(ret));
  } else if (OB_FAIL(files_.dec_handle_ref(handle.ptr_))) {
    STORAGE_LOG(WARN, "fail to dec handle ref without lock", K(ret));
  }
  return ret;
}

int ObTmpFileManager::get_tmp_file_size(const int64_t fd, int64_t &file_size)
{
  int ret = OB_SUCCESS;
  ObTmpFileHandle file_handle;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpFileManager has not been inited", K(ret));
  } else if (fd < 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(fd));
  } else if (OB_FAIL(files_.get(fd, file_handle))) {
    STORAGE_LOG(WARN, "fail to get tmp file handle", K(ret), K(fd));
  } else {
    file_handle.get_resource_ptr()->get_file_size(file_size);
  }
  return ret;
}

} // end namespace blocksstable
} // end namespace oceanbase
