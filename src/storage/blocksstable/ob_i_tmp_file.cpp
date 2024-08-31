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

#include "storage/blocksstable/ob_i_tmp_file.h"
#include "storage/tmp_file/ob_tmp_file_manager.h"
#include "observer/ob_server_struct.h"

#ifdef OB_BUILD_SHARED_STORAGE
#include "storage/blocksstable/ob_shared_storage_tmp_file.h"
#endif

namespace oceanbase
{
using namespace storage;
using namespace share;

namespace blocksstable
{

/* -------------------------- ObTmpFileAsyncFlushWaitTask --------------------------- */

ObTmpFileAsyncFlushWaitTask::ObTmpFileAsyncFlushWaitTask()
    : fd_(-1),
      current_length_(0),
      current_begin_page_id_(ObSSTmpWriteBufferPool::INVALID_PAGE_ID),
      flushed_offset_(0),
      io_tasks_(),
      flushed_page_nums_(0),
      succeed_wait_page_nums_(0),
      cond_(),
      ref_cnt_(0),
      wait_has_finished_(false),
      is_inited_(false)
{
}

ObTmpFileAsyncFlushWaitTask::~ObTmpFileAsyncFlushWaitTask()
{
  release_io_tasks();
}

int ObTmpFileAsyncFlushWaitTask::init(const int64_t fd, const int64_t length, const uint32_t begin_page_id, const ObMemAttr attr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("fail to init tmp file async wait task, init twice", K(ret), K(is_inited_));
  } else if (FALSE_IT(fd_ = fd)) {
  } else if (FALSE_IT(current_length_ = length)) {
  } else if (FALSE_IT(current_begin_page_id_ = begin_page_id)) {
  } else if (FALSE_IT(io_tasks_.set_attr(attr))) {
  } else if (OB_FAIL(cond_.init(ObWaitEventIds::IO_CONTROLLER_COND_WAIT))) {
    LOG_WARN("fail to init conditional variable in tmp file async wait task", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObTmpFileAsyncFlushWaitTask::push_back_io_task(const ObSSTmpFileFlushContext &ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(io_tasks_.push_back(std::make_pair(ctx.object_handle_, ctx.flush_buff_)))) {
    LOG_WARN("fail to push back flush handle and flush buffer", K(ret), K(fd_), K(ctx));
  } else {
    // TODO(baichangmin): add defensive check here, cur_flush_page_count and
    // this_batch_flush_data_size should equal.
    flushed_page_nums_ = ctx.cur_flush_page_count_;
    flushed_offset_ = ctx.write_info_offset_;
  }
  return ret;
}
#endif

int ObTmpFileAsyncFlushWaitTask::release_io_tasks()
{
  int ret = OB_SUCCESS;
  ObIAllocator *allocator = MTL(tmp_file::ObTenantTmpFileManager *)->get_ss_file_manager().get_wait_task_allocator();
  for (int i = 0; OB_SUCC(ret) && i < io_tasks_.count(); ++i) {
    std::pair<ObStorageObjectHandle *, char *> &io_pair = io_tasks_.at(i);
    io_pair.first->~ObStorageObjectHandle();
    allocator->free(io_pair.first);
    allocator->free(io_pair.second);
  }
  io_tasks_.reset();
  flushed_page_nums_ = 0;
  return ret;
}

int ObTmpFileAsyncFlushWaitTask::exec_wait()
{
  int ret = OB_SUCCESS;
  ObSSTenantTmpFileManager *mgr = &MTL(tmp_file::ObTenantTmpFileManager *)->get_ss_file_manager();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("fail to exec wait, tmp file async wait task not inited", K(ret), K(*this));
  } else {
    // Execute `wait` for each async flush handle.
    for (int64_t i = 0; OB_SUCC(ret) && i < io_tasks_.count(); ++i) {
      std::pair<ObStorageObjectHandle *, char *> &p = io_tasks_.at(i);
      if (OB_FAIL(p.first->wait())) {
        LOG_WARN("fail to wait ObStorageObjectHandle", K(ret), K(i),
                 K(io_tasks_.count()), K(*this));
      } else {
#ifdef OB_BUILD_SHARED_STORAGE
        succeed_wait_page_nums_ +=
            p.first->get_io_handle().get_data_size() /
            ObSharedStorageTmpFile::SS_TMP_FILE_PAGE_SIZE;
#endif
      }
    }
    int tmp_ret = OB_SUCCESS;
    if (OB_UNLIKELY(succeed_wait_page_nums_ > flushed_page_nums_)) {
      tmp_ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected flush page nums", K(ret), K(tmp_ret), K(*this));
    }
    // Finish async wait for this task, execute `update_meta_data` and broadcast.
    if (OB_TMP_FAIL(mgr->update_meta_data(*this))) {
      LOG_ERROR("fail to update meta data", K(ret), K(tmp_ret),
                K(io_tasks_.count()), K(*this));
    }
    // Whether ret is success or not, the condition variable should be awakened
    // to prevent other write thread hanging.
    if (OB_TMP_FAIL(cond_broadcast())) {
      LOG_ERROR("fail to cond broadcast", K(ret), K(tmp_ret), K(*this));
    }
  }
  FLOG_INFO("async flush wait succeed", K(ret), K(io_tasks_.count()), K(*this));
  return ret;
}

int ObTmpFileAsyncFlushWaitTask::cond_wait()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("fail to cond wait, tmp file async wait task not inited",
             K(ret), KP(this), K(fd_), K(is_inited_));
  } else {
    int64_t wait_cond_loops = 0;
    ObThreadCondGuard guard(cond_);
    if (OB_FAIL(guard.get_ret())) {
      LOG_ERROR("fail to cond wait, lock failed", K(ret), KP(this), K(fd_));
    }
    while (OB_SUCC(ret) && !ATOMIC_LOAD(&wait_has_finished_)) {
      if (OB_FAIL(cond_.wait(30)) && ret != OB_TIMEOUT) {
        LOG_WARN("fail to cond wait", K(ret), KP(this), K(fd_));
      } else if (ret == OB_TIMEOUT) {
        ret = OB_SUCCESS;
        LOG_INFO("cond wait timeout once", KP(this), K(fd_),
                 K(ATOMIC_LOAD(&wait_has_finished_)), K(wait_cond_loops));
      } else {
        break;
      }
      wait_cond_loops++;
    }
    LOG_INFO("shared storage temporary file finish cond wait", K(ret), KP(this), K(fd_),
             K(ATOMIC_LOAD(&wait_has_finished_)), K(wait_cond_loops));
  }
  return ret;
}

int ObTmpFileAsyncFlushWaitTask::cond_broadcast()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("fail to cond boradcast, tmp file async wait task not inited", K(ret), K(is_inited_));
  } else {
    ObThreadCondGuard guard(cond_);
    if (OB_FAIL(guard.get_ret())) {
      LOG_ERROR("fail to cond boradcast, lock failed", K(ret), KP(this));
    } else if (OB_FAIL(cond_.broadcast())) {
      LOG_WARN("fail to cond broadcast", K(ret), KP(this));
    } else {
      ATOMIC_SET(&wait_has_finished_, true);
      LOG_INFO("cmdebug, cond broadcast succeed", KP(this), K(fd_));
    }
  }
  return ret;
}

/* -------------------------- ObTmpFileAsyncFlushWaitTaskHandle --------------------------- */

ObTmpFileAsyncFlushWaitTaskHandle::ObTmpFileAsyncFlushWaitTaskHandle()
    : wait_task_(nullptr)
{
}

ObTmpFileAsyncFlushWaitTaskHandle::ObTmpFileAsyncFlushWaitTaskHandle(
    ObTmpFileAsyncFlushWaitTask *wait_task)
    : wait_task_(nullptr)
{
  set_wait_task(wait_task);
}

ObTmpFileAsyncFlushWaitTaskHandle::~ObTmpFileAsyncFlushWaitTaskHandle()
{
  reset();
}

void ObTmpFileAsyncFlushWaitTaskHandle::set_wait_task(ObTmpFileAsyncFlushWaitTask *wait_task)
{
  if (OB_NOT_NULL(wait_task_)) {
    reset();
  }
  if (OB_NOT_NULL(wait_task)) {
    wait_task->inc_ref_cnt();
  }
  wait_task_ = wait_task;
}

void ObTmpFileAsyncFlushWaitTaskHandle::reset()
{
  if (OB_NOT_NULL(wait_task_)) {
    int32_t new_ref_cnt = -1;
    wait_task_->dec_ref_cnt(&new_ref_cnt);
    if (new_ref_cnt == 0) {
      wait_task_->~ObTmpFileAsyncFlushWaitTask();
      MTL(tmp_file::ObTenantTmpFileManager *)->get_ss_file_manager().get_wait_task_allocator()->free(wait_task_);
    }
    wait_task_ = nullptr;
  }
}

/* -------------------------- ObSSTmpFileIOHandle --------------------------- */

ObSSTmpFileIOHandle::ObSSTmpFileIOHandle()
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
    expect_write_size_(0),
    last_read_offset_(-1),
    io_flag_(),
    update_offset_in_file_(false),
    last_fd_(OB_INVALID_FD),
    last_extent_id_(0),
    wait_task_handle_()
{
  io_handles_.set_attr(ObMemAttr(MTL_ID(), "TMP_IO_HDL"));
  page_cache_handles_.set_attr(ObMemAttr(MTL_ID(), "TMP_PCACHE_HDL"));
  block_cache_handles_.set_attr(ObMemAttr(MTL_ID(), "TMP_BCACHE_HDL"));
}

ObSSTmpFileIOHandle::~ObSSTmpFileIOHandle()
{
  reset();
}

int ObSSTmpFileIOHandle::prepare_read(
    const int64_t read_size,
    const int64_t read_offset,
    const common::ObIOFlag io_flag,
    char *read_buf,
    const int64_t fd,
    const int64_t dir_id,
    const uint64_t tenant_id /* TODO(yaojiu): remove tenant_id */,
    const bool disable_page_cache)
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

int ObSSTmpFileIOHandle::prepare_write(
    char *write_buf,
    const int64_t write_size,
    const common::ObIOFlag io_flag,
    const int64_t fd,
    const int64_t dir_id,
    const uint64_t tenant_id /* TODO(yaojiu): remove tenant_id */)
{
  int ret = OB_SUCCESS;
  const int64_t bkt_cnt = 17;
  lib::ObMemAttr bkt_mem_attr(tenant_id, "TmpBlkIDBkt");
  if (OB_ISNULL(write_buf)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP_(buf));
  } else if (OB_FAIL(write_block_ids_.create(bkt_cnt, bkt_mem_attr))) {
    STORAGE_LOG(WARN, "create write block id set failed", K(ret), KP_(buf));
  } else {
    buf_ = write_buf;
    size_ = write_size;
    expect_write_size_ = write_size;
    fd_ = fd;
    dir_id_ = dir_id;
    tenant_id_ = tenant_id;
    is_read_ = false;
    has_wait_ = false;
    expect_read_size_ = 0;
    last_read_offset_ = -1;
    io_flag_ = io_flag;
  }
  return ret;
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObSSTmpFileIOHandle::do_shared_storage_write_wait()
{
  int ret = OB_SUCCESS;
  // If this write operation triggers asynchronous flushing, then the semaphore
  // for waiting for the completion of asynchronous flushing is needed.
  if (nullptr != wait_task_handle_.get() &&
      OB_FAIL(wait_task_handle_.get()->cond_wait())) {
    LOG_WARN("fail to async flush wait cond", K(ret), K(wait_task_handle_));
  } else {
    wait_task_handle_.reset();
  }
  return ret;
}

int ObSSTmpFileIOHandle::do_shared_storage_read_wait()
{
  int ret = OB_SUCCESS;

  // Copy page data from kvcache.
  for (int i = 0; OB_SUCC(ret) && i < page_cache_handles_.count(); ++i) {
    ObPageCacheHandle &page_cache_handle = page_cache_handles_.at(i);
    MEMCPY(page_cache_handle.buf_,
           page_cache_handle.page_handle_.value_->get_buffer() + page_cache_handle.offset_,
           page_cache_handle.size_);
    page_cache_handle.page_handle_.reset();
  }
  page_cache_handles_.reset();

  // Wait read io finish.
  for (int i = 0; OB_SUCC(ret) && i < io_handles_.count(); ++i) {
    ObIOReadHandle &io_handle = io_handles_.at(i);
    if (OB_FAIL(io_handle.handle_.wait())) {
      LOG_WARN("fail to do object handle read wait", K(ret), K(i), K(io_handle));
    } else {
      const char * temp_buf = io_handle.handle_.get_buffer();
      int64_t off = io_handle.offset_;
      int64_t siz = io_handle.size_;
      MEMCPY(io_handle.buf_, temp_buf + off, siz);
      io_handle.handle_.reset();
    }
  }
  io_handles_.reset();

  return ret;
}
#endif

int ObSSTmpFileIOHandle::wait()
{
  int ret = OB_SUCCESS;

  const int64_t timeout_ms = MAX(GCONF._data_storage_io_timeout / 1000, DEFAULT_IO_WAIT_TIME_MS);
  if (timeout_ms < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to wait, invalid argument, timeout must be positive", K(ret), K(timeout_ms));
  }

  if (OB_FAIL(ret)) {
    // some error happened, do nothing.
  } else if (GCTX.is_shared_storage_mode()) {
#ifdef OB_BUILD_SHARED_STORAGE
    // Shared Storage
    if (!is_finished_) {
      if (is_read_ && OB_FAIL(shared_storage_wait_read_finish(timeout_ms))) {
        LOG_WARN("fail to read wait in shared storage mode", K(ret), K(timeout_ms));
      } else if (!is_read_ && OB_FAIL(do_shared_storage_write_wait())) {
        LOG_WARN("fail to write wait in shared storage mode", K(ret));
      }
      ret_code_ = ret;
      is_finished_ = true;
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(ret_code_)) {
        LOG_WARN("temporary file io error", K(ret), KPC(this));
      }
    }
#endif
  } else {
    // Shared Nothing
    if (!is_finished_) {
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
  }

  return ret;
}

int ObSSTmpFileIOHandle::wait_write_finish(int64_t timeout_ms)
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

int ObSSTmpFileIOHandle::wait_read_finish(const int64_t timeout_ms)
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
    } else if (OB_FAIL(ObTmpFileManager::get_instance().get_tmp_file_handle(fd_, file_handle))) {
      STORAGE_LOG(WARN, "fail to get tmp file handle", K(ret));
    } else {
      tmp_file::ObTmpFileIOInfo io_info;
      io_info.fd_ = fd_;
      io_info.dir_id_ = dir_id_;
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

#ifdef OB_BUILD_SHARED_STORAGE
int ObSSTmpFileIOHandle::shared_storage_wait_read_finish(const int64_t timeout_ms)
{
  int ret = OB_SUCCESS;
  ObSSTenantTmpFileManager::ObTmpFileHandle tmp_file_handle;
  if (OB_UNLIKELY(has_wait_ && is_read_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("read wait() isn't reentrant interface, shouldn't call again", K(ret));
  } else if (OB_FAIL(do_shared_storage_read_wait())) {
    LOG_WARN("fail to wait tmp file io", K(ret), K(fd_), K(timeout_ms));
  } else if (is_read_ && !has_wait_) {
    if (size_ == expect_read_size_) {
      //do nothing
    } else if (OB_UNLIKELY(size_ > expect_read_size_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("read size more than expected size", K(ret), K(fd_),
               K(timeout_ms), K(size_), K(expect_read_size_));
    } else if (OB_FAIL(MTL(tmp_file::ObTenantTmpFileManager *)->get_ss_file_manager().get_tmp_file(fd_, tmp_file_handle))) {
      LOG_WARN("fail to get temporary file", K(ret), K(fd_));
    } else {
      tmp_file::ObTmpFileIOInfo io_info;
      io_info.fd_ = fd_;
      io_info.dir_id_ = dir_id_;
      io_info.size_ = expect_read_size_;
      io_info.buf_ = buf_;
      io_info.io_desc_ = io_flag_;
      io_info.io_timeout_ms_ = timeout_ms;
      while (OB_SUCC(ret) && size_ < expect_read_size_) {
        if (OB_FAIL(tmp_file_handle.get()->continue_read(io_info, *this))) {
          LOG_WARN("fail to continue read once batch", K(ret), K(timeout_ms), K(io_info), K(*this));
        } else if (OB_FAIL(do_shared_storage_read_wait())) {
          LOG_WARN("fail to wait tmp file io", K(ret), K(timeout_ms));
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
#endif

int ObSSTmpFileIOHandle::do_read_wait(const int64_t timeout_ms)
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
    if (OB_FAIL(tmp.handle_.wait())) {
      STORAGE_LOG(WARN, "fail to wait tmp read io", K(ret));
    } else {
      MEMCPY(tmp.buf_, tmp.handle_.get_buffer() + tmp.offset_, tmp.size_);
      tmp.handle_.reset();
    }
  }
  io_handles_.reset();
  return ret;
}

void ObSSTmpFileIOHandle::reset()
{
  for (int32_t i = 0; i < io_handles_.count(); i++) {
    io_handles_.at(i).handle_.reset();
  }
  for (int32_t i = 0; i < block_cache_handles_.count(); i++) {
    block_cache_handles_.at(i).block_handle_.reset();
  }
  if (0 != block_cache_handles_.count()) {
    if (!GCTX.is_shared_storage_mode()) {
      OB_TMP_FILE_STORE.dec_block_cache_num(tenant_id_, block_cache_handles_.count());
    }
  }
  for (int32_t i = 0; i < page_cache_handles_.count(); i++) {
    page_cache_handles_.at(i).page_handle_.reset();
  }
  if (0 != page_cache_handles_.count()) {
    // TODO: this page handle possessed memory usage in kvcache, maybe add some
    // statistic data for ss tmp file.
    if (!GCTX.is_shared_storage_mode()) {
      OB_TMP_FILE_STORE.dec_page_cache_num(tenant_id_, page_cache_handles_.count());
    }
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

bool ObSSTmpFileIOHandle::is_valid() const
{
  return OB_INVALID_FD != fd_ && OB_INVALID_ID != dir_id_ && OB_INVALID_TENANT_ID != tenant_id_
      && NULL != buf_ && size_ >= 0;
}

int ObSSTmpFileIOHandle::record_block_id(const int64_t block_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(write_block_ids_.set_refactored(block_id, 1))) {
    STORAGE_LOG(WARN, "record block id failed", K(ret), K(block_id));
  }
  return ret;
}

void ObSSTmpFileIOHandle::set_last_extent_id(const int64_t last_extent_id)
{
  last_extent_id_ = last_extent_id;
}

int64_t ObSSTmpFileIOHandle::get_last_extent_id() const
{
  return last_extent_id_;
}

ObSSTmpFileIOHandle::ObIOReadHandle::ObIOReadHandle()
  : handle_(), buf_(NULL), offset_(0), size_(0)
{
}

ObSSTmpFileIOHandle::ObIOReadHandle::ObIOReadHandle(const ObStorageObjectHandle &handle,
    char *buf, const int64_t offset, const int64_t size)
  : handle_(handle), buf_(buf), offset_(offset), size_(size)
{
}

ObSSTmpFileIOHandle::ObIOReadHandle::~ObIOReadHandle()
{
}

ObSSTmpFileIOHandle::ObIOReadHandle::ObIOReadHandle(const ObSSTmpFileIOHandle::ObIOReadHandle &other)
  : handle_(), buf_(NULL), offset_(0), size_(0)
{
  *this = other;
}

ObSSTmpFileIOHandle::ObIOReadHandle &ObSSTmpFileIOHandle::ObIOReadHandle::operator=(
    const ObSSTmpFileIOHandle::ObIOReadHandle &other)
{
  if (&other != this) {
    handle_ = other.handle_;
    offset_ = other.offset_;
    buf_ = other.buf_;
    size_ = other.size_;
  }
  return *this;
}
ObSSTmpFileIOHandle::ObPageCacheHandle::ObPageCacheHandle()
  : page_handle_(), buf_(NULL), offset_(0), size_(0)
{
}

ObSSTmpFileIOHandle::ObPageCacheHandle::ObPageCacheHandle(const ObTmpPageValueHandle &page_handle,
    char *buf, const int64_t offset, const int64_t size)
  : page_handle_(page_handle), buf_(buf), offset_(offset), size_(size)
{
}

ObSSTmpFileIOHandle::ObPageCacheHandle::~ObPageCacheHandle()
{
}

ObSSTmpFileIOHandle::ObPageCacheHandle::ObPageCacheHandle(
    const ObSSTmpFileIOHandle::ObPageCacheHandle &other)
  : page_handle_(), buf_(NULL), offset_(0), size_(0)
{
  *this = other;
}

ObSSTmpFileIOHandle::ObPageCacheHandle &ObSSTmpFileIOHandle::ObPageCacheHandle::operator=(
    const ObSSTmpFileIOHandle::ObPageCacheHandle &other)
{
  if (&other != this) {
    page_handle_ = other.page_handle_;
    offset_ = other.offset_;
    buf_ = other.buf_;
    size_ = other.size_;
  }
  return *this;
}

ObSSTmpFileIOHandle::ObBlockCacheHandle::ObBlockCacheHandle()
  : block_handle_(), buf_(NULL), offset_(0), size_(0)
{
}

ObSSTmpFileIOHandle::ObBlockCacheHandle::ObBlockCacheHandle(const ObTmpBlockValueHandle &block_handle,
    char *buf, const int64_t offset, const int64_t size)
  : block_handle_(block_handle), buf_(buf), offset_(offset), size_(size)
{
}

ObSSTmpFileIOHandle::ObBlockCacheHandle::~ObBlockCacheHandle()
{
}

ObSSTmpFileIOHandle::ObBlockCacheHandle::ObBlockCacheHandle(
    const ObSSTmpFileIOHandle::ObBlockCacheHandle &other)
  : block_handle_(), buf_(NULL), offset_(0), size_(0)
{
  *this = other;
}

ObSSTmpFileIOHandle::ObBlockCacheHandle &ObSSTmpFileIOHandle::ObBlockCacheHandle::operator=(
    const ObSSTmpFileIOHandle::ObBlockCacheHandle &other)
{
  if (&other != this) {
    block_handle_ = other.block_handle_;
    offset_ = other.offset_;
    buf_ = other.buf_;
    size_ = other.size_;
  }
  return *this;
}

/* -------------------------- ObITmpFile --------------------------- */

int ObITmpFile::inc_ref_cnt()
{
  int ret = OB_SUCCESS;
  ATOMIC_INC(&ref_cnt_);
  return ret;
}

int ObITmpFile::dec_ref_cnt(int64_t *new_ref_cnt)
{
  int ret = OB_SUCCESS;
  int ref_cnt = -1;
  ref_cnt = ATOMIC_AAF(&ref_cnt_, -1);
  if (OB_NOT_NULL(new_ref_cnt)) {
    *new_ref_cnt = ref_cnt;
  }
  return ret;
}

bool ObITmpFile::is_flushing()
{
  return ATOMIC_LOAD(&is_flushing_);
}

int ObITmpFile::set_is_flushing()
{
  int ret = OB_SUCCESS;
  if (ATOMIC_BCAS(&is_flushing_, false, true) == false) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR(
        "fail to set is_flushing, unexpected flushing tag, should be false",
        K(ret), K(ATOMIC_LOAD(&is_flushing_)));
  }
  return ret;
}

int ObITmpFile::set_not_flushing()
{
  int ret = OB_SUCCESS;
  if (ATOMIC_BCAS(&is_flushing_, true, false) == false) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR(
        "fail to set not_flushing, unexpected flushing tag, should be true",
        K(ret), K(ATOMIC_LOAD(&is_flushing_)));
  }
  return ret;
}

} // end namespace blocksstable
} // end namespace oceanbase
