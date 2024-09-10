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

#include "share/ob_errno.h"   // KR
#include "storage/tmp_file/ob_tmp_file_flush_ctx.h"

namespace oceanbase
{
namespace tmp_file
{

ObTmpFileFlushInfo::ObTmpFileFlushInfo()
  : fd_(ObTmpFileGlobal::INVALID_TMP_FILE_FD),
    batch_flush_idx_(0),
    has_last_page_lock_(false),
    insert_meta_tree_done_(false),
    update_meta_data_done_(false),
    file_handle_(),
    flush_data_page_disk_begin_id_(ObTmpFileGlobal::INVALID_PAGE_ID),
    flush_data_page_num_(-1),
    flush_virtual_page_id_(ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID),
    file_size_(0),
    flush_meta_page_array_()
{
  flush_meta_page_array_.set_attr(ObMemAttr(MTL_ID(), "TFFlushMetaArr"));
}

void ObTmpFileFlushInfo::reset()
{
  fd_ = ObTmpFileGlobal::INVALID_TMP_FILE_FD;
  batch_flush_idx_ = 0;
  has_last_page_lock_ = false;
  insert_meta_tree_done_ = false;
  update_meta_data_done_ = false;
  file_handle_.reset();
  flush_data_page_disk_begin_id_ = ObTmpFileGlobal::INVALID_PAGE_ID;
  flush_data_page_num_ = -1;
  flush_virtual_page_id_ = ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID;
  file_size_ = 0;
  flush_meta_page_array_.reset();
}

// -------------- ObTmpFileBatchFlushContext --------------- //

int ObTmpFileBatchFlushContext::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTmpFileBatchFlushContext init twice", KR(ret));
  } else if (OB_FAIL(file_ctx_hash_.create(256, ObMemAttr(MTL_ID(), "TFileFLCtx")))) {
    LOG_WARN("failed to create hash map", K(ret));
  } else {
    flush_failed_array_.set_attr(ObMemAttr(MTL_ID(), "TFFlushFailArr"));
    state_ = FlushCtxState::FSM_F1;
    is_inited_ = true;
  }
  return ret;
}

int ObTmpFileBatchFlushContext::prepare_flush_ctx(
    const int64_t expect_flush_size,
    ObTmpFileFlushPriorityManager *prio_mgr,
    ObTmpFileFlushMonitor *flush_monitor)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(expect_flush_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(expect_flush_size));
  } else if (OB_ISNULL(prio_mgr) || OB_ISNULL(flush_monitor)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(prio_mgr), KP(flush_monitor));
  } else if (OB_FAIL(flush_failed_array_.reserve(MAX_COPY_FAIL_COUNT))) {
    LOG_WARN("fail to reserve flush filed array", KR(ret), K(*this));
  } else if (OB_FAIL(iter_.init(prio_mgr))) {
    LOG_WARN("failed to init iterator", KR(ret), K(*this));
  } else {
    expect_flush_size_ = expect_flush_size;
    flush_monitor_ptr_ = flush_monitor;
  }

  if (OB_FAIL(ret)) {
    flush_failed_array_.reset();
    iter_.reset();
    LOG_INFO("failed to prepare flush ctx, rollback ctx", KR(ret), KPC(this));
  }
  return ret;
}

int ObTmpFileBatchFlushContext::clear_flush_ctx(ObTmpFileFlushPriorityManager &prio_mgr)
{
  int ret = OB_SUCCESS;
  // insert the remaining files in the iterator back into the flush priority manager
  if (OB_FAIL(iter_.reset())) {
    LOG_ERROR("failed to reset flush iterator", KR(ret), KPC(this));
  }

  // insert the files that failed to copy data back into the flush queue. after this step,
  // call files taken out by the iterator for this round
  // (excluding files with no dirty pages) will appear in the flush priority manager.
  for (int64_t i = 0; OB_SUCC(ret) && i < flush_failed_array_.count(); i++) {
    const ObTmpFileFlushFailRecord &record = flush_failed_array_.at(i);
    const ObTmpFileHandle &file_handle = record.file_handle_;
    if (OB_ISNULL(file_handle.get())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("file handle is nullptr", KR(ret));
    } else {
      ObSharedNothingTmpFile &file = *file_handle.get();
      file.reinsert_flush_node(record.is_meta_);
    }
  }

  if (OB_FAIL(ret)) {
  } else if (flush_seq_ctx_.prepare_finished_cnt_ == flush_seq_ctx_.create_flush_task_cnt_) {
    LOG_DEBUG("reset flush_seq_ctx_", KPC(this));
    flush_seq_ctx_.prepare_finished_cnt_ = 0;
    flush_seq_ctx_.create_flush_task_cnt_ = 0;
    flush_seq_ctx_.flush_sequence_ += 1;
    // remove the files that have already been successfully flushed(recorded in file_ctx_hash_)
    // from flush priority manager, ensuring that these files do not trigger flushing
    // before the completion of this round of flush IO.
    RemoveFileOp remove_op(prio_mgr);
    if (OB_FAIL(file_ctx_hash_.foreach_refactored(remove_op))) {
      LOG_ERROR("fail to erase file ctx from hash", KR(ret), KPC(this));
    } else {
      file_ctx_hash_.clear();
    }
  } else if (flush_seq_ctx_.prepare_finished_cnt_ < flush_seq_ctx_.create_flush_task_cnt_) {
    // likely to occur when write buffer pool is full and need to fast flush meta page
    LOG_WARN("flush_seq_ctx_ could not increase flush sequence", KPC(this));
  } else if (OB_UNLIKELY(flush_seq_ctx_.prepare_finished_cnt_ > flush_seq_ctx_.create_flush_task_cnt_)) {
    LOG_ERROR("unexpected flush_seq_ctx_", KPC(this));
  }

  fail_too_many_ = false;
  expect_flush_size_ = 0;
  actual_flush_size_ = 0;
  iter_.destroy();
  flush_failed_array_.reset();
  state_ = FlushCtxState::FSM_F1;
  return ret;
}

int ObTmpFileBatchFlushContext::RemoveFileOp::operator () (hash::HashMapPair<int64_t, ObTmpFileSingleFlushContext> &kv)
{
  int ret = OB_SUCCESS;
  ObTmpFileHandle &file_handle = kv.second.file_handle_;
  if (OB_ISNULL(file_handle.get())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file handle is nullptr", KR(ret));
  } else {
    // remove all flush nodes for this file to prevent repeated flushing when flushing only data page or only meta page,
    // this avoids triggering a data page flush while waiting for I/O on meta pages, which could lead to new data items
    // being inserted to the flushing meta page, causing the memory meta page to become inconsistent with the page on disk.
    ObSharedNothingTmpFile &file = *file_handle.get();
    if (file.is_flushing()) {
      if (OB_FAIL(file.remove_flush_node(false/*is_meta*/))) {
        LOG_ERROR("fail to remove file data node from flush priority mgr", KR(ret), K(file));
      } else if (OB_FAIL(file.remove_flush_node(true/*is_meta*/))) {
        LOG_ERROR("fail to remove file meta node from flush priority mgr", KR(ret), K(file));
      } else {
        LOG_DEBUG("succ to remove flush node from flush priority mgr", K(file));
      }
    }
  }
  return ret;
}

void ObTmpFileBatchFlushContext::destroy()
{
  is_inited_ = false;
  fail_too_many_ = false;
  expect_flush_size_ = 0;
  actual_flush_size_ = 0;
  flush_monitor_ptr_ = nullptr;
  state_ = FlushCtxState::FSM_F1;
  iter_.destroy();
  flush_failed_array_.reset();
  file_ctx_hash_.destroy();
}

void ObTmpFileBatchFlushContext::update_actual_flush_size(const ObTmpFileFlushTask &flush_task)
{
  actual_flush_size_ += flush_task.get_data_length();
}

void ObTmpFileBatchFlushContext::try_update_prepare_finished_cnt(const ObTmpFileFlushTask &flush_task, bool& recorded)
{
  int ret = OB_SUCCESS;

  recorded = false;
  if (OB_UNLIKELY(flush_seq_ctx_.prepare_finished_cnt_ >= flush_seq_ctx_.create_flush_task_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected flush_seq_ctx_", KPC(this));
  } else if (ObTmpFileFlushTask::TFFT_WAIT == flush_task.get_state() ||
      ObTmpFileFlushTask::TFFT_ABORT == flush_task.get_state() ||
      flush_task.get_data_length() == 0) {
    ++flush_seq_ctx_.prepare_finished_cnt_;
    recorded = true;
  }
}

void ObTmpFileBatchFlushContext::record_flush_stage()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(flush_monitor_ptr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("flush monitor is null", KR(ret));
  } else {
    flush_monitor_ptr_->record_flush_stage(state_);
  }
}

void ObTmpFileBatchFlushContext::record_flush_task(const int64_t data_length)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(flush_monitor_ptr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("flush monitor is null", KR(ret));
  } else {
    flush_monitor_ptr_->record_flush_task(data_length);
  }
}

// -------------- ObTmpFileFlushTask --------------- //

ObTmpFileFlushTask::ObTmpFileFlushTask()
  : inst_handle_(),
    kvpair_(nullptr),
    block_handle_(),
    write_block_ret_code_(OB_SUCCESS),
    ret_code_(OB_SUCCESS),
    data_length_(0),
    block_index_(-1),
    flush_seq_(-1),
    create_ts_(-1),
    is_io_finished_(false),
    fast_flush_tree_page_(false),
    recorded_as_prepare_finished_(false),
    task_state_(ObTmpFileFlushTaskState::TFFT_INITED),
    tmp_file_block_handle_(),
    handle_(),
    flush_infos_()
{
  flush_infos_.set_attr(ObMemAttr(MTL_ID(), "TFFlushInfos"));
}

void ObTmpFileFlushTask::destroy()
{
  block_handle_.reset();
  inst_handle_.reset();
  kvpair_ = nullptr;
  write_block_ret_code_ = OB_SUCCESS;
  ret_code_ = OB_SUCCESS;
  data_length_ = 0;
  block_index_ = -1;
  flush_seq_ = -1;
  create_ts_ = -1;
  is_io_finished_ = false;
  fast_flush_tree_page_ = false;
  recorded_as_prepare_finished_ = false;
  task_state_ = ObTmpFileFlushTaskState::TFFT_INITED;
  flush_infos_.reset();
}

int ObTmpFileFlushTask::prealloc_block_buf()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("prealloc block buf twice", KR(ret), KPC(this));
  } else if (OB_FAIL(ObTmpBlockCache::get_instance().prealloc_block(
                     ObTmpBlockCacheKey(block_index_, MTL_ID()), inst_handle_, kvpair_, block_handle_))) {
    LOG_WARN("fail to prealloc block", KR(ret), K(block_index_), K(MTL_ID()));
  }
  return ret;
}

int ObTmpFileFlushTask::write_one_block()
{
  int ret = OB_SUCCESS;
  handle_.reset();

  blocksstable::ObMacroBlockWriteInfo write_info;
  write_info.io_desc_.set_wait_event(2); // TODO: 检查是否需要用临时文件自己的event
  write_info.io_desc_.set_resource_group_id(THIS_WORKER.get_group_id());
  write_info.io_desc_.set_sys_module_id(ObIOModule::TMP_TENANT_MEM_BLOCK_IO);
  write_info.buffer_ = get_data_buf();
  write_info.size_ = OB_STORAGE_OBJECT_MGR.get_macro_object_size();
  write_info.offset_ = 0;

  if (FAILEDx(blocksstable::ObBlockManager::async_write_block(write_info, handle_))) {
    LOG_ERROR("fail to async write block", KR(ret), K(write_info));
  } else if (OB_FAIL(OB_SERVER_BLOCK_MGR.update_write_time(handle_.get_macro_id(),
      true/*update_to_max_time)*/))){ // update to max time to skip bad block inspect
    LOG_WARN("failed to update write time", KR(ret), K(handle_));
  }
  atomic_set_write_block_ret_code(ret);

  return ret;
}

// async wait, return OB_EAGAIN immediately if IO is not finished
int ObTmpFileFlushTask::wait_macro_block_handle()
{
  int ret = OB_SUCCESS;
  int64_t wait_timeout_ms = 0; // timeout == 0 for async wait
  if (OB_FAIL(handle_.wait(wait_timeout_ms))) {
    if (OB_EAGAIN == ret) {
      // do nothing
    } else {
      atomic_set_ret_code(ret);
      atomic_set_io_finished(true);
      LOG_WARN("fail to wait macro block handle", KR(ret), KPC(this));
      ret = OB_SUCCESS;
    }
  } else {
    atomic_set_ret_code(OB_SUCCESS);
    atomic_set_io_finished(true);
    LOG_DEBUG("macro block handle io finished", KR(ret), KPC(this));
  }
  return ret;
}

int64_t ObTmpFileFlushTask::get_total_page_num() const
{
  const int64_t PAGE_SIZE = ObTmpFileGlobal::PAGE_SIZE;
  return upper_align(data_length_, PAGE_SIZE) / PAGE_SIZE;
}

}  // end namespace tmp_file
}  // end namespace oceanbase
