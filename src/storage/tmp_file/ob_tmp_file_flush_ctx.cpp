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

#include "storage/tmp_file/ob_tmp_file_flush_ctx.h"

namespace oceanbase
{
namespace tmp_file
{

ObTmpFileWriteBlockTask::ObTmpFileWriteBlockTask(ObTmpFileFlushTask &flush_task)
    : flush_task_(flush_task)
{
}

void ObTmpFileWriteBlockTask::runTimerTask()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(flush_task_.write_one_block())) {
    STORAGE_LOG(WARN, "fail to async write blocks", KR(ret), K(flush_task_));
  }

  flush_task_.atomic_set_write_block_ret_code(ret);
  flush_task_.atomic_set_write_block_executed(true);
}

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
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTmpFileBatchFlushContext not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(expect_flush_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(expect_flush_size));
  } else if (OB_ISNULL(prio_mgr) || OB_ISNULL(flush_monitor)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(prio_mgr), KP(flush_monitor));
  } else if (OB_FAIL(flush_failed_array_.reserve(MAX_COPY_FAIL_COUNT))) {
    LOG_WARN("fail to reserve flush filed array", KR(ret), KPC(this));
  } else if (OB_FAIL(iter_.init(prio_mgr))) {
    LOG_WARN("failed to init iterator", KR(ret), KPC(this));
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
  for (int64_t i = 0; i < flush_failed_array_.count(); i++) {
    const ObTmpFileFlushFailRecord &record = flush_failed_array_.at(i);
    const ObSNTmpFileHandle &file_handle = record.file_handle_;
    if (OB_ISNULL(file_handle.get())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("file handle is nullptr", KR(ret));
    } else {
      ObSharedNothingTmpFile &file = *file_handle.get();
      if (record.is_meta_) {
        if (OB_FAIL(file.reinsert_meta_flush_node())) {
          LOG_ERROR("fail to reinsert meta flush node", KR(ret), KPC(this));
        }
      } else {
        if (OB_FAIL(file.reinsert_data_flush_node())) {
          LOG_ERROR("fail to reinsert data flush node", KR(ret), KPC(this));
        }
      }
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
  ObSNTmpFileHandle &file_handle = kv.second.file_handle_;
  if (OB_ISNULL(file_handle.get())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file handle is nullptr", KR(ret));
  } else {
    // remove all flush nodes for this file to prevent repeated flushing when flushing only data page or only meta page,
    // this avoids triggering a data page flush while waiting for I/O on meta pages, which could lead to new data items
    // being inserted to the flushing meta page, causing the memory meta page to become inconsistent with the page on disk.
    ObSharedNothingTmpFile &file = *file_handle.get();
    if (file.is_flushing()) {
      if (OB_FAIL(file.remove_data_flush_node())) {
        LOG_ERROR("fail to remove file data node from flush priority mgr", KR(ret), K(file));
      } else if (OB_FAIL(file.remove_meta_flush_node())) {
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
    flush_page_id_arr_(),
    write_block_ret_code_(OB_SUCCESS),
    io_result_ret_code_(OB_SUCCESS),
    data_length_(0),
    block_index_(-1),
    flush_seq_(-1),
    create_ts_(-1),
    is_write_block_executed_(false),
    is_io_finished_(false),
    fast_flush_tree_page_(false),
    recorded_as_prepare_finished_(false),
    type_(TaskType::INVALID),
    task_state_(ObTmpFileFlushTaskState::TFFT_INITED),
    tmp_file_block_handle_(),
    handle_(),
    flush_infos_(),
    flush_write_block_task_(*this)
{
  flush_infos_.set_attr(ObMemAttr(MTL_ID(), "TFFlushInfos"));
}

void ObTmpFileFlushTask::destroy()
{
  flush_write_block_task_.~ObTmpFileWriteBlockTask();
  // flush task io buffer is allocated in kv cache, and we hold its memory by keeping block_handle_.
  // since buffer's memory life cycle must be longer than the object handle,
  // the handle_ must be reset before block_handle_.
  handle_.reset();
  block_handle_.reset();
  flush_page_id_arr_.reset();
  inst_handle_.reset();
  kvpair_ = nullptr;
  write_block_ret_code_ = OB_SUCCESS;
  io_result_ret_code_ = OB_SUCCESS;
  data_length_ = 0;
  block_index_ = -1;
  flush_seq_ = -1;
  create_ts_ = -1;
  is_write_block_executed_ = false;
  is_io_finished_ = false;
  fast_flush_tree_page_ = false;
  recorded_as_prepare_finished_ = false;
  type_ = TaskType::INVALID;
  task_state_ = ObTmpFileFlushTaskState::TFFT_INITED;
  tmp_file_block_handle_.reset();
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

int ObTmpFileFlushTask::lazy_alloc_and_fill_block_buf_for_data_page_()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(flush_page_id_arr_.count() <= 0 ||
        flush_page_id_arr_.count() > ObTmpFileGlobal::BLOCK_PAGE_NUMS ||
        flush_page_id_arr_.count() != upper_align(data_length_, ObTmpFileGlobal::PAGE_SIZE) / ObTmpFileGlobal::PAGE_SIZE)){
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid flush page id array size", KR(ret), K(flush_page_id_arr_.count()), KPC(this));
  } else if (flush_infos_.size() == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("flush_infos_ is empty", KR(ret), KPC(this));
  } else if (OB_ISNULL(wbp_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("write buffer pool ptr is nullptr", KR(ret), KPC(this));
  } else if (OB_FAIL(prealloc_block_buf())) {
    LOG_WARN("fail to prealloc block", KR(ret), K(block_index_), KPC(this));
  } else {
    char* page_buf = nullptr;
    int32_t copy_index = 0;
    for (int32_t i = 0; OB_SUCC(ret) && i < flush_infos_.count(); ++i) {
      ObTmpFileFlushInfo &flush_info = flush_infos_.at(i);
      int64_t cur_info_fd = flush_info.fd_;
      int64_t cur_info_disk_begin_id = flush_info.flush_data_page_disk_begin_id_;
      int64_t cur_info_page_num = flush_info.flush_data_page_num_;
      int64_t cur_info_virtual_page_id = flush_info.flush_virtual_page_id_;
      if (copy_index != cur_info_disk_begin_id) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected flush info", KR(ret), K(i), K(copy_index), KPC(this));
      }
      while (OB_SUCC(ret) && copy_index < cur_info_disk_begin_id + cur_info_page_num
                          && copy_index < flush_page_id_arr_.count()) {
        uint32_t page_id = flush_page_id_arr_.at(copy_index);
        uint32_t next_page_id = ObTmpFileGlobal::INVALID_PAGE_ID; // UNUSED
        if (OB_FAIL(wbp_->read_page(cur_info_fd, page_id, ObTmpFilePageUniqKey(cur_info_virtual_page_id), page_buf, next_page_id))) {
          LOG_WARN("fail to read page", KR(ret), K(page_id), KPC(this));
        } else if (OB_UNLIKELY(!check_buf_range_valid(get_data_buf(), ObTmpFileGlobal::PAGE_SIZE))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid buffer range", KR(ret), KP(get_data_buf()), KPC(this));
        } else {
          // only copy the size we recorded if we need to flush the last page
          // since we do not hold last_page_lock and the last page may be appended
          int64_t copy_size = flush_info.file_size_ != 0 && cur_info_disk_begin_id + cur_info_page_num - 1 == copy_index ?
          flush_info.file_size_ % ObTmpFileGlobal::PAGE_SIZE : ObTmpFileGlobal::PAGE_SIZE;
          MEMCPY(get_data_buf() + copy_index * ObTmpFileGlobal::PAGE_SIZE, page_buf, copy_size);
        }
        copy_index += 1;
        cur_info_virtual_page_id += 1;
      }
    }

    if (OB_FAIL(ret)) {
      LOG_ERROR("fail to read page and fill block buf", KR(ret), KPC(this));
    }

    // release truncate_lock regardless of ret
    for (int32_t i = 0; i < flush_infos_.count(); ++i) {
      flush_infos_.at(i).file_handle_.get()->copy_finish();
    }

    if (OB_SUCC(ret)) {
      int tmp_ret = OB_SUCCESS;
      if (TaskType::DATA != type_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid task type when copy data in writing block", KR(ret), KPC(this));
      } else if (OB_TMP_FAIL(ObTmpBlockCache::get_instance().put_block(get_inst_handle(),
                                                                       get_kvpair(),
                                                                       get_block_handle()))) {
        LOG_WARN("fail to put block into block cache", KR(tmp_ret), KR(ret), KPC(this));
      }
    }
  }
  return ret;
}

int ObTmpFileFlushTask::write_one_block()
{
  int ret = OB_SUCCESS;
  handle_.reset();

  if (!block_handle_.is_valid()) {
    if (TaskType::DATA != type_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("block handle is null when writing non data task", KR(ret), KPC(this));
    } else if (OB_FAIL(lazy_alloc_and_fill_block_buf_for_data_page_())) {
      LOG_WARN("fail to lazy alloc and fill block buf", KR(ret), KPC(this));
    }
  }

  static const int64_t SEND_IO_WARN_TIMEOUT_US = 30 * 1000 * 1000; // 30s
  if (ObTimeUtil::current_time() - get_create_ts() > SEND_IO_WARN_TIMEOUT_US) { // debug log
    LOG_WARN("flush task send io takes too much time", KPC(this));
  }

  if (OB_SUCC(ret)) {
    blocksstable::ObMacroBlockWriteInfo write_info;
    write_info.io_desc_.set_wait_event(ObWaitEventIds::TMP_FILE_WRITE);
    write_info.io_desc_.set_sys_module_id(ObIOModule::TMP_TENANT_MEM_BLOCK_IO);
    write_info.buffer_ = get_data_buf();
    write_info.size_ = upper_align(get_data_length(), ObTmpFileGlobal::PAGE_SIZE);
    write_info.offset_ = 0;
    if (OB_FAIL(blocksstable::ObBlockManager::async_write_block(write_info, handle_))) {
      LOG_ERROR("fail to async write block", KR(ret), K(write_info));
    } else if (OB_FAIL(OB_SERVER_BLOCK_MGR.update_write_time(handle_.get_macro_id(),
        true/*update_to_max_time)*/))){ // update to max time to skip bad block inspect
      LOG_WARN("failed to update write time", KR(ret), K(handle_));
    }
  }
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
