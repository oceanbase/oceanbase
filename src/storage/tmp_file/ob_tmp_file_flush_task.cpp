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

#include "storage/tmp_file/ob_tmp_file_flush_task.h"
#include "share/io/ob_io_manager.h"
#include "share/ob_io_device_helper.h"
#include "storage/tmp_file/ob_tmp_file_manager.h"
#include "storage/tmp_file/ob_tmp_file_write_cache.h"
#include "storage/tmp_file/ob_tmp_file_write_cache_page.h"
#include "share/io/ob_io_manager.h"

namespace oceanbase
{
namespace tmp_file
{

void ObTmpFileWriteBlockTimerTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(flush_task_.write())) {
    LOG_WARN("fail to write block", KR(ret), K(flush_task_));
  }
  flush_task_.set_write_ret(ret);
  flush_task_.set_is_written(true);
}

ObTmpFileFlushTask::ObTmpFileFlushTask()
  : is_finished_(false),
    is_written_(false),
    ret_code_(-1),
    page_idx_(0),
    page_cnt_(0),
    size_(0),
    buf_(nullptr),
    create_ts_(ObTimeUtility::current_time()),
    allocator_(nullptr),
    tmp_file_block_handle_(),
    io_handle_(),
    page_array_(),
    write_block_task_(*this)
{
  page_array_.set_attr(ObMemAttr(MTL_ID(), "TmpFileFLTKAr"));
}

ObTmpFileFlushTask::~ObTmpFileFlushTask()
{
  reset();
}

void ObTmpFileFlushTask::reset()
{
  int ret = OB_SUCCESS;
  ObTmpFileWriteCache &write_cache = MTL(ObTenantTmpFileManager*)->get_sn_file_manager().get_write_cache();
  for (int32_t i = 0; OB_SUCC(ret) && i < page_array_.count(); ++i) {
    ObTmpFilePage *page = page_array_.at(i).get_page();
    if (OB_ISNULL(page)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("page is null", KR(ret), K(page_array_), K(i));
    } else if (!page->is_full() && write_cache.unlock(page->get_page_key().fd_)) {
      LOG_ERROR("fail to unlock page", KR(ret), KPC(this));
    }
  } // unlock bucket lock when task destruct

  // do not reset is_written_, is_finished_, ret_code_ and create_ts_ for debugging purpose
  page_idx_ = 0;
  page_cnt_ = 0;
  io_handle_.reset();
  page_array_.reset();
  tmp_file_block_handle_.reset();
  if (OB_NOT_NULL(buf_)) {
    allocator_->free(buf_);
    buf_ = nullptr;
  }
  allocator_ = nullptr;
  size_ = 0;
}

int ObTmpFileFlushTask::init(
    ObFIFOAllocator *flush_allocator,
    ObTmpFileBlockHandle block_handle)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(block_handle.get()) || OB_ISNULL(flush_allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(block_handle), K(flush_allocator));
  } else {
    allocator_ = flush_allocator;
    tmp_file_block_handle_ = block_handle;
  }
  return ret;
}

int ObTmpFileFlushTask::add_page(ObTmpFilePageHandle &page_handle)
{
  int ret = OB_SUCCESS;
  ObTmpFilePage *page = page_handle.get_page();
  if (OB_ISNULL(page)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("page ptr is null", KR(ret), K(page_handle));
  } else if (OB_FAIL(page_array_.push_back(page_handle))) {
    LOG_ERROR("fail to push back node", KR(ret), K(page_handle));
  } else {
    LOG_DEBUG("task add page", K(page_array_.count()), K(page_handle));
  }
  return ret;
}

int ObTmpFileFlushTask::memcpy_pages_()
{
  int ret = OB_SUCCESS;
  const int64_t PAGE_SIZE = ObTmpFileGlobal::PAGE_SIZE;
  for (int32_t i = 0; OB_SUCC(ret) && i < page_array_.count(); ++i) {
    ObTmpFilePageHandle page_handle = page_array_.at(i);
    if (OB_ISNULL(page_handle.get_page())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("page is null", KR(ret), KPC(this));
      break;
    }

    ObTmpFilePage &page = *page_handle.get_page();
    ObTmpFileWriteCacheKey page_key = page.get_page_key();
    if (!check_buf_range_valid(buf_ + i * PAGE_SIZE, PAGE_SIZE)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("task buf out of bound", KR(ret), K(page), KPC(this));
    } else if (PageType::META == page_key.type_ &&
               OB_FAIL(calc_and_set_meta_page_checksum_(page.get_buffer()))) {
      LOG_ERROR("fail to calculate and set meta page checksum", KR(ret), K(page), KPC(this));
    } else if(FALSE_IT(MEMCPY(buf_ + i * PAGE_SIZE, page.get_buffer(), PAGE_SIZE))) {
    } else {
      ObTmpPageCacheValue value(buf_ + i * PAGE_SIZE);
      if (PageType::DATA == page_key.type_) {
        ObTmpPageCacheKey key(page_key.fd_, page_key.virtual_page_id_, MTL_ID());
        ObTmpPageCache::get_instance().try_put_page_to_cache(key, value);
        LOG_DEBUG("put data page to read cache", K(key));
      } else if (PageType::META == page_key.type_) {
        ObTmpPageCacheKey key(page_key.fd_, page_key.tree_level_, page_key.level_page_index_, MTL_ID());
        ObTmpPageCache::get_instance().try_put_page_to_cache(key, value);
        LOG_DEBUG("put meta page to read cache", K(key));
      }
    }
  }
  return ret;
}

int ObTmpFileFlushTask::calc_and_set_meta_page_checksum_(char* page_buff)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(page_buff)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "invalid argument", KR(ret), KP(page_buff));
  } else {
    const int16_t page_header_size = sizeof(ObSharedNothingTmpFileTreePageHeader);
    ObSharedNothingTmpFileTreePageHeader page_header = *((ObSharedNothingTmpFileTreePageHeader *)(page_buff));
    page_header.checksum_ = ob_crc64(page_buff + page_header_size, ObTmpFileGlobal::PAGE_SIZE - page_header_size);
    MEMCPY(page_buff, &page_header, page_header_size);
  }
  return ret;
}

int ObTmpFileFlushTask::write()
{
  int ret = OB_SUCCESS;
  const int64_t PAGE_SIZE = ObTmpFileGlobal::PAGE_SIZE;
  ObTmpFileBlock *block = tmp_file_block_handle_.get();

  LOG_DEBUG("flush task write begin", KR(ret), K(page_idx_), K(page_cnt_), KPC(this));
  int64_t schedule_time_ms = (ObTimeUtility::current_time() - create_ts_) / 1000;
  if (schedule_time_ms > SCHEDULE_TIME_WARN_MS) {
    LOG_WARN("flush task schedule write too long", K(schedule_time_ms), KPC(this));
  }

  if (OB_ISNULL(block)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tmp file block is null", KR(ret));
  } else if (OB_UNLIKELY(page_idx_ < 0 || page_cnt_ <= 0
            || page_idx_ >= ObTmpFileGlobal::BLOCK_PAGE_NUMS
            || page_idx_ + page_cnt_ > ObTmpFileGlobal::BLOCK_PAGE_NUMS)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("page idx or page cnt is invalid", KR(ret), K(page_idx_), K(page_cnt_), KPC(this));
  } else if (OB_ISNULL(buf_ = static_cast<char *>(allocator_->alloc(page_cnt_ * PAGE_SIZE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", KR(ret), KPC(this));
  } else if (FALSE_IT(size_ = page_cnt_ * PAGE_SIZE)) {
  } else if (OB_FAIL(memcpy_pages_())) {
    LOG_WARN("fail to memcpy pages", KR(ret), KPC(this));
  }

  ObIOInfo io_info;
  blocksstable::MacroBlockId macro_id;
  if (OB_FAIL(ret)) {
  } else if (FALSE_IT(macro_id = block->get_macro_block_id())) {
  } else if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("macro_id is invalid", KR(ret), K(macro_id), KPC(this));
  } else {
    io_info.tenant_id_ = MTL_ID();
    io_info.offset_ = page_idx_ * ObTmpFileGlobal::PAGE_SIZE;
    io_info.size_ = size_;
    io_info.buf_ = buf_;
    io_info.flag_.set_write();
    io_info.flag_.set_sync();
    io_info.flag_.set_wait_event(ObWaitEventIds::TMP_FILE_WRITE);
    io_info.flag_.set_sys_module_id(ObIOModule::TMP_TENANT_MEM_BLOCK_IO);
    io_info.fd_.first_id_ = macro_id.first_id();
    io_info.fd_.second_id_ = macro_id.second_id();
    io_info.fd_.third_id_ = macro_id.third_id();
    io_info.fd_.device_handle_ = &LOCAL_DEVICE_INSTANCE;
    io_info.timeout_us_ = GCONF._data_storage_io_timeout;
    if (OB_FAIL(ObIOManager::get_instance().aio_write(io_info, io_handle_))) {
      LOG_WARN("fail to async write", KR(ret), K(io_info));
    } else {
      LOG_DEBUG("flush task send io succ", KR(ret), K(page_idx_), K(page_cnt_), KPC(this));
    }
  }
  LOG_DEBUG("flush task write end", KR(ret), KPC(this));
  return ret;
}

int ObTmpFileFlushTask::wait()
{
  int ret = OB_SUCCESS;
  ObTmpFileBlock *block = tmp_file_block_handle_.get();
  if (OB_ISNULL(block)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("tmp file block is null", KR(ret), KPC(this));
  } else if (is_finished()) { // TODO: use local is_finished flag
    LOG_ERROR("flush task already finished", KPC(this));
  } else if (!is_written()) {
    ret = OB_EAGAIN;
  } else {
    if (OB_FAIL(io_handle_.wait())) {
      if (OB_EAGAIN != ret) {
        LOG_WARN("io error", KR(ret), KPC(this));
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(block->insert_pages_into_flushing_list(page_array_))) {
          if (OB_RESOURCE_RELEASED == tmp_ret) {
            // OB_RESOURCE_RELEASED means the block is deleting
            // and no need to reinsert pages, we can free flush_task safely
            LOG_INFO("block is deleting, no need to insert pages for flushing", KR(tmp_ret), KPC(this));
          } else {
            LOG_ERROR("fail to insert page into block", KR(tmp_ret), KPC(this));
          }
        }
        ATOMIC_SET(&ret_code_, ret);
        ATOMIC_SET(&is_finished_, true);
      }
    } else {
      ATOMIC_SET(&ret_code_, OB_SUCCESS);
      ATOMIC_SET(&is_finished_, true);
    }

    if (is_finished()) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(block->reinsert_into_flush_prio_mgr())) {
        LOG_WARN("fail to reinsert_into_flush_prio_mgr", KR(tmp_ret), KPC(this));
      }
    } else if (REACH_TIME_INTERVAL(SCHEDULE_TIME_WARN_MS * 2)) {
      LOG_WARN("flush task not finished yet", KPC(this));
    }
  }
  LOG_DEBUG("flush task wait end", KR(ret), KR(ret_code_), KPC(this));
  return ret;
}

int ObTmpFileFlushTask::cancel(int ret_code)
{
  int ret = OB_SUCCESS;
  ObTmpFileBlock *block = tmp_file_block_handle_.get();
  if (OB_ISNULL(block)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tmp file block is null", KR(ret), KPC(this));
  } else if (page_array_.count() > 0 &&
             OB_FAIL(block->insert_pages_into_flushing_list(page_array_))) {
    if (OB_RESOURCE_RELEASED == ret) {
      LOG_INFO("block is deleting, no need to insert pages for flushing", KR(ret), KPC(this));
      ret = OB_SUCCESS;
    } else {
      LOG_ERROR("fail to insert page into block", KR(ret), KPC(this));
    }
  } else if (OB_FAIL(block->reinsert_into_flush_prio_mgr())) {
    LOG_ERROR("fail to exec reinsert_into_flush_prio_mgr", KR(ret), KPC(this));
  }
  ATOMIC_SET(&ret_code_, ret_code);
  ATOMIC_SET(&is_finished_, true);
  return ret;
}

}  // end namespace tmp_file
}  // end namespace oceanbase
