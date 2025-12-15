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

#include "storage/tmp_file/ob_tmp_file_write_cache.h"
#include "storage/tmp_file/ob_tmp_file_swap_job.h"
#include "storage/tmp_file/ob_tmp_file_flush_task.h"
#include "storage/tmp_file/ob_tmp_file_write_cache_page.h"
#include "storage/meta_store/ob_server_storage_meta_service.h"
#include "storage/tmp_file/ob_tmp_file_cache.h"
#include "share/io/ob_io_manager.h"

namespace oceanbase
{
namespace tmp_file
{

ObTmpFileWriteCache::ObTmpFileWriteCache()
  : tmp_file_block_manager_(nullptr),
    is_inited_(false),
    is_flush_all_(false),
    tg_id_(-1),
    flush_ret_(OB_SUCCESS),
    used_page_cnt_(0),
    memory_limit_(0),
    tenant_config_access_ts_(0),
    default_memory_limit_(0),
    disk_usage_limit_(0),
    cur_timer_idx_(0),
    flush_tg_id_(),
    swap_queue_size_(0),
    io_waiting_queue_size_(0),
    swap_queue_(),
    io_waiting_queue_(),
    free_page_list_(),
    shrink_ctx_(),
    idle_cond_(),
    metrics_(),
    bucket_lock_(),
    resize_lock_(),
    pages_(),
    page_map_(),
    page_allocator_(),
    block_allocator_(),
    flush_allocator_(),
    swap_allocator_()
{
  for (int32_t i = 0; i < MAX_FLUSH_TIMER_NUM; ++i) {
    flush_tg_id_[i] = -1;
  }
}

ObTmpFileWriteCache::~ObTmpFileWriteCache()
{
  destroy();
}

int ObTmpFileWriteCache::init(ObTmpFileBlockManager *tmp_file_block_manager)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTmpFileWriteCache init twice", KR(ret));
  } else if (OB_FAIL(page_allocator_.init(
                     lib::ObMallocAllocator::get_instance(), OB_MALLOC_NORMAL_BLOCK_SIZE,
                     ObMemAttr(MTL_ID(), "TmpFilePage", ObCtxIds::DEFAULT_CTX_ID)))) {
    LOG_WARN("fail to init page allocator", KR(ret));
  } else if (OB_FAIL(block_allocator_.init(
                     lib::ObMallocAllocator::get_instance(), OB_MALLOC_BIG_BLOCK_SIZE,
                     ObMemAttr(MTL_ID(), "TmpFileCacheBlk", ObCtxIds::DEFAULT_CTX_ID)))) {
    LOG_WARN("fail to init block allocator", KR(ret));
  } else if (OB_FAIL(flush_allocator_.init(
                     lib::ObMallocAllocator::get_instance(), OB_MALLOC_MIDDLE_BLOCK_SIZE,
                     ObMemAttr(MTL_ID(), "TmpFileFLTask", ObCtxIds::DEFAULT_CTX_ID)))) {
    LOG_WARN("fail to init flush task allocator", KR(ret));
  } else if (OB_FAIL(swap_allocator_.init(
                     lib::ObMallocAllocator::get_instance(), OB_MALLOC_NORMAL_BLOCK_SIZE,
                     ObMemAttr(MTL_ID(), "TmpFileSwapJ", ObCtxIds::DEFAULT_CTX_ID)))) {
    LOG_WARN("fail to init swap job allocator", KR(ret));
  } else if (OB_FAIL(bucket_lock_.init(BUCKET_CNT))) {
    LOG_WARN("fail to init bucket lock", KR(ret));
  } else if (OB_FAIL(pages_.init())) {
    LOG_WARN("fail to init pages array", KR(ret));
  } else if (OB_FAIL(free_page_list_.init())) {
    LOG_WARN("fail to init free page list", KR(ret));
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::TmpFileSwap, tg_id_))) {
    LOG_WARN("fail to create timer thread", KR(ret));
  } else if (OB_FAIL(TG_SET_RUNNABLE(tg_id_, *this))) {
    LOG_WARN("fail to set swap tg runnable", KR(ret));
  } else if (OB_FAIL(idle_cond_.init(ObWaitEventIds::THREAD_IDLING_COND_WAIT))) {
    LOG_WARN("fail to init cond", KR(ret));
  } else if (OB_FAIL(page_map_.init("TmpFileBlkMap", MTL_ID()))) {
    LOG_WARN("fail to init page map", KR(ret));
  } else {
    tmp_file_block_manager_ = tmp_file_block_manager;
    is_flush_all_ = false;
    memory_limit_ = 0;
    tenant_config_access_ts_ = 0;
    cur_timer_idx_ = 0;
    for (int32_t i = 0; OB_SUCC(ret) && i < MAX_FLUSH_TIMER_NUM; ++i) {
      if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::TmpFileFlush, flush_tg_id_[i]))) {
        LOG_WARN("fail to create flush timer thread", KR(ret));
      }
    }
    swap_queue_size_ = 0;
    io_waiting_queue_size_ = 0;
    shrink_ctx_.reset();
    is_inited_ = true;
  }
  return ret;
}

int ObTmpFileWriteCache::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTmpFileWriteCache is not inited", KR(ret));
  } else if (OB_FAIL(TG_START(tg_id_))) {
    LOG_WARN("fail to start swap thread", KR(ret));
  } else {
    for (int32_t i = 0; OB_SUCC(ret) && i < MAX_FLUSH_TIMER_NUM; ++i) {
      if (OB_FAIL(TG_START(flush_tg_id_[i]))) {
        LOG_WARN("fail to start flush timer thread", KR(ret));
      }
    }
  }
  return ret;
}

void ObTmpFileWriteCache::stop()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTmpFileWriteCache is not inited", KR(ret));
  } else {
    TG_STOP(tg_id_);
    for (int32_t i = 0; OB_SUCC(ret) && i < MAX_FLUSH_TIMER_NUM; ++i) {
      TG_STOP(flush_tg_id_[i]);
    }
  }
}

void ObTmpFileWriteCache::wait()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTmpFileWriteCache is not inited", KR(ret));
  } else {
    TG_WAIT(tg_id_);
    for (int32_t i = 0; OB_SUCC(ret) && i < MAX_FLUSH_TIMER_NUM; ++i) {
      TG_WAIT(flush_tg_id_[i]);
    }
  }
}

void ObTmpFileWriteCache::destroy()
{
  if (IS_INIT) {
    LOG_INFO("write cache destroy begin");
    print_();
    cleanup_list_();
    if (pages_.size() > 0) {
      release_blocks_reverse_(pages_.size() - BLOCK_PAGE_NUMS, 0);
    }

    memory_limit_ = 0;
    is_flush_all_ = false;
    free_page_list_.reset();
    if (-1 != tg_id_) {
      TG_DESTROY(tg_id_);
      tg_id_ = -1;
    }
    for (int32_t i = 0; i < MAX_FLUSH_TIMER_NUM; ++i) {
      if (-1 != flush_tg_id_[i]) {
        TG_DESTROY(flush_tg_id_[i]);
        flush_tg_id_[i] = -1;
      }
    }
    swap_queue_size_ = 0;
    io_waiting_queue_size_ = 0;

    pages_.destroy();
    page_map_.reset();
    block_allocator_.reset();
    flush_allocator_.reset();
    swap_allocator_.reset();

    is_inited_ = false;
    LOG_INFO("write cache destroy complete");
  }
}

void ObTmpFileWriteCache::run1()
{
  int ret = OB_SUCCESS;
  lib::set_thread_name("TFSwap");
  while (!has_set_stop()) {

    if (OB_FAIL(try_shrink_())) {
      LOG_WARN("try_shrink failed", KR(ret));
      ret = OB_SUCCESS;
    }

    if (OB_FAIL(swap_())) {
      LOG_WARN("swap failed", KR(ret));
      ret = OB_SUCCESS;
    }

    if (OB_FAIL(flush_())) {
      LOG_WARN("flush failed", KR(ret));
      ret = OB_SUCCESS;
    }

    if (TC_REACH_TIME_INTERVAL(REFRESH_CONFIG_INTERVAL)) {
      refresh_disk_usage_limit_();
      tmp_file_block_manager_->print_block_usage();
      // tmp_file_block_manager_->print_blocks();
      metrics_.print();
      print_();
    }

    int64_t idle_time = cal_idle_time_();
    if (!has_set_stop() && idle_time != 0) {
      common::ObBKGDSessInActiveGuard inactive_guard;
      idle_cond_.wait(idle_time);
    }
  }

  if (!has_set_stop()) {
    cleanup_list_();
  }
}

int ObTmpFileWriteCache::swap_()
{
  int ret = OB_SUCCESS;
  int32_t free_page_cnt = free_page_list_.size();
  LOG_DEBUG("doing swap", K(free_page_cnt), K(swap_queue_size_), K(io_waiting_queue_size_), K(ATOMIC_LOAD(&used_page_cnt_)));
  while (OB_SUCC(ret) && free_page_cnt > 0 && !swap_queue_.is_empty()) {
    ObTmpFileSwapJob *swap_job = nullptr;
    if (OB_FAIL(pop_swap_job_(swap_job))) {
    } else {
      metrics_.record_swap_job(ObTimeUtility::current_time() - swap_job->get_create_ts());
      int32_t expect_swap_cnt = swap_job->get_expect_swap_cnt();
      free_page_cnt = max(0, free_page_cnt - expect_swap_cnt);
      swap_job->signal_swap_complete(OB_SUCCESS);
    }
  }

  // wake up timeout jobs
  int32_t swap_queue_size = ATOMIC_LOAD(&swap_queue_size_);
  while (OB_SUCC(ret) && swap_queue_size > 0) {
    ObTmpFileSwapJob *swap_job = nullptr;
    if (OB_FAIL(pop_swap_job_(swap_job))) {
    } else if (swap_job->get_abs_timeout_ts() <= ObTimeUtility::current_time()) {
      metrics_.record_swap_job(ObTimeUtility::current_time() - swap_job->get_create_ts());
      if (OB_FAIL(swap_job->signal_swap_complete(OB_TIMEOUT))) {
        LOG_WARN("fail to signal swap complete", KR(ret), KPC(swap_job));
      }
    } else if (OB_FAIL(add_swap_job_(swap_job))) {
      LOG_WARN("fail to add swap job", KR(ret));
    }
    swap_queue_size -= 1;
  }

  // wake up all jobs if IO error occurs
  int ret_code = get_flush_ret_code_();
  if (OB_SERVER_OUTOF_DISK_SPACE == ret_code ||
      OB_TMP_FILE_EXCEED_DISK_QUOTA == ret_code) {
    while (OB_SUCC(ret) && OB_SUCCESS != ret_code && !swap_queue_.is_empty()) {
      ObTmpFileSwapJob *swap_job = nullptr;
      if (OB_FAIL(pop_swap_job_(swap_job))) {
      } else {
        metrics_.record_swap_job(ObTimeUtility::current_time() - swap_job->get_create_ts());
        swap_job->signal_swap_complete(ret_code);
      }
    }
  }

  return ret;
}

int ObTmpFileWriteCache::flush_()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!SERVER_STORAGE_META_SERVICE.is_started())) {
    ret = OB_NOT_RUNNING;
    LOG_WARN("ObTmpFileFlushTG does not work before server slog replay finished", KR(ret));
  } else {
    if (!io_waiting_queue_.is_empty()) {
      (void) exec_wait_();
    }

    int64_t expect_flush_cnt = cal_flush_page_cnt_();
    ObTmpFileBlockFlushIterator block_iterator;
    ObTmpFileBlockFlushPriorityManager &flush_prio_mgr = tmp_file_block_manager_->get_flush_priority_mgr();
    ObArray<ObTmpFileFlushTask *> tasks;
    ObArray<ObTmpFileBlockHandle> failed_blocks;
    if (expect_flush_cnt == 0) {
      LOG_DEBUG("no need to flush", K(expect_flush_cnt), K(ATOMIC_LOAD(&used_page_cnt_)));
    } else if (OB_FAIL(check_tmp_file_disk_usage_limit_(metrics_.get_flushing_data_size()))) {
      set_flush_ret_code_(ret);
    } else if (OB_FAIL(tasks.reserve(MAX_FLUSH_TASK_NUM_PER_BATCH))) {
      LOG_WARN("fail to prepare allocate flush tasks", KR(ret));
    } else if (OB_FAIL(failed_blocks.reserve(MAX_ITER_BLOCK_NUM_PER_BATCH))) {
      LOG_WARN("fail to prepare allocate failed_blocks", KR(ret));
    } else if (OB_FAIL(block_iterator.init(&flush_prio_mgr))) {
      LOG_WARN("fail to init block iterator", KR(ret));
    } else if (OB_FAIL(build_task_(expect_flush_cnt, block_iterator, tasks, failed_blocks))) {
      LOG_WARN("fail to build flush task", KR(ret));
    } else if (OB_FAIL(exec_wait_())) {
      LOG_WARN("fail to exec wait", KR(ret));
    }
  }
  return ret;
}

int ObTmpFileWriteCache::try_shrink_()
{
  int ret = OB_SUCCESS;
  int64_t actual_evict_num = 0;
  bool is_auto = false;
  ObTimeGuard time_guard("write_cache_shrink", 1 * 1000 * 1000);
  if (is_shrink_required_(is_auto)) {
    LOG_DEBUG("current write cache shrinking state", K(shrink_ctx_));
    switch (shrink_ctx_.shrink_state_) {
      case WriteCacheShrinkContext::INVALID:
        if (OB_FAIL(begin_shrinking_(is_auto))) {
          LOG_WARN("fail to init shrink context", KR(ret), K(shrink_ctx_.shrink_state_));
        } else {
          idle_cond_.signal();
          advance_shrink_state_();
        }
        break;
      case WriteCacheShrinkContext::SHRINKING_SWAP:
        if (!shrink_ctx_.is_valid()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("shrink context is invalid", KR(ret));
        } else if (is_shrink_range_all_free_()) {
          advance_shrink_state_();
        }
        break;
      case WriteCacheShrinkContext::SHRINKING_RELEASE_BLOCKS:
        if (OB_FAIL(release_shrink_range_blocks_())) {
          LOG_ERROR("fail to shrink wbp", KR(ret), K(shrink_ctx_.shrink_state_));
        } else if (pages_.size() == shrink_ctx_.lower_page_id_) {
          LOG_INFO("wbp shrink release blocks complete", K(shrink_ctx_), K(pages_.size()));
          advance_shrink_state_();
        } else {
          LOG_ERROR("wbp shrink not finish, could not advance shrinking state",
              K(pages_.size()), K(shrink_ctx_));
        }
        break;
      case WriteCacheShrinkContext::SHRINKING_FINISH:
        if (OB_FAIL(finish_shrinking_())) {
          LOG_ERROR("fail to finish shrinking", KR(ret), K(shrink_ctx_));
        }
        break;
      default:
        break;
    }
  }

  // abort shrinking if wbp memory limit enlarge or flush fail with OB_SERVER_OUTOF_DISK_SPACE
  int ret_code = get_flush_ret_code_();
  if (shrink_ctx_.is_valid() && (!is_shrink_required_(is_auto) || OB_SERVER_OUTOF_DISK_SPACE == ret)) {
    LOG_INFO("aborting write cache shrinking due to disk out of space or no need to shrink", KR(ret), K(shrink_ctx_));
    finish_shrinking_();
  }
  time_guard.click("wbp_shrink finish one step");
  return ret;
}

// Assert end <= begin
int ObTmpFileWriteCache::release_blocks_reverse_(const int64_t begin, const int64_t end)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(begin < 0 || begin % BLOCK_PAGE_NUMS != 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("begin or end is invalid", KR(ret), K(begin), K(end));
  } else {
    for (int64_t i = begin; OB_SUCC(ret) && i >= end; i -= BLOCK_PAGE_NUMS) {
      if (OB_ISNULL(pages_[i].get_buffer()) || i + BLOCK_PAGE_NUMS > pages_.size()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("unexpected ptr or index out of bound",
            KR(ret), K(i), K(pages_[i]), K(pages_.size()));
      } else {
        LOG_DEBUG("free block at ", K(i), K(pages_.size()));
        block_allocator_.free(pages_[i].get_buffer());
        for (int64_t j = 0; j < BLOCK_PAGE_NUMS; ++j) {
          int32_t last = pages_.size() - 1;
          page_allocator_.free(pages_.get_ptr(last));
          pages_.pop_back();
        }
      }
    }
  }
  return ret;
}

int ObTmpFileWriteCache::expand_()
{
  int ret = OB_SUCCESS;
  common::TCRWLock::WLockGuard guard(resize_lock_);
  int64_t current_capacity = get_current_capacity_();
  const int64_t memory_limit = get_memory_limit();
  const int64_t target_capacity = std::min(memory_limit,
                                           max(current_capacity * 2, int64_t(WBP_BLOCK_SIZE)));
  while (OB_SUCC(ret) && current_capacity < target_capacity) {
    if (shrink_ctx_.is_valid()) {
      ret = OB_OP_NOT_ALLOW;
      if (REACH_COUNT_INTERVAL(10)) {
        LOG_WARN("wbp is shrinking, cannot expand now", K(current_capacity), K(memory_limit));
      }
    }

    int64_t old_size = pages_.size();
    char *buffer = nullptr;
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_ISNULL(buffer = static_cast<char *>(block_allocator_.alloc(WBP_BLOCK_SIZE)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate buffer", KR(ret), K(current_capacity), K(memory_limit));
    } else {
      uint32_t index = pages_.size();
      ObDList<PageNode> tmp_list;
      for (int32_t i = 0; i < BLOCK_PAGE_NUMS; ++i, ++index) {
        void *page_buf = nullptr;
        ObTmpFilePage *page = nullptr;
        int64_t buffer_offset = i * ObTmpFileGlobal::PAGE_SIZE;
        if (OB_ISNULL(page_buf = page_allocator_.alloc(sizeof(ObTmpFilePage)))) {
        } else if (FALSE_IT(page = new (page_buf) ObTmpFilePage(buffer + buffer_offset, index))) {
        } else if (OB_FAIL(pages_.push_back(page))) {
          LOG_WARN("fail to push back page", KR(ret), K(i), K(pages_.size()));
        } else if (!tmp_list.add_last(&page->get_list_node())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("fail to add page to tmp free list in expand", KR(ret), K(i), K(pages_.size()));
        }
      }

      if (OB_SUCC(ret)) {
        free_page_list_.push_range(tmp_list);
      } else {
        if (OB_NOT_NULL(buffer)) {
          block_allocator_.free(buffer);
        }
        for (int32_t i = pages_.size() - 1; i > old_size; --i) {
          page_allocator_.free(pages_.get_ptr(i));
          pages_.pop_back();
        }
      }
    }
    current_capacity = get_current_capacity_();
  }
  LOG_INFO("write cache expand", KR(ret),
    K(pages_.size()), K(free_page_list_.size()), K(ATOMIC_LOAD(&used_page_cnt_)),
    K(current_capacity), K(memory_limit));
  return ret;
}

int ObTmpFileWriteCache::alloc_page(
    const ObTmpFileWriteCacheKey& page_key,
    const ObTmpFilePageId& page_id,
    const int64_t timeout_ms,
    ObTmpFilePageHandle &page_handle)
{
  int ret = OB_SUCCESS;
  bool unused_var = false;
  return alloc_page(page_key, page_id, timeout_ms, page_handle, unused_var);
}

int ObTmpFileWriteCache::alloc_page(
    const ObTmpFileWriteCacheKey& page_key,
    const ObTmpFilePageId& page_id,
    const int64_t timeout_ms,
    ObTmpFilePageHandle &page_handle,
    bool &trigger_swap_page)
{
  int ret = OB_SUCCESS;
  page_handle.reset();
  trigger_swap_page = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("write cache is not init", KR(ret), K(page_key));
  } else if (OB_SUCC(page_map_.get(page_key, page_handle))) {
    // page exist, do nothing
    LOG_DEBUG("page exist", K(page_key), K(page_id), K(page_handle));
  } else if (OB_ENTRY_NOT_EXIST != ret) {
    LOG_WARN("fail to get page", KR(ret), K(page_key), K(page_id));
  } else if (FALSE_IT(ret = OB_SUCCESS)) {
  } else {
    while (OB_SUCC(ret)) {
      int64_t current_capacity = get_current_capacity_();
      int64_t memory_limit = get_memory_limit();
      if (free_page_list_.size() <= 0) {
        if (current_capacity < memory_limit && OB_FAIL(expand_())) {
          if (OB_OP_NOT_ALLOW == ret) {
            LOG_INFO("write cache is shrinking, cannot expand now",
                KR(ret), K(current_capacity), K(memory_limit));
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("fail to expand write cache",
                KR(ret), K(current_capacity), K(memory_limit));
          }
        }
      }

      if (OB_FAIL(ret)) {
      } else if (free_page_list_.size() > 0) {
        common::TCRWLock::RLockGuard guard(resize_lock_);
        PageNode *head = nullptr;
        if (OB_ISNULL(head = free_page_list_.pop_front())) {
          LOG_DEBUG("free page list is empty",
              K(free_page_list_.size()), K(page_key), K(page_id));
        } else if (0 != head->page_.get_ref()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("page ref is not 0! maybe it is not released", KR(ret), K(head->page_));
        } else if (shrink_ctx_.in_shrinking_range(head)) {
          ObSpinLockGuard shrink_guard(shrink_ctx_.shrink_lock_);
          if (!shrink_ctx_.shrink_list_.add_last(head)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("fail to add page to shrink list", KR(ret), K(head->page_));
          }
          continue;
        } else {
          ObTmpFilePage &curr = head->page_;
          curr.set_page_key(page_key);
          curr.set_page_id(page_id);
          page_handle.init(&curr);
          update_current_max_used_percent_();
          ATOMIC_INC(&used_page_cnt_);
          break;
        }
      }

      int64_t real_timeout_ms = min(timeout_ms, OB_IO_MANAGER.get_object_storage_io_timeout_ms(MTL_ID()));
      if (FAILEDx(swap_page_(real_timeout_ms))) {
        LOG_WARN("fail to swap page", KR(ret), K(page_key), K(real_timeout_ms));
      } else {
        trigger_swap_page = true;
      }
    }
  }
  if (OB_SUCC(ret) && !page_handle.is_valid()) {
    ret = OB_ALLOCATE_TMP_FILE_PAGE_FAILED;
  }
  LOG_DEBUG("alloc_page", KR(ret), K(page_key), K(page_id), K(page_handle));
  return ret;
}

int ObTmpFileWriteCache::swap_page_(const int64_t timeout_ms)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  ObTmpFileSwapJob *swap_job = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("write cache not init", KR(ret));
  } else if (OB_ISNULL(buf = swap_allocator_.alloc(sizeof(ObTmpFileSwapJob)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory for swap job", KR(ret));
  } else if (FALSE_IT(swap_job = new (buf) ObTmpFileSwapJob())) {
  } else if (OB_FAIL(swap_job->init(SWAP_PAGE_NUM_PER_BATCH, timeout_ms))) {
    LOG_WARN("fail to init swap job", KR(ret));
  } else if (OB_FAIL(add_swap_job_(swap_job))) {
    LOG_WARN("fail to add swap job", KR(ret));
  } else {
    LOG_DEBUG("swapping page", K(pages_.size()), K(used_page_cnt_));
    idle_cond_.signal();
    if (OB_FAIL(swap_job->wait_swap_complete())) {
      LOG_WARN("fail to wait swap job complete", KR(ret));
    }
  }

  LOG_DEBUG("swapping page complete", K(pages_.size()), K(used_page_cnt_), KPC(swap_job));
  if (OB_NOT_NULL(swap_job)) {
    if (OB_FAIL(swap_job->get_ret_code())) {
      // override OB_TIMEOUT to prevent potential retries when writing temporary files
      ret = OB_TIMEOUT == swap_job->get_ret_code() ? OB_IO_ERROR : swap_job->get_ret_code();
      LOG_WARN("swap job complete with error", KR(ret), KPC(swap_job));
    }
    swap_allocator_.free(swap_job);
  }
  return ret;
}


int ObTmpFileWriteCache::get_page(const ObTmpFileWriteCacheKey& page_key,
                                  ObTmpFilePageHandle &page_handle)
{
  int ret = OB_SUCCESS;
  page_handle.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("write cache is not init", KR(ret), K(page_key));
  } else if (OB_FAIL(page_map_.get(page_key, page_handle))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("get page failed", KR(ret), K(page_key));
    }
  }
  LOG_DEBUG("get page handle", KR(ret), K(page_key), K(page_handle));
  return ret;
}

int ObTmpFileWriteCache::put_page(const ObTmpFilePageHandle &page_handle)
{
  int ret = OB_SUCCESS;
  ObTmpFileWriteCacheKey page_key;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("write cache is not init", KR(ret), K(page_handle));
  } else if (OB_ISNULL(page_handle.get_page())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("page ptr is nullptr", KR(ret));
  } else if (FALSE_IT(page_key = page_handle.get_page()->get_page_key())) {
  } else if (OB_FAIL(page_map_.insert(page_key, page_handle))) {
    LOG_WARN("put page failed", KR(ret), K(page_handle));
  }
  LOG_DEBUG("put page handle", KR(ret), K(page_handle));
  return ret;
}

int ObTmpFileWriteCache::free_page(const ObTmpFileWriteCacheKey& page_key)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("write cache is not init", KR(ret), K(page_key));
  } else if (OB_FAIL(page_map_.erase(page_key))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("fail to free page", KR(ret), K(page_key));
    } else {
      ret = OB_SUCCESS;
    }
  }
  // we only decrease the reference count of the page.
  // the actual release and return of the page are deferred until the reference count drops to zero
  LOG_DEBUG("free page", KR(ret), K(page_key));
  return ret;
}

// TODO: check disabled_kv_cache flag in tmp file, if disabled, we should not look up kv cache here.
int ObTmpFileWriteCache::load_page(
    const ObTmpFilePageHandle &page_handle,
    const blocksstable::MacroBlockId &macro_block_id,
    const int64_t timeout_ms)
{
  int ret = OB_SUCCESS;
  ObTmpPageValueHandle p_handle;
  ObTmpPageCacheKey page_key(page_handle.get_page()->get_page_key(), MTL_ID());
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("write cache is not init", KR(ret), K(page_handle));
  } else if (OB_ISNULL(page_handle.get_page())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("page ptr is nullptr", KR(ret));
  } else if (OB_SUCC(ObTmpPageCache::get_instance().get_page(page_key, p_handle))) {
    MEMCPY(page_handle.get_page()->get_buffer(), p_handle.value_->get_buffer(), ObTmpFileGlobal::PAGE_SIZE);
    LOG_DEBUG("get page from kv cache", K(page_handle));
  } else if (OB_ENTRY_NOT_EXIST != ret) {
    LOG_WARN("fail to get page from kv cache", KR(ret), K(page_handle));
  } else if (FALSE_IT(ret = OB_SUCCESS)) {
  } else {
    int64_t page_idx_in_block = page_handle.get_page()->get_page_id().page_index_in_block_;
    blocksstable::ObMacroBlockHandle mb_handle;
    blocksstable::ObMacroBlockReadInfo read_info;
    read_info.io_desc_.set_mode(ObIOMode::READ);
    read_info.io_desc_.set_wait_event(ObWaitEventIds::TMP_FILE_READ);
    read_info.macro_block_id_ = macro_block_id;
    read_info.size_ = ObTmpFileGlobal::PAGE_SIZE;
    read_info.offset_ = page_idx_in_block * ObTmpFileGlobal::PAGE_SIZE;
    read_info.buf_ = page_handle.get_page()->get_buffer();
    read_info.io_callback_ = nullptr;
    read_info.io_timeout_ms_ = timeout_ms;
    if (OB_FAIL(OB_SERVER_BLOCK_MGR.read_block(read_info, mb_handle))) {
      LOG_WARN("fail to read macro block", KR(ret), K(read_info), K(macro_block_id));
    } else {
      page_handle.get_page()->set_loading(false);
    }
  }
  LOG_DEBUG("load page end", KR(ret), K(macro_block_id), K(page_handle));
  return ret;
}

int ObTmpFileWriteCache::shared_lock(const int64_t bucket_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("write cache is not init", KR(ret), K(bucket_id));
  } else if (OB_FAIL(bucket_lock_.rdlock(bucket_id_to_idx_(bucket_id)))) {
    LOG_WARN("fail to hold shared lock", KR(ret), K(bucket_id));
  }
  return ret;
}

int ObTmpFileWriteCache::try_exclusive_lock(const int64_t bucket_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("write cache is not init", KR(ret), K(bucket_id));
  } else if (OB_FAIL(bucket_lock_.try_wrlock(bucket_id_to_idx_(bucket_id)))){
    if (OB_EAGAIN != ret) {
      LOG_WARN("fail to try lock", KR(ret), K(bucket_id));
    }
  }
  return ret;
}

int ObTmpFileWriteCache::unlock(const int64_t bucket_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("write cache is not init", KR(ret), K(bucket_id));
  } else if (OB_FAIL(bucket_lock_.unlock(bucket_id_to_idx_(bucket_id)))) {
    LOG_ERROR("fail to unlock", KR(ret), K(bucket_id));
  }
  return ret;
}

int ObTmpFileWriteCache::add_free_page_(ObTmpFilePage *page)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("write cache is not init", KR(ret), KPC(page));
  } else if (OB_ISNULL(page) || page->get_ref() != 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KPC(page));
  } else {
    common::TCRWLock::RLockGuard guard(resize_lock_);
    page->reset();
    PageNode *node = &page->get_list_node();
    if (!shrink_ctx_.in_shrinking_range(node)) {
      if (OB_FAIL(free_page_list_.push_back(node))) {
        LOG_ERROR("fail to push back free list", KR(ret), KPC(page));
      }
    } else {
      ObSpinLockGuard shrink_guard(shrink_ctx_.shrink_lock_);
      if (!shrink_ctx_.shrink_list_.add_last(node)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("fail to push back shrink list", KR(ret), KPC(page));
      }
    }
    ATOMIC_DEC(&used_page_cnt_);
  }
  return ret;
}

int ObTmpFileWriteCache::add_swap_job_(ObTmpFileSwapJob *swap_job)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("write cache is not init", KR(ret), KPC(swap_job));
  } else if (OB_FAIL(swap_queue_.push(swap_job))) {
    LOG_WARN("fail to push swap job", KR(ret), KPC(swap_job));
  } else {
    ATOMIC_INC(&swap_queue_size_);
  }
  return ret;
}

int ObTmpFileWriteCache::pop_swap_job_(ObTmpFileSwapJob *&swap_job)
{
  int ret = OB_SUCCESS;
  swap_job = nullptr;
  ObSpLinkQueue::Link *link = nullptr;
  if (IS_NOT_INIT) {
  } else if (OB_FAIL(swap_queue_.pop(link))) {
  } else if (OB_ISNULL(link)) {
  } else {
    ATOMIC_DEC(&swap_queue_size_);
    swap_job = static_cast<ObTmpFileSwapJob *>(link);
  }
  return ret;
}

int ObTmpFileWriteCache::alloc_flush_task_(ObTmpFileFlushTask *&task)
{
  int ret = OB_SUCCESS;
  task = nullptr;
  void *buf = nullptr;
  if (OB_ISNULL(buf = flush_allocator_.alloc(sizeof(ObTmpFileFlushTask)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory for flush task", KR(ret));
  } else {
    task = new (buf) ObTmpFileFlushTask();
  }
  return ret;
}

int ObTmpFileWriteCache::free_flush_task_(ObTmpFileFlushTask *task)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("write cache is not init", KR(ret), KPC(task));
  } else if (OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("task ptr is null", KR(ret), KPC(task));
  } else {
    LOG_DEBUG("free flush task", KPC(task));
    task->~ObTmpFileFlushTask();
    flush_allocator_.free(task);
  }
  return ret;
}

int ObTmpFileWriteCache::add_flush_task_(ObTmpFileFlushTask *task)
{
  int ret = OB_SUCCESS;
  common::ObSpLinkQueue::Link *link = dynamic_cast<ObSpLinkQueue::Link *>(task);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("write cache is not init", KR(ret), KPC(task));
  } else if (OB_ISNULL(link)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("task ptr is null", KR(ret), KPC(task));
  } else if (OB_FAIL(io_waiting_queue_.push(link))) {
    LOG_ERROR("fail to push flush task", KR(ret), KPC(task));
  } else {
    ATOMIC_INC(&io_waiting_queue_size_);
  }
  return ret;
}

int ObTmpFileWriteCache::pop_flush_task_(ObTmpFileFlushTask *&task)
{
  int ret = OB_SUCCESS;
  task = nullptr;
  ObSpLinkQueue::Link *link = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("write cache is not init", KR(ret));
  } else if (OB_FAIL(io_waiting_queue_.pop(link))) {
    LOG_WARN("fail to pop flush task", KR(ret));
  } else if (OB_ISNULL(link)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("link is nullptr", KR(ret));
  } else {
    ATOMIC_DEC(&io_waiting_queue_size_);
    task = static_cast<ObTmpFileFlushTask *>(link);
  }
  return ret;
}

int ObTmpFileWriteCache::try_to_set_macro_block_idx_(ObTmpFileBlock &block)
{
  int ret = OB_SUCCESS;
  blocksstable::ObMacroBlockHandle mb_handle;
  bool first_flush = !block.get_macro_block_id().is_valid();
  if (first_flush) {
    if (OB_FAIL(LOCAL_DEVICE_INSTANCE.check_space_full(ObTmpFileGlobal::SN_BLOCK_SIZE))) {
      LOG_WARN("fail to check space full", KR(ret), K(block));
    } else if (OB_FAIL(OB_SERVER_BLOCK_MGR.alloc_block(mb_handle))) {
      LOG_WARN("fail to async write block", KR(ret));
    } else if (OB_FAIL(block.set_macro_block_id(mb_handle.get_macro_id()))) {
      LOG_WARN("fail to set macro block id", KR(ret), K(block));
    }
  }
  if (OB_SERVER_OUTOF_DISK_SPACE == ret) {
    set_flush_ret_code_(OB_SERVER_OUTOF_DISK_SPACE);
  }
  return ret;
}

int ObTmpFileWriteCache::build_task_(
    const int64_t expect_flush_cnt,
    ObTmpFileBlockFlushIterator &iterator,
    ObIArray<ObTmpFileFlushTask *> &tasks,
    ObIArray<ObTmpFileBlockHandle> &failed_blocks)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t actual_flush_cnt = 0;

  while (OB_SUCC(ret)
        && actual_flush_cnt < expect_flush_cnt
        && MAX_FLUSHING_DATA_SIZE >= metrics_.get_flushing_data_size()
        && MAX_FLUSH_TASK_NUM_PER_BATCH >= tasks.count()
        && MAX_ITER_BLOCK_NUM_PER_BATCH >= failed_blocks.count()) {
    ObTmpFileBlockHandle block_handle;
    if (OB_FAIL(iterator.next(block_handle))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to get next block handle", KR(ret), K(block_handle));
      }
      break;
    } else if (OB_ISNULL(block_handle.get())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("block is nullptr", KR(ret));
    } else if (OB_FAIL(build_task_in_block_(block_handle, tasks, failed_blocks, actual_flush_cnt))) {
      LOG_WARN("fail to build task in block", KR(ret));
    }
  }

  for (int64_t i = 0; i < failed_blocks.count(); ++i) {
    ObTmpFileBlockHandle &block_handle = failed_blocks.at(i);
    if (OB_ISNULL(block_handle.get())) {
      tmp_ret = OB_ERR_UNEXPECTED;
      LOG_WARN("block is nullptr", K(tmp_ret), K(i), K(block_handle));
    } else if (OB_TMP_FAIL(block_handle.get()->reinsert_into_flush_prio_mgr())) {
      LOG_ERROR("fail to reinsert into flush prio mgr", KR(tmp_ret), K(block_handle));
    }
  }
  for (int32_t i = 0; i < tasks.count(); ++i) {
    ObTmpFileFlushTask *task = tasks.at(i);
    if (OB_ISNULL(task) || OB_UNLIKELY(cur_timer_idx_ < 0 || cur_timer_idx_ >= MAX_FLUSH_TIMER_NUM)) {
      tmp_ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid timer index or task ptr", KR(tmp_ret), K(i), K(cur_timer_idx_), KPC(task));
    } else if (OB_TMP_FAIL(TG_SCHEDULE(flush_tg_id_[cur_timer_idx_],
                          task->get_write_block_task(), 0/*delay*/))) {
      LOG_ERROR("fail to schedule flush task", KR(tmp_ret), KPC(task));
    }  else if (OB_TMP_FAIL(add_flush_task_(task))) {
      LOG_ERROR("fail to add task to continue waiting", KR(tmp_ret), KPC(task));
    } else {
      cur_timer_idx_ = (cur_timer_idx_ + 1) % MAX_FLUSH_TIMER_NUM;
      metrics_.record_flush_task(task->get_page_cnt());
    }
  }

  if (TC_REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
    LOG_INFO("build_task_end", KR(ret), K(actual_flush_cnt),
          K(expect_flush_cnt), K(io_waiting_queue_size_));
  }
  return ret;
}

int ObTmpFileWriteCache::build_task_in_block_(
    ObTmpFileBlockHandle &block_handle,
    ObIArray<ObTmpFileFlushTask *> &tasks,
    ObIArray<ObTmpFileBlockHandle> &failed_blocks,
    int64_t &actual_flush_cnt)
{
  int ret = OB_SUCCESS;
  ObTmpFileBlockHandle cur_blk_handle = block_handle;
  ObTmpFileBlockFlushingPageIterator page_iterator;
  if (OB_ISNULL(block_handle.get())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("block is nullptr", KR(ret));
  } else if (OB_FAIL(try_to_set_macro_block_idx_(*block_handle.get()))) {
    LOG_WARN("fail to set macro block index", KR(ret), K(block_handle));
  } else if (OB_FAIL(page_iterator.init(*block_handle.get()))) {
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_ERROR("fail to init page iterator", KR(ret), K(block_handle));
    }
  } else {
    while (OB_SUCC(ret) && MAX_FLUSH_TASK_NUM_PER_BATCH >= tasks.count()) {
      if (OB_FAIL(page_iterator.next_range())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next range", KR(ret));
        } else {
          ret = OB_SUCCESS;
        }
        break;
      }
      for (int32_t i = 0; OB_SUCC(ret) && MAX_FLUSH_TASK_NUM_PER_BATCH >= tasks.count(); ++i) {
        ObTmpFileFlushTask *task = nullptr;
        if (OB_FAIL(alloc_flush_task_(task))) {
          LOG_WARN("fail to alloc flush task", KR(ret), KPC(task));
        } else if (OB_FAIL(task->init(&flush_allocator_, cur_blk_handle))) {
          LOG_WARN("fail to init task", KR(ret), KPC(task));
        } else {
          block_handle.reset();
        }

        if (FAILEDx(collect_pages_(page_iterator, *task, cur_blk_handle))) {
          LOG_WARN("fail to collect pages", KR(ret), KPC(task));
        }

        if (OB_FAIL(ret) || (OB_NOT_NULL(task) && task->get_page_cnt() <= 0)) {
          if (OB_NOT_NULL(task)) {
            task->cancel(OB_CANCELED);
            free_flush_task_(task);
            task = nullptr;
          }
          break;
        } else if (OB_FAIL(tasks.push_back(task))) {
          LOG_ERROR("fail to push back task", KR(ret), KPC(task));
        } else {
          actual_flush_cnt += task->get_page_cnt();
          LOG_DEBUG("build_task_ succ", K(actual_flush_cnt), KPC(task), K(io_waiting_queue_size_));
        }
      }
    }
  }

  if (OB_NOT_NULL(block_handle.get())) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(failed_blocks.push_back(block_handle))) {
      LOG_ERROR("fail to push back block handle to reinsert array", KR(tmp_ret), K(block_handle));
    }
  }
  return ret;
}

int ObTmpFileWriteCache::collect_pages_(
    ObTmpFileBlockFlushingPageIterator &page_iterator,
    ObTmpFileFlushTask &task,
    ObTmpFileBlockHandle block_handle)
{
  int ret = OB_SUCCESS;
  ObTmpFileBlock &block = *block_handle.get();
  int32_t page_cnt = 0;
  int32_t first_page_idx = -1;
  int32_t incomplete_page_cnt = 0;
  int32_t meta_page_cnt = 0;

  while (OB_SUCC(ret)) {
    ObTmpFilePageHandle page_handle;
    if (OB_FAIL(page_iterator.next_page_in_range(page_handle))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to get next page", KR(ret), K(page_handle), K(page_iterator));
      }
      break;
    } else if (OB_ISNULL(page_handle.get_page())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("page is null", KR(ret), K(page_handle), K(page_iterator));
      break;
    }

    ObTmpFilePage *page = page_handle.get_page();
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(task.add_page(page_handle))) {
      LOG_WARN("fail to add page", K(task));
    } else {
      LOG_DEBUG("task collecting page", K(page_handle));
      page_cnt += 1;
      int64_t page_idx_in_blk = page->get_page_id().page_index_in_block_;
      first_page_idx = first_page_idx == -1 ? page_idx_in_blk : first_page_idx;
      incomplete_page_cnt += (page->is_full() ? 0 : 1);
      meta_page_cnt += (PageType::META == page->get_page_key().type_ ? 1 : 0);
    }
  }

  if (OB_ITER_END == ret) {
    metrics_.record_incomplete_page(incomplete_page_cnt);
    metrics_.record_meta_page(meta_page_cnt);
    task.set_page_idx(first_page_idx);
    task.set_page_cnt(page_cnt);
    ret = OB_SUCCESS; // override OB_ITER_END
    LOG_DEBUG("collect pages", KR(ret), K(task));
  }

  LOG_DEBUG("collect pages end", KR(ret), K(task));
  return ret;
}

int ObTmpFileWriteCache::exec_wait_()
{
  int ret = OB_SUCCESS;
  int64_t task_cnt = ATOMIC_LOAD(&io_waiting_queue_size_);
  for (; OB_SUCC(ret) && task_cnt > 0; --task_cnt) {
    ObTmpFileFlushTask *task = nullptr;
    if (OB_FAIL(pop_flush_task_(task))) {
      LOG_WARN("fail to pop flush task", KR(ret));
    } else if (!task->is_written()) {
      add_flush_task_(task);
      task = nullptr;
    } else if (OB_SUCCESS != task->get_write_ret()) {
      task->cancel(task->get_write_ret());
    } else if (OB_FAIL(task->wait())) {
      if (OB_EAGAIN == ret) { // add task into queue again
        ret = OB_SUCCESS;
        add_flush_task_(task);
        task = nullptr;
      } else {
        LOG_WARN("fail to exec wait for tmp file flush io", KR(ret), KPC(task));
      }
    }

    // ignore ret
    if (OB_NOT_NULL(task)) {
      LOG_DEBUG("task is finished, evict", KPC(task));
      set_flush_ret_code_(task->get_ret_code());
      if (OB_SUCCESS == task->get_ret_code()
          && OB_FAIL(evict_(*task))) {
        LOG_WARN("fail to evict", KR(ret), KPC(task));
      } else if (FALSE_IT(metrics_.record_flush_task_over(task->get_page_cnt()))) {
      } else if (OB_FAIL(free_flush_task_(task))) {
        LOG_ERROR("fail to free flush task", KR(ret), K(io_waiting_queue_size_));
      }
    }
  }
  LOG_DEBUG("exec wait", KR(ret), K(io_waiting_queue_size_), K(swap_queue_size_));
  return ret;
}

int ObTmpFileWriteCache::evict_(ObTmpFileFlushTask &task)
{
  int ret = OB_SUCCESS;
  ObIArray<ObTmpFilePageHandle> &page_array = task.get_page_array();
  for (int32_t i = 0; OB_SUCC(ret) && i < page_array.count(); ++i) {
    ObTmpFilePageHandle &page_handle = page_array.at(i);
    if (OB_ISNULL(page_handle.get_page())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("page is null", KR(ret), K(task));
    } else {
      ObTmpFilePage &page = *page_handle.get_page();
      const ObTmpFileWriteCacheKey& page_key = page.get_page_key();
      int64_t fd = page_key.fd_;
      if (OB_FAIL(free_page(page_key))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          LOG_INFO("page is not in page map, maybe it has been evicted", KR(ret), K(page), K(task));
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to free page", KR(ret), K(page), K(task));
        }
      }
    }
  }
  // we will call unlock() in ~ObTmpFileFlushTask() to release locks for incomplete pages
  ObTmpFileBlock *block = task.get_block_handle().get();
  if (FAILEDx(block->flush_pages_succ(task.get_page_idx(), task.get_page_cnt()))) {
    LOG_WARN("fail to flush pages succ", KR(ret), K(task));
  }
  LOG_DEBUG("evict end", KR(ret), K(task));
  return ret;
}

int64_t ObTmpFileWriteCache::cal_idle_time_()
{
  int64_t idle_time = 0;
  if (OB_UNLIKELY(!SERVER_STORAGE_META_SERVICE.is_started())
      || (ATOMIC_LOAD(&swap_queue_size_) == 0
          && ATOMIC_LOAD(&io_waiting_queue_size_) == 0
          && !shrink_ctx_.is_valid())) {
    idle_time = SWAP_INTERVAL;
  } else if (ATOMIC_LOAD(&swap_queue_size_) > 0 && free_page_list_.size() > 0) {
    idle_time = 0;
  } else {
    idle_time = SWAP_FAST_INTERVAL;
  }
  return idle_time;
}

// 简单策略: 1.达到 L1 90%，刷60%，并开始刷meta页和未写满页; 2.达到 L2 80%，刷40%; 3.达到 L3 60%，刷20%;
int64_t ObTmpFileWriteCache::cal_flush_page_cnt_()
{
  int64_t expect_flush_cnt = 0;
  int64_t max_page_cnt = get_memory_limit() / ObTmpFileGlobal::PAGE_SIZE;
  int64_t used_page_cnt = ATOMIC_LOAD(&used_page_cnt_);
  if (ATOMIC_LOAD(&is_flush_all_)) {
    expect_flush_cnt = max_page_cnt;
  } else if (used_page_cnt >= max_page_cnt * FLUSH_WATERMARK_L1 / 100) {
    expect_flush_cnt = max_page_cnt * FLUSH_PERCENT_L1 / 100;
  } else if (used_page_cnt >= max_page_cnt * FLUSH_WATERMARK_L2 / 100) {
    expect_flush_cnt = max_page_cnt * FLUSH_PERCENT_L2 / 100;
  } else if (used_page_cnt >= max_page_cnt * FLUSH_WATERMARK_L3 / 100) {
    expect_flush_cnt = max_page_cnt * FLUSH_PERCENT_L3 / 100;
  }
  LOG_DEBUG("cal_flush_page_cnt_ end", K(expect_flush_cnt), K(max_page_cnt), K(used_page_cnt));
  return expect_flush_cnt;
}

// refresh tmp file disk usage limit from tenant config
void ObTmpFileWriteCache::refresh_disk_usage_limit_()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("write cache is not inited", KR(ret));
  } else {
    omt::ObTenantConfigGuard config(TENANT_CONF_TIL(MTL_ID(), ACCESS_TENANT_CONFIG_TIMEOUT_US));
    if (!config.is_valid()) {
      // do nothing
    } else {
      const int64_t max_disk_usage = config->temporary_file_max_disk_size;
      int64_t disk_limit = max_disk_usage > 0 ? max_disk_usage : 0;
      ATOMIC_SET(&disk_usage_limit_, disk_limit);
    }
  }
}

// ATTENTION! it is time consuming due to we have to iterate tmp_file_block_mgr to get block usage state,
// do not call this function frequently
int ObTmpFileWriteCache::check_tmp_file_disk_usage_limit_(const int64_t flushing_size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("write cache is not inited", KR(ret));
  } else {
    int64_t disk_usage_limit = ATOMIC_LOAD(&disk_usage_limit_);
    int64_t used_page_num = 0;
    int64_t tmp_file_block_num = 0;
    int64_t current_disk_usage = 0;
    if (OB_FAIL(tmp_file_block_manager_->get_block_usage_stat(used_page_num, tmp_file_block_num))) {
      LOG_WARN("fail to get tmp file block usage stat", KR(ret));
    } else if (FALSE_IT(current_disk_usage = flushing_size + (tmp_file_block_num * ObTmpFileGlobal::SN_BLOCK_SIZE))) {
    } else if (disk_usage_limit > 0 && current_disk_usage > disk_usage_limit) {
      ret = OB_TMP_FILE_EXCEED_DISK_QUOTA;
      LOG_INFO("tmp file exceeds disk usage limit",
          KR(ret), K(current_disk_usage), K(disk_usage_limit), K(tmp_file_block_num), K(flushing_size));
    }
  }
  return ret;
}

bool ObTmpFileWriteCache::is_memory_sufficient_()
{
  int64_t used_page_cnt = ATOMIC_LOAD(&used_page_cnt_);
  int64_t max_page_cnt = pages_.size();
  return ATOMIC_LOAD(&is_flush_all_) ? false : used_page_cnt <= max_page_cnt * FLUSH_WATERMARK_L2 / 100;
}

void ObTmpFileWriteCache::print_()
{
  int64_t current_capacity = pages_.size();
  int64_t cache_page_cnt = page_map_.count();
  int64_t free_page_cnt = free_page_list_.size();
  int64_t max_page_cnt = get_memory_limit() / ObTmpFileGlobal::PAGE_SIZE;
  int64_t used_page_cnt = ATOMIC_LOAD(&used_page_cnt_);
  int64_t used_page_watermark = current_capacity > 0 && max_page_cnt > 0 ? used_page_cnt * 100 / max_page_cnt : 0;
  int64_t disk_limit = ATOMIC_LOAD(&disk_usage_limit_);
  bool is_flush_all = ATOMIC_LOAD(&is_flush_all_);

  int64_t io_queue_size = ATOMIC_LOAD(&io_waiting_queue_size_);
  int64_t swap_queue_size = ATOMIC_LOAD(&swap_queue_size_);
  LOG_INFO("tmp file write cache info",
    K(used_page_watermark), K(cache_page_cnt),
    K(free_page_cnt), K(used_page_cnt),
    K(io_queue_size), K(swap_queue_size),
    K(current_capacity), K(max_page_cnt), K(disk_limit),
    K(is_flush_all));
}

int64_t ObTmpFileWriteCache::get_current_capacity_()
{
  return pages_.size() * ObTmpFileGlobal::PAGE_SIZE;
}

int64_t ObTmpFileWriteCache::get_memory_limit()
{
  int64_t memory_limit = 0;
  if (default_memory_limit_ > 0) {
    memory_limit = upper_align_(default_memory_limit_, WBP_BLOCK_SIZE);
  } else if (tenant_config_access_ts_ > 0 &&
             ObTimeUtility::current_time() - tenant_config_access_ts_ < REFRESH_CONFIG_INTERVAL) {
    memory_limit = ATOMIC_LOAD(&memory_limit_);
  } else {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
    if (!tenant_config.is_valid()) {
      memory_limit = memory_limit_ <= 0 ? DEFAULT_MEMORY_LIMIT : memory_limit_;
      LOG_INFO("failed to get tenant config", K(MTL_ID()), K(memory_limit), K(memory_limit_));
    } else if (0 == tenant_config->_temporary_file_io_area_size) {
      memory_limit = WBP_BLOCK_SIZE;
    } else {
      memory_limit =
        lib::get_tenant_memory_limit(MTL_ID()) * tenant_config->_temporary_file_io_area_size / 100;
    }
    memory_limit = upper_align_(memory_limit, WBP_BLOCK_SIZE);
    memory_limit_ = memory_limit;
    tenant_config_access_ts_ = ObTimeUtility::current_time();
  }
  return memory_limit;
}

bool ObTmpFileWriteCache::is_shrink_required_(bool &is_auto)
{
  bool b_ret = false;
  int64_t current_capacity = get_current_capacity_();
  int64_t memory_limit = get_memory_limit();
  int64_t max_used_page_watermark = ATOMIC_LOAD(&shrink_ctx_.current_max_used_watermark_);
  is_auto = false;

  LOG_DEBUG("is_shrink_required_", K(current_capacity), K(memory_limit), K(max_used_page_watermark), K(shrink_ctx_.is_valid()));
  if (current_capacity > memory_limit) {
    b_ret = true;
  } else if (current_capacity <= memory_limit) {
    if (shrink_ctx_.is_valid()) {
      if (shrink_ctx_.is_auto()) {
        // if used_page_num exceeds threshold during auto-shrinking, stop shrinking
        b_ret = WriteCacheShrinkContext::AUTO_SHRINKING_WATERMARK_L1 >= max_used_page_watermark;
      } else {
        // manual-shrinking would stop if memory_limit increase during shrinking
        b_ret = false;
      }
    } else {
      int64_t last_shrink_ts = ATOMIC_LOAD(&shrink_ctx_.last_shrink_complete_ts_);
      if (ObTimeUtility::current_time() - last_shrink_ts >= WriteCacheShrinkContext::SHRINKING_PERIOD) {
        if (WriteCacheShrinkContext::AUTO_SHRINKING_WATERMARK_L1 >= max_used_page_watermark &&
            current_capacity > WBP_BLOCK_SIZE) {
          is_auto = true;
          b_ret = true; // invoke auto-shrinking
          LOG_INFO("invoke auto shrinking due to low watermark", K(max_used_page_watermark), K(last_shrink_ts));
        } else {
          // re-count max watermark for the next period
          b_ret = false;
          LOG_DEBUG("re-count max watermark for the next period",
              K(max_used_page_watermark), K(current_capacity), K(memory_limit));
          ATOMIC_SET(&shrink_ctx_.current_max_used_watermark_, 0);
          ATOMIC_SET(&shrink_ctx_.last_shrink_complete_ts_, ObTimeUtility::current_time());
          update_current_max_used_percent_();
        }
      }
    }
  }

  if (shrink_ctx_.is_execution_too_long()) {
    b_ret = false;
    LOG_INFO("auto-shrinking takes too much time, stop it", K(shrink_ctx_));
  }

  return b_ret;
}

int ObTmpFileWriteCache::begin_shrinking_(const bool is_auto)
{
  int ret = OB_SUCCESS;
  common::TCRWLock::RLockGuard guard(resize_lock_);
  int64_t lower_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
  int64_t upper_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
  uint32_t max_allow_alloc_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("write cache is not init", KR(ret));
  } else if (OB_FAIL(cal_target_shrink_range_(is_auto, lower_page_id, upper_page_id))) {
    LOG_WARN("fail to calculate target shrinking range", KR(ret), K(is_auto));
  } else if (OB_UNLIKELY(!is_valid_page_id_(lower_page_id) ||
                         !is_valid_page_id_(upper_page_id) ||
                          upper_page_id <= BLOCK_PAGE_NUMS)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid boundary page id", KR(ret),
        K(lower_page_id), K(upper_page_id), K(pages_.size()));
  } else if (OB_FAIL(shrink_ctx_.init(lower_page_id, upper_page_id, is_auto))) {
    LOG_WARN("wbp fail to init shrink context", KR(ret), K(lower_page_id), K(upper_page_id));
  } else {
    ATOMIC_SET(&is_flush_all_, true);
    LOG_INFO("init shrinking context", KR(ret), K(shrink_ctx_));
  }
  return ret;
}

int ObTmpFileWriteCache::finish_shrinking_()
{
  int ret = OB_SUCCESS;
  // use w-lock to avoid concurrency issues when checking if page is in_shrinking_range()
  common::TCRWLock::WLockGuard guard(resize_lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("write cache is not init", KR(ret));
  } else if (!shrink_ctx_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("shrink_ctx_ is invalid", KR(ret), K(shrink_ctx_));
  } else if (WriteCacheShrinkContext::SHRINKING_FINISH > shrink_ctx_.shrink_state_) {
    // shrink abort, concat shrink list to free list if needed
    ObSpinLockGuard shrink_guard(shrink_ctx_.shrink_lock_);
    LOG_INFO("write cache shrinking abort", K(shrink_ctx_), K(shrink_ctx_.shrink_list_.get_size()));
    while (!shrink_ctx_.shrink_list_.is_empty()) {
      PageNode *node = shrink_ctx_.shrink_list_.remove_first();
      int tmp_ret = OB_SUCCESS;
      if (OB_ISNULL(node)) {
        tmp_ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("node is nullptr", KR(tmp_ret), K(shrink_ctx_), K(shrink_ctx_.shrink_list_.get_size()));
      } else if (OB_TMP_FAIL(free_page_list_.push_back(node))) {
        LOG_ERROR("fail to push back free list", KR(tmp_ret), K(node->page_));
      }
    }
    shrink_ctx_.reset();
  } else {
    // normal finish
    int32_t max_page_num = get_memory_limit() / ObTmpFileGlobal::PAGE_SIZE;
    LOG_INFO("write cache shrinking finish gracefully", K(pages_.size()), K(max_page_num), K(shrink_ctx_));
    shrink_ctx_.reset();
  }
  ATOMIC_SET(&is_flush_all_, false);
  return ret;
}

int ObTmpFileWriteCache::release_shrink_range_blocks_()
{
  int ret = OB_SUCCESS;
  if (!shrink_ctx_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("shrink_ctx_ is invalid", K(ret), K(shrink_ctx_));
  } else if (OB_FAIL(purge_invalid_pages_())) {
    LOG_WARN("fail to remove invalid page in free list", KR(ret), K(shrink_ctx_));
  } else {
    common::TCRWLock::WLockGuard guard(resize_lock_);
    if (pages_.size() != shrink_ctx_.upper_page_id_ + 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("fat_ size is not equal to the size in shrink_ctx",
          KR(ret), K(shrink_ctx_), K(pages_.size()));
    } else if (OB_FAIL(release_blocks_reverse_(shrink_ctx_.upper_page_id_ - BLOCK_PAGE_NUMS + 1,
                                               shrink_ctx_.lower_page_id_))) {
      LOG_WARN("fail to release blocks", KR(ret), K(shrink_ctx_));
    }
    if (FAILEDx(ret)) {
      uint32_t used_page_cnt = ATOMIC_LOAD(&used_page_cnt_);
      int64_t free_page_cnt = free_page_list_.size();
      int64_t shrink_page_cnt = shrink_ctx_.shrink_list_.get_size();
      if (used_page_cnt + free_page_cnt + shrink_page_cnt != pages_.size()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("page count sum from lists does not match the total page count",
            KR(ret), K(used_page_cnt), K(free_page_cnt), K(shrink_page_cnt),
            K(shrink_ctx_), K(pages_.size()));
      }
    }
  }
  LOG_INFO("write cache shrinking release blocks finish", KR(ret), K(pages_.size()), K(shrink_ctx_));
  return ret;
}

int ObTmpFileWriteCache::purge_invalid_pages_()
{
  int ret = OB_SUCCESS;
  common::TCRWLock::RLockGuard guard(resize_lock_);
  WriteCacheShrinkContext::JudgeInShrinkRangeFunctor in_shrink_range(shrink_ctx_);
  ObTmpFileWriteCacheFreePageList::Function func = in_shrink_range;
  if (!shrink_ctx_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("shrink_ctx_ is invalid", K(ret), K(shrink_ctx_));
  }

  ObSpinLockGuard shrink_guard(shrink_ctx_.shrink_lock_);
  if (FAILEDx(free_page_list_.remove_if(func, shrink_ctx_.shrink_list_))) {
    LOG_WARN("fail to purge invalid pages", KR(ret), K(shrink_ctx_));
  }
  LOG_DEBUG("purge_invalid_pages_ complete", KR(ret), K(pages_.size()));
  return ret;
}

int ObTmpFileWriteCache::advance_shrink_state_()
{
  int ret = OB_SUCCESS;
  switch (shrink_ctx_.shrink_state_) {
    case WriteCacheShrinkContext::INVALID:
      shrink_ctx_.shrink_state_ = WriteCacheShrinkContext::SHRINKING_SWAP;
      break;
    case WriteCacheShrinkContext::SHRINKING_SWAP:
      shrink_ctx_.shrink_state_ = WriteCacheShrinkContext::SHRINKING_RELEASE_BLOCKS;
      break;
    case WriteCacheShrinkContext::SHRINKING_RELEASE_BLOCKS:
      shrink_ctx_.shrink_state_ = WriteCacheShrinkContext::SHRINKING_FINISH;
      break;
    case WriteCacheShrinkContext::SHRINKING_FINISH:
      // do nothing
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected wbp shrink state", KR(ret), K(shrink_ctx_));
      break;
  }
  return ret;
}

// Reduce max_allow_alloc_page_id_ until it falls below lower_page_id_,
// ensuring that no newly allocated pages exist within the range of [max_allow_alloc_page_id_, upper_page_id_].
// After all pages in [lower_page_id_, upper_page_id_] have been freed, we can release the blocks.
bool ObTmpFileWriteCache::is_shrink_range_all_free_()
{
  int ret = OB_SUCCESS;
  bool is_all_free = false;
  cal_max_allow_alloc_page_id_(shrink_ctx_);
  if (OB_UNLIKELY(!is_valid_page_id_(shrink_ctx_.lower_page_id_) ||
                  !is_valid_page_id_(shrink_ctx_.upper_page_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid page id", KR(ret), K(shrink_ctx_));
  } else if (shrink_ctx_.max_allow_alloc_page_id_ < shrink_ctx_.lower_page_id_) {
    is_all_free = true;
    for (uint32_t i = shrink_ctx_.lower_page_id_; i <= shrink_ctx_.upper_page_id_; ++i) {
      if (pages_[i].is_valid()) {
        is_all_free = false;
        LOG_DEBUG("wbp shrink range is not all free", K(i),
            K(page_map_.count()), K(ATOMIC_LOAD(&used_page_cnt_)), K(pages_[i]), K(shrink_ctx_));
        break;
      }
    }
  }

  if (is_all_free) {
    LOG_INFO("shrinking range is free, start to release blocks mem", K(pages_.size()), K(shrink_ctx_));
  }
  return is_all_free;
}

// Used for non-power-of-two alignment
int64_t ObTmpFileWriteCache::upper_align_(const int64_t input, const int64_t align)
{
  return (input + align - 1) / align * align;
}

int ObTmpFileWriteCache::cal_target_shrink_range_(
    const bool is_auto,
    int64_t &lower_page_id,
    int64_t &upper_page_id)
{
  int ret = OB_SUCCESS;
  int64_t capacity = get_current_capacity_();
  int64_t memory_limit = get_memory_limit();
  int64_t current_watermark = ATOMIC_LOAD(&shrink_ctx_.current_max_used_watermark_);
  int64_t exceed_page_num = max(0, (capacity - memory_limit) / ObTmpFileGlobal::PAGE_SIZE);

  if (is_auto) { // calculate target page id according to watermark when auto shrinking
    int64_t shrink_percent = WriteCacheShrinkContext::get_auto_shrinking_percent(current_watermark);
    int64_t target_page_id = shrink_percent * pages_.size() / 100;
    lower_page_id = upper_align_(target_page_id, BLOCK_PAGE_NUMS);
  } else {
    lower_page_id = upper_align_(pages_.count() - exceed_page_num, BLOCK_PAGE_NUMS);
  }

  lower_page_id = max(lower_page_id, BLOCK_PAGE_NUMS); // reserve at least one block
  upper_page_id = max(pages_.count() - 1, BLOCK_PAGE_NUMS);
  return ret;
}

// Based on the current max_allow_alloc_page_id, decrease by 25% each time
uint32_t ObTmpFileWriteCache::cal_max_allow_alloc_page_id_(WriteCacheShrinkContext &shrink_ctx)
{
  int64_t shrink_size = (shrink_ctx.upper_page_id_ - shrink_ctx.lower_page_id_) / 4;
  uint32_t max_allow_alloc_id =  MAX3(0,
      shrink_ctx.lower_page_id_ - 1,
      shrink_ctx.max_allow_alloc_page_id_ - shrink_size);
  shrink_ctx.max_allow_alloc_page_id_ = max_allow_alloc_id;
  return max_allow_alloc_id;
}

void ObTmpFileWriteCache::update_current_max_used_percent_()
{
  const int64_t used_page_cnt = ATOMIC_LOAD(&used_page_cnt_);
  const int64_t page_num = pages_.size();
  const int64_t current_used_percent = page_num > 0 ? used_page_cnt * 100 / page_num : 0;
  int64_t current_max_used_percent = ATOMIC_LOAD(&shrink_ctx_.current_max_used_watermark_);
  if (current_max_used_percent < current_used_percent) {
    // missing a few updates for shrink_ctx_.current_max_used_watermark_ is acceptable
    ATOMIC_CMP_AND_EXCHANGE(&shrink_ctx_.current_max_used_watermark_,
                            &current_max_used_percent,
                             current_used_percent);
  }
}

// release flush tasks that have not send I/O after the flush timer has stopped
// ATTENTION! this function must be called after TG_CANCEL is executed
int ObTmpFileWriteCache::remove_pending_task_()
{
  int ret = OB_SUCCESS;
  ObTmpFileFlushTask *task = nullptr;
  for (int64_t i = io_waiting_queue_size_; i > 0; --i) {
    if (OB_FAIL(pop_flush_task_(task))) {
      LOG_WARN("fail to pop flush task", KR(ret));
    } else if (OB_UNLIKELY(!task->is_written())) {
      LOG_INFO("flush task is not sending IO after flush timer stop", KPC(task));
      free_flush_task_(task);
    } else if (OB_FAIL(add_flush_task_(task))) {
      LOG_ERROR("fail to add flush task", KR(ret), KPC(task));
    }
  }
  return ret;
}

int ObTmpFileWriteCache::cleanup_list_()
{
  int ret = OB_SUCCESS;
  LOG_INFO("cleanup lists begin", K(swap_queue_size_), K(io_waiting_queue_size_));
  for (int32_t i = 0; i < MAX_FLUSH_TIMER_NUM; ++i) {
    TG_CANCEL_ALL(flush_tg_id_[i]);
  }

  while (!swap_queue_.is_empty()) {
    ObTmpFileSwapJob *swap_job = nullptr;
    if (OB_FAIL(pop_swap_job_(swap_job))) {
      LOG_WARN("fail to pop swap job", KR(ret), KPC(swap_job));
      ret = OB_SUCCESS;
    } else {
      swap_job->signal_swap_complete(OB_CANCELED);
    }
  }

  remove_pending_task_();

  for (int32_t retry = 10; io_waiting_queue_size_ > 0 && retry > 0; --retry) {
    LOG_WARN("io waiting queue is not empty", K(io_waiting_queue_size_));
    if (OB_FAIL(exec_wait_())) {
      if (OB_EAGAIN != ret) {
        LOG_ERROR("exec wait failed", KR(ret));
      }
      ret = OB_SUCCESS;
    }
    ob_usleep(10 * 1000);
  }

  return ret;
}

}  // end namespace tmp_file
}  // end namespace oceanbase
