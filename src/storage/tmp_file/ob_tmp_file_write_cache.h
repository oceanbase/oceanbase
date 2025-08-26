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

#ifndef OCEANBASE_STORAGE_TMP_FILE_OB_TMP_FILE_WRITE_CACHE_H_
#define OCEANBASE_STORAGE_TMP_FILE_OB_TMP_FILE_WRITE_CACHE_H_

#include "lib/lock/ob_tc_rwlock.h"
#include "lib/lock/ob_bucket_lock.h"
#include "lib/queue/ob_link_queue.h"
#include "lib/allocator/ob_fifo_allocator.h"
#include "lib/thread/thread_mgr_interface.h"
#include "deps/oblib/src/lib/list/ob_list.h"
#include "storage/tmp_file/ob_tmp_file_flush_task.h"
#include "storage/tmp_file/ob_tmp_file_block_manager.h"
#include "storage/tmp_file/ob_tmp_file_write_cache_page.h"
#include "storage/tmp_file/ob_tmp_file_write_cache_metrics.h"
#include "storage/tmp_file/ob_tmp_file_write_cache_page_array.h"
#include "storage/tmp_file/ob_tmp_file_write_cache_shrink_ctx.h"
#include "storage/tmp_file/ob_tmp_file_write_cache_free_page_list.h"

namespace oceanbase
{
namespace tmp_file
{
class ObTmpFilePageId;
class ObTmpFileSwapJob;
class ObTmpFileFlushTask;
class ObTmpFileWriteCacheKey;
class ObTmpFilePageHandle;
class ObTmpFileBlockFlushIterator;

class ObTmpFileWriteCache : public lib::TGRunnable
{
  friend class ObTmpFilePage;
  friend class ObTmpFileFlushTask;
  friend class ObTmpFileBlock;
public:
  // 2MB - 24KB (keep WBP_BLOCK_SIZE smaller than OB_MALLOC_BIG_BLOCK_SIZE)
  static const int64_t WBP_BLOCK_SIZE = 2 * 1024 * 1024 - 24 * 1024;
  static const int64_t BLOCK_PAGE_NUMS =
      WBP_BLOCK_SIZE / ObTmpFileGlobal::PAGE_SIZE;  // 253 pages
  static const int64_t SWAP_FAST_INTERVAL = 5;      // 5ms
  static const int64_t SWAP_INTERVAL = 1000;        // 1s
  static const int64_t SWAP_PAGE_NUM_PER_BATCH = 16;
  static const int64_t REFRESH_CONFIG_INTERVAL = 10 * 1000 * 1000;  // 10s
  static const int64_t DEFAULT_MEMORY_LIMIT = 64 * WBP_BLOCK_SIZE;  // 126.5MB
  static const int64_t MAX_FLUSH_TIMER_NUM = 4;
  static const int64_t BUCKET_CNT = ObTmpFileWriteCacheFreePageList::MAX_FREE_LIST_NUM * 8;
  static const int64_t MAX_FLUSHING_DATA_SIZE = 400 * ObTmpFileGlobal::SN_BLOCK_SIZE;
  static const int64_t ACCESS_TENANT_CONFIG_TIMEOUT_US = 10 * 1000; // 10ms
  static const int64_t MAX_FLUSH_TASK_NUM_PER_BATCH = 1024;
  static const int64_t MAX_ITER_BLOCK_NUM_PER_BATCH = 1024;

  static const int64_t FLUSH_WATERMARK_L1 = 90;
  static const int64_t FLUSH_WATERMARK_L2 = 80;
  static const int64_t FLUSH_WATERMARK_L3 = 60;
  static const int64_t FLUSH_PERCENT_L1 = 60;
  static const int64_t FLUSH_PERCENT_L2 = 40;
  static const int64_t FLUSH_PERCENT_L3 = 20;
public:
  ObTmpFileWriteCache();
  ~ObTmpFileWriteCache();
  int init(ObTmpFileBlockManager *tmp_file_block_manager);
  int start();
  void stop();
  void wait();
  void destroy();
  void run1() override;
public:
  int alloc_page(const ObTmpFileWriteCacheKey &page_key,
                 const ObTmpFilePageId &page_id,
                 const int64_t timeout_ms,
                 ObTmpFilePageHandle &page_handle);
  int alloc_page(const ObTmpFileWriteCacheKey &page_key,
                 const ObTmpFilePageId &page_id,
                 const int64_t timeout_ms,
                 ObTmpFilePageHandle &page_handle,
                 bool &trigger_swap_page);
  int load_page(const ObTmpFilePageHandle &page_handle,
                const blocksstable::MacroBlockId &macro_id,
                const int64_t timeout_ms);
  int get_page(const ObTmpFileWriteCacheKey &page_key,
               ObTmpFilePageHandle &page_handle);
  int put_page(const ObTmpFilePageHandle &page_handle);
  int free_page(const ObTmpFileWriteCacheKey &page_key);

public:
  int unlock(const int64_t bucket_id);
  // for concurrent write
  int shared_lock(const int64_t bucket_id);
  // for flush
  int try_exclusive_lock(const int64_t bucket_id);
  int64_t get_memory_limit();
private:
  int swap_();
  int add_swap_job_(ObTmpFileSwapJob *swap_job);
  int pop_swap_job_(ObTmpFileSwapJob *&swap_job);
  int evict_(ObTmpFileFlushTask &task);
  int swap_page_(const int64_t timeout_ms);

  int flush_();
  int build_task_(const int64_t expect_flush_cnt,
                  ObTmpFileBlockFlushIterator &iterator,
                  ObIArray<ObTmpFileFlushTask *> &tasks,
                  ObIArray<ObTmpFileBlockHandle> &failed_blocks);
  int build_task_in_block_(
                  ObTmpFileBlockHandle &block_handle,
                  ObIArray<ObTmpFileFlushTask *> &tasks,
                  ObIArray<ObTmpFileBlockHandle> &failed_blocks,
                  int64_t &actual_flush_cnt);
  int collect_pages_(ObTmpFileBlockFlushingPageIterator &page_iterator,
                     ObTmpFileFlushTask &task,
                     ObTmpFileBlockHandle block_handle);
  int exec_wait_();
  int alloc_flush_task_(ObTmpFileFlushTask *&task);
  int free_flush_task_(ObTmpFileFlushTask *task);
  int add_flush_task_(ObTmpFileFlushTask *task);
  int pop_flush_task_(ObTmpFileFlushTask *&task);
  int remove_pending_task_();
  OB_INLINE int get_flush_ret_code_() const { return flush_ret_; }
  OB_INLINE void set_flush_ret_code_(const int ret_code) { flush_ret_ = ret_code; }
  int try_to_set_macro_block_idx_(ObTmpFileBlock &block);

  int add_free_page_(ObTmpFilePage *page);
  int expand_();
  int try_shrink_();
  int release_blocks_reverse_(const int64_t begin, const int64_t end);

  // only used by ObTmpFileBlock
  bool is_memory_sufficient_();
private:
  // shrinking
  bool is_shrink_required_(bool &is_auto);
  int begin_shrinking_(const bool is_auto);
  int release_shrink_range_blocks_();
  int finish_shrinking_();
  int advance_shrink_state_();
  void update_current_max_used_percent_();
  bool is_shrink_range_all_free_();
  int purge_invalid_pages_();
  int cal_target_shrink_range_(const bool is_auto,
                               int64_t &lower_page_id,
                               int64_t &upper_page_id);
  uint32_t cal_max_allow_alloc_page_id_(WriteCacheShrinkContext &shrink_ctx);
private:
  void print_();
  int cleanup_list_();
  int64_t get_current_capacity_();
  int64_t cal_idle_time_();
  int64_t cal_flush_page_cnt_();
  int64_t upper_align_(const int64_t input, const int64_t align);
  void refresh_disk_usage_limit_();
  int check_tmp_file_disk_usage_limit_(const int64_t flushing_size);
  OB_INLINE bool is_valid_page_id_(uint32_t page_id)
  {
    return page_id >= 0 && page_id < pages_.size();
  }
  OB_INLINE int64_t bucket_id_to_idx_(const int64_t bucket_id)
  {
    return bucket_id % BUCKET_CNT;
  }
private:
  ObTmpFileBlockManager *tmp_file_block_manager_;
  bool is_inited_;
  bool is_flush_all_;
  int tg_id_;
  int flush_ret_;
  uint32_t used_page_cnt_;
  int64_t memory_limit_;
  int64_t tenant_config_access_ts_;
  int64_t default_memory_limit_; // used in unit tests to forcibly set the memory limit
  int64_t disk_usage_limit_;

  int cur_timer_idx_;
  int flush_tg_id_[MAX_FLUSH_TIMER_NUM];
  int64_t swap_queue_size_;
  int64_t io_waiting_queue_size_;
  ObSpLinkQueue swap_queue_;
  ObSpLinkQueue io_waiting_queue_;
  ObTmpFileWriteCacheFreePageList free_page_list_;
  WriteCacheShrinkContext shrink_ctx_;
  ObThreadCond idle_cond_;
  ObTmpFileWriteCacheMetrics metrics_;

  // TODO: After supporting concurrent writes, ensure that pages with the same fd can be flushed simultaneously,
  ObBucketLock bucket_lock_;
  common::TCRWLock resize_lock_;
  ObTmpFilePageArray pages_;
  ObLinearHashMap<ObTmpFileWriteCacheKey, ObTmpFilePageHandle> page_map_;
  ObFIFOAllocator page_allocator_;
  ObFIFOAllocator block_allocator_;
  ObFIFOAllocator flush_allocator_;
  ObFIFOAllocator swap_allocator_;
};

}  // end namespace tmp_file
}  // end namespace oceanbase

#endif
