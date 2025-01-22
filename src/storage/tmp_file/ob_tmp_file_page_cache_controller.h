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

#ifndef OCEANBASE_STORAGE_TMP_FILE_OB_TMP_FILE_PAGE_CACHE_CONTROLLER_H_
#define OCEANBASE_STORAGE_TMP_FILE_OB_TMP_FILE_PAGE_CACHE_CONTROLLER_H_

#include "storage/tmp_file/ob_tmp_file_thread_wrapper.h"
#include "storage/tmp_file/ob_tmp_file_flush_manager.h"

namespace oceanbase
{
namespace tmp_file
{
class ObTmpFilePageCacheController
{
public:
  ObTmpFilePageCacheController(ObTmpFileBlockManager &tmp_file_block_manager)
    : is_inited_(false),
      flush_all_data_(false),
      disk_usage_limit_(0),
      tmp_file_block_manager_(tmp_file_block_manager),
      task_allocator_(),
      write_buffer_pool_(),
      flush_priority_mgr_(),
      evict_mgr_(),
      flush_mgr_(*this),
      flush_tg_(write_buffer_pool_, flush_mgr_, task_allocator_, tmp_file_block_manager_),
      swap_tg_(write_buffer_pool_, evict_mgr_, flush_tg_, *this)
  {
  }
  ~ObTmpFilePageCacheController() {}
public:
  static const int64_t FLUSH_FAST_INTERVAL = 5;     // 5ms
  static const int64_t FLUSH_INTERVAL = 1000;       // 1s
  static const int64_t SWAP_FAST_INTERVAL = 5;      // 5ms
  static const int64_t SWAP_INTERVAL = 1000;        // 1s
  static const int64_t REFRESH_CONFIG_INTERVAL = 10 * 1000 * 1000;  // 10s
  static const int64_t ACCESS_TENANT_CONFIG_TIMEOUT_US = 10 * 1000; // 10ms
  virtual int init();
  int start();
  void stop();
  void wait();
  void destroy();
  ObIAllocator &get_task_allocator() { return task_allocator_; }
  ObTmpWriteBufferPool &get_write_buffer_pool() { return write_buffer_pool_; }
  ObTmpFileFlushManager &get_flush_task_mgr() { return flush_mgr_; }
  ObTmpFileEvictionManager &get_eviction_manager() { return evict_mgr_; }
  ObTmpFileFlushPriorityManager &get_flush_priority_mgr() { return flush_priority_mgr_; }
  ObTmpFileBlockManager &get_tmp_file_block_manager() { return tmp_file_block_manager_; }
  OB_INLINE bool is_flush_all_data() { return ATOMIC_LOAD(&flush_all_data_); }
  OB_INLINE void set_flush_all_data(bool flush_all_data) { ATOMIC_STORE(&flush_all_data_, flush_all_data); }
  OB_INLINE int64_t get_disk_usage_limit() const { return ATOMIC_LOAD(&disk_usage_limit_); }
  virtual int invoke_swap_and_wait(int64_t expect_swap_size, int64_t timeout_ms = ObTmpFileSwapJob::DEFAULT_TIMEOUT_MS);
  void refresh_disk_usage_limit();
private:
  int swap_job_enqueue_(ObTmpFileSwapJob *swap_job);
  int free_swap_job_(ObTmpFileSwapJob *swap_job);
  DISALLOW_COPY_AND_ASSIGN(ObTmpFilePageCacheController);
private:
  bool is_inited_;
  bool flush_all_data_;                   // set to true to flush all pages when shrinking write buffer pool
  int64_t disk_usage_limit_;              // periodically read disk usage limit from tenant config
  ObTmpFileBlockManager &tmp_file_block_manager_;    // ref to ObTmpFileBlockManager
  ObFIFOAllocator task_allocator_;        // used by flush_mgr_ to allocate flush tasks
  ObTmpWriteBufferPool write_buffer_pool_;
  ObTmpFileFlushPriorityManager flush_priority_mgr_;
  ObTmpFileEvictionManager evict_mgr_;    // maintain evict lists and evict pages from write buffer pool
  ObTmpFileFlushManager flush_mgr_;       // maintain flush lists and generate flush tasks
  ObTmpFileFlushTG flush_tg_;             // flush thread
  ObTmpFileSwapTG swap_tg_;               // swap thread
};

}  // end namespace tmp_file
}  // end namespace oceanbase
#endif // OCEANBASE_STORAGE_TMP_FILE_OB_TMP_FILE_PAGE_CACHE_CONTROLLER_H_
