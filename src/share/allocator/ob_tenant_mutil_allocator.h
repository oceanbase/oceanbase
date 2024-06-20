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

#ifndef _OB_SHARE_TENANT_MUTIL_ALLOCATOR_H_
#define _OB_SHARE_TENANT_MUTIL_ALLOCATOR_H_
#include "lib/alloc/alloc_struct.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/allocator/ob_block_alloc_mgr.h"
#include "lib/allocator/ob_slice_alloc.h"
#include "lib/allocator/ob_vslice_alloc.h"
#include "lib/queue/ob_link.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace palf
{
class LogIOFlushLogTask;
class LogHandleSubmitTask;
class LogIOTruncateLogTask;
class LogIOFlushMetaTask;
class LogIOTruncatePrefixBlocksTask;
class LogIOFlushMetaTask;
class LogIOFlashbackTask;
class LogIOPurgeThrottlingTask;
class FetchLogTask;
class LogFillCacheTask;
}
namespace logservice
{
class ObLogReplayTask;
}
namespace common
{
class ObTraceProfile;
// Interface for Clog module
class ObILogAllocator : public ObIAllocator
{
public:
  ObILogAllocator() : flying_log_task_(0), flying_log_handle_submit_task_(0), flying_meta_task_(0) {}
  virtual ~ObILogAllocator() {}

public:
  virtual void *alloc(const int64_t size) = 0;
  virtual void* alloc(const int64_t size, const ObMemAttr &attr) = 0;
  virtual void free(void *ptr) = 0;
  virtual void *ge_alloc(const int64_t size) = 0;
  virtual void ge_free(void *ptr) = 0;
  virtual const ObBlockAllocMgr &get_clog_blk_alloc_mgr() const = 0;
  virtual palf::LogHandleSubmitTask *alloc_log_handle_submit_task(const int64_t palf_id, const int64_t palf_epoch) = 0;
  virtual void free_log_handle_submit_task(palf::LogHandleSubmitTask *ptr) = 0;
  virtual palf::LogIOFlushLogTask *alloc_log_io_flush_log_task(const int64_t palf_id, const int64_t palf_epoch) = 0;
  virtual void free_log_io_flush_log_task(palf::LogIOFlushLogTask *ptr) = 0;
  virtual palf::LogIOTruncateLogTask *alloc_log_io_truncate_log_task(const int64_t palf_id, const int64_t palf_epoch) = 0;
  virtual void free_log_io_truncate_log_task(palf::LogIOTruncateLogTask *ptr) = 0;
  virtual palf::LogIOFlushMetaTask *alloc_log_io_flush_meta_task(const int64_t palf_id, const int64_t palf_epoch) = 0;
  virtual void free_log_io_flush_meta_task(palf::LogIOFlushMetaTask *ptr) = 0;
  virtual palf::LogIOTruncatePrefixBlocksTask *alloc_log_io_truncate_prefix_blocks_task(const int64_t palf_id, const int64_t palf_epoch) = 0;
  virtual void free_log_io_truncate_prefix_blocks_task(palf::LogIOTruncatePrefixBlocksTask *ptr) = 0;
  virtual palf::FetchLogTask *alloc_palf_fetch_log_task() = 0;
  virtual void free_palf_fetch_log_task(palf::FetchLogTask *ptr) = 0;
  virtual void *alloc_replay_task(const int64_t size) = 0;
  virtual void *alloc_replay_log_buf(const int64_t size) = 0;
  virtual void free_replay_task(logservice::ObLogReplayTask *ptr) = 0;
  virtual void free_replay_log_buf(void *ptr) = 0;
  virtual palf::LogIOFlashbackTask *alloc_log_io_flashback_task(const int64_t palf_id, const int64_t palf_epoch) = 0;
  virtual void free_log_io_flashback_task(palf::LogIOFlashbackTask *ptr) = 0;
  virtual palf::LogIOPurgeThrottlingTask *alloc_log_io_purge_throttling_task(const int64_t palf_id, const int64_t palf_epoch) = 0;
  virtual void free_log_io_purge_throttling_task(palf::LogIOPurgeThrottlingTask *ptr) = 0;
  virtual palf::LogFillCacheTask *alloc_log_fill_cache_task(const int64_t palf_id, const int64_t palf_epoch) = 0;
  virtual void free_log_fill_cache_task(palf::LogFillCacheTask *ptr) = 0;
  virtual void *alloc_append_compression_buf(const int64_t size) = 0;
  virtual void free_append_compression_buf(void *ptr) = 0;
  virtual void *alloc_replay_decompression_buf(const int64_t size) = 0;
  virtual void free_replay_decompression_buf(void *ptr) = 0;
  virtual ObIAllocator *get_replay_decompression_allocator() = 0;
  TO_STRING_KV(K_(flying_log_task), K_(flying_meta_task));


protected:
  int64_t flying_log_task_;
  int64_t flying_log_handle_submit_task_;
  int64_t flying_meta_task_;
};

class ObTenantMutilAllocator
    : public ObILogAllocator, public common::ObLink
{
public:
  // The memory percent of clog
  const int64_t CLOG_MEM_LIMIT_PERCENT = 30;
  // The memory percent of replay engine
  const int64_t REPLAY_MEM_LIMIT_PERCENT = 5;
  // The memory limit of replay engine
  const int64_t REPLAY_MEM_LIMIT_THRESHOLD = 512 * 1024 * 1024ll;
  // The memory percent of clog compression
  const int64_t CLOG_COMPRESSION_MEM_LIMIT_PERCENT = 3;
  // The memory limit of clog compression
  const int64_t CLOG_COMPRESSION_MEM_LIMIT_THRESHOLD = 128 * 1024 * 1024L;

  // The memory percent of replay engine for inner_table
  static int choose_blk_size(int obj_size);

public:
  explicit ObTenantMutilAllocator(uint64_t tenant_id);
  ~ObTenantMutilAllocator();
  void destroy();
  // update nway when tenant's max_cpu changed
  void set_nway(const int32_t nway);
  // update limit when tenant's memory_limit changed
  void set_limit(const int64_t total_limit);
  int64_t get_limit() const;
  int64_t get_hold() const;
  uint64_t get_tenant_id() const { return tenant_id_; }
  inline ObTenantMutilAllocator *&get_next()
  {
    return reinterpret_cast<ObTenantMutilAllocator*&>(next_);
  }
  void try_purge();
  void *alloc(const int64_t size);
  void* alloc(const int64_t size, const ObMemAttr &attr);
  void free(void *ptr);
  void *ge_alloc(const int64_t size);
  void ge_free(void *ptr);
  const ObBlockAllocMgr &get_clog_blk_alloc_mgr() const;
  // V4.0
  palf::LogIOFlushLogTask *alloc_log_io_flush_log_task(const int64_t palf_id, const int64_t palf_epoch);
  void free_log_io_flush_log_task(palf::LogIOFlushLogTask *ptr);
  palf::LogHandleSubmitTask *alloc_log_handle_submit_task(const int64_t palf_id, const int64_t palf_epoch);
  void free_log_handle_submit_task(palf::LogHandleSubmitTask *ptr);
  palf::LogIOTruncateLogTask *alloc_log_io_truncate_log_task(const int64_t palf_id, const int64_t palf_epoch);
  void free_log_io_truncate_log_task(palf::LogIOTruncateLogTask *ptr);
  palf::LogIOFlushMetaTask *alloc_log_io_flush_meta_task(const int64_t palf_id, const int64_t palf_epoch);
  void free_log_io_flush_meta_task(palf::LogIOFlushMetaTask *ptr);
  palf::LogIOTruncatePrefixBlocksTask *alloc_log_io_truncate_prefix_blocks_task(const int64_t palf_id, const int64_t palf_epoch);
  void free_log_io_truncate_prefix_blocks_task(palf::LogIOTruncatePrefixBlocksTask *ptr);
  palf::FetchLogTask *alloc_palf_fetch_log_task();
  void free_palf_fetch_log_task(palf::FetchLogTask *ptr);
  void *alloc_replay_task(const int64_t size);
  void *alloc_replay_log_buf(const int64_t size);
  void free_replay_task(logservice::ObLogReplayTask *ptr);
  void free_replay_log_buf(void *ptr);
  palf::LogIOFlashbackTask *alloc_log_io_flashback_task(const int64_t palf_id, const int64_t palf_epoch);
  void free_log_io_flashback_task(palf::LogIOFlashbackTask *ptr);
  palf::LogIOPurgeThrottlingTask *alloc_log_io_purge_throttling_task(const int64_t palf_id, const int64_t palf_epoch);
  void free_log_io_purge_throttling_task(palf::LogIOPurgeThrottlingTask *ptr);
  palf::LogFillCacheTask *alloc_log_fill_cache_task(const int64_t palf_id, const int64_t palf_epoch);
  void free_log_fill_cache_task(palf::LogFillCacheTask *ptr);

  void *alloc_append_compression_buf(const int64_t size);
  void free_append_compression_buf(void *ptr);
  //alloc buf from replay_log_task_alloc
  void *alloc_replay_decompression_buf(const int64_t size);
  void free_replay_decompression_buf(void *ptr);
  ObIAllocator *get_replay_decompression_allocator() {return &replay_log_task_alloc_;}

private:
  uint64_t tenant_id_ CACHE_ALIGNED;
  int64_t total_limit_;
  int64_t pending_replay_mutator_size_;
  const int LOG_HANDLE_SUBMIT_TASK_SIZE;
  const int LOG_IO_FLUSH_LOG_TASK_SIZE;
  const int LOG_IO_TRUNCATE_LOG_TASK_SIZE;
  const int LOG_IO_FLUSH_META_TASK_SIZE;
  const int LOG_IO_TRUNCATE_PREFIX_BLOCKS_TASK_SIZE;
  const int PALF_FETCH_LOG_TASK_SIZE;
  const int LOG_IO_FLASHBACK_TASK_SIZE;
  const int LOG_IO_PURGE_THROTTLING_TASK_SIZE;
  const int LOG_FILL_CACHE_TASK_SIZE;
  ObBlockAllocMgr clog_blk_alloc_;
  ObBlockAllocMgr replay_log_task_blk_alloc_;
  ObBlockAllocMgr clog_compressing_blk_alloc_;
  ObVSliceAlloc clog_ge_alloc_;
  ObSliceAlloc log_handle_submit_task_alloc_;
  ObSliceAlloc log_io_flush_log_task_alloc_;
  ObSliceAlloc log_io_truncate_log_task_alloc_;
  ObSliceAlloc log_io_flush_meta_task_alloc_;
  ObSliceAlloc log_io_truncate_prefix_blocks_task_alloc_;
  ObSliceAlloc palf_fetch_log_task_alloc_;
  ObVSliceAlloc replay_log_task_alloc_;
  ObSliceAlloc log_io_flashback_task_alloc_;
  ObSliceAlloc log_io_purge_throttling_task_alloc_;
  ObSliceAlloc log_fill_cache_task_alloc_;
  ObVSliceAlloc clog_compression_buf_alloc_;
};

} // end of namespace common
} // end of namespace oceanbase

#endif /* _OB_SHARE_TENANT_MUTIL_ALLOCATOR_H_ */
