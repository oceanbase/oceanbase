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

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_TMP_FILE_OB_TMP_FILE_TASK_MANAGER_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_TMP_FILE_OB_TMP_FILE_TASK_MANAGER_H_

#include "storage/tmp_file/ob_tmp_file_eviction_manager.h"
#include "storage/tmp_file/ob_tmp_file_global.h"
#include "storage/tmp_file/ob_tmp_file_flush_ctx.h"
#include "storage/tmp_file/ob_tmp_file_flush_priority_manager.h"
#include "storage/tmp_file/ob_tmp_file_thread_job.h"

namespace oceanbase
{
namespace tmp_file
{

// flush manager generates flush tasks according to dirty page watermark
class ObTmpFileFlushManager
{
public:
  static const int32_t FLUSH_WATERMARK_F1 = 50; // percentage
  static const int32_t FLUSH_WATERMARK_F2 = 70;
  static const int32_t FLUSH_WATERMARK_F3 = 75;
  static const int32_t FLUSH_WATERMARK_F4 = 80;
  static const int32_t FLUSH_WATERMARK_F5 = 90;
  static const int32_t FLUSH_LOW_WATERMARK_F1 = 25;
  static const int32_t FLUSH_LOW_WATERMARK_F2 = 60;
  static const int32_t FLUSH_LOW_WATERMARK_F3 = 65;
  static const int32_t FLUSH_LOW_WATERMARK_F4 = 70;
  static const int32_t FLUSH_LOW_WATERMARK_F5 = 80;
  static const int64_t FAST_FLUSH_TREE_PAGE_NUM = 32;
  struct UpdateFlushCtx
  {
  public:
    UpdateFlushCtx(ObTmpFileSingleFlushContext &file_ctx)
      : input_data_ctx_(file_ctx.data_ctx_),
        input_meta_ctx_(file_ctx.meta_ctx_) {}
    void operator() (hash::HashMapPair<int64_t, ObTmpFileSingleFlushContext> &pair);
  private:
    ObTmpFileDataFlushContext &input_data_ctx_;
    ObTmpFileTreeFlushContext &input_meta_ctx_;
  };
  struct ResetFlushCtxOp
  {
  public:
  public:
    ResetFlushCtxOp(const bool is_meta) : is_meta_(is_meta) {}
    void operator() (hash::HashMapPair<int64_t, ObTmpFileSingleFlushContext> &pair);
  private:
    bool is_meta_;
  };
public:
  typedef ObTmpFileGlobal::FlushCtxState FlushCtxState;
  typedef common::ObDList<ObSharedNothingTmpFile::ObTmpFileNode> ObTmpFileFlushList;
  typedef ObTmpFileFlushTask::ObTmpFileFlushTaskState FlushState;
  ObTmpFileFlushManager(ObTmpFilePageCacheController &pc_ctrl);
  ~ObTmpFileFlushManager() {}
  int init();
  void destroy();
  TO_STRING_KV(K(is_inited_), K(flush_ctx_));

public:
  int free_tmp_file_block(ObTmpFileFlushTask &flush_task);
  int alloc_flush_task(ObTmpFileFlushTask *&flush_task);
  int free_flush_task(ObTmpFileFlushTask *flush_task);
  int notify_write_back_failed(ObTmpFileFlushTask *flush_task);
  int flush(ObSpLinkQueue &flushing_queue,
            ObTmpFileFlushMonitor &flush_monitor,
            const int64_t expect_flush_size,
            const bool is_flush_meta_tree);
  int retry(ObTmpFileFlushTask &flush_task);
  int io_finished(ObTmpFileFlushTask &flush_task);
  int update_file_meta_after_flush(ObTmpFileFlushTask &flush_task);
  void try_remove_unused_file_flush_ctx();
private:
  int fill_block_buf_(ObTmpFileFlushTask &flush_task);
  int fast_fill_block_buf_with_meta_(ObTmpFileFlushTask &flush_task);
  int inner_fill_block_buf_(ObTmpFileFlushTask &flush_task,
                            const FlushCtxState flush_stage,
                            const bool is_meta,
                            const bool flush_tail);
  int insert_items_into_meta_tree_(ObTmpFileFlushTask &flush_task,
                                   const int64_t logic_block_index);
  void init_flush_level_();
  void advance_flush_level_(const int ret_code);
  void inner_advance_flush_level_();
  void inner_advance_flush_level_without_checking_watermark_();
  int64_t get_low_watermark_(FlushCtxState state);
  int advance_status_(ObTmpFileFlushTask &flush_task, const FlushState &state);
  int drive_flush_task_prepare_(ObTmpFileFlushTask &flush_task, const FlushState state, FlushState &next_state);
  int drive_flush_task_retry_(ObTmpFileFlushTask &flush_task, const FlushState state, FlushState &next_state);
  int drive_flush_task_wait_to_finish_(ObTmpFileFlushTask &flush_task, FlushState &next_state);
  int handle_alloc_flush_task_(const bool fast_flush_meta, ObTmpFileFlushTask *&flush_task);
  int handle_create_block_index_(ObTmpFileFlushTask &flush_task, FlushState &next_state);
  int handle_fill_block_buf_(ObTmpFileFlushTask &flush_task, FlushState &next_state);
  int handle_insert_meta_tree_(ObTmpFileFlushTask &flush_task, FlushState &next_state);
  int handle_async_write_(ObTmpFileFlushTask &flush_task, FlushState &next_state);
  int handle_wait_(ObTmpFileFlushTask &flush_task, FlushState &next_state);
  int handle_finish_(ObTmpFileFlushTask &flush_task);
private:
  int update_meta_data_after_flush_for_files_(ObTmpFileFlushTask &flush_task);
  int reset_flush_ctx_for_file_(const ObSharedNothingTmpFile *file, const bool is_meta);
  int get_or_create_file_in_ctx_(const int64_t fd, ObTmpFileSingleFlushContext &file_flush_ctx);
  int evict_pages_and_retry_insert_(ObTmpFileFlushTask &flush_task,
                                    ObTmpFileFlushInfo &flush_info,
                                    const int64_t logic_block_index);
  void try_remove_unused_flush_info_(ObTmpFileFlushTask &flush_task);
  DISALLOW_COPY_AND_ASSIGN(ObTmpFileFlushManager);
private:
  bool is_inited_;
  ObTmpFileBatchFlushContext flush_ctx_;
  ObTmpFilePageCacheController &pc_ctrl_;
  ObTmpFileBlockManager &tmp_file_block_mgr_;
  ObIAllocator &task_allocator_;              // ref to ObTmpFilePageCacheController::task_allocator_
  ObTmpWriteBufferPool &write_buffer_pool_;
  ObTmpFileEvictionManager &evict_mgr_;
  ObTmpFileFlushPriorityManager &flush_priority_mgr_;
};

}  // end namespace tmp_file
}  // end namespace oceanbase
#endif // OCEANBASE_STORAGE_BLOCKSSTABLE_TMP_FILE_OB_TMP_FILE_TASK_MANAGER_H_
