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

#ifndef OCEANBASE_STORAGE_TMP_FILE_OB_TMP_FILE_THREAD_WRAPPER_H_
#define OCEANBASE_STORAGE_TMP_FILE_OB_TMP_FILE_THREAD_WRAPPER_H_

#include "storage/tmp_file/ob_tmp_file_thread_job.h"
#include "storage/tmp_file/ob_tmp_file_write_buffer_pool.h"
#include "storage/tmp_file/ob_tmp_file_eviction_manager.h"
#include "storage/tmp_file/ob_tmp_file_global.h"
#include "storage/tmp_file/ob_tmp_file_flush_manager.h"
#include "lib/thread/thread_mgr_interface.h"

namespace oceanbase
{
namespace tmp_file
{
class ObSNTenantTmpFileManager;
class ObTmpFileFlushManager;

// When originally designed, ObTmpFileFlushTG was an independent thread. in order to reduce the
// number of threads, the thread of ObTmpFileFlushTG was driven by ObTmpFileSwapTG.
class ObTmpFileFlushTG
{
public:
  typedef ObTmpFileFlushTask::ObTmpFileFlushTaskState FlushState;
  enum RUNNING_MODE {
    INVALID = 0,
    NORMAL  = 1,
    FAST    = 2
  };
public:
  ObTmpFileFlushTG(ObTmpWriteBufferPool &wbp,
                   ObTmpFileFlushManager &flush_mgr,
                   ObIAllocator &allocator,
                   ObTmpFileBlockManager &tmp_file_block_mgr);
  int init();
  int start();
  void stop();
  void wait();
  void destroy();

  int try_work();
  void set_running_mode(const RUNNING_MODE mode);
  void notify_doing_flush();
  void signal_io_finish(int flush_io_finished_ret);
  int64_t get_flush_io_finished_ret();
  int64_t get_flush_io_finished_round();
  int64_t cal_idle_time();
  void clean_up_lists();
  TO_STRING_KV(K(is_inited_), K(mode_), K(last_flush_timestamp_), K(flush_io_finished_ret_), K(flush_io_finished_round_),
               K(flushing_block_num_), K(is_fast_flush_meta_), K(fast_flush_meta_task_cnt_),
               K(wait_list_size_), K(retry_list_size_), K(finished_list_size_),
               K(normal_loop_cnt_), K(normal_idle_loop_cnt_), K(fast_loop_cnt_), K(fast_idle_loop_cnt_),
               K(flush_mgr_));
private:
  int do_work_();
  int handle_generated_flush_tasks_(ObSpLinkQueue &flushing_list, int64_t &task_num);
  int wash_(const int64_t expect_flush_size, const RUNNING_MODE mode);
  int check_flush_task_io_finished_();
  int retry_fast_flush_meta_task_();
  int retry_task_();
  int special_flush_meta_tree_page_();
  void flush_fast_();
  void flush_normal_();
  int get_fast_flush_size_();
  int64_t get_flushing_block_num_threshold_();
  int push_wait_list_(ObTmpFileFlushTask *flush_task);
  int pop_wait_list_(ObTmpFileFlushTask *&flush_task);
  int push_retry_list_(ObTmpFileFlushTask *flush_task);
  int pop_retry_list_(ObTmpFileFlushTask *&flush_task);
  int push_finished_list_(ObTmpFileFlushTask *flush_task);
  int pop_finished_list_(ObTmpFileFlushTask *&flush_task);
  void flush_task_finished_(ObTmpFileFlushTask *flush_task);
private:
  bool is_inited_;
  RUNNING_MODE mode_;
  int64_t last_flush_timestamp_;
  int flush_io_finished_ret_;
  int64_t flush_io_finished_round_;

  int64_t flushing_block_num_;      // maintain it when ObTmpFileFlushTask is created and freed
  bool is_fast_flush_meta_;         // indicate thread is fast flushing meta page, no new flush tasks will be added and no tasks will be retried
  int64_t fast_flush_meta_task_cnt_;
  int64_t wait_list_size_;
  int64_t retry_list_size_;
  int64_t finished_list_size_;
  ObSpLinkQueue wait_list_;         // list for tasks that are waiting for IO finished
  ObSpLinkQueue retry_list_;        // list for tasks that need to be retried
  ObSpLinkQueue finished_list_;     // list for tasks which have finished IO and need to update file's meta
  ObTmpFileFlushMonitor flush_monitor_;
  ObTmpFileFlushManager &flush_mgr_;
  ObTmpWriteBufferPool &wbp_;
  ObTmpFileBlockManager &tmp_file_block_mgr_;

  int64_t normal_loop_cnt_;
  int64_t normal_idle_loop_cnt_;
  int64_t fast_loop_cnt_;
  int64_t fast_idle_loop_cnt_;

  int flush_timer_tg_id_[ObTmpFileGlobal::FLUSH_TIMER_CNT];
};

class ObTmpFileSwapTG : public lib::TGRunnable
{
public:
  ObTmpFileSwapTG(ObTmpWriteBufferPool &wbp,
                  ObTmpFileEvictionManager &elimination_mgr,
                  ObTmpFileFlushTG &flush_tg,
                  ObTmpFilePageCacheController &pc_ctrl);
  virtual int init();
  int start();
  void stop();
  void wait();
  void destroy();
  void run1() override;
  int64_t cal_idle_time();
  int swap();
  int swap_job_enqueue(ObTmpFileSwapJob *swap_job);
  int swap_job_dequeue(ObTmpFileSwapJob *&swap_job);
  // wake up swap thread to do work
  void notify_doing_swap();
  TO_STRING_KV(K(is_inited_), K(tg_id_), K(swap_job_num_), K(working_list_size_),
               K(flush_io_finished_round_), K(last_swap_timestamp_), K(has_set_stop()));
private:
  static const int64_t PAGE_SIZE = ObTmpFileGlobal::PAGE_SIZE;
  static const int64_t PROCCESS_JOB_NUM_PER_BATCH = 128;
  void clean_up_lists_();
  int do_work_();
  int swap_normal_();
  int swap_fast_();
  int push_working_job_(ObTmpFileSwapJob *swap_job);
  int push_working_job_front_(ObTmpFileSwapJob *swap_job);
  int pop_working_job_(ObTmpFileSwapJob *&swap_job);
  int calculate_swap_page_num_(const int64_t batch_size, int64_t &expect_swap_cnt);
  int wakeup_satisfied_jobs_(int64_t& wakeup_job_cnt);
  int wakeup_timeout_jobs_();
  void wakeup_all_jobs_(int ret_code);
  int shrink_wbp_if_needed_();
private:
  bool is_inited_;
  int tg_id_;
  ObThreadCond idle_cond_;
  int64_t last_swap_timestamp_;

  int64_t swap_job_num_;
  ObSpLinkQueue swap_job_list_;     // list for swap jobs to be processed
  int64_t working_list_size_;
  ObSpLinkQueue working_list_;      // list for swap jobs that are being processed

  ObTmpFileSwapMonitor swap_monitor_;

  ObTmpFileFlushTG &flush_tg_ref_;
  int64_t flush_io_finished_round_;

  ObTmpWriteBufferPool &wbp_;
  ObTmpFileEvictionManager &evict_mgr_;
  ObTmpFilePageCacheController &pc_ctrl_;
  ObSNTenantTmpFileManager *file_mgr_;
};

}  // end namespace tmp_file
}  // end namespace oceanbase
#endif // OCEANBASE_STORAGE_TMP_FILE_OB_TMP_FILE_THREAD_WRAPPER_H_
