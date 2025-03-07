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

#include "ob_tmp_file_thread_wrapper.h"
#include "storage/meta_store/ob_server_storage_meta_service.h"
#include "storage/tmp_file/ob_sn_tmp_file_manager.h"

namespace oceanbase
{
namespace tmp_file
{

ObTmpFileFlushTG::ObTmpFileFlushTG(
    ObTmpWriteBufferPool &wbp,
    ObTmpFileFlushManager &flush_mgr,
    ObIAllocator &allocator,
    ObTmpFileBlockManager &tmp_file_block_mgr)
  : is_inited_(false),
    mode_(RUNNING_MODE::INVALID),
    last_flush_timestamp_(0),
    flush_io_finished_ret_(OB_SUCCESS),
    flush_io_finished_round_(0),
    flushing_block_num_(0),
    is_fast_flush_meta_(false),
    fast_flush_meta_task_cnt_(0),
    wait_list_size_(0),
    retry_list_size_(0),
    finished_list_size_(0),
    wait_list_(),
    retry_list_(),
    finished_list_(),
    flush_monitor_(),
    flush_mgr_(flush_mgr),
    wbp_(wbp),
    tmp_file_block_mgr_(tmp_file_block_mgr),
    normal_loop_cnt_(0),
    normal_idle_loop_cnt_(0),
    fast_loop_cnt_(0),
    fast_idle_loop_cnt_(0)
{
  for (int32_t i = 0; i < ObTmpFileGlobal::FLUSH_TIMER_CNT; ++i) {
    flush_timer_tg_id_[i] = -1;
  }
}

int ObTmpFileFlushTG::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObTmpFileSwapTG init twice");
  } else {
    for (int32_t i = 0; OB_SUCC(ret) && i < ObTmpFileGlobal::FLUSH_TIMER_CNT; ++i) {
      if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::TmpFileFlush, flush_timer_tg_id_[i]))) {
        STORAGE_LOG(WARN, "fail to create flush timer thread", KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      is_inited_ = true;
      mode_ = RUNNING_MODE::NORMAL;
      last_flush_timestamp_ = 0;
      flush_io_finished_ret_ = OB_SUCCESS;
      flush_io_finished_round_ = 0;
      flushing_block_num_ = 0;

      fast_flush_meta_task_cnt_ = 0;
      wait_list_size_ = 0;
      retry_list_size_ = 0;
      finished_list_size_ = 0;

      normal_loop_cnt_ = 0;
      normal_idle_loop_cnt_ = 0;
      fast_loop_cnt_ = 0;
      fast_idle_loop_cnt_ = 0;
    }
  }

  if (OB_FAIL(ret)) {
    destroy();
  }
  return ret;
}

int ObTmpFileFlushTG::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpFileSwapTG not init", KR(ret));
  } else {
    for (int32_t i = 0; OB_SUCC(ret) && i < ObTmpFileGlobal::FLUSH_TIMER_CNT; ++i) {
      if (OB_FAIL(TG_START(flush_timer_tg_id_[i]))) {
        LOG_WARN("TG_START flush_timer_tg_id_ failed", KR(ret), K(flush_timer_tg_id_[i]));
      }
    }

    if (OB_SUCC(ret)) {
      flush_mgr_.set_flush_timer_tg_id(flush_timer_tg_id_, ObTmpFileGlobal::FLUSH_TIMER_CNT);
    }
  }
  return ret;
}

void ObTmpFileFlushTG::stop()
{
  for (int32_t i = 0; i < ObTmpFileGlobal::FLUSH_TIMER_CNT; ++i) {
    TG_STOP(flush_timer_tg_id_[i]);
  }
}

void ObTmpFileFlushTG::wait()
{
  for (int32_t i = 0; i < ObTmpFileGlobal::FLUSH_TIMER_CNT; ++i) {
    TG_WAIT(flush_timer_tg_id_[i]);
  }
}

void ObTmpFileFlushTG::destroy()
{
  clean_up_lists();
  mode_ = RUNNING_MODE::INVALID;
  last_flush_timestamp_ = 0;
  flush_io_finished_ret_ = OB_SUCCESS;
  flush_io_finished_round_ = 0;
  flushing_block_num_ = 0;

  is_fast_flush_meta_ = false;
  fast_flush_meta_task_cnt_ = 0;
  wait_list_size_ = 0;
  retry_list_size_ = 0;
  finished_list_size_ = 0;

  normal_loop_cnt_ = 0;
  normal_idle_loop_cnt_ = 0;
  fast_loop_cnt_ = 0;
  fast_idle_loop_cnt_ = 0;

  is_inited_ = false;
  for (int32_t i = 0; i < ObTmpFileGlobal::FLUSH_TIMER_CNT; ++i) {
    if (-1 != flush_timer_tg_id_[i]) {
      TG_DESTROY(flush_timer_tg_id_[i]);
      flush_timer_tg_id_[i] = -1;
    }
  }
}

void ObTmpFileFlushTG::clean_up_lists()
{
  int ret = OB_SUCCESS;
  while (!retry_list_.is_empty()) {
    ObTmpFileFlushTask *flush_task = nullptr;
    pop_retry_list_(flush_task);
    if (OB_ISNULL(flush_task)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "flush task is null", KR(ret));
    } else {
      STORAGE_LOG(INFO, "free flush task in retry_list_", KPC(flush_task));
      flush_mgr_.free_flush_task(flush_task);
    }
  }

  while (!finished_list_.is_empty()) {
    ObTmpFileFlushTask *flush_task = nullptr;
    pop_finished_list_(flush_task);
    if (OB_ISNULL(flush_task)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "flush task is null", KR(ret));
    } else {
      STORAGE_LOG(INFO, "free flush task in finished_list_", KPC(flush_task));
      flush_mgr_.free_flush_task(flush_task);
    }
  }

  while (!wait_list_.is_empty()) { // clean up tasks after IO complete
    STORAGE_LOG(WARN, "wait_list_ is not empty after flush thread stop", K(wait_list_size_));
    for (int64_t cnt = 0; cnt < wait_list_size_ && !wait_list_.is_empty(); ++cnt) {
      ObTmpFileFlushTask *flush_task = nullptr;
      pop_wait_list_(flush_task);
      if (OB_ISNULL(flush_task)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "flush task is null", K(cnt), K(wait_list_size_));
      } else if (OB_FAIL(flush_task->wait_macro_block_handle())) {
        if (OB_EAGAIN == ret) {
          push_wait_list_(flush_task);
          ret = OB_SUCCESS;
        } else {
          STORAGE_LOG(ERROR, "unexpected error in waiting flush task finished", KR(ret), KPC(this));
        }
      } else if (!flush_task->atomic_get_io_finished()) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "unexpected flush task state", KR(ret), KPC(flush_task));
      } else if (OB_FAIL(flush_mgr_.notify_write_back_failed(flush_task))) {
        STORAGE_LOG(ERROR, "fail to notify_write_back_failed", KR(ret), KPC(flush_task));
      } else {
        STORAGE_LOG(DEBUG, "free flush task in wait_list_", KPC(flush_task));
        flush_mgr_.free_flush_task(flush_task);
      }
    }
    usleep(10 * 1000);  // 10ms
  }
}

void ObTmpFileFlushTG::set_running_mode(const RUNNING_MODE mode)
{
  mode_ = mode;
}

void ObTmpFileFlushTG::notify_doing_flush()
{
  last_flush_timestamp_ = 0;
}

void ObTmpFileFlushTG::signal_io_finish(int flush_io_finished_ret)
{
  flush_io_finished_ret_ = flush_io_finished_ret;
  ++flush_io_finished_round_;
}

int64_t ObTmpFileFlushTG::get_flush_io_finished_round()
{
  return flush_io_finished_round_;
}

int64_t ObTmpFileFlushTG::get_flush_io_finished_ret()
{
  return flush_io_finished_ret_;
}

int64_t ObTmpFileFlushTG::cal_idle_time()
{
  int64_t idle_time = 0;
  int64_t dirty_page_percentage = wbp_.get_dirty_page_percentage();
  if (OB_UNLIKELY(!SERVER_STORAGE_META_SERVICE.is_started())) {
    idle_time = ObTmpFilePageCacheController::FLUSH_INTERVAL;
  } else if (!wait_list_.is_empty() || !retry_list_.is_empty() || !finished_list_.is_empty()
      || ObTmpFileFlushManager::FLUSH_WATERMARK_F1 <= dirty_page_percentage) {
    idle_time = ObTmpFilePageCacheController::FLUSH_FAST_INTERVAL;
  } else if (RUNNING_MODE::FAST == mode_) {
    int64_t flushing_block_num = ATOMIC_LOAD(&flushing_block_num_);
    if (flushing_block_num >= get_flushing_block_num_threshold_()) {
      idle_time = ObTmpFilePageCacheController::FLUSH_FAST_INTERVAL;
    } else {
      idle_time = 0;
    }
  } else {
    idle_time = ObTmpFilePageCacheController::FLUSH_INTERVAL;
  }
  return idle_time;
}

int ObTmpFileFlushTG::try_work()
{
  int ret = OB_SUCCESS;

  int64_t cur_time = ObTimeUtility::current_monotonic_time();
  if (0 == last_flush_timestamp_ || cur_time - last_flush_timestamp_ >= cal_idle_time() * 1000) {
    if (OB_UNLIKELY(!SERVER_STORAGE_META_SERVICE.is_started())) {
      ret = OB_NOT_RUNNING;
      LOG_INFO("ObTmpFileFlushTG does not work before server slog replay finished",
          KR(ret), KPC(this));
    } else if (OB_FAIL(do_work_())) {
      STORAGE_LOG(WARN, "fail do flush", KR(ret), KPC(this));
    }
    last_flush_timestamp_ = ObTimeUtility::current_monotonic_time();
  }

  return ret;
}

int ObTmpFileFlushTG::do_work_()
{
  int ret = OB_SUCCESS;

  if (is_fast_flush_meta_) {
    check_flush_task_io_finished_();
    retry_fast_flush_meta_task_();
  } else {
    if (RUNNING_MODE::FAST == mode_) {
      flush_fast_();
    } else if (RUNNING_MODE::NORMAL == mode_) {
      flush_normal_();
    } else {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unknown running mode", KR(ret), K(mode_));
    }
  }

  flush_mgr_.try_remove_unused_file_flush_ctx();

  if (TC_REACH_TIME_INTERVAL(ObTmpFileGlobal::TMP_FILE_STAT_FREQUENCY)) {
    tmp_file_block_mgr_.print_block_usage();
    flush_monitor_.print_statistics();
    wbp_.print_statistics();
    STORAGE_LOG(INFO, "ObTmpFileFlushTG information", KPC(this));
    normal_loop_cnt_ = 0;
    normal_idle_loop_cnt_ = 0;
    fast_loop_cnt_ = 0;
    fast_idle_loop_cnt_ = 0;
  }
  return ret;
}

// 1. check wait_list_ for IO complete task; 2. retry old task if exists; 3. build new task if needed
void ObTmpFileFlushTG::flush_fast_()
{
  int ret = OB_SUCCESS;
  int64_t BLOCK_SIZE = ObTmpFileGlobal::SN_BLOCK_SIZE;

  if (OB_FAIL(check_flush_task_io_finished_())) {
    STORAGE_LOG(WARN, "fail to check flush task io finished", KR(ret));
  }
  if (OB_FAIL(retry_task_())) {
    STORAGE_LOG(WARN, "fail to retry task", KR(ret));
  }

  int64_t flushing_block_num = ATOMIC_LOAD(&flushing_block_num_);
  if (flushing_block_num >= get_flushing_block_num_threshold_()) {
    STORAGE_LOG(DEBUG, "reach flushing block num threshold, skip flush", KPC(this));
  } else {
    int64_t max_flushing_block_num_cur_round = get_flushing_block_num_threshold_() - flushing_block_num;
    int64_t flush_size = min(get_fast_flush_size_(), max_flushing_block_num_cur_round * BLOCK_SIZE);
    if (flush_size > 0) {
      if (OB_FAIL(wash_(flush_size, RUNNING_MODE::FAST))) {
        STORAGE_LOG(WARN, "fail to flush fast", KR(ret), KPC(this), K(flush_size));
      }
    } else {
      STORAGE_LOG(DEBUG, "current expect flush size is 0, skip flush", K(flush_size), KPC(this));
    }
  }
}

void ObTmpFileFlushTG::flush_normal_()
{
  int ret = OB_SUCCESS;
  int64_t BLOCK_SIZE = ObTmpFileGlobal::SN_BLOCK_SIZE;
  int64_t normal_flush_size = max(0, (ObTmpFileGlobal::MAX_FLUSHING_BLOCK_NUM -
                                      ATOMIC_LOAD(&flushing_block_num_)) * BLOCK_SIZE);
  if (OB_FAIL(check_flush_task_io_finished_())) {
    STORAGE_LOG(WARN, "fail to check flush task io finished", KR(ret));
  }
  if (OB_FAIL(retry_task_())) {
    STORAGE_LOG(WARN, "fail to retry task", KR(ret));
  }
  if (normal_flush_size > 0) {
    if (OB_FAIL(wash_(normal_flush_size, RUNNING_MODE::NORMAL))) {
      STORAGE_LOG(WARN, "fail to flush normal", KR(ret), KPC(this));
    }
  } else {
    STORAGE_LOG(DEBUG, "current expect flush size is 0, skip flush", K(normal_flush_size), K(this));
  }
}

int ObTmpFileFlushTG::handle_generated_flush_tasks_(ObSpLinkQueue &flushing_list, int64_t &task_num)
{
  int ret = OB_SUCCESS;
  task_num = 0;
  while (!flushing_list.is_empty()) { // ignore error code to handle all flush tasks
    task_num += 1;
    ObSpLinkQueue::Link *link = nullptr;
    flushing_list.pop(link);
    if (OB_ISNULL(link)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "flush task is null", KR(ret));
    } else {
      ObTmpFileFlushTask *flush_task = static_cast<ObTmpFileFlushTask *>(link);
      FlushState state = flush_task->get_state();
      bool need_release_resource = false;
      STORAGE_LOG(DEBUG, "check flush task state", KPC(flush_task));
      if (FlushState::TFFT_WAIT == state) {
        ATOMIC_INC(&flushing_block_num_);
        push_wait_list_(flush_task);
        if (flush_task->get_is_fast_flush_tree()) {
          // after setting this flag, the thread will only process tasks that are already in wait_list_ and finished_list_,
          // and will not process tasks in retry_list_ or generate new flush tasks
          is_fast_flush_meta_ = true;
          fast_flush_meta_task_cnt_ += 1;
        }
      } else if (FlushState::TFFT_FILL_BLOCK_BUF > state) {
        need_release_resource = true;
      } else if (FlushState::TFFT_FILL_BLOCK_BUF == state && 0 == flush_task->get_data_length()) {
        need_release_resource = true;
      } else if (FlushState::TFFT_FILL_BLOCK_BUF == state && 0 < flush_task->get_data_length()) {
        // ATTENTION! after the data has been copied, it should transition to the next state.
        // staying in the TFFT_FILL_BLOCK_BUF is an abnormal behavior and the file should be discarded,
        // do not free task here to avoid further corruption.
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("fail to switch state after data copied from tmp file", KR(ret), KPC(flush_task));
      } else if (FlushState::TFFT_FILL_BLOCK_BUF < state) {
        if (FlushState::TFFT_ASYNC_WRITE == state &&
            flush_task->atomic_get_write_block_ret_code() == OB_SERVER_OUTOF_DISK_SPACE) {
          signal_io_finish(OB_SERVER_OUTOF_DISK_SPACE);
        }
        push_retry_list_(flush_task);
        ATOMIC_INC(&flushing_block_num_);
        STORAGE_LOG(DEBUG, "push flush task to retry list", KPC(flush_task));
      }

      if (need_release_resource) {
        if (ObTmpFileGlobal::INVALID_TMP_FILE_BLOCK_INDEX != flush_task->get_block_index()) {
          flush_mgr_.free_tmp_file_block(*flush_task);
        }
        flush_mgr_.free_flush_task(flush_task);
      }
    }
  } // end while
  return ret;
}

int ObTmpFileFlushTG::wash_(const int64_t expect_flush_size, const RUNNING_MODE mode)
{
  int ret = OB_SUCCESS;
  int64_t flushing_task_cnt = 0;
  int64_t current_flush_cnt = ATOMIC_LOAD(&flushing_block_num_);
  ObSpLinkQueue flushing_list;

  if (OB_FAIL(flush_mgr_.flush(flushing_list, flush_monitor_, expect_flush_size,
                               current_flush_cnt, is_fast_flush_meta_))) {
    if (OB_TMP_FILE_EXCEED_DISK_QUOTA == ret) {
      signal_io_finish(ret);
    }
  }

  if (!flushing_list.is_empty()) { // ignore ret
    if (OB_FAIL(handle_generated_flush_tasks_(flushing_list, flushing_task_cnt))) {
      STORAGE_LOG(WARN, "fail to handle generated flush tasks", KR(ret), K(flushing_task_cnt), KPC(this));
    }
  }

  bool idle_loop = flushing_task_cnt == 0;
  if (idle_loop && wbp_.get_cannot_be_evicted_page_percentage() < ObTmpFileFlushManager::FLUSH_WATERMARK_F3) {
    signal_io_finish(OB_SUCCESS);
  }

  if (RUNNING_MODE::NORMAL == mode) {
    ++normal_loop_cnt_;
    if (idle_loop) {
      ++normal_idle_loop_cnt_;
    }
  } else if (RUNNING_MODE::FAST == mode) {
    ++fast_loop_cnt_;
    if (idle_loop) {
      ++fast_idle_loop_cnt_;
    }
  }
  return ret;
}

int ObTmpFileFlushTG::retry_fast_flush_meta_task_()
{
  int ret = OB_SUCCESS;
  for (int64_t cnt = retry_list_size_; cnt > 0 && !retry_list_.is_empty(); --cnt) {
    ObTmpFileFlushTask *flush_task = nullptr;
    pop_retry_list_(flush_task);
    if (OB_ISNULL(flush_task)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "flush task is nullptr", KR(ret));
    } else if (!flush_task->get_is_fast_flush_tree()) {
      push_retry_list_(flush_task);
    } else {
      // only retry is_fast_flush_tree tasks
      STORAGE_LOG(DEBUG, "retry is_fast_flush_tree flush task", KPC(flush_task));
      if (OB_FAIL(flush_mgr_.retry(*flush_task))) {
        STORAGE_LOG(WARN, "fail to retry flush task", KR(ret), KPC(flush_task));
      }

      FlushState state = flush_task->get_state();
      if (FlushState::TFFT_WAIT == state) {
        push_wait_list_(flush_task);
      } else if (FlushState::TFFT_FILL_BLOCK_BUF < state) {
        push_retry_list_(flush_task);
      } else if (FlushState::TFFT_FILL_BLOCK_BUF >= state) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("unexpected flush task status in retry phase", KR(ret), KPC(flush_task));
      }
    }
  }
  return ret;
}

int ObTmpFileFlushTG::retry_task_()
{
  int ret = OB_SUCCESS;
  for (int64_t cnt = retry_list_size_; cnt > 0 && !retry_list_.is_empty(); --cnt) {
    ObTmpFileFlushTask *flush_task = nullptr;
    pop_retry_list_(flush_task);
    if (OB_ISNULL(flush_task)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "flush task is nullptr", KR(ret));
    } else {
      STORAGE_LOG(DEBUG, "retry flush task", KPC(flush_task));
      if (OB_FAIL(flush_mgr_.retry(*flush_task))) {
        STORAGE_LOG(WARN, "fail to retry flush task", KR(ret), KPC(flush_task));
      }
      // push task into wait_list_/retry_list_ according to task state, ignore error code
      FlushState state = flush_task->get_state();
      if (FlushState::TFFT_ABORT == state) {
        STORAGE_LOG(INFO, "free abort flush task", KPC(flush_task));
        flush_task_finished_(flush_task);
      } else if (FlushState::TFFT_WAIT == state) {
        push_wait_list_(flush_task);
      } else if (FlushState::TFFT_FILL_BLOCK_BUF < state) {
        if (FlushState::TFFT_ASYNC_WRITE == state &&
            flush_task->atomic_get_write_block_ret_code() == OB_SERVER_OUTOF_DISK_SPACE) {
          signal_io_finish(OB_SERVER_OUTOF_DISK_SPACE);
        }
        push_retry_list_(flush_task);
        if (FlushState::TFFT_INSERT_META_TREE == state && OB_ALLOCATE_TMP_FILE_PAGE_FAILED == ret) {
          STORAGE_LOG(WARN, "fail to retry insert meta item in TFFT_INSERT_META_TREE", KPC(flush_task));
          if (OB_FAIL(special_flush_meta_tree_page_())) {
            STORAGE_LOG(WARN, "fail to flush meta tree page", KR(ret));
          }
          break;
        }
      } else if (FlushState::TFFT_FILL_BLOCK_BUF >= state) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("unexpected flush task status in retry phase", KR(ret), KPC(flush_task));
      }
    }
  }
  return ret;
}

int ObTmpFileFlushTG::special_flush_meta_tree_page_()
{
  int ret = OB_SUCCESS;
  ObSpLinkQueue flushing_list;
  int64_t flushing_task_cnt = 0;
  int64_t current_flush_cnt = ATOMIC_LOAD(&flushing_block_num_);
  int64_t expect_flush_size = OB_STORAGE_OBJECT_MGR.get_macro_object_size();
  if (OB_FAIL(flush_mgr_.flush(flushing_list, flush_monitor_, expect_flush_size,
                               current_flush_cnt, true/*is_flush_meta_tree*/))) {
    if (OB_TMP_FILE_EXCEED_DISK_QUOTA == ret) {
      signal_io_finish(ret);
    } else {
      STORAGE_LOG(ERROR, "flush mgr fail to do fast flush meta tree page", KR(ret), KPC(this));
    }
  }

  if (!flushing_list.is_empty()) { // ignore ret
    if (OB_FAIL(handle_generated_flush_tasks_(flushing_list, flushing_task_cnt))) {
      STORAGE_LOG(WARN, "fail to handle fast flush meta tasks", KR(ret), K(flushing_task_cnt), KPC(this));
    }
  }
  return ret;
}

int ObTmpFileFlushTG::check_flush_task_io_finished_()
{
  int ret = OB_SUCCESS;
  for (int64_t cnt = ATOMIC_LOAD(&wait_list_size_); cnt > 0 && !wait_list_.is_empty(); --cnt) {
    ObTmpFileFlushTask *flush_task = nullptr;
    pop_wait_list_(flush_task);
    if (OB_ISNULL(flush_task)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "flush task is nullptr", KR(ret));
    }
    bool write_block_success = false;
    if (OB_FAIL(ret)) {
    } else if (!flush_task->atomic_get_write_block_executed()) {
      push_wait_list_(flush_task); // not send IO yet, continue waiting
      ret = OB_SUCCESS;
    } else if (flush_task->atomic_get_write_block_ret_code() != OB_SUCCESS) {
      if (flush_task->atomic_get_write_block_ret_code() == OB_SERVER_OUTOF_DISK_SPACE) {
        signal_io_finish(OB_SERVER_OUTOF_DISK_SPACE);
      }
      // rollback to TFFT_ASYNC_WRITE and re-send IO
      if (OB_FAIL(flush_mgr_.io_finished(*flush_task))) {
        STORAGE_LOG(WARN, "fail to handle flush task finished", KR(ret), KPC(flush_task));
      } else if (FlushState::TFFT_ASYNC_WRITE == flush_task->get_state()) {
        push_retry_list_(flush_task);
        STORAGE_LOG(DEBUG, "write block failure flush task push to retry list", KPC(flush_task));
      } else {
        STORAGE_LOG(ERROR, "unexpected flush task state", KR(ret), KPC(flush_task));
      }
    } else {
      write_block_success = true;
    }

    if (OB_FAIL(ret) || !write_block_success) {
    } else if (OB_FAIL(flush_task->wait_macro_block_handle())) {
      if (OB_EAGAIN == ret) {
        push_wait_list_(flush_task); // IO is not completed, continue waiting
        static const int64_t FLUSH_TASK_IO_WARN_INTERVAL = 30 * 1000 * 1000; // 30s
        if (ObTimeUtil::current_time() - flush_task->get_last_print_ts() > FLUSH_TASK_IO_WARN_INTERVAL) {
          LOG_WARN("flush task execute takes too much time", KPC(flush_task));
          flush_task->set_last_print_ts(ObTimeUtil::current_time());
        }
        ret = OB_SUCCESS;
      } else {
        STORAGE_LOG(WARN, "unexpected error in waiting flush task finished", KR(ret), KPC(this));
      }
    } else if (!flush_task->atomic_get_io_finished()) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "unexpected flush task state", KR(ret), KPC(flush_task));
    } else if (OB_FAIL(flush_mgr_.io_finished(*flush_task))) {
      STORAGE_LOG(WARN, "fail to handle flush task finished", KR(ret), KPC(flush_task));
    } else {
      if (FlushState::TFFT_FINISH == flush_task->get_state()) {
        push_finished_list_(flush_task);
        STORAGE_LOG(DEBUG, "flush task push to finished list", KPC(flush_task));
      } else if (FlushState::TFFT_ASYNC_WRITE == flush_task->get_state()) {
        push_retry_list_(flush_task);
        STORAGE_LOG(DEBUG, "flush task push to retry list", KPC(flush_task));
      } else {
        STORAGE_LOG(ERROR, "unexpected flush task state", KR(ret), KPC(flush_task));
      }
    }
  }

  for (int64_t cnt = ATOMIC_LOAD(&finished_list_size_); cnt > 0 && !finished_list_.is_empty(); --cnt) {
    ObTmpFileFlushTask *flush_task = nullptr;
    pop_finished_list_(flush_task);
    if (OB_ISNULL(flush_task)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "flush task is nullptr", KR(ret));
    } else {
      STORAGE_LOG(DEBUG, "flush task io complete", K(flushing_block_num_), KPC(flush_task));
      // if the update fails, it will be retried during the next wakeup
      if (OB_FAIL(flush_mgr_.update_file_meta_after_flush(*flush_task))) {
        STORAGE_LOG(WARN, "fail to drive flush state machine", KR(ret), KPC(flush_task));
        push_finished_list_(flush_task);
      } else {
        flush_task_finished_(flush_task);
      }
    }
  }
  return ret;
}

void ObTmpFileFlushTG::flush_task_finished_(ObTmpFileFlushTask *flush_task)
{
  int ret = OB_SUCCESS;

  bool is_flush_tree_page = flush_task->get_is_fast_flush_tree();
  flush_mgr_.free_flush_task(flush_task);
  ATOMIC_DEC(&flushing_block_num_);
  fast_flush_meta_task_cnt_ -= is_flush_tree_page ? 1 : 0;
  if (fast_flush_meta_task_cnt_ == 0) {
    // reset is_fast_flush_meta_ flag to resume retry task and flush
    is_fast_flush_meta_ = false;
  } else if (OB_UNLIKELY(fast_flush_meta_task_cnt_ < 0)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "fast_flush_meta_task_cnt_ is negative", KR(ret), KPC(this));
  }
  signal_io_finish(OB_SUCCESS);
}

int ObTmpFileFlushTG::push_wait_list_(ObTmpFileFlushTask *flush_task)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(flush_task)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "flush task is nullptr", KR(ret));
  } else if (OB_FAIL(wait_list_.push(flush_task))) {
    STORAGE_LOG(WARN, "fail push flush task into wait_list_", KR(ret), KP(flush_task));
  } else {
    ATOMIC_INC(&wait_list_size_);
  }
  return ret;
}

int ObTmpFileFlushTG::pop_wait_list_(ObTmpFileFlushTask *&flush_task)
{
  int ret = OB_SUCCESS;
  ObSpLinkQueue::Link *link = nullptr;
  if (OB_FAIL(wait_list_.pop(link))) {
    STORAGE_LOG(DEBUG, "fail to pop flush task from wait_list_", KR(ret));
  } else if (OB_ISNULL(link)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "flush task ptr in wait list is null", KR(ret));
  } else {
    ATOMIC_DEC(&wait_list_size_);
    flush_task = static_cast<ObTmpFileFlushTask *>(link);
  }
  return ret;
}

int ObTmpFileFlushTG::push_retry_list_(ObTmpFileFlushTask *flush_task)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(flush_task)){
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "flush task is nullptr", KR(ret));
  } else if (OB_FAIL(retry_list_.push(flush_task))) {
    STORAGE_LOG(WARN, "fail push flush task into retry_list_", KR(ret), KP(flush_task));
  } else {
    ATOMIC_INC(&retry_list_size_);
  }
  return ret;
}

int ObTmpFileFlushTG::pop_retry_list_(ObTmpFileFlushTask *&flush_task)
{
  int ret = OB_SUCCESS;
  ObSpLinkQueue::Link *link = nullptr;
  if (OB_FAIL(retry_list_.pop(link))) {
    STORAGE_LOG(DEBUG, "fail to pop flush task from retry_list_", KR(ret));
  } else if (OB_ISNULL(link)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "flush task ptr is null", KR(ret));
  } else {
    ATOMIC_DEC(&retry_list_size_);
    flush_task = static_cast<ObTmpFileFlushTask *>(link);
  }
  return ret;
}

int ObTmpFileFlushTG::push_finished_list_(ObTmpFileFlushTask *flush_task)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(flush_task)){
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "flush task is nullptr", KR(ret));
  } else if (OB_FAIL(finished_list_.push(flush_task))) {
    STORAGE_LOG(WARN, "fail push flush task into finished_list_", KR(ret), KP(flush_task));
  } else {
    ATOMIC_INC(&finished_list_size_);
  }
  return ret;
}

int ObTmpFileFlushTG::pop_finished_list_(ObTmpFileFlushTask *&flush_task)
{
  int ret = OB_SUCCESS;
  ObSpLinkQueue::Link *link = nullptr;
  if (OB_FAIL(finished_list_.pop(link))) {
    STORAGE_LOG(DEBUG, "fail to pop flush task from finished_list_", KR(ret));
  } else if (OB_ISNULL(link)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "flush task ptr is null", KR(ret));
  } else {
    ATOMIC_DEC(&finished_list_size_);
    flush_task = static_cast<ObTmpFileFlushTask *>(link);
  }
  return ret;
}

int ObTmpFileFlushTG::get_fast_flush_size_()
{
  // TODO: move to page cache controller
  const int64_t BLOCK_SIZE = ObTmpFileGlobal::SN_BLOCK_SIZE;
  int64_t wbp_mem_limit = wbp_.get_memory_limit();
  int64_t flush_size = max(BLOCK_SIZE, min(ObTmpFileGlobal::MAX_FLUSHING_BLOCK_NUM * BLOCK_SIZE,
                           upper_align(0.1 * wbp_mem_limit, BLOCK_SIZE)));
  return flush_size;
}

int64_t ObTmpFileFlushTG::get_flushing_block_num_threshold_()
{
  const int64_t BLOCK_SIZE = ObTmpFileGlobal::SN_BLOCK_SIZE;
  int64_t wbp_mem_limit = wbp_.get_memory_limit();
  int64_t flush_threshold =
    max(1, min(ObTmpFileGlobal::MAX_FLUSHING_BLOCK_NUM,
               static_cast<int64_t>(0.2 * wbp_mem_limit / BLOCK_SIZE)));
  return flush_threshold;
}

// --------------- swap ----------------//

ObTmpFileSwapTG::ObTmpFileSwapTG(ObTmpWriteBufferPool &wbp,
                                 ObTmpFileEvictionManager &elimination_mgr,
                                 ObTmpFileFlushTG &flush_tg,
                                 ObTmpFilePageCacheController &pc_ctrl)
  : is_inited_(false),
    tg_id_(-1),
    idle_cond_(),
    last_swap_timestamp_(0),
    swap_job_num_(0),
    swap_job_list_(),
    working_list_size_(0),
    working_list_(),
    swap_monitor_(),
    flush_tg_ref_(flush_tg),
    flush_io_finished_round_(0),
    wbp_(wbp),
    evict_mgr_(elimination_mgr),
    pc_ctrl_(pc_ctrl)
{
}

int ObTmpFileSwapTG::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObTmpFileSwapTG init twice");
  } else if (OB_FAIL(idle_cond_.init(ObWaitEventIds::THREAD_IDLING_COND_WAIT))) {
    STORAGE_LOG(WARN, "failed to init condition variable", KR(ret));
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::TmpFileSwap, tg_id_))) {
    STORAGE_LOG(WARN, "fail to create swap thread", KR(ret));
  } else if (OB_FAIL(TG_SET_RUNNABLE(tg_id_, *this))) {
    STORAGE_LOG(WARN, "fail to set swap tg runnable", KR(ret));
  } else {
    is_inited_ = true;
    last_swap_timestamp_ = 0;
    swap_job_num_ = 0;
    working_list_size_ = 0;
    flush_io_finished_round_ = 0;
  }
  return ret;
}

int ObTmpFileSwapTG::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpFileSwapTG thread is not inited", KR(ret));
  } else if (OB_FAIL(TG_START(tg_id_))) {
    STORAGE_LOG(WARN, "fail to start tmp file ObTmpFileSwapTG thread", KR(ret));
  }
  return ret;
}

void ObTmpFileSwapTG::stop()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpFileSwapTG thread is not inited", KR(ret));
  } else {
    TG_STOP(tg_id_);
    ObThreadCondGuard guard(idle_cond_);
    idle_cond_.signal();
  }
}

void ObTmpFileSwapTG::wait()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpFileSwapTG thread is not inited", KR(ret));
  } else {
    TG_WAIT(tg_id_);
  }
}

void ObTmpFileSwapTG::destroy()
{
  int ret = OB_SUCCESS;
  if (-1 != tg_id_) {
    TG_DESTROY(tg_id_);
    tg_id_ = -1;
  }

  clean_up_lists_();
  last_swap_timestamp_ = 0;
  swap_job_num_ = 0;
  working_list_size_ = 0;
  flush_io_finished_round_ = 0;
  idle_cond_.destroy();
  is_inited_ = false;
}

int ObTmpFileSwapTG::swap_job_enqueue(ObTmpFileSwapJob *swap_job)
{
  int ret = OB_SUCCESS;
  if (has_set_stop()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "swap thread has been stopped", KR(ret), K(tg_id_), KP(swap_job));
  } else if (OB_FAIL(swap_job_list_.push(swap_job))) {
    STORAGE_LOG(WARN, "fail push swap job", KR(ret), K(tg_id_), KP(swap_job));
  } else {
    ATOMIC_INC(&swap_job_num_);
  }
  return ret;
}

int ObTmpFileSwapTG::swap_job_dequeue(ObTmpFileSwapJob *&swap_job)
{
  int ret = OB_SUCCESS;
  ObSpLinkQueue::Link *link = nullptr;
  if (OB_FAIL(swap_job_list_.pop(link))) {
    STORAGE_LOG(DEBUG, "fail to pop swap job", KR(ret), K(tg_id_));
  } else if (OB_ISNULL(link)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "fail to get swap job, ptr is nullptr", KR(ret), K(tg_id_));
  } else {
    ATOMIC_DEC(&swap_job_num_);
    swap_job = static_cast<ObTmpFileSwapJob*>(link);
  }
  return ret;
}

void ObTmpFileSwapTG::notify_doing_swap()
{
  ObThreadCondGuard guard(idle_cond_);
  idle_cond_.signal();
}

int64_t ObTmpFileSwapTG::cal_idle_time()
{
  int64_t swap_idle_time = ObTmpFilePageCacheController::SWAP_INTERVAL;
  if (OB_UNLIKELY(!SERVER_STORAGE_META_SERVICE.is_started())) {
    swap_idle_time = ObTmpFilePageCacheController::SWAP_INTERVAL;
  } else if (ATOMIC_LOAD(&swap_job_num_) != 0 || ATOMIC_LOAD(&working_list_size_) != 0) {
    if (flush_io_finished_round_ < flush_tg_ref_.get_flush_io_finished_round()) {
      swap_idle_time  = 0;
    } else {
      swap_idle_time  = ObTmpFilePageCacheController::SWAP_FAST_INTERVAL;
    }
  }
  return swap_idle_time;
}

void ObTmpFileSwapTG::run1()
{
  int ret = OB_SUCCESS;
  lib::set_thread_name("TFSwap");
  while (!has_set_stop()) {
    if (OB_FAIL(shrink_wbp_if_needed_())) {
      STORAGE_LOG(WARN, "fail to flush for shrinking wbp", KR(ret), KPC(this));
    }

    // overwrite ret
    if (OB_FAIL(swap())) {
      STORAGE_LOG(WARN, "fail to try swap work", KR(ret));
    }

    // overwrite ret
    if (OB_FAIL(flush_tg_ref_.try_work())) {
      STORAGE_LOG(WARN, "fail to try flush work", KR(ret));
    }

    ObThreadCondGuard guard(idle_cond_);
    int64_t swap_idle_time = cal_idle_time();
    int64_t flush_idle_time = flush_tg_ref_.cal_idle_time();
    int64_t idle_time = min(swap_idle_time, flush_idle_time);
    if (!has_set_stop() && idle_time != 0) {
      idle_cond_.wait(idle_time);
    }
  }
}

void ObTmpFileSwapTG::clean_up_lists_()
{
  int ret = OB_SUCCESS;
  while (!swap_job_list_.is_empty()) {
    ObTmpFileSwapJob *swap_job = nullptr;
    if (OB_FAIL(swap_job_dequeue(swap_job))) {
      STORAGE_LOG(WARN, "fail dequeue swap job or swap job is nullptr", KR(ret), KP(swap_job));
    } else if (OB_FAIL(swap_job->signal_swap_complete(OB_SUCCESS))){
      STORAGE_LOG(WARN, "fail to signal swap complete", KR(ret));
    }
  }

  ret = OB_SUCCESS;
  while (!working_list_.is_empty()) {
    ObTmpFileSwapJob *swap_job = nullptr;
    if (OB_FAIL(pop_working_job_(swap_job))) {
      STORAGE_LOG(WARN, "fail to pop working job or ptr is null", KR(ret), KP(swap_job));
    } else if (OB_FAIL(swap_job->signal_swap_complete(OB_SUCCESS))){
      STORAGE_LOG(WARN, "fail to signal swap complete", KR(ret));
    }
  }
}

int ObTmpFileSwapTG::swap()
{
  int ret = OB_SUCCESS;

  int64_t cur_time = ObTimeUtility::current_monotonic_time();
  if (0 == last_swap_timestamp_ || cur_time - last_swap_timestamp_ >= cal_idle_time() * 1000) {
    if (OB_FAIL(do_work_())) {
      STORAGE_LOG(WARN, "fail do swap", KR(ret), KPC(this));
    }
    last_swap_timestamp_ = ObTimeUtility::current_monotonic_time();
  }

  return ret;
}

int ObTmpFileSwapTG::do_work_()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpFileSwapTG not init", KR(ret));
  }

  if (OB_SUCC(ret)) {
    if (TC_REACH_TIME_INTERVAL(ObTmpFilePageCacheController::REFRESH_CONFIG_INTERVAL)) {
      pc_ctrl_.refresh_disk_usage_limit();
    }

    if (ATOMIC_LOAD(&swap_job_num_) == 0 && ATOMIC_LOAD(&working_list_size_) == 0) {
      if (OB_FAIL(swap_normal_())){
        STORAGE_LOG(WARN, "fail to do normal swap", KR(ret));
      }
    } else {
      if (OB_FAIL(swap_fast_())){
        STORAGE_LOG(WARN, "fail to do fast swap", KR(ret));
      }
      if (OB_FAIL(swap_normal_())) {
        STORAGE_LOG(WARN, "fail to do normal swap", KR(ret));
      }
    }
    if (ATOMIC_LOAD(&swap_job_num_) == 0 && ATOMIC_LOAD(&working_list_size_) == 0) {
      flush_tg_ref_.set_running_mode(ObTmpFileFlushTG::RUNNING_MODE::NORMAL);
    }
  }
  if (TC_REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
    swap_monitor_.print_statistics();
  }
  return ret;
}

// runs in normal mode when there is no swap job.
// since memory is not tight at this point, we try to evict pages
// but not guarantee eviction occurs if clean pages are not enough
int ObTmpFileSwapTG::swap_normal_()
{
  int ret = OB_SUCCESS;

  int64_t swap_size = wbp_.get_swap_size();
  int64_t actual_swap_page_cnt = 0;
  if (swap_size > 0) { // do swap
    int64_t swap_page_cnt = swap_size / PAGE_SIZE;
    if (OB_FAIL(evict_mgr_.evict(swap_page_cnt, actual_swap_page_cnt))) {
      STORAGE_LOG(WARN, "fail to swap out pages", KR(ret), K(swap_size));
    }
  }
  return ret;
}

// attempt to evict pages and wake up caller threads as soon as possible,
// this may trigger the flush thread to flush a small number of pages (FAST mode).
// caller threads will be awakened if swap job timeout
int ObTmpFileSwapTG::swap_fast_()
{
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret) && !swap_job_list_.is_empty()) {
    ObTmpFileSwapJob *swap_job = nullptr;
    if (OB_FAIL(swap_job_dequeue(swap_job))) {
      STORAGE_LOG(WARN, "fail to get swap job", KR(ret));
    } else if (OB_FAIL(push_working_job_(swap_job))) {
      STORAGE_LOG(WARN, "fail to push working job", KR(ret), KP(swap_job));
    }
  }

  while (OB_SUCC(ret) && !working_list_.is_empty()) {
    int64_t expect_swap_page_cnt = 0;
    int64_t actual_swap_page_cnt = 0;
    // calculate expect swap pages number for a batch of jobs
    if (OB_FAIL(calculate_swap_page_num_(PROCCESS_JOB_NUM_PER_BATCH, expect_swap_page_cnt))) {
      STORAGE_LOG(WARN, "fail to calculate swap page num", KR(ret));
    } else if (OB_UNLIKELY(expect_swap_page_cnt <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "cur expect swap page cnt is invalid", KR(ret), K(expect_swap_page_cnt));
    } else if (OB_FAIL(evict_mgr_.evict(expect_swap_page_cnt, actual_swap_page_cnt))) {
      STORAGE_LOG(WARN, "fail to swap out pages", KR(ret), K(expect_swap_page_cnt), K(actual_swap_page_cnt));
    }

    int64_t wakeup_job_cnt = 0;
    wakeup_satisfied_jobs_(wakeup_job_cnt);
    wakeup_timeout_jobs_();
    int io_finished_ret = flush_tg_ref_.get_flush_io_finished_ret();
    if (OB_SERVER_OUTOF_DISK_SPACE == io_finished_ret ||
        OB_TMP_FILE_EXCEED_DISK_QUOTA == io_finished_ret) {
      wakeup_all_jobs_(io_finished_ret);
    }

    // do flush if could not evict enough pages
    if (OB_SUCC(ret) && !working_list_.is_empty() && wakeup_job_cnt < PROCCESS_JOB_NUM_PER_BATCH) {
      flush_io_finished_round_ = flush_tg_ref_.get_flush_io_finished_round();
      flush_tg_ref_.set_running_mode(ObTmpFileFlushTG::RUNNING_MODE::FAST);
      flush_tg_ref_.notify_doing_flush();
      break;
    }
  } // end while

  return ret;
}

int ObTmpFileSwapTG::calculate_swap_page_num_(const int64_t batch_size, int64_t &expect_swap_cnt)
{
  int ret = OB_SUCCESS;
  ObSpLinkQueue cur_working_list;
  for (int64_t i = 0; OB_SUCC(ret) && i < batch_size && !working_list_.is_empty(); ++i) {
    ObTmpFileSwapJob *swap_job = nullptr;
    if (OB_FAIL(pop_working_job_(swap_job))) {
      STORAGE_LOG(WARN, "fail to pop working job or ptr is null", KR(ret), KP(swap_job));
    } else {
      expect_swap_cnt += upper_align(swap_job->get_expect_swap_size(), PAGE_SIZE) / PAGE_SIZE;
      cur_working_list.push_front(swap_job);
    }
  }
  while (!cur_working_list.is_empty()) {
    ObSpLinkQueue::Link *link = nullptr;
    cur_working_list.pop(link);
    ObTmpFileSwapJob *swap_job = static_cast<ObTmpFileSwapJob *>(link);
    if (OB_ISNULL(swap_job)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "swap job is invalid", KR(ret));
    } else if (OB_FAIL(push_working_job_front_(swap_job))) {
      STORAGE_LOG(WARN, "fail to push swap job", KR(ret), KPC(swap_job));
    }
  }
  return ret;
}

int ObTmpFileSwapTG::wakeup_satisfied_jobs_(int64_t& wakeup_job_cnt)
{
  int ret = OB_SUCCESS;
  wakeup_job_cnt = 0;
  int64_t wbp_free_page_cnt = wbp_.get_free_data_page_num();
  while (OB_SUCC(ret) && wbp_free_page_cnt > 0 && !working_list_.is_empty()) {
    ObTmpFileSwapJob *swap_job = nullptr;
    if (OB_FAIL(pop_working_job_(swap_job))) {
      STORAGE_LOG(WARN, "fail to pop working job or ptr is null", KR(ret), KP(swap_job));
    } else {
      // wake up threads even if free page number < swap job's expect swap size
      int64_t single_job_swap_page_cnt = upper_align(swap_job->get_expect_swap_size(), PAGE_SIZE) / PAGE_SIZE;
      wbp_free_page_cnt -= min(wbp_free_page_cnt, single_job_swap_page_cnt);
      int64_t response_time = ObTimeUtility::current_time() - swap_job->get_create_ts();
      swap_monitor_.record_swap_response_time(response_time);
      if (OB_FAIL(swap_job->signal_swap_complete(OB_SUCCESS))) {
        STORAGE_LOG(WARN, "fail to signal swap complete", KR(ret), KPC(swap_job));
      } else {
        ++wakeup_job_cnt;
      }
    }
  }
  return ret;
}

int ObTmpFileSwapTG::wakeup_timeout_jobs_()
{
  int ret = OB_SUCCESS;
  for (int64_t i = working_list_size_; OB_SUCC(ret) && i > 0 && !working_list_.is_empty(); --i) {
    ObTmpFileSwapJob *swap_job = nullptr;
    if (OB_FAIL(pop_working_job_(swap_job))) {
      STORAGE_LOG(WARN, "fail to pop working job or ptr is null", KR(ret), KP(swap_job));
    } else if (swap_job->get_abs_timeout_ts() <= ObTimeUtility::current_time()) {
      // timeout, wake it up
      int64_t response_time = ObTimeUtility::current_time() - swap_job->get_create_ts();
      swap_monitor_.record_swap_response_time(response_time);
      if (OB_FAIL(swap_job->signal_swap_complete(OB_TIMEOUT))) {
        STORAGE_LOG(WARN, "fail to signal swap complete", KR(ret), KP(swap_job));
      }
    } else {
      if (OB_FAIL(push_working_job_(swap_job))) {
        STORAGE_LOG(WARN, "fail to push swap job", KR(ret), K(tg_id_), KPC(swap_job));
      }
    }
  }
  return ret;
}

void ObTmpFileSwapTG::wakeup_all_jobs_(int ret_code)
{
  int ret = OB_SUCCESS;
  for (int64_t i = working_list_size_; OB_SUCC(ret) && i > 0 && !working_list_.is_empty(); --i) {
    ObTmpFileSwapJob *swap_job = nullptr;
    if (OB_FAIL(pop_working_job_(swap_job))) {
      STORAGE_LOG(WARN, "fail to pop working job or ptr is null", KR(ret), KP(swap_job));
    } else {
      int64_t response_time = ObTimeUtility::current_time() - swap_job->get_create_ts();
      swap_monitor_.record_swap_response_time(response_time);
      if (OB_FAIL(swap_job->signal_swap_complete(ret_code))) {
        STORAGE_LOG(WARN, "fail to signal swap complete", KR(ret), KP(swap_job));
      }
    }
  }
}

int ObTmpFileSwapTG::push_working_job_(ObTmpFileSwapJob *swap_job)
{
  int ret = OB_SUCCESS;
  if (has_set_stop()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "swap thread has been stopped", KR(ret), K(tg_id_), KP(swap_job));
  } else if (OB_FAIL(working_list_.push(swap_job))) {
    STORAGE_LOG(WARN, "fail push swap job", KR(ret), K(tg_id_), KPC(swap_job));
  } else {
    ATOMIC_INC(&working_list_size_);
  }
  return ret;
}

int ObTmpFileSwapTG::pop_working_job_(ObTmpFileSwapJob *&swap_job)
{
  int ret = OB_SUCCESS;
  ObSpLinkQueue::Link *link = nullptr;
  if (OB_FAIL(working_list_.pop(link))) {
    STORAGE_LOG(DEBUG, "fail to pop swap job", KR(ret), K(tg_id_));
  } else if (OB_ISNULL(link)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "fail to get swap job, ptr is nullptr", KR(ret), K(tg_id_));
  } else {
    ATOMIC_DEC(&working_list_size_);
    swap_job = static_cast<ObTmpFileSwapJob*>(link);
  }
  return ret;
}

int ObTmpFileSwapTG::push_working_job_front_(ObTmpFileSwapJob *swap_job)
{
  int ret = OB_SUCCESS;
  if (has_set_stop()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "swap thread has been stopped", KR(ret), K(tg_id_), KP(swap_job));
  } else if (OB_FAIL(working_list_.push_front(swap_job))) {
    STORAGE_LOG(WARN, "fail push swap job", KR(ret), K(tg_id_), KPC(swap_job));
  } else {
    ATOMIC_INC(&working_list_size_);
  }
  return ret;
}

int ObTmpFileSwapTG::shrink_wbp_if_needed_()
{
  int ret = OB_SUCCESS;
  int64_t actual_evict_num = 0;
  bool is_auto = false;
  ObTimeGuard time_guard("wbp_shrink", 1 * 1000 * 1000);
  if (wbp_.need_to_shrink(is_auto)) {
    LOG_DEBUG("current wbp shrinking state", K(wbp_.get_shrink_ctx()));
    switch (wbp_.get_wbp_state()) {
      case WBPShrinkContext::INVALID:
        if (OB_FAIL(wbp_.begin_shrinking(is_auto))) {
          STORAGE_LOG(WARN, "fail to init shrink context", KR(ret), K(wbp_.get_shrink_ctx()));
        } else {
          pc_ctrl_.set_flush_all_data(true);
          flush_tg_ref_.notify_doing_flush();
          wbp_.advance_shrink_state();
        }
        break;
      case WBPShrinkContext::SHRINKING_SWAP:
        if (!wbp_.get_shrink_ctx().is_valid()) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "shrink context is invalid", KR(ret));
        } else if (OB_FAIL(evict_mgr_.evict(INT64_MAX, actual_evict_num))) { // evict all pages
          STORAGE_LOG(WARN, "fail to evict all pages", KR(ret));
        } else {
          wbp_.advance_shrink_state();
        }
        break;
      case WBPShrinkContext::SHRINKING_RELEASE_BLOCKS:
        if (OB_FAIL(wbp_.release_blocks_in_shrink_range())) {
          STORAGE_LOG(WARN, "fail to shrink wbp", KR(ret), K(wbp_.get_shrink_ctx()));
        } else {
          pc_ctrl_.set_flush_all_data(false);
          wbp_.advance_shrink_state();
        }
        break;
      case WBPShrinkContext::SHRINKING_FINISH:
        if (OB_FAIL(wbp_.finish_shrinking())) {
          STORAGE_LOG(ERROR, "fail to finish shrinking", KR(ret));
        }
        break;
      default:
        break;
    }
  }

  // abort shrinking if wbp memory limit enlarge or flush fail with OB_SERVER_OUTOF_DISK_SPACE
  int io_finished_ret = flush_tg_ref_.get_flush_io_finished_ret();
  if (wbp_.get_shrink_ctx().is_valid() && (!wbp_.need_to_shrink(is_auto) || OB_SERVER_OUTOF_DISK_SPACE == io_finished_ret)) {
    wbp_.finish_shrinking();
  }
  time_guard.click("wbp_shrink finish one step");
  return ret;
}

}  // end namespace tmp_file
}  // end namespace oceanbase
