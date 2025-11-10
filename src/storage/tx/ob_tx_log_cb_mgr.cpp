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

#include "storage/tx/ob_tx_log_cb_mgr.h"
#include "storage/tx/ob_trans_part_ctx.h"

namespace oceanbase
{
namespace transaction
{

int ObTxLogCbPoolMgr::init(const int64_t tenant_id, const ObLSID ls_id)
{
  int ret = OB_SUCCESS;

  if (tenant_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(tenant_id));
  } else if (ATOMIC_LOAD(&is_inited_)) {
    ret = OB_INIT_TWICE;
    TRANS_LOG(WARN, "init twice", K(ret), KPC(this));
  } else {
    clear_sync_size_history_();
    allocator_.set_tenant_id(tenant_id);
    ls_id_ = ls_id;
    ATOMIC_STORE(&is_inited_, true);
  }

  return ret;
}

void ObTxLogCbPoolMgr::reset()
{
  ATOMIC_STORE(&is_inited_, false);
  ATOMIC_STORE(&idle_pool_ptr_, nullptr);
  ATOMIC_STORE(&ls_occupying_cnt_, 0);
  ATOMIC_STORE(&acquire_extra_log_cb_group_failed_cnt_, 0);
  clear_sync_size_history_();
  ATOMIC_STORE(&allow_expand_, false);

  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTxLogCbPool *pool_ptr = nullptr;
  while (ret == OB_SUCCESS) {
    SpinWLockGuard guard(pool_list_rw_lock_);
    pool_ptr = pool_list_.get_first();

    if (pool_ptr == nullptr || pool_ptr == pool_list_.get_header()) {
      pool_ptr == nullptr;
    }

    if (pool_list_.get_size() <= 0 && gc_pool_list_.get_size() <= 0) {
      ret = OBP_ITER_END;
    } else if (OB_TMP_FAIL(free_log_cb_pool_(pool_ptr))) {
      TRANS_LOG(WARN, "free a log cb pool failed", K(tmp_ret), KP(pool_ptr));
    }
  }
}

void ObTxLogCbPoolMgr::destroy() { reset(); }

int ObTxLogCbPoolMgr::clear_log_cb_pool(const bool for_replay)
{
  int ret = OB_SUCCESS;

  ATOMIC_STORE(&allow_expand_, false);

  // if (OB_SUCC(ret) && for_replay) {
  //   SpinRLockGuard guard(pool_list_rw_lock_);
  //   if (pool_list_.is_empty()) {
  //     ret = OB_EMPTY_RANGE;
  //   }
  // }

  if (OB_SUCC(ret)) {
    SpinWLockGuard guard(pool_list_rw_lock_);
    ObTxLogCbPool *pool_ptr = nullptr;
    while (OB_SUCC(ret) && (pool_list_.get_size() > 0 || gc_pool_list_.get_size() > 0)) {
      pool_ptr = pool_list_.get_first();
      if (OB_ISNULL(pool_ptr) || pool_ptr == pool_list_.get_header()) {
        pool_ptr = nullptr;
      }

      if (OB_ISNULL(pool_ptr)) {
        //do nothing
      } else if (OB_FALSE_IT(ATOMIC_BCAS(&idle_pool_ptr_, pool_ptr, nullptr))) {
        //do nothing
      }

      if (OB_FAIL(free_log_cb_pool_(pool_ptr, 10 * 1000 /*10ms*/))) {
        if (!for_replay) {
          TRANS_LOG(WARN, "free a log cb pool failed", K(ret), K(pool_ptr), KPC(this));
        } else {
          TRANS_LOG(ERROR, "free a log cb pool in replay error", K(ret), KPC(pool_ptr), KPC(this));
        }
      }
    }
  }

  clear_sync_size_history_();

  return ret;
}

int ObTxLogCbPoolMgr::switch_to_leader(const int64_t active_tx_cnt)
{
  int ret = OB_SUCCESS;
  ATOMIC_STORE(&allow_expand_, true);

  const int64_t active_tx_default_log_pool_cnt =
      ObTxLogCbGroup::ACTIVE_TX_DEFAULT_LOG_GROUP_COUNT * active_tx_cnt
                  % ObTxLogCbPool::MAX_LOG_CB_GROUP_COUNT_IN_POOL
              == 0
          ? ObTxLogCbGroup::ACTIVE_TX_DEFAULT_LOG_GROUP_COUNT * active_tx_cnt
                / ObTxLogCbPool::MAX_LOG_CB_GROUP_COUNT_IN_POOL
          : ObTxLogCbGroup::ACTIVE_TX_DEFAULT_LOG_GROUP_COUNT * active_tx_cnt
                    / ObTxLogCbPool::MAX_LOG_CB_GROUP_COUNT_IN_POOL
                + 1;
  int64_t target_log_pool_cnt =
      active_tx_default_log_pool_cnt <= 0 ? 1 : active_tx_default_log_pool_cnt;

  if (OB_SUCC(ret)) {
    int64_t pool_list_size = 0;
    {
      SpinRLockGuard guard(pool_list_rw_lock_);
      const int64_t pool_list_size = pool_list_.get_size();
    }
    if (target_log_pool_cnt >= pool_list_size) {
      // do nothing
    } else {
      for (int i = pool_list_size; OB_SUCC(ret) && i < target_log_pool_cnt; i++) {
        if (OB_FAIL(append_new_log_cb_pool_())) {
          TRANS_LOG(WARN, "append a new log cb pool failed", K(ret), K(i), K(target_log_pool_cnt),
                    K(active_tx_default_log_pool_cnt), K(pool_list_size), KPC(this));
        }
      }
    }
  }

  return ret;
}

#ifdef ERRSIM
ERRSIM_POINT_DEF(EN_ADJUST_TX_LOG_CB_POOL)
#endif
int ObTxLogCbPoolMgr::adjust_log_cb_pool(const int64_t active_tx_cnt)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int estimated_ret = OB_SUCCESS;
  int64_t expected_pool_cnt = -1;
  int64_t limit_pool_cnt = -1;

#ifdef ERRSIM
  ret = EN_ADJUST_TX_LOG_CB_POOL;
  if (ret != OB_SUCCESS) {
    TRANS_LOG(WARN, "adjust tx log cb pool", K(ret), K(active_tx_cnt), KPC(this));
  }
#endif

  SpinWLockGuard w_lock_guard(sync_size_his_lock_);

  const int64_t start_estimated_time = ObTimeUtility::fast_current_time();
  int64_t total_synced_size = 0;
  int64_t total_synced_time = 0;
  int64_t total_occupied_count = 0;
  int64_t total_occupied_time = 0;
  int64_t total_syncing_size = 0;
  int64_t total_occpying_size = 0;
  int64_t aver_adjust_interval = 0;

  int64_t pool_list_size = 0;
  SyncSizeHistoryFlag sync_his_flag = SyncSizeHistoryFlag::NO_CHANGE;

  int64_t expand_cnt = 0;
  int64_t sync_size_increased_cnt = 0;

  int64_t removed_cnt = 0;

  const int64_t tenant_memory_limit = ::oceanbase::lib::get_tenant_memory_limit(MTL_ID());

  common::ObLabelItem item;
  ObLabel mem_label("TxLogCbPool");
  (void)::oceanbase::lib::get_tenant_label_memory(MTL_ID(), mem_label, item);
  const int64_t log_cb_pool_mem_used = item.hold_;
  // ::oceanbase::lib::get_tenant_label_memory(MTL_ID(), ObLabel &label, common::ObLabelItem &item);

  if (OB_SUCC(ret)) {
    SpinRLockGuard guard(pool_list_rw_lock_);
    pool_list_size = pool_list_.get_size();
    DLIST_FOREACH(pool_iter, pool_list_)
    {
      int64_t estimated_synced_size = 0;
      int64_t estimated_synced_time = 0;
      int64_t estimated_occupied_count = 0;
      int64_t estimated_occupied_time = 0;
      int64_t syncing_size = 0;
      int64_t occupying_size = 0;
      int64_t last_barrier_ts = 0;
      if (OB_FAIL(pool_iter->cal_estimated_log_cb_pool_stat(
              start_estimated_time, estimated_synced_size, estimated_synced_time,
              estimated_occupied_count, estimated_occupied_time, syncing_size, occupying_size,
              last_barrier_ts))) {
        TRANS_LOG(WARN, "cal estimated stat for log cb pool failed", K(ret), KPC(pool_iter));
      } else {
        total_synced_size += estimated_synced_size;
        total_synced_time += estimated_synced_time;
        total_occupied_count += estimated_occupied_count;
        total_occupied_time += estimated_occupied_time;
        total_syncing_size += syncing_size;
        total_occpying_size += occupying_size;
        aver_adjust_interval += start_estimated_time - last_barrier_ts;
      }
    }
    estimated_ret = ret;
    if (pool_list_size != 0) {
      aver_adjust_interval = aver_adjust_interval / pool_list_size;
    }
  }

  // check sync size his  -- allow expand
  if (OB_SUCC(ret)) {
    if (OB_FAIL(check_sync_size_increased_(expand_cnt, sync_size_increased_cnt))) {
      TRANS_LOG(WARN, "check sync_size increased failed", K(ret), K(expand_cnt),
                K(sync_size_increased_cnt));
    } else {
      if ((expand_cnt > 0 && sync_size_increased_cnt < expand_cnt)
          || (log_cb_pool_mem_used >= tenant_memory_limit / 10)) {
        ATOMIC_STORE(&allow_expand_, false);
      } else {
        ATOMIC_STORE(&allow_expand_, true);
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (!ATOMIC_LOAD(&allow_expand_)) {
      ret = OB_OP_NOT_ALLOW;
      TRANS_LOG(WARN, "the expansion operation has been prohibited", K(ret), K(expand_cnt),
                K(sync_size_increased_cnt), KPC(this));
    }
  }

  // start to expand or shrink
  if (OB_SUCC(ret)) {
    const double max_group_occupy_percent = 0.9;
    const double expected_group_occupy_percent = 0.8;
    const double min_group_occupy_percent = 0.6;

    double cur_group_occupy_percent = 0.0;
    double expected_pool_float_cnt = 0.0;

    if (pool_list_size != 0 && aver_adjust_interval != 0) {
      cur_group_occupy_percent =
          ((double)total_occupied_time)
          / (ObTxLogCbPool::MAX_LOG_CB_GROUP_COUNT_IN_POOL * aver_adjust_interval * pool_list_size);
    }

    if (pool_list_size == 0) {
      expected_pool_float_cnt = ObTxLogCbGroup::ACTIVE_TX_DEFAULT_LOG_GROUP_COUNT * active_tx_cnt
                                * 1.0 / ObTxLogCbPool::MAX_LOG_CB_GROUP_COUNT_IN_POOL;
      if (expected_pool_float_cnt <= 0.0
          && ATOMIC_LOAD(&acquire_extra_log_cb_group_failed_cnt_)
                 > ObTxLogCbPool::MAX_LOG_CB_GROUP_COUNT_IN_POOL / 4) {
        expected_pool_float_cnt = 1.0;
      }
    } else if (cur_group_occupy_percent >= max_group_occupy_percent
               || cur_group_occupy_percent <= min_group_occupy_percent) {
      expected_pool_float_cnt = total_occupied_time / expected_group_occupy_percent
                                / aver_adjust_interval
                                / ObTxLogCbPool::MAX_LOG_CB_GROUP_COUNT_IN_POOL;
    } else {
      expected_pool_float_cnt = pool_list_size;
    }

    expected_pool_cnt = ceil(expected_pool_float_cnt);

    if (pool_list_size < expected_pool_cnt) {
      for (int64_t i = pool_list_size; i < expected_pool_cnt && OB_SUCC(ret); i++) {
        if (OB_FAIL(append_new_log_cb_pool_())) {
          TRANS_LOG(WARN, "append a new log cb pool failed", K(ret), K(i), K(pool_list_size),
                    K(expected_pool_cnt), KPC(this));
        } else {
          sync_his_flag = SyncSizeHistoryFlag::EXPAND;
        }
      }
    } else if (pool_list_size > expected_pool_cnt) {
      const int64_t MAX_ITER_POOL_CNT = 30;
      const int64_t MAX_NEED_REMOVE_CNT = 5;

      const int64_t actual_need_remove_cnt =
          pool_list_size - expected_pool_cnt > MAX_NEED_REMOVE_CNT
              ? MAX_NEED_REMOVE_CNT
              : pool_list_size - expected_pool_cnt;
      int64_t iter_cnt = 0;

      SpinWLockGuard guard(pool_list_rw_lock_);
      DLIST_FOREACH_REMOVESAFE_X(
          curr, pool_list_, removed_cnt < actual_need_remove_cnt && iter_cnt < MAX_ITER_POOL_CNT)
      {
        if (curr->can_be_freed()) {
          pool_list_.remove(curr);
          ATOMIC_BCAS(&idle_pool_ptr_, curr, nullptr);
          free_log_cb_pool_(curr);
          removed_cnt++;
        }
        iter_cnt++;
      }

      if (removed_cnt > 0) {
        sync_his_flag = SyncSizeHistoryFlag::SHRINK;
      }
    }

    TRANS_LOG(INFO, "[LogCbPool Adjust] cal expected pool count", K(ret), K(estimated_ret),
              K(expected_pool_cnt), K(expected_pool_float_cnt), K(active_tx_cnt),K(acquire_extra_log_cb_group_failed_cnt_), K(cur_group_occupy_percent),
              K(max_group_occupy_percent), K(pool_list_size), K(sync_his_flag));
  }

  // push back sync size his
  if (estimated_ret == OB_SUCCESS) {
    if (OB_TMP_FAIL(push_back_sync_size_history_(total_synced_size, sync_his_flag))) {
      TRANS_LOG(WARN, "push back sync_size history failed", K(ret), K(tmp_ret),
                K(start_estimated_time), K(total_synced_size), K(sync_his_flag));
    }
  }

  TRANS_LOG(INFO, "[LogCbPool Adjust] adjust log cb pools", K(ret), K(estimated_ret),
            K(removed_cnt), K(sync_size_increased_cnt), K(expand_cnt), K(pool_list_size),
            K(tenant_memory_limit), K(log_cb_pool_mem_used), K(expected_pool_cnt),
            K(limit_pool_cnt), K(start_estimated_time), K(total_synced_size), K(total_synced_time),
            K(total_occupied_count), K(total_occupied_time), K(total_syncing_size),
            K(total_occpying_size), K(aver_adjust_interval), K(sync_his_flag), KPC(this));
  (void)print_sync_size_history_();

  ATOMIC_STORE(&acquire_extra_log_cb_group_failed_cnt_, 0);

  return ret;
}

int ObTxLogCbPoolMgr::acquire_idle_log_cb_group(ObTxLogCbGroup *&group_ptr, ObPartTransCtx *tx_ctx)
{
  int ret = OB_SUCCESS;

  ObTxLogCbPoolRefGuard ref_guard;

  TRANS_LOG(DEBUG, "start to acquire_log_cb_group", K(ret), KP(group_ptr), KP(tx_ctx));
  if (!ATOMIC_LOAD(&is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "the log cb pool mgr is not inited", K(ret), KPC(this));
  } else if (OB_FAIL(iter_idle_pool_(ref_guard))) {
    TRANS_LOG(WARN, "iter a idle pool failed", K(ret), K(ref_guard), KPC(this));
  } else if (OB_FAIL(ref_guard.get_pool_ptr()->acquire_log_cb_group(group_ptr))) {
    if (OB_TX_NOLOGCB == ret) {
      ATOMIC_BCAS(&idle_pool_ptr_, ref_guard.get_pool_ptr(), nullptr);
    }
    TRANS_LOG(WARN, "acquire a log cb group failed", K(ret), K(ref_guard), KPC(group_ptr),
              KPC(this));
  } else if (OB_FAIL(group_ptr->occupy_by_tx(tx_ctx))) {
    TRANS_LOG(WARN, "occupy log cb group by a tx_ctx failed", K(ret), K(ref_guard), KPC(group_ptr),
              KPC(this));
  } else {
    TRANS_LOG(INFO, "[Log Cb Group Life] occupy a idle group by tx", K(ret),
              K(tx_ctx->get_trans_id()), K(tx_ctx->get_ls_id()), K(ref_guard), KPC(group_ptr));
  }

  if (OB_SUCC(ret)) {
    ATOMIC_INC(&ls_occupying_cnt_);
  } else if (OB_TX_NOLOGCB == ret) {
    ATOMIC_INC(&acquire_extra_log_cb_group_failed_cnt_);
  }
  TRANS_LOG(DEBUG, "finish to acquire_log_cb_group", K(ret), KPC(group_ptr), KPC(tx_ctx));

  return ret;
}

bool ObTxLogCbPoolMgr::is_all_busy()
{
  SpinRLockGuard r_guard(pool_list_rw_lock_);

  return pool_list_.get_size() == 0
         || (pool_list_.get_size() > 0
             && ATOMIC_LOAD(&ls_occupying_cnt_)
                    == pool_list_.get_size() * ObTxLogCbPool::MAX_LOG_CB_GROUP_COUNT_IN_POOL);
}

int ObTxLogCbPoolMgr::append_new_log_cb_pool_()
{
  int ret = OB_SUCCESS;

  ObTxLogCbPool *log_cb_alloc_ptr = nullptr;
  if (OB_FAIL(alloc_log_cb_pool_(log_cb_alloc_ptr))) {
    TRANS_LOG(WARN, "alloc a log cb pool failed", K(ret), KPC(log_cb_alloc_ptr), KPC(this));
  } else {
    SpinWLockGuard guard(pool_list_rw_lock_);
    if (false == pool_list_.add_last(log_cb_alloc_ptr)) {
      ret = OB_EAGAIN;
      TRANS_LOG(WARN, "push back into pool_list failed", K(ret), KPC(log_cb_alloc_ptr), KPC(this));
    }

    if (OB_FAIL(ret)) {
      log_cb_alloc_ptr->~ObTxLogCbPool();
      (void)allocator_.free(log_cb_alloc_ptr);
    } else {
      TRANS_LOG(INFO, "append a new log cb pool", K(ret), KPC(log_cb_alloc_ptr), KPC(this));
    }
  }

  return ret;
}

int ObTxLogCbPoolMgr::alloc_log_cb_pool_(ObTxLogCbPool *&alloc_ptr)
{
  int ret = OB_SUCCESS;

  const int64_t log_cb_mem_size = sizeof(ObTxLogCbPool);
  ObTxLogCbPool *tmp_alloc_ptr = nullptr;

  if (OB_ISNULL(tmp_alloc_ptr = static_cast<ObTxLogCbPool *>(allocator_.alloc(log_cb_mem_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TRANS_LOG(WARN, "alloc log cb pool failed", K(ret), K(log_cb_mem_size), KP(tmp_alloc_ptr));
  } else if (OB_FALSE_IT(new (tmp_alloc_ptr) ObTxLogCbPool())) {
  } else if (OB_FAIL(tmp_alloc_ptr->init())) {
    allocator_.free(tmp_alloc_ptr);
    TRANS_LOG(WARN, "init alloc ptr failed", K(ret), KPC(alloc_ptr), KPC(this));
  } else {
    alloc_ptr = tmp_alloc_ptr;
    TRANS_LOG(INFO, "alloc a new log cb pool", K(ret), K(log_cb_mem_size), KP(alloc_ptr),
              KPC(alloc_ptr));
  }

  return ret;
}

int ObTxLogCbPoolMgr::free_log_cb_pool_(ObTxLogCbPool *&free_ptr, const int64_t wait_timeout)
{
  int ret = OB_SUCCESS;

  const int64_t start_time = ObTimeUtility::fast_current_time();
  int64_t gc_pool_count = 0;
  ObTxLogCbPool *gc_pool_ptr = nullptr;

  if (OB_NOT_NULL(free_ptr)) {
    // lock pool_list
    pool_list_.remove(free_ptr);
    if (!gc_pool_list_.add_last(free_ptr)) {
      gc_pool_ptr = free_ptr;
      free_ptr->unlink();
      TRANS_LOG(ERROR, "push into gc_pool_list failed", K(ret), KPC(free_ptr),
                KP(free_ptr->get_prev()), KP(free_ptr->get_next()));
    } else {
      TRANS_LOG(INFO, "move the free_pool into gc_list", K(ret), KPC(free_ptr));
    }

    free_ptr = nullptr;
  }

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(gc_pool_ptr) && gc_pool_list_.get_size() > 0) {
      gc_pool_ptr = gc_pool_list_.get_last();
    }

    if (OB_NOT_NULL(gc_pool_ptr)) {
      for (ObTxLogCbPool* save = gc_pool_ptr->get_next();
           OB_SUCC(ret) && gc_pool_ptr != gc_pool_list_.get_header();
           gc_pool_ptr = save, save = save->get_next()) {

        while (!gc_pool_ptr->can_be_freed() && OB_SUCC(ret)) {
          if (ObTimeUtility::fast_current_time() - start_time >= wait_timeout) {
            ret = OB_EAGAIN;
            TRANS_LOG(INFO, "wait the ref of a free_log_cb until timeout", K(ret), K(start_time),
                      K(wait_timeout), KPC(free_ptr));
          } else {
            ob_usleep(1 * 1000);
            if (REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
              TRANS_LOG(INFO, "wait the tmp_ref of a log cb pool", K(ret), KPC(free_ptr));
            }
          }
        }

        if (OB_SUCC(ret) && OB_NOT_NULL(gc_pool_ptr)) {
          TRANS_LOG(INFO, "free a log cb pool", K(ret), KP(gc_pool_ptr), KPC(gc_pool_ptr));
          if (gc_pool_ptr->get_next() != nullptr || gc_pool_ptr->get_prev() != nullptr) {
            gc_pool_list_.remove(gc_pool_ptr);
          }
          gc_pool_ptr->~ObTxLogCbPool();
          (void)allocator_.free(gc_pool_ptr);
          gc_pool_count++;
        }
      }
    }
  }

  if (gc_pool_count == 0) {
    print_gc_pool_list_();
  }

  return ret;
}

void ObTxLogCbPoolMgr::print_gc_pool_list_()
{
  int ret = OB_SUCCESS;
  const int64_t MAX_PRINT_CNT = 10;
  int64_t print_index = 0;
  if (gc_pool_list_.get_size() > 0) {
    DLIST_FOREACH(cur, gc_pool_list_)
    {
      if (print_index > MAX_PRINT_CNT) {
        break;
      }
      TRANS_LOG(INFO, "print the gc pool", K(ret), K(print_index), K(gc_pool_list_.get_size()),
                KPC(cur));
      print_index++;
    }
  }
}

int ObTxLogCbPoolMgr::iter_idle_pool_(ObTxLogCbPoolRefGuard &ref_guard)
{
  int ret = OB_SUCCESS;

  ObTxLogCbPool *pool_ptr = nullptr;

  if (!ref_guard.is_valid()) {
    // do nothing
  } else {
    if (ref_guard.get_pool_ptr()->contain_idle_log_group()) {
      // do nothing
      TRANS_LOG(DEBUG, "the log cb pool has already been idle", K(ret), K(ref_guard));
    } else {
      ref_guard.reset();
    }
  }

  if (!ref_guard.is_valid()) {
    SpinRLockGuard guard(pool_list_rw_lock_);
    ObTxLogCbPool *tmp_idle_pool_ptr = ATOMIC_LOAD(&idle_pool_ptr_);
    ObTxLogCbPool *origin_idle_pool_ptr = tmp_idle_pool_ptr;

    if (!pool_list_.is_empty() && OB_ISNULL(tmp_idle_pool_ptr)) {
      tmp_idle_pool_ptr = pool_list_.get_first();
    } else if (pool_list_.is_empty()) {
      tmp_idle_pool_ptr = nullptr;
    }
    for (int i = 0; i < pool_list_.get_size() && OB_NOT_NULL(tmp_idle_pool_ptr)
                    && !tmp_idle_pool_ptr->contain_idle_log_group();
         i++) {
      tmp_idle_pool_ptr = tmp_idle_pool_ptr->get_next();
      if (tmp_idle_pool_ptr == pool_list_.get_header()) {
        tmp_idle_pool_ptr = tmp_idle_pool_ptr->get_next();
      }
    }
    if (OB_ISNULL(tmp_idle_pool_ptr) || !tmp_idle_pool_ptr->contain_idle_log_group()) {
      ret = OB_TX_NOLOGCB;
      TRANS_LOG(WARN, "no idle log cb group", K(ret), KPC(tmp_idle_pool_ptr));
    } else {
      ref_guard.set_pool_ptr(tmp_idle_pool_ptr);
      ATOMIC_BCAS(&idle_pool_ptr_, origin_idle_pool_ptr, tmp_idle_pool_ptr);
    }
  }

  TRANS_LOG(DEBUG, "iter a idle pool ", K(ret), K(ref_guard), K(pool_list_.get_size()),
            KPC(idle_pool_ptr_));

  return ret;
}

int ObTxLogCbPoolMgr::check_sync_size_increased_(int64_t &expand_cnt,
                                                 int64_t &sync_size_increased_cnt)
{
  int ret = OB_SUCCESS;

  expand_cnt = 0;
  sync_size_increased_cnt = 0;
  int64_t last_expand_his_index = -1;

  for (int i = 0; i < MAX_SYNC_SIZE_HISTORY_RECORD_SIZE * 2; i = i + 2) {
    if (i > 2 && last_expand_his_index > 0) {
      if (sync_size_history_[i] > sync_size_history_[last_expand_his_index - 1]) {
        sync_size_increased_cnt++;
      }
    }
    if (sync_size_history_[i + 1] == SyncSizeHistoryFlag::EXPAND) {
      expand_cnt = expand_cnt + 1;
      last_expand_his_index = i + 1;
    }
  }

  return ret;
}

int ObTxLogCbPoolMgr::push_back_sync_size_history_(const int64_t sync_size,
                                                   SyncSizeHistoryFlag his_flag)
{
  int ret = OB_SUCCESS;

  for (int i = 0; i < (MAX_SYNC_SIZE_HISTORY_RECORD_SIZE - 1) * 2; i++) {
    sync_size_history_[i] = sync_size_history_[i + 2];
  }
  sync_size_history_[(MAX_SYNC_SIZE_HISTORY_RECORD_SIZE - 1) * 2] = sync_size;
  sync_size_history_[(MAX_SYNC_SIZE_HISTORY_RECORD_SIZE - 1) * 2 + 1] = his_flag;

  return ret;
}

void ObTxLogCbPoolMgr::clear_sync_size_history_()
{
  SpinWLockGuard w_lock_guard(sync_size_his_lock_);
  memset(sync_size_history_, 0, sizeof(int64_t) * MAX_SYNC_SIZE_HISTORY_RECORD_SIZE * 2);
}

int ObTxLogCbPoolMgr::print_sync_size_history_()
{
  int ret = OB_SUCCESS;

  const int64_t PRINT_BUF_LEN = 2048;
  char sync_size_his_print_buf[PRINT_BUF_LEN];
  memset(sync_size_his_print_buf, 0, PRINT_BUF_LEN);
  int64_t pos = 0;

  for (int i = 0; OB_SUCC(ret) && i < MAX_SYNC_SIZE_HISTORY_RECORD_SIZE * 2; i = i + 2) {
    if (OB_FAIL(::oceanbase::common::databuff_printf(
            sync_size_his_print_buf, PRINT_BUF_LEN, pos, "| Sync<%ld> | %s ", sync_size_history_[i],
            sync_size_his_to_str(sync_size_history_[i + 1])))) {
      TRANS_LOG(WARN, "printf sync size history item failed", K(ret), K(pos), K(i),
                K(sync_size_history_[i]), K(sync_size_history_[i + 1]));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(::oceanbase::common::databuff_printf(sync_size_his_print_buf, PRINT_BUF_LEN, pos,
                                                     "%s", "|"))) {
      TRANS_LOG(WARN, "printf sync size history separator failed", K(ret), K(pos));
    }
  }

  _TRANS_LOG(INFO, "[LogCbPool Adjust] print sync size history <ls_id:%ld> : { %s }", ls_id_.id(),
             sync_size_his_print_buf);

  return ret;
}

int ObTxLogCbPoolMgr::cal_expected_log_cb_pool_cnt_(int64_t &expected_pool_cnt)
{
  int ret = OB_SUCCESS;

  expected_pool_cnt = -1;

  if (OB_SUCC(ret)) {
    SpinRLockGuard guard(pool_list_rw_lock_);
    DLIST_FOREACH(cur_pool, pool_list_) {}
  }

  return ret;
}

} // namespace transaction

} // namespace oceanbase
