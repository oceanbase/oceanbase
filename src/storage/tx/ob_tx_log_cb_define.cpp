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

#include "storage/tx/ob_trans_part_ctx.h"
#include "storage/tx/ob_tx_log_cb_define.h"

namespace oceanbase
{
namespace transaction
{

// const int64_t ObTxLogCbGroup::MAX_LOG_CB_COUNT_IN_GROUP = 4;
const int64_t ObTxLogCbGroup::FREEZE_LOG_CB_INDEX = MAX_LOG_CB_COUNT_IN_GROUP - 1;

// const int64_t ObTxLogCbPool::MAX_LOG_CB_GROUP_COUNT_IN_POOL = 1024;
const int64_t ObTxLogCbGroup::RESERVED_LOG_CB_GROUP_NO = INT64_MAX;

/**************************************************
 * ObTxLogCbGroup
 **************************************************/

int ObTxLogCbGroup::init(const int64_t group_no)
{
  int ret = OB_SUCCESS;

  if (is_inited()) {
    if (get_group_no() != ObTxLogCbGroup::RESERVED_LOG_CB_GROUP_NO) {
      ret = OB_INIT_TWICE;
      TRANS_LOG(WARN, "The LogCbGroup is inited", K(ret), KPC(this));
    }
  } else {
    ATOMIC_STORE(&group_no_, group_no);
  }

  return ret;
}

int ObTxLogCbGroup::occupy_by_tx(ObPartTransCtx *tx_ctx)
{
  int ret = OB_SUCCESS;

  //return err_code => log_cb_group_leak
  if (OB_ISNULL(tx_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(ERROR, "invalid argument", K(ret), KPC(tx_ctx));
  } else if (is_occupied()) {
    ret = OB_NEED_WAIT;
    TRANS_LOG(ERROR, "the log cb group is occupied now", K(ret), KPC(tx_ctx), KPC(this));
  } else if (OB_FAIL(check_and_reset_log_cbs(false /*skip_check*/))) {
    TRANS_LOG(ERROR, "reset log cbs failed", K(ret), KPC(tx_ctx), KPC(this));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < MAX_LOG_CB_COUNT_IN_GROUP; i++) {
      // log_cbs_[i].reset();
      if (OB_FAIL(log_cbs_[i].init(this))) {
        TRANS_LOG(WARN, "log cb init failed", KR(ret), K(log_cbs_[i]), KPC(this));
      }
    }
    if (OB_SUCC(ret)) {
      tx_id_ = tx_ctx->get_trans_id();
      tx_ctx_ = tx_ctx;
      ATOMIC_STORE(&occupy_ts_, ObTimeUtility::fast_current_time());
    }
  }

  return ret;
}

int ObTxLogCbGroup::check_and_reset_log_cbs(const bool skip_check)
{
  int ret = OB_SUCCESS;

  for (int i = 0; OB_SUCC(ret) && i < MAX_LOG_CB_COUNT_IN_GROUP; i++) {
    if (log_cbs_[i].is_busy()) {
      ret = OB_NEED_WAIT;
      TRANS_LOG(INFO, "the log cb is logging, need wait", K(ret), K(log_cbs_[i]));
    } else {
      // log_cbs_[i].reset();
    }
  }

  if (OB_SUCC(ret) || skip_check) {
    force_reset_();
  }

  return ret;
}

int ObTxLogCbGroup::append_into_free_list(ObPartTransCtx *tx_ctx)
{
  int ret = OB_SUCCESS;

  if (!is_occupied()) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "the log cb group  is not occupied by tx", K(ret), KPC(this));
  } else if (tx_ctx->get_trans_id() != tx_id_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "unexpected tx owner", K(ret), KPC(this));
  } else {
    // append into free_list
  }

  return ret;
}

/**************************************************
 * ObTxLogCbPool
 **************************************************/
int ObTxLogCbPool::init()
{
  int ret = OB_SUCCESS;

  if (ATOMIC_LOAD(&is_inited_)) {
    ret = OB_INIT_TWICE;
    TRANS_LOG(WARN, "init twice", K(ret), KPC(this));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < MAX_LOG_CB_GROUP_COUNT_IN_POOL; i++) {
      if (OB_FAIL(group_pool_[i].init(i))) {
        TRANS_LOG(WARN, "init group pool failed", K(ret), K(i), K(group_pool_[i]));
      }
    }

    TRANS_LOG(INFO, "init log cb pool successfully", K(ret), KPC(this));
  }

  return ret;
}

int ObTxLogCbPool::acquire_log_cb_group(ObTxLogCbGroup *&group_ptr)
{
  int ret = OB_SUCCESS;

  int64_t cur_free_group_no = 0;
  int64_t prev_free_group_no = 0;
  ObTxLogCbGroup *tmp_group_ptr = nullptr;
  if (OB_SUCC(ret)) {
    do {
      prev_free_group_no = cur_free_group_no;
      cur_free_group_no =
          ATOMIC_VCAS(&max_used_group_no_, cur_free_group_no, cur_free_group_no + 1);

      if (cur_free_group_no >= MAX_LOG_CB_GROUP_COUNT_IN_POOL) {
        ret = OB_TX_NOLOGCB;
        TRANS_LOG(INFO, "none free log_cb_group in pool", K(ret), K(cur_free_group_no),
                  K(prev_free_group_no), KPC(this));
      } else if (cur_free_group_no != prev_free_group_no) {
        ret = OB_EAGAIN;
        TRANS_LOG(DEBUG, "retry to update max_used_group_no_", K(ret), K(cur_free_group_no),
                  K(prev_free_group_no), KPC(this));
      } else {
        ret = OB_SUCCESS;
        tmp_group_ptr = &group_pool_[cur_free_group_no];
        TRANS_LOG(DEBUG, "acquire a new log cb group successfully", K(ret), K(cur_free_group_no),
                  K(prev_free_group_no), KPC(tmp_group_ptr), KPC(this));
      }
    } while (OB_EAGAIN == ret);
  }

  if (OB_TX_NOLOGCB == ret) {
    SpinWLockGuard free_list_guard(free_list_lock_);
    if (!free_group_list_.is_empty()) {
      if (OB_ISNULL(tmp_group_ptr = free_group_list_.remove_first())) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "remove the group_ptr failed without a empty list", K(ret),
                  K(*tmp_group_ptr), KPC(this));
        tmp_group_ptr = nullptr;
      } else {
        ret = OB_SUCCESS;
        cur_free_group_no = tmp_group_ptr->get_group_no();
        TRANS_LOG(INFO, "reuse a log group in the pool", K(ret), K(*tmp_group_ptr), KPC(this));
      }
    }
  }

  if (OB_SUCC(ret) && tmp_group_ptr != nullptr) {
    if (cur_free_group_no < 0 || cur_free_group_no >= MAX_LOG_CB_GROUP_COUNT_IN_POOL
        || cur_free_group_no != tmp_group_ptr->get_group_no()) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "unexpected free_group", K(ret), K(cur_free_group_no), K(*tmp_group_ptr),
                KPC(this));
      // may be leak
      tmp_group_ptr = nullptr;
    } else {
      group_ptr = tmp_group_ptr;
      stat_.occupy_group();
      TRANS_LOG(DEBUG, "acquire a free log_cb_group successfully", K(ret), KP(tmp_group_ptr),
                KPC(group_ptr), K(cur_free_group_no), KPC(this));
    }
  }

  return ret;
}

int ObTxLogCbPool::free_log_cb_group(ObTxLogCbGroup *group_ptr)
{
  int ret = OB_SUCCESS;

  ObTransID occupy_tx_id;
  int64_t start_occupy_ts = 0;
  if (OB_ISNULL(group_ptr) || group_ptr < group_pool_
      || group_ptr > group_pool_ + (MAX_LOG_CB_GROUP_COUNT_IN_POOL - 1)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "the group_ptr does not belong to the current pool", K(ret), KPC(group_ptr),
              KP(group_pool_), K(sizeof(ObTxLogCbGroup)));
  } else if (OB_FALSE_IT(start_occupy_ts = group_ptr->get_occupy_ts())) {
    // do nothing
  } else if (OB_FALSE_IT(occupy_tx_id = group_ptr->get_trans_id())) {
    // do nothing
  } else if (OB_FAIL(group_ptr->check_and_reset_log_cbs(false))) {
    TRANS_LOG(WARN, "There are some busy log cbs in the group", K(ret), KPC(group_ptr), KPC(this));
  } else {
    stat_.revert_group(ObTimeUtility::fast_current_time() - start_occupy_ts, start_occupy_ts);

    SpinWLockGuard free_list_guard(free_list_lock_);
    if (false == (free_group_list_.add_last(group_ptr))) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "insert into free group list failed", K(ret), KPC(group_ptr), KPC(this));
    }
  }

  if (OB_SUCC(ret)) {
    TRANS_LOG(INFO, "[Log Cb Group Life] return a log cb group", K(ret), K(occupy_tx_id),
              K(start_occupy_ts), KPC(group_ptr), KPC(this));
  }

  return ret;
}

#define ESTIMATED_TIME(USED_TIME, USED_SIZE, USING_SIZE, LAST_USING_SIZE, TOTAL_TIME,              \
                       ESTIMATED_USING_TIME)                                                       \
  {                                                                                                \
    int64_t aver_used_time = TOTAL_TIME;                                                           \
    if (USED_TIME > 0 && USED_SIZE > 0) {                                                          \
      aver_used_time = USED_TIME / USED_SIZE;                                                      \
    }                                                                                              \
    ESTIMATED_USING_TIME = USED_TIME + USING_SIZE * aver_used_time + LAST_USING_SIZE * TOTAL_TIME; \
  }

int ObTxLogCbPool::cal_estimated_log_cb_pool_stat(const int64_t start_estimated_time,
                                                  int64_t &estimated_synced_size,
                                                  int64_t &estimated_synced_time,
                                                  int64_t &estimated_occupied_count,
                                                  int64_t &estimated_occupied_time,
                                                  int64_t &syncing_size,
                                                  int64_t &occupying_size,
                                                  int64_t &barrier_ts)
{
  int ret = OB_SUCCESS;

  estimated_synced_size = -1;
  estimated_synced_time = -1;
  estimated_occupied_count = -1;
  estimated_occupied_time = -1;

  syncing_size = -1;
  occupying_size = -1;

  SpinRLockGuard guard(stat_.rw_lock_);

  const int64_t total_interval = start_estimated_time - stat_.barrier_ts_;

  if (OB_SUCC(ret)) {
    estimated_synced_size = stat_.synced_size_;
    estimated_occupied_count = stat_.syncing_size_;
  }

  if (OB_SUCC(ret))
    ESTIMATED_TIME(stat_.synced_time_, stat_.synced_size_, stat_.syncing_size_,
                   stat_.barrier_syncing_size_, total_interval, estimated_synced_time)

  if (OB_SUCC(ret))
    ESTIMATED_TIME(stat_.occupied_time_, stat_.occupied_count_, stat_.occupying_count_,
                   stat_.barrier_occupying_count_, total_interval, estimated_occupied_time)

  if (OB_SUCC(ret)) {
    syncing_size = stat_.syncing_size_;
    occupying_size = stat_.occupying_count_;
  }

  if (OB_SUCC(ret)) {
    barrier_ts = stat_.barrier_ts_;
    TRANS_LOG(INFO, "[LogCbPool Adjust] Print LogCbPool Stat", K(ret), KPC(this),
              K(start_estimated_time), K(estimated_synced_size), K(estimated_synced_time),
              K(estimated_occupied_count), K(estimated_occupied_time), K(syncing_size),
              K(occupying_size), K(stat_));

    stat_.reuse_without_lock(start_estimated_time);
  }

  return ret;
}

bool ObTxLogCbPool::can_be_freed()
{
  bool need_free = false;

  SpinRLockGuard free_list_guard(free_list_lock_);
  need_free = (free_group_list_.get_size() == max_used_group_no_) && get_tmp_ref() <= 0
              && all_log_group_idle();

  return need_free;
}

int ObTxLogCbPool::start_syncing_with_stat(ObTxLogCbGroup *group_ptr, const int64_t sync_size)
{
  int ret = OB_SUCCESS;

  ObTxLogCbPool *log_pool_ptr = nullptr;

  if (OB_FAIL(infer_pool_addr_(group_ptr, log_pool_ptr))) {
    if (OB_NO_NEED_UPDATE != ret) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "infer a pool's addr failed", K(ret), KP(group_ptr), KPC(group_ptr));
    }
  } else {
    log_pool_ptr->stat_.log_submitted(sync_size);
    TRANS_LOG(DEBUG, "before start syncing stat", K(ret), K(sync_size), KP(log_pool_ptr),
              KPC(group_ptr), K(log_pool_ptr->stat_));
  }

  TRANS_LOG(DEBUG, "start syncing stat", K(ret), K(sync_size), KP(log_pool_ptr), KPC(group_ptr));

  return ret;
}

int ObTxLogCbPool::finish_syncing_with_stat(ObTxLogCbGroup *group_ptr,
                                            const int64_t sync_size,
                                            const int64_t sync_time,
                                            const int64_t submit_ts)
{
  int ret = OB_SUCCESS;

  ObTxLogCbPool *log_pool_ptr = nullptr;

  if (OB_FAIL(infer_pool_addr_(group_ptr, log_pool_ptr))) {
    if (OB_NO_NEED_UPDATE != ret) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "infer a pool's addr failed", K(ret), KP(group_ptr), KPC(group_ptr));
    }
  } else {
    log_pool_ptr->stat_.log_synced(sync_size, sync_time, submit_ts);
    TRANS_LOG(DEBUG, "before finish syncing stat", K(ret), K(sync_size), KP(log_pool_ptr),
              KPC(group_ptr), K(log_pool_ptr->stat_));
  }

  TRANS_LOG(DEBUG, "finish syncing stat", K(ret), K(sync_size), KP(log_pool_ptr), KPC(group_ptr));

  return ret;
}

int ObTxLogCbPool::free_target_group(ObTxLogCbGroup *group_ptr)
{
  int ret = OB_SUCCESS;

  ObTxLogCbPool *log_pool_ptr = nullptr;
  if (OB_FAIL(infer_pool_addr_(group_ptr, log_pool_ptr))) {
    if (OB_NO_NEED_UPDATE != ret) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "infer a pool's addr failed", K(ret), KP(group_ptr), KPC(group_ptr));
    }
  } else if (OB_FAIL(log_pool_ptr->free_log_cb_group(group_ptr))) {
    TRANS_LOG(WARN, "free log cb group failed", K(ret), KPC(group_ptr), KPC(log_pool_ptr));
  }

  return ret;
}

int ObTxLogCbPool::infer_pool_addr_(ObTxLogCbGroup *group_ptr, ObTxLogCbPool *&log_pool_ptr)
{
  int ret = OB_SUCCESS;

  // ObTxLogCbPool *log_pool_ptr = nullptr;Vgc

  if (OB_ISNULL(group_ptr)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid group ptr", K(ret), KP(group_ptr));
  } else if (group_ptr->is_reserved()) {
    ret = OB_NO_NEED_UPDATE;
    TRANS_LOG(DEBUG, "NO log cb pool for a reserved group", K(ret), KPC(group_ptr));
  } else {
    char *tmp_pool_ptr = (char *)(group_ptr);
    const int64_t total_bytes = sizeof(ObTxLogCbPool);
    const int64_t group_no = group_ptr->get_group_no();
    tmp_pool_ptr =
        tmp_pool_ptr
        - (total_bytes - (MAX_LOG_CB_GROUP_COUNT_IN_POOL - group_no) * sizeof(ObTxLogCbGroup));

    TRANS_LOG(DEBUG, "INFER RES", K(ret), K(total_bytes), K(group_no), KP(group_ptr),
              KP(tmp_pool_ptr), K(sizeof(ObTxLogCbGroup)));
    log_pool_ptr = (ObTxLogCbPool *)(tmp_pool_ptr);

    if (OB_UNLIKELY((log_pool_ptr->group_pool_ + group_no) != group_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "invalid log cb group ptr", K(ret), K(sizeof(ObTxLogCbPool)),
                K(sizeof(ObTxLogCbGroup)), KP(log_pool_ptr->group_pool_ + group_no), KP(group_ptr),
                KPC(group_ptr), K(total_bytes), KPC(log_pool_ptr));
      log_pool_ptr = nullptr;
    }
  }

  return ret;
}

} // namespace transaction
} // namespace oceanbase
