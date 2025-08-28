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

#ifndef OCEANBASE_TRANSACTION_OB_TX_LOG_CB_DEFINE_HEADER
#define OCEANBASE_TRANSACTION_OB_TX_LOG_CB_DEFINE_HEADER

#include "storage/tx/ob_trans_submit_log_cb.h"

namespace oceanbase
{

namespace transaction
{

class ObTxLogCbGroup : public common::ObDLinkBase<ObTxLogCbGroup>
{
public:
  static const int64_t MAX_LOG_CB_COUNT_IN_GROUP = 4;
  static const int64_t ACTIVE_TX_DEFAULT_LOG_GROUP_COUNT = 3;
  static const int64_t FREEZE_LOG_CB_INDEX;
  static const int64_t RESERVED_LOG_CB_GROUP_NO;

public:
  ObTxLogCbGroup()
  {
    group_no_ = -1;
    force_reset_();
  }

  ObTxLogCbGroup(const bool is_reserved)
  {
    group_no_ = -1;
    force_reset_();
    if (is_reserved) {
      group_no_ = ObTxLogCbGroup::RESERVED_LOG_CB_GROUP_NO;
    }
  }

  ~ObTxLogCbGroup() {}

  int init(const int64_t group_no);
  bool is_inited() { return ATOMIC_LOAD(&group_no_) > 0; }
  bool is_occupied() const { return tx_id_.get_id() > 0; }
  bool is_reserved() const { return get_group_no() == RESERVED_LOG_CB_GROUP_NO; }

  int occupy_by_tx(ObPartTransCtx *tx_ctx);

  int check_and_reset_log_cbs(const bool skip_check);

  int append_into_free_list(ObPartTransCtx *tx_ctx);

  int64_t get_group_no() const { return ATOMIC_LOAD(&group_no_); }
  int64_t get_occupy_ts() const { return occupy_ts_; }
  ObPartTransCtx *get_tx_ctx() const { return tx_ctx_; }
  const ObTransID get_trans_id() const { return tx_id_; }

  ObTxLogCb *get_log_cb_by_index(const int64_t index)
  {
    if (index < 0 || index >= MAX_LOG_CB_COUNT_IN_GROUP) {
      return nullptr;
    } else {
      return log_cbs_ + index;
    }
  }

  TO_STRING_KV(KP(this),
               K(group_no_),
               K(tx_id_),
               KP(tx_ctx_),
               KP(log_cbs_),
               K(sizeof(ObTxLogCb)),
               K(occupy_ts_));

private:
  void force_reset_()
  {
    tx_id_ = 0;
    tx_ctx_ = nullptr;
    for (int i = 0; i < MAX_LOG_CB_COUNT_IN_GROUP; i++) {
      log_cbs_[i].reset();
      TRANS_LOG_RET(DEBUG, OB_SUCCESS, "reset log cb for group", K(ret), K(i), K(log_cbs_[i]),
                    KPC(this));
    }
    occupy_ts_ = 0;
  }

private:
  int64_t group_no_;
  ObTransID tx_id_;
  ObPartTransCtx *tx_ctx_;
  ObTxLogCb log_cbs_[MAX_LOG_CB_COUNT_IN_GROUP];

  int64_t occupy_ts_;
};

typedef common::ObDList<ObTxLogCbGroup> TxLogCbGroupList;

class ObTxLogCbPool : public common::ObDLinkBase<ObTxLogCbPool>
{
public:
  static const int64_t MAX_LOG_CB_GROUP_COUNT_IN_POOL = 1024;

  friend class ObTxLogCbPoolRefGuard;

public:
  ObTxLogCbPool() { force_reset_(); }

  int init();

  TO_STRING_KV(KP(this),
               K(is_inited_),
               K(max_used_group_no_),
               K(free_group_list_.get_size()),
               K(tmp_ref_),
               K(stat_),
               K(sizeof(ObTxLogCbGroup)),
               K(sizeof(ObTxLogCbPool)));

  int acquire_log_cb_group(ObTxLogCbGroup *&group_ptr);
  int free_log_cb_group(ObTxLogCbGroup *group_ptr);

  int cal_estimated_log_cb_pool_stat(const int64_t start_estimated_time,
                                     int64_t &estimated_synced_size,
                                     int64_t &estimated_synced_time,
                                     int64_t &estimated_occupied_count,
                                     int64_t &estimated_occupied_time,
                                     int64_t &syncing_size,
                                     int64_t &occupying_size,
                                     int64_t &barrier_ts);

  bool contain_idle_log_group()
  {
    return ATOMIC_LOAD(&stat_.occupying_count_) + ATOMIC_LOAD(&stat_.barrier_occupying_count_)
           < MAX_LOG_CB_GROUP_COUNT_IN_POOL;
  }

  bool all_log_group_idle()
  {
    return ATOMIC_LOAD(&stat_.occupying_count_) + ATOMIC_LOAD(&stat_.barrier_occupying_count_) == 0; 
  }

  int64_t get_tmp_ref() { return ATOMIC_LOAD(&tmp_ref_); }

  bool can_be_freed();

private:
  void inc_ref_() { ATOMIC_INC(&tmp_ref_); }
  void dec_ref_() { ATOMIC_DEC(&tmp_ref_); }

public:

  static int start_syncing_with_stat(ObTxLogCbGroup *group_ptr, const int64_t sync_size);
  static int finish_syncing_with_stat(ObTxLogCbGroup *group_ptr,
                                      const int64_t sync_size,
                                      const int64_t sync_time,
                                      const int64_t submit_ts);

  static int free_target_group(ObTxLogCbGroup *group_ptr);

private:
  static int infer_pool_addr_(ObTxLogCbGroup *group_ptr, ObTxLogCbPool * & log_cb_pool);

private:
  void force_reset_()
  {
    is_inited_ = false;
    max_used_group_no_ = 0;
    free_group_list_.reset();
    tmp_ref_ = 0;

    stat_.reset();
  }

  struct LogCbPoolStat
  {
    int64_t occupying_count_;
    int64_t occupied_count_;
    int64_t occupied_time_;
    int64_t barrier_occupying_count_;

    int64_t syncing_size_;
    int64_t synced_size_;
    int64_t synced_time_;
    int64_t barrier_syncing_size_;

    int64_t barrier_ts_;

    SpinRWLock rw_lock_;

    TO_STRING_KV(K(occupying_count_),
                 K(occupied_count_),
                 K(occupied_time_),
                 K(barrier_occupying_count_),
                 K(syncing_size_),
                 K(synced_size_),
                 K(synced_time_),
                 K(barrier_syncing_size_),
                 K(barrier_ts_));

    void reset()
    {
      SpinWLockGuard guard(rw_lock_);
      occupying_count_ = 0;
      occupied_count_ = 0;
      occupied_time_ = 0;
      barrier_occupying_count_ = 0;

      syncing_size_ = 0;
      synced_size_ = 0;
      synced_time_ = 0;
      barrier_syncing_size_ = 0;

      barrier_ts_ = ObTimeUtility::fast_current_time();
    }

    void reuse_without_lock(const int64_t cur_time)
    {
      barrier_occupying_count_ += occupying_count_;
      barrier_syncing_size_ += syncing_size_;

      barrier_ts_ = cur_time;

      occupying_count_ = 0;
      occupied_time_ = 0;
      occupied_count_ = 0;

      syncing_size_ = 0;
      synced_time_ = 0;
      synced_size_ = 0;
    }

    void reuse(const int64_t cur_time)
    {
      SpinWLockGuard guard(rw_lock_);
      reuse_without_lock(cur_time);
    }

    void occupy_group()
    {
      SpinWLockGuard guard(rw_lock_);
      occupying_count_++;
    }

    void revert_group(const int64_t group_occupied_time, const int64_t start_occupying_time)
    {
      SpinWLockGuard guard(rw_lock_);
      if (start_occupying_time > barrier_ts_) {
        occupying_count_--;
      } else {
        barrier_occupying_count_--;
      }
      occupied_count_++;
      occupied_time_ += group_occupied_time;
    }

    void log_submitted(const int64_t cb_syncing_size)
    {
      SpinWLockGuard guard(rw_lock_);
      syncing_size_ += cb_syncing_size;
    }

    void log_synced(const int64_t cb_synced_size, const int64_t cb_synced_time, const int64_t cb_submit_ts)
    {
      SpinWLockGuard guard(rw_lock_);
      if (cb_submit_ts > barrier_ts_) {
        syncing_size_ -= cb_synced_size;
      } else {
        barrier_syncing_size_ -= cb_synced_size;
      }
      synced_size_ += cb_synced_size;
      synced_time_ += cb_synced_time;
    }
  };

private:
  bool is_inited_;

  int64_t max_used_group_no_;
  TxLogCbGroupList free_group_list_;
  SpinRWLock free_list_lock_;

  int64_t tmp_ref_;

  LogCbPoolStat stat_;

  ObTxLogCbGroup group_pool_[MAX_LOG_CB_GROUP_COUNT_IN_POOL];
};

class ObTxLogCbPoolRefGuard
{
public:
  ObTxLogCbPoolRefGuard() : pool_(nullptr) {}

  ~ObTxLogCbPoolRefGuard() { reset(); }

  void set_pool_ptr(ObTxLogCbPool *ptr)
  {
    reset();
    ptr->inc_ref_();
    pool_ = ptr;
  }

  bool is_valid() { return OB_NOT_NULL(pool_); }

  ObTxLogCbPool *get_pool_ptr() { return pool_; }

  void reset()
  {
    if (OB_NOT_NULL(pool_)) {
      pool_->dec_ref_();
      pool_ = nullptr;
    }
  }

  TO_STRING_KV(KPC(pool_));

private:
  ObTxLogCbPool *pool_;
};

} // namespace transaction

} // namespace oceanbase

#endif
