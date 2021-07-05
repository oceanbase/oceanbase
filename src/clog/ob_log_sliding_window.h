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

#ifndef OCEANBASE_CLOG_OB_LOG_SLIDING_WINDOW_
#define OCEANBASE_CLOG_OB_LOG_SLIDING_WINDOW_

#include "lib/lock/ob_spin_lock.h"
#include "lib/objectpool/ob_concurrency_objpool.h"
#include "lib/atomic/atomic128.h"
#include "election/ob_election_time_def.h"  // for T_ST macro
#include "ob_log_common.h"
#include "ob_log_define.h"
#include "ob_log_ext_ring_buffer.h"
#include "ob_log_cascading_mgr.h"
#include "ob_max_log_meta_info.h"
#include "ob_log_flush_task.h"
#include "ob_log_req.h"

namespace oceanbase {
namespace common {
class ObBaseStorageInfo;
class ObILogAllocator;
}  // namespace common
namespace unittest {
class TestSlidingWindow;
}
namespace transaction {
class ObTransTraceLog;
}
namespace omt {
class ObTenantConfig;
}
namespace clog {
class ObILogIndex;
class ObILogEngine;
class ObLogReplayEngineWrapper;
class ObILogStateMgrForSW;
class ObILogChecksum;
class ObIFetchLogEngine;
class ObILogMembershipMgr;
class ObLogCascadingMgr;
class ObISubmitLogCb;
class ObLogEntryHeader;
class ObLogEntry;
class ObLogTask;
class ObConfirmedInfo;
class ObILogCallbackEngine;
class ObLogRestoreMgr;
class ObClogAggreRunnable;

enum ObFetchLogAttr {
  NO_DEMAND = 0,
  NEED_CONFIRMED,  // need log confirmed
  NEED_MAJORITY,   // need log majority
};

enum ObFetchLogExecuteType {
  TIMER_FETCH_LOG_EXECUTE_TYPE = 1,
  SLIDE_FETCH_LOG_EXECUTE_TYPE = 2,
  POINT_FETCH_LOG_EXECUTE_TYPE = 3,
};

class ObILogSWForCasMgr {
public:
  ObILogSWForCasMgr()
  {}
  virtual ~ObILogSWForCasMgr()
  {}

public:
  virtual int get_next_replay_log_timestamp(int64_t& next_replay_log_timestamp) const = 0;
  virtual void get_next_replay_log_id_info(uint64_t& next_log_id, int64_t& next_log_ts) const = 0;
  virtual uint64_t get_next_index_log_id() const = 0;
  virtual uint64_t get_start_id() const = 0;
};

class ObILogSWForStateMgr {
public:
  ObILogSWForStateMgr()
  {}
  virtual ~ObILogSWForStateMgr()
  {}

public:
  virtual uint64_t get_start_id() const = 0;
  virtual uint64_t get_max_log_id() const = 0;
  virtual int clean_log() = 0;
  virtual int get_log_task(const uint64_t log_id, ObLogTask*& log_task, const int64_t*& ref) const = 0;
  virtual int revert_log_task(const int64_t* ref) = 0;
  virtual void start_fetch_log_from_leader(bool& is_fetched) = 0;
  virtual int get_next_replay_log_timestamp(int64_t& next_replay_log_timestamp) const = 0;
  virtual uint64_t get_next_index_log_id() const = 0;
  virtual int64_t get_next_index_log_ts() = 0;
  virtual int leader_active() = 0;
  virtual int leader_takeover() = 0;
  virtual int leader_revoke() = 0;
  virtual void get_next_replay_log_id_info(uint64_t& next_log_id, int64_t& next_log_ts) const = 0;
  virtual bool is_fake_info_need_revoke(const uint64_t log_id, const int64_t current_time) = 0;
  virtual int restore_leader_try_confirm_log() = 0;
  virtual uint64_t get_max_confirmed_log_id() const = 0;
  virtual bool is_empty() const = 0;
};

class ObILogSWForReconfirm {
public:
  ObILogSWForReconfirm()
  {}
  virtual ~ObILogSWForReconfirm()
  {}

public:
  virtual uint64_t get_start_id() const = 0;
  virtual uint64_t get_max_log_id() const = 0;
  virtual int try_update_max_log_id(const uint64_t log_id) = 0;
  virtual int submit_log(const ObLogEntryHeader& header, const char* buff, ObISubmitLogCb* cb) = 0;
  virtual int64_t get_epoch_id() const = 0;
  virtual int get_log_task(const uint64_t log_id, ObLogTask*& log_task, const int64_t*& ref) const = 0;
  virtual int revert_log_task(const int64_t* ref) = 0;
  virtual int submit_replay_task(const bool need_async, bool& is_replayed, bool& is_replay_failed) = 0;
  virtual int try_update_submit_timestamp(const int64_t base_ts) = 0;
  virtual int64_t get_last_submit_timestamp() const = 0;
  virtual uint64_t get_max_confirmed_log_id() const = 0;
};

class ObILogSWForMS {
public:
  ObILogSWForMS()
  {}
  virtual ~ObILogSWForMS()
  {}

public:
  virtual uint64_t get_start_id() const = 0;
  virtual int submit_log(const ObLogEntryHeader& header, const char* buff, ObISubmitLogCb* cb) = 0;
  virtual int submit_log(const ObLogType& log_type, const char* buff, const int64_t size, const int64_t base_timestamp,
      const bool is_trans_log, ObISubmitLogCb* cb, uint64_t& log_id, int64_t& log_timestamp) = 0;
  virtual int append_disk_log(const ObLogEntry& log, const ObLogCursor& log_cursor, const int64_t accum_checksum,
      const bool batch_committed) = 0;
  virtual int receive_log(const ObLogEntry& log_entry, const common::ObAddr& server, const int64_t cluster_id,
      const ReceiveLogType type) = 0;
  virtual int receive_recovery_log(const ObLogEntry& log_entry, const bool is_confirmed, const int64_t accum_checksum,
      const bool is_batch_commited) = 0;
  virtual int alloc_log_id(const int64_t base_timestamp, uint64_t& log_id, int64_t& submit_timestamp) = 0;
  virtual uint64_t get_next_index_log_id() const = 0;
  virtual int64_t get_next_index_log_ts() = 0;
  virtual int do_fetch_log(const uint64_t start_id, const uint64_t end_id,
      const enum ObFetchLogExecuteType& fetch_log_execute_type, bool& is_fetched) = 0;
  virtual int set_log_confirmed(const uint64_t log_id, const bool batch_committed) = 0;
  virtual uint64_t get_max_log_id() const = 0;
};

struct LogIdTsPair {
  mutable common::ObSpinLock lock_;
  uint64_t log_id_;
  int64_t ts_;

  LogIdTsPair() : lock_(), log_id_(common::OB_INVALID_ID), ts_(common::OB_INVALID_TIMESTAMP)
  {}

  void set(uint64_t log_id, int64_t ts)
  {
    lib::ObLockGuard<common::ObSpinLock> guard(lock_);
    log_id_ = log_id;
    ts_ = ts;
  }

  void inc_update(const uint64_t log_id, const int64_t ts)
  {
    lib::ObLockGuard<common::ObSpinLock> guard(lock_);
    if (log_id > log_id_) {
      log_id_ = log_id;
      ts_ = ts;
    }
  }

  void get(uint64_t& log_id, int64_t& ts) const
  {
    lib::ObLockGuard<common::ObSpinLock> guard(lock_);
    log_id = log_id_;
    ts = ts_;
  }
  int64_t get_ts() const
  {
    lib::ObLockGuard<common::ObSpinLock> guard(lock_);
    return ts_;
  }
  int64_t get_log_id() const
  {
    lib::ObLockGuard<common::ObSpinLock> guard(lock_);
    return log_id_;
  }
};

// used for checking log sync in standby cluster
struct LeaderMaxLogInfo {
  int64_t switchover_epoch_;
  uint64_t max_log_id_;
  int64_t next_log_ts_;
  LeaderMaxLogInfo()
      : switchover_epoch_(common::OB_INVALID_TIMESTAMP),
        max_log_id_(common::OB_INVALID_ID),
        next_log_ts_(common::OB_INVALID_TIMESTAMP)
  {}
  ~LeaderMaxLogInfo()
  {}
  void reset()
  {
    switchover_epoch_ = common::OB_INVALID_TIMESTAMP;
    max_log_id_ = common::OB_INVALID_ID;
    next_log_ts_ = common::OB_INVALID_TIMESTAMP;
  }
  int64_t get_switchover_epoch() const
  {
    return switchover_epoch_;
  }
  void set_switchover_epoch(const int64_t switchover_epoch)
  {
    switchover_epoch_ = switchover_epoch;
  }
  uint64_t get_max_log_id() const
  {
    return max_log_id_;
  }
  void set_max_log_id(const uint64_t max_log_id)
  {
    max_log_id_ = max_log_id;
  }
  int64_t get_next_log_ts() const
  {
    return next_log_ts_;
  }
  void set_next_log_ts(const int64_t next_log_ts)
  {
    next_log_ts_ = next_log_ts;
  }
  TO_STRING_KV(K_(switchover_epoch), K_(max_log_id), K_(next_log_ts));
};

struct FakeAckInfo {
  FakeAckInfo() : server_(), receive_ts_(common::OB_INVALID_TIMESTAMP)
  {}
  ~FakeAckInfo()
  {}
  void set(const common::ObAddr& server, const int64_t receive_ts)
  {
    server_ = server;
    receive_ts_ = receive_ts;
  }
  void reset()
  {
    server_.reset();
    receive_ts_ = common::OB_INVALID_TIMESTAMP;
  }
  bool is_valid()
  {
    return server_.is_valid() && common::OB_INVALID_TIMESTAMP != receive_ts_;
  }

  common::ObAddr server_;
  int64_t receive_ts_;
};

class FakeAckInfoMgr {
public:
  FakeAckInfoMgr() : lock_()
  {
    reset();
  }
  ~FakeAckInfoMgr()
  {}
  void reset()
  {
    lib::ObLockGuard<common::ObSpinLock> guard(lock_);
    ATOMIC_STORE(&log_id_, common::OB_INVALID_ID);
    for (int64_t i = 0; i < common::OB_MAX_MEMBER_NUMBER; ++i) {
      ack_info_[i].reset();
    }
  }
  uint64_t get_log_id() const
  {
    return ATOMIC_LOAD(&log_id_);
  }
  void process_fake_ack_req(const uint64_t log_id, const common::ObAddr& server, const int64_t receive_ts)
  {
    lib::ObLockGuard<common::ObSpinLock> guard(lock_);
    if (log_id > ATOMIC_LOAD(&log_id_) || common::OB_INVALID_ID == ATOMIC_LOAD(&log_id_)) {
      // log_id not match, inc update
      for (int64_t i = 0; i < common::OB_MAX_MEMBER_NUMBER; ++i) {
        ack_info_[i].reset();
      }
      ATOMIC_STORE(&log_id_, log_id);
    }
    if (log_id == ATOMIC_LOAD(&log_id_)) {
      bool insert_succ = false;
      for (int64_t i = 0; !insert_succ && i < common::OB_MAX_MEMBER_NUMBER; ++i) {
        if (ack_info_[i].server_ == server) {
          // entry exist, inc update
          if (ack_info_[i].receive_ts_ < receive_ts) {
            ack_info_[i].receive_ts_ = receive_ts;
          }
          insert_succ = true;
        } else if (!ack_info_[i].is_valid()) {
          // not exist, insert into first empty pos
          ack_info_[i].set(server, receive_ts);
          insert_succ = true;
        } else {
          // do nothing
        }
      }
    }
  }
  bool check_fake_info_need_revoke(const uint64_t log_id, const int64_t current_time, const common::ObAddr& self,
      const common::ObMemberList& member_list, const int64_t replica_num)
  {
    bool need_revoke = false;
    lib::ObLockGuard<common::ObSpinLock> guard(lock_);
    if (log_id != ATOMIC_LOAD(&log_id_)) {
      // followers are healthy
      need_revoke = true;
    } else {
      int64_t valid_cnt = 0;
      for (int64_t i = 0; i < common::OB_MAX_MEMBER_NUMBER; ++i) {
        if (!ack_info_[i].is_valid() || ack_info_[i].server_ == self) {
          // skip self or invalid entry
        } else if (current_time - ack_info_[i].receive_ts_ >= FAKE_ACK_MSG_VALID_TIME) {
          // ack is expired, skip
        } else if (member_list.contains(ack_info_[i].server_)) {
          valid_cnt++;
        }
      }
      if ((replica_num - valid_cnt) < (replica_num / 2 + 1)) {
        // healthy replicas do not reach majority
        // revoke is meaningless
        need_revoke = false;
      } else {
        need_revoke = true;
      }
    }
    return need_revoke;
  }

private:
  mutable common::ObSpinLock lock_;
  uint64_t log_id_;
  FakeAckInfo ack_info_[common::OB_MAX_MEMBER_NUMBER];
};

class ObLogSlidingWindow : public ObILogSWForCasMgr,
                           public ObILogSWForStateMgr,
                           public ObILogSWForReconfirm,
                           public ObILogSWForMS,
                           public ObILogTaskCallBack {
  friend class unittest::TestSlidingWindow;

public:
  ObLogSlidingWindow();
  virtual ~ObLogSlidingWindow()
  {
    destroy();
  }

public:
  int init(ObLogReplayEngineWrapper* replay_engine, ObILogEngine* log_engine, ObILogStateMgrForSW* state_mgr,
      ObILogMembershipMgr* mm, ObLogCascadingMgr* cascading_mgr, storage::ObPartitionService* partition_service,
      common::ObILogAllocator* alloc_mgr, ObILogChecksum* checksum, ObILogCallbackEngine* cb_engine,
      ObLogRestoreMgr* restore_mgr, const common::ObAddr& self, const common::ObPartitionKey& key,
      const int64_t epoch_id, const uint64_t last_replay_log_id, const int64_t last_submit_ts,
      const int64_t accum_checksum);
  void set_next_replay_log_id_info(const uint64_t log_id, const int64_t log_ts);
  void try_update_next_replay_log_info(const uint64_t log_id, const int64_t log_ts, const bool is_nop_log = false);
  void try_update_next_replay_log_info_on_leader(const int64_t log_ts);
  uint64_t get_start_id() const override;
  void get_max_log_id_info(uint64_t& max_log_id, int64_t& max_log_ts) const;
  uint64_t get_max_log_id() const override;
  int get_switchover_info(int64_t& switchover_epoch, uint64_t& leader_max_log_id, int64_t& leader_next_log_ts) const;
  uint64_t get_max_timestamp() const;
  int64_t get_epoch_id() const override;
  int try_update_max_log_id(const uint64_t log_id) override;
  int submit_log(const ObLogType& log_type, const char* buff, const int64_t size, const int64_t base_timestamp,
      const bool is_trans_log, ObISubmitLogCb* cb, uint64_t& log_id, int64_t& log_timestamp) override;
  int submit_aggre_log(ObAggreBuffer* buffer, const int64_t base_timestamp);
  int submit_log(const ObLogEntryHeader& header, const char* buff, ObISubmitLogCb* cb) override;
  int append_disk_log(const ObLogEntry& log, const ObLogCursor& log_cursor, const int64_t accum_checksum,
      const bool batch_committed) override;
  int receive_log(const ObLogEntry& log_entry, const common::ObAddr& server, const int64_t cluster_id,
      const ReceiveLogType type) override;
  int receive_recovery_log(const ObLogEntry& log_entry, const bool is_confirmed, const int64_t accum_checksum,
      const bool is_batch_commited) override;
  int ack_log(const uint64_t log_id, const common::ObAddr& server, bool& majority);
  int standby_ack_log(const uint64_t log_id, const ObAddr& server, bool& majority);
  int fake_ack_log(const uint64_t log_id, const common::ObAddr& server, const int64_t receive_ts);
  bool is_fake_info_need_revoke(const uint64_t log_id, const int64_t current_time) override;
  int restore_leader_try_confirm_log() override;
  void start_fetch_log_from_leader(bool& is_fetched) override;
  int get_log_task(const uint64_t log_id, ObLogTask*& log_task, const int64_t*& ref) const override;
  int check_left_bound_empty(bool& is_empty);
  int revert_log_task(const int64_t* ref) override;
  int get_log(const uint64_t log_id, const uint32_t log_attr, bool& log_confirmed, ObLogCursor& cursor,
      int64_t& accum_checksum, bool& batch_committed);
  int clean_log() override;
  int process_sync_standby_max_confirmed_id(const uint64_t standby_max_confirmed_id, const uint64_t reconfirm_next_id);
  int set_log_flushed_succ(const uint64_t log_id, const common::ObProposalID proposal_id, const ObLogCursor& log_cursor,
      const int64_t after_consume_timestamp, bool& majority);
  int set_log_confirmed(const uint64_t log_id, const bool batch_committed) override;
  int receive_confirmed_info(const uint64_t log_id, const ObConfirmedInfo& confirmed_info, const bool batch_committed);
  int majority_cb(const uint64_t log_id, const bool batch_committed, const bool batch_first_participant);
  inline void advance_leader_ts(const int64_t ts)
  {
    // safe to compare and swap non-atomically
    if (ATOMIC_LOAD(&leader_ts_) < ts) {
      ATOMIC_STORE(&leader_ts_, ts);
    }
  }
  int sliding_cb(const int64_t sn, const ObILogExtRingBufferData* data) override;
  uint64_t get_next_index_log_id() const override
  {
    return ATOMIC_LOAD(&next_index_log_id_);
  }
  int64_t get_next_index_log_ts() override
  {
    return ATOMIC_LOAD(&next_index_log_ts_);
  }
  int submit_replay_task(const bool need_async, bool& is_replayed, bool& is_replay_failed) override;
  void destroy();
  int alloc_log_id(const int64_t base_timestamp, uint64_t& log_id, int64_t& submit_timestamp) override;
  int get_next_timestamp(const uint64_t last_log_id, int64_t& res_ts);
  int get_next_served_log_info_by_next_replay_log_info(uint64_t& next_served_log_id, int64_t& next_served_log_ts);
  bool is_inited() const
  {
    return is_inited_;
  }
  bool is_empty() const override;
  int set_next_index_log_id(const uint64_t log_id, const int64_t accum_checksum);
  uint64_t get_max_confirmed_log_id() const override;
  bool check_can_receive_larger_log(const uint64_t log_id);
  int truncate_first_stage(const common::ObBaseStorageInfo& base_storage_info);
  int truncate_second_stage(const common::ObBaseStorageInfo& base_storage_info);
  int64_t get_last_submit_timestamp() const override
  {
    return last_replay_log_.get_ts();
  }
  void get_last_replay_log(uint64_t& log_id, int64_t& ts)
  {
    last_replay_log_.get(log_id, ts);
  }
  int get_next_replay_log_timestamp(int64_t& next_replay_log_timestamp) const override;
  void get_next_replay_log_id_info(uint64_t& next_log_id, int64_t& next_log_ts) const override;
  int follower_update_leader_next_log_info(const uint64_t leader_next_log_id, const int64_t leader_next_log_ts);
  int follower_update_leader_max_log_info(
      const int64_t switchover_epoch, const uint64_t leader_max_log_id, const int64_t leader_next_log_ts);
  void set_last_replay_log(const uint64_t last_replay_log_id, const int64_t last_submit_timestamp)
  {
    last_replay_log_.set(last_replay_log_id, last_submit_timestamp);
  }
  uint64_t get_last_replay_log_id() const
  {
    return last_replay_log_.get_log_id();
  }
  void set_saved_accum_checksum(const int64_t accum_checksum)
  {
    saved_accum_checksum_ = accum_checksum;
  }
  int64_t get_saved_accum_checksum() const
  {
    return saved_accum_checksum_;
  }
  int reset_next_log_ts(const common::ObBaseStorageInfo& base_storage_info);
  void record_last_update_next_replay_log_id_info_ts()
  {
    last_update_next_replay_log_id_info_ts_ = common::ObTimeUtility::current_time();
  }
  int64_t get_last_update_next_replay_log_id_info_ts() const
  {
    return last_update_next_replay_log_id_info_ts_;
  }
  bool can_submit_replay_task(const char* log_buf, const int64_t log_buf_len, int64_t& log_type) const;
  bool can_submit_aggre_replay_task() const;
  int do_fetch_log(const uint64_t start_id, const uint64_t end_id,
      const enum ObFetchLogExecuteType& fetch_log_execute_type, bool& is_fetched) override final;
  int backfill_log(const uint64_t log_id, const common::ObProposalID& proposal_id, const char* serialize_buff,
      const int64_t serialize_size, const ObLogCursor& log_cursor, const bool is_leader, ObISubmitLogCb* submit_cb);
  int backfill_confirmed(const uint64_t log_id, const bool batch_first_participant);
  int resubmit_log(const ObLogInfo& log_info, ObISubmitLogCb* cb);
  int reset_has_pop_task()
  {
    ATOMIC_STORE(&has_pop_task_, false);
    return common::OB_SUCCESS;
  }
  int leader_active() override;
  int leader_takeover() override;
  int leader_revoke() override;
  int get_replica_replay_type(ObReplicaReplayType& replay_type) const;
  // is_meta_log: log type that need been replayed by D replica and log replica
  int get_log_meta_info(uint64_t log_id, bool& is_meta_log, int64_t& log_ts, int64_t& next_replay_log_ts_for_rg,
      int64_t& accum_checksum, ObLogType& log_type) const;
  void destroy_aggre_buffer();
  uint64_t get_leader_max_unconfirmed_log_count();
  uint64_t get_follower_max_unconfirmed_log_count();
  void try_update_max_majority_log(const uint64_t log_id, const int64_t log_ts);
  void get_max_majority_log(uint64_t& log_id, int64_t& ts) const
  {
    max_majority_log_.get(log_id, ts);
  }
  void set_last_archive_checkpoint_log_id(const uint64_t log_id)
  {
    last_archive_checkpoint_log_id_ = log_id;
  }

  void set_last_archive_checkpoint_ts(const int64_t checkpoint_ts)
  {
    last_archive_checkpoint_ts_ = checkpoint_ts;
  }

  void set_last_update_archive_checkpoint_time(const int64_t ts)
  {
    last_update_archive_checkpoint_time_ = ts;
  }
  int64_t get_last_update_archive_checkpoint_time() const
  {
    return last_update_archive_checkpoint_time_;
  }
  int64_t get_last_archive_checkpoint_ts() const
  {
    return last_archive_checkpoint_ts_;
  }
  uint64_t get_last_archive_checkpoint_log_id() const
  {
    return last_archive_checkpoint_log_id_;
  }
  int check_if_all_log_replayed(bool& has_replayed) const;
  int try_freeze_aggre_buffer();

private:
  int alloc_log_id_ts_(const int64_t base_timestamp, uint64_t& log_id, int64_t& submit_timestamp);
  int alloc_log_id_ts_(
      const int64_t base_timestamp, const int64_t size, uint64_t& log_id, int64_t& submit_timestamp, int64_t& offset);
  int init_aggre_buffer_(const uint64_t start_id, const uint64_t tenant_id, const bool is_pg);
  int fill_aggre_buffer_(const uint64_t log_id, const int64_t offset, const char* data, const int64_t data_size,
      const int64_t submit_timestamp, ObISubmitLogCb* cb);
  int try_freeze_aggre_buffer_(const uint64_t log_id);
  int submit_freeze_aggre_buffer_task_(const uint64_t log_id);
  int submit_aggre_log_(ObAggreBuffer* buffer, const uint64_t log_id, const int64_t submit_timestamp);
  int try_update_submit_timestamp(const int64_t base_ts) override;
  bool is_confirm_match_(const uint64_t log_id, const int64_t log_data_checksum, const int64_t log_epoch_id,
      const int64_t confirmed_info_data_checksum, const int64_t confirmed_info_epoch_id);
  int receive_log_(const ObLogEntry& log_entry, const common::ObAddr& server, const int64_t cluster_id);
  void update_max_log_id_(const uint64_t log_id);
  int submit_to_sliding_window_(const ObLogEntryHeader& header, const char* buff, ObISubmitLogCb* cb,
      const bool need_replay, const bool send_slave, const common::ObAddr& server, const int64_t cluster_id,
      const bool is_confirmed, const int64_t accum_checksum, const bool is_batch_committed);
  int leader_submit_confirmed_info_(const uint64_t log_id, const ObLogTask* log_task, const int64_t accum_checksum);
  int standby_leader_transfer_confirmed_info_(const uint64_t log_id, const ObLogTask* log_task);
  int submit_confirmed_info_(
      const uint64_t log_id, const ObConfirmedInfo& confirmed_info, const bool is_leader, const bool batch_committed);
  int standby_submit_confirmed_info_(const uint64_t log_id, const ObProposalID& ms_proposal_id,
      const ObConfirmedInfo& confirmed_info, const bool is_leader);
  int generate_null_log_task_(const ObLogEntryHeader& header, bool need_replay, ObLogTask*& log_task);
  int quick_generate_log_task_(const ObLogEntry& log_entry, const bool need_copy, ObLogTask*& task);
  int generate_log_task_(
      const ObLogEntryHeader& header, const char* buff, const bool need_replay, const bool need_copy, ObLogTask*& task);
  int generate_log_task_(
      const ObConfirmedInfo& confirmed_info, const bool need_replay, const bool batch_committed, ObLogTask*& task);
  int need_update_log_task_(
      const ObLogEntryHeader& header, const char* buff, ObLogTask& task, bool& log_need_update, bool& need_send_ack);
  int update_log_task_(const ObLogEntryHeader& header, const char* buff, const bool need_copy, ObLogTask& task,
      bool& log_is_updated, bool& need_send_ack);
  int prepare_flush_task_(const ObLogEntryHeader& header, char* serialize_buff, const int64_t serialize_size,
      const common::ObAddr& server, const int64_t cluster_id, ObLogFlushTask*& flush_task);
  int submit_log_to_net_(const ObLogEntryHeader& header, const char* serialize_buff, const int64_t serialize_size,
      const bool is_log_majority);
  int submit_log_to_member_list_(
      const ObLogEntryHeader& header, const char* serialize_buff, const int64_t serialize_size);
  int submit_log_to_local_children_(
      const ObLogEntryHeader& header, const char* serialize_buff, const int64_t serialize_size);
  int submit_log_to_standby_children_(const ObLogEntryHeader& header, const char* serialize_buff,
      const int64_t serialize_size, const bool is_log_majority);
  int send_log_to_standby_cluster_(const uint64_t log_id, ObLogTask* log_task);
  int send_confirmed_info_to_standby_children_(const uint64_t log_id, ObLogTask* log_task);
  int follower_send_log_to_standby_children_(const uint64_t log_id, ObLogTask* log_task);
  int submit_confirmed_info_to_net_(
      const uint64_t log_id, const ObConfirmedInfo& confirmed_info, const bool batch_committed);
  int submit_confirmed_info_to_member_list_(
      const uint64_t log_id, const ObConfirmedInfo& confirmed_info, const bool batch_committed);
  int submit_confirmed_info_to_local_children_(
      const uint64_t log_id, const ObConfirmedInfo& confirmed_info, const bool batch_committed);
  int submit_confirmed_info_to_standby_children_(
      const uint64_t log_id, const ObConfirmedInfo& confirmed_info, const bool batch_committed);
  int follower_transfer_confirmed_info_to_net_(
      const uint64_t log_id, const ObConfirmedInfo& confirmed_info, const bool batch_committed);
  int append_disk_log_to_sliding_window_(const ObLogEntry& log_entry, const ObLogCursor& log_cursor,
      const int64_t accum_checksum, const bool batch_committed);
  bool is_freeze_log_(const char* log_buf, const int64_t log_buf_len, int64_t& log_type) const;
  bool is_offline_partition_log_(const char* log_buf, const int64_t log_buf_len, int64_t& log_type) const;
  bool is_change_pg_log_(const char* log_buf, const int64_t log_buf_len, int64_t& log_type) const;
  bool is_trans_log_(const char* log_buf, const int64_t log_buf_len, int64_t& log_type) const;
  int try_submit_replay_task_(const uint64_t log_id, const ObLogTask& log_task);
  int handle_first_index_log_(
      const uint64_t log_id, ObLogTask* log_task, const bool do_pop, bool& need_check_succeeding_log);
  int handle_succeeding_index_log_(const uint64_t log_id, const bool need_check_succeeding_log, const bool do_pop);

  int submit_index_log_(const uint64_t log_id, const ObLogTask* log_task, int64_t& accum_checksum);
  bool test_and_set_index_log_submitted_(ObLogTask* log_task);
  bool test_and_submit_index_log_(const uint64_t log_id, ObLogTask* log_task, int& ret);
  int try_submit_mc_success_cb_(const ObLogType& log_type, const uint64_t log_id, const char* log_buf,
      const int64_t log_buf_len, const common::ObProposalID& proposal_id);
  int get_end_log_id_(const uint64_t start_id, uint64_t& end_id);
  int get_slide_log_range_(const uint64_t start_id, uint64_t& ret_start_id, uint64_t& ret_end_id);
  int backfill_log_(const char* serialize_buff, const int64_t serialize_size, const ObLogCursor& log_cursor,
      const bool is_leader, ObISubmitLogCb* submit_cb);
  int backfill_confirmed_(const uint64_t log_id, const bool batch_first_participant);
  int resubmit_log_(const ObLogInfo& log_info, ObISubmitLogCb* cb);
  int generate_backfill_log_task_(const ObLogEntryHeader& header, const char* buff, const ObLogCursor& log_cursor,
      ObISubmitLogCb* submit_cb, const bool need_replay, const bool need_copy, const bool need_pinned,
      ObLogTask*& task);
  int get_log_submit_tstamp_from_task_(const uint64_t log_id, int64_t& log_tstamp);
  int check_pre_barrier_(ObLogType log_type) const;
  void* alloc_log_task_buf_();
  int need_replay_for_data_or_log_replica_(const bool is_trans_log, bool& need_replay) const;
  int send_standby_log_ack_(
      const ObAddr& server, const int64_t cluster_id, const uint64_t log_id, const ObProposalID& proposal_id);
  int try_refresh_unconfirmed_log_count_();
  int set_confirmed_info_without_lock_(
      const ObLogEntryHeader& header, const int64_t accum_checksum, const bool is_batch_committed, ObLogTask& log_task);
  bool can_transfer_confirmed_info_(const uint64_t log_id);
  bool can_set_log_confirmed_(const ObLogTask* log_task) const;
  bool is_primary_need_send_log_to_standby_(ObLogTask* log_task) const;
  bool is_mp_leader_waiting_standby_ack_(ObLogTask* log_task) const;
  bool is_follower_need_send_log_to_standby_(ObLogTask* log_task) const;
  bool is_standby_leader_need_fetch_log_(const uint64_t start_log_id);
  bool check_need_fetch_log_(const uint64_t start_log_id, bool& need_check_rebuild);
  int follower_check_need_rebuild_(const uint64_t start_log_id);

private:
  static const int64_t MAX_TIME_DIFF_BETWEEN_SERVER = T_ST;  // 200 ms
private:
  int64_t tenant_id_;
  ObILogStateMgrForSW* state_mgr_;
  ObLogReplayEngineWrapper* replay_engine_;
  ObILogEngine* log_engine_;
  ObILogMembershipMgr* mm_;
  ObLogCascadingMgr* cascading_mgr_;
  storage::ObPartitionService* partition_service_;
  common::ObILogAllocator* alloc_mgr_;
  ObILogChecksum* checksum_;
  ObILogCallbackEngine* cb_engine_;
  ObLogRestoreMgr* restore_mgr_;
  ObLogExtRingBuffer sw_;
  common::ObAddr self_;
  common::ObPartitionKey partition_key_;
  ObMaxLogMetaInfo max_log_meta_info_;
  struct types::uint128_t next_replay_log_id_info_;
  LogIdTsPair max_majority_log_;
  int64_t leader_ts_;
  int64_t saved_accum_checksum_;
  uint64_t next_index_log_id_;
  uint64_t scan_next_index_log_id_;
  uint64_t last_flushed_log_id_;
  int64_t next_index_log_ts_;
  mutable common::ObSpinLock switchover_info_lock_;  // protect leader_max_log_info_
  LeaderMaxLogInfo leader_max_log_info_;
  LogIdTsPair last_replay_log_;
  FakeAckInfoMgr fake_ack_info_mgr_;
  file_id_t last_slide_fid_;
  mutable int64_t check_can_receive_larger_log_warn_time_;
  mutable int64_t insert_log_try_again_warn_time_;
  mutable int64_t receive_confirmed_info_warn_time_;
  mutable int64_t get_end_log_id_warn_time_;
  mutable int64_t fetch_log_warn_time_;
  mutable int64_t update_log_task_log_time_;
  mutable int64_t sync_replica_reset_fetch_state_time_;
  // When calculating start_service_time during restart, it is necessary to filter out the partitons that have not
  // updated next_replay_log_id_info in a period of time Such partition may have been deleted, or it may take a long
  // time to execute rebuild, which will affect the recovery time of the system;
  int64_t last_update_next_replay_log_id_info_ts_;
  int64_t last_get_tenant_config_time_;
  bool has_pop_task_;
  // aggregate commit
  ObAggreBuffer* aggre_buffer_;
  int64_t aggre_buffer_cnt_;
  uint64_t next_submit_aggre_buffer_;
  uint64_t aggre_buffer_start_id_;
  uint64_t leader_max_unconfirmed_log_cnt_;

  uint64_t last_archive_checkpoint_log_id_;
  int64_t last_archive_checkpoint_ts_;  // default value is 0 rather than OB_INVALID_TIMESTAMP to guarantee
                                        // checkpoint_ts in archive_checkpoint log is larger than 0
  int64_t last_update_archive_checkpoint_time_;

  ObClogAggreRunnable* aggre_worker_;

  bool is_inited_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogSlidingWindow);
};

}  // namespace clog
}  // namespace oceanbase
#endif  // OCEANBASE_CLOG_OB_LOG_SLIDING_WINDOW_
