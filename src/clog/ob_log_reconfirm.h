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

#ifndef OCEANBASE_CLOG_OB_LOG_RECONFIRM_
#define OCEANBASE_CLOG_OB_LOG_RECONFIRM_
#include "lib/lock/ob_spin_lock.h"
#include "ob_log_define.h"
#include "ob_log_task.h"
#include "share/allocator/ob_tenant_mutil_allocator_mgr.h"

namespace oceanbase {
namespace unittest {
class ObLogReconfirmAccessor;
class ReconfirmStateAccessorForTest;
}  // namespace unittest
namespace common {
class ObILogAllocator;
}
namespace clog {
class ObILogSWForReconfirm;
class ObILogStateMgrForReconfirm;
class ObILogMembershipMgr;
class ObLogCascadingMgr;
class ObILogEngine;
class ObLogEntry;

struct ObReconfirmLogInfo {
public:
  ObReconfirmLogInfo()
      : leader_ts_(common::OB_INVALID_TIMESTAMP),
        confirmed_log_ts_(common::OB_INVALID_TIMESTAMP),
        ack_list_(),
        status_map_(),
        log_entry_()
  {}
  ~ObReconfirmLogInfo()
  {}
  int add_server(const common::ObAddr& server)
  {
    return ack_list_.add_server(server);
  }
  int remove_server(const common::ObAddr& server)
  {
    return ack_list_.remove_server(server);
  }
  int64_t get_member_number() const
  {
    return ack_list_.get_member_number();
  }
  void free_log_entry_buf()
  {
    if (NULL != log_entry_.get_buf()) {
      TMA_MGR_INSTANCE.free_log_entry_buf(const_cast<char*>(log_entry_.get_buf()));
      log_entry_.reset();
    }
  }
  int deep_copy_log(const ObLogEntry& log_entry)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(log_entry.deep_copy_to(log_entry_))) {
      CLOG_LOG(WARN, "deep copy log_entry failed", K(ret), K(log_entry));
    }
    return ret;
  }
  const ObLogEntry& get_log_entry() const
  {
    return log_entry_;
  }
  void set_map(const int64_t idx)
  {
    return status_map_.set_map(idx);
  }
  bool test_map(const int64_t idx) const
  {
    return status_map_.test_map(idx);
  }
  void set_leader_ts(const int64_t leader_ts)
  {
    leader_ts_ = leader_ts;
  }
  void set_confirmed_log_ts(const int64_t submit_ts)
  {
    confirmed_log_ts_ = submit_ts;
  }
  int64_t get_leader_ts() const
  {
    return leader_ts_;
  }
  int64_t get_confirmed_log_ts() const
  {
    return confirmed_log_ts_;
  }
  void reset()
  {
    leader_ts_ = OB_INVALID_TIMESTAMP;
    confirmed_log_ts_ = OB_INVALID_TIMESTAMP;
    ack_list_.reset();
    status_map_.reset_all();
    if (NULL != log_entry_.get_buf()) {
      // free log_entry's memory
      TMA_MGR_INSTANCE.free_log_entry_buf(const_cast<char*>(log_entry_.get_buf()));
    }
    log_entry_.reset();
  }
  TO_STRING_KV(K_(leader_ts), K_(ack_list), K_(status_map), K_(log_entry));

private:
  int64_t leader_ts_;
  // The confirmed log's submit_timestamp is maintained by this field and used to advance the last_ts
  int64_t confirmed_log_ts_;
  common::ObMemberList ack_list_;
  ObLogSimpleBitMap status_map_;
  ObLogEntry log_entry_;
};

class ObReconfirmLogInfoArray {
public:
  ObReconfirmLogInfoArray() : start_id_(OB_INVALID_ID), alloc_mgr_(NULL), array_ptr_(NULL), is_inited_(false)
  {}
  ~ObReconfirmLogInfoArray()
  {}
  int init(const uint64_t start_id, common::ObILogAllocator* alloc_mgr);
  void destroy();
  ObReconfirmLogInfo* get_log_info(const int64_t idx);
  int reuse(const uint64_t new_start_id);
  uint64_t get_start_id() const
  {
    return start_id_;
  }
  uint64_t get_end_id() const
  {
    return start_id_ + RECONFIRM_LOG_ARRAY_LENGTH;
  }

private:
  uint64_t start_id_;
  common::ObILogAllocator* alloc_mgr_;
  ObReconfirmLogInfo* array_ptr_;
  bool is_inited_;
};

class ObILogReconfirm {
public:
  ObILogReconfirm()
  {}
  virtual ~ObILogReconfirm()
  {}

public:
  virtual int reconfirm() = 0;
  virtual bool need_start_up() = 0;
  virtual int receive_log(const ObLogEntry& log_entry, const common::ObAddr& server) = 0;
  virtual int receive_max_log_id(const common::ObAddr& server, const uint64_t log_id, const int64_t max_log_ts) = 0;
  virtual void reset() = 0;
};

class ObLogReconfirm : public ObILogReconfirm {
  friend class unittest::ReconfirmStateAccessorForTest;

public:
  ObLogReconfirm();
  virtual ~ObLogReconfirm()
  {}

public:
  int init(ObILogSWForReconfirm* sw, ObILogStateMgrForReconfirm* state_mgr, ObILogMembershipMgr* mm,
      ObLogCascadingMgr* cascading_mgr, ObILogEngine* log_engine, common::ObILogAllocator* alloc_mgr,
      const common::ObPartitionKey& partition_key, const common::ObAddr& self,
      const int64_t last_replay_submit_timestamp);
  virtual int reconfirm();
  virtual bool need_start_up();
  virtual int receive_log(const ObLogEntry& log_entry, const common::ObAddr& server);
  virtual int receive_max_log_id(const common::ObAddr& server, const uint64_t log_id, const int64_t max_log_ts);
  virtual void reset();
  virtual common::ObProposalID get_proposal_id() const
  {
    return new_proposal_id_;
  }
  virtual int handle_standby_prepare_resp(const ObAddr& server, const ObProposalID& proposal_id,
      const uint64_t ms_log_id, const int64_t membership_version, const ObMemberList& member_list);
  virtual uint64_t get_next_id() const
  {
    return ATOMIC_LOAD(&next_id_);
  }

private:
  enum State {
    INITED = 0,
    FLUSHING_PREPARE_LOG = 1,
    FETCH_MAX_LSN = 2,
    RECONFIRMING = 3,
    START_WORKING = 4,
    FINISHED = 5,
  };
  // The mark index of the majority status recorded in the bitmap
  static const int64_t MAJORITY_TAG_BIT = 0;
  // The status that the log has been confirmed is recorded in the mark index of the bitmap
  static const int64_t CONFIRMED_TAG_BIT = 1;

private:
  // generate new proposal_id and flush it to disk
  int init_reconfirm_();
  // check whether new proposal_id has been flushed to disk
  bool is_new_proposal_id_flushed_();
  // fetch max_log_id from all followers
  int fetch_max_log_id_();
  int try_set_majority_ack_tag_of_max_log_id_();
  // get min_log_id for reconfirming
  int get_start_id_and_leader_ts_();
  // check all clog has been flushed
  int check_log_flushed_(bool& is_all_flushed, uint64_t& no_flushed_log_id);
  // alloc memory for reconfirm, copy local log to map if exist, or generate nop log
  int prepare_log_map_();
  int reconfirm_log_();
  bool need_fetch_log_();
  int try_fetch_log_();
  int try_update_nop_or_truncate_timestamp(ObLogEntryHeader& header);
  int confirm_log_();
  int try_filter_invalid_log_();
  int generate_nop_log_(const uint64_t log_id, const int64_t idx);
  void try_update_nop_freeze_version_(ObLogEntryHeader& header);
  void try_update_membership_status_(const ObLogEntry& log_entry);
  void clear_memory_();
  bool is_previous_leader_(const common::ObAddr& server) const;
  int init_log_info_range_(const uint64_t range_start_id);
  int try_update_last_ts_(const int64_t log_ts);
  int try_update_failover_truncate_log_id_(ObLogEntryHeader* header);

private:
  static const int64_t BUF_SIZE = 2048;
  static const int64_t RECEIVE_PREVIOUS_NEXT_REPLAY_LOG_INFO_INTERVAL = 1 * 1000 * 1000;  // 1s
  static const int64_t REPEATED_FETCH_LOG_INTERVAL = 1 * 1000 * 1000;                     // 1s
  State state_;
  common::ObProposalID new_proposal_id_;
  uint64_t max_flushed_id_;
  uint64_t start_id_;
  uint64_t next_id_;
  int64_t leader_ts_;
  uint64_t last_fetched_id_;
  int64_t last_fetched_log_ts_;
  int64_t last_ts_;  // submit timestamp of last log before start working
  ObAckList max_log_ack_list_;
  ObLogSimpleBitMap max_log_ack_map_;
  ObSimpleMemberList curr_member_list_;
  int64_t majority_cnt_;
  ObReconfirmLogInfoArray log_info_array_;

  ObILogSWForReconfirm* sw_;
  ObILogStateMgrForReconfirm* state_mgr_;
  ObILogMembershipMgr* mm_;
  ObLogCascadingMgr* cascading_mgr_;
  ObILogEngine* log_engine_;
  common::ObILogAllocator* alloc_mgr_;

  common::ObSpinLock lock_;
  common::ObPartitionKey partition_key_;
  common::ObAddr self_;
  int64_t fetch_max_log_id_ts_;
  int64_t last_log_range_switch_ts_;   // log_info_range reuse time
  int64_t last_push_renew_ms_log_ts_;  // record renew_ms_log's send time
  int64_t last_renew_sync_standby_loc_ts_;
  uint64_t failover_truncate_log_id_;
  int64_t max_membership_version_;
  bool is_standby_reconfirm_;
  bool receive_previous_max_log_ts_;
  bool is_inited_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogReconfirm);
};
}  // namespace clog
}  // namespace oceanbase
#endif  //_OCEANBASE_CLOG_OB_LOG_RECONFIRM_H_
