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

#ifndef _OCEANBASE_CLOG_OB_LOG_TASK_
#define _OCEANBASE_CLOG_OB_LOG_TASK_

#include "lib/lock/ob_latch.h"
#include "lib/objectpool/ob_concurrency_objpool.h"
#include "ob_log_define.h"
#include "ob_log_entry.h"
#include "ob_log_ext_ring_buffer.h"
#include "ob_log_type.h"
#include "ob_simple_member_list.h"

namespace oceanbase {
namespace common {
class ObTraceProfile;
class ObILogAllocator;
}  // namespace common
namespace clog {
class ObISubmitLogCb;
class ObILogTaskCallBack;
class ObLogSimpleBitMap {
public:
  ObLogSimpleBitMap() : val_(0)
  {}
  ~ObLogSimpleBitMap()
  {}
  void reset_all();
  void reset_map(const int64_t idx);
  void set_map(const int64_t idx);
  bool test_map(const int64_t idx) const;
  void reset_map_unsafe(const int64_t idx);
  void set_map_unsafe(const int64_t idx);
  bool test_map_unsafe(const int64_t idx) const;
  bool test_and_set(const int64_t idx);
  TO_STRING_KV(K_(val));

private:
  uint16_t val_;
};

class ObLogTask : public ObILogExtRingBufferData {
  enum STATE_BIT_INDEX {
    MAJORITY_FINISHED = 0,
    LOCAL_FLUSHED = 1,
    IS_CONFIRMED = 2,
    NEED_REPLAY = 3,
    ON_SUCCESS_CB_CALLED = 4,
    ON_FINISHED_CB_CALLED = 5,
    SUBMIT_LOG_EXIST = 6,
    SUBMIT_LOG_BODY_EXIST = 7,
    INDEX_LOG_SUBMITTED = 8,
    CONFIRMED_INFO_EXIST = 9,
    PRE_INDEX_LOG_SUBMITTED = 10,
    BATCH_COMMITTED = 11,
    IS_PINNED = 12,
    IS_TRANS_LOG = 13,
    STANDBY_MAJORITY_FINISHED = 14,
    ALREADY_SEND_TO_STANDBY = 15,  // whether it has been send to standby cluster
  };

public:
  ObLogTask();
  virtual ~ObLogTask();
  int init(ObISubmitLogCb* submit_cb, const int64_t replica_num, const bool need_replay);
  int quick_init(const int64_t replica_num, const ObLogEntry& log_entry, const bool need_copy);
  void lock() const
  {
    lock_.wrlock(common::ObLatchIds::CLOG_TASK_LOCK);
  }
  void unlock() const
  {
    lock_.unlock();
  }
  int set_log(const ObLogEntryHeader& header, const char* buff, const bool need_copy);
  int reset_log();
  int set_replica_num(const int64_t replica_num);
  void set_log_confirmed();
  void set_confirmed_info(const ObConfirmedInfo& confirmed_info);
  void set_flush_local_finished();
  void set_on_success_cb_called();
  void set_on_finished_cb_called();
  int try_set_need_replay(const bool need_replay);
  void set_index_log_submitted();
  // just for unittest
  void set_submit_log_exist()
  {
    state_map_.set_map(SUBMIT_LOG_EXIST);
  }
  // try set majority tag, return true only once
  bool try_set_majority_finished();
  bool try_pre_index_log_submitted();
  void reset_pre_index_log_submitted();
  void set_batch_committed();
  void set_pinned();
  void reset_pinned();
  void set_standby_majority_finished();
  bool is_standby_majority_finished() const;
  bool is_local_majority_flushed() const;
  bool is_flush_local_finished() const;
  bool is_log_confirmed() const;
  bool is_majority_finished() const;
  bool is_on_success_cb_called() const;
  bool is_on_finished_cb_called() const;
  bool is_submit_log_exist() const;
  bool is_submit_log_body_exist() const;
  bool is_index_log_submitted() const;
  bool is_confirmed_info_exist() const;
  bool need_replay() const;
  bool is_batch_committed() const;
  bool is_pinned() const;
  bool is_trans_log() const;
  bool is_already_send_to_standby() const;

  // Clear tags of ack and majority, keep other state, called when replica's role changes
  // Because the ack corresponds to the proposal_id, after the proposal_id changes, the ack tag must be cleared
  // Other states, such as LOCAL_FLUSHED, etc., do not change with the change of proposal_id, so they cannot be reset
  int reset_state(const bool need_reset_confirmed_info);
  int reset_all_state();
  void reset_log_cursor();
  int ack_log(const common::ObAddr& server);
  int submit_log_succ_cb(const common::ObPartitionKey& pkey, const uint64_t log_id, const bool batch_committed,
      const bool batch_first_participant);
  int submit_log_finished_cb(const common::ObPartitionKey& pkey, const uint64_t log_id);
  int set_log_cursor(const ObLogCursor& cursor);
  int set_log_cursor_without_stat(const ObLogCursor& cursor);
  void set_submit_cb(ObISubmitLogCb* cb);
  void set_already_send_to_standby();
  void reset_submit_cb();
  ObLogType get_log_type() const;
  common::ObProposalID get_proposal_id() const;
  char* get_log_buf() const;
  int32_t get_log_buf_len() const;
  int64_t get_generation_timestamp() const;
  int64_t get_next_replay_log_ts() const;
  int64_t get_submit_timestamp() const;
  int64_t get_data_checksum() const;
  int64_t get_epoch_id() const;
  int64_t get_accum_checksum() const;
  const ObLogCursor& get_log_cursor() const;
  bool is_log_cursor_valid() const;
  bool is_checksum_verified(const int64_t data_checksum) const;
  // common::ObTraceProfile *get_trace_profile() {return trace_profile_;}
  // int report_trace();

  TO_STRING_KV(K(log_type_), K(proposal_id_), K(log_buf_len_), K_(generation_timestamp), K(submit_timestamp_),
      K(next_replay_log_ts_), K_(data_checksum), K(epoch_id_), K(accum_checksum_), K_(state_map), K_(ack_list),
      KP_(submit_cb), K_(majority_cnt), K_(log_cursor));

public:
  virtual void destroy();
  virtual bool can_be_removed();
  virtual bool can_overwrite(const ObILogExtRingBufferData* log_task);

private:
  int log_deep_copy_to_(const ObLogEntry& log_entry, const bool need_copy);

private:
  // The first 4 variables are sorted in order to save space overhead caused by alignment
  //
  // 1 bytes
  uint8_t log_type_;
  // 1 bytes, used to determine whether a single log meets the majority condition
  // Because replica_num will change, and there is no additional member group information in ObLogTask
  // It is necessary to use 1 bytes to record the majority_cnt
  int8_t majority_cnt_;
  // 2 bytes, used to store LogTask's state
  ObLogSimpleBitMap state_map_;
  // 4 bytes, length of log body
  int32_t log_buf_len_;
  // 32 bytes
  common::ObProposalID proposal_id_;
  // 8 bytes
  char* log_buf_;
  // 8 bytes
  int64_t generation_timestamp_;
  // 8 bytes
  int64_t submit_timestamp_;
  // 8 bytes, used for record next_replay_log_ts of total log_entry:
  // for unencrypted OB_LOG_AGGR log: next_replay_log_ts is min_log_submit_ts of all aggregated logs
  // for other logs: equal with submit_timestamp_
  int64_t next_replay_log_ts_;
  // ObConfirmedInfo, 16 bytes
  int64_t epoch_id_;
  int64_t data_checksum_;
  int64_t accum_checksum_;
  // 24 bytes, used to count the number of ack received
  // Since ack msg may be repeated, we need to record each specific server information;
  // Consider supporting up to 7 members, after excluding the leader, still need 3 int64_t, see ObAckList implementation
  ObAckList ack_list_;
  // 8 bytes, used to callback to transaction
  ObISubmitLogCb* submit_cb_;
  // 12 bytes
  ObLogCursor log_cursor_;
  // 4 bytes
  mutable common::ObLatch lock_;
  // common::ObTraceProfile *trace_profile_;
  // int64_t flushed_begin_time_;

  DISALLOW_COPY_AND_ASSIGN(ObLogTask);
};
}  // namespace clog
}  // namespace oceanbase
#endif  // _OCEANBASE_CLOG_OB_LOG_TASK_
