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

#ifndef OCEANBASE_CLOG_OB_LOG_MEMBERSHIP_TASK_MGR_
#define OCEANBASE_CLOG_OB_LOG_MEMBERSHIP_TASK_MGR_

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
#include "ob_log_task.h"

namespace oceanbase {
namespace common {
class ObILogAllocator;
}
namespace omt {
class ObTenantConfig;
}
namespace clog {
class ObILogEngine;
class ObILogStateMgrForSW;
class ObILogMembershipMgr;
class ObLogCascadingMgr;
class ObISubmitLogCb;
class ObLogEntryHeader;
class ObLogEntry;
class ObLogTask;
class ObConfirmedInfo;
class ObILogCallbackEngine;
class ObLogRestoreMgr;

// This class is used to maintain RenewMembershipLog task info for standby cluster.
struct RenewMsLogTask {
  int64_t log_id_;
  ObLogTask log_task_;
  ObSimpleMemberList ack_mlist_;
  RenewMsLogTask() : log_id_(OB_INVALID_ID), log_task_(), ack_mlist_()
  {}
  ~RenewMsLogTask()
  {
    reset();
  }
  void reset()
  {
    log_id_ = OB_INVALID_ID;
    log_task_.reset_log();
    log_task_.reset_state(false);
    ack_mlist_.reset();
  }
  int64_t get_log_id() const
  {
    return log_id_;
  }
  TO_STRING_KV(K_(log_id), K_(log_task), K_(ack_mlist));
};

class ObLogMembershipTaskMgr {
public:
  ObLogMembershipTaskMgr();
  virtual ~ObLogMembershipTaskMgr()
  {
    destroy();
  }
  int init(ObILogEngine* log_engine, ObILogStateMgrForSW* state_mgr, ObILogMembershipMgr* mm,
      ObLogCascadingMgr* cascading_mgr, ObILogCallbackEngine* cb_engine, ObLogRestoreMgr* restore_mgr,
      storage::ObPartitionService* partition_service, common::ObILogAllocator* alloc_mgr, const common::ObAddr& self,
      const common::ObPartitionKey& key);
  void destroy();

public:
  int submit_log(const ObRenewMembershipLog& renew_ms_log, const ObLogType& log_type, const char* buff,
      const int64_t size, const uint64_t log_id, const int64_t log_timestamp, const bool is_trans_log,
      ObISubmitLogCb* cb);
  int submit_log(
      const ObRenewMembershipLog& renew_ms_log, const ObLogEntryHeader& header, const char* buff, ObISubmitLogCb* cb);
  int receive_renew_ms_log(const ObLogEntry& log_entry, const ObRenewMembershipLog& renew_ms_log,
      const common::ObAddr& server, const int64_t cluster_id, const ReceiveLogType type);
  int ack_renew_ms_log(const uint64_t log_id, const int64_t submit_ts, const common::ObProposalID& proposal_id,
      const common::ObAddr& server, bool& majority);
  bool is_renew_ms_log_majority_success() const;
  bool is_renew_ms_log_majority_success(const uint64_t log_id, const int64_t log_ts) const;
  int set_renew_ms_log_flushed_succ(
      const uint64_t log_id, const int64_t submit_timestamp, const ObProposalID proposal_id, bool& majority);
  int set_renew_ms_log_confirmed(
      const uint64_t log_id, const int64_t submit_timestamp, const common::ObProposalID& proposal_id);
  int receive_renew_ms_log_confirmed_info(
      const uint64_t log_id, const common::ObProposalID& ms_proposal_id, const ObConfirmedInfo& confirmed_info);
  int renew_ms_log_majority_cb(
      const uint64_t log_id, const int64_t submit_timestamp, const common::ObProposalID& proposal_id);
  virtual int check_renew_ms_log_sync_state() const;
  int reset_renew_ms_log_task();

private:
  int submit_renew_ms_log_(const ObRenewMembershipLog& renew_ms_log, const ObLogEntryHeader& header, const char* buff,
      ObISubmitLogCb* cb, const bool need_replay, const bool send_slave, const common::ObAddr& server,
      const int64_t cluster_id);
  int need_update_renew_ms_log_task_(
      const ObLogEntryHeader& header, const char* buff, ObLogTask& task, bool& log_need_update, bool& need_send_ack);
  int submit_renew_ms_log_to_net_(
      const ObLogEntryHeader& header, const char* serialize_buff, const int64_t serialize_size);
  int submit_renew_ms_log_to_mlist_(
      const ObLogEntryHeader& header, const char* serialize_buff, const int64_t serialize_size);
  int submit_renew_ms_log_to_children_(
      const ObLogEntryHeader& header, const char* serialize_buff, const int64_t serialize_size);
  int submit_renew_ms_confirmed_info_to_net_(
      const uint64_t log_id, const ObProposalID& ms_proposal_id, const ObConfirmedInfo& confirmed_info);
  int submit_renew_ms_confirmed_info_to_mlist_(
      const uint64_t log_id, const common::ObProposalID& ms_proposal_id, const ObConfirmedInfo& confirmed_info);
  int submit_renew_ms_confirmed_info_to_children_(
      const uint64_t log_id, const common::ObProposalID& ms_proposal_id, const ObConfirmedInfo& confirmed_info);
  int submit_slog_flush_task_(const ObLogType log_type, const uint64_t log_id, const ObRenewMembershipLog& renew_ms_log,
      const ObAddr& server, const int64_t cluster_id);
  int submit_to_disk_(const ObLogEntryHeader& header, char* serialize_buff, const int64_t serialize_size,
      const ObAddr& server, const int64_t cluster_, bool& is_direct_submit);
  int standby_leader_submit_confirmed_info_(
      const uint64_t log_id, const ObLogTask& log_task, const int64_t accum_checksum);
  int submit_confirmed_info_(const uint64_t log_id, const ObProposalID& ms_proposal_id,
      const ObConfirmedInfo& confirmed_info, const bool is_leader);

private:
  ObILogEngine* log_engine_;
  ObILogStateMgrForSW* state_mgr_;
  ObILogMembershipMgr* mm_;
  ObLogCascadingMgr* cascading_mgr_;
  ObILogCallbackEngine* cb_engine_;
  ObLogRestoreMgr* restore_mgr_;
  storage::ObPartitionService* partition_service_;
  common::ObILogAllocator* alloc_mgr_;
  common::ObAddr self_;
  common::ObPartitionKey partition_key_;

  int64_t block_meta_len_;

  mutable common::ObSpinLock renew_ms_task_lock_;  // protect cur_renew_ms_task_
  RenewMsLogTask cur_renew_ms_task_;
  bool is_inited_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogMembershipTaskMgr);
};
}  // namespace clog
}  // namespace oceanbase
#endif  // OCEANBASE_CLOG_OB_LOG_MEMBERSHIP_TASK_MGR__
