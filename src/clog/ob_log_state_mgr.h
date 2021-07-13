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

#ifndef OCEANBASE_CLOG_OB_LOG_STATE_MGR_H_
#define OCEANBASE_CLOG_OB_LOG_STATE_MGR_H_

#include "ob_log_define.h"
#include "common/ob_partition_key.h"
#include "common/ob_role.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/objectpool/ob_concurrency_objpool.h"
#include "lib/trace/ob_trace_event.h"
#include "lib/utility/utility.h"
#include "share/ob_multi_cluster_util.h"
#include "observer/ob_server_struct.h"
#include "ob_log_cascading_mgr.h"

namespace oceanbase {
namespace common {
class ObILogAllocator;
}
namespace unittest {
class ObLogStateMgrTest;
}
namespace election {
class ObIElection;
}
namespace storage {
class ObPartitionService;
}
namespace share {
class ObIPSCb;
}

namespace clog {
class ObILogSWForStateMgr;
class ObILogReconfirm;
class ObILogEngine;
class ObILogMembershipMgr;
class ObLogCascadingMgr;
class ObLogReplayEngineWrapper;
class ObILogCallbackEngine;
class ObLogRestoreMgr;
class ObIOfsMemberListChecker;

enum ObCreatePlsType {
  CREATE_PARTITION,  // for create new partition
  ASSIGN_PARTITION,  // for restart
  ADD_PARTITION,     // for migration
};

class ObILogStateMgrForService {
public:
  ObILogStateMgrForService()
  {}
  virtual ~ObILogStateMgrForService()
  {}

public:
  virtual bool can_submit_log() const = 0;
  virtual bool can_submit_start_working_log() const = 0;
  virtual bool can_append_disk_log() const = 0;
  virtual bool can_receive_recovery_log() const = 0;
  virtual bool can_majority_cb(const common::ObProposalID& proposal_id) const = 0;
  virtual bool can_standby_majority_cb(const common::ObProposalID& proposal_id) const = 0;
  virtual bool can_majority_cb_for_renew_ms_log(const common::ObProposalID& ms_proposal_id) const = 0;
  virtual bool can_receive_log_ack(const common::ObProposalID& proposal_id) const = 0;
  virtual bool can_receive_renew_ms_log_ack(const common::ObProposalID& ms_proposal_id) const = 0;
  virtual bool can_send_renew_ms_log_ack(const common::ObProposalID& proposal_id) const = 0;
  virtual bool can_send_log_ack(const common::ObProposalID& proposal_id) const = 0;
  virtual bool can_send_standby_log_ack(const common::ObProposalID& proposal_id) const = 0;
  virtual bool can_receive_max_log_id(const common::ObProposalID& proposal_id) const = 0;
  virtual bool can_handle_standby_prepare_resp(const common::ObProposalID& proposal_id) const = 0;
  virtual bool can_get_log(const int64_t cluster_id) const = 0;
  virtual bool can_get_log_for_reconfirm(const common::ObProposalID& proposal_id) const = 0;
  virtual bool can_receive_log(const common::ObProposalID& proposal_id, const common::ObProposalID& proposal_id_in_log,
      const int64_t cluster_id) const = 0;
  virtual bool can_receive_renew_ms_log(const common::ObProposalID& proposal_id_in_req,
      const common::ObProposalID& proposal_id_in_log, const int64_t cluster_id) const = 0;
  virtual bool can_change_member() const = 0;
  virtual bool can_change_leader() const = 0;
  virtual bool can_get_leader_curr_member_list() const = 0;
  virtual bool can_handle_prepare_rqst(const common::ObProposalID& proposal_id, const int64_t cluster_id) const = 0;
  virtual bool can_handle_standby_prepare_rqst(
      const common::ObProposalID& proposal_id, const int64_t cluster_id) const = 0;
  virtual bool is_standby_leader_can_receive_log_id(const uint32_t log_id) const = 0;
  virtual bool can_receive_confirmed_info(const int64_t src_cluster_id) const = 0;

  virtual common::ObProposalID get_proposal_id() const = 0;
  virtual int switch_state(bool& need_retry) = 0;
  virtual bool check_sliding_window_state() = 0;
  virtual bool is_state_changed(bool& need_retry) = 0;
  virtual int set_scan_disk_log_finished() = 0;
  virtual int16_t get_state() const = 0;
  virtual common::ObRole get_role() const = 0;
  virtual common::ObVersion get_freeze_version() const = 0;
  virtual int handle_prepare_rqst(
      const common::ObProposalID& proposal_id, const common::ObAddr& new_leader, const int64_t cluster_id) = 0;
  virtual int handle_standby_prepare_rqst(
      const common::ObProposalID& proposal_id, const common::ObAddr& new_leader, const int64_t cluster_id) = 0;
  virtual int update_proposal_id(const common::ObProposalID& proposal_id) = 0;
  virtual int start_election() = 0;
  virtual int stop_election() = 0;
  virtual int set_election_leader(const common::ObAddr& leader, const int64_t lease_start) = 0;
  virtual const common::ObRegion& get_region() const = 0;
  virtual const common::ObIDC& get_idc() const = 0;
  virtual int set_region(const common::ObRegion& region, bool& is_changed) = 0;
  virtual int set_idc(const common::ObIDC& idc, bool& is_changed) = 0;
  virtual int try_update_leader_from_loc_cache() = 0;
  virtual int64_t get_self_cluster_id() const = 0;
  virtual int64_t get_leader_cluster_id() const = 0;
  virtual int get_election_valid_candidates(ObMemberList& valid_candidate_list) const = 0;
  virtual int get_election_leader(
      ObAddr& leader, int64_t& leader_epoch, common::ObTsWindows& changing_leader_windows) const = 0;
  virtual int get_election_leader(ObAddr& leader, int64_t& leader_epoch) const = 0;
  virtual bool is_replay_enabled() const = 0;
};

class ObILogStateMgrForSW {
public:
  ObILogStateMgrForSW()
  {}
  virtual ~ObILogStateMgrForSW()
  {}

public:
  virtual int16_t get_state() const = 0;
  virtual common::ObRole get_role() const = 0;
  virtual common::ObAddr get_leader() const = 0;
  virtual share::ObCascadMember get_leader_member() const = 0;
  virtual int get_cascad_leader(share::ObCascadMember& cascad_leader_member) const = 0;
  virtual int64_t get_leader_cluster_id() const = 0;
  virtual common::ObProposalID get_proposal_id() const = 0;
  virtual bool can_slide_sw() const = 0;
  virtual bool can_send_log_ack(const common::ObProposalID& proposal_id) const = 0;
  virtual bool is_offline() const = 0;
  virtual bool is_leader_active() const = 0;
  virtual bool is_standby_leader_active() const = 0;
  virtual bool is_follower_active() const = 0;
  virtual bool is_leader_taking_over() const = 0;
  virtual bool is_leader_reconfirm() const = 0;
  virtual const common::ObRegion& get_region() const = 0;
  virtual const common::ObIDC& get_idc() const = 0;
  virtual void reset_need_rebuild() = 0;
  virtual void report_start_id_trace(const uint64_t start_id) = 0;
  virtual bool is_cluster_allow_vote() const = 0;
  virtual bool is_can_elect_standby_leader() const = 0;
  virtual int64_t get_self_cluster_id() const = 0;
  virtual bool has_valid_member_list() const = 0;
  virtual bool can_receive_renew_ms_log_confirmed_info() const = 0;
  virtual bool can_send_renew_ms_log_ack(const common::ObProposalID& proposal_id) const = 0;
  virtual share::ObCascadMember get_primary_leader() const = 0;
  virtual int get_pls_epoch(int64_t& pls_epoch) const = 0;
  virtual void start_next_round_fetch() = 0;
  virtual void reset_fetch_state() = 0;
  virtual int try_reset_fetch_state(const uint64_t push_log_id) = 0;
  virtual int try_set_fetched_max_log_id(const uint64_t max_log_id) = 0;
  virtual uint64_t get_last_fetched_max_log_id() const = 0;
  virtual int64_t get_fetch_log_interval() const = 0;
  virtual uint64_t get_fetch_window_size() const = 0;
};

class ObILogStateMgrForCasMgr {
public:
  ObILogStateMgrForCasMgr()
  {}
  virtual ~ObILogStateMgrForCasMgr()
  {}

public:
  virtual common::ObAddr get_leader() const = 0;
  virtual int get_cascad_leader(share::ObCascadMember& cascad_leader_member) const = 0;
  virtual share::ObCascadMember get_leader_member() const = 0;
  virtual common::ObAddr get_primary_leader_addr() const = 0;
  virtual bool is_offline() const = 0;
  virtual common::ObRole get_role() const = 0;
  virtual const common::ObRegion& get_region() const = 0;
  virtual const common::ObIDC& get_idc() const = 0;
  virtual bool is_follower_active() const = 0;
  virtual int try_update_leader_from_loc_cache() = 0;
  virtual int async_get_dst_cluster_leader_from_loc_cache(
      const bool is_need_renew, const int64_t cluster_id, ObAddr& leader) = 0;
  virtual int64_t get_self_cluster_id() const = 0;
  virtual int64_t get_leader_cluster_id() const = 0;
  virtual void reset_need_rebuild() = 0;
  virtual bool is_need_rebuild() const = 0;
  virtual bool is_cluster_allow_vote() const = 0;
  virtual bool is_can_elect_standby_leader() const = 0;
  virtual int get_election_leader(ObAddr& leader, int64_t& leader_epoch) const = 0;
  virtual bool has_valid_member_list() const = 0;
  virtual void reset_fetch_state() = 0;
  virtual int64_t get_fetch_log_interval() const = 0;
};

class ObILogStateMgrForMS {
public:
  ObILogStateMgrForMS()
  {}
  virtual ~ObILogStateMgrForMS()
  {}

public:
  virtual bool can_submit_log() const = 0;
  virtual bool can_submit_with_assigned_id() const = 0;
  virtual common::ObAddr get_leader() const = 0;
  virtual int get_cascad_leader(share::ObCascadMember& cascad_leader_member) const = 0;
  virtual common::ObProposalID get_proposal_id() const = 0;
  virtual common::ObVersion get_freeze_version() const = 0;
  virtual bool is_offline() const = 0;
  virtual common::ObRole get_role() const = 0;
  virtual int try_update_leader_from_loc_cache() = 0;
  virtual const common::ObRegion& get_region() const = 0;
  virtual const common::ObIDC& get_idc() const = 0;
  virtual int64_t get_last_leader_active_time() const = 0;
  virtual int set_election_candidates(
      const int64_t replica_num, const ObMemberList& member_list, const int64_t membership_timestamp) = 0;
  virtual int set_election_replica_num(const int64_t replica_num) = 0;
  virtual int inc_election_replica_num() = 0;
  virtual int dec_election_replica_num() = 0;
  virtual bool can_submit_start_working_log() const = 0;
  virtual bool is_write_disabled() const = 0;
  virtual bool is_archive_restoring() const = 0;
  virtual bool is_archive_restoring_log() const = 0;
  virtual bool is_archive_restore_leader() const = 0;
  virtual bool is_can_elect_standby_leader() const = 0;
  virtual int64_t get_self_cluster_id() const = 0;
  virtual bool has_valid_member_list() const = 0;
  virtual bool is_standby_leader_active() const = 0;
  virtual int get_pls_epoch(int64_t& pls_epoch) const = 0;
};

class ObILogStateMgrForReconfirm {
public:
  ObILogStateMgrForReconfirm()
  {}
  virtual ~ObILogStateMgrForReconfirm()
  {}

public:
  virtual common::ObAddr get_leader() const = 0;
  virtual common::ObAddr get_previous_leader() const = 0;
  virtual common::ObProposalID get_proposal_id() const = 0;
  virtual common::ObVersion get_freeze_version() const = 0;
  virtual int handle_prepare_rqst(
      const common::ObProposalID& proposal_id, const common::ObAddr& new_leader, const int64_t cluster_id) = 0;
  virtual int handle_standby_prepare_rqst(
      const common::ObProposalID& proposal_id, const common::ObAddr& new_leader, const int64_t cluster_id) = 0;
  virtual int64_t get_reconfirm_start_time() const = 0;
  virtual bool is_new_created_leader() const = 0;
  virtual bool is_cluster_allow_handle_prepare() const = 0;
  virtual common::ObRole get_role() const = 0;
  virtual bool is_can_elect_standby_leader() const = 0;
  virtual int64_t get_self_cluster_id() const = 0;
  virtual int try_renew_sync_standby_location() = 0;
};

class ObLogStateMgr : public ObILogStateMgrForService,
                      public ObILogStateMgrForSW,
                      public ObILogStateMgrForCasMgr,
                      public ObILogStateMgrForMS,
                      public ObILogStateMgrForReconfirm {
  friend class unittest::ObLogStateMgrTest;

public:
  ObLogStateMgr();
  virtual ~ObLogStateMgr()
  {
    destroy();
  }

public:
  int init(ObILogSWForStateMgr* sw, ObILogReconfirm* reconfirm, ObILogEngine* log_engine, ObILogMembershipMgr* mm,
      ObLogCascadingMgr* cascading_mgr, ObLogRestoreMgr* restore_mgr, election::ObIElection* election,
      ObLogReplayEngineWrapper* replay_engine, storage::ObPartitionService* partition_service,
      common::ObILogAllocator* alloc_mgr, const common::ObAddr& self, const common::ObProposalID& proposal_id,
      const common::ObVersion& freeze_version, const common::ObPartitionKey& partition_key,
      const ObCreatePlsType& create_pls_type);
  virtual bool can_submit_log() const override
  {
    bool bool_ret = (is_leader_active_() && !is_offline() &&
                     share::ObMultiClusterUtil::is_cluster_allow_submit_log(partition_key_.get_table_id()));
    return bool_ret;
  }
  virtual bool can_submit_with_assigned_id() const override
  {
    bool bool_ret = (is_leader_active_() || is_standby_leader_active_()) && !is_offline();
    return bool_ret;
  }
  virtual bool can_submit_start_working_log() const override;
  virtual bool is_cluster_allow_handle_prepare() const override;
  virtual bool is_switching_cluster_replica(const uint64_t table_id) const
  {
    return (GCTX.is_in_standby_switching_state() && !share::ObMultiClusterUtil::is_cluster_private_table(table_id));
  }
  virtual bool can_append_disk_log() const override;
  virtual bool can_receive_recovery_log() const override;
  virtual bool can_majority_cb(const common::ObProposalID& proposal_id) const override;
  virtual bool can_standby_majority_cb(const common::ObProposalID& proposal_id) const override;
  virtual bool can_majority_cb_for_renew_ms_log(const common::ObProposalID& ms_proposal_id) const override;
  virtual bool can_receive_log_ack(const common::ObProposalID& proposal_id) const override;
  virtual bool can_send_log_ack(const common::ObProposalID& proposal_id) const override;
  virtual bool can_send_standby_log_ack(const common::ObProposalID& proposal_id) const override;
  virtual bool can_receive_max_log_id(const common::ObProposalID& proposal_id) const override;
  virtual bool can_receive_renew_ms_log_ack(const common::ObProposalID& ms_proposal_id) const override;
  virtual bool can_send_renew_ms_log_ack(const common::ObProposalID& proposal_id) const override;
  virtual bool can_handle_standby_prepare_resp(const common::ObProposalID& proposal_id) const override;
  bool is_diff_cluster_req_in_disabled_state(const int64_t cluster_id) const;
  virtual bool can_get_log(const int64_t cluster_id) const override;
  virtual bool can_get_log_for_reconfirm(const common::ObProposalID& proposal_id) const override;
  virtual bool can_receive_log(const common::ObProposalID& proposal_id, const common::ObProposalID& proposal_id_in_log,
      const int64_t cluster_id) const override;
  virtual bool can_receive_renew_ms_log(const common::ObProposalID& proposal_id_in_req,
      const common::ObProposalID& proposal_id_in_log, const int64_t cluster_id) const override;
  virtual bool can_change_member() const override;
  virtual bool can_change_leader() const override;
  virtual bool can_get_leader_curr_member_list() const override;
  virtual bool can_handle_prepare_rqst(
      const common::ObProposalID& proposal_id, const int64_t cluster_id) const override;
  virtual bool can_handle_standby_prepare_rqst(
      const common::ObProposalID& proposal_id, const int64_t cluster_id) const override;
  virtual bool can_slide_sw() const override;
  virtual bool is_standby_leader_can_receive_log_id(const uint32_t log_id) const override;
  virtual bool can_receive_confirmed_info(const int64_t src_cluster_id) const override;
  virtual bool can_receive_renew_ms_log_confirmed_info() const override;
  virtual bool can_set_offline() const;
  virtual bool can_set_replica_type() const;
  bool can_backfill_log(const bool is_leader, const common::ObProposalID& proposal_id) const;
  bool can_backfill_confirmed(const common::ObProposalID& proposal_id) const;

  virtual common::ObProposalID get_proposal_id() const override
  {
    return curr_proposal_id_;
  }
  virtual int switch_state(bool& need_retry) override;
  virtual bool check_sliding_window_state() override;
  virtual bool is_state_changed(bool& need_retry) override;
  virtual int set_scan_disk_log_finished() override;
  virtual bool is_scan_disk_log_finished() const
  {
    return scan_disk_log_finished_;
  }
  virtual int16_t get_state() const override
  {
    return state_;
  }
  virtual common::ObRole get_role() const override
  {
    return role_;
  }
  virtual common::ObAddr get_leader() const override
  {
    return leader_.get_server();
  }
  virtual share::ObCascadMember get_leader_member() const override
  {
    return leader_;
  }
  virtual common::ObAddr get_primary_leader_addr() const override
  {
    return primary_leader_.get_server();
  }
  virtual share::ObCascadMember get_primary_leader() const override
  {
    return primary_leader_;
  }
  virtual share::ObCascadMember get_prepared_primary_leader() const
  {
    return prepared_primary_leader_;
  }
  virtual int get_cascad_leader(share::ObCascadMember& cascad_leader_member) const override;
  virtual common::ObAddr get_previous_leader() const override
  {
    return previous_leader_;
  }
  virtual const common::ObRegion& get_region() const override;
  virtual const common::ObIDC& get_idc() const override;
  virtual int set_region(const common::ObRegion& region, bool& is_changed) override;
  virtual int set_idc(const common::ObIDC& idc, bool& is_changed) override;
  virtual int64_t get_leader_epoch() const
  {
    return leader_epoch_;
  }
  virtual common::ObVersion get_freeze_version() const override;
  virtual int64_t get_last_leader_active_time() const override
  {
    return last_leader_active_time_;
  }
  virtual int update_freeze_version(const common::ObVersion& freeze_version);
  virtual int handle_prepare_rqst(
      const common::ObProposalID& proposal_id, const common::ObAddr& new_leader, const int64_t cluster_id) override;
  virtual int handle_standby_prepare_rqst(
      const common::ObProposalID& proposal_id, const common::ObAddr& new_leader, const int64_t cluster_id) override;
  virtual int update_proposal_id(const common::ObProposalID& proposal_id) override;
  virtual int start_election() override;
  virtual int stop_election() override;
  virtual int set_election_leader(const common::ObAddr& leader, const int64_t lease_start) override;
  virtual void set_offline();
  virtual void set_online();
  virtual bool is_offline() const override
  {
    return true == ATOMIC_LOAD(&is_offline_);
  }
  virtual bool is_changing_leader() const;
  virtual int change_leader_async(const common::ObPartitionKey& partition_key, const common::ObAddr& leader,
      common::ObTsWindows& changing_leader_windows);
  virtual bool is_leader_active() const override
  {
    return is_leader_active_();
  }
  virtual bool is_standby_leader_active() const override
  {
    return is_standby_leader_active_();
  }
  virtual bool is_leader_taking_over() const override
  {
    return is_leader_taking_over_();
  }
  virtual bool is_leader_reconfirm() const override
  {
    return is_leader_reconfirm_();
  }
  virtual bool is_follower_active() const override
  {
    return is_follower_active_();
  }
  virtual int set_follower_active();
  void set_pre_member_changing()
  {
    ATOMIC_STORE(&is_pre_member_changing_, true);
  }
  void reset_pre_member_changing()
  {
    ATOMIC_STORE(&is_pre_member_changing_, false);
  }
  bool is_pre_member_changing() const
  {
    return ATOMIC_LOAD(&is_pre_member_changing_);
  }
  bool is_inited() const
  {
    return is_inited_;
  }
  bool is_restoring() const
  {
    return ATOMIC_LOAD(&is_restoring_);
  }
  bool set_restoring()
  {
    return ATOMIC_BCAS(&is_restoring_, false, true);
  }
  void reset_restoring()
  {
    return ATOMIC_STORE(&is_restoring_, false);
  }
  bool is_write_disabled() const override
  {
    return ATOMIC_LOAD(&is_write_disabled_);
  }
  void disable_write_authority()
  {
    return ATOMIC_STORE(&is_write_disabled_, true);
  }
  void enable_write_authority()
  {
    return ATOMIC_STORE(&is_write_disabled_, false);
  }
  bool is_archive_restoring() const override;
  bool is_archive_restore_leader() const override;
  bool is_archive_restoring_log() const override;
  int check_role_leader_and_state() const;
  int check_role_elect_leader_and_state() const;
  int try_update_leader_from_loc_cache() override;
  virtual int async_get_dst_cluster_leader_from_loc_cache(
      const bool is_need_renew, const int64_t cluster_id, ObAddr& leader) override;
  int64_t get_self_cluster_id() const override;
  int64_t get_leader_cluster_id() const override;
  virtual int64_t get_reconfirm_start_time() const override
  {
    return reconfirm_start_time_;
  }
  void destroy();
  virtual bool is_new_created_leader() const override
  {
    return is_new_created_leader_;
  }
  void set_need_rebuild()
  {
    ATOMIC_STORE(&need_rebuild_, true);
  }
  void reset_need_rebuild() override
  {
    ATOMIC_STORE(&need_rebuild_, false);
  }
  bool is_need_rebuild() const override
  {
    return ATOMIC_LOAD(&need_rebuild_);
  }
  bool is_replaying() const;
  int enable_replay();
  int disable_replay();
  bool is_replay_enabled() const override;
  int set_election_candidates(
      const int64_t replica_num, const ObMemberList& member_list, const int64_t membership_timestamp) override;
  int set_election_replica_num(const int64_t replica_num) override;
  int inc_election_replica_num() override;
  int dec_election_replica_num() override;
  int get_election_valid_candidates(ObMemberList& valid_candidate_list) const override;
  int get_election_leader(
      ObAddr& leader, int64_t& leader_epoch, common::ObTsWindows& changing_leader_windows) const override;
  int get_election_leader(ObAddr& leader, int64_t& leader_epoch) const override;
  bool is_leader_revoke_recently() const;
  void report_start_id_trace(const uint64_t start_id) override;
  int standby_set_election_leader(const common::ObAddr& leader, const int64_t lease_start);
  bool is_cluster_allow_vote() const override;
  bool is_can_elect_standby_leader() const override;
  bool has_valid_member_list() const override;
  virtual int try_renew_sync_standby_location() override;
  int standby_update_protection_level();
  int standby_leader_check_protection_level();
  int handle_sync_start_id_resp(
      const ObAddr& server, const int64_t cluster_id, const int64_t original_send_ts, const uint64_t sync_start_id);
  int get_standby_protection_level(uint32_t& protection_level) const;
  virtual int get_pls_epoch(int64_t& pls_epoch) const override;
  int primary_process_protect_mode_switch();

  // The following two functions cooperate to achieve congestion control of clog
  //
  // Used to called by sliding_cb before streaming fetch logs
  // This function will recalculate the interval between pulling logs
  void start_next_round_fetch() override final;
  // Called at the end of fetch to reset several states for congestion control
  void reset_fetch_state() override final;
  int try_reset_fetch_state(const uint64_t push_log_id) override final;
  int try_set_fetched_max_log_id(const uint64_t max_log_id) override final;
  uint64_t get_last_fetched_max_log_id() const override final;
  int64_t get_fetch_log_interval() const override final;
  uint64_t get_fetch_window_size() const override final;

private:
  int on_leader_takeover_();
  int on_leader_active_();
  int on_leader_revoke_();

  int replay_to_follower_active_();
  int revoking_to_follower_active_();
  int follower_active_to_reconfirm_(const int64_t new_leader_epoch, const ObAddr& previous_leader);
  int reconfirm_to_taking_over_();
  int reconfirm_to_follower_active_();
  int taking_over_to_leader_active_();
  int leader_active_to_revoking_();

  int submit_prepare_log_(const common::ObProposalID& proposal_id, const common::ObAddr& new_leader,
      const int64_t cluster_id, const ObLogType log_type);
  void reset_status_();

  bool is_leader_active_() const
  {
    return (LEADER == role_) && (ACTIVE == state_);
  }
  bool is_leader_taking_over_() const;
  bool is_leader_reconfirm_() const;
  bool is_standby_leader_reconfirm_() const;
  bool is_standby_leader_active_() const
  {
    return (STANDBY_LEADER == role_) && (ACTIVE == state_);
  }
  bool is_follower_replay_() const;
  bool is_leader_revoking_() const;
  bool is_standby_leader_revoking_() const;
  bool is_follower_active_() const;
  bool is_interim_state_() const;

  bool follower_replay_need_switch_();
  bool leader_revoking_need_switch_();
  bool follower_active_need_switch_();
  bool leader_reconfirm_need_switch_();
  bool leader_taking_over_need_switch_();
  bool leader_active_need_switch_(bool& is_error);

  bool check_leader_sliding_window_not_slide_(bool& need_switch_leader_to_self);
  void check_and_try_fetch_log_();
  bool need_update_leader_();
  bool follower_need_update_role_(share::ObCascadMember& new_leader, common::ObAddr& elect_real_leader,
      common::ObAddr& previous_leader, int64_t& new_leader_epoch);
  void set_leader_and_epoch_(const share::ObCascadMember& new_leader, const int64_t new_leader_epoch);

  bool need_update_primary_leader_(share::ObCascadMember& new_primary_leader);
  void set_primary_leader_(const share::ObCascadMember& new_primary_leader);
  bool is_reconfirm_initialize_();
  int get_pending_replay_count_(int64_t& pending_replay_cnt) const;
  bool is_reconfirm_role_change_or_sync_timeout_();
  bool is_reconfirm_not_eagain_(int& ret);
  int revoke_leader_(const uint32_t revoke_type);
  void check_role_change_warn_interval_(bool& is_role_change_timeout) const;
  int get_elect_leader_(share::ObCascadMember& leader, common::ObAddr& elect_real_leader,
      common::ObAddr& previous_leader, int64_t& leader_epoch);
  int standby_get_primary_leader_(share::ObCascadMember& new_primary_leader);
  bool is_log_local_majority_flushed_(const uint64_t log_id);
  int try_send_sync_start_id_request_();
  int reset_standby_protection_level_();
  void primary_leader_check_start_log_state_(
      const uint64_t start_id, bool& is_waiting_standby_ack, bool& need_switch_leader_to_self);
  bool need_fetch_log_() const;
  void reset_fetch_state_();

private:
  typedef common::SpinRWLock RWLock;
  typedef common::SpinRLockGuard RLockGuard;
  typedef common::SpinWLockGuard WLockGuard;
  enum { ROLE_CHANGE_WARN_INTERVAL = 10 * 1000 * 1000 };
  static const int64_t START_PARTITION_WARN_INTERVAL = 1 * 1000;
  static const int64_t BUF_SIZE = 2048;
  static const int64_t STANDBY_CHECK_SYNC_START_ID_INTERVAL = 3l * 1000l * 1000l;

  ObILogSWForStateMgr* sw_;
  ObILogReconfirm* reconfirm_;
  ObILogEngine* log_engine_;
  ObILogMembershipMgr* mm_;
  ObLogCascadingMgr* cascading_mgr_;
  ObLogReplayEngineWrapper* replay_engine_;
  ObLogRestoreMgr* restore_mgr_;
  storage::ObPartitionService* partition_service_;
  election::ObIElection* election_;
  common::ObILogAllocator* alloc_mgr_;
  bool scan_disk_log_finished_;
  bool election_started_;
  common::ObPartitionKey partition_key_;
  union {
    int64_t tmp_val_;
    struct {
      common::ObRole role_;
      int16_t state_;
      bool is_removed_;
    };
  };
  common::ObProposalID curr_proposal_id_;
  share::ObCascadMember leader_;
  common::ObAddr previous_leader_;
  share::ObCascadMember primary_leader_;  // primary cluster leader
  share::ObCascadMember prepared_primary_leader_;
  common::ObRegion region_;
  common::ObIDC idc_;
  int64_t leader_epoch_;
  common::ObAddr self_;
  mutable common::ObSpinLock lock_;  // protect freeze_version_;
  mutable RWLock region_lock_;
  mutable RWLock idc_lock_;
  common::ObVersion freeze_version_;
  int64_t start_role_change_time_;

  uint64_t last_check_start_id_;
  int64_t last_check_start_id_time_;

  // congestion control
  mutable common::ObSpinLock fetch_state_lock_;
  // Save the current timeout retransmission interval and
  // recalculate it when sliding_cb triggers streaming pull
  int64_t curr_fetch_log_interval_;  // GUARDED_BY(fetch_state_lock_)
  // whether it is the first time fetch in this round
  bool curr_round_first_fetch_;  // GUARDED_BY(fetch_state_lock_)
  // The start execution time of this round of log fetch request,
  // used for subsequent calculation of timeout retransmission interval
  int64_t curr_round_fetch_begin_time_;  // GUARDED_BY(fetch_state_lock_)
  // The ending position of this round of log fetch request
  uint64_t last_fetched_max_log_id_;  // GUARDED_BY(fetch_state_lock_)
  uint64_t curr_fetch_window_size_;   // GUARDED_BY(fetch_state_lock_)

  int64_t last_wait_standby_ack_start_time_;
  int64_t last_check_pending_replay_cnt_;
  int64_t last_check_pending_replay_cnt_time_;
  int64_t last_fake_push_time_;
  int64_t last_leader_active_time_;
  bool is_offline_;
  bool replay_enabled_;
  bool is_changing_leader_;
  int64_t role_change_time_;
  mutable int64_t send_log_ack_warn_time_;
  mutable int64_t receive_log_ack_warn_time_;
  mutable int64_t switch_state_warn_time_;
  mutable int64_t leader_sync_timeout_warn_time_;
  int64_t reconfirm_start_time_;
  int64_t on_leader_revoke_start_time_;
  int64_t on_leader_takeover_start_time_;
  int64_t last_renew_loc_time_;
  int64_t last_renew_primary_leader_time_;
  // protection_level for primary standby cluster
  int64_t last_sync_start_id_request_time_;       // time of last query primary for sync_start_id
  mutable common::ObSpinLock standby_sync_lock_;  // protect sync mode standby variables
  uint64_t standby_sync_start_id_;                // barrier log_id for max protection mode
  uint32_t standby_protection_level_;             // current protection_level

  bool is_pre_member_changing_;
  // Indicates whether it is the leader of the newly created partition:
  // In the scenario where the previous_leader cannot be known, reconfirm needs to wait for 1s before taking over
  // But for the newly created partition, there is no need to wait, use this bool variable to distinguish the two cases
  bool is_new_created_leader_;
  // Timing of setting this flag: in notify_log_missing
  // When to reset this flag:
  // 1)set_online;
  // 2)set_parent/reset_parent, because need_rebuild is related to its parent;
  // 3) sliding_cb, in order to avoid incorrect settings of rebuild caused by the delay of the synchronization log
  // network packet,
  //    if it can slide out, need reset this state;
  // 4) state change;
  bool need_rebuild_;
  bool is_elected_by_changing_leader_;
  bool is_restoring_;
  bool is_write_disabled_;

  // Record version of ObPartitionLogService
  // Fixed bug: In the fast recovery scenario, the first fast recovery fails, and there are som write requests in
  // flight, however, this partition has been garbage collect.  the second fast recover start, and previous write which
  // in flight has been written,  at this time, there is no corresponding log_task in sliding window. Solution: To avoid
  // this situation, add epoch to ObPartitionLogService, meanwhile, add the epoch refrence to ObFlushTask. if the epoch
  // of ObPartitionLogService and ObFlushTask are not equal, refuse to execute the callback of ObFlushTask.
  int64_t pls_epoch_;

  enum ObCreatePlsType create_pls_type_;

  bool is_inited_;

  DISALLOW_COPY_AND_ASSIGN(ObLogStateMgr);
};

}  // namespace clog
}  // namespace oceanbase

#endif  // OCEANBASE_CLOG_OB_LOG_STATE_MGR_H_
