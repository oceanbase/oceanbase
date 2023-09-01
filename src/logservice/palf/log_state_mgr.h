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

#ifndef OCEANBASE_LOGSERVICE_LOG_STATE_MGR_
#define OCEANBASE_LOGSERVICE_LOG_STATE_MGR_

#include "log_define.h"
#include "log_meta_info.h"
#include "common/ob_member_list.h"
#include "common/ob_role.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/utility/utility.h"
#include "lsn.h"
#include "share/ob_ls_id.h"
#include "election/interface/election.h"

namespace oceanbase
{
namespace palf
{
class PalfRoleChangeCbWrapper;
}
namespace palf
{
class ObIElection;
class LogSlidingWindow;
class LogReconfirm;
class LogEngine;
class LogConfigMgr;
class LogModeMgr;
class LogPrepareMeta;

class LogStateMgr
{
public:
  LogStateMgr();
  virtual ~LogStateMgr() { destroy(); }
public:
  virtual void destroy();
  virtual int init(const int64_t key,
           const common::ObAddr &self,
           const LogPrepareMeta &log_prepare_meta,
           const LogReplicaPropertyMeta &replica_property_meta,
           election::Election* election,
           LogSlidingWindow *sw,
           LogReconfirm *reconfirm,
           LogEngine *log_engine,
           LogConfigMgr *mm,
           LogModeMgr *mode_mgr_,
           palf::PalfRoleChangeCbWrapper *palf_role_change_cb,
           LogPlugins *plugins);
  virtual bool is_state_changed();
  virtual int switch_state();
  virtual int handle_prepare_request(const common::ObAddr &new_leader,
                             const int64_t &proposal_id);
  virtual int set_scan_disk_log_finished();
  virtual bool can_append(const int64_t proposal_id, const bool need_check_proposal_id) const;
  virtual bool can_raw_write(const int64_t proposal_id, const bool need_check_proposal_id) const;
  virtual bool can_slide_sw() const;
  virtual bool can_handle_committed_info(const int64_t &proposal_id) const;
  virtual bool can_revoke(const int64_t proposal_id) const;
  virtual int check_role_leader_and_state() const;
  virtual int64_t get_proposal_id() const;
  virtual common::ObRole get_role() const { return role_; }
  virtual int16_t get_state() const { return state_; }
  virtual void get_role_and_state(common::ObRole &role, ObReplicaState &state) const;
  virtual const common::ObAddr &get_leader() const { return leader_; }
  virtual int64_t get_leader_epoch() const { return leader_epoch_; }
  virtual bool check_epoch_is_same_with_election(const int64_t expected_epoch) const;
  virtual bool can_handle_prepare_response(const int64_t &proposal_id) const;
  virtual bool can_handle_prepare_request(const int64_t &proposal_id) const;
  virtual bool can_receive_log(const int64_t &proposal_id) const;
  virtual bool can_receive_config_log(const int64_t &proposal_id) const;
  virtual bool can_send_log_ack(const int64_t &proposal_id) const;
  virtual bool can_receive_log_ack(const int64_t &proposal_id) const;
  virtual bool can_truncate_log() const;
  virtual bool need_freeze_group_buffer() const;
  virtual bool is_leader_active() const { return is_leader_active_(); }
  virtual bool is_follower_pending() const { return is_follower_pending_(); }
  virtual bool is_follower_active() const { return is_follower_active_(); }
  virtual bool is_leader_reconfirm() const { return is_leader_reconfirm_(); }
  virtual int truncate(const LSN &truncate_begin_lsn);
  virtual int enable_sync();
  virtual int disable_sync();
  virtual bool is_sync_enabled() const;
  virtual bool is_allow_vote() const;
  virtual bool is_allow_vote_persisted() const;
  //only modify allow_vote_
  virtual int disable_vote_in_mem();
  //modify allow_vote_ and allow_vote_persisted_
  virtual int disable_vote();
  virtual int enable_vote();
  virtual LogReplicaType get_replica_type() const;
  virtual int get_election_role(common::ObRole &role, int64_t &epoch) const;
  virtual bool is_arb_replica() const;
  virtual int set_changing_config_with_arb();
  virtual int reset_changing_config_with_arb();
  virtual bool is_changing_config_with_arb() const;
  virtual const common::ObAddr &get_broadcast_leader() const { return broadcast_leader_;}
  virtual bool can_handle_leader_broadcast(const common::ObAddr &server,
                                           const int64_t &proposal_id) const;
  virtual int handle_leader_broadcast(const common::ObAddr &server,
                                      const int64_t &proposal_id);
  TO_STRING_KV(KP(this), K_(self), K_(palf_id), "role", role_to_string(role_), "replica_type",         \
      replica_type_2_str(replica_type_), "state", replica_state_to_string(state_), K_(prepare_meta),   \
      K_(leader), K_(leader_epoch), K_(is_sync_enabled), K_(allow_vote), K_(allow_vote_persisted), K_(pending_end_lsn),          \
      K_(scan_disk_log_finished), K_(last_check_start_id), K_(is_changing_config_with_arb),            \
      K_(reconfirm_start_time_us), KP_(palf_role_change_cb), K_(allow_vote));
private:
  bool check_role_and_state_(const common::ObRole &role, const ObReplicaState &state) const;
  void update_role_and_state_(const common::ObRole &new_role, const ObReplicaState &new_state);
  int reject_prepare_request_(const common::ObAddr &server,
                              const int64_t &proposal_id);
  bool is_reconfirm_can_receive_log_() const;
  bool is_follower_active_() const;
  bool is_follower_init_() const;
  bool is_leader_reconfirm_() const;
  bool is_leader_active_() const;
  bool is_follower_pending_() const;

  bool follower_init_need_switch_();
  bool follower_active_need_switch_();
  bool leader_reconfirm_need_switch_();
  bool leader_active_need_switch_(bool &is_error);
  bool follower_pending_need_switch_();
  bool is_pending_log_clear_() const;
  int write_prepare_meta_(const int64_t &proposal_id,
                          const common::ObAddr &new_leader);
  int init_to_follower_active_();
  int to_reconfirm_(const int64_t new_leader_epoch);
  int follower_active_to_reconfirm_(const int64_t new_leader_epoch);
  int follower_pending_to_reconfirm_(const int64_t new_leader_epoch);
  int reconfirm_to_leader_active_();
  int reconfirm_to_follower_pending_();
  int leader_active_to_follower_pending_();
  int pending_to_follower_active_();

  int to_leader_active_();
  int to_follower_active_();
  int to_follower_pending_();

  void reset_status_();

  bool is_reconfirm_timeout_();
  bool is_reconfirm_need_wlock_();
  bool is_reconfirm_state_changed_(int &ret);

  bool need_update_leader_(common::ObAddr &new_leader);
  bool follower_need_update_role_(common::ObAddr &new_leader, int64_t &new_leader_epoch);
  void set_leader_and_epoch_(const common::ObAddr &new_leader, const int64_t new_leader_epoch);
  int get_elect_leader_(common::ObAddr &leader, int64_t &leader_epoch) const;
  bool check_leader_log_sync_state_();
  bool need_fetch_log_() const;
  int check_and_try_fetch_log_();
  bool need_reset_broadcast_leader_() const;
  void reset_broadcast_leader_();
private:
  static const uint64_t UNION_ROLE_VAL_MASK = (1ll << 32) - 1;
private:
  common::ObAddr self_;
  int64_t  palf_id_;
  LogPrepareMeta prepare_meta_;
  LogSlidingWindow *sw_;
  LogReconfirm *reconfirm_;
  LogEngine *log_engine_;
  LogConfigMgr *mm_;
  LogModeMgr *mode_mgr_;
  palf::PalfRoleChangeCbWrapper *palf_role_change_cb_;
  election::Election* election_;
  LogPlugins *plugins_;
  union
  {
    int64_t role_state_val_;
    struct
    {
      common::ObRole role_ : 32;
      ObReplicaState state_ : 32;
    };
  };
  common::ObAddr leader_;
  int64_t leader_epoch_;
  int64_t last_check_start_id_;
  int64_t last_check_start_id_time_us_;
  int64_t last_check_pending_replay_cnt_;
  int64_t reconfirm_start_time_us_;
  int64_t check_sync_enabled_time_;
  int64_t check_reconfirm_timeout_time_;
  mutable int64_t check_follower_pending_warn_time_;
  mutable int64_t log_sync_timeout_warn_time_;
  mutable int64_t update_leader_warn_time_;
  LSN pending_end_lsn_;
  bool scan_disk_log_finished_;
  // whether log sync is enabled, it's true by default
  bool is_sync_enabled_;
  // whether this replica is allowed to reply ack when receiving logs
  // it's true by default
  bool allow_vote_;
  // value of allow_vote persisted_, will be modified after meta is flushed
  bool allow_vote_persisted_;
  // whether this replica is an arbitration replica
  LogReplicaType replica_type_;
  // is changing config with arbitration member, stop appending logs
  bool is_changing_config_with_arb_;
  int64_t last_set_changing_config_with_arb_time_us_;
  common::ObAddr broadcast_leader_;
  int64_t last_recv_leader_broadcast_time_us_;
  bool is_inited_;

  DISALLOW_COPY_AND_ASSIGN(LogStateMgr);
};

} // namespace palf
} // namespace oceanbase

#endif // OCEANBASE_LOGSERVICE_LOG_STATE_MGR_
