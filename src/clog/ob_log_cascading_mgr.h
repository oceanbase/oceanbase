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

#ifndef OCEANBASE_CLOG_OB_LOG_CASCADING_MGR_
#define OCEANBASE_CLOG_OB_LOG_CASCADING_MGR_

#include "lib/lock/ob_spin_lock.h"
#include "share/ob_cascad_member_list.h"
#include "common/ob_member_list.h"
#include "common/ob_partition_key.h"
#include "common/ob_idc.h"
#include "ob_i_submit_log_cb.h"
#include "ob_log_define.h"
#include "ob_log_entry.h"
#include "ob_log_type.h"
#include "lib/lock/ob_tc_rwlock.h"

namespace oceanbase {
namespace storage {
class ObPartitionService;
}
namespace election {
class ObIElectionMgr;
}
namespace share {
class ObIPSCb;
}
namespace clog {
class ObILogSWForCasMgr;
class ObILogStateMgrForCasMgr;
class ObILogMembershipMgr;
class ObILogEngine;

class ObLogCascadingMgr {
public:
  // reset parent reason type
  enum ResetPType {
    REJECT_MSG = 0,
    SELF_ELECTED = 1,
    CHECK_STATE = 2,
    SELF_OFFLINE = 3,
    REGION_CHANGE = 4,
    REPLICA_TYPE_CHANGE = 5,
    IDC_CHANGE = 6,
    ADMIN_CMD = 7,
    STANDBY_UPDATE = 8,
    STANDBY_REVOKE = 9,
    STANDBY_PROTECTION_MODE_SWITCH = 10,
  };
  ObLogCascadingMgr();
  virtual ~ObLogCascadingMgr();

public:
  int init(const common::ObAddr& self, const common::ObPartitionKey& partition_key, ObILogSWForCasMgr* sw,
      ObILogStateMgrForCasMgr* state_mgr, ObILogMembershipMgr* mm, ObILogEngine* log_engine,
      share::ObIPSCb* partition_service_cb);
  bool is_inited() const
  {
    return is_inited_;
  }
  void destroy();
  int get_children_list(share::ObCascadMemberList& list) const;
  int get_async_standby_children(share::ObCascadMemberList& list) const;
  share::ObCascadMember get_sync_standby_child() const
  {
    return sync_standby_child_;
  }
  bool is_valid_child(const common::ObAddr& server) const;
  int reject_server(const common::ObAddr& server, const int64_t cluster_id, const int32_t msg_type) const;
  int process_reject_msg(const common::ObAddr& server, const int32_t msg_type);
  int process_region_change(const common::ObRegion& new_region);
  int process_fetch_reg_server_req(const common::ObAddr& server, const common::ObReplicaType replica_type,
      const bool is_self_lag_behind, const bool is_request_leader, const bool is_need_force_register,
      const common::ObRegion& region, const int64_t cluster_id, const common::ObIDC& idc,
      const common::ObRegion& self_region);
  int process_fetch_reg_server_resp(const common::ObAddr& sender, const bool is_assign_parent_succeed,
      const share::ObCascadMemberList& candidate_list, const int32_t msg_type, const common::ObRegion& self_region);
  int try_replace_sick_child(const common::ObAddr& sender, const int64_t cluster_id, const common::ObAddr& sick_child);
  int check_cascading_state();
  void reset_children_list();
  void reset_async_standby_children();
  void reset_sync_standby_child();
  int get_lower_level_replica_list(common::ObChildReplicaList& list) const;
  int reset_last_request_state();
  bool has_valid_child() const;
  bool has_valid_async_standby_child() const;
  bool has_valid_sync_standby_child() const;
  share::ObCascadMember get_parent() const
  {
    return parent_;
  }
  common::ObAddr get_parent_addr() const
  {
    return parent_.get_server();
  }
  common::ObAddr get_prev_parent_addr() const
  {
    return prev_parent_.get_server();
  }
  int set_parent(const share::ObCascadMember& parent);
  int set_parent(const common::ObAddr& parent, const int64_t cluster_id);
  void reset_parent(const uint32_t reset_type);
  int notify_change_parent(const share::ObCascadMemberList& member_list);
  int check_child_legal(
      const common::ObAddr& server, const int64_t cluster_id, const common::ObReplicaType replica_type) const;
  int check_parent_state(const int64_t now, const common::ObRegion& self_region);
  int fetch_register_server(const common::ObAddr& dst_server, const int64_t dst_cluster_id,
      const common::ObRegion& region, const common::ObIDC& idc, const bool is_request_leader,
      const bool is_need_force_register) const;
  int try_remove_child(const common::ObAddr& server);
  int process_reregister_msg(const share::ObCascadMember& new_leader);
  bool is_in_reregister_period() const
  {
    return (common::OB_INVALID_TIMESTAMP != reregister_begin_ts_ &&
            common::ObTimeUtility::current_time() - reregister_begin_ts_ <= REREGISTER_PERIOD);
  }
  int get_sync_standby_cluster_id(int64_t& sync_cluster_id);
  int update_sync_standby_child(const common::ObAddr& server, const int64_t cluster_id);
  int primary_process_protect_mode_switch();
  int leader_try_update_sync_standby_child(const bool need_renew_loc);

private:
  typedef common::ObSEArray<common::ObRegion, 8> RegionArray;

private:
  int set_parent_(const common::ObAddr& new_parent_addr, const int64_t cluster_id);
  void reset_parent_(const uint32_t reset_type);
  int get_parent_candidate_list_(const common::ObAddr& server, const common::ObReplicaType replica_type,
      const common::ObRegion& region, const int64_t cluster_id, const common::ObRegion& self_region,
      share::ObCascadMemberList& candidate_member_list, bool& is_become_my_child);
  int get_server_region_(const common::ObAddr& server, common::ObRegion& region) const;
  bool is_children_list_full_unlock_();
  bool is_async_standby_children_full_unlock_();
  int leader_force_add_child_unlock_(const share::ObCascadMember& member);
  int try_add_child_unlock_(const share::ObCascadMember& member);
  int try_remove_child_unlock_(const common::ObAddr& server);
  int remove_dup_cluster_standby_child_(const share::ObCascadMember& member);
  int try_request_next_candidate_(const common::ObRegion& self_region);
  void reset_last_request_state_unlock_();
  int leader_get_dup_region_child_list_(share::ObCascadMemberList& dup_region_list) const;
  int get_dst_region_children_(const bool only_diff_region, share::ObCascadMemberList& dst_children) const;
  bool is_region_recorded_(const RegionArray& region_list, const common::ObRegion region) const;
  int leader_try_update_sync_standby_child_unlock_(const bool need_renew_loc);
  bool has_async_standby_child_(const int64_t cluster_id, common::ObAddr& server) const;
  bool is_valid_child_unlock_(const common::ObAddr& server) const;
  bool need_update_parent_() const;

private:
  typedef common::SpinRLockGuard RLockGuard;
  typedef common::SpinWLockGuard WLockGuard;

  static const int64_t MAX_CHILD_NUM = 5;

private:
  bool is_inited_;
  mutable common::SpinRWLock lock_;
  common::ObAddr self_;
  common::ObPartitionKey partition_key_;
  share::ObCascadMember parent_;
  share::ObCascadMember prev_parent_;
  ObILogSWForCasMgr* sw_;
  ObILogStateMgrForCasMgr* state_mgr_;
  ObILogMembershipMgr* mm_;
  ObILogEngine* log_engine_;
  share::ObIPSCb* ps_cb_;
  share::ObCascadMemberList children_list_;
  share::ObCascadMemberList async_standby_children_;
  share::ObCascadMember sync_standby_child_;
  int64_t get_parent_candidate_list_log_time_;
  int64_t last_check_parent_time_;
  int64_t last_check_child_state_time_;
  int64_t parent_invalid_start_ts_;
  int64_t reset_parent_warn_time_;
  int64_t update_parent_warn_time_;
  int64_t receive_reject_msg_warn_time_;
  int64_t check_parent_invalid_warn_time_;
  int64_t parent_invalid_warn_time_;
  int64_t last_check_log_ts_;
  int64_t last_replace_child_ts_;
  int64_t reregister_begin_ts_;
  int64_t last_renew_standby_loc_time_;
  common::ObAddr last_request_candidate_;
  share::ObCascadMemberList candidate_server_list_;

  DISALLOW_COPY_AND_ASSIGN(ObLogCascadingMgr);
};
}  // namespace clog
}  // namespace oceanbase

#endif  // OCEANBASE_CLOG_OB_LOG_CASCADING_MGR_
