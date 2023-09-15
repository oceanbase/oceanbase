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

#ifndef OCEANBASE_UNITTEST_LOGSERVICE_MOCK_CONTAINER_LOG_CONFIG_MGR_
#define OCEANBASE_UNITTEST_LOGSERVICE_MOCK_CONTAINER_LOG_CONFIG_MGR_

#define private public
#include "logservice/palf/log_config_mgr.h"
#include "logservice/palf/log_sliding_window.h"
#undef private

namespace oceanbase
{
namespace palf
{
namespace election
{
class Election;
}

class MockLogConfigMgr : public LogConfigMgr
{
public:
  MockLogConfigMgr() {}
  virtual ~MockLogConfigMgr() {}

  int init(const int64_t palf_id,
           const common::ObAddr &self,
           const LogConfigMeta &log_ms_meta,
           LogEngine *log_engine,
           LogSlidingWindow *sw,
           LogStateMgr *state_mgr,
           election::Election *election,
           LogModeMgr *mode_mgr)
  {
    int ret = OB_SUCCESS;
    UNUSED(palf_id);
    UNUSED(self);
    log_ms_meta_ = log_ms_meta;
    UNUSED(log_engine);
    UNUSED(sw);
    UNUSED(state_mgr);
    UNUSED(election);
    UNUSED(mode_mgr);
    return ret;
  }
  void destroy() {}

  // require caller holds WLock in PalfHandleImpl
  int set_initial_member_list(const common::ObMemberList &member_list,
                              const int64_t replica_num,
                              const int64_t proposal_id,
                              LogConfigVersion &config_version)
  {
    int ret = OB_SUCCESS;
    UNUSED(member_list);
    UNUSED(replica_num);
    UNUSED(proposal_id);
    UNUSED(config_version);
    return ret;
  }
  int set_initial_member_list(const common::ObMemberList &member_list,
                              const common::ObMember &arb_replica,
                              const int64_t replica_num,
                              const int64_t proposal_id,
                              LogConfigVersion &config_version)
  {
    UNUSEDx(member_list, arb_replica, replica_num, proposal_id, config_version);
    return OB_SUCCESS;
  }
  // following get ops need caller holds RLock in PalfHandleImpl
  int64_t get_accept_proposal_id() const
  {
    return log_ms_meta_.proposal_id_;
  }
  int get_global_learner_list(common::GlobalLearnerList &learner_list) const
  {
    int ret = OB_SUCCESS;
    UNUSED(learner_list);
    return ret;
  }
  int get_degraded_learner_list(common::GlobalLearnerList &learner_list) const
  {
    int ret = OB_SUCCESS;
    UNUSED(learner_list);
    return ret;
  }
  int get_curr_member_list(common::ObMemberList &member_list, int64_t &replica_num) const
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(log_ms_meta_.curr_.config_.get_expected_paxos_memberlist(member_list, replica_num))) {
      PALF_LOG(WARN, "get_expected_paxos_memberlist failed", KR(ret), K_(palf_id), K_(self));
    }
    return ret;
  }
  int get_log_sync_member_list(common::ObMemberList &member_list, int64_t &replica_num) const
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(member_list.deep_copy(log_ms_meta_.curr_.config_.log_sync_memberlist_))) {
      PALF_LOG(WARN, "deep_copy member_list failed", KR(ret), KPC(this));
    } else {
      replica_num = log_ms_meta_.curr_.config_.log_sync_replica_num_;
    }
    return ret;
  }
  int get_log_sync_member_list_for_generate_committed_lsn(
      ObMemberList &prev_member_list,
      int64_t &prev_replica_num,
      ObMemberList &curr_member_list,
      int64_t &curr_replica_num,
      bool &is_before_barrier,
      LSN &barrier_lsn) const
  {
    int ret = OB_SUCCESS;
    LSN prev_committed_end_lsn;
    sw_->get_committed_end_lsn(prev_committed_end_lsn);
    is_before_barrier = false;
    barrier_lsn = LSN(PALF_INITIAL_LSN_VAL);
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      PALF_LOG(WARN, "LogConfigMgr not init", KR(ret));
    } else if (OB_FAIL(curr_member_list.deep_copy(log_ms_meta_.curr_.config_.log_sync_memberlist_))) {
      PALF_LOG(WARN, "deep_copy member_list failed", KR(ret), K_(palf_id), K_(self));
    } else if (FALSE_IT(curr_replica_num = log_ms_meta_.curr_.config_.log_sync_replica_num_)) {
    } else if (OB_UNLIKELY(prev_committed_end_lsn < reconfig_barrier_.prev_end_lsn_ &&
        reconfig_barrier_.prev_end_lsn_.is_valid())) {
      is_before_barrier = true;
      barrier_lsn = reconfig_barrier_.prev_end_lsn_;
      if (OB_FAIL(prev_member_list.deep_copy(log_ms_meta_.prev_.config_.log_sync_memberlist_))) {
        PALF_LOG(WARN, "deep_copy member_list failed", KR(ret), K_(palf_id), K_(self));
      } else {
        prev_replica_num = log_ms_meta_.prev_.config_.log_sync_replica_num_;
      }
    } else { }
    return ret;
  }
  int get_alive_member_list_with_arb(common::ObMemberList &member_list, int64_t &replica_num) const
  {
    int ret = OB_SUCCESS;
    GlobalLearnerList all_learners;
    if (OB_FAIL(log_ms_meta_.curr_.config_.convert_to_complete_config(member_list, replica_num, all_learners))) {
      PALF_LOG(WARN, "convert_to_complete_config failed", K(ret), KPC(this));
    }
    return ret;
  }
  int get_prev_member_list(common::ObMemberList &member_list) const
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(member_list.deep_copy(log_ms_meta_.prev_.config_.log_sync_memberlist_))) {
      PALF_LOG(WARN, "deep_copy member_list failed", KR(ret), KPC(this));
    }
    return ret;
  }
  int get_children_list(LogLearnerList &children) const
  {
    int ret = OB_SUCCESS;
    return ret;
  }
  int get_config_version(LogConfigVersion &config_version) const
  {
    int ret = OB_SUCCESS;
    config_version = log_ms_meta_.curr_.config_.config_version_;
    return ret;
  }
  int get_replica_num(int64_t &replica_num) const
  {
    int ret = OB_SUCCESS;
    common::ObMemberList member_list;
    if (OB_FAIL(log_ms_meta_.curr_.config_.get_expected_paxos_memberlist(member_list, replica_num))) {
      PALF_LOG(WARN, "get_expected_paxos_memberlist failed", KR(ret), KPC(this));
    }
    return ret;
  }
  int leader_do_loop_work()
  {
    int ret = OB_SUCCESS;
    return ret;
  }

  // for PalfHandleImpl::one_stage_config_change_
  int check_config_change_args(const LogConfigChangeArgs &args, bool &is_already_finished) const
  {
    int ret = OB_SUCCESS;
    UNUSED(args);
    UNUSED(is_already_finished);
    return ret;
  }
  // require caller holds WLock in PalfHandleImpl
  int apply_config_meta(const int64_t curr_proposal_id,
                        const int64_t prev_log_proposal_id,
                        const LSN prev_lsn,
                        const LogConfigChangeArgs &args,
                        bool &is_already_finished,
                        LogConfigVersion &config_version)
  {
    int ret = OB_SUCCESS;
    UNUSED(curr_proposal_id);
    UNUSED(prev_log_proposal_id);
    UNUSED(prev_lsn);
    UNUSED(args);
    UNUSED(is_already_finished);
    UNUSED(config_version);
    return ret;
  }
  int submit_config_log(const int64_t proposal_id, const LogConfigVersion &config_version)
  {
    int ret = OB_SUCCESS;
    UNUSED(proposal_id);
    UNUSED(config_version);
    return ret;
  }
  int check_ms_log_committed(const int64_t proposal_id, const LogConfigVersion &config_version)
  {
    int ret = OB_SUCCESS;
    UNUSED(proposal_id);
    UNUSED(config_version);
    return ret;
  }

  // for reconfirm
  int submit_start_working_log(const int64_t &log_proposal_id,
                               const int64_t &prev_log_proposal_id,
                               const LSN &prev_lsn,
                               LogConfigVersion &config_version)
  {
    int ret = OB_SUCCESS;
    UNUSED(log_proposal_id);
    UNUSED(prev_log_proposal_id);
    UNUSED(prev_lsn);
    UNUSED(config_version);
    return ret;
  }

  // for PalfHandleImpl::receive_config_log
  bool can_receive_config_log(const LogConfigVersion &config_version) const
  {
    int ret = OB_SUCCESS;
    UNUSED(config_version);
    return ret;
  }
  int after_flush_config_log(const LogConfigVersion &config_version)
  {
    int ret = OB_SUCCESS;
    UNUSED(config_version);
    return ret;
  }
  int submit_change_access_mode_log(const int64_t &log_proposal_id,
                                    const int64_t &prev_log_proposal_id,
                                    const LSN &prev_lsn,
                                    const AccessMode &access_mode,
                                    LogConfigVersion &config_version)
  {
    int ret = OB_SUCCESS;
    UNUSED(log_proposal_id);
    UNUSED(prev_log_proposal_id);
    UNUSED(prev_lsn);
    UNUSED(access_mode);
    UNUSED(config_version);
    return ret;
  }

  // follower接收到成员变更日志需要进行前向校验
  int receive_config_log(const common::ObAddr &leader, const LogConfigMeta &meta)
  {
    int ret = OB_SUCCESS;
    UNUSED(leader);
    UNUSED(meta);
    return ret;
  }

  // for PalfHandleImpl::ack_config_log
  int ack_config_log(const common::ObAddr &sender,
                     const int64_t proposal_id,
                     const LogConfigVersion &config_version)
  {
    int ret = OB_SUCCESS;
    UNUSED(sender);
    UNUSED(proposal_id);
    UNUSED(config_version);
    return ret;
  }
  int wait_config_log_persistence(const LogConfigVersion &config_version) const
  {
    int ret = OB_SUCCESS;
    UNUSED(config_version);
    return ret;
  }
  // broadcast leader info to global learners, only called in leader active
  int submit_broadcast_leader_info(const int64_t proposal_id) const
  {
    int ret = OB_SUCCESS;
    UNUSED(proposal_id);
    return ret;
  }
  void reset_status()
  {}
};

} // end of palf
} // end of oceanbase

#endif
