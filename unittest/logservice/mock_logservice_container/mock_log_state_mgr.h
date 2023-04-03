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

#ifndef OCEANBASE_UNITTEST_LOGSERVICE_MOCK_CONTAINER_LOG_STATE_MGR_
#define OCEANBASE_UNITTEST_LOGSERVICE_MOCK_CONTAINER_LOG_STATE_MGR_

#define private public
#include "logservice/palf/log_state_mgr.h"
#undef private

namespace oceanbase
{
namespace palf
{

class MockLogStateMgr : public LogStateMgr
{
public:
  MockLogStateMgr()
    : mock_proposal_id_(1)
  {}
  ~MockLogStateMgr() {}

  virtual void destroy() {}
  virtual int init(const int64_t key,
           const common::ObAddr &self,
           const LogPrepareMeta &log_prepare_meta,
           election::Election* election,
           LogSlidingWindow *sw,
           LogReconfirm *reconfirm,
           LogEngine *log_engine,
           LogConfigMgr *mm,
           palf::PalfRoleChangeCbWrapper *palf_role_change_cb)
  {
    int ret = OB_SUCCESS;
    UNUSED(key);
    UNUSED(self);
    UNUSED(log_prepare_meta);
    UNUSED(election);
    UNUSED(sw);
    UNUSED(reconfirm);
    UNUSED(log_engine);
    UNUSED(mm);
    UNUSED(palf_role_change_cb);
    return ret;
  }
  virtual bool is_state_changed() { return false; }
  virtual int switch_state() { return OB_SUCCESS; }
  virtual int handle_prepare_request(const common::ObAddr &new_leader,
                                     const int64_t &proposal_id)
  {
    int ret = OB_SUCCESS;
    UNUSED(new_leader);
    UNUSED(proposal_id);
    return ret;
  }
  virtual int set_scan_disk_log_finished()
  {
    return OB_SUCCESS;
  }
  virtual bool can_append(const int64_t proposal_id, const bool need_check_proposal_id) const
  {
    UNUSED(proposal_id);
    UNUSED(need_check_proposal_id);
    return true;
  }
  virtual bool can_raw_write() const
  {
    return true;
  }
  virtual bool can_slide_sw() const
  {
    return true;
  }
  virtual bool can_revoke(const int64_t proposal_id) const
  {
    return true;
  }
  virtual int check_role_leader_and_state() const
  {
    return OB_SUCCESS;
  }
  virtual int64_t get_proposal_id() const
  {
    return mock_proposal_id_;
  }
  virtual common::ObRole get_role() const { return role_; }
  virtual int16_t get_state() const { return state_; }
  virtual void get_role_and_state(common::ObRole &role, ObReplicaState &state) const
  {
  }
  virtual const common::ObAddr &get_leader() const { return leader_; }
  virtual int64_t get_leader_epoch() const { return leader_epoch_; }
  virtual bool check_epoch_is_same_with_election(const int64_t expected_epoch) const
  {
    UNUSED(expected_epoch);
    return true;
  }
  virtual bool can_handle_prepare_response(const int64_t &proposal_id) const
  {
    UNUSED(proposal_id);
    return true;
  }
  virtual bool can_handle_prepare_request(const int64_t &proposal_id) const
  {
    UNUSED(proposal_id);
    return true;
  }
  virtual bool can_handle_committed_info(const int64_t &proposal_id) const
  {
    UNUSED(proposal_id);
    return true;
  }
  virtual bool can_receive_log(const int64_t &proposal_id) const
  {
    bool bool_ret = false;
    if (proposal_id == get_proposal_id() && true == is_sync_enabled()) {
      bool_ret = (is_follower_active() || is_follower_pending() || is_leader_reconfirm());
    }
    return bool_ret;
  }
  virtual bool can_send_log_ack(const int64_t &proposal_id) const
  {
    UNUSED(proposal_id);
    return true;
  }
  virtual bool can_receive_log_ack(const int64_t &proposal_id) const
  {
    UNUSED(proposal_id);
    return true;
  }
  virtual bool can_truncate_log() const
  {
    return true;
  }
  virtual bool need_freeze_group_buffer() const
  {
    return false;
  }
  virtual bool is_leader_active() const { return role_ == LEADER && state_ == ACTIVE; }
  virtual bool is_follower_pending() const { return role_ == FOLLOWER && state_ == PENDING; }
  virtual bool is_follower_active() const { return role_ == FOLLOWER && state_ == ACTIVE; }
  virtual bool is_leader_reconfirm() const { return role_ == LEADER && state_ == RECONFIRM; }
  virtual int truncate(const LSN &truncate_begin_lsn)
  {
    UNUSED(truncate_begin_lsn);
    return OB_SUCCESS;
  }
  virtual int enable_sync()
  {
    return OB_SUCCESS;
  }
  virtual int disable_sync()
  {
    return OB_SUCCESS;
  }
  virtual bool is_sync_enabled() const
  {
    return true;
  }
  virtual int set_changing_config_with_arb()
  {
    is_changing_config_with_arb_ = true;
    return OB_SUCCESS;
  }
  virtual int reset_changing_config_with_arb()
  {
    is_changing_config_with_arb_ = false;
    return OB_SUCCESS;
  }
  virtual bool is_changing_config_with_arb() const
  {
    return is_changing_config_with_arb_;
  }
public:
  int64_t mock_proposal_id_;
  bool is_changing_config_with_arb_;
};

} // end of palf
} // end of oceanbase

#endif
