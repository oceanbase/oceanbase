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

#include "ob_log_handler_base.h"
#include "common/ob_role.h"
#include "logservice/palf/log_define.h"

namespace oceanbase
{
namespace logservice
{
ObLogHandlerBase::ObLogHandlerBase() :
  lock_(),
  role_(common::FOLLOWER),
  proposal_id_(palf::INVALID_PROPOSAL_ID),
  id_(-1),
  palf_handle_(),
  palf_env_(NULL),
  is_in_stop_state_(true),
  is_inited_(false)
{}

int ObLogHandlerBase::prepare_switch_role(common::ObRole &curr_role,
                                          int64_t &curr_proposal_id,
                                          common::ObRole &new_role,
                                          int64_t &new_proposal_id,
                                          bool &is_pending_state) const
{
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);
  if (OB_FAIL(palf_handle_.get_role(new_role, new_proposal_id, is_pending_state))) {
    PALF_LOG(WARN, "PalfHandle get_role failed", K(ret), K_(id), K(curr_role), K(curr_proposal_id));
  } else {
    curr_role = role_;
    curr_proposal_id = proposal_id_;
    PALF_LOG(TRACE, "prepare_switch_role success", K(ret), K_(id), K(curr_role), K(curr_proposal_id),
        K(new_role), K(new_proposal_id));
  }
  return ret;
}

int ObLogHandlerBase::advance_election_epoch_and_downgrade_priority(const int64_t downgrade_priority_time_us,
                                                                    const char *reason)
{
  RLockGuard guard(lock_);
  return palf_handle_.advance_election_epoch_and_downgrade_priority(proposal_id_, downgrade_priority_time_us, reason);
}

int ObLogHandlerBase::change_leader_to(const common::ObAddr &dst_addr)
{
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);
  if (OB_FAIL(palf_handle_.change_leader_to(dst_addr))) {
    PALF_LOG(WARN, "palf change_leader failed", K(ret), K(dst_addr), K(palf_handle_));
  } else {
    PALF_LOG(INFO, "ObLogHandler change_laeder success", K(ret), K(dst_addr), K(palf_handle_));
  }
  return ret;
}

int ObLogHandlerBase::get_role(common::ObRole &role, int64_t &proposal_id) const
{
  int ret = OB_SUCCESS;
  bool is_pending_state = false;
  int64_t curr_palf_proposal_id;
  ObRole curr_palf_role;
  // 获取当前的proposal_id
  RLockGuard guard(lock_);
  const int64_t saved_proposal_id = ATOMIC_LOAD(&proposal_id_);
  const ObRole saved_role = ATOMIC_LOAD(&role_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (is_in_stop_state_) {
    ret = OB_NOT_RUNNING;
  } else if (FOLLOWER == saved_role) {
    role = FOLLOWER;
    proposal_id = saved_proposal_id;
  } else if (OB_FAIL(palf_handle_.get_role(curr_palf_role, curr_palf_proposal_id, is_pending_state))) {
    CLOG_LOG(WARN, "get_role failed", K(ret));
  } else if (curr_palf_proposal_id != saved_proposal_id) {
    // palf的proposal_id已经发生变化，返回FOLLOWER
    role = FOLLOWER;
    proposal_id = saved_proposal_id;
  } else {
    role = curr_palf_role;
    proposal_id = saved_proposal_id;
  }
  return ret;
}

// Note: do not acquire any lock in the function
int ObLogHandlerBase::get_role_atomically(common::ObRole &role) const
{
  int ret = OB_SUCCESS;
  role = ATOMIC_LOAD(&role_);
  return ret;
}
} // end namespace logservice
} // end namespace oceanbase
