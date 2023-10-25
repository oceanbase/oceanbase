/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 * http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "mock_election.h"

namespace oceanbase::unittest {
  using namespace palf::election;

  MockElection::MockElection()
      : id_(0),
        self_(),
        role_(common::ObRole::FOLLOWER),
        epoch_(0),
        leader_(),
        is_inited_(false) { }

  MockElection::MockElection(const int64_t id, const common::ObAddr& self)
      : id_(id),
        self_(self),
        role_(common::ObRole::FOLLOWER),
        epoch_(0),
        leader_(),
        is_inited_(true) { }

  void MockElection::stop() { }

  int MockElection::init(const int64_t id, const common::ObAddr& self) {
    if (is_inited_) {
      return OB_INIT_TWICE;
    }
    if (id <= 0 || !self.is_valid()) {
      return OB_INVALID_ARGUMENT;
    }
    id_ = id;
    self_ = self;
    is_inited_ = true;
    SERVER_LOG(INFO, "MockElection::init success", K(id), K(self));
    return OB_SUCCESS;
  }

  int MockElection::get_role(common::ObRole& role, int64_t& epoch) const {
    role = role_;
    epoch = (role_ == common::ObRole::LEADER) ? epoch_ : 0;
    return OB_SUCCESS;
  }

  int MockElection::get_current_leader_likely(common::ObAddr& addr, int64_t& cur_leader_epoch) const {
    addr = leader_;
    cur_leader_epoch = epoch_;
    return OB_SUCCESS;
  }

  int MockElection::change_leader_to(const common::ObAddr& dest_addr) {
    UNUSED(dest_addr);
    return OB_SUCCESS;
  }

  int MockElection::revoke(const RoleChangeReason& reason) {
    UNUSED(reason);
    return OB_SUCCESS;
  }

  const common::ObAddr& MockElection::get_self_addr() const {
    return self_;
  }

  int MockElection::set_priority(ElectionPriority* priority) {
    UNUSED(priority);
    return OB_SUCCESS;
  }

  int MockElection::reset_priority() {
    return OB_SUCCESS;
  }

  int MockElection::handle_message(const ElectionPrepareRequestMsg& msg) {
    UNUSED(msg);
    return OB_SUCCESS;
  }

  int MockElection::handle_message(const ElectionAcceptRequestMsg& msg) {
    UNUSED(msg);
    return OB_SUCCESS;
  }

  int MockElection::handle_message(const ElectionPrepareResponseMsg& msg) {
    UNUSED(msg);
    return OB_SUCCESS;
  }

  int MockElection::handle_message(const ElectionAcceptResponseMsg& msg) {
    UNUSED(msg);
    return OB_SUCCESS;
  }

  int MockElection::handle_message(const ElectionChangeLeaderMsg& msg) {
    UNUSED(msg);
    return OB_SUCCESS;
  }

  int MockElection::set_leader(const common::ObAddr& leader, const int64_t new_epoch) {
    if (!is_inited_) {
      return OB_NOT_INIT;
    }
    if (new_epoch < 0) {
      return OB_INVALID_ARGUMENT;
    }
    role_ = (self_ == leader) ? common::ObRole::LEADER : common::ObRole::FOLLOWER;
    epoch_ = (new_epoch == 0) ? ++epoch_ : new_epoch;
    leader_ = leader;
    SERVER_LOG(INFO, "MockElection::set_leader success", K_(self), K_(id), K(leader), K(new_epoch),
        K(role_), K(epoch_), K(leader_));
    return OB_SUCCESS;
  }
}  // namespace unittest
