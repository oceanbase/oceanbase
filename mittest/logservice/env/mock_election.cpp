/**
 * Copyright (c)2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#include "mock_election.h"

namespace oceanbase
{
using namespace palf::election;
namespace unittest
{

MockElection::MockElection()
    : id_(0),
      self_(),
      role_(common::ObRole::FOLLOWER),
      epoch_(0),
      leader_(),
      is_inited_(false) { }

MockElection::MockElection(const int64_t id, const common::ObAddr &self)
    : id_(id),
      self_(self),
      role_(common::ObRole::FOLLOWER),
      epoch_(0),
      leader_(),
      is_inited_(true) { }

void MockElection::stop()
{
}

int MockElection::init(const int64_t id, const common::ObAddr &self)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
  } else if (id <= 0 || false == self.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    id_ = id;
    self_ = self;
    is_inited_ = true;
    SERVER_LOG(INFO, "MockElection::init success", K(id), K(self));
  }
  return ret;
}

int MockElection::can_set_memberlist(const palf::LogConfigVersion &new_config_version) const
{
  int ret = OB_SUCCESS;
  UNUSED(new_config_version);
  return ret;
}

int MockElection::set_memberlist(const MemberList &new_member_list)
{
  int ret = OB_SUCCESS;
  UNUSED(new_member_list);
  return ret;
}

int MockElection::get_role(common::ObRole &role, int64_t &epoch)const
{
  int ret = OB_SUCCESS;
  role = role_;
  epoch = (common::ObRole::LEADER == role_)? epoch_: 0;
  return ret;
}

int MockElection::get_current_leader_likely(common::ObAddr &addr, int64_t &cur_leader_epoch) const
{
  int ret = OB_SUCCESS;
  addr = leader_;
  cur_leader_epoch = epoch_;
  return ret;
}

int MockElection::change_leader_to(const common::ObAddr &dest_addr)
{
  int ret = OB_SUCCESS;
  UNUSED(dest_addr);
  return ret;
}

int MockElection::temporarily_downgrade_protocol_priority(const int64_t time_us, const char *reason)
{
  int ret = OB_SUCCESS;
  UNUSED(time_us);
  UNUSED(reason);
  return ret;
}

const common::ObAddr &MockElection::get_self_addr() const
{
  return self_;
}

int64_t MockElection::to_string(char *buf, const int64_t buf_len)const
{
  UNUSEDx(buf, buf_len);
  return 0;
}

int MockElection::set_priority(ElectionPriority *priority)
{
  int ret = OB_SUCCESS;
  UNUSED(priority);
  return ret;
}

int MockElection::reset_priority()
{
  int ret = OB_SUCCESS;
  return ret;
}

// 处理消息
int MockElection::handle_message(const ElectionPrepareRequestMsg &msg)
{
  int ret = OB_SUCCESS;
  UNUSED(msg);
  return ret;
}

int MockElection::handle_message(const ElectionAcceptRequestMsg &msg)
{
  int ret = OB_SUCCESS;
  UNUSED(msg);
  return ret;
}

int MockElection::handle_message(const ElectionPrepareResponseMsg &msg)
{
  int ret = OB_SUCCESS;
  UNUSED(msg);
  return ret;
}

int MockElection::handle_message(const ElectionAcceptResponseMsg &msg)
{
  int ret = OB_SUCCESS;
  UNUSED(msg);
  return ret;
}

int MockElection::handle_message(const ElectionChangeLeaderMsg &msg)
{
  int ret = OB_SUCCESS;
  UNUSED(msg);
  return ret;
}

int MockElection::set_leader(const common::ObAddr &leader, const int64_t new_epoch = 0)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (0 > new_epoch) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    role_ = (self_ == leader)? common::ObRole::LEADER: common::ObRole::FOLLOWER;
    epoch_ = (0 == new_epoch)? ++epoch_: new_epoch;
    leader_ = leader;
  }
  SERVER_LOG(INFO, "MockElection::set_leader success", K(ret), K_(self), K_(id), K(leader), K(new_epoch),
      K(role_), K(epoch_), K(leader_));
  return ret;
}
}// unittest
}// oceanbase