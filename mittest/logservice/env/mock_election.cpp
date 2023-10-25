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
  if (is_inited_) {
    ret = OB_INIT_TWICE;
  } else {
    id_ = id;
    self_ = self;
    is_inited_ = true;
    SERVER_LOG(INFO, "MockElection::init success", K(id), K(self));
  }
  return ret;
}

int MockElection::get_role(common::ObRole &role, int64_t &epoch)const
{
  int ret = OB_SUCCESS;
  role = role_;
  epoch = (role_ == common::ObRole::LEADER) ? epoch_ : 0;
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

int MockElection::revoke(const RoleChangeReason &reason)
{
  int ret = OB_SUCCESS;
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

int MockElection::set_leader(const common::ObAddr &leader, const int64_t new_epoch)
{
  int ret = OB_SUCCESS;
  role_ = (self_ == leader) ? common::ObRole::LEADER : common::ObRole::FOLLOWER;
  epoch_ = (new_epoch <= 0) ? ++epoch_ : new_epoch;
  leader_ = leader;
  SERVER_LOG(INFO, "MockElection::set_leader success", K(ret), K_(self), K_(id), K(leader), K(new_epoch),
      K(role_), K(epoch_), K(leader_));
  return ret;
}
}// unittest
}// oceanbase
