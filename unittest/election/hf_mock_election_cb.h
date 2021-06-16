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

#ifndef OB_UNITTEST_MOCK_ELECTION_CALLBACK_H_
#define OB_UNITTEST_MOCK_ELECTION_CALLBACK_H_

#include "election/ob_election_cb.h"
#include "election/ob_election_priority.h"
#include "election/ob_election_group_priority.h"

namespace oceanbase {
using namespace election;

namespace unittest {

class MockElectionCallback : public ObIElectionCallback, public ObIElectionGroupPriorityGetter {
public:
  MockElectionCallback()
  {}
  virtual ~MockElectionCallback()
  {}

  int init(const ObElectionPriority& priority)
  {
    // priority_
    priority_ = priority;
    // eg_priority_
    eg_priority_.set_candidate(true);
    eg_priority_.set_system_score(0);
    return OB_SUCCESS;
  }
  int on_get_election_priority(election::ObElectionPriority& priority)
  {
    priority = priority_;
    return OB_SUCCESS;
  }
  const ObElectionPriority& get_priority() const
  {
    return priority_;
  }
  int on_election_role_change()
  {
    return OB_SUCCESS;
  }
  int get_election_group_priority(const uint64_t tenant_id, ObElectionGroupPriority& priority) const
  {
    UNUSED(tenant_id);
    priority = eg_priority_;
    return OB_SUCCESS;
  }

public:
  ObElectionPriority priority_;
  ObElectionGroupPriority eg_priority_;
};

}  // namespace unittest
}  // namespace oceanbase

#endif
