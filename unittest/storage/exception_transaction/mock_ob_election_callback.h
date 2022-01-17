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

#ifndef OB_TEST_OB_TEST_MOCK_ELECTION_CALLBACK_H_
#define OB_TEST_OB_TEST_MOCK_ELECTION_CALLBACK_H_

#include "election/ob_election_cb.h"
#include "election/ob_election_priority.h"

namespace oceanbase {
namespace unittest {
class MockObElectionCallback : public election::ObIElectionCallback {
public:
  int on_get_election_priority(election::ObElectionPriority& priority)
  {
    priority.set_candidate(true);
    priority.set_membership_version(100);
    priority.set_log_id(10);

    return OB_SUCCESS;
  }
  int on_election_role_change()
  {
    return OB_SUCCESS;
  }
};

}  // namespace unittest
}  // namespace oceanbase

#endif
