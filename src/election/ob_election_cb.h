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

#ifndef OCEANBASE_ELECTION_OB_ELECTION_CB_
#define OCEANBASE_ELECTION_OB_ELECTION_CB_

#include "ob_election_base.h"

namespace oceanbase {

namespace common {
class ObAddr;
}

namespace election {
class ObElectionPriority;
class ObElectionGroupPriority;

class ObIElectionCallback {
public:
  ObIElectionCallback()
  {}
  virtual ~ObIElectionCallback()
  {}

public:
  // when role change
  // virtual int on_role_change(const ObElectionRole &role) = 0;
  // when get election priority
  virtual int on_get_election_priority(ObElectionPriority& priority) = 0;
  // when leader takeover
  // virtual int on_leader_takeover(const common::ObAddr &leader) = 0;
  // when leader revoke
  // virtual int on_leader_revoke(const common::ObAddr &leader) = 0;
  // when state changed
  virtual int on_election_role_change() = 0;
  // when election layer change leader retry
  virtual int on_change_leader_retry(const common::ObAddr& server, common::ObTsWindows& changing_leader_windows) = 0;
};

class ObIElectionGroupPriorityGetter {
public:
  ObIElectionGroupPriorityGetter()
  {}
  virtual ~ObIElectionGroupPriorityGetter()
  {}

public:
  virtual int get_election_group_priority(const uint64_t tenant_id, ObElectionGroupPriority& priority) const = 0;
};

}  // namespace election
}  // namespace oceanbase

#endif  // OCEANBASE_ELECTION_OB_ELECTION_CB_
