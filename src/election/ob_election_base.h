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

#ifndef OCEANBASE_ELECTION_OB_ELECTION_BASE_
#define OCEANBASE_ELECTION_OB_ELECTION_BASE_

#include <stdio.h>

namespace oceanbase {
namespace common {
class ObAddr;
}
namespace election {

// election member type
enum ObElectionMemberType {
  MEMBER_TYPE_UNKNOWN = -1,
  // MEMBER_TYPE_VOTER was setted 1 in older code
  // MEMBER_TYPE_WATCHER has been dropped now
  // MEMBER_TYPE_VOTER setted 1 still, just for compatibility considerations
  // only can vote
  MEMBER_TYPE_VOTER = 1,
  // can vote and be voted
  MEMBER_TYPE_CANDIDATE,
};

// election role, difference from election member type
enum ObElectionRole {
  ROLE_UNKNOWN = -1,
  ROLE_SLAVE = 0,
  ROLE_LEADER,
};

// election stat
enum ObElectionStat {
  STATE_UNKNOWN = -1,
  STATE_IDLE = 0,
  STATE_DECENTRALIZED_VOTING,
  STATE_CENTRALIZED_VOTING,
};

enum ObElectionStage {
  STAGE_UNKNOWN = -1,
  STAGE_PREPARE,
  STAGE_VOTING,
  STAGE_COUNTING,
};

bool check_upgrade_env();
int get_self_addr(common::ObAddr& self, const char* dev, const int32_t port);
const char* ObElectionMemberName(ObElectionMemberType type);
const char* ObElectionRoleName(ObElectionRole role);
const char* ObElectionStatName(ObElectionStat stat);
const char* ObElectionStageName(ObElectionStage stage);
void msleep(int64_t ms);
}  // namespace election
}  // namespace oceanbase

#endif  // OCEANBASE_ELECTION_OB_ELECTION_BASE_
