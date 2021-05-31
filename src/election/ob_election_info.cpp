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

#include "ob_election_info.h"
#include "ob_election_async_log.h"

namespace oceanbase {
using namespace common;
namespace election {
void ObElectionInfo::reset()
{
  is_running_ = false;
  need_change_leader_ = false;
  is_changing_leader_ = false;
  change_leader_timestamp_ = 0;
  self_.reset();
  proposal_leader_.reset();
  current_leader_.reset();
  previous_leader_.reset();
  curr_candidates_.reset();
  curr_membership_version_ = 0;
  replica_num_ = 0;
  leader_lease_.first = 0;
  leader_lease_.second = 0;
  is_elected_by_changing_leader_ = false;
  election_time_offset_ = 0;
  active_timestamp_ = 0;
  start_timestamp_ = 0;
  T1_timestamp_ = 0;
  takeover_t1_timestamp_ = 0;
  leader_epoch_ = OB_INVALID_TIMESTAMP;
  state_ = State::D_IDLE;
  role_ = ROLE_SLAVE;
  stage_ = STAGE_UNKNOWN;
  type_ = MEMBER_TYPE_UNKNOWN;
  eg_id_.reset();
  last_leader_revoke_time_ = OB_INVALID_TIMESTAMP;
}

//              RESET   D_ELECT   D_PREPARE D_VOTE   D_COUNT    D_SUCCESS V_ELECT    V_PREPARE   V_VOTE     V_COUNT
//              V_SUCCESS  CHANGE_LEAER QUERY_RESPONSE LEADER_REVOKE
// D_IDLE       D_IDLE  D_PREPARE D_PREPARE D_VOTING N          N         N           F_V_PREPARE D_IDLE     N V_IDLE N
// V_IDLE        D_IDLE V_IDLE       V_IDLE  N         N         N        N          N         L_V_PREPARE F_V_PREPARE N
// N            V_IDLE    L_V_PREPARE  V_IDLE        D_IDLE QUERY        D_IDLE  N         N         N        N
// D_SUCCESS N           F_V_PREPARE F_V_VOTING N            V_SUCCESS N            V_IDLE        D_IDLE D_PREPARE
// D_IDLE  N         D_PREPARE D_VOTING N          N         N           F_V_PREPARE N          N            V_SUCCESS N
// V_IDLE        D_IDLE D_VOTING     D_IDLE  N         N         N        D_COUNTING D_SUCCESS N           N           N
// N            V_SUCCESS N            N             D_IDLE D_COUNTING   D_IDLE  N         N         N        N
// D_SUCCESS N           N           N          N            N         N            N             D_IDLE D_SUCCESS
// V_IDLE  N         N         N        N          N         N           N           N          N            N
// L_V_PREPARE  N             D_IDLE L_V_PREPARE  V_IDLE  N         N         N        N          N         N
// L_V_PREPARE L_V_VOTING N            N         L_V_PREPARE  N             D_IDLE L_V_VOTING   V_IDLE  N         N N N
// N         N           L_V_VOTING  L_V_VOTING L_V_COUNTING N         L_V_VOTING   N             D_IDLE L_V_COUNTING
// V_IDLE  N         N         N        N          N         N           N        L_V_COUNTING  L_V_COUNTING V_SUCCESS
// F_V_VOTING   N             D_IDLE F_V_PREPARE  V_IDLE  N         N         N        N          N         N
// F_V_PREPARE F_V_VOTING N            V_SUCCESS V_SUCCESS    N             D_IDLE F_V_VOTING   V_IDLE  N         N N N
// N         N           N           N          N            V_SUCCESS V_SUCCESS    N             D_IDLE V_SUCCESS
// V_IDLE  N         N         N        N          N         N           V_SUCCESS   V_SUCCESS  N            N
// L_V_PREPARE  N             D_IDLE
//

int ObElectionInfo::StateHelper::switch_state(const int32_t op)
{
  int ret = OB_SUCCESS;

  static const int32_t N = State::INVALID;
  static const int32_t STATE_MAP[State::MAX][Ops::MAX] = {
      {State::D_IDLE,
          State::D_PREPARE,
          State::D_PREPARE,
          State::D_VOTING,
          N,
          N,
          N,
          State::F_V_PREPARE,
          State::D_IDLE,
          N,
          State::V_IDLE,
          N,
          State::V_IDLE,
          State::D_IDLE},
      {State::V_IDLE,
          N,
          N,
          N,
          N,
          N,
          State::L_V_PREPARE,
          State::F_V_PREPARE,
          N,
          N,
          State::V_IDLE,
          State::L_V_PREPARE,
          State::V_IDLE,
          State::D_IDLE},
      {State::D_IDLE,
          N,
          N,
          N,
          N,
          State::D_SUCCESS,
          N,
          State::F_V_PREPARE,
          State::F_V_VOTING,
          N,
          State::V_SUCCESS,
          N,
          State::V_IDLE,
          State::D_IDLE},
      {State::D_IDLE,
          N,
          State::D_PREPARE,
          State::D_VOTING,
          N,
          N,
          N,
          State::F_V_PREPARE,
          N,
          N,
          State::V_SUCCESS,
          N,
          State::V_IDLE,
          State::D_IDLE},
      {State::D_IDLE, N, N, N, State::D_COUNTING, State::D_SUCCESS, N, N, N, N, State::V_SUCCESS, N, N, State::D_IDLE},
      {State::D_IDLE, N, N, N, N, State::D_SUCCESS, N, N, N, N, N, N, N, State::D_IDLE},
      {State::V_IDLE, N, N, N, N, N, N, N, N, N, N, State::L_V_PREPARE, N, State::D_IDLE},
      {State::V_IDLE,
          N,
          N,
          N,
          N,
          N,
          N,
          State::L_V_PREPARE,
          State::L_V_VOTING,
          N,
          N,
          State::L_V_PREPARE,
          N,
          State::D_IDLE},
      {State::V_IDLE,
          N,
          N,
          N,
          N,
          N,
          N,
          State::L_V_VOTING,
          State::L_V_VOTING,
          State::L_V_COUNTING,
          N,
          State::L_V_VOTING,
          N,
          State::D_IDLE},
      {State::V_IDLE,
          N,
          N,
          N,
          N,
          N,
          N,
          N,
          State::L_V_COUNTING,
          State::L_V_COUNTING,
          State::V_SUCCESS,
          State::F_V_VOTING,
          N,
          State::D_IDLE},
      {State::V_IDLE,
          N,
          N,
          N,
          N,
          N,
          N,
          State::F_V_PREPARE,
          State::F_V_VOTING,
          N,
          State::V_SUCCESS,
          State::V_SUCCESS,
          N,
          State::D_IDLE},
      {State::V_IDLE, N, N, N, N, N, N, N, N, N, State::V_SUCCESS, State::V_SUCCESS, N, State::D_IDLE},
      {State::V_IDLE, N, N, N, N, N, N, State::V_SUCCESS, State::V_SUCCESS, N, N, State::L_V_PREPARE, N, State::D_IDLE},
  };

  if (!Ops::is_valid(op)) {
    ELECT_ASYNC_LOG(WARN, "invalid argument", K(op));
    ret = OB_INVALID_ARGUMENT;
  } else if (!State::is_valid(state_)) {
    ELECT_ASYNC_LOG(WARN, "election state is invalid", K_(state), K(op));
    ret = OB_ERR_UNEXPECTED;
  } else {
    const int32_t new_state = STATE_MAP[state_][op];
    if (!State::is_valid(new_state)) {
      // ELECT_ASYNC_LOG(WARN, "election state not match", K_(state), K(op), K(new_state));
      ret = OB_STATE_NOT_MATCH;
    } else {
      state_ = new_state;
    }
  }

  return ret;
}

}  // namespace election
}  // namespace oceanbase
