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

#ifndef OCEABASE_STORAGE_LS_STATE_
#define OCEABASE_STORAGE_LS_STATE_
namespace oceanbase
{
namespace storage
{
enum ObLSState
{
  LS_INIT = 0,
  LS_F_WORKING, // 1
  LS_L_TAKEOVERED, // 2
  LS_L_WORKING, // 3
  LS_OFFLINE, // 4
  LS_INVALID_STATE, // 5
};

inline bool is_leader_state(const ObLSState &state)
{
  return (LS_L_TAKEOVERED == state || LS_L_WORKING == state);
}

inline bool is_leader_working(const ObLSState &state)
{
  return (LS_L_WORKING == state);
}

inline bool is_follower_state(const ObLSState &state)
{
  return (LS_F_WORKING == state);
}

inline bool is_working_state(const ObLSState &state)
{
  return (state >= LS_F_WORKING && state <= LS_L_WORKING);
}

enum class ObInnerLSStatus
{
  CREATING = 0,
  COMMITTED, // 1
  ABORTED, // 2
  REMOVED // 3
};

inline bool can_update_ls_meta(const ObInnerLSStatus &status)
{
  return (ObInnerLSStatus::COMMITTED == status);
}

}
}
#endif
