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

#ifndef OCEANBASE_COMMON_OB_ROLE_H_
#define OCEANBASE_COMMON_OB_ROLE_H_
#include <stdint.h>
namespace oceanbase
{
namespace common
{
class ObString;

enum ObRole
{
  INVALID_ROLE = 0,

  // Election Leader, supports strong consistent reading and writing, and supports member changes
  LEADER = 1,

  FOLLOWER = 2,

  // Election Leader, supports member changes, does not support strong consistent reading and writing
  // Currently used for physical standby database and physical restore partition (since 3.3)
  STANDBY_LEADER = 3,

  // The pseudo leader in the physical recovery process is not the election leader
  // Does not support member changes, does not support strong consistent read and write
  // This role has been deprecated since 3.3.
  // TODO: To be deleted later
  RESTORE_LEADER = 4,
};

// Is it a STRONG_LEADER role
bool is_strong_leader(const ObRole role);

// Is it STANDBY_LEADER role
bool is_standby_leader(const ObRole role);

// Is it a FOLLOWER role
bool is_follower(const ObRole role);

//////////////////////////////////////////////////////
// Utils function

// Whether it is the leader selected by election
//
// STRONG_LEADER + STANDBY_LEADER
//
// The CLOG layer has a member list and supports member changes
bool is_leader_by_election(const ObRole role);

// Aggregated judgment Leader interface
//
// STRONG_LEADER + STANDBY_LEADER + RESTORE_LEADER
bool is_leader_like(const ObRole role);

int role_to_string(const ObRole &role, char *role_str, const int64_t str_len);

int string_to_role(const char *role_str, ObRole &role);

const char *role_to_string(const ObRole &role);
int string_to_role(const ObString &role_str, ObRole &role);
}//end namespace common
}//end namespace oceanbase

#endif //OCEANBASE_COMMON_OB_ROLE_H_
