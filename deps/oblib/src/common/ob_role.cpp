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

#define USING_LOG_PREFIX COMMON

#include "common/ob_role.h"
#include "lib/ob_define.h"

namespace oceanbase {
namespace common {

bool is_strong_leader(const ObRole role)
{
  return LEADER == role;
}

bool is_standby_leader(const ObRole role)
{
  return STANDBY_LEADER == role;
}

bool is_restore_leader(const ObRole role)
{
  return RESTORE_LEADER == role;
}

bool is_follower(const ObRole role)
{
  return FOLLOWER == role;
}

bool is_leader_by_election(const ObRole role)
{
  return is_strong_leader(role) || is_standby_leader(role);
}

bool is_leader_like(const ObRole role)
{
  return is_leader_by_election(role) || is_restore_leader(role);
}

}  // end namespace common
}  // end namespace oceanbase
