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
#include "lib/string/ob_string.h" // ObString

namespace oceanbase
{
namespace common
{

bool is_strong_leader(const ObRole role)
{
  return LEADER == role;
}

bool is_standby_leader(const ObRole role)
{
  // leader of standby cluster or physical restore partition
  return STANDBY_LEADER == role;
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
  return is_leader_by_election(role);
}

int role_to_string(const ObRole &role, char *role_str, const int64_t str_len)
{
  int ret = OB_SUCCESS;
  if (LEADER == role) {
    strncpy(role_str ,"LEADER", str_len);
  } else if (FOLLOWER == role) {
    strncpy(role_str ,"FOLLOWER", str_len);
  } else {
    ret = OB_INVALID_ARGUMENT;
  }
  return ret;
}

int string_to_role(const char *role_str, ObRole &role)
{
  int ret = OB_SUCCESS;

  if (0 == strcmp("LEADER", role_str)) {
    role = LEADER;
  } else if (0 == strcmp("FOLLOWER", role_str)) {
    role = FOLLOWER;
  } else {
    role = INVALID_ROLE;
    ret = OB_INVALID_ARGUMENT;
  }

  return ret;
}

const char *role_to_string(const ObRole &role)
{
  #define CHECK_OB_ROLE_STR(x) case(ObRole::x): return #x
  switch(role)
  {
    CHECK_OB_ROLE_STR(LEADER);
    CHECK_OB_ROLE_STR(FOLLOWER);
    CHECK_OB_ROLE_STR(STANDBY_LEADER);
    default:
      return "INVALID_ROLE";
  }
  #undef CHECK_OB_ROLE_STR
}

int string_to_role(const ObString &role_str, ObRole &role)
{
  int ret = OB_SUCCESS;
  if (0 == role_str.compare("LEADER")) {
    role = LEADER;
  } else if (0 == role_str.compare("FOLLOWER")) {
    role = FOLLOWER;
  } else {
    role = INVALID_ROLE;
    ret = OB_INVALID_ARGUMENT;
  }
  return ret;
}

}//end namespace common
}//end namespace oceanbase

