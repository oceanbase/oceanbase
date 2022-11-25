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

#ifndef OCEANBASE_COMMON_OB_ROLE_MGR_
#define OCEANBASE_COMMON_OB_ROLE_MGR_

#include "lib/atomic/ob_atomic.h"

namespace oceanbase
{
namespace common
{
/// @brief ObRoleMgr manages the role and status of the process
class ObRoleMgr
{
public:
  static const int32_t OB_MASTER = 1;
  static const int32_t OB_SLAVE = 2;
  enum Role
  {
    // TODO baihua: remove MASTER, SLAVE
    MASTER = 1,
    SLAVE = 2,
    STANDALONE = 3, // used for test
    LEADER = 1,
    FOLLOWER = 2,
  };

  /// @brief Master and Slave status bits
  /// ERROR state: error state
  /// INIT state: Initialization
  /// ACTIVE status: Provide normal service
  /// SWITCHING: The state in the process of switching Slave to Master
  /// STOP:
  /// The sequence of state transition in the process of switching from Slave to Master is:
  ///     (SLAVE, ACTIVE) -> (MASTER, SWITCHING) -> (MASTER, ACTIVE)
  enum State
  {
    ERROR = 1,
    INIT = 2,
    NOTSYNC = 3,
    ACTIVE = 4,
    SWITCHING = 5,
    STOP = 6,
    HOLD = 7,
    BOOTOK = 8,
  };

public:
  ObRoleMgr() : role_(SLAVE), state_(INIT) {}

  virtual ~ObRoleMgr() {}

  /// @brief Get Role
  inline Role get_role() const;

  /// @brief modify Role
  inline void set_role(const Role role);

  inline const char *get_role_str() const;

  inline bool is_master() const;

  /// Get State
  inline State get_state() const;

  /// Modify State
  inline void set_state(const State state);

  inline const char *get_state_str() const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObRoleMgr);

private:
  volatile Role role_;
  volatile State state_;
};

/// @brief Get Role
ObRoleMgr::Role ObRoleMgr::get_role() const
{
  return role_;
}

/// @brief modify Role
void ObRoleMgr::set_role(const ObRoleMgr::Role role)
{
  COMMON_LOG(INFO, "before", K(get_role_str()), K(get_state_str()));
  (void)ATOMIC_TAS(reinterpret_cast<volatile uint32_t *>(&role_), role);
  COMMON_LOG(INFO, "after", K(get_role_str()), K(get_state_str()));
}

const char *ObRoleMgr::get_role_str() const
{
  switch (role_) {
    case MASTER:
      return "MASTER";
    case SLAVE:
      return "SLAVE";
    case STANDALONE:
      return "STANDALONE";
    default:
      return "UNKNOWN";
  }
}

bool ObRoleMgr::is_master() const
{
  return (role_ == ObRoleMgr::MASTER) && (state_ == ObRoleMgr::ACTIVE);
}


ObRoleMgr::State ObRoleMgr::get_state() const
{
  return state_;
}

void ObRoleMgr::set_state(const State state)
{
  COMMON_LOG(INFO, "before", K(get_state_str()), K(get_role_str()));
  (void)ATOMIC_TAS(reinterpret_cast<volatile uint32_t *>(&state_), state);
  COMMON_LOG(INFO, "after", K(get_state_str()), K(get_role_str()));
}

const char *ObRoleMgr::get_state_str() const
{
  switch (state_) {
    case ERROR:
      return "ERROR";
    case INIT:
      return "INIT";
    case NOTSYNC:
      return "NOTSYNC";
    case ACTIVE:
      return "ACTIVE";
    case SWITCHING:
      return "SWITCHING";
    case STOP:
      return "STOP";
    case HOLD:
      return "HOLD";
    case BOOTOK:
      return "BOOTOK";
    default:
      return "UNKNOWN";
  }
}

} // end namespace common
} // end namespace oceanbase

#endif // OCEANBASE_COMMON_OB_ROLE_MGR_
