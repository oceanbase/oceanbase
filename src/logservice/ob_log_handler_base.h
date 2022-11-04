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

#ifndef OCEANBASE_LOGSERVICE_OB_LOG_HANDLER_BASE_
#define OCEANBASE_LOGSERVICE_OB_LOG_HANDLER_BASE_
#include <cstdint>
#include "lib/lock/ob_tc_rwlock.h"
#include "common/ob_role.h"
#include "palf/palf_handle.h"

namespace oceanbase
{
namespace common
{
class ObAddr;
}
namespace palf
{
class PalfEnv;
}
namespace logservice
{
class ObLogHandlerBase
{
public:
  ObLogHandlerBase();
  // @breif query role and proposal_id from ObLogHandlerBase and palf.
  // @param[out], curr_role, role of ObLogHandler.
  // @param[out], curr_proposal_id, proposal_id of ObLogHandler.
  // @param[out], new_role, role of palf.
  // @param[out], new_proposal_id, proposal_id of palf.
  int prepare_switch_role(common::ObRole &curr_role,
                          int64_t &curr_proposal_id,
                          common::ObRole &new_role,
                          int64_t &new_proposal_id,
                          bool &is_pending_state) const;
  // NB: only called by ObRoleChangeService
  virtual void switch_role(const common::ObRole &role, const int64_t proposal_id) = 0;
  int revoke_leader();
  int change_leader_to(const common::ObAddr &dst_addr);
public:
  typedef common::RWLock RWLock;
  typedef RWLock::RLockGuard RLockGuard;
  typedef RWLock::WLockGuard WLockGuard;
  mutable RWLock lock_;
  common::ObRole role_;
  int64_t proposal_id_;
  int64_t id_;
  palf::PalfHandle palf_handle_;
  palf::PalfEnv *palf_env_;
};
} // end namespace logservice
} // end namespace oceanbase
#endif
