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

#include "ob_reconfig_checker_adapter.h"
#include "storage/ls/ob_ls.h"

namespace oceanbase
{
namespace logservice
{

ObReconfigCheckerAdapter::ObReconfigCheckerAdapter() :
    guard_()
  {}

int ObReconfigCheckerAdapter::init(const uint64_t tenant_id,
                                   const share::ObLSID &ls_id,
                                   const int64_t &timeout)
{
  return guard_.init(tenant_id, ls_id, timeout);
}

int ObReconfigCheckerAdapter::check_can_add_member(const ObMember &member,
                                                   const int64_t timeout_us)
{
  return guard_.check_can_add_member(member, timeout_us);
}

int ObReconfigCheckerAdapter::check_can_change_memberlist(const ObMemberList &new_member_list,
                                                          const int64_t paxos_replica_num,
                                                          const int64_t timeout_us)
{
  return guard_.check_can_change_member(new_member_list, paxos_replica_num, timeout_us);
}

} // end namespace logservice
} // end namespace oceanbase
