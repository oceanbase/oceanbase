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

#ifndef OCEANBASE_LOGSERVICE_OB_RECONFIG_CHECKER_ADAPTER_H_
#define OCEANBASE_LOGSERVICE_OB_RECONFIG_CHECKER_ADAPTER_H_

#include <stdint.h>
#include "logservice/palf/palf_callback.h"
#include "lib/net/ob_addr.h"
#include "rootserver/ob_ls_recovery_stat_handler.h"

namespace oceanbase
{
namespace logservice
{
class ObReconfigCheckerAdapter : public palf::PalfReconfigCheckerCb
{
public:
  explicit ObReconfigCheckerAdapter();
  virtual ~ObReconfigCheckerAdapter() { }
  int init(const uint64_t tenant_id, const share::ObLSID &ls_id, const int64_t &timeout = -1);
public:
  virtual int check_can_add_member(const ObMember &member,
                                   const int64_t timeout_us) override final;
  virtual int check_can_change_memberlist(const ObMemberList &new_member_list,
                                          const int64_t paxos_replica_num,
                                          const int64_t timeout_us) override final;
private:
  rootserver::ObLSRecoveryGuard guard_;
};

} // logservice
} // oceanbase

#endif
