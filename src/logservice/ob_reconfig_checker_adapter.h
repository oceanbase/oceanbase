/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(timeout));
public:
  virtual int check_can_add_member(const ObMember &member,
                                   const int64_t timeout_us) override final;
  virtual int check_can_change_memberlist(
      const ObMemberList &new_member_list,
      const int64_t paxos_replica_num,
      const palf::LogConfigVersion &config_version,
      const int64_t timeout_us) override final;
private:
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  int64_t timeout_;
  rootserver::ObLSRecoveryGuard guard_;
};

} // logservice
} // oceanbase

#endif
