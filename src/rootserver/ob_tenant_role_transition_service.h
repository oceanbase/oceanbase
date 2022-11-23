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

#ifndef OCEANBASE_ROOTSERVER_OB_TENANT_ROLE_TRANSITION_SERVICE_H
#define OCEANBASE_ROOTSERVER_OB_TENANT_ROLE_TRANSITION_SERVICE_H

#include "logservice/palf/palf_options.h"//access mode
#include "logservice/palf/log_define.h"//INVALID_PROPOSAL_ID
#include "share/ob_tenant_info_proxy.h"

namespace oceanbase
{
namespace obrpc
{
class  ObSrvRpcProxy;
}
namespace common
{
class ObMySQLProxy;
class ObMySQLTransaction;
}
namespace share
{
struct ObAllTenantInfo;
}

namespace rootserver 
{
/*description:
 * for primary to standby and standby to primary
 */
class ObTenantRoleTransitionService
{
public:
struct LSAccessModeInfo
{
  LSAccessModeInfo(): tenant_id_(OB_INVALID_TENANT_ID), ls_id_(),
                    leader_addr_(),
                    mode_version_(palf::INVALID_PROPOSAL_ID),
                    access_mode_(palf::AccessMode::INVALID_ACCESS_MODE) { }
  virtual ~LSAccessModeInfo() {}
  bool is_valid() const
  {
    return OB_INVALID_TENANT_ID != tenant_id_
         && ls_id_.is_valid()
         && leader_addr_.is_valid()
         && palf::INVALID_PROPOSAL_ID != mode_version_
         && palf::AccessMode::INVALID_ACCESS_MODE != access_mode_;

  }
  int init(uint64_t tenant_id, const share::ObLSID &ls_id, const ObAddr &addr,
           const int64_t mode_version, const palf::AccessMode &access_mode);
  int assign(const LSAccessModeInfo &other);
  void reset();
  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(leader_addr),
               K_(mode_version), K_(access_mode));
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  ObAddr leader_addr_;
  int64_t mode_version_;
  palf::AccessMode access_mode_;
};

public:
  ObTenantRoleTransitionService(const uint64_t tenant_id,
      common::ObMySQLProxy *sql_proxy,
      obrpc::ObSrvRpcProxy *rpc_proxy)
    : tenant_id_(tenant_id), sql_proxy_(sql_proxy),
    rpc_proxy_(rpc_proxy), switchover_epoch_(OB_INVALID_VERSION) {}
  virtual ~ObTenantRoleTransitionService() {}
  int failover_to_primary();
  int check_inner_stat();
private:
  int do_failover_to_primary_(const share::ObAllTenantInfo &tenant_info);
  int do_prepare_flashback_(const share::ObAllTenantInfo &tenant_info);
  int do_flashback_(const share::ObAllTenantInfo &tenant_info);
  int do_switch_access_mode_(const share::ObAllTenantInfo &tenant_info,
                             const share::ObTenantRole &target_tenant_role);
  int try_create_abort_ls_(const share::ObTenantSwitchoverStatus &status);
  int change_ls_access_mode_(palf::AccessMode target_access_mode,
                             int64_t ref_ts);
  int update_tenant_stat_info_();
  int get_ls_access_mode_(ObIArray<LSAccessModeInfo> &ls_access_info);
  int do_change_ls_access_mode_(const ObIArray<LSAccessModeInfo> &ls_access_info,
                                palf::AccessMode target_access_mode,
                                int64_t ref_ts);
private:
  uint64_t tenant_id_;
  common::ObMySQLProxy *sql_proxy_;
  obrpc::ObSrvRpcProxy *rpc_proxy_;
  int64_t switchover_epoch_;
};
}
}


#endif /* !OCEANBASE_ROOTSERVER_OB_TENANT_ROLE_TRANSITION_SERVICE_H */
