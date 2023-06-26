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

#ifndef OCEANBASE_SHARE_OB_LS_LIFE_MANAGER_H_
#define OCEANBASE_SHARE_OB_LS_LIFE_MANAGER_H_

#include "share/ls/ob_ls_status_operator.h"       //ObLSStatusInfo, ObLSStatusOperator
#include "share/ls/ob_ls_recovery_stat_operator.h"//ObLSRecoveryStatOperator
#include "share/ls/ob_ls_i_life_manager.h"
#include "share/ls/ob_ls_election_reference_info_operator.h"

namespace oceanbase
{
namespace common
{
class ObMySQLTransaction;
class ObMySQLProxy;
namespace sqlclient
{
class ObMySQLResult;
}
}
namespace share
{
class SCN;
struct ObLSStatusInfo;
class ObLSID;
/*
 * description:
 * When a log stream is created or deleted, multiple inner_tables need to be modified at the same time,
 * and each internal table operation needs to be registered in this class
 * */
class ObLSLifeAgentManager
{
 public:
  ObLSLifeAgentManager (common::ObMySQLProxy &proxy) : inited_(false)
  {
    proxy_ = &proxy;
    STATIC_ASSERT(MAX_AGENT_NUM == ARRAYSIZEOF(agents_), "too small agent array");
    //set agent to table_operator
    agents_[STATUS_AGENT] = &status_operator_;
    agents_[RECOVERY_AGENT] = &recovery_operator_;
    agents_[ELECTION_AGENT] = &election_operator_;
    inited_ = true;
  }
  virtual ~ObLSLifeAgentManager() {}

public:
  //each ls agent type
  enum LSAgentType
  {
    STATUS_AGENT  = 0,
    RECOVERY_AGENT = 1,
    ELECTION_AGENT = 2,
    MAX_AGENT_NUM
  };
public:
  /*
   * description: for primary cluster, create new ls
   * @param[in]ls_info:new ls status info for __all_ls_status
   * @param[in]create_ls_scn: the create_ls_scn of the ls, it is current GTS of the tenant, used for __all_ls_recovery_stat
   * @param[in]zone_priority: the primary_zone of OB_ALL_LS_ELECTION_REFERENCE_INFO
   * @param[in] working_sw_status only support working on specified switchover status
   * */
  DEFINE_IN_TRANS_FUC(create_new_ls, const ObLSStatusInfo &ls_info,\
                    const SCN &create_ls_scn,\
                    const common::ObString &zone_priority,\
                    const share::ObTenantSwitchoverStatus &working_sw_status)
  /*
   * description: for primary cluster and GC of standby, delete ls from each inner_table
   * @param[in] tenant_id: tenant_id
   * @param[in] ls_id: need delete ls
   * @param[in] working_sw_status only support working on specified switchover status
   * */
  DEFINE_IN_TRANS_FUC(drop_ls, const uint64_t &tenant_id,\
              const share::ObLSID &ls_id,\
              const ObTenantSwitchoverStatus &working_sw_status)
  /*
   * description: for primary cluster set ls to wait offline from tenant_dropping or dropping status 
   * @param[in] tenant_id: tenant_id
   * @param[in] ls_id: need delete ls
   * @param[in] ls_status: tenant_dropping or dropping status 
   * @param[in] drop_scn: there is no user data after drop_scn except offline
   * @param[in] working_sw_status only support working on specified switchover status
   * */
  DEFINE_IN_TRANS_FUC(set_ls_offline, const uint64_t &tenant_id,\
                      const share::ObLSID &ls_id,\
                      const ObLSStatus &ls_status,\
                      const SCN &drop_scn,\
                      const ObTenantSwitchoverStatus &working_sw_status)
  /*
   * description: update ls primary zone, need update __all_ls_status and __all_ls_election_reference 
   * @param[in] tenant_id: tenant_id
   * @param[in] ls_id: need update ls
   * @param[in] primary_zone: primary zone of __all_ls_status 
   * @param[in] zone_priority: primary zone of __all_ls_election_reference 
   * */
  int update_ls_primary_zone(
      const uint64_t &tenant_id,
      const share::ObLSID &ls_id,
      const common::ObZone &primary_zone,
      const common::ObString &zone_priority);

private:
  bool inited_;
  //table_operator
  ObLSLifeIAgent *agents_[MAX_AGENT_NUM];
  ObLSStatusOperator status_operator_;
  ObLSRecoveryStatOperator recovery_operator_;
  ObLsElectionReferenceInfoOperator election_operator_;
  common::ObMySQLProxy *proxy_;
  DISALLOW_COPY_AND_ASSIGN(ObLSLifeAgentManager);
};
}
}

#endif /* !OCEANBASE_SHARE_OB_LS_LIFE_MANAGER_H_ */
