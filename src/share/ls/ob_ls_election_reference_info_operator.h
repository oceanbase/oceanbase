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

#ifndef OCEANBASE_SHARE_OB_LS_ELECTION_REFERNCE_INFO_H_
#define OCEANBASE_SHARE_OB_LS_ELECTION_REFERNCE_INFO_H_

#include "share/ob_ls_id.h"//share::ObLSID
#include "share/ls/ob_ls_i_life_manager.h" //ObLSLifeIAgent
#include "common/ob_zone.h"//ObZone
#include "lib/container/ob_array.h"//ObArray
#include "lib/container/ob_iarray.h"//ObIArray
#include "logservice/palf/lsn.h"//LSN
#include "share/ob_tenant_switchover_status.h"

namespace oceanbase
{

namespace common
{
class ObMySQLProxy;
class ObString;
class ObSqlString;
class ObIAllocator;
class ObISQLClient;
class ObMySQLTransaction;
namespace sqlclient
{
class ObMySQLResult;
}
}
namespace share
{
class SCN;
/*
 * description: operation of __all_ls_election_reference_info
 * insert into __all_ls_election_reference_info while create new ls
 * delete from __all_ls_election_reference_info whild ls GC or create abort*/
class ObLsElectionReferenceInfoOperator : public ObLSLifeIAgent,
                                          public ObLSTemplateOperator
{
 public:
  ObLsElectionReferenceInfoOperator() {};
  virtual ~ObLsElectionReferenceInfoOperator(){}

public:
  /*
   * description: override of ObLSLifeIAgent
   * @param[in] ls_info: ls info
   * @param[in] create_ls_scn: ls's create scn
   * @param[in] working_sw_status UNUSED
   * @param[in] trans:*/
  virtual int create_new_ls(const ObLSStatusInfo &ls_info,
                            const SCN &create_ls_scn,
                            const common::ObString &zone_priority,
                            const share::ObTenantSwitchoverStatus &working_sw_status,
                            ObMySQLTransaction &trans) override final;
  /*
   * description: override of ObLSLifeIAgent
   * @param[in] tenant_id
   * @param[in] ls_id
   * @param[in] working_sw_status UNUSED
   * @param[in] trans:*/
  virtual int drop_ls(const uint64_t &tenant_id,
                      const share::ObLSID &ls_id,
                      const ObTenantSwitchoverStatus &working_sw_status,
                      ObMySQLTransaction &trans) override final;
  /*
   * description: for primary cluster set ls to wait offline from tenant_dropping or dropping status 
   * @param[in] tenant_id: tenant_id
   * @param[in] ls_id: need delete ls
   * @param[in] ls_status: tenant_dropping or dropping status 
   * @param[in] drop_scn: there is no user data after drop_scn except offline
   * @param[in] working_sw_status UNUSED
   * @param[in] trans
   * */
  virtual int set_ls_offline(const uint64_t &tenant_id,
                      const share::ObLSID &ls_id,
                      const ObLSStatus &ls_status,
                      const SCN &drop_scn,
                      const ObTenantSwitchoverStatus &working_sw_status,
                      ObMySQLTransaction &trans) override final;
  /*
   * description: update ls primary zone, need update __all_ls_status and __all_ls_election_reference 
   * @param[in] tenant_id: tenant_id
   * @param[in] ls_id: need update ls
   * @param[in] primary_zone: primary zone of __all_ls_status 
   * @param[in] zone_priority: primary zone of __all_ls_election_reference 
   * @param[in] trans
   * */
  int update_ls_primary_zone(
      const uint64_t &tenant_id,
      const share::ObLSID &ls_id,
      const common::ObZone &primary_zone,
      const common::ObString &zone_priority,
      ObMySQLTransaction &trans) override final; 

};
}
}

#endif /* !OCEANBASE_SHARE_OB_LS_ELECTION_REFERNCE_INFO_H_ */
