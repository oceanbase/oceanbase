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

#ifndef OCEANBASE_SHARE_OB_LS_RECOVERY_STAT_OPERATOR_H_
#define OCEANBASE_SHARE_OB_LS_RECOVERY_STAT_OPERATOR_H_

#include "share/ob_ls_id.h"//share::ObLSID
#include "share/ls/ob_ls_i_life_manager.h" //ObLSLifeIAgent
#include "share/ob_tenant_role.h"//ObTenantRole
#include "common/ob_zone.h"//ObZone
#include "lib/container/ob_array.h"//ObArray
#include "lib/container/ob_iarray.h"//ObIArray
#include "logservice/palf/log_define.h"//SCN
#include "share/scn.h"//SCN

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
struct ObLSRecoveryStat
{
  ObLSRecoveryStat()
      : tenant_id_(OB_INVALID_TENANT_ID),
        ls_id_(),
        sync_scn_(),
        readable_scn_(),
        create_scn_(),
        drop_scn_() {}
  virtual ~ObLSRecoveryStat() {}
  bool is_valid() const;
  int init(const uint64_t tenant_id,
           const ObLSID &id,
           const SCN &sync_scn,
           const SCN &readable_scn,
           const SCN &create_scn,
           const SCN &drop_scn);
  int init_only_recovery_stat(const uint64_t tenant_id, const ObLSID &id,
                              const SCN &sync_scn,
                              const SCN &readable_scn);
  void reset();
  int assign(const ObLSRecoveryStat &other);
  uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  ObLSID get_ls_id() const
  {
    return ls_id_;
  }
  const SCN get_sync_scn() const
  {
    return sync_scn_;
  }
  const SCN get_readable_scn() const
  {
    return readable_scn_;
  }
  const SCN get_create_scn() const
  {
    return create_scn_;
  }
  const SCN get_drop_scn() const
  {
    return drop_scn_;
  }
  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(sync_scn), K_(readable_scn),
               K_(create_scn), K_(drop_scn));

 private:
  uint64_t tenant_id_;
  ObLSID ls_id_;
  SCN sync_scn_;//clog sync ts
  SCN readable_scn_;//min weak read timestamp TODO need different majorty replicas and all replicas
  SCN create_scn_;//ts less than first clog ts
  SCN drop_scn_; //ts larger than last user data's clog and before offline
};

/*
 * description: operation of __all_ls_recovery_stat
 * insert into __all_ls_recovery_stat while create new ls
 * delete from __all_ls_recovery_stat whild ls GC or create abort*/
class ObLSRecoveryStatOperator : public ObLSLifeIAgent,
                                 public ObLSTemplateOperator
{
 public:
  ObLSRecoveryStatOperator() {};
  virtual ~ObLSRecoveryStatOperator(){}

public:
  /*
   * description: override of ObLSLifeIAgent
   * @param[in] ls_info: ls info
   * @param[in] create_ls_scn: ls's create ts
   * @param[in] working_sw_status UNUSED
   * @param[in] trans:*/
  virtual int create_new_ls(const ObLSStatusInfo &ls_info,
                            const SCN &create_ls_scn,
                            const common::ObString &zone_priority,
                            const share::ObTenantSwitchoverStatus &working_sw_status,
                            ObMySQLTransaction &trans) override;
  /*
   * description: override of ObLSLifeIAgent
   * @param[in] tenant_id
   * @param[in] ls_id
   * @param[in] working_sw_status UNUSED
   * @param[in] trans:*/
  virtual int drop_ls(const uint64_t &tenant_id,
                      const share::ObLSID &ls_id,
                      const ObTenantSwitchoverStatus &working_sw_status,
                      ObMySQLTransaction &trans) override;
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
                      ObMySQLTransaction &trans) override;
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
      ObMySQLTransaction &trans) override
  {
    UNUSEDx(tenant_id, ls_id, primary_zone, zone_priority, trans);
    return OB_SUCCESS;
  } 

  /*
   * description:construct ls_recovery by read from inner table*/
  static int fill_cell(sqlclient::ObMySQLResult *result, ObLSRecoveryStat &ls_recovery);
public:
  /*
   * description: update ls recovery stat, only update sync_ts/readable_ts
   * @param[in] recovery_stat: new_recovery_stat
   * @param[in] only_update_readable_scn:only update readable_scn and check sync_scn is same
   * @param[in] proxy*/
  int update_ls_recovery_stat(const ObLSRecoveryStat &recovery_stat,
                              const bool only_update_readable_scn,
                              ObMySQLProxy &proxy);

  /*
   * description: update SYS LS sync scn
   * @param[in] tenant_id:  target user tenant id
   * @param[in] trans:      transaction cient
   * @param[in] sync_scn:   target scn to be updated*/
  int update_sys_ls_sync_scn(
      const uint64_t tenant_id,
      ObMySQLTransaction &trans,
      const SCN &sync_scn);

   /*
   * description: update ls recovery stat in trans, only update sync_ts/readable_ts.
                  only can update when tenant role is normal switchover status
   * @param[in] recovery_stat
   * @param[in] only_update_readable_scn:only update readable_scn and check sync_scn is same
   * @param[in] trans*/
  int update_ls_recovery_stat_in_trans(const ObLSRecoveryStat &recovery_stat,
                                       const bool only_update_readable_scn,
                                       ObMySQLTransaction &trans);
  /*
   * description: get ls recovery stat
   * @param[in] tenant_id
   * @param[in] ls_id
   * @param[in] for_update: select ... for update
   * @param[out] ls_recovery
   * @param[in] client*/
  int get_ls_recovery_stat(
      const uint64_t &tenant_id,
      const share::ObLSID &ls_id,
      const bool need_for_update,
      ObLSRecoveryStat &ls_recovery,
      ObISQLClient &client);
    /*
   * description: get tenant recovery stat, the min sync_scn and readable_ts of all ls
   * @param[in] tenant_id
   * @param[in] client
   * @param[out] sync_scn
   * @param[out] readable_scn 
   * */
  int get_tenant_recovery_stat(const uint64_t tenant_id,
                               ObISQLClient &client,
                               SCN &sync_scn,
                               SCN &min_wrs);

  /**
   * @description:
   *    get tenant min user ls create scn
   * @param[in] tenant_id
   * @param[in] client
   * @param[out] min_user_ls_create_scn
   * @return return code
   */
  int get_tenant_min_user_ls_create_scn(const uint64_t tenant_id,
                                        ObISQLClient &client,
                                        SCN &min_user_ls_create_scn);

    /*
   * description: get tenant max sync_scn across all ls
   * @param[in] tenant_id
   * @param[in] client
   * @param[out] max_sync_scn
   * */
  int get_tenant_max_sync_scn(const uint64_t tenant_id,
                              ObISQLClient &client,
                              SCN &max_sync_scn);

  /*
   * description: get user ls sync scn, for recovery ls
   * @param[in] tenant_id
   * @param[in] client
   * @param[out] sync scn : min sync_scn of all user ls
   * */
  int get_user_ls_sync_scn(const uint64_t tenant_id,
      ObISQLClient &client,
      SCN &sync_scn);
private:

  int get_min_create_scn_(const uint64_t tenant_id,
                          const common::ObSqlString &sql,
                          ObISQLClient &client,
                          SCN &min_create_scn);

 bool need_update_ls_recovery_(const ObLSRecoveryStat &old_recovery,
                               const ObLSRecoveryStat &new_recovery);
 int get_all_ls_recovery_stat_(const uint64_t tenant_id,
                               const common::ObSqlString &sql,
                               ObISQLClient &client,
                               SCN &sync_scn,
                               SCN &min_wrs);

};
}
}

#endif /* !OCEANBASE_SHARE_OB_LS_RECOVERY_STAT_OPERATOR_H_ */
