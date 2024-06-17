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

#ifndef OCEANBASE_SHARE_OB_TENANT_INFO_PROXY_H_
#define OCEANBASE_SHARE_OB_TENANT_INFO_PROXY_H_

#include "share/ob_ls_id.h"//share::ObLSID
#include "share/ob_tenant_role.h"//ObTenantRole
#include "share/ob_tenant_switchover_status.h"//ObTenantSwitchoverStatus
#include "share/backup/ob_archive_mode.h"
#include "lib/container/ob_array.h"//ObArray
#include "lib/container/ob_iarray.h"//ObIArray
#include "share/scn.h"
//#include "share/ls/ob_ls_status_operator.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"  //ObMySQLTransaction
#include "share/ls/ob_ls_i_life_manager.h" // share::OB_LS_INVALID_SCN_VALUE
#include "share/restore/ob_restore_data_mode.h" //share::ObRestoreDataMode

namespace oceanbase
{

namespace common
{
class ObMySQLProxy;
class ObISQLClient;
class ObMySQLTransaction;
namespace sqlclient
{
class ObMySQLResult;
}
}
namespace share
{

bool is_valid_tenant_scn(
  const share::SCN &sync_scn,
  const share::SCN &replayable_scn,
  const share::SCN &standby_scn,
  const share::SCN &recovery_until_scn);

SCN gen_new_sync_scn(const share::SCN &cur_sync_scn, const share::SCN &desired_sync_scn, const share::SCN &cur_recovery_until_scn);

struct ObAllTenantInfo
{
  OB_UNIS_VERSION(1);
public:
 ObAllTenantInfo() {reset();};
 virtual ~ObAllTenantInfo() {}
 /**
  * @description: init_tenant_info by cluster_info
  * @param[in] tenant_id
  * @param[in] tenant_role
  * @param[in] switchover_status
  * @param[in] switchover_epoch
  * @param[in] sync_scn
  * @param[in] replayable_scn
  * @param[in] standby_scn
  * @param[in] recovery_until_scn
  * @param[in] log_mode
  * @param[in] restore_data_mode
  */
 int init(const uint64_t tenant_id,
          const ObTenantRole &tenant_role,
          const ObTenantSwitchoverStatus &switchover_status = NORMAL_SWITCHOVER_STATUS ,
          const int64_t switchover_epoch = 0,
          const SCN &sync_scn = SCN::base_scn(),
          const SCN &replayable_scn = SCN::base_scn(),
          const SCN &standby_scn = SCN::base_scn(),
          const SCN &recovery_until_scn = SCN::base_scn(),
          const ObArchiveMode &log_mode = NOARCHIVE_MODE,
          const share::ObLSID &max_ls_id = share::SYS_LS,
          const share::ObRestoreDataMode &restore_data_mode = NORMAL_RESTORE_DATA_MODE);
 ObAllTenantInfo &operator=(const ObAllTenantInfo &other);
 void assign(const ObAllTenantInfo &other);
 void reset();
 bool is_valid() const;

 // ObTenantRole related function
 bool is_standby() const { return tenant_role_.is_standby(); }
 bool is_primary() const { return tenant_role_.is_primary(); }
 bool is_restore() const { return tenant_role_.is_restore(); }
 bool is_clone() const { return tenant_role_.is_clone(); }

 /**
  * @description:
  * STS is STANDBY tenant's GTS. It is ready only when tenant is STANDBY and not in 'switchover to standby' status.
  * Because STS will be changed when switchover to standby.
  */
 bool is_sts_ready() const { return !(tenant_role_.is_primary()
                                      || tenant_is_switchover_to_standby()); }

 // ************* Functions that describe what tenant is doing *********************
 // tenant is in switchover from primary to standby
 bool tenant_is_switchover_to_standby() const { return is_prepare_switching_to_standby_status()
                                                       || is_switching_to_standby_status(); }

 // ObTenantSwitchoverStatus related function
#define IS_TENANT_STATUS(STATUS) \
  bool is_##STATUS##_status() const { return switchover_status_.is_##STATUS##_status(); };

IS_TENANT_STATUS(normal) 
IS_TENANT_STATUS(switching_to_primary)
IS_TENANT_STATUS(prepare_flashback_for_failover_to_primary)
IS_TENANT_STATUS(flashback) 
IS_TENANT_STATUS(switching_to_standby)
IS_TENANT_STATUS(prepare_switching_to_standby)
IS_TENANT_STATUS(prepare_flashback_for_switch_to_primary)
#undef IS_TENANT_STATUS 

 TO_STRING_KV(K_(tenant_id), K_(tenant_role), K_(switchover_status),
              K_(switchover_epoch), K_(sync_scn), K_(replayable_scn),
              K_(standby_scn), K_(recovery_until_scn), K_(log_mode), K_(max_ls_id), K_(restore_data_mode));
  DECLARE_TO_YSON_KV;

  // Getter&Setter
  const ObTenantRole &get_tenant_role() const { return tenant_role_; }
  const ObTenantSwitchoverStatus &get_switchover_status() const { return switchover_status_; }
  void set_max_ls_id(const share::ObLSID ls_id)
  {
    max_ls_id_ = ls_id;
  }

#define Property_declare_var(variable_type, variable_name)\
private:\
  variable_type variable_name##_;\
public:\
  variable_type get_##variable_name() const\
  { return variable_name##_;}

  Property_declare_var(uint64_t, tenant_id)
  Property_declare_var(int64_t, switchover_epoch)
  Property_declare_var(share::SCN, sync_scn)
  Property_declare_var(share::SCN, replayable_scn)
  Property_declare_var(share::SCN, standby_scn)
  Property_declare_var(share::SCN, recovery_until_scn)
  Property_declare_var(ObArchiveMode, log_mode)
  Property_declare_var(share::ObLSID, max_ls_id)
  Property_declare_var(share::ObRestoreDataMode, restore_data_mode)
#undef Property_declare_var
private:
  ObTenantRole tenant_role_;
  ObTenantSwitchoverStatus switchover_status_;
};

class ObAllTenantInfoProxy
{
public:
  ObAllTenantInfoProxy() {};
  virtual ~ObAllTenantInfoProxy(){}

public:
  static const int64_t DEFAULT_MAX_LS_ID = 0;//for upgrade
  /**
   * @description: init_tenant_info to inner table while create tenant
   * @param[in] tenant_info 
   * @param[in] proxy
   */ 
  static int init_tenant_info(const ObAllTenantInfo &tenant_info, ObISQLClient *proxy);

  static int is_standby_tenant(
      ObISQLClient *proxy,
      const uint64_t tenant_id,
      bool &is_standby);
  static int is_primary_tenant(
      ObISQLClient *proxy,
      const uint64_t tenant_id,
      bool &is_primary);
  static int get_tenant_role(
      ObISQLClient *proxy,
      const uint64_t tenant_id,
      ObTenantRole &tenant_role);

  static int get_primary_tenant_ids(
             ObISQLClient *proxy,
             ObIArray<uint64_t> &tenant_ids);
  /**
   * @description: get target tenant's tenant_info from inner table 
   * @param[in] tenant_id
   * @param[in] proxy
   * @param[out] tenant_info 
   */
  static int load_tenant_info(const uint64_t tenant_id, ObISQLClient *proxy,
                              const bool for_update,
                              ObAllTenantInfo &tenant_info);

  /**
   * @description: get target tenant's tenant_info from inner table
   * @param[in] tenant_id
   * @param[in] proxy
   * @param[in] for_update select for_update
   * @param[out] ora_rowscn
   * @param[out] tenant_info
   */
  static int load_tenant_info(const uint64_t tenant_id, ObISQLClient *proxy,
                              const bool for_update,
                              int64_t &ora_rowscn,
                              ObAllTenantInfo &tenant_info);
  /**
   * @description: load tenant_info fro __all_tenant_info without fix max_ls_id
   * @param[in] tenant_id
   * @param[in] proxy
   * @param[out] tenant_info
   * @param[out] max_ls_id of __all_tenant_info
   */
private:
    static int load_pure_tenant_info_(const uint64_t tenant_id, ObISQLClient *proxy,
                                const bool for_update,
                                int64_t &ora_rowscn,
                                ObAllTenantInfo &tenant_info);
  public:
    /**
     * @description: update tenant recovery status
     * @param[in] tenant_id
     * @param[in] trans
     * @param[in] old_tenant_info : tenant_info get from load for update
     * @param[in] status: the target status while update recovery status
     * @param[in] sync_scn : sync point
     * @param[in] replay_scn : max replay point
     * @param[in] reabable_scn : standby readable scn
     */
    static int update_tenant_recovery_status_in_trans(const uint64_t tenant_id,
                                             ObMySQLTransaction &trans,
                                             const ObAllTenantInfo &old_tenant_info,
                                             const SCN &sync_scn,
                                             const SCN &replay_scn,
                                             const SCN &reabable_scn);
    /**
     * @description: update tenant switchover status of __all_tenant_info
     * @param[in] tenant_id : user tenant id
     * @param[in] proxy
     * @param[in] switchover_epoch, for operator concurrency
     * @param[in] old_status : old_status of current, which must be match
     * @param[in] status : target switchover status to be update
     * return :
     *   OB_SUCCESS update tenant switchover status successfully
     *   OB_NEED_RETRY switchover_epoch or old_status not match, need retry
     */
    static int update_tenant_switchover_status(const uint64_t tenant_id, ObISQLClient *proxy,
                                  int64_t switchover_epoch,
                                  const ObTenantSwitchoverStatus &old_status,
                                  const ObTenantSwitchoverStatus &status);

    /**
     * @description: update tenant status in trans
     * @param[in] tenant_id
     * @param[in] trans
     * @param[in] new_role : target tenant role to be update
     * @param[in] status : target switchover status to be update
     * @param[in] sync_scn
     * @param[in] replayable_scn
     * @param[in] readable_scn
     * @param[in] recovery_until_scn
     * return :
     *   OB_SUCCESS update tenant role successfully
     */
    static int update_tenant_status(
      const uint64_t tenant_id,
      common::ObMySQLTransaction &trans,
      const ObTenantRole new_role,
      const ObTenantSwitchoverStatus &old_status,
      const ObTenantSwitchoverStatus &new_status,
      const share::SCN &sync_scn,
      const share::SCN &replayable_scn,
      const share::SCN &readable_scn,
      const share::SCN &recovery_until_scn,
      const int64_t old_switchover_epoch);

    /**
     * @description: update tenant role of __all_tenant_info
     * @param[in] tenant_id
     * @param[in] proxy
     * @param[in] old_switchover_epoch, for operator concurrency
     * @param[in] new_role : target tenant role to be update
     * @param[in] old_status : old switchover status
     * @param[in] new_status : target switchover status to be update
     * @param[out] new_switchover_epoch, for operator concurrency
     * return :
     *   OB_SUCCESS update tenant role successfully
     *   OB_NEED_RETRY old_switchover_epoch not match, need retry
     */
    static int update_tenant_role(
      const uint64_t tenant_id,
      common::ObMySQLProxy *proxy,
      int64_t old_switchover_epoch,
      const ObTenantRole &new_role,
      const ObTenantSwitchoverStatus &old_status,
      const ObTenantSwitchoverStatus &new_status,
      int64_t &new_switchover_epoch);

    /**
     * @description: update tenant role of __all_tenant_info in trans, make sure conflict with clone
     * @param[in] tenant_id
     * @param[in] trans
     * @param[in] old_switchover_epoch, for operator concurrency
     * @param[in] new_role : target tenant role to be update
     * @param[in] old_status : old switchover status
     * @param[in] new_status : target switchover status to be update
     * @param[out] new_switchover_epoch, for operator concurrency
     * return :
     *   OB_SUCCESS update tenant role successfully
     *   OB_NEED_RETRY old_switchover_epoch not match, need retry
     */
    static int update_tenant_role_in_trans(
      const uint64_t tenant_id,
      ObMySQLTransaction &trans,
      int64_t old_switchover_epoch,
      const ObTenantRole &new_role,
      const ObTenantSwitchoverStatus &old_status,
      const ObTenantSwitchoverStatus &new_status,
      int64_t &new_switchover_epoch);

    static int fill_cell(common::sqlclient::ObMySQLResult *result, ObAllTenantInfo &tenant_info, int64_t &ora_rowscn);
     /**
     * @description: update tenant max ls id while create ls or upgrade, in upgrade from 4100 to 4200,
     *               create ls no need to update max ls id while max_ls_id is zero. after update max_ls_id in post_upgrade,
     *               max_ls_id can not be zero, create ls can update.
     * @param[in] tenant_id
     * @param[in] max_ls_id : max ls id
     * @param[in] trans : must be in trans
     * @param[in] for_upgrade : if for upgrade, must update max ls id
     * return :
     *   OB_SUCCESS update tenant max ls id success
     *   OB_ERR_UNEXPECTED max ls id can not fallback, need retry
     */
    static int update_tenant_max_ls_id(const uint64_t tenant_id, const share::ObLSID &max_ls_id,
                                       ObMySQLTransaction &trans, const bool for_upgrade);

    /**
     * @description: update tenant recovery_until_scn in trans
     * @param[in] tenant_id
     * @param[in] trans
     * @param[in] recovery_until_scn : target recovery_until_scn to be updated
     * return :
     *   OB_SUCCESS update recovery_until_scn successfully
     */
    static int update_tenant_recovery_until_scn(
      const uint64_t tenant_id,
      common::ObMySQLTransaction &trans,
      const int64_t switchover_epoch,
      const share::SCN &recovery_until_scn);

    /**
     * @description: update switchover epoch when entering and leaving normal switchover status
     * @param[in] old_switchover_epoch
     * @param[in] old_status old switchover status
     * @param[in] new_status new switchover status
     * @param[out] new_switchover_epoch
     */
    static int get_new_switchover_epoch_(
      const int64_t old_switchover_epoch,
      const ObTenantSwitchoverStatus &old_status,
      const ObTenantSwitchoverStatus &new_status,
      int64_t &new_switchover_epoch);

    /**
     * @description: update tenant log mode in normal switchover status
     * @param[in] tenant_id
     * @param[in] trans
     * @param[in] old_log_mode old log mode
     * @param[in] new_log_mode new log mode
     */
    static int update_tenant_log_mode(
      const uint64_t tenant_id,
      ObISQLClient *proxy,
      const ObArchiveMode &old_log_mode,
      const ObArchiveMode &new_log_mode);

};
}
}

#endif /* !OCEANBASE_SHARE_OB_TENANT_INFO_PROXY_H_ */
