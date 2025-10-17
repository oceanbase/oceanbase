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

#ifndef OCEANBASE_STANDBY_OB_STANDBY_SERVICE_H_
#define OCEANBASE_STANDBY_OB_STANDBY_SERVICE_H_

#include "share/ob_rpc_struct.h"                          // ObAdminClusterArg
#include "share/ob_rs_mgr.h"                              // ObRsMgr
#include "lib/mysqlclient/ob_isql_client.h"               // ObISQLClient
#include "rootserver/ob_ddl_service.h"                    // ObDDLService
#include "share/schema/ob_multi_version_schema_service.h" // ObMultiVersionSchemaService
#include "rootserver/standby/ob_tenant_role_transition_service.h" // ObTenantRoleTransitionService

// usage: TENANT_ROLE_TRANS_USER_ERR_WITH_SUFFIX(OB_OP_NOT_ALLOW, "tenant status is not normal")
// the output to user will be "tenant status is not normal, switchover to primary is not allowed"
#define TENANT_ROLE_TRANS_USER_ERR_WITH_SUFFIX(TRT_ERR_RET, TRT_ERR_MSG, TRT_OP) \
({ \
  int tmp_ret = OB_SUCCESS; \
  ObSqlString err_msg; \
  if (OB_TMP_FAIL(err_msg.append_fmt(TRT_ERR_MSG))) {  \
    LOG_WARN("fail to assign error message", KR(tmp_ret));  \
  } else { \
    if (obrpc::ObSwitchTenantArg::OpType::SWITCH_TO_PRIMARY == TRT_OP) {  \
      tmp_ret = err_msg.append_fmt(", switchover to primary is"); \
    } else if (obrpc::ObSwitchTenantArg::OpType::SWITCH_TO_STANDBY == TRT_OP) {  \
      tmp_ret = err_msg.append_fmt(", switchover to standby is"); \
    } else if (obrpc::ObSwitchTenantArg::OpType::FAILOVER_TO_PRIMARY == TRT_OP) { \
      tmp_ret = err_msg.append_fmt(", failover to primary is"); \
    } else { \
      tmp_ret = err_msg.append_fmt(", this operation is"); \
    } \
    if (OB_SUCCESS != tmp_ret) { \
      LOG_WARN("fail to assign error message", KR(tmp_ret)); \
    } else { \
      LOG_USER_ERROR(TRT_ERR_RET, err_msg.ptr()); \
    } \
  }  \
})
namespace oceanbase
{

using namespace share;
using namespace rootserver;
namespace share
{
namespace schema
{
class ObMultiVersionSchemaService;
}
}

namespace standby
{

class ObStandbyService
{
public:
  ObStandbyService():
           sql_proxy_(NULL),
           schema_service_(NULL),
           inited_(false) {}
  virtual ~ObStandbyService() {}
  typedef obrpc::ObSwitchTenantArg::OpType RoleTransType;
  int init(ObMySQLProxy *sql_proxy,
           share::schema::ObMultiVersionSchemaService *schema_service);
  void destroy();

  /**
   * @description:
   *    switch tenant role
   * @param[in] arg 
   * @return return code
   */
  int switch_tenant(const obrpc::ObSwitchTenantArg &arg);

  /**
   * @description:
   *    set tenant recover checkpoint
   * @param[in] arg
   * @return return code
   */
  int recover_tenant(const obrpc::ObRecoverTenantArg &arg);

  int write_upgrade_barrier_log(ObMySQLTransaction &trans, const uint64_t tenant_id, const uint64_t data_version);
  int write_upgrade_data_version_barrier_log(ObMySQLTransaction &trans, const uint64_t tenant_id, const uint64_t data_version);

  /**
   * @description:
   *    do recover tenant
   * @param[in] tenant_id recover tenant_id
   * @param[in] working_sw_status recover tenant in expected switchover status
   * @param[in] recover_type recover type UNTIL/CANCEL
   * @param[in] recovery_until_scn
   * @return return code
   */
  int do_recover_tenant(
      const uint64_t tenant_id,
      const share::ObTenantSwitchoverStatus &working_sw_status,
      const obrpc::ObRecoverTenantArg::RecoverType &recover_type,
      const share::SCN &recovery_until_scn);

  /**
   * @description:
   *    check log_restore_source config, check whether can create net standby tenant
   * @param[in] log_restore_source log_restore_source config string
   * @param[out] compat_mode primary tenant compat_mode
   * @return return code
   */
  int check_can_create_standby_tenant(
      const common::ObString &log_restore_source,
      ObCompatibilityMode &compat_mode);

  /**
   * @description:
   *    wait net standby tenant create end
   * @param[in] tenant_id
   * @return return code
   */
  int wait_create_standby_tenant_end(const uint64_t tenant_id);
  /**
   * @description:
   *    get tenant status from all_tenant
   * @param[in] tenant_id
   * @param[out] status tenant status from all_tenant
   * @return return code
   */
  static int get_tenant_status(
      const uint64_t tenant_id,
      ObTenantStatus &status);


private:
  int check_inner_stat_();

  /**
   * @description:
   *    failover standby tenant to primary tenant
   * @param[in] tenant_id the standby tenant id to failover
   * @param[in] arg tenant switch arguments
   * @return return code
   */
  int failover_to_primary(
      const uint64_t tenant_id,
      const obrpc::ObSwitchTenantArg::OpType &switch_optype,
      const bool is_verify,
      const share::ObAllTenantInfo &tenant_info,
      share::SCN &switch_scn,
      ObTenantRoleTransCostDetail &cost_detail,
      ObTenantRoleTransAllLSInfo &all_ls);

  /**
   * @description:
   *    get target tenant_id from tenant_name to operate
   * @param[in] tenant_name tenant_name user specified
   * @param[in] exec_tenant_id user login session tenant_id
   * @param[out] target_tenant_id target tenant_id get from tenant_name
   * @return return code
   */
  int get_target_tenant_id(const ObString &tenant_name, const uint64_t exec_tenant_id, uint64_t &target_tenant_id);

  /**
   * @description:
   *    switch standby tenant to primary tenant
   * @param[in] tenant_id the standby tenant id to switch
   * @param[in] arg tenant switch arguments which include primary tenant switchover checkpoint
   * @return return code
   */
  int switch_to_primary(
      const uint64_t tenant_id,
      const obrpc::ObSwitchTenantArg::OpType &switch_optype,
      const bool is_verify,
      share::SCN &switch_scn,
      ObTenantRoleTransCostDetail &cost_detail,
      ObTenantRoleTransAllLSInfo &all_ls);

  /**
   * @description:
   *    switch primary tenant to standby tenant
   * @param[in] tenant_id the primary tenant id to switch
   * @return return code
   */
  int switch_to_standby(
      const uint64_t tenant_id,
      const obrpc::ObSwitchTenantArg::OpType &switch_optype,
      const bool is_verify,
      share::ObAllTenantInfo &tenant_info,
      share::SCN &switch_scn,
      ObTenantRoleTransCostDetail &cost_detail,
      ObTenantRoleTransAllLSInfo &all_ls);

  /**
   * @description:
   *    when do switchover, update tenant status before call common failover_to_primary interface
   *    before update, check current tenant status doesn't change, otherwise report error
   *    1. update tenant status to <PHYSICAL STANDBY, PREP SWITCHING TO PRIMARY>
   *    3. update sync_snapshot, replay_snapshot, recovery_until_snapshot to max{ all ls max_log_ts in check_point }
   * @param[in] cur_switchover_status
   * @param[in] cur_tenant_role
   * @param[in] cur_switchover_epoch
   * @param[in] tenant_id
   * @param[out] new_tenant_info  after update done, return new_tenant_info get in the same trans
   * @return return code
   */
  int sw_update_tenant_status_before_switch_to_primary_(
      const ObTenantSwitchoverStatus cur_switchover_status,
      const ObTenantRole cur_tenant_role,
      const int64_t cur_switchover_epoch,
      const uint64_t tenant_id,
      ObAllTenantInfo &new_tenant_info);

  /**
   * @description:
   *    update tenant to <PHYSICAL STANDBY, SWITCHING TO STANDBY>
   *    before update, check current tenant status doesn't change, otherwise report error
   *    after update done, return new_tenant_info get in the same trans
   * @param[in] cur_switchover_status
   * @param[in] cur_tenant_role
   * @param[in] cur_switchover_epoch
   * @param[in] tenant_id
   * @param[out] new_tenant_info  after update done, return new_tenant_info get in the same trans
   * @return return code
   */
  int update_tenant_status_before_sw_to_standby_(
      const ObTenantSwitchoverStatus cur_switchover_status,
      const ObTenantRole cur_tenant_role,
      const int64_t cur_switchover_epoch,
      const uint64_t tenant_id,
      ObAllTenantInfo &new_tenant_info);

  /**
   * @description:
   *    when switch to standby, prepare ls_status in all_ls and all_ls_status to proper status
   * @param[in] tenant_id the tenant id to check
   * @param[in] status only prepare in specified switchover status
   * @param[in] switchover_epoch only prepare in specified switchover epoch
   * @param[out] new_tenant_info return the updated tenant_info
   * @return return code
   */
  int switch_to_standby_prepare_ls_status_(
      const uint64_t tenant_id,
      const ObTenantSwitchoverStatus &status,
      const int64_t switchover_epoch,
      ObAllTenantInfo &new_tenant_info);

  /**
   * @description:
   *    check ls restore_status is normal
   * @param[in] tenant_id the tenant id to check
   * @return return code
   */
  int check_ls_restore_status_(const uint64_t tenant_id);
  int write_barrier_log_(const transaction::ObTxDataSourceType type,
                         ObMySQLTransaction &trans,
                         const uint64_t tenant_id,
                         const uint64_t data_version);

  int check_if_tenant_status_is_normal_(const uint64_t tenant_id, const RoleTransType op_type);
  void tenant_event_start_(const uint64_t switch_tenant_id, const obrpc::ObSwitchTenantArg &arg,
      int ret, int64_t begin_ts, const share::ObAllTenantInfo &tenant_info);
  void tenant_event_end_(const uint64_t switch_tenant_id, const obrpc::ObSwitchTenantArg &arg,
      int ret, int64_t cost, int64_t end_ts, const share::SCN switch_scn,
      ObTenantRoleTransCostDetail &cost_detail, ObTenantRoleTransAllLSInfo &all_ls);
private:
  const static int64_t SEC_UNIT = 1000L * 1000L;
  const static int64_t PRINT_INTERVAL = 10 * 1000 * 1000L;

  ObMySQLProxy *sql_proxy_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
  bool inited_;
};

class ObStandbyServiceGetter
{
public:
  static ObStandbyService &get_instance()
  {
    static ObStandbyService standby_service;
    return standby_service;
  }
};

#define OB_STANDBY_SERVICE (oceanbase::standby::ObStandbyServiceGetter::get_instance())

}  // end namespace standby
}  // end namespace oceanbase

#endif  // OCEANBASE_STANDBY_OB_STANDBY_SERVICE_H_
