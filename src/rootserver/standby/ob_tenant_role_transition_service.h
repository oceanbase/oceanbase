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

#include "share/ob_rpc_struct.h"// ObSwitchTenantArg
#include "logservice/palf/palf_options.h"//access mode
#include "logservice/palf/log_define.h"//INVALID_PROPOSAL_ID
#include "share/ob_tenant_info_proxy.h"
#include "share/ls/ob_ls_status_operator.h"//ObLSStatusOperator
#include "share/balance/ob_balance_task_helper_operator.h"//ObBalanceTaskHelper
#include "share/balance/ob_balance_task_table_operator.h"

namespace oceanbase
{
namespace obrpc
{
class  ObSrvRpcProxy;
struct ObLSAccessModeInfo;
}
namespace common
{
class ObMySQLProxy;
class ObMySQLTransaction;
}
namespace share
{
class SCN;
struct ObAllTenantInfo;
}

namespace rootserver 
{

using namespace share;
class ObTenantRoleTransitionConstants
{
public:
  static constexpr int64_t PRIMARY_UPDATE_LS_RECOVERY_STAT_TIME_US = 1000 * 1000;  // 1s
  static constexpr int64_t STANDBY_UPDATE_LS_RECOVERY_STAT_TIME_US = 100 * 1000;  // 100ms
  static constexpr int64_t STS_TENANT_INFO_REFRESH_TIME_US = 100 * 1000;  // 100ms
  static constexpr int64_t DEFAULT_TENANT_INFO_REFRESH_TIME_US = 1000 * 1000;  // 1s
  static constexpr int64_t TENANT_INFO_LEASE_TIME_US = 2 * DEFAULT_TENANT_INFO_REFRESH_TIME_US;  // 2s
  static const char* const SWITCH_TO_PRIMARY_LOG_MOD_STR;
  static const char* const SWITCH_TO_STANDBY_LOG_MOD_STR;
  static const char* const RESTORE_TO_STANDBY_LOG_MOD_STR;
};

/**
  * @description:
  *    Find the ls_id that are not in the array_r from array_l, and refresh the location of these ls
  * @param[in] array_l
  * @param[in] array_r
  * @param[in] tenant_id
  * @return return code
  */
template <typename ARRAY>
int do_nonblock_renew(const ARRAY &array_l, const ARRAY &array_r, const uint64_t tenant_id);

struct ObTenantRoleTransCostDetail
{
public:
  enum CostType {
    WAIT_LOG_SYNC = 0,
    WAIT_BALANCE_TASK,
    LOG_FLASHBACK,
    WAIT_LOG_END,
    CHANGE_ACCESS_MODE,
    WAIT_REBUILD_MASTER_KEY,
    MAX_COST_TYPE
  };
  const char* type_to_str(CostType type) const;
public:
  ObTenantRoleTransCostDetail() : cost_type_{}, start_(0), end_(0) {}
  ~ObTenantRoleTransCostDetail() {}
  void set_start(int64_t start) { start_ = start; }
  void add_cost(CostType type, int64_t cost);
  void set_end(int64_t end) { end_ = end; }
  int64_t get_wait_log_end () { return cost_type_[WAIT_LOG_END]; }
  int64_t to_string (char *buf, const int64_t buf_len) const ;
private:
  int64_t cost_type_[MAX_COST_TYPE];
  int64_t start_;
  int64_t end_;
};

struct ObTenantRoleTransAllLSInfo
{
public:
  ObTenantRoleTransAllLSInfo() : all_ls_{} {}
  ~ObTenantRoleTransAllLSInfo() {}
  int init();
  int add_ls(const ObLSID &ls_id, const ObLSStatus status);
  int64_t to_string (char *buf, const int64_t buf_len) const;
  bool is_valid() const;
private:
  ObArray<ObLSID> all_ls_[ObLSStatus::OB_LS_MAX_STATUS];
};

struct ObTenantRoleTransNonSyncInfo
{
public:
  ObTenantRoleTransNonSyncInfo() : is_sync_(true), not_sync_checkpoints_() {}
  ~ObTenantRoleTransNonSyncInfo() {}
  int init(const ObArray<obrpc::ObCheckpoint> &switchover_checkpoints);
  int64_t to_string (char *buf, const int64_t buf_len) const;
  bool is_sync() const { return is_sync_; }
private:
  static constexpr int64_t MAX_PRINT_LS_NUM = 5;
  bool is_sync_;
  ObArray<obrpc::ObCheckpoint> not_sync_checkpoints_;
};

class ObLSAccessModeModifier {
public:
  ObLSAccessModeModifier(const uint64_t tenant_id, const uint64_t switchover_epoch, const SCN &ref_scn,
    const SCN &sys_ls_sync_scn, const palf::AccessMode target_access_mode,
    const share::ObLSStatusInfoArray *status_info_array,
    common::ObMySQLProxy *sql_proxy, obrpc::ObSrvRpcProxy *rpc_proxy)
    : tenant_id_(tenant_id), switchover_epoch_(switchover_epoch), ls_wait_sync_scn_max_(0),
      ref_scn_(ref_scn), sys_ls_sync_scn_(sys_ls_sync_scn), target_access_mode_(target_access_mode),
      status_info_array_(status_info_array), sql_proxy_(sql_proxy), rpc_proxy_(rpc_proxy) {}
  ~ObLSAccessModeModifier() {}
  int change_ls_access_mode();
  int64_t get_ls_wait_sync_scn_max() const { return ls_wait_sync_scn_max_; }
private:
  int get_ls_access_mode_(ObIArray<obrpc::ObLSAccessModeInfo> &ls_access_info);
  int do_change_ls_access_mode_(const ObIArray<obrpc::ObLSAccessModeInfo> &ls_access_info);
  int check_inner_stat_() const;
  uint64_t tenant_id_;
  uint64_t switchover_epoch_;
  int64_t ls_wait_sync_scn_max_;
  SCN ref_scn_;
  SCN sys_ls_sync_scn_;
  palf::AccessMode target_access_mode_;
  const share::ObLSStatusInfoArray *status_info_array_;
  common::ObMySQLProxy *sql_proxy_;
  obrpc::ObSrvRpcProxy *rpc_proxy_;
};

/*description:
 * for primary to standby and standby to primary
 */
class ObTenantRoleTransitionService
{
public:
  ObTenantRoleTransitionService()
    : tenant_id_(OB_INVALID_TENANT_ID), sql_proxy_(NULL),
    rpc_proxy_(NULL), switchover_epoch_(OB_INVALID_VERSION),
    switch_optype_(obrpc::ObSwitchTenantArg::OpType::INVALID),
    so_scn_(),
    cost_detail_(NULL),
    all_ls_info_(NULL),
    has_restore_source_(false),
    is_verify_(false) {}
  virtual ~ObTenantRoleTransitionService() {}
  int init(
      uint64_t tenant_id,
      const obrpc::ObSwitchTenantArg::OpType &switch_optype,
      const bool is_verify,
      common::ObMySQLProxy *sql_proxy,
      obrpc::ObSrvRpcProxy *rpc_proxy,
      ObTenantRoleTransCostDetail *cost_detail,
      ObTenantRoleTransAllLSInfo *all_ls_info);
  int failover_to_primary();
  int check_inner_stat();
  int do_switch_access_mode_to_append(const share::ObAllTenantInfo &tenant_info,
                             const share::ObTenantRole &target_tenant_role);
  int do_switch_access_mode_to_raw_rw(const share::ObAllTenantInfo &tenant_info);
  int get_tenant_ref_scn_(const share::SCN &sync_scn, share::SCN &ref_scn);
  //before primary tenant switchover to standby, must set sys LS's sync_scn to lastest
  int report_sys_ls_sync_scn_();
  void set_switchover_epoch(const int64_t switchover_epoch)
  {
    switchover_epoch_ = switchover_epoch;
  }
  /**
   * @description:
   *    Update scn/tenant_role/switchover status when switchover is executed
   *    scn is the current sync point obtained through rpc
   * @param[in] tenant_id
   * @param[in] switch_to_primary switch_to_primary or switch_to_standby
   * @param[in] new_role new tenant role
   * @param[in] old_status current switchover status
   * @param[in] new_status new switchover status
   * @param[in] old_switchover_epoch current switchover epoch
   * @param[out] new_tenant_info return the updated tenant_info
   * @return return code
   */
  int switchover_update_tenant_status(
      const uint64_t tenant_id,
      const bool switch_to_primary,
      const ObTenantRole &new_role,
      const ObTenantSwitchoverStatus &old_status,
      const ObTenantSwitchoverStatus &new_status,
      const int64_t old_switchover_epoch,
      ObAllTenantInfo &new_tenant_info);

  int wait_sys_ls_sync_to_latest_until_timeout_(const uint64_t tenant_id, ObAllTenantInfo &tenant_info);
  /**
   * @description:
   *    wait tenant sync to switchover checkpoint until timeout
   * @param[in] tenant_id
   * @param[in] primary_checkpoints primary switchover checkpoint
   * @return return code
   */
  int wait_tenant_sync_to_latest_until_timeout_(const uint64_t tenant_id, const ObAllTenantInfo &tenant_info);
  int check_restore_source_for_switchover_to_primary_(const uint64_t tenant_id);

  /**
   * @description:
   *    do the checking to see whether the standby tenant ls has synchronize to primary tenant checkpoints
   * @param[in] tenant_id the tenant id to check
   * @param[in] primary_checkpoints primary switchover checkpoint
   * @param[out] has_sync_to_checkpoint whether the standby tenant sync to primary tenant checkpoints
   * @return return code
   */
  int check_sync_to_restore_source_(const uint64_t tenant_id,
                                    const ObAllTenantInfo &tenant_info,
                                    bool &has_sync_to_checkpoint);

  void broadcast_tenant_info(const char* const log_mode);

  /**
   * @description:
   *    get specified ls list sync_scn by rpc, which is named as checkpoint
   * @param[in] tenant_id the tenant to get switchover checkpoint
   * @param[in] status_info_array ls list to get sync scn
   * @param[in] get_latest_scn whether to get latest scn
   * @param[out] checkpoints switchover checkpoint
   * @return return code
   */
  static int get_checkpoints_by_rpc(
      const uint64_t tenant_id,
      const share::ObLSStatusInfoIArray &status_info_array,
      const bool get_latest_scn,
      ObIArray<obrpc::ObCheckpoint> &checkpoints
  );
  share::SCN get_so_scn() const { return so_scn_; }

private:
  int do_failover_to_primary_(const share::ObAllTenantInfo &tenant_info);
  int do_prepare_flashback_(share::ObAllTenantInfo &tenant_info);
  int do_flashback_();
  int get_status_and_change_ls_access_mode_(
      palf::AccessMode target_access_mode,
      const SCN &ref_scn);

  /**
   * @description:
   *    get max ls sync_scn across all ls in checkpoints array
   * @param[in] checkpoints switchover checkpoint
   * @param[out] max_checkpoint_scn
   * @return return code
   */
  int get_max_checkpoint_scn_(
      const ObIArray<obrpc::ObCheckpoint> &checkpoints,
      share::SCN &max_checkpoint_scn);

  /**
   * @description:
   *    get sys ls sync_snapshot from checkpoints array
   * @param[in] checkpoints checkpoint
   * @return return code
   */
  int get_sys_ls_sync_scn_(
    const uint64_t tenant_id,
    const bool need_check_sync_to_latest,
    share::SCN &sys_ls_sync_scn,
    bool &is_sync_to_latest);
  int get_sys_ls_sync_scn_(
      const ObIArray<obrpc::ObCheckpoint> &checkpoints,
      share::SCN &sys_ls_sync_scn,
      bool &is_sync_to_latest);
  /**
   * @description:
   *    wait tenant/sys ls sync to switchover checkpoint until timeout
   * @param[in] tenant_id
   * @param[in] only_check_sys_ls true: only wait sys ls sync; false: wait tenant sync
   * @return return code
   */
  int check_sync_to_latest_do_while_(
    const ObAllTenantInfo &tenant_info,
    const bool only_check_sys_ls);

  /**
   * @description:
   *    when switch to primary, check all ls are sync to latest
   * @param[in] tenant_id the tenant id to check
   * @param[in] tenant_info
   * @param[out] is_all_ls_synced whether sync to latest
   * @return return code
   */
  int check_sync_to_latest_(
      const uint64_t tenant_id,
      const bool only_check_sys_ls,
      const ObAllTenantInfo &tenant_info,
      bool &is_sys_ls_synced,
      bool &is_all_ls_synced);

  int do_prepare_flashback_for_switch_to_primary_(share::ObAllTenantInfo &tenant_info);
  int do_prepare_flashback_for_failover_to_primary_(share::ObAllTenantInfo &tenant_info);
  int check_and_update_sys_ls_recovery_stat_in_switchover_(
      const uint64_t tenant_id,
      const bool switch_to_primary,
      ObMySQLTransaction &trans,
      const SCN &max_sys_ls_sync_scn/* SYS LS real max sync scn */,
      const SCN &target_tenant_sync_scn/* tenant target sync scn in switchover */);
  int wait_ls_balance_task_finish_();
  int wait_rebuild_master_key_version_finish_();
  int check_ls_balance_task_finish_(
      ObBalanceTaskArray &balance_task_array, ObArray<ObBalanceTaskHelper> &ls_balance_tasks,
      share::ObAllTenantInfo &cur_tenant_info, bool &is_finish);
  int notify_recovery_ls_service_();
  int get_all_ls_status_and_change_access_mode_(
      const palf::AccessMode target_access_mode,
      const share::SCN &ref_scn,
      const share::SCN &sys_ls_sync_scn);
  int ls_status_stats_when_change_access_mode_(const share::ObLSStatusInfoArray &status_info_array);
  void try_print_wait_balance_task_user_error_(
    const share::ObAllTenantInfo &cur_tenant_info,
    const ObArray<ObBalanceTaskHelper> &ls_balance_tasks,
    const ObBalanceTaskArray &balance_task_array,
    const char * const op_str);

private:
  const static int64_t SEC_UNIT = 1000L * 1000L;
  const static int64_t PRINT_INTERVAL = 1000L * 1000L;

private:
  uint64_t tenant_id_;
  common::ObMySQLProxy *sql_proxy_;
  obrpc::ObSrvRpcProxy *rpc_proxy_;
  int64_t switchover_epoch_;
  obrpc::ObSwitchTenantArg::OpType switch_optype_;
  share::SCN so_scn_;
  ObTenantRoleTransCostDetail *cost_detail_;
  ObTenantRoleTransAllLSInfo *all_ls_info_;
  bool has_restore_source_;
  bool is_verify_;
};
}
}


#endif /* !OCEANBASE_ROOTSERVER_OB_TENANT_ROLE_TRANSITION_SERVICE_H */
