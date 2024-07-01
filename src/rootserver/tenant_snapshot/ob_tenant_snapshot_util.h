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

#ifndef __OB_RS_TENANT_SNAPSHOT_UTIL_H__
#define __OB_RS_TENANT_SNAPSHOT_UTIL_H__

#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "share/scn.h"

namespace oceanbase
{
namespace share
{
class ObTenantSnapshotID;
enum class ObTenantSnapStatus : int64_t;
class ObTenantSnapItem;
class ObTenantSnapLSReplicaSimpleItem;
class ObUnit;
class ObResourcePool;
}
namespace rootserver
{
const char *const CLONE_PROCEDURE_STR = "create snapshot or clone tenant";

class ObConflictCaseWithClone
{
  OB_UNIS_VERSION(1);
public:
  enum ConflictCaseWithClone
  {
    INVALID_CASE_NAME = -1,
    UPGRADE = 0,
    TRANSFER = 1,
    MODIFY_RESOURCE_POOL = 2,
    MODIFY_UNIT = 3,
    MODIFY_LS = 4,
    MODIFY_REPLICA = 5,
    MODIFY_TENANT_ROLE_OR_SWITCHOVER_STATUS = 6,
    DELAY_DROP_TENANT = 7,
    STANDBY_UPGRADE = 8,
    STANDBY_TRANSFER = 9,
    STANDBY_MODIFY_LS = 10,
    MAX_CASE_NAME
  };
public:
  ObConflictCaseWithClone() : case_name_(INVALID_CASE_NAME) {}
  explicit ObConflictCaseWithClone(ConflictCaseWithClone case_name) : case_name_(case_name) {}

  ObConflictCaseWithClone &operator=(const ConflictCaseWithClone case_name) { case_name_ = case_name; return *this; }
  ObConflictCaseWithClone &operator=(const ObConflictCaseWithClone &other) { case_name_ = other.case_name_; return *this; }
  void reset() { case_name_ = INVALID_CASE_NAME; }
  int64_t to_string(char *buf, const int64_t buf_len) const;
  void assign(const ObConflictCaseWithClone &other) { case_name_ = other.case_name_; }
  bool operator==(const ObConflictCaseWithClone &other) const { return other.case_name_ == case_name_; }
  bool operator!=(const ObConflictCaseWithClone &other) const { return other.case_name_ != case_name_; }
  bool is_valid() const { return INVALID_CASE_NAME < case_name_ && MAX_CASE_NAME > case_name_; }
  bool is_upgrade() const { return UPGRADE == case_name_; }
  bool is_transfer() const { return TRANSFER == case_name_; }
  bool is_modify_resource_pool() const { return MODIFY_RESOURCE_POOL == case_name_; }
  bool is_modify_unit() const { return MODIFY_UNIT == case_name_; }
  bool is_modify_ls() const { return MODIFY_LS == case_name_; }
  bool is_modify_replica() const { return MODIFY_REPLICA == case_name_; }
  bool is_modify_tenant_role_or_switchover() const { return MODIFY_TENANT_ROLE_OR_SWITCHOVER_STATUS == case_name_; }
  bool is_delay_drop_tenant() const { return DELAY_DROP_TENANT == case_name_; }
  bool is_standby_upgrade() const { return STANDBY_UPGRADE == case_name_; }
  bool is_standby_transfer() const { return STANDBY_TRANSFER == case_name_; }
  bool is_standby_modify_ls() const { return STANDBY_MODIFY_LS == case_name_; }
  bool is_standby_related() const { return STANDBY_UPGRADE == case_name_ || STANDBY_TRANSFER == case_name_ || STANDBY_MODIFY_LS == case_name_; }
  const ConflictCaseWithClone &get_case_name() const { return case_name_; }
  const char* get_case_name_str() const;
private:
  ConflictCaseWithClone case_name_;
};

class ObTenantSnapshotUtil
{
public:
  /*snapshot operation*/
  enum TenantSnapshotOp : int8_t
  {
    CREATE_OP = 0,
    DROP_OP = 1,
    RESTORE_OP = 2,   // a type of clone job
    FORK_OP = 3,      // a type of clone job
    MAX,
  };
public:
  static const char* get_op_print_str(const TenantSnapshotOp &op);

public:
  static int create_tenant_snapshot(const ObString &tenant_name,
                                    const ObString &tenant_snapshot_name,
                                    uint64_t &tenant_id,
                                    share::ObTenantSnapshotID &tenant_snapshot_id);
  static int create_fork_tenant_snapshot(ObMySQLTransaction &trans,
                                         const uint64_t target_tenant_id,
                                         const ObString &target_tenant_name,
                                         const ObString &tenant_snapshot_name,
                                         const share::ObTenantSnapshotID &tenant_snapshot_id);
  static int drop_tenant_snapshot(const ObString &tenant_name,
                                  const ObString &tenant_snapshot_name);
  static int get_tenant_id(const ObString &tenant_name,
                           uint64_t &tenant_id);
  static int check_source_tenant_info(const uint64_t tenant_id,
                                      const TenantSnapshotOp op);
  static int check_tenant_status(const uint64_t tenant_id,
                                 bool &is_satisfied);
  static int check_log_archive_ready(const uint64_t tenant_id, const ObString &tenant_name);
  static int trylock_tenant_snapshot_simulated_mutex(ObMySQLTransaction &trans,
                                                     const uint64_t tenant_id,
                                                     const TenantSnapshotOp op,
                                                     const int64_t owner_job_id,
                                                     share::ObTenantSnapStatus &original_global_state_status);
  static int unlock_tenant_snapshot_simulated_mutex_from_clone_release_task(ObMySQLTransaction &trans,
                                                                            const uint64_t tenant_id,
                                                                            const int64_t owner_job_id,
                                                                            const share::ObTenantSnapStatus &old_status,
                                                                            bool &is_already_unlocked);
  static int unlock_tenant_snapshot_simulated_mutex_from_snapshot_task(ObMySQLTransaction &trans,
                                                                       const uint64_t tenant_id,
                                                                       const share::ObTenantSnapStatus &old_status,
                                                                       const share::SCN &snapshot_scn);
  static int add_create_tenant_snapshot_task(ObMySQLTransaction &trans,
                                             const uint64_t tenant_id,
                                             const ObString &snapshot_name,
                                             const share::ObTenantSnapshotID &tenant_snapshot_id);
  static int add_drop_tenant_snapshot_task(ObMySQLTransaction &trans,
                                           const uint64_t tenant_id,
                                           const ObString &snapshot_name);
  static int get_tenant_snapshot_info(common::ObISQLClient &sql_client,
                                      const uint64_t source_tenant_id,
                                      const ObString &snapshot_name,
                                      share::ObTenantSnapItem &item);
  static int get_tenant_snapshot_info(common::ObISQLClient &sql_client,
                                      const uint64_t source_tenant_id,
                                      const share::ObTenantSnapshotID &snapshot_id,
                                      share::ObTenantSnapItem &item);
  static int add_clone_tenant_task(ObMySQLTransaction &trans,
                                   const uint64_t tenant_id,
                                   const share::ObTenantSnapshotID &tenant_snapshot_id);
  static int add_clone_tenant_task(ObMySQLTransaction &trans,
                                   const share::ObTenantSnapItem &snap_item);
  static int generate_tenant_snapshot_name(const uint64_t tenant_id,
                                           ObSqlString &tenant_snapshot_name,
                                           bool is_inner = false);
  static int generate_tenant_snapshot_id(const uint64_t tenant_id,
                                         share::ObTenantSnapshotID &tenant_snapshot_id);
  static int check_and_get_data_version(const uint64_t tenant_id,
                                        uint64_t &data_version);
  static int check_tenant_has_snapshot(common::ObISQLClient &sql_client,
                                       const uint64_t tenant_id,
                                       bool &has_snapshot);
  static int notify_scheduler(const uint64_t tenant_id);
  static int recycle_tenant_snapshot_ls_replicas(common::ObISQLClient &sql_client,
                                                 const uint64_t tenant_id,
                                                 const ObString &tenant_snapshot_name);
  static int recycle_tenant_snapshot_ls_replicas(common::ObISQLClient &sql_client,
                                                 const uint64_t tenant_id,
                                                 const share::ObTenantSnapshotID &tenant_snapshot_id);
  // functions to check conflict conditions between clone and other operations
  // some procedure need to conflict with clone, so need to check whether in clone procedure
  //   1. ObDRTaskMgr::persist_task_info_                    (can not do dr-task)
  //   2. ObTenantBalanceService::persist_job_and_task_      (can not do transfer)
  //   3. ObUpgradeExecutor::run_upgrade_begin_action_       (can not do upgrade)
  //   4. ObTenantSqlService::delay_to_drop_tenant           (can not do delay drop tenant)
  //   5. ObAllTenantInfoProxy::update_tenant_role_in_trans  (can not do switchover)
  //      ObAllTenantInfoProxy::update_tenant_status         (can not do switchover)
  //   6. ObUnitTableOperator::update_unit                   (can not modify unit)
  //   7. ObUnitTableOperator::update_resource_pool          (can not modify resource pool)
  //   8. ObLSAttrOperator::operator_ls_in_trans_            (can not modify ls)
  //      ObLSAttrOperator::alter_ls_group_in_trans          (can not modify ls)
  //      ObLSAttrOperator::update_ls_flag_in_trans          (can not modify ls)
  static int check_tenant_not_in_cloning_procedure(
         const uint64_t tenant_id,
         const ObConflictCaseWithClone &case_to_check);

  static int cancel_existed_clone_job_if_need(
         const uint64_t tenant_id,
         const ObConflictCaseWithClone &case_to_check);

  // check whether standby tenant is in clonong procedure when replay log
  static int check_standby_tenant_not_in_cloning_procedure(
         common::ObMySQLTransaction &trans,
         const uint64_t tenant_id,
         bool &is_cloning);

  static int check_tenant_has_no_conflict_tasks(const uint64_t tenant_id);

  // when update __all_unit, in some case we have to lock __all_unit first and then check clone
  // called by ObUnitTableOperator::update_unit
  static int lock_unit_for_tenant(
             common::ObISQLClient &client,
             const share::ObUnit &unit,
             uint64_t &tenant_id_to_lock);

  // when update __all_resource_pool, in some case we have to lock __all_resource_pool first and then check clone
  // called by ObUnitTableOperator::update_resource_pool
  static int lock_resource_pool_for_tenant(
             common::ObISQLClient &client,
             const share::ObResourcePool &resource_pool);

  // when update __all_tenant, inn some case we have to lock __all_tenant first and then check clone
  // called by ObTenantSqlService::delay_to_drop_tenant
  static int lock_status_for_tenant(
             ObMySQLTransaction &trans,
             const uint64_t tenant_id_to_check);

  // inner lock __all_tenant for lock_status_for_tenant and check_tenant_is_in_dropping_procedure_
  static int inner_lock_line_for_all_tenant_table_(
             const uint64_t tenant_id_to_lock,
             ObISQLClient &sql_proxy,
             share::schema::ObTenantStatus &tenant_status);
  // get tenant current/target data version from __all_core_table
  // and compatible version in __tenant_parameter
  static int check_current_and_target_data_version(
             const uint64_t tenant_id,
             uint64_t &data_version);
private:
  // check whether tenant is in cloning procedure in trans
  // @params[in]  user_tenant_id, the tenant to check
  // @params[out] is_tenant_in_cloning, the output
  //
  // is_tenant_in_cloning = false if one of conditions below is satisfied
  //   (1) tenant is not up to version 4.3, clone is not supported
  //   (2) line with snapshot_id = 0 in __all_tenant_snapshot not exists
  //   (3) line with snapshot_id = 0 in __all_tenant_snapshot exists but is NORMAL status
  //       and no snapshot item exists in __all_tenant_snapshot_ls for this tenant
  static int check_tenant_in_cloning_procedure_in_trans_(
         const uint64_t user_tenant_id,
         bool &is_tenant_in_cloning);
  static int inner_check_tenant_in_cloning_procedure_in_trans_(
         common::ObMySQLTransaction &trans,
         const uint64_t tenant_id,
         bool &is_tenant_in_cloning);
  static int check_tenant_snapshot_simulated_mutex_(ObMySQLTransaction &trans,
                                                    const uint64_t tenant_id,
                                                    share::ObTenantSnapItem &special_item,
                                                    bool &is_conflict);
  static int unlock_(ObMySQLTransaction &trans,
                     const uint64_t tenant_id,
                     const int64_t owner_job_id,
                     const share::ObTenantSnapStatus &old_status,
                     const share::SCN &snapshot_scn,
                     bool &is_conflicted_owner_job_id);
  static int lock_(ObMySQLTransaction &trans,
                   const uint64_t tenant_id,
                   const TenantSnapshotOp op,
                   const share::ObTenantSnapItem &special_item);
  // check whether tenant is upgrading
  // @params[in]  tenant_id, tenant to check
  static int check_tenant_is_in_upgrading_procedure_(const uint64_t tenant_id,
                                                     uint64_t &data_version);
  static int check_tenant_is_in_dropping_procedure_(const uint64_t tenant_id);
  static int check_tenant_is_in_transfer_procedure_(const uint64_t tenant_id);
  static int check_tenant_is_in_modify_resource_pool_procedure_(const uint64_t tenant_id);
  static int check_tenant_is_in_modify_unit_procedure_(const uint64_t tenant_id);
  static int check_tenant_is_in_modify_ls_procedure_(const uint64_t tenant_id);
  static int check_tenant_is_in_modify_replica_procedure_(const uint64_t tenant_id);
  static int check_tenant_is_in_switchover_procedure_(const uint64_t tenant_id);
  static int check_standby_tenant_has_no_conflict_tasks_(const uint64_t tenant_id);
  static int check_standby_tenant_is_in_transfer_procedure_(const uint64_t tenant_id);
  static int check_standby_tenant_is_in_modify_ls_procedure_(const uint64_t tenant_id);
  static int check_unit_infos_(common::sqlclient::ObMySQLResult &res, const uint64_t tenant_id);
  static int check_snapshot_table_exists_(const uint64_t user_tenant_id, bool &tenant_snapshot_table_exist);
private:
  static const char* TENANT_SNAP_OP_PRINT_ARRAY[];
private:
  DISALLOW_COPY_AND_ASSIGN(ObTenantSnapshotUtil);
};

}
}


#endif /* __OB_RS_TENANT_SNAPSHOT_UTIL_H__ */
