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

#ifndef OCEANBASE_ROOTSERVER_OB_DISASTER_RECOVERY_TASK_UTILS_H_
#define OCEANBASE_ROOTSERVER_OB_DISASTER_RECOVERY_TASK_UTILS_H_


#include "common/ob_member_list.h"                                     // for ObMemberList
#include "ob_disaster_recovery_task.h"                                 // for ObDRTask
#include "share/ob_ls_id.h"                                            // for ObLSID
#include "share/ob_rpc_struct.h"                                       // for ObNotifyTenantThreadArg
#include "lib/container/ob_iarray.h"                                   // ObIArray
#include "lib/mysqlclient/ob_mysql_transaction.h"                      // ObMySQLTransaction

namespace oceanbase
{

namespace rootserver
{

class DisasterRecoveryUtils
{
public:
  const static int64_t INVALID_DR_SERVICE_EPOCH_VALUE = -1;
  const static int64_t MAX_REPORT_RETRY_TIMES = 3;
  const static int64_t REPORT_RETRY_INTERVAL_MS = 100 * 1000; // 100ms
public:
  DisasterRecoveryUtils() {}
  virtual ~DisasterRecoveryUtils() {}

public:
  static int wakeup_local_service(const uint64_t tenant_id);

  static int wakeup_tenant_service(
    const obrpc::ObNotifyTenantThreadArg &arg);

  static int wakeup_tenant_dr_service(
    const uint64_t tenant_id);

  static int get_dr_tasks_count(
    const uint64_t tenant_id,
    int64_t &task_count);

  // after the task is completed, report and clean up the task
  // @params[in] res, task execution results
  static int report_to_meta_tenant(
    const obrpc::ObDRTaskReplyResult &arg);

  // after the task is completed, report and clean up the task
  // @params[in] res, task execution results
  static int report_to_rs(
    const obrpc::ObDRTaskReplyResult &res);

  // check if data_version is match task_history table version
  // @params[in] tenant_data_version, target data_version to check
  static bool is_history_table_data_version_match(
      uint64_t tenant_data_version);

  // get member_list of target ls from leader in meta table
  // @params[in]  tenant_id, ls of which tenant
  // @params[in]  ls_id, target ls_id
  // @params[out] member_list
  static int get_member_list(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      common::ObMemberList &member_list);

  // build task execute_result by ret_code and ret_comment and start_time
  // @params[in]  ret_code, task execute ret_code
  // @params[in]  ret_comment, task execute ret_comment
  // @params[in]  start_time, start_time of task
  // @params[out] execute_result, execute_result
  static int build_execute_result(
      const int ret_code,
      const ObDRTaskRetComment &ret_comment,
      const int64_t start_time,
      ObSqlString &execute_result);

  // get ObReplicaMember of target server from leader in meta_table
  // @params[in]  tenant_id, ls of which tenant
  // @params[in]  ls_id, target ls_id
  // @params[in]  server_addr, target server addr
  // @params[out] member
  static int get_member_by_server(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const common::ObAddr &server_addr,
      common::ObReplicaMember &member);

  // check if the tenant row in service epoch table exists.
  // if not, insert a row with an initial value of 0.
  // @params[in]  tenant_id, target tenant id
  static int check_service_epoch_exist_or_insert(
      common::ObMySQLTransaction &trans,
      const uint64_t tenant_id);

  // lock row in __all_service_epoch by trans select for update
  // @params[in]  trans, trans to use
  // @params[in]  tenant_id, which tenant'row to lock
  static int lock_service_epoch(
      common::ObMySQLTransaction &trans,
      const uint64_t tenant_id,
      const int64_t service_epoch);

  // while task execute finish
  // record task history in task history table and clean task in task table, and record rs event
  // @params[in]  task, target task to update
  // @params[in]  ret_code, ret code of task
  // @params[out] ret_comment, ret_comment of task
  static int record_history_and_clean_task(
      ObDRTask &task,
      const int ret_code,
      const ObDRTaskRetComment &ret_comment);

  // send rpc to cancel migrate task
  // @params[in]  task, target task to cancel
  static int send_rpc_to_cancel_task(
      const ObDRTask &task);

  // while task execute finish in observer
  // record task history in task history table and clean task in task table, and record rs event
  // @params[in]  task_id, task_id of target task
  // @params[in]  tenant_id, tenant_id of target task
  // @params[in]  ls_id, ls_id of target task
  // @params[in]  ret_code, ret_code of target task
  static int clean_task_while_task_finish(
      const share::ObTaskId &task_id,
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const int ret_code);

  // check whether the tenant was enabled parallel migrate
  // @params[in]  tenant_id, tenant to check
  // @params[out] enable_parallel_migration
  static int check_tenant_enable_parallel_migration(
      const uint64_t &tenant_id,
      bool &enable_parallel_migration);
private:

  // get ObReplicaMember from leader member list in meta table
  // @params[in]  ls_info, ls_info of target ls
  // @params[in]  server_addr, target server
  // @params[out] replica_member, target replica_member
  static int get_member_in_member_list_(
      const share::ObLSInfo &ls_info,
      const common::ObAddr &server_addr,
      common::ObReplicaMember &replica_member);

  // get ObReplicaMember from leader learner list in meta table
  // @params[in]  ls_info, ls_info of target ls
  // @params[in]  server_addr, target server
  // @params[out] replica_member, target replica_member
  static int get_member_in_learner_list_(
      const share::ObLSInfo &ls_info,
      const common::ObAddr &server_addr,
      common::ObReplicaMember &replica_member);

};

} // end namespace rootserver
} // end namespace oceanbase
#endif // OCEANBASE_ROOTSERVER_OB_DISASTER_RECOVERY_TASK_UTILS_H_
