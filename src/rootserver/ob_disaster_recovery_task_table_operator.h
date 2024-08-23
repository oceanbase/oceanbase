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
#ifndef OCEANBASE_ROOTSERVER_OB_DISASTER_RECOVERY_TASK_TABLE_OPERATOR_H_
#define OCEANBASE_ROOTSERVER_OB_DISASTER_RECOVERY_TASK_TABLE_OPERATOR_H_
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/ob_define.h"             // for ObTaskId ObReplicaType ..
#include "lib/string/ob_string.h"      // for ObString
#include "ob_disaster_recovery_task.h" // for ObDRTaskPriority
#include "ob_disaster_recovery_task_table_updater.h" // for ObDRTaskTableUpdateTask
#include "share/ob_ls_id.h"            // for ObLSID

namespace oceanbase
{
namespace rootserver
{

class ObLSReplicaTaskTableOperator
{
  OB_UNIS_VERSION(1);
public:
  ObLSReplicaTaskTableOperator() {}
  virtual ~ObLSReplicaTaskTableOperator() {}

  // through tenant_id, ls_id, task_type and task_id, delete task from __all_ls_replica_task
  // @params[in]  trans, trans proxy to use
  // @params[in]  tenant_id, which tenant's task to delete
  // @params[in]  ls_id, which log stream's task to delete
  // @params[in]  task_type, type of target task
  // @params[in]  task_id, task_id of target task
  // @params[out] affected_rows, number of rows to delete tasks
  int delete_task(
      common::ObMySQLTransaction &trans,
      const uint64_t tenant_id,
      const share::ObLSID& ls_id,
      const ObDRTaskType& task_type,
      const share::ObTaskId& task_id,
      int64_t &affected_rows);

  // insert task into __all_ls_replica_task
  // @params[in] trans, trans proxy to use
  // @params[in] task, target task to insert
  int insert_task(
      common::ObISQLClient &sql_proxy,
      const ObDRTask &task);

  // read task info and delete task from __all_ls_replica_task, insert task into __all_ls_replica_task_history
  // @params[in]  sql_proxy, proxy to use
  // @params[in]  task, contains task info which to finish
  int finish_task(
      common::ObMySQLTransaction& trans,
      const ObDRTaskTableUpdateTask &task);

  // read task info for cancel replica task
  // @params[in]   tenant_id, tenant task to get
  // @params[in]   task_id, task_id to get
  // @params[out]  task_execute_server, which server the task execute
  // @params[out]  ls_id, which ls this task belong to
  int get_task_info_for_cancel(
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const share::ObTaskId &task_id,
      common::ObAddr &task_execute_server,
      share::ObLSID &ls_id);

private:

  int get_task_schedule_time_(
      common::ObMySQLTransaction& trans,
      const ObDRTaskTableUpdateTask &task,
      int64_t &schedule_time);

private:
  DISALLOW_COPY_AND_ASSIGN(ObLSReplicaTaskTableOperator);
};

} // end namespace rootserver
} // end namespace oceanbase
#endif // OCEANBASE_ROOTSERVER_OB_DISASTER_RECOVERY_TASK_TABLE_OPERATOR_H_
