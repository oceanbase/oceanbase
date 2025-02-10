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
#include "lib/ob_define.h"                          // for ObTaskId ObReplicaType ..
#include "lib/string/ob_string.h"                   // for ObString
#include "ob_disaster_recovery_task.h"              // for ObDRTaskPriority
#include "share/ob_ls_id.h"                         // for ObLSID

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

  // delete task from __all_ls_replica_task
  // @params[in]  sql_proxy, sql_proxy to use
  // @params[in]  task, which tenant's task to delete
  int delete_task(common::ObISQLClient &sql_proxy,
                  const ObDRTask &task);

  // insert task into __all_ls_replica_task or __all_ls_replica_task_history
  // @params[in]  sql_proxy, sql_proxy to use
  // @params[in]  task, target task to insert
  // @params[in]  record_history, whether write history table
  int insert_task(common::ObISQLClient &sql_proxy,
                  const ObDRTask &task,
                  const bool record_history);

  // load task_keys from __all_ls_replica_task
  // @params[in]  sql_proxy, sql_proxy to use
  // @params[in]  sql, sql statement
  // @params[in]  tenant_id, which tenant's task to load
  // @params[out] dr_task_keys, task_keys array
  int load_task_key_from_inner_table(common::ObISQLClient &sql_proxy,
                                     const ObSqlString &sql,
                                     const uint64_t tenant_id,
                                     ObIArray<ObDRTaskKey> &dr_task_keys);

  // load ObDRTask* array from __all_ls_replica_task
  // @params[in]  sql_proxy, sql_proxy to use
  // @params[in]  tenant_id, which tenant's task to load
  // @params[in]  sql, sql statement
  // @params[in]  allocator, memory allocator
  // @params[out] dr_tasks, ObDRTask* array
  int load_task_from_inner_table(common::ObISQLClient &sql_proxy,
                                 const uint64_t tenant_id,
                                 const ObSqlString &sql,
                                 common::ObArenaAllocator &allocator,
                                 ObIArray<ObDRTask *> &dr_tasks);

private:
  int read_task_key_from_result_(sqlclient::ObMySQLResult &res,
                                 ObIArray<ObDRTaskKey> &dr_task_keys);

  int read_task_from_result_(common::ObArenaAllocator &allocator,
                             sqlclient::ObMySQLResult &res,
                             ObIArray<ObDRTask *> &dr_tasks);

private:
  DISALLOW_COPY_AND_ASSIGN(ObLSReplicaTaskTableOperator);
};

} // end namespace rootserver
} // end namespace oceanbase
#endif // OCEANBASE_ROOTSERVER_OB_DISASTER_RECOVERY_TASK_TABLE_OPERATOR_H_
