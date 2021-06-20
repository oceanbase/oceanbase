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

#ifndef OCEANBASE_SQL_ENGINE_CMD_OB_ALTER_SYSTEM_EXECUTOR_
#define OCEANBASE_SQL_ENGINE_CMD_OB_ALTER_SYSTEM_EXECUTOR_

#include "share/ob_define.h"
#include "sql/resolver/cmd/ob_alter_system_stmt.h"
#include "sql/resolver/cmd/ob_clear_balance_task_stmt.h"

namespace oceanbase {
namespace sql {
class ObExecContext;
class ObAdminServerStmt;
class ObAdminZoneStmt;
class ObBootstrapStmt;

#define DEF_SIMPLE_EXECUTOR(name)                      \
  class name##Executor {                               \
  public:                                              \
    name##Executor()                                   \
    {}                                                 \
    virtual ~name##Executor()                          \
    {}                                                 \
    int execute(ObExecContext& ctx, name##Stmt& stmt); \
                                                       \
  private:                                             \
    DISALLOW_COPY_AND_ASSIGN(name##Executor);          \
  }

DEF_SIMPLE_EXECUTOR(ObAdminServer);

DEF_SIMPLE_EXECUTOR(ObAdminZone);

DEF_SIMPLE_EXECUTOR(ObFreeze);

DEF_SIMPLE_EXECUTOR(ObFlushCache);

DEF_SIMPLE_EXECUTOR(ObFlushKVCache);

DEF_SIMPLE_EXECUTOR(ObFlushIlogCache);

DEF_SIMPLE_EXECUTOR(ObFlushDagWarnings);

DEF_SIMPLE_EXECUTOR(ObLoadBaseline);

DEF_SIMPLE_EXECUTOR(ObSwitchReplicaRole);

DEF_SIMPLE_EXECUTOR(ObSwitchRSRole);

DEF_SIMPLE_EXECUTOR(ObDropReplica);

DEF_SIMPLE_EXECUTOR(ObChangeReplica);

DEF_SIMPLE_EXECUTOR(ObMigrateReplica);

DEF_SIMPLE_EXECUTOR(ObReportReplica);

DEF_SIMPLE_EXECUTOR(ObRecycleReplica);

DEF_SIMPLE_EXECUTOR(ObAdminMerge);

DEF_SIMPLE_EXECUTOR(ObClearRoottable);

DEF_SIMPLE_EXECUTOR(ObRefreshSchema);

DEF_SIMPLE_EXECUTOR(ObRefreshMemStat);

DEF_SIMPLE_EXECUTOR(ObSetConfig);

DEF_SIMPLE_EXECUTOR(ObClearLocationCache);

DEF_SIMPLE_EXECUTOR(ObReloadGts);

DEF_SIMPLE_EXECUTOR(ObReloadUnit);

DEF_SIMPLE_EXECUTOR(ObReloadServer);

DEF_SIMPLE_EXECUTOR(ObReloadZone);

DEF_SIMPLE_EXECUTOR(ObClearMergeError);

DEF_SIMPLE_EXECUTOR(ObMigrateUnit);

DEF_SIMPLE_EXECUTOR(ObUpgradeVirtualSchema);

DEF_SIMPLE_EXECUTOR(ObAdminUpgradeCmd);

DEF_SIMPLE_EXECUTOR(ObAdminRollingUpgradeCmd);

DEF_SIMPLE_EXECUTOR(ObRunJob);
DEF_SIMPLE_EXECUTOR(ObRunUpgradeJob);
DEF_SIMPLE_EXECUTOR(ObStopUpgradeJob);

DEF_SIMPLE_EXECUTOR(ObBootstrap);

DEF_SIMPLE_EXECUTOR(ObSetTP);

DEF_SIMPLE_EXECUTOR(ObRefreshTimeZoneInfo);

DEF_SIMPLE_EXECUTOR(ObEnableSqlThrottle);

DEF_SIMPLE_EXECUTOR(ObDisableSqlThrottle);

DEF_SIMPLE_EXECUTOR(ObSetDiskValid);
DEF_SIMPLE_EXECUTOR(ObClearBalanceTask);
DEF_SIMPLE_EXECUTOR(ObAddDisk);
DEF_SIMPLE_EXECUTOR(ObDropDisk);

DEF_SIMPLE_EXECUTOR(ObArchiveLog);
DEF_SIMPLE_EXECUTOR(ObBackupDatabase);
DEF_SIMPLE_EXECUTOR(ObBackupManage);
DEF_SIMPLE_EXECUTOR(ObBackupSetEncryption);
DEF_SIMPLE_EXECUTOR(ObBackupSetDecryption);

class ObCancelTaskExecutor {
public:
  ObCancelTaskExecutor()
  {}
  virtual ~ObCancelTaskExecutor()
  {}
  int execute(ObExecContext& ctx, ObCancelTaskStmt& stmt);

private:
  int parse_task_id(const common::ObString& task_id_str, share::ObTaskId& task_id);
  int fetch_sys_task_info(ObExecContext& ctx, const common::ObString& task_id, common::ObAddr& task_server);

private:
  DISALLOW_COPY_AND_ASSIGN(ObCancelTaskExecutor);
};

class ObChangeTenantExecutor {
public:
  ObChangeTenantExecutor()
  {}
  virtual ~ObChangeTenantExecutor()
  {}
  int execute(ObExecContext& ctx, ObChangeTenantStmt& stmt);

private:
private:
  DISALLOW_COPY_AND_ASSIGN(ObChangeTenantExecutor);
};

#undef DEF_SIMPLE_EXECUTOR

}  // end namespace sql
}  // end namespace oceanbase
#endif  // OCEANBASE_SQL_ENGINE_CMD_OB_ALTER_SYSTEM_EXECUTOR_
