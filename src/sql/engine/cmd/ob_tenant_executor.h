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

#ifndef __OB_SQL_TENANT_EXECUTOR_H__
#define __OB_SQL_TENANT_EXECUTOR_H__
#include "share/ob_define.h"
namespace oceanbase
{
namespace common
{
  class ObMySQLProxy;
}
namespace sql
{
#define DEF_SIMPLE_EXECUTOR(name)                          \
  class name##Executor                                     \
  {                                                        \
  public:                                                  \
    name##Executor() {}                                    \
    virtual ~name##Executor() {}                           \
    int execute(ObExecContext &ctx, name##Stmt &stmt);     \
  private:                                                 \
    DISALLOW_COPY_AND_ASSIGN(name##Executor);              \
  }

class ObExecContext;
class ObCreateTenantStmt;
class ObDropTenantStmt;
class ObLockTenantStmt;
class ObModifyTenantStmt;
class ObChangeTenantStmt;
class ObFlashBackTenantStmt;
class ObPurgeTenantStmt;
class ObPurgeRecycleBinStmt;
class ObCreateRestorePointStmt;
class ObDropRestorePointStmt;

class ObCreateTenantExecutor
{
public:
  ObCreateTenantExecutor() {}
  virtual ~ObCreateTenantExecutor() {}
  int execute(ObExecContext &ctx, ObCreateTenantStmt &stmt);
private:
  int wait_schema_refreshed_(const uint64_t tenant_id);
  int wait_user_ls_valid_(const uint64_t tenant_id);
  DISALLOW_COPY_AND_ASSIGN(ObCreateTenantExecutor);
};

class ObCreateStandbyTenantExecutor
{
public:
  ObCreateStandbyTenantExecutor() {}
  virtual ~ObCreateStandbyTenantExecutor() {}
  int execute(ObExecContext &ctx, ObCreateTenantStmt &stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObCreateStandbyTenantExecutor);
};

class ObDropTenantExecutor
{
public:
  ObDropTenantExecutor() {}
  virtual ~ObDropTenantExecutor() {}
  int execute(ObExecContext &ctx, ObDropTenantStmt &stmt);
private:
  int check_tenant_has_been_dropped_(
      ObExecContext &ctx,
      ObDropTenantStmt &stmt,
      const uint64_t tenant_id);
  DISALLOW_COPY_AND_ASSIGN(ObDropTenantExecutor);
};

DEF_SIMPLE_EXECUTOR(ObModifyTenant);

DEF_SIMPLE_EXECUTOR(ObLockTenant);

DEF_SIMPLE_EXECUTOR(ObFlashBackTenant);

DEF_SIMPLE_EXECUTOR(ObPurgeTenant);

DEF_SIMPLE_EXECUTOR(ObPurgeRecycleBin);

DEF_SIMPLE_EXECUTOR(ObCreateRestorePoint);

DEF_SIMPLE_EXECUTOR(ObDropRestorePoint);

#undef DEF_SIMPLE_EXECUTOR
}
}
#endif /* __OB_SQL_TENANT_EXECUTOR_H__ */
//// end of header file
