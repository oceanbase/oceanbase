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
namespace oceanbase {
namespace sql {
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

DEF_SIMPLE_EXECUTOR(ObCreateTenant);

DEF_SIMPLE_EXECUTOR(ObDropTenant);

DEF_SIMPLE_EXECUTOR(ObModifyTenant);

DEF_SIMPLE_EXECUTOR(ObLockTenant);

DEF_SIMPLE_EXECUTOR(ObFlashBackTenant);

DEF_SIMPLE_EXECUTOR(ObPurgeTenant);

DEF_SIMPLE_EXECUTOR(ObPurgeRecycleBin);

DEF_SIMPLE_EXECUTOR(ObCreateRestorePoint);

DEF_SIMPLE_EXECUTOR(ObDropRestorePoint);

#undef DEF_SIMPLE_EXECUTOR
}  // namespace sql
}  // namespace oceanbase
#endif /* __OB_SQL_TENANT_EXECUTOR_H__ */
//// end of header file
