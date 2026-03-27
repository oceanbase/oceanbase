/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_TABLEGROUP_EXECUTOR_
#define OCEANBASE_SQL_OB_TABLEGROUP_EXECUTOR_
#include "share/ob_define.h"
#include "common/object/ob_object.h"
#include "lib/container/ob_se_array.h"
#include "share/ob_rpc_struct.h"
namespace oceanbase
{
namespace share
{
namespace schema
{
class ObPartition;
class ObSubPartition;
}
}
namespace sql
{
class ObRawExpr;

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
class ObCreateTablegroupStmt;
DEF_SIMPLE_EXECUTOR(ObCreateTablegroup);

class ObDropTablegroupStmt;
DEF_SIMPLE_EXECUTOR(ObDropTablegroup);

class ObAlterTablegroupStmt;
class ObAlterTablegroupExecutor
{
public:
  ObAlterTablegroupExecutor() {}
  virtual ~ObAlterTablegroupExecutor() {}
  int execute(ObExecContext &ctx, ObAlterTablegroupStmt &stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObAlterTablegroupExecutor);
};

}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SQL_OB_TABLEGROUP_EXECUTOR_ */
