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
