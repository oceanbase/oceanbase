/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SRC_SQL_ENGINE_CMD_OB_UDT_EXECUTOR_H_
#define OCEANBASE_SRC_SQL_ENGINE_CMD_OB_UDT_EXECUTOR_H_

#include "lib/container/ob_vector.h"
#include "sql/parser/parse_node.h"
#include "sql/resolver/ob_stmt_type.h"
#include "src/share/schema/ob_udt_info.h"

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

namespace oceanbase
{
namespace sql
{
class ObExecContext;
class ObCreateUDTStmt;
class ObDropUDTStmt;
class ObAlterUDTStmt;

class ObCreateUDTExecutor
{
public:
  ObCreateUDTExecutor() {}
  virtual ~ObCreateUDTExecutor() {}
  int execute(ObExecContext &ctx, ObCreateUDTStmt &stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObCreateUDTExecutor);
};

//参考alter system定义
DEF_SIMPLE_EXECUTOR(ObDropUDT);

class ObAlterUDTExecutor
{
public:
  ObAlterUDTExecutor() {}
  virtual ~ObAlterUDTExecutor() {}
  int execute(ObExecContext &ctx, ObAlterUDTStmt &stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObAlterUDTExecutor);
};

}//namespace sql
}//namespace oceanbase



#endif /* OCEANBASE_SRC_SQL_ENGINE_CMD_OB_UDT_EXECUTOR_H_ */
