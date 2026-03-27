/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef __OB_SQL_CONTEXT_EXECUTOR_H__
#define __OB_SQL_CONTEXT_EXECUTOR_H__
#include "share/ob_define.h"
namespace oceanbase
{
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
class ObCreateContextStmt;
class ObDropContextStmt;

DEF_SIMPLE_EXECUTOR(ObCreateContext);
DEF_SIMPLE_EXECUTOR(ObDropContext);

#undef DEF_SIMPLE_EXECUTOR
}
}
#endif /* __OB_SQL_SEQUENCE_EXECUTOR_H__ */
//// end of header file
