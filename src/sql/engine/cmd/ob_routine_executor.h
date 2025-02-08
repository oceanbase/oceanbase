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

#ifndef OCEANBASE_SRC_SQL_ENGINE_CMD_OB_PROCEDURE_EXECUTOR_H_
#define OCEANBASE_SRC_SQL_ENGINE_CMD_OB_PROCEDURE_EXECUTOR_H_

#include "lib/container/ob_vector.h"
#include "lib/container/ob_iarray.h"
#include "sql/parser/parse_node.h"
#include "sql/resolver/ob_stmt_type.h"
#include "src/share/schema/ob_routine_info.h"

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
namespace common
{
class ObField;
class ObObjParam;
}
namespace share
{
namespace schema
{
class ObRoutineInfo;
class ObRoutineParam;
}
}
namespace sql
{
class ObExecContext;
class ObCreateRoutineStmt;
class ObAlterRoutineStmt;
class ObDropRoutineStmt;
class ObCallProcedureStmt;
class ObAnonymousBlockStmt;
class ObRawExpr;

class ObCompileRoutineInf
{
public:
  ObCompileRoutineInf() {}
  virtual ~ObCompileRoutineInf() {}
  static int compile_routine(ObExecContext &ctx,
                      uint64_t tenant_id,
                      uint64_t database_id,
                      ObString &routine_name,
                      share::schema::ObRoutineType routine_type,
                      int64_t schema_version);
private:
  DISALLOW_COPY_AND_ASSIGN(ObCompileRoutineInf);
};

class ObCreateRoutineExecutor : ObCompileRoutineInf
{
public:
  ObCreateRoutineExecutor() {}
  virtual ~ObCreateRoutineExecutor() {}
  int execute(ObExecContext &ctx, ObCreateRoutineStmt &stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObCreateRoutineExecutor);
};

class ObAlterRoutineExecutor : ObCompileRoutineInf
{
public:
  ObAlterRoutineExecutor() {}
  virtual ~ObAlterRoutineExecutor() {}
  int execute(ObExecContext &ctx, ObAlterRoutineStmt &stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObAlterRoutineExecutor);
};

//参考alter system定义
DEF_SIMPLE_EXECUTOR(ObDropRoutine);

class ObCallProcedureExecutor
{
public:
  ObCallProcedureExecutor() {}
  virtual ~ObCallProcedureExecutor() {}
  int execute(ObExecContext &ctx, ObCallProcedureStmt &stmt);

private:
  DISALLOW_COPY_AND_ASSIGN(ObCallProcedureExecutor);
};
  
class ObAnonymousBlockExecutor
{
public:
  ObAnonymousBlockExecutor() {}
  virtual ~ObAnonymousBlockExecutor() {}

  int execute(ObExecContext &ctx, ObAnonymousBlockStmt &stmt);
  int fill_field_with_udt_id(
    ObExecContext &ctx, uint64_t udt_id, common::ObField &field);

private:
  DISALLOW_COPY_AND_ASSIGN(ObAnonymousBlockExecutor);
};


}//namespace sql
}//namespace oceanbase



#endif /* OCEANBASE_SRC_SQL_ENGINE_CMD_OB_PROCEDURE_EXECUTOR_H_ */
