/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCRANBASE_SQL_ENGINE_CMD_OB_TCL_CMD_EXECUTOR_
#define OCRANBASE_SQL_ENGINE_CMD_OB_TCL_CMD_EXECUTOR_

#include "share/ob_define.h"

namespace oceanbase
{
namespace sql
{
class ObExecContext;
class ObEndTransStmt;
class ObEndTransExecutor
{
public:
  ObEndTransExecutor() {}
  virtual ~ObEndTransExecutor() {}
  int execute(ObExecContext &ctx, ObEndTransStmt &stmt);
private:
  int end_trans(ObExecContext &ctx, ObEndTransStmt &stmt);
  DISALLOW_COPY_AND_ASSIGN(ObEndTransExecutor);
};

class ObStartTransStmt;
class ObStartTransExecutor
{
public:
  ObStartTransExecutor() {}
  virtual ~ObStartTransExecutor() {}
  int execute(ObExecContext &ctx, ObStartTransStmt &stmt);
private:
  int start_trans(ObExecContext &ctx, ObStartTransStmt &stmt);
  DISALLOW_COPY_AND_ASSIGN(ObStartTransExecutor);
};

class ObCreateSavePointStmt;
class ObCreateSavePointExecutor
{
public:
  ObCreateSavePointExecutor() {}
  virtual ~ObCreateSavePointExecutor() {}
  int execute(ObExecContext &ctx, ObCreateSavePointStmt &stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObCreateSavePointExecutor);
};

class ObRollbackSavePointStmt;
class ObRollbackSavePointExecutor
{
public:
  ObRollbackSavePointExecutor() {}
  virtual ~ObRollbackSavePointExecutor() {}
  int execute(ObExecContext &ctx, ObRollbackSavePointStmt &stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObRollbackSavePointExecutor);
};

class ObReleaseSavePointStmt;
class ObReleaseSavePointExecutor
{
public:
  ObReleaseSavePointExecutor() {}
  virtual ~ObReleaseSavePointExecutor() {}
  int execute(ObExecContext &ctx, ObReleaseSavePointStmt &stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObReleaseSavePointExecutor);
};

}
}
#endif // OCRANBASE_SQL_ENGINE_CMD_OB_TCL_CMD_EXECUTOR_
