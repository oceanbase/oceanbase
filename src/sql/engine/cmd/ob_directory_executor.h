/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SRC_SQL_ENGINE_CMD_OB_DIRECTORY_EXECUTOR_H_
#define OCEANBASE_SRC_SQL_ENGINE_CMD_OB_DIRECTORY_EXECUTOR_H_
#include "share/ob_define.h"

namespace oceanbase
{
namespace sql
{
class ObExecContext;
class ObCreateDirectoryStmt;
class ObAlterDirectoryStmt;
class ObDropDirectoryStmt;

class ObCreateDirectoryExecutor
{
public:
  ObCreateDirectoryExecutor() {}
  virtual ~ObCreateDirectoryExecutor() {}
  int execute(ObExecContext &ctx, ObCreateDirectoryStmt &stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObCreateDirectoryExecutor);
};

class ObDropDirectoryExecutor
{
public:
  ObDropDirectoryExecutor() {}
  virtual ~ObDropDirectoryExecutor() {}
  int execute(ObExecContext &ctx, ObDropDirectoryStmt &stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObDropDirectoryExecutor);
};
} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SRC_SQL_ENGINE_CMD_OB_DIRECTORY_EXECUTOR_H_
