/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SRC_SQL_ENGINE_CMD_OB_LOCATION_EXECUTOR_H_
#define OCEANBASE_SRC_SQL_ENGINE_CMD_OB_LOCATION_EXECUTOR_H_
#include "share/ob_define.h"

namespace oceanbase
{
namespace sql
{
class ObExecContext;
class ObCreateLocationStmt;
class ObDropLocationStmt;

class ObCreateLocationExecutor
{
public:
  ObCreateLocationExecutor() {}
  virtual ~ObCreateLocationExecutor() {}
  int execute(ObExecContext &ctx, ObCreateLocationStmt &stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObCreateLocationExecutor);
};

class ObDropLocationExecutor
{
public:
  ObDropLocationExecutor() {}
  virtual ~ObDropLocationExecutor() {}
  int execute(ObExecContext &ctx, ObDropLocationStmt &stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObDropLocationExecutor);
};
} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SRC_SQL_ENGINE_CMD_OB_LOCATION_EXECUTOR_H_
