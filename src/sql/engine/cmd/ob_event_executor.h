/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_CMD_EVENT_CMD_EXECUTOR_
#define OCEANBASE_SQL_ENGINE_CMD_EVENT_CMD_EXECUTOR_
#include "lib/string/ob_string.h"
#include "lib/container/ob_array_serialization.h"
#include "share/schema/ob_schema_struct.h"

namespace oceanbase
{
namespace obrpc
{
class ObCommonRpcProxy;
}
namespace sql
{
class ObExecContext;
class ObCreateEventStmt;
class ObDropEventStmt;
class ObAlterEventStmt;

class ObCreateEventExecutor
{
public:
  ObCreateEventExecutor() {}
  virtual ~ObCreateEventExecutor() {}
  int execute(ObExecContext &ctx, ObCreateEventStmt &stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObCreateEventExecutor);
};


class ObAlterEventExecutor
{
public:
  ObAlterEventExecutor() {}
  virtual ~ObAlterEventExecutor() {}
  int execute(ObExecContext &ctx, ObAlterEventStmt &stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObAlterEventExecutor);
};


class ObDropEventExecutor
{
public:
  ObDropEventExecutor() {}
  virtual ~ObDropEventExecutor() {}
  int execute(ObExecContext &ctx, ObDropEventStmt &stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObDropEventExecutor);
};
}
}
#endif //OCEANBASE_SQL_ENGINE_CMD_EVENT_CMD_EXECUTOR_
