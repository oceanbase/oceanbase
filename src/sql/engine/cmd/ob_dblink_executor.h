/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SRC_SQL_ENGINE_CMD_OB_DBLINK_EXECUTOR_H_
#define OCEANBASE_SRC_SQL_ENGINE_CMD_OB_DBLINK_EXECUTOR_H_
#include "share/ob_define.h"

namespace oceanbase
{
namespace sql
{
class ObExecContext;
class ObCreateDbLinkStmt;
class ObDropDbLinkStmt;

class ObCreateDbLinkExecutor
{
public:
  ObCreateDbLinkExecutor() {}
  virtual ~ObCreateDbLinkExecutor() {}
  int execute(ObExecContext &ctx, ObCreateDbLinkStmt &stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObCreateDbLinkExecutor);
};

class ObDropDbLinkExecutor
{
public:
  ObDropDbLinkExecutor() {}
  virtual ~ObDropDbLinkExecutor() {}
  int execute(ObExecContext &ctx, ObDropDbLinkStmt &stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObDropDbLinkExecutor);
};

}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SRC_SQL_ENGINE_CMD_OB_DBLINK_EXECUTOR_H_ */
