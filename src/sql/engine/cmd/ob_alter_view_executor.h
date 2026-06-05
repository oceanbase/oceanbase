/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_CMD_OB_ALTER_VIEW_EXECUTOR_H_
#define OCEANBASE_SQL_ENGINE_CMD_OB_ALTER_VIEW_EXECUTOR_H_

#include "share/ob_define.h"

namespace oceanbase
{
namespace sql
{
class ObExecContext;
class ObAlterViewStmt;

class ObAlterViewExecutor
{
public:
  ObAlterViewExecutor();
  virtual ~ObAlterViewExecutor();
  int execute(ObExecContext &ctx, ObAlterViewStmt &stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObAlterViewExecutor);
};

} // namespace sql
} // namespace oceanbase
#endif // OCEANBASE_SQL_ENGINE_CMD_OB_ALTER_VIEW_EXECUTOR_H_
