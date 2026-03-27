/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SRC_SQL_ENGINE_CMD_OB_MLOG_EXECUTOR_H_
#define OCEANBASE_SRC_SQL_ENGINE_CMD_OB_MLOG_EXECUTOR_H_

#include "share/ob_define.h"

namespace oceanbase
{
namespace sql
{
struct ObCreateMLogStmt;
class ObDropMLogStmt;
struct ObExecContext;

class ObCreateMLogExecutor
{
public:
  ObCreateMLogExecutor();
  virtual ~ObCreateMLogExecutor();
  int execute(ObExecContext &ctx, ObCreateMLogStmt &stmt);
};

class ObDropMLogExecutor
{
public:
  ObDropMLogExecutor();
  virtual ~ObDropMLogExecutor();

  int execute(ObExecContext &ctx, ObDropMLogStmt &stmt);
};
} // namespace sql
} // namespace oceanbase
#endif