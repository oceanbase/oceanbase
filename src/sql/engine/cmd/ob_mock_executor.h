/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_MOCK_EXECUTOR_H_
#define OCEANBASE_SQL_ENGINE_MOCK_EXECUTOR_H_

#include "share/ob_define.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/resolver/cmd/ob_mock_stmt.h"

namespace oceanbase
{
namespace sql
{
class ObMockExecutor
{
public:
  ObMockExecutor() = default;
  virtual ~ObMockExecutor() = default;
  int execute(ObExecContext &exec_ctx, ObMockStmt &stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObMockExecutor);
};

}
}

#endif
