/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "sql/engine/cmd/ob_empty_query_executor.h"

namespace oceanbase
{
using namespace common;
namespace sql
{
int ObEmptyQueryExecutor::execute(ObExecContext &ctx, ObEmptyQueryStmt &stmt)
{
  UNUSED(ctx);
  UNUSED(stmt);
  int ret = OB_SUCCESS;
  return ret;
}

}// sql
}// oceanbase
