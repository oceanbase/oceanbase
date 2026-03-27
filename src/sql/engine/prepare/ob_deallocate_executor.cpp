/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "ob_deallocate_executor.h"
#include "sql/resolver/prepare/ob_deallocate_stmt.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

int ObDeallocateExecutor::execute(ObExecContext &ctx, ObDeallocateStmt &stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx.get_sql_ctx()) || OB_ISNULL(ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql ctx or session is NULL", K(ctx.get_sql_ctx()), K(ctx.get_my_session()), K(ret));
  } else {
    if (OB_FAIL(ctx.get_my_session()->remove_prepare(stmt.get_prepare_name()))) {
      LOG_WARN("failed to remove prepare", K(stmt.get_prepare_name()), K(ret));
    } else if (OB_FAIL(ctx.get_my_session()->close_ps_stmt(stmt.get_prepare_id()))) {
      LOG_WARN("fail to deallocate ps stmt", K(ret), K(stmt.get_prepare_id()));
    } else { /*do nothing*/ }
  }
  return ret;
}

}
}


