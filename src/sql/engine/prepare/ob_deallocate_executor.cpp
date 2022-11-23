/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "ob_deallocate_executor.h"
#include "sql/resolver/prepare/ob_deallocate_stmt.h"
#include "sql/engine/ob_exec_context.h"
#include "observer/ob_server_struct.h"

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


