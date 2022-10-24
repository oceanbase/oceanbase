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
#include "sql/engine/cmd/ob_profile_cmd_executor.h"

#include "lib/encrypt/ob_encrypted_helper.h"
#include "lib/string/ob_sql_string.h"
#include "share/schema/ob_schema_struct.h"
#include "share/ob_rpc_struct.h"
#include "share/ob_common_rpc_proxy.h"
#include "sql/resolver/ddl/ob_create_profile_stmt.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
using namespace common;
using namespace obrpc;
using namespace share::schema;
namespace sql
{

int ObProfileDDLExecutor::execute(ObExecContext &ctx, ObUserProfileStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;

  if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_ISNULL(common_rpc_proxy = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get common rpc proxy failed");
  } else if (OB_FAIL(common_rpc_proxy->do_profile_ddl(stmt.get_ddl_arg()))) {
    LOG_WARN("Create profile error", K(ret));
  }
  return ret;
}

}// ns sql
}// ns oceanbase
