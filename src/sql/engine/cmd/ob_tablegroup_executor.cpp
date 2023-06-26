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
#include "share/object/ob_obj_cast.h"
#include "sql/engine/cmd/ob_tablegroup_executor.h"

#include "share/ob_common_rpc_proxy.h"
#include "share/schema/ob_schema_struct.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/resolver/ddl/ob_create_tablegroup_stmt.h"
#include "sql/resolver/ddl/ob_alter_tablegroup_stmt.h"
#include "sql/resolver/ddl/ob_drop_tablegroup_stmt.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/code_generator/ob_expr_generator_impl.h"
#include "sql/engine/cmd/ob_partition_executor_utils.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;
namespace sql
{
int ObCreateTablegroupExecutor::execute(ObExecContext &ctx, ObCreateTablegroupStmt &stmt)
{
  int ret = OB_SUCCESS;
  obrpc::ObRpcOpts rpc_opt;
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
  obrpc::ObCreateTablegroupArg &create_tablegroup_arg = stmt.get_create_tablegroup_arg();

  ObTablegroupSchema &tablegroup_schema = create_tablegroup_arg.tablegroup_schema_;
  tablegroup_schema.set_part_func_expr_num(stmt.get_part_func_expr_num());
  tablegroup_schema.set_sub_part_func_expr_num(stmt.get_sub_part_func_expr_num());

  ObString first_stmt;
  if (OB_FAIL(stmt.get_first_stmt(first_stmt))) {
    LOG_WARN("fail to get first stmt" , K(ret));
  } else {
    const_cast<obrpc::ObCreateTablegroupArg&>(create_tablegroup_arg).ddl_stmt_str_ = first_stmt;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_FAIL(ObPartitionExecutorUtils::calc_values_exprs(ctx, stmt))) {
    LOG_WARN("compare range parition expr fail", K(ret));
  } else if (OB_FAIL(task_exec_ctx->get_common_rpc(common_rpc_proxy))) {
    LOG_WARN("get common rpc proxy failed", K(ret));
  } else if (OB_ISNULL(common_rpc_proxy)){
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("common rpc proxy should not be null", K(ret));
  } else {
    obrpc::UInt64 tablegroup_id(0);
    if (OB_FAIL(common_rpc_proxy->create_tablegroup(create_tablegroup_arg, tablegroup_id))) {
      LOG_WARN("rpc proxy create tablegroup failed", K(ret));
    }
  }
  LOG_INFO("finish execute create tablegroup.", K(stmt), K(ret));
  return ret;
}

int ObDropTablegroupExecutor::execute(ObExecContext &ctx, ObDropTablegroupStmt &stmt)
{
  int ret = OB_SUCCESS;
  obrpc::ObRpcOpts rpc_opt;
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
  const obrpc::ObDropTablegroupArg &drop_tablegroup_arg = stmt.get_drop_tablegroup_arg();
  ObString first_stmt;
  if (OB_FAIL(stmt.get_first_stmt(first_stmt))) {
    LOG_WARN("fail to get first stmt" , K(ret));
  } else {
    const_cast<obrpc::ObDropTablegroupArg&>(drop_tablegroup_arg).ddl_stmt_str_ = first_stmt;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_FAIL(task_exec_ctx->get_common_rpc(common_rpc_proxy))) {
    LOG_WARN("get common rpc proxy failed", K(ret));
  } else if (OB_ISNULL(common_rpc_proxy)){
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("common rpc proxy should not be null", K(ret));
  } else if (OB_FAIL(common_rpc_proxy->drop_tablegroup(drop_tablegroup_arg))) {
    LOG_WARN("rpc proxy drop tablegroup failed", K(ret));
  }
  LOG_INFO("finish execute drop tablegroup.", K(stmt), K(ret));
  return ret;
}

int ObAlterTablegroupExecutor::execute(ObExecContext &ctx, ObAlterTablegroupStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
  obrpc::ObAlterTablegroupArg &alter_tablegroup_arg = stmt.get_alter_tablegroup_arg();
  alter_tablegroup_arg.alter_tablegroup_schema_.set_part_func_expr_num(stmt.get_part_func_expr_num());
  alter_tablegroup_arg.alter_tablegroup_schema_.set_sub_part_func_expr_num(stmt.get_sub_part_func_expr_num());

  ObString first_stmt;
  if (OB_FAIL(stmt.get_first_stmt(first_stmt))) {
    LOG_WARN("fail to get first stmt" , K(ret));
  } else {
    const_cast<obrpc::ObAlterTablegroupArg&>(alter_tablegroup_arg).ddl_stmt_str_ = first_stmt;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_FAIL(task_exec_ctx->get_common_rpc(common_rpc_proxy))) {
    LOG_WARN("get common rpc proxy failed", K(ret));
  } else if (OB_ISNULL(common_rpc_proxy)){
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("common rpc proxy should not be null", K(ret));
  } else if (OB_FAIL(common_rpc_proxy->alter_tablegroup(alter_tablegroup_arg))) {
    LOG_WARN("rpc proxy alter table group failed", "dst", common_rpc_proxy->get_server(), K(ret), K(alter_tablegroup_arg));
  }
  return ret;
}
}  // namespace sql
}  // namespace oceanbase
