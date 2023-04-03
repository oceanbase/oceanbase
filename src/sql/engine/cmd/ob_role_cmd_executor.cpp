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
#include "sql/engine/cmd/ob_role_cmd_executor.h"

#include "lib/encrypt/ob_encrypted_helper.h"
#include "lib/string/ob_sql_string.h"
#include "share/schema/ob_schema_struct.h"
#include "share/ob_rpc_struct.h"
#include "share/ob_common_rpc_proxy.h"
#include "sql/resolver/dcl/ob_create_role_stmt.h"
#include "sql/resolver/dcl/ob_drop_role_stmt.h"
#include "sql/resolver/dcl/ob_alter_role_stmt.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/cmd/ob_user_cmd_executor.h"

namespace oceanbase
{
using namespace common;
using namespace obrpc;
using namespace share::schema;
namespace sql
{

int ObCreateRoleExecutor::execute(ObExecContext &ctx, ObCreateRoleStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
  ObSQLSessionInfo *mysession = NULL;
  const uint64_t tenant_id = stmt.get_tenant_id();
  const ObString &role_name = stmt.get_role_name();
  const ObString &pwd = stmt.get_password();
  if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_ISNULL(common_rpc_proxy = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get common rpc proxy failed");
  } else if (OB_ISNULL(mysession = ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get mysession", K(ret));
  } else {
    ObCreateUserArg arg;
    arg.tenant_id_ = tenant_id;
    arg.exec_tenant_id_ = tenant_id;
    arg.creator_id_ = mysession->get_user_id();
    ObUserInfo user_info;
    user_info.set_tenant_id(tenant_id);
    user_info.set_type(OB_ROLE);
    user_info.set_host(OB_DEFAULT_HOST_NAME);
    ObSArray<int64_t> failed_index;
    ObString pwd_enc;
    if (pwd.length() > 0 && stmt.get_need_enc()) {
      char enc_buf[ENC_BUF_LEN] = {0};
      // 采用OB统一的加密方式
      if (OB_FAIL(ObCreateUserExecutor::encrypt_passwd(pwd, pwd_enc, enc_buf, ENC_BUF_LEN))) {
        LOG_WARN("Encrypt password failed", K(ret));
      } else if (OB_FAIL(user_info.set_passwd(pwd_enc))) {
        LOG_WARN("set password failed", K(ret));
      }
    } else if (FALSE_IT(pwd_enc = pwd)) {
    } else if (OB_FAIL(user_info.set_passwd(pwd_enc))) {
      LOG_WARN("Failed to set password", K(ret));
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(user_info.set_user_name(role_name))) {
      LOG_WARN("set user name failed", K(ret));
    } else if (OB_FAIL(arg.user_infos_.push_back(user_info))) {
      LOG_WARN("Add user info to array error", K(ret));
    } else if (OB_FAIL(common_rpc_proxy->create_user(arg, failed_index))) {
      LOG_WARN("Create user error", K(ret));
    }
  }
  return ret;
}

int ObDropRoleExecutor::execute(ObExecContext &ctx, ObDropRoleStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
  const uint64_t tenant_id = stmt.get_tenant_id();
  if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant is invalid", K(ret));
  } else if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed", K(ret));
  } else if (NULL == (common_rpc_proxy = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get common rpc proxy failed", K(ret));
  } else {
    ObDropUserArg &arg = static_cast<ObDropUserArg &>(stmt.get_ddl_arg());
    arg.tenant_id_ = tenant_id;
    arg.exec_tenant_id_ = tenant_id;
    arg.is_role_ = true;
    const ObString &role_name = stmt.get_role_name();
    ObString host_name(OB_DEFAULT_HOST_NAME);
    ObSArray<int64_t> failed_index;
    if (OB_FAIL(arg.users_.push_back(role_name))) {
      LOG_WARN("Add user name failed", K(ret));
    } else if (OB_FAIL(arg.hosts_.push_back(host_name))) {
      LOG_WARN("Add host name failed", K(ret));
    } else if (OB_FAIL(common_rpc_proxy->drop_user(arg, failed_index)) || (0 != failed_index.count())) {
      LOG_WARN("drop role failed", K(ret), K(role_name));
      //ret = OB_CANNOT_USER;
      //LOG_USER_ERROR(ret, strlen("DROP ROLE"), "DROP ROLE", role_name.length(), role_name.ptr());
    }
  }

  return ret;
}

int ObAlterRoleExecutor::execute(ObExecContext &ctx, ObAlterRoleStmt &stmt)
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
  } else {
    ObAlterRoleArg &arg = static_cast<ObAlterRoleArg &>(stmt.get_ddl_arg());
    char enc_buf[ENC_BUF_LEN] = {0};
    arg.tenant_id_ = stmt.get_tenant_id();
    arg.role_name_ = stmt.get_role_name();
    arg.host_name_ = ObString(OB_DEFAULT_HOST_NAME);
    const ObString &pwd = stmt.get_password();
    ObString pwd_enc;
    if (pwd.length() > 0 && stmt.get_need_enc()) {
      // 采用OB统一的加密方式
      if (OB_FAIL(ObCreateUserExecutor::encrypt_passwd(pwd, pwd_enc, enc_buf, ENC_BUF_LEN))) {
        LOG_WARN("Encrypt password failed", K(ret));
      }
    } else {
      pwd_enc = pwd;
    }
    arg.pwd_enc_ = pwd_enc;

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(common_rpc_proxy->alter_role(arg))) {
      LOG_WARN("Alter user error", K(ret));
    }
  }
  return ret;
}

}// ns sql
}// ns oceanbase
