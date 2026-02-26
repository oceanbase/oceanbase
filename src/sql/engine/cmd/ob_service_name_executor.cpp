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

#include "sql/engine/cmd/ob_service_name_executor.h"

#include "sql/engine/ob_exec_context.h"
#include "share/ob_common_rpc_proxy.h"
#include "rootserver/ob_service_name_command.h"


namespace oceanbase {
using namespace rootserver;

namespace sql {
int ObServiceNameExecutor::execute(ObExecContext& ctx, ObServiceNameStmt& stmt)
{
  int ret = OB_SUCCESS;
  const ObServiceNameArg &arg = stmt.get_arg();
  const ObServiceNameString &service_name_str = arg.get_service_name_str();
  const ObServiceNameArg::ObServiceOp &service_op = arg.get_service_op();
  const uint64_t tenant_id = arg.get_target_tenant_id();
  ObSQLSessionInfo *session_info = ctx.get_my_session();
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(arg));
  } else if (OB_FAIL(ObServiceNameProxy::check_is_service_name_enabled(tenant_id))) {
    LOG_WARN("fail to execute check_is_service_name_enabled", KR(ret), K(tenant_id));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "The tenant's or meta tenant's data_version is smaller than 4_2_4_0, service name related command is");
  } else if (OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session_info is null", KR(ret), KP(session_info));
  } else if (OB_UNLIKELY(!session_info->get_service_name().is_empty())) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("service_name related commands cannot be executed in the session which is created via service_name",
        KR(ret), K(session_info->get_service_name()));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "This session is created via service_name, service name related command is");
  } else if (arg.is_create_service()) {
    if (OB_FAIL(ObServiceNameCommand::create_service(tenant_id, service_name_str))) {
      LOG_WARN("fail to create service", KR(ret), K(tenant_id), K(service_name_str));
    }
  } else if (arg.is_delete_service()) {
    if (OB_FAIL(ObServiceNameCommand::delete_service(tenant_id, service_name_str))) {
      LOG_WARN("fail to delete service", KR(ret), K(tenant_id), K(service_name_str));
    }
  } else if (arg.is_start_service()) {
    if (OB_FAIL(ObServiceNameCommand::start_service(tenant_id, service_name_str))) {
      LOG_WARN("fail to start service", KR(ret), K(tenant_id), K(service_name_str));
    }
  } else if (arg.is_stop_service()) {
    if (OB_FAIL(ObServiceNameCommand::stop_service(tenant_id, service_name_str))) {
      LOG_WARN("fail to stop service", KR(ret), K(tenant_id), K(service_name_str));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unknown service operation", KR(ret), K(arg));
  }
  return ret;
}
}
} // oceanbase