/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SERVER_OMT
#include "ob_tenant_ai_service.h"
#include "share/ai_service/ob_ai_service_executor.h"
#include "sql/privilege_check/ob_ai_model_priv_util.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"

using namespace oceanbase::share;
using namespace oceanbase::common;

namespace oceanbase
{
namespace omt
{

ObTenantAiService::ObTenantAiService()
  : is_inited_(false)
{
}

ObTenantAiService::~ObTenantAiService()
{
  destroy();
}

int ObTenantAiService::mtl_init(ObTenantAiService* &tenant_ai_service)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(tenant_ai_service)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant ai service is null", KR(ret));
  } else if (OB_FAIL(tenant_ai_service->init())) {
    LOG_WARN("Failed to init ObTenantAiService", KR(ret));
  }
  return ret;
}

int ObTenantAiService::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTenantAiService already initialized", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObTenantAiService::destroy()
{
  is_inited_ = false;
}

int ObTenantAiService::get_ai_service_guard(ObAiServiceGuard &ai_service_guard)
{
  // TODO: implement after cache support
  int ret = OB_SUCCESS;
  return ret;
}

ObAiServiceGuard::ObAiServiceGuard()
  : local_allocator_(ObMemAttr(MTL_ID(), "AiServiceGuard", ObCtxIds::DEFAULT_CTX_ID))
{
}

ObAiServiceGuard::~ObAiServiceGuard()
{
}

int ObAiServiceGuard::check_access_privilege()
{
  int ret = OB_SUCCESS;
  bool has_priv = false;

  ObSQLSessionInfo *session = nullptr;
  if (OB_ISNULL(session = THIS_WORKER.get_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else {
    ObArenaAllocator tmp_allocator;
    share::schema::ObSchemaGetterGuard *schema_guard = session->get_cur_exec_ctx()->get_sql_ctx()->schema_guard_;
    if (OB_ISNULL(schema_guard)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema guard is null", K(ret));
    } else {
      sql::ObAIServiceEndpointPrivUtil priv_util(*schema_guard);
      share::schema::ObSessionPrivInfo session_priv;
      if (OB_FAIL(schema_guard->get_session_priv_info(session->get_priv_tenant_id(),
                                                    session->get_priv_user_id(),
                                                    session->get_database_name(),
                                                    session_priv))) {
        LOG_WARN("failed to get session priv info", K(ret));
      } else if (OB_FAIL(priv_util.check_access_ai_model_priv(tmp_allocator, session_priv, has_priv))) {
        LOG_WARN("failed to check access ai model privilege", K(ret));
      } else if (!has_priv) {
        ret = OB_ERR_NO_PRIVILEGE;
        LOG_WARN("no privilege for ai model access", K(ret));
        LOG_USER_ERROR(OB_ERR_NO_PRIVILEGE, "access ai model");
      }
    }
  }

  return ret;
}

int ObAiServiceGuard::get_ai_endpoint(const common::ObString &name, share::ObAiModelEndpointInfo *&endpoint_info)
{
  int ret = OB_SUCCESS;
  ObAiModelEndpointInfo *tmp_endpoint_info = nullptr;

  if (OB_FAIL(check_access_privilege())) {
    LOG_WARN("failed to check access privilege", K(ret));
  } else if (OB_ISNULL(tmp_endpoint_info = OB_NEWx(ObAiModelEndpointInfo, &local_allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc endpoint info", KR(ret));
  } else if (OB_FAIL(ObAiServiceExecutor::read_ai_endpoint(local_allocator_, name, *tmp_endpoint_info))) {
    LOG_WARN("failed to select ai endpoint", KR(ret), K(name));
  } else {
    endpoint_info = tmp_endpoint_info;
  }
  return ret;
}

int ObAiServiceGuard::get_ai_endpoint_by_ai_model_name(const common::ObString &ai_model_name, share::ObAiModelEndpointInfo *&endpoint_info)
{
  int ret = OB_SUCCESS;
  ObAiModelEndpointInfo *tmp_endpoint_info = nullptr;

  if (OB_FAIL(check_access_privilege())) {
    LOG_WARN("failed to check access privilege", K(ret));
  } else if (OB_ISNULL(tmp_endpoint_info = OB_NEWx(ObAiModelEndpointInfo, &local_allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc endpoint info", KR(ret));
  } else if (OB_FAIL(ObAiServiceExecutor::read_ai_endpoint_by_ai_model_name(local_allocator_, ai_model_name, *tmp_endpoint_info))) {
    LOG_WARN("failed to select ai endpoint by ai model name", KR(ret), K(ai_model_name));
  } else {
    endpoint_info = tmp_endpoint_info;
  }
  return ret;
}

} // namespace omt
} // namespace oceanbase
