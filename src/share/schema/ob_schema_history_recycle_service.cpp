/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX SHARE_SCHEMA

#include "share/schema/ob_schema_history_recycle_service.h"
#include "share/schema/ob_schema_history_recycler.h"
#include "observer/ob_server_struct.h"
#include "lib/utility/ob_defer.h"
#include "share/ob_share_util.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
using namespace common;

ObSchemaHistoryRecycleService::ObSchemaHistoryRecycleService()
  : inited_(false),
    mutex_(common::ObLatchIds::OB_SCHEMA_HISTORY_RECYCLE_SERVICE_LOCK),
    last_completed_version_(OB_INVALID_VERSION)
{
}

int ObSchemaHistoryRecycleService::mtl_init(ObSchemaHistoryRecycleService *&svc)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(svc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("svc is null", KR(ret));
  } else if (OB_FAIL(svc->init())) {
    LOG_WARN("fail to init schema history recycle service", KR(ret), K(MTL_ID()));
  }
  return ret;
}

void ObSchemaHistoryRecycleService::destroy()
{
  inited_ = false;
  last_completed_version_ = OB_INVALID_VERSION;
}

int ObSchemaHistoryRecycleService::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), K(MTL_ID()));
  } else {
    last_completed_version_ = OB_INVALID_VERSION;
    inited_ = true;
  }
  return ret;
}

int ObSchemaHistoryRecycleService::check_tenant_valid_(bool &valid_tenant) const
{
  int ret = OB_SUCCESS;
  valid_tenant = true;
  const uint64_t tenant_id = MTL_ID();

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(is_sys_tenant(tenant_id))) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("sys tenant not allowed to recycle schema history", KR(ret), K(tenant_id));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "SYS tenant. Schema history recycling");
  } else if (OB_UNLIKELY(!is_user_tenant(tenant_id))) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("non-user tenant not allowed to recycle schema history", KR(ret), K(tenant_id));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "Non-user tenant. Schema history recycling");
  } else {
    bool is_primary = false;
    if (OB_FAIL(ObShareUtil::mtl_check_if_tenant_role_is_primary(tenant_id, is_primary))) {
      LOG_WARN("fail to check tenant role is primary", KR(ret), K(tenant_id));
    } else if (!is_primary) {
      valid_tenant = false;
    } else {
      ObSchemaGetterGuard schema_guard;
      const ObSimpleTenantSchema *tenant_schema = NULL;
      if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
        LOG_WARN("fail to get tenant schema guard", KR(ret), K(tenant_id));
      } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_schema))) {
        LOG_WARN("fail to get tenant info", KR(ret), K(tenant_id));
      } else if (OB_ISNULL(tenant_schema)) {
        ret = OB_TENANT_NOT_EXIST;
        LOG_WARN("tenant not exist", KR(ret), K(tenant_id));
      } else if (!tenant_schema->is_normal()) {
        valid_tenant = false;
        LOG_INFO("tenant is not in normal status", K(tenant_id), KPC(tenant_schema));
      }
    }
  }
  return ret;
}

int ObSchemaHistoryRecycleService::run_once()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  FLOG_INFO("[SCHEMA_RECYCLE] run once recycle schema history start", KR(ret), K(tenant_id));
  bool valid_tenant = true;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(GCTX.schema_service_) || OB_ISNULL(GCTX.sql_proxy_) || OB_ISNULL(GCTX.srv_rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("global ctx ptr is null", KR(ret));
  } else if (OB_FAIL(check_tenant_valid_(valid_tenant))) {
    LOG_WARN("fail to check tenant valid", KR(ret), K(tenant_id));
  } else if (!valid_tenant) {
    LOG_INFO("[SCHEMA_RECYCLE] skip tenant by role/status", K(tenant_id));
  } else if (OB_FAIL(mutex_.trylock())) {
    LOG_WARN("run_once trylock failed", KR(ret), K(tenant_id));
  } else {
    ObSchemaHistoryRecycler recycler;
    if (OB_FAIL(recycler.init(*GCTX.schema_service_, *GCTX.sql_proxy_, *GCTX.srv_rpc_proxy_, tenant_id))) {
      LOG_WARN("fail to init schema history recycler", KR(ret), K(tenant_id));
    } else {
      int64_t recycle_schema_version = OB_INVALID_VERSION;

      if (OB_FAIL(recycler.calc_recycle_schema_version(recycle_schema_version, last_completed_version_))) {
        LOG_WARN("fail to calc recycle schema version", KR(ret), K(tenant_id));
      } else if (OB_UNLIKELY(OB_INVALID_VERSION == recycle_schema_version)) {
        LOG_INFO("[SCHEMA_RECYCLE] recycle_schema_version invalid, skip this round", K(tenant_id), K(recycle_schema_version));
      } else if (OB_INVALID_VERSION != last_completed_version_
                 && last_completed_version_ >= recycle_schema_version) {
        LOG_INFO("[SCHEMA_RECYCLE] skip since recycle schema version not increased",
                K(tenant_id), K(recycle_schema_version), K_(last_completed_version));
      } else if (OB_FAIL(recycler.execute(recycle_schema_version))) {
        LOG_WARN("fail to run schema history recycle", KR(ret), K(tenant_id), K(recycle_schema_version));
      } else if (recycler.is_recycle_completed()) {
        last_completed_version_ = recycle_schema_version;
      }
    }

    mutex_.unlock();
  }

  FLOG_INFO("[SCHEMA_RECYCLE] run once recycle schema history end", KR(ret), K(tenant_id));
  return ret;
}

} // namespace schema
} // namespace share
} // namespace oceanbase
