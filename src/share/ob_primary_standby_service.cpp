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

#define USING_LOG_PREFIX STANDBY

#include "ob_primary_standby_service.h"              // ObPrimaryStandbyService
#include "lib/oblog/ob_log_module.h"              // LOG_*
#include "lib/utility/ob_print_utils.h"             // TO_STRING_KV
#include "rootserver/ob_cluster_event.h"          // CLUSTER_EVENT_ADD_CONTROL
#include "rootserver/ob_rs_event_history_table_operator.h" // ROOTSERVICE_EVENT_ADD
#include "rootserver/ob_tenant_role_transition_service.h" // ObTenantRoleTransitionService
#include "share/restore/ob_log_archive_source_mgr.h"  // ObLogArchiveSourceMgr

namespace oceanbase
{
using namespace oceanbase;
using namespace common;
using namespace obrpc;
using namespace share;
using namespace rootserver;

namespace standby
{

int ObPrimaryStandbyService::init(
           ObMySQLProxy *sql_proxy,
           share::schema::ObMultiVersionSchemaService *schema_service)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sql_proxy)
      || OB_ISNULL(schema_service)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(sql_proxy), KP(schema_service));
  } else {
    sql_proxy_ = sql_proxy;
    schema_service_ = schema_service;
    inited_ = true;
  }
  return ret;
}

void ObPrimaryStandbyService::destroy()
{
  if (OB_UNLIKELY(!inited_)) {
    LOG_INFO("ObPrimaryStandbyService has been destroyed", K_(inited));
  } else {
    LOG_INFO("ObPrimaryStandbyService begin to destroy", K_(inited));
    sql_proxy_ = NULL;
    schema_service_ = NULL;
    inited_ = false;
    LOG_INFO("ObPrimaryStandbyService destroyed", K_(inited));
  }
}

int ObPrimaryStandbyService::check_inner_stat_()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(sql_proxy_) || OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Member variables is NULL", KR(ret), KP(sql_proxy_), KP(schema_service_));
  }
  return ret;
}

int ObPrimaryStandbyService::switch_tenant(const obrpc::ObSwitchTenantArg &arg)
{
  int ret = OB_SUCCESS;
  int64_t begin_time = ObTimeUtility::current_time();
  uint64_t switch_tenant_id = OB_INVALID_ID;
  const char *alter_cluster_event = arg.get_alter_type_str();
  CLUSTER_EVENT_ADD_CONTROL_START(ret, alter_cluster_event, "stmt_str", arg.get_stmt_str());
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("inner stat error", KR(ret), K_(inited));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), KR(ret));
  } else if (arg.get_tenant_name().empty()) {
    if (!is_user_tenant(arg.get_exec_tenant_id())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("can't switch tenant without tenant name using SYS/meta tenant session", KR(ret), K(arg));
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "tenant name, should specify tenant name");
    } else {
      switch_tenant_id = arg.get_exec_tenant_id();
    }
  } else {
    // tenant_name not empty
    if (OB_SYS_TENANT_ID != arg.get_exec_tenant_id()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("can't specify tenant name using user tenant session", KR(ret), K(arg), K(switch_tenant_id));
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "tenant name, please don't specify tenant name");
    } else {
      if (OB_ISNULL(schema_service_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid schema service", KR(ret), KP(schema_service_));
      } else {
        share::schema::ObSchemaGetterGuard guard;
        if (OB_FAIL(schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, guard))) {
          LOG_WARN("get_schema_guard failed", KR(ret));
        } else if (OB_FAIL(guard.get_tenant_id(arg.get_tenant_name(), switch_tenant_id))) {
          LOG_WARN("get_tenant_id failed", KR(ret), K(arg));
        } else if (!is_user_tenant(switch_tenant_id)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("only support switch user tenant", KR(ret), K(arg), K(switch_tenant_id));
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "tenant name, only support switch user tenant");
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else {
    switch (arg.get_op_type()) {
      case ObSwitchTenantArg::FAILOVER_TO_PRIMARY :
        if (OB_FAIL(failover_to_primary(switch_tenant_id, arg))) {
          LOG_WARN("failed to failover_to_primary", KR(ret), K(switch_tenant_id), K(arg));
        }
        break;
      default :
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("unkown op_type", K(arg));
    }
  }

  int64_t cost = ObTimeUtility::current_time() - begin_time;
  CLUSTER_EVENT_ADD_CONTROL_FINISH(ret, alter_cluster_event,
      K(cost),
      "stmt_str", arg.get_stmt_str());

  return ret;
}

int ObPrimaryStandbyService::failover_to_primary(const uint64_t tenant_id, const obrpc::ObSwitchTenantArg &arg)
{
  int ret = OB_SUCCESS;
  int64_t begin_time = ObTimeUtility::current_time();
  ObAllTenantInfo tenant_info;
  bool is_restore = false;
  ObSchemaGetterGuard schema_guard;
  const ObSimpleTenantSchema *tenant_schema = nullptr;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("inner stat error", KR(ret), K_(inited));
  } else if (OB_ISNULL(GCTX.srv_rpc_proxy_) || OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pointer is null", KR(ret), KP(GCTX.srv_rpc_proxy_), KP(schema_service_));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(tenant_id, sql_proxy_,
                                                    false, tenant_info))) {
    LOG_WARN("failed to load tenant info", KR(ret), K(tenant_id));
  } else if (tenant_info.is_primary() && tenant_info.is_normal_status()) {
    LOG_INFO("already is primary tenant, no need switch", K(tenant_info));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("failed to get schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_schema))) {
    LOG_WARN("failed to get tenant info", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(tenant_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant_schema is null", KR(ret), K(tenant_id), K(arg));
  } else if (tenant_schema->is_normal()) {
    ObTenantRoleTransitionService role_transition_service(tenant_id, sql_proxy_, GCTX.srv_rpc_proxy_);
    if (OB_FAIL(role_transition_service.failover_to_primary())) {
      LOG_WARN("failed to failover to primary", KR(ret), K(tenant_id), K(arg));
    }
  } else {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("tenant status is not normal, failover is not allowed", KR(ret), K(tenant_id), K(arg), KPC(tenant_schema));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "tenant status is not normal, failover is");
  }

  return ret;
}

}
}