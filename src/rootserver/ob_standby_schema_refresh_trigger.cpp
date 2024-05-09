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

#define USING_LOG_PREFIX RS
#include "ob_standby_schema_refresh_trigger.h"
#include "observer/ob_server_struct.h" // GCTX
#include "observer/ob_service.h" // ObService
#include "share/rc/ob_tenant_base.h" // MTL
#include "share/ob_schema_status_proxy.h" // ObSchemaStatusProxy
#include "rootserver/ob_tenant_info_loader.h" // ObTenantInfoLoader

#define STAT(level, fmt, args...) RS_LOG(level, "[STANDBY_SCHEMA_REFRESH_TRIGGER] " fmt, ##args)
#define ISTAT(fmt, args...) STAT(INFO, fmt, ##args)
#define WSTAT(fmt, args...) STAT(WARN, fmt, ##args)
#define TSTAT(fmt, args...) STAT(TRACE, fmt, ##args)

namespace oceanbase
{
using namespace share;
using namespace common;
namespace rootserver
{
int ObStandbySchemaRefreshTrigger::init()
{
  int ret = OB_SUCCESS;
  sql_proxy_ = GCTX.sql_proxy_;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    WSTAT("init twice", KR(ret));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    WSTAT("sql proxy is null", KR(ret));
  } else if (OB_FAIL(ObTenantThreadHelper::create("StandbySchem",
        lib::TGDefIDs::SimpleLSService, *this))) {
    WSTAT("failed to create STANDBY_SCHEMA_REFRESH_TRIGGER", KR(ret));
  } else if (OB_FAIL(ObTenantThreadHelper::start())) {
    WSTAT("failed to start STANDBY_SCHEMA_REFRESH_TRIGGER", KR(ret));
  } else {
    tenant_id_ = MTL_ID();
    is_inited_ = true;
  }

  return ret;
}

void ObStandbySchemaRefreshTrigger::do_work()
{
  ISTAT("standby schema refresh trigger start");
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    WSTAT("inner stat error", KR(ret), K_(is_inited));
  } else {
    while (!has_set_stop()) {
      ObCurTraceId::init(GCONF.self_addr_);
      if (!is_user_tenant(tenant_id_)) {
      } else if (OB_FAIL(submit_tenant_refresh_schema_task_())) {
        WSTAT("submit_tenant_refresh_schema_task_ failed", KR(ret));
      }

      ISTAT("finish one round", KR(ret));
      idle(DEFAULT_IDLE_TIME);
    }
  }
}

void ObStandbySchemaRefreshTrigger::destroy()
{
  ISTAT("standby schema refresh trigger destory", KPC(this));
  ObTenantThreadHelper::destroy();
  tenant_id_ = OB_INVALID_TENANT_ID;
  sql_proxy_ = NULL;
  is_inited_ = false;
}

int ObStandbySchemaRefreshTrigger::check_inner_stat_()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    WSTAT("not init", KR(ret));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    WSTAT("Member variables is NULL", KR(ret), KP(sql_proxy_));
  }
  return ret;
}

int ObStandbySchemaRefreshTrigger::submit_tenant_refresh_schema_task_()
{
  int ret = OB_SUCCESS;
  rootserver::ObTenantInfoLoader *tenant_info_loader = MTL(rootserver::ObTenantInfoLoader*);
  bool is_standby_normal_status = false;
  DEBUG_SYNC(BLOCK_STANDBY_REFRESH_SCHEMA);

  if (OB_FAIL(check_inner_stat_())) {
    WSTAT("error unexpected", KR(ret), K(tenant_id_), KP(sql_proxy_));
  } else if (!is_user_tenant(tenant_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("only run for user tenant", KR(ret), K_(tenant_id));
  } else if (OB_ISNULL(GCTX.ob_service_) || OB_ISNULL(GCTX.schema_service_) || OB_ISNULL(sql_proxy_) || OB_ISNULL(tenant_info_loader)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("pointer is null", KR(ret), KP(GCTX.ob_service_), KP(GCTX.schema_service_), KP(sql_proxy_), KP(tenant_info_loader));
  } else if (OB_FAIL(tenant_info_loader->check_is_standby_normal_status(is_standby_normal_status))) {
    LOG_WARN("fail to get tenant status", KR(ret), K_(tenant_id));
  } else if (is_standby_normal_status) {
    ObRefreshSchemaStatus schema_status;
    ObSchemaStatusProxy *schema_status_proxy = GCTX.schema_status_proxy_;
    if (OB_ISNULL(schema_status_proxy)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema_status_proxy is null", KR(ret));
    } else if (OB_FAIL(schema_status_proxy->get_refresh_schema_status(tenant_id_, schema_status))) {
      LOG_WARN("fail to get schema status", KR(ret), K(tenant_id_));
    } else if (common::OB_INVALID_TIMESTAMP != schema_status.snapshot_timestamp_) {
      //for fix bugfic:56142375
      //schema_status in memory maybe not valid, try to load from table
      if (OB_FAIL(schema_status_proxy->load_refresh_schema_status(tenant_id_, schema_status))) {
        LOG_WARN("failed to load refresh schema status", KR(ret), K(tenant_id_));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (common::OB_INVALID_TIMESTAMP == schema_status.snapshot_timestamp_) {
      int64_t version_in_inner_table = OB_INVALID_VERSION;
      int64_t local_schema_version = OB_INVALID_VERSION;
      if (OB_FAIL(GCTX.schema_service_->get_tenant_refreshed_schema_version(
                        tenant_id_, local_schema_version))) {
        LOG_WARN("fail to get tenant refreshed schema version", KR(ret), K_(tenant_id));
      } else if (OB_FAIL(GCTX.schema_service_->get_schema_version_in_inner_table(
                  *sql_proxy_, schema_status, version_in_inner_table))) {
        LOG_WARN("fail to get_schema_version_in_inner_table", KR(ret), K(schema_status));
      } else if (local_schema_version > version_in_inner_table) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("local_schema_version > version_in_inner_table", KR(ret), K_(tenant_id),
                  K(local_schema_version), K(version_in_inner_table));
      } else if (local_schema_version == version_in_inner_table) {
        // do nothing
      } else if (OB_FAIL(GCTX.ob_service_->submit_async_refresh_schema_task(tenant_id_, version_in_inner_table))) {
        LOG_WARN("failed to submit_async_refresh_schema_task", KR(ret), K_(tenant_id));
      }
    } else if (REACH_TENANT_TIME_INTERVAL(1 * 1000 * 1000)) {
      LOG_INFO("standby tenant can not refresh schema", K(schema_status));
    }
  }
  return ret;
}

}
}

#undef STAT
#undef ISTAT
#undef WSTAT
#undef TSTAT
