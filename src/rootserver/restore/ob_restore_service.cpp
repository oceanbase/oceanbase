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

#include "ob_restore_service.h"
#include "ob_recover_table_job_scheduler.h"
#include "share/restore/ob_import_table_struct.h"
#include "share/restore/ob_recover_table_persist_helper.h"

using namespace oceanbase;
using namespace rootserver;
using namespace share;


ObRestoreService::ObRestoreService()
  : inited_(false),
    schema_service_(NULL),
    sql_proxy_(NULL),
    rpc_proxy_(NULL),
    srv_rpc_proxy_(NULL),
    self_addr_(),
    tenant_id_(OB_INVALID_TENANT_ID),
    idle_time_us_(1),
    wakeup_cnt_(0),
    restore_scheduler_(),
    recover_table_scheduler_()

{
}

ObRestoreService::~ObRestoreService()
{
  if (!has_set_stop()) {
    stop();
    wait();
  }
}

void ObRestoreService::destroy()
{
  ObTenantThreadHelper::destroy();
  inited_ = false;
}

int ObRestoreService::init()
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_ISNULL(GCTX.schema_service_) || OB_ISNULL(GCTX.sql_proxy_)
      || OB_ISNULL(GCTX.rs_rpc_proxy_) || OB_ISNULL(GCTX.srv_rpc_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(GCTX.schema_service_), KP(GCTX.sql_proxy_),
        KP(GCTX.rs_rpc_proxy_), KP(GCTX.srv_rpc_proxy_), KP(GCTX.lst_operator_));
  } else if (OB_FAIL(ObTenantThreadHelper::create("REST_SER", lib::TGDefIDs::SimpleLSService, *this))) {
    LOG_WARN("failed to create thread", KR(ret));
  } else if (OB_FAIL(ObTenantThreadHelper::start())) {
    LOG_WARN("fail to start thread", KR(ret));
  } else if (OB_FAIL(restore_scheduler_.init(*this))) {
    LOG_WARN("failed to init restore scheduler", K(ret));
  } else if (OB_FAIL(recover_table_scheduler_.init(
      *GCTX.schema_service_, *GCTX.sql_proxy_, *GCTX.rs_rpc_proxy_, *GCTX.srv_rpc_proxy_))) {
    LOG_WARN("failed to init recover table scheduler", K(ret));
  } else if (OB_FAIL(import_table_scheduler_.init(*GCTX.schema_service_, *GCTX.sql_proxy_))) {
    LOG_WARN("failed to init import table scheduler", K(ret));
  } else {
    schema_service_ = GCTX.schema_service_;
    sql_proxy_ = GCTX.sql_proxy_;
    rpc_proxy_ = GCTX.rs_rpc_proxy_;
    srv_rpc_proxy_ = GCTX.srv_rpc_proxy_;
    tenant_id_ = is_sys_tenant(MTL_ID()) ? MTL_ID() : gen_user_tenant_id(MTL_ID());
    self_addr_ = GCTX.self_addr();
    inited_ = true;
  }
  return ret;
}

int ObRestoreService::idle()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!has_set_stop() && 0 < ATOMIC_LOAD(&wakeup_cnt_)) {
    ATOMIC_SET(&wakeup_cnt_, 0); // wake up immediately
  } else {
    ObTenantThreadHelper::idle(idle_time_us_);
    idle_time_us_ = GCONF._restore_idle_time;
  }
  return ret;
}

int ObRestoreService::check_stop() const
{
  int ret = OB_SUCCESS;
  if (has_set_stop()) {
    ret = OB_CANCELED;
    LOG_WARN("restore service stopped", K(ret));
  }
  return ret;
}

void ObRestoreService::do_work()
{
  LOG_INFO("[RESTORE] restore service start");
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    ObRSThreadFlag rs_work;
    // avoid using default idle time when observer restarts.
    idle_time_us_ = GCONF._restore_idle_time;
    const uint64_t tenant_id = MTL_ID();
    while (!has_set_stop()) {
      {
        ObCurTraceId::init(GCTX.self_addr());
        ObArray<ObPhysicalRestoreJob> job_infos;
        share::schema::ObSchemaGetterGuard schema_guard;
        const share::schema::ObTenantSchema *tenant_schema = NULL;
        if (OB_ISNULL(GCTX.schema_service_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected error", KR(ret));
        } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
                OB_SYS_TENANT_ID, schema_guard))) {
          LOG_WARN("fail to get schema guard", KR(ret));
        } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_schema))) {
          LOG_WARN("failed to get tenant ids", KR(ret), K(tenant_id));
        } else if (OB_ISNULL(tenant_schema)) {
          ret = OB_TENANT_NOT_EXIST;
          LOG_WARN("tenant not exist", KR(ret), K(tenant_id));
        } else if (!tenant_schema->is_normal()) {
          //tenant not normal, maybe meta or sys tenant
          //while meta tenant not ready, cannot process tenant restore job
        } else {
          restore_scheduler_.do_work();
          recover_table_scheduler_.do_work();
          import_table_scheduler_.do_work();
          idle_time_us_ = 10;
        }
      }//for schema guard, must be free
      // retry until stopped, reset ret to OB_SUCCESS
      ret = OB_SUCCESS;
      idle();
    }
  }
  LOG_INFO("[RESTORE] restore service quit");
  return;
}