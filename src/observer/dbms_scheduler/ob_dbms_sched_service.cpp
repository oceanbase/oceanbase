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

#include "observer/dbms_scheduler/ob_dbms_sched_service.h"
#define USING_LOG_PREFIX SERVER

namespace oceanbase
{
using namespace common;
using namespace oceanbase::share;
namespace rootserver
{

int ObDBMSSchedService::mtl_init(ObDBMSSchedService *&dbms_sched_service)
{
  return dbms_sched_service->init();
}

int ObDBMSSchedService::init()
{
  int ret = OB_SUCCESS;
  tenant_id_ = MTL_ID();
  if (!is_user_tenant(tenant_id_) && !is_sys_tenant(tenant_id_) && !is_meta_tenant(tenant_id_)) {
    // do nothing
  } else if (job_master_.is_inited()) {
    ret = OB_INIT_TWICE;
    LOG_WARN("has inited", KR(ret), "tenant_id", tenant_id_);
  } else if (OB_FAIL(job_master_.init(GCTX.sql_proxy_, GCTX.schema_service_, tenant_id_))) {
    LOG_WARN("[DBMS_SCHED_SERVICE] job master init failed", "tenant_id", tenant_id_);
  } else if (OB_FAIL(ObTenantThreadHelper::create(
      "DBMSSched",
      lib::TGDefIDs::DBMSSchedService,
      *this))) {
    LOG_WARN("[DBMS_SCHED_SERVICE] fail to create thread", KR(ret));
  } else {
    LOG_INFO("[DBMS_SCHED_SERVICE] ObDBMSSchedService init success", "tenant_id", tenant_id_);
  }
  return ret;
}

int ObDBMSSchedService::start()
{
  int ret = OB_SUCCESS;
  if (!is_user_tenant(tenant_id_) && !is_sys_tenant(tenant_id_) && !is_meta_tenant(tenant_id_)) {
    // do nothing
  } else if (!job_master_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(job_master_.is_inited()));
  } else if (OB_FAIL(job_master_.start())) {
    LOG_WARN("[DBMS_SCHED_SERVICE] job master start failed", K(ret), "tenant_id", tenant_id_);
  } else if (OB_FAIL(ObTenantThreadHelper::start())) {
    LOG_WARN("[DBMS_SCHED_SERVICE] failed to start thread", KR(ret));
  } else {
    LOG_INFO("[DBMS_SCHED_SERVICE] ObDBMSSchedService start success", "tenant_id", tenant_id_);
  }
  return ret;
}

void ObDBMSSchedService::do_work()
{
  int ret = OB_SUCCESS;
  if (!is_user_tenant(tenant_id_) && !is_sys_tenant(tenant_id_) && !is_meta_tenant(tenant_id_)) {
    // do nothing
  } else if (!job_master_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(job_master_.is_inited()));
  } else if (OB_FAIL(job_master_.scheduler())) {
    LOG_WARN("[DBMS_SCHED_SERVICE] job master sched failed", K(ret), "tenant_id", tenant_id_);
  }
}

void ObDBMSSchedService::stop()
{
  int ret = OB_SUCCESS;
  if (!is_user_tenant(tenant_id_) && !is_sys_tenant(tenant_id_) && !is_meta_tenant(tenant_id_)) {
    // do nothing
  } else if (!job_master_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(job_master_.is_inited()));
  } else if (OB_FAIL(job_master_.stop())) {
    LOG_INFO("[DBMS_SCHED_SERVICE] ObDBMSSchedService stop failure", "tenant_id", tenant_id_);
  } else {
    ObTenantThreadHelper::stop();
    LOG_INFO("[DBMS_SCHED_SERVICE] ObDBMSSchedService stop success", "tenant_id", tenant_id_);
  }
}

void ObDBMSSchedService::wait()
{
  int ret = OB_SUCCESS;
  if (!is_user_tenant(tenant_id_) && !is_sys_tenant(tenant_id_) && !is_meta_tenant(tenant_id_)) {
    // do nothing
  } else if (!job_master_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(job_master_.is_inited()));
  } else {
    ObTenantThreadHelper::wait();
    LOG_INFO("[DBMS_SCHED_SERVICE] ObDBMSSchedService wait success", "tenant_id", tenant_id_);
  }
}

void ObDBMSSchedService::destroy()
{
  int ret = OB_SUCCESS;
  if (!is_user_tenant(tenant_id_) && !is_sys_tenant(tenant_id_) && !is_meta_tenant(tenant_id_)) {
    // do nothing
  } else if (job_master_.is_inited()) {
    if (OB_FAIL(job_master_.destroy())) {
      LOG_WARN("[DBMS_SCHED_SERVICE] job master destroy failed", K(ret), "tenant_id", tenant_id_);
    } else {
      LOG_INFO("[DBMS_SCHED_SERVICE] job master destroy success", "tenant_id", tenant_id_);
    }
    ObTenantThreadHelper::destroy();
  }
  LOG_INFO("[DBMS_SCHED_SERVICE] ObDBMSSchedService destroy success", "tenant_id", tenant_id_);
}

void ObDBMSSchedService::switch_to_follower_forcedly()
{
  switch_to_follower_gracefully();
}
int ObDBMSSchedService::switch_to_leader()
{
  int ret = OB_SUCCESS;
  if (!is_user_tenant(tenant_id_) && !is_sys_tenant(tenant_id_) && !is_meta_tenant(tenant_id_)) {
    // do nothing
  } else if (job_master_.is_inited()) {
    job_master_.switch_to_leader();
    ObTenantThreadHelper::switch_to_leader();
    LOG_INFO("[DBMS_SCHED_SERVICE] ObDBMSSchedService switch leader", "tenant_id", tenant_id_);
  }
  return ret;
}
int ObDBMSSchedService::switch_to_follower_gracefully()
{
  int ret = OB_SUCCESS;
  if (!is_user_tenant(tenant_id_) && !is_sys_tenant(tenant_id_) && !is_meta_tenant(tenant_id_)) {
    // do nothing
  } else if (job_master_.is_inited()) {
    job_master_.switch_to_follower();
    ObTenantThreadHelper::switch_to_follower_gracefully();
    LOG_INFO("[DBMS_SCHED_SERVICE] ObDBMSSchedService switch follower", "tenant_id", tenant_id_);
  }
  return ret;
}
int ObDBMSSchedService::resume_leader()
{
  int ret = OB_SUCCESS;
  if (!is_leader()) {
    if (OB_FAIL(switch_to_leader())) {
       LOG_INFO("[DBMS_SCHED_SERVICE] resume leader failed", "tenant_id", tenant_id_);
    }
  }
  return ret;
}

}  // namespace rootserver
}  // namespace oceanbase
