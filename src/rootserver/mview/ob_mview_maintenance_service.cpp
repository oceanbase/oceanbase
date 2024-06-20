/**
 * Copyright (c) 2023 OceanBase
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

#include "rootserver/mview/ob_mview_maintenance_service.h"
#include "observer/omt/ob_multi_tenant.h"
#include "share/ob_errno.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
namespace rootserver
{
using namespace common;

/**
 * ObMViewMaintenanceService
 */

ObMViewMaintenanceService::ObMViewMaintenanceService() : is_inited_(false) {}

ObMViewMaintenanceService::~ObMViewMaintenanceService() {}

int ObMViewMaintenanceService::mtl_init(ObMViewMaintenanceService *&service)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(service)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(service));
  } else if (OB_FAIL(service->init())) {
    LOG_WARN("fail to init mview maintenance service", KR(ret));
  }
  return ret;
}

int ObMViewMaintenanceService::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObMViewMaintenanceService init twice", KR(ret), KP(this));
  } else {
    if (OB_FAIL(mlog_maintenance_task_.init())) {
      LOG_WARN("fail to init mlog maintenance task", KR(ret));
    } else if (OB_FAIL(mview_maintenance_task_.init())) {
      LOG_WARN("fail to init mview maintenance task", KR(ret));
    } else if (OB_FAIL(mvref_stats_maintenance_task_.init())) {
      LOG_WARN("fail to init mvref stats maintenance task", KR(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObMViewMaintenanceService::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMViewMaintenanceService not init", KR(ret), KP(this));
  } else {
    // do nothing
  }
  return ret;
}

void ObMViewMaintenanceService::stop()
{
  mlog_maintenance_task_.stop();
  mview_maintenance_task_.stop();
  mvref_stats_maintenance_task_.stop();
}

void ObMViewMaintenanceService::wait()
{
  mlog_maintenance_task_.wait();
  mview_maintenance_task_.wait();
  mvref_stats_maintenance_task_.wait();
}

void ObMViewMaintenanceService::destroy()
{
  is_inited_ = false;
  mlog_maintenance_task_.destroy();
  mview_maintenance_task_.destroy();
  mvref_stats_maintenance_task_.destroy();
}

int ObMViewMaintenanceService::inner_switch_to_leader()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  int64_t start_time_us = ObTimeUtility::current_time();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMViewMaintenanceService not init", KR(ret), KP(this));
  } else {
    if (OB_FAIL(mlog_maintenance_task_.start())) {
      LOG_WARN("fail to start mlog maintenance task", KR(ret));
    } else if (OB_FAIL(mview_maintenance_task_.start())) {
      LOG_WARN("fail to start mview maintenance task", KR(ret));
    } else if (OB_FAIL(mvref_stats_maintenance_task_.start())) {
      LOG_WARN("fail to start mvref stats maintenance task", KR(ret));
    }
  }
  const int64_t cost_us = ObTimeUtility::current_time() - start_time_us;
  FLOG_INFO("mview_maintenance: switch_to_leader", KR(ret), K(tenant_id), K(cost_us));
  return ret;
}

int ObMViewMaintenanceService::inner_switch_to_follower()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  int64_t start_time_us = ObTimeUtility::current_time();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMViewMaintenanceService not init", KR(ret), KP(this));
  } else {
    mvref_stats_maintenance_task_.stop();
  }
  const int64_t cost_us = ObTimeUtility::current_time() - start_time_us;
  FLOG_INFO("mview_maintenance: switch_to_follower", KR(ret), K(tenant_id), K(cost_us));
  return ret;
}

void ObMViewMaintenanceService::switch_to_follower_forcedly()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(inner_switch_to_follower())) {
    LOG_WARN("failed to switch leader", KR(ret));
  }
}

int ObMViewMaintenanceService::switch_to_leader()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(inner_switch_to_leader())) {
    LOG_WARN("failed to switch leader", KR(ret));
  }
  return ret;
}

int ObMViewMaintenanceService::switch_to_follower_gracefully()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(inner_switch_to_follower())) {
    LOG_WARN("failed to switch leader", KR(ret));
  }
  return ret;
}

int ObMViewMaintenanceService::resume_leader()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(inner_switch_to_leader())) {
    LOG_WARN("failed to switch leader", KR(ret));
  }
  return ret;
}

} // namespace rootserver
} // namespace oceanbase
