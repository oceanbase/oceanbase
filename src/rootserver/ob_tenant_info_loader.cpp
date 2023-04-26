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

#include "share/rc/ob_tenant_base.h"    // MTL_ID
#include "share/scn.h"//SCN
#include "observer/ob_server_struct.h"          // GCTX
#include "rootserver/ob_tenant_info_loader.h"
#include "rootserver/ob_tenant_role_transition_service.h"//ObTenantRoleTransitionConstants

namespace oceanbase
{
using namespace share;
using namespace common;
namespace rootserver
{
int ObTenantInfoLoader::mtl_init(ObTenantInfoLoader *&ka)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(ka)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ka is null", KR(ret));
  } else if (OB_FAIL(ka->init())) {
    LOG_WARN("failed to init", KR(ret), KP(ka));
  }

  return ret;
}

int ObTenantInfoLoader::init()
{
  int ret = OB_SUCCESS;
  lib::ThreadPool::set_run_wrapper(MTL_CTX());
  const int64_t thread_cnt = 1;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else {
    sql_proxy_ = GCTX.sql_proxy_;
    tenant_id_ = MTL_ID();
    tenant_info_cache_.reset();
    if (!is_user_tenant(tenant_id_)) {
    } else if (OB_ISNULL(GCTX.sql_proxy_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sql proxy is null", KR(ret));
    } else if (OB_FAIL(create(thread_cnt, "TenantInf"))) {
      LOG_WARN("failed to create tenant info loader thread", KR(ret), K(thread_cnt));
    }
  }
  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }
  return ret;
}

void ObTenantInfoLoader::destroy()
{
  LOG_INFO("tenant info loader destory", KPC(this));
  stop();
  wait();
  is_inited_ = false;
  tenant_id_ = OB_INVALID_TENANT_ID;
  tenant_info_cache_.reset();
  sql_proxy_ = NULL;
}

int ObTenantInfoLoader::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!is_user_tenant(tenant_id_)) {
    LOG_INFO("not user tenant no need load", K(tenant_id_));
  } else if (OB_FAIL(logical_start())) {
    LOG_WARN("failed to start", KR(ret));
  } else {
    ObThreadCondGuard guard(get_cond());
    get_cond().broadcast();
    LOG_INFO("tenant info loader start", KPC(this));
  }
  return ret;
}

void ObTenantInfoLoader::stop()
{
  logical_stop();
}
void ObTenantInfoLoader::wait()
{
  logical_wait();
}

void ObTenantInfoLoader::wakeup()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    ObThreadCondGuard guard(get_cond());
    get_cond().broadcast();
  }
}

void ObTenantInfoLoader::run2()
{
  int ret = OB_SUCCESS;
  LOG_INFO("tenant info loader run", KPC(this));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    ObThreadCondGuard guard(get_cond());
    const int64_t idle_time = ObTenantRoleTransitionConstants::TENANT_INFO_REFRESH_TIME_US;
    while (!stop_) {
      if (OB_FAIL(tenant_info_cache_.refresh_tenant_info(tenant_id_, sql_proxy_))) {
        LOG_WARN("failed to update tenant info", KR(ret), K_(tenant_id), KP(sql_proxy_));
      }

      if (!stop_) {
        get_cond().wait_us(idle_time);
      }
    }//end while
  }
}

int ObTenantInfoLoader::get_valid_sts_after(const int64_t specified_time_us, share::SCN &standby_scn)
{
  int ret = OB_SUCCESS;
  standby_scn.set_min();
  share::ObAllTenantInfo tenant_info;
  int64_t refresh_time_us = OB_INVALID_TIMESTAMP;

  if (OB_INVALID_TIMESTAMP == specified_time_us) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(specified_time_us));
  } else if (OB_FAIL(tenant_info_cache_.get_tenant_info(tenant_info, refresh_time_us))) {
    if (OB_NEED_WAIT == ret) {
      LOG_TRACE("tenant info cache is not refreshed, need wait", KR(ret));
    } else {
      LOG_WARN("failed to get tenant info", KR(ret));
    }
  } else if (refresh_time_us <= specified_time_us) {
    ret = OB_NEED_WAIT;
    LOG_TRACE("tenant info cache is old, need wait", KR(ret), K(refresh_time_us), K(specified_time_us), K(tenant_info));
    wakeup();
  } else if (!tenant_info.is_sts_ready()) {
    ret = OB_NEED_WAIT;
    LOG_TRACE("sts can not work for current tenant status", KR(ret), K(tenant_info));
  } else {
    standby_scn = tenant_info.get_standby_scn();
  }

  const int64_t PRINT_INTERVAL = 3 * 1000 * 1000L;
  if (REACH_TIME_INTERVAL(PRINT_INTERVAL)) {
    LOG_INFO("get_valid_sts_after", KR(ret), K(specified_time_us), K(refresh_time_us), K(tenant_info));
  }

  return ret;
}

int ObTenantInfoLoader::get_tenant_info(share::ObAllTenantInfo &tenant_info)
{
  int ret = OB_SUCCESS;
  tenant_info.reset();

  if (OB_FAIL(tenant_info_cache_.get_tenant_info(tenant_info))) {
    LOG_WARN("failed to get tenant info", KR(ret));
  }
  return ret;
}

int ObTenantInfoLoader::refresh_tenant_info()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(tenant_info_cache_.refresh_tenant_info(tenant_id_, sql_proxy_))) {
    LOG_WARN("failed to refresh_tenant_info", KR(ret), K_(tenant_id), KP(sql_proxy_));
  }
  return ret;
}

void ObAllTenantInfoCache::reset()
{
  SpinWLockGuard guard(lock_);
  tenant_info_.reset();
  refresh_time_us_ = OB_INVALID_TIMESTAMP;
}

int ObAllTenantInfoCache::refresh_tenant_info(const uint64_t tenant_id, common::ObMySQLProxy *sql_proxy)
{
  int ret = OB_SUCCESS;
  ObAllTenantInfo new_tenant_info;
  const int64_t new_refresh_time_us = ObClockGenerator::getCurrentTime();
  if (OB_ISNULL(sql_proxy) || !is_user_tenant(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), KP(sql_proxy));
  } else if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(tenant_id,
             sql_proxy, false /* for_update */, new_tenant_info))) {
    LOG_WARN("failed to load tenant info", KR(ret), K(tenant_id));
  } else {
    /**
    * Only need to refer to tenant role, no need to refer to switchover status.
    * tenant_role is primary in <primary, normal switchoverstatus> or <primary, prep switching_to_standby switchover_status>.
    * When switch to standby starts, it will change to <standby, switch to standby>.
    * During the switch to standby process, some LS may be in RO state. GTS & STS may not work.
    * This also ensures the consistency of tenant_role cache and the tenant role field in all_tenant_info
    */
    MTL_SET_TENANT_ROLE(new_tenant_info.get_tenant_role().value());
    SpinWLockGuard guard(lock_);
    (void)tenant_info_.assign(new_tenant_info);
    refresh_time_us_ = new_refresh_time_us;
  }

  if (dump_tenant_info_interval_.reach()) {
    LOG_INFO("refresh tenant info", KR(ret), K(new_tenant_info), K(new_refresh_time_us),
                                    K(tenant_id), K(tenant_info_), K(refresh_time_us_));
  }

  return ret;
}

int ObAllTenantInfoCache::get_tenant_info(share::ObAllTenantInfo &tenant_info, int64_t &refresh_time_us)
{
  int ret = OB_SUCCESS;
  tenant_info.reset();
  refresh_time_us = OB_INVALID_TIMESTAMP;
  SpinRLockGuard guard(lock_);

  if (!tenant_info_.is_valid() || OB_INVALID_TIMESTAMP == refresh_time_us_) {
    ret = OB_NEED_WAIT;
    const int64_t PRINT_INTERVAL = 1 * 1000 * 1000L;
    if (REACH_TIME_INTERVAL(PRINT_INTERVAL)) {
      LOG_WARN("tenant info is invalid, need wait", KR(ret), K(refresh_time_us_), K(tenant_info_));
    }
  } else {
    (void)tenant_info.assign(tenant_info_);
    refresh_time_us = refresh_time_us_;
  }
  return ret;
}

int ObAllTenantInfoCache::get_tenant_info(share::ObAllTenantInfo &tenant_info)
{
  int ret = OB_SUCCESS;
  tenant_info.reset();
  int64_t refresh_time_us = OB_INVALID_TIMESTAMP;

  if (OB_FAIL(get_tenant_info(tenant_info, refresh_time_us))) {
    LOG_WARN("failed to get tenant info", KR(ret));
  }
  return ret;
}

}
}
