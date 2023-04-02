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

#define USING_LOG_PREFIX SHARE_LOCATION

#include "share/location_cache/ob_location_service.h"

namespace oceanbase
{
using namespace common;

namespace share
{
ObLocationService::ObLocationService()
    : inited_(false),
      stopped_(false),
      ls_location_service_(),
      tablet_ls_service_()
{
}

int ObLocationService::get(
    const int64_t cluster_id,
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    const int64_t expire_renew_time,
    bool &is_cache_hit,
    ObLSLocation &location)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(ls_location_service_.get(
      cluster_id,
      tenant_id,
      ls_id,
      expire_renew_time,
      is_cache_hit,
      location))) {
    LOG_WARN("fail to get log stream location",
        KR(ret), K(cluster_id), K(tenant_id), K(ls_id),
        K(expire_renew_time), K(is_cache_hit), K(location));
  }
  return ret;
}

int ObLocationService::get_leader(
    const int64_t cluster_id,
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    const bool force_renew,
    common::ObAddr &leader)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(ls_location_service_.get_leader(
      cluster_id,
      tenant_id,
      ls_id,
      force_renew,
      leader))) {
    LOG_WARN("fail to get log stream location leader",
        KR(ret), K(cluster_id), K(tenant_id),
        K(ls_id), K(leader), K(force_renew));
  }
  return ret;
}

int ObLocationService::get_leader_with_retry_until_timeout(
    const int64_t cluster_id,
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    common::ObAddr &leader,
    const int64_t abs_retry_timeout,
    const int64_t retry_interval)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(ls_location_service_.get_leader_with_retry_until_timeout(
      cluster_id,
      tenant_id,
      ls_id,
      leader,
      abs_retry_timeout,
      retry_interval))) {
    LOG_WARN("fail to get log stream location leader with retry until_timeout",
        KR(ret), K(cluster_id), K(tenant_id), K(ls_id), K(leader),
        K(abs_retry_timeout), K(retry_interval));
  }
  return ret;
}

int ObLocationService::nonblock_get(
    const int64_t cluster_id,
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    ObLSLocation &location)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(ls_location_service_.nonblock_get(
      cluster_id,
      tenant_id,
      ls_id,
      location))) {
    LOG_WARN("fail to nonblock get log stream location",
        KR(ret), K(cluster_id), K(tenant_id), K(ls_id), K(location));
  }
  return ret;
}

int ObLocationService::nonblock_get_leader(
    const int64_t cluster_id,
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    common::ObAddr &leader)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(ls_location_service_.nonblock_get_leader(
      cluster_id,
      tenant_id,
      ls_id,
      leader))) {
    LOG_WARN("fail to nonblock get log stream location leader",
        KR(ret), K(cluster_id), K(tenant_id), K(ls_id), K(leader));
  }
  return ret;
}

int ObLocationService::nonblock_renew(
    const int64_t cluster_id,
    const uint64_t tenant_id,
    const ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(ls_location_service_.nonblock_renew(
      cluster_id,
      tenant_id,
      ls_id))) {
    LOG_WARN("fail to nonblock_renew log stream location",
        KR(ret), K(cluster_id), K(tenant_id), K(ls_id));
  }
  return ret;
}

int ObLocationService::get(
    const uint64_t tenant_id,
    const ObTabletID &tablet_id,
    const int64_t expire_renew_time,
    bool &is_cache_hit,
    ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(tablet_ls_service_.get(
      tenant_id,
      tablet_id,
      expire_renew_time,
      is_cache_hit,
      ls_id))) {
    LOG_WARN("fail to get tablet to log stream",
        KR(ret), K(tenant_id), K(tablet_id),
        K(expire_renew_time), K(is_cache_hit), K(ls_id));
  }
  return ret;
}

int ObLocationService::nonblock_get(
    const uint64_t tenant_id,
    const ObTabletID &tablet_id,
    ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(tablet_ls_service_.nonblock_get(
      tenant_id,
      tablet_id,
      ls_id))) {
    LOG_WARN("fail to nonblock get tablet to log stream",
        KR(ret), K(tenant_id), K(tablet_id), K(ls_id));
  }
  return ret;
}

int ObLocationService::nonblock_renew(
    const uint64_t tenant_id,
    const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(tablet_ls_service_.nonblock_renew(
      tenant_id,
      tablet_id))) {
    LOG_WARN("fail to nonblock renew tablet to log stream",
        KR(ret), K(tenant_id), K(tablet_id));
  }
  return ret;
}

int ObLocationService::vtable_get(
    const uint64_t tenant_id,
    const uint64_t table_id,
    const int64_t expire_renew_time,
    bool &is_cache_hit,
    ObIArray<common::ObAddr> &locations)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(vtable_location_service_.vtable_get(
      tenant_id,
      table_id,
      expire_renew_time,
      is_cache_hit,
      locations))) {
    LOG_WARN("fail to get location for virtual table", KR(ret), K(tenant_id),
        K(table_id), K(expire_renew_time), K(is_cache_hit), K(locations));
  }
  return ret;
}

int ObLocationService::vtable_nonblock_renew(
    const uint64_t tenant_id,
    const uint64_t table_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(vtable_location_service_.vtable_nonblock_renew(
      tenant_id,
      table_id))) {
    LOG_WARN("fail to nonblock renew location for virtual table",
        KR(ret), K(tenant_id), K(table_id));
  }
  return ret;
}

int ObLocationService::init(
    ObLSTableOperator &ls_pt,
    schema::ObMultiVersionSchemaService &schema_service,
    common::ObMySQLProxy &sql_proxy,
    ObIAliveServerTracer &server_tracer,
    ObRsMgr &rs_mgr,
    obrpc::ObCommonRpcProxy &rpc_proxy,
    obrpc::ObSrvRpcProxy &srv_rpc_proxy)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("location service init twice", KR(ret));
  } else if (OB_FAIL(ls_location_service_.init(ls_pt, schema_service, rs_mgr, srv_rpc_proxy))) {
    LOG_WARN("ls_location_service init failed", KR(ret));
  } else if (OB_FAIL(tablet_ls_service_.init(sql_proxy))) {
    LOG_WARN("tablet_ls_service init failed", KR(ret));
  } else if (OB_FAIL(vtable_location_service_.init(server_tracer, rs_mgr, rpc_proxy))) {
    LOG_WARN("vtable_location_service init failed", KR(ret));
  } else {
    inited_ = true;
  }
  return ret;
}

int ObLocationService::start()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("location service not init", KR(ret));
  } else if (OB_FAIL(ls_location_service_.start())) {
    LOG_WARN("ls_location_service start failed", KR(ret));
  }
  // tablet_ls_service_ and vtable_location_service_ have no threads need to be started
  return ret;
}

void ObLocationService::stop()
{
  ls_location_service_.stop();
  tablet_ls_service_.stop();
}

void ObLocationService::wait()
{
  ls_location_service_.wait();
  tablet_ls_service_.wait();
}

int ObLocationService::destroy()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ls_location_service_.destroy())) {
    LOG_WARN("destroy ls_location_service failed", KR(ret));
  } else if (OB_FAIL(tablet_ls_service_.destroy())) {
    LOG_WARN("destroy tablet_ls_service failed", KR(ret));
  } else {
    stopped_ = true;
    inited_ = false;
  }
  return ret;
}

int ObLocationService::reload_config()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    if (OB_FAIL(ls_location_service_.reload_config())) {
      LOG_WARN("ls_location_service reload config failed", KR(ret));
    } else if (OB_FAIL(tablet_ls_service_.reload_config())) {
      LOG_WARN("tablet_ls_service reload config failed", KR(ret));
    }
  }
  return ret;
}

} // end namespace share
} // end namespace oceanbase
