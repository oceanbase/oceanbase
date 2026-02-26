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

#include "ob_vtable_location_service.h"
#include "share/cache/ob_cache_name_define.h" // OB_VTABLE_CACHE_NAME
#include "share/ob_common_rpc_proxy.h" // ObCommonRpcProxy
#include "share/ob_all_server_tracer.h" // ObAllServerTracer(SVR_TRACER)

namespace oceanbase
{
namespace share
{
OB_SERIALIZE_MEMBER(ObVtableLocationType, tenant_id_, type_);

int ObVtableLocationType::gen_by_tenant_id_and_table_id(
    const uint64_t tenant_id,
    const uint64_t table_id)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || OB_INVALID_ID == table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id or table_id", KR(ret), K(tenant_id), K(table_id));
  } else if (OB_UNLIKELY(!is_virtual_table(table_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table_id is not virtual table", KR(ret), K(tenant_id), K(table_id));
  } else if (is_only_rs_virtual_table(table_id)) {
    type_ = ONLY_RS;
  } else if (is_cluster_distributed_vtables(table_id)) {
    type_ = CLUSTER_DISTRIBUTED;
  } else if (is_tenant_distributed_vtables(table_id)) {
    if (is_sys_tenant(tenant_id)) {
      type_ = CLUSTER_DISTRIBUTED; // sys_tenant get cluster location
    } else {
      type_ = TENANT_DISTRIBUTED;
    }
  } else {
    type_ = ONLY_LOCAL;
  }
  if (OB_SUCC(ret)) {
    tenant_id_ = tenant_id;
  }
  return ret;
}

ObVTableLocationService::ObVTableLocationService()
    : inited_(false),
      update_queue_(),
      rs_mgr_(NULL)
{
}

int ObVTableLocationService::init(ObRsMgr &rs_mgr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_FAIL(update_queue_.init(this, 1/*thread_cnt*/, 100/*queue_size*/, "VTblLocAsyncUp"))) {
    LOG_WARN("update_queue init failed", KR(ret));
  } else {
    rs_mgr_ = &rs_mgr;
    inited_ = true;
  }
  return ret;
}

int ObVTableLocationService::vtable_get(
    const uint64_t tenant_id,
    const uint64_t table_id,
    const int64_t expire_renew_time,
    bool &is_cache_hit,
    common::ObIArray<common::ObAddr> &locations)
{
  int ret = OB_SUCCESS;
  bool need_renew = false;
  is_cache_hit = false;
  int64_t renew_time = 0;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("failed to check inner stat", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == table_id
      || !is_valid_tenant_id(tenant_id)
      || !is_virtual_table(table_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(table_id));
  } else if (OB_FAIL(get_from_vtable_cache_(tenant_id, table_id, locations, renew_time))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      need_renew = true;
      LOG_INFO("location not exist, need renew", K(tenant_id), K(table_id));
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("get location failed", KR(ret), K(tenant_id), K(table_id));
    }
  } else if (OB_UNLIKELY(locations.count() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("vtable location size should larger than 0", KR(ret), K(locations));
  } else if (renew_time <= expire_renew_time) {
    need_renew = true;
    LOG_TRACE("locations are expired, need renew", K(tenant_id), K(table_id),
        K(expire_renew_time), K(renew_time));
  }

  if (OB_SUCC(ret)) {
    if (need_renew) { // synchronous renew
      is_cache_hit = false;
      if (OB_FAIL(renew_vtable_location_(tenant_id, table_id, locations))) {
        LOG_WARN("renew vtable location failed", KR(ret), K(table_id));
      }
    } else {
      is_cache_hit = true;
      if (locations.count() > 0
          && (ObTimeUtility::current_time() 
              - renew_time
              >= GCONF.virtual_table_location_cache_expire_time)) { // asynchronous renew
        int temp_ret = vtable_nonblock_renew(tenant_id, table_id);
        if (OB_SUCCESS != temp_ret) {
          LOG_WARN("add location update task failed", 
              KR(temp_ret), K(tenant_id), K(table_id), K(locations));
        } else {
          LOG_INFO("vtable nonblock renew success", K(tenant_id), K(table_id),
              "current time", ObTimeUtility::current_time(),
              "last renew time", renew_time,
              "cache expire time", GCONF.virtual_table_location_cache_expire_time.str());
        }
      }
    }
  }

  if (OB_TIMEOUT == ret) {
    ret = OB_GET_LOCATION_TIME_OUT;
  }
  return ret;
}

int ObVTableLocationService::get_from_vtable_cache_(
    const uint64_t tenant_id,
    const uint64_t table_id,
    common::ObIArray<common::ObAddr> &locations,
    int64_t &renew_time) const
{
  int ret = OB_SUCCESS;
  locations.reset();
  renew_time = 0;
  ObVtableLocationType vtable_type;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("failed to check inner stat", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == table_id
        || !is_valid_tenant_id(tenant_id)
        || !is_virtual_table(table_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(table_id));
  } else if (OB_FAIL(vtable_type.gen_by_tenant_id_and_table_id(tenant_id, table_id))) {
    LOG_WARN("failed to generate vtable location type by tenant_id and table_id",
        KR(ret), K(tenant_id), K(table_id));
  } else if (vtable_type.is_only_local()) {
    if (OB_FAIL(locations.push_back(GCONF.self_addr_))) {
      LOG_WARN("push_back failed", KR(ret), "server", GCONF.self_addr_);
    } else {
      renew_time = ObTimeUtility::current_time();
    }
  } else if (vtable_type.is_only_rs()) {
    if (OB_FAIL(get_rs_locations_(locations, renew_time))) {
      LOG_WARN("failed to get rs location", KR(ret), K(tenant_id), K(table_id));
    }
  } else if (vtable_type.is_cluster_distributed()) {
    if (OB_FAIL(get_cluster_locations_(locations, renew_time))) {
      LOG_WARN("failed to get cluster locations ", KR(ret), K(tenant_id), K(table_id));
    }
  } else if (vtable_type.is_tenant_distributed()) {
    if (OB_FAIL(get_tenant_locations_(tenant_id, locations, renew_time))) {
      LOG_WARN("failed to get tenant locations", KR(ret), K(tenant_id), K(table_id));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the vtable_type is invalid", KR(ret), K(vtable_type));
  }
  if (OB_FAIL(ret)) {
  } else if (locations.empty()) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_INFO("location needs to be refreshed", K(tenant_id), K(table_id));
  } else {
    LOG_TRACE("location hit in vtable cache", K(vtable_type),
        K(table_id), K(locations), K(renew_time));
  }
  return ret;
}

int ObVTableLocationService::renew_vtable_location_(
    const uint64_t tenant_id,
    const uint64_t table_id,
    common::ObIArray<common::ObAddr> &locations)
{
  NG_TRACE(renew_vtable_loc_begin);
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  locations.reset();
  int64_t renew_time = 0;   // useless
  ObTimeoutCtx ctx;
  ObVtableLocationType vtable_type;
  int64_t default_timeout = GCONF.rpc_timeout;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("failed to check inner stat", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == table_id
      || !is_virtual_table(table_id))
      || !is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(table_id));
  } else if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, default_timeout))) {
    LOG_WARN("failed to set timeout ctx", KR(ret));
  } else if (OB_FAIL(vtable_type.gen_by_tenant_id_and_table_id(tenant_id, table_id))) {
    LOG_WARN("failed to generate vtable location type by table id",
        KR(ret), K(tenant_id), K(table_id));
  } else if (vtable_type.is_only_local()) {
    if (OB_FAIL(locations.push_back(GCONF.self_addr_))) {
      LOG_WARN("push_back failed", KR(ret), "server", GCONF.self_addr_);
    }
  } else if (vtable_type.is_only_rs()){
    if (OB_TMP_FAIL(rs_mgr_->renew_master_rootserver())) {
      LOG_WARN("failed to renew rs_mgr", KR(tmp_ret));
    }
    // ignore tmp_ret
    if (OB_FAIL(get_rs_locations_(locations, renew_time))) {
      LOG_WARN("failed to get refreshed rs_server", KR(ret));
    }
  } else if (vtable_type.is_cluster_distributed()) {
    if (OB_TMP_FAIL(SVR_TRACER.refresh())) {
      LOG_WARN("failed to refresh all_server_tracer", KR(tmp_ret));
    }
    // ignore tmp_ret
    if (OB_FAIL(get_cluster_locations_(locations, renew_time))) {
      LOG_WARN("failed to get refreshed alive server list", KR(ret));
    }
  } else if (vtable_type.is_tenant_distributed()) {
    if (OB_TMP_FAIL(SVR_TRACER.renew_tenant_servers_cache_by_id(tenant_id))) {
      LOG_WARN("failed to renew_tenant_servers_cache_by_id", KR(tmp_ret), K(tenant_id));
    }
    // ignore tmp_ret
    if (OB_FAIL(get_tenant_locations_(tenant_id, locations, renew_time))) {
      LOG_WARN("failed to get_alive_tenant_servers", KR(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the vtable_type is invalid", KR(ret), K(vtable_type));
  }
  // If unable to retrieve after attempting refresh,
  // set ret to OB_LOCATION_NOT_EXIST.
  if (OB_SUCC(ret) && locations.empty()) {
    ret = OB_LOCATION_NOT_EXIST;
    LOG_WARN("can not find location after attempting refresh", KR(ret),
        K(tenant_id), K(table_id));
  }
  NG_TRACE_EXT(renew_vtable_loc_end, OB_ID(ret), ret, OB_ID(table_id), table_id);
  return ret;
}

int ObVTableLocationService::get_rs_locations_(
    common::ObIArray<common::ObAddr> &server,
    int64_t &renew_time) const
{
  // Get rs location from rs_mgr_
  int ret = OB_SUCCESS;
  common::ObAddr rs_addr;
  server.reset();
  renew_time = 0;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("failed to check inner stat", KR(ret));
  } else if (OB_FAIL(rs_mgr_->get_master_root_server(rs_addr))) {
    LOG_WARN("failed to get master root servcer", KR(ret));
  } else if (OB_UNLIKELY(!rs_addr.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rs_addr is invalid", KR(ret), K(rs_addr));
  } else if (OB_FAIL(server.push_back(rs_addr))) {
    LOG_WARN("push_back failed", KR(ret), K(rs_addr));
  } else {
    renew_time = ObTimeUtility::current_time();
  }
  return ret;
}

int ObVTableLocationService::get_cluster_locations_(
    common::ObIArray<common::ObAddr> &servers,
    int64_t &renew_time) const
{
  // Get the location of all cluster machines from all_server_tracer
  int ret = OB_SUCCESS;
  ObZone all_zone;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("failed to check inner stat", KR(ret));
  } else if (OB_FAIL(SVR_TRACER.get_alive_servers_with_renew_time(all_zone, servers, renew_time))) {
    LOG_WARN("failed to get alive server list", KR(ret));
  }
  return ret;
}

int ObVTableLocationService::get_tenant_locations_(
    const uint64_t tenant_id,
    common::ObIArray<common::ObAddr> &servers,
    int64_t &renew_time) const
{
  // Get the machine location of each tenant from all_server_tracer
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("failed to check inner stat", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("the tenant_id is invalid", KR(ret), K(tenant_id));
  } else if (OB_FAIL(SVR_TRACER.get_alive_tenant_servers(tenant_id, servers, renew_time))) {
    LOG_WARN("failed to get tenant locations", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObVTableLocationService::add_update_task(const ObVTableLocUpdateTask &task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("service not init", KR(ret));
  } else if (!task.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task", KR(ret), K(task));
  } else if (OB_FAIL(update_queue_.add(task))) {
    LOG_WARN("fail to add task", KR(ret), K(task));
  }
  return ret;
}

int ObVTableLocationService::batch_process_tasks(
    const common::ObIArray<ObVTableLocUpdateTask> &tasks,
    bool &stopped)
{
  int ret = OB_SUCCESS;
  ObCurTraceId::init(GCONF.self_addr_);
  UNUSED(stopped);
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("service not init", KR(ret));
  } else if (1 != tasks.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected task count", KR(ret), "tasks count", tasks.count());
  } else {
    common::ObArray<common::ObAddr> locations;
    if (OB_FAIL(renew_vtable_location_(
        tasks.at(0).get_tenant_id(),
        tasks.at(0).get_table_id(),
        locations))) {
      LOG_WARN("fail to renew location", KR(ret), "task", tasks.at(0));
    } else {
      LOG_INFO("success to process renew task", "task", tasks.at(0), K(locations));
    }
  }
  return ret;
}

int ObVTableLocationService::process_barrier(
    const ObVTableLocUpdateTask &task,
    bool &stopped)
{
  UNUSEDx(task, stopped);
  return OB_NOT_SUPPORTED;
}

int ObVTableLocationService::check_inner_stat_() const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObVTableLocationService not init", KR(ret));
  } else if (OB_ISNULL(rs_mgr_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ptr is null", KR(ret), KP(rs_mgr_));
  }
  return ret;
}

int ObVTableLocationService::vtable_nonblock_renew(
    const uint64_t tenant_id,
    const uint64_t table_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("service not init", KR(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id || OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid log stream key",
        KR(ret), K(tenant_id), K(table_id));
  } else {
    const int64_t now = ObTimeUtility::current_time();
    ObVTableLocUpdateTask task(tenant_id, table_id, now);
    if (OB_FAIL(add_update_task(task))) {
      LOG_WARN("add location update task failed", KR(ret), K(task));
    } else {
      LOG_INFO("add_update_task succeed", KR(ret), K(task));
    }
  }
  return ret;
}

void ObVTableLocationService::stop()
{
  update_queue_.stop();
}

void ObVTableLocationService::wait()
{
  update_queue_.wait();
}

int ObVTableLocationService::destroy()
{
  int ret = OB_SUCCESS;
  update_queue_.destroy();
  inited_ = false;
  return ret;
}

int ObVTableLocationService::reload_config()
{
  // nothing need reload
  return OB_SUCCESS;
}

} // end namespace share
} // end namespace oceanbase
