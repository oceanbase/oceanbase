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

#include "share/location_cache/ob_vtable_location_service.h"
#include "share/location_cache/ob_location_struct.h" // ObVTableLocationCacheKey/Value
#include "share/cache/ob_cache_name_define.h" // OB_VTABLE_CACHE_NAME
#include "share/ob_common_rpc_proxy.h" // ObCommonRpcProxy
#include "share/ob_rs_mgr.h" // ObRsMgr
#include "share/ob_alive_server_tracer.h" // ObIAliveServerTracer
#include "share/ob_share_util.h" //ObShareUtil

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
      vtable_cache_(),
      update_queue_(),
      server_tracer_(NULL),
      rs_mgr_(NULL),
      rpc_proxy_(NULL)
{
}

int ObVTableLocationService::init(
    ObIAliveServerTracer &server_tracer,
    ObRsMgr &rs_mgr,
    obrpc::ObCommonRpcProxy &rpc_proxy)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
    //TODO move vtable location cache from kvcache to inner cache
  } else if (OB_FAIL(vtable_cache_.init(OB_VTABLE_CACHE_NAME, 1000))) {
    LOG_WARN("vtable_cache init failed", KR(ret));
  } else if (OB_FAIL(update_queue_.init(this, 1/*thread_cnt*/, 100/*queue_size*/, "VTblLocAsyncUp"))) {
    LOG_WARN("update_queue init failed", KR(ret));
  } else {
    server_tracer_ = &server_tracer;
    rs_mgr_ = &rs_mgr;
    rpc_proxy_ = &rpc_proxy;
    inited_ = true;
  }
  return ret;
}

//TODO: 1.remove ObPartitionLocation 2.add auto renew 3.cached by tenant_id
int ObVTableLocationService::vtable_get(
    const uint64_t tenant_id,
    const uint64_t table_id,
    const int64_t expire_renew_time,
    bool &is_cache_hit,
    ObIArray<common::ObAddr> &locations)
{
  int ret = OB_SUCCESS;
  bool need_renew = false;
  is_cache_hit = false;
  ObSArray<ObPartitionLocation> vtable_locations;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == table_id
      || !is_virtual_table(table_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(table_id));
  } else if (OB_FAIL(get_from_vtable_cache_(tenant_id, table_id, vtable_locations))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      need_renew = true;
      LOG_TRACE("location not exist, need renew", K(table_id));
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("get location from vtable cache failed", KR(ret), K(table_id));
    }
  } else if (vtable_locations.size() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("vtable location size should larger than 0", KR(ret), K(locations));
  } else if (vtable_locations.at(0).get_renew_time() <= expire_renew_time) {
    need_renew = true;
    LOG_TRACE("locations are expired, need renew", K(table_id), K(expire_renew_time),
        "location renew time", vtable_locations.at(0).get_renew_time());
  } else { // check server alive
    bool alive = false;
    int64_t trace_time = 0;
    FOREACH_CNT_X(location, vtable_locations, OB_SUCC(ret) && !need_renew) {
      FOREACH_CNT_X(l, location->get_replica_locations(), OB_SUCC(ret) && !need_renew) {
        if (OB_FAIL(server_tracer_->is_alive(l->server_, alive, trace_time))) {
          LOG_WARN("check server alive failed", KR(ret), "server", l->server_);
        } else if (!alive && trace_time > location->get_renew_time()) {
          need_renew = true;
          LOG_TRACE("force renew vtable location for non-alive server", K(location));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (need_renew) { // synchronous renew
      is_cache_hit = false;
      if (OB_FAIL(renew_vtable_location_(tenant_id, table_id, vtable_locations))) {
        LOG_WARN("renew vtable location failed", KR(ret), K(table_id));
      }
    } else {
      is_cache_hit = true;
      if (vtable_locations.size() > 0 
          && (ObTimeUtility::current_time() 
              - vtable_locations.at(0).get_renew_time()
              >= GCONF.virtual_table_location_cache_expire_time)) { // asynchronous renew
        int temp_ret = vtable_nonblock_renew(tenant_id, table_id);
        if (OB_SUCCESS != temp_ret) {
          LOG_WARN("add location update task failed", 
              KR(temp_ret), K(tenant_id), K(table_id), K(vtable_locations));
        } else {
          LOG_TRACE("vtable nonblock renew success", K(tenant_id), K(table_id));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    ARRAY_FOREACH_N(vtable_locations, idx, cnt) {
      const ObPartitionLocation &pt_location = vtable_locations.at(idx);
      if (OB_UNLIKELY(pt_location.get_replica_locations().empty())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("empty replica location in vtable locations", KR(ret), K(vtable_locations));
      } else {
        const ObAddr &server = pt_location.get_replica_locations().at(0).server_;
        if (OB_FAIL(locations.push_back(server))) {
          LOG_WARN("fail to push back serever", KR(ret), K(server), K(locations));
        }
      }
    }
  }

  if (OB_TIMEOUT == ret) {
    ret = OB_GET_LOCATION_TIME_OUT;
  }
  return ret;
}

int ObVTableLocationService::renew_vtable_location_(
    const uint64_t tenant_id,
    const uint64_t table_id,
    ObSArray<ObPartitionLocation> &locations)
{
  int ret = OB_SUCCESS;
  ObTimeoutCtx ctx;
  int64_t default_timeout = GCONF.location_cache_refresh_sql_timeout;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == table_id || !is_virtual_table(table_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(table_id));
  } else if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, default_timeout))) {
    LOG_WARN("failed to set timeout ctx", KR(ret));
  } else if (OB_FAIL(fetch_vtable_location_(tenant_id, table_id, locations))) {
    LOG_WARN("fetch_vtable_location_ failed", KR(ret), K(table_id));
  } else if (OB_FAIL(update_vtable_cache_(tenant_id, table_id, locations))) {
    LOG_WARN("update_vtable_location failed",  KR(ret), K(table_id));
  } else {
    ObTaskController::get().allow_next_syslog();
    LOG_INFO("renew vtable location success",  KR(ret), K(tenant_id), K(table_id), K(locations));
  }
  return ret;
}

int ObVTableLocationService::get_from_vtable_cache_(
    const uint64_t tenant_id,
    const uint64_t table_id,
    common::ObSArray<ObPartitionLocation> &locations)
{
  int ret = OB_SUCCESS;
  ObKVCacheHandle handle;
  ObVTableLocationCacheKey cache_key(tenant_id, table_id);
  const ObLocationKVCacheValue *cache_value = NULL;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == table_id || !is_virtual_table(table_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(table_id));
  } else if (OB_FAIL(vtable_cache_.get(cache_key, cache_value, handle))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("get location from vtable cache failed", KR(ret), K(cache_key));
    } else {
      LOG_TRACE("location is not in vtable cache", KR(ret), K(cache_key));
    }
  } else if (OB_FAIL(cache_value2location_(*cache_value, locations))) {
    LOG_WARN("transform cache value to location failed", KR(ret), K(cache_value));
  } else {
    LOG_TRACE("location hit in vtable cache", KR(ret), K(table_id), K(locations));
  }
  return ret;
}

int ObVTableLocationService::fetch_vtable_location_(
    const uint64_t tenant_id,
    const uint64_t table_id,
    ObSArray<ObPartitionLocation> &locations)
{
  NG_TRACE(fetch_vtable_loc_begin);
  int ret = OB_SUCCESS;
  ObVtableLocationType vtable_type;
  locations.reset();
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_INVALID_ID == table_id || !is_virtual_table(table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table is not virtual table", K(table_id), KR(ret));
  } else if (OB_FAIL(vtable_type.gen_by_tenant_id_and_table_id(tenant_id, table_id))) {
    LOG_WARN("fail to generate vtable location type by table id",
        KR(ret), K(tenant_id), K(table_id));
  } else if (vtable_type.is_only_local()) {
    // FIXME:(yanmu.ztl) just to make it work around
    const int64_t now = ObTimeUtility::current_time();
    ObPartitionLocation location;
    location.set_table_id(table_id);
    location.set_partition_id(table_id);
    location.set_partition_cnt(0);
    location.set_renew_time(now);
    location.set_sql_renew_time(now);

    ObReplicaLocation replica_location;
    replica_location.role_ = LEADER;
    replica_location.server_ = GCONF.self_addr_;
    replica_location.sql_port_ = GCONF.mysql_port;

    if (OB_FAIL(location.add(replica_location))) {
      LOG_WARN("location add failed", KR(ret), K(table_id));
    } else if (OB_FAIL(locations.push_back(location))) {
      LOG_WARN("fail to push back location", KR(ret), K(location));
    }
  } else {
    LOG_INFO("fetch virtual table location with tenant",
        "rpc tenant", THIS_WORKER.get_rpc_tenant(), K(tenant_id), K(table_id), K(vtable_type));
    obrpc::ObFetchLocationArg arg;
    obrpc::ObFetchLocationResult res;
    ObTimeoutCtx ctx;
    int64_t default_timeout = GCONF.location_cache_refresh_sql_timeout;
    if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, default_timeout))) {
      LOG_WARN("failed to set timeout ctx", KR(ret));
    } else if (OB_ISNULL(rs_mgr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rs_mgr is null", KR(ret));
    } else if (OB_FAIL(arg.init(vtable_type))) {
      LOG_WARN("fail to init arg", KR(ret), K(arg));
    } else {
      int64_t timeout_us = ctx.get_timeout();
      if (OB_FAIL(rpc_proxy_->to_rs(*rs_mgr_)
                        .by(OB_SYS_TENANT_ID)
                        .timeout(timeout_us)
                        .fetch_location(arg, res))) {
        LOG_WARN("fetch location through rpc failed", KR(ret), K(arg));
      } else {
        //FIXME:just to make it work around. ObPartitionLocation will be removed later.
        const int64_t now = ObTimeUtility::current_time();
        ObPartitionLocation location;
        ObReplicaLocation replica_location;
        for (int64_t i = 0; OB_SUCC(ret) && i < res.get_servers().count(); ++i) {
          location.reset();
          replica_location.reset();
          location.set_table_id(table_id); // meaningless
          location.set_partition_id(i); // meaningless
          location.set_partition_cnt(res.get_servers().count()); // meaningless
          location.set_renew_time(now); // useful
          replica_location.role_ = LEADER; // meaningless
          replica_location.sql_port_ = GCONF.mysql_port; // meaningless
          replica_location.server_ = res.get_servers().at(i);  //useful
          if (OB_FAIL(location.add(replica_location))) {
            LOG_WARN("location add failed", K(replica_location), K(ret));
          } else if (OB_FAIL(locations.push_back(location))) {
            LOG_WARN("push_back failed", K(location), K(ret));
          }
        }
      }
    }
  }
  NG_TRACE_EXT(fetch_vtable_loc_end, OB_ID(ret), ret, OB_ID(table_id), table_id);
  return ret;
}

int ObVTableLocationService::update_vtable_cache_(
    const uint64_t tenant_id,
    const uint64_t table_id,
    const common::ObSArray<ObPartitionLocation> &locations)
{
  const int64_t MAX_BUFFER_SIZE = 1L << 20; // 1MB
  int ret = OB_SUCCESS;
  ObVTableLocationCacheKey cache_key(tenant_id, table_id);
  ObLocationKVCacheValue cache_value;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == table_id
      || !is_virtual_table(table_id)
      || locations.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(table_id), "location count", locations.count());
  } else {
    const int64_t buffer_size = locations.get_serialize_size();
    if (OB_UNLIKELY(buffer_size <= 0 || buffer_size > MAX_BUFFER_SIZE)) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("size overflow", KR(ret), K(buffer_size));
    } else {
      char *buffer = static_cast<char*>(ob_malloc(buffer_size, ObModIds::OB_MS_LOCATION_CACHE));
      if (OB_ISNULL(buffer)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory",
            KR(ret), K(buffer_size), K(table_id), K(locations));
      } else if (OB_FAIL(location2cache_value_(
          locations,
          buffer,
          buffer_size,
          cache_value))) {
        LOG_WARN("transform location to cache_value failed", K(locations),
            KR(ret), K(buffer_size));
      } else if (OB_FAIL(vtable_cache_.put(cache_key, cache_value))) {
        LOG_WARN("put location to vtable cache failed",
            KR(ret), K(cache_key), K(cache_value));
      } else {
        LOG_TRACE("renew location in vtable_cache succeed", K(cache_key), K(locations));
      }
      if (NULL != buffer) {
        ob_free(buffer);
        buffer = NULL;
      }
    }
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
    ObSArray<ObPartitionLocation> locations;
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

int ObVTableLocationService::cache_value2location_(
    const ObLocationKVCacheValue &cache_value,
    common::ObSArray<ObPartitionLocation> &locations)
{
  int ret = OB_SUCCESS;
  NG_TRACE(plc_serialize_begin);
  char *buffer = cache_value.get_buffer_ptr();
  int64_t size = cache_value.get_size();
  int64_t pos = 0;
  if (NULL == buffer || size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid cache_value", KR(ret), K(buffer), K(size));
  } else if (OB_FAIL(locations.deserialize(buffer, size, pos))) {
    LOG_WARN("deserialize location failed", KR(ret), K(cache_value));
  }
  NG_TRACE(plc_serialize_end);
  return ret;
}

int ObVTableLocationService::location2cache_value_(
    const common::ObSArray<ObPartitionLocation> &locations,
    char *buf, const int64_t buf_size,
    ObLocationKVCacheValue &cache_value)
{
  int ret = OB_SUCCESS;
  NG_TRACE(plc_serialize_begin);
  int64_t pos = 0;
  if (OB_UNLIKELY(NULL == buf || buf_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf is null or invalid buf_size", KP(buf), K(buf_size), KR(ret));
  } else if (OB_FAIL(locations.serialize(buf, buf_size, pos))) {
    LOG_WARN("serialize location failed", KR(ret), K(locations),
        "buf", reinterpret_cast<int64_t>(buf));
  } else {
    cache_value.set_size(pos);
    cache_value.set_buffer(buf);
  }
  NG_TRACE(plc_serialize_end);
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
  vtable_cache_.destroy();
  return ret;
}

int ObVTableLocationService::reload_config()
{
  // nothing need reload
  return OB_SUCCESS;
}

} // end namespace share
} // end namespace oceanbase
