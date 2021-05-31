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

#define USING_LOG_PREFIX SHARE

#include "share/ob_alive_server_tracer.h"
#include "lib/container/ob_array_iterator.h"
#include "ob_common_rpc_proxy.h"
#include "config/ob_server_config.h"
#include "share/ob_cluster_info_proxy.h"
#include "share/ob_multi_cluster_util.h"
#include "share/ob_web_service_root_addr.h"
#include "share/ob_thread_mgr.h"
#include "observer/ob_server_struct.h"

namespace oceanbase {
namespace share {
using namespace common;

ObAliveServerMap::ObAliveServerMap() : is_inited_(false), trace_time_(0), active_servers_(), inactive_servers_()
{}

ObAliveServerMap::~ObAliveServerMap()
{}

int ObAliveServerMap::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(active_servers_.create(HASH_SERVER_CNT))) {
    LOG_WARN("create hash set failed", K(ret));
  } else if (OB_FAIL(inactive_servers_.create(HASH_SERVER_CNT))) {
    LOG_WARN("create hash set failed", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObAliveServerMap::is_alive(const ObAddr& addr, bool& alive, int64_t& trace_time) const
{
  int ret = OB_SUCCESS;
  alive = false;
  trace_time = 0;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!addr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument, invalid address", K(ret), K(addr));
  } else {
    ObLatchRGuard guard(lock_, ObLatchIds::ALIVE_SERVER_TRACER_LOCK);
    int ret = active_servers_.exist_refactored(addr);
    if (OB_HASH_EXIST == ret || OB_HASH_NOT_EXIST == ret) {
      alive = (OB_HASH_EXIST == ret);
      ret = OB_SUCCESS;
      // return alive before refresh success.
      if (!alive && 0 == active_servers_.size()) {
        alive = true;
      }
      trace_time = trace_time_;
    } else {
      LOG_WARN("hash set exist failed", K(ret), K(addr));
    }
  }
  return ret;
}

int ObAliveServerMap::get_server_status(
    const ObAddr& addr, bool& alive, bool& is_server_exist, int64_t& trace_time) const
{
  int ret = OB_SUCCESS;
  alive = false;
  trace_time = 0;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!addr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument, invalid address", K(ret), K(addr));
  } else {
    ObLatchRGuard guard(lock_, ObLatchIds::ALIVE_SERVER_TRACER_LOCK);
    int ret = active_servers_.exist_refactored(addr);
    if (OB_HASH_EXIST == ret || OB_HASH_NOT_EXIST == ret) {
      alive = (OB_HASH_EXIST == ret);
      ret = OB_SUCCESS;
      // return alive before refresh success.
      if (!alive && 0 == active_servers_.size()) {
        alive = true;
      }
      trace_time = trace_time_;
    } else {
      LOG_WARN("hash set exist failed", K(ret), K(addr));
    }
  }
  is_server_exist = true;
  if (OB_FAIL(ret) || alive) {
  } else if (OB_HASH_NOT_EXIST == (ret = inactive_servers_.exist_refactored(addr))) {
    is_server_exist = false;
    ret = OB_SUCCESS;
  } else if (OB_HASH_EXIST == ret) {
    is_server_exist = true;
    ret = OB_SUCCESS;
  } else {
    LOG_WARN("hash set exist failed", K(ret), K(addr));
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObAliveServerMap::refresh(
    common::ObIArray<ObAddr>& active_server_list, common::ObIArray<ObAddr>& inactive_server_list)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (active_server_list.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument, empty server list", K(ret));
  } else {
    ObLatchWGuard guard(lock_, ObLatchIds::ALIVE_SERVER_TRACER_LOCK);
    if (OB_FAIL(refresh_server_list(active_server_list, active_servers_))) {
      LOG_WARN("fail to refresh server list", KR(ret), K(active_server_list));
    } else if (OB_FAIL(refresh_server_list(inactive_server_list, inactive_servers_))) {
      LOG_WARN("fail to refresh server list", KR(ret), K(inactive_server_list));
    }
    if (OB_SUCC(ret)) {
      trace_time_ = ObTimeUtility::current_time();
    }
  }
  return ret;
}

int ObAliveServerMap::refresh_server_list(
    const ObIArray<ObAddr>& server_list, hash::ObHashSet<ObAddr, common::hash::NoPthreadDefendMode>& servers)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    FOREACH_CNT_X(s, server_list, OB_SUCC(ret))
    {
      ret = servers.exist_refactored(*s);
      if (OB_HASH_EXIST == ret) {
        ret = OB_SUCCESS;
      } else if (OB_HASH_NOT_EXIST == ret) {
        if (OB_FAIL(servers.set_refactored(*s))) {
          LOG_WARN("add server hash set failed", K(ret), "server", *s);
        } else {
          LOG_INFO("add server to alive server map", "server", *s);
        }
      } else {
        LOG_WARN("hash set exist failed", K(ret), "server", *s);
      }
    }
    if (OB_SUCC(ret) && servers.size() > server_list.count()) {
      ObArray<ObAddr> remove_set;
      FOREACH_X(s, servers, OB_SUCC(ret))
      {
        if (!has_exist_in_array(server_list, s->first)) {
          if (OB_FAIL(remove_set.push_back(s->first))) {
            LOG_WARN("add server to array failed", K(ret), "server", s->first);
          }
        }
      }
      FOREACH_X(s, remove_set, OB_SUCC(ret))
      {
        if (OB_FAIL(servers.erase_refactored(*s))) {
          LOG_WARN("erase from hash set failed", K(ret), "server", *s);
        } else {
          LOG_INFO("remove server from alive server map", "server", *s);
        }
      }
    }
  }
  return ret;
}

ObAliveServerRefreshTask::ObAliveServerRefreshTask(ObAliveServerTracer& tracer) : tracer_(tracer), is_inited_(false)
{}

ObAliveServerRefreshTask::~ObAliveServerRefreshTask()
{}

int ObAliveServerRefreshTask::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(TG_SCHEDULE(lib::TGDefIDs::ServerTracerTimer, *this, REFRESH_INTERVAL_US))) {
    if (OB_CANCELED != ret) {
      LOG_ERROR("schedule task failed", K(ret), "task", *this);
    } else {
      LOG_WARN("schedule task failed", K(ret), "task", *this);
    }
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObAliveServerRefreshTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), "task", *this);
  } else {
    if (OB_FAIL(tracer_.refresh())) {
      LOG_WARN("refresh alive server list failed", K(ret));
    }

    if (OB_FAIL(TG_SCHEDULE(lib::TGDefIDs::ServerTracerTimer, *this, REFRESH_INTERVAL_US))) {
      // schedule task fail is fatal ERROR
      if (OB_CANCELED != ret) {
        LOG_ERROR("schedule task failed", K(ret), "task", *this);
      } else {
        LOG_WARN("schedule task failed", K(ret), "task", *this);
      }
    }
  }
}

ObAliveServerTracer::ObAliveServerTracer()
    : is_inited_(false),
      cur_map_(&server_maps_[0]),
      last_map_(&server_maps_[1]),
      rpc_proxy_(NULL),
      task_(*this),
      primary_cluster_id_(-1)
{}

ObAliveServerTracer::~ObAliveServerTracer()
{}

int ObAliveServerTracer::init(obrpc::ObCommonRpcProxy& rpc_proxy, common::ObMySQLProxy& sql_proxy)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(server_maps_); ++i) {
      if (OB_FAIL(server_maps_[i].init())) {
        LOG_WARN("init server map failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    is_inited_ = true;
    rpc_proxy_ = &rpc_proxy;
    sql_proxy_ = &sql_proxy;
    if (OB_FAIL(task_.init())) {
      LOG_WARN("init refresh task failed", K(ret));
      is_inited_ = false;
    }
  }
  return ret;
}

int ObAliveServerTracer::is_alive(const ObAddr& addr, bool& alive, int64_t& trace_time) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!addr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(addr));
  } else {
    const ObAliveServerMap* volatile map = cur_map_;
    if (OB_ISNULL(map)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null pointer", K(ret));
    } else if (OB_FAIL(map->is_alive(addr, alive, trace_time))) {
      LOG_WARN("check server alive failed", K(ret), K(addr));
    }
  }
  return ret;
}

int ObAliveServerTracer::get_server_status(
    const ObAddr& addr, bool& alive, bool& is_server_exist, int64_t& trace_time) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!addr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(addr));
  } else {
    const ObAliveServerMap* volatile map = cur_map_;
    if (OB_ISNULL(map)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null pointer", K(ret));
    } else if (OB_FAIL(map->get_server_status(addr, alive, is_server_exist, trace_time))) {
      LOG_WARN("check server alive failed", K(ret), K(addr));
    }
  }
  return ret;
}

int ObAliveServerTracer::refresh()
{
  int ret = OB_SUCCESS;
  ObCurTraceId::init(GCONF.self_addr_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    obrpc::ObFetchAliveServerArg arg;
    obrpc::ObFetchAliveServerResult result;
    arg.cluster_id_ = GCONF.cluster_id;
    if (OB_FAIL(rpc_proxy_->fetch_alive_server(arg, result))) {
      LOG_WARN("fetch alive server failed", K(ret));
    } else if (!result.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid alive server list", K(ret), K(result));
    } else if (OB_FAIL(last_map_->refresh(result.active_server_list_, result.inactive_server_list_))) {
      LOG_WARN("refresh sever list failed", K(ret));
    } else {
      ObAliveServerMap* volatile map = cur_map_;
      ATOMIC_SET(&cur_map_, last_map_);
      last_map_ = map;
    }
    int64_t tmp_ret = refresh_primary_cluster_id();
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("fail to refresh primary cluster id", KR(ret));
    }
  }
  return ret;
}

int ObAliveServerTracer::refresh_primary_cluster_id()
{
  int ret = OB_SUCCESS;
  ObClusterAddr parent_addr;
  ObClusterInfo parent_cluster;
  DEBUG_SYNC(BEFORE_REFRESH_CLUSTER_ID);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!GCTX.is_standby_cluster()) {
    primary_cluster_id_ = GCONF.cluster_id;
  } else if (OB_FAIL(ObMultiClusterUtil::get_parent_cluster(*rpc_proxy_, *sql_proxy_, parent_cluster, parent_addr))) {
    LOG_WARN("fail to get parent cluster", KR(ret));
  } else {
    primary_cluster_id_ = parent_cluster.cluster_id_;
  }
  LOG_INFO("refresh primary cluster finish", K(ret), K(primary_cluster_id_));
  return ret;
}

int ObAliveServerTracer::get_primary_cluster_id(int64_t& cluster_id) const
{
  UNUSED(cluster_id);
  return OB_NOT_SUPPORTED;
}
}  // end namespace share
}  // end namespace oceanbase
