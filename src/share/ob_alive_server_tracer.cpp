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
#include "share/ob_web_service_root_addr.h"
#include "share/ob_thread_mgr.h"
#include "observer/ob_server_struct.h"
#include "share/ob_all_server_tracer.h"
namespace oceanbase
{
namespace share
{
using namespace common;

ObAliveServerMap::ObAliveServerMap() : is_inited_(false), trace_time_(0),
    active_servers_(), inactive_servers_()
{
}

ObAliveServerMap::~ObAliveServerMap()
{
}

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

int ObAliveServerMap::is_alive(const ObAddr &addr, bool &alive, int64_t &trace_time) const
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

int ObAliveServerMap::get_server_status(const ObAddr &addr, bool &alive,
                                        bool &is_server_exist,int64_t &trace_time) const
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

int ObAliveServerMap::refresh()
{
  int ret = OB_SUCCESS;
  common::ObArray<ObAddr> active_server_list;
  common::ObArray<ObAddr> inactive_server_list;
  ObZone empty_zone;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(SVR_TRACER.get_servers_by_status(empty_zone, active_server_list, inactive_server_list))) {
    LOG_WARN("fail to get servers by status", KR(ret));
  } else if (active_server_list.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument, empty server list", K(ret));
  } else {
    ObLatchWGuard guard(lock_, ObLatchIds::ALIVE_SERVER_TRACER_LOCK);
    if (OB_FAIL(refresh_server_list(active_server_list, active_servers_, "active"))) {
      LOG_WARN("fail to refresh server list", KR(ret), K(active_server_list));
    } else if (OB_FAIL(refresh_server_list(inactive_server_list, inactive_servers_, "inactive"))) {
      LOG_WARN("fail to refresh server list", KR(ret), K(inactive_server_list));
    }
    if (OB_SUCC(ret)) {
      trace_time_ = ObTimeUtility::current_time();
    }
  }
  return ret;
}

int ObAliveServerMap::refresh_server_list(const ObIArray<ObAddr> &server_list,
                                          hash::ObHashSet<ObAddr, common::hash::NoPthreadDefendMode> &servers,
                                          const char *server_list_type)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(server_list_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("server_list_type is null", K(ret));
  } else {
    FOREACH_CNT_X(s, server_list, OB_SUCC(ret)) {
      ret = servers.exist_refactored(*s);
      if (OB_HASH_EXIST == ret) {
        ret = OB_SUCCESS;
      } else if (OB_HASH_NOT_EXIST == ret) {
        if (OB_FAIL(servers.set_refactored(*s))) {
          LOG_WARN("add server hash set failed", KCSTRING(server_list_type), K(ret), "server", *s);
        } else {
          _LOG_INFO("add server to %s server map: %s", server_list_type, to_cstring(s));
        }
      } else {
        LOG_WARN("hash set exist failed", KCSTRING(server_list_type), K(ret), "server", *s);
      }
    }
    if (OB_SUCC(ret) && servers.size() > server_list.count()) {
      ObArray<ObAddr> remove_set;
      FOREACH_X(s, servers, OB_SUCC(ret)) {
        if (!has_exist_in_array(server_list, s->first)) {
          if (OB_FAIL(remove_set.push_back(s->first))) {
            LOG_WARN("add server to array failed", KCSTRING(server_list_type), K(ret), "server", s->first);
          }
        }
      }
      FOREACH_X(s, remove_set, OB_SUCC(ret)) {
        if (OB_FAIL(servers.erase_refactored(*s))) {
          LOG_WARN("erase from hash set failed", KCSTRING(server_list_type), K(ret), "server", *s);
        } else {
          _LOG_INFO("remove server from %s server map: %s", server_list_type, to_cstring(*s));
        }
      }
    }
  }
  return ret;
}

int ObAliveServerMap::get_active_server_list(common::ObIArray<common::ObAddr> &addrs) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    addrs.reset();
    ObLatchRGuard guard(lock_, ObLatchIds::ALIVE_SERVER_TRACER_LOCK);
    common::hash::ObHashSet<common::ObAddr, common::hash::NoPthreadDefendMode>::const_iterator iter;
    for (iter = active_servers_.begin(); OB_SUCC(ret) && iter != active_servers_.end(); ++iter) {
      if (OB_FAIL(addrs.push_back(iter->first))) {
        LOG_WARN("fail to push back addr", KR(ret));
      }
    }
  }
  return ret;
}

ObAliveServerRefreshTask::ObAliveServerRefreshTask(ObAliveServerTracer &tracer)
  : tracer_(tracer), is_inited_(false)
{
}

ObAliveServerRefreshTask::~ObAliveServerRefreshTask()
{
}

int ObAliveServerRefreshTask::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(TG_SCHEDULE(lib::TGDefIDs::ServerTracerTimer, *this,
                                 REFRESH_INTERVAL_US))) {
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

    if (OB_FAIL(TG_SCHEDULE(lib::TGDefIDs::ServerTracerTimer, *this,
                            REFRESH_INTERVAL_US))) {
      // overwrite ret
      // schedule task fail is fatal ERROR
      if (OB_CANCELED != ret) {
        LOG_ERROR("schedule task failed", K(ret), "task", *this);
      } else {
        LOG_WARN("schedule task failed", K(ret), "task", *this);
      }
    }
  }
}

void ObAliveServerTracer::ServerAddr::reset()
{
  server_.reset();
  sql_port_ = OB_INVALID_ID;
}
int ObAliveServerTracer::ServerAddr::init(
    const common::ObAddr addr, const int64_t sql_port)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!addr.is_valid() || OB_INVALID_ID == sql_port)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("addr or sql port is invalid", KR(ret), K(addr), K(sql_port));
  } else {
    server_ = addr;
    sql_port_ = sql_port;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObAliveServerTracer::ServerAddr, server_, sql_port_);


ObAliveServerTracer::ObAliveServerTracer()
  : is_inited_(false), cur_map_(&server_maps_[0]), last_map_(&server_maps_[1]),
    rpc_proxy_(NULL), task_(*this), sql_proxy_(NULL), primary_cluster_id_(OB_INVALID_ID)
{
}

ObAliveServerTracer::~ObAliveServerTracer()
{
}

int ObAliveServerTracer::init(obrpc::ObCommonRpcProxy &rpc_proxy,
                              common::ObMySQLProxy &sql_proxy)
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

int ObAliveServerTracer::is_alive(const ObAddr &addr, bool &alive, int64_t &trace_time) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!addr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(addr));
  } else {
    const ObAliveServerMap *volatile map = cur_map_;
    if (OB_ISNULL(map)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null pointer", K(ret));
    } else if (OB_FAIL(map->is_alive(addr, alive, trace_time))) {
      LOG_WARN("check server alive failed", K(ret), K(addr));
    }
  }
  return ret;
}

int ObAliveServerTracer::get_server_status(const ObAddr &addr, bool &alive,
                                           bool &is_server_exist, int64_t &trace_time) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!addr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(addr));
  } else {
    const ObAliveServerMap *volatile map = cur_map_;
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
    if (OB_FAIL(last_map_->refresh())) {
      LOG_WARN("refresh sever list failed", K(ret));
    } else {
      ObAliveServerMap *volatile map = cur_map_;
      ATOMIC_SET(&cur_map_, last_map_);
      last_map_ = map;
      int64_t tmp_ret = refresh_primary_cluster_id();
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("fail to refresh primary cluster id", KR(ret));
      }
    }
  }
  return ret;
}

int ObAliveServerTracer::refresh_primary_cluster_id()
{
  int ret = OB_SUCCESS;
  LOG_INFO("refresh primary cluster finish", K(ret), K(primary_cluster_id_));
  return ret;
}

int ObAliveServerTracer::get_primary_cluster_id(int64_t &cluster_id) const
{
  int ret = OB_SUCCESS;
  cluster_id = primary_cluster_id_;
  return ret;
}

int ObAliveServerTracer::get_active_server_list(common::ObIArray<common::ObAddr> &addrs) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    const ObAliveServerMap *volatile map = cur_map_;
    if (OB_ISNULL(map)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null pointer", KR(ret));
    } else if (OB_FAIL(map->get_active_server_list(addrs))) {
      LOG_WARN("check server alive failed", KR(ret), K(addrs));
    }
  }
  return ret;
}
} // end namespace share
} // end namespace oceanbase
