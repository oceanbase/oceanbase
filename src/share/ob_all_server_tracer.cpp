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
#include "share/ob_all_server_tracer.h"
#include "observer/ob_server_struct.h"
#include "share/ob_rpc_struct.h"
#include "rootserver/ob_rs_async_rpc_proxy.h"     // ObAllServerTracerProxy
#include "share/ob_zone_table_operation.h"

using namespace oceanbase::common;
using namespace oceanbase::share;

ObServerTraceMap::ObServerTraceMap()
    : is_inited_(false),
      has_build_(false),
      lock_(ObLatchIds::ALL_SERVER_TRACER_LOCK),
      renew_time_(0),
      server_info_arr_()
{
}

ObServerTraceMap::~ObServerTraceMap()
{
}

int ObServerTraceMap::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObServerTraceMap has already been inited", K(ret));
  } else {
    SpinWLockGuard guard(lock_);
    int64_t block_size = (DEFAULT_SERVER_COUNT * sizeof(ObServerInfoInTable));
    server_info_arr_.set_block_size(block_size);
    if (OB_FAIL(server_info_arr_.reserve(DEFAULT_SERVER_COUNT))) {
      LOG_WARN("fail to reserve server info array", KR(ret));
    } else {
      has_build_ = false;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObServerTraceMap::is_server_exist(const common::ObAddr &server, bool &exist) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("server trace map has not inited", KR(ret));
  } else if (OB_UNLIKELY(!server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), K(ret));
  } else {
    SpinRLockGuard guard(lock_);
    ObServerInfoInTable server_info;
    if (OB_FAIL(find_server_info(server, server_info))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        exist = false;
      } else {
        LOG_WARN("fail to find server info", K(server), K_(server_info_arr), KR(ret));
      }
    } else {
      exist = true;
    }
  }
  return ret;
}

int ObServerTraceMap::get_server_rpc_port(const common::ObAddr &server, const int64_t sql_port,
                                                    int64_t &rpc_port, bool &exist) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("server trace map has not inited", K(ret));
  } else if (OB_UNLIKELY(!server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), K(ret));
  } else {
    SpinRLockGuard guard(lock_);
    ObServerInfoInTable status;
    if (OB_FAIL(get_rpc_port_status(server, sql_port, rpc_port, status))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        exist = false;
      } else {
        LOG_WARN("fail to find server status", K(server), K_(server_info_arr), K(ret));
      }
    } else {
      exist = true;
      LOG_TRACE("success to get rpc port in loacl", K(ret), K(rpc_port), K(sql_port));
    }
  }
  return ret;
}

int ObServerTraceMap::check_server_alive(const ObAddr &server, bool &is_alive) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("server trace map has not inited", KR(ret));
  } else if (OB_UNLIKELY(!server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", KR(ret), K(server));
  } else {
    SpinRLockGuard guard(lock_);
    ObServerInfoInTable server_info;
    if (OB_FAIL(find_server_info(server, server_info))) {
      LOG_WARN("fail to find server info", KR(ret), K(server), K_(server_info_arr));
    } else {
      is_alive = server_info.is_alive();
    }
  }
  return ret;
}

int ObServerTraceMap::check_in_service(const ObAddr &server, bool &service_started) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("server trace map has not inited", KR(ret));
  } else if (OB_UNLIKELY(!server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", KR(ret), K(server));
  } else {
    SpinRLockGuard guard(lock_);
    ObServerInfoInTable server_info;
    if (OB_FAIL(find_server_info(server, server_info))) {
      LOG_WARN("fail to find server info", KR(ret), K(server), K_(server_info_arr));
    } else {
      service_started = server_info.in_service();
    }
  }
  return ret;
}

int ObServerTraceMap::check_server_permanent_offline(const ObAddr &addr, bool &is_offline) const
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);
  ObServerInfoInTable server_info;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(find_server_info(addr, server_info))) {
    LOG_WARN("fail to find server info", K(addr), K_(server_info_arr), KR(ret));
  } else {
    is_offline = server_info.is_permanent_offline();
  }

  if ((OB_ENTRY_NOT_EXIST == ret) && server_info_arr_.empty()) {
    // if server list is empty, treat as not offline
    ret = OB_SUCCESS;
    is_offline = false;
  }
  return ret;
}

int ObServerTraceMap::for_each_server_info(
  const ObFunction<int(const ObServerInfoInTable &server_info)> &functor)
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);
  ObServerInfoInTable server_info;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < server_info_arr_.count(); i++) {
      const ObServerInfoInTable &server_info = server_info_arr_.at(i);
      if (OB_UNLIKELY(!functor.is_valid())) {
        ret = OB_EAGAIN;
        LOG_WARN("functor is invalid");
      } else if (OB_FAIL(functor(server_info))) {
        LOG_WARN("invoke functor failed", K(ret), K(server_info));
      }
    }
  }
  return ret;
}

int ObServerTraceMap::find_server_info(const ObAddr &addr, ObServerInfoInTable &server_info) const
{
  int ret = OB_SUCCESS;
  bool found = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < server_info_arr_.count() && !found; i++) {
    const ObServerInfoInTable &server_info_i = server_info_arr_.at(i);
    if (server_info_i.get_server() == addr) {
      if (OB_FAIL(server_info.assign(server_info_i))) {
        LOG_WARN("fail to assign server_info", KR(ret), K(server_info_i));
      } else {
        found = true;
      }
    }
  }
  if (OB_SUCC(ret) && !found) {
    ret = OB_ENTRY_NOT_EXIST;
  }
  return ret;
}

int ObServerTraceMap::broadcast_server_trace_() const
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObArray<ObAddr> alive_servers;
  ObZone all_zone;
  if (OB_ISNULL(GCTX.srv_rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("srv_rpc_proxy_ is nullptr", KR(ret));
  } else if (OB_FAIL(SVR_TRACER.get_alive_servers(all_zone, alive_servers))) {
    LOG_WARN("fail to get_alive_servers", KR(ret));
  } else {
    ObTimeoutCtx ctx;
    if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, GCONF.rpc_timeout))) {
      LOG_WARN("fail to set timeout ctx", KR(ret));
    } else {
      rootserver::ObAllServerTracerProxy rpc_proxy(*GCTX.srv_rpc_proxy_,
          &obrpc::ObSrvRpcProxy::all_server_tracer_send);
      obrpc::ObRefreshTenantInfoArg rpc_arg;
      int64_t timeout = ctx.get_timeout();
      // This init value has no meaning for server_trace refresh
      if (OB_FAIL(rpc_arg.init(OB_SYS_TENANT_ID))) {
        LOG_WARN("failed to init ObRefreshTenantInfoArg", KR(ret));
      } else {
        FOREACH_CNT(addr, alive_servers) {
          if (OB_TMP_FAIL(rpc_proxy.call(*addr, timeout, rpc_arg))) {
            ret = OB_SUCC(ret) ? tmp_ret : ret;
            LOG_WARN("fail to send rpc", KR(ret), KR(tmp_ret), KPC(addr), K(rpc_arg));
          }
        }
        ObArray<int> rc_array;
        if (OB_TMP_FAIL(rpc_proxy.wait_all(rc_array))) {
          ret = OB_SUCC(ret) ? tmp_ret : ret;
          LOG_WARN("fail to wait all results", KR(ret), KR(tmp_ret));
        }
      }
    }
  }
  LOG_INFO("finished broadcasting server trace", KR(ret), KR(tmp_ret), K(alive_servers));
  return ret;
}

// get rpc port by sql port, ip
int ObServerTraceMap::get_rpc_port_status(const ObAddr &addr, const int64_t sql_port,
                              int64_t &rpc_port, ObServerInfoInTable &status) const
{
  int ret = OB_SUCCESS;
  bool found = false;
  for (int64_t i = 0; (i < server_info_arr_.count()) && !found; i++) {
    const static int MAX_IP_BUFFER_LEN = 32;
    char server_ip_buf[MAX_IP_BUFFER_LEN];
    server_ip_buf[0] = '\0';
    char addr_buf[MAX_IP_BUFFER_LEN];
    addr_buf[0] = '\0';
    server_info_arr_.at(i).get_server().ip_to_string(server_ip_buf, MAX_IP_BUFFER_LEN);
    addr.ip_to_string(addr_buf, MAX_IP_BUFFER_LEN);
    if (0 == strcmp(server_ip_buf, addr_buf) && server_info_arr_.at(i).get_sql_port() == sql_port) {
      status = server_info_arr_.at(i);
      rpc_port = server_info_arr_.at(i).get_server().get_port();
      found = true;
    }
  }

  if (!found) {
    ret = OB_ENTRY_NOT_EXIST;
  }

  return ret;
}


int ObServerTraceMap::is_server_stopped(const ObAddr &addr, bool &is_stopped) const
{
  int ret = OB_SUCCESS;
  ObServerInfoInTable server_info;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("addr trace map has not inited", KR(ret));
  } else if (OB_UNLIKELY(!addr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid addr", KR(ret), K(addr));
  } else {
    SpinRLockGuard guard(lock_);
    if (OB_FAIL(find_server_info(addr, server_info))) {
      LOG_WARN("fail to find server info", KR(ret), K(addr));
    } else if (server_info.is_stopped()) {
      is_stopped = true;
    } else {

      for (int64_t j = 0; OB_SUCC(ret) && j < inactive_zone_list_.count(); ++j) {
        if (server_info.get_zone() == inactive_zone_list_.at(j)) {
          is_stopped = true;
          break;
        }
      }
    }
  }
  return ret;
}

int ObServerTraceMap::check_migrate_in_blocked(const ObAddr &addr, bool &is_block) const
{
  int ret = OB_NOT_SUPPORTED;
  UNUSED(addr);
  UNUSED(is_block);
  return ret;
}

int ObServerTraceMap::get_server_zone(const ObAddr &server, ObZone &zone) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("server trace map has not inited", KR(ret));
  } else if (OB_UNLIKELY(!server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", KR(ret), K(server));
  } else {
    SpinRLockGuard guard(lock_);
    ObServerInfoInTable server_info;
    if (OB_FAIL(find_server_info(server, server_info))) {
      LOG_WARN("fail to find server info", KR(ret), K(server));
    } else if (OB_FAIL(zone.assign(server_info.get_zone()))) {
      LOG_WARN("fail to assign zone", KR(ret), K(server_info));
    }
  }
  return ret;
}
int ObServerTraceMap::get_servers_of_zone(
      const ObZone &zone,
      ObIArray<ObAddr> &servers) const
{
  int ret = OB_SUCCESS;
  servers.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("server trace map has not inited", KR(ret));
  } else {
    SpinRLockGuard guard(lock_);
    for (int64_t i = 0; OB_SUCC(ret) && i < server_info_arr_.count(); i++) {
      const ObAddr& server = server_info_arr_.at(i).get_server();
      const ObZone& server_zone = server_info_arr_.at(i).get_zone();
      if ((server_zone == zone || zone.is_empty())) {
        if (OB_FAIL(servers.push_back(server))) {
          LOG_WARN("fail to push an element into servers", KR(ret),
              K(server), K(zone), K(server_zone));
        }
      }
    }
  }
  return ret;
}
int ObServerTraceMap::get_servers_of_zone(
      const common::ObZone &zone,
      common::ObIArray<common::ObAddr> &servers,
      common::ObIArray<uint64_t> &server_id_list) const
{
  int ret = OB_SUCCESS;
  servers.reset();
  server_id_list.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("server trace map has not inited", KR(ret));
  } else {
    SpinRLockGuard guard(lock_);
    for (int64_t i = 0; OB_SUCC(ret) && i < server_info_arr_.count(); i++) {
      const ObServerInfoInTable &server_info = server_info_arr_.at(i);
      const ObAddr& server = server_info.get_server();
      const ObZone& server_zone = server_info.get_zone();
      const uint64_t server_id = server_info.get_server_id();
      if ((server_zone == zone || zone.is_empty())) {
        if (OB_FAIL(servers.push_back(server))) {
          LOG_WARN("fail to push an element into servers", KR(ret),
              K(server), K(zone), K(server_zone));
        } else if (OB_FAIL(server_id_list.push_back(server_id))) {
          LOG_WARN("fail to push an element into server_id_list", KR(ret),
              K(server), K(zone), K(server_zone), K(server_id));
        }
      }
    }
  }
  return ret;
}
int ObServerTraceMap::get_server_info(const common::ObAddr &server, ObServerInfoInTable &server_info) const
{
  int ret = OB_SUCCESS;
  server_info.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("server trace map has not inited", KR(ret));
  } else if (OB_UNLIKELY(!server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", KR(ret), K(server));
  } else {
    SpinRLockGuard guard(lock_);
    if (OB_FAIL(find_server_info(server, server_info))) {
      LOG_WARN("fail to find server info", KR(ret), K(server));
    }
  }
  return ret;
}
int ObServerTraceMap::get_servers_info(
      const common::ObZone &zone,
      common::ObIArray<ObServerInfoInTable> &servers_info,
      bool include_permanent_offline) const
{
  int ret = OB_SUCCESS;
  servers_info.reset();
  SpinRLockGuard guard(lock_);
  for (int64_t i = 0; OB_SUCC(ret) && i < server_info_arr_.count(); ++i) {
    const ObServerInfoInTable &server_info = server_info_arr_.at(i);
    if (server_info.get_zone() == zone || zone.is_empty()) {
      if (include_permanent_offline || !server_info.is_permanent_offline()) {
        if (OB_UNLIKELY(!server_info.is_valid())) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("server info is not valid", "server info", server_info, KR(ret));
        } else if (OB_FAIL(servers_info.push_back(server_info))) {
          LOG_WARN("push back to servers_info failed", KR(ret), K(server_info));
        }
      }
    }
  }
  return ret;
}
int ObServerTraceMap::get_active_servers_info(
      const common::ObZone &zone,
      common::ObIArray<ObServerInfoInTable> &active_servers_info) const
{
  int ret = OB_SUCCESS;
  active_servers_info.reset();
  SpinRLockGuard guard(lock_);
  for (int64_t i = 0; OB_SUCC(ret) && i < server_info_arr_.count(); ++i) {
    const ObServerInfoInTable &server_info = server_info_arr_.at(i);
    if ((server_info.get_zone() == zone || zone.is_empty()) && server_info.is_active()) {
      if (OB_UNLIKELY(!server_info.is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("server info is not valid", "server info", server_info, KR(ret));
      } else if (OB_FAIL(active_servers_info.push_back(server_info))) {
        LOG_WARN("push back to active_servers_info failed", KR(ret), K(server_info));
      }
    }
  }
  return ret;
}
int ObServerTraceMap::get_alive_servers(const ObZone &zone, ObIArray<ObAddr> &server_list) const
{
  int ret = OB_SUCCESS;

  server_list.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("server trace map has not inited", KR(ret));
  } else {
    SpinRLockGuard guard(lock_);
    for (int64_t i = 0; OB_SUCC(ret) && i < server_info_arr_.count(); ++i) {
      const ObServerInfoInTable &server_info = server_info_arr_.at(i);
      const ObAddr &server = server_info.get_server();
      if ((server_info.get_zone() == zone || zone.is_empty()) && server_info.is_alive()) {
        if (OB_FAIL(server_list.push_back(server))) {
          LOG_WARN("fail to push an element into server_list", KR(ret), K(server));
        }
      }
    }
  }

  return ret;
}

int ObServerTraceMap::get_alive_servers_with_renew_time(
    const common::ObZone &zone,
    common::ObIArray<common::ObAddr> &server_list,
    int64_t &renew_time) const
{
  int ret = OB_SUCCESS;
  server_list.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("server trace map has not inited", KR(ret));
  } else {
    SpinRLockGuard guard(lock_);
    for (int64_t i = 0; OB_SUCC(ret) && i < server_info_arr_.count(); ++i) {
      const ObServerInfoInTable &server_info = server_info_arr_.at(i);
      const ObAddr &server = server_info.get_server();
      if ((server_info.get_zone() == zone || zone.is_empty()) && server_info.is_alive()) {
        if (OB_FAIL(server_list.push_back(server))) {
          LOG_WARN("fail to push an element into server_list", KR(ret), K(server));
        }
      }
    }
    if (OB_SUCC(ret)) {
      renew_time = renew_time_;
    }
  }
  return ret;
}

int ObServerTraceMap::get_alive_and_not_stopped_servers(const ObZone &zone, ObIArray<ObAddr> &server_list) const
{
  int ret = OB_SUCCESS;

  server_list.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("server trace map has not inited", KR(ret));
  } else {
    SpinRLockGuard guard(lock_);
    for (int64_t i = 0; OB_SUCC(ret) && i < server_info_arr_.count(); ++i) {
      const ObServerInfoInTable &server_info = server_info_arr_.at(i);
      const ObAddr &server = server_info.get_server();
      if ((server_info.get_zone() == zone || zone.is_empty())
           && !server_info.is_stopped()
           && server_info.is_alive()) {
        if (OB_FAIL(server_list.push_back(server))) {
          LOG_WARN("fail to push an element into server_list", KR(ret), K(server));
        }
      }
    }
  }

  return ret;
}

int ObServerTraceMap::check_server_active(const ObAddr &server, bool &is_active) const
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("server trace map has not inited", KR(ret));
  } else if (OB_UNLIKELY(!server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", KR(ret), K(server));
  } else {
    SpinRLockGuard guard(lock_);
    ObServerInfoInTable server_info;
    if (OB_FAIL(find_server_info(server, server_info))) {
      LOG_WARN("fail to find server info", KR(ret), K(server), K_(server_info_arr));
    } else {
      is_active = server_info.is_active();
    }
  }

  return ret;
}

int ObServerTraceMap::check_server_can_migrate_in(const ObAddr &server, bool &can_migrate_in) const
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("server trace map has not inited", KR(ret));
  } else if (OB_UNLIKELY(!server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", KR(ret), K(server));
  } else {
    SpinRLockGuard guard(lock_);
    ObServerInfoInTable server_info;
    if (OB_FAIL(find_server_info(server, server_info))) {
      LOG_WARN("fail to find server info", KR(ret), K(server), K_(server_info_arr));
    } else {
      can_migrate_in = server_info.can_migrate_in();
    }
  }

  return ret;
}

int ObServerTraceMap::get_alive_servers_count(const common::ObZone &zone, int64_t &count) const
{
  // empty zone means check if there exists stopped servers in the whole cluster
  int ret = OB_SUCCESS;
  ObArray<ObAddr> server_list;
  count = 0;
  if (OB_FAIL(get_alive_servers(zone, server_list))) {
    LOG_WARN("fail to get alive servers", KR(ret), K(zone));
  } else {
    count = server_list.count();
  }
  return ret;
}
int ObServerTraceMap::get_servers_by_status(
      const ObZone &zone,
      common::ObIArray<common::ObAddr> &alive_server_list,
      common::ObIArray<common::ObAddr> &not_alive_server_list) const
{
  int ret = OB_SUCCESS;
  alive_server_list.reset();
  not_alive_server_list.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("server trace map has not inited", KR(ret));
  } else {
    SpinRLockGuard guard(lock_);
    for (int64_t i = 0; OB_SUCC(ret) && i < server_info_arr_.count(); ++i) {
      const ObServerInfoInTable &server_info = server_info_arr_.at(i);
      if (server_info.get_zone() == zone || zone.is_empty()) {
        const ObAddr &server = server_info.get_server();
        if (server_info.is_alive()) {
          if (OB_FAIL(alive_server_list.push_back(server))) {
            LOG_WARN("fail to push back to alive_server_list", KR(ret), K(server));
          }
        } else if (OB_FAIL(not_alive_server_list.push_back(server))) {
          LOG_WARN("fail to push back to not_alive_server_list", KR(ret), K(server));
        }
      }
    }
  }
  return ret;
}

int ObServerTraceMap::get_min_server_version(
    char min_server_version[OB_SERVER_VERSION_LENGTH],
    uint64_t &min_observer_version)
{
  int ret = OB_SUCCESS;
  ObZone zone; // empty zone, get all server statuses
  ObArray<ObServerInfoInTable> servers_info;
  min_observer_version = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("server trace map has not inited", KR(ret));
  } else {
    SpinRLockGuard guard(lock_);
    if (OB_FAIL(servers_info.assign(server_info_arr_))) {
      LOG_WARN("fail to assign servers_info", KR(ret), K(server_info_arr_));
    } else if (OB_UNLIKELY(servers_info.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("servers_info is empty", KR(ret), K(servers_info), K(server_info_arr_));
    }
  }
  if (OB_SUCC(ret)) {
    ObClusterVersion version_parser;
    uint64_t cur_min_version = UINT64_MAX;
    for (int64_t i = 0; OB_SUCC(ret) && i < servers_info.count(); i++) {
      const ObServerInfoInTable::ObBuildVersion &build_version = servers_info.at(i).get_build_version();
      char *saveptr = NULL;
      char build_version_ptr[common::OB_SERVER_VERSION_LENGTH] = {0};
      MEMCPY(build_version_ptr, build_version.ptr(), OB_SERVER_VERSION_LENGTH);
      char *version = STRTOK_R(build_version_ptr, "_", &saveptr);
      if (OB_ISNULL(version) || OB_UNLIKELY(strlen(version) + 1 > OB_SERVER_VERSION_LENGTH)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid build version format", KR(ret), K(build_version_ptr));
      } else if (OB_FAIL(version_parser.refresh_cluster_version(version))) {
        LOG_WARN("failed to parse version", KR(ret), K(version));
      } else {
        if (version_parser.get_cluster_version() < cur_min_version) {
          size_t len = strlen(version);
          MEMCPY(min_server_version, version, len);
          min_server_version[len] = '\0';
          cur_min_version = version_parser.get_cluster_version();
          min_observer_version = cur_min_version;
        }
      }
      if (OB_SUCC(ret) && UINT64_MAX == cur_min_version) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("no valid server version found", KR(ret));
      }
    }
  }
  return ret;
}

int ObServerTraceMap::refresh(bool allow_broadcast /* = false */)
{
  int ret = OB_SUCCESS;
  ObArray<ObServerInfoInTable> servers_info;
  common::ObSEArray<common::ObZone, 4> inactive_zone_list;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerTraceMap has not been inited", KR(ret), K(is_inited_));
  } else if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.schema_service_ is null", KR(ret));
  } else if (!GCTX.schema_service_->is_tenant_refreshed(OB_SYS_TENANT_ID)) {
    ret = OB_NEED_RETRY;
    LOG_WARN("can not renew tenant map because sys tenant schema is not ready", KR(ret));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.sql_proxy_ is null", KR(ret), KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(ObServerTableOperator::get(*GCTX.sql_proxy_, servers_info))) {
    LOG_WARN("fail to get servers_info", KR(ret), KP(GCTX.sql_proxy_));
  } else if (OB_UNLIKELY(servers_info.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("read nothing in table", KR(ret), K(servers_info));
  } else if (OB_FAIL(ObZoneTableOperation::get_inactive_zone_list(*GCTX.sql_proxy_, inactive_zone_list))) {
    LOG_WARN("failed to get inactive zone list", K(ret));
  } else {
    {
      SpinWLockGuard guard(lock_);
      // reuse memory
      server_info_arr_.reuse();
      inactive_zone_list_.reuse();
      if (OB_FAIL(inactive_zone_list_.assign(inactive_zone_list))) {
        LOG_WARN("failed to assign inactive zone list", K(ret));
      }
      // can not use ObArray's assign, which will reallocate memory
      for (int64_t i = 0; i < servers_info.count() && OB_SUCC(ret); ++i) {
        const ObServerInfoInTable &server_info_i = servers_info.at(i);
        if (OB_FAIL(server_info_arr_.push_back(server_info_i))) {
          LOG_WARN("fail to push back", K(server_info_i), KR(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FALSE_IT(renew_time_ = ObTimeUtility::current_time())) {
      } else if (!has_build_) {
        has_build_ = true;
        FLOG_INFO("server tracer has built", KR(ret), K(has_build_), K(renew_time_), K(server_info_arr_));
      }
    }

    if (OB_SUCC(ret) && allow_broadcast) {
      if (OB_FAIL(broadcast_server_trace_())) {
        LOG_WARN("failed to broadcast server trace ",KR(ret));
      }
    }
  }
  return ret;
}

ObTenantServersCacheMap::ObTenantServersCacheMap()
    : is_inited_(false),
      tenant_map_(),
      ut_operator_(),
      last_cache_clear_ts_(0)
{
}

ObTenantServersCacheMap::~ObTenantServersCacheMap()
{
}

int ObTenantServersCacheMap::init()
{
  int ret = OB_SUCCESS;
  const int64_t BUCKET_NUM = 512;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTenantServersCacheMap has already been inited", K(ret));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the GCTX.sql_proxy_ is null", KR(ret));
  } else if (OB_FAIL(ut_operator_.init(*GCTX.sql_proxy_))) {
    LOG_WARN("failed to init ObUnitTableOperator", KR(ret));
  } else if (OB_FAIL(tenant_map_.create(BUCKET_NUM,
        SET_USE_500("TntSrvCaMap"), SET_USE_500("TntSrvCaMap")))){
    LOG_WARN("failed to create tenant map", KR(ret));
  } else {
    last_cache_clear_ts_ = ObTimeUtility::current_time();
    is_inited_ = true;
  }
  return ret;
}

ERRSIM_POINT_DEF(ERRSIM_GET_TENANT_MAP_ERROR)
int ObTenantServersCacheMap::get_alive_tenant_servers(
    const uint64_t tenant_id,
    common::ObIArray<common::ObAddr> &alive_servers,
    int64_t &renew_time) const
{
  int ret = OB_SUCCESS;
  alive_servers.reset();
  renew_time = 0;
  ObTenantServers tenant_servers;
  uint64_t valid_tnt_id = is_meta_tenant(tenant_id) ? gen_user_tenant_id(tenant_id) : tenant_id;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantServersCacheMap has not been initialized", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(valid_tnt_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("valid_tnt_id is invalid", KR(ret), K(valid_tnt_id), K(tenant_id));
  } else if (OB_UNLIKELY(ERRSIM_GET_TENANT_MAP_ERROR != OB_SUCCESS)) {
    LOG_WARN("ERRSIM_GET_TENANT_MAP_ERROR", K(ret));
  } else if (OB_FAIL(tenant_map_.get_refactored(valid_tnt_id, tenant_servers))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get from tenant_map_", KR(ret), K(valid_tnt_id));
    }
  } else if (OB_FALSE_IT(renew_time = tenant_servers.get_renew_time())) {
  } else {
    bool is_alive = false;
    const ObArray<ObAddr> &all_servers = tenant_servers.get_servers();
    ARRAY_FOREACH(all_servers, idx) {
      if (OB_FAIL(SVR_TRACER.check_server_alive(all_servers.at(idx), is_alive))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          // skip this server and continue judging
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to check_server_alive", KR(ret), "server", all_servers.at(idx));
        }
      } else if (is_alive) {
        if (OB_FAIL(alive_servers.push_back(all_servers.at(idx)))) {
          LOG_WARN("push back failed", KR(ret), "server", all_servers.at(idx));
        }
      }
    }
  }
  return ret;
}

int ObTenantServersCacheMap::set_tenant_servers_cache_(
    const ObTenantServers &tenant_servers)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantServersCacheMap has not been initialized", KR(ret));
  } else if (OB_UNLIKELY(!tenant_servers.is_valid()
        || tenant_servers.get_renew_time() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("the input parameter is invalid", KR(ret),
        K(tenant_servers), "renew_time", tenant_servers.get_renew_time());
  } else if (OB_UNLIKELY(is_meta_tenant(tenant_servers.get_tenant_id()))) {
    // defensive check. The previous step ensures that tenant_servers should not be meta tenants.
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("this tenant_servers should not belong to meta tenants", KR(ret), K(tenant_servers));
  } else {
    ObTenantServersCacheUpdater updater(tenant_servers);
    if (OB_FAIL(tenant_map_.set_or_update(tenant_servers.get_tenant_id(), tenant_servers, updater))) {
      LOG_WARN("failed to set_or_update", KR(ret), K(tenant_servers));
    }
  }
  return ret;
}

int ObTenantServersCacheMap::clear_expired_tenent_servers_cache_()
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> dropped_tenant_ids;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantServersCacheMap has not been initialized", KR(ret));
  } else if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.schema_service_ is null", KR(ret));
  } else if (!GCTX.schema_service_->is_tenant_refreshed(OB_SYS_TENANT_ID)) {
    ret = OB_NEED_RETRY;
    LOG_WARN("can not clear expiered cache because sys tenant schema is not ready", KR(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_dropped_tenant_ids(dropped_tenant_ids))) {
    LOG_WARN("failed to get tenant ids", KR(ret));
  } else {
    ARRAY_FOREACH(dropped_tenant_ids, idx) {
      if (is_meta_tenant(dropped_tenant_ids.at(idx))) {
        // skip
      } else if (OB_FAIL(tenant_map_.erase_refactored(dropped_tenant_ids.at(idx)))) {
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to delete tenant_servers_cache from tenant_map", K(ret),
              "tenant_id", dropped_tenant_ids.at(idx));
        }
      }
    }
  }
  return ret;
}

int ObTenantServersCacheMap::renew_tenant_map()
{
  int ret = OB_SUCCESS;
  ObArray<share::ObTenantServers> all_tenant_servers;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantServersCacheMap has not been initialized", KR(ret));
  } else if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.schema_service_ is null", KR(ret));
  } else if (!GCTX.schema_service_->is_tenant_refreshed(OB_SYS_TENANT_ID)) {
    ret = OB_NEED_RETRY;
    LOG_WARN("can not renew tenant map because sys tenant schema is not ready", KR(ret));
  } else if (OB_FAIL(ut_operator_.get_tenant_servers(all_tenant_servers))) {
    LOG_WARN("failed to get tenant servers information in batches", KR(ret));
  } else if (OB_UNLIKELY(all_tenant_servers.count() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the tenant_servers should be greater than 0", KR(ret));
  } else {
    ARRAY_FOREACH(all_tenant_servers, idx) {
      if (OB_FAIL(set_tenant_servers_cache_(all_tenant_servers.at(idx)))) {
        LOG_WARN("set_tenant_location failed", KR(ret),
            "tenant_servers", all_tenant_servers.at(idx),
            "renew_time", all_tenant_servers.at(idx).get_renew_time());
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (ObTimeUtil::current_time() - last_cache_clear_ts_ > CLEAR_CACHE_INTERVAL) {
    if (OB_FAIL(clear_expired_tenent_servers_cache_())) {
      LOG_WARN("failed to clear expired cache", KR(ret));
    } else {
      last_cache_clear_ts_ = ObTimeUtil::current_time();
    }
  }
  return ret;
}

int ObTenantServersCacheMap::renew_tenant_map_by_id(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObArray<share::ObTenantServers> all_tenant_servers;
  bool has_been_dropped = false;
  uint64_t valid_tnt_id = is_meta_tenant(tenant_id) ? gen_user_tenant_id(tenant_id) : tenant_id;
  if (OB_UNLIKELY(!is_inited_)) {
    LOG_WARN("ObTenantServersCacheMap has not been initialized", KR(ret));
  } else if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.schema_service_ is null", KR(ret));
  } else if (OB_FAIL(GCTX.schema_service_->check_if_tenant_has_been_dropped(tenant_id, has_been_dropped))) {
    LOG_WARN("failed to check_if_tenant_has_been_dropped", KR(ret), K(tenant_id));
  } else if (has_been_dropped) {
    // skip
  } else if (OB_UNLIKELY(!is_valid_tenant_id(valid_tnt_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("the tenant_id is invalid", KR(ret), K(valid_tnt_id), K(tenant_id));
  } else if (OB_FAIL(ut_operator_.get_tenant_servers(all_tenant_servers, valid_tnt_id))) {
    LOG_WARN("failed to get_tenant_servers", KR(ret), K(valid_tnt_id), K(tenant_id));
  } else if (OB_UNLIKELY(all_tenant_servers.count() != 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the number of tenant_servers of this tenant is not one", KR(ret), K(tenant_id), K(all_tenant_servers));
  } else if (OB_FAIL(set_tenant_servers_cache_(all_tenant_servers.at(0)))) {
    LOG_WARN("set_tenant_location failed", KR(ret), "tenant_servers by tenant_id", all_tenant_servers.at(0));
  }
  return ret;
}

void ObTenantServersCacheUpdater::operator()(
    common::hash::HashMapPair<
    uint64_t,
    ObTenantServers> &entry)
{
  int ret = OB_SUCCESS;
  if (update_value_.get_renew_time() > entry.second.get_renew_time()) {
    if (OB_FAIL(entry.second.assign(update_value_))) {
      LOG_WARN("assign failed", KR(ret), K(update_value_));
    }
  } else {
    LOG_TRACE("ObTenantServers is not the latest",
        "already_inserted_renew_time", entry.second.get_renew_time(),
        "desired_renew_time", update_value_.get_renew_time(),
        "desired_tenant_id", update_value_.get_tenant_id());
  }
}

ObServerTraceTask::ObServerTraceTask()
    : tracer_(nullptr), is_inited_(false)
{
}

ObServerTraceTask::~ObServerTraceTask()
{
}

int ObServerTraceTask::init(ObAllServerTracer *tracer, int tg_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObServerTraceTask has already been inited", K(ret));
  } else if (OB_ISNULL(tracer)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(tracer));
  } else if (OB_FAIL(TG_SCHEDULE(tg_id, *this,
                                 GCONF.virtual_table_location_cache_expire_time,
                                 true /*repeat schedule*/))) {
    LOG_WARN("fail to schedule timer", K(ret));
  } else {
    tg_id_ = tg_id;
    tracer_ = tracer;
    is_inited_ = true;
  }
  return ret;
}

void ObServerTraceTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerTraceTask has not been inited", KR(ret));
  } else if (OB_ISNULL(GCTX.schema_service_) || OB_ISNULL(tracer_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.schema_service_ or tracer_ is null", KR(ret));
  } else if (!GCTX.schema_service_->is_tenant_refreshed(OB_SYS_TENANT_ID)) {
    ret = OB_NEED_RETRY;
    LOG_WARN("sys tenant schema is not ready", KR(ret));
  } else {
    // the following operations all ignore error codes
    // to ensure that they do not interfere with each other

    // this operation will refresh all servers to the cache
    if (OB_TMP_FAIL(tracer_->refresh())) {
      LOG_WARN("fail to refresh all server map", KR(tmp_ret));
    }
    // this operation will refresh all tenant servers into the cache.
    if (OB_TMP_FAIL(tracer_->renew_tenant_servers_cache_map())) {
      // overwrite tmp_ret
      LOG_WARN("failed to renew tenant servers map", KR(tmp_ret));
    }
  }
}

ObAllServerTracer::ObAllServerTracer()
    : is_inited_(false), trace_map_(), tenant_servers_cache_map_()
{
}

ObAllServerTracer::~ObAllServerTracer()
{
}

ObAllServerTracer &ObAllServerTracer::get_instance()
{
  static ObAllServerTracer instance;
  return instance;
}

int ObAllServerTracer::init(int tg_id, ObServerTraceTask &trace_task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObAllServerTracer has already been inited", K(ret));
  } else if (OB_FAIL(trace_map_.init())) {
    LOG_WARN("fail to init trace map", K(ret));
  } else if (OB_FAIL(trace_task.init(this, tg_id))) {
    LOG_WARN("fail to init trace task", K(ret));
  } else if (OB_FAIL(tenant_servers_cache_map_.init())) {
    LOG_WARN("fail to init tenant servers cache map", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObAllServerTracer::is_server_exist(const common::ObAddr &server, bool &exist) const
{
  return trace_map_.is_server_exist(server, exist);
}

int ObAllServerTracer::get_server_rpc_port(const common::ObAddr &server, const int64_t sql_port,
                                                  int64_t &rpc_port, bool &exist) const
{
  return trace_map_.get_server_rpc_port(server, sql_port, rpc_port, exist);
}

int ObAllServerTracer::check_server_alive(const ObAddr &server, bool &is_alive) const
{
  return trace_map_.check_server_alive(server, is_alive);
}

int ObAllServerTracer::check_in_service(const ObAddr &addr, bool &service_started) const
{
  return trace_map_.check_in_service(addr, service_started);
}

int ObAllServerTracer::check_server_permanent_offline(const ObAddr &addr, bool &is_offline) const
{
  return trace_map_.check_server_permanent_offline(addr, is_offline);
}

int ObAllServerTracer::is_server_stopped(const ObAddr &addr, bool &is_stopped) const
{
  return trace_map_.is_server_stopped(addr, is_stopped);
}
int ObAllServerTracer::check_migrate_in_blocked(const ObAddr &addr, bool &is_block) const
{
  return trace_map_.check_migrate_in_blocked(addr, is_block);
}

int ObAllServerTracer::for_each_server_info(
  const ObFunction<int(const ObServerInfoInTable &server_info)> &functor)
{
  return trace_map_.for_each_server_info(functor);
}

int ObAllServerTracer::get_server_zone(const ObAddr &server, ObZone &zone) const
{
  return trace_map_.get_server_zone(server, zone);
}

int ObAllServerTracer::get_servers_of_zone(
    const ObZone &zone,
    ObIArray<ObAddr> &servers) const
{
  // empty zone means that get all servers
  return trace_map_.get_servers_of_zone(zone, servers);
}

int ObAllServerTracer::get_servers_of_zone(
      const common::ObZone &zone,
      common::ObIArray<common::ObAddr> &servers,
      common::ObIArray<uint64_t> &server_id_list) const
{
  // empty zone means that get all servers
  return trace_map_.get_servers_of_zone(zone, servers, server_id_list);
}

int ObAllServerTracer::get_server_info(
    const common::ObAddr &server,
    ObServerInfoInTable &server_info) const
{
  return trace_map_.get_server_info(server, server_info);
}

int ObAllServerTracer::get_servers_info(
    const common::ObZone &zone,
    common::ObIArray<ObServerInfoInTable> &servers_info,
    bool include_permanent_offline) const
{
  return trace_map_.get_servers_info(zone, servers_info, include_permanent_offline);
}

int ObAllServerTracer::get_active_servers_info(
      const common::ObZone &zone,
      common::ObIArray<ObServerInfoInTable> &active_servers_info) const
{
  return trace_map_.get_active_servers_info(zone, active_servers_info);
}

int ObAllServerTracer::get_alive_servers(const ObZone &zone, ObIArray<ObAddr> &server_list) const
{
  return trace_map_.get_alive_servers(zone, server_list);
}

int ObAllServerTracer::get_alive_servers_with_renew_time(
    const common::ObZone &zone,
    common::ObIArray<common::ObAddr> &server_list,
    int64_t &renew_time) const
{
  return trace_map_.get_alive_servers_with_renew_time(zone, server_list, renew_time);
}

int ObAllServerTracer::check_server_active(const ObAddr &server, bool &is_active) const
{
  return trace_map_.check_server_active(server, is_active);
}

int ObAllServerTracer::check_server_can_migrate_in(const ObAddr &server, bool &can_migrate_in) const
{
  return trace_map_.check_server_can_migrate_in(server, can_migrate_in);
}

int ObAllServerTracer::get_alive_servers_count(const common::ObZone &zone, int64_t &count) const
{
  return trace_map_.get_alive_servers_count(zone, count);
}

int ObAllServerTracer::get_alive_and_not_stopped_servers(const ObZone &zone, ObIArray<ObAddr> &server_list) const
{
  return trace_map_.get_alive_and_not_stopped_servers(zone, server_list);
}

int ObAllServerTracer::refresh(bool allow_broadcast /* = false */)
{
  return trace_map_.refresh(allow_broadcast);
}

int ObAllServerTracer::get_servers_by_status(
      const ObZone &zone,
      common::ObIArray<common::ObAddr> &alive_server_list,
      common::ObIArray<common::ObAddr> &not_alive_server_list) const
{
  return trace_map_.get_servers_by_status(zone, alive_server_list, not_alive_server_list);
}

int ObAllServerTracer::get_min_server_version(
    char min_server_version[OB_SERVER_VERSION_LENGTH],
    uint64_t &min_observer_version)
{
  return trace_map_.get_min_server_version(min_server_version, min_observer_version);
}

bool ObAllServerTracer::has_build() const
{
  return trace_map_.has_build();
}

int ObAllServerTracer::get_alive_tenant_servers(
    const uint64_t tenant_id,
    common::ObIArray<common::ObAddr> &servers,
    int64_t &renew_time) const
{
  return tenant_servers_cache_map_.get_alive_tenant_servers(tenant_id, servers, renew_time);
}

int ObAllServerTracer::renew_tenant_servers_cache_map()
{
  return tenant_servers_cache_map_.renew_tenant_map();
}

int ObAllServerTracer::renew_tenant_servers_cache_by_id(const uint64_t tenant_id)
{
  return tenant_servers_cache_map_.renew_tenant_map_by_id(tenant_id);
}
