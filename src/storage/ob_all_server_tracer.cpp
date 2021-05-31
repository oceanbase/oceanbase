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

#define USING_LOG_PREFIX STORAGE
#include "ob_all_server_tracer.h"
#include "lib/thread/thread_mgr.h"
#include "observer/ob_server_struct.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::storage;

ObServerTraceMap::ObServerTraceMap() : is_inited_(false), lock_(), servers_()
{}

ObServerTraceMap::~ObServerTraceMap()
{}

int ObServerTraceMap::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObServerTraceMap has already been inited", K(ret));
  } else {
    SpinWLockGuard guard(lock_);
    if (OB_FAIL(servers_.create(DEFAULT_SERVER_COUNT))) {
      LOG_WARN("fail to create server hashset", K(ret));
    } else if (OB_FAIL(server_table_operator_.init(GCTX.sql_proxy_))) {
      LOG_WARN("fail to init server table operator", K(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObServerTraceMap::is_server_exist(const common::ObAddr& server, bool& exist) const
{
  int ret = OB_SUCCESS;
  exist = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerTraceMap has not been inited", K(ret));
  } else {
    SpinRLockGuard guard(lock_);
    if (OB_FAIL(servers_.exist_refactored(server))) {
      if (OB_HASH_EXIST == ret || OB_HASH_NOT_EXIST == ret) {
        exist = (OB_HASH_EXIST == ret) || (0 == servers_.size());
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to check server exist", K(ret), K(server));
      }
    }
  }
  return ret;
}

int ObServerTraceMap::check_server_alive(const ObAddr& server, bool& is_alive) const
{
  int ret = OB_NOT_SUPPORTED;
  UNUSED(server);
  UNUSED(is_alive);
  return ret;
}

int ObServerTraceMap::check_in_service(const ObAddr& addr, bool& service_started) const
{
  int ret = OB_NOT_SUPPORTED;
  UNUSED(addr);
  UNUSED(service_started);
  return ret;
}

int ObServerTraceMap::check_server_permanent_offline(const ObAddr& addr, bool& is_offline) const
{
  int ret = OB_NOT_SUPPORTED;
  UNUSED(addr);
  UNUSED(is_offline);
  return ret;
}

int ObServerTraceMap::is_server_stopped(const ObAddr& addr, bool& is_stopped) const
{
  int ret = OB_NOT_SUPPORTED;
  UNUSED(addr);
  UNUSED(is_stopped);
  return ret;
}

int ObServerTraceMap::check_migrate_in_blocked(const ObAddr& addr, bool& is_block) const
{
  int ret = OB_NOT_SUPPORTED;
  UNUSED(addr);
  UNUSED(is_block);
  return ret;
}
int ObServerTraceMap::refresh()
{
  int ret = OB_SUCCESS;
  ObArray<ObServerStatus> server_statuses;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerTraceMap has not been inited", K(ret));
  } else if (OB_FAIL(server_table_operator_.get(server_statuses))) {
    LOG_WARN("fail to get server status", K(ret));
  } else {
    ObArray<ObAddr> server_list;
    SpinWLockGuard guard(lock_);
    for (int64_t i = 0; OB_SUCC(ret) && i < server_statuses.count(); ++i) {
      if (OB_FAIL(server_list.push_back(server_statuses.at(i).server_))) {
        LOG_WARN("fail to push back server", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < server_list.count(); ++i) {
      ret = servers_.exist_refactored(server_list.at(i));
      if (OB_HASH_EXIST == ret) {
        ret = OB_SUCCESS;
      } else if (OB_HASH_NOT_EXIST == ret) {
        if (OB_FAIL(servers_.set_refactored(server_list.at(i)))) {
          LOG_WARN("fail to add server to all server set", K(ret), "server", server_list.at(i));
        } else {
          LOG_INFO("succ to add server to all server set", K(ret), "server", server_list.at(i), K(server_list));
        }
      } else {
        LOG_WARN("fail to check server exist", K(ret));
      }
    }
    if (OB_SUCC(ret) && servers_.size() > server_list.count()) {
      ObArray<ObAddr> remove_set;
      FOREACH_X(s, servers_, OB_SUCC(ret))
      {
        if (!has_exist_in_array(server_list, s->first)) {
          if (OB_FAIL(remove_set.push_back(s->first))) {
            LOG_WARN("fail to push back server", K(ret), "server", s->first);
          }
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < remove_set.count(); ++i) {
        if (OB_FAIL(servers_.erase_refactored(remove_set.at(i)))) {
          LOG_WARN("fail to earse from all server set", K(ret), "server", remove_set.at(i));
        } else {
          LOG_INFO("remove server from all server set", "server", remove_set.at(i));
        }
      }
    }
  }
  return ret;
}

ObServerTraceTask::ObServerTraceTask() : trace_map_(NULL), is_inited_(false)
{}

ObServerTraceTask::~ObServerTraceTask()
{}

int ObServerTraceTask::init(ObServerTraceMap* trace_map, int tg_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObServerTraceTask has already been inited", K(ret));
  } else if (OB_ISNULL(trace_map)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(trace_map));
  } else if (OB_FAIL(TG_SCHEDULE(tg_id, *this, REFRESH_INTERVAL_US, true /*repeat schedule*/))) {
    LOG_WARN("fail to schedule timer", K(ret));
  } else {
    trace_map_ = trace_map;
    is_inited_ = true;
  }
  return ret;
}

void ObServerTraceTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerTraceTask has not been inited", K(ret));
  } else if (OB_FAIL(trace_map_->refresh())) {
    LOG_WARN("fail to refresh all server map", K(ret));
  }
}

ObAllServerTracer::ObAllServerTracer() : is_inited_(false), trace_map_()
{}

ObAllServerTracer::~ObAllServerTracer()
{}

ObAllServerTracer& ObAllServerTracer::get_instance()
{
  static ObAllServerTracer instance;
  return instance;
}

int ObAllServerTracer::init(int tg_id, ObServerTraceTask& trace_task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObAllServerTracer has already been inited", K(ret));
  } else if (OB_FAIL(trace_map_.init())) {
    LOG_WARN("fail to init trace map", K(ret));
  } else if (OB_FAIL(trace_task.init(&trace_map_, tg_id))) {
    LOG_WARN("fail to init trace task", K(ret));
  }
  return ret;
}

int ObAllServerTracer::is_server_exist(const common::ObAddr& server, bool& exist) const
{
  return trace_map_.is_server_exist(server, exist);
}

int ObAllServerTracer::check_server_alive(const ObAddr& server, bool& is_alive) const
{
  return trace_map_.check_server_alive(server, is_alive);
}

int ObAllServerTracer::check_in_service(const ObAddr& addr, bool& service_started) const
{
  return trace_map_.check_in_service(addr, service_started);
}

int ObAllServerTracer::check_server_permanent_offline(const ObAddr& addr, bool& is_offline) const
{
  return trace_map_.check_server_permanent_offline(addr, is_offline);
}

int ObAllServerTracer::is_server_stopped(const ObAddr& addr, bool& is_stopped) const
{
  return trace_map_.is_server_stopped(addr, is_stopped);
}
int ObAllServerTracer::check_migrate_in_blocked(const ObAddr& addr, bool& is_block) const
{
  return trace_map_.check_migrate_in_blocked(addr, is_block);
}
