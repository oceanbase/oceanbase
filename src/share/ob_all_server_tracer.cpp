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
#include "lib/thread/thread_mgr.h"
#include "observer/ob_server_struct.h"

using namespace oceanbase::common;
using namespace oceanbase::share;

ObServerTraceMap::ObServerTraceMap()
  : is_inited_(false), lock_(ObLatchIds::ALL_SERVER_TRACER_LOCK), server_status_arr_()
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
    int64_t block_size = (DEFAULT_SERVER_COUNT * sizeof(ObServerStatus));
    server_status_arr_.set_block_size(block_size);
    if (OB_FAIL(server_status_arr_.reserve(DEFAULT_SERVER_COUNT))) {
      LOG_WARN("fail to reserve server status array", KR(ret));
    } else if (OB_FAIL(server_table_operator_.init(GCTX.sql_proxy_))) {
      LOG_WARN("fail to init server table operator", KR(ret));
    } else {
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
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), K(ret));
  } else {
    SpinRLockGuard guard(lock_);
    ObServerStatus status;
    if (OB_FAIL(find_server_status(server, status))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        exist = false;
      } else {
        LOG_WARN("fail to find server status", K(server), K_(server_status_arr), KR(ret));
      }
    } else {
      exist = true;
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
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), K(ret));
  } else {
    SpinRLockGuard guard(lock_);
    ObServerStatus status;
    if (OB_FAIL(find_server_status(server, status))) {
      LOG_WARN("fail to find server status", K(server), K_(server_status_arr), KR(ret));
    } else {
      is_alive = status.is_alive();
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
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), K(ret));
  } else {
    SpinRLockGuard guard(lock_);
    ObServerStatus status;
    if (OB_FAIL(find_server_status(server, status))) {
      LOG_WARN("fail to find server status", K(server), K_(server_status_arr), KR(ret));
    } else {
      service_started = status.in_service();
    }
  }

  return ret;
}

int ObServerTraceMap::check_server_permanent_offline(const ObAddr &addr, bool &is_offline) const
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);

  ObServerStatus status;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(find_server_status(addr, status))) {
    LOG_WARN("fail to find server status", K(addr), K_(server_status_arr), KR(ret));
  } else {
    is_offline = status.is_permanent_offline();
  }

  if ((OB_ENTRY_NOT_EXIST == ret) && server_status_arr_.empty()) {
    // if server list is empty, treat as not offline
    ret = OB_SUCCESS;
    is_offline = false;
  }

  return ret;
}

int ObServerTraceMap::for_each_server_status(
  const ObFunction<int(const ObServerStatus &status)> &functor)
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);

  ObServerStatus status;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < server_status_arr_.count(); i++) {
      ObServerStatus &status = server_status_arr_[i];
      if (OB_UNLIKELY(!functor.is_valid())) {
        ret = OB_EAGAIN;
        LOG_WARN("functor is invalid");
      } else if (OB_FAIL(functor(status))) {
        LOG_WARN("invoke functor failed", K(ret), K(status));
      }
    }
  }

  return ret;
}

int ObServerTraceMap::find_server_status(const ObAddr &addr, ObServerStatus &status) const
{
  int ret = OB_SUCCESS;
  bool found = false;
  for (int64_t i = 0; (i < server_status_arr_.count()) && !found; i++) {
    if (server_status_arr_[i].server_ == addr) {
      status = server_status_arr_[i];
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
  int ret = OB_NOT_SUPPORTED;
  UNUSED(addr);
  UNUSED(is_stopped);
  return ret;
}

int ObServerTraceMap::check_migrate_in_blocked(const ObAddr &addr, bool &is_block) const
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
    SpinWLockGuard guard(lock_);
    // reuse memory
    server_status_arr_.reuse();

    // can not use ObArray's assign, which will reallocate memory
    for (int64_t i = 0; (i < server_statuses.count()) && OB_SUCC(ret); ++i) {
      if (OB_FAIL(server_status_arr_.push_back(server_statuses[i]))) {
        LOG_WARN("fail to push back", K(server_statuses[i]), K(i), KR(ret));
      }
    }
  }
  return ret;
}

ObServerTraceTask::ObServerTraceTask()
  : trace_map_(NULL), is_inited_(false)
{
}

ObServerTraceTask::~ObServerTraceTask()
{
}

int ObServerTraceTask::init(ObServerTraceMap *trace_map, int tg_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObServerTraceTask has already been inited", K(ret));
  } else if (OB_ISNULL(trace_map)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(trace_map));
  } else if (OB_FAIL(TG_SCHEDULE(tg_id, *this, REFRESH_INTERVAL_US,
                                 true /*repeat schedule*/))) {
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

ObAllServerTracer::ObAllServerTracer()
  : is_inited_(false), trace_map_()
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
  } else if (OB_FAIL(trace_task.init(&trace_map_, tg_id))) {
    LOG_WARN("fail to init trace task", K(ret));
  }
  return ret;
}

int ObAllServerTracer::is_server_exist(const common::ObAddr &server, bool &exist) const
{
  return trace_map_.is_server_exist(server, exist);
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

int ObAllServerTracer::for_each_server_status(
  const ObFunction<int(const ObServerStatus &status)> &functor)
{
  return trace_map_.for_each_server_status(functor);
}
