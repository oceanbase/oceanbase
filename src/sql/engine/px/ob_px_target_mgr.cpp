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

#define USING_LOG_PREFIX  SQL_ENG
#include "ob_px_target_mgr.h"
#include "observer/omt/ob_multi_tenant.h"

namespace oceanbase
{
using namespace common;
using namespace share;

namespace sql
{

ObPxTargetMgr &ObPxTargetMgr::get_instance()
{
  static ObPxTargetMgr px_res_mgr;
  return px_res_mgr;
}

int ObPxTargetMgr::init(const common::ObAddr &server,
                        ObIAliveServerTracer &server_tracer)
{
  int ret = OB_SUCCESS;
  auto attr = SET_USE_500("PxResMgr");
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObPxTargetMgr inited twice", K(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(server));
  } else if (OB_FAIL(px_info_map_.init(attr))) {
    LOG_WARN("px_info_map_ init failed", K(ret));
  } else if (OB_FAIL(alive_server_set_.create(PX_MAX_ALIVE_SERVER_NUM, attr))) {
    LOG_WARN("create alive_server_set_ failed", K(ret));
  } else {
    server_ = server;
    server_tracer_ = &server_tracer;
    is_inited_ = true;
    LOG_INFO("ObPxTargetMgr inited success", K(server_));
  }
  return ret;
}

void ObPxTargetMgr::reset()
{
  is_inited_ = false;
  is_running_ = false;
  server_.reset();
  server_tracer_ = NULL;
  px_info_map_.reset();
  alive_server_set_.clear();
  LOG_INFO("ObPxTargetMgr reset success", K(server_));
}

int ObPxTargetMgr::start()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPxTargetMgr is not inited", K(ret));
  } else if (is_running_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObPxTargetMgr is already running", K(ret));
  } else if (OB_FAIL(share::ObThreadPool::start())) {
    LOG_WARN("PX global resource manager refresh worker thread start error", K(ret));
  } else {
    is_running_ = true;
    LOG_INFO("ObPxTargetMgr start success");
  }
  return ret;
}

void ObPxTargetMgr::stop()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPxTargetMgr is not inited", K(ret));
  } else {
    (void)share::ObThreadPool::stop();
    is_running_ = false;
    LOG_INFO("ObPxTargetMgr stop success");
  }
}

void ObPxTargetMgr::wait()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPxTargetMgr is not inited", K(ret));
  } else if (is_running_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObPxTargetMgr is running", K(ret));
  } else {
    (void)share::ObThreadPool::wait();
    LOG_INFO("ObPxTargetMgr wait success");
  }
}

void ObPxTargetMgr::destroy()
{
  if (is_inited_) {
    if (is_running_) {
      stop();
      wait();
    }
    is_inited_ = false;
    LOG_INFO("ObPxTargetMgr destroyed");
  }
}

void ObPxTargetMgr::run1()
{
  int ret = OB_SUCCESS;
  uint64_t refresh_times = 0;
  ObPxResRefreshFunctor px_res_refresh_funtor;

  lib::set_thread_name("PxTargetMgr", get_thread_idx());
  while (!has_set_stop()) {
    // sleep 100 * 1000 us
    ob_usleep(PX_REFRESH_TARGET_INTERVEL_US);
    refresh_times++;

    px_info_map_.for_each(px_res_refresh_funtor);
    px_res_refresh_funtor.set_need_refresh_all(false);

    // check alive is a very slow and not necessary oper
    if (refresh_times % 100 == 0) {
      for (hash::ObHashSet<ObAddr>::const_iterator it = alive_server_set_.begin();
          OB_SUCC(ret) && it != alive_server_set_.end(); it++) {
        bool alive = true;
        int64_t trace_time;
        if (OB_FAIL(server_tracer_->is_alive(it->first, alive, trace_time))) {
          LOG_WARN("check server alive failed", K(ret), K(it->first));
          // ignore ret
          ret = OB_SUCCESS;
        } else if (!alive) {
          // TODO: it's not very good, maybe not all tenant in this server
          px_res_refresh_funtor.set_need_refresh_all(true);
          LOG_INFO("found a server is not longer alive, so refresh all", K(it->first));
        }
      }
    }
  }
}

int ObPxTargetMgr::add_tenant(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  void *ptr = NULL;
  ObTimeGuard timeguard("add px tenant");
  ObMemAttr memattr(tenant_id, "PxResMgr");
  ObPxTenantInfo tenant_info(tenant_id);
  ObPxResInfo *px_res_info = NULL;

  if (OB_SYS_TENANT_ID != tenant_id && OB_MAX_RESERVED_TENANT_ID >= tenant_id) {
    // 除系统租户外, 内部租户不分配 px 线程
  } else if (!is_inited_) {
    LOG_ERROR("px target mgr not inited");
  } else if (OB_FAIL(px_info_map_.contains_key(tenant_info))) {
    if (OB_ENTRY_EXIST == ret) {
      ret = OB_SUCCESS;
    } else if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("check contains key in map failed", K(ret), K(tenant_id));
    } else {
      if (OB_ISNULL(ptr = ob_malloc(sizeof(ObPxResInfo), memattr))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret), K(tenant_id));
      } else {
        if (OB_ISNULL(px_res_info = new (ptr) ObPxResInfo())) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("px resouce info construct failed", K(ret), KP(ptr));
        } else {
          ObPxTenantTargetMonitor *target_monoitor = px_res_info->get_target_monitor();
          if (OB_FAIL(target_monoitor->init(tenant_id, server_))) {
            LOG_WARN("px monitor init failed", K(ret), K(tenant_id), K(server_), K(lbt()));
          } else if (OB_FAIL(px_info_map_.insert_and_get(tenant_info, px_res_info))) {
            LOG_WARN("wait queue hashmap insert failed", K(ret), K(tenant_id), KP(px_res_info));
          } else {
            px_info_map_.revert(px_res_info);
            LOG_INFO("px res info add tenant success", K(tenant_id), K(server_), K(timeguard), K(lbt()));
          }
        }
        if (OB_FAIL(ret)) {
          px_res_info->~ObPxResInfo();
          ob_free(px_res_info);
          px_res_info = NULL;
        }
      }
    }
  }
  return ret;
}

int ObPxTargetMgr::delete_tenant(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObPxTenantInfo tenant_info(tenant_id);
  if (OB_SYS_TENANT_ID != tenant_id && OB_MAX_RESERVED_TENANT_ID >= tenant_id) {
    // 除系统租户外, 内部租户不分配 px 线程
  } else {
    ObPxResInfo *res_info = NULL;
    if (OB_FAIL(px_info_map_.del(tenant_info))) {
      LOG_WARN("delete tenant from hashmap failed", K(ret), K(tenant_id));
    }
    if (OB_SUCCESS == ret) {
      // ignore ret
      /*if (OB_LIKELY(tenant_id < TS_SOURCE_INFO_CACHE_NUM)) {
        lock_.wrlock();
        lock_.wrunlock();
      }*/
    }
  }

  if (OB_SUCCESS == ret) {
    LOG_INFO("delete tenant success", K(tenant_id));
  } else {
    LOG_WARN("delete tenant failed", K(ret), K(tenant_id));
  }
  return ret;
}

#define GET_TARGET_MONITOR(tenant_id, stmt)                      \
  ObPxTenantTargetMonitor *target_monitor = NULL;                \
  ObPxTenantInfo tenant_info(tenant_id);                         \
  ObPxResInfo *res_info = NULL;                                  \
  if (OB_FAIL(px_info_map_.get(tenant_info, res_info))) {        \
    LOG_WARN("get res_info failed", K(ret), K(tenant_id));       \
  } else if (OB_ISNULL(res_info)) {                              \
    ret = OB_ERR_UNEXPECTED;                                     \
    LOG_WARN("res_info is null", K(ret), K(tenant_id));          \
  } else if (OB_ISNULL(target_monitor = res_info->get_target_monitor())) { \
    ret = OB_ERR_UNEXPECTED;                                     \
    LOG_WARN("target_monitor is null", K(ret), K(tenant_id));    \
  } else if (OB_ISNULL(target_monitor)) {                        \
    ret = OB_ERR_UNEXPECTED;                                     \
    LOG_WARN("target_monitor is null", K(ret), K(tenant_id));    \
  } else {                                                       \
    stmt;                                                        \
    px_info_map_.revert(res_info);                               \
  }

int ObPxTargetMgr::set_parallel_servers_target(uint64_t tenant_id, int64_t parallel_servers_target)
{
  int ret = OB_SUCCESS;
  GET_TARGET_MONITOR(tenant_id, {
    target_monitor->set_parallel_servers_target(parallel_servers_target);
  });
  return ret;
}

int ObPxTargetMgr::get_parallel_servers_target(uint64_t tenant_id, int64_t &parallel_servers_target)
{
  int ret = OB_SUCCESS;
  GET_TARGET_MONITOR(tenant_id, {
    parallel_servers_target = target_monitor->get_parallel_servers_target();
  });
  return ret;
}

int ObPxTargetMgr::get_parallel_session_count(uint64_t tenant_id, int64_t &parallel_session_count)
{
  int ret = OB_SUCCESS;
  GET_TARGET_MONITOR(tenant_id, {
    parallel_session_count = target_monitor->get_parallel_session_count();
  });
  return ret;
}

int ObPxTargetMgr::is_leader(uint64_t tenant_id, bool &is_leader)
{
  int ret = OB_SUCCESS;
  GET_TARGET_MONITOR(tenant_id, {
    is_leader = target_monitor->is_leader();
  });
  return ret;
}

int ObPxTargetMgr::get_version(uint64_t tenant_id, uint64_t &version)
{
  int ret = OB_SUCCESS;
  GET_TARGET_MONITOR(tenant_id, {
    version = target_monitor->get_version();
  });
  return ret;
}

int ObPxTargetMgr::update_peer_target_used(uint64_t tenant_id, const ObAddr &server,
                                           int64_t peer_used, uint64_t version)
{
  int ret = OB_SUCCESS;
  // return OB_HASH_EXIST instead of replacing the element.
  int flag = 0;
  GET_TARGET_MONITOR(tenant_id, {
    if (OB_FAIL(target_monitor->update_peer_target_used(server, peer_used, version))) {
      LOG_WARN("update peer target_used failed", K(ret), K(tenant_id), K(peer_used));
    } else if (server_ != server && OB_FAIL(alive_server_set_.set_refactored(server, flag))) {
      if (OB_HASH_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("alive_server_set_ push_back failed", K(ret), K(server));
      }
    }
  });
  return ret;
}

int ObPxTargetMgr::gather_global_target_usage(uint64_t tenant_id, ObPxGlobalResGather &gather)
{
  int ret = OB_SUCCESS;
  const hash::ObHashMap<ObAddr, ServerTargetUsage> *global_target_usage = NULL;
  GET_TARGET_MONITOR(tenant_id, {
    if (OB_FAIL(target_monitor->get_global_target_usage(global_target_usage))) {
      LOG_WARN("get global target usage failed", K(ret), K(tenant_id));
    } else if (OB_ISNULL(global_target_usage)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("global_target_usage is null", K(ret), K(tenant_id));
    } else if (OB_FAIL(global_target_usage->foreach_refactored(gather))) {
      LOG_WARN("gather global px resource usage failed", K(ret));
    }
  });
  return ret;
}

int ObPxTargetMgr::reset_leader_statistics(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  GET_TARGET_MONITOR(tenant_id, {
    if (OB_FAIL(target_monitor->reset_leader_statistics())) {
      LOG_WARN("reset statistics failed", K(ret));
    }
  });
  return ret;
}

int ObPxTargetMgr::apply_target(uint64_t tenant_id, hash::ObHashMap<ObAddr, int64_t> &worker_map,
                                int64_t wait_time_us, int64_t session_target, int64_t req_cnt,
                                int64_t &admit_count, uint64_t &admit_version)
{
  int ret = OB_SUCCESS;
  GET_TARGET_MONITOR(tenant_id, {
    if (OB_FAIL(target_monitor->apply_target(worker_map, wait_time_us, session_target, req_cnt, admit_count, admit_version))) {
      LOG_WARN("apply target failed", K(ret), K(tenant_id), K(session_target), K(req_cnt));
    }
  });
  return ret;
}

int ObPxTargetMgr::release_target(uint64_t tenant_id, hash::ObHashMap<ObAddr, int64_t> &worker_map, uint64_t admit_version)
{
  int ret = OB_SUCCESS;
  GET_TARGET_MONITOR(tenant_id, {
    if (OB_FAIL(target_monitor->release_target(worker_map, admit_version))) {
      LOG_WARN("return target failed", K(ret), K(tenant_id), K(admit_version));
    }
  });
  return ret;
}

int ObPxTargetMgr::get_all_tenant(common::ObSEArray<uint64_t, 4> &tenant_array)
{
  int ret = OB_SUCCESS;
  tenant_array.reset();
  ObPxInfoMap::Iterator iter(px_info_map_);
  for (ObPxResInfo *value = NULL;
      OB_SUCC(ret) && OB_NOT_NULL(value = iter.next(value)); iter.revert(value)) {
    ObPxTenantInfo tenant_info = value->hash_node_->hash_link_.key_;
    if (OB_FAIL(tenant_array.push_back(tenant_info.get_value()))) {
      LOG_WARN("tenant_array push_back failed", K(ret));
    }
  }
  return ret;
}

int ObPxTargetMgr::get_all_target_info(uint64_t tenant_id, common::ObIArray<ObPxTargetInfo> &target_info_array)
{
  int ret = OB_SUCCESS;
  GET_TARGET_MONITOR(tenant_id, {
    if (OB_FAIL(target_monitor->get_all_target_info(target_info_array))) {
      LOG_WARN("get all target_info failed", K(ret), K(tenant_id));
    }
  });
  return ret;
}

}
}
