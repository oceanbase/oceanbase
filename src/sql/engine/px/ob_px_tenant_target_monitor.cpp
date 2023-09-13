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
#include "ob_px_tenant_target_monitor.h"
#include "lib/oblog/ob_log_module.h"
#include "ob_px_rpc_processor.h"
#include "share/ob_rpc_share.h"
#include "share/schema/ob_schema_utils.h"
#include "storage/tx/ob_trans_service.h"
#include "logservice/ob_log_service.h"
#include "lib/utility/ob_tracepoint.h"

namespace oceanbase
{
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::transaction;
using namespace oceanbase::share::schema;
using namespace obutil;

namespace sql
{

OB_SERIALIZE_MEMBER(ServerTargetUsage, peer_target_used_, local_target_used_, report_target_used_);

int ObPxTenantTargetMonitor::init(const uint64_t tenant_id, ObAddr &server)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_init_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)) ||
             OB_UNLIKELY(!server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(server));
  } else if (OB_FAIL(init_obrpc_proxy(rpc_proxy_))) {
    LOG_WARN("fail init rpc proxy", K(ret));
  } else if (!global_target_usage_.created() && OB_FAIL(global_target_usage_.create(PX_SERVER_TARGET_BUCKET_NUM, ObModIds::OB_SQL_PX))) {
    LOG_WARN("global target usage create failed", K(ret));
  } else if (OB_FAIL(global_target_usage_.set_refactored(server, ServerTargetUsage()))) {
    LOG_WARN("set refactored failed", K(ret));
  } else {
    tenant_id_ = tenant_id;
    server_ = server;
    role_ = FOLLOWER;
    cluster_id_ = GCONF.cluster_id;
    parallel_servers_target_ = INT64_MAX;
    version_ = 0;
    is_init_ = true;
    parallel_session_count_ = 0;
  }
  return ret;
}

void ObPxTenantTargetMonitor::reset()
{
  is_init_ = false;
  tenant_id_ = OB_INVALID_TENANT_ID;
  server_.reset();
  role_ = FOLLOWER;
  dummy_cache_leader_.reset();
  rpc_proxy_.destroy();
  parallel_servers_target_ = INT64_MAX;
  global_target_usage_.clear();
  version_ = UINT64_MAX;
  parallel_session_count_ = 0;
  print_debug_log_ = false;
  need_send_refresh_all_ = true;
}

void ObPxTenantTargetMonitor::set_parallel_servers_target(int64_t parallel_servers_target)
{
  parallel_servers_target_ = parallel_servers_target;
}

int64_t ObPxTenantTargetMonitor::get_parallel_servers_target()
{
  return parallel_servers_target_;
}

int64_t ObPxTenantTargetMonitor::get_parallel_session_count()
{
  return parallel_session_count_;
}

int ObPxTenantTargetMonitor::refresh_statistics(bool need_refresh_all)
{
  int ret = OB_SUCCESS;
  ObAddr leader;
  if (OB_FAIL(get_dummy_leader(leader))) {
    LOG_WARN("get dummy leader fail", K(ret));
  } else if (server_ != leader) {
    LOG_TRACE("follower refresh statistics", K(tenant_id_), K(server_), K(leader), K(role_), K(version_));
    // 单机情况下，不走全局排队
    if (role_ == LEADER) {
      LOG_INFO("leader switch to follower", K(tenant_id_), K(server_), K(leader), K(version_));
      role_ = FOLLOWER;
      // from leader to follower, refresh all the statistics
      if (OB_FAIL(reset_follower_statistics(-1))) {
        LOG_WARN("reset statistics failed", K(ret));
      }
    }
    if (OB_FAIL(query_statistics(leader))) {
      LOG_WARN("query statistics failed", K(ret));
    }
  } else {
    // 只有leader能进，可以无主，但不能多主
    LOG_TRACE("leader refresh statistics", K(tenant_id_), K(server_), K(leader), K(role_), K(need_refresh_all), K(version_));
    if (role_ == FOLLOWER || need_refresh_all) {
      role_ = LEADER;
      // from follower to leader or observer is not longer alive, refresh all the statistics
      if (OB_FAIL(reset_leader_statistics())) {
        LOG_WARN("reset statistics failed", K(ret));
      }
      LOG_INFO("refresh global_target_usage_", K(tenant_id_), K(version_), K(server_), K(need_refresh_all));
    }
  }
  if (!print_debug_log_ && OB_SUCCESS != OB_E(EventTable::EN_PX_PRINT_TARGET_MONITOR_LOG) OB_SUCCESS) {
    print_debug_log_ = true;
  }
  return ret;
}

int ObPxTenantTargetMonitor::get_dummy_leader(ObAddr &leader)
{
  int ret = OB_SUCCESS;
  bool need_refresh = true;
  if (dummy_cache_leader_.is_valid()) {
    leader = dummy_cache_leader_;
  } else {
    MTL_SWITCH(tenant_id_) {
      transaction::ObILocationAdapter *location_adapter = NULL;
      if (OB_ISNULL(location_adapter = MTL(ObTransService*)->get_location_adapter())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("location adapter is null", K(ret), K(tenant_id_));
      } else if (OB_FAIL(location_adapter->nonblock_get_leader(cluster_id_, tenant_id_,
                                                                SYS_LS, leader))) {
        LOG_WARN("nonblock get strong leader failed", K(ret), K(leader));
      } else {
        dummy_cache_leader_ = leader;
      }
    } else {
      LOG_WARN("switch to tenant failed", K(tenant_id_));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(check_dummy_location_credible(need_refresh))) {
      LOG_WARN("check dummy location failed", K(ret));
    }
  }

  if (need_refresh) {
    refresh_dummy_location();
    LOG_WARN("refresh dummy location cache", K(ret));
  }
  return ret;
}

int ObPxTenantTargetMonitor::check_dummy_location_credible(bool &need_refresh)
{
  int ret = OB_SUCCESS;
  ObRole role = FOLLOWER;
  need_refresh = true;
  if (OB_FAIL(get_role(role))) {
    LOG_WARN("get role failed", K(ret));
  } else if ((server_ == dummy_cache_leader_ && role == LEADER) ||
             (server_ != dummy_cache_leader_ && role != LEADER)) {
    need_refresh = false;
  } else {
    LOG_INFO("dummy location not credible, need refresh");
  }
  return ret;
}

int ObPxTenantTargetMonitor::get_role(ObRole &role)
{
  int ret = OB_SUCCESS;
  role = FOLLOWER;
  const uint64_t tenant_id = tenant_id_;
  MTL_SWITCH(tenant_id) {
    bool palf_exist = false;
    int64_t leader_epoch = 0;  // unused
    logservice::ObLogService *log_service = nullptr;
    palf::PalfHandleGuard palf_handle_guard;
    if (OB_ISNULL(log_service = MTL(logservice::ObLogService*))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("MTL ObLogService is null", KR(ret), K(tenant_id));
    } else if (OB_FAIL(log_service->check_palf_exist(SYS_LS, palf_exist))) {
      LOG_WARN("fail to check palf exist", KR(ret), K(tenant_id), K(SYS_LS));
    } else if (!palf_exist) {
      // bypass
    } else if (OB_FAIL(log_service->open_palf(SYS_LS, palf_handle_guard))) {
      LOG_WARN("open palf failed", KR(ret), K(tenant_id), K(SYS_LS));
    } else if (OB_FAIL(palf_handle_guard.get_role(role, leader_epoch))) {
      LOG_WARN("get role failed", KR(ret), K(tenant_id));
    }
  }
  return ret;
}

int ObPxTenantTargetMonitor::refresh_dummy_location()
{
  int ret = OB_SUCCESS;
  static int refresh_ctrl = 0;
  dummy_cache_leader_.reset();
  if ((refresh_ctrl++ % 10) == 0) {
    MTL_SWITCH(tenant_id_) {
      transaction::ObILocationAdapter *location_adapter = NULL;
      if (OB_ISNULL(location_adapter = MTL(ObTransService*)->get_location_adapter())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("location adapter is null", K(ret), K(tenant_id_));
      } else if (OB_FAIL(location_adapter->nonblock_renew(cluster_id_, tenant_id_, SYS_LS))) {
        LOG_WARN("nonblock renew failed", K(ret));
      } else {
        LOG_INFO("refresh locatition cache for target_monitor", K(tenant_id_), K(refresh_ctrl));
      }
    } else {
      LOG_WARN("switch to tenant failed", K(tenant_id_));
    }
  } else {
    LOG_INFO("waiting for refresh locatition cache for target_monitor", K(tenant_id_), K(refresh_ctrl));
  }
  return ret;
}

int ObPxTenantTargetMonitor::query_statistics(ObAddr &leader)
{
  int ret = OB_SUCCESS;
  // send once in 100ms, 1s timeout should be more appropriate than default
  static const int64_t OB_TARGET_MONITOR_RPC_TIMEOUT = 1000 * 1000; // 1s
  ObPxRpcFetchStatArgs args(tenant_id_, version_, need_send_refresh_all_);
  SMART_VAR(ObPxRpcFetchStatResponse, result) {
    need_send_refresh_all_ = false;
    if (!args.need_refresh_all()) {
      for (hash::ObHashMap<ObAddr, ServerTargetUsage>::iterator it = global_target_usage_.begin();
          OB_SUCC(ret) && it != global_target_usage_.end(); ++it) {
        auto report_local_used = [&](hash::HashMapPair<ObAddr, ServerTargetUsage> &entry) -> int {
          int ret = OB_SUCCESS;
          // 和上次汇报相比，本机又消耗了entry 机器几个资源，把这个数目汇报给 leader，leader 会把这个值加到全局统计中。
          // 为什么是汇报“增量”呢？因为 entry 机器的资源被多台机器使用，任何一个人都拿不到全量数据
          int64_t local_used = entry.second.get_local_used();
          if (local_used == entry.second.get_report_used()) {
            // do nothing
          } else if (OB_FAIL(args.push_local_target_usage(entry.first, local_used - entry.second.get_report_used()))) {
            LOG_WARN("push server and target_usage failed", K(ret));
          } else {
            entry.second.set_report_used(local_used);
          }
          return ret;
        };

        if (OB_FAIL(global_target_usage_.atomic_refactored(it->first, report_local_used))) {
          LOG_WARN("atomic refactored, report_local_used failed", K(ret));
        }
      }
    }
    if (OB_FAIL(rpc_proxy_
                .to(leader)
                .by(tenant_id_)
                .timeout(OB_TARGET_MONITOR_RPC_TIMEOUT)
                .fetch_statistics(args, result))) {
      // whether leader receive the rpc is unknown
      need_send_refresh_all_ = true;
      LOG_WARN("send rpc to query statistics failed, need send refresh all", K(ret));
    } else if (result.get_status() == MONITOR_READY) {
      for (int i = 0; OB_SUCC(ret) && i < result.addr_target_array_.count(); i++) {
        ObAddr &server = result.addr_target_array_.at(i).addr_;
        int64_t peer_used_full = result.addr_target_array_.at(i).target_;
        if (OB_FAIL(update_peer_target_used(server, peer_used_full, UINT64_MAX))) {
          LOG_WARN("set thread count failed", K(ret), K(server), K(peer_used_full));
        }
      }
    } else if (result.get_status() == MONITOR_NOT_MASTER) {
      refresh_dummy_location();
      need_send_refresh_all_ = true;
      LOG_INFO("report to not master, need send refresh all", K(tenant_id_), K(leader), K(version_));
    } else if (result.get_status() == MONITOR_VERSION_NOT_MATCH) {
      uint64_t leader_version = result.get_version();
      LOG_INFO("monitor version not match", K(tenant_id_), K(leader_version), K(version_));
      if (OB_FAIL(reset_follower_statistics(leader_version))) {
        LOG_WARN("reset statistics failed", K(ret));
      }
    }
  }
  return ret;
}

bool ObPxTenantTargetMonitor::is_leader()
{
  ObRole role = FOLLOWER;
  (void)get_role(role);
  return (server_ == dummy_cache_leader_) && (role == LEADER);
}

uint64_t ObPxTenantTargetMonitor::get_version()
{
  return version_;
}

int ObPxTenantTargetMonitor::update_peer_target_used(const ObAddr &server, int64_t peer_used, uint64_t version)
{
  int ret = OB_SUCCESS;
  ServerTargetUsage target_usage;
  if (print_debug_log_) {
    LOG_INFO("update_peer_target_used", K(tenant_id_), K(is_leader()), K(version_), K(server), K(peer_used));
  }
  auto update_peer_used = [=](hash::HashMapPair<ObAddr, ServerTargetUsage> &entry) -> void {
    if (is_leader()) {
      entry.second.update_peer_used(peer_used);
      if (OB_UNLIKELY(entry.second.get_peer_used() < 0)) {
        LOG_ERROR("peer used negative", K(tenant_id_), K(version_), K(server), K(entry.second), K(peer_used));
      }
    } else {
      entry.second.set_peer_used(peer_used);
    }
  };
  SpinWLockGuard rlock_guard(spin_lock_);
  if (OB_UNLIKELY(version != OB_INVALID_ID && version != version_)) {
    // version mismatch, do nothing.
  } else if (OB_FAIL(global_target_usage_.get_refactored(server, target_usage))) {
    LOG_WARN("get refactored failed", K(ret), K(tenant_id_), K(server), K(version_));
    if (ret != OB_HASH_NOT_EXIST) {
    } else {
      target_usage.set_peer_used(peer_used);
      if (OB_FAIL(global_target_usage_.set_refactored(server, target_usage))) {
        LOG_WARN("set refactored failed", K(ret));
        if (OB_HASH_EXIST == ret
            && OB_FAIL(global_target_usage_.atomic_refactored(server, update_peer_used))) {
          LOG_WARN("atomic refactored, update_peer_used failed", K(ret));
        }
      }
    }
  } else if (OB_FAIL(global_target_usage_.atomic_refactored(server, update_peer_used))) {
    LOG_WARN("atomic refactored, update_peer_used failed", K(ret));
  }
  return ret;
}

int ObPxTenantTargetMonitor::get_global_target_usage(const hash::ObHashMap<ObAddr, ServerTargetUsage> *&global_target_usage)
{
  int ret = OB_SUCCESS;
  global_target_usage = &global_target_usage_;
  return ret;
}

int ObPxTenantTargetMonitor::reset_follower_statistics(uint64_t version)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard wlock_guard(spin_lock_);
  global_target_usage_.clear();
  if (OB_FAIL(global_target_usage_.set_refactored(server_, ServerTargetUsage()))) {
    LOG_WARN("set refactored failed", K(ret));
  } else {
    version_ = version;
  }
  LOG_INFO("reset follower statistics", K(tenant_id_), K(ret), K(version));
  return ret;
}

int ObPxTenantTargetMonitor::reset_leader_statistics()
{
  int ret = OB_SUCCESS;
  // write lock before reset map and refresh version.
  SpinWLockGuard wlock_guard(spin_lock_);
  global_target_usage_.clear();
  if (OB_FAIL(global_target_usage_.set_refactored(server_, ServerTargetUsage()))) {
    LOG_WARN("set refactored failed", K(ret));
  } else {
    version_ = get_new_version();
  }
  LOG_INFO("reset leader statistics", K(tenant_id_), K(ret), K(version_), K(GCTX.server_id_));
  return ret;
}

int ObPxTenantTargetMonitor::apply_target(hash::ObHashMap<ObAddr, int64_t> &worker_map,
                              int64_t wait_time_us, int64_t session_target, int64_t req_cnt,
                              int64_t &admit_count, uint64_t &admit_version)
{
  int ret = OB_SUCCESS;
  admit_count = 0;
  admit_version = UINT64_MAX;
  bool need_wait = false;
  if (OB_SUCC(ret)) {
    // read lock to avoid reset map.
    SpinRLockGuard rlock_guard(spin_lock_); // Just for avoid multiple SQL applications at the same time
    // for pmas
    int64_t target = session_target;
    uint64_t version = version_;
    bool is_first_query = true;
    bool is_target_enough = true;
    for (hash::ObHashMap<ObAddr, int64_t>::iterator it = worker_map.begin();
        OB_SUCC(ret) && it != worker_map.end(); it++) {
      const ObAddr &server = it->first;
      int64_t exp_target_count = it->second;
      ServerTargetUsage target_usage;
      if (OB_FAIL(global_target_usage_.get_refactored(server, target_usage))) {
        if (ret != OB_HASH_NOT_EXIST) {
          LOG_WARN("get refactored failed", K(ret));
        } else {
          // maybe the status is not_ready, because of version++,
          // but still can use local, so now, rebuild local
          ret = OB_SUCCESS;
          if (OB_FAIL(global_target_usage_.set_refactored(server, target_usage))) {
            if (OB_HASH_EXIST == ret) {
              // add empty target_usage for server. if hash exist, means others already set just ignore.
              ret = OB_SUCCESS;
            } else {
              LOG_WARN("set refactored failed", K(ret));
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        // 计算当前 server 视角下，target_usage 对应 server 已经分配出去的资源数量：
        //  total_user = leader 反馈的 + 本地尚未同步给 leader 的
        //  (显然，这是不精确的，比如其它 follower 尚未同步给 leader 的就没有计算在内）
        uint64_t total_use = target_usage.get_peer_used() + (target_usage.get_local_used() - target_usage.get_report_used());
        if (total_use != 0) {
          is_first_query = false;
        }
        if (is_target_enough && (total_use + exp_target_count > target)) {
          is_target_enough = false;
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (is_first_query || is_target_enough) {
        for (hash::ObHashMap<ObAddr, int64_t>::iterator it = worker_map.begin();
            OB_SUCC(ret) && it != worker_map.end(); it++) {
          it->second = std::min(it->second, target);
          ObAddr &server = it->first;
          int64_t acquired_cnt = it->second;
          auto apply_local_target = [=](hash::HashMapPair<ObAddr, ServerTargetUsage> &entry) -> void {
            entry.second.update_local_used(acquired_cnt);
            if (is_leader()) {
              // leader no need report, so set as all reported
              entry.second.set_report_used(entry.second.get_local_used());
              entry.second.update_peer_used(acquired_cnt);
            }
          };
          if (OB_FAIL(global_target_usage_.atomic_refactored(server, apply_local_target))) {
            LOG_WARN("atomic refactored, update_peer_used failed", K(ret));
          } else if (print_debug_log_) {
            LOG_INFO("apply target success", K(tenant_id_), K(server), K(acquired_cnt), K(version), K(version_),
                                              K(parallel_servers_target_));
          }
        }
        admit_count = std::min(req_cnt, target);
        admit_version = version;
        parallel_session_count_++;
      } else {
        need_wait = true;
      }
    }
  }
  if (OB_SUCC(ret) && need_wait) {
    // when got no resource, wait for next available resource
    //
    // NOTE: when any resource returned , ALL waiting threads are waken up
    //       this is because the returned resource maybe a very big chunk,
    //       which can feed many waiting threads
    LOG_DEBUG("wait begin", K(wait_time_us), K(session_target), K(req_cnt));
    int64_t wait_us = min(wait_time_us, 1000000L);
    target_cond_.wait(wait_us); // sleep at most 1sec, in order to check interrupt
    LOG_DEBUG("wait finish");
  }
  return ret;
}

int ObPxTenantTargetMonitor::release_target(hash::ObHashMap<ObAddr, int64_t> &worker_map, uint64_t version)
{
  int ret = OB_SUCCESS;
  SpinRLockGuard rlock_guard(spin_lock_);
  if (version == version_) {
    for (hash::ObHashMap<ObAddr, int64_t>::iterator it = worker_map.begin();
        OB_SUCC(ret) && it != worker_map.end(); it++) {
      ObAddr &server = it->first;
      int64_t acquired_cnt = it->second;
      auto release_local_target = [=](hash::HashMapPair<ObAddr, ServerTargetUsage> &entry) -> void {
        entry.second.update_local_used(-acquired_cnt);
        if (is_leader()) {
          // leader no need report, so set as all reported
          entry.second.set_report_used(entry.second.get_local_used());
          entry.second.update_peer_used(-acquired_cnt);
        }
      };
      if (OB_FAIL(global_target_usage_.atomic_refactored(server, release_local_target))) {
        LOG_WARN("atomic refactored, update_peer_used failed", K(ret));
      } else if (print_debug_log_) {
        LOG_INFO("release target success", K(tenant_id_), K(server), K(acquired_cnt), K(version_),
                                            K(parallel_servers_target_));
      }
    }
    target_cond_.notifyAll();
  } else {
    LOG_INFO("version changed", K(tenant_id_), K(version_), K(version));
  }
  parallel_session_count_--;
  return ret;
}

int ObPxTenantTargetMonitor::get_all_target_info(common::ObIArray<ObPxTargetInfo> &target_info_array)
{
  int ret = OB_SUCCESS;
  target_info_array.reset();
  bool leader = is_leader();
  auto get_target_info = [&](hash::HashMapPair<ObAddr, ServerTargetUsage> &entry) -> int {
    int ret = OB_SUCCESS;
    ObPxTargetInfo monitor_info;
    monitor_info.server_ = server_;
    monitor_info.tenant_id_ = tenant_id_;
    monitor_info.is_leader_ = leader;
    monitor_info.version_ = version_;
    monitor_info.parallel_servers_target_ = parallel_servers_target_;
    monitor_info.peer_server_ = entry.first;
    monitor_info.peer_target_used_ = entry.second.get_peer_used() + (entry.second.get_local_used() - entry.second.get_report_used());
    monitor_info.local_target_used_ = entry.second.get_local_used();
    monitor_info.local_parallel_session_count_ = parallel_session_count_;
    if (OB_FAIL(target_info_array.push_back(monitor_info))) {
      LOG_WARN("target_info_array push_back failed", K(ret), K(monitor_info));
    }
    return ret;
  };
  if (OB_FAIL(global_target_usage_.foreach_refactored(get_target_info))) {
    LOG_WARN("foreach refactored get_target_info failed", K(ret));
  }
  return ret;
}

uint64_t ObPxTenantTargetMonitor::get_new_version()
{
  uint64_t current_time = common::ObTimeUtility::current_time();
	uint64_t svr_id = GCTX.server_id_;
	uint64_t new_version = ((current_time & 0x0000FFFFFFFFFFFF) | (svr_id << SERVER_ID_SHIFT));
  return new_version;
}

uint64_t ObPxTenantTargetMonitor::get_server_id(uint64_t version) {
  return (version >> SERVER_ID_SHIFT);
}

int ObPxTargetCond::wait(const int64_t wait_time_us)
{
  int ret = OB_SUCCESS;
  if (wait_time_us < 0) {
    TRANS_LOG(WARN, "invalid argument", K(wait_time_us));
    ret = OB_INVALID_ARGUMENT;
  } else {
    THIS_WORKER.sched_wait();
    {
      ObMonitor<Mutex>::Lock guard(monitor_);
      if (!monitor_.timed_wait(ObSysTime(wait_time_us))) { // timeout
        ret = OB_TIMEOUT;
      }
    }
    THIS_WORKER.sched_run();
  }
  return ret;
}

void ObPxTargetCond::notifyAll()
{
  ObMonitor<Mutex>::Lock guard(monitor_);
  monitor_.notify_all();
}

void ObPxTargetCond::usleep(const int64_t us)
{
  if (us > 0) {
    ObMonitor<Mutex> monitor;
    THIS_WORKER.sched_wait();
    (void)monitor.timed_wait(ObSysTime(us));
    THIS_WORKER.sched_run();
  }
}

}
}
