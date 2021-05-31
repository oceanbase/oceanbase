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

#if 0
#define USING_LOG_PREFIX RS

#include "ob_in_zone_server_tracer.h"
#include "ob_in_zone_master.h"
#include "ob_zone_master_storage_util.h"
#include "share/ob_define.h"
#include "lib/container/ob_array_iterator.h"
#include "share/ob_srv_rpc_proxy.h"
#include "share/config/ob_server_config.h"
namespace oceanbase
{
using namespace common;
using namespace share;
using namespace obrpc;
using namespace blocksstable;
namespace rootserver
{

// ================= ObInZoneServerStatus ================
int ObInZoneServerStatus::assign(
    const ObInZoneServerStatus &that)
{
  int ret = OB_SUCCESS;
  server_ = that.server_;
  last_hb_time_ = that.last_hb_time_;
  in_zone_hb_status_ = that.in_zone_hb_status_;
  return ret;
}

// ================ ObInZoneServerTracer ===============
int ObInZoneServerTracer::init(
    common::ObServerConfig *config,
    obrpc::ObSrvRpcProxy *rpc_proxy,
    ObInZoneMaster *in_zone_master,
    const common::ObAddr &self_addr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(nullptr == config
                         || nullptr == rpc_proxy
                         || nullptr == in_zone_master
                         || !self_addr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(config), KP(rpc_proxy),
            KP(in_zone_master), K(self_addr));
  } else {
    config_ = config;
    rpc_proxy_ = rpc_proxy;
    in_zone_master_ = in_zone_master;
    self_addr_ = self_addr;
    inited_ = true;
  }
  return ret;
}

int ObInZoneServerTracer::reload()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("in zone server tracer not init", K(ret));
  } else if (OB_UNLIKELY(nullptr == config_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("config_ ptr is null", K(ret), KP(config_));
  } else {
    loaded_ = false;
    common::ObArray<ObServerWorkingDir> server_dir_array;
    if (OB_FAIL(ObZoneMasterStorageUtil::get_all_server_working_dirs(
            server_dir_array))) {
      LOG_WARN("fail to get all server working dirs", K(ret));
    } else {
      SpinWLockGuard guard(maintenance_lock_);
      for (int64_t i = 0; OB_SUCC(ret) && i < server_dir_array.count(); ++i) {
        const ObServerWorkingDir &server_dir = server_dir_array.at(i);
        if (OB_UNLIKELY(!server_dir.is_valid())) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid argument", K(ret), K(server_dir));
        } else if (ObServerWorkingDir::DirStatus::NORMAL == server_dir.status_) {
          bool is_exist = false;
          if (OB_FAIL(inner_check_server_exist(server_dir.svr_addr_, is_exist))) {
            LOG_WARN("fail to inner check server exist", K(ret),
                     "server", server_dir.svr_addr_);
          } else if (is_exist) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("server already exist in zone server tracer", K(ret),
                     "server", server_dir.svr_addr_);
          } else {
            const int64_t now = ObTimeUtility::current_time();
            const int64_t last_hb_time = server_dir.lease_end_ts_;
            const bool is_pmt_f = (now - last_hb_time >= config_->server_permanent_offline_time
                                   ? true
                                   : false);
            ObInZoneServerStatus server_status;
            server_status.server_ = server_dir.svr_addr_;
            server_status.last_hb_time_ = last_hb_time;
            server_status.in_zone_hb_status_ = (is_pmt_f
                                                ? IN_ZONE_HEARTBEAT_PERMANENT_OFFLINE
                                                : IN_ZONE_HEARTBEAT_ACTIVE);
            if (OB_FAIL(server_status_array_.push_back(server_status))) {
              LOG_WARN("fail to push back server", K(ret));
            }
          }
        } else if (ObServerWorkingDir::DirStatus::RECOVERING == server_dir.status_) {
          bool is_exist = false;
          if (OB_FAIL(inner_check_server_exist(server_dir.svr_addr_, is_exist))) {
            LOG_WARN("fail to inner check server exist", K(ret),
                     "server", server_dir.svr_addr_);
          } else if (is_exist) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("server already exist in zone server tracer", K(ret),
                     "server", server_dir.svr_addr_);
          } else {
            ObInZoneServerStatus server_status;
            server_status.server_ = server_dir.svr_addr_;
            server_status.last_hb_time_ = 0;
            server_status.in_zone_hb_status_ = IN_ZONE_HEARTBEAT_TAKENOVER_BY_MASTER;
            if (OB_FAIL(server_status_array_.push_back(server_status))) {
              LOG_WARN("fail to push back", K(ret));
            }
          }
        } else if (ObServerWorkingDir::DirStatus::RECOVERED == server_dir.status_) {
          // TODO: remove recovered server working dir on ofs
        } else if (ObServerWorkingDir::DirStatus::TEMPORARY == server_dir.status_) {
          if (OB_FAIL(ObZoneMasterStorageUtil::clear_tmp_server_working_dir(server_dir))) {
            LOG_WARN("fail to clear tmp server working dir", K(ret));
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected server dir status", K(ret), K(server_dir));
        }
      }
    }
    if (OB_SUCC(ret)) {
      loaded_ = true;
    }
  }
  return ret;
}

int ObInZoneServerTracer::get_servers_takenover_by_in_zone_master(
    common::ObIArray<common::ObAddr> &servers_takenover_by_in_zone_master) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_ || !loaded_)) {
    ret = OB_CANCELED;
    LOG_WARN("in zone server tracer canceled", K(ret));
  } else {
    servers_takenover_by_in_zone_master.reset();
    SpinRLockGuard guard(maintenance_lock_);
    for (int64_t i = 0; OB_SUCC(ret) && i < server_status_array_.count(); ++i) {
      const ObInZoneServerStatus &server_status = server_status_array_.at(i);
      SpinRLockGuard inner_guard(server_status.lock_);
      if (IN_ZONE_HEARTBEAT_TAKENOVER_BY_MASTER != server_status.in_zone_hb_status_) {
        // bypass
      } else if (OB_FAIL(servers_takenover_by_in_zone_master.push_back(
              server_status.get_server()))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
  }
  return ret;
}

int ObInZoneServerTracer::get_active_servers(
    common::ObIArray<common::ObAddr> &active_servers) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_ || !loaded_)) {
    ret = OB_CANCELED;
    LOG_WARN("in zone server tracer canceled", K(ret));
  } else {
    active_servers.reset();
    SpinRLockGuard guard(maintenance_lock_);
    for (int64_t i = 0; OB_SUCC(ret) && i < server_status_array_.count(); ++i) {
      const ObInZoneServerStatus &server_status = server_status_array_.at(i);
      SpinRLockGuard inner_guard(server_status.lock_);
      if (IN_ZONE_HEARTBEAT_ACTIVE != server_status.in_zone_hb_status_) {
        // bypass
      } else if (OB_FAIL(active_servers.push_back(server_status.get_server()))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
  }
  return ret;
}

int ObInZoneServerTracer::check_server_active(
    const common::ObAddr &server,
    bool &is_active) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_ || !loaded_)) {
    ret = OB_CANCELED;
    LOG_WARN("in zone server tracer canceled", K(ret));
  } else {
    bool find = false;
    SpinRLockGuard guard(maintenance_lock_);
    for (int64_t i = 0;
         OB_SUCC(ret) && !find && i < server_status_array_.count();
         ++i) {
      const ObInZoneServerStatus &server_status = server_status_array_.at(i);
      SpinRLockGuard inner_guard(server_status.lock_);
      if (server != server_status.get_server()) {
        // bypass
      } else if (IN_ZONE_HEARTBEAT_ACTIVE != server_status.in_zone_hb_status_) {
        is_active = false;
        find = true;
      } else {
        is_active = true;
        find = true;
      }
    }
    if (OB_SUCC(ret) && !find) {
      // when server not exist, treat it as inactive
      is_active = false;
    }
  }
  return ret;
}

int ObInZoneServerTracer::check_server_beyond_permanent_offline(
    const common::ObAddr &server,
    bool &beyond_permanent_offline) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_ || !loaded_)) {
    ret = OB_CANCELED;
    LOG_WARN("in zone server tracer canceled", K(ret));
  } else {
    bool find = false;
    SpinRLockGuard guard(maintenance_lock_);
    for (int64_t i = 0;
         OB_SUCC(ret) && !find && i < server_status_array_.count();
         ++i) {
      const ObInZoneServerStatus &server_status = server_status_array_.at(i);
      SpinRLockGuard inner_guard(server_status.lock_);
      if (server != server_status.get_server()) {
        // bypass
      } else if (IN_ZONE_HEARTBEAT_PERMANENT_OFFLINE == server_status.in_zone_hb_status_
          || IN_ZONE_HEARTBEAT_TAKENOVER_BY_MASTER == server_status.in_zone_hb_status_) {
        beyond_permanent_offline = true;
        find = true;
      } else {
        beyond_permanent_offline = false;
        find = true;
      }
    }
    if (OB_SUCC(ret) && !find) {
      beyond_permanent_offline = true;
    }
  }
  return ret;
}

int ObInZoneServerTracer::inner_check_server_exist(
    const common::ObAddr &server,
    bool &exist)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_ || !loaded_)) {
    ret = OB_CANCELED;
    LOG_WARN("in zone server not in service", K(ret));
  } else {
    exist = false;
    for (int64_t i = 0;
         OB_SUCC(ret) && !exist && i < server_status_array_.count();
         ++i) {
      const ObInZoneServerStatus &server_status = server_status_array_.at(i);
      SpinRLockGuard inner_guard(server_status.lock_);
      if (server != server_status.get_server()) {
        // bypass
      } else {
        exist = true;
      }
    }
  }
  return ret;
}

int ObInZoneServerTracer::check_server_exist(
    const common::ObAddr &server,
    bool &exist)
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(maintenance_lock_);
  if (OB_UNLIKELY(!inited_ || !loaded_)) {
    ret = OB_CANCELED;
    LOG_WARN("in zone server tracer not in service", K(ret));
  } else if (OB_FAIL(inner_check_server_exist(server, exist))) {
    LOG_WARN("fail to inner check server exist", K(ret), K(server));
  }
  return ret;
}

int ObInZoneServerTracer::check_servers()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_ || !loaded_)) {
    ret = OB_CANCELED;
    LOG_WARN("in zone server tracer not in service", K(ret));
  } else if (OB_UNLIKELY(nullptr == config_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("config ptr is null", K(ret));
  } else {
    const int64_t now = common::ObTimeUtility::current_time();
    SpinRLockGuard guard(maintenance_lock_);
    for (int64_t i = 0; OB_SUCC(ret) && i < server_status_array_.count(); ++i) {
      ObInZoneServerStatus &server_status = server_status_array_.at(i);
      SpinWLockGuard item_guard(server_status.lock_);
      const int64_t last_hb_time = server_status.last_hb_time_;
      InZoneHeartBeatStatus in_zone_hb_status = server_status.in_zone_hb_status_;
      if (now - last_hb_time >= config_->lease_time) {
        in_zone_hb_status = IN_ZONE_HEARTBEAT_LEASE_EXPIRED;
      }
      if (now - last_hb_time >= config_->server_permanent_offline_time) {
        in_zone_hb_status = IN_ZONE_HEARTBEAT_PERMANENT_OFFLINE;
      }
      if (in_zone_hb_status != server_status.in_zone_hb_status_) {
        if (IN_ZONE_HEARTBEAT_TAKENOVER_BY_MASTER == server_status.in_zone_hb_status_) {
          // already taken over by in zone master, ignore it
        } else {
          LOG_INFO("server hb status change", "server", server_status.server_,
                   "from_hb_status", server_status.in_zone_hb_status_,
                   "to_hb_status", in_zone_hb_status);
          server_status.in_zone_hb_status_ = in_zone_hb_status;
        }
      } else if (in_zone_hb_status == IN_ZONE_HEARTBEAT_PERMANENT_OFFLINE) {
        if (now - last_hb_time > ObLeaseRequest::RS_TAKENOVER_OBS_INTERVAL + config_->lease_time) {
          LOG_INFO("server malfunction, in zone master start to take over it",
                   "server", server_status.server_, K(now));
          if (OB_FAIL(in_zone_master_->on_server_takenover_by_in_zone_master(
                  server_status.server_))) {
            LOG_WARN("fail to commit server takenover by in zone master task", K(ret));
          } else {
            LOG_INFO("succeed to commit server takenover by in zone master task", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObInZoneServerTracer::modify_permanent_offline_server_dir(
    const common::ObAddr &server)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_ || !loaded_)) {
    ret = OB_CANCELED;
    LOG_WARN("in zone server tracer not in service", K(ret));
  } else if (OB_UNLIKELY(!server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(server));
  } else {
    ObServerWorkingDir svr_working_dir;
    svr_working_dir.svr_addr_ = server;
    svr_working_dir.status_ = ObServerWorkingDir::DirStatus::RECOVERING;
    if (OB_FAIL(ObZoneMasterStorageUtil::try_recovering_server_working_dir(svr_working_dir))) {
      LOG_WARN("fail to try recovering server working dir", K(ret));
    }
  }
  return ret;
}

int ObInZoneServerTracer::takeover_permanent_offline_server(
    const common::ObAddr &server)
{
  int ret = OB_SUCCESS;
  bool exist = false;
  if (OB_UNLIKELY(!inited_ || !loaded_)) {
    ret = OB_CANCELED;
    LOG_WARN("in zone server tracer not in service", K(ret));
  } else if (OB_UNLIKELY(!server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(server));
  } else if (OB_FAIL(check_server_exist(server, exist))) {
    LOG_WARN("fail to check server exist", K(ret));
  } else if (!exist) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("server not exist", K(ret), K(server));
  } else {
    SpinRLockGuard guard(maintenance_lock_);
    for (int64_t i = 0; OB_SUCC(ret) && i < server_status_array_.count(); ++i) {
      ObInZoneServerStatus &server_status = server_status_array_.at(i);
      SpinWLockGuard inner_guard(server_status.lock_);
      if (server_status.server_ != server) {
        // bypass, not this server
      } else if (IN_ZONE_HEARTBEAT_TAKENOVER_BY_MASTER == server_status.in_zone_hb_status_) {
        // bypass, maybe already process by previos task
      } else if (IN_ZONE_HEARTBEAT_PERMANENT_OFFLINE != server_status.in_zone_hb_status_) {
        ret = OB_STATE_NOT_MATCH;
        LOG_WARN("may by modified by others", K(ret), K(server));
      } else if (OB_FAIL(modify_permanent_offline_server_dir(server))) {
        LOG_WARN("fail to do modify permanent offline server dir", K(ret));
      } else {
        server_status.in_zone_hb_status_ = IN_ZONE_HEARTBEAT_TAKENOVER_BY_MASTER;
      }
    }
  }
  return ret;
}

int ObInZoneServerTracer::inner_import_in_zone_server(
    const ObInZoneHbRequest &in_zone_hb_request,
    ObInZoneHbResponse &in_zone_hb_response)
{
  int ret = OB_SUCCESS;
  bool exist = false;
  ObServerWorkingDir server_dir;
  if (OB_UNLIKELY(!inited_ || !loaded_)) {
    ret = OB_CANCELED;
    LOG_WARN("in zone server tracer not in service", K(ret));
  } else if (OB_UNLIKELY(!in_zone_hb_request.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(in_zone_hb_request));
  } else if (OB_UNLIKELY(nullptr == config_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("config_ ptr is null", K(ret));
  } else if (OB_FAIL(inner_check_server_exist(
          in_zone_hb_request.server_, exist))) {
    LOG_WARN("fail to inner check server exist", K(ret),
             "server", in_zone_hb_request.server_);
  } else if (exist) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("server already exist", K(ret));
  } else if (OB_FAIL(ObZoneMasterStorageUtil::create_server_working_dir(
          in_zone_hb_request.server_, 0/*start lease ts*/, 0/*end lease ts*/, server_dir))) {
    LOG_WARN("fail to create server working dir", K(ret));
  } else {
    const int64_t now = common::ObTimeUtility::current_time();
    ObInZoneServerStatus server_status;
    server_status.server_ = in_zone_hb_request.server_;
    server_status.last_hb_time_ = now;
    server_status.in_zone_hb_status_ = IN_ZONE_HEARTBEAT_ACTIVE;
    if (OB_FAIL(server_status_array_.push_back(server_status))) {
      LOG_WARN("fail to push back", K(ret));
    } else if (OB_FAIL(inner_receive_in_zone_heartbeat(
            in_zone_hb_request, in_zone_hb_response))) {
      LOG_WARN("fail to inner receive in zone heartbeat", K(ret));
    }
  }
  return ret;
}

int ObInZoneServerTracer::inner_receive_in_zone_heartbeat(
    const ObInZoneHbRequest &in_zone_hb_request,
    ObInZoneHbResponse &in_zone_hb_response)
{
  int ret = OB_SUCCESS;
  bool exist = false;
  if (OB_UNLIKELY(!inited_ || !loaded_)) {
    ret = OB_CANCELED;
    LOG_WARN("in zone server not in service", K(ret));
  } else if (OB_FAIL(inner_check_server_exist(
          in_zone_hb_request.server_, exist))) {
    LOG_WARN("fail to inner check server exist", K(ret),
             "server", in_zone_hb_request.server_);
  } else if (!exist) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("server");
  } else if (OB_UNLIKELY(nullptr == config_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("config ptr is null", K(ret));
  } else {
    const common::ObAddr &server = in_zone_hb_request.server_;
    for (int64_t i = 0;
         OB_SUCC(ret) && i < server_status_array_.count();
         ++i) {
      ObInZoneServerStatus &server_status = server_status_array_.at(i);
      SpinWLockGuard inner_guard(server_status.lock_);
      const int64_t now = ObTimeUtility::current_time();
      if (server != server_status.server_) {
        // bypass
      } else if (IN_ZONE_HEARTBEAT_TAKENOVER_BY_MASTER
                 == server_status.in_zone_hb_status_) {
        // do not serve the heartbeat for the server which is taken over by in zone master,
        // set to hb_expire_time to 0
        in_zone_hb_response.in_zone_hb_expire_time_ = 0;
      } else {
        server_status.last_hb_time_ = now;
        server_status.in_zone_hb_status_ = IN_ZONE_HEARTBEAT_ACTIVE;
        in_zone_hb_response.in_zone_hb_expire_time_ = now + config_->lease_time;
      }
    }
  }
  return ret;
}

int ObInZoneServerTracer::receive_in_zone_heartbeat(
    const ObInZoneHbRequest &in_zone_hb_request,
    ObInZoneHbResponse &in_zone_hb_response)
{
  int ret = OB_SUCCESS;
  const common::ObAddr &server = in_zone_hb_request.server_;
  if (OB_UNLIKELY(!inited_ || !loaded_)) {
    ret = OB_CANCELED;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(in_zone_hb_request));
  } else {
    bool exist = false;
    {
      SpinRLockGuard guard(maintenance_lock_);
      if (OB_FAIL(inner_check_server_exist(server, exist))) {
        LOG_WARN("fail to inner check server exist", K(ret), K(server));
      }
    }
    if (OB_SUCC(ret)) {
      if (exist) {
        SpinRLockGuard guard(maintenance_lock_);
        if (OB_FAIL(inner_receive_in_zone_heartbeat(
                in_zone_hb_request, in_zone_hb_response))) {
          LOG_WARN("fail to inner receive in zone heartbeat",
                   K(ret), K(in_zone_hb_request));
        }
      } else {
        SpinWLockGuard guard(maintenance_lock_);
        if (OB_FAIL(inner_import_in_zone_server(
                in_zone_hb_request, in_zone_hb_response))) {
          LOG_WARN("fail to inner import in zone server",
                   K(ret), K(in_zone_hb_request));
        }
      }
    }
  }
  return ret;
}

// ============================ InZoneHeartBeatChecker =============================
int ObInZoneHeartbeatChecker::init(
    ObInZoneServerTracer &in_zone_server_tracer)
{
  int ret = OB_SUCCESS;
  static const int64_t heartbeat_checker_thread_cnt = 1;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("heart beat checker has already been inited", K(ret));
  } else if (OB_FAIL(create(heartbeat_checker_thread_cnt, "InZoneHbChecker"))) {
    LOG_WARN("create heartbeat checker thread failed",
             K(ret), K(heartbeat_checker_thread_cnt));
  } else {
    in_zone_server_tracer_ = &in_zone_server_tracer;
    inited_ = true;
  }
  return ret;
}

void ObInZoneHeartbeatChecker::run3()
{
  if (!inited_) {
    int tmp_ret = OB_NOT_INIT;
    LOG_WARN("In zone heart beat checker has not inited", "ret", tmp_ret);
  } else {
    LOG_INFO("In zone heartbeat checker start");
    while (!stop_) {
      LOG_TRACE("begin check in zone server heartbeat");
      int tmp_ret = in_zone_server_tracer_->check_servers();
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("server managers check servers failed", "ret", tmp_ret);
      }
      for (int64_t i = 0; !stop_ && i < CHECK_INTERVAL_TIMES; ++i) {
        usleep(CHECK_INTERVAL_US);
      }
    }
    LOG_INFO("in zone heartbeat checker stop");
  }
}

}//end namespace rootserver
}//end namespace oceanbase
#endif
