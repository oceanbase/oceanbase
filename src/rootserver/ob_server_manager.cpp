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

#define USING_LOG_PREFIX RS

#include "ob_server_manager.h"

#include "share/ob_define.h"
#include "lib/container/ob_array_iterator.h"
#include "share/ob_debug_sync.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/ob_srv_rpc_proxy.h"
#include "share/config/ob_server_config.h"
#include "share/ob_max_id_fetcher.h"
#include "rootserver/ob_unit_manager.h"
#include "rootserver/ob_zone_manager.h"
#include "rootserver/ob_rs_event_history_table_operator.h"
#include "rootserver/ob_leader_coordinator.h"
#include "rootserver/ob_rebalance_task_mgr.h"
#include "common/storage/ob_freeze_define.h"
#include "clog/ob_clog_history_reporter.h"
#include "rootserver/ob_rs_job_table_operator.h"
namespace oceanbase {
using namespace common;
using namespace share;
using namespace obrpc;
namespace rootserver {
ObServerManager::ObServerManager()
    : inited_(false),
      has_build_(false),
      server_status_rwlock_(ObLatchIds::SERVER_STATUS_LOCK),
      maintaince_lock_(ObLatchIds::SERVER_MAINTAINCE_LOCK),
      status_change_callback_(NULL),
      server_change_callback_(NULL),
      config_(NULL),
      unit_mgr_(NULL),
      zone_mgr_(NULL),
      leader_coordinator_(NULL),
      rpc_proxy_(NULL),
      st_operator_(),
      rs_addr_(),
      server_statuses_()
{
  reset();
}

ObServerManager::~ObServerManager()
{}

int ObServerManager::init(ObIStatusChangeCallback& status_change_callback,
    ObIServerChangeCallback& server_change_callback, ObMySQLProxy& proxy, ObUnitManager& unit_mgr,
    ObZoneManager& zone_mgr, ObILeaderCoordinator& leader_coordinator, common::ObServerConfig& config,
    const common::ObAddr& rs_addr, obrpc::ObSrvRpcProxy& rpc_proxy)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("server manager have already init", K(ret));
  } else if (!rs_addr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid rs_addr", K(rs_addr), K(ret));
  } else if (OB_FAIL(st_operator_.init(&proxy))) {
    LOG_WARN("server table operator init failed", K(ret));
  } else {
    has_build_ = false;
    status_change_callback_ = &status_change_callback;
    server_change_callback_ = &server_change_callback;
    config_ = &config;
    unit_mgr_ = &unit_mgr;
    zone_mgr_ = &zone_mgr;
    leader_coordinator_ = &leader_coordinator;
    rs_addr_ = rs_addr;
    rpc_proxy_ = &rpc_proxy;
    server_statuses_.reset();
    inited_ = true;
  }
  return ret;
}

int ObServerManager::add_server(const common::ObAddr& server, const ObZone& zone)
{
  int ret = OB_SUCCESS;
  ObServerStatus* server_status = NULL;
  uint64_t server_id = OB_INVALID_ID;
  // avoid maintain operation run concurrently
  SpinWLockGuard guard(maintaince_lock_);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), K(ret));
  } else if (!zone.is_empty()) {
    bool zone_active = false;
    if (OB_FAIL(zone_mgr_->check_zone_active(zone, zone_active))) {
      LOG_WARN("check_zone_active failed", K(zone), K(ret));
    } else if (!zone_active) {
      ret = OB_ZONE_NOT_ACTIVE;
      LOG_WARN("zone not active", K(zone), K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    {
      SpinRLockGuard guard(server_status_rwlock_);
      if (OB_SUCC(find(server, server_status))) {
        if (NULL == server_status) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("server_status is null", "server_status ptr", OB_P(server_status), K(ret));
        } else if (!server_status->is_status_valid()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("status not valid", K(ret), "status", *server_status);
        } else {
          ret = OB_ENTRY_EXIST;
          LOG_WARN("server already added", K(server), K(ret));
        }
      } else if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("find failed", K(server), K(ret));
      } else {
        ret = OB_SUCCESS;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(fetch_new_server_id(server_id))) {
      LOG_WARN("fetch_new_server_id failed", K(ret));
    } else {
      const int64_t now = common::ObTimeUtility::current_time();
      ObServerStatus new_server_status;
      new_server_status.id_ = server_id;
      new_server_status.server_ = server;
      new_server_status.zone_ = zone;
      new_server_status.admin_status_ = ObServerStatus::OB_SERVER_ADMIN_NORMAL;
      new_server_status.hb_status_ = ObServerStatus::OB_HEARTBEAT_ALIVE;
      new_server_status.last_hb_time_ = now;
      new_server_status.lease_expire_time_ = now + ObLeaseRequest::SERVICE_LEASE;

      if (OB_SUCC(ret)) {
        if (OB_FAIL(st_operator_.update(new_server_status))) {
          LOG_WARN("st_operator update failed", K(new_server_status), K(ret));
        } else {
          SpinWLockGuard inner_guard(server_status_rwlock_);
          if (OB_FAIL(server_statuses_.push_back(new_server_status))) {
            LOG_WARN("push_back failed", K(ret));
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    ROOTSERVICE_EVENT_ADD("server", "add_server", K(server));
    LOG_INFO("add new server", K(server), K(zone));
    int tmp = server_change_callback_->on_server_change();
    if (OB_SUCCESS != tmp) {
      LOG_WARN("fail to callback on server change", K(ret));
    } else {
      LOG_WARN("callback on add server success");
    }
  }
  return ret;
}

int ObServerManager::finish_server_recovery(const common::ObAddr& server)
{
  int ret = OB_SUCCESS;
  // avoid maintain operation run concurrently
  SpinWLockGuard guard(maintaince_lock_);
  ObServerStatus* server_status = nullptr;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(server));
  } else {
    {
      SpinRLockGuard guard(server_status_rwlock_);
      if (OB_FAIL(find(server, server_status))) {
        LOG_WARN("fail to find server status", K(ret), K(server));
      } else if (nullptr == server_status) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("server status is null", K(ret), K(server));
      } else if (ObServerStatus::OB_SERVER_ADMIN_TAKENOVER_BY_RS != server_status->admin_status_) {
        ret = OB_STATE_NOT_MATCH;
        LOG_WARN("server admin status not match", K(ret), "admin_status", server_status->admin_status_);
      }
    }
    if (OB_SUCC(ret)) {
      common::ObMySQLTransaction trans;
      if (OB_FAIL(trans.start(&st_operator_.get_proxy()))) {
        LOG_WARN("fail to start trans", K(ret));
      } else if (OB_FAIL(st_operator_.remove(server, trans))) {
        LOG_WARN("fail to remove", K(ret), K(server));
      } else if (OB_FAIL(update_admin_status(server, server_status->admin_status_, true /* remove */))) {
        LOG_WARN("fail to update admin status", K(ret));
      }
      if (trans.is_started()) {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
          LOG_WARN("fail to end trans", K(tmp_ret));
          ret = OB_SUCC(ret) ? tmp_ret : ret;
        }
      }
      if (OB_SUCC(ret)) {
        // delete associated records from __all_clog_history_info_v2 about this server
        clog::ObClogHistoryReporter::get_instance().delete_server_record(server);
        LOG_INFO("finish server recovery", K(server));
        if (OB_SUCCESS != server_change_callback_->on_server_change()) {
          LOG_WARN("fail to callback on server change", K(server));
        } else {
          LOG_INFO("callback on server change succeed", K(server));
        }
      }
    }
  }
  return ret;
}

int ObServerManager::delete_server(const ObIArray<ObAddr>& servers, const ObZone& zone)
{
  int ret = OB_SUCCESS;
  // avoid maintain operation run concurrently
  SpinWLockGuard guard(maintaince_lock_);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (servers.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("empty server array", K(servers), K(ret));
  } else {
    FOREACH_CNT_X(s, servers, OB_SUCC(ret))
    {
      if (!s->is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid server", K(ret), "server", *s);
      }
    }
  }
  FOREACH_CNT_X(s, servers, OB_SUCC(ret))
  {
    const ObAddr& server = *s;
    common::ObMySQLTransaction trans;
    ObServerStatus new_server_status;
    {
      SpinRLockGuard guard(server_status_rwlock_);
      ObServerStatus* server_status = NULL;
      if (OB_FAIL(find(server, server_status))) {
        LOG_WARN("find failed", K(server), K(ret));
      } else if (NULL == server_status) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("server_status is null", "server_status ptr", OB_P(server_status), K(ret));
      } else if (!zone.is_empty() && server_status->zone_ != zone) {
        ret = OB_SERVER_ZONE_NOT_MATCH;
        LOG_WARN("server zone not match", "server zone", server_status->zone_, K(zone), K(ret));
      } else if (ObServerStatus::OB_SERVER_ADMIN_DELETING == server_status->admin_status_ ||
                 ObServerStatus::OB_SERVER_ADMIN_TAKENOVER_BY_RS == server_status->admin_status_) {
        ret = OB_SERVER_ALREADY_DELETED;
        LOG_WARN("server has already been deleted, can not delete again", K(ret));
      } else if (OB_FAIL(trans.start(&(st_operator_.get_proxy())))) {
        LOG_WARN("failed to start trans", K(ret));
      } else {
        new_server_status = *server_status;
      }
    }
    if (OB_FAIL(ret)) {
    } else {
      new_server_status.admin_status_ = ObServerStatus::OB_SERVER_ADMIN_DELETING;
      ROOTSERVICE_EVENT_ADD("server", "delete_server", K(server));
      LOG_INFO("delete server, server change status to deleting", K(server), K(zone));
      char ip_buf[common::MAX_IP_ADDR_LENGTH];
      (void)server.ip_to_string(ip_buf, common::MAX_IP_ADDR_LENGTH);
      int64_t job_id = RS_JOB_CREATE(DELETE_SERVER, trans, "svr_ip", ip_buf, "svr_port", server.get_port());
      if (job_id < 1) {
        ret = OB_SQL_OPT_ERROR;
        LOG_WARN("insert into all_rootservice_job failed ", K(ret));
      } else if (OB_FAIL(st_operator_.update_status(
                     server, new_server_status.get_display_status(), new_server_status.last_hb_time_, trans))) {
        LOG_WARN("st_operator update_status failed", K(server), K(new_server_status), K(ret));
      } else {
        const ObServerStatus::ServerAdminStatus status = ObServerStatus::OB_SERVER_ADMIN_DELETING;
        const bool remove = false;
        if (OB_FAIL(update_admin_status(server, status, remove))) {
          LOG_WARN("update admin status failed", K(ret), K(server), K(status), K(remove));
        }
      }
    }
    if (trans.is_started()) {
      int tmp_ret = OB_SUCCESS;
      const bool commit = OB_SUCC(ret);
      if (OB_SUCCESS != (tmp_ret = trans.end(commit))) {
        LOG_WARN("trans end failed", K(tmp_ret), K(commit));
        ret = OB_SUCC(ret) ? tmp_ret : ret;
      }
    }
  }
  return ret;
}

int ObServerManager::end_delete_server(const ObAddr& server, const ObZone& zone, const bool commit)
{
  int ret = OB_SUCCESS;
  common::ObMySQLTransaction trans;
  // avoid maintain operation run concurrently
  SpinWLockGuard guard(maintaince_lock_);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), K(ret));
  } else {
    ObServerStatus copy;
    {
      SpinRLockGuard guard(server_status_rwlock_);
      ObServerStatus* server_status = NULL;
      if (OB_FAIL(find(server, server_status))) {
        LOG_WARN("find failed", K(server), K(ret));
      } else if (NULL == server_status) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("server_status is null", "server_status ptr", OB_P(server_status), K(ret));
      } else if (!zone.is_empty() && server_status->zone_ != zone) {
        ret = OB_SERVER_ZONE_NOT_MATCH;
        LOG_WARN("server zone not match", "server zone", server_status->zone_, K(zone), K(ret));
      } else if (ObServerStatus::OB_SERVER_ADMIN_TAKENOVER_BY_RS == server_status->admin_status_) {
        ret = OB_STATE_NOT_MATCH;
        LOG_WARN("server state not match", K(ret), K(server), K(zone));
      } else if (ObServerStatus::OB_SERVER_ADMIN_DELETING != server_status->admin_status_) {
        ret = OB_SERVER_NOT_DELETING;
        LOG_ERROR("server not in deleting status, can not cancel", "server_status", *server_status);
      } else {
        copy = *server_status;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(trans.start(&(st_operator_.get_proxy())))) {
      LOG_WARN("failed to start transaction", K(ret));
    } else {
      const ObServerStatus::ServerAdminStatus status = ObServerStatus::OB_SERVER_ADMIN_NORMAL;
      const bool remove = commit;
      copy.admin_status_ = status;
      if (!remove) {
        bool server_empty = false;
        if (OB_FAIL(st_operator_.update_status(server, copy.get_display_status(), copy.last_hb_time_, trans))) {
          LOG_WARN("st_operator update_status failed", "server_status", copy, K(ret));
        } else if (OB_FAIL(unit_mgr_->check_server_empty(copy.server_, server_empty))) {
          LOG_WARN("check server empty failed", "server", copy.server_, K(ret));
        } else if (!server_empty) {
          if (OB_FAIL(unit_mgr_->cancel_migrate_out_units(server))) {
            LOG_WARN("unit_mgr cancel_migrate_out_units failed", K(server), K(ret));
          }
        }
      } else {
        if (OB_FAIL(st_operator_.remove(server, trans))) {
          LOG_WARN("st_operator remove failed", K(server), K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        // complete the job
        char ip_buf[common::MAX_IP_ADDR_LENGTH];
        (void)server.ip_to_string(ip_buf, common::MAX_IP_ADDR_LENGTH);
        ObRsJobInfo job_info;
        ret = RS_JOB_FIND(job_info,
            trans,
            "job_type",
            "DELETE_SERVER",
            "job_status",
            "INPROGRESS",
            "svr_ip",
            ip_buf,
            "svr_port",
            server.get_port());
        if (OB_SUCC(ret) && job_info.job_id_ > 0) {
          int tmp_ret = commit ? OB_SUCCESS : OB_CANCELED;
          if (OB_FAIL(RS_JOB_COMPLETE(job_info.job_id_, tmp_ret, trans))) {
            LOG_WARN("all_rootservice_job update failed", K(ret), K(server));
          }
        } else {
          LOG_WARN("failed to find job", K(ret), K(server));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(update_admin_status(server, status, remove))) {
          LOG_WARN("update admin status failed", K(ret), K(server), K(status), K(remove));
        }
      }
    }
    if (trans.is_started()) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
        LOG_WARN("failed to end trans", K(tmp_ret));
        ret = OB_SUCC(ret) ? tmp_ret : ret;
      }
    }

    if (OB_SUCC(ret)) {
      if (commit) {
        // delete associated records from __all_clog_history_info_v2 about this server
        clog::ObClogHistoryReporter::get_instance().delete_server_record(server);
        ROOTSERVICE_EVENT_ADD("server", "finish_delete_server", K(server));
      } else {
        ROOTSERVICE_EVENT_ADD("server", "cancel_delete_server", K(server));
      }
      LOG_INFO("end delete server", K(server), K(commit));
      int tmp = server_change_callback_->on_server_change();
      if (OB_SUCCESS != tmp) {
        LOG_WARN("fail to callback on server change", K(ret));
      } else {
        LOG_WARN("callback on server change success", K(server));
      }
    }
  }
  return ret;
}

int ObServerManager::add_server_list(const ObServerInfoList& server_list, uint64_t& server_id)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(server_status_rwlock_);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (server_list.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("server_list is empty", K(server_list), K(ret));
  } else {
    const int64_t now = ObTimeUtility::current_time();
    for (int64_t i = 0; OB_SUCC(ret) && i < server_list.count(); ++i) {
      server_id = OB_INIT_SERVER_ID;
      ObServerStatus* server_status = NULL;
      if (OB_SUCC(find(server_list.at(i).server_, server_status))) {
        if (NULL == server_status) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("server_status is null", "server_status ptr", OB_P(server_status), K(ret));
        } else if (server_status->id_ > server_id) {
          server_id = server_status->id_;
        }
      } else if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("find failed", "server", server_list.at(i).server_, K(ret));
      } else {
        ret = OB_SUCCESS;
        bool server_id_used = false;
        while (OB_SUCC(ret)) {
          if (OB_FAIL(check_server_id_used(server_id, server_id_used))) {
            LOG_WARN("check server_id_used failed", K(server_id), K(ret));
          } else if (server_id_used) {
            ++server_id;
          } else {
            break;
          }
        }
        if (OB_SUCC(ret)) {
          ObServerStatus new_server_status;
          new_server_status.id_ = server_id;
          new_server_status.server_ = server_list.at(i).server_;
          new_server_status.zone_ = server_list.at(i).zone_;
          new_server_status.admin_status_ = ObServerStatus::OB_SERVER_ADMIN_NORMAL;
          new_server_status.hb_status_ = ObServerStatus::OB_HEARTBEAT_LEASE_EXPIRED;
          new_server_status.lease_expire_time_ = now + ObLeaseRequest::SERVICE_LEASE;
          new_server_status.last_hb_time_ = now - config_->lease_time;
          new_server_status.with_partition_ = true;
          if (OB_FAIL(server_statuses_.push_back(new_server_status))) {
            BOOTSTRAP_LOG(WARN, "fail to push back server status", K(ret), K(new_server_status));
          } else {
            BOOTSTRAP_LOG(INFO, "add server list success", K(new_server_status));
            ROOTSERVICE_EVENT_ADD("server", "add_server", "server", new_server_status.server_);
            int tmp = server_change_callback_->on_server_change();
            if (OB_SUCCESS != tmp) {
              LOG_WARN("fail to callback on server change", K(ret));
            } else {
              LOG_WARN("callback on add server success");
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObServerManager::start_server_list(const ObServerList& server_list, const ObZone& zone)
{
  int ret = OB_SUCCESS;
  int ret_bak = OB_SUCCESS;
  int success_count = 0;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (server_list.count() <= 0) {
    // zone can be empty, so we don't check zone here
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("server_list is empty", K(server_list), K(ret));
  } else {
    FOREACH(server, server_list)
    {
      // ignore the error, continue for servers that can be started.
      if (OB_FAIL(start_server(*server, zone))) {
        if (OB_SUCCESS == ret_bak) {
          ret_bak = ret;
        }
        LOG_WARN("start server failed", "server", *server, K(ret));
      } else {
        ++success_count;
      }
    }
    if (success_count > 0) {
      leader_coordinator_->signal();
    }
    ret = ret_bak;
  }
  return ret;
}

int ObServerManager::stop_server_list(const ObServerList& server_list, const ObZone& zone)
{
  int ret = OB_SUCCESS;
  int ret_bak = OB_SUCCESS;
  int success_count = 0;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (server_list.count() <= 0) {
    // zone can be empty, so we don't check zone here
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("server_list is empty", K(server_list), K(ret));
  } else {
    FOREACH(server, server_list)
    {
      // ignore the error, continue for servers that can be stopped.
      if (OB_FAIL(stop_server(*server, zone))) {
        if (OB_SUCCESS == ret_bak) {
          ret_bak = ret;
        }
        LOG_WARN("stop server failed", "server", *server, K(ret));
      } else {
        ++success_count;
      }
    }
    if (success_count > 0) {
      leader_coordinator_->signal();
    }
    ret = ret_bak;
  }
  return ret;
}

int ObServerManager::start_server(const ObAddr& server, const ObZone& zone)
{
  int ret = OB_SUCCESS;
  const bool is_start = true;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), K(ret));
  } else if (OB_FAIL(start_or_stop_server(server, zone, is_start))) {
    LOG_WARN("start server failed", K(server), K(zone), K(is_start), K(ret));
  } else {
    ROOTSERVICE_EVENT_ADD("server", "start_server", K(server));
  }
  return ret;
}

int ObServerManager::is_server_stopped(const ObAddr& server, bool& is_stopped) const
{
  int ret = OB_SUCCESS;
  is_stopped = false;
  bool zone_active = false;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("server manager has not inited", K(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), K(ret));
  } else {
    SpinRLockGuard guard(server_status_rwlock_);
    const ObServerStatus* status_ptr = NULL;
    if (OB_FAIL(find(server, status_ptr))) {
      LOG_WARN("find failed", K(server), K(ret));
    } else if (NULL == status_ptr) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("status_ptr is null", "status_ptr", OB_P(status_ptr), K(ret));
    } else if (OB_FAIL(zone_mgr_->check_zone_active(status_ptr->zone_, zone_active))) {
      LOG_WARN("fail to check zone active", K(ret), K(server));
    } else {
      is_stopped = status_ptr->is_stopped() || !zone_active;
    }
  }
  return ret;
}

int ObServerManager::get_server_leader_cnt(const ObAddr& server, int64_t& leader_cnt) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("server manager has not init", K(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(server));
  } else {
    SpinRLockGuard guard(server_status_rwlock_);
    const ObServerStatus* status_ptr = NULL;
    if (OB_FAIL(find(server, status_ptr))) {
      LOG_WARN("find failed", K(ret), K(server));
    } else if (NULL == status_ptr) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("status ptr is null", K(ret), KP(status_ptr));
    } else {
      leader_cnt = status_ptr->leader_cnt_;
    }
  }
  return ret;
}

int ObServerManager::stop_server(const ObAddr& server, const ObZone& zone)
{
  int ret = OB_SUCCESS;
  const bool is_start = false;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), K(ret));
  } else if (OB_FAIL(start_or_stop_server(server, zone, is_start))) {
    LOG_WARN("stop server failed", K(server), K(zone), K(is_start), K(ret));
  } else {
    ROOTSERVICE_EVENT_ADD("server", "stop_server", K(server));
  }
  return ret;
}

int ObServerManager::update_leader_cnt_status(ObServerSwitchLeaderInfoStat::ServerSwitchLeaderInfoMap& info_map)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(maintaince_lock_);
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    SpinWLockGuard guard(server_status_rwlock_);
    if (info_map.size() <= 0) {
      // leader coordinator not in working status, shall not enter the branch
    } else {
      ObArray<int64_t> tmp_leader_cnt;
      FOREACH_CNT_X(server_status, server_statuses_, OB_SUCC(ret))
      {
        const ObAddr& server = server_status->server_;
        ObServerSwitchLeaderInfoStat::ServerSwitchLeaderInfo info;
        ret = info_map.get_refactored(server, info);
        if (OB_SUCCESS == ret) {
          if (OB_FAIL(tmp_leader_cnt.push_back(info.leader_count_))) {
            LOG_WARN("fail to push back", K(ret));
          }
        } else if (OB_HASH_NOT_EXIST == ret) {
          // rewrite ret
          if (OB_FAIL(tmp_leader_cnt.push_back(0))) {
            LOG_WARN("fail to push back", K(ret));
          }
        } else {
          LOG_WARN("fail to get server leader info", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (tmp_leader_cnt.count() != server_statuses_.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("count not match", K(ret), "tmp cnt", tmp_leader_cnt.count(), "server cnt", server_statuses_.count());
      } else {
        for (int64_t i = 0; i < tmp_leader_cnt.count(); ++i) {
          server_statuses_.at(i).leader_cnt_ = tmp_leader_cnt.at(i);
        }
      }
    }
  }
  return ret;
}

int ObServerManager::start_or_stop_server(const ObAddr& server, const ObZone& zone, const bool is_start)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(maintaince_lock_);
  int64_t stop_time = -1;
  bool need_update = true;
  ObServerStatus* server_status = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("server is invalid", K(server), K(ret));
  } else {
    SpinRLockGuard guard(server_status_rwlock_);
    if (OB_FAIL(find(server, server_status))) {
      LOG_WARN("find failed", K(server), K(ret));
    } else if (NULL == server_status) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("server_status is null", "server_status ptr", OB_P(server_status), K(ret));
    } else if (!zone.is_empty() && server_status->zone_ != zone) {
      ret = OB_SERVER_ZONE_NOT_MATCH;
      LOG_WARN("server zone not match", "server zone", server_status->zone_, K(zone), K(ret));
    } else {
      if (is_start) {
        if (!server_status->is_stopped()) {
          need_update = false;
        } else {
          stop_time = 0;
        }
      } else {
        bool other_zone_stopped = false;
        if (server_status->is_stopped()) {
          need_update = false;
        } else {
          stop_time = ObTimeUtility::current_time();
        }
      }
    }
  }
  if (OB_SUCCESS == ret && need_update) {
    if (OB_FAIL(st_operator_.update_stop_time(server, stop_time))) {
      LOG_WARN("update stop time failed", K(server), K(ret));
    } else {
      SpinWLockGuard guard(server_status_rwlock_);
      server_status->stop_time_ = stop_time;
    }
  }

  return ret;
}

int ObServerManager::expend_server_lease(const common::ObAddr& server, const int64_t new_lease_end)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(server_status_rwlock_);
  ObServerStatus* status_ptr = NULL;
  if (OB_UNLIKELY(!server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(server));
  } else if (OB_FAIL(find(server, status_ptr))) {
    LOG_WARN("fail to find server", K(ret));
  } else if (OB_UNLIKELY(NULL == status_ptr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("status ptr is null", K(ret), K(server));
  } else if (status_ptr->lease_expire_time_ <= new_lease_end) {
    status_ptr->lease_expire_time_ = new_lease_end;
  } else {
  }  // no need to update lease_expire_time
  return ret;
}

void ObServerManager::clear_in_recovery_server_takenover_by_rs(const common::ObAddr& server)
{
  SpinWLockGuard guard(server_status_rwlock_);
  ObServerStatus* status_ptr = NULL;
  int tmp_ret = find(server, status_ptr);
  if (OB_SUCCESS == tmp_ret) {
    status_ptr->in_recovery_for_takenover_by_rs_ = false;
    ;
  } else if (OB_ENTRY_NOT_EXIST != tmp_ret) {
    LOG_WARN("find failed", K(server), K(tmp_ret));
  } else {
    LOG_WARN("fail to find server", K(server), K(tmp_ret));
  }
}

int ObServerManager::receive_hb(
    const ObLeaseRequest& lease_request, uint64_t& server_id, bool& to_alive, bool& update_delay_time_flag)
{
  int ret = OB_SUCCESS;
  to_alive = false;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("server manager has not inited", K(ret));
  } else if (!lease_request.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid lease_request", K(lease_request), K(ret));
  } else {
    SpinWLockGuard guard(server_status_rwlock_);
    const bool with_rootserver = rs_addr_ == lease_request.server_;
    ObServerStatus* status_ptr = NULL;
    if (OB_FAIL(find(lease_request.server_, status_ptr))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("find failed", "server", lease_request.server_, K(ret));
      } else {
        // overwrite ret on purpose
        ret = OB_SERVER_NOT_IN_WHITE_LIST;
        LOG_WARN("server not in white list", "server", lease_request.server_, K(ret));
        if (!has_build_) {
          LOG_INFO("accept server heartbeat before white list loaded", "server", lease_request.server_);
          ret = OB_SUCCESS;
        }
      }
    } else if (NULL == status_ptr) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("status_ptr is null", "status_ptr", OB_P(status_ptr), K(ret));
    } else if (ObServerStatus::OB_SERVER_ADMIN_TAKENOVER_BY_RS == status_ptr->admin_status_) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("server taken over by rs, state not match", K(ret), "server_status", *status_ptr);
    } else if (!status_ptr->zone_.is_empty() && status_ptr->zone_ != lease_request.zone_) {
      ret = OB_SERVER_ZONE_NOT_MATCH;
      LOG_WARN("server zone not match", "zone", status_ptr->zone_, "lease zone", lease_request.zone_, K(ret));
    } else {
      // if force_stop_hb is true then won't extend server's last_hb_time_
      const int64_t now =
          status_ptr->force_stop_hb_ ? status_ptr->last_hb_time_ : ::oceanbase::common::ObTimeUtility::current_time();
      ObServerStatus::HeartBeatStatus old_hb_status = status_ptr->hb_status_;
      server_id = status_ptr->id_;
      if (status_ptr->with_rootserver_ != with_rootserver) {
        LOG_INFO("server change with_rootserver",
            "old with_rootserver",
            status_ptr->with_rootserver_,
            "new with_rootserver",
            with_rootserver);
        if (with_rootserver) {
          if (OB_FAIL(reset_existing_rootserver())) {
            LOG_WARN("reset_existing_rootserver failed", K(ret));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(set_server_status(lease_request, now, with_rootserver, *status_ptr))) {
          LOG_WARN("set server status failed", K(lease_request), K(now), K(with_rootserver), K(ret));
        } else if (OB_FAIL(status_change_callback_->on_server_status_change(status_ptr->server_))) {
          LOG_WARN("commit server change with_rootserver task failed", "server", status_ptr->server_, K(ret));
        }
      } else if (ObServerStatus::OB_HEARTBEAT_ALIVE != status_ptr->hb_status_ && !status_ptr->force_stop_hb_) {
        const char* hb_status_str = NULL;
        // for logging, ignore result and do not check NULL string.
        int tmp_ret = ObServerStatus::heartbeat_status_str(status_ptr->hb_status_, hb_status_str);
        if (OB_SUCCESS != tmp_ret) {
          LOG_WARN("heartbeat status to string failed", K(tmp_ret), "hb_status", status_ptr->hb_status_);
        } else {
          if (OB_FAIL(set_server_status(lease_request, now, with_rootserver, *status_ptr))) {
            LOG_WARN("set server status failed", K(lease_request), K(now), K(with_rootserver), K(ret));
          } else {
            LOG_INFO("server alive again",
                "server",
                status_ptr->server_,
                "last hb status",
                hb_status_str,
                "last hb time",
                status_ptr->last_hb_time_);
            status_ptr->register_time_ = now;
            // commit server online task
            // ignore wakeup balancer and wakeup daily merger failed
            int temp_ret = OB_SUCCESS;
            if (OB_SUCCESS != (temp_ret = status_change_callback_->wakeup_balancer())) {
              LOG_WARN("wakeup_balancer failed", K(temp_ret));
            } else if (OB_SUCCESS != (temp_ret = status_change_callback_->wakeup_daily_merger())) {
              LOG_WARN("wakeup_daily_merger failed", K(temp_ret));
            }
            if (OB_FAIL(status_change_callback_->on_server_status_change(status_ptr->server_))) {
              LOG_WARN("commit new server online task failed", "server", status_ptr->server_, K(ret));
            } else {
              ROOTSERVICE_EVENT_ADD("server", "online", "server", status_ptr->server_);
            }
          }
        }
      } else if (0 != MEMCMP(status_ptr->build_version_, lease_request.build_version_, OB_SERVER_VERSION_LENGTH)) {
        LOG_INFO("server change build_version",
            "old svn version",
            status_ptr->build_version_,
            "new svn version",
            lease_request.build_version_);
        // commit server svn version change
        if (OB_FAIL(set_server_status(lease_request, now, with_rootserver, *status_ptr))) {
          LOG_WARN("set server status failed", K(lease_request), K(now), K(with_rootserver), K(ret));
        } else if (OB_FAIL(status_change_callback_->on_server_status_change(status_ptr->server_))) {
          LOG_WARN("commit server change build_version task failed", "server", status_ptr->server_, K(ret));
        }
      } else if (status_ptr->ssl_key_expired_time_ != lease_request.ssl_key_expired_time_) {
        LOG_INFO("server ssl_key_expired_time changed",
            "old ssl_key_expired_time",
            status_ptr->ssl_key_expired_time_,
            "new ssl_key_expired_time",
            lease_request.ssl_key_expired_time_);
        // commit server svn version change
        if (OB_FAIL(set_server_status(lease_request, now, with_rootserver, *status_ptr))) {
          LOG_WARN("set server status failed", K(lease_request), K(now), K(with_rootserver), K(ret));
        } else if (OB_FAIL(status_change_callback_->on_server_status_change(status_ptr->server_))) {
          LOG_WARN("commit server change build_version task failed", "server", status_ptr->server_, K(ret));
        }
      } else if (status_ptr->start_service_time_ != lease_request.start_service_time_) {
        LOG_INFO("server change start_service_time",
            "old start_service_time",
            status_ptr->start_service_time_,
            "new start_service_time",
            lease_request.start_service_time_);
        if (OB_FAIL(set_server_status(lease_request, now, with_rootserver, *status_ptr))) {
          LOG_WARN("set server status failed", K(lease_request), K(now), K(with_rootserver), K(ret));
        } else {
          // ignore wakeup balancer and wakeup daily merger failed
          int temp_ret = OB_SUCCESS;
          if (OB_SUCCESS != (temp_ret = status_change_callback_->wakeup_balancer())) {
            LOG_WARN("wakeup_balancer failed", K(temp_ret));
          } else if (OB_SUCCESS != (temp_ret = status_change_callback_->wakeup_daily_merger())) {
            LOG_WARN("wakeup_daily_merger failed", K(temp_ret));
          }
          if (OB_FAIL(status_change_callback_->on_server_status_change(status_ptr->server_))) {
            LOG_WARN("commit server change start_service_time task failed", "server", status_ptr->server_, K(ret));
          } else {
            if (0 == status_ptr->start_service_time_ && 0 != lease_request.start_service_time_) {
              ROOTSERVICE_EVENT_ADD("server", "start_service", "server", status_ptr->server_);
            }
          }
        }
      } else if (status_ptr->server_report_status_ != lease_request.server_status_) {
        LOG_INFO("server report status change",
            "old report status",
            status_ptr->server_report_status_,
            "new report status",
            lease_request.server_status_);
        if (OB_FAIL(set_server_status(lease_request, now, with_rootserver, *status_ptr))) {
          LOG_WARN("set server status failed", K(ret), K(lease_request), K(now), K(with_rootserver));
        } else if (OB_FAIL(process_report_status_change(lease_request, *status_ptr))) {
          LOG_WARN("fail to proc report status change", K(ret));
        } else {
          status_ptr->server_report_status_ = lease_request.server_status_;
        }
      } else if (status_ptr->resource_info_ != lease_request.resource_info_) {
        LOG_INFO("server resource changed",
            "old_resource_info",
            status_ptr->resource_info_,
            "new_resource_info",
            lease_request.resource_info_);
        if (OB_FAIL(set_server_status(lease_request, now, with_rootserver, *status_ptr))) {
          LOG_WARN("set server status failed", K(lease_request), K(now), K(with_rootserver), K(ret));
        }
      } else {
        status_ptr->last_hb_time_ = now;
        if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_1450) {
          // for compatibility
        } else if (update_delay_time_flag) {
          const int64_t current_rs_time = ObTimeUtility::current_time();
          int64_t server_behind_time =
              current_rs_time - (lease_request.current_server_time_ + lease_request.round_trip_time_ / 2);
          if (std::abs(server_behind_time) > GCONF.rpc_timeout) {
            LOG_WARN(
                "clock between rs and server not sync", "ret", OB_ERR_UNEXPECTED, K(lease_request), K(current_rs_time));
          }
          if (OB_FAIL(set_server_delay_time(server_behind_time, lease_request.round_trip_time_, *status_ptr))) {
            LOG_WARN("set server delay time failed",
                K(ret),
                K(server_behind_time),
                "round_trip_time",
                lease_request.round_trip_time_);
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (ObServerStatus::OB_HEARTBEAT_ALIVE == status_ptr->hb_status_ &&
            ObServerStatus::OB_HEARTBEAT_ALIVE != old_hb_status) {
          to_alive = true;
        }
      }
    }
  }
  return ret;
}

int ObServerManager::set_server_delay_time(
    const int64_t server_behind_time, const int64_t round_trip_time, ObServerStatus& server_status)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (round_trip_time < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("round_trip_time is invalid", K(ret), K(round_trip_time));
  } else {
    server_status.last_server_behind_time_ = server_behind_time;
    server_status.last_round_trip_time_ = round_trip_time;
  }
  return ret;
}

int ObServerManager::process_report_status_change(
    const share::ObLeaseRequest& lease_request, share::ObServerStatus& server_status)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!lease_request.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid lease request or invalid hb_timestamp", K(ret), K(lease_request));
  } else if (lease_request.server_status_ == server_status.server_report_status_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("status not match", K(ret), K(lease_request), K(server_status));
  } else if (lease_request.server_status_ == LEASE_REQUEST_NORMAL) {
    if (0 != server_status.stop_time_) {
      if (OB_FAIL(status_change_callback_->on_start_server(server_status.server_))) {
        LOG_WARN("fail to on start server", K(ret));
      }
    } else {
    }  // server not start, no need to restart
  } else if (lease_request.server_status_ != LEASE_REQUEST_NORMAL) {
    if (0 == server_status.stop_time_) {
      if (OB_FAIL(status_change_callback_->on_stop_server(server_status.server_))) {
        LOG_WARN("fail to on stop server", K(ret));
      }
    } else {
    }  // server already stop, no need to stop again
  }
  return ret;
}

int ObServerManager::set_server_status(const ObLeaseRequest& lease_request, const int64_t hb_timestamp,
    const bool with_rootserver, ObServerStatus& server_status)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!lease_request.is_valid() || hb_timestamp <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid lease_request or invalid hb_timestamp", K(lease_request), K(hb_timestamp), K(ret));
  } else {
    server_status.zone_ = lease_request.zone_;
    MEMCPY(server_status.build_version_, lease_request.build_version_, OB_SERVER_VERSION_LENGTH);
    server_status.server_ = lease_request.server_;
    server_status.sql_port_ = lease_request.inner_port_;
    server_status.last_hb_time_ = hb_timestamp;
    server_status.hb_status_ = ObServerStatus::OB_HEARTBEAT_ALIVE;
    server_status.with_rootserver_ = with_rootserver;
    server_status.resource_info_ = lease_request.resource_info_;
    server_status.start_service_time_ = lease_request.start_service_time_;
    server_status.ssl_key_expired_time_ = lease_request.ssl_key_expired_time_;
  }
  return ret;
}

int ObServerManager::reset_existing_rootserver()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    FOREACH_CNT(server_status, server_statuses_)
    {
      if (server_status->with_rootserver_) {
        server_status->with_rootserver_ = false;
      }
    }
  }
  return ret;
}

int ObServerManager::update_admin_status(
    const ObAddr& server, const ObServerStatus::ServerAdminStatus status, const bool remove)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard inner_guard(server_status_rwlock_);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), K(ret));
  } else if (status < ObServerStatus::OB_SERVER_ADMIN_NORMAL || status >= ObServerStatus::OB_SERVER_ADMIN_MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid status", K(status), K(ret));
  } else {
    int64_t idx = OB_INVALID_INDEX;
    for (int64_t i = 0; OB_INVALID_INDEX == idx && i < server_statuses_.count(); ++i) {
      if (server_statuses_.at(i).server_ == server) {
        idx = i;
      }
    }
    if (OB_INVALID_INDEX == idx) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("server is expected to exist", K(server), K(ret));
    } else {
      if (!remove) {
        if (ObServerStatus::OB_SERVER_ADMIN_DELETING == server_statuses_[idx].admin_status_ &&
            ObServerStatus::OB_SERVER_ADMIN_NORMAL == status) {
          server_statuses_[idx].force_stop_hb_ = false;
        }
        server_statuses_[idx].admin_status_ = status;
      } else {
        if (OB_FAIL(server_statuses_.remove(idx))) {
          LOG_WARN("remove status failed", K(ret), "size", server_statuses_.count(), K(idx));
        }
      }
    }
  }
  return ret;
}

int ObServerManager::check_server_permanent_offline(const ObAddr& server, bool& is_offline) const
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("server manager has not inited", K(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), K(ret));
  } else {
    SpinRLockGuard guard(server_status_rwlock_);
    is_offline = false;
    const ObServerStatus* status_ptr = NULL;
    if (OB_FAIL(find(server, status_ptr))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("find failed", K(server), K(ret));
      } else {
        ret = OB_SUCCESS;
        is_offline = false;
        LOG_DEBUG("treat not exist server as not alive", K(server));
      }
    } else if (NULL == status_ptr) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("status_ptr is null", "status_ptr", OB_P(status_ptr), K(ret));
    } else {
      is_offline = status_ptr->is_permanent_offline();
    }
  }
  return ret;
}

int ObServerManager::check_server_alive(const ObAddr& server, bool& is_alive) const
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("server manager has not inited", K(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), K(ret));
  } else {
    SpinRLockGuard guard(server_status_rwlock_);
    is_alive = false;
    const ObServerStatus* status_ptr = NULL;
    if (OB_FAIL(find(server, status_ptr))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("find failed", K(server), K(ret));
      } else {
        ret = OB_SUCCESS;
        is_alive = false;
        LOG_DEBUG("treat not exist server as not alive", K(server));
      }
    } else if (NULL == status_ptr) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("status_ptr is null", "status_ptr", OB_P(status_ptr), K(ret));
    } else {
      is_alive = status_ptr->is_alive();
    }
  }
  return ret;
}

int ObServerManager::check_server_active(const ObAddr& server, bool& is_active) const
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("server manager has not inited", K(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), K(ret));
  } else {
    SpinRLockGuard guard(server_status_rwlock_);
    is_active = false;
    const ObServerStatus* status_ptr = NULL;
    if (OB_FAIL(find(server, status_ptr))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("find failed", K(server), K(ret));
      } else {
        ret = OB_SUCCESS;
        is_active = false;
        LOG_INFO("treat not exist server as not active", K(server));
      }
    } else if (NULL == status_ptr) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("status_ptr is null", "status_ptr", OB_P(status_ptr), K(ret));
    } else {
      is_active = status_ptr->is_active();
    }
  }
  return ret;
}

int ObServerManager::check_server_stopped(const common::ObAddr& server, bool& is_stopped) const
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("server manager has not inited", K(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), K(ret));
  } else {
    SpinRLockGuard guard(server_status_rwlock_);
    is_stopped = true;
    const ObServerStatus* status_ptr = NULL;
    if (OB_FAIL(find(server, status_ptr))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("find failed", K(server), K(ret));
      } else {
        ret = OB_SUCCESS;
        is_stopped = true;
        LOG_INFO("treat server that is not exist as stopped", K(server));
      }
    } else if (NULL == status_ptr) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("status_ptr is null", "status_ptr", OB_P(status_ptr), K(ret));
    } else {
      is_stopped = status_ptr->is_stopped();
    }
  }
  return ret;
}

int ObServerManager::check_server_valid_for_partition(const common::ObAddr& server, bool& is_valid) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("server manager has not inited", KR(ret));
  } else if (OB_UNLIKELY(!server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", KR(ret), K(server));
  } else {
    SpinRLockGuard guard(server_status_rwlock_);
    is_valid = false;
    bool zone_active = false;
    const ObServerStatus* status_ptr = NULL;
    if (OB_FAIL(find(server, status_ptr))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("failed to find server", KR(ret), K(server));
      } else {
        ret = OB_SUCCESS;
        LOG_INFO("server not exist, not valid for partition", K(server));
      }
    } else if (OB_ISNULL(status_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("status_ptr is null", KR(ret), "status_ptr", OB_P(status_ptr));
    } else if (OB_FAIL(zone_mgr_->check_zone_active(status_ptr->zone_, zone_active))) {
      LOG_WARN("fail to check zone active", K(ret), K(server));
    } else {
      is_valid = status_ptr->can_migrate_in() && !status_ptr->is_stopped() && zone_active;
    }
  }
  return ret;
}

int ObServerManager::get_servers_by_status(
    ObIServerArray& active_server_list, ObIServerArray& inactive_server_list) const
{
  ObZone empty_zone;
  return get_servers_by_status(empty_zone, active_server_list, inactive_server_list);
}

int ObServerManager::get_servers_by_status(
    const ObZone& zone, ObIServerArray& active_server_list, ObIServerArray& inactive_server_list) const
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("server manager has not inited", K(ret));
  } else {
    active_server_list.reuse();
    SpinRLockGuard guard(server_status_rwlock_);
    for (int64_t i = 0; OB_SUCC(ret) && i < server_statuses_.count(); ++i) {
      if (server_statuses_[i].zone_ == zone || zone.is_empty()) {
        if (server_statuses_[i].is_alive()) {
          ret = active_server_list.push_back(server_statuses_[i].server_);
          if (OB_FAIL(ret)) {
            LOG_WARN("push back to active_server_list failed", K(ret));
          }
        } else if (OB_FAIL(inactive_server_list.push_back(server_statuses_[i].server_))) {
          LOG_WARN("fail to push back to inactive_server_list", KR(ret));
        }
      }
    }
  }
  return ret;
}

int ObServerManager::get_servers_takenover_by_rs(const ObZone& zone, ObIServerArray& server_list) const
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("server manager has not inited", K(ret));
  } else {
    server_list.reuse();
    SpinRLockGuard guard(server_status_rwlock_);
    for (int64_t i = 0; OB_SUCC(ret) && i < server_statuses_.count(); ++i) {
      if (zone.is_empty() || server_statuses_[i].zone_ == zone) {
        if (ObServerStatus::OB_SERVER_ADMIN_TAKENOVER_BY_RS != server_statuses_[i].admin_status_) {
          // bypass since this server not taken over by rs
        } else if (OB_FAIL(server_list.push_back(server_statuses_[i].server_))) {
          LOG_WARN("fail to push back to server list", K(ret));
        }
      } else {
      }  // zone not match
    }
  }
  return ret;
}

int ObServerManager::get_alive_servers(const ObZone& zone, ObIServerArray& server_list) const
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("server manager has not inited", K(ret));
  } else {
    server_list.reuse();
    SpinRLockGuard guard(server_status_rwlock_);
    for (int64_t i = 0; OB_SUCC(ret) && i < server_statuses_.count(); ++i) {
      if ((server_statuses_[i].zone_ == zone || zone.is_empty()) && server_statuses_[i].is_alive()) {
        ret = server_list.push_back(server_statuses_[i].server_);
        if (OB_FAIL(ret)) {
          LOG_WARN("push back to server_list failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObServerManager::get_alive_server_count(const ObZone& zone, int64_t& count) const
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("server manager has not inited", K(ret));
  } else {
    count = 0;
    SpinRLockGuard guard(server_status_rwlock_);
    for (int64_t i = 0; i < server_statuses_.count(); ++i) {
      if ((server_statuses_[i].zone_ == zone || zone.is_empty()) && server_statuses_[i].is_alive()) {
        ++count;
      }
    }
  }
  return ret;
}

int ObServerManager::get_active_server_array(const common::ObZone& zone, ObIServerArray& active_server_array) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("server manager not inited", K(ret));
  } else {
    active_server_array.reset();
    SpinRLockGuard guard(server_status_rwlock_);
    for (int64_t i = 0; OB_SUCC(ret) && i < server_statuses_.count(); ++i) {
      if (!zone.is_empty() && zone != server_statuses_.at(i).zone_) {
        // zone not match, bypass
      } else if (server_statuses_.at(i).is_active()) {
        if (OB_FAIL(active_server_array.push_back(server_statuses_.at(i).server_))) {
          LOG_WARN("fail to push back", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObServerManager::get_active_server_count(const ObZone& zone, int64_t& count) const
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("server manager has not inited", K(ret));
  } else {
    count = 0;
    SpinRLockGuard guard(server_status_rwlock_);
    for (int64_t i = 0; i < server_statuses_.count(); ++i) {
      if ((server_statuses_[i].zone_ == zone || zone.is_empty()) && server_statuses_[i].is_active() &&
          server_statuses_[i].in_service()) {
        ++count;
      }
    }
  }
  return ret;
}

int ObServerManager::get_server_count(const ObZone& zone, int64_t& alive_count, int64_t& not_alive_count) const
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("server manager has not inited", K(ret));
  } else {
    alive_count = 0;
    not_alive_count = 0;
    SpinRLockGuard guard(server_status_rwlock_);
    for (int64_t i = 0; i < server_statuses_.count(); ++i) {
      if (server_statuses_[i].zone_ == zone || zone.is_empty()) {
        if (server_statuses_[i].is_alive()) {
          ++alive_count;
        } else {
          ++not_alive_count;
        }
      }
    }
  }
  return ret;
}

int ObServerManager::get_servers_of_zone(const common::ObZone& zone, ObServerArray& server_list) const
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("server manager has not inited", K(ret));
  } else {
    server_list.reuse();
    SpinRLockGuard guard(server_status_rwlock_);
    for (int64_t i = 0; OB_SUCC(ret) && i < server_statuses_.count(); ++i) {
      if ((server_statuses_[i].zone_ == zone || zone.is_empty())) {
        ret = server_list.push_back(server_statuses_[i].server_);
        if (OB_FAIL(ret)) {
          LOG_WARN("push back to server_list failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObServerManager::try_renew_rs_list()
{
  int ret = OB_SUCCESS;
  ObZone empty_zone;
  ObServerStatusArray statuses;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("server manager has not inited", K(ret));
  } else if (OB_FAIL(get_server_statuses(empty_zone, statuses))) {
    LOG_WARN("fail to get server statuses", KR(ret));
  } else {
    for (int64_t i = 0; i < statuses.count(); i++) {
      if (statuses.at(i).is_alive()) {
        // nothing todo
      } else if (OB_FAIL(status_change_callback_->on_offline_server(statuses.at(i).server_))) {
        LOG_WARN("fail to on offline server", KR(ret), "server", statuses.at(i).server_);
      }
      // ignore ret
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObServerManager::construct_not_empty_server_set(common::hash::ObHashSet<common::ObAddr>& not_empty_server_set)
{
  int ret = OB_SUCCESS;
  common::ObArray<common::ObAddr> server_array;
  const common::ObZone empty_zone;  // all server
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("server manager has not inited", K(ret));
  } else if (OB_ISNULL(unit_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unit mgr ptr is null", K(ret), KP(unit_mgr_));
  } else if (OB_FAIL(get_servers_of_zone(empty_zone, server_array))) {
    LOG_WARN("fail to get servers of zone", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < server_array.count(); ++i) {
      bool empty = false;
      const common::ObAddr& server = server_array.at(i);
      const int overwrite = 0;
      if (OB_UNLIKELY(!server.is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(ret), K(server));
      } else if (OB_FAIL(unit_mgr_->check_server_empty(server, empty))) {
        LOG_WARN("fail to check server empty", K(ret), K(server));
      } else if (empty) {
        // empty server and with no partition, do not need to set into empty server set
      } else {  // not empty server
        if (OB_FAIL(not_empty_server_set.set_refactored(server, overwrite))) {
          LOG_WARN("fail to set refactored", K(ret), K(server));
        }
      }
    }
  }
  return ret;
}

int ObServerManager::check_servers()
{
  int ret = OB_SUCCESS;
  /* construct server_set out of the server_status_wrlock_, avoiding deadlock with
   * unit_mgr_->check_server_empty
   */
  common::hash::ObHashSet<common::ObAddr> not_empty_server_set;
  const int64_t max_server_cnt = 5000;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("server manager has not inited", K(ret));
  } else if (OB_FAIL(not_empty_server_set.create(max_server_cnt))) {
    LOG_WARN("fail to create empty server set", K(ret));
  } else if (OB_FAIL(construct_not_empty_server_set(not_empty_server_set))) {
    LOG_WARN("fail to construct empty server set", K(ret));
  } else {
    SpinWLockGuard guard(server_status_rwlock_);
    const int64_t now = ::oceanbase::common::ObTimeUtility::current_time();
    for (int64_t i = 0; OB_SUCC(ret) && i < server_statuses_.count(); ++i) {
      const int64_t last_hb_time = server_statuses_[i].last_hb_time_;
      ObServerStatus::HeartBeatStatus hb_status = server_statuses_[i].hb_status_;
      if (now - last_hb_time >= config_->lease_time) {
        hb_status = ObServerStatus::OB_HEARTBEAT_LEASE_EXPIRED;
      }
      if (now - last_hb_time >= config_->server_permanent_offline_time) {
        // normal multiple zone deployment mode, when hb_ts exceeds permanent offline time,
        // we set the hb_status to permanent offline
        hb_status = ObServerStatus::OB_HEARTBEAT_PERMANENT_OFFLINE;
      }

      if (hb_status != server_statuses_[i].hb_status_) {
        // for logging, ignore result and do not check NULL string.
        const char* from_hb_status_str = NULL;
        const char* hb_status_str = NULL;
        int tmp_ret = ObServerStatus::heartbeat_status_str(server_statuses_[i].hb_status_, from_hb_status_str);
        if (OB_SUCCESS != tmp_ret) {
          LOG_WARN("heartbeat status to string failed", K(tmp_ret), "from_hb_status", server_statuses_[i].hb_status_);
        }
        tmp_ret = ObServerStatus::heartbeat_status_str(hb_status, hb_status_str);
        if (OB_SUCCESS != tmp_ret) {
          LOG_WARN("heartbeat status to string failed", K(tmp_ret), K(hb_status));
        }
        LOG_INFO("server hb status change",
            "server",
            server_statuses_[i].server_,
            "from_hb_status",
            from_hb_status_str,
            "to_hb_status",
            hb_status_str);
        server_statuses_[i].hb_status_ = hb_status;
        server_statuses_[i].start_service_time_ = 0;
        // ignore wakeup balancer and wakeup daily merger failed
        int temp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (temp_ret = status_change_callback_->wakeup_balancer())) {
          LOG_WARN("wakeup_balancer failed", K(temp_ret));
        } else if (OB_SUCCESS != (temp_ret = status_change_callback_->wakeup_daily_merger())) {
          LOG_WARN("wakeup_daily_merger failed", K(temp_ret));
        }
        ret = status_change_callback_->on_server_status_change(server_statuses_[i].server_);
        if (OB_FAIL(ret)) {
          LOG_WARN(
              "commit active to inactive status change task failed", "server", server_statuses_[i].server_, K(ret));
        } else {
          LOG_INFO("commit active to inactive status change task succeed", "server", server_statuses_[i].server_);
          if (ObServerStatus::OB_HEARTBEAT_LEASE_EXPIRED == hb_status) {
            ROOTSERVICE_EVENT_ADD("server", "lease_expire", "server", server_statuses_[i].server_);
          } else if (ObServerStatus::OB_HEARTBEAT_PERMANENT_OFFLINE == hb_status) {
            ROOTSERVICE_EVENT_ADD("server", "permanent_offline", "server", server_statuses_[i].server_);
          }
        }
      } else if (hb_status == ObServerStatus::OB_HEARTBEAT_PERMANENT_OFFLINE) {
      }

      const int64_t block_migrate_in_time = server_statuses_[i].block_migrate_in_time_;
      if (server_statuses_[i].is_migrate_in_blocked() && now - block_migrate_in_time >= GCONF.migration_disable_time) {
        LOG_INFO("server block migrate in long enough, unblock it",
            "server",
            server_statuses_[i].server_,
            "block_migrate_in_time",
            server_statuses_[i].block_migrate_in_time_,
            K(now));
        server_statuses_[i].unblock_migrate_in();
        // ignore wakeup balancer and wakeup daily merger failed
        int temp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (temp_ret = status_change_callback_->wakeup_balancer())) {
          LOG_WARN("wakeup_balancer failed", K(temp_ret));
        } else if (OB_SUCCESS != (temp_ret = status_change_callback_->wakeup_daily_merger())) {
          LOG_WARN("wakeup_daily_merger failed", K(temp_ret));
        }
        if (OB_FAIL(status_change_callback_->on_server_status_change(server_statuses_[i].server_))) {
          LOG_WARN("commit unblock migrate in task failed", "server", server_statuses_[i].server_, K(ret));
        } else {
          LOG_INFO("commit unblock migrate in task succeed", "server", server_statuses_[i].server_);
        }
      }
    }
  }
  return ret;
}

int ObServerManager::is_server_exist(const ObAddr& server, bool& exist) const
{
  int ret = OB_SUCCESS;
  exist = false;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("server manager has not inited", K(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), K(ret));
  } else {
    SpinRLockGuard guard(server_status_rwlock_);
    const ObServerStatus* status_ptr = NULL;
    if (OB_FAIL(find(server, status_ptr))) {
      LOG_WARN("find failed", K(server), K(ret));
      ret = OB_SUCCESS;
      exist = false;
    } else if (NULL == status_ptr) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("status_ptr is null", "status_ptr", OB_P(status_ptr), K(ret));
    } else {
      exist = true;
    }
  }
  return ret;
}

int ObServerManager::get_server_status(const ObAddr& server, ObServerStatus& server_status) const
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("server manager has not inited", K(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), K(ret));
  } else {
    SpinRLockGuard guard(server_status_rwlock_);
    const ObServerStatus* status_ptr = NULL;
    if (OB_FAIL(find(server, status_ptr))) {
      LOG_WARN("find failed", K(server), K(ret));
    } else if (NULL == status_ptr) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("status_ptr is null", "status_ptr", OB_P(status_ptr), K(ret));
    } else {
      server_status = *status_ptr;
    }
  }
  return ret;
}

int ObServerManager::update_server_status(const ObServerStatus& server_status)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("server manager has not inited", K(ret));
  } else if (!server_status.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server_status", K(server_status), K(ret));
  } else {
    SpinWLockGuard guard(server_status_rwlock_);
    ObServerStatus* status_ptr = NULL;
    if (OB_FAIL(find(server_status.server_, status_ptr))) {
      LOG_WARN("find failed", "server", server_status.server_, K(ret));
    } else if (NULL == status_ptr) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("status_ptr is null", "status_ptr", OB_P(status_ptr), K(ret));
    } else {
      *status_ptr = server_status;
    }
  }
  return ret;
}

int ObServerManager::load_server_manager()
{
  int ret = OB_SUCCESS;
  ObArray<share::ObServerStatus> statuses;
  SpinWLockGuard guard(maintaince_lock_);
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(st_operator_.get(statuses))) {
    LOG_WARN("get server statuses from all server failed", K(ret));
  } else if (OB_FAIL(load_server_statuses(statuses))) {
    LOG_WARN("server manager build from all server failed", K(ret), K(statuses));
  } else {
  }  // no more to do
  return ret;
}

int ObServerManager::load_server_statuses(const ObServerStatusArray& server_statuses)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("server manager has not inited", K(ret));
  } else if (server_statuses.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("server_statuses is empty", K(server_statuses), K(ret));
  } else {
    // to protect from executing concurrently with add_server(),
    SpinWLockGuard guard2(server_status_rwlock_);
    for (int64_t idx = server_statuses_.count() - 1; OB_SUCC(ret) && idx >= 0; --idx) {
      bool found = false;
      FOREACH_X(s, server_statuses, !found)
      {
        if (s->server_ == server_statuses_.at(idx).server_) {
          found = true;
        }
      }
      if (!found) {
        if (OB_FAIL(server_statuses_.remove(idx))) {
          LOG_WARN("remove server status failed", K(ret), K(idx));
        }
      }
    }

    FOREACH_X(s, server_statuses, OB_SUCCESS == ret)
    {
      ObServerStatus status = *s;
      ObServerStatus* exist_status = NULL;
      if (OB_FAIL(find(status.server_, exist_status))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          LOG_WARN("find failed", "server", status.server_, K(ret));
        } else {
          ret = OB_SUCCESS;
          // import servers can not in alive heartbeat status.
          if (ObServerStatus::OB_HEARTBEAT_ALIVE == status.hb_status_) {
            status.hb_status_ = ObServerStatus::OB_HEARTBEAT_LEASE_EXPIRED;
          }
          LOG_INFO("import server", K(status));
          if (OB_FAIL(server_statuses_.push_back(status))) {
            LOG_WARN("push back to server_statuses failed", K(ret));
          }
        }
      } else if (NULL == exist_status) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("exist_status is null", "server_status ptr", OB_P(exist_status), K(ret));
      } else {
        LOG_INFO("update server admin status, before update", "server", status.server_, "status", *exist_status);
        ObServerStatus bak = *exist_status;
        *exist_status = status;
        exist_status->last_hb_time_ = bak.last_hb_time_;
        exist_status->with_rootserver_ = bak.with_rootserver_;
        exist_status->hb_status_ = bak.hb_status_;
        exist_status->register_time_ = bak.register_time_;
        exist_status->merged_version_ = bak.merged_version_;
        exist_status->resource_info_ = bak.resource_info_;
        exist_status->lease_expire_time_ = bak.lease_expire_time_;
        LOG_INFO("update server admin status, after update", "server", status.server_, "status", *exist_status);
        if (OB_SUCCESS != (ret = status_change_callback_->on_server_status_change(status.server_))) {
          LOG_WARN("submit server status update task failed", K(ret), "status", *exist_status);
        }
      }
    }
    if (OB_SUCC(ret)) {
      has_build_ = true;
    }
  }
  ROOTSERVICE_EVENT_ADD("server", "load_servers", K(ret), K_(has_build));
  return ret;
}

bool ObServerManager::has_build() const
{
  return has_build_;
}

int ObServerManager::get_lease_duration(int64_t& lease_time) const
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    lease_time = config_->lease_time;
  }
  return ret;
}

int ObServerManager::get_server_zone(const ObAddr& addr, ObZone& zone) const
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!addr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid addr", K(addr), K(ret));
  } else {
    ObServerStatus server_status;
    if (OB_FAIL(get_server_status(addr, server_status))) {
      LOG_WARN("get_server_status failed", K(addr), K(ret));
    } else {
      zone = server_status.zone_;
    }
  }
  return ret;
}

int ObServerManager::get_all_server_list(common::ObIArray<ObAddr>& server_list)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("server manager has not inited", K(ret));
  } else {
    server_list.reset();
    SpinRLockGuard guard(server_status_rwlock_);
    for (int64_t i = 0; OB_SUCC(ret) && i < server_statuses_.count(); ++i) {
      ObAddr& addr = server_statuses_[i].server_;
      if (!addr.is_valid()) {
        ret = OB_INVALID_SERVER_STATUS;
        LOG_WARN("invalid addr", K(ret), K(addr));
      } else if (OB_FAIL(server_list.push_back(addr))) {
        LOG_WARN("fail to push back addr", K(ret), K(addr));
      }
    }
  }
  return ret;
}

int ObServerManager::get_server_statuses(const ObZone& zone, ObServerStatusArray& server_statuses) const
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("server manager has not inited", K(ret));
  } else {
    server_statuses.reset();
    SpinRLockGuard guard(server_status_rwlock_);
    for (int64_t i = 0; OB_SUCC(ret) && i < server_statuses_.count(); ++i) {
      if (server_statuses_[i].zone_ == zone || zone.is_empty()) {
        if (!server_statuses_[i].is_valid()) {
          ret = OB_INVALID_SERVER_STATUS;
          LOG_WARN("server status is not valid", "server status", server_statuses_[i], K(ret));
        } else if (OB_SUCCESS != (ret = server_statuses.push_back(server_statuses_[i]))) {
          LOG_WARN("push back to server_statuses failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObServerManager::get_server_statuses(const ObServerArray& servers, ObServerStatusArray& server_statuses) const
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    server_statuses.reset();
    SpinRLockGuard guard(server_status_rwlock_);
    FOREACH_CNT_X(server, servers, OB_SUCC(ret))
    {
      bool find = false;
      FOREACH_CNT_X(status, server_statuses_, !find && OB_SUCC(ret))
      {
        if (!status->is_valid()) {
          ret = OB_INVALID_SERVER_STATUS;
          LOG_WARN("server status is not valid", "server status", *status, K(ret));
        } else if (*server == status->server_) {
          if (OB_FAIL(server_statuses.push_back(*status))) {
            LOG_WARN("push_back failed", K(ret));
          } else {
            find = true;
          }
        }
      }
      if (OB_SUCC(ret) && !find) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("server not found", "server", *server, K(ret));
      }
    }
  }
  return ret;
}

int ObServerManager::get_persist_server_statuses(ObServerStatusArray& server_statuses)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(st_operator_.get(server_statuses))) {
    LOG_WARN("fail to read server status from all server table", K(ret));
  } else {
  }  // no more to do
  return ret;
}

int ObServerManager::adjust_server_status(
    const common::ObAddr& server, ObRebalanceTaskMgr& rebalance_task_mgr, const bool with_rootserver)
{
  int ret = OB_SUCCESS;
  int64_t discard_rebalance_task_time = 0;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObServerStatus server_status;
    SpinRLockGuard guard(maintaince_lock_);
    if (OB_FAIL(get_server_status(server, server_status))) {
      LOG_WARN("get server status failed", K(ret), K(server));
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        discard_rebalance_task_time = INT64_MAX;
      }
    } else if (OB_UNLIKELY(!server_status.is_valid())) {
      ret = OB_INVALID_SERVER_STATUS;
      LOG_WARN("server status is invalid", K(ret), K(server_status));
    } else {
      if (server_status.is_permanent_offline()) {
        discard_rebalance_task_time = INT64_MAX;
      } else {
        discard_rebalance_task_time = server_status.start_service_time_;
      }
      if (with_rootserver) {
        // clear with rootserver flag of other observers.
        if (OB_FAIL(st_operator_.reset_rootserver(server))) {
          LOG_WARN("reset rootserver in all server table failed", K(ret), K(server));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(st_operator_.update(server_status))) {
          LOG_WARN("update server in all server table failed", K(ret), K(server_status));
        }
      }
    }
  }
  if (OB_SUCC(ret) && discard_rebalance_task_time > 0) {
    if (OB_FAIL(rebalance_task_mgr.discard_task(server, discard_rebalance_task_time))) {
      LOG_WARN("discard rebalance task failed", K(ret), K(server), K(discard_rebalance_task_time));
    }
  }
  return ret;
}

void ObServerManager::reset()
{
  SpinWLockGuard guard(server_status_rwlock_);
  has_build_ = false;
  server_statuses_.reset();
}

int ObServerManager::have_server_stopped(const common::ObZone& zone, bool& stopped) const
{
  int ret = OB_SUCCESS;
  stopped = false;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (zone.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("zone is empty", K(zone), K(ret));
  } else {
    for (int64_t i = 0; i < server_statuses_.count(); ++i) {
      if (server_statuses_[i].is_stopped() && server_statuses_[i].zone_ == zone) {
        stopped = true;
        LOG_DEBUG("have server in stopped status",
            K(zone),
            "other_server",
            server_statuses_[i].server_,
            "other_zone",
            server_statuses_[i].zone_);
        break;
      }
    }
  }
  return ret;
}

int ObServerManager::check_other_zone_stopped(const common::ObZone& zone, bool& stopped)
{
  int ret = OB_SUCCESS;
  stopped = false;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (zone.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("zone is empty", K(zone), K(ret));
  } else {
    for (int64_t i = 0; i < server_statuses_.count(); ++i) {
      if (server_statuses_[i].is_stopped() && server_statuses_[i].zone_ != zone) {
        stopped = true;
        LOG_WARN("have other server in stopped status",
            K(zone),
            "other_server",
            server_statuses_[i].server_,
            "other_zone",
            server_statuses_[i].zone_);
        break;
      }
    }
  }
  return ret;
}

int64_t ObServerManager::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_KV("has_build", has_build_, "server_statuses", server_statuses_);
  return pos;
}

int ObServerManager::find(const ObAddr& server, const ObServerStatus*& status) const
{
  int ret = OB_SUCCESS;
  status = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), K(ret));
  } else {
    bool find = false;
    for (int64_t i = 0; i < server_statuses_.count() && !find; ++i) {
      if (server_statuses_[i].server_ == server) {
        status = &server_statuses_[i];
        find = true;
      }
    }
    if (!find) {
      ret = OB_ENTRY_NOT_EXIST;
      // we print info log here, because sometime this is normal(such as add server)
      LOG_INFO("server not exist", K(server), K(ret));
    }
  }
  return ret;
}

int ObServerManager::find(const ObAddr& server, ObServerStatus*& status)
{
  int ret = OB_SUCCESS;
  status = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), K(ret));
  } else {
    const ObServerStatus* temp_status = NULL;
    if (OB_FAIL(static_cast<const ObServerManager&>(*this).find(server, temp_status))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("find failed", K(server), K(ret));
      } else {
        // don't print log here, invoked function "find" already print
      }
    } else if (NULL == temp_status) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("temp_status is null", "temp_status ptr", OB_P(temp_status), K(ret));
    } else {
      status = const_cast<ObServerStatus*>(temp_status);
    }
  }
  return ret;
}

int ObServerManager::fetch_new_server_id(uint64_t& server_id)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid sql proxy", KR(ret));
  } else {
    uint64_t combine_id = OB_INVALID_ID;
    ObMaxIdFetcher id_fetcher(*GCTX.sql_proxy_);
    if (OB_FAIL(id_fetcher.fetch_new_max_id(OB_SYS_TENANT_ID, OB_MAX_USED_SERVER_ID_TYPE, combine_id))) {
      LOG_WARN("fetch_new_max_id failed", K(ret));
    } else {
      server_id = extract_pure_id(combine_id);
    }
  }
  return ret;
}

int ObServerManager::check_server_id_used(const uint64_t server_id, bool& server_id_used)
{
  int ret = OB_SUCCESS;
  server_id_used = false;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == server_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server_id", K(server_id), K(ret));
  } else {
    for (int64_t i = 0; i < server_statuses_.count(); ++i) {
      if (server_statuses_[i].id_ == server_id) {
        server_id_used = true;
        break;
      }
    }
  }
  return ret;
}

int ObServerManager::update_merged_version(const ObAddr& addr, int64_t frozen_version, bool& zone_merged)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("server manager not inited", K(ret));
  } else if (!addr.is_valid() || frozen_version <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(addr), K(frozen_version), K(ret));
  } else {
    SpinWLockGuard guard(server_status_rwlock_);
    ObServerStatus* status = NULL;
    if (OB_FAIL(find(addr, status))) {
      LOG_WARN("find failed", K(addr), K(ret));
    } else if (NULL == status) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("status is null", "status ptr", OB_P(status), K(ret));
    } else if (status->merged_version_ > frozen_version) {
      LOG_WARN("receive obs report merge finish, version is invallid",
          K(frozen_version),
          "server version",
          status->merged_version_);
    } else {
      status->merged_version_ = frozen_version;
      zone_merged = true;
      FOREACH_X(s, server_statuses_, zone_merged)
      {
        if (s->zone_ == status->zone_ && s->is_alive() && s->merged_version_ < frozen_version) {
          zone_merged = false;
        }
      }
    }
  }
  return ret;
}

int ObServerManager::get_merged_version(const common::ObAddr& addr, int64_t& merged_version) const
{
  int ret = OB_SUCCESS;
  merged_version = 0;
  SpinRLockGuard guard(server_status_rwlock_);
  const ObServerStatus* status = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!addr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid addr", K(addr), K(ret));
  } else if (OB_FAIL(find(addr, status))) {
    LOG_WARN("find failed", K(addr), K(ret));
  } else if (NULL == status) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("status is null", "status ptr", OB_P(status), K(ret));
  } else {
    merged_version = status->merged_version_;
  }
  return ret;
}

int ObServerManager::block_migrate_in(const ObAddr& addr)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("server manager has not inited", K(ret));
  } else if (!addr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid addr", K(addr), K(ret));
  } else {
    const bool blocked = true;
    if (OB_FAIL(set_migrate_in_blocked(addr, blocked))) {
      LOG_WARN("set_migrate_in_blocked failed", K(addr), K(blocked), K(ret));
    } else {
      ROOTSERVICE_EVENT_ADD("server", "block_migrate_in", "server", addr);
    }
  }
  return ret;
}

int ObServerManager::unblock_migrate_in(const ObAddr& addr)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("server manager has not inited", K(ret));
  } else if (!addr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid addr", K(addr), K(ret));
  } else {
    const bool blocked = false;
    if (OB_FAIL(set_migrate_in_blocked(addr, blocked))) {
      LOG_WARN("set_migrate_in_blocked failed", K(addr), K(blocked), K(ret));
    } else {
      ROOTSERVICE_EVENT_ADD("server", "unblock_migrate_in", "server", addr);
    }
  }
  return ret;
}

int ObServerManager::set_migrate_in_blocked(const common::ObAddr& addr, const bool blocked)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(server_status_rwlock_);
  ObServerStatus* status_ptr = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("server manager has not inited", K(ret));
  } else if (!addr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid addr", K(addr), K(ret));
  } else if (OB_FAIL(find(addr, status_ptr))) {
    LOG_WARN("find failed", K(addr), K(ret));
  } else if (NULL == status_ptr) {
    ret = OB_ERR_UNEXPECTED;
    LOG_INFO("status_ptr is null", "status_ptr", OB_P(status_ptr), K(ret));
  } else {
    if (!blocked) {
      status_ptr->unblock_migrate_in();
    } else {
      status_ptr->block_migrate_in();
    }
    // ignore wakeup balancer and wakeup daily merger failed
    int temp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (temp_ret = status_change_callback_->wakeup_balancer())) {
      LOG_WARN("wakeup_balancer failed", K(temp_ret));
    } else if (OB_SUCCESS != (temp_ret = status_change_callback_->wakeup_daily_merger())) {
      LOG_WARN("wakeup_daily_merger failed", K(temp_ret));
    }
    ret = status_change_callback_->on_server_status_change(addr);
    if (OB_FAIL(ret)) {
      LOG_WARN("commit block migrate in status change task failed",
          K(addr),
          K(blocked),
          "block_migrate_in_time",
          status_ptr->block_migrate_in_time_,
          K(ret));
    } else {
      LOG_INFO("commit block migrate in status change task succeed",
          K(addr),
          K(blocked),
          "block_migrate_in_time",
          status_ptr->block_migrate_in_time_,
          K(ret));
    }
  }
  return ret;
}

int ObServerManager::check_migrate_in_blocked(const common::ObAddr& addr, bool& blocked) const
{
  int ret = OB_SUCCESS;
  blocked = true;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("server manager has not inited", K(ret));
  } else if (!addr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid addr", K(addr), K(ret));
  } else {
    SpinRLockGuard guard(server_status_rwlock_);
    const ObServerStatus* status_ptr = NULL;
    if (OB_FAIL(find(addr, status_ptr))) {
      LOG_WARN("find failed", K(addr), K(ret));
    } else if (NULL == status_ptr) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("status_ptr is null", "status_ptr", OB_P(status_ptr), K(ret));
    } else {
      blocked = status_ptr->is_migrate_in_blocked();
    }
  }
  return ret;
}

int ObServerManager::check_in_service(const common::ObAddr& addr, bool& in_service) const
{
  int ret = OB_SUCCESS;
  in_service = false;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("server manager has not inited", K(ret));
  } else if (!addr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid addr", K(addr), K(ret));
  } else {
    SpinRLockGuard guard(server_status_rwlock_);
    const ObServerStatus* status_ptr = NULL;
    if (OB_FAIL(find(addr, status_ptr))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("find failed", K(addr), K(ret));
      } else {
        ret = OB_SUCCESS;
        in_service = false;
        LOG_INFO("treat server that not exist as not in service", K(addr));
      }
    } else if (NULL == status_ptr) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("status_ptr is null", "status_ptr", OB_P(status_ptr), K(ret));
    } else {
      in_service = status_ptr->in_service();
    }
  }
  return ret;
}

int ObServerManager::set_with_partition(const common::ObAddr& server)
{
  int ret = OB_SUCCESS;
  bool need_update = false;
  ObServerStatus* status = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("server manager not inited", K(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(ret), K(server));
  } else {
    SpinRLockGuard guard(server_status_rwlock_);
    if (OB_FAIL(find(server, status))) {
      LOG_WARN("find server failed", K(ret), K(server));
    } else if (NULL == status) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL server status", K(ret));
    } else if (!status->is_alive()) {
      ret = OB_SERVER_NOT_ALIVE;
      LOG_WARN("server not alive", K(ret));
    } else {
      need_update = !status->with_partition_;
    }
    status = NULL;
  }

  if (OB_SUCC(ret) && need_update) {
    need_update = false;
    SpinWLockGuard guard(maintaince_lock_);
    {
      SpinRLockGuard guard(server_status_rwlock_);
      // check status again
      if (OB_FAIL(find(server, status))) {
        LOG_WARN("find server failed", K(ret), K(server));
      } else if (NULL == status) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL server status", K(ret));
      } else if (!status->is_alive()) {
        ret = OB_SERVER_NOT_ALIVE;
        LOG_WARN("server not alive", K(ret));
      } else if (!status->with_partition_) {
        status = NULL;
        need_update = true;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (need_update) {
      bool with_partition = true;
      if (OB_FAIL(st_operator_.update_with_partition(server, with_partition))) {
        LOG_WARN("update with partition failed", K(ret), K(server), K(with_partition));
      } else {
        ROOTSERVICE_EVENT_ADD("server", "set_with_partition", K(server));
        LOG_INFO("set with partition", K(server));
      }

      if (OB_SUCC(ret)) {
        SpinWLockGuard guard(server_status_rwlock_);
        if (OB_FAIL(find(server, status))) {
          LOG_WARN("find server failed", K(ret), K(server));
        } else if (NULL == status) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL server status", K(ret));
        } else {
          // It's safe to update with partition flag here because we hold %maintaince_lock_,
          // flag will not be modified after previous check.
          status->with_partition_ = true;
        }
      }
    }
  }
  return ret;
}

int ObServerManager::clear_with_partiton(const common::ObAddr& server, const int64_t last_hb_time)
{
  int ret = OB_SUCCESS;
  ObServerStatus* status = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("server manager not inited", K(ret));
  } else if (!server.is_valid() || last_hb_time < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(ret), K(server), K(last_hb_time));
  } else {
    SpinWLockGuard guard(maintaince_lock_);
    bool need_update = false;
    {
      SpinRLockGuard guard(server_status_rwlock_);
      if (OB_FAIL(find(server, status))) {
        LOG_WARN("find server failed", K(ret), K(server));
      } else if (NULL == status) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL server status", K(ret));
      } else if (!status->with_partition_) {
        // with partition flag not set, do nothing
      } else if (last_hb_time != status->last_hb_time_) {
        // last heartbeat time mismatch, do nothing, return success.
        LOG_WARN(
            "last hb time mismatch, do not clear with partition flag", K(server), K(last_hb_time), "status", *status);
      } else {
        need_update = true;
        status = NULL;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (!need_update) {
    } else {
      bool with_partition = false;
      if (OB_FAIL(st_operator_.update_with_partition(server, with_partition))) {
        LOG_WARN("update with partition failed", K(ret), K(server), K(with_partition));
      } else {
        ROOTSERVICE_EVENT_ADD("server", "clear_with_partition", K(server));
        LOG_INFO("clear with partition", K(server));
      }

      if (OB_SUCC(ret)) {
        SpinWLockGuard guard(server_status_rwlock_);
        if (OB_FAIL(find(server, status))) {
          LOG_WARN("find server failed", K(ret), K(server));
        } else if (NULL == status) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL server status", K(ret));
        } else {
          // It's safe to update with partition flag here because we hold %maintaince_lock_,
          // flag will not be modified after previous check.
          status->with_partition_ = false;
        }
      }
    }
  }
  return ret;
}

int ObServerManager::set_force_stop_hb(const ObAddr& server, const bool& force_stop_hb)
{
  int ret = OB_SUCCESS;
  ObServerStatus* status = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("server manager not inited", K(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(ret));
  } else {
    // force_stop_hb_ is only in memory
    SpinWLockGuard guard(server_status_rwlock_);
    if (OB_FAIL(find(server, status))) {
      LOG_WARN("find server failed", K(ret), K(server));
    } else if (NULL == status) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("server status is NULL", K(ret));
    } else if (ObServerStatus::OB_SERVER_ADMIN_DELETING != status->admin_status_ &&
               ObServerStatus::OB_SERVER_ADMIN_TAKENOVER_BY_RS != status->admin_status_) {
      ret = OB_SERVER_NOT_DELETING;
      LOG_WARN("server not in deleting status, cannot set force stop hb", K(ret), K(status));
    } else {
      status->force_stop_hb_ = force_stop_hb;
      LOG_INFO("success to set force stop hb!", K(force_stop_hb));
    }
  }
  return ret;
}

int ObServerManager::get_min_server_version(char min_server_version[OB_SERVER_VERSION_LENGTH])
{
  int ret = OB_SUCCESS;
  ObZone zone;  // empty zone, get all server statuses
  ObArray<ObServerStatus> server_statuses;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("server manager not inited", K(ret));
  } else {
    // no need to add lock
    // check all servers' build versions are identical
    if (OB_FAIL(get_server_statuses(zone, server_statuses))) {
      LOG_WARN("get all server statuses failed", K(ret));
    } else if (OB_UNLIKELY(true == server_statuses.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("server_statuses is empty", K(ret));
    } else {
      ObClusterVersion version_parser;
      uint64_t cur_min_version = UINT64_MAX;
      FOREACH_CNT_X(status, server_statuses, OB_SUCC(ret))
      {
        char* saveptr = NULL;
        char* version = STRTOK_R(status->build_version_, "_", &saveptr);
        if (NULL == version || strlen(version) + 1 > OB_SERVER_VERSION_LENGTH) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid build version format", "build_version", status->build_version_);
        } else if (OB_FAIL(version_parser.refresh_cluster_version(version))) {
          LOG_WARN("failed to parse version", "version", version);
        } else {
          if (version_parser.get_cluster_version() < cur_min_version) {
            size_t len = strlen(version);
            MEMCPY(min_server_version, version, len);
            min_server_version[len] = '\0';
            cur_min_version = version_parser.get_cluster_version();
          }
        }
      }
      if (OB_SUCC(ret) && UINT64_MAX == cur_min_version) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("no valid server version found", K(ret));
      }
    }
  }

  return ret;
}

bool ObServerManager::have_server_deleting() const
{
  bool bret = false;
  int tmp_ret = OB_SUCCESS;
  ObZone zone;
  ObArray<ObServerStatus> server_statuses;
  if (!inited_) {
    tmp_ret = OB_NOT_INIT;
    LOG_WARN("server manager not inited", K(tmp_ret));
  } else if (OB_SUCCESS != (tmp_ret = get_server_statuses(zone, server_statuses))) {
    LOG_WARN("fail to get server status", K(zone), K(tmp_ret));
  } else {
    FOREACH_CNT_X(status, server_statuses, OB_SUCCESS == tmp_ret)
    {
      if (OB_ISNULL(status)) {
        tmp_ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid status", K(tmp_ret));
      } else if (ObServerStatus::OB_SERVER_ADMIN_DELETING == status->admin_status_) {
        bret = true;
        break;
      }
    }
  }
  return bret;
}
int ObServerManager::check_all_server_active(bool& all_active) const
{
  int ret = OB_SUCCESS;
  all_active = true;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    SpinRLockGuard guard(server_status_rwlock_);
    FOREACH_CNT_X(status, server_statuses_, all_active && OB_SUCC(ret))
    {
      if (OB_ISNULL(status)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid status", K(ret));
      } else if (status->in_service() && status->is_active()) {
      } else {
        all_active = false;
        LOG_WARN("server status not valid", "server status", *status);
      }
    }
  }
  return ret;
}

//////////////////////////////////////
//////////////////////////////////////
ObHeartbeatChecker::ObHeartbeatChecker() : ObRsReentrantThread(true), inited_(false), server_manager_(NULL)
{}

ObHeartbeatChecker::~ObHeartbeatChecker()
{}

int ObHeartbeatChecker::init(ObServerManager& server_manager)
{
  int ret = OB_SUCCESS;
  static const int64_t heartbeat_checker_thread_cnt = 1;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("heart beat checker has already been inited", K(ret));
  } else if (OB_FAIL(create(heartbeat_checker_thread_cnt, "HBChecker"))) {
    LOG_WARN("create heartbeat checker thread failed", K(ret), K(heartbeat_checker_thread_cnt));
  } else {
    server_manager_ = &server_manager;
    inited_ = true;
  }
  return ret;
}

void ObHeartbeatChecker::run3()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("heart beat checker has not inited", K(ret));
  } else {
    LOG_INFO("heartbeat checker start");
    int64_t now = ObTimeUtility::current_time();
    int64_t last_renew_rs_time = 0;
    int64_t RENEW_INTERVAL = 3 * 1000 * 1000;  // 3s
    while (!stop_) {
      update_last_run_timestamp();
      LOG_TRACE("begin check all server heartbeat");
      ret = server_manager_->check_servers();
      if (OB_FAIL(ret)) {
        LOG_WARN("server managers check servers failed", K(ret));
      }
      // ignore ret
      now = ObTimeUtility::current_time();
      if (now - last_renew_rs_time > RENEW_INTERVAL) {
        last_renew_rs_time = now;
        if (OB_FAIL(server_manager_->try_renew_rs_list())) {
          LOG_WARN("fail to try renew rs list", KR(ret));
        }
      }
      DEBUG_SYNC(HUNG_HEARTBEAT_CHECK);
      usleep(CHECK_INTERVAL_US);
    }
    LOG_INFO("heartbeat checker stop");
  }
}

int64_t ObHeartbeatChecker::get_schedule_interval() const
{
  return CHECK_INTERVAL_US;
}
}  // end namespace rootserver
}  // end namespace oceanbase
