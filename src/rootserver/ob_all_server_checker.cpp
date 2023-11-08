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

#include "ob_all_server_checker.h"

#include "share/config/ob_server_config.h"
#include "share/ob_srv_rpc_proxy.h"
#include "share/ob_rpc_struct.h"
#include "rootserver/ob_server_manager.h"
#include "rootserver/ob_all_server_task.h"
#include "rootserver/ob_update_rs_list_task.h"

namespace oceanbase {
using namespace common;
using namespace share;
namespace rootserver {
ObAllServerChecker::ObAllServerChecker()
    : inited_(false), server_manager_(NULL), rebalance_task_mgr_(NULL), rs_addr_(), rpc_proxy_(NULL), pt_operator_(NULL)
{}

ObAllServerChecker::~ObAllServerChecker()
{}

int ObAllServerChecker::init(ObServerManager& server_manager, ObRebalanceTaskMgr& rebalance_task_mgr,
    obrpc::ObSrvRpcProxy& rpc_proxy, share::ObPartitionTableOperator* pt_operator, const ObAddr& rs_addr)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObAllServerChecker has already inited", K(ret));
  } else if (!rs_addr.is_valid() || OB_ISNULL(pt_operator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(rs_addr), K(ret), KP(pt_operator));
  } else {
    server_manager_ = &server_manager;
    rebalance_task_mgr_ = &rebalance_task_mgr;
    rpc_proxy_ = &rpc_proxy;
    rs_addr_ = rs_addr;
    pt_operator_ = pt_operator;
    inited_ = true;
  }
  return ret;
}

int ObAllServerChecker::check_all_server()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ObCurTraceId::get_trace_id())) {
    // Prevent the current trace_id from being overwritten
    ObCurTraceId::init(GCONF.self_addr_);
  }

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("all server checker is not inited", K(ret));
  } else if (OB_ISNULL(rpc_proxy_) || OB_ISNULL(server_manager_) || OB_ISNULL(rebalance_task_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected", KR(ret), KP(rpc_proxy_), KP(server_manager_), KP(rebalance_task_mgr_));
  } else {
    if (!server_manager_->has_build()) {
      ret = OB_EAGAIN;
      LOG_INFO("server_manager has not build from all server table, try it later");
    } else {
      ObArray<ObServerStatus> inmem_statuses;
      ObArray<ObServerStatus> persist_statuses;
      ObZone zone;  // empty zone means get all server statuses
      if (OB_FAIL(server_manager_->get_server_statuses(zone, inmem_statuses))) {
        LOG_WARN("get server statuses from server manager failed", K(ret));
      } else if (OB_FAIL(server_manager_->get_persist_server_statuses(persist_statuses))) {
        LOG_WARN("read server statuses form all server table failed", K(ret));
      } else {
        bool server_status_need_change = false;
        int tmp_ret = OB_SUCCESS;
        ObArray<ObAddr> inactive_server_list;
        for (int64_t i = 0; i < inmem_statuses.count(); ++i) {
          int64_t j = 0;
          const ObServerStatus &in_memory_server_status = inmem_statuses[i];
          const ObAddr &server = in_memory_server_status.server_;
          server_status_need_change = false;
          for (j = 0; j < persist_statuses.count(); ++j) {
            if (server == persist_statuses[j].server_) {
              break;
            }
          }
          if (j < persist_statuses.count()) {
            // find in persist_statuses
            bool same = false;
            if (OB_FAIL(check_status_same(in_memory_server_status, persist_statuses[j], same))) {
              LOG_WARN("check_status_same failed",
                  "inmem_status",
                  in_memory_server_status,
                  "persist_status",
                  persist_statuses[j],
                  K(ret));
            } else if (!same) {
              LOG_INFO("find server status not same",
                  "inmem_status",
                  in_memory_server_status,
                  "persist_status",
                  persist_statuses[j]);
              server_status_need_change = true;
            } else {
              // do nothing
            }
          } else {
            // not find in persist_statues
            LOG_INFO("find server not exist in all_server table", "inmem_status", in_memory_server_status);
            server_status_need_change = true;
          }
          if (OB_SUCC(ret) && server_status_need_change) {
            const bool with_rootserver = (server == rs_addr_);
            ObAllServerTask task(*server_manager_, *rebalance_task_mgr_, server, with_rootserver);
            if (OB_SUCCESS != (tmp_ret = task.process())) {
              LOG_WARN("failed to process all server task", KR(tmp_ret), K(server), K(with_rootserver));
            }
          }
          if (OB_SUCC(ret) && !in_memory_server_status.is_alive()) {
            if (OB_FAIL(inactive_server_list.push_back(server))) {
              LOG_WARN("failed to push back", KR(ret), K(server));
            }
          }
        }  // end for i
        if (OB_SUCC(ret) && inactive_server_list.count() > 0) {
          if (OB_SUCCESS != (tmp_ret = try_renew_rs_list(inactive_server_list))) {
            LOG_WARN("failed to renew rs list", KR(tmp_ret), K(inactive_server_list));
          }
        }
      }
    }
  }
  return ret;
}

int ObAllServerChecker::check_status_same(const ObServerStatus& left, const ObServerStatus& right, bool& same) const
{
  // don't compare last_hb_time here, because we don't update all_server
  // when only last_hb_time change
  int ret = OB_SUCCESS;
  same = false;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!left.is_valid() || !right.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(left), K(right), K(ret));
  } else {
    same = left.server_ == right.server_ && left.zone_ == right.zone_ && left.sql_port_ == right.sql_port_ &&
           0 == STRCMP(left.build_version_, right.build_version_) &&
           left.get_display_status() == right.get_display_status() && left.with_rootserver_ == right.with_rootserver_ &&
           left.block_migrate_in_time_ == right.block_migrate_in_time_ && left.stop_time_ == right.stop_time_ &&
           left.start_service_time_ == right.start_service_time_ && left.with_rootserver_ == right.with_rootserver_ &&
           left.with_partition_ == right.with_partition_;
  }
  return ret;
}

int ObAllServerChecker::try_renew_rs_list(const common::ObIArray<ObAddr> &inactive_servers)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("all server checker is not inited", K(ret));
  } else if (OB_ISNULL(rpc_proxy_) || OB_ISNULL(server_manager_) || OB_ISNULL(rebalance_task_mgr_) ||
             OB_ISNULL(pt_operator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN(
        "error unexpected", KR(ret), KP(rpc_proxy_), KP(server_manager_), KP(rebalance_task_mgr_), KP(pt_operator_));
  } else if (0 == inactive_servers.count()) {
  } else if (!server_manager_->has_build()) {
    ret = OB_EAGAIN;
    LOG_WARN("server_manager has not build from all server table, try it later", KR(ret));
  } else {
    obrpc::ObRsListArg arg;
    bool rs_list_diff_member_list = false;
    ObAddrList rs_list;
    ObAddrList readonly_rs_list;
    if (OB_FAIL(ObUpdateRsListTask::get_rs_list(
            *pt_operator_, *server_manager_, rs_addr_, rs_list, readonly_rs_list, rs_list_diff_member_list))) {
      LOG_WARN("failed to get rs list", KR(ret));
    } else if (OB_FAIL(arg.init(rs_addr_, rs_list))) {
      LOG_WARN("failed to init arg", KR(ret), K(rs_addr_), K(rs_list));
    } else {
      int tmp_ret = OB_SUCCESS;
      const int64_t timeout = GCONF.rpc_timeout;
      LOG_INFO("renew rs list for inactive server", K(inactive_servers), K(arg));
      for (int64_t i = 0; i < inactive_servers.count(); ++i) {
        // ignore error of each server
        if (OB_SUCCESS != (tmp_ret = rpc_proxy_->to(inactive_servers.at(i)).timeout(timeout).broadcast_rs_list(arg))) {
          ret = OB_SUCC(ret) ? tmp_ret : ret;
          LOG_WARN(
              "failed to broadcast rs list", KR(tmp_ret), K(timeout), K(i), "server", inactive_servers.at(i), K(arg));
        } else {
          LOG_INFO("success to broadcast rs list", K(i), "server", inactive_servers.at(i), K(arg));
        }
      }
    }
  }
  return ret;
}

ObCheckServerTask::ObCheckServerTask(common::ObWorkQueue& work_queue, ObAllServerChecker& checker)
    : ObAsyncTimerTask(work_queue), checker_(checker)
{
  set_retry_times(0);  // don't retry when process failed
}

int ObCheckServerTask::process()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(checker_.check_all_server())) {
    LOG_WARN("checker all server failed", K(ret));
  }
  return ret;
}

ObAsyncTask* ObCheckServerTask::deep_copy(char* buf, const int64_t buf_size) const
{
  ObCheckServerTask* task = NULL;
  if (NULL == buf || buf_size < static_cast<int64_t>(sizeof(*this))) {
    LOG_WARN("buffer not large enough", K(buf_size));
  } else {
    task = new (buf) ObCheckServerTask(work_queue_, checker_);
  }
  return task;
}

}  // end namespace rootserver
}  // end namespace oceanbase
