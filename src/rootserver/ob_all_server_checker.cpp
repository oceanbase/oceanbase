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
#include "rootserver/ob_server_manager.h"
#include "rootserver/ob_heartbeat_service.h"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace rootserver
{
ObAllServerChecker::ObAllServerChecker()
  : inited_(false),
    server_manager_(NULL),
    rs_addr_()
{
}

ObAllServerChecker::~ObAllServerChecker()
{
}

int ObAllServerChecker::init(ObServerManager &server_manager,
                             const ObAddr &rs_addr)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObAllServerChecker has already inited", K(ret));
  } else if (!rs_addr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid rs_addr", K(rs_addr), K(ret));
  } else {
    server_manager_ = &server_manager;
    rs_addr_ = rs_addr;
    inited_ = true;
  }
  return ret;
}

int ObAllServerChecker::check_all_server()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("all server checker is not inited", K(ret));
  } else {
    if (!server_manager_->has_build()) {
      ret = OB_EAGAIN;
      LOG_INFO("server_manager has not build from all server table, try it later");
    } else {
      ObArray<ObServerStatus> inmem_statuses;
      ObArray<ObServerStatus> persist_statuses;
      ObZone zone; // empty zone means get all server statuses
      if (OB_FAIL(server_manager_->get_server_statuses(zone, inmem_statuses))) {
        LOG_WARN("get server statuses from server manager failed", K(ret));
      } else if (OB_FAIL(server_manager_->get_persist_server_statuses(persist_statuses))) {
        LOG_WARN("read server statuses form all server table failed", K(ret));
      } else {
        ObIStatusChangeCallback &cb = server_manager_->get_status_change_callback();
        for (int64_t i = 0; i < inmem_statuses.count(); ++i) {
          int64_t j = 0;
          for (j = 0; j < persist_statuses.count(); ++j) {
            if (inmem_statuses[i].server_ == persist_statuses[j].server_) {
              break;
            }
          }
          if (j < persist_statuses.count()) {
            //find in persist_statuses
            bool same = false;
            if (OB_FAIL(check_status_same(inmem_statuses[i], persist_statuses[j], same))) {
              LOG_WARN("check_status_same failed", "inmem_status", inmem_statuses[i],
                  "persist_status", persist_statuses[j], K(ret));
            } else if (!same) {
              LOG_INFO("find server status not same", "inmem_status", inmem_statuses[i],
                  "persist_status", persist_statuses[j]);
              if (OB_FAIL(cb.on_server_status_change(inmem_statuses[i].server_))) {
                LOG_WARN("commit task failed", "server status", inmem_statuses[i], K(ret));
              }
            } else {
              // do nothing
            }
          } else {
            // not find in persist_statues
            LOG_INFO("find server not exist in all_server table",
                "inmem_status", inmem_statuses[i]);
            if (OB_FAIL(cb.on_server_status_change(inmem_statuses[i].server_))) {
              LOG_WARN("commit task failed", "server status", inmem_statuses[i], K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObAllServerChecker::check_status_same(const ObServerStatus &left,
                                          const ObServerStatus &right,
                                          bool &same) const
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
    same = left.server_ == right.server_ && left.zone_ == right.zone_
      && left.sql_port_ == right.sql_port_
      && 0 == STRCMP(left.build_version_, right.build_version_)
      && left.get_display_status() == right.get_display_status()
      && left.with_rootserver_ == right.with_rootserver_
      && left.block_migrate_in_time_ == right.block_migrate_in_time_
      && left.stop_time_ == right.stop_time_
      && left.start_service_time_ == right.start_service_time_
      && left.with_rootserver_ == right.with_rootserver_
      && left.with_partition_ == right.with_partition_;
  }
  return ret;
}

ObCheckServerTask::ObCheckServerTask(common::ObWorkQueue &work_queue,
                                     ObAllServerChecker &checker)
    :ObAsyncTimerTask(work_queue),
     checker_(checker)
{
  set_retry_times(0);  // don't retry when process failed
}

int ObCheckServerTask::process()
{
  int ret = OB_SUCCESS;
  if (!ObHeartbeatService::is_service_enabled()) {
    if (OB_FAIL(checker_.check_all_server())) {
      LOG_WARN("checker all server failed", K(ret));
    }
  } else {
    LOG_TRACE("no need to do ObCheckServerTask in version >= 4.2");
  }
  return ret;
}

ObAsyncTask *ObCheckServerTask::deep_copy(char *buf, const int64_t buf_size) const
{
  ObCheckServerTask *task = NULL;
  if (NULL == buf || buf_size < static_cast<int64_t>(sizeof(*this))) {
    LOG_WARN_RET(common::OB_BUF_NOT_ENOUGH, "buffer not large enough", K(buf_size));
  } else {
    task = new(buf) ObCheckServerTask(work_queue_, checker_);
  }
  return task;
}

}//end namespace rootserver
}//end namespace oceanbase
