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
#include "ob_server_checker.h"

#include "lib/container/ob_array_iterator.h"
#include "ob_server_manager.h"
#include "ob_unit_manager.h"
#include "ob_zone_manager.h"
#include "ob_empty_server_checker.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::rootserver;

ObServerChecker::ObServerChecker()
    : inited_(false), server_mgr_(NULL), unit_mgr_(NULL), empty_server_checker_(NULL), tenant_stat_(NULL)
{}

int ObServerChecker::init(ObUnitManager& unit_mgr, ObServerManager& server_mgr,
    ObEmptyServerChecker& empty_server_checker, TenantBalanceStat& tenant_stat)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
  } else {
    server_mgr_ = &server_mgr;
    unit_mgr_ = &unit_mgr;
    empty_server_checker_ = &empty_server_checker;
    tenant_stat_ = &tenant_stat;
    inited_ = true;
  }
  return ret;
}

int ObServerChecker::try_delete_server()
{
  int ret = OB_SUCCESS;
  ObZone zone;  // empty means all zones
  ObArray<ObServerStatus> statuses;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (0 == tenant_stat_->get_replica_count_mgr().get_server_count()) {
    // no need to try delete server, do nothing.
  } else if (OB_FAIL(server_mgr_->get_server_statuses(zone, statuses))) {
    LOG_WARN("get_server_statuses failed", K(zone), K(ret));
  } else {
    int first_error_ret = OB_SUCCESS;
    FOREACH_CNT_X(status, statuses, OB_SUCCESS == ret)
    {
      if (ObServerStatus::OB_SERVER_ADMIN_DELETING == status->admin_status_) {
        bool server_empty = false;
        if (OB_FAIL(unit_mgr_->check_server_empty(status->server_, server_empty))) {
          LOG_WARN("check_server_empty failed", "server", status->server_, K(ret));
        } else if (server_empty && !(status->force_stop_hb_)) {
          // stop server's heartbeat
          bool force_stop_hb = true;
          if (OB_FAIL(server_mgr_->set_force_stop_hb(status->server_, force_stop_hb))) {
            LOG_WARN("set force stop hb failed", K(status->server_), K(ret));
          } else {
            LOG_INFO("force set stop hb", KR(ret), K(status->server_));
          }
          DEBUG_SYNC(SET_FORCE_STOP_HB_DONE);
        } else if (server_empty && !(status->with_partition_)) {
          const bool commit = true;
          if (OB_FAIL(server_mgr_->end_delete_server(status->server_, status->zone_, commit))) {
            LOG_WARN("server_mgr end_delete_server failed",
                "server",
                status->server_,
                "zone",
                status->zone_,
                K(commit),
                K(ret));
          }
        }
      }
      // ignore single server error
      if (OB_FAIL(ret)) {
        first_error_ret = OB_SUCC(first_error_ret) ? ret : first_error_ret;
        ret = OB_SUCCESS;
      }
    }
    ret = OB_SUCC(first_error_ret) ? ret : first_error_ret;
  }
  return ret;
}

int ObServerChecker::try_notify_empty_server_checker()
{
  int ret = OB_SUCCESS;
  ObZone zone;  // empty means all zones
  ObArray<ObServerStatus> statuses;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (0 == tenant_stat_->get_replica_count_mgr().get_server_count()) {
    // no need to notify empty server check, do nothing.
  } else if (OB_FAIL(server_mgr_->get_server_statuses(zone, statuses))) {
    LOG_WARN("get_server_statuses failed", K(zone), K(ret));
  } else {
    const int64_t now = ObTimeUtility::current_time();
    bool notify_check = false;
    int first_error_ret = OB_SUCCESS;
    FOREACH_X(s, statuses, OB_SUCC(ret))
    {
      if (s->need_check_empty(now)) {
        int64_t replica_cnt = 0;
        if (OB_FAIL(tenant_stat_->get_replica_count_mgr().get_replica_count(s->server_, replica_cnt))) {
          if (OB_ENTRY_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("get replica count failed", K(ret), "server", s->server_);
          }
        } else {
          if (replica_cnt == 0) {
            notify_check = true;
          } else {
            LOG_INFO("still have replica on non-alive server", "server", s->server_, K(replica_cnt));
          }
        }
      }
      // ignore single server error
      if (OB_FAIL(ret)) {
        first_error_ret = OB_SUCC(first_error_ret) ? ret : first_error_ret;
        ret = OB_SUCCESS;
      }
    }

    if (OB_SUCC(ret) && notify_check) {
      if (OB_FAIL(empty_server_checker_->notify_check())) {
        LOG_WARN("notify check empty server failed", K(ret));
      }
      DEBUG_SYNC(UPDATE_WITH_PARTITION_FLAG_DONE);
    }

    ret = OB_SUCC(first_error_ret) ? ret : first_error_ret;
  }
  return ret;
}
