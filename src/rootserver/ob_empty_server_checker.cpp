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

#include "ob_empty_server_checker.h"

#include "share/ls/ob_ls_table_iterator.h"//ObAllLSTableIterator
#include "share/ob_all_server_tracer.h"
#include "share/ls/ob_ls_table_operator.h"
#include "share/ob_server_check_utils.h"
#include "lib/utility/ob_tracepoint.h" // ERRSIM_POINT_DEF


#include "ob_server_manager.h"
#include "ob_unit_manager.h"//ObUnitManager
#include "ob_server_zone_op_service.h"
#include "rootserver/ob_heartbeat_service.h"

namespace oceanbase
{
namespace rootserver
{
using namespace common;
using namespace obrpc;
using namespace share;

int ObEmptyServerChecker::init(
    ObServerManager &server_mgr,
    ObUnitManager &unit_mgr,
    share::ObLSTableOperator &lst_operator,
    schema::ObMultiVersionSchemaService &schema_service,
    ObServerZoneOpService &server_zone_op_service)
{
  int ret = OB_SUCCESS;
  const int64_t empty_server_checker_thread_cnt = 1;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(create(empty_server_checker_thread_cnt, "EmptSvrCheck", ObWaitEventIds::EMPTY_SERVER_CHECK_COND_WAIT))) {
    LOG_WARN("create empty server checker thread failed", K(ret),
             K(empty_server_checker_thread_cnt));
  } else {
    server_mgr_ = &server_mgr;
    lst_operator_ = &lst_operator;
    schema_service_ = &schema_service;
    unit_mgr_ = &unit_mgr;
    server_zone_op_service_ = &server_zone_op_service;
    empty_servers_.reset();
    need_check_ = true;
    inited_ = true;
  }
  return ret;
}

void ObEmptyServerChecker::run3()
{
  LOG_INFO("empty server checker start");
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    int64_t wait_time_ms = 0;
    while (!stop_) {
      ret = OB_SUCCESS;
      ObThreadCondGuard guard(get_cond());
      wait_time_ms = 10 * 1000;//10s
      if (OB_FAIL(try_delete_server_())) {
        LOG_WARN("failed to delete server", KR(ret));
      }
      if (OB_SUCC(ret) && !stop_ && !need_check_) {
        wait_time_ms = 100;
      }
      if (OB_SUCCESS != idle_wait(wait_time_ms)) {
          LOG_DEBUG("wait timeout", K(wait_time_ms));
      }
    }
  }
  LOG_INFO("empty server checker stop");
}

int ObEmptyServerChecker::try_delete_server_()
{
  int ret = OB_SUCCESS;
  ObZone zone; // empty means all zones
  ObArray<ObServerInfoInTable> servers_info;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(server_mgr_) || OB_ISNULL(unit_mgr_) || OB_ISNULL(server_zone_op_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", KR(ret), KP(server_mgr_), KP(unit_mgr_), KP(server_zone_op_service_));
  } else if (OB_FAIL(SVR_TRACER.get_servers_info(zone, servers_info))) {
    LOG_WARN("get_servers_info failed", KR(ret), K(zone));
  } else {
    need_check_ = false;
    empty_servers_.reset();
    FOREACH_CNT_X(server_info, servers_info, OB_SUCC(ret)) {
      if (server_info->is_deleting()) {
        need_check_ = true;
        bool server_empty = false;
        const ObAddr &addr= server_info->get_server();
        if (OB_FAIL(unit_mgr_->check_server_empty(addr, server_empty))) {
          LOG_WARN("check_server_empty failed", "server", addr, KR(ret));
        } else if (!server_empty) {
          LOG_INFO("server not empty and has units on it", "server", addr, KR(ret));
        } else if (OB_FAIL(empty_servers_.push_back(addr))) {
          LOG_WARN("failed to push back empty server", KR(ret), KPC(server_info));
        }
      }
    }
    DEBUG_SYNC(END_DELETE_SERVER_BEFORE_CHECK_META_TABLE);
    if (OB_SUCC(ret) && empty_servers_.count() > 0) {
      //need check empty
      if (OB_FAIL(check_server_empty_())) {
        LOG_WARN("failed to check server empty", KR(ret));
      }
    }
    if (OB_SUCC(ret) && empty_servers_.count() > 0) {
      const bool commit = true;
      for (int64_t i = 0; OB_SUCC(ret) && i < empty_servers_.count(); ++i) {
        const ObAddr &addr = empty_servers_.at(i);
        if (!ObHeartbeatService::is_service_enabled()) { // the old logic
          LOG_INFO("sys tenant data version < 4.2, server manager executes end_delete_server");
          if (OB_FAIL(server_mgr_->end_delete_server(addr, zone, commit))) {
            LOG_WARN("server_mgr end_delete_server failed", KR(ret), K(addr), K(zone));
          }
        } else {
          LOG_INFO("sys tenant data version >= 4.2, server zone op service executes finish_delete_server");
          if (OB_FAIL(server_zone_op_service_->finish_delete_server(addr, zone))) {
            LOG_WARN("server_zone_op_service finish_delete_server failed", KR(ret), K(addr), K(zone));
          } else if (OB_FAIL(server_mgr_->load_server_manager())) {
            LOG_WARN("fail to load server manager", KR(ret));
          }
        }
      }
    }
  }
  return ret;
}

void ObEmptyServerChecker::wakeup()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    get_cond().broadcast();
  }
}

void ObEmptyServerChecker::stop()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObRsReentrantThread::stop();
    ObThreadCondGuard guard(get_cond());
    get_cond().broadcast();
  }
}

//check server not in meta table
int ObEmptyServerChecker::check_server_empty_()
{
  int ret = OB_SUCCESS;
  ObAllLSTableIterator iter;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (stop_) {
    ret = OB_CANCELED;
    LOG_WARN("cancle empty server check", KR(ret));
  } else if (OB_ISNULL(schema_service_) || OB_ISNULL(lst_operator_)) {
    ret  = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null", KR(ret), KP(schema_service_), KP(lst_operator_));
  } else if (OB_FAIL(iter.init(*lst_operator_, *schema_service_))) {
    LOG_WARN("failed to init iter", KR(ret));
  } else {
    ObLSInfo ls_info;
    ls_info.reset();
    while (!stop_ && OB_SUCC(ret) && empty_servers_.size() > 0) {
      if (OB_FAIL(iter.next(ls_info))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("iterate ls table failed", K(ret));
        }
        break;
      } else if (OB_FAIL(ObServerCheckUtils::check_server_empty_by_ls(ls_info, empty_servers_))) {
        LOG_WARN("failed to check server empty", KR(ret), K(ls_info), K(empty_servers_));
      }
    }
  }

  if (OB_SUCC(ret) && stop_) {
    ret = OB_CANCELED;
  }
  return ret;

}

}  // end namespace rootserver
} // end namespace oceanbase
