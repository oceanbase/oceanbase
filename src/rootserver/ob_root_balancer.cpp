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

#define USING_LOG_PREFIX RS_LB

#include "ob_root_balancer.h"

#include "lib/ob_define.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/stat/ob_diagnose_info.h"
#include "share/config/ob_server_config.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/ob_unit_replica_counter.h"
#include "rootserver/ob_rs_event_history_table_operator.h"
#include "rootserver/ob_root_service.h"
#include "observer/ob_server_struct.h"
#include "ob_unit_manager.h"
#include "ob_balance_info.h"

namespace oceanbase
{

using namespace common;
using namespace share;
using namespace share::schema;
using namespace obrpc;
namespace rootserver
{
using namespace balancer;

int64_t ObRootBalanceIdling::get_idle_interval_us()
{
  const int64_t min_idle_time = 10 * 1000000;
  return min_idle_time;
}

ObRootBalancer::ObRootBalancer()
  : ObRsReentrantThread(true), inited_(false), active_(0), idling_(stop_, *this),
    server_balancer_(),
    disaster_recovery_worker_(stop_),
    rootservice_util_checker_(stop_)
{
}

ObRootBalancer::~ObRootBalancer()
{
}

int ObRootBalancer::init(common::ObServerConfig &cfg,
    share::schema::ObMultiVersionSchemaService &schema_service,
    ObUnitManager &unit_mgr,
    ObServerManager &server_mgr,
    ObZoneManager &zone_mgr,
    ObSrvRpcProxy &rpc_proxy,
    ObAddr &self_addr,
    ObMySQLProxy &sql_proxy,
    ObDRTaskMgr &dr_task_mgr)
{
  int ret = OB_SUCCESS;
  static const int64_t root_balancer_thread_cnt = 1;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("root balancer already inited", K(ret));
  } else if (OB_UNLIKELY(nullptr == GCTX.lst_operator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lst_operator_ ptr is null", KR(ret));
  } else if (OB_FAIL(server_balancer_.init(schema_service, unit_mgr,
                                           zone_mgr, server_mgr, sql_proxy))) {
    LOG_WARN("init failed", K(ret));
  } else if (OB_FAIL(create(root_balancer_thread_cnt, "RootBalance"))) {
    LOG_WARN("create root balancer thread failed", K(ret), K(root_balancer_thread_cnt));
  } else if (OB_FAIL(disaster_recovery_worker_.init(
          self_addr, cfg, zone_mgr,
          dr_task_mgr, *GCTX.lst_operator_, schema_service, rpc_proxy, sql_proxy))) {
    LOG_WARN("fail to init disaster recovery worker", KR(ret));
  } else if (OB_FAIL(rootservice_util_checker_.init(
          unit_mgr, zone_mgr, *GCTX.rs_rpc_proxy_, self_addr, schema_service, sql_proxy, *GCTX.lst_operator_))) {
    LOG_WARN("fail to init rootservice util checker", KR(ret));
  } else {
    inited_ = true;
  }
  return ret;
}

int ObRootBalancer::idle() const
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(idling_.idle())) {
    LOG_WARN("idle failed", K(ret));
  } else {
    LOG_INFO("root balance idle", "idle_time", idling_.get_idle_interval_us());
  }
  return ret;
}

void ObRootBalancer::wakeup()
{
  if (!inited_) {
    LOG_WARN_RET(OB_NOT_INIT, "not init");
  } else {
    idling_.wakeup();
  }
}

void ObRootBalancer::stop()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObRsReentrantThread::stop();
    idling_.wakeup();
  }
}

int ObRootBalancer::check_stop() const
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ret = stop_ ? OB_CANCELED : OB_SUCCESS;
  }
  return ret;
}

void ObRootBalancer::set_active()
{
  const int64_t old_val = ATOMIC_SET(&active_, 1);
  UNUSED(old_val);
  wakeup();
}

void ObRootBalancer::run3()
{
  LOG_INFO("root balance start");
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(do_balance())) {
    LOG_WARN("all balance failed", K(ret));
  }
  LOG_INFO("root balance stop", K(ret), K(stop_));
}

int ObRootBalancer::do_balance()
{
  int ret = OB_SUCCESS;
  int64_t failed_times = 0;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  }

  ObArray<uint64_t> all_tenant;
  while (OB_SUCC(ret) && !stop_) {
    update_last_run_timestamp();

    if (!ATOMIC_LOAD(&active_)) {
      if (OB_FAIL(idle())) {
        LOG_WARN("idle failed", K(ret));
        break;
      } else {
        continue;
      }
    }
    if (OB_FAIL(all_balance())) {
      failed_times++;
      LOG_WARN("do balance round failed", K(ret), K(failed_times));
      ret = OB_SUCCESS;
      EVENT_ADD(RS_BALANCER_FAIL_COUNT, 1);
    } else {
      failed_times = 0;
      EVENT_ADD(RS_BALANCER_SUCC_COUNT, 1);
    }

    // idle after success or failed 2 times
    if (0 == failed_times || failed_times >= 2) {
      if (OB_FAIL(idle())) {
        LOG_WARN("idle failed", K(ret));
      }
    }
  }
  return ret;
}

int ObRootBalancer::all_balance()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    if (OB_SUCC(ret)) {
      if (OB_FAIL(disaster_recovery_worker_.try_disaster_recovery())) {
        LOG_WARN("fail to try disaster recovery", KR(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(rootservice_util_checker_.rootservice_util_check())) {
        LOG_WARN("fail to do rootservice util check", KR(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_SUCCESS != (tmp_ret = server_balancer_.balance_servers())) {
        LOG_WARN("do unit balance failed", K(tmp_ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(server_balancer_.tenant_group_balance())) {
        LOG_WARN("fail to do tenant group balance", KR(ret));
      }
    }
  }
  return ret;
}

int64_t ObRootBalancer::get_schedule_interval() const
{
  return idling_.get_idle_interval_us();
}


} // end namespace rootserver
} // end namespace oceanbase
