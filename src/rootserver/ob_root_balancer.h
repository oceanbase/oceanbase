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

#ifndef OCEANBASE_ROOTSERVER_OB_ROOT_BALANCER_H_
#define OCEANBASE_ROOTSERVER_OB_ROOT_BALANCER_H_

#include "share/ob_define.h"
#include "rootserver/ob_rs_reentrant_thread.h"      // ObRsReentrantThread
#include "ob_thread_idling.h"                       // ObThreadIdling
#include "ob_unit_stat_manager.h"                   // ObUnitStatManager
#include "ob_server_balancer.h"                     // ObServerBalancer
#include "rootserver/ob_disaster_recovery_worker.h" // ObDRWorker
#include "rootserver/ob_rootservice_util_checker.h" // ObRootServiceUtilChecker

namespace oceanbase
{
namespace common
{
class ObServerConfig;
}
namespace share
{
namespace schema
{
class ObMultiVersionSchemaService;
}
}
namespace rootserver
{
class ObUnitManager;
class ObServerManager;
class ObZoneManager;
class ObRootBalancer;
class ObDRTaskMgr;

class ObRootBalanceIdling : public ObThreadIdling
{
public:
  explicit ObRootBalanceIdling(volatile bool &stop, const ObRootBalancer &host)
    : ObThreadIdling(stop), host_(host) {}

  virtual int64_t get_idle_interval_us();
private:
  const ObRootBalancer &host_;
};

class ObRootBalancer : public ObRsReentrantThread, public share::ObCheckStopProvider
{
public:
  ObRootBalancer();
  virtual ~ObRootBalancer();
  int init(common::ObServerConfig &cfg,
      share::schema::ObMultiVersionSchemaService &schema_service,
      ObUnitManager &unit_mgr,
      ObServerManager &server_mgr,
      ObZoneManager &zone_mgr,
      obrpc::ObSrvRpcProxy &rpc_proxy,
      common::ObAddr &self_addr,
      common::ObMySQLProxy &sql_proxy,
      ObDRTaskMgr &dr_task_mgr);

  virtual void run3() override;
  virtual int blocking_run() { BLOCKING_RUN_IMPLEMENT(); }


  // main entry, never exit.
  virtual int do_balance();
  // do balance for all tenant
  virtual int all_balance();

  void stop();
  void wakeup();

  bool is_inited() const { return inited_; }
  bool is_stop() const { return stop_; }
  void set_active();
  // return OB_CANCELED if stop, else return OB_SUCCESS
  int check_stop() const;
  int idle() const;
  int64_t get_schedule_interval() const;
  ObDRWorker &get_disaster_recovery_worker() { return disaster_recovery_worker_; }
private:
  static const int64_t LOG_INTERVAL = 30 * 1000 * 1000;
private:
  bool inited_;
  volatile int64_t active_;
  mutable ObRootBalanceIdling idling_;

  ObServerBalancer server_balancer_;

  ObDRWorker disaster_recovery_worker_;
  ObRootServiceUtilChecker rootservice_util_checker_;
};
} // end namespace rootserver
} // end namespace oceanbase

#endif // OCEANBASE_ROOTSERVER_OB_ROOT_BALANCER_H_
