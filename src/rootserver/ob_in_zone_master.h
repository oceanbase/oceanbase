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

#ifndef OCEANBASE_ROOTSERVER_OB_IN_ZONE_MASTER_H_
#define OCEANBASE_ROOTSERVER_OB_IN_ZONE_MASTER_H_
#if 0
#include <stdint.h>

#include "share/ob_define.h"
#include "lib/task/ob_timer.h"
#include "lib/net/ob_addr.h"
#include "lib/thread/ob_work_queue.h"
#include "share/ob_lease_struct.h"
#include "ob_in_zone_hb_state_mgr.h"
#include "ob_in_zone_server_tracer.h"
#include "ob_zone_recovery_task_mgr.h"
#include "ob_zone_server_recovery_machine.h"

namespace oceanbase
{

namespace obrpc
{
class ObSrvRpcProxy;
}
namespace rootserver
{
class ObInZoneMaster;

class ObInZoneMasterTakeOverTask : public common::ObAsyncTimerTask
{
public:
  ObInZoneMasterTakeOverTask(ObInZoneMaster &in_zone_master,
                             common::ObWorkQueue &work_queue)
    : common::ObAsyncTimerTask(work_queue),
      in_zone_master_(in_zone_master),
      work_queue_(work_queue) {}
  virtual ~ObInZoneMasterTakeOverTask() {}
public:
  virtual int process() override;
  virtual int64_t get_deep_copy_size() const override;
  virtual share::ObAsyncTask *deep_copy(char *buf, const int64_t buf_size) const override;
private:
  ObInZoneMaster &in_zone_master_;
  common::ObWorkQueue &work_queue_;
};

class ObInZoneMasterRevokeTask : public common::ObAsyncTimerTask
{
public:
  ObInZoneMasterRevokeTask(ObInZoneMaster &in_zone_master,
                           common::ObWorkQueue &work_queue)
    : common::ObAsyncTimerTask(work_queue),
      in_zone_master_(in_zone_master),
      work_queue_(work_queue) {}
  virtual ~ObInZoneMasterRevokeTask() {}
public:
  virtual int process() override;
  virtual int64_t get_deep_copy_size() const override;
  virtual share::ObAsyncTask *deep_copy(char *buf, const int64_t buf_size) const override;
private:
  ObInZoneMaster &in_zone_master_;
  common::ObWorkQueue &work_queue_;
};

class ObInZoneServerTracerTask : public common::ObAsyncTimerTask
{
public:
  ObInZoneServerTracerTask(ObInZoneMaster &in_zone_master,
                           common::ObWorkQueue &work_queue,
                           const common::ObAddr &server)
    : common::ObAsyncTimerTask(work_queue),
      in_zone_master_(in_zone_master),
      work_queue_(work_queue),
      server_(server) {}
  virtual ~ObInZoneServerTracerTask() {}
public:
  virtual int process() override;
  virtual int64_t get_deep_copy_size() const override;
  virtual share::ObAsyncTask *deep_copy(char *buf, const int64_t buf_size) const override;
private:
  ObInZoneMaster &in_zone_master_;
  common::ObWorkQueue &work_queue_;
  common::ObAddr server_;
};

// take over:IDLE->TAKING_OVER->WORKING
// revoke:WORKING->REVOKING->IDLE
enum InZoneMasterWorkingStatus
{
  IN_ZONE_MASTER_IDLE = 0,
  IN_ZONE_MASTER_TAKING_OVER,
  IN_ZONE_MASTER_WORKING,
  IN_ZONE_MASTER_REVOKING,
  IN_ZONE_MASTER_MAX,
};

class ObInZoneMaster
{
public:
  ObInZoneMaster()
    : inited_(false),
      self_addr_(),
      zone_server_recovery_machine_(in_zone_server_tracer_, zone_recovery_task_mgr_),
      zone_recovery_task_mgr_(),
      in_zone_server_tracer_(),
      in_zone_hb_checker_(),
      in_zone_master_addr_mgr_(),
      in_zone_master_locker_(),
      in_zone_hb_state_mgr_(),
      task_queue_(),
      take_over_task_(*this, task_queue_),
      revoke_task_(*this, task_queue_),
      working_status_(IN_ZONE_MASTER_MAX) {}
  virtual ~ObInZoneMaster() {}
public:
  int init(
      const common::ObAddr &self_addr,
      common::ObServerConfig &config,
      obrpc::ObSrvRpcProxy *rpc_proxy);
  void destroy();
public:
  int try_trigger_stop_in_zone_master();
  int try_trigger_restart_in_zone_master();
  int on_server_takenover_by_in_zone_master(
      const common::ObAddr &server);
  int receive_in_zone_heartbeat(
      const share::ObInZoneHbRequest &in_zone_hb_request,
      share::ObInZoneHbResponse &in_zone_hb_response);
  int takeover_permanent_offline_server(
      const common::ObAddr &server);
  int restart_in_zone_master();
  int stop_in_zone_master();
  int get_server_or_preprocessor(
      const common::ObAddr &server,
      common::ObAddr &rescue_server,
      share::ServerPreProceStatus &ret_code);
  int pre_process_server_reply(
      const obrpc::ObPreProcessServerReplyArg &arg);
  int register_self_busy_wait();
private:
  static const int64_t TASK_QUEUE_SIZE = 16;
  static const int64_t TASK_THREAD_NUM = 1;
private:
  bool inited_;
  common::ObAddr self_addr_;
  ObZoneServerRecoveryMachine zone_server_recovery_machine_;
  ObZoneRecoveryTaskMgr zone_recovery_task_mgr_;
  ObInZoneServerTracer in_zone_server_tracer_;
  ObInZoneHeartbeatChecker in_zone_hb_checker_;
  ObInZoneMasterAddrMgr in_zone_master_addr_mgr_;
  ObInZoneMasterLocker in_zone_master_locker_;
  ObInZoneHbStateMgr in_zone_hb_state_mgr_;
  common::ObWorkQueue task_queue_;
  ObInZoneMasterTakeOverTask take_over_task_;
  ObInZoneMasterRevokeTask revoke_task_;
  InZoneMasterWorkingStatus working_status_;
};

}//end namespace rootserver
}//end namespace oceanbase
#endif  // 0
#endif  // OCEANBASE_ROOTSERVER_OB_IN_ZONE_MASTER_H_
