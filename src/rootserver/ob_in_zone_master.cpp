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

#include "ob_in_zone_master.h"
#include "share/ob_srv_rpc_proxy.h"
#include "observer/ob_server.h"

namespace oceanbase
{
using namespace common;
using namespace obrpc;
using namespace share;
namespace rootserver
{
// ============== ObInZoneMasterTakeOverTask ==============
int ObInZoneMasterTakeOverTask::process()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(in_zone_master_.restart_in_zone_master())) {
    LOG_WARN("fail to start in zone master", K(ret));
  }
  return ret;
}

int64_t ObInZoneMasterTakeOverTask::get_deep_copy_size() const
{
  return sizeof(*this);
}

share::ObAsyncTask *ObInZoneMasterTakeOverTask::deep_copy(
    char *buf, const int64_t buf_size) const
{
  ObInZoneMasterTakeOverTask *task = nullptr;
  if (nullptr == buf || buf_size < get_deep_copy_size()) {
    int tmp_ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf is null or buf size is not enought", "ret", tmp_ret,
             KP(buf), K(buf_size), "need_size", get_deep_copy_size());
  } else {
    task = new (buf) ObInZoneMasterTakeOverTask(in_zone_master_, work_queue_);
  }
  return task;
}

// ============= ObInZoneMasterRevokeTask =================
int ObInZoneMasterRevokeTask::process()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(in_zone_master_.stop_in_zone_master())) {
    LOG_WARN("fail to stop in zone master", K(ret));
  }
  return ret;
}

int64_t ObInZoneMasterRevokeTask::get_deep_copy_size() const
{
  return sizeof(*this);
}

share::ObAsyncTask *ObInZoneMasterRevokeTask::deep_copy(
    char *buf, const int64_t buf_size) const
{
  ObInZoneMasterRevokeTask *task = nullptr;
  if (nullptr == buf || buf_size < get_deep_copy_size()) {
    int tmp_ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf is null or buf size is not enough", "ret", tmp_ret,
             KP(buf), K(buf_size), "need_size", get_deep_copy_size());
  } else {
    task = new (buf) ObInZoneMasterRevokeTask(in_zone_master_, work_queue_);
  }
  return task;
}

// ============= ObInZoneServerTracerTask ==================
int ObInZoneServerTracerTask::process()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(in_zone_master_.takeover_permanent_offline_server(server_))) {
    LOG_WARN("fail to takeover server", K(ret));
  }
  return ret;
}

int64_t ObInZoneServerTracerTask::get_deep_copy_size() const
{
  return sizeof(*this);
}

share::ObAsyncTask *ObInZoneServerTracerTask::deep_copy(
    char *buf, const int64_t buf_size) const
{
  ObInZoneServerTracerTask *task = nullptr;
  if (nullptr == buf || buf_size < get_deep_copy_size()) {
    int tmp_ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf is null or buf size is not enough", "ret", tmp_ret,
             KP(buf), K(buf_size), "need_size", get_deep_copy_size());
  } else {
    task = new (buf) ObInZoneServerTracerTask(in_zone_master_, work_queue_, server_);
  }
  return task;
}

int ObInZoneMaster::init(
    const common::ObAddr &self_addr,
    common::ObServerConfig &config,
    obrpc::ObSrvRpcProxy *rpc_proxy)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(nullptr == rpc_proxy || !self_addr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(self_addr));
  } else if (OB_FAIL(zone_server_recovery_machine_.init(rpc_proxy))) {
    LOG_WARN("fail to init zone server recovery machine", K(ret));
  } else if (OB_FAIL(zone_recovery_task_mgr_.init(
          config, &in_zone_server_tracer_, rpc_proxy, &zone_server_recovery_machine_))) {
    LOG_WARN("fail to init zone recovery task mgr", K(ret));
  } else if (OB_FAIL(in_zone_server_tracer_.init(&config, rpc_proxy, this, self_addr))) {
    LOG_WARN("fail to init in zone server tracer", K(ret));
  } else if (OB_FAIL(in_zone_hb_checker_.init(in_zone_server_tracer_))) {
    LOG_WARN("fail to init in zone heartbeat checker", K(ret));
  } else if (OB_FAIL(in_zone_master_addr_mgr_.init())) {
    LOG_WARN("fail to init in zone master addr mgr", K(ret));
  } else if (OB_FAIL(in_zone_master_locker_.init(
          self_addr, *rpc_proxy, in_zone_master_addr_mgr_, *this))) {
    LOG_WARN("fail to init in zone master locker", K(ret));
  } else if (OB_FAIL(in_zone_hb_state_mgr_.init(
          self_addr, *rpc_proxy, in_zone_master_locker_, in_zone_master_addr_mgr_))) {
    LOG_WARN("fail to init in zone hb state mgr", K(ret));
  } else if (OB_FAIL(task_queue_.init(
          TASK_THREAD_NUM, TASK_QUEUE_SIZE, "InZoneMasterAsyncTask"))) {
    LOG_WARN("fail to init task queue", K(ret));
  } else {
    working_status_ = IN_ZONE_MASTER_IDLE;
    self_addr_ = self_addr;
    inited_ = true;
  }
  return ret;
}

int ObInZoneMaster::try_trigger_stop_in_zone_master()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    InZoneMasterWorkingStatus this_status = ATOMIC_LOAD(&working_status_);
    if (IN_ZONE_MASTER_TAKING_OVER == this_status || IN_ZONE_MASTER_WORKING == this_status) {
      if (task_queue_.exist_timer_task(revoke_task_)) {
        LOG_INFO("already have revoke task in queue");
      } else if (OB_FAIL(task_queue_.add_timer_task(
              revoke_task_, 0/*do not delay*/, false/*do not repeat*/))) {
        LOG_WARN("fail to schedule stop in zone master task", K(ret));
      } else {
        LOG_INFO("succeed to schedule stop in zone master task", K(ret));
      }
    }
  }
  return ret;
}

int ObInZoneMaster::try_trigger_restart_in_zone_master()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    InZoneMasterWorkingStatus this_status = ATOMIC_LOAD(&working_status_);
    if (IN_ZONE_MASTER_IDLE == this_status) {
      if (task_queue_.exist_timer_task(take_over_task_)) {
        LOG_INFO("already have take over task in queue");
      } else if (OB_FAIL(task_queue_.add_timer_task(
              take_over_task_, 0/*do not delay*/, false/*do not repeat*/))) {
        LOG_WARN("fail to schedule restart in zone master task", K(ret));
      } else {
        LOG_INFO("succeed to schedule restart in zone master task", K(ret));
      }
    }
  }
  return ret;
}

int ObInZoneMaster::on_server_takenover_by_in_zone_master(
    const common::ObAddr &server)
{
  int ret = OB_SUCCESS;
  InZoneMasterWorkingStatus this_status = ATOMIC_LOAD(&working_status_);
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (IN_ZONE_MASTER_WORKING != this_status) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("in zone master not in service", K(ret));
  } else {
    ObInZoneServerTracerTask server_tracer_task(*this, task_queue_, server);
    server_tracer_task.set_retry_times(0);
    if (OB_FAIL(task_queue_.add_async_task(server_tracer_task))) {
      LOG_WARN("fail to add async task", K(ret));
    }
  }
  return ret;
}

int ObInZoneMaster::receive_in_zone_heartbeat(
    const ObInZoneHbRequest &in_zone_hb_request,
    ObInZoneHbResponse &in_zone_hb_response)
{
  int ret = OB_SUCCESS;
  InZoneMasterWorkingStatus this_status = ATOMIC_LOAD(&working_status_);
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("InZoneMaster not init", K(ret));
  } else if (IN_ZONE_MASTER_WORKING != this_status) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("in zone master not in service", K(ret));
  } else if (OB_FAIL(in_zone_server_tracer_.receive_in_zone_heartbeat(
          in_zone_hb_request, in_zone_hb_response))) {
    LOG_WARN("fail to execute receive in zone heartbeat", K(ret));
  }
  return ret;
}

int ObInZoneMaster::takeover_permanent_offline_server(
    const common::ObAddr &server)
{
  int ret = OB_SUCCESS;
  InZoneMasterWorkingStatus this_status = ATOMIC_LOAD(&working_status_);
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("InZoneMaster not init", K(ret));
  } else if (IN_ZONE_MASTER_WORKING != this_status) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("in zone master not in service", K(ret));
  } else if (OB_UNLIKELY(!server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(server));
  } else if (OB_FAIL(in_zone_server_tracer_.takeover_permanent_offline_server(server))) {
    LOG_WARN("fail to takeover permanent offline server", K(ret));
  }
  return ret;
}

int ObInZoneMaster::restart_in_zone_master()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("InZoneMaster not init", K(ret));
  } else if (!ATOMIC_BCAS(&working_status_, IN_ZONE_MASTER_IDLE, IN_ZONE_MASTER_TAKING_OVER)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("in zone master status not match", K(ret));
  }
  if (OB_SUCC(ret)) {
    // the restart and stop and processed in one single thread, the working_status shall not be
    // modified to values other than TAKING_OVER during restart
    if (IN_ZONE_MASTER_TAKING_OVER != ATOMIC_LOAD(&working_status_)) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("in zone master state not match", K(ret));
    } else if (OB_FAIL(in_zone_server_tracer_.reload())) {
      LOG_WARN("fail to reload in zone server tracer", K(ret));
    } else {
      LOG_INFO("in zone server tracer reload success");
    }
  }
  if (OB_SUCC(ret)) {
    if (IN_ZONE_MASTER_TAKING_OVER != ATOMIC_LOAD(&working_status_)) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("in zone master state not match", K(ret));
    } else if (OB_FAIL(in_zone_hb_checker_.start())) {
      LOG_WARN("fail to start in zone hb checker", K(ret));
    } else {
      LOG_INFO("start in zone hb checker success");
    }
  }
  if (OB_SUCC(ret)) {
    if (IN_ZONE_MASTER_TAKING_OVER != ATOMIC_LOAD(&working_status_)) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("in zone master state not match", K(ret));
    } else if (OB_FAIL(zone_recovery_task_mgr_.start())) {
      LOG_WARN("fail to start zone recovery task mgr", K(ret));
    } else {
      LOG_INFO("start zone recovery task mgr success");
    }
  }
  if (OB_SUCC(ret)) {
    if (IN_ZONE_MASTER_TAKING_OVER != ATOMIC_LOAD(&working_status_)) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("in zone master state not match", K(ret));
    } else if (OB_FAIL(zone_server_recovery_machine_.start())) {
      LOG_WARN("fail to start zone server recovery machine", K(ret));
    } else {
      LOG_INFO("start zone server recovery machine success");
    }
  }
  if (OB_SUCC(ret)) {
    if (!ATOMIC_BCAS(&working_status_, IN_ZONE_MASTER_TAKING_OVER, IN_ZONE_MASTER_WORKING)) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("in zone master status not match", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
    ATOMIC_STORE(&working_status_, IN_ZONE_MASTER_IDLE);
  }
  return ret;
}

int ObInZoneMaster::stop_in_zone_master()
{
  int ret = OB_SUCCESS;
  ATOMIC_STORE(&working_status_, IN_ZONE_MASTER_REVOKING);
  // wait
  in_zone_hb_checker_.wait();
  LOG_INFO("wait in zone hb checker success");
  zone_recovery_task_mgr_.wait();
  LOG_INFO("wait zone recovery task mgr success");
  zone_server_recovery_machine_.wait();
  LOG_INFO("wait zone server recovery machine success");
  // stop
  in_zone_hb_checker_.stop();
  LOG_INFO("stop in zone hb checker success");
  zone_recovery_task_mgr_.stop();
  zone_recovery_task_mgr_.reuse();
  LOG_INFO("stop zone recovery task mgr success");
  zone_server_recovery_machine_.stop();
  LOG_INFO("stop zone server recovery machine success");
  ATOMIC_STORE(&working_status_, IN_ZONE_MASTER_IDLE);
  return ret;
}

int ObInZoneMaster::get_server_or_preprocessor(
    const common::ObAddr &server,
    common::ObAddr &rescue_server,
    ServerPreProceStatus &ret_code)
{
  int ret = OB_SUCCESS;
  InZoneMasterWorkingStatus this_status = ATOMIC_LOAD(&working_status_);
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (IN_ZONE_MASTER_WORKING != this_status) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("in zone master not in service", K(ret));
  } else if (OB_FAIL(zone_server_recovery_machine_.get_server_or_preprocessor(
          server, rescue_server, ret_code))) {
    LOG_WARN("fail to get server or preprocessor");
  }
  return ret;
}

int ObInZoneMaster::pre_process_server_reply(
    const obrpc::ObPreProcessServerReplyArg &arg)
{
  int ret = OB_SUCCESS;
  InZoneMasterWorkingStatus this_status = ATOMIC_LOAD(&working_status_);
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (IN_ZONE_MASTER_WORKING != this_status) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("in zone master not in service", K(ret));
  } else if (OB_FAIL(zone_server_recovery_machine_.on_pre_process_server_reply(
          arg.server_, arg.rescue_server_, arg.ret_code_))) {
    LOG_WARN("fail to pre process server reply", K(ret), K(arg));
  }
  return ret;
}

int ObInZoneMaster::register_self_busy_wait()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(in_zone_master_locker_.register_self())) {
    LOG_WARN("fail to register self", K(ret));
  } else if (OB_FAIL(in_zone_hb_state_mgr_.register_self_busy_wait())) {
    LOG_WARN("fail to register self", K(ret));
  }
  return ret;
}

}//end namespace rootserver
}//end namespace oceanbase
#endif
