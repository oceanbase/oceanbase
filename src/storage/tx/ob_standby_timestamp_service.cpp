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

#include "ob_standby_timestamp_service.h"
#include "share/rc/ob_tenant_base.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "ob_timestamp_access.h"
#include "ob_timestamp_service.h"
#include "logservice/ob_log_service.h"
#include "share/scn.h"
#include "observer/ob_server_struct.h"
#include "observer/ob_srv_network_frame.h"
#include "storage/tx/ob_trans_service.h"
#include "rootserver/ob_tenant_info_loader.h"

namespace oceanbase
{
namespace transaction
{

int ObStandbyTimestampService::init(rpc::frame::ObReqTransport *req_transport)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
  } else {
    tenant_id_ = MTL_ID();
    self_ = GCTX.self_addr();
    if (OB_FAIL(rpc_.init(req_transport, self_))) {
      TRANS_LOG(WARN, "rpc init fail", K(ret), K_(self), K_(tenant_id));
    } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::StandbyTimestampService, tg_id_))) {
      TRANS_LOG(ERROR, "create tg failed", K(ret));
    } else if (OB_FAIL(TG_SET_RUNNABLE(tg_id_, *this))) {
      TRANS_LOG(ERROR, "start standby timestamp service thread pool fail", K(ret), K_(tenant_id));
    } else {
      inited_ = true;
      TRANS_LOG(INFO, "standby timestamp service init succ", K_(tenant_id));
    }
  }

  return ret;
}

int ObStandbyTimestampService::mtl_init(ObStandbyTimestampService *&sts)
{
  int ret = OB_SUCCESS;
  rpc::frame::ObReqTransport *req_transport = GCTX.net_frame_->get_req_transport();
  if (OB_ISNULL(req_transport)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KP(req_transport));
  } else {
    ret = sts->init(req_transport);
  }
  return ret;
}

int ObStandbyTimestampService::start()
{
  int ret = OB_SUCCESS;

  if (!inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObStandbyTimestampService not init", K(ret), K_(tenant_id));
  } else if (OB_FAIL(TG_START(tg_id_))) {
    TRANS_LOG(WARN, "ObStandbyTimestampService start error", K(ret), K_(tenant_id));
  } else if (OB_FAIL(rpc_.start())) {
    TRANS_LOG(WARN, "ObStandbyTimestampService rpc start error", K(ret), K_(tenant_id));
  } else {
    // do nothing
  }

  return ret;
}

void ObStandbyTimestampService::stop()
{
  TG_STOP(tg_id_);
  rpc_.stop();
}

void ObStandbyTimestampService::wait()
{
  TG_WAIT(tg_id_);
  rpc_.wait();
}

void ObStandbyTimestampService::destroy()
{
  if (inited_) {
    inited_ = false;
    tenant_id_ = OB_INVALID_ID;
    //TODO(SCN):zhaoxing last_id should be uint64_t
    last_id_ = OB_INVALID_VERSION;
    epoch_ = OB_INVALID_TIMESTAMP;
    switch_to_leader_ts_ = OB_INVALID_TIMESTAMP;
    TG_DESTROY(tg_id_);
    rpc_.destroy();
    TRANS_LOG(INFO, "standby timestamp service destroy", K_(tenant_id));
  }
}

int ObStandbyTimestampService::query_and_update_last_id()
{
  int ret = OB_SUCCESS;
  SCN standby_scn;
  int64_t switch_to_leader_ts = ATOMIC_LOAD(&switch_to_leader_ts_);
  int64_t query_ts = OB_INVALID_TIMESTAMP != switch_to_leader_ts ? switch_to_leader_ts : 0;
  if (OB_FAIL(MTL(rootserver::ObTenantInfoLoader *)->get_valid_sts_after(query_ts, standby_scn))) {
    if (print_error_log_interval_.reach()) {
      TRANS_LOG(INFO, "tenant info is invalid", K(ret), K(query_ts), K(standby_scn), KPC(this));
    }
  } else if (!standby_scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(query_ts), K(standby_scn), KPC(this));
  } else {
    if (last_id_ > 0 && (standby_scn.get_val_for_gts() < last_id_)) {
      TRANS_LOG(ERROR, "snapshot rolls back ", K_(switch_to_leader_ts), K(standby_scn), K_(last_id));
    } else {
      inc_update(&last_id_, (int64_t)standby_scn.get_val_for_gts());
      ATOMIC_BCAS(&switch_to_leader_ts_, switch_to_leader_ts, OB_INVALID_TIMESTAMP);
    }
  }
  if (print_id_log_interval_.reach()) {
    TRANS_LOG(INFO, "ObStandbyTimestampService state", K(*this));
  }
  return ret;
}

void ObStandbyTimestampService::run1()
{
  lib::set_thread_name("STSWorker");
  TRANS_LOG(INFO, "ObStandbyTimestampService thread start", K_(tenant_id));
  int ret = OB_SUCCESS;
  while (is_user_tenant(tenant_id_) && !has_set_stop()) {
    int64_t begin_tstamp = ObTimeUtility::current_time();
    if (OB_FAIL(query_and_update_last_id())) {
      TRANS_LOG(WARN, "query and update last id fail", KR(ret));
    }
    int64_t end_tstamp = ObTimeUtility::current_time();
    int64_t wait_interval = GCONF.weak_read_version_refresh_interval - (end_tstamp - begin_tstamp);
    if (wait_interval > 0) {
      ob_usleep(wait_interval);
    }
  }
  TRANS_LOG(INFO, "ObStandbyTimestampService thread end", K_(tenant_id));
}

int ObStandbyTimestampService::switch_to_follower_gracefully()
{
  int64_t type = MTL(ObTimestampAccess *)->get_service_type();
  if (ObTimestampAccess::ServiceType::STS_LEADER == type) {
    MTL(ObTimestampAccess *)->set_service_type(ObTimestampAccess::ServiceType::FOLLOWER);
  }
  TRANS_LOG(INFO, "ObStandbyTimestampService switch to follower gracefully success", K(type), "service_type", MTL(ObTimestampAccess *)->get_service_type(), KPC(this));
  return OB_SUCCESS;
}

void ObStandbyTimestampService::switch_to_follower_forcedly()
{
  int64_t type = MTL(ObTimestampAccess *)->get_service_type();
  if (ObTimestampAccess::ServiceType::STS_LEADER == type) {
    MTL(ObTimestampAccess *)->set_service_type(ObTimestampAccess::ServiceType::FOLLOWER);
  }
  TRANS_LOG(INFO, "ObStandbyTimestampService switch to follower forcedly success", K(type), "service_type", MTL(ObTimestampAccess *)->get_service_type(), KPC(this));
}

int ObStandbyTimestampService::resume_leader()
{
  int64_t type = MTL(ObTimestampAccess *)->get_service_type();
  if (ObTimestampAccess::ServiceType::FOLLOWER == type) {
    MTL(ObTimestampAccess *)->set_service_type(ObTimestampAccess::ServiceType::STS_LEADER);
  }
  (void)query_and_update_last_id();
  TRANS_LOG(INFO, "ObStandbyTimestampService resume leader success", K(type), "service_type", MTL(ObTimestampAccess *)->get_service_type(), KPC(this));
  return OB_SUCCESS;
}

int ObStandbyTimestampService::switch_to_leader()
{
  int ret = OB_SUCCESS;
  common::ObRole role;
  int64_t tmp_epoch = OB_INVALID_TIMESTAMP;
  if (OB_FAIL(MTL(logservice::ObLogService *)->get_palf_role(share::GTS_LS, role, tmp_epoch))) {
    TRANS_LOG(WARN, "get ObStandbyTimestampService role fail", KR(ret));
  } else {
    ATOMIC_STORE(&switch_to_leader_ts_, ObClockGenerator::getClock());
    epoch_ = tmp_epoch;
    int64_t type = MTL(ObTimestampAccess *)->get_service_type();
    if (ObTimestampAccess::ServiceType::FOLLOWER == type) {
      MTL(ObTimestampAccess *)->set_service_type(ObTimestampAccess::ServiceType::STS_LEADER);
    }
    (void)query_and_update_last_id();
    TRANS_LOG(INFO, "ObStandbyTimestampService switch to leader success", K(type), "service_type", MTL(ObTimestampAccess *)->get_service_type(), KPC(this));
  }
  return ret;
}

int ObStandbyTimestampService::handle_request(const ObGtsRequest &request, obrpc::ObGtsRpcResult &result)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!request.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(request));
  } else {
    TRANS_LOG(DEBUG, "handle gts request", K(request));
    int64_t gts = 0;
    const MonotonicTs srr = request.get_srr();
    const uint64_t tenant_id = request.get_tenant_id();
    const ObAddr &requester = request.get_sender();
    if (requester == self_) {
     // Go local call to get gts
     TRANS_LOG(DEBUG, "handle local gts request", K(requester));
     ret = handle_local_request_(request, result);
    } else if (OB_FAIL(get_number(gts))) {
      TRANS_LOG(WARN, "get timestamp failed", KR(ret));
      int tmp_ret = OB_SUCCESS;
      ObGtsErrResponse response;
      if (OB_SUCCESS != (tmp_ret = result.init(tenant_id, ret, srr, 0, 0))) {
        TRANS_LOG(WARN, "gts result init failed", K(tmp_ret), K(request));
      } else if (OB_SUCCESS != (tmp_ret = response.init(tenant_id, srr, ret, self_))) {
        TRANS_LOG(WARN, "gts err response init failed", K(tmp_ret), K(request));
      } else if (OB_SUCCESS != (tmp_ret = rpc_.post(tenant_id, requester, response))) {
        TRANS_LOG(WARN, "post gts err response failed", K(tmp_ret), K(response));
      } else {
        TRANS_LOG(DEBUG, "post gts err response success", K(response));
      }
    } else {
      if (OB_FAIL(result.init(tenant_id, ret, srr, gts, gts))) {
        TRANS_LOG(WARN, "gts result init failed", KR(ret), K(request));
      }
    }
  }
  return ret;
}

int ObStandbyTimestampService::handle_local_request_(const ObGtsRequest &request, obrpc::ObGtsRpcResult &result)
{
  int ret = OB_SUCCESS;
  int64_t gts = 0;
  const uint64_t tenant_id = request.get_tenant_id();
  const MonotonicTs srr = request.get_srr();
  if (OB_FAIL(get_number(gts))) {
    if (EXECUTE_COUNT_PER_SEC(10)) {
      TRANS_LOG(WARN, "get timestamp failed", KR(ret));
    }
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = result.init(tenant_id, ret, srr, 0, 0))) {
      TRANS_LOG(WARN, "gts result init failed", K(tmp_ret), K(request));
    }
  } else {
    if (OB_FAIL(result.init(tenant_id, ret, srr, gts, gts))) {
      TRANS_LOG(WARN, "local gts result init failed", KR(ret), K(request));
    }
  }
  return ret;
}

int ObStandbyTimestampService::get_number(int64_t &gts)
{
  int ret = OB_SUCCESS;
  bool leader = false;
  if (OB_FAIL(check_leader(leader))) {
    TRANS_LOG(WARN, "check leader fail", K(ret), KPC(this));
  } else if (!leader) {
    ret = OB_NOT_MASTER;
    if (EXECUTE_COUNT_PER_SEC(10)) {
      TRANS_LOG(WARN, "ObStandbyTimestampService is not leader", K(ret), KPC(this));
    }
  } else if (OB_INVALID_TIMESTAMP != ATOMIC_LOAD(&switch_to_leader_ts_)) {
    ret = OB_GTS_NOT_READY;
    if (EXECUTE_COUNT_PER_SEC(10)) {
      TRANS_LOG(WARN, "ObStandbyTimestampService is not serving", K(ret), KPC(this));
    }
  } else {
    gts = ATOMIC_LOAD(&last_id_);
    if (OB_INVALID_VERSION == gts) {
      ret = OB_GTS_NOT_READY;
      if (EXECUTE_COUNT_PER_SEC(10)) {
        TRANS_LOG(WARN, "ObStandbyTimestampService is not ready", K(ret));
      }
    }
  }
  return ret;
}

int ObStandbyTimestampService::check_leader(bool &leader)
{
  int ret = OB_SUCCESS;
  common::ObRole role;
  int64_t tmp_epoch = OB_INVALID_TIMESTAMP;
  if (OB_FAIL(MTL(logservice::ObLogService *)->get_palf_role(share::GTS_LS, role, tmp_epoch))) {
    TRANS_LOG(WARN, "get ObStandbyTimestampService role fail", KR(ret));
  } else if (role == LEADER && tmp_epoch == epoch_) {
    leader = true;
  } else {
    leader = false;
  }
  return ret;
}

void ObStandbyTimestampService::get_virtual_info(int64_t &ts_value, common::ObRole &role, int64_t &proposal_id)
{
  int ret = OB_SUCCESS;
  ts_value = last_id_;
  bool is_leader = false;
  if (OB_FAIL(MTL(logservice::ObLogService *)->get_palf_role(share::GTS_LS, role, proposal_id))) {
    TRANS_LOG(WARN, "get ObStandbyTimestampService role fail", KR(ret));
  } else if (role == LEADER && proposal_id != epoch_) {
    role = FOLLOWER;
    proposal_id = epoch_;
  }
  TRANS_LOG(INFO, "sts get virtual info", K(ret), K_(last_id), K(ts_value),
                  K(role), K(proposal_id), K_(epoch), K_(switch_to_leader_ts));
}

}
}
