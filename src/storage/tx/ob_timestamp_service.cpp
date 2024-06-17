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

#include "ob_timestamp_service.h"
#include "ob_standby_timestamp_service.h"
#include "ob_trans_event.h"
#include "observer/ob_server_struct.h"
#include "observer/ob_srv_network_frame.h"
#include "ob_timestamp_access.h"
#include "storage/tx_storage/ob_ls_map.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "share/scn.h"

namespace oceanbase
{

using namespace oceanbase::share;
namespace transaction
{

int ObTimestampService::init(rpc::frame::ObReqTransport *req_transport)
{
  const ObAddr &self = GCTX.self_addr();
  self_ = self;
  service_type_ = ServiceType::TimestampService;
  pre_allocated_range_ = TIMESTAMP_PREALLOCATED_RANGE;
  ATOMIC_STORE(&last_gts_, 0);
  ATOMIC_STORE(&last_request_ts_, 0);
  ATOMIC_STORE(&check_gts_speed_lock_, 0);
  return rpc_.init(req_transport, self);
}

int ObTimestampService::mtl_init(ObTimestampService *&timestamp_service)
{
  int ret = OB_SUCCESS;
  rpc::frame::ObReqTransport *req_transport = GCTX.net_frame_->get_req_transport();
  if (OB_ISNULL(req_transport)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KP(req_transport));
  } else {
    ret = timestamp_service->init(req_transport);
  }
  return ret;
}

// The interface for getting gts timestamp, actually a wrapper of ObIDService::get_number.
//
// For most cases, the gts service uses the machine clock's time as gts timestamp, which means
// the gts service advances as fast as the machine clock. Howerver, when the gts service switches
// leader, the new leader will pre-allocate a range of timestamps, and this can lead to the gts
// timestamp becoming larger than the machine clock's time. Then the gts service will slow down to
// wait the machine clock. But we don't want the gts service to advance too slowly(when the request
// rate is low), since the observer may wait too long before the gts timestamp crosses log SCN.
// So we periodically check the gts service's advancing speed, and if it's far slower than the
// machine clock, we manually push the gts ahead.
int ObTimestampService::get_timestamp(int64_t &gts)
{
  int ret = OB_SUCCESS;
  int64_t unused_id;
  // 100ms
  const int64_t CHECK_INTERVAL = 100000000;
  const int64_t current_time = ObClockGenerator::getClock() * 1000;
  int64_t last_request_ts = ATOMIC_LOAD(&last_request_ts_);
  int64_t time_delta = current_time - last_request_ts;

  ret = get_number(1, current_time, gts, unused_id);

  if (OB_SUCC(ret)) {
    if ((last_request_ts == 0 || time_delta < 0) && ATOMIC_BCAS(&check_gts_speed_lock_, 0, 1)) {
      last_request_ts = ATOMIC_LOAD(&last_request_ts_);
      time_delta = current_time - last_request_ts;
      // before, we only do a fast check, and we should check again after we get the lock
      if (last_request_ts == 0 || time_delta < 0) {
        ATOMIC_STORE(&last_request_ts_, current_time);
        ATOMIC_STORE(&last_gts_, gts);
      }
      ATOMIC_STORE(&check_gts_speed_lock_, 0);
    } else if (time_delta > CHECK_INTERVAL && ATOMIC_BCAS(&check_gts_speed_lock_, 0, 1)) {
      last_request_ts = ATOMIC_LOAD(&last_request_ts_);
      time_delta = current_time - last_request_ts;
      // before, we only do a fast check, and we should check again after we get the lock
      if (time_delta > CHECK_INTERVAL) {
        const int64_t last_gts = ATOMIC_LOAD(&last_gts_);
        const int64_t gts_delta = gts - last_gts;
        const int64_t compensation_threshold = time_delta / 2;
        const int64_t compensation_value = time_delta / 10;
        // if the gts service advanced too slowly, then we add it up with `compensation_value`
        if (time_delta - gts_delta > compensation_threshold) {
          ret = get_number(compensation_value, current_time, gts, unused_id);
          TRANS_LOG(WARN, "the gts service advanced too slowly", K(ret), K(current_time),
              K(last_request_ts), K(time_delta), K(last_gts), K(gts), K(gts_delta),
              K(compensation_value));
        }
        if (OB_SUCC(ret)) {
          ATOMIC_STORE(&last_request_ts_, current_time);
          ATOMIC_STORE(&last_gts_, gts);
        }
        TRANS_LOG(DEBUG, "check the gts service advancing speed", K(ret), K(current_time),
            K(last_request_ts), K(time_delta), K(last_gts), K(gts), K(gts_delta),
            K(compensation_value));
      }
      ATOMIC_STORE(&check_gts_speed_lock_, 0);
    }
  }

  return ret;
}

int ObTimestampService::handle_request(const ObGtsRequest &request, ObGtsRpcResult &result)
{
  static int64_t total_cnt = 0;
  static int64_t total_rt = 0;
  static const int64_t STATISTICS_INTERVAL_US = 10000000;
  const MonotonicTs start = MonotonicTs::current_time();
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
    } else if (OB_FAIL(get_timestamp(gts))) {
      if (EXECUTE_COUNT_PER_SEC(10)) {
        TRANS_LOG(WARN, "get timestamp failed", KR(ret));
      }
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
  //obtain gts information:
  //(1) How many gts requests are processed per second
  //(2) How long does it take to process a request
  const MonotonicTs end = MonotonicTs::current_time();
  const int64_t cost_us = request.get_srr().mts_ - end.mts_;
  //Print the gts request that takes a long time for network transmission
  if (cost_us > 500 * 1000) {
    TRANS_LOG_RET(WARN, OB_ERR_TOO_MUCH_TIME, "gts request fly too much time", K(request), K(result), K(cost_us));
  }
  ATOMIC_INC(&total_cnt);
  ObTransStatistic::get_instance().add_gts_request_total_count(request.get_tenant_id(), 1);
  (void)ATOMIC_FAA(&total_rt, end.mts_ - start.mts_);
  if (REACH_TIME_INTERVAL(STATISTICS_INTERVAL_US)) {
    TRANS_LOG(INFO, "handle gts request statistics", K(total_rt), K(total_cnt),
        "avg_rt", (double)total_rt / (double)(total_cnt + 1),
        "avg_cnt", (double)total_cnt / (double)(STATISTICS_INTERVAL_US / 1000000));
    ATOMIC_STORE(&total_cnt, 0);
    ATOMIC_STORE(&total_rt, 0);
  }
  return ret;
}

#ifndef ERRSIM
ERRSIM_POINT_DEF(EN_GTS_HANDLE_REQUEST)
#endif

int ObTimestampService::handle_local_request_(const ObGtsRequest &request, obrpc::ObGtsRpcResult &result)
{
  int ret = OB_SUCCESS;
  int64_t gts = 0;
  const uint64_t tenant_id = request.get_tenant_id();
  const MonotonicTs srr = request.get_srr();
#ifndef ERRSIM
  ret = EN_GTS_HANDLE_REQUEST;
#endif
  // the fisrt case for errsim
  if (OB_SUCCESS != ret) {
    TRANS_LOG(WARN, "errsim for gts handle local request", KR(ret));
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = result.init(tenant_id, ret, srr, 0, 0))) {
      TRANS_LOG(WARN, "gts result init failed", K(tmp_ret), K(request));
    }
  } else if (OB_FAIL(get_timestamp(gts))) {
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

int ObTimestampService::switch_to_follower_gracefully()
{
  (void)ObIDService::switch_to_follower_gracefully();
  int64_t type = MTL(ObTimestampAccess *)->get_service_type();
  if (ObTimestampAccess::ServiceType::GTS_LEADER == type) {
    MTL(ObTimestampAccess *)->set_service_type(ObTimestampAccess::ServiceType::FOLLOWER);
  }
  TRANS_LOG(INFO, "ObTimestampService switch to follower gracefully success", K(type), "service_type", MTL(ObTimestampAccess *)->get_service_type());
  return OB_SUCCESS;
}

void ObTimestampService::switch_to_follower_forcedly()
{
  int64_t type = MTL(ObTimestampAccess *)->get_service_type();
  if (ObTimestampAccess::ServiceType::GTS_LEADER == type) {
    MTL(ObTimestampAccess *)->set_service_type(ObTimestampAccess::ServiceType::FOLLOWER);
  }
  TRANS_LOG(INFO, "ObTimestampService switch to follower forcedly success", K(type), "service_type", MTL(ObTimestampAccess *)->get_service_type());
}

int ObTimestampService::resume_leader()
{
  MTL(ObTimestampAccess *)->set_service_type(ObTimestampAccess::ServiceType::GTS_LEADER);
  TRANS_LOG(INFO, "ObTimestampService resume leader success", "service_type", MTL(ObTimestampAccess *)->get_service_type());
  return OB_SUCCESS;
}

int ObTimestampService::switch_to_leader()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(check_and_fill_ls())) {
    TRANS_LOG(WARN, "ls set fail", K(ret));
  } else {
    SCN version;
    if (OB_FAIL(ls_->get_log_handler()->get_max_scn(version))) {
      TRANS_LOG(WARN, "get max ts fail", K(ret));
    } else {
      int64_t version_val = version.is_valid() ? version.get_val_for_gts() : -1;
      if (version_val >= ATOMIC_LOAD(&limited_id_)) {
        inc_update(&last_id_, version_val);
        ATOMIC_STORE(&tmp_last_id_, 0);
      } else if (ATOMIC_LOAD(&tmp_last_id_) != 0 && version_val > ATOMIC_LOAD(&tmp_last_id_)) {
        inc_update(&tmp_last_id_, version_val);
      } else {
        // do nothing
      }
      const int64_t standby_last_id = MTL(ObStandbyTimestampService *)->get_last_id();
      const int64_t tmp_last_id = ATOMIC_LOAD(&tmp_last_id_);
      if ((tmp_last_id != 0 && standby_last_id > tmp_last_id)
           || (tmp_last_id == 0 && standby_last_id > ATOMIC_LOAD(&last_id_))) {
        TRANS_LOG(ERROR, "snapshot rolls back", K(standby_last_id), K(tmp_last_id), "limit_id", ATOMIC_LOAD(&limited_id_),
                         "last_id", ATOMIC_LOAD(&last_id_), K(version));
      }
      MTL(ObTimestampAccess *)->set_service_type(ObTimestampAccess::ServiceType::GTS_LEADER);
      TRANS_LOG(INFO, "ObTimestampService switch to leader success", K(ret), K(version), K(last_id_), K(limited_id_),
                      "service_type", MTL(ObTimestampAccess *)->get_service_type());
    }
  }

  return ret;
}

void ObTimestampService::get_virtual_info(int64_t &ts_value, common::ObRole &role, int64_t &proposal_id)
{
  int ret = OB_SUCCESS;
  ts_value = last_id_;
  if (OB_FAIL(check_and_fill_ls())) {
    TRANS_LOG(WARN, "ls set fail", K(ret));
  } else if (OB_FAIL(ls_->get_log_handler()->get_role(role, proposal_id))) {
    TRANS_LOG(WARN, "get ls role fail", K(ret));
  }
  TRANS_LOG(INFO, "gts get virtual info", K(ret), K_(last_id), K(ts_value), K(role), K(proposal_id));
}

}
}
