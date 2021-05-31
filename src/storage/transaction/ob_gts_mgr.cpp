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

#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "ob_gts_define.h"
#include "ob_gts_mgr.h"
#include "ob_trans_event.h"
#include "ob_gts_source.h"

namespace oceanbase {
using namespace common;
using namespace share;
using namespace obrpc;

namespace transaction {
int ObGlobalTimestampService::init(const ObITimestampService* ts_service, ObIGtsResponseRpc* rpc, const ObAddr& self)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    TRANS_LOG(WARN, "not init", KR(ret));
  } else if (NULL == ts_service || NULL == rpc || !self.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), KP(ts_service), KP(rpc), K(self));
  } else {
    ts_service_ = ts_service;
    rpc_ = rpc;
    self_ = self;
    is_inited_ = true;
    TRANS_LOG(INFO, "global timestamp service init success", KP(this), K(self), KP(ts_service), KP(rpc));
  }
  return ret;
}

int ObGlobalTimestampService::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (is_running_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "global timestamp service already running", KR(ret));
  } else {
    is_running_ = true;
    TRANS_LOG(INFO, "global timestamp service start success");
  }
  return ret;
}

int ObGlobalTimestampService::stop()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!is_running_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "global timestamp service not running");
  } else {
    is_running_ = false;
    TRANS_LOG(INFO, "global timestamp service stop success");
  }
  return ret;
}

int ObGlobalTimestampService::wait()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (is_running_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "global timestamp service is running", KR(ret));
  } else {
    TRANS_LOG(INFO, "global timestamp service wait success");
    // FIXME.
  }
  return ret;
}

void ObGlobalTimestampService::destroy()
{
  if (is_inited_) {
    if (is_running_) {
      (void)stop();
      (void)wait();
    }
    is_inited_ = false;
    TRANS_LOG(INFO, "global timestamp service destroyed");
  }
}

int ObGlobalTimestampService::get_gts(const ObPartitionKey& gts_pkey, ObAddr& leader, int64_t& gts)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", KR(ret));
  } else if (OB_UNLIKELY(!is_running_)) {
    ret = OB_NOT_RUNNING;
    TRANS_LOG(WARN, "not running", KR(ret));
  } else {
    if (OB_FAIL(ts_service_->get_timestamp(gts_pkey, gts, leader))) {
      if (EXECUTE_COUNT_PER_SEC(100)) {
        TRANS_LOG(WARN, "get timestamp failed", KR(ret), K(gts_pkey), K(gts), K(leader));
      }
    }
  }

  return ret;
}

int ObGlobalTimestampService::handle_request(const ObGtsRequest& request, ObGtsRpcResult& result)
{
  static int64_t total_cnt = 0;
  static int64_t total_rt = 0;
  static const int64_t STATISTICS_INTERVAL_US = 10000000;
  const MonotonicTs start = MonotonicTs::current_time();
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", KR(ret));
  } else if (OB_UNLIKELY(!is_running_)) {
    ret = OB_NOT_RUNNING;
    TRANS_LOG(WARN, "not running", KR(ret));
  } else if (OB_UNLIKELY(!request.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(request));
  } else {
    TRANS_LOG(DEBUG, "handle gts request", K(request));
    int64_t gts = 0;
    ObAddr leader;
    const MonotonicTs srr = request.get_srr();
    const uint64_t tenant_id = request.get_tenant_id();
    const ObPartitionKey& gts_pkey = request.get_gts_pkey();
    const ObAddr& requester = request.get_sender();
    if (requester == self_) {
      // Go local call to get gts
      TRANS_LOG(DEBUG, "handle local gts request", K(requester));
      ret = handle_local_request_(request, result);
    } else if (OB_FAIL(ts_service_->get_timestamp(gts_pkey, gts, leader))) {
      TRANS_LOG(WARN, "get timestamp failed", KR(ret), K(gts_pkey), K(leader));
      int tmp_ret = OB_SUCCESS;
      ObGtsErrResponse response;
      if (OB_SUCCESS != (tmp_ret = result.init(tenant_id, ret, srr, 0, 0))) {
        TRANS_LOG(WARN, "gts result init failed", K(tmp_ret), K(request));
      } else if (OB_SUCCESS != (tmp_ret = response.init(tenant_id, srr, ret, leader, self_))) {
        TRANS_LOG(WARN, "gts err response init failed", K(tmp_ret), K(request));
      } else if (OB_SUCCESS != (tmp_ret = rpc_->post(tenant_id, requester, response))) {
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
  // obtain gts information:
  //(1) How many gts requests are processed per second
  //(2) How long does it take to process a request
  const MonotonicTs end = MonotonicTs::current_time();
  const int64_t cost_us = request.get_srr().mts_ - end.mts_;
  // Print the gts request that takes a long time for network transmission
  if (cost_us > 500 * 1000) {
    TRANS_LOG(WARN, "gts request fly too much time", K(request), K(result), K(cost_us));
  }
  ATOMIC_INC(&total_cnt);
  ObTransStatistic::get_instance().add_gts_request_total_count(request.get_tenant_id(), 1);
  (void)ATOMIC_FAA(&total_rt, end.mts_ - start.mts_);
  if (REACH_TIME_INTERVAL(STATISTICS_INTERVAL_US)) {
    TRANS_LOG(INFO,
        "handle gts request statistics",
        K(total_rt),
        K(total_cnt),
        "avg_rt",
        (double)total_rt / (double)(total_cnt + 1),
        "avg_cnt",
        (double)total_cnt / (double)(STATISTICS_INTERVAL_US / 1000000));
    ATOMIC_STORE(&total_cnt, 0);
    ATOMIC_STORE(&total_rt, 0);
  }
  return ret;
}

int ObGlobalTimestampService::handle_local_request_(const ObGtsRequest& request, obrpc::ObGtsRpcResult& result)
{

  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", KR(ret));
  } else if (OB_UNLIKELY(!is_running_)) {
    ret = OB_NOT_RUNNING;
    TRANS_LOG(WARN, "not running", KR(ret));
  } else if (OB_UNLIKELY(!request.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(request));
  } else {
    TRANS_LOG(DEBUG, "handle gts request", K(request));
    int64_t gts = 0;
    ObAddr leader;
    const uint64_t tenant_id = request.get_tenant_id();
    const MonotonicTs srr = request.get_srr();
    const ObPartitionKey& gts_pkey = request.get_gts_pkey();
    const ObAddr& requester = request.get_sender();
    if (requester != self_) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "requester not equal to self in local gts request", KR(ret), K(requester), K(self_), K(request));
    } else if (OB_FAIL(ts_service_->get_timestamp(gts_pkey, gts, leader))) {
      TRANS_LOG(WARN, "get timestamp failed", KR(ret), K(gts_pkey), K(leader));
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = result.init(tenant_id, ret, srr, 0, 0))) {
        TRANS_LOG(WARN, "gts result init failed", K(tmp_ret), K(request));
      }
    } else {
      if (OB_FAIL(result.init(tenant_id, ret, srr, gts, gts))) {
        TRANS_LOG(WARN, "local gts result init failed", KR(ret), K(request));
      }
    }
  }
  return ret;
}

}  // namespace transaction
}  // namespace oceanbase
