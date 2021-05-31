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

#define USING_LOG_PREFIX RPC_FRAME

#include "rpc/frame/ob_req_processor.h"

#include "lib/time/ob_time_utility.h"
#include "rpc/ob_request.h"
#include "lib/oblog/ob_trace_log.h"
#include "lib/rc/context.h"
#include "rpc/obmysql/ob_mysql_request_utils.h"
using namespace oceanbase::common;
using namespace oceanbase::rpc::frame;

int ObReqProcessor::run()
{
  int ret = OB_SUCCESS;
  bool deseri_succ = true;
  process_ret_ = OB_SUCCESS;
  before_process_ret_ = OB_SUCCESS;

  run_timestamp_ = ObTimeUtility::current_time();
  if (OB_FAIL(check_timeout())) {
    LOG_WARN("req timeout", K(ret));
  } else if (OB_FAIL(deserialize())) {
    before_process_ret_ = ret;
    deseri_succ = false;
    LOG_WARN("deserialize argument fail", K(ret));
  } else if (OB_FAIL(before_process())) {
    before_process_ret_ = ret;
    LOG_WARN("before process fail", K(ret));
  } else {
    // Only handle RPC protocol packets
    if (conn_valid_ && !req_has_wokenup_ && NULL != req_ && ObRequest::OB_RPC == req_->get_type() &&
        NULL != req_->ez_req_) {
      req_->set_pop_process_start_diff(run_timestamp_);
    }
    if (OB_FAIL(process())) {
      process_ret_ = ret;
      LOG_DEBUG("process fail", K(ret));
    } else {
    }
    // Only handle RPC protocol packets
    if (conn_valid_ && !req_has_wokenup_ && NULL != req_ && ObRequest::OB_RPC == req_->get_type() &&
        NULL != req_->ez_req_) {
      req_->set_process_start_end_diff(ObTimeUtility::current_time());
    }
  }

  int ret_bk = ret;
  // Only handle RPC protocol packets
  if (conn_valid_ && !req_has_wokenup_ && NULL != req_ && ObRequest::OB_RPC == req_->get_type() &&
      NULL != req_->ez_req_) {
    req_->set_process_end_response_diff(ObTimeUtility::current_time());
  }
  if (OB_FAIL(before_response())) {
    LOG_WARN("before response result fail", K(ret));
  }
  ret = ret_bk;

  if (conn_valid_ && !req_has_wokenup_ && OB_FAIL(response(ret))) {
    LOG_WARN("response rpc result fail", K(ret));
  }
  if (deseri_succ && OB_FAIL(after_process())) {
    LOG_WARN("after process fail", K(ret));
  }

  cleanup();

  return ret;
}

int ObReqProcessor::after_process()
{
  int ret = OB_SUCCESS;
  // RPC requests exclude SQL query
  NG_TRACE_EXT(process_end, OB_ID(run_ts), get_run_timestamp());
  const int64_t elapsed_time = common::ObTimeUtility::current_time() - get_receive_timestamp();
  // @todo config flag for slow rpc
  bool is_slow = (elapsed_time > 300000);
  if (is_slow) {
    // deleted by : logging cost too much time
    // FORCE_PRINT_TRACE(THE_TRACE, "[slow rpc]");
  } else if (can_force_print(process_ret_)) {
    FORCE_PRINT_TRACE(THE_TRACE, "[err rpc]");
  }
  return ret;
}

int ObReqProcessor::before_response()
{
  return OB_SUCCESS;
}

char* ObReqProcessor::easy_alloc(int64_t size) const
{
  void* buf = NULL;
  if (req_has_wokenup_ || OB_ISNULL(req_)) {
    LOG_ERROR("request is invalid", KP(req_), K(req_has_wokenup_));
  } else if (OB_ISNULL(req_->get_request()) || OB_ISNULL(req_->get_request()->ms) ||
             OB_ISNULL(req_->get_request()->ms->pool)) {
    LOG_ERROR("request is invalid", K(req_));
  } else {
    if (conn_valid_) {
      buf = easy_pool_alloc(req_->get_request()->ms->pool, static_cast<uint32_t>(size));
    }
  }
  return static_cast<char*>(buf);
}

void ObReqProcessor::wakeup_request()
{
  if (!io_thread_mark_ && conn_valid_ && !req_has_wokenup_) {
    if (OB_ISNULL(req_) || OB_ISNULL(req_->get_request())) {
      LOG_ERROR("reqest is NULL or request has wokenup");
    } else {
      if (req_->get_request()->retcode != EASY_AGAIN) {
        req_has_wokenup_ = true;
        obmysql::request_finish_callback();
      }
      easy_request_wakeup(req_->get_request());
    }
  }
}
