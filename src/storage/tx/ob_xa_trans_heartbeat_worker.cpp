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

#include "ob_xa_trans_heartbeat_worker.h"
#include "share/ob_thread_pool.h"
#include "storage/tx/ob_xa_service.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
using namespace common;
using namespace storage;

namespace transaction
{

int ObXATransHeartbeatWorker::init(ObXAService *xa_service)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
  } else if (NULL == xa_service) {
    TRANS_LOG(WARN, "xa_service is null", KP(xa_service));
    ret = OB_INVALID_ARGUMENT;
  } else {
    xa_service_ = xa_service;
    is_inited_ = true;
  }
  return ret;
}

int ObXATransHeartbeatWorker::start()
{
  int ret = OB_SUCCESS;
  share::ObThreadPool::set_run_wrapper(MTL_CTX());
  if (OB_UNLIKELY(!is_inited_)) {
    TRANS_LOG(WARN, "xa trans heartbeat not init");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(share::ObThreadPool::start())) {
    TRANS_LOG(ERROR, "XA trans heartbeat worker thread start error", K(ret));
  } else {
    TRANS_LOG(INFO, "XA trans heartbeat worker thread start");
  }
  return ret;
}

void ObXATransHeartbeatWorker::run1()
{
  int ret = OB_SUCCESS;
  static const int64_t INTERVAL_US = 5 * 1000 * 1000; // 5s
  int64_t loop_count = 0;
  int64_t total_time = 0;

  lib::set_thread_name("ObXAHbWorker");
  while (!has_set_stop()) {
    int64_t start_time = ObTimeUtility::current_time();
    loop_count++;
    // MTL(ObXAService *)->get_xa_statistics().print_statistics(start_time);
    MTL(ObXAService *)->try_print_statistics();

    if (OB_UNLIKELY(!is_inited_)) {
      TRANS_LOG(WARN, "xa trans heartbeat not init");
      ret = OB_NOT_INIT;
    } else {
      if (OB_FAIL(xa_service_->xa_scheduler_hb_req())) {
        TRANS_LOG(WARN, "xa scheduler heartbeat failed", K(ret));
      }
    }

    int64_t time_used = ObTimeUtility::current_time() - start_time;
    total_time += time_used;

    if (REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
      TRANS_LOG(INFO, "XA scheduler heartbeat task statistics",
          "avg_time", total_time / loop_count);
      total_time = 0;
      loop_count = 0;
    }

    if (time_used < INTERVAL_US) {
      ob_usleep((uint32_t)(INTERVAL_US - time_used));
    }
  }
}

void ObXATransHeartbeatWorker::stop()
{
  TRANS_LOG(INFO, "XA trans heartbeat worker thread stop");
  share::ObThreadPool::stop();
}

void ObXATransHeartbeatWorker::wait()
{
  TRANS_LOG(INFO, "XA trans heartbeat worker thread wait");
  share::ObThreadPool::wait();
}

void ObXATransHeartbeatWorker::destroy()
{
  if (is_inited_) {
    stop();
    wait();
    is_inited_ = false;
  }
}

} // transaction
} // oceanbase
