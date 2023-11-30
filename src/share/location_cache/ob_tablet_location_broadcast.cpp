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

#define USING_LOG_PREFIX SERVER

#include "ob_tablet_location_broadcast.h"

#include "lib/oblog/ob_log.h"                              // LOG_*
#include "share/ob_all_server_tracer.h"                    // SVR_TRACER
#include "share/location_cache/ob_tablet_ls_service.h"     // ObTabletLSService
#include "share/ob_cluster_version.h"                      // GET_MIN_CLUSTER_VERSION
#include "observer/omt/ob_multi_tenant.h"                  // omt

namespace oceanbase
{
using namespace common;
using namespace observer;
using namespace obrpc;
namespace share
{
void TabletLocationStatistics::reset_()
{
  task_cnt_ = 0;
  tablet_suc_cnt_ = 0;
  tablet_fail_cnt_ = 0;
  total_exec_us_ = 0;
  total_wait_us_ = 0;
}

int64_t TabletLocationStatistics::get_total_cnt_() const
{
  return tablet_suc_cnt_ + tablet_fail_cnt_;
}

void TabletLocationStatistics::calc_(
    const int ret,
    const int64_t exec_us,
    const int64_t wait_us,
    const int64_t tablet_cnt)
{
  total_exec_us_ += static_cast<uint64_t>(exec_us);
  total_wait_us_ += static_cast<uint64_t>(wait_us);
  task_cnt_++;
  if (OB_SUCCESS == ret) {
    tablet_suc_cnt_ += tablet_cnt;
  } else {
    tablet_fail_cnt_ += tablet_cnt;
  }
}

void TabletLocationStatistics::dump_statistics(
    const int exec_ret,
    const int64_t exec_ts,
    const int64_t wait_ts,
    const int64_t tablet_cnt,
    const int64_t rate_limit)
{
  ObSpinLockGuard guard(lock_);
  // calc statistic
  if (tablet_cnt > 0) {
    (void) calc_(exec_ret, max(exec_ts, 0), max(wait_ts, 0), tablet_cnt);
  }
  // print statistics if reach interval
  int64_t tablet_total_cnt = get_total_cnt_();
  if (TC_REACH_TIME_INTERVAL(CHECK_INTERVAL_US) && tablet_total_cnt > 0) {
    FLOG_INFO("[LOCATION_STATISTIC] tablet location statistics",
              K_(task_cnt), K(tablet_total_cnt), K_(tablet_suc_cnt), K_(tablet_fail_cnt),
              K_(total_exec_us), K_(total_wait_us),
              "avg_rate", total_exec_us_ <= 0 ? -1 : (tablet_total_cnt * ONE_SECOND_US / total_exec_us_),
              "rate_limit", rate_limit < 0 ? "no_limit" : to_cstring(rate_limit),
              "avg_exec_us", total_exec_us_ / tablet_total_cnt,
              "avg_wait_us", total_wait_us_ / tablet_total_cnt);
    (void) reset_();
  }
}

void TabletLocationRateLimit::reset_()
{
  tablet_cnt_ = 0;
  start_ts_ = OB_INVALID_TIMESTAMP;
}

int64_t TabletLocationRateLimit::calc_wait_ts_(
     const int64_t tablet_cnt,
     const int64_t exec_ts,
     const int64_t frequency)
{
  int64_t wait_ts = 0;
  int64_t current_ts = ObTimeUtility::current_time();
  // init or >= 1s, reset tablet_cnt and start_ts_
  if (current_ts - start_ts_ >= ONE_SECOND_US) {
    tablet_cnt_ = tablet_cnt;
    start_ts_ = current_ts - exec_ts;
  } else {
    tablet_cnt_ += tablet_cnt;
  }
  // calc wait_ts if tablet_cnt_ exceed frequency
  if (frequency > 0 && tablet_cnt_ > frequency) {
    wait_ts = tablet_cnt_ / (double) frequency * ONE_SECOND_US - (current_ts - start_ts_);
  }
  return wait_ts > 0 ? wait_ts : 0;
}

void TabletLocationRateLimit::control_rate_limit(
     const int64_t tablet_cnt,
     const int64_t exec_ts,
     int64_t rate_limit_conf,
     int64_t &wait_ts)
{
  ObSpinLockGuard guard(lock_);
  if (rate_limit_conf <= 0 || tablet_cnt <= 0) {
    // invalid argument, no need to calc
    wait_ts = 0;
  } else {
    wait_ts = calc_wait_ts_(tablet_cnt, max(exec_ts, 0), rate_limit_conf);
    if (wait_ts > 0) {
      FLOG_INFO("[LOCATION_STATISTIC] rate limit",
                K_(tablet_cnt), K_(start_ts), K(exec_ts),
                K(rate_limit_conf), K(wait_ts));
      ob_usleep(static_cast<useconds_t>(wait_ts));
    }
  }
}

int ObTabletLocationSender::init(ObSrvRpcProxy *srv_rpc_proxy)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tablet_location_sender inited twice", KR(ret));
  } else if (OB_ISNULL(srv_rpc_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("srv_rpc_proxy is nullptr", KR(ret), KP(srv_rpc_proxy));
  } else if (OB_FAIL(send_queue_.init(this,
                                      THREAD_CNT,
                                      TASK_QUEUE_SIZE,
                                      "TabletLocSender"))) {
    LOG_WARN("fail to init send_queue", KR(ret));
  } else {
    srv_rpc_proxy_ = srv_rpc_proxy;
    inited_ = true;
    stopped_ = false;
  }
  return ret;
}

int ObTabletLocationSender::check_inner_stat() const
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("updater is not inited yet", KR(ret));
  } else if (stopped_) {
    ret = OB_CANCELED;
    LOG_WARN("updater is stopped now", KR(ret));
  }
  return ret;
}

int ObTabletLocationSender::process_barrier(const ObTabletLocationBroadcastTask &task, bool &stopped)
{
  UNUSEDx(task, stopped);
  return OB_NOT_SUPPORTED;
}

void ObTabletLocationSender::stop() {
  stopped_ = true;
  send_queue_.stop();
}

void ObTabletLocationSender::wait()
{
  if (stopped_) {
    send_queue_.wait();
  }
}

void ObTabletLocationSender::destroy()
{
  stop();
  wait();
  inited_ = false;
  stopped_ = true;
  srv_rpc_proxy_ = nullptr;
}

int ObTabletLocationSender::submit_broadcast_task(const ObTabletLocationBroadcastTask &task)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check_inner_stat", KR(ret), K(task));
  } else if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_2_1_2) {
    // skip
  } else if (0 == get_rate_limit()) {
    // skip
  } else if (OB_UNLIKELY(!task.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task_info", KR(ret), K(task));
  } else if (OB_FAIL(send_queue_.add(task))) {
    LOG_WARN("fail to add task to send_queue_", KR(ret), K(task));
  }
  return ret;
}

int ObTabletLocationSender::batch_process_tasks(
    const common::ObIArray<ObTabletLocationBroadcastTask> &tasks,
    bool &stopped)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObArray<ObAddr> alive_servers;
  int64_t rate_limit_conf = 0;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check_inner_stat", KR(ret), K(tasks));
  } else if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_2_1_2) {
    // skip
  } else if (0 == (rate_limit_conf = get_rate_limit())) {
    // skip
  } else if (OB_UNLIKELY(1 != tasks.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tasks count is not 1", KR(ret), K(tasks));
  } else if (OB_ISNULL(srv_rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("srv_rpc_proxy_ is nullptr", KR(ret));
  } else if (OB_FAIL(SVR_TRACER.get_alive_servers(ObZone(), alive_servers))) {
    LOG_WARN("fail to get_alive_servers", KR(ret));
  } else {
    ObTabletLocationSendProxy rpc_proxy(*srv_rpc_proxy_,
                                        &ObSrvRpcProxy::tablet_location_send);
    // count of tasks is expected to be 1
    int64_t tablet_cnt = tasks.at(0).get_tablet_cnt();
    ObTabletLocationSendArg rpc_arg;
    int64_t timeout = GCONF.location_cache_refresh_rpc_timeout;  // 500ms by default
    int64_t start_ts = ObTimeUtility::current_time();
    if (OB_FAIL(rpc_arg.set(tasks))) {
      LOG_WARN("fail to set tablet_location_send rpc_arg", KR(ret));
    } else {
      FOREACH_CNT(addr, alive_servers) {
        if (OB_TMP_FAIL(rpc_proxy.call(*addr, timeout, rpc_arg))) {
          ret = OB_SUCC(ret) ? tmp_ret : ret;
          LOG_WARN("fail to send rpc", KR(tmp_ret), KR(ret), KPC(addr), K(rpc_arg));
        }
      }
      ObArray<int> rc_array;
      if (OB_TMP_FAIL(rpc_proxy.wait_all(rc_array))) {
        ret = OB_SUCC(ret) ? tmp_ret : ret;
        LOG_WARN("fail to wait all results", KR(ret), KR(tmp_ret));
      }
    }
    // flow control and dump statistics
    int64_t exec_ts = ObTimeUtility::current_time() - start_ts;
    int64_t wait_ts = 0;
    if (OB_SUCC(ret)) {
      (void) rate_limit_.control_rate_limit(tablet_cnt, exec_ts,
                                            rate_limit_conf, wait_ts);
    }
    (void) statistics_.dump_statistics(ret, exec_ts, wait_ts, tablet_cnt, rate_limit_conf);
  }
  return ret;
}

int ObTabletLocationUpdater::init(ObTabletLSService *tablet_ls_service)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tablet_location_sender inited twice", KR(ret));
  } else if (OB_ISNULL(tablet_ls_service)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet_ls_service is nullptr", KR(ret), KP(tablet_ls_service));
  } else if (OB_FAIL(update_queue_.init(this,
                                        THREAD_CNT,
                                        TASK_QUEUE_SIZE,
                                        "TabletLocUpdater"))) {
    LOG_WARN("fail to init update_queue_", KR(ret));
  } else {
    tablet_ls_service_ = tablet_ls_service;
    inited_ = true;
    stopped_ = false;
  }
  return ret;
}

int ObTabletLocationUpdater::check_inner_stat() const
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("updater is not inited yet", KR(ret));
  } else if (stopped_) {
    ret = OB_CANCELED;
    LOG_WARN("updater is stopped now", KR(ret));
  }
  return ret;
}

int ObTabletLocationUpdater::process_barrier(const ObTabletLocationBroadcastTask &task, bool &stopped)
{
  UNUSEDx(task, stopped);
  return OB_NOT_SUPPORTED;
}

void ObTabletLocationUpdater::stop() {
  stopped_ = true;
  update_queue_.stop();
}

void ObTabletLocationUpdater::wait()
{
  if (stopped_) {
    update_queue_.wait();
  }
}

void ObTabletLocationUpdater::destroy()
{
  stop();
  wait();
  inited_ = false;
  stopped_ = true;
  tablet_ls_service_ = nullptr;
}

int ObTabletLocationUpdater::submit_update_task(const ObTabletLocationBroadcastTask &received_task)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check_inner_stat", KR(ret), K(received_task));
  } else if (OB_UNLIKELY(!received_task.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid received_task", KR(ret), K(received_task));
  } else if (OB_FAIL(update_queue_.add(received_task))) {
    LOG_WARN("fail to add task to update_queue_", KR(ret), K(received_task));
  }
  return ret;
}


int ObTabletLocationUpdater::batch_process_tasks(
    const ObIArray<ObTabletLocationBroadcastTask> &tasks,
    bool &stopped)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check_inner_stat", KR(ret), K(tasks));
  } else if (OB_UNLIKELY(1 != tasks.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tasks count is not 1", KR(ret), K(tasks));
  } else if (OB_ISNULL(tablet_ls_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet_ls_service_ is nullptr", KR(ret));
  } else {
    // tasks count is expected to be 1
    const ObTabletLocationBroadcastTask &task = tasks.at(0);
    int64_t start_ts = ObTimeUtility::current_time();
    ObTabletLSCache tablet_ls_cache;
    const bool update_only = true;
    FOREACH_CNT_X(tablet_info, task.get_tablet_list(), OB_SUCC(ret)) {
      tablet_ls_cache.reset();
      int64_t now = ObTimeUtility::current_time();
      if (OB_UNLIKELY(!tablet_info->is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid tablet_info", KR(ret), K(task), KPC(tablet_info));
      } else if (OB_FAIL(tablet_ls_cache.init(task.get_tenant_id(),
                                              tablet_info->tablet_id(),
                                              task.get_ls_id(),
                                              now,
                                              tablet_info->transfer_seq()))) {
        LOG_WARN("fail to init tablet_ls_cache", KR(ret), K(task), KPC(tablet_info));
      } else if (OB_FAIL(tablet_ls_service_->update_cache(tablet_ls_cache, update_only))) {
        LOG_WARN("fail to update_cache", KR(ret), K(tablet_ls_cache), K(update_only));
      }
    }
    int64_t exec_ts = ObTimeUtility::current_time() - start_ts;
    int64_t wait_ts = 0;
    (void) statistics_.dump_statistics(ret, exec_ts, wait_ts, task.get_tablet_cnt(), -1/*rate_limit*/);
  }
  return ret;
}

} // end namespace share
} // end namespace oceanbase
