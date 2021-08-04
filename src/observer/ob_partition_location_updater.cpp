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

#include "common/ob_partition_key.h"
#include "common/ob_role.h"
#include "lib/thread_local/ob_tsi_factory.h"
#include "rootserver/ob_rs_async_rpc_proxy.h"
#include "share/config/ob_server_config.h"
#include "share/partition_table/ob_partition_location_cache.h"
#include "share/ob_alive_server_tracer.h"
#include "storage/ob_partition_service.h"
#include "observer/ob_service.h"
#include "observer/ob_partition_location_updater.h"

namespace oceanbase {
namespace observer {

using namespace common;
using namespace share;

static const char* location_queue_type[] = {"LOCATION_SENDER", "LOCATION_RECEIVER", "LOCATION_INVALID"};

int ObPartitionLocationUpdater::init(observer::ObService& ob_service, storage::ObPartitionService*& partition_service,
    obrpc::ObSrvRpcProxy*& srv_rpc_proxy, share::ObPartitionLocationCache*& location_cache,
    share::ObIAliveServerTracer& server_tracer)
{
  int ret = OB_SUCCESS;
  static_assert(
      QueueType::MAX_TYPE == ARRAYSIZEOF(location_queue_type) - 1, "type str array size mismatch with type cnt");
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("inited twice", KR(ret));
  } else if (OB_ISNULL(partition_service) || OB_ISNULL(srv_rpc_proxy) || OB_ISNULL(location_cache)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ptr is null", KR(ret), KP(partition_service), KP(srv_rpc_proxy), KP(location_cache));
  } else {
    queue_size_ = !lib::is_mini_mode() ? MAX_PARTITION_CNT : MINI_MODE_MAX_PARTITION_CNT;
    thread_cnt_ = !lib::is_mini_mode() ? GCONF.location_refresh_thread_count : UPDATER_THREAD_CNT;
    if (OB_FAIL(sender_.init(this, thread_cnt_, queue_size_, "PTSender"))) {
      LOG_WARN("init sender updater queue failed", KR(ret), K_(queue_size), K_(thread_cnt));
    } else if (OB_FAIL(receiver_.init(this, thread_cnt_, queue_size_, "PTReceiver"))) {
      LOG_WARN("init receiver updater queue failed", KR(ret), K_(queue_size), K_(thread_cnt));
    } else {
      ob_service_ = &ob_service;
      partition_service_ = partition_service;
      srv_rpc_proxy_ = srv_rpc_proxy;
      location_cache_ = location_cache;
      server_tracer_ = &server_tracer;
      inited_ = true;
    }
  }
  return ret;
}

int ObPartitionLocationUpdater::check_inner_stat() const
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

void ObPartitionLocationUpdater::stop()
{
  stopped_ = true;
  sender_.stop();
  receiver_.stop();
}

void ObPartitionLocationUpdater::wait()
{
  if (stopped_) {
    sender_.wait();
    receiver_.wait();
  }
}

void ObPartitionLocationUpdater::destroy()
{
  stop();
  wait();
}

int ObPartitionLocationUpdater::submit_broadcast_task(const ObPartitionBroadcastTask& task)
{
  int ret = OB_SUCCESS;
  if (!GCONF.enable_auto_refresh_location_cache) {
    // skip
  } else if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(sender_.add(task))) {
    if (OB_EAGAIN == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to add task", KR(ret), K(task));
    }
  }
  return ret;
}

int ObPartitionLocationUpdater::submit_update_task(const ObPartitionUpdateTask& task)
{
  int ret = OB_SUCCESS;
  if (!GCONF.enable_auto_refresh_location_cache) {
    // skip
  } else if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(receiver_.add(task))) {
    if (OB_EAGAIN == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to add task", KR(ret), K(task));
    }
  }
  return ret;
}

int ObPartitionLocationUpdater::process_barrier(const ObPartitionBroadcastTask& task, bool& stopped)
{
  UNUSED(task);
  UNUSED(stopped);
  return OB_NOT_SUPPORTED;
}

int ObPartitionLocationUpdater::process_barrier(const ObPartitionUpdateTask& task, bool& stopped)
{
  UNUSED(task);
  UNUSED(stopped);
  return OB_NOT_SUPPORTED;
}

int ObPartitionLocationUpdater::batch_process_tasks(
    const ObIArray<ObPartitionBroadcastTask>& batch_tasks, bool& stopped)
{
  int ret = OB_SUCCESS;
  ObCurTraceId::init(GCONF.self_addr_);
  obrpc::ObPartitionBroadcastArg arg;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (stopped) {
    ret = OB_CANCELED;
    LOG_WARN("updater is is stopped", KR(ret), K(stopped));
  } else if (!GCONF.enable_auto_refresh_location_cache) {
    // skip
  } else if (OB_UNLIKELY(batch_tasks.count() <= 0)) {
    // skip
  } else {
    int64_t start_ts = ObTimeUtility::current_time();
    for (int64_t i = 0; OB_SUCC(ret) && i < batch_tasks.count(); i++) {
      const ObPartitionBroadcastTask& task = batch_tasks.at(i);
      ObPartitionKey pkey;
      ObRole role = FOLLOWER;
      if (OB_FAIL(pkey.init(task.get_table_id(), task.get_partition_id(), task.get_partition_cnt()))) {
        LOG_WARN("init pkey failed", KR(ret), K(task));
      } else if (OB_FAIL(partition_service_->get_role(pkey, role))) {
        if (OB_ENTRY_NOT_EXIST == ret || OB_PARTITION_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to get role", KR(ret), K(pkey));
        }
      } else if (is_strong_leader(role)) {
        if (OB_FAIL(arg.keys_.push_back(task))) {
          LOG_WARN("fail to push back task", KR(ret), K(task));
        }
      }
      LOG_DEBUG("broadcast task is", KR(ret), K(task), K(pkey), K(role));
    }  // end for

    if (OB_SUCC(ret) && arg.keys_.count() > 0) {
      rootserver::ObBroadcastLocationProxy proxy(*srv_rpc_proxy_, &obrpc::ObSrvRpcProxy::broadcast_locations);
      ObArray<ObAddr> alive_servers;
      const int64_t timeout = GCONF.location_cache_refresh_rpc_timeout;
      if (OB_FAIL(server_tracer_->get_active_server_list(alive_servers))) {
        LOG_WARN("fail to get alive server list", KR(ret));
      }

      for (int64_t i = 0; OB_SUCC(ret) && i < alive_servers.count(); i++) {
        ObAddr& addr = alive_servers.at(i);
        if (OB_FAIL(proxy.call(addr, timeout, arg))) {
          LOG_WARN("fail to call addr", KR(ret), K(addr), K(timeout), K(arg));
        }
      }  // end for

      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = proxy.wait())) {
        LOG_WARN("fail to wait rpc callback", KR(tmp_ret));
        ret = OB_SUCC(ret) ? tmp_ret : ret;
      }

      LOG_DEBUG("try broadcast location", K(arg));

      int64_t exec_ts = ObTimeUtility::current_time() - start_ts;
      int64_t wait_ts = 0;
      (void)control_rate_limit(
          QueueType::SENDER, exec_ts, arg.keys_.count(), GCONF.auto_broadcast_location_cache_rate_limit, wait_ts);
      (void)dump_statistic(QueueType::SENDER, ret, exec_ts, wait_ts, arg.keys_.count());
    }
  }
  return ret;
}

int ObPartitionLocationUpdater::batch_process_tasks(const ObIArray<ObPartitionUpdateTask>& batch_tasks, bool& stopped)
{
  int ret = OB_SUCCESS;
  ObCurTraceId::init(GCONF.self_addr_);
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (stopped) {
    ret = OB_CANCELED;
    LOG_WARN("updater is is stopped", KR(ret), K(stopped));
  } else if (!GCONF.enable_auto_refresh_location_cache) {
    // skip
  } else if (OB_UNLIKELY(batch_tasks.count() <= 0)) {
    // skip
  } else {
    int64_t start_ts = ObTimeUtility::current_time();
    int64_t renew_cnt = 0;
    LOG_DEBUG("try renew location", K(batch_tasks));
    for (int64_t i = 0; OB_SUCC(ret) && i < batch_tasks.count(); i++) {
      const ObPartitionUpdateTask& task = batch_tasks.at(i);
      ObPartitionKey pkey;
      ObPartitionLocation location;
      if (OB_FAIL(pkey.init(task.get_table_id(), task.get_partition_id(), 0 /*partition_cnt, no used here*/))) {
        LOG_WARN("init pkey failed", KR(ret), K(task));
      } else if (OB_FAIL(location_cache_->nonblock_get(pkey, location))) {
        if (OB_LOCATION_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to get location", KR(ret), K(pkey));
        }
      } else if (task.get_timestamp() > location.get_renew_time()) {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = location_cache_->nonblock_renew(pkey, 0 /*expire_renew_time*/))) {
          LOG_WARN("nonblock renew failed", KR(tmp_ret), K(pkey));
        } else {
          renew_cnt++;
          LOG_DEBUG("try renew location", K(pkey), K(task), K(location));
        }
      }
    }
    int64_t exec_ts = ObTimeUtility::current_time() - start_ts;
    int64_t wait_ts = 0;
    (void)control_rate_limit(
        QueueType::RECEIVER, exec_ts, renew_cnt, GCONF.auto_refresh_location_cache_rate_limit, wait_ts);
    (void)dump_statistic(QueueType::RECEIVER, ret, exec_ts, wait_ts, renew_cnt);
  }
  return ret;
}

void ObPartitionLocationUpdater::dump_statistic(
    const QueueType queue_type, const int exec_ret, const int64_t exec_ts, const int64_t wait_ts, const int64_t cnt)
{
  TSILocationStatistics* statistics = GET_TSI(TSILocationStatistics);
  if (OB_ISNULL(statistics)) {
    LOG_WARN("fail to get statistic", "ret", OB_ERR_UNEXPECTED);
  } else {
    // calc statistic
    (void)statistics->calc(exec_ret, exec_ts, wait_ts, cnt);
    int64_t total_cnt = statistics->get_total_cnt();
    if (TC_REACH_TIME_INTERVAL(CHECK_INTERVAL_US) && total_cnt > 0) {
      QueueType type =
          (QueueType::SENDER <= queue_type && queue_type <= QueueType::RECEIVER) ? queue_type : QueueType::MAX_TYPE;
      ObTaskController::get().allow_next_syslog();
      LOG_INFO("[LOCATION_STATISTIC] auto refresh location statistics",
          "queue_type",
          location_queue_type[type],
          KPC(statistics),
          "avg_exec_us",
          statistics->total_exec_us_ / total_cnt,
          "avg_wait_us",
          statistics->total_wait_us_ / total_cnt);
      (void)statistics->reset();
    }
  }
}

void ObPartitionLocationUpdater::control_rate_limit(
    const QueueType queue_type, const int64_t exec_ts, const int64_t cnt, int64_t rate_limit_conf, int64_t& wait_ts)
{
  wait_ts = 0;
  TSILocationRateLimit* info = GET_TSI(TSILocationRateLimit);
  if (OB_ISNULL(info)) {
    LOG_WARN("fail to get info", "ret", OB_ERR_UNEXPECTED);
  } else {
    int64_t rate_limit = max(rate_limit_conf / thread_cnt_, 1);
    wait_ts = info->calc_wait_ts(cnt, exec_ts, rate_limit);
    if (wait_ts > 0) {
      QueueType type =
          (QueueType::SENDER <= queue_type && queue_type <= QueueType::RECEIVER) ? queue_type : QueueType::MAX_TYPE;
      ObTaskController::get().allow_next_syslog();
      LOG_INFO("[LOCATION_STATISTIC] rate limit",
          "queue_type",
          location_queue_type[type],
          KPC(info),
          K(rate_limit),
          K(wait_ts));
      usleep(wait_ts);
    }
  }
}

}  // end namespace observer
}  // end namespace oceanbase
