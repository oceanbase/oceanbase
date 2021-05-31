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

#define USING_LOG_PREFIX SHARE_PT

#include "ob_location_update_task.h"

#include "lib/profile/ob_trace_id.h"
#include "share/config/ob_server_config.h"
#include "share/ob_worker.h"
//#include "ob_partition_location_cache.h"
#include "ob_ipartition_table.h"
#include "observer/ob_server_struct.h"
namespace oceanbase {
using namespace common;
namespace share {
void TSILocationCacheStatistics::reset()
{
  type_ = ObLocationCacheQueueSet::LOC_QUEUE_MAX;
  suc_cnt_ = 0;
  fail_cnt_ = 0;
  sql_suc_cnt_ = 0;
  sql_fail_cnt_ = 0;
  total_wait_us_ = 0;
  total_exec_us_ = 0;
}

void TSILocationCacheStatistics::calc(ObLocationCacheQueueSet::Type type, bool sql_renew, int64_t succ_cnt,
    int64_t fail_cnt, int64_t wait_us, int64_t exec_us)
{
  type_ = type;
  total_wait_us_ += static_cast<uint64_t>(wait_us);
  total_exec_us_ += static_cast<uint64_t>(exec_us);
  if (sql_renew) {
    sql_suc_cnt_ += succ_cnt;
    sql_fail_cnt_ += fail_cnt;
  } else {
    suc_cnt_ += succ_cnt;
    fail_cnt_ += fail_cnt;
  }
}

void TSILocationCacheStatistics::calc(
    ObLocationCacheQueueSet::Type type, int ret, bool sql_renew, int64_t wait_us, int64_t exec_us)
{
  type_ = type;
  total_wait_us_ += static_cast<uint64_t>(wait_us);
  total_exec_us_ += static_cast<uint64_t>(exec_us);
  if (OB_SUCC(ret)) {
    if (sql_renew) {
      sql_suc_cnt_++;
    } else {
      suc_cnt_++;
    }
  } else {
    if (sql_renew) {
      sql_fail_cnt_++;
    } else {
      fail_cnt_++;
    }
  }
}

void TSILocationCacheStatistics::dump()
{
  int64_t total_cnt = suc_cnt_ + fail_cnt_ + sql_suc_cnt_ + sql_fail_cnt_;
  ObTaskController::get().allow_next_syslog();
  const char* queue_type_str = ObLocationCacheQueueSet::get_str_by_queue_type(type_);
  LOG_INFO("[LOCATION_STATISTIC] dump location cache statistics",
      "queue_type",
      queue_type_str,
      K_(suc_cnt),
      K_(fail_cnt),
      K_(sql_suc_cnt),
      K_(sql_fail_cnt),
      K(total_cnt),
      "avg_wait_us",
      total_wait_us_ / total_cnt,
      "avg_exec_us",
      total_exec_us_ / total_cnt);
}

ObLocationUpdateTask::ObLocationUpdateTask(ObPartitionLocationCache& loc_cache, const volatile bool& is_stopped,
    const uint64_t table_id, const int64_t partition_id, const int64_t add_timestamp, const int64_t cluster_id)
    : IObDedupTask(T_PL_UPDATE),
      loc_cache_(loc_cache),
      is_stopped_(is_stopped),
      table_id_(table_id),
      partition_id_(partition_id),
      add_timestamp_(add_timestamp),
      cluster_id_(cluster_id)
{
  force_sql_renew_ = false;
}

ObLocationUpdateTask::ObLocationUpdateTask(ObPartitionLocationCache& loc_cache, const volatile bool& is_stopped,
    const uint64_t table_id, const int64_t partition_id, const int64_t add_timestamp, const int64_t cluster_id,
    const bool force_sql_renew)
    : IObDedupTask(T_PL_UPDATE),
      loc_cache_(loc_cache),
      is_stopped_(is_stopped),
      table_id_(table_id),
      partition_id_(partition_id),
      add_timestamp_(add_timestamp),
      cluster_id_(cluster_id),
      force_sql_renew_(force_sql_renew)
{}

ObLocationUpdateTask::~ObLocationUpdateTask()
{}

bool ObLocationUpdateTask::is_valid() const
{
  return ObIPartitionTable::is_valid_key(table_id_, partition_id_);
}

int64_t ObLocationUpdateTask::hash() const
{
  return (table_id_ << 16) | ((partition_id_ * 29 + 7) & 0xFFFF);
}

bool ObLocationUpdateTask::operator==(const IObDedupTask& other) const
{
  bool is_equal = false;
  if (!is_valid()) {
    is_equal = false;
    LOG_WARN("invalid task", "self", *this);
  } else if (&other == this) {
    is_equal = true;
  } else {
    const ObLocationUpdateTask& o = static_cast<const ObLocationUpdateTask&>(other);
    if (!o.is_valid()) {
      is_equal = false;
      LOG_WARN("invalid task", "other", o);
    } else {
      is_equal = (o.table_id_ == table_id_ && o.partition_id_ == partition_id_ && o.cluster_id_ == cluster_id_ &&
                  o.force_sql_renew_ == force_sql_renew_);
    }
  }
  return is_equal;
}

IObDedupTask* ObLocationUpdateTask::deep_copy(char* buf, const int64_t buf_size) const
{
  ObLocationUpdateTask* task = NULL;
  if (NULL == buf || buf_size < get_deep_copy_size()) {
    LOG_WARN("invalid argument", KP(buf), K(buf_size), "need size", get_deep_copy_size());
  } else {
    task = new (buf) ObLocationUpdateTask(
        loc_cache_, is_stopped_, table_id_, partition_id_, add_timestamp_, cluster_id_, force_sql_renew_);
  }
  return task;
}

bool ObLocationUpdateTask::need_discard() const
{
  return false;
}

int ObLocationUpdateTask::process()
{
  int ret = OB_SUCCESS;
  ObCurTraceId::init(GCONF.self_addr_);
  THIS_WORKER.set_timeout_ts(INT64_MAX);
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid location update task", "task", (*this));
  } else if (need_discard()) {
    LOG_INFO("task no need to process any more", KPC(this));
    // nothing todo
  } else if (!is_stopped_) {
    const int64_t now = ObTimeUtility::current_time();
    const int64_t wait_cost = now - add_timestamp_;
    if (wait_cost >= WAIT_PROCESS_WARN_TIME) {
      LOG_WARN("location update task waits to be processed too long", "task", *this, K(now), K(wait_cost));
    }
    const int64_t expire_renew_time = now - GCONF.location_cache_refresh_min_interval;
    const bool auto_update = false;
    if (!is_virtual_table(table_id_)) {
      ObPartitionLocation location;
      if (OB_FAIL(loc_cache_.renew_location(table_id_,
              partition_id_,
              cluster_id_,
              location,
              expire_renew_time,
              true /*result_filter_not_readable_replica*/,
              force_sql_renew_,
              auto_update))) {
        LOG_WARN("renew location failed",
            K(ret),
            KT_(table_id),
            K_(partition_id),
            K(expire_renew_time),
            K_(cluster_id),
            K(wait_cost));
      } else {
        LOG_DEBUG("async renew location succeed",
            K(ret),
            KT_(table_id),
            K_(partition_id),
            K(location),
            K(expire_renew_time),
            K_(cluster_id),
            K(wait_cost));
      }
    } else {
      // renew virtual table location cache
      ObSArray<ObPartitionLocation> locations;
      bool is_cache_hit = false;
      if (OB_FAIL(loc_cache_.get(table_id_, locations, expire_renew_time, is_cache_hit, auto_update))) {
        LOG_WARN("renew location failed", K(ret), KT_(table_id), K(wait_cost));
      } else {
        LOG_DEBUG("async renew vtable location succeed",
            K(ret),
            KT_(table_id),
            K_(partition_id),
            K(locations),
            K(expire_renew_time),
            K_(cluster_id),
            K(wait_cost));
      }
    }
    const int64_t end = ObTimeUtility::current_time();

    auto* statistics = GET_TSI(TSILocationCacheStatistics);
    if (OB_ISNULL(statistics)) {
      LOG_WARN("fail to get statistic", "ret", OB_ERR_UNEXPECTED);
    } else {
      ObLocationCacheQueueSet::Type type = ObLocationCacheQueueSet::get_queue_type(table_id_, partition_id_);
      (void)statistics->calc(type, ret, force_sql_renew_, wait_cost, end - now);
      const int64_t interval = 1 * 1000 * 1000;  // 1s
      if (TC_REACH_TIME_INTERVAL(interval)) {
        (void)statistics->dump();
        (void)statistics->reset();
      }
    }
  }
  return ret;
}
}  // end namespace share
}  // end namespace oceanbase
