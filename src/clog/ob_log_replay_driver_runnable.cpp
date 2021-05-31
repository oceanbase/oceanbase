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

#include "ob_log_replay_driver_runnable.h"
#include "lib/statistic_event/ob_stat_event.h"
#include "lib/stat/ob_diagnose_info.h"
#include "ob_partition_log_service.h"
#include "storage/ob_partition_service.h"

namespace oceanbase {
using namespace common;
namespace clog {
ObLogReplayDriverRunnable::ObLogReplayDriverRunnable()
    : partition_service_(NULL), can_start_service_(false), is_inited_(false)
{}

ObLogReplayDriverRunnable::~ObLogReplayDriverRunnable()
{
  destroy();
}

int ObLogReplayDriverRunnable::init(storage::ObPartitionService* partition_service)
{
  int ret = OB_SUCCESS;

  if (NULL == partition_service) {
    CLOG_LOG(WARN, "invalid argument");
    ret = OB_INVALID_ARGUMENT;
  } else if (is_inited_) {
    CLOG_LOG(WARN, "ObLogReplayDriverRunnable has already been inited");
    ret = OB_INIT_TWICE;
  } else {
    partition_service_ = partition_service;
  }
  if ((OB_SUCC(ret)) && OB_FAIL(start())) {
    CLOG_LOG(ERROR, "ObLogReplayDriverRunnable thread failed to start");
  }
  if ((OB_FAIL(ret)) && (OB_INIT_TWICE != ret)) {
    destroy();
  }
  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }
  CLOG_LOG(INFO, "ObLogReplayDriverRunnable init finished", K(ret));
  return ret;
}

void ObLogReplayDriverRunnable::destroy()
{
  stop();
  wait();
  partition_service_ = NULL;
  is_inited_ = false;
}

void ObLogReplayDriverRunnable::run1()
{
  (void)prctl(PR_SET_NAME, "LogReplayDriver", 0, 0, 0);
  try_replay_loop();
  CLOG_LOG(INFO, "ob_log_replay_driver_runnable will stop");
}

void ObLogReplayDriverRunnable::try_replay_loop()
{
  while (!has_set_stop()) {
    int ret = OB_SUCCESS;
    bool is_replayed = false;
    bool is_replay_failed = false;
    const int64_t start_time = ObTimeUtility::current_time();
    storage::ObIPartitionGroupIterator* partition_iter = NULL;

    if (!can_start_service_ && REACH_TIME_INTERVAL(1000 * 1000)) {
      int tmp_ret = OB_SUCCESS;
      bool can_start_service = false;
      int64_t unused_value = 0;
      ObPartitionKey unused_partition_key;
      if (OB_SUCCESS != (tmp_ret = partition_service_->check_can_start_service(
                             can_start_service, unused_value, unused_partition_key))) {
        CLOG_LOG(WARN, "partition_service_ check_can_start_service failed", K(tmp_ret));
      } else {
        can_start_service_ = can_start_service;
      }
    }

    if (NULL == (partition_iter = partition_service_->alloc_pg_iter())) {
      CLOG_LOG(WARN, "alloc_scan_iter failed");
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      storage::ObIPartitionGroup* partition = NULL;
      ObIPartitionLogService* pls = NULL;
      while ((!has_set_stop()) && OB_SUCC(ret)) {
        bool tmp_is_replayed = false;
        bool tmp_is_replay_failed = false;
        if (OB_SUCC(partition_iter->get_next(partition)) && NULL != partition) {
          if (partition->is_valid() && (NULL != (pls = partition->get_log_service()))) {
            int tmp_ret = OB_SUCCESS;
            const bool need_async_replay = !can_start_service_;
            if (OB_SUCCESS != (tmp_ret = pls->try_replay(need_async_replay, tmp_is_replayed, tmp_is_replay_failed)) &&
                OB_CLOG_SLIDE_TIMEOUT != tmp_ret) {
              CLOG_LOG(WARN, "try replay failed", K(tmp_ret));
            }
          } else {
            // ship
          }
        } else {
        }
        if (tmp_is_replayed) {
          is_replayed = tmp_is_replayed;
        }
        if (tmp_is_replay_failed) {
          is_replay_failed = tmp_is_replay_failed;
        }
      }
    }

    if (NULL != partition_iter) {
      partition_service_->revert_pg_iter(partition_iter);
      partition_iter = NULL;
    }
    const int64_t round_cost_time = ObTimeUtility::current_time() - start_time;
    EVENT_INC(CLOG_REPLAY_LOOP_COUNT);
    EVENT_ADD(CLOG_REPLAY_LOOP_TIME, round_cost_time);
    int32_t sleep_ts = 0;
    if (is_replayed || is_replay_failed) {
      sleep_ts = CLOG_REPLAY_DRIVER_LOWER_INTERVAL - static_cast<const int32_t>(round_cost_time);
    } else {
      sleep_ts = CLOG_REPLAY_DRIVER_UPPER_INTERVAL - static_cast<const int32_t>(round_cost_time);
    }
    if (sleep_ts < 0) {
      sleep_ts = 0;
    }
    usleep(sleep_ts);

    if (REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
      CLOG_LOG(INFO, "ObLogReplayDriverRunnable round_cost_time", K(round_cost_time), K(sleep_ts));
    }
  }
}
}  // namespace clog
}  // namespace oceanbase
