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

#include "ob_log_state_driver_runnable.h"
#include "share/backup/ob_backup_info_mgr.h"
#include "lib/statistic_event/ob_stat_event.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/io/ob_io_manager.h"
#include "election/ob_election_mgr.h"
#include "ob_partition_log_service.h"
#include "storage/ob_partition_service.h"
#include "storage/transaction/ob_weak_read_util.h"  //ObWeakReadUtil
#include "lib/thread/ob_thread_name.h"

namespace oceanbase {
using namespace common;
using namespace share;

namespace clog {
ObLogStateDriverRunnable::ObLogStateDriverRunnable()
    : partition_service_(NULL),
      election_mgr_(NULL),
      already_disk_error_(false),
      is_inited_(false),
      can_start_service_(false),
      last_check_time_for_keepalive_(OB_INVALID_TIMESTAMP),
      last_check_time_for_replica_state_(OB_INVALID_TIMESTAMP),
      last_check_time_for_broadcast_(OB_INVALID_TIMESTAMP)
{}

ObLogStateDriverRunnable::~ObLogStateDriverRunnable()
{
  destroy();
}

int ObLogStateDriverRunnable::init(
    storage::ObPartitionService* partition_service, election::ObIElectionMgr* election_mgr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(partition_service) || OB_ISNULL(election_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), KP(partition_service), KP(election_mgr));
  } else if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(WARN, "ObLogStateDriverRunnable has already been inited", K(ret));
  } else {
    partition_service_ = partition_service;
    election_mgr_ = election_mgr;
  }
  if ((OB_SUCC(ret)) && OB_FAIL(start())) {
    CLOG_LOG(ERROR, "ObLogStateDriverRunnable thread failed to start");
  }
  if ((OB_FAIL(ret)) && (OB_INIT_TWICE != ret)) {
    destroy();
  }
  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }
  last_check_time_for_keepalive_ = OB_INVALID_TIMESTAMP;
  last_check_time_for_replica_state_ = OB_INVALID_TIMESTAMP;
  CLOG_LOG(INFO, "ObLogStateDriverRunnable init finished", K(ret));
  return ret;
}

void ObLogStateDriverRunnable::destroy()
{
  stop();
  wait();
  partition_service_ = NULL;
  election_mgr_ = NULL;
  is_inited_ = false;
}

void ObLogStateDriverRunnable::run1()
{
  lib::set_thread_name("LogStateDri");
  state_driver_loop();
  CLOG_LOG(INFO, "ob_log_state_driver_runnable will stop");
}

void ObLogStateDriverRunnable::state_driver_loop()
{
  while (!has_set_stop()) {
    int ret = OB_SUCCESS;
    int tmp_ret = OB_SUCCESS;
    const int64_t start_ts = ObTimeUtility::current_time();
    int64_t clog_keepalive_interval = transaction::ObWeakReadUtil::replica_keepalive_interval();

    bool update_flag_for_keepalive = false;
    bool update_flag_for_replica_state = false;
    bool update_flag_for_broadcast = false;

    bool is_replayed = false;
    bool is_replay_failed = false;
    storage::ObIPartitionGroupIterator* partition_iter = NULL;

    check_can_start_service_();

    const int64_t start_time = ObTimeUtility::current_time();
    if (NULL == (partition_iter = partition_service_->alloc_pg_iter())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      CLOG_LOG(ERROR, "alloc_scan_iter failed", K(ret));
    } else {
      storage::ObIPartitionGroup* partition = NULL;
      ObIPartitionLogService* pls = NULL;
      while (!has_set_stop() && OB_SUCC(ret)) {
        if (OB_FAIL(partition_iter->get_next(partition))) {
          // do nothing
        } else if (NULL == partition) {
          ret = OB_ERR_UNEXPECTED;
          CLOG_LOG(WARN, "Unexpected error, partition is NULL, ", K(ret));
        } else if (!partition->is_valid() || (NULL == (pls = partition->get_log_service()))) {
          // do nothing
        } else {
          tmp_ret = OB_SUCCESS;
          tmp_ret = pls->check_mc_and_sliding_window_state();
          if (OB_SUCCESS != tmp_ret && OB_EAGAIN != tmp_ret) {
            CLOG_LOG(WARN, "log_info switch_state failed", "partition_key", partition->get_partition_key(), K(tmp_ret));
          }
          // leader keepalive
          if ((start_ts - last_check_time_for_keepalive_) > clog_keepalive_interval) {
            (void)pls->leader_keepalive(clog_keepalive_interval);
            update_flag_for_keepalive = true;
          }
          // check cascading state
          if ((start_ts - last_check_time_for_replica_state_) > CLOG_CHECK_REPLICA_STATE_INTERVAL) {
            if (OB_SUCCESS != (tmp_ret = pls->check_cascading_state())) {
              CLOG_LOG(
                  WARN, "check_cascading_state failed", "partition_key", partition->get_partition_key(), K(tmp_ret));
            }
            update_flag_for_replica_state = true;
          }

          if (start_ts - last_check_time_for_broadcast_ > CLOG_BROADCAST_INTERVAL) {
            if (OB_SUCCESS != (tmp_ret = pls->broadcast_info())) {
              CLOG_LOG(WARN, "broadcast_info failed", "partition_key", partition->get_partition_key(), K(tmp_ret));
            }
            update_flag_for_broadcast = true;
          }

          // for replay driver runnable
          bool tmp_is_replayed = false;
          bool tmp_is_replay_failed = false;
          const bool need_async_replay = !can_start_service_;
          if (OB_SUCCESS != (tmp_ret = pls->try_replay(need_async_replay, tmp_is_replayed, tmp_is_replay_failed)) &&
              OB_CLOG_SLIDE_TIMEOUT != tmp_ret) {
            CLOG_LOG(WARN, "try replay failed", K(tmp_ret));
          }
          if (tmp_is_replayed) {
            is_replayed = tmp_is_replayed;
          }
          if (tmp_is_replay_failed) {
            is_replay_failed = tmp_is_replay_failed;
          }
        }
      }  // end of while
    }
    if (NULL != partition_iter) {
      partition_service_->revert_pg_iter(partition_iter);
      partition_iter = NULL;
    }
    EVENT_INC(CLOG_STATE_LOOP_COUNT);
    EVENT_ADD(CLOG_STATE_LOOP_TIME, ObTimeUtility::current_time() - start_time);
    bool disk_error = false;
    if (OB_SUCCESS != (tmp_ret = ObIOManager::get_instance().is_disk_error(disk_error))) {
      CLOG_LOG(WARN, "is_disk_error failed", K(tmp_ret));
    }
    if (!disk_error) {
      already_disk_error_ = false;
    }
    if (!already_disk_error_ && disk_error) {
      already_disk_error_ = true;
      const uint32_t revoke_type = election::ObElection::RevokeType::DISK_ERROR;
      election_mgr_->revoke_all(revoke_type);
    }
    if (update_flag_for_keepalive) {
      last_check_time_for_keepalive_ = start_ts;
    }
    if (update_flag_for_replica_state) {
      last_check_time_for_replica_state_ = start_ts;
    }
    if (update_flag_for_broadcast) {
      last_check_time_for_broadcast_ = start_ts;
    }

    const int64_t round_cost_time = ObTimeUtility::current_time() - start_ts;
    int32_t sleep_ts = 0;
    if (is_replayed || is_replay_failed) {
      sleep_ts = CLOG_REPLAY_DRIVER_LOWER_INTERVAL - static_cast<const int32_t>(round_cost_time);
    } else {
      sleep_ts = CLOG_STATE_DRIVER_INTERVAL - static_cast<const int32_t>(round_cost_time);
    }
    if (sleep_ts < 0) {
      sleep_ts = 0;
    }
    usleep(sleep_ts);

    if (REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
      CLOG_LOG(INFO,
          "ObLogStateDriverRunnable round_cost_time",
          K(round_cost_time),
          K(sleep_ts),
          K(clog_keepalive_interval));
    }
  }
}

void ObLogStateDriverRunnable::check_can_start_service_()
{
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
}
}  // namespace clog
}  // namespace oceanbase
