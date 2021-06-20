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

#include "ob_log_replay_engine.h"
#include "lib/ob_running_mode.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/stat/ob_session_stat.h"
#include "lib/statistic_event/ob_stat_event.h"
#include "lib/utility/ob_tracepoint.h"
#include "lib/hash_func/murmur_hash.h"
#include "share/ob_thread_mgr.h"
#include "storage/ob_partition_service.h"
#include "share/allocator/ob_memstore_allocator_mgr.h"
#include "storage/ob_replay_status.h"
#include "storage/transaction/ob_trans_service.h"
#include "share/ob_multi_cluster_util.h"
#include "clog/ob_partition_log_service.h"
#include "clog/ob_log_entry.h"
#include "clog/ob_clog_mgr.h"

namespace oceanbase {
using namespace common;
using namespace share;
using namespace transaction;
using namespace storage;
using namespace clog;
namespace replayengine {
ObLogReplayEngine::ObLogReplayEngine()
    : is_inited_(false),
      is_stopped_(true),
      tg_id_(-1),
      total_task_num_(0),
      trans_replay_service_(NULL),
      partition_service_(NULL)
{}

ObLogReplayEngine::~ObLogReplayEngine()
{
  destroy();
}

int ObLogReplayEngine::init(
    ObTransService* trans_replay_service, ObPartitionService* partition_service, const ObLogReplayEngineConfig& config)
{
  int ret = OB_SUCCESS;
  const ObAdaptiveStrategy adaptive_strategy(LEAST_THREAD_NUM, ESTIMATE_TS, EXPAND_RATE, SHRINK_RATE);
  const int64_t ALLOCATOR_OBJ_SIZE = sizeof(ObThrottleEndTime);
  const uint64_t ALLOCATOR_TENANT_ID = OB_SERVER_TENANT_ID;
  const int64_t ALLOCATOR_MIN_OBJ_COUNT_ON_BLOCK = 128;
  const int64_t ALLOCATOR_LIMIT_NUM = 2L * 1024L * 1024L * 1024L;  // 2G
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    REPLAY_LOG(WARN, "ObLogReplayEngine init twice", K(ret));
  } else if (OB_UNLIKELY(OB_ISNULL(trans_replay_service) || OB_ISNULL(partition_service) || !config.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    REPLAY_LOG(WARN, "invalid argument", K(ret), KP(trans_replay_service), KP(partition_service), K(config));
  } else if (OB_FAIL(throttle_ts_allocator_.init(ALLOCATOR_OBJ_SIZE,
                 "ThrottleTs",
                 ALLOCATOR_TENANT_ID,
                 OB_MALLOC_NORMAL_BLOCK_SIZE,
                 ALLOCATOR_MIN_OBJ_COUNT_ON_BLOCK,
                 ALLOCATOR_LIMIT_NUM))) {
    CSR_LOG(WARN, "throttle_ts_allocator_ init error", K(ret));
  } else if (OB_FAIL(replay_control_map_.init("ReplayContrMap", OB_SERVER_TENANT_ID))) {
    REPLAY_LOG(WARN, "failed to init replay_control_map_", K(ret));
  } else if (OB_FAIL(TG_CREATE(lib::TGDefIDs::ReplayEngine, tg_id_))) {
    REPLAY_LOG(WARN, "fail to create thread group", K(ret));
  } else if (OB_FAIL(TG_SET_HANDLER_AND_START(tg_id_, *this))) {
    REPLAY_LOG(WARN, "ObSimpleThreadPool init error", K(ret));
  } else if (OB_FAIL(TG_SET_ADAPTIVE_STRATEGY(tg_id_, adaptive_strategy))) {
    REPLAY_LOG(WARN, "set adaptive strategy failed", K(ret));
  } else {
    trans_replay_service_ = trans_replay_service;
    partition_service_ = partition_service;
    total_task_num_ = 0;
    is_inited_ = true;
    is_stopped_ = false;
  }
  if ((OB_FAIL(ret)) && (OB_INIT_TWICE != ret)) {
    destroy();
  }

  if (OB_SUCC(ret)) {
    REPLAY_LOG(
        INFO, "replay engine init success", KP(trans_replay_service), KP(partition_service), K(config), K(tg_id_));
  }
  return ret;
}

void ObLogReplayEngine::destroy()
{
  is_inited_ = false;
  REPLAY_LOG(INFO, "Replay Engine destroy");
  if (-1 != tg_id_) {
    TG_STOP(tg_id_);
    TG_WAIT(tg_id_);
    tg_id_ = -1;
  }
  total_task_num_ = 0;
  trans_replay_service_ = NULL;
  partition_service_ = NULL;
  replay_control_map_.destroy();
  throttle_ts_allocator_.destroy();
}

int ObLogReplayEngine::is_replay_finished(const ObPartitionKey& pkey, bool& is_finished) const
{
  int ret = OB_SUCCESS;
  is_finished = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    REPLAY_LOG(WARN, "ObLogReplayEngine doesn't have been inited", K(pkey), K(ret));
  } else if (OB_ISNULL(partition_service_)) {
    ret = OB_ERR_UNEXPECTED;
    REPLAY_LOG(WARN, "partition service is null", K(ret), K(pkey));
  } else {
    ObIPartitionGroup* partition = NULL;
    ObIPartitionGroupGuard guard;
    ObReplayStatus* replay_status = NULL;
    if (OB_FAIL(partition_service_->get_partition(pkey, guard))) {
      REPLAY_LOG(WARN, "get partition failed", K(pkey), K(ret));
    } else if (OB_ISNULL(partition = guard.get_partition_group()) || !partition->is_valid()) {
      ret = OB_PARTITION_NOT_EXIST;
      REPLAY_LOG(WARN, "replay engine check is replay finish error", KR(ret), K(pkey), KP(partition));
    } else if (OB_ISNULL(replay_status = partition->get_replay_status())) {
      ret = OB_ERR_UNEXPECTED;
      REPLAY_LOG(ERROR, "replay status is NULL when check is replay finished", K(ret), K(pkey));
    } else {
      ObReplayStatus::RLockGuard lock_guard(replay_status->get_rwlock());
      const bool need_submit_log_task = replay_status->need_submit_log_task_without_lock();
      const bool has_pending_task = replay_status->has_pending_task(pkey);
      is_finished = (!has_pending_task && !need_submit_log_task);
      if (REACH_TIME_INTERVAL(10 * 1000)) {
        if (is_finished) {
          REPLAY_LOG(INFO,
              "replay engine has finished replay",
              K(pkey),
              K(has_pending_task),
              K(need_submit_log_task),
              "replay_status",
              *replay_status);
        } else {
          REPLAY_LOG(INFO,
              "replay engine has not finished replay",
              K(pkey),
              K(has_pending_task),
              K(need_submit_log_task),
              "replay_status",
              *replay_status);
        }
      }
    }
  }
  return ret;
}

int ObLogReplayEngine::is_submit_finished(const ObPartitionKey& pkey, bool& is_finished) const
{
  int ret = OB_SUCCESS;
  is_finished = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    REPLAY_LOG(WARN, "ObLogReplayEngine doesn't have been inited", K(ret));
  } else if (OB_ISNULL(partition_service_)) {
    ret = OB_ERR_UNEXPECTED;
    REPLAY_LOG(WARN, "partition service is null", K(ret), K(pkey));
  } else {
    ObIPartitionGroup* partition = NULL;
    ObIPartitionGroupGuard guard;
    ObReplayStatus* replay_status = NULL;
    if (OB_FAIL(partition_service_->get_partition(pkey, guard))) {
      REPLAY_LOG(WARN, "get partition failed", K(pkey), K(ret));
    } else if (OB_ISNULL(partition = guard.get_partition_group()) || !partition->is_valid()) {
      ret = OB_PARTITION_NOT_EXIST;
      REPLAY_LOG(WARN, "replay engine check is replay finish error", KR(ret), K(pkey), KP(partition));
    } else if (OB_ISNULL(replay_status = partition->get_replay_status())) {
      ret = OB_ERR_UNEXPECTED;
      REPLAY_LOG(ERROR, "replay status is NULL when check is replay finished", K(ret), K(pkey));
    } else {
      ObReplayStatus::RLockGuard lock_guard(replay_status->get_rwlock());
      is_finished = !(replay_status->need_submit_log_task_without_lock());
      if (REACH_TIME_INTERVAL(10 * 1000)) {
        if (is_finished) {
          REPLAY_LOG(INFO, "replay engine has finished submit replay task", K(pkey), "replay_status", *replay_status);
        } else {
          REPLAY_LOG(
              INFO, "replay engine has not finished submit replay task", K(pkey), "replay_status", *replay_status);
        }
      }
    }
  }
  return ret;
}

int ObLogReplayEngine::check_can_receive_log(const ObPartitionKey& pkey, bool& can_receive_log) const
{
  // This interface is to prevent the clog disk from being full
  int ret = OB_SUCCESS;
  can_receive_log = true;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    REPLAY_LOG(WARN, "ObLogReplayEngine doesn't have been inited", K(pkey), KR(ret));
  } else if (OB_ISNULL(partition_service_)) {
    ret = OB_ERR_UNEXPECTED;
    REPLAY_LOG(WARN, "partition service is null", K(pkey), KR(ret));
  } else {
    ObIPartitionGroup* partition = NULL;
    ObIPartitionGroupGuard guard;
    ObReplayStatus* replay_status = NULL;
    if (OB_FAIL(partition_service_->get_partition(pkey, guard))) {
      REPLAY_LOG(WARN, "get partition failed", K(pkey), KR(ret));
    } else if (OB_ISNULL(partition = guard.get_partition_group()) || !partition->is_valid()) {
      ret = OB_PARTITION_NOT_EXIST;
      REPLAY_LOG(WARN, "replay engine check is replay finish error", KR(ret), K(pkey), KP(partition));
    } else if (OB_ISNULL(replay_status = partition->get_replay_status())) {
      ret = OB_ERR_UNEXPECTED;
      REPLAY_LOG(ERROR, "replay status is NULL when check is replay finished", KR(ret), K(pkey));
    } else {
      ObReplayStatus::RLockGuard lock_guard(replay_status->get_rwlock());
      if (replay_status->can_receive_log()) {
        can_receive_log = true;
      } else {
        const bool need_submit_log_task = replay_status->need_submit_log_task_without_lock();
        const bool has_pending_task = replay_status->has_pending_task(pkey);
        can_receive_log = (!has_pending_task && !need_submit_log_task);
        if (can_receive_log) {
          replay_status->set_can_receive_log(true);
        }
      }
    }
  }
  return ret;
}

int ObLogReplayEngine::is_tenant_out_of_memory(const common::ObPartitionKey& partition_key, bool& is_out_of_mem)
{
  int ret = OB_SUCCESS;
  is_out_of_mem = false;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    REPLAY_LOG(WARN, "ObLogReplayEngine doesn't have been inited", K(ret));
  } else if (OB_ISNULL(partition_service_)) {
    ret = OB_ERR_UNEXPECTED;
    REPLAY_LOG(WARN, "partition service is null", K(ret), K(partition_key));
  } else {
    ObIPartitionGroup* partition = NULL;
    ObIPartitionGroupGuard guard;
    ObReplayStatus* replay_status = NULL;
    if (OB_FAIL(partition_service_->get_partition(partition_key, guard))) {
      REPLAY_LOG(WARN, "get partition failed", K(partition_key), K(ret));
    } else if (OB_ISNULL(partition = guard.get_partition_group()) || !partition->is_valid()) {
      ret = OB_PARTITION_NOT_EXIST;
      REPLAY_LOG(WARN, "partition not exist", K(ret), K(partition_key), KP(partition));
    } else if (OB_ISNULL(replay_status = partition->get_replay_status())) {
      ret = OB_ERR_UNEXPECTED;
      REPLAY_LOG(ERROR, "replay status is NULL when call is_tenant_out_of_memory", K(ret), K(partition_key));
    } else {
      ObReplayStatus::RLockGuard lock_guard(replay_status->get_rwlock());
      is_out_of_mem = replay_status->is_tenant_out_of_memory();
      if (is_out_of_mem) {
        if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
          REPLAY_LOG(INFO, "tenant has been out of memory", K(partition_key), "replay_status", *replay_status);
        }
      }
    }
  }
  return ret;
}

int ObLogReplayEngine::get_pending_submit_task_count(
    const ObPartitionKey& pkey, int64_t& pending_submit_task_count) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    REPLAY_LOG(WARN, "ObLogReplayEngine doesn't have been inited", K(ret));
  } else if (OB_ISNULL(partition_service_)) {
    ret = OB_ERR_UNEXPECTED;
    REPLAY_LOG(WARN, "partition service is null", K(ret), K(pkey));
  } else {
    ObIPartitionGroup* partition = NULL;
    ObIPartitionGroupGuard guard;
    ObReplayStatus* replay_status = NULL;
    if (OB_FAIL(partition_service_->get_partition(pkey, guard))) {
      REPLAY_LOG(WARN, "get partition failed", K(pkey), K(ret));
    } else if (OB_ISNULL(partition = guard.get_partition_group()) || !partition->is_valid()) {
      ret = OB_PARTITION_NOT_EXIST;
      REPLAY_LOG(WARN, "replay engine check is replay finish error", KR(ret), K(pkey), KP(partition));
    } else if (OB_ISNULL(replay_status = partition->get_replay_status())) {
      ret = OB_ERR_UNEXPECTED;
      REPLAY_LOG(ERROR, "replay status is NULL when check is replay finished", K(ret), K(pkey));
    } else {
      ObReplayStatus::RLockGuard lock_guard(replay_status->get_rwlock());
      pending_submit_task_count = replay_status->get_pending_submit_task_count();
      if (REACH_TIME_INTERVAL(100 * 1000)) {
        REPLAY_LOG(INFO,
            "get_pending_submit_task_count",
            K(pending_submit_task_count),
            K(pkey),
            "replay_status",
            *replay_status);
      }
    }
  }
  return ret;
}

// Notify ReplayEngine that the replay task submitted afterwards should be replayed
int ObLogReplayEngine::add_partition(const ObPartitionKey& pkey)
{
  int ret = OB_SUCCESS;
  ObBaseStorageInfo clog_info;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    REPLAY_LOG(WARN, "replay engine not inited", K(ret));
  } else if (OB_ISNULL(partition_service_)) {
    ret = OB_ERR_UNEXPECTED;
    REPLAY_LOG(WARN, "partition service is null", K(ret), KP(partition_service_));
  } else {
    ObIPartitionGroup* partition = NULL;
    ObReplayStatus* replay_status = NULL;
    ObIPartitionGroupGuard guard;

    if (OB_FAIL(partition_service_->get_partition(pkey, guard))) {
      REPLAY_LOG(ERROR, "get partition failed", K(pkey), K(ret));
    } else if (OB_ISNULL(partition = guard.get_partition_group())) {  // do not check partiiton->is_valid()
      ret = OB_ERR_UNEXPECTED;
      REPLAY_LOG(ERROR, "replay engine add partition error, partition not exist", K(ret), K(pkey));
    } else if (OB_ISNULL(replay_status = partition->get_replay_status())) {
      ret = OB_ERR_UNEXPECTED;
      REPLAY_LOG(ERROR, "replay status is NULL ", K(ret), K(pkey));
    } else if (OB_FAIL(partition->get_saved_clog_info(clog_info))) {
      REPLAY_LOG(WARN, "failed to get_saved_clog_info", K(ret), K(pkey));
    } else {
      ObReplayStatus::WLockGuard wlock_guard(replay_status->get_rwlock());
      replay_status->reuse();
      replay_status->set_submit_log_task_info(pkey, clog_info);
      if (OB_FAIL(replay_status->enable(pkey))) {
        REPLAY_LOG(ERROR, "replay status set online error", K(ret), K(pkey));
      }
    }
  }
  REPLAY_LOG(INFO, "replay engine add partition finish", K(ret), K(pkey), K(clog_info));
  return ret;
}

int ObLogReplayEngine::remove_partition(const ObPartitionKey& pkey, ObIPartitionGroup* partition)
{
  int ret = OB_SUCCESS;
  ObReplayStatus* replay_status = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    REPLAY_LOG(WARN, "replay engine not inited", K(ret));
  } else if (OB_ISNULL(partition) || (!pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    REPLAY_LOG(WARN, "partition is NULL", K(ret), KP(partition), K(pkey));
  } else if (OB_ISNULL(replay_status = partition->get_replay_status())) {
    ret = OB_ERR_UNEXPECTED;
    REPLAY_LOG(ERROR, "replay status is NULL when remove partition", K(ret), K(pkey));
  } else {
    ObReplayStatus::WLockGuard wlock_guard(replay_status->get_rwlock());
    if (OB_FAIL(replay_status->disable(pkey))) {
      REPLAY_LOG(ERROR, "replay status set offline error", K(ret), K(pkey));
    }
  }
  REPLAY_LOG(INFO, "replay engine remove partition finish", K(ret), K(pkey));
  return ret;
}

int ObLogReplayEngine::remove_partition(const ObPartitionKey& pkey)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    REPLAY_LOG(WARN, "replay engine not inited", K(ret));
  } else if (OB_ISNULL(partition_service_)) {
    ret = OB_ERR_UNEXPECTED;
    REPLAY_LOG(WARN, "partition service is null", K(ret), KP(partition_service_));
  } else {
    ObIPartitionGroup* partition = NULL;
    ObReplayStatus* replay_status = NULL;
    ObIPartitionGroupGuard guard;
    if (OB_FAIL(partition_service_->get_partition(pkey, guard))) {
      REPLAY_LOG(WARN, "get partition failed", K(pkey), K(ret));
    } else if (OB_ISNULL(partition = guard.get_partition_group())) {  // do not check partiiton->is_valid()
      // ReplayEngine is notified that the partition is offline and will no longer replay the replay tasks accumulated
      // in the thread pool, but the partition cannot be found in the ObPartitionService, which is not expected.
      ret = OB_ERR_UNEXPECTED;
      REPLAY_LOG(ERROR, "replay engine remove partition error, partition not exist", K(ret), K(pkey));
    } else if (OB_ISNULL(replay_status = partition->get_replay_status())) {
      ret = OB_ERR_UNEXPECTED;
      REPLAY_LOG(ERROR, "replay status is NULL when remove partition", K(ret), K(pkey));
    } else {
      ObReplayStatus::WLockGuard wlock_guard(replay_status->get_rwlock());
      if (OB_FAIL(replay_status->disable(pkey))) {
        REPLAY_LOG(ERROR, "replay status set offline error", K(ret), K(pkey));
      }
    }
  }
  REPLAY_LOG(INFO, "replay engine remove partition finish", K(ret), K(pkey));
  return ret;
}

int ObLogReplayEngine::reset_partition(const ObPartitionKey& pkey)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    REPLAY_LOG(WARN, "replay engine not inited", K(ret));
  } else if (OB_FAIL(remove_partition(pkey))) {
    REPLAY_LOG(WARN, "failed to remove_partition", K(pkey), K(ret));
  } else if (OB_FAIL(add_partition(pkey))) {
    REPLAY_LOG(WARN, "failed to add_partition", K(pkey), K(ret));
  } else {
    REPLAY_LOG(INFO, "success to reset_partition", K(pkey), K(ret));
  }
  return ret;
}

int ObLogReplayEngine::set_need_filter_trans_log(const ObPartitionKey& pkey, const bool need_filter)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    REPLAY_LOG(WARN, "replay engine not inited", K(ret));
  } else if (OB_ISNULL(partition_service_)) {
    ret = OB_ERR_UNEXPECTED;
    REPLAY_LOG(WARN, "partition service is null", K(ret), KP(partition_service_));
  } else {
    ObIPartitionGroup* partition = NULL;
    ObReplayStatus* replay_status = NULL;
    ObIPartitionGroupGuard guard;
    if (OB_FAIL(partition_service_->get_partition(pkey, guard))) {
      REPLAY_LOG(ERROR, "get partition failed", K(pkey), K(ret));
    } else if (OB_ISNULL(partition = guard.get_partition_group())) {  // do not check partiiton->is_valid()
      ret = OB_ERR_UNEXPECTED;
      REPLAY_LOG(ERROR, "partition is not exist when set_need_filter_trans_log", KR(ret), K(pkey));
    } else if (OB_ISNULL(replay_status = partition->get_replay_status())) {
      ret = OB_ERR_UNEXPECTED;
      REPLAY_LOG(ERROR, "replay status is NULL when set_need_filter_trans_log", KR(ret), K(pkey));
    } else {
      ObReplayStatus::WLockGuard wlock_guard(replay_status->get_rwlock());
      (void)replay_status->set_need_filter_trans_log(need_filter);
    }
  }
  REPLAY_LOG(INFO, "replay engine set_need_filter_trans_log", KR(ret), K(pkey), K(need_filter));
  return ret;
}

void ObLogReplayEngine::add_task(ObReplayStatus& replay_status, ObReplayLogTask& replay_task)
{
  replay_status.add_task(replay_task);
  ATOMIC_INC(&total_task_num_);
}

void ObLogReplayEngine::destroy_task(ObReplayStatus& replay_status, ObReplayLogTask& replay_task)
{
  replay_status.free_replay_task(&replay_task);
}

void ObLogReplayEngine::remove_task(ObReplayStatus& replay_status, ObReplayLogTask& replay_task)
{
  replay_status.remove_task(replay_task);
  ATOMIC_DEC(&total_task_num_);
}

int ObLogReplayEngine::push_task(ObReplayStatus& replay_status, ObReplayLogTask& replay_task, uint64_t task_sign)
{
  int ret = OB_SUCCESS;
  add_task(replay_status, replay_task);
  ObReplayPostBarrierStatus old_post_barrier_status = POST_BARRIER_FINISHED;
  ObReplayTaskInfo old_task_info(replay_status.get_last_task_log_id(), replay_status.get_last_task_log_type());

  ObReplayTaskInfo task_info(replay_task.log_id_, replay_task.log_type_);
  replay_status.set_last_task_info(task_info);
  if (ObStorageLogTypeChecker::is_post_barrier_required_log(replay_task.log_type_)) {
    old_post_barrier_status = replay_status.get_post_barrier_status();
    replay_status.set_post_barrier_status(POST_BARRIER_SUBMITTED);
  }
  if (OB_SUCC(replay_status.push_task(replay_task, task_sign))) {
    common::ObTenantStatEstGuard tenantguard(OB_SYS_TENANT_ID);
    EVENT_INC(CLOG_REPLAY_TASK_SUBMIT_COUNT);
  } else {
    // push error, dec pending count now
    replay_status.set_last_task_info(old_task_info);
    if (ObStorageLogTypeChecker::is_post_barrier_required_log(replay_task.log_type_)) {
      replay_status.set_post_barrier_status(old_post_barrier_status);
    }
    remove_task(replay_status, replay_task);
  }
  return ret;
}

bool ObLogReplayEngine::is_valid_param(
    const ObPartitionKey& pkey, const int64_t log_submit_timestamp, const uint64_t log_id) const
{
  return pkey.is_valid() && OB_INVALID_TIMESTAMP != log_submit_timestamp && OB_INVALID_ID != log_id;
}

/* -----------------submit log task related begin------ */
int ObLogReplayEngine::submit_replay_log_task_sequentially(const common::ObPartitionKey& pkey, const uint64_t log_id,
    const int64_t log_ts, const bool need_replay, const ObLogType log_type, const int64_t next_replay_log_ts)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    REPLAY_LOG(ERROR, "replay engine not init", K(pkey), K(need_replay), K(log_id), K(log_ts), K(ret));
  } else if (OB_UNLIKELY(!is_valid_param(pkey, log_id, log_ts))) {
    ret = OB_INVALID_ARGUMENT;
    REPLAY_LOG(ERROR, "invalid arguments", K(pkey), K(need_replay), K(log_id), K(log_ts), K(ret));
  } else if (OB_ISNULL(partition_service_)) {
    ret = OB_INVALID_ARGUMENT;
    REPLAY_LOG(WARN, "partition_service_ is NULL", K(pkey), K(need_replay), K(log_id), K(log_ts), K(log_type), K(ret));
  } else {
    ObReplayStatus* replay_status = NULL;
    ObIPartitionGroup* partition = NULL;
    ObIPartitionGroupGuard guard;
    if (OB_FAIL(partition_service_->get_partition(pkey, guard))) {
      REPLAY_LOG(WARN, "get partition failed", K(pkey), K(ret), K(log_id), K(need_replay));
    } else if (OB_ISNULL(partition = guard.get_partition_group()) || (!partition->is_valid())) {
      // The member change log is a barrier. no logs will be submitted to replay after offline member change log
      // replayed
      ret = OB_ERR_UNEXPECTED;
      REPLAY_LOG(ERROR,
          "partition is not valid when submit replay task",
          KP(partition),
          K(pkey),
          K(need_replay),
          K(log_id),
          K(ret));
    } else if (OB_ISNULL(replay_status = partition->get_replay_status())) {
      ret = OB_ERR_UNEXPECTED;
      REPLAY_LOG(ERROR,
          "replay status is NULL when submit replay task by sw",
          K(pkey),
          K(need_replay),
          K(log_id),
          K(log_ts),
          K(ret));
    } else {
      ObReplayStatus::RLockGuard rlock_guard(replay_status->get_rwlock());
      bool need_submit = true;
      if (need_replay &&
          OB_FAIL(check_need_submit_current_log_(pkey, *partition, log_id, log_ts, *replay_status, need_submit))) {
        REPLAY_LOG(
            ERROR, "failed to check_need_submit_current_log_", K(pkey), K(need_replay), K(log_id), K(log_ts), K(ret));
      } else if (OB_FAIL(replay_status->check_and_submit_task(
                     pkey, log_id, log_ts, (need_replay && need_submit), log_type, next_replay_log_ts))) {
        if (OB_EAGAIN == ret) {
          if (REACH_TIME_INTERVAL(1000 * 1000)) {
            REPLAY_LOG(WARN,
                "failed to check and submit task",
                K(ret),
                K(pkey),
                K(log_id),
                K(log_ts),
                K(log_type),
                "replay_status",
                *replay_status);
          }
        } else {
          REPLAY_LOG(WARN,
              "failed to check and submit task",
              K(ret),
              K(pkey),
              K(log_id),
              K(log_ts),
              K(log_type),
              "replay_status",
              *replay_status);
        }
      }
    }
  }
  return ret;
}

int ObLogReplayEngine::submit_replay_log_task_by_restore(
    const ObPartitionKey& pkey, const uint64_t log_id, const int64_t log_ts)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    REPLAY_LOG(WARN, "replay engine not init", K(pkey), K(log_id), K(log_ts), K(ret));
  } else if (OB_UNLIKELY(!pkey.is_valid() || OB_INVALID_ID == log_id)) {
    ret = OB_INVALID_ARGUMENT;
    REPLAY_LOG(WARN, "invalid arguments", K(pkey), K(log_id), K(log_ts), K(ret));
  } else if (OB_ISNULL(partition_service_)) {
    ret = OB_INVALID_ARGUMENT;
    REPLAY_LOG(WARN, "partition_service_ is NULL", K(pkey), K(log_id), K(log_ts), K(ret));
  } else {
    ObReplayStatus* replay_status = NULL;
    ObIPartitionGroup* partition = NULL;
    ObIPartitionGroupGuard guard;
    if (OB_FAIL(partition_service_->get_partition(pkey, guard))) {
      REPLAY_LOG(WARN, "get partition failed", K(pkey), K(ret), K(log_id), K(log_ts));
    } else if (OB_ISNULL(partition = guard.get_partition_group()) || (!partition->is_valid())) {
      // The member change log is a barrier. no logs will be submitted to replay after offline member change log
      // replayed
      ret = OB_ERR_UNEXPECTED;
      REPLAY_LOG(ERROR, "partition is not valid when submit replay task", K(pkey), K(log_id), K(log_ts), K(ret));
    } else if (OB_ISNULL(replay_status = partition->get_replay_status())) {
      ret = OB_ERR_UNEXPECTED;
      REPLAY_LOG(ERROR, "replay status is NULL when submit replay task", K(pkey), K(log_id), K(log_ts), K(ret));
    } else {
      ObReplayStatus::RLockGuard rlock_guard(replay_status->get_rwlock());
      if (OB_FAIL(replay_status->submit_restore_task(pkey, log_id, log_ts))) {
        if (OB_EAGAIN == ret) {
          if (REACH_TIME_INTERVAL(1000 * 1000)) {
            REPLAY_LOG(WARN,
                "failed to submit_restore_task",
                K(ret),
                K(pkey),
                K(log_id),
                K(log_ts),
                "replay_status",
                *replay_status);
          }
        } else {
          REPLAY_LOG(ERROR,
              "failed to submit_restore_task",
              K(ret),
              K(pkey),
              K(log_id),
              K(log_ts),
              "replay_status",
              *replay_status);
        }
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

void ObLogReplayEngine::handle(void* task)
{
  int ret = OB_SUCCESS;
  ObReplayTask* task_to_handle = static_cast<ObReplayTask*>(task);
  ObReplayStatus* replay_status = NULL;
  bool need_push_back = false;
  ObIPartitionGroup* pt = NULL;
  if (OB_ISNULL(task_to_handle)) {
    ret = OB_INVALID_ARGUMENT;
    REPLAY_LOG(ERROR, " is null", K(ret));
    on_replay_error();
  } else if (OB_ISNULL(replay_status = task_to_handle->get_replay_status())) {
    ret = OB_ERR_UNEXPECTED;
    REPLAY_LOG(ERROR, "replay status is NULL", K(ret));
    on_replay_error();
  } else if (OB_UNLIKELY(IS_NOT_INIT)) {
    ret = OB_NOT_INIT;
    if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
      REPLAY_LOG(ERROR, "replay engine not init", K(ret));
    }
  } else if (OB_ISNULL(partition_service_)) {
    ret = OB_NOT_INIT;
    REPLAY_LOG(ERROR, "partition service is NULL", K(ret));
  } else if (is_stopped_) {
    if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
      REPLAY_LOG(INFO, "replay engine has been stopped, just ignore the task");
    }
    partition_service_->revert_replay_status(replay_status);
    task_to_handle = NULL;
  } else if (NULL == (pt = (ObIPartitionGroup*)REF_KEEPER.lock(replay_status->get_safe_ref()))) {
    REPLAY_LOG(INFO, "partition is already offline, no need to handle the task", "replay_status", *replay_status);
    partition_service_->revert_replay_status(replay_status);
    task_to_handle = NULL;
  } else {
    const ObPartitionKey& pkey = pt->get_partition_key();
    ObIPartitionGroup* partition = NULL;
    ObIPartitionGroupGuard guard;
    bool is_timeslice_run_out = false;
    if (OB_FAIL(pre_check_(pkey, *replay_status, *task_to_handle))) {
      // print log in pre_check_
    } else if (OB_FAIL(partition_service_->get_partition(pkey, guard))) {
      REPLAY_LOG(WARN, "get partition failed, may be partition has been removed", K(pkey), K(ret));
    } else if (OB_ISNULL(partition = guard.get_partition_group())) {  // do not check partiiton->is_valid()
      ret = OB_ERR_UNEXPECTED;
      REPLAY_LOG(ERROR, " partition not exist", K(pkey), K(ret));
    } else if (OB_UNLIKELY(!partition->is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      REPLAY_LOG(ERROR, " partition is not valid", K(pkey), K(partition), K(ret));
    } else {
      ObReplayTaskType task_type = task_to_handle->get_type();

      if (REPLAY_LOG_TASK == task_type) {
        ObReplayLogTaskQueue* task_queue = static_cast<ObReplayLogTaskQueue*>(task_to_handle);
        ret = handle_replay_log_task_(pkey, partition, task_queue, is_timeslice_run_out);
      } else if (FETCH_AND_SUBMIT_LOG_TASK == task_type) {
        ObSubmitReplayLogTask* submit_task = static_cast<ObSubmitReplayLogTask*>(task_to_handle);
        ret = handle_submit_log_task_(pkey, partition, submit_task, is_timeslice_run_out);
      } else {
        ret = OB_ERR_UNEXPECTED;
        REPLAY_LOG(ERROR, "invalid task_type", K(ret), K(pkey), K(task_type), "replay_status", *replay_status);
      }
    }

    REF_KEEPER.unlock(replay_status->get_safe_ref());
    (void)update_replay_fail_info_(pkey, OB_SUCCESS == ret);

    if (OB_FAIL(ret) || is_timeslice_run_out) {
      // replay failed or timeslice is run out or failed to get lock
      need_push_back = true;
    } else if (task_to_handle->revoke_lease()) {
      // success to set state to idle, no need to push back
      partition_service_->revert_replay_status(replay_status);
    } else {
      need_push_back = true;
    }
  }

  if ((OB_FAIL(ret) || need_push_back) && NULL != task_to_handle) {
    // the ret is not OB_SUCCESS  or revoke fails, or the count of logs replayed this time reaches the threshold,
    // the task needs to be push back to the end of the queue
    int tmp_ret = OB_SUCCESS;
    task_to_handle->set_enqueue_ts(ObTimeUtility::fast_current_time());
    if (OB_SUCCESS != (tmp_ret = TG_PUSH_TASK(tg_id_, task_to_handle))) {
      if (NULL != replay_status) {
        REPLAY_LOG(
            ERROR, "failed to submit log replay batch task", "replay_status", *replay_status, K(tmp_ret), K(ret));
      } else {
        REPLAY_LOG(ERROR, "failed to submit log replay batch task", K(replay_status), K(tmp_ret), K(ret));
      }
      // It's impossible to get to this branch, just take a defense
      on_replay_error();
    }
  }
}

int ObLogReplayEngine::submit_task_into_queue(ObReplayTask* task)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    REPLAY_LOG(WARN, "replay engine not init", K(ret));
  } else if (OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
    REPLAY_LOG(ERROR, "invalid batch task is NULL", K(ret));
  } else {
    task->set_enqueue_ts(ObTimeUtility::fast_current_time());
    if (OB_FAIL(TG_PUSH_TASK(tg_id_, task))) {
      REPLAY_LOG(ERROR, "failed to push", K(ret));
    }
  }
  return ret;
}

void ObLogReplayEngine::stop()
{
  REPLAY_LOG(INFO, "Replay Engine stop begin");
  is_stopped_ = true;
  REPLAY_LOG(INFO, "Replay Engine stop finish");
  return;
}

void ObLogReplayEngine::wait()
{
  REPLAY_LOG(INFO, "Replay Engine wait begin");
  while (TG_GET_QUEUE_NUM(tg_id_) > 0) {
    PAUSE();
  }
  REPLAY_LOG(INFO, "Replay Engine SimpleQueue empty");
  TG_STOP(tg_id_);
  TG_WAIT(tg_id_);
  REPLAY_LOG(INFO, "Replay Engine SimpleQueue destroy finish");
  REPLAY_LOG(INFO, "Replay Engine wait finish");
  return;
}

//------------submit log related-----------//
int ObLogReplayEngine::get_log_replay_status(
    const ObPartitionKey& pkey, uint64_t& start_log_id, uint64_t& min_unreplay_log_id)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    REPLAY_LOG(WARN, "ObLogReplayEngine doesn't have been inited", K(pkey), K(ret));
  } else if (OB_ISNULL(partition_service_) || !pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    REPLAY_LOG(WARN, "invalid argument", K(ret), KP(partition_service_), K(pkey));
  } else {
    ObIPartitionGroup* partition = NULL;
    ObIPartitionGroupGuard guard;
    ObIPartitionLogService* log_service = NULL;
    ObReplayStatus* replay_status = NULL;
    if (OB_FAIL(partition_service_->get_partition(pkey, guard))) {
      REPLAY_LOG(WARN, "get partition failed", K(pkey), K(ret));
    } else if (OB_ISNULL(partition = guard.get_partition_group())) {
      ret = OB_ERR_UNEXPECTED;
      REPLAY_LOG(WARN, "partition is NULL", KP(partition), K(pkey), K(ret));
    } else if (!partition->is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      REPLAY_LOG(WARN, "partition is invalid", KPC(partition), K(pkey), K(ret));
    } else if (OB_ISNULL(log_service = partition->get_log_service())) {
      ret = OB_ERR_UNEXPECTED;
      REPLAY_LOG(ERROR, "partition log service is NULL", K(ret), K(pkey), KP(log_service));
    } else if (OB_ISNULL(replay_status = partition->get_replay_status())) {
      ret = OB_ERR_UNEXPECTED;
      REPLAY_LOG(ERROR, "replay status is NULL ", K(ret), K(pkey));
    } else {
      start_log_id = log_service->get_start_log_id_unsafe();
      ObReplayStatus::RLockGuard rlock_guard(replay_status->get_rwlock());
      min_unreplay_log_id = replay_status->get_min_unreplay_log_id();
    }
  }
  return ret;
}

//------------replay related-----------//
int ObLogReplayEngine::do_replay_task(storage::ObIPartitionGroup* partition, ObReplayLogTask* replay_task,
    const int64_t replay_queue_index, int64_t& table_version)
{
  int ret = OB_SUCCESS;
  UNUSED(table_version);
  ObReplayStatus* replay_status = NULL;
  if (OB_ISNULL(replay_task)) {
    ret = OB_INVALID_ARGUMENT;
    REPLAY_LOG(WARN, "invalid argument", KP(replay_task), K(ret));
  } else if (OB_ISNULL(partition) || OB_UNLIKELY(!partition->is_valid())) {
    // It has been determined that the replay_task should be replayed, and get_partition() should
    // not fail at this time
    ret = OB_ERR_UNEXPECTED;
    REPLAY_LOG(ERROR, "get partition failed when check tells need replay", K(partition), K(ret));
  } else if (OB_ISNULL(replay_status = partition->get_replay_status())) {
    ret = OB_ERR_UNEXPECTED;
    REPLAY_LOG(ERROR, "replay status is NULL", K(partition), K(ret));
  } else {
    int64_t start_ts = ObTimeUtility::fast_current_time();
    int64_t task_wait_time = start_ts - replay_task->task_submit_timestamp_;

    ObStorageLogType log_type = replay_task->log_type_;
    if (replay_status->need_filter_trans_log()) {
      // Log replica only needs to replay add_partition_to_pg and remove_partition_to_pg logs and offline logs
      if (ObStorageLogTypeChecker::is_add_partition_to_pg_log(log_type)) {
        REPLAY_LOG(INFO, "start to replay add partition to pg log", "replay_task", *replay_task);
        if (OB_FAIL(partition_service_->replay(replay_task->pk_,
                replay_task->log_buf_,
                replay_task->log_size_,
                replay_task->log_id_,
                replay_task->log_submit_timestamp_))) {
          EVENT_INC(CLOG_FAIL_REPLAY_ADD_PARTITION_TO_PG_LOG_COUNT);
        } else {
          EVENT_INC(CLOG_SUCC_REPLAY_ADD_PARTITION_TO_PG_LOG_COUNT);
          REPLAY_LOG(INFO, "add partition to pg log replay succ", "replay_task", *replay_task);
        }
      } else if (ObStorageLogTypeChecker::is_remove_partition_from_pg_log(log_type)) {
        REPLAY_LOG(INFO, "start to replay remove partition from pg log", "replay_task", *replay_task);
        if (OB_FAIL(partition_service_->replay(replay_task->pk_,
                replay_task->log_buf_,
                replay_task->log_size_,
                replay_task->log_id_,
                replay_task->log_submit_timestamp_))) {
          EVENT_INC(CLOG_FAIL_REPLAY_REMOVE_PG_PARTITION_LOG_COUNT);
        } else {
          EVENT_INC(CLOG_SUCC_REPLAY_REMOVE_PG_PARTITION_LOG_COUNT);
          REPLAY_LOG(INFO, "remove partition from pg log replay succ", "replay_task", *replay_task);
        }
      } else if (OB_LOG_OFFLINE_PARTITION == log_type || OB_LOG_OFFLINE_PARTITION_V2 == log_type) {
        REPLAY_LOG(INFO, "start to replay offline partition log", "replay_task", *replay_task);
        if (OB_SUCC(partition_service_->replay(replay_task->pk_,
                replay_task->log_buf_,
                replay_task->log_size_,
                replay_task->log_id_,
                replay_task->log_submit_timestamp_))) {
          EVENT_INC(CLOG_SUCC_REPLAY_OFFLINE_COUNT);
          REPLAY_LOG(INFO, "succ to replay offline partition log", K(ret), K(*replay_task));
        } else {
          EVENT_INC(CLOG_FAIL_REPLAY_OFFLINE_COUNT);
          REPLAY_LOG(WARN, "failed to replay offline partition log", K(ret), K(*replay_task));
        }
      } else {
        // no need replay
      }
    } else {
      if (ObStorageLogTypeChecker::is_trans_log(log_type)) {
        get_replay_queue_index() = replay_queue_index;
        const int64_t trans_replay_start_time = common::ObTimeUtility::fast_current_time();
        int tmp_ret = OB_SUCCESS;
        int64_t safe_slave_read_timestamp = 0;
        int64_t checkpoint = 0;
        // ignore error, do not set ret
        if (OB_SUCCESS != (tmp_ret = partition->get_weak_read_timestamp(safe_slave_read_timestamp))) {
          if (EXECUTE_COUNT_PER_SEC(10)) {
            REPLAY_LOG(WARN, "get save safe slave read timestamp fail", K(tmp_ret));
          }
          safe_slave_read_timestamp = 0;
          // -1 will be returned when weak read timestamp has not been generated. so set safe_slave_read_timestamp 0 to
          // protect from printing too many warning logs. It does not affect correctness.
        } else if (safe_slave_read_timestamp < 0) {
          safe_slave_read_timestamp = 0;
        } else if (OB_SUCCESS != (tmp_ret = partition->get_replay_checkpoint(checkpoint))) {
          REPLAY_LOG(WARN, "get replay checkpoint failed", K(tmp_ret));
        } else {
          // do nothing
        }
        if (OB_SUCC(trans_replay_service_->replay(replay_task->pk_,
                replay_task->log_buf_,
                replay_task->log_size_,
                replay_task->log_submit_timestamp_,
                replay_task->log_id_,
                safe_slave_read_timestamp,
                replay_task->batch_committed_,
                checkpoint,
                table_version))) {
          EVENT_INC(CLOG_SUCC_REPLAY_TRANS_LOG_COUNT);
          EVENT_ADD(
              CLOG_SUCC_REPLAY_TRANS_LOG_TIME, common::ObTimeUtility::fast_current_time() - trans_replay_start_time);
        } else {
          EVENT_INC(CLOG_FAIL_REPLAY_TRANS_LOG_COUNT);
          EVENT_ADD(
              CLOG_FAIL_REPLAY_TRANS_LOG_TIME, common::ObTimeUtility::fast_current_time() - trans_replay_start_time);
        }
        // reset to -1
        get_replay_queue_index() = -1;
      } else if (ObStorageLogTypeChecker::is_partition_meta_log(log_type)) {
        REPLAY_LOG(INFO, "begin to replay partition meta log", K(ret), K(*replay_task));
        if (OB_SUCC(partition_service_->replay(replay_task->pk_,
                replay_task->log_buf_,
                replay_task->log_size_,
                replay_task->log_id_,
                replay_task->log_submit_timestamp_))) {
          EVENT_INC(CLOG_SUCC_REPLAY_META_LOG_COUNT);
          REPLAY_LOG(INFO, "succ to replay partition meta log", K(ret), K(*replay_task));
        } else {
          REPLAY_LOG(WARN, "failed to replay partition meta log", K(ret), K(*replay_task));
          EVENT_INC(CLOG_FAIL_REPLAY_META_LOG_COUNT);
        }
      } else if (ObStorageLogTypeChecker::is_add_partition_to_pg_log(log_type)) {
        REPLAY_LOG(INFO, "start to replay add partition to pg log", "replay_task", *replay_task);
        if (OB_FAIL(partition_service_->replay(replay_task->pk_,
                replay_task->log_buf_,
                replay_task->log_size_,
                replay_task->log_id_,
                replay_task->log_submit_timestamp_))) {
          EVENT_INC(CLOG_FAIL_REPLAY_ADD_PARTITION_TO_PG_LOG_COUNT);
        } else {
          EVENT_INC(CLOG_SUCC_REPLAY_ADD_PARTITION_TO_PG_LOG_COUNT);
          REPLAY_LOG(INFO, "add partition to pg log replay succ", "replay_task", *replay_task);
        }
      } else if (ObStorageLogTypeChecker::is_remove_partition_from_pg_log(log_type)) {
        REPLAY_LOG(INFO, "start to replay remove partition from pg log", "replay_task", *replay_task);
        if (OB_FAIL(partition_service_->replay(replay_task->pk_,
                replay_task->log_buf_,
                replay_task->log_size_,
                replay_task->log_id_,
                replay_task->log_submit_timestamp_))) {
          EVENT_INC(CLOG_FAIL_REPLAY_REMOVE_PG_PARTITION_LOG_COUNT);
        } else {
          EVENT_INC(CLOG_SUCC_REPLAY_REMOVE_PG_PARTITION_LOG_COUNT);
          REPLAY_LOG(INFO, "remove partition from pg log replay succ", "replay_task", *replay_task);
        }
      } else if (OB_LOG_OFFLINE_PARTITION == log_type || OB_LOG_OFFLINE_PARTITION_V2 == log_type) {
        REPLAY_LOG(INFO, "start to replay offline partition log", "replay_task", *replay_task);
        if (OB_SUCC(partition_service_->replay(replay_task->pk_,
                replay_task->log_buf_,
                replay_task->log_size_,
                replay_task->log_id_,
                replay_task->log_submit_timestamp_))) {
          EVENT_INC(CLOG_SUCC_REPLAY_OFFLINE_COUNT);
          REPLAY_LOG(INFO, "succ to replay offline partition log", K(ret), K(*replay_task));
        } else {
          EVENT_INC(CLOG_FAIL_REPLAY_OFFLINE_COUNT);
          REPLAY_LOG(WARN, "failed to replay offline partition log", K(ret), K(*replay_task));
        }
      } else if (ObStorageLogTypeChecker::is_schema_version_change_log(log_type)) {
        REPLAY_LOG(INFO, "start to replay partition schema version change log", "replay_task", *replay_task);
        if (OB_FAIL(partition_service_->replay(replay_task->pk_,
                replay_task->log_buf_,
                replay_task->log_size_,
                replay_task->log_id_,
                replay_task->log_submit_timestamp_))) {
        } else {
          REPLAY_LOG(INFO, "replay partition schema version change log success", "replay_task", *replay_task);
        }
      } else if (ObStorageLogTypeChecker::is_split_log(log_type)) {
        REPLAY_LOG(INFO, "start to replay split log", K(ret), K(*replay_task));
        if (OB_SUCC(partition_service_->replay(replay_task->pk_,
                replay_task->log_buf_,
                replay_task->log_size_,
                replay_task->log_id_,
                replay_task->log_submit_timestamp_))) {
          EVENT_INC(CLOG_SUCC_REPLAY_SPLIT_LOG_COUNT);
          REPLAY_LOG(INFO, "succ to replay split log", K(ret), K(*replay_task));
        } else {
          EVENT_INC(CLOG_FAIL_REPLAY_SPLIT_LOG_COUNT);
          REPLAY_LOG(WARN, "failed to replay split log", K(ret), K(*replay_task));
        }
      } else if (OB_LOG_TRANS_CHECKPOINT == log_type) {
        if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
          REPLAY_LOG(INFO, "start to replay trans checkpoint log", "replay_task", *replay_task);
        }
        ObCheckpointLog log;
        int64_t pos = serialization::encoded_length_i64(log_type);
        if (OB_FAIL(log.deserialize(replay_task->log_buf_, replay_task->log_size_, pos))) {
          REPLAY_LOG(WARN, "deserialize checkpoint log failed", K(ret));
        } else if (OB_FAIL(partition->set_replay_checkpoint(log.get_checkpoint()))) {
          REPLAY_LOG(WARN, "set replay checkpoint failed", K(ret), K(log), K(*replay_task));
        } else {
          if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
            REPLAY_LOG(INFO, "succ to replay trans checkpoint log", "replay_task", *replay_task);
          }
        }
      } else if (ObStorageLogTypeChecker::is_start_membership_log(log_type)) {
        REPLAY_LOG(INFO, "start to replay start membership log", "replay_task", *replay_task);
        const int64_t trans_replay_start_time = common::ObTimeUtility::fast_current_time();
        if (OB_FAIL(trans_replay_service_->replay_start_working_log(
                replay_task->pk_, replay_task->log_submit_timestamp_, replay_task->log_id_))) {
          REPLAY_LOG(WARN, "start membership log replay fail", "replay_task", *replay_task);
          EVENT_INC(CLOG_FAIL_REPLAY_START_MEMBERSHIP_LOG_COUNT);
          EVENT_ADD(CLOG_FAIL_REPLAY_START_MEMBERSHIP_LOG_TIME,
              common::ObTimeUtility::fast_current_time() - trans_replay_start_time);
        } else {
          REPLAY_LOG(INFO, "start membership log replay succ", "replay_task", *replay_task);
          EVENT_INC(CLOG_SUCC_REPLAY_START_MEMBERSHIP_LOG_COUNT);
          EVENT_ADD(CLOG_SUCC_REPLAY_START_MEMBERSHIP_LOG_TIME,
              common::ObTimeUtility::fast_current_time() - trans_replay_start_time);
        }
      } else if (ObStorageLogTypeChecker::is_test_log(log_type) || ObStorageLogTypeChecker::is_freeze_log(log_type)) {
        // do nothing
      } else {
        ret = OB_INVALID_LOG;
        REPLAY_LOG(ERROR, "invalid log type", K(log_type), "replay_task", *replay_task, KR(ret));
      }
    }

    if (OB_SUCC(ret)) {
      replay_status->inc_total_replayed_task_num();
      if (ObStorageLogTypeChecker::is_post_barrier_required_log(log_type)) {
        ObReplayTaskInfo task_info(replay_task->log_id_, replay_task->log_type_);
        if (OB_FAIL(replay_status->set_post_barrier_finished(task_info))) {
          REPLAY_LOG(WARN,
              "post barrier log replay failed",
              K(ret),
              "replay_task",
              *replay_task,
              K(task_info),
              "replay_status",
              *replay_status);
        } else {
          REPLAY_LOG(INFO, "post barrier log replay succ", K(task_info), "replay_task", *replay_task);
        }
      }

      int64_t replay_used_time = ObTimeUtility::fast_current_time() - start_ts;
      if (replay_used_time > REPLAY_TASK_REPLAY_TIMEOUT) {
        REPLAY_LOG(WARN, "single replay task cost too much time", K(replay_used_time), "replay_task", *replay_task);
      }
    }
  }

  return ret;
}

// the caller needs to lock
int ObLogReplayEngine::submit_single_replay_task_(ObReplayLogTask& replay_task, ObReplayStatus& replay_status)
{
  int ret = OB_SUCCESS;
  int64_t trans_id = 0;
  const char* log_buf = replay_task.log_buf_;
  const int64_t buf_len = replay_task.log_size_;
  const ObStorageLogType log_type = replay_task.log_type_;
  const uint64_t log_id = replay_task.log_id_;
  const ObPartitionKey& pk = replay_task.pk_;

  int64_t pos = serialization::encoded_length_i64(int64_t(0));  // skip log type
  if (ObStorageLogTypeChecker::is_trans_log(log_type)) {
    if (OB_FAIL(serialization::decode_i64(log_buf, buf_len, pos, &trans_id))) {
      REPLAY_LOG(WARN, "deserialize trans id error", KP(log_buf), K(buf_len), K(pos), K(trans_id), K(replay_task));
    } else if (OB_FAIL(submit_trans_log_(replay_task, trans_id, replay_status))) {
      REPLAY_LOG(ERROR, "failed to submit trans log", K(log_type), K(replay_task), K(replay_status), K(ret));
    } else { /*do nothing*/
    }
  } else if (OB_FAIL(submit_non_trans_log_(replay_task, replay_status))) {
    if (OB_EAGAIN == ret) {
      if (REACH_TIME_INTERVAL(1000 * 1000)) {
        REPLAY_LOG(WARN, "failed to submit non trans log", K(log_type), K(replay_task), K(replay_status), K(ret));
      }
    } else {
      REPLAY_LOG(ERROR, "failed to submit non trans log", K(log_type), K(replay_task), K(replay_status), K(ret));
    }
  } else {
    if (OB_LOG_TRANS_CHECKPOINT == log_type) {
      if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
        REPLAY_LOG(INFO, "succ to submit checkpoint log", K(pk), K(log_type), K(log_id), K(replay_status), KR(ret));
      }
    } else {
      REPLAY_LOG(INFO,
          "succ to submit non trans log",
          "log_type",
          ObStorageLogTypeToString::storage_log_type_to_string(log_type),
          K(pk),
          K(log_id),
          K(replay_status));
    }
  }

  if (OB_SUCC(ret)) {
    replay_status.inc_total_submitted_task_num();
  }
  return ret;
}

// the caller needs to lock
int ObLogReplayEngine::submit_aggre_replay_task_(
    ObReplayLogTask& replay_task, ObReplayStatus& replay_status, bool& is_first_task_pushed_in_aggr_log)
{
  int ret = OB_SUCCESS;
  ReplayLogTaskArray task_array;
  if (OB_FAIL(prepare_replay_task_array_(replay_task, replay_status, task_array))) {
    if (OB_EAGAIN == ret) {
      if (REACH_TIME_INTERVAL(1000 * 1000)) {
        REPLAY_LOG(WARN, "prepare_replay_task_array_ failed", K(replay_task), K(replay_status), K(ret));
      }
    } else {
      REPLAY_LOG(WARN, "prepare_replay_task_array_ failed", K(replay_task), K(replay_status), K(ret));
    }
  } else if (OB_FAIL(submit_aggre_trans_log_(replay_status, task_array, is_first_task_pushed_in_aggr_log))) {
    // This function is not allowed to fail
    REPLAY_LOG(ERROR, "submit_aggre_trans_log_ failed", K(ret), K(replay_task));
    if (!is_first_task_pushed_in_aggr_log) {
      (void)destroy_replay_task_array_(replay_status, task_array);
    }
  } else {
    replay_status.inc_x_total_submitted_task_num(task_array.count());
  }
  return ret;
}

int ObLogReplayEngine::prepare_replay_task_array_(
    ObReplayLogTask& replay_task, ObReplayStatus& replay_status, ReplayLogTaskArray& task_array)
{
  int ret = OB_SUCCESS;
  int32_t next_log_offset = 0;
  char* log_buf = replay_task.log_buf_;
  const int64_t buf_len = replay_task.log_size_;
  const int64_t log_submit_timestamp = replay_task.log_submit_timestamp_;
  ObPartitionKey& pkey = replay_task.pk_;
  uint64_t log_id = replay_task.log_id_;
  while (OB_SUCC(ret) && next_log_offset < buf_len) {
    int64_t saved_pos = next_log_offset;
    int64_t pos = next_log_offset;
    void* buff = NULL;
    int64_t log_type_in_buf = storage::OB_LOG_UNKNOWN;
    ObStorageLogType log_type = storage::OB_LOG_UNKNOWN;
    int64_t trans_id = 0;
    int64_t submit_timestamp = 0;
    if (OB_FAIL(serialization::decode_i32(log_buf, buf_len, pos, &next_log_offset))) {
      REPLAY_LOG(ERROR,
          "serialization decode_i32 failed",
          KR(ret),
          K(buf_len),
          K(pos),
          K(pkey),
          K(log_id),
          K(next_log_offset));
    } else if (OB_FAIL(serialization::decode_i64(log_buf, buf_len, pos, &submit_timestamp))) {
      REPLAY_LOG(ERROR,
          "serialization decode_i64 failed",
          KR(ret),
          K(buf_len),
          K(pos),
          K(pkey),
          K(log_id),
          K(next_log_offset));
    } else if (OB_FAIL(serialization::decode_i64(log_buf, buf_len, pos, &log_type_in_buf))) {
      REPLAY_LOG(ERROR,
          "serialization decode_i64 failed",
          KR(ret),
          K(buf_len),
          K(pos),
          K(pkey),
          K(log_id),
          K(next_log_offset));
    } else if (OB_FAIL(serialization::decode_i64(log_buf, buf_len, pos, &trans_id))) {
      REPLAY_LOG(ERROR,
          "serialization decode_i64 failed",
          KR(ret),
          K(buf_len),
          K(pos),
          K(pkey),
          K(log_id),
          K(next_log_offset));
    } else {
      const int64_t log_len = next_log_offset - saved_pos - AGGRE_LOG_RESERVED_SIZE;
      log_type = static_cast<ObStorageLogType>(log_type_in_buf);
      ObReplayLogTaskEx next_replay_task_ex;
      if (0 == saved_pos) {
        // The first replay task, reuse the incoming replay_task, no need to allocate one
        replay_task.log_type_ = log_type;
        replay_task.log_size_ = log_len;
        replay_task.log_submit_timestamp_ =
            submit_timestamp != TRANS_AGGRE_LOG_TIMESTAMP ? submit_timestamp : log_submit_timestamp;
        replay_task.log_buf_ = log_buf + saved_pos + AGGRE_LOG_RESERVED_SIZE;
        replay_task.batch_committed_ = false;

        next_replay_task_ex.task_ = &replay_task;
        next_replay_task_ex.trans_id_ = trans_id;
        if (OB_FAIL(task_array.push_back(next_replay_task_ex))) {
          REPLAY_LOG(ERROR, "task_array push failed", K(ret), K(pkey), K(log_id), K(replay_task), K(saved_pos));
          // memory of replay_task will be free outside
        }
      } else {
        ObReplayLogTask::RefBuf* ref_buf = replay_task.ref_buf_;
        if (OB_ISNULL(ref_buf) || OB_ISNULL(ref_buf->buf_)) {
          ret = OB_ERR_UNEXPECTED;
          REPLAY_LOG(ERROR, "invalid buf ref", KP(ref_buf), K(pkey), K(log_id), K(replay_task), K(replay_status));
        } else if (OB_UNLIKELY(NULL == (buff = replay_status.alloc_replay_task_buf(sizeof(ObReplayLogTask), pkey)))) {
          ret = OB_EAGAIN;
          if (REACH_TIME_INTERVAL(1000 * 1000)) {
            REPLAY_LOG(WARN, "alloc memory failed for aggre log", K(ret), K(pkey), K(log_id), K(replay_status));
          }
        } else {
          ObReplayLogTask* next_replay_task = new (buff) ObReplayLogTask();
          next_replay_task->pk_ = pkey;
          next_replay_task->log_type_ = log_type;
          next_replay_task->log_size_ = log_len;
          next_replay_task->log_submit_timestamp_ =
              submit_timestamp != TRANS_AGGRE_LOG_TIMESTAMP ? submit_timestamp : log_submit_timestamp;
          next_replay_task->log_id_ = log_id;
          next_replay_task->batch_committed_ = false;
          next_replay_task->task_submit_timestamp_ = ObTimeUtility::fast_current_time();
          next_replay_task->log_buf_ = log_buf + saved_pos + AGGRE_LOG_RESERVED_SIZE;
          next_replay_task->set_ref_buf(ref_buf);

          next_replay_task_ex.task_ = next_replay_task;
          next_replay_task_ex.trans_id_ = trans_id;
          if (OB_FAIL(task_array.push_back(next_replay_task_ex))) {
            REPLAY_LOG(ERROR, "task_array push failed", K(ret), K(pkey), K(log_id), K(saved_pos));
            replay_status.free_replay_task(next_replay_task);
            next_replay_task = NULL;
            buff = NULL;
          }
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    destroy_replay_task_array_(replay_status, task_array);
  }

  return ret;
}

int ObLogReplayEngine::submit_aggre_trans_log_(
    storage::ObReplayStatus& replay_status, ReplayLogTaskArray& task_array, bool& is_first_task_pushed_in_aggr_log)
{
  int ret = OB_SUCCESS;
  const int64_t task_cnt = task_array.count();
  for (int64_t i = 0; (OB_SUCCESS == ret) && i < task_cnt; i++) {
    ObReplayLogTaskEx task_ex = task_array[i];
    ObReplayLogTask* task = task_ex.task_;
    if (OB_ISNULL(task)) {
      ret = OB_ERR_UNEXPECTED;
      REPLAY_LOG(ERROR, "log task is NULL", K(ret), K(replay_status), K(i));
    } else {
      const uint64_t task_sign = get_replay_task_sign_(task->pk_, task_ex.trans_id_);
      if (OB_FAIL(push_task(replay_status, *task, task_sign))) {
        REPLAY_LOG(ERROR, "push_task failed", K(ret), "task", *task, K(i));
        if (i > 0) {
          // only part of tasks has been pushed， which can not be retry anymore
          is_first_task_pushed_in_aggr_log = true;
        }
      } else {
        EVENT_INC(CLOG_REPLAY_TASK_TRANS_SUBMIT_COUNT);
      }
    }
  }
  return ret;
}

void ObLogReplayEngine::destroy_replay_task_array_(
    storage::ObReplayStatus& replay_status, ReplayLogTaskArray& task_array)
{
  const int64_t task_cnt = task_array.count();
  for (int64_t i = 1; i < task_cnt; i++) {
    // The first replay_task is cleaned up by the outside
    if (NULL != task_array[i].task_) {
      replay_status.free_replay_task(task_array[i].task_);
    }
  }
}

int ObLogReplayEngine::submit_trans_log_(ObReplayLogTask& replay_task, int64_t trans_id, ObReplayStatus& replay_status)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(trans_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    REPLAY_LOG(ERROR, "invalid arguemnts", K(replay_task), K(trans_id), K(replay_status), K(ret));
  } else {
    const uint64_t task_sign = get_replay_task_sign_(replay_task.pk_, trans_id);
    if (OB_FAIL(push_task(replay_status, replay_task, task_sign))) {
      REPLAY_LOG(ERROR, "failed to push_task when submitting trans log", K(ret), K(trans_id), K(replay_task));
    } else {
      EVENT_INC(CLOG_REPLAY_TASK_TRANS_SUBMIT_COUNT);
    }
  }
  return ret;
}

int ObLogReplayEngine::check_condition_before_submit_non_trans_log_(ObReplayLogTask& task)
{
  int ret = OB_SUCCESS;
  const ObPartitionKey& partition_key = task.pk_;
  const ObStorageLogType log_type = task.log_type_;
  const char* log_buf = task.log_buf_;
  const int64_t buf_len = task.log_size_;
  if (OB_LOG_SPLIT_SOURCE_PARTITION == log_type) {
    if (OB_FAIL(E(EventTable::EN_DELAY_REPLAY_SOURCE_SPLIT_LOG) OB_SUCCESS)) {
      STORAGE_LOG(WARN, "ERRSIM: EN_DELAY_REPLAY_SOURCE_SPLIT_LOG", K(ret), K(partition_key));
    }
  } else if (OB_LOG_SPLIT_DEST_PARTITION == log_type) {
    uint64_t start_log_id = 0;
    uint64_t min_unreplay_log_id = 0;
    ObPartitionSplitDestLog dest_log;
    int64_t pos = serialization::encoded_length_i64(0);  // skip log type
    if (OB_FAIL(dest_log.deserialize(log_buf, buf_len, pos))) {
      REPLAY_LOG(WARN, "deserialize split dest log failed", K(ret), K(partition_key));
    } else if (OB_FAIL(
                   get_log_replay_status(dest_log.get_spp().get_source_pkey(), start_log_id, min_unreplay_log_id))) {
      REPLAY_LOG(WARN, "get log replay status failed", K(ret), K(partition_key), K(dest_log));
      if (OB_PARTITION_NOT_EXIST == ret) {
        // rewrite ret
        ret = OB_SUCCESS;
      }
    } else if (start_log_id <= dest_log.get_source_log_id() || min_unreplay_log_id <= dest_log.get_source_log_id()) {
      ret = OB_EAGAIN;
      if (REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
        REPLAY_LOG(WARN,
            "source split log has not replayed",
            K(ret),
            K(partition_key),
            K(dest_log),
            K(start_log_id),
            K(min_unreplay_log_id));
      }
    }
  }

  return ret;
}

int ObLogReplayEngine::submit_non_trans_log_(ObReplayLogTask& replay_task, ObReplayStatus& replay_status)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObStorageLogTypeChecker::is_transfer_log(replay_task.log_type_))) {
    ret = OB_ERR_UNEXPECTED;
    REPLAY_LOG(ERROR, "transfer log is not supported so far", K(ret), K(replay_task), K(replay_status));
  } else if (OB_FAIL(check_condition_before_submit_non_trans_log_(replay_task))) {
    //
  } else {
    ObStorageLogType log_type = replay_task.log_type_;
    if (OB_FAIL(push_task(replay_status, replay_task, replay_task.pk_.hash()))) {
      REPLAY_LOG(ERROR,
          "failed to submit_non_trans_log. ",
          "log_type",
          ObStorageLogTypeToString::storage_log_type_to_string(replay_task.log_type_),
          K(replay_task),
          K(ret),
          K(replay_status));
    } else {
      // Improve statistics
      switch (log_type) {
        case OB_LOG_OFFLINE_PARTITION:
        case OB_LOG_OFFLINE_PARTITION_V2: {
          EVENT_INC(CLOG_REPLAY_TASK_OFFLINE_SUBMIT_COUNT);
          break;
        }
        case OB_LOG_START_MEMBERSHIP_STORAGE: {
          EVENT_INC(CLOG_REPLAY_TASK_START_MEMBERSHIP_SUBMIT_COUNT);
          break;
        }
        case OB_LOG_ADD_PARTITION_TO_PG: {
          EVENT_INC(CLOG_REPLAY_TASK_ADD_PARTITION_TO_PG_SUBMIT_COUNT);
          break;
        }
        case OB_LOG_REMOVE_PARTITION_FROM_PG: {
          EVENT_INC(CLOG_REPLAY_TASK_REMOVE_PG_PARTITION_SUBMIT_COUNT);
          break;
        }
        case OB_LOG_SPLIT_DEST_PARTITION:
        case OB_LOG_SPLIT_SOURCE_PARTITION: {
          EVENT_INC(CLOG_REPLAY_TASK_SPLIT_SUBMIT_COUNT);
          break;
        }
        case OB_LOG_PARTITION_SCHEMA: {
          EVENT_INC(CLOG_REPLAY_TASK_META_SUBMIT_COUNT);
          break;
        }
        default: { /*do nothing*/
        }
      }
    }
  }
  return ret;
}

int ObLogReplayEngine::handle_replay_log_task_(const common::ObPartitionKey& pkey, ObIPartitionGroup* partition,
    ObReplayLogTaskQueue* task_queue, bool& is_timeslice_run_out)
{
  int ret = OB_SUCCESS;
  bool is_queue_empty = false;
  ObReplayStatus* replay_status = NULL;
  if (OB_ISNULL(partition) || OB_ISNULL(task_queue)) {
    ret = OB_INVALID_ARGUMENT;
    REPLAY_LOG(ERROR, "invalid argument is null", KP(partition), KP(task_queue), K(ret));
    on_replay_error();
  } else if (OB_ISNULL(replay_status = task_queue->get_replay_status())) {
    ret = OB_ERR_UNEXPECTED;
    REPLAY_LOG(ERROR, "replay status is NULL", K(ret));
    on_replay_error();
  } else {
    int64_t start_ts = ObTimeUtility::fast_current_time();
    do {
      const int64_t single_replay_start_ts = ObTimeUtility::fast_current_time();
      int64_t replay_task_used = 0;
      int64_t destroy_task_used = 0;
      ObLink* link = NULL;
      ObLink* link_to_destroy = NULL;
      ObReplayLogTask* replay_task = NULL;
      ObReplayLogTask* replay_task_to_destroy = NULL;
      bool this_trans_pending = false;
      int64_t table_version = 0;

      if (replay_status->get_rwlock().try_rdlock()) {
        int64_t table_version = 0;
        if (NULL == (link = task_queue->top())) {
          // queue is empty
          ret = OB_SUCCESS;
          is_queue_empty = true;
          task_queue->reset_fail_info();
        } else if (OB_ISNULL(replay_task = static_cast<ObReplayLogTask*>(link))) {
          ret = OB_ERR_UNEXPECTED;
          REPLAY_LOG(ERROR, "replay_task is NULL", "replay_status", *replay_status, K(ret));
        } else if (OB_FAIL(do_replay_task(partition, replay_task, task_queue->index_, table_version))) {
          (void)process_replay_ret_code_(ret, table_version, *replay_status, *task_queue, *replay_task);
        } else if (OB_ISNULL(link_to_destroy = task_queue->pop())) {
          REPLAY_LOG(ERROR, "failed to pop task after replay", "replay_task", *replay_task, K(ret));
          // It's impossible to get to this branch. Use on_replay_error to defend it.
          on_replay_error(*replay_task, ret);
        } else if (OB_ISNULL(replay_task_to_destroy = static_cast<ObReplayLogTask*>(link_to_destroy))) {
          ret = OB_ERR_UNEXPECTED;
          REPLAY_LOG(ERROR, "replay_task_to_destroy is NULL when pop after replay", K(replay_task), K(ret));
          // It's impossible to get to this branch. Use on_replay_error to defend it.
          on_replay_error(*replay_task, ret);
        } else {
          task_queue->reset_fail_info();
          // try to ensure log in one transaction to be replayed in one round.
          ObReplayLogTask* next_task = (ObReplayLogTask*)task_queue->top();
          int64_t this_trans_inc = replay_task->get_trans_inc_no();
          this_trans_pending =
              this_trans_inc >= 0 && (NULL != next_task) && (next_task->get_trans_inc_no() == this_trans_inc);

          replay_task_used = ObTimeUtility::fast_current_time() - single_replay_start_ts;
          EVENT_INC(CLOG_HANDLE_REPLAY_TASK_COUNT);
          EVENT_ADD(CLOG_HANDLE_REPLAY_TIME, replay_task_used);

          int64_t destroy_start_ts = ObTimeUtility::fast_current_time();
          // Ensure that the memory access is valid when invoking get_min_unreplay_log_id()
          task_queue->wait_sync();
          // free memory of replay_task
          remove_task(*replay_status, *replay_task_to_destroy);
          destroy_task(*replay_status, *replay_task_to_destroy);
          destroy_task_used = ObTimeUtility::fast_current_time() - destroy_start_ts;

          // To avoid a single task occupies too long thread time, the upper limit of
          // single occupancy time is set to 10ms
          int64_t used_time = ObTimeUtility::fast_current_time() - start_ts;
          if (!this_trans_pending && used_time > MAX_REPLAY_TIME_PER_ROUND) {
            is_timeslice_run_out = true;
          }
        }
        replay_status->get_rwlock().unlock();

        int64_t retry_count = OB_SUCC(ret) ? 0 : 1;
        // for writing throttling
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (update_replay_throttling_info_(pkey, *replay_status))) {
          REPLAY_LOG(
              ERROR, "failed to update_replay_throttling_info_", K(tmp_ret), K(pkey), "replay_status", *replay_status);
        }

        statistics_replay(replay_task_used, destroy_task_used, retry_count);
      } else {
        // write lock is locked, it may be that the parition is being cleaned up.
        // Just return OB_EAGAIN to aviod occuping worker threads
        ret = OB_EAGAIN;
        if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
          REPLAY_LOG(INFO, "try lock failed", "replay_status", *replay_status, K(ret));
        }
      }
    } while (OB_SUCC(ret) && (!is_queue_empty) && (!is_timeslice_run_out));
  }
  return ret;
}

int ObLogReplayEngine::handle_submit_log_task_(const ObPartitionKey& pkey, ObIPartitionGroup* partition,
    ObSubmitReplayLogTask* submit_task, bool& is_timeslice_run_out)
{
  int ret = OB_SUCCESS;
  ObReplayStatus* replay_status = NULL;
  ObIPartitionLogService* log_service = NULL;
  if (OB_ISNULL(partition) || OB_ISNULL(log_service = partition->get_log_service()) || OB_ISNULL(submit_task) ||
      OB_ISNULL(replay_status = submit_task->get_replay_status())) {
    ret = OB_INVALID_ARGUMENT;
    REPLAY_LOG(ERROR,
        "invalid argument",
        KP(partition),
        KP(log_service),
        KP(submit_task),
        KP(replay_status),
        K(pkey),
        KR(ret));
    on_replay_error();
  } else {
    const int64_t start_ts = ObTimeUtility::fast_current_time();
    int64_t single_end_time = ObTimeUtility::fast_current_time();
    bool need_submit_log = true;
    int64_t total_log_size = 0;
    int64_t total_log_count = 0;
    while (OB_SUCC(ret) && need_submit_log && (!is_timeslice_run_out)) {
      int64_t accum_checksum = 0;
      int64_t submit_timestamp = 0;
      const int64_t single_start_time = ObTimeUtility::fast_current_time();
      int64_t log_size = 0;
      uint64_t to_submit_log_id = OB_INVALID_ID;
      if (replay_status->get_rwlock().try_rdlock()) {
        {
          ObReplayStatus::RLockGuard Rlock_guard(replay_status->get_submit_log_info_rwlock());
          need_submit_log = submit_task->need_submit_log();
          to_submit_log_id = submit_task->get_next_submit_log_id();
        }
        bool has_encount_fatal_error = false;
        if (!need_submit_log) {
          // do nothing
        } else {
          if (replay_status->is_pending()) {
            ret = OB_EAGAIN;
            if (REACH_TIME_INTERVAL(1000 * 1000)) {
              REPLAY_LOG(INFO, "replay status is pending", K(ret), K(pkey), "submit_task", *submit_task);
            }
          } else if (OB_SUCC(fetch_and_submit_single_log_(pkey,
                         *partition,
                         *replay_status,
                         submit_task,
                         to_submit_log_id,
                         accum_checksum,
                         submit_timestamp,
                         log_size,
                         has_encount_fatal_error))) {
            submit_task->set_storage_log_type(storage::OB_LOG_UNKNOWN);
            submit_task->accum_checksum_ = accum_checksum;
            {

              ObReplayStatus::WLockGuard wlock_guard(replay_status->get_submit_log_info_rwlock());
              if (OB_FAIL(replay_status->update_next_submit_log_info(to_submit_log_id + 1, submit_timestamp + 1))) {
                REPLAY_LOG(ERROR,
                    "failed to update_next_submit_log_info",
                    KR(ret),
                    K(pkey),
                    K(to_submit_log_id),
                    K(submit_timestamp));
                replay_status->set_replay_err_info(to_submit_log_id, storage::OB_LOG_UNKNOWN, single_start_time, ret);
              }
            }
            total_log_size += log_size;
            total_log_count++;
            if (REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
              REPLAY_LOG(INFO,
                  "succ to submit log task to replay engine",
                  K(pkey),
                  K(to_submit_log_id),
                  K(submit_timestamp),
                  "replay_status",
                  *replay_status);
            }
          } else if (OB_EAGAIN == ret) {
            if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
              REPLAY_LOG(WARN,
                  "submit single log task need retry",
                  K(pkey),
                  K(to_submit_log_id),
                  "replay_status",
                  *replay_status,
                  K(ret));
            }
          } else {
            REPLAY_LOG(WARN,
                "failed to fetch and submit single log",
                K(pkey),
                K(to_submit_log_id),
                "replay_status",
                *replay_status,
                K(ret));
          }
        }
        if (OB_SUCCESS != ret) {
          if (has_encount_fatal_error) {
            submit_task->set_encount_fatal_error(ret, single_start_time);
          } else {
            submit_task->set_simple_fail_info(ret, single_start_time);
          }
        } else {
          submit_task->reset_fail_info();
        }
        replay_status->get_rwlock().unlock();
      } else {
        // Return OB_EAGAIN to avoid taking up worker threads
        ret = OB_EAGAIN;
        if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
          REPLAY_LOG(INFO, "try lock failed", "replay_status", *replay_status, K(ret));
        }
      }

      // To avoid a single task occupying too much thread time, set the upper limit of single
      // occupancy time to 10ms
      single_end_time = ObTimeUtility::fast_current_time();
      int64_t used_time = single_end_time - start_ts;
      if (OB_SUCC(ret) && used_time > MAX_SUBMIT_TIME_PER_ROUND) {
        is_timeslice_run_out = true;
      }
    };

    if (OB_SUCC(ret) && total_log_count > 0) {
      int64_t task_used_time = single_end_time - start_ts;
      EVENT_INC(CLOG_HANDLE_SUBMIT_TASK_COUNT);
      EVENT_ADD(CLOG_HANDLE_SUBMIT_TASK_SIZE, total_log_size);
      EVENT_ADD(CLOG_HANDLE_SUBMIT_TIME, task_used_time);
      statistics_submit(task_used_time, total_log_size, total_log_count);
    }
  }
  return ret;
}

// external caller locks
int ObLogReplayEngine::check_condition_after_fetch_log_(const ObPartitionKey& pkey, ObSubmitReplayLogTask* submit_task,
    clog::ObLogEntry& log_entry, ObStorageLogType& storage_log_type, bool& need_replay)
{
  int ret = OB_SUCCESS;
  ObReplayStatus* replay_status = NULL;
  const clog::ObLogEntryHeader& header = log_entry.get_header();
  ObLogType log_type = header.get_log_type();
  uint64_t log_id = header.get_log_id();
  int64_t log_type_in_buf = storage::OB_LOG_UNKNOWN;
  if (!is_log_type_need_replay(log_type)) {
    // Member change log or NOP log do not need to be replayed
    need_replay = false;
  } else {
    if (OB_ISNULL(submit_task) || OB_ISNULL(replay_status = submit_task->get_replay_status())) {
      ret = OB_INVALID_ARGUMENT;
      REPLAY_LOG(ERROR, "submit task or replay status is NULL", KP(submit_task), KP(replay_status), K(log_entry));
    } else if (OB_LOG_SUBMIT == log_type) {
      int64_t pos = 0;
      if (OB_FAIL(serialization::decode_i64(
              log_entry.get_buf(), log_entry.get_header().get_data_len(), pos, &log_type_in_buf))) {
        REPLAY_LOG(ERROR, "failed to deserialize log type", K(ret), K(log_entry));
      } else {
        storage_log_type = static_cast<ObStorageLogType>(log_type_in_buf);
      }
    } else if (OB_LOG_AGGRE == log_type) {
      storage_log_type = OB_LOG_TRANS_AGGRE;
    } else if (OB_LOG_START_MEMBERSHIP == log_type) {
      storage_log_type = OB_LOG_START_MEMBERSHIP_STORAGE;
    } else {
      ret = OB_ERR_UNEXPECTED;
      REPLAY_LOG(ERROR, "invalid log_type", K(ret), K(pkey), K(log_type), K(log_entry));
    }

    if (OB_SUCC(ret)) {
      need_replay = (!replay_status->need_filter_trans_log()) ||
                    ObStorageLogTypeChecker::is_log_replica_need_replay_log(storage_log_type);
    }

    if (OB_SUCC(ret) && need_replay) {
      ObReplayTaskInfo task_info(log_id, storage_log_type);
      ObReplayStatus::CheckCanReplayResult check_result(pkey, task_info);
      submit_task->set_storage_log_type(storage_log_type);
      if (OB_FAIL(replay_status->can_replay_log(check_result))) {
        if (OB_EAGAIN == ret) {
          if (REACH_TIME_INTERVAL(1000 * 1000)) {
            REPLAY_LOG(WARN, "can not replay log now", K(ret), K(check_result), "replay_status", *replay_status);
          }
        } else {
          REPLAY_LOG(WARN, "can not replay log now", K(ret), K(check_result), "replay_status", *replay_status);
        }
      }
    }
  }
  return ret;
}

void ObLogReplayEngine::statistics_submit(
    const int64_t single_submit_task_used, const int64_t log_size, const int64_t log_count)
{
  static __thread int64_t SUBMIT_TASK_USED = 0;
  static __thread int64_t SUBMIT_LOG_SIZE = 0;
  static __thread int64_t SUBMIT_LOG_COUNT = 0;
  static __thread int64_t TASK_COUNT = 0;

  SUBMIT_TASK_USED += single_submit_task_used;
  SUBMIT_LOG_SIZE += log_size;
  SUBMIT_LOG_COUNT += log_count;
  TASK_COUNT++;

  if (TC_REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
    REPLAY_LOG(INFO,
        "submit log statistics",
        "avg_submit_task_used",
        SUBMIT_TASK_USED / (TASK_COUNT + 1),
        "avg_submit_task_log_size",
        SUBMIT_LOG_SIZE / (TASK_COUNT + 1),
        "avg_submit_task_log_count",
        SUBMIT_LOG_COUNT / (TASK_COUNT + 1),
        "avg_log_size",
        SUBMIT_LOG_SIZE / (SUBMIT_LOG_COUNT + 1),
        K(SUBMIT_LOG_SIZE),
        K(SUBMIT_LOG_COUNT),
        K(TASK_COUNT));
    SUBMIT_TASK_USED = 0;
    SUBMIT_LOG_SIZE = 0;
    TASK_COUNT = 0;
  }
}

void ObLogReplayEngine::statistics_replay(
    const int64_t single_replay_task_used, const int64_t single_destroy_task_used, const int64_t retry_count)
{
  static __thread int64_t REPLAY_TASK_USED = 0;
  static __thread int64_t RETIRE_TASK_USED = 0;
  static __thread int64_t RETRY_COUNT = 0;
  static __thread int64_t TASK_COUNT = 0;

  REPLAY_TASK_USED += single_replay_task_used;
  RETIRE_TASK_USED += single_destroy_task_used;
  RETRY_COUNT += retry_count;
  TASK_COUNT++;

  if (TC_REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
    REPLAY_LOG(INFO,
        "log replay engine statistics",
        "avg_replay_task_used",
        REPLAY_TASK_USED / (TASK_COUNT + 1),
        "avg_retire_task_used",
        RETIRE_TASK_USED / (TASK_COUNT + 1),
        "avg_retry_count",
        RETRY_COUNT / (TASK_COUNT + 1),
        "task_count",
        TASK_COUNT);
    REPLAY_TASK_USED = 0;
    RETIRE_TASK_USED = 0;
    RETRY_COUNT = 0;
    TASK_COUNT = 0;
  }
}

void ObLogReplayEngine::on_replay_error(ObReplayLogTask& replay_task, int ret)
{
  const int64_t bad_moment = ObTimeUtility::fast_current_time();
  while (is_inited_) {
    if (REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
      REPLAY_LOG(ERROR, "REPLAY_ERROR", K(ret), K(replay_task), K(bad_moment));
    }
    usleep(1 * 1000 * 1000);  // sleep 1s
  }
}

void ObLogReplayEngine::on_replay_error()
{
  const int64_t bad_moment = ObTimeUtility::fast_current_time();
  while (is_inited_) {
    if (REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
      REPLAY_LOG(ERROR, "REPLAY_ERROR", K(bad_moment));
    }
    usleep(1 * 1000 * 1000);  // sleep 1s
  }
}

int ObLogReplayEngine::fetch_and_submit_single_log_(const ObPartitionKey& pkey, ObIPartitionGroup& partition,
    ObReplayStatus& replay_status, storage::ObSubmitReplayLogTask* submit_task, uint64_t log_id,
    int64_t& accum_checksum, int64_t& submit_timestamp, int64_t& log_size, bool& has_encount_fatal_error)
{
  int ret = OB_SUCCESS;
  clog::ObICLogMgr* clog_mgr = NULL;
  ObILogEngine* log_engine = NULL;
  log_size = 0;
  if (OB_ISNULL(partition_service_) || OB_ISNULL(clog_mgr = partition_service_->get_clog_mgr()) ||
      OB_ISNULL(log_engine = clog_mgr->get_log_engine())) {
    ret = OB_ERR_UNEXPECTED;
    REPLAY_LOG(ERROR,
        "partition_service or clog_mgr or log_engine is NULL",
        KP(partition_service_),
        KP(clog_mgr),
        KP(log_engine),
        K(ret),
        K(pkey),
        K(log_id));
  } else {
    ObLogCursorExt log_cursor_ext;
    log_size = 0;
    if (OB_FAIL(log_engine->get_cursor(pkey, log_id, log_cursor_ext))) {
      if (OB_CURSOR_NOT_EXIST == ret) {
        // return OB_EAGAIN if get_cursor failed
        ret = OB_EAGAIN;
        CLOG_LOG(WARN, "ilog_storage get invalid log, cursor not exist", K(ret), K(pkey), K(log_id));
      } else if (OB_NEED_RETRY == ret || OB_ERR_OUT_OF_UPPER_BOUND == ret) {
        CLOG_LOG(WARN, "get cursor need retry", K(ret), K(pkey), K(log_id));
        ret = OB_EAGAIN;
      } else {
        CLOG_LOG(WARN, "log_engine get_cursor failed", K(ret), K(pkey), K(log_id));
      }
    } else {
      uint32_t clog_min_using_file_id = log_engine->get_clog_min_using_file_id();
      if (log_cursor_ext.get_file_id() <= clog_min_using_file_id) {
        bool need_block_receive_log = false;
        if (OB_FAIL(log_engine->check_need_block_log(need_block_receive_log))) {
          REPLAY_LOG(WARN, "failed to check_need_block_log ", K(ret), K(pkey), K(log_id));
        } else if (need_block_receive_log) {
          // mark replay status
          replay_status.set_can_receive_log(false);
          if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
            REPLAY_LOG(
                WARN, "can not receive log now", K(log_cursor_ext), K(pkey), K(clog_min_using_file_id), K(log_id));
          }
        } else {
          replay_status.set_can_receive_log(true);
        }
      } else {
        replay_status.set_can_receive_log(true);
      }

      if (OB_SUCC(ret)) {
        accum_checksum = log_cursor_ext.get_accum_checksum();
        submit_timestamp = log_cursor_ext.get_submit_timestamp();

        ObReadParam read_param;
        read_param.file_id_ = log_cursor_ext.get_file_id();
        read_param.offset_ = log_cursor_ext.get_offset();
        read_param.read_len_ = log_cursor_ext.get_size();
        int64_t log_real_length = 0;
        bool need_submit = true;
        if (OB_FAIL(check_need_submit_current_log_(
                pkey, partition, log_id, submit_timestamp, replay_status, need_submit))) {
          REPLAY_LOG(ERROR, "failed to check need_submit_crrent_log", K(pkey), K(log_id), KR(ret));
        } else if (!need_submit) {
          // dp nothing
        } else if (OB_FAIL(log_engine->get_clog_real_length(read_param, log_real_length))) {
          REPLAY_LOG(WARN, "failed to get_clog_real_length", K(pkey), K(read_param), KR(ret));
        } else {
          const int64_t buf_size = sizeof(ObReplayLogTask) + sizeof(ObReplayLogTask::RefBuf) + log_real_length;
          void* buff = NULL;

          if (OB_UNLIKELY(NULL == (buff = replay_status.alloc_replay_task_buf(buf_size, pkey)))) {
            ret = OB_EAGAIN;
            if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
              REPLAY_LOG(WARN, "failed to alloc replay task buf", K(ret), K(pkey), K(log_id));
            }
          } else {
            ObReplayLogTask* replay_task = new (buff) ObReplayLogTask();
            ObReplayLogTask::RefBuf* ref_buf = new ((char*)buff + sizeof(ObReplayLogTask)) ObReplayLogTask::RefBuf();
            ref_buf->buf_ = buff;
            replay_task->pk_ = pkey;
            replay_task->set_ref_buf(ref_buf);

            char* log_entry_buf = (char*)buff + sizeof(ObReplayLogTask) + sizeof(ObReplayLogTask::RefBuf);
            ObReadBuf read_buf;
            read_buf.buf_ = log_entry_buf;
            read_buf.buf_len_ = log_real_length;
            log_size = log_real_length;

            clog::ObLogEntry log_entry;
            has_encount_fatal_error = false;
            if (OB_FAIL(log_engine->read_log_by_location(read_param, read_buf, log_entry))) {
              CLOG_LOG(WARN,
                  "read_log_by_location failed",
                  K(ret),
                  K(pkey),
                  K(log_id),
                  K(read_param),
                  K(read_buf),
                  K(log_cursor_ext),
                  K(log_real_length));
            } else {
              ObLogType log_type = log_entry.get_header().get_log_type();
              ObStorageLogType storage_log_type = ObStorageLogType::OB_LOG_UNKNOWN;
              bool need_replay = true;
              if (OB_FAIL(
                      check_condition_after_fetch_log_(pkey, submit_task, log_entry, storage_log_type, need_replay))) {
                // no log here
              } else if (!need_replay) {
                replay_status.free_replay_task(replay_task);
              } else {
                bool is_batch_committed = log_cursor_ext.is_batch_committed();
                replay_task->pk_ = pkey;
                replay_task->log_type_ = storage_log_type;
                replay_task->log_buf_ = const_cast<char*>(log_entry.get_buf());
                replay_task->log_size_ = log_entry.get_header().get_data_len();
                replay_task->log_submit_timestamp_ = log_entry.get_header().get_submit_timestamp();
                replay_task->log_id_ = log_entry.get_header().get_log_id();
                replay_task->batch_committed_ = is_batch_committed;
                replay_task->task_submit_timestamp_ = ObTimeUtility::fast_current_time();
                if (OB_UNLIKELY(!replay_task->is_valid())) {
                  ret = OB_INVALID_ARGUMENT;
                  REPLAY_LOG(ERROR,
                      "replay_task is invalid",
                      K(storage_log_type),
                      "replay_task",
                      *replay_task,
                      K(replay_status),
                      K(ret));
                } else if (OB_LOG_SUBMIT == log_type || OB_LOG_START_MEMBERSHIP == log_type) {
                  ret = submit_single_replay_task_(*replay_task, replay_status);
                } else if (OB_LOG_AGGRE == log_type) {
                  ret = submit_aggre_replay_task_(*replay_task, replay_status, has_encount_fatal_error);
                } else {
                  ret = OB_ERR_UNEXPECTED;
                  REPLAY_LOG(ERROR, "invalid log_type", K(ret), K(pkey), K(log_type), K(log_entry));
                }
              }
            }

            if (OB_FAIL(ret) && !has_encount_fatal_error) {
              // if has_encount_fatal_error = true, the replay_task may has been push into queue in aggr
              // log, so we can not free replay_task
              replay_status.free_replay_task(replay_task);
            }
          }
        }
      }
    }
  }
  return ret;
}

void ObLogReplayEngine::process_replay_ret_code_(const int ret_code, const int64_t table_version,
    ObReplayStatus& replay_status, ObReplayLogTaskQueue& task_queue, ObReplayLogTask& replay_task)
{
  if (OB_SUCCESS != ret_code) {
    int64_t cur_ts = ObTimeUtility::fast_current_time();
    if (replay_status.is_fatal_error(ret_code)) {
      replay_status.set_replay_err_info(replay_task.log_id_, replay_task.log_type_, cur_ts, ret_code);
      REPLAY_LOG(ERROR,
          "replay task encount fatal error",
          "replay_status",
          replay_status,
          "replay_task",
          replay_task,
          K(ret_code));
    } else if (OB_TRANS_WAIT_SCHEMA_REFRESH == ret_code || REACH_TIME_INTERVAL(1 * 1000 * 1000L)) {
      REPLAY_LOG(WARN,
          "replay task failed because of schema is not new enough",
          K(ret_code),
          K(table_version),
          "replay_status",
          replay_status,
          "replay_task",
          replay_task);
    } else { /*do nothing*/
    }

    if (OB_SUCCESS == task_queue.get_fail_info().ret_code_) {
      task_queue.set_fail_info(table_version, cur_ts, replay_task.log_id_, replay_task.log_type_, ret_code);
    } else if (task_queue.get_fail_info().ret_code_ != ret_code) {
      // just override ret_code
      task_queue.set_fail_ret_code(ret_code);
    } else { /*do nothing*/
    }
  }
}

int ObLogReplayEngine::pre_check_(const ObPartitionKey& pkey, ObReplayStatus& replay_status, ObReplayTask& task)
{
  // Only set the status of ObReplaystatus to ERROR when encounts fatal error,
  // to avoid replay tasks of current partiiton run out the memory of tenant, resulting in affecting other
  // partitions
  int ret = OB_SUCCESS;
  if (replay_status.get_rwlock().try_rdlock()) {
    /*
     *check steps:
     *1. Check the waiting time of the task in the global queue
     *2. replaying of log task has encounted fatal error, just exists directly
     *3. check if writing throttling is required
     *4. check whether the replay conditions of current round are met
     *   4.1 if last round returned OB_TRANS_WAIT_SCHEMA_REFRESH, then check whether schema_version meets expectations
     *   4.2 If it is a log submission task, check whether the current memory can continue to submit
     *   log task, and check barrier conditions, etc.
     * */

    // 1. Check the waiting time of the task in the global queue
    int64_t cur_time = ObTimeUtility::fast_current_time();
    int64_t enqueue_ts = task.get_enqueue_ts();
    if ((cur_time - enqueue_ts > TASK_QUEUE_WAIT_IN_GLOBAL_QUEUE_TIME_THRESHOLD) && (!task.has_been_throttled())) {
      REPLAY_LOG(WARN, "task queue has waitted too much time in global queue", K(pkey), K(replay_status), K(task));
    }

    // 2. replaying of log task has encounted fatal error, just exists directly
    ObReplayTaskType task_type = task.get_type();
    if (OB_UNLIKELY(replay_status.has_encount_fatal_error() || task.has_encount_fatal_error())) {
      // encounted fatal error last round,set OB_EAGAIN here
      if (REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
        REPLAY_LOG(ERROR, "replay has encount fatal error", "replay_status", replay_status, K(task), K(pkey));
      }
      ret = OB_EAGAIN;  // set egain just to push back into thread_pool
    } else {
      if (task.is_last_round_failed() && (cur_time - task.fail_info_.fail_ts_ > FAILURE_TIME_THRESHOLD) &&
          REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
        if ((REPLAY_LOG_TASK == task_type && GCTX.is_standby_cluster() &&
                (OB_TRANS_WAIT_SCHEMA_REFRESH == task.fail_info_.ret_code_)) ||
            task.has_been_throttled() || replay_status.is_pending()) {
          REPLAY_LOG(WARN, "task has been failed for more than 10 seconds", K(pkey), K(task), K(replay_status));
        } else {
          REPLAY_LOG(ERROR, "task has been failed for more than 10 seconds", K(pkey), K(task), K(replay_status));
        }
      }

      int tmp_ret = OB_SUCCESS;
      bool need_throttle = false;
      // 3. check if writing throttling is required
      if (OB_SUCCESS != (check_need_replay_throttling_(pkey, task, replay_status, need_throttle))) {
        // ignore this
      } else if (need_throttle) {
        tmp_ret = OB_EAGAIN;
        if (REACH_TIME_INTERVAL(1 * 1000 * 1000L)) {
          REPLAY_LOG(WARN, "replay throttling", "replay_status", replay_status, K(pkey));
        }
      } else { /*do nothing*/
      }

      if (OB_SUCCESS == ret && OB_SUCCESS == tmp_ret) {
        /*4. check whether the replay conditions of current round are met
         *   4.1 if last round returned OB_TRANS_WAIT_SCHEMA_REFRESH, then check whether schema_version meets
         * expectations 4.2 If it is a log submission task, check whether the current memory can continue to submit
         *   log task, and check barrier conditions, etc.*/
        if (REPLAY_LOG_TASK == task_type) {
          if (OB_TRANS_WAIT_SCHEMA_REFRESH == task.fail_info_.ret_code_) {
            const int64_t table_version = task.get_fail_info().table_version_;
            if (OB_SUCCESS != (tmp_ret = check_standby_cluster_schema_condition_(pkey, table_version))) {
              if (OB_TRANS_WAIT_SCHEMA_REFRESH != tmp_ret) {
                REPLAY_LOG(ERROR,
                    "failed to check_standby_cluster_schema_condition_",
                    K(tmp_ret),
                    K(pkey),
                    K(replay_status),
                    K(task));
              }
            }
          }
        } else if (FETCH_AND_SUBMIT_LOG_TASK == task_type) {
          const int64_t LOG_TASK_SIZE = 2 * 1024 * 1024L;
          if (!replay_status.can_alloc_replay_task(LOG_TASK_SIZE, pkey)) {
            tmp_ret = OB_EAGAIN;
            if (REACH_TIME_INTERVAL(1 * 1000 * 1000L)) {
              REPLAY_LOG(
                  WARN, "can not alloc replay task now, no memory", K(tmp_ret), K(pkey), K(replay_status), K(task));
            }
          } else {
            ObSubmitReplayLogTask& submit_task = static_cast<ObSubmitReplayLogTask&>(task);
            ObStorageLogType storage_log_type = submit_task.storage_log_type_;
            // check if can replay in last round return no. this round checks again to save the overhead of reading logs
            if (storage::OB_LOG_UNKNOWN != storage_log_type) {
              // Here do not need lock. there must be logs that need to be replayed, and there is no
              // concurrency problem
              uint64_t to_submit_log_id = submit_task.get_next_submit_log_id();
              ObReplayTaskInfo task_info(to_submit_log_id, storage_log_type);
              ObReplayStatus::CheckCanReplayResult check_result(pkey, task_info);
              if (OB_SUCCESS != (tmp_ret = replay_status.can_replay_log(check_result))) {
                if (OB_EAGAIN == tmp_ret) {
                  if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
                    REPLAY_LOG(WARN,
                        "can not submit replay task now",
                        K(tmp_ret),
                        K(check_result),
                        K(replay_status),
                        K(submit_task));
                  }
                } else {
                  REPLAY_LOG(ERROR,
                      "can not submit replay task now",
                      K(tmp_ret),
                      K(check_result),
                      K(submit_task),
                      K(replay_status));
                }
              }
            }
          }
        } else {
          tmp_ret = OB_ERR_UNEXPECTED;
          REPLAY_LOG(ERROR, "invalid task_type", K(tmp_ret), K(pkey), K(task_type), K(replay_status), K(task));
        }
      }

      if (OB_SUCCESS != tmp_ret) {
        if (OB_SUCCESS == task.get_fail_info().ret_code_) {
          task.set_simple_fail_info(tmp_ret, cur_time);
        } else if (task.get_fail_info().ret_code_ != tmp_ret) {
          // just override ret_code
          task.set_fail_ret_code(tmp_ret);
        }
        ret = tmp_ret;
      }
    }
    // unlock
    replay_status.get_rwlock().unlock();

    if (OB_FAIL(ret) && REACH_TIME_INTERVAL(1 * 1000 * 1000L)) {
      REPLAY_LOG(WARN, "failed to pre_check", K(ret), K(pkey), K(replay_status), K(task));
    }
  } else {
    // At this time, the write lock is added, and the partition may be cleaning up
    if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
      REPLAY_LOG(INFO, "try lock failed in pre_check", K(replay_status), K(task), K(ret));
    }
  }
  return ret;
}

int ObLogReplayEngine::check_standby_cluster_schema_condition_(const ObPartitionKey& pkey, const int64_t table_version)
{
  int ret = OB_SUCCESS;
  if (table_version > 0 && GCTX.is_standby_cluster()) {
    // only stand_by cluster need to be check
    uint64_t tenant_id = pkey.get_tenant_id();
    if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)) || OB_SYS_TENANT_ID == tenant_id) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "invalid tenant_id", K(ret), K(tenant_id), K(pkey), K(table_version));
    } else {
      // user tables of normal tenants(not sys tenant) need to be checked by schema version of tenant belonging to;
      // sys tables of normal tenants(not sys tenant) need to be checked by schema version of sys tenent;
      uint64_t referred_tenant_id = is_inner_table(pkey.get_table_id()) ? OB_SYS_TENANT_ID : tenant_id;
      int64_t tenant_schema_version = 0;
      if (OB_FAIL(GSCHEMASERVICE.get_tenant_refreshed_schema_version(referred_tenant_id, tenant_schema_version))) {
        TRANS_LOG(WARN,
            "get_tenant_schema_version failed",
            K(ret),
            K(referred_tenant_id),
            K(pkey),
            K(tenant_id),
            K(table_version));
        if (OB_ENTRY_NOT_EXIST == ret) {
          // In scenarios such as restarting, the tenant schema has not been updated yet.
          // rewrite OB_ENTRY_NOT_EXIST with OB_TRANS_WAIT_SCHEMA_REFRESH
          ret = OB_TRANS_WAIT_SCHEMA_REFRESH;
        }
      } else if (table_version > tenant_schema_version) {
        // The table version of the data to be replayed is greater than the schema version of the reference tenant,
        // and replaying is not allowed
        if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
          TRANS_LOG(WARN,
              "tenant_schema_version is not new enough",
              K(ret),
              K(referred_tenant_id),
              K(pkey),
              K(tenant_id),
              K(table_version),
              K(tenant_schema_version));
        }
        ret = OB_TRANS_WAIT_SCHEMA_REFRESH;
      }
    }
  }
  return ret;
}

// Used to filter the transaction log during the follower replaying
int ObLogReplayEngine::check_need_submit_current_log_(const ObPartitionKey& partition_key, ObIPartitionGroup& partition,
    const uint64_t log_id, const int64_t log_submit_timestamp, ObReplayStatus& replay_status, bool& need_submit)
{
  int ret = OB_SUCCESS;
  uint64_t last_replay_log_id = replay_status.get_last_replay_log_id();
  need_submit = true;
  if (common::OB_INVALID_ID == last_replay_log_id) {
    ObDataStorageInfo data_info;
    if (OB_FAIL(partition.get_saved_data_info(data_info))) {
      REPLAY_LOG(ERROR, "get saved data info error", K(ret), K(partition_key), K(log_id), K(log_submit_timestamp));
    } else {
      replay_status.update_last_replay_log_id(partition_key, data_info.get_last_replay_log_id());
      // Ensure that only one thread updates and all threads consume
      last_replay_log_id = replay_status.get_last_replay_log_id();
    }
  }
  if (OB_SUCC(ret)) {
    if (log_id <= last_replay_log_id) {
      need_submit = false;
    } else {
      need_submit = true;
    }
  }
  return ret;
}

int ObLogReplayEngine::check_need_replay_throttling_(
    const ObPartitionKey& pkey, ObReplayTask& task, ObReplayStatus& replay_status, bool& need_control)
{
  int ret = OB_SUCCESS;
  need_control = false;
  if (replay_status.is_pending()) {
    // The partition in the process of taking the freezing point, its log replaying will not be
    // affected by the writing throttling
  } else {
    const uint64_t tenant_id = pkey.get_tenant_id();
    ObGMemstoreAllocator* memstore_allocator = NULL;
    bool need_control = false;
    int tmp_ret = OB_SUCCESS;
    int64_t cur_time = ObTimeUtility::fast_current_time();
    ObThrottleEndTime* throttle_end_time = NULL;
    const bool create_if_not_exist = false;
    if (OB_FAIL(get_throttle_end_time_(pkey.get_tenant_id(), create_if_not_exist, throttle_end_time)) &&
        OB_ENTRY_NOT_EXIST != ret) {
      REPLAY_LOG(WARN, "failed to get throttle_end_time", K(pkey), K(ret), K(replay_status));
    } else if (OB_ENTRY_NOT_EXIST == ret) {
      task.throttled_ts_ = OB_INVALID_TIMESTAMP;
      ret = OB_SUCCESS;
    } else if (OB_ISNULL(throttle_end_time)) {
      ret = OB_ERR_UNEXPECTED;
      REPLAY_LOG(ERROR, "throttle_end_time is NULL", K(pkey), K(ret), K(replay_status));
    } else if (cur_time > throttle_end_time->end_ts_) {
      // do nothing
      task.throttled_ts_ = OB_INVALID_TIMESTAMP;
    } else if (OB_FAIL(ObMemstoreAllocatorMgr::get_instance().get_tenant_memstore_allocator(
                   tenant_id, memstore_allocator))) {
      REPLAY_LOG(ERROR, "failed to get_tenant_memstore_allocator", K(pkey), K(ret), K(replay_status));
    } else if (OB_ISNULL(memstore_allocator)) {
      ret = OB_ENTRY_NOT_EXIST;
      REPLAY_LOG(ERROR, "get_tenant_mutil_allocator failed", K(pkey), K(ret), K(replay_status));
    } else {
      need_control = memstore_allocator->need_do_writing_throttle();
      if (OB_INVALID_TIMESTAMP == task.throttled_ts_) {
        task.throttled_ts_ = cur_time;
      }
    }
  }
  return ret;
}

int ObLogReplayEngine::update_replay_throttling_info_(const ObPartitionKey& pkey, ObReplayStatus& replay_status)
{
  // This function is single-threaded serial, no concurrency
  int ret = OB_SUCCESS;
  uint32_t& sleep_interval = get_writing_throttling_sleep_interval();
  if (sleep_interval > 0) {
    ObThrottleEndTime* throttle_end_time = NULL;
    const bool create_if_not_exist = true;
    if (OB_FAIL(get_throttle_end_time_(pkey.get_tenant_id(), create_if_not_exist, throttle_end_time))) {
      REPLAY_LOG(WARN, "failed to get throttle_end_time", K(pkey), KR(ret), K(replay_status));
    } else if (OB_ISNULL(throttle_end_time)) {
      ret = OB_ERR_UNEXPECTED;
      REPLAY_LOG(ERROR, "throttle_end_time is NULL", K(pkey), KR(ret), K(replay_status));
    } else {
      throttle_end_time->end_ts_ += sleep_interval;
    }
  }
  sleep_interval = 0;
  return ret;
}

int ObLogReplayEngine::update_replay_fail_info_(const ObPartitionKey& pkey, bool is_replay_succ)
{
  // This function is single-threaded serial, no concurrency, count the number of consecutive
  // failures of the thread, more than 50 times, then sleep 500us
  int ret = OB_SUCCESS;
  ObThrottleEndTime* throttle_end_time = NULL;
  // Only when the playback fails, you need to ensure that the throttle_end_time must exist
  const bool create_if_not_exist = !is_replay_succ;
  if (OB_FAIL(
          get_throttle_end_time_(0 /*record fail count with tenant_id=0*/, create_if_not_exist, throttle_end_time)) &&
      OB_ENTRY_NOT_EXIST != ret) {
    REPLAY_LOG(WARN, "failed to get throttle_end_time", K(pkey), KR(ret));
  } else if (OB_ENTRY_NOT_EXIST == ret) {
    if (is_replay_succ) {
      // dothing
      ret = OB_SUCCESS;
    } else {
      REPLAY_LOG(WARN, "failed to get throttle_end_time", K(pkey), KR(ret));
    }
  } else if (OB_ISNULL(throttle_end_time)) {
    ret = OB_ERR_UNEXPECTED;
    REPLAY_LOG(ERROR, "throttle_end_time is NULL", K(pkey), K(ret));
  } else if (is_replay_succ) {
    // just reset_fail_count
    throttle_end_time->fail_count_ = 0;
  } else if (SLEEP_TRIGGER_CNT <= throttle_end_time->fail_count_) {
    usleep(SLEEP_INTERVAL);
    throttle_end_time->fail_count_ = 0;
  } else {
    // increase the consecutive errors
    throttle_end_time->fail_count_ += 1;
  }
  return ret;
}

bool ObLogReplayEngine::need_simple_thread_replay_table_(const uint64_t table_id) const
{
  return (OB_ALL_WEAK_READ_SERVICE_TID == extract_pure_id(table_id));
}

uint64_t ObLogReplayEngine::get_replay_task_sign_(const ObPartitionKey& pkey, const int64_t trans_id) const
{
  uint64_t task_sign = 0;
  if (need_simple_thread_replay_table_(pkey.get_table_id())) {
    task_sign = 0;
  } else {
    const uint64_t seed = pkey.hash();
    task_sign = common::murmurhash(&trans_id, sizeof(trans_id), seed);
  }
  return task_sign;
}

// ObThrottleEndTime corresponding to slot that 0 == tennat_id actully records the number of
// consecutive failures of each thread, which is used to control cpu usage
int ObLogReplayEngine::get_throttle_end_time_(
    const uint64_t tenant_id, const bool create_if_not_exist_in_map, ObThrottleEndTime*& throttle_end_time)
{
  int ret = OB_SUCCESS;
  const uint64_t thread_idx = get_thread_idx();
  const uint64_t thread_cnt = get_thread_cnt();
  if (OB_UNLIKELY(is_virtual_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    REPLAY_LOG(ERROR, "invalid arguments: pkey is invalid", K(tenant_id), K(thread_idx), KR(ret));
  } else {
    int64_t array_idx = INT64_MAX;
    if (tenant_id <= MAX_CACHE_TENANT_ID) {
      if (0 == tenant_id) {
        // reserved for record fail count
        array_idx = thread_idx;
      } else if (OB_SYS_TENANT_ID == tenant_id) {
        // for sys tenant
        array_idx = thread_cnt + thread_idx;
      } else if (OB_UNLIKELY(tenant_id <= OB_MAX_RESERVED_TENANT_ID)) {
        ret = OB_ERR_UNEXPECTED;
        REPLAY_LOG(ERROR, "invalid tenant_id", K(tenant_id), K(thread_idx), K(thread_cnt), KR(ret));
      } else {
        // for user tenant
        array_idx = (tenant_id - OB_MAX_RESERVED_TENANT_ID + 1) * thread_cnt + thread_idx;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (array_idx < 0) {
      ret = OB_ERR_UNEXPECTED;
      REPLAY_LOG(ERROR,
          "invalid array_idx",
          K(array_idx),
          K(tenant_id),
          K(thread_idx),
          K(thread_cnt),
          K(OB_MAX_RESERVED_TENANT_ID),
          K(ret));
    } else if (array_idx < REPLAY_CONTROL_ARRAY_SIZE) {
      // get_ from array
      throttle_end_time = &(replay_control_array_[array_idx]);
    } else {
      // get from map
      ObTenantThreadKey key(tenant_id, thread_idx);
      if (OB_FAIL(replay_control_map_.get(key, throttle_end_time)) && OB_ENTRY_NOT_EXIST != ret) {
        REPLAY_LOG(ERROR, "failed to get throttle_end_time", K(tenant_id), K(thread_idx), K(ret));
      } else if (OB_ENTRY_NOT_EXIST == ret && create_if_not_exist_in_map) {
        ret = OB_SUCCESS;  // overwrite ret
        char* buf = NULL;
        if (NULL == (buf = (char*)throttle_ts_allocator_.alloc())) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          REPLAY_LOG(WARN, "failed to alloc throttle_ts_allocator_", K(tenant_id), K(thread_idx), K(ret));
        } else {
          throttle_end_time = new (buf) ObThrottleEndTime();
          if (OB_FAIL(replay_control_map_.insert(key, throttle_end_time))) {
            REPLAY_LOG(ERROR, "failed to insert into replay_control_map_", K(key), K(ret));
            throttle_end_time = NULL;
            throttle_ts_allocator_.free(buf);
            buf = NULL;
          }
        }
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

}  // namespace replayengine
}  // namespace oceanbase
