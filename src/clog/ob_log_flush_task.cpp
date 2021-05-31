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

#include "ob_log_flush_task.h"
#include "common/ob_clock_generator.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/statistic_event/ob_stat_event.h"
#include "storage/ob_partition_service.h"
#include "ob_i_log_engine.h"
#include "ob_log_callback_engine.h"
#include "ob_partition_log_service.h"
#include "share/allocator/ob_tenant_mutil_allocator.h"

namespace oceanbase {
namespace clog {
using namespace common;
ObLogFlushTask::ObLogFlushTask()
{
  reset();
  EVENT_INC(CLOG_FLUSH_TASK_GENERATE_COUNT);
}

ObLogFlushTask::~ObLogFlushTask()
{
  reset();
  EVENT_INC(CLOG_FLUSH_TASK_RELEASE_COUNT);
}

int ObLogFlushTask::init(const ObLogType log_type, const uint64_t log_id, const common::ObProposalID proposal_id,
    const common::ObPartitionKey& partition_key, storage::ObPartitionService* partition_service,
    const common::ObAddr& leader, const int64_t cluster_id, ObILogEngine* log_engine, const int64_t submit_timestamp,
    const int64_t pls_epoch)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    CLOG_LOG(WARN, "ObLogFlushTask init twice");
    ret = OB_INIT_TWICE;
  } else if (!proposal_id.is_valid() || !partition_key.is_valid() || OB_ISNULL(partition_service) ||
             OB_ISNULL(log_engine)) {
    // If log_type is ObPreparedLog, submit_timestamp is OB_INVALID_TIMESTAMP
    CLOG_LOG(WARN,
        "invalid argument",
        K(proposal_id),
        K(partition_key),
        K(partition_service),
        K(log_engine),
        K(submit_timestamp),
        K(pls_epoch));
    ret = OB_INVALID_ARGUMENT;
  } else {
    log_type_ = log_type;
    log_id_ = log_id;
    proposal_id_ = proposal_id;
    partition_key_ = partition_key;
    partition_service_ = partition_service;
    leader_ = leader;
    cluster_id_ = cluster_id;
    log_engine_ = log_engine;
    submit_timestamp_ = submit_timestamp;
    pls_epoch_ = pls_epoch;
    is_inited_ = true;
  }
  return ret;
}

void ObLogFlushTask::reset()
{
  ObDiskBufferTask::reset();
  log_type_ = OB_LOG_UNKNOWN;
  log_id_ = OB_INVALID_ID;
  proposal_id_.reset();
  partition_key_.reset();
  partition_service_ = NULL;
  leader_.reset();
  cluster_id_ = OB_INVALID_CLUSTER_ID;
  log_engine_ = NULL;
  submit_timestamp_ = OB_INVALID_TIMESTAMP;
  pls_epoch_ = OB_INVALID_TIMESTAMP;
  is_inited_ = false;
}

int ObLogFlushTask::st_after_consume(const int handle_err)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_SUCCESS != handle_err) {
    // FIXME() : will solve it later
    CLOG_LOG(ERROR,
        "ob_log_flush_task write disk error, the server will kill self",
        K_(partition_key),
        K(handle_err),
        K_(log_type),
        K_(log_id),
        K_(proposal_id));
    on_fatal_error(handle_err);
  } else {
    if (is_prepared_log(log_type_)) {
      // do nothing
    } else {
      log_engine_->update_clog_info(partition_key_, log_id_, submit_timestamp_);
    }
  }
  return ret;
}

int ObLogFlushTask::after_consume(const int handle_err, const void* arg, const int64_t before_push_cb_ts)
{
  static int64_t flush_cb_cnt = 0;
  static int64_t flush_cb_cost_time = 0;
  const int64_t before_flush_cb = ObClockGenerator::getClock();
  int ret = OB_SUCCESS;
  ObLogCursor log_cursor;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_SUCCESS != handle_err) {
    // FIXME() : will solve it later
    CLOG_LOG(ERROR,
        "ObLogFlushTask write disk error, the server will kill self",
        K_(partition_key),
        K(handle_err),
        K_(log_type),
        K_(log_id),
        K_(proposal_id));
    on_fatal_error(handle_err);
  } else if (OB_FAIL(log_cursor.deep_copy(*(static_cast<const ObLogCursor*>(arg))))) {
    CLOG_LOG(WARN, "log_cursor deep_copy failed", K(ret));
  } else {
    EVENT_INC(CLOG_TASK_CB_COUNT);
    EVENT_ADD(CLOG_CB_QUEUE_TIME, ObTimeUtility::current_time() - before_push_cb_ts);
    log_cursor.offset_ += static_cast<offset_t>(offset_);
    log_cursor.size_ = (int32_t)len_;
    storage::ObIPartitionGroupGuard guard;
    ObIPartitionLogService* log_service = NULL;
    int64_t curr_pls_epoch = OB_INVALID_TIMESTAMP;
    if (OB_FAIL(partition_service_->get_partition(partition_key_, guard)) || NULL == guard.get_partition_group() ||
        NULL == (log_service = guard.get_partition_group()->get_log_service())) {
      CLOG_LOG(WARN, "invalid partition", K(partition_service_), K(partition_key_));
      ret = OB_PARTITION_NOT_EXIST;
    } else if (!(guard.get_partition_group()->is_valid())) {
      ret = OB_INVALID_PARTITION;
      CLOG_LOG(WARN, "partition is invalid", K_(partition_key), K(ret));
    } else if (OB_FAIL(log_service->get_pls_epoch(curr_pls_epoch))) {
      CLOG_LOG(WARN, "get_pls_epoch failed", K(partition_key_), K(ret));
    } else {
      // The different pls_epoch means the flush_cb is stale
      if (curr_pls_epoch != pls_epoch_) {
        CLOG_LOG(INFO, "filter stale log", K(partition_key_), K(pls_epoch_), K(curr_pls_epoch));
      } else {
        const int64_t after_consume_timestamp = ObClockGenerator::getClock();
        ObLogFlushCbArg cb_arg(log_type_,
            log_id_,
            submit_timestamp_,
            proposal_id_,
            leader_,
            cluster_id_,
            log_cursor,
            after_consume_timestamp);
        if (OB_SUCCESS != (ret = log_service->flush_cb(cb_arg))) {
          CLOG_LOG(ERROR, "flush_cb failed", K_(partition_key), K(ret), K(cb_arg));
        }
        const int64_t after_flush_cb = ObClockGenerator::getClock();
        ATOMIC_INC(&flush_cb_cnt);
        ATOMIC_AAF(&flush_cb_cost_time, after_flush_cb - before_flush_cb);
        if (REACH_TIME_INTERVAL(1000 * 1000)) {
          CLOG_LOG(INFO,
              "clog flush cb cost time",
              K(flush_cb_cnt),
              K(flush_cb_cost_time),
              "avg time",
              flush_cb_cost_time / (flush_cb_cnt + 1));
          ATOMIC_STORE(&flush_cb_cnt, 0);
          ATOMIC_STORE(&flush_cb_cost_time, 0);
        }
      }
    }
  }
  ob_slice_free_log_flush_task(this);
  return ret;
}
}  // namespace clog
}  // namespace oceanbase
