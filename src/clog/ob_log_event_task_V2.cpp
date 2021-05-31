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

#include "ob_log_event_task_V2.h"
#include "ob_log_event_scheduler.h"
#include "ob_partition_log_service.h"
#include "common/ob_clock_generator.h"
#include "share/allocator/ob_tenant_mutil_allocator.h"
#include "storage/ob_partition_service.h"

namespace oceanbase {
namespace clog {
ObLogStateEventTaskV2::ObLogStateEventTaskV2()
    : is_inited_(false), partition_key_(), event_scheduler_(NULL), partition_service_(NULL), expected_ts_(0)
{}

ObLogStateEventTaskV2::~ObLogStateEventTaskV2()
{
  destroy();
}

int ObLogStateEventTaskV2::init(const common::ObPartitionKey& partition_key, ObLogEventScheduler* event_scheduler,
    storage::ObPartitionService* partition_service)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(ERROR, "ObLogStateEventTaskV2 init twice", K(ret));
  } else if (!partition_key.is_valid() || OB_ISNULL(event_scheduler) || OB_ISNULL(partition_service)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "invalid argument", K(ret), K(partition_key), K(event_scheduler), K(partition_service));
  } else {
    is_inited_ = true;
    partition_key_ = partition_key;
    event_scheduler_ = event_scheduler;
    partition_service_ = partition_service;
    expected_ts_ = 0;
  }
  return ret;
}

void ObLogStateEventTaskV2::destroy()
{
  is_inited_ = false;
  partition_key_.reset();
  event_scheduler_ = NULL;
  partition_service_ = NULL;
  expected_ts_ = 0;
}

uint64_t ObLogStateEventTaskV2::hash() const
{
  return partition_key_.hash();
}

void ObLogStateEventTaskV2::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "ObLogStateEventTaskV2 is not inited", K(partition_key_));
  } else {
    bool need_next_task = false;
    bool need_retry = false;
    const int64_t current_ts = common::ObClockGenerator::getClock();
    storage::ObIPartitionGroupGuard guard;
    ObIPartitionLogService* log_service = NULL;

    if (current_ts - expected_ts_ > MAX_EVENT_EXECUTE_DEPLAY_TS) {
      CLOG_LOG(WARN, "run time out of range", K(partition_key_), K(current_ts), "delta", current_ts - expected_ts_);
    }

    if (OB_FAIL(partition_service_->get_partition(partition_key_, guard))) {
      CLOG_LOG(INFO, "get_partition failed", K(ret), K(partition_key_));
    } else if (NULL == guard.get_partition_group()) {
      CLOG_LOG(INFO, "partition not exist", K(ret), K(partition_key_));
      ret = OB_PARTITION_NOT_EXIST;
    } else if (NULL == (log_service = guard.get_partition_group()->get_log_service())) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(ERROR, "get partition log service error", K(ret), K(partition_key_));
    } else {
      log_service->reset_has_event_task();
      if (OB_SUCCESS != (ret = log_service->switch_state(need_retry)) && OB_EAGAIN != ret) {
        need_next_task = true;
        CLOG_LOG(WARN, "switch state failed", K(ret), K(partition_key_));
      } else if (OB_EAGAIN == ret || need_retry) {
        need_next_task = true;
      } else {
        // do nothing
      }

      if (need_next_task) {
        need_next_task = log_service->need_add_event_task();
      }
    }

    if (need_next_task) {
      if (OB_FAIL(event_scheduler_->add_state_change_delay_event(this))) {
        CLOG_LOG(WARN, "event_scheduler_ add task failed", K(ret), K(partition_key_));
      }
    } else {
      common::ob_slice_free_log_event_task(this);
    }
  }
  return;
}

int ObLogStateEventTaskV2::set_expected_ts(const int64_t delay)
{
  int ret = OB_SUCCESS;
  if (delay < 0) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    expected_ts_ = common::ObClockGenerator::getClock() + delay;
  }
  return ret;
}

}  // namespace clog
}  // namespace oceanbase
