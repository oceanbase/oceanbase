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

#include "ob_log_callback_handler.h"
#include "lib/objectpool/ob_concurrency_objpool.h"
#include "lib/stat/ob_diagnose_info.h"
#include "share/ob_i_ps_cb.h"
#include "storage/ob_i_partition_group.h"
#include "storage/ob_partition_service.h"
#include "ob_log_callback_task.h"
#include "ob_partition_log_service.h"

namespace oceanbase {
using namespace common;
namespace clog {
int ObLogCallbackHandler::init(storage::ObPartitionService* partition_service, ObILogCallbackEngine* callback_engine)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    CLOG_LOG(WARN, "ObLogCallbackHandler init twice");
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(partition_service) || OB_ISNULL(callback_engine)) {
    CLOG_LOG(WARN, "invalid argument", KP(partition_service), KP(callback_engine));
    ret = OB_INVALID_ARGUMENT;
  } else {
    partition_service_ = partition_service;
    callback_engine_ = callback_engine;
    is_inited_ = true;
  }
  return ret;
}

void ObLogCallbackHandler::handle(void* task)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    CLOG_LOG(WARN, "ObLogCallbackHandler not init");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(task)) {
    CLOG_LOG(WARN, "invalid argument", KP(task));
  } else {
    ObLogCallbackTask* cb_task = static_cast<ObLogCallbackTask*>(task);
    const CallbackTaskType type = cb_task->get_cb_task_type();

    switch (type) {
      case CLOG_BATCH_CB: {
        ObBatchCallbackTask* batch_cb = static_cast<ObBatchCallbackTask*>(task);
        EVENT_INC(CLOG_BATCH_CB_COUNT);
        EVENT_ADD(CLOG_BATCH_CB_QUEUE_TIME, ObTimeUtility::current_time() - batch_cb->before_push_cb_ts_);
        batch_cb->handle_callback();
        op_reclaim_free(batch_cb);
        batch_cb = NULL;
        break;
      }
      case CLOG_MEMBER_CHANGE_SUCCESS_CB: {
        ObMemberChangeCallbackTask* mc_task = static_cast<ObMemberChangeCallbackTask*>(task);
        if (OB_FAIL(partition_service_->on_member_change_success(mc_task->get_partition_key(),
                mc_task->get_log_type(),
                mc_task->get_ms_log_id(),
                mc_task->get_mc_timestamp(),
                mc_task->get_replica_num(),
                mc_task->get_prev_member_list(),
                mc_task->get_curr_member_list(),
                mc_task->get_ms_proposal_id()))) {
          CLOG_LOG(WARN,
              "on_member_change_success cb failed",
              K(ret),
              "partition_key",
              mc_task->get_partition_key(),
              "ms_log_id",
              mc_task->get_ms_log_id(),
              "mc_timestamp",
              mc_task->get_mc_timestamp(),
              "replica_num",
              mc_task->get_replica_num(),
              "prev_member_list",
              mc_task->get_prev_member_list(),
              "curr_member_list",
              mc_task->get_curr_member_list());
        }
        CLOG_LOG(INFO,
            "on_member_change_success cb finished",
            K(ret),
            "partition_key",
            mc_task->get_partition_key(),
            "ms_log_id",
            mc_task->get_ms_log_id(),
            "mc_timestamp",
            mc_task->get_mc_timestamp(),
            "prev_member_list",
            mc_task->get_prev_member_list(),
            "curr_member_list",
            mc_task->get_curr_member_list());
        op_reclaim_free(mc_task);
        mc_task = NULL;
        break;
      }
      case CLOG_POP_TASK_CB: {
        const common::ObPartitionKey& partition_key = cb_task->get_partition_key();
        (void)handle_pop_task_(partition_key);
        op_reclaim_free(cb_task);
        cb_task = NULL;
        break;
      }
      default:
        CLOG_LOG(WARN, "invalid callback type", K(type));
        ret = OB_ERR_UNEXPECTED;
        break;
    }
  }
}

void ObLogCallbackHandler::handle_pop_task_(const common::ObPartitionKey& partition_key)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  storage::ObIPartitionGroupGuard guard;
  ObIPartitionLogService* log_service = NULL;
  const bool need_async_replay = false;
  bool is_replayed = false;
  bool is_replay_failed = false;
  if (OB_FAIL(partition_service_->get_partition(partition_key, guard)) || NULL == guard.get_partition_group() ||
      NULL == (log_service = guard.get_partition_group()->get_log_service())) {
    ret = OB_PARTITION_NOT_EXIST;
    CLOG_LOG(WARN, "invalid partition", K(ret), K(partition_key));
  } else if (!(guard.get_partition_group()->is_valid())) {
    ret = OB_INVALID_PARTITION;
    CLOG_LOG(WARN, "partition is invalid", K(ret), K(partition_key));
  } else {
    ret = log_service->try_replay(need_async_replay, is_replayed, is_replay_failed);
    if (OB_CLOG_SLIDE_TIMEOUT != ret) {
      log_service->reset_has_pop_task();
    } else if (OB_SUCCESS != (tmp_ret = callback_engine_->submit_pop_task(partition_key))) {
      CLOG_LOG(WARN, "submit_pop_task failed", K(tmp_ret), K(partition_key));
      log_service->reset_has_pop_task();
    } else {
      // do nothing
    }
  }
}
}  // namespace clog
}  // namespace oceanbase
