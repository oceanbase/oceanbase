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

#include "ob_log_callback_engine.h"
#include "lib/objectpool/ob_resource_pool.h"
#include "share/ob_thread_mgr.h"
#include "ob_log_callback_task.h"
#include "ob_log_define.h"

namespace oceanbase {
using namespace common;
namespace clog {

int ObLogCallbackEngine::init(int clog_tg_id, int sp_tg_id)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    CLOG_LOG(WARN, "ObLogCallbackEngine init twice");
    ret = OB_INIT_TWICE;
  } else if (clog_tg_id < 0 || sp_tg_id < 0) {
    CLOG_LOG(WARN, "invalid argument", K(clog_tg_id), K(sp_tg_id));
    ret = OB_INVALID_ARGUMENT;
  } else {
    clog_tg_id_ = clog_tg_id;
    sp_tg_id_ = sp_tg_id;
    is_inited_ = true;
  }
  if (OB_SUCCESS != ret && OB_INIT_TWICE != ret) {
    destroy();
  }
  return ret;
}

void ObLogCallbackEngine::destroy()
{
  clog_tg_id_ = -1;
  sp_tg_id_ = -1;
  is_inited_ = false;
  return;
}

int ObLogCallbackEngine::handle_callback(ObICallback* callback)
{
  int ret = OB_SUCCESS;
  ObBatchCallbackTask* batch_callback_task = NULL;

  if (!is_inited_) {
    CLOG_LOG(ERROR, "ObLogCallbackEngine is not inited");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(callback)) {
    CLOG_LOG(WARN, "invalid argument", KP(callback));
    ret = OB_INVALID_ARGUMENT;
  } else if (NULL == (batch_callback_task = op_reclaim_alloc(ObBatchCallbackTask))) {
    CLOG_LOG(ERROR, "alloc failed!");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    if (OB_FAIL(batch_callback_task->init(callback))) {
      CLOG_LOG(WARN, "ObBatchCallbackTask init error", K(ret));
    } else {
      batch_callback_task->before_push_cb_ts_ = ObTimeUtility::current_time();
      do {
        ret = TG_PUSH_TASK(clog_tg_id_, batch_callback_task);
        if (OB_SUCC(ret)) {
          // do nothing
        } else if (OB_EAGAIN == ret) {
          if (REACH_TIME_INTERVAL(1000 * 1000)) {
            CLOG_LOG(WARN, "worker_thread_pool is full, try again", KP(callback));
          }
          usleep(10);
        } else {
          CLOG_LOG(WARN, "worker_thread_pool push error", K(ret), KP(callback));
        }
      } while (OB_EAGAIN == ret);
      if (REACH_TIME_INTERVAL(1000 * 1000)) {
        int64_t queue_num = TG_GET_QUEUE_NUM(clog_tg_id_);
        CLOG_LOG(INFO, "callback queue task number", "clog", queue_num, K(ret));
      }
    }
    if (OB_FAIL(ret)) {
      op_reclaim_free(batch_callback_task);
      batch_callback_task = NULL;
    }
  }

  return ret;
}

int ObLogCallbackEngine::submit_member_change_success_cb_task(const common::ObPartitionKey& partition_key,
    const clog::ObLogType log_type, const uint64_t ms_log_id, const int64_t mc_timestamp, const int64_t replica_num,
    const common::ObMemberList& prev_member_list, const common::ObMemberList& curr_member_list,
    const common::ObProposalID& ms_proposal_id)
{
  int ret = OB_SUCCESS;
  ObMemberChangeCallbackTask* member_change_callback_task = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogCallbackEngine is not inited", K(ret), K(partition_key));
  } else if (!partition_key.is_valid() || OB_INVALID_ID == ms_log_id || mc_timestamp <= 0 || replica_num <= 0 ||
             prev_member_list.get_member_number() < 0 || curr_member_list.get_member_number() < 0) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN,
        "invalid argument",
        K(ret),
        K(partition_key),
        K(ms_log_id),
        K(mc_timestamp),
        K(replica_num),
        K(prev_member_list),
        K(curr_member_list));
  } else if (NULL == (member_change_callback_task = op_reclaim_alloc(ObMemberChangeCallbackTask))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    CLOG_LOG(WARN, "alloc failed!", K(ret), K(partition_key));
  } else {
    if (OB_FAIL(member_change_callback_task->init(CLOG_MEMBER_CHANGE_SUCCESS_CB,
            partition_key,
            log_type,
            ms_log_id,
            mc_timestamp,
            replica_num,
            prev_member_list,
            curr_member_list,
            ms_proposal_id))) {
    } else {
      do {
        ret = TG_PUSH_TASK(sp_tg_id_, member_change_callback_task);
        if (OB_SUCC(ret)) {
          if (REACH_TIME_INTERVAL(1000 * 1000)) {
            CLOG_LOG(INFO,
                "sp_thread_pool push task success",
                K(member_change_callback_task),
                "task num",
                TG_GET_QUEUE_NUM(sp_tg_id_));
          }
        } else if (OB_EAGAIN == ret) {
          if (REACH_TIME_INTERVAL(1000 * 1000)) {
            CLOG_LOG(WARN,
                "sp_thread_pool is full, try again",
                K(partition_key),
                K(mc_timestamp),
                K(ms_log_id),
                K(prev_member_list),
                K(curr_member_list));
          }
          usleep(10);
        } else {
          CLOG_LOG(WARN,
              "sp_thread_pool push error",
              K(ret),
              K(partition_key),
              K(mc_timestamp),
              K(prev_member_list),
              K(curr_member_list));
        }
      } while (OB_EAGAIN == ret);
    }
    if (OB_FAIL(ret)) {
      op_reclaim_free(member_change_callback_task);
      member_change_callback_task = NULL;
    }
  }

  return ret;
}

int ObLogCallbackEngine::submit_pop_task(const common::ObPartitionKey& partition_key)
{
  int ret = OB_SUCCESS;

  ObLogCallbackTask* pop_task = NULL;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogCallbackEngine is not inited", K(ret));
  } else if (!partition_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), K(partition_key));
  } else if (NULL == (pop_task = op_reclaim_alloc(ObLogCallbackTask))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    CLOG_LOG(WARN, "op_reclaim_alloc failed", K(ret), K(partition_key));
  } else if (OB_FAIL(pop_task->init(CLOG_POP_TASK_CB, partition_key))) {
    CLOG_LOG(WARN, "pop_task init failed", K(ret), K(partition_key));
  } else if (OB_FAIL(TG_PUSH_TASK(clog_tg_id_, pop_task))) {
    CLOG_LOG(WARN, "cb_thread_pool_ push failed", K(ret), K(partition_key));
  }
  if (OB_SUCCESS != ret && NULL != pop_task) {
    op_reclaim_free(pop_task);
    pop_task = NULL;
  }
  return ret;
}
}  // namespace clog
}  // namespace oceanbase
