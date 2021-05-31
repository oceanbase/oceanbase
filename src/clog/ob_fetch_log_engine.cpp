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

#include "ob_fetch_log_engine.h"
#include "ob_partition_log_service.h"
#include "share/allocator/ob_tenant_mutil_allocator.h"
#include "share/ob_thread_mgr.h"
#include "storage/ob_partition_service.h"

namespace oceanbase {
using namespace common;
namespace clog {

int ObFetchLogTask::init(const common::ObPartitionKey& partition_key, const common::ObAddr& server,
    const int64_t cluster_id, const uint64_t start_log_id, const uint64_t end_log_id, const ObFetchLogType fetch_type,
    const common::ObProposalID& proposal_id, const int64_t network_limit)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    CLOG_LOG(WARN, "ObFetchLogTask init twice");
    ret = OB_INIT_TWICE;
  } else if (!partition_key.is_valid() || !server.is_valid() || fetch_type < OB_FETCH_LOG_UNKNOWN ||
             fetch_type >= OB_FETCH_LOG_TYPE_MAX || network_limit < 0) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN,
        "invalid argument",
        K(partition_key),
        K(server),
        K(start_log_id),
        K(end_log_id),
        K(fetch_type),
        K(proposal_id),
        K(network_limit));
  } else {
    timestamp_ = ObTimeUtility::current_time();
    partition_key_ = partition_key;
    server_ = server;
    cluster_id_ = cluster_id;
    start_log_id_ = start_log_id;
    end_log_id_ = end_log_id;
    fetch_type_ = fetch_type;
    proposal_id_ = proposal_id;
    network_limit_ = network_limit;
  }
  return ret;
}

void ObFetchLogTask::reset()
{
  timestamp_ = common::OB_INVALID_TIMESTAMP;
  partition_key_.reset();
  server_.reset();
  cluster_id_ = common::OB_INVALID_CLUSTER_ID;
  start_log_id_ = common::OB_INVALID_ID;
  end_log_id_ = common::OB_INVALID_ID;
  fetch_type_ = OB_FETCH_LOG_UNKNOWN;
  proposal_id_.reset();
  network_limit_ = 0;
}

int ObFetchLogEngine::init(storage::ObPartitionService* partition_service)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    CLOG_LOG(WARN, "ObFetchLogEngine init twice");
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(partition_service)) {
    CLOG_LOG(WARN, "invalid argument", KP(partition_service));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(TG_SET_HANDLER_AND_START(lib::TGDefIDs::FetchLogEngine, *this))) {
    CLOG_LOG(WARN, "ObSimpleThreadPool::init failed", K(ret));
  } else {
    partition_service_ = partition_service;
    is_inited_ = true;
  }
  if ((OB_FAIL(ret)) && (OB_INIT_TWICE != ret)) {
    CLOG_LOG(WARN, "ObFetchLogEngine init failed", K(ret), KP(partition_service));
    destroy();
  }

  return ret;
}

int ObFetchLogEngine::destroy()
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    CLOG_LOG(WARN, "ObFetchLogEngine not init");
    ret = OB_NOT_INIT;
  } else {
    TG_DESTROY(lib::TGDefIDs::FetchLogEngine);
    partition_service_ = NULL;
  }

  return ret;
}

int ObFetchLogEngine::submit_fetch_log_task(ObFetchLogTask* fetch_log_task)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    CLOG_LOG(WARN, "ObFetchLogEngine not init");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(fetch_log_task)) {
    CLOG_LOG(WARN, "invalid argument", KP(fetch_log_task));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(TG_PUSH_TASK(lib::TGDefIDs::FetchLogEngine, fetch_log_task))) {
    CLOG_LOG(WARN, "push failed", K(ret), "fetch_log_task", *fetch_log_task);
  } else {
    CLOG_LOG(INFO, "submit_fetch_log_task success");
  }

  return ret;
}

void ObFetchLogEngine::handle(void* task)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    CLOG_LOG(WARN, "ObFetchLogEngine not init");
  } else if (OB_ISNULL(task)) {
    CLOG_LOG(WARN, "invalid argument", KP(task));
  } else {
    int64_t handle_start_time = ObTimeUtility::current_time();
    ObFetchLogTask* fetch_log_task = static_cast<ObFetchLogTask*>(task);
    if (OB_ISNULL(fetch_log_task)) {
      CLOG_LOG(ERROR, "fetch_log_task is NULL");
    } else if (!is_task_queue_timeout_(fetch_log_task)) {
      storage::ObIPartitionGroupGuard guard;
      ObIPartitionLogService* log_service = NULL;
      if (OB_FAIL(partition_service_->get_partition(fetch_log_task->get_partition_key(), guard)) ||
          NULL == guard.get_partition_group() ||
          NULL == (log_service = guard.get_partition_group()->get_log_service())) {
        CLOG_LOG(WARN, "invalid partition", "partition_key", fetch_log_task->get_partition_key());
        ret = OB_PARTITION_NOT_EXIST;
      } else if (!(guard.get_partition_group()->is_valid())) {
        ret = OB_INVALID_PARTITION;
        CLOG_LOG(WARN, "partition is invalid", "partition_key", fetch_log_task->get_partition_key(), K(ret));
      } else if (OB_FAIL(log_service->async_get_log(fetch_log_task->get_server(),
                     fetch_log_task->get_cluster_id(),
                     fetch_log_task->get_start_log_id(),
                     fetch_log_task->get_end_log_id(),
                     fetch_log_task->get_fetch_log_type(),
                     fetch_log_task->get_proposal_id(),
                     fetch_log_task->get_network_limit()))) {
        CLOG_LOG(WARN, "async get log failed", K(ret), "fetch_log_task", *fetch_log_task);
      } else {
        // do nothing
      }
    } else {
      ret = OB_WAITQUEUE_TIMEOUT;
      CLOG_LOG(WARN, "handle fetch log task fail, task wait timeout", K(ret), "fetch_log_task", *fetch_log_task);
    }
    int64_t handle_finish_time = ObTimeUtility::current_time();
    int64_t handle_cost_time = handle_finish_time - handle_start_time;
    if (REACH_TIME_INTERVAL(100 * 1000)) {
      CLOG_LOG(INFO,
          "handle fetch log task",
          K(ret),
          "handle_cost_time",
          handle_cost_time,
          "fetch_log_task",
          *fetch_log_task);
    } else if (handle_cost_time > 200 * 1000) {
      CLOG_LOG(INFO,
          "handle fetch log task cost too much time",
          K(ret),
          "handle_cost_time",
          handle_cost_time,
          "fetch_log_task",
          *fetch_log_task);
    }
    if (NULL != fetch_log_task) {
      common::ob_slice_free_fetch_log_task(fetch_log_task);
      fetch_log_task = NULL;
    }
  }
}

bool ObFetchLogEngine::is_task_queue_timeout_(ObFetchLogTask* task) const
{
  bool bool_ret = false;

  if (!is_inited_) {
    CLOG_LOG(WARN, "ObFetchLogEngine not init");
  } else if (OB_ISNULL(task)) {
    CLOG_LOG(WARN, "invalid argument", KP(task));
  } else {
    bool_ret = (ObTimeUtility::current_time() - task->get_timestamp()) > CLOG_FETCH_LOG_TASK_QUEUE_TIMEOUT;
  }

  return bool_ret;
}

}  // namespace clog
}  // namespace oceanbase
