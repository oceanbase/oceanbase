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

#include "ob_clog_adapter.h"
#include "lib/stat/ob_session_stat.h"
#include "lib/utility/utility.h"
#include "lib/utility/ob_tracepoint.h"
#include "share/ob_thread_mgr.h"
#include "clog/ob_clog_mgr.h"
#include "storage/ob_partition_service.h"
#include "storage/ob_i_partition_group.h"
#include "ob_trans_part_ctx.h"

namespace oceanbase {

using namespace storage;
using namespace clog;

namespace transaction {
ObClogAdapter::ObClogAdapter() : is_inited_(false), is_running_(false), partition_service_(NULL), tg_id_(-1)
{
  reset_statistics();
}

int ObClogAdapter::init(ObPartitionService* partition_service)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    TRANS_LOG(WARN, "ObClogAdapter inited twice");
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(partition_service)) {
    TRANS_LOG(WARN, "invalid argument", KP(partition_service));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(TG_CREATE(lib::TGDefIDs::CLogAdapter, tg_id_))) {
    TRANS_LOG(WARN, "create tg failed", K(ret));
  } else if (OB_FAIL(TG_SET_HANDLER_AND_START(tg_id_, *this))) {
    TRANS_LOG(WARN, "thread pool init error", K(ret));
  } else if (OB_FAIL(allocator_.init(INT64_MAX, TOTAL_TASK / 10, common::OB_MALLOC_NORMAL_BLOCK_SIZE))) {
    TRANS_LOG(WARN, "ObConcurrentFIFOAllocator init error", KR(ret));
  } else {
    allocator_.set_label(ObModIds::OB_TRANS_CLOG_BUF);
    partition_service_ = partition_service;
    is_inited_ = true;
    TRANS_LOG(INFO, "ObClogAdapter inited success");
  }

  return ret;
}

int ObClogAdapter::start()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObClogAdapter is not inited");
    ret = OB_NOT_INIT;
  } else if (is_running_) {
    TRANS_LOG(WARN, "ObClogAdapter is already running");
    ret = OB_ERR_UNEXPECTED;
  } else {
    is_running_ = true;
    TRANS_LOG(INFO, "ObClogAdapter start success");
  }

  return ret;
}

void ObClogAdapter::stop()
{
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObClogAdapter is not inited");
  } else if (!is_running_) {
    TRANS_LOG(WARN, "ObClogAdapter already has been stopped");
  } else {
    TG_STOP(tg_id_);
    is_running_ = false;
    TRANS_LOG(INFO, "ObClogAdapter stop success");
  }
}

void ObClogAdapter::wait()
{
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObClogAdapter is not inited");
  } else if (is_running_) {
    TRANS_LOG(WARN, "ObClogAdapter is running");
  } else {
    TRANS_LOG(INFO, "ObClogAdapter wait success");
  }
}

void ObClogAdapter::destroy()
{
  if (is_inited_) {
    if (is_running_) {
      stop();
      wait();
    }
    allocator_.destroy();
    is_inited_ = false;
    TRANS_LOG(INFO, "ObClogAdapter destroyed");
  }
}

void ObClogAdapter::handle(void* task)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(task)) {
    TRANS_LOG(ERROR, "task is null", KP(task));
  } else {
    ObTransTask* tmp_task = static_cast<ObTransTask*>(task);
    if (tmp_task->get_task_type() == ObTransRetryTaskType::ALLOC_LOG_ID) {
      AllocLogIdTask* alloc_log_id_task = static_cast<AllocLogIdTask*>(task);
      ObPartTransCtx* part_ctx = static_cast<ObPartTransCtx*>(alloc_log_id_task->get_trans_ctx());
      // part_ctx may already have been released when printing log, do not use reference!!!
      const ObTransID trans_id = part_ctx->get_trans_id();
      if (OB_ISNULL(part_ctx)) {
        TRANS_LOG(WARN, "part ctx is null, unexpected error", KP(part_ctx));
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(part_ctx->submit_log(alloc_log_id_task->get_log_type()))) {
        if (EXECUTE_COUNT_PER_SEC(16)) {
          TRANS_LOG(WARN, "submit log error", KR(ret), "log_type", alloc_log_id_task->get_log_type(), K(trans_id));
        }
        AllocLogIdTaskFactory::release(alloc_log_id_task);
        alloc_log_id_task = NULL;
        task = NULL;
      } else {
        if (REACH_TIME_INTERVAL(1000 * 1000)) {
          TRANS_LOG(INFO,
              "handle submit alloc log id task",
              KP(task),
              "log_type",
              alloc_log_id_task->get_log_type(),
              K(trans_id));
        }
        AllocLogIdTaskFactory::release(alloc_log_id_task);
        alloc_log_id_task = NULL;
        task = NULL;
      }
    } else if (tmp_task->get_task_type() == ObTransRetryTaskType::SUBMIT_LOG) {
      SubmitLogTask* log_task = reinterpret_cast<SubmitLogTask*>(task);
      const ObPartitionKey& partition = log_task->get_partition();
      const ObVersion& version = log_task->get_version();
      char* clog_buf = log_task->get_clog_buf();
      const int64_t clog_size = log_task->get_clog_size();
      const bool with_need_update_version = log_task->with_need_update_version();
      const int64_t local_trans_version = log_task->get_local_trans_version();
      uint64_t cur_log_id = OB_INVALID_ID;
      int64_t cur_log_timestamp = OB_INVALID_TIMESTAMP;
      const bool with_base_ts = log_task->with_base_ts();
      const int64_t base_ts = log_task->get_base_ts();
      ObTransSubmitLogCb* cb = static_cast<ObTransSubmitLogCb*>(log_task->get_cb());
      ObTransCtx* ctx = NULL;
      if (OB_ISNULL(cb)) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "unexpected error, cb is NULL", KR(ret), KP(cb), K(log_task));
        SubmitLogTaskFactory::release(log_task);
        log_task = NULL;
        task = NULL;
      } else if (OB_ISNULL(ctx = cb->get_ctx())) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "unexpected error, ctx is NULL", KR(ret), KP(ctx), K(log_task));
      } else {
        log_task->process_begin();
        CtxLockGuard ctx_guard;
        ctx->get_ctx_guard(ctx_guard);
        if (with_need_update_version) {
          ret = submit_log_(
              partition, version, clog_buf, clog_size, local_trans_version, cb, NULL, cur_log_id, cur_log_timestamp);
        } else if (with_base_ts) {
          ret = submit_log_(partition, version, clog_buf, clog_size, base_ts, cb, NULL, cur_log_id, cur_log_timestamp);
        } else {
          ret = submit_log_(partition, version, clog_buf, clog_size, cb, NULL, cur_log_id, cur_log_timestamp);
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(cb->on_submit_log_success(with_need_update_version, cur_log_id, cur_log_timestamp))) {
            TRANS_LOG(WARN, "submit log success callback error", KR(ret), K(log_task));
          }
          SubmitLogTaskFactory::release(log_task);
          log_task = NULL;
          task = NULL;
        } else {
          if (EXECUTE_COUNT_PER_SEC(16)) {
            TRANS_LOG(WARN, "submit log error", KR(ret), K(log_task));
          }
          if (!need_retry(ret)) {
            if (OB_FAIL(cb->on_submit_log_fail(ret))) {
              TRANS_LOG(WARN, "submit log fail callback error", KR(ret), K(log_task));
            }
            SubmitLogTaskFactory::release(log_task);
            log_task = NULL;
            task = NULL;
          } else {
            if (OB_FAIL(TG_PUSH_TASK(tg_id_, task))) {
              TRANS_LOG(ERROR, "push submit log task failed", K(ret), K(log_task));
              if (OB_FAIL(cb->on_submit_log_fail(ret))) {
                TRANS_LOG(WARN, "submit log fail callback error", KR(ret), K(log_task));
              }
              SubmitLogTaskFactory::release(log_task);
              log_task = NULL;
              task = NULL;
            } else {
              if (REACH_TIME_INTERVAL(100 * 1000)) {
                TRANS_LOG(INFO, "push submit log task success", K(log_task));
              }
            }
          }
        }
      }
    } else if (tmp_task->get_task_type() == ObTransRetryTaskType::BACKFILL_NOP_LOG) {
      BackfillNopLogTask* backfill_nop_log_task = reinterpret_cast<BackfillNopLogTask*>(task);
      if (!backfill_nop_log_task->is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "backfill_nop_log_task invalid", KR(ret), "BackfillNopLogTask", *(backfill_nop_log_task));
      } else if (OB_FAIL(backfill_nop_log(
                     backfill_nop_log_task->get_partition(), NULL, backfill_nop_log_task->get_log_meta()))) {
        // fail to complement nop log, check whether it's possile to retry submit task
        if (OB_EAGAIN != ret || OB_FAIL(TG_PUSH_TASK(tg_id_, backfill_nop_log_task))) {
          TRANS_LOG(
              WARN, "backfill or push nop log task error", K(ret), "BackfillNopLogTask", *(backfill_nop_log_task));
          BackfillNopLogTaskFactory::release(backfill_nop_log_task);
          backfill_nop_log_task = NULL;
          task = NULL;
        }
      } else {
        // success
        BackfillNopLogTaskFactory::release(backfill_nop_log_task);
        backfill_nop_log_task = NULL;
        task = NULL;
      }
    } else if (tmp_task->get_task_type() == ObTransRetryTaskType::AGGRE_LOG_TASK) {
      AggreLogTask* aggre_log_task = reinterpret_cast<AggreLogTask*>(task);
      const ObPartitionKey& partition = aggre_log_task->get_partition();
      ObVersion version(2, 0);
      if (OB_FAIL(submit_aggre_log_(partition, version, NULL, *aggre_log_task->get_container()))) {
        TRANS_LOG(WARN, "submit aggre log error", K(ret), K(partition));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "ObTransRetryTaskType invalid", KR(ret), K(tmp_task));
    }
  }

  // print the number of tasks in the queue periodically
  if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
    int64_t queue_num = TG_GET_QUEUE_NUM(tg_id_);
    TRANS_LOG(INFO, "[statisic] clog adaptor task queue statisic : ", K(queue_num));
  }
}

void ObClogAdapter::reset_statistics()
{
  total_access_ = 0;
  error_count_ = 0;
}

void ObClogAdapter::statistics()
{
  if (REACH_TIME_INTERVAL(TRANS_ACCESS_STAT_INTERVAL)) {
    TRANS_LOG(INFO, "clog adapter access statistics", K_(total_access), K_(error_count));
    reset_statistics();
  }
}

int ObClogAdapter::get_status_impl_(const ObPartitionKey& partition, const bool need_safe, int& clog_status,
    int64_t& leader_epoch, ObTsWindows& changing_leader_windows)
{
  int ret = OB_SUCCESS;
  storage::ObIPartitionGroupGuard guard;
  ObIPartitionLogService* log_service = NULL;

  if (OB_FAIL(partition_service_->get_partition(partition, guard))) {
    TRANS_LOG(WARN, "get partition failed", K(partition), KR(ret));
  } else if (OB_ISNULL(guard.get_partition_group())) {
    TRANS_LOG(WARN, "partition not exist", K(partition));
    ret = OB_PARTITION_NOT_EXIST;
  } else if (OB_ISNULL(log_service = guard.get_partition_group()->get_log_service())) {
    TRANS_LOG(WARN, "get partiton log service error", K(partition));
    ret = OB_ERR_UNEXPECTED;
  } else if (need_safe) {
    clog_status = log_service->get_role_unlock(leader_epoch, changing_leader_windows);
  } else {
    clog_status = log_service->get_role_unsafe(leader_epoch, changing_leader_windows);
  }

  return ret;
}

int ObClogAdapter::get_status_unsafe(
    const ObPartitionKey& partition, int& clog_status, int64_t& leader_epoch, ObTsWindows& changing_leader_windows)
{
  int ret = OB_SUCCESS;
  const bool need_safe = false;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObClogAdapter not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObClogAdapter is not running");
    ret = OB_NOT_RUNNING;
  } else if (OB_UNLIKELY(!partition.is_valid())) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(get_status_impl_(partition, need_safe, clog_status, leader_epoch, changing_leader_windows))) {
    TRANS_LOG(WARN, "get status impl error", KR(ret), K(partition), K(need_safe));
  } else {
    // do nothing
  }

  return ret;
}

int ObClogAdapter::get_status(storage::ObIPartitionGroup* partition, const bool check_election, int& clog_status,
    int64_t& leader_epoch, ObTsWindows& changing_leader_windows)
{
  UNUSED(check_election);
  int ret = OB_SUCCESS;
  ObIPartitionLogService* log_service = NULL;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObClogAdapter not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObClogAdapter is not running");
    ret = OB_NOT_RUNNING;
  } else if (OB_ISNULL(partition)) {
    TRANS_LOG(WARN, "invalid argument", KP(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(log_service = static_cast<ObIPartitionLogService*>(partition->get_log_service()))) {
    TRANS_LOG(WARN, "get partiton log service error", KP(partition), KP(log_service));
    ret = OB_ERR_UNEXPECTED;
  } else {
    clog_status = log_service->get_role_unlock(leader_epoch, changing_leader_windows);
  }

  return ret;
}

int ObClogAdapter::get_status(const ObPartitionKey& partition, const bool check_election, int& clog_status,
    int64_t& leader_epoch, ObTsWindows& changing_leader_windows)
{
  UNUSED(check_election);
  int ret = OB_SUCCESS;
  const bool need_safe = true;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObClogAdapter not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObClogAdapter is not running");
    ret = OB_NOT_RUNNING;
  } else if (OB_UNLIKELY(!partition.is_valid())) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(get_status_impl_(partition, need_safe, clog_status, leader_epoch, changing_leader_windows))) {
    TRANS_LOG(WARN, "get status impl error", KR(ret), K(partition), K(need_safe));
  } else {
    // do nothing
  }

  return ret;
}

int ObClogAdapter::submit_log(const ObPartitionKey& partition, const ObVersion& version, const char* buf,
    const int64_t size, ObITransSubmitLogCb* cb, ObIPartitionGroup* pg, uint64_t& cur_log_id,
    int64_t& cur_log_timestamp)
{
  int ret = OB_SUCCESS;
  // const int64_t start = ObTimeUtility::current_time();

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObClogAdapter not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObClogAdapter is not running");
    ret = OB_NOT_RUNNING;
  } else if (OB_UNLIKELY(!partition.is_valid()) || OB_UNLIKELY(!version.is_valid()) || OB_ISNULL(buf) ||
             OB_UNLIKELY(size <= 0) || OB_ISNULL(cb)) {
    TRANS_LOG(WARN, "invalid argument", K(partition), K(version), KP(buf), K(size), KP(cb));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(submit_log_(partition, version, buf, size, cb, pg, cur_log_id, cur_log_timestamp))) {
    if (EXECUTE_COUNT_PER_SEC(16)) {
      TRANS_LOG(WARN, "submit log error", KR(ret), K(partition), K(version), K(cur_log_id), K(cur_log_timestamp));
    }
  } else {
    // do nothing
  }
  /*
  if (OB_SUCCESS == ret) {
    ObTransStatistic::get_instance().add_submit_trans_log_count(partition.get_tenant_id(), 1);
    ObTransStatistic::get_instance().add_submit_trans_log_time(partition.get_tenant_id(),
        ObTimeUtility::current_time() - start);
  }*/

  return ret;
}

int ObClogAdapter::submit_log(const ObPartitionKey& partition, const ObVersion& version, const char* buf,
    const int64_t size, const int64_t base_ts, ObITransSubmitLogCb* cb, ObIPartitionGroup* pg, uint64_t& cur_log_id,
    int64_t& cur_log_timestamp)
{
  int ret = OB_SUCCESS;
  // const int64_t start = ObTimeUtility::current_time();

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObClogAdapter not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObClogAdapter is not running");
    ret = OB_NOT_RUNNING;
  } else if (OB_UNLIKELY(!partition.is_valid()) || OB_UNLIKELY(!version.is_valid()) || OB_ISNULL(buf) ||
             OB_UNLIKELY(size <= 0) || OB_ISNULL(cb)) {
    TRANS_LOG(WARN, "invalid argument", K(partition), K(version), KP(buf), K(size), KP(cb));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(submit_log_(partition, version, buf, size, base_ts, cb, pg, cur_log_id, cur_log_timestamp))) {
    if (EXECUTE_COUNT_PER_SEC(16)) {
      TRANS_LOG(
          WARN, "submit log error", KR(ret), K(partition), K(version), K(base_ts), K(cur_log_id), K(cur_log_timestamp));
    }
  } else {
    // do nothing
  }
  /*
  if (OB_SUCCESS == ret) {
    ObTransStatistic::get_instance().add_submit_trans_log_count(partition.get_tenant_id(), 1);
    ObTransStatistic::get_instance().add_submit_trans_log_time(partition.get_tenant_id(),
        ObTimeUtility::current_time() - start);
  }*/

  return ret;
}

int ObClogAdapter::submit_aggre_log(const common::ObPartitionKey& partition, const common::ObVersion& version,
    const char* buf, const int64_t size, ObITransSubmitLogCb* cb, ObIPartitionGroup* pg, const int64_t base_ts,
    ObTransLogBufferAggreContainer& container)
{
  int ret = OB_SUCCESS;
  const int64_t start = ObTimeUtility::fast_current_time();
  bool need_submit_log = false;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObClogAdapter not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObClogAdapter is not running");
    ret = OB_NOT_RUNNING;
  } else if (OB_UNLIKELY(!partition.is_valid()) || OB_UNLIKELY(!version.is_valid()) || OB_ISNULL(buf) ||
             OB_UNLIKELY(size <= 0) || OB_ISNULL(cb) || OB_UNLIKELY(0 > base_ts)) {
    TRANS_LOG(WARN, "invalid argument", K(partition), K(version), KP(buf), K(size), KP(cb), K(base_ts));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(container.fill(buf, size, cb, base_ts, need_submit_log))) {
    if (OB_NOT_INIT != ret && OB_EAGAIN != ret) {
      TRANS_LOG(WARN, "fill ObTransLogBufferAggreContainer failed", K(ret), K(partition));
    } else {
      ret = OB_SUCCESS;
      uint64_t cur_log_id = 0;
      int64_t cur_log_timestamp = 0;
      if (OB_FAIL(submit_log_(partition, version, buf, size, base_ts, cb, pg, cur_log_id, cur_log_timestamp))) {
        if (EXECUTE_COUNT_PER_SEC(16)) {
          TRANS_LOG(WARN,
              "submit log error",
              K(ret),
              K(partition),
              K(version),
              K(base_ts),
              K(cur_log_id),
              K(cur_log_timestamp));
        }
      }
    }
  } else if (OB_FAIL(cb->set_real_submit_timestamp(ObClockGenerator::getRealClock() + 3000000))) {
    TRANS_LOG(WARN, "set submit timestamp error", K(ret), K(partition));
  } else if (need_submit_log && OB_FAIL(submit_aggre_log_(partition, version, pg, container))) {
    TRANS_LOG(WARN, "submit aggre log failed", K(ret), K(partition));
  } else {
    // do nothing
  }

  if (OB_SUCCESS == ret) {
    ObTransStatistic::get_instance().add_submit_trans_log_count(partition.get_tenant_id(), 1);
    ObTransStatistic::get_instance().add_submit_trans_log_time(
        partition.get_tenant_id(), ObTimeUtility::fast_current_time() - start);
  }
  return ret;
}

int ObClogAdapter::batch_submit_log(const transaction::ObTransID& trans_id,
    const common::ObPartitionArray& partition_array, const ObLogInfoArray& log_info_array,
    const ObISubmitLogCbArray& cb_array)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObClogAdapter not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObClogAdapter is not running");
    ret = OB_NOT_RUNNING;
  } else if (OB_UNLIKELY(!trans_id.is_valid()) || OB_UNLIKELY(partition_array.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(trans_id), K(partition_array), K(log_info_array));
  } else if (OB_UNLIKELY(GCTX.is_in_phy_fb_mode())) {
    // observer is started by flashback mode
    ret = OB_STATE_NOT_MATCH;
    TRANS_LOG(WARN, "observer is started by physical flashback mode, cannot submit log", K(ret), K(trans_id));
    TRANS_LOG(WARN, "cannot submit trans log", K(ret), K(trans_id), K(partition_array), K(log_info_array));
  } else if (OB_FAIL(partition_service_->get_clog_mgr()->batch_submit_log(
                 trans_id, partition_array, log_info_array, cb_array))) {
    TRANS_LOG(WARN, "batch_submit_log error", K(ret), K(trans_id), K(partition_array), K(log_info_array));
  } else {
    // do nothing
  }

  return ret;
}

int ObClogAdapter::submit_log_id_alloc_task(const int64_t log_type, ObTransCtx* ctx)
{
  int ret = OB_SUCCESS;
  AllocLogIdTask* task = NULL;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObClogAdapter not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObClogAdapter is not running");
    ret = OB_NOT_RUNNING;
  } else if (OB_UNLIKELY(!ObTransLogType::is_valid(log_type)) || OB_ISNULL(ctx)) {
    TRANS_LOG(WARN, "invalid argument", K(log_type));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(task = AllocLogIdTaskFactory::alloc())) {
    TRANS_LOG(ERROR, "alloc log id task error");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (TG_GET_QUEUE_NUM(tg_id_) >= TOTAL_TASK - RESERVE_TASK) {
    TRANS_LOG(ERROR, "alloc log id queue is full", K(log_type), "context", *ctx);
    ret = OB_SIZE_OVERFLOW;
  } else if (OB_FAIL(task->make(log_type, ctx))) {
    TRANS_LOG(WARN, "make log task error", KR(ret), K(log_type), "context", *ctx);
  } else if (OB_FAIL(TG_PUSH_TASK(tg_id_, task))) {
    TRANS_LOG(WARN, "push alloc log task error", K(ret));
  } else {
    ObTransStatistic::get_instance().add_clog_submit_count(ctx->get_tenant_id(), 1);
    if (REACH_TIME_INTERVAL(1000 * 1000)) {
      TRANS_LOG(INFO, "push alloc log task success", K(task));
    }
  }
  if (OB_FAIL(ret) && NULL != task) {
    AllocLogIdTaskFactory::release(task);
    task = NULL;
  }

  return ret;
}

int ObClogAdapter::submit_log_task(const ObPartitionKey& partition, const ObVersion& version, const char* buf,
    const int64_t size, const bool with_need_update_version, const int64_t local_trans_version, const bool with_base_ts,
    const int64_t base_ts, ObITransSubmitLogCb* cb)
{
  int ret = OB_SUCCESS;
  SubmitLogTask* task = NULL;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObClogAdapter not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObClogAdapter is not running");
    ret = OB_NOT_RUNNING;
  } else if (OB_UNLIKELY(!partition.is_valid()) || OB_UNLIKELY(!version.is_valid()) || OB_ISNULL(buf) ||
             OB_UNLIKELY(size <= 0) || OB_ISNULL(cb) || (with_need_update_version && local_trans_version <= 0) ||
             (with_base_ts && base_ts <= 0)) {
    TRANS_LOG(WARN,
        "invalid argument",
        K(partition),
        K(version),
        KP(buf),
        K(size),
        KP(cb),
        K(local_trans_version),
        K(with_base_ts),
        K(base_ts));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(task = SubmitLogTaskFactory::alloc())) {
    TRANS_LOG(ERROR, "alloc submit log task error");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_UNLIKELY(TG_GET_QUEUE_NUM(tg_id_) >= TOTAL_TASK - RESERVE_TASK)) {
    TRANS_LOG(ERROR, "submit log task queue is full", K(partition), K(version), KP(buf), K(size), KP(cb));
    ret = OB_SIZE_OVERFLOW;
  } else if (OB_FAIL(task->make(partition,
                 version,
                 buf,
                 size,
                 with_need_update_version,
                 local_trans_version,
                 with_base_ts,
                 base_ts,
                 cb,
                 &allocator_))) {
    TRANS_LOG(WARN, "make submit log task error", KR(ret), K(partition), K(local_trans_version));
  } else if (OB_FAIL(TG_PUSH_TASK(tg_id_, task))) {
    TRANS_LOG(WARN, "push submit log task error", K(ret));
  } else {
    TRANS_LOG(INFO, "push submit log task success", K(task));
  }
  if (OB_FAIL(ret) && NULL != task) {
    SubmitLogTaskFactory::release(task);
    task = NULL;
  }

  return ret;
}

int ObClogAdapter::get_log_id_timestamp(
    const ObPartitionKey& partition, const int64_t prepare_version, ObIPartitionGroup* pg, ObLogMeta& log_meta)
{
  int ret = OB_SUCCESS;
#ifdef ERRSIM
  ret = E(EventTable::ALLOC_LOG_ID_AND_TIMESTAMP_ERROR) OB_SUCCESS;
  if (OB_SUCCESS != ret && partition.get_table_id() == 1099511677777L) {
    TRANS_LOG(WARN, "alloc log id and timestamp error", KR(ret), K(partition), K(prepare_version));
    return ret;
  }
#endif
  storage::ObIPartitionGroupGuard guard;
  ObIPartitionLogService* log_service = NULL;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObClogAdapter not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObClogAdapter is not running");
    ret = OB_NOT_RUNNING;
  } else if (OB_UNLIKELY(!partition.is_valid()) || OB_UNLIKELY(prepare_version <= 0)) {
    TRANS_LOG(WARN, "invalid argument", K(partition), K(prepare_version));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(GCTX.is_in_phy_fb_mode())) {
    // observer is started by flashback mode
    ret = OB_STATE_NOT_MATCH;
    TRANS_LOG(WARN, "observer is started by physical flashback mode, cannot submit log", K(ret), K(partition));
  } else if (NULL != pg) {
    if (OB_ISNULL(log_service = pg->get_log_service())) {
      TRANS_LOG(WARN, "get partiton log service error", K(partition));
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(log_service->get_log_id_timestamp(prepare_version, log_meta))) {
      if (EXECUTE_COUNT_PER_SEC(16)) {
        TRANS_LOG(WARN, "get log id timestamp error", K(ret), K(partition), K(prepare_version));
      }
    }
  } else if (OB_FAIL(partition_service_->get_partition(partition, guard))) {
    TRANS_LOG(WARN, "get partition failed", K(partition), KR(ret));
  } else if (NULL == guard.get_partition_group()) {
    TRANS_LOG(WARN, "partition info is null", K(partition));
    ret = OB_ERR_UNEXPECTED;
  } else if (NULL == (log_service = guard.get_partition_group()->get_log_service())) {
    TRANS_LOG(WARN, "get partiton log service error", K(partition));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(log_service->get_log_id_timestamp(prepare_version, log_meta))) {
    if (EXECUTE_COUNT_PER_SEC(16)) {
      TRANS_LOG(WARN, "get log id timestamp error", K(ret), K(partition), K(prepare_version));
    }
  } else {
    // do nothing
  }

  return ret;
}

int ObClogAdapter::flush_aggre_log(const common::ObPartitionKey& partition, const common::ObVersion& version,
    ObIPartitionGroup* pg, ObTransLogBufferAggreContainer& container)
{
  int ret = OB_SUCCESS;
  const int64_t start = ObTimeUtility::fast_current_time();
  bool need_submit_log = false;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObClogAdapter not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObClogAdapter is not running");
    ret = OB_NOT_RUNNING;
  } else if (OB_UNLIKELY(!partition.is_valid()) || OB_UNLIKELY(!version.is_valid())) {
    TRANS_LOG(WARN, "invalid argument", K(partition), K(version));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(container.flush(need_submit_log))) {
    TRANS_LOG(WARN, "flush ObTransLogBufferAggreContainer failed", K(ret), K(partition));
  } else if (need_submit_log && OB_FAIL(submit_aggre_log_(partition, version, pg, container))) {
    TRANS_LOG(WARN, "submit aggre log failed", K(ret), K(partition));
  } else {
    // do nothing
  }

  if (OB_SUCCESS == ret) {
    ObTransStatistic::get_instance().add_submit_trans_log_count(partition.get_tenant_id(), 1);
    ObTransStatistic::get_instance().add_submit_trans_log_time(
        partition.get_tenant_id(), ObTimeUtility::fast_current_time() - start);
  }
  return ret;
}

int ObClogAdapter::submit_aggre_log_(const ObPartitionKey& partition, const ObVersion& version, ObIPartitionGroup* pg,
    ObTransLogBufferAggreContainer& container)
{
  int ret = OB_SUCCESS;
  UNUSED(version);
  storage::ObIPartitionGroupGuard guard;
  ObIPartitionLogService* log_service = NULL;

  if (!partition.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(partition), K(ret));
  } else if (OB_UNLIKELY(GCTX.is_in_phy_fb_mode())) {
    // observer is started by flashback mode
    ret = OB_STATE_NOT_MATCH;
    TRANS_LOG(WARN, "observer is started by physical flashback mode, cannot submit log", K(ret), K(partition));
  } else if (NULL != pg) {
    if (OB_ISNULL(log_service = pg->get_log_service())) {
      TRANS_LOG(WARN, "get partiton log service error", K(partition));
      ret = OB_ERR_UNEXPECTED;
    }
  } else if (OB_FAIL(partition_service_->get_partition(partition, guard))) {
    TRANS_LOG(WARN, "get partition failed", K(partition), K(ret));
  } else if (OB_ISNULL(guard.get_partition_group())) {
    TRANS_LOG(WARN, "partition info is null", K(partition));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(log_service = guard.get_partition_group()->get_log_service())) {
    TRANS_LOG(WARN, "get partiton log service error", K(partition));
    ret = OB_ERR_UNEXPECTED;
  } else {
    // do nothing
  }
  if (OB_SUCC(ret) && NULL != log_service) {
    if (OB_FAIL(log_service->submit_aggre_log(container.get_aggre_buffer(), container.get_base_timestamp()))) {
      if (need_retry(ret)) {
        ret = OB_SUCCESS;
        if (OB_FAIL(TG_PUSH_TASK(tg_id_, container.get_task()))) {
          TRANS_LOG(WARN, "push aggre submit log task error", K(ret));
          container.get_task()->set_in_queue(false);
        } else {
          container.get_task()->set_in_queue(true);
          TRANS_LOG(INFO, "push aggre submit log task success", K(partition));
        }
      } else {
        TRANS_LOG(WARN, "submit aggre log error", K(ret), K(partition));
        container.get_task()->set_in_queue(false);
      }
    } else {
      container.get_task()->set_in_queue(false);
      container.reuse();
      // ObTransStatistic::get_instance().add_clog_submit_count(partition.get_tenant_id(), 1);
      TRANS_LOG(DEBUG, "submit log success", K(partition));
    }
  }

  // statistics
  ++total_access_;
  if (OB_FAIL(ret)) {
    ++error_count_;
    container.get_task()->set_in_queue(false);
  }
  statistics();
  return ret;
}

int ObClogAdapter::submit_log_(const ObPartitionKey& partition, const ObVersion& version, const char* buf,
    const int64_t size, ObITransSubmitLogCb* cb, ObIPartitionGroup* pg, uint64_t& cur_log_id,
    int64_t& cur_log_timestamp)
{
  int ret = OB_SUCCESS;
  UNUSED(version);
  storage::ObIPartitionGroupGuard guard;
  ObIPartitionLogService* log_service = NULL;
  const int64_t base_timestamp = 0;
  const bool is_trans_log = true;

  if (OB_UNLIKELY(!partition.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(partition), K(ret));
  } else if (OB_UNLIKELY(GCTX.is_in_phy_fb_mode())) {
    // observer is started by flashback mode
    ret = OB_STATE_NOT_MATCH;
    TRANS_LOG(WARN, "observer is started by physical flashback mode, cannot submit log", K(ret), K(partition));
  } else if (NULL != pg) {
    if (OB_ISNULL(log_service = pg->get_log_service())) {
      TRANS_LOG(WARN, "get partiton log service error", K(partition));
      ret = OB_ERR_UNEXPECTED;
    }
  } else if (OB_FAIL(partition_service_->get_partition(partition, guard))) {
    TRANS_LOG(WARN, "get partition failed", K(partition), K(ret));
  } else if (OB_ISNULL(guard.get_partition_group())) {
    TRANS_LOG(WARN, "partition info is null", K(partition));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(log_service = guard.get_partition_group()->get_log_service())) {
    TRANS_LOG(WARN, "get partiton log service error", K(partition));
    ret = OB_ERR_UNEXPECTED;
  } else {
    // do nothing
  }
  if (OB_SUCC(ret) && NULL != log_service) {
    if (OB_FAIL(cb->set_real_submit_timestamp(ObClockGenerator::getRealClock()))) {
      TRANS_LOG(WARN, "set submit timestamp error", K(ret), K(partition), K(size));
    } else if (OB_FAIL(log_service->submit_log(
                   buf, size, base_timestamp, cb, is_trans_log, cur_log_id, cur_log_timestamp))) {
      if (EXECUTE_COUNT_PER_SEC(16)) {
        TRANS_LOG(WARN, "submit log error", K(ret), K(partition), K(size));
      }
    } else {
    }
    if (OB_SUCC(ret)) {
      // ObTransStatistic::get_instance().add_clog_submit_count(partition.get_tenant_id(), 1);
      TRANS_LOG(DEBUG, "submit log success", K(partition), K(size));
    }
  }

  // statistics
  ++total_access_;
  if (OB_FAIL(ret)) {
    ++error_count_;
  }
  statistics();
  return ret;
}

int ObClogAdapter::submit_log_(const ObPartitionKey& partition, const ObVersion& version, const char* buf,
    const int64_t size, const int64_t base_ts, ObITransSubmitLogCb* cb, ObIPartitionGroup* pg, uint64_t& cur_log_id,
    int64_t& cur_log_timestamp)
{
  int ret = OB_SUCCESS;
  UNUSED(version);
  storage::ObIPartitionGroupGuard guard;
  ObIPartitionLogService* log_service = NULL;
  const bool is_trans_log = true;

  if (OB_UNLIKELY(!partition.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(partition), K(ret));
  } else if (OB_UNLIKELY(GCTX.is_in_phy_fb_mode())) {
    // observer is started by flashback mode
    ret = OB_STATE_NOT_MATCH;
    TRANS_LOG(WARN, "observer is started by physical flashback mode, cannot submit log", K(ret), K(partition));
  } else if (NULL != pg) {
    if (OB_ISNULL(log_service = pg->get_log_service())) {
      TRANS_LOG(WARN, "get partiton log service error", K(partition));
      ret = OB_ERR_UNEXPECTED;
    }
  } else if (OB_FAIL(partition_service_->get_partition(partition, guard))) {
    TRANS_LOG(WARN, "get partition failed", K(partition), K(ret));
  } else if (OB_ISNULL(guard.get_partition_group())) {
    TRANS_LOG(WARN, "partition info is null", K(partition));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(log_service = guard.get_partition_group()->get_log_service())) {
    TRANS_LOG(WARN, "get partiton log service error", K(partition));
    ret = OB_ERR_UNEXPECTED;
  } else {
    // do nothing
  }
  if (OB_SUCC(ret) && NULL != log_service) {
    if (OB_FAIL(cb->set_real_submit_timestamp(ObClockGenerator::getRealClock()))) {
      TRANS_LOG(WARN, "set submit timestamp error", K(ret), K(partition), K(size));
    } else if (OB_FAIL(log_service->submit_log(buf, size, base_ts, cb, is_trans_log, cur_log_id, cur_log_timestamp))) {
      if (EXECUTE_COUNT_PER_SEC(16)) {
        TRANS_LOG(WARN, "submit log error", K(ret), K(partition), K(size));
      }
    } else {
      // do nothing
    }
    if (OB_SUCC(ret)) {
      // ObTransStatistic::get_instance().add_clog_submit_count(partition.get_tenant_id(), 1);
      TRANS_LOG(DEBUG, "submit log success", K(partition), K(size));
    }
  }

  // statistics
  ++total_access_;
  if (OB_FAIL(ret)) {
    ++error_count_;
  }
  statistics();

  return ret;
}

int ObClogAdapter::backfill_nop_log(const ObPartitionKey& partition, ObIPartitionGroup* pg, const ObLogMeta& log_meta)
{
  int ret = OB_SUCCESS;
  storage::ObIPartitionGroupGuard guard;
  ObIPartitionLogService* pls = NULL;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObClogAdapter not inited");
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObClogAdapter is not running");
  } else if (OB_UNLIKELY(!partition.is_valid()) || OB_UNLIKELY(!log_meta.is_valid())) {
    TRANS_LOG(WARN, "invalid argument", K(partition), K(log_meta));
    ret = OB_INVALID_ARGUMENT;
  } else if (NULL != pg) {
    if (OB_ISNULL(pls = pg->get_log_service())) {
      TRANS_LOG(WARN, "get partiton log service error", K(partition));
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(pls->backfill_nop_log(log_meta))) {
      TRANS_LOG(WARN, "backfill nop log error", KR(ret), K(partition), K(log_meta));
    }
  } else if (OB_FAIL(partition_service_->get_partition(partition, guard))) {
    TRANS_LOG(WARN, "get partition failed", K(partition), KR(ret));
  } else if (OB_ISNULL(guard.get_partition_group())) {
    TRANS_LOG(WARN, "partition not exist", K(partition));
    ret = OB_PARTITION_NOT_EXIST;
  } else if (OB_ISNULL(pls = guard.get_partition_group()->get_log_service())) {
    TRANS_LOG(WARN, "get partiton log service error", K(partition));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(pls->backfill_nop_log(log_meta))) {
    TRANS_LOG(WARN, "backfill nop log error", KR(ret), K(partition), K(log_meta));
  } else {
    // do nothing
  }

  return ret;
}

int ObClogAdapter::submit_backfill_nop_log_task(const ObPartitionKey& partition, const ObLogMeta& log_meta)
{
  int ret = OB_SUCCESS;

  BackfillNopLogTask* task = NULL;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObClogAdapter not inited", KR(ret));
  } else if (OB_UNLIKELY(!is_running_)) {
    ret = OB_NOT_RUNNING;
    TRANS_LOG(WARN, "ObClogAdapter is not running", KR(ret));
  } else if (OB_ISNULL(task = BackfillNopLogTaskFactory::alloc())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TRANS_LOG(WARN, "backfill nop log task is null", KR(ret), K(partition));
  } else if (TG_GET_QUEUE_NUM(tg_id_) >= TOTAL_TASK - RESERVE_TASK) {
    TRANS_LOG(ERROR, "alloc rollback trans task queue is full", K(partition), K(log_meta));
    ret = OB_SIZE_OVERFLOW;
  } else if (OB_FAIL(task->make(partition, log_meta))) {
    TRANS_LOG(WARN, "make BackfillNopLogTask fail", KR(ret), K(partition), K(log_meta));
  } else if (OB_UNLIKELY(!task->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "backfill nop log task invalid", KR(ret), "BackfillNopLogTask", *(task));
  } else if (OB_FAIL(TG_PUSH_TASK(tg_id_, task))) {
    TRANS_LOG(WARN, "push backfill nop log task fail", K(ret), "BackfillNopLogTask", *(task));
  } else {
    TRANS_LOG(INFO, "push backfill nop log task succ", KR(ret), "BackfillNopLogTask", *(task));
  }
  if (OB_FAIL(ret) && (NULL != task)) {
    BackfillNopLogTaskFactory::release(task);
    task = NULL;
  }

  return ret;
}

int ObClogAdapter::get_last_submit_timestamp(const ObPartitionKey& partition, int64_t& timestamp)
{
  int ret = OB_SUCCESS;
  storage::ObIPartitionGroupGuard guard;
  ObIPartitionLogService* log_service = NULL;
  if (!is_inited_) {
    TRANS_LOG(WARN, "ObClogAdapter not inited");
    ret = OB_NOT_INIT;
  } else if (!is_running_) {
    TRANS_LOG(WARN, "ObClogAdapter is not running");
    ret = OB_NOT_RUNNING;
  } else if (!partition.is_valid()) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(partition_service_->get_partition(partition, guard))) {
    TRANS_LOG(WARN, "get partition failed", K(partition), K(ret));
  } else if (NULL == guard.get_partition_group()) {
    TRANS_LOG(WARN, "partition not exist", K(partition));
    ret = OB_PARTITION_NOT_EXIST;
  } else if (NULL == (log_service = guard.get_partition_group()->get_log_service())) {
    TRANS_LOG(WARN, "get partiton log service error", K(partition));
    ret = OB_ERR_UNEXPECTED;
  } else {
    timestamp = log_service->get_last_submit_timestamp();
  }

  return ret;
}

void AllocLogIdTask::reset()
{
  ObTransTask::reset();
  log_type_ = storage::OB_LOG_UNKNOWN;
  ctx_ = NULL;
}

int AllocLogIdTask::make(const int64_t log_type, ObTransCtx* ctx)
{
  int ret = OB_SUCCESS;

  if (!ObTransLogType::is_valid(log_type) || OB_ISNULL(ctx)) {
    TRANS_LOG(WARN, "invalid argument", K(log_type), KP(ctx));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ObTransTask::make(ObTransRetryTaskType::ALLOC_LOG_ID))) {
    TRANS_LOG(WARN, "ObTransTask make error", K(log_type), "context", *ctx);
  } else {
    log_type_ = log_type;
    ctx_ = ctx;
  }

  return ret;
}

void SubmitLogTask::reset()
{
  partition_.reset();
  version_.reset();
  with_need_update_version_ = false;
  local_trans_version_ = OB_INVALID_TIMESTAMP;
  with_base_ts_ = false;
  base_ts_ = OB_INVALID_TIMESTAMP;
  if (NULL != clog_buf_) {
    ob_free(clog_buf_);
    clog_buf_ = NULL;
  }
  clog_size_ = 0;
  cb_ = NULL;
  allocator_ = NULL;
  submit_task_ts_ = 0;
  process_begin_ts_ = 0;
}

void SubmitLogTask::destroy()
{
  const int64_t process_end_ts = ObTimeUtility::fast_current_time();
  const int64_t total_used_time = process_end_ts - submit_task_ts_;
  const int64_t pending_used_time = process_begin_ts_ - submit_task_ts_;
  const int64_t process_used_time = process_end_ts - process_begin_ts_;
  if (total_used_time > 30 * 1000) {
    TRANS_LOG(WARN,
        "submit_log task cost too much time",
        K(total_used_time),
        K(pending_used_time),
        K(process_used_time),
        K_(partition));
  }
  reset();
}

int SubmitLogTask::make(const ObPartitionKey& partition, const ObVersion& version, const char* buf, const int64_t size,
    const bool with_need_update_version, const int64_t local_trans_version, const bool with_base_ts,
    const int64_t base_ts, ObITransSubmitLogCb* cb, ObConcurrentFIFOAllocator* allocator)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!partition.is_valid()) || OB_UNLIKELY(!version.is_valid()) || OB_ISNULL(buf) ||
      OB_UNLIKELY(size <= 0) || OB_ISNULL(cb) || OB_ISNULL(allocator)) {
    TRANS_LOG(WARN, "invalid argument", K(partition), K(version), KP(buf), K(size), KP(cb), KP(allocator));
    ret = OB_INVALID_ARGUMENT;
    //} else if (NULL == (clog_buf_ = static_cast<char *>(allocator->alloc(size)))) {
  } else if (OB_ISNULL(clog_buf_ = static_cast<char*>(ob_malloc(size, ObModIds::OB_TRANS_CLOG_BUF)))) {
    TRANS_LOG(WARN, "clog_buf memory alloc failed", KP(clog_buf_));
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_FAIL(ObTransTask::make(ObTransRetryTaskType::SUBMIT_LOG))) {
    TRANS_LOG(WARN, "ObTransTask make error", K(partition), K(version), KP(buf), K(size));
  } else {
    partition_ = partition;
    version_ = version;
    with_need_update_version_ = with_need_update_version;
    local_trans_version_ = local_trans_version;
    with_base_ts_ = with_base_ts;
    base_ts_ = base_ts;
    MEMCPY(clog_buf_, buf, size);
    clog_size_ = size;
    cb_ = cb;
    allocator_ = allocator;
    submit_task_ts_ = ObTimeUtility::fast_current_time();
  }

  return ret;
}

void BackfillNopLogTask::reset()
{
  ObTransTask::reset();
  partition_.reset();
  log_meta_.reset();
}

int BackfillNopLogTask::make(const ObPartitionKey& partition, const ObLogMeta& log_meta)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!partition.is_valid()) || OB_UNLIKELY(!log_meta.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(partition), K(log_meta));
  } else if (OB_FAIL(ObTransTask::make(ObTransRetryTaskType::BACKFILL_NOP_LOG))) {
    TRANS_LOG(WARN, "make backfill nop log fail", KR(ret));
  } else {
    partition_ = partition;
    log_meta_ = log_meta;
  }

  return ret;
}

bool BackfillNopLogTask::is_valid() const
{
  return (partition_.is_valid() && log_meta_.is_valid());
}

void AggreLogTask::reset()
{
  ObTransTask::reset();
  partition_.reset();
  container_ = NULL;
  in_queue_ = false;
}

int AggreLogTask::make(const common::ObPartitionKey& partition, ObTransLogBufferAggreContainer* container)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!partition.is_valid()) || OB_ISNULL(container)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(partition), KP(container));
  } else if (OB_FAIL(ObTransTask::make(ObTransRetryTaskType::AGGRE_LOG_TASK))) {
    TRANS_LOG(WARN, "make aggre log task fail", K(ret));
  } else {
    partition_ = partition;
    container_ = container;
  }

  return ret;
}

}  // namespace transaction
}  // namespace oceanbase
