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

#include "ob_batch_submit_ctx_mgr.h"
#include "lib/thread/ob_thread_name.h"
#include "common/ob_clock_generator.h"
#include "ob_batch_submit_ctx.h"
#include "ob_batch_submit_task.h"
#include "ob_log_define.h"

namespace oceanbase {
using namespace common;
namespace clog {
class ObBatchSubmitCtxMgr::RemoveIfFunctor {
public:
  RemoveIfFunctor()
  {}
  ~RemoveIfFunctor()
  {}

public:
  bool operator()(const transaction::ObTransID& trans_id, ObBatchSubmitCtx* ctx)
  {
    int tmp_ret = OB_SUCCESS;

    if (!trans_id.is_valid() || OB_ISNULL(ctx)) {
      tmp_ret = OB_INVALID_ARGUMENT;
      CLOG_LOG(ERROR, "invalid arguments", K(tmp_ret), K(trans_id), KP(ctx));
    } else if (OB_SUCCESS != (tmp_ret = ctx->try_split()) && OB_NO_NEED_BATCH_CTX != tmp_ret) {
      CLOG_LOG(ERROR, "batch_submit_ctx try_split failed", K(tmp_ret), K(trans_id));
    }

    return OB_NO_NEED_BATCH_CTX == tmp_ret;
  }
};

int ObBatchSubmitCtxMgr::init(
    storage::ObPartitionService* partition_service, ObILogEngine* log_engine, const common::ObAddr& self)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(WARN, "ObBatchSubmitCtxMgr inited twice", K(ret));
  } else if (OB_ISNULL(partition_service) || OB_ISNULL(log_engine) || !self.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), KP(partition_service), KP(log_engine), K(self));
  } else if (OB_FAIL(ctx_map_.init(ObModIds::OB_HASH_BUCKET_BATCH_SUBMIT_CTX))) {
    CLOG_LOG(WARN, "batch submit ctx map init failed", K(ret));
  } else {
    partition_service_ = partition_service;
    log_engine_ = log_engine;
    self_ = self;
    is_inited_ = true;
  }

  return ret;
}

int ObBatchSubmitCtxMgr::start()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(share::ObThreadPool::start())) {
    CLOG_LOG(ERROR, "ObBatchSubmitCtxMgr thread failed to start", K(ret));
  } else {
    is_running_ = true;
  }
  CLOG_LOG(INFO, "ObBatchSubmitCtxMgr thread start finished", K(ret));
  return ret;
}

void ObBatchSubmitCtxMgr::stop()
{
  share::ObThreadPool::stop();
  is_running_ = false;
  CLOG_LOG(INFO, "ObBatchSubmitCtxMgr stop finished");
}

void ObBatchSubmitCtxMgr::wait()
{
  share::ObThreadPool::wait();
  CLOG_LOG(INFO, "ObBatchSubmitCtxMgr wait finished");
}

void ObBatchSubmitCtxMgr::reset()
{
  is_inited_ = false;
  is_running_ = false;
  ctx_map_.reset();
  partition_service_ = NULL;
  log_engine_ = NULL;
  self_.reset();
  CLOG_LOG(INFO, "ObBatchSubmitCtxMgr reset");
}

void ObBatchSubmitCtxMgr::destroy()
{
  is_inited_ = false;
  stop();
  wait();
  ctx_map_.destroy();
  partition_service_ = NULL;
  log_engine_ = NULL;
  CLOG_LOG(INFO, "ObBatchSubmitCtxMgr destroy");
}

int ObBatchSubmitCtxMgr::alloc_ctx(const transaction::ObTransID& trans_id,
    const common::ObPartitionArray& partition_array, const ObLogInfoArray& log_info_array,
    const ObLogPersistSizeArray& size_array, const ObISubmitLogCbArray& cb_array,
    const common::ObMemberList& member_list, const int64_t replica_num, const common::ObAddr& leader,
    common::ObILogAllocator* alloc_mgr, ObBatchSubmitCtx*& ctx)
{
  int ret = OB_SUCCESS;

  ObBatchSubmitCtx* tmp_ctx = NULL;
  const int64_t partition_array_count = partition_array.count();
  const int64_t log_info_array_count = log_info_array.count();
  const int64_t log_persist_size_count = size_array.count();
  const int64_t cb_array_count = cb_array.count();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObBatchSubmitCtxMgr is not inited", K(ret), K(trans_id));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    CLOG_LOG(WARN, "ObBatchSubmitCtxMgr is not running", K(ret), K(trans_id));
  } else if (!trans_id.is_valid() || 0 == partition_array_count || partition_array_count != log_info_array_count ||
             log_persist_size_count != log_info_array_count || partition_array_count != cb_array_count ||
             !leader.is_valid() || leader != self_ || !member_list.is_valid() || replica_num <= 0 ||
             NULL == alloc_mgr) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN,
        "invalid arguments",
        K(ret),
        K(trans_id),
        K(partition_array),
        K(log_info_array),
        K(size_array),
        K(member_list),
        K(replica_num),
        K(leader),
        K(self_),
        KP(alloc_mgr));
  } else if (OB_SUCCESS == (ret = ctx_map_.get(trans_id, tmp_ctx))) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "when alloc_ctx, batch_ctx has already existed", K(ret));
  } else if (OB_ENTRY_NOT_EXIST != ret) {
    CLOG_LOG(ERROR, "get batch_submit_ctx error", K(ret), K(trans_id));
  } else {
    ret = OB_SUCCESS;
    if (NULL == (tmp_ctx = ObBatchSubmitCtxFactory::alloc(alloc_mgr))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      CLOG_LOG(WARN, "alloc batch_submit_ctx failed", K(ret), K(trans_id));
    } else if (OB_FAIL(tmp_ctx->init(trans_id,
                   partition_array,
                   log_info_array,
                   size_array,
                   cb_array,
                   member_list,
                   replica_num,
                   leader,
                   self_,
                   partition_service_,
                   log_engine_,
                   alloc_mgr))) {
      CLOG_LOG(WARN,
          "batch_submit_ctx init failed",
          K(ret),
          K(trans_id),
          K(partition_array),
          K(log_info_array),
          K(size_array),
          K(leader),
          K(self_));
    } else if (OB_FAIL(ctx_map_.insert_and_get(trans_id, tmp_ctx))) {
      CLOG_LOG(WARN, "insert batch_submit_ctx error", K(ret), K(trans_id));
    } else {
      ctx = tmp_ctx;
    }

    if (OB_SUCCESS != ret && NULL != tmp_ctx) {
      ObBatchSubmitCtxFactory::free(tmp_ctx);
      tmp_ctx = NULL;
    }
  }

  return ret;
}

int ObBatchSubmitCtxMgr::alloc_ctx(const transaction::ObTransID& trans_id,
    const common::ObPartitionArray& partition_array, const ObLogInfoArray& log_info_array,
    const ObLogPersistSizeArray& size_array, const common::ObAddr& leader, common::ObILogAllocator* alloc_mgr,
    ObBatchSubmitCtx*& ctx)
{
  int ret = OB_SUCCESS;

  ObBatchSubmitCtx* tmp_ctx = NULL;
  const int64_t partition_array_count = partition_array.count();
  const int64_t log_info_array_count = log_info_array.count();
  const int64_t log_persist_size_count = size_array.count();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObBatchSubmitCtxMgr is not inited", K(ret), K(trans_id));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    CLOG_LOG(WARN, "ObBatchSubmitCtxMgr is not running", K(ret), K(trans_id));
  } else if (!trans_id.is_valid() || 0 == partition_array_count || partition_array_count != log_info_array_count ||
             log_persist_size_count != log_info_array_count || !leader.is_valid() || leader == self_ ||
             NULL == alloc_mgr) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN,
        "invalid arguments",
        K(ret),
        K(trans_id),
        K(partition_array),
        K(log_info_array),
        K(size_array),
        K(leader),
        K(self_),
        KP(alloc_mgr));
  } else if (OB_SUCCESS == (ret = ctx_map_.get(trans_id, tmp_ctx))) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "when alloc_ctx, batch_ctx has already existed", K(ret));
  } else if (OB_ENTRY_NOT_EXIST != ret) {
    CLOG_LOG(ERROR, "get batch_submit_ctx error", K(ret), K(trans_id));
  } else {
    ret = OB_SUCCESS;
    if (NULL == (tmp_ctx = ObBatchSubmitCtxFactory::alloc(alloc_mgr))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      CLOG_LOG(WARN, "alloc batch_submit_ctx failed", K(ret), K(trans_id));
    } else if (OB_FAIL(tmp_ctx->init(trans_id,
                   partition_array,
                   log_info_array,
                   size_array,
                   leader,
                   self_,
                   partition_service_,
                   log_engine_,
                   alloc_mgr))) {
      CLOG_LOG(WARN,
          "batch_submit_ctx init failed",
          K(ret),
          K(trans_id),
          K(partition_array),
          K(log_info_array),
          K(leader),
          K(self_));
    } else if (OB_FAIL(ctx_map_.insert_and_get(trans_id, tmp_ctx))) {
      CLOG_LOG(WARN, "insert batch_submit_ctx error", K(ret), K(trans_id));
    } else {
      ctx = tmp_ctx;
    }

    if (OB_SUCCESS != ret && NULL != tmp_ctx) {
      ObBatchSubmitCtxFactory::free(tmp_ctx);
      tmp_ctx = NULL;
    }
  }

  return ret;
}

int ObBatchSubmitCtxMgr::get_ctx(const transaction::ObTransID& trans_id, ObBatchSubmitCtx*& ctx)
{
  int ret = OB_SUCCESS;

  ObBatchSubmitCtx* tmp_ctx = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObBatchSubmitCtxMgr is not inited", K(ret), K(trans_id));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    CLOG_LOG(WARN, "ObBatchSubmitCtxMgr is not running", K(ret), K(trans_id));
  } else if (!trans_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), K(trans_id));
  } else if (OB_FAIL(ctx_map_.get(trans_id, tmp_ctx))) {
    CLOG_LOG(TRACE, "ctx_map_ get failed", K(ret), K(trans_id));
  } else if (OB_ISNULL(tmp_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "ctx is NULL", K(ret));
  } else {
    ctx = tmp_ctx;
  }

  return ret;
}

int ObBatchSubmitCtxMgr::revert_ctx(ObBatchSubmitCtx* ctx)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObBatchSubmitCtxMgr is not inited", K(ret));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    CLOG_LOG(WARN, "ObBatchSubmitCtxMgr is not running", K(ret));
  } else if (OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret));
  } else {
    ctx_map_.revert(ctx);
  }

  return ret;
}

int ObBatchSubmitCtxMgr::free_ctx(const transaction::ObTransID& trans_id)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObBatchSubmitCtxMgr is not inited", K(ret), K(trans_id));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    CLOG_LOG(WARN, "ObBatchSubmitCtxMgr is not running", K(ret), K(trans_id));
  } else if (OB_FAIL(ctx_map_.del(trans_id))) {
    // free_ctx is triggered by special op, failure is unexpected
    CLOG_LOG(ERROR, "del batch_submit_ctx failed", K(ret), K(trans_id));
  }

  return ret;
}

void ObBatchSubmitCtxMgr::run1()
{
  lib::set_thread_name("BatchSubmitCtx");
  while (!has_set_stop()) {
    int ret = OB_SUCCESS;
    const int64_t begin_ts = common::ObClockGenerator::getClock();
    RemoveIfFunctor fn;
    if (OB_FAIL(ctx_map_.remove_if(fn))) {
      CLOG_LOG(WARN, "ctx_map_ remove_if failed", K(ret));
    }
    const int64_t end_ts = common::ObClockGenerator::getClock();

    const int32_t sleep_ts = static_cast<const int32_t>(LOOP_INTERVAL - (end_ts - begin_ts));
    if (sleep_ts > 0) {
      usleep(sleep_ts);
    }
    ObBatchSubmitCtxFactory::statistics();
    ObBatchSubmitDiskTaskFactory::statistics();
  }
}
}  // namespace clog
}  // namespace oceanbase
