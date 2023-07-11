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
 *
 * Fetch Stream Container
 */

#define USING_LOG_PREFIX OBLOG_FETCHER

#include "ob_log_fetch_stream_container.h"

#include "lib/utility/ob_macro_utils.h"     // OB_UNLIKELY
#include "lib/oblog/ob_log_module.h"        // LOG_ERROR

#include "ob_log_ls_fetch_ctx.h"            // LSFetchCtx
#include "ob_log_fetch_stream_pool.h"       // IFetchStreamPool

using namespace oceanbase::common;
namespace oceanbase
{
namespace logfetcher
{
FetchStreamContainer::FetchStreamContainer():
    lock_(ObLatchIds::OBCDC_FETCHSREAM_CONTAINER_LOCK)
{
  reset();
}

FetchStreamContainer::~FetchStreamContainer()
{
  reset();
}

void FetchStreamContainer::reset()
{
  // TODO: Free all task memory from global considerations
  free_fs_list_();

  stype_ = FETCH_STREAM_TYPE_UNKNOWN;
  self_tenant_id_ = OB_INVALID_TENANT_ID;

  rpc_ = NULL;
  fs_pool_ = NULL;
  stream_worker_ = NULL;
  rpc_result_pool_ = NULL;
  progress_controller_ = NULL;
  log_handler_ = NULL;

  fs_list_.reset();
}

void FetchStreamContainer::reset(const FetchStreamType stype,
    const uint64_t self_tenant_id,
    IObLogRpc &rpc,
    IFetchStreamPool &fs_pool,
    IObLSWorker &stream_worker,
    IFetchLogARpcResultPool &rpc_result_pool,
    PartProgressController &progress_controller,
    ILogFetcherHandler &log_handler)
{
  reset();

  stype_ = stype;
  self_tenant_id_ = self_tenant_id;
  rpc_ = &rpc;
  fs_pool_ = &fs_pool;
  stream_worker_ = &stream_worker;
  rpc_result_pool_ = &rpc_result_pool;
  progress_controller_ = &progress_controller;
  log_handler_ = &log_handler;
}

int FetchStreamContainer::dispatch(LSFetchCtx &task,
    const common::ObAddr &request_svr)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(task.get_fetch_stream_type() != stype_)) {
    LOG_ERROR("invalid part task, fetch stream type does not match", K(stype_),
        K(task.get_fetch_stream_type()), K(task));
    ret = OB_INVALID_ARGUMENT;
  } else {
    if (OB_FAIL(try_create_new_fs_and_add_task_(task, request_svr))) {
      LOG_ERROR("try_create_new_fs_and_add_task_ fail", KR(ret), K(task), K(request_svr));
    }
  }

  return ret;
}

void FetchStreamContainer::do_stat()
{
  // Add read locks to allow concurrent lookups and inserts
  SpinRLockGuard guard(lock_);

  FetchStream *fs = fs_list_.head();
  while (NULL != fs) {
    if (fs->get_fetch_task_count() > 0) {
      fs->do_stat();
    }
    fs = fs->get_next();
  }
}

// Release all FetchStream objects in the linklist
void FetchStreamContainer::free_fs_list_()
{
  if (NULL != fs_pool_ && fs_list_.count() > 0) {
    FetchStream *fs = fs_list_.head();
    while (NULL != fs) {
      FetchStream *next = fs->get_next();
      fs->reset();
      (void)fs_pool_->free(fs);
      fs = next;
    }

    fs_list_.reset();
  }
}

int FetchStreamContainer::try_create_new_fs_and_add_task_(LSFetchCtx &ls_fetch_ctx,
    const common::ObAddr &request_svr)
{
  int ret = OB_SUCCESS;
  FetchStream *fs = NULL;
  const uint64_t tenant_id = ls_fetch_ctx.get_tls_id().get_tenant_id();

  // Add write lock to ensure that only one thread creates a new fetch stream
  SpinWLockGuard guard(lock_);

  if (MAX_FS_COUNT == fs_list_.count()) {
    fs = fs_list_.head();
  } else {
    if (OB_FAIL(alloc_fetch_stream_(tenant_id, ls_fetch_ctx, fs))) {
      LOG_ERROR("alloc fetch stream fail", KR(ret), K(ls_fetch_ctx));
    } else if (OB_ISNULL(fs)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid fetch stream", KR(ret), K(fs));
    } else {
      // Update fetch stream linklist
      fs_list_.add_head(*fs);
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(fs)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid fetch stream", KR(ret), K(fs));
    } else if (OB_FAIL(fs->prepare_to_fetch_logs(ls_fetch_ctx, request_svr))) {
      LOG_ERROR("add fetch task into new fetch stream fail", KR(ret), K(ls_fetch_ctx), K(fs));
    }
  }

  return ret;
}

int FetchStreamContainer::alloc_fetch_stream_(
    const uint64_t tenant_id,
    LSFetchCtx &ls_fetch_ctx,
    FetchStream *&fs)
{
  int ret = OB_SUCCESS;
  fs = NULL;

  if (OB_ISNULL(rpc_)
      || OB_ISNULL(fs_pool_)
      || OB_ISNULL(stream_worker_)
      || OB_ISNULL(rpc_result_pool_)
      || OB_ISNULL(progress_controller_)) {
    LOG_ERROR("invalid handlers", K(rpc_), K(fs_pool_),
        K(stream_worker_), K(rpc_result_pool_), K(progress_controller_));
    ret = OB_INVALID_ERROR;
  } else if (OB_FAIL(fs_pool_->alloc(fs))) {
    LOG_ERROR("alloc fetch stream fail", KR(ret), K(fs_pool_));
  } else if (OB_ISNULL(fs)) {
    LOG_ERROR("invalid fetch stream", K(fs));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(fs->init(tenant_id,
        self_tenant_id_,
        ls_fetch_ctx,
        stype_,
        *rpc_,
        *stream_worker_,
        *rpc_result_pool_,
        *progress_controller_,
        *log_handler_))) {
    LOG_ERROR("FetchStream init failed", KR(ret), K(ls_fetch_ctx), K(fs), KPC(fs));
  } else {
    LOG_INFO("[STAT] [FETCH_STREAM_CONTAINER] [ALLOC_FETCH_STREAM]", K(fs), KPC(fs));
  }

  return ret;
}

}
}
