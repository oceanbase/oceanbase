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

#define USING_LOG_PREFIX OBLOG_FETCHER

#include "ob_log_fetch_stream_container.h"

#include "lib/utility/ob_macro_utils.h"     // OB_UNLIKELY
#include "lib/oblog/ob_log_module.h"        // LOG_ERROR

#include "ob_log_part_fetch_ctx.h"          // PartFetchCtx
#include "ob_log_fetch_stream.h"            // FetchStream
#include "ob_log_fetch_stream_pool.h"       // IFetchStreamPool

using namespace oceanbase::common;
namespace oceanbase
{
namespace liboblog
{

FetchStreamContainer::FetchStreamContainer(const FetchStreamType stype) : stype_(stype)
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

  svr_.reset();

  rpc_ = NULL;
  fs_pool_ = NULL;
  svr_finder_ = NULL;
  heartbeater_ = NULL;
  stream_worker_ = NULL;
  rpc_result_pool_ = NULL;
  progress_controller_ = NULL;

  fs_list_.reset();
}

void FetchStreamContainer::reset(const common::ObAddr &svr,
    IObLogRpc &rpc,
    IFetchStreamPool &fs_pool,
    IObLogSvrFinder &svr_finder,
    IObLogFetcherHeartbeatWorker &heartbeater,
    IObLogStreamWorker &stream_worker,
    IFetchLogARpcResultPool &rpc_result_pool,
    PartProgressController &progress_controller)
{
  reset();

  svr_ = svr;
  rpc_ = &rpc;
  fs_pool_ = &fs_pool;
  svr_finder_ = &svr_finder;
  heartbeater_ = &heartbeater;
  stream_worker_ = &stream_worker;
  rpc_result_pool_ = &rpc_result_pool;
  progress_controller_ = &progress_controller;
}

int FetchStreamContainer::dispatch(PartFetchCtx &task)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(task.get_fetch_stream_type() != stype_)) {
    LOG_ERROR("invalid part task, fetch stream type does not match", K(stype_),
        K(task.get_fetch_stream_type()), K(task));
    ret = OB_INVALID_ARGUMENT;
  } else {
    bool succeed = false;

    while (OB_SUCCESS == ret && ! succeed) {
      FSList base_fs_list;
      // Find a fetch stream with free space and add it to the partition task
      // If no free fetch stream is found, return the head of the linklist used for this lookup
      if (OB_FAIL(find_fs_and_add_task_(task, succeed, base_fs_list))) {
        LOG_ERROR("find_fs_and_add_task_ fail", KR(ret), K(task));
      } else if (! succeed) {
        // Try to add a new available fetch stream, in order to solve the multi-threaded duplicate creation and lookup problem
        // Compare the fetch stream linklist for changes, and if someone else has already created a new one, do not create it in
        if (OB_FAIL(try_create_new_fs_and_add_task_(task, base_fs_list, succeed))) {
          LOG_ERROR("try_create_new_fs_and_add_task_ fail", KR(ret), K(task), K(base_fs_list));
        }
      }
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

int FetchStreamContainer::find_fs_and_add_task_(PartFetchCtx &task,
    bool &succeed,
    FSList &base_fs_list)
{
  int ret = OB_SUCCESS;

  // Add read locks to allow concurrent lookups and inserts
  SpinRLockGuard guard(lock_);

  base_fs_list = fs_list_;
  succeed = false;

  FetchStream *fs = fs_list_.head();
  while (OB_SUCCESS == ret && ! succeed && NULL != fs) {
    ret = fs->add_fetch_task(task);

    if (OB_SUCCESS == ret) {
      succeed = true;
    } else if (OB_SIZE_OVERFLOW == ret) {
      // The task is full, change the next object
      ret = OB_SUCCESS;
      fs = fs->get_next();
    } else {
      LOG_ERROR("add fetch task into fetch stream fail", KR(ret), K(task), K(fs));
    }
  }

  return ret;
}

int FetchStreamContainer::alloc_fetch_stream_(FetchStream *&fs)
{
  int ret = OB_SUCCESS;
  fs = NULL;

  if (OB_ISNULL(rpc_)
      || OB_ISNULL(fs_pool_)
      || OB_ISNULL(svr_finder_)
      || OB_ISNULL(heartbeater_)
      || OB_ISNULL(stream_worker_)
      || OB_ISNULL(rpc_result_pool_)
      || OB_ISNULL(progress_controller_)) {
    LOG_ERROR("invalid handlers", K(rpc_), K(fs_pool_), K(svr_finder_), K(heartbeater_),
        K(stream_worker_), K(rpc_result_pool_), K(progress_controller_));
    ret = OB_INVALID_ERROR;
  } else if (OB_FAIL(fs_pool_->alloc(fs))) {
    LOG_ERROR("alloc fetch stream fail", KR(ret), K(fs_pool_));
  } else if (OB_ISNULL(fs)) {
    LOG_ERROR("invalid fetch stream", K(fs));
    ret = OB_ERR_UNEXPECTED;
  } else {
    fs->reset(svr_, stype_,
        *rpc_,
        *svr_finder_,
        *heartbeater_,
        *stream_worker_,
        *rpc_result_pool_,
        *progress_controller_);

    LOG_INFO("[STAT] [FETCH_STREAM_CONTAINER] [ALLOC_FETCH_STREAM]", K(fs), KPC(fs));
  }

  return ret;
}

int FetchStreamContainer::try_create_new_fs_and_add_task_(PartFetchCtx &task,
    FSList &base_fs_list,
    bool &succeed)
{
  int ret = OB_SUCCESS;
  FetchStream *fs = NULL;

  // Add write lock to ensure that only one thread creates a new fetch stream
  SpinWLockGuard guard(lock_);

  succeed = false;
  // If the linklist changes, just exit and retry next time
  if (base_fs_list != fs_list_) {
    LOG_DEBUG("new fetch stream has been created, retry next time", K(fs_list_), K(base_fs_list));
  } else if (OB_FAIL(alloc_fetch_stream_(fs))) {
    LOG_ERROR("alloc fetch stream fail", KR(ret));
  } else if (OB_ISNULL(fs)) {
    LOG_ERROR("invalid fetch stream", K(fs));
    ret = OB_ERR_UNEXPECTED;
  } else {
    // Update fetch stream linklist
    fs_list_.add_head(*fs);

    // Add the task to the new fetch log stream, expecting certain success
    if (OB_FAIL(fs->add_fetch_task(task))) {
      LOG_ERROR("add fetch task into new fetch stream fail", KR(ret), K(task), K(fs));
    } else {
      succeed = true;
    }
  }

  return ret;
}

}
}
