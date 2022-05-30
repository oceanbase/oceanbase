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

#include "ob_log_svr_stream.h"

#include "ob_log_part_fetch_ctx.h"        // PartFetchCtx
#include "ob_log_fetch_stream_type.h"     // FETCH_STREAM_TYPE_HOT
#include "ob_log_utils.h"                 // SIZE_TO_STR

namespace oceanbase
{
namespace liboblog
{

SvrStream::SvrStream() :
    ddl_stream_(FETCH_STREAM_TYPE_DDL),
    hot_stream_(FETCH_STREAM_TYPE_HOT),
    cold_stream_(FETCH_STREAM_TYPE_COLD)
{
  reset();
}

SvrStream::~SvrStream()
{
  reset();
}

void SvrStream::reset()
{
  svr_.reset();
  ddl_stream_.reset();
  hot_stream_.reset();
  cold_stream_.reset();
}

void SvrStream::reset(const common::ObAddr &svr,
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

  ddl_stream_.reset(svr,
      rpc,
      fs_pool,
      svr_finder,
      heartbeater,
      stream_worker,
      rpc_result_pool,
      progress_controller);

  hot_stream_.reset(svr,
      rpc,
      fs_pool,
      svr_finder,
      heartbeater,
      stream_worker,
      rpc_result_pool,
      progress_controller);

  cold_stream_.reset(svr,
      rpc,
      fs_pool,
      svr_finder,
      heartbeater,
      stream_worker,
      rpc_result_pool,
      progress_controller);
}

int SvrStream::dispatch(PartFetchCtx &task)
{
  int ret = OB_SUCCESS;
  FetchStreamType stype = task.get_fetch_stream_type();

  if (FETCH_STREAM_TYPE_HOT == stype) {
    if (OB_FAIL(hot_stream_.dispatch(task))) {
      LOG_ERROR("dispatch fetch task to fetch stream container fail", KR(ret), K(hot_stream_),
          K(task));
    }
  } else if (FETCH_STREAM_TYPE_COLD == stype) {
    if (OB_FAIL(cold_stream_.dispatch(task))) {
      LOG_ERROR("dispatch fetch task to fetch stream container fail", KR(ret), K(cold_stream_),
          K(task));
    }
  } else if (FETCH_STREAM_TYPE_DDL == stype) {
    if (OB_FAIL(ddl_stream_.dispatch(task))) {
      // It is required that the join must be successful and that a DDL stream can hold all the DDL partitions
      LOG_ERROR("dispatch fetch task to fetch stream container fail", KR(ret), K(ddl_stream_),
          K(task));
    }
  } else {
    LOG_ERROR("invalid stream type", K(stype), K(task));
    ret = OB_INVALID_ARGUMENT;
  }

  return ret;
}

void SvrStream::do_stat()
{
  ddl_stream_.do_stat();
  hot_stream_.do_stat();
  cold_stream_.do_stat();
}

}
}
