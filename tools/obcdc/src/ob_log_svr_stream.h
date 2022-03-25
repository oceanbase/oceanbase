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

#ifndef OCEANBASE_LIBOBLOG_OB_LOG_SVR_STREAM_H__
#define OCEANBASE_LIBOBLOG_OB_LOG_SVR_STREAM_H__

#include "lib/net/ob_addr.h"                // ObAddr

#include "ob_log_fetch_stream_container.h"  // FetchStreamContainer

namespace oceanbase
{
namespace liboblog
{
class IFetchStreamPool;
class IObLogRpc;
class IObLogStreamWorker;
class IFetchLogARpcResultPool;
class IObLogSvrFinder;
class IObLogFetcherHeartbeatWorker;
class PartProgressController;
class PartFetchCtx;

class SvrStream
{
public:
  SvrStream();
  virtual ~SvrStream();

public:
  void reset();
  void reset(const common::ObAddr &svr,
      IObLogRpc &rpc,
      IFetchStreamPool &fs_pool,
      IObLogSvrFinder &svr_finder,
      IObLogFetcherHeartbeatWorker &heartbeater,
      IObLogStreamWorker &stream_worker,
      IFetchLogARpcResultPool &rpc_result_pool,
      PartProgressController &progress_controller);
  int dispatch(PartFetchCtx &task);

  void do_stat();

  TO_STRING_KV(K_(svr),
      K_(ddl_stream),
      K_(hot_stream),
      K_(cold_stream));

private:
  common::ObAddr        svr_;
  FetchStreamContainer  ddl_stream_;
  FetchStreamContainer  hot_stream_;
  FetchStreamContainer  cold_stream_;

private:
  DISALLOW_COPY_AND_ASSIGN(SvrStream);
};

}
}

#endif
