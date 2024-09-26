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
 * OBCDC header file
 * This file defines interface of OBCDC
 */

#ifndef  OCEANBASE_LIBOBCDC_MISS_LOG_HANDLER_H_
#define  OCEANBASE_LIBOBCDC_MISS_LOG_HANDLER_H_

#include "lib/net/ob_addr.h"
#include "logservice/palf/log_entry.h"
#include "logservice/logfetcher/ob_log_fetch_stat_info.h"     // TransStatInfo
#include "ob_log_ls_fetch_ctx.h"                              // LSFetchCtx
#include "ob_cdc_part_trans_resolver.h"                       // MissingLogInfo
#include "ob_log_fetch_log_rpc.h"

namespace oceanbase
{
using namespace palf;
using namespace common;
using namespace logfetcher;
using namespace obrpc;

namespace libobcdc
{

struct MissLogTask
{
public:
  MissLogTask(
      const common::ObAddr &orig_svr,
      IObLogRpc &rpc,
      LSFetchCtx &ls_fetch_ctx,
      IObCDCPartTransResolver::MissingLogInfo &missing_info,
      logfetcher::TransStatInfo &tsi);
  ~MissLogTask() { reset(); }

  void reset();
public:
  uint64_t get_tenant_id() const
  { return ls_fetch_ctx_.get_tls_id().get_tenant_id(); }

  const PartTransID &get_part_trans_id() const
  { return missing_info_.get_part_trans_id(); }

  int64_t get_handle_progress() const { return missing_info_.get_last_misslog_progress(); }

  int try_change_server(const int64_t timeout, volatile bool &stop_flag);

  TO_STRING_KV(K_(missing_info), K_(svr), K_(ls_fetch_ctx));
public:
  common::ObAddr svr_;
  IObLogRpc &rpc_; // rpc client
  LSFetchCtx &ls_fetch_ctx_;

  IObCDCPartTransResolver::MissingLogInfo &missing_info_;
  bool need_change_server_;
  logfetcher::TransStatInfo &tsi_;
};

// NOTICE: SINGLETON TOOL TO HANDLE MISSLOG. NOT THREAD-SAFE. DO NOT ADD FIELD IN ObCDCMissLogHandler
class ObCDCMissLogHandler
{
public:
  static ObCDCMissLogHandler &get_instance();
  ~ObCDCMissLogHandler() {}
public:
  int handle_log_miss(
      const common::ObAddr &cur_svr,
      IObLogRpc *rpc,
      LSFetchCtx &ls_fetch_ctx,
      IObCDCPartTransResolver::MissingLogInfo &missing_info,
      logfetcher::TransStatInfo &tsi,
      volatile bool &stop_flag);
  static void configure(const ObLogConfig &config);
private:
  static int64_t g_rpc_timeout;
  static const int64_t RETRY_LOG_PRINT_INTERVAL;
  static const int64_t RETRY_TIMEOUT;
  static const int64_t MAX_RPC_TIMEOUT;
private:
  int handle_miss_log_task_(MissLogTask &misslog_task, volatile bool &stop_flag);
  int handle_miss_record_or_state_log_(
      MissLogTask &misslog_task,
      FetchLogSRpc &fetch_log_srpc,
      volatile bool &stop_flag);
  int handle_miss_redo_log_(
      MissLogTask &misslog_task,
      FetchLogSRpc &fetch_log_srpc,
      volatile bool &stop_flag);
  // split all miss_logs by batch
  int build_batch_misslog_lsn_arr_(
      const int64_t fetched_log_idx,
      IObCDCPartTransResolver::MissingLogInfo &missing_log_info,
      ObIArray<obrpc::ObCdcLSFetchMissLogReq::MissLogParam> &batched_misslog_lsn_arr);
  // read batched misslog
  int read_batch_misslog_(
      LSFetchCtx &ls_fetch_ctx,
      const obrpc::ObCdcLSFetchLogResp &resp,
      int64_t &fetched_missing_log_cnt,
      logfetcher::TransStatInfo &tsi,
      IObCDCPartTransResolver::MissingLogInfo &missing_info);
  int alloc_fetch_log_srpc_(const uint64_t tenant_id, FetchLogSRpc *&fetch_log_srpc);
  void free_fetch_log_srpc_(FetchLogSRpc *fetch_log_srpc);

  int fetch_miss_log_with_retry_(
      MissLogTask &misslog_task,
      const ObIArray<obrpc::ObCdcLSFetchMissLogReq::MissLogParam> &miss_log_array,
      FetchLogSRpc &fetch_srpc,
      volatile bool &stop_flag);
  int fetch_miss_log_(
      const ObIArray<obrpc::ObCdcLSFetchMissLogReq::MissLogParam> &miss_log_array,
      const int64_t timeout,
      MissLogTask &misslog_task,
      FetchLogSRpc &fetch_srpc,
      volatile bool &stop_flag);
  int fetch_miss_log_direct_(
      const ObIArray<obrpc::ObCdcLSFetchMissLogReq::MissLogParam> &miss_log_array,
      const int64_t timeout,
      FetchLogSRpc &fetch_log_srpc,
      LSFetchCtx &ls_fetch_ctx);
  void check_feedback_(
      const obrpc::ObCdcLSFetchLogResp &resp,
      bool &has_valid_feedback,
      bool &rpc_fatal_err);
private:
  ObCDCMissLogHandler() {}
  DISABLE_COPY_ASSIGN(ObCDCMissLogHandler);
};

} // namespace libobcdc
} // namespace oceanbase

#endif
