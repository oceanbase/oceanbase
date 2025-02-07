/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 *
 * Fetching log-related RPC implementation
 */

#ifndef OCEANBASE_LOG_FETCH_LOG_RPC_RESULT_H_
#define OCEANBASE_LOG_FETCH_LOG_RPC_RESULT_H_

#include "lib/objectpool/ob_small_obj_pool.h"
#include "logservice/cdcservice/ob_cdc_raw_log_req.h"
#include "logservice/cdcservice/ob_cdc_req.h"
#include "rpc/obrpc/ob_rpc_result_code.h"
#include "ob_log_fetch_log_rpc_stop_reason.h"

namespace oceanbase
{
namespace logfetcher
{

class LogFileDataBuffer;

////////////////////////////// FetchLog Request Result //////////////////////////////

struct FetchLogRpcResult
{
  FetchLogRpcResult(const obrpc::ObCdcFetchLogProtocolType type):
    type_(type),
    trace_id_(),
    rpc_time_(0),
    rpc_callback_time_(0),
    rpc_stop_upon_result_(false),
    rpc_stop_reason_(RpcStopReason::INVALID_REASON) {}
  virtual ~FetchLogRpcResult() = 0;

  DECLARE_PURE_VIRTUAL_TO_STRING;

  virtual void reset() {
    trace_id_.reset();
    rpc_time_ = 0;
    rpc_callback_time_ = 0;
    rpc_stop_upon_result_ = false;
    rpc_stop_reason_ = RpcStopReason::INVALID_REASON;
  }

  obrpc::ObCdcFetchLogProtocolType type_;
  common::ObCurTraceId::TraceId   trace_id_;

  // Statistical items
  // The time spent on the server side is in the fetch log result
  int64_t                         rpc_time_;              // Total RPC time: network + server + asynchronous processing
  int64_t                         rpc_callback_time_;     // RPC asynchronous processing time
  bool                            rpc_stop_upon_result_;  // Whether the RPC stops after the result is processed, i.e. whether it stops because of that result
  RpcStopReason                   rpc_stop_reason_;       // RPC stop reason
};

////////////////////////////// RawLogFile Request Result //////////////////////////////

struct RawLogDataRpcStatus
{
  RawLogDataRpcStatus():
    rcode_(),
    err_(OB_SUCCESS),
    feed_back_(obrpc::FeedbackType::INVALID_FEEDBACK),
    fetch_status_(),
    rpc_callback_time_(0),
    rpc_time_(0) {}

  RawLogDataRpcStatus(const obrpc::ObRpcResultCode &rcode,
      const int err,
      const obrpc::FeedbackType feedback,
      const obrpc::ObCdcFetchRawStatus &status,
      const int64_t rpc_callback_time,
      const int64_t rpc_time):
      rcode_(rcode),
      err_(err),
      feed_back_(feedback),
      fetch_status_(status),
      rpc_callback_time_(rpc_callback_time),
      rpc_time_(rpc_time) {}

  void reset() {
    rcode_.reset();
    err_ = OB_SUCCESS;
    feed_back_ = obrpc::FeedbackType::INVALID_FEEDBACK;
    fetch_status_.reset();
    rpc_callback_time_ = 0;
    rpc_time_ = 0;
  }

  void reset(const obrpc::ObRpcResultCode &rcode,
      const int err,
      const obrpc::FeedbackType feedback,
      const obrpc::ObCdcFetchRawStatus &status,
      const int64_t rpc_callback_time,
      const int64_t rpc_time)
  {
    rcode_ = rcode;
    err_ = err;
    feed_back_ = feedback;
    fetch_status_ = status;
    rpc_callback_time_ = rpc_callback_time;
    rpc_time_ = rpc_time;
  }

  TO_STRING_KV(
    K(rcode_),
    K(err_),
    K(feed_back_),
    K(fetch_status_),
    K(rpc_callback_time_),
    K(rpc_time_)
  );

  obrpc::ObRpcResultCode rcode_;
  int err_;
  obrpc::FeedbackType feed_back_;
  obrpc::ObCdcFetchRawStatus fetch_status_;
  int64_t rpc_callback_time_;
  int64_t rpc_time_;
};

struct RawLogFileRpcResult: public FetchLogRpcResult
{
  RawLogFileRpcResult():
      FetchLogRpcResult(obrpc::ObCdcFetchLogProtocolType::RawLogDataProto),
      data_buffer_(nullptr),
      data_(nullptr),
      data_len_(0),
      is_readable_(false),
      is_active_(false),
      data_end_source_(obrpc::ObCdcFetchRawSource::UNKNOWN),
      replayable_point_(),
      sub_rpc_status_(),
      rpc_prepare_time_()
      {}

  virtual ~RawLogFileRpcResult() { reset(); }

  virtual void reset();

  int generate_result_from_data(LogFileDataBuffer *&buffer);

  VIRTUAL_TO_STRING_KV(
    KPC(data_buffer_),
    KP(data_),
    K(data_len_),
    K(valid_rpc_cnt_),
    K(is_readable_),
    K(is_active_),
    K(data_end_source_),
    K(replayable_point_),
    K(sub_rpc_status_),
    K(rpc_prepare_time_)
  );

  LogFileDataBuffer *data_buffer_;
  const char *data_;
  int64_t data_len_;
  int32_t valid_rpc_cnt_;
  bool is_readable_;
  bool is_active_;
  obrpc::ObCdcFetchRawSource data_end_source_;
  share::SCN replayable_point_;
  ObSEArray<RawLogDataRpcStatus, 4> sub_rpc_status_;
  int64_t rpc_prepare_time_;
};

////////////////////////////// LogGroupEntry Request Result //////////////////////////////
struct LogGroupEntryRpcResult: public FetchLogRpcResult
{
  obrpc::ObCdcLSFetchLogResp      resp_;    // Fetch log response
  obrpc::ObRpcResultCode          rcode_;   // Fetch log result

  LogGroupEntryRpcResult(): FetchLogRpcResult(obrpc::ObCdcFetchLogProtocolType::LogGroupEntryProto) { reset(); }
  virtual ~LogGroupEntryRpcResult() {}

  int set(const obrpc::ObRpcResultCode &rcode,
      const obrpc::ObCdcLSFetchLogResp *resp,
      const common::ObCurTraceId::TraceId &trace_id,
      const int64_t rpc_start_time,
      const int64_t rpc_callback_start_time,
      const bool rpc_stop_upon_result,
      const RpcStopReason rpc_stop_reason);

  virtual void reset()
  {
    FetchLogRpcResult::reset();
    resp_.reset();
    rcode_.reset();
  }

  TO_STRING_KV(K_(rcode), K_(resp), K_(trace_id), K_(rpc_time),
      K_(rpc_callback_time), K_(rpc_stop_upon_result),
      "rpc_stop_reason", print_rpc_stop_reason(rpc_stop_reason_),
      K(resp_));
};

////////////////////////////// LogGroupEntryRpcResultPool //////////////////////////////
class FetchLogRpcResultPool
{
  typedef common::ObSmallObjPool<LogGroupEntryRpcResult> LogGroupEntryResultPool;
  typedef common::ObSmallObjPool<RawLogFileRpcResult> RawLogFileRpcResultPool;
  // 16M
public:
  static const int64_t DEFAULT_RESULT_POOL_BLOCK_SIZE = 18L << 20;

public:
  FetchLogRpcResultPool() : inited_(false), alloc_count_(0), alloc_() {}
  virtual ~FetchLogRpcResultPool() { destroy(); }

public:
  int init(const uint64_t tenant_id);
  void destroy();
  void print_stat();

public:
  template <typename T>
  int alloc(T *&result)
  {
    static_assert(std::is_base_of<FetchLogRpcResult, T>::value, "alloc type not matched");
    int ret = OB_SUCCESS;
    void *ptr = nullptr;
    if (OB_UNLIKELY(! inited_)) {
      ret = OB_NOT_INIT;
    } else if (OB_ISNULL(ptr = alloc_.alloc(sizeof(T)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      ATOMIC_INC(&alloc_count_);
      result = new (ptr)T;
    }
    return ret;
  }
  template <typename T>
  void free(T *result)
  {
    static_assert(std::is_base_of<FetchLogRpcResult, T>::value, "free type not matched");
    if (inited_ && OB_NOT_NULL(result)) {
      result->~T();
      alloc_.free(result);
      ATOMIC_DEC(&alloc_count_);
    }
  }

private:
  bool     inited_;
  int64_t  alloc_count_ CACHE_ALIGNED;
  ObMalloc alloc_;
private:
  DISALLOW_COPY_AND_ASSIGN(FetchLogRpcResultPool);
};

}
}

#endif