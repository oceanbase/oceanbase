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

#define USING_LOG_PREFIX OBLOG_FETCHER

#include "ob_log_fetch_log_rpc_result.h"
#include "ob_log_file_buffer_pool.h"


namespace oceanbase
{
namespace logfetcher
{

FetchLogRpcResult::~FetchLogRpcResult() {}

////////////////////////////// RawLogFileRpcResult //////////////////////////////

void RawLogFileRpcResult::reset()
{
  FetchLogRpcResult::reset();
  if (nullptr != data_buffer_) {
    data_buffer_->revert();
    data_buffer_ = nullptr;
  }
  data_ = nullptr;
  data_len_ = 0;
  is_readable_ = false;
  is_active_ = false;
  data_end_source_ = obrpc::ObCdcFetchRawSource::UNKNOWN;
  replayable_point_.reset();
  sub_rpc_status_.reset();
  rpc_prepare_time_ = 0;
}

int RawLogFileRpcResult::generate_result_from_data(LogFileDataBuffer *&buffer)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buffer)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("buffer is null, invalid argument", K(buffer));
  } else {
    buffer->deliver_owner(this);
    data_buffer_ = buffer;
    buffer = nullptr;
    if (OB_FAIL(data_buffer_->get_data(data_, data_len_, valid_rpc_cnt_, is_readable_,
        is_active_, data_end_source_, replayable_point_, sub_rpc_status_))) {
      LOG_ERROR("failed to get data from buffer", KPC(buffer), KPC(this));
    }
  }

  return ret;
}

////////////////////////////// LogGroupEntryRpcResult //////////////////////////////

int LogGroupEntryRpcResult::set(const obrpc::ObRpcResultCode &rcode,
    const obrpc::ObCdcLSFetchLogResp *resp,
    const common::ObCurTraceId::TraceId &trace_id,
    const int64_t rpc_start_time,
    const int64_t rpc_callback_start_time,
    const bool rpc_stop_upon_result,
    const RpcStopReason rpc_stop_reason)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(OB_SUCCESS == rcode.rcode_ && NULL == resp)) {
    LOG_ERROR("invalid fetch log response", K(rcode), K(resp));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(rpc_start_time <= 0) || OB_UNLIKELY(rpc_callback_start_time <= 0)) {
    LOG_ERROR("invalid argument", K(rpc_start_time), K(rpc_callback_start_time));
    ret = OB_INVALID_ARGUMENT;
  } else {
    rcode_ = rcode;
    trace_id_ = trace_id;

    // The result is valid when no error occurs
    if (OB_SUCCESS == rcode.rcode_) {
      if (OB_FAIL(resp_.assign(*resp))) {
        LOG_ERROR("assign new fetch log resp fail", KR(ret), KPC(resp), K(resp_));
      }
    } else {
      resp_.reset();
    }
  }

  // After setting all the result items, only then start setting the statistics items,
  // because the results need a memory copy and this time must be considered
  if (OB_SUCCESS == ret) {
    int64_t rpc_end_time = get_timestamp();

    rpc_time_ = rpc_end_time - rpc_start_time;
    rpc_callback_time_ = rpc_end_time - rpc_callback_start_time;
    rpc_stop_upon_result_ = rpc_stop_upon_result;
    rpc_stop_reason_ = rpc_stop_reason;
  }

  return ret;
}

////////////////////////////// FetchLogARpcResult Object Pool //////////////////////////////

int FetchLogRpcResultPool::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("init twice");
  } else {
    alloc_.set_attr(lib::ObMemAttr(tenant_id, ObModIds::OB_LOG_FETCH_LOG_ARPC_RESULT));
    alloc_count_ = 0;
    inited_ = true;
    LOG_INFO("FetchLogARpcResultPool init succ", K(tenant_id));
  }

  return ret;
}

void FetchLogRpcResultPool::destroy()
{
  inited_ = false;
}

void FetchLogRpcResultPool::print_stat()
{
  _LOG_INFO("[STAT] [RPC_RESULT_POOL] ALLOC=%ld", alloc_count_);
}

ERRSIM_POINT_DEF(ALLOC_FETCH_LOG_ARPC_RESULT_FAIL)
}
}