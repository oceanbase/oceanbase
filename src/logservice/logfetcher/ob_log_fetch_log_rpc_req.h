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
#ifndef OCEANBAE_LOG_FETCH_LOG_RPC_REQ_H_
#define OCEANBAE_LOG_FETCH_LOG_RPC_REQ_H_

#include "logservice/cdcservice/ob_cdc_req.h"
#include "logservice/cdcservice/ob_cdc_rpc_proxy.h"
#include "ob_log_fetch_log_rpc_stop_reason.h"
#include "ob_log_utils.h"
#include <cstdint>

namespace oceanbase
{

namespace logfetcher
{

class FetchLogARpc;
class LogGroupEntryRpcRequest;
class LogFileDataBuffer;

struct FetchLogRpcReq
{
  FetchLogRpcReq(const obrpc::ObCdcFetchLogProtocolType proto,
      FetchLogARpc &host,
      const int64_t rpc_timeout):
      host_(host),
      proto_type_(proto),
      rpc_timeout_(rpc_timeout),
      trace_id_(),
      next_(NULL),
      force_stop_flag_(false),
      rpc_start_time_(OB_INVALID_TIMESTAMP),
      rpc_is_flying_(false) {}

  virtual ~FetchLogRpcReq() = 0;

  obrpc::ObCdcFetchLogProtocolType get_proto_type() const {
    return proto_type_;
  }

  // Marking RPC run status
  void mark_flying_state(const bool rpc_is_flying) {
    ATOMIC_SET(&rpc_is_flying_, rpc_is_flying);
  }

  // Mark the stop of RPC
  // Request the next round of RPC to stop
  // make sure the lock of host_ is held when calling this method;
  void mark_stop_flag() { ATOMIC_STORE(&force_stop_flag_, true); }

  const common::ObCurTraceId::TraceId &get_trace_id() const { return trace_id_; }
  int64_t get_rpc_start_time() const { return rpc_start_time_; }

  // make sure the lock of host_ is held when calling this method;
  bool rpc_is_flying() const { return ATOMIC_LOAD(&rpc_is_flying_); }
  bool get_stop_flag() const { return ATOMIC_LOAD(&force_stop_flag_); }

  DECLARE_PURE_VIRTUAL_TO_STRING;


  // Invariant member variables within the request
  FetchLogARpc                    &host_;
  obrpc::ObCdcFetchLogProtocolType proto_type_;
  // Variables that change with the request
  int64_t                         rpc_timeout_;     // RPC timeout
  common::ObCurTraceId::TraceId   trace_id_;
  FetchLogRpcReq                  *next_;           // Link list Structure

  // Forced stop flag
  // The life of this flag is consistent with the RPC, and the flag is to be reset before each round of RPC starts
  bool                            force_stop_flag_ CACHE_ALIGNED;

  //  Start time of this RPC
  int64_t                         rpc_start_time_ CACHE_ALIGNED;

  // Whether the RPC is being executed and no callback has been executed yet
  volatile bool                   rpc_is_flying_ CACHE_ALIGNED;

private:
  DISALLOW_COPY_AND_ASSIGN(FetchLogRpcReq);
};

typedef obrpc::ObCdcProxy::AsyncCB<obrpc::OB_LS_FETCH_LOG2> LogGroupEntryRpcCBBase;
class LogGroupEntryRpcCB : public LogGroupEntryRpcCBBase
{
public:
  explicit LogGroupEntryRpcCB(LogGroupEntryRpcRequest &host) : host_(host) {}
  virtual ~LogGroupEntryRpcCB() { result_.reset(); }

public:
  rpc::frame::ObReqTransport::AsyncCB *clone(const rpc::frame::SPAlloc &alloc) const;
  int process();
  void on_timeout();
  void on_invalid();
  typedef typename obrpc::ObCdcProxy::ObRpc<obrpc::OB_LS_FETCH_LOG2> ProxyRpc;
  void set_args(const typename ProxyRpc::Request &args) { UNUSED(args); }

  TO_STRING_KV("host", reinterpret_cast<void *>(&host_));

private:
  int do_process_(const obrpc::ObRpcResultCode &rcode, const obrpc::ObCdcLSFetchLogResp *resp);

private:
  LogGroupEntryRpcRequest &host_;

private:
  DISALLOW_COPY_AND_ASSIGN(LogGroupEntryRpcCB);
};

////////////////////////////// LogGroupEntryRpcRequest //////////////////////////////
// RPC request structure
// Each RPC request corresponds to a stream identifier uniquely
struct LogGroupEntryRpcRequest: public FetchLogRpcReq
{
  LogGroupEntryRpcCB              cb_;              // RPC callback
  obrpc::ObCdcLSFetchLogReq       req_;             // Fetch log request

  LogGroupEntryRpcRequest(FetchLogARpc &host,
      const share::ObLSID &ls_id,
      const int64_t rpc_timeout);
  virtual ~LogGroupEntryRpcRequest() {}

  // Update request parameters
  void update_request(const int64_t upper_limit,
      const int64_t rpc_timeout);

  // Prepare the RPC request structure, set the final parameters, and immediately launch the RPC request next
  int prepare(
      const palf::LSN &req_start_lsn,
      const int64_t upper_limit,
      const int64_t progress);

  int64_t get_upper_limit() const { return req_.get_upper_limit_ts(); }

  TO_STRING_KV(K_(rpc_is_flying),
      "rpc_start_time", TS_TO_STR(rpc_start_time_),
      K_(force_stop_flag),
      K_(rpc_timeout),
      K_(req),
      K_(trace_id),
      KP_(next));

private:
  DISALLOW_COPY_AND_ASSIGN(LogGroupEntryRpcRequest);
};

////////////////////////////// RawLogRpcRequest //////////////////////////////
struct RawLogDataRpcRequest;

typedef obrpc::ObCdcProxy::AsyncCB<obrpc::OB_CDC_FETCH_RAW_LOG> RawLogDataRpcCBBase;
class RawLogDataRpcCB : public RawLogDataRpcCBBase
{
public:
  explicit RawLogDataRpcCB(RawLogDataRpcRequest &host) : host_(host) {}
  virtual ~RawLogDataRpcCB() { result_.reset(); }

public:
  rpc::frame::ObReqTransport::AsyncCB *clone(const rpc::frame::SPAlloc &alloc) const;
  int process();
  void on_timeout();
  void on_invalid();
  typedef typename obrpc::ObCdcProxy::ObRpc<obrpc::OB_CDC_FETCH_RAW_LOG> ProxyRpc;
  void set_args(const typename ProxyRpc::Request &args) { UNUSED(args); }

  TO_STRING_KV("host", reinterpret_cast<void *>(&host_));

private:
  int do_process_(const obrpc::ObRpcResultCode &rcode, const obrpc::ObCdcFetchRawLogResp *resp);

private:
  RawLogDataRpcRequest &host_;

private:
  DISALLOW_COPY_AND_ASSIGN(RawLogDataRpcCB);
};

struct RawLogFileRpcRequest: public FetchLogRpcReq
{
  typedef ObDList<RawLogDataRpcRequest> RawLogDataRpcReqList;
  // ceiling
  static constexpr int64_t MAX_SEND_REQ_CNT =
      (palf::PALF_BLOCK_SIZE + obrpc::ObCdcFetchRawLogResp::FETCH_BUF_LEN - 1) / obrpc::ObCdcFetchRawLogResp::FETCH_BUF_LEN;
  RawLogFileRpcRequest(FetchLogARpc &host,
      const share::ObLSID &ls_id,
      const int64_t rpc_timeout):
      FetchLogRpcReq(obrpc::ObCdcFetchLogProtocolType::RawLogDataProto, host, rpc_timeout),
      free_list_(),
      busy_list_(),
      buffer_(nullptr),
      ls_id_(ls_id),
      self_tenant_id_(OB_INVALID_TENANT_ID),
      start_lsn_(),
      progress_(OB_INVALID_TIMESTAMP),
      prepare_end_time_(OB_INVALID_TIMESTAMP),
      sub_rpc_req_count_(0),
      complete_count_(0),
      failed_count_(0),
      flying_count_(0) { }
  virtual ~RawLogFileRpcRequest() { destroy(); }

  // make sure the lock of host_ is held when calling this method;
  void destroy();

  // make sure the lock of host_ is held when calling this method;
  int prepare(const uint64_t self_tenant_id,
      const palf::LSN &start_lsn,
      const int64_t progress);

  int complete_request();

  int handle_sub_rpc_response(RawLogDataRpcRequest &rpc_req,
      const obrpc::ObRpcResultCode &rcode,
      const obrpc::ObCdcFetchRawLogResp *resp,
      const bool need_lock);

  // make sure the lock of host_ is held when calling this method;
  int alloc_sub_rpc_request(RawLogDataRpcRequest *&req);

  int mark_sub_rpc_req_busy(RawLogDataRpcRequest *req);

  // make sure the lock of host_ is held when calling this method;
  int free_sub_rpc_request(RawLogDataRpcRequest *req);

  int free_requests_in_free_list();

  int revert_request_in_busy_list();

  VIRTUAL_TO_STRING_KV(K(rpc_is_flying_),
    "rpc_start_time", TS_TO_STR(rpc_start_time_),
    K(force_stop_flag_),
    K(rpc_timeout_),
    K(trace_id_),
    KP(buffer_),
    K(ls_id_),
    K(self_tenant_id_),
    K(start_lsn_),
    K(progress_),
    K(sub_rpc_req_count_),
    K(complete_count_),
    K(failed_count_),
    K(flying_count_),
    KP(next_));

  // we can use DLIST_FOREACH to iterate through it.
  RawLogDataRpcReqList free_list_;
  RawLogDataRpcReqList busy_list_;
  LogFileDataBuffer *buffer_;
  share::ObLSID ls_id_;
  uint64_t self_tenant_id_;
  palf::LSN start_lsn_;
  int64_t progress_;
  int64_t prepare_end_time_;
  int32_t sub_rpc_req_count_;
  int32_t complete_count_;
  int32_t failed_count_;
  int32_t flying_count_;
private:
  int free_all_sub_rpc_request_();
};

struct RawLogDataRpcRequest: public ObDLinkBase<RawLogDataRpcRequest>
{
  RawLogDataRpcRequest(RawLogFileRpcRequest &host):
      host_(host),
      rpc_prepare_time_(OB_INVALID_TIMESTAMP),
      rpc_is_flying_(false),
      cb_(*this),
      req_()
      { }
  ~RawLogDataRpcRequest() { }

  int prepare(const share::ObLSID &ls_id,
              const palf::LSN &start_lsn,
              const int64_t req_size,
              const int64_t progress,
              const int64_t file_id,
              const int32_t seq_no,
              const int32_t cur_round_rpc_count);

  void reset();

  TO_STRING_KV(
    K(host_),
    K(rpc_prepare_time_),
    K(rpc_is_flying_),
    K(cb_),
    K(req_)
  );

  RawLogFileRpcRequest &host_;
  int64_t rpc_prepare_time_;
  bool rpc_is_flying_;
  RawLogDataRpcCB cb_;
  obrpc::ObCdcFetchRawLogReq req_;
};

////////////////////////////// RpcRequestList //////////////////////////////
// RPC request list
struct RpcRequestList
{
  FetchLogRpcReq  *head_;
  FetchLogRpcReq  *tail_;
  int64_t     count_;

  RpcRequestList() { reset(); }

  void reset()
  {
    head_ = NULL;
    tail_ = NULL;
    count_ = 0;
  }

  void add(FetchLogRpcReq *req);

  // Use the stream identifier to delete the request structure
  int remove(FetchLogRpcReq *target);

  TO_STRING_KV(K(count_), KP(head_), KP(tail_));
};

}

}

#endif