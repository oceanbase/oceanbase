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

#ifndef OCEANBASE_CLOG_OB_LOG_EXTERNAL_RPC_
#define OCEANBASE_CLOG_OB_LOG_EXTERNAL_RPC_

#include "share/ob_define.h"
#include "lib/container/ob_se_array.h"
#include "common/ob_partition_key.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "rpc/obrpc/ob_rpc_processor.h"
#include "ob_log_define.h"
#include "ob_log_entry.h"
#include "share/config/ob_server_config.h"
#include "observer/ob_server_struct.h"

namespace oceanbase {
namespace storage {
class ObPartitionService;
}

namespace clog {
class ObICLogMgr;
}

namespace obrpc {
// Rpc interface for Liboblog.
class ObLogReqStartLogIdByTsRequest;
class ObLogReqStartLogIdByTsResponse;
class ObLogReqStartPosByLogIdRequest;
class ObLogReqStartPosByLogIdResponse;
class ObLogExternalFetchLogRequest;
class ObLogExternalFetchLogResponse;
class ObLogReqHeartbeatInfoRequest;
class ObLogReqHeartbeatInfoResponse;

// for request with breakpoint
class ObLogReqStartLogIdByTsRequestWithBreakpoint;
class ObLogReqStartLogIdByTsResponseWithBreakpoint;
class ObLogReqStartPosByLogIdRequestWithBreakpoint;
class ObLogReqStartPosByLogIdResponseWithBreakpoint;

class ObLogOpenStreamReq;
class ObLogOpenStreamResp;

class ObLogStreamFetchLogReq;
class ObLogStreamFetchLogResp;

class ObLogLeaderHeartbeatReq;
class ObLogLeaderHeartbeatResp;

class ObLogExternalProxy : public ObRpcProxy {
public:
  DEFINE_TO(ObLogExternalProxy);

  // req_start_log_id_by_ts()
  RPC_S(@PR5 req_start_log_id_by_ts, OB_LOG_REQ_START_LOG_ID_BY_TS, (ObLogReqStartLogIdByTsRequest),
      ObLogReqStartLogIdByTsResponse);

  // req_start_pos_by_log_id()
  RPC_S(@PR5 req_start_pos_by_log_id, OB_LOG_REQ_START_POS_BY_LOG_ID, (ObLogReqStartPosByLogIdRequest),
      ObLogReqStartPosByLogIdResponse);

  // fetch_log()
  RPC_S(@PR5 fetch_log, OB_LOG_FETCH_LOG_EXTERNAL, (ObLogExternalFetchLogRequest), ObLogExternalFetchLogResponse);

  // req_last_log_serv_info()
  RPC_S(@PR5 req_heartbeat_info, OB_LOG_REQUEST_HEARTBEAT_INFO, (ObLogReqHeartbeatInfoRequest),
      ObLogReqHeartbeatInfoResponse);

  // for request with breakpoint
  RPC_S(@PR5 req_start_log_id_by_ts_with_breakpoint, OB_LOG_REQ_START_LOG_ID_BY_TS_WITH_BREAKPOINT,
      (ObLogReqStartLogIdByTsRequestWithBreakpoint), ObLogReqStartLogIdByTsResponseWithBreakpoint);

  RPC_S(@PR5 req_start_pos_by_log_id_with_breakpoint, OB_LOG_REQ_START_POS_BY_LOG_ID_WITH_BREAKPOINT,
      (ObLogReqStartPosByLogIdRequestWithBreakpoint), ObLogReqStartPosByLogIdResponseWithBreakpoint);

  RPC_S(@PR5 open_stream, OB_LOG_OPEN_STREAM, (ObLogOpenStreamReq), ObLogOpenStreamResp);

  RPC_S(@PR5 leader_heartbeat, OB_LOG_LEADER_HEARTBEAT, (ObLogLeaderHeartbeatReq), ObLogLeaderHeartbeatResp);

  RPC_AP(@PR5 async_stream_fetch_log, OB_LOG_STREAM_FETCH_LOG, (ObLogStreamFetchLogReq), ObLogStreamFetchLogResp);
};

///------------- old rpc -----------------------

// Request start log id by start timestamp.
//  Partition Err:
//   - OB_SUCCESS
//   - OB_PARTITION_NOT_EXIST: partition doesn't exist
//   - OB_NO_LOG: partition exists without any log
//   - OB_ERR_OUT_OF_LOWER_BOUND: timestamp beyond lower bound
//   - OB_ERR_OUT_OF_UPPER_BOUND: timestamp beyond upper bound
//   - OB_RESULT_UNKNOWN: timestamp in log data hole
//  Rpc Err:
//   - OB_SUCCESS
//   - OB_ERR_SYS: observer internal error
class ObLogReqStartLogIdByTsRequest {
  static const int64_t CUR_RPC_VER = 1;
  static const int64_t ITEM_CNT_LMT = 10000;  // Around 400kb for cur version.
public:
  struct Param {
    common::ObPartitionKey pkey_;
    int64_t start_tstamp_;
    void reset();
    TO_STRING_KV(K_(pkey), K_(start_tstamp));
    OB_UNIS_VERSION(1);
  };
  typedef common::ObSEArray<Param, 16> ParamArray;

public:
  ObLogReqStartLogIdByTsRequest();
  ~ObLogReqStartLogIdByTsRequest();

public:
  void reset();
  bool is_valid() const;
  int set_params(const ParamArray& params);
  int append_param(const Param& param);
  const ParamArray& get_params() const;
  // Backward compatible.
  int64_t rpc_ver() const;
  TO_STRING_KV(K_(rpc_ver), K_(params));
  OB_UNIS_VERSION(1);

private:
  int64_t rpc_ver_;
  ParamArray params_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogReqStartLogIdByTsRequest);
};

class ObLogReqStartLogIdByTsResponse {
  static const int64_t CUR_RPC_VER = 1;
  static const int64_t ITEM_CNT_LMT = 10000;  // Around 400kb for cur version.
public:
  // Stored in the same order as Request's params.
  struct Result {
    // Partition specific error code.
    int err_;
    uint64_t start_log_id_;
    bool predict_;  // start_log_id is not written and will be written
    void reset();
    TO_STRING_KV(K_(err), K_(start_log_id), K_(predict));
    OB_UNIS_VERSION(1);
  };
  typedef common::ObSEArray<Result, 16> ResultArray;

public:
  ObLogReqStartLogIdByTsResponse();
  ~ObLogReqStartLogIdByTsResponse();

public:
  void reset();
  void set_err(const int err);
  int set_results(const ResultArray& results);
  int append_result(const Result& result);
  int get_err() const;
  const ResultArray& get_results() const;
  int64_t rpc_ver() const;
  void set_rpc_ver(const int64_t ver);
  TO_STRING_KV(K_(rpc_ver), K_(err), K_(res));
  OB_UNIS_VERSION(1);

private:
  int64_t rpc_ver_;
  int err_;
  ResultArray res_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogReqStartLogIdByTsResponse);
};

class ObLogReqStartLogIdByTsProcessor
    : public ObRpcProcessor<ObLogExternalProxy::ObRpc<OB_LOG_REQ_START_LOG_ID_BY_TS> > {
public:
  ObLogReqStartLogIdByTsProcessor(storage::ObPartitionService* partition_service)
      : partition_service_(partition_service)
  {}
  ~ObLogReqStartLogIdByTsProcessor()
  {
    partition_service_ = NULL;
  }

protected:
  int process();

private:
  storage::ObPartitionService* partition_service_;
};

// Request i-log start positions by log ids.
// Err:
//  - OB_SUCCESS
//  - OB_PARTITION_NOT_EXIST: partition doesn't exist
//  - OB_NO_LOG: partition exist, but no log found
//  - OB_LOG_ID_NOT_FOUND: partition exist, but start log id not found
//  - OB_ERR_OUT_OF_LOWER_BOUND: log id beyond lower bound
//  - OB_ERR_OUT_OF_UPPER_BOUND: log id beyond upper bound
// Rpc Err:
//  - OB_SUCCESS
//  - OB_ERR_SYS: observer internal error
// Note:
//  - On observer internal error, error code is returned by Response::err_.
//  - On partition specific error, Response::err_ is set to OB_SUCCESS,
//    partition specific error is returned by Result::err_.
class ObLogReqStartPosByLogIdRequest {
  static const int64_t CUR_RPC_VER = 1;
  static const int64_t ITEM_CNT_LMT = 10000;  // Around 400kb for cur version.
public:
  struct Param {
    common::ObPartitionKey pkey_;
    uint64_t start_log_id_;
    void reset();
    // Tostring.
    TO_STRING_KV(K_(pkey), K_(start_log_id));
    // Rpc serialize & deserialize.
    OB_UNIS_VERSION(1);
  };
  typedef common::ObSEArray<Param, 16> ParamArray;

public:
  ObLogReqStartPosByLogIdRequest();
  ~ObLogReqStartPosByLogIdRequest();

public:
  void reset();
  bool is_valid() const;
  int set_params(const ParamArray& params);
  int append_param(const Param& param);
  const ParamArray& get_params() const;
  int64_t rpc_ver() const;
  TO_STRING_KV(K_(rpc_ver), K_(params));
  OB_UNIS_VERSION(1);

private:
  int64_t rpc_ver_;
  ParamArray params_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogReqStartPosByLogIdRequest);
};

class ObLogReqStartPosByLogIdResponse {
  static const int64_t CUR_RPC_VER = 1;
  static const int64_t ITEM_CNT_LMT = 10000;  // Around 500kb for cur version.
public:
  // Stored in the same order as Request's params.
  struct Result {
    // Partition specific error code.
    int err_;
    clog::file_id_t file_id_;
    clog::offset_t offset_;
    // Valid log id range on this observer.
    uint64_t first_log_id_;
    uint64_t last_log_id_;
    void reset();
    TO_STRING_KV(K_(err), K_(file_id), K_(offset), K_(first_log_id), K_(last_log_id));
    OB_UNIS_VERSION(1);
  };
  typedef common::ObSEArray<Result, 16> ResultArray;

public:
  ObLogReqStartPosByLogIdResponse();
  ~ObLogReqStartPosByLogIdResponse();

public:
  void reset();
  void set_err(const int err);
  int set_results(const ResultArray& results);
  int append_result(const Result& result);
  int get_err() const;
  const ResultArray& get_results() const;
  int64_t rpc_ver() const;
  void set_rpc_ver(const int64_t ver);
  TO_STRING_KV(K_(rpc_ver), K_(err), K_(res));
  OB_UNIS_VERSION(1);

private:
  int64_t rpc_ver_;
  int err_;
  ResultArray res_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogReqStartPosByLogIdResponse);
};

class ObLogReqStartPosByLogIdProcessor
    : public ObRpcProcessor<ObLogExternalProxy::ObRpc<OB_LOG_REQ_START_POS_BY_LOG_ID> > {
public:
  ObLogReqStartPosByLogIdProcessor(storage::ObPartitionService* partition_service)
      : partition_service_(partition_service)
  {}
  ~ObLogReqStartPosByLogIdProcessor()
  {
    partition_service_ = NULL;
  }

protected:
  int process();

private:
  storage::ObPartitionService* partition_service_;
};

// Fetch log and return offline partitions with their last log id.
// Fetch range is [start_log_id, last_log_id].
// Err:
//  - OB_SUCCESS
//  - OB_LOG_ID_RANGE_ERROR: log id range error
//  - OB_ERR_SYS: observer internal error
class ObLogExternalFetchLogRequest {
  static const int64_t CUR_RPC_VER = 1;
  static const int64_t ITEM_CNT_LMT = 10000;  // Around 500kb for cur version.
  static const int64_t DEFAULT_OFFLINE_TIMEOUT = 5 * 1000 * 1000;

public:
  struct Param {
    common::ObPartitionKey pkey_;
    uint64_t start_log_id_;
    uint64_t last_log_id_;
    void reset();
    // Tostring.
    TO_STRING_KV(K_(pkey), K_(start_log_id), K_(last_log_id));
    // Serialize & deserialize.
    OB_UNIS_VERSION(1);
  };
  typedef common::ObSEArray<Param, 16> ParamArray;

public:
  ObLogExternalFetchLogRequest();
  ~ObLogExternalFetchLogRequest();

public:
  void reset();
  bool is_valid() const;
  int set_params(const ParamArray& params);
  int append_param(const Param& param);
  int set_file_id_offset(const clog::file_id_t& file_id, const clog::offset_t& offset);
  int set_offline_timeout(const int64_t offline_timeout);
  const ParamArray& get_params() const;
  clog::file_id_t get_file_id() const;
  clog::offset_t get_offset() const;
  int64_t get_offline_timeout() const;
  static bool is_valid_offline_timeout(const int64_t offline_timestamp);
  int64_t rpc_ver() const;
  TO_STRING_KV(K_(rpc_ver), K_(params), K_(file_id), K_(offset), K_(offline_timeout));
  OB_UNIS_VERSION(1);

private:
  static const int64_t MAX_OFFLINE_TIMEOUT = 100 * 1000 * 1000;  // 100 second
private:
  int64_t rpc_ver_;
  ParamArray params_;
  clog::file_id_t file_id_;
  clog::offset_t offset_;
  int64_t offline_timeout_;
};

class ObLogExternalFetchLogResponse {
  static const int64_t CUR_RPC_VER = 1;

public:
  struct OfflinePartition {
    common::ObPartitionKey pkey_;
    int64_t sync_ts_;
    void reset();
    TO_STRING_KV(K_(pkey), K_(sync_ts));
    OB_UNIS_VERSION(1);
  };
  typedef common::ObSEArray<OfflinePartition, 16> OfflinePartitionArray;

public:
  ObLogExternalFetchLogResponse();
  ~ObLogExternalFetchLogResponse();

public:
  void reset();
  int set_offline_partitions(const OfflinePartitionArray& offlines);
  int append_offline_partition(const common::ObPartitionKey& pkey, const int64_t sync_ts);
  int append_offline_partition(const OfflinePartition& offline);
  int set_file_id_offset(const clog::file_id_t& file_id, const clog::offset_t& offset);
  int append_log(const clog::ObLogEntry& log_entry);
  int get_err() const;
  void set_err(const int err)
  {
    err_ = err;
  }
  const OfflinePartitionArray& get_offline_partitions() const;
  clog::file_id_t get_file_id() const;
  clog::offset_t get_offset() const;
  int32_t get_log_num() const;
  int64_t get_pos() const;
  const char* get_log_entry_buf() const;
  int64_t rpc_ver() const;
  void set_rpc_ver(const int64_t ver);
  TO_STRING_KV(K_(rpc_ver), K_(err), K_(offline_partitions), K_(file_id), K_(offset), K_(log_num), K_(pos));
  OB_UNIS_VERSION(1);

private:
  int64_t offline_partition_cnt_lmt_();
  int64_t cur_ver_offline_partition_cnt_lmt_();

private:
  static const int64_t FETCH_BUF_LEN = common::OB_MAX_LOG_BUFFER_SIZE;

private:
  int64_t rpc_ver_;
  int err_;
  OfflinePartitionArray offline_partitions_;
  clog::file_id_t file_id_;
  clog::offset_t offset_;
  int32_t log_num_;
  int64_t pos_;
  char log_entry_buf_[FETCH_BUF_LEN];
};

class ObLogExternalFetchLogProcessor : public ObRpcProcessor<ObLogExternalProxy::ObRpc<OB_LOG_FETCH_LOG_EXTERNAL> > {
public:
  ObLogExternalFetchLogProcessor(storage::ObPartitionService* partition_service) : partition_service_(partition_service)
  {}
  ~ObLogExternalFetchLogProcessor()
  {
    partition_service_ = NULL;
  }

protected:
  int process();

private:
  storage::ObPartitionService* partition_service_;
};

// Request heartbeat information of given partitions.
// Err:
//  - OB_SUCCESS
//  - OB_NEED_RETRY: can't generate heartbeat info
//  - OB_ERR_SYS: observer internal error
class ObLogReqHeartbeatInfoRequest {
  static const int64_t CUR_RPC_VER = 1;
  static const int64_t ITEM_CNT_LMT = 10000;  // Around 300kb for cur version.
public:
  struct Param {
    common::ObPartitionKey pkey_;
    uint64_t log_id_;
    void reset();
    TO_STRING_KV(K_(pkey), K_(log_id));
    OB_UNIS_VERSION(1);
  };
  typedef common::ObSEArray<Param, 16> ParamArray;

public:
  ObLogReqHeartbeatInfoRequest();
  ~ObLogReqHeartbeatInfoRequest();

public:
  void reset();
  bool is_valid() const;
  int set_params(const ParamArray& params);
  int append_param(const Param& param);
  const ParamArray& get_params() const;
  int64_t rpc_ver() const;
  TO_STRING_KV(K_(rpc_ver), K_(params));
  OB_UNIS_VERSION(1);

private:
  int64_t rpc_ver_;
  ParamArray params_;
};

class ObLogReqHeartbeatInfoResponse {
  static const int64_t CUR_RPC_VER = 1;
  static const int64_t ITEM_CNT_LMT = 10000;  // Around 300kb for cur version.
public:
  struct Result {
    int err_;
    int64_t tstamp_;
    void reset();
    TO_STRING_KV(K_(err), K_(tstamp));
    OB_UNIS_VERSION(1);
  };
  typedef common::ObSEArray<Result, 16> ResultArray;

public:
  ObLogReqHeartbeatInfoResponse();
  ~ObLogReqHeartbeatInfoResponse();

public:
  void reset();
  void set_err(const int err);
  int set_results(const ResultArray& results);
  int append_result(const Result& result);
  int get_err() const;
  const ResultArray& get_results() const;
  int64_t rpc_ver() const;
  void set_rpc_ver(const int64_t ver);
  TO_STRING_KV(K_(rpc_ver), K_(err), K_(res));
  OB_UNIS_VERSION(1);

private:
  int64_t rpc_ver_;
  int err_;
  ResultArray res_;
};

class ObLogReqHeartbeatInfoProcessor
    : public ObRpcProcessor<ObLogExternalProxy::ObRpc<OB_LOG_REQUEST_HEARTBEAT_INFO> > {
public:
  ObLogReqHeartbeatInfoProcessor(storage::ObPartitionService* partition_service) : partition_service_(partition_service)
  {}
  ~ObLogReqHeartbeatInfoProcessor()
  {
    partition_service_ = NULL;
  }

protected:
  int process();

private:
  storage::ObPartitionService* partition_service_;
};

// check ps & ps->clog_mgr not null
// to simplify code
class ObExternalProcessorHelper {
public:
  static int get_clog_mgr(storage::ObPartitionService* ps, clog::ObICLogMgr*& clog_mgr);
};

// -------------- for breakpoint ------------

// BreakPoint info:
// Except for break_file_id, when locating start_log_id based on timestamp,
// after scanning an InfoBlock in the middle of emergency exit, the next scan,
// you need to restore the minimum log_id that has been scanned before.
struct BreakInfo {
  clog::file_id_t break_file_id_;
  uint64_t min_greater_log_id_;

  BreakInfo()
  {
    reset();
  }
  ~BreakInfo()
  {
    reset();
  }
  void reset()
  {
    break_file_id_ = common::OB_INVALID_FILE_ID;
    min_greater_log_id_ = common::OB_INVALID_ID;
  }
  TO_STRING_KV(K(break_file_id_), K(min_greater_log_id_));
  OB_UNIS_VERSION(1);
};

class ObLogReqStartLogIdByTsRequestWithBreakpoint {
public:
  static const int64_t CUR_RPC_VER = 1;
  static const int64_t ITEM_CNT_LMT = 10000;  // Around 400kb for cur version.

public:
  struct Param {
    common::ObPartitionKey pkey_;
    int64_t start_tstamp_;
    BreakInfo break_info_;
    void reset();
    void reset(const common::ObPartitionKey& pkey, const int64_t start_ts, const BreakInfo& bkinfo)
    {
      pkey_ = pkey;
      start_tstamp_ = start_ts;
      break_info_ = bkinfo;
    }
    bool is_valid() const
    {
      return pkey_.is_valid() && (start_tstamp_ > 0);
    }
    TO_STRING_KV(K_(pkey), K_(start_tstamp), K(break_info_));
    OB_UNIS_VERSION(1);
  };
  typedef common::ObSEArray<Param, 16> ParamArray;

public:
  ObLogReqStartLogIdByTsRequestWithBreakpoint();
  ~ObLogReqStartLogIdByTsRequestWithBreakpoint();

public:
  void reset();
  bool is_valid() const;
  int set_params(const ParamArray& params);
  int append_param(const Param& param);
  const ParamArray& get_params() const;
  // Backward compatible.
  int64_t rpc_ver() const;
  TO_STRING_KV(K_(rpc_ver), "param_count", params_.count(), K_(params));
  OB_UNIS_VERSION(1);

private:
  int64_t rpc_ver_;
  ParamArray params_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogReqStartLogIdByTsRequestWithBreakpoint);
};

/// start log id request result
///
/// 1. Return results for each partition separately, in the same order as the request
/// 2. If the partition request is completed, the error code OB_SUCCESS and start_log_id will be returned in the
/// partition result
/// 3. If the partition request is not completed, the error code OB_EXT_HANDLE_UNFINISH will be returned in the
/// partition result, and break_info will be set.
class ObLogReqStartLogIdByTsResponseWithBreakpoint {
  static const int64_t CUR_RPC_VER = 1;
  static const int64_t ITEM_CNT_LMT = 10000;  // Around 400kb for cur version.
public:
  // Stored in the same order as Request's params.
  struct Result {
    // Partition specific error code.
    int err_;
    uint64_t start_log_id_;
    int64_t start_log_ts_;

    BreakInfo break_info_;  // deprecated!!!

    void reset();
    void reset(const int err, const uint64_t start_log_id, const int64_t start_log_ts)
    {
      err_ = err;
      start_log_id_ = start_log_id;
      start_log_ts_ = start_log_ts;
    }

    TO_STRING_KV(K_(err), K_(start_log_id), K_(start_log_ts));
    OB_UNIS_VERSION(1);
  };
  typedef common::ObSEArray<Result, 16> ResultArray;

public:
  ObLogReqStartLogIdByTsResponseWithBreakpoint();
  ~ObLogReqStartLogIdByTsResponseWithBreakpoint();

public:
  void reset();
  void set_err(const int err);
  int set_results(const ResultArray& results);
  int append_result(const Result& result);
  int get_err() const;
  const ResultArray& get_results() const;
  int64_t rpc_ver() const;
  void set_rpc_ver(const int64_t ver);
  TO_STRING_KV(K_(rpc_ver), K_(err), "result_count", res_.count(), "result", res_);
  OB_UNIS_VERSION(1);

private:
  int64_t rpc_ver_;
  int err_;
  ResultArray res_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogReqStartLogIdByTsResponseWithBreakpoint);
};

class ObLogReqStartLogIdByTsProcessorWithBreakpoint
    : public ObRpcProcessor<ObLogExternalProxy::ObRpc<OB_LOG_REQ_START_LOG_ID_BY_TS_WITH_BREAKPOINT> > {
public:
  ObLogReqStartLogIdByTsProcessorWithBreakpoint(storage::ObPartitionService* partition_service)
      : partition_service_(partition_service)
  {}
  ~ObLogReqStartLogIdByTsProcessorWithBreakpoint()
  {
    partition_service_ = NULL;
  }

protected:
  int process();

private:
  storage::ObPartitionService* partition_service_;
};

class ObLogReqStartPosByLogIdRequestWithBreakpoint {
  static const int64_t CUR_RPC_VER = 1;
  static const int64_t ITEM_CNT_LMT = 10000;  // Around 400kb for cur version.
public:
  struct Param {
    common::ObPartitionKey pkey_;
    uint64_t start_log_id_;
    BreakInfo break_info_;
    void reset();
    bool is_valid() const
    {
      return pkey_.is_valid() && (start_log_id_ > 0);
    }
    TO_STRING_KV(K(pkey_), K(start_log_id_), K(break_info_));
    OB_UNIS_VERSION(1);
  };
  typedef common::ObSEArray<Param, 16> ParamArray;

public:
  ObLogReqStartPosByLogIdRequestWithBreakpoint();
  ~ObLogReqStartPosByLogIdRequestWithBreakpoint();

public:
  void reset();
  bool is_valid() const;
  int set_params(const ParamArray& params);
  int append_param(const Param& param);
  const ParamArray& get_params() const;
  int64_t rpc_ver() const;
  TO_STRING_KV(K(rpc_ver_), K(params_));
  OB_UNIS_VERSION(1);

private:
  int64_t rpc_ver_;
  ParamArray params_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogReqStartPosByLogIdRequestWithBreakpoint);
};

class ObLogReqStartPosByLogIdResponseWithBreakpoint {
  static const int64_t CUR_RPC_VER = 1;
  static const int64_t ITEM_CNT_LMT = 10000;  // Around 500kb for cur version.
public:
  // Stored in the same order as Request's params.
  struct Result {
    // Partition specific error code.
    int err_;
    clog::file_id_t file_id_;
    clog::offset_t offset_;
    // Valid log id range on this observer.
    uint64_t first_log_id_;
    uint64_t last_log_id_;
    BreakInfo break_info_;
    void reset();
    TO_STRING_KV(K_(err), K_(file_id), K_(offset), K_(first_log_id), K_(last_log_id), K(break_info_));
    OB_UNIS_VERSION(1);
  };
  typedef common::ObSEArray<Result, 16> ResultArray;

public:
  ObLogReqStartPosByLogIdResponseWithBreakpoint();
  ~ObLogReqStartPosByLogIdResponseWithBreakpoint();

public:
  void reset();
  void set_err(const int err);
  int set_results(const ResultArray& results);
  int append_result(const Result& result);
  int get_err() const;
  const ResultArray& get_results() const;
  int64_t rpc_ver() const;
  void set_rpc_ver(const int64_t ver);
  TO_STRING_KV(K_(rpc_ver), K_(err), K_(res));
  OB_UNIS_VERSION(1);

private:
  int64_t rpc_ver_;
  int err_;
  ResultArray res_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogReqStartPosByLogIdResponseWithBreakpoint);
};

class ObLogReqStartPosByLogIdProcessorWithBreakpoint
    : public ObRpcProcessor<ObLogExternalProxy::ObRpc<OB_LOG_REQ_START_POS_BY_LOG_ID_WITH_BREAKPOINT> > {
public:
  ObLogReqStartPosByLogIdProcessorWithBreakpoint(storage::ObPartitionService* partition_service)
      : partition_service_(partition_service)
  {}
  ~ObLogReqStartPosByLogIdProcessorWithBreakpoint()
  {
    partition_service_ = NULL;
  }

protected:
  int process();

private:
  storage::ObPartitionService* partition_service_;
};

struct ObStreamSeq {
  common::ObAddr self_;
  int64_t seq_ts_;

  ObStreamSeq() : self_(), seq_ts_(common::OB_INVALID_TIMESTAMP)
  {}
  void reset()
  {
    self_.reset();
    seq_ts_ = common::OB_INVALID_TIMESTAMP;
  }
  bool operator==(const ObStreamSeq& that) const
  {
    return self_ == that.self_ && seq_ts_ == that.seq_ts_;
  }
  bool operator!=(const ObStreamSeq& that) const
  {
    return !(*this == that);
  }
  bool is_valid() const
  {
    return self_.is_valid() && common::OB_INVALID_TIMESTAMP != seq_ts_;
  }
  uint64_t hash() const
  {
    return murmurhash(&self_, sizeof(self_), seq_ts_);
  }
  TO_STRING_KV(K(self_), K(seq_ts_));
  OB_UNIS_VERSION(1);
};

class ObLogOpenStreamReq {
  static const int64_t CUR_RPC_VER = 1;
  static const int64_t ITEM_CNT_LMT = 10000;  // Around 400kb for cur version.
public:
  struct Param {
    common::ObPartitionKey pkey_;
    uint64_t start_log_id_;
    void reset()
    {
      pkey_.reset();
      start_log_id_ = common::OB_INVALID_ID;
    }
    bool is_valid() const
    {
      return pkey_.is_valid() && (common::OB_INVALID_ID != start_log_id_);
    }
    TO_STRING_KV(K(pkey_), K(start_log_id_));
    OB_UNIS_VERSION(1);
  };
  typedef common::ObSEArray<Param, 16> ParamArray;

public:
  ObLogOpenStreamReq()
  {
    reset();
  }
  ~ObLogOpenStreamReq()
  {}

public:
  void reset()
  {
    rpc_ver_ = CUR_RPC_VER;
    params_.reset();
    stream_lifetime_ = INVALID_STREAM_LIFETIME;
    liboblog_pid_ = 0;
    stale_stream_.reset();
  }
  bool is_valid() const
  {
    bool bool_ret = (params_.count() > 0 && is_valid_stream_lifetime(stream_lifetime_));
    for (int i = 0; i < params_.count() && bool_ret; i++) {
      bool_ret = params_[i].is_valid();
      if (!bool_ret) {
        EXTLOG_LOG(WARN, "invalid open stream param", K(i), "param", params_[i]);
      }
    }
    return bool_ret;
  }
  int set_params(const ParamArray& params)
  {
    return params.count() > ITEM_CNT_LMT ? common::OB_BUF_NOT_ENOUGH : params_.assign(params);
  }
  int append_param(const Param& param)
  {
    return params_.count() >= ITEM_CNT_LMT ? common::OB_BUF_NOT_ENOUGH : params_.push_back(param);
  }
  int set_stream_lifetime(const int64_t lifetime)
  {
    int ret = common::OB_SUCCESS;
    if (!is_valid_stream_lifetime(lifetime)) {
      ret = common::OB_INVALID_ARGUMENT;
    } else {
      stream_lifetime_ = lifetime;
    }
    return ret;
  }
  int set_liboblog_pid(const uint64_t id)
  {
    liboblog_pid_ = id;
    return common::OB_SUCCESS;
  }
  void set_stale_stream(const ObStreamSeq& seq)
  {
    stale_stream_ = seq;
  }
  const ParamArray& get_params() const
  {
    return params_;
  }
  int64_t get_stream_lifetime() const
  {
    return stream_lifetime_;
  }
  uint64_t get_liboblog_pid() const
  {
    return liboblog_pid_;
  }
  const ObStreamSeq& get_stale_stream() const
  {
    return stale_stream_;
  }
  int64_t rpc_ver() const
  {
    return rpc_ver_;
  }
  TO_STRING_KV(
      K_(rpc_ver), K_(stream_lifetime), K_(liboblog_pid), K_(stale_stream), "param_count", params_.count(), K_(params));
  OB_UNIS_VERSION(1);

private:
  inline static bool is_valid_stream_lifetime(const int64_t lifetime)
  {
    // 0 means stream can be washed immediately
    return lifetime >= 0;
  }

private:
  static const int64_t INVALID_STREAM_LIFETIME = -1;

private:
  int64_t rpc_ver_;
  ParamArray params_;
  int64_t stream_lifetime_;  // us
  uint64_t liboblog_pid_;    // Process ID.
  ObStreamSeq stale_stream_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogOpenStreamReq);
};

/*
 * Error code:
 *  - OB_INVALID_ARGUMENT: param error
 *  - OB_SIZE_OVERFLOW: refuse open stream
 *  - OB_ERR_SYS: internal error
 */
class ObLogOpenStreamResp {
  static const int64_t CUR_RPC_VER = 1;

public:
  ObLogOpenStreamResp()
  {
    reset();
  }
  ~ObLogOpenStreamResp()
  {}

public:
  int assign(const ObLogOpenStreamResp& other)
  {
    if (this != &other) {
      rpc_ver_ = other.rpc_ver_;
      err_ = other.err_;
      debug_err_ = other.debug_err_;
      seq_ = other.seq_;
    }
    return common::OB_SUCCESS;
  }

  void reset()
  {
    rpc_ver_ = CUR_RPC_VER;
    err_ = common::OB_NOT_INIT;
    debug_err_ = common::OB_NOT_INIT;
    seq_.reset();
  }
  void set_err(const int err)
  {
    err_ = err;
  }
  void set_debug_err(const int err)
  {
    debug_err_ = err;
  }
  int set_stream_seq(const ObStreamSeq& seq)
  {
    int ret = common::OB_SUCCESS;
    if (OB_UNLIKELY(!seq.is_valid())) {
      ret = common::OB_INVALID_ARGUMENT;
      EXTLOG_LOG(WARN, "invalid stream seq", K(ret), K(seq));
    } else {
      seq_ = seq;
    }
    return ret;
  }
  int get_err() const
  {
    return err_;
  }
  int get_debug_err() const
  {
    return debug_err_;
  }
  int64_t rpc_ver() const
  {
    return rpc_ver_;
  }
  void set_rpc_ver(const int64_t ver)
  {
    rpc_ver_ = ver;
  }
  const ObStreamSeq& get_stream_seq() const
  {
    return seq_;
  }
  TO_STRING_KV(K(rpc_ver_), K(err_), K(debug_err_), K(seq_));
  OB_UNIS_VERSION(1);

private:
  int64_t rpc_ver_;
  int err_;
  int debug_err_;
  ObStreamSeq seq_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogOpenStreamResp);
};

class ObLogOpenStreamProcessor : public ObRpcProcessor<ObLogExternalProxy::ObRpc<OB_LOG_OPEN_STREAM> > {
public:
  ObLogOpenStreamProcessor(storage::ObPartitionService* partition_service) : partition_service_(partition_service)
  {}
  ~ObLogOpenStreamProcessor()
  {
    partition_service_ = NULL;
  }

protected:
  int process();

private:
  storage::ObPartitionService* partition_service_;
};

class ObLogStreamFetchLogReq {
  static const int64_t CUR_RPC_VER = 1;

public:
  ObLogStreamFetchLogReq()
  {
    reset();
  }
  ~ObLogStreamFetchLogReq()
  {}

public:
  void reset()
  {
    rpc_ver_ = CUR_RPC_VER;
    seq_.reset();
    enable_feedback_ = false;
    upper_limit_ts_ = 0;
    log_cnt_per_part_per_round_ = 0;
  }

  void reset(const obrpc::ObStreamSeq& seq, const int64_t upper_limit, const int64_t fetch_log_cnt_per_part_per_round,
      const bool need_feed_back)
  {
    reset();

    seq_ = seq;
    upper_limit_ts_ = upper_limit;
    log_cnt_per_part_per_round_ = fetch_log_cnt_per_part_per_round;
    enable_feedback_ = need_feed_back;
  }

  bool is_valid() const
  {
    return seq_.is_valid() && upper_limit_ts_ > 0 && common::OB_INVALID_TIMESTAMP != upper_limit_ts_ &&
           log_cnt_per_part_per_round_ > 0;
  }
  int set_stream_seq(const ObStreamSeq& seq)
  {
    int ret = common::OB_SUCCESS;
    if (OB_UNLIKELY(!seq.is_valid())) {
      ret = common::OB_INVALID_ARGUMENT;
    } else {
      seq_ = seq;
    }
    return ret;
  }
  void enable_feedback()
  {
    enable_feedback_ = true;
  }
  void set_feedback(const bool enable_feedback)
  {
    enable_feedback_ = enable_feedback;
  }
  bool is_feedback_enabled() const
  {
    return enable_feedback_;
  }
  int set_upper_limit_ts(const int64_t ts)
  {
    int ret = common::OB_SUCCESS;
    if (OB_UNLIKELY(ts <= 0)) {
      ret = common::OB_INVALID_ARGUMENT;
    } else {
      upper_limit_ts_ = ts;
    }
    return ret;
  }
  int set_log_cnt_per_part_per_round(const int64_t cnt)
  {
    int ret = common::OB_SUCCESS;
    if (OB_UNLIKELY(cnt <= 0)) {
      ret = common::OB_INVALID_ARGUMENT;
    } else {
      log_cnt_per_part_per_round_ = cnt;
    }
    return ret;
  }
  const ObStreamSeq& get_stream_seq() const
  {
    return seq_;
  }
  int64_t get_upper_limit_ts() const
  {
    return upper_limit_ts_;
  }
  int64_t get_log_cnt_per_part_per_round() const
  {
    return log_cnt_per_part_per_round_;
  }
  int64_t rpc_ver() const
  {
    return rpc_ver_;
  }

  ObLogStreamFetchLogReq& operator=(const ObLogStreamFetchLogReq& other)
  {
    rpc_ver_ = other.rpc_ver_;
    seq_ = other.seq_;
    enable_feedback_ = other.enable_feedback_;
    upper_limit_ts_ = other.upper_limit_ts_;
    log_cnt_per_part_per_round_ = other.log_cnt_per_part_per_round_;
    return *this;
  }

  bool operator==(const ObLogStreamFetchLogReq& that) const
  {
    return rpc_ver_ == that.rpc_ver_ && seq_ == that.seq_ && enable_feedback_ == that.enable_feedback_ &&
           upper_limit_ts_ == that.upper_limit_ts_ && log_cnt_per_part_per_round_ == that.log_cnt_per_part_per_round_;
  }

  bool operator!=(const ObLogStreamFetchLogReq& that) const
  {
    return !(*this == that);
  }

  TO_STRING_KV(K_(rpc_ver), K_(seq), K_(upper_limit_ts), K_(log_cnt_per_part_per_round), K_(enable_feedback));
  OB_UNIS_VERSION(1);

private:
  int64_t rpc_ver_;
  ObStreamSeq seq_;
  bool enable_feedback_;
  int64_t upper_limit_ts_;
  int64_t log_cnt_per_part_per_round_;
};

struct ObFetchStatus {
  // Statistics for pkey
  int64_t touched_pkey_count_;               // The number of partitions scanned this time
  int64_t need_fetch_pkey_count_;            // The number of partitions that need to take logs
  int64_t reach_max_log_id_pkey_count_;      // The number of partitions that reach the maximum log ID
  int64_t reach_upper_limit_ts_pkey_count_;  // The number of partitions reached the upper limit
  int64_t scan_round_count_;                 // Number of rounds for complete scan

  // For time-consuming statistics:
  // Time-consuming for sending data from liboblog to receiving data on server, including sending queue on liboblog +
  // time-consuming network transmission
  int64_t l2s_net_time_;
  // Server receiving queue queuing time(run_ts - recv_ts)
  int64_t svr_queue_time_;
  // ext_log_service actual processing time(msg.deserialize + processor::process)
  int64_t ext_process_time_;

  ObFetchStatus()
  {
    reset();
  }
  void reset()
  {
    touched_pkey_count_ = 0;
    need_fetch_pkey_count_ = 0;
    reach_max_log_id_pkey_count_ = 0;
    reach_upper_limit_ts_pkey_count_ = 0;
    scan_round_count_ = 0;
    l2s_net_time_ = 0;
    svr_queue_time_ = 0;
    ext_process_time_ = 0;
  }
  TO_STRING_KV(K(touched_pkey_count_), K(need_fetch_pkey_count_), K(reach_max_log_id_pkey_count_),
      K(reach_upper_limit_ts_pkey_count_), K(scan_round_count_), K(l2s_net_time_), K(svr_queue_time_),
      K(ext_process_time_));
  OB_UNIS_VERSION(1);
};

/*
 * Error code:
 *  - OB_STREAM_NOT_EXIST: no such stream
 *  - OB_STREAM_BUSY: same stream_seq is fetching (the prev fetch rpc timeout maybe)
 *  - OB_NEED_RETRY: file_id_cache trying to extend_min
 *  - OB_ERR_SYS: other error
 */
class ObLogStreamFetchLogResp {
  static const int64_t CUR_RPC_VER = 1;

public:
  enum FeedbackType {
    INVALID_FEEDBACK = -1,
    LAGGED_FOLLOWER = 0,         // lagged follower
    LOG_NOT_IN_THIS_SERVER = 1,  // this server does not server this log
    PARTITION_OFFLINED = 2,      // partition offlined
  };
  struct FeedbackPartition {
    common::ObPartitionKey pkey_;
    int64_t feedback_type_;
    void reset()
    {
      pkey_.reset();
      feedback_type_ = INVALID_FEEDBACK;
    }
    bool is_valid() const
    {
      return pkey_.is_valid() && INVALID_FEEDBACK != feedback_type_;
    }
    TO_STRING_KV(K(pkey_), K(feedback_type_));
    OB_UNIS_VERSION(1);
  };
  typedef common::ObSEArray<FeedbackPartition, 16> FeedBackPartitionArray;
  struct FetchLogHeartbeatItem {
    common::ObPartitionKey pkey_;
    uint64_t next_log_id_;
    int64_t heartbeat_ts_;
    void reset()
    {
      pkey_.reset();
      next_log_id_ = common::OB_INVALID_ID;
      heartbeat_ts_ = common::OB_INVALID_TIMESTAMP;
    }
    bool is_valid() const
    {
      return pkey_.is_valid() && (common::OB_INVALID_ID != next_log_id_) &&
             (common::OB_INVALID_TIMESTAMP != heartbeat_ts_);
    }
    TO_STRING_KV(K(pkey_), K(next_log_id_), K(heartbeat_ts_));
    OB_UNIS_VERSION(1);
  };
  typedef common::ObSEArray<FetchLogHeartbeatItem, 16> FetchLogHeartbeatArray;

public:
  ObLogStreamFetchLogResp();
  ~ObLogStreamFetchLogResp()
  {
    reset();
  }

public:
  int assign(const ObLogStreamFetchLogResp& other);

  void reset()
  {
    rpc_ver_ = CUR_RPC_VER;
    err_ = common::OB_NOT_INIT;
    debug_err_ = common::OB_NOT_INIT;
    fetch_status_.reset();
    feedback_partition_arr_.reset();
    fetch_log_heartbeat_arr_.reset();
    log_num_ = 0;
    pos_ = 0;
  }
  int append_feedback(const FeedbackPartition& feedback)
  {
    int ret = common::OB_SUCCESS;
    if (OB_UNLIKELY(!feedback.is_valid())) {
      ret = common::OB_INVALID_ARGUMENT;
    } else if (feedback_partition_arr_.count() >= feedback_array_cnt_lmt_()) {
      ret = common::OB_BUF_NOT_ENOUGH;
    } else {
      ret = feedback_partition_arr_.push_back(feedback);
    }
    return ret;
  }
  int append_hb(const FetchLogHeartbeatItem& item)
  {
    int ret = common::OB_SUCCESS;
    if (OB_UNLIKELY(!item.is_valid())) {
      ret = common::OB_INVALID_ARGUMENT;
    } else if (fetch_log_heartbeat_arr_.count() >= fetch_log_hb_array_cnt_lmt_()) {
      ret = common::OB_BUF_NOT_ENOUGH;
    } else {
      ret = fetch_log_heartbeat_arr_.push_back(item);
    }
    return ret;
  }
  inline char* get_remain_buf(int64_t& remain_size)
  {
    remain_size = FETCH_BUF_LEN - pos_;
    return log_entry_buf_ + pos_;
  }
  inline bool has_enough_buffer(const int64_t want_size) const
  {
    return (pos_ + want_size) <= FETCH_BUF_LEN;
  }
  inline void clog_entry_filled(const int64_t want_size)
  {
    pos_ += want_size;
    log_num_++;
  }
  void set_err(const int err)
  {
    err_ = err;
  }
  void set_debug_err(const int err)
  {
    debug_err_ = err;
  }
  int get_err() const
  {
    return err_;
  }
  int get_debug_err() const
  {
    return debug_err_;
  }
  const FeedBackPartitionArray& get_feedback_array() const
  {
    return feedback_partition_arr_;
  }
  const FetchLogHeartbeatArray& get_hb_array() const
  {
    return fetch_log_heartbeat_arr_;
  }
  const char* get_log_entry_buf() const
  {
    return log_entry_buf_;
  }
  int64_t rpc_ver() const
  {
    return rpc_ver_;
  }
  void set_rpc_ver(const int64_t ver)
  {
    rpc_ver_ = ver;
  }
  int64_t get_log_num() const
  {
    return log_num_;
  }
  int64_t get_pos() const
  {
    return pos_;
  }
  void set_fetch_status(const ObFetchStatus& fetch_status)
  {
    fetch_status_ = fetch_status;
  }
  const ObFetchStatus& get_fetch_status() const
  {
    return fetch_status_;
  }
  void set_l2s_net_time(const int64_t l2s_net_time)
  {
    fetch_status_.l2s_net_time_ = l2s_net_time;
  }
  void set_svr_queue_time(const int64_t svr_q_time)
  {
    fetch_status_.svr_queue_time_ = svr_q_time;
  }
  void set_process_time(const int64_t process_time)
  {
    fetch_status_.ext_process_time_ = process_time;
  }
  // just for test, easier to fill response msg
  int append_clog_entry(const clog::ObLogEntry& entry)
  {
    int ret = common::OB_SUCCESS;
    int64_t old_pos = pos_;
    if (OB_FAIL(entry.serialize(log_entry_buf_, FETCH_BUF_LEN, pos_))) {
      ret = common::OB_BUF_NOT_ENOUGH;
      pos_ = old_pos;
    } else {
      log_num_++;
    }
    return ret;
  }
  TO_STRING_KV(K_(rpc_ver), K_(err), K_(debug_err), K_(fetch_status), K_(log_num), K_(pos), "feedback_count",
      feedback_partition_arr_.count(), "heartbeat_count", fetch_log_heartbeat_arr_.count(), "feedback_array",
      feedback_partition_arr_, "heartbeat_array", fetch_log_heartbeat_arr_);
  OB_UNIS_VERSION(1);

private:
  int64_t feedback_array_cnt_lmt_()
  {
    return CUR_RPC_VER == rpc_ver_ ? (cur_ver_array_cnt_lmt_() / 2) : 0;
  }
  int64_t fetch_log_hb_array_cnt_lmt_()
  {
    return CUR_RPC_VER == rpc_ver_ ? (cur_ver_array_cnt_lmt_() / 2) : 0;
  }
  int64_t cur_ver_array_cnt_lmt_();

private:
  static const int64_t FETCH_BUF_LEN = common::OB_MAX_LOG_BUFFER_SIZE;

private:
  int64_t rpc_ver_;
  int err_;
  int debug_err_;
  ObFetchStatus fetch_status_;
  FeedBackPartitionArray feedback_partition_arr_;
  FetchLogHeartbeatArray fetch_log_heartbeat_arr_;
  int64_t log_num_;
  int64_t pos_;
  char log_entry_buf_[FETCH_BUF_LEN];

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogStreamFetchLogResp);
};

class ObLogStreamFetchLogProcessor : public ObRpcProcessor<ObLogExternalProxy::ObRpc<OB_LOG_STREAM_FETCH_LOG> > {
public:
  ObLogStreamFetchLogProcessor(storage::ObPartitionService* partition_service) : partition_service_(partition_service)
  {}
  ~ObLogStreamFetchLogProcessor()
  {
    partition_service_ = NULL;
  }

protected:
  int process();

private:
  storage::ObPartitionService* partition_service_;
};

class ObLogLeaderHeartbeatReq {
public:
  struct Param {
    common::ObPartitionKey pkey_;
    uint64_t next_log_id_;  // Do not rely on the log_id, only used for debugging

    void reset()
    {
      pkey_.reset();
      next_log_id_ = common::OB_INVALID_ID;
    }

    void reset(const common::ObPartitionKey& pkey, const uint64_t next_log_id)
    {
      pkey_ = pkey;
      next_log_id_ = next_log_id;
    }

    TO_STRING_KV(K(pkey_), K(next_log_id_));
    OB_UNIS_VERSION(1);
  };
  typedef common::ObSEArray<Param, 256> ParamArray;

public:
  ObLogLeaderHeartbeatReq()
  {
    reset();
  }
  ~ObLogLeaderHeartbeatReq()
  {}
  void reset()
  {
    rpc_ver_ = CUR_RPC_VER;
    params_.reset();
  }
  bool is_valid() const
  {
    return params_.count() > 0;
  }
  int set_params(const ParamArray& params)
  {
    int ret = common::OB_SUCCESS;
    if (params.count() > ITEM_CNT_LMT) {
      ret = OB_BUF_NOT_ENOUGH;
      EXTLOG_LOG(WARN, "set params error", K(ret), K(params));
    } else if (OB_FAIL(params_.assign(params))) {
      EXTLOG_LOG(WARN, "assign params error", K(ret), K(params_), K(params));
    }
    return ret;
  }
  int append_param(const Param& param)
  {
    int ret = common::OB_SUCCESS;
    if (params_.count() > ITEM_CNT_LMT) {
      ret = OB_BUF_NOT_ENOUGH;
    } else if (OB_FAIL(params_.push_back(param))) {
      EXTLOG_LOG(WARN, "append param error", K(ret), K(param), K(params_));
    }
    return ret;
  }
  const ParamArray& get_params() const
  {
    return params_;
  }
  int64_t rpc_ver() const
  {
    return rpc_ver_;
  }
  TO_STRING_KV(K_(rpc_ver), "param_count", params_.count(), K_(params));
  OB_UNIS_VERSION(1);

public:
  static const int64_t CUR_RPC_VER = 1;
  static const int64_t ITEM_CNT_LMT = 10000;  // Around 300kb for cur version.
private:
  int64_t rpc_ver_;
  ParamArray params_;
};

/*
The result agreement of per_pkey:
   The expected value of pkey_ret is OB_SUCCSS, OB_NOT_MASTER, or OB_PARTITION_NOT_EXIST.
   OB_SUCCESS: next_served_log_id_ is the largest log_id that has been served + 1, next_served_ts_ is the predicted
timestamp (in cold partition, this value is equal to the current timestamp) OB_NOT_MASTER: next_served_log_id_ is the
largest log_id that has been served + 1, next_served_ts_ is the timestamp corresponding to the largest log that has been
served + 1 OB_PARTITION_NOT_EXIST: next_served_log_id_/next_served_ts_ is an invalid value
*/
class ObLogLeaderHeartbeatResp {
public:
  struct Result {
    int err_;
    uint64_t next_served_log_id_;
    int64_t next_served_ts_;

    void reset()
    {
      err_ = common::OB_NOT_INIT;
      next_served_log_id_ = common::OB_INVALID_ID;
      next_served_ts_ = common::OB_INVALID_TIMESTAMP;
    }
    TO_STRING_KV(K(err_), K(next_served_log_id_), K(next_served_ts_));
    OB_UNIS_VERSION(1);
  };
  typedef common::ObSEArray<Result, 256> ResultArray;

public:
  ObLogLeaderHeartbeatResp()
  {
    reset();
  }
  ~ObLogLeaderHeartbeatResp()
  {}

public:
  void reset()
  {
    rpc_ver_ = CUR_RPC_VER;
    err_ = common::OB_NOT_INIT;
    debug_err_ = common::OB_NOT_INIT;
    res_.reset();
  }
  void set_err(const int err)
  {
    err_ = err;
  }
  void set_debug_err(const int debug_err)
  {
    debug_err_ = debug_err;
  }
  int set_results(const ResultArray& results)
  {
    return res_.assign(results);
  }
  int append_result(const Result& result)
  {
    return res_.push_back(result);
  }
  int get_err() const
  {
    return err_;
  }
  int get_debug_err() const
  {
    return debug_err_;
  }
  const ResultArray& get_results() const
  {
    return res_;
  }
  int64_t rpc_ver() const
  {
    return rpc_ver_;
  }
  void set_rpc_ver(const int64_t ver)
  {
    rpc_ver_ = ver;
  }
  TO_STRING_KV(K_(rpc_ver), K_(err), K_(debug_err), "result_count", res_.count(), "result", res_);
  OB_UNIS_VERSION(1);

private:
  static const int64_t CUR_RPC_VER = 1;
  static const int64_t ITEM_CNT_LMT = 10000;  // Around 300kb for cur version.
private:
  int64_t rpc_ver_;
  int err_;
  int debug_err_;  // For debugging, liboblog does not rely on this field
  ResultArray res_;
};

class ObLogLeaderHeartbeatProcessor : public ObRpcProcessor<ObLogExternalProxy::ObRpc<OB_LOG_LEADER_HEARTBEAT> > {
public:
  ObLogLeaderHeartbeatProcessor(storage::ObPartitionService* partition_service) : partition_service_(partition_service)
  {}
  ~ObLogLeaderHeartbeatProcessor()
  {
    partition_service_ = NULL;
  }

protected:
  int process();

private:
  storage::ObPartitionService* partition_service_;
};

}  // namespace obrpc
}  // namespace oceanbase

#endif  // OCEANBASE_CLOG_OB_LOG_EXTERNAL_RPC_
