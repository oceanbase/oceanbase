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

#ifndef OCEANBASE_LOGSERVICE_OB_CDC_REQ
#define OCEANBASE_LOGSERVICE_OB_CDC_REQ

#include "observer/ob_server_struct.h"          // GCTX
#include "share/ob_ls_id.h"                     // ObLSID
#include "lib/compress/ob_compress_util.h"      // ObCompressorType
#include "logservice/palf/lsn.h"                // LSN
#include "logservice/palf/log_group_entry.h"    // LogGroupEntry
#include "logservice/palf/log_entry.h"          // LogEntry


namespace oceanbase
{
namespace obrpc
{
using oceanbase::share::ObLSID;
using oceanbase::palf::LSN;
using oceanbase::palf::LogGroupEntry;
using oceanbase::palf::LogEntry;

class ObCdcReqStartLSNByTsReq;
class ObCdcReqStartLSNByTsResp;

class ObCdcLSFetchLogReq;
class ObCdcLSFetchLogResp;

class ObCdcLSFetchMissLogReq;

class ObCdcRpcTestFlag {
public:
  // rpc request flag bit
  static const int8_t OBCDC_RPC_FETCH_ARCHIVE = 1;
  static const int8_t OBCDC_RPC_TEST_SWITCH_MODE = 1 << 1;

public:
  static bool is_fetch_archive_only(int8_t flag) {
    return flag & OBCDC_RPC_FETCH_ARCHIVE;
  }
  static bool is_test_switch_mode(int8_t flag) {
    return flag & OBCDC_RPC_TEST_SWITCH_MODE;
  }
};

class ObCdcRpcId {
public:
  OB_UNIS_VERSION(1);
public:
  ObCdcRpcId(): client_pid_(0), client_addr_()  {}
  ~ObCdcRpcId() = default;
  int init(const uint64_t pid, const ObAddr &addr) {
    int ret = OB_SUCCESS;
    if (pid > 0 && addr.is_valid()) {
      // addr may not be valid
      client_pid_ = pid;
      client_addr_ = addr;
    } else {
      ret = OB_INVALID_ARGUMENT;
      EXTLOG_LOG(WARN, "invalid arguments for ObCdcRpcId", KR(ret), K(pid), K(addr));
    }
    return ret;
  }

  void reset() {
    client_pid_ = 0;
    client_addr_.reset();
  }

  bool operator==(const ObCdcRpcId &that) const {
    return client_pid_ == that.client_pid_ &&
           client_addr_ == that.client_addr_;
  }

  bool operator!=(const ObCdcRpcId &that) const {
    return !(*this == that);
  }

  ObCdcRpcId &operator=(const ObCdcRpcId &that) {
    client_pid_ = that.client_pid_;
    client_addr_ = that.client_addr_;
    return *this;
  }

  void set_addr(ObAddr &addr) { client_addr_ = addr; }
  const ObAddr& get_addr() const { return client_addr_; }

  void set_pid(uint64_t pid) { client_pid_ = pid; }
  uint64_t get_pid() const { return client_pid_; }

  TO_STRING_KV(K_(client_addr), K_(client_pid));
private:
  uint64_t client_pid_;
  ObAddr client_addr_;
};

class ObCdcReqStartLSNByTsReq
{
public:
  static const int64_t CUR_RPC_VER = 1;
  static const int64_t ITEM_CNT_LMT = 10000; // Around 400kb for cur version.

public:
  struct LocateParam
  {
    ObLSID ls_id_;
    int64_t start_ts_ns_;
    void reset();
    void reset(const ObLSID &ls_id, const int64_t start_ts_ns);
    bool is_valid() const;
    TO_STRING_KV(K_(ls_id), K_(start_ts_ns));
    OB_UNIS_VERSION(1);
  };
  typedef common::ObSEArray<LocateParam, 16> LocateParamArray;
public:
  ObCdcReqStartLSNByTsReq();
  ~ObCdcReqStartLSNByTsReq();
public:
  void reset();
  bool is_valid() const;
  int64_t get_rpc_version() const { return rpc_ver_; }
  void set_rpc_version(const int64_t ver) { rpc_ver_ = ver; }

  int set_params(const LocateParamArray &params);
  int append_param(const LocateParam &param);
  const LocateParamArray &get_params() const;

  void set_client_id(ObCdcRpcId &id) { client_id_ = id; }
  const ObCdcRpcId &get_client_id() const { return client_id_; }

  void set_flag(int8_t flag) { flag_ |= flag; }
  int8_t get_flag() const { return flag_; }

  TO_STRING_KV(K_(rpc_ver), "param_count", params_.count(), K_(params), K_(client_id), K_(flag));
  OB_UNIS_VERSION(1);
private:
  int64_t rpc_ver_;
  LocateParamArray params_;
  ObCdcRpcId client_id_;
  int8_t flag_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObCdcReqStartLSNByTsReq);
};

/// start LSN request result
///
/// 1. Return results for each LS separately, in the same order as the request
/// 2. If the LS request is completed, the error code OB_SUCCESS and start LSN will be returned in the LS result
/// 3. If the LS request is not completed, the error code OB_EXT_HANDLE_UNFINISH will be returned in the partition result, and break_info will be set.
class ObCdcReqStartLSNByTsResp
{
  static const int64_t CUR_RPC_VER = 1;
  static const int64_t ITEM_CNT_LMT = 10000; // Around 400kb for cur version.
public:
  // Stored in the same order as Request's params.
  struct LocateResult
  {
    // LS specific error code.
    // - OB_SUCCESS: locate_by_ts_ns_coarsely success
    // - OB_ENTRY_NOT_EXIST: there is no log in disk
    // - OB_ERR_OUT_OF_LOWER_BOUND: ts_ns is too old, log files may have been recycled
    // - OB_LS_NOT_EXIST: get PalfHandleGuard not exist
    // - OB_EXT_HANDLE_UNFINISH: not finished, timeout
    // - Other code: fail
    // CDC Connector need to handle to this error code.
    int err_;
    LSN start_lsn_;
    int64_t start_ts_ns_;

    void reset();
    void reset(const int err, const LSN start_lsn, const int64_t start_ts_ns);
    void set_locate_err(const int err) { err_ = err; }

    TO_STRING_KV(K_(err), K_(start_lsn), K_(start_ts_ns));
    OB_UNIS_VERSION(1);
  };
  typedef common::ObSEArray<LocateResult, 16> LocateResultArray;
public:
  ObCdcReqStartLSNByTsResp();
  ~ObCdcReqStartLSNByTsResp();
public:
  void reset();
  int64_t get_rpc_version() const { return rpc_ver_; }
  void set_rpc_version(const int64_t ver) { rpc_ver_ = ver; }
  void set_err(const int err) { err_ = err; }
  int get_err() const { return err_; }

  int set_results(const LocateResultArray &results);
  int append_result(const LocateResult &result);
  const LocateResultArray &get_results() const;
  TO_STRING_KV(K_(rpc_ver), K_(err), "result_count", res_.count(), "result", res_);
  OB_UNIS_VERSION(1);
private:
  int64_t rpc_ver_;
  int err_;
  LocateResultArray res_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObCdcReqStartLSNByTsResp);
};

/*
 * Error code:
 *  - OB_INVALID_ARGUMENT: param error
 *  - OB_SIZE_OVERFLOW: refuse open stream
 *  - OB_ERR_SYS: internal error
 */
class ObCdcLSFetchLogReq
{
  static const int64_t CUR_RPC_VER = 1;
public:
  ObCdcLSFetchLogReq() { reset(); }
  ~ObCdcLSFetchLogReq() {}
  ObCdcLSFetchLogReq &operator=(const ObCdcLSFetchLogReq &other);
  bool operator==(const ObCdcLSFetchLogReq &that) const;
  bool operator!=(const ObCdcLSFetchLogReq &that) const;
public:
  void reset();
  void reset(const ObLSID ls_id,
    const LSN start_lsn,
    const int64_t upper_limit);
  bool is_valid() const;
  int64_t get_rpc_version() const { return rpc_ver_; }
  void set_rpc_version(const int64_t ver) { rpc_ver_ = ver; }

  void set_ls_id(const ObLSID &ls_id) { ls_id_ = ls_id; }
  const ObLSID &get_ls_id() const { return ls_id_; }
  void set_start_lsn(const LSN &start_lsn) { start_lsn_ = start_lsn; }
  const LSN &get_start_lsn() const { return start_lsn_; }

  int set_upper_limit_ts(const int64_t ts);
  int64_t get_upper_limit_ts() const { return upper_limit_ts_; }

  void set_client_pid(const uint64_t id) { client_pid_ = id; }
  uint64_t get_client_pid() const { return client_pid_; }

  void set_client_id(const ObCdcRpcId &id) { client_id_ = id; }
  const ObCdcRpcId &get_client_id() const { return client_id_; }

  void set_progress(const int64_t progress) { progress_ = progress; }
  int64_t get_progress() const { return progress_; }

  void set_flag(int8_t flag) { flag_ |= flag; }
  int8_t get_flag() const { return flag_; }

  void set_tenant_id(const uint64_t tenant_id) { tenant_id_ = tenant_id; }
  uint64_t get_tenant_id() const { return tenant_id_; }

  void set_compressor_type(const common::ObCompressorType &compressor_type) { compressor_type_ = compressor_type; }
  common::ObCompressorType get_compressor_type() const { return compressor_type_; }

  TO_STRING_KV(K_(rpc_ver),
      K_(ls_id),
      K_(start_lsn),
      K_(upper_limit_ts),
      K_(client_pid),
      K_(client_id),
      K_(progress),
      K_(flag),
      K_(compressor_type),
      K_(tenant_id));

  OB_UNIS_VERSION(1);

private:
  int64_t rpc_ver_;
  ObLSID ls_id_;
  LSN start_lsn_;
  int64_t upper_limit_ts_;
  uint64_t client_pid_;  // Process ID.
  ObCdcRpcId client_id_;
  // the progress represents the latest log ts(scn) of the log which the client has fetched (not precisely)
  // the progress is a necessary parameter for cdc service to locate the logentry in archive.
  // the main reason of adding this field in rpc is to handle a special case when fetching log:
  // client has fetched log in server A, and server A is down suddenly, the client switched to server B and continue
  // to fetch log. But server B doesn't maintain a context for client to fetch log, and without the client progress,
  // server B can hardly locate log in archive.
  int64_t progress_;
  int8_t flag_;
  common::ObCompressorType compressor_type_;
  uint64_t tenant_id_;
};

// Statistics for LS
struct ObCdcFetchStatus
{
  bool is_reach_max_lsn_;                       // Whether the max lsn is reached
  bool is_reach_upper_limit_ts_;                // Whether the upper limit is reached
  int64_t scan_round_count_;                    // Number of rounds for complete scan

  // For time-consuming statistics:
  // Time-consuming for sending data from CDC Connector to receiving data on server, including sending queue on Connector+ time-consuming network transmission
  int64_t l2s_net_time_;
  // Server receiving queue queuing time(run_ts - recv_ts)
  int64_t svr_queue_time_;
  // total time for fetch log while processing current RPC
  int64_t log_fetch_time_;
  // ext_log_service actual processing time(msg.deserialize + processor::process)
  int64_t ext_process_time_;

  ObCdcFetchStatus() { reset(); }
  void reset()
  {
    is_reach_max_lsn_ = false;
    is_reach_upper_limit_ts_ = false;
    scan_round_count_ = 0;
    l2s_net_time_ = 0;
    svr_queue_time_ = 0;
    log_fetch_time_ = 0;
    ext_process_time_ = 0;
  }
  void reset(const bool is_reach_max_lsn,
      const bool is_reach_upper_limit_ts,
      const int64_t scan_round_count)
  {
    is_reach_max_lsn_ = is_reach_max_lsn;
    is_reach_upper_limit_ts_ = is_reach_upper_limit_ts;
    scan_round_count_ = scan_round_count;
  }

  TO_STRING_KV(K_(is_reach_max_lsn),
               K_(is_reach_upper_limit_ts),
               K_(scan_round_count),
               K_(l2s_net_time),
               K_(svr_queue_time),
               K_(log_fetch_time),
               K_(ext_process_time));
  OB_UNIS_VERSION(1);
};

/*
 * Error code:
 *  - OB_NEED_RETRY: need retry
 *  - OB_ERR_SYS: other error
 */
class ObCdcLSFetchLogResp
{
  static const int64_t CUR_RPC_VER = 1;
public:
  enum FeedbackType
  {
    INVALID_FEEDBACK = -1,
    LAGGED_FOLLOWER = 0,         // lagged follower
    LOG_NOT_IN_THIS_SERVER = 1,  // this server does not server this log
    LS_OFFLINED = 2,             // LS offlined
    ARCHIVE_ITER_END_BUT_LS_NOT_EXIST_IN_PALF = 3,   // Reach Max LSN in archive log but cannot switch
                                 // to palf because ls not exists in current server
  };
public:
  ObCdcLSFetchLogResp() { reset(); }
  ~ObCdcLSFetchLogResp() { reset(); }
  void reset();
  int assign(const ObCdcLSFetchLogResp &other);
public:
  int64_t get_rpc_version() const { return rpc_ver_; }
  void set_rpc_version(const int64_t ver) { rpc_ver_ = ver; }

  /*
   * Error code:
   *  - OB_NEED_RETRY: need retry
   *  - OB_ERR_SYS: other error
   *  - OB_LS_NOT_EXIST: LS may gc, CDC Connector need switch server
   */
  void set_err(const int err) { err_ = err; }
  int get_err() const { return err_; }
  void set_debug_err(const int err) { debug_err_ = err; }
  int get_debug_err() const { return debug_err_; }

  void set_ls_id(const ObLSID &ls_id) { ls_id_ = ls_id; }
  const ObLSID &get_ls_id() const { return ls_id_; }

  void set_feedback_type(FeedbackType feedback_type) { feedback_type_ = feedback_type; }
  const FeedbackType &get_feedback_type() const { return feedback_type_; }

  void set_fetch_status(const ObCdcFetchStatus &fetch_status) { fetch_status_ = fetch_status; }
  const ObCdcFetchStatus& get_fetch_status() const { return fetch_status_; }
  void set_l2s_net_time(const int64_t l2s_net_time) { fetch_status_.l2s_net_time_ = l2s_net_time; }
  void set_svr_queue_time(const int64_t svr_q_time) { fetch_status_.svr_queue_time_ = svr_q_time; }
  void set_process_time(const int64_t process_time) { fetch_status_.ext_process_time_ = process_time; }
  void inc_log_fetch_time(const int64_t log_fetch_time) {
    ATOMIC_AAF(&fetch_status_.log_fetch_time_, log_fetch_time);
  }

  // For Fetch GroupLogEntry
  // The start LSN of the next RPC request.
  // 1. The initial value is start LSN of the current RPC request.
  //    So next RPC request will retry even if without fetching one log.
  // 2. We will update next_req_lsn_ when fetching log successfully, in order to help next RPC request.
  LSN get_next_req_lsn() const { return next_req_lsn_; }
  void set_next_req_lsn(LSN next_req_lsn) { next_req_lsn_ = next_req_lsn; }

  // For Fetch missing LogEntry
  // 1. The initial value is the first missing LSN of the RPC request.
  //    So next RPC request will retry even if without fetching one missing log.
  // 2. We will update next_req_lsn_ when fetching missing log, in order to help next RPC request.
  // 3. If all missing log are filled, the next_req_lsn_ is the last missing log LSN.
  // The Caller needs to handle the above scenarios.
  LSN get_next_miss_lsn() const { return next_req_lsn_; }
  void set_next_miss_lsn(LSN next_miss_lsn) { next_req_lsn_ = next_miss_lsn; }

  const char *get_log_entry_buf() const { return log_entry_buf_; }
  char *get_log_entry_buf() { return log_entry_buf_; }
  int64_t get_log_num() const { return log_num_; }
  int64_t get_pos() const { return pos_; }

  void set_progress(int64_t progress) { server_progress_ = progress; }
  int64_t get_progress() const { return server_progress_; }

  inline char *get_remain_buf(int64_t &remain_size)
  {
    remain_size = FETCH_BUF_LEN - pos_;
    return log_entry_buf_ + pos_;
  }
  inline bool has_enough_buffer(const int64_t want_size) const
  {
    return (pos_ + want_size) <= FETCH_BUF_LEN;
  }
  inline void log_entry_filled(const int64_t want_size)
  {
    pos_ += want_size;
    log_num_++;
  }
  bool log_reach_threshold() const {
    return pos_ > FETCH_BUF_THRESHOLD;
  }
  bool is_valid() const
  {
    return pos_ >= 0 && pos_ <= FETCH_BUF_LEN;
  }

  TO_STRING_KV(
      K_(rpc_ver),
      K_(err),
      K_(debug_err),
      K_(ls_id),
      K_(feedback_type),
      K_(fetch_status),
      K_(next_req_lsn),
      K_(log_num),
      K_(pos));
  OB_UNIS_VERSION(1);

private:
  static const int64_t FETCH_BUF_LEN = palf::MAX_LOG_BUFFER_SIZE * 8;
  static const int64_t FETCH_BUF_THRESHOLD = FETCH_BUF_LEN - palf::MAX_LOG_BUFFER_SIZE;

private:
  int64_t rpc_ver_;
  int err_;
  int debug_err_;
  ObLSID ls_id_;
  FeedbackType feedback_type_;
  ObCdcFetchStatus fetch_status_;
  LSN next_req_lsn_;
  int64_t log_num_;
  int64_t pos_;
  char log_entry_buf_[FETCH_BUF_LEN];
  int64_t server_progress_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObCdcLSFetchLogResp);
};

class ObCdcLSFetchMissLogReq
{
  static const int64_t CUR_RPC_VER = 1;
public:
  struct MissLogParam
  {
    LSN miss_lsn_;

    TO_STRING_KV("lsn", miss_lsn_);
    OB_UNIS_VERSION(1);
  };
  typedef common::ObSEArray<MissLogParam, 16> MissLogParamArray;
public:
  ObCdcLSFetchMissLogReq() { reset(); }
  ~ObCdcLSFetchMissLogReq() {}
  ObCdcLSFetchMissLogReq &operator=(const ObCdcLSFetchMissLogReq &other);
  bool operator==(const ObCdcLSFetchMissLogReq &that) const;
  bool operator!=(const ObCdcLSFetchMissLogReq &that) const;
public:
  void reset();
  bool is_valid() const { return ls_id_.is_valid(); }

  int64_t get_rpc_version() const { return rpc_ver_; }
  void set_rpc_version(const int64_t ver) { rpc_ver_ = ver; }

  void set_ls_id(const ObLSID &ls_id) { ls_id_ = ls_id; }
  const ObLSID &get_ls_id() const { return ls_id_; }

  const MissLogParamArray &get_miss_log_array() const { return miss_log_array_; }
  int append_miss_log(const MissLogParam &param);

  void set_client_pid(const uint64_t id) { client_pid_ = id; }
  uint64_t get_client_pid() const { return client_pid_; }

  void set_client_id(const ObCdcRpcId &id) { client_id_ = id; }
  const ObCdcRpcId &get_client_id() const { return client_id_; }

  void set_flag(int8_t flag) { flag_ |= flag; }
  int8_t get_flag() const { return flag_; }

  void set_tenant_id(const uint64_t tenant_id) { tenant_id_ = tenant_id; }
  uint64_t get_tenant_id() const { return tenant_id_; }

  void set_compressor_type(const common::ObCompressorType &compressor_type) { compressor_type_ = compressor_type; }
  common::ObCompressorType get_compressor_type() const { return compressor_type_; }

  TO_STRING_KV(K_(rpc_ver),
      K_(ls_id),
      K_(miss_log_array),
      K_(client_pid),
      K_(flag),
      K_(compressor_type));
  OB_UNIS_VERSION(1);

private:
  int64_t rpc_ver_;
  ObLSID ls_id_;
  MissLogParamArray miss_log_array_;
  uint64_t client_pid_;  // Process ID.
  ObCdcRpcId client_id_;
  int8_t flag_;
  common::ObCompressorType compressor_type_;
  uint64_t tenant_id_;
};

} // namespace obrpc
} // namespace oceanbase

#endif
