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

#include <gtest/gtest.h>
#include <vector>
#include <algorithm>

#include "share/ob_define.h"
#include "storage/ob_storage_log_type.h"
#include "storage/transaction/ob_trans_log.h"

#include "liboblog/src/ob_log_instance.h"
#include "liboblog/src/ob_log_fetcher_stream.h"
#include "liboblog/src/ob_log_fetcher_part_stream.h"

#include "ob_log_utils.h"     // get_timestamp

using namespace oceanbase;
using namespace common;
using namespace liboblog;
using namespace fetcher;
using namespace transaction;
using namespace storage;
using namespace clog;

namespace oceanbase
{
namespace unittest
{

/*
 * Utils.
 */
typedef std::vector<ObAddr> Svrs;
typedef std::vector<ObPartitionKey> PKeys;
typedef std::vector<uint64_t> LogIds;
typedef std::vector<int64_t> Tstamps;

/*
 * Mock Rpc Interface 1.
 * It owns N partitions, each has M log entries.
 * It returns L log entries in each fetch_log() call.
 * Log entry contains nothing.
 * Used to test:
 *  - add partition into Stream.
 *  - update file id & offset.
 *  - fetch log.
 *  - kick out offline partitions.
 *  - discard partitions.
 */
class MockRpcInterface1 : public IFetcherRpcInterface
{
  struct Entry
  {
    file_id_t file_id_;
    offset_t offset_;
    uint64_t log_id_;
    ObPartitionKey pkey_;
  };
  // Use offset_ as index of Entry in EntryVec.
  typedef std::vector<Entry> EntryVec;
public:
  MockRpcInterface1(const PKeys &pkeys,
                    const int64_t log_entry_per_p,
                    const int64_t log_entry_per_call)
  {
    log_entry_per_call_ = log_entry_per_call;
    log_entry_per_p_ = log_entry_per_p;
    addr_ = ObAddr(ObAddr::IPV4, "127.0.0.1", 5999);
    // Gen entries.
    int64_t log_entry_cnt = 0;
    for (int64_t log_id = 1; log_id < log_entry_per_p + 1; ++log_id) {
      for (int64_t pidx = 0, cnt = pkeys.size(); pidx < cnt; ++pidx) {
        // Gen entry.
        Entry entry;
        entry.pkey_ = pkeys.at(pidx);
        entry.file_id_ = 1;
        entry.offset_ = static_cast<offset_t>(log_entry_cnt++);
        entry.log_id_ = log_id;
        // Save it.
        entries_.push_back(entry);
      }
    }
  }

  virtual ~MockRpcInterface1() { }

  virtual void set_svr(const common::ObAddr& svr) { UNUSED(svr); }

  virtual const ObAddr& get_svr() const { static ObAddr svr; return svr; }

  virtual void set_timeout(const int64_t timeout) { UNUSED(timeout); }

  virtual int req_start_log_id_by_ts(
    const obrpc::ObLogReqStartLogIdByTsRequest& req,
    obrpc::ObLogReqStartLogIdByTsResponse& res)
  {
    UNUSED(req);
    UNUSED(res);
    return OB_NOT_IMPLEMENT;
  }

  virtual int req_start_log_id_by_ts_2(const obrpc::ObLogReqStartLogIdByTsRequestWithBreakpoint &req,
                                       obrpc::ObLogReqStartLogIdByTsResponseWithBreakpoint &res) {
    UNUSED(req);
    UNUSED(res);
    return OB_NOT_IMPLEMENT;
  }

  virtual int req_start_pos_by_log_id_2(const obrpc::ObLogReqStartPosByLogIdRequestWithBreakpoint &req,
                                        obrpc::ObLogReqStartPosByLogIdResponseWithBreakpoint &res) {
    UNUSED(req);
    UNUSED(res);
    return OB_NOT_IMPLEMENT;
  }

  virtual int req_start_pos_by_log_id(
    const obrpc::ObLogReqStartPosByLogIdRequest& req,
    obrpc::ObLogReqStartPosByLogIdResponse& res)
  {
    /*
     * Err Supportted:
     *  - OB_SUCCESS
     *  - OB_ENTRY_NOT_EXIST: partition exists without any log
     *  - OB_ERR_OUT_OF_LOWER_BOUND: log id beyond lower bound
     *  - OB_ERR_OUT_OF_UPPER_BOUND: log id beyond upper bound
     */
    typedef obrpc::ObLogReqStartPosByLogIdRequest::Param Param;
    typedef obrpc::ObLogReqStartPosByLogIdResponse::Result Result;
    for (int64_t idx = 0, cnt = req.get_params().count(); idx < cnt; ++idx) {
      // Locate for a partition.
      const Param &param = req.get_params().at(idx);
      const ObPartitionKey &pkey = param.pkey_;
      const uint64_t start_log_id = param.start_log_id_;
      Result result;
      result.reset();
      // Search.
      bool done = false;
      bool partition_exist = false;
      for (int64_t entry_idx = 0, entry_cnt = entries_.size();
           _SUCC_(result.err_) && !done && entry_idx < entry_cnt;
           ++entry_idx) {
        const Entry &entry = entries_.at(entry_idx);
        if (entry.pkey_ == pkey) {
          partition_exist = true;
          // Got it.
          if (start_log_id == entry.log_id_) {
            result.err_ = OB_SUCCESS;
            result.file_id_ = 1;
            result.offset_ = entry.offset_;
            done = true;
          }
          // Too small log id.
          else if (start_log_id < entry.log_id_) {
            result.err_ = OB_ERR_OUT_OF_LOWER_BOUND;
          }
        }
      }
      if (!done && _SUCC_(result.err_)) {
        // No log entry.
        if (!partition_exist) {
          result.err_ = OB_ENTRY_NOT_EXIST;
        }
        // Too large log id.
        else {
          result.err_ = OB_ERR_OUT_OF_UPPER_BOUND;
        }
      }
      res.append_result(result);
    }
    _D_("mock rpc 1 req pos", K(req), K(res));
    return OB_SUCCESS;
  }

  virtual int fetch_log(
    const obrpc::ObLogExternalFetchLogRequest& req,
    obrpc::ObLogExternalFetchLogResponse& res)
  {
    typedef obrpc::ObLogExternalFetchLogRequest::Param Param;
    typedef obrpc::ObLogExternalFetchLogResponse::OfflinePartition OP;

    // Fetch log.
    const offset_t offset = req.get_offset();
    if (offset < 0) {
      return OB_INVALID_ARGUMENT;
    }
    offset_t ret_offset = offset;
    // Scan.
    for (int64_t idx = static_cast<int64_t>(offset), cnt = entries_.size();
         idx < cnt && res.get_log_num() < log_entry_per_call_;
         ++idx) {
      const Entry &entry = entries_.at(idx);
      bool fetch = false;
      for (int64_t pidx = 0, pcnt = req.get_params().count();
           !fetch && pidx < pcnt;
           ++pidx) {
        const Param &param = req.get_params().at(pidx);
        if (entry.pkey_ == param.pkey_
            && param.start_log_id_ <= entry.log_id_
            && entry.log_id_ <= param.last_log_id_) {
          fetch = true;
        }
      }
      if (fetch) {
        ret_offset = static_cast<offset_t>(entry.offset_);
        // Gen header.
        int64_t ts = get_timestamp();
        ObProposalID proposal_id;
        proposal_id.addr_ = addr_;
        proposal_id.ts_ = ts;
        ObLogEntryHeader header;
        header.generate_header(OB_LOG_SUBMIT, entry.pkey_,
                               entry.log_id_, mock_load_, mock_load_len_,
                               ts, ts, proposal_id, ts, ObVersion(1));
        ObLogEntry log_entry;
        log_entry.generate_entry(header, mock_load_);
        res.append_log(log_entry);
      }
    }
    res.set_file_id_offset(1, ret_offset + 1);

    // Handle offline partition.
    // Here, if a partition reaches its last log, it is offline.
    for (int64_t idx = 0, cnt = req.get_params().count(); idx < cnt; ++idx) {
      const Param &param = req.get_params().at(idx);
      const uint64_t last_log_id =  log_entry_per_p_;
      if (last_log_id < param.start_log_id_) {
        OP op;
        op.pkey_ = param.pkey_;
        // op.last_log_id_ = last_log_id;
        op.sync_ts_ = last_log_id;
        res.append_offline_partition(op);
      }
    }

    _D_("mock rpc 1 fetch log", K(req), K(res));

    return OB_SUCCESS;
  }

  virtual int req_heartbeat_info(
    const obrpc::ObLogReqHeartbeatInfoRequest& req,
    obrpc::ObLogReqHeartbeatInfoResponse& res)
  {
    UNUSED(req);
    UNUSED(res);
    return OB_NOT_IMPLEMENT;
  }

  virtual int req_leader_heartbeat(
    const obrpc::ObLogLeaderHeartbeatReq &req,
    obrpc::ObLogLeaderHeartbeatResp &res)
  {
    UNUSED(req);
    UNUSED(res);
    return OB_NOT_IMPLEMENT;
  }

  virtual int open_stream(const obrpc::ObLogOpenStreamReq &req,
                          obrpc::ObLogOpenStreamResp &res) {
    UNUSED(req);
    UNUSED(res);
    return OB_NOT_IMPLEMENT;
  }

  virtual int fetch_stream_log(const obrpc::ObLogStreamFetchLogReq &req,
                               obrpc::ObLogStreamFetchLogResp &res) {
    UNUSED(req);
    UNUSED(res);
    return OB_NOT_IMPLEMENT;
  }

  virtual int req_svr_feedback(const ReqLogSvrFeedback &feedback)
  {
    UNUSED(feedback);
    return OB_SUCCESS;
  }
private:
  ObAddr addr_;
  int64_t log_entry_per_call_;
  int64_t log_entry_per_p_;
  EntryVec entries_;
  static const int64_t mock_load_len_ = 8;
  char mock_load_[mock_load_len_];
};

/*
 * Factory.
 */
class MockRpcInterface1Factory : public IFetcherRpcInterfaceFactory
{
public:
  MockRpcInterface1Factory(const PKeys &pkeys,
                           const int64_t log_entry_per_p,
                           const int64_t log_entry_per_call)
    : pkeys_(pkeys),
    log_entry_per_p_(log_entry_per_p),
    log_entry_per_call_(log_entry_per_call)
  { }
  virtual int new_fetcher_rpc_interface(IFetcherRpcInterface*& rpc)
  {
    rpc = new MockRpcInterface1(pkeys_, log_entry_per_p_, log_entry_per_call_);
    return OB_SUCCESS;
  }
  virtual int delete_fetcher_rpc_interface(IFetcherRpcInterface* rpc)
  {
    delete rpc;
    return OB_SUCCESS;
  }
private:
  PKeys pkeys_;
  int64_t log_entry_per_p_;
  int64_t log_entry_per_call_;
};

/*
 * Mock Rpc Interface 2.
 * It owns N servers, each hold some partitions, one of them is
 * the leader. When request start log id, a preseted value is returned.
 * Used to test:
 *  - fetch partition
 *  - locate start log id
 *  - activate partition stream
 *  - discard partition stream
 */
class MockRpcInterface2 : public IFetcherRpcInterface
{
  struct Partition
  {
    ObPartitionKey pkey_;
    uint64_t start_log_id_;
    bool is_leader_;
  };
  struct Svr
  {
    ObAddr svr_;
    std::vector<Partition> partitions_;
    bool operator==(const Svr &other) const
    {
      return svr_ == other.svr_;
    }
    bool operator<(const Svr &other) const
    {
      return svr_ < other.svr_;
    }
  };
public:
  /*
   * Set static result set. The first partition in svrs is the leader.
   */
  static void add_partition(const ObPartitionKey &pkey,
                            uint64_t start_log_id,
                            std::vector<ObAddr> svrs)
  {
    EXPECT_NE(0, svrs.size());
    Partition pt = { pkey, start_log_id, false };
    for (int64_t idx = 0, cnt = svrs.size();
         idx < cnt;
         ++idx) {
      pt.is_leader_ = (0 == idx);
      Svr target;
      target.svr_ = svrs.at(idx);
      std::vector<Svr>::iterator itor =
        std::find(svrs_.begin(), svrs_.end(), target);
      if (svrs_.end() == itor) {
        target.partitions_.push_back(pt);
        svrs_.push_back(target);
        std::sort(svrs_.begin(), svrs_.end());
      }
      else {
        (*itor).partitions_.push_back(pt);
      }
    }
  }
  /*
   * Clear static result set.
   */
  static void clear_result_set()
  {
    svrs_.clear();
  }
public:
  virtual void set_svr(const common::ObAddr& svr) { svr_ = svr; }

  virtual const ObAddr& get_svr() const { return svr_; }

  virtual void set_timeout(const int64_t timeout) { UNUSED(timeout); }

  virtual int req_start_log_id_by_ts(
    const obrpc::ObLogReqStartLogIdByTsRequest& req,
    obrpc::ObLogReqStartLogIdByTsResponse& res)
  {
    bool svr_exist = false;
    for (int64_t idx = 0, cnt = req.get_params().count(); idx < cnt; ++idx) {
      const ObPartitionKey &pkey = req.get_params().at(idx).pkey_;
      bool done = false;
      for (int64_t svr_idx = 0, svr_cnt = svrs_.size();
           svr_idx < svr_cnt;
           ++svr_idx) {
        // Simulating sending rpc to svr.
        if (svr_ == svrs_.at(svr_idx).svr_) {
          svr_exist = true;
          const Svr &svr = svrs_.at(svr_idx);
          for (int64_t pidx = 0, pcnt = svr.partitions_.size();
               pidx < pcnt;
               ++pidx) {
            const Partition &p = svr.partitions_.at(pidx);
            if (pkey == p.pkey_) {
              done = true;
              typedef obrpc::ObLogReqStartLogIdByTsResponse::Result Result;
              Result result = { OB_SUCCESS, p.start_log_id_, false};
              res.append_result(result);
            }
          }
          if (!done) {
            res.set_err(OB_PARTITION_NOT_EXIST);
          }
        }
      } // End for.
    }

    _D_("mock rpc req start log id", K(req), K(res));

    return (svr_exist) ? OB_SUCCESS : OB_TIMEOUT;
  }

  virtual int req_start_log_id_by_ts_2(const obrpc::ObLogReqStartLogIdByTsRequestWithBreakpoint &req,
                                       obrpc::ObLogReqStartLogIdByTsResponseWithBreakpoint &res) {
    UNUSED(req);
    UNUSED(res);
    return OB_NOT_IMPLEMENT;
  }

  virtual int req_start_pos_by_log_id_2(const obrpc::ObLogReqStartPosByLogIdRequestWithBreakpoint &req,
                                        obrpc::ObLogReqStartPosByLogIdResponseWithBreakpoint &res) {
    UNUSED(req);
    UNUSED(res);
    return OB_NOT_IMPLEMENT;
  }

  virtual int req_start_pos_by_log_id(
    const obrpc::ObLogReqStartPosByLogIdRequest& req,
    obrpc::ObLogReqStartPosByLogIdResponse& res)
  {
    UNUSED(req);
    UNUSED(res);
    // Timeout.
    _D_("mock rpc req pos by log id", K(req), K(res));
    return OB_TIMEOUT;
  }

  virtual int fetch_log(
    const obrpc::ObLogExternalFetchLogRequest& req,
    obrpc::ObLogExternalFetchLogResponse& res)
  {
    UNUSED(req);
    UNUSED(res);
    // Timeout.
    _D_("mock rpc req pos by log id", K(req), K(res));
    return OB_TIMEOUT;
  }

  virtual int req_heartbeat_info(
    const obrpc::ObLogReqHeartbeatInfoRequest& req,
    obrpc::ObLogReqHeartbeatInfoResponse& res)
  {
    UNUSED(req);
    UNUSED(res);
    return OB_NOT_IMPLEMENT;
  }

  virtual int req_leader_heartbeat(
    const obrpc::ObLogLeaderHeartbeatReq &req,
    obrpc::ObLogLeaderHeartbeatResp &res)
  {
    UNUSED(req);
    UNUSED(res);
    return OB_NOT_IMPLEMENT;
  }

  virtual int open_stream(const obrpc::ObLogOpenStreamReq &req,
                          obrpc::ObLogOpenStreamResp &res) {
    UNUSED(req);
    UNUSED(res);
    return OB_NOT_IMPLEMENT;
  }

  virtual int fetch_stream_log(const obrpc::ObLogStreamFetchLogReq &req,
                               obrpc::ObLogStreamFetchLogResp &res) {
    UNUSED(req);
    UNUSED(res);
    return OB_NOT_IMPLEMENT;
  }
  virtual int get_log_svr(const ObPartitionKey& pkey, const uint64_t log_id,
                          ObSvrs& svrs, int& leader_cnt)
  {
    UNUSED(log_id);
    leader_cnt = 0;
    for (int64_t svr_idx = 0, svr_cnt = svrs_.size();
         svr_idx < svr_cnt;
         ++svr_idx) {
      Svr &svr = svrs_.at(svr_idx);
      for (int64_t pidx = 0, pcnt = svr.partitions_.size();
           pidx < pcnt;
           ++pidx) {
        const Partition &p = svr.partitions_.at(pidx);
        if (pkey == p.pkey_) {
          svrs.push_back(svr.svr_);
          if (p.is_leader_) {
            std::swap(svrs.at(leader_cnt), svrs.at(svrs.count() - 1));
            leader_cnt += 1;
          }
        }
      }
    }
    _D_("mock rpc req log servers", K(pkey), K(svrs), K(leader_cnt));
    return OB_SUCCESS;
  }
  virtual int req_svr_feedback(const ReqLogSvrFeedback &feedback)
  {
    UNUSED(feedback);
    return OB_SUCCESS;
  }
private:
  // Target svr.
  ObAddr svr_;
  // Data set.
  static std::vector<Svr> svrs_;
};
// Static data set. So all instances could access it.
std::vector<MockRpcInterface2::Svr> MockRpcInterface2::svrs_;

/*
 * Factory.
 */
class MockRpcInterface2Factory : public IFetcherRpcInterfaceFactory
{
public:
  virtual int new_fetcher_rpc_interface(IFetcherRpcInterface*& rpc)
  {
    rpc = new MockRpcInterface2();
    return OB_SUCCESS;
  }
  virtual int delete_fetcher_rpc_interface(IFetcherRpcInterface* rpc)
  {
    delete rpc;
    return OB_SUCCESS;
  }
};

/*
 * Mock Rpc Interface 3.
 * It owns some servers and partitions, can return
 * svr addr and heartbeat timestamps.
 * Notice: user set <svr, partition key, log id, tstamp> tuples
 * as results.
 * Used to test:
 *  - Heartbeat facilities.
 */
class MockRpcInterface3 : public IFetcherRpcInterface
{
  struct Entry
  {
    ObAddr svr_;
    ObPartitionKey pkey_;
    uint64_t log_id_;
    int64_t tstamp_;
    bool operator<(const Entry &other)
    {
      return log_id_ < other.log_id_;
    }
  };
  typedef std::vector<Entry> EntryVec;
public:
  static void clear_result()
  {
    entry_vec_.clear();
  }
  static void add_result(const ObAddr &svr, const ObPartitionKey &pkey,
                         const uint64_t log_id, const int64_t tstamp)
  {
    Entry entry = { svr, pkey, log_id, tstamp };
    entry_vec_.push_back(entry);
  }
public:
  virtual void set_svr(const common::ObAddr& svr) { UNUSED(svr); }

  virtual const ObAddr& get_svr() const { static ObAddr svr; return svr; }

  virtual void set_timeout(const int64_t timeout) { UNUSED(timeout); }

  virtual int req_start_log_id_by_ts(
    const obrpc::ObLogReqStartLogIdByTsRequest& req,
    obrpc::ObLogReqStartLogIdByTsResponse& res)
  {
    UNUSED(req);
    UNUSED(res);
    return OB_NOT_IMPLEMENT;
  }

  virtual int req_start_log_id_by_ts_2(const obrpc::ObLogReqStartLogIdByTsRequestWithBreakpoint &req,
                                       obrpc::ObLogReqStartLogIdByTsResponseWithBreakpoint &res) {
    UNUSED(req);
    UNUSED(res);
    return OB_NOT_IMPLEMENT;
  }

  virtual int req_start_pos_by_log_id_2(const obrpc::ObLogReqStartPosByLogIdRequestWithBreakpoint &req,
                                        obrpc::ObLogReqStartPosByLogIdResponseWithBreakpoint &res) {
    UNUSED(req);
    UNUSED(res);
    return OB_NOT_IMPLEMENT;
  }

  virtual int req_start_pos_by_log_id(
    const obrpc::ObLogReqStartPosByLogIdRequest& req,
    obrpc::ObLogReqStartPosByLogIdResponse& res)
  {
    UNUSED(req);
    UNUSED(res);
    return OB_NOT_IMPLEMENT;
  }

  virtual int fetch_log(
    const obrpc::ObLogExternalFetchLogRequest& req,
    obrpc::ObLogExternalFetchLogResponse& res)
  {
    UNUSED(req);
    UNUSED(res);
    return OB_NOT_IMPLEMENT;
  }

  virtual int req_heartbeat_info(
    const obrpc::ObLogReqHeartbeatInfoRequest& req,
    obrpc::ObLogReqHeartbeatInfoResponse& res)
  {
    typedef obrpc::ObLogReqHeartbeatInfoRequest::Param Param;
    typedef obrpc::ObLogReqHeartbeatInfoResponse::Result Result;
    // Itor params.
    for (int64_t idx = 0, cnt = req.get_params().count(); idx < cnt; ++idx) {
      const Param &param = req.get_params().at(idx);
      // Find result.
      bool done = false;
      for (int64_t idx2 = 0, cnt2 = entry_vec_.size();
           !done && idx2 < cnt2;
           ++idx2) {
        const Entry &entry = entry_vec_[idx2];
        if (param.pkey_ == entry.pkey_
            && param.log_id_ == entry.log_id_) {
          done = true;
          Result result;
          result.err_ = OB_SUCCESS;
          result.tstamp_ = entry.tstamp_;
          res.append_result(result);
        }
      }
      if (!done) {
        Result result;
        result.err_ = OB_NEED_RETRY;
        res.append_result(result);
      }
    }

    _D_("mock rpc: req heartbeat", K(req), K(res));
    return OB_SUCCESS;
  }

  virtual int req_leader_heartbeat(
    const obrpc::ObLogLeaderHeartbeatReq &req,
    obrpc::ObLogLeaderHeartbeatResp &res)
  {
    UNUSED(req);
    UNUSED(res);
    return OB_NOT_IMPLEMENT;
  }

  virtual int open_stream(const obrpc::ObLogOpenStreamReq &req,
                          obrpc::ObLogOpenStreamResp &res) {
    UNUSED(req);
    UNUSED(res);
    return OB_NOT_IMPLEMENT;
  }

  virtual int fetch_stream_log(const obrpc::ObLogStreamFetchLogReq &req,
                               obrpc::ObLogStreamFetchLogResp &res) {
    UNUSED(req);
    UNUSED(res);
    return OB_NOT_IMPLEMENT;
  }

  virtual int get_log_svr(const ObPartitionKey& pkey, const uint64_t log_id,
                          ObSvrs& svrs, int& leader_cnt)
  {
    // Todo. In this version, only one result is enough, log_id is not used.
    UNUSED(log_id);
    UNUSED(leader_cnt);

    for (int64_t idx = 0, cnt = entry_vec_.size(); idx < cnt; ++idx) {
      const Entry &entry = entry_vec_[idx];
      if (pkey == entry.pkey_) {
        svrs.push_back(entry.svr_);
        break;
      }
    }
    return OB_SUCCESS;
  }

  virtual int req_svr_feedback(const ReqLogSvrFeedback &feedback)
  {
    UNUSED(feedback);
    return OB_SUCCESS;
  }
private:
  static EntryVec entry_vec_;
};

MockRpcInterface3::EntryVec MockRpcInterface3::entry_vec_;

/*
 * Factory.
 */
class MockRpcInterface3Factory : public IFetcherRpcInterfaceFactory
{
public:
  virtual int new_fetcher_rpc_interface(IFetcherRpcInterface*& rpc)
  {
    rpc = new MockRpcInterface3();
    return OB_SUCCESS;
  }
  virtual int delete_fetcher_rpc_interface(IFetcherRpcInterface* rpc)
  {
    delete rpc;
    return OB_SUCCESS;
  }
};

/*
 * TransLog Generator 1.
 * Generate single partition transaction logs.
 * Support get trans logs in CORRECT order.
 * Use:
 *  - Call next_trans(), specify trans params.
 *  - Get logs in correct order: redo, redo, ..., prepare, commit/abort.
 */
struct TransParam1
{
  // Params used in trans log.
  ObPartitionKey pkey_;
  ObTransID trans_id_;
  ObAddr scheduler_;
  ObPartitionKey coordinator_;
  ObPartitionArray participants_;
  ObStartTransParam trans_param_;
};

class TransLogGenerator1
{
public:
  TransLogGenerator1()
    : param_(),
    redo_(),
    prepare_(),
    commit_(),
    abort_()
  { }
  virtual ~TransLogGenerator1() { }
public:
  void next_trans(const TransParam1 &param)
  {
    param_ = param;
  }
  const ObTransRedoLog& next_redo(const uint64_t log_id)
  {
    int err = OB_SUCCESS;
    uint64_t tenant_id = 100;
    const uint64_t cluster_id = 1000;
    redo_.reset();
    ObVersion active_memstore_version(1);
    err = redo_.init(OB_LOG_TRANS_REDO, param_.pkey_, param_.trans_id_,
                     tenant_id, log_id, param_.scheduler_, param_.coordinator_,
                     param_.participants_, param_.trans_param_, cluster_id, active_memstore_version);
    EXPECT_EQ(OB_SUCCESS, err);
    ObTransMutator &mutator = redo_.get_mutator();
    if (NULL == mutator.get_mutator_buf()) {
      mutator.init(true);
    }
    const char *data = "fly";
    char *buf = static_cast<char*>(mutator.alloc(strlen(data)));
    strcpy(buf, data);
    return redo_;
  }
  const ObTransPrepareLog& next_prepare(const ObRedoLogIdArray &all_redos)
  {
    int err = OB_SUCCESS;
    uint64_t tenant_id = 100;
    const uint64_t cluster_id = 1000;
    prepare_.reset();
    ObVersion active_memstore_version(1);
    err = prepare_.init(OB_LOG_TRANS_PREPARE, param_.pkey_, param_.trans_id_,
                        tenant_id, param_.scheduler_, param_.coordinator_,
                        param_.participants_, param_.trans_param_,
                        OB_SUCCESS, all_redos, 0, cluster_id, active_memstore_version);
    EXPECT_EQ(OB_SUCCESS, err);
    return prepare_;
  }
  const ObTransCommitLog& next_commit(const uint64_t prepare_log_id)
  {
    int err = OB_SUCCESS;
    const uint64_t cluster_id = 1000;
    ObPartitionLogInfo ptl_id(param_.pkey_, prepare_log_id, get_timestamp());
    PartitionLogInfoArray ptl_ids;
    if (OB_INVALID_ID == prepare_log_id) {
      // Pass. For prepare-commit trans log.
    }
    else {
      err = ptl_ids.push_back(ptl_id);
      EXPECT_EQ(OB_SUCCESS, err);
    }
    commit_.reset();
    err = commit_.init(OB_LOG_TRANS_COMMIT, param_.pkey_, param_.trans_id_,
                       ptl_ids, 1, 0, cluster_id);
    EXPECT_EQ(OB_SUCCESS, err);
    return commit_;
  }
  const ObTransAbortLog& next_abort()
  {
    int err = OB_SUCCESS;
    const uint64_t cluster_id = 1000;
    PartitionLogInfoArray array;
    abort_.reset();
    err = abort_.init(OB_LOG_TRANS_ABORT, param_.pkey_, param_.trans_id_, array, cluster_id);
    EXPECT_EQ(OB_SUCCESS, err);
    return abort_;
  }
private:
  TransParam1 param_;
  ObTransRedoLog redo_;
  ObTransPrepareLog prepare_;
  ObTransCommitLog commit_;
  ObTransAbortLog abort_;
};

/*
 * Transaction Log Entry Generator 1.
 * Generate log entries of transactions.
 */
class TransLogEntryGenerator1
{
public:
  TransLogEntryGenerator1(const ObPartitionKey &pkey)
    : pkey_(pkey),
    log_id_(0),
    remain_log_cnt_(0),
    commit_(false),
    param_(),
    trans_log_gen_(),
    prepare_id_(0),
    redos_(),
    data_len_(0)
  {
    ObAddr addr(ObAddr::IPV4, "127.0.0.1", 5566);
    param_.pkey_ = pkey_;
    param_.trans_id_ = ObTransID(addr);
    param_.scheduler_ = addr;
    param_.coordinator_ = pkey_;
    int err = param_.participants_.push_back(pkey_);
    EXPECT_EQ(OB_SUCCESS, err);
    param_.trans_param_.set_access_mode(ObTransAccessMode::READ_WRITE);
    param_.trans_param_.set_isolation(ObTransIsolation::READ_COMMITED);
    param_.trans_param_.set_type(ObTransType::TRANS_NORMAL);

    buf_ = new char[buf_len_];
    EXPECT_TRUE(NULL != buf_);
  }
  virtual ~TransLogEntryGenerator1()
  {
    delete[] buf_;
  }
  // Generate normal trans.
  // Start a new trans.
  void next_trans(const int64_t redo_cnt, bool commit)
  {
    remain_log_cnt_ = 2 + redo_cnt;
    commit_ = commit;
    redos_.reset();
    trans_log_gen_.next_trans(param_);
  }
  // Get next log entry.
  int next_log_entry(ObLogEntry &log_entry)
  {
    int ret = OB_SUCCESS;
    if (2 < remain_log_cnt_) {
      next_redo_(log_entry);
      // Store redo id.
      int err = redos_.push_back(log_id_);
      EXPECT_EQ(OB_SUCCESS, err);
      log_id_ += 1;
      remain_log_cnt_ -= 1;
    }
    else if (2 == remain_log_cnt_) {
      next_prepare_(log_entry);
      prepare_id_ = log_id_;
      log_id_ += 1;
      remain_log_cnt_ -= 1;
    }
    else if (1 == remain_log_cnt_ && commit_) {
      next_commit_(log_entry);
      log_id_ += 1;
      remain_log_cnt_ -= 1;
    }
    else if (1 == remain_log_cnt_ && !commit_) {
      next_abort_(log_entry);
      log_id_ += 1;
      remain_log_cnt_ -= 1;
    }
    else {
      ret = OB_ITER_END;
    }
    return ret;
  }
  // Generate: redo, redo, redo, prepare-commit.
  int next_log_entry_2(ObLogEntry &log_entry)
  {
    int ret = OB_SUCCESS;
    if (2 < remain_log_cnt_) {
      next_redo_(log_entry);
      // Store redo id.
      int err = redos_.push_back(log_id_);
      EXPECT_EQ(OB_SUCCESS, err);
      log_id_ += 1;
      remain_log_cnt_ -= 1;
    }
    else if (2 == remain_log_cnt_ && commit_) {
      next_prepare_with_commit(log_entry);
      log_id_ += 1;
      remain_log_cnt_ -= 2;
    }
    else if (2 == remain_log_cnt_ && !commit_) {
      next_prepare_(log_entry);
      log_id_ += 1;
      remain_log_cnt_ -= 1;
    }
    else if (1 == remain_log_cnt_ && !commit_) {
      next_abort_(log_entry);
      log_id_ += 1;
      remain_log_cnt_ -= 1;
    }
    else {
      ret = OB_ITER_END;
    }
    return ret;
  }
private:
  void next_redo_(ObLogEntry &log_entry)
  {
    int err = OB_SUCCESS;
    // Gen trans log.
    const ObTransRedoLog &redo = trans_log_gen_.next_redo(log_id_);
    int64_t pos = 0;
    err = serialization::encode_i64(buf_, buf_len_, pos, OB_LOG_TRANS_REDO);
    EXPECT_EQ(OB_SUCCESS, err);
    err = serialization::encode_i64(buf_, buf_len_, pos, 0);
    EXPECT_EQ(OB_SUCCESS, err);
    err = redo.serialize(buf_, buf_len_, pos);
    EXPECT_EQ(OB_SUCCESS, err);
    data_len_ = pos;
    // Gen entry header.
    ObLogEntryHeader header;
    header.generate_header(OB_LOG_SUBMIT, pkey_, log_id_, buf_,
                           data_len_, get_timestamp(), get_timestamp(),
                           ObProposalID(), get_timestamp(), ObVersion(0));
    // Gen log entry.
    log_entry.generate_entry(header, buf_);
  }
  void next_prepare_(ObLogEntry &log_entry)
  {
    int err = OB_SUCCESS;
    // Gen trans log.
    const ObTransPrepareLog &prepare= trans_log_gen_.next_prepare(redos_);
    int64_t pos = 0;
    err = serialization::encode_i64(buf_, buf_len_, pos, OB_LOG_TRANS_PREPARE);
    EXPECT_EQ(OB_SUCCESS, err);
    err = serialization::encode_i64(buf_, buf_len_, pos, 0);
    EXPECT_EQ(OB_SUCCESS, err);
    err = prepare.serialize(buf_, buf_len_, pos);
    EXPECT_EQ(OB_SUCCESS, err);
    data_len_ = pos;
    // Gen entry header.
    ObLogEntryHeader header;
    header.generate_header(OB_LOG_SUBMIT, pkey_, log_id_, buf_,
                           data_len_, get_timestamp(), get_timestamp(),
                           ObProposalID(), get_timestamp(), ObVersion(0));
    // Gen log entry.
    log_entry.generate_entry(header, buf_);
  }
  void next_commit_(ObLogEntry &log_entry)
  {
    int err = OB_SUCCESS;
    // Gen trans log.
    const ObTransCommitLog &commit = trans_log_gen_.next_commit(prepare_id_);
    int64_t pos = 0;
    err = serialization::encode_i64(buf_, buf_len_, pos, OB_LOG_TRANS_COMMIT);
    EXPECT_EQ(OB_SUCCESS, err);
    err = serialization::encode_i64(buf_, buf_len_, pos, 0);
    EXPECT_EQ(OB_SUCCESS, err);
    err = commit.serialize(buf_, buf_len_, pos);
    EXPECT_EQ(OB_SUCCESS, err);
    data_len_ = pos;
    // Gen entry header.
    ObLogEntryHeader header;
    header.generate_header(OB_LOG_SUBMIT, pkey_, log_id_, buf_,
    data_len_, get_timestamp(), get_timestamp(),
    ObProposalID(), get_timestamp(), ObVersion(0));
    // Gen log entry.
    log_entry.generate_entry(header, buf_);
  }
  void next_abort_(ObLogEntry &log_entry)
  {
    int err = OB_SUCCESS;
    // Gen trans log.
    const ObTransAbortLog &abort = trans_log_gen_.next_abort();
    int64_t pos = 0;
    err = serialization::encode_i64(buf_, buf_len_, pos, OB_LOG_TRANS_ABORT);
    EXPECT_EQ(OB_SUCCESS, err);
    err = serialization::encode_i64(buf_, buf_len_, pos, 0);
    EXPECT_EQ(OB_SUCCESS, err);
    err = abort.serialize(buf_, buf_len_, pos);
    EXPECT_EQ(OB_SUCCESS, err);
    data_len_ = pos;
    // Gen entry header.
    ObLogEntryHeader header;
    header.generate_header(OB_LOG_SUBMIT, pkey_, log_id_, buf_,
    data_len_, get_timestamp(), get_timestamp(),
    ObProposalID(), get_timestamp(), ObVersion(0));
    // Gen log entry.
    log_entry.generate_entry(header, buf_);
  }
  void next_prepare_with_commit(ObLogEntry &log_entry)
  {
    int err = OB_SUCCESS;
    // Gen trans log.
    const ObTransPrepareLog &prepare= trans_log_gen_.next_prepare(redos_);
    const ObTransCommitLog &commit = trans_log_gen_.next_commit(OB_INVALID_ID);
    int64_t pos = 0;
    err = serialization::encode_i64(buf_, buf_len_,
                                    pos, OB_LOG_TRANS_PREPARE_WITH_COMMIT);
    EXPECT_EQ(OB_SUCCESS, err);
    err = serialization::encode_i64(buf_, buf_len_, pos, 0);
    EXPECT_EQ(OB_SUCCESS, err);
    err = prepare.serialize(buf_, buf_len_, pos);
    EXPECT_EQ(OB_SUCCESS, err);
    err = commit.serialize(buf_, buf_len_, pos);
    EXPECT_EQ(OB_SUCCESS, err);
    data_len_ = pos;
    // Gen entry header.
    ObLogEntryHeader header;
    header.generate_header(OB_LOG_SUBMIT, pkey_, log_id_, buf_,
                           data_len_, get_timestamp(), get_timestamp(),
                           ObProposalID(), get_timestamp(), ObVersion(0));
    // Gen log entry.
    log_entry.generate_entry(header, buf_);
  }
private:
  // Params.
  ObPartitionKey pkey_;
  uint64_t log_id_;
  int64_t remain_log_cnt_;
  bool commit_;
  // Gen.
  TransParam1 param_;
  TransLogGenerator1 trans_log_gen_;
  uint64_t prepare_id_;
  ObRedoLogIdArray redos_;
  // Buf.
  int64_t data_len_;
  static const int64_t buf_len_ = 2 * _M_;
  char *buf_;
};

/*
 * Mock Parser 1.
 * Read Task, revert it immediately, and count Task number.
 */
class MockParser1 : public IObLogParser
{
public:
  MockParser1() : trans_cnt_(0) { }
  virtual ~MockParser1() { }
  virtual int start() { return OB_SUCCESS; }
  virtual void stop() { }
  virtual void mark_stop_flag() { }
  virtual int push(PartTransTask* task, const int64_t timeout)
  {
    UNUSED(timeout);
    if (NULL != task && task->is_normal_trans()) {
      task->revert();
      trans_cnt_ += 1;
      // Debug.
      // _I_(">>> push parser", "req", task->get_seq());
    }
    return OB_SUCCESS;
  }
  int64_t get_trans_cnt() const { return trans_cnt_; }
private:
  int64_t trans_cnt_;
};

/*
 * Mock Fetcher Error Handler.
 */
class MockFetcherErrHandler1 : public IErrHandler
{
public:
  virtual ~MockFetcherErrHandler1() { }
public:
  virtual void handle_err(int err_no, const char* fmt, ...)
  {
    UNUSED(err_no);
    va_list ap;
    va_start(ap, fmt);
    __E__(fmt, ap);
    va_end(ap);
    abort();
  }
};

/*
 * Mock Liboblog Error Handler.
 */
class MockLiboblogErrHandler1 : public IObLogErrHandler
{
public:
  virtual void handle_error(int err_no, const char* fmt, ...)
  {
    UNUSED(err_no);
    va_list ap;
    va_start(ap, fmt);
    __E__(fmt, ap);
    va_end(ap);
  }
};

/*
 * Mock SvrProvider.
 * User set svrs into it.
 */
class MockSvrProvider1 : public sqlclient::ObMySQLServerProvider
{
public:
  virtual ~MockSvrProvider1() { }
  void add_svr(const ObAddr &svr) { svrs_.push_back(svr); }
public:
  virtual int get_cluster_list(common::ObIArray<int64_t> &cluster_list)
  {
    int ret = OB_SUCCESS;
    if (svrs_.size() > 0) {
      if (OB_FAIL(cluster_list.push_back(common::OB_INVALID_ID))) {
        LOG_WARN("fail to push back cluster_id", K(ret));
      }
    }
    return ret;
  }
  virtual int get_server(const int64_t cluster_id, const int64_t svr_idx, ObAddr& server)
  {
    UNUSED(cluster_id);
    int ret = OB_SUCCESS;
    if (0 <= svr_idx && svr_idx < static_cast<int64_t>(svrs_.size())) {
      server = svrs_[svr_idx];
    }
    else {
      ret = OB_ERR_UNEXPECTED;
    }
    return ret;
  }
  virtual int64_t get_cluster_count() const
  {
    return svrs_.size() > 0 ? 1 : 0;
  }
  virtual int64_t get_server_count(const int64_t cluster_id) const
  {
    UNUSED(cluster_id)
    return static_cast<int64_t>(svrs_.size());
  }
  virtual int refresh_server_list() { return OB_SUCCESS; }
private:
  std::vector<ObAddr> svrs_;
};

/*
 * Test Dataset Generator.
 */

/*
 * Svr Config.
 * Set svr address and mysql port.
 */
struct SvrCfg
{
  // Svr.
  const char *svr_addr_;
  int internal_port_;
  // Mysql.
  int mysql_port_;
  const char *mysql_user_;
  const char *mysql_password_;
  const char *mysql_db_;
  int64_t mysql_timeout_;
};

/*
 * Configuration for mysql connector.
 */
inline ConnectorConfig prepare_cfg_1(const SvrCfg &svr_cfg)
{
  ConnectorConfig cfg;
  cfg.mysql_addr_ = svr_cfg.svr_addr_;
  cfg.mysql_port_ = svr_cfg.mysql_port_;
  cfg.mysql_user_ = svr_cfg.mysql_user_;
  cfg.mysql_password_ = svr_cfg.mysql_password_;
  cfg.mysql_db_ = svr_cfg.mysql_db_;
  cfg.mysql_timeout_ = svr_cfg.mysql_timeout_;
  return cfg;
}

/*
 * Build table names.
 */
inline const char** prepare_table_name_1()
{
  static const char* tnames[] = {
    "table1",
    "table2",
    "table3",
    "table4",
    "table5",
    "table6",
    "table7",
    "table8",
    "table9",
    "table10",
    "table11",
    "table12",
    "table13",
    "table14",
    "table15",
    "table16"
  };
  return tnames;
}

/*
 * Build table schema.
 */
inline const char* prepare_table_schema_1()
{
  return "c1 int primary key";
}

/*
 * Create table.
 */
class CreateTable : public MySQLQueryBase
{
public:
  CreateTable(const char *tname, const char *schema)
  {
    snprintf(buf_, 512, "create table %s(%s)", tname, schema);
    sql_ = buf_;
    sql_len_ = strlen(sql_);
  }
private:
  char buf_[512];
};

/*
 * Drop table.
 */
class DropTable : public MySQLQueryBase
{
public:
  DropTable(const char *tname)
  {
    snprintf(buf_, 512, "drop table if exists %s", tname);
    sql_ = buf_;
    sql_len_ = strlen(sql_);
  }
private:
  char buf_[512];
};

/*
 * Get table id.
 */
class GetTableId : public MySQLQueryBase
{
public:
  GetTableId(const char *tname)
  {
    snprintf(buf_, 512, "select table_id "
                        "from __all_table where table_name='%s'", tname);
    sql_ = buf_;
    sql_len_ = strlen(sql_);
  }
  int get_tid(uint64_t &tid)
  {
    int ret = common::OB_SUCCESS;
    while (common::OB_SUCCESS == (ret = next_row())) {
      uint64_t table_id = 0;
      if (OB_SUCC(ret)) {
        if (common::OB_SUCCESS != (ret = get_uint(0, table_id))) {
          OBLOG_LOG(WARN, "err get uint", K(ret));
        }
      }
      tid = table_id;
    }
    ret = (common::OB_ITER_END == ret) ? common::OB_SUCCESS : ret;
    return ret;
  }
private:
  char buf_[512];
};

/*
 * Get partition key by table id from system table.
 */
class GetPartitionKey : public MySQLQueryBase
{
public:
  GetPartitionKey(const uint64_t tid)
  {
    snprintf(buf_, 512, "select table_id, partition_id, partition_cnt "
                        "from __all_meta_table where table_id=%lu", tid);
    sql_ = buf_;
    sql_len_ = strlen(sql_);
  }
  int get_pkeys(ObArray<ObPartitionKey> &pkeys)
  {
    int ret = common::OB_SUCCESS;
    while (common::OB_SUCCESS == (ret = next_row())) {
      uint64_t table_id = 0;
      int32_t partition_id = 0;
      int32_t partition_cnt = 0;
      if (OB_SUCC(ret)) {
        if (common::OB_SUCCESS != (ret = get_uint(0, table_id))) {
          OBLOG_LOG(WARN, "err get uint", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        int64_t val = 0;
        if (common::OB_SUCCESS != (ret = get_int(1, val))) {
          OBLOG_LOG(WARN, "err get int", K(ret));
        } else {
          partition_id = static_cast<int32_t>(val);
        }
      }
      if (OB_SUCC(ret)) {
        int64_t val = 0;
        if (common::OB_SUCCESS != (ret = get_int(2, val))) {
          OBLOG_LOG(WARN, "err get int", K(ret));
        } else {
          partition_cnt = static_cast<int32_t>(val);
        }
      }
      ObPartitionKey pkey;
      pkey.init(table_id, partition_id, partition_cnt);
      pkeys.push_back(pkey);
    }
    ret = (common::OB_ITER_END == ret) ? common::OB_SUCCESS : ret;
    return ret;
  }
private:
  char buf_[512];
};

/*
 * Create table and return their partition keys.
 */
inline void prepare_table_1(const SvrCfg& svr_cfg,
                            const char** tnames,
                            const int64_t tcnt,
                            const char* schema,
                            ObArray<ObPartitionKey>& pkeys)
{
  ObLogMySQLConnector conn;
  ConnectorConfig cfg = prepare_cfg_1(svr_cfg);

  int ret = conn.init(cfg);
  EXPECT_EQ(OB_SUCCESS, ret);

  // Prepare tables.

  for (int64_t idx = 0; idx < tcnt; ++idx) {
    // Drop.
    DropTable drop_table(tnames[idx]);
    ret = conn.exec(drop_table);
    EXPECT_EQ(OB_SUCCESS, ret);
    // Create.
    CreateTable create_table(tnames[idx], schema);
    ret = conn.exec(create_table);
    EXPECT_EQ(OB_SUCCESS, ret);
    // Get tid.
    GetTableId get_tid(tnames[idx]);
    ret = conn.query(get_tid);
    EXPECT_EQ(OB_SUCCESS, ret);
    uint64_t tid = OB_INVALID_ID;
    ret = get_tid.get_tid(tid);
    EXPECT_EQ(OB_SUCCESS, ret);
    // Get pkeys.
    GetPartitionKey get_pkey(tid);
    ret = conn.query(get_pkey);
    EXPECT_EQ(OB_SUCCESS, ret);
    ret = get_pkey.get_pkeys(pkeys);
    EXPECT_EQ(OB_SUCCESS, ret);
  }

  ret = conn.destroy();
  EXPECT_EQ(OB_SUCCESS, ret);
}

/*
 * Data generator.
 * Insert a bunch of data in server.
 * For schema 1.
 */
class DataGenerator1 : public Runnable
{
  class Inserter : public MySQLQueryBase
  {
  public:
    void set_data(const char *tname, const int64_t data)
    {
      reuse();
      snprintf(buf_, 512, "insert into %s (c1) values (%ld)", tname, data);
      sql_ = buf_;
      sql_len_ = strlen(sql_);
    }
  private:
    char buf_[512];
  };
public:
  DataGenerator1(const ConnectorConfig &cfg) :
    conn_(), cfg_(cfg), tname_(NULL), start_(0), end_(0)
  {
    int err = conn_.init(cfg_);
    EXPECT_EQ(OB_SUCCESS, err);
  }
  ~DataGenerator1()
  {
    conn_.destroy();
  }
  void insert(const char *tname, const int64_t start, const int64_t end)
  {
    tname_ = tname;
    start_ = start;
    end_ = end;
    create();
  }
private:
  int routine()
  {
    Inserter inserter;
    int err = OB_SUCCESS;
    for (int64_t cur = start_;
         OB_SUCCESS == err && cur < end_;
         cur++) {
      inserter.set_data(tname_, cur);
      err = conn_.exec(inserter);
      EXPECT_EQ(OB_SUCCESS, err);
    }
    return OB_SUCCESS;
  }
private:
  ObLogMySQLConnector conn_;
  ConnectorConfig cfg_;
  const char *tname_;
  int64_t start_;
  int64_t end_;
};

/*
 * End of Test Dataset Generator.
 */

}
}
