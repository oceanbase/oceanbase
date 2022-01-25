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

#include <stdio.h>        // fprintf
#include <getopt.h>       // getopt_long
#include <stdlib.h>       // strtoull

#include "share/ob_define.h"
#include "lib/file/file_directory_utils.h"
#include "liboblog/src/ob_log_fetcher_impl.h"
#include "ob_log_trans_log.h"

using namespace oceanbase;
using namespace common;
using namespace transaction;
using namespace storage;
using namespace liboblog;
using namespace fetcher;
using namespace clog;

#define OB_LOGGER ::oceanbase::common::ObLogger::get_logger()

#define EXPECT_EQ(EXP, VAL) \
  do { \
    if ((EXP) != (VAL)) { _E_("assert failed", #EXP, (EXP), #VAL, (VAL)); exit(1); } \
  } while(0)

namespace oceanbase
{
namespace liboblog
{
namespace integrationtesting
{

/*
 * FetchLogTest:
 *  - add n partitions, find m servers for each one, let it dispatch and create new workers
 *  - in time log, no heartbeat
 *  - parser prints current min and max process
 *  - discard all, quit
 */
class FetchLogTest
{
public:
  int64_t partition_cnt_;
  int64_t server_cnt_;
  int64_t runtime_; // usec
public:
  /*
   * Mock systable helper.
   *  - provide all m servers for each request, 127.0.0.[1-m]
   */
  class MockSystableHelper : public ObILogSysTableHelper
  {
  public:
    int64_t server_cnt_;
    int64_t now_;
    void init(const int64_t svr_cnt)
    {
      server_cnt_ = svr_cnt;
      now_ = get_timestamp();
    }
  public:
    virtual int query_all_clog_history_info_by_log_id_1(
        const common::ObPartitionKey &pkey, const uint64_t log_id,
        AllClogHistoryInfos &records) {
      // Generate random results.
      int ret = OB_SUCCESS;
      records.reset();
      AllClogHistoryInfoRecord rec;
      const int64_t cnt = server_cnt_;
      for (int64_t idx = 0; idx < cnt; ++idx) {
        rec.reset();
        rec.table_id_ = (uint64_t)(pkey.table_id_);
        rec.partition_idx_ = (int32_t)(pkey.get_partition_id());
        rec.partition_cnt_ = 0;//partition cnt
        rec.start_log_id_ = log_id;
        rec.end_log_id_ = log_id + 100000000;
        rec.start_log_timestamp_ = now_;
        rec.end_log_timestamp_ = now_ + 100 * _HOUR_;
        snprintf(rec.svr_ip_, common::MAX_IP_ADDR_LENGTH + 1, "127.0.0.%ld", 1 + idx);
        rec.svr_port_ = 8888;
        records.push_back(rec);
      }
      return ret;
    }

    virtual int query_all_clog_history_info_by_timestamp_1(
        const common::ObPartitionKey &pkey, const int64_t timestamp,
        AllClogHistoryInfos &records) {
      // Generate random results.
      int ret = OB_SUCCESS;
      records.reset();
      AllClogHistoryInfoRecord rec;
      const int64_t cnt = server_cnt_;
      for (int64_t idx = 0; idx < cnt; ++idx) {
        rec.reset();
        rec.table_id_ = (uint64_t)(pkey.table_id_);
        rec.partition_idx_ = (int32_t)(pkey.get_partition_id());
        rec.partition_cnt_ = 0;
        rec.start_log_id_ = 0;
        rec.end_log_id_ = 100000000;
        rec.start_log_timestamp_ = timestamp;
        rec.end_log_timestamp_ = timestamp + (100 * _HOUR_);
        snprintf(rec.svr_ip_, common::MAX_IP_ADDR_LENGTH + 1, "127.0.0.%ld", 1 + idx);
        rec.svr_port_ = 8888;
        records.push_back(rec);
      }
      return ret;
    }
    virtual int query_all_meta_table_1(
        const common::ObPartitionKey &pkey, AllMetaTableRecords &records) {
      // Generate random results.
      int ret = OB_SUCCESS;
      UNUSED(pkey);
      records.reset();
      AllMetaTableRecord rec;
      const int64_t cnt = server_cnt_;
      for (int64_t idx = 0; idx < cnt; ++idx) {
        rec.reset();
        snprintf(rec.svr_ip_, common::MAX_IP_ADDR_LENGTH + 1, "127.0.0.%ld", 1 + idx);
        rec.svr_port_ = 8888;
        rec.role_ = (0 == idx) ? LEADER : FOLLOWER;
        records.push_back(rec);
      }
      return ret;
    }
    virtual int query_all_meta_table_for_leader(
        const common::ObPartitionKey &pkey,
        bool &has_leader,
        common::ObAddr &leader)
    {
      UNUSED(pkey);
      has_leader = true;
      leader.set_ip_addr("127.0.0.1", 8888);
      return OB_SUCCESS;
    }
    virtual int query_all_server_table_1(
        AllServerTableRecords &records)
    {
      UNUSED(records);
      // Generate random results.
      int ret = OB_SUCCESS;
      records.reset();
      AllServerTableRecord rec;
      const int64_t cnt = server_cnt_;
      for (int64_t idx = 0; idx < cnt; ++idx) {
        rec.reset();
        snprintf(rec.svr_ip_, common::MAX_IP_ADDR_LENGTH + 1, "127.0.0.%ld", 1 + idx);
        rec.svr_port_ = 8888;
        records.push_back(rec);
      }
      return ret;
    }
  };
  /*
   * Rpc.
   *  - return start log id as 1
   *  - in time log
   *  - can open stream
   *  - no heartbeat
   *
   *  **********
   *  Update: 1. to all partition, redo logs id 10-17, etc, prepare log id 18, etc, commit log id 19, etc transactions are all consecutive;
   *  2. to all partition, start log id is always 7, so 7 redo, 8 prepare, then fetch 0-6 as missing redo logs.
   *  Result: check process, if process keeps going, it works.
   *  Impl: call gen_log(id), it returns log entry. It judges the log type by id, generates the log data, serialize it into buffer,
   *  and you get it.
   *  Update: 1. set feedback randomly: 1/100 rpc.
   *  Todo... all logs use the same trans id.
   *  **********
   */
  class MockRpcInterface : public IFetcherRpcInterface
  {
    struct PInfo
    {
      ObPartitionKey pkey_;
      uint64_t next_log_id_;
      ObTransID current_trans_id_;
      TO_STRING_KV(K(pkey_), K(next_log_id_), K(current_trans_id_));
    };
    typedef ObArray<PInfo> PInfoArray;
    struct StreamInfo
    {
      PInfoArray pinfo_array_;
      int64_t last_partition_;
    };
    typedef ObLinearHashMap<obrpc::ObStreamSeq, StreamInfo*> StreamMap;
    StreamMap stream_map_;

    int64_t log_pos_;
    static const int64_t LOG_BUFFER_SIZE_ = 10240;
    char log_buffer_[LOG_BUFFER_SIZE_];

    static const int64_t MOCK_LOAD_SIZE_ = 8;
    char mock_load_[MOCK_LOAD_SIZE_];

  public:
    MockRpcInterface() { EXPECT_EQ(OB_SUCCESS, stream_map_.init()); log_pos_ = 0;}
    ~MockRpcInterface() { stream_map_.reset(); stream_map_.destroy();}
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
      res.reset();
      for (int64_t idx = 0, cnt = req.get_params().count(); idx < cnt; ++idx) {
        obrpc::ObLogReqStartLogIdByTsResponseWithBreakpoint::Result result;
        result.reset();
        result.err_ = OB_SUCCESS;
        result.start_log_id_ = 16;
        res.append_result(result);
      }
      _D_(">>> req start log id", K(req), K(res));
      return OB_SUCCESS;
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
      res.reset();
      for (int64_t idx = 0, cnt = req.get_params().count(); idx < cnt; ++idx) {
        obrpc::ObLogReqHeartbeatInfoResponse::Result result;
        result.reset();
        result.err_ = OB_NEED_RETRY;
        result.tstamp_ = OB_INVALID_TIMESTAMP;
        res.append_result(result);
      }
      _D_(">>> heartbeat", K(req), K(res));
      return OB_SUCCESS;
    }
    virtual int req_leader_heartbeat(
        const obrpc::ObLogLeaderHeartbeatReq &req,
        obrpc::ObLogLeaderHeartbeatResp &res)
    {
      res.reset();
      res.set_err(OB_SUCCESS);
      res.set_debug_err(OB_SUCCESS);
      for (int64_t idx = 0, cnt = req.get_params().count(); idx < cnt; ++idx) {
        obrpc::ObLogLeaderHeartbeatResp::Result result;
        const obrpc::ObLogLeaderHeartbeatReq::Param &param = req.get_params().at(idx);

        result.reset();
        result.err_ = OB_NOT_MASTER;
        result.next_served_log_id_ = param.next_log_id_;
        result.next_served_ts_ = 1;

        EXPECT_EQ(OB_SUCCESS, res.append_result(result));
      }

      _D_(">>> heartbeat", K(req), K(res));
      return OB_SUCCESS;
    }
    virtual int open_stream(const obrpc::ObLogOpenStreamReq &req,
                            obrpc::ObLogOpenStreamResp &res) {
      int ret = OB_SUCCESS;
      UNUSED(req);
      // Build stream.
      obrpc::ObStreamSeq seq;
      seq.reset();
      seq.self_.set_ip_addr("127.0.0.1", 8888);
      seq.seq_ts_ = get_timestamp();
      PInfo pinfo;
      StreamInfo *stream_info = new StreamInfo();
      for (int64_t idx = 0, cnt = req.get_params().count(); (idx < cnt); ++idx) {
        pinfo.pkey_ = req.get_params().at(idx).pkey_;
        pinfo.next_log_id_ = req.get_params().at(idx).start_log_id_;
        EXPECT_EQ(OB_SUCCESS, stream_info->pinfo_array_.push_back(pinfo));
      }
      stream_info->last_partition_ = 0;
      EXPECT_EQ(OB_SUCCESS, stream_map_.insert(seq, stream_info));
      // Response.
      res.reset();
      res.set_err(OB_SUCCESS);
      res.set_debug_err(OB_SUCCESS);
      res.set_stream_seq(seq);
      _D_(">>> open stream", K(req), K(res));
      return ret;
    }
    virtual int fetch_stream_log(const obrpc::ObLogStreamFetchLogReq &req,
                                 obrpc::ObLogStreamFetchLogResp &res) {
      // Get stream info.
      StreamInfo *stream_info = NULL;
      EXPECT_EQ(OB_SUCCESS, stream_map_.get(req.get_stream_seq(), stream_info));
      res.reset();
      res.set_err(OB_SUCCESS);
      res.set_debug_err(OB_SUCCESS);
      // Build logs.
      int ret = OB_SUCCESS;
      int64_t reach_upper_limit_cnt = 0;
      while (_SUCC_(ret) && (reach_upper_limit_cnt < stream_info->pinfo_array_.count())) {
        for (int64_t idx = 0, cnt = req.get_log_cnt_per_part_per_round();
             (idx < cnt) && _SUCC_(ret); ++idx) {
          int64_t &pidx = stream_info->last_partition_;
          pidx += 1;
          if (stream_info->pinfo_array_.count() <= pidx) {
            pidx = 0;
          }
          const ObPartitionKey &pkey = stream_info->pinfo_array_.at(pidx).pkey_;
          uint64_t &log_id = stream_info->pinfo_array_.at(pidx).next_log_id_;
          ObTransID &trans_id = stream_info->pinfo_array_.at(pidx).current_trans_id_;
          int64_t ts = get_timestamp();
          if (ts < req.get_upper_limit_ts()) {
            // Gen log.
            gen_log(pkey, log_id, trans_id);
            ObProposalID proposal_id;
            proposal_id.addr_ = ObAddr(ObAddr::IPV4, "127.0.0.1", 8888);
            proposal_id.ts_ = ts;
            ObLogEntryHeader header;
            header.generate_header(OB_LOG_SUBMIT, pkey,
                                   log_id, log_buffer_, log_pos_,
                                   ts, ts, proposal_id, ts, ObVersion(1));
            ObLogEntry log_entry;
            log_entry.generate_entry(header, log_buffer_);
            ret = res.append_clog_entry(log_entry); // May buf not enough.
            if (_SUCC_(ret)) {
              log_id += 1;
            }
          }
          else {
            reach_upper_limit_cnt += 1;
            obrpc::ObLogStreamFetchLogResp::FetchLogHeartbeatItem hb;
            hb.reset();
            hb.pkey_ = pkey;
            hb.next_log_id_ = log_id;
            hb.heartbeat_ts_ = get_timestamp() - 1;
            ret = res.append_hb(hb);
            break;
          }
        }
      }
      if (OB_BUF_NOT_ENOUGH == ret) {
        ret = OB_SUCCESS;
      }
      // Do some feedback randomly.
      if ((get_timestamp() % 1000000) < 10000) {
        // 1 / 100.
        const ObPartitionKey &pkey = stream_info->pinfo_array_.at(stream_info->last_partition_).pkey_;
        obrpc::ObLogStreamFetchLogResp::FeedbackPartition feedback;
        feedback.pkey_ = pkey;
        feedback.feedback_type_ = obrpc::ObLogStreamFetchLogResp::LAGGED_FOLLOWER;
        EXPECT_EQ(OB_SUCCESS, res.append_feedback(feedback));
      }
      _D_(">>> fetch log", K(req), K(res));
      return OB_SUCCESS;
    }
    virtual int req_svr_feedback(const ReqLogSvrFeedback &feedback)
    {
      UNUSED(feedback);
      return OB_SUCCESS;
    }

    void gen_header(const int64_t log_type, char *buf, const int64_t size, int64_t &pos)
    {
      EXPECT_EQ(OB_SUCCESS, serialization::encode_i64(buf, size, pos, log_type));
      EXPECT_EQ(OB_SUCCESS, serialization::encode_i64(buf, size, pos, 0));
    }

    void init_redo(ObTransRedoLog &redo_log, const ObPartitionKey &pkey,
        const uint64_t log_id, const ObTransID &trans_id)
    {
      int64_t log_type = OB_LOG_TRANS_REDO;
      ObPartitionKey partition_key(pkey);
      ObAddr observer;
      observer.set_ip_addr("127.0.0.1", 8888);
      const int64_t log_no = (int64_t)(log_id % 10);
      const uint64_t tenant_id = 100;
      ObAddr scheduler;
      ObPartitionKey coordinator;
      ObPartitionArray participants;
      ObStartTransParam parms;
      parms.set_access_mode(ObTransAccessMode::READ_ONLY);
      parms.set_type(ObTransType::TRANS_USER);
      parms.set_isolation(ObTransIsolation::READ_COMMITED);
      coordinator = partition_key;
      participants.push_back(partition_key);
      scheduler = observer;
      const uint64_t cluster_id = 1000;

      ObVersion active_memstore_version(1);
      EXPECT_EQ(OB_SUCCESS, redo_log.init(log_type, partition_key, trans_id, tenant_id, log_no,
                                          scheduler, coordinator, participants, parms, cluster_id,
                                          active_memstore_version));
      redo_log.get_mutator().set_data(mock_load_, MOCK_LOAD_SIZE_);
      redo_log.get_mutator().get_position() += MOCK_LOAD_SIZE_;
      EXPECT_EQ(true, redo_log.is_valid());
    }

    void init_prepare(ObTransPrepareLog &prepare_log,
        const ObPartitionKey &pkey, const uint64_t log_id,
        const ObRedoLogIdArray &redo_log_ids,
        const ObTransID &trans_id)
    {
      UNUSED(log_id);
      int64_t log_type = OB_LOG_TRANS_PREPARE;
      const uint64_t cluster_id = 1000;
      ObPartitionKey partition_key(pkey);
      ObAddr observer;
      observer.set_ip_addr("127.0.0.1", 8888);
      ObAddr &scheduler = observer;
      ObPartitionKey &coordinator = partition_key;
      ObPartitionArray participants;
      participants.push_back(partition_key);

      ObStartTransParam trans_param;
      trans_param.set_access_mode(ObTransAccessMode::READ_WRITE);
      trans_param.set_type(ObTransType::TRANS_USER);
      trans_param.set_isolation(ObTransIsolation::READ_COMMITED);

      const int prepare_status = true;
      const int64_t local_trans_version = 1000;
      const uint64_t tenant_id = 100;

      ObVersion active_memstore_version(1);
      EXPECT_EQ(OB_SUCCESS, prepare_log.init(log_type, partition_key, trans_id, tenant_id,
          scheduler, coordinator, participants, trans_param, prepare_status,
          redo_log_ids, local_trans_version, cluster_id, active_memstore_version));
      EXPECT_EQ(true, prepare_log.is_valid());
    }

    void init_commit(ObTransCommitLog &commit_log,
        const ObPartitionKey &pkey, const uint64_t log_id, const uint64_t prepare_id,
        const int64_t prepare_tstamp,
        const ObTransID &trans_id)
    {
      UNUSED(log_id);
      int64_t log_type = OB_LOG_TRANS_COMMIT;
      const uint64_t cluster_id = 1000;
      const int64_t global_trans_version = 1000;
      ObPartitionKey partition_key(pkey);
      ObAddr observer;
      observer.set_ip_addr("127.0.0.1", 8888);
      PartitionLogInfoArray array;
      ObPartitionLogInfo pidinfo(pkey, prepare_id, prepare_tstamp);
      EXPECT_EQ(OB_SUCCESS, array.push_back(pidinfo));
      EXPECT_EQ(OB_SUCCESS, commit_log.init(log_type, partition_key, trans_id, array,
                                            global_trans_version, 0, cluster_id));
      EXPECT_EQ(true, commit_log.is_valid());
    }

    // Log generators.
    void gen_redo(const ObPartitionKey &pkey, const uint64_t log_id,
                  ObTransID &trans_id, char *buf, const int64_t size, int64_t &pos)
    {
      ObTransRedoLog redo_log;
      init_redo(redo_log, pkey, log_id, trans_id);

      gen_header(OB_LOG_TRANS_REDO, buf, size, pos);
      EXPECT_EQ(OB_SUCCESS, redo_log.serialize(buf, size, pos));
    }

    void gen_prepare(const ObPartitionKey &pkey, const uint64_t log_id,
                     const ObRedoLogIdArray &redo_log_ids,
                     const ObTransID &trans_id,
                     char *buf, const int64_t size, int64_t &pos)
    {
      ObTransPrepareLog prepare_log;
      init_prepare(prepare_log, pkey, log_id, redo_log_ids, trans_id);

      gen_header(OB_LOG_TRANS_PREPARE, buf, size, pos);
      EXPECT_EQ(OB_SUCCESS, prepare_log.serialize(buf, size, pos));
    }

    void gen_commit(const ObPartitionKey &pkey, const uint64_t log_id, const uint64_t prepare_id,
                    const int64_t prepare_tstamp,
                    const ObTransID &trans_id,
                    char *buf, const int64_t size, int64_t &pos)
    {
      ObTransCommitLog commit_log;
      init_commit(commit_log, pkey, log_id, prepare_id, prepare_tstamp, trans_id);

      gen_header(OB_LOG_TRANS_COMMIT, buf, size, pos);
      EXPECT_EQ(OB_SUCCESS, commit_log.serialize(buf, size, pos));
    }

    void gen_redo_with_prepare(const ObPartitionKey &pkey, const uint64_t log_id,
        const ObRedoLogIdArray &redo_log_ids,
        const ObTransID &trans_id,
        char *buf, const int64_t size, int64_t &pos)
    {
      ObTransRedoLog redo_log;
      ObTransPrepareLog prepare_log;

      init_redo(redo_log, pkey, log_id, trans_id);
      init_prepare(prepare_log, pkey, log_id, redo_log_ids, trans_id);

      gen_header(OB_LOG_TRANS_REDO | OB_LOG_TRANS_PREPARE, buf, size, pos);
      EXPECT_EQ(OB_SUCCESS, redo_log.serialize(buf, size, pos));
      EXPECT_EQ(OB_SUCCESS, prepare_log.serialize(buf, size, pos));
    }

    void gen_log(const ObPartitionKey &pkey, const uint64_t id, ObTransID &trans_id)
    {
      // redo: id % 10 range [0, 7]
      // prepare: id % 10 == 8
      // commit: id % 10 == 9
      log_pos_ = 0;
      uint64_t mod = (id % 10);
      // All logs uses a same trans id. TODO...
      static const ObTransID trans_id_2(ObTransID(ObAddr(ObAddr::IPV4, "127.0.0.1", 8888)));
      trans_id = trans_id_2;
      if (mod <= 7) {
        gen_redo(pkey, id, trans_id, log_buffer_, LOG_BUFFER_SIZE_, log_pos_);
      }

      if (8 == mod) {
        ObRedoLogIdArray redo_log_id_array;
        for (int64_t idx = 0, cnt = 8; (idx < cnt); ++idx) {
          EXPECT_EQ(OB_SUCCESS, redo_log_id_array.push_back((int64_t)(id - 8 + idx)));
        }
        gen_redo_with_prepare(pkey, id, redo_log_id_array, trans_id, log_buffer_, LOG_BUFFER_SIZE_, log_pos_);
      }

      if (9 == mod) {
        // Prepare tstamp is incorrect.
        gen_commit(pkey, id, (id - 1), get_timestamp(),trans_id, log_buffer_, LOG_BUFFER_SIZE_, log_pos_);
      }
    }
  };
  /*
   * Factory.
   */
  class MockRpcInterfaceFactory : public IFetcherRpcInterfaceFactory
  {
  public:
    virtual int new_fetcher_rpc_interface(IFetcherRpcInterface*& rpc)
    {
      rpc = new MockRpcInterface();
      return OB_SUCCESS;
    }
    virtual int delete_fetcher_rpc_interface(IFetcherRpcInterface* rpc)
    {
      delete rpc;
      return OB_SUCCESS;
    }
  };

  /*
   * Mock parser.
   *  - track process
   *  - print min & max process
   */
  class MockParser : public IObLogParser
  {
    typedef common::ObLinearHashMap<common::ObPartitionKey, int64_t> ProcessMap;
    struct Updater
    {
      int64_t tstamp_;
      bool operator()(const common::ObPartitionKey &pkey, int64_t &val)
      {
        UNUSED(pkey);
        if (val < tstamp_) { val = tstamp_; }
        return true;
      }
    };
    struct ProcessGetter
    {
      int64_t min_process_;
      int64_t max_process_;
      int64_t partition_cnt_;
      bool operator()(const common::ObPartitionKey &pkey, const int64_t &val)
      {
        UNUSED(pkey);
        if (OB_INVALID_TIMESTAMP == min_process_ || val < min_process_) {
          min_process_ = val;
        }
        if (OB_INVALID_TIMESTAMP == max_process_ || max_process_ < val) {
          max_process_ = val;
        }
        partition_cnt_ += 1;
        return true;
      }
      void reset() { min_process_ = OB_INVALID_TIMESTAMP; max_process_ = OB_INVALID_TIMESTAMP; partition_cnt_ = 0;}
    };
  public:
    MockParser() : trans_cnt_(0) { EXPECT_EQ(OB_SUCCESS, process_map_.init()); }
    virtual ~MockParser() { process_map_.reset(); process_map_.destroy(); }
    virtual int start() { return OB_SUCCESS; }
    virtual void stop() { }
    virtual void mark_stop_flag() { }
    virtual int push(PartTransTask* task, const int64_t timeout)
    {
      UNUSED(timeout);
      if (NULL != task) {
        if (task->is_heartbeat() || task->is_normal_trans()) {
          const common::ObPartitionKey &pkey = task->get_partition();
          const int64_t tstamp = task->get_timestamp();
          Updater updater;
          updater.tstamp_ = tstamp;
          EXPECT_EQ(OB_SUCCESS, process_map_.operate(pkey, updater));

          if (task->is_normal_trans()) {
            EXPECT_EQ(OB_SUCCESS, handle_normal_trans_(task));
          }
        }
        task->revert();
        ATOMIC_INC(&trans_cnt_);
        // Debug.
        // _I_(">>> push parser", "req", task->get_seq());
      }
      return OB_SUCCESS;
    }
    void add_partition(const common::ObPartitionKey &pkey)
    {
      EXPECT_EQ(OB_SUCCESS, process_map_.insert(pkey, 0));
    }
    void print_process()
    {
      ProcessGetter process_getter;
      process_getter.reset();
      EXPECT_EQ(OB_SUCCESS, process_map_.for_each(process_getter));
      fprintf(stderr, ">>> parser process: %s-%s partition count: %ld trans count: %ld\n",
              TS_TO_STR(process_getter.min_process_),
              TS_TO_STR(process_getter.max_process_),
              process_getter.partition_cnt_,
              ATOMIC_LOAD(&trans_cnt_));
    }
  private:
    int handle_normal_trans_(PartTransTask *task)
    {
      int ret = OB_SUCCESS;
      if (NULL == task) {
        ret = OB_INVALID_ARGUMENT;
      } else {
        RedoLogList &redo_list = task->get_redo_list();

        // 如果存在Redo日志，则解析Redo日志
        if (redo_list.num_ > 0 && OB_FAIL(parse_redo_log_(task))) {
          _E_("parse_redo_log_ fail", K(ret), "task", *task);
        }

      }
      return ret;
    }
    int parse_redo_log_(PartTransTask *task)
    {
      int ret = OB_SUCCESS;

      if (OB_ISNULL(task)) {
        ret = OB_INVALID_ARGUMENT;
      } else {
        int64_t redo_num = 0;
        RedoLogList &redo_list = task->get_redo_list();
        RedoLogNode *redo_node = redo_list.head_;

        if (OB_UNLIKELY(! redo_list.is_valid())) {
          _E_("redo log list is invalid", K(redo_list), K(*task));
          ret = OB_ERR_UNEXPECTED;
        } else {
          while (OB_SUCCESS == ret && NULL != redo_node) {
            _D_("parse redo log", "redo_node", *redo_node);

            if (OB_UNLIKELY(! redo_node->is_valid())) {
              _E_("redo_node is invalid", "redo_node", *redo_node, "redo_index", redo_num);
              ret = OB_INVALID_DATA;
              // 校验Redo日志序号是否准确
            } else if (OB_UNLIKELY(redo_node->log_no_ != redo_num)) {
              _E_("redo log_no is incorrect", "redo_no", redo_node->log_no_,
                  "expected_redo_no", redo_num, "redo_node", *redo_node);
              ret = OB_INVALID_DATA;
            } else {
              redo_num++;
              redo_node = redo_node->next_;
            }
          }
        }
      }

      return ret;
    }
  private:
    int64_t trans_cnt_;
    ProcessMap process_map_;
  };
  /*
   * Err handler.
   *  - exit on error
   */
  class MockFetcherErrHandler : public IErrHandler
  {
  public:
    virtual ~MockFetcherErrHandler() { }
  public:
    virtual void handle_err(int err_no, const char* fmt, ...)
    {
      UNUSED(err_no);
      va_list ap;
      va_start(ap, fmt);
      __E__(fmt, ap);
      va_end(ap);
      exit(1);
    }
  };

public:
  void run()
  {
    int err = OB_SUCCESS;

    // Init clock generator.
    ObClockGenerator::init();

    // Task Pool.
    ObLogTransTaskPool<PartTransTask> task_pool;
    ObConcurrentFIFOAllocator task_pool_alloc;
    err = task_pool_alloc.init(128 * _G_, 8 * _M_, OB_MALLOC_NORMAL_BLOCK_SIZE);
    EXPECT_EQ(OB_SUCCESS, err);
    err = task_pool.init(&task_pool_alloc, 10240, 1024, 4 * 1024 * 1024, true);
    EXPECT_EQ(OB_SUCCESS, err);

    // Parser.
    MockParser parser;

    // Err Handler.
    MockFetcherErrHandler err_handler;

    // Rpc.
    MockRpcInterfaceFactory rpc_factory;

    // Worker Pool.
    FixedJobPerWorkerPool worker_pool;
    err = worker_pool.init(1);
    EXPECT_EQ(OB_SUCCESS, err);

    // StartLogIdLocator.
    ::oceanbase::liboblog::fetcher::StartLogIdLocator locator;
    err = locator.init(&rpc_factory, &err_handler, &worker_pool, 3);
    EXPECT_EQ(OB_SUCCESS, err);

    // Heartbeater.
    Heartbeater heartbeater;
    err = heartbeater.init(&rpc_factory, &err_handler, &worker_pool, 3);
    EXPECT_EQ(OB_SUCCESS, err);

    // SvrFinder.
    MockSystableHelper systable_helper;
    systable_helper.init(server_cnt_);
    ::oceanbase::liboblog::fetcher::SvrFinder svrfinder;
    err = svrfinder.init(&systable_helper, &err_handler, &worker_pool, 3);
    EXPECT_EQ(OB_SUCCESS, err);

    // Fetcher Config.
    FetcherConfig cfg;
    cfg.reset();

    // Init.
    ::oceanbase::liboblog::fetcher::Fetcher fetcher;
    err = fetcher.init(&task_pool, &parser, &err_handler, &rpc_factory,
                       &worker_pool, &svrfinder, &locator, &heartbeater, &cfg);
    EXPECT_EQ(OB_SUCCESS, err);

    // Add partition.
    for (int64_t idx = 0, cnt = partition_cnt_; (idx < cnt); ++idx) {
      ObPartitionKey p1(1001 + idx, 1, partition_cnt_);
      err = fetcher.fetch_partition(p1, 1, OB_INVALID_ID);
      EXPECT_EQ(OB_SUCCESS, err);
      parser.add_partition(p1);
    }

    // Run.
    err = fetcher.start();
    EXPECT_EQ(OB_SUCCESS, err);

    // Runtime.
    int64_t start = get_timestamp();
    int64_t last_print_process = start;
    while ((get_timestamp() - start) < runtime_) {
      usec_sleep(500 * _MSEC_);
      if (1 * _SEC_ < get_timestamp() - last_print_process) {
        last_print_process = get_timestamp();
        parser.print_process();
      }
    }

    // Discard partition.
    for (int64_t idx = 0, cnt = partition_cnt_; (idx < cnt); ++idx) {
      ObPartitionKey p1(1001 + idx, 1, partition_cnt_);
      err = fetcher.discard_partition(p1);
      EXPECT_EQ(OB_SUCCESS, err);
    }

    // Stop.
    err = fetcher.stop(true);
    EXPECT_EQ(OB_SUCCESS, err);

    // Destroy.
    err = fetcher.destroy();
    EXPECT_EQ(OB_SUCCESS, err);
    err = locator.destroy();
    EXPECT_EQ(OB_SUCCESS, err);
    err = svrfinder.destroy();
    EXPECT_EQ(OB_SUCCESS, err);
    err = heartbeater.destroy();
    EXPECT_EQ(OB_SUCCESS, err);
    worker_pool.destroy();
    EXPECT_EQ(OB_SUCCESS, err);
    task_pool.destroy();
    EXPECT_EQ(OB_SUCCESS, err);
  }
};

}
}
}

void print_usage(const char *prog_name)
{
  printf("USAGE: %s\n"
         "   -p, --partition              partition count\n"
         "   -s, --server                 server count\n"
         "   -r, --runtime                run time in seconds, default -1, means to run forever\n",
          prog_name);
}
int main(const int argc, char **argv)
{
  // option variables
  int opt = -1;
  const char *opt_string = "p:s:r:";
  struct option long_opts[] =
      {
          {"partition", 1, NULL, 'p'},
          {"server", 1, NULL, 's'},
          {"runtime", 1, NULL, 'r'},
          {0, 0, 0, 0}
      };

  if (argc <= 1) {
    print_usage(argv[0]);
    return 1;
  }

  // Params.
  int64_t partition_cnt = 0;
  int64_t server_cnt = 0;
  int64_t runtime = 1 * ::oceanbase::liboblog::_YEAR_;

  // Parse command line
  while ((opt = getopt_long(argc, argv, opt_string, long_opts, NULL)) != -1) {
    switch (opt) {
      case 'p': {
        partition_cnt = strtoll(optarg, NULL, 10);
        break;
      }
      case 's': {
        server_cnt = strtoll(optarg, NULL, 10);
        break;
      }
      case 'r': {
        runtime = strtoll(optarg, NULL, 10);
        break;
      }
      default:
        print_usage(argv[0]);
        break;
    } // end switch
  } // end while

  printf("partition_cnt:%ld server_cnt:%ld runtime:%ld sec\n", partition_cnt, server_cnt, runtime);

  // Logger.
  ::oceanbase::liboblog::fetcher::FetcherLogLevelSetter::get_instance().set_mod_log_levels("TLOG.*:INFO");
  OB_LOGGER.set_log_level("INFO");

  // Run test.
  ::oceanbase::liboblog::integrationtesting::FetchLogTest test;
  test.partition_cnt_ = partition_cnt;
  test.server_cnt_ = server_cnt;
  test.runtime_ = ::oceanbase::liboblog::_SEC_ * runtime;
  test.run();
  return 0;
}
