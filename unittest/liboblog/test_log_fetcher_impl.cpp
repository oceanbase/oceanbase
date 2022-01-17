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

#include "share/ob_define.h"
#include "lib/allocator/ob_concurrent_fifo_allocator.h"


#include "test_log_fetcher_common_utils.h"
#include "liboblog/src/ob_log_fetcher_impl.h"

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

namespace BasicFunction1
{
/*
 * Mock systable helper. // Todo...
 */
class MockSystableHelper : public ObILogSysTableHelper
{
public:
  virtual int query_all_clog_history_info_by_log_id_1(
      const common::ObPartitionKey &pkey, const uint64_t log_id,
      AllClogHistoryInfos &records) {
    // Generate random results.
    int ret = OB_SUCCESS;
    int64_t seed = get_timestamp() / 3333333;
    records.reset();
    AllClogHistoryInfoRecord rec;
    const int64_t cnt = 1 + (seed % 6);
    for (int64_t idx = 0; idx < cnt; ++idx) {
      rec.reset();
      rec.table_id_ = (uint64_t)(pkey.table_id_);
      rec.partition_idx_ = (int32_t)(pkey.get_partition_id());
      rec.partition_cnt_ = (int32_t)(pkey.get_partition_cnt());
      rec.start_log_id_ = log_id;
      rec.end_log_id_ = log_id + 10000;
      rec.start_log_timestamp_ = seed - (1 * _HOUR_);
      rec.end_log_timestamp_ = seed + (1 * _HOUR_);
      snprintf(rec.svr_ip_, common::MAX_IP_ADDR_LENGTH + 1, "127.0.0.%ld", (seed % 128));
      rec.svr_port_ = 8888;
      records.push_back(rec);
      seed += 17;
    }
    return ret;
  }

  virtual int query_all_clog_history_info_by_timestamp_1(
      const common::ObPartitionKey &pkey, const int64_t timestamp,
      AllClogHistoryInfos &records) {
    // Generate random results.
    int ret = OB_SUCCESS;
    int64_t seed = get_timestamp() / 7777777;
    records.reset();
    AllClogHistoryInfoRecord rec;
    const int64_t cnt = 1 + (seed % 6);
    for (int64_t idx = 0; idx < cnt; ++idx) {
      rec.reset();
      rec.table_id_ = (uint64_t)(pkey.table_id_);
      rec.partition_idx_ = (int32_t)(pkey.get_partition_id());
      rec.partition_cnt_ = (int32_t)(pkey.get_partition_cnt());
      rec.start_log_id_ = 0;
      rec.end_log_id_ = 65536;
      rec.start_log_timestamp_ = timestamp - (1 * _HOUR_);
      rec.end_log_timestamp_ = timestamp + (1 * _HOUR_);
      snprintf(rec.svr_ip_, common::MAX_IP_ADDR_LENGTH + 1, "127.0.0.%ld", (seed % 128));
      rec.svr_port_ = 8888;
      records.push_back(rec);
      seed += 17;
    }
    return ret;
  }
  virtual int query_all_meta_table_1(
      const common::ObPartitionKey &pkey, AllMetaTableRecords &records) {
    // Generate random results.
    int ret = OB_SUCCESS;
    UNUSED(pkey);
    int64_t seed = get_timestamp() / 3333333;
    records.reset();
    AllMetaTableRecord rec;
    const int64_t cnt = 1 + (seed % 6);
    for (int64_t idx = 0; idx < cnt; ++idx) {
      rec.reset();
      snprintf(rec.svr_ip_, common::MAX_IP_ADDR_LENGTH + 1, "127.0.0.%ld", (seed % 128));
      rec.svr_port_ = 8888;
      rec.role_ = (0 == idx) ? LEADER : FOLLOWER;
      records.push_back(rec);
      seed += 17;
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

  virtual int query_all_server_table_1(AllServerTableRecords &records) {
    UNUSED(records);
    return OB_SUCCESS;
  }
};

/*
 * Mock rpc.
 */
class MockRpcInterface : public IFetcherRpcInterface
{
public:
  ~MockRpcInterface() {}
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
    // Seed.
    int64_t seed = (get_timestamp());
    for (int64_t idx = 0, cnt = req.get_params().count(); idx < cnt; ++idx) {
      // 30% success, 30% break.
      int64_t rand = (idx + seed) % 100;
      bool succeed = (rand < 30);
      bool breakrpc = (30 <= rand) && (rand < 60);
      obrpc::ObLogReqStartLogIdByTsResponseWithBreakpoint::Result result;
      result.reset();
      result.err_ = (succeed) ? OB_SUCCESS : ((breakrpc) ? OB_EXT_HANDLE_UNFINISH : OB_NEED_RETRY);
      result.start_log_id_ = 1;
      // Break info is actually not returned.
      EXPECT_EQ(OB_SUCCESS, res.append_result(result));
    }
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
      result.err_ = OB_SUCCESS;
      result.tstamp_ = get_timestamp();
      EXPECT_EQ(OB_SUCCESS, res.append_result(result));
    }
    _D_(">>> req heartbeat", K(req), K(res));
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
      result.err_ = OB_SUCCESS;
      result.next_served_log_id_ = param.next_log_id_;
      result.next_served_ts_ = get_timestamp();

      EXPECT_EQ(OB_SUCCESS, res.append_result(result));
    }

    _D_(">>> heartbeat", K(req), K(res));
    return OB_SUCCESS;
  }

  virtual int open_stream(const obrpc::ObLogOpenStreamReq &req,
                          obrpc::ObLogOpenStreamResp &res) {
    int ret = OB_SUCCESS;
    UNUSED(req);
    obrpc::ObStreamSeq seq;
    seq.reset();
    seq.self_.set_ip_addr("127.0.0.1", 8888);
    seq.seq_ts_ = get_timestamp();
    res.reset();
    res.set_err(OB_SUCCESS);
    res.set_debug_err(OB_SUCCESS);
    res.set_stream_seq(seq);
    _D_(">>> open stream", K(req), K(res));
    return ret;
  }
  virtual int fetch_stream_log(const obrpc::ObLogStreamFetchLogReq &req,
                               obrpc::ObLogStreamFetchLogResp &res) {
    UNUSED(req);
    res.set_err(OB_SUCCESS);
    res.set_debug_err(OB_SUCCESS);
    return OB_SUCCESS;
  }
  virtual int req_svr_feedback(const ReqLogSvrFeedback &feedback)
  {
    UNUSED(feedback);
    return OB_SUCCESS;
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

TEST(Fetcher, BasicFunction1)
{
  int err = OB_SUCCESS;

  // Task Pool.
  ObLogTransTaskPool<PartTransTask> task_pool;
  ObConcurrentFIFOAllocator task_pool_alloc;
  err = task_pool_alloc.init(128 * _G_, 8 * _M_, OB_MALLOC_NORMAL_BLOCK_SIZE);
  EXPECT_EQ(OB_SUCCESS, err);
  err = task_pool.init(&task_pool_alloc, 10240, 1024, 4 * 1024 * 1024, true);
  EXPECT_EQ(OB_SUCCESS, err);

  // Parser.
  MockParser1 parser;

  // Err Handler.
  MockLiboblogErrHandler1 err_handler;
  MockFetcherErrHandler1 err_handler2;

  // Rpc.
  MockRpcInterfaceFactory rpc_factory;

  // Worker Pool.
  FixedJobPerWorkerPool worker_pool;
  err = worker_pool.init(1);
  EXPECT_EQ(OB_SUCCESS, err);

  // StartLogIdLocator.
  ::oceanbase::liboblog::fetcher::StartLogIdLocator locator;
  err = locator.init(&rpc_factory, &err_handler2, &worker_pool, 3);
  EXPECT_EQ(OB_SUCCESS, err);

  // Heartbeater.
  Heartbeater heartbeater;
  err = heartbeater.init(&rpc_factory, &err_handler2, &worker_pool, 3);
  EXPECT_EQ(OB_SUCCESS, err);

  // SvrFinder.
  MockSystableHelper systable_helper;
  ::oceanbase::liboblog::fetcher::SvrFinder svrfinder;
  err = svrfinder.init(&systable_helper, &err_handler2, &worker_pool, 3);
  EXPECT_EQ(OB_SUCCESS, err);

  // Fetcher Config.
  FetcherConfig cfg;
  cfg.reset();

  // Init.
  ::oceanbase::liboblog::fetcher::Fetcher fetcher;
  err = fetcher.init(&task_pool, &parser, &err_handler2, &rpc_factory,
                     &worker_pool, &svrfinder, &locator, &heartbeater, &cfg);
  EXPECT_EQ(OB_SUCCESS, err);

  // Add partition.
  ObPartitionKey p1(1001, 1, 1);
  ObPartitionKey p2(1002, 1, 1);
  ObPartitionKey p3(1003, 1, 1);
  err = fetcher.fetch_partition(p1, 1, OB_INVALID_ID);
  EXPECT_EQ(OB_SUCCESS, err);
//  err = fetcher.fetch_partition(p2, 1, OB_INVALID_ID);
//  EXPECT_EQ(OB_SUCCESS, err);
//  err = fetcher.fetch_partition(p3, 1, OB_INVALID_ID);
//  EXPECT_EQ(OB_SUCCESS, err);

  // Run.
  err = fetcher.start();
  EXPECT_EQ(OB_SUCCESS, err);

  usleep(10 * _SEC_);

  // Discard partition.
  err = fetcher.discard_partition(p1);
  EXPECT_EQ(OB_SUCCESS, err);
//  err = fetcher.discard_partition(p2);
//  EXPECT_EQ(OB_SUCCESS, err);
//  err = fetcher.discard_partition(p3);
//  EXPECT_EQ(OB_SUCCESS, err);

  usleep(10 * _SEC_);

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

}
}
}

int main(int argc, char **argv)
{
  ObLogger::get_logger().set_mod_log_levels("ALL.*:ERROR, TLOG.*:DEBUG");
  testing::InitGoogleTest(&argc,argv);
  // testing::FLAGS_gtest_filter = "DO_NOT_RUN";
  return RUN_ALL_TESTS();
}
