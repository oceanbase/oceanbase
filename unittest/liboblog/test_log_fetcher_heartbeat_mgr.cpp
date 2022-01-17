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

#include "share/ob_define.h"

#include "liboblog/src/ob_log_fetcher_heartbeat_mgr.h"

#include "test_log_fetcher_common_utils.h"

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
 * Heartbeater Tests.
 */
/*
 * Basic function test 1.
 *  - N thread & M requests for each thread
 *  - result timestamp == next log id
 *  - rpc always succeed, no server internal error
 *  - rpc interface returns correct result or an error code randomly
 *    (30% correct so most requests are sent to at least 2 servers)
 */
namespace basic_func_test_1
{
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
    res.reset();
    // Seed.
    int64_t seed = (get_timestamp());
    for (int64_t idx = 0, cnt = req.get_params().count(); idx < cnt; ++idx) {
      // 30%.
      bool succeed = ((idx + seed) % 100) < 30;
      obrpc::ObLogReqHeartbeatInfoResponse::Result result;
      result.reset();
      result.err_ = (succeed) ? OB_SUCCESS : OB_NEED_RETRY;
      result.tstamp_ = (succeed) ? (int64_t)(req.get_params().at(idx).log_id_) : OB_INVALID_TIMESTAMP;
      EXPECT_EQ(OB_SUCCESS, res.append_result(result));
    }
    return OB_SUCCESS;
  }

  virtual int req_leader_heartbeat(
      const obrpc::ObLogLeaderHeartbeatReq &req,
      obrpc::ObLogLeaderHeartbeatResp &res)
  {
    res.reset();
    res.set_err(OB_SUCCESS);
    res.set_debug_err(OB_SUCCESS);
    // Seed.
    int64_t seed = (get_timestamp());
    for (int64_t idx = 0, cnt = req.get_params().count(); idx < cnt; ++idx) {
      obrpc::ObLogLeaderHeartbeatResp::Result result;
      const obrpc::ObLogLeaderHeartbeatReq::Param &param = req.get_params().at(idx);
      // 30%.
      bool succeed = ((idx + seed) % 100) < 30;

      result.reset();
      result.err_ = succeed ? OB_SUCCESS : OB_NOT_MASTER;
      result.next_served_log_id_ = param.next_log_id_;
      result.next_served_ts_ = succeed ? get_timestamp() : 1;

      EXPECT_EQ(OB_SUCCESS, res.append_result(result));
    }

    _D_(">>> heartbeat", K(req), K(res));
    return OB_SUCCESS;
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

//////////////////////基本功能测试//////////////////////////////////////////
/*
 * Test HeartbeatRequest
 */
TEST(Heartbeater, BasicFuncTest1)
{
  // Build Heartbeater requests.
  const int64_t AllSvrCnt = 3;
  ObAddr svrs[AllSvrCnt];
  for (int64_t idx = 0, cnt = AllSvrCnt; idx < cnt; ++idx) {
    svrs[idx] = ObAddr(ObAddr::IPV4, "127.0.0.1", (int32_t)(idx + 1000));
  }
  const int64_t HeartbeatRequestCnt = 10000;
  HeartbeatRequest *request_array = static_cast<HeartbeatRequest*>(ob_malloc(
    HeartbeatRequestCnt * sizeof(HeartbeatRequest)));
  // test assignment
  for (int64_t idx = 0, cnt = HeartbeatRequestCnt; idx < cnt; ++idx) {
    HeartbeatRequest &r = request_array[idx];
    r.reset();
    // reset IDLE
    EXPECT_EQ(HeartbeatRequest::IDLE, r.get_state());
    r.pkey_ = ObPartitionKey((uint64_t)(1000 + idx), 0, 1);
    r.next_log_id_ = (uint64_t)(1 + idx);
    r.svr_ = svrs[idx % AllSvrCnt];
    // test getter and  setter
    EXPECT_EQ(HeartbeatRequest::IDLE, r.get_state());
    r.set_state(HeartbeatRequest::REQ);
    EXPECT_EQ(HeartbeatRequest::REQ, r.get_state());
    r.set_state(HeartbeatRequest::DONE);
    EXPECT_EQ(HeartbeatRequest::DONE, r.get_state());
  }

  ob_free(request_array);
  request_array = NULL;
}

/*
 * Test Heartbeater
 */
TEST(Heartbeater, BasicFuncTest2)
{
  // Build Heartbeater requests.
  const int64_t AllSvrCnt = 3;
  ObAddr svrs[AllSvrCnt];
  for (int64_t idx = 0, cnt = AllSvrCnt; idx < cnt; ++idx) {
    svrs[idx] = ObAddr(ObAddr::IPV4, "127.0.0.1", (int32_t)(idx + 1000));
  }
  const int64_t HeartbeatRequestCnt = 10000;
  HeartbeatRequest *request_array = static_cast<HeartbeatRequest*>(ob_malloc(
    HeartbeatRequestCnt * sizeof(HeartbeatRequest)));
  for (int64_t idx = 0, cnt = HeartbeatRequestCnt; idx < cnt; ++idx) {
    HeartbeatRequest &r = request_array[idx];
    r.reset();
    r.pkey_ = ObPartitionKey((uint64_t)(1000 + idx), 0, 1);
    r.next_log_id_ = (uint64_t)(1 + idx);
    r.svr_ = svrs[idx % AllSvrCnt];
  }
  // Heartbeater
  Heartbeater heartbeater;
  MockRpcInterfaceFactory rpc_factory;
  MockFetcherErrHandler1 err_handler1;
  FixedJobPerWorkerPool worker_pool;
  const int64_t heartbeat_worker_cnt = 3;

  int err = OB_SUCCESS;
  err = worker_pool.init(1);
  EXPECT_EQ(OB_SUCCESS, err);

  err = heartbeater.init(&rpc_factory, &err_handler1, &worker_pool, heartbeat_worker_cnt);
  EXPECT_EQ(OB_SUCCESS, err);
  // test async_heartbeat_req
  for (int64_t idx = 0, cnt = HeartbeatRequestCnt; idx < cnt; ++idx) {
    HeartbeatRequest &r = request_array[idx];
    EXPECT_EQ(OB_SUCCESS, heartbeater.async_heartbeat_req(&r));
  }
  // test destroy
  err = heartbeater.destroy();
  EXPECT_EQ(OB_SUCCESS, err);

  err = worker_pool.destroy();
  EXPECT_EQ(OB_SUCCESS, err);

  ob_free(request_array);
  request_array = NULL;
}

/*
 * Test Worker.
 */
class TestWorker : public Runnable
{
public:
  Heartbeater *heartbeater_;
  virtual int routine()
  {
    // Build requests.
    const int64_t AllSvrCnt = 3;
    ObAddr svrs[AllSvrCnt];
    for (int64_t idx = 0, cnt = AllSvrCnt; idx < cnt; ++idx) {
      svrs[idx] = ObAddr(ObAddr::IPV4, "127.0.0.1", (int32_t)(idx + 1000));
    }
    const int64_t HeartbeatRequestCnt = 10 * 10000;
    HeartbeatRequest *request_array = new HeartbeatRequest[HeartbeatRequestCnt];
    for (int64_t idx = 0, cnt = HeartbeatRequestCnt; idx < cnt; ++idx) {
      HeartbeatRequest &r = request_array[idx];
      r.reset();
      r.pkey_ = ObPartitionKey((uint64_t)(1000 + idx), 0, 1);
      r.next_log_id_ = (uint64_t)(1 + idx);
      r.svr_ = svrs[idx % AllSvrCnt];
    }
    // Push requests into heartbeater.
    for (int64_t idx = 0, cnt = HeartbeatRequestCnt; idx < cnt; ++idx) {
      HeartbeatRequest &r = request_array[idx];
      EXPECT_EQ(OB_SUCCESS, heartbeater_->async_heartbeat_req(&r));
      if (0 == (idx % 1000)) {
        usec_sleep(10 * _MSEC_);
      }
    }
    // Wait for requests end. Max test time should set.
    int64_t end_request_cnt = 0;
    const int64_t TestTimeLimit = 10 * _MIN_;
    const int64_t start_test_tstamp = get_timestamp();
    while (((get_timestamp() - start_test_tstamp) < TestTimeLimit)
           && (end_request_cnt < HeartbeatRequestCnt)) {
      for (int64_t idx = 0, cnt = HeartbeatRequestCnt; idx < cnt; ++idx) {
        HeartbeatRequest &r = request_array[idx];
        if (HeartbeatRequest::DONE == r.get_state()) {
          end_request_cnt += 1;
          r.set_state(HeartbeatRequest::IDLE);
        }
      }
      usec_sleep(100 * _MSEC_);
    }
    // Assert if test cannot finish.
    EXPECT_EQ(HeartbeatRequestCnt, end_request_cnt);
    // Do some statistics.
    int64_t svr_consume_distribution[AllSvrCnt]; // 1, 2, 3, ...
    for (int64_t idx = 0, cnt = AllSvrCnt; idx < cnt; ++idx) {
      svr_consume_distribution[idx] = 0;
    }
    int64_t succ_cnt = 0;
    for (int64_t idx = 0, cnt = HeartbeatRequestCnt; idx < cnt; ++idx) {
      svr_consume_distribution[idx % AllSvrCnt] += 1;
    }
    delete[] request_array;
    const int64_t BuffSize = 1024;
    char buf[BuffSize];
    int64_t pos = 0;
    for (int64_t idx = 0, cnt = AllSvrCnt; idx < cnt; ++idx) {
      pos += snprintf(buf + pos, BuffSize - pos, "svr_cnt:%ld perc:%f  ", (1 + idx),
                      ((double)svr_consume_distribution[idx] / (double)HeartbeatRequestCnt));
    }
    fprintf(stderr, "request count: %ld  distribution: %s  succeed perc: %f \n",
            HeartbeatRequestCnt, buf, (double)succ_cnt / (double)HeartbeatRequestCnt);
    return OB_SUCCESS;
  }
};

////////////////////// Boundary tests //////////////////////////////////////////
// Heartbeater init fail
TEST(Heartbeater, BasicFuncTest3)
{
  //_I_("called", "prepare:", 100);

  MockRpcInterfaceFactory rpc_factory;
  MockFetcherErrHandler1 err_handler1;
  FixedJobPerWorkerPool worker_pool;
  const int64_t heartbeat_worker_cnt = 3;
  Heartbeater heartbeater;

  int err = OB_SUCCESS;
  err = worker_pool.init(1);
  EXPECT_EQ(OB_SUCCESS, err);

  err = heartbeater.init(NULL, &err_handler1, &worker_pool, heartbeat_worker_cnt);
  EXPECT_EQ(OB_INVALID_ARGUMENT, err);
  err = heartbeater.init(&rpc_factory, NULL, &worker_pool, heartbeat_worker_cnt);
  EXPECT_EQ(OB_INVALID_ARGUMENT, err);
  err = heartbeater.init(&rpc_factory, &err_handler1, NULL, heartbeat_worker_cnt);
  EXPECT_EQ(OB_INVALID_ARGUMENT, err);
  // heartbeat_worker_cnt error, [0, 32]
  int64_t heartbeat_worker_cnt_err1 = -1;
  err = heartbeater.init(&rpc_factory, &err_handler1, &worker_pool, heartbeat_worker_cnt_err1);
  EXPECT_EQ(OB_INVALID_ARGUMENT, err);
  int64_t heartbeat_worker_cnt_err2 = 33;
  err = heartbeater.init(&rpc_factory, &err_handler1, &worker_pool, heartbeat_worker_cnt_err2);
  EXPECT_EQ(OB_INVALID_ARGUMENT, err);
}

// Heartbeater aync_heartbeat_req fail
TEST(Heartbeater, BasicFuncTest4)
{
  MockRpcInterfaceFactory rpc_factory;
  MockFetcherErrHandler1 err_handler1;
  FixedJobPerWorkerPool worker_pool;
  const int64_t heartbeat_worker_cnt = 3;
  Heartbeater heartbeater;

  int err = OB_SUCCESS;
  err = worker_pool.init(1);
  EXPECT_EQ(OB_SUCCESS, err);

  err = heartbeater.init(&rpc_factory, &err_handler1, &worker_pool, heartbeat_worker_cnt);
  EXPECT_EQ(OB_SUCCESS, err);

  // Build Heartbeater requests.
  ObAddr svr = ObAddr(ObAddr::IPV4, "127.0.0.1", (int32_t)(1000));
  HeartbeatRequest req;
  req.reset();
  req.pkey_ = ObPartitionKey((uint64_t)(1000), 0, 1);
  req.next_log_id_ = (uint64_t)(100);
  req.svr_ = svr;
  req.set_state(HeartbeatRequest::REQ);

  err = heartbeater.async_heartbeat_req(NULL);
	EXPECT_NE(OB_SUCCESS, err);
  err = heartbeater.async_heartbeat_req(&req);
	EXPECT_NE(OB_SUCCESS, err);

  err = heartbeater.destroy();
  EXPECT_EQ(OB_SUCCESS, err);

  err = worker_pool.destroy();
  EXPECT_EQ(OB_SUCCESS, err);
}

/*
 * Test workflow
 */
//TEST(DISABLED_Heartbeater, BasicFuncTest5)
TEST(Heartbeater, BasicFuncTest5)
{
  _I_("called", "func:", "workflow");
  MockFetcherErrHandler1 err_handler1;
  MockRpcInterfaceFactory rpc_factory;
  FixedJobPerWorkerPool worker_pool;
  Heartbeater heartbeater;

  int err = OB_SUCCESS;
  err = worker_pool.init(1);
  EXPECT_EQ(OB_SUCCESS, err);
  _I_("workflow", "worker_pool:", "init OB_SUCCESS");

  err = heartbeater.init(&rpc_factory, &err_handler1, &worker_pool, 3);
  EXPECT_EQ(OB_SUCCESS, err);
  _I_("workflow", "heartbeat:", "init OB_SUCCESS");

  const int64_t TestWorkerCnt = 3;
  TestWorker workers[TestWorkerCnt];
  for (int64_t idx = 0, cnt = TestWorkerCnt; idx < cnt; ++idx) {
    TestWorker &w = workers[idx];
    w.heartbeater_ = &heartbeater;
    w.create();
    _I_("workflow", "thread:", "create OB_SUCCESS");
  }

  for (int64_t idx = 0, cnt = TestWorkerCnt; idx < cnt; ++idx) {
    TestWorker &w = workers[idx];
    w.join();
    _I_("workflow", "thread:", "join OB_SUCCESS");
  }

  err = heartbeater.destroy();
  EXPECT_EQ(OB_SUCCESS, err);
  _I_("workflow", "heartbeat:", "destroy OB_SUCCESS");

  err = worker_pool.destroy();
  EXPECT_EQ(OB_SUCCESS, err);
  _I_("workflow", "work pool:", "destroy OB_SUCCESS");
}

}//end of basic_func_test_1
}//end of unittest
}//end of oceanbase

int main(int argc, char **argv)
{
  // ObLogger::get_logger().set_mod_log_levels("ALL.*:DEBUG, TLOG.*:DEBUG");
  // testing::InitGoogleTest(&argc,argv);
  // testing::FLAGS_gtest_filter = "DO_NOT_RUN";
  int ret = 1;
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_log_fetcher_heartbeat_mgr.log", true);
  logger.set_log_level(OB_LOG_LEVEL_INFO);
  testing::InitGoogleTest(&argc, argv);
  ret = RUN_ALL_TESTS();
  return ret;
}
