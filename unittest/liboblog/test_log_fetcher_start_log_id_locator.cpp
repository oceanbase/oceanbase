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
#include "liboblog/src/ob_log_fetcher_start_log_id_locator.h"

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
 * StartLogIdLocator Tests.
 */
/*
 * Basic function test 1.
 *  - N thread & M requests for each thread
 *  - result start log id = 1
 *  - rpc always succeed, no server internal error
 *  - rpc interface breaks the locating process randomly (30%)
 *  - rpc interface returns correct result or an error code randomly (30%)
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

/*
 * Worker.
 */
class TestWorker : public Runnable
{
public:
  StartLogIdLocator *locator_;
  virtual int routine()
  {
    // Build requests.
    const int64_t AllSvrCnt = 3;
    ObAddr svrs[AllSvrCnt];
    for (int64_t idx = 0, cnt = AllSvrCnt; idx < cnt; ++idx) {
      svrs[idx] = ObAddr(ObAddr::IPV4, "127.0.0.1", (int32_t)(idx + 1000));
    }
    const int64_t RequestCnt = 10 * 10000;
    StartLogIdLocatorRequest *request_array = new StartLogIdLocatorRequest[RequestCnt];
    for (int64_t idx = 0, cnt = RequestCnt; idx < cnt; ++idx) {
      StartLogIdLocatorRequest &r = request_array[idx];
      r.reset();
      r.pkey_ = ObPartitionKey((uint64_t)(1000 + idx), 0, 1);
      r.start_tstamp_ = 1 + idx;
      // Set server list.
      for (int64_t idx2 = 0, cnt2 = AllSvrCnt; idx2 < cnt2; ++idx2) {
        StartLogIdLocatorRequest::SvrListItem item;
        item.reset();
        item.svr_ = svrs[idx2];
        r.svr_list_.push_back(item);
      }
    }
    // Push requests into locator.
    for (int64_t idx = 0, cnt = RequestCnt; idx < cnt; ++idx) {
      StartLogIdLocatorRequest &r = request_array[idx];
      EXPECT_EQ(OB_SUCCESS, locator_->async_start_log_id_req(&r));
      if (0 == (idx % 1000)) {
        usec_sleep(10 * _MSEC_);
      }
    }
    // Wait for requests end. Max test time should set.
    int64_t end_request_cnt = 0;
    const int64_t TestTimeLimit = 10 * _MIN_;
    const int64_t start_test_tstamp = get_timestamp();
    while (((get_timestamp() - start_test_tstamp) < TestTimeLimit)
           && (end_request_cnt < RequestCnt)) {
      for (int64_t idx = 0, cnt = RequestCnt; idx < cnt; ++idx) {
        StartLogIdLocatorRequest &r = request_array[idx];
        if (StartLogIdLocatorRequest::DONE == r.get_state()) {
          end_request_cnt += 1;
          r.set_state(StartLogIdLocatorRequest::IDLE);
        }
      }
      usec_sleep(100 * _MSEC_);
    }
    // Assert if test cannot finish.
    EXPECT_EQ(RequestCnt, end_request_cnt);
    // Do some statistics.
    int64_t svr_consume_distribution[AllSvrCnt]; // 1, 2, 3, ...
    for (int64_t idx = 0, cnt = AllSvrCnt; idx < cnt; ++idx) {
      svr_consume_distribution[idx] = 0;
    }
    int64_t succ_cnt = 0;
    for (int64_t idx = 0, cnt = RequestCnt; idx < cnt; ++idx) {
      StartLogIdLocatorRequest &r = request_array[idx];
      EXPECT_GE(r.svr_list_consumed_, 0);
      svr_consume_distribution[(r.svr_list_consumed_ - 1)] += 1;
      uint64_t start_log_id = 0;
      if (r.get_result(start_log_id)) {
        succ_cnt += 1;
        EXPECT_EQ(1, start_log_id);
      }
    }
    delete[] request_array;
    const int64_t BuffSize = 1024;
    char buf[BuffSize];
    int64_t pos = 0;
    for (int64_t idx = 0, cnt = AllSvrCnt; idx < cnt; ++idx) {
      pos += snprintf(buf + pos, BuffSize - pos, "svr_cnt:%ld perc:%f  ", (1 + idx),
                      ((double)svr_consume_distribution[idx] / (double)RequestCnt));
    }
    fprintf(stderr, "request count: %ld  distribution: %s  succeed perc: %f \n",
            RequestCnt, buf, (double)succ_cnt / (double)RequestCnt);
    return OB_SUCCESS;
  }
};

TEST(StartLogIdLocator, BasicFuncTest1)
{
  MockFetcherErrHandler1 err_handler1;
  MockRpcInterfaceFactory rpc_factory;
  FixedJobPerWorkerPool worker_pool;
  StartLogIdLocator locator;

  int err = OB_SUCCESS;
  err = worker_pool.init(1);
  EXPECT_EQ(OB_SUCCESS, err);

  err = locator.init(&rpc_factory, &err_handler1, &worker_pool, 3);
  EXPECT_EQ(OB_SUCCESS, err);

  const int64_t TestWorkerCnt = 3;
  TestWorker workers[TestWorkerCnt];
  for (int64_t idx = 0, cnt = TestWorkerCnt; idx < cnt; ++idx) {
    TestWorker &w = workers[idx];
    w.locator_ = &locator;
    w.create();
  }

  for (int64_t idx = 0, cnt = TestWorkerCnt; idx < cnt; ++idx) {
    TestWorker &w = workers[idx];
    w.join();
  }

  err = locator.destroy();
  EXPECT_EQ(OB_SUCCESS, err);

  err = worker_pool.destroy();
  EXPECT_EQ(OB_SUCCESS, err);
}

}

}
}

int main(int argc, char **argv)
{
  //ObLogger::get_logger().set_mod_log_levels("ALL.*:DEBUG, TLOG.*:DEBUG");
  testing::InitGoogleTest(&argc,argv);
  // testing::FLAGS_gtest_filter = "DO_NOT_RUN";
  return RUN_ALL_TESTS();
}
