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

#define USING_LOG_PREFIX OBLOG_FETCHER

#include <gtest/gtest.h>
#include "share/ob_define.h"
#include "lib/hash/ob_linear_hash_map.h"  // ObLinearHashMap
#include "lib/atomic/ob_atomic.h"
#define private public
#include "test_ob_log_fetcher_common_utils.h"
#include "ob_log_utils.h"
#include "ob_log_rpc.h"
#include "ob_log_fetcher_heartbeat_worker.h"

using namespace oceanbase;
using namespace common;
using namespace liboblog;

namespace oceanbase
{
namespace unittest
{
class TestObLogFetcherHeartbeatWorker: public ::testing::Test
{
public :
  virtual void SetUp() {}
  virtual void TearDown() {}
public :
  static const int64_t SINGLE_WORKER_COUNT = 1;
  static const int64_t WORKER_COUNT = 3;
};

static const int64_t ONE_SERVER_COUNT = 1;
static const int64_t SERVER_COUNT = 3;

static const int64_t HEARTBEATER_REQUEST_COUNT = 5 * 10000;
static const int64_t SMALL_HEARTBEATER_REQUEST_COUNT = 1000;

static const int64_t MAP_MOD_ID = 1;

static const int64_t TEST_TIME_LIMIT = 10 * _MIN_;
static const int64_t FIXED_TIMESTAMP = 10000;


class MockObLogRpcBaseHeartbeat : public IObLogRpc
{
public:
	typedef common::ObLinearHashMap<common::ObPartitionKey, common::ObAddr> PkeySvrMap;
public:
	MockObLogRpcBaseHeartbeat(PkeySvrMap &map) : pkey_svr_map_(map) {}
  virtual ~MockObLogRpcBaseHeartbeat() { }

  // Request start log id based on timestamp
  virtual int req_start_log_id_by_tstamp(const common::ObAddr &svr,
      const obrpc::ObLogReqStartLogIdByTsRequestWithBreakpoint& req,
      obrpc::ObLogReqStartLogIdByTsResponseWithBreakpoint& res,
      const int64_t timeout)
  {
	  int ret = OB_SUCCESS;
		UNUSED(svr);
		UNUSED(req);
		UNUSED(res);
		UNUSED(timeout);

		return ret;
	}

  // Request Leader Heartbeat
  virtual int req_leader_heartbeat(const common::ObAddr &svr,
      const obrpc::ObLogLeaderHeartbeatReq &req,
      obrpc::ObLogLeaderHeartbeatResp &res,
      const int64_t timeout)
  {
	  int ret = OB_SUCCESS;
		UNUSED(svr);
		UNUSED(req);
		UNUSED(res);
		UNUSED(timeout);

		return ret;
	}

  // Open a new stream
  // Synchronous RPC
  virtual int open_stream(const common::ObAddr &svr,
      const obrpc::ObLogOpenStreamReq &req,
      obrpc::ObLogOpenStreamResp &resp,
      const int64_t timeout)
  {
	  int ret = OB_SUCCESS;
		UNUSED(svr);
		UNUSED(req);
		UNUSED(resp);
		UNUSED(timeout);

		return ret;
	}

  // Stream based, get logs
  // Asynchronous RPC
  virtual int async_stream_fetch_log(const common::ObAddr &svr,
      const obrpc::ObLogStreamFetchLogReq &req,
      obrpc::ObLogExternalProxy::AsyncCB<obrpc::OB_LOG_STREAM_FETCH_LOG> &cb,
      const int64_t timeout)
  {
	  int ret = OB_SUCCESS;
		UNUSED(svr);
		UNUSED(req);
		UNUSED(cb);
		UNUSED(timeout);

		return ret;
	}
private:
    // Record the pkey-svr mapping, which is used to verify that the pkey was sent to the expected observer when the rpc is received
	PkeySvrMap &pkey_svr_map_;
};

class MockObLogRpcDerived1Heartbeat : public MockObLogRpcBaseHeartbeat
{
public:
	MockObLogRpcDerived1Heartbeat(PkeySvrMap &map) : MockObLogRpcBaseHeartbeat(map) {}
  virtual ~MockObLogRpcDerived1Heartbeat() { }

  // Requesting a leader heartbeat
	// 1. rpc always assumes success
	// 2. 10% probability of server internal error
	// 3. 30% probability that the partition returns OB_SUCESS, 30% probability that OB_NOT_MASTER when server succeeds,
	// 30% chance of returning OB_PARTITON_NOT_EXIST, 10% chance of returning other
  virtual int req_leader_heartbeat(const common::ObAddr &svr,
      const obrpc::ObLogLeaderHeartbeatReq &req,
      obrpc::ObLogLeaderHeartbeatResp &res,
      const int64_t timeout)
  {
	  int ret = OB_SUCCESS;
		UNUSED(svr);
		UNUSED(timeout);

		res.reset();
    res.set_debug_err(OB_SUCCESS);
    // Seed.
    int64_t seed = (get_timestamp());
		int64_t rand = (seed) % 100;
		bool svr_internal_err = (rand < 10);
		if (svr_internal_err) {
			res.set_err(OB_ERR_UNEXPECTED);
		} else {
			res.set_err(OB_SUCCESS);
    	for (int64_t idx = 0, cnt = req.get_params().count(); OB_SUCCESS == ret && idx < cnt; ++idx) {
      	// 30%.
        seed = get_timestamp();
				rand = (idx + seed) % 100;
      	bool succeed = (rand < 30);
      	bool not_master = (30 <= rand) && (rand < 60);
      	bool partition_not_exist = (60 <= rand) && (rand < 90);

      	const obrpc::ObLogLeaderHeartbeatReq::Param &param = req.get_params().at(idx);
      	obrpc::ObLogLeaderHeartbeatResp::Result result;
      	result.reset();
				if (succeed) {
      		result.err_ = OB_SUCCESS;
		    } else if (not_master) {
      		result.err_ = OB_NOT_MASTER;
				} else if (partition_not_exist) {
      		result.err_ = OB_PARTITION_NOT_EXIST;
				} else {
      		result.err_ = OB_ERR_UNEXPECTED;
				}
        result.next_served_log_id_ = (succeed || not_master) ? param.next_log_id_ : OB_INVALID_ID;
        result.next_served_ts_ = (succeed || not_master) ? FIXED_TIMESTAMP : OB_INVALID_TIMESTAMP;

    	  EXPECT_EQ(OB_SUCCESS, res.append_result(result));

				// Verify that the partitions correspond to the same request server
				common::ObAddr cur_svr;
				if (OB_FAIL(pkey_svr_map_.get(param.pkey_, cur_svr))) {
					LOG_ERROR("pkey_svr_map_ get error", K(ret), K(param), K(cur_svr));
				} else {
					EXPECT_EQ(svr, cur_svr);
				}
			}
		}

    LOG_DEBUG("req leader heartbeat", K(req), K(res));

		return ret;
	}
};

class MockObLogRpcDerived2Heartbeat : public MockObLogRpcBaseHeartbeat
{
public:
	MockObLogRpcDerived2Heartbeat(PkeySvrMap &map) : MockObLogRpcBaseHeartbeat(map) {}
	virtual ~MockObLogRpcDerived2Heartbeat() { }

	// Request leader heartbeat
	// 1. rpc always assumes success, no server internal error
	// 2. partitions all return OB_SUCESS
	virtual int req_leader_heartbeat(const common::ObAddr &svr,
			const obrpc::ObLogLeaderHeartbeatReq &req,
			obrpc::ObLogLeaderHeartbeatResp &res,
			const int64_t timeout)
	{
		int ret = OB_SUCCESS;
		UNUSED(svr);
		UNUSED(timeout);

		res.reset();
		res.set_debug_err(OB_SUCCESS);
		res.set_err(OB_SUCCESS);
		for (int64_t idx = 0, cnt = req.get_params().count(); OB_SUCCESS == ret && idx < cnt; ++idx) {
			obrpc::ObLogLeaderHeartbeatResp::Result result;
			const obrpc::ObLogLeaderHeartbeatReq::Param &param = req.get_params().at(idx);
			result.reset();
			result.err_ = OB_SUCCESS;
			result.next_served_log_id_ = param.next_log_id_;
			result.next_served_ts_ = FIXED_TIMESTAMP;

			EXPECT_EQ(OB_SUCCESS, res.append_result(result));

			// Verify that the partitions correspond to the same request server
			common::ObAddr cur_svr;
			if (OB_FAIL(pkey_svr_map_.get(param.pkey_, cur_svr))) {
				LOG_ERROR("pkey_svr_map_ get error", K(ret), K(param), K(cur_svr));
			} else {
				EXPECT_EQ(svr, cur_svr);
			}
		}

		LOG_DEBUG("req leader heartbeat", K(req), K(res));

		return ret;
	}
};

void generate_req(const int64_t all_svr_cnt,
		    					const int64_t req_cnt,
									HeartbeatRequest *&request_array,
	                common::ObLinearHashMap<common::ObPartitionKey, common::ObAddr> &map)
{
	// Build requests.
	ObAddr svrs[all_svr_cnt];
	for (int64_t idx = 0, cnt = all_svr_cnt; idx < cnt; ++idx) {
		svrs[idx] = ObAddr(ObAddr::IPV4, "127.0.0.1", (int32_t)(idx + 1000));
	}

  request_array = new HeartbeatRequest[req_cnt];
  for (int64_t idx = 0, cnt = req_cnt; idx < cnt; ++idx) {
  	HeartbeatRequest &r = request_array[idx];
  	r.reset();
		// set pkey, next_log_id, svr
		// next_log_id = pkey.table_id + 1
  	r.reset(ObPartitionKey((uint64_t)(1000 + idx), 0, 1), 1000 + idx + 1, svrs[idx % all_svr_cnt]);

		int ret = OB_SUCCESS;
		if (OB_FAIL(map.insert(r.pkey_, svrs[idx % all_svr_cnt]))) {
			if (OB_ENTRY_EXIST != ret) {
				LOG_ERROR("map insert error", K(ret), K(r), K(idx));
			}
		}
  }
}

void free_req(HeartbeatRequest *request_array)
{
  delete[] request_array;
}

/*
 * Worker.
 */
class TestWorker : public liboblog::Runnable
{
public:
  ObLogFetcherHeartbeatWorker *heartbeater_;
	HeartbeatRequest *request_array_;
	int64_t request_cnt_;
	int64_t all_svr_cnt_;
	bool push_req_finish_;
	double success_rate_;

	void reset(ObLogFetcherHeartbeatWorker *heartbeater, HeartbeatRequest *req_array,
						 int64_t req_cnt, int64_t all_svr_cnt)
	{
		heartbeater_ = heartbeater;
		request_array_ = req_array;
		request_cnt_ = req_cnt;
		all_svr_cnt_ = all_svr_cnt;
		push_req_finish_ = false;
		success_rate_ = 0;
	}

	virtual int routine()
  {
     // Push requests into heartbeater
    for (int64_t idx = 0, cnt = request_cnt_; idx < cnt; ++idx) {
      HeartbeatRequest &r = request_array_[idx];
      EXPECT_EQ(OB_SUCCESS, heartbeater_->async_heartbeat_req(&r));
      if (0 == (idx % 1000)) {
        usec_sleep(10 * _MSEC_);
      }
    }
		ATOMIC_STORE(&push_req_finish_, true);

    // Wait for requests end. Max test time should set.
    int64_t end_request_cnt = 0;
    const int64_t start_test_tstamp = get_timestamp();
    while (((get_timestamp() - start_test_tstamp) < TEST_TIME_LIMIT)
           && (end_request_cnt < request_cnt_)) {
      for (int64_t idx = 0, cnt = request_cnt_; idx < cnt; ++idx) {
        HeartbeatRequest &r = request_array_[idx];
        if (HeartbeatRequest::DONE == r.get_state()) {
          end_request_cnt += 1;
          r.set_state(HeartbeatRequest::IDLE);
        }
      }
      usec_sleep(100 * _MSEC_);
    }
    // Assert if test cannot finish.
    EXPECT_EQ(request_cnt_, end_request_cnt);

    // Do some statistics.
    int64_t svr_consume_distribution[all_svr_cnt_]; // 1, 2, 3, ...
    for (int64_t idx = 0, cnt = all_svr_cnt_; idx < cnt; ++idx) {
      svr_consume_distribution[idx] = 0;
    }
    int64_t succ_cnt = 0;
    for (int64_t idx = 0, cnt = request_cnt_; idx < cnt; ++idx) {
      HeartbeatRequest &req = request_array_[idx];
      svr_consume_distribution[idx % all_svr_cnt_] += 1;

	    const HeartbeatResponse &res = req.get_resp();
      if (res.next_served_log_id_ != OB_INVALID_ID
					&& res.next_served_tstamp_ != OB_INVALID_TIMESTAMP) {
        EXPECT_EQ(req.pkey_.table_id_ + 1, res.next_served_log_id_);
        EXPECT_EQ(FIXED_TIMESTAMP, res.next_served_tstamp_);
        succ_cnt += 1;
				LOG_DEBUG("verify", K(res), K(succ_cnt));
      }
    }

    const int64_t BuffSize = 1024;
    char buf[BuffSize];
    int64_t pos = 0;
    for (int64_t idx = 0, cnt = all_svr_cnt_; idx < cnt; ++idx) {
      pos += snprintf(buf + pos, BuffSize - pos, "svr_cnt:%ld perc:%f  ", (1 + idx),
                      ((double)svr_consume_distribution[idx] / (double)request_cnt_));
    }
		success_rate_ = (double)succ_cnt / (double)request_cnt_;
    fprintf(stderr, "request count: %ld  distribution: %s  succeed perc: %f \n",
            request_cnt_, buf, success_rate_);


    return OB_SUCCESS;
  }
};

//////////////////////Basic function tests//////////////////////////////////////////
TEST_F(TestObLogFetcherHeartbeatWorker, HeartbeatRequest)
{
	HeartbeatRequest req;
	req.reset();
	EXPECT_TRUE(req.is_state_idle());

	req.set_state_req();
	EXPECT_TRUE(req.is_state_req());
	EXPECT_EQ(HeartbeatRequest::REQ, req.get_state());

	req.set_state_done();
	EXPECT_TRUE(req.is_state_done());
	EXPECT_EQ(HeartbeatRequest::DONE, req.get_state());

	req.set_state_idle();
	EXPECT_TRUE(req.is_state_idle());
	EXPECT_EQ(HeartbeatRequest::IDLE, req.get_state());
}

//TEST_F(TestObLogStartLogIdLocator, DISABLED_locator)
TEST_F(TestObLogFetcherHeartbeatWorker, heartbeater)
{
	const int64_t TestWorkerCnt = 3;
	// generate data
	HeartbeatRequest *request_arrays[TestWorkerCnt];
	common::ObLinearHashMap<common::ObPartitionKey, common::ObAddr> map;
	EXPECT_EQ(OB_SUCCESS, map.init(MAP_MOD_ID));
	for (int64_t idx = 0; idx < TestWorkerCnt; idx++) {
		generate_req(SERVER_COUNT, HEARTBEATER_REQUEST_COUNT, request_arrays[idx], map);
		OB_ASSERT(NULL != request_arrays[idx]);
	}

  MockFetcherErrHandler1 err_handler1;
	MockObLogRpcDerived1Heartbeat rpc(map);
	ObLogFetcherHeartbeatWorker heartbeater;

	EXPECT_EQ(OB_SUCCESS, heartbeater.init(WORKER_COUNT, rpc, err_handler1));
	EXPECT_EQ(OB_SUCCESS, heartbeater.start());

  TestWorker workers[TestWorkerCnt];
  for (int64_t idx = 0, cnt = TestWorkerCnt; idx < cnt; ++idx) {
    TestWorker &w = workers[idx];
		w.reset(&heartbeater, request_arrays[idx], HEARTBEATER_REQUEST_COUNT, SERVER_COUNT);
    w.create();
  }

  for (int64_t idx = 0, cnt = TestWorkerCnt; idx < cnt; ++idx) {
    TestWorker &w = workers[idx];
    w.join();
  }

	// free
	for (int64_t idx = 0; idx < TestWorkerCnt; idx++) {
		free_req(request_arrays[idx]);
		request_arrays[idx] = NULL;
	}
	map.destroy();
	heartbeater.destroy();
}

// Test the request logic
// Currently aggregating up to 10,000 requests at a time, pushing more than 10,000 requests to test if multiple aggregations are possible
TEST_F(TestObLogFetcherHeartbeatWorker, aggregation)
{
	// generate data
	HeartbeatRequest *request_array;
	common::ObLinearHashMap<common::ObPartitionKey, common::ObAddr> map;
	EXPECT_EQ(OB_SUCCESS, map.init(MAP_MOD_ID));
	// All requests are made to the same server
	generate_req(ONE_SERVER_COUNT, HEARTBEATER_REQUEST_COUNT, request_array, map);
	OB_ASSERT(NULL != request_array);

	MockFetcherErrHandler1 err_handler1;
	MockObLogRpcDerived1Heartbeat rpc(map);

	ObLogFetcherHeartbeatWorker heartbeater;

	EXPECT_EQ(OB_SUCCESS, heartbeater.init(SINGLE_WORKER_COUNT, rpc, err_handler1));

	// Insert all data first, then open the StartLogIdLocator thread to ensure that all subsequent requests are aggregated on a single server;
	TestWorker worker;
	worker.reset(&heartbeater, request_array, HEARTBEATER_REQUEST_COUNT, ONE_SERVER_COUNT);
	worker.create();

	while (false == ATOMIC_LOAD(&worker.push_req_finish_)) {
	}

	EXPECT_EQ(OB_SUCCESS, heartbeater.start());

	// join
	worker.join();
	// free
	free_req(request_array);
	request_array = NULL;

	map.destroy();
	heartbeater.destroy();
}

// Test scenario: when the observer returns all the correct data, whether the ObLogFetcherHeartbeatWorker processes it correctly
TEST_F(TestObLogFetcherHeartbeatWorker, heartbeater_handle)
{
	// generate data
	HeartbeatRequest *request_array;
	common::ObLinearHashMap<common::ObPartitionKey, common::ObAddr> map;
	EXPECT_EQ(OB_SUCCESS, map.init(MAP_MOD_ID));
	generate_req(SERVER_COUNT, SMALL_HEARTBEATER_REQUEST_COUNT, request_array, map);
	OB_ASSERT(NULL != request_array);

	MockFetcherErrHandler1 err_handler1;
	MockObLogRpcDerived2Heartbeat rpc(map);
	ObLogFetcherHeartbeatWorker heartbeater;

	EXPECT_EQ(OB_SUCCESS, heartbeater.init(WORKER_COUNT, rpc, err_handler1));
	EXPECT_EQ(OB_SUCCESS, heartbeater.start());

	TestWorker worker;
	worker.reset(&heartbeater, request_array, SMALL_HEARTBEATER_REQUEST_COUNT, SERVER_COUNT);
	worker.create();

	while (0 == ATOMIC_LOAD((int64_t*)&worker.success_rate_)) {
	}
	// all request succ
	EXPECT_EQ(1, worker.success_rate_);

	worker.join();

	// free
	free_req(request_array);
	request_array = NULL;

	map.destroy();
	heartbeater.destroy();
}


}//end of unittest
}//end of oceanbase

int main(int argc, char **argv)
{
  // ObLogger::get_logger().set_mod_log_levels("ALL.*:DEBUG, TLOG.*:DEBUG");
  // testing::InitGoogleTest(&argc,argv);
  // testing::FLAGS_gtest_filter = "DO_NOT_RUN";
  int ret = 1;
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_ob_log_heartbeater.log", true);
  logger.set_log_level(OB_LOG_LEVEL_INFO);
  testing::InitGoogleTest(&argc, argv);
  ret = RUN_ALL_TESTS();
  return ret;
}
