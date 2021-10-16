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
#include "lib/atomic/ob_atomic.h"
#include "ob_log_utils.h"
#define private public
#include "test_ob_log_fetcher_common_utils.h"
#include "ob_log_start_log_id_locator.h"

using namespace oceanbase;
using namespace common;
using namespace liboblog;

namespace oceanbase
{
namespace unittest
{
class TestObLogStartLogIdLocator: public ::testing::Test
{
public :
  virtual void SetUp() {}
  virtual void TearDown() {}
public :
  static const int64_t WORKER_COUNT = 3;
  static const int64_t LOCATE_COUNT = 1;
  static const int64_t SINGLE_WORKER_COUNT = 1;
};

static const int64_t SERVER_COUNT = 10;
static const int64_t START_LOG_ID_REQUEST_COUNT = 5 * 10000;
// for test break info
static const int64_t BREAK_INFO_START_LOG_ID_REQUEST_COUNT = 256;
static const int64_t TEST_TIME_LIMIT = 10 * _MIN_;

void generate_req(const int64_t req_cnt, StartLogIdLocateReq *&request_array,
    const int64_t start_tstamp)
{
	// Build requests.
  const int64_t AllSvrCnt = 10;
  ObAddr svrs[AllSvrCnt];
  for (int64_t idx = 0, cnt = AllSvrCnt; idx < cnt; ++idx) {
    svrs[idx] = ObAddr(ObAddr::IPV4, "127.0.0.1", (int32_t)(idx + 1000));
  }

  request_array = new StartLogIdLocateReq[req_cnt];
  for (int64_t idx = 0, cnt = req_cnt; idx < cnt; ++idx) {
  	StartLogIdLocateReq &r = request_array[idx];
  	r.reset();
  	r.pkey_ = ObPartitionKey((uint64_t)(1000 + idx), 0, 1);
  	r.start_tstamp_ = start_tstamp;
  	// Set server list.
  	for (int64_t idx2 = 0, cnt2 = AllSvrCnt; idx2 < cnt2; ++idx2) {
    	StartLogIdLocateReq::SvrItem item;
      item.reset();
      item.svr_ = svrs[idx2];
      r.svr_list_.push_back(item);
    }
  }
}

void free_req(StartLogIdLocateReq *request_array)
{
  delete[] request_array;
}

/*
 * Worker.
 */
class TestWorker : public liboblog::Runnable
{
public:
  ObLogStartLogIdLocator *locator_;
	StartLogIdLocateReq *request_array_;
	int64_t request_cnt_;
	int64_t all_svr_cnt_;
	bool push_req_finish_;

	void reset(ObLogStartLogIdLocator *locator, StartLogIdLocateReq *req_array,
						 int64_t req_cnt, int64_t all_svr_cnt)
	{
		locator_ = locator;
		request_array_ = req_array;
		request_cnt_ = req_cnt;
		all_svr_cnt_ = all_svr_cnt;
		push_req_finish_ = false;
	}

	virtual int routine()
  {
     // Push requests into locator.
    for (int64_t idx = 0, cnt = request_cnt_; idx < cnt; ++idx) {
      StartLogIdLocateReq &r = request_array_[idx];
      EXPECT_EQ(OB_SUCCESS, locator_->async_start_log_id_req(&r));
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
        StartLogIdLocateReq &r = request_array_[idx];
        if (StartLogIdLocateReq::DONE == r.get_state()) {
          end_request_cnt += 1;
          r.set_state(StartLogIdLocateReq::IDLE);
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
      StartLogIdLocateReq &r = request_array_[idx];
      EXPECT_GE(r.svr_list_consumed_, 0);
      svr_consume_distribution[(r.svr_list_consumed_ - 1)] += 1;
      uint64_t start_log_id = 0;
			common::ObAddr svr;
      if (r.get_result(start_log_id, svr)) {
        succ_cnt += 1;
        EXPECT_EQ(r.pkey_.table_id_, start_log_id);
      }
    }

    const int64_t BuffSize = 1024;
    char buf[BuffSize];
    int64_t pos = 0;
    for (int64_t idx = 0, cnt = all_svr_cnt_; idx < cnt; ++idx) {
      pos += snprintf(buf + pos, BuffSize - pos, "svr_cnt:%ld perc:%f  ", (1 + idx),
                      ((double)svr_consume_distribution[idx] / (double)request_cnt_));
    }
    fprintf(stderr, "request count: %ld  distribution: %s  succeed perc: %f \n",
            request_cnt_, buf, (double)succ_cnt / (double)request_cnt_);


    return OB_SUCCESS;
  }
};

//////////////////////Basic function tests//////////////////////////////////////////
TEST_F(TestObLogStartLogIdLocator, start_log_id_request)
{
	StartLogIdLocateReq req;
	req.reset();
	EXPECT_TRUE(req.is_state_idle());

	req.set_state_req();
	EXPECT_TRUE(req.is_state_req());
	EXPECT_EQ(StartLogIdLocateReq::REQ, req.get_state());

	req.set_state_done();
	EXPECT_TRUE(req.is_state_done());
	EXPECT_EQ(StartLogIdLocateReq::DONE, req.get_state());

	req.set_state_idle();
	EXPECT_TRUE(req.is_state_idle());
	EXPECT_EQ(StartLogIdLocateReq::IDLE, req.get_state());

	/// build svr_list
	int ret = OB_SUCCESS;
  ObAddr svr_list[SERVER_COUNT];
  for (int64_t idx = 0, cnt = SERVER_COUNT; idx < cnt; ++idx) {
     svr_list[idx] = ObAddr(ObAddr::IPV4, "127.0.0.1", (int32_t)(idx + 1000));
  }

  for (int64_t idx = 0, cnt = SERVER_COUNT; (OB_SUCCESS == ret) && idx < cnt; ++idx) {
		StartLogIdLocateReq::SvrItem item;
		item.reset(svr_list[idx]);
		if (OB_FAIL(req.svr_list_.push_back(item))) {
			LOG_ERROR("push error", K(ret));
		}
  }
	EXPECT_EQ(SERVER_COUNT, req.svr_list_.count());

	// next_svr_item, cur_svr_item
  for (int64_t idx = 0, cnt = SERVER_COUNT; (OB_SUCCESS == ret) && idx < cnt; ++idx) {
		StartLogIdLocateReq::SvrItem *item;

		EXPECT_EQ(OB_SUCCESS, req.next_svr_item(item));
		EXPECT_EQ(svr_list[idx], item->svr_);
		EXPECT_EQ(OB_SUCCESS, req.cur_svr_item(item));
		EXPECT_EQ(svr_list[idx], item->svr_);
	}
	// is_request_ended, get_result
	EXPECT_TRUE(req.is_request_ended(LOCATE_COUNT));
	uint64_t start_log_id = 0;
	common::ObAddr svr;
	EXPECT_FALSE(req.get_result(start_log_id, svr));
  EXPECT_EQ(OB_INVALID_ID, start_log_id);
}

//TEST_F(TestObLogStartLogIdLocator, DISABLED_locator)
TEST_F(TestObLogStartLogIdLocator, locator)
{
	const int64_t TestWorkerCnt = 3;
	// genereate data
	StartLogIdLocateReq *request_arrays[TestWorkerCnt];
	for (int64_t idx = 0; idx < TestWorkerCnt; idx++) {
		//StartLogIdLocateReq *request_array = NULL;
		generate_req(START_LOG_ID_REQUEST_COUNT, request_arrays[idx], get_timestamp());
		OB_ASSERT(NULL != request_arrays[idx]);
	}

  MockFetcherErrHandler1 err_handler1;
	MockObLogStartLogIdRpc rpc;
	ObLogStartLogIdLocator locator;

	EXPECT_EQ(OB_SUCCESS, locator.init(WORKER_COUNT, LOCATE_COUNT, rpc, err_handler1));
	EXPECT_EQ(OB_SUCCESS, locator.start());

  TestWorker workers[TestWorkerCnt];
  for (int64_t idx = 0, cnt = TestWorkerCnt; idx < cnt; ++idx) {
    TestWorker &w = workers[idx];
		w.reset(&locator, request_arrays[idx], START_LOG_ID_REQUEST_COUNT, SERVER_COUNT);
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
	locator.destroy();
}

TEST_F(TestObLogStartLogIdLocator, test_out_of_lower_bound)
{
  // Default configuration of observer log retention time
  int64_t default_clog_save_time = ObLogStartLogIdLocator::g_observer_clog_save_time;

  MockFetcherErrHandler1 err_handler1;
	MockObLogStartLogIdRpc rpc;
	ObLogStartLogIdLocator locator;

	EXPECT_EQ(OB_SUCCESS, locator.init(WORKER_COUNT, LOCATE_COUNT, rpc, err_handler1));
	EXPECT_EQ(OB_SUCCESS, locator.start());

	// Generate data, set start time to current time
	StartLogIdLocateReq *req = NULL;
  int64_t start_tstamp = get_timestamp();
  generate_req(1, req, start_tstamp);

  // RPC setup, observer returns success, but partition returns log less than lower bound
  rpc.set_err(OB_SUCCESS, OB_ERR_OUT_OF_LOWER_BOUND);

  EXPECT_EQ(OB_SUCCESS, locator.async_start_log_id_req(req));
  while (req->get_state() != StartLogIdLocateReq::DONE) {
    usec_sleep(100 * _MSEC_);
  }

  // Since all servers return less than the lower bound and have a start time stamp of less than 2 hours,
  // expect the location to succeed
  uint64_t start_log_id = OB_INVALID_ID;
  common::ObAddr svr;
  EXPECT_EQ(true, req->get_result(start_log_id, svr));
  EXPECT_EQ(req->pkey_.get_table_id(), start_log_id);

	// free
  free_req(req);
  req = NULL;

  ///////////////  Set the start time past the log retention time, in which case the location returns a failure  ///////////////////
  // Start-up time less than minimum log retention time
  start_tstamp = get_timestamp() - default_clog_save_time - 1;
  generate_req(1, req, start_tstamp); // Regeneration request

  // RPC setup, observer returns success, but partition returns less than lower bound
  rpc.set_err(OB_SUCCESS, OB_ERR_OUT_OF_LOWER_BOUND);

  // Execute location requests
  EXPECT_EQ(OB_SUCCESS, locator.async_start_log_id_req(req));
  while (req->get_state() != StartLogIdLocateReq::DONE) {
    usec_sleep(100 * _MSEC_);
  }

  // Although all servers return less than the lower bound, the start-up timestamp is no longer within the log retention time
  // and expects the location to fail
  EXPECT_EQ(false, req->get_result(start_log_id, svr));

	// free
  free_req(req);
  req = NULL;

  // destroy locator
	locator.destroy();
}

// When the break_info message is returned, test the correct processing
TEST_F(TestObLogStartLogIdLocator, break_info_test)
{
	// genereate data
	StartLogIdLocateReq *request_array;
	generate_req(BREAK_INFO_START_LOG_ID_REQUEST_COUNT, request_array, get_timestamp());
	OB_ASSERT(NULL != request_array);

	MockFetcherErrHandler1 err_handler1;
	MockObLogRpcDerived2 rpc;
	EXPECT_EQ(OB_SUCCESS, rpc.init(BREAK_INFO_START_LOG_ID_REQUEST_COUNT));
	ObLogStartLogIdLocator locator;

	EXPECT_EQ(OB_SUCCESS, locator.init(SINGLE_WORKER_COUNT, LOCATE_COUNT, rpc, err_handler1));

	// Insert all data first, then open the StartLogIdLocator thread to ensure that all subsequent requests are aggregated on a single server;
	TestWorker worker;
	worker.reset(&locator, request_array, BREAK_INFO_START_LOG_ID_REQUEST_COUNT, SERVER_COUNT);
	worker.create();

	while (false == ATOMIC_LOAD(&worker.push_req_finish_)) {
	}

	EXPECT_EQ(OB_SUCCESS, locator.start());

	// join
	worker.join();
	// free
	free_req(request_array);
	request_array = NULL;

	locator.destroy();
	rpc.destroy();
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
  logger.set_file_name("test_ob_log_start_log_id_locator.log", true);
  logger.set_log_level(OB_LOG_LEVEL_INFO);
  testing::InitGoogleTest(&argc, argv);
  ret = RUN_ALL_TESTS();
  return ret;
}
