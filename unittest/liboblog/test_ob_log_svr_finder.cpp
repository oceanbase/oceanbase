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
#define private public
#include "test_ob_log_fetcher_common_utils.h"
#include "ob_log_utils.h"
#include "ob_log_svr_finder.h"
#include "ob_log_all_svr_cache.h"
#include "lib/atomic/ob_atomic.h"

using namespace oceanbase;
using namespace common;
using namespace liboblog;

namespace oceanbase
{
namespace unittest
{
class TestObLogSvrFinder: public ::testing::Test
{
public :
  virtual void SetUp() {}
  virtual void TearDown() {}
public :
  static const int64_t SVR_FINDER_THREAD_NUM = 1;
};

static const int64_t TEST_TIME_LIMIT    = 10 * _MIN_;

void generate_part_svr_list(const int64_t count, PartSvrList *&part_svr_list)
{
	part_svr_list = static_cast<PartSvrList *>(
			            ob_malloc(sizeof(PartSvrList) * count));
	for (int64_t idx = 0; idx < count; idx++) {
		new (part_svr_list + idx) PartSvrList();
	}
}

// Constructing SvrFindReq, two types of requests
// 1. logid request
// 2. timestamp request
void generate_svr_finder_requset(const int64_t count,
		                             PartSvrList *part_svr_list,
		 			                       SvrFindReq *&svr_req_array)
{
	svr_req_array = static_cast<SvrFindReq *>(
			            ob_malloc(sizeof(SvrFindReq) * count));
	for (int64_t idx = 0; idx < count; idx++) {
		new (svr_req_array + idx) SvrFindReq();
    ObPartitionKey pkey = ObPartitionKey((uint64_t)(1000 + idx), 0, 1);

		const int64_t seed = get_timestamp();
		if ((seed % 100) < 50) {
    	svr_req_array[idx].reset_for_req_by_log_id(part_svr_list[idx], pkey, idx);
			EXPECT_TRUE(svr_req_array[idx].is_state_idle());
		} else {
    	svr_req_array[idx].reset_for_req_by_tstamp(part_svr_list[idx], pkey, seed);
			EXPECT_TRUE(svr_req_array[idx].is_state_idle());
		}
	}
}

// build LeaderFindReq
void generate_leader_finder_request(const int64_t count, LeaderFindReq *&leader_req_array)
{
	leader_req_array = static_cast<LeaderFindReq *>(
			               ob_malloc(sizeof(LeaderFindReq) * count));

	for (int64_t idx = 0; idx < count; idx++) {
		new (leader_req_array + idx) LeaderFindReq();
    ObPartitionKey pkey = ObPartitionKey((uint64_t)(1000 + idx), 0, 1);
		leader_req_array[idx].reset(pkey);
		EXPECT_TRUE(leader_req_array[idx].is_state_idle());
	}
}

void wait_svr_finer_req_end(SvrFindReq *svr_req_array,
		                        const int64_t count,
		                        int64_t &end_request_cnt)
{
  end_request_cnt = 0;
  const int64_t start_test_tstamp = get_timestamp();
  while (((get_timestamp() - start_test_tstamp) < TEST_TIME_LIMIT)
           && (end_request_cnt < count)) {
    for (int64_t idx = 0, cnt = count; idx < cnt; ++idx) {
    	SvrFindReq &r = svr_req_array[idx];
      if (SvrFindReq::DONE == r.get_state()) {
        end_request_cnt += 1;
				r.set_state_idle();
      }
    }
    usec_sleep(100 * _MSEC_);
  }
}

void wait_leader_finer_req_end(LeaderFindReq *leader_req_array,
		                           const int64_t count,
		                           int64_t &end_request_cnt)
{
  end_request_cnt = 0;
  const int64_t start_test_tstamp = get_timestamp();
  while (((get_timestamp() - start_test_tstamp) < TEST_TIME_LIMIT)
           && (end_request_cnt < count)) {
    for (int64_t idx = 0, cnt = count; idx < cnt; ++idx) {
    	LeaderFindReq &r = leader_req_array[idx];
      if (LeaderFindReq::DONE == r.get_state()) {
        end_request_cnt += 1;
				r.set_state_idle();
      }
    }
    usec_sleep(100 * _MSEC_);
  }
}

//////////////////////Basic function tests//////////////////////////////////////////
TEST_F(TestObLogSvrFinder, init)
{
	MockFetcherErrHandler1 err_handler;
	MockSysTableHelperDerive1 mock_systable_helper;

	// AllSvrCache init
	ObLogAllSvrCache all_svr_cache;
	EXPECT_EQ(OB_SUCCESS, all_svr_cache.init(mock_systable_helper, err_handler));

	// SvrFinder init
	ObLogSvrFinder svr_finder;
	EXPECT_EQ(OB_SUCCESS, svr_finder.init(SVR_FINDER_THREAD_NUM, err_handler,
																				all_svr_cache, mock_systable_helper));
	// sever list for partition
	PartSvrList *part_svr_list = NULL;
	generate_part_svr_list(SVR_FINDER_REQ_NUM, part_svr_list);

	// Constructing SvrFindReq, two types of requests
	// 1. logid request
	// 2. timestamp request
	SvrFindReq *svr_req_array = NULL;
	generate_svr_finder_requset(SVR_FINDER_REQ_NUM, part_svr_list, svr_req_array);

	// build LeaderFindReq
	LeaderFindReq *leader_req_array = NULL;
	generate_leader_finder_request(LEADER_FINDER_REQ_NUM, leader_req_array);

	// push request to svr_finder
	for (int64_t idx = 0; idx < SVR_FINDER_REQ_NUM; idx++) {
		EXPECT_EQ(OB_SUCCESS, svr_finder.async_svr_find_req(svr_req_array + idx));
	}
	for (int64_t idx = 0; idx < LEADER_FINDER_REQ_NUM; idx++) {
		EXPECT_EQ(OB_SUCCESS, svr_finder.async_leader_find_req(leader_req_array + idx));
	}

	// SvrFinder start
	EXPECT_EQ(OB_SUCCESS, svr_finder.start());

  // Wait for asynchronous SvrFinderReq to finish
  int64_t end_svr_finder_req_cnt = 0;
	wait_svr_finer_req_end(svr_req_array, SVR_FINDER_REQ_NUM, end_svr_finder_req_cnt);
  // Assert
  EXPECT_EQ(SVR_FINDER_REQ_NUM, end_svr_finder_req_cnt);

  // Waiting for the end of the asynchronous LeaderFinderReq
  int64_t end_leader_finder_req_cnt = 0;
	wait_leader_finer_req_end(leader_req_array, LEADER_FINDER_REQ_NUM, end_leader_finder_req_cnt);
  // Assert
  EXPECT_EQ(LEADER_FINDER_REQ_NUM, end_leader_finder_req_cnt);

	// Validate SvrFinderReq results
	for (int64_t idx = 0; idx < SVR_FINDER_REQ_NUM; idx++) {
		PartSvrList &svr_list = part_svr_list[idx];
		PartSvrList::SvrItemArray svr_items = svr_list.svr_items_;
		int64_t	EXPECT_START_LOG_ID = 0;
		int64_t	EXPECT_END_LOG_ID = 0;

		if (svr_req_array[idx].req_by_next_log_id_) {
			EXPECT_START_LOG_ID = svr_req_array[idx].next_log_id_;
			EXPECT_END_LOG_ID = EXPECT_START_LOG_ID + 10000;
		} else if (svr_req_array[idx].req_by_start_tstamp_) {
			EXPECT_START_LOG_ID = 0;
			EXPECT_END_LOG_ID = 65536;
		}

		int cnt = QUERY_CLOG_HISTORY_VALID_COUNT + QUERY_META_INFO_ADD_COUNT;
		EXPECT_EQ(cnt, svr_list.count());
		// Validate log range
		for (int64_t svr_idx = 0; svr_idx < cnt; svr_idx++) {
			const PartSvrList::LogIdRange &range = svr_items[svr_idx].log_ranges_[0];
			if (svr_idx < QUERY_CLOG_HISTORY_VALID_COUNT) {
				// clog history record
				EXPECT_EQ(EXPECT_START_LOG_ID, range.start_log_id_);
				EXPECT_EQ(EXPECT_END_LOG_ID, range.end_log_id_);
			} else {
				// Additional records
				EXPECT_EQ(0, range.start_log_id_);
				EXPECT_EQ(OB_INVALID_ID, range.end_log_id_);
			}
		}
	}

	// Validate LeaderFinderReq results
	ObAddr EXPECT_ADDR;
  EXPECT_ADDR.set_ip_addr("127.0.0.1", 8888);
	for (int64_t idx = 0; idx < LEADER_FINDER_REQ_NUM; idx++) {
		LeaderFindReq &req = leader_req_array[idx];
		EXPECT_TRUE(req.has_leader_);
		EXPECT_EQ(EXPECT_ADDR, req.leader_);
	}

	// destroy
	ob_free(part_svr_list);
	ob_free(svr_req_array);
	svr_finder.destroy();
	all_svr_cache.destroy();
}

// Used to test if SvrFinder can filter INACTIVE records
TEST_F(TestObLogSvrFinder, inactive_test)
{
	MockFetcherErrHandler1 err_handler;
	MockSysTableHelperDerive2 mock_systable_helper;

	// AllSvrCache init
	ObLogAllSvrCache all_svr_cache;
	EXPECT_EQ(OB_SUCCESS, all_svr_cache.init(mock_systable_helper, err_handler));

	// SvrFinder init
	ObLogSvrFinder svr_finder;
	EXPECT_EQ(OB_SUCCESS, svr_finder.init(SVR_FINDER_THREAD_NUM, err_handler,
																				all_svr_cache, mock_systable_helper));
	// Declaration of partition sever list
	PartSvrList *part_svr_list = NULL;
	generate_part_svr_list(SVR_FINDER_REQ_NUM, part_svr_list);

	// Constructing SvrFindReq, two types of requests
	// 1. logid request
	// 2. timestamp request
	SvrFindReq *svr_req_array = NULL;
	generate_svr_finder_requset(SVR_FINDER_REQ_NUM, part_svr_list, svr_req_array);

	// push request to svr_finder
	for (int64_t idx = 0; idx < SVR_FINDER_REQ_NUM; idx++) {
		EXPECT_EQ(OB_SUCCESS, svr_finder.async_svr_find_req(svr_req_array + idx));
	}

	// SvrFinder start
	EXPECT_EQ(OB_SUCCESS, svr_finder.start());

  // Wait for asynchronous SvrFinderReq to finish
  int64_t end_svr_finder_req_cnt = 0;
	wait_svr_finer_req_end(svr_req_array, SVR_FINDER_REQ_NUM, end_svr_finder_req_cnt);
  // Assert
  EXPECT_EQ(SVR_FINDER_REQ_NUM, end_svr_finder_req_cnt);

	// Validate SvrFinderReq results
  int cnt = (QUERY_CLOG_HISTORY_VALID_COUNT + QUERY_META_INFO_ADD_COUNT) / 2;
	for (int64_t idx = 0; idx < 1; idx++) {
		PartSvrList &svr_list = part_svr_list[idx];
		PartSvrList::SvrItemArray svr_items = svr_list.svr_items_;

		EXPECT_EQ(cnt, svr_list.count());
	}

	ob_free(part_svr_list);
	ob_free(svr_req_array);
	svr_finder.destroy();
	all_svr_cache.destroy();
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
  logger.set_file_name("test_ob_log_svr_finder.log", true);
  logger.set_log_level(OB_LOG_LEVEL_INFO);
  testing::InitGoogleTest(&argc, argv);
  ret = RUN_ALL_TESTS();
  return ret;
}
