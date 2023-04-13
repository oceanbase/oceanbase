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
#include "logservice/libobcdc/src/ob_map_queue.h"
#include "ob_log_utils.h"

using namespace oceanbase;
using namespace common;
using namespace libobcdc;

namespace oceanbase
{
namespace unittest
{
class TestObMapQueue : public ::testing::Test
{
public :
  virtual void SetUp() {}
  virtual void TearDown() {}
public :
  // ObMapQueue label
  static constexpr const char *LABEL = "TestObMapQueue";
  // push thread
  static const int64_t ONE_PUSH_THREAD_NUM = 1;
  static const int64_t MULTI_PUSH_THREAD_NUM = 3;
  // pop thread
  static const int64_t ONE_POP_THREAD_NUM = 1;
  static const int64_t MULTI_POP_THREAD_NUM = 5;

  static const int64_t TEST_TIME_LIMIT = 10 * _MIN_;
};

// ObMapQueue type
typedef int64_t Type;
// push ObMapQueue value
static const int64_t START_VALUE = 0;
static const int64_t END_VALUE = 1 * 1000 * 1000 - 1;
static const int64_t VALUE_COUNT = END_VALUE - START_VALUE + 1;

class TestPushWorker : public libobcdc::Runnable
{
public:
  enum State
  {
	  IDLE, //
		REQ,  // pushing
		DONE  // push DONE
	};
	// Identifies the current thread status
	State state_;

	// thread index
	int64_t thread_idx_;
	// thread count
	int64_t thread_count_;
	// ObMapQueue
	ObMapQueue<Type> *map_queue_;
	// record map_queue push count
	int64_t push_count_;
  // value interval
	int64_t interval_;

	virtual int routine()
	{
		int64_t start = thread_idx_ * interval_;
		int64_t end = (thread_count_ - 1 != thread_idx_) ? start + interval_ - 1 : END_VALUE;
		LOG_INFO("TestPushWorker", K(start), K(end));

		int64_t val = start;
		while (val <= end) {
			EXPECT_EQ(OB_SUCCESS, map_queue_->push(val++));
			push_count_++;
		}

		if (end + 1 == val) {
			state_ = DONE;
		}

    return OB_SUCCESS;
	}
};

class TestPopWorker: public libobcdc::Runnable
{
public:
	// thread index
	int64_t thread_idx_;
	// ObMapQueue
	ObMapQueue<Type> *map_queue_;
	// record thread map_queue pop count
	int64_t pop_count_ CACHE_ALIGNED;
	// record poped count for all threads
	int64_t *end_pop_count_ CACHE_ALIGNED;
  // 保存pop出来的数据
	Type *array_;

	virtual int routine()
	{
		int ret = OB_SUCCESS;

		while (OB_SUCC(ret)) {
		  Type val;
			while (OB_SUCC(map_queue_->pop(val))) {
				if (val >= START_VALUE && val <= END_VALUE) {
					if (0 == array_[val]) {
						array_[val] = val;
						ATOMIC_INC(&pop_count_);
					}
				}
			}

		  if (OB_EAGAIN == ret) {
			ret = OB_SUCCESS;
		  }
			if (ATOMIC_LOAD(end_pop_count_) == VALUE_COUNT) {
				break;
			}
		}

    return ret;
	}
};


////////////////////// Basic function tests //////////////////////////////////////////
// ObMapQueue init
TEST_F(TestObMapQueue, init)
{
	ObMapQueue<Type> map_queue;
	EXPECT_EQ(OB_SUCCESS, map_queue.init(LABEL));
	EXPECT_TRUE(map_queue.is_inited());

	map_queue.destroy();
	EXPECT_FALSE(map_queue.is_inited());
}

// Test scenarios.
// 1. single-threaded push - single-threaded pop
// 2. single-threaded push - multi-threaded pop
// 3. multi-threaded push - single-threaded pop
// 4. multi-threaded push - multi-threaded pop
TEST_F(TestObMapQueue, push_pop_test)
{
	ObMapQueue<Type> map_queue;
	EXPECT_EQ(OB_SUCCESS, map_queue.init(LABEL));
	EXPECT_TRUE(map_queue.is_inited());

	// malloc array
	Type *array = (Type *)ob_malloc(sizeof(Type) * VALUE_COUNT, ObNewModIds::TEST);
	OB_ASSERT(NULL != array);

	for (int64_t test_type = 0, test_cnt = 4; test_type < test_cnt; ++test_type) {
		memset(array, 0, sizeof(Type) * VALUE_COUNT);
		int64_t PUSH_THREAD_NUM = 0;
		int64_t POP_THREAD_NUM = 0;
		int64_t end_push_count = 0;
		int64_t end_pop_count = 0;

		switch (test_type) {
			// single-threaded push - single-threaded pop
			case 0:
				PUSH_THREAD_NUM = ONE_PUSH_THREAD_NUM;
				POP_THREAD_NUM = ONE_POP_THREAD_NUM;
				break;
			// single-threaded push - multi-threaded pop
			case 1:
				PUSH_THREAD_NUM = ONE_PUSH_THREAD_NUM;
				POP_THREAD_NUM = MULTI_POP_THREAD_NUM;
				break;
			// multi-threaded push - single-threaded pop
			case 2:
				PUSH_THREAD_NUM = MULTI_PUSH_THREAD_NUM;
				POP_THREAD_NUM = ONE_POP_THREAD_NUM;
				break;
			// multi-threaded push - multi-threaded pop
			case 3:
				PUSH_THREAD_NUM = MULTI_PUSH_THREAD_NUM;
				POP_THREAD_NUM = MULTI_POP_THREAD_NUM;
				break;
			default:
				break;
		}
		LOG_INFO("push_pop_test", K(test_type), K(PUSH_THREAD_NUM), K(POP_THREAD_NUM));

		// push thread
		TestPushWorker push_workers[PUSH_THREAD_NUM];
		const int64_t INTERVAL = VALUE_COUNT / PUSH_THREAD_NUM;
		for (int64_t idx = 0, cnt = PUSH_THREAD_NUM; idx < cnt; ++idx) {
			TestPushWorker &w = push_workers[idx];
			// assign value
			w.state_ = TestPushWorker::REQ;
			w.thread_idx_ = idx;
			w.thread_count_ = PUSH_THREAD_NUM;
			w.map_queue_ = &map_queue;
			w.push_count_ = 0;
			w.interval_ = INTERVAL;
			// create threads
			w.create();
			LOG_INFO("push_pop_test", "push thread", "create OB_SUCCESS");
		}

		// pop thread
		TestPopWorker pop_workers[POP_THREAD_NUM];
		for (int64_t idx = 0, cnt = POP_THREAD_NUM; idx < cnt; ++idx) {
			TestPopWorker &w = pop_workers[idx];
			// addign value
			w.map_queue_ = &map_queue;
			w.array_ = array;
			w.pop_count_ = 0;
			w.end_pop_count_ = &end_pop_count;
			// create threads
			w.create();
			LOG_INFO("push_pop_test", "pop thread", "create OB_SUCCESS");
		}

		// Verify the correctness of the push: verify the total number of pushes into the ObMapQueue-Type
		int64_t start_test_tstamp = get_timestamp();
		while (((get_timestamp() - start_test_tstamp) < TEST_TIME_LIMIT)
						 && (end_push_count < VALUE_COUNT)) {
			for (int64_t idx = 0, cnt = PUSH_THREAD_NUM; idx < cnt; ++idx) {
				TestPushWorker &w = push_workers[idx];
					if (TestPushWorker::DONE == w.state_) {
						end_push_count += w.push_count_;
						w.state_ = TestPushWorker::IDLE;
					}
			}
		}
		EXPECT_EQ(VALUE_COUNT, end_push_count);

		// Verify that the pop is correct:
		// 1. verify the total number of -Types popped from ObMapQueue
		// 2. Correctness of the fields
		start_test_tstamp = get_timestamp();
		while (((get_timestamp() - start_test_tstamp) < TEST_TIME_LIMIT)
						 && (end_pop_count < VALUE_COUNT)) {
			for (int64_t idx = 0, cnt = POP_THREAD_NUM; idx < cnt; ++idx) {
				TestPopWorker &w = pop_workers[idx];

				int64_t pop_cnt = ATOMIC_LOAD(&w.pop_count_);
				while (!ATOMIC_BCAS(&w.pop_count_, pop_cnt, 0)) {
					pop_cnt = ATOMIC_LOAD(&w.pop_count_);
				}

				end_pop_count += pop_cnt;
				//LOG_DEBUG("pop verify", K(idx), K(pop_cnt), K(end_pop_count));
				LOG_INFO("pop verify", K(idx), K(pop_cnt), K(end_pop_count));
			}
		}
		EXPECT_EQ(VALUE_COUNT, end_pop_count);

		int64_t correct_field = 0;
		for (int64_t idx = 0, cnt = VALUE_COUNT; idx < cnt; ++idx) {
			if (idx == array[idx]) {
				correct_field++;
			}
		}
		EXPECT_EQ(VALUE_COUNT, correct_field);

		// push thread join
		for (int64_t idx = 0, cnt = PUSH_THREAD_NUM; idx < cnt; ++idx) {
			TestPushWorker &w = push_workers[idx];
			w.join();
			LOG_INFO("push_pop_test", "push thread", "join OB_SUCCESS");
		}

		// pop thread join
		for (int64_t idx = 0, cnt = POP_THREAD_NUM; idx < cnt; ++idx) {
			TestPopWorker &w = pop_workers[idx];
			w.join();
			LOG_INFO("push_pop_test", "pop thread", "join OB_SUCCESS");
		}

		EXPECT_EQ(OB_SUCCESS, map_queue.reset());
	}

	// free array
	ob_free(array);
	map_queue.destroy();
	EXPECT_FALSE(map_queue.is_inited());
}

// 1. push performance test: push data with 10 threads
// 2. pop performance test: pop data with 10 threads
TEST_F(TestObMapQueue, DISABLED_performance)
{
	int64_t start_test_tstamp = 0;
	int64_t end_test_tstamp = 0;

	ObMapQueue<Type> map_queue;
	EXPECT_EQ(OB_SUCCESS, map_queue.init(LABEL));

	// push
	int64_t PUSH_THREAD_NUM = 10;
  const int64_t INTERVAL = VALUE_COUNT / PUSH_THREAD_NUM;
	int64_t end_push_count = 0;
	TestPushWorker push_workers[PUSH_THREAD_NUM];

	start_test_tstamp = get_timestamp();
	for (int64_t idx = 0, cnt = PUSH_THREAD_NUM; idx < cnt; ++idx) {
		TestPushWorker &w = push_workers[idx];
		w.state_ = TestPushWorker::REQ;
		w.thread_idx_ = idx;
		w.thread_count_ = PUSH_THREAD_NUM;
		w.map_queue_ = &map_queue;
		w.push_count_ = 0;
		w.interval_ = INTERVAL;
		w.create();
		LOG_INFO("push_performance", "push thread", "create OB_SUCCESS");
	}
	// Detect the end of push in all threads
  while (((get_timestamp() - start_test_tstamp) < TEST_TIME_LIMIT)
           && (end_push_count < VALUE_COUNT)) {
		for (int64_t idx = 0, cnt = PUSH_THREAD_NUM; idx < cnt; ++idx) {
		  TestPushWorker &w = push_workers[idx];
			if (TestPushWorker::DONE == w.state_) {
					end_push_count += w.push_count_;
					w.state_ = TestPushWorker::IDLE;
			}
		}
  }
  EXPECT_EQ(VALUE_COUNT, end_push_count);
	end_test_tstamp = get_timestamp();

	double push_time = static_cast<double>(end_test_tstamp - start_test_tstamp) * 1.0 / 1000000;
	double push_cnt_per_second = static_cast<double>(VALUE_COUNT) * 1.0 / (push_time);
	LOG_INFO("push_performance", K(end_push_count), K(push_time), "push count/s", push_cnt_per_second);

	// pop
	int64_t POP_THREAD_NUM = 10;
	int64_t end_pop_count = 0;
	TestPopWorker pop_workers[POP_THREAD_NUM];

	// malloc array
	Type *array = (Type *)ob_malloc(sizeof(Type) * VALUE_COUNT, ObNewModIds::TEST);
	OB_ASSERT(NULL != array);
	memset(array, 0, sizeof(Type) * VALUE_COUNT);

	start_test_tstamp = get_timestamp();
	for (int64_t idx = 0, cnt = POP_THREAD_NUM; idx < cnt; ++idx) {
		TestPopWorker &w = pop_workers[idx];
		w.map_queue_ = &map_queue;
		w.array_ = array;
		w.pop_count_ = 0;
		w.end_pop_count_ = &end_pop_count;
		w.create();
		LOG_INFO("pop_performance", "pop thread", "create OB_SUCCESS");
	}

	while (((get_timestamp() - start_test_tstamp) < TEST_TIME_LIMIT)
					 && (end_pop_count < VALUE_COUNT)) {
		for (int64_t idx = 0, cnt = POP_THREAD_NUM; idx < cnt; ++idx) {
			TestPopWorker &w = pop_workers[idx];

			int64_t pop_cnt = ATOMIC_LOAD(&w.pop_count_);
			while (!ATOMIC_BCAS(&w.pop_count_, pop_cnt, 0)) {
				pop_cnt = ATOMIC_LOAD(&w.pop_count_);
			}

			end_pop_count += pop_cnt;
			LOG_DEBUG("pop verify", K(idx), K(pop_cnt), K(end_pop_count));
		}
	}
	EXPECT_EQ(VALUE_COUNT, end_pop_count);
	end_test_tstamp = get_timestamp();

	double pop_time = static_cast<double>(end_test_tstamp - start_test_tstamp) * 1.0 / 1000000;
	double pop_cnt_per_second = static_cast<double>(VALUE_COUNT) * 1.0 / (pop_time);
	LOG_INFO("pop_performance", K(end_pop_count), K(pop_time), "pop count/s", pop_cnt_per_second);

	// push thread join
	for (int64_t idx = 0, cnt = PUSH_THREAD_NUM; idx < cnt; ++idx) {
		TestPushWorker &w = push_workers[idx];
		w.join();
		LOG_INFO("performance", "push thread", "join OB_SUCCESS");
	}

	// pop thread join
	for (int64_t idx = 0, cnt = POP_THREAD_NUM; idx < cnt; ++idx) {
		TestPopWorker &w = pop_workers[idx];
		w.join();
		LOG_INFO("performance", "pop thread", "join OB_SUCCESS");
	}

	ob_free(array);
	map_queue.destroy();
}

//////////////////////// Boundary condition testing //////////////////////////////////////////
// ObMapQueue init fail
TEST_F(TestObMapQueue, init_failed)
{
	ObMapQueue<Type> map_queue;
	EXPECT_EQ(OB_SUCCESS, map_queue.init(LABEL));
	EXPECT_EQ(OB_INIT_TWICE, map_queue.init(LABEL));
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
  logger.set_file_name("test_ob_map_queue.log", true);
  logger.set_log_level(OB_LOG_LEVEL_INFO);
  testing::InitGoogleTest(&argc, argv);
  ret = RUN_ALL_TESTS();
  return ret;
}
