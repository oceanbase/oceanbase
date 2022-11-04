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
#include "logservice/libobcdc/src/ob_map_queue_thread.h"
#include "ob_log_utils.h"
#define private public
#include "test_ob_log_fetcher_common_utils.h"
#include "logservice/libobcdc/src/ob_log_timer.h"

using namespace oceanbase;
using namespace common;
using namespace libobcdc;

namespace oceanbase
{
namespace unittest
{
static const int MAX_THREAD_NUM = 16;
typedef common::ObMapQueueThread<MAX_THREAD_NUM> QueueThread;

// Timed task implementation
class PushMapTimerTask : public ObLogTimerTask
{
public:
	PushMapTimerTask() : host_(NULL), start_time_(0), end_time_(0), process_count_(NULL)
	                     {}
	virtual ~PushMapTimerTask() {}

	void reset()
	{
		host_ = NULL;
		start_time_ = 0;
		end_time_ = 0;
		process_count_ = NULL;
	}

	void reset(int64_t *&process_count, QueueThread *host)
	{
		reset();
		process_count_ = process_count;
		host_ = host;
	}

public:
	virtual void process_timer_task() override
	{
    EXPECT_EQ(OB_SUCCESS, host_->push(this, static_cast<uint64_t>(get_timestamp())));
	  end_time_ = get_timestamp();
	  (*process_count_)++;
	}
private:
  QueueThread *host_;
	int64_t start_time_;
	int64_t end_time_;
	// Record the number of successfully executed timed tasks
	int64_t *process_count_;
private:
  DISALLOW_COPY_AND_ASSIGN(PushMapTimerTask);
};
typedef PushMapTimerTask Type;
// Timer task count
static const int64_t TASK_COUNT = 1000;

class TestObLogTimer: public ::testing::Test
{
public :
  virtual void SetUp() {}
  virtual void TearDown() {}
public :
  static constexpr const int64_t TEST_TIME_LIMIT = 10 * _MIN_;
  // ObMapQueue mod_id
  static constexpr const char *MOD_ID = "1";
  // ObMapQueueThread线程个数
  static const int THREAD_NUM = 6;
  // max task count
	static const int64_t MAX_TASK_COUNT = 10 * 1000;
public:
	// Generate timed task data
  void generate_data(const int64_t count, QueueThread *host, int64_t *&process_count, Type *&datas);
};

void TestObLogTimer::generate_data(const int64_t count,
														 QueueThread *host,
																	 int64_t *&process_count,
																	 Type *&datas)
{
	datas = (Type *)ob_malloc(sizeof(Type) * count);
	OB_ASSERT(NULL != datas);
	for (uint64_t idx = 0; idx < count; idx++) {
		new (datas + idx) Type();
		datas[idx].reset(process_count, host);
	}
}

//////////////////////Basic function tests//////////////////////////////////////////
TEST_F(TestObLogTimer, timer)
{
	// ObMapQueueThread init
	QueueThread host;
	EXPECT_EQ(OB_SUCCESS, host.init(THREAD_NUM, MOD_ID));

	// Number of timer tasks handled
	int64_t process_timer_task_count = 0;
	int64_t *ptr = &process_timer_task_count;

	// Generate timed tasks
	Type *datas = NULL;
	generate_data(TASK_COUNT, &host, ptr, datas);
	OB_ASSERT(NULL != datas);

  // ObLogFixedTimer init
	ObLogFixedTimer timer;
	MockFetcherErrHandler1 err_handle;
	EXPECT_EQ(OB_SUCCESS, timer.init(err_handle, MAX_TASK_COUNT));

	// Insert timed tasks
	int64_t start_push_time = 0;
	int64_t end_push_time = 0;
	start_push_time = get_timestamp();
	for (int64_t idx = 0; idx < TASK_COUNT; idx++) {
		// Giving start time
		datas[idx].start_time_ = get_timestamp();
		EXPECT_EQ(OB_SUCCESS, timer.schedule(&datas[idx]));
	}
	end_push_time = get_timestamp();
	int64_t push_take_time = end_push_time - start_push_time;
	EXPECT_EQ(TASK_COUNT, timer.task_queue_.get_total());
	LOG_INFO("timer push", K(push_take_time));

	// ObLogTimer start
	EXPECT_EQ(OB_SUCCESS, timer.start());

	int64_t start_test_tstamp = get_timestamp();
  while (((get_timestamp() - start_test_tstamp) < TEST_TIME_LIMIT)
		       && (process_timer_task_count < TASK_COUNT)) {
	}
	LOG_INFO("process", K(process_timer_task_count));

	int64_t min_interval = 1 * _SEC_;
	int64_t max_interval = 0;

	for (int64_t idx = 0; idx < TASK_COUNT; idx++) {
		int64_t inv = datas[idx].end_time_ - datas[idx].start_time_;
		if (inv < min_interval) {
			min_interval = inv;
		}

		if (inv > max_interval) {
			max_interval = inv;
		}
	}
	LOG_INFO("interval", K(min_interval), K(max_interval));

	host.destroy();
	ob_free(datas);
	timer.destroy();
}

////////////////////////Boundary condition testing//////////////////////////////////////////
// ObLogTimer init fail
TEST_F(TestObLogTimer, init_failed)
{
  ObLogFixedTimer timer;
  MockFetcherErrHandler1 err_handle;
  EXPECT_EQ(OB_SUCCESS, timer.init(err_handle, MAX_TASK_COUNT));
  EXPECT_EQ(OB_INIT_TWICE, timer.init(err_handle, MAX_TASK_COUNT));
  timer.destroy();
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
  logger.set_file_name("test_ob_log_timer.log", true);
  logger.set_log_level(OB_LOG_LEVEL_INFO);
  testing::InitGoogleTest(&argc, argv);
  ret = RUN_ALL_TESTS();
  return ret;
}
