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
#include <vector>
#include "share/ob_define.h"
#include "logservice/libobcdc/src/ob_map_queue_thread.h"
#include "ob_log_utils.h"

using namespace oceanbase;
using namespace common;
using namespace libobcdc;
using namespace std;

namespace oceanbase
{
namespace unittest
{
// ObMapQueue label
static const char *label = "test";
// Thread num of ObMapQueueThread
static const int THREAD_NUM = 6;

// ObMapQueue type
struct MapQueueType
{
	int64_t value_;
	uint64_t hash_val_;
	void reset(int64_t value, uint64_t hash_val)
	{
		value_ = value;
		hash_val_ = hash_val;
	}
	TO_STRING_KV(K(value_), K(hash_val_));
};
typedef MapQueueType Type;

// ObMapQueue value range
static const int64_t START_VALUE = 0;
static const int64_t END_VALUE = 1 * 100 * 1000 - 1;
static const int64_t VALUE_COUNT = END_VALUE - START_VALUE + 1;

class TestObMapQueueThread : public ::testing::Test
{
public :
	TestObMapQueueThread() {}
	virtual ~TestObMapQueueThread() {}
  virtual void SetUp() {}
  virtual void TearDown() {}
public :
  // push thread
  static const int64_t ONE_PUSH_THREAD_NUM = 1;
  static const int64_t MULTI_PUSH_THREAD_NUM = 3;
  // time limit
  static const int64_t TEST_TIME_LIMIT = 1 * _MIN_;
public:
	// generate data
	void generate_data(const int64_t count, Type *&datas);
};

void TestObMapQueueThread::generate_data(const int64_t count, Type *&datas)
{
	datas = (Type *)ob_malloc(sizeof(Type) * count, ObNewModIds::TEST);
	OB_ASSERT(NULL != datas);
	for (int64_t idx = 0; idx < count; idx++) {
		datas[idx].reset(idx, idx % THREAD_NUM);
	}
	for (int64_t idx = 0; idx < count; idx++) {
		LOG_DEBUG("data", K(datas[idx]));
	}
}

static const int MAX_THREAD_NUM = 16;
typedef common::ObMapQueueThread<MAX_THREAD_NUM> QueueThread;
// DerivedQueueThread1
// Overload handle, test the correctness of ObMapQueueThread execution
class DerivedQueueThread1 : public QueueThread
{
public:
	DerivedQueueThread1(vector<vector<Type> > &handle_result) : end_handle_count_(0),
																					        handle_result_(handle_result),
									                                            inited_(false) {}
	virtual ~DerivedQueueThread1() { destroy(); }
public:
	int init();
	void destroy();
	int start();
public:
	// Record the number of data that has been processed by the thread
	int64_t end_handle_count_ CACHE_ALIGNED;
public:
	// Implement ObMapQueueThread dummy function-handle to overload the thread handling function
  virtual int handle(void *data, const int64_t thread_index, volatile bool &stop_flag);
private:
	vector<vector<Type> > &handle_result_;
	bool inited_;
};

// DerivedQueueThread2
// Overload run, test the correctness of ObMapQueueThread execution
class DerivedQueueThread2 : public QueueThread
{
public:
	DerivedQueueThread2(vector<vector<Type> > &handle_result) : end_handle_count_(0),
																					        handle_result_(handle_result),
									                                            inited_(false) {}
	virtual ~DerivedQueueThread2() { destroy(); }
public:
	int init();
	void destroy();
	int start();
public:
	// Record the number of data that has been processed by the thread
	int64_t end_handle_count_ CACHE_ALIGNED;
public:
	// 实Overload run, test the correctness of ObMapQueueThread execution
	virtual void run(const int64_t thread_index);
private:
  static const int64_t IDLE_WAIT_TIME = 10 * 1000;
private:
	vector<vector<Type> > &handle_result_;
	bool inited_;
};

int DerivedQueueThread1::init()
{
	int ret = OB_SUCCESS;

	if (OB_UNLIKELY(inited_)) {
		LOG_ERROR("DerivedQueueThread1 init twice");
		ret = OB_INIT_TWICE;
	} else if (OB_FAIL(QueueThread::init(THREAD_NUM, label))) {
		LOG_ERROR("init QueueThread fail", K(ret), K(THREAD_NUM), K(label));
	} else {
	  EXPECT_EQ(THREAD_NUM, QueueThread::get_thread_num());
	  EXPECT_TRUE(QueueThread::is_stoped());
		end_handle_count_ = 0;
		inited_ = true;

		LOG_INFO("DerivedQueueThread1 init ok", K(ret));
	}

	return ret;
}

void DerivedQueueThread1::destroy()
{
	if (inited_) {
		QueueThread::destroy();
		EXPECT_TRUE(QueueThread::is_stoped());
		inited_ = false;

		LOG_INFO("DerivedQueueThread1 destory");
  }
}

int DerivedQueueThread1::start()
{
	int ret = OB_SUCCESS;

	if (OB_UNLIKELY(!inited_)) {
		LOG_ERROR("DerivedQueueThread1 not init");
		ret = OB_NOT_INIT;
	} else if (OB_FAIL(QueueThread::start())) {
		LOG_ERROR("DerivedQueueThread1 start error", K(ret));
	} else {
	  LOG_INFO("DerivedQueueThread1 start ok");
	}
	EXPECT_FALSE(QueueThread::is_stoped());

	return ret;
}

int DerivedQueueThread1::handle(void *data, const int64_t thread_index, volatile bool &stop_flag)
{
	int ret = OB_SUCCESS;
	stop_flag = stop_flag;
	Type *task = NULL;

	if (OB_UNLIKELY(!inited_)) {
		LOG_ERROR("DerivedQueueThread1 not init");
		ret = OB_NOT_INIT;
  } else if (OB_ISNULL(data)
			       || OB_UNLIKELY(thread_index < 0)
						 || OB_UNLIKELY(thread_index >= get_thread_num())) {
		LOG_ERROR("invalid argument", K(thread_index), K(get_thread_num()));
		ret = OB_ERR_UNEXPECTED;
	} else if (OB_ISNULL(task = static_cast<Type *>(data))) {
		ret = OB_INVALID_ARGUMENT;
		LOG_ERROR("invalid argument", K(ret), KP(task), K(thread_index));
  } else {
		LOG_DEBUG("DerivedQueueThread1 handle", K(ret), K(*task), K(thread_index));

		handle_result_[thread_index].push_back(*task);
		ATOMIC_INC(&end_handle_count_);
	}

	return ret;
}

int DerivedQueueThread2::init()
{
	int ret = OB_SUCCESS;

	if (OB_UNLIKELY(inited_)) {
		LOG_ERROR("DerivedQueueThread2 init twice");
		ret = OB_INIT_TWICE;
	} else if (OB_FAIL(QueueThread::init(THREAD_NUM, label))) {
		LOG_ERROR("init QueueThread fail", K(ret), K(THREAD_NUM), K(label));
	} else {
	  EXPECT_EQ(THREAD_NUM, QueueThread::get_thread_num());
	  EXPECT_TRUE(QueueThread::is_stoped());
		end_handle_count_ = 0;
		inited_ = true;

		LOG_INFO("DerivedQueueThread2 init ok", K(ret));
	}

	return ret;
}

void DerivedQueueThread2::destroy()
{
	if (inited_) {
		QueueThread::destroy();
		EXPECT_TRUE(QueueThread::is_stoped());
		inited_ = false;

		LOG_INFO("DerivedQueueThread2 destory");
  }
}

int DerivedQueueThread2::start()
{
	int ret = OB_SUCCESS;

	if (OB_UNLIKELY(!inited_)) {
		LOG_ERROR("DerivedQueueThread2 not init");
		ret = OB_NOT_INIT;
	} else if (OB_FAIL(QueueThread::start())) {
		LOG_ERROR("DerivedQueueThread2 start error", K(ret));
	} else {
	  LOG_INFO("DerivedQueueThread2 start ok");
	}
	EXPECT_FALSE(QueueThread::is_stoped());

	return ret;
}

void DerivedQueueThread2::run(const int64_t thread_index)
{
	int ret = OB_SUCCESS;

	if (OB_UNLIKELY(!inited_)) {
		LOG_ERROR("DerivedQueueThread2 not init");
		ret = OB_NOT_INIT;
	} else if (OB_UNLIKELY(thread_index < 0) || OB_UNLIKELY(thread_index >= get_thread_num())) {
		LOG_ERROR("invalid argument", K(thread_index), K(get_thread_num()));
		ret = OB_ERR_UNEXPECTED;
	} else {
		LOG_INFO("DerivedQueueThread2 run start", K(thread_index));

		while (!stop_flag_ && OB_SUCCESS == ret) {
			void *data = NULL;
			Type *task = NULL;

			if (OB_FAIL(pop(thread_index, data))) {
				if (OB_EAGAIN == ret) {
					// empty
					ret = OB_SUCCESS;
					cond_timedwait(thread_index, IDLE_WAIT_TIME);
					LOG_DEBUG("DerivedQueueThread2 pop empty");
				} else {
				  LOG_ERROR("DerivedQueueThread2 pop data error", K(ret));
				}
			}	else if (OB_ISNULL(data) || OB_ISNULL(task = static_cast<Type *>(data))) {
				LOG_ERROR("invalid argument", KPC(task), K(thread_index));
				ret = OB_ERR_UNEXPECTED;
			} else {
		    LOG_DEBUG("DerivedQueueThread2 handle", K(ret), K(*task), K(thread_index));

				handle_result_[thread_index].push_back(*task);
		    ATOMIC_INC(&end_handle_count_);
			}
		}
	}
}

class TestPushWorker : public libobcdc::Runnable
{
public:
	enum State
	{
		IDLE, // empty
		REQ,  // pushing
		DONE  // push done
	};
	// Identifies the current thread status
	State state_;
	// thread index
	int64_t thread_idx_;
	// thread count
	int64_t thread_count_;
	// push data
	Type *datas_;
	// value interval
	int64_t interval_;
	// ObMapQueueThread
	QueueThread *host_;
	// record thread map_queue push count
	int64_t push_count_;

	void reset(const int64_t thread_idx,
			       const int64_t push_thread_num,
			       Type *datas,
						 QueueThread *host)
	{
		state_ = TestPushWorker::REQ;
		thread_idx_ = thread_idx;
		thread_count_ = push_thread_num;
		datas_ = datas;
	  interval_ = VALUE_COUNT / push_thread_num;
		host_ = host;
		push_count_ = 0;
	}

	void start()
	{
		// create threads
		create();
		LOG_INFO("TestPushWorker start", "push worker thread", "create OB_SUCCESS");
	}

	void stop()
	{
		join();
		LOG_INFO("TestPushWorker join", "push worker thread", "join OB_SUCCESS");
	}

	virtual int routine()
	{
		int64_t start = thread_idx_ * interval_;
		int64_t end = (thread_count_ - 1 != thread_idx_) ? start + interval_ - 1 : END_VALUE;
		LOG_INFO("TestPushWorker", K(start), K(end));

		int64_t idx = 0;
		for (idx = start; idx <= end; idx++) {
			Type *type = datas_ + idx;
			EXPECT_EQ(OB_SUCCESS, host_->push(type, type->hash_val_));
			push_count_++;
			LOG_DEBUG("TestPushWorker", K(idx), KPC(type));
		}

		if (end + 1 == idx) {
			state_ = DONE;
		}

		return OB_SUCCESS;
	}
};

const int64_t MAX_PUSH_WORKER_NUM = 32;
TestPushWorker push_workers[MAX_PUSH_WORKER_NUM];
// start push worker
static void start_push_worker(const int64_t push_thread_num,
															Type *datas,
															QueueThread *host)
{
	 //push thread
	for (int64_t idx = 0, cnt = push_thread_num; idx < cnt; ++idx) {
		TestPushWorker &w = push_workers[idx];
		w.reset(idx, push_thread_num, datas, host);
		w.start();
	}
}

// stop push worker
static void stop_push_worker(const int64_t push_thread_num)
{
	// push thread join
	for (int64_t idx = 0, cnt = push_thread_num; idx < cnt; ++idx) {
		TestPushWorker &w = push_workers[idx];
		w.stop();
	}
}

////////////////////// Basic function tests //////////////////////////////////////////
// ObMapQueueThread init, destory, start, stop, is_stoped, get_thread_num
// overload ObMapQueueThread-handle
TEST_F(TestObMapQueueThread, DerivedQueueThread1)
{
	// genereate data
	Type *datas = NULL;
	generate_data(VALUE_COUNT, datas);
	OB_ASSERT(NULL != datas);

	// savd result of DerivedQueueThread1 handle
	vector<vector<Type> > handle_result;
	for (int64_t idx = 0; idx < THREAD_NUM; idx++) {
		vector<Type> res;
		res.clear();
		handle_result.push_back(res);
	}

	// init and start worker thread
  DerivedQueueThread1 derived1(handle_result);
	EXPECT_EQ(OB_SUCCESS, derived1.init());
	EXPECT_EQ(OB_SUCCESS, derived1.start());

	// start push thread
	int64_t PUSH_THREAD_NUM = ONE_PUSH_THREAD_NUM;
  start_push_worker(PUSH_THREAD_NUM, datas, &derived1);

	// Check handle completion and verify that the result totals and fields are correct
	int64_t end_handle_count = 0;
	int64_t start_test_tstamp = get_timestamp();
	while (((get_timestamp() - start_test_tstamp) < TEST_TIME_LIMIT)
					 && (end_handle_count < VALUE_COUNT)) {
		end_handle_count = ATOMIC_LOAD(&derived1.end_handle_count_);
	  usleep(static_cast<__useconds_t>(1000));
		LOG_DEBUG("handle verify", K(end_handle_count));
	}
	EXPECT_EQ(VALUE_COUNT, end_handle_count);

	int64_t handle_result_count = 0;
	for (int64_t idx = 0; idx < THREAD_NUM; idx++) {
		int64_t cnt = handle_result[idx].size();
		LOG_INFO("DerivedQueueThread1 vector count", K(idx), K(cnt));
		handle_result_count += cnt;
		for (int64_t i = 0; i < cnt; i++) {
			Type t = handle_result[idx][i];
			LOG_DEBUG("type", K(t));
			EXPECT_TRUE(idx == (t.value_ % THREAD_NUM));
			EXPECT_TRUE(idx == t.hash_val_);
		}
	}
	EXPECT_EQ(VALUE_COUNT, handle_result_count);

	stop_push_worker(PUSH_THREAD_NUM);
	derived1.destroy();
	ob_free(datas);
}

// ObMapQueueThread run, pop, cond_timewait
// overload ObMapQueueThread-run
TEST_F(TestObMapQueueThread, DerivedQueueThread2)
{
	// genereate data
	Type *datas = NULL;
	generate_data(VALUE_COUNT, datas);
	OB_ASSERT(NULL != datas);

	// save result of DerivedQueueThread2 handle
	vector<vector<Type> > handle_result;
	for (int64_t idx = 0; idx < THREAD_NUM; idx++) {
		vector<Type> res;
		res.clear();
		handle_result.push_back(res);
	}

	// init and start worker thread
  DerivedQueueThread2 derived2(handle_result);
	EXPECT_EQ(OB_SUCCESS, derived2.init());
	EXPECT_EQ(OB_SUCCESS, derived2.start());

	// start push thread
	int64_t PUSH_THREAD_NUM = ONE_PUSH_THREAD_NUM;
  start_push_worker(PUSH_THREAD_NUM, datas, &derived2);

	// Check handle completion and verify that the result totals and fields are correct
	int64_t end_handle_count = 0;
	int64_t start_test_tstamp = get_timestamp();
	while (((get_timestamp() - start_test_tstamp) < TEST_TIME_LIMIT)
					 && (end_handle_count < VALUE_COUNT)) {
		end_handle_count = ATOMIC_LOAD(&derived2.end_handle_count_);
	  usleep(static_cast<__useconds_t>(1000));
		LOG_DEBUG("handle verify", K(end_handle_count));
	}
	EXPECT_EQ(VALUE_COUNT, end_handle_count);

	int64_t handle_result_count = 0;
	for (int64_t idx = 0; idx < THREAD_NUM; idx++) {
		int64_t cnt = handle_result[idx].size();
		LOG_INFO("DerivedQueueThread2 vector count", K(idx), K(cnt));
		handle_result_count += cnt;
		for (int64_t i = 0; i < cnt; i++) {
			Type t = handle_result[idx][i];
			LOG_DEBUG("type", K(t));
			EXPECT_TRUE(idx == (t.value_ % THREAD_NUM));
			EXPECT_TRUE(idx == t.hash_val_);
		}
	}
	EXPECT_EQ(VALUE_COUNT, handle_result_count);

	stop_push_worker(PUSH_THREAD_NUM);
	derived2.destroy();
	ob_free(datas);
}

////////////////////////Boundary condition testing//////////////////////////////////////////
// ObMapQueue init fail
TEST_F(TestObMapQueueThread, init_failed)
{
	QueueThread queue_thread;
	EXPECT_EQ(OB_SUCCESS, queue_thread.init(THREAD_NUM, label));
	EXPECT_EQ(OB_INIT_TWICE, queue_thread.init(THREAD_NUM, label));
	queue_thread.destroy();

	// MAX_THREAD_NUM = 16
	const int64_t INVALID_THREAD_NUM1 = 0;
	const int64_t INVALID_THREAD_NUM2 = 17;
	EXPECT_EQ(OB_INVALID_ARGUMENT, queue_thread.init(INVALID_THREAD_NUM1, label));
	EXPECT_EQ(OB_INVALID_ARGUMENT, queue_thread.init(INVALID_THREAD_NUM2, label));
	EXPECT_EQ(OB_SUCCESS, queue_thread.init(THREAD_NUM, label));
	queue_thread.destroy();
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
	logger.set_file_name("test_ob_map_queue_thread.log", true);
	logger.set_log_level(OB_LOG_LEVEL_INFO);
	testing::InitGoogleTest(&argc, argv);
  ret = RUN_ALL_TESTS();
  return ret;
}
