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

#include "storage/tx/ob_time_wheel.h"
#include <gtest/gtest.h>
#include "common/ob_clock_generator.h"
#include "share/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "lib/random/ob_random.h"

#define BASE_TEST
#define CURRENTCY_TEST
#define EDGE_TEST

#define TEST_TW_CREATE_INIT_DES
#define TEST_TASK_CREATE_INIT_DES
#define TEST_TW_SCHEDULE_RUN
#define TEST_TW_SCHEDULE_RUN_CANCEL
#define TEST_TW_TASK_RUN_PRECISION

#define TEST_TW_HIGHCURRENCY_SCHEDULE
#define TEST_TW_HIGHCURRENCY_SCHEDULE_RUN
#define TEST_TW_HIGHCURRENCY_SCHEDULE_CANCEL
//#define TEST_TW_HIGHCURRENCY_SCHEDULE_CANCEL_RANDOM
#define TEST_TW_LOCK_AVAILABLITY

#define TEST_TIMEWHEELBASE_NO_INIT
#define TEST_TIMEWHEEL_NO_INIT
#define TEST_TIMEWHEEL_RUNNING
#define TEST_TIMEWHEEL_INVALID_INIT
#define TEST_TIMEWHEEL_REPEAT_INIT
#define TEST_TIMEWHEEL_REPEAT_DES
#define TEST_TIMEWHEEL_SCHEDULE_AFTER_DES
#define TEST_TIMEWHEEL_INVALID_TASK_SCHEDULE
#define TEST_TIMEWHEEL_REPEAT_SCHEDULE
#define TEST_TIMEWHEEL_REPEAT_CANCEL
#define TEST_TIMEWHEEL_REPEAT_SCHEDULE_CANCEL
#define TEST_TIMEWHEEL_SCHEDULE_SELF
#define TEST_TIMEWHEEL_CANCEL_RUNOVER_TASK

namespace oceanbase
{
using namespace common;

namespace unittest
{

struct NumTask{
  // thread count
  static const int64_t THREAD_NUM = 15;

  // high concurrency test task count
  static const int64_t MAX_NUM_TASK = 1000000;
  // static const int64_t HIGH_CURRENCY_NUM_TASK = 100000;
  static const int64_t HIGH_CURRENCY_NUM_TASK = 100000;

  // lock availablity test paras
  static const int64_t HIGH_CURRENCY_LOCK_NUM_TASK = 1000;
  static const int64_t HIGH_CURRENCY_LOCK_TEST_COUNT = 10;

  // inc_num for task hash value
  static int64_t inc_num_;

  // time wheel thread num
  static const int64_t TW_THREAD_NUM = 6;
};
int64_t NumTask::inc_num_ = 0;

class TestObTimeWheel : public ::testing::Test
{
public :
  virtual void SetUP(){}
  virtual  void TearDown(){}
};

class ObSampleTimer : public ObTimeWheel{};

class ObSampleTask : public ObTimeWheelTask
{
public :
  ObSampleTask() : run_over_(false), inc_num_(++NumTask::inc_num_) {}
  ~ObSampleTask(){}
  void runTimerTask();
  bool is_run_over() const;
  void reset();
  static int64_t get_total_run_count();
  static void clear_total_run_count();
  uint64_t hash() const { return inc_num_; }
public :
  static int64_t task_run_count_num_;
private :
  bool run_over_;
  int64_t inc_num_;
};

int64_t ObSampleTask::task_run_count_num_ = 0;

void ObSampleTask::runTimerTask()
{
  ATOMIC_FAA(&task_run_count_num_, 1);
  run_over_ = true;
}

bool ObSampleTask::is_run_over() const
{
  return run_over_;
}

void ObSampleTask::reset()
{
  run_over_ = false;
}

void ObSampleTask::clear_total_run_count()
{
  task_run_count_num_ = 0;
}

int64_t ObSampleTask::get_total_run_count()
{
  return task_run_count_num_;
}

class ObSampleTask2 : public ObTimeWheelTask
{
public :
  ObSampleTask2() : time_wheel_(NULL), retry_time_(0), inc_num_(++NumTask::inc_num_) { }
  ~ObSampleTask2(){};
  void set_time_wheel(ObTimeWheel *time_wheel);
  void runTimerTask();
  int64_t get_retry_time() const;
  uint64_t hash() const { return inc_num_; }
private :
  ObTimeWheel *time_wheel_;
  int64_t retry_time_;
  int64_t inc_num_;
};

void ObSampleTask2::set_time_wheel(ObTimeWheel *time_wheel)
{
  if (NULL != time_wheel) {
    time_wheel_ = time_wheel;
  } else {
    TRANS_LOG(INFO, "",  "input paras time_wheel = NULL", time_wheel);
  }
}

void ObSampleTask2::runTimerTask()
{
  if (retry_time_ < 100) {
    retry_time_++;
    EXPECT_EQ(OB_SUCCESS, time_wheel_->schedule(this, 100000));
  }
}

int64_t ObSampleTask2::get_retry_time() const
{
  return retry_time_;
}

class ObSampleTask3 : public ObTimeWheelTask
{
public :
  ObSampleTask3() : tw_precision_(0), run_time_(0), inc_num_(++NumTask::inc_num_) { }
  ~ObSampleTask3() {};
  void set_run_time(const int64_t run_time) { run_time_ = run_time; }
  int64_t get_run_time() const { return run_time_; }
  void runTimerTask();
  void set_precision(const int64_t precision) { tw_precision_ = precision; }
  int64_t get_precision() const  { return tw_precision_;}
  uint64_t hash() const { return inc_num_; }
private :
  int64_t tw_precision_;
  int64_t run_time_;
  int64_t inc_num_;
};

void ObSampleTask3::runTimerTask()
{
  int64_t max = ((get_run_time() + get_precision() - 1) / get_precision() ) * get_precision() + 5000;
  int64_t min = (get_run_time() / get_precision()) * get_precision();
  int64_t current = ObClockGenerator::getClock();

  if (current < min || current > max) {
    TRANS_LOG(INFO, "run task", K(min), K(max), K(current), "run_time", get_run_time());
  }
  EXPECT_TRUE(current >= min && current <= max);
}

struct InputParas{
  InputParas() : tw_(NULL), task_(NULL), task_num_(0), task_delay_(0) {}
  ~InputParas() {}
  ObSampleTimer *tw_;
  ObSampleTask *task_;
  int64_t task_num_;
  int64_t task_delay_;
};

struct OutputParas
{
  explicit OutputParas(const int64_t count);
  ~OutputParas();
  ObSampleTask *task_arr_[NumTask::MAX_NUM_TASK];
  int64_t valid_task_count_;
};

OutputParas::OutputParas(const int64_t count) : valid_task_count_(0)
{
  for (int64_t i = 0; i < NumTask::MAX_NUM_TASK; i++) {
    task_arr_[i] = NULL;
  }
  valid_task_count_ = count;
}

OutputParas::~OutputParas()
{
  for (int64_t i = 0; i < valid_task_count_; ++i) {
    task_arr_[i] = NULL;
  }
}

class MyThread
{
public :
  static void *create_task(void *args);
  static void *schedule_task(void *args);
  static void *cancel_task(void *args);
  static void schedule_run(const int64_t thread_num, const int64_t task_num, const int64_t sleep_time);
};

// according to the schedule's task, cancel the task randomly
void MyThread::schedule_run(const int64_t thread_num, const int64_t task_num, const int64_t sleep_time)
{
  ObSampleTimer *ob_sample_timer = new ObSampleTimer();
  ASSERT_TRUE(NULL != ob_sample_timer);
  EXPECT_EQ(OB_SUCCESS, ob_sample_timer->init(1000, NumTask::TW_THREAD_NUM, "test"));
  EXPECT_EQ(OB_SUCCESS, ob_sample_timer->start());

  pthread_attr_t attr;
  pthread_attr_init(&attr);
  pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);

  // create and init the paras
  const int64_t NUM = thread_num;
  const int64_t TASK_NUM_FOR_THREAD = task_num;
  const int64_t SLEEP_TIME = sleep_time;
  InputParas *params = new InputParas[NUM];
  EXPECT_TRUE(NULL != params);

  for (int64_t i = 0; i < NUM; ++i) {
    params[i].tw_ = ob_sample_timer;
    params[i].task_ = new ObSampleTask[TASK_NUM_FOR_THREAD];
    EXPECT_TRUE(NULL != params[i].task_);
    params[i].task_num_ = NumTask::HIGH_CURRENCY_LOCK_NUM_TASK;
    params[i].task_delay_ = SLEEP_TIME * 1000 +  ObRandom::rand(0, 200) % (int64_t) ((i +1) * 1000); // delay = 5s + rand()
  }
  // create five schedule threads, and schedule above tasks respectively
  pthread_t tids[NUM * 2];
  for (int64_t i = 0; i < NUM; ++i) {
    tids[i] = i;
    EXPECT_TRUE(0 == pthread_create(&tids[i], &attr, MyThread::schedule_task, static_cast<void *>(&(params[i]))));
  }
  // sleep 5s
  ObClockGenerator::msleep(SLEEP_TIME);
  // create five cancel threads, and cancel above taask accordingly
  for (int64_t i = 0; i < NUM; ++i) {
    tids[NUM + i] = i;
    EXPECT_TRUE(0 == pthread_create(&tids[NUM + i], &attr, MyThread::cancel_task, static_cast<void *>(&(params[i]))));
  }
  // collect resources of threads
  for (int64_t i = 0; i < NUM * 2; ++i) {
    pthread_join(tids[i], NULL);
  }
  EXPECT_EQ(OB_SUCCESS, ob_sample_timer->stop());
  EXPECT_EQ(OB_SUCCESS, ob_sample_timer->wait());

  delete ob_sample_timer;
  ob_sample_timer = NULL;

  if (NULL != params) {
    for (int64_t i = 0; i < NUM; ++i) {
      if (NULL != params[i].task_) {
        delete [] params[i].task_;
        params[i].task_ = NULL;
      }
    }
    delete [] params;
    params = NULL;
  }
}
// schedule tasks in thread functions
void *MyThread::schedule_task(void *args)
{
  InputParas *in_paras = static_cast<InputParas *>(args);
  EXPECT_TRUE(NULL != in_paras);
  for (int64_t i = 0; i < in_paras->task_num_; ++i) {
    EXPECT_TRUE(OB_SUCCESS == in_paras->tw_->schedule(&(in_paras->task_[i]), in_paras->task_delay_));
    TRANS_LOG(INFO, "schedule task", "task", &(in_paras->task_[i]), "delay", in_paras->task_delay_);
  }
  pthread_exit(NULL);
}
// cancel tasks in thread functions
void *MyThread::cancel_task(void *args)
{
  InputParas *in_paras = static_cast<InputParas *>(args);
  EXPECT_TRUE(NULL != in_paras);
  for (int64_t i = 0; i < in_paras->task_num_; ++i) {
    EXPECT_EQ(OB_SUCCESS, in_paras->tw_->cancel(&(in_paras->task_[i])));
    TRANS_LOG(INFO, "cancel task", "task", &(in_paras->task_[i]), "delay", in_paras->task_delay_);
  }
  pthread_exit(NULL);
}
// register tasks in thread functions
void *MyThread::create_task(void *args)
{
  int64_t start = 0;
  int64_t end = 0;
  InputParas *in_para = static_cast<InputParas *>(args);
  EXPECT_TRUE(in_para != NULL);
  OutputParas *out_para = new OutputParas(in_para->task_num_);
  EXPECT_TRUE(NULL != out_para);

  start = ObClockGenerator::getClock();
  for (int64_t i = 0; i < in_para->task_num_; i++) {
    out_para->task_arr_[i] = new ObSampleTask();
    EXPECT_TRUE(NULL != out_para->task_arr_[i]);
  }
  end = ObClockGenerator::getClock();
  TRANS_LOG(INFO, "", "alloc used", end - start);

  start = ObClockGenerator::getClock();
  for (int64_t i = 0; i < in_para->task_num_; i++) {
    EXPECT_TRUE(OB_SUCCESS == in_para->tw_->schedule(out_para->task_arr_[i], in_para->task_delay_));
  }
  end = ObClockGenerator::getClock();
  TRANS_LOG(INFO, "", "schedule used", end - start);

  pthread_exit(static_cast<void *>(out_para));
}

//------------------------------basic funtions test--------------------------
// basic functions test are as follows
// test the time wheel work flows which contain create,init and destroy operations

#ifdef BASE_TEST
#ifdef TEST_TW_CREATE_INIT_DES
TEST_F(TestObTimeWheel, test_tw_create_init_des)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  int64_t precision = 1;
  for (int64_t i = 0; i < 4; i++) {
    if( i!= 0) {
      precision *= 10;
    }
    ObSampleTimer ob_sample_timer;
    EXPECT_EQ(OB_SUCCESS, ob_sample_timer.init(precision, NumTask::TW_THREAD_NUM, "test"));
    EXPECT_EQ(OB_SUCCESS, ob_sample_timer.start());
    EXPECT_EQ(OB_SUCCESS, ob_sample_timer.stop());
    EXPECT_EQ(OB_SUCCESS, ob_sample_timer.wait());
    ob_sample_timer.destroy();
  }
}
#endif
// test the task token work flows which contain create,init and destroy operations
#ifdef TEST_TASK_CREATE_INIT_DES
TEST_F(TestObTimeWheel, test_task_create_init_des)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObSampleTask *ob_sample_task = new ObSampleTask();
  ASSERT_TRUE(NULL != ob_sample_task);
  EXPECT_TRUE(!ob_sample_task->is_run_over());

  delete ob_sample_task;
  ob_sample_task = NULL;
}
#endif
// test the task which delay time is too long.
#ifdef TEST_TW_SCHEDULE_RUN
TEST_F(TestObTimeWheel, test_tw_schedule_run)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObSampleTimer ob_sample_timer;
  EXPECT_EQ(OB_SUCCESS, ob_sample_timer.init(1000, NumTask::TW_THREAD_NUM, "test"));
  EXPECT_EQ(OB_SUCCESS, ob_sample_timer.start());

  ObSampleTask ob_sample_task1;
  EXPECT_EQ(OB_SUCCESS, ob_sample_timer.schedule(&ob_sample_task1, 100000));

  ObSampleTask ob_sample_task2;
  EXPECT_EQ(OB_SUCCESS, ob_sample_timer.schedule(&ob_sample_task2, 15000000));

  ObClockGenerator::msleep(110);
  EXPECT_TRUE(ob_sample_task1.is_run_over());
  ObClockGenerator::msleep(15010);
  EXPECT_TRUE(ob_sample_task2.is_run_over());

  EXPECT_EQ(OB_SUCCESS, ob_sample_timer.stop());
  EXPECT_EQ(OB_SUCCESS, ob_sample_timer.wait());
}
#endif
// test the run process of task
#ifdef TEST_TW_SCHEDULE_RUN_CANCEL
TEST_F(TestObTimeWheel, test_tw_schedule_run_cancel)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObSampleTimer ob_sample_timer;
  EXPECT_EQ(OB_SUCCESS, ob_sample_timer.init(1000, NumTask::TW_THREAD_NUM, "test"));
  EXPECT_EQ(OB_SUCCESS, ob_sample_timer.start());

  ObSampleTask ob_sample_task;
  EXPECT_EQ(OB_SUCCESS, ob_sample_timer.schedule(&ob_sample_task, 1000000));
  ObClockGenerator::msleep(1010);
  EXPECT_TRUE(ob_sample_task.is_run_over());

  ob_sample_task.reset();
  EXPECT_EQ(OB_SUCCESS, ob_sample_timer.schedule(&ob_sample_task, 1000000));
  EXPECT_EQ(OB_SUCCESS, ob_sample_timer.cancel(&ob_sample_task));
  ObClockGenerator::msleep(1010);
  EXPECT_TRUE(!ob_sample_task.is_run_over());

  EXPECT_EQ(OB_SUCCESS, ob_sample_timer.stop());
  EXPECT_EQ(OB_SUCCESS, ob_sample_timer.wait());
}
#endif

// test the precision of task
#ifdef TEST_TW_TASK_RUN_PRECISION
TEST_F(TestObTimeWheel, test_tw_task_run_precision)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObSampleTimer ob_sample_timer;
  EXPECT_EQ(OB_SUCCESS, ob_sample_timer.init(1000, NumTask::TW_THREAD_NUM, "test"));
  EXPECT_EQ(OB_SUCCESS, ob_sample_timer.start());

  const int64_t count = 100;
  const int64_t delay = 1000000;

  ObSampleTask3 *tmp[count];
  for (int64_t i = 0; i < count; i++) {
    tmp[i] = new ObSampleTask3();
    ASSERT_TRUE(NULL != tmp[i]);
    tmp[i]->set_precision(1000);
    tmp[i]->set_run_time(ObClockGenerator::getClock() + delay);
    EXPECT_EQ(OB_SUCCESS, ob_sample_timer.schedule(tmp[i], delay));
  }

  ObClockGenerator::msleep(1000);
  EXPECT_EQ(OB_SUCCESS, ob_sample_timer.stop());
  EXPECT_EQ(OB_SUCCESS, ob_sample_timer.wait());

  for (int64_t i = 0; i < count; i++) {
    EXPECT_TRUE(NULL != tmp[i]);
    delete tmp[i];
    tmp[i] = NULL;
  }
}
#endif
#endif

//--------------------- high concurrency schedule and run ------------------
// high concurrency test are as follows
// schedule the task in high concurrency way.
//

#ifdef CURRENTCY_TEST

#ifdef TEST_TW_HIGHCURRENCY_SCHEDULE
TEST_F(TestObTimeWheel, test_tw_highcurrency_schedule)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObSampleTimer *ob_sample_timer = new ObSampleTimer();
  ASSERT_TRUE(NULL != ob_sample_timer);
  EXPECT_EQ(OB_SUCCESS, ob_sample_timer->init(1000, NumTask::TW_THREAD_NUM, "test"));
  EXPECT_EQ(OB_SUCCESS, ob_sample_timer->start());

  pthread_attr_t attr;
  pthread_attr_init(&attr);
  pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
  pthread_t tids[NumTask::THREAD_NUM];

  //create para and init
  InputParas *in_para = new InputParas();
  in_para->tw_ = ob_sample_timer;
  in_para->task_num_ = NumTask::HIGH_CURRENCY_NUM_TASK;
  in_para->task_delay_ = 1000000;

  for (int64_t i = 0; i < NumTask::THREAD_NUM; i++) {
    EXPECT_TRUE(pthread_create(&tids[i], &attr, MyThread::create_task, static_cast<void *>(in_para)) == 0);
  }

  void *tmp[NumTask::THREAD_NUM];
  pthread_attr_destroy(&attr);
  for (int64_t i = 0; i < NumTask::THREAD_NUM; i++) {
    EXPECT_EQ(0, pthread_join(tids[i], &tmp[i]));
  }

  delete in_para;
  in_para = NULL;
  EXPECT_EQ(OB_SUCCESS, ob_sample_timer->stop());
  EXPECT_EQ(OB_SUCCESS, ob_sample_timer->wait());

  for (int64_t i = 0; i < NumTask::THREAD_NUM; i++) {
    OutputParas *out_pars = static_cast<OutputParas *>(tmp[i]);
    EXPECT_TRUE(NULL != out_pars);
    delete out_pars;
    out_pars = NULL;
  }
  delete ob_sample_timer;
  ob_sample_timer = NULL;
}
#endif

// run the task in high concurrency way.
#ifdef TEST_TW_HIGHCURRENCY_SCHEDULE_RUN
TEST_F(TestObTimeWheel, test_tw_highcurrency_schedule_run)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObSampleTask::clear_total_run_count();
  ObSampleTimer *ob_sample_timer = new ObSampleTimer();
  ASSERT_TRUE(NULL != ob_sample_timer);
  EXPECT_EQ(OB_SUCCESS, ob_sample_timer->init(1000, NumTask::TW_THREAD_NUM, "test"));
  EXPECT_EQ(OB_SUCCESS, ob_sample_timer->start());

  pthread_attr_t attr;
  pthread_attr_init(&attr);
  pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
  pthread_t tids[NumTask::THREAD_NUM];

  // create para and init
  InputParas *in_para = new InputParas();
  in_para->tw_ = ob_sample_timer;
  in_para->task_num_ = NumTask::HIGH_CURRENCY_NUM_TASK;
  in_para->task_delay_ = 100000;

  for (int64_t i = 0; i < NumTask::THREAD_NUM; i++){
    EXPECT_TRUE(pthread_create(&tids[i], &attr, MyThread::create_task, static_cast<void *>(in_para)) == 0);
  }

  void *tmp[NumTask::THREAD_NUM];
  for (int64_t i = 0; i < NumTask::THREAD_NUM; i++) {
    EXPECT_EQ(0, pthread_join(tids[i], &tmp[i]));
  }
  // Error:there are several tasks which have not been executed.
  // success: all tasks have been executed.
  ObClockGenerator::msleep(10000);
  EXPECT_EQ(NumTask::THREAD_NUM * NumTask::HIGH_CURRENCY_NUM_TASK, ObSampleTask::get_total_run_count());

  delete in_para;
  in_para = NULL;
  EXPECT_EQ(OB_SUCCESS, ob_sample_timer->stop());
  EXPECT_EQ(OB_SUCCESS, ob_sample_timer->wait());

  for (int64_t i = 0; i < NumTask::THREAD_NUM; i++) {
    OutputParas *out_pars = static_cast<OutputParas *>(tmp[i]);
    EXPECT_TRUE(NULL != out_pars);
    delete out_pars;
    out_pars = NULL;
  }
  delete ob_sample_timer;
  ob_sample_timer = NULL;
  ObSampleTask::clear_total_run_count();
}
#endif

// run the task and cancel in high concurrency way.
#ifdef TEST_TW_HIGHCURRENCY_SCHEDULE_CANCEL
TEST_F(TestObTimeWheel, test_tw_highcurrency_schedule_cancel)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObSampleTask::clear_total_run_count();
  ObSampleTimer *ob_sample_timer = new ObSampleTimer();
  ASSERT_TRUE(NULL != ob_sample_timer);
  EXPECT_EQ(OB_SUCCESS, ob_sample_timer->init(1000, NumTask::TW_THREAD_NUM, "test"));
  EXPECT_EQ(OB_SUCCESS, ob_sample_timer->start());

  pthread_attr_t attr;
  pthread_attr_init(&attr);
  pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
  pthread_t tids[NumTask::THREAD_NUM];

  // create para and init
  InputParas *in_para = new InputParas();
  in_para->tw_ = ob_sample_timer;
  in_para->task_num_ = NumTask::HIGH_CURRENCY_NUM_TASK;
  in_para->task_delay_ = 12000000;

  for (int64_t i = 0; i < NumTask::THREAD_NUM; i++) {
    EXPECT_TRUE(pthread_create(&tids[i], &attr, MyThread::create_task, static_cast<void *>(in_para)) == 0);
  }
  void *tmp[NumTask::THREAD_NUM];
  for (int64_t i = 0; i < NumTask::THREAD_NUM; i++) {
    EXPECT_EQ(0, pthread_join(tids[i], &tmp[i]));
  }
  int64_t start = 0;
  int64_t end = 0;
  start = ObClockGenerator::getClock();
  for (int64_t i = 0; i < NumTask::THREAD_NUM; i++) {
    OutputParas *out_para = static_cast<OutputParas *>(tmp[i]);
    ASSERT_TRUE(NULL != out_para);
    for (int64_t j = 0; j < in_para->task_num_; j++) {
      ASSERT_TRUE(NULL != out_para->task_arr_[j]);
      ob_sample_timer->cancel(out_para->task_arr_[j]);
    }
    delete out_para;
    out_para = NULL;
  }
  end = ObClockGenerator::getClock();
  TRANS_LOG(WARN, "", "used", end - start);
  EXPECT_EQ(0, ObSampleTask::get_total_run_count());

  delete in_para;
  in_para = NULL;
  ObSampleTask::clear_total_run_count();
  EXPECT_EQ(OB_SUCCESS, ob_sample_timer->stop());
  EXPECT_EQ(OB_SUCCESS, ob_sample_timer->wait());
  delete ob_sample_timer;
  ob_sample_timer = NULL;
}
#endif

// purpose: test whether there are bugs after/before executing tasks
// steps: (a special case is as follows)
// (1) Create 100,000 tasks in main thread, and use the same timer to schedule these tasks in threads A, B, C, D and E,
//     where A is responsible for the first 20,000 tasks, B the next 20,000 tasks, ....
//     All tasks' delay is set to [5s, 6s]
// (2) Threads F, G, H, I and J start to cancel the corresponding tasks after main thread sleeps 5s
#ifdef TEST_TW_HIGHCURRENCY_SCHEDULE_CANCEL_RANDOM
TEST_F(TestObTimeWheel, test_tw_high_concurrency_run_cancel)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  // 1 schedule thread and 1 cancel thread, each thread schedules 1,000 tasks
  // sleep time is set to 1s
  MyThread::schedule_run(1, 1000, 1000);
  // 2 schedule thread and 2 cancel thread, each thread schedules 10,000 tasks
  // sleep time is set to 2s
  MyThread::schedule_run(2, 10000, 2000);
  // 4 schedule thread and 4 cancel thread, each thread schedules 100,000 tasks
  // sleep time is set to 3s
  MyThread::schedule_run(4, 100 * 1000, 3000);
  // 6 schedule thread and 6 cancel thread, each thread schedules 1,000,000 tasks
  // sleep time is set to 4s
  MyThread::schedule_run(6, 1000 * 1000, 4000 );
  // 8 schedule thread and 8 cancel thread, each thread schedules 10,000,000 tasks
  // sleep time is set to 4s
  MyThread::schedule_run(8, 10 * 1000 * 1000, 4000);
}
#endif

// when the task will run, cancel it.
// this case is to test the availablity of lock between run and cancel thread.
#ifdef TEST_TW_LOCK_AVAILABLITY
TEST_F(TestObTimeWheel, test_tw_lock_availablity)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObSampleTimer *ob_sample_timer = new ObSampleTimer();
  ASSERT_TRUE(NULL != ob_sample_timer);
  EXPECT_EQ(OB_SUCCESS, ob_sample_timer->init(1000, NumTask::TW_THREAD_NUM, "test"));
  EXPECT_EQ(OB_SUCCESS, ob_sample_timer->start());

  pthread_attr_t attr;
  pthread_attr_init(&attr);
  pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);

  // create and init the paras
  InputParas *in_para = new InputParas();
  in_para->tw_ = ob_sample_timer;
  in_para->task_num_ = NumTask::HIGH_CURRENCY_LOCK_NUM_TASK;
  in_para->task_delay_ = 1500000;

  for (int64_t i = 0; i < NumTask::HIGH_CURRENCY_LOCK_TEST_COUNT; i++) {
    // create a pthread
    pthread_t tid = 0;
    EXPECT_TRUE(pthread_create(&tid, &attr, MyThread::create_task, static_cast<void *>(in_para)) == 0);

    ObClockGenerator::msleep(1450);
    void *tmp = NULL;
    EXPECT_EQ(0, pthread_join(tid, &tmp));
    OutputParas *out_para = static_cast<OutputParas *>(tmp);
    ASSERT_TRUE(NULL != out_para);

    for (int i = 0; i < out_para->valid_task_count_; i++) {
      ASSERT_TRUE(NULL != out_para->task_arr_[i]);
      ob_sample_timer->cancel(out_para->task_arr_[i]);
      EXPECT_TRUE(false == (out_para->task_arr_[i])->is_run_over());
    }
  }

  delete in_para;
  in_para = NULL;
  EXPECT_EQ(OB_SUCCESS, ob_sample_timer->stop());
  EXPECT_EQ(OB_SUCCESS, ob_sample_timer->wait());
  delete ob_sample_timer;
  ob_sample_timer = NULL;
}
#endif
#endif

//-------------------------------boundary test-----------------------------------------
// boundary test are as follows
// test the time wheel which wasn't inited before used.

#ifdef EDGE_TEST

#ifdef TEST_TIMEWHEELBASE_NO_INIT
TEST_F(TestObTimeWheel, test_timewheel_base_init)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  TimeWheelBase tw_base;
  // not init tw_base
  EXPECT_EQ(OB_NOT_INIT, tw_base.start());
  EXPECT_EQ(OB_NOT_INIT, tw_base.stop());
  EXPECT_EQ(OB_NOT_INIT, tw_base.wait());
  EXPECT_EQ(OB_NOT_INIT, tw_base.schedule(NULL, 1000));
  EXPECT_EQ(OB_NOT_INIT, tw_base.cancel(NULL));
  // after init, no start
  EXPECT_EQ(OB_SUCCESS, tw_base.init(1000));
  EXPECT_EQ(OB_ERR_UNEXPECTED, tw_base.stop());
  EXPECT_EQ(OB_SUCCESS, tw_base.wait());
  EXPECT_EQ(OB_ERR_UNEXPECTED, tw_base.schedule(NULL, 1000));
  EXPECT_EQ(OB_INVALID_ARGUMENT, tw_base.cancel(NULL));

  EXPECT_EQ(OB_SUCCESS, tw_base.start());
  EXPECT_EQ(OB_INVALID_ARGUMENT, tw_base.schedule(NULL, 1000));
  EXPECT_EQ(OB_INVALID_ARGUMENT, tw_base.cancel(NULL));

  tw_base.destroy();
}
#endif
// test the fucntions of time wheel
#ifdef TEST_TIMEWHEEL_NO_INIT
TEST_F(TestObTimeWheel, test_timewheel_no_init)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObSampleTimer ob_sample_timer;
  ObSampleTask ob_sample_task;
  const static int64_t precision= 1000;
  const static int64_t delay = 100000;
  EXPECT_EQ(OB_NOT_INIT, ob_sample_timer.schedule(&ob_sample_task, precision));
  EXPECT_EQ(OB_NOT_INIT, ob_sample_timer.start());
  EXPECT_EQ(OB_NOT_INIT, ob_sample_timer.stop());
  EXPECT_EQ(OB_NOT_INIT, ob_sample_timer.wait());

  EXPECT_EQ(OB_NOT_INIT, ob_sample_timer.schedule(&ob_sample_task, delay));
  EXPECT_EQ(OB_NOT_INIT, ob_sample_timer.cancel(&ob_sample_task));
}
#endif
// test the order of calling start, stop and wait
#ifdef TEST_TIMEWHEEL_RUNNING
TEST_F(TestObTimeWheel, test_timewheel_start_stop_wait)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObSampleTimer ob_sample_timer;
  ObSampleTask ob_sample_task;
  const static int64_t precision= 1000;
  const static int64_t delay = 100000;
  EXPECT_EQ(OB_SUCCESS, ob_sample_timer.init(precision, NumTask::TW_THREAD_NUM, "test"));
  // call the functions schedule and cancle without calling start
  EXPECT_EQ(OB_ERR_UNEXPECTED, ob_sample_timer.schedule(&ob_sample_task, delay));
  EXPECT_EQ(OB_TIMER_TASK_HAS_NOT_SCHEDULED, ob_sample_timer.cancel(&ob_sample_task));
  // call the functions stop/wait directly without calling start
  EXPECT_EQ(OB_ERR_UNEXPECTED, ob_sample_timer.stop());
  // call the function start repeatedly
  EXPECT_EQ(OB_SUCCESS, ob_sample_timer.start());
  EXPECT_EQ(OB_ERR_UNEXPECTED, ob_sample_timer.start());
  // call the fucntion wait without calling stop
  EXPECT_EQ(OB_ERR_UNEXPECTED, ob_sample_timer.wait());
  ob_sample_timer.destroy();
}
#endif
// init the timewheel with invalid argument
#ifdef TEST_TIMEWHEEL_INVALID_INIT
TEST_F(TestObTimeWheel, test_timewheel_invalid_init)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObSampleTimer ob_sample_timer;
  EXPECT_EQ(OB_INVALID_ARGUMENT, ob_sample_timer.init(-1, NumTask::TW_THREAD_NUM, "test"));
}
#endif
// test the time wheel which was inited twice.
#ifdef TEST_TIMEWHEEL_REPEAT_INIT
TEST_F(TestObTimeWheel, test_timewheel_repeat_init)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObSampleTimer ob_sample_timer;
  EXPECT_EQ(OB_SUCCESS, ob_sample_timer.init(1000, NumTask::TW_THREAD_NUM, "test"));
  EXPECT_EQ(OB_INIT_TWICE, ob_sample_timer.init(1000, NumTask::TW_THREAD_NUM, "test"));
}
#endif
// destroy the time wheel twice
#ifdef TEST_TIMEWHEEL_REPEAT_DES
TEST_F(TestObTimeWheel, test_timewheel_repeat_des)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObSampleTimer ob_sample_timer;
  EXPECT_EQ(OB_SUCCESS, ob_sample_timer.init(1000, NumTask::TW_THREAD_NUM, "test"));
  EXPECT_EQ(OB_SUCCESS, ob_sample_timer.start());
  EXPECT_EQ(OB_SUCCESS, ob_sample_timer.stop());
  EXPECT_EQ(OB_SUCCESS, ob_sample_timer.wait());
  ob_sample_timer.destroy();
  ob_sample_timer.destroy();
}
#endif
// schedule the task after having destroy time wheel
#ifdef TEST_TIMEWHEEL_SCHEDULE_AFTER_DES
TEST_F(TestObTimeWheel, test_timewheel_schedule_after_des)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObSampleTimer ob_sample_timer;
  EXPECT_EQ(OB_SUCCESS, ob_sample_timer.init(1000, NumTask::TW_THREAD_NUM, "test"));
  EXPECT_EQ(OB_SUCCESS, ob_sample_timer.start());
  EXPECT_EQ(OB_SUCCESS, ob_sample_timer.stop());
  EXPECT_EQ(OB_SUCCESS, ob_sample_timer.wait());
  ob_sample_timer.destroy();

  ObSampleTask ob_sample_task;
  EXPECT_EQ(OB_NOT_INIT, ob_sample_timer.schedule(&ob_sample_task, 1000));
}
#endif
// schedule the task which has invalid argument
#ifdef TEST_TIMEWHEEL_INVALID_TASK_SCHEDULE
TEST_F(TestObTimeWheel, test_timewheel_invalid_task_schedule)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObSampleTimer ob_sample_timer;
  EXPECT_EQ(OB_SUCCESS, ob_sample_timer.init(1000, NumTask::TW_THREAD_NUM, "test"));
  EXPECT_EQ(OB_SUCCESS, ob_sample_timer.start());

  ObSampleTask *ob_sample_task = NULL;
  EXPECT_EQ(OB_INVALID_ARGUMENT, ob_sample_timer.schedule(ob_sample_task, 1000));
  ob_sample_task = new ObSampleTask();
  EXPECT_EQ(OB_INVALID_ARGUMENT, ob_sample_timer.schedule(ob_sample_task, -1));

  delete ob_sample_task;
  ob_sample_task = NULL;
}
#endif
// schedule the task twice
#ifdef TEST_TIMEWHEEL_REPEAT_SCHEDULE
TEST_F(TestObTimeWheel, test_timewheel_repeat_schedule)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObSampleTimer ob_sample_timer;
  EXPECT_EQ(OB_SUCCESS, ob_sample_timer.init(1000, NumTask::TW_THREAD_NUM, "test"));
  EXPECT_EQ(OB_SUCCESS, ob_sample_timer.start());

  ObSampleTask ob_sample_task;
  EXPECT_EQ(OB_SUCCESS, ob_sample_timer.schedule(&ob_sample_task, 100000));
  EXPECT_EQ(OB_TIMER_TASK_HAS_SCHEDULED, ob_sample_timer.schedule(&ob_sample_task, 1000));
  EXPECT_EQ(OB_SUCCESS, ob_sample_timer.stop());
  EXPECT_EQ(OB_SUCCESS, ob_sample_timer.wait());
}
#endif

// cancel the same task for several times
#ifdef TEST_TIMEWHEEL_REPEAT_CANCEL
TEST_F(TestObTimeWheel, test_timewheel_repeat_cancel)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObSampleTimer ob_sample_timer;
  EXPECT_EQ(OB_SUCCESS, ob_sample_timer.init(1000, NumTask::TW_THREAD_NUM, "test"));
  EXPECT_EQ(OB_SUCCESS, ob_sample_timer.start());

  ObSampleTask ob_sample_task;
  EXPECT_EQ(OB_SUCCESS, ob_sample_timer.schedule(&ob_sample_task, 1000));
  EXPECT_EQ(OB_SUCCESS, ob_sample_timer.cancel(&ob_sample_task));
  EXPECT_EQ(OB_TIMER_TASK_HAS_NOT_SCHEDULED, ob_sample_timer.cancel(&ob_sample_task));
  ObSampleTask *task = NULL;
  EXPECT_EQ(OB_INVALID_ARGUMENT, ob_sample_timer.cancel(task));
  EXPECT_EQ(OB_SUCCESS, ob_sample_timer.stop());
  EXPECT_EQ(OB_SUCCESS, ob_sample_timer.wait());
}
#endif

// repeat the process "schedule->cancel" for several times
#ifdef TEST_TIMEWHEEL_REPEAT_SCHEDULE_CANCEL
TEST_F(TestObTimeWheel, test_timewheel_repeat_schedule_cancel)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObSampleTimer *ob_sample_timer = new ObSampleTimer();
  ASSERT_TRUE(NULL != ob_sample_timer);
  EXPECT_EQ(OB_SUCCESS, ob_sample_timer->init(1000, NumTask::TW_THREAD_NUM, "test"));
  EXPECT_EQ(OB_SUCCESS, ob_sample_timer->start());
  ObSampleTask ob_sample_task;
  for (int64_t i = 0; i < 1000000; i++) {
    // the delay time should great and equal to 1000us.
    EXPECT_EQ(OB_SUCCESS, ob_sample_timer->schedule(&ob_sample_task, 100000));
    EXPECT_TRUE(!ob_sample_task.is_run_over());
    EXPECT_EQ(OB_SUCCESS, ob_sample_timer->cancel(&ob_sample_task));
  }
  EXPECT_EQ(OB_SUCCESS, ob_sample_timer->stop());
  EXPECT_EQ(OB_SUCCESS, ob_sample_timer->wait());
  delete ob_sample_timer;
  ob_sample_timer = NULL;
}
#endif

// schedule self for 100 times
#ifdef TEST_TIMEWHEEL_SCHEDULE_SELF
TEST_F(TestObTimeWheel, test_timewheel_schedule_self)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObSampleTimer ob_sample_timer;
  EXPECT_EQ(OB_SUCCESS, ob_sample_timer.init(1000, NumTask::TW_THREAD_NUM, "test"));
  EXPECT_EQ(OB_SUCCESS, ob_sample_timer.start());

  ObSampleTask2 ob_sample_task;
  ob_sample_task.set_time_wheel(&ob_sample_timer);
  EXPECT_EQ(OB_SUCCESS, ob_sample_timer.schedule(&ob_sample_task, 100000));

  ObClockGenerator::msleep(15000);
  EXPECT_EQ(100, ob_sample_task.get_retry_time());
  EXPECT_EQ(OB_SUCCESS, ob_sample_timer.stop());
  EXPECT_EQ(OB_SUCCESS, ob_sample_timer.wait());
}
#endif

// cancel the task which has been executed.
#ifdef TEST_TIMEWHEEL_CANCEL_RUNOVER_TASK
TEST_F(TestObTimeWheel, test_timewheel_cancel_runover_task)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObSampleTimer ob_sample_timer;
  EXPECT_EQ(OB_SUCCESS, ob_sample_timer.init(1000, NumTask::TW_THREAD_NUM, "test"));
  EXPECT_EQ(OB_SUCCESS, ob_sample_timer.start());

  ObSampleTask ob_sample_task;
  EXPECT_EQ(OB_SUCCESS, ob_sample_timer.schedule(&ob_sample_task, 100000));

  ObClockGenerator::msleep(1000);
  EXPECT_TRUE(true ==  ob_sample_task.is_run_over());
  EXPECT_EQ(OB_SUCCESS, ob_sample_timer.stop());
  EXPECT_EQ(OB_SUCCESS, ob_sample_timer.wait());
}
#endif
#endif

}//end of unittest
}//end of oceanbase

using namespace oceanbase;
using namespace oceanbase::common;

int main(int argc, char **argv)
{
  int ret = 1;
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_ob_time_wheel.log", true);
  logger.set_log_level(OB_LOG_LEVEL_INFO);
    TRANS_LOG(WARN, "init memory pool error!");
  } else if (OB_SUCCESS != (ret = ObClockGenerator::init())) {
    TRANS_LOG(WARN, "init ObClockGenerator error!");
  } else {
    testing::InitGoogleTest(&argc, argv);
    ret = RUN_ALL_TESTS();
  }
  return ret;
}
