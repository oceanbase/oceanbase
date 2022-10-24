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

#define USING_LOG_PREFIX SQL_MONITOR
#include <gtest/gtest.h>
#include "share/ob_thread_pool.h"
#include "sql/monitor/ob_monitor_info_manager.h"
#include "sql/monitor/ob_phy_plan_monitor_info.h"
#include "sql/engine/ob_physical_plan.h"
using namespace std;
using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql;

static const int64_t TEST_MAX_OPERATOR_COUNT = 5;
static const int64_t TEST_MAX_EXECUTOR_COUNT = 1000;
static const int64_t TEST_MAX_MONITOR_COUNT = 300;
static const int64_t TEST_BATCH_CG_COUNT = 5000;
static const int64_t OB_MAX_INFORMATION_COUNT = 5;
static const int64_t N = 10;
static const int64_t TEST_GET_COUNT = 4;
//class TestGetByQueryId : public CDefaultRunnable
//{
//public:
//  TestGetByQueryId(ObMonitorInfoManager *monitor_info)
//      : CDefaultRunnable(TEST_GET_COUNT), monitor_info_(monitor_info), query_id_(0)
//  {}
//  virtual ~TestGetByQueryId() {}
//  virtual void run(CThread*, void *) {do_get();}
//  int64_t get_query_id() { return ATOMIC_AAF(&query_id_, 1); }
//  static int do_check_operator_info(int64_t query_id, ObPhyOperatorMonitorInfo &op_info);
//private:
//  int do_get();
//private:
//  ObMonitorInfoManager *monitor_info_;
//  int64_t query_id_;
//};
//int TestGetByQueryId::do_get()
//{
//  int ret = OB_SUCCESS;
//  int64_t query_id = 0;
//  ObPhyOperatorMonitorInfo op_info;
//  //while (query_id < TEST_MAX_EXECUTOR_COUNT) {
//  //  query_id = get_query_id();
//  //  for (int64_t i = 0; i < TEST_MAX_OPERATOR_COUNT; i++) {
//  //    ret = monitor_info_->get_monitor_info_by_query_id(query_id, i, op_info);
//  //    EXPECT_TRUE(OB_SUCCESS == ret || OB_ERROR_OUT_OF_RANGE == ret);
//  //    if (OB_SUCCESS != ret && OB_ERROR_OUT_OF_RANGE != ret) {
//  //      LOG_ERROR("fail to get monitor by query id", K(ret), K(query_id), K(i));
//  //    }
//  //    if (OB_SUCC(ret)) {
//  //      EXPECT_EQ(OB_SUCCESS, do_check_operator_info(query_id, op_info));
//  //    }
//  //  }
//  //}
//  return ret;
//}
//int TestGetByQueryId::do_check_operator_info(int64_t query_id, ObPhyOperatorMonitorInfo &op_info)
//{
//  int ret = OB_SUCCESS;
//  for (int64_t i = 0; i < OB_MAX_INFORMATION_COUNT; i++) {
//    int64_t value = 0;
//    EXPECT_EQ(OB_SUCCESS, op_info.get_value(i, value));
//    EXPECT_EQ(query_id, value);
//  }
//  return ret;
//}
//class TestGetByPlanId: public CDefaultRunnable
//{
//public:
//  TestGetByPlanId(ObMonitorInfoManager *monitor_info)
//      : CDefaultRunnable(TEST_GET_COUNT), monitor_info_(monitor_info), plan_id_(0)
//  {}
//  virtual ~TestGetByPlanId() {}
//  virtual void run(CThread*, void *) {do_get();}
//  int64_t get_plan_id() { return ATOMIC_AAF(&plan_id_, 1); }
//  static int do_check_operator_info(int64_t plan_id, ObPhyOperatorMonitorInfo &op_info);
//  static int do_check_avg_operator_info(int64_t plan_id, ObPhyOperatorMonitorInfo &op_info);
//private:
//  int do_get();
//private:
//  ObMonitorInfoManager *monitor_info_;
//  int64_t plan_id_;
//};
//int TestGetByPlanId::do_get()
//{
//  int ret = OB_SUCCESS;
//  int64_t plan_id = 0;
//  ObPhyOperatorMonitorInfo op_info;
//  plan_id = 500;
//  ret = monitor_info_->get_avg_monitor_info(plan_id, 0, op_info);
//  EXPECT_TRUE(OB_SUCCESS == ret || OB_ENTRY_NOT_EXIST == ret);
//  while (plan_id < TEST_MAX_EXECUTOR_COUNT) {
//    plan_id = get_plan_id();
//    for (int64_t i = 0; i < TEST_MAX_OPERATOR_COUNT; i++) {
//      ret = monitor_info_->get_monitor_info_by_plan_id(plan_id, i, op_info);
//      EXPECT_TRUE(OB_SUCCESS == ret || OB_ENTRY_NOT_EXIST == ret || OB_ERROR_OUT_OF_RANGE == ret);
//      if (OB_SUCCESS != ret && OB_ENTRY_NOT_EXIST != ret && OB_ERROR_OUT_OF_RANGE != ret) {
//        LOG_ERROR("fail to get avg monitor info", K(ret), K(plan_id), K(i));
//      }
//      if (OB_SUCC(ret)) {
//        EXPECT_EQ(OB_SUCCESS, do_check_operator_info(plan_id, op_info));
//      }
//      ret = monitor_info_->get_avg_monitor_info(plan_id, i, op_info);
//      EXPECT_TRUE(OB_SUCCESS == ret || OB_ENTRY_NOT_EXIST == ret);
//      if (OB_SUCCESS != ret && OB_ENTRY_NOT_EXIST != ret) {
//        LOG_ERROR("fail to get avg monitor info", K(ret), K(plan_id), K(i));
//      }
//      if (OB_SUCC(ret)) {
//        EXPECT_EQ(OB_SUCCESS, do_check_avg_operator_info(plan_id, op_info));
//      }
//    }
//  }
//  return ret;
//}
//int TestGetByPlanId::do_check_operator_info(int64_t plan_id, ObPhyOperatorMonitorInfo &op_info)
//{
//  int ret = OB_SUCCESS;
//  for (int64_t i = 0; i < OB_MAX_INFORMATION_COUNT; i++) {
//    int64_t value = 0;
//    EXPECT_EQ(OB_SUCCESS, op_info.get_value(i, value));
//    if (value < plan_id * N || value >= (plan_id + 1) * N) {
//      LOG_ERROR("get invalid monitor info", K(value), K(plan_id));
//      abort();
//    }
//  }
//  return ret;
//}
//int TestGetByPlanId::do_check_avg_operator_info(int64_t plan_id, ObPhyOperatorMonitorInfo &op_info)
//{
//  int ret = OB_SUCCESS;
//  for (int64_t i = 0; i < OB_MAX_INFORMATION_COUNT; i++) {
//    int64_t value = 0;
//    EXPECT_EQ(OB_SUCCESS, op_info.get_value(i, value));
//    if (value < plan_id * N || value > (plan_id + 1) * N)
//    {
//      LOG_ERROR("get invalid monitor info", K(plan_id), K(value));
//      abort();
//    }
//  }
//  return ret;
//}
namespace oceanbase
{
namespace sql
{
class TestMonitorInfoManager : public ::testing::Test, public share::ObThreadPool
{
public:
  TestMonitorInfoManager() :
    seq_(0),
    monitor_mgr_()
    //test1_(&monitor_mgr_),
    //test2_(&monitor_mgr_)
  {
    monitor_mgr_.init();
    //monitor_mgr_.set_max_monitor_count(TEST_MAX_MONITOR_COUNT);
    //monitor_mgr_.set_batch_cg_count(TEST_BATCH_CG_COUNT);
  }
  virtual ~TestMonitorInfoManager() {}
  virtual void SetUp() {}
  virtual void TearDown() {}
  virtual void run1() {
    //do_add();
  }
  void do_stress()
  {
    start();
    sleep(5);
    //test1_.start();
    //test2_.start();
    int64_t last_seq = ATOMIC_LOAD(&seq_);
    while(ATOMIC_LOAD(&seq_) < TEST_MAX_EXECUTOR_COUNT) {
      sleep(1);
      int64_t cur_seq = ATOMIC_LOAD(&seq_);
      LOG_INFO("ring array", "tps", cur_seq - last_seq);
      if (seq_ % 1000 == 0) {
        monitor_mgr_.print_memory_size();
      }
      last_seq = cur_seq;
    }
    //test1_.wait();
    //test2_.wait();
    wait();
  }
  int64_t get_seq() { return ATOMIC_AAF(&seq_, 1); }
  int do_add();
  int do_add_and_check();
  int do_gc();
  int build_monitor_info(int64_t query_id, int64_t plan_id, ObPhyPlanMonitorInfo *plan);
public:
  int64_t seq_;
  ObMonitorInfoManager monitor_mgr_;
  //TestGetByQueryId test1_;
  //TestGetByPlanId test2_;
};

int TestMonitorInfoManager::do_add_and_check()
{
  int ret = OB_SUCCESS;
  int64_t query_id = 0;
  while (query_id < TEST_MAX_EXECUTOR_COUNT) {
    query_id = get_seq() + 9;
    int64_t plan_id = query_id / N;
    if (plan_id == 0) {
    } else {
      ObPhyPlanMonitorInfo *info = NULL;
      if (OB_FAIL(monitor_mgr_.alloc(query_id, info))) {
        LOG_ERROR("fail to create monitor info", K(ret), K(query_id), K(plan_id));
      } else if (OB_FAIL(build_monitor_info(query_id, plan_id, info))) {
        LOG_ERROR("fail to buil monitor info", K(ret), K(query_id));
      } else if (OB_FAIL(monitor_mgr_.add_monitor_info(info))) {
        LOG_ERROR("fail to add monitor info", K(ret), K(query_id), K(plan_id));
      } else {
        LOG_INFO("add monitor info success", K(query_id), K(plan_id), K(info));
        ObPhyPlanMonitorInfo *plan_info;
        common::ObRaQueue::Ref ref;
        if (OB_FAIL(monitor_mgr_.get_by_index(query_id, plan_info, &ref))) {
          LOG_WARN("fail to get monitor info", K(ret), K(query_id));
        } else {
          LOG_INFO("get from index success", K(ret), K(query_id), K(*plan_info));
        }
        monitor_mgr_.revert(&ref);
      }
    }
  }
  return ret;
}

//int TestMonitorInfoManager::do_add()
//{
//  int ret = OB_SUCCESS;
//  int64_t query_id = 0;
//  while (query_id < TEST_MAX_EXECUTOR_COUNT) {
//    query_id = get_seq();
//    int64_t plan_id = query_id / N;
//    if (plan_id == 0) {
//    } else {
//      ObPhyPlanMonitorInfo *info = NULL;
//      if (OB_FAIL(monitor_mgr_.create_monitor_info(query_id, plan_id, info))) {
//        LOG_ERROR("fail to create monitor info", K(ret), K(query_id), K(plan_id));
//      } else if (OB_FAIL(build_monitor_info(query_id, plan_id, info))) {
//        LOG_ERROR("fail to buil monitor info", K(ret), K(query_id));
//      } else if (OB_FAIL(monitor_mgr_.add_monitor_info(query_id, plan_id, info))) {
//        LOG_ERROR("fail to add monitor info", K(ret), K(query_id), K(plan_id));
//      }
//    }
//  }
//  return ret;
//}
//
//int TestMonitorInfoManager::do_gc()
//{
//  int ret = OB_SUCCESS;
//  int64_t gc_count = (TEST_MAX_EXECUTOR_COUNT - TEST_MAX_MONITOR_COUNT) / TEST_BATCH_CG_COUNT;
//  for (int64_t i = 0; i < gc_count; i++) {
//    monitor_mgr_.gc();
//  }
//  int64_t query_id = 0;
//  ObPhyOperatorMonitorInfo op_info;
//  while (query_id < TEST_MAX_EXECUTOR_COUNT - TEST_MAX_MONITOR_COUNT) {
//    query_id ++;
//    int64_t plan_id = query_id / N;
//    if (plan_id != 0) {
//      for (int64_t i = 0; i < TEST_MAX_OPERATOR_COUNT; i++) {
//        EXPECT_EQ(OB_ERROR_OUT_OF_RANGE, monitor_mgr_.get_monitor_info_by_query_id(query_id, i, op_info));
//      }
//      for (int64_t i = 0; i < TEST_MAX_OPERATOR_COUNT; i++) {
//        EXPECT_EQ(OB_ENTRY_NOT_EXIST, monitor_mgr_.get_monitor_info_by_plan_id(plan_id, i, op_info));
//        EXPECT_EQ(OB_ENTRY_NOT_EXIST, monitor_mgr_.get_avg_monitor_info(plan_id, i, op_info));
//      }
//    }
//  }
//  while (query_id >= TEST_MAX_EXECUTOR_COUNT - TEST_MAX_MONITOR_COUNT
//         && query_id < TEST_MAX_EXECUTOR_COUNT)
//  {
//    query_id ++;
//    int64_t plan_id = query_id / N;
//    if (plan_id != 0) {
//      for (int64_t i = 0; i < TEST_MAX_OPERATOR_COUNT; i++) {
//        EXPECT_EQ(OB_SUCCESS, monitor_mgr_.get_monitor_info_by_query_id(query_id, i, op_info));
//        EXPECT_EQ(OB_SUCCESS, TestGetByQueryId::do_check_operator_info(query_id, op_info));
//      }
//      for (int64_t i = 0; i < TEST_MAX_OPERATOR_COUNT; i++) {
//        EXPECT_EQ(OB_SUCCESS, monitor_mgr_.get_monitor_info_by_plan_id(plan_id, i, op_info));
//        EXPECT_EQ(OB_SUCCESS, TestGetByPlanId::do_check_operator_info(plan_id, op_info));
//        EXPECT_EQ(OB_SUCCESS, monitor_mgr_.get_avg_monitor_info(plan_id, i, op_info));
//        EXPECT_EQ(OB_SUCCESS, TestGetByPlanId::do_check_avg_operator_info(plan_id, op_info));
//      }
//    }
//  }
//  return ret;
//}

int TestMonitorInfoManager::build_monitor_info(int64_t query_id, int64_t plan_id, ObPhyPlanMonitorInfo *info)
{
  int ret = OB_SUCCESS;
  info->set_plan_id(plan_id);
  ObPhyOperatorMonitorInfo op_info;
  for (int64_t i = 0; i < OB_MAX_INFORMATION_COUNT; i++) {
    if (0 == i % 2) {
      op_info.set_value(static_cast<ObOperatorMonitorInfoIds>(i), query_id);
    } else {
      op_info.set_value(static_cast<ObOperatorMonitorInfoIds>(i), 0);
    }
  }
  for (int64_t i = 0; i < TEST_MAX_OPERATOR_COUNT && OB_SUCC(ret); i++) {
    op_info.set_operator_id(i);
    EXPECT_EQ(OB_SUCCESS, info->add_operator_info(op_info));
  }
  return ret;
}
//TEST_F(TestMonitorInfoManager, stress)
//{
//  do_stress();
//}

TEST_F(TestMonitorInfoManager, simple_add_and_get)
{
  do_add_and_check();
  //do_gc();
}
TEST_F(TestMonitorInfoManager, test_serialize)
{
  //ObPhyOperatorMonitorInfo op_info;
  const int64_t buf_len = 1024;
  char buf[buf_len];
  int64_t pos = 0;
  //EXPECT_EQ(OB_SUCCESS, op_info.serialize(buf, buf_len, pos));
  //EXPECT_EQ(pos, op_info.get_serialize_size());
  ObPhyOperatorMonitorInfo dest_info;
  int64_t data_len = pos;
  pos = 0;
  int ret = OB_SUCCESS;
  //int64_t ret = dest_info.deserialize(buf, data_len, pos);
  //EXPECT_EQ(OB_SUCCESS, ret);
  //LOG_INFO("output dest info", K(dest_info));
  ObPhyOperatorMonitorInfo op_info2;

  op_info2.set_job_id(10);
  op_info2.set_task_id(11);
  op_info2.set_operator_id(12);
  for (int64_t i = 0; i < OB_MAX_INFORMATION_COUNT; i++) {
    if (i % 2 == 0) {
      op_info2.set_value(static_cast<ObOperatorMonitorInfoIds>(i), i);
    } else {
      op_info2.set_value(static_cast<ObOperatorMonitorInfoIds>(i), 0);
    }
  }
  EXPECT_EQ(OB_SUCCESS, op_info2.serialize(buf, buf_len, pos));
  data_len = pos;
  pos = 0;
  ret = dest_info.deserialize(buf, data_len, pos);
  EXPECT_EQ(OB_SUCCESS, ret);
  for (int64_t i = 0; i < OB_MAX_INFORMATION_COUNT; i++) {
    int64_t value = 0;
    dest_info.get_value(static_cast<ObOperatorMonitorInfoIds>(i), value);
    if (value != 0) {
      EXPECT_EQ(i, value);
    }
  }
  LOG_INFO("output dest info", K(dest_info));
}
TEST_F(TestMonitorInfoManager, test_dispatch)
{
  ObConcurrentFIFOAllocator allocator;
  ObArenaAllocator arena_allocator;
  ObPhyPlanMonitorInfo plan_info(allocator);
  int64_t query_id = 10;
  int64_t plan_id = 15;
  EXPECT_EQ(OB_SUCCESS, build_monitor_info(query_id, plan_id, &plan_info));
  ObExecStatCollector collector;
  int64_t job_id = 12;
  int64_t task_id = 13;
  EXPECT_EQ(OB_SUCCESS, collector.collect_plan_monitor_info(job_id, task_id, &plan_info));
  ObString extend_buf;
  EXPECT_EQ(OB_SUCCESS, collector.get_extend_info(arena_allocator, extend_buf));
  ObPhyPlanMonitorInfo dest_plan_info(allocator);
  ObExecStatDispatch dispatch;
  ObArenaAllocator alloc;
  ObExecContext ctx(alloc);
  ObPhysicalPlan plan;
  EXPECT_EQ(OB_SUCCESS, dispatch.set_extend_info(extend_buf));
  EXPECT_EQ(OB_SUCCESS, dispatch.dispatch(true, &dest_plan_info, false, &plan));
  for (int64_t i = 0; i < TEST_MAX_OPERATOR_COUNT; i++) {
    ObPhyOperatorMonitorInfo *op_info = NULL;
    EXPECT_EQ(OB_SUCCESS, dest_plan_info.get_operator_info_by_index(i, op_info));
    for (int64_t i = 0; i < OB_MAX_INFORMATION_COUNT; i++) {
      int64_t value = 0;
      op_info->get_value(static_cast<ObOperatorMonitorInfoIds>(i), value);
      if (value != 0) {
        EXPECT_EQ(query_id, value);
      }
    }
  }
}

//TEST_F(TestMonitorInfoManager, tets_duplicated)
//{
//  ObMonitorInfoManager monitor_mgr;
//  monitor_mgr.init(0);
//  ObConcurrentFIFOAllocator allocator;
//  ObPhyPlanMonitorInfo plan_info(allocator);
//  ObPhyPlanMonitorInfo plan_info1(allocator);
//  ObPhyPlanMonitorInfo plan_info2(allocator);
//  int64_t time = 1 * 60 * 1000 * 1000;
//  monitor_mgr_.set_max_push_interval(time);
//  int64_t query_id = 10;
//  int64_t plan_id = 15;
//  EXPECT_EQ(OB_SUCCESS, build_monitor_info(query_id, plan_id, &plan_info));
//  EXPECT_EQ(OB_SUCCESS, monitor_mgr.add_monitor_info(&plan_info));
//  EXPECT_EQ(1, monitor_mgr.slow_query_queue_.get_count());
//  bool is_duplicated = false;
//  EXPECT_EQ(OB_SUCCESS, build_monitor_info(query_id, plan_id, &plan_info1));
//  EXPECT_EQ(OB_SUCCESS, monitor_mgr_.is_info_nearly_duplicated(plan_info1.get_plan_id(), is_duplicated));
//  EXPECT_TRUE(is_duplicated);
//  EXPECT_EQ(OB_SUCCESS, monitor_mgr_.add_monitor_info(&plan_info1));
//  EXPECT_EQ(1, monitor_mgr_.slow_query_queue_.get_count());
//  EXPECT_EQ(1, monitor_mgr_.plan_execution_time_map_.count());
//  plan_id = 30;
//  EXPECT_EQ(OB_SUCCESS, build_monitor_info(query_id, plan_id, &plan_info2));
//  EXPECT_EQ(OB_SUCCESS, monitor_mgr_.add_monitor_info(&plan_info2));
//  EXPECT_EQ(2, monitor_mgr_.slow_query_queue_.get_count());
//  EXPECT_EQ(2, monitor_mgr_.plan_execution_time_map_.count());
//  time = 1;
//  monitor_mgr_.set_max_push_interval(time);
//  plan_id = 15;
//  EXPECT_EQ(OB_SUCCESS, monitor_mgr_.is_info_nearly_duplicated(plan_info1.get_plan_id(), is_duplicated));
//  EXPECT_FALSE(is_duplicated);
//  EXPECT_EQ(OB_SUCCESS, monitor_mgr_.add_monitor_info(&plan_info1));
//  EXPECT_EQ(3, monitor_mgr_.slow_query_queue_.get_count());
//  EXPECT_EQ(2, monitor_mgr_.plan_execution_time_map_.count());
//  monitor_mgr_.set_max_push_interval(time);
//  monitor_mgr_.reclain_map();
//  EXPECT_EQ(0, monitor_mgr_.plan_execution_time_map_.count());
//}
}
}
int main(int argc, char *argv[])
{
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_monitor.log", true);
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
