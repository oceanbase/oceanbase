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

#include "ob_mock_utils.h"
#include "sql/executor/ob_root_transmit.h"
#include "sql/executor/ob_distributed_transmit.h"
#include "sql/executor/ob_direct_receive.h"
#include "sql/executor/ob_fifo_receive.h"
#include "sql/executor/ob_distributed_job_control.h"
#include "sql/executor/ob_job_parser.h"
#include "sql/executor/ob_task_spliter_factory.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/executor/ob_local_job_control.h"
#include "sql/executor/ob_remote_job_control.h"
#include "sql/executor/ob_distributed_job_control.h"
#include "sql/executor/ob_job.h"
#include "sql/engine/table/ob_table_scan.h"
#include "sql/ob_sql_init.h"
#include "lib/utility/ob_tracepoint.h"
#include <gtest/gtest.h>

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::storage;
using namespace oceanbase::share;

class ObJobControlTest : public ::testing::Test
{
public:
  ObJobControlTest();
  virtual ~ObJobControlTest();
  virtual void SetUp();
  virtual void TearDown();

  ObPhysicalPlan *phy_plan_;

  int create_plan_tree(ObExecContext &ctx);
  void build_input_test(int exepected_ret, ObJobControl &jc);
  void exception_test(int expected_ret, ObJobControl &jc);
  void valid_test(ObJobControl &jc);
private:
  // disallow copy
  ObJobControlTest(const ObJobControlTest &other);
  ObJobControlTest& operator=(const ObJobControlTest &other);
private:
  // data members
};
ObJobControlTest::ObJobControlTest()
{
}

ObJobControlTest::~ObJobControlTest()
{
}

void ObJobControlTest::SetUp()
{
}

void ObJobControlTest::TearDown()
{
}

int ObJobControlTest::create_plan_tree(ObExecContext &ctx)
{
  UNUSED(ctx);

  const static int64_t TEST_PARA_DEGREE = 2;

  int ret = OB_SUCCESS;

  phy_plan_ = ObPhysicalPlan::alloc();
  ObPhyOperator *cur_op = NULL;
  ObPhyOperator *tmp_op = NULL;
  int err_code = OB_SUCCESS;


  /*
   * calculate c0 % TEST_PARA_DEGREE
   * */
  ObSqlExpression *hash_expr = NULL;
  EXPECT_EQ(OB_SUCCESS, ObSqlExpressionUtil::make_sql_expr(phy_plan_, hash_expr));
  EXPECT_FALSE(NULL == hash_expr);
  ObPostExprItem expr_item;
  expr_item.set_column(0);
  EXPECT_EQ(OB_SUCCESS, hash_expr->add_expr_item(expr_item));
  expr_item.set_int(TEST_PARA_DEGREE);
  expr_item.set_item_type(T_INT);
  EXPECT_EQ(OB_SUCCESS, hash_expr->add_expr_item(expr_item));
  expr_item.set_op("%", 2);
  EXPECT_EQ(OB_SUCCESS, hash_expr->add_expr_item(expr_item));

  /*
   * calculate c0 % 1
   * */
  ObSqlExpression *iden_hash_expr = NULL;
  EXPECT_EQ(OB_SUCCESS, ObSqlExpressionUtil::make_sql_expr(phy_plan_, iden_hash_expr));
  EXPECT_FALSE(NULL == iden_hash_expr);
  ObPostExprItem iden_expr_item;
  iden_expr_item.set_column(0);
  EXPECT_EQ(OB_SUCCESS, iden_hash_expr->add_expr_item(iden_expr_item));
  iden_expr_item.set_int(1);
  iden_expr_item.set_item_type(T_INT);
  EXPECT_EQ(OB_SUCCESS, iden_hash_expr->add_expr_item(iden_expr_item));
  iden_expr_item.set_op("%", 2);
  EXPECT_EQ(OB_SUCCESS, iden_hash_expr->add_expr_item(iden_expr_item));


  TEST_CREATE_PHY_OPERATOR(tmp_op, ObTableScan, PHY_TABLE_SCAN, phy_plan_, err_code);
  tmp_op->set_column_count(TEST_MOCK_COL_NUM);
  cur_op = tmp_op;
  SQL_EXE_LOG(INFO, "op info", "op_id", cur_op->get_id(), "op_type", cur_op->get_type());

  TEST_CREATE_PHY_OPERATOR(tmp_op, ObDistributedTransmit, PHY_DISTRIBUTED_TRANSMIT, phy_plan_, err_code);
  tmp_op->set_column_count(TEST_MOCK_COL_NUM);
  tmp_op->set_child(0, *cur_op);
  static_cast<ObTransmit*>(tmp_op)->get_job_conf().set_task_split_type(ObTaskSpliter::IDENTITY_SPLIT);
  //static_cast<ObTransmit*>(tmp_op)->set_parallel_degree(TEST_PARA_DEGREE);
  static_cast<ObTransmit*>(tmp_op)->set_shuffle_func(hash_expr);
  cur_op = tmp_op;
  SQL_EXE_LOG(INFO, "op info", "op_id", cur_op->get_id(), "op_type", cur_op->get_type());

  TEST_CREATE_PHY_OPERATOR(tmp_op, ObFifoReceive, PHY_FIFO_RECEIVE, phy_plan_, err_code);
  tmp_op->set_column_count(TEST_MOCK_COL_NUM);
  tmp_op->set_child(0, *cur_op);
  cur_op = tmp_op;
  SQL_EXE_LOG(INFO, "op info", "op_id", cur_op->get_id(), "op_type", cur_op->get_type());

  TEST_CREATE_PHY_OPERATOR(tmp_op, ObDistributedTransmit, PHY_DISTRIBUTED_TRANSMIT, phy_plan_, err_code);
  tmp_op->set_column_count(TEST_MOCK_COL_NUM);
  tmp_op->set_child(0, *cur_op);
  static_cast<ObTransmit*>(tmp_op)->get_job_conf().set_task_split_type(ObTaskSpliter::INTERM_SPLIT);
  //static_cast<ObTransmit*>(tmp_op)->set_parallel_degree(TEST_PARA_DEGREE);
  static_cast<ObTransmit*>(tmp_op)->set_shuffle_func(iden_hash_expr);
  cur_op = tmp_op;
  SQL_EXE_LOG(INFO, "op info", "op_id", cur_op->get_id(), "op_type", cur_op->get_type());

  TEST_CREATE_PHY_OPERATOR(tmp_op, ObFifoReceive, PHY_FIFO_RECEIVE, phy_plan_, err_code);
  tmp_op->set_column_count(TEST_MOCK_COL_NUM);
  tmp_op->set_child(0, *cur_op);
  cur_op = tmp_op;
  SQL_EXE_LOG(INFO, "op info", "op_id", cur_op->get_id(), "op_type", cur_op->get_type());

  TEST_CREATE_PHY_OPERATOR(tmp_op, ObRootTransmit, PHY_ROOT_TRANSMIT, phy_plan_, err_code);
  tmp_op->set_column_count(TEST_MOCK_COL_NUM);
  tmp_op->set_child(0, *cur_op);
  static_cast<ObTransmit*>(tmp_op)->get_job_conf().set_task_split_type(ObTaskSpliter::IDENTITY_SPLIT);
  //static_cast<ObTransmit*>(tmp_op)->set_parallel_degree(TEST_PARA_DEGREE);
  static_cast<ObTransmit*>(tmp_op)->set_shuffle_func(hash_expr);
  cur_op = tmp_op;
  SQL_EXE_LOG(INFO, "op info", "op_id", cur_op->get_id(), "op_type", cur_op->get_type());

  phy_plan_->set_main_query(cur_op);

  return ret;
}

void ObJobControlTest::build_input_test(int expected_ret, ObJobControl &jc) {
  int ret = OB_SUCCESS;
  ObAddrsProviderFactory spf;
  ObTaskSpliterFactory tsf;
  ObJobParser parser;
  ObExecContext exec_ctx;
  exec_ctx.init_phy_op(100);
  exec_ctx.create_physical_plan_ctx();

  create_plan_tree(exec_ctx);
  if (OB_FAIL(jc.build_jobs_ctx(exec_ctx))) {
    //empty
  } else if (OB_FAIL(parser.parse_job(exec_ctx, phy_plan_, 1, tsf, spf, jc))) {
    //empty
  }
  ASSERT_EQ(expected_ret, ret);
}

void ObJobControlTest::exception_test(int expected_ret, ObJobControl &jc)
{
  int ret = OB_SUCCESS;
  ObJob *job1 = NULL;
  ObJob *job2 = NULL;
  ObJob *job3 = NULL;
  ObJob *tmp_job = NULL;
  ObArray<ObJob *> jobs;
  ObArray<ObJob *> jobs1;
  ObArray<ObJob *> jobs2;
  ObArenaAllocator allocator(ObModIds::OB_SQL_EXEC_CONTEXT);
  int64_t query_id = OB_INVALID_ID;

  if (OB_FAIL(jc.create_job(allocator, query_id, job1))) {
    //empty
  } else if (OB_FAIL(jc.create_job(allocator, query_id, job2))) {
    //empty
  } else if (OB_FAIL(jc.create_job(allocator, query_id, job3))) {
    //empty
  } else {
    job1->set_priority(1);
    job2->set_priority(2);
    job3->set_priority(3);
    if (OB_FAIL(jc.add_job(job1))) {
      //empty
    } else if (OB_FAIL(jc.add_job(job2))) {
      //empty
    } else if (OB_FAIL(jc.add_job(job3))) {
      //empty
    } else if (OB_FAIL(jc.find_job(job1->get_job_id(), tmp_job))) {
      //empty
    } else if (OB_FAIL(jc.get_all_jobs(jobs))) {
      //empty
    } else {
      OB_ASSERT(3 == jobs.count());
      OB_ASSERT(1 == jobs.at(0)->get_priority());
      OB_ASSERT(2 == jobs.at(1)->get_priority());
      OB_ASSERT(3 == jobs.at(2)->get_priority());
      if (OB_FAIL(jc.arrange_jobs())) {
        //empty
      } else if (OB_FAIL(jc.get_all_jobs(jobs1))) {
        //empty
      } else {
        bool b1 = true;
        bool b2 = true;
        OB_ASSERT(3 == jobs1.count());
        OB_ASSERT(3 == jobs1.at(0)->get_priority());
        OB_ASSERT(2 == jobs1.at(1)->get_priority());
        OB_ASSERT(1 == jobs1.at(2)->get_priority());
        if(OB_FAIL(jc.all_jobs_finished(b1))) {
          //empty
        } else if (OB_FAIL(jc.all_jobs_finished_except_root_job(b2))) {
          //empty
        } else {
          OB_ASSERT(false == b1);
          OB_ASSERT(false == b2);
          if (OB_FAIL(jc.get_running_jobs(jobs2))) {
            //empty
          } else {
            OB_ASSERT(0 == jobs2.count());
            jobs1.at(0)->set_state(OB_JOB_STATE_RUNNING);
            OB_ASSERT(OB_SUCCESS == jc.all_jobs_finished(b1));
            OB_ASSERT(OB_SUCCESS == jc.all_jobs_finished_except_root_job(b2));
            OB_ASSERT(false == b1);
            OB_ASSERT(false == b2);
            jobs2.reset();
            if (OB_FAIL(jc.get_running_jobs(jobs2))) {
              //empty
            } else {
              OB_ASSERT(1 == jobs2.count());
              OB_ASSERT(3 == jobs2.at(0)->get_priority());
              jobs1.at(1)->set_state(OB_JOB_STATE_RUNNING);
              OB_ASSERT(OB_SUCCESS == jc.all_jobs_finished(b1));
              OB_ASSERT(OB_SUCCESS == jc.all_jobs_finished_except_root_job(b2));
              OB_ASSERT(false == b1);
              OB_ASSERT(false == b2);
              jobs2.reset();
              if (OB_FAIL(jc.get_running_jobs(jobs2))) {
                //empty
              } else {
                OB_ASSERT(2 == jobs2.count());
                OB_ASSERT(3 == jobs2.at(0)->get_priority());
                OB_ASSERT(2 == jobs2.at(1)->get_priority());
                jobs1.at(2)->set_state(OB_JOB_STATE_RUNNING);
                OB_ASSERT(OB_SUCCESS == jc.all_jobs_finished(b1));
                OB_ASSERT(OB_SUCCESS == jc.all_jobs_finished_except_root_job(b2));
                OB_ASSERT(false == b1);
                OB_ASSERT(false == b2);
                jobs2.reset();
                if (OB_FAIL(jc.get_running_jobs(jobs2))) {
                  //empty
                } else {
                  OB_ASSERT(3 == jobs2.count());
                  OB_ASSERT(3 == jobs2.at(0)->get_priority());
                  OB_ASSERT(2 == jobs2.at(1)->get_priority());
                  OB_ASSERT(1 == jobs2.at(2)->get_priority());
                  jobs1.at(0)->set_state(OB_JOB_STATE_FINISHED);
                  jobs1.at(1)->set_state(OB_JOB_STATE_FINISHED);
                  OB_ASSERT(OB_SUCCESS == jc.all_jobs_finished(b1));
                  OB_ASSERT(OB_SUCCESS == jc.all_jobs_finished_except_root_job(b2));
                  OB_ASSERT(false == b1);
                  OB_ASSERT(true == b2);
                  jobs1.at(2)->set_state(OB_JOB_STATE_FINISHED);
                  OB_ASSERT(OB_SUCCESS == jc.all_jobs_finished(b1));
                  OB_ASSERT(OB_SUCCESS == jc.all_jobs_finished_except_root_job(b2));
                  OB_ASSERT(true == b1);
                  OB_ASSERT(true == b2);
                }
              }
            }
          }
        }
      }
    }
  }
  ASSERT_EQ(expected_ret, ret);
}

void ObJobControlTest::valid_test(ObJobControl &jc)
{
  ObJob *job = NULL;
  ObJob *job2 = NULL;
  bool b = true;
  ObArenaAllocator allocator(ObModIds::OB_SQL_EXEC_CONTEXT);
  int64_t query_id = OB_INVALID_ID;
  OB_ASSERT(OB_SUCCESS == jc.create_job(allocator, query_id, job));
  job->set_root_job();
  OB_ASSERT(true == job->is_root_job());
  OB_ASSERT(OB_ENTRY_NOT_EXIST == jc.find_job(100, job2));
  OB_ASSERT(NULL == job2);
  OB_ASSERT(OB_ENTRY_NOT_EXIST == jc.find_job(job->get_job_id(), job2));
  OB_ASSERT(OB_ERR_UNEXPECTED == jc.all_jobs_finished_except_root_job(b));
  OB_ASSERT(OB_SUCCESS == jc.add_job(job));
  OB_ASSERT(OB_SUCCESS == jc.find_job(job->get_job_id(), job2));
  OB_ASSERT(job2->get_job_id() == job->get_job_id());
  OB_ASSERT(true == job2->is_root_job());
  OB_ASSERT(OB_SUCCESS == jc.all_jobs_finished_except_root_job(b));
}

TEST_F(ObJobControlTest, valid_test)
{
  ObLocalJobControl local_jc;
  ObLocalJobControl remote_jc;
  ObLocalJobControl distributed_jc;
  valid_test(local_jc);
  valid_test(remote_jc);
  valid_test(distributed_jc);
}

TEST_F(ObJobControlTest, build_input_test)
{
  ObLocalJobControl local_jc;
  ObLocalJobControl remote_jc;
  ObLocalJobControl distributed_jc;
  build_input_test(OB_SUCCESS, local_jc);
  build_input_test(OB_SUCCESS, remote_jc);
  build_input_test(OB_SUCCESS, distributed_jc);
}

TEST_F(ObJobControlTest, basic_test)
{
  ObLocalJobControl local_jc;
  ObLocalJobControl remote_jc;
  ObLocalJobControl distributed_jc;
  exception_test(OB_SUCCESS, local_jc);
  exception_test(OB_SUCCESS, remote_jc);
  exception_test(OB_SUCCESS, distributed_jc);
}

#define EXCEPTION_TEST(test_func, test_name, func, key, err, expect_ret) \
          TEST_F(ObJobControlTest, test_name) \
          {\
            ObLocalJobControl local_jc; \
            ObLocalJobControl remote_jc; \
            ObLocalJobControl distributed_jc; \
            TP_SET_ERROR("executor/ob_job_control.cpp", func, key, err); \
            test_func(expect_ret, local_jc); \
            test_func(expect_ret, remote_jc); \
            test_func(expect_ret, distributed_jc); \
            TP_SET_ERROR("executor/ob_job_control.cpp", func, key, NULL); \
          }\

EXCEPTION_TEST(exception_test, et1, "all_jobs_finished", "t1", 1, OB_ERR_UNEXPECTED);
EXCEPTION_TEST(exception_test, et2, "all_jobs_finished_except_root_job", "t1", 1, OB_ERR_UNEXPECTED);
EXCEPTION_TEST(exception_test, et3, "find_job", "t1", 1, OB_ERR_UNEXPECTED);
EXCEPTION_TEST(exception_test, et4, "get_running_jobs", "t1", 1, OB_ERR_UNEXPECTED);
EXCEPTION_TEST(exception_test, et5, "get_all_jobs", "t1", 1, OB_ERR_UNEXPECTED);
EXCEPTION_TEST(exception_test, et6, "create_job", "t1", 1, OB_ALLOCATE_MEMORY_FAILED);
EXCEPTION_TEST(exception_test, et7, "create_job", "t2", 1, OB_ERR_UNEXPECTED);
//EXCEPTION_TEST(build_input_test, et8, "build_job_ctx", "t1", 1, OB_ERR_UNEXPECTED);
EXCEPTION_TEST(build_input_test, et9, "build_job_ctx", "t2", 1, OB_ERR_UNEXPECTED);

int main(int argc, char **argv)
{
  init_sql_factories();
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
