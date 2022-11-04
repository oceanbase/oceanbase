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
#include "ob_mock_utils.h"
#include "lib/allocator/page_arena.h"
#include "sql/ob_sql_init.h"
//#include "sql/engine/basic/ob_project.h"
#include "sql/engine/sort/ob_sort.h"
//#include "sql/engine/join/ob_merge_join.h"
//#include "sql/engine/dml/ob_when_filter.h"
#include "sql/executor/ob_root_transmit.h"
#include "sql/executor/ob_direct_receive.h"
#include "sql/engine/table/ob_table_scan.h"
#include "sql/executor/ob_distributed_job_control.h"
#include "sql/executor/ob_job_parser.h"
#include "sql/executor/ob_task_spliter_factory.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql;

class ObJobParserTest : public ::testing::Test
{
  public:
    ObJobParserTest();
    virtual ~ObJobParserTest();
    virtual void SetUp();
    virtual void TearDown();
    static void print_phy_op_tree(char *buf, const int64_t buf_len, int64_t& pos, ObPhyOperator *op);
    static void print_sub_phy_op_tree(char *buf, const int64_t buf_len, int64_t& pos, ObPhyOperator *op);
    static void print_job_tree(char *buf, const int64_t buf_len, int64_t& pos, ObJobControl *jc);
  private:
    // disallow copy
    ObJobParserTest(const ObJobParserTest &other);
    ObJobParserTest& operator=(const ObJobParserTest &ohter);
  private:
    static void print_op_tree(char *buf, const int64_t buf_len, int64_t& pos, ObPhyOperator *op, int32_t level);
    static void print_sub_op_tree(char *buf, const int64_t buf_len, int64_t& pos, ObPhyOperator *op, int32_t level);
};


ObJobParserTest::ObJobParserTest()
{
}

ObJobParserTest::~ObJobParserTest()
{
}

void ObJobParserTest::SetUp()
{
}

void ObJobParserTest::TearDown()
{
}

void ObJobParserTest::print_phy_op_tree(char *buf, const int64_t buf_len, int64_t& pos, ObPhyOperator *op)
{
  ObJobParserTest::print_op_tree(buf, buf_len, pos, op, 0);
  buf[pos] = '\0';
}

void ObJobParserTest::print_op_tree(char *buf, const int64_t buf_len, int64_t& pos, ObPhyOperator *op, int32_t level)
{
  const char* op_name = NULL;
  if (NULL != op)
  {
    op_name = ob_phy_operator_type_str(op->get_type());
  }
  for (int32_t i = 0; i < level; i++)
  {
    ::oceanbase::common::databuff_printf(buf, buf_len, pos, " ");
  }
  ::oceanbase::common::databuff_printf(buf, buf_len, pos, "|- %s\n", op_name);
  if (NULL != op)
  {
    for (int32_t i = 0; i < op->get_child_num(); i++)
    {
      ObJobParserTest::print_op_tree(buf, buf_len, pos, op->get_child(i), level + 1);
    }
  }
}

void ObJobParserTest::print_job_tree(char *buf, const int64_t buf_len, int64_t& pos, ObJobControl *jc)
{
  ::oceanbase::common::databuff_printf(buf, buf_len, pos, "total jobs: %ld\n", jc->get_job_count());
  ObArray<ObJob *> jobs;
  jc->get_all_jobs(jobs);
  for (int64_t i = 0; i < jobs.count(); ++i)
  {
    ObJob *job = jobs.at(i);
    ::oceanbase::common::databuff_printf(buf, buf_len, pos, "job_id=%ld, priority=%ld:\n", i, job->get_priority());
    ObJobParserTest::print_sub_phy_op_tree(buf, buf_len, pos, job->get_root_op());
  }
  buf[pos] = '\0';
}

void ObJobParserTest::print_sub_phy_op_tree(char *buf, const int64_t buf_len, int64_t& pos, ObPhyOperator *op)
{
  ObJobParserTest::print_sub_op_tree(buf, buf_len, pos, op, 0);
  buf[pos] = '\0';
}

void ObJobParserTest::print_sub_op_tree(char *buf, const int64_t buf_len, int64_t& pos, ObPhyOperator *op, int32_t level)
{
  const char* op_name = NULL;
  if (NULL != op)
  {
    op_name = ob_phy_operator_type_str(op->get_type());
  }
  for (int32_t i = 0; i < level; i++)
  {
    ::oceanbase::common::databuff_printf(buf, buf_len, pos, " ");
  }
  ::oceanbase::common::databuff_printf(buf, buf_len, pos, "|- %s\n", op_name);
  if (NULL != op)
  {
    for (int32_t i = 0; i < op->get_child_num(); i++)
    {
      ObTransmit* trans_op = dynamic_cast<ObTransmit*>(op->get_child(i));
      if (NULL == trans_op)
      {
        ObJobParserTest::print_sub_op_tree(buf, buf_len, pos, op->get_child(i), level + 1);
      }
    }
  }
}

TEST_F(ObJobParserTest, basic_test)
{
  ObPhysicalPlan *physical_plan = ObPhysicalPlan::alloc();
  ObPhyOperator *cur_op = NULL;
  ObPhyOperator *tmp_op = NULL;
  int err_code = OB_SUCCESS;

  TEST_CREATE_PHY_OPERATOR(tmp_op, ObTableScan, PHY_TABLE_SCAN, physical_plan, err_code);
  cur_op = tmp_op;

  TEST_CREATE_PHY_OPERATOR(tmp_op, ObSort, PHY_SORT, physical_plan, err_code);
  tmp_op->set_child(0, *cur_op);
  cur_op = tmp_op;

  TEST_CREATE_PHY_OPERATOR(tmp_op, ObSort, PHY_SORT, physical_plan, err_code);
  tmp_op->set_child(0, *cur_op);
  cur_op = tmp_op;

  TEST_CREATE_PHY_OPERATOR(tmp_op, ObRootTransmit, PHY_ROOT_TRANSMIT, physical_plan, err_code);
  tmp_op->set_child(0, *cur_op);
  static_cast<ObTransmit*>(tmp_op)->get_job_conf().set_task_split_type(ObTaskSpliter::IDENTITY_SPLIT);
  cur_op = tmp_op;

  TEST_CREATE_PHY_OPERATOR(tmp_op, ObDirectReceive, PHY_DIRECT_RECEIVE, physical_plan, err_code);
  tmp_op->set_child(0, *cur_op);
  cur_op = tmp_op;

  TEST_CREATE_PHY_OPERATOR(tmp_op, ObSort, PHY_SORT, physical_plan, err_code);
  tmp_op->set_child(0, *cur_op);
  cur_op = tmp_op;

  TEST_CREATE_PHY_OPERATOR(tmp_op, ObRootTransmit, PHY_ROOT_TRANSMIT, physical_plan, err_code);
  tmp_op->set_child(0, *cur_op);
  static_cast<ObTransmit*>(tmp_op)->get_job_conf().set_task_split_type(ObTaskSpliter::IDENTITY_SPLIT);
  cur_op = tmp_op;

  TEST_CREATE_PHY_OPERATOR(tmp_op, ObDirectReceive, PHY_DIRECT_RECEIVE, physical_plan, err_code);
  tmp_op->set_child(0, *cur_op);
  cur_op = tmp_op;

  TEST_CREATE_PHY_OPERATOR(tmp_op, ObRootTransmit, PHY_ROOT_TRANSMIT, physical_plan, err_code);
  tmp_op->set_child(0, *cur_op);
  static_cast<ObTransmit*>(tmp_op)->get_job_conf().set_task_split_type(ObTaskSpliter::IDENTITY_SPLIT);
  cur_op = tmp_op;

  physical_plan->set_main_query(cur_op);

  char buf[3000];
  int64_t pos = 0;
  ObJobParserTest::print_phy_op_tree(buf, 3000 - 1, pos, cur_op);
  _OB_LOG(INFO, "physical operator tree:\n%s", buf);

  ObExecContext exec_ctx;
  exec_ctx.init_phy_op(1);
  exec_ctx.create_physical_plan_ctx();
  ObDistributedJobControl jc;
  ObAddrsProviderFactory spf;
  ObTaskSpliterFactory tsf;
  ObJobParser parser;
  ASSERT_EQ(OB_INVALID_ARGUMENT, parser.parse_job(exec_ctx, NULL, OB_INVALID_ID, tsf, spf, jc));
  ASSERT_EQ(OB_SUCCESS, parser.parse_job(exec_ctx, physical_plan, OB_INVALID_ID, tsf, spf, jc));
  ASSERT_EQ(3, jc.get_job_count());
  ObJob *tmp_job = NULL;
  ObArray<ObJob *> jobs;
  jc.get_all_jobs(jobs);
  tmp_job = jobs.at(0);
  ASSERT_EQ(3, tmp_job->get_priority());
  tmp_job = jobs.at(1);
  ASSERT_EQ(2, tmp_job->get_priority());
  tmp_job = jobs.at(2);
  ASSERT_EQ(1, tmp_job->get_priority());

  char buf1[3000];
  int64_t pos1 = 0;
  ObJobParserTest::print_job_tree(buf1, 3000 - 1, pos1, &jc);
  _OB_LOG(INFO, "job tree:\n%s", buf1);

  jobs.reset();
  jc.get_ready_jobs(jobs);
  ASSERT_EQ(1, jobs.count());
  ASSERT_EQ(3, jobs.at(0)->get_priority());
  //ASSERT_EQ(3, jobs.at(1)->get_priority());
  //ASSERT_EQ(3, jobs.at(2)->get_priority());
  //ASSERT_EQ(3, jobs.at(3)->get_priority());
}

TEST_F(ObJobParserTest, error_test_1)
{
  ObPhysicalPlan *physical_plan = ObPhysicalPlan::alloc();
  ObPhyOperator *cur_op = NULL;
  ObPhyOperator *tmp_op = NULL;
  int err_code = OB_SUCCESS;

  TEST_CREATE_PHY_OPERATOR(tmp_op, ObTableScan, PHY_TABLE_SCAN, physical_plan, err_code);
  cur_op = tmp_op;

  TEST_CREATE_PHY_OPERATOR(tmp_op, ObSort, PHY_SORT, physical_plan, err_code);
  tmp_op->set_child(0, *cur_op);
  cur_op = tmp_op;

  TEST_CREATE_PHY_OPERATOR(tmp_op, ObSort, PHY_SORT, physical_plan, err_code);
  tmp_op->set_child(0, *cur_op);
  cur_op = tmp_op;

  TEST_CREATE_PHY_OPERATOR(tmp_op, ObRootTransmit, PHY_ROOT_TRANSMIT, physical_plan, err_code);
  tmp_op->set_child(0, *cur_op);
  static_cast<ObTransmit*>(tmp_op)->get_job_conf().set_task_split_type(ObTaskSpliter::IDENTITY_SPLIT);
  cur_op = tmp_op;

  TEST_CREATE_PHY_OPERATOR(tmp_op, ObDirectReceive, PHY_DIRECT_RECEIVE, physical_plan, err_code);
  tmp_op->set_child(0, *cur_op);
  cur_op = tmp_op;

  TEST_CREATE_PHY_OPERATOR(tmp_op, ObSort, PHY_SORT, physical_plan, err_code);
  tmp_op->set_child(0, *cur_op);
  cur_op = tmp_op;

  TEST_CREATE_PHY_OPERATOR(tmp_op, ObRootTransmit, PHY_ROOT_TRANSMIT, physical_plan, err_code);
  tmp_op->set_child(0, *cur_op);
  cur_op = tmp_op;

  TEST_CREATE_PHY_OPERATOR(tmp_op, ObDirectReceive, PHY_DIRECT_RECEIVE, physical_plan, err_code);
  tmp_op->set_child(0, *cur_op);
  cur_op = tmp_op;

  TEST_CREATE_PHY_OPERATOR(tmp_op, ObRootTransmit, PHY_ROOT_TRANSMIT, physical_plan, err_code);
  tmp_op->set_child(0, *cur_op);
  static_cast<ObTransmit*>(tmp_op)->get_job_conf().set_task_split_type(ObTaskSpliter::IDENTITY_SPLIT);
  cur_op = tmp_op;

  physical_plan->set_main_query(cur_op);

  char buf[3000];
  int64_t pos = 0;
  ObJobParserTest::print_phy_op_tree(buf, 3000 - 1, pos, cur_op);
  _OB_LOG(INFO, "physical operator tree:\n%s", buf);

  ObExecContext exec_ctx;
  exec_ctx.init_phy_op(1);
  exec_ctx.create_physical_plan_ctx();
  ObDistributedJobControl jc;
  ObAddrsProviderFactory spf;
  ObTaskSpliterFactory tsf;
  ObJobParser parser;
  ASSERT_EQ(OB_INVALID_ARGUMENT, parser.parse_job(exec_ctx, NULL, OB_INVALID_ID, tsf, spf, jc));
  ASSERT_EQ(OB_SUCCESS, parser.parse_job(exec_ctx, physical_plan, OB_INVALID_ID, tsf, spf, jc));
}

TEST_F(ObJobParserTest, error_test_2)
{
  ObPhysicalPlan *physical_plan = ObPhysicalPlan::alloc();
  ObPhyOperator *cur_op = NULL;
  ObPhyOperator *tmp_op = NULL;
  int err_code = OB_SUCCESS;

  TEST_CREATE_PHY_OPERATOR(tmp_op, ObTableScan, PHY_TABLE_SCAN, physical_plan, err_code);
  cur_op = tmp_op;

  TEST_CREATE_PHY_OPERATOR(tmp_op, ObSort, PHY_SORT, physical_plan, err_code);
  tmp_op->set_child(0, *cur_op);
  cur_op = tmp_op;

  TEST_CREATE_PHY_OPERATOR(tmp_op, ObSort, PHY_SORT, physical_plan, err_code);
  tmp_op->set_child(0, *cur_op);
  cur_op = tmp_op;

  TEST_CREATE_PHY_OPERATOR(tmp_op, ObRootTransmit, PHY_ROOT_TRANSMIT, physical_plan, err_code);
  tmp_op->set_child(0, *cur_op);
  static_cast<ObTransmit*>(tmp_op)->get_job_conf().set_task_split_type(ObTaskSpliter::IDENTITY_SPLIT);
  cur_op = tmp_op;

  TEST_CREATE_PHY_OPERATOR(tmp_op, ObDirectReceive, PHY_DIRECT_RECEIVE, physical_plan, err_code);
  tmp_op->set_child(0, *cur_op);
  cur_op = tmp_op;

  TEST_CREATE_PHY_OPERATOR(tmp_op, ObSort, PHY_SORT, physical_plan, err_code);
  tmp_op->set_child(0, *cur_op);
  cur_op = tmp_op;

  TEST_CREATE_PHY_OPERATOR(tmp_op, ObRootTransmit, PHY_ROOT_TRANSMIT, physical_plan, err_code);
  tmp_op->set_child(0, *cur_op);
  static_cast<ObTransmit*>(tmp_op)->get_job_conf().set_task_split_type(ObTaskSpliter::IDENTITY_SPLIT);
  cur_op = tmp_op;

  TEST_CREATE_PHY_OPERATOR(tmp_op, ObDirectReceive, PHY_DIRECT_RECEIVE, physical_plan, err_code);
  tmp_op->set_child(0, *cur_op);
  cur_op = tmp_op;

  TEST_CREATE_PHY_OPERATOR(tmp_op, ObRootTransmit, PHY_ROOT_TRANSMIT, physical_plan, err_code);
  tmp_op->set_child(0, *cur_op);
  static_cast<ObTransmit*>(tmp_op)->get_job_conf().set_task_split_type(ObTaskSpliter::IDENTITY_SPLIT);
  cur_op = tmp_op;

  physical_plan->set_main_query(cur_op);

  char buf[3000];
  int64_t pos = 0;
  ObJobParserTest::print_phy_op_tree(buf, 3000 - 1, pos, cur_op);
  _OB_LOG(INFO, "physical operator tree:\n%s", buf);

  ObExecContext exec_ctx;
  exec_ctx.init_phy_op(1);
  exec_ctx.create_physical_plan_ctx();
  ObDistributedJobControl jc;
  ObAddrsProviderFactory spf;
  ObTaskSpliterFactory tsf;
  ObJobParser parser;
  ASSERT_EQ(OB_INVALID_ARGUMENT, parser.parse_job(exec_ctx, NULL, OB_INVALID_ID, tsf, spf, jc));
  ASSERT_EQ(OB_SUCCESS, parser.parse_job(exec_ctx, physical_plan, OB_INVALID_ID, tsf, spf, jc));
}

TEST_F(ObJobParserTest, error_test_3)
{
  ObPhysicalPlan *physical_plan = ObPhysicalPlan::alloc();
  ObPhyOperator *cur_op = NULL;
  ObPhyOperator *tmp_op = NULL;
  int err_code = OB_SUCCESS;

  TEST_CREATE_PHY_OPERATOR(tmp_op, ObTableScan, PHY_TABLE_SCAN, physical_plan, err_code);
  cur_op = tmp_op;

  TEST_CREATE_PHY_OPERATOR(tmp_op, ObSort, PHY_SORT, physical_plan, err_code);
  tmp_op->set_child(0, *cur_op);
  cur_op = tmp_op;

  TEST_CREATE_PHY_OPERATOR(tmp_op, ObSort, PHY_SORT, physical_plan, err_code);
  tmp_op->set_child(0, *cur_op);
  cur_op = tmp_op;

  TEST_CREATE_PHY_OPERATOR(tmp_op, ObRootTransmit, PHY_ROOT_TRANSMIT, physical_plan, err_code);
  tmp_op->set_child(0, *cur_op);
  static_cast<ObTransmit*>(tmp_op)->get_job_conf().set_task_split_type(ObTaskSpliter::IDENTITY_SPLIT);
  cur_op = tmp_op;

  TEST_CREATE_PHY_OPERATOR(tmp_op, ObDirectReceive, PHY_DIRECT_RECEIVE, physical_plan, err_code);
  tmp_op->set_child(0, *cur_op);
  cur_op = tmp_op;

  TEST_CREATE_PHY_OPERATOR(tmp_op, ObSort, PHY_SORT, physical_plan, err_code);
  tmp_op->set_child(0, *cur_op);
  cur_op = tmp_op;

  TEST_CREATE_PHY_OPERATOR(tmp_op, ObRootTransmit, PHY_ROOT_TRANSMIT, physical_plan, err_code);
  tmp_op->set_child(0, *cur_op);
  static_cast<ObTransmit*>(tmp_op)->get_job_conf().set_task_split_type(ObTaskSpliter::IDENTITY_SPLIT);
  cur_op = tmp_op;

  TEST_CREATE_PHY_OPERATOR(tmp_op, ObDirectReceive, PHY_DIRECT_RECEIVE, physical_plan, err_code);
  tmp_op->set_child(0, *cur_op);
  cur_op = tmp_op;

  TEST_CREATE_PHY_OPERATOR(tmp_op, ObRootTransmit, PHY_ROOT_TRANSMIT, physical_plan, err_code);
  tmp_op->set_child(0, *cur_op);
  static_cast<ObTransmit*>(tmp_op)->get_job_conf().set_task_split_type(ObTaskSpliter::IDENTITY_SPLIT);
  cur_op = tmp_op;

  physical_plan->set_main_query(cur_op);

  char buf[3000];
  int64_t pos = 0;
  ObJobParserTest::print_phy_op_tree(buf, 3000 - 1, pos, cur_op);
  _OB_LOG(INFO, "physical operator tree:\n%s", buf);

  ObExecContext exec_ctx;
  exec_ctx.init_phy_op(1); // actually should be 9, the operator count
  exec_ctx.create_physical_plan_ctx();
  ObDistributedJobControl jc;
  ObAddrsProviderFactory spf;
  ObTaskSpliterFactory tsf;
  ObJobParser parser;
  ASSERT_EQ(OB_INVALID_ARGUMENT, parser.parse_job(exec_ctx, NULL, OB_INVALID_ID, tsf, spf, jc));
  ASSERT_EQ(OB_SUCCESS, parser.parse_job(exec_ctx, physical_plan, OB_INVALID_ID, tsf, spf, jc));
}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  init_sql_factories();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
