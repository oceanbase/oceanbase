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
#include "lib/allocator/page_arena.h"
#include "sql/ob_sql_init.h"
#include "create_op_util.h"
//#include "sql/engine/basic/ob_project.h"
//#include "sql/engine/join/ob_merge_join.h"
//#include "sql/engine/dml/ob_when_filter.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/executor/ob_root_transmit.h"
#include "sql/executor/ob_direct_receive.h"
#include "sql/engine/table/ob_table_scan.h"
#include "sql/executor/ob_job_parser.h"
#include "sql/executor/ob_task_spliter_factory.h"
#include "sql/plan_cache/ob_cache_object_factory.h"
using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql;

class ObJobParserPerfTest : public ::testing::Test
{
  public:
    ObJobParserPerfTest();
    virtual ~ObJobParserPerfTest();
    virtual void SetUp();
    virtual void TearDown();
    static void print_phy_op_tree(char *buf, const int64_t buf_len, int64_t& pos, ObPhyOperator *op);
    static void print_sub_phy_op_tree(char *buf, const int64_t buf_len, int64_t& pos, ObPhyOperator *op);
    static void print_job_tree(char *buf, const int64_t buf_len, int64_t& pos, ObJobControl *jc);
  private:
    // disallow copy
    ObJobParserPerfTest(const ObJobParserPerfTest &other);
    ObJobParserPerfTest& operator=(const ObJobParserPerfTest &ohter);
  private:
    static void print_op_tree(char *buf, const int64_t buf_len, int64_t& pos, ObPhyOperator *op, int32_t level);
    static void print_sub_op_tree(char *buf, const int64_t buf_len, int64_t& pos, ObPhyOperator *op, int32_t level);
};


ObJobParserPerfTest::ObJobParserPerfTest()
{
}

ObJobParserPerfTest::~ObJobParserPerfTest()
{
}

void ObJobParserPerfTest::SetUp()
{
}

void ObJobParserPerfTest::TearDown()
{
}

void ObJobParserPerfTest::print_phy_op_tree(char *buf, const int64_t buf_len, int64_t& pos, ObPhyOperator *op)
{
  ObJobParserPerfTest::print_op_tree(buf, buf_len, pos, op, 0);
  buf[pos] = '\0';
}

void ObJobParserPerfTest::print_op_tree(char *buf, const int64_t buf_len, int64_t& pos, ObPhyOperator *op, int32_t level)
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
      ObJobParserPerfTest::print_op_tree(buf, buf_len, pos, op->get_child(i), level + 1);
    }
  }
}

void ObJobParserPerfTest::print_job_tree(char *buf, const int64_t buf_len, int64_t& pos, ObJobControl *jc)
{
  ::oceanbase::common::databuff_printf(buf, buf_len, pos, "total jobs: %ld\n", jc->get_job_count());
  ObArray<ObJob *> jobs;
  jc->get_all_jobs(jobs);
  for (int64_t i = 0; i < jobs.count(); ++i)
  {
    ObJob *job = jobs.at(i);
    ::oceanbase::common::databuff_printf(buf, buf_len, pos, "job_id=%ld\n", i);
    ObJobParserPerfTest::print_sub_phy_op_tree(buf, buf_len, pos, job->get_root_op());
  }
  buf[pos] = '\0';
}

void ObJobParserPerfTest::print_sub_phy_op_tree(char *buf, const int64_t buf_len, int64_t& pos, ObPhyOperator *op)
{
  ObJobParserPerfTest::print_sub_op_tree(buf, buf_len, pos, op, 0);
  buf[pos] = '\0';
}

void ObJobParserPerfTest::print_sub_op_tree(char *buf, const int64_t buf_len, int64_t& pos, ObPhyOperator *op, int32_t level)
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
        ObJobParserPerfTest::print_sub_op_tree(buf, buf_len, pos, op->get_child(i), level + 1);
      }
    }
  }
}

TEST_F(ObJobParserPerfTest, basic_test)
{
  ObPhysicalPlan tmp_plan;
  UNUSED(tmp_plan);
  ObPhysicalPlan *physical_plan =NULL;
  ObCacheObjectFactory::alloc(physical_plan);
  ObPhyOperator *cur_op = NULL;
  ObPhyOperator *tmp_op = NULL;

  ASSERT_FALSE(NULL == physical_plan);
  ASSERT_EQ(OB_SUCCESS, physical_plan->alloc_operator_by_type(PHY_TABLE_SCAN, tmp_op));
  static_cast<ObTransmit*>(tmp_op)->get_job_conf().set_task_split_type(ObTaskSpliter::LOCAL_IDENTITY_SPLIT);
  cur_op = tmp_op;

  physical_plan->set_main_query(cur_op);

  char buf[3000];
  int64_t pos = 0;
  ObJobParserPerfTest::print_phy_op_tree(buf, 3000 - 1, pos, cur_op);
  _OB_LOG(INFO, "physical operator tree:\n%s", buf);

  int counter = 0;
  int64_t sum = 0;
  while(counter < 10000) {
    counter++;
    ObExecContext exec_ctx;
    exec_ctx.init_phy_op(1);
    exec_ctx.create_physical_plan_ctx();
    ObLocalJobControl jc;
    ObAddrsProviderFactory spf;
    ObTaskSpliterFactory tsf;
    ObJobParser parser;
    ObExecutionID execution_id;
    int64_t start = ::oceanbase::common::ObTimeUtility::current_time();
    ASSERT_EQ(OB_SUCCESS, parser.parse_job(exec_ctx, physical_plan, execution_id, tsf, spf, jc));
    int64_t end = ::oceanbase::common::ObTimeUtility::current_time();
    sum += (end - start);

    ASSERT_EQ(1, jc.get_job_count());
//    ObJob *tmp_job = NULL;
    ObArray<ObJob *> jobs;
//    jc.get_all_jobs(jobs);
//    tmp_job = jobs.at(0);
//    ASSERT_EQ(1, tmp_job->get_priority());

    jobs.reset();
    jc.get_ready_jobs(jobs);
    ASSERT_EQ(1, jobs.count());
//    ASSERT_EQ(1, jobs.at(0)->get_priority());
  }
  _OB_LOG(INFO, "time cost %ld", sum * 1000 / counter);
}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  init_sql_factories();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
