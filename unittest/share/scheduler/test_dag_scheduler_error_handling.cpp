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

#define USING_LOG_PREFIX TEST
#include <getopt.h>
#include <gtest/gtest.h>
#define protected public
#define private public
#include "share/scheduler/test_dag_common.h"

int64_t dag_cnt = 1;
int64_t stress_time= 1; // 100ms
char log_level[20] = "INFO";
uint32_t time_slice = 1000;

namespace oceanbase
{
using namespace common;
using namespace lib;
using namespace share;
using namespace omt;
namespace unittest
{

TEST_F(TestDagScheduler, test_error_handling)
{
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler*);
  ASSERT_TRUE(nullptr != scheduler);
  ASSERT_EQ(OB_SUCCESS, scheduler->init(MTL_ID(), time_slice));

  AtomicOperator op(0);
  TestDag *dag = NULL;
  AtomicMulTask *mul_task = NULL;
  AtomicIncTask *inc_task = NULL;
  AtomicIncTask *inc_task2 = NULL;
  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(dag));
  EXPECT_EQ(OB_SUCCESS, dag->init(1));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag, mul_task));
  EXPECT_EQ(OB_SUCCESS, mul_task->init(1, 10, op, 0, 8));
  EXPECT_EQ(OB_SUCCESS, dag->add_task(*mul_task));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag, inc_task));
  EXPECT_EQ(OB_SUCCESS, inc_task->init(1, 10, op));
  EXPECT_EQ(OB_SUCCESS, mul_task->add_child(*inc_task));
  EXPECT_EQ(OB_SUCCESS, dag->add_task(*inc_task));
  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(dag));
  wait_scheduler();
  EXPECT_EQ(0, op.value());

  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(dag));
  EXPECT_EQ(OB_SUCCESS, dag->init(1));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag, mul_task));
  EXPECT_EQ(OB_SUCCESS, mul_task->init(1, 1, op, 0, 1));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag, inc_task));
  EXPECT_EQ(OB_SUCCESS, inc_task->init(1, 10, op));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag, inc_task2));
  EXPECT_EQ(OB_SUCCESS, inc_task2->init(1, 10, op));

  EXPECT_EQ(OB_SUCCESS, inc_task->add_child(*mul_task));
  EXPECT_EQ(OB_SUCCESS, mul_task->add_child(*inc_task2));

  EXPECT_EQ(OB_SUCCESS, dag->add_task(*inc_task));
  EXPECT_EQ(OB_SUCCESS, dag->add_task(*mul_task));
  EXPECT_EQ(OB_SUCCESS, dag->add_task(*inc_task2));
  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(dag));
  wait_scheduler();
  EXPECT_EQ(10, op.value());

  TestDag *dag1 = NULL;
  TestPrepareTask *prepare_task = NULL;
  op.reset();
  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(dag1));
  EXPECT_EQ(OB_SUCCESS, dag1->init(1));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag1, prepare_task));
  EXPECT_EQ(OB_SUCCESS, prepare_task->init(1, &op, true));
  EXPECT_EQ(OB_SUCCESS, dag1->add_task(*prepare_task));
  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(dag1));
  wait_scheduler();
  EXPECT_EQ(0, op.value());

  op.reset();
  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(dag1));
  EXPECT_EQ(OB_SUCCESS, dag1->init(1));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag1, prepare_task));
  EXPECT_EQ(OB_SUCCESS, prepare_task->init(1, &op, true, 1000*1000));
  EXPECT_EQ(OB_SUCCESS, dag1->add_task(*prepare_task));
  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(dag1));
  wait_scheduler();
  EXPECT_EQ(0, op.value());
}


}
}

void parse_cmd_arg(int argc, char **argv)
{
  int opt = 0;
  const char *opt_string = "p:s:l:";

  struct option longopts[] = {
      {"dag cnt for performance test", 1, NULL, 'p'},
      {"stress test time", 1, NULL, 's'},
      {"log level", 1, NULL, 'l'},
      {0,0,0,0} };

  while (-1 != (opt = getopt_long(argc, argv, opt_string, longopts, NULL))) {
    switch(opt) {
    case 'p':
      dag_cnt = strtoll(optarg, NULL, 10);
      break;
    case 's':
      stress_time = strtoll(optarg, NULL, 10);
      break;
    case 'l':
      snprintf(log_level, 20, "%s", optarg);
      break;
    default:
      break;
    }
  }
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  parse_cmd_arg(argc, argv);
  OB_LOGGER.set_log_level(log_level);
  OB_LOGGER.set_max_file_size(256*1024*1024);
  system("rm -f test_dag_scheduler_error_handling.log*");
  OB_LOGGER.set_file_name("test_dag_scheduler_error_handling.log");
  return RUN_ALL_TESTS();
}
