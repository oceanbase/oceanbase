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
#include "sql/engine/ob_physical_plan.h"
#include "sql/monitor/ob_phy_operator_stats.h"
#include "sql/monitor/ob_phy_operator_monitor_info.h"
#include "sql/plan_cache/ob_plan_cache_util.h"
#include "lib/allocator/page_arena.h"
using namespace std;
using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
class TestPhyOperatorStats : public ::testing::Test
{
public:
  TestPhyOperatorStats() {}
  ~TestPhyOperatorStats() {}
  virtual void SetUp() {}
  virtual void TearDown() {}
  static int build_operator_info(int64_t index, ObPhyOperatorMonitorInfo &info);
};

int TestPhyOperatorStats::build_operator_info(int64_t index, ObPhyOperatorMonitorInfo &info)
{
  int ret = OB_SUCCESS;
  info.set_value(INPUT_ROW_COUNT, index + 1);
  info.set_value(OUTPUT_ROW_COUNT, index + 2);
  info.set_value(RESCAN_TIMES, index + 3);
  return ret;
}

TEST_F(TestPhyOperatorStats, init)
{
  ObArenaAllocator alloc;
  int64_t op_count = 5;
  ObPhyOperatorStats stat;
  EXPECT_EQ(OB_SUCCESS, stat.init(&alloc, op_count));
  EXPECT_EQ(stat.count(), 5);
  EXPECT_EQ(stat.array_size_, 5 * (StatId::MAX_STAT * ObPhyOperatorStats::COPY_COUNT));
}

TEST_F(TestPhyOperatorStats, test_add)
{
  ObPhyOperatorStats stats;
  ObArenaAllocator alloc;
  int64_t op_count = 3;
  EXPECT_EQ(OB_SUCCESS, stats.init(&alloc, op_count));
  ObPhyOperatorMonitorInfo op_info;
  for (int64_t i  = 0; i < op_count; i++) {
    op_info.set_operator_id(i);
    EXPECT_EQ(OB_SUCCESS, build_operator_info(i, op_info));
    EXPECT_EQ(OB_SUCCESS, stats.add_op_stat(op_info));
  }
  for (int64_t i  = 0; i < op_count; i++) {
    op_info.set_operator_id(i);
    EXPECT_EQ(OB_SUCCESS, build_operator_info(i, op_info));
    EXPECT_EQ(OB_SUCCESS, stats.add_op_stat(op_info));
  }
  ObOperatorStat stat;
  ObPhysicalPlan plan;
  plan.stat_.execute_times_ = 2;
  //int64_t op_id = 0;
  for (int64_t i = 0; i < op_count; i++) {
    EXPECT_EQ(OB_SUCCESS, stats.get_op_stat_accumulation(&plan, i, stat));
    EXPECT_EQ(stat.input_rows_,  (i + 1)* 2);
    EXPECT_EQ(stat.output_rows_,  (i + 2) * 2);
    EXPECT_EQ(stat.rescan_times_,  (i+3) * 2);
  }
}
}
}
int main(int argc, char *argv[])
{
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_phy_operator.log", true);
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
