/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <iostream>
using namespace std;

#define USING_LOG_PREFIX SQL_EXE
#include <gtest/gtest.h>

#include "sql/ob_sql_init.h"
#define private public
#define protected public
#include "sql/engine/px/exchange/ob_px_transmit_op.h"
#undef private
#undef protected

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::storage;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

class ObRandomAffiTaskSplitTest : public ::testing::Test
{
public:

  const static int64_t TEST_PARTITION_COUNT = 5;
  const static int64_t TEST_SPLIT_TASK_COUNT = 8;

  ObRandomAffiTaskSplitTest() = default;
  virtual ~ObRandomAffiTaskSplitTest() = default;
  virtual void SetUp() {};
  virtual void TearDown() {};

private:
  // disallow copy
  ObRandomAffiTaskSplitTest(const ObRandomAffiTaskSplitTest &other);
  ObRandomAffiTaskSplitTest& operator=(const ObRandomAffiTaskSplitTest &other);
};

TEST_F(ObRandomAffiTaskSplitTest, split_task_test) {
  {
    int64_t parallel = 3;
    int64_t tenant_id = 1;
    ObPxTabletInfo px_part_info;
    ObPxAffinityByRandom affinitize_rule(true, true);
    // 1000, 900, 800, 700, 600 assign to 3 workers
    // worker1 worker2 worker3
    // 1000    900     800
    //         600     700
    for (int i = 0; i < 5; ++i) {
      px_part_info.estimated_row_count_ = (10 - i) * 100;
      affinitize_rule.add_partition(i,i,parallel,tenant_id,px_part_info);
    }
    affinitize_rule.do_random(true, tenant_id);
    const common::ObIArray<ObPxAffinityByRandom::TabletHashValue>& result = affinitize_rule.get_result();
    for (int i = 0; i < result.count(); ++i) {
      LOG_INFO("result", K(result.at(i).tablet_id_), K(result.at(i).worker_id_), K(result.at(i).tablet_info_.estimated_row_count_));
    }
    ASSERT_EQ(0, result.at(0).worker_id_);
    ASSERT_EQ(1, result.at(1).worker_id_);
    ASSERT_EQ(2, result.at(2).worker_id_);
    ASSERT_EQ(2, result.at(3).worker_id_);
    ASSERT_EQ(1, result.at(4).worker_id_);
  }

  {
    int64_t parallel = 16;
    int64_t tenant_id = 1;
    ObPxTabletInfo px_part_info;
    ObPxAffinityByRandom affinitize_rule(true, true);
    px_part_info.estimated_row_count_ = 3000;
    affinitize_rule.add_partition(0,0,parallel,tenant_id,px_part_info);
    px_part_info.estimated_row_count_ = 1000;
    affinitize_rule.add_partition(1,1,parallel,tenant_id,px_part_info);
    px_part_info.estimated_row_count_ = 2500;
    affinitize_rule.add_partition(2,2,parallel,tenant_id,px_part_info);
    px_part_info.estimated_row_count_ = 3500;
    affinitize_rule.add_partition(3,3,parallel,tenant_id,px_part_info);
    px_part_info.estimated_row_count_ = 2000;
    affinitize_rule.add_partition(4,4,parallel,tenant_id,px_part_info);

    affinitize_rule.do_random(true, tenant_id);

    const common::ObIArray<ObPxAffinityByRandom::TabletHashValue>& result = affinitize_rule.get_result();
    for (int i = 0; i < 5; ++i) {
      LOG_INFO("result", K(result.at(i).tablet_id_), K(result.at(i).worker_id_), K(result.at(i).tablet_info_.estimated_row_count_));
    }
    ASSERT_EQ(1, result.at(0).worker_id_);
    ASSERT_EQ(4, result.at(1).worker_id_);
    ASSERT_EQ(2, result.at(2).worker_id_);
    ASSERT_EQ(0, result.at(3).worker_id_);
    ASSERT_EQ(3, result.at(4).worker_id_);
  }

  {
    int64_t parallel = 3;
    int64_t tenant_id = 1;
    ObPxTabletInfo px_part_info;
    ObPxAffinityByRandom affinitize_rule(true, true);
    // 3000, 4000, 2500, 1500, 2000 assign to 3 workers
    // worker1 worker2 worker3
    // 4000    3000    2500
    //         1500    2000


    px_part_info.estimated_row_count_ = 3000;
    affinitize_rule.add_partition(0,0,parallel,tenant_id,px_part_info);
    px_part_info.estimated_row_count_ = 4000;
    affinitize_rule.add_partition(1,1,parallel,tenant_id,px_part_info);
    px_part_info.estimated_row_count_ = 2500;
    affinitize_rule.add_partition(2,2,parallel,tenant_id,px_part_info);
    px_part_info.estimated_row_count_ = 1500;
    affinitize_rule.add_partition(3,3,parallel,tenant_id,px_part_info);
    px_part_info.estimated_row_count_ = 2000;
    affinitize_rule.add_partition(4,4,parallel,tenant_id,px_part_info);

    affinitize_rule.do_random(true, tenant_id);

    const common::ObIArray<ObPxAffinityByRandom::TabletHashValue>& result = affinitize_rule.get_result();
    for (int i = 0; i < 5; ++i) {
      LOG_INFO("result", K(result.at(i).tablet_id_), K(result.at(i).worker_id_), K(result.at(i).tablet_info_.estimated_row_count_));
    }

    ASSERT_EQ(1, result.at(0).worker_id_);
    ASSERT_EQ(0, result.at(1).worker_id_);
    ASSERT_EQ(2, result.at(2).worker_id_);
    ASSERT_EQ(1, result.at(3).worker_id_);
    ASSERT_EQ(2, result.at(4).worker_id_);
 }

}

TEST_F(ObRandomAffiTaskSplitTest, px_transmit_fallback_offset_exceeds_uint16)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObPxTransmitOp::VectorSendParams params(allocator);
  const int64_t row_count = 104;
  const int64_t fallback_slice_count = 640;
  void *buf = allocator.alloc(row_count * sizeof(*params.fallback_array_));
  ASSERT_NE(nullptr, buf);
  params.fallback_array_ = static_cast<decltype(params.fallback_array_)>(buf);

  // The first row takes the direct-send path. Each of the remaining 103 rows
  // falls back to 640 channels, so the final cumulative offset is 65920.
  params.fallback_cnt_ = 0;
  params.fallback_array_[0] = params.fallback_cnt_;
  for (int64_t i = 1; i < row_count; ++i) {
    params.fallback_cnt_ += fallback_slice_count;
    params.fallback_array_[i] = params.fallback_cnt_;
  }

  ASSERT_EQ(65920, params.fallback_cnt_);
  EXPECT_EQ(params.fallback_cnt_, static_cast<int64_t>(params.fallback_array_[row_count - 1]));
  EXPECT_EQ(fallback_slice_count,
            static_cast<int64_t>(params.fallback_array_[row_count - 1])
                - static_cast<int64_t>(params.fallback_array_[row_count - 2]));
}

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("TRACE");
  //oceanbase::common::ObLogger::get_logger().set_log_level("TRACE");
  init_sql_factories();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
