/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>

#define USING_LOG_PREFIX SQL_ENG
#define private public
#include "sql/engine/table/ob_parquet_table_row_iter.h"
#undef private

namespace oceanbase
{
namespace sql
{
namespace unittest
{

class TestFilterExecutor : public ObPushdownFilterExecutor
{
public:
  explicit TestFilterExecutor(common::ObIAllocator &allocator)
      : ObPushdownFilterExecutor(allocator, nullptr), col_ids_()
  {
  }

  common::ObIArray<uint64_t> &get_col_ids() override
  {
    return col_ids_;
  }

private:
  common::ObSEArray<uint64_t, 1> col_ids_;
};

TEST(TestParquetLazyRanges, FlushReadRangeBeforePositionDeleteGap)
{
  common::ObArenaAllocator allocator;
  TestFilterExecutor filter(allocator);
  common::ObBitmap *filter_result = nullptr;
  ASSERT_EQ(OB_SUCCESS, filter.init_bitmap(2, filter_result));
  ASSERT_NE(nullptr, filter_result);
  ASSERT_EQ(OB_SUCCESS, filter_result->set(0));
  ASSERT_EQ(OB_SUCCESS, filter_result->set(1));

  ObParquetTableRowIterator iter;
  ObParquetTableRowIterator::BatchReadState batch(allocator);
  ASSERT_EQ(OB_SUCCESS, batch.source_plan_.mutable_skip_ranges().push_back(0));
  ASSERT_EQ(OB_SUCCESS, batch.source_plan_.mutable_read_ranges().push_back(1));
  ASSERT_EQ(OB_SUCCESS, batch.source_plan_.mutable_skip_ranges().push_back(1));
  ASSERT_EQ(OB_SUCCESS, batch.source_plan_.mutable_read_ranges().push_back(1));

  ASSERT_EQ(OB_SUCCESS, iter.fill_selected_source_ranges(batch, *filter_result));
  ASSERT_EQ(2, batch.lazy_plan_.skip_ranges().count());
  ASSERT_EQ(2, batch.lazy_plan_.read_ranges().count());
  EXPECT_EQ(0, batch.lazy_plan_.skip_ranges().at(0));
  EXPECT_EQ(1, batch.lazy_plan_.read_ranges().at(0));
  EXPECT_EQ(1, batch.lazy_plan_.skip_ranges().at(1));
  EXPECT_EQ(1, batch.lazy_plan_.read_ranges().at(1));
}

TEST(TestParquetLazyRanges, PreserveFilterSkipsAcrossDeleteGaps)
{
  common::ObArenaAllocator allocator;
  TestFilterExecutor filter(allocator);
  common::ObBitmap *filter_result = nullptr;
  ASSERT_EQ(OB_SUCCESS, filter.init_bitmap(5, filter_result));
  ASSERT_NE(nullptr, filter_result);
  ASSERT_EQ(OB_SUCCESS, filter_result->set(0));
  ASSERT_EQ(OB_SUCCESS, filter_result->set(1));
  ASSERT_EQ(OB_SUCCESS, filter_result->set(2));
  ASSERT_EQ(OB_SUCCESS, filter_result->set(4));

  ObParquetTableRowIterator iter;
  ObParquetTableRowIterator::BatchReadState batch(allocator);
  ASSERT_EQ(OB_SUCCESS, batch.source_plan_.mutable_skip_ranges().push_back(0));
  ASSERT_EQ(OB_SUCCESS, batch.source_plan_.mutable_read_ranges().push_back(2));
  ASSERT_EQ(OB_SUCCESS, batch.source_plan_.mutable_skip_ranges().push_back(2));
  ASSERT_EQ(OB_SUCCESS, batch.source_plan_.mutable_read_ranges().push_back(2));
  ASSERT_EQ(OB_SUCCESS, batch.source_plan_.mutable_skip_ranges().push_back(1));
  ASSERT_EQ(OB_SUCCESS, batch.source_plan_.mutable_read_ranges().push_back(1));

  ASSERT_EQ(OB_SUCCESS, iter.fill_selected_source_ranges(batch, *filter_result));
  ASSERT_EQ(3, batch.lazy_plan_.skip_ranges().count());
  ASSERT_EQ(3, batch.lazy_plan_.read_ranges().count());
  EXPECT_EQ(0, batch.lazy_plan_.skip_ranges().at(0));
  EXPECT_EQ(2, batch.lazy_plan_.read_ranges().at(0));
  EXPECT_EQ(2, batch.lazy_plan_.skip_ranges().at(1));
  EXPECT_EQ(1, batch.lazy_plan_.read_ranges().at(1));
  EXPECT_EQ(2, batch.lazy_plan_.skip_ranges().at(2));
  EXPECT_EQ(1, batch.lazy_plan_.read_ranges().at(2));
}

TEST(TestParquetLazyRanges, DynamicModeFollowsCumulativeSelectivity)
{
  ObParquetTableRowIterator iter;
  iter.mode_ = FilterCalcMode::DYNAMIC_EAGER_CALC;
  iter.stat_.projected_eager_cnt_ = 100;
  iter.stat_.projected_lazy_cnt_ = 67;

  iter.dynamic_switch_calc_mode();
  EXPECT_EQ(FilterCalcMode::DYNAMIC_LAZY_CALC, iter.mode_);

  // Model another row group whose low pass rate lowers the cumulative ratio to 50%.
  iter.stat_.projected_eager_cnt_ = 200;
  iter.stat_.projected_lazy_cnt_ = 100;
  iter.dynamic_switch_calc_mode();
  EXPECT_EQ(FilterCalcMode::DYNAMIC_EAGER_CALC, iter.mode_);

  iter.mode_ = FilterCalcMode::FORCE_LAZY_CALC;
  iter.stat_.projected_eager_cnt_ = 300;
  iter.stat_.projected_lazy_cnt_ = 10;
  iter.dynamic_switch_calc_mode();
  EXPECT_EQ(FilterCalcMode::FORCE_LAZY_CALC, iter.mode_);
}

} // namespace unittest
} // namespace sql
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
