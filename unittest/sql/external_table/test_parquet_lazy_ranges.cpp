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
  ASSERT_EQ(OB_SUCCESS, iter.rg_skip_ranges_.push_back(0));
  ASSERT_EQ(OB_SUCCESS, iter.rg_read_ranges_.push_back(1));
  ASSERT_EQ(OB_SUCCESS, iter.rg_skip_ranges_.push_back(1));
  ASSERT_EQ(OB_SUCCESS, iter.rg_read_ranges_.push_back(1));

  ASSERT_EQ(OB_SUCCESS, iter.fill_lazy_ranges(filter));
  ASSERT_EQ(2, iter.lazy_skip_ranges_.count());
  ASSERT_EQ(2, iter.lazy_read_ranges_.count());
  EXPECT_EQ(0, iter.lazy_skip_ranges_.at(0));
  EXPECT_EQ(1, iter.lazy_read_ranges_.at(0));
  EXPECT_EQ(1, iter.lazy_skip_ranges_.at(1));
  EXPECT_EQ(1, iter.lazy_read_ranges_.at(1));
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
  ASSERT_EQ(OB_SUCCESS, iter.rg_skip_ranges_.push_back(0));
  ASSERT_EQ(OB_SUCCESS, iter.rg_read_ranges_.push_back(2));
  ASSERT_EQ(OB_SUCCESS, iter.rg_skip_ranges_.push_back(2));
  ASSERT_EQ(OB_SUCCESS, iter.rg_read_ranges_.push_back(2));
  ASSERT_EQ(OB_SUCCESS, iter.rg_skip_ranges_.push_back(1));
  ASSERT_EQ(OB_SUCCESS, iter.rg_read_ranges_.push_back(1));

  ASSERT_EQ(OB_SUCCESS, iter.fill_lazy_ranges(filter));
  ASSERT_EQ(3, iter.lazy_skip_ranges_.count());
  ASSERT_EQ(3, iter.lazy_read_ranges_.count());
  EXPECT_EQ(0, iter.lazy_skip_ranges_.at(0));
  EXPECT_EQ(2, iter.lazy_read_ranges_.at(0));
  EXPECT_EQ(2, iter.lazy_skip_ranges_.at(1));
  EXPECT_EQ(1, iter.lazy_read_ranges_.at(1));
  EXPECT_EQ(2, iter.lazy_skip_ranges_.at(2));
  EXPECT_EQ(1, iter.lazy_read_ranges_.at(2));
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
