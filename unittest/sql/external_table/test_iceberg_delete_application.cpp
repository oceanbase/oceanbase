/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>

#include "lib/allocator/page_arena.h"
#include "lib/container/ob_bitmap.h"
#include "share/datum/ob_datum.h"
#include "sql/engine/table/ob_external_table_access_service.h"

namespace oceanbase
{
namespace sql
{
namespace unittest
{

class ObExternalTableRowIteratorTestAccessor : public ObExternalTableRowIterator
{
public:
  using ObExternalTableRowIterator::compact_selected_rows;
};

TEST(TestIcebergDeleteApplication, compact_fragmented_physical_line_numbers)
{
  const int64_t row_count = 128;
  ObArenaAllocator allocator(ObModIds::TEST);
  ObBitmap selection(allocator);
  ASSERT_EQ(OB_SUCCESS, selection.init(row_count, false));

  int64_t line_numbers[row_count];
  int64_t expected[row_count];
  int64_t expected_count = 0;
  for (int64_t i = 0; i < row_count; ++i) {
    line_numbers[i] = 10000 + i;
    // More than the fast-path run threshold, matching a fragmented delete selection.
    if (i % 5 < 2) {
      ASSERT_EQ(OB_SUCCESS, selection.set(i));
      expected[expected_count++] = line_numbers[i];
    }
  }

  int64_t compacted_count = -1;
  ASSERT_EQ(OB_SUCCESS,
            ObExternalTableRowIteratorTestAccessor::compact_selected_rows(
                selection, row_count, line_numbers, compacted_count));
  ASSERT_EQ(expected_count, compacted_count);
  for (int64_t i = 0; i < compacted_count; ++i) {
    EXPECT_EQ(expected[i], line_numbers[i]) << "row " << i;
  }
}

TEST(TestIcebergDeleteApplication, compact_legacy_physical_line_number_datums)
{
  const int64_t row_count = 8;
  ObArenaAllocator allocator(ObModIds::TEST);
  ObBitmap selection(allocator);
  ASSERT_EQ(OB_SUCCESS, selection.init(row_count, false));

  const int64_t selected_rows[] = {0, 3, 4, 7};
  for (int64_t i = 0; i < ARRAYSIZEOF(selected_rows); ++i) {
    ASSERT_EQ(OB_SUCCESS, selection.set(selected_rows[i]));
  }

  int64_t datum_storage[row_count];
  ObDatum datums[row_count];
  for (int64_t i = 0; i < row_count; ++i) {
    datums[i].ptr_ = reinterpret_cast<const char *>(&datum_storage[i]);
    datums[i].set_int(20000 + i);
  }

  int64_t compacted_count = -1;
  ASSERT_EQ(OB_SUCCESS,
            ObExternalTableRowIteratorTestAccessor::compact_selected_rows(
                selection, row_count, datums, compacted_count));
  ASSERT_EQ(ARRAYSIZEOF(selected_rows), compacted_count);
  for (int64_t i = 0; i < compacted_count; ++i) {
    EXPECT_EQ(20000 + selected_rows[i], datums[i].get_int()) << "row " << i;
  }
}

} // namespace unittest
} // namespace sql
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("TRACE");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
