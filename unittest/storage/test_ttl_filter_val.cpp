/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include "storage/compaction_ttl/ob_ttl_filter_info_array.h"
#include "storage/compaction_ttl/ob_ttl_filter_val.h"

namespace oceanbase
{
namespace storage
{
TEST(TestTTLFilterVal, sort_preserves_column_value_pairs)
{
  common::ObArenaAllocator allocator;
  ObTTLFilterInfoArray infos;
  ASSERT_EQ(OB_SUCCESS, infos.init_for_first_creation(allocator));
  ObTTLFilterVal values;
  ASSERT_EQ(OB_SUCCESS, values.init(infos));
  EXPECT_EQ(0, values.count());

  const int64_t columns[] = {8, 2, 5, 2, 0};
  for (int64_t i = 0; i < ARRAYSIZEOF(columns); ++i) {
    ObTTLFilterInfo info;
    info.key_.tx_id_ = i + 1;
    info.commit_version_ = 100 + i;
    info.ttl_filter_col_type_ = ObTTLFilterColType::INT64;
    info.ttl_filter_col_idx_ = columns[i];
    info.ttl_filter_value_ = i + 1;
    ASSERT_EQ(OB_SUCCESS, infos.append_with_deep_copy(info));
    values.reset();
    ASSERT_EQ(OB_SUCCESS, values.init(infos));
    ASSERT_EQ(i + 1, values.count());
    int64_t seen = 0;
    for (int64_t j = 0; j < values.count(); ++j) {
      const auto &pair = values.at(j);
      ASSERT_GE(pair.filter_val_, 1);
      ASSERT_LE(pair.filter_val_, i + 1);
      EXPECT_EQ(columns[pair.filter_val_ - 1], pair.col_idx_);
      EXPECT_EQ(0, seen & (1L << pair.filter_val_));
      seen |= 1L << pair.filter_val_;
      if (j > 0) {
        EXPECT_LE(values.at(j - 1).col_idx_, pair.col_idx_);
      }
    }
  }
}
} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
