/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "share/ob_cluster_version.h"
#include "share/schema/ob_table_param.h"
#include "storage/blocksstable/ob_datum_range.h"
#include <gtest/gtest.h>

namespace oceanbase {
namespace blocksstable {

TEST(ObDatumRangeTest, group_idx_round_trip) {
  common::ObClusterVersion &cluster_version = common::ObClusterVersion::get_instance();
  const uint64_t original_cluster_version = cluster_version.get_cluster_version();
  cluster_version.update_cluster_version(CLUSTER_VERSION_4_3_2_0);

  ObDatumRange range;
  const int64_t expected_group_idx = (static_cast<int64_t>(7) << 32) | 2;
  range.set_group_idx(expected_group_idx);
  const int64_t actual_group_idx = range.get_group_idx();

  cluster_version.update_cluster_version(original_cluster_version);
  EXPECT_EQ(expected_group_idx, actual_group_idx);
}

} // namespace blocksstable
} // namespace oceanbase

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
