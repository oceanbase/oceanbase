/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of the License at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * either express or implied, including but not limited to non-infringement,
 * merchantability or fit for a particular purpose.
 */

#include <gtest/gtest.h>

#include "ob_log_instance_hash_mode.h"

namespace oceanbase
{
namespace libobcdc
{

TEST(TestLogInstanceHashMode, parse_and_print)
{
  EXPECT_EQ(INSTANCE_HASH_BY_LS, parse_instance_hash_mode("LS"));
  EXPECT_EQ(INSTANCE_HASH_BY_LS, parse_instance_hash_mode("ls"));
  EXPECT_EQ(INSTANCE_HASH_BY_TABLE, parse_instance_hash_mode("TABLE"));
  EXPECT_EQ(INSTANCE_HASH_BY_TABLE, parse_instance_hash_mode("table"));
  EXPECT_EQ(INSTANCE_HASH_BY_TABLET, parse_instance_hash_mode("TABLET"));
  EXPECT_EQ(INSTANCE_HASH_BY_TABLET, parse_instance_hash_mode("tablet"));
  EXPECT_EQ(INSTANCE_HASH_BY_TABLET, parse_instance_hash_mode("TaBlEt"));
  EXPECT_EQ(INVALID_INSTANCE_HASH_MODE, parse_instance_hash_mode(NULL));
  EXPECT_EQ(INVALID_INSTANCE_HASH_MODE, parse_instance_hash_mode("TABLE_ID"));
  EXPECT_EQ(INVALID_INSTANCE_HASH_MODE, parse_instance_hash_mode(""));
  EXPECT_EQ(INVALID_INSTANCE_HASH_MODE, parse_instance_hash_mode("TAB"));
  EXPECT_EQ(INVALID_INSTANCE_HASH_MODE, parse_instance_hash_mode("TABLETS"));
  EXPECT_EQ(INVALID_INSTANCE_HASH_MODE, parse_instance_hash_mode("TABLE "));

  EXPECT_STREQ("LS", print_instance_hash_mode(INSTANCE_HASH_BY_LS));
  EXPECT_STREQ("TABLE", print_instance_hash_mode(INSTANCE_HASH_BY_TABLE));
  EXPECT_STREQ("TABLET", print_instance_hash_mode(INSTANCE_HASH_BY_TABLET));
  EXPECT_STREQ("INVALID", print_instance_hash_mode(INVALID_INSTANCE_HASH_MODE));
  EXPECT_TRUE(is_instance_hash_mode_valid(INSTANCE_HASH_BY_LS));
  EXPECT_TRUE(is_instance_hash_mode_valid(INSTANCE_HASH_BY_TABLE));
  EXPECT_TRUE(is_instance_hash_mode_valid(INSTANCE_HASH_BY_TABLET));
  EXPECT_FALSE(is_instance_hash_mode_valid(INVALID_INSTANCE_HASH_MODE));
  EXPECT_FALSE(is_table_or_tablet_hash_mode(INVALID_INSTANCE_HASH_MODE));
  EXPECT_FALSE(is_table_or_tablet_hash_mode(INSTANCE_HASH_BY_LS));
  EXPECT_TRUE(is_table_or_tablet_hash_mode(INSTANCE_HASH_BY_TABLE));
  EXPECT_TRUE(is_table_or_tablet_hash_mode(INSTANCE_HASH_BY_TABLET));
}

TEST(TestLogInstanceHashMode, unique_owner_for_all_bucket_counts)
{
  const int64_t instance_counts[] = {1, 2, 3, 4, 7, 16, 63, 64};
  const uint64_t ids[] = {0, 1, 2, 1001, 2002, UINT64_MAX - 1, UINT64_MAX};
  for (const int64_t instance_num : instance_counts) {
    for (const uint64_t id : ids) {
      int64_t table_owner_count = 0;
      int64_t tablet_owner_count = 0;
      for (int64_t index = 0; index < instance_num; ++index) {
        const bool expected = id % instance_num == static_cast<uint64_t>(index);
        const bool table_served = is_table_served_by_instance(id, instance_num, index);
        const bool tablet_served = is_tablet_served_by_instance(id, instance_num, index);
        EXPECT_EQ(expected, table_served);
        EXPECT_EQ(expected, tablet_served);
        table_owner_count += table_served;
        tablet_owner_count += tablet_served;
      }
      EXPECT_EQ(1, table_owner_count);
      EXPECT_EQ(1, tablet_owner_count);
    }
  }
}

TEST(TestLogInstanceHashMode, route_by_table)
{
  const uint64_t table_id = 1001;

  EXPECT_FALSE(is_table_served_by_instance(table_id, 3, 0));
  EXPECT_TRUE(is_table_served_by_instance(table_id, 3, 2));
  EXPECT_TRUE(is_table_served_by_instance(1002, 3, 0));
}

TEST(TestLogInstanceHashMode, route_by_tablet)
{
  EXPECT_TRUE(is_tablet_served_by_instance(2001, 3, 0));
  EXPECT_FALSE(is_tablet_served_by_instance(2002, 3, 0));
  EXPECT_TRUE(is_tablet_served_by_instance(2002, 3, 1));

  // Power-of-two instance counts use the mask fast path.
  EXPECT_TRUE(is_tablet_served_by_instance(2002, 2, 0));
  EXPECT_FALSE(is_tablet_served_by_instance(2002, 2, 1));
}

} // namespace libobcdc
} // namespace oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
