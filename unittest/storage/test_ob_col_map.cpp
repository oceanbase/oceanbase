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

#include "storage/ob_col_map.h"
#include "lib/allocator/page_arena.h"
#include <gtest/gtest.h>
#include <gmock/gmock.h>

namespace oceanbase
{
using namespace common;
using namespace storage;

namespace unittest
{
void test_col_map(const int64_t col_count)
{
  int ret = OB_SUCCESS;
  ObColMap col_map;

  ret = col_map.init(col_count);
  ASSERT_EQ(OB_SUCCESS, ret);

  for (int64_t i = 0; i < col_count; i++) {
    ret = col_map.set_refactored(static_cast<uint64_t>(i), col_count - i);
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  int64_t col_idx = 0;
  for (int64_t i = col_count - 1; OB_SUCC(ret) && i >= 0; i--) {
    ret = col_map.get_refactored(static_cast<uint64_t>(i), col_idx);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(col_idx, col_count - i);
  }

  if (col_count < 1024 /* FIRST_LEVEL_MAX_COL_NUM */) {
    ASSERT_EQ(NULL, col_map.get(1023));
  } else if (col_count < 65535 /* FINAL_LEVEL_MAX_COL_NUM */) {
    ASSERT_EQ(NULL, col_map.get(65534));
  }
}

TEST(TestObColMap, test_col_map_first_level)
{
  // less than FIRST_LEVEL_MAX_COL_NUM = 1024
  int64_t col_count = 752;
  test_col_map(col_count);
}

TEST(TestObColMap, test_col_map_final_level)
{
  // greater than FIRST_LEVEL_MAX_COL_NUM = 1024
  int64_t col_count = 12281;
  test_col_map(col_count);
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_ob_col_map.log");
  OB_LOGGER.set_log_level("WARN");
  CLOG_LOG(INFO, "begin unittest: test_ob_col_map");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
