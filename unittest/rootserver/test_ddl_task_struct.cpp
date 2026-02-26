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
#define USING_LOG_PREFIX SERVER
#define protected public
#define private public

#include "rootserver/ddl_task/ob_ddl_task.h"

#define LOG_FILE_PATH "./test_ddl_task_struct.log"

namespace oceanbase
{
namespace unittest
{

using namespace oceanbase::common;
using namespace oceanbase::storage;
using namespace oceanbase::rootserver;

#define ASSERT_SUCC(expr) ASSERT_EQ(OB_SUCCESS, (expr))

TEST(test_ddl_task_struct, ddl_slice_info)
{
  // 1. default construct
  ObDDLSliceInfo slice_info;
  ASSERT_FALSE(slice_info.is_valid());

  // 2. push back an tablet range
  sql::ObPxTabletRange tablet_range;
  tablet_range.tablet_id_ = 0;
  ASSERT_SUCC(slice_info.part_ranges_.push_back(tablet_range));
  ASSERT_TRUE(slice_info.is_valid());

  // 3. test assign
  ObDDLSliceInfo assign_slice_info;
  tablet_range.tablet_id_ = 1;
  sql::ObPxTabletRange::DatumKey endkey;
  char test_string[32] = { 0 };
  const char *hello_str = "hello oceanbase";
  memcpy(test_string, hello_str, strlen(hello_str));
  ASSERT_SUCC(endkey.prepare_allocate(2));
  char datum_buffer[16] = { 0 };
  endkey.at(0).ptr_ = datum_buffer;
  endkey.at(0).set_int(100);
  endkey.at(1).ptr_ = datum_buffer + 8;
  endkey.at(1).set_string(test_string, sizeof(test_string));
  ASSERT_SUCC(tablet_range.range_cut_.push_back(endkey));
  ASSERT_SUCC(slice_info.part_ranges_.push_back(tablet_range));
  ASSERT_SUCC(assign_slice_info.assign(slice_info));
  ASSERT_TRUE(assign_slice_info.part_ranges_.count() == 2);

  // 4. test deep copy
  ObDDLSliceInfo deep_copied_slice_info;
  ObArenaAllocator arena;
  ASSERT_SUCC(deep_copied_slice_info.deep_copy(slice_info, arena));
  ASSERT_TRUE(assign_slice_info.part_ranges_.count() == 2);
  memset(test_string, 0, sizeof(test_string));
  LOG_INFO("after deep copy", K(slice_info), K(deep_copied_slice_info), K(assign_slice_info));
  ASSERT_EQ(0, strcmp(deep_copied_slice_info.part_ranges_.at(1).range_cut_.at(0).at(1).get_string().ptr(), hello_str));
  ASSERT_NE(0, strcmp(     assign_slice_info.part_ranges_.at(1).range_cut_.at(0).at(1).get_string().ptr(), hello_str));

  // 5. reset
  slice_info.reset();
  ASSERT_FALSE(slice_info.is_valid());
}

}
}

int main(int argc, char **argv)
{
  system("rm -rf " LOG_FILE_PATH "*");
  oceanbase::common::ObLogger::get_logger().set_log_level("WDIAG");
  oceanbase::common::ObLogger::get_logger().set_file_name(LOG_FILE_PATH, true);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
