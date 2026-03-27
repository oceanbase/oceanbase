/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "storage/memtable/mvcc/ob_mvcc_row.h"

#include <gtest/gtest.h>

namespace oceanbase
{
namespace unittest
{
using namespace oceanbase::common;
using namespace oceanbase::memtable;

TEST(TestObMemtableValue, smoke_test)
{
  ObMvccRow mr;

  ObCStringHelper helper;
  fprintf(stdout, "mr=[%s]\n", helper.convert(mr));
  fprintf(stdout, "mr=[%s]\n", helper.convert(mr, true));
}

}
}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_file_name("test_mvcc_row.log", true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
