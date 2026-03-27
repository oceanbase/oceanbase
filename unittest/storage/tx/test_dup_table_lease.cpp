// Copyright (c) 2021 OceanBase
// SPDX-License-Identifier: Apache-2.0

#include <gtest/gtest.h>
#include "deps/oblib/src/lib/hash/ob_hashmap.h"

namespace oceanbase
{

namespace unittest
{

class TestDupTableLease : public ::testing::Test
{
public:
  virtual void SetUp() {}
  virtual void TearDown() {}
public:
};

TEST_F(TestDupTableLease, test_dup_table_lease_log)
{

}

}

} // namespace oceanbase

using namespace oceanbase;

int main(int argc, char **argv)
{
  int ret = 1;
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_dup_table_lease.log", true);
  logger.set_log_level(OB_LOG_LEVEL_INFO);
  testing::InitGoogleTest(&argc, argv);
  ret = RUN_ALL_TESTS();
  return ret;
}
