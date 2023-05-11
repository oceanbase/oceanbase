// Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.

#include <gtest/gtest.h>
// #include "ob_mock_dup_table_ls_mgr.h"
#include "storage/tx/ob_dup_table_lease.h"

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
