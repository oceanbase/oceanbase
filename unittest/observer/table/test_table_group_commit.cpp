/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#define private public  // 获取private成员
#define protected public  // 获取protect成员
#include "observer/table/group/ob_table_tenant_group.h"

using namespace oceanbase::common;
using namespace oceanbase::table;

class TestTableGroupCommit: public ::testing::Test
{
public:
  static const int64_t DEFAULT_MAX_GROUP_SIZE;
  static const int64_t DEFAULT_MIN_GROUP_SIZE;
  TestTableGroupCommit() {};
  virtual ~TestTableGroupCommit() {}
public:
  ObTableGroupCommitMgr mgr_;
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestTableGroupCommit);
};

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_table_group.log", true);
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
