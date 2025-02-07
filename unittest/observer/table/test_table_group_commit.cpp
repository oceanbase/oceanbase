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
