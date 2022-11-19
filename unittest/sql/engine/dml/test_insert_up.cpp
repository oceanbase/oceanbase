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
#include "sql/engine/dml/ob_table_insert_up.h"
#include "sql/engine/dml/ob_table_update.h"
#include "sql/engine/dml/ob_table_replace.h"
using namespace oceanbase::common;
namespace oceanbase
{
namespace sql
{
class TestInsertUp : public ::testing::Test
{
public:
  TestInsertUp() {}
  virtual ~TestInsertUp() {}
  virtual void SetUp() {}
  virtual void TearDown() {}
};

TEST_F(TestInsertUp, test_reset)
{
  ObArenaAllocator alloc;
  ObTableInsertUp insert_up(alloc);
  const int64_t COUNT=5;
  EXPECT_EQ(OB_SUCCESS, insert_up.init_scan_column_id_array(COUNT));
  EXPECT_EQ(OB_SUCCESS, insert_up.init_update_related_column_array(COUNT));
  EXPECT_EQ(OB_SUCCESS, insert_up.init_column_ids_count(COUNT));
  EXPECT_EQ(OB_SUCCESS, insert_up.init_column_infos_count(COUNT));
  insert_up.reset();
  insert_up.reuse();
  ObTableUpdate update(alloc);
  EXPECT_EQ(OB_SUCCESS, update.init_updated_column_count(alloc, COUNT));
  EXPECT_EQ(OB_SUCCESS, update.init_column_ids_count(COUNT));
  EXPECT_EQ(OB_SUCCESS, update.init_column_infos_count(COUNT));
  update.reset();
  update.reuse();
  ObTableReplace replace(alloc);
  EXPECT_EQ(OB_SUCCESS, replace.init_column_ids_count(COUNT));
  EXPECT_EQ(OB_SUCCESS, replace.init_column_infos_count(COUNT));
  replace.reset();
  replace.reuse();
}
} //namespace sql
} //namespace oceanbase

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
