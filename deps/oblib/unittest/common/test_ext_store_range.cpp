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
#include "common/ob_store_range.h"
using namespace oceanbase::common;

TEST(TestExtStoreRange, test_collation_free)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;

  ObObj int_cell;
  int_cell.set_int(1);
  ObStoreRowkey int_key(&int_cell, 1);

  const char* varchar = "key";
  ObObj varchar_cell;
  varchar_cell.set_varchar(varchar);
  varchar_cell.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  varchar_cell.set_collation_level(CS_LEVEL_IMPLICIT);
  ObStoreRowkey varchar_key(&varchar_cell, 1);

  ObStoreRange range;
  range.set_start_key(int_key);
  range.set_end_key(varchar_key);
  ObExtStoreRange ext_range(range);

  ASSERT_FALSE(ext_range.get_collation_free_start_key().is_valid());
  ASSERT_FALSE(ext_range.get_collation_free_end_key().is_valid());
  ret = ext_range.to_collation_free_range_on_demand(allocator);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_FALSE(ext_range.get_collation_free_start_key().is_valid());
  ASSERT_TRUE(ext_range.get_collation_free_end_key().is_valid());
}

TEST(TestExtStoreRange, test_serialize_and_deserialize)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  const int buf_size = 1024;
  char buf[buf_size];
  int64_t pos = 0;
  ObExtStoreRange dst_ext_range;

  ObObj int_cell;
  int_cell.set_int(1);
  ObStoreRowkey int_key(&int_cell, 1);

  const char* varchar = "key";
  ObObj varchar_cell;
  varchar_cell.set_varchar(varchar);
  varchar_cell.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  varchar_cell.set_collation_level(CS_LEVEL_IMPLICIT);
  ObStoreRowkey varchar_key(&varchar_cell, 1);

  ObStoreRange range;
  range.set_start_key(int_key);
  range.set_end_key(varchar_key);
  ObExtStoreRange ext_range(range);

  pos = 0;
  ret = ext_range.serialize(buf, buf_size, pos);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(pos, ext_range.get_serialize_size());

  pos = 0;
  ret = dst_ext_range.deserialize(allocator, buf, buf_size, pos);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(pos, ext_range.get_serialize_size());
  ASSERT_TRUE(dst_ext_range.get_range().is_valid());
  ASSERT_FALSE(dst_ext_range.get_collation_free_start_key().is_valid());
  ASSERT_FALSE(dst_ext_range.get_collation_free_end_key().is_valid());

  ret = dst_ext_range.to_collation_free_range_on_demand(allocator);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_FALSE(dst_ext_range.get_collation_free_start_key().is_valid());
  ASSERT_TRUE(dst_ext_range.get_collation_free_end_key().is_valid());
}

int main(int argc, char** argv)
{
  testing::InitGoogleTest(&argc, argv);
  OB_LOGGER.set_log_level("INFO");
  return RUN_ALL_TESTS();
}
