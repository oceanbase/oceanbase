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
#include "common/rowkey/ob_store_rowkey.h"
using namespace oceanbase::common;

TEST(TestExtStoreRowkey, test_collation_free)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  bool need_transform = false;
  bool use_collation_free = false;

  ObObj int_cell;
  int_cell.set_int(1);
  ObStoreRowkey int_key(&int_cell, 1);
  ObExtStoreRowkey int_ext_key(int_key);
  ret = int_ext_key.need_transform_to_collation_free(need_transform);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_FALSE(need_transform);
  ret = int_ext_key.check_use_collation_free(false, use_collation_free);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_FALSE(use_collation_free);
  ret = int_ext_key.to_collation_free_store_rowkey_on_demand(allocator);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_FALSE(int_ext_key.get_collation_free_store_rowkey().is_valid());
  ret = int_ext_key.to_collation_free_store_rowkey(allocator);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(int_ext_key.get_collation_free_store_rowkey().is_valid());
  ret = int_ext_key.check_use_collation_free(false, use_collation_free);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_FALSE(use_collation_free);

  const char* varchar = "key";
  ObObj varchar_cell;
  varchar_cell.set_varchar(varchar);
  varchar_cell.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  varchar_cell.set_collation_level(CS_LEVEL_IMPLICIT);
  ObStoreRowkey varchar_key(&varchar_cell, 1);
  ObExtStoreRowkey varchar_ext_key(varchar_key);
  ret = varchar_ext_key.need_transform_to_collation_free(need_transform);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(need_transform);
  ret = varchar_ext_key.check_use_collation_free(false, use_collation_free);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_FALSE(use_collation_free);
  ret = varchar_ext_key.to_collation_free_store_rowkey_on_demand(allocator);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(varchar_ext_key.get_collation_free_store_rowkey().is_valid());
  ret = varchar_ext_key.check_use_collation_free(false, use_collation_free);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(use_collation_free);
}

TEST(TestExtStoreRowkey, test_serialize_and_deserialize)
{
  int ret = OB_SUCCESS;
  const int buf_size = 1024;
  char buf[buf_size];
  int64_t pos = 0;
  ObExtStoreRowkey dst_key;
  ObArenaAllocator allocator;

  ObObj int_cell;
  int_cell.set_int(1);
  ObStoreRowkey int_key(&int_cell, 1);
  ObExtStoreRowkey int_ext_key(int_key);
  ret = int_ext_key.serialize(buf, buf_size, pos);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(pos, int_ext_key.get_serialize_size());

  pos = 0;
  ret = dst_key.deserialize(buf, buf_size, pos);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(pos, int_ext_key.get_serialize_size());
  ASSERT_TRUE(dst_key.get_store_rowkey().simple_equal(int_ext_key.get_store_rowkey()));
  ASSERT_FALSE(dst_key.get_collation_free_store_rowkey().is_valid());

  dst_key.reset();
  pos = 0;
  ret = dst_key.deserialize(allocator, buf, buf_size, pos);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(pos, int_ext_key.get_serialize_size());
  ASSERT_TRUE(dst_key.get_store_rowkey().simple_equal(int_ext_key.get_store_rowkey()));
  ASSERT_FALSE(dst_key.get_collation_free_store_rowkey().is_valid());

  const char* varchar = "key";
  ObObj varchar_cell;
  varchar_cell.set_varchar(varchar);
  varchar_cell.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  varchar_cell.set_collation_level(CS_LEVEL_IMPLICIT);
  ObStoreRowkey varchar_key(&varchar_cell, 1);
  ObExtStoreRowkey varchar_ext_key(varchar_key);
  varchar_ext_key.to_collation_free_store_rowkey(allocator);

  pos = 0;
  ret = varchar_ext_key.serialize(buf, buf_size, pos);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(pos, varchar_ext_key.get_serialize_size());

  pos = 0;
  dst_key.reset();
  ret = dst_key.deserialize(buf, buf_size, pos);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(pos, varchar_ext_key.get_serialize_size());
  ASSERT_TRUE(dst_key.get_store_rowkey().simple_equal(varchar_ext_key.get_store_rowkey()));
  ASSERT_FALSE(dst_key.get_collation_free_store_rowkey().is_valid());
  dst_key.to_collation_free_store_rowkey(allocator);
  ASSERT_TRUE(dst_key.get_collation_free_store_rowkey().is_valid());
  ASSERT_TRUE(
      dst_key.get_collation_free_store_rowkey().simple_equal(varchar_ext_key.get_collation_free_store_rowkey()));

  dst_key.reset();
  pos = 0;
  ret = dst_key.deserialize(allocator, buf, buf_size, pos);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(pos, varchar_ext_key.get_serialize_size());
  ASSERT_TRUE(dst_key.get_store_rowkey().simple_equal(varchar_ext_key.get_store_rowkey()));
  ASSERT_FALSE(dst_key.get_collation_free_store_rowkey().is_valid());
  dst_key.to_collation_free_store_rowkey(allocator);
  ASSERT_TRUE(dst_key.get_collation_free_store_rowkey().is_valid());
  ASSERT_TRUE(
      dst_key.get_collation_free_store_rowkey().simple_equal(varchar_ext_key.get_collation_free_store_rowkey()));
}

int main(int argc, char** argv)
{
  testing::InitGoogleTest(&argc, argv);
  OB_LOGGER.set_log_level("INFO");
  return RUN_ALL_TESTS();
}
