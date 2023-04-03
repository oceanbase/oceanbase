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

TEST(TestObRange, test_multi_version_range)
{
  ObArenaAllocator allocator;
  ObStoreRowkey start_key;
  ObStoreRowkey end_key;
  ObStoreRange range;
  ObStoreRowkey res_start_key;
  ObStoreRowkey res_end_key;
  ObStoreRange res_range;
  ObObj start_cells[2];
  ObObj end_cells[2];
  ObObj res_start_cells[3];
  ObObj res_end_cells[3];
  const char *start_var = "key1";
  const char *end_var = "key2";

  ObVersionRange trans_version_range;
  ObStoreRange multi_version_range;
  start_cells[0].set_int(1);
  start_cells[1].set_varchar(start_var);
  start_cells[1].set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  start_cells[1].set_collation_level(CS_LEVEL_IMPLICIT);
  start_key.assign(start_cells, 2);

  ASSERT_EQ(OB_INVALID_ARGUMENT,
      ObVersionStoreRangeConversionHelper::store_rowkey_to_multi_version_range(
      start_key, trans_version_range, allocator, multi_version_range));

  trans_version_range.snapshot_version_ = 10;
  trans_version_range.base_version_ = ObVersionRange::MIN_VERSION;
  trans_version_range.multi_version_start_ = ObVersionRange::MIN_VERSION;
  res_start_cells[0].set_int(1);
  res_start_cells[1].set_varchar(start_var);
  res_start_cells[1].set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  res_start_cells[1].set_collation_level(CS_LEVEL_IMPLICIT);
  res_start_cells[2].set_int(-trans_version_range.snapshot_version_);
  res_start_key.assign(res_start_cells, 3);
  res_end_cells[0].set_int(1);
  res_end_cells[1].set_varchar(start_var);
  res_end_cells[1].set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  res_end_cells[1].set_collation_level(CS_LEVEL_IMPLICIT);
  res_end_cells[2].set_int(0);
  res_end_key.assign(res_end_cells, 3);

  ASSERT_EQ(OB_SUCCESS,
      ObVersionStoreRangeConversionHelper::store_rowkey_to_multi_version_range(
      start_key, trans_version_range, allocator, multi_version_range));
  ASSERT_TRUE(multi_version_range.get_start_key().simple_equal(res_start_key));
  //ASSERT_TRUE(multi_version_range.get_collation_free_start_key().simple_equal(res_start_key.get_collation_free_store_rowkey()));
  ASSERT_TRUE(multi_version_range.get_end_key().simple_equal(res_end_key));
  //ASSERT_TRUE(multi_version_range.get_collation_free_end_key().simple_equal(res_end_key.get_collation_free_store_rowkey()));

  multi_version_range.reset();
  trans_version_range.reset();
  start_cells[0].set_int(1);
  start_cells[1].set_varchar(start_var);
  start_cells[1].set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  start_cells[1].set_collation_level(CS_LEVEL_IMPLICIT);
  start_key.assign(start_cells, 2);

  ASSERT_EQ(OB_INVALID_ARGUMENT,
      ObVersionStoreRangeConversionHelper::store_rowkey_to_multi_version_range(
      start_key, trans_version_range, allocator, multi_version_range));

  trans_version_range.snapshot_version_ = 10;
  trans_version_range.base_version_ = 5;
  trans_version_range.multi_version_start_ = 5;
  res_start_cells[0].set_int(1);
  res_start_cells[1].set_varchar(start_var);
  res_start_cells[1].set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  res_start_cells[1].set_collation_level(CS_LEVEL_IMPLICIT);
  res_start_cells[2].set_int(-trans_version_range.snapshot_version_);
  res_start_key.assign(res_start_cells, 3);
  res_end_cells[0].set_int(1);
  res_end_cells[1].set_varchar(start_var);
  res_end_cells[1].set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  res_end_cells[1].set_collation_level(CS_LEVEL_IMPLICIT);
  res_end_cells[2].set_int(0);
  res_end_key.assign(res_end_cells, 3);

  ASSERT_EQ(OB_SUCCESS,
      ObVersionStoreRangeConversionHelper::store_rowkey_to_multi_version_range(
      start_key, trans_version_range, allocator, multi_version_range));
  ASSERT_TRUE(multi_version_range.get_start_key().simple_equal(res_start_key));
  //ASSERT_TRUE(multi_version_range.get_collation_free_start_key().simple_equal(res_start_key.get_collation_free_store_rowkey()));
  ASSERT_TRUE(multi_version_range.get_end_key().simple_equal(res_end_key));
  //ASSERT_TRUE(multi_version_range.get_collation_free_end_key().simple_equal(res_end_key.get_collation_free_store_rowkey()));

  multi_version_range.reset();
  trans_version_range.reset();
  end_cells[0].set_int(2);
  end_cells[1].set_varchar(end_var);
  end_cells[1].set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  end_cells[1].set_collation_level(CS_LEVEL_IMPLICIT);
  end_key.assign(end_cells, 2);
  res_end_cells[0].set_int(2);
  res_end_cells[1].set_varchar(end_var);
  res_end_cells[1].set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  res_end_cells[1].set_collation_level(CS_LEVEL_IMPLICIT);
  res_start_cells[2].set_int(0);
  res_end_cells[2].set_int(-INT64_MAX);
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.base_version_ = ObVersionRange::MIN_VERSION;
  trans_version_range.multi_version_start_ = ObVersionRange::MIN_VERSION;

  range.set_start_key(start_key);
  range.set_end_key(end_key);
  range.set_left_open();
  range.set_right_open();

  ASSERT_EQ(OB_SUCCESS,
      ObVersionStoreRangeConversionHelper::range_to_multi_version_range(
      range, trans_version_range, allocator, multi_version_range));
  ASSERT_TRUE(multi_version_range.get_start_key().simple_equal(res_start_key));
  //ASSERT_TRUE(multi_version_range.get_collation_free_start_key().simple_equal(res_start_key.get_collation_free_store_rowkey()));
  ASSERT_TRUE(multi_version_range.get_end_key().simple_equal(res_end_key));
  //ASSERT_TRUE(multi_version_range.get_collation_free_end_key().simple_equal(res_end_key.get_collation_free_store_rowkey()));

  multi_version_range.reset();
  trans_version_range.reset();
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 1000;
  trans_version_range.base_version_ = 8;

  res_start_cells[2].set_int(-trans_version_range.snapshot_version_);
  res_end_cells[2].set_int(0);

  range.set_start_key(start_key);
  range.set_end_key(end_key);

  range.set_left_closed();
  range.set_right_closed();

  ASSERT_EQ(OB_INVALID_ARGUMENT,
      ObVersionStoreRangeConversionHelper::range_to_multi_version_range(
      range, trans_version_range, allocator, multi_version_range));

  multi_version_range.reset();
  trans_version_range.multi_version_start_ = 10;
  trans_version_range.base_version_ = ObVersionRange::MIN_VERSION;
  trans_version_range.multi_version_start_ = ObVersionRange::MIN_VERSION;
  ASSERT_EQ(OB_SUCCESS,
      ObVersionStoreRangeConversionHelper::range_to_multi_version_range(
      range, trans_version_range, allocator, multi_version_range));
  ASSERT_TRUE(multi_version_range.get_start_key().simple_equal(res_start_key));
  //ASSERT_TRUE(multi_version_range.get_collation_free_start_key().simple_equal(res_start_key.get_collation_free_store_rowkey()));
  ASSERT_TRUE(multi_version_range.get_end_key().simple_equal(res_end_key));
  //ASSERT_TRUE(multi_version_range.get_collation_free_end_key().simple_equal(res_end_key.get_collation_free_store_rowkey()));
}

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc,argv);
  OB_LOGGER.set_log_level("INFO");
  return RUN_ALL_TESTS();
}
