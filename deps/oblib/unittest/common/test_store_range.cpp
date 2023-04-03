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
#include "lib/container/ob_se_array.h"
using namespace oceanbase::common;

class TestStoreRange : public ::testing::Test
{
public:
  TestStoreRange() {}
  ~TestStoreRange() {}
  virtual void SetUp();
  virtual void TearDown();

private:
  // disallow copy
  TestStoreRange(const TestStoreRange &other);
  TestStoreRange& operator=(const TestStoreRange &other);

protected:
  ObObj start_cells_[2];
  ObObj end_cells_[2];
  const char *start_var_ = "key1";
  const char *end_var_ = "key2";
  ObStoreRowkey start_key_;
  ObStoreRowkey end_key_;
  uint64_t table_id_;
  ObArenaAllocator allocator_;
};

void TestStoreRange::SetUp()
{
  start_cells_[0].set_int(1);
  start_cells_[1].set_varchar(start_var_);
  start_cells_[1].set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  start_cells_[1].set_collation_level(CS_LEVEL_IMPLICIT);
  start_key_.assign(start_cells_, 2);

  end_cells_[0].set_int(2);
  end_cells_[1].set_varchar(end_var_);
  end_cells_[1].set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  end_cells_[1].set_collation_level(CS_LEVEL_IMPLICIT);
  end_key_.assign(end_cells_, 2);

  table_id_ = OB_INVALID_ID;
}

void TestStoreRange::TearDown()
{
  allocator_.clear();
}

TEST_F(TestStoreRange, test_is_valid_whole)
{
  // initial range is invalid
  ObStoreRange invalid_range;
  ASSERT_FALSE(invalid_range.is_valid());

  // range is invalid after reset;
  ObStoreRange regular_range;
  regular_range.set_table_id(table_id_);
  regular_range.set_start_key(start_key_);
  regular_range.set_end_key(end_key_);
  ASSERT_TRUE(regular_range.is_valid());
  regular_range.reset();
  ASSERT_FALSE(regular_range.is_valid());

  // a range should be valid even when it does not refer to a particular, valid table
  // as long as start_key_ and end_key_ are valid
  ObStoreRange range_on_invalid_table;
  range_on_invalid_table.set_start_key(start_key_);
  range_on_invalid_table.set_end_key(end_key_);
  ASSERT_TRUE(range_on_invalid_table.is_valid());
}

TEST_F(TestStoreRange, test_whole_range)
{
  int ret = OB_SUCCESS;
  bool is_whole = false;
  ObArenaAllocator allocator(ObModIds::TEST);

  ObSEArray<ObOrderType, 2> all_asc_orders;
  ret = all_asc_orders.push_back(ObOrderType::ASC);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = all_asc_orders.push_back(ObOrderType::ASC);
  ASSERT_EQ(ret, OB_SUCCESS);

  ObStoreRange all_asc_whole_range;
  all_asc_whole_range.set_whole_range();
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(all_asc_whole_range.is_valid());
  ret = all_asc_whole_range.is_whole_range();
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(is_whole);

  ObSEArray<ObOrderType, 2> asc_desc_orders;
  ret = asc_desc_orders.push_back(ObOrderType::ASC);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = asc_desc_orders.push_back(ObOrderType::DESC);
  ASSERT_EQ(ret, OB_SUCCESS);

  ObStoreRange asc_desc_whole_range;
  asc_desc_whole_range.set_whole_range();
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(asc_desc_whole_range.is_valid());
  is_whole = false;
  ret = asc_desc_whole_range.is_whole_range();
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(is_whole);
  ret = asc_desc_whole_range.is_whole_range();
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_FALSE(is_whole);

  ObStoreRange regular_range;
  regular_range.set_table_id(table_id_);
  regular_range.set_start_key(start_key_);
  regular_range.set_end_key(end_key_);
  is_whole = true;
  ret = regular_range.is_whole_range();
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_FALSE(is_whole);
  is_whole = true;
  ret = regular_range.is_whole_range();
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_FALSE(is_whole);
}


TEST_F(TestStoreRange, test_serialize_deserialize)
{
  ObStoreRange range;
  range.set_start_key(start_key_);
  range.set_end_key(end_key_);
  range.set_table_id(table_id_);
  range.set_left_open();
  range.set_right_closed();

  int ret = OB_SUCCESS;
  const int buf_size = 1024;
  char buf[buf_size];
  int64_t pos = 0;

  ret = range.serialize(buf, buf_size, pos);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(pos, range.get_serialize_size());

  pos = 0;
  ObStoreRange dst_store_range;
  ret = dst_store_range.deserialize(allocator_, buf, buf_size, pos);
  ASSERT_EQ(ret, OB_SUCCESS);
  // clear buf, to make sure deserialized range does not contain shallow-copied ObObjs
  memset(buf, 0, buf_size);
  ASSERT_EQ(range.get_table_id(), dst_store_range.get_table_id());
  ASSERT_EQ(range.get_border_flag().get_data(), dst_store_range.get_border_flag().get_data());
  ASSERT_TRUE(range.get_start_key().simple_equal(dst_store_range.get_start_key()));
  ASSERT_TRUE(range.get_end_key().simple_equal(dst_store_range.get_end_key()));
}

TEST_F(TestStoreRange, test_deep_copy)
{
  int ret = OB_SUCCESS;
  ObStoreRange src_range;
  ObStoreRange dst_range;;

  src_range.set_start_key(start_key_);
  src_range.set_end_key(end_key_);
  src_range.set_table_id(table_id_);
  src_range.set_left_closed();
  src_range.set_right_closed();

  ret = src_range.deep_copy(allocator_, dst_range);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(src_range.get_table_id(), dst_range.get_table_id());
  ASSERT_EQ(src_range.get_border_flag().get_data(), dst_range.get_border_flag().get_data());
  ASSERT_TRUE(src_range.get_start_key().simple_equal(dst_range.get_start_key()));
  ASSERT_TRUE(src_range.get_end_key().simple_equal(dst_range.get_end_key()));
}

TEST_F(TestStoreRange, test_get_common_store_rowkey)
{
  ObStoreRowkey common_store_rowkey;

  ObStoreRange has_common_range;
  has_common_range.set_start_key(start_key_);
  has_common_range.set_end_key(start_key_);
  has_common_range.get_common_store_rowkey(common_store_rowkey);
  ASSERT_TRUE(common_store_rowkey.is_valid());
  ASSERT_TRUE(start_key_.simple_equal(common_store_rowkey));

  ObStoreRange no_common_range;
  no_common_range.set_start_key(start_key_);
  no_common_range.set_end_key(end_key_);
  no_common_range.get_common_store_rowkey(common_store_rowkey);
  ASSERT_FALSE(common_store_rowkey.is_valid());

}


int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  OB_LOGGER.set_log_level("INFO");
  return RUN_ALL_TESTS();
}
