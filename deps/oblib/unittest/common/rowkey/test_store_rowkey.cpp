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
#define private public
#include "common/rowkey/ob_store_rowkey.h"
#include "common/rowkey/ob_rowkey.h"
#include "lib/container/ob_se_array.h"
using namespace oceanbase::common;

class TestStoreRowkey: public ::testing::Test
{
public:
  static const int OBJ_CNT = 5;

  TestStoreRowkey() {}
  ~TestStoreRowkey() {}
  virtual void SetUp();
  virtual void TearDown();

private:
  // disallow copy
  TestStoreRowkey(const TestStoreRowkey &other);
  TestStoreRowkey& operator=(const TestStoreRowkey &other);
protected:
  // data memebers
  ObObj obj_array_[OBJ_CNT];
  ObArenaAllocator allocator_;
  ObSEArray<ObOrderType, OB_MAX_ROWKEY_COLUMN_NUMBER> all_asc_orders_;
  ObSEArray<ObOrderType, OB_MAX_ROWKEY_COLUMN_NUMBER> asc_desc_orders_;
  ObSEArray<ObOrderType, OB_MAX_ROWKEY_COLUMN_NUMBER> all_desc_orders_;
};

void TestStoreRowkey::SetUp()
{
  for(int i = 0; i < OBJ_CNT; i++) {
    obj_array_[i].set_int(i);
  }
  for (int64_t i = 0; i < OB_MAX_ROWKEY_COLUMN_NUMBER; i++) {
    all_asc_orders_.push_back(ObOrderType::ASC);
  }
  for (int64_t i = 0; i < OB_MAX_ROWKEY_COLUMN_NUMBER; i++) {
    all_desc_orders_.push_back(ObOrderType::DESC);
  }
  for (int64_t i = 0; i < OB_MAX_ROWKEY_COLUMN_NUMBER; i++) {
    if (0 == i % 2) {
      asc_desc_orders_.push_back(ObOrderType::ASC);
    } else {
      asc_desc_orders_.push_back(ObOrderType::DESC);
    }
  }
}

void TestStoreRowkey::TearDown()
{
  all_asc_orders_.reset();
  all_desc_orders_.reset();
  asc_desc_orders_.reset();
  allocator_.clear();
}

TEST_F(TestStoreRowkey, test_valid_min_max)
{
  ObStoreRowkey store_rowkey;
  bool is_min_key = false;
  bool is_max_key = false;
  int ret = OB_SUCCESS;

  ASSERT_FALSE(store_rowkey.is_valid());
  ret = store_rowkey.is_min(all_asc_orders_, 1, is_min_key);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_FALSE(is_min_key);
  ret = store_rowkey.is_max(all_asc_orders_, 1, is_max_key);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_FALSE(is_max_key);


  ret = store_rowkey.set_min(all_asc_orders_, 1, allocator_);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(store_rowkey.is_valid());
  ret = store_rowkey.is_min(all_asc_orders_, 1, is_min_key);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(is_min_key);
  ret = store_rowkey.is_min(all_desc_orders_, 1, is_min_key);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_FALSE(is_min_key);

  store_rowkey.assign(obj_array_, 2);
  ret = store_rowkey.set_max(asc_desc_orders_, 2, allocator_);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(store_rowkey.is_valid());
  ret = store_rowkey.is_min(asc_desc_orders_, 0, is_min_key);
  ASSERT_EQ(ret, OB_INVALID_ARGUMENT);
  ret = store_rowkey.is_min(asc_desc_orders_, OB_MAX_ROWKEY_COLUMN_NUMBER + 1, is_min_key);
  ASSERT_EQ(ret, OB_INVALID_ARGUMENT);
  ret = store_rowkey.is_max(asc_desc_orders_, 1, is_max_key);
  ASSERT_EQ(ret, OB_INVALID_ARGUMENT);
  ret = store_rowkey.is_max(asc_desc_orders_, 2, is_max_key);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(is_max_key);
  ret = store_rowkey.is_max(all_asc_orders_, 2, is_max_key);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_FALSE(is_max_key);

  store_rowkey.assign(obj_array_, OBJ_CNT);
  ASSERT_TRUE(store_rowkey.is_valid());
  ret = store_rowkey.is_min(all_asc_orders_, OBJ_CNT, is_min_key);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_FALSE(is_min_key);
  ret = store_rowkey.is_max(all_desc_orders_, OBJ_CNT, is_max_key);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_FALSE(is_max_key);
}

TEST_F(TestStoreRowkey, test_deep_copy)
{
  int ret = OB_SUCCESS;
  ObStoreRowkey dst_store_rowkey;

  ObStoreRowkey invalid_store_rowkey;
  ret = invalid_store_rowkey.deep_copy(dst_store_rowkey, allocator_);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_FALSE(dst_store_rowkey.is_valid());

  ObStoreRowkey min_rowkey;
  min_rowkey.set_min(all_asc_orders_, 2, allocator_);
  min_rowkey.deep_copy(dst_store_rowkey, allocator_);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(dst_store_rowkey.is_valid());
  bool is_min_key = false;
  ret = dst_store_rowkey.is_min(all_asc_orders_, 2, is_min_key);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(is_min_key);

  ObStoreRowkey regular_store_rowkey(obj_array_, OBJ_CNT);
  regular_store_rowkey.deep_copy(dst_store_rowkey, allocator_);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(dst_store_rowkey.is_valid());
  ASSERT_NE(dst_store_rowkey.get_obj_ptr(),
      regular_store_rowkey.get_obj_ptr());
  ASSERT_TRUE(regular_store_rowkey.simple_equal(dst_store_rowkey));
}

TEST_F(TestStoreRowkey, test_serialize_and_deserialize)
{
  int ret = OB_SUCCESS;
  const int buf_size = 1024;
  char buf[buf_size];
  int64_t pos = 0;

  ObStoreRowkey invalid_store_rowkey;
  ret = invalid_store_rowkey.serialize(buf, buf_size, pos);
  ASSERT_EQ(pos, invalid_store_rowkey.get_serialize_size());
  ASSERT_EQ(ret,OB_SUCCESS);

  pos = 0;
  ObStoreRowkey dst_store_rowkey;
  ret = dst_store_rowkey.deserialize(allocator_, buf, buf_size, pos);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(pos, invalid_store_rowkey.get_serialize_size());
  ASSERT_FALSE(dst_store_rowkey.is_valid());

  pos = 0;
  ObStoreRowkey regular_store_rowkey(obj_array_, OBJ_CNT);
  ret = regular_store_rowkey.serialize(buf, buf_size, pos);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(pos, regular_store_rowkey.get_serialize_size());

  pos = 0;
  ret = dst_store_rowkey.deserialize(allocator_, buf, buf_size, pos);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(pos, regular_store_rowkey.get_serialize_size());
  ASSERT_TRUE(dst_store_rowkey.is_valid());
  ASSERT_TRUE(dst_store_rowkey.simple_equal(regular_store_rowkey));
}

//for compatibility, ObStoreRowkey should be able to deserialize ObRowkey serialization correctly
TEST_F(TestStoreRowkey, test_deserialize_compatibility)
{
  int ret = OB_SUCCESS;
  const int buf_size = 1024;
  char buf[buf_size];
  int64_t pos = 0;

  ObRowkey rowkey(obj_array_, OBJ_CNT);
  ret = rowkey.serialize(buf, buf_size, pos);
  ASSERT_EQ(ret, OB_SUCCESS);

  pos = 0;
  ObStoreRowkey dst_store_rowkey;
  ret = dst_store_rowkey.deserialize(allocator_, buf, buf_size, pos);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(pos, rowkey.get_serialize_size());
  ASSERT_TRUE(dst_store_rowkey.is_valid());
  ASSERT_TRUE(dst_store_rowkey.key_.simple_equal(rowkey));
}

TEST_F(TestStoreRowkey, test_checksum)
{
  int ret = OB_SUCCESS;
  ObBatchChecksum regular_bc, min_bc, max_bc, invalid_bc, rowkey_bc;
  ObStoreRowkey regular_store_rowkey(obj_array_, OBJ_CNT);


  //for compatibility,regular ObStoreRowkey should return the same checksum as the underlying rowkey
  ret = regular_store_rowkey.checksum(regular_bc);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = regular_store_rowkey.key_.checksum(rowkey_bc);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(regular_bc.calc(), rowkey_bc.calc());

  ObStoreRowkey min_store_rowkey;
  min_store_rowkey.set_min(asc_desc_orders_, OBJ_CNT, allocator_);
  min_store_rowkey.checksum(min_bc);

  ObStoreRowkey max_store_rowkey;
  max_store_rowkey.set_max(asc_desc_orders_, OBJ_CNT, allocator_);
  max_store_rowkey.checksum(max_bc);

  ObStoreRowkey invalid_store_rowkey(NULL, 0);
  invalid_store_rowkey.checksum(invalid_bc);

  ASSERT_NE(min_bc.calc(), regular_bc.calc());
  ASSERT_NE(max_bc.calc(), regular_bc.calc());
  ASSERT_NE(invalid_bc.calc(), regular_bc.calc());
  ASSERT_NE(min_bc.calc(), max_bc.calc());
  ASSERT_NE(min_bc.calc(), invalid_bc.calc());
}

TEST_F(TestStoreRowkey, test_hash)
{
   ObStoreRowkey regular_store_rowkey(obj_array_, OBJ_CNT);
   ObStoreRowkey another_regular_store_rowkey(obj_array_, OBJ_CNT);
   ObStoreRowkey invalid_store_rowkey(NULL, 0);

   ObStoreRowkey min_store_rowkey;
   min_store_rowkey.set_min(asc_desc_orders_, OBJ_CNT, allocator_);
   ObStoreRowkey max_store_rowkey;
   max_store_rowkey.set_max(asc_desc_orders_, OBJ_CNT, allocator_);

   ASSERT_EQ(regular_store_rowkey.hash(), another_regular_store_rowkey.hash());
   ASSERT_NE(min_store_rowkey.hash(), regular_store_rowkey.hash());
   ASSERT_NE(max_store_rowkey.hash(), regular_store_rowkey.hash());
   ASSERT_NE(min_store_rowkey.hash(), invalid_store_rowkey.hash());
   ASSERT_NE(max_store_rowkey.hash(), invalid_store_rowkey.hash());
   ASSERT_NE(max_store_rowkey.hash(), min_store_rowkey.hash());
}

// simple_equal should *always* return the same results as test compare() == 0;
TEST_F(TestStoreRowkey, test_simple_equal)
{
  int ret = OB_SUCCESS;
  bool simple_equal_res = false;
  int32_t compare_res = false;

  ObStoreRowkey regular_store_rowkey(obj_array_, OBJ_CNT);
  ObStoreRowkey another_regular_store_rowkey(obj_array_, OBJ_CNT);
  ObStoreRowkey min_store_rowkey;
  min_store_rowkey.set_min(asc_desc_orders_, OBJ_CNT, allocator_);
  ObStoreRowkey another_min_store_rowkey;
  another_min_store_rowkey.set_min(asc_desc_orders_, OBJ_CNT, allocator_);

  simple_equal_res = regular_store_rowkey.simple_equal(regular_store_rowkey);
  ret = regular_store_rowkey.compare(regular_store_rowkey, asc_desc_orders_,
                                      OBJ_CNT, compare_res);
  ASSERT_EQ(simple_equal_res, compare_res == 0);

  simple_equal_res = regular_store_rowkey.simple_equal(another_regular_store_rowkey);
  ret = regular_store_rowkey.compare(another_regular_store_rowkey,
                        asc_desc_orders_, OBJ_CNT, compare_res);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(simple_equal_res, compare_res == 0);

  simple_equal_res = regular_store_rowkey.simple_equal(min_store_rowkey);
  ret = regular_store_rowkey.compare(min_store_rowkey, asc_desc_orders_, OBJ_CNT, compare_res);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(simple_equal_res, compare_res == 0);

  simple_equal_res = min_store_rowkey.simple_equal(another_min_store_rowkey);
  ret = min_store_rowkey.compare(another_min_store_rowkey, asc_desc_orders_,
                                 OBJ_CNT, compare_res);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(simple_equal_res, compare_res == 0);
}

//FIXME-yangsuli: use an input file, instead of hard-code all the cases
TEST_F(TestStoreRowkey, test_compare)
{
  int ret = OB_SUCCESS;
  int compare_res = 0;

  ObObj small_objs[OBJ_CNT];
  for(int i = 0; i < OBJ_CNT; i++) {
      small_objs[i].set_int(1);
  }

  ObObj big_objs[OBJ_CNT];
  for(int i = 0; i < OBJ_CNT; i++) {
    big_objs[i].set_int(9);
  }

  ObStoreRowkey small_rowkey(small_objs, OBJ_CNT);
  ObStoreRowkey small_rowkey_with_less_objs(small_objs, OBJ_CNT - 1);
  ObStoreRowkey big_rowkey(big_objs, OBJ_CNT);
  ObStoreRowkey min_rowkey;
  ObStoreRowkey max_rowkey;
  ObStoreRowkey invalid_rowkey(NULL, 0);

  ret = max_rowkey.set_max(all_asc_orders_, OBJ_CNT, allocator_);
  ASSERT_EQ(ret, OB_SUCCESS);

  // regular compare return opposite results for asc/desc orders
  ret = small_rowkey.compare(big_rowkey, all_asc_orders_, OBJ_CNT, compare_res);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_LT(compare_res, 0);
  ret = small_rowkey.compare(big_rowkey, all_desc_orders_, OBJ_CNT, compare_res);
  ASSERT_GT(compare_res, 0);


  // min_rowkey is smaller than regular rowkey, under the given column order
  ret = min_rowkey.set_min(all_asc_orders_, OBJ_CNT, allocator_);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = min_rowkey.compare(small_rowkey, all_asc_orders_, OBJ_CNT, compare_res);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_LT(compare_res, 0);
  ret = small_rowkey.compare(min_rowkey, all_asc_orders_, OBJ_CNT, compare_res);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_GT(compare_res, 0);

  ret = min_rowkey.set_min(asc_desc_orders_, OBJ_CNT, allocator_);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = min_rowkey.compare(small_rowkey, asc_desc_orders_, OBJ_CNT, compare_res);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_LT(compare_res, 0);
  ret = small_rowkey.compare(min_rowkey, asc_desc_orders_, OBJ_CNT, compare_res);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_GT(compare_res, 0);

  ret = min_rowkey.set_min(all_desc_orders_, OBJ_CNT, allocator_);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = min_rowkey.compare(small_rowkey, all_desc_orders_, OBJ_CNT, compare_res);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_LT(compare_res, 0);
  ret = small_rowkey.compare(min_rowkey, all_desc_orders_, OBJ_CNT, compare_res);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_GT(compare_res, 0);


  // max_rowkey is larger than regular rowkey, under the given column orders
  ret = max_rowkey.set_max(all_asc_orders_, OBJ_CNT, allocator_);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = max_rowkey.compare(small_rowkey, all_asc_orders_, OBJ_CNT, compare_res);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_GT(compare_res, 0);
  ret = small_rowkey.compare(max_rowkey, all_asc_orders_, OBJ_CNT, compare_res);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_LT(compare_res, 0);

  ret = max_rowkey.set_max(asc_desc_orders_, OBJ_CNT, allocator_);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = max_rowkey.compare(small_rowkey, asc_desc_orders_, OBJ_CNT, compare_res);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_GT(compare_res, 0);
  ret = small_rowkey.compare(max_rowkey, asc_desc_orders_, OBJ_CNT, compare_res);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_LT(compare_res, 0);

  ret = max_rowkey.set_max(all_desc_orders_, OBJ_CNT, allocator_);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = max_rowkey.compare(small_rowkey, all_desc_orders_, OBJ_CNT, compare_res);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_GT(compare_res, 0);
  ret = small_rowkey.compare(max_rowkey, all_desc_orders_, OBJ_CNT, compare_res);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_LT(compare_res, 0);

  // min rowkey is smaller than max rowkey, under the given column orders
  ret = min_rowkey.set_min(all_asc_orders_, OBJ_CNT, allocator_);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = max_rowkey.set_max(all_asc_orders_, OBJ_CNT, allocator_);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = max_rowkey.compare(min_rowkey, all_asc_orders_, OBJ_CNT, compare_res);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_GT(compare_res, 0);
  ret = min_rowkey.compare(max_rowkey, all_asc_orders_, OBJ_CNT, compare_res);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_LT(compare_res, 0);

  ret = min_rowkey.set_min(asc_desc_orders_, OBJ_CNT, allocator_);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = max_rowkey.set_max(asc_desc_orders_, OBJ_CNT, allocator_);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = max_rowkey.compare(min_rowkey, asc_desc_orders_, OBJ_CNT, compare_res);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_GT(compare_res, 0);
  ret = min_rowkey.compare(max_rowkey, asc_desc_orders_, OBJ_CNT, compare_res);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_LT(compare_res, 0);

  ret = min_rowkey.set_min(all_desc_orders_, OBJ_CNT, allocator_);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = max_rowkey.set_max(all_desc_orders_, OBJ_CNT, allocator_);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = max_rowkey.compare(min_rowkey, all_desc_orders_, OBJ_CNT, compare_res);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_GT(compare_res, 0);
  ret = min_rowkey.compare(max_rowkey, all_desc_orders_, OBJ_CNT, compare_res);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_LT(compare_res, 0);

  // min == min, while max == max
  ret = min_rowkey.set_min(all_asc_orders_, OBJ_CNT, allocator_);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = max_rowkey.set_max(all_asc_orders_, OBJ_CNT, allocator_);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = min_rowkey.compare(min_rowkey, all_asc_orders_, OBJ_CNT, compare_res);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(compare_res, 0);
  ret = max_rowkey.compare(max_rowkey, all_asc_orders_, OBJ_CNT, compare_res);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(compare_res, 0);

  ret = min_rowkey.set_min(asc_desc_orders_, OBJ_CNT, allocator_);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = max_rowkey.set_max(asc_desc_orders_, OBJ_CNT, allocator_);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = max_rowkey.compare(min_rowkey, asc_desc_orders_, OBJ_CNT, compare_res);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_GT(compare_res, 0);
  ret = min_rowkey.compare(max_rowkey, asc_desc_orders_, OBJ_CNT, compare_res);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_LT(compare_res, 0);

  ret = min_rowkey.set_min(all_desc_orders_, OBJ_CNT, allocator_);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = max_rowkey.set_max(all_desc_orders_, OBJ_CNT, allocator_);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = max_rowkey.compare(min_rowkey, all_desc_orders_, OBJ_CNT, compare_res);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_GT(compare_res, 0);
  ret = min_rowkey.compare(max_rowkey, all_desc_orders_, OBJ_CNT, compare_res);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_LT(compare_res, 0);
}


int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
