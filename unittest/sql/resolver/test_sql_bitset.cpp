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
#include "sql/resolver/expr/ob_raw_expr.h"
#include "lib/container/ob_bit_set.h"
#include <cstring>
using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::lib;
using namespace oceanbase;

class TestSqlBitSet: public ::testing::Test
{
public:
  TestSqlBitSet();
  virtual ~TestSqlBitSet();
  virtual void SetUp();
  virtual void TearDown();
private:
  DISALLOW_COPY_AND_ASSIGN(TestSqlBitSet);
};

TEST_F(TestSqlBitSet, baisc_interfaces)
{
  ObSqlBitSet<16> bs;
  ASSERT_TRUE(0 == bs.bitset_word_count());
  ASSERT_TRUE(bs.is_empty());
  ASSERT_TRUE(0 == bs.num_members());
  ASSERT_TRUE(OB_SUCCESS == bs.add_member(33));
  ASSERT_TRUE(2 == bs.bitset_word_count());
  ASSERT_TRUE(!bs.is_empty());
  ASSERT_TRUE(bs.has_member(33));
  ASSERT_TRUE(!bs.has_member(32));
  ASSERT_TRUE(1 == bs.num_members());
}

TEST_F(TestSqlBitSet, add_and_del_number)
{
  ObSqlBitSet<16> bs;
  ASSERT_TRUE(OB_SUCCESS != bs.add_member(-1));
  ASSERT_TRUE(OB_SUCCESS == bs.add_member(5));
  ASSERT_TRUE(OB_SUCCESS == bs.add_member(50));
  ASSERT_TRUE(!bs.has_member(-1));
  ASSERT_TRUE(!bs.has_member(1));
  ASSERT_TRUE(bs.has_member(5));
  ASSERT_TRUE(bs.has_member(50));

  ASSERT_FALSE(OB_SUCCESS == bs.del_member(-1));
  ASSERT_TRUE(OB_SUCCESS == bs.del_member(5));
  ASSERT_TRUE(OB_SUCCESS == bs.del_member(500));
  ASSERT_TRUE(!bs.has_member(1));
  ASSERT_TRUE(!bs.has_member(5));
  ASSERT_TRUE(bs.has_member(50));
  ASSERT_TRUE(2 == bs.bitset_word_count());
  ASSERT_TRUE(64 == bs.bit_count());
}

TEST_F(TestSqlBitSet, get_bitset_word)
{
  ObSqlBitSet<16> bs;
  ASSERT_EQ(0, bs.get_bitset_word(-1));
  ASSERT_EQ(0, bs.get_bitset_word(1));
  ASSERT_EQ(0, bs.get_bitset_word(2));
  ASSERT_EQ(0, bs.get_bitset_word(3));
  ASSERT_TRUE(OB_SUCCESS == bs.add_member(5));
  ASSERT_TRUE(OB_SUCCESS == bs.add_member(50));
  ASSERT_TRUE(OB_SUCCESS == bs.add_member(90));

  ASSERT_EQ(32, bs.get_bitset_word(0));
  ASSERT_EQ(262144, bs.get_bitset_word(1));
  ASSERT_EQ(67108864, bs.get_bitset_word(2));
}

TEST_F(TestSqlBitSet, add_and_del_members)
{
  ObSqlBitSet<16> bs;

  ASSERT_TRUE(OB_SUCCESS == bs.add_member(5));
  ASSERT_TRUE(OB_SUCCESS == bs.add_member(50));
  ASSERT_TRUE(OB_SUCCESS == bs.add_member(100));
  ASSERT_TRUE(OB_SUCCESS == bs.add_member(111));

  ObSqlBitSet<16> bs2;
  ASSERT_TRUE(OB_SUCCESS == bs2.add_member(15));
  ASSERT_TRUE(OB_SUCCESS == bs2.add_member(50));
  ASSERT_TRUE(OB_SUCCESS == bs2.add_member(100));
  ASSERT_TRUE(OB_SUCCESS == bs2.add_member(200));

  bs.del_members(bs2);
  ASSERT_TRUE(bs.has_member(5));
  ASSERT_TRUE(!bs.has_member(50));
  ASSERT_TRUE(!bs.has_member(100));
  ASSERT_TRUE(bs.has_member(111));

  ASSERT_TRUE(OB_SUCCESS == bs.add_members(bs2));
  ASSERT_TRUE(OB_SUCCESS == bs2.add_member(200));
  ASSERT_TRUE(bs.has_member(50));
  ASSERT_TRUE(bs.has_member(100));
  ASSERT_TRUE(bs.has_member(200));
  ASSERT_TRUE(bs.has_member(15));

  ASSERT_EQ(6, bs.num_members());
}

// TEST_F(TestSqlBitSet, add_and_del_members2)
// {
//   ObSqlBitSet<16> bs;

//   ASSERT_TRUE(OB_SUCCESS == bs.add_member(5));
//   ASSERT_TRUE(OB_SUCCESS == bs.add_member(50));
//   ASSERT_TRUE(OB_SUCCESS == bs.add_member(100));
//   ASSERT_TRUE(OB_SUCCESS == bs.add_member(111));

//   ObBitSet<16> bs2;
//   ASSERT_TRUE(OB_SUCCESS == bs2.add_member(15));
//   ASSERT_TRUE(OB_SUCCESS == bs2.add_member(50));
//   ASSERT_TRUE(OB_SUCCESS == bs2.add_member(100));
//   ASSERT_TRUE(OB_SUCCESS == bs2.add_member(200));

//   bs.del_members(bs2);
//   ASSERT_TRUE(bs.has_member(5));
//   ASSERT_TRUE(!bs.has_member(50));
//   ASSERT_TRUE(!bs.has_member(100));
//   ASSERT_TRUE(bs.has_member(111));

//   ASSERT_TRUE(OB_SUCCESS == bs.add_members(bs2));
//   ASSERT_TRUE(OB_SUCCESS == bs2.add_member(200));
//   ASSERT_TRUE(bs.has_member(50));
//   ASSERT_TRUE(bs.has_member(100));
//   ASSERT_TRUE(bs.has_member(200));
//   ASSERT_TRUE(bs.has_member(15));

//   ASSERT_EQ(6, bs.num_members());
// }

TEST_F(TestSqlBitSet, do_mask)
{
  ObSqlBitSet<16> bs;
  ASSERT_TRUE(OB_SUCCESS == bs.add_member(5));
  ASSERT_TRUE(OB_SUCCESS == bs.add_member(50));
  ASSERT_TRUE(OB_SUCCESS == bs.add_member(100));
  ASSERT_TRUE(OB_SUCCESS == bs.add_member(111));
  ASSERT_TRUE(OB_SUCCESS == bs.do_mask(6,101));
  ASSERT_TRUE(bs.has_member(50));
  ASSERT_TRUE(bs.has_member(100));
  ASSERT_TRUE(!bs.has_member(5));
  ASSERT_TRUE(!bs.has_member(111));
  ASSERT_TRUE(!bs.has_member(121));

  ObSqlBitSet<16> bs2;
  ASSERT_TRUE(OB_SUCCESS == bs2.add_member(5));
  ASSERT_TRUE(OB_SUCCESS == bs2.add_member(32));
  ASSERT_TRUE(OB_SUCCESS == bs2.add_member(63));
  ASSERT_TRUE(OB_SUCCESS == bs2.do_mask(32, 63));
  ASSERT_FALSE(bs2.has_member(5));
  ASSERT_TRUE(bs2.has_member(32));
  ASSERT_TRUE(bs2.has_member(63));
}

TEST_F(TestSqlBitSet, set_operation)
{
  ObSqlBitSet<16> bs;
  ASSERT_TRUE(OB_SUCCESS == bs.add_member(5));
  ASSERT_TRUE(OB_SUCCESS == bs.add_member(50));
  ASSERT_TRUE(OB_SUCCESS == bs.add_member(100));
  ASSERT_TRUE(OB_SUCCESS == bs.add_member(111));
  ASSERT_TRUE(OB_SUCCESS == bs.do_mask(5,101));

  //is_superset && is_subset
  ObSqlBitSet<16> bs1;
  ASSERT_TRUE(bs.is_superset(bs1));
  ASSERT_TRUE(bs1.is_subset(bs));
  ASSERT_FALSE(bs1.is_superset(bs));
  ASSERT_FALSE(bs.is_subset(bs1));
  ASSERT_TRUE(OB_SUCCESS == bs1.add_member(5));
  ASSERT_FALSE(bs1.is_superset(bs));
  ASSERT_FALSE(bs.is_subset(bs1));
  ASSERT_TRUE(OB_SUCCESS == bs1.add_member(50));
  ASSERT_TRUE(OB_SUCCESS == bs1.add_member(100));
  ASSERT_TRUE(bs.is_superset(bs1));
  ASSERT_TRUE(bs1.is_subset(bs));
}

// TEST_F(TestSqlBitSet, set_operation2)
// {
//   ObSqlBitSet<16> bs;
//   ASSERT_TRUE(OB_SUCCESS == bs.add_member(5));
//   ASSERT_TRUE(OB_SUCCESS == bs.add_member(50));
//   ASSERT_TRUE(OB_SUCCESS == bs.add_member(100));
//   ASSERT_TRUE(OB_SUCCESS == bs.add_member(111));
//   ASSERT_TRUE(OB_SUCCESS == bs.do_mask(5,101));

//   //is_superset && is_subset
//   ObBitSet<16> bs1;
//   ASSERT_TRUE(bs.is_superset(bs1));
//   ASSERT_FALSE(bs.is_subset(bs1));
//   ASSERT_TRUE(OB_SUCCESS == bs1.add_member(5));
//   ASSERT_FALSE(bs.is_subset(bs1));
//   ASSERT_TRUE(OB_SUCCESS == bs1.add_member(50));
//   ASSERT_TRUE(OB_SUCCESS == bs1.add_member(100));
//   ASSERT_TRUE(bs.is_superset(bs1));
// }

TEST_F(TestSqlBitSet, overlap)
{
  ObSqlBitSet<16> bs;
  ObSqlBitSet<16> bs1;
  ASSERT_FALSE(bs.overlap(bs1));
  ASSERT_FALSE(bs1.overlap(bs));
  ASSERT_TRUE(OB_SUCCESS == bs.add_member(5));
  ASSERT_TRUE(OB_SUCCESS == bs.add_member(50));

  ASSERT_TRUE(OB_SUCCESS == bs1.add_member(51));
  ASSERT_TRUE(OB_SUCCESS == bs1.add_member(101));
  ASSERT_FALSE(bs.overlap(bs1));
  ASSERT_FALSE(bs1.overlap(bs));
  ASSERT_TRUE(OB_SUCCESS == bs.add_member(101));
  ASSERT_TRUE(bs.overlap(bs1));
  ASSERT_TRUE(bs1.overlap(bs));
}

TEST_F(TestSqlBitSet, equal)
{
  ObSqlBitSet<16> bs;
  ObSqlBitSet<16> bs2;

  ASSERT_TRUE(OB_SUCCESS == bs.add_member(5));
  ASSERT_TRUE(OB_SUCCESS == bs.add_member(50));

  ASSERT_FALSE(bs.equal(bs2));
  ASSERT_TRUE(OB_SUCCESS == bs2.add_member(50));
  ASSERT_FALSE(bs2.equal(bs));
  ASSERT_TRUE(OB_SUCCESS == bs2.add_member(5));
  ASSERT_TRUE(bs2.equal(bs));

  ASSERT_TRUE(OB_SUCCESS == bs.add_member(550));
  ASSERT_FALSE(bs.equal(bs2));
  ASSERT_TRUE(OB_SUCCESS == bs2.add_member(550));
  ASSERT_TRUE(bs.equal(bs2));

  // ObBitSet<16> bs3;
  // ASSERT_FALSE(bs.equal(bs3));
  // ASSERT_TRUE(OB_SUCCESS == bs3.add_member(550));
  // ASSERT_TRUE(OB_SUCCESS == bs3.add_member(50));
  // ASSERT_FALSE(bs.equal(bs3));
  // ASSERT_TRUE(OB_SUCCESS == bs3.add_member(5));
  // ASSERT_TRUE(bs.equal(bs3));

  ASSERT_TRUE(OB_SUCCESS == bs2.add_member(1));
  ObSqlBitSet<16> bs4 = bs2;
  ASSERT_TRUE(bs4 == bs2);
  ASSERT_TRUE(bs4.has_member(5));
  ASSERT_TRUE(bs4.has_member(50));
  ASSERT_TRUE(bs4.has_member(550));
  ASSERT_TRUE(bs4.has_member(1));
  ASSERT_FALSE(bs4.has_member(555));
}

TEST_F(TestSqlBitSet, to_string)
{
  ObSqlBitSet<16> bs;
  ASSERT_TRUE(OB_SUCCESS == bs.add_member(1));
  ASSERT_TRUE(OB_SUCCESS == bs.add_member(5));
  ASSERT_TRUE(OB_SUCCESS == bs.add_member(50));
  ASSERT_TRUE(OB_SUCCESS == bs.add_member(550));

  char buf[32];
  static const int64_t buf_len = 32;

  int64_t pos = bs.to_string(buf, buf_len);
  ASSERT_TRUE(0 == std::strncmp("[1, 5, 50, 550]", buf, pos));
}

TEST_F(TestSqlBitSet, number_and_clear)
{
  ObSqlBitSet<128> bs;
  ASSERT_TRUE(OB_SUCCESS == bs.add_member(5));
  ASSERT_TRUE(OB_SUCCESS == bs.add_member(50));
  ASSERT_TRUE(OB_SUCCESS == bs.add_member(100));
  ASSERT_TRUE(OB_SUCCESS == bs.add_member(111));
  ASSERT_EQ(4, bs.num_members());
  ObSqlBitSet<> bs1;
  ASSERT_EQ(0, bs1.bitset_word_count());
  ASSERT_TRUE(OB_SUCCESS == bs1.add_member(5));
  ASSERT_EQ(1, bs1.bitset_word_count());
  ASSERT_TRUE(OB_SUCCESS == bs1.add_member(50));
  ASSERT_EQ(2, bs1.bitset_word_count());
  ASSERT_TRUE(OB_SUCCESS == bs1.add_member(100));
  ASSERT_EQ(4, bs1.bitset_word_count());
  ASSERT_TRUE(OB_SUCCESS == bs1.add_member(111));
  ASSERT_EQ(4, bs1.bitset_word_count());
  ASSERT_EQ(4, bs1.num_members());
  ASSERT_EQ(4, bs1.bitset_word_count());
  ASSERT_TRUE(bs1.has_member(5));
  ASSERT_TRUE(bs1.has_member(50));
  ASSERT_TRUE(bs1.has_member(100));
  ASSERT_TRUE(bs1.has_member(111));

  bs1.reuse();
  ASSERT_EQ(0, bs1.bitset_word_count());
  ASSERT_TRUE(bs1.is_empty());

  ASSERT_FALSE(bs1.has_member(5));
  ASSERT_FALSE(bs1.has_member(50));
  ASSERT_FALSE(bs1.has_member(100));
  ASSERT_FALSE(bs1.has_member(101));
}

TEST_F(TestSqlBitSet, to_array)
{
  ObSqlBitSet<128> bs;
  ASSERT_TRUE(OB_SUCCESS == bs.add_member(5));
  ASSERT_TRUE(OB_SUCCESS == bs.add_member(50));
  ASSERT_TRUE(OB_SUCCESS == bs.add_member(100));
  ASSERT_TRUE(OB_SUCCESS == bs.add_member(111));
  ObArray<int64_t> arr;
  ASSERT_TRUE(OB_SUCCESS == bs.to_array(arr));
  ASSERT_EQ(4, arr.count());
  ASSERT_EQ(5, arr.at(0));
  ASSERT_EQ(50, arr.at(1));
  ASSERT_EQ(100, arr.at(2));
  ASSERT_EQ(111, arr.at(3));
}

TestSqlBitSet::TestSqlBitSet() {}
TestSqlBitSet::~TestSqlBitSet() {}
void TestSqlBitSet::SetUp() {}
void TestSqlBitSet::TearDown() {}



int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  int ret = 0;
  ContextParam param;
  param.set_mem_attr(1001, "SqlBitset", ObCtxIds::WORK_AREA)
    .set_page_size(OB_MALLOC_BIG_BLOCK_SIZE);
  CREATE_WITH_TEMP_CONTEXT(param) {
    ret = RUN_ALL_TESTS();
  }
  return ret;
}
