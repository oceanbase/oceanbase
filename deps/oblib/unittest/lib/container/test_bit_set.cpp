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
#include "lib/container/ob_bit_set.h"
#include "lib/utility/ob_test_util.h"
#include "lib/container/ob_array.h"
using namespace oceanbase::common;
class TestBitSet: public ::testing::Test
{
public:
  TestBitSet();
  virtual ~TestBitSet();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestBitSet);
protected:
  // function members
protected:
  // data members
};

TestBitSet::TestBitSet()
{
}

TestBitSet::~TestBitSet()
{
}

void TestBitSet::SetUp()
{
}

void TestBitSet::TearDown()
{
}

TEST_F(TestBitSet, add_and_del_number)
{
  ObBitSet<16> bs;
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
}

TEST_F(TestBitSet, get_bitset_word)
{
  ObBitSet<16> bs;
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

TEST_F(TestBitSet, add_and_del_members)
{
  ObBitSet<16> bs;

  ASSERT_TRUE(OB_SUCCESS == bs.add_member(5));
  ASSERT_TRUE(OB_SUCCESS == bs.add_member(50));
  ASSERT_TRUE(OB_SUCCESS == bs.add_member(100));
  ASSERT_TRUE(OB_SUCCESS == bs.add_member(111));

  ObBitSet<16> bs2;
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

TEST_F(TestBitSet, do_mask)
{
  ObBitSet<16> bs;
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
}

TEST_F(TestBitSet, to_string)
{
  ObBitSet<16> bs;
  ASSERT_TRUE(bs.is_empty());
  OB_LOG(INFO, "TO_STRING", K(bs));

  ASSERT_TRUE(OB_SUCCESS == bs.add_member(5));
  ASSERT_TRUE(OB_SUCCESS == bs.add_member(50));
  ASSERT_TRUE(OB_SUCCESS == bs.add_member(100));
  ASSERT_TRUE(OB_SUCCESS == bs.add_member(111));
  ASSERT_FALSE(bs.is_empty());
  OB_LOG(INFO, "TO_STRING", K(bs));
}

TEST_F(TestBitSet, set_operation)
{
  ObBitSet<16> bs;
  ASSERT_TRUE(OB_SUCCESS == bs.add_member(5));
  ASSERT_TRUE(OB_SUCCESS == bs.add_member(50));
  ASSERT_TRUE(OB_SUCCESS == bs.add_member(100));
  ASSERT_TRUE(OB_SUCCESS == bs.add_member(111));
  ASSERT_TRUE(OB_SUCCESS == bs.do_mask(5,101));

  //is_superset && is_subset
  ObBitSet<16> bs1;
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

TEST_F(TestBitSet, overlap)
{
  ObBitSet<16> bs;
  ObBitSet<16> bs1;
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

TEST_F(TestBitSet, number_and_clear)
{
  ObBitSet<128> bs;
  ASSERT_TRUE(OB_SUCCESS == bs.add_member(5));
  ASSERT_TRUE(OB_SUCCESS == bs.add_member(50));
  ASSERT_TRUE(OB_SUCCESS == bs.add_member(100));
  ASSERT_TRUE(OB_SUCCESS == bs.add_member(111));
  ASSERT_EQ(4, bs.num_members());
  ObArenaAllocator allocator("BitSet");
  //test case with allocator
  ObBitSet<16, ObIAllocator&> bs1(allocator);
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

TEST_F(TestBitSet, to_array)
{
  ObBitSet<128> bs;
  ASSERT_TRUE(OB_SUCCESS == bs.add_member(5));
  ASSERT_TRUE(OB_SUCCESS == bs.add_member(50));
  ASSERT_TRUE(OB_SUCCESS == bs.add_member(100));
  ASSERT_TRUE(OB_SUCCESS == bs.add_member(111));
  ObArray<int64_t> arr;
  OK(bs.to_array(arr));
  ASSERT_EQ(4, arr.count());
  ASSERT_EQ(5, arr.at(0));
  ASSERT_EQ(50, arr.at(1));
  ASSERT_EQ(100, arr.at(2));
  ASSERT_EQ(111, arr.at(3));
}

TEST_F(TestBitSet, construct)
{
  ObBitSet<16> bs;
  ASSERT_TRUE(OB_SUCCESS == bs.add_member(5));
  ASSERT_TRUE(OB_SUCCESS == bs.add_member(50));
  ASSERT_TRUE(OB_SUCCESS == bs.add_member(100));
  ASSERT_TRUE(OB_SUCCESS == bs.add_member(111));

  ObBitSet<16> bs1(bs);
  ASSERT_TRUE(bs == bs1);
  ASSERT_TRUE(bs1.has_member(5));
  ASSERT_TRUE(bs1.has_member(50));
  ASSERT_TRUE(bs1.has_member(100));
  ASSERT_TRUE(bs1.has_member(111));
  ASSERT_FALSE(bs1.has_member(101));

  ASSERT_TRUE(OB_SUCCESS == bs1.del_member(5));

  ASSERT_FALSE(bs == bs1);
  ASSERT_TRUE(OB_SUCCESS == bs.del_member(5));
  ASSERT_TRUE(OB_SUCCESS == bs1.add_member(10));
  ASSERT_TRUE(OB_SUCCESS == bs.add_member(10));
  ASSERT_TRUE(bs == bs1);
  ASSERT_TRUE(bs.equal(bs1));
}

TEST_F(TestBitSet, get_first_peculiar_bit)
{
  ObBitSet<16> bs;
  ObBitSet<16> bs1;
  ASSERT_EQ(-1, bs.get_first_peculiar_bit(bs1));
  ASSERT_TRUE(OB_SUCCESS == bs.add_member(10));
  ASSERT_TRUE(OB_SUCCESS == bs1.add_member(10));
  ASSERT_TRUE(OB_SUCCESS == bs.add_member(5));
  ASSERT_EQ(5, bs.get_first_peculiar_bit(bs1));
  ASSERT_TRUE(OB_SUCCESS == bs1.add_member(5));
  ASSERT_TRUE(OB_SUCCESS == bs.add_member(50));
  ASSERT_EQ(50, bs.get_first_peculiar_bit(bs1));

}

TEST_F(TestBitSet, serialize)
{
  ObBitSet<128> bs;
  ASSERT_TRUE(OB_SUCCESS == bs.add_member(5));
  ASSERT_TRUE(OB_SUCCESS == bs.add_member(50));
  ASSERT_TRUE(OB_SUCCESS == bs.add_member(100));
  ASSERT_TRUE(OB_SUCCESS == bs.add_member(111));
  const int64_t BUF_LEN = 1024;
  char buf[1024];
  int64_t pos = 0;
  int64_t pos1 = 0;
  ASSERT_EQ(OB_SUCCESS, bs.serialize(buf, BUF_LEN, pos));
  ObBitSet<128> bs1;
  ASSERT_EQ(OB_SUCCESS, bs1.deserialize(buf, BUF_LEN, pos1));
  ASSERT_TRUE(bs.equal(bs1));
  ASSERT_EQ(bs.get_serialize_size(), bs.get_serialize_size());
}

TEST_F(TestBitSet, find_and_clear)
{
  ObBitSet<128> bs;
  // test find next
  ASSERT_EQ(OB_SUCCESS, bs.add_member(0));
  ASSERT_EQ(OB_SUCCESS, bs.add_member(5));
  ASSERT_EQ(OB_SUCCESS, bs.add_member(31));
  ASSERT_EQ(OB_SUCCESS, bs.add_member(32));
  ASSERT_EQ(OB_SUCCESS, bs.add_member(52));
  ASSERT_EQ(OB_SUCCESS, bs.add_member(63));
  ASSERT_EQ(OB_SUCCESS, bs.add_member(64));
  ASSERT_EQ(OB_SUCCESS, bs.add_member(121));
  int64_t pos = 0;
  ASSERT_EQ(OB_INVALID_ARGUMENT, bs.find_next(-2, pos));
  ASSERT_EQ(OB_SUCCESS, bs.find_next(-1, pos));
  ASSERT_EQ(0, pos);
  ASSERT_EQ(OB_SUCCESS, bs.find_first(pos));
  ASSERT_EQ(0, pos);
  ASSERT_EQ(OB_SUCCESS, bs.find_next(pos, pos));
  ASSERT_EQ(5, pos);
  ASSERT_EQ(OB_SUCCESS, bs.find_next(pos, pos));
  ASSERT_EQ(31, pos);
  ASSERT_EQ(OB_SUCCESS, bs.find_next(pos, pos));
  ASSERT_EQ(32, pos);
  ASSERT_EQ(OB_SUCCESS, bs.find_next(pos, pos));
  ASSERT_EQ(52, pos);
  ASSERT_EQ(OB_SUCCESS, bs.find_next(pos, pos));
  ASSERT_EQ(63, pos);
  ASSERT_EQ(OB_SUCCESS, bs.find_next(pos, pos));
  ASSERT_EQ(64, pos);
  ASSERT_EQ(OB_SUCCESS, bs.find_next(pos, pos));
  ASSERT_EQ(121, pos);
  ASSERT_EQ(OB_SUCCESS, bs.find_next(pos, pos));
  ASSERT_EQ(-1, pos);
  ASSERT_EQ(OB_SUCCESS, bs.find_next(128, pos));
  ASSERT_EQ(-1, pos);

  // test clear all
  bs.clear_all();
  ASSERT_EQ(OB_SUCCESS, bs.find_first(pos));
  ASSERT_EQ(-1, pos);

  // test find prev
  ASSERT_EQ(OB_SUCCESS, bs.find_prev(150, pos));
  ASSERT_EQ(-1, pos);
  ASSERT_EQ(OB_SUCCESS, bs.find_prev(128, pos));
  ASSERT_EQ(-1, pos);
  ASSERT_EQ(OB_SUCCESS, bs.find_prev(129, pos));
  ASSERT_EQ(-1, pos);
  ASSERT_EQ(OB_INVALID_ARGUMENT, bs.find_prev(-1, pos));
  ASSERT_EQ(OB_INVALID_ARGUMENT, bs.find_prev(-10, pos));
  ASSERT_EQ(OB_SUCCESS, bs.find_prev(0, pos));
  ASSERT_EQ(-1, pos);
  ASSERT_EQ(OB_SUCCESS, bs.find_prev(28, pos));
  ASSERT_EQ(-1, pos);

  bs.reset();
  ASSERT_EQ(OB_SUCCESS, bs.add_member(90));
  bs.clear_all();
  ASSERT_EQ(OB_SUCCESS, bs.add_member(59));
  ASSERT_EQ(OB_SUCCESS, bs.find_prev(75, pos));
  ASSERT_EQ(59, pos);

  bs.reset();
  ASSERT_EQ(OB_SUCCESS, bs.add_member(31));
  ASSERT_EQ(OB_SUCCESS, bs.find_prev(100, pos));
  ASSERT_EQ(1, bs.bitset_word_count());
  ASSERT_EQ(31, pos);

  bs.clear_all();
  ASSERT_EQ(OB_SUCCESS, bs.add_member(0));
  ASSERT_EQ(OB_SUCCESS, bs.add_member(121));

  ASSERT_EQ(OB_SUCCESS, bs.find_prev(120, pos));
  ASSERT_EQ(0, pos);
  ASSERT_EQ(OB_SUCCESS, bs.find_prev(127, pos));
  ASSERT_EQ(121, pos);

  ASSERT_EQ(OB_SUCCESS, bs.add_member(5));
  ASSERT_EQ(OB_SUCCESS, bs.add_member(31));
  ASSERT_EQ(OB_SUCCESS, bs.add_member(32));
  ASSERT_EQ(OB_SUCCESS, bs.add_member(52));
  ASSERT_EQ(OB_SUCCESS, bs.add_member(63));
  ASSERT_EQ(OB_SUCCESS, bs.add_member(96));

  ASSERT_EQ(OB_SUCCESS, bs.find_prev(120, pos));
  ASSERT_EQ(96, pos);
  ASSERT_EQ(OB_SUCCESS, bs.find_prev(pos, pos));
  ASSERT_EQ(63, pos);
  ASSERT_EQ(OB_SUCCESS, bs.find_prev(pos, pos));
  ASSERT_EQ(52, pos);
  ASSERT_EQ(OB_SUCCESS, bs.find_prev(pos, pos));
  ASSERT_EQ(32, pos);
  ASSERT_EQ(OB_SUCCESS, bs.find_prev(pos, pos));
  ASSERT_EQ(31, pos);
  ASSERT_EQ(OB_SUCCESS, bs.find_prev(pos, pos));
  ASSERT_EQ(5, pos);
}


TEST_F(TestBitSet, find_and_clear_for_fix_bit_set)
{
  ObFixedBitSet<128> bs;
  // test find next
  ASSERT_EQ(OB_SUCCESS, bs.add_member(0));
  ASSERT_EQ(OB_SUCCESS, bs.add_member(5));
  ASSERT_EQ(OB_SUCCESS, bs.add_member(31));
  ASSERT_EQ(OB_SUCCESS, bs.add_member(32));
  ASSERT_EQ(OB_SUCCESS, bs.add_member(52));
  ASSERT_EQ(OB_SUCCESS, bs.add_member(63));
  ASSERT_EQ(OB_SUCCESS, bs.add_member(64));
  ASSERT_EQ(OB_SUCCESS, bs.add_member(121));
  int64_t pos = 0;
  ASSERT_EQ(OB_INVALID_ARGUMENT, bs.find_next(-2, pos));
  ASSERT_EQ(OB_SUCCESS, bs.find_next(-1, pos));
  ASSERT_EQ(0, pos);
  ASSERT_EQ(OB_SUCCESS, bs.find_first(pos));
  ASSERT_EQ(0, pos);
  ASSERT_EQ(OB_SUCCESS, bs.find_next(pos, pos));
  ASSERT_EQ(5, pos);
  ASSERT_EQ(OB_SUCCESS, bs.find_next(pos, pos));
  ASSERT_EQ(31, pos);
  ASSERT_EQ(OB_SUCCESS, bs.find_next(pos, pos));
  ASSERT_EQ(32, pos);
  ASSERT_EQ(OB_SUCCESS, bs.find_next(pos, pos));
  ASSERT_EQ(52, pos);
  ASSERT_EQ(OB_SUCCESS, bs.find_next(pos, pos));
  ASSERT_EQ(63, pos);
  ASSERT_EQ(OB_SUCCESS, bs.find_next(pos, pos));
  ASSERT_EQ(64, pos);
  ASSERT_EQ(OB_SUCCESS, bs.find_next(pos, pos));
  ASSERT_EQ(121, pos);
  ASSERT_EQ(OB_SUCCESS, bs.find_next(pos, pos));
  ASSERT_EQ(-1, pos);
  ASSERT_EQ(OB_SUCCESS, bs.find_next(128, pos));
  ASSERT_EQ(-1, pos);

  // test clear all
  bs.clear_all();
  ASSERT_EQ(OB_SUCCESS, bs.find_first(pos));
  ASSERT_EQ(-1, pos);

  // test find prev
  ASSERT_EQ(OB_SUCCESS, bs.find_prev(150, pos));
  ASSERT_EQ(-1, pos);
  ASSERT_EQ(OB_SUCCESS, bs.find_prev(128, pos));
  ASSERT_EQ(-1, pos);
  ASSERT_EQ(OB_SUCCESS, bs.find_prev(129, pos));
  ASSERT_EQ(-1, pos);
  ASSERT_EQ(OB_INVALID_ARGUMENT, bs.find_prev(-1, pos));
  ASSERT_EQ(OB_INVALID_ARGUMENT, bs.find_prev(-10, pos));
  ASSERT_EQ(OB_SUCCESS, bs.find_prev(0, pos));
  ASSERT_EQ(-1, pos);
  ASSERT_EQ(OB_SUCCESS, bs.find_prev(28, pos));
  ASSERT_EQ(-1, pos);

  bs.reset();
  ASSERT_EQ(OB_SUCCESS, bs.add_member(90));
  bs.clear_all();
  ASSERT_EQ(OB_SUCCESS, bs.add_member(59));
  ASSERT_EQ(OB_SUCCESS, bs.find_prev(75, pos));
  ASSERT_EQ(59, pos);

  bs.reset();
  ASSERT_EQ(OB_SUCCESS, bs.add_member(31));
  ASSERT_EQ(OB_SUCCESS, bs.find_prev(100, pos));
  ASSERT_EQ(31, pos);

  bs.clear_all();
  ASSERT_EQ(OB_SUCCESS, bs.add_member(0));
  ASSERT_EQ(OB_SUCCESS, bs.add_member(121));

  ASSERT_EQ(OB_SUCCESS, bs.find_prev(120, pos));
  ASSERT_EQ(0, pos);
  ASSERT_EQ(OB_SUCCESS, bs.find_prev(127, pos));
  ASSERT_EQ(121, pos);

  ASSERT_EQ(OB_SUCCESS, bs.add_member(5));
  ASSERT_EQ(OB_SUCCESS, bs.add_member(31));
  ASSERT_EQ(OB_SUCCESS, bs.add_member(32));
  ASSERT_EQ(OB_SUCCESS, bs.add_member(52));
  ASSERT_EQ(OB_SUCCESS, bs.add_member(63));
  ASSERT_EQ(OB_SUCCESS, bs.add_member(96));

  ASSERT_EQ(OB_SUCCESS, bs.find_prev(120, pos));
  ASSERT_EQ(96, pos);
  ASSERT_EQ(OB_SUCCESS, bs.find_prev(pos, pos));
  ASSERT_EQ(63, pos);
  ASSERT_EQ(OB_SUCCESS, bs.find_prev(pos, pos));
  ASSERT_EQ(52, pos);
  ASSERT_EQ(OB_SUCCESS, bs.find_prev(pos, pos));
  ASSERT_EQ(32, pos);
  ASSERT_EQ(OB_SUCCESS, bs.find_prev(pos, pos));
  ASSERT_EQ(31, pos);
  ASSERT_EQ(OB_SUCCESS, bs.find_prev(pos, pos));
  ASSERT_EQ(5, pos);

  ObFixedBitSet<126> bs2;
  // test find next
  ASSERT_EQ(OB_SUCCESS, bs2.add_member(0));
  ASSERT_EQ(OB_SUCCESS, bs2.add_member(5));
  ASSERT_EQ(OB_SUCCESS, bs2.add_member(31));
  ASSERT_EQ(OB_SUCCESS, bs2.add_member(32));
  ASSERT_EQ(OB_SUCCESS, bs2.add_member(52));
  ASSERT_EQ(OB_SUCCESS, bs2.add_member(63));
  ASSERT_EQ(OB_SUCCESS, bs2.add_member(64));
  ASSERT_EQ(OB_SUCCESS, bs2.add_member(121));
  pos = 0;
  ASSERT_EQ(OB_INVALID_ARGUMENT, bs2.find_next(-2, pos));
  ASSERT_EQ(OB_SUCCESS, bs2.find_next(-1, pos));
  ASSERT_EQ(0, pos);
  ASSERT_EQ(OB_SUCCESS, bs2.find_first(pos));
  ASSERT_EQ(0, pos);
  ASSERT_EQ(OB_SUCCESS, bs2.find_next(pos, pos));
  ASSERT_EQ(5, pos);
  ASSERT_EQ(OB_SUCCESS, bs2.find_next(pos, pos));
  ASSERT_EQ(31, pos);
  ASSERT_EQ(OB_SUCCESS, bs2.find_next(pos, pos));
  ASSERT_EQ(32, pos);
  ASSERT_EQ(OB_SUCCESS, bs2.find_next(pos, pos));
  ASSERT_EQ(52, pos);
  ASSERT_EQ(OB_SUCCESS, bs2.find_next(pos, pos));
  ASSERT_EQ(63, pos);
  ASSERT_EQ(OB_SUCCESS, bs2.find_next(pos, pos));
  ASSERT_EQ(64, pos);
  ASSERT_EQ(OB_SUCCESS, bs2.find_next(pos, pos));
  ASSERT_EQ(121, pos);
  ASSERT_EQ(OB_SUCCESS, bs2.find_next(pos, pos));
  ASSERT_EQ(-1, pos);
  ASSERT_EQ(OB_SUCCESS, bs2.find_next(126, pos));
  ASSERT_EQ(-1, pos);
  // test clear all
  bs2.clear_all();
  ASSERT_EQ(OB_SUCCESS, bs2.find_first(pos));
  ASSERT_EQ(-1, pos);
  // test find prev
  ASSERT_EQ(OB_SUCCESS, bs2.find_prev(150, pos));
  ASSERT_EQ(-1, pos);
  ASSERT_EQ(OB_SUCCESS, bs2.find_prev(126, pos));
  ASSERT_EQ(-1, pos);
  ASSERT_EQ(OB_SUCCESS, bs2.find_prev(127, pos));
  ASSERT_EQ(-1, pos);
  ASSERT_EQ(OB_INVALID_ARGUMENT, bs2.find_prev(-1, pos));
  ASSERT_EQ(OB_INVALID_ARGUMENT, bs2.find_prev(-10, pos));
  ASSERT_EQ(OB_SUCCESS, bs2.find_prev(0, pos));
  ASSERT_EQ(-1, pos);
  ASSERT_EQ(OB_SUCCESS, bs2.find_prev(28, pos));
  ASSERT_EQ(-1, pos);
  bs2.reset();
  ASSERT_EQ(OB_SUCCESS, bs2.add_member(90));
  bs2.clear_all();
  ASSERT_EQ(OB_SUCCESS, bs2.add_member(59));
  ASSERT_EQ(OB_SUCCESS, bs2.find_prev(75, pos));
  ASSERT_EQ(59, pos);
  bs2.reset();
  ASSERT_EQ(OB_SUCCESS, bs2.add_member(31));
  ASSERT_EQ(OB_SUCCESS, bs2.find_prev(100, pos));
  ASSERT_EQ(31, pos);
  bs2.clear_all();
  ASSERT_EQ(OB_SUCCESS, bs2.add_member(0));
  ASSERT_EQ(OB_SUCCESS, bs2.add_member(121));
  ASSERT_EQ(OB_SUCCESS, bs2.find_prev(120, pos));
  ASSERT_EQ(0, pos);
  ASSERT_EQ(OB_SUCCESS, bs2.find_prev(125, pos));
  ASSERT_EQ(121, pos);
  ASSERT_EQ(OB_SUCCESS, bs2.add_member(5));
  ASSERT_EQ(OB_SUCCESS, bs2.add_member(31));
  ASSERT_EQ(OB_SUCCESS, bs2.add_member(32));
  ASSERT_EQ(OB_SUCCESS, bs2.add_member(52));
  ASSERT_EQ(OB_SUCCESS, bs2.add_member(63));
  ASSERT_EQ(OB_SUCCESS, bs2.add_member(96));
  ASSERT_EQ(OB_SUCCESS, bs2.find_prev(120, pos));
  ASSERT_EQ(96, pos);
  ASSERT_EQ(OB_SUCCESS, bs2.find_prev(pos, pos));
  ASSERT_EQ(63, pos);
  ASSERT_EQ(OB_SUCCESS, bs2.find_prev(pos, pos));
  ASSERT_EQ(52, pos);
  ASSERT_EQ(OB_SUCCESS, bs2.find_prev(pos, pos));
  ASSERT_EQ(32, pos);
  ASSERT_EQ(OB_SUCCESS, bs2.find_prev(pos, pos));
  ASSERT_EQ(31, pos);
  ASSERT_EQ(OB_SUCCESS, bs2.find_prev(pos, pos));
  ASSERT_EQ(5, pos);


}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
