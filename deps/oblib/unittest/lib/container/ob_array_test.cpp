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
#include "lib/container/ob_array.h"
#include "lib/container/ob_array_serialization.h"
using namespace oceanbase::common;

TEST(ObArrayTest, array_of_array)
{
  ObArray<ObArray<int> > aai;
  {
    ObArray<int> ai;
    for (int j = 0; j < 3; ++j)
    {
      ai.reset();
      for (int i = 0; i < 32; ++i)
      {
        ASSERT_EQ(OB_SUCCESS, ai.push_back(i*j));
      }
      ASSERT_EQ(OB_SUCCESS, aai.push_back(ai));
    } // end for
  }
  for (int j = 0; j < 3; ++j)
  {
    for (int i = 0; i < 32; ++i)
    {
      ASSERT_EQ(i*j, aai.at(j).at(i));
    }
  }
}

TEST(ObArrayTest, test_array_push_back)
{
  const int64_t loop_times = 3;
  const int64_t array_size = 32;
  ObArray<int > array;
  {
    ObArray<int> ai;
    for (int j = 0; j < loop_times; ++j)
    {
      ai.reset();
      for (int i = 0; i < array_size; ++i)
      {
        ASSERT_EQ(OB_SUCCESS, ai.push_back(i*j));
      }
      ASSERT_EQ(OB_SUCCESS, array.push_back(ai));
    } // end for
  }
  ASSERT_EQ(array.count(), loop_times * array_size);
  int64_t idx = 0;
  for (int j = 0; j < loop_times; ++j)
  {
    for (int i = 0; i < array_size; ++i)
    {
      ASSERT_EQ(i*j, array.at(idx++));
    }
  }
}

TEST(ObArrayTest, array_remove)
{
  ObArray<int> ai;
  for (int i = 0; i < 32; ++i)
  {
    ASSERT_EQ(OB_SUCCESS, ai.push_back(i));
  }
  ASSERT_EQ(32, ai.count());

  ASSERT_EQ(OB_ARRAY_OUT_OF_RANGE, ai.remove(-1));
  ASSERT_EQ(OB_ARRAY_OUT_OF_RANGE, ai.remove(32));
  ASSERT_EQ(32, ai.count());

  ASSERT_EQ(OB_SUCCESS, ai.remove(10));
  int v = 0;
  ASSERT_EQ(31, static_cast<int>(ai.count()));
  for (int i = 0; i < 31; ++i)
  {
    ASSERT_EQ(OB_SUCCESS, ai.at(static_cast<int64_t>(i), v));
    ASSERT_NE(10, v);
  }
}

TEST(ObArrayTest, serialize)
{
  char buf[1024];
  ObSArray<int64_t> array;
  const int64_t NUM = 20;
  for (int64_t i = 0; i < NUM; i ++)
  {
    array.push_back(i);
  }

  int64_t pos = 0;
  array.serialize(buf, 1024, pos);

  ObSArray<int64_t> array2;
  int64_t data_len = pos;
  pos = 0;
  array2.push_back(30);
  array2.deserialize(buf, data_len, pos);

  ASSERT_EQ(array2.count(), NUM);
  for (int64_t i = 0; i < NUM; i ++)
  {
    ASSERT_EQ(i, array2.at(i));
  }
}

class TestItem
{
public:
  static int64_t construct_cnt_;
  static int64_t copy_construct_cnt_;
  static int64_t destruct_cnt_;
  static int64_t assign_cnt_;

  static void clear(void)
  {
    construct_cnt_ = 0;
    copy_construct_cnt_ = 0;
    destruct_cnt_ = 0;
    assign_cnt_ = 0;
  }
  TestItem()
  {
    construct_cnt_++;
  }

  TestItem(const TestItem &)
  {
    copy_construct_cnt_++;
  }
  ~TestItem()
  {
    destruct_cnt_++;
  }

  TestItem &operator=(const TestItem &)
  {
    assign_cnt_++;
    return *this;
  }
  TO_STRING_EMPTY();
};

int64_t TestItem::construct_cnt_ = 0;
int64_t TestItem::copy_construct_cnt_ = 0;
int64_t TestItem::destruct_cnt_ = 0;
int64_t TestItem::assign_cnt_ = 0;

TEST(ObArrayTest, test_item)
{
  TestItem::clear();
  TestItem item;
  ASSERT_EQ(1, TestItem::construct_cnt_);

  {
    TestItem item2(item);
    ASSERT_EQ(1, TestItem::copy_construct_cnt_);

    item2 = item;
    ASSERT_EQ(1, TestItem::assign_cnt_);
  }
  ASSERT_EQ(1, TestItem::destruct_cnt_);
}

TEST(ObArrayTest, item_life_cycle)
{
  TestItem::clear();
  typedef ObArray<TestItem> ItemArray;

  const int64_t block_item_cnt = 8;
  const int64_t block_size = sizeof(TestItem) * block_item_cnt;
  TestItem::clear();
  ItemArray array(block_size);
  ASSERT_EQ(block_size, array.get_block_size());
  array.reserve(block_item_cnt);

  ASSERT_EQ(0, TestItem::construct_cnt_);
  TestItem item;
  ASSERT_EQ(1, TestItem::construct_cnt_);
  ASSERT_EQ(OB_SUCCESS, array.push_back(item));
  ASSERT_EQ(1, TestItem::copy_construct_cnt_);

  array.pop_back();
  ASSERT_EQ(0, array.count());
  ASSERT_EQ(OB_SUCCESS, array.push_back(item));
  ASSERT_EQ(1, TestItem::copy_construct_cnt_);
  ASSERT_EQ(1, TestItem::assign_cnt_);

  array.reset();
  ASSERT_EQ(0, array.count());
  ASSERT_EQ(OB_SUCCESS, array.push_back(item));
  ASSERT_EQ(2, TestItem::copy_construct_cnt_);
  ASSERT_EQ(1, TestItem::assign_cnt_);

  ASSERT_EQ(1, TestItem::destruct_cnt_);

  ASSERT_EQ(OB_SUCCESS, array.push_back(item));
  array.pop_back(); // count_ = 1; valid_count_ = 2
  ASSERT_EQ(1, array.count());

  TestItem::clear();
  {
    ItemArray array2(block_size);
    array2 = array;
    ASSERT_EQ(0, TestItem::construct_cnt_);
    ASSERT_EQ(1, TestItem::copy_construct_cnt_);
    ASSERT_EQ(OB_SUCCESS, array2.push_back(item));
    ASSERT_EQ(OB_SUCCESS, array2.push_back(item));
    ASSERT_EQ(3, TestItem::copy_construct_cnt_);
    TestItem::clear();

    array = array2;
    ASSERT_EQ(3, TestItem::copy_construct_cnt_);
    ASSERT_EQ(0, TestItem::assign_cnt_);

    for (int64_t i = array2.count(); i < block_item_cnt; i++)
    {
      ASSERT_EQ(OB_SUCCESS, array2.push_back(item));
    }
    ASSERT_EQ(block_item_cnt, array2.count());
    TestItem::clear();
    ASSERT_EQ(OB_SUCCESS, array2.push_back(item));
    ASSERT_EQ(block_item_cnt + 1, TestItem::copy_construct_cnt_);
    ASSERT_EQ(block_item_cnt, TestItem::destruct_cnt_);

    array2.pop_back();
    TestItem::clear();
    array2.reset(); // more than one block, call destroy
    ASSERT_EQ(block_item_cnt + 1, TestItem::destruct_cnt_);
    array2 = array;
    TestItem::clear();
  }
  int64_t cnt = array.count();
  ASSERT_EQ(cnt, TestItem::destruct_cnt_);

  array.pop_back();
  TestItem::clear();
  array.reserve(cnt + block_item_cnt);
  ASSERT_EQ(cnt - 1, TestItem::copy_construct_cnt_);
  ASSERT_EQ(cnt, TestItem::destruct_cnt_);

  // extend buffer while valid_count_ > count_
  TestItem::clear();
  {
    ItemArray array2(block_size);

    cnt = 3;
    for (int64_t i = 0; i < cnt; i++)
    {
      ASSERT_EQ(OB_SUCCESS, array2.push_back(item));
    }
    array2.reuse();
    array2.reserve(block_item_cnt * 2);
  }
  ASSERT_EQ(cnt, TestItem::destruct_cnt_);

  // alloc_place_holder
  TestItem::clear();
  {
    ItemArray array2(block_size);

    cnt = 3;
    for (int64_t i = 0; i < cnt; i++)
    {
      ASSERT_TRUE(NULL != array2.alloc_place_holder());
    }
    ASSERT_EQ(cnt, array2.count());
    array2.reuse();
  }
  ASSERT_EQ(cnt, TestItem::destruct_cnt_);
}

TEST(ObArrayTest, memory_leak)
{
  typedef ObArray<int> IntArray;
  typedef ObArray<IntArray> IntArrayArray;

  IntArray array;
  ASSERT_EQ(OB_SUCCESS, array.push_back(0));
  IntArrayArray array_array;
  ob_print_mod_memory_usage();
  ASSERT_EQ(OB_SUCCESS, array_array.push_back(array));
  array_array.reset();
  for (int  i = 0; i < 1024 * 1024; i++)
  {
    ASSERT_EQ(OB_SUCCESS, array_array.push_back(array));
    array_array.reset();
  }
  ob_print_mod_memory_usage();
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
