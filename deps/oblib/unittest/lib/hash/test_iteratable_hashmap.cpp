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

#include "lib/hash/ob_iteratable_hashmap.h"
#include <gtest/gtest.h>
using namespace oceanbase::common;
using namespace oceanbase::common::hash;
class TestIteratableHashMap: public ::testing::Test
{
  public:
    TestIteratableHashMap();
    virtual ~TestIteratableHashMap();
    virtual void SetUp();
    virtual void TearDown();
  private:
    // disallow copy
    TestIteratableHashMap(const TestIteratableHashMap &other);
    TestIteratableHashMap& operator=(const TestIteratableHashMap &other);
  protected:
    // data members
};

TestIteratableHashMap::TestIteratableHashMap()
{
}

TestIteratableHashMap::~TestIteratableHashMap()
{
}

void TestIteratableHashMap::SetUp()
{
}

void TestIteratableHashMap::TearDown()
{
}

TEST_F(TestIteratableHashMap, iterator_test)
{
  ObIteratableHashMap<int32_t, int32_t> map;
  for (int round = 0; round < 3; ++round)
  {
    _OB_LOG(INFO, "round=%d", round);
    ASSERT_EQ(map.begin(), map.end());
    ASSERT_EQ(0, map.count());
    ASSERT_EQ(OB_SUCCESS, map.set_refactored(1, 2));
    ASSERT_EQ(1, map.count());
    ASSERT_EQ(OB_HASH_EXIST, map.set_refactored(1, 2));
    ASSERT_EQ(1, map.count());
    ASSERT_EQ(OB_SUCCESS, map.set_refactored(2, 3));
    ASSERT_EQ(OB_HASH_EXIST, map.set_refactored(2, 3));
    ASSERT_EQ(2, map.count());
    //
    int i = 0;
    ObIteratableHashMap<int32_t, int32_t>::const_iterator_t it = map.begin();
    for (;it != map.end(); ++it)
    {
      //printf("%d %d\n", (*it).first, (*it).second);
      ASSERT_EQ(i+1, (*it).first);
      ASSERT_EQ(i+2, (*it).second);
      i++;
    }
    ASSERT_EQ(OB_SUCCESS, map.set_refactored(3, 4));
    ASSERT_EQ(3, map.count());
    i = 0;
    it = map.begin();
    for (;it != map.end(); ++it)
    {
      //printf("%d %d\n", (*it).first, (*it).second);
      ASSERT_EQ(i+1, (*it).first);
      ASSERT_EQ(i+2, (*it).second);
      i++;
    }
    map.reuse();
    ASSERT_EQ(0, map.count());
  } // end for
}

/**
 * tests copy from placement_hashmap test
 *
 */

TEST(TestObIteratableHashMap, single_bucket)
{
  ObIteratableHashMap<int64_t, int64_t, 1> hashmap;
  int64_t v;
  ASSERT_EQ(OB_SUCCESS, hashmap.set_refactored(1, 1));
  ASSERT_EQ(OB_HASH_EXIST, hashmap.set_refactored(1, 1));
  ASSERT_EQ(OB_SUCCESS, hashmap.set_refactored(1, 1, 1));
  ASSERT_EQ(OB_SUCCESS, hashmap.set_refactored(1, 1, 1, 1));
  ASSERT_EQ(OB_SUCCESS, hashmap.get_refactored(1, v));
  ASSERT_EQ(1, v);
  ASSERT_EQ(1, *hashmap.get(1));
}

TEST(TestObIteratableHashMap, many_buckets)
{
  const uint64_t N = 10345;
  int64_t v = 0;
  ObIteratableHashMap<int64_t, int64_t, N> hashmap;
  for (uint64_t i = 0; i < N; i++)
  {
    ASSERT_EQ(OB_HASH_NOT_EXIST, hashmap.get_refactored(i, v));
    ASSERT_EQ(NULL, hashmap.get(i));
  }
  for (uint64_t i = 0; i < N; i++)
  {
    ASSERT_EQ(OB_SUCCESS, hashmap.set_refactored(i, i * i));
  }
  ASSERT_EQ(OB_HASH_FULL, hashmap.set_refactored(N, N * N));
  for (uint64_t i = 0; i < N; i++)
  {
    ASSERT_EQ(OB_SUCCESS, hashmap.get_refactored(i, v));
    ASSERT_EQ(static_cast<int64_t>(i * i) , v);
    ASSERT_EQ(static_cast<int64_t>(i * i), *hashmap.get(i));
  }
  for (uint64_t i = 0; i < N; i++)
  {
    ASSERT_EQ(OB_SUCCESS, hashmap.set_refactored(i, i * i, 1));
    ASSERT_EQ(OB_SUCCESS, hashmap.set_refactored(i, i * i, 1, 1));
  }
  ASSERT_EQ(OB_HASH_FULL, hashmap.set_refactored(N, N * N));
  for (uint64_t i = 0; i < N; i++)
  {
    ASSERT_EQ(OB_SUCCESS, hashmap.get_refactored(i, v));
    ASSERT_EQ(static_cast<int64_t>(i * i) , v);
    ASSERT_EQ(static_cast<int64_t>(i * i), *hashmap.get(i));
  }
}

TEST(TestObIteratableHashMap, many_buckets2)
{
  const uint64_t N = 10345;
  int64_t v = 0;
  ObIteratableHashMap<int64_t, int64_t, N> hashmap;
  for (uint64_t i = N; i > 0; i--)
  {
    ASSERT_EQ(OB_HASH_NOT_EXIST, hashmap.get_refactored(i, v));
    ASSERT_EQ(NULL, hashmap.get(i));
  }
  for (uint64_t i = N; i > 0; i--)
  {
    ASSERT_EQ(OB_SUCCESS, hashmap.set_refactored(i, i * i));
  }
  ASSERT_EQ(OB_HASH_FULL, hashmap.set_refactored(0, 0));
  for (uint64_t i = N; i > 0; i--)
  {
    ASSERT_EQ(OB_SUCCESS, hashmap.get_refactored(i, v));
    ASSERT_EQ(static_cast<int64_t>(i * i) , v);
    ASSERT_EQ(static_cast<int64_t>(i * i), *hashmap.get(i));
  }
  for (uint64_t i = N; i > 0; i--)
  {
    ASSERT_EQ(OB_SUCCESS, hashmap.set_refactored(i, i * i, 1));
    ASSERT_EQ(OB_SUCCESS, hashmap.set_refactored(i, i * i, 1, 1));
  }
  ASSERT_EQ(OB_HASH_FULL, hashmap.set_refactored(0, 0));
  for (uint64_t i = N; i > 0; i--)
  {
    ASSERT_EQ(OB_SUCCESS, hashmap.get_refactored(i, v));
    ASSERT_EQ(static_cast<int64_t>(i * i) , v);
    ASSERT_EQ(static_cast<int64_t>(i * i), *hashmap.get(i));
  }
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
