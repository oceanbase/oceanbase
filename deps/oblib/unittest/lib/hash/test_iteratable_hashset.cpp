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

#include "lib/hash/ob_iteratable_hashset.h"
#include "lib/allocator/ob_malloc.h"
#include <gtest/gtest.h>

using namespace oceanbase::common;
using namespace oceanbase::common::hash;
class ObIteratableHashSetTest: public ::testing::Test
{
  public:
    ObIteratableHashSetTest();
    virtual ~ObIteratableHashSetTest();
    virtual void SetUp();
    virtual void TearDown();
  private:
    // disallow copy
    ObIteratableHashSetTest(const ObIteratableHashSetTest &other);
    ObIteratableHashSetTest& operator=(const ObIteratableHashSetTest &other);
  protected:
    // data members
};

ObIteratableHashSetTest::ObIteratableHashSetTest()
{
}

ObIteratableHashSetTest::~ObIteratableHashSetTest()
{
}

void ObIteratableHashSetTest::SetUp()
{
}

void ObIteratableHashSetTest::TearDown()
{
}

TEST_F(ObIteratableHashSetTest, basic_test)
{
  ObIteratableHashSet<int32_t> set;
  for (int round = 0; round < 3; ++round)
  {
    ASSERT_EQ(0, set.count());
    ASSERT_EQ(OB_HASH_NOT_EXIST, set.exist_refactored(1));
    ASSERT_EQ(OB_SUCCESS, set.set_refactored(1));
    ASSERT_EQ(OB_HASH_EXIST, set.exist_refactored(1));
    ASSERT_EQ(1, set.count());
    ASSERT_EQ(OB_HASH_EXIST, set.set_refactored(1));
    ASSERT_EQ(1, set.count());
    ASSERT_EQ(OB_SUCCESS, set.set_refactored(2));
    ASSERT_EQ(OB_HASH_EXIST, set.exist_refactored(1));
    ASSERT_EQ(2, set.count());
    int i = 0;
    ObIteratableHashSet<int32_t>::const_iterator_t it = set.begin();
    for(;it != set.end(); ++it)
    {
      ASSERT_EQ(i+1, *it);
      //printf("%d\n", *it);
      ++i;
    }
    set.clear();
    ASSERT_EQ(0, set.count());

  } // end for
}

/**
 * the following cases are copied from placement_hashset
 *
 */
TEST(TestObIteratableHashSet, single_bucket)
{
  ObIteratableHashSet<int64_t, 1> hashset;
  ASSERT_EQ(OB_SUCCESS, hashset.set_refactored(1));
  ASSERT_EQ(OB_HASH_EXIST, hashset.set_refactored(1));
  ASSERT_EQ(OB_HASH_EXIST, hashset.exist_refactored(1));
}

TEST(TestObIteratableHashSet, many_buckets)
{
  const uint64_t N = 10345;
  ObIteratableHashSet<int64_t, N> hashset;
  for (uint64_t i = 0; i < N; i++)
  {
    ASSERT_EQ(OB_HASH_NOT_EXIST, hashset.exist_refactored(i));
  }
  for (uint64_t i = 0; i < N; i++)
  {
    ASSERT_EQ(OB_SUCCESS, hashset.set_refactored(i));
  }
  ASSERT_EQ(OB_HASH_FULL, hashset.set_refactored(N));
  for (uint64_t i = 0; i < N; i++)
  {
    ASSERT_EQ(OB_HASH_EXIST, hashset.exist_refactored(i));
  }
  for (uint64_t i = 0; i < N; i++)
  {
    ASSERT_EQ(OB_HASH_EXIST, hashset.set_refactored(i));
  }
  ASSERT_EQ(OB_HASH_FULL, hashset.set_refactored(N));
  for (uint64_t i = 0; i < N; i++)
  {
    ASSERT_EQ(OB_HASH_EXIST, hashset.exist_refactored(i));
  }

  hashset.clear();
  for (uint64_t i = 0; i < N; i++)
  {
    ASSERT_EQ(OB_HASH_NOT_EXIST, hashset.exist_refactored(i));
  }
  for (uint64_t i = 0; i < N; i++)
  {
    ASSERT_EQ(OB_SUCCESS, hashset.set_refactored(i));
  }
  ASSERT_EQ(OB_HASH_FULL, hashset.set_refactored(N));
  for (uint64_t i = 0; i < N; i++)
  {
    ASSERT_EQ(OB_HASH_EXIST, hashset.exist_refactored(i));
  }
  for (uint64_t i = 0; i < N; i++)
  {
    ASSERT_EQ(OB_HASH_EXIST, hashset.set_refactored(i));
  }
  ASSERT_EQ(OB_HASH_FULL, hashset.set_refactored(N));
  for (uint64_t i = 0; i < N; i++)
  {
    ASSERT_EQ(OB_HASH_EXIST, hashset.exist_refactored(i));
  }
}

TEST(TestObIteratableHashSet, many_buckets2)
{
  const uint64_t N = 10345;
  ObIteratableHashSet<int64_t, N> hashset;
  for (uint64_t i = N; i > 0; i--)
  {
    ASSERT_EQ(OB_HASH_NOT_EXIST, hashset.exist_refactored(i));
  }
  for (uint64_t i = N; i > 0; i--)
  {
    ASSERT_EQ(OB_SUCCESS, hashset.set_refactored(i));
  }
  ASSERT_EQ(OB_HASH_FULL, hashset.set_refactored(0));
  for (uint64_t i = N; i > 0; i--)
  {
    ASSERT_EQ(OB_HASH_EXIST, hashset.exist_refactored(i));
  }
  for (uint64_t i = N; i > 0; i--)
  {
    ASSERT_EQ(OB_HASH_EXIST, hashset.set_refactored(i));
  }
  ASSERT_EQ(OB_HASH_FULL, hashset.set_refactored(0));
  for (uint64_t i = N; i > 0; i--)
  {
    ASSERT_EQ(OB_HASH_EXIST, hashset.exist_refactored(i));
  }

  hashset.clear();
  for (uint64_t i = N; i > 0; i--)
  {
    ASSERT_EQ(OB_HASH_NOT_EXIST, hashset.exist_refactored(i));
  }
  for (uint64_t i = N; i > 0; i--)
  {
    ASSERT_EQ(OB_SUCCESS, hashset.set_refactored(i));
  }
  ASSERT_EQ(OB_HASH_FULL, hashset.set_refactored(0));
  for (uint64_t i = N; i > 0; i--)
  {
    ASSERT_EQ(OB_HASH_EXIST, hashset.exist_refactored(i));
  }
  for (uint64_t i = N; i > 0; i--)
  {
    ASSERT_EQ(OB_HASH_EXIST, hashset.set_refactored(i));
  }
  ASSERT_EQ(OB_HASH_FULL, hashset.set_refactored(0));
  for (uint64_t i = N; i > 0; i--)
  {
    ASSERT_EQ(OB_HASH_EXIST, hashset.exist_refactored(i));
  }
}

TEST(TestObIteratableHashSet, hash_full)
{
  const uint64_t N = 10000;
  typedef ObIteratableHashSet<int64_t, N> HashSetType;
  HashSetType *hashset = (HashSetType *)ob_malloc(sizeof(HashSetType), ObNewModIds::TEST);
  ASSERT_TRUE(NULL != hashset);
  new(hashset) HashSetType();
  ASSERT_TRUE(NULL != hashset);

  for (uint64_t i = 0; i < N; i++) {
    ASSERT_EQ(OB_SUCCESS, hashset->set_refactored(i));
  }
  for (uint64_t i = N; i < 2 * N; i++) {
    ASSERT_EQ(OB_HASH_FULL, hashset->set_refactored(i));
  }

  hashset->~HashSetType();
  ob_free(hashset);
  hashset = NULL;
}

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("WARN");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
