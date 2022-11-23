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

#include "gtest/gtest.h"
#include "lib/hash/ob_placement_hashset.h"

using namespace oceanbase;
using namespace common;
using namespace hash;

TEST(TestObPlacementHashSet, single_bucket)
{
  ObPlacementHashSet<int64_t, 1> hashset;
  ASSERT_EQ(OB_SUCCESS, hashset.set_refactored(1));
  ASSERT_EQ(OB_HASH_EXIST, hashset.set_refactored(1));
  ASSERT_EQ(OB_HASH_EXIST, hashset.exist_refactored(1));
}

TEST(TestObPlacementHashSet, many_buckets)
{
  const uint64_t N = 10345;
  ObPlacementHashSet<int64_t, N> hashset;
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

TEST(TestObPlacementHashSet, many_buckets2)
{
  const uint64_t N = 10345;
  ObPlacementHashSet<int64_t, N> hashset;
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

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
