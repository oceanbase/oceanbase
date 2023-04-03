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
#include "lib/hash/ob_placement_hashmap.h"
#include "storage/ob_col_map.h"
using namespace oceanbase::common;
using namespace oceanbase::common::hash;

TEST(TestObPlacementHashMap, single_bucket)
{
  ObPlacementHashMap<int64_t, int64_t, 1> hashmap;
  int64_t v;
  ASSERT_EQ(OB_SUCCESS, hashmap.set_refactored(1, 1));
  ASSERT_EQ(OB_HASH_EXIST, hashmap.set_refactored(1, 1));
  ASSERT_EQ(OB_SUCCESS, hashmap.set_refactored(1, 1, 1));
  ASSERT_EQ(OB_SUCCESS, hashmap.set_refactored(1, 1, 1, 1));
  ASSERT_EQ(OB_SUCCESS, hashmap.get_refactored(1, v));
  ASSERT_EQ(1, v);
  ASSERT_EQ(1, *hashmap.get(1));
}

TEST(TestObPlacementHashMap, many_buckets)
{
  const uint64_t N = 10345;
  int64_t v = 0;
  ObPlacementHashMap<int64_t, int64_t, N> hashmap;
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

TEST(TestObPlacementHashMap, many_buckets2)
{
  const uint64_t N = 10345;
  int64_t v = 0;
  ObPlacementHashMap<int64_t, int64_t, N> hashmap;
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

TEST(TestObPlacementHashMap, performance)
{
  //const int64_t round = 10240;
  const int64_t round = 1;
  int64_t begin = ObTimeUtility::current_time();
  for (int64_t i = 0; i < round; ++i) {
    ObPlacementHashMap<uint64_t, int64_t> hashmap;
    for (uint64_t j = 0; j < 4; ++j) {
      ASSERT_EQ(OB_SUCCESS, hashmap.set_refactored(j+16LL, i));
    } // end for
  } // end for
  int64_t end = ObTimeUtility::current_time();
  int64_t elapsed = end - begin;
  COMMON_LOG(INFO, "time", K(elapsed), K(((double)elapsed)/(4*round)));
}

TEST(TestObPlacementHashMap, col_map_performance)
{
  const uint64_t round = 10240;
  int64_t begin = ObTimeUtility::current_time();
  ObArenaAllocator alloc(ObModIds::TEST);
  int64_t elapsed_t1 = 0;
  int64_t elapsed_t2 = 0;
  int64_t max_t2 = 0;
  uint64_t max_round = 0;
  typedef ObPlacementHashMap<uint64_t, int64_t, 511> my_hashmap_t;
  for (uint64_t i = 0; i < round; ++i) {
    int64_t t1_beg = ObTimeUtility::current_time();
    char* ptr = (char*)alloc.alloc(sizeof(my_hashmap_t));
    //char* ptr = (char*)alloc2.alloc(sizeof(my_hashmap_t));
    ASSERT_TRUE(NULL != ptr);
    my_hashmap_t *p_hashmap = new(ptr) my_hashmap_t();
    //my_hashmap_t *p_hashmap = new my_hashmap_t();
    my_hashmap_t &hashmap = *p_hashmap;
    //ASSERT_EQ(OB_SUCCESS, hashmap.init(alloc, 4));

    int64_t t1_end = ObTimeUtility::current_time();
    elapsed_t1 += t1_end - t1_beg;
    for (int64_t j = 0; j < 4; ++j) {
      //int64_t set_beg = ObTimeUtility::current_time();
      //ASSERT_EQ(OB_OB_SUCCESS, );
      hashmap.set_refactored(j+16LL, i);
      //int64_t set_end = ObTimeUtility::current_time();
      //printf("set timeu=%ld set_end=%ld\n", set_end-set_beg, set_end);
    } // end for
    int64_t t2_end = ObTimeUtility::current_time();
    if (t2_end - t1_end > max_t2) {
      max_t2 = t2_end - t1_end;
      max_round = i;
    }
    elapsed_t2 += t2_end - t1_end;
    p_hashmap->~my_hashmap_t();
  } // end for
  int64_t end = ObTimeUtility::current_time();
  int64_t elapsed = end - begin;
  COMMON_LOG(INFO, "time", K(elapsed), K(((double)elapsed)/(4*round)));
  COMMON_LOG(INFO, "t1 time: construct", K(elapsed_t1), K(((double)elapsed_t1)/(round)));
  COMMON_LOG(INFO, "t2 time: set", K(elapsed_t2), K(((double)elapsed_t2)/(4*round)));
  COMMON_LOG(INFO, "max set time", K(max_t2/4), K(max_round));
}

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
