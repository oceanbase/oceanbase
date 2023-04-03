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
#include "lib/hash/ob_pointer_hashmap.h"

using namespace oceanbase;
using namespace common;
using namespace hash;

struct PairValue
{
  int64_t key_;
  int64_t value_;

  PairValue()
  : key_(0),
  value_(0)
  {

  }

  PairValue(const int64_t key, const int64_t value)
      : key_(key), value_(value)
  {

  }

  int64_t get_key() const
  {
    return key_;
  }
};

template <class K, class V>
struct GetKey
{
  K operator()(const V value) const
  {
    return value->get_key();
  }
};

TEST(TestObPointerHashMap, basic_test)
{
  ObPointerHashMap<int64_t, PairValue *, GetKey> hashmap;
  PairValue val1(1, 1);
  PairValue val2(2, 2);
  PairValue *val = NULL;
  ASSERT_EQ(OB_SUCCESS, hashmap.set_refactored(1, &val1));
  ASSERT_EQ(OB_HASH_EXIST, hashmap.set_refactored(1, &val1));
  ASSERT_EQ(OB_SUCCESS, hashmap.set_refactored(1, &val1, 1));
  ASSERT_EQ(OB_SUCCESS, hashmap.set_refactored(1, &val1, 1, 1));
  ASSERT_EQ(OB_SUCCESS, hashmap.get_refactored(1, val));
  ASSERT_EQ(1, val->value_);
  ASSERT_EQ(1, (*hashmap.get(1))->value_);
  ASSERT_EQ(1, hashmap.count());
  ASSERT_EQ(1, hashmap.item_count());

  ASSERT_EQ(OB_SUCCESS, hashmap.set_refactored(2, &val2));
  ASSERT_EQ(OB_SUCCESS, hashmap.get_refactored(2, val));
  ASSERT_EQ(2, val->value_);
  ASSERT_EQ(2, hashmap.count());
  ASSERT_EQ(2, hashmap.item_count());

  ASSERT_EQ(OB_SUCCESS, hashmap.erase_refactored(2L));
  ASSERT_EQ(OB_HASH_NOT_EXIST, hashmap.erase_refactored(2L));
  ASSERT_EQ(OB_HASH_NOT_EXIST, hashmap.get_refactored(2, val));
  ASSERT_EQ(1, hashmap.item_count());

  hashmap.clear();
  ASSERT_EQ(0, hashmap.count());
  ASSERT_EQ(0, hashmap.item_count());
}

//TODO
TEST(TestObPointerHashMap, test_erase)
{
  ObPointerHashMap<int64_t, PairValue *, GetKey, 8 * 1024> hashmap;
  PairValue pair;
  pair.key_ = 1;
  for (int64_t i = 0; i < 11000; ++i) {
    pair.value_ = i;
    ASSERT_EQ(OB_SUCCESS, hashmap.set_refactored(pair.key_, &pair));
    ASSERT_EQ(OB_SUCCESS, hashmap.erase_refactored(pair.key_));
  }
}

TEST(TestObPointerHashMap, test_erase_many)
{
  const int64_t size_16k = 16 * 1024;
  const int64_t count = 1000;
  ObPointerHashMap<int64_t, PairValue *, GetKey, size_16k> hashmap; // 16K can contain 1400+ ponter

  for (int64_t j = 0; j < 1000; ++j) { // repeat 100 times, and will not extends
    PairValue *pair = new PairValue[count];
    for (int64_t i = 0; i < count; ++i) {
      static int64_t value = 0;
      value++;
      pair[i].key_ = value;
      pair[i].value_ = value;
      ASSERT_EQ(OB_SUCCESS, hashmap.set_refactored(pair[i].key_, &pair[i], 1, 1));
    }
    for (int64_t i = 0; i < count; ++i) {
      ASSERT_EQ(OB_SUCCESS, hashmap.erase_refactored(pair[i].key_));
    }
    printf("item_count=%ld, count=%ld\n", hashmap.item_count(), hashmap.count());
    delete []pair;
    pair = NULL;
  }
  ASSERT_EQ(size_16k, hashmap.get_sub_map_mem_size());
}

TEST(TestObPointerHashMap, test_two_submap)
{
  ObPointerHashMap<int64_t, PairValue *, GetKey> hashmap;

  PairValue *pairs = new PairValue[300000];
  for (int64_t i = 0; i < 300000; ++i) {
    pairs[i].key_ = i;
    pairs[i].value_ = i;
  }

  PairValue *val = NULL;
  for (int64_t i = 0; i < 300000; ++i) {
    ASSERT_EQ(OB_SUCCESS, hashmap.set_refactored(pairs[i].key_, &pairs[i]));
    ASSERT_EQ(OB_SUCCESS, hashmap.get_refactored(pairs[i].key_, val));
    ASSERT_EQ(i, val->value_);
    ASSERT_EQ(i + 1, hashmap.count());
    ASSERT_EQ(i + 1, hashmap.item_count());
  }
  ASSERT_EQ(300000, hashmap.count());
  ASSERT_EQ(300000, hashmap.item_count());

  for (int64_t i = 0; i < 300000; i += 100) {
    ASSERT_EQ(OB_SUCCESS, hashmap.set_refactored(pairs[i].key_, &pairs[i], 1));
    ASSERT_EQ(OB_SUCCESS, hashmap.get_refactored(pairs[i].key_, val));
    ASSERT_EQ(i, val->value_);
  }
  ASSERT_EQ(300000, hashmap.count());
  ASSERT_EQ(300000, hashmap.item_count());

  for (int64_t i = 0; i < 300000; i += 100) {
    ASSERT_EQ(OB_SUCCESS, hashmap.erase_refactored(pairs[i].key_));
    ASSERT_EQ(OB_HASH_NOT_EXIST, hashmap.erase_refactored(pairs[i].key_));
    ASSERT_EQ(OB_HASH_NOT_EXIST, hashmap.get_refactored(pairs[i].key_, val));
  }
  ASSERT_EQ(300000, hashmap.count());
  ASSERT_EQ(300000 - 3000, hashmap.item_count());

  // test reuse all erased pos
  for (int64_t i = 0; i < 300000; i += 100) {
    ASSERT_EQ(OB_SUCCESS, hashmap.set_refactored(pairs[i].key_, &pairs[i]));
    ASSERT_EQ(OB_SUCCESS, hashmap.get_refactored(pairs[i].key_, val));
    ASSERT_EQ(i, val->value_);
  }
  ASSERT_EQ(300000, hashmap.count());
  ASSERT_EQ(300000, hashmap.item_count());

  hashmap.clear();
  ASSERT_EQ(0, hashmap.count());
  ASSERT_EQ(0, hashmap.item_count());

  //reuse ok
  for (int64_t i = 0; i < 300000; ++i) {
    ASSERT_EQ(OB_SUCCESS, hashmap.set_refactored(pairs[i].key_, &pairs[i]));
    ASSERT_EQ(OB_SUCCESS, hashmap.get_refactored(pairs[i].key_, val));
    ASSERT_EQ(i, val->value_);
    ASSERT_EQ(i + 1, hashmap.count());
  }
  ASSERT_EQ(300000, hashmap.count());
  ASSERT_EQ(300000, hashmap.item_count());

  //copy
  ObPointerHashMap<int64_t, PairValue *, GetKey> hashmap_copy(hashmap);
  for (int64_t i = 0; i < 300000; ++i) {
    ASSERT_EQ(OB_SUCCESS, hashmap_copy.get_refactored(pairs[i].key_, val));
    ASSERT_EQ(i, val->value_);
  }
  ASSERT_EQ(300000, hashmap_copy.count());
  ASSERT_EQ(300000, hashmap_copy.item_count());

  delete [] pairs;
}

TEST(TestObPointerHashMap, test_large_pairs)
{
  ObPointerHashMap<int64_t, PairValue *, GetKey> hashmap;

  int64_t pair_count = 1000000;
  PairValue *pairs = new PairValue[pair_count];
  for (int64_t i = 0; i < pair_count; ++i) {
    pairs[i].key_ = i;
    pairs[i].value_ = i;
  }

  PairValue *val = NULL;
  for (int64_t i = 0; i < pair_count; ++i) {
    ASSERT_EQ(OB_SUCCESS, hashmap.set_refactored(pairs[i].key_, &pairs[i]));
    ASSERT_EQ(OB_SUCCESS, hashmap.get_refactored(pairs[i].key_, val));
    ASSERT_EQ(i, val->value_);
    ASSERT_EQ(i + 1, hashmap.count());
    ASSERT_EQ(i + 1, hashmap.item_count());
  }
  ASSERT_EQ(pair_count, hashmap.count());
  ASSERT_EQ(pair_count, hashmap.item_count());

  delete [] pairs;
}

TEST(TestObPointerHashMap, test_micro_benchmark)
{
  ObPointerHashMap<int64_t, PairValue *, GetKey> hashmap;

  int64_t pair_count = 250000;
  PairValue *pairs = new PairValue[pair_count];
  for (int64_t i = 0; i < pair_count; ++i) {
    pairs[i].key_ = i;
    pairs[i].value_ = i;
  }

  int64_t start_time = ::oceanbase::common::ObTimeUtility::current_time();
  for (int64_t i = 0; i < pair_count; ++i) {
    hashmap.set_refactored(pairs[i].key_, &pairs[i]);
    if (0 == i % 3) {
      hashmap.erase_refactored(pairs[i].key_);
    }
  }
  int64_t end_time = ::oceanbase::common::ObTimeUtility::current_time();
  int64_t set_time = end_time - start_time;

  PairValue *val = NULL;
  start_time = ::oceanbase::common::ObTimeUtility::current_time();
  for (int64_t j = 0; j < 10; ++ j) {
    for (int64_t i = 0; i < pair_count; ++i) {
      if (0 != i % 3) {
        ASSERT_EQ(OB_SUCCESS, hashmap.get_refactored(pairs[i].key_, val));
      } else {
        ASSERT_EQ(OB_HASH_NOT_EXIST, hashmap.get_refactored(pairs[i].key_, val));
      }
    }
  }
  end_time = ::oceanbase::common::ObTimeUtility::current_time();
  int64_t get_time = end_time - start_time;
  int64_t set_count = pair_count * 4 / 3;
  printf("hash map set_count=%ld, set_time=%ld, set_rate=%ld, get_count=%ld, "
         "get_time=%ld, get_rate=%ld\n",
         set_count, set_time, set_count * 1000L * 1000L / set_time,
         pair_count * 10, get_time, pair_count * 10 * 1000L * 1000L / get_time);

  delete [] pairs;
}

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
