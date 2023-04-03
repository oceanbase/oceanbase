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
#include "lib/hash/ob_build_in_hashmap.h"

using namespace oceanbase;
using namespace common;
using namespace hash;

struct PairValue
{
  int64_t key_;
  int64_t value_;
  LINK(PairValue, hash_link_);

  PairValue()
      : key_(0),
        value_(0)
  {
    memset(&hash_link_, 0, sizeof(hash_link_));
  }

  PairValue(const int64_t key, const int64_t value)
      : key_(key), value_(value)
  {
    memset(&hash_link_, 0, sizeof(hash_link_));
  }
};

struct KVHashing
{
  typedef int64_t Key;
  typedef PairValue Value;
  typedef ObDLList(PairValue, hash_link_) ListHead;

  static uint64_t hash(Key key) { return do_hash(key); }
  static Key key(Value const* value) { return value->key_; }
  static bool equal(Key lhs, Key rhs) { return lhs == rhs; }
};

TEST(TestObBuildInHashMap, basic_test)
{
  ObBuildInHashMap<KVHashing> hashmap;
  PairValue val1(1, 1);
  PairValue val2(2, 2);
  PairValue *val = NULL;
  ASSERT_EQ(OB_SUCCESS, hashmap.set_refactored(&val1));
  ASSERT_EQ(OB_HASH_EXIST, hashmap.set_refactored(&val1));
  ASSERT_EQ(OB_SUCCESS, hashmap.get_refactored(1, val));
  ASSERT_EQ(1, val->value_);
  ASSERT_EQ(1, (hashmap.get(1))->value_);
  ASSERT_EQ(1, hashmap.count());

  ASSERT_EQ(OB_SUCCESS, hashmap.set_refactored(&val2));
  ASSERT_EQ(OB_SUCCESS, hashmap.get_refactored(2, val));
  ASSERT_EQ(2, val->value_);
  ASSERT_EQ(2, (hashmap.get(2))->value_);
  ASSERT_EQ(2, hashmap.count());

  ASSERT_EQ(OB_SUCCESS, hashmap.erase_refactored(2L));
  ASSERT_EQ(OB_HASH_NOT_EXIST, hashmap.erase_refactored(2L));
  ASSERT_EQ(1, hashmap.count());

  hashmap.reset();
  ASSERT_EQ(0, hashmap.count());
}

TEST(TestObPointerHashMap, test_many_items)
{
  ObBuildInHashMap<KVHashing, 8 * 1024> hashmap;

  int64_t pair_count = 90000;
  PairValue *pairs = new PairValue[pair_count];
  memset(pairs, 0, pair_count * sizeof(PairValue));
  for (int64_t i = 0; i < pair_count; ++i) {
    pairs[i].key_ = i;
    pairs[i].value_ = i;
  }

  PairValue *val = NULL;
  for (int64_t i = 0; i < pair_count; ++i) {
    ASSERT_EQ(OB_SUCCESS, hashmap.set_refactored(&pairs[i]));
    ASSERT_EQ(OB_SUCCESS, hashmap.get_refactored(pairs[i].key_, val));
    ASSERT_EQ(i, val->value_);
    ASSERT_EQ(i, (hashmap.get(pairs[i].key_))->value_);
    ASSERT_EQ(i + 1, hashmap.count());
  }
  ASSERT_EQ(pair_count, hashmap.count());

  ObBuildInHashMap<KVHashing, 8 * 1024>::iterator it = hashmap.begin();
  int64_t count = 0;
  for (; it != hashmap.end(); ++it) {
    ++count;
  }
  ASSERT_EQ(pair_count, count);

  delete [] pairs;
}

TEST(TestObPointerHashMap, test_micro_benchmark)
{
  ObBuildInHashMap<KVHashing, 256 * 1024> *hashmap = new ObBuildInHashMap<KVHashing, 256 * 1024>();

  int64_t pair_count = 250000;
  PairValue *pairs = new PairValue[pair_count];
  memset(pairs, 0, pair_count * sizeof(PairValue));
  for (int64_t i = 0; i < pair_count; ++i) {
    pairs[i].key_ = i;
    pairs[i].value_ = i;
  }

  int64_t start_time = ::oceanbase::common::ObTimeUtility::current_time();
  for (int64_t i = 0; i < pair_count; ++i) {
    hashmap->set_refactored(&pairs[i]);
  }
  int64_t end_time = ::oceanbase::common::ObTimeUtility::current_time();
  int64_t set_time = end_time - start_time;

  PairValue *val = NULL;
  start_time = ::oceanbase::common::ObTimeUtility::current_time();
  for (int64_t j = 0; j < 10; ++ j) {
    for (int64_t i = 0; i < pair_count; ++i) {
      ASSERT_EQ(OB_SUCCESS, hashmap->get_refactored(pairs[i].key_, val));
    }
  }
  end_time = ::oceanbase::common::ObTimeUtility::current_time();
  int64_t get_time = end_time - start_time;
  int64_t set_count = pair_count;
  printf("hash map set_count=%ld, set_time=%ld, set_rate=%ld, get_count=%ld, "
         "get_time=%ld, get_rate=%ld\n",
         set_count, set_time, set_count * 1000L * 1000L / set_time,
         pair_count * 10, get_time, pair_count * 10 * 1000L * 1000L / get_time);

  delete hashmap;
  delete [] pairs;
}

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
