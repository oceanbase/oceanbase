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
#include "lib/hash/ob_array_index_hash_set.h"
#include "lib/container/ob_array.h"

using namespace oceanbase;
using namespace common;
using namespace hash;

struct Key
{
  int64_t value_;
  bool operator==(const Key & key) const { return value_ == key.value_; }
  uint32_t hash() const { return (uint32_t)value_; }
  TO_STRING_EMPTY();
};

TEST(TestArrayIndexHashSet, intarray)
{
  const static int ARRAY_SIZE = 1024;
  const static int HASH_MAP_SIZE = 1000;
  typedef int array_type[ARRAY_SIZE];

  array_type array;
  ObArrayIndexHashSet<array_type, int, HASH_MAP_SIZE> map(array);
  //ASSERT_EQ(2, sizeof(ObArrayIndexHashSet<array_type, int, HASH_MAP_SIZE>::IndexType));
  for (int i = 0; i < HASH_MAP_SIZE; i++)
  {
    array[i] = i;
    uint64_t idx = 0;
    ASSERT_EQ(OB_ENTRY_NOT_EXIST, map.get_index(array[i], idx));
    ASSERT_EQ(OB_SUCCESS, map.set_index(i));
    ASSERT_EQ(OB_SUCCESS, map.get_index(array[i], idx));
    ASSERT_EQ(static_cast<uint64_t>(i), idx);
  }
  array[HASH_MAP_SIZE] = HASH_MAP_SIZE;
  ASSERT_NE(OB_SUCCESS, map.set_index(HASH_MAP_SIZE));

  map.reset();
  array[2] = 1;
  ASSERT_EQ(OB_SUCCESS, map.set_index(0));
  ASSERT_EQ(OB_SUCCESS, map.set_index(1));
  ASSERT_EQ(OB_ENTRY_EXIST, map.set_index(2));
}

TEST(TestArrayIndexHashSet, keyarray)
{
  const static int HASH_MAP_SIZE = 1000;
  typedef ObArray<Key> array_type;
  array_type array;
  ObArrayIndexHashSet<array_type, Key, HASH_MAP_SIZE> map(array);
  for (int i = 0; i < HASH_MAP_SIZE; i++)
  {
    Key key;
    key.value_ = i;
    array.push_back(key);
    uint64_t idx = 0;
    ASSERT_EQ(OB_ENTRY_NOT_EXIST, map.get_index(array[i], idx));
    ASSERT_EQ(OB_SUCCESS, map.set_index(i));
    ASSERT_EQ(OB_SUCCESS, map.get_index(array[i], idx));
    ASSERT_EQ(static_cast<uint64_t>(i), idx);
  }
  Key key;
  key.value_ = HASH_MAP_SIZE;
  array.push_back(key);
  ASSERT_NE(OB_SUCCESS, map.set_index(HASH_MAP_SIZE));
}

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
