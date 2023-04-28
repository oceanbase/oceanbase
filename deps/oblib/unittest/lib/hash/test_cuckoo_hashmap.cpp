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
#include "lib/allocator/page_arena.h"
#include "lib/hash/ob_cuckoo_hashmap.h"
#include "lib/hash/ob_hashmap.h"

using namespace oceanbase;
using namespace oceanbase::common;

struct Key
{
public:
  Key()
    : key_(0)
  {}
  explicit Key(int64_t k)
    : key_(k)
  {}
  uint64_t hash() const { return murmurhash(&key_, sizeof(key_), 0); }
  int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; }
  bool operator==(const Key &other) const { return key_ == other.key_; }
  TO_STRING_KV(K_(key));
  int64_t key_;
};

struct MacroKey
{
public:
  static const uint64_t HASH_MAGIC_NUM = 2654435761;
  MacroKey()
    : key_(0)
  {}
  explicit MacroKey(int64_t k)
    : key_(k)
  {}
  uint64_t hash() const { return key_ * HASH_MAGIC_NUM; }
  int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; }
  bool operator==(const MacroKey &other) const { return key_ == other.key_; }
  TO_STRING_KV(K_(key));
  int64_t key_;
};

struct BadKey
{
public:
  BadKey()
    : key_(0)
  {}
  explicit BadKey(int64_t k)
    : key_(k)
  {}
  uint64_t hash() const { return 1; }
  int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; }
  bool operator==(const BadKey &other) const { return key_ == other.key_; }
  TO_STRING_KV(K_(key));
  int64_t key_;
};

struct ModKey
{
public:
  ModKey()
    : key_(0)
  {}
  explicit ModKey(int64_t k)
    : key_(k)
  {}
  uint64_t hash() const { return key_ % 7; }
  int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; }
  bool operator==(const ModKey &other) const { return key_ == other.key_; }
  TO_STRING_KV(K_(key));
  int64_t key_;
};

struct Value
{
public:
  Value()
    : value_(0)
  {}
  explicit Value(int64_t v)
    : value_(v)
  {}
  bool operator ==(const Value &other) const { return value_ == other.value_; }
  bool operator !=(const Value &other) const { return !operator==(other); }
  TO_STRING_KV(K_(value));
  int64_t value_;
};

class TestCuckooHashMap : public ::testing::Test
{
public:
  TestCuckooHashMap();
  virtual ~TestCuckooHashMap();
};

TestCuckooHashMap::TestCuckooHashMap()
{
}

TestCuckooHashMap::~TestCuckooHashMap()
{
}

TEST_F(TestCuckooHashMap, basic_test)
{
  const int64_t bucket_num= 100;
  hash::ObCuckooHashMap<Key, Value> map;
  ObMalloc allocator;
  ASSERT_EQ(OB_SUCCESS, map.create(bucket_num, &allocator));
  for (int64_t i = 0; i < 50; ++i) {
    Key key(i);
    Value value(i);
    ASSERT_EQ(OB_SUCCESS, map.set(key, value));
  }
  for (int64_t i = 0; i < 50; ++i) {
    Key key(i);
    Value value;
    ASSERT_EQ(OB_SUCCESS, map.get(key, value));
    ASSERT_EQ(value.value_, i);
  }

  // test overwrite
  for (int64_t i = 0; i < 50; ++i) {
    Key key(i);
    Value value(i + 50);
    ASSERT_EQ(OB_SUCCESS, map.set(key, value, true));
  }
  for (int64_t i = 0; i < 50; ++i) {
    Key key(i);
    Value value(i + 100);
    ASSERT_EQ(OB_HASH_EXIST, map.set(key, value, false));
  }
  for (int64_t i = 0; i < 50; ++i) {
    Key key(i);
    Value value;
    ASSERT_EQ(OB_SUCCESS, map.get(key, value));
    ASSERT_EQ(value.value_, i + 50);
  }

  for (int64_t i = 50; i < 100; ++i) {
    Key key(i);
    Value value;
    ASSERT_EQ(OB_HASH_NOT_EXIST, map.get(key, value));
  }
  for (int64_t i = 0; i < 50; ++i) {
    Key key(i);
    Value value;
    ASSERT_EQ(OB_SUCCESS, map.erase(key, &value));
    ASSERT_EQ(i + 50, value.value_);
  }
  for (int64_t i = 0; i < 50; ++i) {
    Key key(i);
    Value value;
    ASSERT_EQ(OB_HASH_NOT_EXIST, map.get(key, value));
  }
  // erase not exist item
  for (int64_t i = 0; i < 50; ++i) {
    Key key(i);
    Value value;
    ASSERT_EQ(OB_HASH_NOT_EXIST, map.erase(key, &value));
  }
}

TEST_F(TestCuckooHashMap, test_rehash)
{
  const int64_t bucket_num= 40;
  hash::ObCuckooHashMap<Key, Value> map;
  ObMalloc allocator;
  ASSERT_EQ(OB_SUCCESS, map.create(bucket_num, &allocator));
  for (int64_t i = 0; i < 50; ++i) {
    Key key(i);
    Value value(i);
    ASSERT_EQ(OB_SUCCESS, map.set(key, value));
  }
  for (int64_t i = 0; i < 50; ++i) {
    Key key(i);
    Value value;
    ASSERT_EQ(OB_SUCCESS, map.get(key, value));
    ASSERT_EQ(value.value_, i);
  }

  // test overwrite
  for (int64_t i = 0; i < 50; ++i) {
    Key key(i);
    Value value(i + 50);
    ASSERT_EQ(OB_SUCCESS, map.set(key, value, true));
  }
  for (int64_t i = 0; i < 50; ++i) {
    Key key(i);
    Value value;
    ASSERT_EQ(OB_SUCCESS, map.get(key, value));
    ASSERT_EQ(value.value_, i + 50);
  }

  for (int64_t i = 50; i < 100; ++i) {
    Key key(i);
    Value value;
    ASSERT_EQ(OB_HASH_NOT_EXIST, map.get(key, value));
  }
  for (int64_t i = 0; i < 50; ++i) {
    Key key(i);
    Value value;
    ASSERT_EQ(OB_SUCCESS, map.erase(key, &value));
    ASSERT_EQ(i + 50, value.value_);
  }
  for (int64_t i = 0; i < 50; ++i) {
    Key key(i);
    Value value;
    ASSERT_EQ(OB_HASH_NOT_EXIST, map.get(key, value));
  }
  // erase not exist item
  for (int64_t i = 0; i < 50; ++i) {
    Key key(i);
    Value value;
    ASSERT_EQ(OB_HASH_NOT_EXIST, map.erase(key, &value));
  }
}

TEST_F(TestCuckooHashMap, test_bad_hash)
{
  const int64_t bucket_num= 40;
  hash::ObCuckooHashMap<BadKey, Value> map;
  ObMalloc allocator;
  ASSERT_EQ(OB_SUCCESS, map.create(bucket_num, &allocator));
  for (int64_t i = 0; i < 50; ++i) {
    BadKey key(i);
    Value value(i);
    ASSERT_EQ(OB_SUCCESS, map.set(key, value));
  }
  for (int64_t i = 0; i < 50; ++i) {
    BadKey key(i+50);
    Value value;
    ASSERT_EQ(OB_HASH_NOT_EXIST, map.get(key, value));
  }
  for (int64_t i = 0; i < 50; ++i) {
    BadKey key(i);
    Value value;
    ASSERT_EQ(OB_SUCCESS, map.get(key, value));
    ASSERT_EQ(value.value_, i);
  }
  // test overwrite
  for (int64_t i = 0; i < 50; ++i) {
    BadKey key(i);
    Value value(i);
    ASSERT_EQ(OB_HASH_EXIST, map.set(key, value));
  }
  for (int64_t i = 0; i < 50; ++i) {
    BadKey key(i);
    Value value(i+50);
    ASSERT_EQ(OB_SUCCESS, map.set(key, value, true));
  }
  for (int64_t i = 0; i < 50; ++i) {
    BadKey key(i);
    Value value;
    ASSERT_EQ(OB_SUCCESS, map.get(key, value));
    ASSERT_EQ(i+50, value.value_);
  }
  // test erase
  for (int64_t i = 0; i < 50; ++i) {
    BadKey key(i+50);
    Value value;
    ASSERT_EQ(OB_HASH_NOT_EXIST, map.erase(key));
  }
  // test erase success
  for (int64_t i = 0; i < 50; ++i) {
    BadKey key(i);
    Value value;
    ASSERT_EQ(OB_SUCCESS, map.erase(key, &value));
    ASSERT_EQ(i+50, value.value_);
  }
  for (int64_t i = 0; i < 50; ++i) {
    BadKey key(i);
    Value value;
    ASSERT_EQ(OB_HASH_NOT_EXIST, map.get(key, value));
  }
}

TEST_F(TestCuckooHashMap, test_bad_hash_big_data)
{
  const int64_t bucket_num= 50;
  const int64_t insert_count = 10000;
  hash::ObCuckooHashMap<BadKey, Value> map;
  ObMalloc allocator(ObModIds::TEST);
  ASSERT_EQ(OB_SUCCESS, map.create(bucket_num, &allocator));
  for (int64_t i = 0; i < insert_count; ++i) {
    BadKey key(i);
    Value value(i);
    ASSERT_EQ(OB_SUCCESS, map.set(key, value));
  }
  for (int64_t i = 0; i < insert_count; ++i) {
    BadKey key(i);
    Value value(i);
    ASSERT_EQ(OB_SUCCESS, map.get(key, value));
    ASSERT_EQ(i, value.value_);
  }
  for (int64_t i = 0; i < insert_count / 2; ++i) {
    BadKey key(i);
    Value value(i + 100);
    ASSERT_EQ(OB_SUCCESS, map.set(key, value, true));
  }
  for (int64_t i = 0; i < insert_count / 2; ++i) {
    BadKey key(i);
    Value value;
    ASSERT_EQ(OB_SUCCESS, map.get(key, value));
    ASSERT_EQ(i + 100, value.value_);
  }
  for (int64_t i = 0; i < insert_count / 2; ++i) {
    BadKey key(i);
    Value value;
    ASSERT_EQ(OB_SUCCESS, map.erase(key, &value));
    ASSERT_EQ(i + 100, value.value_);
  }
  for (int64_t i = 0; i < insert_count / 2; ++i) {
    BadKey key(i);
    Value value;
    ASSERT_EQ(OB_HASH_NOT_EXIST, map.get(key, value));
  }
  for (int64_t i = insert_count / 2; i < insert_count; ++i) {
    BadKey key(i);
    Value value;
    ASSERT_EQ(OB_SUCCESS, map.get(key, value));
    ASSERT_EQ(i, value.value_);
  }
  for (int64_t i = 0; i < insert_count / 2; ++i) {
    BadKey key(i);
    Value value(i);
    ASSERT_EQ(OB_SUCCESS, map.set(key, value));
  }
  for (int64_t i = 0; i < insert_count; ++i) {
    BadKey key(i);
    Value value;
    ASSERT_EQ(OB_SUCCESS, map.get(key, value));
    ASSERT_EQ(i, value.value_);
  }
}

TEST_F(TestCuckooHashMap, test_bad_key_iterator)
{
  const int64_t bucket_num= 50;
  const int64_t insert_count = 100;
  hash::ObCuckooHashMap<BadKey, Value> map;
  ObMalloc allocator(ObModIds::TEST);
  int64_t iter_count = 0;
  ASSERT_EQ(OB_SUCCESS, map.create(bucket_num, &allocator));
  OB_LOG(INFO, "test bad key iterator");
  for (int64_t i = 0; i < insert_count; ++i) {
    BadKey key(i);
    Value value(i);
    ASSERT_EQ(OB_SUCCESS, map.set(key, value));
  }
  for (hash::ObCuckooHashMap<BadKey, Value>::iterator iter = map.begin(); iter != map.end(); ++iter) {
    Value value;
    ASSERT_EQ(OB_SUCCESS, map.get(iter->first, value));
    ASSERT_EQ(value, iter->second);
  }
  iter_count = 0;
  for (int64_t i = 0; i < insert_count / 2; ++i) {
    BadKey key(i);
    Value value;
    ASSERT_EQ(OB_SUCCESS, map.erase(key, &value));
    ASSERT_EQ(i, value.value_);
  }
  for (hash::ObCuckooHashMap<BadKey, Value>::iterator iter = map.begin(); iter != map.end(); ++iter) {
    Value value;
    ASSERT_EQ(OB_SUCCESS, map.get(iter->first, value));
    ASSERT_EQ(value, iter->second);
    ++iter_count;
    OB_LOG(INFO, "iter data", K(iter->first));
  }
  ASSERT_EQ(iter_count, insert_count / 2);
}

TEST_F(TestCuckooHashMap, test_normal_key_iterator)
{
  const int64_t bucket_num= 100;
  hash::ObCuckooHashMap<Key, Value> map;
  ObMalloc allocator;
  int64_t iter_count = 0;
  ASSERT_EQ(OB_SUCCESS, map.create(bucket_num, &allocator));
  for (int64_t i = 0; i < 50; ++i) {
    Key key(i);
    Value value(i);
    ASSERT_EQ(OB_SUCCESS, map.set(key, value));
  }
  for (int64_t i = 0; i < 50; ++i) {
    Key key(i);
    Value value;
    ASSERT_EQ(OB_SUCCESS, map.get(key, value));
    ASSERT_EQ(value.value_, i);
  }
  for (hash::ObCuckooHashMap<Key, Value>::iterator iter = map.begin(); iter != map.end(); ++iter) {
    Value value;
    ASSERT_EQ(OB_SUCCESS, map.get(iter->first, value));
    ASSERT_EQ(value, iter->second);
    ++iter_count;
  }
  ASSERT_EQ(50, iter_count);

  // test overwrite
  for (int64_t i = 0; i < 50; ++i) {
    Key key(i);
    Value value(i + 50);
    ASSERT_EQ(OB_SUCCESS, map.set(key, value, true));
  }
  for (int64_t i = 0; i < 50; ++i) {
    Key key(i);
    Value value(i + 100);
    ASSERT_EQ(OB_HASH_EXIST, map.set(key, value, false));
  }
  iter_count = 0;
  for (hash::ObCuckooHashMap<Key, Value>::iterator iter = map.begin(); iter != map.end(); ++iter) {
    Value value;
    ASSERT_EQ(OB_SUCCESS, map.get(iter->first, value));
    ASSERT_EQ(value, iter->second);
    ++iter_count;
  }
  ASSERT_EQ(50, iter_count);

  for (int64_t i = 0; i < 50; ++i) {
    Key key(i);
    Value value;
    ASSERT_EQ(OB_SUCCESS, map.erase(key, &value));
    ASSERT_EQ(i + 50, value.value_);
  }
  iter_count = 0;
  for (hash::ObCuckooHashMap<Key, Value>::iterator iter = map.begin(); iter != map.end(); ++iter) {
    Value value;
    ASSERT_EQ(OB_SUCCESS, map.get(iter->first, value));
    ASSERT_EQ(value, iter->second);
    ++iter_count;
  }
  ASSERT_EQ(0, iter_count);
}

TEST_F(TestCuckooHashMap, test_rehash_iterator)
{
  const int64_t bucket_num= 40;
  hash::ObCuckooHashMap<Key, Value> map;
  ObMalloc allocator;
  int64_t iter_count = 0;
  ASSERT_EQ(OB_SUCCESS, map.create(bucket_num, &allocator));
  for (int64_t i = 0; i < 50; ++i) {
    Key key(i);
    Value value(i);
    ASSERT_EQ(OB_SUCCESS, map.set(key, value));
  }
  iter_count = 0;
  for (hash::ObCuckooHashMap<Key, Value>::iterator iter = map.begin(); iter != map.end(); ++iter) {
    Value value;
    ASSERT_EQ(OB_SUCCESS, map.get(iter->first, value));
    ASSERT_EQ(value, iter->second);
    ++iter_count;
  }
  ASSERT_EQ(50, iter_count);

  // test overwrite
  for (int64_t i = 0; i < 50; ++i) {
    Key key(i);
    Value value(i + 50);
    ASSERT_EQ(OB_SUCCESS, map.set(key, value, true));
  }
  iter_count = 0;
  for (hash::ObCuckooHashMap<Key, Value>::iterator iter = map.begin(); iter != map.end(); ++iter) {
    Value value;
    ASSERT_EQ(OB_SUCCESS, map.get(iter->first, value));
    ASSERT_EQ(value, iter->second);
    ++iter_count;
  }
  ASSERT_EQ(50, iter_count);

  for (int64_t i = 0; i < 50; ++i) {
    Key key(i);
    Value value;
    ASSERT_EQ(OB_SUCCESS, map.erase(key, &value));
    ASSERT_EQ(i + 50, value.value_);
  }
  iter_count = 0;
  for (hash::ObCuckooHashMap<Key, Value>::iterator iter = map.begin(); iter != map.end(); ++iter) {
    Value value;
    ASSERT_EQ(OB_SUCCESS, map.get(iter->first, value));
    ASSERT_EQ(value, iter->second);
    ++iter_count;
  }
  ASSERT_EQ(0, iter_count);
}

int map_random(const int64_t row_num, hash::ObHashMap<ModKey, Value> &map, ModKey &key)
{
  int ret = OB_SUCCESS;
  int64_t iter_count = 1;
  for (hash::ObHashMap<ModKey, Value>::iterator iter = map.begin(); iter != map.end(); ++iter) {
    if (iter_count == row_num) {
      key = iter->first;
      break;
    }
    ++iter_count;
  }
  return ret;
}

int map_insert(const ModKey &mod_key, const Value &value,
    hash::ObCuckooHashMap<ModKey, Value> &map, hash::ObHashMap<ModKey, Value> &right_map)
{
  int ret = OB_SUCCESS;
  Value got_value;
  const int64_t original_size = map.size();
  const int64_t original_right_size = right_map.size();
  if (OB_FAIL(right_map.set_refactored(mod_key, value, true/*overwrite*/))) {
    OB_LOG(WARN, "fail to set to hashmap", K(ret), K(mod_key), K(value));
  } else if (OB_FAIL(map.set(mod_key, value, true/*overwrite*/))) {
    OB_LOG(WARN, "fail to set to cuckoo hashmap", K(ret), K(mod_key), K(value));
  } else if (OB_FAIL(map.get(mod_key, got_value))) {
    OB_LOG(WARN, "fail to get value", K(ret));
  } else if (value != got_value) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "error unexpected, got value is mismatch", K(value), K(got_value));
  } else if (map.size() != right_map.size()) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "error unexpected, hash map size not match", K(ret), K(original_size), K(original_right_size), K(map.size()), K(right_map.size()));
  }
  return ret;
}

int map_erase(const ModKey &mod_key,
    hash::ObCuckooHashMap<ModKey, Value> &map, hash::ObHashMap<ModKey, Value> &right_map)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(right_map.erase_refactored(mod_key))) {
    OB_LOG(WARN, "fail to set to hashmap", K(ret), K(mod_key));
  } else if (OB_FAIL(map.erase(mod_key))) {
    OB_LOG(WARN, "fail to set to cuckoo hashmap", K(ret), K(mod_key));
  } else if (map.size() != right_map.size()) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "error unexpected, hash map size not match", K(ret), K(map.size()), K(right_map.size()));
  }
  return ret;
}

int map_iterator(hash::ObCuckooHashMap<ModKey, Value> &map, hash::ObHashMap<ModKey, Value> &right_map)
{
  int ret = OB_SUCCESS;
  const int64_t map_size = map.size();
  const int64_t right_map_size = right_map.size();
  if (map_size != right_map_size) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "map size do not match", K(ret), K(map_size), K(right_map_size));
  } else {
    int64_t iter_count = 0;
    for (hash::ObCuckooHashMap<ModKey, Value>::iterator iter = map.begin(); OB_SUCC(ret) && iter != map.end(); ++iter) {
      Value value;
      if (OB_FAIL(right_map.get_refactored(iter->first, value))) {
        OB_LOG(WARN, "fail to get from right map", K(ret), K(iter->first));
      } else if (iter->second != value) {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "error unexpected, value not match", K(iter->first), K(iter->second), K(value));
      } else {
        ++iter_count;
      }
    }
    if (OB_SUCC(ret)) {
      if (iter_count != map_size) {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "size not match", K(iter_count), K(map_size));
      } else {
        OB_LOG(INFO, "succeed to check map iterator", K(map_size));
      }
    }
  }
  return ret;
}

TEST_F(TestCuckooHashMap, test_random_run)
{
  const int64_t bucket_num= 2;
  hash::ObCuckooHashMap<ModKey, Value> map;
  ObMalloc allocator;
  int64_t iter_count = 0;
  ObRandom random;
  const int64_t start_time = ObTimeUtility::current_time();
  // const int64_t RUN_TIME = 3 * 3600L * 1000 * 1000;
  const int64_t RUN_TIME = 6L * 1000 * 1000;
  int64_t op_types[4][6] = { {0, 0, 0, 1, 2, 3}, {0, 0, 0, 1, 2, 3}, {1, 1, 1, 0, 2, 3}, {1, 1, 1, 0, 2, 3}}; // 0: insert 1: delete 2: iter 3: get
  hash::ObHashMap<ModKey, Value> right_map;
  ASSERT_EQ(OB_SUCCESS, right_map.create(1000000, lib::ObLabel("hashmap")));
  ASSERT_EQ(OB_SUCCESS, map.create(bucket_num, &allocator));
  while (start_time + RUN_TIME > ObTimeUtility::current_time()) {
     const int64_t group_type = random.get(0, 3);
      ModKey key;
     for (int64_t i = 0; i < 100; ++i) {
      const int64_t op_index = random.get(0, 5);
      const int64_t op = op_types[group_type][op_index];
      if (0 == op) {
        key.key_ = random.get(0, 100000);
        Value value(key.key_);
        OB_LOG(INFO, "insert key", K(key), K(value));
        ASSERT_EQ(OB_SUCCESS, map_insert(key, value, map, right_map));
      } else if (1 == op) {
        if (right_map.size() >= 1) {
          const int64_t random_row_num = random.get(1, right_map.size());
          ASSERT_EQ(OB_SUCCESS, map_random(random_row_num, right_map, key));
          OB_LOG(INFO, "erase key", K(key));
          ASSERT_EQ(OB_SUCCESS, map_erase(key, map, right_map));
        }
      } else if (2 == op) {
        ASSERT_EQ(OB_SUCCESS, map_iterator(map, right_map));
      }
    }
  }
}

TEST_F(TestCuckooHashMap, test_rehash_bug)
{
  int64_t macro_id_array[] = {121634816,123731968,125829120,127926272,130023424,132120576,134217728,136314880,138412032,140509184,142606336,144703488,146800640,148897792,150994944,153092096,155189248,157286400,159383552,161480704,163577856,165675008,167772160,169869312,171966464,174063616,176160768,178257920,180355072,182452224,184549376,186646528,188743680,190840832,192937984,299892736,301989888,304087040,306184192,308281344,310378496,312475648,314572800,316669952,318767104,320864256,322961408,325058560,327155712,329252864,331350016,333447168,335544320,337641472,339738624,341835776,343932928,346030080,348127232,350224384,352321536,354418688,356515840,358612992,360710144,362807296,364904448,367001600,369098752,488636416,490733568,492830720,494927872,497025024,499122176,501219328,503316480,505413632,507510784,509607936,511705088,513802240,515899392,517996544,520093696,522190848,524288000,526385152,528482304,530579456,532676608,534773760,536870912,538968064,541065216,543162368,545259520,547356672,549453824,551550976,553648128,555745280,557842432,679477248,681574400,683671552,685768704,687865856,689963008,692060160,694157312,696254464,698351616,700448768,702545920,704643072,706740224,708837376,710934528,713031680,715128832,717225984,719323136,721420288,723517440,725614592,727711744,729808896,731906048,734003200,736100352,738197504,740294656,742391808,744488960,746586112,748683264,750780416,868220928,870318080,872415232,874512384,876609536,878706688,880803840,882900992,884998144,887095296,889192448,891289600,893386752,895483904,897581056,899678208,901775360,903872512,905969664,908066816,910163968,912261120,914358272,916455424,918552576,920649728,922746880,924844032,926941184,929038336,931135488,933232640,935329792,937426944,1054867456,1056964608,1059061760,1061158912,1063256064,1065353216,1067450368,1069547520,1071644672,1073741824,1075838976,1077936128,1080033280,1082130432,1084227584,1086324736,1088421888,1090519040,1092616192,1094713344,1096810496,1098907648,1101004800,1103101952,1105199104,1107296256,1109393408,1111490560,1113587712,1115684864,1117782016,1119879168,1231028224,1233125376,1235222528,1237319680,1239416832,1241513984,1243611136,1245708288,1247805440,1249902592,1251999744,1254096896,1256194048,1258291200,1260388352,1262485504,1264582656,1266679808,1268776960,1270874112,1272971264,1275068416,1277165568,1279262720,1281359872,1283457024,1285554176,1287651328,1289748480,1291845632,1293942784,1296039936,1298137088,1300234240,1302331392,1419771904,1421869056,1423966208,1426063360,1428160512,1430257664,1432354816,1434451968,1436549120,1438646272,1440743424,1442840576,1444937728,1447034880,1449132032,1451229184,1453326336,1455423488,1457520640,1459617792,1461714944,1463812096,1465909248,1468006400,1470103552,1472200704,1474297856,1476395008,1478492160,1480589312,1482686464,1484783616,1486880768,1589641216,1591738368,1593835520,1595932672,1598029824,1600126976,1602224128,1604321280,1606418432,1608515584,1610612736,1612709888,1614807040,1616904192,1619001344,1621098496,1623195648,1625292800,1627389952,1629487104,1631584256,1633681408,1635778560,1637875712,1639972864,1642070016,1644167168,1646264320,1648361472,1650458624,1652555776,1654652928,1656750080,1765801984,1767899136,1769996288,1772093440,1774190592,1776287744,1778384896,1780482048,1782579200,1784676352,1786773504,1788870656,1790967808,1793064960,1795162112,1797259264,1799356416,1801453568,1803550720,1805647872,1807745024,1809842176,1811939328,1814036480,1816133632,1818230784,1820327936,1822425088,1824522240,1826619392,1828716544,1830813696,1832910848,1944059904,1946157056,1948254208,1950351360,1952448512,1954545664,1956642816,1958739968,1960837120,1962934272,1965031424,1967128576,1969225728,1971322880,1973420032,1975517184,1977614336,1979711488,1981808640,1983905792,1986002944,1988100096,1990197248,1992294400,1994391552,1996488704,1998585856,2000683008,2002780160,2004877312,2006974464,2009071616,2011168768,2124414976,2126512128,2128609280,2130706432,2132803584,2134900736,2136997888,2139095040,2141192192,2143289344,2145386496,2147483648,2149580800,2151677952,2153775104,2155872256,2157969408,2160066560,2162163712,2164260864,2166358016,2168455168,2170552320,2172649472,2174746624,2176843776,2178940928,2181038080,2183135232,2185232384,2187329536,2189426688,2191523840,2313158656,2315255808,2317352960,2319450112,2321547264,2323644416,2325741568,2327838720,2329935872,2332033024,2334130176,2336227328,2338324480,2340421632,2342518784,2344615936,2346713088,2348810240,2350907392,2353004544,2355101696,2357198848,2359296000,2361393152,2363490304,2365587456,2367684608,2369781760,2371878912,2373976064,2376073216,2378170368,2380267520,2382364672,2384461824,2386558976,2388656128,2518679552,2520776704,2522873856,2524971008,2527068160,2529165312,2531262464,2533359616,2535456768,2537553920,2539651072,2541748224,2543845376,2545942528,2548039680,2550136832,2552233984,2554331136,2556428288,2558525440,2560622592,2562719744,2564816896,2566914048,2569011200,2571108352,2573205504,2575302656,2577399808,2579496960,2581594112,2583691264,2585788416,2587885568,2589982720,2592079872,2594177024,2596274176,2717908992,2720006144,2722103296,2724200448,2726297600,2728394752,2730491904,2732589056,2734686208,2736783360,2738880512,2740977664,2743074816,2745171968,2747269120,2749366272,2751463424,2753560576,2755657728,2757754880,2759852032,2761949184,2764046336,2766143488,2768240640,2770337792,2772434944,2774532096,2776629248,2778726400,2780823552,2782920704,2785017856,2787115008,2789212160,2791309312,2910846976,2912944128,2915041280,2917138432,2919235584,2921332736,2923429888,2925527040,2927624192,2929721344,2931818496,2933915648,2936012800,2938109952,2940207104,2942304256,2944401408,2946498560,2948595712,2950692864,2952790016,2954887168,2956984320,2959081472,2961178624,2963275776,2965372928,2967470080,2969567232,2971664384,2973761536,2975858688,2977955840,2980052992,2982150144,2984247296,2986344448,104987623424,104989720576,104991817728,104993914880,104996012032,104998109184,105000206336,105002303488,105004400640,105006497792,105008594944,105010692096,105012789248,105014886400,105016983552,105019080704,105021177856,105023275008,105025372160,105027469312,105029566464,105031663616,105033760768,105035857920,105037955072,105040052224,105042149376,105044246528,105046343680,105048440832,105050537984,105052635136,105054732288,105056829440,105058926592,105061023744,105063120896,105065218048,105067315200};
  hash::ObCuckooHashMap<MacroKey, int64_t> map;
  ObArenaAllocator allocator;
  const int64_t array_size = sizeof(macro_id_array) / sizeof(macro_id_array[0]);
  ASSERT_EQ(OB_SUCCESS, map.create(array_size, &allocator));
  for (int64_t i = 0; i< array_size; ++i) {
    int64_t value = 0;
    ASSERT_EQ(OB_SUCCESS, map.set(MacroKey(macro_id_array[i]), i));
    ASSERT_EQ(OB_SUCCESS, map.get(MacroKey(macro_id_array[i]), value));
  }
  ASSERT_EQ(array_size, map.size());
}

TEST_F(TestCuckooHashMap, test_clear_and_iterate)
{
  const int64_t bucket_num= 2;
  hash::ObCuckooHashMap<int, int> map;
  hash::ObCuckooHashMap<int, int>::iterator iter;
  ObArenaAllocator allocator;
  ASSERT_EQ(OB_SUCCESS, map.create(bucket_num, &allocator));
  ASSERT_EQ(OB_SUCCESS, map.set(0, 0));
  iter = map.begin();
  ASSERT_TRUE(iter != map.end());
  map.clear();
  iter = map.begin();
  ASSERT_TRUE(iter == map.end());
}

int main(int argc, char **argv)
{
  system("rm -f test_cuckoo_hash_map.log*");
  OB_LOGGER.set_log_level("DEBUG");
  OB_LOGGER.set_file_name("test_cuckoo_hash_map.log", true);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
