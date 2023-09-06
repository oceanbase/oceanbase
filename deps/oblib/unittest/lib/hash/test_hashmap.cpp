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

#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>
#include <unistd.h>
#include "lib/hash/ob_hashmap.h"
#include "lib/hash/ob_hashutils.h"
#include "lib/allocator/ob_malloc.h"

#include "gtest/gtest.h"

using namespace oceanbase;
using namespace common;
using namespace hash;

uint32_t gHashItemNum = 128;
typedef uint64_t HashKey;
typedef uint64_t HashValue;

class CallBack
{
  public:
    void operator () (HashMapPair<HashKey, HashValue> &v)
    {
      v.second = v_;
    };
    void set_v(HashValue v)
    {
      v_ = v;
    };
  private:
    HashValue v_;
};

class Predicate
{
  public:
    bool operator () (HashMapPair<HashKey, HashValue> &v)
    {
      return v.second >= min_value_;
    };
    void set_min_value(HashValue v)
    {
      min_value_ = v;
    };
  private:
    HashValue min_value_;
};


TEST(TestObHashMap, create)
{
  ObHashMap<HashKey, HashValue> hm;
  // invalid parameters
  EXPECT_EQ(OB_INVALID_ARGUMENT, hm.create(0, ObModIds::OB_HASH_BUCKET));
  // normal create
  EXPECT_EQ(0, hm.create(cal_next_prime(gHashItemNum), ObModIds::OB_HASH_BUCKET));
  // duplicated create
  EXPECT_EQ(OB_INIT_TWICE, hm.create(cal_next_prime(gHashItemNum), ObModIds::OB_HASH_BUCKET));
}

TEST(TestObHashMap, set)
{
  ObHashMap<HashKey, HashValue> hm;
  uint64_t key[4] = {1, 2, 1, 1 + static_cast<uint64_t> (cal_next_prime(gHashItemNum))};
  uint64_t value[4] = {100, 200, 300, 301};

  // no create
  EXPECT_EQ(OB_NOT_INIT, hm.set_refactored(key[0], value[0], 0));

  hm.create(cal_next_prime(gHashItemNum), ObModIds::OB_HASH_BUCKET);
  // normal insert 
  EXPECT_EQ(OB_SUCCESS, hm.set_refactored(key[0], value[0], 0));
  // insert different bucket keys
  EXPECT_EQ(OB_SUCCESS, hm.set_refactored(key[1], value[1], 0));
  // insert common bucket keys
  EXPECT_EQ(OB_SUCCESS, hm.set_refactored(key[3], value[3], 0));
  // key exists but doesnt cover
  EXPECT_EQ(OB_HASH_EXIST, hm.set_refactored(key[2], value[2], 0));
  EXPECT_EQ(3, hm.size());
  // key exists and covers
  EXPECT_EQ(OB_SUCCESS, hm.set_refactored(key[2], value[2], 1));
  EXPECT_EQ(3, hm.size());
}

TEST(TestObHashMap, get)
{
  ObHashMap<HashKey, HashValue> hm;
  uint64_t key[4] = {1, 2, 1, 1 + static_cast<uint64_t> (cal_next_prime(gHashItemNum))};
  uint64_t value[4] = {100, 200, 300, 301};
  HashValue value_tmp;

  // no create
  EXPECT_EQ(OB_NOT_INIT, hm.get_refactored(key[0], value_tmp));

  hm.create(cal_next_prime(gHashItemNum), ObModIds::OB_HASH_BUCKET);
  // query existing data
  hm.set_refactored(key[0], value[0], 0);
  hm.set_refactored(key[1], value[1], 0);
  hm.set_refactored(key[3], value[3], 0);
  EXPECT_EQ(OB_SUCCESS, hm.get_refactored(key[0], value_tmp));
  EXPECT_EQ(value[0], value_tmp);
  EXPECT_EQ(OB_SUCCESS, hm.get_refactored(key[1], value_tmp));
  EXPECT_EQ(value[1], value_tmp);
  EXPECT_EQ(OB_SUCCESS, hm.get_refactored(key[3], value_tmp));
  EXPECT_EQ(value[3], value_tmp);
  // query updated data
  hm.set_refactored(key[0], value[2], 1);
  EXPECT_EQ(OB_SUCCESS, hm.get_refactored(key[0], value_tmp));
  EXPECT_EQ(value[2], value_tmp);
  // query not existing data
  EXPECT_EQ(OB_HASH_NOT_EXIST, hm.get_refactored(-1, value_tmp));
}

TEST(TestObHashMap, erase)
{
  ObHashMap<HashKey, HashValue> hm;
  uint64_t key[4] = {1, 2, 1, 1 + static_cast<uint64_t> (cal_next_prime(gHashItemNum))};
  uint64_t value[4] = {100, 200, 300, 301};

  // no create
  EXPECT_EQ(OB_NOT_INIT, hm.erase_refactored(key[0]));

  hm.create(cal_next_prime(gHashItemNum), ObModIds::OB_HASH_BUCKET);
  // delete existing data
  hm.set_refactored(key[0], value[0], 0);
  hm.set_refactored(key[1], value[1], 0);
  hm.set_refactored(key[3], value[3], 0);
  EXPECT_EQ(OB_SUCCESS, hm.erase_refactored(key[0]));
  EXPECT_EQ(OB_SUCCESS, hm.erase_refactored(key[1]));
  uint64_t value_ret = 0;
  EXPECT_EQ(OB_SUCCESS, hm.erase_refactored(key[3], &value_ret));
  EXPECT_EQ(value[3], value_ret);
  EXPECT_EQ(0, hm.size());
  // delete not existing data
  EXPECT_EQ(OB_HASH_NOT_EXIST, hm.erase_refactored(-1));
}

TEST(TestObHashMap, clear)
{
  ObHashMap<HashKey, HashValue> hm;
  uint64_t key[4] = {1, 2, 1, 1 + static_cast<uint64_t> (cal_next_prime(gHashItemNum))};
  uint64_t value[4] = {100, 200, 300, 301};

  // no create
  EXPECT_EQ(OB_NOT_INIT, hm.clear());
  hm.create(cal_next_prime(gHashItemNum), ObModIds::OB_HASH_BUCKET);
  EXPECT_EQ(0, hm.clear());
  hm.set_refactored(key[0], value[0], 0);
  hm.set_refactored(key[1], value[1], 0);
  hm.set_refactored(key[3], value[3], 0);
  EXPECT_EQ(3, hm.size());
  EXPECT_EQ(0, hm.clear());
  EXPECT_EQ(0, hm.size());
}

TEST(TestObHashMap, destroy)
{
  ObHashMap<HashKey, HashValue> hm;

  // no create
  EXPECT_EQ(0, hm.destroy());
  hm.create(cal_next_prime(gHashItemNum), ObModIds::OB_HASH_BUCKET);
  EXPECT_EQ(0, hm.destroy());
  EXPECT_EQ(0, hm.create(gHashItemNum, ObModIds::OB_HASH_BUCKET));
}

TEST(TestObHashMap, iterator)
{
  ObHashMap<HashKey, HashValue> hm;
  const ObHashMap<HashKey, HashValue>& chm = hm;
  ObHashMap<HashKey, HashValue>::iterator iter;
  ObHashMap<HashKey, HashValue>::const_iterator citer;

  // no create
  EXPECT_EQ(true, hm.begin() == hm.end());
  iter = hm.begin();
  citer = chm.begin();
  EXPECT_EQ(true, iter == hm.end());
  EXPECT_EQ(true, citer == chm.end());
  EXPECT_EQ(true, (++iter) == hm.end());
  EXPECT_EQ(true, (++citer) == chm.end());

  // no data
  hm.create(cal_next_prime(gHashItemNum), ObModIds::OB_HASH_BUCKET);
  EXPECT_EQ(true, hm.begin() == hm.end());
  iter = hm.begin();
  citer = hm.begin();
  EXPECT_EQ(true, iter == hm.end());
  EXPECT_EQ(true, citer == hm.end());
  EXPECT_EQ(true, (++iter) == hm.end());
  EXPECT_EQ(true, (++citer) == hm.end());

  uint64_t key[4] = {1, 2, 5, 5 + static_cast<uint64_t> (cal_next_prime(gHashItemNum))};
  uint64_t value[4] = {100, 200, 500, 501};
  for (int32_t i = 3; i >= 0; i--)
  {
    hm.set_refactored(key[i], value[i], 0);
  }
  iter = hm.begin();
  citer = chm.begin();
  for (uint32_t i = 0; iter != hm.end(); iter++, i++)
  {
    EXPECT_EQ(value[i], iter->second);
  }
  for (uint32_t i = 0; citer != chm.end(); citer++, i++)
  {
    EXPECT_EQ(value[i], citer->second);
  }
}

TEST(TestObHashMap, serialization)
{
  ObHashMap<HashKey, HashValue> hm;
  SimpleArchive arw, arr;
  arw.init("./hash.data", SimpleArchive::FILE_OPEN_WFLAG);
  arr.init("./hash.data", SimpleArchive::FILE_OPEN_RFLAG);
  SimpleArchive arw_nil, arr_nil;

  // no create
  EXPECT_EQ(OB_NOT_INIT, hm.serialization(arw));
  hm.create(cal_next_prime(gHashItemNum), ObModIds::OB_HASH_BUCKET);
  // no data
  EXPECT_EQ(0, hm.serialization(arw));
  EXPECT_EQ(0, hm.deserialization(arr));

  uint64_t key[4] = {1, 2, 1, 1 + static_cast<uint64_t> (cal_next_prime(gHashItemNum))};
  uint64_t value[4] = {100, 200, 300, 301};
  for (uint32_t i = 0; i < 4; i++)
  {
    hm.set_refactored(key[i], value[i], 0);
  }
  EXPECT_NE(OB_SUCCESS, hm.serialization(arw_nil));

  arw.destroy();
  arr.destroy();
  arw.init("./hash.data", SimpleArchive::FILE_OPEN_WFLAG);
  arr.init("./hash.data", SimpleArchive::FILE_OPEN_RFLAG);
  EXPECT_EQ(0, hm.serialization(arw));
  hm.destroy();
  EXPECT_EQ(0, hm.deserialization(arr));
  EXPECT_NE(OB_SUCCESS, hm.deserialization(arr));
  EXPECT_EQ(3, hm.size());

  arr_nil.init("./hash.data.nil", SimpleArchive::FILE_OPEN_RFLAG);
  EXPECT_NE(OB_SUCCESS, hm.deserialization(arr_nil));

  remove("./hash.data");
  remove("./hash.data.nil");
}

TEST(TestObHashMap, atomic)
{
  ObHashMap<HashKey, HashValue> hm;
  uint64_t key = 1;
  uint64_t value = 100;
  uint64_t value_update = 3000;
  CallBack callback;
  callback.set_v(value_update);
  HashValue value_tmp;

  //no create
  EXPECT_EQ(OB_NOT_INIT, hm.atomic_refactored(key, callback));

  hm.create(cal_next_prime(gHashItemNum), ObModIds::OB_HASH_BUCKET);

  hm.set_refactored(key, value, 0);
  EXPECT_EQ(OB_SUCCESS, hm.get_refactored(key, value_tmp));
  EXPECT_EQ(value, value_tmp);

  EXPECT_EQ(OB_SUCCESS, hm.atomic_refactored(key, callback));
  EXPECT_EQ(OB_SUCCESS, hm.get_refactored(key, value_tmp));
  EXPECT_EQ(value_update, value_tmp);

  EXPECT_EQ(OB_HASH_NOT_EXIST, hm.atomic_refactored(key + 1, callback));
  EXPECT_EQ(OB_SUCCESS, hm.get_refactored(key, value_tmp));
  EXPECT_EQ(value_update, value_tmp);
}

TEST(TestObHashMap, set_or_update)
{
  ObHashMap<HashKey, HashValue> hm;
  uint64_t key = 1;
  uint64_t value = 100;
  CallBack callback;
  HashValue value_tmp;

  // 没有create
  EXPECT_EQ(OB_NOT_INIT, hm.set_or_update(key, value, callback));
  hm.create(cal_next_prime(gHashItemNum), ObModIds::OB_HASH_BUCKET);

  callback.set_v(value);
  EXPECT_EQ(OB_HASH_NOT_EXIST, hm.get_refactored(key, value_tmp));
  EXPECT_EQ(OB_SUCCESS, hm.set_or_update(key, value, callback));
  EXPECT_EQ(OB_SUCCESS, hm.get_refactored(key, value_tmp));
  EXPECT_EQ(value, value_tmp);

  uint64_t value_update = 3000;
  callback.set_v(value_update);
  EXPECT_EQ(OB_SUCCESS, hm.set_or_update(key, value, callback));
  EXPECT_EQ(OB_SUCCESS, hm.get_refactored(key, value_tmp));
  EXPECT_EQ(value_update, value_tmp);
}

TEST(TestObHashMap, erase_if)
{
  ObHashMap<HashKey, HashValue> hm;
  uint64_t key = 1;
  uint64_t value = 100;
  Predicate pred;
  HashValue value_tmp;
  bool is_erased = true;

  // 没有create
  EXPECT_EQ(OB_NOT_INIT, hm.erase_if(key, pred, is_erased));
  hm.create(cal_next_prime(gHashItemNum), ObModIds::OB_HASH_BUCKET);

  pred.set_min_value(value + 1);
  EXPECT_EQ(OB_HASH_NOT_EXIST, hm.get_refactored(key, value_tmp));
  EXPECT_EQ(OB_SUCCESS, hm.set_refactored(key, value));
  EXPECT_EQ(OB_SUCCESS, hm.erase_if(key, pred, is_erased, &value_tmp));
  EXPECT_EQ(false, is_erased);
  EXPECT_EQ(OB_SUCCESS, hm.get_refactored(key, value_tmp));
  EXPECT_EQ(value, value_tmp);

  pred.set_min_value(value);
  value_tmp = 0;
  EXPECT_EQ(OB_SUCCESS, hm.erase_if(key, pred, is_erased, &value_tmp));
  EXPECT_EQ(true, is_erased);
  EXPECT_EQ(value, value_tmp);
  EXPECT_EQ(OB_HASH_NOT_EXIST, hm.get_refactored(key, value_tmp));
}

struct GAllocator
{
  void *alloc(const int64_t sz)
  {
    fprintf(stdout, "::malloc\n");
    return ::malloc(sz);
  }
  void free(void *p)
  {
    fprintf(stdout, "::free\n");
    ::free(p);
  }
  void clear() {};
  void set_attr(const ObMemAttr &attr) {UNUSED(attr);};
  void set_label(const lib::ObLabel &label) {UNUSED(label);};
};

template <class T>
class GAllocBigArray : public BigArrayTemp<T, GAllocator>
{
};

TEST(TestObHashMap, use_gallocator)
{
  ObHashMap<HashKey,
            HashValue,
            ReadWriteDefendMode,
            hash_func<HashKey>,
            equal_to<HashKey>,
            SimpleAllocer<HashMapTypes<HashKey, HashValue>::AllocType, 1024, SpinMutexDefendMode, GAllocator>,
            GAllocBigArray> hm;

  uint64_t key[4] = {1, 2, 1, 1 + static_cast<uint64_t> (cal_next_prime(gHashItemNum))};
  uint64_t value[4] = {100, 200, 300, 301};

  // no create
  EXPECT_EQ(OB_NOT_INIT, hm.set_refactored(key[0], value[0], 0));

  hm.create(cal_next_prime(gHashItemNum), ObModIds::OB_HASH_BUCKET);
  // normal insert
  EXPECT_EQ(OB_SUCCESS, hm.set_refactored(key[0], value[0], 0));
  // insert different bucket keys
  EXPECT_EQ(OB_SUCCESS, hm.set_refactored(key[1], value[1], 0));
  // insert common bucket keys
  EXPECT_EQ(OB_SUCCESS, hm.set_refactored(key[3], value[3], 0));
  // key exists but does not cover
  EXPECT_EQ(OB_HASH_EXIST, hm.set_refactored(key[2], value[2], 0));
  // key exists and covers
  EXPECT_EQ(OB_SUCCESS, hm.set_refactored(key[2], value[2], 1));
}

TEST(TestObHashMap, buckect_iterator)
{
  ObHashMap<HashKey, HashValue> hm;
  ObHashMap<HashKey, HashValue> hm2;

  hm.create(64, ObModIds::OB_HASH_BUCKET);
  hm2.create(64, ObModIds::OB_HASH_BUCKET);
  int i = 1024;
  while (i--) {
    EXPECT_EQ(OB_SUCCESS, hm.set_refactored(i, i, 0));
    EXPECT_EQ(OB_SUCCESS, hm2.set_refactored(i, i, 0));
  }
  EXPECT_EQ(hm.size(), hm2.size());
  EXPECT_NE(0, hm2.size());
  using hashtable = std::remove_reference<decltype(hm)>::type::hashtable;
  auto bucket_it = hm.bucket_begin();
  while (bucket_it != hm.bucket_end()) {
    hashtable::bucket_lock_cond blc(*bucket_it);
    hashtable::readlocker locker(blc.lock());
    hashtable::hashbucket::const_iterator node_it = bucket_it->node_begin();
    while (node_it != bucket_it->node_end()) {
      HashValue value_tmp;
      EXPECT_EQ(OB_SUCCESS, hm2.erase_refactored(node_it->first, &value_tmp));
      EXPECT_EQ(value_tmp, node_it->second);
      node_it++;
    }
    bucket_it++;
  }
  EXPECT_EQ(0, hm2.size());
}

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
