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

#include <thread>
#include "lib/hash/ob_link_hashmap.h"
#include "lib/hash/ob_hashutils.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/utility/ob_print_utils.h"

#include "gtest/gtest.h"

using namespace oceanbase;
using namespace common;
using namespace hash;

class HashKey
{
public:
  HashKey(): v_(0) {}
  HashKey(uint64_t v): v_(v) {}
  HashKey(const HashKey& that): v_(that.v_) {}
  int compare(const HashKey& that) {
    int ret = 0;
    if (v_ > that.v_) {
      ret = 1;
    } else if (v_ < that.v_) {
      ret = -1;
    }
    return ret;
  }
  uint64_t hash() const { return v_; }
  uint64_t v_;
};

class HashValue : public LinkHashValue<HashKey>
{
public:
  HashValue(): v_(0) {}
  HashValue(uint64_t v): v_(v) {}
  HashValue(const HashValue& that): v_(that.v_) {}
  uint64_t v_;
};

static uint64_t value_alloc CACHE_ALIGNED;
static uint64_t value_free CACHE_ALIGNED;
static uint64_t node_alloc CACHE_ALIGNED;
static uint64_t node_free CACHE_ALIGNED;

ObMemAttr attr(1, ObNewModIds::OB_MEMSTORE);

static int64_t STEP = 0;

class TestAllocHandle
{
  typedef LinkHashNode<HashKey> Node;
public:
  TestAllocHandle() : is_inited_(true) {}
  ~TestAllocHandle() { ATOMIC_STORE(&is_inited_, false); }
  HashValue* alloc_value()
  {
    abort_unless(ATOMIC_LOAD(&is_inited_) == true);
    ATOMIC_INC(&value_alloc);
    HashValue* value = (HashValue*)ob_malloc(sizeof(HashValue), attr);
    new(value) HashValue();
    return value;
  }
  void free_value(HashValue* val)
  {
    abort_unless(ATOMIC_LOAD(&is_inited_) == true);
    ATOMIC_INC(&value_free);
    val->~HashValue();
    ob_free(val);
  }
  Node* alloc_node(HashValue* val)
  {
    abort_unless(ATOMIC_LOAD(&is_inited_) == true);
    UNUSED(val);
    ATOMIC_INC(&node_alloc);
    Node* node = (Node*)ob_malloc(sizeof(Node), attr);
    new(node) Node();
    return node;
  }
  void free_node(Node* node)
  {
    if (ATOMIC_LOAD(&STEP) == 1) {
      IGNORE_RETURN ATOMIC_BCAS(&STEP, 1, 2);
      usleep(1 * 1000 * 1000);
    }
    abort_unless(ATOMIC_LOAD(&is_inited_) == true);
    ATOMIC_INC(&node_free);
    node->~Node();
    ob_free(node);
  }
private:
  bool is_inited_;
};

class AtomicGetFunctor
{
public:
  explicit AtomicGetFunctor() : ret_val_(0) {}
  void operator()(const HashKey &key, HashValue *value)
  {
    UNUSED(key);
    ret_val_ = value->v_;
  }
  uint64_t ret_val_;
};

static bool print(HashKey &key, HashValue *value) { UNUSED(key); _OB_LOG(INFO, "key=%lu val=%lu", key.v_, value->v_); return true; }

typedef ObLinkHashMap<HashKey, HashValue, TestAllocHandle> Hashmap;

TEST(TestObHashMap, Feature)
{
  Hashmap hm;
  HashKey key;
  HashValue *val_ptr = nullptr;

  EXPECT_EQ(OB_SUCCESS, hm.init());

  // insert
  key.v_ = 1;
  EXPECT_EQ(OB_SUCCESS, hm.create(key, val_ptr));
  hm.revert(val_ptr);
  // 1:0

  // insert key of different bucket
  key.v_ = 2;
  EXPECT_EQ(OB_SUCCESS, hm.create(key, val_ptr));
  hm.revert(val_ptr);
  // 1:0, 2:0

  // insert key of common bucket
  key.v_ = 1;
  EXPECT_EQ(OB_ENTRY_EXIST, hm.create(key, val_ptr));
  // 1:0, 2:0

  // insert
  key.v_ = 3;
  EXPECT_EQ(OB_SUCCESS, hm.alloc_value(val_ptr));
  val_ptr->v_ = 1;
  EXPECT_EQ(OB_SUCCESS, hm.insert_and_get(key, val_ptr));
  hm.revert(val_ptr);
  // 1:0, 2:0, 3:1

  // insert key of different bucket
  key.v_ = 4;
  EXPECT_EQ(OB_SUCCESS, hm.alloc_value(val_ptr));
  val_ptr->v_ = 1;
  EXPECT_EQ(OB_SUCCESS, hm.insert_and_get(key, val_ptr));
  hm.revert(val_ptr);
  // 1:0, 2:0, 3:1, 4:1

  // insert key of common bucket
  key.v_ = 3;
  EXPECT_EQ(OB_SUCCESS, hm.alloc_value(val_ptr));
  val_ptr->v_ = 2;
  EXPECT_EQ(OB_ENTRY_EXIST, hm.insert_and_get(key, val_ptr));
  hm.free_value(val_ptr);
  // 1:0, 2:0, 3:1, 4:1

  // delete existing key
  key.v_ = 1;
  EXPECT_EQ(OB_SUCCESS, hm.del(key));
  // 2:0, 3:1, 4:1

  // delete not existing key
  key.v_ = 5;
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, hm.del(key));
  // 2:0, 3:1, 4:1

  // query existing key
  key.v_ = 2;
  val_ptr = nullptr;
  EXPECT_EQ(OB_SUCCESS, hm.get(key, val_ptr));
  val_ptr->v_ +=1 ;
  hm.revert(val_ptr);
  // 2:1, 3:1, 4:1

  // query not existing key
  key.v_ = 6;
  val_ptr = nullptr;
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, hm.get(key, val_ptr));
  // 2:1, 3:1, 4:1

  // query existing key
  key.v_ = 3;
  EXPECT_EQ(OB_ENTRY_EXIST, hm.contains_key(key));
  // 2:1, 3:1, 4:1

  // query not existing key
  key.v_ = 7;
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, hm.contains_key(key));
  // 2:1, 3:1, 4:1

  EXPECT_EQ(OB_SUCCESS, hm.for_each(print));
  // 2:1, 3:1, 4:1

  AtomicGetFunctor fn;
  key.v_ = 2;
  EXPECT_EQ(OB_SUCCESS, hm.operate(key, fn));
  EXPECT_EQ(1, fn.ret_val_);

  hm.reset();
  hm.purge();

  EXPECT_EQ(0, hm.count());
  EXPECT_EQ(0, hm.size());
  EXPECT_EQ(value_free, value_alloc);
  EXPECT_EQ(node_free, node_alloc);
}

TEST(TestObHashMap, Stress)
{
  constexpr int64_t THREAD_COUNT = 8;
  constexpr int64_t DATA_COUNT_PER_THREAD = 2048;

  Hashmap hm(1024);
  // 0 for not inserted, 1 for inserted, 2 for inserted but deleted soon
  int db[DATA_COUNT_PER_THREAD * THREAD_COUNT] CACHE_ALIGNED;

  memset(db, 0, sizeof(db));
  EXPECT_EQ(OB_SUCCESS, hm.init());

  // insert continuous DATA_COUNT_PER_THREAD nodes in each thread
  std::thread insert_threads[THREAD_COUNT];
  for (auto i = 0; i < THREAD_COUNT; ++i) {
    insert_threads[i] = std::thread([&, i]() {
      HashKey key;
      HashValue *val_ptr = nullptr;
      for (auto j = 0; j < DATA_COUNT_PER_THREAD; ++j) {
        key.v_ = j + i * DATA_COUNT_PER_THREAD;
        EXPECT_EQ(OB_SUCCESS, hm.create(key, val_ptr));
        ATOMIC_INC(db + key.v_);
        hm.revert(val_ptr);
        val_ptr = nullptr;
      }
    });
  }
  // delete incontinuous DATA_COUNT_PER_THREAD nodes, half of them are duplicated
  std::thread del_threads[THREAD_COUNT];
  for (auto i = 0; i < THREAD_COUNT; ++i) {
    del_threads[i] = std::thread([&, i]() {
      HashKey key;
      for (auto j = 0; j < DATA_COUNT_PER_THREAD; ++j) {
        int ret = OB_SUCCESS;
        key.v_ = i / 2 + j * THREAD_COUNT;
        while (OB_FAIL(hm.del(key)) && ATOMIC_LOAD(db + key.v_) < 2)
          ;
        if (OB_SUCC(ret)) {
          ATOMIC_INC(db + key.v_);
        } else {
          EXPECT_EQ(OB_ENTRY_NOT_EXIST, ret);
        }
      }
    });
  }

  std::thread get_threads[THREAD_COUNT];
  for (auto i = 0; i < THREAD_COUNT / 2; ++i) {
    get_threads[i] = std::thread([&, i]() {
      int ret = OB_SUCCESS;
      HashKey key;
      HashValue *val_ptr = nullptr;
      for (auto j = 0; j < DATA_COUNT_PER_THREAD; ++j) {
        key.v_ = j + i * DATA_COUNT_PER_THREAD;
        if (OB_SUCC(hm.get(key, val_ptr))) {
          hm.revert(val_ptr);
        }
        val_ptr = nullptr;
      }
    });
  }
  for (auto i = THREAD_COUNT / 2; i < THREAD_COUNT; ++i) {
    get_threads[i] = std::thread([&, i]() {
      HashKey key;
      for (auto j = 0; j < DATA_COUNT_PER_THREAD; ++j) {
        key.v_ = j + i * DATA_COUNT_PER_THREAD;
        UNUSED(hm.contains_key(key));
      }
    });
  }

  for (auto i = 0; i < THREAD_COUNT; ++i) {
    insert_threads[i].join();
  }

  for (auto i = 0; i < THREAD_COUNT; ++i) {
    del_threads[i].join();
  }

  for (auto i = 0; i < THREAD_COUNT; ++i) {
    get_threads[i].join();
  }

  EXPECT_EQ(DATA_COUNT_PER_THREAD * THREAD_COUNT / 2, hm.size());
  int counter[3];
  memset(counter, 0, sizeof(counter));
  for (int i = 0; i < DATA_COUNT_PER_THREAD * THREAD_COUNT; ++i) {
    ASSERT_TRUE(db[i] > 0);
    ASSERT_TRUE(db[i] <= 2);
    ++counter[db[i]];
  }
  EXPECT_EQ(0, counter[0]);
  EXPECT_EQ(DATA_COUNT_PER_THREAD * THREAD_COUNT / 2, counter[1]);
  EXPECT_EQ(DATA_COUNT_PER_THREAD * THREAD_COUNT / 2, counter[2]);

  hm.reset();
  hm.purge();

  EXPECT_EQ(0, hm.count());
  EXPECT_EQ(0, hm.size());
  EXPECT_EQ(value_free, value_alloc);
  EXPECT_EQ(node_free, node_alloc);
}

TEST(TestObHashMap, Retire)
{
  std::thread* t;
  {
    Hashmap A;
    HashKey key;
    HashValue *val_ptr = nullptr;
    EXPECT_EQ(OB_SUCCESS, A.init());
    key.v_ = 1;
    EXPECT_EQ(OB_SUCCESS, A.create(key, val_ptr));
    A.revert(val_ptr);
    EXPECT_EQ(OB_SUCCESS, A.del(key));
    ATOMIC_INC(&STEP);
    t = new std::thread([&]() {
      usleep(10 * 1000);
      Hashmap B;
      HashKey key;
      EXPECT_EQ(OB_SUCCESS, B.init());
      key.v_ = 1;
      EXPECT_EQ(OB_SUCCESS, B.create(key, val_ptr));
      B.revert(val_ptr);
    });
    while(ATOMIC_LOAD(&STEP) != 2);
    // ~A()
  }
  t->join();
  delete t;
}

/*
TEST(TestObHashMap, Dummy)
{
  struct Fn {
    bool operator()(HashKey& key, HashValue* value) {
      return true;
    }
  };
  Fn fn;
  bool stop = false;
  constexpr int64_t THREAD_COUNT = 8;
  constexpr int64_t DATA_COUNT_PER_THREAD = 81920;
  Hashmap hm(2);
  EXPECT_EQ(OB_SUCCESS, hm.init());
  std::thread insert_threads[THREAD_COUNT];
  int64_t current = 0;
  for (auto i = 0; i < THREAD_COUNT; ++i) {
    insert_threads[i] = std::thread([&]() {
      HashKey key;
      HashValue *val_ptr = nullptr;
      while (ATOMIC_LOAD(&current) < THREAD_COUNT * DATA_COUNT_PER_THREAD) {
        key.v_ = ATOMIC_FAA(&current, 1);
        EXPECT_EQ(OB_SUCCESS, hm.create(key, val_ptr));
        hm.revert(val_ptr);
        val_ptr = nullptr;
      }
    });
  }
  std::thread foreach_threads[THREAD_COUNT];
  for (auto i = 0; i < THREAD_COUNT; ++i) {
    foreach_threads[i] = std::thread([&]() {
      while (!stop) {
        hm.for_each(fn);
      }
    });
  }
  for (auto i = 0; i < THREAD_COUNT; ++i) {
    insert_threads[i].join();
  }
  hm.reset();
  hm.purge();
  ATOMIC_STORE(&stop, true);
  for (auto i = 0; i < THREAD_COUNT; ++i) {
    foreach_threads[i].join();
  };
}
*/

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc,argv);
  oceanbase::common::ObLogger::get_logger().set_file_name("test_link_hashmap.log", true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  return RUN_ALL_TESTS();
}
