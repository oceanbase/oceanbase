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

#include "storage/memtable/mvcc/ob_keybtree.h"

#include "common/object/ob_object.h"
#include "common/rowkey/ob_store_rowkey.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/random/ob_random.h"
#include "storage/memtable/ob_memtable_key.h"
#include "storage/memtable/mvcc/ob_mvcc_row.h"

#include <gtest/gtest.h>
#include <thread>

namespace oceanbase
{
namespace unittest
{
using namespace oceanbase::common;
using namespace oceanbase::keybtree;
using namespace oceanbase::memtable;

//#define IS_EQ(x, y) ASSERT_EQ(x, y)
//#define IS_EQ(x, y) EXPECT_EQ(x, y)
#define DUMP_BTREE \
{ \
  FILE *file = fopen("dump_btree.txt", "w+"); \
  btree.dump(file); \
  fclose(file); \
}

#define IS_EQ(x, y) if ((x) != (y)) { abort(); }

#define judge(key, val) \
{ \
  if ((int64_t)(val) >> 3 != get_v(key)) { \
    abort(); \
  } \
}

#define MAX_CPU_NUM 8
#define CPU_NUM 8

cpu_set_t get_cpu_set(pthread_t thread)
{
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  int start_id = ObRandom::rand(0, MAX_CPU_NUM - 1);
  int cpu_id = start_id + 1;
  int count = 0;
  while (count < CPU_NUM && cpu_id != start_id) {
    CPU_SET(cpu_id, &cpuset);
    if (0 == pthread_setaffinity_np(thread, sizeof(cpu_set_t), &cpuset)) {
      ++count;
    } else {
      CPU_CLR(cpu_id, &cpuset);
    }
    cpu_id = (cpu_id + 1) % MAX_CPU_NUM;
  }
  return cpuset;
}

int BIND_CPU(pthread_t thread)
{
  static cpu_set_t cpuset = get_cpu_set(thread);
  return pthread_setaffinity_np(thread, sizeof(cpu_set_t), &cpuset);
}

const char *attr = ObModIds::TEST;

void init_key(BtreeKey *ptr, int64_t key)
{
  ptr->get_rowkey()->get_rowkey().get_obj_ptr()[0].set_int(key);
}

int alloc_key(BtreeKey *&ret_key, int64_t key)
{
  int ret = OB_SUCCESS;
  ObObj *obj_ptr = nullptr;
  ObStoreRowkey *storerowkey = nullptr;
  if (OB_ISNULL(obj_ptr = (ObObj *)ob_malloc(sizeof(ObObj), attr)) || OB_ISNULL(new(obj_ptr)ObObj(key))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_ISNULL(storerowkey = (ObStoreRowkey *)ob_malloc(sizeof(ObStoreRowkey), attr)) || OB_ISNULL(new(storerowkey)ObStoreRowkey(obj_ptr, 1))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_ISNULL(ret_key = (BtreeKey *)ob_malloc(sizeof(BtreeKey), attr)) || OB_ISNULL(new(ret_key)BtreeKey(storerowkey))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  }
  return ret;
}

class FakeAllocator : public ObIAllocator
{
public:
  void *alloc(int64_t size) override { return ob_malloc(size, attr); }
  void* alloc(const int64_t size, const ObMemAttr &attr) override
  {
    UNUSED(attr);
    return alloc(size);
  }
  void free(void *ptr) override { ob_free(ptr); }
  static FakeAllocator*get_instance()
  {
    static FakeAllocator allocator;
    return &allocator;
  }
};

int64_t get_v(BtreeKey *ptr)
{
  int64_t tmp = 0;
  IS_EQ(OB_SUCCESS, ptr->get_rowkey()->get_rowkey().get_obj_ptr()[0].get_int(tmp));
  return tmp;
}

typedef ObKeyBtree Btree;

constexpr int64_t THREAD_COUNT = (1 << 6);

constexpr int64_t ORDER_INSERT_THREAD_COUNT = THREAD_COUNT;
constexpr int64_t RANDOM_INSERT_THREAD_COUNT = THREAD_COUNT >> 3;
constexpr int64_t INSERT_COUNT_PER_THREAD = (1 << 20);

constexpr int64_t DELETE_THREAD_COUNT = 2;//THREAD_COUNT;

constexpr int64_t REINSERT_THREAD_COUNT = 2;//THREAD_COUNT;

constexpr int64_t SCAN_THREAD_COUNT = THREAD_COUNT;

constexpr int64_t MAX_INSERT_NUM = ORDER_INSERT_THREAD_COUNT * INSERT_COUNT_PER_THREAD * 4;

TEST(TestKeyBtree, smoke_test)
{
  constexpr int64_t THREAD_COUNT = (1 << 2);

  constexpr int64_t INSERT_THREAD_COUNT = THREAD_COUNT;
  constexpr int64_t INSERT_COUNT_PER_THREAD = (1 << 16);

  constexpr int64_t SCAN_THREAD_COUNT = THREAD_COUNT;

  constexpr int64_t DELETE_THREAD_COUNT = THREAD_COUNT;
  constexpr int64_t DELETE_RANGE_COUNT_PER_THREAD = (1 << 4);
  constexpr int64_t DELETE_RANGE_SIZE = (1 << 10);

  lib::set_memory_limit(200 * 1024 * 1024 * 1024L);

  IS_EQ(INSERT_THREAD_COUNT * INSERT_COUNT_PER_THREAD
        >= DELETE_THREAD_COUNT * DELETE_RANGE_COUNT_PER_THREAD * DELETE_RANGE_SIZE, true);
  BtreeNodeAllocator allocator(*FakeAllocator::get_instance());
  Btree btree(allocator);
  int ret = OB_SUCCESS;

  IS_EQ(OB_SUCCESS, btree.init());

  // naughty thread
  std::thread normal_threads[2];
  CACHE_ALIGNED bool should_stop = false;
  for (int64_t i = 0; i < 2; ++i) {
    normal_threads[i] = std::thread([&]() {
      BtreeNodeAllocator allocator(*FakeAllocator::get_instance());
      Btree btree(allocator);
      IS_EQ(OB_SUCCESS, btree.init());
      BtreeKey *key = nullptr;
      for (int64_t j = 0; !ATOMIC_LOAD(&should_stop); ++j) {
        auto v = (BtreeVal)(j << 3);
        IS_EQ(OB_SUCCESS, alloc_key(key, j));
        IS_EQ(OB_SUCCESS, btree.insert(*key, v));
      }
      btree.destroy();
    });
  }
  // keep inserting at left bound
  std::thread head_insert_thread[2];
  CACHE_ALIGNED int64_t head_num = -1;
  for (int64_t i = 0; i < 2; ++i) {
    head_insert_thread[i] = std::thread([&]() {
      BtreeKey *key = nullptr;
      while (!ATOMIC_LOAD(&should_stop)) {
        int64_t j = ATOMIC_FAA(&head_num, -1);
        auto v = (BtreeVal)(j << 3);
        IS_EQ(OB_SUCCESS, alloc_key(key, j));
        IS_EQ(OB_SUCCESS, btree.insert(*key, v));
      }
    });
  }
  // scan with terrible range, there should be nothing returned.
  std::thread bad_scan_threads[2];
  for (int64_t i = 0; i < 2; ++i) {
    bad_scan_threads[i] = std::thread([&]() {
      int ret = OB_SUCCESS;
      BtreeKey *start_key = nullptr;
      BtreeKey *end_key = nullptr;
      BtreeKey *tmp_key = nullptr;
      BtreeVal tmp_value = nullptr;
      IS_EQ(OB_SUCCESS, alloc_key(start_key, 0));
      IS_EQ(OB_SUCCESS, alloc_key(end_key, 0));
      IS_EQ(OB_SUCCESS, alloc_key(tmp_key, 0));
      while (!ATOMIC_LOAD(&should_stop)) {
        BtreeIterator iter;
        init_key(start_key, MAX_INSERT_NUM);
        init_key(end_key, INT64_MAX);
        ret = btree.set_key_range(iter, *start_key, false, *end_key, false, 2);
        IS_EQ(OB_SUCCESS, ret);
        ret = iter.get_next(*tmp_key, tmp_value);
        IS_EQ(OB_ITER_END, ret);
        iter.reset();
        init_key(start_key, INT64_MIN);
        init_key(end_key, INT64_MIN + 1);
        ret = btree.set_key_range(iter,*start_key, false, *end_key, false, 2);
        IS_EQ(OB_SUCCESS, ret);
        ret = iter.get_next(*tmp_key, tmp_value);
        IS_EQ(OB_ITER_END, ret);
        iter.reset();
      }
    });
  }
  // scan with normal range, there should be a incresing sequence returned.
  std::thread scan_all_threads[2];
  for (int64_t i = 0; i < 2; ++i) {
    scan_all_threads[i] = std::thread([&]() {
      int ret = OB_SUCCESS;
      BtreeKey *start_key = nullptr;
      BtreeKey *end_key = nullptr;
      BtreeKey *tmp_key = nullptr;
      BtreeVal tmp_value = nullptr;
      BtreeKey *last = nullptr;
      IS_EQ(OB_SUCCESS, alloc_key(start_key, INT64_MIN));
      IS_EQ(OB_SUCCESS, alloc_key(end_key, INT64_MAX));
      IS_EQ(OB_SUCCESS, alloc_key(tmp_key, 0));
      IS_EQ(OB_SUCCESS, alloc_key(last, 0));
      while (!ATOMIC_LOAD(&should_stop)) {
        BtreeIterator iter;
        init_key(last, INT64_MIN);
        ret = btree.set_key_range(iter, *start_key, false, *end_key, false, 2);
        IS_EQ(OB_SUCCESS, ret);
        while (OB_SUCC(iter.get_next(*tmp_key, tmp_value))) {
          int cmp = 0;
          judge(tmp_key, tmp_value);
          IS_EQ(OB_SUCCESS, tmp_key->compare(*last, cmp));
          IS_EQ(true, cmp > 0);
          init_key(last, get_v(tmp_key));
        }
        IS_EQ(OB_ITER_END, ret);
        iter.reset();
      }
    });
  }

  _OB_LOG(INFO, "insert with increment key");
  std::thread order_insert_threads[ORDER_INSERT_THREAD_COUNT];
  CACHE_ALIGNED int64_t global_key = 0;
  for (int64_t i = 0; i < ORDER_INSERT_THREAD_COUNT; ++i) {
    order_insert_threads[i] = std::thread([&]() {
      int ret = OB_SUCCESS;
      BtreeKey *tmp_key = nullptr;
      BtreeVal tmp_value = nullptr;
      for (int64_t j = 0; j < INSERT_COUNT_PER_THREAD; ++j) {
        int64_t key = ATOMIC_FAA(&global_key, 1);
        auto v = (BtreeVal)(key << 3);
        IS_EQ(OB_SUCCESS, alloc_key(tmp_key, key));
        if (OB_SUCC(btree.get(*tmp_key, tmp_value))) {
          // do nothing
        } else if (OB_FAIL(btree.insert(*tmp_key, v))) {
          IS_EQ(OB_ENTRY_EXIST, ret);
        }
        IS_EQ(OB_SUCCESS, btree.get(*tmp_key, tmp_value));
        judge(tmp_key, tmp_value);
      }
    });
  }

  _OB_LOG(INFO, "insert with random key");
  std::thread random_insert_threads[RANDOM_INSERT_THREAD_COUNT];
  CACHE_ALIGNED int64_t random_sum = 0;
  for (int64_t i = 0; i < RANDOM_INSERT_THREAD_COUNT; ++i) {
    random_insert_threads[i] = std::thread([&]() {
      int ret = OB_SUCCESS;
      for (int64_t j = 0; j < INSERT_COUNT_PER_THREAD * 4; ++j) {
        BtreeKey *tmp_key = nullptr;
        BtreeVal tmp_value = nullptr;
        int64_t key = ObRandom::rand(0, MAX_INSERT_NUM - 1);
        auto v = (BtreeVal)(key << 3);
        IS_EQ(OB_SUCCESS, alloc_key(tmp_key, key));
        if (OB_SUCC(btree.get(*tmp_key, tmp_value))) {
          // do nothing
        } else if (OB_FAIL(btree.insert(*tmp_key, v))) {
          IS_EQ(OB_ENTRY_EXIST, ret);
        } else if (get_v(tmp_key) >= ORDER_INSERT_THREAD_COUNT * INSERT_COUNT_PER_THREAD) {
          ATOMIC_FAA(&random_sum, get_v(tmp_key));
        }
        IS_EQ(OB_SUCCESS, btree.get(*tmp_key, tmp_value));
        judge(tmp_key, tmp_value);
      }
    });
  }

  _OB_LOG(INFO, "del inorder");
  std::thread delete_threads[DELETE_THREAD_COUNT];
  CACHE_ALIGNED int64_t del_key = 0;
  for (int64_t i = 0; i < DELETE_THREAD_COUNT; ++i) {
    delete_threads[i] = std::thread([&]() {
      int ret = OB_SUCCESS;
      BtreeKey *tmp_key = nullptr;
      BtreeVal tmp_value = nullptr;
      int64_t key = 0;
      IS_EQ(OB_SUCCESS, alloc_key(tmp_key, 0));
      while ((key = ATOMIC_FAA(&del_key, 1)) < ORDER_INSERT_THREAD_COUNT * INSERT_COUNT_PER_THREAD) {
        init_key(tmp_key, key);
        while (OB_FAIL(btree.del(*tmp_key, tmp_value, 3))) {
          IS_EQ(OB_ENTRY_NOT_EXIST, ret);
        }
        judge(tmp_key, tmp_value);
      }
    });
  }

  _OB_LOG(INFO, "reinsert inorder");
  std::thread reinsert_threads[REINSERT_THREAD_COUNT];
  CACHE_ALIGNED int64_t reinsert_key = 0;
  for (int64_t i = 0; i < REINSERT_THREAD_COUNT; ++i) {
    reinsert_threads[i] = std::thread([&]() {
      int ret = OB_SUCCESS;
      BtreeKey *tmp_key = nullptr;
      BtreeVal tmp_value = nullptr;
      int64_t key = 0;
      IS_EQ(OB_SUCCESS, alloc_key(tmp_key, 0));
      while ((key = ATOMIC_FAA(&reinsert_key, 1)) < ORDER_INSERT_THREAD_COUNT * INSERT_COUNT_PER_THREAD) {
        init_key(tmp_key, key);
        while (OB_FAIL(btree.re_insert(*tmp_key, (BtreeVal)(key << 3)))) {
          IS_EQ(OB_ENTRY_NOT_EXIST, ret);
        }
        ret = btree.del(*tmp_key, tmp_value, 3);
        IS_EQ(OB_SUCCESS, ret);
        judge(tmp_key, tmp_value);
      }
    });
  }

  for (int64_t i = 0; i < RANDOM_INSERT_THREAD_COUNT; ++i) {
    random_insert_threads[i].join();
    _OB_LOG(INFO, "random insert end");
  }
  for (int64_t i = 0; i < ORDER_INSERT_THREAD_COUNT; ++i) {
    order_insert_threads[i].join();
    _OB_LOG(INFO, "order insert end");
  }
  for (int64_t i = 0; i < DELETE_THREAD_COUNT; ++i) {
    delete_threads[i].join();
    _OB_LOG(INFO, "delete end");
  }
  for (int64_t i = 0; i < REINSERT_THREAD_COUNT; ++i) {
    reinsert_threads[i].join();
    _OB_LOG(INFO, "reinsert end");
  }

  _OB_LOG(INFO, "cal sum");
  std::thread scan_threads[SCAN_THREAD_COUNT];
  CACHE_ALIGNED int64_t sum = 0;
  for (int64_t i = 0; i < SCAN_THREAD_COUNT; ++i) {
    scan_threads[i] = std::thread([&, i]() {
      BtreeIterator iter1, iter2;
      BtreeKey *start_key = nullptr;
      BtreeKey *end_key = nullptr;
      BtreeKey *tmp_key = nullptr;
      BtreeVal tmp_value = nullptr;
      int64_t len = ORDER_INSERT_THREAD_COUNT * INSERT_COUNT_PER_THREAD / SCAN_THREAD_COUNT;
      IS_EQ(OB_SUCCESS, alloc_key(start_key, i * len));
      IS_EQ(OB_SUCCESS, alloc_key(end_key, (i + 1) * len));
      IS_EQ(OB_SUCCESS, alloc_key(tmp_key, 0));
      IS_EQ(OB_SUCCESS, btree.set_key_range(iter1, *start_key, false, *end_key, true, 2));
      IS_EQ(OB_SUCCESS, btree.set_key_range(iter2, *start_key, false, *end_key, true, 4));
      for (int64_t j = 0; j < len; ++j) {
        IS_EQ(OB_SUCCESS, iter1.get_next(*tmp_key, tmp_value));
        IS_EQ((uint64_t)tmp_value & 1, 0);
        IS_EQ(get_v(tmp_key), i * len + j);
        judge(tmp_key, tmp_value);
        IS_EQ(OB_SUCCESS, iter2.get_next(*tmp_key, tmp_value));
        IS_EQ((uint64_t)tmp_value & 1, 1);
        IS_EQ(get_v(tmp_key), i * len + j);
        judge(tmp_key, tmp_value);
        ATOMIC_AAF(&sum, get_v(tmp_key));
      }
      IS_EQ(OB_ITER_END, iter1.get_next(*tmp_key, tmp_value));
      IS_EQ(OB_ITER_END, iter2.get_next(*tmp_key, tmp_value));
    });
  }
  for (int64_t i = 0; i < SCAN_THREAD_COUNT; ++i) {
    scan_threads[i].join();
  }
  IS_EQ((ORDER_INSERT_THREAD_COUNT * INSERT_COUNT_PER_THREAD - 1) * ORDER_INSERT_THREAD_COUNT * INSERT_COUNT_PER_THREAD / 2, sum);
  _OB_LOG(INFO, "cal sum end");

  BtreeIterator iter;
  BtreeKey *start_key = nullptr;
  BtreeKey *end_key = nullptr;
  BtreeKey *tmp_key = nullptr;
  BtreeVal tmp_value = nullptr;
  IS_EQ(OB_SUCCESS, alloc_key(start_key, 0));
  IS_EQ(OB_SUCCESS, alloc_key(end_key, INT64_MAX));
  IS_EQ(OB_SUCCESS, alloc_key(tmp_key, 0));
  IS_EQ(OB_SUCCESS, btree.set_key_range(iter, *start_key, false, *end_key, true, 5));
  for (int64_t key = 0; OB_SUCC(iter.get_next(*tmp_key, tmp_value)); ++key) {
    judge(tmp_key, tmp_value);
    if (get_v(tmp_key) < ORDER_INSERT_THREAD_COUNT * INSERT_COUNT_PER_THREAD) {
      IS_EQ(get_v(tmp_key), key);
    } else {
      random_sum -= get_v(tmp_key);
    }
  }
  IS_EQ(OB_ITER_END, ret);
  IS_EQ(random_sum, 0);

  _OB_LOG(INFO, "reinsert");
  ATOMIC_STORE(&global_key, 0);
  for (int64_t i = 0; i < SCAN_THREAD_COUNT; ++i) {
    scan_threads[i] = std::thread([&]() {
      int ret = OB_SUCCESS;
      BtreeKey *tmp_key = nullptr;
      int64_t key = 0;
      IS_EQ(OB_SUCCESS, alloc_key(tmp_key, 0));
      while ((key = ATOMIC_FAA(&global_key, 1)) < ORDER_INSERT_THREAD_COUNT * INSERT_COUNT_PER_THREAD) {
        init_key(tmp_key, key);
        while (OB_FAIL(btree.re_insert(*tmp_key, (BtreeVal)(key << 3)))) {
          IS_EQ(OB_ENTRY_NOT_EXIST, ret);
        }
      }
    });
  }
  for (int64_t i = 0; i < SCAN_THREAD_COUNT; ++i) {
    scan_threads[i].join();
  }
  _OB_LOG(INFO, "reinsert end");
  int32_t pos = btree.update_split_info(7);
  _OB_LOG(INFO, "btree split info %d", pos);
  IS_EQ(OB_SUCCESS, btree.destroy());

  ATOMIC_STORE(&should_stop, true);
  for (int64_t i = 0; i < 2; ++i) {
    normal_threads[i].join();
    head_insert_thread[i].join();
    bad_scan_threads[i].join();
    scan_all_threads[i].join();
  }
}

}
}

int main(int argc, char **argv)
{
  //oceanbase::unittest::BIND_CPU(pthread_self());
  oceanbase::common::ObLogger::get_logger().set_file_name("test_keybtree.log", true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
