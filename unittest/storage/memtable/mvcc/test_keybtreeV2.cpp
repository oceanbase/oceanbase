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
#include "lib/allocator/ob_malloc.h"
#include "lib/oblog/ob_log.h"
#include "lib/random/ob_random.h"
#include "common/object/ob_object.h"
#include <gtest/gtest.h>
#include <thread>
#include <algorithm>
#include <iostream>
#include <vector>
#include <unordered_set>
#include <atomic>

namespace oceanbase {
namespace unittest {
using namespace oceanbase::keybtree;

#define DUMP_BTREE                              \
  {                                             \
    FILE *file = fopen("dump_btree.txt", "w+"); \
    btree.dump(file);                           \
    fclose(file);                               \
  }

class FakeAllocator : public ObIAllocator {
public:
  FakeAllocator() : remain_(0), is_limited_(false)
  {}
  const char *attr = ObModIds::TEST;
  void *alloc(int64_t size) override
  {
    void *block = nullptr;
    if (is_limited_) {
      if (remain_ > 0) {
        block = ob_malloc(size, attr);
        remain_ -= size;
      }
    } else {
      block = ob_malloc(size, attr);
    }
    return block;
  }
  void *alloc_key(int64_t size)
  {
    return ob_malloc(size, attr);
  }
  void *alloc(const int64_t size, const ObMemAttr &attr) override
  {
    void *block = nullptr;
    UNUSED(attr);
    if (is_limited_) {
      if (remain_ > 0) {
        block = alloc(size);
        remain_ -= size;
      }
    } else {
      block = alloc(size);
    }
    return block;
  }
  void free(void *ptr) override
  {
    ob_free(ptr);
  }
  void set_remain(int remain)
  {
    remain_ = remain;
    is_limited_ = true;
  }
  void unset_limited()
  {
    is_limited_ = false;
  }
  static FakeAllocator *get_instance()
  {
    static FakeAllocator allocator;
    return &allocator;
  }

private:
  std::atomic<int64_t> remain_;
  bool is_limited_;
};

class FakeKey {
public:
  FakeKey() : obj_(nullptr)
  {}
  FakeKey(ObObj *obj) : obj_(obj)
  {}
  void set_int(int64_t data)
  {
    obj_->set_int(data);
  }
  int compare(FakeKey other, int &cmp) const
  {
    return obj_->compare(*other.obj_, cmp);
  }
  int64_t to_string(char *buf, const int64_t limit) const
  {
    return obj_->to_string(buf, limit);
  }

  ObObj *get_ptr() const
  {
    return obj_;
  }
  ObObj *obj_;
};

FakeKey build_int_key(int64_t key)
{
  auto alloc = FakeAllocator::get_instance();
  void *block = alloc->alloc_key(sizeof(ObObj));
  EXPECT_TRUE(OB_NOT_NULL(block));
  ObObj *obj = new (block) ObObj(key);
  return FakeKey(obj);
}

void free_key(FakeKey &key)
{
  auto alloc = FakeAllocator::get_instance();
  alloc->free((void *)key.obj_);
}

using BtreeNode = BtreeNode<FakeKey, int64_t *>;
using ObKeyBtree = ObKeyBtree<FakeKey, int64_t *>;
using BtreeIterator = BtreeIterator<FakeKey, int64_t *>;
void judge_tree_scan(ObKeyBtree *btree, FakeKey start_key, FakeKey end_key, bool exclude_start_key,
    bool exclude_end_key, bool is_backward, std::vector<int64_t> &answer)
{
  FakeKey key;
  int64_t *val = nullptr;
  int i = 0;

  BtreeIterator iter;
  btree->set_key_range(iter, start_key, exclude_start_key, end_key, exclude_end_key);
  while (iter.get_next(key, val) == OB_SUCCESS) {
    ASSERT_EQ(key.get_ptr()->get_int(), answer[i]);
    i++;
  }
  ASSERT_EQ(i, answer.size());
}

void free_btree(ObKeyBtree &btree)
{
  FakeKey start_key = build_int_key(INT64_MIN);
  FakeKey end_key = build_int_key(INT64_MAX);
  FakeKey key;
  int64_t *val = nullptr;
  FakeAllocator *allocator = FakeAllocator::get_instance();
  BtreeIterator iter;
  btree.set_key_range(iter, start_key, true, end_key, true);
  std::vector<FakeKey> keys;
  while (iter.get_next(key, val) == OB_SUCCESS) {
    keys.push_back(key);
  }
  for (auto &key : keys) {
    allocator->free(key.get_ptr());
  }
  btree.destroy(false /*is_batch_destroy*/);
}

TEST(TestBtree, smoke_test)
{
  constexpr int64_t KEY_NUM = 100000;
  std::vector<int64_t> data(KEY_NUM);

  FakeAllocator *allocator = FakeAllocator::get_instance();
  BtreeNodeAllocator<FakeKey, int64_t *> node_allocator(*allocator);
  ObKeyBtree btree(node_allocator);
  FakeKey search_key = build_int_key(0);

  ASSERT_EQ(btree.init(), OB_SUCCESS);

  for (int i = 0; i <= KEY_NUM; i++) {
    data[i] = i * 2;
  }
  std::random_shuffle(data.begin(), data.end());

  // test insert and search
  for (int len = 1; len <= KEY_NUM; len++) {
    int64_t cur = data[len - 1];
    FakeKey key = build_int_key(cur);
    int64_t *val = &data[len - 1];
    btree.insert(key, val);
    if (len % (KEY_NUM / 49) == 0) {
      for (int i = 0; i < len * 2; i++) {
        search_key.set_int(data[i / 2] + i % 2);
        if (i % 2 == 0) {
          ASSERT_EQ(btree.get(search_key, val), OB_SUCCESS);
        } else {
          ASSERT_EQ(btree.get(search_key, val), OB_ENTRY_NOT_EXIST);
        }
      }
    }
  }

  std::sort(data.begin(), data.end());
  search_key.set_int(-1);
  int64_t *val = nullptr;
  ASSERT_EQ(btree.get(search_key, val), OB_ENTRY_NOT_EXIST);

  FakeKey start_key = build_int_key(0);
  FakeKey end_key = build_int_key(0);

  // test scan
  int REPEAT_COUNT = 100;

  // forward include
  REPEAT_COUNT = 100;
  while (REPEAT_COUNT--) {
    int64_t start_int = ObRandom::rand(-KEY_NUM, KEY_NUM * 3);
    int64_t end_int = ObRandom::rand(start_int + 1, KEY_NUM * 3);
    start_key.set_int(start_int);
    end_key.set_int(end_int);
    std::vector<int64_t> ans;
    for (int i = max(0, (start_int + 1) / 2 * 2); i <= min((KEY_NUM - 1) * 2, min(end_int / 2 * 2, end_int)); i += 2) {
      ans.push_back(i);
    }
    judge_tree_scan(&btree, start_key, end_key, false, false, false, ans);
  }

  // forward exclude
  REPEAT_COUNT = 100;
  while (REPEAT_COUNT--) {
    int64_t start_int = ObRandom::rand(-KEY_NUM, KEY_NUM * 3);
    int64_t end_int = ObRandom::rand(start_int + 1, KEY_NUM * 3);
    start_key.set_int(start_int);
    end_key.set_int(end_int);
    std::vector<int64_t> ans;
    for (int i = max(0, (start_int + 2) / 2 * 2); i <= min((KEY_NUM - 1) * 2, min((end_int - 1) / 2 * 2, end_int - 1));
         i += 2) {
      ans.push_back(i);
    }
    judge_tree_scan(&btree, start_key, end_key, true, true, false, ans);
  }

  // backward include
  REPEAT_COUNT = 100;
  while (REPEAT_COUNT--) {
    int64_t start_int = ObRandom::rand(-KEY_NUM, KEY_NUM * 3);
    int64_t end_int = ObRandom::rand(start_int + 1, KEY_NUM * 3);
    start_key.set_int(start_int);
    end_key.set_int(end_int);
    std::vector<int64_t> ans;
    for (int i = min((KEY_NUM - 1) * 2, min(end_int / 2 * 2, end_int)); i >= max(0, (start_int + 1) / 2 * 2); i -= 2) {
      ans.push_back(i);
    }
    judge_tree_scan(&btree, end_key, start_key, false, false, true, ans);
  }

  // backward exclude
  REPEAT_COUNT = 100;
  while (REPEAT_COUNT--) {
    int64_t start_int = ObRandom::rand(-KEY_NUM, KEY_NUM * 3);
    int64_t end_int = ObRandom::rand(start_int + 1, KEY_NUM * 3);
    start_key.set_int(start_int);
    end_key.set_int(end_int);
    std::vector<int64_t> ans;
    for (int i = min((KEY_NUM - 1) * 2, min((end_int - 1) / 2 * 2, end_int - 1)); i >= max(0, (start_int + 2) / 2 * 2);
         i -= 2) {
      ans.push_back(i);
    }
    judge_tree_scan(&btree, end_key, start_key, true, true, true, ans);
  }

  free_btree(btree);
  allocator->free(search_key.get_ptr());
  allocator->free(start_key.get_ptr());
  allocator->free(end_key.get_ptr());
}

TEST(TestEventualConsistency, smoke_test)
{
  constexpr uint64_t KEY_NUM = 6400000;
  constexpr uint64_t THREAD_COUNT = 64;
  constexpr uint64_t PER_THREAD_INSERT_COUNT = KEY_NUM / THREAD_COUNT;

  FakeAllocator *allocator = FakeAllocator::get_instance();
  BtreeNodeAllocator<FakeKey, int64_t *> node_allocator(*allocator);
  ObKeyBtree btree(node_allocator);
  std::thread threads[THREAD_COUNT];

  ASSERT_EQ(btree.init(), OB_SUCCESS);

  // prepare insert keys
  std::vector<std::vector<int64_t>> data(THREAD_COUNT, std::vector<int64_t>(PER_THREAD_INSERT_COUNT));
  for (int i = 0; i < THREAD_COUNT; i++) {
    for (int j = 0; j < PER_THREAD_INSERT_COUNT; j++) {
      data[i][j] = THREAD_COUNT * j + i;
    }
    std::random_shuffle(data[i].begin(), data[i].end());
  }

  // concurrent insert
  for (int thread_id = 0; thread_id < THREAD_COUNT; thread_id++) {
    threads[thread_id] = std::thread(
        [&](int i) {
          for (int j = 0; j < PER_THREAD_INSERT_COUNT; j++) {
            int64_t *val = &(data[i][j]);
            btree.insert(build_int_key(data[i][j]), val);
          }
        },
        thread_id);
  }

  for (int thread_id = 0; thread_id < THREAD_COUNT; thread_id++) {
    threads[thread_id].join();
  }

  // evaluate the tree
  FakeKey start_key = build_int_key(0);
  FakeKey end_key = build_int_key(KEY_NUM);
  FakeKey key;
  int64_t *val;

  BtreeIterator iter;
  btree.set_key_range(iter, start_key, false, end_key, false);
  int i = 0;
  while (iter.get_next(key, val) == OB_SUCCESS) {
    ASSERT_EQ(key.get_ptr()->get_int(), i);
    i++;
  }
  ASSERT_EQ(i, KEY_NUM);

  free_btree(btree);
  allocator->free(start_key.get_ptr());
  allocator->free(end_key.get_ptr());
}

TEST(TestMonotonicReadWrite, smoke_test)
{
  constexpr int KEY_NUM = 6400000;
  constexpr int WRITE_THREAD_COUNT = 32;
  constexpr int PER_THREAD_INSERT_COUNT = KEY_NUM / WRITE_THREAD_COUNT;
  constexpr int SCAN_THREAD_COUNT = 32;
  constexpr int PER_THREAD_SCAN_COUNT = 8;

  FakeAllocator *allocator = FakeAllocator::get_instance();
  BtreeNodeAllocator<FakeKey, int64_t *> node_allocator(*allocator);
  ObKeyBtree btree(node_allocator);

  ASSERT_EQ(btree.init(), OB_SUCCESS);

  // constructing insert keys
  std::vector<std::vector<int64_t>> data(WRITE_THREAD_COUNT, std::vector<int64_t>(PER_THREAD_INSERT_COUNT));
  for (int i = 0; i < WRITE_THREAD_COUNT; i++) {
    for (int j = 0; j < PER_THREAD_INSERT_COUNT; j++) {
      data[i][j] = WRITE_THREAD_COUNT * j + i;
    }
    std::random_shuffle(data[i].begin(), data[i].end());
  }

  std::thread write_threads[WRITE_THREAD_COUNT];
  for (int thread_id = 0; thread_id < WRITE_THREAD_COUNT; thread_id++) {
    write_threads[thread_id] = std::thread(
        [&](int i) {
          // insert in order
          for (int j = 0; j < PER_THREAD_INSERT_COUNT; j++) {
            int64_t *val = &(data[i][j]);
            btree.insert(build_int_key(data[i][j]), val);
            usleep(1);
          }
        },
        thread_id);
  }

  std::thread scan_threads[SCAN_THREAD_COUNT];
  for (int thread_id = 0; thread_id < SCAN_THREAD_COUNT; thread_id++) {
    scan_threads[thread_id] = std::thread(
        [&](int thread_id) {
          FakeKey start_key = build_int_key(-1);
          FakeKey end_key = build_int_key(KEY_NUM + 1);
          int scan_count = PER_THREAD_SCAN_COUNT;
          std::unordered_set<int64_t *> last_results;
          while (scan_count--) {
            std::unordered_set<int64_t *> results;
            FakeKey key;
            int64_t *val;
            if (thread_id % 2 == 0) {
              // scan forward
              BtreeIterator iter;
              btree.set_key_range(iter, start_key, false, end_key, false);
              int64_t last = -1;
              while (iter.get_next(key, val) == OB_SUCCESS) {
                results.insert(val);
                ASSERT_GT(key.get_ptr()->get_int(), last);
                last = key.get_ptr()->get_int();
              }
            } else {
              // scan backward
              BtreeIterator iter;
              btree.set_key_range(iter, end_key, false, start_key, false);
              int64_t last = KEY_NUM + 1;
              while (iter.get_next(key, val) == OB_SUCCESS) {
                results.insert(val);
                ASSERT_LT(key.get_ptr()->get_int(), last);
                last = key.get_ptr()->get_int();
              }
            }
            // test monotonic write, if a thread see a key A, then it should see all keys inserted before A
            for (int i = 0; i < WRITE_THREAD_COUNT; i++) {
              if (thread_id % 2 == 0) {
                int64_t min = KEY_NUM + 1;
                for (int j = PER_THREAD_INSERT_COUNT - 1; j >= 0; j--) {
                  ASSERT_TRUE(data[i][j] < min || results.count(&data[i][j]) == 1);
                  if (results.count(&data[i][j]) == 1) {
                    min = std::min(min, data[i][j]);
                  }
                }
              } else {
                int64_t max = -1;
                for (int j = PER_THREAD_INSERT_COUNT - 1; j >= 0; j--) {
                  ASSERT_TRUE(data[i][j] > max || results.count(&data[i][j]) == 1);
                  if (results.count(&data[i][j]) == 1) {
                    max = std::max(max, data[i][j]);
                  }
                }
              }
            }
            // test monotonic read, if a thread do two scan, then the frist scan result should be the subset of
            // the second scan result.
            for (auto i : last_results) {
              ASSERT_TRUE(results.count(i) == 1);
            }
            last_results = results;
          }
          allocator->free(start_key.get_ptr());
          allocator->free(end_key.get_ptr());
        },
        thread_id);
  }

  for (int thread_id = 0; thread_id < WRITE_THREAD_COUNT; thread_id++) {
    write_threads[thread_id].join();
  }
  for (int thread_id = 0; thread_id < SCAN_THREAD_COUNT; thread_id++) {
    scan_threads[thread_id].join();
  }

  free_btree(btree);
}

TEST(TestSequentialConsistency, smoke_test)
{
  constexpr int PER_THREAD_INSERT_COUNT = 200000;
  constexpr int READ_THREAD_COUNT = 16;

  std::atomic<int> progress(-1);

  FakeAllocator *allocator = FakeAllocator::get_instance();
  BtreeNodeAllocator<FakeKey, int64_t *> node_allocator(*allocator);
  ObKeyBtree btree(node_allocator);
  ASSERT_EQ(btree.init(), OB_SUCCESS);

  std::thread main_thread([&] {
    for (; progress < PER_THREAD_INSERT_COUNT; progress++) {
      usleep(1);
    }
  });

  std::vector<int64_t> insert_keys(PER_THREAD_INSERT_COUNT * 2);
  for (int i = 0; i < insert_keys.size(); i++) {
    insert_keys[i] = i;
  }
  std::random_shuffle(insert_keys.begin(), insert_keys.end());

  std::thread write_threads[2];
  for (int thread_id = 0; thread_id < 2; thread_id++) {
    write_threads[thread_id] = std::thread(
        [&](int thread_id) {
          int last = -1;
          int insert_id = 0;
          while (last < PER_THREAD_INSERT_COUNT) {
            while (last >= progress) {}
            last++;
            insert_id = last * 2 + thread_id;
            int64_t *val = &(insert_keys[insert_id]);
            btree.insert(build_int_key(insert_keys[insert_id]), val);
          }
        },
        thread_id);
  }

  std::vector<std::vector<bool>> read_results(READ_THREAD_COUNT, std::vector<bool>(PER_THREAD_INSERT_COUNT));

  std::thread read_threads[READ_THREAD_COUNT];
  for (int thread_id = 0; thread_id < READ_THREAD_COUNT; thread_id++) {
    read_threads[thread_id] = std::thread(
        [&](int thread_id) {
          int64_t *val;
          for (int i = 0; i < PER_THREAD_INSERT_COUNT; i++) {
            FakeKey search_key1 = build_int_key(insert_keys[i * 2]);
            FakeKey search_key2 = build_int_key(insert_keys[i * 2 + 1]);
            if (thread_id % 2 == 0) {
              while (btree.get(search_key1, val) != OB_SUCCESS) {}
              if (btree.get(search_key2, val) == OB_ENTRY_NOT_EXIST) {
                // the order this thread saw is: search_key1 -> search_key2
                read_results[thread_id][i] = true;
              }
            } else {
              while (btree.get(search_key2, val) != OB_SUCCESS) {}
              if (btree.get(search_key1, val) == OB_ENTRY_NOT_EXIST) {
                // the order this thread saw is: search_key2 -> search_key1
                read_results[thread_id][i] = true;
              }
            }
            allocator->free(search_key1.get_ptr());
            allocator->free(search_key2.get_ptr());
          }
        },
        thread_id);
  }

  main_thread.join();
  write_threads[0].join();
  write_threads[1].join();
  for (int i = 0; i < READ_THREAD_COUNT; i++) {
    read_threads[i].join();
  }

  int count = 0;
  for (int j = 0; j < PER_THREAD_INSERT_COUNT; j++) {
    for (int i = 0; i < READ_THREAD_COUNT; i++) {
      read_results[i % 2][j] = read_results[i % 2][j] || read_results[i][j];
    }
    // threads shouldn't see different order
    ASSERT_FALSE(read_results[0][j] && read_results[1][j]);
  }

  free_btree(btree);
}

void test_memory_not_enough(int max_nodes_cnt)
{
  constexpr uint64_t KEY_NUM = 6400000;
  constexpr uint64_t THREAD_COUNT = 64;
  constexpr uint64_t PER_THREAD_INSERT_COUNT = KEY_NUM / THREAD_COUNT;

  FakeAllocator *allocator = FakeAllocator::get_instance();
  BtreeNodeAllocator<FakeKey, int64_t *> node_allocator(*allocator);
  ObKeyBtree btree(node_allocator);
  int64_t max_size = max_nodes_cnt * sizeof(BtreeNode);

  ASSERT_EQ(btree.init(), OB_SUCCESS);

  std::thread threads[THREAD_COUNT];
  std::vector<std::vector<int64_t>> data(THREAD_COUNT, std::vector<int64_t>(PER_THREAD_INSERT_COUNT));
  for (int i = 0; i < THREAD_COUNT; i++) {
    for (int j = 0; j < PER_THREAD_INSERT_COUNT; j++) {
      data[i][j] = THREAD_COUNT * j + i;
    }
    std::random_shuffle(data[i].begin(), data[i].end());
  }

  allocator->set_remain(max_size);

  int insert_progress[THREAD_COUNT];
  // concurrent insert
  for (int thread_id = 0; thread_id < THREAD_COUNT; thread_id++) {
    threads[thread_id] = std::thread(
        [&](int i) {
          int ret = OB_SUCCESS;
          int64_t *val = &data[i][0];
          insert_progress[i] = -1;
          for (int j = 0; j < PER_THREAD_INSERT_COUNT && OB_SUCC(btree.insert(build_int_key(data[i][j]), val));
               j++, val = &data[i][j]) {
            insert_progress[i] = j;
          }
          ASSERT_EQ(ret, OB_ALLOCATE_MEMORY_FAILED);
        },
        thread_id);
  }

  for (int thread_id = 0; thread_id < THREAD_COUNT; thread_id++) {
    threads[thread_id].join();
  }

  allocator->unset_limited();

  // evaluate the tree
  FakeKey key = build_int_key(0);
  int64_t *val = nullptr;
  std::unordered_set<int64_t> results;
  for (int i = 0; i < THREAD_COUNT; i++) {
    for (int j = 0; j <= insert_progress[i]; j++) {
      key.set_int(data[i][j]);
      ASSERT_EQ(btree.get(key, val), OB_SUCCESS);
      ASSERT_EQ(*val, data[i][j]);
      results.insert(*val);
    }
  }
  free_key(key);

  FakeKey start_key = build_int_key(0);
  FakeKey end_key = build_int_key(KEY_NUM);

  BtreeIterator iter;
  btree.set_key_range(iter, start_key, false, end_key, false);
  int64_t last = -1;
  while (iter.get_next(key, val) == OB_SUCCESS) {
    ASSERT_GT(*val, last);
    last = *val;
    results.erase(*val);
  }

  ASSERT_EQ(results.size(), 0);

  free_btree(btree);
  free_key(start_key);
  free_key(end_key);
}

TEST(TestMemoryNotEnough, smoke_test)
{
  int nodes_cnt[20] = {1, 2, 3, 15, 16, 17, 18, 19, 20, 225, 227, 229, 230, 500, 1000, 2000, 3375, 5000, 8000, 10000};
  for (int i = 0; i < 20; i++) {
    test_memory_not_enough(nodes_cnt[i]);
  }
}

}  // namespace unittest
}  // namespace oceanbase

int main(int argc, char **argv)
{
  // oceanbase::unittest::BIND_CPU(pthread_self());
  oceanbase::common::ObLogger::get_logger().set_file_name("test_keybtreeV2.log", true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
