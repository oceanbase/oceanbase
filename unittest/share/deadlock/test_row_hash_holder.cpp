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


#include <algorithm>
#include <map>
#include <mutex>
#define UNITTEST_DEBUG
#include <gtest/gtest.h>
#include <gmock/gmock.h>
#define private public
#define protected public
#include "storage/memtable/hash_holder/ob_row_hash_holder_map.cpp"
#include "storage/memtable/hash_holder/ob_row_hash_holder_info.cpp"

using namespace oceanbase::obrpc;
using namespace std;

namespace oceanbase {
namespace unittest {

using namespace common;
using namespace share::detector;
using namespace share;
using namespace std;
using namespace memtable;
using namespace transaction;

class TestRowHolderMapper : public ::testing::Test {
public:
  TestRowHolderMapper() {}
  ~TestRowHolderMapper() {}
  virtual void SetUp() {
    HashHolderAllocator::ALLOC_TIMES = 0;
    HashHolderAllocator::FREE_TIMES = 0;
    HashHolderAllocator::NO_MEMORY = false;
    map = new RowHolderMapper();
    ASSERT_EQ(map->init(true), OB_SUCCESS);
  }
  virtual void TearDown() {
    delete map;
    ASSERT_EQ(HashHolderAllocator::ALLOC_TIMES, HashHolderAllocator::FREE_TIMES);// 确保没有内存泄露
  }
  void fetch_all_cached_nodes(RowHolderMapper &map) {
    for (int64_t i = 0; i < HashHolderFactory::MIN_NODE_CACHE_CNT; ++i) {
      RowHolderNode *node = nullptr;
      node = map.factory_.node_cache_.fetch_cache_object_and_construct(1, ObTransID(1), ObTxSEQ(2), share::SCN());
      OB_ASSERT(nullptr != node);
    }
    ASSERT_EQ(nullptr, map.factory_.node_cache_.fetch_cache_object_and_construct(1, ObTransID(1), ObTxSEQ(2), share::SCN()));
    for (int64_t i = 0; i < HashHolderFactory::MIN_LIST_CACHE_CNT; ++i) {
      RowHolderList *list = nullptr;
      list = map.factory_.list_cache_.fetch_cache_object_and_construct(1, nullptr);
      OB_ASSERT(nullptr != list);
    }
    ASSERT_EQ(nullptr, map.factory_.list_cache_.fetch_cache_object_and_construct(1, nullptr));
  }
  RowHolderMapper *map;
};

TEST_F(TestRowHolderMapper, test_cached_node) {
  RowHolderNode *node = nullptr;
  node = map->factory_.node_cache_.fetch_cache_object_and_construct(1, ObTransID(1), ObTxSEQ(2), share::SCN());
  OB_ASSERT(nullptr != node);
  RowHolderNode *node2 = nullptr;
  map->factory_.node_cache_.revert_cache_object(node, map->factory_.get_object_cached_thread_idx(node));
  node2 = map->factory_.node_cache_.fetch_cache_object_and_construct(1, ObTransID(1), ObTxSEQ(2), share::SCN());
  ASSERT_EQ(node, node2);
}

TEST_F(TestRowHolderMapper, test_multi_thread_revert_node_across_threads) {
  HashHolderGlocalMonitor monitor;
  HashHolderFactory factory(monitor);
  factory.node_cache_.init(1, 16);
  ObPromise<void> second_start;
  ASSERT_EQ(OB_SUCCESS, second_start.init());
  vector<thread> v_t;
  vector<ObPromise<void>> v_promise;
  mutex lock;
  vector<RowHolderNode *> v_p_node;
  for (int i = 0; i < 16; ++i) {
    v_promise.push_back(ObPromise<void>());
  }
  for (int i = 0; i < 16; ++i) {
    ASSERT_EQ(OB_SUCCESS, v_promise[i].init());
    auto &promise = v_promise[i];
    OCCAM_LOG(DEBUG, "promise init", KP(&promise), KP(promise.p_future_base_));
    v_t.emplace_back([&factory, &second_start, &promise, &lock, &v_p_node]() {
      RowHolderNode *node = nullptr;
      ASSERT_EQ(OB_SUCCESS, factory.create(node, 1, ObTransID(1), ObTxSEQ(2), share::SCN()));
      {
        lock_guard<mutex> lg(lock);
        v_p_node.push_back(node);
        int idx = v_p_node.size() - 1;
        OCCAM_LOG(DEBUG, "thread create node", K(idx), KP(node));
      }
      ASSERT_EQ(OB_SUCCESS, promise.set());
      ASSERT_EQ(OB_SUCCESS, second_start.wait());
      {
        lock_guard<mutex> lg(lock);
        int idx = rand() % v_p_node.size();
        node = v_p_node[idx];
        v_p_node.erase(v_p_node.begin() + idx);
        factory.destroy(node, 1);
        OCCAM_LOG(DEBUG, "thread destroy node", K(idx), KP(node));
      }
    });
  }
  for (int i = 0; i < 16; ++i) {// wait all threads to create nodes
    OCCAM_LOG(DEBUG, "promise init", KP(&v_promise[i]), KP(v_promise[i].p_future_base_));
    ASSERT_EQ(OB_SUCCESS, v_promise[i].wait());
  }
  ASSERT_EQ(OB_SUCCESS, second_start.set());// start all threads to destroy nodes
  for (int i = 0; i < 16; ++i) {// wait all threads done
    v_t[i].join();
  }
}

inline share::SCN mock_scn(int64_t val) { share::SCN scn; scn.convert_for_gts(val); return scn; }

TEST_F(TestRowHolderMapper, test_replay_out_of_order) {
  fetch_all_cached_nodes(*map);
  map->insert_or_replace_hash_holder(1, ObTransID(1), ObTxSEQ::mk_v0(5), mock_scn(1));
  map->insert_or_replace_hash_holder(1, ObTransID(1), ObTxSEQ::mk_v0(7), mock_scn(2));
  map->insert_or_replace_hash_holder(1, ObTransID(1), ObTxSEQ::mk_v0(6), mock_scn(3));
  map->insert_or_replace_hash_holder(1, ObTransID(1), ObTxSEQ::mk_v0(4), mock_scn(4));
  map->insert_or_replace_hash_holder(1, ObTransID(2), ObTxSEQ::mk_v0(1), mock_scn(4));
  map->insert_or_replace_hash_holder(1, ObTransID(3), ObTxSEQ::mk_v0(8), mock_scn(2));
  map->~RowHolderMapper();
}

TEST_F(TestRowHolderMapper, test_list) {
  int64_t init_alloc_time = HashHolderAllocator::ALLOC_TIMES;
  int64_t init_free_time = HashHolderAllocator::FREE_TIMES;
  fetch_all_cached_nodes(*map);
  RowHolderList list(map);
  ASSERT_EQ(true, list.is_empty());// 最开始list无效
  ASSERT_EQ(OB_SUCCESS, list.insert(1, ObTransID(1), ObTxSEQ(1), share::SCN()));// 插入记录
  ASSERT_EQ(1 + init_alloc_time, HashHolderAllocator::ALLOC_TIMES);// 分配堆内存
  ASSERT_EQ(false, list.is_empty());// 插入一条记录后生效
  ASSERT_EQ(1, list.size());// 此时记录数量为1
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, list.erase(1, ObTransID(1), ObTxSEQ(2)));// 擦除记录时必须两个值都能对应上
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, list.erase(1, ObTransID(2), ObTxSEQ(1)));// 擦除记录时必须两个值都能对应上
  ASSERT_EQ(OB_SUCCESS, list.erase(1, ObTransID(1), ObTxSEQ(1)));// 该记录可被擦除
  ASSERT_EQ(true, list.is_empty());// 空list无效
  ASSERT_EQ(OB_SUCCESS, list.insert(1, ObTransID(1), ObTxSEQ(2), share::SCN()));// 同一事务的第一条记录
  ASSERT_EQ(OB_SUCCESS, list.insert(1, ObTransID(1), ObTxSEQ(4), share::SCN()));// 同一事务的第二条记录
  ASSERT_EQ(3 + init_alloc_time, HashHolderAllocator::ALLOC_TIMES);// 分配堆内存
  ASSERT_EQ(ObTxSEQ(4), list.list_tail_->holder_info_.seq_);// 尾部是seq较大的记录
  ASSERT_EQ(OB_SUCCESS, list.insert(1, ObTransID(1), ObTxSEQ(5), share::SCN()));// 尾部新插记录
  ASSERT_EQ(4 + init_alloc_time, HashHolderAllocator::ALLOC_TIMES);// 分配堆内存
  ASSERT_EQ(ObTxSEQ(5), list.list_tail_->holder_info_.seq_);// 尾部是seq较大的记录
  ASSERT_EQ(OB_SUCCESS, list.insert(1, ObTransID(1), ObTxSEQ(3), share::SCN()));// 中间新插记录
  ASSERT_EQ(5 + init_alloc_time, HashHolderAllocator::ALLOC_TIMES);// 分配堆内存
  ASSERT_EQ(ObTxSEQ(3), list.list_tail_->prev_->prev_->holder_info_.seq_);// 确认为倒数第3
  ASSERT_EQ(OB_SUCCESS, list.insert(1, ObTransID(1), ObTxSEQ(1), share::SCN()));// 头部新插记录
  ASSERT_EQ(6 + init_alloc_time, HashHolderAllocator::ALLOC_TIMES);// 分配堆内存
  ASSERT_EQ(ObTxSEQ(1), list.list_tail_->prev_->prev_->prev_->prev_->holder_info_.seq_);// 确认为倒数第1
  ASSERT_EQ(false, list.is_empty());// 此时list有效
  ASSERT_EQ(5, list.size());// 此时记录数量为5
  ASSERT_EQ(OB_SUCCESS, list.insert(1, ObTransID(1), ObTxSEQ(3), share::SCN()));// 记录重复, 但仍然可以插入
  ASSERT_EQ(7 + init_alloc_time, HashHolderAllocator::ALLOC_TIMES);// 分配堆内存
  HashHolderAllocator::NO_MEMORY = true;
  ASSERT_EQ(OB_ALLOCATE_MEMORY_FAILED, list.insert(1, ObTransID(1), ObTxSEQ(6), share::SCN()));// 无内存插入失败
  ASSERT_EQ(false, list.is_empty());// list状态不变
  ASSERT_EQ(6, list.size());// list状态不变
  HashHolderAllocator::NO_MEMORY = false;
  ASSERT_EQ(OB_SUCCESS, list.insert(1, ObTransID(2), ObTxSEQ(1), share::SCN()));// 出现同一hash的tx id冲突，链表重置
  ASSERT_EQ(1, list.size());// 只有一条新纪录
  ASSERT_EQ(OB_SUCCESS, list.insert(1, ObTransID(2), ObTxSEQ(2), share::SCN()));// 新增一条堆上记录
  ASSERT_EQ(9 + init_alloc_time, HashHolderAllocator::ALLOC_TIMES);// 分配堆内存
}

TEST_F(TestRowHolderMapper, test_list_chaos_operation) {
  int64_t init_alloc_time = HashHolderAllocator::ALLOC_TIMES;
  int64_t init_free_time = HashHolderAllocator::FREE_TIMES;
  fetch_all_cached_nodes(*map);
  int test_times = 100;
  while (--test_times) {
    // 生成[0, 99)的seq序列
    std::vector<int> seq_seed;
    for (int i = 0; i < 100; ++i) {
      seq_seed.push_back(i);
    }
    // 将序列随机打乱
    std::random_device rd; // 真正的随机数生成器，用于生成种子
    std::mt19937 g(rd()); // 使用种子初始化伪随机数生成器
    std::shuffle(seq_seed.begin(), seq_seed.end(), rd);
    // 将序列插入到list中
    RowHolderList list(map);
    for (auto &seq : seq_seed) {
      ASSERT_EQ(OB_SUCCESS, list.insert(1, ObTransID(1), ObTxSEQ(seq), share::SCN()));
    }
    // 校验插入后的list中元素顺序是降序
    RowHolderNode *node = list.list_tail_;
    RowHolderNode *last_node = nullptr;
    while (node) {
      if (last_node) {
        ASSERT_EQ(last_node->holder_info_.seq_.raw_val_, node->holder_info_.seq_.raw_val_ + 1);
      }
      last_node = node;
      node = node->prev_;
    }
    // 再打乱一次
    std::shuffle(seq_seed.begin(), seq_seed.end(), rd);
    // 使用另一种混乱的顺序擦除这些记录
    for (auto &seq : seq_seed) {
      ASSERT_EQ(OB_SUCCESS, list.erase(1, ObTransID(1), ObTxSEQ(seq)));
    }
  }
  ASSERT_EQ(HashHolderAllocator::ALLOC_TIMES - init_alloc_time, HashHolderAllocator::FREE_TIMES - init_free_time);// 确保没有内存泄露
}

TEST_F(TestRowHolderMapper, test_bucket) {
  int64_t init_alloc_time = HashHolderAllocator::ALLOC_TIMES;
  int64_t init_free_time = HashHolderAllocator::FREE_TIMES;
  HashHolderGlocalMonitor monitor;
  fetch_all_cached_nodes(*map);
  RowHolderBucketHead bucket(map);
  RowHolderInfo info;
  ASSERT_EQ(0, bucket.size());
  // 首先测试单list单记录
  ASSERT_EQ(OB_SUCCESS, bucket.insert(1, ObTransID(1), ObTxSEQ(1), share::SCN(), monitor));
  ASSERT_EQ(2 + init_alloc_time, HashHolderAllocator::ALLOC_TIMES);
  ASSERT_EQ(1, bucket.size());
  ASSERT_EQ(OB_SUCCESS, bucket.get(1, info, monitor));
  ASSERT_EQ(ObTransID(1), info.tx_id_);
  ASSERT_EQ(ObTxSEQ(1), info.seq_);
  ASSERT_EQ(OB_SUCCESS, bucket.erase(1, ObTransID(1), ObTxSEQ(1), monitor));
  ASSERT_EQ(0, bucket.size());
  // 测试单list多记录
  ASSERT_EQ(OB_SUCCESS, bucket.insert(1, ObTransID(1), ObTxSEQ(1), share::SCN(), monitor));
  ASSERT_EQ(OB_SUCCESS, bucket.insert(1, ObTransID(1), ObTxSEQ(2), share::SCN(), monitor));
  ASSERT_EQ(5 + init_alloc_time, HashHolderAllocator::ALLOC_TIMES);
  ASSERT_EQ(2, bucket.size());
  ASSERT_EQ(OB_SUCCESS, bucket.get(1, info, monitor));
  ASSERT_EQ(ObTransID(1), info.tx_id_);
  ASSERT_EQ(ObTxSEQ(2), info.seq_);
  ASSERT_EQ(OB_SUCCESS, bucket.erase(1, ObTransID(1), ObTxSEQ(1), monitor));
  ASSERT_EQ(OB_SUCCESS, bucket.erase(1, ObTransID(1), ObTxSEQ(2), monitor));
  ASSERT_EQ(0, bucket.size());
  // 测试多list单记录
  ASSERT_EQ(OB_SUCCESS, bucket.insert(2, ObTransID(2), ObTxSEQ(2), share::SCN(), monitor));
  ASSERT_EQ(7 + init_alloc_time, HashHolderAllocator::ALLOC_TIMES);
  ASSERT_EQ(OB_SUCCESS, bucket.insert(1, ObTransID(1), ObTxSEQ(1), share::SCN(), monitor));
  ASSERT_EQ(9 + init_alloc_time, HashHolderAllocator::ALLOC_TIMES);
  ASSERT_EQ(2, bucket.size());
  ASSERT_EQ(OB_SUCCESS, bucket.erase(1, ObTransID(1), ObTxSEQ(1), monitor));// 此时只剩下一个list
  ASSERT_EQ(1, bucket.size());
  ASSERT_EQ(OB_SUCCESS, bucket.insert(3, ObTransID(3), ObTxSEQ(3), share::SCN(), monitor));
  ASSERT_EQ(11 + init_alloc_time, HashHolderAllocator::ALLOC_TIMES);
  ASSERT_EQ(OB_SUCCESS, bucket.erase(2, ObTransID(2), ObTxSEQ(2), monitor));// 此时只剩下一个list 3
  ASSERT_EQ(1, bucket.size());
  ASSERT_EQ(3, bucket.list_->hash_val_);
  ASSERT_EQ(OB_SUCCESS, bucket.get(3, info, monitor));
  ASSERT_EQ(ObTransID(3), info.tx_id_);
  ASSERT_EQ(ObTxSEQ(3), info.seq_);
  ASSERT_EQ(OB_SUCCESS, bucket.erase(3, ObTransID(3), ObTxSEQ(3), monitor));// 空了
  // 测试多list多记录
  ASSERT_EQ(OB_SUCCESS, bucket.insert(2, ObTransID(2), ObTxSEQ(2), share::SCN(), monitor));
  ASSERT_EQ(13 + init_alloc_time, HashHolderAllocator::ALLOC_TIMES);
  ASSERT_EQ(OB_SUCCESS, bucket.insert(1, ObTransID(1), ObTxSEQ(1), share::SCN(), monitor));
  ASSERT_EQ(15 + init_alloc_time, HashHolderAllocator::ALLOC_TIMES);
  ASSERT_EQ(2, bucket.size());
  ASSERT_EQ(OB_SUCCESS, bucket.insert(2, ObTransID(2), ObTxSEQ(3), share::SCN(), monitor));
  ASSERT_EQ(OB_SUCCESS, bucket.insert(1, ObTransID(1), ObTxSEQ(4), share::SCN(), monitor));
  ASSERT_EQ(4, bucket.size());
  ASSERT_EQ(OB_SUCCESS, bucket.get(2, info, monitor));
  ASSERT_EQ(ObTransID(2), info.tx_id_);
  ASSERT_EQ(ObTxSEQ(3), info.seq_);
  ASSERT_EQ(OB_SUCCESS, bucket.get(1, info, monitor));
  ASSERT_EQ(ObTransID(1), info.tx_id_);
  ASSERT_EQ(ObTxSEQ(4), info.seq_);
}

TEST_F(TestRowHolderMapper, test_dynamic_close_deadlock) {
  map->insert_or_replace_hash_holder(123, ObTransID(1), ObTxSEQ(1), share::SCN());
  ASSERT_EQ(1, map->count());
  map->clear();
  ASSERT_EQ(0, map->count());
  auto seq = transaction::ObTxSEQ::mk_v0(1722849323368309);
  cout << seq.cast_to_int() << endl;
}

}// namespace unittest
}// namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_row_hash_holder.log");
  oceanbase::common::ObClockGenerator::init();
  oceanbase::common::ObLogger &logger = oceanbase::common::ObLogger::get_logger();
  logger.set_file_name("test_row_hash_holder.log", false);
  logger.set_log_level(OB_LOG_LEVEL_DEBUG);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}