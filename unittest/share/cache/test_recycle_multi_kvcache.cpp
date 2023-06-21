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
#include "lib/allocator/ob_allocator.h"
#define UNITTEST_DEBUG
#include <gtest/gtest.h>
#define private public
#define protected public
#include "storage/multi_data_source/runtime_utility/mds_factory.h"
#include "share/ob_ls_id.h"
#include "storage/multi_data_source/mds_writer.h"
#include <thread>
#include <iostream>
#include <vector>
#include <chrono>
#include <exception>
#include "lib/ob_errno.h"
#include "share/ob_errno.h"
#include "storage/multi_data_source/adapter_define/mds_dump_node.h"
#include "lib/allocator/ob_malloc.h"
#include "storage/multi_data_source/mds_node.h"
#include "common/ob_clock_generator.h"
#include "storage/multi_data_source/mds_row.h"
#include "storage/multi_data_source/mds_unit.h"
#include "storage/multi_data_source/mds_table_handle.h"
#include "storage/multi_data_source/mds_table_handler.h"
#include "storage/tx/ob_trans_define.h"
#include <algorithm>
#include <numeric>
#include "storage/multi_data_source/runtime_utility/mds_lock.h"
#include "storage/tablet/ob_tablet_meta.h"
#include "share/cache/ob_recycle_multi_kvcache.h"

namespace oceanbase {
namespace unittest {

struct DefaultAllocator : public ObIAllocator
{
  void *alloc(const int64_t size)  { return ob_malloc(size, "MDS"); }
  void *alloc(const int64_t size, const ObMemAttr &attr) { return ob_malloc(size, attr); }
  void free(void *ptr) { ob_free(ptr); }
  void set_label(const lib::ObLabel &) {}
  static DefaultAllocator &get_instance() { static DefaultAllocator alloc; return alloc; }
  static int64_t get_alloc_times() { return ATOMIC_LOAD(&get_instance().alloc_times_); }
  static int64_t get_free_times() { return ATOMIC_LOAD(&get_instance().free_times_); }
private:
  DefaultAllocator() : alloc_times_(0), free_times_(0) {}
  int64_t alloc_times_;
  int64_t free_times_;
};

using namespace common;
using namespace std;
using namespace storage;
using namespace mds;
using namespace transaction;
using namespace common::cache;

class TestObVtableRecycleEventBuffer: public ::testing::Test
{
public:
  TestObVtableRecycleEventBuffer() {}
  virtual ~TestObVtableRecycleEventBuffer() {}
  virtual void SetUp() {
  }
  virtual void TearDown() {
  }
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestObVtableRecycleEventBuffer);
};

struct TestEvent {
  TestEvent() : buffer_(nullptr), len_(0), alloc_(nullptr) {}
  TestEvent &operator=(const TestEvent &rhs) = delete;
  ~TestEvent() {
    if (OB_NOT_NULL(alloc_)) {
      alloc_->free(buffer_);
      alloc_ = nullptr;
      buffer_ = nullptr;
      len_ = 0;
    }
  }
  int init(ObIAllocator &alloc, int64_t size) {
    int ret = OB_SUCCESS;
    if (nullptr == (buffer_ = (char *)alloc.alloc(size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      OCCAM_LOG(DEBUG, "fail to alloc", K(*this));
    } else {
      alloc_ = &alloc;
      len_ = size;
    }
    return ret;
  }
  int assign(ObIAllocator &alloc, const TestEvent &rhs) {
    return init(alloc, rhs.len_);
  }
  TO_STRING_KV(KP(this), KP_(buffer), KP_(alloc), K_(len));
  char *buffer_;
  int64_t len_;
  ObIAllocator *alloc_;
};

struct HashKey {
  HashKey() : key_(0) {}
  HashKey(int key) : key_(key) {}
  bool operator<(const HashKey &rhs) { return key_ < rhs.key_; }
  bool operator==(const HashKey &rhs) { return key_ == rhs.key_; }
  int64_t hash() const { return key_ % 3; }
  TO_STRING_KV(K_(key));
  int key_;
};

struct Simple {
  Simple() : val_(0) {}
  Simple(int val) : val_(val) {}
  bool operator==(const Simple &rhs) const { return val_ == rhs.val_; }
  TO_STRING_KV(K_(val));
  int val_;
};

struct Complicated {
  Complicated() : data_(nullptr), len_(0), alloc_(nullptr) {}
  ~Complicated() {
    if (OB_NOT_NULL(alloc_)) {
      alloc_->free(data_);
      alloc_ = nullptr;
      len_ = 0;
      alloc_ = 0;
    }
  }
  Complicated(char c, int len) : Complicated() {
    data_ = new char[len];
    for (int i = 0; i < len; ++i) {
      data_[i] = c;
    }
    len_ = len;
  }
  bool operator==(const Complicated &rhs) const {
    bool ret = false;
    if (len_ == rhs.len_) {
      int i = 0;
      for (; i < rhs.len_; ++i) {
        if (data_[i] != rhs.data_[i]) {
          break;
        }
      }
      if (i == len_) {
        ret = true;
      }
    }
    return ret;
  }
  bool operator!=(const Complicated &rhs) const {
    return !this->operator==(rhs);
  }
  int assign(ObIAllocator &alloc, const Complicated &rhs) {
    int ret = OB_SUCCESS;
    if (nullptr == (data_ = (char*)alloc.alloc(rhs.len_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      OCCAM_LOG(DEBUG, "assign new", KP_(data));
      for (int i = 0; i < rhs.len_; ++i) {
        data_[i] = rhs.data_[i];
      }
      len_ = rhs.len_;
      alloc_ = &alloc;
    }
    return ret;
  }
  TO_STRING_KV(KP_(data), K_(len), KP_(alloc));
  char *data_;
  int64_t len_;
  ObIAllocator *alloc_;
};

TEST_F(TestObVtableRecycleEventBuffer, test_hash_bkt) {
  KVNode<HashKey, Simple> *stack_buffer[3];
  ObRecycleMultiKVCache<HashKey, Simple>::HashBkt hash_bkt(stack_buffer, 3, 0);
  auto node1_1 = new KVNode<HashKey, Simple>(1, 1);
  auto node1_2 = new KVNode<HashKey, Simple>(1, 2);
  auto node1_3 = new KVNode<HashKey, Simple>(1, 3);
  auto node2_1 = new KVNode<HashKey, Simple>(2, 1);
  auto node4_1 = new KVNode<HashKey, Simple>(4, 1);
  auto node4_2 = new KVNode<HashKey, Simple>(4, 2);
  auto node4_3 = new KVNode<HashKey, Simple>(4, 3);
  hash_bkt.insert(node1_1);
  ASSERT_EQ(node1_1, hash_bkt.hash_bkt_[1]);
  ASSERT_EQ(nullptr, hash_bkt.hash_bkt_[1]->hash_bkt_next_);
  ASSERT_EQ(node1_1, hash_bkt.hash_bkt_[1]->key_node_next_);
  ASSERT_EQ(node1_1, hash_bkt.hash_bkt_[1]->key_node_prev_);
  hash_bkt.insert(node2_1);
  ASSERT_EQ(node2_1, hash_bkt.hash_bkt_[2]);
  hash_bkt.insert(node4_1);
  ASSERT_EQ(node4_1, hash_bkt.hash_bkt_[1]->hash_bkt_next_);
  ASSERT_EQ(hash_bkt.hash_bkt_[1]->hash_bkt_next_, *node4_1->hash_bkt_prev_next_ptr_);
  ASSERT_EQ(nullptr, hash_bkt.hash_bkt_[1]->hash_bkt_next_->hash_bkt_next_);
  hash_bkt.insert(node1_2);
  ASSERT_EQ(node1_2, hash_bkt.hash_bkt_[1]->key_node_next_);
  ASSERT_EQ(node1_2, hash_bkt.hash_bkt_[1]->key_node_prev_);
  ASSERT_EQ(hash_bkt.hash_bkt_[1], node1_2->key_node_next_);
  ASSERT_EQ(hash_bkt.hash_bkt_[1], node1_2->key_node_prev_);
  hash_bkt.remove(node1_2);
  ASSERT_EQ(node1_1, hash_bkt.hash_bkt_[1]->key_node_next_);
  ASSERT_EQ(node1_1, hash_bkt.hash_bkt_[1]->key_node_prev_);
  hash_bkt.insert(node1_2);
  hash_bkt.remove(node1_1);
  ASSERT_EQ(node1_2, hash_bkt.hash_bkt_[1]);
  ASSERT_EQ(node1_2, hash_bkt.hash_bkt_[1]->key_node_next_);
  ASSERT_EQ(node1_2, hash_bkt.hash_bkt_[1]->key_node_prev_);
  ASSERT_EQ(node4_1, hash_bkt.hash_bkt_[1]->hash_bkt_next_);
  ASSERT_EQ(node1_2->hash_bkt_next_, *node4_1->hash_bkt_prev_next_ptr_);
  hash_bkt.remove(node1_2);
  ASSERT_EQ(node4_1, hash_bkt.hash_bkt_[1]);
  KVNode<HashKey, Simple> **bkt_prev_node_next_ptr;
  KVNode<HashKey, Simple> *bkt_next;
  auto list = hash_bkt.find_list(4, bkt_prev_node_next_ptr, bkt_next);
  ASSERT_EQ(Simple(1), list->v_);
  ASSERT_EQ(list, list->key_node_next_);
  hash_bkt.insert(node4_2);
  hash_bkt.insert(node4_3);
  list = hash_bkt.find_list(4, bkt_prev_node_next_ptr, bkt_next);
  ASSERT_EQ(Simple(1), list->v_);
  ASSERT_EQ(Simple(2), list->key_node_next_->v_);
  ASSERT_EQ(Simple(3), list->key_node_next_->key_node_next_->v_);
  ASSERT_EQ(list, list->key_node_next_->key_node_next_->key_node_next_);
  hash_bkt.remove(node4_2);
  hash_bkt.insert(node4_2);
  ASSERT_EQ(Simple(1), list->v_);
  ASSERT_EQ(Simple(3), list->key_node_next_->v_);
  ASSERT_EQ(Simple(2), list->key_node_next_->key_node_next_->v_);
  ASSERT_EQ(list, list->key_node_next_->key_node_next_->key_node_next_);
}

TEST_F(TestObVtableRecycleEventBuffer, test_recycle_buffer) {
  ObRecycleMultiKVCache<HashKey, Complicated> recycle_buffer;
  int64_t sizeof_event_node = sizeof(KVNode<HashKey, Complicated>);
  ASSERT_EQ(OB_SUCCESS, recycle_buffer.init("TEST",
                                            DefaultAllocator::get_instance(),
                                            3 * (sizeof_event_node + 1),
                                            3));
  ASSERT_EQ(recycle_buffer.total_buffer_ + sizeof(void*) * 3, recycle_buffer.buffer_);
  ASSERT_EQ((char *)recycle_buffer.hash_bkt_.hash_bkt_, recycle_buffer.total_buffer_);
  ASSERT_EQ(recycle_buffer.round_end_(recycle_buffer.offset_next_to_appended_), recycle_buffer.offset_can_write_end_);
  auto value1 = Complicated({'1', 1});
  auto value2 = Complicated({'2', 1});
  auto value3 = Complicated({'3', 1});
  auto value4 = Complicated({'4', 1});
  auto value5 = Complicated({'5', 1});
  auto value6 = Complicated({'6', 2});
  auto value7 = Complicated({'7', 2});
  auto value8 = Complicated({'8', 1});
  auto value9 = Complicated({'9', 1});
  auto value10 = Complicated({'0', 1});
  auto value11 = Complicated({'1', 1_KB});
  ASSERT_EQ(OB_SUCCESS, recycle_buffer.append({1}, value1));
  ASSERT_EQ(sizeof_event_node + 1, recycle_buffer.offset_next_to_appended_);
  ASSERT_EQ(recycle_buffer.round_end_(recycle_buffer.offset_next_to_appended_), recycle_buffer.offset_can_write_end_);
  ASSERT_EQ(OB_SUCCESS, recycle_buffer.append({1}, value2));
  ASSERT_EQ(2 * (sizeof_event_node + 1), recycle_buffer.offset_next_to_appended_);
  ASSERT_EQ(recycle_buffer.round_end_(recycle_buffer.offset_next_to_appended_), recycle_buffer.offset_can_write_end_);
  ASSERT_EQ(OB_SUCCESS, recycle_buffer.append({1}, value3));
  ASSERT_EQ(OB_SUCCESS, recycle_buffer.append({1}, value4));
  ASSERT_EQ(4 * (sizeof_event_node + 1), recycle_buffer.offset_next_to_appended_);
  ASSERT_EQ(recycle_buffer.buffer_len_ + (sizeof_event_node + 1), recycle_buffer.offset_can_write_end_);
  ASSERT_EQ(OB_SUCCESS, recycle_buffer.append({1}, value5));
  ASSERT_EQ(OB_SUCCESS, recycle_buffer.append({1}, value6));
  ASSERT_EQ(recycle_buffer.buffer_len_ * 2 + (sizeof_event_node + 2), recycle_buffer.offset_next_to_appended_);
  ASSERT_EQ(recycle_buffer.buffer_len_ * 2 + 2 * (sizeof_event_node + 1), recycle_buffer.offset_reserved_);
  ASSERT_EQ(recycle_buffer.buffer_len_ * 3, recycle_buffer.offset_can_write_end_);
  int idx = 0;
  int ret = recycle_buffer.for_each(HashKey(1), [&](const Complicated &vale) -> int {
    int ret = OB_SUCCESS;
    switch (++idx) {
    case 1:
      if (value6 != vale) {
        ret = OB_ERR_UNEXPECTED;
        OCCAM_LOG(ERROR, "not expected", K(vale));
      }
      break;
    default:
      OB_ASSERT(false);
      break;
    }
    return ret;
  });
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, recycle_buffer.append({1}, value7));
  ASSERT_EQ(OB_SUCCESS, recycle_buffer.append({1}, value8));
  ASSERT_EQ(recycle_buffer.buffer_len_ * 3 + (sizeof_event_node + 1), recycle_buffer.offset_next_to_appended_);
  ASSERT_EQ(recycle_buffer.buffer_len_ * 3 + 2 * (sizeof_event_node + 2), recycle_buffer.offset_reserved_);
  ASSERT_EQ(recycle_buffer.buffer_len_ * 3 + sizeof_event_node + 2, recycle_buffer.offset_can_write_end_);
  ASSERT_EQ(OB_SUCCESS, recycle_buffer.append({1}, value9));
  ASSERT_EQ(recycle_buffer.buffer_len_ * 3 + 2 * (sizeof_event_node + 1), recycle_buffer.offset_next_to_appended_);
  ASSERT_EQ(recycle_buffer.buffer_len_ * 3 + 2 * (sizeof_event_node + 2), recycle_buffer.offset_reserved_);
  ASSERT_EQ(recycle_buffer.buffer_len_ * 4, recycle_buffer.offset_can_write_end_);
  ASSERT_EQ(OB_SUCCESS, recycle_buffer.append({1}, value10));
  ASSERT_EQ(recycle_buffer.buffer_len_ * 4, recycle_buffer.offset_next_to_appended_);
  ASSERT_EQ(recycle_buffer.buffer_len_ * 3 + 2 * (sizeof_event_node + 2), recycle_buffer.offset_reserved_);
  ASSERT_EQ(recycle_buffer.buffer_len_ * 4, recycle_buffer.offset_can_write_end_);
  idx = 0;
  ret = recycle_buffer.for_each(HashKey(1), [&](const Complicated &vale) -> int {
    int ret = OB_SUCCESS;
    switch (++idx) {
    case 1:
      if (value8 != vale) {
        ret = OB_ERR_UNEXPECTED;
        OCCAM_LOG(ERROR, "not expected", K(vale));
      }
      break;
    case 2:
      if (value9 != vale) {
        ret = OB_ERR_UNEXPECTED;
        OCCAM_LOG(ERROR, "not expected", K(vale));
      }
      break;
    case 3:
      if (value10 != vale) {
        ret = OB_ERR_UNEXPECTED;
        OCCAM_LOG(ERROR, "not expected", K(vale));
      }
      break;
    default:
      OB_ASSERT(false);
      break;
    }
    return ret;
  });
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_BUF_NOT_ENOUGH, recycle_buffer.append({11}, value11));
  ASSERT_EQ(recycle_buffer.buffer_len_ * 4, recycle_buffer.offset_next_to_appended_);
  ASSERT_EQ(recycle_buffer.buffer_len_ * 3 + 2 * (sizeof_event_node + 2), recycle_buffer.offset_reserved_);
  ASSERT_EQ(recycle_buffer.buffer_len_ * 5, recycle_buffer.offset_can_write_end_);
}

}
}

int main(int argc, char **argv)
{
  system("rm -rf test_recycle_multi_kvcache.log");
  oceanbase::common::ObLogger &logger = oceanbase::common::ObLogger::get_logger();
  logger.set_file_name("test_recycle_multi_kvcache.log", false);
  logger.set_log_level(OB_LOG_LEVEL_DEBUG);
  testing::InitGoogleTest(&argc, argv);
  int ret = RUN_ALL_TESTS();
  int64_t alloc_times = oceanbase::unittest::DefaultAllocator::get_alloc_times();
  int64_t free_times = oceanbase::unittest::DefaultAllocator::get_free_times();
  if (alloc_times != free_times) {
    MDS_LOG(ERROR, "memory may leak", K(free_times), K(alloc_times));
    ret = -1;
  } else {
    MDS_LOG(INFO, "all memory released", K(free_times), K(alloc_times));
  }
  return ret;
}
