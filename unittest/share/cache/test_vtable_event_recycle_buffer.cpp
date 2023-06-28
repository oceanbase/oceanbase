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
#include <gtest/gtest.h>
#define private public
#define protected public
#include "lib/allocator/ob_allocator.h"
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
#include "share/cache/ob_vtable_event_recycle_buffer.h"
#include "observer/virtual_table/ob_mds_event_buffer.h"

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

class TestObVtableEventRecycleBuffer: public ::testing::Test
{
public:
  TestObVtableEventRecycleBuffer() {}
  virtual ~TestObVtableEventRecycleBuffer() {}
  virtual void SetUp() {
  }
  virtual void TearDown() {
  }
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestObVtableEventRecycleBuffer);
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

TEST_F(TestObVtableEventRecycleBuffer, basic_test) {
  ObVtableEventRecycleBuffer<HashKey, Complicated> vtable_event_buffer;
  auto value1 = Complicated({'1', 1});
  auto value2 = Complicated({'2', 1});
  auto for_each_dummy_op = [](const Complicated &) { return OB_SUCCESS; };
  ASSERT_EQ(OB_SUCCESS, vtable_event_buffer.init("TEST", DefaultAllocator::get_instance(), 2, 100, 100));
  ASSERT_EQ(OB_SUCCESS, vtable_event_buffer.append({1}, value1));
  ASSERT_NE(HashKey(1).hash(), vtable_event_buffer.buffer_bkt_[1].cache_.hash_bkt_.re_hash_idx_({1}));
  ASSERT_EQ(OB_SUCCESS, vtable_event_buffer.for_each({1}, for_each_dummy_op));
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, vtable_event_buffer.for_each({0}, for_each_dummy_op));
  ASSERT_EQ(OB_SUCCESS, vtable_event_buffer.append({0}, value2));
  ASSERT_EQ(OB_SUCCESS, vtable_event_buffer.buffer_bkt_[1].cache_.for_each({1}, for_each_dummy_op));
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, vtable_event_buffer.buffer_bkt_[1].cache_.for_each({0}, for_each_dummy_op));
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, vtable_event_buffer.buffer_bkt_[0].cache_.for_each({1}, for_each_dummy_op));
  ASSERT_EQ(OB_SUCCESS, vtable_event_buffer.buffer_bkt_[0].cache_.for_each({0}, for_each_dummy_op));
}

// TEST_F(TestObVtableEventRecycleBuffer, test_global_event_buffer) {
//   ASSERT_EQ(OB_SUCCESS, observer::ObMdsEventBuffer::init());
//   observer::MdsEvent e;
//   ASSERT_EQ(OB_SUCCESS, e.set_event(DefaultAllocator::get_instance(), "TEST EVENT", ObString("test info str")));
//   observer::ObMdsEventBuffer::append({1, share::ObLSID(1), ObTabletID(1)}, e);
//   observer::ObMdsEventBuffer::destroy();
// }

}
}

int main(int argc, char **argv)
{
  system("rm -rf test_vtable_event_recycle_buffer.log");
  oceanbase::common::ObLogger &logger = oceanbase::common::ObLogger::get_logger();
  logger.set_file_name("test_vtable_event_recycle_buffer.log", false);
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
