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
#include "lib/container/ob_ring_array.h"
#include "lib/allocator/ob_concurrent_fifo_allocator.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/thread/thread_pool.h"

using namespace oceanbase::common;
using oceanbase::lib::ThreadPool;

class TestRingBuffer: public ::testing::Test, public ThreadPool
{
public:
  struct RBData
  {
    RBData(): valid_(0), val_(0) {}
    RBData(int64_t val): valid_(1), val_(val) {}
    ~RBData() {}
    int64_t valid_;
    int64_t val_;
    int read_from(RBData* that) {
      int err = 0;
      if (that->valid_ == 0) {
        err = -ENOENT;
      } else {
        *this = *that;
      }
      return err;
    }
    int write_to(RBData* that) {
      int err = 0;
      if (valid_ == 0) {
        err = -EINVAL;
        //error("RBData invalid");
      } else {
        *that = *this;
      }
      return err;
    }
  };
  typedef ObRingArray<RBData> RingArray;
  TestRingBuffer(): seq_(0) {
    set_thread_count(8);
    allocator_.init(4 * 1024 * 1024, 4 * 1024 * 1024, OB_MALLOC_NORMAL_BLOCK_SIZE);
    assert(0 == rbuffer_.init(0, &allocator_));
    limit_ = atoll(getenv("limit")?: "1000000");
  }
  virtual ~TestRingBuffer() {}
  virtual void SetUp() {}
  virtual void TearDown() {}
  void do_stress()
  {
    start();
    int64_t last_seq = ATOMIC_LOAD(&seq_);
    while(ATOMIC_LOAD(&seq_) < limit_) {
      sleep(1);
      int64_t cur_seq = ATOMIC_LOAD(&seq_);
      LIB_LOG(INFO, "ring_buffer", "tps", cur_seq - last_seq);
      last_seq = cur_seq;
    }
    wait();
  }
  int64_t get_seq() {
    return ATOMIC_FAA(&seq_, 1);
  }
  int insert(int64_t seq) {
    int err = 0;
    RBData data(seq);
    if (0 != (err = rbuffer_.set(seq, data))) {
      //error("rbuffer.set fail: seq=%ld err=%d", seq, err);
    }
    return err;
  }
  int del(int64_t seq) {
    return rbuffer_.truncate(seq);
  }

  int mix() {
    int err = 0;
    int64_t stable_size = 1000000;
    int64_t seq = 0;
    while((seq = get_seq()) < limit_) {
      if (0 != (err = insert(seq))) {
        //error("insert fail: err=%d seq=%ld", err, seq);
      }
      if (seq > stable_size
          && (0 != (err = del(seq - stable_size)))) {
        //error("del fail: err=%d seq=%ld", err, seq - stable_size);
      }
    }
    return err;
  }
  void run1() final {
    mix();
  }
private:
  int64_t seq_ CACHE_ALIGNED;
  int64_t limit_;
  RingArray rbuffer_;
  ObConcurrentFIFOAllocator allocator_;
}; // end of class Consumer

TEST_F(TestRingBuffer, stress)
{
  do_stress();
}

int main(int argc, char *argv[])
{
  oceanbase::common::ObLogger::get_logger().set_log_level("debug");
  testing::InitGoogleTest(&argc,argv);
  //testing::FLAGS_gtest_filter = "DO_NOT_RUN";
  return RUN_ALL_TESTS();
}
