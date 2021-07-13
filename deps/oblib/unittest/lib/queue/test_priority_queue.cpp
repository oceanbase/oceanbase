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
#include "lib/allocator/ob_malloc.h"
#include "lib/queue/ob_priority_queue.h"
#include "lib/coro/co.h"
#include "lib/thread/thread_pool.h"

using namespace oceanbase::lib;
using namespace oceanbase::common;

class TestQueue : public ThreadPool {
public:
  enum { BATCH = 64 };
  struct QData : public ObLink {
    QData() : val_(0)
    {}
    QData(int64_t x) : val_(x)
    {}
    ~QData()
    {}
    int64_t val_;
  };
  typedef ObPriorityQueue<3> Queue;
  TestQueue() : seq_(0)
  {
    limit_ = atoll(getenv("limit") ?: "1000000");
  }
  virtual ~TestQueue()
  {}
  void do_stress()
  {
    set_thread_count(atoi(getenv("n_thread") ?: "8"));
    int ret = OB_SUCCESS;
    if (OB_FAIL(start())) {
      LIB_LOG(ERROR, "start fail", K(ret), K(errno));
      exit(-1);
    }
    print();
    wait();
  }
  void print()
  {
    int64_t last_seq = ATOMIC_LOAD(&seq_);
    while (ATOMIC_LOAD(&seq_) < limit_) {
      sleep(1);
      int64_t cur_seq = ATOMIC_LOAD(&seq_);
      LIB_LOG(INFO, "queue", "tps", BATCH * (cur_seq - last_seq));
      last_seq = cur_seq;
    }
  }
  int64_t get_seq()
  {
    return ATOMIC_FAA(&seq_, 1);
  }
  int insert(int64_t seq)
  {
    int err = 0;
    QData* data = new QData(seq);
    err = queue_.push(data, (int)data->val_ % 3);
    return err;
  }
  int del(int64_t seq)
  {
    UNUSED(seq);
    int err;
    QData* data = NULL;
    err = queue_.pop((ObLink*&)data, 500);
    delete data;
    return err;
  }
  void run1() override
  {
    int ret = OB_SUCCESS;
    int64_t seq = 0;
    const uint64_t idx = get_thread_idx();
    while ((seq = get_seq()) < limit_) {
      if (0 == (idx % 2)) {
        for (int i = 0; i < BATCH; i++) {
          do {
            ret = insert(i);
          } while (OB_FAIL(ret));
        }
      } else {
        for (int i = 0; i < BATCH; i++) {
          do {
            ret = del(i);
          } while (OB_FAIL(ret));
        }
      }
    }
  }

private:
  int64_t seq_ CACHE_ALIGNED;
  int64_t limit_;
  Queue queue_;
};  // end of class Consumer

TEST(TestPriorityQueue, WithCoro)
{
  TestQueue tq;
  tq.do_stress();
}

int main(int argc, char* argv[])
{
  oceanbase::common::ObLogger::get_logger().set_log_level("debug");
  testing::InitGoogleTest(&argc, argv);
  // testing::FLAGS_gtest_filter = "DO_NOT_RUN";
  return RUN_ALL_TESTS();
}
