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
#include "lib/allocator/ob_malloc.h"
#include "lib/allocator/ob_small_allocator.h"
#include "lib/queue/ob_fixed_queue.h"
#include <stdlib.h>
#include <sys/time.h>
#include <ctime>
#include "lib/utility/ob_print_utils.h"
#include "lib/coro/testing.h"

using namespace oceanbase;
using namespace common;

/* Simple stopwatch */
class StopWatch {
public:
  StopWatch () : run_(false) {}
  void start () { run_ = true; gettimeofday(&S_, NULL); }
  void stop () { run_ = false; gettimeofday(&E_, NULL); }
  double tspan_sec ()
  {
    timeval T = E_; if (run_) { gettimeofday(&T, NULL); }
    if (T.tv_usec < S_.tv_usec) { T.tv_sec -= 1; T.tv_usec += 1000000; }
    return (double)((int)(T.tv_sec - S_.tv_sec) + (double)((int)((T.tv_usec - S_.tv_usec) / 1000)) / 1000);
  }
  long int tspan_usec ()
  {
    timeval T = E_; if (run_) { gettimeofday(&T, NULL); }
    if (T.tv_usec < S_.tv_usec) { T.tv_sec -= 1; T.tv_usec += 1000000; }
    return (long int)(T.tv_sec - S_.tv_sec) * 1000000 + (long int)(T.tv_usec - S_.tv_usec);
  }
private:
  bool run_;
  timeval S_, E_;
};

/* Test data */
struct TestData {
  volatile bool *run_;
  ObSmallAllocator *alloc_;
  int64_t hold_n_;
  double tspan_;
  int64_t obj_size_;
  volatile int64_t count_;
};

/* Test function */
void* thread_func (void *data)
{
  OB_ASSERT(data);
  TestData *td = (TestData*)(data);
  char **buf = new char*[td->hold_n_];
  StopWatch sw;

  sw.start();
  for (int64_t buf_i = 0; buf_i < td->hold_n_; ++buf_i)
  {
    buf[buf_i] = (char*)td->alloc_->alloc();
    EXPECT_NE((char*)NULL, buf[buf_i]);
    memset(buf[buf_i], 0, td->obj_size_);
  }
  td->count_ += td->hold_n_;
  while(*td->run_)
  {
    for (int64_t buf_i = 0; buf_i < td->hold_n_; ++buf_i)
    {
      td->alloc_->free(buf[buf_i]);
      buf[buf_i] = (char*)td->alloc_->alloc();
      EXPECT_NE((char*)NULL, buf[buf_i]);
    }
    td->count_ += td->hold_n_;
  }
  for (int64_t buf_i = 0; buf_i < td->hold_n_; ++buf_i)
  {
    td->alloc_->free(buf[buf_i]);
  }
  td->tspan_ = sw.tspan_sec();
  return NULL;
}

void test_func (ObSmallAllocator &alloc, int64_t obj_size, int64_t blk_size,
    int64_t hold_n, double testtime, int64_t thread_n, bool verbose = false)
{
  TestData *data = new TestData[thread_n];
  volatile bool run = true;
  StopWatch sw;
  int64_t last_log_time = 0;
  double total_tspan = 0.0;
  double avg_tspan  = 0.0;
  int64_t total_count = 0;
  time_t now;
  auto thread_idx = 0;

  ASSERT_EQ(0, alloc.init(obj_size, 0, OB_SERVER_TENANT_ID, blk_size));
  sw.start();

  cotesting::FlexPool pool(
      [&]() {
        TestData &data_t = data[ATOMIC_FAA(&thread_idx, 1)];
        data_t.run_ = &run;
        data_t.alloc_ = &alloc;
        data_t.hold_n_ = hold_n;
        data_t.count_ = 0;
        data_t.obj_size_ = obj_size;
        data_t.tspan_ = 0;
        thread_func(&data_t);
      },
      (int)thread_n
  );
  pool.start(false);

  while (testtime > sw.tspan_sec())
  {
    ::usleep(500 * 1000);
    if (verbose && 1000000 * 5 < sw.tspan_usec() - last_log_time)
    {
      last_log_time = sw.tspan_usec();
      now = time(0);
      fprintf(stderr, "%s%s\n", ctime(&now), to_cstring(alloc));
    }
  }
  run = false;
  pool.wait();

  // Print perf data.
  for (int64_t th_i = 0; th_i < thread_n; ++th_i)
  {
    total_tspan += data[th_i].tspan_;
    total_count += data[th_i].count_;
  }
  avg_tspan = total_tspan / (double)thread_n;
  fprintf(stdout, "th[%ld] avgTspan[%fsec] size[%ldB] block[%ldK] "
        "hold[%ld] perf[%ldk/sec]\n",
        thread_n, avg_tspan, obj_size, blk_size / 1024,  hold_n,
        (long int)((double)total_count / avg_tspan) / 1000);

  ASSERT_EQ(0, alloc.destroy());
  delete[] data;
}

/* Perf Tests */
TEST(ObSmallAllocator, PerfTest)
{
  const int64_t normal_blk = 64 * 1024;
  const int64_t large_blk = 2 * 1024 * 1024;
  const int64_t th_n = 3;
  const int64_t th[th_n] = { 1, 4, 16 };
  const int64_t size_n = 3;
  const int64_t sizes[size_n] = { 32, 256, 1024 };
  const int64_t blk_n = 2;
  const int64_t blk[blk_n] = { normal_blk, large_blk };
  const double test_time = 1;
  int64_t hold_n = 32; // objects alloc() - free() at a moment
  ObSmallAllocator alloc;

  for (int64_t th_i = 0; th_i < th_n; ++th_i)
  {
    for (int64_t size_i = 0; size_i < size_n; ++size_i)
    {
      for (int64_t blk_i = 0; blk_i < blk_n; ++blk_i)
      {
        // Make sure a thread uses at least 3 blocks at any time.
        hold_n = (3 * blk[blk_i]) / sizes[size_i];
        test_func(alloc, sizes[size_i], blk[blk_i], hold_n, test_time, th[th_i], true);
      }
    }
  }

}

/* Cross threads Tests */
/* Tests locks and other multi-thread things. */
/* Alloc from one thread and free by another thread */
ObSmallAllocator m2;
const int64_t qn = 6;
ObFixedQueue<char> qs[qn];
int64_t alloc_n = 1000;// * 1000 * 10;
void* run_cross_threads_alloc (int index)
{
  char *buf;
  for (int64_t i = 0; i < alloc_n; ++i)
  {
    // Alloc and push in current queue.
    EXPECT_NE((char*)NULL, buf = (char*)m2.alloc());
    while (0 != qs[index].push(buf));
    // Slow down
    if ((rand() % 19) == 1)
      ::usleep(1);
    // Print log
    if (i % (10000 * 100) == 0)
    {
      time_t now = time(0);
      fprintf(stderr, "%s%s\n", ctime(&now), to_cstring(m2));
    }
  }
  return NULL;
}
void* run_cross_threads_free (int index)
{
  char *buf;
  for (int64_t i = 0; i < alloc_n; ++i)
  {
    while (0 != qs[(index + 1) % qn].pop(buf))
      m2.free(buf);
    // Slow down
    if ((rand() % 20) == 1)
      ::usleep(1);
  }
  return NULL;
}

void cross_threads ()
{
  const int64_t size = 1024;
  for (int64_t qi = 0; qi < qn; ++qi)
  {
    qs[qi].init(alloc_n);
  }
  EXPECT_EQ(0, m2.init(size));
  auto idx1 = 0;
  auto pool1 = cotesting::FlexPool([&](){
    run_cross_threads_alloc(ATOMIC_FAA(&idx1, 1));
  }, qn);
  pool1.start(false);
  auto idx2 = 0;
  auto pool2 = cotesting::FlexPool([&](){
    run_cross_threads_free(ATOMIC_FAA(&idx2, 1));
  }, qn);
  pool2.start(false);
  pool1.wait();
  pool2.wait();
  EXPECT_EQ(0, m2.destroy());
  for (int64_t qi = 0; qi < qn; ++qi)
  {
    qs[qi].destroy();
  }
}

TEST(ObSmallAllocator, CrossThreads)
{
  cross_threads();
}

/* Stress Tests */
TEST(DISABLED_ObSmallAllocator, StressTest)
{
  const double test_time = 60 * 15;
  ObSmallAllocator alloc;

  while (true)
  {
    test_func(alloc, 64, 64 * 1024, 1024 * 3, test_time, 8, true);
    cross_threads();
  }

}

int main (int argc, char **argv)
{
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
