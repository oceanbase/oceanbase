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
#include "lib/allocator/ob_malloc.h"
#include "lib/objectpool/ob_concurrency_objpool.h"

using namespace oceanbase;
using namespace common;

#define obj_alloc(type) op_alloc(type)
#define obj_free(ptr) op_free(ptr)

#define obj_tc_alloc(type) op_tc_alloc(type)
#define obj_tc_free(ptr) op_tc_free(ptr)

#define obj_reclaim_alloc(type) op_reclaim_alloc(type)
#define obj_reclaim_free(ptr) op_reclaim_free(ptr)

template <int64_t size>
struct FixedMemStruct {
  char data_[size];
};
#define fixed_mem_alloc(size) obj_tc_alloc(FixedMemStruct<size>)
#define fixed_mem_free(ptr, size) obj_tc_free((FixedMemStruct<size>*)ptr)

struct TestObject1 {
  static const int64_t OP_LOCAL_NUM = 1280000;

  TestObject1() : next_(NULL)
  {}
  void* next_;
  char data_[8];
};

struct TestObject2 : public TestObject1 {
  char other_[16];
};

struct TestObject3 : public TestObject1 {
  char other_[16];
};

struct TestObject4 : public TestObject1 {
  char other_[32];
};

struct TestObject5 : public TestObject1 {
  char other_[48];
};

struct TestStat {
  TestStat()
  {
    memset(this, 0, sizeof(TestStat));
  }
  volatile int64_t global_alloc_count_;
  volatile int64_t global_alloc_time_;
  volatile int64_t global_free_count_;
  volatile int64_t global_free_time_;

  volatile int64_t tc_alloc_count_;
  volatile int64_t tc_alloc_time_;
  volatile int64_t tc_free_count_;
  volatile int64_t tc_free_time_;

  volatile int64_t reclaim_alloc_count_;
  volatile int64_t reclaim_alloc_time_;
  volatile int64_t reclaim_free_count_;
  volatile int64_t reclaim_free_time_;
};

TestStat perf_stat;
TestStat single_thread_perf_stat;
volatile int64_t prepared = 0;
volatile int64_t prepared1 = 0;
volatile int64_t prepared2 = 0;
volatile int64_t prepared3 = 0;
volatile int64_t prepared4 = 0;
static const int64_t THREAD_COUNT = 15;

void test_fixed_mem_op()
{
  void* ptr = fixed_mem_alloc(2048);
  OB_ASSERT(NULL != ptr);
  fixed_mem_free(ptr, 2048);
}

void test_global_op()
{
  TestObject1* obj = NULL;
  obj = obj_alloc(TestObject1);
  OB_ASSERT(NULL != obj);
  obj_free(obj);

  ObObjFreeListList::get_freelists().dump();
  ObObjFreeListList::get_freelists().snap_baseline();
}

void test_tc_op()
{
  TestObject2* obj2 = NULL;
  obj2 = obj_tc_alloc(TestObject2);
  OB_ASSERT(NULL != obj2);
  obj_tc_free(obj2);

  // reuse freelist of TestObject2
  TestObject3* obj3 = obj_tc_alloc(TestObject3);
  OB_ASSERT(NULL != obj3);
  obj_tc_free(obj3);

  // double free
  // obj_tc_free(obj3);

  TestObject4* obj4 = obj_reclaim_alloc(TestObject4);
  OB_ASSERT(NULL != obj4);
  obj_reclaim_free(obj4);
  // double free
  // obj_reclaim_free(obj4);

  ObObjFreeListList::get_freelists().dump();
  ObObjFreeListList::get_freelists().dump_baselinerel();
}

enum AllocType { GLOBAL, TC, RECLAIM };

template <class T>
void alloc_objs(T& head, int64_t count, AllocType type = TC)
{
  T* obj = NULL;

  for (int64_t i = 0; i < count; i++) {
    if (GLOBAL == type) {
      obj = obj_alloc(T);
    } else if (TC == type) {
      obj = obj_tc_alloc(T);
    } else {
      obj = obj_reclaim_alloc(T);
    }
    OB_ASSERT(NULL != obj);
    obj->next_ = head.next_;
    head.next_ = obj;
  }
}

template <class T>
void free_objs(T& head, int64_t count, AllocType type = TC)
{
  T* obj = NULL;
  for (int64_t i = 0; i < count; i++) {
    obj = (T*)head.next_;
    if (NULL == obj) {
      break;
    }
    head.next_ = obj->next_;
    if (GLOBAL == type) {
      obj_free(obj);
    } else if (TC == type) {
      obj_tc_free(obj);
    } else {
      obj_reclaim_free(obj);
    }
  }
}

void test_reclaim()
{
  TestObject4 head4;
  alloc_objs<TestObject4>(head4, 10000, RECLAIM);
  free_objs<TestObject4>(head4, 10000, RECLAIM);
  ObObjFreeListList::get_freelists().dump();

  TestObject5 head5;
  alloc_objs<TestObject5>(head5, 40000, RECLAIM);
  free_objs<TestObject5>(head5, 40000, RECLAIM);
  ObObjFreeListList::get_freelists().dump();
}

void* thread_func(void* arg)
{
  UNUSED(arg);
  int64_t index = *(int64_t*)arg;

  TestObject1 head1;
  TestObject2 head2;
  TestObject3 head3;
  TestObject4 head4;

  for (int64_t i = 0; i < 100; i++) {
    int64_t total_num = random() % 100000L;
    int64_t op = random() % 8;
    switch (op) {
      case 0:
        alloc_objs<TestObject1>(head1, random() % total_num, GLOBAL);
        break;
      case 1:
        alloc_objs<TestObject2>(head2, random() % total_num);
        break;
      case 2:
        alloc_objs<TestObject3>(head3, random() % total_num);
        break;
      case 3:
        alloc_objs<TestObject4>(head4, random() % total_num, RECLAIM);
        break;
      case 4:
        free_objs<TestObject1>(head1, random() % total_num, GLOBAL);
        break;
      case 5:
        free_objs<TestObject2>(head2, random() % total_num);
        break;
      case 6:
        free_objs<TestObject3>(head3, random() % total_num);
        break;
      case 7:
        free_objs<TestObject4>(head4, random() % total_num, RECLAIM);
        break;
      default:
        break;
    }

    if (0 == (i + 1) % 100) {
      printf("thread %ld run 100 obj alloc/free times\n", index);
      ObObjFreeListList::get_freelists().dump();
    }
  }

  free_objs<TestObject1>(head1, INT64_MAX, GLOBAL);
  free_objs<TestObject2>(head2, INT64_MAX);
  free_objs<TestObject3>(head3, INT64_MAX);
  free_objs<TestObject4>(head4, INT64_MAX, RECLAIM);

  int64_t alloc_count = 1000000L;
  alloc_objs<TestObject1>(head1, alloc_count, GLOBAL);
  alloc_objs<TestObject2>(head2, alloc_count, TC);
  alloc_objs<TestObject4>(head4, alloc_count, RECLAIM);

  ATOMIC_FAA(&prepared, 1);
  while (0 != prepared % THREAD_COUNT) {
    this_routine::usleep(10000);
  }

  free_objs<TestObject1>(head1, INT64_MAX, GLOBAL);
  free_objs<TestObject2>(head2, INT64_MAX, TC);
  free_objs<TestObject4>(head4, INT64_MAX, RECLAIM);

  ATOMIC_FAA(&prepared, 1);
  while (0 != prepared % THREAD_COUNT) {
    this_routine::usleep(10000);
  }

  // int64_t alloc_count = 1000000L;
  int64_t start_time = ::oceanbase::common::ObTimeUtility::current_time();
  alloc_objs<TestObject1>(head1, alloc_count, GLOBAL);
  int64_t end_time = ::oceanbase::common::ObTimeUtility::current_time();
  ATOMIC_FAA(&perf_stat.global_alloc_count_, alloc_count);
  ATOMIC_FAA(&perf_stat.global_alloc_time_, end_time - start_time);

  ATOMIC_FAA(&prepared, 1);
  while (0 != prepared % THREAD_COUNT) {
    this_routine::usleep(10000);
  }

  start_time = ::oceanbase::common::ObTimeUtility::current_time();
  free_objs<TestObject1>(head1, INT64_MAX, GLOBAL);
  end_time = ::oceanbase::common::ObTimeUtility::current_time();
  ATOMIC_FAA(&perf_stat.global_free_count_, alloc_count);
  ATOMIC_FAA(&perf_stat.global_free_time_, end_time - start_time);

  ATOMIC_FAA(&prepared, 1);
  while (0 != prepared % THREAD_COUNT) {
    this_routine::usleep(10000);
  }

  start_time = ::oceanbase::common::ObTimeUtility::current_time();
  alloc_objs<TestObject2>(head2, alloc_count);
  end_time = ::oceanbase::common::ObTimeUtility::current_time();
  ATOMIC_FAA(&perf_stat.tc_alloc_count_, alloc_count);
  ATOMIC_FAA(&perf_stat.tc_alloc_time_, end_time - start_time);

  ATOMIC_FAA(&prepared, 1);
  while (0 != prepared % THREAD_COUNT) {
    this_routine::usleep(10000);
  }

  start_time = ::oceanbase::common::ObTimeUtility::current_time();
  free_objs<TestObject2>(head2, INT64_MAX, TC);
  end_time = ::oceanbase::common::ObTimeUtility::current_time();
  ATOMIC_FAA(&perf_stat.tc_free_count_, alloc_count);
  ATOMIC_FAA(&perf_stat.tc_free_time_, end_time - start_time);

  ATOMIC_FAA(&prepared, 1);
  while (0 != prepared % THREAD_COUNT) {
    this_routine::usleep(10000);
  }

  start_time = ::oceanbase::common::ObTimeUtility::current_time();
  alloc_objs<TestObject4>(head4, alloc_count, RECLAIM);
  end_time = ::oceanbase::common::ObTimeUtility::current_time();
  ATOMIC_FAA(&perf_stat.reclaim_alloc_count_, alloc_count);
  ATOMIC_FAA(&perf_stat.reclaim_alloc_time_, end_time - start_time);

  ATOMIC_FAA(&prepared, 1);
  while (0 != prepared % THREAD_COUNT) {
    this_routine::usleep(10000);
  }

  start_time = ::oceanbase::common::ObTimeUtility::current_time();
  free_objs<TestObject4>(head4, INT64_MAX, RECLAIM);
  end_time = ::oceanbase::common::ObTimeUtility::current_time();
  ATOMIC_FAA(&perf_stat.reclaim_free_count_, alloc_count);
  ATOMIC_FAA(&perf_stat.reclaim_free_time_, end_time - start_time);

  return NULL;
}

void single_thread_perf()
{
  TestObject1 head1;
  TestObject2 head2;
  TestObject4 head4;
  int64_t alloc_count = 1000000L;

  int64_t start_time = ::oceanbase::common::ObTimeUtility::current_time();
  alloc_objs<TestObject1>(head1, alloc_count, GLOBAL);
  int64_t end_time = ::oceanbase::common::ObTimeUtility::current_time();
  ATOMIC_FAA(&single_thread_perf_stat.global_alloc_count_, alloc_count);
  ATOMIC_FAA(&single_thread_perf_stat.global_alloc_time_, end_time - start_time);

  start_time = ::oceanbase::common::ObTimeUtility::current_time();
  free_objs<TestObject1>(head1, INT64_MAX, GLOBAL);
  end_time = ::oceanbase::common::ObTimeUtility::current_time();
  ATOMIC_FAA(&single_thread_perf_stat.global_free_count_, alloc_count);
  ATOMIC_FAA(&single_thread_perf_stat.global_free_time_, end_time - start_time);

  start_time = ::oceanbase::common::ObTimeUtility::current_time();
  alloc_objs<TestObject2>(head2, alloc_count);
  end_time = ::oceanbase::common::ObTimeUtility::current_time();
  ATOMIC_FAA(&single_thread_perf_stat.tc_alloc_count_, alloc_count);
  ATOMIC_FAA(&single_thread_perf_stat.tc_alloc_time_, end_time - start_time);

  start_time = ::oceanbase::common::ObTimeUtility::current_time();
  free_objs<TestObject2>(head2, INT64_MAX, TC);
  end_time = ::oceanbase::common::ObTimeUtility::current_time();
  ATOMIC_FAA(&single_thread_perf_stat.tc_free_count_, alloc_count);
  ATOMIC_FAA(&single_thread_perf_stat.tc_free_time_, end_time - start_time);

  start_time = ::oceanbase::common::ObTimeUtility::current_time();
  alloc_objs<TestObject4>(head4, alloc_count, RECLAIM);
  end_time = ::oceanbase::common::ObTimeUtility::current_time();
  ATOMIC_FAA(&single_thread_perf_stat.reclaim_alloc_count_, alloc_count);
  ATOMIC_FAA(&single_thread_perf_stat.reclaim_alloc_time_, end_time - start_time);

  start_time = ::oceanbase::common::ObTimeUtility::current_time();
  free_objs<TestObject4>(head4, INT64_MAX, RECLAIM);
  end_time = ::oceanbase::common::ObTimeUtility::current_time();
  ATOMIC_FAA(&single_thread_perf_stat.reclaim_free_count_, alloc_count);
  ATOMIC_FAA(&single_thread_perf_stat.reclaim_free_time_, end_time - start_time);
}

void thread_run()
{
  pthread_t id[THREAD_COUNT];
  srandom((uint32_t)time(NULL));

  printf("start multi-thread concurrency test\n");
  for (int64_t i = 0; i < THREAD_COUNT; i++) {
    if (0 != pthread_create(&id[i], NULL, thread_func, &i)) {
      printf("create thread error\n");
    }
  }

  void* ret = NULL;
  for (int64_t i = 0; i < THREAD_COUNT; i++) {
    pthread_join(id[i], &ret);
  }

  single_thread_perf();

  ObObjFreeListList::get_freelists().dump();
  printf("%15s %15s %15s %15s %15s %15s %15s\n", "type", "gp", "sgp", "tp", "stp", "rp", "srp");
  printf("%15s %15ld %15ld %15ld %15ld %15ld %15ld\n",
      "alloc",
      perf_stat.global_alloc_count_ * 1000L * 1000L / perf_stat.global_alloc_time_,
      single_thread_perf_stat.global_alloc_count_ * 1000L * 1000L / single_thread_perf_stat.global_alloc_time_,
      perf_stat.tc_alloc_count_ * 1000L * 1000L / perf_stat.tc_alloc_time_,
      single_thread_perf_stat.tc_alloc_count_ * 1000L * 1000L / single_thread_perf_stat.tc_alloc_time_,
      perf_stat.reclaim_alloc_count_ * 1000L * 1000L / perf_stat.reclaim_alloc_time_,
      single_thread_perf_stat.reclaim_alloc_count_ * 1000L * 1000L / single_thread_perf_stat.reclaim_alloc_time_);
  printf("%15s %15ld %15ld %15ld %15ld %15ld %15ld\n",
      "free",
      perf_stat.global_free_count_ * 1000L * 1000L / perf_stat.global_free_time_,
      single_thread_perf_stat.global_free_count_ * 1000L * 1000L / single_thread_perf_stat.global_free_time_,
      perf_stat.tc_free_count_ * 1000L * 1000L / perf_stat.tc_free_time_,
      single_thread_perf_stat.tc_free_count_ * 1000L * 1000L / single_thread_perf_stat.tc_free_time_,
      perf_stat.reclaim_free_count_ * 1000L * 1000L / perf_stat.reclaim_free_time_,
      single_thread_perf_stat.reclaim_free_count_ * 1000L * 1000L / single_thread_perf_stat.reclaim_free_time_);
}

int main(int argc, char** argv)
{
  UNUSED(argc);
  UNUSED(argv);

  test_fixed_mem_op();
  test_global_op();
  test_tc_op();
  test_reclaim();

  thread_run();

  return 0;
}
