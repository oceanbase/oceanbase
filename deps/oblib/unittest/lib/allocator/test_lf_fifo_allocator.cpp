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
#include "gtest/gtest.h"
#include "lib/allocator/ob_lf_fifo_allocator.h"
#include "lib/allocator/ob_mod_define.h"
#include "lib/hash/ob_concurrent_hash_map.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/coro/testing.h"

using namespace oceanbase;
using namespace oceanbase::common;

static const int64_t PAGE_SIZE = 64 * 1024;

class TestC
{
public:
  TestC() {}
  virtual ~TestC() {}
public:
  // a successful virtual function invoking represents a good vtable
  virtual void set_mem_a(int64_t value) { mem_a_ = value; }
  virtual int64_t get_mem_a() { return mem_a_; }
  virtual void set_mem_b(int64_t value) { mem_b_ = value; }
  virtual int64_t get_mem_b() { return mem_b_; }
private:
  int64_t mem_a_;
  int64_t mem_b_;
};

TEST(TestLfFIFOAllocator, single_thread)
{
  LIB_LOG(INFO, "start single thread test");
  static const int64_t loop = 4096;
  TestC *ptr_buffer[loop];
  ObLfFIFOAllocator allocator;
  ASSERT_EQ(OB_SUCCESS, allocator.init(PAGE_SIZE, ObModIds::OB_PARTITION_STORAGE));
  for (int64_t i = 0; i < loop; ++i) {
    ptr_buffer[i] = NULL;
  }
  for (int64_t i = 0; i < loop; ++i) {
    void *ptr = allocator.alloc(sizeof(TestC));
    ASSERT_TRUE(NULL != ptr);
    TestC *test_c = new (ptr) TestC();
    test_c->set_mem_a(i);
    test_c->set_mem_b(i);
    ptr_buffer[i] = test_c;
  }
  for (int64_t i = 0; i < loop; ++i) {
    ptr_buffer[i]->~TestC();
    allocator.free(ptr_buffer[i]);
    ptr_buffer[i] = NULL;
  }
}

TEST(TestLfFIFOAllocator, single_thread2)
{
  LIB_LOG(INFO, "start single thread test2");
  static const int64_t MALLOC_PER_LOOP = 1024;
  static const int64_t LOOP = 32 * 512;
  void *ptr_buffer[MALLOC_PER_LOOP];
  ObLfFIFOAllocator allocator;
  ASSERT_EQ(OB_SUCCESS, allocator.init(PAGE_SIZE, ObModIds::OB_PARTITION_STORAGE));
  for (int64_t i = 0; i < MALLOC_PER_LOOP; ++i) {
    ptr_buffer[i] = NULL;
  }
  for (int64_t loop = 0; loop < LOOP; ++loop) {
    for (int64_t i = 0; i < MALLOC_PER_LOOP; ++i) {
      void *ptr = allocator.alloc(sizeof(TestC));
      ASSERT_TRUE(NULL != ptr);
      ptr_buffer[i] = ptr;
    }
    for (int64_t i = 0; i < MALLOC_PER_LOOP; ++i) {
      allocator.free(ptr_buffer[i]);
      ptr_buffer[i] = NULL;
    }
  }
}

ObLfFIFOAllocator allocator1;
pthread_barrier_t barrier1;
//typedef common::ObConcurrentHashMap<int64_t, int64_t> PointerContainer;
typedef common::hash::ObHashMap<int64_t, int64_t> PointerContainer;
PointerContainer pc;

void *th_direct_alloc_func(void *arg)
{
  UNUSED(arg);
  static const int64_t MALLOC_TIMES_PER_THREAD = 1024;
  void *ptr_buffer[MALLOC_TIMES_PER_THREAD];
  for (int64_t i = 0; i < MALLOC_TIMES_PER_THREAD; ++i) {
    ptr_buffer[i] = NULL;;
  }
  pthread_barrier_wait(&barrier1);
  for (int64_t times = 0; times < MALLOC_TIMES_PER_THREAD; ++times) {
    void *ptr = allocator1.alloc(65536);
    EXPECT_TRUE(NULL != ptr);
    ptr_buffer[times] = ptr;
  }
  for (int64_t times = 0; times < MALLOC_TIMES_PER_THREAD; ++times) {
    allocator1.free(ptr_buffer[times]);
    ptr_buffer[times] = NULL;
  }
  return NULL;
}

TEST(TestLfFIFOAllocator, multipe_threads_direct_alloc)
{
  LIB_LOG(INFO, "start multiple threads direct alloc test");
  static const int64_t THREAD_NUM = 16;
  pthread_t work_thread[THREAD_NUM];
  ASSERT_EQ(OB_SUCCESS, allocator1.init(PAGE_SIZE, ObModIds::OB_PARTITION_STORAGE));
  ASSERT_EQ(0, pthread_barrier_init(&barrier1, NULL, THREAD_NUM));
  for (int64_t i = 0; i < THREAD_NUM; ++i) {
    int ret = pthread_create(&work_thread[i], NULL, &th_direct_alloc_func, NULL);
    ASSERT_EQ(0, ret);
  }
  for (int64_t i = 0; i < THREAD_NUM; ++i) {
    ASSERT_EQ(0, pthread_join(work_thread[i], NULL));
  }
  ASSERT_EQ(0, pthread_barrier_destroy(&barrier1));
  allocator1.destroy();
}

void *th_normal_alloc_func(void *arg)
{
  int64_t size = *(int64_t *)arg;
  static const int64_t MALLOC_PER_LOOP = 1024;
  static const int64_t LOOP = 8192;
  void *ptr_buffer[MALLOC_PER_LOOP];
  for (int64_t i = 0; i < MALLOC_PER_LOOP; ++i) {
    ptr_buffer[i] = NULL;
  }
  pthread_barrier_wait(&barrier1);
  for (int64_t i = 0; i < LOOP; ++i) {
    for (int64_t times = 0; times < MALLOC_PER_LOOP; ++times) {
      void *ptr = allocator1.alloc(sizeof(size));
      EXPECT_TRUE(NULL != ptr);
      ptr_buffer[times] = ptr;
    }
    for (int64_t times = 0; times < MALLOC_PER_LOOP; ++times) {
      allocator1.free(ptr_buffer[times]);
      ptr_buffer[times] = NULL;
    }
  }
  return NULL;
}

int book_alloc(const int64_t size, void *&ptr)
{
  ptr = allocator1.alloc(size);
  int64_t ptr_key = reinterpret_cast<int64_t>(ptr);
  int64_t ptr_value = 0;
  int ret = pc.get_refactored(ptr_key, ptr_value);
  if (OB_SUCCESS != ret) {
    ret = pc.set_refactored(ptr_key, 1, 0);
    if (OB_SUCCESS == ret) {
    } else {
      printf("put key[%ld], value[%ld] ret[%d].\n", ptr_key, ptr_value, ret);
      ret = OB_ERR_UNEXPECTED;
    }
  } else {
    if (0 != ptr_value) {
      printf("bug here! found same ptr_key[%ld] not free.", ptr_key);
      ret = OB_ERR_UNEXPECTED;
    } else {
      // overwrite;
      ret = pc.set_refactored(ptr_key, 1, 1);
      if (OB_SUCCESS != ret) {
        printf("system error, cannot overwrite ptr_key[%ld] with 1,ret[%d]\n", ptr_key, ret);
        ret = OB_ERR_UNEXPECTED;
      } else {
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int book_free(void *ptr)
{
  int64_t ptr_key = reinterpret_cast<int64_t>(ptr);
  int64_t ptr_value = 0;
  int ret = pc.get_refactored(ptr_key, ptr_value);
  if (OB_SUCCESS != ret) {
    printf("bug here! ptr_key[%ld] has not allocated[%d].\n", ptr_key, ret);
  } else {
    if (1 != ptr_value) {
      printf("bug here! found ptr_key[%ld] has freed[%ld].\n", ptr_key, ptr_value);
    } else {
      // overwrite;
      allocator1.free(ptr);
      ret = pc.set_refactored(ptr_key, 0, 1);
      if (OB_SUCCESS != ret) {
        printf("system error, cannot overwrite ptr_key[%ld] with 0,ret[%d]\n", ptr_key, ret);
        ret = OB_ERR_UNEXPECTED;
      } else {

        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

void *th_normal_alloc_free_func(void *arg)
{
  int64_t size = *(int64_t *)arg;
  UNUSED(size);
  static const int64_t MALLOC_PER_LOOP = 16;
  static const int64_t LOOP = 8192;
  void *ptr_buffer[MALLOC_PER_LOOP];
  int64_t alloc_time = 0;
  int64_t free_time = 0;
  int64_t start = 0;
  int64_t end = 0;
  int ret = 0;
  for (int64_t i = 0; i < MALLOC_PER_LOOP; ++i) {
    ptr_buffer[i] = NULL;
  }
  pthread_barrier_wait(&barrier1);
  for (int64_t i = 0; i < LOOP; ++i) {
    for (int64_t times = 0; times < MALLOC_PER_LOOP; ++times) {
      start = ObTimeUtility::current_time();
      //ret = book_alloc(32, ptr_buffer[times]);
      ptr_buffer[times] = allocator1.alloc(32);
      end = ObTimeUtility::current_time();
      alloc_time += (end - start);
      EXPECT_TRUE(ret == 0);
      EXPECT_TRUE(NULL != ptr_buffer[times]);
    }
    for (int64_t times = 0; times < MALLOC_PER_LOOP; ++times) {
      start = ObTimeUtility::current_time();
      //ret = book_free(ptr_buffer[times]);
      allocator1.free(ptr_buffer[times]);
      end = ObTimeUtility::current_time();
      free_time += (end - start);
      EXPECT_TRUE(ret == 0);
      ptr_buffer[times] = NULL;
    }
  }
  printf("average alloc[%ld], free[%ld]\n",
         alloc_time/LOOP/MALLOC_PER_LOOP,
         free_time/LOOP/MALLOC_PER_LOOP);
  return NULL;
}

TEST(TestLfFIFOAllocator, multiple_threads_normal_alloc_32B)
{
  LIB_LOG(INFO, "start multipe threads normal alloc test");
  static const int64_t THREAD_NUM = 8;
  static int64_t ALLOC_SIZE = 32;
  pthread_t work_thread[THREAD_NUM];
  ASSERT_EQ(OB_SUCCESS, allocator1.init(PAGE_SIZE, ObModIds::OB_PARTITION_STORAGE));
  ASSERT_EQ(0, pthread_barrier_init(&barrier1, NULL, THREAD_NUM));
  for (int64_t i = 0; i < THREAD_NUM; ++i) {
    int ret = pthread_create(&work_thread[i], NULL, &th_normal_alloc_func, &ALLOC_SIZE);
    ASSERT_EQ(0, ret);
  }
  for (int64_t i = 0; i < THREAD_NUM; ++i) {
    ASSERT_EQ(0, pthread_join(work_thread[i], NULL));
  }
  ASSERT_EQ(0, pthread_barrier_destroy(&barrier1));
  allocator1.destroy();
}

TEST(TestLfFIFOAllocator, multiple_threads_normal_alloc_128B)
{
  LIB_LOG(INFO, "start multipe threads normal alloc test");
  static const int64_t THREAD_NUM = 8;
  static int64_t ALLOC_SIZE = 128;
  pthread_t work_thread[THREAD_NUM];
  ASSERT_EQ(OB_SUCCESS, allocator1.init(PAGE_SIZE, ObModIds::OB_PARTITION_STORAGE));
  ASSERT_EQ(0, pthread_barrier_init(&barrier1, NULL, THREAD_NUM));
  for (int64_t i = 0; i < THREAD_NUM; ++i) {
    int ret = pthread_create(&work_thread[i], NULL, &th_normal_alloc_func, &ALLOC_SIZE);
    ASSERT_EQ(0, ret);
  }
  for (int64_t i = 0; i < THREAD_NUM; ++i) {
    ASSERT_EQ(0, pthread_join(work_thread[i], NULL));
  }
  ASSERT_EQ(0, pthread_barrier_destroy(&barrier1));
  allocator1.destroy();
}

TEST(TestLfFIFOAllocator, multiple_threads_normal_alloc_1K)
{
  LIB_LOG(INFO, "start multipe threads normal alloc test");
  static const int64_t THREAD_NUM = 8;
  static int64_t ALLOC_SIZE = 1024;
  pthread_t work_thread[THREAD_NUM];
  ASSERT_EQ(OB_SUCCESS, allocator1.init(PAGE_SIZE, ObModIds::OB_PARTITION_STORAGE));
  ASSERT_EQ(0, pthread_barrier_init(&barrier1, NULL, THREAD_NUM));
  for (int64_t i = 0; i < THREAD_NUM; ++i) {
    int ret = pthread_create(&work_thread[i], NULL, &th_normal_alloc_func, &ALLOC_SIZE);
    ASSERT_EQ(0, ret);
  }
  for (int64_t i = 0; i < THREAD_NUM; ++i) {
    ASSERT_EQ(0, pthread_join(work_thread[i], NULL));
  }
  ASSERT_EQ(0, pthread_barrier_destroy(&barrier1));
  allocator1.destroy();
}

TEST(TestLfFIFOAllocator, multiple_threads_normal_alloc_4K)
{
  LIB_LOG(INFO, "start multipe threads normal alloc test");
  static const int64_t THREAD_NUM = 8;
  static int64_t ALLOC_SIZE = 4 * 1024;
  pthread_t work_thread[THREAD_NUM];
  ASSERT_EQ(OB_SUCCESS, allocator1.init(PAGE_SIZE, ObModIds::OB_PARTITION_STORAGE));
  ASSERT_EQ(0, pthread_barrier_init(&barrier1, NULL, THREAD_NUM));
  for (int64_t i = 0; i < THREAD_NUM; ++i) {
    int ret = pthread_create(&work_thread[i], NULL, &th_normal_alloc_func, &ALLOC_SIZE);
    ASSERT_EQ(0, ret);
  }
  for (int64_t i = 0; i < THREAD_NUM; ++i) {
    ASSERT_EQ(0, pthread_join(work_thread[i], NULL));
  }
  ASSERT_EQ(0, pthread_barrier_destroy(&barrier1));
  allocator1.destroy();
}

TEST(TestLfFIFOAllocator, multiple_threads_normal_alloc_16K)
{
  LIB_LOG(INFO, "start multipe threads normal alloc test");
  static const int64_t THREAD_NUM = 8;
  static int64_t ALLOC_SIZE = 16 * 1024;
  pthread_t work_thread[THREAD_NUM];
  ASSERT_EQ(OB_SUCCESS, allocator1.init(PAGE_SIZE, ObModIds::OB_PARTITION_STORAGE));
  ASSERT_EQ(0, pthread_barrier_init(&barrier1, NULL, THREAD_NUM));
  for (int64_t i = 0; i < THREAD_NUM; ++i) {
    int ret = pthread_create(&work_thread[i], NULL, &th_normal_alloc_func, &ALLOC_SIZE);
    ASSERT_EQ(0, ret);
  }
  for (int64_t i = 0; i < THREAD_NUM; ++i) {
    ASSERT_EQ(0, pthread_join(work_thread[i], NULL));
  }
  ASSERT_EQ(0, pthread_barrier_destroy(&barrier1));
  allocator1.destroy();
}

TEST(TestLfFIFOAllocator, multiple_threads_normal_alloc_free_32B)
{
  LIB_LOG(INFO, "start multipe threads normal alloc test");
  static const int64_t THREAD_NUM = 128;
  static int64_t ALLOC_SIZE = 32;
  ASSERT_EQ(OB_SUCCESS, pc.create(10240, ObModIds::OB_CS_COMMON, ObModIds::OB_CS_COMMON));
  ASSERT_EQ(OB_SUCCESS, allocator1.init(PAGE_SIZE, ObModIds::OB_PARTITION_STORAGE));
  ASSERT_EQ(0, pthread_barrier_init(&barrier1, NULL, THREAD_NUM));

  cotesting::FlexPool([&]{
    th_normal_alloc_free_func(&ALLOC_SIZE);
  }, THREAD_NUM).start();
  ASSERT_EQ(0, pthread_barrier_destroy(&barrier1));
  allocator1.destroy();
}

int main(int argc, char** argv)
{
  system("rm -f test_lf_fifo_allocator.log*");
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_lf_fifo_allocator.log", true);
  logger.set_log_level("info");
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
